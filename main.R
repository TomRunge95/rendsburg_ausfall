# =========================================
# R-Skript: DB Fahrplan + FCHG + Telegram Alert
# =========================================

if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos = "https://packagemanager.posit.co/cran/latest")
}

renv::restore()


library(xml2)
library(dplyr)
library(purrr)
library(tibble)
library(lubridate)
library(httr)


# -------------------
# Parameter
# -------------------
evaNo <- 8000312                        # Hamburg Hbf
client_id <- "4d202863a4c79e8d7e069b9ef38b57f7"
client_secret <- Sys.getenv("DB_API")   # DB API Key
date <- format(Sys.Date(), "%y%m%d")
hour <- format(Sys.time(), "%H")

bot_token <- Sys.getenv("Telegram_BOT")
chat_id   <- 75538067

# -------------------
# Funktion: Telegram senden
# -------------------
send_telegram <- function(message) {
  url <- paste0("https://api.telegram.org/bot", bot_token, "/sendMessage")
  POST(url, body = list(chat_id = chat_id, text = message), encode = "form")
}




# -------------------
# Funktion: Stop-Knoten aus XML in tibble
# -------------------
parse_stop <- function(stop, include_trip = FALSE) {
  stop_attr <- as.list(xml_attrs(stop))
  
  ar <- xml_find_first(stop, ".//ar")
  ar_attr <- if (!is.na(ar)) as.list(xml_attrs(ar)) else list()
  
  dp <- xml_find_first(stop, ".//dp")
  dp_attr <- if (!is.na(dp)) as.list(xml_attrs(dp)) else list()
  
  msgs <- xml_find_all(stop, ".//m")
  msg_attr <- if(length(msgs) > 0) {
    paste(sapply(msgs, function(m) paste(xml_attrs(m), collapse=";")), collapse=" | ")
  } else {
    NA_character_
  }
  
  # Optional: Trip info
  if(include_trip) {
    tl <- xml_find_first(stop, ".//tl")
    tl_attr <- if (!is.na(tl)) as.list(xml_attrs(tl)) else list()
    tibble(
      stop_attr = list(stop_attr),
      tl_attr = list(tl_attr),
      ar_attr = list(ar_attr),
      dp_attr = list(dp_attr),
      messages = msg_attr
    )
  } else {
    tibble(
      stop_attr = list(stop_attr),
      ar_attr = list(ar_attr),
      dp_attr = list(dp_attr),
      messages = msg_attr
    )
  }
}

# =========================================
# 1. PLAN-Abfrage
# =========================================
url_plan <- paste0("https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/", 
                   evaNo, "/", date, "/", hour)

res <- GET(url_plan, add_headers(
  "DB-Client-Id" = client_id,
  "DB-Api-Key"   = client_secret,
  "accept"       = "application/xml"
))

if(status_code(res) == 200) {
  writeBin(content(res, "raw"), "plan_hamburg.xml")
  cat("PLAN XML erfolgreich gespeichert\n")
} else {
  stop("Fehler bei PLAN API: ", status_code(res))
}

xml_plan <- read_xml("plan_hamburg.xml")
stops_plan <- xml_find_all(xml_plan, ".//s")
plan <- map_df(stops_plan, parse_stop, include_trip = TRUE)

plan_simple <- plan %>%
  mutate(
    stop_id  = map_chr(stop_attr, ~ .x[["id"]] %||% NA_character_),
    eva      = map_chr(stop_attr, ~ .x[["eva"]] %||% NA_character_),
    
    trip_n   = map_chr(tl_attr, ~ .x[["n"]] %||% NA_character_),
    trip_cat = map_chr(tl_attr, ~ .x[["c"]] %||% NA_character_),
    
    arr_pt   = map_chr(ar_attr, ~ .x[["pt"]] %||% NA_character_),
    arr_line = map_chr(ar_attr, ~ .x[["l"]] %||% NA_character_),
    arr_ppth = map_chr(ar_attr, ~ .x[["ppth"]] %||% NA_character_),
    
    dep_pt   = map_chr(dp_attr, ~ .x[["pt"]] %||% NA_character_),
    dep_line = map_chr(dp_attr, ~ .x[["l"]] %||% NA_character_),
    dep_ppth = map_chr(dp_attr, ~ .x[["ppth"]] %||% NA_character_)
  ) %>%
  mutate(
    arr_time = parse_date_time(arr_pt, orders = "ymdHM", tz = "Europe/Berlin"),
    dep_time = parse_date_time(dep_pt, orders = "ymdHM", tz = "Europe/Berlin")
  ) %>%
  select(stop_id, eva, trip_n, trip_cat, dep_line, arr_ppth, dep_ppth, arr_line, arr_time, dep_time, messages)

# =========================================
# 2. FCHG-Abfrage
# =========================================
url_fchg <- paste0(
  "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/",
  evaNo,
  "?schema=timetable"
)

res <- GET(url_fchg, add_headers(
  "DB-Client-Id" = client_id,
  "DB-Api-Key"   = client_secret,
  "accept"       = "application/xml"
))

if(status_code(res) == 200) {
  writeBin(content(res, "raw"), "fchg_hamburg.xml")
  cat("FCHG XML erfolgreich gespeichert\n")
} else {
  stop("Fehler bei FCHG API: ", status_code(res))
}

xml_fchg <- read_xml("fchg_hamburg.xml")
stops_fchg <- xml_find_all(xml_fchg, ".//s")
fchg <- map_df(stops_fchg, parse_stop)

fchg_simple <- fchg %>%
  mutate(
    stop_id       = map_chr(stop_attr, ~ .x[["id"]] %||% NA_character_),
    eva           = map_chr(stop_attr, ~ .x[["eva"]] %||% NA_character_),
    
    dep_ct        = map_chr(dp_attr, ~ .x[["ct"]]  %||% NA_character_),
    dep_clt       = map_chr(dp_attr, ~ .x[["clt"]] %||% NA_character_),
    dep_line_fchg = map_chr(dp_attr, ~ .x[["l"]]  %||% NA_character_),
    
    arr_ct        = map_chr(ar_attr, ~ .x[["ct"]]  %||% NA_character_),
    arr_clt       = map_chr(ar_attr, ~ .x[["clt"]] %||% NA_character_),
    arr_line_fchg = map_chr(ar_attr, ~ .x[["l"]]  %||% NA_character_)
  ) %>%
  select(stop_id, eva, dep_ct, dep_line_fchg, dep_clt, arr_ct, arr_line_fchg, arr_clt, messages) %>%
  mutate(
    dep_time_fchg = ymd_hm(dep_ct, tz = "Europe/Berlin"),
    arr_time_fchg = ymd_hm(arr_ct, tz = "Europe/Berlin")
  )

# =========================================
# 3. Merge und Berechnungen
# =========================================
df_merged <- merge(
  plan_simple,
  fchg_simple,
  by = "stop_id",
  all.x = TRUE,
  suffixes = c("", "_fchg")
)

df_merged <- df_merged %>%
  mutate(
    is_canceled   = if_else(!is.na(dep_clt) | !is.na(arr_clt), TRUE, FALSE),
    dep_delay_min = as.numeric(difftime(dep_time_fchg, dep_time, units = "mins")),
    arr_delay_min = as.numeric(difftime(arr_time_fchg, arr_time, units = "mins"))
  )

# =========================================
# 4. Telegram Alerts
# =========================================
df_alert <- df_merged %>%
  filter(is_canceled | dep_delay_min >= 3 | arr_delay_min >= 3)

if(nrow(df_alert) > 0){
  msg <- paste0(
    "🚨 Ausfälle/Verspätungen ≥ 60 min:\n",
    paste0(
      df_alert$trip_cat, " ", df_alert$dep_line,
      " – Abfahrt: ", round(df_alert$dep_delay_min,0), " min, ",
      "Ankunft: ", round(df_alert$arr_delay_min,0), " min",
      ifelse(df_alert$is_canceled, " ⚠️ entfällt", ""),
      "\n",
      collapse = ""
    )
  )
  send_telegram(msg)
}





