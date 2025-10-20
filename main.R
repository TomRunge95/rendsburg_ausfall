library(xml2)
library(dplyr)
library(purrr)
library(tibble)
library(lubridate)
library(httr)


# ---------------- Parameter ----------------
evaNo <- 8000312                       # Hamburg Hbf
client_id <- "4d202863a4c79e8d7e069b9ef38b57f7"
client_secret <- Sys.getenv("DB_API")    # DB API Key
date <- format(Sys.Date(), "%y%m%d")
hour <- format(Sys.time(), "%H")

# ---------------- XML von API abrufen ----------------
url_plan <- paste0("https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/", 
                   evaNo, "/", date, "/", hour)

res <- GET(url_plan, add_headers(
  "DB-Client-Id" = client_id,
  "DB-Api-Key"   = client_secret,
  "accept"       = "application/xml"
))

# Prüfen, ob Abruf erfolgreich
if(status_code(res) == 200) {
  # XML in Datei speichern
  writeBin(content(res, "raw"), "plan_hamburg.xml")
  cat("XML erfolgreich gespeichert in 'plan_hamburg.xml'\n")
} else {
  cat("Fehler bei API-Abruf:", status_code(res), "\n")
}


# XML einlesen
xml_file <- read_xml("plan_hamburg.xml")
stops <- xml_find_all(xml_file, ".//s")

# Funktion: Stop-Knoten in tibble
parse_stop <- function(stop) {
  # Stop-Attribute
  stop_attr <- as.list(xml_attrs(stop))
  
  # Trip info
  tl <- xml_find_first(stop, ".//tl")
  tl_attr <- if (!is.na(tl)) as.list(xml_attrs(tl)) else list()
  
  # Arrival
  ar <- xml_find_first(stop, ".//ar")
  ar_attr <- if (!is.na(ar)) as.list(xml_attrs(ar)) else list()
  
  # Departure
  dp <- xml_find_first(stop, ".//dp")
  dp_attr <- if (!is.na(dp)) as.list(xml_attrs(dp)) else list()
  
  # Nachrichten: falls mehrere <m> vorhanden, als Liste zusammenfassen
  msgs <- xml_find_all(stop, ".//m")
  msg_attr <- if(length(msgs) > 0) {
    paste(sapply(msgs, function(m) paste(xml_attrs(m), collapse=";")), collapse=" | ")
  } else {
    NA_character_
  }
  
  # Alle zusammenführen
  tibble(
    stop_attr = list(stop_attr),
    tl_attr = list(tl_attr),
    ar_attr = list(ar_attr),
    dp_attr = list(dp_attr),
    messages = msg_attr
  )
}

# Alle Stops in Dataframe
plan <- map_df(stops, parse_stop)

# Ergebnis prüfen
plan
plan_simple <- plan %>%
  mutate(
    stop_id   = map_chr(stop_attr, ~ .x[["id"]] %||% NA_character_),
    eva       = map_chr(stop_attr, ~ .x[["eva"]] %||% NA_character_),
    
    trip_n    = map_chr(tl_attr, ~ .x[["n"]] %||% NA_character_),
    trip_cat  = map_chr(tl_attr, ~ .x[["c"]] %||% NA_character_),
    
    arr_pt    = map_chr(ar_attr, ~ .x[["pt"]] %||% NA_character_),
    arr_line  = map_chr(ar_attr, ~ .x[["l"]] %||% NA_character_),
    arr_ppth    = map_chr(ar_attr, ~ .x[["ppth"]] %||% NA_character_),
    
    
    dep_pt    = map_chr(dp_attr, ~ .x[["pt"]] %||% NA_character_),
    dep_line  = map_chr(dp_attr, ~ .x[["l"]] %||% NA_character_),
    dep_ppth    = map_chr(dp_attr, ~ .x[["ppth"]] %||% NA_character_)
  ) %>%
  mutate(
    arr_time = parse_date_time(arr_pt, orders = "ymdHM", tz = "Europe/Berlin"),
    dep_time = parse_date_time(dep_pt, orders = "ymdHM", tz = "Europe/Berlin")
  ) %>%
  select(stop_id, eva, trip_n, trip_cat, dep_line, arr_ppth, dep_ppth, arr_line, arr_time, dep_time,  messages)

# Ergebnis prüfen
glimpse(plan_simple)
head(plan_simple)




# ---------------- URL für fchg ----------------
url_fchg <- paste0(
  "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/",
  evaNo,
  "?schema=timetable"
)

# ---------------- XML abrufen ----------------
res <- GET(url_fchg, add_headers(
  "DB-Client-Id" = client_id,
  "DB-Api-Key"   = client_secret,
  "accept"       = "application/xml"
))

# Prüfen, ob Abruf erfolgreich
if(status_code(res) == 200) {
  # XML in Datei speichern
  writeBin(content(res, "raw"), "fchg_hamburg.xml")
  cat("XML erfolgreich gespeichert in 'fchg_hamburg.xml'\n")
} else {
  cat("Fehler bei API-Abruf:", status_code(res), "\n")
}

xml_file <- read_xml("fchg_hamburg.xml")
stops <- xml_find_all(xml_file, ".//s")

# Funktion: Stop-Knoten in tibble
parse_stop <- function(stop) {
  # Stop-Attribute
  stop_attr <- as.list(xml_attrs(stop))
  
  # Arrival
  ar <- xml_find_first(stop, ".//ar")
  ar_attr <- if (!is.na(ar)) as.list(xml_attrs(ar)) else list()
  
  # Departure
  dp <- xml_find_first(stop, ".//dp")
  dp_attr <- if (!is.na(dp)) as.list(xml_attrs(dp)) else list()
  
  # Nachrichten: falls mehrere <m> vorhanden, als Liste zusammenfassen
  msgs <- xml_find_all(stop, ".//m")
  msg_attr <- if(length(msgs) > 0) {
    paste(sapply(msgs, function(m) paste(xml_attrs(m), collapse=";")), collapse=" | ")
  } else {
    NA_character_
  }
  
  # Alle zusammenführen
  tibble(
    stop_attr = list(stop_attr),
    ar_attr = list(ar_attr),
    dp_attr = list(dp_attr),
    messages = msg_attr
  )
}

# Alle Stops in Dataframe
fchg <- map_df(stops, parse_stop)

# Ergebnis prüfen
fchg

fchg_simple <- fchg %>%
  mutate(
    stop_id    = map_chr(stop_attr, ~ .x[["id"]] %||% NA_character_),
    eva        = map_chr(stop_attr, ~ .x[["eva"]] %||% NA_character_),
    
    dep_ct     = map_chr(dp_attr, ~ .x[["ct"]] %||% NA_character_),
    dep_line   = map_chr(dp_attr, ~ .x[["l"]] %||% NA_character_),
    dep_clt     = map_chr(dp_attr, ~ .x[["clt"]] %||% NA_character_),
    
    
    arr_ct     = map_chr(ar_attr, ~ .x[["ct"]] %||% NA_character_),
    arr_line   = map_chr(ar_attr, ~ .x[["l"]] %||% NA_character_),
    arr_clt     = map_chr(dp_attr, ~ .x[["clt"]] %||% NA_character_),
    
  ) %>%
  select(stop_id, eva , dep_ct, dep_line, dep_clt, arr_ct, arr_line, arr_clt, messages) %>%
  mutate(
    dep_time = ymd_hm(dep_ct, tz = "Europe/Berlin"),
    arr_time = ymd_hm(arr_ct, tz = "Europe/Berlin")
  )

# Ergebnis prüfen
glimpse(fchg_simple)



df_merged <- merge(
  plan_simple, 
  fchg_simple, 
  by = "stop_id",
  all.x = TRUE,
  suffixes = c("", "_fchg")  # plan_simple bleibt unverändert, fchg_simple bekommt _fchg
)



df_merged <- df_merged |>
  mutate(
    is_canceled = if_else(!is.na(dep_clt) |
                            !is.na(arr_clt), TRUE, FALSE),
    dep_delay_min = as.numeric(difftime(dep_time_fchg, dep_time, units = "mins")),
    arr_delay_min = as.numeric(difftime(arr_time_fchg, arr_time, units = "mins"))
  )



