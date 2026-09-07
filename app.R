# ------------------------------------------
# DB Fahrplan Alert (Standalone Script)
# ------------------------------------------

library(dplyr)
library(lubridate)
library(httr)
library(xml2)
library(purrr)
library(stringr)
library(tibble)

# Telegram Funktion
split_telegram_message <- function(header, items, max_chars = 3900) {
  chunks <- character()
  current <- header

  for(item in items) {
    candidate <- paste0(current, item, "\n\n")
    if(nchar(candidate, type = "chars") > max_chars && current != header) {
      chunks <- c(chunks, str_trim(current))
      current <- paste0(header, item, "\n\n")
    } else {
      current <- candidate
    }
  }

  c(chunks, str_trim(current))
}

html_escape <- function(x) {
  x <- coalesce(as.character(x), "")
  x %>%
    str_replace_all("&", "&amp;") %>%
    str_replace_all("<", "&lt;") %>%
    str_replace_all(">", "&gt;")
}

format_delay <- function(planned_time, changed_time, delay_min) {
  if(is.na(changed_time) || is.na(delay_min)) {
    return(paste0("Plan: ", format(planned_time, "%H:%M"), " Uhr"))
  }

  paste0(
    "Plan: ", format(planned_time, "%H:%M"), " Uhr · ",
    "Neu: <b>", format(changed_time, "%H:%M"), " Uhr</b> ",
    "(+", round(delay_min), " Min)"
  )
}

format_alert_item <- function(row) {
  is_departure <- !is.na(row$dep_line) && row$dep_line != ""
  line <- if(is_departure) row$dep_line else row$arr_line
  planned_time <- if(is_departure) row$dep_time else row$arr_time
  changed_time <- if(is_departure) row$dep_time_fchg else row$arr_time_fchg
  delay_min <- if(is_departure) row$dep_delay_min else row$arr_delay_min
  event_label <- if(is_departure) "Abfahrt" else "Ankunft"
  status <- if(row$is_canceled) "❌ <b>Ausfall</b>" else "⚠️ <b>Verspätung</b>"

  paste0(
    status, " · <b>", html_escape(line), "</b> · ", event_label, "\n",
    html_escape(row$von), " → ", html_escape(row$nach), "\n",
    if(row$is_canceled) {
      paste0("Plan: ", format(planned_time, "%H:%M"), " Uhr")
    } else {
      format_delay(planned_time, changed_time, delay_min)
    }
  )
}

alert_state_file <- function() {
  Sys.getenv("ALERT_STATE_FILE", unset = ".cache/alert_state.rds")
}

read_alert_state <- function(path) {
  if(!file.exists(path)) {
    return(tibble(alert_key = character(), alert_signature = character()))
  }

  state <- tryCatch(
    readRDS(path),
    error = function(e) {
      warning("Alert-State konnte nicht gelesen werden: ", conditionMessage(e))
      tibble(alert_key = character(), alert_signature = character())
    }
  )
  if(!all(c("alert_key", "alert_signature") %in% names(state))) {
    return(tibble(alert_key = character(), alert_signature = character()))
  }

  state %>%
    select(alert_key, alert_signature) %>%
    distinct()
}

write_alert_state <- function(state, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(state, path)
}

send_telegram <- function(message) {
  bot_token <- Sys.getenv("TELEGRAM_BOT")
  chat_id   <-  Sys.getenv("TELEGRAM_CHAT_ID")
  url <- paste0("https://api.telegram.org/bot", bot_token, "/sendMessage")
  res <- POST(
    url,
    body = list(
      chat_id = chat_id,
      text = message,
      parse_mode = "HTML",
      disable_web_page_preview = TRUE
    ),
    encode = "form"
  )

  if(status_code(res) >= 300) {
    warning("Telegram API meldet Status ", status_code(res))
  }

  res
}

# XML Parser (wie gehabt)
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
  if(include_trip) {
    tl <- xml_find_first(stop, ".//tl")
    tl_attr <- if (!is.na(tl)) as.list(xml_attrs(tl)) else list()
    tibble(stop_attr = list(stop_attr), tl_attr = list(tl_attr), ar_attr = list(ar_attr), dp_attr = list(dp_attr), messages = msg_attr)
  } else {
    tibble(stop_attr = list(stop_attr), ar_attr = list(ar_attr), dp_attr = list(dp_attr), messages = msg_attr)
  }
}

# ------------------------------------------
# Hauptlogik (Datum + Zeitprüfung angepasst)
# ------------------------------------------
now_berlin <- with_tz(Sys.time(), "Europe/Berlin")

evaNo <- 8000312
client_id <- "4d202863a4c79e8d7e069b9ef38b57f7"
client_secret <- Sys.getenv("DB_API")

# Aktuelles Datum und Stunde
current_date <- as.Date(now_berlin, tz = "Europe/Berlin")
current_hour_int <- as.integer(format(now_berlin, "%H"))

# Nächsten 3 Stunden inkl. Überlauf über Mitternacht
hours_ahead <- 0:2
times <- tibble(
  hour = (current_hour_int + hours_ahead) %% 24,
  date = current_date + if_else((current_hour_int + hours_ahead) >= 24, 1, 0)
)

plan_list <- list()

for(i in seq_len(nrow(times))) {
  date_str <- format(times$date[i], "%y%m%d")
  hour_str <- sprintf("%02d", times$hour[i])

  url_plan <- paste0("https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/", evaNo, "/", date_str, "/", hour_str)
  res <- GET(url_plan, add_headers("DB-Client-Id" = client_id, "DB-Api-Key" = client_secret, "accept" = "application/xml"))

  if(status_code(res) == 404) {
    message("⚠️ Keine Daten für ", date_str, " ", hour_str, " Uhr verfügbar.")
    next
  }
  if(status_code(res) != 200) stop("Fehler bei PLAN API: ", status_code(res))

  xml_plan <- content(res, "raw") %>% read_xml()
  stops_plan <- xml_find_all(xml_plan, ".//s")
  if(length(stops_plan) == 0) {
    message("Keine Halte für ", date_str, " ", hour_str, " Uhr gefunden.")
    next
  }

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
      dep_time = parse_date_time(dep_pt, orders = "ymdHM", tz = "Europe/Berlin"),
      von  = coalesce(str_extract(arr_ppth, "^[^|]+"), "Rendsburg"),
      nach = coalesce(str_extract(dep_ppth, "(?<=\\|)[^|]+$"), "Rendsburg")
    ) %>%
    select(stop_id, eva, trip_n, trip_cat, dep_line, arr_ppth, von, dep_ppth, nach, arr_line, arr_time, dep_time, messages)

  plan_list[[length(plan_list)+1]] <- plan_simple
}

plan_simple <- bind_rows(plan_list)
if(nrow(plan_simple) == 0) {
  message("Keine Fahrplandaten im betrachteten Zeitraum gefunden. Lauf wird ohne Fehler beendet.")
  quit(save = "no", status = 0)
}

url_fchg <- paste0("https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/", evaNo, "?schema=timetable")
res <- GET(url_fchg, add_headers("DB-Client-Id" = client_id, "DB-Api-Key" = client_secret, "accept" = "application/xml"))
if(status_code(res) != 200) stop("Fehler bei FCHG API: ", status_code(res))
xml_fchg <- content(res, "raw") %>% read_xml()
stops_fchg <- xml_find_all(xml_fchg, ".//s")
if(length(stops_fchg) == 0) {
  fchg_simple <- tibble(
    stop_id = character(),
    eva = character(),
    dep_ct = character(),
    dep_clt = character(),
    dep_line_fchg = character(),
    arr_ct = character(),
    arr_clt = character(),
    arr_line_fchg = character(),
    dep_time_fchg = as.POSIXct(character(), tz = "Europe/Berlin"),
    arr_time_fchg = as.POSIXct(character(), tz = "Europe/Berlin")
  )
} else {
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
    mutate(
      dep_time_fchg = ymd_hm(dep_ct, tz = "Europe/Berlin"),
      arr_time_fchg = ymd_hm(arr_ct, tz = "Europe/Berlin")
    )
}

df_merged <- merge(plan_simple, fchg_simple, by = "stop_id", all.x = TRUE, suffixes = c("", "_fchg")) %>%
  mutate(
    is_canceled   = if_else(!is.na(dep_clt) | !is.na(arr_clt), TRUE, FALSE),
    dep_delay_min = as.numeric(difftime(dep_time_fchg, dep_time, units = "mins")),
    arr_delay_min = as.numeric(difftime(arr_time_fchg, arr_time, units = "mins")),
    dep_line = case_when(dep_line == "7" ~ "RE7", TRUE ~ dep_line),
    arr_line = case_when(arr_line == "7" ~ "RE7", TRUE ~ arr_line)
  )

df_alert <- df_merged %>%
  mutate(
    is_departure = !is.na(dep_line) & dep_line != "",
    alert_time = if_else(
      is_departure,
      coalesce(dep_time_fchg, dep_time),
      coalesce(arr_time_fchg, arr_time)
    ),
    alert_delay_min = if_else(is_departure, dep_delay_min, arr_delay_min),
    alert_key = paste(
      stop_id,
      if_else(is_departure, "dep", "arr"),
      coalesce(dep_line, arr_line, trip_n, "unknown"),
      sep = "|"
    ),
    alert_signature = paste(
      if_else(is_canceled, "canceled", "delayed"),
      if_else(is_departure, format(dep_time_fchg, "%Y-%m-%d %H:%M"), format(arr_time_fchg, "%Y-%m-%d %H:%M")),
      sep = "|"
    ),
    dep_time_fmt = format(dep_time, "%H:%M"),
    arr_time_fmt = format(arr_time, "%H:%M"),
    sort_time = alert_time
  ) %>%
  filter((is_canceled | alert_delay_min >= 15) & alert_time >= now_berlin) %>%
  arrange(sort_time)

state_path <- alert_state_file()
previous_alert_state <- read_alert_state(state_path)

df_alert_new <- df_alert %>%
  anti_join(previous_alert_state, by = c("alert_key", "alert_signature"))

current_alert_state <- df_alert %>%
  select(alert_key, alert_signature) %>%
  distinct()

write_alert_state(current_alert_state, state_path)

if(nrow(df_alert_new) > 0){
  header <- paste0("🚨 <b>DB Meldungen Rendsburg</b>\nStand: ", format(now_berlin, "%H:%M"), " Uhr\n\n")
  alert_items <- vapply(
    split(df_alert_new, seq_len(nrow(df_alert_new))),
    format_alert_item,
    character(1)
  )
  messages <- split_telegram_message(header, alert_items)

  walk(messages, send_telegram)
  message("✅ Telegram-Nachricht gesendet.")
} else if(nrow(df_alert) > 0) {
  message("ℹ️ Nur bereits bekannte Meldungen gefunden. Keine Telegram-Nachricht gesendet.")
} else {
  message("ℹ️ Keine Meldungen gefunden.")
}
