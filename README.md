# DB Fahrplan Alert 🚆⚠️

![R](https://img.shields.io/badge/R-4.0+-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg)

Dieses R-Skript überwacht den Fahrplan der Deutschen Bahn für einen bestimmten Bahnhof und sendet Telegram-Benachrichtigungen bei **Verspätungen ≥ 15 Minuten** oder **Zugausfällen**.

---

## Features

- ✅ Abruf von geplanten Fahrplänen via DB-Plan API
- ✅ Abruf von Echtzeit-Änderungen via FCHG API
- ✅ Berechnung von Verspätungen und Erkennung von Zugausfällen
- ✅ Benachrichtigung per Telegram
- ✅ Keine Wiederholung bereits gemeldeter unveränderter Meldungen
- ✅ Meldet nur Züge, deren relevante Abfahrts- oder Ankunftszeit noch nicht vorbei ist

---

## Benachrichtigungslogik

Das Skript speichert den zuletzt gemeldeten Status in `.cache/alert_state.rds`.
Eine Telegram-Nachricht wird nur gesendet, wenn eine Meldung neu ist oder sich relevant geändert hat, zum Beispiel:

- ein Zug fällt neu aus
- eine Verspätung überschreitet erstmals die 15-Minuten-Grenze
- die neue Abfahrts- oder Ankunftszeit ändert sich
- aus einer Verspätung wird ein Ausfall

Bereits abgefahrene oder angekommene Züge werden nicht mehr gemeldet. Bei Verspätungen zählt dafür die aktualisierte Echtzeit-Uhrzeit, falls die DB sie liefert.

Für GitHub Actions wird `.cache` im Workflow per `actions/cache` wiederhergestellt und nach jedem Lauf neu gespeichert.

---

## Voraussetzungen

- **R** ≥ 4.0
- R-Pakete:
  ```r
  dplyr, lubridate, httr, xml2, purrr, stringr, tibble
