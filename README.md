# DB Fahrplan Alert 🚆⚠️

![R](https://img.shields.io/badge/R-4.0+-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg)

Dieses R-Skript überwacht den Fahrplan der Deutschen Bahn für einen bestimmten Bahnhof und sendet Telegram-Benachrichtigungen bei **Verspätungen ≥ 15 Minuten** oder **Zugausfällen**.  

---

## Features

- ✅ Abruf von geplanten Fahrplänen via DB-Plan API  
- ✅ Abruf von Echtzeit-Änderungen via FCHG API  
- ✅ Berechnung von Verspätungen und Erkennung von Zugausfällen  
- ✅ Benachrichtigung per Telegram  

---

## Voraussetzungen

- **R** ≥ 4.0  
- R-Pakete:  
  ```r
  dplyr, lubridate, httr, xml2, purrr, stringr, tibble
