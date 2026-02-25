# Dienstplan → CalDAV – Refactored

Dieses Projekt automatisiert den Prozess des Herunterladens und Aktualisierens von Kalendern für Kollegen aus der ARD-ZDF Box.

## Funktionen

- Lädt die neuesten Kalenderdaten herunter.
- Aktualisiert Kalender für mehrere Kollegen.
- Protokolliert den Prozess in eine Datei und auf die Konsole.
- Misst die Zeit, die für den gesamten Prozess benötigt wird.

## Anforderungen

- Python 3.x
- Erforderliche Python-Pakete (mit `pip install -r requirements.txt` installieren)
- Eine config.json mit den notwendigen Login-Daten

## Architektur-Überblick

```
dienstplan/
├── main.py                  # Einziger Einstiegspunkt (ersetzt DienstplanStart.py)
├── config.py                # Zentrale Konfiguration (einmal laden, überall nutzen)
├── downloader.py            # ZIP-Download & Entpacken (ersetzt DienstplanDownload.py)
├── excel_parser.py          # Excel-Dateien lesen & Dienste extrahieren
├── laufzettel.py            # Laufzettel-HTML parsen & verwalten
├── calendar_client.py       # CalDAV-Verbindung, Cache, Event-CRUD
├── event_builder.py         # iCal-Event-Erzeugung (sauberes VCALENDAR)
├── notifier.py              # E-Mail-Benachrichtigungen
├── holidays_de.py           # Deutsche Feiertage (Hamburg)
├── utils.py                 # Hilfsfunktionen (Timer, Logging, Datums-Parsing)
├── cleaner.py               # Alte Termine löschen (ersetzt Diensteloeschen.py)
├── config.json              # Credentials & URLs
├── colleagues.json          # Kollegen-Liste mit Optionen
├── email_config.json        # E-Mail-Zuordnung pro Kollege
└── requirements.txt         # Python-Abhängigkeiten
```

## Wesentliche Änderungen gegenüber der alten Version

### 1. Keine Subprocesses mehr
Das Original startet für jeden der 40 Kollegen einen neuen Python-Prozess.
Jeder Prozess lädt erneut: Imports, Config, CalDAV-Login, Laufzettel, Holidays.

**Neu:** Ein einzelner Prozess mit ThreadPoolExecutor. Geteilte Ressourcen
(Config, Laufzettel, Holidays) werden einmal geladen und an alle Threads übergeben.
Nur die CalDAV-Verbindung ist pro Kollege individuell.

### 2. Config wird einmal geladen
`load_credentials()` las bei jedem Aufruf die JSON-Datei von der Platte.
**Neu:** `AppConfig` lädt alles einmal beim Start und bietet typisierte Zugriffsmethoden.

### 3. Keine globalen Variablen
Das Original nutzte ~12 globale Variablen (`cal_cache`, `night_shifts_count`,
`laufzettel_data`, `nextlaufzettel`, etc.).
**Neu:** Alles über Klassen und Funktionsparameter. Jeder Thread bekommt seinen
eigenen `CalendarClient`, aber teilt sich den `LaufzettelManager`.

### 4. Saubere Trennung
- Excel-Parsing kennt kein CalDAV
- CalDAV kennt kein Excel
- Event-Erzeugung ist eine reine Funktion (Input → iCal-String)
- Laufzettel-Verwaltung ist ein eigenes Modul mit eigenem Cache

### 5. Bessere Fehlerbehandlung
- Keine bare `except:` mehr
- Strukturiertes Logging statt `print()`
- Fehler in einem Kollegen-Thread brechen nicht die anderen ab

## Migration

Die JSON-Konfigurationsdateien (config.json, colleagues.json, email_config.json)
bleiben **unverändert** kompatibel. Laufzettel-HTML-Dateien ebenfalls.

## Ausführung

```bash
# Normal (mit Download, nur neue Dateien)
python main.py

# Alles neu schreiben (Force)
python main.py --force

# Ohne Download, direkt verarbeiten
python main.py --no-download

# Alte Termine löschen
python main.py --delete

# Einzelnen Kollegen verarbeiten (Debug)
python main.py --single "Meier" -c -o -n
```

## Protokollierung

Das Skript protokolliert seine Ausgabe sowohl in eine Protokolldatei (`Dienstplanscript.log`) als auch auf die Konsole. Die Protokolldatei verwendet einen rotierenden Datei-Handler, um die Dateigröße und Backups zu verwalten.

## Timer-Funktionalität

Das Skript enthält eine Timer-Funktionalität, um die für verschiedene Aufgaben benötigte Zeit zu messen. Die Gesamtdauer wird am Ende der Skriptausführung protokolliert.

## Aufbau der json Dateien

config.json
```sh
{
  "username_login_ard": "mailadresse",
  "password_login_ard": "passwort",
  "mailpassword": "mailpasswort",
  "ardbox": "####",
  "downloadordner": "####",
  "kalenderbase": "####",
  "abobase": "####",
  "caldavard": "####",
  "notifymail": "mailadresse",
  "user1": "Beispielnutzer",
  "user2": "Beispielnutzer2"
}
```

colleagues.json
```sh
{
    "colleagues": [
        ["user_name", ["-o", "-n"]],
        ["user_name2", ["-o"]],
        ["user_name3", []]
    ]
}
```
