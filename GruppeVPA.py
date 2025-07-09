import sys
import time
import os
import argparse
import json
import pandas as pd
import datetime
import locale
import re
from caldav import DAVClient
from datetime import date, timedelta
import datetime as dt
import pytz
from itertools import chain
import logging
from logging.handlers import RotatingFileHandler
import signal
from bs4 import BeautifulSoup
import holidays
from dateutil.easter import easter
import subprocess

# Basisverzeichnis des Skripts
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Logging-Konfiguration
log_formatter = logging.Formatter('%(message)s')
log_file = os.path.join(BASE_DIR, "DienstplanscriptGruppe.log")
log_handler = RotatingFileHandler(log_file, maxBytes=5120 * 1024, backupCount=3)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.DEBUG)

# Custom filter to exclude specific messages
class ExcludeCaldavFilter(logging.Filter):
    def filter(self, record):
        # Exclude messages containing "GET" or "HTTP/1.1"
        return not any(keyword in record.getMessage() for keyword in ["HTTP/11", "DEPRECATION NOTICE", "share.ard-zdf-box.de"])

# Add the filter to the log handler
log_handler.addFilter(ExcludeCaldavFilter())
logging.getLogger("caldav").disabled = True

# Logger einrichten
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Ausgabe auf Konsole
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.DEBUG)
console_handler.addFilter(ExcludeCaldavFilter())  # Add the same filter to console output
logger.addHandler(console_handler)

# Initialisiere Timer
timers = {}


def start_timer(timer_name):
    timers[timer_name] = time.time()


start_timer("gesamt")
start_timer("initial")


def end_timer(timer_name, task_description):
    if timer_name in timers:
        end_time = time.time()
        elapsed_time = end_time - timers[timer_name]
        # if timer_name == "gesamt":
        #    logger.debug(f"[TIME] {task_description}: {elapsed_time:.2f} Sekunden", end="")
        if not (timer_name in ("caldav", "initial", "gesamt") and elapsed_time <= 10):
            logger.debug(f"[TIME] {task_description}: {elapsed_time:.2f} Sekunden")
        del timers[timer_name]  # Timer entfernen, wenn er fertig ist
    else:
        logger.error(f"Kein aktiver Timer mit dem Namen: {timer_name}")

def load_from_config(config_path, key):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            return config.get(key, [])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei: {e}")
        return []


def is_holiday_or_weekend(datum):
    if datum in de_holidays:
        # print(f"[DEBUG] {datum.strftime('%d.%m.%Y')} ist ein Feiertag: {de_holidays.get(datum)}")
        return True, de_holidays.get(datum)
    if datum.weekday() >= 5:
        # print(f"[DEBUG] {datum.strftime('%d.%m.%Y')} ist ein Wochenende")
        return True, "Samstag" if datum.weekday() == 5 else "Sonntag"
    # print(f"[DEBUG] {datum.strftime('%d.%m.%Y')} ist ein Werktag")
    return False, None


def parse_html_for_workplace_info_with_cache(html_file_path):
    global laufzettel_cache
    if html_file_path in laufzettel_cache:
        return laufzettel_cache[html_file_path]
    
    # Wenn nicht im Cache, parse die Datei und speichere das Ergebnis
    laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info(html_file_path)
    laufzettel_cache[html_file_path] = (laufzettel_werktags, laufzettel_we)
    # print(f"[DEBUG] Laufzettel-Cache aktualisiert für {html_file_path}")
    return laufzettel_werktags, laufzettel_we


def parse_html_for_workplace_info(html_file_path):  # Function to parse HTML and extract workplace, breaks, and tasks
    # start_timer("html")
    def extract_info(table):
        # Extrahiert die Informationen aus einer HTML-Tabelle und gibt eine Liste von Einträgen zurück.
        info = []
        if table:
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 5:
                    info.append({
                        'dienstname': cols[0].text.replace('\u00A0', ' ').strip(),
                        'dienstzeit': cols[1].text.replace('\u00A0', ' ').strip(),
                        'arbeitsplatz': cols[2].text.replace('\u00A0', ' ').strip(),
                        'pausenzeit': cols[3].text.replace('\u00A0', ' ').strip(),
                        'task': cols[4].text.replace('\u00A0', ' ').strip()
                    })
                    if len(info) > 40:
                        break
        return info

    # HTML parsen
    with open(html_file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')

    # Werktags- und Wochenend-Bereich extrahieren
    werktags_table = soup.find('table', summary='Laufzettel ab 01.01.23 Werktags')
    wochenende_table = soup.find('table', summary='Laufzettel ab 01.01.23 Wochenende')

    laufzettel_werktags = extract_info(werktags_table) if werktags_table else []
    laufzettel_we = extract_info(wochenende_table) if wochenende_table else []

    # Rückgabe der getrennten Listen
    # end_timer("html", "HTML auslesen")
    return laufzettel_werktags, laufzettel_we


def create_ical_event(full_title, start_datetime, end_datetime, description):
    try:
        now = datetime.datetime.now(pytz.timezone("Europe/Berlin"))
        location = (
            description if description else
            f"Eintrag: {full_title}, Alle Angaben und Inhalte sind ohne Gewähr. "
            f"Änderungsdatum: {datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')}"
        )
        # Normal events with time and timezone
        start_str = start_datetime.strftime('%Y%m%dT%H%M%S')
        end_str = end_datetime.strftime('%Y%m%dT%H%M%S') if end_datetime else start_str
        dtstart_str = f"DTSTART;TZID=Europe/Berlin:{start_str}"
        dtend_str = f"DTEND;TZID=Europe/Berlin:{end_str}"

        # Use the provided description or default to Dienst information
        description_str = (
            description if description else
            f"Eintrag: {full_title}, Alle Angaben und Inhalte sind ohne Gewähr. "
            f"Änderungsdatum: {datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')}"
        )
        # print(f"[DEBUG] Beschreibung: {description_str}")
        busy = "X-MICROSOFT-CDO-BUSYSTATUS:BUSY"
        transparent = "TRANSP:OPAQUE"
        sanitized_title = full_title.replace("\n", " ").replace("\r", "").strip()
        sanitized_desc = description_str.replace("\n", " ").replace("\r", "").strip()
        location = location.replace("\n", " ").replace("\r", "").strip()
        ical_event = f"""BEGIN:VCALENDAR
CALSCALE:GREGORIAN
VERSION:2.0
PRODID:-//meiersmarkus//NONSGML v1.0//DE
BEGIN:VEVENT
SUMMARY:{sanitized_title}
{transparent}
{dtstart_str}
{dtend_str}
DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}
UID:{now.timestamp()}@meiersmarkus.de
SEQUENCE:1
DESCRIPTION:{sanitized_desc}
LAST-MODIFIED:{now.strftime('%Y%m%dT%H%M%SZ')}
LOCATION:{location}
{busy}
END:VEVENT
BEGIN:VTIMEZONE
TZID:Europe/Berlin
BEGIN:DAYLIGHT
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
TZNAME:CEST
DTSTART:19700329T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
BEGIN:STANDARD
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
TZNAME:CET
DTSTART:19701025T030000
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
END:VTIMEZONE
END:VCALENDAR
"""
        # Termine in die Liste einfügen
        zeit = f" von {start_datetime.strftime('%H:%M')} - {end_datetime.strftime('%H:%M')} Uhr"
        eingetragene_termine.append(f"{start_datetime.strftime('%d.%m.%Y')}{zeit}: {full_title}")

        return ical_event
    except Exception as e:
        logger.error(f"[ERROR] Fehler beim Erstellen des Events: {e}")
        return None

# Funktion zur Verarbeitung eines zeitgebundenen Events
def process_timed_event(service_entry, date, name_without_brackets, laufzettel_werktags, laufzettel_we):
    # Define a timeout handler
    def timeout_handler(signum, frame):
        raise TimeoutError("The script execution timed out.")

    # Set the timeout duration (e.g., 300 seconds = 5 minutes)
    TIMEOUT_DURATION = 300

    # Register the timeout handler
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(TIMEOUT_DURATION)  # Start the timer

    try:
        # Extract start and end time from Excel entry
        time_match = re.match(r'(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})', service_entry)
        # logger.debug(f"[DEBUG] '{service_entry}' ist ein zeitgebundenes Event.")
        if time_match:
            start_time_str = time_match.group(1)
            end_time_str = time_match.group(2)

            start_datetime = datetime.datetime.strptime(f"{date.strftime('%Y-%m-%d')} {start_time_str}", '%Y-%m-%d %H:%M')
            end_datetime = datetime.datetime.strptime(f"{date.strftime('%Y-%m-%d')} {end_time_str}", '%Y-%m-%d %H:%M')

            if start_datetime.tzinfo is None:
                start_datetime = tz_berlin.localize(start_datetime)
            if end_datetime.tzinfo is None:
                end_datetime = tz_berlin.localize(end_datetime)
            if end_datetime < start_datetime:
                end_datetime += datetime.timedelta(days=1)

            # Get the basic title from the Excel entry (e.g., "Schnitt 2")
            title = service_entry[time_match.end():].strip()
            # Entferne "Info " und "(WT) " von dem Titel
            title = re.sub(r'\s*\(WT\)|\s*Info |\s+', '', title)
            # logger.debug(f"[DEBUG] '{title}' ist der Titel des Events.")
            # logger.debug(f"[DEBUG] Excel event: {title}, start: {start_time_str}, end: {end_time_str}")
            # logger.debug(f"[DEBUG] Excel event: {cleaned_service_entry}")
            if title == "Supervisor" and start_datetime.hour == 9 and start_datetime.minute == 30:
                full_title = f"{name_without_brackets}, {service_entry[time_match.end():].strip()} Büro"
            else:
                full_title = f"{name_without_brackets}, {service_entry[time_match.end():].strip()}"

            workplace = None
            is_holiday_flag, holiday_name = is_holiday_or_weekend(date)
            workplace_info = laufzettel_we if is_holiday_flag else laufzettel_werktags
            # print(f"[DEBUG] {workplace_info}")
            cleaned_service_entry = re.sub(r'\s*\(WT\)|\s*Info |\s+', '', service_entry[time_match.end():].strip())
            for info in workplace_info:
                dienstname = info['dienstname'].replace("Samstag: ", "").replace("Sonntag: ", "").replace(" ", "").strip()
                # print(f"[DEBUG] Vergleiche Excel '{cleaned_service_entry}' mit Laufzettel '{dienstname}'")
                if cleaned_service_entry.lower() in dienstname.lower():
                    # print(f"[DEBUG] {dienstname} gefunden.")
                    # DIENSTZEIT ist im HHMM-HHMM Format
                    # print(f"[DEBUG] Dienstzeit: {info['dienstzeit']}")
                    html_time_match = re.match(
                        r'(\d{4})\s*-\s*(\d{4})', info['dienstzeit']
                    )
                    if html_time_match:
                        html_start_time = f"{html_time_match.group(1)[:2]}:{html_time_match.group(1)[2:]}"
                        # print(f"[DEBUG] HTML Startzeit: {html_start_time}")
                        html_end_time = f"{html_time_match.group(2)[:2]}:{html_time_match.group(2)[2:]}"
                        # print(f"[DEBUG] HTML Endzeit: {html_end_time}")
                        # print(f"[DEBUG] Vergleich die Startzeiten zwischen Excel {start_time_str} und HTML {html_start_time}")
                        if html_start_time == start_time_str and html_end_time == end_time_str:
                            workplace = info.get('arbeitsplatz', None)
                            # print(f"[DEBUG] {cleaned_service_entry}, {workplace}")
                            break
            # print(f"[DEBUG] Datum: {start_datetime.date()}, Dienst: {title}, Workplace: {workplace}")
            # Wenn der Dienst "IngSchni" ist, setze den Arbeitsplatz auf das, was bei workplace nach dem letzten Leerzeichen steht
            if cleaned_service_entry == "IngSchni": workplace = workplace.split()[-1]

            # logger.debug(f"[DEBUG] Excel: '{full_title.strip()}' am '{start_datetime.date()}'.")
            # Now check if the event with the full title already exists
            existing_events = calendar.search(
                start=start_datetime.replace(hour=0, minute=0, second=0),
                end=end_datetime.replace(hour=23, minute=59, second=59),
                event=True
            )
            event_exists = False

            for event in existing_events:
                event.load()
                event_summary = event.vobject_instance.vevent.summary.value
                event_start = event.vobject_instance.vevent.dtstart.value
                event_end = (event.vobject_instance.vevent.dtend.value
                                if hasattr(event.vobject_instance.vevent, 'dtend')
                                else None)
                # logger.debug(f"[DEBUG] {len(existing_events)} Termine gefunden.")
                # Check if the beginnung of the event_summary is the name_without_brackets of the colleague
                if event_summary.startswith(name_without_brackets):
                    # logger.debug(f"[DEBUG] Event '{event_summary}' gehört zu '{name_without_brackets}'.")

                    # Ensure event_start and event_end are datetime objects, and localize if necessary
                    if isinstance(event_start, datetime.date) and not isinstance(event_start, datetime.datetime):
                        event_start = datetime.datetime.combine(event_start, datetime.time.min)
                    if isinstance(event_start, datetime.datetime) and event_start.tzinfo is None:
                        event_start = tz_berlin.localize(event_start)
                    if event_end and isinstance(event_end, datetime.date) and not isinstance(event_end, datetime.datetime):
                        event_end = datetime.datetime.combine(event_end, datetime.time.min)
                    if event_end and isinstance(event_end, datetime.datetime) and event_end.tzinfo is None:
                        event_end = tz_berlin.localize(event_end)

                    if rewrite:
                        if event_start.date() == start_datetime.date():
                            event.delete()
                            continue
                    # Compare the fully generated title with the existing event's summary
                    if (event_summary.strip() == full_title.replace("\n", " ").replace("\r", "").strip() and
                            event_start == start_datetime and
                            event_end == end_datetime):
                        event_exists = True
                        # logger.debug(f"[DEBUG] Event '{full_title}' already exists. Skipping creation.")
                        break
                    # logger.debug(f"[DEBUG] Excel: '{full_title.strip()}' am '{start_datetime.date()}'. "
                    #       f"Kalender: '{event_summary}' am '{event_start.date()}'.")
                    if event_start.date() == start_datetime.date():
                        logger.debug(f"[DEBUG] Anderer Termin: '{event_summary}' am {start_datetime.strftime('%d.%m.%Y')} wird gelöscht.")
                        event.delete()

            # If the event does not exist, create it with all the information collected
            if not event_exists:
                # Create the description by including the break time (if available) and the task
                description = f"Dienst: {title} von {name_without_brackets}, "
                if workplace:
                    description += f"Platz: {workplace}, "
                else:
                    description += "Platz: none, "
                last_modified = datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')
                description += "Alle Angaben und Inhalte sind ohne Gewähr. "
                description += f"Änderungsdatum: {last_modified}"

                # Create the iCal event with the full description
                # if is_holiday_flag:
                #    logger.debug(f"[DEBUG] {start_date.strftime('%a, %d.%m.%Y')} ist ein Feiertag oder Wochenende: {holiday_name}")
                ical_data = create_ical_event(
                    full_title, start_datetime, end_datetime, description=description
                )
                if ical_data:
                    calendar.add_event(ical_data)
                    logger.debug(f"[Dienst] {start_datetime.strftime('%d.%m.%Y')}, "
                            f"{start_datetime.strftime('%H:%M')} bis {end_datetime.strftime('%H:%M')}: {full_title}")
    finally:
        # Cancel the alarm if the script finishes before the timeout
        signal.alarm(0)

def process_excel_file(file_path, heute, schichten, laufzettel_werktags, laufzettel_we):
    global nextlaufzettel, current_laufzettel

    # Kompilierte Regex-Ausdrücke (einmalig)
    re_dot_to_colon = re.compile(r'(\b\d{2})\.(\d{2}\b)')
    re_time_range = re.compile(r'^\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\s*')
    re_cleanup = re.compile(r'\s*\(WT\)|\s*Info |\s+')
    re_time_spacing = re.compile(r'(\b\d{2}:\d{2})\s*-\s*(\d{2}:\d{2}\b)')
    re_name_brackets = re.compile(r'\s*[\r\n]*\(.*\)\s*[\r\n]*')

    # Excel laden
    df = pd.read_excel(file_path, header=None, engine='openpyxl')

    # Finde die Zeile mit "I" in Spalte 0
    identifier_row_index = df[df[0].astype(str).str.contains("I", na=False)].index[0]
    identifier_row = df.iloc[identifier_row_index]

    for day in range(1, 8):  # Spalten B bis H
        date = identifier_row[day]
        try:
            date = pd.to_datetime(date).date()
        except Exception:
            continue

        if date != heute and date != heute + timedelta(days=1):
            continue

        # logger.debug(f"[DEBUG] Verarbeite {date.strftime('%a, %d.%m.%Y')}")

        for row in range(identifier_row_index + 1, df.shape[0]):
            cell = df.iat[row, day]
            if pd.isna(cell):
                continue

            raw_entry = str(cell)
            service_entry = re_dot_to_colon.sub(r'\1:\2', raw_entry)
            schicht_key = re_cleanup.sub('', re_time_range.sub('', service_entry))

            if schicht_key not in schichten:
                continue

            # Sonderfall Projekt und Bereitschaft (setze Uhrzeit des Dienstes von 09:00 bis 09:01 Uhr)
            if "Projekt" in service_entry or "Bereitschaft" in service_entry:
                # Prüfe, ob Zeile kleiner als 86 ist
                if row >= 86:
                    continue
                # logger.debug(f"[DEBUG] Sonderfall Projekt oder Bereitschaft: {service_entry}")
                # Füge Uhrzeit an Anfang des service_entrys hinzu
                service_entry = f"09:00 - 09:01 {service_entry}"

            # Jetzt service_entry vollständig normalisieren
            service_entry = service_entry.replace('\n', ' ').replace('\r', ' ')
            service_entry = re.sub(r' {2,}', ' ', service_entry)  # Mehrfach-Leerzeichen reduzieren
            service_entry = re_time_spacing.sub(r'\1 - \2', service_entry)

            name = str(df.iat[row, 0])
            name_without_brackets = re_name_brackets.sub('', name)

            # Laufzettel-Wechsel prüfen
            if nextlaufzettel and (date >= nextlaufzettel.date() > current_laufzettel.date()):
                print(f"[INFO] Wechsel zu Laufzettel ab {nextlaufzettel.strftime('%d.%m.%Y')}")
                html_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                              f'Laufzettel_{nextlaufzettel.strftime("%Y%m%d")}.html')
                laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info_with_cache(html_file_path)
                current_laufzettel = nextlaufzettel
                getnextlaufzettel()

            # print(f"[DEBUG] Dienst: {service_entry} am {date.strftime('%d.%m.%Y')} für {name_without_brackets}")

            if re_time_spacing.search(service_entry):
                process_timed_event(service_entry, date, name_without_brackets, laufzettel_werktags, laufzettel_we)

    return


def load_all_laufzettel(folder_path):
    global laufzettel_data  # Zugriff auf die globale Variable
    html_files = [f for f in os.listdir(folder_path) if re.match(r'Laufzettel_\d{8}\.html', f)]
    html_files.sort()
    laufzettel_data = {}
    for html_file in html_files:
        file_path = os.path.join(folder_path, html_file)
        with open(file_path, 'r', encoding='utf-8') as file:
            laufzettel_data[html_file] = file.read()
    return


def initialize_laufzettel():
    global nextlaufzettel, current_laufzettel, laufzettel_data  # Zugriff auf die globalen Variablen
    today = date.today()
    
    # print(f"[DEBUG] Mehrere HTML-Dateien gefunden: {html_files}")
    # Sammle alle Laufzettel-Daten
    laufzettel_dates = []
    for html_file in laufzettel_data:
        try:
            laufzettel_datum = datetime.datetime.strptime(
                re.search(r'Laufzettel_(\d{8})\.html', html_file).group(1),
                "%Y%m%d"
            )
            laufzettel_dates.append(laufzettel_datum)
        except Exception as e:
            print(f"[ERROR] Fehler beim Parsen des Datums aus {html_file}: {e}")
            continue
    
    # Finde den aktuellen Laufzettel, indem du das erste Laufzettel-Datum suchst
    valid_current = [d for d in laufzettel_dates if d.date() <= today]
    if valid_current:
        current_laufzettel = min(valid_current)
        
        # Finde den nächsten Laufzettel (erster nach dem aktuellen)
        valid_next = [d for d in laufzettel_dates if d.date() > current_laufzettel.date()]
        if valid_next:
            nextlaufzettel = min(valid_next)
    
    if current_laufzettel:
        html_file_path = os.path.join(folder_path, f'Laufzettel_{current_laufzettel.strftime("%Y%m%d")}.html')
        # print(f"[DEBUG] Erster Laufzettel: {current_laufzettel.strftime('%d.%m.%Y')}")
        # if nextlaufzettel:
        #     print(f"[DEBUG] Nächster Laufzettel ab: {nextlaufzettel.strftime('%d.%m.%Y')}")
        laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info_with_cache(html_file_path)
        return laufzettel_werktags, laufzettel_we
    return None, None


def getnextlaufzettel():
    global nextlaufzettel, laufzettel_data  # Zugriff auf die globale Variable
    """Bestimmt den chronologisch nächsten verfügbaren Laufzettel."""
    if nextlaufzettel is None:
        print("[WARNING] Eingabe-Laufzettel ist None")
        return None

    # Sammle alle Laufzettel-Daten
    laufzettel_dates = []
    for html_file in laufzettel_data:
        try:
            laufzettel_datum = datetime.datetime.strptime(
                re.search(r'Laufzettel_(\d{8})\.html', html_file).group(1), 
                "%Y%m%d"
            )
            laufzettel_dates.append(laufzettel_datum)
        except Exception as e:
            print(f"[ERROR] Fehler beim Parsen des Datums aus {html_file}: {e}")
            continue

    # Sortiere die Daten chronologisch
    laufzettel_dates.sort()
    
    # Finde den nächsten Laufzettel nach dem aktuellen
    valid_next = [d for d in laufzettel_dates if d.date() > nextlaufzettel.date()]
    if valid_next:
        nextlaufzettel = min(valid_next)
        # print(f"[DEBUG] Gefunden: Nächster Laufzettel ab {next_date.strftime('%d.%m.%Y')}")
        return nextlaufzettel
    
    # print(f"[DEBUG] Kein weiterer Laufzettel nach {nextlaufzettel.strftime('%d.%m.%Y')} gefunden")
    return None

def extract_date(entry):
    # Suche nach dem Datum im Format TT.MM.JJJJ
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', entry)
    if match:
        date_str = match.group(1)
        # Datum in das Format JJJJMMTT umwandeln, um lexikografische Sortierung zu ermöglichen
        return date_str[6:] + date_str[3:5] + date_str[:2]  # Format: JJJJMMTT
    return None

def load_credentials(service_name, config_path):
    # Laden der JSON-Datei
    with open(config_path, 'r') as file:
        config = json.load(file)

    # Der Schlüssel in der config.json entspricht direkt dem service_name (z.B. "username_mm" oder "password_mm")
    if not "_" in service_name:
        if service_name in config:
            return config[service_name]  # Nur das Passwort zurückgeben
        else:
            raise ValueError(f"Config für '{service_name}' nicht gefunden.")
    else:
        # Allgemeiner Fall für andere Dienste
        username_key = f"username_{service_name}"
        password_key = f"password_{service_name}"

        # Überprüfen, ob der Benutzername und das Passwort existieren
        if username_key in config and password_key in config:
            username = config[username_key]
            password = config[password_key]
            return username, password
        else:
            raise ValueError(f"Benutzername oder Passwort für '{service_name}' nicht gefunden.")


def extract_date_from_filename(filename):
    # Regulärer Ausdruck für die Datumsangaben im Dateinamen
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4}) - (\d{1,2})\.(\d{1,2})\.(\d{2,4})', filename)
    if match:
        start_day = int(match.group(1))
        start_month = int(match.group(2))
        start_year = int(match.group(3))
        if start_year < 100:
            start_year += 2000
        start_date = dt.datetime(start_year, start_month, start_day)  # Verwende 'dt.datetime'
        return start_date
    else:
        return None  # Falls das Datum nicht gefunden wird

# Main
logger.debug(f"[INFO] Starte Gruppenkalenderaktualisierung VPA...")
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
tz_berlin = pytz.timezone('Europe/Berlin')
script_path = os.path.abspath(__file__)
folder_path = os.path.dirname(script_path)
# logger.debug(f"[DEBUG] folder_path: {folder_path}")
config_path = os.path.join(folder_path, 'config.json')
# logger.debug(f"[DEBUG] Config: {config_path}")
laufzettel_cache = {}
laufzettel_data = {}
# Globale Variablen für Laufzettel
nextlaufzettel = None
current_laufzettel = None

parser = argparse.ArgumentParser(description="Dienst zu Gruppenkalender VPA")
parser.add_argument("-r", "--rewrite", help="Alle vorhandenen Termine im Zeitbereich neu erstellen", action="store_true")

args = parser.parse_args()
rewrite = args.rewrite
service_name = "ard"

caldavlogin = "caldav" + service_name
caldav_start = load_credentials(caldavlogin, config_path)
calendar_name = 'Dienstplan VPA'
caldav_url = caldav_start + 'dienstplan-vpa/'

current_year = date.today().year
years = [current_year - 1, current_year, current_year + 1]
de_holidays = holidays.Germany(years=years, observed=False, prov="HH", language="de")
for year in years:
    de_holidays[date(year, 10, 31)] = "Reformationstag"  # Reformationstag
    de_holidays[date(year, 12, 24)] = "Heiligabend"  # Heiligabend
    de_holidays[date(year, 12, 31)] = "Silvester"    # Silvester
    de_holidays[easter(year)] = "Ostersonntag"       # Ostersonntag
    de_holidays[easter(year) + timedelta(days=49)] = "Pfingstsonntag"  # Pfingstsonntag
# for holiday_date, holiday_name in de_holidays.items():
#    print(f"{holiday_date.strftime('%d.%m.%Y')}: {holiday_name}")

# logger.debug(f"[DEBUG] Kalendername: {calendar_name}")
# logger.debug(f"[DEBUG] Kalender-URL: {caldav_url}")

feiertag, holiday_name = is_holiday_or_weekend(date.today())
if feiertag and time.localtime().tm_hour > 2:
    print(f"[INFO] {date.today().strftime('%d.%m.%Y')} ist {holiday_name}. Skript wird nicht ausgeführt.")
    sys.exit(0)

start_timer("caldav")
try:
    # logger.debug(f"[DEBUG] Verbinde mit CalDAV-Server '{service_name}'...")
    login_service = "login_" + service_name
    username, password = load_credentials(login_service, config_path)
    # logger.debug(f"[DEBUG] Username und Passwort geladen: {username}")
    client = DAVClient(caldav_start, username=username, password=password)
    principal = client.principal()
    try:
        calendar = principal.calendar(name=calendar_name)
    except Exception as e:
        logger.error(f"[ERROR] Kalender nicht gefunden: {e}")

    # Erfolgreiche Verbindung herstellen, falls Kalender gefunden oder erstellt wurde
    if not calendar:
        logger.error(f"[ERROR] Es konnte keine Verbindung zum Kalender '{calendar_name}' hergestellt werden.")

except Exception as e:
    logger.error(f"[ERROR] CalDAV-Verbindung fehlgeschlagen oder Fehler bei der Kalendererstellung: {e}")
    sys.exit(1)
end_timer("caldav", "Verbindung zu CalDAV")
eingetragene_termine = []
target_folder = os.path.join(folder_path, "Plaene", "MAZ_TAZ Dienstplan")
load_all_laufzettel(folder_path)
laufzettel_werktags, laufzettel_we = initialize_laufzettel()

end_timer("initial", "Initialisierung")

# Download der Dienstpläne mit dem Script DownloadDienste.py
try:
    result = subprocess.run(
        ["python", os.path.join(BASE_DIR, "DienstplanDownload.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30  # Timeout von 30 Sekunden
    )
    if result.stdout:
        logger.debug(result.stdout.decode().rstrip())
    if result.stderr:
        logger.debug(result.stderr.decode().rstrip())
except subprocess.TimeoutExpired:
    logger.error("Download-Skript hat das Zeitlimit überschritten.")
except Exception as e:
    logger.error(f"Fehler beim Ausführen des Download-Skripts: {e}")


xlsx_files = [
    os.path.join(root, f)
    for root, _, files in os.walk(target_folder)
    for f in files if f.endswith('.xlsx')
]
heute = date.today()

# Lösche alle Termine, die ein früheres Datum haben als gestern
start_date = heute - timedelta(days=4)
end_date = heute - timedelta(days=1)
# print(f"[DEBUG] Lösche alle Termine zwischen {start_date.strftime('%d.%m.%Y')} und {end_date.strftime('%d.%m.%Y')}")
events = calendar.search(start=start_date, end=end_date, event=True)
for event in events:
    vevent = event.icalendar_instance
    for component in vevent.walk('VEVENT'):
        summary = component.get('SUMMARY', 'Ohne Titel')
        dtstart = component.get('DTSTART')
        dtstart = dtstart.dt if dtstart else "Unbekanntes Datum"
deleted_count = sum(1 for event in events if not event.delete())
logger.debug(f"[DEBUG] {deleted_count} alte Termine wurden gelöscht.")


with_date, without_date = [], []
for file in xlsx_files:
    date = extract_date_from_filename(os.path.basename(file))
    (with_date if date else without_date).append((file, date) if date else file)
with_date.sort(key=lambda x: x[1])
xlsx_files = list(chain((f[0] for f in with_date), without_date))

ingestpath = os.path.join(folder_path, 'vpa.json')
schichten = load_from_config(ingestpath, "schichten")
schichten = [item for sublist in schichten for item in sublist]
# logger.debug(f"[DEBUG] Verfügbare Schichten: {schichten}")

if xlsx_files:
    for file_path in xlsx_files:
        file_name = os.path.basename(file_path)
        # logger.info(f"[INFO] Verarbeite Datei: {file_name}")
        process_excel_file(file_path, heute, schichten, laufzettel_werktags, laufzettel_we)
else:
    logger.debug("[DEBUG] Keine .xlsx-Dateien gefunden.")

if eingetragene_termine:
    logger.info(f"[INFO] {len(eingetragene_termine)} neue Termine eingetragen.")
end_timer("gesamt", "Zeit")
