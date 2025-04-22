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

# Logging-Konfiguration
log_formatter = logging.Formatter('%(message)s')
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DienstplanscriptGruppe.log")
log_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=3)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.DEBUG)

# Custom filter to exclude specific messages
class ExcludeCaldavFilter(logging.Filter):
    def filter(self, record):
        # Exclude messages containing "GET" or "HTTP/1.1"
        return not any(keyword in record.getMessage() for keyword in ["HTTP/11", "DEPRECATION NOTICE", "share.ard-zdf-box.de"])

# Add the filter to the log handler
log_handler.addFilter(ExcludeCaldavFilter())

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

def create_ical_event(full_title, start_datetime, end_datetime, description):
    try:
        now = datetime.datetime.now(pytz.timezone("Europe/Berlin"))
        location = ''
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
        busy = "X-MICROSOFT-CDO-BUSYSTATUS:BUSY"
        transparent = "TRANSP:OPAQUE"
        sanitized_title = full_title.replace("\n", " ").replace("\r", "").strip()
        sanitized_desc = description_str.replace("\n", " ").replace("\r", "").strip()
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
{location}
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
def process_timed_event(service_entry, date, name_without_brackets):
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
            title = re.sub(r'\s*\(WT\)|\s*Info ', '', title)
            # logger.debug(f"[DEBUG] '{title}' ist der Titel des Events.")
            # logger.debug(f"[DEBUG] Excel event: {title}, start: {start_time_str}, end: {end_time_str}")
            # logger.debug(f"[DEBUG] Excel event: {cleaned_service_entry}")
            full_title = f"{name_without_brackets}, {service_entry[time_match.end():].strip()}"

            # logger.debug(f"[DEBUG] Excel: '{full_title.strip()}' am '{start_datetime.date()}'.")
            # Now check if the event with the full title already exists
            existing_events = calendar.date_search(
                start=start_datetime.replace(hour=0, minute=0, second=0),
                end=end_datetime.replace(hour=23, minute=59, second=59)
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

def process_excel_file(file_path, heute, schichten):
    df = pd.read_excel(file_path, header=None, engine='openpyxl')
    # Finde die erste Zeile, die Datumsangaben enthält (z. B. mit einem "I" in Spalte 0)
    identifier_row_index = df[df[0].astype(str).str.contains("I", na=False)].index[0]
    identifier_row = df.iloc[identifier_row_index]
    # Versuch, den Namen flexibler zu finden
    for day in range(1, 8):  # Spalten B bis H (1 bis 7)
        date = identifier_row[day]
        try:
            date = pd.to_datetime(date).date()
        except Exception:
            continue
        if not (heute - timedelta(days=1) <= date <= heute + timedelta(days=1)):
            # logger.debug(f"[DEBUG] {start_date.strftime('%a, %d.%m.%Y')} ist außerhalb des Zeitrahmens.")
            continue
        # Durchsuche die Spalte unterhalb der Datumzeile
        for row in range(identifier_row_index + 1, df.shape[0]):
            service_entry = str(df.iat[row, day])  # Inhalt der aktuellen Zelle
            # Entferne den vorderen Teil von service_entry, wenn es dem Muster "HH:MM - HH:MM" entspricht
            service_entry = re.sub(r'(\b\d{2})\.(\d{2}\b)', r'\1:\2', service_entry)
            schicht =  re.sub(r'^\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\s*', '', service_entry)
            schicht = re.sub(r'\s*\(WT\)|\s*Info ', '', schicht)
            # Prüfe, ob einer der Schichtbegriffe (schichten) schicht entspricht.
            if schicht in schichten:
                # Überprüfung, ob die Zelle leer oder NaN ist
                if pd.isna(service_entry) or not isinstance(service_entry, str):
                    service_entry = "FT"
                else:
                    service_entry = service_entry.replace('\n', ' ') \
                                                .replace('\r', ' ') \
                                                .replace('    ', ' ') \
                                                .replace('   ', ' ') \
                                                .replace('  ', ' ')
                    service_entry = re.sub(
                        r'(\b\d{2}:\d{2})\s*-\s*(\d{2}:\d{2}\b)',
                        r'\1 - \2',
                        service_entry
                    )  # Vereinheitliche das Zeitformat auf "HH:MM - HH:MM" (mit oder ohne Leerzeichen um den Bindestrich)
                # logger.info(f"[INFO] {identifier_row[day].strftime('%a, %d.%m.%Y')}, {service_entry}")
                name = str(df.iat[row, 0])
                # name_cleaned = re.sub(r',\s*[A-Z]\.?$', '', name).strip()
                name_without_brackets = re.sub(r'\s*[\r\n]*\(.*\)\s*[\r\n]*', '', name)
                print(f"[DEBUG] '{name_without_brackets}' am {date.strftime('%d.%m.%Y')}: {service_entry}")
                # logger.debug(f"[DEBUG] Am {date} bei '{name_without_brackets}': {service_entry}")
                # Hier kannst du die Verarbeitung starten, z.B. weitergeben an eine Funktion

                if re.search(r'\b\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\b', service_entry):
                    # logger.debug(f"[DEBUG] '{service_entry}' ist ein zeitgebundenes Event.")
                    process_timed_event(service_entry, date, name_without_brackets)


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
logger.debug(f"[INFO] Starte Gruppenkalenderaktualisierung Ingest...")
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
tz_berlin = pytz.timezone('Europe/Berlin')
script_path = os.path.abspath(__file__)
folder_path = os.path.dirname(script_path)
# logger.debug(f"[DEBUG] folder_path: {folder_path}")
config_path = os.path.join(folder_path, 'config.json')
# logger.debug(f"[DEBUG] Config: {config_path}")

parser = argparse.ArgumentParser(description="Dienst zu Gruppenkalender Ingest")
parser.add_argument("-r", "--rewrite", help="Alle vorhandenen Termine im Zeitbereich neu erstellen", action="store_true")

args = parser.parse_args()
rewrite = args.rewrite
service_name = "ard"

caldavlogin = "caldav" + service_name
caldav_start = load_credentials(caldavlogin, config_path)
calendar_name = 'Dienstplan Ingest'
caldav_url = caldav_start + 'dienstplan-ingest/'

# logger.debug(f"[DEBUG] Kalendername: {calendar_name}")
# logger.debug(f"[DEBUG] Kalender-URL: {caldav_url}")

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
end_timer("initial", "Initialisierung")

xlsx_files = [
    os.path.join(root, f)
    for root, _, files in os.walk(target_folder)
    for f in files if f.endswith('.xlsx')
]
heute = date.today()

# Lösche alle Termine, die ein früheres Datum haben als gestern
start_date = heute - timedelta(days=7)
end_date = heute - timedelta(days=1)
events = calendar.date_search(start=start_date, end=end_date)
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

ingestpath = os.path.join(folder_path, 'ingest.json')
schichten = load_from_config(ingestpath, "schichten")
schichten = [item for sublist in schichten for item in sublist]
# logger.debug(f"[DEBUG] Verfügbare Schichten: {schichten}")

if xlsx_files:
    for file_path in xlsx_files:
        file_name = os.path.basename(file_path)
        # logger.info(f"[INFO] Verarbeite Datei: {file_name}")
        process_excel_file(file_path, heute, schichten)
else:
    logger.debug("[DEBUG] Keine .xlsx-Dateien gefunden.")

if eingetragene_termine:
    logger.info(f"[INFO] {len(eingetragene_termine)} neue Termine eingetragen.")
end_timer("gesamt", "Zeit")
