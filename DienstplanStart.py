import os
import math
import subprocess
import sys
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import holidays
from dateutil.easter import easter
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.reader.drawings")

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass # Ältere Python-Versionen ignorieren

# Basisverzeichnis des Skripts
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = "/share/LOGS"

# Prüfen, ob LOG_DIR existiert, sonst BASE_DIR nehmen
if os.path.isdir(LOG_DIR):
    log_file = os.path.join(LOG_DIR, "Dienstplanscript.log")
else:
    log_file = os.path.join(BASE_DIR, "Dienstplanscript.log")

# Logging-Konfiguration
log_formatter = logging.Formatter('%(message)s')
log_handler = RotatingFileHandler(log_file, maxBytes=5120 * 1024, backupCount=3, encoding="utf-8")
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.DEBUG)

# Logger einrichten
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Filter für caldav-Warnungen hinzufügen
class CaldavWarningFilter(logging.Filter):
    def filter(self, record):
        if record.name == "caldav" and "Deviation from expectations found: Unexpected content type: text/html; charset=UTF-8" in record.getMessage():
            return False
        return True

logger.addFilter(CaldavWarningFilter())

# Ausgabe auf Konsole
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

# caldav-Logger auf ERROR setzen, um WARNINGS zu unterdrücken (optional)
logging.getLogger("caldav").setLevel(logging.ERROR)

# Timer-Funktionalität
timers = {}


def start_timer(timer_name):
    timers[timer_name] = time.time()


def end_timer(timer_name, task_description):
    if timer_name in timers:
        end_time = time.time()
        elapsed_time = end_time - timers[timer_name]
        if elapsed_time > 20:
            minutes, seconds_remainder = divmod(elapsed_time, 60)
            formatted_time = f"{int(minutes)}:{int(seconds_remainder):02d}"
            logger.info(f"[TIME] {task_description}: {formatted_time} Min.")
            del timers[timer_name]
    else:
        logger.error(f"[ERROR] Kein aktiver Timer mit dem Namen: {timer_name}")

def is_holiday_or_weekend(datum):
    if datum in de_holidays:
        return True, de_holidays.get(datum)
    if datum.weekday() >= 5:
        return True, "Samstag" if datum.weekday() == 5 else "Sonntag"
    return False, None

# Überprüfung und Installation fehlender Pakete
parser = argparse.ArgumentParser(description="Dienst zu ARD ZDF Box Script.")
parser.add_argument("-f", "--force", help="Erzwingen", action="store_true")
parser.add_argument("-n", "--nodownload", help="Kein Dienstpläne hHerunterladen, direkt starten", action="store_true")
parser.add_argument("-d", "--delete", help="Alte Termine löschen", action="store_true")
args = parser.parse_args()
force = args.force
nodownload = args.nodownload
delete = args.delete

current_year = datetime.today().year
years = [current_year - 1, current_year, current_year + 1]
de_holidays = holidays.Germany(years=years, observed=False, prov="HH", language="de")
for year in years:
    de_holidays[datetime(year, 10, 31)] = "Reformationstag"  # Reformationstag
    de_holidays[datetime(year, 12, 24)] = "Heiligabend"  # Heiligabend
    de_holidays[datetime(year, 12, 31)] = "Silvester"    # Silvester
    de_holidays[easter(year)] = "Ostersonntag"       # Ostersonntag
    de_holidays[easter(year) + timedelta(days=49)] = "Pfingstsonntag"  # Pfingstsonntag
# for holiday_date, holiday_name in de_holidays.items():
#    print(f"{holiday_date.strftime('%d.%m.%Y')}: {holiday_name}")

feiertag, holiday_name = is_holiday_or_weekend(datetime.today())
#if feiertag and time.localtime().tm_hour > 8:
    #logger.info(f"[INFO] {datetime.today().strftime('%d.%m.%Y')} ist {holiday_name}. Skript wird nicht ausgeführt.")
    #sys.exit(0)

def run_download_script():
    """Führt das Download-Skript aus und gibt den Rückgabewert zurück."""
    try:
        if force:
            result = subprocess.run(
                ["python", os.path.join(BASE_DIR, "DienstplanDownload.py")],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30  # Timeout von 30 Sekunden
            )
        else:
            result = subprocess.run(
                ["python", os.path.join(BASE_DIR, "DienstplanDownload.py"), "-f"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30  # Timeout von 30 Sekunden
            )
        if result.stdout:
            logger.debug(result.stdout.decode().rstrip())
        if result.stderr:
            logger.debug(result.stderr.decode().rstrip())
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error("Download-Skript hat das Zeitlimit überschritten.")
        return -1
    except Exception as e:
        logger.error(f"Fehler beim Ausführen des Download-Skripts: {e}")
        return -1


def update_calendar(name, args):
    """Aktualisiert den Kalender für einen einzelnen Kollegen."""
    try:
        result = subprocess.run(
            ["python", os.path.join(BASE_DIR, "DienstzuARDZDFBox.py"), name] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=240  # Timeout von 4 Minuten
        )
        if result.stdout:
            output_lines = result.stdout.decode().splitlines()
#            if len(output_lines) <= 2:
#                logger.debug(f"[INFO] {name} aktualisiert.")
            if len(output_lines) > 2:
                logger.debug("\n".join(output_lines))
        if result.stderr:
            logger.debug(result.stderr.decode().rstrip())
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error(f"Kalenderaktualisierung für {name} hat das Zeitlimit überschritten.")
        return -1
    except Exception as e:
        logger.error(f"Fehler bei der Aktualisierung des Kalenders für {name}: {e}")
        return -1


def load_colleagues_from_config(config_path):
    """Lädt die Kollegen aus einer Konfigurationsdatei."""
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
            return config.get("colleagues", [])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei: {e}")
        return []


def update_calendars():
    """Startet die Kalenderaktualisierungen für alle Kollegen parallel."""
    config_path = os.path.join(BASE_DIR, 'colleagues.json')
    colleagues = load_colleagues_from_config(config_path)
    
    cpu_count = os.cpu_count() or 1
    workers = max(1, math.floor(cpu_count * 0.75))
    logger.info(f"Gefundene CPUs: {cpu_count}, benutze {workers} Executor-Threads")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(update_calendar, name, args) for name, args in colleagues]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Fehler bei der Kalenderaktualisierung: {e}")


def get_latest_modification_date(folder_path):
    """Findet das neueste Änderungsdatum aller .xlsx-Dateien im angegebenen Verzeichnis."""
    latest_mod_time = 0
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".xlsx"):
                file_path = os.path.join(root, file)
                mod_time = os.path.getmtime(file_path)
                if mod_time > latest_mod_time:
                    latest_mod_time = mod_time
    return datetime.fromtimestamp(latest_mod_time) if latest_mod_time > 0 else None


def deleteoldentries():
    """Löscht alte Einträge für alle Kollegen."""
    config_path = os.path.join(BASE_DIR, 'colleagues.json')
    whitelist_path = os.path.join(BASE_DIR, 'deletewhitelist.json')
    colleagues = load_colleagues_from_config(config_path)
    whitelist = load_colleagues_from_config(whitelist_path)

    # Erstellen einer Liste der Namen in der Whitelist
    whitelist_names = [colleague[0] for colleague in whitelist]
    # Filtern der Kollegen, die nicht in der Whitelist sind
    filtered_colleagues = [colleague for colleague in colleagues if colleague[0] not in whitelist_names]
    # print("Ursprüngliche Kollegen:", colleagues)
    # print("Whitelist:", whitelist_names)
    # print("Gefilterte Kollegen:", filtered_colleagues)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(delete_events, name, args) for name, args in filtered_colleagues]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Fehler bei der Kalenderaktualisierung: {e}")


def delete_events(name, args):
    """Löscht alte Enträge für einen einzelnen Kollegen."""
    try:
        result = subprocess.run(
            ["python", os.path.join(BASE_DIR, "Diensteloeschen.py"), name] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=240  # Timeout von 4 Minuten
        )
        if result.stdout:
            output_lines = result.stdout.decode().splitlines()
#            if len(output_lines) <= 2:
#                logger.debug(f"[INFO] {name} aktualisiert.")
            if len(output_lines) > 2:
                logger.debug("\n".join(output_lines))
        if result.stderr:
            logger.debug(result.stderr.decode().rstrip())
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error(f"Kalenderaktualisierung für {name} hat das Zeitlimit überschritten.")
        return -1
    except Exception as e:
        logger.error(f"Fehler bei der Aktualisierung des Kalenders für {name}: {e}")
        return -1


def main():
    if delete:
        logger.debug("[DEBUG] Lösche alte Einträge...")
        deleteoldentries()
        return
    if nodownload:
        logger.debug("[DEBUG] Direkter Start ohne Download...")
        update_calendars()
        return
    if run_download_script() == 0 or force:
        logger.debug("[DEBUG] Download erfolgreich, Kalender werden aktualisiert.")
        update_calendars()
    else:
        logger.debug("[DEBUG] Keine Änderungen festgestellt.")


if __name__ == "__main__":
    start_timer("mega")
    main()
    end_timer("mega", "Gesamtdauer für alle")
