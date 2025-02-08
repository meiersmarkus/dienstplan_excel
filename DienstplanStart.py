import os
import subprocess
import sys
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json

# Logging-Konfiguration
log_formatter = logging.Formatter('%(message)s')
log_file = "Dienstplanscript.log"
log_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=3)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.DEBUG)

# Logger einrichten
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Ausgabe auf Konsole
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

# Timer-Funktionalität
timers = {}


def start_timer(timer_name):
    timers[timer_name] = time.time()


def end_timer(timer_name, task_description):
    if timer_name in timers:
        end_time = time.time()
        elapsed_time = end_time - timers[timer_name]
        minutes, seconds_remainder = divmod(elapsed_time, 60)
        formatted_time = f"{int(minutes)}:{int(seconds_remainder):02d}"
        logger.info(f"[TIME] {task_description}: {formatted_time} Min.")
        del timers[timer_name]
    else:
        logger.error(f"[ERROR] Kein aktiver Timer mit dem Namen: {timer_name}")


# Überprüfung und Installation fehlender Pakete
parser = argparse.ArgumentParser(description="Dienst zu ARD ZDF Box Script.")
parser.add_argument("-f", "--force", help="Herunterladen", action="store_true")
args = parser.parse_args()
force = args.force


def run_download_script():
    """Führt das Download-Skript aus und gibt den Rückgabewert zurück."""
    try:
        result = subprocess.run(
            ["python", "/volume1/CloudSync/ARD-ZDF-Box/ADienstplanCaldav/DienstplanDownload.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300  # Timeout von 5 Minuten
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
            ["python", "/volume1/CloudSync/ARD-ZDF-Box/ADienstplanCaldav/DienstzuARDZDFBox.py", name] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300  # Timeout von 5 Minuten
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
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            return config.get("colleagues", [])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei: {e}")
        return []


def update_calendars():
    """Startet die Kalenderaktualisierungen für alle Kollegen parallel."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'colleagues.json')
    colleagues = load_colleagues_from_config(config_path)

    with ThreadPoolExecutor() as executor:
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


def main():
    script_path = os.path.abspath(__file__)
    folder_path = os.path.dirname(script_path)
    original_latest_date = get_latest_modification_date(folder_path)
    if run_download_script() == 0 or force:
        logger.info("[DEBUG] Download erfolgreich, Kalender werden aktualisiert.")
        update_calendars()
    else:
        logger.info("[DEBUG] Keine Änderungen festgestellt.")


if __name__ == "__main__":
    start_timer("mega")
    main()
    end_timer("mega", "Gesamtdauer für alle")
