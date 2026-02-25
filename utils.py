"""Hilfsfunktionen: Logging, Timer, Datums-Parsing."""

import logging
import os
import sys
import time
import datetime
import re
from logging.handlers import RotatingFileHandler
from typing import Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(base_dir: str, level: int = logging.DEBUG,
                   console_level: int = logging.INFO) -> logging.Logger:
    """Richtet das Logging ein (Datei + Konsole). Gibt den Root-Logger zurück.
    
    Die Logdatei bekommt alles (DEBUG), die Konsole nur INFO+.
    """
    log_dir = "/share/LOGS"
    if not os.path.isdir(log_dir):
        log_dir = base_dir

    log_path = os.path.join(log_dir, "Dienstplanscript.log")

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_handler.setLevel(console_level)

    root = logging.getLogger()
    root.setLevel(level)
    # Vorherige Handler entfernen, falls setup_logging mehrfach aufgerufen wird
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # Geschwätzige Third-Party-Logger ruhigstellen
    # caldav sendet "Deviation from expectations" als WARNING, daher ERROR nötig
    for noisy in ("caldav", "urllib3", "requests", "PIL", "PIL.PngImagePlugin",
                   "PIL.Image", "httpcore", "httpx", "chardet", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.ERROR)

    return root


# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------

class Timer:
    """Einfacher Kontext-Manager-Timer für Performance-Messungen.

    Verwendung:
        with Timer("CalDAV-Verbindung") as t:
            do_stuff()
        # Loggt automatisch die Dauer
    """

    def __init__(self, label: str, log_threshold_seconds: float = 0.0):
        self.label = label
        self.threshold = log_threshold_seconds
        self.start_time: float = 0
        self.elapsed: float = 0
        self._logger = logging.getLogger("timer")

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self.start_time
        if self.elapsed >= self.threshold:
            if self.elapsed > 60:
                minutes, secs = divmod(self.elapsed, 60)
                self._logger.info("[TIME] %s: %d:%02d Min.", self.label, int(minutes), int(secs))
            else:
                self._logger.info("[TIME] %s: %.2f Sek.", self.label, self.elapsed)


# ---------------------------------------------------------------------------
# Datums-Parsing
# ---------------------------------------------------------------------------

_DATE_FORMATS = ("%Y-%m-%d", "%d.%m.%Y", "%Y%m%d", "%Y-%m-%d %H:%M:%S")


def to_datetime(value) -> Optional[datetime.datetime]:
    """Konvertiert diverse Datumsformate in datetime.datetime.

    Akzeptiert: datetime.datetime, datetime.date, str (diverse Formate), None.
    Gibt None zurück wenn value None/leer ist.
    Wirft ValueError bei unbekannten Formaten.
    """
    if value is None:
        return None

    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)

    if isinstance(value, (int, float)):
        # Excel-Seriennummern oder Timestamps
        return None

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        for fmt in _DATE_FORMATS:
            try:
                return datetime.datetime.strptime(value, fmt)
            except ValueError:
                continue

    raise ValueError(f"Unbekanntes Datumsformat: {value!r} ({type(value).__name__})")


def extract_date_from_filename(filename: str) -> Optional[datetime.datetime]:
    """Extrahiert das Startdatum aus einem Dateinamen wie 'Dienstplan_04.10.25_-_10.10.25.xlsx'."""
    # Format: D.M.YY oder DD.MM.YYYY, getrennt durch " - " oder "_-_"
    match = re.search(
        r"(\d{1,2})[._](\d{1,2})[._](\d{2,4})\s*[-_]+\s*(\d{1,2})[._](\d{1,2})[._](\d{2,4})",
        filename,
    )
    if not match:
        return None
    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    if year < 100:
        year += 2000
    try:
        return datetime.datetime(year, month, day)
    except ValueError:
        return None


def normalize_string(s: str) -> str:
    """Reduziert Whitespace auf einzelne Leerzeichen und trimmt."""
    return re.sub(r"\s+", " ", s.strip())
