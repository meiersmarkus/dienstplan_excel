"""Download-Modul: Lädt Dienstplan-ZIP herunter und entpackt Excel-Dateien."""

import datetime
import logging
import os
import shutil
import time
import zipfile
from enum import Enum
from typing import Optional

import requests

from config import AppConfig

logger = logging.getLogger(__name__)


class DownloadResult(Enum):
    """Ergebnis des Download-Vorgangs."""
    NEW_DATA = "new_data"           # Neue/geänderte Dateien heruntergeladen
    NO_CHANGES = "no_changes"       # Keine Änderungen festgestellt
    CONNECTION_ERROR = "conn_error"  # Server nicht erreichbar
    ERROR = "error"                  # Sonstiger Fehler


def download_plans(app_config: AppConfig, folder_path: str, fast: bool = True) -> DownloadResult:
    """Lädt Dienstpläne herunter und entpackt sie.

    Args:
        app_config: Zentrale Konfiguration
        folder_path: Basisverzeichnis des Projekts
        fast: Wenn True, werden nur neue Dateien behalten (ältere gelöscht)

    Returns:
        DownloadResult mit dem Ergebnis-Status
    """
    plaene_dir = os.path.join(folder_path, "Plaene")

    # Letztes Änderungsdatum vor dem Download merken
    original_latest = _get_latest_xlsx_date(plaene_dir)

    # Server-Erreichbarkeit prüfen
    if not _check_server(app_config.server_check_url):
        logger.warning("Server nicht erreichbar. Download nicht möglich.")
        return DownloadResult.CONNECTION_ERROR

    try:
        # Verzeichnis vorbereiten
        shutil.rmtree(plaene_dir, ignore_errors=True)
        os.makedirs(plaene_dir, exist_ok=True)

        # ZIP herunterladen
        zip_path = os.path.join(folder_path, "Plaene.zip")
        response = requests.get(app_config.download_url, timeout=60)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            f.write(response.content)

        # Entpacken: nur .xlsx, Metadaten beibehalten
        _extract_xlsx_from_zip(zip_path, plaene_dir)

        # ZIP aufräumen
        os.remove(zip_path)

        # Test-Dateien löschen
        _delete_test_files(plaene_dir)

        # Neues Änderungsdatum nach Download
        new_latest = _get_latest_xlsx_date(plaene_dir)

        # Vergleich
        has_changes = (original_latest is None or new_latest is None or original_latest != new_latest)

        # Im Fast-Modus: alte Dateien löschen
        if fast and original_latest and not has_changes:
            deleted = _delete_old_files(original_latest, plaene_dir)
            logger.debug("Fast-Modus: %d alte Dateien entfernt (keine Änderungen).", deleted)
        elif fast and original_latest and has_changes:
            deleted = _delete_old_files(original_latest, plaene_dir)
            logger.debug("Fast-Modus: %d alte Dateien entfernt (mit Änderungen).", deleted)

        if has_changes:
            logger.info("Neue Dienstpläne heruntergeladen.")
            return DownloadResult.NEW_DATA
        else:
            logger.debug("Keine Änderungen festgestellt.")
            return DownloadResult.NO_CHANGES

    except requests.RequestException as e:
        logger.error("Download-Fehler: %s", e)
        return DownloadResult.ERROR
    except Exception as e:
        logger.error("Unerwarteter Fehler beim Download: %s", e)
        return DownloadResult.ERROR


# ---------------------------------------------------------------------------
# Interne Hilfsfunktionen
# ---------------------------------------------------------------------------

def _check_server(url: str, timeout: int = 5) -> bool:
    """Prüft ob der Server erreichbar ist."""
    try:
        response = requests.head(url, timeout=timeout)
        return response.status_code == 200
    except requests.RequestException:
        return False


def _get_latest_xlsx_date(folder_path: str) -> Optional[datetime.datetime]:
    """Findet das neueste Änderungsdatum aller .xlsx-Dateien."""
    latest = 0.0
    if not os.path.exists(folder_path):
        return None
    for root, _, files in os.walk(folder_path):
        for fname in files:
            if fname.endswith(".xlsx"):
                mod_time = os.path.getmtime(os.path.join(root, fname))
                if mod_time > latest:
                    latest = mod_time
    return datetime.datetime.fromtimestamp(latest) if latest > 0 else None


def _extract_xlsx_from_zip(zip_path: str, target_dir: str):
    """Entpackt .xlsx-Dateien aus einem ZIP-Archiv mit beibehaltenen Zeitstempeln."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.filename.endswith(".xlsx"):
                zf.extract(info, target_dir)
                extracted = os.path.join(target_dir, info.filename)
                # Original-Änderungsdatum setzen
                mod_time = time.mktime(info.date_time + (0, 0, -1))
                os.utime(extracted, (mod_time, mod_time))


def _delete_test_files(folder_path: str):
    """Löscht Dateien mit 'test' im Namen."""
    for root, _, files in os.walk(folder_path):
        for fname in files:
            if "test" in fname.lower():
                path = os.path.join(root, fname)
                try:
                    os.remove(path)
                    logger.debug("Test-Datei gelöscht: %s", fname)
                except OSError as e:
                    logger.warning("Konnte %s nicht löschen: %s", fname, e)


def _delete_old_files(cutoff: datetime.datetime, folder_path: str) -> int:
    """Löscht .xlsx-Dateien, die älter als cutoff sind. Gibt Anzahl zurück."""
    deleted = 0
    cutoff_ts = cutoff.timestamp()
    for root, _, files in os.walk(folder_path):
        for fname in files:
            if fname.endswith(".xlsx"):
                fpath = os.path.join(root, fname)
                if os.path.getmtime(fpath) < cutoff_ts:
                    try:
                        os.remove(fpath)
                        deleted += 1
                    except OSError as e:
                        logger.warning("Konnte %s nicht löschen: %s", fname, e)
    return deleted
