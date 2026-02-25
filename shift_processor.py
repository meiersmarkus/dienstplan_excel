"""Kernlogik: Verarbeitet Dienstpläne für einen einzelnen Kollegen.

Ersetzt die Hauptlogik aus DienstzuARDZDFBox.py. Keine globalen Variablen,
keine Seiteneffekte außer CalDAV-Operationen.
"""

import datetime
import logging
import re
from typing import List, Optional, Tuple

import pytz

from calendar_client import CalendarClient, get_event_details
from config import AppConfig, ColleagueConfig
from event_builder import (
    ABSENCE_TYPES,
    build_event_description,
    build_ical_event,
    format_event_log,
)
from excel_parser import ShiftEntry, get_sorted_excel_files, parse_excel_file
from holidays_de import GermanHolidays
from laufzettel import LaufzettelManager, ShiftInfo
from notifier import build_night_shift_summary, send_notification
from utils import Timer

logger = logging.getLogger(__name__)

TZ_BERLIN = pytz.timezone("Europe/Berlin")
LOCATION_ADDRESS = r"Hugh-Greene-Weg 1\, 22529 Hamburg"


def process_colleague(
    app_config: AppConfig,
    colleague: ColleagueConfig,
    laufzettel_mgr: LaufzettelManager,
    holidays: GermanHolidays,
    plans_folder: str,
):
    """Verarbeitet alle Dienstpläne für einen einzelnen Kollegen.

    Dies ist die Hauptfunktion, die pro Kollege aufgerufen wird (ggf. parallel).
    Sie hat keine globalen Abhängigkeiten.

    Args:
        app_config: Zentrale Konfiguration
        colleague: Konfiguration dieses Kollegen
        laufzettel_mgr: Geteilter Laufzettel-Manager (thread-safe für Lesezugriffe)
        holidays: Geteilte Feiertags-Instanz (thread-safe, read-only)
        plans_folder: Ordner mit den Excel-Dateien
    """
    name = colleague.name

    # 1. CalDAV-Verbindung aufbauen
    with Timer(f"CalDAV {name}", log_threshold_seconds=5):
        client = CalendarClient(app_config, colleague)
        if not client.connect():
            logger.error("Kalender für %s nicht erreichbar – überspringe.", name)
            return

    # 2. Cache laden (aktuelles Jahr bis +90 Tage)
    current_year = datetime.date.today().year
    cache_start = datetime.datetime(current_year, 1, 1, 0, 0)
    cache_end = datetime.datetime.now() + datetime.timedelta(days=90)
    client.load_cache(cache_start, cache_end)

    # 3. Excel-Dateien verarbeiten
    xlsx_files = get_sorted_excel_files(plans_folder)
    if not xlsx_files:
        logger.debug("%s: Keine Excel-Dateien gefunden.", name)
        return

    new_entries: List[str] = []   # Für E-Mail-Benachrichtigung
    night_shift_count = 0
    night_shift_counting_started = False

    for file_path in xlsx_files:
        entries, user_found = parse_excel_file(file_path, name)

        if not user_found and not colleague.only_shifts:
            # Benutzer nicht im Plan → vorhandene Termine für diese Woche löschen
            _delete_entries_for_dates(client, entries)
            continue

        if not user_found:
            continue  # Im only_shifts-Modus nichts löschen

        for entry in entries:
            if entry.is_timed:
                result = _process_timed_entry(
                    client=client,
                    entry=entry,
                    colleague=colleague,
                    app_config=app_config,
                    laufzettel_mgr=laufzettel_mgr,
                    holidays=holidays,
                    night_shift_count=night_shift_count,
                    counting_started=night_shift_counting_started,
                )
                if result:
                    log_text, night_shift_count, night_shift_counting_started = result
                    if log_text:
                        new_entries.append(log_text)
            else:
                log_text = _process_allday_entry(
                    client=client,
                    entry=entry,
                    colleague=colleague,
                    app_config=app_config,
                )
                if log_text:
                    new_entries.append(log_text)

    # 4. Benachrichtigung senden
    if new_entries:
        logger.info("%s: %d neue Termine eingetragen.", name, len(new_entries))
        if colleague.send_notification:
            night_summary = _get_night_shift_summary(client, current_year)
            send_notification(app_config, name, new_entries, night_summary)
    """else:
        logger.debug("%s: Keine neuen Termine.", name)"""


# ---------------------------------------------------------------------------
# Zeitgebundene Dienste (z.B. "09:00 - 17:00 OMSchni 3")
# ---------------------------------------------------------------------------

def _process_timed_entry(
    client: CalendarClient,
    entry: ShiftEntry,
    colleague: ColleagueConfig,
    app_config: AppConfig,
    laufzettel_mgr: LaufzettelManager,
    holidays: GermanHolidays,
    night_shift_count: int,
    counting_started: bool,
) -> Optional[Tuple[Optional[str], int, bool]]:
    """Verarbeitet einen zeitgebundenen Dienst.

    Returns:
        (log_text_or_None, updated_night_shift_count, counting_started)
        oder None bei Fehler
    """
    try:
        start_dt = datetime.datetime.strptime(
            f"{entry.date.strftime('%Y-%m-%d')} {entry.start_time}", "%Y-%m-%d %H:%M"
        )
        end_dt = datetime.datetime.strptime(
            f"{entry.date.strftime('%Y-%m-%d')} {entry.end_time}", "%Y-%m-%d %H:%M"
        )

        # Sonderbehandlung: User1 bekommt Nachtschichten bis 23:59 gekürzt
        if colleague.name == app_config.user1_name and end_dt.time() < datetime.time(8, 0):
            end_dt = TZ_BERLIN.localize(
                datetime.datetime.combine(end_dt.date(), datetime.time(23, 59))
            )
        elif end_dt < start_dt:
            end_dt += datetime.timedelta(days=1)

        # Zeitzonen setzen
        if start_dt.tzinfo is None:
            start_dt = TZ_BERLIN.localize(start_dt)
        if end_dt.tzinfo is None:
            end_dt = TZ_BERLIN.localize(end_dt, is_dst=None)

        # Laufzettel-Info holen
        is_holiday, _ = holidays.is_holiday_or_weekend(entry.date.date())
        werktags, wochenende = laufzettel_mgr.get_for_date(entry.date.date())
        shift_infos = wochenende if is_holiday else werktags

        workplace, break_time, task = _match_laufzettel(
            entry.shift_name, entry.start_time, entry.end_time, shift_infos
        )

        # Titel zusammenbauen
        if colleague.name == app_config.user1_name:
            full_title = f"{entry.start_time}-{entry.end_time} {entry.shift_name}"
        else:
            full_title = (
                f"{entry.shift_name}, {workplace}"
                if workplace and workplace not in entry.shift_name
                else entry.shift_name
            )

        # Nachtschicht-Zählung (Dienste ab 20:00)
        if start_dt.time() >= datetime.time(20, 0):
            if not counting_started or entry.date.month in (1, 2, 3, 4):
                night_shift_count = _count_night_shifts_from_cache(client, entry.date)
                counting_started = True
            night_shift_count += 1
            full_title += f" ({night_shift_count})"

        # Duplikat-Check & Konflikt-Lösung
        if _should_skip_entry(full_title, colleague, app_config):
            return None, night_shift_count, counting_started

        existing = client.get_events_on_date(start_dt.date())
        event_exists = False

        for event in existing:
            try:
                summary, evt_date, evt_start, evt_end = get_event_details(event)
            except Exception:
                continue

            if _should_delete_event(summary, colleague, app_config):
                client.delete_event(event)
                continue

            # Timezone-Normalisierung für Vergleich
            if evt_start and evt_start.tzinfo is None:
                evt_start = TZ_BERLIN.localize(evt_start)
            if evt_end and evt_end.tzinfo is None:
                evt_end = TZ_BERLIN.localize(evt_end)

            if colleague.rewrite and evt_date == start_dt.date():
                client.delete_event(event)
                continue

            # Exakter Match → überspringen
            safe_title = full_title.replace("\n", " ").replace("\r", "").strip()
            safe_summary = summary.replace("\n", " ").replace("\r", "").strip()
            if safe_summary == safe_title and evt_start == start_dt and evt_end == end_dt:
                event_exists = True
                break

            # Gleicher Tag, aber anderer Inhalt → löschen
            if evt_date == start_dt.date():
                logger.debug(
                    "%s: Lösche '%s' am %s, weil ungleich '%s'.",
                    colleague.name, summary, evt_date.strftime("%d.%m.%Y"), full_title,
                )
                client.delete_event(event)

        if event_exists:
            return None, night_shift_count, counting_started

        # Neues Event erstellen
        if not workplace:
            logger.debug("Keinen Platz für '%s' am %s", entry.shift_name, start_dt.strftime("%d.%m.%Y"))

        description = build_event_description(entry.shift_name, workplace, break_time, task)
        location = LOCATION_ADDRESS if colleague.add_location else None

        ical_data = build_ical_event(
            title=full_title,
            start=start_dt,
            end=end_dt,
            description=description,
            location=location,
        )
        if client.add_event(ical_data):
            log_text = format_event_log(start_dt, full_title, end_dt)
            logger.info("[Dienst] %s: %s", colleague.name, log_text)
            return log_text, night_shift_count, counting_started

        return None, night_shift_count, counting_started

    except Exception as e:
        logger.error("Fehler bei %s, %s: %s", colleague.name, entry.raw_text, e)
        return None, night_shift_count, counting_started


# ---------------------------------------------------------------------------
# Ganztägige Einträge (FT, UR, NV, KD, KR, etc.)
# ---------------------------------------------------------------------------

def _process_allday_entry(
    client: CalendarClient,
    entry: ShiftEntry,
    colleague: ColleagueConfig,
    app_config: AppConfig,
) -> Optional[str]:
    title = entry.raw_text.strip() or "Ganztägiger Termin"
    start_dt = entry.date

    if _should_skip_entry(title, colleague, app_config):
        return None

    try:
        existing = client.get_events_on_date(start_dt.date())
        event_exists = False

        for event in existing:
            try:
                summary, evt_date, evt_start, evt_end = get_event_details(event)
            except Exception:
                continue

            if _should_delete_event(summary, colleague, app_config):
                client.delete_event(event)
                continue

            evt_date_cmp = evt_start.date() if isinstance(evt_start, datetime.datetime) else evt_date

            if colleague.rewrite and evt_date_cmp == start_dt.date():
                client.delete_event(event)
                continue

            safe_title = title.replace("\n", " ").replace("\r", "").strip()
            safe_summary = summary.replace("\n", " ").replace("\r", "").strip()
            if safe_summary == safe_title and evt_date_cmp == start_dt.date():
                event_exists = True
                break

            if evt_date_cmp == start_dt.date():
                client.delete_event(event)

        if not event_exists:
            description = build_event_description(title)
            ical_data = build_ical_event(
                title=title, 
                start=start_dt, 
                all_day=True, 
                description=description
            )
            if client.add_event(ical_data):
                log_text = format_event_log(start_dt, title)
                logger.info("[Dienst] %s: %s", colleague.name, log_text)
                return log_text

    except Exception as e:
        logger.error("Fehler bei ganztägigem Event '%s' am %s: %s", title, start_dt, e)

    return None


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _delete_entries_for_dates(client: CalendarClient, entries: List[ShiftEntry]):
    """Löscht vorhandene Kalendereinträge für die Daten der übergebenen Einträge."""
    for entry in entries:
        existing = client.get_events_on_date(entry.date.date())
        for event in existing:
            try:
                summary, evt_date, _, _ = get_event_details(event)
                logger.debug("Lösche '%s' vom %s (Nutzer nicht im Plan).", summary, evt_date)
                client.delete_event(event)
            except Exception as e:
                logger.warning("Fehler beim Löschen: %s", e)


def _match_laufzettel(
    shift_name: str,
    start_time: str,
    end_time: str,
    shift_infos: List[ShiftInfo],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Findet den passenden Laufzettel-Eintrag für eine Schicht.

    Returns:
        (arbeitsplatz, pausenzeit, task) oder (None, None, None)
    """
    # Bereinigung: "(WT)", "Info " und Leerzeichen entfernen für den Vergleich
    cleaned = re.sub(r"\s*\(WT\)|\s*Info |\s+", "", shift_name)

    for info in shift_infos:
        dienstname = (
            info.dienstname
            .replace("Samstag: ", "")
            .replace("Sonntag: ", "")
            .replace(" ", "")
            .strip()
        )

        if cleaned.lower() not in dienstname.lower():
            continue

        # Zeitvergleich: HHMM-HHMM aus Laufzettel
        time_match = re.match(r"(\d{4})\s*-\s*(\d{4})", info.dienstzeit)
        if not time_match:
            continue

        html_start = f"{time_match.group(1)[:2]}:{time_match.group(1)[2:]}"
        html_end = f"{time_match.group(2)[:2]}:{time_match.group(2)[2:]}"

        if html_start == start_time and html_end == end_time:
            return info.arbeitsplatz, info.pausenzeit, info.task

    return None, None, None


def _should_skip_entry(title: str, colleague: ColleagueConfig, app_config: AppConfig) -> bool:
    """Prüft ob ein Eintrag übersprungen werden soll."""
    # FT für User2 nicht eintragen
    if "FT" in title and colleague.name == app_config.user2_name:
        return True
    # Im only_shifts-Modus: Abwesenheiten überspringen
    if colleague.only_shifts and any(t in title for t in ("FT", "UR", "NV", "KD", "KR")):
        return True
    return False


def _should_delete_event(summary: str, colleague: ColleagueConfig, app_config: AppConfig) -> bool:
    """Prüft ob ein vorhandenes Event gelöscht werden soll."""
    if "FT" in summary and colleague.name == app_config.user2_name:
        return True
    if colleague.only_shifts and any(t in summary for t in ("FT", "UR", "NV", "KD", "KR")):
        return True
    return False


def _count_night_shifts_from_cache(client: CalendarClient, before_date: datetime.datetime) -> int:
    """Zählt Nachtschichten (Start >= 20:00) im aktuellen Jahr vor einem Datum."""
    if isinstance(before_date, datetime.datetime):
        limit = before_date.date()
    else:
        limit = before_date

    count = 0
    for event in client.all_events:
        try:
            start = event.vobject_instance.vevent.dtstart.value
            if isinstance(start, datetime.datetime):
                if start.date().year == limit.year and start.date() < limit:
                    if start.time() >= datetime.time(20, 0):
                        count += 1
        except Exception:
            continue
    return count


def _get_night_shift_summary(client: CalendarClient, year: int) -> Optional[str]:
    """Erstellt die Nachtschicht-Statistik für E-Mails (nur ab November)."""
    today = datetime.date.today()
    if today.month < 11:
        return None

    today_dt = datetime.datetime.combine(today, datetime.time.min)
    year_end = datetime.datetime(year, 12, 31, 23, 59)

    count_current = _count_night_shifts_from_cache(client, today_dt)
    count_year = _count_night_shifts_from_cache(client, year_end)

    return build_night_shift_summary(count_current, count_year, year)
