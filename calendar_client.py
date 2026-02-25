"""CalDAV-Client mit lokalem Event-Cache."""

import datetime
import logging
import re
import urllib.parse
from typing import Dict, List, Optional, Tuple

from caldav import DAVClient

from config import AppConfig, CalDAVCredentials, ColleagueConfig

logger = logging.getLogger(__name__)


class CalendarClient:
    """CalDAV-Kalender-Client mit lokalem Cache für schnelle Duplikatserkennung.

    Pro Kollege wird eine eigene Instanz erzeugt. Der Cache wird einmal beim
    Start aus dem Server geladen und danach lokal synchron gehalten.

    Verwendung:
        client = CalendarClient(config, colleague_config)
        client.connect()
        client.load_cache(start_date, end_date)
        events = client.get_events_on_date(some_date)
        client.add_event(ical_string)
    """

    def __init__(self, app_config: AppConfig, colleague: ColleagueConfig):
        self._app_config = app_config
        self._colleague = colleague
        self._dav_client: Optional[DAVClient] = None
        self._calendar = None

        # Lokaler Cache
        self._events_by_date: Dict[datetime.date, List] = {}
        self._all_events: List = []

    def connect(self) -> bool:
        """Verbindet zum CalDAV-Server und findet den Kalender.

        Returns:
            True bei Erfolg, False bei Fehler.
        """
        service = self._colleague.service_name
        try:
            creds = self._app_config.get_caldav_credentials(service)
        except ValueError as e:
            logger.error("Keine Credentials für Service '%s': %s", service, e)
            return False

        target_name = "Dienstplan " + self._colleague.name.replace(",", "").replace(".", "")
        target_clean = " ".join(target_name.split()).lower()

        try:
            self._dav_client = DAVClient(
                creds.base_url, username=creds.username, password=creds.password
            )
            principal = self._dav_client.principal()
            calendars = principal.calendars()

            for cal in calendars:
                if not cal.name:
                    continue
                server_name_clean = " ".join(str(cal.name).split()).lower()

                if server_name_clean == target_clean:
                    self._calendar = cal
                    return True

                # Fallback: Umlaut-toleranter Vergleich
                if _strip_umlauts(server_name_clean) == _strip_umlauts(target_clean):
                    self._calendar = cal
                    return True

            logger.error("Kalender '%s' nicht gefunden auf Server '%s'.", target_name, service)
            return False

        except Exception as e:
            logger.error("CalDAV-Verbindungsfehler für %s: %s", self._colleague.name, e)
            return False

    def load_cache(self, start: datetime.datetime, end: datetime.datetime):
        """Lädt alle Events im Zeitraum in den lokalen Cache."""
        if not self._calendar:
            logger.error("load_cache aufgerufen ohne verbundenen Kalender.")
            return

        try:
            events = self._calendar.search(start=start, end=end, event=True, expand=False)
            self._events_by_date.clear()
            self._all_events.clear()

            for event in events:
                self._index_event(event)

            logger.debug(
                "%s: %d Termine im Cache (%s bis %s).",
                self._colleague.name, len(self._all_events),
                start.strftime("%d.%m.%Y"), end.strftime("%d.%m.%Y"),
            )
        except Exception as e:
            logger.error("Fehler beim Laden des Caches für %s: %s", self._colleague.name, e)

    def get_events_on_date(self, check_date: datetime.date) -> List:
        """Gibt Events für ein Datum zurück (aus dem lokalen Cache)."""
        if isinstance(check_date, datetime.datetime):
            check_date = check_date.date()
        return list(self._events_by_date.get(check_date, []))  # Kopie!

    @property
    def all_events(self) -> List:
        """Alle gecachten Events (für Nachtschicht-Zählung etc.)."""
        return self._all_events

    def add_event(self, ical_data: str) -> bool:
        """Fügt ein Event zum Server UND zum lokalen Cache hinzu.

        Returns:
            True bei Erfolg.
        """
        if not self._calendar:
            return False
        try:
            new_event = self._calendar.add_event(ical_data)
            self._index_event(new_event)
            return True
        except Exception as e:
            logger.error("Fehler beim Hinzufügen eines Events: %s", e)
            return False

    def delete_event(self, event) -> bool:
        """Löscht ein Event vom Server UND aus dem lokalen Cache.

        Returns:
            True bei Erfolg.
        """
        try:
            # Zuerst aus Cache entfernen (bevor Server-Löschung Daten verliert)
            self._unindex_event(event)
            event.delete()
            return True
        except Exception as e:
            logger.error("Fehler beim Löschen eines Events: %s", e)
            return False

    # --- Interne Methoden ---

    def _index_event(self, event):
        """Fügt ein Event in den lokalen Index ein."""
        try:
            if not hasattr(event, "vobject_instance") or event.vobject_instance is None:
                if not event.data:
                    return
            start = event.vobject_instance.vevent.dtstart.value
            date_key = start.date() if isinstance(start, datetime.datetime) else start

            self._events_by_date.setdefault(date_key, []).append(event)
            self._all_events.append(event)
        except Exception as e:
            logger.debug("Konnte Event nicht indexieren: %s", e)

    def _unindex_event(self, event):
        """Entfernt ein Event aus dem lokalen Index."""
        try:
            start = event.vobject_instance.vevent.dtstart.value
            date_key = start.date() if isinstance(start, datetime.datetime) else start

            if date_key in self._events_by_date:
                day_events = self._events_by_date[date_key]
                if event in day_events:
                    day_events.remove(event)
        except Exception:
            pass  # Falls vobject-Zugriff fehlschlägt

        if event in self._all_events:
            self._all_events.remove(event)


def get_event_details(event) -> Tuple[str, datetime.date, Optional[datetime.datetime], Optional[datetime.datetime]]:
    """Extrahiert Summary, Datum, Start- und Endzeit aus einem CalDAV-Event.

    Returns:
        (summary, date, start_datetime_or_None, end_datetime_or_None)
    """
    vevent = event.vobject_instance.vevent
    summary = vevent.summary.value if hasattr(vevent, "summary") else ""
    start = vevent.dtstart.value
    end = vevent.dtend.value if hasattr(vevent, "dtend") else None

    if isinstance(start, datetime.datetime):
        date_key = start.date()
    else:
        date_key = start
        start = None  # Ganztägig

    if end and isinstance(end, datetime.date) and not isinstance(end, datetime.datetime):
        end = None  # Ganztägig

    return summary, date_key, start, end


def _strip_umlauts(text: str) -> str:
    """Entfernt deutsche Umlaute für URL/Vergleichszwecke."""
    replacements = {
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text
