"""iCal-Event-Erzeugung – reine Funktionen ohne Seiteneffekte."""

import datetime
import uuid
import re
from typing import Optional

import pytz

TZ_BERLIN = pytz.timezone("Europe/Berlin")

# Abwesenheitstypen, die als ganztägig/transparent markiert werden
ABSENCE_TYPES = frozenset({"FT", "UR", "NV", "KD", "KR", "FU", "AS"})


def build_ical_event(
    title: str,
    start: datetime.datetime,
    end: Optional[datetime.datetime] = None,
    all_day: bool = False,
    description: Optional[str] = None,
    location: Optional[str] = None,
) -> str:
    """Erzeugt einen vollständigen VCALENDAR-String.

    Args:
        title: Event-Titel (wird sanitisiert)
        start: Startdatum/-zeit
        end: Enddatum/-zeit (ignoriert bei all_day)
        all_day: Ganztägiges Event
        description: Beschreibungstext
        location: Ort (optional)

    Returns:
        iCal-String (VCALENDAR mit VEVENT und VTIMEZONE)
    """
    now = datetime.datetime.now(TZ_BERLIN)
    uid = f"{uuid.uuid4()}@dienstplan"

    # Titel und Beschreibung säubern
    safe_title = _sanitize_ical_text(title)
    safe_desc = _sanitize_ical_text(description or "")

    if all_day:
        start_str = start.strftime("%Y%m%d")
        end_date = start + datetime.timedelta(days=1)
        end_str = end_date.strftime("%Y%m%d")
        dtstart = f"DTSTART;VALUE=DATE:{start_str}"
        dtend = f"DTEND;VALUE=DATE:{end_str}"
    else:
        dtstart = f"DTSTART;TZID=Europe/Berlin:{start.strftime('%Y%m%dT%H%M%S')}"
        if end:
            dtend = f"DTEND;TZID=Europe/Berlin:{end.strftime('%Y%m%dT%H%M%S')}"
        else:
            dtend = f"DTEND;TZID=Europe/Berlin:{start.strftime('%Y%m%dT%H%M%S')}"

    # Busy-Status: Abwesenheiten als OOF/transparent
    is_absence = any(term in title for term in ABSENCE_TYPES)
    busy_status = "X-MICROSOFT-CDO-BUSYSTATUS:OOF" if is_absence else "X-MICROSOFT-CDO-BUSYSTATUS:BUSY"
    transp = "TRANSP:TRANSPARENT" if is_absence else "TRANSP:OPAQUE"

    # Location-Zeile nur wenn gesetzt
    location_line = f"LOCATION:{_sanitize_ical_text(location)}" if location else ""

    # Description-Zeile nur wenn gesetzt
    desc_line = f"DESCRIPTION:{safe_desc}" if safe_desc else ""

    lines = [
        "BEGIN:VCALENDAR",
        "CALSCALE:GREGORIAN",
        "VERSION:2.0",
        "PRODID:-//Dienstplan//v2.0//DE",
        "BEGIN:VEVENT",
        f"SUMMARY:{safe_title}",
        transp,
        dtstart,
        dtend,
        f"DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}",
        f"UID:{uid}",
        "SEQUENCE:1",
        desc_line,
        f"LAST-MODIFIED:{now.strftime('%Y%m%dT%H%M%SZ')}",
        location_line,
        busy_status,
        "END:VEVENT",
        _vtimezone_berlin(),
        "END:VCALENDAR",
    ]

    # Leere Zeilen entfernen (z.B. wenn location_line leer ist)
    return "\n".join(line for line in lines if line) + "\n"


def build_event_description(
    title: str,
    workplace: Optional[str] = None,
    break_time: Optional[str] = None,
    task: Optional[str] = None,
) -> str:
    parts = [f"Eintrag: {title}"]
    if workplace:
        parts.append(f"Platz: {workplace}")
    if break_time:
        parts.append(f"Pause: {break_time}")
    if task:
        parts.append(f"Aufgabe: {task}")
    parts.append("Alle Angaben und Inhalte sind ohne Gewähr.")
    parts.append(f"Änderungsdatum: {datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')}")
    return ", ".join(parts)


def format_event_log(start: datetime.datetime, title: str, end: Optional[datetime.datetime] = None) -> str:
    """Erzeugt einen formatierten Logeintrag für einen Termin."""
    if end:
        return f"{start.strftime('%d.%m.%Y')}, {start.strftime('%H:%M')} bis {end.strftime('%H:%M')}: {title}"
    return f"{start.strftime('%d.%m.%Y')}: {title}"


def _sanitize_ical_text(text: str) -> str:
    if not text:
        return ""
    # Maskiert Backslashes, Semikolons und Kommas iCal-konform
    text = text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    # Ersetzt Zeilenumbrüche durch die iCal-Sequenz \n
    return text.replace("\n", "\\n").replace("\r", "")


def _vtimezone_berlin() -> str:
    """Gibt den VTIMEZONE-Block für Europe/Berlin zurück."""
    return """BEGIN:VTIMEZONE
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
END:VTIMEZONE"""
