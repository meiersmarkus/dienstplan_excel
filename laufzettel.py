"""Laufzettel-Verwaltung: HTML parsen, datumsbezogen den richtigen Laufzettel liefern."""

import datetime
import os
import re
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class ShiftInfo:
    """Informationen zu einer Schicht aus dem Laufzettel."""
    dienstname: str
    dienstzeit: str
    arbeitsplatz: str
    pausenzeit: str
    task: str


class LaufzettelManager:
    """Verwaltet alle verfügbaren Laufzettel und liefert den passenden für ein Datum.

    Thread-safe für lesende Zugriffe (die Daten werden beim Init geladen und
    danach nicht mehr verändert).

    Verwendung:
        mgr = LaufzettelManager("/pfad/zum/projektordner")
        werktags, wochenende = mgr.get_for_date(datetime.date(2025, 6, 20))
    """

    def __init__(self, folder_path: str):
        self._folder = folder_path
        # Sortierte Liste aller verfügbaren Laufzettel-Daten
        self._dates: List[datetime.datetime] = []
        # Cache: Datum → (werktags, wochenende)
        self._parsed: Dict[datetime.datetime, Tuple[List[ShiftInfo], List[ShiftInfo]]] = {}
        self._warned_empty = False
        self._load_all()

    def _load_all(self):
        """Findet und sortiert alle Laufzettel-HTML-Dateien."""
        pattern = re.compile(r"Laufzettel_(\d{8})\.html")
        dates = []
        for fname in os.listdir(self._folder):
            m = pattern.match(fname)
            if m:
                try:
                    dt = datetime.datetime.strptime(m.group(1), "%Y%m%d")
                    dates.append(dt)
                except ValueError:
                    logger.warning("Ungültiges Datum in Laufzettel-Dateiname: %s", fname)
        dates.sort()
        self._dates = dates
        logger.debug("%d Laufzettel-Dateien gefunden.", len(dates))

    def get_for_date(self, target_date: datetime.date) -> Tuple[List[ShiftInfo], List[ShiftInfo]]:
        """Gibt (werktags, wochenende)-Schichtinfos für ein Datum zurück.

        Wählt den Laufzettel mit dem höchsten Gültigkeitsdatum <= target_date.
        """
        if isinstance(target_date, datetime.datetime):
            target_date = target_date.date()

        # Finde den passenden Laufzettel (letzter, dessen Datum <= target_date)
        active_date = None
        for dt in self._dates:
            if dt.date() <= target_date:
                active_date = dt
            else:
                break

        if active_date is None:
            if self._dates:
                active_date = self._dates[0]  # Fallback: ältester verfügbarer
                logger.warning(
                    "Kein Laufzettel für %s gefunden, verwende ältesten: %s",
                    target_date, active_date.strftime("%d.%m.%Y"),
                )
            else:
                if not self._warned_empty:
                    logger.warning("Keine Laufzettel-Dateien vorhanden in %s.", self._folder)
                    self._warned_empty = True
                return [], []

        # Aus Cache oder neu parsen
        if active_date not in self._parsed:
            html_path = os.path.join(
                self._folder, f"Laufzettel_{active_date.strftime('%Y%m%d')}.html"
            )
            self._parsed[active_date] = _parse_html(html_path)

        return self._parsed[active_date]


def _parse_html(html_path: str) -> Tuple[List[ShiftInfo], List[ShiftInfo]]:
    """Parst eine Laufzettel-HTML-Datei und extrahiert Werktags- und Wochenend-Tabellen."""
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
    except FileNotFoundError:
        logger.error("Laufzettel-Datei nicht gefunden: %s", html_path)
        return [], []
    except Exception as e:
        logger.error("Fehler beim Lesen von %s: %s", html_path, e)
        return [], []

    werktags = _extract_table(soup.find("table", summary="Laufzettel ab 01.01.23 Werktags"))
    wochenende = _extract_table(soup.find("table", summary="Laufzettel ab 01.01.23 Wochenende"))

    logger.debug(
        "Laufzettel %s: %d Werktags-Einträge, %d WE-Einträge",
        os.path.basename(html_path), len(werktags), len(wochenende),
    )
    return werktags, wochenende


def _extract_table(table) -> List[ShiftInfo]:
    """Extrahiert Schicht-Daten aus einer HTML-Tabelle."""
    if not table:
        return []

    results = []
    for row in table.find_all("tr"):
        cols = [col.text.replace("\u00A0", " ").strip() for col in row.find_all("td")]
        if len(cols) >= 5:
            results.append(ShiftInfo(
                dienstname=cols[0],
                dienstzeit=cols[1],
                arbeitsplatz=cols[2],
                pausenzeit=cols[3],
                task=cols[4],
            ))
            if len(results) > 40:  # Sicherheitslimit
                break
    return results
