"""Alte Kalendereinträge löschen (ersetzt Diensteloeschen.py)."""

import datetime
import logging

from caldav import DAVClient

from config import AppConfig

logger = logging.getLogger(__name__)


def delete_old_entries(app_config: AppConfig, user_name: str, years_back: int = 2):
    """Löscht alle Kalendereinträge eines Kollegen für ein vergangenes Jahr.

    Args:
        app_config: Zentrale Konfiguration
        user_name: Name des Kollegen
        years_back: Wie viele Jahre zurück löschen (Standard: 2)
    """
    target_year = datetime.datetime.now().year - years_back
    service = "ard"

    try:
        creds = app_config.get_caldav_credentials(service)
    except ValueError as e:
        logger.error("Keine Credentials für %s: %s", user_name, e)
        return

    calendar_name = "Dienstplan " + user_name.replace(",", "").replace(".", "")

    try:
        client = DAVClient(creds.base_url, username=creds.username, password=creds.password)
        principal = client.principal()

        try:
            calendar = principal.calendar(name=calendar_name)
        except Exception:
            logger.warning("Kalender '%s' nicht gefunden – überspringe.", calendar_name)
            return

        start_date = datetime.datetime(target_year, 1, 1)
        end_date = datetime.datetime(target_year, 12, 31, 23, 59, 59)

        events = calendar.date_search(start=start_date, end=end_date)
        if not events:
            logger.debug("Keine Termine für %s im Jahr %d.", user_name, target_year)
            return

        deleted = 0
        for event in events:
            try:
                event.delete()
                deleted += 1
            except Exception as e:
                logger.warning("Konnte Event nicht löschen: %s", e)

        logger.info("%d Termine für %s aus %d gelöscht.", deleted, user_name, target_year)

    except Exception as e:
        logger.error("Fehler beim Löschen für %s: %s", user_name, e)
