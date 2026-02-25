"""E-Mail-Benachrichtigungen für neue Dienstplan-Einträge."""

import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from config import AppConfig

logger = logging.getLogger(__name__)

# Wochentags-Abkürzungen
_WOCHENTAGE = ["Mo.", "Di.", "Mi.", "Do.", "Fr.", "Sa.", "So."]


def send_notification(
    app_config: AppConfig,
    user_name: str,
    entries: List[str],
    night_shift_summary: Optional[str] = None,
):
    """Sendet eine E-Mail-Benachrichtigung über neue Termine.

    Args:
        app_config: Zentrale Konfiguration
        user_name: Name des Kollegen
        entries: Liste von formatierten Termin-Strings ("DD.MM.YYYY, HH:MM bis HH:MM: Titel")
        night_shift_summary: Optionaler HTML-Text zur Nachtschicht-Statistik
    """
    email_entry = app_config.get_email_entry(user_name)
    if not email_entry:
        logger.warning("Keine E-Mail-Konfiguration für '%s' – Benachrichtigung übersprungen.", user_name)
        return

    # Nur zukünftige Termine in die Mail aufnehmen
    today = datetime.date.today()
    future_entries = []
    for entry in entries:
        try:
            date_str = entry.split(",")[0].split(" ")[0] if ", " in entry else entry.split(":")[0].strip()
            # Versuche das Datum zu parsen
            parts = date_str.strip().split(".")
            if len(parts) == 3:
                entry_date = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
                if entry_date < today:
                    continue
        except (ValueError, IndexError):
            pass  # Im Zweifel aufnehmen

        # Wochentag voranstellen
        future_entries.append(_add_weekday(entry))

    if not future_entries:
        logger.debug("Keine zukünftigen Termine für %s – keine Mail.", user_name)
        return

    # Mail-Body zusammenbauen
    body = "Es wurden folgende Termine eingetragen:<br><br>"
    body += "<br>".join(future_entries)

    if night_shift_summary:
        body += "<br><br>" + night_shift_summary

    body += "<br><br>"
    body += (
        f'<a href="{app_config.kalender_base_url}{email_entry.kalender_id}">Kalender</a><br>'
        f'<a href="{app_config.abo_base_url}{email_entry.kalender_id}?export">Abo-URL</a><br>'
        f"Alle Angaben und Inhalte sind ohne Gewähr."
    )

    _send_smtp(
        app_config=app_config,
        to_email=email_entry.email,
        subject=f"Dienstplan Update {user_name}",
        html_body=body,
    )
    logger.info("Mail an %s gesendet (%d Termine).", user_name, len(future_entries))


def build_night_shift_summary(night_count_current: int, night_count_year: int, year: int) -> Optional[str]:
    """Baut den Nachtschicht-Statistik-Text für die Mail.

    Nur relevant ab November.

    Returns:
        HTML-String oder None wenn nicht relevant.
    """
    today = datetime.date.today()
    if today.month < 11:
        return None

    if night_count_current == 0 and night_count_year == 0:
        return None

    parts = []

    if night_count_current == 1:
        parts.append(f"Im Jahr {year} hattest du bisher {night_count_current} Nachtschicht.")
    elif night_count_current > 1:
        parts.append(f"Im Jahr {year} hattest du bisher {night_count_current} Nachtschichten.")
    else:
        parts.append(f"Im Jahr {year} hattest du bisher keine Nachtschichten.")

    remaining = night_count_year - night_count_current
    if remaining > 0:
        word = "Nachtschicht" if remaining == 1 else "Nachtschichten"
        parts.append(f"<br>Es sind noch {remaining} {word} für dich disponiert.<br>")
        total_word = "Nachtschicht" if night_count_year == 1 else "Nachtschichten"
        parts.append(f"Das wären dann insgesamt {night_count_year} {total_word} für {year}.")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Interne Hilfsfunktionen
# ---------------------------------------------------------------------------

def _add_weekday(entry: str) -> str:
    """Stellt den deutschen Wochentag vor einen Termin-String."""
    try:
        date_str = entry.split(",")[0].split(" ")[0] if ", " in entry else entry.split(":")[0].strip()
        parts = date_str.strip().split(".")
        if len(parts) == 3:
            dt = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
            return f"{_WOCHENTAGE[dt.weekday()]} {entry}"
    except (ValueError, IndexError):
        pass
    return entry


def _send_smtp(app_config: AppConfig, to_email: str, subject: str, html_body: str):
    """Sendet eine HTML-E-Mail über SMTP."""
    from_email = app_config.smtp_email
    password = app_config.smtp_password

    msg = MIMEMultipart()
    msg["From"] = f"Dein Dienstplan <{from_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP("smtp.ionos.de", 587, timeout=30) as server:
            server.starttls()
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
    except Exception as e:
        logger.error("Fehler beim Senden der E-Mail an %s: %s", to_email, e)
