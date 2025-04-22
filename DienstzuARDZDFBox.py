import sys
import time
import os
import argparse
import json
import pandas as pd
import datetime
import locale
import re
from bs4 import BeautifulSoup
from caldav import DAVClient
from datetime import date, timedelta
import datetime as dt
import pytz
import holidays
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dateutil.easter import easter
from itertools import chain

# Initialisiere Timer
timers = {}


def start_timer(timer_name):
    timers[timer_name] = time.time()


start_timer("gesamt")
start_timer("initial")


def end_timer(timer_name, task_description):
    if timer_name in timers:
        end_time = time.time()
        elapsed_time = end_time - timers[timer_name]
        # if timer_name == "gesamt":
        #    print(f"[TIME] {task_description}: {elapsed_time:.2f} Sekunden", end="")
        if not (timer_name in ("caldav", "initial", "gesamt") and elapsed_time <= 20):
            print(f"[TIME] {task_description}: {elapsed_time:.2f} Sekunden")
        del timers[timer_name]  # Timer entfernen, wenn er fertig ist
    else:
        print(f"[ERROR] Kein aktiver Timer mit dem Namen: {timer_name}")


def is_holiday_or_weekend(datum):
    if datum in de_holidays:
        return True, de_holidays.get(datum)
    if datum.weekday() >= 5:
        return True, "Samstag" if datum.weekday() == 5 else "Sonntag"
    return False, None


def parse_html_for_workplace_info(html_file_path):  # Function to parse HTML and extract workplace, breaks, and tasks
    # start_timer("html")
    def extract_info(table):
        # Extrahiert die Informationen aus einer HTML-Tabelle und gibt eine Liste von Einträgen zurück.
        info = []
        if table:
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 5:
                    info.append({
                        'dienstname': cols[0].text.replace('\u00A0', ' ').strip(),
                        'dienstzeit': cols[1].text.replace('\u00A0', ' ').strip(),
                        'arbeitsplatz': cols[2].text.replace('\u00A0', ' ').strip(),
                        'pausenzeit': cols[3].text.replace('\u00A0', ' ').strip(),
                        'task': cols[4].text.replace('\u00A0', ' ').strip()
                    })
        return info

    # HTML parsen
    with open(html_file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')

    # Werktags- und Wochenend-Bereich extrahieren
    werktags_table = soup.find('table', summary='Laufzettel ab 01.01.23 Werktags')
    wochenende_table = soup.find('table', summary='Laufzettel ab 01.01.23 Wochenende')

    laufzettel_werktags = extract_info(werktags_table) if werktags_table else []
    laufzettel_we = extract_info(wochenende_table) if wochenende_table else []

    # Rückgabe der getrennten Listen
    # end_timer("html", "HTML auslesen")
    return laufzettel_werktags, laufzettel_we


def count_night_shifts(client, calendar, search_start):
    # Sicherstellen, dass das Datum ein datetime-Objekt ist
    if isinstance(search_start, str):
        search_start = pd.to_datetime(search_start)

    # Suchzeitraum: Jahresbeginn bis zum aktuellen Tag
    year = search_start.year
    startoftheyear = datetime.datetime(year, 1, 1, 11, 0)
    dateofentry = search_start.replace(hour=12, minute=0, second=0)

    try:
        # Events im Zeitraum abrufen
        events = calendar.date_search(start=startoftheyear, end=dateofentry)
    except Exception as e:
        print(f"[ERROR] Fehler beim Abrufen der Events: {e}")
        return 0

    # Zählen aller Events mit Startzeit nach 20:00 Uhr
    night_shift_count = 0
    for i, event in enumerate(events):
        try:
            start_time = event.vobject_instance.vevent.dtstart.value
            if isinstance(start_time, datetime.datetime):
                start_time = start_time.time()
            elif isinstance(start_time, datetime.date):
                start_time = datetime.time(0, 0)  # Falls nur Datum ohne Zeit angegeben wurde

            # Prüfen, ob die Startzeit nach 20:00 Uhr liegt
            if start_time >= datetime.time(20, 0):
                night_shift_count += 1
        except Exception as e:
            print(f"[ERROR] Fehler beim Verarbeiten von Event {i + 1}: {e}")
    return night_shift_count


def create_ical_event(
    title,
    start_datetime,
    end_datetime=None,
    all_day=False,
    description=None,
    workplace=None,  # Add workplace parameter
    break_time=None,
    task=None
):
    if any(term in title for term in ['FT']) and user_name == load_credentials("user2", config_path):
        # print(f"[DEBUG] {title} übersprungen, keine FT ausgewählt wurde.")
        return
    if any(term in title for term in ['FT', 'UR', 'NV', 'KD', 'KR']) and dienste:
        # print(f"[DEBUG] {title} übersprungen, weil -d ausgewählt wurde.")
        return
    try:
        now = datetime.datetime.now(pytz.timezone("Europe/Berlin"))
        location = ''
        if all_day:
            # For all-day events, only include the date, without time and timezone
            start_str = start_datetime.strftime('%Y%m%d')
            end_str = (start_datetime + pd.Timedelta(days=1)).strftime('%Y%m%d')
            dtstart_str = f"DTSTART;VALUE=DATE:{start_str}"
            dtend_str = f"DTEND;VALUE=DATE:{end_str}"
        else:
            # Normal events with time and timezone
            start_str = start_datetime.strftime('%Y%m%dT%H%M%S')
            end_str = end_datetime.strftime('%Y%m%dT%H%M%S') if end_datetime else start_str
            dtstart_str = f"DTSTART;TZID=Europe/Berlin:{start_str}"
            dtend_str = f"DTEND;TZID=Europe/Berlin:{end_str}"
            if ort:
                location = r'LOCATION:Hugh-Greene-Weg 1\, 22529 Hamburg'

        # Use the provided description or default to Dienst information
        description_str = (
            description if description else
            f"Eintrag: {title}, Alle Angaben und Inhalte sind ohne Gewähr. "
            f"Änderungsdatum: {datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')}"
        )
        busy = (
            "X-MICROSOFT-CDO-BUSYSTATUS:OOF"
            if any(term in title for term in ["FT", "UR", "NV", "KD", "KR"])
            else "X-MICROSOFT-CDO-BUSYSTATUS:BUSY"
        )
        transparent = "TRANSP:TRANSPARENT" if any(
            term in title for term in ["FT", "UR", "NV", "KD", "KR"]
        ) else "TRANSP:OPAQUE"
        sanitized_title = title.replace("\n", " ").replace("\r", "").strip()
        sanitized_desc = description_str.replace("\n", " ").replace("\r", "").strip()
        ical_event = f"""BEGIN:VCALENDAR
CALSCALE:GREGORIAN
VERSION:2.0
PRODID:-//meiersmarkus//NONSGML v1.0//DE
BEGIN:VEVENT
SUMMARY:{sanitized_title}
{transparent}
{dtstart_str}
{dtend_str}
DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}
UID:{now.timestamp()}@meiersmarkus.de
SEQUENCE:1
DESCRIPTION:{sanitized_desc}
LAST-MODIFIED:{now.strftime('%Y%m%dT%H%M%SZ')}
{location}
{busy}
END:VEVENT
BEGIN:VTIMEZONE
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
END:VTIMEZONE
END:VCALENDAR
"""
        # Termine in die Liste einfügen
        zeit = f" von {start_datetime.strftime('%H:%M')} - {end_datetime.strftime('%H:%M')} Uhr" if not all_day else ""
        eingetragene_termine.append(f"{start_datetime.strftime('%d.%m.%Y')}{zeit}: {title}")

        return ical_event
    except Exception as e:
        print(f"[ERROR] Fehler beim Erstellen des Events: {e}")
        return None


def process_all_day_event(service_entry, start_date):  # Funktion zur Verarbeitung eines ganztägigen Events
    title = service_entry.strip() or "Ganztägiger Termin"
    start_datetime = pd.to_datetime(start_date)

    try:
        # Suche nach vorhandenen ganztägigen Terminen
        existing_events = calendar.date_search(start_datetime, start_datetime + pd.Timedelta(days=1))
        event_exists = False

        for event in existing_events:
            event.load()
            event_summary = event.vobject_instance.vevent.summary.value
            event_start = event.vobject_instance.vevent.dtstart.value

            if any(term in event_summary for term in ['FT']) and user_name == load_credentials("user2", config_path):
                print(f"[DEBUG] Lösche {event_summary} vom {event_start}, da FT nicht eingetragen werden sollen.")
                event.delete()
                continue
            if any(term in event_summary for term in ['FT', 'UR', 'NV', 'KD', 'KR']) and dienste:
                print(f"[DEBUG] Lösche {event_summary} vom {event_start}, da nur Dienste eingetragen werden sollen.")
                event.delete()
                continue

            # Prüfen, ob event_start eine Uhrzeit enthält
            if isinstance(event_start, dt.datetime):
                event_start = event_start.date()  # Nur das Datum extrahieren

            if rewrite and event_start == start_datetime.date():
                event.delete()
                continue
            elif event_summary == title.replace("\n", " ").replace("\r", "").strip() and event_start == start_datetime.date():
                event_exists = True
                break
            elif event_start == start_datetime.date():
                print(f"[DEBUG] Anderer Termin: '{event_summary}' am {start_datetime.strftime('%d.%m.%Y')} wird gelöscht.")
                event.delete()

        # Event erstellen, wenn kein passender Termin vorhanden ist
        if not event_exists:
            ical_data = create_ical_event(title, start_datetime, all_day=True)
            if ical_data:
                calendar.add_event(ical_data)
                print(f"[Dienst] {start_datetime.strftime('%d.%m.%Y')}: {title}")

    except Exception as e:
        print(f"[ERROR] Fehler beim Speichern oder Löschen des ganztägigen Events: {title} am {start_datetime}")
        print(e)


# Funktion zur Verarbeitung eines zeitgebundenen Events
def process_timed_event(service_entry, start_date, laufzettel_werktags, laufzettel_we, countnightshifts, nonightshifts):
    # Extract start and end time from Excel entry
    time_match = re.match(r'(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})', service_entry)
    if time_match:
        start_time_str = time_match.group(1)
        end_time_str = time_match.group(2)

        start_datetime = datetime.datetime.strptime(f"{start_date.strftime('%Y-%m-%d')} {start_time_str}", '%Y-%m-%d %H:%M')
        end_datetime = datetime.datetime.strptime(f"{start_date.strftime('%Y-%m-%d')} {end_time_str}", '%Y-%m-%d %H:%M')

        if start_datetime.tzinfo is None:
            start_datetime = tz_berlin.localize(start_datetime)
        if end_datetime.tzinfo is None:
            end_datetime = tz_berlin.localize(end_datetime)

        if user_name == load_credentials("user1", config_path) and end_datetime.time() < datetime.time(8, 0):
            end_datetime = tz_berlin.localize(datetime.datetime.combine(end_datetime.date(), datetime.time(23, 59)))
        elif end_datetime < start_datetime:
            end_datetime += datetime.timedelta(days=1)

        # Get the basic title from the Excel entry (e.g., "Schnitt 2")
        title = service_entry[time_match.end():].strip() or "Kein Titel"
        # print(f"[DEBUG] Excel event: {title}, start: {start_time_str}, end: {end_time_str}")

        # Default values for workplace info (in case no match is found in HTML)
        workplace = None
        break_time = None
        task = None

        is_holiday_flag, holiday_name = is_holiday_or_weekend(start_date.date())
        workplace_info = laufzettel_we if is_holiday_flag else laufzettel_werktags
        try:
            cleaned_service_entry = re.sub(r'\s*\(WT\)|\s*Info ', '', service_entry[time_match.end():].strip())
            for info in workplace_info:
                dienstname = info['dienstname'].replace("Samstag: ", "").replace("Sonntag: ", "").strip()
                # print(f"[DEBUG] Vergleiche Excel '{cleaned_service_entry}' mit Laufzettel '{dienstname}'")
                if cleaned_service_entry.lower() in dienstname.lower():
                    # print(f"[DEBUG] {dienstname} gefunden.")
                    # DIENSTZEIT ist im HHMM-HHMM Format
                    # print(f"[DEBUG] Dienstzeit: {info['dienstzeit']}")
                    html_time_match = re.match(
                        r'(\d{4})\s*-\s*(\d{4})', info['dienstzeit']
                    )
                    if html_time_match:
                        html_start_time = f"{html_time_match.group(1)[:2]}:{html_time_match.group(1)[2:]}"
                        # print(f"[DEBUG] HTML Startzeit: {html_start_time}")
                        html_end_time = f"{html_time_match.group(2)[:2]}:{html_time_match.group(2)[2:]}"
                        # print(f"[DEBUG] HTML Endzeit: {html_end_time}")

                        if html_start_time == start_time_str and html_end_time == end_time_str:
                            workplace = info.get('arbeitsplatz', None)
                            break_time = info.get('pausenzeit', None)
                            task = info.get('task', None)
                            # print(f"[DEBUG] {cleaned_service_entry}, {workplace}")
                            break
            # print(f"[DEBUG] Workplace: {workplace}, Break: {break_time}, Task: {task}")

            if user_name == load_credentials("user1", config_path):
                full_title = f"{start_time_str}-{end_time_str} {title}"
            else:
                full_title = f"{title}, {workplace}" if workplace and workplace not in title else title

            if start_datetime.time() >= datetime.time(20, 0) and nonightshifts == False:  # Dienste mit Startzeit nach 20:00 Uhr
                # Prüfen, ob die Nachtschichtzählung neu gestartet werden soll
                if not countnightshifts or start_date.month in [1, 2, 3, 4]:
                    # print(f"[DEBUG] Starte neue Zählung: Monat={start_date.month}, Datum={start_date}")
                    night_shifts_count = count_night_shifts(client, calendar, start_date)
                    night_shifts_count += 1  # Erhöhe die Anzahl um 1 für den aktuellen Dienst
                    countnightshifts = True  # Markiere, dass die Zählung gestartet wurde
                else:
                    night_shifts_count += 1  # Erhöhe die Anzahl um 1 für den aktuellen Dienst
                    # print(f"[DEBUG] Nachtschicht-Zähler: {night_shifts_count}")
                full_title += f" ({night_shifts_count})"

            # print(f"[DEBUG] Full event title: {full_title}")
            # print(f"[DEBUG] Excel: '{full_title.strip()}' am '{start_datetime.date()}' von '{start_datetime.time()}' bis '{end_datetime.time()}' Uhr.")
            # Now check if the event with the full title already exists
            existing_events = calendar.date_search(
                start_datetime.replace(hour=0, minute=0, second=0),
                end_datetime.replace(hour=23, minute=59, second=59)
            )
            event_exists = False

            for event in existing_events:
                event.load()
                event_summary = event.vobject_instance.vevent.summary.value
                event_start = event.vobject_instance.vevent.dtstart.value
                event_end = (event.vobject_instance.vevent.dtend.value
                             if hasattr(event.vobject_instance.vevent, 'dtend')
                             else None)
                # print(f"[DEBUG] {len(existing_events)} Termine gefunden.")

                if any(term in event_summary for term in ['FT']) and user_name == load_credentials("user2", config_path):
                    print(f"[DEBUG] Lösche {event_summary} vom {event_start}, da FT nicht eingetragen werden sollen.")
                    event.delete()
                    continue
                if any(term in event_summary for term in ['FT', 'UR', 'NV', 'KD', 'KR']) and dienste:
                    print(f"[DEBUG] Lösche {event_summary} vom {event_start}, da nur Dienste eingetragen werden sollen.")
                    event.delete()
                    continue

                # Ensure event_start and event_end are datetime objects, and localize if necessary
                if isinstance(event_start, datetime.date) and not isinstance(event_start, datetime.datetime):
                    event_start = datetime.datetime.combine(event_start, datetime.time.min)
                if isinstance(event_start, datetime.datetime) and event_start.tzinfo is None:
                    event_start = tz_berlin.localize(event_start)
                if event_end and isinstance(event_end, datetime.date) and not isinstance(event_end, datetime.datetime):
                    event_end = datetime.datetime.combine(event_end, datetime.time.min)
                if event_end and isinstance(event_end, datetime.datetime) and event_end.tzinfo is None:
                    event_end = tz_berlin.localize(event_end)

                if rewrite:
                    if event_start.date() == start_datetime.date():
                        event.delete()
                        continue
                # Compare the fully generated title with the existing event's summary
                if (event_summary.strip() == full_title.replace("\n", " ").replace("\r", "").strip() and
                        event_start == start_datetime and
                        event_end == end_datetime):
                    event_exists = True
                    # print(f"[DEBUG] Event '{full_title}' already exists. Skipping creation.")
                    break
                # print(f"[DEBUG] Excel: '{full_title.strip()}' am '{start_datetime.date()}'. "
                #       f"Kalender: '{event_summary}' am '{event_start.date()}'.")
                if event_start.date() == start_datetime.date():
                    print(f"[DEBUG] Anderer Termin: '{event_summary}' am {start_datetime.strftime('%d.%m.%Y')} wird gelöscht.")
                    event.delete()

            # If the event does not exist, create it with all the information collected
            if not event_exists:
                # Create the description by including the break time (if available) and the task
                description = f"Dienst: {title}, "
                if workplace:
                    description += f"Platz: {workplace}, "
                if break_time:
                    description += f"Pause: {break_time}, "
                if task:
                    description += f"Aufgabe: {task}. "

                # Add the current modification date
                last_modified = datetime.datetime.now().strftime('%d.%m.%Y, %H:%M')
                description += "Alle Angaben und Inhalte sind ohne Gewähr. "
                description += f"Änderungsdatum: {last_modified}"

                # Create the iCal event with the full description
                # if is_holiday_flag:
                #    print(f"[DEBUG] {start_date.strftime('%a, %d.%m.%Y')} ist ein Feiertag oder Wochenende: {holiday_name}")
                ical_data = create_ical_event(
                    full_title, start_datetime, end_datetime,
                    workplace=workplace, break_time=break_time,
                    task=task, description=description
                )
                if ical_data:
                    calendar.add_event(ical_data)
                    print(f"[Dienst] {start_datetime.strftime('%d.%m.%Y')}, "
                          f"{start_datetime.strftime('%H:%M')} bis {end_datetime.strftime('%H:%M')}: {full_title}")
        except Exception as e:
            print(f"[ERROR] Fehler beim Speichern oder Löschen des Events: {full_title}, {e}")


def process_excel_file(file_path, user_name, laufzettel_werktags, laufzettel_we, countnightshifts, nextlaufzettel, current_laufzettel):
    df = pd.read_excel(file_path, header=None, engine='openpyxl')
    # print(f"[DEBUG] Verfügbare Namen: {df[0].unique()}")
    identifier_row = df[df[0].str.contains("I", na=False)].iloc[0]
    # Versuch, den Namen flexibler zu finden
    user_name_cleaned = re.sub(r',\s*[A-Z]\.?$', '', user_name).strip()  # Entfernt ", V." oder ähnliche Initialen
    try:
        user_row = df[
            df[0].str.strip().str.casefold().str.replace(
                r'\s*[\r\n]*\(.*\)\s*[\r\n]*', '', regex=True
            ) == user_name.strip().casefold()
        ].iloc[0]
    except IndexError:
        try:
            # 2. Fallback: Suche nur nach Nachnamen (z.B. "Nachname")
            user_row = df[
                df[0].str.strip().str.casefold().str.replace(
                    r'\s*[\r\n]*\(.*\)\s*[\r\n]*', '', regex=True
                ) == user_name_cleaned.casefold()
            ].iloc[0]
        except IndexError:
            if not dienste:
                print(f"[ERROR] Benutzer '{user_name}' oder '{user_name_cleaned}' nicht gefunden!")
            # Alle Termine für die entsprechenden Daten löschen
            for day in range(1, 8):  # Spalten B bis H (1 bis 7)
                try:
                    date = identifier_row[day]
                    start_datetime = pd.to_datetime(date)

                    # Prüfe Laufzettelwechsel
                    # print(f"[DEBUG] Prüfe Laufzettelwechsel für {start_datetime.date()} und {nextlaufzettel.date()}")
                    if nextlaufzettel and isinstance(nextlaufzettel, datetime.datetime) and start_datetime.date() >= nextlaufzettel.date():
                        print(f"[INFO] Wechsel zu Laufzettel ab {nextlaufzettel.strftime('%d.%m.%Y')}")
                        html_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                                    'Laufzettel_' + nextlaufzettel.strftime('%Y%m%d') + '.html')
                        laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info(html_file_path)
                        current_laufzettel = nextlaufzettel
                        next_date = getnextlaufzettel(nextlaufzettel)
                        if next_date and next_date != nextlaufzettel:
                            nextlaufzettel = next_date
                            print(f"[DEBUG] Nächster Laufzettel wird sein: {nextlaufzettel.strftime('%d.%m.%Y')}")

                    existing_events = calendar.date_search(start_datetime, start_datetime + pd.Timedelta(days=1))
                    # print(f"[DEBUG] Prüfe {start_datetime.strftime('%d.%m.%Y')} auf Termine.")
                    for event in existing_events:
                        event.load()
                        event_summary = event.vobject_instance.vevent.summary.value
                        event_start = event.vobject_instance.vevent.dtstart.value
                        # Ensure event_start and event_end are datetime objects, and localize if necessary
                        if event_start.date() == start_datetime.date():
                            print(f"[DEBUG] Lösche {event_summary} vom {event_start.strftime('%d.%m.%Y')}, "
                                  "da Nutzer nicht gefunden wurde.")
                            event.delete()
                except Exception as e:
                    print(f"[ERROR] Fehler beim Löschen der Termine für {date}: {e}")
            return nextlaufzettel, current_laufzettel

    # print(f"[DEBUG] '{user_name}'")
    # Prüfe, ob Termine im Januar des aktuellen Jahres eingetragen sind
    nonightshifts = True
    year = identifier_row[1].year
    start_of_january = datetime.datetime(year, 1, 1, 0, 0, tzinfo=tz_berlin)
    end_of_january = datetime.datetime(year, 1, 31, 23, 59, tzinfo=tz_berlin)

    try:
        termine = calendar.date_search(start=start_of_january, end=end_of_january)
        if len(termine) == 0:
            nonightshifts = True
        else:
            nonightshifts = False
            # print(f"[DEBUG] {user_name} hat bereits Termine im Januar {year} eingetragen.")
    except Exception as e:
        print(f"[ERROR] Fehler beim Durchsuchen des Kalenders: {e}")
        nonightshifts = True

    latest_date = None
    for day in range(1, 8):  # Spalten B bis H (1 bis 7)
        date = identifier_row[day]
        service_entry = user_row[day]
        start_date = pd.to_datetime(date)
        # print(f"[DEBUG] {start_date.strftime('%a, %d.%m.%Y')}: {service_entry}")
        if latest_date is None or start_date.date() > latest_date:
            latest_date = start_date.date()
            # print(f"[DEBUG] Latest_date: {latest_date.strftime('%d.%m.%Y')}")

            # Prüfe Laufzettelwechsel basierend auf latest_date
            if nextlaufzettel and latest_date >= nextlaufzettel.date():
                print(f"[INFO] Wechsel zu Laufzettel ab {nextlaufzettel.strftime('%d.%m.%Y')}")
                html_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                            'Laufzettel_' + nextlaufzettel.strftime('%Y%m%d') + '.html')
                laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info(html_file_path)
                next_date = getnextlaufzettel(nextlaufzettel)
                if next_date and next_date != nextlaufzettel:
                    nextlaufzettel = next_date
                    print(f"[DEBUG] Nächster Laufzettel wird sein: {nextlaufzettel.strftime('%d.%m.%Y')}")

        # Überprüfung, ob die Zelle leer oder NaN ist
        if pd.isna(service_entry) or not isinstance(service_entry, str):
            service_entry = "FT"
        else:
            service_entry = service_entry.replace('\n', ' ') \
                                         .replace('\r', ' ') \
                                         .replace('    ', ' ') \
                                         .replace('   ', ' ') \
                                         .replace('  ', ' ')
            service_entry = re.sub(
                r'(\b\d{2})\.(\d{2}\b)',
                r'\1:\2',
                service_entry
            )  # Ersetze Punkte im Zeitformat "HH.MM" durch Doppelpunkte "HH:MM"
            service_entry = re.sub(
                r'(\b\d{2}:\d{2})\s*-\s*(\d{2}:\d{2}\b)',
                r'\1 - \2',
                service_entry
            )  # Vereinheitliche das Zeitformat auf "HH:MM - HH:MM" (mit oder ohne Leerzeichen um den Bindestrich)
        # print(f"[INFO] {identifier_row[day].strftime('%a, %d.%m.%Y')}, {service_entry}")

        # Prüfe ob nextlaufzettel None ist
        # print(f"[DEBUG] Nächster Laufzettel: {nextlaufzettel}")
        if nextlaufzettel is None:
            print("[WARNING] Kein nächster Laufzettel verfügbar, verwende aktuellen weiter")
        else:
            # Prüfe ob das aktuelle oder späteste Datum den Laufzettel-Wechsel erfordert
            if (start_date.date() >= nextlaufzettel.date()):
                # print(f"[INFO] Wechsel zu Laufzettel ab {nextlaufzettel.strftime('%d.%m.%Y')}")
                html_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                            'Laufzettel_' + nextlaufzettel.strftime('%Y%m%d') + '.html')
                laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info(html_file_path)
                next_date = getnextlaufzettel(nextlaufzettel)
                if next_date and next_date != nextlaufzettel:  # Prüfe ob ein neues Datum gefunden wurde
                    nextlaufzettel = next_date
                    print(f"[DEBUG] Nächster Laufzettel wird sein: {nextlaufzettel.strftime('%d.%m.%Y')}")

        # Abfrage zur Unterscheidung zwischen ganztägigen und zeitgebundenen Terminen
        if re.search(r'\b\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\b', service_entry):
            process_timed_event(
                service_entry, start_date,
                laufzettel_werktags, laufzettel_we,
                countnightshifts, nonightshifts
            )
        else:
            process_all_day_event(service_entry, start_date)
    return nextlaufzettel, current_laufzettel

def initialize_laufzettel():
    html_files = [f for f in os.listdir(folder_path) if re.match(r'Laufzettel_\d{8}\.html', f)]
    if not html_files:
        print("[ERROR] Keine Laufzettel-Dateien im Verzeichnis gefunden")
        return None, None, None
    
    html_files.sort()
    current_laufzettel = None
    nextlaufzettel = None
    today = date.today()
    
    # print(f"[DEBUG] Mehrere HTML-Dateien gefunden: {html_files}")
    # Sammle alle Laufzettel-Daten
    laufzettel_dates = []
    for html_file in html_files:
        # print(f"[DEBUG] Laufzettel-Datei: {html_file}")
        laufzettel_datum = datetime.datetime.strptime(
            re.search(r'Laufzettel_(\d{8})\.html', html_file).group(1), 
            "%Y%m%d"
        )
        laufzettel_dates.append(laufzettel_datum)
    
    # Finde den aktuellen Laufzettel (letzter vor oder gleich heute)
    valid_current = [d for d in laufzettel_dates if d.date() <= today]
    if valid_current:
        current_laufzettel = max(valid_current)
        
        # Finde den nächsten Laufzettel (erster nach dem aktuellen)
        valid_next = [d for d in laufzettel_dates if d.date() > current_laufzettel.date()]
        if valid_next:
            nextlaufzettel = min(valid_next)
    
    if current_laufzettel:
        html_file_path = os.path.join(folder_path, f'Laufzettel_{current_laufzettel.strftime("%Y%m%d")}.html')
        print(f"[DEBUG] Aktueller Laufzettel: {current_laufzettel.strftime('%d.%m.%Y')}")
        if nextlaufzettel:
            print(f"[DEBUG] Nächster Laufzettel ab: {nextlaufzettel.strftime('%d.%m.%Y')}")
        laufzettel_werktags, laufzettel_we = parse_html_for_workplace_info(html_file_path)
        return current_laufzettel, nextlaufzettel, (laufzettel_werktags, laufzettel_we)
    return None, None, None


def getnextlaufzettel(nextlaufzettel):
    """Bestimmt den chronologisch nächsten verfügbaren Laufzettel."""
    if nextlaufzettel is None:
        print("[WARNING] Eingabe-Laufzettel ist None")
        return None

    html_files = [f for f in os.listdir(folder_path) if re.match(r'Laufzettel_\d{8}\.html', f)]
    if not html_files:
        print("[WARNING] Keine Laufzettel-Dateien gefunden")
        return nextlaufzettel

    # Sammle alle Laufzettel-Daten
    laufzettel_dates = []
    for html_file in html_files:
        try:
            laufzettel_datum = datetime.datetime.strptime(
                re.search(r'Laufzettel_(\d{8})\.html', html_file).group(1), 
                "%Y%m%d"
            )
            laufzettel_dates.append(laufzettel_datum)
        except Exception as e:
            print(f"[ERROR] Fehler beim Parsen des Datums aus {html_file}: {e}")
            continue

    # Sortiere die Daten chronologisch
    laufzettel_dates.sort()
    
    # Finde den nächsten Laufzettel nach dem aktuellen
    valid_next = [d for d in laufzettel_dates if d.date() > nextlaufzettel.date()]
    if valid_next:
        next_date = min(valid_next)
        # print(f"[DEBUG] Gefunden: Nächster Laufzettel ab {next_date.strftime('%d.%m.%Y')}")
        return next_date
    else:
        # print(f"[DEBUG] Kein weiterer Laufzettel gefunden")
        nextlaufzettel = None
    
    # print(f"[DEBUG] Kein weiterer Laufzettel gefunden, behalte aktuellen ({nextlaufzettel.strftime('%d.%m.%Y')})")
    return None


def extract_date(entry):
    # Suche nach dem Datum im Format TT.MM.JJJJ
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', entry)
    if match:
        date_str = match.group(1)
        # Datum in das Format JJJJMMTT umwandeln, um lexikografische Sortierung zu ermöglichen
        return date_str[6:] + date_str[3:5] + date_str[:2]  # Format: JJJJMMTT
    return None


def send_email(subject, body, to_email, kalender_id):
    from_email = load_credentials("notifymail", config_path)
    from_display_name = "Dein Dienstplan"
    password = load_credentials("mailpassword", config_path)
    kalenderbase = load_credentials("kalenderbase", config_path)
    abobase = load_credentials("abobase", config_path)
    kalenderurls = (
        f'<a href="{kalenderbase}{kalender_id}">Kalender</a><br>'
        f'<a href="{abobase}{kalender_id}?export">Abo-URL</a><br>Alle Angaben und Inhalte sind ohne Gewähr.'
    )
    body += "<br><br>" + kalenderurls
    msg = MIMEMultipart()
    msg['From'] = f"{from_display_name} <{from_email}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    try:
        server = smtplib.SMTP('smtp.ionos.de', 587)
        server.starttls()
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        print(f"[INFO] E-Mail an {to_email} gesendet.")
    except Exception as e:
        print(f"[ERROR] Fehler beim Senden der E-Mail: {e}")


def load_email_config(user_name, email_config_path):  # Funktion zum Laden der E-Mail-Konfiguration
    try:
        with open(email_config_path, 'r') as file:
            email_config = json.load(file)

        if user_name in email_config:
            return email_config[user_name]
        else:
            raise ValueError(f"Keine E-Mail-Konfiguration für '{user_name}' gefunden.")
    except FileNotFoundError:
        raise FileNotFoundError(f"Die Datei '{email_config_path}' wurde nicht gefunden.")
    except Exception as e:
        raise ValueError(f"Fehler beim Laden der E-Mail-Konfiguration: {e}")


def load_credentials(service_name, config_path):
    # Laden der JSON-Datei
    with open(config_path, 'r') as file:
        config = json.load(file)

    # Der Schlüssel in der config.json entspricht direkt dem service_name (z.B. "username_mm" oder "password_mm")
    if not "_" in service_name:
        if service_name in config:
            return config[service_name]  # Nur das Passwort zurückgeben
        else:
            raise ValueError(f"Config für '{service_name}' nicht gefunden.")
    else:
        # Allgemeiner Fall für andere Dienste
        username_key = f"username_{service_name}"
        password_key = f"password_{service_name}"

        # Überprüfen, ob der Benutzername und das Passwort existieren
        if username_key in config and password_key in config:
            username = config[username_key]
            password = config[password_key]
            return username, password
        else:
            raise ValueError(f"Benutzername oder Passwort für '{service_name}' nicht gefunden.")


def extract_date_from_filename(filename):
    # Regulärer Ausdruck für die Datumsangaben im Dateinamen
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4}) - (\d{1,2})\.(\d{1,2})\.(\d{2,4})', filename)
    if match:
        start_day = int(match.group(1))
        start_month = int(match.group(2))
        start_year = int(match.group(3))
        if start_year < 100:
            start_year += 2000
        start_date = dt.datetime(start_year, start_month, start_day)  # Verwende 'dt.datetime'
        return start_date
    else:
        return None  # Falls das Datum nicht gefunden wird


# Main
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
tz_berlin = pytz.timezone('Europe/Berlin')
script_path = os.path.abspath(__file__)
folder_path = os.path.dirname(script_path)
# print(f"[DEBUG] folder_path: {folder_path}")
email_config_path = os.path.join(folder_path, 'email_config.json')
config_path = os.path.join(folder_path, 'config.json')
# print(f"[DEBUG] Config: {config_path}")
night_shifts_count = 0
countnightshifts = False  # Initialisierung vor der Verarbeitung
changedlaufzettel = False

parser = argparse.ArgumentParser(description="Dienst zu ARD ZDF Box Script.")
parser.add_argument("user_name", help="Name des Benutzers", type=str)
parser.add_argument("-c", "--calendar", help="Dienste in MeiersMarkus eintragen", action="store_true")
parser.add_argument("-o", "--ort", help="Ort als Adresse hinzufügen", action="store_true")
parser.add_argument("-r", "--rewrite", help="Alle vorhandenen Termine im Zeitbereich neu erstellen", action="store_true")
parser.add_argument("-n", "--notify", help="E-Mail mit eingetragenen Diensten an Benutzer senden", action="store_true")
parser.add_argument("-s", "--nas", help="Direkt aufs NAS", action="store_true")
parser.add_argument("-d", "--dienste", help="Nur Dienste eintragen", action="store_true")

args = parser.parse_args()
user_name = args.user_name
meiersmarkus = args.calendar
ort = args.ort
rewrite = args.rewrite
notify = args.notify
nas = args.nas
dienste = args.dienste

if meiersmarkus:
    service_name = "mm"
elif nas:
    service_name = "nas"
else:
    service_name = "ard"

caldavlogin = "caldav" + service_name
caldav_start = load_credentials(caldavlogin, config_path)
calendar_name = 'Dienstplan ' + user_name.replace(',', '').replace('.', '')
caldav_url = caldav_start + 'dienstplan-' + user_name.lower().replace(' ', '-').replace(',', '').replace('.', '') + '/'

# print(f"[DEBUG] Kalendername: {calendar_name}")
# print(f"[DEBUG] Kalender-URL: {caldav_url}")
# print(f"[DEBUG] Download? {'Ja' if should_download else 'Nein'}")

options = [
    "'-c'" if meiersmarkus else None,
    "'-o'" if ort else None,
    "'-r'" if rewrite else None,
    "'-n'" if notify else None,
    "'-s'" if nas else None,
    "'-d'" if dienste else None
]
options = [opt for opt in options if opt]
if options:
    print(f"[DEBUG] Benutzername: {user_name}, " + ", ".join(options) + "")
else:
    print(f"[DEBUG] Benutzername: {user_name}")

current_year = date.today().year
years = [current_year - 1, current_year, current_year + 1]
de_holidays = holidays.Germany(years=years, observed=False, prov="HH", language="de")
for year in years:
    de_holidays[date(year, 10, 31)] = "Reformationstag"  # Reformationstag
    de_holidays[date(year, 12, 24)] = "Heiligabend"  # Heiligabend
    de_holidays[date(year, 12, 31)] = "Silvester"    # Silvester
    de_holidays[easter(year)] = "Ostersonntag"       # Ostersonntag
    de_holidays[easter(year) + timedelta(days=49)] = "Pfingstsonntag"  # Pfingstsonntag
# for holiday_date, holiday_name in de_holidays.items():
#    print(f"{holiday_date.strftime('%d.%m.%Y')}: {holiday_name}")

start_timer("caldav")
try:
    # print(f"[DEBUG] Verbinde mit CalDAV-Server '{service_name}'...")
    login_service = "login_" + service_name
    username, password = load_credentials(login_service, config_path)
    # print(f"[DEBUG] Username und Passwort geladen: {username}")
    client = DAVClient(caldav_start, username=username, password=password)
    principal = client.principal()
    try:
        calendar = principal.calendar(name=calendar_name)
    except Exception as e:
        print(f"[ERROR] Kalender nicht gefunden: {e}")
        if not nas:
            # print(f"[ERROR] Dienstplan-Kalender nicht gefunden. Versuche, einen neuen Kalender zu erstellen...")
            # new_calendar = principal.make_calendar(calendar_name)
            # new_calendar.save()  # Save the newly created calendar to the server
            print(f"[DEBUG] Kalender '{calendar_name}' nicht gefunden.")
            # calendar = new_calendar

    # Erfolgreiche Verbindung herstellen, falls Kalender gefunden oder erstellt wurde
    if not calendar:
        print(f"[ERROR] Es konnte keine Verbindung zum Kalender '{calendar_name}' hergestellt werden.")

except Exception as e:
    print(f"[ERROR] CalDAV-Verbindung fehlgeschlagen oder Fehler bei der Kalendererstellung: {e}")
    sys.exit(1)
end_timer("caldav", "Verbindung zu CalDAV")

eingetragene_termine = []
target_folder = os.path.join(folder_path, "Plaene", "MAZ_TAZ Dienstplan")
current_laufzettel, nextlaufzettel, laufzettel_data = initialize_laufzettel()
if not current_laufzettel:
    print("[ERROR] Kein gültiger Laufzettel gefunden")
    sys.exit(1)

laufzettel_werktags, laufzettel_we = laufzettel_data
end_timer("initial", "Initialisierung")
# print(f"[DEBUG] Absoluter Pfad zum Skript: {script_path}")
# print(f"[DEBUG] Absoluter Pfad zum Zielordner: {target_folder}")

xlsx_files = [
    os.path.join(root, f)
    for root, _, files in os.walk(target_folder)
    for f in files if f.endswith('.xlsx')
]
with_date, without_date = [], []
for file in xlsx_files:
    date = extract_date_from_filename(os.path.basename(file))
    (with_date if date else without_date).append((file, date) if date else file)
with_date.sort(key=lambda x: x[1])
xlsx_files = list(chain((f[0] for f in with_date), without_date))

if xlsx_files:
    # print(f"[DEBUG] Anzahl der gefundenen .xlsx-Dateien in {root}: {len(xlsx_files)}")
    # print(f"[DEBUG] Anzahl der .xlsx-Dateien: {len(xlsx_files)}")
    for file_path in xlsx_files:
        file_name = os.path.basename(file_path)
        # start_timer("xlsx")
        # print(f"[DEBUG] Verarbeite Datei: {file_name}")
        nextlaufzettel, current_laufzettel = process_excel_file(
            file_path, user_name, laufzettel_werktags, laufzettel_we, countnightshifts, nextlaufzettel, current_laufzettel
        )
        # end_timer("xlsx", f"Verarbeitung der Excel-Datei {file_name}")
else:
    print("[DEBUG] Keine .xlsx-Dateien gefunden.")
# eingetragene_termine.sort(key=lambda x: extract_date(x))
if eingetragene_termine and notify:
    print(f"[INFO] {len(eingetragene_termine)} neue Termine eingetragen.")
    eingetragene_termine_wochentag = []
    for term in eingetragene_termine:
        split_term = term.split(":")
        split_term = split_term[0].split(" ")
        wochentag = pd.to_datetime(split_term[0], format='%d.%m.%Y').strftime('%a')[:2] + '.'
        # print(f"[INFO] {wochentag} {term}")
        # Wochentag vor jedes Datum einfügen und in die Liste eingetragene_termine_wochentag schreiben
        eingetragene_termine_wochentag += [f"{wochentag} {term}"]
    # start_timer("mail")
    # Zusammenstellung der eingetragenen Termine
    mail_body = "Es wurden folgende Termine eingetragen:<br><br>"
    mail_body += "<br>".join(eingetragene_termine_wochentag)
    # Laden der E-Mail-Konfiguration
    try:
        email_config = load_email_config(user_name, email_config_path)
        send_email(
            subject=f"Dienstplan Update {user_name}",
            body=mail_body,
            to_email=email_config["email"],
            kalender_id=email_config["kalender_id"]
        )
    except Exception as e:
        print(f"[ERROR] Fehler bei der Benachrichtigung: {e}")
    # end_timer("mail", "Mail")
elif eingetragene_termine and not notify:
    print(f"[INFO] {len(eingetragene_termine)} neue Termine eingetragen.")
# else:
#    print("[INFO] Keine neuen Termine eingetragen.")
end_timer("gesamt", "Zeit")
