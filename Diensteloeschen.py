import sys
import caldav
import json
import os
from datetime import datetime, timedelta
from caldav.elements import dav, cdav
from caldav import DAVClient

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


def delete_events(username, year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31, 23, 59, 59)
    events = calendar.date_search(start=start_date, end=end_date)
    
    if not events:
        print(f"Keine Termine für das Jahr {year} gefunden.")
        return
    
    print(f"Termine im Jahr {year} werden aus '{calendar_name}' gelöscht...")
    for event in events:
        vevent = event.icalendar_instance
        for component in vevent.walk('VEVENT'):
            summary = component.get('SUMMARY', 'Ohne Titel')
            dtstart = component.get('DTSTART')
            dtstart = dtstart.dt if dtstart else "Unbekanntes Datum"
            #print(f"{dtstart}: {summary}")
    
    deleted_count = sum(1 for event in events if not event.delete())
    print(f"{deleted_count} Termine wurden gelöscht.")

# Main
script_path = os.path.abspath(__file__)
folder_path = os.path.dirname(script_path)
# print(f"[DEBUG] folder_path: {folder_path}")
config_path = os.path.join(folder_path, 'config.json')
user_name = sys.argv[1]
if not sys.argv[2].isdigit():
    print("Fehler: Jahr muss eine Zahl sein.")
    sys.exit(1)
year = int(sys.argv[2])

service_name = "ard"
caldavlogin = "caldav" + service_name
caldav_start = load_credentials(caldavlogin, config_path)
calendar_name = 'Dienstplan ' + user_name.replace(',', '').replace('.', '')
caldav_url = caldav_start + 'dienstplan-' + user_name.lower().replace(' ', '-').replace(',', '').replace('.', '') + '/'

print(f"[DEBUG] Kalendername: {calendar_name}")
print(f"[DEBUG] Kalender-URL: {caldav_url}")

try:
    # print(f"[DEBUG] Verbinde mit CalDAV-Server '{service_name}'...")
    login_service = "login_" + service_name
    username, password = load_credentials(login_service, config_path)
    # print(f"[DEBUG] Username und Passwort geladen: {username}")
    client = DAVClient(caldav_start, username=username, password=password)
    principal = client.principal()
    try:
        calendar = principal.calendar(name=calendar_name)
        print(f"[DEBUG] Kalender {calendar} gefunden.")
    except Exception as e:
        print(f"[ERROR] Kalender nicht gefunden: {e}")

except Exception as e:
    print(f"[ERROR] CalDAV-Verbindung fehlgeschlagen oder Fehler bei der Kalendererstellung: {e}")
    sys.exit(1)


delete_events(user_name, year)
