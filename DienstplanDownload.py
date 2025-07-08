import argparse
import os
import sys
import requests
import shutil
import zipfile
import time
import datetime as dt
import json


def get_latest_modification_date(folder_path):
    """Findet das neueste Änderungsdatum aller .xlsx-Dateien im angegebenen Verzeichnis."""
    latest_mod_time = 0
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".xlsx"):
                file_path = os.path.join(root, file)
                mod_time = os.path.getmtime(file_path)
                if mod_time > latest_mod_time:
                    latest_mod_time = mod_time
    return dt.datetime.fromtimestamp(latest_mod_time) if latest_mod_time > 0 else None


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


def download_dienste(folder_path):
    # Analyse des letzten Änderungsdatums vor dem Download
    original_latest_date = get_latest_modification_date(folder_path)
    server_url = load_credentials("ardbox", config_path)
    if check_server_connection(server_url):
        plaene_dir = os.path.join(folder_path, "Plaene")
        zip_path = os.path.join(folder_path, "Plaene.zip")
        url = load_credentials("downloadordner", config_path)

        # Existierendes Verzeichnis und ZIP-Datei löschen
        shutil.rmtree(plaene_dir, ignore_errors=True)
        os.makedirs(plaene_dir, exist_ok=True)

        # ZIP-Datei herunterladen und speichern
        response = requests.get(url)
        with open(zip_path, 'wb') as file:
            file.write(response.content)

        # ZIP-Datei entpacken und .xlsx-Dateien extrahieren, Metadaten beibehalten
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith(".xlsx"):
                    zip_ref.extract(file_info, plaene_dir)
                    extracted_file_path = os.path.join(plaene_dir, file_info.filename)

                    # Änderungsdatum setzen
                    mod_time = time.mktime(file_info.date_time + (0, 0, -1))
                    os.utime(extracted_file_path, (mod_time, mod_time))

        os.remove(zip_path)

        # Analyse des neuesten Änderungsdatums nach dem Download
        new_latest_date = get_latest_modification_date(plaene_dir)

        # Vergleich der Änderungsdaten und Exit-Status
        if original_latest_date and new_latest_date and original_latest_date == new_latest_date:
            # print("[DEBUG] Keine Änderungen festgestellt.")
            if fast and original_latest_date:
                deleted_count = delete_old_files(original_latest_date, plaene_dir)
                # print(f"[DEBUG] Schneller Modus aktiviert. '{deleted_count}' Excel-Dateien entfernt.")
            sys.exit(1)  # Beende das Skript ohne Änderungen
        else:
            # print("[DEBUG] Neue Änderungen festgestellt.")
            if fast and original_latest_date:
                deleted_count = delete_old_files(original_latest_date, plaene_dir)
                print(f"[DEBUG] Schneller Modus aktiviert. '{deleted_count}' Excel-Dateien entfernt.")
            sys.exit(0)  # Beende das Skript mit Änderungen
    else:
        print("[DEBUG] Verbindung fehlgeschlagen. Download nicht möglich.")
        sys.exit(2)  # Fehlercode bei Verbindungsfehler

def delete_old_files(original_latest_date, plaene_dir):
    # Prüfe jede Datei im Verzeichnis auf ein neues Änderungsdatum und lösche Dateien, die älter sind als original_latest_date
    for root, _, files in os.walk(plaene_dir):
        deleted_count = sum(1 for file in files if file.endswith(".xlsx") and os.path.getmtime(os.path.join(root, file)) < original_latest_date.timestamp())
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".xlsx"):
                mod_time = os.path.getmtime(file_path)
                # print(f"[DEBUG] Überprüfe Datei: {file}, Änderungsdatum: {dt.datetime.fromtimestamp(mod_time)} gegenüber {original_latest_date}")
                if original_latest_date and dt.datetime.fromtimestamp(mod_time) < original_latest_date:
                    # print(f"[DEBUG] Lösche alte Datei: {file}")
                    os.remove(file_path)
    return deleted_count

def check_server_connection(url):
    """Überprüft, ob der Server erreichbar ist."""
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dienstplan Download Script")
    parser.add_argument("-f", "--fast", help="Nur neue Excel-Dateien", action="store_true")
    args = parser.parse_args()
    fast = args.fast
    script_path = os.path.abspath(__file__)
    folder_path = os.path.dirname(script_path)
    config_path = os.path.join(folder_path, 'config.json')
    download_dienste(folder_path)
