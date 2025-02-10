# Dienstplan Excel

Dieses Projekt automatisiert den Prozess des Herunterladens und Aktualisierens von Kalendern für Kollegen aus der ARD-ZDF Box.

## Funktionen

- Lädt die neuesten Kalenderdaten herunter.
- Aktualisiert Kalender für mehrere Kollegen.
- Protokolliert den Prozess in eine Datei und auf die Konsole.
- Misst die Zeit, die für den gesamten Prozess benötigt wird.

## Anforderungen

- Python 3.x
- Erforderliche Python-Pakete (mit `pip install -r requirements.txt` installieren)
- Eine config.json mit den notwendigen Login-Daten
- Eine colleagues.json, sofern DienstplanStart.py genutzt wird, um mehrere Kalender zu aktualisieren

## Verwendung

### Befehlszeilenoptionen

- `user_name`: Name des Benutzers (erforderlich)
- `-c`, `--calendar`: Dienste in MeiersMarkus eintragen.
- `-o`, `--ort`: Ort als Adresse hinzufügen.
- `-r`, `--rewrite`: Alle vorhandenen Termine im Zeitbereich neu erstellen.
- `-n`, `--notify`: E-Mail mit eingetragenen Diensten an Benutzer senden.
- `-s`, `--nas`: Direkt aufs NAS.
- `-d`, `--dienste`: Nur Dienste eintragen.

### Beispiel

Optionen zum Erzwingen des Updates (-f) oder ohne Download (-n) auszuführen:

```sh
python DienstplanStart.py -f -n
```

Um das Skript mit dem Benutzernamen und weiteren Optionen auszuführen:

```sh
python DienstzuARDZDFBox.py user_name -c -o -r -n -s -d
```

## Protokollierung

Das Skript protokolliert seine Ausgabe sowohl in eine Protokolldatei (`Dienstplanscript.log`) als auch auf die Konsole. Die Protokolldatei verwendet einen rotierenden Datei-Handler, um die Dateigröße und Backups zu verwalten.

## Timer-Funktionalität

Das Skript enthält eine Timer-Funktionalität, um die für verschiedene Aufgaben benötigte Zeit zu messen. Die Gesamtdauer wird am Ende der Skriptausführung protokolliert.

## Aufbau der json Dateien

config.json
```sh
{
  "username_login_ard": "mailadresse",
  "password_login_ard": "passwort",
  "mailpassword": "mailpasswort",
  "ardbox": "####",
  "downloadordner": "####",
  "kalenderbase": "####",
  "abobase": "####",
  "caldavard": "####",
  "notifymail": "mailadresse",
  "user1": "Beispielnutzer",
  "user2": "Beispielnutzer2"
}
```

colleagues.json
```sh
{
    "colleagues": [
        ["user_name", ["-o", "-n"]],
        ["user_name2", ["-o"]],
        ["user_name3", []]
    ]
}
```
