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

Um das Skript mit der Option zum Erzwingen des Downloads auszuführen:

```sh
python DienstplanStart.py -f
```

Um das Skript mit dem Benutzernamen und weiteren Optionen auszuführen:

```sh
python DienstzuARDZDFBox.py user_name -c -o -r -n -s -d
```

## Protokollierung

Das Skript protokolliert seine Ausgabe sowohl in eine Protokolldatei (`Dienstplanscript.log`) als auch auf die Konsole. Die Protokolldatei verwendet einen rotierenden Datei-Handler, um die Dateigröße und Backups zu verwalten.

## Timer-Funktionalität

Das Skript enthält eine Timer-Funktionalität, um die für verschiedene Aufgaben benötigte Zeit zu messen. Die Gesamtdauer wird am Ende der Skriptausführung protokolliert.
