"""Zentrale Konfiguration – wird einmal geladen und überall verwendet."""

import json
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


@dataclass
class ColleagueConfig:
    """Konfiguration eines einzelnen Kollegen."""
    name: str
    use_mm_calendar: bool = False      # -c
    add_location: bool = False         # -o
    send_notification: bool = False    # -n
    rewrite: bool = False              # -r
    use_nas: bool = False              # -s
    only_shifts: bool = False          # -d

    @classmethod
    def from_json_entry(cls, entry: list) -> "ColleagueConfig":
        """Erzeugt ColleagueConfig aus einem colleagues.json-Eintrag wie ["Name", ["-c", "-o"]]."""
        name = entry[0]
        flags = set(entry[1]) if len(entry) > 1 else set()
        return cls(
            name=name,
            use_mm_calendar="-c" in flags,
            add_location="-o" in flags,
            send_notification="-n" in flags,
            rewrite="-r" in flags,
            use_nas="-s" in flags,
            only_shifts="-d" in flags,
        )

    @property
    def service_name(self) -> str:
        if self.use_mm_calendar:
            return "mm"
        elif self.use_nas:
            return "nas"
        return "ard"


@dataclass
class CalDAVCredentials:
    """CalDAV-Verbindungsdaten für einen Service."""
    base_url: str
    username: str
    password: str


@dataclass
class EmailEntry:
    """E-Mail-Konfiguration für einen Kollegen."""
    email: str
    kalender_id: str


class AppConfig:
    """Zentrale Konfiguration – lädt alle JSON-Dateien einmal beim Start.

    Verwendung:
        config = AppConfig("/pfad/zum/projektordner")
        creds = config.get_caldav_credentials("ard")
        colleagues = config.colleagues
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self._raw: Dict = {}
        self._colleagues: List[ColleagueConfig] = []
        self._email_config: Dict[str, EmailEntry] = {}
        self._load_all()

    def _load_all(self):
        """Lädt alle Konfigurationsdateien."""
        # config.json
        config_path = os.path.join(self.base_dir, "config.json")
        with open(config_path, "r", encoding="utf-8") as f:
            self._raw = json.load(f)

        # colleagues.json
        colleagues_path = os.path.join(self.base_dir, "colleagues.json")
        if os.path.exists(colleagues_path):
            with open(colleagues_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._colleagues = [
                ColleagueConfig.from_json_entry(entry)
                for entry in data.get("colleagues", [])
            ]

        # email_config.json
        email_path = os.path.join(self.base_dir, "email_config.json")
        if os.path.exists(email_path):
            with open(email_path, "r", encoding="utf-8") as f:
                raw_email = json.load(f)
            self._email_config = {
                name: EmailEntry(email=v["email"], kalender_id=v["kalender_id"])
                for name, v in raw_email.items()
            }

        logger.debug(
            "Config geladen: %d Kollegen, %d E-Mail-Einträge",
            len(self._colleagues), len(self._email_config),
        )

    # --- Zugriffsmethoden ---

    @property
    def colleagues(self) -> List[ColleagueConfig]:
        return self._colleagues

    def get_raw(self, key: str) -> str:
        """Direkter Zugriff auf einen config.json-Schlüssel."""
        if key not in self._raw:
            raise KeyError(f"Config-Schlüssel '{key}' nicht gefunden.")
        return self._raw[key]

    def get_caldav_credentials(self, service: str) -> CalDAVCredentials:
        """Gibt CalDAV-URL + Login für einen Service zurück (ard/mm/nas)."""
        base_url = self._raw.get(f"caldav{service}", "")
        username = self._raw.get(f"username_login_{service}", "")
        password = self._raw.get(f"password_login_{service}", "")
        if not all([base_url, username, password]):
            raise ValueError(f"Unvollständige CalDAV-Credentials für Service '{service}'.")
        return CalDAVCredentials(base_url=base_url, username=username, password=password)

    def get_email_entry(self, user_name: str) -> Optional[EmailEntry]:
        """Gibt die E-Mail-Config für einen Kollegen zurück, oder None."""
        return self._email_config.get(user_name)

    @property
    def download_url(self) -> str:
        return self.get_raw("downloadordner")

    @property
    def server_check_url(self) -> str:
        return self.get_raw("ardbox")

    @property
    def smtp_email(self) -> str:
        return self.get_raw("notifymail")

    @property
    def smtp_password(self) -> str:
        return self.get_raw("mailpassword")

    @property
    def kalender_base_url(self) -> str:
        return self.get_raw("kalenderbase")

    @property
    def abo_base_url(self) -> str:
        return self.get_raw("abobase")

    @property
    def user1_name(self) -> str:
        return self.get_raw("user1")

    @property
    def user2_name(self) -> str:
        return self.get_raw("user2")

    def get_delete_whitelist(self) -> List[str]:
        """Lädt die Whitelist für das Löschen alter Termine."""
        path = os.path.join(self.base_dir, "deletewhitelist.json")
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [entry[0] for entry in data.get("colleagues", [])]
