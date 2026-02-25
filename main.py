#!/usr/bin/env python3
"""Dienstplan -> CalDAV - Hauptskript.

Ersetzt DienstplanStart.py als einziger Einstiegspunkt.
Orchestriert Download, Verarbeitung und Benachrichtigung.
"""

import argparse
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from config import AppConfig, ColleagueConfig
from cleaner import delete_old_entries
from downloader import DownloadResult, download_plans
from holidays_de import GermanHolidays
from laufzettel import LaufzettelManager
from shift_processor import process_colleague
from utils import Timer, setup_logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_args():
    parser = argparse.ArgumentParser(description="Dienstplan -> CalDAV Sync")
    parser.add_argument("-f", "--force", action="store_true",
                        help="Erzwinge Download aller Dateien und Neuschreiben")
    parser.add_argument("-n", "--no-download", action="store_true",
                        help="Kein Download, direkt verarbeiten")
    parser.add_argument("--delete", action="store_true",
                        help="Alte Termine loeschen (2 Jahre zurueck)")
    parser.add_argument("--single", type=str, default=None, metavar="NAME",
                        help="Nur einen einzelnen Kollegen verarbeiten")
    parser.add_argument("-c", "--calendar", action="store_true",
                        help="(single) MeiersMarkus-Kalender")
    parser.add_argument("-o", "--ort", action="store_true",
                        help="(single) Ort als Adresse")
    parser.add_argument("-r", "--rewrite", action="store_true",
                        help="(single) Alle Termine neu schreiben")
    parser.add_argument("-s", "--nas", action="store_true",
                        help="(single) NAS-Kalender")
    parser.add_argument("-d", "--dienste", action="store_true",
                        help="(single) Nur Dienste eintragen")
    parser.add_argument("--notify", action="store_true",
                        help="(single) E-Mail-Benachrichtigung senden")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Ausfuehrliche Konsolenausgabe (DEBUG)")
    return parser.parse_args()


def run_delete_mode(app_config):
    """Loescht alte Eintraege fuer alle Kollegen (ausser Whitelist)."""
    whitelist = set(app_config.get_delete_whitelist())
    colleagues = [c for c in app_config.colleagues if c.name not in whitelist]
    logger.info("Loesche alte Eintraege fuer %d Kollegen...", len(colleagues))

    cpu_count = os.cpu_count() or 1
    workers = max(1, math.floor(cpu_count * 0.5))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(delete_old_entries, app_config, c.name): c.name
            for c in colleagues
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("Fehler beim Loeschen fuer %s: %s", name, e)


def run_update_mode(app_config, force=False):
    """Aktualisiert Kalender fuer alle Kollegen parallel."""
    # Gemeinsame Ressourcen einmal laden
    with Timer("Laufzettel + Feiertage laden"):
        holidays = GermanHolidays()
        laufzettel_mgr = LaufzettelManager(BASE_DIR)

    plans_folder = os.path.join(BASE_DIR, "Plaene", "MAZ_TAZ Dienstplan")
    colleagues = app_config.colleagues

    logger.info("Verarbeite %d Kollegen...", len(colleagues))

    cpu_count = os.cpu_count() or 1
    workers = max(1, math.floor(cpu_count * 0.5))
    logger.info("CPUs: %d, Threads: %d", cpu_count, workers)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_colleague,
                app_config, c, laufzettel_mgr, holidays, plans_folder,
            ): c.name
            for c in colleagues
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("Fehler bei %s: %s", name, e, exc_info=True)


def run_single_mode(app_config, args):
    """Verarbeitet einen einzelnen Kollegen (Debug-Modus)."""
    colleague = ColleagueConfig(
        name=args.single,
        use_mm_calendar=args.calendar,
        add_location=args.ort,
        send_notification=args.notify,
        rewrite=args.rewrite,
        use_nas=args.nas,
        only_shifts=args.dienste,
    )

    holidays = GermanHolidays()
    laufzettel_mgr = LaufzettelManager(BASE_DIR)
    plans_folder = os.path.join(BASE_DIR, "Plaene", "MAZ_TAZ Dienstplan")

    process_colleague(app_config, colleague, laufzettel_mgr, holidays, plans_folder)


def main():
    args = parse_args()
    console_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(BASE_DIR, console_level=console_level)

    with Timer("Gesamtdauer", log_threshold_seconds=0):
        app_config = AppConfig(BASE_DIR)

        if args.delete:
            run_delete_mode(app_config)
            return

        if args.single:
            run_single_mode(app_config, args)
            return

        if not args.no_download:
            with Timer("Download"):
                fast = not args.force
                result = download_plans(app_config, BASE_DIR, fast=fast)

            if result == DownloadResult.CONNECTION_ERROR:
                logger.error("Server nicht erreichbar. Abbruch.")
                sys.exit(2)

            if result == DownloadResult.NO_CHANGES and not args.force:
                logger.debug("Keine Aenderungen festgestellt.")
                return

        run_update_mode(app_config, force=args.force)


if __name__ == "__main__":
    main()
