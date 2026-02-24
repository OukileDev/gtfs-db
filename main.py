import argparse
import hashlib
import io
import logging
import os
import sys
import zipfile
from datetime import datetime

import requests
from dotenv import load_dotenv

# Chargement du .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Ajout du dossier functions/ au path pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'functions'))

from db.connection import (
    apply_gtfs_schema,
    create_database_if_not_exists,
    get_active_version,
    get_connection,
    register_version,
    setup_registry,
)
from functions.lines import get_cleaned_lines
from functions.schedules import generate_all_schedules
from functions.shapes import generate_shapes
from functions.stops import generate_all_stops

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GTFS_URL = os.getenv("GTFS_URL")


def fetch_gtfs() -> tuple[str, bytes]:
    """Télécharge le GTFS et retourne (sha256, contenu brut)."""
    log.info("Téléchargement du GTFS...")
    r = requests.get(GTFS_URL)
    sha256 = hashlib.sha256(r.content).hexdigest()
    log.info(f"Hash SHA256 : {sha256}")
    return sha256, r.content


def extract_gtfs(raw: bytes, db_name: str):
    gtfs_data_dir = os.path.join(os.path.dirname(__file__), 'data', 'gtfs_data')
    os.makedirs(gtfs_data_dir, exist_ok=True)
    zipfile.ZipFile(io.BytesIO(raw)).extractall(gtfs_data_dir)
    log.info("Données extraites dans data/gtfs_data/")


def run_daily():
    """
    Pipeline quotidien : télécharge le GTFS, compare le hash,
    crée la base et importe les données si nécessaire.
    """
    log.info("=== Pipeline quotidien ===")

    sha256, raw = fetch_gtfs()

    active_db, active_sha256 = get_active_version()
    if active_sha256 == sha256:
        log.info("GTFS inchangé (même hash), rien à faire.")
        return

    db_name = f"gtfs_{datetime.now().strftime('%Y%m%d')}"
    log.info(f"Nouveau GTFS détecté → base cible : {db_name}")

    create_database_if_not_exists(db_name)
    extract_gtfs(raw, db_name)
    apply_gtfs_schema(db_name)

    with get_connection(db_name) as conn:
        log.info("Arrêts...")
        generate_all_stops(conn)

        log.info("Réseau nettoyé...")
        get_cleaned_lines(conn)

        log.info("Plannings...")
        generate_all_schedules(conn)

    log.info("Shapes...")
    generate_shapes()

    register_version(db_name, sha256)
    log.info(f"✅ Pipeline quotidien terminé → {db_name}")


def run_realtime():
    """Mise à jour du temps réel."""
    try:
        generate_trip_updates()
    except Exception as e:
        log.error(f"Erreur temps réel : {e}")


if __name__ == "__main__":
    setup_registry()
    run_daily()
