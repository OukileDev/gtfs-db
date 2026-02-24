import hashlib
import io
import os
import sys
import zipfile

import requests
from dotenv import load_dotenv

# Chargement du .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Ajout du dossier functions/ au path pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'functions'))

from functions.lines import get_cleaned_lines
from functions.realtime import generate_trip_updates
from functions.schedules import generate_all_schedules
from functions.shapes import generate_shapes
from functions.stops import generate_all_stops

GTFS_URL = os.getenv("GTFS_URL")


def fetch_gtfs():
    print("=== Téléchargement du GTFS ===")
    r = requests.get(GTFS_URL)
    file_hash = hashlib.sha256(r.content).hexdigest()
    print(f"Hash SHA256 : {file_hash}")

    gtfs_data_dir = os.path.join(os.path.dirname(__file__), 'data', 'gtfs_data')
    os.makedirs(gtfs_data_dir, exist_ok=True)

    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(gtfs_data_dir)
    print(f"Données extraites dans data/gtfs_data/")


if __name__ == "__main__":
    fetch_gtfs()

    print("\n=== Arrêts ===")
    generate_all_stops()

    print("\n=== Shapes ===")
    generate_shapes()

    print("\n=== Réseau nettoyé ===")
    get_cleaned_lines()

    print("\n=== Plannings ===")
    generate_all_schedules()

    print("\n=== Temps réel ===")
    generate_trip_updates()

    print("\n✅ Pipeline terminé.")
