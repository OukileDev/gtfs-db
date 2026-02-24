import json
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'all_stops.json')


def generate_all_stops():
    """
    Génère all_stops.json : la liste de tous les arrêts avec leurs coordonnées.
    """
    print("--- Génération de la liste des arrêts ---")

    stops = pd.read_csv(os.path.join(DATA_DIR, 'stops.txt'))

    all_stops = []

    for _, row in stops.iterrows():
        # On filtre les arrêts qui n'ont pas de coordonnées valides (sécurité)
        if pd.isna(row['stop_lat']) or pd.isna(row['stop_lon']):
            continue

        all_stops.append({
            "id": str(row['stop_id']),
            "name": row['stop_name'],
            "lat": float(row['stop_lat']),
            "lon": float(row['stop_lon'])
        })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_stops, f, indent=4, ensure_ascii=False)

    print(f"Terminé ! {len(all_stops)} arrêts enregistrés dans '{OUTPUT_FILE}'.")
