import json
import os
import urllib.request

import pandas as pd
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')

load_dotenv(os.path.join(BASE_DIR, '.env'))

BUNNY_STORAGE_HOSTNAME = os.getenv("BUNNY_STORAGE_HOSTNAME")
BUNNY_STORAGE_ZONE     = os.getenv("BUNNY_STORAGE_ZONE")
BUNNY_API_KEY          = os.getenv("BUNNY_API_KEY")
BUNNY_PATH_PREFIX      = os.getenv("BUNNY_PATH_PREFIX", "shapes")

NAVETTES = ["NAV", "NAVETTE", "Ne8"]


def _upload_to_bunny(filename: str, content: bytes) -> None:
    """Upload un fichier vers BunnyCDN Storage via l'API HTTP."""
    url = f"https://{BUNNY_STORAGE_HOSTNAME}/{BUNNY_STORAGE_ZONE}/{BUNNY_PATH_PREFIX}/{filename}"
    req = urllib.request.Request(
        url,
        data=content,
        method="PUT",
        headers={
            "AccessKey": BUNNY_API_KEY,
            "Content-Type": "application/octet-stream",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status not in (200, 201):
            raise RuntimeError(f"BunnyCDN a répondu {resp.status} pour {filename}")


def generate_shapes():
    """
    Génère un fichier GeoJSON par ligne et l'uploade sur BunnyCDN Storage.
    """
    print("--- Génération des shapes GeoJSON ---")

    shapes = pd.read_csv(os.path.join(DATA_DIR, 'shapes.txt'))
    trips = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))
    routes = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))

    def is_target_route(short_name):
        name = str(short_name)
        return name.isdigit() or name in NAVETTES

    target_routes = routes[routes['route_short_name'].apply(is_target_route)]

    print(f"Upload des fichiers GeoJSON pour {len(target_routes)} lignes vers BunnyCDN...")

    for _, route in target_routes.iterrows():
        r_id = route['route_id']
        r_short = str(route['route_short_name'])

        route_trips = trips[trips['route_id'] == r_id]
        geojson_features = []

        for d_id in route_trips['direction_id'].unique():
            dir_trips = route_trips[route_trips['direction_id'] == d_id]

            # Label de direction = trip_headsign le plus fréquent (source fiable)
            dir_label = dir_trips['trip_headsign'].value_counts().idxmax()

            # Shape de référence = shape la plus fréquente parmi les trips canoniques
            canonical_trips = dir_trips[dir_trips['trip_headsign'] == dir_label]
            most_frequent_shape = canonical_trips['shape_id'].value_counts().idxmax()

            shape_pts = shapes[shapes['shape_id'] == most_frequent_shape].sort_values('shape_pt_sequence')

            # GeoJSON attend [lon, lat]
            coordinates = shape_pts[['shape_pt_lon', 'shape_pt_lat']].values.tolist()

            geojson_features.append({
                "type": "Feature",
                "properties": {
                    "line": r_short,
                    "direction": dir_label,
                    "shape_id": str(most_frequent_shape)
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                }
            })

        geojson = {
            "type": "FeatureCollection",
            "features": geojson_features
        }

        filename = f"{r_short}.geojson"
        content = json.dumps(geojson).encode("utf-8")
        _upload_to_bunny(filename, content)

        print(f"  → {BUNNY_PATH_PREFIX}/{filename} ({len(geojson_features)} direction(s))")

    print("Shapes uploadées sur BunnyCDN avec succès !")
