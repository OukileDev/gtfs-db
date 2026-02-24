import json
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')
SHAPES_DIR = os.path.join(BASE_DIR, 'data', 'shapes')

NAVETTES = ["NAV", "NAVETTE", "Ne8"]


def generate_shapes():
    """
    Génère un fichier GeoJSON par ligne dans le dossier shapes/.
    Chaque fichier contient les tracés pour chaque direction.
    """
    print("--- Génération des shapes GeoJSON ---")

    shapes = pd.read_csv(os.path.join(DATA_DIR, 'shapes.txt'))
    trips = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))
    routes = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))

    def is_target_route(short_name):
        name = str(short_name)
        return name.isdigit() or name in NAVETTES

    target_routes = routes[routes['route_short_name'].apply(is_target_route)]

    os.makedirs(SHAPES_DIR, exist_ok=True)

    print(f"Génération des fichiers GeoJSON pour {len(target_routes)} lignes...")

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

        output_path = os.path.join(SHAPES_DIR, f"{r_short}.geojson")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f)

        print(f"  → shapes/{r_short}.geojson ({len(geojson_features)} direction(s))")

    print("Dossier 'shapes/' généré avec succès !")
