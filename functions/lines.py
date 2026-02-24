import json
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'network_cleaned.json')

NAVETTES = ["NAVETTE", "Ne8"]


def get_cleaned_lines():
    """
    Génère network_cleaned.json : pour chaque ligne, la liste ordonnée des arrêts
    par direction, basée sur le voyage canonique (shape la plus fréquente).
    """
    print("--- Génération des lignes nettoyées ---")

    routes = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))
    trips = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))
    stop_times = pd.read_csv(os.path.join(DATA_DIR, 'stop_times.txt'), dtype={'stop_headsign': str})
    stops = pd.read_csv(os.path.join(DATA_DIR, 'stops.txt'))

    def is_target_route(short_name):
        name = str(short_name)
        return name.isdigit() or name in NAVETTES

    filtered_routes = routes[routes['route_short_name'].apply(is_target_route)]
    print(f"Ménage terminé : {len(filtered_routes)} lignes conservées sur {len(routes)}.")

    network_data = {}

    for _, route in filtered_routes.iterrows():
        r_id = route['route_id']
        r_name = str(route['route_short_name'])

        network_data[r_name] = {
            "long_name": route['route_long_name'],
            "directions": {}
        }

        route_trips = trips[trips['route_id'] == r_id]
        directions = route_trips['direction_id'].unique()

        for d_id in directions:
            try:
                dir_trips = route_trips[route_trips['direction_id'] == d_id]

                # Label de direction = trip_headsign le plus fréquent (source fiable)
                dir_label = dir_trips['trip_headsign'].value_counts().idxmax()

                # Shape de référence = shape la plus fréquente parmi les trips canoniques
                canonical_trips = dir_trips[dir_trips['trip_headsign'] == dir_label]
                ref_shape_id = canonical_trips['shape_id'].value_counts().idxmax()
                t_id = canonical_trips[canonical_trips['shape_id'] == ref_shape_id].iloc[0]['trip_id']

                # Récupération et jointure des arrêts
                times = stop_times[stop_times['trip_id'] == t_id].sort_values('stop_sequence')
                full_stops = pd.merge(times, stops, on='stop_id')

                stop_list = [
                    {
                        "id": str(s['stop_id']),
                        "name": s['stop_name'],
                        "lat": float(s['stop_lat']),
                        "lon": float(s['stop_lon'])
                    }
                    for _, s in full_stops.iterrows()
                ]

                network_data[r_name]["directions"][dir_label] = stop_list
            except (IndexError, KeyError):
                continue

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(network_data, f, indent=4, ensure_ascii=False)

    print(f"Terminé ! Réseau sauvegardé dans '{OUTPUT_FILE}'.")
