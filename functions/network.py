import os
import os
import json
import pandas as pd
from psycopg import Connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')


def generate_network(conn: Connection):
    """
    Construit la table `network` : pour chaque `route_short_name` (numéro de ligne)
    on collecte les arrêts traversés pour les deux directions (direction_id 0/1)
    et on insère les colonnes : line_number, direction_name_1, stop_list_1 (JSON texte),
    direction_name_2, stop_list_2.

    Logique :
    - lire `routes.txt`, `trips.txt`, `stop_times.txt`, `stops.txt`.
    - garder uniquement les routes cibles (même filtre que `lines.get_cleaned_lines`).
    - pour chaque `route_short_name`, grouper les trips par `direction_id` et prendre
      l'ordre des arrêts par `stop_sequence` sur un trip représentatif (le plus long).
    - convertir la liste d'arrêts en JSON (liste d'objets avec stop_id et stop_name).
    """
    print("--- Génération de la table network ---")

    routes = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))
    trips = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))
    stop_times = pd.read_csv(os.path.join(DATA_DIR, 'stop_times.txt'))
    stops = pd.read_csv(os.path.join(DATA_DIR, 'stops.txt'))

    # Filtre: conserver les lignes numériques ou NAVETTES — reprendre la logique de lines.py
    NAVETTES = ["NAVETTE", "Ne8"]

    def is_target_route(short_name):
        name = str(short_name)
        return name.isdigit() or name in NAVETTES

    filtered_routes = routes[routes['route_short_name'].apply(is_target_route)]
    filtered_route_short_names = filtered_routes['route_short_name'].astype(str).unique()

    # Préparer lookup pour stop names
    stops_lookup = {str(r['stop_id']): r['stop_name'] for _, r in stops.iterrows()}

    network_rows = []

    for route_short in filtered_route_short_names:
        # tous les route_ids correspondant à ce short name
        route_ids = filtered_routes[filtered_routes['route_short_name'].astype(str) == str(route_short)]['route_id'].astype(str).tolist()
        # trips pour ces route_ids
        route_trips = trips[trips['route_id'].astype(str).isin(route_ids)].copy()
        if route_trips.empty:
            continue

        # Normaliser direction_id -> fillna with -1 and convert to int where possible
        route_trips['direction_id'] = route_trips['direction_id'].fillna(-1).astype(int)

        # On va construire pour directions 0 et 1; si absent, laisser None
        dir_data = {}
        for direction in [0, 1]:
            dir_trips = route_trips[route_trips['direction_id'] == direction]
            if dir_trips.empty:
                dir_data[direction] = None
                continue

            # Pour robustesse, choisir le trip qui a le plus grand nombre d'arrêts (stop_times)
            trip_ids = dir_trips['trip_id'].astype(str).tolist()
            st = stop_times[stop_times['trip_id'].astype(str).isin(trip_ids)].copy()
            if st.empty:
                dir_data[direction] = None
                continue

            counts = st.groupby('trip_id').size().reset_index(name='n')
            best_trip_id = counts.sort_values('n', ascending=False).iloc[0]['trip_id']

            trip_stops = st[st['trip_id'] == best_trip_id].sort_values('stop_sequence')
            stop_list = []
            for _, row in trip_stops.iterrows():
                sid = str(row['stop_id'])
                stop_list.append({'stop_id': sid, 'stop_name': stops_lookup.get(sid)})

            # récupérer un nom de direction à partir du trip_headsign s'il existe
            sample_trip = dir_trips.iloc[0]
            direction_name = sample_trip['trip_headsign'] if pd.notna(sample_trip.get('trip_headsign')) else None

            dir_data[direction] = {'direction_name': direction_name, 'stop_list': stop_list}

        row = (
            str(route_short),
            dir_data[0]['direction_name'] if dir_data.get(0) else None,
            json.dumps(dir_data[0]['stop_list']) if dir_data.get(0) else None,
            dir_data[1]['direction_name'] if dir_data.get(1) else None,
            json.dumps(dir_data[1]['stop_list']) if dir_data.get(1) else None,
        )
        network_rows.append(row)

    # Insérer dans la table network
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO network (line_number, direction_name_1, stop_list_1, direction_name_2, stop_list_2)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (line_number) DO UPDATE
              SET direction_name_1 = EXCLUDED.direction_name_1,
                  stop_list_1      = EXCLUDED.stop_list_1,
                  direction_name_2 = EXCLUDED.direction_name_2,
                  stop_list_2      = EXCLUDED.stop_list_2
            """,
            network_rows
        )
        conn.commit()

    print(f"Terminé ! {len(network_rows)} lignes insérées/maj dans 'network'.")
