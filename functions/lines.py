import os
import pandas as pd
from psycopg import Connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')

NAVETTES = ["NAVETTE", "Ne8"]


def get_cleaned_lines(conn: Connection):
    """
    Insère les routes et les trips filtrés dans la base cible.
    """
    print("--- Import des lignes et voyages ---")

    routes = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))
    trips = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))

    def is_target_route(short_name):
        name = str(short_name)
        return name.isdigit() or name in NAVETTES

    filtered_routes = routes[routes['route_short_name'].apply(is_target_route)]
    print(f"Ménage terminé : {len(filtered_routes)} lignes conservées sur {len(routes)}.")

    # Insertion des routes
    route_rows = [
        (
            str(row['route_id']),
            str(row['route_short_name']),
            str(row['route_long_name']),
            int(row['route_type']),
            str(row['route_color']) if pd.notna(row['route_color']) else None,
            str(row['route_text_color']) if pd.notna(row['route_text_color']) else None,
        )
        for _, row in filtered_routes.iterrows()
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO routes (route_id, route_short_name, route_long_name, route_type, route_color, route_text_color)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (route_id) DO NOTHING
            """,
            route_rows
        )

    # Insertion des trips pour ces routes uniquement
    filtered_route_ids = set(filtered_routes['route_id'].astype(str))
    filtered_trips = trips[trips['route_id'].astype(str).isin(filtered_route_ids)]

    trip_rows = [
        (
            str(row['trip_id']),
            str(row['route_id']),
            str(row['service_id']),
            str(row['trip_headsign']) if pd.notna(row['trip_headsign']) else None,
            int(row['direction_id']) if pd.notna(row['direction_id']) else None,
            str(row['shape_id']) if pd.notna(row['shape_id']) else None,
        )
        for _, row in filtered_trips.iterrows()
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO trips (trip_id, route_id, service_id, trip_headsign, direction_id, shape_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
            """,
            trip_rows
        )

    conn.commit()
    print(f"Terminé ! {len(route_rows)} routes et {len(trip_rows)} trips insérés.")
