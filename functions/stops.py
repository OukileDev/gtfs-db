import os
import pandas as pd
from psycopg import Connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')


def generate_all_stops(conn: Connection):
    """
    Insère tous les arrêts dans la table stops de la base cible.
    """
    print("--- Import des arrêts ---")

    stops = pd.read_csv(os.path.join(DATA_DIR, 'stops.txt'))

    rows = [
        (str(row['stop_id']), row['stop_name'], float(row['stop_lat']), float(row['stop_lon']))
        for _, row in stops.iterrows()
        if not (pd.isna(row['stop_lat']) or pd.isna(row['stop_lon']))
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stop_id) DO NOTHING
            """,
            rows
        )
    conn.commit()

    print(f"Terminé ! {len(rows)} arrêts insérés.")
