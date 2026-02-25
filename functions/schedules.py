import csv
import datetime
import os
from psycopg import Connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')


def get_path(filename):
    return os.path.join(DATA_DIR, filename)


def get_active_services(date_str):
    """
    Retourne l'ensemble des service_id actifs pour une date donnée (format YYYYMMDD).
    Combine calendar.txt (récurrent) et calendar_dates.txt (exceptions).
    """
    jours = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
    jour = jours[date_obj.weekday()]

    active_services = set()

    try:
        with open(get_path('calendar.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row[jour] == '1'
                        and row['start_date'] <= date_str <= row['end_date']):
                    active_services.add(row['service_id'])
    except FileNotFoundError:
        pass

    try:
        with open(get_path('calendar_dates.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['date'] == date_str:
                    if row['exception_type'] == '1':
                        active_services.add(row['service_id'])
                    elif row['exception_type'] == '2':
                        active_services.discard(row['service_id'])
    except FileNotFoundError:
        pass

    return active_services


def generate_all_schedules(conn: Connection):
    """
    Insère calendar_dates et stop_times dans la base cible.
    Seuls les trips déjà présents dans la table trips sont traités (FK).
    """
    print("--- Import des calendriers et horaires ---")

    # 1. Insérer toutes les calendar_dates
    calendar_rows = []
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    try:
        with open(get_path('calendar_dates.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['date'] >= today_str:
                    calendar_rows.append((
                        row['service_id'],
                        datetime.datetime.strptime(row['date'], '%Y%m%d').date(),
                        int(row['exception_type'])
                    ))
    except FileNotFoundError:
        print("ERREUR : calendar_dates.txt introuvable.")
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO calendar_dates (service_id, date, exception_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (service_id, date) DO NOTHING
            """,
            calendar_rows
        )
    conn.commit()
    print(f"{len(calendar_rows)} entrées calendar_dates insérées.")

    # 2. Récupérer les trip_id valides en base (contrainte FK)
    with conn.cursor() as cur:
        valid_trip_ids = {row[0] for row in cur.execute("SELECT trip_id FROM trips").fetchall()}

    # 3. Insérer les stop_times par batch
    print("Lecture et import de stop_times.txt...")
    BATCH_SIZE = 5000
    batch = []
    total = 0

    with open(get_path('stop_times.txt'), mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['trip_id'] not in valid_trip_ids:
                continue
            # On conserve le texte d'arrivée et on calcule les secondes depuis minuit
            arrival_text = row.get('arrival_time') or ''
            crossing_seconds = None
            if arrival_text:
                # GTFS autorise des heures > 23 (ex: 25:30:00). On convertit en secondes depuis minuit
                try:
                    parts = arrival_text.split(':')
                    if len(parts) == 3:
                        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
                        crossing_seconds = h * 3600 + m * 60 + s
                    else:
                        crossing_seconds = None
                except Exception:
                    crossing_seconds = None

            batch.append((
                row['trip_id'],
                int(row['stop_sequence']),
                row['stop_id'],
                crossing_seconds,
                arrival_text,
            ))
            if len(batch) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO stop_times (trip_id, stop_sequence, stop_id, crossing_time_seconds, crossing_time_text)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (trip_id, stop_sequence) DO NOTHING
                        """,
                        batch
                    )
                conn.commit()
                total += len(batch)
                batch = []

    # Dernier batch
    if batch:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO stop_times (trip_id, stop_sequence, stop_id, crossing_time_seconds, crossing_time_text)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, stop_sequence) DO NOTHING
                """,
                batch
            )
        conn.commit()
        total += len(batch)

    print(f"Terminé ! {total} horaires insérés.")
