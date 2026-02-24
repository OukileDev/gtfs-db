import csv
import json
import datetime
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')
SCHEDULES_DIR = os.path.join(BASE_DIR, 'data', 'schedules')


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

    # calendar.txt (services récurrents)
    try:
        with open(get_path('calendar.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row[jour] == '1'
                        and row['start_date'] <= date_str <= row['end_date']):
                    active_services.add(row['service_id'])
    except FileNotFoundError:
        pass

    # calendar_dates.txt (exceptions : ajouts et suppressions)
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


def generate_schedule_for_date(date_str, trip_info):
    """
    Génère schedules/stop_times_YYYYMMDD.json pour une date donnée.
    trip_info doit être pré-chargé (dict trip_id -> {route, service}).
    """
    active_services = get_active_services(date_str)
    if not active_services:
        return False

    stops_dict = defaultdict(list)
    with open(get_path('stop_times.txt'), mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            t_id = row['trip_id']
            info = trip_info.get(t_id)
            if info and info['service'] in active_services:
                stops_dict[row['stop_id']].append({
                    't': row['arrival_time'][:5],
                    'r': info['route'],
                    'id': t_id
                })

    for s_id in stops_dict:
        stops_dict[s_id].sort(key=lambda x: x['t'])

    output_file = os.path.join(SCHEDULES_DIR, f'stop_times_{date_str}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stops_dict, f, separators=(',', ':'))

    return True


def generate_all_schedules():
    """
    Pré-génère un fichier stop_times_YYYYMMDD.json pour chaque date
    présente/future trouvée dans calendar_dates.txt.
    """
    print("--- Génération de tous les plannings ---")
    print(f"Dossier source    : {DATA_DIR}")
    print(f"Dossier de sortie : {SCHEDULES_DIR}")

    os.makedirs(SCHEDULES_DIR, exist_ok=True)

    today_str = datetime.datetime.now().strftime('%Y%m%d')

    # Collecter toutes les dates présentes/futures dans calendar_dates.txt
    dates_to_generate = set()
    try:
        with open(get_path('calendar_dates.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['date'] >= today_str:
                    dates_to_generate.add(row['date'])
    except FileNotFoundError:
        print("ERREUR : calendar_dates.txt introuvable.")
        return

    # Ajouter les dates couvertes par calendar.txt (récurrent)
    try:
        with open(get_path('calendar.txt'), mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                start = max(row['start_date'], today_str)
                end = row['end_date']
                if start > end:
                    continue
                d = datetime.datetime.strptime(start, '%Y%m%d')
                end_d = datetime.datetime.strptime(end, '%Y%m%d')
                while d <= end_d:
                    dates_to_generate.add(d.strftime('%Y%m%d'))
                    d += datetime.timedelta(days=1)
    except FileNotFoundError:
        pass

    print(f"Dates à générer : {len(dates_to_generate)}")

    # Pré-charger trips.txt une seule fois
    trip_info = {}
    print("Lecture de trips.txt...")
    with open(get_path('trips.txt'), mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_info[row['trip_id']] = {
                'route': row['route_id'],
                'service': row['service_id']
            }

    # Générer un fichier par date
    success = 0
    skipped = 0
    for date_str in sorted(dates_to_generate):
        output_file = os.path.join(SCHEDULES_DIR, f'stop_times_{date_str}.json')
        if os.path.exists(output_file):
            skipped += 1
            continue
        ok = generate_schedule_for_date(date_str, trip_info)
        if ok:
            success += 1
        else:
            print(f"  [VIDE] {date_str} — aucun service actif")

    print(f"\nTerminé : {success} fichiers générés, {skipped} déjà existants.")
    print(f"Dossier : {SCHEDULES_DIR}")
