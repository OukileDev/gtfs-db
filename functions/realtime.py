import json
import os
import urllib.request

from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIP_UPDATES_FILE = os.path.join(BASE_DIR, 'data', 'trip_updates.json')

load_dotenv(os.path.join(BASE_DIR, '.env'))

GTFSRT_URL = os.getenv("GTFSRT_URL")


def generate_trip_updates():
    """
    Récupère le flux GTFS-RT et génère trip_updates.json.
    Structure : { "trip_id": { "vehicle": "422", "delays": { "stop_id": delay_sec } } }
    Nettoie les données non conformes :
      - route_id vide ignoré (on garde quand même l'entrée si trip_id est présent)
      - stop_sequence=0 accepté (certains opérateurs ne le renseignent pas)
      - on ignore les StopTimeUpdate sans stop_id
    """
    print("\n--- GTFS-RT TripUpdate ---")
    print(f"Source : {GTFSRT_URL}")

    try:
        req = urllib.request.Request(GTFSRT_URL, headers={"User-Agent": "gtfs-decoder/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except Exception as e:
        print(f"ERREUR lors du téléchargement GTFS-RT : {e}")
        return

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    print(f"Timestamp flux    : {feed.header.timestamp}")
    print(f"Entités reçues    : {len(feed.entity)}")

    trip_updates = {}
    skipped = 0

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip_id = tu.trip.trip_id

        # On ignore les entités sans trip_id
        if not trip_id:
            skipped += 1
            continue

        vehicle_id = tu.vehicle.id if tu.vehicle.id else None

        delays = {}
        for stu in tu.stop_time_update:
            stop_id = stu.stop_id
            if not stop_id:
                continue  # stop_id obligatoire pour la jointure

            delay = None
            if stu.HasField("arrival"):
                delay = stu.arrival.delay
            elif stu.HasField("departure"):
                delay = stu.departure.delay

            delays[stop_id] = delay  # None = pas d'info, 0 = à l'heure

        trip_updates[trip_id] = {
            "vehicle": vehicle_id,
            "delays": delays
        }

    valid = len(trip_updates)
    print(f"TripUpdates valides : {valid} | Ignorées : {skipped}")

    with open(TRIP_UPDATES_FILE, 'w', encoding='utf-8') as f:
        json.dump(trip_updates, f, separators=(',', ':'))

    print(f"SUCCÈS : {TRIP_UPDATES_FILE}")
