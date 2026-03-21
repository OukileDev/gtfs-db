import json
import logging
import os

import pandas as pd
import redis as redislib

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'gtfs_data')

NAVETTES = ["NAVETTE", "Ne8"]

log = logging.getLogger(__name__)


def _is_target_route(short_name: str) -> bool:
    return short_name.isdigit() or short_name in NAVETTES


def write_gtfs_to_redis(r: redislib.Redis) -> None:
    """
    Écrit les données GTFS dans Redis. Chaque clé contient exactement
    le payload retourné par l'endpoint correspondant — zéro calcul au runtime.

    Structure des clés (miroir des routes API) :
      gtfs:lines:all    → payload de GET /api/lines
      gtfs:lines:{id}   → payload de GET /api/lines/[id]
      gtfs:stops:all    → payload de GET /api/stops
      gtfs:stops:{id}   → payload de GET /api/stops/[id]
    """
    log.info("[redis] Lecture des fichiers GTFS...")
    stops_df = pd.read_csv(os.path.join(DATA_DIR, 'stops.txt'))
    routes_df = pd.read_csv(os.path.join(DATA_DIR, 'routes.txt'))
    trips_df = pd.read_csv(os.path.join(DATA_DIR, 'trips.txt'))
    stop_times_df = pd.read_csv(os.path.join(DATA_DIR, 'stop_times.txt'))

    # ── gtfs:stops:all  /  gtfs:stops:{id} ───────────────────────────────────
    # stops_lookup contient TOUS les arrêts (même sans coords) pour que les
    # listes de stops dans les lignes ne soient jamais vides.
    stops_lookup: dict[str, dict] = {}
    stops_list = []
    pipe = r.pipeline()

    for _, row in stops_df.iterrows():
        stop_id = str(row['stop_id'])
        has_coords = not (pd.isna(row['stop_lat']) or pd.isna(row['stop_lon']))
        stop = {
            'stop_id': stop_id,
            'stop_name': str(row['stop_name']),
            'stop_lat': float(row['stop_lat']) if has_coords else None,
            'stop_lon': float(row['stop_lon']) if has_coords else None,
        }
        stops_lookup[stop_id] = stop
        if has_coords:
            stops_list.append(stop)
            pipe.set(f"gtfs:stops:{stop_id}", json.dumps(stop))

    pipe.set('gtfs:stops:all', json.dumps(stops_list))
    pipe.execute()
    log.info(f"[redis] {len(stops_list)} arrêts écrits (gtfs:stops:*).")

    # ── gtfs:lines:all ────────────────────────────────────────────────────────
    filtered_routes = routes_df[
        routes_df['route_short_name'].apply(lambda x: _is_target_route(str(x)))
    ]

    lines_list = [
        {
            'route_id': str(row['route_id']),
            'route_short_name': str(row['route_short_name']),
            'route_long_name': str(row['route_long_name']),
            'route_color': str(row['route_color']) if pd.notna(row.get('route_color')) else None,
            'route_text_color': str(row['route_text_color']) if pd.notna(row.get('route_text_color')) else None,
        }
        for _, row in filtered_routes.iterrows()
    ]

    r.set('gtfs:lines:all', json.dumps(lines_list))
    log.info(f"[redis] {len(lines_list)} lignes écrites (gtfs:lines:all).")

    # ── gtfs:lines:{id} ───────────────────────────────────────────────────────
    route_color_lookup: dict[str, str | None] = {
        str(row['route_short_name']): str(row['route_color']) if pd.notna(row.get('route_color')) else None
        for _, row in filtered_routes.iterrows()
    }

    filtered_short_names = filtered_routes['route_short_name'].astype(str).unique()

    pipe = r.pipeline()
    count = 0

    for route_short in filtered_short_names:
        route_ids = filtered_routes[
            filtered_routes['route_short_name'].astype(str) == route_short
        ]['route_id'].astype(str).tolist()

        route_trips = trips_df[trips_df['route_id'].astype(str).isin(route_ids)].copy()
        if route_trips.empty:
            continue

        route_trips['direction_id'] = route_trips['direction_id'].fillna(-1).astype(int)

        dir_data: dict[int, dict | None] = {}
        for direction in [0, 1]:
            dir_trips = route_trips[route_trips['direction_id'] == direction]
            if dir_trips.empty:
                dir_data[direction] = None
                continue

            trip_ids = dir_trips['trip_id'].astype(str).tolist()
            st = stop_times_df[stop_times_df['trip_id'].astype(str).isin(trip_ids)].copy()
            if st.empty:
                dir_data[direction] = None
                continue

            # Trip le plus long = le plus représentatif de la direction.
            # On garde le dtype original de trip_id pour que la comparaison
            # groupby → filtre soit sans cast (évite les mismatches int/str).
            counts = st.groupby('trip_id').size().reset_index(name='n')
            best_trip_id = counts.sort_values('n', ascending=False).iloc[0]['trip_id']
            trip_stops = st[st['trip_id'] == best_trip_id].sort_values('stop_sequence')

            stop_list = [
                stops_lookup[str(srow['stop_id'])]
                for _, srow in trip_stops.iterrows()
                if str(srow['stop_id']) in stops_lookup
            ]

            sample_trip = dir_trips.iloc[0]
            direction_name = (
                str(sample_trip['trip_headsign'])
                if pd.notna(sample_trip.get('trip_headsign'))
                else None
            )
            dir_data[direction] = {'direction_name': direction_name, 'stop_list': stop_list}

        # Payload exact retourné par GET /api/lines/[id]
        line_payload = {
            'direction_name_1': dir_data[0]['direction_name'] if dir_data.get(0) else None,
            'stops_1': dir_data[0]['stop_list'] if dir_data.get(0) else [],
            'direction_name_2': dir_data[1]['direction_name'] if dir_data.get(1) else None,
            'stops_2': dir_data[1]['stop_list'] if dir_data.get(1) else [],
            'route_color': route_color_lookup.get(route_short),
        }

        pipe.set(f"gtfs:lines:{route_short}", json.dumps(line_payload))
        count += 1

    pipe.execute()
    log.info(f"[redis] {count} lignes détaillées écrites (gtfs:lines:*).")
    log.info("[redis] ✅ Écriture GTFS dans Redis terminée.")
