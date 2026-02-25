-- Base : gtfs_infos
-- Registre central de toutes les versions GTFS importées.
-- C'est cette base qu'on interroge en premier pour savoir quelle base créer ou utiliser.
CREATE TABLE IF NOT EXISTS gtfs_version (
    id          SERIAL PRIMARY KEY,
    db_name     VARCHAR(50)  NOT NULL UNIQUE,  -- ex: 'gtfs_20260224' (= nom de la base cible)
    sha256      CHAR(64)     NOT NULL,          -- hash du zip, pour détecter les changements
    imported_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_ignored  BOOLEAN      NOT NULL DEFAULT false
);

-- -----------------------------------------------------------------------
-- Schéma des bases cibles (gtfs_YYYYMMDD)
-- Ces tables sont créées dans chaque base cible, pas dans gtfs_infos.
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stops (
    stop_id   VARCHAR(100) PRIMARY KEY,
    stop_name VARCHAR(200) NOT NULL,
    stop_lat  DOUBLE PRECISION NOT NULL,
    stop_lon  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    route_id         VARCHAR(100) PRIMARY KEY,
    route_short_name VARCHAR(50),
    route_long_name  VARCHAR(300),
    route_type       SMALLINT NOT NULL,
    route_color      CHAR(6),
    route_text_color CHAR(6)
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id       VARCHAR(100) PRIMARY KEY,
    route_id      VARCHAR(100) NOT NULL REFERENCES routes(route_id),
    service_id    VARCHAR(200) NOT NULL,
    trip_headsign VARCHAR(200),
    direction_id  SMALLINT,
    shape_id      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS calendar_dates (
    service_id     VARCHAR(200),
    date           DATE        NOT NULL,
    exception_type SMALLINT    NOT NULL,
    PRIMARY KEY (service_id, date)
);

CREATE TABLE IF NOT EXISTS stop_times (
    trip_id        VARCHAR(100) NOT NULL REFERENCES trips(trip_id),
    stop_sequence  INT          NOT NULL,
    stop_id        VARCHAR(100) NOT NULL REFERENCES stops(stop_id),
    arrival_time   INTERVAL     NOT NULL,
    departure_time INTERVAL     NOT NULL,
    PRIMARY KEY (trip_id, stop_sequence)
);