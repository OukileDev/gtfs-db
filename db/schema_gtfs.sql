-- Schéma des bases cibles (gtfs_YYYYMMDD)

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
    date           DATE     NOT NULL,
    exception_type SMALLINT NOT NULL,
    PRIMARY KEY (service_id, date)
);

CREATE TABLE IF NOT EXISTS stop_times (
    trip_id        VARCHAR(100) NOT NULL REFERENCES trips(trip_id),
    stop_sequence  INT          NOT NULL,
    stop_id        VARCHAR(100) NOT NULL REFERENCES stops(stop_id),
    crossing_time_seconds INT,
    crossing_time_text    VARCHAR(20),
    PRIMARY KEY (trip_id, stop_sequence)
);

-- Table network : une ligne par numero de ligne, avec les deux directions et
-- la liste ordonnée des arrêts pour chaque direction (format JSONB).

CREATE TABLE IF NOT EXISTS network (
    line_number      VARCHAR(50) PRIMARY KEY,
    direction_name_1 VARCHAR(200),
    -- stocke la liste d'arrêts sérialisée en JSON texte pour compatibilité
    -- avec des ORM / drivers qui ne supportent pas JSONB (ex: Prisma)
    stop_list_1      TEXT,
    direction_name_2 VARCHAR(200),
    stop_list_2      TEXT
);
