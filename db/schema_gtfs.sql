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
    arrival_time   INTERVAL     NOT NULL,
    departure_time INTERVAL     NOT NULL,
    PRIMARY KEY (trip_id, stop_sequence)
);
