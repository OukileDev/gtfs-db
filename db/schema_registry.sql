-- Schéma de la base registre : gtfs_infos
CREATE TABLE IF NOT EXISTS gtfs_version (
    id          SERIAL PRIMARY KEY,
    db_name     VARCHAR(50)  NOT NULL UNIQUE,
    sha256      CHAR(64)     NOT NULL,
    imported_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_ignored  BOOLEAN      NOT NULL DEFAULT false
);
