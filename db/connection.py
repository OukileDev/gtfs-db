import os
import psycopg
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

# Utilisateur qui a besoin de lire les données (ton app web)
DB_USER = os.getenv("POSTGRES_USER", "oukile")

# gtfs_infos : base registre
REGISTRY_URL = os.getenv("REGISTRY_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/gtfs_infos")

# URL admin (base 'postgres')
ADMIN_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")


def get_registry_connection():
    return psycopg.connect(REGISTRY_URL)


def get_connection(db_name: str):
    base = ADMIN_URL.rsplit("/", 1)[0]
    return psycopg.connect(f"{base}/{db_name}")


def create_database_if_not_exists(db_name: str):
    """Crée la base et donne le droit de connexion à l'utilisateur oukile."""
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
        ).fetchone()

        if not exists:
            conn.execute(f'CREATE DATABASE "{db_name}"')
            # Autorise l'utilisateur à se connecter
            conn.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO {DB_USER}')
            print(f"Base '{db_name}' créée. Accès CONNECT accordé à {DB_USER}.")
        else:
            print(f"Base '{db_name}' déjà existante.")


def setup_registry():
    """Prépare le registre et donne les droits de lecture sur les tables de version."""
    create_database_if_not_exists("gtfs_infos")

    schema_path = os.path.join(os.path.dirname(__file__), 'schema_registry.sql')
    with open(schema_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    with get_registry_connection() as conn:
        conn.execute(sql)
        # Droits de lecture sur le registre pour l'app web
        conn.execute(f"GRANT USAGE ON SCHEMA public TO {DB_USER}")
        conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {DB_USER}")
        conn.commit()
    print("Registre gtfs_infos prêt (lecture accordée).")


def apply_gtfs_schema(db_name: str):
    """Applique le schéma et donne les droits de lecture sur les données GTFS."""
    schema_path = os.path.join(os.path.dirname(__file__), 'schema_gtfs.sql')
    with open(schema_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    with get_connection(db_name) as conn:
        conn.execute(sql)
        # Droits de lecture sur les tables GTFS pour l'app web
        conn.execute(f"GRANT USAGE ON SCHEMA public TO {DB_USER}")
        conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {DB_USER}")
        conn.commit()
    print(f"Schéma GTFS appliqué et droits SELECT accordés sur '{db_name}'.")


def apply_gtfs_schema(db_name: str):
    """
    Crée les tables GTFS (stops, routes, trips, etc.) dans la base cible.
    """
    schema_path = os.path.join(os.path.dirname(__file__), 'schema_gtfs.sql')
    with open(schema_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    with get_connection(db_name) as conn:
        conn.execute(sql)
        conn.commit()
    print(f"Schéma GTFS appliqué sur '{db_name}'.")


def get_active_version():
    """
    Retourne (db_name, sha256) de la version active : la plus récente importée,
    sauf si elle est ignorée — auquel cas on remonte à la plus récente non ignorée.
    Retourne (None, None) si aucune version n'existe.
    """
    with get_registry_connection() as conn:
        row = conn.execute(
            """
            SELECT db_name, sha256 FROM gtfs_version
            WHERE is_ignored = false
            ORDER BY imported_at DESC
            LIMIT 1
            """
        ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def register_version(db_name: str, sha256: str):
    """
    Inscrit une nouvelle version dans le registre.
    Par défaut is_ignored = false, elle devient donc la version active.
    """
    with get_registry_connection() as conn:
        conn.execute(
            """
            INSERT INTO gtfs_version (db_name, sha256)
            VALUES (%s, %s)
            ON CONFLICT (db_name) DO UPDATE
                SET sha256      = EXCLUDED.sha256,
                    imported_at = now(),
                    is_ignored  = false
            """,
            (db_name, sha256)
        )
        conn.commit()
    print(f"Version '{db_name}' enregistrée.")
