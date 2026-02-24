import os
import psycopg
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

# gtfs_infos : base registre, toujours existante
REGISTRY_URL = os.getenv("REGISTRY_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/gtfs_infos")

# URL admin (base 'postgres') pour pouvoir créer d'autres bases
ADMIN_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")


def get_registry_connection():
    """Connexion à gtfs_infos (le registre)."""
    return psycopg.connect(REGISTRY_URL)


def get_connection(db_name: str):
    """Connexion à une base de données GTFS cible (ex: gtfs_20260224)."""
    base = ADMIN_URL.rsplit("/", 1)[0]
    return psycopg.connect(f"{base}/{db_name}")


def create_database_if_not_exists(db_name: str):
    """
    Crée la base db_name si elle n'existe pas.
    Requiert une connexion admin en autocommit car CREATE DATABASE
    ne peut pas s'exécuter dans une transaction.
    """
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
        ).fetchone()

        if not exists:
            conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Base '{db_name}' créée.")
        else:
            print(f"Base '{db_name}' déjà existante.")


def setup_registry():
    """
    Crée la base gtfs_infos et y applique schema_registry.sql si nécessaire.
    À appeler une seule fois au démarrage.
    """
    create_database_if_not_exists("gtfs_infos")

    schema_path = os.path.join(os.path.dirname(__file__), 'schema_registry.sql')
    with open(schema_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    with get_registry_connection() as conn:
        conn.execute(sql)
        conn.commit()
    print("Registre gtfs_infos prêt.")


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
