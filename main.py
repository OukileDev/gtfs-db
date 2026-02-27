import argparse
import hashlib
import io
import json
import logging
import os
import subprocess
import sys
import zipfile
from datetime import datetime

import requests
from dotenv import load_dotenv

# Chargement du .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Ajout du dossier functions/ au path pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'functions'))

from db.connection import (
    apply_gtfs_schema,
    create_database_if_not_exists,
    get_active_version,
    get_connection,
    register_version,
    setup_registry,
)
from functions.lines import get_cleaned_lines
from functions.schedules import generate_all_schedules
from functions.shapes import generate_shapes
from functions.stops import generate_all_stops
from functions.network import generate_network

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GTFS_URL = os.getenv("GTFS_URL")
KUBERNETES_ENABLED = os.getenv("KUBERNETES_ENABLED", "false").lower() == "true"
POSTGRES_USER = os.getenv("POSTGRES_USER", "oukile")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "changeme")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")


def update_k8s_configmap(db_name: str):
    """
    Crée ou met à jour un ConfigMap Kubernetes avec le nom de la base active.
    Cela permet aux autres services (notamment oukile-webapp) de connaître la base courante.
    """
    if not KUBERNETES_ENABLED:
        log.info("Kubernetes n'est pas activé, skip ConfigMap update")
        return

    try:
        configmap_name = "gtfs-active-db"
        namespace = "oukile"
        database_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"

        # Construire le ConfigMap en YAML
        configmap_yaml = f"""
apiVersion: v1
kind: ConfigMap
metadata:
  name: {configmap_name}
  namespace: {namespace}
data:
  ACTIVE_DATABASE: "{db_name}"
  DATABASE_URL: "{database_url}"
"""
        
        # Appliquer le ConfigMap via kubectl
        result = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=configmap_yaml.encode(),
            capture_output=True,
            timeout=30
        )
        
        if result.returncode == 0:
            log.info(f"✅ ConfigMap '{configmap_name}' mis à jour avec la base '{db_name}'")
        else:
            log.error(f"❌ Erreur lors de la mise à jour du ConfigMap : {result.stderr.decode()}")
    except Exception as e:
        log.error(f"❌ Erreur lors de la mise à jour du ConfigMap Kubernetes : {e}")


def fetch_gtfs() -> tuple[str, bytes]:
    """Télécharge le GTFS et retourne (sha256, contenu brut)."""
    log.info("Téléchargement du GTFS...")
    r = requests.get(GTFS_URL)
    sha256 = hashlib.sha256(r.content).hexdigest()
    log.info(f"Hash SHA256 : {sha256}")
    return sha256, r.content


def extract_gtfs(raw: bytes, db_name: str):
    gtfs_data_dir = os.path.join(os.path.dirname(__file__), 'data', 'gtfs_data')
    os.makedirs(gtfs_data_dir, exist_ok=True)
    zipfile.ZipFile(io.BytesIO(raw)).extractall(gtfs_data_dir)
    log.info("Données extraites dans data/gtfs_data/")


def run_daily():
    """
    Pipeline quotidien : télécharge le GTFS, compare le hash,
    crée la base et importe les données si nécessaire.
    """
    log.info("=== Pipeline quotidien ===")

    sha256, raw = fetch_gtfs()

    active_db, active_sha256 = get_active_version()
    if active_sha256 == sha256:
        log.info("GTFS inchangé (même hash), rien à faire.")
        return

    db_name = f"gtfs_{datetime.now().strftime('%Y%m%d')}"
    log.info(f"Nouveau GTFS détecté → base cible : {db_name}")

    create_database_if_not_exists(db_name)
    extract_gtfs(raw, db_name)
    apply_gtfs_schema(db_name)

    with get_connection(db_name) as conn:
        log.info("Arrêts...")
        generate_all_stops(conn)

        log.info("Réseau nettoyé...")
        get_cleaned_lines(conn)
        # Génération de la table network (liste d'arrêts par direction)
        log.info("Réseau (network)...")
        generate_network(conn)

        log.info("Plannings...")
        generate_all_schedules(conn)

    # log.info("Shapes...")
    # generate_shapes()

    register_version(db_name, sha256)
    log.info(f"✅ Pipeline quotidien terminé → {db_name}")
    
    # Mettre à jour le ConfigMap Kubernetes si activé
    update_k8s_configmap(db_name)

if __name__ == "__main__":
    setup_registry()
    run_daily()