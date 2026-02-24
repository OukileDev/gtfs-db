# Oukilé GTFS — Pipeline de données

## Contexte
Ce projet est le pipeline de données qui alimentera la base de données d'Oukilé.

Il télécharge, traite et structure les données GTFS ouvertes d'AggloBus (réseau de transport en commun de l'agglomération de Bourges) pour les rendre exploitables en base de données.

Ce projet est personnel et n'est pas affilié à Agglobus ou à la RATPDev.

## Ce que fait ce pipeline
1. **Téléchargement** du GTFS statique depuis data.gouv.fr
2. **Arrêts** — liste de tous les arrêts avec leurs coordonnées GPS (`all_stops.json`)
3. **Shapes** — tracés GeoJSON par ligne et par direction (`shapes/*.geojson`)
4. **Réseau nettoyé** — lignes avec leurs arrêts ordonnés par direction (`network_cleaned.json`)
5. **Plannings** — horaires par arrêt et par jour de service (`schedules/stop_times_YYYYMMDD.json`)

## Stack technique
- **Docker (prochainement)**
- **Python 3**
- **Psycopg 3**
- **pandas**
- **requests**
- **python-dotenv**

## Structure du projet
```
gtfs-api/
├── main.py                  ← point d'entrée, orchestre tout le pipeline
├── functions/
│   ├── stops.py
│   ├── shapes.py
│   ├── lines.py
│   ├── schedules.py
│   └── realtime.py
└── data/                    ← généré, ignoré par Git
    ├── gtfs_data/           ← fichiers GTFS bruts extraits
    ├── schedules/           ← un JSON par jour de service
    ├── shapes/              ← un GeoJSON par ligne
    ├── all_stops.json
    ├── network_cleaned.json
    └── trip_updates.json
```

## Installation
1. Clonez le dépôt :
   ```bash
   git clone https://github.com/OukileDev/gtfs-api
   cd gtfs-api
   ```
2. Créez un environnement virtuel et installez les dépendances :
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install requests pandas gtfs-realtime-bindings python-dotenv
   ```
3. Configurez les variables d'environnement :
   ```bash
   cp .env.example .env
   # puis éditez .env
   ```
4. Lancez le pipeline :
   ```bash
   python3 main.py
   ```

## Variables d'environnement
| Variable | Description |
|---|---|
| `GTFS_URL` | URL du fichier GTFS statique (zip) |
| `GTFSRT_URL` | URL du flux GTFS-RT (TripUpdates) |

## Contribution
Si vous souhaitez contribuer, n'hésitez pas à ouvrir une issue ou une pull request.