FROM python:3.13-slim

WORKDIR /app

# Installer les dépendances système pour Postgres
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copier les requirements et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le code de l'application
COPY . .

# La commande par défaut : lancer le pipeline quotidien
CMD ["python", "main.py"]
