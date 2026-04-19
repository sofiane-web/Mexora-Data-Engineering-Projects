"""
config/settings.py — Configuration centralisée du pipeline ETL Mexora
"""

import os
from pathlib import Path

# ─── Chemins du projet ───────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent
DATA_DIR  = BASE_DIR / "data"
LOGS_DIR  = BASE_DIR / "logs"

# Fichiers source
COMMANDES_FILE = DATA_DIR / "commandes_mexora.csv"
PRODUITS_FILE  = DATA_DIR / "produits_mexora.json"
CLIENTS_FILE   = DATA_DIR / "clients_mexora.csv"
REGIONS_FILE   = DATA_DIR / "regions_maroc.csv"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "mexora_dwh",
    "user":     "postgres",       
    "password": "Admin123",       
}

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ─── Schémas PostgreSQL ───────────────────────────────────────────────────────
SCHEMA_STAGING   = "staging_mexora"
SCHEMA_DWH       = "dwh_mexora"
SCHEMA_REPORTING = "reporting_mexora"

# ─── Paramètres ETL ──────────────────────────────────────────────────────────
CHUNK_SIZE           = 1_000    # lignes par batch lors du chargement
DIM_TEMPS_DATE_DEBUT = "2020-01-01"
DIM_TEMPS_DATE_FIN   = "2026-12-31"

# ─── Règles métier Mexora ─────────────────────────────────────────────────────
SEGMENT_GOLD_SEUIL   = 15_000   # MAD — CA 12 mois
SEGMENT_SILVER_SEUIL =  5_000   # MAD — CA 12 mois
DELAI_LIVRAISON_ALERTE = 3      # jours — seuil de retard

# Fériés marocains (YYYY-MM-DD) — à compléter chaque année
FERIES_MAROC = {
    "2022-01-01", "2022-01-11", "2022-05-01", "2022-07-30",
    "2022-08-14", "2022-11-06", "2022-11-18",
    "2023-01-01", "2023-01-11", "2023-05-01", "2023-07-30",
    "2023-08-14", "2023-11-06", "2023-11-18",
    "2024-01-01", "2024-01-11", "2024-05-01", "2024-07-30",
    "2024-08-14", "2024-11-06", "2024-11-18",
    "2025-01-01", "2025-01-11", "2025-05-01", "2025-07-30",
    "2025-08-14", "2025-11-06", "2025-11-18",
    "2026-01-01", "2026-01-11", "2026-05-01", "2026-07-30",
    "2026-08-14", "2026-11-06", "2026-11-18",
}

# Périodes Ramadan (approximatives)
RAMADAN_PERIODES = [
    ("2020-04-23", "2020-05-23"),
    ("2021-04-12", "2021-05-12"),
    ("2022-04-02", "2022-05-01"),
    ("2023-03-22", "2023-04-20"),
    ("2024-03-10", "2024-04-09"),
    ("2025-03-01", "2025-03-30"),
    ("2026-02-17", "2026-03-18"),
]

# Mapping statuts non-standards → standard
MAPPING_STATUTS = {
    "livré":    "livré",  "livre":    "livré",  "LIVRE":   "livré",
    "DONE":     "livré",  "Livré":    "livré",  "livree":  "livré",
    "annulé":   "annulé", "annule":   "annulé", "KO":      "annulé",
    "Annulé":   "annulé", "ANNULE":   "annulé",
    "en_cours": "en_cours", "OK":     "en_cours", "EN_COURS": "en_cours",
    "retourné": "retourné", "retourne": "retourné", "RETOURNE": "retourné",
}

STATUTS_VALIDES = {"livré", "annulé", "en_cours", "retourné"}

# Mapping sexe → standard
MAPPING_SEXE = {
    "m": "m", "M": "m", "1": "m", "homme": "m", "Homme": "m",
    "HOMME": "m", "male": "m", "Male": "m", "h": "m", "H": "m",
    "f": "f", "F": "f", "0": "f", "femme": "f", "Femme": "f",
    "FEMME": "f", "female": "f", "Female": "f",
}

# TVA Maroc
TVA = 0.20
