"""
extract/extractor.py — Phase EXTRACT du pipeline ETL Mexora
Fonctions d'extraction par source de données.
Retourne des DataFrames BRUTS sans aucune transformation.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional

from config.settings import (
    COMMANDES_FILE, PRODUITS_FILE, CLIENTS_FILE, REGIONS_FILE
)
from utils.logger import etl_logger as log


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _check_file(filepath: Path) -> None:
    """Vérifie l'existence et la non-nullité du fichier source."""
    if not filepath.exists():
        raise FileNotFoundError(f"[EXTRACT] Fichier introuvable : {filepath}")
    if filepath.stat().st_size == 0:
        raise ValueError(f"[EXTRACT] Fichier vide : {filepath}")


# ─────────────────────────────────────────────────────────────────────────────
# Extraction Commandes (CSV)
# ─────────────────────────────────────────────────────────────────────────────

def extract_commandes(filepath: Optional[Path] = None) -> pd.DataFrame:
    """
    Extrait les commandes depuis le fichier CSV source.

    Stratégie :
      - Lecture en dtype=str pour éviter toute conversion implicite
        (les dates mixtes, les codes produits et livreur doivent rester
        des chaînes brutes jusqu'à la phase Transform).
      - Le séparateur est une virgule, encodage UTF-8.

    Returns
    -------
    pd.DataFrame — données brutes, aucune modification appliquée.
    """
    filepath = Path(filepath or COMMANDES_FILE)
    _check_file(filepath)

    df = pd.read_csv(
        filepath,
        encoding="utf-8",
        dtype=str,           # TOUT en str — conversion gérée dans Transform
        keep_default_na=False,  # Les chaînes vides restent '' et non NaN
        na_values=["NULL", "null", "NaN", "nan", "N/A", "n/a"],
    )

    # Nettoyage minimal : espaces de tête/fin dans les noms de colonnes
    df.columns = df.columns.str.strip()

    # Remplacer les chaînes vides par NaN (nécessaire pour isna() dans Transform)
    df.replace("", pd.NA, inplace=True)

    log.log_extract("commandes_mexora.csv", len(df), str(filepath))
    log.debug(f"[EXTRACT] Colonnes commandes : {list(df.columns)}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Extraction Produits (JSON)
# ─────────────────────────────────────────────────────────────────────────────

def extract_produits(filepath: Optional[Path] = None) -> pd.DataFrame:
    """
    Extrait les produits depuis le fichier JSON.

    La structure attendue est :
        { "produits": [ { ... }, ... ] }

    Returns
    -------
    pd.DataFrame — données brutes issues du JSON.
    """
    filepath = Path(filepath or PRODUITS_FILE)
    _check_file(filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "produits" not in data:
        raise KeyError(f"[EXTRACT] Clé 'produits' absente dans {filepath}")

    df = pd.DataFrame(data["produits"])

    # Uniformiser les colonnes en str pour cohérence
    df.columns = df.columns.str.strip()

    log.log_extract("produits_mexora.json", len(df), str(filepath))
    log.debug(f"[EXTRACT] Colonnes produits : {list(df.columns)}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Extraction Clients (CSV)
# ─────────────────────────────────────────────────────────────────────────────

def extract_clients(filepath: Optional[Path] = None) -> pd.DataFrame:
    """
    Extrait les clients depuis le fichier CSV source.

    Returns
    -------
    pd.DataFrame — données brutes.
    """
    filepath = Path(filepath or CLIENTS_FILE)
    _check_file(filepath)

    df = pd.read_csv(
        filepath,
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
        na_values=["NULL", "null", "NaN", "nan", "N/A", "n/a"],
    )

    df.columns = df.columns.str.strip()
    df.replace("", pd.NA, inplace=True)

    log.log_extract("clients_mexora.csv", len(df), str(filepath))
    log.debug(f"[EXTRACT] Colonnes clients : {list(df.columns)}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Extraction Régions (CSV — référentiel géographique propre)
# ─────────────────────────────────────────────────────────────────────────────

def extract_regions(filepath: Optional[Path] = None) -> pd.DataFrame:
    """
    Extrait le référentiel géographique des régions marocaines.
    Ce fichier est PROPRE — aucune transformation n'est requise.

    Returns
    -------
    pd.DataFrame — référentiel géographique complet.
    """
    filepath = Path(filepath or REGIONS_FILE)
    _check_file(filepath)

    df = pd.read_csv(
        filepath,
        encoding="utf-8",
        dtype={
            "code_ville":         str,
            "nom_ville_standard": str,
            "province":           str,
            "region_admin":       str,
            "zone_geo":           str,
            "population":         "int64",
            "code_postal":        str,
        },
    )

    df.columns = df.columns.str.strip()

    log.log_extract("regions_maroc.csv", len(df), str(filepath))

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Fonction utilitaire : chargement du référentiel des villes
# ─────────────────────────────────────────────────────────────────────────────

def charger_referentiel_villes(df_regions: pd.DataFrame) -> dict[str, str]:
    """
    Construit un dictionnaire de mapping :
        {variante_brute → nom_standard}

    Pour chaque ville standard, on crée des variantes canoniques :
      - code_ville (ex: "tanger")
      - nom_ville_standard (ex: "Tanger")
      - variantes connues pour les grandes villes

    Returns
    -------
    dict — mapping {variante.lower().strip() → nom_standard}
    """
    mapping: dict[str, str] = {}

    for _, row in df_regions.iterrows():
        std = row["nom_ville_standard"]
        code = row["code_ville"].lower().strip()

        # Code ville → standard
        mapping[code] = std
        # Nom standard → lui-même
        mapping[std.lower().strip()] = std
        # Variantes avec/sans accents et abréviations connues
        sans_accent = (
            std.lower()
            .replace("é", "e").replace("è", "e").replace("ê", "e")
            .replace("à", "a").replace("â", "a").replace("ô", "o")
            .replace("û", "u").replace("ï", "i").strip()
        )
        mapping[sans_accent] = std

    # Ajout manuel des variantes orthographiques problématiques documentées
    VARIANTES_MANUELLES: dict[str, str] = {
        "tng": "Tanger", "tnja": "Tanger", "tanger": "Tanger",
        "casa": "Casablanca", "cas": "Casablanca", "casablanca": "Casablanca",
        "mrakch": "Marrakech", "marrakesh": "Marrakech",
        "fez": "Fès", "fes": "Fès",
        "rabat": "Rabat",
        "agadir": "Agadir",
        "oujda": "Oujda",
        "meknes": "Meknès", "meknas": "Meknès",
        "kenitra": "Kénitra",
        "tetouan": "Tétouan", "tetuan": "Tétouan",
    }
    mapping.update(VARIANTES_MANUELLES)

    return mapping
