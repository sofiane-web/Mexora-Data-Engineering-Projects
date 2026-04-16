"""
transform/clean_produits.py — Nettoyage et transformation des produits Mexora

Règles appliquées :
  R1 — Normalisation de la casse des catégories (title case)
  R2 — Gestion des prix catalogue null (produits anciens)
  R3 — Nettoyage des champs texte (strip, normalisation)
  R4 — Marquage SCD Type 2 pour les produits inactifs avec commandes
  R5 — Validation de la date de création
"""

import pandas as pd
from datetime import date

from utils.logger import etl_logger as log

# Catégories valides Mexora (après normalisation)
CATEGORIES_VALIDES = {"Electronique", "Mode", "Alimentation"}


# ─────────────────────────────────────────────────────────────────────────────
# Fonctions atomiques
# ─────────────────────────────────────────────────────────────────────────────

def normaliser_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    R1 — Normalisation de la casse des catégories.
    Source : 'electronique' | 'ELECTRONIQUE' | 'Electronique'
    Cible  : 'Electronique' (title case)

    Même règle pour sous_categorie et marque.
    """
    df = df.copy()

    # Mapping explicite pour les variantes connues + title case générique
    mapping_cat: dict[str, str] = {
        "electronique":  "Electronique",
        "ELECTRONIQUE":  "Electronique",
        "Electronique":  "Electronique",
        "mode":          "Mode",
        "MODE":          "Mode",
        "Mode":          "Mode",
        "alimentation":  "Alimentation",
        "ALIMENTATION":  "Alimentation",
        "Alimentation":  "Alimentation",
    }

    avant_dist = df["categorie"].value_counts().to_dict()

    df["categorie"]      = df["categorie"].map(mapping_cat).fillna(
        df["categorie"].str.title()
    )
    df["sous_categorie"] = df["sous_categorie"].str.strip().str.title()
    df["marque"]         = df["marque"].str.strip()
    df["fournisseur"]    = df["fournisseur"].str.strip()

    nb_remappes = sum(
        1 for k in avant_dist if k not in {"Electronique", "Mode", "Alimentation"}
    )
    log.log_replace("R1 — Normalisation catégories", "produits", nb_remappes,
                    f"Distribution avant : {avant_dist}")
    return df


def gerer_prix_nuls(df: pd.DataFrame, prix_defaut: float = 0.01) -> pd.DataFrame:
    """
    R2 — Gestion des prix catalogue null (anciens produits).
    Règle : les prix null correspondent à des produits discontinués.
    On les remplace par 0.01 (symbolique) pour maintenir l'intégrité référentielle
    et on logue la liste des produits concernés.
    """
    df = df.copy()
    df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")

    masque_null = df["prix_catalogue"].isna()
    nb_null = masque_null.sum()

    if nb_null > 0:
        produits_null = df.loc[masque_null, "id_produit"].tolist()
        log.warning(
            f"[TRANSFORM] R2 — {nb_null} produits avec prix_catalogue NULL : "
            f"{produits_null} → remplacés par {prix_defaut}"
        )
        df.loc[masque_null, "prix_catalogue"] = prix_defaut

    df["prix_catalogue"] = df["prix_catalogue"].round(2)

    log.log_replace("R2 — Prix catalogue null", "produits", nb_null,
                    f"Produits discontinués : {nb_null}")
    return df


def nettoyer_champs_texte(df: pd.DataFrame) -> pd.DataFrame:
    """
    R3 — Nettoyage des champs texte : strip, suppression espaces multiples.
    Champs concernés : nom, marque, fournisseur, origine_pays.
    """
    df = df.copy()
    champs_texte = ["nom", "marque", "fournisseur", "origine_pays"]

    for col in champs_texte:
        if col in df.columns:
            df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True)

    log.info("[TRANSFORM] R3 — Champs texte nettoyés (strip + espaces)")
    return df


def preparer_scd_type2(df: pd.DataFrame) -> pd.DataFrame:
    """
    R4 — Préparation des colonnes SCD Type 2.
    Les produits 'actif=False' qui ont des commandes associées doivent
    être conservés avec leur historique dans dim_produit.

    Colonnes ajoutées :
      date_debut  : date de création du produit (ou date d'import)
      date_fin    : '9999-12-31' pour les lignes actives
      est_actif   : True pour les versions courantes

    Note : la mise à jour effective des SCD est gérée dans build_dimensions.py
    lors du chargement incrémental.
    """
    df = df.copy()

    # Convertir la colonne 'actif' en booléen (source JSON peut avoir True/False/str)
    if df["actif"].dtype == object:
        df["actif"] = df["actif"].map(
            {"true": True, "True": True, "false": False, "False": False}
        ).fillna(df["actif"])

    df["actif"] = df["actif"].astype(bool)

    # Colonnes SCD Type 2
    today = pd.Timestamp(date.today())
    df["date_debut"] = pd.to_datetime(df["date_creation"], errors="coerce").fillna(today)
    df["date_fin"]   = pd.Timestamp("9999-12-31")
    df["est_actif"]  = True   # Toutes les lignes sont actives à l'import initial

    nb_inactifs = (~df["actif"]).sum()
    log.log_replace("R4 — Colonnes SCD Type 2", "produits", len(df),
                    f"{nb_inactifs} produits marqués actif=False (à gérer en SCD)")
    return df


def valider_dates_creation(df: pd.DataFrame) -> pd.DataFrame:
    """
    R5 — Validation des dates de création produit.
    Les dates invalides ou futures sont remplacées par la date du jour.
    """
    df = df.copy()
    df["date_creation"] = pd.to_datetime(df["date_creation"], errors="coerce")

    today = pd.Timestamp(date.today())
    masque_invalide = df["date_creation"].isna() | (df["date_creation"] > today)
    nb_invalides = masque_invalide.sum()

    df.loc[masque_invalide, "date_creation"] = today

    if nb_invalides:
        log.log_replace("R5 — Dates création invalides", "produits", nb_invalides)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les produits Mexora.

    Parameters
    ----------
    df : DataFrame brut issu du JSON (via extract_produits)

    Returns
    -------
    pd.DataFrame propre, prêt pour la construction de dim_produit.
    """
    initial = len(df)
    log.section("TRANSFORM — Produits")
    log.info(f"[TRANSFORM] Début nettoyage produits : {initial} lignes en entrée")

    df = normaliser_categories(df)
    df = gerer_prix_nuls(df)
    df = nettoyer_champs_texte(df)
    df = valider_dates_creation(df)
    df = preparer_scd_type2(df)

    final = len(df)
    log.info(f"[TRANSFORM] Produits terminé : {initial} → {final} lignes (aucune suppression)")

    return df
