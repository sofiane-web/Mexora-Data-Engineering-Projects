"""
transform/clean_commandes.py — Nettoyage et transformation des commandes Mexora

Règles appliquées (documentées pour le rapport) :
  R1 — Suppression des doublons sur id_commande (conserver la dernière occurrence)
  R2 — Standardisation des dates (format cible : YYYY-MM-DD)
  R3 — Harmonisation des noms de villes via le référentiel régions_maroc
  R4 — Standardisation des statuts de commande
  R5 — Suppression des lignes avec quantite <= 0 (erreurs de saisie)
  R6 — Suppression des lignes avec prix_unitaire = 0 (commandes test)
  R7 — Remplacement des id_livreur manquants par '-1' (livreur inconnu)
  R8 — Calcul du montant_ht et montant_ttc
  R9 — Calcul du délai de livraison en jours
"""

import pandas as pd
import numpy as np
from datetime import date

from config.settings import MAPPING_STATUTS, STATUTS_VALIDES, TVA
from utils.logger import etl_logger as log


# ─────────────────────────────────────────────────────────────────────────────
# Fonctions de nettoyage atomiques (testables unitairement)
# ─────────────────────────────────────────────────────────────────────────────

def supprimer_doublons(df: pd.DataFrame) -> pd.DataFrame:
    """
    R1 — Suppression des doublons sur id_commande.
    Règle métier : en cas de doublon, on conserve la dernière occurrence
    (la ré-insertion est supposée être la version la plus récente).
    """
    avant = len(df)
    df = df.drop_duplicates(subset=["id_commande"], keep="last")
    log.log_transform("R1 — Doublons id_commande", avant, len(df), "commandes",
                      f"{avant - len(df)} doublons supprimés")
    return df.reset_index(drop=True)


def standardiser_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    R2 — Standardisation des dates de commande.
    Formats source possibles : DD/MM/YYYY | YYYY-MM-DD | Mon DD YYYY
    Format cible : YYYY-MM-DD (datetime64)
    Les dates non parsables sont supprimées (invalides métier).
    """
    avant = len(df)

    df["date_commande"] = pd.to_datetime(
        df["date_commande"],
        format="mixed",   # pandas 2.x — détecte automatiquement le format
        dayfirst=True,    # DD/MM/YYYY prioritaire
        errors="coerce",  # invalides → NaT
    )

    nb_nat = df["date_commande"].isna().sum()
    df = df.dropna(subset=["date_commande"])

    # Même traitement pour date_livraison (moins critique — peut être vide)
    df["date_livraison"] = pd.to_datetime(
        df["date_livraison"],
        format="mixed",
        dayfirst=True,
        errors="coerce",
    )

    log.log_transform("R2 — Standardisation dates", avant, len(df), "commandes",
                      f"{nb_nat} dates_commande invalides supprimées")
    return df


def harmoniser_villes(df: pd.DataFrame, mapping_villes: dict) -> pd.DataFrame:
    """
    R3 — Harmonisation des noms de villes via le référentiel.
    Les villes non trouvées dans le référentiel sont conservées mais
    taguées 'Non renseignée' pour permettre une analyse ultérieure.
    """
    df = df.copy()
    ville_brute = df["ville_livraison"].str.strip().str.lower().fillna("inconnu")

    ville_harmonisee = ville_brute.map(mapping_villes)
    nb_non_mappes = ville_harmonisee.isna().sum()

    df["ville_livraison"] = ville_harmonisee.fillna("Non renseignée")

    log.log_replace("R3 — Harmonisation villes", "commandes", len(df),
                    f"{nb_non_mappes} villes non trouvées dans le référentiel")
    return df


def standardiser_statuts(df: pd.DataFrame) -> pd.DataFrame:
    """
    R4 — Standardisation des statuts de commande.
    Valeurs cibles : livré | annulé | en_cours | retourné
    Les valeurs non reconnues après mapping → 'inconnu'
    """
    df = df.copy()
    statut_avant = df["statut"].copy()

    df["statut"] = df["statut"].map(MAPPING_STATUTS)

    nb_inconnus = (~df["statut"].isin(STATUTS_VALIDES)).sum()
    df["statut"] = df["statut"].where(df["statut"].isin(STATUTS_VALIDES), "inconnu")

    nb_remappes = (statut_avant != df["statut"]).sum()
    log.log_replace("R4 — Standardisation statuts", "commandes", nb_remappes,
                    f"{nb_inconnus} statuts → 'inconnu'")
    return df


def filtrer_quantites_invalides(df: pd.DataFrame) -> pd.DataFrame:
    """
    R5 — Suppression des lignes avec quantite <= 0.
    Règle métier : une commande doit avoir au moins 1 unité commandée.
    Les quantités négatives sont des erreurs de saisie documentées.
    """
    avant = len(df)
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")

    # Supprimer quantite NaN ou <= 0
    df = df[df["quantite"].notna() & (df["quantite"] > 0)].copy()
    df["quantite"] = df["quantite"].astype(int)

    log.log_transform("R5 — Quantités invalides (<=0)", avant, len(df), "commandes",
                      f"{avant - len(df)} lignes supprimées")
    return df


def filtrer_prix_nuls(df: pd.DataFrame) -> pd.DataFrame:
    """
    R6 — Suppression des lignes avec prix_unitaire = 0.
    Règle métier : les commandes à prix 0 sont des commandes test à exclure.
    """
    avant = len(df)
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")

    df = df[df["prix_unitaire"].notna() & (df["prix_unitaire"] > 0)].copy()
    df["prix_unitaire"] = df["prix_unitaire"].round(2)

    log.log_transform("R6 — Prix nuls (commandes test)", avant, len(df), "commandes",
                      f"{avant - len(df)} commandes test supprimées")
    return df


def gerer_livreurs_manquants(df: pd.DataFrame) -> pd.DataFrame:
    """
    R7 — Remplacement des id_livreur manquants par '-1' (livreur inconnu).
    Règle métier : un livreur inconnu correspond souvent à un retrait en point
    relais ou à une erreur de saisie — on ne supprime pas la commande.
    """
    nb_manquants = df["id_livreur"].isna().sum()
    df["id_livreur"] = df["id_livreur"].fillna("-1")

    log.log_replace("R7 — Livreurs manquants", "commandes", nb_manquants,
                    f"{nb_manquants} valeurs remplacées par '-1'")
    return df


def calculer_montants(df: pd.DataFrame) -> pd.DataFrame:
    """
    R8 — Calcul des montants HT et TTC.
    montant_ht  = quantite × prix_unitaire
    montant_ttc = montant_ht × (1 + TVA)  [TVA Maroc = 20%]
    """
    df = df.copy()
    df["montant_ht"]  = (df["quantite"] * df["prix_unitaire"]).round(2)
    df["montant_ttc"] = (df["montant_ht"] * (1 + TVA)).round(2)

    log.info(f"[TRANSFORM] R8 — Montants HT/TTC calculés | "
             f"CA TTC total brut : {df['montant_ttc'].sum():,.2f} MAD")
    return df


def calculer_delai_livraison(df: pd.DataFrame) -> pd.DataFrame:
    """
    R9 — Calcul du délai de livraison en jours.
    delai_livraison_jours = date_livraison - date_commande
    NULL si date_livraison absente (commandes en cours ou annulées).
    Délais négatifs (incohérence) → NULL.
    """
    df = df.copy()
    df["delai_livraison_jours"] = (
        df["date_livraison"] - df["date_commande"]
    ).dt.days

    # Délais négatifs = données incohérentes
    nb_negatifs = (df["delai_livraison_jours"] < 0).sum()
    df.loc[df["delai_livraison_jours"] < 0, "delai_livraison_jours"] = pd.NA

    if nb_negatifs:
        log.warning(f"[TRANSFORM] R9 — {nb_negatifs} délais négatifs neutralisés")

    log.info(f"[TRANSFORM] R9 — Délai moyen livraison : "
             f"{df['delai_livraison_jours'].mean():.1f} jours")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def transform_commandes(
    df: pd.DataFrame,
    mapping_villes: dict,
) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les commandes Mexora.

    Parameters
    ----------
    df             : DataFrame brut extrait depuis CSV
    mapping_villes : dict {variante → nom_standard} issu du référentiel régions

    Returns
    -------
    pd.DataFrame propre, avec mesures calculées, prêt pour la phase Load.
    """
    initial = len(df)
    log.section("TRANSFORM — Commandes")
    log.info(f"[TRANSFORM] Début nettoyage commandes : {initial} lignes en entrée")

    df = supprimer_doublons(df)
    df = standardiser_dates(df)
    df = harmoniser_villes(df, mapping_villes)
    df = standardiser_statuts(df)
    df = filtrer_quantites_invalides(df)
    df = filtrer_prix_nuls(df)
    df = gerer_livreurs_manquants(df)
    df = calculer_montants(df)
    df = calculer_delai_livraison(df)

    final = len(df)
    log.info(
        f"[TRANSFORM] Commandes terminé : {initial} → {final} lignes "
        f"({initial - final} supprimées, {(initial - final) / initial * 100:.1f}%)"
    )

    return df
