"""
transform/build_dimensions.py — Construction des tables de dimensions du DWH Mexora

Tables construites :
  - DIM_TEMPS    : calendrier enrichi avec fériés marocains et Ramadan
  - DIM_PRODUIT  : dimension produit (SCD Type 2)
  - DIM_CLIENT   : dimension client avec segmentation
  - DIM_REGION   : dimension géographique
  - DIM_LIVREUR  : dimension livreur
  - FAIT_VENTES  : table de faits avec résolution des clés surrogate
"""

import pandas as pd
import numpy as np
from datetime import date

from config.settings import (
    DIM_TEMPS_DATE_DEBUT, DIM_TEMPS_DATE_FIN,
    FERIES_MAROC, RAMADAN_PERIODES
)
from transform.clean_clients import calculer_segments_clients
from utils.logger import etl_logger as log


# ─────────────────────────────────────────────────────────────────────────────
# DIM_TEMPS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_temps(
    date_debut: str = DIM_TEMPS_DATE_DEBUT,
    date_fin:   str = DIM_TEMPS_DATE_FIN,
) -> pd.DataFrame:
    """
    Génère la dimension temporelle complète entre deux dates.
    Inclut les jours fériés marocains et les périodes Ramadan.

    id_date est au format YYYYMMDD (INTEGER) — clé primaire.
    """
    log.section("BUILD — DIM_TEMPS")

    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")
    feries_set = set(FERIES_MAROC)

    df = pd.DataFrame({
        "id_date":         dates.strftime("%Y%m%d").astype(int),
        "date_complete":   dates.date,
        "jour":            dates.day.astype("int16"),
        "mois":            dates.month.astype("int16"),
        "trimestre":       dates.quarter.astype("int16"),
        "annee":           dates.year.astype("int16"),
        "semaine":         dates.isocalendar().week.astype("int16"),
        "libelle_jour":    dates.strftime("%A"),         # Lundi, Mardi, …
        "libelle_mois":    dates.strftime("%B"),         # Janvier, Février, …
        "libelle_trimestre": ("T" + dates.quarter.astype(str)
                              + " " + dates.year.astype(str)),
        "est_weekend":     (dates.dayofweek >= 5),
        "est_ferie_maroc": dates.strftime("%Y-%m-%d").isin(feries_set),
    })

    # Calcul période Ramadan
    df["periode_ramadan"] = False
    for debut, fin in RAMADAN_PERIODES:
        masque = (df["date_complete"] >= pd.Timestamp(debut).date()) & \
                 (df["date_complete"] <= pd.Timestamp(fin).date())
        df.loc[masque, "periode_ramadan"] = True

    log.log_extract("DIM_TEMPS construite", len(df))
    log.info(f"[BUILD] DIM_TEMPS : {date_debut} → {date_fin} | "
             f"{df['est_ferie_maroc'].sum()} fériés | "
             f"{df['periode_ramadan'].sum()} jours Ramadan")

    return df[[
        "id_date", "jour", "mois", "trimestre", "annee", "semaine",
        "libelle_jour", "libelle_mois", "libelle_trimestre",
        "est_weekend", "est_ferie_maroc", "periode_ramadan"
    ]]


# ─────────────────────────────────────────────────────────────────────────────
# DIM_PRODUIT  (SCD Type 2)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension produit.
    SCD Type 2 : une ligne par version du produit.

    Colonnes SCD : date_debut | date_fin | est_actif
    Clé surrogate : id_produit_sk (auto-incrément via index+1)
    Clé naturelle : id_produit_nk
    """
    log.section("BUILD — DIM_PRODUIT")

    dim = df_produits[[
        "id_produit", "nom", "categorie", "sous_categorie",
        "marque", "fournisseur", "prix_catalogue", "origine_pays",
        "date_creation", "actif", "date_debut", "date_fin", "est_actif"
    ]].copy()

    dim = dim.rename(columns={
        "id_produit":    "id_produit_nk",
        "nom":           "nom_produit",
        "prix_catalogue":"prix_standard",
    })

    # Clé surrogate = index + 1
    dim = dim.reset_index(drop=True)
    dim.insert(0, "id_produit_sk", dim.index + 1)

    log.log_extract("DIM_PRODUIT construite", len(dim))
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# DIM_CLIENT  (SCD Type 1 pour attributs courants + segmentation)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_client(
    df_clients: pd.DataFrame,
    df_commandes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la dimension client enrichie de la segmentation.

    Segmentation Gold/Silver/Bronze calculée depuis df_commandes.
    SCD Type 2 préparé pour segment_client (peut changer chaque mois).
    """
    log.section("BUILD — DIM_CLIENT")

    # Segmentation
    segments = calculer_segments_clients(df_commandes)

    dim = df_clients[[
        "id_client", "nom_complet", "tranche_age", "sexe",
        "ville", "canal_acquisition", "date_inscription"
    ]].copy()

    # Joindre la région (ville → région via dim_region si dispo)
    dim = dim.rename(columns={"id_client": "id_client_nk"})

    # Jointure avec segments
    if not segments.empty:
        dim = dim.merge(
            segments[["id_client", "segment_client"]],
            left_on="id_client_nk",
            right_on="id_client",
            how="left",
        ).drop(columns=["id_client"], errors="ignore")
    else:
        dim["segment_client"] = "Bronze"

    dim["segment_client"] = dim["segment_client"].fillna("Bronze")

    # Colonnes SCD Type 2
    today = pd.Timestamp(date.today())
    dim["date_debut"] = today
    dim["date_fin"]   = pd.Timestamp("9999-12-31")
    dim["est_actif"]  = True

    # Clé surrogate
    dim = dim.reset_index(drop=True)
    dim.insert(0, "id_client_sk", dim.index + 1)

    dist_seg = dim["segment_client"].value_counts().to_dict()
    log.log_extract("DIM_CLIENT construite", len(dim))
    log.info(f"[BUILD] DIM_CLIENT segments : {dist_seg}")

    return dim


# ─────────────────────────────────────────────────────────────────────────────
# DIM_REGION
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension géographique à partir du référentiel propre.
    Ajoute une ligne 'Non renseignée' pour les villes non trouvées.
    """
    log.section("BUILD — DIM_REGION")

    dim = df_regions[[
        "nom_ville_standard", "province", "region_admin", "zone_geo"
    ]].copy()

    dim = dim.rename(columns={"nom_ville_standard": "ville"})
    dim["pays"] = "Maroc"

    # Ligne pour les villes non renseignées (clé -1)
    inconnu = pd.DataFrame([{
        "ville": "Non renseignée",
        "province": "Inconnu",
        "region_admin": "Inconnu",
        "zone_geo": "Inconnu",
        "pays": "Maroc",
    }])
    dim = pd.concat([inconnu, dim], ignore_index=True)

    dim = dim.reset_index(drop=True)
    dim.insert(0, "id_region", dim.index + 1)
    # id_region=1 → 'Non renseignée' (référence pour les villes inconnues)

    log.log_extract("DIM_REGION construite", len(dim))
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# DIM_LIVREUR
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension livreur à partir des codes livreurs présents
    dans les commandes.

    Note : dans un vrai projet, cette dimension viendrait d'un système RH.
    Ici on l'infère des commandes et on génère des données plausibles.
    """
    log.section("BUILD — DIM_LIVREUR")

    TYPES_TRANSPORT = ["Camionnette", "Moto", "Vélo électrique", "Voiture"]
    ZONES = [
        "Tanger Nord", "Tanger Sud", "Casablanca Centre", "Casablanca Est",
        "Rabat", "Marrakech", "Fès", "Agadir", "National"
    ]

    import random
    random.seed(99)

    livreurs_codes = df_commandes["id_livreur"].dropna().unique()
    livreurs_codes = [c for c in livreurs_codes if c != "-1"]

    rows = []
    for code in sorted(livreurs_codes):
        rows.append({
            "id_livreur_nk":  code,
            "nom_livreur":    f"Livreur {code}",
            "type_transport": random.choice(TYPES_TRANSPORT),
            "zone_couverture":random.choice(ZONES),
        })

    # Livreur inconnu (id_livreur_nk = '-1')
    rows.insert(0, {
        "id_livreur_nk":  "-1",
        "nom_livreur":    "Livreur Inconnu",
        "type_transport": "Inconnu",
        "zone_couverture":"Inconnu",
    })

    dim = pd.DataFrame(rows).reset_index(drop=True)
    dim.insert(0, "id_livreur", dim.index + 1)

    log.log_extract("DIM_LIVREUR construite", len(dim))
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# FAIT_VENTES
# ─────────────────────────────────────────────────────────────────────────────

def build_fait_ventes(
    df_commandes: pd.DataFrame,
    dim_temps:    pd.DataFrame,
    dim_client:   pd.DataFrame,
    dim_produit:  pd.DataFrame,
    dim_region:   pd.DataFrame,
    dim_livreur:  pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la table de faits FAIT_VENTES.

    Résout les clés surrogate en jointures sur les dimensions.
    Granularité : une ligne = une ligne de commande (commande × produit).

    Mesures :
      - quantite_vendue    (additive)
      - montant_ht         (additive)
      - montant_ttc        (additive)
      - cout_livraison     (additive — fixé à 0 si inconnu)
      - delai_livraison_jours (semi-additive)
      - remise_pct         (non-additive)
    """
    log.section("BUILD — FAIT_VENTES")

    df = df_commandes.copy()

    # ── Résolution clé DIM_TEMPS ──────────────────────────────────────────────
    df["id_date"] = df["date_commande"].dt.strftime("%Y%m%d").astype(int)

    temps_keys = dim_temps[["id_date"]].copy()
    df = df.merge(temps_keys, on="id_date", how="left")

    nb_temps_manquants = df["id_date"].isna().sum()
    if nb_temps_manquants:
        log.warning(f"[BUILD] {nb_temps_manquants} dates non trouvées dans DIM_TEMPS")

    # ── Résolution clé DIM_PRODUIT ────────────────────────────────────────────
    prod_sk_map = dim_produit.set_index("id_produit_nk")["id_produit_sk"]
    # Convertir en object str pour compatibilité avec Arrow-backed StringDtype
    df["id_produit"] = df["id_produit"].astype(object).map(prod_sk_map)

    nb_prod_manquants = df["id_produit"].isna().sum()
    if nb_prod_manquants:
        log.warning(f"[BUILD] {nb_prod_manquants} produits non trouvés dans DIM_PRODUIT")

    # ── Résolution clé DIM_CLIENT ─────────────────────────────────────────────
    client_sk_map = (
        dim_client.drop_duplicates(subset=["id_client_nk"], keep="last")
        .set_index("id_client_nk")["id_client_sk"]
    )
    df["id_client_sk"] = df["id_client"].astype(object).map(client_sk_map)

    nb_cli_manquants = df["id_client_sk"].isna().sum()
    if nb_cli_manquants:
        log.warning(f"[BUILD] {nb_cli_manquants} clients non trouvés dans DIM_CLIENT")

    # ── Résolution clé DIM_REGION ─────────────────────────────────────────────
    region_sk_map = dim_region.set_index("ville")["id_region"]
    df["id_region"] = df["ville_livraison"].astype(object).map(region_sk_map)
    df["id_region"] = df["id_region"].fillna(1).astype(int)  # 1 = Non renseignée

    # ── Résolution clé DIM_LIVREUR ────────────────────────────────────────────
    livreur_sk_map = dim_livreur.set_index("id_livreur_nk")["id_livreur"]
    df["id_livreur_sk"] = df["id_livreur"].astype(object).map(livreur_sk_map).fillna(1).astype(int)

    # ── Coût livraison (non disponible dans la source → 0) ────────────────────
    df["cout_livraison"] = 0.0

    # ── Remise (non disponible dans la source → 0) ────────────────────────────
    df["remise_pct"] = 0.0

    # ── Sélection et renommage des colonnes finales ───────────────────────────
    fait = pd.DataFrame({
        "id_date":               df["id_date"],
        "id_produit":            df["id_produit"].fillna(-1).astype(int),
        "id_client":             df["id_client_sk"].fillna(-1).astype(int),
        "id_region":             df["id_region"],
        "id_livreur":            df["id_livreur_sk"],
        "quantite_vendue":       df["quantite"].astype(int),
        "montant_ht":            df["montant_ht"],
        "montant_ttc":           df["montant_ttc"],
        "cout_livraison":        df["cout_livraison"],
        "delai_livraison_jours": df["delai_livraison_jours"],
        "remise_pct":            df["remise_pct"],
        "statut_commande":       df["statut"],
        "date_chargement":       pd.Timestamp.now(),
    })

    # Supprimer les lignes sans clé valide (produit ou client non résolu)
    avant = len(fait)
    fait = fait.dropna(subset=["id_date", "id_produit", "id_client"])
    fait = fait[fait["id_produit"] > 0]
    fait = fait[fait["id_client"] > 0]

    log.log_transform("BUILD — FAIT_VENTES", avant, len(fait), "faits",
                      f"CA TTC total : {fait['montant_ttc'].sum():,.2f} MAD")

    return fait.reset_index(drop=True)
