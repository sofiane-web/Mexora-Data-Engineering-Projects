import pandas as pd
import numpy as np
import logging
import holidays
from datetime import datetime

# =====================================================================
# 1. DIMENSION TEMPS
# =====================================================================
def build_dim_temps(date_debut: str, date_fin: str) -> pd.DataFrame:
    """
    Génère la dimension temporelle complète entre deux dates.
    Inclut les jours fériés marocains générés dynamiquement et les périodes Ramadan.
    """
    logging.info(f"[TRANSFORM] Génération de la dimension temps de {date_debut} à {date_fin}...")
    
    dates = pd.date_range(start=date_debut, end=date_fin, freq='D')
    annees = dates.year.unique().tolist()
    
    jours_feries_ma = holidays.country_holidays('MA', years=annees)
    
    ramadan_periodes = [
        ('2020-04-24', '2020-05-23'), ('2021-04-13', '2021-05-12'),
        ('2022-04-03', '2022-05-01'), ('2023-03-23', '2023-04-20'),
        ('2024-03-11', '2024-04-09'), ('2025-02-28', '2025-03-29')
    ]

    df = pd.DataFrame({
        'id_date': dates.strftime('%Y%m%d').astype(int),
        'date_complete': dates,
        'jour': dates.day,
        'mois': dates.month,
        'trimestre': dates.quarter,
        'annee': dates.year,
        'semaine': dates.isocalendar().week.astype(int),
        'libelle_jour': dates.strftime('%A'),
        'libelle_mois': dates.strftime('%B'),
        'est_weekend': dates.dayofweek >= 5,
        # Utilisation de pd.Series pour pouvoir utiliser .apply()
        'est_ferie_maroc': pd.Series(dates.strftime('%Y-%m-%d')).apply(lambda d: d in jours_feries_ma).values,
    })

    df['periode_ramadan'] = False
    for debut, fin in ramadan_periodes:
        masque = (df['date_complete'] >= pd.to_datetime(debut)) & (df['date_complete'] <= pd.to_datetime(fin))
        df.loc[masque, 'periode_ramadan'] = True

    colonnes_finales = [
        'id_date', 'jour', 'mois', 'trimestre', 'annee', 'semaine',
        'libelle_jour', 'libelle_mois', 'est_weekend',
        'est_ferie_maroc', 'periode_ramadan'
    ]
    
    df_final = df[colonnes_finales]
    logging.info(f"[TRANSFORM] Dimension temps construite : {len(df_final)} jours générés.")
    return df_final


# =====================================================================
# 2. CALCUL DE LA SEGMENTATION (Fonction intermédiaire)
# =====================================================================
def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le segment client (Gold/Silver/Bronze) basé sur le CA cumulé
    des 12 derniers mois.
    """
    logging.info("[TRANSFORM] Début du calcul de la segmentation client...")
    
    date_limite = pd.Timestamp.now().normalize() - pd.DateOffset(years=1)
    df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'], errors='coerce')
    
    df_recents = df_commandes[
        (df_commandes['date_commande'] >= date_limite) &
        (df_commandes['statut'] == 'livré')
    ].copy()

    quantite = pd.to_numeric(df_recents['quantite'], errors='coerce').fillna(0)
    prix_unitaire = pd.to_numeric(df_recents['prix_unitaire'], errors='coerce').fillna(0)
    df_recents['montant_ttc'] = quantite * prix_unitaire

    ca_par_client = df_recents.groupby('id_client')['montant_ttc'].sum().reset_index()
    ca_par_client.rename(columns={'montant_ttc': 'ca_12m'}, inplace=True)

    ca_par_client['segment_client'] = pd.cut(
        ca_par_client['ca_12m'],
        bins=[-np.inf, 5000, 15000, np.inf],
        labels=['Bronze', 'Silver', 'Gold'],
        right=False
    ).astype(str)

    return ca_par_client[['id_client', 'segment_client', 'ca_12m']]


# =====================================================================
# 3. DIMENSION CLIENT
# =====================================================================
def build_dim_client(df_clients: pd.DataFrame, df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension DIM_CLIENT en fusionnant les infos de base 
    et la segmentation calculée via les commandes.
    """
    logging.info("[TRANSFORM] Construction de la dimension DIM_CLIENT...")

    # Calcul de la segmentation
    df_segments = calculer_segments_clients(df_commandes)

    # Préparation
    dim_client = df_clients.copy()
    dim_client['nom_complet'] = dim_client['nom'].str.upper() + ' ' + dim_client['prenom'].str.title()

    # Jointure
    dim_client = pd.merge(dim_client, df_segments, on='id_client', how='left')

    # Gestion des clients inactifs
    dim_client['segment_client'] = dim_client['segment_client'].fillna('Bronze')
    dim_client['ca_12m'] = dim_client['ca_12m'].fillna(0.0)

    # Préparation des colonnes pour PostgreSQL (SCD Type 2)
    dim_client = dim_client.rename(columns={'id_client': 'id_client_nk'})
    dim_client['date_debut'] = datetime.now().date()
    dim_client['date_fin'] = pd.to_datetime('9999-12-31').date()
    dim_client['est_actif'] = True

    colonnes_finales = [
        'id_client_nk', 'nom_complet', 'tranche_age', 'sexe', 
        'ville', 'segment_client', 'canal_acquisition',
        'date_debut', 'date_fin', 'est_actif'
    ]
    
    df_final = dim_client[colonnes_finales]
    logging.info(f"[TRANSFORM] Dimension Client terminée : {len(df_final)} lignes.")
    
    return df_final
# =====================================================================
# 4. DIMENSION PRODUIT
# =====================================================================
def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension DIM_PRODUIT avec gestion SCD Type 2.
    """
    logging.info("[TRANSFORM] Construction de la dimension DIM_PRODUIT...")
    
    dim_produit = df_produits.copy()
    
    # Mapping des colonnes vers le schéma SQL [cite: 549-571]
    dim_produit = dim_produit.rename(columns={
        'id_produit': 'id_produit_nk',
        'nom': 'nom_produit',
        'prix_catalogue': 'prix_standard'
    })
    
    # Ajout des métadonnées SCD Type 2 [cite: 572-577]
    # Note: date_debut est aujourd'hui, date_fin est l'infini (9999)
    dim_produit['date_debut'] = datetime.now().date()
    dim_produit['date_fin'] = pd.to_datetime('9999-12-31').date()
    # 'est_actif' est déjà géré par transform_produits, on s'assure juste du type
    dim_produit['est_actif'] = dim_produit['actif'].astype(bool)

    colonnes_finales = [
        'id_produit_nk', 'nom_produit', 'categorie', 'sous_categorie', 
        'marque', 'fournisseur', 'prix_standard', 'origine_pays',
        'date_debut', 'date_fin', 'est_actif'
    ]
    
    df_final = dim_produit[colonnes_finales]
    logging.info(f"[TRANSFORM] Dimension Produit terminée : {len(df_final)} lignes.")
    return df_final

# =====================================================================
# 5. DIMENSION RÉGION
# =====================================================================
def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension DIM_REGION à partir du référentiel géographique.
    """
    logging.info("[TRANSFORM] Construction de la dimension DIM_REGION...")
    
    dim_region = df_regions.copy()
    
    # Mapping vers le schéma SQL [cite: 608-621]
    dim_region = dim_region.rename(columns={
        'nom_ville_standard': 'ville'
    })
    
    # Le pays est fixé à 'Maroc' par défaut dans le schéma [cite: 620]
    if 'pays' not in dim_region.columns:
        dim_region['pays'] = 'Maroc'
        
    colonnes_finales = ['ville', 'province', 'region_admin', 'zone_geo', 'pays']
    
    # On s'assure de supprimer les doublons potentiels dans le référentiel
    df_final = dim_region[colonnes_finales].drop_duplicates()
    logging.info(f"[TRANSFORM] Dimension Région terminée : {len(df_final)} villes.")
    return df_final

# =====================================================================
# 6. DIMENSION LIVREUR
# =====================================================================
def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension DIM_LIVREUR à partir des IDs uniques des commandes.
    """
    logging.info("[TRANSFORM] Construction de la dimension DIM_LIVREUR...")
    
    # On extrait les livreurs uniques présents dans les commandes [cite: 185]
    livreurs_uniques = df_commandes['id_livreur'].unique()
    
    dim_livreur = pd.DataFrame({'id_livreur_nk': livreurs_uniques})
    
    # Règle métier : Pour les livreurs inconnus (-1), on assigne des valeurs par défaut
    # Pour les autres, on simule des noms (ou on les laisse vides selon besoin)
    dim_livreur['nom_livreur'] = dim_livreur['id_livreur_nk'].apply(
        lambda x: "Livreur Inconnu" if x == "-1" else f"Livreur Partenaire {x}"
    )
    dim_livreur['type_transport'] = "Moto" # Valeur par défaut
    dim_livreur['zone_couverture'] = "Nationale"
    
    colonnes_finales = ['id_livreur_nk', 'nom_livreur', 'type_transport', 'zone_couverture']
    
    df_final = dim_livreur[colonnes_finales]
    logging.info(f"[TRANSFORM] Dimension Livreur terminée : {len(df_final)} livreurs identifiés.")
    return df_final