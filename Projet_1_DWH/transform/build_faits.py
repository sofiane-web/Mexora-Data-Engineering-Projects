import pandas as pd
import numpy as np
import logging

def build_fait_ventes(df_commandes: pd.DataFrame, 
                      dim_temps: pd.DataFrame, 
                      dim_client: pd.DataFrame, 
                      dim_produit: pd.DataFrame, 
                      dim_region: pd.DataFrame, 
                      dim_livreur: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la table de faits centrale en croisant les commandes 
    avec toutes les dimensions pour récupérer les clés étrangères.
    """
    logging.info("[TRANSFORM] Construction de la table FAIT_VENTES...")
    
    # 1. Préparation de la base de faits (Commandes nettoyées)
    faits = df_commandes.copy()

    # 2. Résolution de id_date (Format YYYYMMDD) [cite: 521-525]
    # On s'assure que la date_commande est bien au format datetime
    faits['date_commande'] = pd.to_datetime(faits['date_commande'])
    faits['id_date'] = faits['date_commande'].dt.strftime('%Y%m%d').astype(int)

    # 3. Résolution des SK (Surrogate Keys) par jointure
    # Note : Puisque PostgreSQL génère les SERIAL PK à l'insertion, on utilise 
    # l'index du DataFrame + 1 pour simuler les IDs de la base de données.
    
    # Client
    dim_client_map = dim_client.reset_index().rename(columns={'index': 'id_client_sk'})
    dim_client_map['id_client_sk'] += 1
    faits = pd.merge(faits, dim_client_map[['id_client_nk', 'id_client_sk']], 
                     left_on='id_client', right_on='id_client_nk', how='left')

    # Produit
    dim_produit_map = dim_produit.reset_index().rename(columns={'index': 'id_produit_sk'})
    dim_produit_map['id_produit_sk'] += 1
    faits = pd.merge(faits, dim_produit_map[['id_produit_nk', 'id_produit_sk']], 
                     left_on='id_produit', right_on='id_produit_nk', how='left')

    # Région
    dim_region_map = dim_region.reset_index().rename(columns={'index': 'id_region_sk'})
    dim_region_map['id_region_sk'] += 1
    faits = pd.merge(faits, dim_region_map[['ville', 'id_region_sk']], 
                     left_on='ville_livraison', right_on='ville', how='left')

    # Livreur
    dim_livreur_map = dim_livreur.reset_index().rename(columns={'index': 'id_livreur_sk'})
    dim_livreur_map['id_livreur_sk'] += 1
    faits = pd.merge(faits, dim_livreur_map[['id_livreur_nk', 'id_livreur_sk']], 
                     left_on='id_livreur', right_on='id_livreur_nk', how='left')

    # 4. Calcul des mesures [cite: 661-677]
    faits['quantite_vendue'] = pd.to_numeric(faits['quantite'], errors='coerce').fillna(0).astype(int)
    faits['prix_u'] = pd.to_numeric(faits['prix_unitaire'], errors='coerce').fillna(0)
    
    faits['montant_ttc'] = faits['quantite_vendue'] * faits['prix_u']
    # Règle métier : Calcul du montant HT (TVA 20% au Maroc)
    faits['montant_ht'] = (faits['montant_ttc'] / 1.2).round(2)
    
    # Calcul du délai de livraison [cite: 674]
    faits['date_livraison'] = pd.to_datetime(faits['date_livraison'], errors='coerce')
    faits['delai_livraison_jours'] = (faits['date_livraison'] - faits['date_commande']).dt.days
    
    # Valeurs par défaut pour les mesures non fournies
    faits['cout_livraison'] = 40.0 # Forfait simulé
    faits['remise_pct'] = 0.0
    faits['statut_commande'] = faits['statut']

    # 5. Sélection finale des colonnes (ordre conforme au SQL) [cite: 636-684]
    colonnes_finales = [
        'id_date', 'id_produit_sk', 'id_client_sk', 'id_region_sk', 'id_livreur_sk',
        'quantite_vendue', 'montant_ht', 'montant_ttc', 'cout_livraison',
        'delai_livraison_jours', 'remise_pct', 'statut_commande'
    ]
    
    # Renommage pour correspondre exactement aux noms SQL
    df_final = faits[colonnes_finales].rename(columns={
        'id_produit_sk': 'id_produit',
        'id_client_sk': 'id_client',
        'id_region_sk': 'id_region',
        'id_livreur_sk': 'id_livreur'
    })

    logging.info(f"[TRANSFORM] Table de faits terminée : {len(df_final)} transactions prêtes.")
    return df_final