import os
import logging
from datetime import datetime
import sqlalchemy
import pandas as pd

# =====================================================================
# IMPORTS DES MODULES DU PROJET
# =====================================================================

from extract.extractor import extract_commandes, extract_produits, extract_clients, extract_regions
from transform.clean_commandes import transform_commandes
from transform.clean_clients import transform_clients
from transform.clean_produits import transform_produits
from transform.build_dimensions import build_dim_temps, build_dim_client, build_dim_produit, build_dim_region, build_dim_livreur
from transform.build_faits import build_fait_ventes
from load.loader import charger_dimension, charger_faits
# =====================================================================
# CONFIGURATION et demarage du pipeline
print(">>> LE SCRIPT DÉMARRE ENFIN...")
# =====================================================================
# 1. Création du dossier logs s'il n'existe pas (Sécurité)
os.makedirs('logs', exist_ok=True)

# 2. Configuration du Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 3. Paramètres de connexion 
DB_URI = "postgresql://postgres:Admin123@localhost/mexora_dwh"

def run_pipeline():
    start = datetime.now()
    logging.info("=" * 60)
    logging.info("DÉMARRAGE PIPELINE ETL MEXORA")
    logging.info("=" * 60)

    try:
        # --- 1. EXTRACT ---
        logging.info("--- PHASE EXTRACT ---")
        df_commandes_raw = extract_commandes('data/commandes_mexora.csv')
        df_produits_raw  = extract_produits('data/produits_mexora.json')
        df_clients_raw   = extract_clients('data/clients_mexora.csv')
        df_regions       = extract_regions('data/regions_maroc.csv')

        # --- 2. TRANSFORM ---
        logging.info("--- PHASE TRANSFORM ---")
        df_commandes = transform_commandes(df_commandes_raw)
        df_clients   = transform_clients(df_clients_raw)
        df_produits  = transform_produits(df_produits_raw)

        dim_temps    = build_dim_temps('2020-01-01', '2025-12-31')
        dim_client   = build_dim_client(df_clients, df_commandes)
        dim_produit  = build_dim_produit(df_produits) 
        dim_region   = build_dim_region(df_regions) 
        dim_livreur  = build_dim_livreur(df_commandes) 
        fait_ventes  = build_fait_ventes(df_commandes, dim_temps, dim_client, dim_produit, dim_region, dim_livreur) 

        df_clients = df_clients.rename(columns={'id_client': 'id_client_nk', 'ville': 'ville_residence'})
        df_produits = df_produits.rename(columns={'id_produit': 'id_produit_nk', 'nom': 'nom_produit'})
        fait_ventes = fait_ventes.rename(columns={'id_commande': 'id_vente', 'quantite': 'quantite_vendue'})
        # --- 2.5 NETTOYAGE DES NOMS ET COLONNES ---
        
        # On renomme quantité et statut, MAIS on ne touche pas à id_commande ici
        fait_ventes = fait_ventes.rename(columns={'quantite': 'quantite_vendue', 'statut': 'statut_commande'})

        # --- 3. LOAD ---
        logging.info("--- PHASE LOAD ---")
        engine = sqlalchemy.create_engine(DB_URI)

        charger_dimension(dim_temps,   'dim_temps',   engine)
        charger_dimension(dim_client,  'dim_client',  engine)
        charger_dimension(dim_produit, 'dim_produit', engine) 
        charger_dimension(dim_region,  'dim_region',  engine) 
        charger_dimension(dim_livreur, 'dim_livreur', engine) 

        # --- 4. ÉTAPE DE TRADUCTION DES CLÉS (LOOKUPS BLINDÉS) ---
        logging.info("--- LOOKUPS DES FAITS ---")

        # 1. Lookup Produits
        dim_prod = pd.read_sql("SELECT id_produit_sk, id_produit_nk FROM dwh_mexora.dim_produit", engine)
        fait_ventes = fait_ventes.merge(dim_prod, left_on='id_produit', right_on='id_produit_nk', how='left')

        # 2. Lookup Clients
        dim_client = pd.read_sql("SELECT id_client_sk, id_client_nk FROM dwh_mexora.dim_client", engine)
        fait_ventes = fait_ventes.merge(dim_client, left_on='id_client', right_on='id_client_nk', how='left')

        # 3. Lookup Livreurs
        dim_livreur = pd.read_sql("SELECT id_livreur_sk, id_livreur_nk FROM dwh_mexora.dim_livreur", engine)
        fait_ventes = fait_ventes.merge(dim_livreur, left_on='id_livreur', right_on='id_livreur_nk', how='left')

        # 4. Lookup Région
        dim_region = pd.read_sql("SELECT id_region_sk, ville FROM dwh_mexora.dim_region", engine)
        fait_ventes = fait_ventes.merge(dim_region, left_on='ville_livraison', right_on='ville', how='left')

        # Remplacement par Inconnu (-1) et formatage
        fait_ventes['id_produit'] = fait_ventes['id_produit_sk'].fillna(-1).astype(int)
        fait_ventes['id_client']  = fait_ventes['id_client_sk'].fillna(-1).astype(int)
        fait_ventes['id_livreur'] = fait_ventes['id_livreur_sk'].fillna(-1).astype(int)
        fait_ventes['id_region']  = fait_ventes['id_region_sk'].fillna(-1).astype(int)

        # 5. La transformation de la Date (format YYYYMMDD en Entier)
        fait_ventes['id_date'] = pd.to_datetime(fait_ventes['date_commande']).dt.strftime('%Y%m%d').astype(int)

        # --- 5. LE COUP DE BALAI FINAL (CRUCIAL) ---
        # On supprime toutes les colonnes "textes" brutes pour ne garder que les entiers attendus par DBeaver
        cols_to_drop = [
            'id_commande', 'date_commande', 'ville_livraison', # Les données brutes d'origine
            'id_produit_sk', 'id_produit_nk', 
            'id_client_sk', 'id_client_nk', 
            'id_livreur_sk', 'id_livreur_nk',
            'id_region_sk', 'ville'
        ]
        fait_ventes = fait_ventes.drop(columns=[c for c in cols_to_drop if c in fait_ventes.columns])

        # --- 6. CHARGEMENT FINAL ---
        logging.info(">>> LOOKUP TERMINÉ : Insertion dans fait_ventes en cours...")
        charger_faits(fait_ventes, engine)

        # --- 3. LOAD ---
        logging.info("--- PHASE LOAD ---")
        engine = sqlalchemy.create_engine(DB_URI)

        charger_dimension(dim_temps,   'dim_temps',   engine)
        charger_dimension(dim_client,  'dim_client',  engine)
        charger_dimension(dim_produit, 'dim_produit', engine) 
        charger_dimension(dim_region,  'dim_region',  engine) 
        charger_dimension(dim_livreur, 'dim_livreur', engine) 


        # --- ÉTAPE DE TRADUCTION DES CLÉS (VERSION BLINDÉE) ---

        # 1. Lookup Produits
        dim_prod = pd.read_sql("SELECT id_produit_sk, id_produit_nk FROM dwh_mexora.dim_produit", engine)
        fait_ventes = fait_ventes.merge(dim_prod, left_on='id_produit', right_on='id_produit_nk', how='left')

        # 2. Lookup Clients
        dim_client = pd.read_sql("SELECT id_client_sk, id_client_nk FROM dwh_mexora.dim_client", engine)
        fait_ventes = fait_ventes.merge(dim_client, left_on='id_client', right_on='id_client_nk', how='left')

        # 3. Lookup Livreurs
        dim_livreur = pd.read_sql("SELECT id_livreur_sk, id_livreur_nk FROM dwh_mexora.dim_livreur", engine)
        fait_ventes = fait_ventes.merge(dim_livreur, left_on='id_livreur', right_on='id_livreur_nk', how='left')

        # --- NETTOYAGE CRUCIAL POUR POSTGRESQL ---

        # Remplacer les IDs non trouvés par notre fameux -1 (Inconnu)
        fait_ventes['id_produit_sk'] = fait_ventes['id_produit_sk'].fillna(-1)
        fait_ventes['id_client_sk'] = fait_ventes['id_client_sk'].fillna(-1)
        fait_ventes['id_livreur_sk'] = fait_ventes['id_livreur_sk'].fillna(-1)

        # FORCER le type Entier (Pour éviter le bug du 10.0)
        fait_ventes['id_produit'] = fait_ventes['id_produit_sk'].astype(int)
        fait_ventes['id_client'] = fait_ventes['id_client_sk'].astype(int)
        fait_ventes['id_livreur'] = fait_ventes['id_livreur_sk'].astype(int)
        # --- 4. Le Lookup oublié : La Région ---
        dim_region = pd.read_sql("SELECT id_region_sk, ville FROM dwh_mexora.dim_region", engine)
        # On suppose que ta colonne s'appelle 'ville_livraison' dans tes ventes
        fait_ventes = fait_ventes.merge(dim_region, left_on='ville_livraison', right_on='ville', how='left')

        # On remplace les villes inconnues par -1 et on force l'entier
        fait_ventes['id_region_sk'] = fait_ventes['id_region_sk'].fillna(-1).astype(int)
        fait_ventes['id_region'] = fait_ventes['id_region_sk']


        # --- 5. La transformation de la Date (format YYYYMMDD en Entier) ---
        # On s'assure que la date devient un nombre comme 20240516
        fait_ventes['id_date'] = pd.to_datetime(fait_ventes['date_commande']).dt.strftime('%Y%m%d').astype(int)


        # (N'oublie pas d'ajouter 'id_region_sk' et 'ville' dans la liste cols_to_drop pour nettoyer)
                # Nettoyage final des colonnes de jointure
        cols_to_drop = ['id_produit_sk', 'id_produit_nk', 'id_client_sk', 'id_client_nk', 'id_livreur_sk', 'id_livreur_nk']
        fait_ventes = fait_ventes.drop(columns=[c for c in cols_to_drop if c in fait_ventes.columns])

        print(">>> LOOKUP TERMINÉ : Données prêtes pour PostgreSQL !")

        charger_faits(fait_ventes, engine)

        duree = (datetime.now() - start).seconds
        logging.info(f"PIPELINE TERMINÉ AVEC SUCCÈS EN {duree} SECONDES")

    except Exception as e:
        logging.error(f"ERREUR CRITIQUE PIPELINE : {e}", exc_info=True)
        raise
if __name__ == "__main__":
    run_pipeline()