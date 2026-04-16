import pandas as pd
import logging
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

def charger_dimension(df: pd.DataFrame, table_name: str, engine, if_exists='append'):
    """
    Charge une table de dimension dans PostgreSQL.
    Stratégie : replace (truncate + reload) pour les dimensions.
    """
    logging.info(f"[LOAD] Début du chargement de la dimension : {table_name}...")
    
    if df.empty:
        logging.warning(f"[LOAD] Le DataFrame pour {table_name} est vide. Annulation.")
        return

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema='dwh_mexora', # Schéma exigé par le projet
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        logging.info(f"[LOAD] {table_name} : {len(df)} lignes chargées avec succès.")
    except Exception as e:
        logging.error(f"[LOAD] Erreur fatale lors du chargement de {table_name} : {e}")
        raise # On relance l'erreur pour stopper le pipeline en cas de crash

def charger_faits(df: pd.DataFrame, engine):
    """
    Charge la table de faits avec une stratégie UPSERT.
    Utilise ON CONFLICT pour mettre à jour les lignes existantes.
    """
    logging.info("[LOAD] Début du chargement de la table de faits...")
    
    if df.empty:
        logging.warning("[LOAD] fait_ventes : Aucune ligne à charger.")
        return

    try:
        # 1. Réflexion de la table depuis la base de données (LA CORRECTION MAJEURE)
        metadata = MetaData(schema='dwh_mexora')
        table_fait_ventes = Table('fait_ventes', metadata, autoload_with=engine)

        # 2. Ouverture de la connexion
        with engine.connect() as conn:
            # 3. Découpage en lots (chunks) avec .iloc (plus sécurisé que le slicing simple)
            for i in range(0, len(df), 5000):
                chunk = df.iloc[i:i+5000]
                records = chunk.to_dict('records')
                
                # Construction de la requête d'insertion
                stmt = insert(table_fait_ventes).values(records)
                
                # Ajout de la clause ON CONFLICT DO UPDATE (Upsert)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id_vente'], # La clé primaire sur laquelle on vérifie le conflit
                    # On met à jour toutes les colonnes sauf id_vente
                    set_={c.key: c for c in stmt.excluded if c.key != 'id_vente'}
                )
                
                # Exécution et validation
                conn.execute(stmt)
                conn.commit() 
                
        logging.info(f"[LOAD] fait_ventes : {len(df)} lignes chargées (upsert réussi).")
    except Exception as e:
        logging.error(f"[LOAD] Erreur lors du chargement de fait_ventes : {e}")
        raise