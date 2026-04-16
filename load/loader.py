"""
load/loader.py — Phase LOAD du pipeline ETL Mexora
Chargement des dimensions et de la table de faits dans PostgreSQL.

Stratégies :
  - Dimensions : REPLACE (truncate + reload) — chargement complet à chaque run
  - Fait_ventes : UPSERT (ON CONFLICT DO UPDATE) — idempotent
"""

from tkinter import CASCADE

import pandas as pd
import sqlalchemy
from sqlalchemy import text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Literal

from config.settings import DATABASE_URL, SCHEMA_DWH, CHUNK_SIZE
from utils.logger import etl_logger as log


# ─────────────────────────────────────────────────────────────────────────────
# Initialisation de la connexion
# ─────────────────────────────────────────────────────────────────────────────

def creer_engine() -> sqlalchemy.Engine:
    DB_URI = "postgresql://postgres:Admin123@localhost:5432/mexora_dwh" 
    engine = sqlalchemy.create_engine(
        DATABASE_URL,
        pool_pre_ping=True,      # vérifie la connexion avant chaque requête
        pool_size=5,
        max_overflow=10,
        connect_args={"connect_timeout": 10},
    )

    # Test de connexion
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info(f"[LOAD] Connexion PostgreSQL établie → {DATABASE_URL.split('@')[-1]}")

    return engine


def creer_schemas(engine: sqlalchemy.Engine) -> None:
    """Crée les schémas PostgreSQL s'ils n'existent pas déjà."""
    schemas = ["staging_mexora", "dwh_mexora", "reporting_mexora"]
    with engine.connect() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()
    log.info(f"[LOAD] Schémas vérifiés/créés : {schemas}")


# ─────────────────────────────────────────────────────────────────────────────
# Chargement des dimensions (stratégie REPLACE)
# ─────────────────────────────────────────────────────────────────────────────

def charger_dimension(
    df: pd.DataFrame,
    table_name: str,
    engine: sqlalchemy.Engine,
    schema: str = SCHEMA_DWH,
    if_exists: Literal["replace", "append", "fail"] = "replace",
) -> None:
    """
    Charge une table de dimension dans PostgreSQL.

    Stratégie 'replace' : TRUNCATE + INSERT (cohérence totale à chaque run).
    Pour les dimensions stables (DIM_TEMPS, DIM_REGION), c'est optimal.

    Parameters
    ----------
    df         : DataFrame de la dimension
    table_name : Nom de la table cible (sans schéma)
    engine     : Moteur SQLAlchemy
    schema     : Schéma PostgreSQL cible (défaut : dwh_mexora)
    if_exists  : Stratégie pandas ('replace', 'append', 'fail')
    """
    if df.empty:
        log.warning(f"[LOAD] {table_name} — DataFrame vide, chargement ignoré")
        return

    # Conversion des types Python non-standards avant chargement
    df = _preparer_df_pour_sql(df)

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE,
        )
        log.log_load(f"{schema}.{table_name}", len(df), "replace")

    except Exception as exc:
        log.error(f"[LOAD] Échec chargement {schema}.{table_name} : {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Chargement de la table de faits (stratégie UPSERT)
# ─────────────────────────────────────────────────────────────────────────────

def charger_faits(
    df: pd.DataFrame,
    engine: sqlalchemy.Engine,
    table_name: str = "fait_ventes",
    schema: str = SCHEMA_DWH,
) -> None:
    """
    Charge la table de faits avec une stratégie UPSERT.
    Utilise ON CONFLICT sur id_vente pour mettre à jour les lignes existantes.

    Pour les nouveaux pipelines (première exécution), utilise un INSERT direct
    plus rapide (pas de conflit possible).

    Parameters
    ----------
    df         : DataFrame de la table de faits
    engine     : Moteur SQLAlchemy
    table_name : Nom de la table (défaut : fait_ventes)
    schema     : Schéma PostgreSQL (défaut : dwh_mexora)
    """
    if df.empty:
        log.warning("[LOAD] fait_ventes — DataFrame vide, chargement ignoré")
        return

    df = _preparer_df_pour_sql(df)

    # Vérifier si la table existe
    inspector = sqlalchemy.inspect(engine)
    table_existe = inspector.has_table(table_name, schema=schema)

    if not table_existe:
        # Premier chargement : INSERT simple (plus rapide)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE,
        )
        log.log_load(f"{schema}.{table_name}", len(df), "insert initial")
        return

    # Chargements suivants : UPSERT par chunks
    metadata = MetaData(schema=schema)
    metadata.reflect(bind=engine, schema=schema, only=[table_name])
    table_obj = Table(table_name, metadata, schema=schema, autoload_with=engine)

    total_upserted = 0
    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with engine.connect() as conn:
        for chunk in chunks:
            records = chunk.to_dict("records")
            stmt = pg_insert(table_obj).values(records)

            # ON CONFLICT : mise à jour de toutes les colonnes sauf la PK
            update_cols = {
                col.key: getattr(stmt.excluded, col.key)
                for col in table_obj.c
                if col.key != "id_vente"
            }

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["id_vente"],
                set_=update_cols,
            )
            conn.execute(upsert_stmt)
            total_upserted += len(chunk)

        conn.commit()

    log.log_load(f"{schema}.{table_name}", total_upserted, "upsert")


# ─────────────────────────────────────────────────────────────────────────────
# Vérification d'intégrité post-chargement
# ─────────────────────────────────────────────────────────────────────────────

def verifier_integrite(engine: sqlalchemy.Engine) -> dict:
    """
    Vérifie l'intégrité référentielle après chargement.
    Retourne un dict de métriques de qualité.

    Vérifications :
      - Clés étrangères orphelines dans fait_ventes
      - Doublons résiduels dans les dimensions
      - Valeurs NULL critiques
    """
    requetes = {
        "orphelins_date": f"""
            SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f
            LEFT JOIN {SCHEMA_DWH}.dim_temps t ON f.id_date = t.id_date
            WHERE t.id_date IS NULL
        """,
        "orphelins_produit": f"""
            SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f
            LEFT JOIN {SCHEMA_DWH}.dim_produit p ON f.id_produit = p.id_produit_sk
            WHERE p.id_produit_sk IS NULL
        """,
        "orphelins_client": f"""
            SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f
            LEFT JOIN {SCHEMA_DWH}.dim_client c ON f.id_client = c.id_client_sk
            WHERE c.id_client_sk IS NULL
        """,
        "nb_faits":    f"SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes",
        "nb_clients":  f"SELECT COUNT(*) FROM {SCHEMA_DWH}.dim_client",
        "nb_produits": f"SELECT COUNT(*) FROM {SCHEMA_DWH}.dim_produit",
        "ca_total_ttc":f"SELECT ROUND(SUM(montant_ttc)::numeric, 2) FROM {SCHEMA_DWH}.fait_ventes WHERE statut_commande = 'livré'",
    }

    resultats = {}
    with engine.connect() as conn:
        for nom, requete in requetes.items():
            try:
                val = conn.execute(text(requete)).scalar()
                resultats[nom] = val
                niveau = "warning" if (nom.startswith("orphelins") and val > 0) else "info"
                getattr(log, niveau)(f"[INTEGRITY] {nom:<25} = {val}")
            except Exception as exc:
                log.error(f"[INTEGRITY] Échec {nom} : {exc}")
                resultats[nom] = None

    return resultats


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires internes
# ─────────────────────────────────────────────────────────────────────────────

def _preparer_df_pour_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prépare un DataFrame pour l'insertion SQL :
    - Convertit les types pandas non-compatibles (Timestamp, date, bool pandas)
    - Remplace les NaN/NaT par None (NULL SQL)
    """
    df = df.copy()

    for col in df.columns:
        # Convertir les colonnes de type date Python
        if df[col].dtype == "object":
            pass  # laisser en str
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].where(df[col].notna(), other=None)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].astype(bool)

    # Remplacer pd.NA et np.nan par None
    df = df.where(pd.notnull(df), other=None)

    return df
