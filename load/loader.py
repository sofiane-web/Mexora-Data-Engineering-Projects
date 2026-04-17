"""
load/loader.py — Phase LOAD du pipeline ETL Mexora
Chargement des dimensions et de la table de faits dans PostgreSQL.

STRATÉGIES CORRIGÉES :
  - Dimensions : TRUNCATE RESTART IDENTITY CASCADE + INSERT
                 (préserve la structure, les FK et les vues — pas de DROP)
  - Fait_ventes : TRUNCATE + INSERT (chargement complet idempotent)
  - Vues matérialisées : REFRESH automatique après chargement

POURQUOI PAS df.to_sql(if_exists='replace') ?
  pandas génère un DROP TABLE nu → PostgreSQL refuse car des objets en
  dépendent (clés étrangères de fait_ventes, vues matérialisées).
  La solution correcte est TRUNCATE ... RESTART IDENTITY CASCADE
  qui vide les données sans toucher à la structure ni aux objets dépendants.
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import text, inspect as sa_inspect

from config.settings import DATABASE_URL, SCHEMA_DWH, SCHEMA_REPORTING, CHUNK_SIZE
from utils.logger import etl_logger as log

# Vues matérialisées à rafraîchir après chaque chargement
VUES_MATERIALISEES = [
    "mv_ca_mensuel",
    "mv_top_produits",
    "mv_performance_livreurs",
]


# ─────────────────────────────────────────────────────────────────────────────
# Connexion
# ─────────────────────────────────────────────────────────────────────────────

def creer_engine() -> sqlalchemy.Engine:
    """Crée et valide le moteur SQLAlchemy vers PostgreSQL."""
    engine = sqlalchemy.create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        connect_args={"connect_timeout": 10},
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info(f"[LOAD] Connexion PostgreSQL établie → {DATABASE_URL.split('@')[-1]}")
    return engine


def creer_schemas(engine: sqlalchemy.Engine) -> None:
    """Crée les schémas PostgreSQL s'ils n'existent pas."""
    schemas = ["staging_mexora", "dwh_mexora", "reporting_mexora"]
    with engine.connect() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()
    log.info(f"[LOAD] Schémas vérifiés/créés : {schemas}")


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires internes
# ─────────────────────────────────────────────────────────────────────────────

def _table_existe(engine: sqlalchemy.Engine, table: str, schema: str) -> bool:
    inspector = sa_inspect(engine)
    return inspector.has_table(table, schema=schema)


def _truncate_table(conn, table: str, schema: str) -> None:
    """
    Vide une table proprement avec TRUNCATE RESTART IDENTITY CASCADE.

    - RESTART IDENTITY : remet les séquences SERIAL à 1
    - CASCADE          : vide aussi les tables enfants liées par FK
                         (les contraintes FK elles-mêmes restent intactes)

    NE PAS utiliser DROP TABLE : PostgreSQL refuse quand des vues
    matérialisées ou des FK dépendent de la table.
    """
    conn.execute(text(
        f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"
    ))
    log.debug(f"[LOAD] TRUNCATE {schema}.{table} RESTART IDENTITY CASCADE — OK")


def _preparer_df_pour_sql(df: pd.DataFrame) -> pd.DataFrame:
    """Prépare un DataFrame : remplace NaN/NaT par None (NULL SQL)."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].where(df[col].notna(), other=None)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].astype(bool)
    df = df.where(pd.notnull(df), other=None)
    return df


def _insert_par_chunks(conn, df: pd.DataFrame, table: str, schema: str) -> None:
    """Insère un DataFrame par chunks avec if_exists='append'."""
    df.to_sql(
        name=table,
        con=conn,
        schema=schema,
        if_exists="append",   # ← JAMAIS 'replace' — utiliser TRUNCATE avant
        index=False,
        method="multi",
        chunksize=CHUNK_SIZE,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Chargement d'une dimension — TRUNCATE + INSERT
# ─────────────────────────────────────────────────────────────────────────────

def charger_dimension(
    df: pd.DataFrame,
    table_name: str,
    engine: sqlalchemy.Engine,
    schema: str = SCHEMA_DWH,
) -> None:
    """
    Charge une table de dimension dans PostgreSQL.

    Stratégie :
      ✅ Table EXISTE   → TRUNCATE RESTART IDENTITY CASCADE + INSERT
      ⚠️  Table ABSENTE → Création automatique via pandas (sans contraintes)
                          → Exécuter create_dwh.sql en amont pour les contraintes

    On N'utilise JAMAIS if_exists='replace' car pandas émet un DROP TABLE
    brut que PostgreSQL refuse quand des FK ou des vues matérialisées
    référencent la table.
    """
    if df.empty:
        log.warning(f"[LOAD] {table_name} — DataFrame vide, chargement ignoré")
        return

    df = _preparer_df_pour_sql(df)
    table_fqn = f"{schema}.{table_name}"
    existe = _table_existe(engine, table_name, schema)

    try:
        with engine.begin() as conn:
            if existe:
                _truncate_table(conn, table_name, schema)
                strategie = "truncate+insert"
            else:
                log.warning(
                    f"[LOAD] {table_fqn} absente — création auto (sans contraintes). "
                    "Exécutez create_dwh.sql pour les FK et index."
                )
                strategie = "create+insert" 

            _insert_par_chunks(conn, df, table_name, schema)

        log.log_load(table_fqn, len(df), strategie)

    except Exception as exc:
        log.error(f"[LOAD] Échec chargement {table_fqn} : {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Chargement de la table de faits — TRUNCATE + INSERT
# ─────────────────────────────────────────────────────────────────────────────

def charger_faits(
    df: pd.DataFrame,
    engine: sqlalchemy.Engine,
    table_name: str = "fait_ventes",
    schema: str = SCHEMA_DWH,
) -> None:
    """
    Charge la table de faits dans PostgreSQL.

    Stratégie : TRUNCATE + INSERT (reconstruction complète à chaque run).
    La table de faits est toujours rechargée entièrement depuis les sources.

    Note : TRUNCATE CASCADE vide aussi les éventuelles tables enfants
    mais ne touche pas aux vues matérialisées (elles seront rafraîchies
    séparément via rafraichir_vues_materialisees()).
    """
    if df.empty:
        log.warning("[LOAD] fait_ventes — DataFrame vide, chargement ignoré")
        return

    df = _preparer_df_pour_sql(df)
    table_fqn = f"{schema}.{table_name}"
    existe = _table_existe(engine, table_name, schema)

    try:
        with engine.begin() as conn:
            if existe:
                _truncate_table(conn, table_name, schema)
                strategie = "truncate+insert"
            else:
                log.warning(
                    f"[LOAD] {table_fqn} absente — création auto. "
                    "Exécutez create_dwh.sql pour les FK et index."
                )
                strategie = "create+insert"

            _insert_par_chunks(conn, df, table_name, schema)

        log.log_load(table_fqn, len(df), strategie)

    except Exception as exc:
        log.error(f"[LOAD] Échec chargement {table_fqn} : {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Rafraîchissement des vues matérialisées
# ─────────────────────────────────────────────────────────────────────────────

def rafraichir_vues_materialisees(
    engine: sqlalchemy.Engine,
    schema: str = SCHEMA_REPORTING,
    vues: list | None = None,
) -> None:
    """
    Rafraîchit les vues matérialisées du schéma reporting après chargement.

    Tente d'abord CONCURRENTLY (ne bloque pas les lectures), sinon fallback
    sur REFRESH simple (bloque brièvement mais toujours fonctionnel).
    """
    vues = vues or VUES_MATERIALISEES

    with engine.connect() as conn:
        for vue in vues:
            vue_fqn = f"{schema}.{vue}"
            existe = conn.execute(text(
                "SELECT 1 FROM pg_matviews "
                "WHERE schemaname = :s AND matviewname = :v"
            ), {"s": schema, "v": vue}).scalar()

            if not existe:
                log.warning(f"[LOAD] Vue matérialisée {vue_fqn} introuvable — ignorée")
                continue

            try:
                conn.execute(text(
                    f"REFRESH MATERIALIZED VIEW CONCURRENTLY {vue_fqn}"
                ))
                conn.commit()
                log.info(f"[LOAD] Vue {vue_fqn} rafraîchie (CONCURRENTLY)")
            except Exception:
                try:
                    conn.rollback()
                    conn.execute(text(
                        f"REFRESH MATERIALIZED VIEW {vue_fqn}"
                    ))
                    conn.commit()
                    log.info(f"[LOAD] Vue {vue_fqn} rafraîchie")
                except Exception as exc2:
                    conn.rollback()
                    log.warning(f"[LOAD] Rafraîchissement {vue_fqn} échoué : {exc2}")


# ─────────────────────────────────────────────────────────────────────────────
# Vérification d'intégrité post-chargement
# ─────────────────────────────────────────────────────────────────────────────

def verifier_integrite(engine: sqlalchemy.Engine) -> dict:
    """Vérifie l'intégrité référentielle après chargement."""
    requetes = {
        "orphelins_date": (
            f"SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f "
            f"LEFT JOIN {SCHEMA_DWH}.dim_temps t ON f.id_date = t.id_date "
            "WHERE t.id_date IS NULL"
        ),
        "orphelins_produit": (
            f"SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f "
            f"LEFT JOIN {SCHEMA_DWH}.dim_produit p ON f.id_produit = p.id_produit_sk "
            "WHERE p.id_produit_sk IS NULL"
        ),
        "orphelins_client": (
            f"SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes f "
            f"LEFT JOIN {SCHEMA_DWH}.dim_client c ON f.id_client = c.id_client_sk "
            "WHERE c.id_client_sk IS NULL"
        ),
        "nb_faits":     f"SELECT COUNT(*) FROM {SCHEMA_DWH}.fait_ventes",
        "nb_clients":   f"SELECT COUNT(*) FROM {SCHEMA_DWH}.dim_client",
        "nb_produits":  f"SELECT COUNT(*) FROM {SCHEMA_DWH}.dim_produit",
        "ca_total_ttc": (
            f"SELECT ROUND(SUM(montant_ttc)::numeric, 2) "
            f"FROM {SCHEMA_DWH}.fait_ventes WHERE statut_commande = 'livré'"
        ),
    }

    resultats = {}
    with engine.connect() as conn:
        for nom, requete in requetes.items():
            try:
                val = conn.execute(text(requete)).scalar()
                resultats[nom] = val
                niveau = "warning" if (nom.startswith("orphelins") and val and val > 0) else "info"
                getattr(log, niveau)(f"[INTEGRITY] {nom:<25} = {val}")
            except Exception as exc:
                log.error(f"[INTEGRITY] Échec {nom} : {exc}")
                resultats[nom] = None

    return resultats
