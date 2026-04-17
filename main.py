"""
main.py — Orchestration du pipeline ETL Mexora Analytics
Point d'entrée unique du pipeline. Lance les phases Extract → Transform → Load.

Usage :
    python main.py                    # pipeline complet
    python main.py --dry-run          # sans chargement PostgreSQL
    python main.py --phase extract    # extraction seule
    python main.py --phase transform  # extraction + transformation
"""

import sys
import argparse
import traceback
from datetime import datetime
from pathlib import Path

# Ajout du répertoire racine au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import BASE_DIR
from utils.logger import etl_logger as log

from extract.extractor import (
    extract_commandes,
    extract_produits,
    extract_clients,
    extract_regions,
    charger_referentiel_villes,
)
from transform.clean_commandes import transform_commandes
from transform.clean_clients   import transform_clients
from transform.clean_produits  import transform_produits
from transform.build_dimensions import (
    build_dim_temps,
    build_dim_produit,
    build_dim_client,
    build_dim_region,
    build_dim_livreur,
    build_fait_ventes,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline complet
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(dry_run: bool = False, phase: str = "all") -> dict:
    """
    Exécute le pipeline ETL complet.

    Parameters
    ----------
    dry_run : bool — si True, skip la phase Load (PostgreSQL)
    phase   : str  — 'extract' | 'transform' | 'all'

    Returns
    -------
    dict — métriques d'exécution du pipeline
    """
    start = datetime.now()
    metrics = {"statut": "en_cours", "start": start.isoformat()}

    log.section("DÉMARRAGE PIPELINE ETL MEXORA ANALYTICS")
    log.info(f"Mode : {'DRY-RUN (sans PostgreSQL)' if dry_run else 'COMPLET'}")
    log.info(f"Phase cible : {phase}")

    try:
        # ── PHASE EXTRACT ────────────────────────────────────────────────────
        log.section("PHASE 1 — EXTRACT")

        df_commandes_raw = extract_commandes()
        df_produits_raw  = extract_produits()
        df_clients_raw   = extract_clients()
        df_regions       = extract_regions()

        metrics["extract"] = {
            "commandes": len(df_commandes_raw),
            "produits":  len(df_produits_raw),
            "clients":   len(df_clients_raw),
            "regions":   len(df_regions),
        }
        log.info(f"[EXTRACT] Résumé : {metrics['extract']}")

        if phase == "extract":
            log.info("Phase 'extract' uniquement — arrêt.")
            metrics["statut"] = "partiel"
            return metrics

        # ── PHASE TRANSFORM ──────────────────────────────────────────────────
        log.section("PHASE 2 — TRANSFORM")

        # Référentiel villes (nécessaire pour commandes ET clients)
        mapping_villes = charger_referentiel_villes(df_regions)
        log.info(f"[TRANSFORM] Référentiel villes : {len(mapping_villes)} entrées")

        # Nettoyage des entités sources
        df_commandes = transform_commandes(df_commandes_raw, mapping_villes)
        df_clients   = transform_clients(df_clients_raw, mapping_villes)
        df_produits  = transform_produits(df_produits_raw)

        # Construction des dimensions
        dim_temps   = build_dim_temps()
        dim_produit = build_dim_produit(df_produits)
        dim_client  = build_dim_client(df_clients, df_commandes)
        dim_region  = build_dim_region(df_regions)
        dim_livreur = build_dim_livreur(df_commandes)

        # Construction de la table de faits
        fait_ventes = build_fait_ventes(
            df_commandes, dim_temps, dim_client,
            dim_produit, dim_region, dim_livreur
        )

        metrics["transform"] = {
            "commandes_clean":   len(df_commandes),
            "clients_clean":     len(df_clients),
            "produits_clean":    len(df_produits),
            "dim_temps":         len(dim_temps),
            "dim_produit":       len(dim_produit),
            "dim_client":        len(dim_client),
            "dim_region":        len(dim_region),
            "dim_livreur":       len(dim_livreur),
            "fait_ventes":       len(fait_ventes),
            "ca_ttc_total":      round(fait_ventes["montant_ttc"].sum(), 2),
        }
        log.info(f"[TRANSFORM] Résumé : {metrics['transform']}")

        if phase == "transform":
            log.info("Phase 'transform' uniquement — arrêt avant Load.")
            metrics["statut"] = "partiel"
            return metrics

        # ── PHASE LOAD ───────────────────────────────────────────────────────
        log.section("PHASE 3 — LOAD")

        if dry_run:
            log.info("[LOAD] DRY-RUN activé — chargement PostgreSQL ignoré")
            _sauvegarder_parquet(
                dim_temps, dim_produit, dim_client,
                dim_region, dim_livreur, fait_ventes
            )
        else:
            from load.loader import (
                creer_engine, creer_schemas,
                charger_dimension, charger_faits,
                rafraichir_vues_materialisees, verifier_integrite,
            )

            engine = creer_engine()
            creer_schemas(engine)

            # Chargement dans l'ordre FK : dimensions d'abord, faits ensuite
            charger_dimension(dim_temps,   "dim_temps",   engine)
            charger_dimension(dim_produit, "dim_produit", engine)
            charger_dimension(dim_client,  "dim_client",  engine)
            charger_dimension(dim_region,  "dim_region",  engine)
            charger_dimension(dim_livreur, "dim_livreur", engine)
            charger_faits(fait_ventes, engine)

            # Rafraîchir les vues matérialisées après chargement
            log.info("--- RAFRAÎCHISSEMENT DES VUES MATÉRIALISÉES ---")
            rafraichir_vues_materialisees(engine)

            metrics["integrity"] = verifier_integrite(engine)

        # ── RAPPORT ──────────────────────────────────────────────────────────
        rapport_path = BASE_DIR / "rapport_transformations.md"
        log.generer_rapport(str(rapport_path))

        duree = (datetime.now() - start).total_seconds()
        metrics["statut"]     = "succès"
        metrics["duree_sec"]  = round(duree, 2)

        log.section("PIPELINE TERMINÉ AVEC SUCCÈS")
        log.info(f"Durée totale : {duree:.1f} secondes")
        log.info(f"Rapport      : {rapport_path}")

    except FileNotFoundError as exc:
        log.error(f"Fichier source manquant : {exc}")
        log.error("Exécutez d'abord : python generate_dataset.py")
        metrics["statut"] = "erreur"
        metrics["erreur"] = str(exc)
        raise

    except Exception as exc:
        log.error(f"ERREUR PIPELINE : {exc}")
        log.error(traceback.format_exc())
        metrics["statut"] = "erreur"
        metrics["erreur"] = str(exc)
        raise

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Mode dry-run : sauvegarde Parquet (sans PostgreSQL)
# ─────────────────────────────────────────────────────────────────────────────

def _sauvegarder_parquet(*dfs_and_names) -> None:
    """
    En mode dry-run, sauvegarde les DataFrames en Parquet pour vérification.
    """
    output_dir = BASE_DIR / "output_parquet"
    output_dir.mkdir(exist_ok=True)

    noms = [
        "dim_temps", "dim_produit", "dim_client",
        "dim_region", "dim_livreur", "fait_ventes"
    ]

    for nom, df in zip(noms, dfs_and_names):
        path = output_dir / f"{nom}.parquet"
        try:
            df.to_parquet(path, index=False)
            log.info(f"[DRY-RUN] {nom} → {path} ({len(df)} lignes)")
        except Exception as exc:
            log.warning(f"[DRY-RUN] Parquet {nom} impossible ({exc}) — CSV fallback")
            df.to_csv(output_dir / f"{nom}.csv", index=False, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Mexora Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python main.py                          # pipeline complet avec PostgreSQL
  python main.py --dry-run                # pipeline sans PostgreSQL (→ Parquet)
  python main.py --phase extract          # extraction seule
  python main.py --phase transform        # extraction + transformation
        """,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Exécuter sans charger dans PostgreSQL (sortie Parquet)"
    )
    parser.add_argument(
        "--phase", choices=["extract", "transform", "all"], default="all",
        help="Phase à exécuter (défaut : all)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    metrics = run_pipeline(dry_run=args.dry_run, phase=args.phase)
    print("\n=== MÉTRIQUES FINALES ===")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    sys.exit(0 if metrics.get("statut") in ("succès", "partiel") else 1)
