from pipeline.bronze_ingestion import ingerer_bronze
from pipeline.silver_transform import executer_silver
from pipeline.gold_aggregation import construire_gold

FICHIER_SOURCE = 'offres_emploi_it_maroc.json'
DATA_LAKE_ROOT = 'data_lake'
REFERENTIEL = 'referentiel_competences_it.json'

if __name__ == "__main__":
    print("=== DÉMARRAGE DU PIPELINE MEXORA RH ===")
    
    # 1. Étape Bronze (Ingestion)
    print("\n--- 1. Exécution de l'ingestion Bronze ---")
    stats_bronze = ingerer_bronze(FICHIER_SOURCE, DATA_LAKE_ROOT)
    
    # 2. Étape Silver (Nettoyage & NLP)
    print("\n--- 2. Exécution de la transformation Silver ---")
    executer_silver(DATA_LAKE_ROOT, REFERENTIEL)
    
    # 3. Étape Gold (Agrégations DuckDB)
    print("\n--- 3. Exécution de l'agrégation Gold ---")
    construire_gold(DATA_LAKE_ROOT)
    
    print("\n=== PIPELINE COMPLET (BRONZE -> SILVER -> GOLD) TERMINÉ AVEC SUCCÈS ! ===")