from pipeline.bronze_ingestion import ingerer_bronze
from pipeline.silver_transform import executer_silver

FICHIER_SOURCE = 'offres_emploi_it_maroc.json'
DATA_LAKE_ROOT = 'data_lake'
REFERENTIEL = 'referentiel_competences_it.json'

if __name__ == "__main__":
    print("=== DÉMARRAGE DU PIPELINE MEXORA RH ===")
    
    # 1. Étape Bronze (Ingestion)
    print("\n1. Exécution de l'ingestion Bronze...")
    stats_bronze = ingerer_bronze(FICHIER_SOURCE, DATA_LAKE_ROOT)
    
    # 2. Étape Silver (Nettoyage & NLP)
    print("\n2. Exécution de la transformation Silver...")
    executer_silver(DATA_LAKE_ROOT, REFERENTIEL)
    
    print("\n=== PIPELINE BRONZE ET SILVER TERMINÉ AVEC SUCCÈS ! ===")