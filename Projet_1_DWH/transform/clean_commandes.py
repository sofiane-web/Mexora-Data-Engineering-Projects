import pandas as pd
import logging
from pathlib import Path

def charger_referentiel_villes(filepath: str) -> dict:
    """Helper : Charge le fichier des régions et crée un dictionnaire de mapping."""
    try:
        df_regions = pd.read_csv(filepath)
        # Crée un dictionnaire { 'tanger': 'tanger', 'casablanca': 'casablanca', ... }
        # à adapter selon la structure exacte de ton fichier regions_maroc.csv
        return dict(zip(df_regions['code_ville'].str.lower(), df_regions['nom_ville_standard']))
    except Exception as e:
        logging.error(f"[TRANSFORM] Erreur de chargement du référentiel des villes : {e}")
        return {}

def transform_commandes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les commandes Mexora.
    """
    # Créer une copie indépendante pour éviter les SettingWithCopyWarning de Pandas
    df = df.copy()
    initial = len(df)

    # R1 — Suppression des doublons
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    logging.info(f"[TRANSFORM] R1 doublons : {initial - len(df)} lignes supprimées")

    # R2 — Standardisation des dates (Format cible YYYY-MM-DD en sortie)
    df['date_commande'] = pd.to_datetime(
        df['date_commande'], format='mixed', dayfirst=True, errors='coerce'
    )
    dates_invalides = df['date_commande'].isna().sum()
    df = df.dropna(subset=['date_commande'])
    # Optionnel : si tu veux forcer le type string au format YYYY-MM-DD pour la BDD
    # df['date_commande'] = df['date_commande'].dt.strftime('%Y-%m-%d')
    logging.info(f"[TRANSFORM] R2 dates : {dates_invalides} dates invalides supprimées")

    # R3 — Harmonisation des villes
    mapping_villes = charger_referentiel_villes('data/regions_maroc.csv')
    # On gère le cas où la ville est NaN avant d'appliquer des méthodes de string
    df['ville_livraison'] = df['ville_livraison'].fillna('').str.strip().str.lower()
    # On remplace par la ville standard, ou on garde la valeur d'origine si non trouvée, puis on gère les vides
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna(df['ville_livraison'])
    df.loc[df['ville_livraison'] == '', 'ville_livraison'] = 'non renseignée'

    # R4 — Standardisation des statuts
    mapping_statuts = {
        'livré': 'livré', 'livre': 'livré', 'livre': 'livré', 'done': 'livré',
        'annulé': 'annulé', 'annule': 'annulé', 'ko': 'annulé',
        'en_cours': 'en_cours', 'ok': 'en_cours',
        'retourné': 'retourné', 'retourne': 'retourné'
    }
    # On passe tout en minuscules pour attraper 'DONE', 'Done', 'done', etc.
    df['statut'] = df['statut'].fillna('').str.lower().str.strip().replace(mapping_statuts)
    
    invalides = ~df['statut'].isin(['livré', 'annulé', 'en_cours', 'retourné'])
    logging.warning(f"[TRANSFORM] R4 statuts : {invalides.sum()} valeurs non reconnues → remplacées par 'inconnu'")
    df.loc[invalides, 'statut'] = 'inconnu'

    # R5 — Quantités invalides (Sécurisé avec to_numeric)
    avant = len(df)
    # to_numeric transforme le texte ou les erreurs en NaN (grâce à errors='coerce')
    df['quantite'] = pd.to_numeric(df['quantite'], errors='coerce')
    df = df[df['quantite'] > 0] # Exclut automatiquement les NaN et les valeurs <= 0
    logging.info(f"[TRANSFORM] R5 quantités : {avant - len(df)} lignes supprimées (quantité <= 0 ou texte)")

    # R6 — Prix nuls (commandes test) (Sécurisé avec to_numeric)
    avant = len(df)
    df['prix_unitaire'] = pd.to_numeric(df['prix_unitaire'], errors='coerce')
    df = df[df['prix_unitaire'] > 0]
    logging.info(f"[TRANSFORM] R6 prix : {avant - len(df)} commandes test supprimées (prix <= 0 ou texte)")

    # R7 — Livreurs manquants
    nb_manquants = df['id_livreur'].isna().sum()
    # On s'assure que tout est en string avant de fillna
    df['id_livreur'] = df['id_livreur'].astype(str).replace('nan', '-1').fillna('-1')
    logging.info(f"[TRANSFORM] R7 livreurs : {nb_manquants} valeurs manquantes remplacées par -1")

    logging.info(f"[TRANSFORM] Commandes : {initial} → {len(df)} lignes ({initial - len(df)} supprimées au total)")
    
    return df