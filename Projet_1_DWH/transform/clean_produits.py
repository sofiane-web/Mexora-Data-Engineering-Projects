import pandas as pd
import logging

def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les règles de nettoyage sur les produits Mexora.
    """
    logging.info("[TRANSFORM] Début du nettoyage des produits...")
    
    # Toujours travailler sur une copie
    df = df.copy()
    initial = len(df)
    
    # R1 — Harmonisation de la casse des catégories et sous-catégories
    # On utilise .str.title() pour transformer "electronique" ou "ELECTRONIQUE" en "Electronique"
    df['categorie'] = df['categorie'].fillna('Inconnu').str.strip().str.title()
    df['sous_categorie'] = df['sous_categorie'].fillna('Inconnu').str.strip().str.title()
    logging.info("[TRANSFORM] R1 Produits : Casse des catégories harmonisée.")

    # R2 — Gestion des prix nuls ou manquants
    # On s'assure que la colonne est bien numérique, les erreurs deviennent NaN, puis on remplit par 0.0
    nb_prix_manquants = df['prix_catalogue'].isna().sum()
    df['prix_catalogue'] = pd.to_numeric(df['prix_catalogue'], errors='coerce').fillna(0.0)
    
    # Optionnel : Si on considère qu'un prix strictement égal à 0 doit aussi être tracé
    nb_prix_zeros = (df['prix_catalogue'] == 0.0).sum()
    logging.info(f"[TRANSFORM] R2 Produits : {nb_prix_manquants} manquants et {nb_prix_zeros} prix à zéro gérés.")

    # R3 — Formatage des dates de création
    df['date_creation'] = pd.to_datetime(df['date_creation'], errors='coerce')
    
    # R4 — Standardisation des booléens
    # On s'assure que la colonne 'actif' est un vrai booléen Python/Pandas
    df['actif'] = df['actif'].fillna(False).astype(bool)

    logging.info(f"[TRANSFORM] Produits : {initial} → {len(df)} lignes prêtes pour la dimension.")
    
    return df