import pandas as pd
import logging
import re
from datetime import date

def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les clients Mexora.

    Règles appliquées :
      R1 - Déduplication sur email normalisé (conserver inscription la plus récente)
      R2 - Standardisation du sexe (cible : 'm' / 'f' / 'inconnu')
      R3 - Validation des dates de naissance (âge entre 16 et 100 ans)
      R4 - Validation du format email
      (Note: R5 - Segmentation client sera appliquée lors du build de la dimension)
    """
    initial = len(df)
    logging.info("--- DÉBUT DE LA TRANSFORMATION DES CLIENTS ---")

    # ==========================================
    # R4 — Validation email (A7ssen ndiroha hya lowla 9bel la déduplication)
    # ==========================================
    avant_email = df['email'].isna().sum()
    pattern_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    masque_invalide = ~df['email'].astype(str).str.match(pattern_email, na=False)
    df.loc[masque_invalide, 'email'] = None
    emails_invalides = df['email'].isna().sum() - avant_email
    logging.info(f"[TRANSFORM] R4 emails : {emails_invalides} emails au format invalide ont été vidés")

    # ==========================================
    # R1 — Déduplication
    # ==========================================
    avant_dedup = len(df)
    df['email_norm'] = df['email'].str.lower().str.strip()
    # On sécurise le tri en convertissant d'abord en datetime
    df['date_inscription'] = pd.to_datetime(df['date_inscription'], errors='coerce')
    df = df.sort_values('date_inscription').drop_duplicates(subset=['email_norm'], keep='last')
    
    # On supprime la colonne temporaire
    df = df.drop(columns=['email_norm'])
    
    lignes_supprimees = avant_dedup - len(df)
    logging.info(f"[TRANSFORM] R1 doublons : {lignes_supprimees} clients en doublon supprimés")

    # ==========================================
    # R2 — Standardisation du sexe
    # ==========================================
    mapping_sexe = {
        'm': 'm', 'f': 'f', '1': 'm', '0': 'f',
        'homme': 'm', 'femme': 'f', 'male': 'm', 'female': 'f',
        'h': 'm'
    }
    df['sexe'] = df['sexe'].astype(str).str.lower().str.strip().map(mapping_sexe).fillna('inconnu')
    nb_inconnus = (df['sexe'] == 'inconnu').sum()
    logging.info(f"[TRANSFORM] R2 sexe : {nb_inconnus} valeurs non reconnues transformées en 'inconnu'")

    # ==========================================
    # R3 — Validation des dates de naissance
    # ==========================================
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce')
    today = pd.Timestamp(date.today())
    df['age'] = (today - df['date_naissance']).dt.days // 365
    
    # On compte les anomalies avant de les écraser
    ages_anormaux = ((df['age'] < 16) | (df['age'] > 100)).sum()
    df.loc[(df['age'] < 16) | (df['age'] > 100), 'date_naissance'] = pd.NaT
    
    # Création des tranches d'âge (avec des bins corrigés pour la justesse mathématique)
    df['tranche_age'] = pd.cut(
        df['age'].fillna(0),
        bins=[0, 17, 24, 34, 44, 54, 64, 200],
        labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    )
    logging.info(f"[TRANSFORM] R3 dates : {ages_anormaux} dates de naissance improbables (âge < 16 ou > 100) invalidées")

    # Conclusion
    lignes_finales_supprimees = initial - len(df)
    logging.info(f"[TRANSFORM] FIN : Clients {initial} → {len(df)} lignes ({lignes_finales_supprimees} supprimées au total)")

    return df