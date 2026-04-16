import pandas as pd
import numpy as np
import logging
import re

def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les clients Mexora.
    """
    # 1. Toujours travailler sur une copie pour éviter les avertissements mémoires
    df = df.copy()
    initial = len(df)

    # R1 — Déduplication
    # Il FAUT convertir la date en datetime avant de trier, sinon le tri est alphabétique
    df['date_inscription'] = pd.to_datetime(df['date_inscription'], errors='coerce', format='mixed', dayfirst=True)
    
    df['email_norm'] = df['email'].fillna('').str.lower().str.strip()
    df = df.sort_values('date_inscription', na_position='first').drop_duplicates(subset=['email_norm'], keep='last')
    logging.info(f"[TRANSFORM] R1 doublons clients : {initial - len(df)} lignes supprimées")

    # R2 — Standardisation du sexe
    mapping_sexe = {
        'm': 'm', 'f': 'f', '1': 'm', '0': 'f',
        'homme': 'm', 'femme': 'f', 'male': 'm', 'female': 'f', 'h': 'm'
    }
    # On gère les NaN avant d'appliquer les méthodes str
    df['sexe'] = df['sexe'].fillna('').str.lower().str.strip().map(mapping_sexe).fillna('inconnu')
    logging.info("[TRANSFORM] R2 sexe : standardisation appliquée")

    # R3 — Validation des dates de naissance
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce', format='mixed', dayfirst=True)
    today = pd.Timestamp.now().normalize()
    
    # Calcul de l'âge
    df['age'] = (today - df['date_naissance']).dt.days // 365
    
    # Invalidation des âges aberrants
    masque_invalide = (df['age'] < 16) | (df['age'] > 100)
    nb_aberrants = masque_invalide.sum()
    df.loc[masque_invalide, 'date_naissance'] = pd.NaT
    df.loc[masque_invalide, 'age'] = np.nan # On repasse l'âge à NaN pour ces lignes
    logging.info(f"[TRANSFORM] R3 dates naissance : {nb_aberrants} âges aberrants mis à NaT")

    # Discrétisation en gérant proprement les valeurs manquantes (catégorie 'Inconnu')
    df['tranche_age'] = pd.cut(
        df['age'],
        bins=[0, 18, 25, 35, 45, 55, 65, 200],
        labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+'],
        right=False # Ex: [18, 25[
    ).astype(str).replace('nan', 'Inconnu')

    # R4 — Validation email
    pattern_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    # Utilisation de fillna('') pour éviter les erreurs match() sur des valeurs nulles
    masque_invalid_email = ~df['email'].fillna('').str.match(pattern_email)
    nb_emails_invalides = masque_invalid_email.sum()
    df.loc[masque_invalid_email, 'email'] = None
    logging.info(f"[TRANSFORM] R4 emails : {nb_emails_invalides} formats invalides mis à NULL")

    logging.info(f"[TRANSFORM] Clients : {initial} → {len(df)} lignes ({initial - len(df)} supprimées au total)")

    # Nettoyage des colonnes temporaires
    df = df.drop(columns=['email_norm', 'age'])

    return df