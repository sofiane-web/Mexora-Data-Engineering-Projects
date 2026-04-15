import pandas as pd
import logging

def transform_commandes(df: pd.DataFrame, df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les commandes Mexora.

    Règles appliquées :
      R1 - Suppression des doublons sur id_commande (conserver la dernière occurrence)
      R2 - Standardisation des dates (format cible : YYYY-MM-DD)
      R3 - Harmonisation des noms de villes via le référentiel régions_maroc
      R4 - Standardisation des statuts de commande
      R5 - Suppression des lignes avec quantite <= 0
      R6 - Suppression des lignes avec prix_unitaire = 0 (commandes test)
      R7 - Remplacement des id_livreur manquants par la valeur -1 (livreur inconnu)
    """
    initial = len(df)
    logging.info("--- DÉBUT DE LA TRANSFORMATION DES COMMANDES ---")

    # ==========================================
    # R1 — Suppression des doublons
    # ==========================================
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    logging.info(f"[TRANSFORM] R1 doublons : {initial - len(df)} lignes supprimées")

    # ==========================================
    # R2 — Standardisation des dates
    # ==========================================
    df['date_commande'] = pd.to_datetime(
        df['date_commande'], format='mixed', dayfirst=True, errors='coerce'
    )
    dates_invalides = df['date_commande'].isna().sum()
    df = df.dropna(subset=['date_commande'])
    logging.info(f"[TRANSFORM] R2 dates : {dates_invalides} dates invalides supprimées")

    # ==========================================
    # R3 — Harmonisation des villes
    # ==========================================
    # Création du dictionnaire de mapping depuis le dataframe des régions (Extract)
    # On map le code (ex: 'tng') et le nom standard vers le nom standard
    mapping_villes = dict(zip(df_regions['code_ville'].str.lower(), df_regions['nom_ville_standard']))
    mapping_villes.update(dict(zip(df_regions['nom_ville_standard'].str.lower(), df_regions['nom_ville_standard'])))
    # Ajout de quelques alias manuels pour les erreurs de frappe (ex: tnja, casa)
    alias_villes = {'tnja': 'tanger', 'casa': 'casablanca'}
    mapping_villes.update(alias_villes)

    df['ville_livraison'] = df['ville_livraison'].astype(str).str.strip().str.lower()
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')
    logging.info("[TRANSFORM] R3 villes : Harmonisation des villes effectuée")

    # ==========================================
    # R4 — Standardisation des statuts
    # ==========================================
    mapping_statuts = {
        'livré': 'livré', 'livre': 'livré', 'LIVRE': 'livré', 'DONE': 'livré',
        'annulé': 'annulé', 'annule': 'annulé', 'KO': 'annulé',
        'en_cours': 'en_cours', 'OK': 'en_cours',
        'retourné': 'retourné', 'retourne': 'retourné'
    }
    df['statut'] = df['statut'].replace(mapping_statuts)
    invalides = ~df['statut'].isin(['livré', 'annulé', 'en_cours', 'retourné'])
    logging.warning(f"[TRANSFORM] R4 statuts : {invalides.sum()} valeurs non reconnues → 'inconnu'")
    df.loc[invalides, 'statut'] = 'inconnu'

    # ==========================================
    # R5 — Quantités invalides
    # ==========================================
    avant = len(df)
    # Convertir en float d'abord pour gérer les chaînes, puis en int si nécessaire
    df = df[df['quantite'].astype(float) > 0]
    logging.info(f"[TRANSFORM] R5 quantités : {avant - len(df)} lignes supprimées (quantité <= 0)")

    # ==========================================
    # R6 — Prix nuls (commandes test)
    # ==========================================
    avant = len(df)
    df = df[df['prix_unitaire'].astype(float) > 0]
    logging.info(f"[TRANSFORM] R6 prix : {avant - len(df)} commandes test supprimées")

    # ==========================================
    # R7 — Livreurs manquants
    # ==========================================
    nb_manquants = df['id_livreur'].isna().sum()
    df['id_livreur'] = df['id_livreur'].fillna('-1')
    logging.info(f"[TRANSFORM] R7 livreurs : {nb_manquants} valeurs manquantes remplacées par -1")

    # Conclusion
    lignes_supprimees = initial - len(df)
    logging.info(f"[TRANSFORM] FIN : Commandes {initial} → {len(df)} lignes ({lignes_supprimees} supprimées au total)")
    
    return df