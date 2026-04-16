"""
transform/clean_clients.py — Nettoyage et transformation des clients Mexora

Règles appliquées :
  R1 — Déduplication sur email normalisé (conserver l'inscription la plus récente)
  R2 — Standardisation du sexe (cible : 'm' / 'f' / 'inconnu')
  R3 — Validation des dates de naissance (âge entre 16 et 100 ans)
  R4 — Calcul de la tranche d'âge
  R5 — Validation du format email
  R6 — Harmonisation des villes (même référentiel que commandes)
  R7 — Normalisation du nom complet (nom + prénom)
"""

import re
import pandas as pd
from datetime import date

from config.settings import MAPPING_SEXE
from utils.logger import etl_logger as log

# Regex email valide (RFC 5322 simplifié)
_EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# Tranches d'âge Mexora
_AGE_BINS   = [0,  18,  25,  35,  45,  55,  65, 200]
_AGE_LABELS = ["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]


# ─────────────────────────────────────────────────────────────────────────────
# Fonctions atomiques
# ─────────────────────────────────────────────────────────────────────────────

def dedupliquer_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    R1 — Déduplication sur email normalisé.
    Problème documenté : même email, id_client différent (erreur de migration).
    Règle : conserver l'inscription la plus récente.
    """
    avant = len(df)

    df["email_norm"] = df["email"].str.lower().str.strip()
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")

    # Trier par date_inscription croissante, garder la dernière (keep='last')
    df = (
        df.sort_values("date_inscription", ascending=True, na_position="first")
        .drop_duplicates(subset=["email_norm"], keep="last")
        .reset_index(drop=True)
    )

    log.log_transform("R1 — Doublons email", avant, len(df), "clients",
                      f"{avant - len(df)} doublons supprimés")
    return df


def standardiser_sexe(df: pd.DataFrame) -> pd.DataFrame:
    """
    R2 — Standardisation du sexe.
    Encodages source : m/f | 1/0 | Homme/Femme | M/F | male/female | h/f
    Valeur cible : 'm' | 'f' | 'inconnu'
    """
    df = df.copy()
    sexe_brut = df["sexe"].str.strip()
    df["sexe"] = sexe_brut.map(MAPPING_SEXE).fillna("inconnu")

    nb_inconnus = (df["sexe"] == "inconnu").sum()
    log.log_replace("R2 — Standardisation sexe", "clients", len(df),
                    f"{nb_inconnus} valeurs → 'inconnu'")
    return df


def valider_dates_naissance(df: pd.DataFrame) -> pd.DataFrame:
    """
    R3 — Validation des dates de naissance.
    Règle métier : âge entre 16 et 100 ans à la date du jour.
    Les dates hors plage sont invalidées (→ NaT) mais la ligne est conservée.
    """
    df = df.copy()
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")

    today = pd.Timestamp(date.today())
    df["age"] = ((today - df["date_naissance"]).dt.days / 365.25).astype("float64")

    masque_invalide = (df["age"] < 16) | (df["age"] > 100) | df["age"].isna()
    nb_invalides = masque_invalide.sum()

    df.loc[masque_invalide, "date_naissance"] = pd.NaT
    df.loc[masque_invalide, "age"] = pd.NA

    log.log_replace("R3 — Ages invalides (<16 ou >100)", "clients", nb_invalides,
                    f"{nb_invalides} dates de naissance invalidées")
    return df


def calculer_tranche_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    R4 — Calcul de la tranche d'âge à partir de l'âge calculé.
    Age inconnu → tranche 'Inconnu'.
    """
    df = df.copy()
    df["tranche_age"] = pd.cut(
        df["age"].fillna(-1),
        bins=_AGE_BINS,
        labels=_AGE_LABELS,
        right=False,
    ).astype(str)

    # Les âges inconnus (NaN → -1) tombent hors bins → remplacer
    df["tranche_age"] = df["tranche_age"].replace("nan", "Inconnu")

    log.info(f"[TRANSFORM] R4 — Répartition tranches d'âge :\n"
             f"             {df['tranche_age'].value_counts().to_dict()}")
    return df


def valider_emails(df: pd.DataFrame) -> pd.DataFrame:
    """
    R5 — Validation du format email.
    Les emails mal formatés (sans @, sans domaine) sont mis à NULL.
    La ligne client est conservée.
    """
    df = df.copy()
    masque_invalide = ~df["email"].fillna("").apply(
        lambda e: bool(_EMAIL_REGEX.match(e))
    )
    nb_invalides = masque_invalide.sum()

    df.loc[masque_invalide, "email"] = pd.NA
    df.loc[masque_invalide, "email_norm"] = pd.NA

    log.log_replace("R5 — Emails invalides", "clients", nb_invalides,
                    f"{nb_invalides} emails invalidés (NULL)")
    return df


def harmoniser_villes_clients(df: pd.DataFrame, mapping_villes: dict) -> pd.DataFrame:
    """
    R6 — Harmonisation des villes clients via le référentiel.
    Même logique que pour les commandes.
    """
    df = df.copy()
    ville_brute = df["ville"].str.strip().str.lower().fillna("inconnu")
    ville_harmonisee = ville_brute.map(mapping_villes)
    nb_non_mappes = ville_harmonisee.isna().sum()

    df["ville"] = ville_harmonisee.fillna("Non renseignée")

    log.log_replace("R6 — Harmonisation villes clients", "clients", len(df),
                    f"{nb_non_mappes} villes non trouvées dans le référentiel")
    return df


def normaliser_nom_complet(df: pd.DataFrame) -> pd.DataFrame:
    """
    R7 — Concaténation prénom + nom en nom_complet normalisé (title case).
    """
    df = df.copy()
    df["nom_complet"] = (
        df["prenom"].fillna("").str.strip().str.title()
        + " "
        + df["nom"].fillna("").str.strip().str.title()
    ).str.strip()

    log.info("[TRANSFORM] R7 — Champ nom_complet créé")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Segmentation client (dépend des commandes)
# ─────────────────────────────────────────────────────────────────────────────

def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le segment client (Gold / Silver / Bronze) basé sur le CA cumulé
    des 12 derniers mois pour chaque client.

    Règles métier Mexora :
      Gold   : CA 12 mois >= 15 000 MAD
      Silver : CA 12 mois >=  5 000 MAD
      Bronze : CA 12 mois <   5 000 MAD

    Parameters
    ----------
    df_commandes : DataFrame des commandes transformées (avec montant_ttc)

    Returns
    -------
    pd.DataFrame avec colonnes [id_client, segment_client, ca_12m]
    """
    from config.settings import SEGMENT_GOLD_SEUIL, SEGMENT_SILVER_SEUIL

    # Référence = date max des commandes (données historiques 2022-2024)
    date_max    = df_commandes["date_commande"].max()
    date_limite = date_max - pd.Timedelta(days=365)

    df_recents = df_commandes[
        (df_commandes["date_commande"] >= date_limite)
        & (df_commandes["statut"] == "livré")
    ].copy()

    if df_recents.empty:
        log.warning("[TRANSFORM] Aucune commande récente pour calculer les segments")
        return pd.DataFrame(columns=["id_client", "segment_client", "ca_12m"])

    ca_par_client = (
        df_recents.groupby("id_client")["montant_ttc"]
        .sum()
        .reset_index()
        .rename(columns={"montant_ttc": "ca_12m"})
    )

    def segmenter(ca: float) -> str:
        if ca >= SEGMENT_GOLD_SEUIL:
            return "Gold"
        elif ca >= SEGMENT_SILVER_SEUIL:
            return "Silver"
        return "Bronze"

    ca_par_client["segment_client"] = ca_par_client["ca_12m"].apply(segmenter)
    ca_par_client["ca_12m"] = ca_par_client["ca_12m"].round(2)

    dist = ca_par_client["segment_client"].value_counts().to_dict()
    log.info(f"[TRANSFORM] Segmentation clients : {dist}")

    return ca_par_client[["id_client", "segment_client", "ca_12m"]]


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def transform_clients(
    df: pd.DataFrame,
    mapping_villes: dict,
) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les clients Mexora.

    Parameters
    ----------
    df             : DataFrame brut extrait depuis CSV
    mapping_villes : dict {variante → nom_standard}

    Returns
    -------
    pd.DataFrame propre avec âges et tranches d'âge calculés.
    """
    initial = len(df)
    log.section("TRANSFORM — Clients")
    log.info(f"[TRANSFORM] Début nettoyage clients : {initial} lignes en entrée")

    df = dedupliquer_clients(df)
    df = standardiser_sexe(df)
    df = valider_dates_naissance(df)
    df = calculer_tranche_age(df)
    df = valider_emails(df)
    df = harmoniser_villes_clients(df, mapping_villes)
    df = normaliser_nom_complet(df)

    final = len(df)
    log.info(
        f"[TRANSFORM] Clients terminé : {initial} → {final} lignes "
        f"({initial - final} supprimées, {(initial - final) / max(initial, 1) * 100:.1f}%)"
    )

    return df
