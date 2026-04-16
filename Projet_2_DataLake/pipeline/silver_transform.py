import pandas as pd
import re
import json
from pathlib import Path

def charger_depuis_bronze(data_lake_root: str) -> pd.DataFrame:
    all_offres = []
    bronze_path = Path(data_lake_root) / 'bronze'

    # Kay9ra ga3 les fichiers JSON li f Bronze
    for json_file in bronze_path.rglob('offres_raw.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        all_offres.extend(data.get('offres', []))

    df = pd.DataFrame(all_offres)
    print(f"[SILVER] {len(df)} offres chargées depuis la zone Bronze.")
    return df

def nettoyer_titres_postes(df: pd.DataFrame) -> pd.DataFrame:
    mapping_profils = {
        r'data\s*eng(ineer|ineer\w*|\.)?|ingénieur\s+data|dev\s+data\s+eng': 'Data Engineer',
        r'etl\s*dev|pipeline\s*dev|ingénieur\s+etl': 'Data Engineer',
        r'data\s*anal(yst|yste|ytics)|analyste?\s+data|bi\s+anal': 'Data Analyst',
        r'business\s+intel(ligence)?|ingénieur\s+bi|développeur\s+bi': 'Data Analyst',
        r'data\s*sci(entist|ence)|machine\s*learn|ml\s*eng|ia\s*eng': 'Data Scientist',
        r'full\s*stack|fullstack': 'Développeur Full Stack',
        r'back[\s-]*end|backend': 'Développeur Backend',
        r'front[\s-]*end|frontend': 'Développeur Frontend',
        r'devops|sre|site\s*reliab': 'DevOps / SRE',
    }
    
    df['profil_normalise'] = 'Autre IT'
    df['profil_source'] = df['titre_poste'].astype(str).str.lower().str.strip()

    for pattern, profil in mapping_profils.items():
        masque = df['profil_source'].str.contains(pattern, regex=True, na=False)
        df.loc[masque, 'profil_normalise'] = profil

    return df

def normaliser_salaires(df: pd.DataFrame) -> pd.DataFrame:
    TAUX_EUR_MAD = 10.8

    def parser_salaire(valeur):
        if pd.isna(valeur) or str(valeur).lower() in ['null', 'confidentiel', 'selon profil', '']:
            return None, None, False

        s = str(valeur).lower().replace(' ', '').replace('\u202f', '')
        est_eur = 'eur' in s or '€' in s
        s = s.replace('eur', '').replace('€', '').replace('mad', '').replace('dh', '')
        s = re.sub(r'(\d+(?:\.\d+)?)k', lambda m: str(int(float(m.group(1)) * 1000)), s)
        nombres = re.findall(r'\d+(?:\.\d+)?', s)

        if not nombres: return None, None, False

        montants = [float(n) for n in nombres]
        if est_eur: montants = [m * TAUX_EUR_MAD for m in montants]

        sal_min = min(montants[:2]) if len(montants) >= 2 else montants[0]
        sal_max = max(montants[:2]) if len(montants) >= 2 else montants[0]

        if sal_min < 3000 or sal_max > 100000: return None, None, False
        return sal_min, sal_max, True

    resultats = df['salaire_brut'].apply(lambda x: pd.Series(parser_salaire(x), index=['salaire_min_mad', 'salaire_max_mad', 'salaire_connu']))
    df = pd.concat([df, resultats], axis=1)
    df['salaire_median_mad'] = (df['salaire_min_mad'] + df['salaire_max_mad']) / 2
    return df

def normaliser_champs(df: pd.DataFrame) -> pd.DataFrame:
    def parser_experience(valeur):
        if pd.isna(valeur): return None, None
        s = str(valeur).lower()
        if any(mot in s for mot in ['débutant', 'junior', 'stage', 'sans expérience']): return 0, 2
        if any(mot in s for mot in ['senior', 'confirmé', 'expert', 'lead']): return 5, None
        fourchette = re.search(r'(\d+)\s*[-àa]\s*(\d+)', s)
        if fourchette: return int(fourchette.group(1)), int(fourchette.group(2))
        min_seul = re.search(r'(\d+)\s*(?:ans?|years?)', s)
        if min_seul: return int(min_seul.group(1)), None
        return None, None

    resultats = df['experience_requise'].apply(lambda x: pd.Series(parser_experience(x), index=['experience_min_ans', 'experience_max_ans']))
    df = pd.concat([df, resultats], axis=1)
    
    df['ville_std'] = df['ville'].astype(str).str.title().str.strip() if 'ville' in df.columns else 'Inconnu'
    df['type_contrat_std'] = df['type_contrat'].astype(str).str.upper().str.strip() if 'type_contrat' in df.columns else 'Inconnu'
    df['region_admin'] = df['ville_std']
    df['annee'] = df['date_publication'].astype(str).str[:4]
    df['mois'] = df['date_publication'].astype(str).str[5:7]
    return df

def extraire_competences(df: pd.DataFrame, referentiel_path: str) -> pd.DataFrame:
    with open(referentiel_path, 'r', encoding='utf-8') as f:
        referentiel = json.load(f)

    dict_competences = {}
    for famille, competences in referentiel['familles'].items():
        for nom_normalise, aliases in competences.items():
            for alias in aliases:
                dict_competences[alias.lower()] = {'competence': nom_normalise, 'famille': famille}

    aliases_tries = sorted(dict_competences.keys(), key=len, reverse=True)
    resultats = []

    for _, offre in df.iterrows():
        texte_complet = ' '.join(filter(None, [str(offre.get('competences_brut', '')), str(offre.get('description', ''))])).lower()
        competences_trouvees = set()

        for alias in aliases_tries:
            pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, texte_complet):
                info = dict_competences[alias]
                cle = info['competence']
                if cle not in competences_trouvees:
                    competences_trouvees.add(cle)
                    resultats.append({
                        'id_offre': offre['id_offre'], 'profil': offre.get('profil_normalise'),
                        'ville': offre.get('ville_std'), 'competence': info['competence'],
                        'famille': info['famille'], 'date_pub': offre.get('date_publication'),
                        'annee': str(offre.get('date_publication', ''))[:4], 'mois': str(offre.get('date_publication', ''))[5:7],
                    })

        if not competences_trouvees:
            resultats.append({
                'id_offre': offre['id_offre'], 'profil': offre.get('profil_normalise'),
                'ville': offre.get('ville_std'), 'competence': 'non_détecté',
                'famille': 'inconnu', 'date_pub': offre.get('date_publication'),
                'annee': str(offre.get('date_publication', ''))[:4], 'mois': str(offre.get('date_publication', ''))[5:7],
            })

    df_competences = pd.DataFrame(resultats)
    print(f"[SILVER NLP] {len(df_competences)} lignes de compétences extraites.")
    return df_competences

def sauvegarder_silver(df_offres: pd.DataFrame, df_competences: pd.DataFrame, data_lake_root: str):
    silver_path = Path(data_lake_root) / 'silver'
    
    chemin_offres = silver_path / 'offres_clean' / 'offres_clean.parquet'
    chemin_offres.parent.mkdir(parents=True, exist_ok=True)
    df_offres.to_parquet(chemin_offres, index=False, compression='snappy')
    print(f"[SILVER] Fichier 'offres_clean.parquet' sauvegardé avec succès.")

    chemin_comp = silver_path / 'competences_extraites' / 'competences.parquet'
    chemin_comp.parent.mkdir(parents=True, exist_ok=True)
    df_competences.to_parquet(chemin_comp, index=False, compression='snappy')
    print(f"[SILVER] Fichier 'competences.parquet' sauvegardé avec succès.")

def executer_silver(data_lake_root: str, referentiel_path: str):
    df = charger_depuis_bronze(data_lake_root)
    df = nettoyer_titres_postes(df)
    df = normaliser_salaires(df)
    df = normaliser_champs(df)
    df_competences = extraire_competences(df, referentiel_path)
    sauvegarder_silver(df, df_competences, data_lake_root)