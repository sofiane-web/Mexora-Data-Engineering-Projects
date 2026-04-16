import json
import random
from datetime import datetime, timedelta

# Listes des fausses données pour simuler les problèmes (villes mal écrites, salaires bizarres...)
sources = ["rekrute", "linkedin", "marocannonce"]
titres = ["Développeur Full Stack React/Node.js", "Dev Data", "Ingénieur Big Data", "Data Eng.", "Développeur BI", "Data Engineer Junior", "Data Scientist", "Analyste Data", "DevOps Engineer", "Chef de Projet IT"]
entreprises = ["TechMaroc SARL", "Mexora", "DataCorp", "WebAgency", "Bank IT", "Telecom Plus", "StartupX", "Consulting Group"]
villes = ["casa", "CASABLANCA", "Casablanca", "Tanger", "Rabat", "Marrakech", "Fès", "tanger", "RABAT"]
contrats = ["CDI", "cdi", "Contrat à durée indéterminée", "Permanent", "Freelance", "Stage"]
experiences = ["3-5 ans", "3 à 5 ans", "min 3 ans", "Débutant accepté", "Senior", "1-2 ans", None]
salaires = ["15000-20000 MAD", "15K-20K", "Selon profil", None, "Confidentiel", "2000-3000 EUR", "10000 MAD", "12K-18K"]
competences_list = ["React, Node.js", "Python, SQL", "Spark, Hadoop", "AWS, Docker", "Power BI, SQL, Excel", "Java, Spring", "Python, Machine Learning", "GCP, Kubernetes"]
descriptions = [
    "Nous recherchons un talent avec de l'expérience en {}.",
    "Rejoignez notre équipe dynamique. Compétences requises : {}.",
    "Poste basé à {ville}. Vous devez maîtriser {}.",
    "Opportunité unique pour un profil maîtrisant {}."
]

offres = []
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 11, 30)

print("⏳ Génération de 5000 offres d'emploi en cours...")

for i in range(5000):
    source = random.choice(sources)
    id_offre = f"{source[:2].upper()}-2024-{random.randint(10000, 99999)}"
    ville = random.choice(villes)
    comps = random.choice(competences_list)
    
    # Générer une date aléatoire
    random_days = random.randint(0, (end_date - start_date).days)
    date_pub = start_date + timedelta(days=random_days)
    date_exp = date_pub + timedelta(days=30)

    offre = {
        "id_offre": id_offre,
        "source": source,
        "titre_poste": random.choice(titres),
        "description": random.choice(descriptions).format(comps, ville=ville),
        "competences_brut": comps + (", Agile" if random.random() > 0.5 else ""),
        "entreprise": random.choice(entreprises),
        "ville": ville,
        "type_contrat": random.choice(contrats),
        "experience_requise": random.choice(experiences),
        "salaire_brut": random.choice(salaires),
        "niveau_etudes": random.choice(["Bac+3", "Bac+5", "Ingénieur"]),
        "secteur": "Informatique / Télécom",
        "date_publication": date_pub.strftime("%Y-%m-%d"),
        "date_expiration": date_exp.strftime("%Y-%m-%d"),
        "nb_postes": random.randint(1, 5),
        "teletravail": random.choice(["Hybride", "Remote", "Non"]),
        "langue_requise": ["Français", "Anglais"] if random.random() > 0.3 else ["Français"]
    }
    offres.append(offre)

# Enregistrement direct dans le fichier JSON
with open("offres_emploi_it_maroc.json", "w", encoding="utf-8") as f:
    json.dump({"offres": offres}, f, ensure_ascii=False, indent=2)

print(f"✅ Fichier 'offres_emploi_it_maroc.json' généré avec succès ! ({len(offres)} offres créées).")