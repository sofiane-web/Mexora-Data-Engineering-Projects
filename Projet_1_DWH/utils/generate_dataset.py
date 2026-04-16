import pandas as pd
import numpy as np
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker('fr_FR')
np.random.seed(42)
random.seed(42)

# Création du dossier data s'il n'existe pas
os.makedirs('data', exist_ok=True)

# ==========================================
# 1. Génération de Fichier 4 : REGIONS MAROC
# ==========================================
regions_data = [
    {"code_ville": "TNG", "nom_ville_standard": "tanger", "province": "Tanger-Assilah", "region_admin": "Tanger-Tétouan-Al Hoceïma", "zone_geo": "Nord", "population": 1000000, "code_postal": "90000"},
    {"code_ville": "CAS", "nom_ville_standard": "casablanca", "province": "Casablanca", "region_admin": "Casablanca-Settat", "zone_geo": "Centre", "population": 3500000, "code_postal": "20000"},
    {"code_ville": "RBA", "nom_ville_standard": "rabat", "province": "Rabat", "region_admin": "Rabat-Salé-Kénitra", "zone_geo": "Centre", "population": 600000, "code_postal": "10000"},
    {"code_ville": "MRK", "nom_ville_standard": "marrakech", "province": "Marrakech", "region_admin": "Marrakech-Safi", "zone_geo": "Sud", "population": 1000000, "code_postal": "40000"}
]
df_regions = pd.DataFrame(regions_data)
df_regions.to_csv('data/regions_maroc.csv', index=False)

# ==========================================
# 2. Génération de Fichier 2 : PRODUITS JSON
# ==========================================
categories = ["Electronique", "Mode", "Alimentation"]
sous_categories = {"Electronique": ["Smartphones", "PC", "Accessoires"], "Mode": ["Hommes", "Femmes", "Enfants"], "Alimentation": ["Frais", "Epicerie", "Boissons"]}

produits = []
for i in range(1, 151): # 150 produits
    cat = random.choice(categories)
    
    # Injection: Casse incohérente pour les catégories
    cat_anomalie = random.choice([cat, cat.lower(), cat.upper()])
    
    # Injection: prix nul pour vieux produits
    prix = round(random.uniform(50, 15000), 2)
    if random.random() < 0.05: prix = None 
    
    # Injection: inactif mais sera lié à des commandes
    actif = True if random.random() > 0.1 else False
    
    produits.append({
        "id_produit": f"P{str(i).zfill(3)}",
        "nom": f"Produit {i} {cat}",
        "categorie": cat_anomalie,
        "sous_categorie": random.choice(sous_categories[cat]),
        "marque": fake.company(),
        "fournisseur": fake.company() + " MENA",
        "prix_catalogue": prix,
        "origine_pays": random.choice(["USA", "Maroc", "Chine", "France"]),
        "date_creation": fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d'),
        "actif": actif
    })

with open('data/produits_mexora.json', 'w', encoding='utf-8') as f:
    json.dump({"produits": produits}, f, indent=4, ensure_ascii=False)

# ==========================================
# 3. Génération de Fichier 3 : CLIENTS CSV
# ==========================================
clients = []
villes_anomalies = ["tanger", "TNG", "TANGER", "Tnja", "Casa", "Casablanca", "rabat", "RBT", "Marrakesh"]

for i in range(1, 1001): # 1000 clients
    # Injection: Sexe codé différemment
    sexe = random.choice(['m', 'f', '1', '0', 'Homme', 'Femme', 'Male', 'Female'])
    
    # Injection: Dates incohérentes (trop vieux ou pas nés)
    if random.random() < 0.02:
        date_naiss = fake.date_of_birth(minimum_age=130, maximum_age=150) # > 120 ans
    elif random.random() < 0.02:
        date_naiss = fake.date_time_between(start_date='+1y', end_date='+5y') # Négatif
    else:
        date_naiss = fake.date_of_birth(minimum_age=16, maximum_age=80)
        
    # Injection: Emails mal formatés
    email = fake.email()
    if random.random() < 0.05:
        email = email.replace('@', '') if random.random() < 0.5 else email.split('@')[0]
        
    clients.append({
        "id_client": f"C{str(i).zfill(4)}",
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "email": email,
        "date_naissance": date_naiss.strftime('%Y-%m-%d'),
        "sexe": sexe,
        "ville": random.choice(villes_anomalies),
        "telephone": fake.phone_number(),
        "date_inscription": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
        "canal_acquisition": random.choice(["SEO", "Social Media", "Ads", "Direct"])
    })

df_clients = pd.DataFrame(clients)

# Injection: Doublons erreur de migration (même email, id différent)
doublons = df_clients.sample(30).copy()
doublons['id_client'] = [f"C{str(x).zfill(4)}" for x in range(2000, 2030)]
df_clients = pd.concat([df_clients, doublons], ignore_index=True)

df_clients.to_csv('data/clients_mexora.csv', index=False)

# ==========================================
# 4. Génération de Fichier 1 : COMMANDES CSV (5000 lignes)
# ==========================================
commandes = []
statuts_anomalies = ["OK", "KO", "DONE", "livré", "annulé", "en_cours", "retourné"]
formats_dates = ['%d/%m/%Y', '%Y-%m-%d', '%b %d %Y'] # ex: Nov 15 2024

for i in range(1, 5001):
    date_cmd = fake.date_between(start_date='-1y', end_date='today')
    # Injection: Formats mixtes
    fmt = random.choice(formats_dates)
    date_str = date_cmd.strftime(fmt)
    
    # Injection: Quantité négative
    quantite = random.randint(1, 5)
    if random.random() < 0.02: quantite = random.randint(-5, -1)
        
    # Injection: Prix 0 (commandes test)
    prix_u = round(random.uniform(10, 5000), 2)
    if random.random() < 0.03: prix_u = 0.0
        
    # Injection: Id livreur manquant (7%)
    id_livreur = f"L{random.randint(1, 50)}"
    if random.random() < 0.07: id_livreur = np.nan
        
    commandes.append({
        "id_commande": f"CMD{str(i).zfill(6)}",
        "id_client": random.choice(df_clients['id_client'].dropna().tolist()),
        "id_produit": random.choice([p['id_produit'] for p in produits]),
        "date_commande": date_str,
        "quantite": quantite,
        "prix_unitaire": prix_u,
        "statut": random.choice(statuts_anomalies),
        "ville_livraison": random.choice(villes_anomalies),
        "mode_paiement": random.choice(["Carte", "Cash", "Virement"]),
        "id_livreur": id_livreur,
        "date_livraison": (date_cmd + timedelta(days=random.randint(1, 5))).strftime('%Y-%m-%d')
    })

df_commandes = pd.DataFrame(commandes)

# Injection: Doublons sur id_commande (~3%)
doublons_cmd = df_commandes.sample(int(5000 * 0.03)).copy()
df_commandes = pd.concat([df_commandes, doublons_cmd], ignore_index=True)
# Mélanger le dataframe
df_commandes = df_commandes.sample(frac=1).reset_index(drop=True)

df_commandes.to_csv('data/commandes_mexora.csv', index=False)

print("Dataset généré avec succès dans le dossier 'data/' !")