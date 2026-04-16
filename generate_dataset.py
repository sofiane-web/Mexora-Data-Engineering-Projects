"""
generate_dataset.py — Générateur de données brutes intentionnellement imparfaites
Mexora Analytics — Miniprojet ETL & Data Warehouse
Génère 5 000 commandes + produits + clients + référentiel régions
avec TOUS les défauts documentés dans le cahier des charges.
"""

import random
import json
import csv
import os
from datetime import date, timedelta, datetime

random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# Référentiel des régions du Maroc (fichier propre et complet)
# ─────────────────────────────────────────────────────────────────────────────
REGIONS_DATA = [
    # code_ville, nom_ville_standard, province, region_admin, zone_geo, population, code_postal
    ("tanger",      "Tanger",           "Tanger-Assilah",       "Tanger-Tétouan-Al Hoceïma",  "Nord",    947952,  90000),
    ("casablanca",  "Casablanca",       "Casablanca",           "Casablanca-Settat",           "Centre",  3752000, 20000),
    ("rabat",       "Rabat",            "Rabat",                "Rabat-Salé-Kénitra",          "Centre",  577827,  10000),
    ("marrakech",   "Marrakech",        "Marrakech",            "Marrakech-Safi",              "Sud",     928850,  40000),
    ("fes",         "Fès",              "Fès",                  "Fès-Meknès",                  "Centre",  1112072, 30000),
    ("agadir",      "Agadir",           "Agadir-Ida-Ou-Tanane", "Souss-Massa",                 "Sud",     421844,  80000),
    ("meknes",      "Meknès",           "Meknès",               "Fès-Meknès",                  "Centre",  632079,  50000),
    ("oujda",       "Oujda",            "Oujda-Angad",          "Oriental",                    "Est",     405253,  60000),
    ("kenitra",     "Kénitra",          "Kénitra",              "Rabat-Salé-Kénitra",          "Centre",  431282,  14000),
    ("tetouan",     "Tétouan",          "Tétouan",              "Tanger-Tétouan-Al Hoceïma",  "Nord",    380787,  93000),
    ("safi",        "Safi",             "Safi",                 "Marrakech-Safi",              "Sud",     308508,  46000),
    ("elhajeb",     "El Hajeb",         "El Hajeb",             "Fès-Meknès",                  "Centre",  97210,   33000),
    ("beni_mellal", "Beni Mellal",      "Beni Mellal",          "Béni Mellal-Khénifra",        "Centre",  219847,  23000),
    ("nador",       "Nador",            "Nador",                "Oriental",                    "Est",     161726,  62000),
    ("settat",      "Settat",           "Settat",               "Casablanca-Settat",           "Centre",  142250,  26000),
    ("larache",     "Larache",          "Larache",              "Tanger-Tétouan-Al Hoceïma",  "Nord",    125008,  92000),
    ("khouribga",   "Khouribga",        "Khouribga",            "Béni Mellal-Khénifra",        "Centre",  196196,  25000),
    ("berkane",     "Berkane",          "Berkane",              "Oriental",                    "Est",     109236,  63300),
    ("errachidia",  "Errachidia",       "Errachidia",           "Drâa-Tafilalet",              "Sud",     92374,   52000),
    ("ouarzazate",  "Ouarzazate",       "Ouarzazate",           "Drâa-Tafilalet",              "Sud",     71067,   45000),
]

# ─────────────────────────────────────────────────────────────────────────────
# Produits Mexora (électronique, mode, alimentation)
# ─────────────────────────────────────────────────────────────────────────────
PRODUITS = [
    # Électronique — Smartphones
    ("P001", "iPhone 16 Pro 256Go",          "Electronique",   "Smartphones",     "Apple",     "Apple MENA",       12999.00, "USA",    "2024-09-20", True),
    ("P002", "Samsung Galaxy S24 Ultra",     "electronique",   "Smartphones",     "Samsung",   "Samsung Maroc",     9999.00, "Corée",  "2024-01-10", True),
    ("P003", "Xiaomi 14 Pro",               "ELECTRONIQUE",   "Smartphones",     "Xiaomi",    "Xiaomi MENA",       5499.00, "Chine",  "2024-02-15", True),
    ("P004", "Oppo Find X7",               "Electronique",   "Smartphones",     "Oppo",      "Oppo Africa",       4999.00, "Chine",  "2024-03-01", True),
    ("P005", "iPhone 15 128Go",            "Electronique",   "Smartphones",     "Apple",     "Apple MENA",        9999.00, "USA",    "2023-09-22", False),
    # Électronique — Laptops
    ("P006", "MacBook Pro M3 14\"",        "Electronique",   "Laptops",         "Apple",     "Apple MENA",       19999.00, "USA",    "2023-10-25", True),
    ("P007", "Dell XPS 15 OLED",          "electronique",   "Laptops",         "Dell",      "Dell Maroc",       14999.00, "USA",    "2024-01-05", True),
    ("P008", "Lenovo ThinkPad X1 Carbon", "ELECTRONIQUE",   "Laptops",         "Lenovo",    "Lenovo MENA",      12500.00, "Chine",  "2024-02-20", True),
    ("P009", "HP Spectre x360 14",        "Electronique",   "Laptops",         "HP",        "HP Maroc",         11999.00, "USA",    "2024-03-15", True),
    ("P010", "Asus ROG Zephyrus G14",     "Electronique",   "Laptops",         "Asus",      "Asus MENA",        13500.00, "Taiwan", "2024-04-01", True),
    # Électronique — Audio
    ("P011", "AirPods Pro 2ème gen",       "Electronique",   "Audio",           "Apple",     "Apple MENA",        2999.00, "USA",    "2023-09-22", True),
    ("P012", "Sony WH-1000XM5",           "electronique",   "Audio",           "Sony",      "Sony Maroc",        2499.00, "Japon",  "2022-05-12", True),
    ("P013", "Bose QuietComfort 45",      "Electronique",   "Audio",           "Bose",      "Bose MENA",         2299.00, "USA",    "2021-09-23", True),
    ("P014", "JBL Charge 5",             "ELECTRONIQUE",   "Audio",           "JBL",       "JBL Africa",          799.00, "USA",    "2021-07-01", True),
    ("P015", "Samsung Galaxy Buds2 Pro", "Electronique",   "Audio",           "Samsung",   "Samsung Maroc",      1299.00, "Corée",  "2022-08-10", True),
    # Mode — Vêtements Homme
    ("P016", "Polo Ralph Lauren Classic", "Mode",           "Vetements Homme", "Ralph Lauren", "Luxury MENA",    1299.00, "USA",    "2023-01-15", True),
    ("P017", "Chemise Zara Slim Fit",    "mode",           "Vetements Homme", "Zara",      "Inditex Maroc",       399.00, "Espagne","2024-01-20", True),
    ("P018", "Jean Levi's 501",          "MODE",           "Vetements Homme", "Levi's",    "Levi's MENA",         699.00, "USA",    "2023-06-01", True),
    ("P019", "Veste Tommy Hilfiger",     "Mode",           "Vetements Homme", "Tommy H",   "PVH Maroc",          1499.00, "USA",    "2023-09-01", True),
    ("P020", "T-shirt Adidas Originals", "Mode",           "Vetements Homme", "Adidas",    "Adidas Maroc",        299.00, "Allemagne","2024-02-10", True),
    # Mode — Vêtements Femme
    ("P021", "Robe Zara Fleurie",        "mode",           "Vetements Femme", "Zara",      "Inditex Maroc",       499.00, "Espagne","2024-03-01", True),
    ("P022", "Abaya Dubai Brodée",       "Mode",           "Vetements Femme", "Kamar",     "Kamar Fashion",       899.00, "EAU",    "2023-11-15", True),
    ("P023", "Sac à main Michael Kors",  "MODE",           "Accessoires",     "M. Kors",   "Luxury MENA",        2499.00, "USA",    "2023-08-20", True),
    ("P024", "Nike Air Max 90 W",        "Mode",           "Chaussures",      "Nike",      "Nike Maroc",         1199.00, "USA",    "2024-01-15", True),
    ("P025", "Adidas Stan Smith W",      "mode",           "Chaussures",      "Adidas",    "Adidas Maroc",        899.00, "Allemagne","2023-07-01", True),
    # Alimentation
    ("P026", "Huile d'Olive Kamel 5L",   "Alimentation",   "Huiles",          "Kamel",     "Kamel Agro",          229.00, "Maroc",  "2024-01-01", True),
    ("P027", "Miel Naturel Atlas 1kg",   "alimentation",   "Miels",           "Atlas Bio", "Atlas Bio SARL",      349.00, "Maroc",  "2023-06-01", True),
    ("P028", "Dates Mejhoul Premium 1kg","ALIMENTATION",   "Fruits Secs",     "Tafilalet", "Tafilalet Exp.",       199.00, "Maroc",  "2024-09-01", True),
    ("P029", "Argan Cosmétique 100ml",   "Alimentation",   "Huiles",          "Aknari",    "Aknari SARL",         299.00, "Maroc",  "2023-03-15", True),
    ("P030", "Sardines Habibas 120g x6", "alimentation",   "Conserves",       "Habibas",   "Habibas Pêche",        89.00, "Algérie","2024-02-01", True),
    # Produit ancien avec prix null
    ("P031", "Nokia 3310 2G",           "Electronique",   "Téléphones",      "Nokia",     "HMD Maroc",           None,   "Finlande","2017-01-01", False),
    ("P032", "Blackberry Bold 9900",    "electronique",   "Téléphones",      "Blackberry","BB Maroc",             None,   "Canada", "2011-06-01", False),
]

# ─────────────────────────────────────────────────────────────────────────────
# Données clients
# ─────────────────────────────────────────────────────────────────────────────
PRENOMS_H = ["Mohammed", "Ahmed", "Youssef", "Omar", "Hassan", "Ibrahim", "Khalid",
             "Amine", "Mehdi", "Tariq", "Rachid", "Samir", "Karim", "Nabil", "Soufiane"]
PRENOMS_F = ["Fatima", "Zineb", "Nadia", "Samira", "Khadija", "Amina", "Layla",
             "Meryem", "Houda", "Sara", "Nisrine", "Imane", "Hajar", "Salma", "Rim"]
NOMS = ["El Amrani", "Benali", "Berrada", "Cherkaoui", "Ait Benhaddou", "Benkirane",
        "Hajji", "Tazi", "El Mansouri", "Bouazza", "Lahlou", "Filali", "Kadiri",
        "El Idrissi", "Bensouda", "Hassani", "El Fassi", "Chraibi", "Belkadi", "Sahraoui"]
DOMAINES = ["gmail.com", "yahoo.fr", "hotmail.com", "outlook.fr", "menara.ma", "iam.ma"]
CANAUX = ["organic", "social_media", "email_campaign", "referral", "paid_ads", "marketplace"]

VILLES_CLIENTS_BRUTS = [
    "tanger", "TNG", "TANGER", "Tnja", "Tanger",
    "casablanca", "CASABLANCA", "Casa", "CAS", "Casablanca",
    "rabat", "RABAT", "Rabat",
    "marrakech", "MARRAKECH", "Marrakesh", "Mrakch",
    "fes", "FES", "Fès", "Fez",
    "agadir", "AGADIR", "Agadir",
    "meknes", "MEKNES", "Meknès",
]

LIVREURS = [f"L{str(i).zfill(3)}" for i in range(1, 31)]


def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def format_date_mixte(d: date) -> str:
    """Retourne une date dans un des 3 formats problématiques."""
    fmt = random.choice(["fr", "iso", "en"])
    if fmt == "fr":
        return d.strftime("%d/%m/%Y")
    elif fmt == "iso":
        return d.strftime("%Y-%m-%d")
    else:
        return d.strftime("%b %d %Y")


def statut_brut() -> str:
    """Retourne un statut avec les variantes non-standards."""
    valeurs = [
        "livré", "livré", "livré", "livré",      # majorité
        "livre", "LIVRE", "DONE",                  # variantes "livré"
        "annulé", "annule", "KO",                  # variantes "annulé"
        "en_cours", "OK",                           # variantes "en_cours"
        "retourné", "retourne",                     # variantes "retourné"
    ]
    return random.choice(valeurs)


def ville_brute() -> str:
    return random.choice(VILLES_CLIENTS_BRUTS)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Génération regions_maroc.csv  (fichier propre)
# ─────────────────────────────────────────────────────────────────────────────
def generate_regions(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code_ville", "nom_ville_standard", "province",
                         "region_admin", "zone_geo", "population", "code_postal"])
        for row in REGIONS_DATA:
            writer.writerow(row)
    print(f"[GEN] regions_maroc.csv → {len(REGIONS_DATA)} villes")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Génération produits_mexora.json  (avec défauts de casse + prix null)
# ─────────────────────────────────────────────────────────────────────────────
def generate_produits(path: str):
    produits = []
    for row in PRODUITS:
        produits.append({
            "id_produit":      row[0],
            "nom":             row[1],
            "categorie":       row[2],        # casse intentionnellement variable
            "sous_categorie":  row[3],
            "marque":          row[4],
            "fournisseur":     row[5],
            "prix_catalogue":  row[6],        # None pour certains anciens produits
            "origine_pays":    row[7],
            "date_creation":   row[8],
            "actif":           row[9],
        })
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"produits": produits}, f, ensure_ascii=False, indent=2)
    print(f"[GEN] produits_mexora.json → {len(produits)} produits")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Génération clients_mexora.csv  (avec tous les défauts)
# ─────────────────────────────────────────────────────────────────────────────
def generate_clients(path: str, n: int = 1200):
    """
    Défauts intentionnels :
    - Doublons (même email, id_client différent) ~5%
    - Sexe codé différemment : m/f, 1/0, Homme/Femme
    - Dates de naissance invalides (âge négatif ou >120 ans)
    - Villes incohérentes (mêmes variantes que commandes)
    - Emails mal formatés (~5%)
    """
    clients = []
    emails_pool = []

    SEXE_FORMATS = [
        lambda s: s,            # m / f
        lambda s: "1" if s == "m" else "0",
        lambda s: "Homme" if s == "m" else "Femme",
        lambda s: s.upper(),    # M / F
        lambda s: "male" if s == "m" else "female",
        lambda s: "h" if s == "m" else "f",
    ]

    for i in range(1, n + 1):
        sexe_base = random.choice(["m", "f"])
        prenom = random.choice(PRENOMS_H if sexe_base == "m" else PRENOMS_F)
        nom    = random.choice(NOMS)
        domaine = random.choice(DOMAINES)

        # Email normal
        email_clean = f"{prenom.lower().replace(' ', '')}.{nom.lower().replace(' ', '')}{random.randint(1,99)}@{domaine}"

        # ~5% emails mal formatés
        if random.random() < 0.05:
            choix = random.randint(0, 2)
            if choix == 0:
                email = email_clean.replace("@", "")         # sans @
            elif choix == 1:
                email = email_clean.split("@")[0] + "@"      # sans domaine
            else:
                email = "invalide-" + str(i)
        else:
            email = email_clean

        emails_pool.append(email_clean)

        # Date naissance — la plupart valides, ~3% invalides
        if random.random() < 0.03:
            if random.random() < 0.5:
                # Âge négatif (date dans le futur)
                dob = date.today() + timedelta(days=random.randint(1, 365))
            else:
                # Âge >120 ans
                dob = date(random.randint(1880, 1900), random.randint(1, 12), random.randint(1, 28))
        else:
            dob = rand_date(date(1960, 1, 1), date(2005, 12, 31))

        sexe_fmt = random.choice(SEXE_FORMATS)
        ville_c  = ville_brute()
        date_ins = rand_date(date(2020, 1, 1), date(2024, 12, 31))
        canal    = random.choice(CANAUX)

        clients.append({
            "id_client":       f"C{str(i).zfill(5)}",
            "nom":             nom,
            "prenom":          prenom,
            "email":           email,
            "date_naissance":  dob.strftime("%Y-%m-%d"),
            "sexe":            sexe_fmt(sexe_base),
            "ville":           ville_c,
            "telephone":       f"+2126{random.randint(10000000, 99999999)}",
            "date_inscription": date_ins.strftime("%Y-%m-%d"),
            "canal_acquisition": canal,
        })

    # Doublons intentionnels (~5%) : même email, nouvel id_client
    nb_doublons = int(n * 0.05)
    for _ in range(nb_doublons):
        original = random.choice(clients)
        doublon  = original.copy()
        doublon["id_client"] = f"C{str(random.randint(n+1, n+500)).zfill(5)}"
        # Même email (normalisé)
        doublon["email"]     = original["email"]
        # Date inscription légèrement différente
        doublon["date_inscription"] = (
            datetime.strptime(original["date_inscription"], "%Y-%m-%d")
            + timedelta(days=random.randint(1, 30))
        ).strftime("%Y-%m-%d")
        clients.append(doublon)

    random.shuffle(clients)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id_client", "nom", "prenom", "email", "date_naissance",
            "sexe", "ville", "telephone", "date_inscription", "canal_acquisition"
        ])
        writer.writeheader()
        writer.writerows(clients)
    print(f"[GEN] clients_mexora.csv → {len(clients)} clients (dont {nb_doublons} doublons)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Génération commandes_mexora.csv  (5 000 lignes avec tous les défauts)
# ─────────────────────────────────────────────────────────────────────────────
def generate_commandes(path: str, n: int = 5000):
    """
    Défauts intentionnels (selon le cahier des charges) :
    - Doublons sur id_commande ~3%
    - Dates en formats mixtes (DD/MM/YYYY, YYYY-MM-DD, Mon DD YYYY)
    - ville_livraison incohérente (tanger / TNG / TANGER / Tnja)
    - Valeurs manquantes sur id_livreur ~7%
    - quantite négative ~1%
    - prix_unitaire = 0 ~1% (commandes test)
    - statut avec valeurs non-standards (OK, KO, DONE, livre, etc.)
    """
    produit_ids = [p[0] for p in PRODUITS]
    produit_prix = {p[0]: p[6] or 999.0 for p in PRODUITS}

    # Générer pool de clients
    client_ids = [f"C{str(i).zfill(5)}" for i in range(1, 1201)]

    commandes = []
    ids_utilises = set()

    for i in range(1, n + 1):
        cmd_id = f"CMD{str(i).zfill(6)}"
        ids_utilises.add(cmd_id)

        client_id  = random.choice(client_ids)
        produit_id = random.choice(produit_ids)
        prix_base  = produit_prix[produit_id]

        date_cmd = rand_date(date(2022, 1, 1), date(2024, 12, 31))

        # Quantité — ~1% négatif
        if random.random() < 0.01:
            quantite = random.randint(-5, -1)
        else:
            quantite = random.randint(1, 10)

        # Prix unitaire — ~1% = 0 (commandes test)
        if random.random() < 0.01:
            prix_unitaire = 0
        else:
            # Légère variation autour du prix catalogue
            prix_unitaire = round(prix_base * random.uniform(0.85, 1.10), 2)

        # Statut avec variantes
        statut = statut_brut()

        # Ville livraison — variantes incohérentes
        ville = ville_brute()

        # Mode paiement
        mode_paiement = random.choice(["carte_bancaire", "cash", "virement", "cmi", "wafacash"])

        # Livreur — ~7% manquant
        if random.random() < 0.07:
            livreur = ""
        else:
            livreur = random.choice(LIVREURS)

        # Date livraison (si livré)
        if "livr" in statut.lower() or statut in ("DONE", "OK"):
            delai = random.randint(1, 7)
            date_liv = (date_cmd + timedelta(days=delai)).strftime("%Y-%m-%d")
        elif statut in ("retourné", "retourne"):
            delai = random.randint(3, 14)
            date_liv = (date_cmd + timedelta(days=delai)).strftime("%Y-%m-%d")
        else:
            date_liv = ""

        commandes.append({
            "id_commande":    cmd_id,
            "id_client":      client_id,
            "id_produit":     produit_id,
            "date_commande":  format_date_mixte(date_cmd),   # format mixte !
            "quantite":       quantite,
            "prix_unitaire":  prix_unitaire,
            "statut":         statut,
            "ville_livraison": ville,
            "mode_paiement":  mode_paiement,
            "id_livreur":     livreur,
            "date_livraison": date_liv,
        })

    # Doublons intentionnels ~3% : copier des lignes existantes avec le même id_commande
    nb_doublons = int(n * 0.03)
    doublons = []
    for _ in range(nb_doublons):
        original = random.choice(commandes)
        doublon  = original.copy()
        # Légère variation pour simuler une ré-insertion
        doublon["quantite"] = original["quantite"] + random.randint(-1, 1)
        doublons.append(doublon)

    all_commandes = commandes + doublons
    random.shuffle(all_commandes)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id_commande", "id_client", "id_produit", "date_commande",
            "quantite", "prix_unitaire", "statut", "ville_livraison",
            "mode_paiement", "id_livreur", "date_livraison"
        ])
        writer.writeheader()
        writer.writerows(all_commandes)

    print(f"[GEN] commandes_mexora.csv → {len(all_commandes)} lignes "
          f"(dont {nb_doublons} doublons sur id_commande)")


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(DATA_DIR, exist_ok=True)

    print("=" * 60)
    print("GÉNÉRATION DES DATASETS MEXORA ANALYTICS")
    print("=" * 60)

    generate_regions(  os.path.join(DATA_DIR, "regions_maroc.csv"))
    generate_produits( os.path.join(DATA_DIR, "produits_mexora.json"))
    generate_clients(  os.path.join(DATA_DIR, "clients_mexora.csv"),  n=1200)
    generate_commandes(os.path.join(DATA_DIR, "commandes_mexora.csv"), n=5000)

    print("=" * 60)
    print("✅  Tous les fichiers ont été générés dans ./data/")
    print("=" * 60)
