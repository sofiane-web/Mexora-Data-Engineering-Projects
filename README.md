# Pipeline ETL Mexora Analytics 🏪

Data Warehouse complet pour Mexora, marketplace e-commerce basée à Tanger.

## Architecture du projet

```
mexora_etl/
├── config/
│   └── settings.py          # Paramètres centralisés (DB, chemins, règles métier)
├── extract/
│   └── extractor.py         # Extraction par source (CSV, JSON)
├── transform/
│   ├── clean_commandes.py   # 9 règles de nettoyage des commandes
│   ├── clean_clients.py     # 7 règles de nettoyage des clients
│   ├── clean_produits.py    # 5 règles de nettoyage des produits
│   └── build_dimensions.py  # Construction des 5 dimensions + table de faits
├── load/
│   └── loader.py            # Chargement PostgreSQL (replace + upsert)
├── utils/
│   └── logger.py            # Logging structuré + rapport de transformation
├── data/                    # Fichiers sources (générés par generate_dataset.py)
│   ├── commandes_mexora.csv
│   ├── produits_mexora.json
│   ├── clients_mexora.csv
│   └── regions_maroc.csv
├── logs/                    # Logs horodatés du pipeline
├── output_parquet/          # Sortie dry-run
├── generate_dataset.py      # Générateur de données de test (5 000 commandes)
├── main.py                  # Orchestrateur du pipeline
├── create_dwh.sql           # DDL complet du Data Warehouse
├── check_integrity.sql      # Script de vérification post-chargement
├── rapport_transformations.md
└── requirements.txt
```

## Installation

```bash
# Cloner le dépôt
git clone https://github.com/votre-compte/mexora-etl.git
cd mexora-etl

# Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## Démarrage rapide

### 1. Générer les données de test

```bash
python generate_dataset.py
```

Crée dans `data/` :
| Fichier | Lignes | Défauts intentionnels |
|---------|-------:|----------------------|
| `commandes_mexora.csv` | 5 150 | 150 doublons, dates mixtes, statuts non-standards |
| `clients_mexora.csv` | 1 260 | 60 doublons email, sexe incohérent, emails invalides |
| `produits_mexora.json` | 32 | Casse catégories incohérente, prix NULL |
| `regions_maroc.csv` | 20 | ✅ Propre — référentiel géographique |

### 2. Lancer le pipeline (sans PostgreSQL)

```bash
python main.py --dry-run
```

Sortie dans `output_parquet/` :
- `dim_temps.parquet`   (2 557 jours)
- `dim_produit.parquet` (32 produits)
- `dim_client.parquet`  (1 197 clients nets)
- `dim_region.parquet`  (21 régions)
- `dim_livreur.parquet` (31 livreurs)
- `fait_ventes.parquet` (~4 675 lignes)

### 3. Lancer le pipeline complet (avec PostgreSQL)

```bash
# Variables d'environnement (ou modifier config/settings.py)
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=mexora_dwh
export PG_USER=mexora_etl
export PG_PASSWORD=mexora_secret

# Créer le DWH PostgreSQL
psql -U postgres -c "CREATE DATABASE mexora_dwh;"
psql -U postgres -d mexora_dwh -f create_dwh.sql

# Lancer le pipeline
python main.py

# Vérifier l'intégrité
psql -U mexora_etl -d mexora_dwh -f check_integrity.sql
```

### 4. Options de la CLI

```bash
python main.py --dry-run           # Sans PostgreSQL (→ Parquet)
python main.py --phase extract     # Extraction seule
python main.py --phase transform   # Extraction + transformation
python main.py                     # Pipeline complet
```

## Règles de transformation

### Commandes (9 règles)

| Règle | Description | Lignes affectées |
|-------|-------------|:---:|
| R1 | Suppression doublons `id_commande` (keep=last) | ~150 |
| R2 | Standardisation dates (DD/MM/YYYY, YYYY-MM-DD, Mon DD YYYY → datetime) | ~0 |
| R3 | Harmonisation villes via référentiel (tanger/TNG/TANJA → Tanger) | 5 000 |
| R4 | Standardisation statuts (OK→en_cours, KO→annulé, DONE→livré) | ~2 500 |
| R5 | Suppression quantités ≤ 0 | ~50 |
| R6 | Suppression prix = 0 (commandes test) | ~50 |
| R7 | Remplacement `id_livreur` manquants par -1 | ~350 |
| R8 | Calcul `montant_ht` et `montant_ttc` (TVA 20%) | Tous |
| R9 | Calcul `delai_livraison_jours`, neutralisation délais négatifs | Variable |

### Clients (7 règles)

| Règle | Description |
|-------|-------------|
| R1 | Déduplication sur email normalisé (conserver inscription la plus récente) |
| R2 | Standardisation sexe (m/f/1/0/Homme/Femme → m/f/inconnu) |
| R3 | Validation dates de naissance (âge 16-100 ans) |
| R4 | Calcul tranche d'âge (<18, 18-24, 25-34, 35-44, 45-54, 55-64, 65+) |
| R5 | Validation format email (invalides → NULL, ligne conservée) |
| R6 | Harmonisation villes via référentiel |
| R7 | Normalisation nom_complet (prénom + nom en title case) |

### Produits (5 règles)

| Règle | Description |
|-------|-------------|
| R1 | Normalisation casse catégories (electronique/ELECTRONIQUE → Electronique) |
| R2 | Remplacement prix NULL par 0.01 (produits discontinués) |
| R3 | Nettoyage champs texte (strip + espaces multiples) |
| R4 | Préparation colonnes SCD Type 2 (date_debut, date_fin, est_actif) |
| R5 | Validation dates de création |

## Modèle dimensionnel

```
                    ┌──────────────┐
                    │  DIM_TEMPS   │
                    │  id_date PK  │
                    └──────┬───────┘
                           │
┌────────────────┐   ┌─────┴──────────┐   ┌──────────────────┐
│  DIM_PRODUIT   │   │  FAIT_VENTES   │   │   DIM_CLIENT     │
│ id_produit_sk  ├───┤  id_vente PK   ├───┤  id_client_sk PK │
│ (SCD Type 2)   │   │  quantite      │   │  (SCD Type 2)    │
└────────────────┘   │  montant_ht    │   └──────────────────┘
                     │  montant_ttc   │
┌────────────────┐   │  cout_livraison│   ┌──────────────────┐
│  DIM_REGION    ├───┤  delai_jours   ├───┤   DIM_LIVREUR    │
│  id_region PK  │   │  remise_pct    │   │  id_livreur PK   │
└────────────────┘   └───────────────┘   └──────────────────┘
```

**Granularité** : une ligne = une commande × un produit

## Stack technique

| Outil | Version | Usage |
|-------|---------|-------|
| Python | 3.11+ | ETL |
| pandas | 2.1+ | Transformation |
| SQLAlchemy | 2.0+ | Connexion PostgreSQL |
| psycopg2 | 2.9+ | Driver PostgreSQL |
| pyarrow | 14+ | Format Parquet (dry-run) |
| PostgreSQL | 15+ | Data Warehouse |
