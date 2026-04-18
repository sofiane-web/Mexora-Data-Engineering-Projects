# 📊 Mexora Data Engineering - Pipeline ETL

Ce dépôt contient le travail de fin de module DATA ENGINEERING  sur la mise en place d'un **Data Warehouse** et **DataLake** pour la gestion des ventes de Mexora.


 **Projet 1 : DataWarehouse  & Pipeline Analytique**
## 🛠️ Stack Technique
* **Langage :** Python 3.x
* **Librairies :** Pandas, SQLAlchemy, Psycopg2.
* **Base de données :** PostgreSQL (DBeaver pour la gestion).
* **Format de données :** CSV, JSON.

## 🚀 Fonctionnalités du Pipeline (Projet 1)
1. **Extraction :** Récupération multi-sources (Ventes, Produits, Clients).
2. **Transformation :** - Nettoyage des données (doublons, formats de date).
   - Gestion des **Surrogate Keys (SK)** et **Natural Keys (NK)**.
   - Processus de **Lookup** pour lier les ventes aux dimensions.
3. **Chargement :** Insertion automatisée (Upsert) dans le schéma `dwh_mexora`.

## ⚙️ Installation
```bash
cd Projet_1_DWH
pip install -r requirements.txt
python main.py 
```
# 🚀 Projet 2 : Data Lake & Pipeline Analytique (Mexora RH)

## 📌 Contexte
Ce projet vise à mettre en place un **Data Lake** robuste pour le département RH de Mexora, permettant d'analyser le marché de l'emploi IT au Maroc (salaires, compétences, tendances).

## 🏗️ Architecture du Pipeline (Médaillon)

Le pipeline est divisé en trois zones distinctes :

### 🥉 1. Zone Bronze (Ingestion)
- **Objectif :** Stockage des données brutes telles qu'elles ont été extraites, sans altération.
- **Processus :** Lecture du fichier source JSON (5000 offres) et partitionnement par `source` (LinkedIn, Rekrute, etc.) et par `mois_publication`.
- **Format :** JSON.

### 🥈 2. Zone Silver (Nettoyage et Transformation)
- **Objectif :** Nettoyage, standardisation et extraction de valeur (NLP).
- **Processus (Pandas) :**
  - **Standardisation :** Normalisation des titres de postes (ex: "Dev Data" -> "Data Engineer").
  - **Nettoyage :** Extraction des salaires min/max à partir de texte brut et conversion en MAD.
  - **NLP :** Recherche par mots-clés (Regex) pour extraire les compétences (Python, SQL, React, etc.) en se basant sur un référentiel.
- **Format :** Parquet (pour la compression et les performances de lecture).

### 🥇 3. Zone Gold (Agrégation et Business Intelligence)
- **Objectif :** Création de tables prêtes pour la visualisation et la réponse aux questions métiers.
- **Processus (DuckDB) :** Utilisation du moteur DuckDB pour exécuter des requêtes SQL ultra-rapides directement sur les fichiers Parquet de la zone Silver.
- **Tables générées :** `top_competences`, `salaires_par_profil`, `offres_par_ville`, `entreprises_recruteurs`, `tendances_mensuelles`.
- **Format :** Parquet.

## 🛠️ Technologies Utilisées
- **Langage :** Python 3
- **Manipulation de données :** Pandas, PyArrow
- **Moteur SQL Analytique :** DuckDB
- **Visualisation :** Jupyter Notebook, Matplotlib, Seaborn

