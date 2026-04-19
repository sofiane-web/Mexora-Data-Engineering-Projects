# 🚀 Projet 2 : Data Lake & Analyse du Marché de l'Emploi IT au Maroc

Ce projet fait partie du programme d'Ingénierie des Données (FST Tangier). Il s'agit d'un **Data Lake** complet basé sur l'architecture **Medallion**, permettant d'analyser les tendances du marché de l'emploi IT au Maroc (compétences, salaires, opportunités par ville).

## 🏗️ Architecture du Projet

Le pipeline de données est structuré en trois couches distinctes :

* **🥉 Zone Bronze (`bronze_ingestion.py`) :** Ingestion des données brutes (JSON) issues du scraping (Rekrute, LinkedIn, etc.) selon le paradigme *Schema-on-Read*. Données immuables.
* **🥈 Zone Silver (`silver_transform.py`) :** Nettoyage avec **Pandas**, extraction automatique des compétences via **NLP (Expressions Régulières)**, et sauvegarde en format **Parquet**.
* **🥇 Zone Gold (`gold_aggregation.py`) :** Utilisation de **DuckDB** pour exécuter des requêtes analytiques ultra-rapides directement sur les fichiers Parquet et générer des Data Marts agrégés.

## 🛠️ Technologies Utilisées
* **Langage :** Python 3
* **Data Processing :** Pandas, PyArrow
* **Base de données analytique :** DuckDB
* **Visualisation :** Jupyter Notebook, Matplotlib, Seaborn

## 🚀 Comment exécuter le projet ?

1. **Installer les dépendances :**
   ```bash
   pip install -r requirements.txt