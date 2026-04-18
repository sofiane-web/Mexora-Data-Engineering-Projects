# 🚀 Projet Data Warehouse & ETL - Mexora

## 📋 Présentation du Projet
Ce projet consiste en la création d'un pipeline de données (ETL) de bout en bout pour l'entreprise Mexora. L'objectif est d'extraire des données brutes hétérogènes (fichiers CSV et JSON), de les nettoyer et de les transformer, puis de les charger dans un Data Warehouse PostgreSQL modélisé en **Schéma en Étoile**. 

Ce socle de données propre et structuré alimente ensuite un Dashboard Power BI pour l'analyse décisionnelle (Business Intelligence).

---

## 🛠️ Stack Technique & Dépendances
Le projet repose sur Python et les bibliothèques suivantes :
* `pandas>=2.1.0` : Manipulation et nettoyage des données.
* `numpy>=1.26.0` : Opérations numériques.
* `sqlalchemy>=2.0.0` : ORM pour l'interaction avec la base de données.
* `psycopg2-binary>=2.9.9` : Connecteur PostgreSQL.
* `pyarrow>=14.0.0` : Optimisation du traitement des données.

---

## 📂 Architecture du Projet

L'architecture du code est modulaire pour séparer clairement les étapes de l'ETL :

* **`data/`** : Contient les fichiers sources bruts (`clients_mexora.csv`, `commandes_mexora.csv`, `produits_mexora.json`, `regions_maroc.csv`).
* **`extract/`** : Scripts chargés de la lecture et de l'ingestion des données brutes.
* **`transform/`** : Coeur de la logique métier. Contient les scripts de nettoyage (gestion des doublons, valeurs nulles, formatage) et la création des tables de dimensions et de faits.
* **`load/`** : Scripts gérant la connexion et l'insertion sécurisée des données finales vers PostgreSQL.
* **`config/`** : Fichiers de configuration (paramètres de la base de données).
* **`utils/`** : Fonctions partagées (ex: système de logging pour suivre l'exécution).
* **`main.py`** : Script principal (Orchestrateur) qui lance le pipeline complet.

---

## 🚀 Comment exécuter le projet

### 1. Préparation de l'environnement
Il est recommandé de créer un environnement virtuel pour installer les dépendances du projet :
```bash
python -m venv .venv
source .venv/Scripts/activate  # Sous Windows : .venv\Scripts\activate
pip install -r requirements.txt
```
Lancez le script principal depuis la racine du dossier Projet_1_DWH :
`python main.py`
