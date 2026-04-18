🚀 Mexora Data Platform : DWH & Data Lake
Ce dépôt centralise les travaux du module Data Engineering portant sur la conception d'une plateforme de données hybride pour l'entreprise Mexora. Le projet combine une approche Data Warehouse traditionnelle pour le suivi commercial et une architecture Data Lake moderne pour l'analyse RH.

🏗️ Architecture Globale
Le projet s'articule autour de deux pôles d'ingénierie complémentaires :

1️⃣ **Module Ventes : Data Warehouse (Moad AFYLAL)**
Objectif : Industrialiser le suivi des performances commerciales via un pipeline ETL structuré.

Modélisation : Schéma en étoile (Star Schema) optimisé pour le décisionnel.

Pipeline ETL : Extraction multi-sources, gestion des clés (Surrogate & Natural Keys) et processus de Lookup pour l'intégrité référentielle.

Stockage : PostgreSQL (Schéma dwh_mexora).

Stack : Python (Pandas, SQLAlchemy, Psycopg2).

⚙️ Installation et Utilisation
cd Projet_1_DWH
pip install -r requirements.txt
python main.py

2️⃣ **Module RH : Data Lake (Sofyane FRITIT)**
Objectif : Analyser le marché de l'emploi IT au Maroc via une architecture Médaillon.

Architecture : Flux de données divisé en zones Bronze (Brut), Silver (Nettoyage/NLP) et Gold (Agrégation).

Traitement : Extraction de salaires, standardisation de profils et détection de compétences par Regex.

Stockage : Formats JSON et Parquet (Optimisation colonne).

Stack : Python (Pandas), DuckDB (Moteur analytique), PyArrow.

🛠️ Détails Techniques & Fonctionnalités
📊 Pipeline ETL (Ventes)
Extraction : Consolidation des flux Ventes, Produits et Clients.

Transformation : Nettoyage rigoureux, formatage des dates et dédoublonnage.

Chargement : Procédure d'insertion automatisée avec gestion des mises à jour.

🌊 Pipeline Analytique (RH)
Ingestion : Stockage brut partitionné par source et période.

Silver Layer : Transformation des descriptions textuelles en données structurées (Extraction salaires & compétences).

Gold Layer : Requêtage SQL ultra-rapide via DuckDB pour générer les KPIs métiers (Top compétences, tendances salariales).

⚙️ Installation et Utilisation 
cd Projet_2_DataLake