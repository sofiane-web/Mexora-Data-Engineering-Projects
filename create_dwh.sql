-- ============================================================
-- create_dwh.sql — Création complète du Data Warehouse Mexora
-- PostgreSQL 15+
-- Schémas : staging_mexora | dwh_mexora | reporting_mexora
-- ============================================================

-- ─── Schémas dédiés ──────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging_mexora;
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;

-- ─── Extension uuid (optionnelle) ────────────────────────────
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- DIMENSIONS
-- ============================================================

-- Dimension Temps
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_temps (
    id_date             INTEGER PRIMARY KEY,        -- format YYYYMMDD
    jour                SMALLINT  NOT NULL CHECK (jour BETWEEN 1 AND 31),
    mois                SMALLINT  NOT NULL CHECK (mois BETWEEN 1 AND 12),
    trimestre           SMALLINT  NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee               SMALLINT  NOT NULL,
    semaine             SMALLINT,
    libelle_jour        VARCHAR(20),
    libelle_mois        VARCHAR(20),
    libelle_trimestre   VARCHAR(10),
    est_weekend         BOOLEAN   DEFAULT FALSE,
    est_ferie_maroc     BOOLEAN   DEFAULT FALSE,
    periode_ramadan     BOOLEAN   DEFAULT FALSE
);

-- Dimension Produit (SCD Type 2)
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_produit (
    id_produit_sk       SERIAL      PRIMARY KEY,          -- surrogate key
    id_produit_nk       VARCHAR(20) NOT NULL,             -- natural key (source)
    nom_produit         VARCHAR(200) NOT NULL,
    categorie           VARCHAR(100),
    sous_categorie      VARCHAR(100),
    marque              VARCHAR(100),
    fournisseur         VARCHAR(100),
    prix_standard       DECIMAL(10,2),
    origine_pays        VARCHAR(50),
    date_creation       DATE,
    actif               BOOLEAN     DEFAULT TRUE,
    -- SCD Type 2
    date_debut          DATE        NOT NULL DEFAULT CURRENT_DATE,
    date_fin            DATE        NOT NULL DEFAULT '9999-12-31',
    est_actif           BOOLEAN     NOT NULL DEFAULT TRUE
);

-- Dimension Client (SCD Type 2 sur segment_client)
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_client (
    id_client_sk        SERIAL      PRIMARY KEY,
    id_client_nk        VARCHAR(20) NOT NULL,
    nom_complet         VARCHAR(200),
    tranche_age         VARCHAR(10),
    sexe                CHAR(1)     CHECK (sexe IN ('m','f','i')),
    ville               VARCHAR(100),
    canal_acquisition   VARCHAR(50),
    segment_client      VARCHAR(20) CHECK (segment_client IN ('Gold','Silver','Bronze')),
    date_inscription    DATE,
    -- SCD Type 2
    date_debut          DATE        NOT NULL DEFAULT CURRENT_DATE,
    date_fin            DATE        NOT NULL DEFAULT '9999-12-31',
    est_actif           BOOLEAN     NOT NULL DEFAULT TRUE
);

-- Dimension Région
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_region (
    id_region           SERIAL      PRIMARY KEY,
    ville               VARCHAR(100) NOT NULL,
    province            VARCHAR(100),
    region_admin        VARCHAR(100),
    zone_geo            VARCHAR(50),
    pays                VARCHAR(50)  DEFAULT 'Maroc'
);

-- Dimension Livreur
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_livreur (
    id_livreur          SERIAL      PRIMARY KEY,
    id_livreur_nk       VARCHAR(20),
    nom_livreur         VARCHAR(100),
    type_transport      VARCHAR(50),
    zone_couverture     VARCHAR(100)
);

-- ============================================================
-- TABLE DE FAITS
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh_mexora.fait_ventes (
    id_vente                BIGSERIAL   PRIMARY KEY,
    -- Clés étrangères
    id_date                 INTEGER     NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit              INTEGER     NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client               INTEGER     NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region               INTEGER     NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur              INTEGER     REFERENCES dwh_mexora.dim_livreur(id_livreur),
    -- Mesures additives
    quantite_vendue         INTEGER     NOT NULL CHECK (quantite_vendue > 0),
    montant_ht              DECIMAL(12,2) NOT NULL,
    montant_ttc             DECIMAL(12,2) NOT NULL,
    cout_livraison          DECIMAL(8,2)  DEFAULT 0,
    -- Mesures semi-additives
    delai_livraison_jours   SMALLINT,
    -- Mesures non-additives (taux — à recalculer)
    remise_pct              DECIMAL(5,2)  DEFAULT 0,
    -- Métadonnées ETL
    statut_commande         VARCHAR(20)   CHECK (statut_commande IN ('livré','annulé','en_cours','retourné')),
    date_chargement         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXATION
-- ============================================================

-- Index sur les clés étrangères (jointures analytiques)
CREATE INDEX IF NOT EXISTS idx_fv_date     ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX IF NOT EXISTS idx_fv_produit  ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX IF NOT EXISTS idx_fv_client   ON dwh_mexora.fait_ventes(id_client);
CREATE INDEX IF NOT EXISTS idx_fv_region   ON dwh_mexora.fait_ventes(id_region);
CREATE INDEX IF NOT EXISTS idx_fv_livreur  ON dwh_mexora.fait_ventes(id_livreur);

-- Index composites pour les requêtes analytiques fréquentes
CREATE INDEX IF NOT EXISTS idx_fv_date_region
    ON dwh_mexora.fait_ventes(id_date, id_region)
    INCLUDE (montant_ttc, quantite_vendue);

CREATE INDEX IF NOT EXISTS idx_fv_statut_livre
    ON dwh_mexora.fait_ventes(statut_commande)
    WHERE statut_commande = 'livré';

CREATE INDEX IF NOT EXISTS idx_fv_annee_mois
    ON dwh_mexora.fait_ventes(id_date, id_produit, id_client)
    INCLUDE (montant_ttc, quantite_vendue, statut_commande);

-- Index sur les natural keys des dimensions (lookups ETL)
CREATE INDEX IF NOT EXISTS idx_dp_nk ON dwh_mexora.dim_produit(id_produit_nk);
CREATE INDEX IF NOT EXISTS idx_dc_nk ON dwh_mexora.dim_client(id_client_nk);
CREATE INDEX IF NOT EXISTS idx_dl_nk ON dwh_mexora.dim_livreur(id_livreur_nk);

-- ============================================================
-- VUES MATÉRIALISÉES — REPORTING
-- ============================================================

-- Vue 1 : CA mensuel par région et catégorie (avec Ramadan)
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_ca_mensuel AS
SELECT
    t.annee,
    t.mois,
    t.libelle_mois,
    t.libelle_trimestre,
    t.periode_ramadan,
    r.region_admin,
    r.zone_geo,
    p.categorie,
    SUM(f.montant_ttc)              AS ca_ttc,
    SUM(f.montant_ht)               AS ca_ht,
    COUNT(DISTINCT f.id_client)     AS nb_clients_actifs,
    SUM(f.quantite_vendue)          AS volume_vendu,
    ROUND(AVG(f.montant_ttc)::numeric, 2) AS panier_moyen,
    COUNT(DISTINCT f.id_vente)      AS nb_commandes
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_region  r ON f.id_region  = r.id_region
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE f.statut_commande = 'livré'
GROUP BY
    t.annee, t.mois, t.libelle_mois, t.libelle_trimestre, t.periode_ramadan,
    r.region_admin, r.zone_geo, p.categorie
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_ca_annee_mois ON reporting_mexora.mv_ca_mensuel(annee, mois);
CREATE INDEX IF NOT EXISTS idx_mv_ca_region     ON reporting_mexora.mv_ca_mensuel(region_admin);
CREATE INDEX IF NOT EXISTS idx_mv_ca_categorie  ON reporting_mexora.mv_ca_mensuel(categorie);

-- Vue 2 : Top produits par trimestre (avec rang dans catégorie)
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_top_produits AS
SELECT
    t.annee,
    t.trimestre,
    t.libelle_trimestre,
    r.ville,
    r.region_admin,
    p.nom_produit,
    p.categorie,
    p.marque,
    SUM(f.quantite_vendue)              AS qte_totale,
    SUM(f.montant_ttc)                  AS ca_total,
    COUNT(DISTINCT f.id_client)         AS nb_clients_distincts,
    RANK() OVER (
        PARTITION BY t.annee, t.trimestre, r.region_admin, p.categorie
        ORDER BY SUM(f.montant_ttc) DESC
    ) AS rang_dans_categorie_region
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
JOIN dwh_mexora.dim_region  r ON f.id_region  = r.id_region
WHERE f.statut_commande = 'livré'
GROUP BY
    t.annee, t.trimestre, t.libelle_trimestre,
    r.ville, r.region_admin,
    p.nom_produit, p.categorie, p.marque
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_tp_trim  ON reporting_mexora.mv_top_produits(annee, trimestre);
CREATE INDEX IF NOT EXISTS idx_mv_tp_ville ON reporting_mexora.mv_top_produits(ville);

-- Vue 3 : Performance livreurs (taux de retard)
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_performance_livreurs AS
SELECT
    l.nom_livreur,
    l.zone_couverture,
    l.type_transport,
    t.annee,
    t.mois,
    COUNT(*)                                              AS nb_livraisons,
    ROUND(AVG(f.delai_livraison_jours)::numeric, 1)     AS delai_moyen_jours,
    COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3)  AS nb_livraisons_retard,
    ROUND(
        COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3) * 100.0
        / NULLIF(COUNT(*), 0)
    , 2)                                                  AS taux_retard_pct
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_livreur l ON f.id_livreur = l.id_livreur
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
WHERE f.statut_commande IN ('livré','retourné')
  AND f.delai_livraison_jours IS NOT NULL
GROUP BY l.nom_livreur, l.zone_couverture, l.type_transport, t.annee, t.mois
WITH DATA;

-- ============================================================
-- KPIs ANALYTIQUES (REQUÊTES DASHBOARD)
-- ============================================================

-- KPI 1 : Évolution CA mensuel avec comparaison N-1
CREATE OR REPLACE VIEW reporting_mexora.v_ca_evolution AS
WITH ca_mensuel AS (
    SELECT annee, mois, SUM(ca_ttc) AS ca_total
    FROM reporting_mexora.mv_ca_mensuel
    GROUP BY annee, mois
)
SELECT
    annee,
    mois,
    ca_total                                                               AS ca_mois_actuel,
    LAG(ca_total) OVER (ORDER BY annee, mois)                             AS ca_mois_precedent,
    ROUND(
        (ca_total - LAG(ca_total) OVER (ORDER BY annee, mois))
        / NULLIF(LAG(ca_total) OVER (ORDER BY annee, mois), 0) * 100
    , 2)                                                                   AS evolution_pct
FROM ca_mensuel
ORDER BY annee DESC, mois DESC;

-- KPI 2 : Panier moyen par segment client
CREATE OR REPLACE VIEW reporting_mexora.v_segment_client_kpi AS
SELECT
    c.segment_client,
    COUNT(DISTINCT f.id_vente)                                            AS nb_commandes,
    ROUND(SUM(f.montant_ttc) / NULLIF(COUNT(DISTINCT f.id_vente), 0)::decimal, 2)
                                                                           AS panier_moyen,
    ROUND(SUM(f.montant_ttc)::numeric, 2)                                 AS ca_total,
    ROUND(SUM(f.montant_ttc) * 100.0 / SUM(SUM(f.montant_ttc)) OVER (), 2)
                                                                           AS pct_ca_total
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_client  c ON f.id_client = c.id_client_sk
WHERE f.statut_commande = 'livré'
  AND c.est_actif = TRUE
GROUP BY c.segment_client
ORDER BY panier_moyen DESC;

-- KPI 3 : Taux de retour par catégorie
CREATE OR REPLACE VIEW reporting_mexora.v_taux_retour AS
SELECT
    p.categorie,
    COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') AS nb_retours,
    COUNT(*)                                                AS nb_total,
    ROUND(
        COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
        / NULLIF(COUNT(*), 0)
    , 2)                                                   AS taux_retour_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
             / NULLIF(COUNT(*), 0) > 5  THEN 'ALERTE_ROUGE'
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
             / NULLIF(COUNT(*), 0) > 3  THEN 'ALERTE_ORANGE'
        ELSE 'OK'
    END                                                    AS seuil_alerte
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
GROUP BY p.categorie
ORDER BY taux_retour_pct DESC;

-- KPI 4 : Effet Ramadan sur l'alimentation
CREATE OR REPLACE VIEW reporting_mexora.v_effet_ramadan AS
SELECT
    periode_ramadan,
    ROUND(AVG(ca_ttc)::numeric, 2)  AS ca_moyen_journalier,
    SUM(volume_vendu)               AS volume_total,
    COUNT(*)                        AS nb_jours
FROM reporting_mexora.mv_ca_mensuel
WHERE categorie = 'Alimentation'
  AND annee >= 2022
GROUP BY periode_ramadan;

-- ============================================================
-- COMMANDES DE RAFRAÎCHISSEMENT
-- ============================================================
-- À exécuter après chaque run ETL complet :
-- REFRESH MATERIALIZED VIEW CONCURRENTLY reporting_mexora.mv_ca_mensuel;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY reporting_mexora.mv_top_produits;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY reporting_mexora.mv_performance_livreurs;
