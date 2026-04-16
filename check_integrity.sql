-- ============================================================
-- check_integrity.sql — Vérification de l'intégrité référentielle
-- Mexora Analytics Data Warehouse
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. Orphelins dans fait_ventes (FK non résolues)
-- ──────────────────────────────────────────────────────────────
SELECT 'Orphelins dim_temps' AS check_name,
       COUNT(*) AS nb_problemes
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
WHERE t.id_date IS NULL

UNION ALL

SELECT 'Orphelins dim_produit',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE p.id_produit_sk IS NULL

UNION ALL

SELECT 'Orphelins dim_client',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_client c ON f.id_client = c.id_client_sk
WHERE c.id_client_sk IS NULL

UNION ALL

SELECT 'Orphelins dim_region',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_region r ON f.id_region = r.id_region
WHERE r.id_region IS NULL;

-- ──────────────────────────────────────────────────────────────
-- 2. Comptes par table
-- ──────────────────────────────────────────────────────────────
SELECT 'dim_temps'    AS table_name, COUNT(*) AS nb_lignes FROM dwh_mexora.dim_temps    UNION ALL
SELECT 'dim_produit',                COUNT(*)               FROM dwh_mexora.dim_produit  UNION ALL
SELECT 'dim_client',                 COUNT(*)               FROM dwh_mexora.dim_client   UNION ALL
SELECT 'dim_region',                 COUNT(*)               FROM dwh_mexora.dim_region   UNION ALL
SELECT 'dim_livreur',                COUNT(*)               FROM dwh_mexora.dim_livreur  UNION ALL
SELECT 'fait_ventes',                COUNT(*)               FROM dwh_mexora.fait_ventes;

-- ──────────────────────────────────────────────────────────────
-- 3. Qualité des données dans fait_ventes
-- ──────────────────────────────────────────────────────────────
SELECT
    statut_commande,
    COUNT(*)                          AS nb,
    ROUND(SUM(montant_ttc)::numeric, 2) AS ca_ttc,
    ROUND(AVG(quantite_vendue)::numeric, 1) AS qte_moy
FROM dwh_mexora.fait_ventes
GROUP BY statut_commande
ORDER BY nb DESC;

-- ──────────────────────────────────────────────────────────────
-- 4. Doublons résiduels dans les dimensions
-- ──────────────────────────────────────────────────────────────
SELECT 'Doublons dim_produit (nk)' AS check_name,
       COUNT(*) - COUNT(DISTINCT id_produit_nk) AS nb_doublons
FROM dwh_mexora.dim_produit WHERE est_actif = TRUE

UNION ALL

SELECT 'Doublons dim_client (nk)',
       COUNT(*) - COUNT(DISTINCT id_client_nk)
FROM dwh_mexora.dim_client WHERE est_actif = TRUE;

-- ──────────────────────────────────────────────────────────────
-- 5. Cohérence montants HT / TTC
-- ──────────────────────────────────────────────────────────────
SELECT COUNT(*) AS nb_incoherences_ttc
FROM dwh_mexora.fait_ventes
WHERE ABS(montant_ttc - montant_ht * 1.20) > 0.01;

-- ──────────────────────────────────────────────────────────────
-- 6. KPI synthèse pour validation
-- ──────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                            AS total_transactions,
    COUNT(DISTINCT id_client)                           AS clients_uniques,
    COUNT(DISTINCT id_produit)                          AS produits_uniques,
    ROUND(SUM(montant_ttc)::numeric, 2)                AS ca_ttc_total,
    ROUND(AVG(montant_ttc)::numeric, 2)                AS panier_moyen,
    MIN(id_date)                                        AS date_min,
    MAX(id_date)                                        AS date_max
FROM dwh_mexora.fait_ventes
WHERE statut_commande = 'livré';
