# Rapport des transformations ETL — Mexora Analytics

Généré le : 2026-04-17 14:25:47

## Récapitulatif des règles appliquées

| Règle | Entité | Avant | Après | Supprimées | % |
|-------|--------|------:|-----:|----------:|---:|
| R1 — Doublons id_commande | commandes | 5150 | 5000 | 150 | 2.91% |
| R2 — Standardisation dates | commandes | 5000 | 5000 | 0 | 0.0% |
| R3 — Harmonisation villes | commandes | 5000 | 5000 | 0 | 0.0% |
| R4 — Standardisation statuts | commandes | 2520 | 2520 | 0 | 0.0% |
| R5 — Quantités invalides (<=0) | commandes | 5000 | 4951 | 49 | 0.98% |
| R6 — Prix nuls (commandes test) | commandes | 4951 | 4908 | 43 | 0.87% |
| R7 — Livreurs manquants | commandes | 355 | 355 | 0 | 0.0% |
| R1 — Doublons email | clients | 1260 | 1197 | 63 | 5.0% |
| R2 — Standardisation sexe | clients | 1197 | 1197 | 0 | 0.0% |
| R3 — Ages invalides (<16 ou >100) | clients | 26 | 26 | 0 | 0.0% |
| R5 — Emails invalides | clients | 55 | 55 | 0 | 0.0% |
| R6 — Harmonisation villes clients | clients | 1197 | 1197 | 0 | 0.0% |
| R1 — Normalisation catégories | produits | 6 | 6 | 0 | 0.0% |
| R2 — Prix catalogue null | produits | 2 | 2 | 0 | 0.0% |
| R4 — Colonnes SCD Type 2 | produits | 32 | 32 | 0 | 0.0% |
| BUILD — FAIT_VENTES | faits | 4908 | 4675 | 233 | 4.75% |