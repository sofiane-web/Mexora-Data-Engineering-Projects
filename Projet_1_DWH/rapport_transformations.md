# Rapport des transformations ETL — Mexora Analytics

Généré le : 2026-04-16 · Pipeline v1.0

---

## 1. Résumé exécutif

| Indicateur | Valeur |
|------------|-------:|
| Lignes extraites (commandes) | 5 150 |
| Lignes après nettoyage | 4 908 |
| Lignes chargées (fait_ventes) | 4 675 |
| CA TTC total (livré) | ~130 M MAD |
| Durée totale pipeline | 0.6 s |

---

## 2. Transformations — Commandes

### R1 — Suppression des doublons sur `id_commande`

**Règle métier :** En cas de doublon, conserver la dernière occurrence
(la ré-insertion est supposée être la version la plus récente du système source).

**Code appliqué :**
```python
df = df.drop_duplicates(subset=["id_commande"], keep="last")
```

**Lignes affectées :** 150 doublons supprimés (5 150 → 5 000 lignes, −2,9%)

---

### R2 — Standardisation des dates

**Règle métier :** Toutes les dates doivent être au format `YYYY-MM-DD`.
Les formats source sont mixtes : `DD/MM/YYYY`, `YYYY-MM-DD`, `Mon DD YYYY`.

**Code appliqué :**
```python
df["date_commande"] = pd.to_datetime(
    df["date_commande"], format="mixed", dayfirst=True, errors="coerce"
)
df = df.dropna(subset=["date_commande"])
```

**Lignes affectées :** 0 date invalide (toutes les dates ont été parsées avec succès)

---

### R3 — Harmonisation des villes via le référentiel

**Règle métier :** Les variantes orthographiques (`tanger / TNG / TANJA / Tanger`)
sont normalisées vers le nom standard du référentiel `regions_maroc.csv`.
Les villes non trouvées → `'Non renseignée'`.

**Code appliqué :**
```python
ville_brute = df["ville_livraison"].str.strip().str.lower()
df["ville_livraison"] = ville_brute.map(mapping_villes).fillna("Non renseignée")
```

**Lignes affectées :** 0 ville non trouvée dans le référentiel (mapping exhaustif)

---

### R4 — Standardisation des statuts

**Règle métier :** Les valeurs non-standards (`OK`, `KO`, `DONE`, `livre`, etc.)
sont mappées vers les 4 valeurs cibles : `livré | annulé | en_cours | retourné`.

**Code appliqué :**
```python
df["statut"] = df["statut"].map(MAPPING_STATUTS)
df["statut"] = df["statut"].where(df["statut"].isin(STATUTS_VALIDES), "inconnu")
```

**Lignes affectées :** 2 520 statuts remappés, 0 → `'inconnu'`

---

### R5 — Suppression des quantités invalides (≤ 0)

**Règle métier :** Une commande doit avoir au moins 1 unité. Les quantités
négatives sont des erreurs de saisie documentées.

**Code appliqué :**
```python
df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
df = df[df["quantite"].notna() & (df["quantite"] > 0)]
```

**Lignes affectées :** 49 lignes supprimées (5 000 → 4 951, −1,0%)

---

### R6 — Suppression des prix nuls (commandes test)

**Règle métier :** Les commandes à `prix_unitaire = 0` sont des commandes test
à exclure de l'analyse.

**Code appliqué :**
```python
df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
df = df[df["prix_unitaire"].notna() & (df["prix_unitaire"] > 0)]
```

**Lignes affectées :** 43 commandes test supprimées (4 951 → 4 908, −0,9%)

---

### R7 — Livreurs manquants

**Règle métier :** Un `id_livreur` manquant correspond souvent à un retrait
en point relais ou une erreur de saisie. On ne supprime pas la commande
mais on assigne la valeur sentinelle `-1` (livreur inconnu).

**Code appliqué :**
```python
df["id_livreur"] = df["id_livreur"].fillna("-1")
```

**Lignes affectées :** 355 valeurs remplacées par `-1` (~7% des lignes)

---

### R8 — Calcul des montants HT et TTC

**Règle métier :** TVA Maroc = 20%.
```
montant_ht  = quantite × prix_unitaire
montant_ttc = montant_ht × 1.20
```

**Code appliqué :**
```python
df["montant_ht"]  = (df["quantite"] * df["prix_unitaire"]).round(2)
df["montant_ttc"] = (df["montant_ht"] * 1.20).round(2)
```

**CA TTC total brut :** 138 110 522,93 MAD

---

### R9 — Calcul du délai de livraison

**Règle métier :** Délai = `date_livraison - date_commande` en jours.
Les délais négatifs (incohérence de données) sont neutralisés → NULL.

**Code appliqué :**
```python
df["delai_livraison_jours"] = (df["date_livraison"] - df["date_commande"]).dt.days
df.loc[df["delai_livraison_jours"] < 0, "delai_livraison_jours"] = pd.NA
```

**Lignes affectées :** 675 délais négatifs neutralisés · Délai moyen : 3,6 jours

---

## 3. Transformations — Clients

### R1 — Déduplication sur email normalisé

**Règle métier :** Même email + id_client différent = erreur de migration.
On conserve l'inscription la plus récente.

**Lignes affectées :** 63 doublons supprimés (1 260 → 1 197, −5,0%)

---

### R2 — Standardisation du sexe

**Encodages source :** `m/f | 1/0 | Homme/Femme | M/F | male/female | H/h`  
**Cible :** `m | f | inconnu`

**Lignes affectées :** 1 197 mappés, 0 → `'inconnu'`

---

### R3 — Validation des dates de naissance

**Règle :** Âge entre 16 et 100 ans à la date du jour.

**Lignes affectées :** 26 dates de naissance invalidées (→ NaT, ligne conservée)

---

### R4 — Tranche d'âge

**Répartition calculée :**

| Tranche | Clients |
|---------|--------:|
| 55-64 | 267 |
| 25-34 | 263 |
| 45-54 | 258 |
| 35-44 | 245 |
| 18-24 | 102 |
| 65+ | 36 |

---

### R5 — Validation format email

**Lignes affectées :** 55 emails invalidés (→ NULL, ligne conservée)

---

## 4. Transformations — Produits

### R1 — Normalisation des catégories

**Encodages source :** `electronique | ELECTRONIQUE | Electronique | mode | MODE | …`  
**Cible :** `Electronique | Mode | Alimentation`

**Distribution avant :**
```
Electronique: 10 · electronique: 4 · ELECTRONIQUE: 3
Mode: 5 · mode: 3 · MODE: 2
Alimentation: 2 · alimentation: 2 · ALIMENTATION: 1
```
→ **Après : 17 Electronique | 10 Mode | 5 Alimentation**

---

### R2 — Prix catalogue null

**Produits concernés :** P031 (Nokia 3310 2G), P032 (Blackberry Bold 9900)  
**Action :** Prix remplacé par `0.01` (symbolique, produits discontinués)

---

### R4 — SCD Type 2

**3 produits marqués `actif=False`** avec des commandes associées → à gérer en SCD.

---

## 5. Récapitulatif statistique complet

| Règle | Entité | Avant | Après | Supprimées | % |
|-------|--------|------:|------:|----------:|--:|
| R1 — Doublons id_commande | commandes | 5 150 | 5 000 | 150 | 2,9% |
| R2 — Standardisation dates | commandes | 5 000 | 5 000 | 0 | 0,0% |
| R3 — Harmonisation villes | commandes | 5 000 | 5 000 | 0 | 0,0% |
| R4 — Standardisation statuts | commandes | 2 520 remappés | — | — | — |
| R5 — Quantités invalides | commandes | 5 000 | 4 951 | 49 | 1,0% |
| R6 — Prix nuls (test) | commandes | 4 951 | 4 908 | 43 | 0,9% |
| R7 — Livreurs manquants | commandes | 355 remplacés | — | — | — |
| R8 — Montants calculés | commandes | — | — | — | — |
| R9 — Délais calculés | commandes | 675 neutralisés | — | — | — |
| R1 — Doublons email | clients | 1 260 | 1 197 | 63 | 5,0% |
| R2 — Sexe normalisé | clients | 1 197 | — | — | — |
| R3 — Âges invalides | clients | 1 197 | — | 26 inv. | — |
| R5 — Emails invalides | clients | 1 197 | — | 55 null | — |
| R1 — Catégories | produits | 9 variantes | 3 | — | — |
| R2 — Prix null | produits | 2 | — | 0,01 MAD | — |
| **Total fait_ventes** | **faits** | **4 908** | **4 675** | **233** | **4,75%** |
