---
name: apex-backtest-simulation
description: Module OBLIGATOIRE de toute analyse football APEX (pronostic, coupon, value, scan de journée, match isolé) — backtesting chronologique walk-forward, calibration, simulation Monte-Carlo du match complet et dérivation cohérente des marchés, puis sélection à EV ≥ 3 % et journal append-only. À exécuter AVANT de publier la moindre probabilité ou sélection football, en plus du moteur de ligue apex-engine-* et de la chaîne S1-S8. Outil mécanique : tools/apex_bsm.py (backtest, simulate, settle, audit). Ne s'applique pas au hockey ni au turf.
---

# APEX-BSM — Backtesting, calibration et simulation des matchs

Ce module est **obligatoire pour toute analyse football**. Aucune probabilité, aucun score, aucune sélection n'est publiée sans :

1. un statut de validation issu d'un backtest chronologique **réellement exécuté** ;
2. une simulation du match dont le nombre d'itérations est indiqué ;
3. un enregistrement au journal avant le coup d'envoi.

Outil : `tools/apex_bsm.py`. Il faut `numpy` et `scipy` (`pip install numpy scipy` si nécessaire).

> **Règle d'or** : n'invente jamais un backtest, une cote historique ou une simulation. Si les données ou le calcul manquent, écris précisément ce qui manque, et sépare le protocole proposé des résultats réellement obtenus.

## Statut actuel (run `bsm-20260928T153332Z`, détail dans `backtests/README.md`)

- **Face au skill actuel et au modèle simple** : APEX-BSM est meilleur en 1X2.
- **Face au modèle simple en Over/Under 2,5** : APEX-BSM est moins bon.
- **Face au marché démarginé** : APEX-BSM est **significativement moins bon** (Δ log-loss +0,021). Les paris à EV ≥ 3 % perdent −14 % par unité sur le test final.
- ⇒ Statut **NON SUPÉRIEUR AU MARCHÉ**. Par défaut, toute EV contre une cote est vetoée : **surveillance ou abstention**.
- Ce veto est global. On ne le lève pas pour une seule ligue « favorable » : ce serait une sélection a posteriori.
- Il ne sera levé que par une nouvelle version validée sur un test vierge (saison 2026-27).

---

## Étape 1 — Historique pertinent et vérifiable

- **Source par défaut** : football-data.co.uk (`data/history/<div>_<saison>.csv`, mis en cache). Elle fournit les buts, et les cotes Pinnacle **relevées avant le match** : `PSH/PSD/PSA` et `P>2.5`, relevées le vendredi pour le week-end et le mardi pour le milieu de semaine. Elle fournit aussi les cotes de clôture (`PSCH…`), mais celles-ci ne servent qu'à mesurer le CLV.
- **Pas de xG, de compositions, de repos ni de BTTS dans cette source.** Si on les intègre, il faut citer une autre source, horodatée, et documenter les différences de définition entre fournisseurs (un xG FBref n'est pas un xG Understat).
- **Ce que la source couvre** : 22 divisions, dont E0-E3 (Premier League à League Two), SP1/SP2, I1/I2, D1/D2, F1/F2, N1, B1, P1, T1, G1 et SC0-SC3. Les sélections nationales, la MLS et les coupes ne sont **pas couvertes** : pour elles, le statut est « NON VALIDÉ, hors périmètre » (voir l'étape 7).
- **La forme récente ne valide rien.** Les derniers matchs mesurent la forme, pas la fiabilité du modèle. La validation vient du backtest multi-saisons.
- **Chaque rapport documente** : les sources, la couverture (matchs, part des matchs avec cotes), les données manquantes et les divergences de définition.

## Étape 2 — Backtest avant tout usage

```bash
python3 tools/apex_bsm.py backtest --divs E0,E1,E2,E3,SP1,I1,D1,F1 --seasons 2122,2223,2324,2425,2526 --n-train 2 --n-val 2
```

- **Heure de prévision fixe** : le lundi 00:00 de la semaine du match. Les forces sont réestimées avec les seuls matchs antérieurs, et les cotes utilisées sont celles d'avant-match. Les compositions définitives ne sont jamais utilisées.
- **Découpage chronologique** en trois blocs :
  - **apprentissage** : historique seul ;
  - **validation** : réglage de ξ (décroissance), K (rétrécissement), ρ (Dixon-Coles) et σ (rythme partagé) ;
  - **test final** : paramètres gelés, jamais réutilisés pour un réglage.
- **Fenêtres hebdomadaires glissantes** : apprendre sur le passé, prévoir la semaine, avancer. Tous les marchés d'un match restent dans le même enregistrement, et le bootstrap se fait par match.
- **Informations interdites** : statistiques du match prédit, classement final, blessures apprises plus tard, cotes postérieures à l'heure retenue.
- **Traçabilité** : chaque variante de la grille est publiée (`variantes_essayees`), et toute modification de la grille est documentée dans le code avec sa raison.
- **Sorties** : `backtests/<run_id>/REPORT.md`, `report.json`, `bets.json`, et `backtests/latest_params.json` (paramètres gelés et statut par ligue).

**Relancer un backtest** au début de chaque nouvelle saison, et après toute modification du modèle. Une nouvelle version ne remplace l'ancienne que si elle fait mieux **sur un test chronologique qu'elle n'a pas servi à régler**.

## Étape 3 — Mesures et références

Le rapport de backtest publie :

- **1X2** : Brier multiclasses, log-loss et RPS.
- **Over/Under 2,5 et BTTS** : Brier et log-loss binaires. Il n'y a pas de cote BTTS historique : BTTS est évalué contre les résultats seulement, sans comparaison au marché.
- **Calibration** : fréquence réellement observée par tranche de probabilité, avec les effectifs.
- **Distribution des buts** : buts prédits contre observés, et log-loss du score exact.
- **Stabilité par ligue** : Δ log-loss contre le marché, avec IC 95 % bootstrap.
- **Trois références sur le même échantillon et au même instant** :
  - le skill actuel (legacy, `analyses/tools/final.py`) ;
  - un modèle simple de buts (moyennes de la ligue) ;
  - le marché démarginé.
- **Paris simulés**, avec des règles fixées **avant** le test (`BET_RULES` : EV ≥ 3 %, un marché maximum par match, mise de 1 unité, cote d'avant-match). On publie : rendement net avec IC 95 %, perte maximale cumulée, résultats par marché, taux de réussite et CLV.
- **Trois choses distinctes à ne pas confondre** : la qualité prédictive, le taux de réussite et la rentabilité.
- **Pas de sélection a posteriori** : on ne retient jamais après coup les seules ligues, marchés ou périodes favorables.

**Statut de validation** (écrit dans `latest_params.json`) :

| Statut | Condition | Conséquence |
|---|---|---|
| `VALIDÉ` | IC95 du Δ log-loss contre le marché entièrement < 0 | EV exploitable sous les autres conditions |
| `NON CONCLUANT` | IC95 contient 0 | EV à interpréter prudemment ; sélection possible si elle est stable en sensibilité |
| `NON SUPÉRIEUR AU MARCHÉ` | IC95 entièrement > 0 | **Veto** : une EV contre le marché est présumée illusoire → surveillance ou abstention |

## Étape 4 — Forces des deux équipes

- **Modèle de base** (validé) : Poisson log-linéaire (attaque, défense, avantage du terrain estimé), pondéré dans le temps (ξ), rétréci vers la moyenne (K), sur 730 jours. L'adversaire est donc pris en compte : la forme est corrigée du niveau des adversaires rencontrés.
- **Pas de double comptage** : ne pas ajouter indépendamment buts, xG et tirs. Un signal supplémentaire (xG, par exemple) n'entre qu'en **remplacement ou en mélange estimé et backtesté**, jamais en surcouche.
- **Absences, fatigue, rotation, contexte** : sans coefficient estimé historiquement, ces facteurs sont des **corrections subjectives**. Elles s'expriment uniquement via `--scenario proba:mult_dom:mult_ext`, sont étiquetées comme telles et sont soumises à la sensibilité (étape 5).
- **Pièges de raisonnement** :
  - une série de résultats n'est pas une cause tactique démontrée ;
  - une « obligation de gagner » ne garantit ni la victoire ni un match riche en buts.

## Étape 5 — Simulation du match complet

```bash
python3 tools/apex_bsm.py simulate --div E0 --home Arsenal --away Chelsea --asof 2026-10-03 \
  --kickoff 2026-10-04T16:30Z --odds-1x2 1.95,3.60,4.10 --odds-ou25 1.90,1.95 \
  --odds-ah dom:-0.5:1.96 --odds-dnb 1:1.45 --odds-source "Pinnacle" --odds-time 2026-10-03T18:00Z --record
```

- **Périmètre** : 90 minutes plus le temps additionnel. Pas de prolongation ni de tirs au but.
- **Scénario partagé** : chaque simulation tire un scénario commun aux deux équipes. Ce scénario comprend :
  - un rythme `g ~ Gamma(1, σ)`, avec σ estimé en validation (σ = 0 si l'apport n'est pas démontré) ;
  - une configuration de composition (scénarios subjectifs éventuels).

  Le score est ensuite tiré dans la loi Dixon-Coles correspondante.
- **Événements (expulsions, penalties, ouverture du score)** : ils sont **déjà inclus** dans les taux de buts historiques. Le module ne les ajoute pas en surcouche, pour ne pas les compter deux fois. Une couche dédiée n'est admissible qu'avec des fréquences et des effets estimés et backtestés.
- **Précision définie à l'avance** : départ à 10 000 simulations, doublées jusqu'à ce que la demi-largeur de l'IC 95 % Monte-Carlo soit ≤ 0,5 point sur 1X2, Over 1,5/2,5/3,5 et BTTS (plafond : 640 000). **Le nombre exécuté est toujours affiché.**
- **Deux incertitudes distinctes** :
  - l'erreur Monte-Carlo (demi-largeur affichée) ;
  - la sensibilité aux hypothèses : λ ±10 %, avec ou sans scénarios de composition. **Une sensibilité n'est pas un intervalle de confiance.**

## Étape 6 — Marchés déduits des mêmes simulations

Tous les marchés sont calculés à partir des scores simulés :

- 1X2, double chance, DNB ;
- Over/Under de 0,5 à 5,5, totaux asiatiques (1,75 à 3,25) ;
- BTTS, buts par équipe ;
- handicaps asiatiques de −3 à +3 par quarts de but ;
- dix scores exacts.

**Contrôles automatiques** : 1X2 = 100 %, et les Over sont décroissants.

**DNB et handicaps asiatiques** : l'outil sépare cinq états de règlement — gain, demi-gain, remboursement, demi-perte, perte :

- EV générale = P(gain)(c−1) + P(demi-gain)(c−1)/2 − P(demi-perte)/2 − P(perte)
- EV d'un DNB = P(victoire)(c−1) − P(défaite)

**Interdit de déduire des scores** : corners, cartons, tirs et buteurs. Ces marchés demandent des modèles spécifiques et les minutes probables des joueurs.

**Combinés** :

- dans un **même match**, la probabilité jointe se calcule sur les mêmes simulations, jamais par produit ;
- entre matchs différents, le produit suppose l'indépendance, et cette hypothèse doit être dite.

## Étape 7 — Présentation et sélection

**Ordre obligatoire de la restitution** :

1. **Bilan du backtest** : période, volume, méthode, références, résultats, limites et statut. On reprend `backtests/<run_id>/REPORT.md`, et on n'invente jamais un chiffre absent.
2. **Pour chaque match** :
   - les λ des deux équipes ;
   - 1X2, BTTS et les lignes de buts principales ;
   - les trois scores les plus probables avec leur probabilité ;
   - les scénarios favorables et défavorables ;
   - la sensibilité ;
   - la qualité et la fraîcheur des données (date `asof`, source et heure des cotes).
   Le score le plus probable n'est pas la moyenne des buts, et il n'est jamais un pari par défaut.
3. **Sélection** : au plus **un marché officiel** par match. Les autres marchés sont des alternatives conditionnelles.

**Conditions cumulatives pour un marché officiel** :

- une cote vérifiée et horodatée ;
- des données suffisantes ;
- aucun veto ;
- une EV ≥ 3 % ;
- une EV qui reste ≥ 0 dans tous les cas de sensibilité.

Le seuil de 3 % est un filtre de décision, pas une preuve de rentabilité.

**Surveillance ou abstention** dans quatre cas :

- une composition déterminante manque (`--missing-lineup`) ;
- l'avantage disparaît en sensibilité ;
- le statut est `NON SUPÉRIEUR AU MARCHÉ` ;
- le match est hors périmètre du backtest (sélections, MLS, coupes). Pour ces matchs, λ vient d'une source externe (Elo, marché) via `--lh/--la` : on les présente comme **indicatifs, NON VALIDÉS**.

## Étape 8 — Journal et audit sans réécrire le passé

- **Avant le coup d'envoi**, `simulate --record` ajoute une ligne à `ledger/forecasts.jsonl`. Elle contient : l'heure, la version du modèle, les paramètres, la source des λ, le statut, les probabilités, les cotes (source et heure), les EV, la sensibilité, la décision et `statut_mise`.
- **Trois types de mises à ne jamais confondre** : `PROPOSÉE` (décision du modèle), simulée (backtest) et exécutée (seulement si `settle --executed-stake/--executed-odds` est renseigné). Une cote consultée ne prouve pas qu'un pari a été placé.
- **Après le match** : `python3 tools/apex_bsm.py settle --forecast-id <id> --score 2-1 --note "occasions, expulsions…"`. Les reports, annulations et exclusions se déclarent via `--status`. Le journal est **append-only** : rien n'est effacé.
- **Pour confronter prévisions et résultats**, abstentions comprises : `python3 tools/apex_bsm.py audit`.
- **Tirer les leçons avec mesure** : une défaite isolée ne justifie pas une modification générale, et une victoire isolée ne valide pas le modèle. Toute correction est testée chronologiquement (nouveau `backtest`) avant de remplacer la version précédente (`MODEL_VERSION`).

---

## Intégration dans la chaîne APEX

| Maillon | Obligation BSM |
|---|---|
| `apex-quant` (S4) | Lit `backtests/latest_params.json` et exécute `simulate` : les probabilités publiées sont celles des simulations, avec n et statut. Les règles du moteur `apex-engine-*` qui ne sont pas estimées deviennent des scénarios subjectifs. |
| `apex-market-analyst` (S6) | Fournit les cotes vérifiées et horodatées (source et heure) pour `--odds-*`. |
| `apex-decision-maker` (S7) | Applique l'étape 7 : un marché maximum, EV ≥ 3 %, stable en sensibilité, aucun veto BSM. |
| Synthèse | Commence par le bilan du backtest ; chaque sélection porte son `forecast_id`. |

Pour une analyse rapide hors chaîne (coupon, scan de journée), les étapes 1 à 8 s'appliquent quand même. Seul le découpage en agents est facultatif.
