# Backtest APEX-BSM — bsm-20260928T153332Z

- Version du modèle : `apex-bsm-1.0.0`
- Heure de prévision simulée : lundi précédant la semaine du match (00:00), cotes relevées avant match
- Ligues : E0, E1, E2, E3, SP1, I1, D1, F1
- Apprentissage (historique seul) : 2122, 2223 · Validation (réglage) : 2324, 2425 · **Test final isolé : 2526**
- Paramètres gelés après validation : {'xi': 0.0015, 'K': 6.0, 'rho': -0.05, 'sigma': 0.0}
- **Statut : NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**

## Couverture des données

| Ligue | Saison | Matchs | Cotes 1X2 avant match | Source | Cotes O/U 2,5 |
|---|---|---|---|---|---|
| E0 | 2122 | 380 | 380 | pinnacle | 380 |
| E0 | 2223 | 380 | 380 | pinnacle | 380 |
| E0 | 2324 | 380 | 380 | pinnacle | 380 |
| E0 | 2425 | 380 | 380 | pinnacle | 380 |
| E0 | 2526 | 380 | 380 | moyenne, pinnacle | 380 |
| E1 | 2122 | 552 | 552 | pinnacle | 552 |
| E1 | 2223 | 552 | 552 | pinnacle | 552 |
| E1 | 2324 | 552 | 552 | pinnacle | 552 |
| E1 | 2425 | 552 | 552 | pinnacle | 552 |
| E1 | 2526 | 552 | 552 | moyenne, pinnacle | 552 |
| E2 | 2122 | 552 | 552 | pinnacle | 552 |
| E2 | 2223 | 552 | 552 | pinnacle | 552 |
| E2 | 2324 | 552 | 552 | pinnacle | 552 |
| E2 | 2425 | 552 | 552 | pinnacle | 552 |
| E2 | 2526 | 552 | 552 | moyenne, pinnacle | 552 |
| E3 | 2122 | 552 | 552 | pinnacle | 552 |
| E3 | 2223 | 552 | 551 | pinnacle | 551 |
| E3 | 2324 | 552 | 552 | pinnacle | 552 |
| E3 | 2425 | 552 | 552 | moyenne, pinnacle | 552 |
| E3 | 2526 | 552 | 552 | moyenne, pinnacle | 552 |
| SP1 | 2122 | 380 | 380 | moyenne, pinnacle | 380 |
| SP1 | 2223 | 380 | 380 | pinnacle | 380 |
| SP1 | 2324 | 380 | 380 | pinnacle | 380 |
| SP1 | 2425 | 380 | 380 | pinnacle | 380 |
| SP1 | 2526 | 380 | 380 | moyenne, pinnacle | 380 |
| I1 | 2122 | 380 | 379 | pinnacle | 379 |
| I1 | 2223 | 380 | 380 | pinnacle | 380 |
| I1 | 2324 | 380 | 380 | pinnacle | 380 |
| I1 | 2425 | 380 | 380 | pinnacle | 380 |
| I1 | 2526 | 380 | 380 | moyenne, pinnacle | 380 |
| D1 | 2122 | 306 | 306 | pinnacle | 306 |
| D1 | 2223 | 306 | 306 | pinnacle | 306 |
| D1 | 2324 | 306 | 306 | pinnacle | 306 |
| D1 | 2425 | 306 | 306 | pinnacle | 306 |
| D1 | 2526 | 306 | 306 | moyenne, pinnacle | 306 |
| F1 | 2122 | 380 | 380 | pinnacle | 380 |
| F1 | 2223 | 380 | 380 | pinnacle | 380 |
| F1 | 2324 | 306 | 306 | pinnacle | 306 |
| F1 | 2425 | 306 | 306 | pinnacle | 306 |
| F1 | 2526 | 306 | 306 | moyenne, pinnacle | 306 |

## Test final — comparaison aux références (même échantillon, même instant)

| Modèle | n | Brier 1X2 | Log-loss 1X2 | RPS | n O/U | Brier O/U 2,5 | Log-loss O/U 2,5 |
|---|---|---|---|---|---|---|---|
| APEX-BSM (gelé) | 3279 | 0.6152 | 1.0262 | 0.2126 | 3279 | 0.2509 | 0.6952 |
| Skill actuel (legacy) | 3279 | 0.6235 | 1.038 | 0.2163 | 3279 | 0.257 | 0.7085 |
| Modèle simple de buts | 3279 | 0.6508 | 1.0753 | 0.2301 | 3279 | 0.2491 | 0.6914 |
| Marché démarginé (avant match) | 3279 | 0.6013 | 1.0048 | 0.206 | 3279 | 0.2449 | 0.6827 |

Écart de log-loss 1X2 (APEX-BSM − référence ; négatif = APEX-BSM meilleur), IC 95 % bootstrap par match :

- vs_marche : +0.0214  IC95 [0.016, 0.0271]
- vs_legacy : -0.0118  IC95 [-0.0187, -0.0051]
- vs_simple : -0.0491  IC95 [-0.0595, -0.039]

### Stabilité par ligue (log-loss 1X2 vs marché)

| Ligue | n | Δ | IC95 |
|---|---|---|---|
| E0 | 361 | +0.0098 | [-0.0073, 0.0263] |
| E1 | 540 | +0.0266 | [0.0143, 0.0392] |
| E2 | 540 | +0.0216 | [0.0055, 0.0386] |
| E3 | 540 | +0.0212 | [0.0061, 0.0357] |
| SP1 | 362 | +0.0184 | [0.0032, 0.0337] |
| I1 | 360 | +0.0215 | [0.0037, 0.04] |
| D1 | 288 | +0.0218 | [0.0028, 0.0407] |
| F1 | 288 | +0.0292 | [0.0122, 0.0461] |

### BTTS : Brier 0.2481, log-loss 0.6895 (n=3408) — aucune cote BTTS historique dans la source : pas de comparaison marché

### Buts : prédits 2.677 / observés 2.687 par match ; log-loss du score exact 2.8876

### Calibration (test final)

**1X2 (3 issues regroupées)**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.0-0.1 | 89 | 0.079 | 0.067 |
| 0.1-0.2 | 815 | 0.164 | 0.171 |
| 0.2-0.3 | 4374 | 0.257 | 0.254 |
| 0.3-0.4 | 2309 | 0.343 | 0.338 |
| 0.4-0.5 | 1431 | 0.447 | 0.448 |
| 0.5-0.6 | 784 | 0.545 | 0.547 |
| 0.6-0.7 | 313 | 0.638 | 0.681 |
| 0.7-0.8 | 89 | 0.746 | 0.787 |
| 0.8-0.9 | 19 | 0.833 | 0.895 |
| 0.9-1.0 | 1 | 0.914 | 0.0 |

**Over 2.5**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.2-0.3 | 70 | 0.272 | 0.429 |
| 0.3-0.4 | 558 | 0.361 | 0.466 |
| 0.4-0.5 | 1183 | 0.453 | 0.491 |
| 0.5-0.6 | 1062 | 0.546 | 0.535 |
| 0.6-0.7 | 449 | 0.638 | 0.568 |
| 0.7-0.8 | 75 | 0.733 | 0.693 |
| 0.8-0.9 | 11 | 0.83 | 0.909 |

**BTTS**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.2-0.3 | 2 | 0.288 | 0.5 |
| 0.3-0.4 | 207 | 0.373 | 0.425 |
| 0.4-0.5 | 1172 | 0.459 | 0.51 |
| 0.5-0.6 | 1518 | 0.546 | 0.558 |
| 0.6-0.7 | 489 | 0.633 | 0.597 |
| 0.7-0.8 | 20 | 0.718 | 0.6 |

### Paris simulés (règles fixées avant le test)

Règles : {'markets': ['1X2', 'OU2.5'], 'ev_min': 0.03, 'one_market_per_match': True, 'stake_units': 1.0, 'price': 'cote relevée avant match (Pinnacle PSH/PSD/PSA, P>2.5/P<2.5 ; à défaut moyenne marché)'}

- Paris : 2813 · taux de réussite : 0.321 · rendement net/unité : -0.1413 (IC95 [-0.1921, -0.0877]) · P&L : -397.51 u · perte max cumulée : -427.32 u · CLV moyen 1X2 : -0.0562
- Par marché : {'1X2': {'n': 1944, 'rendement': -0.1746, 'reussite': 0.2479}, 'OU2.5': {'n': 869, 'rendement': -0.0668, 'reussite': 0.4845}}

*Qualité prédictive (log-loss), taux de réussite et rentabilité sont trois choses distinctes. Ces paris sont simulés : aucune mise n'a été exécutée.*

## Variantes essayées (validation uniquement)

256 variantes. Les 10 meilleures :

| xi | K | rho | sigma | n | LL 1X2 | LL O/U | LL score | objectif |
|---|---|---|---|---|---|---|---|---|
| 0.0015 | 6.0 | -0.05 | 0.1 | 6816 | 1.0140 | 0.6842 | 2.9151 | 1.6982 |
| 0.0015 | 6.0 | -0.05 | 0.0 | 6816 | 1.0140 | 0.6842 | 2.9146 | 1.6982 |
| 0.003 | 6.0 | -0.05 | 0.1 | 6816 | 1.0137 | 0.6845 | 2.9147 | 1.6982 |
| 0.003 | 6.0 | -0.05 | 0.2 | 6816 | 1.0138 | 0.6845 | 2.9176 | 1.6983 |
| 0.003 | 6.0 | -0.05 | 0.0 | 6816 | 1.0137 | 0.6846 | 2.9142 | 1.6983 |
| 0.0015 | 6.0 | -0.05 | 0.2 | 6816 | 1.0141 | 0.6842 | 2.9181 | 1.6983 |
| 0.003 | 6.0 | 0.0 | 0.2 | 6816 | 1.0139 | 0.6845 | 2.9177 | 1.6984 |
| 0.0015 | 6.0 | 0.0 | 0.2 | 6816 | 1.0142 | 0.6842 | 2.9182 | 1.6984 |
| 0.0015 | 6.0 | 0.0 | 0.1 | 6816 | 1.0143 | 0.6842 | 2.9153 | 1.6985 |
| 0.0015 | 6.0 | -0.1 | 0.0 | 6816 | 1.0143 | 0.6842 | 2.9159 | 1.6985 |

Apport du rythme partagé (validation) : meilleur objectif sans rythme 1.6982 · retenu 1.6982.
Le rythme partagé n'est adopté que s'il améliore l'objectif de validation d'au moins 0.0005 ; sinon le modèle simple (σ=0) est gelé.
