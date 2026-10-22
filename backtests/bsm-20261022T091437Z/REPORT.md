# Backtest APEX-BSM — bsm-20261022T091437Z

- Version du modèle : `apex-bsm-1.0.0`
- Heure de prévision simulée : lundi précédant la semaine du match (00:00), cotes relevées avant match
- Ligues : E0, E1, E2, E3, EC, SP1, I1, D1, F1
- Apprentissage (historique seul) : 2122, 2223 · Validation (réglage) : 2324, 2425 · **Test final isolé : 2526**
- Paramètres gelés après validation : {'xi': 0.003, 'K': 6.0, 'rho': -0.05, 'sigma': 0.0}
- **Statut : NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**

## Couverture des données

| Ligue | Saison | Matchs | Cotes 1X2 avant match | Source | Cotes O/U 2,5 |
|---|---|---|---|---|---|
| E0 | 2122 | 380 | 380 | pinnacle | 380 |
| E0 | 2223 | 380 | 380 | pinnacle | 380 |
| E0 | 2324 | 380 | 380 | pinnacle | 380 |
| E0 | 2425 | 380 | 380 | pinnacle | 380 |
| E0 | 2526 | 380 | 380 | moyenne | 380 |
| E1 | 2122 | 552 | 552 | pinnacle | 552 |
| E1 | 2223 | 552 | 552 | pinnacle | 552 |
| E1 | 2324 | 552 | 552 | pinnacle | 552 |
| E1 | 2425 | 552 | 552 | pinnacle | 552 |
| E1 | 2526 | 552 | 552 | moyenne | 552 |
| E2 | 2122 | 552 | 552 | pinnacle | 552 |
| E2 | 2223 | 552 | 552 | pinnacle | 552 |
| E2 | 2324 | 552 | 552 | pinnacle | 552 |
| E2 | 2425 | 552 | 552 | pinnacle | 552 |
| E2 | 2526 | 552 | 552 | moyenne | 552 |
| E3 | 2122 | 552 | 552 | pinnacle | 552 |
| E3 | 2223 | 552 | 551 | pinnacle | 551 |
| E3 | 2324 | 552 | 552 | pinnacle | 552 |
| E3 | 2425 | 552 | 552 | moyenne, pinnacle | 552 |
| E3 | 2526 | 552 | 552 | moyenne | 552 |
| EC | 2122 | 506 | 506 | moyenne, pinnacle | 506 |
| EC | 2223 | 552 | 552 | pinnacle | 552 |
| EC | 2324 | 552 | 551 | moyenne, pinnacle | 551 |
| EC | 2425 | 552 | 552 | pinnacle | 552 |
| EC | 2526 | 552 | 540 | moyenne | 540 |
| SP1 | 2122 | 380 | 380 | moyenne, pinnacle | 380 |
| SP1 | 2223 | 380 | 380 | pinnacle | 380 |
| SP1 | 2324 | 380 | 380 | pinnacle | 380 |
| SP1 | 2425 | 380 | 380 | pinnacle | 380 |
| SP1 | 2526 | 380 | 380 | moyenne | 380 |
| I1 | 2122 | 380 | 379 | pinnacle | 379 |
| I1 | 2223 | 380 | 380 | pinnacle | 380 |
| I1 | 2324 | 380 | 380 | pinnacle | 380 |
| I1 | 2425 | 380 | 380 | pinnacle | 380 |
| I1 | 2526 | 380 | 380 | moyenne | 380 |
| D1 | 2122 | 306 | 306 | pinnacle | 306 |
| D1 | 2223 | 306 | 306 | pinnacle | 306 |
| D1 | 2324 | 306 | 306 | pinnacle | 306 |
| D1 | 2425 | 306 | 306 | pinnacle | 306 |
| D1 | 2526 | 306 | 306 | moyenne | 306 |
| F1 | 2122 | 380 | 380 | pinnacle | 380 |
| F1 | 2223 | 380 | 380 | pinnacle | 380 |
| F1 | 2324 | 306 | 306 | pinnacle | 306 |
| F1 | 2425 | 306 | 306 | pinnacle | 306 |
| F1 | 2526 | 306 | 306 | moyenne | 306 |

## Test final — comparaison aux références (même échantillon, même instant)

| Modèle | n | Brier 1X2 | Log-loss 1X2 | RPS | n O/U | Brier O/U 2,5 | Log-loss O/U 2,5 |
|---|---|---|---|---|---|---|---|
| APEX-BSM (gelé) | 3808 | 0.6115 | 1.0208 | 0.2117 | 3808 | 0.2503 | 0.6939 |
| Skill actuel (legacy) | 3808 | 0.6197 | 1.0327 | 0.2154 | 3808 | 0.2555 | 0.7054 |
| Modèle simple de buts | 3808 | 0.6506 | 1.0748 | 0.2309 | 3808 | 0.2488 | 0.6908 |
| Marché démarginé (avant match) | 3808 | 0.5993 | 1.0022 | 0.206 | 3808 | 0.2443 | 0.6814 |

Écart de log-loss 1X2 (APEX-BSM − référence ; négatif = APEX-BSM meilleur), IC 95 % bootstrap par match :

- vs_marche : +0.0186  IC95 [0.0136, 0.0236]
- vs_legacy : -0.0119  IC95 [-0.017, -0.0065]
- vs_simple : -0.0540  IC95 [-0.0635, -0.0443]

### Stabilité par ligue (log-loss 1X2 vs marché)

| Ligue | n | Δ | IC95 |
|---|---|---|---|
| E0 | 361 | +0.0104 | [-0.0065, 0.0266] |
| E1 | 540 | +0.0218 | [0.0102, 0.0343] |
| E2 | 540 | +0.0206 | [0.0052, 0.0364] |
| E3 | 540 | +0.0191 | [0.0045, 0.0329] |
| EC | 529 | +0.0054 | [-0.0059, 0.0171] |
| SP1 | 362 | +0.0196 | [0.0043, 0.035] |
| I1 | 360 | +0.0217 | [0.0046, 0.0395] |
| D1 | 288 | +0.0252 | [0.0088, 0.0413] |
| F1 | 288 | +0.0302 | [0.0138, 0.0465] |

### BTTS : Brier 0.2484, log-loss 0.6901 (n=3960) — aucune cote BTTS historique dans la source : pas de comparaison marché

### Buts : prédits 2.69 / observés 2.72 par match ; log-loss du score exact 2.9017

### Calibration (test final)

**1X2 (3 issues regroupées)**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.0-0.1 | 103 | 0.078 | 0.078 |
| 0.1-0.2 | 968 | 0.164 | 0.154 |
| 0.2-0.3 | 5095 | 0.257 | 0.253 |
| 0.3-0.4 | 2591 | 0.343 | 0.343 |
| 0.4-0.5 | 1710 | 0.446 | 0.443 |
| 0.5-0.6 | 917 | 0.544 | 0.563 |
| 0.6-0.7 | 371 | 0.64 | 0.687 |
| 0.7-0.8 | 99 | 0.743 | 0.778 |
| 0.8-0.9 | 25 | 0.837 | 0.88 |
| 0.9-1.0 | 1 | 0.917 | 0.0 |

**Over 2.5**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.2-0.3 | 71 | 0.274 | 0.394 |
| 0.3-0.4 | 619 | 0.361 | 0.481 |
| 0.4-0.5 | 1380 | 0.453 | 0.483 |
| 0.5-0.6 | 1258 | 0.545 | 0.557 |
| 0.6-0.7 | 514 | 0.64 | 0.57 |
| 0.7-0.8 | 101 | 0.732 | 0.663 |
| 0.8-0.9 | 17 | 0.829 | 0.941 |

**BTTS**

| Tranche | n | Proba moyenne | Fréquence observée |
|---|---|---|---|
| 0.2-0.3 | 2 | 0.281 | 0.5 |
| 0.3-0.4 | 204 | 0.373 | 0.461 |
| 0.4-0.5 | 1363 | 0.458 | 0.503 |
| 0.5-0.6 | 1780 | 0.546 | 0.557 |
| 0.6-0.7 | 578 | 0.634 | 0.614 |
| 0.7-0.8 | 33 | 0.722 | 0.636 |

### Paris simulés (règles fixées avant le test)

Règles : {'markets': ['1X2', 'OU2.5'], 'ev_min': 0.03, 'one_market_per_match': True, 'stake_units': 1.0, 'price': 'cote relevée avant match (Pinnacle PSH/PSD/PSA, P>2.5/P<2.5 ; à défaut moyenne marché)'}

- Paris : 3232 · taux de réussite : 0.34 · rendement net/unité : -0.1195 (IC95 [-0.1665, -0.0702]) · P&L : -386.19 u · perte max cumulée : -425.23 u · CLV moyen 1X2 : -0.0545
- Par marché : {'1X2': {'n': 2017, 'rendement': -0.1547, 'reussite': 0.2519}, 'OU2.5': {'n': 1215, 'rendement': -0.061, 'reussite': 0.4864}}

*Qualité prédictive (log-loss), taux de réussite et rentabilité sont trois choses distinctes. Ces paris sont simulés : aucune mise n'a été exécutée.*

## Variantes essayées (validation uniquement)

256 variantes. Les 10 meilleures :

| xi | K | rho | sigma | n | LL 1X2 | LL O/U | LL score | objectif |
|---|---|---|---|---|---|---|---|---|
| 0.003 | 6.0 | -0.05 | 0.2 | 7920 | 1.0191 | 0.6853 | 2.9262 | 1.7044 |
| 0.003 | 6.0 | -0.05 | 0.1 | 7920 | 1.0190 | 0.6854 | 2.9234 | 1.7045 |
| 0.0015 | 6.0 | -0.05 | 0.1 | 7920 | 1.0194 | 0.6850 | 2.9239 | 1.7045 |
| 0.0015 | 6.0 | -0.05 | 0.2 | 7920 | 1.0195 | 0.6850 | 2.9268 | 1.7045 |
| 0.003 | 6.0 | -0.05 | 0.0 | 7920 | 1.0190 | 0.6855 | 2.9229 | 1.7046 |
| 0.0015 | 6.0 | -0.05 | 0.0 | 7920 | 1.0194 | 0.6851 | 2.9235 | 1.7046 |
| 0.003 | 6.0 | 0.0 | 0.2 | 7920 | 1.0193 | 0.6853 | 2.9264 | 1.7046 |
| 0.0015 | 6.0 | 0.0 | 0.2 | 7920 | 1.0197 | 0.6850 | 2.9270 | 1.7047 |
| 0.003 | 6.0 | -0.1 | 0.1 | 7920 | 1.0193 | 0.6854 | 2.9246 | 1.7047 |
| 0.003 | 6.0 | 0.0 | 0.3 | 7920 | 1.0192 | 0.6856 | 2.9348 | 1.7048 |

Apport du rythme partagé (validation) : meilleur objectif sans rythme 1.7046 · retenu 1.7046.
Le rythme partagé n'est adopté que s'il améliore l'objectif de validation d'au moins 0.0005 ; sinon le modèle simple (σ=0) est gelé.
