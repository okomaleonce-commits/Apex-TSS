# Backtest international APEX-INTL — intl-20261015T114610Z

- Source : https://raw.githubusercontent.com/martj42/international_results/master/results.csv + complément `intl_supplement.csv` (API-Football)
- 49840 matchs officiels jusqu'au 2026-10-07 ; Elo reconstruit match par match (avant-match uniquement)
- Apprentissage 2008-2018 (10382) · validation 2019-2022 (3547) · **test final 2023 → 2026-10-07 (3995)** ; équipes avec ≥ 10 matchs d'historique
- Variante retenue (validation) : {'quad': True, 'friendly': False, 'groupes': False, 'rho': -0.1, 'theta': [0.16982530019882458, 0.8137724515386949, -0.0605865387108185, 0.0, 1.056019748327453], 'groupes_ordre': ['africa_q', 'uefa_nl', 'uefa_q', 'concacaf', 'amical', 'phase_finale']}
- **Statut : H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques)**

## Test final

| Modèle | n | LL 1X2 | Brier | RPS | LL O/U 2,5 | LL BTTS | LL score | buts prédits | observés |
|---|---|---|---|---|---|---|---|---|---|
| H1 régression gelée | 3995 | 0.8643 | 0.5083 | 0.1689 | 0.6807 | 0.6742 | 2.8755 | 2.731 | 2.776 |
| H0 heuristique actuelle | 3995 | 0.8808 | 0.5178 | 0.1731 | 0.6825 | 0.707 | 2.9355 | 2.602 | 2.776 |
| Référence simple (moyennes) | 3995 | 1.0553 | 0.6361 | 0.2299 | 0.6935 | 0.6993 | 3.2186 | 2.67 | 2.776 |

Δ log-loss 1X2 (négatif = premier meilleur), IC95 bootstrap par match :

- H1 − H0 : -0.0165  IC95 [-0.0227, -0.0101]
- H1 − simple : -0.1911  IC95 [-0.2083, -0.1744]
- H0 − simple : -0.1746  IC95 [-0.195, -0.1553]

### Par type de match (H1 vs H0)

| Type | n | LL H1 | LL H0 | Δ | IC95 |
|---|---|---|---|---|---|
| amical | 989 | 0.9153 | 0.9359 | -0.0206 | [-0.0328, -0.0081] |
| qualification | 1534 | 0.797 | 0.8077 | -0.0106 | [-0.0176, -0.0033] |
| ligue des nations | 608 | 0.8942 | 0.8939 | +0.0002 | [-0.0105, 0.0111] |
| phase finale | 417 | 0.9009 | 0.9242 | -0.0233 | [-0.0522, 0.0042] |
| autre tournoi | 447 | 0.9072 | 0.9513 | -0.0441 | [-0.0713, -0.0182] |

### Calibration 1X2 H1

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.0-0.1 | 1794 | 0.05 | 0.059 |
| 0.1-0.2 | 1984 | 0.152 | 0.16 |
| 0.2-0.3 | 3175 | 0.259 | 0.261 |
| 0.3-0.4 | 1497 | 0.331 | 0.327 |
| 0.4-0.5 | 853 | 0.45 | 0.465 |
| 0.5-0.6 | 726 | 0.549 | 0.529 |
| 0.6-0.7 | 633 | 0.649 | 0.627 |
| 0.7-0.8 | 565 | 0.75 | 0.719 |
| 0.8-0.9 | 457 | 0.848 | 0.853 |
| 0.9-1.0 | 301 | 0.943 | 0.93 |

### Calibration Over 2.5 H1

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.4-0.5 | 2586 | 0.446 | 0.459 |
| 0.5-0.6 | 801 | 0.543 | 0.547 |
| 0.6-0.7 | 368 | 0.644 | 0.595 |
| 0.7-0.8 | 161 | 0.744 | 0.714 |
| 0.8-0.9 | 53 | 0.848 | 0.906 |
| 0.9-1.0 | 26 | 0.939 | 0.769 |

### Calibration Over 2.5 H0

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.3-0.4 | 934 | 0.385 | 0.449 |
| 0.4-0.5 | 1604 | 0.448 | 0.458 |
| 0.5-0.6 | 1196 | 0.55 | 0.572 |
| 0.6-0.7 | 195 | 0.621 | 0.682 |
| 0.7-0.8 | 37 | 0.744 | 0.919 |
| 0.8-0.9 | 29 | 0.833 | 0.793 |

### Buts par compétition (test final, H1 gelée)

| Compétition | n | buts prédits | observés | O2,5 prédit | observé |
|---|---|---|---|---|---|
| African Cup of Nations qualification | 286 | 2.69 | 2.32 | 0.499 | 0.395 |
| UEFA Nations League | 294 | 2.62 | 2.62 | 0.482 | 0.51 |
| CONCACAF Nations League | 314 | 2.58 | 3.28 | 0.474 | 0.634 |
| Friendly | 989 | 2.71 | 2.67 | 0.5 | 0.482 |
| FIFA World Cup qualification | 901 | 2.86 | 2.82 | 0.53 | 0.508 |

## Variantes essayées (validation)

| quad | amical | groupes | rho | LL 1X2 | LL O/U | objectif |
|---|---|---|---|---|---|---|
| True | False | False | -0.1 | 0.858 | 0.6764 | 1.5344 |
| True | False | False | -0.05 | 0.8581 | 0.6764 | 1.5345 |
| True | True | False | -0.1 | 0.8577 | 0.6769 | 1.5346 |
| True | True | False | -0.05 | 0.8579 | 0.6769 | 1.5348 |
| True | False | False | 0.0 | 0.8589 | 0.6764 | 1.5353 |
| True | True | False | 0.0 | 0.8588 | 0.6769 | 1.5357 |
| False | False | False | -0.1 | 0.8587 | 0.6772 | 1.5359 |
| False | False | False | -0.05 | 0.8587 | 0.6772 | 1.5359 |
| False | True | False | -0.1 | 0.8584 | 0.6776 | 1.5360 |
| False | True | False | -0.05 | 0.8586 | 0.6776 | 1.5362 |
| False | False | False | 0.0 | 0.8595 | 0.6772 | 1.5367 |
| False | True | False | 0.0 | 0.8594 | 0.6776 | 1.5370 |
| True | False | True | -0.1 | 0.8576 | 0.6813 | 1.5389 |
| True | False | True | -0.05 | 0.8578 | 0.6813 | 1.5391 |
| True | False | True | 0.0 | 0.8586 | 0.6813 | 1.5399 |
| False | False | True | -0.1 | 0.8583 | 0.6823 | 1.5406 |
| False | False | True | -0.05 | 0.8585 | 0.6823 | 1.5408 |
| False | False | True | 0.0 | 0.8592 | 0.6823 | 1.5415 |

**Limite majeure** : aucune cote historique de sélections n'est disponible ; ce backtest ne dit rien de la précision face au marché. Toute sélection sur un match de sélections reste donc INDICATIVE.
