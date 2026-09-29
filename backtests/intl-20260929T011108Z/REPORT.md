# Backtest international APEX-INTL — intl-20260929T011108Z

- Source : https://raw.githubusercontent.com/martj42/international_results/master/results.csv + complément `intl_supplement.csv` (API-Football)
- 49672 matchs officiels jusqu'au 2026-09-28 ; Elo reconstruit match par match (avant-match uniquement)
- Apprentissage 2008-2018 (10382) · validation 2019-2022 (3547) · **test final 2023 → 2026-09-28 (3837)** ; équipes avec ≥ 10 matchs d'historique
- Variante retenue (validation) : {'quad': True, 'friendly': False, 'rho': -0.1, 'theta': [0.16982530019882458, 0.8137724515386949, -0.0605865387108185, 0.0, 1.056019748327453]}
- **Statut : H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques)**

## Test final

| Modèle | n | LL 1X2 | Brier | RPS | LL O/U 2,5 | LL BTTS | LL score | buts prédits | observés |
|---|---|---|---|---|---|---|---|---|---|
| H1 régression gelée | 3837 | 0.8623 | 0.5074 | 0.1681 | 0.6817 | 0.6739 | 2.8697 | 2.731 | 2.772 |
| H0 heuristique actuelle | 3837 | 0.8789 | 0.5171 | 0.1723 | 0.6831 | 0.7068 | 2.9297 | 2.599 | 2.772 |
| Référence simple (moyennes) | 3837 | 1.0539 | 0.6351 | 0.2291 | 0.6935 | 0.6994 | 3.2107 | 2.671 | 2.772 |

Δ log-loss 1X2 (négatif = premier meilleur), IC95 bootstrap par match :

- H1 − H0 : -0.0166  IC95 [-0.0232, -0.0103]
- H1 − simple : -0.1916  IC95 [-0.2088, -0.1745]
- H0 − simple : -0.1750  IC95 [-0.195, -0.1542]

### Par type de match (H1 vs H0)

| Type | n | LL H1 | LL H0 | Δ | IC95 |
|---|---|---|---|---|---|
| amical | 950 | 0.9112 | 0.931 | -0.0198 | [-0.0331, -0.0066] |
| qualification | 1514 | 0.7954 | 0.8057 | -0.0103 | [-0.018, -0.0029] |
| ligue des nations | 509 | 0.8989 | 0.8987 | +0.0002 | [-0.0126, 0.0121] |
| phase finale | 417 | 0.9009 | 0.9242 | -0.0233 | [-0.0522, 0.0042] |
| autre tournoi | 447 | 0.9072 | 0.9513 | -0.0441 | [-0.0713, -0.0182] |

### Calibration 1X2 H1

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.0-0.1 | 1727 | 0.05 | 0.056 |
| 0.1-0.2 | 1907 | 0.152 | 0.158 |
| 0.2-0.3 | 3042 | 0.259 | 0.265 |
| 0.3-0.4 | 1441 | 0.331 | 0.328 |
| 0.4-0.5 | 824 | 0.45 | 0.459 |
| 0.5-0.6 | 691 | 0.55 | 0.527 |
| 0.6-0.7 | 601 | 0.648 | 0.627 |
| 0.7-0.8 | 545 | 0.75 | 0.727 |
| 0.8-0.9 | 446 | 0.848 | 0.85 |
| 0.9-1.0 | 287 | 0.942 | 0.93 |

### Calibration Over 2.5 H1

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.4-0.5 | 2479 | 0.445 | 0.459 |
| 0.5-0.6 | 772 | 0.543 | 0.547 |
| 0.6-0.7 | 357 | 0.644 | 0.591 |
| 0.7-0.8 | 156 | 0.743 | 0.712 |
| 0.8-0.9 | 48 | 0.849 | 0.896 |
| 0.9-1.0 | 25 | 0.94 | 0.76 |

### Calibration Over 2.5 H0

| Tranche | n | Proba | Fréquence |
|---|---|---|---|
| 0.3-0.4 | 914 | 0.385 | 0.447 |
| 0.4-0.5 | 1527 | 0.448 | 0.46 |
| 0.5-0.6 | 1151 | 0.55 | 0.57 |
| 0.6-0.7 | 183 | 0.619 | 0.672 |
| 0.7-0.8 | 34 | 0.744 | 0.912 |
| 0.8-0.9 | 28 | 0.833 | 0.786 |

## Variantes essayées (validation)

| quad | amical | rho | LL 1X2 | LL O/U | objectif |
|---|---|---|---|---|---|
| True | False | -0.1 | 0.858 | 0.6764 | 1.5344 |
| True | False | -0.05 | 0.8581 | 0.6764 | 1.5345 |
| True | True | -0.1 | 0.8577 | 0.6769 | 1.5346 |
| True | True | -0.05 | 0.8579 | 0.6769 | 1.5348 |
| True | False | 0.0 | 0.8589 | 0.6764 | 1.5353 |
| True | True | 0.0 | 0.8588 | 0.6769 | 1.5357 |
| False | False | -0.1 | 0.8587 | 0.6772 | 1.5359 |
| False | False | -0.05 | 0.8587 | 0.6772 | 1.5359 |
| False | True | -0.1 | 0.8584 | 0.6776 | 1.5360 |
| False | True | -0.05 | 0.8586 | 0.6776 | 1.5362 |
| False | False | 0.0 | 0.8595 | 0.6772 | 1.5367 |
| False | True | 0.0 | 0.8594 | 0.6776 | 1.5370 |

**Limite majeure** : aucune cote historique de sélections n'est disponible ; ce backtest ne dit rien de la précision face au marché. Toute sélection sur un match de sélections reste donc INDICATIVE.
