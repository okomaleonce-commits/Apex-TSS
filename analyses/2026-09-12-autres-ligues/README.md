# 12/09/2026 — Hors Bundesliga : où grignoter

La Bundesliga (et les 5 grands championnats) sont bloqués par la gate G3 du preflight :
2 journées jouées. Les ligues à calendrier civil, elles, ont un échantillon mûr.

| Ligue | Journées | G3 preflight | Retenu |
|---|---|---|---|
| Brasileirão | J25–27 | PASS | ✅ |
| Eliteserien (NOR) | J19–20 | PASS | ✅ |
| Allsvenskan (SWE) | J20 | PASS | ✅ |
| RPL (RUS) | J7 | 6–9 → Kelly ×0.25 | ❌ split domicile/extérieur = 3–5 matchs |
| Superliga (ROU) | J8 | 6–9 → Kelly ×0.25 | ❌ idem |

## Le modèle de totaux a échoué à sa propre calibration

`calib.py` compare le λ total du modèle à la ligne de total Pinnacle (meilleur estimateur
disponible du vrai λ), pour 4 valeurs du coefficient de shrinkage k :

| k | biais moyen | MAE |
|---|---|---|
| 1.00 | +0.37 but | 0.66 |
| 0.75 | +0.32 | 0.53 |
| 0.50 | +0.26 | 0.41 |
| 0.35 | +0.22 | 0.34 |

L'erreur résiduelle (0.34–0.66 but) dépasse tout edge que le modèle prétend trouver sur les
marchés de total. **Aucun pari Over/Under n'est publié.** Conformément à la gate G2 du
preflight, on se rabat sur les marchés où l'entrée est directement mesurée.

## Ce qui passe : les marchés à un seul paramètre (Team Total 0.5)

Ils ne dépendent que du taux de marque d'une équipe — quantité mesurée sur 20+ matchs — et
non du total du match.

| Pari | Cote | P marché | P modèle (débiaisée) | Taux empirique | EV |
|---|---|---|---|---|---|
| Halmstad **ne marque pas** @ IFK Göteborg | 2.44 | 39.1 % | 54.3 % | 50 % (5/10 ext.) | +32 % |
| Fluminense **marque** @ Atlético MG | 1.54 | 62.7 % | 72.7 % | 92 % (11/12 ext.) | +12 % |

Écarté de justesse : Tromsø marque @ Rosenborg 1.50 (EV +5 % après débiaisage).

## Le drapeau rouge du jour : Grêmio–Vasco

Pinnacle fait de Vasco (17e, 0 victoire en 12 déplacements) le favori à 2.31 chez Grêmio
(15e, 7 victoires en 12 réceptions, coté 3.39), avec 3.3 % de marge. Le modèle donne Grêmio
à 64 %. Un écart de 35 points contre un book sharp n'est pas une value : c'est de
l'information que je n'ai pas. **NO TOUCH.**
