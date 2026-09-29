# Analyse APEX-BSM — mardi 29 septembre 2026

Relevé des cotes : API-Football (Pinnacle en priorité), 2026-09-29T01:05:43+00:00 UTC. Les compositions n'étaient pas encore publiées : c'est une prévision « veille ». Modèle `apex-bsm-1.0.0`. Chaque ligne du tableau correspond à une prévision enregistrée dans `ledger/forecasts.jsonl`, identifiants dans `runs.json`.

## 1. Bilan du backtest (run `bsm-20260929T010237Z`, National League ajoutée)

- **Données** : 9 championnats (E0, E1, E2, E3, EC, SP1, I1, D1, F1). Apprentissage 2021-23, validation 2023-25, **test final isolé 2025-26 : 3 808 matchs**.
- **Log-loss 1X2** : APEX-BSM 1,0208 · skill actuel 1,0327 · modèle simple 1,0748 · **marché 1,0022**.
- **Face au marché** : Δ +0,0186, IC95 [+0,014 ; +0,024]. En National League, Δ +0,005, IC95 [−0,006 ; +0,017], non significatif.
- **Paris simulés** (EV ≥ 3 %) : 3 186 paris, **−12,8 % par unité** (IC95 [−17,9 % ; −7,8 %]).
- **Statut : NON SUPÉRIEUR AU MARCHÉ.** Toute EV calculée contre une cote est vetoée.

## 2. Périmètre et statut de validation

| Compétition | Matchs | Statut |
|---|---|---|
| National League (EC) | 10 | Forces backtestées. **Veto** : le modèle est moins précis que le marché |
| Ligue des Nations, qualifs CAN, CONCACAF | 32 | Hors périmètre du backtest. λ issus du classement Elo (eloratings.net, +100 à domicile) via une conversion **non validée** → sélections au mieux **indicatives** |

Les stades n'ont pas été vérifiés : plusieurs sélections africaines jouent leurs matchs « à domicile » sur terrain neutre, ce qui surestime alors l'avantage du terrain. Chaque match a été simulé 40 000 fois (demi-largeur IC95 Monte-Carlo ≤ 0,5 pt).

## 3. Tous les matchs — version finale (sélections : conversion H1 backtestée ; heures UTC, +2 h pour Paris)

| UTC | Comp. | Match | λ dom–ext | 1/X/2 % | Cotes 1X2 | O2,5 % | BTTS % | 3 scores les plus probables | Sims | Décision |
|---|---|---|---|---|---|---|---|---|---|---|
| 02:00 | CNL | Guatemala – El Salvador | 1.84–0.73 | 62/24/14 | 2.04/3.39/3.77 | 47 | 45 | 1-0 13%, 2-0 13%, 1-1 11% | 40k | — abstention (fragile) |
| 11:00 | CAN | Comoros – Namibia | 1.60–0.86 | 53/28/19 | 1.86/2.98/4.85 | 44 | 47 | 1-1 13%, 1-0 13%, 2-0 11% | 40k | ⚪ Over 2.5 @ 2.6 (p 44%, EV +15%, écart marché +8 pts) |
| 13:00 | CAN | Burundi – Algeria | 0.52–2.43 | 5/16/79 | 10.53/4.82/1.29 | 56 | 37 | 0-2 15%, 0-3 13%, 0-1 12% | 40k | ⚪ AH ext -2.00 @ 2.63 (p 34%, EV +12%, écart marché +8 pts) |
| 13:00 | CAN | Ethiopia – Senegal | 0.52–2.43 | 5/16/79 | 5.91/3.69/1.36 | 56 | 37 | 0-2 15%, 0-3 13%, 0-1 12% | 40k | ⚪ AH ext -1.75 @ 2.13 (p 57%, EV +8%, écart marché +10 pts) |
| 13:00 | CAN | Lesotho – Morocco | 0.37–3.07 | 2/9/89 | 34.0/14.85/1.03 | 67 | 30 | 0-3 16%, 0-2 15%, 0-4 12% | 40k | ⚪ 1X2 X @ 14.85 (p 9%, EV +34%, écart marché +3 pts) |
| 13:00 | CAN | Madagascar – Tanzania | 1.89–0.71 | 64/24/13 | 2.42/2.75/3.33 | 48 | 44 | 2-0 13%, 1-0 13%, 1-1 11% | 40k | — abstention |
| 13:00 | CAN | Mozambique – Sudan | 1.53–0.91 | 50/28/21 | 1.94/2.84/4.75 | 44 | 48 | 1-1 14%, 1-0 12%, 0-0 10% | 40k | ⚪ AH dom -1.25 @ 3.49 (p 26%, EV +4%, écart marché +3 pts) |
| 13:00 | CAN | South Sudan – Egypt | 0.41–2.85 | 3/11/86 | 13.77/5.72/1.2 | 63 | 32 | 0-2 16%, 0-3 15%, 0-4 10% | 40k | — abstention (fragile) |
| 16:00 | CAN | Cape Verde Islands – Rwanda | 2.17–0.60 | 73/19/8 | 1.44/3.89/8.07 | 52 | 40 | 2-0 15%, 1-0 13%, 3-0 11% | 40k | ⚪ AH dom -1.50 @ 2.43 (p 49%, EV +18%, écart marché +10 pts) |
| 16:00 | CAN | Ghana – Gambia | 1.88–0.72 | 63/24/13 | 1.47/3.84/6.93 | 48 | 44 | 1-0 13%, 2-0 13%, 1-1 11% | 40k | — abstention |
| 16:00 | CAN | Guinea-Bissau – Nigeria | 0.52–2.41 | 5/16/79 | 7.5/3.61/1.5 | 56 | 38 | 0-2 15%, 0-3 13%, 0-1 12% | 40k | — abstention |
| 16:00 | CAN | Uganda – Libya | 1.48–0.94 | 48/29/23 | 1.95/2.95/4.43 | 44 | 48 | 1-1 14%, 1-0 12%, 0-0 10% | 40k | ⚪ Over 2.5 @ 2.64 (p 44%, EV +15%, écart marché +8 pts) |
| 16:00 | CAN | Zambia – Togo | 1.58–0.87 | 53/28/20 | 1.93/3.11/4.07 | 44 | 47 | 1-1 13%, 1-0 12%, 2-0 11% | 40k | ⚪ Over 2.5 @ 2.55 (p 44%, EV +12%, écart marché +8 pts) |
| 16:00 | LdN | Finland – Belarus | 1.56–0.89 | 52/28/20 | 1.6/3.84/6.07 | 44 | 47 | 1-1 13%, 1-0 12%, 2-0 10% | 40k | ⚪ 1X2 2 @ 6.07 (p 20%, EV +23%, écart marché +5 pts) |
| 16:00 | LdN | Moldova – Faroe Islands | 1.15–1.22 | 33/31/37 | 2.89/2.87/2.86 | 42 | 49 | 1-1 14%, 0-0 11%, 0-1 10% | 40k | ⚪ Over 2.5 @ 2.67 (p 42%, EV +12%, écart marché +6 pts) |
| 17:00 | CAN | Benin – Mauritania | 1.74–0.78 | 59/26/16 | 2.52/2.55/3.46 | 46 | 46 | 1-0 13%, 1-1 12%, 2-0 12% | 40k | — abstention |
| 18:00 | NL | Boreham Wood – Kidderminster | 1.62–1.01 | 51/26/23 | 1.5/4.0/6.94 | 49 | 51 | 1-1 12%, 1-0 11%, 2-1 10% | 40k | ⛔ veto backtest |
| 18:45 | LdN | Bulgaria – Estonia | 1.67–0.82 | 56/27/17 | 1.7/3.53/5.67 | 45 | 46 | 1-0 13%, 1-1 13%, 2-0 11% | 40k | — abstention (fragile) |
| 18:45 | LdN | Czechia – England | 0.57–2.24 | 7/18/75 | 8.6/5.22/1.37 | 53 | 40 | 0-2 15%, 0-1 13%, 0-3 11% | 40k | ⚪ AH ext -2.00 @ 2.95 (p 29%, EV +8%, écart marché +4 pts) |
| 18:45 | LdN | Luxembourg – Iceland | 1.14–1.24 | 32/31/37 | 3.5/3.39/2.16 | 42 | 49 | 1-1 15%, 0-0 11%, 0-1 10% | 40k | ⚪ 1X2 1 @ 3.5 (p 32%, EV +12%, écart marché +5 pts) |
| 18:45 | LdN | San Marino – Albania | 0.25–3.95 | 0/4/96 | 39.79/19.72/1.04 | 79 | 22 | 0-3 15%, 0-4 15%, 0-5 12% | 40k | — abstention |
| 18:45 | LdN | Scotland – Switzerland | 0.99–1.41 | 25/29/46 | 4.35/3.62/1.87 | 43 | 49 | 1-1 14%, 0-1 12%, 0-0 11% | 40k | ⚪ AH dom +0.00 @ 3.22 (p 25%, EV +10%, écart marché +5 pts) |
| 18:45 | LdN | Slovakia – Kazakhstan | 2.21–0.59 | 74/19/8 | 1.27/5.65/11.88 | 52 | 40 | 2-0 15%, 1-0 13%, 3-0 11% | 40k | ⚪ AH ext +0.50 @ 3.97 (p 26%, EV +4%, écart marché +2 pts) |
| 18:45 | LdN | Slovenia – FYR Macedonia | 1.78–0.77 | 60/25/15 | 1.65/3.78/5.88 | 47 | 46 | 1-0 13%, 2-0 12%, 1-1 12% | 40k | ⚪ AH dom -1.75 @ 3.74 (p 35%, EV +5%, écart marché +3 pts) |
| 18:45 | LdN | Spain – Croatia | 2.93–0.40 | 87/10/3 | 1.19/7.83/13.37 | 64 | 32 | 2-0 15%, 3-0 15%, 4-0 11% | 40k | ⚪ AH dom -1.75 @ 1.7 (p 69%, EV +10%, écart marché +9 pts) |
| 18:45 | NL | Barrow – Scunthorpe | 1.66–1.36 | 44/25/31 | 2.09/3.82/3.16 | 58 | 60 | 1-1 11%, 2-1 9%, 1-2 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Forest Green – Wealdstone | 1.98–1.20 | 55/23/22 | 1.74/4.14/4.13 | 61 | 60 | 1-1 10%, 2-1 10%, 2-0 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Fylde – Carlisle | 1.67–1.62 | 39/24/37 | 2.49/3.96/2.49 | 63 | 65 | 1-1 10%, 1-2 8%, 2-1 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Gateshead – Altrincham | 1.18–1.59 | 27/26/46 | 4.06/4.13/1.75 | 52 | 55 | 1-1 12%, 0-1 9%, 1-2 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Halifax – Boston Utd | 1.49–1.14 | 44/27/29 | 2.23/3.67/2.99 | 49 | 53 | 1-1 13%, 1-0 10%, 2-1 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Hartlepool – Harrogate | 1.41–1.78 | 30/24/46 | 3.56/3.85/1.93 | 62 | 63 | 1-1 11%, 1-2 9%, 2-1 7% | 40k | ⛔ veto backtest |
| 18:45 | NL | Hornchurch – Aldershot | 1.51–1.29 | 42/26/32 | 2.13/3.75/3.12 | 53 | 57 | 1-1 12%, 2-1 9%, 1-0 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Woking – Solihull | 1.94–1.06 | 57/23/20 | 1.95/3.75/3.58 | 57 | 56 | 1-1 11%, 2-1 10%, 1-0 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Yeovil – Worthing | 1.43–1.60 | 34/25/41 | 2.14/4.04/2.93 | 58 | 61 | 1-1 12%, 1-2 9%, 2-1 8% | 40k | ⛔ veto backtest |
| 19:00 | CAN | Congo – Cameroon | 0.61–2.13 | 9/20/72 | 5.98/3.47/1.61 | 52 | 41 | 0-2 15%, 0-1 13%, 0-3 11% | 40k | ⚪ Over 2.5 @ 2.25 (p 52%, EV +16%, écart marché +10 pts) |
| 19:00 | CAN | Gabon – Niger | 1.50–0.93 | 49/29/22 | 1.56/3.51/6.52 | 44 | 48 | 1-1 14%, 1-0 12%, 0-0 10% | 40k | ⚪ 1X2 2 @ 6.52 (p 22%, EV +47%, écart marché +8 pts) |
| 19:00 | CAN | Liberia – Mali | 0.77–1.78 | 14/25/60 | 7.17/3.74/1.49 | 46 | 45 | 0-1 13%, 0-2 12%, 1-1 12% | 40k | ⚪ BTTS oui @ 2.5 (p 45%, EV +13%, écart marché +8 pts) |
| 19:00 | CAN | Somalia – Ivory Coast | 0.24–4.03 | 0/4/96 | 33.31/13.15/1.04 | 80 | 21 | 0-4 15%, 0-3 15%, 0-5 12% | 40k | — abstention |
| 19:00 | CNL | British Virgin Islands – Turks and Caicos Islands | 1.63–0.85 | 54/27/18 | — | 45 | 47 | 1-1 13%, 1-0 13%, 2-0 11% | 40k | — sans cote |
| 20:00 | CNL | US Virgin Islands – Bahamas | 1.75–0.78 | 59/26/15 | — | 46 | 46 | 1-0 13%, 2-0 12%, 1-1 12% | 40k | — sans cote |
| 21:00 | CNL | French Guyana – Sint Maarten | 1.88–0.72 | 64/24/13 | — | 48 | 44 | 1-0 13%, 2-0 13%, 1-1 11% | 40k | — sans cote |
| 23:00 | CNL | Anguilla – Aruba | 0.72–1.87 | 13/24/64 | — | 48 | 44 | 0-1 13%, 0-2 13%, 1-1 11% | 40k | — sans cote |

## 3 bis. Backtest international (option 3) — `backtests/intl-20260929T011417Z/REPORT.md`

**Données**
- 49 672 matchs officiels (github.com/martj42/international_results), complétés par 126 matchs de septembre 2026 via API-Football.
- Elo reconstruit match par match : seul l'Elo d'avant-match sert à chaque prévision.

**Découpage chronologique**
- Apprentissage 2008-2018 (10 382 matchs).
- Validation 2019-2022 (3 547 matchs).
- **Test final isolé 2023 → 28/09/2026 (3 837 matchs).**

**Résultats sur le test final**

| Log-loss 1X2 | |
|---|---|
| **H1** : régression de Poisson Elo → λ estimée, avantage du terrain estimé à ≈ 106 points Elo, ρ = −0,10 | **0,8623** |
| H0 : heuristique utilisée jusqu'ici | 0,8789 |
| Référence simple | 1,0539 |

- **H1 face à H0** : Δ −0,0166, IC95 [−0,023 ; −0,010]. H1 est meilleure sur les amicaux, les qualifications et les autres tournois ; **égalité en Ligue des Nations**.
- **Correction d'une erreur** : H0 ne prévoyait pas « trop de buts ». Il en prévoyait **trop peu** : 2,60 contre 2,77 observés. Mon diagnostic précédent était faux.
- **Biais par compétition de H1 sur le test** :
  - qualifs CAN : 2,68 buts prévus pour 2,29 observés, Over 2,5 à 49,7 % prévu pour 39,1 % observé ;
  - CONCACAF NL : 2,58 prévus pour 3,27 observés.
- **Variante corrective rejetée** : j'ai testé un niveau de buts propre à chaque compétition. Elle est **rejetée sur la validation** (log-loss O/U 0,6813 contre 0,6764) : ces niveaux ne sont pas stables dans le temps.
  - *Transparence* : j'ai conçu cette variante **après** avoir vu le découpage du test. Le rejet, lui, repose uniquement sur la validation.
- **Aucune cote historique de sélections n'existe** dans ces sources. H1 n'est donc **pas comparé au marché**, et toute sélection sur un match de sélections reste INDICATIVE.
- **Comparaison avec Pinnacle aujourd'hui** :
  - écart moyen de 5,7 points sur P(victoire à domicile) ;
  - **+5,4 points en moyenne sur P(Over 2,5)**, c'est-à-dire le biais CAN mesuré au backtest.

## 4. Lecture

- **Sélections officielles : aucune.** Pour les sélections nationales, le passage de H0 à H1 ne change pas la conclusion.
  - En National League, le veto du backtest s'applique : le modèle y est moins précis que le marché, donc aucune EV n'est crédible.
  - Pour les sélections nationales, les 19 sélections « ⚪ indicatives » viennent de la conversion Elo → λ, qui n'a jamais été backtestée.
- **Pourquoi je ne les recommande pas**, même en demi-mise :
  - Leur écart avec le marché est presque toujours entre +7 et +10 points, juste sous le seuil de 10 points qui les aurait écartées.
  - Cet écart va **toujours dans le même sens** : trop de buts sur les Over 2,5 de CAN, favoris trop forts (Espagne 90 % contre 81 % pour le marché, Angleterre, Slovaquie).
  - Un biais aussi systématique trahit une erreur de calibration de ma conversion, pas une vraie valeur.
- **Les probabilités restent utiles comme estimation**, pour les scénarios et la hiérarchie des matchs. Par exemple :
  - Espagne–Croatie : victoire espagnole à 2-0 ou 3-0 comme scénario central.
  - Écosse–Suisse et Moldavie–Féroé : très ouverts.
  - En National League, Fylde–Carlisle et Hartlepool–Harrogate sont les matchs où le modèle attend le plus de buts.

## 5. Incident corrigé en cours d'analyse (transparence)

Le premier passage avait lu **à l'envers** le signe des handicaps asiatiques côté extérieur. Dans la convention API-Football, « Away −0.25 » signifie extérieur +0,25. J'ai donc fait trois choses :

- **Journal** : les 42 prévisions concernées sont conservées dans le journal et marquées `EXCLU` avec la raison. Rien n'a été effacé.
- **Nouvelle prévision** : un nouveau relevé a été fait, suivi d'une nouvelle simulation.
- **Correctifs** :
  - le connecteur vérifie désormais la cohérence des deux côtés de chaque ligne et conserve les cotes brutes pour audit ;
  - `simulate` écarte, sur un modèle non validé, toute EV dont l'écart avec le marché dépasse 10 points, sur **tous** les marchés binaires (O/U, BTTS, handicaps asiatiques), et plus seulement le 1X2.

## 6. Suite

- Refaire un relevé après publication des compositions (`snapshot`), relancer `run.py`, et ajouter `--missing-lineup` si une absence déterminante reste incertaine.
- Après les matchs, enregistrer les résultats avec `python3 tools/apex_bsm.py settle --forecast-id <id> --score X-Y`, puis lancer `audit`.
- Sélections nationales : la conversion H1 est backtestée (option 3). Prochaine étape : **l'évaluer face au marché** à partir des relevés horodatés API-Football accumulés d'un rassemblement international à l'autre, puis traiter le biais de buts par compétition sur des données de test encore jamais vues.
