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

## 3. Tous les matchs (heures UTC ; ajouter 2 h pour l'heure de Paris)

| UTC | Comp. | Match | λ dom–ext | 1/X/2 % | Cotes 1X2 | O2,5 % | BTTS % | 3 scores les plus probables | Sims | Décision |
|---|---|---|---|---|---|---|---|---|---|---|
| 02:00 | CNL | Guatemala – El Salvador | 1.94–0.61 | 68/21/11 | 2.04/3.39/3.77 | 47 | 40 | 1-0 15%, 2-0 15%, 1-1 10% | 40k | — abstention |
| 11:00 | CAN | Comoros – Namibia | 1.61–0.76 | 57/26/17 | 1.86/2.98/4.85 | 42 | 43 | 1-0 14%, 1-1 12%, 2-0 12% | 40k | ⚪ AH dom -1.25 @ 3.12 (p 31%, EV +10%, écart marché +6 pts) |
| 13:00 | CAN | Burundi – Algeria | 0.38–2.50 | 4/13/83 | 10.53/4.82/1.29 | 55 | 29 | 0-2 18%, 0-3 15%, 0-1 14% | 40k | ⚪ BTTS non @ 1.5 (p 71%, EV +7%, écart marché +9 pts) |
| 13:00 | CAN | Ethiopia – Senegal | 0.30–2.65 | 2/11/87 | 5.91/3.69/1.36 | 56 | 24 | 0-2 18%, 0-3 16%, 0-1 14% | 40k | — abstention |
| 13:00 | CAN | Lesotho – Morocco | 0.06–3.08 | 0/5/94 | 34.0/14.85/1.03 | 60 | 5 | 0-3 21%, 0-2 20%, 0-4 16% | 40k | — abstention |
| 13:00 | CAN | Madagascar – Tanzania | 1.85–0.65 | 65/23/12 | 2.42/2.75/3.33 | 46 | 41 | 1-0 15%, 2-0 14%, 1-1 10% | 40k | ⚪ Over 2.5 @ 2.55 (p 46%, EV +16%, écart marché +9 pts) |
| 13:00 | CAN | Mozambique – Sudan | 1.40–0.87 | 48/29/23 | 1.94/2.84/4.75 | 39 | 44 | 1-0 14%, 1-1 13%, 0-0 11% | 40k | ⚪ Over 2.5 @ 2.95 (p 39%, EV +16%, écart marché +7 pts) |
| 13:00 | CAN | South Sudan – Egypt | 0.16–2.91 | 1/7/92 | 13.77/5.72/1.2 | 59 | 14 | 0-2 20%, 0-3 19%, 0-4 14% | 40k | — abstention (fragile) |
| 16:00 | LdN | Finland – Belarus | 1.69–0.89 | 55/25/19 | 1.6/3.84/6.07 | 48 | 49 | 1-0 12%, 1-1 12%, 2-0 11% | 40k | ⚪ 1X2 2 @ 6.07 (p 19%, EV +16%, écart marché +4 pts) |
| 16:00 | LdN | Moldova – Faroe Islands | 1.18–1.28 | 33/29/38 | 2.89/2.87/2.86 | 44 | 50 | 1-1 14%, 0-1 10%, 0-0 10% | 40k | ⚪ Over 2.5 @ 2.67 (p 44%, EV +17%, écart marché +8 pts) |
| 16:00 | CAN | Cape Verde Islands – Rwanda | 2.36–0.43 | 80/15/5 | 1.44/3.89/8.07 | 53 | 32 | 2-0 17%, 1-0 14%, 3-0 13% | 40k | — abstention |
| 16:00 | CAN | Ghana – Gambia | 1.97–0.60 | 69/21/10 | 1.47/3.84/6.93 | 47 | 39 | 2-0 15%, 1-0 15%, 3-0 10% | 40k | ⚪ AH dom -1.75 @ 3.07 (p 44%, EV +13%, écart marché +7 pts) |
| 16:00 | CAN | Guinea-Bissau – Nigeria | 0.38–2.48 | 4/13/83 | 7.5/3.61/1.5 | 54 | 29 | 0-2 17%, 0-3 15%, 0-1 14% | 40k | — abstention |
| 16:00 | CAN | Uganda – Libya | 1.43–0.85 | 49/28/22 | 1.95/2.95/4.43 | 40 | 44 | 1-0 14%, 1-1 13%, 0-0 11% | 40k | — abstention (fragile) |
| 16:00 | CAN | Zambia – Togo | 1.35–0.90 | 46/29/25 | 1.93/3.11/4.07 | 39 | 45 | 1-1 13%, 1-0 13%, 0-0 11% | 40k | — abstention (fragile) |
| 17:00 | CAN | Benin – Mauritania | 1.68–0.73 | 59/25/15 | 2.52/2.55/3.46 | 43 | 43 | 1-0 15%, 2-0 13%, 1-1 12% | 40k | ⚪ Over 2.5 @ 2.77 (p 43%, EV +19%, écart marché +9 pts) |
| 18:00 | NL | Boreham Wood – Kidderminster | 1.62–1.01 | 51/26/23 | 1.5/4.0/6.94 | 49 | 51 | 1-1 12%, 1-0 11%, 2-1 10% | 40k | ⛔ veto backtest |
| 18:45 | LdN | Spain – Croatia | 2.97–0.30 | 90/8/2 | 1.19/7.83/13.37 | 63 | 25 | 2-0 17%, 3-0 17%, 4-0 12% | 40k | ⚪ 1X2 1 @ 1.19 (p 90%, EV +7%, écart marché +9 pts) |
| 18:45 | LdN | Czechia – England | 0.52–2.52 | 5/14/81 | 8.6/5.22/1.37 | 58 | 38 | 0-2 15%, 0-3 13%, 0-1 12% | 40k | ⚪ AH ext -0.50 @ 1.36 (p 81%, EV +10%, écart marché +10 pts) |
| 18:45 | LdN | San Marino – Albania | 0.05–3.38 | 0/4/96 | 39.79/19.72/1.04 | 66 | 5 | 0-3 21%, 0-2 19%, 0-4 17% | 40k | — abstention |
| 18:45 | LdN | Slovakia – Kazakhstan | 2.52–0.52 | 80/14/5 | 1.27/5.65/11.88 | 58 | 38 | 2-0 15%, 3-0 13%, 1-0 12% | 40k | ⚪ AH dom -2.00 @ 2.58 (p 36%, EV +15%, écart marché +9 pts) |
| 18:45 | LdN | Bulgaria – Estonia | 1.79–0.84 | 59/24/17 | 1.7/3.53/5.67 | 49 | 48 | 1-0 12%, 2-0 11%, 1-1 11% | 40k | ⚪ Over 2.5 @ 2.39 (p 49%, EV +17%, écart marché +9 pts) |
| 18:45 | LdN | Luxembourg – Iceland | 1.29–1.17 | 38/29/33 | 3.5/3.39/2.16 | 44 | 50 | 1-1 14%, 1-0 10%, 0-0 9% | 40k | ⚪ AH dom +1.25 @ 1.27 (p 86%, EV +7%, écart marché +10 pts) |
| 18:45 | LdN | Scotland – Switzerland | 0.97–1.55 | 23/27/50 | 4.35/3.62/1.87 | 46 | 50 | 1-1 13%, 0-1 12%, 0-2 10% | 40k | — abstention |
| 18:45 | LdN | Slovenia – FYR Macedonia | 1.99–0.76 | 66/21/13 | 1.65/3.78/5.88 | 51 | 46 | 2-0 13%, 1-0 12%, 1-1 10% | 40k | ⚪ AH dom -1.75 @ 3.74 (p 41%, EV +25%, écart marché +9 pts) |
| 18:45 | NL | Fylde – Carlisle | 1.67–1.62 | 39/24/37 | 2.49/3.96/2.49 | 63 | 65 | 1-1 10%, 1-2 8%, 2-1 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Barrow – Scunthorpe | 1.66–1.36 | 44/25/31 | 2.09/3.82/3.16 | 58 | 60 | 1-1 11%, 2-1 9%, 1-2 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Halifax – Boston Utd | 1.49–1.14 | 44/27/29 | 2.23/3.67/2.99 | 49 | 53 | 1-1 13%, 1-0 10%, 2-1 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Forest Green – Wealdstone | 1.98–1.20 | 55/23/22 | 1.74/4.14/4.13 | 61 | 60 | 1-1 10%, 2-1 10%, 2-0 8% | 40k | ⛔ veto backtest |
| 18:45 | NL | Gateshead – Altrincham | 1.18–1.59 | 27/26/46 | 4.06/4.13/1.75 | 52 | 55 | 1-1 12%, 0-1 9%, 1-2 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Hartlepool – Harrogate | 1.41–1.78 | 30/24/46 | 3.56/3.85/1.93 | 62 | 63 | 1-1 11%, 1-2 9%, 2-1 7% | 40k | ⛔ veto backtest |
| 18:45 | NL | Hornchurch – Aldershot | 1.51–1.29 | 42/26/32 | 2.13/3.75/3.12 | 53 | 57 | 1-1 12%, 2-1 9%, 1-0 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Woking – Solihull | 1.94–1.06 | 57/23/20 | 1.95/3.75/3.58 | 57 | 56 | 1-1 11%, 2-1 10%, 1-0 9% | 40k | ⛔ veto backtest |
| 18:45 | NL | Yeovil – Worthing | 1.43–1.60 | 34/25/41 | 2.14/4.04/2.93 | 58 | 61 | 1-1 12%, 1-2 9%, 2-1 8% | 40k | ⛔ veto backtest |
| 19:00 | CAN | Congo – Cameroon | 0.46–2.31 | 5/16/79 | 5.98/3.47/1.61 | 52 | 34 | 0-2 17%, 0-1 14%, 0-3 13% | 40k | ⚪ BTTS non @ 1.62 (p 66%, EV +8%, écart marché +9 pts) |
| 19:00 | CAN | Gabon – Niger | 1.46–0.83 | 51/28/21 | 1.56/3.51/6.52 | 40 | 44 | 1-0 14%, 1-1 13%, 0-0 11% | 40k | ⚪ 1X2 2 @ 6.52 (p 21%, EV +39%, écart marché +7 pts) |
| 19:00 | CAN | Liberia – Mali | 0.62–1.92 | 11/21/68 | 7.17/3.74/1.49 | 47 | 40 | 0-1 15%, 0-2 14%, 1-1 10% | 40k | ⚪ AH ext -1.00 @ 1.85 (p 42%, EV +4%, écart marché +5 pts) |
| 19:00 | CAN | Somalia – Ivory Coast | 0.06–3.11 | 0/5/95 | 33.31/13.15/1.04 | 61 | 5 | 0-3 21%, 0-2 20%, 0-4 16% | 40k | — abstention |
| 19:00 | CNL | British Virgin Islands – Turks and Caicos Islands | 1.76–0.69 | 62/24/14 | — | 44 | 42 | 1-0 15%, 2-0 13%, 1-1 11% | 40k | — sans cote |
| 20:00 | CNL | US Virgin Islands – Bahamas | 1.45–0.84 | 50/28/22 | — | 40 | 44 | 1-0 14%, 1-1 13%, 0-0 11% | 40k | — sans cote |
| 21:00 | CNL | French Guyana – Sint Maarten | 2.34–0.44 | 80/15/5 | — | 52 | 32 | 2-0 17%, 1-0 14%, 3-0 13% | 40k | — sans cote |
| 23:00 | CNL | Anguilla – Aruba | 0.56–2.08 | 8/19/73 | — | 49 | 37 | 0-2 15%, 0-1 15%, 0-3 11% | 40k | — sans cote |

## 4. Lecture

- **Sélections officielles : aucune.**
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
- Pour les sélections nationales, **backtester la conversion Elo → λ** sur l'historique des matchs internationaux avant de lui accorder le moindre statut.
