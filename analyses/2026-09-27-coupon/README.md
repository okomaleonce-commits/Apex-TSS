# Coupon APEX — dimanche 27 septembre 2026

Matchs restants à partir de 14h (heure de Paris) : 8 matchs de Ligue des Nations, plus Columbus – Inter Miami en MLS dans la nuit.
Méthode : identique à `../2026-09-26-journee/` (Elo + Dixon-Coles, fusion 62/38 avec les cotes odds-radar). Script : `model.py`, sortie : `final.json`.

## Probabilités du modèle

| Heure | Match | Cotes 1/X/2 | Modèle 1/X/2 | Score probable | Lecture |
|---|---|---|---|---|---|
| 15:00 | Lituanie – Azerbaïdjan | 2,34 / 3,05 / 4,00 | 44 / 30 / 27 | 1-1 | Under 3,5 (80 %) |
| 18:00 | Serbie – Pays-Bas | 8,80 / 5,30 / 1,43 | 12 / 22 / 66 | 0-2 | Pays-Bas X2 (88 %). Vlahović forfait |
| 18:00 | Danemark – Pays de Galles | 1,43 / 5,26 / 8,60 | 72 / 19 / 10 | 2-0 | Danemark gagne. Rodon, Lawlor et Wilson absents côté gallois |
| 18:00 | Autriche – Kosovo | 1,64 / 4,25 / 6,40 | 62 / 23 / 14 | 2-0 / 1-0 | Autriche gagne |
| 18:00 | Gibraltar – Andorre | 4,00 / 2,84 / 2,52 | 32 / 31 / 37 | 1-1 / 0-0 | Under 3,5 (82 %) |
| 20:45 | Allemagne – Grèce | 1,46 / 5,26 / 8,00 | 70 / 19 / 11 | 2-0 | Allemagne gagne. Musiala et Havertz forfaits |
| 20:45 | Norvège – Portugal | 2,44 / 4,00 / 2,94 | 42 / 28 / 30 | 1-1 | BTTS Oui (55 %) |
| 20:45 | Israël – Irlande | 2,86 / 3,40 / 2,78 | 38 / 29 / 33 | 1-1 | Under 3,5 (76 %) |
| 01:00 | Columbus – Inter Miami | 2,94 / 4,20 / 2,38 | 35 / 24 / 42 | 1-1 / 1-2 | Over 2,5 (69 %) et BTTS (69 %) |

## 🟢 Coupon « prudent » — 3 sélections

| Match | Pari | Cote | Proba |
|---|---|---|---|
| Allemagne – Grèce | Allemagne gagne | 1,46 | 70 % |
| Danemark – Pays de Galles | Danemark gagne | 1,43 | 72 % |
| Serbie – Pays-Bas | Double chance X2 | ~1,10* | 88 % |

**Cote totale ≈ 2,30 · probabilité que le coupon passe ≈ 44 %** · mise conseillée : 2 % de la bankroll

## 🟡 Coupon « équilibré » — 4 sélections

| Match | Pari | Cote | Proba |
|---|---|---|---|
| Allemagne – Grèce | Allemagne gagne | 1,46 | 70 % |
| Danemark – Pays de Galles | Danemark gagne | 1,43 | 72 % |
| Serbie – Pays-Bas | Pays-Bas gagne | 1,43 | 66 % |
| Autriche – Kosovo | Autriche gagne | 1,64 | 62 % |

**Cote totale ≈ 4,90 · probabilité ≈ 21 %** · mise conseillée : 1 % de la bankroll

## 🔴 Simples « value » (petite mise, 0,5 % chacun)

| Match | Pari | Cote | Modèle | EV |
|---|---|---|---|---|
| Norvège – Portugal | Match nul | 4,00 | 28 % | +10 % |
| Israël – Irlande | Israël gagne | 2,86 | 38 % | +8 % |

\* Cote de double chance estimée à partir du 1X2 (marge d'environ 3 %). Vérifiez-la chez votre bookmaker.

## Honnêteté sur l'edge

Les cotes du jour ont une marge proche de 0 % : ce sont des prix très efficients. **Sur les deux combinés, l'espérance de gain calculée est quasi nulle (≈ +1 %).** Ils sont construits pour la probabilité de réussite, pas pour battre le marché. Le seul avantage mesurable vient des deux simples « value », et il reste modeste.

Autre point : Gibraltar gagne à 4,00 ressort à +28 % d'EV, mais l'Elo est peu fiable entre deux micro-nations. Je l'écarte.
