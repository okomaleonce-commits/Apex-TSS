# Analyse APEX — 40 matchs du samedi 26 septembre 2026

## Contexte de la journée

**C'est une pause internationale FIFA.** Premier League, Liga, Serie A, Bundesliga, Ligue 1, Brasileirão et la plupart des championnats sont à l'arrêt. Ce qui se joue :

| Compétition | Matchs | Cotes | Base du modèle |
|---|---|---|---|
| Ligue des Nations (J1 de l'édition 2026/27) | 10 | oui (odds-radar, marge ≈ 0 %) | Elo mondial + marché |
| Qualifs CAN 2027 | 2 | non | Elo seul |
| MLS (soir US du 26 = nuit du 27 heure de Paris) | 14 | oui | forme récente + marché |
| EFL League One | 4 (4 autres reportés, internationaux appelés) | non | forme saison seule |
| EFL League Two | 10 | non | forme saison seule |

## Méthode

- **Sélections** : notes Elo (`eloratings.net`, +100 pour le pays hôte) → probabilité de victoire attendue → λ Dixon-Coles (ρ = −0,08). Là où il y a des cotes, fusion 62 % marché / 38 % modèle.
- **Clubs** : attaque/défense de chaque équipe, régressées vers la moyenne de la ligue (K = 4,5 matchs fictifs), Poisson Dixon-Coles. Fusion 62/38 avec le marché en MLS.
- Script : `model.py` (entrées : `results.json`, `odds.json`, `elo.json`, `matches.json` ; sortie : `final.json`).

## Limites

- **Les compositions ne sont pas connues.** En sélection, les forfaits pèsent lourd : l'Angleterre, par exemple, joue sans Rice, Palmer, Rashford, Mainoo, Stones et James.
- **League One et League Two n'ont pas de cotes publiques**, et les échantillons font 6-7 matchs par équipe. Le modèle tourne seul : confiance plafonnée à « Moyenne ».
- En MLS, la fenêtre de résultats ne couvre que les 6-7 derniers matchs de chaque équipe : c'est un signal de **forme**, pas de niveau global.
- Le score indiqué est le plus probable de la distribution (9-18 %). **C'est un scénario, pas un pari.**

---

## 🌍 Ligue des Nations

**1. Slovénie – Écosse** (15:00) · cotes 2,82 / 3,23 / 3,00 · modèle 38/30/33
Elo très proche (1682 contre 1745, avantage du terrain compris). Aucun des deux n'a une grosse attaque, et le marché voit juste. Total attendu 2,4.
→ **1-1** · **Pari : Under 3,5 (78 %)** · Confiance Moyenne-Élevée

**2. Islande – Estonie** (18:00) · 1,32 / 6,00 / 13,50 · 76/17/7
Plus de 200 points Elo d'écart. L'Estonie est 119e mondiale et ne marque presque jamais à l'extérieur contre ce niveau.
→ **2-0** · **Pari : Islande gagne (76 %)** · *Islande −1 (gagne par 2+) à 54 %* · Confiance Élevée

**3. Îles Féroé – Kazakhstan** (18:00) · 2,38 / 3,35 / 3,57 · 43/29/28
Match serré (1386 contre 1428 Elo), et Tórshavn est un vrai terrain difficile. Le marché et le modèle sont parfaitement alignés.
→ **1-1** · **Pari : Under 2,5 (56 %)** ou 1X (72 %) · Confiance Moyenne

**4. Bulgarie – Luxembourg** (18:00) · 2,48 / 3,12 / 3,65 · 45/29/26
Elo quasi identique (1458 contre 1450). Le modèle voit la Bulgarie un peu plus haut que le marché (EV 1 = +12 %).
→ **1-0 / 1-1** · **Pari : Bulgarie 1X (74 %)** · *Value : Bulgarie gagne à 2,48* · Confiance Moyenne

**5. Saint-Marin – Finlande** (18:00) · 50 / 14 / 1,10 · 1/6/93
Saint-Marin est 206e mondial. La cote du 2 sec (1,10) n'a pas d'intérêt ; les buts, si.
→ **0-3** · **Pari : Finlande −1,5 (gagne par 2+, 79 %)** · Confiance Élevée

**6. Angleterre – Espagne** (20:45, Wembley) · 3,60 / 3,70 / 2,22 · 29/28/43
L'Espagne est **championne du monde en titre** et 1re au classement Elo (2259). L'Angleterre est décimée : Rice, Palmer, Rashford, Mainoo, Livramento, Stones, James et Spence sont forfaits. Le marché fait déjà de l'Espagne la favorite à Wembley, et je le suis.
→ **1-1 / 1-2** · **Pari : Double chance X2 — Espagne ou nul (71 %)** · Confiance Moyenne-Élevée

**7. Tchéquie – Croatie** (20:45) · 3,85 / 3,60 / 2,15 · 24/28/48
200 points Elo d'écart en faveur de la Croatie. La Tchéquie est solide à domicile mais peu créative.
→ **0-1 / 1-1** · **Pari : Croatie X2 (76 %)** · Confiance Moyenne-Élevée

**8. Slovaquie – Moldavie** (20:45) · 1,18 / 9,09 / 27 · 88/10/2
C'est le plus gros écart de la journée (1667 contre 1270). La Moldavie encaisse beaucoup contre les équipes de ce niveau.
→ **2-0 / 3-0** · **Pari : Slovaquie −1 (gagne par 2+, 70 %)** · Confiance Élevée

**9. Albanie – Biélorussie** (20:45) · 1,64 / 4,00 / 6,80 · 62/24/14
L'Albanie est costaude à Tirana, et la Biélorussie joue très bas. Attention au 1-0.
→ **1-0** · **Pari : Albanie 1X (86 %)** · *Albanie gagne à 1,64 (62 %) pour une cote plus intéressante* · Confiance Élevée

**10. Macédoine du Nord – Suisse** (20:45) · 11,11 / 5,20 / 1,39 · 10/20/71
La Suisse (11e Elo) domine logiquement, mais la Macédoine défend dur à Skopje.
→ **0-2** · **Pari : Suisse gagne (71 %)** · Confiance Élevée

## 🌍 Qualifications CAN 2027 (sans cotes)

**11. Kenya – Érythrée** · 72/20/8 · Elo 1363 contre 1202
→ **2-0** · **Pari : Kenya 1X (92 %)**, ou Kenya gagne (72 %) · Confiance Moyenne-Élevée

**12. Afrique du Sud – Guinée** · 64/24/13 · Elo 1560 contre 1464
→ **1-0** · **Pari : Afrique du Sud 1X (87 %)** · Under 2,5 à 55 % · Confiance Moyenne

## 🏴 League One (15:00, pas de cotes)

**13. Plymouth – Burton Albion** · λ 2,24–1,28 · 58/21/20
Plymouth a pris 13 points en 6 matchs, 13 buts marqués. La défense de Burton encaisse (11 buts en 6).
→ **2-1** · **Pari : Over 1,5 (87 %)** · Plymouth 1X à 80 % · Confiance Moyenne

**14. Cambridge Utd – AFC Wimbledon** · λ 1,18–1,04 · 38/31/31
Deux équipes fermées : Wimbledon n'a encaissé que 4 buts en 6 matchs.
→ **1-1** · **Pari : Under 3,5 (81 %)** · Confiance Moyenne

**15. Wycombe – Reading** · λ 1,42–2,22 · 24/22/55
Reading a marqué 14 buts pour 7 encaissés, et Wycombe prend l'eau (13 encaissés).
→ **1-2** · **Pari : Reading X2 (76 %)** · Over 1,5 à 88 % · Confiance Moyenne

**16. Stockport – Peterborough** · λ 2,17–0,91 · 65/21/14
Peterborough ne marque plus : 4 buts en 6 matchs, 5 points.
→ **2-0** · **Pari : Stockport 1X (86 %)** · Confiance Moyenne

## 🏴 League Two (15:00, pas de cotes)

| # | Match | 1/X/2 | Score | Pari | Proba |
|---|---|---|---|---|---|
| 17 | Tranmere – Walsall | 37/32/31 | 1-1 | Under 3,5 | 84 % |
| 18 | Newport – Grimsby | 30/27/42 | 1-1 / 1-2 | Grimsby X2 | 70 % |
| 19 | York – Gillingham | 31/29/40 | 1-1 / 0-1 | Gillingham X2 (3 buts encaissés en 6 matchs) | 69 % |
| 20 | Cheltenham – Chesterfield | 48/24/28 | 2-1 / 1-1 | Over 1,5 | 83 % |
| 21 | Bristol Rovers – Exeter | 48/30/22 | 1-0 | Bristol Rovers 1X | 78 % |
| 22 | Rotherham – Crewe | 45/30/26 | 1-1 / 1-0 | Under 3,5 | 79 % |
| 23 | Fleetwood – Rochdale | 46/29/25 | 1-1 / 1-0 | Fleetwood 1X | 75 % |
| 24 | Oldham – Salford | 37/30/33 | 1-1 | Under 3,5 | 80 % |
| 25 | Swindon – Accrington | 51/24/25 | 2-1 | Over 1,5 | 83 % |
| 26 | Shrewsbury – Colchester | 41/30/29 | 1-1 | Under 3,5 | 80 % |

Confiance **Moyenne** sur toute la League Two : le modèle tourne seul, sans cotes, sur 6 matchs par équipe.

## 🇺🇸 MLS (samedi soir heure US, nuit du 27 heure de Paris)

| # | Match (heure Paris) | Cotes | Final 1/X/2 | Score | Pari | Proba | Conf. |
|---|---|---|---|---|---|---|---|
| 27 | Philadelphia – Orlando (01:30) | 1,51/5,30/6,00 | 66/18/16 | 2-1 | Over 2,5 (Philly 20:5 en 6 matchs) | 77 % | Élevée |
| 28 | NY Red Bulls – St. Louis (01:30) | 3,85/4,40/1,92 | 25/25/49 | 1-1 | St. Louis X2 | 75 % | Moyenne |
| 29 | Atlanta – NYCFC (01:30) | 2,38/3,90/3,05 | 40/30/30 | 1-1 | Under 3,5 | 79 % | Moyenne |
| 30 | Montréal – Cincinnati (01:30) | 2,80/4,00/2,48 | 35/24/41 | 1-1 | BTTS Oui (Montréal 4:16) | 67 % | Moyenne |
| 31 | Charlotte – Chicago (01:30) | 2,24/4,20/3,12 | 53/22/25 | 2-1 | **Charlotte gagne — value +18 %** | 53 % | Moyenne |
| 32 | Seattle – Minnesota (02:30) | 2,32/4,00/3,12 | 47/22/31 | 2-1 | Over 2,5 (Minnesota 17:18 à l'ext.) | 75 % | Moyenne-Élevée |
| 33 | Dallas – LAFC (02:30) | 2,36/3,80/3,05 | 41/25/34 | 1-1 | BTTS Oui | 64 % | Moyenne |
| 34 | Houston – Sporting KC (02:30) | 1,62/4,60/5,70 | 56/24/20 | 1-1 / 2-1 | Houston 1X | 80 % | Moyenne |
| 35 | Nashville – Toronto (02:30) | 1,47/5,30/7,40 | 67/18/14 | 2-1 | Nashville gagne | 67 % | Élevée |
| 36 | Austin – San Diego (02:30) | 2,50/3,85/2,86 | 44/26/30 | 1-1 | Austin 1X (value +10 % sur le 1) | 70 % | Moyenne |
| 37 | Real Salt Lake – New England (03:30) | 2,58/4,00/2,72 | 27/25/48 | 1-1 / 1-2 | **New England X2 — value +30 % sur le 2** | 73 % | Moyenne |
| 38 | San Jose – Portland (04:30) | 2,05/4,20/3,60 | 47/24/29 | 1-1 | Over 1,5 | 85 % | Moyenne-Élevée |
| 39 | Vancouver – DC United (04:30) | 1,28/7,40/11,50 | 77/15/7 | 2-0 | Vancouver −1 (gagne par 2+) | 57 % | Moyenne |
| 40 | LA Galaxy – Colorado (04:30) | 2,28/4,17/3,05 | 46/26/28 | 1-1 | Galaxy 1X | 72 % | Moyenne |

**Le plus gros écart modèle/marché du jour est le n° 37.** Real Salt Lake a pris 1 point sur ses 7 derniers matchs (4 buts marqués, 14 encaissés). New England en a pris 16 sur 7. Le marché les met pourtant à égalité. L'échantillon est court, d'où le choix du X2 plutôt que du 2 sec.

---

## 🎯 Sélection finale

**Les plus sûrs (combinables avec prudence)**
1. Albanie 1X — 86 %
2. Slovaquie −1 contre la Moldavie — 70 %
3. Espagne X2 à Wembley — 71 %
4. Croatie X2 en Tchéquie — 76 %
5. Finlande −1,5 à Saint-Marin — 79 %

**Value (mise réduite)**
- New England gagne à Salt Lake @2,72 — modèle 48 %, EV +30 %
- Charlotte gagne contre Chicago @2,24 — modèle 53 %, EV +18 %
- Bulgarie gagne contre le Luxembourg @2,48 — modèle 45 %, EV +12 %

**À éviter** : toute League Two en simple à grosse mise (pas de cotes, petit échantillon), et le 1X2 sec d'Angleterre–Espagne.

*Ces pronostics sont des estimations probabilistes, pas des certitudes. Ne misez que ce que vous pouvez perdre.*
