# Analyse APEX — 55 matchs du dimanche 20 septembre 2026

## Méthodologie — identique à celle du 19/09, avec une base de données rafraîchie

Même pipeline qu'hier, mais **les résultats du 19 septembre sont désormais intégrés** : chaque équipe a une journée de plus dans son échantillon, les baselines de ligue ont été recalculées, et les notes d'attaque/défense ont bougé.

- **soccerstats.com** : 16 pages de championnat re-scrapées → **776 matchs joués** cette saison servent de base au modèle, et les 55 affiches du jour ont été extraites directement des calendriers (aucune liste fournie à la main).
- **odds-radar.com** : cotes 1X2 fraîches du 20/09. Marge toujours ~0 %. **30 des 55 matchs ont une cote publique**, les 25 autres non (Portugal, Écosse, Turquie, Danemark, D2 espagnole/italienne, Eerste Divisie, une partie de la 2. Bundesliga).
- Modèle : Poisson bivarié Dixon-Coles (ρ = −0,08), shrinkage bayésien vers la moyenne de ligue (K = 4,5), baseline par championnat, puis fusion 62/38 avec le marché là où il existe, total de buts piloté à 50/50 par le modèle.

**Les mêmes limites qu'hier s'appliquent** : pas de xG, pas de compositions, pas de blessures. Et l'échantillon reste court (4 à 7 journées selon les ligues) — la Bundesliga n'en est qu'à 33 matchs joués, c'est la ligue la moins fiable du lot.

---

## 🏴 Premier League

**1. Manchester City – Sunderland** · λ 2,05–0,68 · cotes 1,36 / 5,90 / 10,0
City est à **4/4, 8 buts marqués pour 2 encaissés**. Sunderland a 4 points et vient de gagner 1-0, mais reste une équipe à 3 buts inscrits en 4 journées. Probabilité de clean sheet pour City : 51 %, probabilité de victoire par 2 buts ou plus : 44 %.
→ **Score anticipé 2-0** · **Pari : Manchester City gagne (69 %)** · Confiance **Élevée** · *La cote de 1,36 est serrée : la ligne préférée du modèle est City −1 (44 %)*

**2. Leeds Utd – Crystal Palace** · λ 2,15–1,13 · cotes 1,76 / 4,17 / 5,26
Leeds est invaincu avec 8 points et **5 buts marqués pour 2 encaissés à domicile** (4-1 au dernier match). Palace a 3 points, a encaissé 11 buts en 4 journées et reste sur trois défaites avant un 3-2. Le modèle donne Leeds à 65 %, le marché à 57 % — rare cas où mon estimation est **plus** favorable au favori que le marché.
→ **Score anticipé 2-1** · **Pari : Leeds gagne (60 %)** · Confiance **Moyenne-Élevée**

**3. Bournemouth – Liverpool** · λ 1,34–1,59 · cotes 3,25 / 3,90 / 2,26
Deux équipes en mode « match nul industriel » : Bournemouth reste sur 3 nuls, Liverpool sur 3 nuls en 4 journées. Aucune des deux ne gagne beaucoup, aucune ne perd beaucoup. Modèle et marché sont alignés au point près (31/26/42 contre 31/25/44).
→ **Score anticipé 1-1** · **Pari : Double chance X2 (69 %)** · Confiance **Moyenne**

**4. Fulham – Manchester Utd** · λ 1,42–1,74 · cotes 3,65 / 4,00 / 2,08
Fulham est 19e ou 20e avec **1 point et 6 buts encaissés en 2 matchs à domicile**. United est instable (4 points) mais vient de planter 5 buts. Le marché installe United à 48 %, le modèle brut à 39 %. Les deux défenses fuient : BTTS à 63 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (63 %)** · Confiance **Moyenne** · *Angle value : Fulham à 3,65 (+12 % d'EV)*

---

## 🇮🇹 Serie A

**5. Frosinone – Como** · λ 1,10–1,95 · cotes 6,80 / 4,90 / 1,54
Como a 10 points et **7 buts marqués pour 3 encaissés en 3 déplacements** — la meilleure équipe à l'extérieur de la Serie A. Frosinone a pourtant 7 points et n'est pas ridicule (7:4). Le marché met Como à 65 %, le modèle brut à 41 % : écart considérable pour un promu contre un promu.
→ **Score anticipé 1-2** · **Pari : Double chance X2 (80 %)** · Confiance **Moyenne-Élevée** · *Contrarien : Frosinone à 6,80 est la cote la plus « gonflée » du coupon selon le modèle*

**6. AC Milan – Lecce** · λ 2,14–0,76 · cotes 1,29 / 6,40 / 13,50
Milan est invaincu (8 points) et solide chez lui. Lecce a 6 points mais a pris un 0-4 et n'a marqué que 5 buts. Probabilité que Lecce ne marque pas : 47 %. Victoire de Milan par 2 buts ou plus : 45 %.
→ **Score anticipé 2-0** · **Pari : AC Milan gagne (68 %)** · Confiance **Élevée** · *À 1,29 le prix est tendu — la ligne à valeur est Milan −1*

**7. Juventus – Atalanta** · λ 1,64–0,96 · cotes 1,76 / 3,95 / 5,40
La Juve a 7 points et **3 buts marqués pour 1 encaissé à domicile**, mais vient de perdre 2-3. Atalanta est en redressement (2 victoires) tout en restant à 5 buts marqués en 4 matchs. Le marché est plus enthousiaste sur la Juve (56 %) que le modèle (45 %). Match fermé attendu : total 2,60.
→ **Score anticipé 1-1** · **Pari : Juventus 1X (79 %)** · Confiance **Élevée**

**8. Fiorentina – Napoli** · λ 1,30–1,72 · cotes 3,45 / 3,45 / 2,38
La Fiorentina est en crise : 3 points et **11 buts encaissés en 4 journées**, dont 1 but marqué pour 5 encaissés à domicile. Naples reste sur 2 victoires à zéro encaissé. Convergence nette modèle/marché sur l'avantage napolitain.
→ **Score anticipé 1-2** · **Pari : Double chance X2 (72 %)** · Confiance **Moyenne-Élevée**

**9. Parma – Genoa** · λ 1,07–1,07 · cotes 3,30 / 3,10 / 2,68
Les deux équipes ont **1 point**, 2 buts marqués chacune, 6 et 8 encaissés. Symétrie parfaite (34/32/34) et stérilité offensive des deux côtés : total projeté 2,14, le plus bas de Serie A ce week-end.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (64 %)** · Confiance **Moyenne-Élevée**

---

## 🇪🇸 La Liga

**10. Atletico Madrid – Real Madrid** · λ 1,53–1,70 · cotes 3,60 / 4,00 / 2,14
Le derby. Atlético : 13 points, 14 buts, et **8 buts marqués pour 2 encaissés au Metropolitano**. Real : 15 points, 17 buts. Les deux ont pris une gifle cette saison (0-3 pour l'Atleti, 0-1 pour le Real). Le marché donne le Real à 47 %, le modèle brut donne l'Atlético à 44 % — l'avantage du terrain est manifestement sous-évalué ici. Les deux attaques tournent : BTTS 65 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (65 %)** · Confiance **Moyenne-Élevée** · *Value : Atlético à 3,60 (+22 % d'EV)*

**11. Deportivo – Real Betis** · λ 1,23–1,43 · cotes 3,85 / 3,85 / 2,06
Le Betis a 15 points mais **8 buts marqués en 6 matchs** : ses 5 victoires sont des 1-0, 1-0, 1-0, 2-1, 1-0. C'est une machine à gagner petit, pas une machine à marquer. Deportivo est invaincu à domicile ou presque (9 points, 4:3). Le marché sur-cote le Betis.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (72 %)** · Confiance **Moyenne** · *Value : Deportivo à 3,85 (+21 % d'EV)*

**12. Valencia – Real Sociedad** · λ 1,12–1,25 · cotes 2,94 / 3,45 / 2,70
Valence est en détresse offensive : **2 buts marqués en 6 matchs, 0 sur ses 3 matchs à domicile**, et un 0-5 encaissé. La Real Sociedad n'est guère mieux (6 buts, 11 encaissés) mais vient de gagner deux fois. Total projeté 2,37.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (58 %)** · Confiance **Moyenne**

**13. Villarreal – Levante** · λ 2,06–1,14 · cotes 1,56 / 5,00 / 6,25
Villarreal est irrégulier (5 points) mais vient de gagner 3-1. Levante est le cas le plus frappant de la journée : **0 but marqué sur ses 3 déplacements** (0-3, 0-0, 0-0). Le marché le sait (64 % Villarreal), le modèle est plus prudent (47 %) à cause du bilan global médiocre du Sous-marin jaune.
→ **Score anticipé 2-1** · **Pari : Villarreal 1X (80 %)** · Confiance **Élevée**

**14. Getafe – Malaga** · λ 1,28–0,75 · cotes 2,08 / 3,30 / 4,62
Duel d'anti-jeu : Getafe 3 buts marqués en 6 matchs, Malaga 3 buts et **1 but marqué pour 7 encaissés en 3 déplacements**. Total projeté **2,03** — un des trois plus bas de la journée. Clean sheet Getafe : 47 %.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (67 %)** · Confiance **Élevée**

---

## 🇫🇷 Ligue 1

**15. Nice – Lille** · λ 0,85–1,30 · cotes 3,60 / 3,57 / 2,24
Nice est à **2 points et 1 seul but marqué en 4 journées**. Lille a 10 points et **3 buts marqués pour 0 encaissé en déplacement**. Le modèle donne Lille à 50 %, le marché à 44 %. Mais surtout : total projeté 2,15, aucune des deux équipes ne produit de buts.
→ **Score anticipé 0-1** · **Pari : Under 2,5 buts (64 %)** · Confiance **Moyenne-Élevée** · *Alternative : X2 à 77 %*

**16. Auxerre – Brest** · λ 1,41–1,69 · cotes 3,20 / 3,60 / 2,40
Auxerre a encaissé **11 buts en 4 journées** (1-3, 1-3, 2-5). Brest est le roi du 2-2 (deux nuls à deux buts partout). Les deux équipes marquent et encaissent systématiquement : BTTS 62 %, Over 1,5 à 82 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (62 %)** · Confiance **Moyenne**

**17. Marseille – Paris SG** · λ 1,24–2,04 · cotes 7,50 / 5,60 / 1,47
Le Classique. Situation statistiquement étrange : **l'OM a 3 points et le PSG 5** — aucun des deux n'a démarré sa saison. L'OM vient de gagner 4-0 à domicile ; le PSG reste sur trois matchs sans victoire avant un 1-0. Le marché applique un prix de réputation (PSG à 69 %) que les résultats de cette saison ne soutiennent pas du tout : le modèle brut donne l'OM à 44 %. Je n'ai pas assez d'information (compositions, blessures) pour parier contre le marché sur un match de cette ampleur, mais je signale l'écart.
→ **Score anticipé 1-2** · **Pari : Over 1,5 buts (85 %)** · Confiance **Élevée** · *⚠️ Le plus gros écart modèle/marché de la journée : OM à 7,50 affiche +65 % d'EV théorique. À traiter comme un ticket spéculatif, pas comme une conviction*

---

## 🇩🇪 Bundesliga
⚠️ *33 matchs joués seulement (4 journées) — ligue la moins fiable du coupon, priorité aux marchés de buts.*

**18. Leverkusen – RB Leipzig** · λ 2,14–1,59 · cotes 1,96 / 4,50 / 3,75
Leverkusen a gagné 4-0 son dernier match à domicile ; Leipzig reste sur 3-0 et 5-0. Deux attaques en feu, deux défenses qui prennent l'eau (5 et 3 encaissés en 3 matchs). Total projeté 3,73 — le plus élevé de Bundesliga. Over 2,5 à 72 %, BTTS à 71 %.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (72 %)** · Confiance **Élevée**

**19. Schalke 04 – Elversberg** · λ 1,84–1,44 · cotes 2,08 / 4,00 / 3,90
Schalke alterne 0-0, 0-3 et 3-1. Elversberg vient de gagner 4-3 et 3-2 : **toutes ses rencontres produisent des buts des deux côtés**. BTTS à 65 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (65 %)** · Confiance **Moyenne**

**20. Paderborn – Hoffenheim** · λ 1,27–1,79 · cotes 4,54 / 4,60 / 1,78
Paderborn est à **1 point et 0 but marqué en 3 journées**. Hoffenheim n'est pas brillant non plus (3 points, 6:7) et le marché le met à 56 % là où le modèle n'en voit que 37 %. Le point d'accord : ça va marquer (Over 1,5 à 82 %).
→ **Score anticipé 1-2** · **Pari : Over 1,5 buts (82 %)** · Confiance **Moyenne-Élevée**

---

## 🇳🇱 Eredivisie

**21. FC Twente – PSV Eindhoven** · λ 1,57–1,89 · cotes 3,05 / 4,20 / 2,25
Le PSV écrase la ligue : **16 points, 22 buts marqués, et 11 buts pour 3 encaissés en 3 déplacements**. Mais Twente est 2e avec 13 points et un solide 6:1 à domicile. Choc entre deux attaques : total 3,46, Over 2,5 à 67 %.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (67 %)** · Confiance **Moyenne-Élevée**

**22. AZ Alkmaar – Telstar** · λ 2,55–0,83 · cotes 1,27 / 7,14 / 12,0
AZ est en tête avec 16 points et 17 buts. Telstar a 5 points, 11 encaissés, et n'a jamais gagné à l'extérieur. Victoire d'AZ par 2 buts ou plus : **53 %** — c'est la ligne de handicap la mieux soutenue de la journée, et à un prix bien plus intéressant que le 1,27 du 1X2.
→ **Score anticipé 2-0** · **Pari : AZ Alkmaar −1 (gagne par 2 buts ou plus, 53 %)** · Confiance **Moyenne-Élevée**

**23. Feyenoord – FC Utrecht** · λ 3,09–1,34 · cotes 1,34 / 6,25 / 10,0
Le plus gros λ de la journée. Feyenoord : 14 points, **20 buts marqués**, et vient de gagner **7-0**. Utrecht a encaissé **19 buts en 6 matchs** (1-6, 1-4, 3-3, 3-3). Probabilité qu'Utrecht garde sa cage inviolée : 5 %. Total projeté 4,43.
→ **Score anticipé 3-1** · **Pari : Over 2,5 buts (82 %)** · Confiance **Élevée**

**24. NEC Nijmegen – Go Ahead Eagles** · λ 2,14–1,96 · cotes 2,14 / 4,20 / 3,30
Deux équipes qui ne savent pas défendre : NEC 12 encaissés, Go Ahead 13 — et 15 buts marqués pour le visiteur. Total projeté **4,10**, BTTS 76 %, Over 2,5 à 78 %. Le marché sur-cote NEC (46 % contre 35 % au modèle).
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (78 %)** · Confiance **Élevée** · *Value : Go Ahead Eagles à 3,30 (+19 % d'EV)*

---

## 🇵🇹 Liga Portugal
*(aucune cote publique)*

**25. FC Porto – Benfica** · λ 1,32–1,52
Le Clássico, et un duel de monstres statistiques. Porto : **6 victoires en 6, 15 buts marqués, 2 encaissés**. Benfica : 16 points, **21 buts marqués, 4 encaissés, et 14 buts pour 0 encaissé en 3 déplacements** (dont un 7-0). Les deux profils s'annulent presque parfaitement : le modèle sort 32 / 27 / 41, avec un léger avantage à Benfica dû à son rendement extérieur hallucinant. Sans cotes ni compositions sur un match de ce calibre, prendre un camp serait de la devinette.
→ **Score anticipé 1-1** · **Pari : Over 1,5 buts (78 %)** · Confiance **Moyenne**

**26. Santa Clara – Sporting Braga** · λ 1,59–1,02
Santa Clara est la surprise du championnat : **14 points, 7 buts marqués pour 2 encaissés à domicile**. Braga a 10 points et gagne petit (1-0, 2-1, 1-0). Match fermé attendu, avantage au terrain.
→ **Score anticipé 1-1** · **Pari : Santa Clara 1X (77 %)** · Confiance **Moyenne-Élevée**

**27. Estrela Amadora – Academico Viseu** · λ 1,89–1,48
Estrela est l'équipe la plus « ouverte » du Portugal : **14 buts marqués, 12 encaissés en 6 matchs**, avec 2-2, 2-2, 3-3, 3-2. Viseu marque aussi régulièrement. Total projeté 3,37 contre 2,81 de moyenne portugaise.
→ **Score anticipé 2-1** · **Pari : Les deux équipes marquent — Oui (66 %)** · Confiance **Moyenne**

**28. Guimaraes – Moreirense** · λ 1,33–1,05
Derby du Minho entre deux équipes décevantes : Guimarães 4 points (1 but marqué en 3 matchs à domicile), Moreirense 7 points mais **13 buts encaissés**. Match crispé attendu, total 2,38.
→ **Score anticipé 1-1** · **Pari : Guimaraes 1X (72 %)** · Confiance **Moyenne**

**29. Estoril – Casa Pia** · λ 1,20–0,77
Le match le plus pauvre de la journée. Estoril : **2 points, 2 buts marqués en 6 journées**. Casa Pia : **1 point, 1 but marqué, 15 encaissés** — dont un 0-7. Total projeté **1,97**, le plus bas des 55 matchs. Clean sheet Estoril : 46 %.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (69 %)** · Confiance **Élevée**

---

## 🏴󠁧󠁢󠁳󠁣󠁴󠁿 Scottish Premiership
*(aucune cote publique)*

**30. Celtic – Rangers** · λ 1,43–0,84
L'Old Firm. Celtic est parfait : **6/6, 14 buts marqués, 3 encaissés, 6 buts pour 1 encaissé au Celtic Park**. Les Rangers ont 13 points mais gagnent exclusivement 1-0 (quatre victoires 1-0 ou 2-1) avec seulement **7 buts marqués en 6 matchs**. Deux défenses d'élite, une attaque des Rangers en panne → derby fermé attendu : total 2,27, Under 3,5 à 81 %.
→ **Score anticipé 1-0** · **Pari : Celtic 1X (79 %)** · Confiance **Élevée** · *⚠️ Sans cotes ni compositions sur un derby de cette intensité, la volatilité réelle dépasse celle du modèle*

---

## 🇳🇱 Eerste Divisie

**31. De Graafschap – NAC Breda** · λ 1,81–2,32
De Graafschap a encaissé **18 buts en 6 matchs** (0-5, 1-4, 2-5). NAC marque 11 buts mais en encaisse 14. Total projeté **4,13**, BTTS 76 %.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (78 %)** · Confiance **Élevée**

---

## 🇩🇪 2. Bundesliga
*(aucune cote publique)*

**32. Bielefeld – Heidenheim** · λ 1,91–1,77
Heidenheim est en forme (12 points, 4 victoires) mais encaisse beaucoup (10 buts en 5), avec des 4-3 et 5-3. Bielefeld est irrégulier. Total 3,68, BTTS 71 %, Over 2,5 à 71 %.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (71 %)** · Confiance **Élevée**

**33. Energie Cottbus – Sankt Pauli** · λ 2,08–1,74
Cottbus est la définition du chaos : **13 buts marqués, 12 encaissés en 5 matchs** (4-4, 3-4, 3-1, 3-0). St. Pauli enchaîne les nuls à buts (1-1, 2-2, 2-2). Total projeté **3,82**.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (73 %)** · Confiance **Élevée**

**34. Hannover 96 – Bochum** · λ 1,13–1,27
Exception défensive de la 2. Bundesliga : Hanovre 5 buts en 5 matchs, Bochum **4 buts marqués et 4 encaissés en 5 matchs**. Total projeté 2,40 contre 3,14 de moyenne de ligue.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (78 %)** · Confiance **Moyenne**

---

## 🇪🇸 LaLiga Hypermotion (D2)
*(aucune cote publique — ligue la moins prolifique, 2,57 buts/match)*

**35. Las Palmas – Burgos** · λ 1,30–1,10
Las Palmas a 10 points, Burgos 8, les deux avec des bilans de buts quasi identiques. Burgos n'a marqué **aucun but sur ses 2 déplacements**. Match serré et peu prolifique.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (78 %)** · Confiance **Moyenne**

**36. Almeria – Celta Vigo B** · λ 1,62–1,01
Almeria est solide chez elle (**6 buts pour 2 encaissés en 2 matchs**) et vient d'enchaîner deux victoires. Celta B est une équipe réserve à 7 points, battue 0-4 en cours de saison.
→ **Score anticipé 1-1** · **Pari : Almeria 1X (77 %)** · Confiance **Moyenne-Élevée**

**37. Sabadell – Real Oviedo** · λ 0,80–0,98
**Le match le plus fermé des 55 : total projeté 1,78.** Oviedo a encaissé **1 seul but en 5 matchs** (0-0, 0-1, 1-0, 0-0, 3-0) ; Sabadell a marqué 4 buts en 5 matchs, dont trois 0-0. Probabilité de 0-0 : 17,8 % — le score le plus probable de tout le coupon.
→ **Score anticipé 0-0** · **Pari : Under 2,5 buts (73 %)** · Confiance **Élevée**

**38. Leganes – Granada** · λ 1,04–0,92
Même registre : Leganés 3 buts marqués et 3 encaissés en 5 matchs, Granada 6 et 5. Total 1,96, Under 2,5 à 69 %.
→ **Score anticipé 0-0** · **Pari : Under 2,5 buts (69 %)** · Confiance **Élevée**

**39. Ceuta – Valladolid** · λ 0,96–1,26
Ceuta est dernier : **0 point, 5 défaites, 2 buts marqués pour 15 encaissés**. Valladolid est faible aussi (4 points, 2 buts) mais vient de gagner 1-0. Avantage visiteur malgré le déplacement le plus long d'Espagne.
→ **Score anticipé 1-1** · **Pari : Double chance X2 (73 %)** · Confiance **Moyenne**

---

## 🇮🇹 Serie B
*(aucune cote publique)*

**40. Mantova – Pisa** · λ 1,76–1,06
Mantova est leader ou presque : **10 points, 8 buts marqués pour 3 encaissés**, 5:1 à domicile. Pisa alterne 3-0 et 1-4. Meilleure asymétrie de Serie B.
→ **Score anticipé 2-1** · **Pari : Mantova 1X (78 %)** · Confiance **Élevée**

**41. Arezzo – Sudtirol** · λ 1,10–1,55
Arezzo vient de prendre **0-5** et a encaissé 9 buts en 4 matchs. Südtirol a 8 points avec **2 buts encaissés seulement** — la meilleure défense de la ligue. Avantage visiteur net (47 % contre 26 %).
→ **Score anticipé 1-1** · **Pari : Double chance X2 (74 %)** · Confiance **Moyenne-Élevée**

**42. Modena – Empoli** · λ 1,37–0,89
Modène a 7 points et une défense sérieuse (3 encaissés en 4). Empoli n'a marqué que 3 buts en 4 matchs. Total 2,26 — match fermé typique de Serie B.
→ **Score anticipé 1-1** · **Pari : Modena 1X (77 %)** · Confiance **Moyenne**

**43. Hellas Verona – Vicenza** · λ 2,19–1,19
Derby vénète. Verone vient de gagner **5-0** et affiche 6 buts pour 2 encaissés à domicile. Vicenza a encaissé 5 buts sur ses 2 déplacements. Total 3,38, très au-dessus de la moyenne Serie B (2,68).
→ **Score anticipé 2-1** · **Pari : Hellas Verona 1X (81 %)** · Confiance **Élevée**

---

## 🇧🇪 Pro League

**44. Antwerp – Royale Union SG** · λ 1,30–2,57 · cotes 5,75 / 4,50 / 1,64
Union SG domine : **16 points, 19 buts marqués pour 3 encaissés**, et **11 buts pour 3 encaissés en 3 déplacements** (5-1, 3-0, 5-0). Antwerp a encaissé 13 buts en 6 matchs. Modèle (71 %) et marché (61 %) convergent sur Union.
→ **Score anticipé 1-2** · **Pari : Union Saint-Gilloise gagne (64 %)** · Confiance **Moyenne-Élevée**

**45. Club Brugge – KRC Genk** · λ 2,09–1,02 · cotes 1,62 / 4,80 / 6,00
Bruges reste sur **5 victoires consécutives**, 12 buts marqués pour 3 encaissés, dont 7:1 à domicile. Genk est irrégulier (4-4, 4-0, 0-0). Modèle et marché s'accordent à 61 % / 62 % — l'un des alignements les plus propres du jour.
→ **Score anticipé 2-1** · **Pari : Club Brugge 1X (83 %)** · Confiance **Élevée**

**46. Kortrijk – Beveren** · λ 1,19–1,41 · cotes 2,78 / 3,65 / 2,70
Kortrijk est catastrophique : **0 point, 6 défaites, 1 but marqué pour 15 encaissés**. Mais Beveren est bizarre : 9 points avec **1 but marqué pour 10 encaissés à l'extérieur**. Le marché met les deux à égalité ; le modèle brut donne Beveren à 48 %.
→ **Score anticipé 1-1** · **Pari : Double chance X2 (69 %)** · Confiance **Moyenne**

**47. Sint-Truiden – Westerlo** · λ 2,43–1,53 · cotes 1,76 / 4,40 / 5,00
Le match le plus offensif de Belgique : STVV vient de faire 4-1 et 4-0, Westerlo a encaissé **16 buts en 6 matchs** tout en en marquant 13 (1-5, 4-2, 0-4, 4-0). Total projeté **3,96**, Over 2,5 à 76 %.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (76 %)** · Confiance **Élevée**

---

## 🇹🇷 Süper Lig
*(aucune cote publique)*

**48. Amed SK – Besiktas** · λ 1,74–1,23
Amed SK est une forteresse : **10 buts marqués pour 1 encaissé en 3 matchs à domicile** (dont un 5-0). Beşiktaş a 12 points et 4 victoires en 5, mais seulement 2:2 sur ses déplacements. Le terrain compense largement l'écart de niveau.
→ **Score anticipé 1-1** · **Pari : Amed SK 1X (74 %)** · Confiance **Moyenne**

**49. Goztepe – Rizespor** · λ 1,26–1,60
Göztepe a **2 points en 5 matchs** et encaisse 13 buts. Rizespor a 9 points et **5 buts marqués pour 1 encaissé en 3 déplacements**. Avantage visiteur franc (45 % contre 29 %).
→ **Score anticipé 1-2** · **Pari : Double chance X2 (71 %)** · Confiance **Moyenne-Élevée**

**50. Fenerbahce – Eyupspor** · λ 1,89–0,80
Fenerbahçe reste sur 2 victoires (4-2, 2-0). Eyüpspor est en chute : **3 points, 2 buts marqués en 5 matchs, 0 but sur ses 2 déplacements pour 4 encaissés**. Clean sheet Fener : 45 %.
→ **Score anticipé 2-0** · **Pari : Fenerbahce gagne (62 %)** · Confiance **Moyenne-Élevée**

**51. Erzurum BB – Samsunspor** · λ 1,21–1,48
Deux équipes à 4 points, toutes deux avec une différence de buts très négative (−9 et −5). Erzurum a marqué 2 buts en 5 matchs. Match indécis et peu lisible.
→ **Score anticipé 1-1** · **Pari : Double chance X2 (70 %)** · Confiance **Faible-Moyenne**

---

## 🇩🇰 Superliga
*(aucune cote publique)*

**52. Brondby IF – FC Kobenhavn** · λ 1,24–2,48
Le derby de Copenhague. FCK est en feu : **15 points, 16 buts marqués pour 5 encaissés**, 9:4 en déplacement (5-0, 4-0). Brøndby a 9 points mais **13 buts encaissés**, dont un 0-3 au dernier match à domicile. Le modèle donne FCK à 64 %.
→ **Score anticipé 1-2** · **Pari : FC Kobenhavn gagne (64 %)** · Confiance **Moyenne** · *⚠️ Derby : la volatilité réelle dépasse toujours celle du modèle. Version prudente : X2 à 83 %*

**53. Viborg – Nordsjaelland** · λ 1,36–1,34
Deux équipes en forme et de niveau quasi identique (11 et 13 points). Symétrie presque parfaite : 37 / 28 / 36. Total 2,70.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (71 %)** · Confiance **Moyenne**

**54. AC Horsens – AGF Aarhus** · λ 1,95–1,27
Horsens reste sur **3 victoires consécutives** (2-1, 2-1, 5-2). AGF est dernier ou presque avec **2 points en 6 matchs** et 12 buts encaissés. Renversement de tendance net en faveur du promu.
→ **Score anticipé 2-1** · **Pari : AC Horsens 1X (76 %)** · Confiance **Moyenne-Élevée**

**55. SonderjyskE – Randers FC** · λ 1,10–1,88
SønderjyskE est en grande difficulté : **2 points, 15 buts encaissés, et 2 buts marqués pour 8 encaissés à domicile**. Randers a 9 points et gagne à l'extérieur (3-0). Écart net (55 % contre 21 %).
→ **Score anticipé 1-2** · **Pari : Double chance X2 (79 %)** · Confiance **Moyenne-Élevée**

---
## 📋 RÉCAPITULATIF DES PRONOSTICS

| #   | Match | Score anticipé | Pari recommandé | Confiance |
| --- | ----- | -------------- | --------------- | --------- |
| 1 | Manchester City – Sunderland | 2-0 | Manchester City gagne (69 %) | Élevée |
| 2 | Leeds Utd – Crystal Palace | 2-1 | Leeds gagne (60 %) | Moyenne-Élevée |
| 3 | Bournemouth – Liverpool | 1-1 | Double chance X2 (69 %) | Moyenne |
| 4 | Fulham – Manchester Utd | 1-1 | BTTS Oui (63 %) | Moyenne |
| 5 | Frosinone – Como | 1-2 | Double chance X2 (80 %) | Moyenne-Élevée |
| 6 | AC Milan – Lecce | 2-0 | AC Milan gagne (68 %) | Élevée |
| 7 | Juventus – Atalanta | 1-1 | Juventus 1X (79 %) | Élevée |
| 8 | Fiorentina – Napoli | 1-2 | Double chance X2 (72 %) | Moyenne-Élevée |
| 9 | Parma – Genoa | 1-1 | Under 2,5 buts (64 %) | Moyenne-Élevée |
| 10 | Atletico Madrid – Real Madrid | 1-1 | BTTS Oui (65 %) | Moyenne-Élevée |
| 11 | Deportivo – Real Betis | 1-1 | Under 3,5 buts (72 %) | Moyenne |
| 12 | Valencia – Real Sociedad | 1-1 | Under 2,5 buts (58 %) | Moyenne |
| 13 | Villarreal – Levante | 2-1 | Villarreal 1X (80 %) | Élevée |
| 14 | Getafe – Malaga | 1-0 | Under 2,5 buts (67 %) | Élevée |
| 15 | Nice – Lille | 0-1 | Under 2,5 buts (64 %) | Moyenne-Élevée |
| 16 | Auxerre – Brest | 1-1 | BTTS Oui (62 %) | Moyenne |
| 17 | Marseille – Paris SG | 1-2 | Over 1,5 buts (85 %) | Élevée |
| 18 | Leverkusen – RB Leipzig | 2-1 | Over 2,5 buts (72 %) | Élevée |
| 19 | Schalke 04 – Elversberg | 1-1 | BTTS Oui (65 %) | Moyenne |
| 20 | Paderborn – Hoffenheim | 1-2 | Over 1,5 buts (82 %) | Moyenne-Élevée |
| 21 | FC Twente – PSV Eindhoven | 1-2 | Over 2,5 buts (67 %) | Moyenne-Élevée |
| 22 | AZ Alkmaar – Telstar | 2-0 | AZ −1 (gagne par 2+, 53 %) | Moyenne-Élevée |
| 23 | Feyenoord – FC Utrecht | 3-1 | Over 2,5 buts (82 %) | Élevée |
| 24 | NEC Nijmegen – Go Ahead Eagles | 2-1 | Over 2,5 buts (78 %) | Élevée |
| 25 | FC Porto – Benfica | 1-1 | Over 1,5 buts (78 %) | Moyenne |
| 26 | Santa Clara – Sporting Braga | 1-1 | Santa Clara 1X (77 %) | Moyenne-Élevée |
| 27 | Estrela Amadora – Academico Viseu | 2-1 | BTTS Oui (66 %) | Moyenne |
| 28 | Guimaraes – Moreirense | 1-1 | Guimaraes 1X (72 %) | Moyenne |
| 29 | Estoril – Casa Pia | 1-0 | Under 2,5 buts (69 %) | Élevée |
| 30 | Celtic – Rangers | 1-0 | Celtic 1X (79 %) | Élevée |
| 31 | De Graafschap – NAC Breda | 1-2 | Over 2,5 buts (78 %) | Élevée |
| 32 | Bielefeld – Heidenheim | 2-1 | Over 2,5 buts (71 %) | Élevée |
| 33 | Energie Cottbus – Sankt Pauli | 2-1 | Over 2,5 buts (73 %) | Élevée |
| 34 | Hannover 96 – Bochum | 1-1 | Under 3,5 buts (78 %) | Moyenne |
| 35 | Las Palmas – Burgos | 1-1 | Under 3,5 buts (78 %) | Moyenne |
| 36 | Almeria – Celta Vigo B | 1-1 | Almeria 1X (77 %) | Moyenne-Élevée |
| 37 | Sabadell – Real Oviedo | 0-0 | Under 2,5 buts (73 %) | Élevée |
| 38 | Leganes – Granada | 0-0 | Under 2,5 buts (69 %) | Élevée |
| 39 | Ceuta – Valladolid | 1-1 | Double chance X2 (73 %) | Moyenne |
| 40 | Mantova – Pisa | 2-1 | Mantova 1X (78 %) | Élevée |
| 41 | Arezzo – Sudtirol | 1-1 | Double chance X2 (74 %) | Moyenne-Élevée |
| 42 | Modena – Empoli | 1-1 | Modena 1X (77 %) | Moyenne |
| 43 | Hellas Verona – Vicenza | 2-1 | Hellas Verona 1X (81 %) | Élevée |
| 44 | Antwerp – Royale Union SG | 1-2 | Union Saint-Gilloise gagne (64 %) | Moyenne-Élevée |
| 45 | Club Brugge – KRC Genk | 2-1 | Club Brugge 1X (83 %) | Élevée |
| 46 | Kortrijk – Beveren | 1-1 | Double chance X2 (69 %) | Moyenne |
| 47 | Sint-Truiden – Westerlo | 2-1 | Over 2,5 buts (76 %) | Élevée |
| 48 | Amed SK – Besiktas | 1-1 | Amed SK 1X (74 %) | Moyenne |
| 49 | Goztepe – Rizespor | 1-2 | Double chance X2 (71 %) | Moyenne-Élevée |
| 50 | Fenerbahce – Eyupspor | 2-0 | Fenerbahce gagne (62 %) | Moyenne-Élevée |
| 51 | Erzurum BB – Samsunspor | 1-1 | Double chance X2 (70 %) | Faible-Moyenne |
| 52 | Brondby IF – FC Kobenhavn | 1-2 | FC Kobenhavn gagne (64 %) | Moyenne |
| 53 | Viborg – Nordsjaelland | 1-1 | Under 3,5 buts (71 %) | Moyenne |
| 54 | AC Horsens – AGF Aarhus | 2-1 | AC Horsens 1X (76 %) | Moyenne-Élevée |
| 55 | SonderjyskE – Randers FC | 1-2 | Double chance X2 (79 %) | Moyenne-Élevée |

---

## 🎯 Sélection resserrée — les 10 lignes les plus solides

| Match | Pari | Proba | Pourquoi |
| ----- | ---- | ----- | -------- |
| 23. Feyenoord – Utrecht | Over 2,5 | 82 % | Feyenoord 20 buts, Utrecht 19 encaissés, λ total 4,43 |
| 24. NEC – Go Ahead | Over 2,5 | 78 % | Total projeté 4,10, BTTS 76 % |
| 31. De Graafschap – NAC | Over 2,5 | 78 % | 18 buts encaissés par le local en 6 matchs |
| 47. Sint-Truiden – Westerlo | Over 2,5 | 76 % | Westerlo 16 encaissés, λ total 3,96 |
| 18. Leverkusen – Leipzig | Over 2,5 | 72 % | Deux attaques en feu, deux défenses ouvertes |
| 37. Sabadell – Oviedo | Under 2,5 | 73 % | λ total 1,78, le plus bas du coupon — Oviedo 1 but encaissé en 5 |
| 38. Leganes – Granada | Under 2,5 | 69 % | λ total 1,96, deux attaques à l'arrêt |
| 29. Estoril – Casa Pia | Under 2,5 | 69 % | 3 buts marqués à eux deux en 12 matchs |
| 45. Club Brugge – Genk | Club Brugge 1X | 83 % | 5 victoires de rang, 7:1 à domicile |
| 43. Hellas Verona – Vicenza | Verona 1X | 81 % | 5-0 au dernier match, Vicenza 5 encaissés en 2 déplacements |

## 💰 Value bets (là où des cotes existent)

| Match | Pari | Cote | EV modèle | Lecture |
| ----- | ---- | ---- | --------- | ------- |
| 13. Villarreal – Levante | Levante (2) | 6,25 | +23 % | Levante 0 but en 3 déplacements — le modèle doute quand même de Villarreal |
| 10. Atlético – Real Madrid | Atlético (1) | 3,60 | +22 % | Avantage du Metropolitano sous-évalué (8:2 à domicile) |
| 11. Deportivo – Betis | Deportivo (1) | 3,85 | +21 % | Le Betis gagne 1-0 en série, il ne domine pas |
| 24. NEC – Go Ahead | Go Ahead (2) | 3,30 | +19 % | Meilleure attaque des deux, marché trop favorable à NEC |
| 20. Paderborn – Hoffenheim | Paderborn (1) | 4,54 | +19 % | Marché à 56 % sur un Hoffenheim à 3 points |
| 7. Juventus – Atalanta | Atalanta (2) | 5,40 | +15 % | Marché à 56 % sur une Juve qui vient de perdre |

**Tickets spéculatifs** (EV théorique très élevé mais divergence modèle/marché > 30 points — à traiter comme des loteries, pas des convictions) : Marseille à 7,50 (+65 %), Frosinone à 6,80 (+37 %), Lecce à 13,50 (+53 %), Utrecht à 10,0 (+30 %).

---

## ⛔ Inchangé depuis hier

- **Aucun pari joueur** : les screeners Odds-Radar (buteur, passeur, tirs, cartons) exigent une session authentifiée. Vos identifiants n'ont pas été utilisés.
- **Pinnacle** toujours inaccessible (redirection 301).
- **Pas de xG, pas de compositions, pas de blessures** — les projections reposent sur les buts réels uniquement.
- **25 des 55 matchs n'ont aucune cote publique** : Portugal, Écosse, Turquie, Danemark, D2 espagnole/italienne, Eerste Divisie et une partie de la 2. Bundesliga. Sur ceux-là, le modèle tourne sans garde-fou — d'où les confiances plafonnées.
