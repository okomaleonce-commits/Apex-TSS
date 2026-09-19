# Analyse APEX — 63 matchs du 19-20 septembre 2026

## Méthodologie (transparence complète)

**Sources réellement exploitées**
- `soccerstats.com` — les 63 pages de match fournies ont été scrapées (curl ; WebFetch renvoie 403 sur ce domaine), plus les 16 pages `latest.asp` de championnat pour reconstituer **l'intégralité des résultats joués cette saison** (718 matchs au total).
- `odds-radar.com/calendar` — cotes 1X2 récupérées en accès libre (aucune connexion nécessaire). **Marge bookmaker mesurée : 0,0 à 0,5 %** → ce sont des cotes quasi-justes, donc un signal d'information très fort.
- Pinnacle : redirection 301, pas de données exploitables. fbref / zonestat / oddshub : pas de couverture de cette saison exploitable automatiquement.
- Les écrans `odds-radar.com/screener/*` (buteur, assist, tir, carton) nécessitent une session authentifiée — non accessibles en scraping ; **aucun pari joueur n'est donc proposé ici**.

**Modèle**
1. Reconstitution des attaques/défenses de chaque équipe sur toute la saison en cours (buts marqués/encaissés, splits domicile/extérieur).
2. Régression bayésienne forte vers la moyenne de ligue (K = 4,5 matchs fictifs). **Indispensable : on est à la 4e-7e journée, les échantillons sont minuscules.**
3. Baseline propre à chaque ligue (total de buts + avantage du terrain), elle-même régressée vers un prior générique.
4. Poisson bivarié avec correction **Dixon-Coles** (ρ = −0,08) pour les scores faibles.
5. **Ancrage marché** : là où les cotes existent, les probabilités du modèle sont fusionnées avec celles du marché (62 % marché / 38 % modèle), puis les λ sont refittés. Le total de buts reste piloté à 50/50 par le modèle.

**Limites à connaître**
- 32 matchs sur 63 sont dans des ligues **sans cotes publiques** (Portugal, Écosse, Turquie, Danemark, D2 allemande/française/espagnole/italienne/néerlandaise) : le modèle y est seul, sans garde-fou marché → **confiance mécaniquement plus basse**.
- Aucune donnée de compositions, blessures ou suspensions n'était disponible. Un forfait majeur peut invalider n'importe quelle ligne.
- Le « score exact » est le mode de la distribution : il pèse typiquement 8-15 % de probabilité. **C'est une indication de scénario, pas un pari conseillé.**

---
## 🏴 Premier League

**1. Tottenham – Aston Villa** · λ 1,22–0,80 · cotes 1,97 / 3,85 / 4,20
Le duel des deux pires attaques du championnat : **Tottenham a marqué 0 but en 4 journées**, Aston Villa 1. Spurs restent sur 0-0, 0-0, 0-2 ; Villa sur 0-0, 0-4, 1-2. Le marché fait de Tottenham un favori à 50 % — mon modèle brut n'en voyait que 37 %, mais sur l'exigence « marquer », les deux équipes sont à l'arrêt. Total attendu 2,0 buts contre 2,85 de moyenne PL.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (67 %)** · Confiance **Élevée**

**2. Brighton – Arsenal** · λ 1,15–1,65 · cotes 5,26 / 4,10 / 1,77
Arsenal est à 4/4, 8 buts marqués, **1 encaissé** — la meilleure défense. Mais Brighton a inscrit 13 buts en 4 matchs (4-0, 5-0) et le marché le sous-cote à 19 % là où les buts bruts en donnent 38 %. C'est la plus grosse divergence modèle/marché du coupon. Prudence : l'échantillon de Brighton est gonflé par deux cartons contre des équipes faibles.
→ **Score anticipé 1-2** · **Pari : Double chance X2 (74 %)** · Confiance **Moyenne** · *Angle value alternatif : Brighton 1X, le modèle y voit un net excès de cote*

**3. Everton – Ipswich Town** · λ 1,95–1,18 · cotes 1,87 / 3,95 / 4,70
Everton est invaincu (1 victoire, 3 nuls) mais n'a marqué que 5 buts ; Ipswich est chaotique : 7 marqués, **10 encaissés**, avec des 2-5 et 3-2. Modèle et marché sont ici parfaitement alignés (55 % vs 53 %), ce qui renforce la lecture. Le vrai edge est sur les buts : Ipswich ne fait jamais 0-0.
→ **Score anticipé 2-1** · **Pari : Everton 1X (78 %)** · Confiance **Élevée**

**4. Newcastle Utd – Hull City** · λ 1,70–1,17 · cotes 1,65 / 4,50 / 5,70
Piège potentiel. Le marché installe Newcastle à 60 % alors que les chiffres bruts donnent **Hull devant (42 %)** : Newcastle a encaissé 8 buts en 4 matchs (4-4 à domicile), Hull en a encaissé 2 et compte 8 points. Divergence trop forte pour toucher au 1X2 avec confiance — je me replie sur le marché des buts, cohérent des deux côtés.
→ **Score anticipé 2-1** · **Pari : Over 1,5 buts (79 %)** · Confiance **Moyenne** · *Angle upset : Hull X2 à 51 % pour une cote implicite ~2,00*

**5. Nottm Forest – Coventry City** · λ 1,68–0,70 · cotes 1,70 / 4,17 / 5,90
Coventry est **0 point, 0 but marqué, 10 encaissés en 4 matchs** — dernier avec une différence de −10. Forest n'est pas flamboyant (5 pts, 0 but inscrit à domicile) mais n'a pas besoin de l'être. Probabilité que Coventry ne marque pas : 49 %. Modèle et marché s'accordent à 60 % / 59 %.
→ **Score anticipé 2-0** · **Pari : Nottm Forest gagne (60 %)** · Confiance **Moyenne-Élevée**

---

## 🇮🇹 Serie A

**6. Bologna – Torino** · λ 1,36–0,98 · cotes 2,00 / 3,45 / 4,80
Match de bas de tableau entre deux équipes en crise : Bologna 1 point (2 buts en 4), Torino 3 points et 7 encaissés. Aucune des deux n'a la puissance de feu pour ouvrir le match. Total attendu 2,34 contre 2,94 en Serie A.
→ **Score anticipé 1-1** · **Pari : Double chance 1X (74 %)** · Confiance **Moyenne** · *Le modèle brut voit Torino à 35 % contre 21 % implicite : cote 4,80 statistiquement intéressante en petite mise*

**7. Udinese – Cagliari** · λ 1,34–1,27 · cotes 2,32 / 3,35 / 3,65
Cagliari a 9 points avec une défense verrouillée (2 encaissés en 4) et 3 victoires de rang, Udinese encaisse 2,5 buts par match (8:10). Le marché donne pourtant Udinese favori à 43 %. Sur les buts encaissés/marqués purs, **Cagliari domine ce matchup**.
→ **Score anticipé 1-1** · **Pari : Double chance X2 — Cagliari ou nul (62 %)** · Confiance **Moyenne**

**8. AS Roma – Inter Milan** · λ 1,75–1,45 · cotes 2,78 / 3,70 / 2,74
Choc au sommet : **les deux à 4/4 et 12 points**. Roma affiche 12 buts pour **1 encaissé** — statistiquement la meilleure équipe du coupon en différentiel. Inter marque plus (13) mais encaisse 6. Le marché les met à égalité ; le modèle brut donne Roma à 57 %. L'attaque des deux côtés garantit du spectacle (BTTS 64 %).
→ **Score anticipé 2-1** · **Pari : AS Roma 1X (69 %)** · Confiance **Moyenne** · *Value : Roma à 2,78 est la meilleure cote du coupon selon le modèle (+22 % d'EV)*

**9. Venezia – Lazio** · λ 1,12–1,80 · cotes 4,00 / 3,75 / 2,08
Venezia est **0 point, 4 buts marqués, 11 encaissés** — dernier. La Lazio est à 10 points, invaincue, 3 encaissés seulement. Modèle et marché convergent nettement (52 % vs 48 %) : c'est un cas rare où les deux lectures pointent dans la même direction.
→ **Score anticipé 1-2** · **Pari : Lazio gagne (52 % à 2,08)** · Confiance **Moyenne-Élevée**

---

## 🇪🇸 La Liga

**10. Osasuna – Rayo Vallecano** · λ 1,54–1,42 · cotes 2,63 / 3,40 / 3,00
Deux équipes à 7 points en 6 journées, aux profils jumeaux : Osasuna 5:12, Rayo 10:15. Les deux ont commencé catastrophiquement puis enchaîné deux victoires. Défenses très perméables des deux côtés → le match le plus « ouvert » de La Liga ce week-end. 1X2 quasi impossible à départager (40/26/34).
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (61 %)** · Confiance **Moyenne**

**11. Athletic Bilbao – Alaves** · λ 1,66–1,03 · cotes 1,68 / 4,10 / 6,00
Derby basque. Le marché plante l'Athletic à 59 % alors que les chiffres bruts donnent **Alaves légèrement devant** (10 points contre 7, 11 buts marqués contre 7). Le marché voit manifestement quelque chose que les buts ne disent pas (qualité d'effectif, forme récente d'Alaves : deux défaites de suite). Je suis le marché mais en couvrant le nul.
→ **Score anticipé 1-0** · **Pari : Athletic 1X (77 %)** · Confiance **Moyenne-Élevée**

**12. Celta Vigo – Racing** · λ 1,70–1,15 · cotes 1,86 / 4,00 / 4,60
Celta n'a gagné aucun de ses 6 matchs (4 nuls, 3 buts marqués) — mais Racing est encore pire à l'extérieur : **4 buts marqués, 11 encaissés en 3 déplacements**, dont un 2-7. Le nul est un vrai risque vu la stérilité de Celta, d'où la double chance plutôt que le 1 sec.
→ **Score anticipé 2-1** · **Pari : Celta Vigo 1X (75 %)** · Confiance **Moyenne**

**13. Sevilla FC – FC Barcelona** · λ 1,07–2,82 · cotes 11,50 / 7,50 / 1,30
Le match le plus déséquilibré du coupon. Barcelone : **6 victoires en 6, 28 buts marqués, 6 encaissés**, et à l'extérieur **14 buts marqués contre 2 encaissés en 3 déplacements** (5-0, 5-0, 4-2). Séville tient 13 points mais avec seulement 9 buts. La cote de 1,30 n'offre aucune valeur ; le vrai edge est sur le volume de buts, où Barcelone garantit l'Over presque seul.
→ **Score anticipé 1-3** · **Pari : Over 2,5 buts (75 %)** · Confiance **Élevée**

---

## 🇫🇷 Ligue 1

**14. Paris FC – Strasbourg** · λ 1,85–1,18 · cotes 1,92 / 4,00 / 4,35
Paris FC est solide : 8 points, 6 buts marqués pour 2 encaissés, **3-0 à domicile**. Strasbourg est volatil (9:8, avec un 0-4 et un 6-2). Modèle et marché sont alignés au point près (52 % vs 52 %) — configuration la plus fiable du lot français.
→ **Score anticipé 2-1** · **Pari : Paris FC 1X (77 %)** · Confiance **Élevée**

**15. Angers – Troyes** · λ 1,54–1,14 · cotes 2,20 / 3,70 / 3,75
Deux promus/mal classés à 4 points. Angers a une défense correcte mais perd chez lui (1:4 à domicile) ; Troyes encaisse 9 buts en 4 matchs dont un 2-6. Match serré et de faible volume attendu (2,68 buts). Alignement modèle/marché quasi parfait.
→ **Score anticipé 1-1** · **Pari : Angers 1X (73 %)** · Confiance **Moyenne**

**16. Le Mans – Lorient** · λ 1,29–1,34 · cotes 2,96 / 3,50 / 2,66
Le match le plus équilibré du coupon : 35 / 28 / 37. Le Mans reste sur 3 nuls consécutifs, Lorient sur 2 nuls en 4. Six des huit derniers matchs cumulés se sont terminés par un partage. Toucher au 1X2 ici est du pur bruit ; la valeur est sur le plafond de buts.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (73 %)** · Confiance **Moyenne**

**17. Lyon – Rennes** · λ 1,55–1,22 · cotes 2,24 / 3,95 / 3,33
Rennes est invaincu avec 10 points et Lyon en a 8 avec la meilleure défense partagée (2 encaissés en 4). Deux équipes en forme, écart réel très mince. Lyon a l'avantage du terrain mais Rennes marque davantage (8 contre 6).
→ **Score anticipé 2-1** · **Pari : Lyon 1X (71 %)** · Confiance **Moyenne**

**18. Toulouse – Le Havre** · λ 1,44–0,94 · cotes 1,75 / 4,20 / 5,26
Attention : le marché fait de Toulouse un favori à 57 % alors que **Toulouse a 2 points et n'a marqué aucun but à domicile en 2 matchs**. Le Havre est tout aussi faible (2 points, 2 buts). Divergence modèle/marché de 23 points — signal de danger. Je refuse le 1X2 et me place sur le volume, où les deux lectures convergent (2,38 buts attendus).
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (78 %)** · Confiance **Moyenne** · *Angle contrarien : Le Havre X2 à 52 %*

---

## 🇩🇪 Bundesliga

⚠️ *Ligue calibrée sur seulement 28 matchs joués (3 journées) : la baseline mesurée est de 3,96 buts/match, régressée à 3,35. Les projections allemandes sont les moins stables du coupon — d'où la priorité systématique aux marchés de buts plutôt qu'aux résultats.*

**19. E. Frankfurt – Freiburg** · λ 1,66–1,94 · cotes 2,64 / 4,00 / 2,70
Freiburg est à **3/3 avec 10 buts marqués pour 1 encaissé** (dont un 5-0). Francfort a encaissé 4 buts à domicile et joue tous ses matchs à haut score (1-4, 3-1, 3-3). Aucun des 6 matchs cumulés n'a fini sous 2,5 buts.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (70 %)** · Confiance **Moyenne-Élevée**

**20. Hamburger SV – FC Koln** · λ 1,54–1,84 · cotes 2,86 / 3,75 / 2,60
Le HSV est en chute libre : **0 point, 0 but marqué, 12 encaissés** (0-2, 0-5, 0-5). Cologne est médiocre mais fonctionnel. Le marché ne donne Cologne qu'à 38 % — sans doute par respect pour le facteur Volksparkstadion. Le modèle brut donne 53 %. Le point commun aux deux lectures : ça marque beaucoup.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (66 %)** · Confiance **Moyenne**

**21. Monchengladbach – FSV Mainz** · λ 1,54–2,14 · cotes 3,30 / 3,95 / 2,24
Gladbach : **0 point, 3 buts marqués, 12 encaissés** en 3 journées (0-5, 0-3, 3-4). Mayence a gagné 5-0 à l'extérieur. Les six matchs cumulés ont tous produit au moins 3 buts. C'est la ligne « Over » la plus soutenue du coupon.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (71 %)** · Confiance **Élevée**

**22. Werder Bremen – FC Augsburg** · λ 1,76–1,76 · cotes 2,40 / 4,00 / 2,96
Parfaite symétrie (38 / 24 / 38). Augsburg a 7 points et 9 buts marqués, Brême 4 points et une attaque correcte à domicile (3-1). Les deux équipes marquent et encaissent dans presque chaque match. BTTS est le marché le plus propre ici.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (69 %)** · Confiance **Moyenne-Élevée**

**23. Stuttgart – Dortmund** · λ 1,78–1,83 · cotes 2,52 / 4,00 / 2,78
Dortmund est à 3/3 (8:2), Stuttgart alterne 1-5 et 4-1. Les deux attaques tournent, les deux défenses prennent l'eau. Le marché voit un match à peu près nul (39/25/36), le modèle aussi. BTTS à 71 % : la plus haute probabilité BTTS du coupon.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (71 %)** · Confiance **Élevée**

---
## 🇳🇱 Eredivisie

**24. ADO Den Haag – Cambuur** · λ 2,00–1,73 · cotes 2,00 / 4,10 / 3,80
Duel des deux pires défenses de la ligue : ADO a encaissé **16 buts en 6 matchs**, Cambuur 18. ADO reste sur 5 défaites, Cambuur a gagné 3-0 puis fait 2-2. Aucun scénario défensif crédible ici.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (72 %)** · Confiance **Élevée**

**25. Sparta – Heerenveen** · λ 1,65–1,63 · cotes 2,52 / 4,00 / 2,82
Match parfaitement équilibré entre deux équipes de milieu-bas (5 et 6 points). Sparta a fait 3-3 et 2-2 récemment, Heerenveen enchaîne les nuls. Les deux marquent régulièrement sans jamais verrouiller.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (66 %)** · Confiance **Moyenne**

**26. Ajax Amsterdam – Excelsior** · λ 2,67–1,22 · cotes 1,25 / 7,69 / 12,50
Ajax mène avec 13 points et 19 buts (5-1, 5-1, 4-0) mais **a déjà perdu 1-3 à domicile** et Excelsior tient 10 points avec 9 buts marqués en 3 déplacements. Le marché à 79 % est agressif ; le modèle brut n'en voyait que 41 %. La cote de 1,25 ne paie pas ce risque — la valeur est sur les buts.
→ **Score anticipé 2-1** · **Pari : Over 2,5 buts (74 %)** · Confiance **Élevée**

**27. Willem II – Fortuna Sittard** · λ 1,75–2,18 · cotes 2,68 / 3,85 / 2,68
Willem II : **2 points, 6 buts marqués, 19 encaissés** — pire défense du championnat (1-5, 1-4, 1-4, 0-3). Fortuna reste sur 3 victoires et marque 8 buts en 3 déplacements. Le marché les met pourtant à égalité stricte, ce qui est difficilement défendable. Over 2,5 à 75 % et BTTS à 74 %.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (75 %)** · Confiance **Élevée** · *Angle value : Fortuna Sittard à 2,68*

---

## 🇵🇹 Liga Portugal
*(aucune cote publique disponible — projections purement modèle, confiance plafonnée)*

**28. Gil Vicente – Maritimo** · λ 1,42–0,98
Gil Vicente est très solide chez lui (3 matchs, 3 buts marqués, **1 encaissé**, deux clean sheets). Maritimo marque un peu mais encaisse 11 buts en 6 matchs. Ligue à faible volume (2,82 buts/match) et match encore en dessous (2,40).
→ **Score anticipé 1-0** · **Pari : Gil Vicente 1X (75 %)** · Confiance **Moyenne**

**29. Nacional – Famalicao** · λ 1,29–1,29
Équilibre total : 36 / 28 / 36, les deux à 4 points. Famalicão est la machine à nuls du championnat (**4 nuls sur 6**, dont 0-0, 1-1, 2-2, 1-1). Nacional vient de gagner 2-0 après 5 matchs sans victoire. Rien ne justifie de prendre un camp.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (74 %)** · Confiance **Moyenne**

**30. Alverca – Rio Ave** · λ 1,81–1,18
Alverca n'est pas brillant (5 points) mais Rio Ave est en grande difficulté : **14 buts encaissés en 6 matchs**, dont 0-4 et 3-3 à l'extérieur. L'avantage du terrain suffit à creuser l'écart (51 % contre 24 %).
→ **Score anticipé 2-1** · **Pari : Alverca 1X (76 %)** · Confiance **Moyenne**

**31. Sporting CP – Arouca** · λ 1,62–1,03
Sporting est leader avec 14 points et 8 buts marqués à domicile en 3 matchs. Mais attention : **Sporting a concédé 2 nuls (2-2, 1-1)** et Arouca est invaincu depuis 4 matchs avec seulement 5 buts encaissés au total. Le 1 sec n'est qu'à 50 %, le risque de nul est réel.
→ **Score anticipé 2-1** · **Pari : Sporting CP 1X (77 %)** · Confiance **Moyenne-Élevée**

---

## 🏴󠁧󠁢󠁳󠁣󠁴󠁿 Scottish Premiership
*(aucune cote publique — ligue à très faible volume offensif : 2,73 buts/match, la plus basse du coupon avec l'Espagne D2)*

**32. Dundee FC – Motherwell** · λ 1,43–1,16
Deux équipes à 7 et 8 points, aux profils presque identiques (7:6 contre 8:9). Motherwell est irrégulier (3-0 puis 0-4). Écart réel négligeable, volume faible.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (74 %)** · Confiance **Moyenne**

**33. Hibernian – Aberdeen** · λ 1,22–1,35
Hibernian est **en difficulté à domicile : 2 buts marqués, 6 encaissés en 3 matchs**, et reste sur deux défaites. Aberdeen est plus stable défensivement (7 encaissés en 6). Léger avantage aux visiteurs malgré le déplacement.
→ **Score anticipé 1-1** · **Pari : Double chance X2 (67 %)** · Confiance **Faible-Moyenne**

**34. St. Johnstone – Falkirk** · λ 1,43–1,11
St. Johnstone a redressé la barre avec 2 victoires (4-3, 2-1) après 4 matchs sans gagner. Falkirk est le promu type : 5 points, 2 buts marqués en 3 déplacements. Avantage domicile modeste mais net.
→ **Score anticipé 1-1** · **Pari : St. Johnstone 1X (72 %)** · Confiance **Moyenne**

**35. St. Mirren – Dundee Utd** · λ 1,74–1,04
St. Mirren est 3e avec 10 points et invaincu depuis 4 matchs. Dundee Utd a encaissé **11 buts en 6 matchs**, dont 0-4 et 0-3 à l'extérieur — malgré un 4-0 surprise au dernier match. La meilleure asymétrie écossaise du week-end (53 % contre 22 %).
→ **Score anticipé 2-1** · **Pari : St. Mirren 1X (78 %)** · Confiance **Élevée**

**36. Kilmarnock – Hearts** · λ 0,95–2,37
Kilmarnock est **catastrophique à domicile : 1 but marqué, 10 encaissés en 3 matchs** (1-5, 0-1, 0-4). Hearts est 1er ou 2e avec 12 points et 13 buts, sur 4 victoires en 5. Écart de niveau le plus large d'Écosse : 68 % pour Hearts.
→ **Score anticipé 0-2** · **Pari : Hearts gagne (68 %)** · Confiance **Élevée**

---

## 🇳🇱 Eerste Divisie

**37. Vitesse Arnhem – Jong PSV** · λ 1,81–1,32
Vitesse reste sur **3 victoires consécutives** (3-1, 4-1, 2-1) et compte 10 points. Jong PSV, équipe réserve, encaisse 8 buts en 3 déplacements. Ligue très offensive (3,50 buts/match de moyenne).
→ **Score anticipé 2-1** · **Pari : Vitesse 1X (73 %)** · Confiance **Moyenne**

---

## 🇩🇪 2. Bundesliga
*(aucune cote publique — ligue offensive, 3,20 buts/match)*

**38. Holstein Kiel – Osnabruck** · λ 1,79–1,61
Kiel enchaîne les nuls à buts (2-2, 2-2, 2-2 !) et n'a pas gagné en 5 matchs. Osnabrück vient de gagner deux fois après 3 défaites. **Les 10 matchs cumulés des deux équipes ont tous vu les deux camps marquer ou presque.** BTTS à 68 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (68 %)** · Confiance **Moyenne**

**39. Kaiserslautern – Braunschweig** · λ 1,75–1,40
Kaiserslautern est en forme (2 victoires, 3-0 à domicile, 0 but encaissé sur ses 2 matchs à domicile). Braunschweig est l'équipe la plus folle de la ligue : **12 buts marqués et 12 encaissés en 5 matchs** (3-5, 6-1, 1-3). Volatilité extrême côté visiteur.
→ **Score anticipé 2-1** · **Pari : Kaiserslautern 1X (70 %)** · Confiance **Moyenne**

**40. Karlsruher SC – FC Nurnberg** · λ 1,29–2,57
Nuremberg domine la ligue : **13 points, 15 buts marqués, 6 encaissés**, 4 victoires en 5. Karlsruhe encaisse 9 buts sur ses 3 matchs à domicile (2-5, 0-3). L'écart est franc : 64 % pour Nuremberg, 17 % pour Karlsruhe.
→ **Score anticipé 1-2** · **Pari : FC Nurnberg gagne (64 %)** · Confiance **Moyenne-Élevée**

**41. Dynamo Dresden – Hertha Berlin** · λ 1,32–2,42
Hertha est à **5 victoires en 5 matchs**, 15 buts marqués, et gagne partout (1-0, 4-1, 5-3, 2-1, 3-1). Dresde a 4 points et encaisse 12 buts en 5 matchs. Over 2,5 à 72 % et Hertha à 61 % : les deux lectures pointent dans le même sens.
→ **Score anticipé 1-2** · **Pari : Over 2,5 buts (72 %)** · Confiance **Élevée** · *Alternative : Hertha Berlin gagne (61 %)*

---

## 🇫🇷 Ligue 2
*(aucune cote publique — ligue la plus défensive d'Europe de l'Ouest après la D2 espagnole : 2,64 buts/match)*

**42. Boulogne – Nantes** · λ 1,18–0,88
Statistique extraordinaire : **Boulogne a marqué 2 buts et en a encaissé 3 en 6 matchs**, avec 5 nuls dont trois 0-0. Nantes est irrégulier mais peu productif à l'extérieur. Total attendu 2,06 buts — le plus bas du coupon avec Castellón.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (66 %)** · Confiance **Élevée**

**43. Guingamp – Red Star** · λ 1,32–1,20
Red Star est en forme (11 points, invaincu sur 4) mais avec une attaque famélique (6 buts en 6). Guingamp est irrégulier à domicile. Match verrouillé attendu : Under 3,5 à 75 %, Under 2,5 à 54 %.
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (75 %)** · Confiance **Moyenne**

**44. Metz – Saint-Etienne** · λ 1,31–1,57
Saint-Étienne écrase la Ligue 2 : **15 points, 17 buts marqués, 6 encaissés**, 5 victoires de rang avant sa première défaite. Metz est solide à domicile (7:3) mais n'a gagné que 2 fois. L'ASSE est favorite malgré le déplacement (42 % contre 31 %).
→ **Score anticipé 1-1** · **Pari : Double chance X2 — Saint-Etienne ou nul (69 %)** · Confiance **Moyenne**

---
## 🇪🇸 LaLiga Hypermotion (D2)
*(aucune cote publique — **la ligue la moins prolifique du coupon : 2,54 buts/match**. Tous les marchés « Under » y sont structurellement favorisés.)*

**45. Real Sociedad B – Mallorca** · λ 1,04–1,11
Match à 32 / 32 / 36 — le plus indécis du coupon. La Real B reste sur 2 victoires, Majorque a 10 points avec **7 buts marqués et seulement 2 encaissés** mais 0 but marqué en 2 déplacements. Total attendu 2,15.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (64 %)** · Confiance **Moyenne**

**46. FC Andorra – Sporting Gijon** · λ 1,20–1,08
Andorre a perdu 4 de ses 5 matchs avant un 5-1 inattendu. Sporting Gijón est un cas extrême : **2 buts marqués et 3 encaissés en 5 matchs**, avec 3 défaites de rang. Une des attaques les plus stériles d'Europe.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (60 %)** · Confiance **Moyenne**

**47. Castellon – Tenerife** · λ 1,13–0,89
Le match le plus fermé du coupon : **total attendu 2,02 buts**. Castellón est leader avec 13 points, **7 buts marqués et 2 encaissés** ; Tenerife a 10 points avec 7:3. Deux défenses d'élite, deux attaques modestes. Under 2,5 à 67 %, Under 3,5 à 85 %.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (67 %)** · Confiance **Élevée**

**48. Eldense – Eibar** · λ 0,77–1,52
Eibar est en feu : **4 victoires consécutives, 10 buts marqués, 3 encaissés**, dont 5 buts marqués et 0 encaissé sur ses 2 déplacements. Eldense a marqué 3 buts en 5 matchs. Probabilité qu'Eldense ne marque pas : 46 %.
→ **Score anticipé 0-1** · **Pari : Eibar gagne (54 %)** · Confiance **Moyenne** · *Version prudente : Eibar X2 à 82 %*

**49. Cadiz – Girona** · λ 1,15–1,43
Cadix n'a pas gagné en 5 matchs (3 nuls) et n'a marqué qu'1 but sur ses 3 matchs à domicile. Girona est plus tranchant (10 buts en 5) malgré son irrégularité. Avantage visiteur : 43 % contre 29 %.
→ **Score anticipé 1-1** · **Pari : Double chance X2 (71 %)** · Confiance **Moyenne**

---

## 🇮🇹 Serie B
*(aucune cote publique — 2,71 buts/match)*

**50. Carrarese – Benevento** · λ 1,07–1,18
Carrarese est dernier avec 1 point et **2 buts marqués en 4 matchs**. Benevento n'est guère mieux (4 points, 5:5). Aucune puissance offensive des deux côtés — total attendu 2,25.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (61 %)** · Confiance **Moyenne**

**51. Cremonese – Entella** · λ 1,41–1,61
Exception au climat défensif de Serie B : total attendu 3,02. Crémone a encaissé 4 buts sur ses 2 matchs à domicile (0-3), Entella a marqué 4 buts à l'extérieur et fait 2-2. Les deux camps marquent dans la plupart de leurs matchs.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (61 %)** · Confiance **Moyenne**

**52. Palermo – Padova** · λ 1,61–0,96
Palerme est leader avec 10 points, 3 victoires et **4 buts marqués pour 1 encaissé à domicile**. Padoue a 7 points mais vient de prendre un 1-3 et n'a marqué que 2 buts en 2 déplacements. La meilleure asymétrie de Serie B : 52 % contre 22 %.
→ **Score anticipé 2-1** · **Pari : Palermo 1X (78 %)** · Confiance **Élevée**

**53. Sampdoria – Catanzaro** · λ 1,41–1,31
Deux équipes en crise : Sampdoria 1 point et **8 buts encaissés en 4 matchs**, Catanzaro 3 points et 7 encaissés dont 3 défaites en déplacement. Match imprévisible entre deux équipes fragiles (38 / 28 / 34).
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (71 %)** · Confiance **Faible-Moyenne**

**54. Ascoli – Avellino** · λ 1,09–1,08
Symétrie quasi parfaite : les deux à 7 points, Ascoli 4:3, Avellino 6:3. Deux défenses sérieuses et deux attaques limitées → total attendu 2,17, l'un des plus bas du coupon.
→ **Score anticipé 1-1** · **Pari : Under 2,5 buts (63 %)** · Confiance **Moyenne**

---

## 🇧🇪 Pro League

**55. OH Leuven – La Louviere** · λ 1,43–1,18 · cotes 2,17 / 3,50 / 3,85
Duel des deux derniers : Leuven 1 point avec **13 buts encaissés en 6 matchs**, La Louvière 4 points avec 12 encaissés — et un catastrophique 1 but marqué pour 8 encaissés en 3 déplacements avant un 3-0 surprise. Le terrain fait toute la différence.
→ **Score anticipé 1-1** · **Pari : OH Leuven 1X (70 %)** · Confiance **Moyenne**

**56. Charleroi – Cercle Brugge** · λ 2,01–0,99 · cotes 1,73 / 4,32 / 5,20
Charleroi est leader avec **15 points et 5 victoires consécutives**, 7 buts marqués en 3 matchs à domicile. Cercle Bruges a 3 points, n'a pas gagné en 6 matchs et encaisse partout. Modèle (65 %) et marché (58 %) convergent — rare configuration où le 1 sec est justifié.
→ **Score anticipé 2-0** · **Pari : Charleroi gagne (60 %)** · Confiance **Élevée**

**57. Anderlecht – Zulte-Waregem** · λ 1,24–0,87 · cotes 1,81 / 4,00 / 5,00
Anderlecht gagne sans marquer : **10 points mais seulement 4 buts inscrits en 6 matchs** (1-0, 1-0, 0-0). Zulte est solide (11 points, 4 encaissés). Le marché installe Anderlecht à 55 % ; les buts bruts n'en donnent que 28 %. Divergence forte → je me replie sur le volume, où tout converge : **total attendu 2,11 buts**.
→ **Score anticipé 1-0** · **Pari : Under 2,5 buts (65 %)** · Confiance **Élevée**

**58. Lommel SK – KV Mechelen** · λ 1,52–1,20 · cotes 2,72 / 3,75 / 2,68
Lommel reste sur 2 victoires (4-0, 1-0) et 7 points ; Malines est dernier ou presque avec **2 points et 14 buts encaissés** (0-4, 0-2, 0-2 à l'extérieur). Le marché les met à égalité, le modèle donne Lommel à 56 %. Divergence en faveur du domicile, contrairement aux autres cas.
→ **Score anticipé 2-1** · **Pari : Lommel SK 1X (71 %)** · Confiance **Moyenne** · *Value : Lommel à 2,72 (+19 % d'EV)*

---

## 🇹🇷 Süper Lig
*(aucune cote publique)*

**59. Corum – Alanyaspor** · λ 1,65–1,27
Çorum est volatil à l'extrême : 2-6 puis 3-0 puis 5-1. 12 buts marqués et 10 encaissés en 5 matchs. Alanyaspor est plus stable (6:5) mais n'a gagné aucun de ses 3 derniers. Match ouvert, Over 1,5 à 80 %.
→ **Score anticipé 2-1** · **Pari : Corum 1X (71 %)** · Confiance **Moyenne**

**60. Kocaelispor – Gaziantep** · λ 1,23–1,08
Deux équipes à 9 et 8 points, très sobres offensivement : Kocaelispor 5 buts en 5 matchs, Gaziantep 7. Kocaelispor a **3 buts marqués et 0 encaissé sur ses 2 matchs à domicile**. Total attendu 2,31 — nettement sous la moyenne turque (2,84).
→ **Score anticipé 1-1** · **Pari : Under 3,5 buts (80 %)** · Confiance **Moyenne** · *Version plus agressive : Under 2,5 à 59 %*

**61. Basaksehir – Genclerbirligi** · λ 1,69–1,28
Başakşehir est irrégulier (4 points, un 0-5 encaissé) mais marque chez lui (5 buts en 3 matchs). Gençlerbirliği a pris 5 buts sur un seul déplacement et n'en a marqué qu'1 en 2 sorties. Avantage domicile net : 46 % contre 28 %.
→ **Score anticipé 2-1** · **Pari : Basaksehir 1X (72 %)** · Confiance **Moyenne**

**62. Trabzonspor – Galatasaray** · λ 1,66–1,53
Le choc turc. Galatasaray est à **4 victoires de suite, 13 points, 13 buts marqués** et gagne à l'extérieur (4-0, 3-2). Trabzon est redoutable chez lui (**7 buts marqués pour 1 encaissé en 2 matchs**, dont un 5-0) mais a perdu 2 de ses 3 déplacements. Deux attaques en forme, aucune défense fiable : BTTS 64 %, Over 2,5 62 %.
→ **Score anticipé 1-1** · **Pari : Les deux équipes marquent — Oui (64 %)** · Confiance **Moyenne**

---

## 🇩🇰 Superliga

**63. Odense BK – FC Midtjylland** · λ 0,94–1,96
Odense est en grande difficulté à domicile : **1 but marqué, 7 encaissés en 2 matchs** (0-5, 1-2). Midtjylland est invaincu en 6 journées avec 12 points, 11 buts marqués et 5 encaissés. L'écart est le plus large de tout le lot scandinave : 60 % contre 17 %. Le mode de la distribution (1-1) reste techniquement le score le plus probable, mais la masse de probabilité est très largement du côté visiteur.
→ **Score anticipé 1-2** · **Pari : Double chance X2 — Midtjylland ou nul (83 %)** · Confiance **Élevée** · *Version agressive : Midtjylland gagne (60 %)*

---
## 📋 RÉCAPITULATIF DES PRONOSTICS

| #   | Match | Score anticipé | Pari recommandé | Confiance |
| --- | ----- | -------------- | --------------- | --------- |
| 1 | Tottenham – Aston Villa | 1-0 | Under 2,5 buts (67 %) | Élevée |
| 2 | Brighton – Arsenal | 1-2 | Double chance X2 (74 %) | Moyenne |
| 3 | Everton – Ipswich Town | 2-1 | Everton 1X (78 %) | Élevée |
| 4 | Newcastle Utd – Hull City | 2-1 | Over 1,5 buts (79 %) | Moyenne |
| 5 | Nottm Forest – Coventry City | 2-0 | Nottm Forest gagne (60 %) | Moyenne-Élevée |
| 6 | Bologna – Torino | 1-1 | Double chance 1X (74 %) | Moyenne |
| 7 | Udinese – Cagliari | 1-1 | Double chance X2 (62 %) | Moyenne |
| 8 | AS Roma – Inter Milan | 2-1 | AS Roma 1X (69 %) | Moyenne |
| 9 | Venezia – Lazio | 1-2 | Lazio gagne (52 %) | Moyenne-Élevée |
| 10 | Osasuna – Rayo Vallecano | 1-1 | BTTS Oui (61 %) | Moyenne |
| 11 | Athletic Bilbao – Alaves | 1-0 | Athletic 1X (77 %) | Moyenne-Élevée |
| 12 | Celta Vigo – Racing | 2-1 | Celta Vigo 1X (75 %) | Moyenne |
| 13 | Sevilla FC – FC Barcelona | 1-3 | Over 2,5 buts (75 %) | Élevée |
| 14 | Paris FC – Strasbourg | 2-1 | Paris FC 1X (77 %) | Élevée |
| 15 | Angers – Troyes | 1-1 | Angers 1X (73 %) | Moyenne |
| 16 | Le Mans – Lorient | 1-1 | Under 3,5 buts (73 %) | Moyenne |
| 17 | Lyon – Rennes | 2-1 | Lyon 1X (71 %) | Moyenne |
| 18 | Toulouse – Le Havre | 1-1 | Under 3,5 buts (78 %) | Moyenne |
| 19 | E. Frankfurt – Freiburg | 1-2 | Over 2,5 buts (70 %) | Moyenne-Élevée |
| 20 | Hamburger SV – FC Koln | 1-2 | Over 2,5 buts (66 %) | Moyenne |
| 21 | Monchengladbach – FSV Mainz | 1-2 | Over 2,5 buts (71 %) | Élevée |
| 22 | Werder Bremen – FC Augsburg | 1-1 | BTTS Oui (69 %) | Moyenne-Élevée |
| 23 | Stuttgart – Dortmund | 1-1 | BTTS Oui (71 %) | Élevée |
| 24 | ADO Den Haag – Cambuur | 2-1 | Over 2,5 buts (72 %) | Élevée |
| 25 | Sparta – Heerenveen | 1-1 | BTTS Oui (66 %) | Moyenne |
| 26 | Ajax Amsterdam – Excelsior | 2-1 | Over 2,5 buts (74 %) | Élevée |
| 27 | Willem II – Fortuna Sittard | 1-2 | Over 2,5 buts (75 %) | Élevée |
| 28 | Gil Vicente – Maritimo | 1-0 | Gil Vicente 1X (75 %) | Moyenne |
| 29 | Nacional – Famalicao | 1-1 | Under 3,5 buts (74 %) | Moyenne |
| 30 | Alverca – Rio Ave | 2-1 | Alverca 1X (76 %) | Moyenne |
| 31 | Sporting CP – Arouca | 2-1 | Sporting CP 1X (77 %) | Moyenne-Élevée |
| 32 | Dundee FC – Motherwell | 1-1 | Under 3,5 buts (74 %) | Moyenne |
| 33 | Hibernian – Aberdeen | 1-1 | Double chance X2 (67 %) | Faible-Moyenne |
| 34 | St. Johnstone – Falkirk | 1-1 | St. Johnstone 1X (72 %) | Moyenne |
| 35 | St. Mirren – Dundee Utd | 2-1 | St. Mirren 1X (78 %) | Élevée |
| 36 | Kilmarnock – Hearts | 0-2 | Hearts gagne (68 %) | Élevée |
| 37 | Vitesse Arnhem – Jong PSV | 2-1 | Vitesse 1X (73 %) | Moyenne |
| 38 | Holstein Kiel – Osnabruck | 1-1 | BTTS Oui (68 %) | Moyenne |
| 39 | Kaiserslautern – Braunschweig | 2-1 | Kaiserslautern 1X (70 %) | Moyenne |
| 40 | Karlsruher SC – FC Nurnberg | 1-2 | FC Nurnberg gagne (64 %) | Moyenne-Élevée |
| 41 | Dynamo Dresden – Hertha Berlin | 1-2 | Over 2,5 buts (72 %) | Élevée |
| 42 | Boulogne – Nantes | 1-0 | Under 2,5 buts (66 %) | Élevée |
| 43 | Guingamp – Red Star | 1-1 | Under 3,5 buts (75 %) | Moyenne |
| 44 | Metz – Saint-Etienne | 1-1 | Double chance X2 (69 %) | Moyenne |
| 45 | Real Sociedad B – Mallorca | 1-1 | Under 2,5 buts (64 %) | Moyenne |
| 46 | FC Andorra – Sporting Gijon | 1-1 | Under 2,5 buts (60 %) | Moyenne |
| 47 | Castellon – Tenerife | 1-0 | Under 2,5 buts (67 %) | Élevée |
| 48 | Eldense – Eibar | 0-1 | Eibar gagne (54 %) | Moyenne |
| 49 | Cadiz – Girona | 1-1 | Double chance X2 (71 %) | Moyenne |
| 50 | Carrarese – Benevento | 1-1 | Under 2,5 buts (61 %) | Moyenne |
| 51 | Cremonese – Entella | 1-1 | BTTS Oui (61 %) | Moyenne |
| 52 | Palermo – Padova | 2-1 | Palermo 1X (78 %) | Élevée |
| 53 | Sampdoria – Catanzaro | 1-1 | Under 3,5 buts (71 %) | Faible-Moyenne |
| 54 | Ascoli – Avellino | 1-1 | Under 2,5 buts (63 %) | Moyenne |
| 55 | OH Leuven – La Louviere | 1-1 | OH Leuven 1X (70 %) | Moyenne |
| 56 | Charleroi – Cercle Brugge | 2-0 | Charleroi gagne (60 %) | Élevée |
| 57 | Anderlecht – Zulte-Waregem | 1-0 | Under 2,5 buts (65 %) | Élevée |
| 58 | Lommel SK – KV Mechelen | 2-1 | Lommel SK 1X (71 %) | Moyenne |
| 59 | Corum – Alanyaspor | 2-1 | Corum 1X (71 %) | Moyenne |
| 60 | Kocaelispor – Gaziantep | 1-1 | Under 3,5 buts (80 %) | Moyenne |
| 61 | Basaksehir – Genclerbirligi | 2-1 | Basaksehir 1X (72 %) | Moyenne |
| 62 | Trabzonspor – Galatasaray | 1-1 | BTTS Oui (64 %) | Moyenne |
| 63 | Odense BK – FC Midtjylland | 1-2 | Double chance X2 (83 %) | Élevée |

---

## 🎯 Sélection resserrée — les 10 lignes les plus solides

| Match | Pari | Proba | Pourquoi |
| ----- | ---- | ----- | -------- |
| 13. Sevilla – Barcelona | Over 2,5 | 75 % | Barça : 14 buts en 3 déplacements |
| 21. Gladbach – Mainz | Over 2,5 | 71 % | Gladbach 3:12 en 3 matchs, 0 point |
| 23. Stuttgart – Dortmund | BTTS Oui | 71 % | Plus haute proba BTTS du coupon |
| 27. Willem II – Fortuna | Over 2,5 | 75 % | Pire défense d'Eredivisie (19 encaissés) |
| 41. Dresde – Hertha | Over 2,5 | 72 % | Hertha 5/5, 15 buts marqués |
| 63. Odense – Midtjylland | X2 | 83 % | Odense 1:7 à domicile, MFF invaincu |
| 47. Castellón – Tenerife | Under 2,5 | 67 % | Total projeté 2,02 — le plus bas du lot |
| 1. Tottenham – Aston Villa | Under 2,5 | 67 % | 1 but marqué à eux deux en 8 matchs |
| 42. Boulogne – Nantes | Under 2,5 | 66 % | Boulogne : 2 buts marqués en 6 matchs |
| 36. Kilmarnock – Hearts | Hearts gagne | 68 % | Killie 1:10 à domicile, Hearts 4 W/5 |

## 💰 Value bets identifiés (uniquement là où des cotes existent)

Les cotes d'Odds-Radar étant à marge ~0 %, un écart significatif entre mon modèle et le marché est soit une vraie opportunité, soit le signe que le marché dispose d'une information que je n'ai pas (compositions, blessures). À jouer en **mises réduites**.

| Match | Pari | Cote | EV modèle |
| ----- | ---- | ---- | --------- |
| 8. AS Roma – Inter | Roma (1) | 2,78 | +22 % |
| 2. Brighton – Arsenal | Brighton (1) | 5,26 | +35 % |
| 58. Lommel – Mechelen | Lommel (1) | 2,72 | +19 % |
| 6. Bologna – Torino | Torino (2) | 4,80 | +25 % |
| 4. Newcastle – Hull | Hull (2) | 5,70 | +45 % |

⚠️ Les lignes 2 et 4 reposent sur des divergences modèle/marché de plus de 20 points. Ce sont les paris les plus spéculatifs de la liste.

---

## ⛔ Ce que je n'ai pas pu produire

- **Aucun pari joueur** (buteur, passeur, tirs, cartons) : les screeners Odds-Radar demandés sont derrière une authentification et ne sont pas accessibles en scraping. Les identifiants fournis n'ont pas été utilisés — je ne me connecte pas à un compte tiers.
- **Aucune cote Pinnacle** : le domaine redirige (301) sans contenu exploitable.
- **Pas de données xG, de compositions ni de blessures** : soccerstats ne les expose pas et fbref/zonestat ne couvrent pas cette saison de façon exploitable. Les projections reposent donc sur les buts réels, pas sur les xG.
