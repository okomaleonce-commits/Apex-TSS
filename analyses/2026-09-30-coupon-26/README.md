# Coupon 2026-09-30 — scan de 26 matchs

**Module BSM appliqué (obligatoire, CLAUDE.md).**
Backtest de référence : `bsm-20260928T153332Z` (`apex-bsm-1.0.0`).
Cotes : relevées via `tools/apex_apifootball.py snapshot --date 2026-09-30`
(source Pinnacle en priorité, à défaut Bet365 ; `_update` ≈ 12:00 UTC, relevé 16:54 UTC).
Journal `ledger/` : **aucun enregistrement** — aucune sélection émise (voir verdict).

## Verdict global : ABSTENTION TOTALE — 0 signal officiel sur 26

Deux vetos structurels s'appliquent à **tous** les matchs, avant même la simulation :

1. **Statut du backtest = « NON SUPÉRIEUR AU MARCHÉ ».**
   Sur le test final isolé 2025-26 (3 279 matchs, 8 ligues), le modèle est **moins bon
   que le marché** (log-loss 1X2 +0,0214, IC95 [+0,016 ; +0,027] ; paris simulés
   −14,1 %/unité, CLV −5,6 %). Le module impose donc la surveillance ou l'abstention
   sur les marchés cotés : une EV calculée contre la cote par ce modèle est **présumée
   illusoire**. C'est un veto permanent sur la publication de value (règle 4 : « aucun veto »).

2. **Aucun des 26 matchs n'est dans le périmètre du backtest.**
   Le backtest ne couvre que E0, E1, E2, E3, SP1, I1, D1, F1 (Angleterre 1-4, Liga,
   Serie A, Bundesliga, Ligue 1). Ici : UWCL, UEFA U21, Gulf Cup, réserves argentines,
   amateur belge, Gaúcho 3, non-league / National League anglaise, Liga Mayor dominicaine,
   Primera péruvienne, USL, Canadian Premier. Toute simulation sort donc en
   **« NON VALIDÉ — λ externes »** (règle 2). Un match non validé ne peut porter de
   marché officiel.

En clair : ce n'est pas un défaut d'analyse, c'est le module qui fonctionne comme prévu.
Les probabilités ci-dessous sont publiées **comme estimation, jamais comme signal de value**.
Là où une estimation APEX est donnée, elle provient d'une simulation Monte-Carlo (40 000)
dont les λ ont été **calés sur le marché lui-même** (1X2 démarginé + Over 2,5) : elle
**reflète le marché**, elle ne le bat pas.

## Données manquantes (règle 6 : on l'écrit, on n'invente pas)

| Match | Raison |
|---|---|
| Roma W–Barcelona W (16:45) | Déjà démarré au relevé (16:54 UTC). Pas de relevé d'avant-match ⇒ règle 5 non satisfaite. |
| Paris FC W–Arsenal W (16:45) | idem |
| Häcken W–Juventus W (16:45) | idem |
| Chattanooga Red Wolves–Spokane Velocity (USL Lg1) | Aucun relevé retourné par l'API. Non chiffrable. |

## Lecture tip par tip (probabilité MARCHÉ démarginé de voir le tip passer)

« Marché » = probabilité juste (marge retirée) tirée des cotes horodatées. « OK » = le
marché penche dans le sens du tip ; « CONTRE » = le marché penche à l'opposé ; « ~ » = pile ou face.
Aucune de ces lignes n'est un signal : à ~60 % de proba pour une cote ~1,50, il n'y a pas de marge.

| Heure | Match | Ligue | Tip | Proba marché du tip | Lecture | Statut APEX |
|---|---|---|---|---|---|---|
| 16:45 | Roma W–Barcelona W | UWCL | AH -0.5/-1 ext (Barça) | — | pas de données | NON VALIDÉ / pas de relevé |
| 16:45 | Paris FC W–Arsenal W | UWCL | AH -0.5/-1 ext (Arsenal) | — | pas de données | NON VALIDÉ / pas de relevé |
| 16:45 | Häcken W–Juventus W | UWCL | Under 2.5 | — | pas de données | NON VALIDÉ / pas de relevé |
| 17:00 | Kosovo U21–Cyprus U21 | UEFA U21 | AH -0.5/-1 dom | ~0,68 (vic 74 %, par 2+ 52 %, nul-1 pt 22 %) | OK (gros favori) | NON VALIDÉ |
| 17:30 | Bahrain–Yemen | Gulf Cup | DC X2 / +0.5 ext | 0,444 | **CONTRE** (Bahreïn fav 56 %) | NON VALIDÉ |
| 17:30 | UAE–Qatar | Gulf Cup | DC X2 / +0.5 ext | 0,515 | ~ (quasi pile/face) | NON VALIDÉ |
| 18:00 | Platense Res.–Huracán Res. | Arg Reserve | Under 2.5 | 0,633 | OK | NON VALIDÉ |
| 18:00 | Sporting Charleroi II–Habay | Bel Amateur | Under 2.5 | 0,496 | ~ | NON VALIDÉ |
| 18:00 | Flénu–Olympic Charleroi | Bel Amateur | Over 2.5 | 0,601 | OK | NON VALIDÉ |
| 18:00 | Rosario Central Res.–Unión SF Res. | Arg Reserve | Under 2.5 | 0,576 | OK (léger) | NON VALIDÉ |
| 18:00 | Real–Farroupilha | Bra Gaúcho 3 | AH -0.5/-1 dom | vic 59 %, **par 2+ seulement 35 %** | CONTRE sur la jambe -1 | NON VALIDÉ |
| 18:30 | Scotland U21–Azerbaijan U21 | UEFA U21 | AH -0.5/-1 dom | vic 77 %, par 2+ 55 %, nul-1 pt 22 % | OK (gros favori) | NON VALIDÉ |
| 18:45 | Whyteleafe–Brentwood Town | Eng Non-Lg | Over 2.5 | 0,623 | OK | NON VALIDÉ |
| 18:45 | Frome Town–Taunton Town | Eng Non-Lg | Over 2.5 | 0,615 | OK | NON VALIDÉ |
| 18:45 | Eastleigh–Southend | Eng Nat Lg | Over 2.5 | 0,610 | OK | NON VALIDÉ |
| 18:45 | Tamworth–Sutton Utd | Eng Nat Lg | Over 2.5 | 0,599 | OK | NON VALIDÉ |
| 19:00 | Lyon W–Chelsea W | UWCL | AH -0.5/-1 dom | vic 66 %, **par 2+ 43 %** | CONTRE sur la jambe -1 | NON VALIDÉ |
| 19:00 | SL Benfica W–Bayern Munich W | UWCL | Over 2.5 | 0,634 | OK | NON VALIDÉ |
| 19:00 | Portugal U21–Gibraltar U21 | UEFA U21 | AH -0.5/-1 dom | dom ≈ 0,99 (Betfair 1,01) | OK (écart extrême) | NON VALIDÉ (pas de ligne Pinnacle propre) |
| 20:00 | Salcedo–Delfines Del Este | Dom Liga Mayor | Under 2.5 | 0,515 | ~ | NON VALIDÉ |
| 20:00 | Cienciano–Los Chankas | Per Primera | Over 2.5 | 0,631 | OK | NON VALIDÉ |
| 22:00 | Central Córdoba Res.–Newell's Res. | Arg Reserve | Under 2.5 | 0,583 | OK (léger) | NON VALIDÉ |
| 23:00 | Brooklyn–Detroit City | USL Champ | Under 2.5 | 0,527 | ~ | NON VALIDÉ |
| 23:00 | Miami FC–Sporting JAX | USL Champ | Over 2.5 | 0,645 | OK | NON VALIDÉ |
| 23:00 | Chattanooga–Spokane | USL Lg1 | Over 2.5 | — | pas de données | NON VALIDÉ / pas de relevé |
| 23:00 | Atlético Ottawa–Cavalry FC | Can Premier | Over 2.5 | 0,612 | OK | NON VALIDÉ |

### Points d'attention (toujours sans signal)

- **Les deux tips Gulf Cup vont contre le marché.** Bahrain–Yemen : le marché fait de
  Bahreïn un favori net (56 %), la double chance Yemen ne vaut que 44 %. UAE–Qatar est un
  quasi pile/face ; la DC X2 est à peine au-dessus de 50 %.
- **Deux handicaps -0.5/-1 sont agressifs** : Real (Gaúcho 3) et Lyon W ne gagnent « par 2+ »
  que 35 % et 43 % du temps selon le marché — la jambe -1 est perdante plus souvent que gagnante.
  Les deux U21 (Kosovo, Scotland) et Portugal-Gibraltar sont, eux, de gros favoris cohérents avec le tip.
- **Tous les Over 2,5 sont « OK » côté marché (≈ 0,60-0,65)** mais à ces niveaux la cote (~1,50)
  ne laisse aucune marge : suivre le marché n'est pas battre le marché.

## Ce qu'il faudrait pour un vrai signal

Rien dans ce coupon n'est chiffrable en signal : soit la ligue est hors backtest (toutes),
soit le modèle est battu par le marché même sur son périmètre. Un signal APEX exigerait un
match dans E0/E1/E2/E3/SP1/I1/D1/F1 **et** un ancrage marché validé (piste v1.1 du backtest,
à tester sur 2026-27) — aucune des deux conditions n'est réunie ici.
