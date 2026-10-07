# APEX-BSM — relevé automatique du 2026-09-30

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-09-28 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Brasileiro U17 : 10
- First Amateur Division : 9
- Liga Alef : 7
- UEFA Europa Cup - Women : 7
- WSL Cup : 7
- Reserve League : 6
- Gaúcho - 3 : 5
- Pernambucano - U20 : 4
- Friendlies : 4
- Second Amateur Division - VFV B : 4
- Segunda División : 4
- UEFA U21 Championship - Qualification : 3
- Svenska Cupen - Women : 3
- Copa Federacion : 3
- Second Amateur Division - VFV A : 3
- Premier League International Cup : 3
- Paraibano 2 : 3
- USL Championship : 3
- NPFL : 2
- CAF U23 Cup of Nations : 2
- Gulf Cup of Nations : 2
- Friendlies Clubs : 2
- Non League Premier - Southern South : 2
- Copa de la División Profesional : 2
- Copa Uruguay : 2
- UEFA Champions League Women : 2
- Primera División : 2
- Copa Paraguay : 2
- NM Cupen : 1
- 3. SNL - East : 1
- Division 2 - Norrland : 1
- Africa Cup of Nations U20 : 1
- Regionalliga - West : 1
- Oberliga - Westfalen : 1
- 1. Liga Promotion : 1
- Second Amateur Division - ACFF : 1
- 1. Liga Classic - Group 2 : 1
- Non League Premier - Isthmian : 1
- Liga Mayor : 1
- Primera B : 1

## Règlements automatiques de la veille

Aucun.

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 43,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8492,
  "modèle": 0.912,
  "mélange": 0.8492
 },
 "delta_vs_marche": {
  "modèle": [
   0.0629,
   [
    -0.0018,
    0.1318
   ]
  ],
  "mélange": [
   -0.0,
   [
    -0.0,
    0.0
   ]
  ]
 },
 "periode": "2026-09-29 → 2026-09-30"
}

⚠ 43 matchs seulement : bien trop peu pour conclure. Il en faut plusieurs centaines. Le journal se remplit à chaque passage quotidien.
```
