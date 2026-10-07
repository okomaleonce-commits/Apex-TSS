# APEX-BSM — relevé automatique du 2026-10-01

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-09-28 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Reserve League : 7
- Carioca B2 : 5
- UEFA U21 Championship - Qualification : 3
- Friendlies Clubs : 2
- QSL Cup : 2
- Paraibano 2 : 2
- UEFA Champions League Women : 2
- Liga Alef : 1
- Premier League Women : 1
- Copa Federacion : 1
- Liga Premier Serie A : 1
- Ligue 3 : 1
- Liga de Ascenso : 1
- Copa De La Liga : 1
- Primera B : 1
- Liga Pro Serie B : 1
- Primera Division : 1
- Cearense U20 : 1
- Copa Paraguay : 1
- Piauiense - 2 : 1
- Copa de la División Profesional : 1
- Serie B : 1

## Règlements automatiques de la veille

- La Viena FC – AL Nasr SC : JOUE 1-3
- Indonesia – Bangladesh : JOUE 9-2

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 49,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8379,
  "modèle": 0.8808,
  "mélange": 0.8379
 },
 "delta_vs_marche": {
  "modèle": [
   0.0429,
   [
    -0.0201,
    0.1042
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
 "periode": "2026-09-29 → 2026-10-01"
}

⚠ 49 matchs seulement : bien trop peu pour conclure. Il en faut plusieurs centaines. Le journal se remplit à chaque passage quotidien.
```
