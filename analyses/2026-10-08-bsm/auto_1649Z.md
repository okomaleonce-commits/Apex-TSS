# APEX-BSM — relevé automatique du 2026-10-08

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-10-06 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Copa do Brasil U20 : 8
- Reserve League : 7
- Premier League : 5
- Division 1 : 4
- First League : 3
- Ligue 1 : 3
- Liga Alef : 3
- Liga Pro Serie B : 3
- Paraibano 2 : 3
- Botola Pro : 3
- League Cup : 2
- Tercera División RFEF - Group 15 : 2
- Primera División : 2
- Primera B : 2
- Serie A : 2
- Serie B : 2
- Friendlies Women : 1
- Pro League A : 1
- Stars League : 1
- Svenska Cupen - Women : 1
- Türkiye Kupası : 1
- Division 2 - Norra Götaland : 1
- Liga I : 1
- Iraqi League : 1
- Ligi kuu Bara : 1
- Tercera División RFEF - Group 16 : 1
- Premier Division : 1
- Copa de la División Profesional : 1
- Segunda División : 1
- Primera A : 1

## Règlements automatiques de la veille

Aucun.

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 130,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8862,
  "modèle": 0.9248,
  "mélange": 0.8862
 },
 "delta_vs_marche": {
  "modèle": [
   0.0387,
   [
    -0.0091,
    0.0908
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
 "periode": "2026-09-29 → 2026-10-05"
}
```
