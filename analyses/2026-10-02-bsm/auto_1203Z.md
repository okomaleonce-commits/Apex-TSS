# APEX-BSM — relevé automatique du 2026-10-02

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-09-28 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Friendlies Clubs : 47
- Friendlies : 11
- Second League : 6
- Ligue 2 : 6
- Regionalliga - South : 6
- Oberliga - Niederrhein : 6
- Liga III - Serie 5 : 5
- UEFA U21 Championship - Qualification : 5
- Oberliga - Hamburg : 5
- First Division : 5
- Liga III - Serie 6 : 4
- Liga III - Serie 8 : 4
- League Cup : 4
- FA Cup : 4
- Division 2 - Västra Götaland : 4
- Regionalliga - Ost : 4
- FAW Championship : 4
- Liga III - Serie 2 : 3
- Liga III - Serie 4 : 3
- Mineiro U20 : 3
- 1. Division : 3
- Botola Pro : 3
- Oberliga - Bayern Nord : 3
- 2. Division : 3
- National 2 - Group B : 3
- Serie B : 3
- Super League Women : 2
- Liga III - Serie 1 : 2
- Liga III - Serie 3 : 2
- Liga III - Serie 7 : 2
- Carioca U20 : 2
- Premier League : 2
- 1 Lyga : 2
- Cup : 2
- Premier League Women : 2
- 3. Division - Girone 2 : 2
- Ettan - Södra : 2
- Division 2 - Södra Götaland : 2
- Division 2 - Södra Svealand : 2
- Oberliga - Bayern Süd : 2

## Règlements automatiques de la veille

Aucun.

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 73,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8802,
  "modèle": 0.8874,
  "mélange": 0.8802
 },
 "delta_vs_marche": {
  "modèle": [
   0.0073,
   [
    -0.0515,
    0.0654
   ]
  ],
  "mélange": [
   -0.0,
   [
    -0.0,
    -0.0
   ]
  ]
 },
 "periode": "2026-09-29 → 2026-10-01"
}

⚠ 73 matchs seulement : bien trop peu pour conclure. Il en faut plusieurs centaines. Le journal se remplit à chaque passage quotidien.
```
