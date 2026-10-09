# APEX-BSM — relevé automatique du 2026-10-04

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-10-03 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Segunda División : 11
- Division One League : 10
- Primera Nacional : 10
- 2. Frauen Bundesliga : 7
- Toppserien : 6
- Eredivisie Women : 5
- Premier League : 5
- Ekstraliga Women : 4
- Gamma Ethniki - Group 6 : 4
- Second Amateur Division - ACFF : 4
- Ligue A : 4
- Championnat National : 4
- Premiership Women : 4
- Liga Profesional Argentina : 4
- Primera C : 4
- Primera A : 4
- 3. liga - West : 3
- Gamma Ethniki - Group 4 : 3
- Super League : 3
- Gamma Ethniki - Group 8 : 3
- NPFL : 3
- NWSL Women : 3
- Liga de Ascenso : 3
- Primera B : 3
- Serie C - Girone C : 3
- Torneo Federal A : 3
- Copa de la División Profesional : 3
- 3. liga - Center : 2
- 3. SNL - East : 2
- 3. liga - East : 2
- Gamma Ethniki - Group 3 : 2
- Serie D - Girone F : 2
- Segunda División RFEF - Group 2 : 2
- Tercera División RFEF - Group 1 : 2
- Ligue 1 : 2
- Primera División Femenina : 2
- Primera B Metropolitana : 2
- Primera División : 2
- Liga Mayor : 2
- Liga Panameña de Fútbol : 2

## Règlements automatiques de la veille

Aucun.

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 101,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8861,
  "modèle": 0.9169,
  "mélange": 0.8861
 },
 "delta_vs_marche": {
  "modèle": [
   0.0308,
   [
    -0.0245,
    0.0919
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
 "periode": "2026-09-29 → 2026-10-04"
}
```
