# APEX-BSM — relevé automatique du 2026-10-07

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-10-03 (+27 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Coppa Italia Serie D : 32
- Türkiye Kupası : 24
- Cup : 14
- Emperor Cup : 10
- Football League - Highland League : 9
- Copa do Brasil U20 : 8
- Copa Venezuela : 8
- Gamma Ethniki - Group 10 : 6
- Ligue 1 : 6
- Reserve League : 6
- U19 League : 5
- Third League - Southwest : 5
- Premier League : 5
- Goiano - 2 : 5
- Serie A : 5
- Serie B : 5
- WK-League : 4
- League Cup : 4
- Segunda División : 4
- Brasileiro U17 : 4
- League Two : 3
- League One : 3
- Pro League A : 3
- ASEAN Club Championship : 3
- Friendlies Women : 2
- Canadian Premier League : 2
- 1. Liga Classic - Group 3 : 2
- Paraibano 2 : 2
- Gaúcho - 3 : 2
- Copa Uruguay : 2
- Liga Pro Serie B : 2
- Primera B : 2
- Copa Chile : 2
- Piauiense - 2 : 2
- USL League One : 2
- 3. liga - CFL B : 1
- Segunda División RFEF - Group 5 : 1
- Ýokary Liga : 1
- Birinci Dasta : 1
- Central Youth League : 1

## Règlements automatiques de la veille

- Mauritius – Sri Lanka : JOUE 2-3
- Cyprus – Latvia : JOUE 2-1
- France – Belgium : JOUE 4-1
- Italy – Türkiye : JOUE 3-1
- Ukraine – Hungary : JOUE 1-2
- Northern Ireland – Georgia : JOUE 0-0
- Bosnia & Herzegovina – Poland : JOUE 1-0
- Romania – Sweden : JOUE 0-1
- Montenegro – Armenia : JOUE 0-0
- Liechtenstein – Gibraltar : JOUE 0-2
- Guadeloupe – St. Lucia : JOUE 0-1
- Cuba – St. Kitts and Nevis : JOUE 2-2
- Martinique – El Salvador : JOUE 1-1

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
