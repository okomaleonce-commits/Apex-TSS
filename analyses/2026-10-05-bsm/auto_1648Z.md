# APEX-BSM — relevé automatique du 2026-10-05

- Clubs : backtest `bsm-20260929T010237Z` — statut **NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire**
- Sélections : conversion `intl-20260929T011417Z` — H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques) ; Elo à jour au 2026-10-03 (+0 résultats ajoutés)
- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`

## Sélections

Aucune sélection (vetos du protocole).

## Tous les matchs simulés (UTC)

| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |
|---|---|---|---|---|---|---|---|

## Hors périmètre (non simulés)

- Primera C : 4
- Carioca B2 : 4
- Liga Profesional Argentina : 3
- Paraibano 2 : 3
- Friendlies : 2
- Primera B Metropolitana : 2
- Division Profesional - Clausura : 2
- Division Intermedia : 2
- Division One League : 1
- CAF U23 Cup of Nations : 1
- Ligue 1 : 1
- Torneo Promocional Amateur : 1
- 1. Division : 1
- 3. Division - Girone 6 : 1
- Division 2 - Norra Götaland : 1
- Segunda División : 1
- Non League Premier - Isthmian : 1
- Primera Nacional : 1
- Copa De La Liga : 1
- Primera B : 1
- Premier League : 1
- Mineiro - 3 : 1
- Primera División : 1

## Règlements automatiques de la veille

Aucun.

## Évaluation en avant — modèle ancré sur le marché

```
{
 "n_matchs_regles": 117,
 "poids_melange_w": 0.0,
 "logloss": {
  "marché": 0.8746,
  "modèle": 0.9034,
  "mélange": 0.8746
 },
 "delta_vs_marche": {
  "modèle": [
   0.0288,
   [
    -0.0195,
    0.0818
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
