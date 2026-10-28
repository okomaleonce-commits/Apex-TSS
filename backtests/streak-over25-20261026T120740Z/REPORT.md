# Backtest APEX-STREAK — streak-over25-20261026T120740Z

- Marché : **over25**
- Ligues : E0, E1, E2, E3, SP1, I1, D1, F1
- Attente (« stat contredite ») : Pinnacle dé-viggé (football-data ou25)
- Split : amorçage ['2122', '2223'] · validation ['2324', '2425'] · **test ['2526']**
- Seuils gelés (appris en validation) : `{'N': 3, 'C': 0.5, 'Q': 0.8, 'F': 6}`

## Verdict

**NON VALIDÉ — ROI test ≤ 0 ou IC95 franchit 0 → WATCH, jamais JOUER (J26)**

## Test final (isolé)

| n paris | hits | hit-rate | ROI | ROI IC95 |
|---|---|---|---|---|
| 28 | 18 | 0.6429 | 0.1518 | [-0.1854, 0.4547] |

> J26 : signal WATCH tant que ROI test n'est pas > 0 de façon stable. Les facteurs latents (prématch→postmatch) restent observationnels (`bet_authority=false`). Aucun seuil inventé : grille apprise en validation, gelée.