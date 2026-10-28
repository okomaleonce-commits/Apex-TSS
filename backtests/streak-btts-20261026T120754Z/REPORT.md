# Backtest APEX-STREAK — streak-btts-20261026T120754Z

- Marché : **btts**
- Ligues : E0, E1, E2, E3, SP1, I1, D1, F1
- Attente (« stat contredite ») : base empirique antérieure de l'équipe (pas de cote BTTS)
- Split : amorçage ['2122', '2223'] · validation ['2324', '2425'] · **test ['2526']**
- Seuils gelés (appris en validation) : `{'N': 4, 'C': 0.5, 'Q': 0.8, 'F': 6}`

## Verdict

**NON VALIDÉ — BTTS : football-data ne fournit pas de cote, ROI NON CALCULABLE ; hit-rate seul, insuffisant pour parier (J25)**

## Test final (isolé)

| n paris | hits | hit-rate | ROI | ROI IC95 |
|---|---|---|---|---|
| 17 | 12 | 0.7059 | None | None |

> J26 : signal WATCH tant que ROI test n'est pas > 0 de façon stable. Les facteurs latents (prématch→postmatch) restent observationnels (`bet_authority=false`). Aucun seuil inventé : grille apprise en validation, gelée.