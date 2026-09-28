# Apex-TSS — consignes permanentes

## Analyses football : module BSM obligatoire

Toute analyse football, sans exception, applique le skill `apex-backtest-simulation`, outillé par `tools/apex_bsm.py`. Cela vaut pour un pronostic, un coupon, une recherche de value, un scan de journée ou un match isolé, que la chaîne d'agents soit utilisée ou non.

1. **Commencer par le bilan du backtest chronologique.** Il se trouve dans `backtests/<run_id>/REPORT.md`, avec le statut dans `backtests/latest_params.json`. S'il manque ou ne couvre pas la ligue, lancer `backtest` d'abord.
2. **Simuler chaque match avec `simulate`.** Indiquer le nombre de simulations exécutées et le statut du modèle. Les matchs hors périmètre (sélections, MLS, coupes) passent par `--lh/--la` et sont marqués NON VALIDÉS.
3. **Déduire tous les marchés des mêmes simulations.** Pour les combinés d'un même match, utiliser la probabilité jointe, jamais un produit.
4. **Au maximum un marché officiel par match**, et seulement si toutes ces conditions sont réunies : cote vérifiée et horodatée, EV ≥ 3 %, EV stable en sensibilité, aucun veto.
5. **Enregistrer chaque prévision avant le coup d'envoi** (`--record`), puis la régler après le match avec `settle`. Le journal `ledger/` est append-only.
6. **Ne jamais inventer** un backtest, une cote historique, une statistique ou une simulation. Si une donnée manque, l'écrire.

Le hockey (`apex-hockey-team`) et le turf (`apex-turf-team`) ont leurs propres protocoles.

## Environnement

- Dépendances Python du module : `pip install numpy scipy`.
- Historique mis en cache dans `data/history/`, rapports de backtest dans `backtests/`, journal dans `ledger/`.
