---
name: apex-sync-orchestrator
description: Chef d'orchestre APEX-SYNC qui synchronise le radar rapide APEX-WORM (détection d'anomalies) et le validateur lent APEX-PROTOCOL (simulation calibrée BSM). DÉCLENCHER pour transformer un scan WORM en décisions actionnables — lire le snapshot du jour, filtrer et prioriser les candidats, ne transmettre que les meilleurs au PROTOCOL, appliquer un Risk Manager strict (Kelly fractionné, plafonds d'exposition, stop-loss, anti-corrélation) et rendre un feu VERT / ORANGE / ROUGE par match. Outil mécanique : tools/apex_sync.py (candidates, sync, risk, kpi, window). Ne remplace ni apex-worm-scanner (qui produit le snapshot) ni apex-backtest-simulation / apex_bsm.py (qui price les matchs) : il les enchaîne.
---

# APEX-SYNC — orchestrateur WORM ↔ PROTOCOL

APEX-SYNC n'analyse aucun match lui-même. Il **synchronise** deux outils existants :

- **APEX-WORM** (`tools/apex_worm.py`, skill `apex-worm-scanner`) — radar rapide : scanne toute la
  fenêtre APEX et repère les anomalies (Sharp / Blowout / Upset / StatsConvergence, mouvements de ligne).
- **APEX-PROTOCOL** (`tools/apex_bsm.py simulate`, skill `apex-backtest-simulation` ; ou la chaîne
  d'agents `apex-protocol-team`) — validateur lent et rigoureux : simulation Monte-Carlo calibrée,
  dérivation des marchés, EV, veto.

```
WORM scan → candidats → filtres/priorité → PROTOCOL (bsm simulate) → Risk Manager → feu → journal
```

## Commandes

```bash
python3 tools/apex_sync.py candidates                 # lit le snapshot WORM du jour, filtre, priorise
python3 tools/apex_sync.py candidates --date 2026-10-01 --max 8 --min-priority 0.55
python3 tools/apex_sync.py sync                        # dry-run : émet la commande PROTOCOL par candidat
python3 tools/apex_sync.py sync --run-protocol --bankroll 1000   # exécute réellement bsm + Risk Manager
python3 tools/apex_sync.py risk --p 0.58 --odds 2.05 --bankroll 1000   # Risk Manager autonome
python3 tools/apex_sync.py kpi --date 2026-10-01      # KPI du pipeline (taux de GO, feux)
python3 tools/apex_sync.py window                     # fenêtre APEX courante
```

Prérequis : un snapshot WORM du jour doit exister (`python3 tools/apex_worm.py scan` d'abord).
Le PROTOCOL utilise `numpy`/`scipy` (`pip install numpy scipy`).

## Priorisation (plan §4)

Tous les signaux WORM ne vont pas au PROTOCOL. Priorité ∈ [0,1] :

```
priority = 0.4·worm_score + 0.2·liquidité + 0.2·fenêtre_temps + 0.2·poids_tags
```

- **worm_score** : meilleur score d'anomalie / 100 (`None` = absent, pas 0).
- **liquidité** : nombre de books, faible dispersion inter-books, volume d'échange si connu. Dispersion
  ou volume inconnus → terme **neutre**, jamais compté comme nul (on n'invente pas de liquidité).
- **fenêtre_temps** : palier haut 2 h–18 h avant le coup d'envoi ; décote si trop tôt (>72 h, données
  incomplètes) ou trop tard (<30 min, cotes figées).
- **poids_tags** : Sharp (1.0) > StatsConvergence (0.9) > Upset (0.7) > Blowout (0.6) ; Sharp +
  convergence simultanés renforcent la confiance.

## Portes d'entrée (plan §3.6)

Un candidat n'entre dans la file que si : phase **PREMATCH**, coup d'envoi ni trop proche ni trop
lointain, `data_quality` suffisante, meilleur signal WORM ≥ seuil, WORM ≠ `NO BET`, et une cote 1X2
existe. La file a une **capacité** (défaut 12) pour ne pas engorger le PROTOCOL ; le reste passe en
débordement, jamais perdu.

## Pont vers le PROTOCOL (plan §3.7) — honnêteté absolue

APEX-SYNC **n'invente jamais** une simulation. La commande `bsm simulate` est construite à partir du
**seul snapshot WORM** : les λ sont les **λ structurels réels** de WORM (issus du classement), les cotes
sont celles du **snapshot horodaté**. Ces λ ne sont pas comparés au marché en backtest, donc BSM les
marque lui-même **`NON VALIDÉ`** — et son filtre « écart > 10 pts avec le marché » écarte les faux bords.
Si WORM n'a pas pu estimer de λ (force d'équipe absente), le PROTOCOL est déclaré **non exécutable** et le
match reste en ORANGE, jamais forcé. Compositions absentes → `--missing-lineup` est transmis, et le
PROTOCOL pose son veto `WAIT`.

## Risk Manager (plan §6)

Appliqué à toute sélection du PROTOCOL avant le feu vert :

- **Kelly fractionné** (défaut ×0,25), borné par la **mise max** (défaut 1 % de bankroll).
- **Plafonds d'exposition** par match, par ligue, par marché.
- **Stop-loss** quotidien / hebdomadaire (passer `--pnl-day` / `--pnl-week`).
- **Anti-corrélation** : au plus **un pari par match** (aligné sur CLAUDE.md §4).
- Mise résultante sous le **plancher** (défaut 0,25 %) → ORANGE (WATCH), pas GO.

Seuils surchargeables par `--bankroll`, `--kelly-fraction`, `--min-priority`, `--queue-capacity`, ou un
fichier `--config` JSON reprenant les clés de `DEFAULT_CONFIG`.

## Feux (plan §1)

| Feu | Sens | Condition |
|-----|------|-----------|
| 🔴 **ROUGE** | NO BET | porte échouée, ou PROTOCOL en abstention / erreur, ou Risk Manager refuse |
| 🟠 **ORANGE** | WATCH | candidat valide mais PROTOCOL non exécuté, `WAIT` (lineups), ou mise sous plancher |
| 🟢 **VERT** | GO | anomalie WORM + sélection PROTOCOL (EV ≥ seuil, stable, non suspecte) + Risk Manager OK |

**WORM seul ne produit jamais un VERT** : son modèle structurel ne bat pas le marché, donc la value reste
`NON CONFIRMÉE` tant que le PROTOCOL n'a pas tranché. Le meilleur feu d'un scan non validé est ORANGE.

## Journal & feedback (plan §5, §7)

Chaque passage `sync` est journalisé en append-only dans `data/sync/decisions/<jour>.jsonl` (horodaté,
un dernier état par match par passage). La notation par résultat (ROI, Brier, log-loss) passe par le
journal du PROTOCOL : `python3 tools/apex_bsm.py settle` puis `audit`. La **CLV** reste indisponible tant
qu'aucune cote de clôture n'est relevée — écrite comme indisponible, jamais estimée.

## Ce que l'outil ne fait pas

APEX-SYNC n'est pas une promesse de gain. Les bookmakers prennent une marge et limitent les comptes
gagnants ; un backtest réussi ne garantit pas l'avenir. L'outil **optimise le processus de décision, pas
le résultat** : il sépare le bruit (WORM) du signal validé (PROTOCOL) et refuse par défaut.
