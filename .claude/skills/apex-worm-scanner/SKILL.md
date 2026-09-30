---
name: apex-worm-scanner
description: Scanner football « ver informationnel » APEX-WORM, indépendant des protocoles APEX précédents (BSM, chaîne S1-S8, hockey, turf). DÉCLENCHER pour un scan de journée orienté anomalies (pas seulement value bets) — détecter Sharp/Blowout/Upset/StatsConvergence/mouvements de ligne sur toutes compétitions dans la fenêtre APEX 08:00→07:59, historiser chaque passage, comparer au précédent et recommander un marché par match. Outil mécanique : tools/apex_worm.py (scan, report, window). Ne remplace pas apex-backtest-simulation pour un pronostic calibré sur une ligue à moteur dédié.
---

# APEX-WORM — scanner « ver informationnel »

Protocole **autonome et séparé** des autres modules APEX. Il ne cherche pas d'abord la value : il cherche
des **anomalies** exploitables (spec §45) et recommande toujours un marché quand le match est assez
documenté, sinon `NO BET`. La règle « PAS DE VALUE = PAS DE SIGNAL » ne s'applique pas ici.

## Boucle WORM (spec §28)

```
DISCOVER → COLLECT → NORMALIZE → STORE → COMPARE → ANALYZE → RANK → REPORT
```

Le moteur mécanique fait tout cela en un appel :

```bash
python3 tools/apex_worm.py scan            # journée APEX courante, toutes compétitions
python3 tools/apex_worm.py scan --leagues 39,140,135 --max-calls 300
python3 tools/apex_worm.py scan --date 2026-09-30 --max-fixtures 40
python3 tools/apex_worm.py report          # régénère reports/worm/<jour>.md depuis le dernier snapshot
python3 tools/apex_worm.py window          # affiche la fenêtre APEX courante et le fuseau
```

## Fenêtre APEX (spec §2)

Une journée va de **08:00:00 à 07:59:59** le lendemain, dans le fuseau **`APEX_TIMEZONE`**
(variable d'environnement, jamais codée en dur ; défaut UTC). Avant 08:00 on est encore dans la journée
de la veille. Le moteur range chaque coup d'envoi dans la bonne journée.

## Cycle permanent (spec §4)

Un passage par heure. Chaque passage **compare** au relevé précédent du même jour et **n'écrase jamais**
l'historique : `data/worm/snapshots/<jour>.jsonl` est append-only (un relevé horodaté par match et par
passage). La trajectoire des cotes entre relevés est elle-même une donnée (spec §4). En production, le
workflow GitHub Actions `.github/workflows/apex-worm.yml` lance le scan toutes les heures et publie le
rapport + les snapshots en **artefacts** (pas 24 commits/jour, spec §30).

## Moteurs (spec §13-18)

- **Sharp** (proxy) : trajectoire de ligne entre nos relevés, consensus inter-books (dispersion), écart
  Pinnacle↔médiane. **Le volume de mises et le % de parieurs publics ne sont PAS disponibles** dans la
  source (API-Football) : ces composantes sont marquées `UNAVAILABLE`, jamais estimées. Le Reverse Line
  Movement complet (§14) exige le côté public → non calculable ici, à ne pas prétendre.
- **Blowout** (§15) : supériorité multidimensionnelle (proba marché du favori, écart de points/match,
  écart de différence de buts, avantage terrain).
- **Upset** (§16) : outsider sous-évalué (petit écart de niveau malgré une cote généreuse).
- **StatsConvergence** (§17) : nombre de familles indépendantes pointant vers Over/Under 2.5.
- **Divergence** (§18) : contradiction stats↔marché signalée, avec pistes de cause.

Chaque score est borné 0-100 ; `None` quand la donnée nécessaire (classement) manque — on ne bricole pas
un score sur du vide.

## Provenance obligatoire (spec §34)

Toute valeur porte `OBSERVED` / `CALCULATED` / `INFERRED` / `UNCONFIRMED`, et `UNAVAILABLE` quand la
source ne fournit pas la donnée. **Ne jamais transformer une rumeur ou une absence en fait.**

## Probabilités et value (spec §19-20)

Trois probabilités distinctes : `MODEL` (Poisson structurel depuis le classement), `MARKET` (cotes
démarginées), `ADJUSTED` (ancrée sur le marché, poids modèle faible car le modèle ne bat pas le marché).
La value reste un indicateur : `EV = p × cote − 1`, mais **n'est plus obligatoire**. Un marché peut être
signalé pour Sharp/Blowout/Upset/Convergence même sans value ; le rapport écrit alors
`VALUE: NON CONFIRMÉE`. On n'invente jamais un pari pour remplir une case ; `NO BET` est une conclusion
valide (spec §22).

## Sortie

- `data/worm/snapshots/<jour>.jsonl` — historique horodaté append-only (non versionné).
- `reports/worm/<jour>.md` — TOP SIGNALS, tableau principal, fiches détaillées (spec §36-38).

## Équipe d'agents

Le conducteur `apex-worm-conductor` lance le scan, puis délègue la lecture fine :
`apex-worm-market` (Sharp/mouvement/RLM), `apex-worm-anomaly` (Blowout/Upset/Convergence),
`apex-worm-live` (matchs en cours). Chaque agent lit le JSONL/rapport produit — il ne recalcule pas et
n'invente rien.

## Secrets (spec §33)

`API_FOOTBALL_KEY` (ou identifiant API injecté en en-tête par l'environnement cloud) et
`FOOTYSTATS_KEY` restent hors du code, des logs et des commits. En CI : GitHub Actions Secrets.
