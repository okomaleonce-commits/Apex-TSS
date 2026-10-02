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
python3 tools/apex_worm.py scan --money --email     # + volume excapper + notification email
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

## Argent public : excapper (public) + arbworld (sous réserve)

Avec `--money`, le scan collecte **excapper** (`tools/apex_excapper.py`) : le tableau « Betfair MoneyWay »
de la page d'accueil est **rendu côté serveur, public, sans clé ni login**. Il donne le **volume d'argent
réellement matché** par match sur l'échange Betfair — la donnée de volume que les bookmakers cachent.
Chaque fixture API-Football est appariée par similarité de noms ; le volume réel alimente la composante
`volume` du moteur Sharp (`OBSERVED`).

Collecte éthique (spec §6) : uniquement des pages publiques, `robots.txt` respecté (excapper autorise
tout), User-Agent identifiable, rythme raisonnable. **Aucun contournement de login, paywall ou CAPTCHA.**

**arbworld** (`tools/apex_arbworld.py`) sert ses cotes via `/api/`, que son `robots.txt` **interdit** —
donc pas de scraping. Il n'est utilisé que si tu fournis une **API autorisée** (`ARBWORLD_API_URL` +
`ARBWORLD_KEY`) ; sinon la composante arbitrage reste `UNAVAILABLE`.

Sans `--money`, ou si un match n'a pas de volume excapper, ces composantes restent `UNAVAILABLE` — jamais
estimées (spec §34).

## Moteurs (spec §13-18)

- **Sharp** : trajectoire de ligne entre nos relevés, consensus inter-books (dispersion), écart
  Pinnacle↔médiane, et — avec `--money` — **volume d'argent réel** (excapper). La confirmation d'échange
  et le RLM réel s'activent si la répartition d'argent par issue est connue ; sinon ils restent
  `UNAVAILABLE`.
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

## Décision de marché (spec §22, §44)

Un signal ne reste pas « à surveiller » : il conduit à une **décision**. Chaque match assez documenté reçoit
un palier — `JOUER` (signal fort ≥70, DQ ≥65, ET confirmation d'échange ou value 1X2 directe), `JOUER_PETIT`
(≥55, DQ ≥50), `SURVEILLER` (≥45), sinon `NO BET` — avec le marché retenu et une **mise en unités
indicatives de suivi** (1.0 / 0.5 / 0.25 / 0). Ce ne sont pas des conseils de mise : la cote est à vérifier
et horodater avant tout pari, et le modèle structurel ne bat pas le marché. La value directe n'est
`CONFIRMÉE` que sur un marché réellement price (1X2).

## Email (notification, spec §36-38)

Avec `--email`, le scan écrit `reports/worm/<jour>.email.html` (digest mis en forme : décisions du jour,
matchs en direct, meilleures anomalies) et l'envoie par SMTP si `WORM_SMTP_HOST/USER/PASS` + `WORM_EMAIL_TO`
sont configurés (secrets CI). En session interactive, le même HTML peut être envoyé via le connecteur Gmail.

### Canal d'envoi réel dans cette installation : connecteur Gmail (MCP), pas SMTP

Les secrets `WORM_SMTP_*` **ne sont pas configurés** dans cet environnement : `--email` écrit donc le HTML
(`reports/worm/<jour>.email.html`) mais **n'envoie rien** par SMTP (il affiche « Email non envoyé :
WORM_SMTP_* non configurés »). L'envoi réel se fait **en session interactive via le connecteur Gmail**
(outil `mcp__Gmail__send_message`), depuis l'adresse du compte (`okoma.leonce@gmail.com`), auth OAuth gérée
par le connecteur — aucun mot de passe ni token manipulé dans le code ou le chat.

Procédure à chaque passage, en session :
1. `python3 tools/apex_worm.py scan --money --email` → snapshot + `reports/worm/<jour>.email.html` (sans envoi SMTP).
2. En Python, `apex_worm.build_email_html("<jour>")` → renvoie `(sujet, html)` (décisions, live, caractère, APEX-SYNC, et la section APEX-MI UPSET).
3. `mcp__Gmail__send_message` avec `to=["okoma.leonce@gmail.com"]`, `subject=<sujet>`, `htmlBody=<html>` (+ un `body` texte court en repli). Si le connecteur Gmail s'est déconnecté (« MCP server disconnected »), recharger l'outil via `ToolSearch` avant d'envoyer.
4. Preuve d'envoi = l'`id`/`threadId` Gmail renvoyé par l'outil.

Pour un envoi **100 % autonome sans modèle** (par ex. depuis le cron GitHub Actions ou une Routine à session fraîche sans connecteur), il faut renseigner les secrets `WORM_SMTP_*` ; le script enverra alors seul.

L'email joint automatiquement une section **« APEX-MI — bruit de marché H-60 (focus UPSET) »** : à chaque
passage, le scan active la cellule `apex-market-intel-team` sur les matchs en PREMATCH dont le coup d'envoi
est dans l'heure (`python3 tools/apex_mi.py worm-hook`, désactivable par `--no-mi`, fenêtre réglable par
`--mi-within`). Elle croise l'UPSET structurel de WORM avec le mouvement du marché vers l'outsider
(`UPSET_WATCH = 0.55·UPSET + 0.45·confirmation`), produit `data/worm/mi/<jour>.json` +
`reports/worm/<jour>.mi.md`, et ne price jamais — ses signaux alimentent le moteur statistique.

## Sortie

- `data/worm/snapshots/<jour>.jsonl` — historique horodaté append-only (non versionné).
- `reports/worm/<jour>.md` + `.email.html` — TOP SIGNALS, décisions, tableau, fiches (spec §36-38).

## Équipe d'agents

Le conducteur `apex-worm-conductor` lance le scan, puis délègue la lecture fine :
`apex-worm-market` (Sharp/mouvement/RLM), `apex-worm-anomaly` (Blowout/Upset/Convergence),
`apex-worm-live` (matchs en cours). Chaque agent lit le JSONL/rapport produit — il ne recalcule pas et
n'invente rien.

## Secrets (spec §33)

Tous hors du code, des logs et des commits — en CI, GitHub Actions Secrets :
`API_FOOTBALL_KEY` (ou identifiant API en en-tête via l'environnement cloud), `FOOTYSTATS_KEY`,
`WORM_SMTP_HOST` / `WORM_SMTP_USER` / `WORM_SMTP_PASS` + `WORM_EMAIL_TO` (email), et — seulement si tu as
un accès autorisé — `ARBWORLD_API_URL` / `ARBWORLD_KEY`. excapper ne demande **aucune** clé (données
publiques). `APEX_TIMEZONE` fixe le fuseau de la fenêtre.
