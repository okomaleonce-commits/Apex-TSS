# APEX-SOURCES — brancher les sources externes (robustesse des modèles)

But : donner à ORION/APEX des **voix indépendantes** supplémentaires — surtout une voix
**marché sharp** (Pinnacle dé-viggé) et une voix **xG** — pour sortir des matchs du `COLLECTER`
et, surtout, **mesurer le CLV** (seul juge du bord réel). Outil : `tools/apex_sources.py`.

> ⚠️ Ajouter des sources **ne lève pas le gel**. Plus de données ≠ bord. Le gel reste tant que le
> CLV cumulé n'est pas positif (`tools/apex_clv.py`). Les sources renforcent la *mesure*, pas la licence de parier.

## Ce qui est déjà câblé (rien à faire)

- **football-data.co.uk** — CSV gratuit, cotes **Pinnacle ouverture + clôture** (PSH/PSD/PSA,
  PSCH/PSCD/PSCA) et O/U 2.5, pour E0/E1/E2/E3/EC/SP1/I1/D1/F1… C'est la **référence sharp + CLV**.
  Utilisé automatiquement par `apex_fusion` comme voix `marche_sharp` (couche **K**) sur les ligues
  couvertes. Test : `python3 tools/apex_sources.py`.

## À brancher côté client (toi) — jamais simulé tant que non configuré

Ces sources renvoient « non configuré » (honnête) jusqu'à ce que tu les actives. Deux canaux :

### 1) Serveurs MCP (dans TA config Claude Code, pas depuis cette session cloud)

```bash
# Infersports — Pinnacle + books asiatiques dé-viggés (handicap quarter lines), gratuit sans clé
claude mcp add --transport http infersports https://api.infersports.dev/mcp

# SSB (Sharp Sportsbook Intelligence) — 31 outils de signaux sharp (compte PropProfessor gratuit)
# suivre la procédure d'ajout MCP fournie par PropProfessor (URL + token)
```

**INFERSPORT : CONNECTÉ ✅** — outils vus en session : `get_sharp_line` (cotes justes dé-viggées
du book sharp + best price par issue), `find_value` (+EV vs ligne sharp), `find_arbitrage`,
`get_opening_line` / `get_result` (utiles au **CLV** : ligne d'ouverture vs clôture), `scan_slate`,
`score_prob`. Il couvre les **books asiatiques** → donc des ligues que football-data ignore (Chine…).

Flux d'intégration (important) : les outils MCP sont appelables par **l'agent en session**, pas par
le sous-processus `apex_sources.py`. Donc, en passage interactif, l'agent appelle
`mcp__INFERSPORT__get_sharp_line(query, market_type, format="probability")`, passe le résultat à
`apex_sources.parse_infersports_sharp(result, market)`, et injecte la voix dans
`apex_fusion.orion_votes(..., sharp=<voix>)`. Aucune valeur n'est fabriquée : résultat ambigu ou
proba absente ⇒ voix absente.

> ⚠️ **Tier gratuit sans clé = 200 requêtes/jour/IP**, partagé sur l'IP de l'environnement cloud —
> il peut être déjà épuisé (c'est le cas au moment de ce branchement). Une clé/limite supérieure
> lève ce plafond. Quand le quota est épuisé, APEX l'écrit comme tel et n'invente rien.

SSB : à ajouter de la même façon ; adaptateur `ssb_sharp()` à brancher une fois les outils visibles.

**SHARPAPI : CONNECTÉ ✅ (tier « sharp » testé)** — `mcp__SHARPAPI__*` : `get_events`, `get_event_odds`,
`get_best_odds`, `compare_odds`, `find_ev_opportunities` (+EV ancré Pinnacle, dé-vig POWER),
`find_arbitrage`, `find_middles`, `find_low_hold`, et surtout `get_closing_lines` (**CLV réel**).
30+ books, Pinnacle en référence, couvre les grandes ligues (Bundesliga, Liga, Serie A, Europa…).

Flux (comme INFERSPORT) : l'agent appelle `mcp__SHARPAPI__find_ev_opportunities` /
`get_event_odds`, passe une ligne à `apex_sources.parse_sharpapi_sharp(row, market)` → voix ORION
`marche_sharp` (avec `fair_probability`, `ev_percentage`, `kelly_percent`, `warnings`).

> ⚠️ Les lignes +EV portent souvent `STALE_PREMATCH_ODDS` / `SINGLE_SHARP_REF` et visent des **books
> soft** (ballybet, rebet…) pas forcément accessibles localement. Ce sont des pistes à vérifier, pas
> des gains. **`get_closing_lines` est l'apport majeur : il permet enfin de mesurer le CLV réel** —
> et le CLV reste le seul juge pour (un jour) lever le gel.

### CLV réel vs clôture sharp (apex_sources)

`closing_clv(entry_odd, fair_prob_close)` = `entry_odd × fair_prob_close − 1` : **positif = on a battu
la ligne de clôture sharp** (meilleur prédicteur de bord réel). Comme le MCP n'est appelable que par
l'agent, le flux est en deux temps :
1. **L'agent** appelle `mcp__SHARPAPI__get_closing_lines` (ou `get_event_odds` dé-viggé) après coup
   d'envoi, et dépose les probas justes de clôture via `save_closing_cache(day, {"<home>|<away>":
   {"<market>": p_close}})` → `data/sources/closing/<day>.json` (non versionné).
2. **apex_clv / apex_sources** lisent ce cache (`sharp_close_for`, `closing_clv`) et calculent le CLV
   des entrées enregistrées. Cache absent ⇒ « absent », jamais de clôture inventée.

Prérequis pour des chiffres réels : des **entrées enregistrées** (APEX ne journalise rien sous gel sur
ces ligues) — donc le CLV vs sharp se mesurera dès qu'on couvrira un vrai créneau grandes ligues.

### 2) Clés API REST (variables d'environnement de l'environnement cloud)

| Source | Variable | Tier gratuit |
|---|---|---|
| SharpAPI | `SHARPAPI_KEY` | 12 req/min, sans CB |
| odds-api.io | `ODDSAPI_IO_KEY` | 100 req/h, tous endpoints |
| TheStatsAPI | `THESTATSAPI_KEY` | football, Pinnacle réf. |
| football-data season | `APEX_FD_SEASON` | ex. `2526` (défaut) |

Renseigne-les dans **Identifiants / Variables** de l'environnement cloud (jamais dans le code).
Dès qu'une clé est présente, j'active l'adaptateur correspondant (fetch réel + dé-vig + voix ORION).

## xG (calibration FORECAST)

FBref / Understat / Sofascore : via `tools/apex_footystats.py` (clé `FOOTYSTATS_KEY`) ou scrape
ponctuel. Objectif : alimenter une voix `xg` indépendante et, à terme, recalibrer BSM sur xG réels.

## Comment ça entre dans ORION

`apex_fusion.orion_votes(...)` accepte maintenant une voix `sharp` → source **`marche_sharp`**
(couche K), **indépendante** du classement WORM et du bruit MI. META la compte comme une voix
distincte ; c'est elle qui peut porter le total de voix indépendantes à ≥ 3 et donc permettre à
ORION d'**arbitrer** (ACCEPTER/REJETER/ATTENDRE) au lieu de seulement COLLECTER — sous gel, tout
« ACCEPTER » reste ATTENDRE. Anti-invention : ligue non couverte ou source absente ⇒ voix absente,
écrite absente, jamais fabriquée.
