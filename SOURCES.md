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
