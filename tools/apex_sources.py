#!/usr/bin/env python3
"""APEX-SOURCES — couche d'agrégation de sources externes pour renforcer la ROBUSTESSE des modèles.

Objectif : fournir à ORION/APEX des VOIX INDÉPENDANTES supplémentaires, en particulier
  • une voix « marché sharp » : probabilités Pinnacle DÉ-VIGGÉES (la référence des pros) ;
  • une voix « xG » : buts attendus (FBref/Understat) ;
sans jamais rien inventer. Une source non joignable / non configurée renvoie None AVEC une raison.

Règles NON négociables (héritées de l'audit APEX) :
  1. ANTI-INVENTION : donnée absente = None + raison écrite. On ne fabrique JAMAIS une cote/proba.
  2. SOURCES HONNÊTES : chaque valeur porte son origine réelle (provider). La Poisson-classement
     WORM n'est jamais étiquetée « sharp » ni « xG ».
  3. AJOUTER DES SOURCES NE LÈVE PAS LE GEL : plus de données ≠ bord. Le CLV reste juge (apex_clv).
  4. Le dé-vigging retire la marge du book pour estimer la proba « vraie » implicite ; c'est une
     estimation, pas une vérité — surtout hors clôture.

Sources REST/MCP (Infersports, SSB, SharpAPI, odds-api.io, TheStatsAPI, Apify) : branchées via
variables d'environnement / connecteurs MCP côté client. Tant qu'elles ne sont pas configurées,
leurs adaptateurs renvoient un statut « non configuré » — honnête, jamais simulé. Voir SOURCES.md.

Sources réellement joignables et CÂBLÉES par défaut ici :
  • football-data.co.uk (CSV gratuit, Pinnacle ouverture PSH/PSD/PSA + clôture PSCH/PSCD/PSCA + O/U 2.5).
    Benchmark CLV de référence (clôture = vraie ligne sharp a posteriori).
  • The Odds API (REST, clé ODDS_API_KEY) : Pinnacle dé-viggé en DIRECT pré-match — complète
    football-data pour les matchs à venir (dont la clôture n'existe pas encore). Clé lue depuis
    l'environnement uniquement, jamais codée en dur.
"""
from __future__ import annotations

import csv
import io
import os
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

try:
    import apex_worm as W   # réutilise la normalisation de noms d'équipes
    _norm = W.norm_name
    _sim = W.name_sim
except Exception:  # pragma: no cover - fallback si apex_worm indisponible
    def _norm(s): return "".join(c for c in str(s).lower() if c.isalnum())
    def _sim(a, b): return 1.0 if _norm(a) == _norm(b) else 0.0

CACHE = ROOT / "data" / "sources"
CACHE.mkdir(parents=True, exist_ok=True)

# Divisions football-data.co.uk ↔ codes APEX (mêmes codes que le backtest BSM).
FD_DIVS = {"E0", "E1", "E2", "E3", "EC", "SP1", "SP2", "I1", "I2", "D1", "D2", "F1", "F2",
           "N1", "B1", "P1", "T1", "G1", "SC0", "SC1"}
FD_SEASON = os.environ.get("APEX_FD_SEASON", "2526")   # saison courante par défaut (2025/26)


# ───────────────────────── dé-vigging (retrait de marge) ─────────────────────────
def devig(odds: list[float]) -> list[float] | None:
    """Probabilités dé-viggées (méthode proportionnelle) depuis des cotes décimales.
    Retire l'overround du book. Renvoie None si une cote est invalide."""
    try:
        inv = [1.0 / float(o) for o in odds if o and float(o) > 1.0]
    except (TypeError, ValueError):
        return None
    if len(inv) != len(odds) or not inv:
        return None
    s = sum(inv)
    if s <= 0:
        return None
    return [x / s for x in inv]


# ───────────────────────── football-data.co.uk (Pinnacle, gratuit) ─────────────────────────
def _fd_url(div: str) -> str:
    return f"https://www.football-data.co.uk/mmz4281/{FD_SEASON}/{div}.csv"


def _fd_fetch(div: str, max_age_h: float = 6.0) -> list[dict] | None:
    """Télécharge (et met en cache) le CSV football-data d'une division. None si injoignable."""
    if div not in FD_DIVS:
        return None
    path = CACHE / f"fd_{FD_SEASON}_{div}.csv"
    fresh = path.exists() and (time.time() - path.stat().st_mtime) < max_age_h * 3600
    if not fresh:
        try:
            req = urllib.request.Request(_fd_url(div), headers={"User-Agent": "Mozilla/5.0 APEX"})
            with urllib.request.urlopen(req, timeout=30) as r:    # noqa: S310 (URL fixe, domaine connu)
                data = r.read()
            if data and len(data) > 500:
                path.write_bytes(data)
        except Exception:
            if not path.exists():
                return None   # injoignable et pas de cache → absent (honnête)
    try:
        text = path.read_text(encoding="latin-1", errors="replace")
        return list(csv.DictReader(io.StringIO(text)))
    except OSError:
        return None


def pinnacle_devig(div: str, home: str, away: str, *, prefer_close: bool = True):
    """Probabilités Pinnacle DÉ-VIGGÉES (1X2 et O/U 2.5) pour un match d'une division football-data.

    Renvoie un dict {source, phase(open|close), p1x2, over25, odds} ou {"absent": raison}.
    Jamais inventé : si la division n'est pas couverte, le CSV injoignable, ou le match introuvable,
    on renvoie une raison explicite.
    """
    if div not in FD_DIVS:
        return {"absent": f"division {div} hors football-data"}
    rows = _fd_fetch(div)
    if not rows:
        return {"absent": "football-data injoignable (et pas de cache)"}

    # Appariement flou des noms (football-data a ses propres libellés).
    best, bestsc = None, 0.0
    for r in rows:
        h, a = r.get("HomeTeam", ""), r.get("AwayTeam", "")
        if not h or not a:
            continue
        sc = (_sim(home, h) + _sim(away, a)) / 2
        if sc > bestsc:
            best, bestsc = r, sc
    if not best or bestsc < 0.6:
        return {"absent": f"match introuvable dans {div} (meilleur score {bestsc:.2f})"}

    def f(k):
        try:
            return float(best.get(k) or "")
        except (TypeError, ValueError):
            return None

    # Clôture d'abord (PSC*) — la vraie référence sharp ; sinon ouverture (PS*).
    for phase, (kh, kd, ka, ko_over, ko_under) in (
        ("close", ("PSCH", "PSCD", "PSCA", "PC>2.5", "PC<2.5")),
        ("open", ("PSH", "PSD", "PSA", "P>2.5", "P<2.5")),
    ):
        if not prefer_close and phase == "close":
            continue
        oh, od, oa = f(kh), f(kd), f(ka)
        p = devig([oh, od, oa]) if None not in (oh, od, oa) else None
        over = None
        oo, ou = f(ko_over), f(ko_under)
        if None not in (oo, ou):
            dv = devig([oo, ou])
            over = dv[0] if dv else None
        if p:
            return {"source": "pinnacle_footballdata", "phase": phase, "match": f"{best['HomeTeam']} - {best['AwayTeam']}",
                    "p1x2": {"home": round(p[0], 4), "draw": round(p[1], 4), "away": round(p[2], 4)},
                    "over25": round(over, 4) if over is not None else None,
                    "odds_1x2": [oh, od, oa], "score_appariement": round(bestsc, 2)}
    return {"absent": "cotes Pinnacle absentes pour ce match (colonnes PS vides)"}


# ───────────────────────── adaptateurs REST/MCP (à configurer côté client) ─────────────────────────
def _needs(env_or_mcp: str) -> dict:
    return {"absent": f"source non configurée ({env_or_mcp}) — voir SOURCES.md"}


def infersports(home, away):
    """MCP INFERSPORT (Pinnacle + books ASIATIQUES dé-viggés — couvre Chine/Asie).
    Connecté via `claude mcp add --transport http infersports https://api.infersports.dev/mcp`.

    Les outils MCP (`mcp__INFERSPORT__get_sharp_line`, `find_value`, `find_arbitrage`,
    `get_opening_line`) sont appelables par l'AGENT EN SESSION, pas par ce sous-processus Python.
    Le flux est donc : l'agent appelle get_sharp_line, passe le résultat à parse_infersports_sharp()
    ci-dessous, et injecte la voix dans apex_fusion.orion_votes(sharp=...).
    Tier gratuit : 200 req/jour/IP (quota partagé sur l'IP de l'environnement)."""
    return _needs("MCP INFERSPORT — appelé par l'agent en session (voir parse_infersports_sharp)")


def parse_infersports_sharp(result: dict, market: str = "over25"):
    """Transforme une sortie INFERSPORT (get_sharp_line, format probability de préférence) en voix
    ORION {p, source, provider, phase} ou {"absent": raison}. Défensif : cherche les champs usuels
    sans rien inventer. `market` ∈ {over25, under25, home, draw, away}.

    N.B. le schéma exact varie ; on tente plusieurs chemins (fair/devig/probability) et on échoue
    proprement si la proba de l'issue demandée n'est pas trouvée.
    """
    if not isinstance(result, dict):
        return {"absent": "résultat INFERSPORT non exploitable"}
    if result.get("status") == "ambiguous":
        return {"absent": "fixture ambigu côté INFERSPORT (ask_user)"}
    # Cherche un bloc de probabilités dé-viggées, quel que soit son nom.
    cand = {}
    for key in ("fair_probability", "fair_prob", "devig", "probability", "fair", "comparison"):
        v = result.get(key)
        if isinstance(v, dict):
            cand = v
            break
    key_map = {"over25": ("over", "over_2_5", "o2.5", "over25"),
               "under25": ("under", "under_2_5", "u2.5", "under25"),
               "home": ("home", "1", "h"), "draw": ("draw", "x", "d"), "away": ("away", "2", "a")}
    for k in key_map.get(market, ()):  # essaie les alias du marché
        for src in (cand, result):
            if isinstance(src, dict) and k in src:
                try:
                    p = float(src[k])
                    if p > 1.0:            # cote décimale → proba
                        p = 1.0 / p
                    if 0.0 < p <= 1.0:
                        return {"p": round(p, 4), "source": "marche_sharp",
                                "provider": "infersports", "phase": "live", "market": market}
                except (TypeError, ValueError):
                    pass
    return {"absent": f"proba sharp introuvable pour {market} dans le résultat INFERSPORT"}


def ssb_sharp(home, away):
    """MCP SSB (PropProfessor) — signaux sharp coordonnés. Idem : MCP côté client."""
    return _needs("MCP ssb / PROPPROFESSOR_TOKEN")


def sharpapi(home, away):
    """SharpAPI (MCP connecté : mcp__SHARPAPI__*) — 30+ books, Pinnacle de référence, EV/arbitrage,
    closing lines. Comme INFERSPORT, les outils MCP sont appelables par l'AGENT EN SESSION, pas par ce
    sous-processus. Flux : l'agent appelle get_event_odds / find_ev_opportunities / get_closing_lines,
    passe le résultat à parse_sharpapi_sharp() ci-dessous, et injecte la voix dans orion_votes(sharp=…).
    En REST autonome (cron), poser SHARPAPI_KEY et brancher l'appel HTTP ici."""
    if os.environ.get("SHARPAPI_KEY"):
        return {"absent": "SharpAPI REST à brancher (clé présente) ; sinon MCP via l'agent"}
    return _needs("MCP SHARPAPI (agent) ou SHARPAPI_KEY (REST)")


def parse_sharpapi_sharp(row: dict, market: str = "over25"):
    """Transforme une ligne SharpAPI (find_ev_opportunities ou get_event_odds dé-viggé) en voix ORION
    {p, source=marche_sharp, provider=sharpapi, ev, phase} ou {absent}. Anti-invention : sans
    fair_probability exploitable, renvoie {absent}. `market` sert d'étiquette (SharpAPI donne déjà la
    proba juste de la sélection concernée)."""
    if not isinstance(row, dict):
        return {"absent": "ligne SharpAPI non exploitable"}
    fp = row.get("fair_probability")
    try:
        p = float(fp)
    except (TypeError, ValueError):
        return {"absent": "fair_probability absente dans la ligne SharpAPI"}
    if not (0.0 < p <= 1.0):
        return {"absent": f"fair_probability hors bornes ({fp})"}
    out = {"p": round(p, 4), "source": "marche_sharp", "provider": "sharpapi",
           "market": market, "phase": "prematch"}
    for k in ("ev_percentage", "ev_calibrated", "kelly_percent", "quality_tier",
              "confidence", "sharp_book", "warnings"):
        if k in row:
            out[k] = row[k]
    return out


def oddsapi_io(home, away):
    key = os.environ.get("ODDSAPI_IO_KEY")
    if not key:
        return _needs("ODDSAPI_IO_KEY")
    return {"absent": "adaptateur odds-api.io à brancher (clé présente)"}


def thestatsapi(home, away):
    key = os.environ.get("THESTATSAPI_KEY")
    if not key:
        return _needs("THESTATSAPI_KEY")
    return {"absent": "adaptateur TheStatsAPI à brancher (clé présente)"}


# ───────────────────────── The Odds API (ODDS_API_KEY, Pinnacle live pré-match) ─────────────────────────
# Divisions football-data ↔ sport_key The Odds API. Clés VÉRIFIÉES dans le repo
# (tools/apex_resolve.py : ENGINE_BY_SPORT_KEY / LEAGUE_ALIASES). Une division absente
# d'ici n'est PAS devinée : on renvoie "absent" plutôt qu'un sport_key inventé.
DIV_TO_ODDS_SPORT = {
    "E0":  "soccer_epl",
    "E1":  "soccer_efl_champ",
    "SP1": "soccer_spain_la_liga",
    "SP2": "soccer_spain_segunda_division",
    "I1":  "soccer_italy_serie_a",
    "D1":  "soccer_germany_bundesliga",
    "F1":  "soccer_france_ligue_one",
    "N1":  "soccer_netherlands_eredivisie",
    "SC0": "soccer_spl",
}
TOA_BASE = "https://api.the-odds-api.com/v4"


def _toa_fetch(sport_key: str, max_age_min: float = 10.0):
    """Cotes h2h + totals d'un sport The Odds API (cache ~10 min). None si clé absente/injoignable.
    La clé vient UNIQUEMENT de l'environnement (ODDS_API_KEY), jamais codée en dur (règle CLAUDE.md)."""
    key = os.environ.get("ODDS_API_KEY")
    if not key:
        return None
    path = CACHE / f"toa_{sport_key}.json"
    fresh = path.exists() and (time.time() - path.stat().st_mtime) < max_age_min * 60
    if not fresh:
        from urllib.parse import urlencode
        qs = urlencode({"apiKey": key, "regions": "eu", "markets": "h2h,totals",
                        "oddsFormat": "decimal", "dateFormat": "iso"})
        url = f"{TOA_BASE}/sports/{sport_key}/odds/?{qs}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 APEX"})
            with urllib.request.urlopen(req, timeout=30) as r:    # noqa: S310 (domaine connu)
                data = r.read()
            if data and data.strip().startswith(b"["):
                path.write_bytes(data)
        except Exception:
            if not path.exists():
                return None
    try:
        import json as _json
        return _json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def the_odds_api(div: str, home: str, away: str):
    """Pinnacle DÉ-VIGGÉ en DIRECT (pré-match) via The Odds API (ODDS_API_KEY).

    Complète football-data : donne la cote sharp AVANT que le CSV de clôture n'existe (match à venir).
    Renvoie {source, phase:"prematch", p1x2, over25, odds_1x2, score_appariement} ou {"absent": raison}.
    Anti-invention : clé absente, division non mappée, match introuvable ou colonnes vides → raison écrite."""
    if not os.environ.get("ODDS_API_KEY"):
        return _needs("ODDS_API_KEY")
    sport_key = DIV_TO_ODDS_SPORT.get(div)
    if not sport_key:
        return {"absent": f"division {div} non mappée vers The Odds API"}
    events = _toa_fetch(sport_key)
    if not events:
        return {"absent": "The Odds API injoignable / aucune cote (et pas de cache)"}

    best, bestsc = None, 0.0
    for ev in events:
        h, a = ev.get("home_team", ""), ev.get("away_team", "")
        if not h or not a:
            continue
        sc = (_sim(home, h) + _sim(away, a)) / 2
        if sc > bestsc:
            best, bestsc = ev, sc
    if not best or bestsc < 0.6:
        return {"absent": f"match introuvable dans {sport_key} (meilleur score {bestsc:.2f})"}

    books = best.get("bookmakers", []) or []
    bk = next((b for b in books if b.get("key") == "pinnacle"), None)
    provider = "the_odds_api/pinnacle"
    if bk is None:                     # pas de Pinnacle → premier book, étiqueté honnêtement
        bk = books[0] if books else None
        provider = f"the_odds_api/{bk.get('key')}" if bk else None
    if bk is None:
        return {"absent": "aucun book pour ce match"}

    mkts = {m.get("key"): m for m in bk.get("markets", [])}
    p1x2, odds_1x2 = None, None
    h2h = mkts.get("h2h")
    if h2h:
        oh = od = oa = None
        for o in h2h.get("outcomes", []):
            nm, pr = o.get("name", ""), o.get("price")
            if nm == best["home_team"]:
                oh = pr
            elif nm == best["away_team"]:
                oa = pr
            elif nm.lower() == "draw":
                od = pr
        if None not in (oh, od, oa):
            dv = devig([oh, od, oa])
            if dv:
                p1x2 = {"home": round(dv[0], 4), "draw": round(dv[1], 4), "away": round(dv[2], 4)}
                odds_1x2 = [oh, od, oa]

    over25 = None
    tot = mkts.get("totals")
    if tot:
        oo = ou = None
        for o in tot.get("outcomes", []):
            try:
                pt = float(o.get("point") or 0)
            except (TypeError, ValueError):
                continue
            if abs(pt - 2.5) < 1e-9:
                if o.get("name", "").lower() == "over":
                    oo = o.get("price")
                elif o.get("name", "").lower() == "under":
                    ou = o.get("price")
        if None not in (oo, ou):
            dv = devig([oo, ou])
            over25 = round(dv[0], 4) if dv else None

    if p1x2 is None and over25 is None:
        return {"absent": "ni 1X2 ni O/U 2.5 exploitables pour ce match"}
    return {"source": provider, "phase": "prematch",
            "match": f"{best['home_team']} - {best['away_team']}",
            "p1x2": p1x2, "over25": over25, "odds_1x2": odds_1x2,
            "score_appariement": round(bestsc, 2)}


# ───────────────────────── voix consolidée pour ORION ─────────────────────────
def sharp_voice(div: str, home: str, away: str, market: str = "over25"):
    """Renvoie une proba « marché sharp » exploitable comme VOIX ORION, ou None + raison.

    Priorité : Pinnacle dé-viggé (football-data) → Infersports → SSB → SharpAPI → odds-api.io.
    `market` ∈ {"over25","home","draw","away"}. La proba renvoyée est celle du marché demandé.
    """
    trace = []
    pin = pinnacle_devig(div, home, away)
    trace.append(("pinnacle_footballdata", pin.get("absent", "ok")))
    if "p1x2" in pin:
        if market == "over25":
            val = pin.get("over25")
        else:
            val = pin["p1x2"].get(market)
        if val is not None:
            return {"p": val, "source": "marche_sharp", "provider": pin["source"],
                    "phase": pin["phase"], "market": market, "trace": trace}

    # The Odds API (ODDS_API_KEY) : Pinnacle dé-viggé en DIRECT pré-match — couvre les matchs
    # à venir que football-data n'a pas encore (clôture inexistante avant le coup d'envoi).
    toa = the_odds_api(div, home, away)
    trace.append(("the_odds_api", toa.get("absent", "ok")))
    if not toa.get("absent"):
        val = toa.get("over25") if market == "over25" else (toa.get("p1x2") or {}).get(market)
        if val is not None:
            return {"p": val, "source": "marche_sharp", "provider": toa["source"],
                    "phase": toa["phase"], "market": market, "trace": trace}

    for name, fn in (("infersports", infersports), ("ssb", ssb_sharp),
                     ("sharpapi", sharpapi), ("oddsapi_io", oddsapi_io)):
        r = fn(home, away)
        trace.append((name, r.get("absent", "ok")))
    return {"p": None, "source": "marche_sharp", "raison": "aucune source sharp disponible", "trace": trace}


# ───────────────────────── CLV réel vs clôture sharp ─────────────────────────
CLOSING = CACHE / "closing"


def closing_clv(entry_odd: float, fair_prob_close: float):
    """CLV d'un pari : as-tu battu la ligne de CLÔTURE sharp ?

    entry_odd = cote décimale obtenue à l'entrée. fair_prob_close = proba JUSTE (dé-viggée Pinnacle)
    de l'issue à la clôture. La cote juste de clôture = 1/fair_prob_close. On a battu la clôture si
    entry_odd > cote juste de clôture, i.e. CLV = entry_odd * fair_prob_close − 1 > 0.
    (C'est l'EV du pari évalué au prix sharp de clôture — le meilleur prédicteur de bord réel.)
    Renvoie None si entrées invalides (anti-invention)."""
    try:
        eo, p = float(entry_odd), float(fair_prob_close)
    except (TypeError, ValueError):
        return None
    if eo <= 1.0 or not (0.0 < p <= 1.0):
        return None
    return round(eo * p - 1.0, 4)


def load_closing_cache(day: str) -> dict:
    """Lit le cache de clôtures sharp écrit par l'agent (via MCP). Absent = {} (jamais inventé).
    Format : data/sources/closing/<day>.json = {"<home>|<away>": {"<market>": fair_prob_close}}."""
    p = CLOSING / f"{day}.json"
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_closing_cache(day: str, entries: dict):
    """Écrit/fusionne le cache de clôtures sharp (l'agent y dépose les get_closing_lines dé-viggés)."""
    import json
    CLOSING.mkdir(parents=True, exist_ok=True)
    cur = load_closing_cache(day)
    cur.update(entries or {})
    (CLOSING / f"{day}.json").write_text(json.dumps(cur, ensure_ascii=False), encoding="utf-8")
    return len(cur)


def sharp_close_for(day: str, home: str, away: str, market: str = "over25"):
    """Proba juste de clôture (sharp) pour un match/marché depuis le cache. None + raison sinon."""
    cache = load_closing_cache(day)
    if not cache:
        return {"absent": "cache de clôtures sharp vide (l'agent ne l'a pas encore rempli)"}
    best, bestsc = None, 0.0
    for key, mk in cache.items():
        try:
            h, a = key.split("|", 1)
        except ValueError:
            continue
        sc = (_sim(home, h) + _sim(away, a)) / 2
        if sc > bestsc:
            best, bestsc = mk, sc
    if not best or bestsc < 0.6:
        return {"absent": f"match absent du cache clôtures (score {bestsc:.2f})"}
    p = best.get(market)
    if p is None:
        return {"absent": f"marché {market} absent du cache clôtures pour ce match"}
    return {"fair_prob_close": float(p), "score_appariement": round(bestsc, 2)}


def registry() -> list[dict]:
    """Inventaire honnête des sources et de leur statut de configuration."""
    return [
        {"nom": "football-data.co.uk", "type": "sharp (Pinnacle open/close) + benchmark CLV",
         "transport": "CSV", "statut": "CÂBLÉ (gratuit, joignable)", "couvre": sorted(FD_DIVS)},
        {"nom": "The Odds API", "type": "sharp (Pinnacle dé-viggé live pré-match) — 1X2 + O/U 2.5",
         "transport": "REST",
         "statut": "CÂBLÉ (ODDS_API_KEY " + ("présente)" if os.environ.get("ODDS_API_KEY") else "ABSENTE)"),
         "couvre": sorted(DIV_TO_ODDS_SPORT)},
        {"nom": "Infersports", "type": "sharp (Pinnacle + books asiatiques dé-viggés)",
         "transport": "MCP", "statut": "à ajouter côté client (claude mcp add)"},
        {"nom": "SSB / PropProfessor", "type": "signaux sharp coordonnés (31 outils)",
         "transport": "MCP", "statut": "à ajouter côté client (compte gratuit)"},
        {"nom": "SharpAPI", "type": "+EV / arbitrage (réf. Pinnacle)",
         "transport": "REST", "statut": "clé SHARPAPI_KEY requise"},
        {"nom": "odds-api.io", "type": "265+ books, dropping odds, value/arb",
         "transport": "REST", "statut": "clé ODDSAPI_IO_KEY requise"},
        {"nom": "TheStatsAPI", "type": "football, Pinnacle réf. + CLV",
         "transport": "REST", "statut": "clé THESTATSAPI_KEY requise"},
        {"nom": "Apify odds", "type": "Pinnacle + limites de mise, Kalshi",
         "transport": "REST payant", "statut": "à l'usage (~2,10$/1000 lignes)"},
        {"nom": "FBref / Understat / Sofascore", "type": "xG historiques (calibration)",
         "transport": "scrape", "statut": "via apex_footystats / scrape ponctuel"},
    ]


def _demo():
    import json
    print("=== Registre des sources ===")
    for s in registry():
        print(f"  [{s['statut'][:22]:22}] {s['nom']:22} — {s['type']}")
    print("\n=== Test Pinnacle dé-viggé (football-data) ===")
    # Exemple sur un match EPL présent dans le CSV courant (si saison en cours).
    rows = _fd_fetch("E0")
    if rows:
        last = rows[-1]
        h, a = last.get("HomeTeam"), last.get("AwayTeam")
        print(f"  dernier match E0 du CSV : {h} - {a}")
        print(" ", json.dumps(pinnacle_devig("E0", h, a), ensure_ascii=False))
    else:
        print("  (E0 injoignable)")
    print("\n=== Test The Odds API (Pinnacle dé-viggé live, ODDS_API_KEY) ===")
    if os.environ.get("ODDS_API_KEY"):
        ev = _toa_fetch("soccer_epl")
        if ev:
            e0 = ev[0]
            h, a = e0.get("home_team"), e0.get("away_team")
            print(f"  1er match EPL The Odds API : {h} - {a}")
            print(" ", json.dumps(the_odds_api("E0", h, a), ensure_ascii=False))
        else:
            print("  (soccer_epl injoignable)")
    else:
        print("  (ODDS_API_KEY absente)")


if __name__ == "__main__":
    _demo()
