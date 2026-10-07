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

Seule source réellement joignable et CÂBLÉE par défaut ici : football-data.co.uk (CSV gratuit,
cotes Pinnacle ouverture PSH/PSD/PSA et clôture PSCH/PSCD/PSCA + O/U 2.5). Benchmark CLV de référence.
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
    """MCP Infersports (Pinnacle + books asiatiques dé-viggés). Connecté côté client :
    `claude mcp add --transport http infersports https://api.infersports.dev/mcp`.
    Depuis cette session on ne peut pas appeler le MCP du client → non configuré ici."""
    return _needs("MCP infersports (client)")


def ssb_sharp(home, away):
    """MCP SSB (PropProfessor) — signaux sharp coordonnés. Idem : MCP côté client."""
    return _needs("MCP ssb / PROPPROFESSOR_TOKEN")


def sharpapi(home, away):
    key = os.environ.get("SHARPAPI_KEY")
    if not key:
        return _needs("SHARPAPI_KEY")
    return {"absent": "adaptateur SharpAPI à brancher (clé présente) — endpoint get_ev/get_arbitrage"}


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
    for name, fn in (("infersports", infersports), ("ssb", ssb_sharp),
                     ("sharpapi", sharpapi), ("oddsapi_io", oddsapi_io)):
        r = fn(home, away)
        trace.append((name, r.get("absent", "ok")))
    return {"p": None, "source": "marche_sharp", "raison": "aucune source sharp disponible", "trace": trace}


def registry() -> list[dict]:
    """Inventaire honnête des sources et de leur statut de configuration."""
    return [
        {"nom": "football-data.co.uk", "type": "sharp (Pinnacle open/close) + benchmark CLV",
         "transport": "CSV", "statut": "CÂBLÉ (gratuit, joignable)", "couvre": sorted(FD_DIVS)},
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


if __name__ == "__main__":
    _demo()
