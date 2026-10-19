#!/usr/bin/env python3
"""Connecteur The Odds API (api.the-odds-api.com/v4) pour APEX — bruit de marché multi-books.

La clé est lue UNIQUEMENT dans la variable d'environnement ODDS_API_KEY (jamais dans le code).

Ce que cette source APPORTE à APEX
-----------------------------------
  - Beaucoup plus de bookmakers (dont Pinnacle et l'exchange Betfair) → dispersion inter-books,
    Pinnacle-vs-médiane et steam mieux alimentés dans la cellule APEX-MI et dans APEX-WORM.
  - Trajectoire ouverture→clôture réelle à partir des relevés horodatés (Closing Line Value).

Ce que cette source N'APPORTE PAS (honnêteté anti-invention)
------------------------------------------------------------
  - AUCUN pourcentage de parieurs publics ni volume. Le Reverse Line Movement (RLM) reste donc
    NON calculable. On ne stocke que des PRIX. `public_pct` est toujours null.

Commandes
---------
  status                       Vérifie la clé et le quota (en-têtes x-requests-remaining / -used).
  sports [--all]               Liste les compétitions foot actives (clés `sport_key` réelles de l'API).
  snapshot --sport KEY         Relève les cotes multi-books d'une compétition et ajoute, horodaté, chaque
          [--regions eu,uk,us] event à data/oddsapi/snapshots/<date>.jsonl (append-only). --league accepte
          [--markets h2h,totals] un alias courant (ex. « Ligue des Nations ») résolu en sport_key.
  closing --sport KEY [--date D]
                               Trajectoire ouverture (1er relevé) → clôture (dernier relevé) par event,
                               à partir du store local — pas d'appel réseau. Base honnête de CLV.

Modèle de données (compatible API-Football / apex_mi)
-----------------------------------------------------
  odds[<Nom du book>] = {"1X2": [cote_home, cote_draw, cote_away], "OU2.5": [cote_over, cote_under]}
  Les noms de books utilisent le `title` de l'API (ex. "Pinnacle") pour s'apparier au moteur APEX-MI.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from difflib import SequenceMatcher
from pathlib import Path

BASE = "https://api.the-odds-api.com/v4"
ROOT = Path(__file__).resolve().parent.parent
STORE = ROOT / "data" / "oddsapi" / "snapshots"
CA_BUNDLE = "/root/.ccr/ca-bundle.crt"

# Alias courants → sport_key The Odds API (liste non exhaustive ; `sports` donne les clés réelles vivantes).
LEAGUE_ALIASES = {
    "ligue des nations": "soccer_uefa_nations_league",
    "uefa nations league": "soccer_uefa_nations_league",
    "nations league": "soccer_uefa_nations_league",
    "premier league": "soccer_epl",
    "epl": "soccer_epl",
    "serie a": "soccer_italy_serie_a",
    "la liga": "soccer_spain_la_liga",
    "bundesliga": "soccer_germany_bundesliga",
    "ligue 1": "soccer_france_ligue_one",
    "eredivisie": "soccer_netherlands_eredivisie",
    "champions league": "soccer_uefa_champs_league",
    "ligue des champions": "soccer_uefa_champs_league",
    "europa league": "soccer_uefa_europa_league",
    "world cup": "soccer_fifa_world_cup",
    "coupe du monde": "soccer_fifa_world_cup",
    "euro": "soccer_uefa_european_championship",
}


# ───────────────────────────── HTTP ─────────────────────────────

class OddsApiError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.environ.get("ODDS_API_KEY")
    if not key:
        raise OddsApiError(
            "ODDS_API_KEY absente : définir la variable d'environnement dans les réglages de "
            "l'environnement cloud (jamais de clé en dur, jamais collée en clair)."
        )
    return key


def _ssl_context() -> ssl.SSLContext:
    # Fait confiance au CA du proxy d'agent si présent, sinon trust système par défaut.
    if os.path.exists(CA_BUNDLE):
        try:
            return ssl.create_default_context(cafile=CA_BUNDLE)
        except Exception:
            pass
    return ssl.create_default_context()


def _get(path: str, **params):
    """GET JSON + en-têtes de quota. La clé est ajoutée en paramètre `apiKey`."""
    params = {k: v for k, v in params.items() if v is not None}
    params["apiKey"] = _api_key()
    url = f"{BASE}/{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "apex-tss"})
    try:
        with urllib.request.urlopen(req, timeout=40, context=_ssl_context()) as r:
            body = json.loads(r.read().decode())
            quota = {"remaining": r.headers.get("x-requests-remaining"),
                     "used": r.headers.get("x-requests-used")}
        return body, quota
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode()[:200]
        except Exception:
            pass
        if e.code in (401, 403):
            raise OddsApiError(f"HTTP {e.code} : clé refusée ou plan insuffisant. {detail}") from e
        if e.code == 429:
            raise OddsApiError(f"HTTP 429 : quota mensuel épuisé. {detail}") from e
        raise OddsApiError(f"HTTP {e.code} sur {path}. {detail}") from e
    except urllib.error.URLError as e:
        raise OddsApiError(f"réseau indisponible pour {path} : {e.reason}") from e


# ───────────────────────────── Utilitaires ─────────────────────────────

def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_sport(sport: str | None, league: str | None) -> str:
    if sport:
        return sport
    if league:
        key = league.strip().lower()
        if key in LEAGUE_ALIASES:
            return LEAGUE_ALIASES[key]
        raise OddsApiError(f"alias de ligue inconnu : « {league} ». Utiliser --sport <sport_key> "
                           f"(voir `sports`), ou l'un de : {', '.join(sorted(set(LEAGUE_ALIASES)))}.")
    raise OddsApiError("préciser --sport <sport_key> ou --league <alias>.")


def _norm(name: str) -> str:
    clean = (name or "").lower()
    for tok in ("fc", "sc", "ac", "afc", "cf", "fk", "sk", "if", "il", "vfl", "vfb", "1.", "1fc"):
        clean = clean.replace(tok, " ")
    return "".join(c for c in clean if c.isalnum() or c == " ").strip()


def _sim(a: str, b: str) -> float:
    na, nb = _norm(a).replace(" ", ""), _norm(b).replace(" ", "")
    return SequenceMatcher(None, na, nb).ratio()


def _event_to_odds(event: dict) -> dict:
    """Convertit un event The Odds API en {book_title: {"1X2":[h,d,a], "OU2.5":[o,u]}}."""
    home = event.get("home_team", "")
    away = event.get("away_team", "")
    out = {}
    for bk in event.get("bookmakers", []):
        name = bk.get("title") or bk.get("key") or "unknown"
        entry = {}
        for mkt in bk.get("markets", []):
            mkey = mkt.get("key")
            outs = mkt.get("outcomes", [])
            if mkey == "h2h":
                omap = {o.get("name"): o.get("price") for o in outs}
                draw = next((k for k in omap if k not in (home, away)), None)
                if home in omap and away in omap:
                    entry["1X2"] = [round(omap[home], 3),
                                    round(omap[draw], 3) if draw else None,
                                    round(omap[away], 3)]
            elif mkey == "totals":
                over = next((o for o in outs if o.get("name") == "Over" and o.get("point") == 2.5), None)
                under = next((o for o in outs if o.get("name") == "Under" and o.get("point") == 2.5), None)
                if over and under:
                    entry["OU2.5"] = [round(over["price"], 3), round(under["price"], 3)]
        if entry:
            out[name] = entry
    return out


def _fetch_events(sport_key: str, regions: str, markets: str) -> list[dict]:
    body, quota = _get(f"sports/{sport_key}/odds", regions=regions, markets=markets,
                       oddsFormat="decimal", dateFormat="iso")
    if not isinstance(body, list):
        raise OddsApiError(f"réponse inattendue pour {sport_key} : {str(body)[:160]}")
    return body


# ───────────────────────────── API importable (pour apex_mi / apex_worm) ─────────────────────────────

def _iter_store(date: str | None = None):
    if not STORE.is_dir():
        return
    files = sorted(STORE.glob("*.jsonl"))
    if date:
        files = [f for f in files if f.stem == date] or files
    for f in files:
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def books_for_fixture(home: str, away: str, date: str | None = None, min_sim: float = 0.58) -> dict:
    """Books The Odds API pour un match, apparié par noms d'équipes (dernier relevé). {} si rien.

    Additif et non destructif : destiné à ENRICHIR la dispersion / le Pinnacle-vs-médiane APEX-MI.
    Ne renvoie jamais de donnée inventée ; absence de store ou d'appariement ⇒ {}.
    """
    best, best_score = None, 0.0
    for rec in _iter_store(date):
        score = (_sim(rec.get("home_team", ""), home) + _sim(rec.get("away_team", ""), away)) / 2
        if score >= min_sim and score >= best_score:
            # garde le relevé le plus récent à score égal ou supérieur
            if best is None or rec.get("retrieved_at_utc", "") >= best.get("retrieved_at_utc", ""):
                best, best_score = rec, score
    return (best or {}).get("odds", {}) if best else {}


# ───────────────────────────── Commandes ─────────────────────────────

def cmd_status(a):
    try:
        body, quota = _get("sports")
    except OddsApiError as e:
        print(f"STATUS : ÉCHEC — {e}")
        return 1
    n = len(body) if isinstance(body, list) else 0
    print(f"STATUS : OK — clé valide. {n} sports exposés.")
    print(f"Quota : {quota.get('remaining')} requêtes restantes · {quota.get('used')} utilisées (mois).")
    print("Note : The Odds API ne fournit AUCUN % de parieurs publics → RLM non calculable.")
    return 0


def cmd_sports(a):
    body, quota = _get("sports", all=("true" if a.all else None))
    soccer = [s for s in body if s.get("group") == "Soccer"]
    rows = soccer if a.all else [s for s in soccer if s.get("active")]
    print(f"{len(rows)} compétitions foot {'(toutes)' if a.all else 'actives'} · quota {quota.get('remaining')} restant")
    for s in sorted(rows, key=lambda x: x.get("key", "")):
        flag = "" if s.get("active") else " [inactive]"
        print(f"  {s.get('key'):40s} {s.get('title','')}{flag}")
    return 0


def cmd_snapshot(a):
    sport_key = _resolve_sport(a.sport, a.league)
    events = _fetch_events(sport_key, a.regions, a.markets)
    date = a.date or now_utc()[:10]
    STORE.mkdir(parents=True, exist_ok=True)
    path = STORE / f"{date}.jsonl"
    ts = now_utc()
    n_written = 0
    with path.open("a", encoding="utf-8") as fh:
        for ev in events:
            # Filtre date : ne garder que les events du jour visé (si --date fourni)
            if a.date and (ev.get("commence_time", "")[:10] != date):
                continue
            odds = _event_to_odds(ev)
            if not odds:
                continue
            rec = {
                "source": "the-odds-api",
                "sport_key": sport_key,
                "event_id": ev.get("id"),
                "commence_time": ev.get("commence_time"),
                "home_team": ev.get("home_team"),
                "away_team": ev.get("away_team"),
                "retrieved_at_utc": ts,
                "regions": a.regions,
                "n_books": len(odds),
                "odds": odds,
                "public_pct": None,  # jamais fourni par The Odds API — RLM non calculable
            }
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n_written += 1
    print(f"snapshot {sport_key} : {n_written} events relevés → {path.relative_to(ROOT)} (horodaté {ts})")
    if n_written == 0:
        print("  (aucun event avec cotes — compétition hors saison, trop lointaine, ou --date sans match)")
    else:
        sample = next((r for r in _iter_store(date)), None)
        if sample:
            print(f"  ex. {sample['home_team']} – {sample['away_team']} : {sample['n_books']} books")
    print("Rappel : % public NON fourni → RLM reste UNAVAILABLE. Cette source enrichit books/dispersion/CLV.")
    return 0


def _demargin(odds_list):
    inv = [1.0 / o for o in odds_list if o and o > 1.0]
    if len(inv) < 2:
        return None
    s = sum(1.0 / o if (o and o > 1.0) else 0.0 for o in odds_list)
    return [round((1.0 / o) / s, 4) if (o and o > 1.0) else None for o in odds_list]


def cmd_closing(a):
    date = a.date or now_utc()[:10]
    by_event: dict = {}
    for rec in _iter_store(date):
        if a.sport and rec.get("sport_key") != a.sport:
            continue
        by_event.setdefault(rec.get("event_id"), []).append(rec)
    if not by_event:
        print(f"closing : aucun relevé dans le store pour {date}"
              + (f" / {a.sport}" if a.sport else "") + ". Lancer `snapshot` d'abord.")
        return 0
    print(f"Trajectoire ouverture→clôture (store local, {date}) — fair 1X2 Pinnacle si dispo, sinon médiane :")
    for eid, recs in by_event.items():
        recs.sort(key=lambda r: r.get("retrieved_at_utc", ""))
        first, last = recs[0], recs[-1]

        def fair(rec):
            books = {k: v for k, v in rec.get("odds", {}).items() if "1X2" in v}
            if not books:
                return None
            src = books.get("Pinnacle", {}).get("1X2")
            if src:
                return _demargin(src)
            cols = [b["1X2"] for b in books.values()]
            med = [sorted(c[i] for c in cols if c[i])[len(cols) // 2] for i in range(3)]
            return _demargin(med)

        f0, f1 = fair(first), fair(last)
        label = f"{last.get('home_team')} – {last.get('away_team')}"
        if f0 and f1:
            delta = [round((f1[i] or 0) - (f0[i] or 0), 4) for i in range(3)]
            print(f"  {label:42s} open {f0} → close {f1}  Δ {delta}  ({len(recs)} relevés)")
        else:
            print(f"  {label:42s} 1X2 non démarginable ({len(recs)} relevés)")
    print("CLV honnête : « clôture » = dernier relevé pré-coup d'envoi capté localement, pas une cote officielle.")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="Connecteur The Odds API pour APEX (clé via ODDS_API_KEY).")
    sp = p.add_subparsers(dest="cmd", required=True)

    sp.add_parser("status", help="Vérifie la clé et le quota")

    ps = sp.add_parser("sports", help="Liste les compétitions foot")
    ps.add_argument("--all", action="store_true", help="inclure les compétitions inactives")

    pn = sp.add_parser("snapshot", help="Relève les cotes multi-books d'une compétition")
    pn.add_argument("--sport", help="sport_key The Odds API (ex. soccer_uefa_nations_league)")
    pn.add_argument("--league", help="alias courant (ex. « Ligue des Nations »)")
    pn.add_argument("--regions", default="eu,uk", help="régions de books (défaut eu,uk)")
    pn.add_argument("--markets", default="h2h,totals", help="marchés (défaut h2h,totals)")
    pn.add_argument("--date", help="ne garder que les events de cette date (YYYY-MM-DD)")

    pc = sp.add_parser("closing", help="Trajectoire ouverture→clôture depuis le store local")
    pc.add_argument("--sport", help="filtrer sur un sport_key")
    pc.add_argument("--date", help="date du store (YYYY-MM-DD, défaut = aujourd'hui)")

    a = p.parse_args(argv)
    fns = {"status": cmd_status, "sports": cmd_sports, "snapshot": cmd_snapshot, "closing": cmd_closing}
    try:
        return fns[a.cmd](a)
    except OddsApiError as e:
        print(f"ERREUR : {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
