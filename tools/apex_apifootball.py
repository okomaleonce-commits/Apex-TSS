#!/usr/bin/env python3
"""Connecteur API-Football (v3.football.api-sports.io) pour le module APEX-BSM.

La clé est lue dans la variable d'environnement API_FOOTBALL_KEY (jamais dans le code).

Commandes
---------
  status                     Vérifie la clé, le plan et le quota restant.
  snapshot --date D          Relève, pour chaque match non commencé du jour : cotes (1X2, O/U, BTTS, AH,
                             double chance), blessures, compositions si publiées. Ajoute chaque relevé,
                             horodaté, à data/apifootball/snapshots/<date>.jsonl (append-only).
  history --league L --season S [--stats]
                             Matchs terminés d'une saison (+ tirs, tirs cadrés, xG si --stats : 1 appel/match)
                             → data/apifootball/history/<L>_<S>.csv, avec l'heure de relevé.
  bsm-args --fixture ID      Construit la commande `apex_bsm.py simulate` à partir du dernier relevé
                             (cotes vérifiées + source + heure), avec correspondance des noms d'équipes.

Discipline de données (skill apex-backtest-simulation) : chaque valeur garde l'heure à laquelle elle a
été relevée ; une cote relevée après l'heure de prévision ne doit jamais servir à évaluer cette prévision.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import statistics
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://v3.football.api-sports.io"
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "apifootball"
HIST_FD = ROOT / "data" / "history"

# ligue API-Football → code football-data.co.uk (périmètre du backtest BSM)
LEAGUE_TO_DIV = {39: "E0", 40: "E1", 41: "E2", 42: "E3", 140: "SP1", 141: "SP2", 135: "I1", 136: "I2",
                 78: "D1", 79: "D2", 61: "F1", 62: "F2", 88: "N1", 144: "B1", 94: "P1", 203: "T1",
                 197: "G1", 179: "SC0", 180: "SC1"}
BET_IDS = {1: "1X2", 5: "OU", 8: "BTTS", 4: "AH", 12: "DC"}
BOOK_PRIORITY = ["Pinnacle", "Bet365", "Marathonbet", "1xBet", "Unibet", "Betfair"]


# ───────────────────────────── HTTP ─────────────────────────────

class ApiError(RuntimeError):
    pass


def api(path: str, **params) -> dict:
    # Deux modes : variable API_FOOTBALL_KEY, ou « Identifiants API » de l'environnement cloud
    # (le proxy ajoute l'en-tête x-apisports-key lui-même : la session ne voit jamais la clé).
    key = os.environ.get("API_FOOTBALL_KEY")
    url = f"{BASE}/{path}?{urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})}"
    headers = {"User-Agent": "apex-tss"}
    if key:
        headers["x-apisports-key"] = key
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                body = json.loads(r.read().decode())
                remaining = r.headers.get("x-ratelimit-requests-remaining")
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(6 * (attempt + 1))
                continue
            if e.code in (401, 403) and b"token" in e.read():
                raise ApiError("clé absente ou refusée : configure l'identifiant API (hôte v3.football.api-sports.io, "
                               "en-tête x-apisports-key) dans l'environnement, puis ouvre une nouvelle session.") from e
            raise ApiError(f"HTTP {e.code} sur {path}") from e
    errs = body.get("errors")
    if errs:
        if isinstance(errs, dict) and "token" in errs:
            raise ApiError("clé absente ou refusée : configure l'identifiant API (hôte v3.football.api-sports.io, "
                           "en-tête x-apisports-key) dans l'environnement, puis ouvre une nouvelle session.")
        raise ApiError(f"{path} : {errs}")
    body["_quota_restant"] = remaining
    body["_retrieved_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    return body


def api_all(path: str, **params) -> list:
    """Parcourt la pagination (odds, injuries…)."""
    out, page = [], 1
    while True:
        b = api(path, page=page, **params) if page > 1 else api(path, **params)
        out += b.get("response", [])
        tot = (b.get("paging") or {}).get("total", 1)
        if page >= tot:
            return out
        page += 1


# ───────────────────────────── parsing ─────────────────────────────

def _f(x):
    try:
        v = float(x)
        return v if v > 1.0 else None
    except (TypeError, ValueError):
        return None


def parse_odds(resp: list) -> dict:
    """→ {bookmaker: {"1X2": [h,d,a], "OU": {line: [over, under]}, "BTTS": [oui, non],
                      "AH": {"dom" / "ext": {line: cote}}, "DC": {...}}, "_update": iso}"""
    out = {}
    for item in resp:
        out["_update"] = item.get("update")
        for bk in item.get("bookmakers", []):
            b = out.setdefault(bk["name"], {})
            for bet in bk.get("bets", []):
                kind = BET_IDS.get(bet.get("id"))
                vals = {str(v["value"]): _f(v["odd"]) for v in bet.get("values", [])}
                if kind == "1X2" and all(vals.get(k) for k in ("Home", "Draw", "Away")):
                    b["1X2"] = [vals["Home"], vals["Draw"], vals["Away"]]
                elif kind == "OU":
                    for lab, o in vals.items():
                        m = re.match(r"(Over|Under) ([\d.]+)$", lab)
                        if m and o:
                            b.setdefault("OU", {}).setdefault(m.group(2), [None, None])[0 if m.group(1) == "Over" else 1] = o
                elif kind == "BTTS" and vals.get("Yes") and vals.get("No"):
                    b["BTTS"] = [vals["Yes"], vals["No"]]
                elif kind == "AH":
                    for lab, o in vals.items():
                        m = re.match(r"(Home|Away) ([+-]?[\d.]+)$", lab)
                        if m and o:
                            b.setdefault("AH", {}).setdefault("dom" if m.group(1) == "Home" else "ext", {})[m.group(2)] = o
                elif kind == "DC":
                    b["DC"] = vals
    return out


def pick_book(odds: dict, market: str):
    books = [k for k in odds if not k.startswith("_") and market in odds[k]]
    for name in BOOK_PRIORITY:
        if name in books:
            return name, odds[name][market]
    if market == "1X2" and books:   # sinon médiane des bookmakers disponibles
        return f"médiane de {len(books)} bookmakers", [statistics.median(odds[b]["1X2"][i] for b in books) for i in range(3)]
    return (books[0], odds[books[0]][market]) if books else (None, None)


def parse_stats(resp: list) -> dict:
    keymap = {"Total Shots": "tirs", "Shots on Goal": "tirs_cadres", "expected_goals": "xg"}
    out = {}
    for t in resp:
        d = {}
        for s in t.get("statistics", []):
            k = keymap.get(s.get("type"))
            if k and s.get("value") is not None:
                try:
                    d[k] = float(str(s["value"]).rstrip("%"))
                except ValueError:
                    pass
        out[t["team"]["id"]] = d
    return out


# ───────────────────────────── commandes ─────────────────────────────

def cmd_status(a):
    b = api("status")
    r = b.get("response") or {}
    sub = r.get("subscription", {}); req = r.get("requests", {})
    print(f"Clé valide · plan : {sub.get('plan')} · actif : {sub.get('active')} · fin : {sub.get('end')}")
    print(f"Requêtes aujourd'hui : {req.get('current')} / {req.get('limit_day')}")


def cmd_snapshot(a):
    date = a.date or dt.date.today().isoformat()
    leagues = {int(x) for x in a.leagues.split(",")} if a.leagues else None
    fx = api("fixtures", date=date, timezone="UTC")
    calls = 1
    rows = [f for f in fx["response"] if (not leagues or f["league"]["id"] in leagues)
            and f["fixture"]["status"]["short"] in ("NS", "TBD")]
    print(f"{len(rows)} matchs non commencés le {date}" + (f" (ligues {sorted(leagues)})" if leagues else ""))
    OUT.joinpath("snapshots").mkdir(parents=True, exist_ok=True)
    path = OUT / "snapshots" / f"{date}.jsonl"
    n_ok = 0
    for f in rows:
        if calls + 3 > a.max_calls:
            print(f"Arrêt : plafond de {a.max_calls} appels atteint (quota).")
            break
        fid = f["fixture"]["id"]
        rec = {"fixture_id": fid, "retrieved_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
               "kickoff_utc": f["fixture"]["date"], "league_id": f["league"]["id"], "league": f["league"]["name"],
               "season": f["league"]["season"], "home": f["teams"]["home"]["name"], "away": f["teams"]["away"]["name"],
               "home_id": f["teams"]["home"]["id"], "away_id": f["teams"]["away"]["id"]}
        try:
            rec["odds"] = parse_odds(api_all("odds", fixture=fid)); calls += 1
        except ApiError as e:
            rec["odds_erreur"] = str(e)
        if not a.odds_only:
            try:
                rec["blessures"] = [{"equipe": i["team"]["name"], "joueur": i["player"]["name"],
                                     "type": i["player"].get("type"), "raison": i["player"].get("reason")}
                                    for i in api_all("injuries", fixture=fid)]; calls += 1
            except ApiError as e:
                rec["blessures_erreur"] = str(e)
            try:
                lu = api("fixtures/lineups", fixture=fid)["response"]; calls += 1
                rec["compositions"] = [{"equipe": t["team"]["name"], "formation": t.get("formation"),
                                        "titulaires": [p["player"]["name"] for p in t.get("startXI", [])]} for t in lu] or None
            except ApiError as e:
                rec["compositions_erreur"] = str(e)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        n_ok += 1
        bk, o = pick_book(rec.get("odds", {}), "1X2")
        print(f"  {rec['kickoff_utc'][11:16]} {rec['league'][:18]:18s} {rec['home']} – {rec['away']}  1X2 {o} ({bk})"
              + (f"  · {len(rec.get('blessures') or [])} absents" if not a.odds_only else ""))
    print(f"{n_ok} relevés ajoutés à {path.relative_to(ROOT)} · ~{calls} appels")


def cmd_history(a):
    b = api("fixtures", league=a.league, season=a.season, status="FT-AET-PEN", timezone="UTC")
    fx = sorted(b["response"], key=lambda f: f["fixture"]["date"])
    OUT.joinpath("history").mkdir(parents=True, exist_ok=True)
    path = OUT / "history" / f"{a.league}_{a.season}.csv"
    done = set()
    if path.exists():
        done = {r["fixture_id"] for r in csv.DictReader(open(path, encoding="utf-8"))}
    cols = ["fixture_id", "date_utc", "home", "away", "hg", "ag", "ht_hg", "ht_ag",
            "tirs_dom", "tirs_ext", "tirs_cadres_dom", "tirs_cadres_ext", "xg_dom", "xg_ext", "retrieved_at_utc"]
    new = not path.exists()
    calls = 1; n = 0
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        if new:
            w.writeheader()
        for f in fx:
            fid = str(f["fixture"]["id"])
            if fid in done:
                continue
            row = {"fixture_id": fid, "date_utc": f["fixture"]["date"], "home": f["teams"]["home"]["name"],
                   "away": f["teams"]["away"]["name"], "hg": f["score"]["fulltime"]["home"], "ag": f["score"]["fulltime"]["away"],
                   "ht_hg": f["score"]["halftime"]["home"], "ht_ag": f["score"]["halftime"]["away"],
                   "retrieved_at_utc": b["_retrieved_at_utc"]}
            if a.stats:
                if calls >= a.max_calls:
                    print(f"Arrêt : plafond de {a.max_calls} appels ; relancer la commande pour reprendre.")
                    break
                st = parse_stats(api("fixtures/statistics", fixture=fid)["response"]); calls += 1
                hs, as_ = st.get(f["teams"]["home"]["id"], {}), st.get(f["teams"]["away"]["id"], {})
                row.update(tirs_dom=hs.get("tirs"), tirs_ext=as_.get("tirs"), tirs_cadres_dom=hs.get("tirs_cadres"),
                           tirs_cadres_ext=as_.get("tirs_cadres"), xg_dom=hs.get("xg"), xg_ext=as_.get("xg"))
            w.writerow(row); n += 1
    have_xg = sum(1 for r in csv.DictReader(open(path, encoding="utf-8")) if r.get("xg_dom"))
    print(f"{n} matchs ajoutés à {path.relative_to(ROOT)} · ~{calls} appels · matchs avec xG : {have_xg}")


def _norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = s.replace("manchester", "man").replace("united", "utd")
    return re.sub(r"[^a-z]", "", re.sub(r"\b(fc|afc|cf|sc|ac|as|ss|us|club|de|the)\b", "", s))


def _sim(a, b):
    A = {a[i:i + 3] for i in range(max(1, len(a) - 2))}; B = {b[i:i + 3] for i in range(max(1, len(b) - 2))}
    return len(A & B) / max(1, len(A | B))


# noms API-Football → noms football-data.co.uk quand la similarité textuelle ne suffit pas
ALIASES = {
    "Wolverhampton Wanderers": "Wolves", "Nottingham Forest": "Nott'm Forest", "Brighton & Hove Albion": "Brighton",
    "Brighton": "Brighton", "Sheffield Wednesday": "Sheffield Weds", "Queens Park Rangers": "QPR",
    "West Bromwich Albion": "West Brom", "Paris Saint Germain": "Paris SG", "Athletic Club": "Ath Bilbao",
    "Atletico Madrid": "Ath Madrid", "Real Sociedad": "Sociedad", "Rayo Vallecano": "Vallecano",
    "Celta Vigo": "Celta", "Espanyol": "Espanol", "Borussia Monchengladbach": "M'gladbach",
    "Bayer Leverkusen": "Leverkusen", "Eintracht Frankfurt": "Ein Frankfurt", "FC St. Pauli": "St Pauli",
    "1899 Hoffenheim": "Hoffenheim", "AC Milan": "Milan", "Inter": "Inter", "AS Roma": "Roma",
    "Saint Etienne": "St Etienne", "Stade Brestois 29": "Brest", "Milton Keynes Dons": "Milton Keynes",
}


def fd_name(div: str, name: str):
    """Correspondance nom API-Football → nom football-data ; score < 0,6 = à vérifier à la main."""
    teams = set()
    for p in HIST_FD.glob(f"{div}_*.csv"):
        for r in csv.DictReader(open(p, encoding="utf-8-sig", errors="replace")):
            if r.get("HomeTeam"):
                teams.add(r["HomeTeam"].strip())
    if not teams:
        return None, 0.0
    if ALIASES.get(name) in teams:
        return ALIASES[name], 1.0
    n = _norm(name)

    def score(t):
        m = _norm(t)
        return 0.9 if m and (m in n or n in m) else _sim(m, n)
    best = max(teams, key=score)
    return best, score(best)


def cmd_bsm_args(a):
    recs = []
    for p in sorted((OUT / "snapshots").glob("*.jsonl")):
        recs += [json.loads(l) for l in open(p, encoding="utf-8") if f'"fixture_id": {a.fixture},' in l]
    if not recs:
        sys.exit("Aucun relevé pour ce match : lancer d'abord `snapshot`.")
    r = recs[-1]
    div = LEAGUE_TO_DIV.get(r["league_id"])
    args = ["python3 tools/apex_bsm.py simulate"]
    if div:
        hn, hs = fd_name(div, r["home"]); an, as_ = fd_name(div, r["away"])
        args += [f"--div {div}", f'--home "{hn}"', f'--away "{an}"']
        warn = [f"{x} → {y} ({s:.2f})" for x, y, s in ((r["home"], hn, hs), (r["away"], an, as_)) if s < 0.6]
    else:
        args += [f'--home "{r["home"]}"', f'--away "{r["away"]}"', "--lh <λdom> --la <λext>"]
        warn = [f"ligue {r['league']} hors périmètre du backtest : λ externes, statut NON VALIDÉ"]
    args.append(f"--asof {r['kickoff_utc'][:10]} --kickoff {r['kickoff_utc']}")
    odds = r.get("odds", {}); srcs = set()
    bk, o = pick_book(odds, "1X2")
    if o:
        args.append(f"--odds-1x2 {o[0]},{o[1]},{o[2]}"); srcs.add(bk)
    bk2, ou = pick_book(odds, "OU")
    if ou and ou.get("2.5") and all(ou["2.5"]):
        args.append(f"--odds-ou25 {ou['2.5'][0]},{ou['2.5'][1]}"); srcs.add(bk2)
    bk3, bt = pick_book(odds, "BTTS")
    if bt:
        args.append(f"--odds-btts {bt[0]},{bt[1]}"); srcs.add(bk3)
    bk4, ah = pick_book(odds, "AH")
    if ah:
        for side in ("dom", "ext"):
            for line, c in list((ah.get(side) or {}).items())[:3]:
                args.append(f"--odds-ah {side}:{float(line):+.2f}:{c}")
        srcs.add(bk4)
    args.append(f'--odds-source "API-Football/{"+".join(sorted(s for s in srcs if s))}" --odds-time {r["retrieved_at_utc"]}')
    if r.get("compositions") is None and not r.get("compositions_erreur"):
        args.append("--missing-lineup")
        warn.append("compositions non publiées au moment du relevé → --missing-lineup (surveillance)")
    args.append("--record")
    print(" \\\n  ".join(args))
    for w_ in warn:
        print("⚠", w_)
    if r.get("blessures"):
        print("Absents relevés :", "; ".join(f"{b['equipe']}: {b['joueur']} ({b['raison']})" for b in r["blessures"][:12]))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("status")
    s = sp.add_parser("snapshot"); s.add_argument("--date"); s.add_argument("--leagues", help="ids séparés par des virgules")
    s.add_argument("--odds-only", action="store_true"); s.add_argument("--max-calls", type=int, default=90)
    h = sp.add_parser("history"); h.add_argument("--league", type=int, required=True); h.add_argument("--season", type=int, required=True)
    h.add_argument("--stats", action="store_true"); h.add_argument("--max-calls", type=int, default=90)
    b = sp.add_parser("bsm-args"); b.add_argument("--fixture", type=int, required=True)
    a = p.parse_args()
    try:
        {"status": cmd_status, "snapshot": cmd_snapshot, "history": cmd_history, "bsm-args": cmd_bsm_args}[a.cmd](a)
    except ApiError as e:
        sys.exit(f"Erreur API-Football : {e}")


if __name__ == "__main__":
    main()
