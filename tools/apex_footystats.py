#!/usr/bin/env python3
"""Connecteur FootyStats (api.football-data-api.com) — xG historiques par match pour APEX.

La clé se lit dans la variable d'environnement FOOTYSTATS_KEY. FootyStats s'authentifie par un
paramètre d'URL `?key=`, PAS par un en-tête : la clé doit donc être une variable d'environnement,
jamais un « identifiant API » injecté en en-tête, et jamais écrite dans le code.

Commandes :
  status               Vérifie la clé et le quota restant.
  leagues              Résout les season_id FootyStats pour les divisions/saisons du backtest APEX.
  history --divs --seasons
                       Écrit data/footystats/<div>_<season>.csv : date, équipes, buts, xG (réalisé et
                       avant-match), cotes 1X2. Sert au backtest xG (tools/apex_bsm à venir en variante).

Discipline : le xG « avant-match » (team_x_xg_prematch) est une estimation disponible avant le coup
d'envoi ; le xG réalisé (team_x_xg) ne l'est qu'après et ne sert JAMAIS à une prévision d'avant-match.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://api.football-data-api.com"
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "footystats"

# division football-data.co.uk → nom de ligue FootyStats
DIV_TO_NAME = {
    "E0": "England Premier League", "E1": "England Championship", "E2": "England League One",
    "E3": "England League Two", "EC": "England National League", "SP1": "Spain La Liga",
    "SP2": "Spain Segunda División", "I1": "Italy Serie A", "I2": "Italy Serie B",
    "D1": "Germany Bundesliga", "D2": "Germany 2. Bundesliga", "F1": "France Ligue 1", "F2": "France Ligue 2",
    "N1": "Netherlands Eredivisie", "B1": "Belgium Pro League", "P1": "Portugal Liga NOS", "T1": "Turkey Super Lig",
}


def season_year(s: str) -> int:
    """'2122' → 20212022."""
    y = int(s)
    return (2000 + y // 100) * 10000 + (2000 + y % 100)


class FSError(RuntimeError):
    pass


def key(demo=False):
    if demo:
        return "example"
    k = os.environ.get("FOOTYSTATS_KEY")
    if not k:
        raise FSError("FOOTYSTATS_KEY absente. Ajoute-la comme VARIABLE D'ENVIRONNEMENT (pas un identifiant API "
                      "en en-tête : FootyStats utilise ?key=), puis ouvre une nouvelle session.")
    return k


def api(path, demo=False, **params):
    params["key"] = key(demo)
    url = f"{BASE}/{path}?{urllib.parse.urlencode(params)}"
    for att in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and att < 2:
                time.sleep(6 * (att + 1)); continue
            body = e.read()[:200].decode(errors="replace")
            if e.code in (401, 403):
                raise FSError(f"clé refusée (HTTP {e.code}). {body}") from e
            raise FSError(f"HTTP {e.code} sur {path}: {body}") from e
    raise FSError("échec après retries")


def league_index(demo=False):
    """{nom_ligue: {year: season_id}} sur les ligues du compte."""
    d = api("league-list", demo=demo, chosen_leagues_only="true")
    idx = {}
    for x in d.get("data", []):
        idx[x["name"]] = {int(s.get("year", 0)): s["id"] for s in x.get("season", []) if s.get("year")}
    return idx


def cmd_status(a):
    d = api("league-list", demo=a.demo, chosen_leagues_only="true")
    md = d.get("metadata", {})
    print(f"Clé OK · requêtes restantes : {md.get('request_remaining')} / {md.get('request_limit')} · "
          f"ligues sélectionnées : {len(d.get('data', []))}")
    print("Reset :", md.get("request_reset_message", "—"))


def cmd_leagues(a):
    idx = league_index(a.demo)
    divs = a.divs.split(","); seasons = a.seasons.split(",")
    print(f"{len(idx)} ligues dans le compte. Résolution des season_id :")
    for d in divs:
        name = DIV_TO_NAME.get(d)
        row = idx.get(name, {})
        got = {s: row.get(season_year(s)) for s in seasons}
        miss = [s for s, v in got.items() if not v]
        flag = "" if name in idx else "  ⚠ ligue absente du compte (à cocher sur footystats.org)"
        print(f"  {d:4s} {name or '???':28s} {got}{flag}" + (f"  manquantes: {miss}" if miss and name in idx else ""))


def cmd_history(a):
    idx = league_index(a.demo)
    OUT.mkdir(parents=True, exist_ok=True)
    divs = a.divs.split(","); seasons = a.seasons.split(",")
    cols = ["date_utc", "home", "away", "hg", "ag", "xg_h", "xg_a", "xg_pre_h", "xg_pre_a",
            "odds_1", "odds_x", "odds_2", "retrieved_at_utc"]
    total = 0
    for d in divs:
        name = DIV_TO_NAME.get(d)
        if name not in idx:
            print(f"  {d}: ligue absente du compte, ignorée"); continue
        for s in seasons:
            sid = idx[name].get(season_year(s))
            if not sid:
                print(f"  {d} {s}: saison introuvable"); continue
            resp = api("league-matches", demo=a.demo, season_id=sid)
            now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            rows = []
            for m in resp.get("data", []):
                if m.get("status") != "complete":
                    continue
                du = m.get("date_unix")
                rows.append({
                    "date_utc": dt.datetime.fromtimestamp(du, dt.timezone.utc).date().isoformat() if du else "",
                    "home": m.get("home_name"), "away": m.get("away_name"),
                    "hg": m.get("homeGoalCount"), "ag": m.get("awayGoalCount"),
                    "xg_h": m.get("team_a_xg"), "xg_a": m.get("team_b_xg"),
                    "xg_pre_h": m.get("team_a_xg_prematch"), "xg_pre_a": m.get("team_b_xg_prematch"),
                    "odds_1": m.get("odds_ft_1"), "odds_x": m.get("odds_ft_x"), "odds_2": m.get("odds_ft_2"),
                    "retrieved_at_utc": now})
            p = OUT / f"{d}_{s}.csv"
            with open(p, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)
            nx = sum(1 for r in rows if r["xg_h"] not in (None, "", 0))
            total += len(rows)
            print(f"  {d} {s}: {len(rows)} matchs, {nx} avec xG → {p.relative_to(ROOT)}")
    print(f"Total : {total} matchs écrits.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    for nm in ("status", "leagues", "history"):
        q = sp.add_parser(nm); q.add_argument("--demo", action="store_true", help="clé publique de démonstration")
        if nm != "status":
            q.add_argument("--divs", default="E0,E1,E2,E3,SP1,I1,D1,F1")
            q.add_argument("--seasons", default="2122,2223,2324,2425,2526")
    a = p.parse_args()
    try:
        {"status": cmd_status, "leagues": cmd_leagues, "history": cmd_history}[a.cmd](a)
    except FSError as e:
        sys.exit(f"Erreur FootyStats : {e}")


if __name__ == "__main__":
    main()
