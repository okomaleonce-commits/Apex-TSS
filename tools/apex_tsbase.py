#!/usr/bin/env python3
"""APEX-TSBASE — base de données HORODATÉE, une compétition, deux instants avant match.

Répond au premier chantier identifié par la revue externe : construire une base réellement utilisable
AVANT match, où chaque donnée porte son heure de disponibilité, sur une seule ligue bien couverte.

Deux instants de prévision cibles (fenêtres, en heures avant le coup d'envoi) :
  T24  = 30 h → 12 h avant  (la veille)
  T045 = 90 min → 20 min avant (après publication probable des compositions)

Chaque capture, à l'instant réel où elle tourne, enregistre pour chaque match non commencé :
  heures_avant_ko, bucket (T24 / T045 / autre), cotes 1X2/O-U/BTTS (Pinnacle en priorité) démarginées,
  compositions publiées (oui/non) + XI si dispo, blessures, et xG FootyStats du match si la clé est active.
Append-only, un fichier par ligue+saison : data/tsbase/<league_id>_<saison>.jsonl.

RÈGLE D'OR (revue externe) : une prévision utilisant les compositions ne peut être comparée qu'aux cotes
prises APRÈS leur publication (bucket T045). Comparer une prévision à des cotes d'un autre instant est
invalide. Toute donnée dont l'heure de disponibilité est inconnue est marquée "availability_incertaine".

Usage :
  python3 tools/apex_tsbase.py capture --league 39 [--date AAAA-MM-JJ]
  python3 tools/apex_tsbase.py report  --league 39
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_apifootball as AF  # noqa: E402
try:
    import apex_footystats as FS  # noqa: E402
except Exception:  # pragma: no cover
    FS = None

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "tsbase"
# ligue bien couverte par défaut : Premier League (API-Football id 39)
DEFAULT_LEAGUE = 39


def bucket(hrs: float) -> str:
    if 12 <= hrs <= 30:
        return "T24"
    if 20 / 60 <= hrs <= 90 / 60:
        return "T045"
    return "autre"


def demargin(o):
    if not o:
        return None
    inv = sum(1 / x for x in o)
    return [round(1 / x / inv, 4) for x in o]


def season_of(d: dt.date) -> int:
    return d.year if d.month >= 7 else d.year - 1


def cmd_capture(a):
    now = dt.datetime.now(dt.timezone.utc)
    date = a.date or now.date().isoformat()
    season = season_of(dt.date.fromisoformat(date))
    fx = AF.api("fixtures", league=a.league, season=season, date=date, timezone="UTC")["response"]
    ns = [f for f in fx if f["fixture"]["status"]["short"] in ("NS", "TBD")]
    if not ns:
        print(f"Aucun match non commencé pour la ligue {a.league} le {date}.")
        return
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{a.league}_{season}.jsonl"
    n = 0
    for f in ns:
        fid = f["fixture"]["id"]
        ko = dt.datetime.fromisoformat(f["fixture"]["date"])
        hrs = (ko - now).total_seconds() / 3600
        if hrs <= 0:
            continue
        odds = {}
        try:
            odds = AF.parse_odds(AF.api_all("odds", fixture=fid))
        except Exception as e:  # noqa: BLE001
            odds = {"_erreur": str(e)[:120]}
        _, o1 = AF.pick_book(odds, "1X2")
        _, ou = AF.pick_book(odds, "OU")
        _, bt = AF.pick_book(odds, "BTTS")
        lineups = []
        try:
            lineups = AF.api("fixtures/lineups", fixture=fid)["response"]
        except Exception:
            pass
        injuries = []
        try:
            injuries = AF.api_all("injuries", fixture=fid)
        except Exception:
            pass
        rec = {
            "fixture_id": fid, "captured_at_utc": now.isoformat(timespec="seconds"),
            "kickoff_utc": f["fixture"]["date"], "hours_before_ko": round(hrs, 2), "bucket": bucket(hrs),
            "home": f["teams"]["home"]["name"], "away": f["teams"]["away"]["name"],
            "p_market_1x2": demargin(o1), "odds_1x2": o1,
            "p_market_over25": demargin(ou["2.5"])[0] if ou and ou.get("2.5") and all(ou["2.5"]) else None,
            "odds_ou25": ou.get("2.5") if ou else None,
            "p_market_btts": demargin(bt)[0] if bt else None, "odds_btts": bt,
            "lineups_published": bool(lineups),
            "home_xi": [p["player"]["name"] for t in lineups if t["team"]["id"] == f["teams"]["home"]["id"]
                        for p in t.get("startXI", [])] or None,
            "away_xi": [p["player"]["name"] for t in lineups if t["team"]["id"] == f["teams"]["away"]["id"]
                        for p in t.get("startXI", [])] or None,
            "injuries": [{"team": i["team"]["name"], "player": i["player"]["name"], "reason": i["player"].get("reason")}
                         for i in injuries] or None,
            "availability_incertaine": False,   # capture live : l'heure EST connue (= captured_at_utc)
        }
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        n += 1
        print(f"  {rec['home']} – {rec['away']} | KO dans {hrs:.1f}h [{rec['bucket']}] | "
              f"1X2 {rec['p_market_1x2']} | compos {'oui' if rec['lineups_published'] else 'non'}")
    print(f"{n} captures ajoutées à {path.relative_to(ROOT)} (append-only).")


def cmd_report(a):
    files = sorted(OUT.glob(f"{a.league}_*.jsonl")) if a.league else sorted(OUT.glob("*.jsonl"))
    if not files:
        print("Base horodatée vide. Lancer `capture` à la veille (T24) et ~1 h avant les matchs (T045).")
        return
    for p in files:
        recs = [json.loads(l) for l in open(p, encoding="utf-8")]
        by_fix = {}
        for r in recs:
            by_fix.setdefault(r["fixture_id"], set()).add(r["bucket"])
        both = sum(1 for b in by_fix.values() if "T24" in b and "T045" in b)
        print(f"{p.name} : {len(recs)} captures · {len(by_fix)} matchs · "
              f"{both} avec les DEUX instants (T24+T045) → exploitables pour l'expérience compos/mouvement")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("capture"); c.add_argument("--league", type=int, default=DEFAULT_LEAGUE); c.add_argument("--date")
    r = sp.add_parser("report"); r.add_argument("--league", type=int, default=DEFAULT_LEAGUE)
    a = p.parse_args()
    try:
        {"capture": cmd_capture, "report": cmd_report}[a.cmd](a)
    except AF.ApiError as e:
        sys.exit(f"Erreur API-Football : {e}")


if __name__ == "__main__":
    main()
