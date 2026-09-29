#!/usr/bin/env python3
"""APEX — pipeline quotidien automatique (module BSM).

Étapes, dans l'ordre :
  1. settle   : règle les prévisions passées du journal à partir des résultats officiels API-Football
                (score à 90 minutes + temps additionnel ; reports et annulations tracés) — append-only.
  2. elo      : complète l'historique international avec les derniers résultats de sélections et
                recalcule l'Elo (paramètres de conversion H1 inchangés, jamais re-réglés ici).
  3. snapshot : relève cotes / blessures / compositions de J pour les compétitions couvertes.
  4. simulate : simule chaque match non commencé avec le bon moteur, enregistre la prévision (--record).
                - clubs : ligues du backtest de référence (backtests/latest_params.json) ;
                - sélections A : conversion Elo→λ H1 (tools/apex_intl.py), statut INDICATIF ;
                - le reste est listé comme « hors périmètre », jamais simulé à l'aveugle.
  5. rapport  : analyses/<J>-bsm/README.md + audit du journal.

Usage : python3 tools/apex_daily.py [--date AAAA-MM-JJ] [--skip-settle] [--max-calls 1500]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_apifootball as AF  # noqa: E402
import apex_intl as I  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "ledger"
YOUTH = re.compile(r"\b(U1\d|U2\d|Women|Femenil|Feminin|Youth|Olympic|Reserve)\b", re.I)
INTL_TOURNAMENT = {5: "UEFA Nations League", 36: "African Cup of Nations qualification", 536: "CONCACAF Nations League",
                   10: "Friendly", 1: "FIFA World Cup", 4: "UEFA Euro", 6: "African Cup of Nations", 9: "Copa América",
                   22: "Gold Cup", 7: "AFC Asian Cup", 29: "FIFA World Cup qualification", 30: "FIFA World Cup qualification",
                   31: "FIFA World Cup qualification", 32: "FIFA World Cup qualification", 33: "FIFA World Cup qualification",
                   34: "FIFA World Cup qualification"}


def sh(cmd):
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


# ───────── 1. règlement automatique ─────────

def fixture_map():
    """forecast_id → fixture_id (champ du journal, sinon fichiers runs*.json des analyses)."""
    m = {}
    for p in ROOT.glob("analyses/*/runs*.json"):
        for r in json.loads(p.read_text()):
            if r.get("forecast_id") and r.get("fixture_id"):
                m[r["forecast_id"]] = r["fixture_id"]
    return m


def settle_auto(now):
    if not (LEDGER / "forecasts.jsonl").exists():
        return []
    F = [json.loads(l) for l in open(LEDGER / "forecasts.jsonl", encoding="utf-8")]
    done = set()
    if (LEDGER / "settlements.jsonl").exists():
        done = {json.loads(l)["forecast_id"] for l in open(LEDGER / "settlements.jsonl", encoding="utf-8")}
    fmap = fixture_map()
    out, cache = [], {}
    for f in F:
        fid = f["forecast_id"]
        fx = f.get("fixture_id") or fmap.get(fid)
        if fid in done or not fx or not f.get("coup_envoi"):
            continue
        if dt.datetime.fromisoformat(f["coup_envoi"].replace("Z", "+00:00")) > now - dt.timedelta(hours=3):
            continue
        if fx not in cache:
            resp = AF.api("fixtures", id=fx)["response"]
            cache[fx] = resp[0] if resp else None
        g = cache[fx]
        if not g:
            continue
        st = g["fixture"]["status"]["short"]
        if st in ("FT", "AET", "PEN"):
            ft = g["score"]["fulltime"]            # score à 90 min + temps additionnel (hors prolongation)
            args = ["--status", "JOUE", "--score", f"{ft['home']}-{ft['away']}",
                    "--note", f"réglé automatiquement (API-Football, statut {st})"]
        elif st in ("PST", "CANC", "ABD", "AWD", "WO"):
            args = ["--status", "REPORTE" if st == "PST" else "ANNULE", "--note", f"statut officiel {st}"]
        else:
            continue
        r = sh(["python3", "tools/apex_bsm.py", "settle", "--forecast-id", fid] + args)
        out.append((fid, f["home"], f["away"], args[1], args[3] if len(args) > 3 else "", r.returncode))
    return out


# ───────── 2. Elo international à jour ─────────

def refresh_elo(until: dt.date):
    rows = list(csv.DictReader(open(I.SUPP, encoding="utf-8"))) if I.SUPP.exists() else []
    last = max([dt.date.fromisoformat(r["date"]) for r in rows] + [dt.date(2026, 8, 26)])
    d, added = last + dt.timedelta(days=1), 0
    while d < until:
        for f in AF.api("fixtures", date=d.isoformat(), timezone="UTC")["response"]:
            lid = f["league"]["id"]
            h, a = f["teams"]["home"]["name"], f["teams"]["away"]["name"]
            if lid not in INTL_TOURNAMENT or YOUTH.search(h + " " + a + " " + f["league"]["name"]):
                continue
            if f["fixture"]["status"]["short"] not in ("FT", "AET", "PEN"):
                continue
            rows.append({"date": d.isoformat(), "home_team": I.canon(h), "away_team": I.canon(a),
                         "home_score": f["score"]["fulltime"]["home"], "away_score": f["score"]["fulltime"]["away"],
                         "tournament": INTL_TOURNAMENT[lid], "city": f["fixture"]["venue"]["city"] or "", "country": "",
                         "neutral": "FALSE"})
            added += 1
        d += dt.timedelta(days=1)
    if rows:
        w = csv.DictWriter(open(I.SUPP, "w", newline="", encoding="utf-8"), fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
    P = json.loads((I.BT / "intl_latest_params.json").read_text())
    allr = I.load()
    final = I.run_elo(allr)
    P["elo"] = {k: round(v, 1) for k, v in sorted(final.items())}
    P["elo_au"] = allr[-1]["date"].isoformat()
    (I.BT / "intl_latest_params.json").write_text(json.dumps(P, ensure_ascii=False, indent=0))
    return added, P["elo_au"]


# ───────── 3-4. relevé + simulations ─────────

def already_forecast():
    """fixture_ids ayant déjà une prévision active (non exclue) au journal."""
    if not (LEDGER / "forecasts.jsonl").exists():
        return set()
    excl = set()
    if (LEDGER / "settlements.jsonl").exists():
        excl = {json.loads(l)["forecast_id"] for l in open(LEDGER / "settlements.jsonl", encoding="utf-8")
                if json.loads(l).get("statut") == "EXCLU"}
    fmap = fixture_map(); out = set()
    for l in open(LEDGER / "forecasts.jsonl", encoding="utf-8"):
        f = json.loads(l)
        fx = f.get("fixture_id") or fmap.get(f["forecast_id"])
        if fx and f["forecast_id"] not in excl:
            out.add(fx)
    return out


def run_day(day: str, max_calls: int, now, refresh=False):
    params = json.loads((ROOT / "backtests/latest_params.json").read_text())
    club_divs = set(params["divs"])
    P_intl = json.loads((I.BT / "intl_latest_params.json").read_text())
    fx = AF.api("fixtures", date=day, timezone="UTC")["response"]
    todo, skipped = [], {}
    for f in fx:
        if f["fixture"]["status"]["short"] not in ("NS", "TBD"):
            continue
        lid = f["league"]["id"]; name = f["league"]["name"]
        h, a = f["teams"]["home"]["name"], f["teams"]["away"]["name"]
        div = AF.LEAGUE_TO_DIV.get(lid)
        if div in club_divs:
            todo.append(("club", f, div))
        elif (lid in INTL_TOURNAMENT and not YOUTH.search(f"{h} {a} {name}")
              and I.canon(h) in P_intl["elo"] and I.canon(a) in P_intl["elo"]):
            todo.append(("intl", f, INTL_TOURNAMENT[lid]))
        else:
            skipped[name] = skipped.get(name, 0) + 1
    leagues = sorted({t[1]["league"]["id"] for t in todo})
    if leagues:
        r = sh(["python3", "tools/apex_apifootball.py", "snapshot", "--date", day, "--leagues", ",".join(map(str, leagues)),
                "--max-calls", str(max_calls)])
        if r.returncode:
            print(r.stderr[-500:])
    snap = {}
    sp = ROOT / "data/apifootball/snapshots" / f"{day}.jsonl"
    if sp.exists():
        for line in open(sp, encoding="utf-8"):
            j = json.loads(line); snap[j["fixture_id"]] = j
    runs = []
    seen = set() if refresh else already_forecast()
    for kind, f, extra in sorted(todo, key=lambda t: t[1]["fixture"]["date"]):
        fid = f["fixture"]["id"]; ko = f["fixture"]["date"]
        if dt.datetime.fromisoformat(ko) <= now or fid in seen:
            continue
        rec = snap.get(fid, {})
        cmd = ["python3", "tools/apex_bsm.py", "simulate", "--kickoff", ko, "--fixture-id", str(fid), "--record"]
        h, a = f["teams"]["home"]["name"], f["teams"]["away"]["name"]
        if kind == "club":
            hn, hs = AF.fd_name(extra, h); an, as_ = AF.fd_name(extra, a)
            if min(hs, as_) < 0.6:
                runs.append({"fixture_id": fid, "match": f"{h} – {a}", "erreur": f"correspondance de noms incertaine ({hn}, {an})"})
                continue
            season = dt.date.fromisoformat(day)
            yy = season.year % 100 if season.month >= 7 else season.year % 100 - 1
            seasons = f"{yy - 1:02d}{yy:02d},{yy:02d}{yy + 1:02d}"
            cmd += ["--div", extra, "--home", hn, "--away", an, "--asof", day, "--seasons", seasons]
        else:
            lh, la, P = I.lambdas_for(h, a, tournament=extra)
            cmd += ["--home", h, "--away", a, "--lh", f"{lh:.3f}", "--la", f"{la:.3f}", "--rho", str(P["rho"]),
                    "--lambda-source", f"APEX-INTL H1 ({P['run_id']}, Elo au {P['elo_au']})",
                    "--status-note", "NON COMPARÉ AU MARCHÉ — conversion Elo→λ validée contre H0 et référence simple"]
        oa, src = odds_args(rec.get("odds", {}))
        cmd += oa + ["--odds-source", f"API-Football/{src}" if src else "aucune",
                     "--odds-time", rec.get("retrieved_at_utc", "")]
        r = sh(cmd)
        if r.returncode:
            runs.append({"fixture_id": fid, "match": f"{h} – {a}", "erreur": r.stderr.strip()[-200:]})
            continue
        fcid = [l for l in r.stdout.splitlines() if l.startswith("forecast_id")][0].split()[2]
        runs.append({"fixture_id": fid, "forecast_id": fcid, "league": f["league"]["name"], "kind": kind})
    return runs, skipped


def odds_args(odds):
    args, srcs = [], set()
    bk, o = AF.pick_book(odds, "1X2")
    if o:
        args += ["--odds-1x2", f"{o[0]},{o[1]},{o[2]}"]; srcs.add(bk)
    bk, ou = AF.pick_book(odds, "OU")
    if ou and ou.get("2.5") and all(ou["2.5"]):
        args += ["--odds-ou25", f"{ou['2.5'][0]},{ou['2.5'][1]}"]; srcs.add(bk)
    bk, bt = AF.pick_book(odds, "BTTS")
    if bt:
        args += ["--odds-btts", f"{bt[0]},{bt[1]}"]; srcs.add(bk)
    bk, ah = AF.pick_book(odds, "AH")
    if ah:
        for side in ("dom", "ext"):
            for line, c in (ah.get(side) or {}).items():
                if abs(float(line)) <= 2:
                    args += ["--odds-ah", f"{side}:{float(line) + 0.0:+.2f}:{c}"]
        srcs.add(bk)
    return args, "+".join(sorted(s for s in srcs if s))


# ───────── 5. rapport ─────────

def report(day, runs, skipped, settled, elo_info):
    L = {}
    for line in open(LEDGER / "forecasts.jsonl", encoding="utf-8"):
        r = json.loads(line); L[r["forecast_id"]] = r
    out = ROOT / "analyses" / f"{day}-bsm"
    out.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%H%MZ")   # jamais d'écrasement d'un rapport existant
    (out / f"runs_auto_{stamp}.json").write_text(json.dumps(runs, ensure_ascii=False, indent=1))
    bt = json.loads((ROOT / "backtests/latest_params.json").read_text())
    it = json.loads((I.BT / "intl_latest_params.json").read_text())
    rows, sel = [], []
    for m in runs:
        if "erreur" in m:
            rows.append(f"| — | {m['match']} | — | — | — | — | — | ⚠ {m['erreur']} |")
            continue
        r = L[m["forecast_id"]]; M = r["marches"]; o = r["cotes"].get("1X2")
        dec = r["decision"]
        if dec.startswith("SÉLECTION"):
            sel.append(f"- {r['home']} – {r['away']} : {dec}")
        sc = ", ".join(f"{s} {p * 100:.0f}%" for s, p in M["scores"][:3])
        rows.append(f"| {r['coup_envoi'][11:16]} | {r['home']} – {r['away']} ({m['league']}) | {r['lh']:.2f}–{r['la']:.2f} | "
                    f"{M['1'] * 100:.0f}/{M['X'] * 100:.0f}/{M['2'] * 100:.0f} | {'/'.join(map(str, o)) if o else '—'} | "
                    f"{M['Over2.5'] * 100:.0f} / {M['BTTS_oui'] * 100:.0f} | {sc} | {dec} |")
    txt = [f"# APEX-BSM — relevé automatique du {day}", "",
           f"- Clubs : backtest `{bt['run_id']}` — statut **{bt['statut_validation']}**",
           f"- Sélections : conversion `{it['run_id']}` — {it['statut']} ; Elo à jour au {it['elo_au']} "
           f"(+{elo_info[0]} résultats ajoutés)",
           f"- Prévisions « veille » (compositions en général non publiées), 1 ligne = 1 prévision de `ledger/forecasts.jsonl`",
           "", "## Sélections", "", *(sel or ["Aucune sélection (vetos du protocole)."]),
           "", "## Tous les matchs simulés (UTC)", "",
           "| UTC | Match | λ | 1/X/2 % | Cotes 1X2 | O2,5 / BTTS % | 3 scores | Décision |", "|---|---|---|---|---|---|---|---|",
           *rows, "", "## Hors périmètre (non simulés)", "",
           *[f"- {k} : {v}" for k, v in sorted(skipped.items(), key=lambda kv: -kv[1])[:40]],
           "", "## Règlements automatiques de la veille", "",
           *([f"- {h} – {a} : {st} {sc}" for _, h, a, st, sc, _ in settled] or ["Aucun."])]
    target = out / f"auto_{stamp}.md"
    target.write_text("\n".join(txt) + "\n", encoding="utf-8")
    return target


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date"); p.add_argument("--skip-settle", action="store_true")
    p.add_argument("--max-calls", type=int, default=1500)
    p.add_argument("--refresh", action="store_true", help="nouvelle prévision même si le match en a déjà une (après compositions)")
    a = p.parse_args()
    now = dt.datetime.now(dt.timezone.utc)
    day = a.date or now.date().isoformat()
    settled = [] if a.skip_settle else settle_auto(now)
    print(f"Règlements : {len(settled)}")
    elo_info = refresh_elo(dt.date.fromisoformat(day))
    print(f"Elo international : +{elo_info[0]} résultats, à jour au {elo_info[1]}")
    runs, skipped = run_day(day, a.max_calls, now, a.refresh)
    out = report(day, runs, skipped, settled, elo_info)
    print(f"{sum(1 for r in runs if 'forecast_id' in r)} prévisions enregistrées → {out.relative_to(ROOT)}")
    print(sh(["python3", "tools/apex_bsm.py", "audit"]).stdout[-1500:])


if __name__ == "__main__":
    main()
