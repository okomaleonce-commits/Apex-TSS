#!/usr/bin/env python3
"""APEX-CLV — ROI réel et Closing Line Value par décision (audit 2026-10-05, brique n°1).

La validation par taux (apex_validate) ne dit pas si on GAGNE de l'argent, parce que les cotes
n'étaient pas rattachées aux décisions. Or elles le sont déjà : chaque passage WORM persiste
l'intégralité des cotes dans les snapshots append-only. Ce module les exploite — sans rien capter de
neuf — pour reconstruire, par décision :

  • cote d'ENTRÉE  : la cote du marché recommandé au 1er passage où le match devient une décision
                     (JOUER/JOUER_PETIT), en prématch ;
  • cote de CLÔTURE: la cote du même marché au DERNIER passage prématch avant le coup d'envoi ;
  • RÉSULTAT       : via grade_market contre le score final du snapshot ;
  • P&L à plat (1 u) à la cote d'entrée, et CLV = cote_entrée / cote_clôture − 1.

Honnêteté : seules les familles dont la cote est réellement stockée (Over/Under 2.5, via le marché
OU) donnent un ROI/CLV ; les autres (handicap asiatique -0.75, double chance) sont marquées
« cote indisponible » et EXCLUES du ROI — jamais estimées. Un ROI positif sur quelques jours
corrélés ne lève pas le gel : c'est une mesure en avant à accumuler, pas une autorisation.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAP = ROOT / "data" / "worm" / "snapshots"
# Journal CLV DURABLE (suivi par git, contrairement aux snapshots qui sont gitignorés et effacés au
# recyclage du conteneur). C'est lui qui permet au CLV de S'ACCUMULER d'une session/jour à l'autre
# jusqu'à un échantillon de centaines de paris — condition pour que la porte globale CLV ait un sens.
CLV_LEDGER = ROOT / "ledger" / "clv.jsonl"
LIVE_DONE = {"FT", "AET", "PEN"}
TIERS = {"JOUER", "JOUER_PETIT"}


def _now_utc() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ledger_key(b: dict) -> str:
    return f"{b.get('day')}|{b.get('fixture_id')}|{b.get('market')}"


def _read_ledger() -> list:
    """Paris CLV déjà réglés et persistés (durables). Chaque ligne est immuable (append-only)."""
    if not CLV_LEDGER.exists():
        return []
    out = []
    for line in CLV_LEDGER.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def collect(days=None) -> int:
    """Journalise durablement chaque pari RÉGLÉ (cote d'entrée, cote de clôture, résultat, P&L, CLV)
    des jours donnés dans ledger/clv.jsonl. Append-only, dédupliqué par (jour, fixture, marché) :
    on n'écrit un pari qu'une fois résolu (score final + cote réelle), donc jamais réécrit ensuite.
    C'est le branchement « journalisation automatique » : appelé en fin de scan WORM pour le jour
    courant et la veille (matchs qui viennent de se terminer). Retourne le nombre de paris ajoutés."""
    import datetime as _dt
    if days is None:
        today = _dt.date.today()
        days = [today.isoformat(), (today - _dt.timedelta(days=1)).isoformat()]
    existing = {_ledger_key(b) for b in _read_ledger()}
    CLV_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new = 0
    with open(CLV_LEDGER, "a", encoding="utf-8") as fh:
        for d in days:
            for b in bets_for_day(d):
                # On ne persiste QUE les paris entièrement résolus : cote d'entrée réelle, score final
                # connu, donc P&L et CLV calculés. Un match non terminé sera capté à un passage ultérieur.
                if not (b.get("odds_available") and b.get("pnl") is not None and b.get("clv") is not None):
                    continue
                key = _ledger_key(b)
                if key in existing:
                    continue
                rec = dict(b)
                rec["collected_at_utc"] = _now_utc()
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                existing.add(key)
                new += 1
    return new


def _ou_odd(odds, market):
    """Cote décimale obtenable pour un marché OU 2.5 (Over→index 0, Under→index 1), sinon None."""
    import apex_apifootball as AF
    m = (market or "").lower()
    _, ou = AF.pick_book(odds or {}, "OU")
    if not ou or not ou.get("2.5"):
        return None
    pair = ou["2.5"]
    idx = 0 if m.startswith("over 2.5") else (1 if m.startswith("under 2.5") else None)
    if idx is None:
        return None
    try:
        o = float(pair[idx])
    except (TypeError, ValueError, IndexError):
        return None
    return o if math.isfinite(o) and o > 1.0 else None


def _final_score(passes):
    """Score final (hg, ag) à partir du dernier passage terminé du match, sinon None."""
    for r in reversed(passes):
        if r.get("status") in LIVE_DONE and isinstance(r.get("score"), dict):
            s = r["score"]
            if s.get("home") is not None and s.get("away") is not None:
                return int(s["home"]), int(s["away"])
    return None


def _passes_by_fixture(day):
    p = SNAP / f"{day}.jsonl"
    if not p.exists():
        return {}
    by = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        by.setdefault(r.get("fixture_id"), []).append(r)
    for fid in by:
        by[fid].sort(key=lambda r: r.get("scan_time_utc", ""))
    return by


def bets_for_day(day):
    """Reconstruit les paris (décisions OU avec cote) d'une journée, réglés au score final."""
    import apex_worm as W
    out = []
    for fid, passes in _passes_by_fixture(day).items():
        # 1er passage prématch où le match est une décision JOUER/JOUER_PETIT
        entry = None
        for r in passes:
            dec = (r.get("reco", {}).get("decision", {}) or {})
            if r.get("phase") == "PREMATCH" and dec.get("tier") in TIERS:
                entry = r
                break
        if entry is None:
            continue
        market = (entry.get("reco", {}).get("decision", {}) or {}).get("marche")
        entry_odd = _ou_odd(entry.get("odds"), market)
        prematch = [r for r in passes if r.get("phase") == "PREMATCH"]
        close_odd = _ou_odd(prematch[-1].get("odds"), market) if prematch else None
        fin = _final_score(passes)
        rec = {"day": day, "fixture_id": fid, "match": f"{entry.get('home')}-{entry.get('away')}",
               "market": market, "tier": (entry.get("reco", {}).get("decision", {}) or {}).get("tier"),
               "entry_odd": entry_odd, "close_odd": close_odd, "final": fin,
               "odds_available": entry_odd is not None}
        if entry_odd is None or fin is None:
            rec["result"], rec["pnl"], rec["clv"] = None, None, None
            out.append(rec)
            continue
        res = W.grade_market(market, fin[0], fin[1])
        rec["result"] = res
        rec["pnl"] = {"gagné": entry_odd - 1.0, "demi-gagné": (entry_odd - 1.0) / 2,
                      "perdu": -1.0, "push": 0.0}.get(res)
        rec["clv"] = (entry_odd / close_odd - 1.0) if close_odd else None
        out.append(rec)
    return out


def _snapshot_bets(days):
    """Paris reconstruits à la volée depuis les snapshots (éphémères) des jours donnés."""
    import apex_worm as W  # noqa: F401 (assure l'import pour grade_market via bets_for_day)
    bets, skipped = [], 0
    for d in days:
        for b in bets_for_day(d):
            if b["odds_available"] and b["pnl"] is not None:
                bets.append(b)
            else:
                skipped += 1
    return bets, skipped


def _aggregate(bets, skipped, days, source):
    staked = len(bets)
    pnl = sum(b["pnl"] for b in bets)
    clvs = [b["clv"] for b in bets if b["clv"] is not None]
    roi = (pnl / staked) if staked else None
    beat = sum(1 for c in clvs if c > 0)
    return {"days": days, "source": source, "n_bets_roi": staked, "n_sans_cote": skipped,
            "pnl_units": round(pnl, 3), "roi": (round(roi, 4) if roi is not None else None),
            "clv_moyen": (round(sum(clvs) / len(clvs), 4) if clvs else None),
            "clv_n": len(clvs), "clv_beat_close_pct": (round(100 * beat / len(clvs), 1) if clvs else None),
            "bets": bets}


def summary(days=None):
    """Résumé ROI/CLV.

    • `days` explicite → reconstruction depuis les snapshots de ces jours (ciblage, tests).
    • `days=None` (défaut, utilisé par la porte du gel) → lit le JOURNAL DURABLE ledger/clv.jsonl, qui
      s'accumule dans le temps ; s'il est vide (jamais collecté), repli sur les snapshots présents —
      comportement identique à l'ancien. Ainsi le CLV survit au recyclage du conteneur."""
    if days is not None:
        bets, skipped = _snapshot_bets(days)
        return _aggregate(bets, skipped, days, "snapshots")
    led = _read_ledger()
    if led:
        led = [b for b in led if b.get("pnl") is not None]
        return _aggregate(led, 0, sorted({b.get("day") for b in led}), "ledger")
    snap_days = sorted(p.stem for p in SNAP.glob("*.jsonl"))
    bets, skipped = _snapshot_bets(snap_days)
    return _aggregate(bets, skipped, snap_days, "snapshots")


def _print_summary():
    s = summary()
    src = {"ledger": "journal durable ledger/clv.jsonl (cumulatif)",
           "snapshots": "snapshots du jour (éphémères — journal durable encore vide)"}.get(
        s.get("source"), s.get("source"))
    print(f"APEX-CLV · {len(s['days'])} jours · source : {src} · marché avec cote stockée (Over/Under 2.5)")
    print(f"  Paris avec ROI calculable : {s['n_bets_roi']}  (exclus faute de cote : {s['n_sans_cote']})")
    if s["n_bets_roi"]:
        print(f"  P&L (mises à plat 1u, cote d'ENTRÉE réelle) : {s['pnl_units']:+.2f} u")
        print(f"  ROI RÉEL : {s['roi']*100:+.2f} %  (sur {s['n_bets_roi']} paris)")
    if s["clv_n"]:
        print(f"  CLV moyen : {s['clv_moyen']*100:+.2f} %  · bat la clôture : {s['clv_beat_close_pct']} % "
              f"des {s['clv_n']} paris")
    print("\nLecture : le CLV est le meilleur prédicteur d'un vrai bord (battre la clôture > gagner un")
    print("soir). ROI/CLV positifs ICI ne lèvent PAS le gel : échantillon encore court et corrélé, et seules")
    print("les familles Over/Under 2.5 sont couvertes (handicap/DC : cote non stockée → exclus).")
    print("Pour accumuler : `apex_clv.py collect` à chaque passage (déjà branché en fin de scan WORM).")


def main(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="APEX-CLV — ROI réel et Closing Line Value")
    sub = p.add_subparsers(dest="cmd")
    c = sub.add_parser("collect", help="journalise durablement les paris réglés (ledger/clv.jsonl)")
    c.add_argument("--date", help="un jour AAAA-MM-JJ")
    c.add_argument("--days", help="plusieurs jours séparés par des virgules")
    sub.add_parser("summary", help="résumé ROI/CLV (défaut)")
    a = p.parse_args(argv)
    if a.cmd == "collect":
        days = None
        if a.days:
            days = [d.strip() for d in a.days.split(",") if d.strip()]
        elif a.date:
            days = [a.date]
        n = collect(days)
        print(f"APEX-CLV collect : {n} pari(s) réglé(s) ajouté(s) au journal durable "
              f"{CLV_LEDGER.relative_to(ROOT)} (append-only, dédupliqué).")
        s = summary()
        if s["clv_n"]:
            print(f"  Cumul : {s['clv_n']} paris CLV · CLV moyen {s['clv_moyen']*100:+.2f} % · "
                  f"bat la clôture {s['clv_beat_close_pct']} % · P&L {s['pnl_units']:+.2f} u")
        return 0
    _print_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
