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
LIVE_DONE = {"FT", "AET", "PEN"}
TIERS = {"JOUER", "JOUER_PETIT"}


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


def summary(days=None):
    import apex_worm as W  # noqa: F401 (assure l'import pour grade_market via bets_for_day)
    if days is None:
        days = sorted(p.stem for p in SNAP.glob("*.jsonl"))
    bets, skipped = [], 0
    for d in days:
        for b in bets_for_day(d):
            if b["odds_available"] and b["pnl"] is not None:
                bets.append(b)
            else:
                skipped += 1
    staked = len(bets)
    pnl = sum(b["pnl"] for b in bets)
    clvs = [b["clv"] for b in bets if b["clv"] is not None]
    roi = (pnl / staked) if staked else None
    beat = sum(1 for c in clvs if c > 0)
    return {"days": days, "n_bets_roi": staked, "n_sans_cote": skipped,
            "pnl_units": round(pnl, 3), "roi": (round(roi, 4) if roi is not None else None),
            "clv_moyen": (round(sum(clvs) / len(clvs), 4) if clvs else None),
            "clv_n": len(clvs), "clv_beat_close_pct": (round(100 * beat / len(clvs), 1) if clvs else None),
            "bets": bets}


def main(argv=None):
    s = summary()
    print(f"APEX-CLV · {len(s['days'])} jours · marchés avec cote stockée (Over/Under 2.5)")
    print(f"  Paris avec ROI calculable : {s['n_bets_roi']}  (exclus faute de cote : {s['n_sans_cote']})")
    if s["n_bets_roi"]:
        print(f"  P&L (mises à plat 1u, cote d'ENTRÉE réelle) : {s['pnl_units']:+.2f} u")
        print(f"  ROI RÉEL : {s['roi']*100:+.2f} %  (sur {s['n_bets_roi']} paris)")
    if s["clv_n"]:
        print(f"  CLV moyen : {s['clv_moyen']*100:+.2f} %  · bat la clôture : {s['clv_beat_close_pct']} % "
              f"des {s['clv_n']} paris")
    print("\nLecture : le CLV est le meilleur prédicteur d'un vrai bord (battre la clôture > gagner un")
    print("soir). ROI/CLV positifs ICI ne lèvent PAS le gel : échantillon court et corrélé, et seules")
    print("les familles Over/Under 2.5 sont couvertes (handicap/DC : cote non stockée → exclus).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
