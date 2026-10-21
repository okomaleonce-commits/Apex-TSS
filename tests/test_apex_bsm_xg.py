#!/usr/bin/env python3
"""Tests APEX-BSM-xG — jointure robuste football-data ↔ xG et mélange buts↔xG (w=0 = modèle gelé)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_bsm_xg as X  # noqa: E402


def test_attach_xg_joins_despite_name_diff():
    """« Man United » (football-data) ↔ « Manchester United » (FootyStats) s'apparie par (date, score)."""
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Man United", "away": "Wolves", "hg": 2, "ag": 1, "date": d}]
    by_date = {"2026-01-10": [{"home": "Manchester United", "away": "Wolverhampton Wanderers",
                               "hg": 2, "ag": 1, "xg_h": 1.8, "xg_a": 0.7}]}
    n_ok, n_tot = X.attach_xg(matches, by_date)
    assert n_ok == 1 and n_tot == 1
    assert matches[0]["xg_h"] == 1.8 and matches[0]["xg_a"] == 0.7


def test_attach_xg_absent_falls_back_to_none():
    """Aucune donnée xG ce jour-là → pas d'invention : xg_h/xg_a restent None (repli sur buts en aval)."""
    matches = [{"home": "A", "away": "B", "hg": 1, "ag": 1, "date": dt.date(2026, 1, 10)}]
    n_ok, n_tot = X.attach_xg(matches, {})
    assert n_ok == 0 and matches[0]["xg_h"] is None and matches[0]["xg_a"] is None


def test_attach_xg_disambiguates_same_score_by_name():
    """Deux matchs même date/même score → on départage par similarité de nom, pas au hasard."""
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "date": d}]
    by_date = {"2026-01-10": [
        {"home": "Everton", "away": "Fulham", "hg": 1, "ag": 1, "xg_h": 0.9, "xg_a": 1.1},
        {"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "xg_h": 1.5, "xg_a": 1.3}]}
    X.attach_xg(matches, by_date)
    assert matches[0]["xg_h"] == 1.5 and matches[0]["xg_a"] == 1.3


def test_fit_blend_w0_matches_frozen_model():
    """w_xg=0 doit reproduire EXACTEMENT les notes du cœur gelé apex_bsm.fit_ratings."""
    import apex_bsm as B
    base = dt.date(2026, 1, 1)
    teams = ["A", "B", "C", "D"]
    hist = []
    import random
    random.seed(1)
    for i in range(80):
        h, a = random.sample(teams, 2)
        hist.append({"home": h, "away": a, "hg": random.randint(0, 3), "ag": random.randint(0, 3),
                     "xg_h": 1.2, "xg_a": 1.0, "date": base - dt.timedelta(days=i + 1)})
    ref = base
    R_frozen = B.fit_ratings(hist, ref, xi=0.003, K=6.0)
    R_blend = X.fit_ratings_blend(hist, ref, xi=0.003, K=6.0, w_xg=0.0)
    assert R_frozen is not None and R_blend is not None
    for t in teams:
        af, df = R_frozen["teams"][t]
        ab, db = R_blend["teams"][t]
        assert abs(af - ab) < 1e-9 and abs(df - db) < 1e-9


if __name__ == "__main__":
    import types
    fails = 0
    for k, fn in dict(globals()).items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            try:
                fn()
                print("PASS", k)
            except Exception as e:  # noqa: BLE001
                fails += 1
                print("FAIL", k, repr(e))
    raise SystemExit(1 if fails else 0)
