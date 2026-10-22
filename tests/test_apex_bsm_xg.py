#!/usr/bin/env python3
"""Tests APEX-BSM-xG — correctifs d'audit : identité de jointure, rejets, benchmark, verdicts,
exclusion Pinnacle datée. Logique pure + un test de fetch_matches sur CSV temporaire (sans réseau)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_bsm_xg as X  # noqa: E402
import apex_bsm as B     # noqa: E402


# ───────── identité d'équipe & jointure ─────────

def test_same_team_short_vs_long():
    assert X._same_team("Wolves", "Wolverhampton Wanderers")
    assert X._same_team("Man United", "Manchester United")
    assert X._same_team("Arsenal", "Arsenal FC")
    assert not X._same_team("Arsenal", "Everton")


def test_attach_requires_date_score_and_both_teams():
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Man United", "away": "Wolves", "hg": 2, "ag": 1, "date": d}]
    by = {"2026-01-10": [{"home": "Manchester United", "away": "Wolverhampton Wanderers",
                          "hg": 2, "ag": 1, "xg_h": 1.8, "xg_a": 0.7}]}
    stat = X.attach_xg(matches, by)
    assert stat["apparie"] == 1 and matches[0]["xg_h"] == 1.8


def test_attach_rejects_wrong_identity_even_if_score_matches():
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "date": d}]
    by = {"2026-01-10": [{"home": "Everton", "away": "Fulham", "hg": 1, "ag": 1, "xg_h": 0.9, "xg_a": 1.1}]}
    stat = X.attach_xg(matches, by)
    assert stat["apparie"] == 0 and matches[0]["xg_h"] is None   # mauvaise identité → refusé


def test_attach_refuses_ambiguous():
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "date": d}]
    by = {"2026-01-10": [
        {"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "xg_h": 1.5, "xg_a": 1.3},
        {"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "xg_h": 0.8, "xg_a": 0.9}]}
    stat = X.attach_xg(matches, by)
    assert stat["ambigu"] == 1 and matches[0]["xg_h"] is None     # ≥2 candidats → refusé


def test_attach_refuses_reuse():
    d = dt.date(2026, 1, 10)
    matches = [{"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "date": d},
               {"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "date": d}]
    by = {"2026-01-10": [{"home": "Arsenal", "away": "Chelsea", "hg": 1, "ag": 1, "xg_h": 1.5, "xg_a": 1.3}]}
    stat = X.attach_xg(matches, by)
    assert stat["apparie"] == 1 and stat["sans_candidat"] == 1    # ligne xG consommée une seule fois
    assert matches[0]["xg_h"] == 1.5 and matches[1]["xg_h"] is None


def test_load_xg_rejects_bad_values(tmp_path):
    X.FS_DIR = tmp_path
    (tmp_path / "ZZ_2526.csv").write_text(
        "date_utc,home,away,hg,ag,xg_h,xg_a\n"
        "2026-01-10,A,B,1,0,1.4,0.8\n"      # OK
        "2026-01-11,C,D,2,2,-1.0,1.2\n"     # xG négatif → rejeté
        "2026-01-12,E,F,0,0,0,0\n",         # xG nul → absent (pas rejet numérique)
        encoding="utf-8")
    by, rej = X._load_xg("ZZ", ["2526"])
    assert rej == 1                          # la ligne négative
    assert "2026-01-10" in by and "2026-01-12" not in by


# ───────── fit, verdict ─────────

def test_fit_blend_w0_matches_frozen_model():
    base = dt.date(2026, 1, 1)
    import random
    random.seed(1)
    teams = ["A", "B", "C", "D"]
    hist = [{"home": random.choice(teams), "away": random.choice(teams), "hg": random.randint(0, 3),
             "ag": random.randint(0, 3), "xg_h": 1.2, "xg_a": 1.0,
             "date": base - dt.timedelta(days=i + 1)} for i in range(90)]
    hist = [m for m in hist if m["home"] != m["away"]]
    Rf = B.fit_ratings(hist, base, xi=0.003, K=6.0)
    Rb = X.fit_ratings_blend(hist, base, xi=0.003, K=6.0, w_xg=0.0)
    for t in teams:
        assert abs(Rf["teams"][t][0] - Rb["teams"][t][0]) < 1e-9
        assert abs(Rf["teams"][t][1] - Rb["teams"][t][1]) < 1e-9


def test_verdict_labels():
    assert X._verdict((+0.02, +0.01, +0.03), 10) == "PIRE"
    assert X._verdict((-0.02, -0.03, -0.01), 10) == "MEILLEUR"
    assert X._verdict((+0.01, -0.01, +0.03), 10) == "NON_CONCLUANT"   # IC contient 0 ≠ égalité
    assert X._verdict((+0.01, -0.01, +0.03), 3) == "DONNEES_INSUFFISANTES"
    assert X._verdict((None, None, None), 10) == "DONNEES_INSUFFISANTES"


# ───────── exclusion Pinnacle datée (fetch_matches, CSV temporaire) ─────────

def test_pinnacle_excluded_from_stale_date(tmp_path):
    B.HIST_DIR = tmp_path
    # 23/07/2025 = frontière ; Pinnacle (PSH/PSD/PSA) présent + moyenne (AvgH…) + Bet365.
    csv = ("Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,PSH,PSD,PSA,AvgH,AvgD,AvgA,B365H,B365D,B365A,"
           "PSCH,PSCD,PSCA,AvgCH,AvgCD,AvgCA\n"
           "ZZ,22/07/2025,A,B,1,0,2.00,3.40,3.80,1.95,3.30,3.70,1.98,3.35,3.75,2.05,3.45,3.85,2.00,3.40,3.80\n"
           "ZZ,23/07/2025,C,D,2,1,2.00,3.40,3.80,1.95,3.30,3.70,1.98,3.35,3.75,2.05,3.45,3.85,2.00,3.40,3.80\n")
    (tmp_path / "ZZ_2526.csv").write_text(csv, encoding="utf-8")
    m = {x["date"]: x for x in B.fetch_matches("ZZ", "2526", refresh=False)}
    pre = m[dt.date(2025, 7, 22)]; post = m[dt.date(2025, 7, 23)]
    assert pre["o1x2_src"] == "pinnacle"            # avant la frontière : Pinnacle
    assert post["o1x2_src"] == "moyenne"            # à partir du 23/07 : Pinnacle exclu → moyenne
    assert post["o1x2_bet_src"] == "bet365"         # prix de mise : Bet365, jamais la moyenne
    assert pre["close1x2"] is not None and post["close1x2"] is not None  # clôture : AvgC en post-stale


if __name__ == "__main__":
    import tempfile
    import types
    fails = 0
    for k, fn in list(globals().items()):
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            try:
                if "tmp_path" in fn.__code__.co_varnames[:fn.__code__.co_argcount]:
                    with tempfile.TemporaryDirectory() as d:
                        fn(Path(d))
                else:
                    fn()
                print("PASS", k)
            except Exception as e:  # noqa: BLE001
                fails += 1
                import traceback
                print("FAIL", k, repr(e))
                traceback.print_exc()
    raise SystemExit(1 if fails else 0)
