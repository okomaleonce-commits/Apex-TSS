#!/usr/bin/env python3
"""Tests du scanner APEX-WORM (spec §31). Fonctions pures uniquement — aucun appel réseau.

Couvre : fenêtre APEX + fuseau, normalisation des cotes (démarge), Poisson, moteurs d'anomalies
(bornes 0-100 et sens), convergence, données manquantes, détection de changements, déduplication.
Lancer : python3 -m pytest tests/test_apex_worm.py -q   (ou: python3 tests/test_apex_worm.py)
"""
import datetime as dt
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_worm as W  # noqa: E402


# ───────── fenêtre APEX (spec §2) ─────────

def test_window_after_8am_is_same_day():
    tz = ZoneInfo("UTC")
    now = dt.datetime(2026, 9, 30, 10, 0, tzinfo=tz)
    day, start, end = W.apex_window(now)
    assert day == dt.date(2026, 9, 30)
    assert start == dt.datetime(2026, 9, 30, 8, 0, 0, tzinfo=tz)
    assert end == dt.datetime(2026, 10, 1, 7, 59, 59, tzinfo=tz)


def test_window_before_8am_is_previous_day():
    tz = ZoneInfo("UTC")
    now = dt.datetime(2026, 9, 30, 3, 0, tzinfo=tz)
    day, start, end = W.apex_window(now)
    assert day == dt.date(2026, 9, 29)
    assert start.hour == 8 and start.date() == dt.date(2026, 9, 29)


def test_window_respects_configurable_timezone():
    # 07:00 à Paris = on est encore dans la journée APEX de la veille
    tz = ZoneInfo("Europe/Paris")
    now = dt.datetime(2026, 9, 30, 7, 0, tzinfo=tz)
    day, start, end = W.apex_window(now)
    assert day == dt.date(2026, 9, 29)


def test_in_window_bounds():
    tz = ZoneInfo("UTC")
    _, start, end = W.apex_window(dt.datetime(2026, 9, 30, 10, 0, tzinfo=tz))
    assert W.in_window(dt.datetime(2026, 9, 30, 20, 0, tzinfo=tz), start, end)
    assert W.in_window(dt.datetime(2026, 10, 1, 2, 0, tzinfo=tz), start, end)   # après minuit, même journée APEX
    assert not W.in_window(dt.datetime(2026, 10, 1, 9, 0, tzinfo=tz), start, end)
    assert not W.in_window(dt.datetime(2026, 9, 30, 7, 0, tzinfo=tz), start, end)


def test_timezone_env_default_utc(monkeypatch=None):
    old = os.environ.pop("APEX_TIMEZONE", None)
    try:
        assert W.apex_tz() == ZoneInfo("UTC")
    finally:
        if old:
            os.environ["APEX_TIMEZONE"] = old


# ───────── normalisation des cotes ─────────

def test_demargin_sums_to_one():
    p = W.demargin([2.0, 3.5, 4.0])
    assert p is not None
    assert abs(sum(p) - 1.0) < 1e-3   # arrondi à 4 décimales


def test_demargin_removes_margin_order_preserved():
    p = W.demargin([1.5, 4.0, 7.0])
    assert p[0] > p[1] > p[2]


def test_demargin_rejects_bad_odds():
    assert W.demargin([0, 3, 4]) is None
    assert W.demargin([1.0, 2.0]) is None
    assert W.demargin(None) is None


def test_margin_positive():
    m = W.margin([1.9, 1.9])
    assert m > 0


def test_book_dispersion_missing_returns_none():
    disp, n = W.book_dispersion({"Pinnacle": {"1X2": [2.0, 3.4, 3.8]}}, "1X2")
    assert disp is None and n == 1  # un seul book → pas de dispersion


# ───────── Poisson / modèle structurel ─────────

def test_poisson_1x2_sums_to_one():
    p = W.poisson_1x2(1.6, 1.1)
    assert abs(sum(p) - 1.0) < 1e-3


def test_poisson_over_monotone():
    assert W.poisson_over(2.0, 2.0, 2.5) > W.poisson_over(0.8, 0.7, 2.5)


def test_structural_lambdas_missing_team():
    strength = {1: {"points": 20, "played": 10, "gf": 18, "ga": 8}}
    assert W.structural_lambdas(strength, 1, 999, 2.7) is None  # équipe 999 absente


def test_structural_lambdas_home_advantage():
    strength = {1: {"points": 15, "played": 10, "gf": 15, "ga": 10},
                2: {"points": 15, "played": 10, "gf": 15, "ga": 10}}
    lh, la = W.structural_lambdas(strength, 1, 2, 2.7)
    assert lh > la  # équipes identiques → l'avantage terrain fait pencher le domicile


# ───────── moteurs d'anomalies : bornes et sens ─────────

STRONG = {"points": 27, "played": 10, "gf": 25, "ga": 5}
WEAK = {"points": 3, "played": 10, "gf": 6, "ga": 24}


def test_blowout_none_without_standings():
    sc, comp = W.blowout_engine([0.7, 0.2, 0.1], {}, 1, 2)
    assert sc is None


def test_blowout_high_for_dominant_favorite():
    strength = {1: STRONG, 2: WEAK}
    sc, comp = W.blowout_engine([0.82, 0.12, 0.06], strength, 1, 2)
    assert sc is not None and 0 <= sc <= 100
    assert sc >= 50
    assert comp["fav"] == "domicile"


def test_upset_bounds_and_dog_side():
    strength = {1: WEAK, 2: STRONG}  # domicile faible
    sc, comp = W.upset_engine([0.30, 0.28, 0.42], strength, 1, 2)
    assert sc is not None and 0 <= sc <= 100
    assert comp["dog"] == "domicile"


def test_convergence_over_direction():
    # deux équipes offensives et perméables → Over doit dominer
    off = {"points": 15, "played": 10, "gf": 22, "ga": 20}
    strength = {1: off, 2: off}
    sc, direction, comp = W.convergence_engine(strength, 1, 2, market_over=0.62, avg_gf=2.7)
    assert 0 <= sc <= 100
    assert direction == "Over 2.5"


def test_convergence_none_without_standings():
    sc, d, comp = W.convergence_engine({}, 1, 2, None, 2.7)
    assert sc is None


def test_sharp_marks_unavailable_components():
    sc, comp = W.sharp_signal([0.5, 0.3, 0.2], [0.45, 0.32, 0.23], 2.0, 0.01, 0.02)
    assert 0 <= sc <= 100
    assert comp["volume"] == W.UNAVAILABLE
    assert comp["public_pct"] == W.UNAVAILABLE


# ───────── recommandation & données manquantes ─────────

def test_recommend_no_bet_on_poor_data():
    rec = {"data_quality": 20, "sharp": 0, "blowout": None, "upset": None, "convergence": None, "odds": {}}
    out = W.recommend(rec)
    assert out["primary_market"] == "NO BET"
    assert out["value"] == "NON CONFIRMÉE"


def test_recommend_never_confirms_value_without_ev():
    rec = {"data_quality": 80, "blowout": 70, "upset": 10, "convergence": 30, "sharp": 20,
           "odds": {"Pinnacle": {"1X2": [1.4, 4.5, 7.0]}}, "ev_best": None}
    out = W.recommend(rec)
    assert out["value"] == "NON CONFIRMÉE"
    assert out["primary_market"] != "NO BET"


def test_data_quality_bounds():
    assert 0 <= W.data_quality({"odds": {}}) <= 100
    rich = {"odds": {"Pinnacle": {"1X2": [2, 3, 4], "OU": {"2.5": [1.9, 1.9]}},
                     "Bet365": {"1X2": [2, 3, 4]}, "1xBet": {"1X2": [2, 3, 4]}},
            "strength_ok": True, "compositions": [{"x": 1}], "blessures": []}
    assert W.data_quality(rich) > 60


# ───────── détection de changements & déduplication ─────────

def test_detect_odds_move():
    prev = {"phase": "PREMATCH", "market_prob_1x2": [0.50, 0.30, 0.20], "compositions": None,
            "sharp": 10, "blowout": 10, "upset": 10, "convergence": 10}
    rec = {"phase": "PREMATCH", "market_prob_1x2": [0.60, 0.25, 0.15], "compositions": None,
           "home": "A", "away": "B", "sharp": 10, "blowout": 10, "upset": 10, "convergence": 10,
           "reco": {"primary_market": "NO BET"}}
    types = {c["type"] for c in W.detect_changes(prev, rec)}
    assert "ODDS MOVE" in types


def test_detect_lineup_change_and_phase():
    prev = {"phase": "PREMATCH", "market_prob_1x2": [0.5, 0.3, 0.2], "compositions": None,
            "sharp": 10, "blowout": 10, "upset": 10, "convergence": 10}
    rec = {"phase": "LIVE", "market_prob_1x2": [0.5, 0.3, 0.2], "compositions": [{"x": 1}],
           "home": "A", "away": "B", "sharp": 10, "blowout": 10, "upset": 10, "convergence": 10,
           "reco": {"primary_market": "NO BET"}}
    types = {c["type"] for c in W.detect_changes(prev, rec)}
    assert "PHASE" in types and "LINEUP CHANGE" in types


def test_latest_by_fixture_dedups(tmp_path):
    day = dt.date(2026, 9, 30)
    old_snap = W.SNAP
    try:
        W.SNAP = tmp_path
        p = tmp_path / f"{day.isoformat()}.jsonl"
        import json
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"fixture_id": 1, "sharp": 10}) + "\n")
            fh.write(json.dumps({"fixture_id": 1, "sharp": 55}) + "\n")  # relevé plus récent
            fh.write(json.dumps({"fixture_id": 2, "sharp": 30}) + "\n")
        rows = W.latest_by_fixture(day)
        assert len(rows) == 2                                   # dédupliqué par fixture
        assert next(r for r in rows if r["fixture_id"] == 1)["sharp"] == 55  # dernier gagne
    finally:
        W.SNAP = old_snap


# ───────── argent public (excapper) : volume, confirmation, RLM ─────────


def test_excapper_parses_public_money_table():
    import importlib
    XC = importlib.import_module("apex_excapper")
    html = (
        '<tr class="a_link" game_id="123" data-game-link="x">'
        '<td>30.09.2026 16:00</td><td><img alt="RS" title="RS"></td>'
        '<td>Montenegrin 2nd League</td><td>FK Iskra - FK Grbalj</td><td>20 694 €</td></tr>'
        '<tr class="a_link" game_id="456" data-game-link="y">'
        '<td>30.09.2026 16:30</td><td><img alt="CZ" title="CZ"></td>'
        '<td>Czech Cup</td><td>FC Vsetin - Bohemians 1905</td><td>68812 €</td></tr>')
    rows = XC.parse_matches(html)
    assert len(rows) == 2
    assert rows[0]["home"] == "FK Iskra" and rows[0]["away"] == "FK Grbalj"
    assert rows[0]["all_money_eur"] == 20694.0
    assert rows[1]["all_money_eur"] == 68812.0


def test_excapper_robots_blocks_disallowed():
    import importlib
    XC = importlib.import_module("apex_excapper")
    XC._ROBOTS_CACHE["ex.test"] = ["/private/"]      # simule une règle Disallow
    assert XC.robots_allows("https://ex.test/public/x") is True
    assert XC.robots_allows("https://ex.test/private/y") is False

def test_sharp_uses_exchange_volume_and_marks_observed():
    exch = {"total_matched": 100000.0, "fair": [0.55, 0.28, 0.17], "money": [0.6, 0.2, 0.2]}
    sc, comp = W.sharp_signal([0.52, 0.30, 0.18], [0.50, 0.31, 0.19], 2.0, 0.01, 0.02, exch)
    assert comp["volume"]["provenance"] == W.OBSERVED
    assert comp["volume"]["total_matched"] == 100000.0
    assert comp["public_pct"]["provenance"] == W.OBSERVED
    assert 0 <= sc <= 100


def test_sharp_detects_reverse_line_movement():
    # argent public majoritaire sur l'issue 0, mais sa proba a BAISSÉ (cote qui dérive) → RLM
    exch = {"total_matched": 50000.0, "fair": [0.40, 0.30, 0.30], "money": [0.70, 0.15, 0.15]}
    sc, comp = W.sharp_signal([0.40, 0.30, 0.30], [0.46, 0.29, 0.25], 3.0, 0.02, None, exch)
    assert "rlm" in comp
    assert comp["rlm"]["issue"] == 0


def test_align_exchange_maps_runners_to_hda():
    es = {"exchange_fair": {"Arsenal": 0.6, "The Draw": 0.25, "Leeds": 0.15},
          "money_pct": {"Arsenal": 0.7, "The Draw": 0.1, "Leeds": 0.2}, "total_matched": 12345.0}
    out = W.align_exchange(es, "Arsenal", "Leeds")
    assert out["fair"][0] == 0.6 and out["fair"][2] == 0.15
    assert out["money"][0] == 0.7
    assert out["total_matched"] == 12345.0


# ───────── décision de marché (spec §22, §44) ─────────

def test_decision_present_for_signal():
    rec = {"data_quality": 80, "blowout": 72, "upset": 10, "convergence": 30, "sharp": 20,
           "odds": {"Pinnacle": {"1X2": [1.4, 4.5, 7.0]}}, "ev_best": None, "exchange_confirmation": False}
    out = W.recommend(rec)
    assert "decision" in out
    assert out["decision"]["tier"] in ("JOUER", "JOUER_PETIT", "SURVEILLER")
    assert out["decision"]["marche"] == out["primary_market"]


def test_decision_jouer_needs_confirmation():
    base = {"data_quality": 80, "blowout": 75, "upset": 10, "convergence": 30, "sharp": 20,
            "odds": {"Pinnacle": {"1X2": [1.4, 4.5, 7.0]}}, "ev_best": None}
    no_conf = W.recommend({**base, "exchange_confirmation": False, "rlm": None})
    with_conf = W.recommend({**base, "exchange_confirmation": True, "rlm": None})
    assert no_conf["decision"]["tier"] == "JOUER_PETIT"     # fort mais sans confirmation
    assert with_conf["decision"]["tier"] == "JOUER"          # fort + confirmation d'échange


def test_decision_no_bet_units_zero():
    out = W.recommend({"data_quality": 20, "sharp": 0, "blowout": None, "upset": None,
                       "convergence": None, "odds": {}})
    assert out["decision"]["tier"] == "NO BET"
    assert out["decision"]["unites_indicatives"] == 0.0


# ───────── recalibration : seuils par signal (reco audit) ─────────

def test_statsconvergence_below_min_is_no_bet():
    # convergence 50 : au-dessus de l'ancien seuil global (45) mais sous le nouveau seuil
    # STATSCONVERGENCE (60) → plus de pari officiel.
    rec = {"data_quality": 80, "blowout": None, "upset": None, "convergence": 50, "sharp": None,
           "convergence_dir": "Over 2.5", "odds": {"Pinnacle": {"1X2": [2.0, 3.3, 3.6]}}, "ev_best": None}
    assert W.recommend(rec)["primary_market"] == "NO BET"


def test_statsconvergence_over_at_60_is_actionable():
    rec = {"data_quality": 80, "blowout": None, "upset": None, "convergence": 60, "sharp": None,
           "convergence_dir": "Over 2.5", "odds": {"Pinnacle": {"1X2": [2.0, 3.3, 3.6]}}, "ev_best": None}
    assert W.recommend(rec)["primary_market"] == "Over 2.5"


def test_statsconvergence_under_needs_higher_bar():
    # Under 2.5 à 60 : sous le seuil renforcé (67) → NO BET ; à 67 → actionnable.
    base = {"data_quality": 80, "blowout": None, "upset": None, "sharp": None,
            "convergence_dir": "Under 2.5", "odds": {"Pinnacle": {"1X2": [2.0, 3.3, 3.6]}}, "ev_best": None}
    assert W.recommend({**base, "convergence": 60})["primary_market"] == "NO BET"
    assert W.recommend({**base, "convergence": 67})["primary_market"] == "Under 2.5"


def test_blowout_threshold_unchanged():
    # BLOWOUT reste actionnable dès 45 (barre basse inchangée).
    rec = {"data_quality": 80, "blowout": 45, "upset": None, "convergence": None, "sharp": None,
           "odds": {"Pinnacle": {"1X2": [1.4, 4.5, 7.0]}}, "ev_best": None}
    assert "Handicap" in W.recommend(rec)["primary_market"]


# ───────── recalibration : confiance pondérée par fiabilité du signal ─────────

def test_confidence_blowout_outranks_statsconvergence():
    base = {"data_quality": 80, "min_played": 8, "compositions": None, "signal_stable": True}
    blow = W.confidence({**base, "blowout": 80, "convergence": None}, 0.01)
    conv = W.confidence({**base, "blowout": None, "convergence": 80, "convergence_dir": "Over 2.5"}, 0.01)
    assert blow > conv   # même contexte, BLOWOUT plus fiable → confiance plus haute


def test_confidence_under_malus():
    base = {"data_quality": 80, "min_played": 8, "compositions": None, "signal_stable": True,
            "blowout": None, "convergence": 80}
    over = W.confidence({**base, "convergence_dir": "Over 2.5"}, 0.01)
    under = W.confidence({**base, "convergence_dir": "Under 2.5"}, 0.01)
    assert under < over   # Under 2.5 pénalisé (marché le plus faible en bilan)


def test_confidence_bounds_still_0_100():
    assert 0 <= W.confidence({"data_quality": 100, "min_played": 20, "compositions": [{"x": 1}],
                              "signal_stable": True, "blowout": 100}, 0.0) <= 100


# ───────── email digest ─────────

def test_build_email_html(tmp_path):
    import json
    day = dt.date(2026, 9, 30)
    old = W.SNAP
    try:
        W.SNAP = tmp_path
        rec = {"fixture_id": 1, "home": "A", "away": "B", "league": "L", "country": "Eng",
               "kickoff": "2026-09-30T18:00:00+00:00", "phase": "PREMATCH", "sharp": 20, "blowout": 75,
               "upset": 10, "convergence": 30, "confidence": 70, "data_quality": 80,
               "adjusted_prob_1x2": [0.6, 0.25, 0.15], "signal_stable": False,
               "reco": {"primary_market": "AH -0.5 dom", "value": "NON CONFIRMÉE",
                        "decision": {"tier": "JOUER_PETIT", "unites_indicatives": 0.5, "marche": "AH -0.5 dom",
                                     "signal": "BLOWOUT 75/100", "confirmation_echange": False}}}
        with open(tmp_path / f"{day.isoformat()}.jsonl", "w", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
        subject, html = W.build_email_html(day)
        assert "APEX-WORM" in subject
        assert "<html" in html and "JOUER_PETIT" in html
        assert "A–B" in html
    finally:
        W.SNAP = old


# ───────── bilan : notation des marchés ─────────

def test_grade_over_under():
    assert W.grade_market("Over 2.5", 2, 1) == "gagné"
    assert W.grade_market("Over 2.5", 1, 1) == "perdu"
    assert W.grade_market("Under 2.5", 1, 1) == "gagné"
    assert W.grade_market("Under 2.5", 2, 1) == "perdu"


def test_grade_ah_half_lines():
    # AH -0.5/-1 domicile : +2 = gagné, +1 = demi, nul/défaite = perdu
    assert W.grade_market("Handicap asiatique -0.5/-1 domicile (ou Team Over 1.5 domicile)", 3, 1) == "gagné"
    assert W.grade_market("Handicap asiatique -0.5/-1 domicile", 1, 0) == "demi-gagné"
    assert W.grade_market("Handicap asiatique -0.5/-1 domicile", 1, 1) == "perdu"
    # côté extérieur
    assert W.grade_market("Handicap asiatique -0.5/-1 extérieur", 0, 2) == "gagné"
    assert W.grade_market("Handicap asiatique -0.5/-1 extérieur", 0, 1) == "demi-gagné"


def test_grade_double_chance():
    assert W.grade_market("Double chance 1X / +0.5 AH domicile", 0, 0) == "gagné"
    assert W.grade_market("Double chance 1X / +0.5 AH domicile", 0, 1) == "perdu"
    assert W.grade_market("Double chance X2 / +0.5 AH extérieur", 0, 1) == "gagné"
    assert W.grade_market("Double chance X2 / +0.5 AH extérieur", 2, 0) == "perdu"


def test_grade_non_gradable_and_missing():
    assert W.grade_market("Aligné sur le mouvement de ligne", 1, 1) == "non-gradé"
    assert W.grade_market("Over 2.5", None, None) == "non-gradé"


def test_hit_rate_half_counts():
    rows = [{"result": "gagné"}, {"result": "demi-gagné"}, {"result": "perdu"}, {"result": "push"}]
    taux, n = W._hit_rate(rows)
    assert n == 3            # push exclu
    assert taux == round((1 + 0.5) / 3, 3)


def test_compute_bilan_grades_done_only(tmp_path):
    import json
    day = dt.date(2026, 9, 30)
    old = W.SNAP
    try:
        W.SNAP = tmp_path
        recs = [
            {"fixture_id": 1, "home": "A", "away": "B", "league": "L", "phase": "DONE",
             "score": {"home": 3, "away": 0}, "confidence": 80, "data_quality": 80,
             "reco": {"decision": {"tier": "JOUER", "signal": "BLOWOUT 100/100",
                                   "marche": "Handicap asiatique -0.5/-1 domicile"}}},
            {"fixture_id": 2, "home": "C", "away": "D", "league": "L", "phase": "DONE",
             "score": {"home": 0, "away": 0}, "confidence": 60, "data_quality": 70,
             "reco": {"decision": {"tier": "JOUER_PETIT", "signal": "STATSCONVERGENCE 83/100",
                                   "marche": "Over 2.5"}}},
            {"fixture_id": 3, "home": "E", "away": "F", "league": "L", "phase": "LIVE",
             "score": {"home": 1, "away": 0}, "confidence": 60, "data_quality": 70,
             "reco": {"decision": {"tier": "JOUER_PETIT", "signal": "UPSET 60/100", "marche": "Under 2.5"}}},
        ]
        with open(tmp_path / f"{day}.jsonl", "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r) + "\n")
        b = W.compute_bilan(day)
        assert b["n_decisions"] == 3
        assert b["n_terminees_gradees"] == 2        # le LIVE n'est pas gradé
        assert b["n_non_terminees"] == 1
        results = {g["match"]: g["result"] for g in b["decisions"]}
        assert results["A – B"] == "gagné"
        assert results["C – D"] == "perdu"          # 0-0, Over 2.5 perdu
        subj, html = W.build_bilan_email(b)
        assert "BILAN" in subj and "Conclusions" in html
    finally:
        W.SNAP = old


def _run_all():
    import types
    g = dict(globals())
    fns = [v for k, v in g.items() if k.startswith("test_") and isinstance(v, types.FunctionType)]
    passed = 0
    for fn in fns:
        try:
            import inspect
            if "tmp_path" in inspect.signature(fn).parameters:
                import tempfile
                with tempfile.TemporaryDirectory() as d:
                    fn(Path(d))
            else:
                fn()
            passed += 1
        except Exception as e:  # noqa: BLE001
            print(f"ÉCHEC {fn.__name__}: {e}")
            raise
    print(f"{passed}/{len(fns)} tests OK")


if __name__ == "__main__":
    _run_all()
