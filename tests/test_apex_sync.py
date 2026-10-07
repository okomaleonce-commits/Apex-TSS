#!/usr/bin/env python3
"""Tests d'APEX-SYNC — l'orchestrateur WORM ↔ PROTOCOL. Fonctions pures uniquement, aucun réseau,
aucun sous-processus. Couvre : priorisation (plan §4), portes d'entrée (§3.6), pont PROTOCOL (§3.7),
Risk Manager (§6), feux tricolores et traduction d'un forecast BSM.

Lancer : python3 -m pytest tests/test_apex_sync.py -q   (ou: python3 tests/test_apex_sync.py)
"""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_sync as S  # noqa: E402

NOW = dt.datetime(2026, 10, 1, 12, 0, tzinfo=dt.timezone.utc)


def _rec(**kw):
    base = {
        "fixture_id": 1, "home": "Arsenal", "away": "Chelsea", "league": "Premier League",
        "league_id": 39, "phase": "PREMATCH", "kickoff": "2026-10-01T20:00:00+00:00",
        "data_quality": 80, "confidence": 70,
        "sharp": 20, "blowout": 72, "upset": 10, "convergence": 30,
        "market_dispersion": 0.008, "scan_time_utc": "2026-10-01T10:00:00+00:00",
        "lambdas": [1.9, 0.9],
        "odds": {"Pinnacle": {"1X2": [1.55, 4.2, 6.0], "OU": {"2.5": [1.8, 2.05]}, "BTTS": [1.95, 1.85]},
                 "Bet365": {"1X2": [1.57, 4.0, 5.8]}},
        "reco": {"primary_market": "Handicap asiatique -0.5/-1 domicile", "signal_dominant": "BLOWOUT 72/100",
                 "decision": {"tier": "JOUER_PETIT"}},
    }
    base.update(kw)
    return base


# ───────── priorisation (plan §4) ─────────

def test_worm_best_ignores_none():
    assert S.worm_best({"sharp": None, "blowout": 55, "upset": None, "convergence": 40}) == 55
    assert S.worm_best({"sharp": None, "blowout": None, "upset": None, "convergence": None}) == 0


def test_tag_weight_sharp_convergence_boost():
    assert S.tag_weight(["BLOWOUT"]) == 0.6
    assert S.tag_weight([]) == 0.0
    both = S.tag_weight(["SHARP", "STATSCONVERGENCE"])
    assert both > S.tag_weight(["SHARP"]) - 1e-9  # le combo ne peut que renforcer
    assert both <= 1.0


def test_time_score_sweet_spot_and_extremes():
    assert S.time_score(6) == 1.0           # palier haut
    assert S.time_score(0.25) == 0.2        # trop tard
    assert S.time_score(120) == 0.2         # trop tôt
    assert S.time_score(-3) == 0.0          # coup d'envoi passé
    assert S.time_score(None) == 0.0


def test_liquidity_bounds_and_missing_dispersion():
    assert 0.0 <= S.liquidity_score(_rec()) <= 1.0
    r = _rec(market_dispersion=None, odds={"Pinnacle": {"1X2": [2, 3, 4]}})
    assert 0.0 <= S.liquidity_score(r) <= 1.0   # dispersion inconnue → terme neutre, pas une erreur


def test_compute_priority_formula_weights():
    pr = S.compute_priority(_rec(), now=NOW)
    manual = (0.4 * pr["worm_score"] + 0.2 * pr["liquidity"]
              + 0.2 * pr["time_score"] + 0.2 * pr["tag_weight"])
    assert abs(pr["priority"] - round(manual, 4)) < 1e-6
    assert 0.0 <= pr["priority"] <= 1.0


# ───────── portes d'entrée (plan §3.6) ─────────

def test_filters_accept_good_candidate():
    ok, reasons = S.basic_filters(_rec(), S.DEFAULT_CONFIG, now=NOW)
    assert ok and reasons == []


def test_filters_reject_live_match():
    ok, reasons = S.basic_filters(_rec(phase="LIVE"), S.DEFAULT_CONFIG, now=NOW)
    assert not ok and any("prématch" in r for r in reasons)


def test_filters_reject_too_close_and_too_far():
    soon = S.basic_filters(_rec(kickoff="2026-10-01T12:10:00+00:00"), S.DEFAULT_CONFIG, now=NOW)
    assert not soon[0] and any("trop proche" in r for r in soon[1])
    far = S.basic_filters(_rec(kickoff="2026-10-20T20:00:00+00:00"), S.DEFAULT_CONFIG, now=NOW)
    assert not far[0] and any("trop lointain" in r for r in far[1])


def test_filters_reject_no_bet_and_poor_data():
    r = _rec(data_quality=20, reco={"primary_market": "NO BET"},
             sharp=0, blowout=10, upset=5, convergence=10)
    ok, reasons = S.basic_filters(r, S.DEFAULT_CONFIG, now=NOW)
    assert not ok and len(reasons) >= 2


def test_select_candidates_ranks_and_caps():
    recs = [_rec(fixture_id=i, blowout=50 + i, home=f"H{i}") for i in range(20)]
    cfg = {**S.DEFAULT_CONFIG, "queue_capacity": 5}
    out = S.select_candidates(recs, cfg, now=NOW)
    assert len(out["queued"]) == 5
    prios = [c["priority"] for c in out["queued"]]
    assert prios == sorted(prios, reverse=True)       # trié par priorité décroissante
    assert len(out["overflow"]) == 15


# ───────── pont vers le PROTOCOL (plan §3.7) ─────────

def test_build_protocol_command_uses_worm_lambdas_and_odds():
    cmd = S.build_protocol_command(_rec(), S.DEFAULT_CONFIG)
    assert cmd["runnable"]
    argv = cmd["argv"]
    assert "--lh" in argv and "--la" in argv
    assert "--odds-1x2" in argv and "1.55,4.20,6.00" in argv
    assert "--odds-ou25" in argv and "--odds-btts" in argv
    # compositions absentes → le pont le déclare honnêtement au PROTOCOL
    assert "--missing-lineup" in argv
    # statut NON VALIDÉ transmis (le modèle ne bat pas le marché tant qu'il n'est pas backtesté)
    assert any("NON VALIDÉ" in x for x in argv)


def test_build_protocol_command_not_runnable_without_lambdas():
    cmd = S.build_protocol_command(_rec(lambdas=None), S.DEFAULT_CONFIG)
    assert not cmd["runnable"] and cmd["reason"]


# ───────── traduction d'un forecast BSM (plan §3.9) ─────────

def test_summarize_forecast_selection():
    fc = {"forecast_id": "abc", "statut_mise": "PROPOSÉE", "statut_modele": "NON VALIDÉ",
          "decision": "SÉLECTION INDICATIVE : Over 2.5 @ 1.95",
          "ev": [{"marche": "1X2 1", "p": 0.60, "cote": 1.55, "ev": 0.01},
                 {"marche": "Over 2.5", "p": 0.58, "cote": 1.95, "ev": 0.05}]}
    out = S.summarize_forecast(fc)
    assert out["status"] == "selection"
    assert out["official"]["marche"] == "Over 2.5"
    assert out["official"]["full_settle"] is True


def test_summarize_forecast_abstention_and_wait():
    assert S.summarize_forecast({"statut_mise": "AUCUNE", "decision": "ABSTENTION — aucune EV ≥ 3 %",
                                 "ev": []})["status"] == "abstention"
    wait = S.summarize_forecast({"statut_mise": "AUCUNE",
                                 "decision": "SURVEILLANCE / ABSTENTION — composition déterminante manquante",
                                 "ev": []})
    assert wait["status"] == "wait"


def test_summarize_forecast_rejects_suspect_and_low_ev():
    fc = {"statut_mise": "PROPOSÉE", "decision": "SÉLECTION",
          "ev": [{"marche": "Over 2.5", "p": 0.7, "cote": 1.6, "ev": 0.12, "suspect": True},
                 {"marche": "1X2 1", "p": 0.5, "cote": 2.0, "ev": 0.0}]}
    assert S.summarize_forecast(fc)["status"] == "abstention"


# ───────── Risk Manager (plan §6) ─────────

def test_kelly_positive_edge_and_capped():
    # p=0.58, cote=2.0 → Kelly plein 0.16 ; ×0.25 = 0.04 ; plafonné à 0.01
    f = S.kelly_stake_pct(0.58, 2.0, 0.25, 0.01)
    assert f == 0.01
    assert S.kelly_stake_pct(0.40, 2.0, 0.25, 0.01) == 0.0   # pas de bord → 0


def test_risk_decision_approves_within_caps():
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    r = S.risk_decision({"match_id": "m1", "league": "L", "market": "Over 2.5", "p": 0.58, "odds": 2.0}, cfg)
    assert r["approved"]
    assert r["stake_amount"] == round(r["stake_pct"] * 1000.0, 2)
    assert 0 < r["stake_pct"] <= cfg["max_stake_pct"]


def test_risk_decision_blocks_correlated_second_bet():
    cfg = S.DEFAULT_CONFIG
    exp = {"matchs": {"m1": 0.01}, "ligues": {}, "marches": {}}
    r = S.risk_decision({"match_id": "m1", "league": "L", "market": "BTTS oui", "p": 0.6, "odds": 2.0}, cfg, exp)
    assert not r["approved"] and any("corrélation" in x for x in r["reasons"])


def test_risk_decision_stop_loss_daily():
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    r = S.risk_decision({"match_id": "m1", "league": "L", "market": "Over 2.5", "p": 0.58, "odds": 2.0},
                        cfg, pnl_day=-60.0)   # −6 % < stop-loss −5 %
    assert not r["approved"] and any("stop-loss quotidien" in x for x in r["reasons"])


def test_risk_decision_league_cap_limits_stake():
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0, "max_exposure_league": 0.012}
    exp = {"matchs": {}, "ligues": {"L": 0.011}, "marches": {}}   # reste 0.001 < plancher
    r = S.risk_decision({"match_id": "m2", "league": "L", "market": "Over 2.5", "p": 0.58, "odds": 2.0}, cfg, exp)
    assert not r["approved"] and any("plancher" in x for x in r["reasons"])
    assert "plafond ligue" in r["caps_applied"]


def test_add_exposure_accumulates():
    exp = {"matchs": {}, "ligues": {}, "marches": {}}
    S.add_exposure(exp, {"match_id": "m1", "league": "L", "market": "Over 2.5"}, 0.01)
    S.add_exposure(exp, {"match_id": "m1", "league": "L", "market": "Over 2.5"}, 0.005)
    assert exp["matchs"]["m1"] == 0.015 and exp["ligues"]["L"] == 0.015


# ───────── feux tricolores ─────────

def test_traffic_light_rules():
    assert S.traffic_light(False, None, None) == S.RED
    assert S.traffic_light(True, None, None) == S.ORANGE            # WORM seul → jamais VERT
    assert S.traffic_light(True, {"status": "abstention"}, None) == S.RED
    assert S.traffic_light(True, {"status": "error"}, None) == S.RED
    assert S.traffic_light(True, {"status": "wait"}, None) == S.ORANGE
    assert S.traffic_light(True, {"status": "selection"}, {"approved": False}) == S.ORANGE
    assert S.traffic_light(True, {"status": "selection"}, {"approved": True}) == S.GREEN


def _run_all():
    import inspect
    import types
    fns = [v for k, v in dict(globals()).items()
           if k.startswith("test_") and isinstance(v, types.FunctionType)]
    passed = 0
    for fn in fns:
        assert not inspect.signature(fn).parameters, f"{fn.__name__} ne doit pas prendre d'argument"
        fn()
        passed += 1
    print(f"{passed}/{len(fns)} tests OK")


if __name__ == "__main__":
    _run_all()
