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
    # Chemin heureux : modèle VALIDÉ + sélection officielle structurée transmise par BSM → sélection.
    fc = {"forecast_id": "abc", "statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ",
          "decision": "SÉLECTION INDICATIVE : Over 2.5 @ 1.95",
          "official_selection": {"marche": "Over 2.5", "p": 0.58, "cote": 1.95}}
    out = S.summarize_forecast(fc)
    assert out["status"] == "selection"
    assert out["official"]["marche"] == "Over 2.5"
    assert out["official"]["full_settle"] is True
    # EV recalculée depuis p·cote (0.58·1.95−1 = 0.131), pas une valeur fournie
    assert abs(out["official"]["ev"] - 0.131) < 1e-3


def test_summarize_forecast_validation_whitelist_strict():
    # DÉFAUT AUDIT : _model_validated acceptait tout texte non vide sans « NON VALID » (ex. INCONNU).
    for bad in ("INCONNU", "", "NON VALIDÉ — λ", "modèle non validé", None):
        fc = {"statut_mise": "PROPOSÉE", "statut_modele": bad,
              "official_selection": {"marche": "Over 2.5", "p": 0.58, "cote": 1.95}}
        assert S.summarize_forecast(fc)["status"] == "unvalidated"
        assert S.traffic_light(True, S.summarize_forecast(fc), {"approved": True}) == S.ORANGE


def test_summarize_forecast_keeps_bsm_official_no_reselection():
    # DÉFAUT AUDIT : SYNC re-choisissait une EV 1X2 écartée par BSM. Désormais il ne retient QUE la
    # sélection officielle (Under 2.5), sans jamais reconstruire depuis une liste d'EV brute.
    fc = {"statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ",
          "official_selection": {"marche": "Under 2.5", "p": 0.64, "cote": 1.66},
          "ev": [{"marche": "1X2 1", "p": 0.37, "cote": 2.76, "ev": 0.021}]}  # branche écartée, ignorée
    out = S.summarize_forecast(fc)
    assert out["official"]["marche"] == "Under 2.5"   # EV réelle 0.64·1.66−1 = 6.2 %
    # Sans official_selection → abstention (SYNC ne choisit pas seul)
    fc2 = {"statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ",
           "ev": [{"marche": "1X2 1", "p": 0.37, "cote": 2.76, "ev": 0.021}]}
    assert S.summarize_forecast(fc2)["status"] == "abstention"


def test_summarize_forecast_abstention_and_wait():
    assert S.summarize_forecast({"statut_mise": "AUCUNE", "decision": "ABSTENTION — aucune EV ≥ 3 %",
                                 "ev": []})["status"] == "abstention"
    wait = S.summarize_forecast({"statut_mise": "AUCUNE",
                                 "decision": "SURVEILLANCE / ABSTENTION — composition déterminante manquante",
                                 "ev": []})
    assert wait["status"] == "wait"


def test_summarize_forecast_rejects_veto_and_low_ev():
    veto = {"statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ",
            "official_selection": {"marche": "Over 2.5", "p": 0.7, "cote": 1.6, "veto": True}}
    assert S.summarize_forecast(veto)["status"] == "abstention"
    low = {"statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ",
           "official_selection": {"marche": "1X2 1", "p": 0.50, "cote": 2.0}}  # EV = 0 < 3 %
    assert S.summarize_forecast(low)["status"] == "abstention"


def test_is_full_settle_lines():
    assert S._is_full_settle("Over 2.5") is True
    assert S._is_full_settle("Under 2.5") is True
    assert S._is_full_settle("1X2 1") is True
    assert S._is_full_settle("BTTS oui") is True
    assert S._is_full_settle("Over 2.25") is False     # ligne quart → demi-règlement
    assert S._is_full_settle("Over 2.0") is False      # ligne entière → push possible
    assert S._is_full_settle("Handicap asiatique -0.5/-1 extérieur") is False
    assert S._is_full_settle(None) is False            # marché absent → refusé


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
    assert S.traffic_light(True, {"status": "selection"}, {"approved": False}) == S.ORANGE  # refus mou
    assert S.traffic_light(True, {"status": "unvalidated"}, {"approved": True}) == S.ORANGE  # audit
    # DÉFAUT AUDIT : un refus Risk FERME doit produire ROUGE, pas ORANGE.
    assert S.traffic_light(True, {"status": "selection"},
                           {"approved": False, "hard_block": True}) == S.RED
    # DÉFAUT AUDIT : sous gel, une sélection approuvée ne doit JAMAIS afficher VERT.
    assert S.PROMOTION_FROZEN is True
    assert S.traffic_light(True, {"status": "selection"}, {"approved": True}) == S.ORANGE


def test_traffic_light_green_only_when_unfrozen(monkeypatch):
    # Le VERT reste calculable (gel levé) : garantit que c'est bien le gel, pas une impossibilité de fond.
    monkeypatch.setattr(S, "PROMOTION_FROZEN", False)
    assert S.traffic_light(True, {"status": "selection"}, {"approved": True}) == S.GREEN


def test_risk_light_requires_validation_and_freeze(monkeypatch):
    approved = {"approved": True, "hard_block": False}
    hard = {"approved": False, "hard_block": True}
    soft = {"approved": False, "hard_block": False}
    # modèle non validé → ROUGE même si Risk approuve
    assert S.risk_light(False, approved) == S.RED
    # validé + approuvé mais GELÉ → ORANGE (jamais VERT)
    assert S.risk_light(True, approved) == S.ORANGE
    # refus ferme → ROUGE ; refus mou → ORANGE
    assert S.risk_light(True, hard) == S.RED
    assert S.risk_light(True, soft) == S.ORANGE
    # gel levé + validé + approuvé → VERT (le calcul existe)
    monkeypatch.setattr(S, "PROMOTION_FROZEN", False)
    assert S.risk_light(True, approved) == S.GREEN


# ───────── verrous de décision (audit 2026-10-05) ─────────

def test_basic_filters_veto_integrity_neutralised():
    # DÉFAUT AUDIT : un match NEUTRALISÉ pour intégrité AH passait encore les filtres de candidature.
    r = _rec(reco={"primary_market": "Handicap asiatique -0.5/-1 domicile",
                   "signal_dominant": "BLOWOUT 72/100", "decision": {"tier": "NO BET"},
                   "integrity_blocked": True},
             asian_integrity={"suspect": True, "shift": -0.75})
    ok, reasons = S.basic_filters(r, S.DEFAULT_CONFIG, now=NOW)
    assert ok is False
    assert any("intégrité" in x for x in reasons)


def test_risk_rejects_ev_below_threshold():
    # DÉFAUT AUDIT : le Risk autonome acceptait une EV ~2 % (sous le seuil 3 %).
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    # p=0.52, cote=1.96 → EV = 0.52·1.96 − 1 = +1.9 % < 3 %
    r = S.risk_decision({"match_id": "m1", "league": "L", "market": "Over 2.5", "p": 0.52, "odds": 1.96}, cfg)
    assert r["approved"] is False
    assert any("EV" in x for x in r["reasons"])


def test_risk_rejects_partial_settle_handicap():
    # DÉFAUT AUDIT : un handicap à règlement partiel passait le Kelly binaire.
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    # EV largement positive, mais marché à règlement partiel → doit être refusé
    r = S.risk_decision({"match_id": "m1", "league": "L",
                         "market": "Handicap asiatique -0.5/-1 extérieur", "p": 0.60, "odds": 2.0}, cfg)
    assert r["approved"] is False
    assert any("règlement partiel" in x for x in r["reasons"]) and r["hard_block"] is True


def test_risk_recomputes_ev_ignoring_supplied():
    # DÉFAUT AUDIT : fournir ev=0.10 contournait le seuil. L'EV est TOUJOURS recalculée depuis p·cote.
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    r = S.risk_decision({"match_id": "m", "league": "L", "market": "Over 2.5",
                         "p": 0.52, "odds": 1.96, "ev": 0.10}, cfg)   # EV réelle = 1.92 %
    assert r["approved"] is False
    assert any("EV" in x for x in r["reasons"])


def test_risk_rejects_quarter_line_and_missing_market():
    # DÉFAUT AUDIT : « Over 2.25 » (ligne quart) et un marché absent étaient acceptés.
    cfg = {**S.DEFAULT_CONFIG, "bankroll": 1000.0}
    q = S.risk_decision({"match_id": "m", "league": "L", "market": "Over 2.25", "p": 0.60, "odds": 2.0}, cfg)
    assert q["approved"] is False and q["hard_block"] is True
    absent = S.risk_decision({"match_id": "m", "league": "L", "market": None, "p": 0.60, "odds": 2.0}, cfg)
    assert absent["approved"] is False and absent["hard_block"] is True


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
