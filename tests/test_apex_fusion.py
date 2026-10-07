#!/usr/bin/env python3
"""Tests APEX-FUSION — moteur unique (voix sourcées honnêtes + digest/ email uniques)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_fusion as F  # noqa: E402
import orion_consensus as O  # noqa: E402


def _worm(tier="JOUER_PETIT", sharp=None, rlm=False, ev=None):
    return {"reco": {"decision": {"tier": tier}}, "sharp": sharp, "rlm": rlm, "ev_best": ev}


def test_worm_only_is_one_voice_and_collects():
    votes, couches = F.orion_votes(_worm("JOUER"), None, None)
    assert couches == {"W": True, "M": False, "C": False, "S": False, "K": False}
    assert len(votes) == 1 and votes[0]["source"] == "worm_classement"
    # une seule voix indépendante -> ORION COLLECTE, même si WORM dit JOUER
    assert O.arbitrate(votes)["decision"] == "COLLECTER"


def test_market_layer_adds_independent_voice():
    # RLM présent -> voix marché distincte
    votes, couches = F.orion_votes(_worm("JOUER_PETIT", rlm=True), None, None)
    assert couches["M"] is True
    srcs = sorted(v["source"] for v in votes)
    assert srcs == ["marche", "worm_classement"]


def test_mi_live_watch_feeds_market_voice():
    mi = {"status": "LIVE_UPSET_WATCH"}
    votes, couches = F.orion_votes(_worm("SURVEILLER"), mi, None)
    assert couches["M"] is True
    mv = [v for v in votes if v["source"] == "marche"][0]
    assert mv["p"] >= 0.6


def test_bsm_voice_only_when_provided_and_labelled_stats_bsm():
    # FORECAST n'apparaît QUE si une sim calibrée est fournie, et sous la source stats_bsm —
    # jamais la Poisson-classement WORM remaquillée en BSM (défaut D1).
    v0, c0 = F.orion_votes(_worm("JOUER"), None, None)
    assert c0["S"] is False and all(v["source"] != "stats_bsm" for v in v0)
    v1, c1 = F.orion_votes(_worm("JOUER"), None, {"p": 0.66})
    assert c1["S"] is True
    bsm = [v for v in v1 if v["source"] == "stats_bsm"][0]
    assert bsm["agent"] == "FORECAST/bsm" and abs(bsm["p"] - 0.66) < 1e-9


def test_three_independent_voices_can_arbitrate_not_collect():
    # W + M + S = 3 sources distinctes -> ORION sort du COLLECTER (ici accord fort, gel -> ATTENDRE)
    votes, couches = F.orion_votes(_worm("JOUER", rlm=True), None, {"p": 0.8})
    assert sum(couches.values()) == 3
    out = O.arbitrate(votes, frozen=True)
    assert out["n_independantes"] == 3 and out["decision"] != "COLLECTER"


def test_sharp_layer_adds_independent_voice():
    # une proba sharp (Pinnacle dé-viggé) ajoute une voix source "marche_sharp" distincte
    votes, couches = F.orion_votes(_worm("JOUER_PETIT"), None, None, {"p": 0.55})
    assert couches["K"] is True
    srcs = sorted(v["source"] for v in votes)
    assert "marche_sharp" in srcs and "worm_classement" in srcs


def test_sharp_absent_no_layer():
    votes, couches = F.orion_votes(_worm("JOUER_PETIT"), None, None, None)
    assert couches["K"] is False
    assert all(v["source"] != "marche_sharp" for v in votes)


def test_worm_plus_market_plus_sharp_three_independent_voices():
    # W (reco) + M (rlm) + K (sharp) = 3 sources distinctes -> ORION peut arbitrer (hors COLLECTER)
    votes, couches = F.orion_votes(_worm("JOUER", rlm=True), None, None, {"p": 0.8})
    assert sum(1 for k in ("W", "M", "K") if couches[k]) == 3
    out = O.arbitrate(votes, frozen=True)
    assert out["n_independantes"] == 3 and out["decision"] != "COLLECTER"


def test_reco_market_param_mapping():
    def wr(m): return {"reco": {"decision": {"marche": m}}}
    assert F._reco_market_param(wr("Over 2.5")) == "over25"
    assert F._reco_market_param(wr("Under 2.5")) == "under25"
    assert F._reco_market_param(wr("Handicap asiatique -0.5/-1 domicile")) == "home"
    assert F._reco_market_param(wr("Handicap asiatique -0.5/-1 extérieur")) == "away"
    assert F._reco_market_param(wr("Double chance X2 / +0.5 AH extérieur")) == "away"


def test_tier_edge_monotone():
    # un tier plus fort donne une voix structurelle au moins aussi confiante
    p_jouer = F.orion_votes(_worm("JOUER"), None, None)[0][0]["p"]
    p_petit = F.orion_votes(_worm("JOUER_PETIT"), None, None)[0][0]["p"]
    p_nobet = F.orion_votes(_worm("NO BET"), None, None)[0][0]["p"]
    assert p_jouer > p_petit > p_nobet


if __name__ == "__main__":
    import types
    for k, fn in dict(globals()).items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            fn()
    print("apex_fusion tests OK")
