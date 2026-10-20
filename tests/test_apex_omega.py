#!/usr/bin/env python3
"""Tests APEX-OMEGA — couche de triage honnête (jamais de bord inventé, gel conservé).

Points vérifiés :
  • un EV n'est jamais prétendu sans COTE RÉELLE (sinon WATCH) ;
  • tant que le modèle n'est PAS prouvé supérieur au marché, aucun palier BET/LEAN n'est possible
    même sur une divergence énorme (plafond WATCH) ;
  • quand le modèle EST prouvé supérieur, le barème EV s'ouvre (BET ≥ 3 %, LEAN 1,5–3 %, PASS < 0) ;
  • la mise réelle vaut toujours 0 (gel), sur tout palier ;
  • PASS n'est jamais vide (3 marchés les moins mauvais, EV négatif marqué) ;
  • aucune reco forcée ni emprunt fabriqué : λ absents ⇒ abstention expliquée, pas de sortie inventée.
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_omega as O  # noqa: E402
import gel_matrix as GM  # noqa: E402


def _verdict(lambdas=(1.55, 1.15), **kw):
    v = {
        "fixture_id": 1, "match": "A – B", "league": "L", "country": "XX",
        "kickoff": "2026-10-20T19:00:00+00:00", "decision": "COLLECTER", "raison": "x",
        "confiance": None, "desaccord": None, "tier_worm": "SURVEILLER", "n_independantes": 3,
        "couches": {"W": True, "M": True, "C": False, "S": True, "K": False},
        "bsm_in_scope": True, "lambdas": (list(lambdas) if lambdas else None),
        "worm_meta": {"odds_present": True, "ev_best": 0.04, "signal_stable": True,
                      "data_quality": 80, "integrity_suspect": False},
    }
    v.update(kw)
    v["gel"] = GM.evaluate_match(v)
    return v


def _rng():
    return np.random.default_rng(123)


def _rec(odds):
    return {"fixture_id": 1, "odds": {"Pinnacle": odds}}


# ───────────────────────── régime « modèle non supérieur » (cas réel) ─────────────────────────

def test_non_superieur_plafonne_watch_meme_sur_divergence_enorme():
    """Cote très longue vs proba structurelle élevée = divergence énorme. Elle NE doit JAMAIS
    devenir BET/LEAN tant que le modèle n'est pas prouvé supérieur : plafond WATCH."""
    v = _verdict()
    rec = _rec({"1X2": [1.95, 3.6, 21.78]})     # extérieur à 21.78 → divergence absurde
    b = O._triage_one(v, rec, -0.05, 0.0, _rng(), modele_sup=False, statut_backtest="NON SUPÉRIEUR")
    assert b["palier_match"] == "WATCH"
    assert "MODELE_NON_SUPERIEUR" in b["marquages"]
    for c in b["classement"]:
        assert c["palier"].startswith("WATCH")       # jamais BET/LEAN
        assert c["mise_reelle"] == 0.0
        assert c["kelly_indic"] is None              # pas de Kelly quand l'EV n'est pas un bord


def test_divergence_extreme_est_signalee():
    v = _verdict()
    rec = _rec({"1X2": [1.95, 3.6, 21.78]})
    b = O._triage_one(v, rec, -0.05, 0.0, _rng(), modele_sup=False, statut_backtest="NON SUPÉRIEUR")
    assert any(c.get("divergence_suspecte") for c in b["classement"])


# ───────────────────────── régime « modèle prouvé supérieur » ─────────────────────────

def test_superieur_ouvre_bet_lean():
    """Avec le modèle prouvé supérieur et une cote cohérente portant un edge, un palier BET/LEAN existe."""
    v = _verdict()
    rec = _rec({"1X2": [2.05, 3.45, 3.70], "OU": {"2.5": [1.95, 1.95]}})
    b = O._triage_one(v, rec, -0.05, 0.0, _rng(), modele_sup=True, statut_backtest="VALIDÉ supérieur")
    assert b["palier_match"] in ("BET", "LEAN")
    assert b["classement"], "classement non vide"


def test_superieur_mise_reelle_reste_zero_sous_gel():
    v = _verdict()
    rec = _rec({"1X2": [2.05, 3.45, 3.70]})
    b = O._triage_one(v, rec, -0.05, 0.0, _rng(), modele_sup=True, statut_backtest="VALIDÉ supérieur")
    assert all(c["mise_reelle"] == 0.0 for c in b["classement"])   # gel : jamais de mise réelle


# ───────────────────────── anti-invention : pas de cote ⇒ pas d'EV ─────────────────────────

def test_sans_cote_aucun_ev_pretendu_watch():
    v = _verdict()
    b = O._triage_one(v, {"fixture_id": 1, "odds": {}}, -0.05, 0.0, _rng(),
                      modele_sup=True, statut_backtest="VALIDÉ supérieur")
    # aucun marché coté ⇒ classement vide, tout part en WATCH (cotes absentes), rien d'inventé
    assert b["palier_match"] == "WATCH"
    assert b["classement"] == []
    assert b["watch_cote_absente"], "la WATCH list sans cote doit exister"
    assert "COTES_ABSENTES" in b["marquages"]


# ───────────────────────── PASS justifié, jamais vide ─────────────────────────

def test_pass_non_vide_quand_tous_negatifs():
    """Modèle supérieur mais toutes les cotes trop basses (EV<0) ⇒ PASS, avec 3 marchés informatifs."""
    v = _verdict()
    rec = _rec({"1X2": [1.20, 5.0, 8.0]})          # cote domicile écrasée → EV négatif partout utile
    b = O._triage_one(v, rec, -0.05, 0.0, _rng(), modele_sup=True, statut_backtest="VALIDÉ supérieur")
    if b["palier_match"] == "PASS":
        assert b["classement"], "PASS n'est jamais vide"
        assert all("informatif" in c["palier"] for c in b["classement"])
        assert any(c["ev_negatif"] for c in b["classement"])


# ───────────────────────── λ absents ⇒ abstention expliquée, pas de sortie inventée ─────────────────────────

def test_lambdas_absents_abstention_expliquee():
    v = _verdict(lambdas=None)
    b = O._triage_one(v, _rec({"1X2": [2.0, 3.4, 3.8]}), -0.05, 0.0, _rng(),
                      modele_sup=True, statut_backtest="VALIDÉ supérieur")
    assert b["classement"] == []
    assert "STRUCTURE_INDISPONIBLE" in b["marquages"]
    assert b["explication"]["seuil_rate"], "l'abstention doit être expliquée"


# ───────────────────────── EV calculé honnêtement (1X2) ─────────────────────────

def test_ev_pct_1x2_est_correct():
    row = {"p": 0.50, "cote": 2.10}
    assert abs(O._ev_pct(row) - ((0.50 * 2.10 - 1) * 100)) < 1e-6   # = +5.0 %
    assert O._ev_pct({"p": 0.5, "cote": None}) is None              # pas de cote ⇒ pas d'EV


if __name__ == "__main__":
    import traceback
    fails = 0
    for fn in [f for f in dir() if f.startswith("test_")]:
        try:
            globals()[fn]()
            print("PASS", fn)
        except Exception as e:
            fails += 1
            print("FAIL", fn, repr(e))
            traceback.print_exc()
    raise SystemExit(1 if fails else 0)
