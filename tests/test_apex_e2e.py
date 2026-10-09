#!/usr/bin/env python3
"""Test BOUT-EN-BOUT APEX (audit 2026-10-05, point 5) : chaîne complète
forecast BSM validé → summarize_forecast (SYNC) → risk_decision → commit_decision (transaction
unique, sous GEL) → settle (règlement séparé), avec vérification de reprise depuis le disque.

Ne fait AUCUN réseau ni sous-processus : on part d'un forecast BSM déjà structuré (official_selection
+ preuve de validation), tel que `apex_bsm.py simulate --record` l'écrit au journal.

Le gel de promotion reste actif : la décision est figée comme RECHERCHE, mise effective 0, aucune
exposition engagée — ce test ne démontre donc PAS un pari exécuté en production.
"""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_sync as S   # noqa: E402
import apex_ledger as L  # noqa: E402

DAY = dt.date(2026, 10, 5)
UTC = dt.timezone.utc
BEFORE = dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
KO = "2026-10-05T18:45:00+00:00"


def _forecast_validated():
    """Forecast BSM d'un modèle VALIDÉ, avec sélection officielle structurée et preuve de validation."""
    return {
        "forecast_id": "fc-e2e-1", "home": "Ukraine", "away": "Hungary",
        "statut_mise": "PROPOSÉE", "statut_modele": "VALIDÉ (walk-forward run=bsm-2026)",
        "validation": {"validated": True, "run_id": "bsm-2026", "statut": "VALIDÉ"},
        "decision": "SÉLECTION : Under 2.5 @ 1.66",
        "official_selection": {"marche": "Under 2.5", "p": 0.64, "cote": 1.66,
                               "ev": 0.0624, "sensibilite_min": 0.02384, "veto": False},
    }


def test_e2e_bsm_to_sync_to_ledger_to_settlement(monkeypatch, tmp_path):
    monkeypatch.setattr(L, "STATE", Path(tmp_path))
    assert S.PROMOTION_FROZEN is True        # le gel reste requis

    # 1) SYNC traduit le forecast BSM : sélection conservée EXACTEMENT, EV recalculée, sensibilité gardée
    out = S.summarize_forecast(_forecast_validated())
    assert out["status"] == "selection"
    off = out["official"]
    assert off["marche"] == "Under 2.5" and off["full_settle"] is True
    assert abs(off["sensibilite_min"] - 0.02384) < 1e-9

    # 2) Risk Manager : EV/full_settle TOUJOURS recalculés depuis p & cote finies
    sel = {"match_id": "UKR-HUN", "league": "UEFA Nations", "market": off["marche"],
           "p": off["p"], "odds": off["cote"]}
    risk = S.risk_decision(sel, dict(S.DEFAULT_CONFIG))
    assert risk["stake_pct"] > 0            # un bord existe (mise calculée non nulle)

    # 3) Autorisation finale EXPLICITE sous gel : authorized=False
    authorized = bool(risk["approved"]) and not S.PROMOTION_FROZEN
    assert authorized is False

    # 4) TRANSACTION UNIQUE : fige la décision + réserve l'exposition (effective 0 sous gel)
    committed, rec = L.commit_decision(DAY, sel, KO, {"marche": off["marche"], "p": off["p"],
                                       "cote": off["cote"], "ev": off["ev"]},
                                       risk["stake_pct"], authorized=authorized, now=BEFORE)
    assert committed is True
    assert rec["authorized"] is False and rec["stake_effectif"] == 0.0
    assert rec["stake_calcule"] > 0         # le montant Kelly reste une info de recherche
    assert L.current_exposure(DAY)["total"] == 0.0   # AUCUNE exposition engagée sous gel

    # 5) Règlement APRÈS le match, dans un fichier SÉPARÉ (append-only) : ne touche pas la décision figée
    settled, _ = L.settle(DAY, "UKR-HUN", "Under 2.5", "1-0", "gagné",
                          now=dt.datetime(2026, 10, 5, 21, 0, tzinfo=UTC))
    assert settled is True
    paths = L._paths(DAY)
    assert "gagné" in paths["settlements"].read_text()
    assert "gagné" not in paths["commitments"].read_text()   # décision figée non réécrite

    # 6) REPRISE depuis le disque (nouveau conteneur simulé) : état reconstruit, exposition toujours 0
    assert L.get_committed_decision(DAY, "UKR-HUN")["decision"]["marche"] == "Under 2.5"
    assert L.current_exposure(DAY)["total"] == 0.0
    # jointure décisions ⋈ règlements : le bilan se lit sans réécriture
    dec = L.get_committed_decision(DAY, "UKR-HUN")
    stl = L._read_jsonl(paths["settlements"])[0]
    assert dec["match_id"] == stl["match_id"] and stl["result"] == "gagné"


if __name__ == "__main__":
    import tempfile
    import types
    import pytest  # noqa: F401

    class _MP:
        def setattr(self, obj, name, val):
            setattr(obj, name, val)
    with tempfile.TemporaryDirectory() as d:
        test_e2e_bsm_to_sync_to_ledger_to_settlement(_MP(), Path(d))
    print("e2e OK")
