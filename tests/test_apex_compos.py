#!/usr/bin/env python3
"""Tests APEX-COMPOS — logique pure : implication, absents, malus borné, scénario (sans réseau)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_compos as K  # noqa: E402


def test_involvement_shares_sum_to_one():
    players = [{"name": "Buteur", "goals": 10, "assists": 5},      # 15
               {"name": "Milieu", "goals": 2, "assists": 3},       # 5
               {"name": "Defenseur", "goals": 0, "assists": 0}]    # 0
    shares, total = K.involvement(players)
    assert abs(total - 20.0) < 1e-9
    assert abs(shares["buteur"] - 0.75) < 1e-9 and abs(shares["milieu"] - 0.25) < 1e-9
    assert shares["defenseur"] == 0.0


def test_absent_confirmed_vs_questionable():
    bl = [{"joueur": "Star", "type": "Missing Fixture"}, {"joueur": "Doute", "type": "Questionable"}]
    # compo NON publiée : douteux à moitié
    a = {x["name"]: x for x in K.absent_players(bl, None)}
    assert a["Star"]["weight"] == 1.0 and a["Doute"]["weight"] == K.QUESTIONABLE_WEIGHT


def test_absent_refined_by_published_lineup():
    bl = [{"joueur": "Doute", "type": "Questionable"}, {"joueur": "Star", "type": "Missing Fixture"}]
    # compo publiée : « Doute » est dans le XI → il joue (retiré) ; « Star » absent du XI → forfait
    out = {x["name"]: x for x in K.absent_players(bl, ["Doute", "Autre1", "Autre2"])}
    assert "Doute" not in out
    assert out["Star"]["weight"] == 1.0


def test_team_malus_bounded_and_sourced():
    shares = {"star": 0.40, "milieu": 0.10}
    absents = [{"name": "Star", "weight": 1.0, "statut": "forfait"}]
    m = K.team_malus(absents, shares, k=0.6)
    # part absente = 0.40 → mult = 1 - 0.6*0.40 = 0.76 → planché à 0.80
    assert m["part_absente"] == 0.4 and m["mult"] == 0.80
    # absent sans part sourcée : signalé, non chiffré
    m2 = K.team_malus([{"name": "Inconnu", "weight": 1.0, "statut": "forfait"}], shares)
    assert m2["part_absente"] == 0.0 and m2["mult"] == 1.0 and m2["importance_non_sourcee"] == ["Inconnu"]


def test_absent_dedups_keeping_worst_status():
    """L'API injuries peut répéter un joueur : on n'en garde qu'un, au statut le plus sévère."""
    bl = [{"joueur": "Star", "type": "Questionable"}, {"joueur": "Star", "type": "Missing Fixture"},
          {"joueur": "Star", "type": "Questionable"}]
    out = K.absent_players(bl, None)
    assert len(out) == 1 and out[0]["weight"] == 1.0          # dédupliqué, forfait retenu


def test_malus_no_absence_is_neutral():
    m = K.team_malus([], {"star": 0.4})
    assert m["mult"] == 1.0 and m["part_absente"] == 0.0


def test_scenario_string_format():
    assert K.scenario_string(0.88, 1.0) == "1.0:0.880:1.000"


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
