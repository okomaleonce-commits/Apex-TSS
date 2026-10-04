#!/usr/bin/env python3
"""Tests APEX-MI — journal des veilles H-60 (côté pricé + cote juste-implicite) et settle.
Fonctions pures / IO sur tmp uniquement, aucun appel réseau.
Lancer : python3 -m pytest tests/test_apex_mi.py -q
"""
import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_mi as MI  # noqa: E402


def test_watch_market_mapping():
    assert "Double chance 1X" in MI._watch_market("UPSET", "home")
    assert "Double chance X2" in MI._watch_market("UPSET", "away")
    assert "Handicap asiatique -0.5/-1 domicile" == MI._watch_market("BLOWOUT", "home")
    assert "Handicap asiatique -0.5/-1 extérieur" == MI._watch_market("BLOWOUT", "away")


def test_watch_picks_only_live_watches():
    artifact = {"items": [
        {"fixture_id": 1, "match": "A – B", "league": "L", "status": "WATCH",
         "blowout_status": "WATCH"},  # ni l'un ni l'autre → ignoré
        {"fixture_id": 2, "match": "C – D", "league": "L", "status": "LIVE_UPSET_WATCH",
         "dog_side": "home", "dog_prob_now": 0.40, "upset_watch_score": 72},
        {"fixture_id": 3, "match": "E – F", "league": "L", "status": "WATCH",
         "blowout_status": "LIVE_BLOWOUT_WATCH", "fav_side": "away", "fav_prob_now": 0.50,
         "blowout_watch_score": 66},
    ]}
    rows = MI._watch_picks("2026-10-03", artifact)
    assert len(rows) == 2
    up = next(r for r in rows if r[4] == "UPSET")
    bl = next(r for r in rows if r[4] == "BLOWOUT")
    assert up[5] == "home" and "Double chance 1X" in up[6]
    assert up[8] == 2.5          # cote juste-implicite = 1/0.40
    assert bl[5] == "away" and bl[8] == 2.0   # 1/0.50


def test_journal_and_settle(tmp_path, monkeypatch):
    # redirige journal + snapshots vers tmp
    wj = tmp_path / "apex_mi_watch.csv"
    snapdir = tmp_path / "wsnap"
    snapdir.mkdir()
    monkeypatch.setattr(MI, "WATCH_JOURNAL", str(wj))
    monkeypatch.setattr(MI, "WORM_SNAP", str(snapdir))

    artifact = {"items": [
        {"fixture_id": 10, "match": "Fav – Dog", "league": "L", "status": "WATCH",
         "blowout_status": "LIVE_BLOWOUT_WATCH", "fav_side": "home", "fav_prob_now": 0.6,
         "blowout_watch_score": 70},
        {"fixture_id": 11, "match": "Dog2 – Fav2", "league": "L", "status": "LIVE_UPSET_WATCH",
         "dog_side": "home", "dog_prob_now": 0.33, "upset_watch_score": 61},
    ]}
    added = MI._journal_watches("2026-10-03", artifact)
    assert added == 2
    # idempotence : un second passage n'ajoute pas de doublon
    assert MI._journal_watches("2026-10-03", artifact) == 0

    # snapshot final : fixture 10 → favori domicile gagne 2-0 (handicap -0.5/-1 dom = gagné) ;
    # fixture 11 → dog domicile perd 0-1 (double chance 1X = perdu)
    import json
    with open(snapdir / "2026-10-03.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"fixture_id": 10, "phase": "DONE", "score": {"home": 2, "away": 0}}) + "\n")
        fh.write(json.dumps({"fixture_id": 11, "phase": "DONE", "score": {"home": 0, "away": 1}}) + "\n")

    class A:  # args factices
        pass
    MI.cmd_settle(A())

    with open(wj, encoding="utf-8", newline="") as fh:
        rows = {r["fixture_id"]: r for r in csv.DictReader(fh)}
    assert rows["10"]["result"] == "gagné"
    assert rows["10"]["score"] == "2-0"
    assert rows["11"]["result"] == "perdu"


def test_settle_no_journal_is_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(MI, "WATCH_JOURNAL", str(tmp_path / "absent.csv"))

    class A:
        pass
    assert MI.cmd_settle(A()) == 0


if __name__ == "__main__":
    import types
    g = dict(globals())
    for k, v in g.items():
        if k.startswith("test_") and isinstance(v, types.FunctionType):
            import inspect
            params = inspect.signature(v).parameters
            if params:
                print(f"(skip {k} — nécessite pytest fixtures)")
            else:
                v(); print(f"OK {k}")
