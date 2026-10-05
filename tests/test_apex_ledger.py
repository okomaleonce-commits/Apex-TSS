"""APEX-LEDGER — état durable (audit 2026-10-05, étape 2).

Couvre : décisions figées avant KO (idempotentes, refus post-KO), règlements séparés (append-only),
réservations atomiques + idempotentes, plafonds cumulés inter-passage, concurrence RÉELLE
multi-processus (verrou fcntl) et reconstruction après « redémarrage » (relecture disque).
"""
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import apex_ledger as L  # noqa: E402

DAY = dt.date(2026, 10, 5)
UTC = dt.timezone.utc


def _use_tmp(monkeypatch, tmp_path):
    monkeypatch.setattr(L, "STATE", Path(tmp_path))


# ───────── décisions figées avant KO ─────────

def test_freeze_decision_idempotent_and_first_wins(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    now = dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    ko = "2026-10-05T18:45:00+00:00"
    w1, r1 = L.freeze_decision(DAY, "m1", ko, {"market": "Under 2.5", "tier": "CANDIDAT"}, now=now)
    assert w1 is True
    # deuxième tentative (même match) : refusée, la première décision est conservée
    w2, r2 = L.freeze_decision(DAY, "m1", ko, {"market": "Over 2.5", "tier": "CANDIDAT"}, now=now)
    assert w2 is False
    assert L.get_frozen_decision(DAY, "m1")["decision"]["market"] == "Under 2.5"


def test_freeze_decision_refused_after_kickoff(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    now = dt.datetime(2026, 10, 5, 19, 0, tzinfo=UTC)   # après le KO
    ok, payload = L.freeze_decision(DAY, "m2", "2026-10-05T18:45:00+00:00", {"market": "x"}, now=now)
    assert ok is False and "dépassé" in payload["reason"]


# ───────── réservations : idempotence, plafonds, restart ─────────

def test_reserve_idempotent_same_key(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    sel = {"match_id": "m1", "league": "L", "market": "Under 2.5"}
    a_ok, _ = L.reserve_exposure(DAY, sel, 0.01)
    b_ok, _ = L.reserve_exposure(DAY, sel, 0.01)   # même clé → pas de doublon
    assert a_ok is True and b_ok is False
    assert L.current_exposure(DAY)["matchs"]["m1"] == 0.01   # comptée une seule fois


def test_reserve_respects_league_cap(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    caps = {"match": 0.01, "league": 0.03, "market": 0.99, "total_day": 0.99}
    ok_count = 0
    for i in range(6):   # 6 matchs distincts, même ligue, 0.01 chacun ; plafond ligue 0.03
        ok, _ = L.reserve_exposure(DAY, {"match_id": f"m{i}", "league": "L", "market": f"k{i}"}, 0.01, caps)
        ok_count += int(ok)
    assert ok_count == 3                                   # jamais 4 % dans une ligue plafonnée à 3 %
    assert L.current_exposure(DAY)["ligues"]["L"] <= 0.03 + 1e-9


def test_settlement_is_separate_append_only(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    L.freeze_decision(DAY, "m1", "2026-10-05T18:45:00+00:00", {"market": "Under 2.5"},
                      now=dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC))
    L.settle(DAY, "m1", "Under 2.5", "1-0", "gagné")
    paths = L._paths(DAY)
    # la décision figée n'a pas été réécrite ; le règlement est dans un fichier distinct
    assert paths["decisions"].read_text().count("\n") == 1
    assert paths["settlements"].exists() and "gagné" in paths["settlements"].read_text()
    assert "gagné" not in paths["decisions"].read_text()
    # règlement idempotent
    w2, _ = L.settle(DAY, "m1", "Under 2.5", "1-0", "gagné")
    assert w2 is False


def test_restart_rebuilds_exposure_from_disk(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Under 2.5"}, 0.01)
    L.reserve_exposure(DAY, {"match_id": "m2", "league": "L", "market": "Over 2.5"}, 0.005)
    # « redémarrage » : aucun état en mémoire, on relit le journal
    exp = L.current_exposure(DAY)
    assert exp["ligues"]["L"] == 0.015 and exp["total"] == 0.015


# ───────── concurrence RÉELLE multi-processus (verrou fcntl) ─────────

_WORKER = (
    "import os,sys,datetime as dt;"
    "sys.path.insert(0, os.path.join(os.environ['APEX_TOOLS']));"
    "import apex_ledger as L;"
    "day=dt.date(2026,10,5);"
    "sel={'match_id':os.environ['MID'],'league':'L','market':os.environ['MK']};"
    "ok,_=L.reserve_exposure(day, sel, 0.01, {'match':0.01,'league':0.03,'market':0.99,'total_day':0.99});"
    "print('OK' if ok else 'NO')"
)


def _spawn(n, state_dir, mid_fn, mk_fn):
    env = {**os.environ, "APEX_LEDGER_STATE": str(state_dir),
           "APEX_TOOLS": os.path.join(os.path.dirname(__file__), "..", "tools")}
    procs = []
    for i in range(n):
        e = {**env, "MID": mid_fn(i), "MK": mk_fn(i)}
        procs.append(subprocess.Popen([sys.executable, "-c", _WORKER], env=e,
                                      stdout=subprocess.PIPE, text=True))
    return [p.communicate()[0].strip() for p in procs]


def test_concurrent_same_key_idempotent(tmp_path):
    # 8 processus réservent SIMULTANÉMENT la même clé → une seule réservation écrite.
    outs = _spawn(8, tmp_path, lambda i: "mSAME", lambda i: "Under 2.5")
    assert outs.count("OK") == 1 and outs.count("NO") == 7
    os.environ["APEX_LEDGER_STATE"] = str(tmp_path)
    try:
        import importlib
        import apex_ledger as L2
        importlib.reload(L2)
        assert L2.current_exposure(DAY)["matchs"]["mSAME"] == 0.01
    finally:
        os.environ.pop("APEX_LEDGER_STATE", None)


def test_concurrent_distinct_keys_respect_cap(tmp_path):
    # 8 processus, matchs distincts, même ligue, 0.01 chacun, plafond ligue 0.03 → au plus 3 acceptés.
    outs = _spawn(8, tmp_path, lambda i: f"m{i}", lambda i: f"k{i}")
    assert outs.count("OK") == 3    # le verrou inter-processus empêche de dépasser 3 %


def _run_all():
    import inspect
    import tempfile
    import types
    g = dict(globals())
    fns = [v for k, v in g.items() if k.startswith("test_") and isinstance(v, types.FunctionType)]
    passed = 0
    for fn in fns:
        params = inspect.signature(fn).parameters
        if "monkeypatch" in params:
            continue   # nécessite pytest
        try:
            if "tmp_path" in params:
                with tempfile.TemporaryDirectory() as d:
                    fn(Path(d))
            else:
                fn()
            passed += 1
        except Exception as e:  # noqa: BLE001
            print(f"ÉCHEC {fn.__name__}: {e}")
            raise
    print(f"{passed} tests (hors monkeypatch) OK")


if __name__ == "__main__":
    _run_all()
