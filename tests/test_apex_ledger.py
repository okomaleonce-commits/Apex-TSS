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


def test_freeze_decision_requires_valid_kickoff(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    now = dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    for bad in (None, "", "pas-une-date"):
        ok, payload = L.freeze_decision(DAY, "mX", bad, {"market": "x"}, now=now)
        assert ok is False and "coup d'envoi" in payload["reason"]


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


def test_reserve_rejects_invalid_amounts(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    for bad in (float("nan"), float("inf"), -0.02, 0.0, "x", None):
        ok, payload = L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Under 2.5"}, bad)
        assert ok is False and "invalide" in payload["reason"] or "non numérique" in payload["reason"]
    # un montant négatif refusé ne doit PAS permettre de dépasser ensuite le plafond ligue
    assert L.current_exposure(DAY)["total"] == 0.0


def test_reserve_one_bet_per_match(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    a, _ = L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Under 2.5"}, 0.005)
    b, payload = L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Over 2.5"}, 0.005)
    assert a is True and b is False
    assert "un pari par match" in payload["reason"]
    assert L.current_exposure(DAY)["matchs"]["m1"] == 0.005   # un seul pari engagé


def test_corrupt_journal_fails_closed(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Under 2.5"}, 0.01)
    # on corrompt la fin du journal (ligne JSON tronquée)
    p = L._paths(DAY)["reservations"]
    with open(p, "a", encoding="utf-8") as fh:
        fh.write('{"reservation_id": "zz", "stake_pct": 0.0')   # ligne incomplète
    ok, payload = L.reserve_exposure(DAY, {"match_id": "m2", "league": "L", "market": "Over 2.5"}, 0.01)
    assert ok is False and "illisible" in payload["reason"]     # fail-closed, pas « succès »
    import pytest
    with pytest.raises(L.LedgerError):
        L.current_exposure(DAY)                                 # ne renvoie jamais 0 en silence


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


# ───────── D5 : fin de journal sans saut de ligne (objet COMPLET) ─────────

def test_d5_complete_last_object_without_newline(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    p = L._paths(DAY)["reservations"]
    p.parent.mkdir(parents=True, exist_ok=True)
    # dernier objet COMPLET mais sans « \n » final (écriture interrompue juste avant le saut de ligne)
    p.write_text('{"reservation_id":"aaa","match_id":"m0","league":"L","market":"Under 2.5","stake_pct":0.01}')
    ok, _ = L.reserve_exposure(DAY, {"match_id": "m1", "league": "L", "market": "Over 2.5"}, 0.01)
    assert ok is True
    raw = p.read_text()
    assert "}{" not in raw                                   # pas de concaténation
    assert L.current_exposure(DAY)["total"] == 0.02          # relecture lisible, pas d'illisible


# ───────── point 3 : transaction unique commit_decision ─────────

KO_FUT = "2026-10-05T18:45:00+00:00"
BEFORE = dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def _sel(mid="m1", league="L", market="Under 2.5"):
    return {"match_id": mid, "league": league, "market": market}


def test_commit_frozen_under_gel_reserves_zero(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    # sous gel : authorized=False → décision figée, mais mise effective 0 (recherche seule)
    ok, rec = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "Under 2.5"}, 0.01,
                                authorized=False, now=BEFORE)
    assert ok is True
    assert rec["authorized"] is False and rec["stake_effectif"] == 0.0 and rec["stake_calcule"] == 0.01
    assert L.current_exposure(DAY)["total"] == 0.0           # AUCUNE exposition engagée
    assert L.get_committed_decision(DAY, "m1")["decision"]["marche"] == "Under 2.5"


def test_commit_authorized_reserves_exposure(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    ok, rec = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "Under 2.5"}, 0.01,
                                authorized=True, now=BEFORE)
    assert ok is True and rec["stake_effectif"] == 0.01
    assert L.current_exposure(DAY)["matchs"]["m1"] == 0.01


def test_commit_refused_after_kickoff_atomic(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    after = dt.datetime(2026, 10, 5, 19, 0, tzinfo=UTC)
    ok, payload = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "x"}, 0.01,
                                    authorized=True, now=after)
    assert ok is False and "dépassé" in payload["reason"]
    # atomicité : RIEN n'est écrit (ni décision figée ni exposition)
    assert L.get_committed_decision(DAY, "m1") is None
    assert L.current_exposure(DAY)["total"] == 0.0


def test_commit_requires_valid_kickoff(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    for bad in (None, "", "pas-une-date"):
        ok, payload = L.commit_decision(DAY, _sel(), bad, {"marche": "x"}, 0.01,
                                        authorized=False, now=BEFORE)
        assert ok is False and "coup d'envoi" in payload["reason"]
    assert L.get_committed_decision(DAY, "m1") is None


def test_commit_one_bet_per_match(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    # Un second marché sur le MÊME match ne crée jamais un second engagement : la clé de commit est
    # par match, donc la 2e tentative est idempotente (1re gagne) → une seule exposition engagée.
    a, _ = L.commit_decision(DAY, _sel(market="Under 2.5"), KO_FUT, {"marche": "Under 2.5"}, 0.01,
                             authorized=True, now=BEFORE)
    b, payload = L.commit_decision(DAY, _sel(market="Over 2.5"), KO_FUT, {"marche": "Over 2.5"}, 0.01,
                                   authorized=True, now=BEFORE)
    assert a is True and b is False
    assert payload["decision"]["marche"] == "Under 2.5"        # la 1re décision est conservée
    assert L.current_exposure(DAY)["matchs"]["m1"] == 0.01     # un seul pari engagé sur le match


def test_commit_idempotent_per_match(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    a, _ = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "Under 2.5"}, 0.01,
                             authorized=False, now=BEFORE)
    b, _ = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "Over 2.5"}, 0.01,
                             authorized=False, now=BEFORE)      # 2e tentative même match
    assert a is True and b is False
    assert L.get_committed_decision(DAY, "m1")["decision"]["marche"] == "Under 2.5"  # 1re gagne


def test_commit_rejects_invalid_stake(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    for bad in (float("nan"), float("inf"), -0.01, "x"):
        ok, payload = L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "x"}, bad,
                                        authorized=True, now=BEFORE)
        assert ok is False and ("invalide" in payload["reason"] or "non numérique" in payload["reason"])
    assert L.current_exposure(DAY)["total"] == 0.0


def test_commit_respects_day_cap(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    caps = {"match": 0.01, "league": 0.99, "market": 0.99, "total_day": 0.02}
    ok_count = 0
    for i in range(5):
        ok, _ = L.commit_decision(DAY, _sel(mid=f"m{i}", market=f"k{i}"), KO_FUT,
                                  {"marche": "x"}, 0.01, authorized=True, caps=caps, now=BEFORE)
        ok_count += int(ok)
    assert ok_count == 2                                   # plafond jour 2 % → au plus 2 paris à 1 %
    assert L.current_exposure(DAY)["total"] <= 0.02 + 1e-9


def test_commit_restart_rebuilds_from_disk(monkeypatch, tmp_path):
    _use_tmp(monkeypatch, tmp_path)
    L.commit_decision(DAY, _sel(mid="m1", market="Under 2.5"), KO_FUT, {"marche": "Under 2.5"}, 0.01,
                      authorized=True, now=BEFORE)
    L.commit_decision(DAY, _sel(mid="m2", market="Over 2.5"), KO_FUT, {"marche": "Over 2.5"}, 0.005,
                      authorized=True, now=BEFORE)
    # « redémarrage » : relecture pure du disque
    exp = L.current_exposure(DAY)
    assert exp["total"] == 0.015 and exp["matchs"]["m1"] == 0.01 and exp["matchs"]["m2"] == 0.005


def test_commit_storage_unavailable_fails_closed(monkeypatch, tmp_path):
    import pytest
    # STATE pointe « dans » un fichier → mkdir impossible → LedgerError (pas d'OSError nue, pas de succès)
    f = tmp_path / "not_a_dir"
    f.write_text("x")
    monkeypatch.setattr(L, "STATE", f / "ledger")
    with pytest.raises(L.LedgerError):
        L.commit_decision(DAY, _sel(), KO_FUT, {"marche": "x"}, 0.01, authorized=False, now=BEFORE)


# ───────── concurrence RÉELLE sur la transaction commit (verrou fcntl) ─────────

_WORKER_COMMIT = (
    "import os,sys,datetime as dt;"
    "sys.path.insert(0, os.path.join(os.environ['APEX_TOOLS']));"
    "import apex_ledger as L;"
    "day=dt.date(2026,10,5);"
    "sel={'match_id':os.environ['MID'],'league':'L','market':os.environ['MK']};"
    "ko='2026-10-05T18:45:00+00:00';"
    "now=dt.datetime(2026,10,5,12,0,tzinfo=dt.timezone.utc);"
    "ok,_=L.commit_decision(day, sel, ko, {'marche':'x'}, 0.01, authorized=True,"
    " caps={'match':0.01,'league':0.99,'market':0.99,'total_day':0.03}, now=now);"
    "print('OK' if ok else 'NO')"
)


def _spawn_commit(n, state_dir, mid_fn, mk_fn):
    env = {**os.environ, "APEX_LEDGER_STATE": str(state_dir),
           "APEX_TOOLS": os.path.join(os.path.dirname(__file__), "..", "tools")}
    procs = []
    for i in range(n):
        e = {**env, "MID": mid_fn(i), "MK": mk_fn(i)}
        procs.append(subprocess.Popen([sys.executable, "-c", _WORKER_COMMIT], env=e,
                                      stdout=subprocess.PIPE, text=True))
    return [p.communicate()[0].strip() for p in procs]


def test_commit_concurrent_same_match_one_wins(tmp_path):
    outs = _spawn_commit(8, tmp_path, lambda i: "mSAME", lambda i: "Under 2.5")
    assert outs.count("OK") == 1 and outs.count("NO") == 7


def test_commit_concurrent_distinct_respect_day_cap(tmp_path):
    # 8 processus, matchs distincts, 0.01 chacun, plafond jour 0.03 → au plus 3 autorisés
    outs = _spawn_commit(8, tmp_path, lambda i: f"m{i}", lambda i: f"k{i}")
    assert outs.count("OK") == 3


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
