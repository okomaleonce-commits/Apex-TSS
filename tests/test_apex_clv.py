#!/usr/bin/env python3
"""Tests APEX-CLV — reconstruction cote entrée/clôture, ROI réel et CLV (audit brique n°1)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_clv as C  # noqa: E402


def _pass(fid, phase, status, over, under, tier, market, t, score=None):
    return {"fixture_id": fid, "phase": phase, "status": status, "scan_time_utc": t,
            "home": "A", "away": "B", "score": score,
            "odds": {"Pinnacle": {"OU": {"2.5": [over, under]}}},
            "reco": {"decision": {"tier": tier, "marche": market}}}


def test_ou_odd_picks_correct_side():
    od = {"Pinnacle": {"OU": {"2.5": [1.80, 2.05]}}}
    assert abs(C._ou_odd(od, "Over 2.5") - 1.80) < 1e-9
    assert abs(C._ou_odd(od, "Under 2.5") - 2.05) < 1e-9
    assert C._ou_odd(od, "Handicap asiatique -0.5/-1 domicile") is None   # pas de cote OU → exclu
    assert C._ou_odd({}, "Over 2.5") is None


def test_bets_for_day_roi_and_clv(monkeypatch, tmp_path):
    monkeypatch.setattr(C, "SNAP", tmp_path)
    day = "2026-10-05"
    rows = [
        # entrée : Under 2.5 @2.00 ; clôture : @1.80 (on a pris mieux que la clôture → CLV positif)
        _pass(1, "PREMATCH", "NS", 1.90, 2.00, "JOUER_PETIT", "Under 2.5", "2026-10-05T10:00:00+00:00"),
        _pass(1, "PREMATCH", "NS", 2.10, 1.80, "JOUER_PETIT", "Under 2.5", "2026-10-05T12:00:00+00:00"),
        _pass(1, "FT", "FT", 2.10, 1.80, "JOUER_PETIT", "Under 2.5", "2026-10-05T14:00:00+00:00",
              score={"home": 0, "away": 1}),   # total 1 → Under 2.5 gagné
    ]
    (tmp_path / f"{day}.jsonl").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    bets = C.bets_for_day(day)
    assert len(bets) == 1
    b = bets[0]
    assert abs(b["entry_odd"] - 2.00) < 1e-9 and abs(b["close_odd"] - 1.80) < 1e-9
    assert b["result"] == "gagné"
    assert abs(b["pnl"] - 1.00) < 1e-9                      # (2.00 - 1) à plat 1u
    assert b["clv"] > 0                                     # 2.00/1.80 - 1 ≈ +11 %


def test_summary_excludes_markets_without_stored_odds(monkeypatch, tmp_path):
    monkeypatch.setattr(C, "SNAP", tmp_path)
    day = "2026-10-05"
    rows = [
        # décision AH : aucune cote OU → exclue du ROI (jamais estimée)
        _pass(2, "PREMATCH", "NS", 1.9, 1.9, "JOUER",
              "Handicap asiatique -0.5/-1 domicile", "2026-10-05T10:00:00+00:00"),
        _pass(2, "FT", "FT", 1.9, 1.9, "JOUER",
              "Handicap asiatique -0.5/-1 domicile", "2026-10-05T14:00:00+00:00", score={"home": 3, "away": 0}),
    ]
    (tmp_path / f"{day}.jsonl").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    s = C.summary([day])
    assert s["n_bets_roi"] == 0 and s["n_sans_cote"] == 1   # AH exclu honnêtement


def test_collect_persists_and_dedups(monkeypatch, tmp_path):
    """collect() journalise un pari réglé dans ledger/clv.jsonl (durable) et ne le réécrit jamais."""
    monkeypatch.setattr(C, "SNAP", tmp_path)
    monkeypatch.setattr(C, "CLV_LEDGER", tmp_path / "clv.jsonl")
    day = "2026-10-05"
    rows = [
        _pass(1, "PREMATCH", "NS", 1.90, 2.00, "JOUER_PETIT", "Under 2.5", "2026-10-05T10:00:00+00:00"),
        _pass(1, "PREMATCH", "NS", 2.10, 1.80, "JOUER_PETIT", "Under 2.5", "2026-10-05T12:00:00+00:00"),
        _pass(1, "FT", "FT", 2.10, 1.80, "JOUER_PETIT", "Under 2.5", "2026-10-05T14:00:00+00:00",
              score={"home": 0, "away": 1}),
    ]
    (tmp_path / f"{day}.jsonl").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    assert C.collect([day]) == 1            # 1er passage : persiste
    assert C.collect([day]) == 0            # 2e passage : déjà présent → dédupliqué
    led = C._read_ledger()
    assert len(led) == 1 and led[0]["market"] == "Under 2.5" and led[0]["clv"] > 0


def test_collect_ignores_unsettled(monkeypatch, tmp_path):
    """Un match non terminé (pas de score final) n'est PAS journalisé (immutabilité : on attend le règlement)."""
    monkeypatch.setattr(C, "SNAP", tmp_path)
    monkeypatch.setattr(C, "CLV_LEDGER", tmp_path / "clv.jsonl")
    day = "2026-10-05"
    rows = [_pass(1, "PREMATCH", "NS", 1.90, 2.00, "JOUER_PETIT", "Under 2.5", "2026-10-05T10:00:00+00:00")]
    (tmp_path / f"{day}.jsonl").write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    assert C.collect([day]) == 0
    assert C._read_ledger() == []


def test_summary_reads_durable_ledger(monkeypatch, tmp_path):
    """summary() sans argument lit le journal durable (source='ledger'), pas les snapshots éphémères."""
    monkeypatch.setattr(C, "SNAP", tmp_path / "empty")   # aucun snapshot
    monkeypatch.setattr(C, "CLV_LEDGER", tmp_path / "clv.jsonl")
    (tmp_path / "clv.jsonl").write_text(json.dumps({
        "day": "2026-10-05", "fixture_id": 1, "market": "Under 2.5", "entry_odd": 2.0,
        "close_odd": 1.8, "final": [0, 1], "result": "gagné", "pnl": 1.0, "clv": 0.1111,
        "odds_available": True}) + "\n", encoding="utf-8")
    s = C.summary()
    assert s["source"] == "ledger" and s["clv_n"] == 1 and s["n_bets_roi"] == 1
    assert s["clv_moyen"] > 0


if __name__ == "__main__":
    import tempfile
    import types

    class _MP:
        def setattr(self, o, n, v):
            setattr(o, n, v)
    for k, fn in dict(globals()).items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            import inspect
            p = inspect.signature(fn).parameters
            if "tmp_path" in p:
                with tempfile.TemporaryDirectory() as d:
                    fn(_MP(), Path(d))
            else:
                fn()
    print("apex_clv tests OK")
