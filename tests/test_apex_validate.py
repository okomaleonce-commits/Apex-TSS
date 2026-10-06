#!/usr/bin/env python3
"""Tests APEX-VALIDATE — agrégation multi-jours, Wilson, et verdict NON-levable (audit étape 4)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_validate as V  # noqa: E402


def test_wilson_bounds_order_and_range():
    lo, mid, hi = V.wilson(66, 100)
    assert 0.0 < lo < mid < hi < 1.0
    assert abs(mid - 0.66) < 1e-9
    # n=0 → tout à zéro, pas de division par zéro
    assert V.wilson(0, 0) == (0.0, 0.0, 0.0)


def _write_bilan(d, jour, par_marche):
    (d / f"{jour}.json").write_text(json.dumps({"jour": jour, "par_marche": par_marche,
                                                "par_signal": {}, "par_palier": {}}), encoding="utf-8")


def test_evaluate_accumulates_and_flags_breakeven(monkeypatch, tmp_path):
    monkeypatch.setattr(V, "BILANS", tmp_path)
    # 2 jours, Under 2.5 : 66/100 cumulés à cote réf 1,55 (break-even 64,5%) → borne basse < seuil
    _write_bilan(tmp_path, "2026-10-01", {"Under 2.5": {"gagné": 33, "demi": 0, "perdu": 17}})
    _write_bilan(tmp_path, "2026-10-02", {"Under 2.5": {"gagné": 33, "demi": 0, "perdu": 17}})
    ev = V.evaluate("par_marche")
    row = next(r for r in ev["rows"] if r["nom"] == "Under 2.5")
    assert row["n"] == 100 and abs(row["taux"] - 0.66) < 1e-9
    assert row["breakeven"] == round(1 / 1.55, 3)
    assert row["ic_bas"] < row["breakeven"]            # 66% à cote 1,55 : bord NON démontré
    assert row["verdict"] in ("insuffisant", "sous le seuil")


def test_verdict_global_never_lifts_freeze(monkeypatch, tmp_path):
    monkeypatch.setattr(V, "BILANS", tmp_path)
    # Marché très au-dessus du seuil, gros N → « bord conditionnel » mais gel JAMAIS levable
    _write_bilan(tmp_path, "2026-10-01", {"Handicap asiatique -0.5/-1": {"gagné": 80, "demi": 0, "perdu": 20}})
    v = V.verdict_global()
    assert v["bord_conditionnel"] is True
    assert v["gel_levable"] is False                   # invariant de sécurité : jamais levable ici


if __name__ == "__main__":
    import tempfile
    import types

    class _MP:
        def setattr(self, o, n, val):
            setattr(o, n, val)
    g = dict(globals())
    for k, fn in g.items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            import inspect
            p = inspect.signature(fn).parameters
            if "tmp_path" in p:
                with tempfile.TemporaryDirectory() as d:
                    fn(_MP(), Path(d)) if "monkeypatch" in p else fn(Path(d))
            else:
                fn()
    print("apex_validate tests OK")
