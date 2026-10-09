#!/usr/bin/env python3
"""Tests APEX-SOURCES — dé-vigging, voix sharp honnête, parseur INFERSPORT."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import apex_sources as S  # noqa: E402


def test_devig_removes_overround():
    p = S.devig([1.8, 3.6, 4.5])          # overround présent (somme des inverses > 1)
    assert p is not None and abs(sum(p) - 1.0) < 1e-9
    assert p[0] > p[1] > p[2]              # le favori garde la plus grosse part
    assert S.devig([1.0, 2.0]) is None     # cote invalide -> None
    assert S.devig([]) is None


def test_unconfigured_sources_report_absent_never_invent():
    # sans clé/MCP, chaque adaptateur renvoie un statut honnête, jamais une proba
    for fn in (S.infersports, S.ssb_sharp, S.sharpapi, S.oddsapi_io, S.thestatsapi):
        r = fn("A", "B")
        assert "absent" in r and "p" not in r


def test_pinnacle_devig_absent_for_unknown_division():
    r = S.pinnacle_devig("XX", "A", "B")
    assert r.get("absent")                 # division hors football-data -> absent (pas d'invention)


def test_parse_infersports_probability_and_odds():
    assert S.parse_infersports_sharp({"probability": {"over": 0.57}}, "over25")["p"] == 0.57
    # cote décimale convertie en proba
    r = S.parse_infersports_sharp({"fair": {"home": 2.0}}, "home")
    assert abs(r["p"] - 0.5) < 1e-9 and r["provider"] == "infersports"


def test_parse_infersports_fails_cleanly():
    assert S.parse_infersports_sharp({"status": "ambiguous"}).get("absent")
    assert S.parse_infersports_sharp({"probability": {"home": 0.5}}, "over25").get("absent")
    assert S.parse_infersports_sharp("nope").get("absent")


def test_parse_sharpapi_sharp_ok_and_fail():
    row = {"fair_probability": 0.5561, "ev_percentage": 14, "sharp_book": "pinnacle",
           "warnings": ["STALE_PREMATCH_ODDS"]}
    r = S.parse_sharpapi_sharp(row, "home")
    assert r["p"] == 0.5561 and r["provider"] == "sharpapi" and r["ev_percentage"] == 14
    assert S.parse_sharpapi_sharp({"ev_percentage": 9}).get("absent")      # pas de fair_probability
    assert S.parse_sharpapi_sharp({"fair_probability": 1.4}).get("absent")  # hors bornes
    assert S.parse_sharpapi_sharp("nope").get("absent")


def test_closing_clv_sign_and_guards():
    assert S.closing_clv(2.05, 0.556) > 0          # cote > juste clôture -> battu la clôture
    assert S.closing_clv(1.70, 0.556) < 0          # cote < juste clôture -> sous la clôture
    assert S.closing_clv(0.9, 0.5) is None          # cote invalide
    assert S.closing_clv(2.0, 1.4) is None          # proba hors bornes


def test_closing_cache_roundtrip_and_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "CLOSING", tmp_path)
    S.save_closing_cache("2026-10-10", {"Union Berlin|Elversberg": {"home": 0.556}})
    got = S.sharp_close_for("2026-10-10", "Union Berlin", "Elversberg", "home")
    assert abs(got["fair_prob_close"] - 0.556) < 1e-9
    assert S.sharp_close_for("2026-10-10", "Foo", "Bar", "home").get("absent")   # pas d'invention
    assert S.sharp_close_for("2099-01-01", "X", "Y").get("absent")               # cache vide


def test_registry_lists_sources():
    noms = {s["nom"] for s in S.registry()}
    assert "football-data.co.uk" in noms and "Infersports" in noms


if __name__ == "__main__":
    import types
    for k, fn in dict(globals()).items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            fn()
    print("apex_sources tests OK")
