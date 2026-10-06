#!/usr/bin/env python3
"""Tests ORION SUPERBRAIN — cœur d'arbitrage : META anti-corrélation, désaccord→confiance, gel."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import orion_consensus as O  # noqa: E402


def test_meta_collapses_correlated_sources():
    # 3 agents, 2 partagent la source 'stats' → 2 voix indépendantes, pas 3
    votes = [{"agent": "A", "p": 0.8, "source": "stats"},
             {"agent": "B", "p": 0.8, "source": "stats"},
             {"agent": "C", "p": 0.4, "source": "marche"}]
    out = O.arbitrate(votes)
    assert out["n_votes"] == 3 and out["n_independantes"] == 2
    # consensus = moyenne des 2 voix (0.8 et 0.4) = 0.6, pas (0.8+0.8+0.4)/3
    assert abs(out["consensus"] - 0.6) < 1e-9


def test_high_disagreement_forces_abstention():
    votes = [{"agent": "A", "p": 0.85, "source": "s1"},
             {"agent": "B", "p": 0.80, "source": "s2"},
             {"agent": "C", "p": 0.30, "source": "s3"},
             {"agent": "D", "p": 0.35, "source": "s4"}]
    out = O.arbitrate(votes)
    assert out["desaccord"] >= O.HIGH_DISAGREEMENT
    assert out["decision"] == "ATTENDRE"
    assert out["confiance"] < out["consensus"]          # la confiance est pénalisée par le désaccord


def test_too_few_independent_voices_collects():
    votes = [{"agent": "A", "p": 0.9, "source": "stats"},
             {"agent": "B", "p": 0.9, "source": "stats"}]   # 1 seule voix indépendante
    out = O.arbitrate(votes)
    assert out["decision"] == "COLLECTER"


def test_strong_agreement_but_frozen_never_accepts():
    votes = [{"agent": "A", "p": 0.80, "source": "s1"},
             {"agent": "B", "p": 0.82, "source": "s2"},
             {"agent": "C", "p": 0.79, "source": "s3"},
             {"agent": "D", "p": 0.81, "source": "s4"}]
    out = O.arbitrate(votes, frozen=True)
    assert out["gel_actif"] is True and out["decision"] == "ATTENDRE"
    # sans gel, le même accord fort accepterait
    out2 = O.arbitrate(votes, frozen=False)
    assert out2["decision"] == "ACCEPTER"


def test_strong_agreement_low_side_rejects():
    votes = [{"agent": "A", "p": 0.20, "source": "s1"},
             {"agent": "B", "p": 0.22, "source": "s2"},
             {"agent": "C", "p": 0.19, "source": "s3"}]
    out = O.arbitrate(votes, frozen=False)
    assert out["decision"] == "REJETER"


def test_failure_memory_bias_applied():
    votes = [{"agent": "A", "p": 0.70, "source": "s1"},
             {"agent": "B", "p": 0.70, "source": "s2"},
             {"agent": "C", "p": 0.70, "source": "s3"}]
    base = O.arbitrate(votes, frozen=False)["consensus"]
    corr = O.arbitrate(votes, frozen=False, failure_bias=-0.11)["consensus"]
    assert abs(base - 0.70) < 1e-9 and abs(corr - 0.59) < 1e-9   # -11 % appliqué


if __name__ == "__main__":
    import types
    for k, fn in dict(globals()).items():
        if k.startswith("test_") and isinstance(fn, types.FunctionType):
            fn()
    print("orion_consensus tests OK")
