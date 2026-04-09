"""
TSS LAYER 3 — SIGNAL ENGINE
============================
Calcul des 4 dimensions du signal :
  Δ   (Delta)      — Écart P_synth vs P_réelle
  IC  (Indice de Convergence) — Cohérence entre modules
  EV  (Expected Value)        — Valeur espérée nette
  SDT (Score de Déclenchement Total) — Score composite

Décision finale : BET / NO BET + niveau de stake.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logger = logging.getLogger("TSS.Layer3")


class SignalDecision(Enum):
    NO_BET        = "NO_BET"
    WEAK          = "WEAK"
    MODERATE      = "MODERATE"
    STRONG        = "STRONG"
    MAXIMAL       = "MAXIMAL"


@dataclass
class SignalMetrics:
    target_market: str
    cote: float
    p_synth: float
    p_real: float

    delta: float = 0.0         # P_synth - P_real
    ic: float    = 0.0         # Indice de convergence [0, 1]
    ev: float    = 0.0         # Expected Value
    sdt: float   = 0.0         # Score de Déclenchement Total

    decision: SignalDecision = SignalDecision.NO_BET
    kelly_fraction: float    = 0.0   # Fraction Kelly recommandée
    kelly_raw: float         = 0.0   # Kelly brut avant fraction
    stake_pct_bankroll: float = 0.0  # Stake recommandé en % bankroll TSS

    p_module_A: Optional[float] = None
    p_module_B: Optional[float] = None
    p_module_C: Optional[float] = None

    gates_failed: list[str]  = field(default_factory=list)
    flags: list[str]         = field(default_factory=list)

    def is_valid(self) -> bool:
        return self.decision != SignalDecision.NO_BET

    def to_dict(self) -> dict:
        return {
            "target_market": self.target_market,
            "cote": self.cote,
            "p_synth": round(self.p_synth, 4),
            "p_real": round(self.p_real, 4),
            "p_module_A": round(self.p_module_A, 4) if self.p_module_A is not None else None,
            "p_module_B": round(self.p_module_B, 4) if self.p_module_B is not None else None,
            "p_module_C": round(self.p_module_C, 4) if self.p_module_C is not None else None,
            "delta": round(self.delta, 4),
            "ic": round(self.ic, 4),
            "ev": round(self.ev, 4),
            "sdt": round(self.sdt, 4),
            "decision": self.decision.value,
            "kelly_raw": round(self.kelly_raw, 4),
            "kelly_fraction": self.kelly_fraction,
            "stake_pct_bankroll": round(self.stake_pct_bankroll, 4),
            "gates_failed": self.gates_failed,
            "flags": self.flags
        }


# ─────────────────────────────────────────────
#  PARAMÈTRES DES GATES
# ─────────────────────────────────────────────

GATE_DELTA_MIN   = 0.08    # Δ minimum absolu
GATE_IC_MIN      = 0.85    # IC minimum
GATE_EV_MIN      = 0.05    # EV minimum (+5%)
GATE_SDT_MIN     = 0.60    # SDT minimum pour déclencher un bet

# Plafonds SDT → décision
SDT_LEVELS = [
    (0.85, SignalDecision.MAXIMAL,  3/4, 0.025),
    (0.70, SignalDecision.STRONG,   1/2, 0.015),
    (0.60, SignalDecision.MODERATE, 1/4, 0.0075),
    (0.50, SignalDecision.WEAK,     1/8, 0.0025),
]

DELTA_MAX_SUSPECT = 0.18   # Au-delà → probable erreur de modèle


class SignalEngine:
    """
    Calcule les métriques de signal à partir des outputs Layer 2 et Layer 1.
    """

    def compute(
        self,
        target_market: str,
        p_synth: float,
        p_A: Optional[float],
        p_B: Optional[float],
        p_C: Optional[float],
        p_real: float,
        cote: float,
        extra_flags: Optional[list[str]] = None
    ) -> SignalMetrics:

        flags = list(extra_flags or [])
        gates_failed = []

        # ── Δ (Delta)
        delta = p_synth - p_real

        # ── IC (Indice de Convergence)
        available_probs = [p for p in [p_A, p_B, p_C] if p is not None and p > 0]
        if len(available_probs) < 2:
            ic = 0.0
            flags.append("IC_INSUFFICIENT_MODULES")
        else:
            mean_p = sum(available_probs) / len(available_probs)
            std_p  = math.sqrt(sum((x - mean_p) ** 2 for x in available_probs) / len(available_probs))
            ic = 1.0 - (std_p / mean_p) if mean_p > 0 else 0.0
            ic = max(min(ic, 1.0), 0.0)

        # ── EV (Expected Value)
        ev = (p_synth * cote) - 1.0

        # ── Vérification des gates
        if delta < GATE_DELTA_MIN:
            gates_failed.append(f"GATE_DELTA(Δ={delta:.3f}<{GATE_DELTA_MIN})")
        if delta > DELTA_MAX_SUSPECT:
            flags.append(f"DELTA_SUSPECT(Δ={delta:.3f}>{DELTA_MAX_SUSPECT})")
        if ic < GATE_IC_MIN:
            gates_failed.append(f"GATE_IC(IC={ic:.3f}<{GATE_IC_MIN})")
        if ev < GATE_EV_MIN:
            gates_failed.append(f"GATE_EV(EV={ev:.3f}<{GATE_EV_MIN})")

        # ── SDT
        delta_norm = min(delta / 0.20, 1.0) if delta > 0 else 0.0
        ev_norm    = min(ev / 0.25, 1.0) if ev > 0 else 0.0
        sdt = (delta_norm * 0.35) + (ic * 0.40) + (ev_norm * 0.25)
        sdt = max(min(sdt, 1.0), 0.0)

        if sdt < GATE_SDT_MIN and not gates_failed:
            gates_failed.append(f"GATE_SDT(SDT={sdt:.3f}<{GATE_SDT_MIN})")

        # ── Décision
        if gates_failed:
            decision = SignalDecision.NO_BET
            kelly_fraction = 0.0
            stake_cap = 0.0
        else:
            decision, kelly_fraction, stake_cap = self._resolve_decision(sdt)

        # ── Kelly
        kelly_raw = self._kelly(p_synth, cote)
        stake_pct = 0.0
        if decision != SignalDecision.NO_BET:
            stake_pct = min(kelly_raw * kelly_fraction, stake_cap)

        metrics = SignalMetrics(
            target_market=target_market,
            cote=cote,
            p_synth=p_synth,
            p_real=p_real,
            p_module_A=p_A,
            p_module_B=p_B,
            p_module_C=p_C,
            delta=delta,
            ic=ic,
            ev=ev,
            sdt=sdt,
            decision=decision,
            kelly_fraction=kelly_fraction,
            kelly_raw=kelly_raw,
            stake_pct_bankroll=stake_pct,
            gates_failed=gates_failed,
            flags=flags
        )

        logger.info(
            f"[SignalEngine] {target_market} | Δ={delta:.3f} IC={ic:.3f} "
            f"EV={ev:.3f} SDT={sdt:.3f} → {decision.value} stake={stake_pct:.4f}"
        )
        return metrics

    @staticmethod
    def _resolve_decision(sdt: float) -> tuple[SignalDecision, float, float]:
        for threshold, decision, fraction, cap in SDT_LEVELS:
            if sdt >= threshold:
                return decision, fraction, cap
        return SignalDecision.NO_BET, 0.0, 0.0

    @staticmethod
    def _kelly(p: float, cote: float) -> float:
        """Kelly brut = (p × (cote-1) - (1-p)) / (cote-1)."""
        b = cote - 1.0
        if b <= 0:
            return 0.0
        k = (p * b - (1.0 - p)) / b
        return max(k, 0.0)


# ─────────────────────────────────────────────
#  DESCRIPTION TEXTUELLE DU SIGNAL
# ─────────────────────────────────────────────

DECISION_STARS = {
    SignalDecision.MAXIMAL:  "★★★★★",
    SignalDecision.STRONG:   "★★★★☆",
    SignalDecision.MODERATE: "★★★☆☆",
    SignalDecision.WEAK:     "★★☆☆☆",
    SignalDecision.NO_BET:   "✗ NO BET"
}


def signal_summary(metrics: SignalMetrics) -> str:
    stars = DECISION_STARS.get(metrics.decision, "")
    lines = [
        f"TARGET     : {metrics.target_market.upper()}",
        f"DÉCISION   : {metrics.decision.value} {stars}",
        f"Cote       : {metrics.cote}",
        f"P_synth    : {metrics.p_synth*100:.1f}%",
        f"P_réelle   : {metrics.p_real*100:.1f}%",
        f"Δ          : {metrics.delta*100:+.1f}%",
        f"IC         : {metrics.ic:.3f}",
        f"EV         : {metrics.ev*100:+.1f}%",
        f"SDT        : {metrics.sdt:.3f}",
    ]
    if metrics.decision != SignalDecision.NO_BET:
        lines += [
            f"Kelly brut : {metrics.kelly_raw*100:.1f}%",
            f"Fraction   : {metrics.kelly_fraction:.3f}",
            f"Stake TSS  : {metrics.stake_pct_bankroll*100:.2f}% bankroll"
        ]
    if metrics.gates_failed:
        lines.append(f"Gates KO   : {', '.join(metrics.gates_failed)}")
    if metrics.flags:
        lines.append(f"Flags      : {', '.join(metrics.flags)}")
    return "\n".join(lines)
