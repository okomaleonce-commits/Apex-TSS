"""
TSS LAYER 5 — RISK ENGINE
==========================
Gestion de la bankroll TSS (30% bankroll totale).
Kelly fractionné selon SDT.
Détection de concentration (même match APEX + TSS).
Logging de toutes les opérations.
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from enum import Enum

from tss.layer3_signal_engine import SignalDecision, SignalMetrics

logger = logging.getLogger("TSS.Layer5")


# ─────────────────────────────────────────────
#  CONFIGURATION BANKROLL
# ─────────────────────────────────────────────

@dataclass
class BankrollConfig:
    total_bankroll: float       = 1000.0   # Bankroll totale
    tss_allocation_pct: float   = 0.30     # 30% alloué au TSS
    apex_allocation_pct: float  = 0.70     # 70% APEX-ENGINE
    max_single_stake_pct: float = 0.025    # Plafond stake = 2.5% bankroll TSS
    max_exposure_match_pct: float = 0.03   # Exposition max sur un match (toutes bankrolls)

    @property
    def tss_bankroll(self) -> float:
        return self.total_bankroll * self.tss_allocation_pct

    @property
    def apex_bankroll(self) -> float:
        return self.total_bankroll * self.apex_allocation_pct


# ─────────────────────────────────────────────
#  DÉTECTION DIVERGENCE TSS ↔ APEX-ENGINE
# ─────────────────────────────────────────────

class ApexAlignment(Enum):
    ALIGNED    = "ALIGNED"       # Même sens, même marché
    ABSENT     = "ABSENT"        # APEX ne couvre pas ce marché
    DIVERGENT  = "DIVERGENT"     # APEX dit NO BET / sens opposé
    CONTRADICT = "CONTRADICT"    # TSS et APEX opposés sur même marché


@dataclass
class AlignmentResult:
    status: ApexAlignment
    stake_modifier: float = 1.0    # Multiplicateur appliqué au stake TSS
    notes: str = ""


def check_apex_alignment(
    target_market: str,
    tss_decision: SignalDecision,
    apex_signals: Optional[dict] = None
) -> AlignmentResult:
    """
    apex_signals : dict des signaux APEX-ENGINE pour ce match.
    Format : {"btts": "BET_YES", "over25": "NO_BET", ...}
    """
    if apex_signals is None or target_market not in apex_signals:
        return AlignmentResult(
            status=ApexAlignment.ABSENT,
            stake_modifier=1.0,
            notes="APEX absent sur ce marché — TSS opère seul."
        )

    apex_val = apex_signals[target_market]

    if apex_val == "NO_BET" and tss_decision != SignalDecision.NO_BET:
        return AlignmentResult(
            status=ApexAlignment.DIVERGENT,
            stake_modifier=0.5,
            notes="APEX dit NO_BET. Stake TSS réduit de 50%."
        )

    if apex_val == "BET_NO" and tss_decision != SignalDecision.NO_BET:
        return AlignmentResult(
            status=ApexAlignment.CONTRADICT,
            stake_modifier=0.0,
            notes="CONTRADICTION TSS↔APEX. NO BET automatique."
        )

    # Aligné
    return AlignmentResult(
        status=ApexAlignment.ALIGNED,
        stake_modifier=1.0,
        notes="TSS et APEX alignés. Conviction maximale."
    )


# ─────────────────────────────────────────────
#  CALCUL DU STAKE FINAL
# ─────────────────────────────────────────────

@dataclass
class StakeResult:
    match_id: str
    target_market: str
    cote: float
    tss_bankroll: float
    stake_units: float             # En unités absolues
    stake_pct_tss: float           # En % bankroll TSS
    kelly_raw: float
    kelly_fraction: float
    alignment: ApexAlignment
    alignment_modifier: float
    decision: SignalDecision
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["alignment"] = self.alignment.value
        d["decision"] = self.decision.value
        return d


class RiskEngine:
    """
    Calcule le stake final et gère le log des mises TSS.
    """

    def __init__(self, config: Optional[BankrollConfig] = None, log_path: str = "data/tss_stakes.jsonl"):
        self.config = config or BankrollConfig()
        self.log_path = log_path
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

    def compute_stake(
        self,
        match_id: str,
        metrics: SignalMetrics,
        apex_signals: Optional[dict] = None
    ) -> StakeResult:
        """
        Calcule le stake final TSS pour un signal validé.
        """
        notes = []
        tss_br = self.config.tss_bankroll

        # Si NO BET → stake 0
        if metrics.decision == SignalDecision.NO_BET:
            return StakeResult(
                match_id=match_id,
                target_market=metrics.target_market,
                cote=metrics.cote,
                tss_bankroll=tss_br,
                stake_units=0.0,
                stake_pct_tss=0.0,
                kelly_raw=metrics.kelly_raw,
                kelly_fraction=0.0,
                alignment=ApexAlignment.ABSENT,
                alignment_modifier=0.0,
                decision=SignalDecision.NO_BET,
                notes=["Signal NO_BET — stake=0"]
            )

        # Vérifier alignement APEX
        alignment = check_apex_alignment(metrics.target_market, metrics.decision, apex_signals)

        if alignment.status == ApexAlignment.CONTRADICT:
            notes.append(alignment.notes)
            return StakeResult(
                match_id=match_id,
                target_market=metrics.target_market,
                cote=metrics.cote,
                tss_bankroll=tss_br,
                stake_units=0.0,
                stake_pct_tss=0.0,
                kelly_raw=metrics.kelly_raw,
                kelly_fraction=0.0,
                alignment=alignment.status,
                alignment_modifier=0.0,
                decision=SignalDecision.NO_BET,
                notes=[alignment.notes, "CONTRADICT → NO BET forcé"]
            )

        # Stake de base (Kelly fractionné)
        stake_pct_base = metrics.stake_pct_bankroll  # déjà plaffonné dans Layer 3

        # Appliquer modificateur APEX
        stake_pct_adjusted = stake_pct_base * alignment.stake_modifier

        # Plafond global
        stake_pct_final = min(stake_pct_adjusted, self.config.max_single_stake_pct)
        stake_units = stake_pct_final * tss_br

        notes.append(alignment.notes)
        if alignment.stake_modifier < 1.0:
            notes.append(f"Stake réduit par alignement (×{alignment.stake_modifier})")

        result = StakeResult(
            match_id=match_id,
            target_market=metrics.target_market,
            cote=metrics.cote,
            tss_bankroll=tss_br,
            stake_units=round(stake_units, 2),
            stake_pct_tss=round(stake_pct_final, 4),
            kelly_raw=metrics.kelly_raw,
            kelly_fraction=metrics.kelly_fraction,
            alignment=alignment.status,
            alignment_modifier=alignment.stake_modifier,
            decision=metrics.decision,
            notes=notes
        )

        self._log_stake(result)
        logger.info(
            f"[RiskEngine] {match_id} | {metrics.target_market} | "
            f"Stake={stake_units:.2f}u ({stake_pct_final*100:.2f}% TSS bankroll) | "
            f"Alignment={alignment.status.value}"
        )
        return result

    def _log_stake(self, result: StakeResult) -> None:
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(result.to_dict(), ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log stake : {e}")

    def get_stakes_log(self) -> list[dict]:
        if not os.path.exists(self.log_path):
            return []
        results = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return results
