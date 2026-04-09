"""
TSS — TRIANGULATION SIGNAL SYSTEM
Orchestrateur Principal v1.0
================================
Pipeline complet : Layer 0 → Layer 6
Système de signal autonome, indépendant d'APEX-ENGINE.
"""

import logging
import os
from typing import Optional

from tss.layer0_data_intake import (
    DataIntake, MatchSnapshot, build_snapshot, OddsSource
)
from tss.layer1_demarginalisation import DemarginalisationEngine
from tss.layer2_triangulation import TriangulationCore
from tss.layer3_signal_engine import SignalEngine, SignalMetrics, SignalDecision
from tss.layer4_calibration import CalibrationLayer, CalibrationContext
from tss.layer5_risk_engine import RiskEngine, BankrollConfig
from tss.layer6_output import OutputLayer, TSSSignal

# ─────────────────────────────────────────────
#  LOGGING CONFIGURATION
# ─────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/tss.log", encoding="utf-8")
        ]
    )

logger = logging.getLogger("TSS.Orchestrator")


# ─────────────────────────────────────────────
#  ORCHESTRATEUR
# ─────────────────────────────────────────────

class TSS:
    """
    Triangulation Signal System — Point d'entrée unique.

    Utilisation typique :
        tss = TSS(bankroll_config=BankrollConfig(total_bankroll=500))
        signal = tss.analyze_match(
            home="Napoli", away="Lazio", league="serie_a",
            kickoff="2026-04-09T20:45:00",
            odds_dict={...},
            target_markets=["btts", "over25"]
        )
    """

    SUPPORTED_MARKETS = ["btts", "over25", "over15"]

    def __init__(
        self,
        bankroll_config: Optional[BankrollConfig] = None,
        telegram_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
        log_level: str = "INFO"
    ):
        setup_logging(log_level)

        self.intake          = DataIntake()
        self.demarg          = DemarginalisationEngine(method="shin")
        self.triangulation   = TriangulationCore()
        self.signal_engine   = SignalEngine()
        self.calibration     = CalibrationLayer()
        self.risk_engine     = RiskEngine(
            config=bankroll_config or BankrollConfig(),
            log_path="data/tss_stakes.jsonl"
        )
        self.output          = OutputLayer(
            store_path="data/tss_signals.jsonl",
            telegram_token=telegram_token,
            telegram_chat_id=telegram_chat_id
        )
        logger.info("TSS v1.0 initialisé.")

    def analyze_match(
        self,
        home: str,
        away: str,
        league: str,
        kickoff: str,
        odds_dict: dict,
        target_markets: Optional[list[str]] = None,
        match_id: Optional[str] = None,
        matchday: Optional[int] = None,
        is_midweek: bool = False,
        book_exact_scores: Optional[dict] = None,
        apex_signals: Optional[dict] = None,
        snapshot_label: str = "H-3"
    ) -> list[TSSSignal]:
        """
        Analyse complète d'un match.

        Paramètres :
        ─────────────
        odds_dict : {
            "1x2":         {"home": 2.10, "draw": 3.40, "away": 3.60},
            "over25":      {"over": 1.85, "under": 2.05},
            "over15":      {"over": 1.40, "under": 3.10},
            "btts":        {"yes": 1.90, "no": 2.00},
            "home_over05": {"over": 1.55, "under": 2.60},
            "away_over05": {"over": 2.10, "under": 1.80},
            "ah":          {"home_line": -0.5, "home_odds": 2.08, "away_odds": 1.85}
        }
        target_markets : liste parmi ["btts", "over25", "over15"]
        apex_signals   : signaux APEX-ENGINE pour détection alignement
        """

        targets = target_markets or self.SUPPORTED_MARKETS
        mid = match_id or f"{home.replace(' ','_')}_vs_{away.replace(' ','_')}"
        signals_emitted = []

        logger.info(f"\n{'═'*50}")
        logger.info(f"TSS ANALYSE | {home} vs {away} | {league} | {kickoff}")
        logger.info(f"{'═'*50}")

        # ── LAYER 0 : Snapshot
        snap = build_snapshot(
            match_id=mid, home=home, away=away, league=league,
            kickoff=kickoff, label=snapshot_label,
            odds_dict=odds_dict, source=OddsSource.MANUAL
        )
        self.intake.register_snapshot(snap)

        # ── Vérification mouvement de ligne
        line_moved = False
        line_move_pct = 0.0
        movement_report = self.intake.check_line_movement(mid)
        if movement_report and movement_report.get("no_bet_recommended"):
            logger.warning(f"[TSS] Mouvement de ligne critique détecté. Analyse suspendue.")
            line_moved = True
            line_move_pct = max(
                a["move_pct"] / 100 for a in movement_report.get("alerts", [{"move_pct": 0}])
            )

        # ── LAYER 1 : Démarginalisation
        probs = self.demarg.run_all_markets(snap)

        # Injecter la ligne AH dans les probs pour usage dans les modules
        if "ah" in odds_dict:
            probs["ah"] = odds_dict["ah"]   # conservé brut pour line extraction

        # ── LAYER 2 : Triangulation
        tri_results = self.triangulation.run(probs, book_exact_scores)

        # Extraire AH line pour calibration
        ah_line = None
        if "ah" in odds_dict:
            ah_line = odds_dict["ah"].get("home_line")

        # ── LAYER 3-4-5-6 : Signal → Calibration → Risk → Output
        for target in targets:
            if target not in tri_results:
                logger.warning(f"[TSS] Marché cible '{target}' non disponible dans la triangulation.")
                continue

            tri = tri_results[target]
            if tri.p_synth == 0.0:
                logger.warning(f"[TSS] P_synth=0 pour '{target}' — skipped.")
                continue

            # Cote réelle demarginalisée du marché cible
            p_real = self._get_p_real(probs, target)
            if p_real is None:
                logger.warning(f"[TSS] P_réelle non disponible pour '{target}' — skipped.")
                continue

            cote_book = self._get_cote(odds_dict, target)

            # LAYER 4 : Calibration
            ctx = CalibrationContext(
                league=league,
                matchday=matchday,
                ah_line=ah_line,
                is_midweek=is_midweek,
                target_odds=cote_book,
                line_moved=line_moved,
                line_move_pct=line_move_pct
            )
            p_synth_cal, gate_overrides, cal_flags = self.calibration.apply(
                target, tri.p_synth, ctx
            )

            # LAYER 3 : Signal Engine
            metrics = self.signal_engine.compute(
                target_market=target,
                p_synth=p_synth_cal,
                p_A=tri.p_module_A,
                p_B=tri.p_module_B,
                p_C=tri.p_module_C,
                p_real=p_real,
                cote=cote_book,
                extra_flags=tri.flags + cal_flags
            )

            # Appliquer gate_overrides (Layer 4 peut forcer NO_BET)
            if gate_overrides.get("FORCE_NO_BET"):
                metrics.decision = SignalDecision.NO_BET
                metrics.gates_failed.append("LINE_MOVE_CRITICAL → FORCED_NO_BET")

            # LAYER 5 : Risk Engine
            stake = self.risk_engine.compute_stake(mid, metrics, apex_signals)

            # LAYER 6 : Output
            signal = self.output.emit(
                home=home, away=away, league=league, kickoff=kickoff,
                metrics=metrics, stake=stake, match_id=mid
            )
            signals_emitted.append(signal)

        return signals_emitted

    @staticmethod
    def _get_p_real(probs: dict, target: str) -> Optional[float]:
        """Extrait P_réelle demarginalisée pour le marché cible."""
        mapping = {
            "btts":   ("btts", "yes"),
            "over25": ("over25", "over"),
            "over15": ("over15", "over"),
        }
        if target in mapping:
            mkt, outcome = mapping[target]
            return probs.get(mkt, {}).get(outcome)
        return None

    @staticmethod
    def _get_cote(odds_dict: dict, target: str) -> float:
        """Cote brute du bookmaker pour le marché cible."""
        mapping = {
            "btts":   ("btts", "yes"),
            "over25": ("over25", "over"),
            "over15": ("over15", "over"),
        }
        if target in mapping:
            mkt, outcome = mapping[target]
            return odds_dict.get(mkt, {}).get(outcome, 2.0)
        return 2.0
