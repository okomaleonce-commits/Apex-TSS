"""
TSS — BACKTESTING MODULE
=========================
Validation empirique du système sur données historiques.
Walk-forward testing sur 3 saisons.
Métriques : ROI, Yield, Brier Score, Drawdown max, Sharpe adapté.
"""

import csv
import json
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Optional

from tss.orchestrator import TSS
from tss.layer5_risk_engine import BankrollConfig

logger = logging.getLogger("TSS.Backtest")


@dataclass
class BacktestRecord:
    """Un match dans le dataset historique."""
    match_id: str
    home: str
    away: str
    league: str
    kickoff: str
    matchday: Optional[int]
    odds: dict
    result_btts: bool
    result_over25: bool
    result_over15: bool
    goals_home: int
    goals_away: int


@dataclass
class BacktestResult:
    """Résultat d'un bet dans le backtest."""
    match_id: str
    target_market: str
    decision: str
    p_synth: float
    p_real: float
    cote: float
    stake_pct: float
    outcome: bool        # True = bet gagné
    pnl_units: float     # P&L en unités de bankroll


@dataclass
class BacktestReport:
    """Rapport complet de backtest."""
    n_matches_analyzed: int = 0
    n_bets: int = 0
    n_won: int = 0
    total_staked: float = 0.0
    total_pnl: float = 0.0
    roi: float = 0.0
    win_rate: float = 0.0
    max_drawdown: float = 0.0
    brier_score: float = 0.0
    sharpe: float = 0.0
    by_market: dict = field(default_factory=dict)
    by_league: dict = field(default_factory=dict)

    def print_summary(self):
        print(f"\n{'═'*50}")
        print(f"TSS BACKTEST REPORT")
        print(f"{'─'*50}")
        print(f"Matchs analysés  : {self.n_matches_analyzed}")
        print(f"Bets émis        : {self.n_bets}")
        print(f"Bets gagnés      : {self.n_won} ({self.win_rate*100:.1f}%)")
        print(f"ROI              : {self.roi*100:+.2f}%")
        print(f"Drawdown max     : {self.max_drawdown*100:.2f}%")
        print(f"Brier Score      : {self.brier_score:.4f}")
        print(f"Sharpe adapté    : {self.sharpe:.3f}")
        print(f"{'─'*50}")
        print("PAR MARCHÉ :")
        for mkt, stats in self.by_market.items():
            print(f"  {mkt:12} | Bets={stats['n']:3} | Win={stats['win_rate']*100:.0f}% | ROI={stats['roi']*100:+.1f}%")
        print(f"{'─'*50}")
        print("PAR LIGUE :")
        for lg, stats in self.by_league.items():
            print(f"  {lg:16} | Bets={stats['n']:3} | ROI={stats['roi']*100:+.1f}%")
        print(f"{'═'*50}\n")


class BacktestEngine:
    """
    Rejoue le pipeline TSS sur un dataset historique.
    """

    def __init__(self, bankroll: float = 1000.0):
        self.config = BankrollConfig(total_bankroll=bankroll)
        self.tss = TSS(bankroll_config=self.config, log_level="WARNING")

    def load_from_csv(self, filepath: str) -> list[BacktestRecord]:
        """
        Charge un dataset CSV avec les colonnes :
        match_id, home, away, league, kickoff, matchday,
        odds_json (JSON string), goals_home, goals_away
        """
        records = []
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    odds = json.loads(row["odds_json"])
                    goals_h = int(row["goals_home"])
                    goals_a = int(row["goals_away"])
                    records.append(BacktestRecord(
                        match_id=row["match_id"],
                        home=row["home"],
                        away=row["away"],
                        league=row["league"],
                        kickoff=row["kickoff"],
                        matchday=int(row["matchday"]) if row.get("matchday") else None,
                        odds=odds,
                        result_btts=(goals_h >= 1 and goals_a >= 1),
                        result_over25=(goals_h + goals_a > 2),
                        result_over15=(goals_h + goals_a > 1),
                        goals_home=goals_h,
                        goals_away=goals_a
                    ))
                except Exception as e:
                    logger.warning(f"Ligne ignorée ({row.get('match_id','?')}): {e}")
        logger.info(f"[Backtest] {len(records)} matchs chargés depuis {filepath}")
        return records

    def run(
        self,
        records: list[BacktestRecord],
        target_markets: Optional[list[str]] = None
    ) -> BacktestReport:

        targets = target_markets or ["btts", "over25"]
        results: list[BacktestResult] = []
        bankroll_curve = [1.0]  # relatif

        for record in records:
            signals = self.tss.analyze_match(
                home=record.home,
                away=record.away,
                league=record.league,
                kickoff=record.kickoff,
                odds_dict=record.odds,
                target_markets=targets,
                match_id=record.match_id,
                matchday=record.matchday
            )

            for sig in signals:
                mkt = sig.metrics.get("target_market")
                dec = sig.metrics.get("decision")
                if dec == "NO_BET":
                    continue

                stake_pct = sig.stake.get("stake_pct_tss", 0)
                cote = sig.metrics.get("cote", 2.0)
                p_synth = sig.metrics.get("p_synth", 0)
                p_real = sig.metrics.get("p_real", 0)

                # Résultat réel
                outcome_map = {
                    "btts": record.result_btts,
                    "over25": record.result_over25,
                    "over15": record.result_over15
                }
                outcome = outcome_map.get(mkt, False)
                pnl = stake_pct * (cote - 1) if outcome else -stake_pct

                results.append(BacktestResult(
                    match_id=record.match_id,
                    target_market=mkt,
                    decision=dec,
                    p_synth=p_synth,
                    p_real=p_real,
                    cote=cote,
                    stake_pct=stake_pct,
                    outcome=outcome,
                    pnl_units=pnl
                ))

                # Courbe bankroll
                new_val = bankroll_curve[-1] + pnl
                bankroll_curve.append(new_val)

        return self._compute_report(results, bankroll_curve, len(records))

    def _compute_report(
        self,
        results: list[BacktestResult],
        bankroll_curve: list[float],
        n_matches: int
    ) -> BacktestReport:

        n_bets = len(results)
        if n_bets == 0:
            report = BacktestReport(n_matches_analyzed=n_matches)
            return report

        n_won = sum(1 for r in results if r.outcome)
        total_staked = sum(r.stake_pct for r in results)
        total_pnl = sum(r.pnl_units for r in results)
        roi = total_pnl / total_staked if total_staked > 0 else 0.0
        win_rate = n_won / n_bets

        # Drawdown max
        max_dd = self._max_drawdown(bankroll_curve)

        # Brier Score
        brier = sum((r.p_synth - (1 if r.outcome else 0)) ** 2 for r in results) / n_bets

        # Sharpe adapté (sur les ROI par bet)
        roi_list = [r.pnl_units / r.stake_pct if r.stake_pct > 0 else 0 for r in results]
        mean_roi = sum(roi_list) / len(roi_list)
        std_roi  = math.sqrt(sum((x - mean_roi) ** 2 for x in roi_list) / len(roi_list)) if len(roi_list) > 1 else 0
        sharpe = mean_roi / std_roi if std_roi > 0 else 0.0

        # Par marché
        by_market = {}
        for mkt in set(r.target_market for r in results):
            subset = [r for r in results if r.target_market == mkt]
            s_staked = sum(r.stake_pct for r in subset)
            s_pnl = sum(r.pnl_units for r in subset)
            by_market[mkt] = {
                "n": len(subset),
                "win_rate": sum(1 for r in subset if r.outcome) / len(subset),
                "roi": s_pnl / s_staked if s_staked > 0 else 0.0
            }

        # Par ligue (non disponible ici, placeholder)
        by_league = {}

        return BacktestReport(
            n_matches_analyzed=n_matches,
            n_bets=n_bets,
            n_won=n_won,
            total_staked=total_staked,
            total_pnl=total_pnl,
            roi=roi,
            win_rate=win_rate,
            max_drawdown=max_dd,
            brier_score=brier,
            sharpe=sharpe,
            by_market=by_market,
            by_league=by_league
        )

    @staticmethod
    def _max_drawdown(curve: list[float]) -> float:
        """Calcul du drawdown maximum sur la courbe de bankroll."""
        max_val = curve[0]
        max_dd = 0.0
        for val in curve:
            if val > max_val:
                max_val = val
            dd = (max_val - val) / max_val if max_val > 0 else 0
            if dd > max_dd:
                max_dd = dd
        return max_dd


if __name__ == "__main__":
    """
    Exemple d'utilisation du backtesting.
    Préparez un fichier CSV avec la structure attendue.
    """
    engine = BacktestEngine(bankroll=1000.0)

    # Exemple avec données synthétiques
    sample_records = [
        BacktestRecord(
            match_id="test_001",
            home="Napoli", away="Lazio",
            league="serie_a", kickoff="2025-10-15T20:45:00",
            matchday=8,
            odds={
                "1x2": {"home": 2.10, "draw": 3.40, "away": 3.60},
                "over25": {"over": 1.85, "under": 2.05},
                "over15": {"over": 1.40, "under": 3.10},
                "btts": {"yes": 1.90, "no": 2.00},
                "home_over05": {"over": 1.55, "under": 2.60},
                "away_over05": {"over": 2.10, "under": 1.80},
                "ah": {"home_line": -0.5, "home_odds": 2.08, "away_odds": 1.85}
            },
            result_btts=True, result_over25=True, result_over15=True,
            goals_home=2, goals_away=1
        ),
    ]

    report = engine.run(sample_records)
    report.print_summary()
