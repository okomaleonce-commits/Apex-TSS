"""
TSS LAYER 0 — DATA INTAKE
=========================
Snapshot cotes multi-bookmakers avec hiérarchie de source.
Sources supportées : Pinnacle (priorité 1), Betfair Exchange (2), Consensus top-5 (3).
Détection de mouvement de ligne H-3 → H-1.
"""

import json
import time
import logging
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum

logger = logging.getLogger("TSS.Layer0")


class OddsSource(Enum):
    PINNACLE = "pinnacle"
    BETFAIR  = "betfair"
    CONSENSUS = "consensus"
    MANUAL   = "manual"


@dataclass
class MarketOdds:
    """Cotes brutes d'un marché donné."""
    market: str                        # "btts", "over25", "1x2", "ah", etc.
    outcomes: dict[str, float]         # {"yes": 1.90, "no": 2.05}
    source: OddsSource = OddsSource.MANUAL
    snapshot_time: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["source"] = self.source.value
        return d


@dataclass
class MatchSnapshot:
    """Ensemble de marchés pour un match à un instant T."""
    match_id: str
    home: str
    away: str
    league: str
    kickoff: str                       # ISO format "2026-04-09T20:45:00"
    snapshot_label: str                # "H-24", "H-3", "H-1"
    markets: dict[str, MarketOdds] = field(default_factory=dict)
    source: OddsSource = OddsSource.MANUAL
    raw_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def add_market(self, market_key: str, odds: dict[str, float],
                   source: OddsSource = OddsSource.MANUAL) -> None:
        self.markets[market_key] = MarketOdds(
            market=market_key,
            outcomes=odds,
            source=source,
            snapshot_time=datetime.utcnow().isoformat()
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["source"] = self.source.value
        d["markets"] = {k: v.to_dict() for k, v in self.markets.items()}
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class LineMovementDetector:
    """
    Détecte les mouvements de ligne entre deux snapshots.
    Si mouvement > 10% sur un marché → ALERT.
    """

    MOVEMENT_THRESHOLD = 0.10  # 10%

    def compare(
        self,
        snap_early: MatchSnapshot,
        snap_late: MatchSnapshot
    ) -> dict:
        alerts = []
        movements = {}

        common_markets = set(snap_early.markets) & set(snap_late.markets)

        for mkt in common_markets:
            early_outcomes = snap_early.markets[mkt].outcomes
            late_outcomes  = snap_late.markets[mkt].outcomes

            mkt_movements = {}
            for outcome_key in early_outcomes:
                if outcome_key not in late_outcomes:
                    continue
                odds_early = early_outcomes[outcome_key]
                odds_late  = late_outcomes[outcome_key]
                if odds_early == 0:
                    continue
                move = abs(odds_late - odds_early) / odds_early
                mkt_movements[outcome_key] = {
                    "from": odds_early,
                    "to": odds_late,
                    "move_pct": round(move * 100, 2)
                }
                if move >= self.MOVEMENT_THRESHOLD:
                    alerts.append({
                        "market": mkt,
                        "outcome": outcome_key,
                        "from": odds_early,
                        "to": odds_late,
                        "move_pct": round(move * 100, 2),
                        "level": "CRITICAL" if move >= 0.15 else "WARNING"
                    })
                    logger.warning(
                        f"[LINE MOVEMENT] {snap_early.home} vs {snap_early.away} | "
                        f"{mkt}/{outcome_key} : {odds_early} → {odds_late} "
                        f"({move*100:.1f}%)"
                    )
            movements[mkt] = mkt_movements

        return {
            "match_id": snap_early.match_id,
            "from_snapshot": snap_early.snapshot_label,
            "to_snapshot": snap_late.snapshot_label,
            "movements": movements,
            "alerts": alerts,
            "no_bet_recommended": any(a["level"] == "CRITICAL" for a in alerts)
        }


class DataIntake:
    """
    Gestionnaire principal des snapshots.
    Stockage en mémoire + export JSON.
    """

    def __init__(self):
        self._snapshots: dict[str, list[MatchSnapshot]] = {}  # match_id → liste snapshots

    def register_snapshot(self, snapshot: MatchSnapshot) -> None:
        mid = snapshot.match_id
        if mid not in self._snapshots:
            self._snapshots[mid] = []
        self._snapshots[mid].append(snapshot)
        logger.info(f"[INTAKE] Snapshot '{snapshot.snapshot_label}' enregistré pour {snapshot.home} vs {snapshot.away}")

    def get_latest_snapshot(self, match_id: str) -> Optional[MatchSnapshot]:
        snaps = self._snapshots.get(match_id, [])
        return snaps[-1] if snaps else None

    def get_snapshots(self, match_id: str) -> list[MatchSnapshot]:
        return self._snapshots.get(match_id, [])

    def check_line_movement(self, match_id: str) -> Optional[dict]:
        snaps = self.get_snapshots(match_id)
        if len(snaps) < 2:
            return None
        detector = LineMovementDetector()
        return detector.compare(snaps[-2], snaps[-1])

    def save_to_file(self, match_id: str, filepath: str) -> None:
        snaps = self.get_snapshots(match_id)
        data = [s.to_dict() for s in snaps]
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"[INTAKE] Snapshots sauvegardés → {filepath}")


# ─────────────────────────────────────────────
# HELPER : construction manuelle d'un snapshot
# ─────────────────────────────────────────────

def build_snapshot(
    match_id: str,
    home: str,
    away: str,
    league: str,
    kickoff: str,
    label: str,
    odds_dict: dict,
    source: OddsSource = OddsSource.MANUAL
) -> MatchSnapshot:
    """
    odds_dict exemple :
    {
      "1x2":  {"home": 2.10, "draw": 3.40, "away": 3.60},
      "over25": {"over": 1.85, "under": 2.05},
      "over15": {"over": 1.40, "under": 3.10},
      "btts":  {"yes": 1.90, "no": 2.00},
      "home_over05": {"over": 1.55, "under": 2.60},
      "away_over05": {"over": 2.10, "under": 1.80},
      "ah":   {"home_line": -0.5, "home_odds": 2.08, "away_odds": 1.85}
    }
    """
    snap = MatchSnapshot(
        match_id=match_id,
        home=home,
        away=away,
        league=league,
        kickoff=kickoff,
        snapshot_label=label,
        source=source
    )
    for mkt_key, outcomes in odds_dict.items():
        snap.add_market(mkt_key, outcomes, source)
    return snap
