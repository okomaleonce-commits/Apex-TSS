#!/usr/bin/env python3
"""
TSS — TRIANGULATION SIGNAL SYSTEM v1.0
Exemple d'utilisation avec un match réel.

Usage :
    python main.py
    python main.py --telegram-token TOKEN --chat-id CHAT_ID
"""

import argparse
from tss.orchestrator import TSS
from tss.layer5_risk_engine import BankrollConfig


def main():
    parser = argparse.ArgumentParser(description="TSS — Triangulation Signal System v1.0")
    parser.add_argument("--telegram-token", type=str, default=None)
    parser.add_argument("--chat-id", type=str, default=None)
    parser.add_argument("--bankroll", type=float, default=1000.0)
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()

    # ─────────────────────────────────────────
    #  INITIALISATION
    # ─────────────────────────────────────────
    config = BankrollConfig(total_bankroll=args.bankroll)

    tss = TSS(
        bankroll_config=config,
        telegram_token=args.telegram_token,
        telegram_chat_id=args.chat_id,
        log_level=args.log_level
    )

    # ─────────────────────────────────────────
    #  EXEMPLE DE MATCH
    #  Napoli vs Lazio — Serie A
    # ─────────────────────────────────────────
    odds = {
        "1x2": {
            "home": 2.10,
            "draw": 3.40,
            "away": 3.60
        },
        "over25": {
            "over": 1.85,
            "under": 2.05
        },
        "over15": {
            "over": 1.40,
            "under": 3.10
        },
        "btts": {
            "yes": 1.90,
            "no": 2.00
        },
        "home_over05": {
            "over": 1.55,
            "under": 2.60
        },
        "away_over05": {
            "over": 2.10,
            "under": 1.80
        },
        "ah": {
            "home_line": -0.5,
            "home_odds": 2.08,
            "away_odds": 1.85
        }
    }

    # Signaux APEX-ENGINE (optionnel — pour détection alignement)
    apex_signals = {
        "btts": "BET_YES",     # APEX confirme BTTS Yes
        "over25": "NO_BET"     # APEX ne joue pas Over 2.5
    }

    # ─────────────────────────────────────────
    #  ANALYSE
    # ─────────────────────────────────────────
    signals = tss.analyze_match(
        home="Napoli",
        away="Lazio",
        league="serie_a",
        kickoff="2026-04-09T20:45:00",
        odds_dict=odds,
        target_markets=["btts", "over25"],
        matchday=30,
        is_midweek=False,
        apex_signals=apex_signals
    )

    # ─────────────────────────────────────────
    #  RÉSUMÉ FINAL
    # ─────────────────────────────────────────
    print(f"\n{'═'*50}")
    print(f"TSS — {len(signals)} signal(s) émis")
    for s in signals:
        dec = s.metrics.get("decision", "N/A")
        mkt = s.metrics.get("target_market", "N/A")
        sdt = s.metrics.get("sdt", 0)
        stake = s.stake.get("stake_units", 0)
        print(f"  → {mkt.upper():10} | {dec:10} | SDT={sdt:.3f} | Stake={stake:.2f}u")
    print(f"{'═'*50}\n")


if __name__ == "__main__":
    main()
