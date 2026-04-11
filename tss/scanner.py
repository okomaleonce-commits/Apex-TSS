"""
APEX-TSS — Scanner
====================
Scans a list of fixtures through the full TSS pipeline and
returns only matches with confirmed BET signals.

Usage:
  from tss.scanner import scan_fixtures
  results = scan_fixtures(fixtures, min_stars=3)
"""

import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

log = logging.getLogger("scanner")


def scan_fixtures(
    fixtures: List[Dict],
    min_stars: int = 2,
    min_ev: float = 0.03,
) -> List[Dict]:
    """
    Run TSS analysis on each fixture.
    Returns only fixtures with at least one BET signal ≥ min_stars.
    """
    from tss.match_analyzer import (
        _load_gates, _best_team_match, _detect_league,
        _get_dc_model, _league_average_probs,
        _simulate_odds, _run_gates, MARKET_LABELS
    )

    gates   = _load_gates()
    results = []

    log.info(f"Scanning {len(fixtures)} fixtures (min_stars={min_stars})")

    for fix in fixtures:
        home_raw = fix["home"]
        away_raw = fix["away"]
        league   = fix["league"]
        date_str = fix["date"]
        time_str = fix.get("time", "")

        # Resolve team names
        home = _best_team_match(home_raw) or home_raw
        away = _best_team_match(away_raw) or away_raw

        # Load DC model
        model    = _get_dc_model(league)
        fallback = False

        if model:
            try:
                probs = model.predict_probs(home, away)
            except Exception:
                probs    = _league_average_probs(home, away)
                fallback = True
        else:
            probs    = _league_average_probs(home, away)
            fallback = True

        # Simulate odds + run gates
        odds    = _simulate_odds(probs, margin=gates["book_margin"])
        signals = _run_gates(probs, odds, gates)

        # Filter BET signals meeting min_stars threshold
        bets = [s for s in signals
                if s["bet"] and s["stars"] >= min_stars and s["ev"] >= min_ev]

        if bets:
            results.append({
                "league":   league,
                "home":     home,
                "away":     away,
                "date":     date_str,
                "time":     time_str,
                "fallback": fallback,
                "bets":     bets,
                "probs":    probs,
                "top_ev":   max(s["ev"] for s in bets),
                "top_stars":max(s["stars"] for s in bets),
            })
            log.info(f"  ✅ BET: {home} vs {away} | {len(bets)} signal(s)")
        else:
            log.debug(f"  — NO BET: {home} vs {away}")

    # Sort by best EV descending
    results.sort(key=lambda x: x["top_ev"], reverse=True)
    return results


def format_scan_message(
    results: List[Dict],
    window_label: str,
    total_scanned: int,
) -> str:
    """Format scan results into a Telegram message."""

    ts = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

    if not results:
        return (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔭 <b>APEX-TSS | SCAN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 {window_label}  ·  🕐 {ts}\n"
            f"🔍 {total_scanned} matchs scannés\n\n"
            f"🚫 <b>AUCUN SIGNAL BET détecté</b>\n"
            f"Tous les marchés échouent aux gates TSS.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>APEX-TSS · NO BET par défaut</i>"
        )

    def _stars(n): return "⭐" * n + "☆" * (5-n)

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "🔭 <b>APEX-TSS | SCAN RÉSULTATS</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 {window_label}  ·  🕐 {ts}",
        f"🔍 {total_scanned} matchs scannés  →  "
        f"<b>{len(results)} signal(s) BET</b>",
        "",
    ]

    for i, fix in enumerate(results, 1):
        fallback_tag = " ⚠️" if fix["fallback"] else ""
        time_tag     = f" · ⏰ {fix['time']}" if fix.get("time") else ""

        lines.append(
            f"{'═'*25}\n"
            f"<b>{i}. {fix['home']}  vs  {fix['away']}</b>\n"
            f"🌍 {fix['league']}  ·  📅 {fix['date']}{time_tag}{fallback_tag}"
        )

        # Probabilities summary
        p = fix["probs"]
        lines.append(
            f"  1️⃣ {p['H']*100:.0f}%  ➖ {p['D']*100:.0f}%  "
            f"2️⃣ {p['A']*100:.0f}%  "
            f"| O2.5: {p['over2.5']*100:.0f}%  BTTS: {p['btts_yes']*100:.0f}%"
        )

        # BET signals
        for s in fix["bets"]:
            stake_pct = s["stake"] * 100
            lines.append(
                f"\n  {_stars(s['stars'])}  <b>{s['label']}</b>\n"
                f"  📊 Cote: <code>{s['odds']}</code>  "
                f"EV: <code>{s['ev']:+.3f}</code>  "
                f"Edge: <code>{s['edge']:+.3f}</code>\n"
                f"  💰 Mise: <code>{stake_pct:.2f}%</code> bankroll"
            )
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>APEX-TSS · Dixon-Coles + Shin · NO BET par défaut</i>",
    ]

    return "\n".join(lines)


def format_scan_summary(results: List[Dict], window_label: str,
                        total_scanned: int) -> str:
    """
    Short summary message sent first (while full analysis loads).
    """
    if not results:
        return (f"🔭 <b>Scan {window_label}</b>\n"
                f"🔍 {total_scanned} matchs — <b>0 signal BET</b>")

    top = results[0]
    return (
        f"🔭 <b>Scan {window_label}</b>\n"
        f"🔍 {total_scanned} matchs → <b>{len(results)} signal(s)</b>\n\n"
        f"🥇 Meilleur: <b>{top['home']} vs {top['away']}</b>\n"
        f"   EV max: <code>{top['top_ev']:+.3f}</code>  "
        f"{'⭐'*top['top_stars']}"
    )
