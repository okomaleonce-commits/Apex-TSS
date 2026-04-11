"""
APEX-TSS — Suspicion Engine
==============================
Détecte les matchs suspects via 5 indicateurs quantifiables.

Indicateurs:
  S1 — GAP MODÈLE/BOOK excessif  (DC dit 70% home, book donne 40%)
  S2 — CONSENSUS INVERSÉ          (tous les books contre le modèle)
  S3 — MARCHÉ UNDER ANORMAL      (Under 2.5 surévalué → contrôle du score ?)
  S4 — ÉCART SHARP/SOFT          (Pinnacle vs Bet365 divergence forte)
  S5 — ODDS EXTRÊMES SUR OUTSIDER (Away @ < 1.40 dans ligue faible)

Score de suspicion: 0-100
  0-29   → Normal
  30-49  → Attention
  50-74  → Suspect
  75-100 → Très suspect

Usage:
  from tss.suspicion_engine import analyze_suspicion
  result = analyze_suspicion(fix, probs, odds_dict, p_book)
"""

import logging
import numpy as np
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("suspicion")

# ── Seuils ────────────────────────────────────────────────────────────────────
GAP_WARNING    = 0.15   # S1: |P_synth - P_book| > 15%
GAP_CRITICAL   = 0.25   # S1 critique: > 25%
UNDER25_THRESH = 1.55   # S3: Under 2.5 < 1.55 = suspect (P_impl > 64%)
UNDER35_THRESH = 1.30   # S3 critique
SHARP_SOFT_DIV = 0.12   # S4: |Pinnacle - Bet365| normalisé > 12%
EXTREME_FAV    = 1.25   # S5: outsider @ < 1.25 = anomalie

# Ligues "à risque" connu (niveau faible, arbitrage difficile)
HIGH_RISK_LEAGUES = {
    "Bolivia - Primera Division", "Bolivia",
    "Cyprus - First Division", "Cyprus",
    "Malta - Premier League", "Malta",
    "Kosovo - Superliga", "Kosovo",
    "North Macedonia", "Albania",
    "San Marino", "Andorra",
    "Azerbaijan - Premier League",
    "Armenia - Armenian Premier League",
    "Kazakhstan - Premier League",
    "NPL", "Amateur", "Women",  # sous-divisions amateurs
}


# ═══════════════════════════════════════════════════════════════════════════════
# INDICATEURS INDIVIDUELS
# ═══════════════════════════════════════════════════════════════════════════════

def s1_model_book_gap(probs: Dict, p_book: Dict) -> Tuple[float, str]:
    """
    S1 — Gap entre probabilité modèle (DC) et probabilité implicite book.
    Un gap > 25% sur le résultat principal est anormal.
    """
    gaps = []
    desc = []

    for market, p_key in [("H","P_H"), ("D","P_D"), ("A","P_A")]:
        p_s = probs.get(market, 0)
        p_b = p_book.get(p_key, 0)
        if p_b > 0:
            gap = abs(p_s - p_b)
            gaps.append((gap, market, p_s, p_b))

    if not gaps:
        return 0.0, ""

    max_gap, market, p_s, p_b = max(gaps, key=lambda x: x[0])

    if max_gap >= GAP_CRITICAL:
        score = min(40, 15 + (max_gap - GAP_CRITICAL) * 100)
        desc  = (f"S1 \u26a0\ufe0f Gap mod\u00e8le/book critique: "
                 f"{market}={p_s*100:.0f}% vs book={p_b*100:.0f}% "
                 f"(\u0394={max_gap*100:.0f}%)")
    elif max_gap >= GAP_WARNING:
        score = 10 + (max_gap - GAP_WARNING) * 60
        desc  = (f"S1 \u26a0\ufe0f Gap mod\u00e8le/book: "
                 f"{market}={p_s*100:.0f}% vs book={p_b*100:.0f}% "
                 f"(\u0394={max_gap*100:.0f}%)")
    else:
        return 0.0, ""

    return round(score, 1), desc


def s2_consensus_inversion(probs: Dict, odds: Dict) -> Tuple[float, str]:
    """
    S2 — Le book désigne un favori opposé au modèle.
    Ex: DC dit Home 65%, mais odds implicites donnent Away à 55%.
    """
    h_model = probs.get("H", 0)
    a_model = probs.get("A", 0)

    odds_h = odds.get("odds_H", 0)
    odds_a = odds.get("odds_A", 0)

    if not (odds_h and odds_a and odds_h > 1 and odds_a > 1):
        return 0.0, ""

    p_h_book = 1 / odds_h
    p_a_book = 1 / odds_a

    # Model says Home wins, but book says Away wins (or vice versa)
    model_favors_home = h_model > a_model
    book_favors_home  = p_h_book > p_a_book

    if model_favors_home != book_favors_home:
        gap = abs(h_model - a_model) + abs(p_h_book - p_a_book)
        score = min(35, gap * 60)
        winner_model = "Home" if model_favors_home else "Away"
        winner_book  = "Home" if book_favors_home  else "Away"
        desc = (f"S2 \u274c Consensus invers\u00e9: "
                f"mod\u00e8le\u2192{winner_model} ({max(h_model,a_model)*100:.0f}%) "
                f"vs book\u2192{winner_book} ({max(p_h_book,p_a_book)*100:.0f}%)")
        return round(score, 1), desc

    return 0.0, ""


def s3_under_anomaly(odds: Dict, league: str = "") -> Tuple[float, str]:
    """
    S3 — Prix Under 2.5 ou Under 3.5 anormalement bas.
    Possible indicateur de match à score contrôlé.
    """
    under25 = odds.get("odds_under2.5", 0)
    under35 = odds.get("odds_under3.5", 0)

    alerts = []
    score  = 0.0

    if under35 and 1.0 < under35 <= EXTREME_FAV:
        score = max(score, 40)
        alerts.append(f"Under 3.5 @ {under35} (P_impl={1/under35*100:.0f}%)")

    elif under25 and 1.0 < under25 <= UNDER25_THRESH:
        score = max(score, 25)
        alerts.append(f"Under 2.5 @ {under25} (P_impl={1/under25*100:.0f}%)")

    if score > 0:
        # Extra weight in high-risk leagues
        for risk_league in HIGH_RISK_LEAGUES:
            if risk_league.lower() in league.lower():
                score = min(score * 1.5, 50)
                alerts.append("ligue \u00e0 risque")
                break
        desc = f"S3 \u26a0\ufe0f March\u00e9 Under anormal: {' | '.join(alerts)}"
        return round(score, 1), desc

    return 0.0, ""


def s4_sharp_soft_divergence(fix: Dict) -> Tuple[float, str]:
    """
    S4 — Divergence entre bookmaker sharp (Pinnacle) et soft (Bet365).
    Grande divergence = possible information asymétrique.
    """
    # Check if we have multiple bookmaker odds stored
    pinnacle_h = fix.get("odds_H_pinnacle")
    bet365_h   = fix.get("odds_H_bet365")

    if not (pinnacle_h and bet365_h and pinnacle_h > 1 and bet365_h > 1):
        return 0.0, ""  # Data not available

    div = abs(1/pinnacle_h - 1/bet365_h)
    if div >= SHARP_SOFT_DIV:
        score = min(30, div * 200)
        desc  = (f"S4 \u26a0\ufe0f Sharp/Soft: "
                 f"Pinnacle={pinnacle_h} vs Bet365={bet365_h} "
                 f"(\u0394P={div*100:.0f}%)")
        return round(score, 1), desc

    return 0.0, ""


def s5_extreme_underdog(odds: Dict, probs: Dict) -> Tuple[float, str]:
    """
    S5 — Book propose un outsider extrême que le modèle ne confirme pas.
    Ex: Away @ 1.20 mais DC dit Away 38%.
    """
    for side, odds_key, prob_key in [
        ("Home", "odds_H", "H"),
        ("Away", "odds_A", "A"),
    ]:
        odd = odds.get(odds_key, 0)
        p_s = probs.get(prob_key, 0)

        if not odd or odd <= 1.0:
            continue

        p_book_impl = 1 / odd  # raw implied (before demarg)

        # Book makes this team a heavy favorite but model disagrees
        if odd <= EXTREME_FAV and p_s < 0.45:
            gap   = p_book_impl - p_s
            score = min(35, gap * 80)
            desc  = (f"S5 \u26a0\ufe0f Outsider extr\u00eame: "
                     f"{side} @ {odd} (P_book={p_book_impl*100:.0f}% "
                     f"vs P_synth={p_s*100:.0f}%)")
            return round(score, 1), desc

    return 0.0, ""


def s6_high_risk_league(league: str, fix: Dict) -> Tuple[float, str]:
    """
    S6 — Ligue classée à risque élevé d'intégrité.
    """
    league_full = f"{fix.get('league','')} {league}"
    for risk in HIGH_RISK_LEAGUES:
        if risk.lower() in league_full.lower():
            desc = f"S6 \u26a0\ufe0f Ligue \u00e0 risque d'int\u00e9grit\u00e9: {league}"
            return 15.0, desc
    return 0.0, ""


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SUSPICION ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_suspicion(
    fix:      Dict,
    probs:    Dict,
    odds:     Dict,
    p_book:   Dict,
) -> Dict:
    """
    Run all 6 suspicion indicators on a match.

    Returns:
        {
            score: float (0-100),
            level: str ("normal"|"attention"|"suspect"|"tres_suspect"),
            alerts: List[str],
            emoji: str,
        }
    """
    league = fix.get("league", "")

    indicators = [
        s1_model_book_gap(probs, p_book),
        s2_consensus_inversion(probs, odds),
        s3_under_anomaly(odds, league),
        s4_sharp_soft_divergence(fix),
        s5_extreme_underdog(odds, probs),
        s6_high_risk_league(league, fix),
    ]

    total_score = 0.0
    alerts      = []

    for score, desc in indicators:
        if score > 0:
            total_score += score
            if desc:
                alerts.append(desc)

    # Cap at 100
    total_score = min(total_score, 100.0)

    if total_score >= 75:
        level = "tres_suspect"
        emoji = "\U0001f6a8"   # 🚨
    elif total_score >= 50:
        level = "suspect"
        emoji = "\u26a0\ufe0f"  # ⚠️
    elif total_score >= 30:
        level = "attention"
        emoji = "\U0001f440"   # 👀
    else:
        level = "normal"
        emoji = "\u2705"        # ✅

    return {
        "score":  round(total_score, 1),
        "level":  level,
        "emoji":  emoji,
        "alerts": alerts,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM FORMATTER
# ═══════════════════════════════════════════════════════════════════════════════

def format_suspicion_block(result: Dict) -> str:
    """
    Formats suspicion analysis for inline display in scan/analyse messages.
    """
    score  = result["score"]
    level  = result["level"]
    emoji  = result["emoji"]
    alerts = result["alerts"]

    level_labels = {
        "normal":      "Normal",
        "attention":   "Attention",
        "suspect":     "Suspect",
        "tres_suspect":"TRES SUSPECT",
    }

    bar_filled = int(score / 10)
    bar_empty  = 10 - bar_filled
    bar        = "\u2588" * bar_filled + "\u2591" * bar_empty

    lines = [
        f"\n{emoji} <b>Suspicion: {score:.0f}/100</b> — {level_labels[level]}",
        f"  [{bar}]",
    ]
    for a in alerts:
        lines.append(f"  \u2022 {a}")

    return "\n".join(lines)


def format_suspect_message(suspects: List[Dict], window_label: str) -> str:
    """
    Formats full suspicious matches message for /suspect command.
    """
    from datetime import datetime
    ts = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

    if not suspects:
        return (
            "\u26a0\ufe0f <b>APEX-TSS | MATCHS SUSPECTS</b>\n\n"
            f"Fen\u00eatre: {window_label}\n"
            "\u2705 Aucun match suspect d\u00e9tect\u00e9.\n\n"
            "<i>APEX-TSS Suspicion Engine v1.0</i>"
        )

    lines = [
        "\U0001f6a8 <b>APEX-TSS | MATCHS SUSPECTS</b>",
        f"\U0001f4c5 {window_label}  \u00b7  \U0001f550 {ts}",
        f"\U0001f50d {len(suspects)} match(s) signal\u00e9(s)\n",
    ]

    for i, item in enumerate(suspects, 1):
        fix    = item["fix"]
        result = item["suspicion"]
        p      = item["probs"]

        lines += [
            f"{'='*25}",
            f"<b>{i}. {fix['home']}  vs  {fix['away']}</b>",
            "🌍 " + fix['league'] + "  ·  📅 " + fix['date'] +
            (" · ⏰ " + fix['time'] if fix.get('time') else ""),
            f"  1\ufe0f\u20e3 {p['H']*100:.0f}%  \u2796 {p['D']*100:.0f}%  "
            f"2\ufe0f\u20e3 {p['A']*100:.0f}%",
            format_suspicion_block(result),
            "",
        ]

    lines += [
        "="*25,
        "<i>\u26a0\ufe0f Ces matchs pr\u00e9sentent des anomalies statistiques.</i>",
        "<i>NON RECOMMAND\u00c9 pour miser. Surveiller uniquement.</i>",
        "",
        "<i>APEX-TSS Suspicion Engine v1.0</i>",
    ]

    return "\n".join(lines)
