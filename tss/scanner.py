"""
APEX-TSS — Scanner with Real Odds
====================================
Scans fixtures through TSS with real bookmaker odds from The Odds API.
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

log = logging.getLogger("scanner")

try:
    from tss.suspicion_engine import analyze_suspicion, format_suspicion_block
    SUSPICION_AVAILABLE = True
except ImportError:
    SUSPICION_AVAILABLE = False


def scan_fixtures(
    fixtures: List[Dict],
    min_stars: int = 2,
    min_ev: float = 0.03,
    use_real_odds: bool = True,
) -> List[Dict]:
    """
    Run full TSS pipeline on fixtures.
    With use_real_odds=True: fetches real bookmaker odds first.
    Returns only fixtures with ≥1 BET signal.
    """
    from tss.match_analyzer import (
        _load_gates, _best_team_match, _get_dc_model,
        _league_average_probs, _simulate_odds, _run_gates,
    )

    gates = _load_gates()

    # Enrich with real odds
    if use_real_odds:
        try:
            from tss.odds_api import enrich_fixtures_with_odds, demarginalize_odds
            fixtures = enrich_fixtures_with_odds(fixtures)
            has_real_odds = True
        except Exception as e:
            log.warning(f"Real odds fetch failed: {e} — using synthetic")
            has_real_odds = False
    else:
        has_real_odds = False

    # ── Filter: keep only future matches ──────────────────────────────────────
    now_utc = datetime.utcnow()
    future  = []
    skipped = 0
    for fix in fixtures:
        try:
            fix_date = fix.get("date", "")
            fix_time = fix.get("time", "00:00") or "00:00"
            # Parse kickoff time (UTC)
            kickoff_str = f"{fix_date} {fix_time}"
            kickoff     = datetime.strptime(kickoff_str, "%Y-%m-%d %H:%M")
            # Add 90 min buffer — skip if match likely finished
            if kickoff < now_utc - __import__('datetime').timedelta(minutes=90):
                log.info(f"  SKIP (past): {fix['home']} vs {fix['away']} @ {kickoff_str}")
                skipped += 1
                continue
        except Exception:
            pass  # keep if can't parse time
        future.append(fix)

    if skipped:
        log.info(f"  Filtered {skipped} past matches — {len(future)} remaining")
    fixtures = future

    results = []
    log.info(f"Scanning {len(fixtures)} fixtures | real_odds={has_real_odds}")

    for fix in fixtures:
        home_raw = fix["home"]
        away_raw = fix["away"]
        league   = fix["league"]

        home = _best_team_match(home_raw) or home_raw
        away = _best_team_match(away_raw) or away_raw

        # DC model → P_synth
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

        # Real odds or synthetic
        if has_real_odds and fix.get("odds_matched"):
            # Use real book odds
            real_odds = {
                "odds_H":         fix.get("odds_H"),
                "odds_D":         fix.get("odds_D"),
                "odds_A":         fix.get("odds_A"),
                "odds_over2.5":   fix.get("odds_over2.5"),
                "odds_under2.5":  fix.get("odds_under2.5"),
                "odds_over3.5":   fix.get("odds_over3.5"),
                "odds_under3.5":  fix.get("odds_under3.5"),
                "odds_btts_yes":  fix.get("odds_btts_yes"),
                "odds_btts_no":   fix.get("odds_btts_no"),
            }
            # Fill missing with synthetic
            synth = _simulate_odds(probs, margin=gates["book_margin"])
            for k, v in real_odds.items():
                if not v or v <= 1.0:
                    real_odds[k] = synth.get(k, 2.0)
            odds_dict = real_odds
            odds_source = fix.get("bookie_used", "real")
        else:
            odds_dict  = _simulate_odds(probs, margin=gates["book_margin"])
            odds_source = "synthetic"

        # Demarginalize real odds → P_book
        if has_real_odds and fix.get("odds_matched"):
            from tss.odds_api import demarginalize_odds
            p_book = demarginalize_odds(fix)
            # Fill missing markets with synthetic demarg
            synth_book = _compute_synthetic_pbook(probs, gates["book_margin"])
            for k, v in synth_book.items():
                if k not in p_book:
                    p_book[k] = v
        else:
            p_book = _compute_synthetic_pbook(probs, gates["book_margin"])

        # Run TSS gates with real P_book
        signals = _run_gates_with_pbook(probs, odds_dict, p_book, gates)

        bets = [s for s in signals
                if s["bet"] and s["stars"] >= min_stars and s["ev"] >= min_ev]

        # Suspicion analysis (always run, regardless of BET/NO BET)
        suspicion = {}
        if SUSPICION_AVAILABLE:
            try:
                suspicion = analyze_suspicion(fix, probs, odds_dict, p_book)
            except Exception as e:
                log.debug(f"Suspicion error: {e}")

        if bets:
            results.append({
                "league":      league,
                "home":        home,
                "away":        away,
                "date":        fix["date"],
                "time":        fix.get("time", ""),
                "fallback":    fallback,
                "odds_source": odds_source,
                "bets":        bets,
                "probs":       probs,
                "top_ev":      max(s["ev"] for s in bets),
                "top_stars":   max(s["stars"] for s in bets),
                "suspicion":   suspicion,
            })
            log.info(f"  ✅ {home} vs {away} | {len(bets)} BET(s) | "
                     f"best EV={max(s['ev'] for s in bets):+.3f} [{odds_source}]")

    results.sort(key=lambda x: x["top_ev"], reverse=True)

    # Gate failure diagnostics
    if not results:
        gate_fails = {"no_odds": 0, "ev": 0, "edge": 0, "odds_range": 0, "all_past": 0}
        for fix in fixtures:
            home = _best_team_match(fix["home"]) or fix["home"]
            away = _best_team_match(fix["away"]) or fix["away"]
            model = _get_dc_model(fix["league"])
            if not model:
                gate_fails["no_odds"] += 1
                continue
            try:
                probs = model.predict_probs(home, away)
            except Exception:
                probs = _league_average_probs(home, away)
            odds = _simulate_odds(probs, margin=gates["book_margin"])
            p_b  = _compute_synthetic_pbook(probs, gates["book_margin"])
            sigs = _run_gates_with_pbook(probs, odds, p_b, gates)
            for s in sigs:
                for f in s.get("fails", []):
                    if "EV" in f:     gate_fails["ev"]        += 1
                    if "Edge" in f:   gate_fails["edge"]      += 1
                    if "Odds" in f:   gate_fails["odds_range"]+= 1
        log.info(f"Gate failure breakdown: {gate_fails}")
        if gate_fails["ev"] > 0 and gate_fails["edge"] == 0:
            log.info("  → All failures on EV gate. Try /setgates ev_min=0.005")
        elif gate_fails["edge"] > 0:
            log.info("  → Edge failures dominant. Synthetic odds = near-zero real edge.")
            log.info("  → Solution: real bookmaker odds needed (Odds API)")

    return results


def _compute_synthetic_pbook(probs: Dict, margin: float) -> Dict:
    """Compute synthetic P_book from DC probs + margin via Shin."""
    from tss.match_analyzer import _simulate_odds, _shin_demarg

    odds = _simulate_odds(probs, margin=margin)
    result = {}

    def _demarg(keys_odds, keys_out):
        vals = [odds.get(k, 2.0) for k in keys_odds]
        try:
            ps = _shin_demarg(vals)
            for k, p in zip(keys_out, ps):
                result[k] = p
        except Exception:
            pass

    _demarg(["odds_H","odds_D","odds_A"],            ["P_H","P_D","P_A"])
    _demarg(["odds_over2.5","odds_under2.5"],         ["P_over2.5","P_under2.5"])
    _demarg(["odds_over3.5","odds_under3.5"],         ["P_over3.5","P_under3.5"])
    _demarg(["odds_btts_yes","odds_btts_no"],         ["P_btts_yes","P_btts_no"])
    return result


def _run_gates_with_pbook(probs: Dict, odds: Dict, p_book: Dict, gates: Dict) -> List[Dict]:
    """Run TSS gates using pre-computed P_book."""
    from tss.match_analyzer import MARKET_LABELS

    ODDS_KEY = {
        "H": "odds_H", "D": "odds_D", "A": "odds_A",
        "over2.5": "odds_over2.5", "under2.5": "odds_under2.5",
        "over3.5": "odds_over3.5", "under3.5": "odds_under3.5",
        "btts_yes": "odds_btts_yes", "btts_no": "odds_btts_no",
    }
    PBOOK_KEY = {
        "H": "P_H", "D": "P_D", "A": "P_A",
        "over2.5": "P_over2.5", "under2.5": "P_under2.5",
        "over3.5": "P_over3.5", "under3.5": "P_under3.5",
        "btts_yes": "P_btts_yes", "btts_no": "P_btts_no",
    }

    signals = []
    for market in MARKET_LABELS:
        p_s  = probs.get(market, 0)
        odd  = odds.get(ODDS_KEY.get(market, ""), 0)
        p_b  = p_book.get(PBOOK_KEY.get(market, ""), p_s * 0.95)

        if not odd or odd <= 1.0:
            continue

        ev   = round(p_s * odd - 1, 4)
        edge = round(p_s - p_b, 4)

        fails = []
        if ev   < gates["ev_min"]:   fails.append(f"EV={ev:.3f}&lt;{gates['ev_min']}")
        if edge < gates["edge_min"]: fails.append(f"Edge={edge:.3f}&lt;{gates['edge_min']}")
        if not (gates["odds_min"] <= odd <= gates["odds_max"]):
            fails.append(f"Odds={odd} hors [{gates['odds_min']},{gates['odds_max']}]")

        stars = 0
        if not fails:
            if ev >= 0.10:   stars += 2
            elif ev >= 0.05: stars += 1
            if edge >= 0.10: stars += 2
            elif edge >= 0.07: stars += 1
            stars = min(stars, 5)

        b  = odd - 1
        k  = max(0, (b*p_s - (1-p_s))/b * gates["kelly_fraction"]) if b > 0 else 0
        sk = min(k, gates["max_stake_pct"])

        signals.append({
            "market":  market,
            "label":   MARKET_LABELS[market],
            "p_synth": round(p_s, 4),
            "p_book":  round(p_b, 4),
            "odds":    round(odd, 3),
            "ev":      ev, "edge": edge,
            "kelly":   round(k, 4),
            "stake":   round(sk, 4),
            "bet":     len(fails) == 0,
            "fails":   fails,
            "stars":   stars,
        })

    return sorted(signals, key=lambda x: x["ev"], reverse=True)


def format_scan_message(results: List[Dict], window_label: str,
                        total_scanned: int) -> str:

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
        f"🔍 {total_scanned} matchs  →  <b>{len(results)} signal(s) BET</b>",
        "",
    ]

    for i, fix in enumerate(results, 1):
        src_icon  = "📡" if fix.get("odds_source","") not in ("synthetic","") else "🔮"
        src_label = fix.get("odds_source", "synthetic")
        fb_tag    = " ⚠️" if fix["fallback"] else ""
        time_tag  = f" · ⏰ {fix['time']}" if fix.get("time") else ""

        p = fix["probs"]
        lines += [
            f"{'─'*25}",
            f"<b>{i}. {fix['home']}  vs  {fix['away']}</b>",
            f"🌍 {fix['league']}  ·  📅 {fix['date']}{time_tag}  {src_icon} {src_label}{fb_tag}",
            f"  1️⃣ {p['H']*100:.0f}%  ➖ {p['D']*100:.0f}%  2️⃣ {p['A']*100:.0f}%  "
            f"O2.5:{p['over2.5']*100:.0f}%",
            "",
        ]

        for s in fix["bets"]:
            lines.append(
                f"  {_stars(s['stars'])}  <b>{s['label']}</b>\n"
                f"  📊 <code>{s['odds']}</code>  "
                f"EV:<code>{s['ev']:+.3f}</code>  "
                f"Edge:<code>{s['edge']:+.3f}</code>  "
                f"💰<code>{s['stake']*100:.1f}%</code>"
            )
        # Suspicion flag inline
        susp = fix.get("suspicion", {})
        if susp and susp.get("score", 0) >= 30:
            lines.append(format_suspicion_block(susp))
        lines.append("")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>APEX-TSS · Dixon-Coles + Shin + Real Odds</i>",
    ]
    return "\n".join(lines)


def format_scan_messages(results: list, window_label: str,
                         total_scanned: int, top_n: int = 5) -> list:
    """Split scan results into Telegram messages (≤4000 chars each), Top-N by EV."""
    from datetime import datetime
    ts = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

    if not results:
        return [format_scan_message([], window_label, total_scanned)]

    def _st(n): return "\u2b50" * n + "\u2606" * (5 - n)

    n_signals = sum(len(r["bets"]) for r in results)
    top       = results[:top_n]
    rest      = results[top_n:]

    header = (
        "\u2501" * 25 + "\n"
        "\U0001f52d <b>APEX-TSS | SCAN R\u00c9SULTATS</b>\n"
        "\u2501" * 25 + "\n"
        f"\U0001f4c5 {window_label}  \u00b7  \U0001f550 {ts}\n"
        f"\U0001f50d {total_scanned} matchs  \u2192  "
        f"<b>{len(results)} BET / {n_signals} signal(s)</b>\n"
        f"\U0001f3c6 TOP {min(top_n, len(results))} par EV\n"
    )

    blocks = []
    for i, fix in enumerate(top, 1):
        src  = "\U0001f4e1" if fix.get("odds_source", "") not in ("synthetic", "") else "\U0001f52e"
        tstr = f" \u00b7 \u23f0 {fix['time']}" if fix.get("time") else ""
        p    = fix["probs"]
        b    = (
            "\u2500" * 25 + "\n"
            f"<b>{i}. {fix['home']}  vs  {fix['away']}</b>\n"
            f"\U0001f30d {fix['league']}  \u00b7  \U0001f4c5 {fix['date']}{tstr}  {src}\n"
            f"  1\ufe0f\u20e3 {p['H']*100:.0f}%  \u2796 {p['D']*100:.0f}%  "
            f"2\ufe0f\u20e3 {p['A']*100:.0f}%  O2.5:{p['over2.5']*100:.0f}%\n\n"
        )
        for s in fix["bets"]:
            b += (
                f"  {_st(s['stars'])}  <b>{s['label']}</b>\n"
                f"  \U0001f4ca <code>{s['odds']}</code>  "
                f"EV:<code>{s['ev']:+.3f}</code>  "
                f"Edge:<code>{s['edge']:+.3f}</code>  "
                f"\U0001f4b0<code>{s['stake']*100:.1f}%</code>\n"
            )
        blocks.append(b)

    rest_block = ""
    if rest:
        rest_block = "\u2501" * 25 + "\n"
        rest_block += f"\U0001f4cb <b>+{len(rest)} autres BET (EV plus faible)</b>\n"
        for r in rest:
            mkt = r["bets"][0]["label"] if r["bets"] else "?"
            rest_block += f"  \u2022 {r['home']} vs {r['away']} \u2014 {mkt} EV:<code>{r['top_ev']:+.3f}</code>\n"

    footer = "\n<i>APEX-TSS \u00b7 Dixon-Coles + Shin + Real Odds</i>"

    # Pack into ≤4000 char messages
    messages  = []
    current   = header
    for block in blocks:
        if len(current) + len(block) > 3900:
            messages.append(current)
            current = block
        else:
            current += block
    current += rest_block + footer
    messages.append(current)
    return messages
