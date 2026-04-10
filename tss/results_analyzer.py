"""
APEX-TSS — Results Analyzer
============================
Produces 3 objective outputs:
  1. ROI global + Sharpe ratio + drawdown
  2. Gate calibration report (optimal ev_min / edge_min grid)
  3. Market profitability heatmap (league × market)
"""

import json
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ANALYZER] %(message)s")
log = logging.getLogger("results_analyzer")

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ROI + PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════════════════════════

def compute_roi_metrics(df: pd.DataFrame) -> Dict:
    """Full performance metrics on BET signals only."""
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty:
        return {"error": "No BET signals found."}

    total_staked  = bets["stake_pct"].sum()
    total_pnl     = bets["pnl_units"].sum()
    roi           = total_pnl / total_staked if total_staked > 0 else 0.0
    win_rate      = (bets["outcome"] == "WIN").mean()
    n_bets        = len(bets)
    n_wins        = (bets["outcome"] == "WIN").sum()
    avg_odds      = bets["odds"].mean()

    # Running PnL for drawdown & Sharpe
    bets_sorted   = bets.sort_values("date").reset_index(drop=True)
    pnl_series    = bets_sorted["pnl_units"].cumsum()
    peak          = pnl_series.cummax()
    drawdown      = pnl_series - peak
    max_drawdown  = drawdown.min()

    # Daily Sharpe (annualised)
    daily_pnl     = bets_sorted.groupby("date")["pnl_units"].sum()
    sharpe        = (daily_pnl.mean() / daily_pnl.std() * np.sqrt(252)
                    if daily_pnl.std() > 0 else 0.0)

    # Yield per bet
    avg_stake     = bets["stake_pct"].mean()
    yield_per_bet = total_pnl / n_bets if n_bets > 0 else 0.0

    return {
        "n_signals_total":  len(df),
        "n_bets":           int(n_bets),
        "n_no_bets":        int(len(df) - n_bets),
        "bet_rate_pct":     round(n_bets / len(df) * 100, 2),
        "win_rate":         round(float(win_rate), 4),
        "total_staked":     round(float(total_staked), 4),
        "total_pnl":        round(float(total_pnl), 4),
        "roi":              round(float(roi), 4),
        "roi_pct":          round(float(roi) * 100, 2),
        "avg_odds":         round(float(avg_odds), 3),
        "avg_stake_pct":    round(float(avg_stake), 4),
        "yield_per_bet":    round(float(yield_per_bet), 4),
        "max_drawdown":     round(float(max_drawdown), 4),
        "sharpe_annualised":round(float(sharpe), 3),
        "n_wins":           int(n_wins),
        "n_losses":         int(n_bets - n_wins),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. GATE CALIBRATION REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def gate_calibration_report(df: pd.DataFrame, min_bets: int = 20) -> pd.DataFrame:
    """
    Grid search EV × Edge thresholds.
    Returns sorted DataFrame with ROI per (ev_min, edge_min) combination.
    """
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty:
        log.warning("No bets for gate calibration.")
        return pd.DataFrame()

    ev_range   = np.arange(0.01, 0.15, 0.01)
    edge_range = np.arange(0.02, 0.18, 0.01)

    rows = []
    for ev_t in ev_range:
        for ed_t in edge_range:
            mask = (bets["ev"] >= ev_t) & (bets["edge"] >= ed_t)
            sub  = bets[mask]
            if len(sub) < min_bets:
                continue
            staked = sub["stake_pct"].sum()
            pnl    = sub["pnl_units"].sum()
            roi    = pnl / staked if staked > 0 else 0.0
            wr     = (sub["outcome"] == "WIN").mean()
            rows.append({
                "ev_min":   round(ev_t, 3),
                "edge_min": round(ed_t, 3),
                "n_bets":   len(sub),
                "roi_pct":  round(roi * 100, 2),
                "win_rate": round(wr, 3),
                "total_pnl":round(pnl, 4),
            })

    calib = pd.DataFrame(rows).sort_values("roi_pct", ascending=False)

    # Top 10 configurations
    log.info("\n── TOP 10 GATE CONFIGURATIONS ──")
    log.info(calib.head(10).to_string(index=False))

    # Optimal config
    if not calib.empty:
        best = calib.iloc[0]
        log.info(f"\n✅ OPTIMAL: ev_min={best['ev_min']}  "
                 f"edge_min={best['edge_min']}  "
                 f"ROI={best['roi_pct']}%  n={best['n_bets']}")

    return calib


# ═══════════════════════════════════════════════════════════════════════════════
# 3. MARKET PROFITABILITY HEATMAP (data for rendering)
# ═══════════════════════════════════════════════════════════════════════════════

def market_league_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a pivot: rows=league, cols=market, values=ROI%
    Only BET signals, minimum 5 bets per cell.
    """
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty:
        return pd.DataFrame()

    rows = []
    for (league, market), group in bets.groupby(["league", "market"]):
        if len(group) < 5:
            continue
        staked = group["stake_pct"].sum()
        pnl    = group["pnl_units"].sum()
        roi    = pnl / staked if staked > 0 else 0.0
        rows.append({
            "league": league,
            "market": market,
            "n_bets": len(group),
            "roi_pct": round(roi * 100, 2),
            "win_rate": round((group["outcome"] == "WIN").mean(), 3),
            "total_pnl": round(pnl, 4),
        })

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail

    pivot = detail.pivot_table(
        index="league", columns="market", values="roi_pct", aggfunc="mean"
    ).round(2)

    return pivot


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SEASON-BY-SEASON BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════════

def season_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """ROI per season (test season)."""
    bets = df[df["decision"] == "BET"].copy()
    rows = []
    for season, group in bets.groupby("season"):
        staked = group["stake_pct"].sum()
        pnl    = group["pnl_units"].sum()
        roi    = pnl / staked if staked > 0 else 0.0
        rows.append({
            "season":   season,
            "n_bets":   len(group),
            "roi_pct":  round(roi * 100, 2),
            "total_pnl":round(pnl, 4),
            "win_rate": round((group["outcome"] == "WIN").mean(), 3),
        })
    return pd.DataFrame(rows).sort_values("season")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. FULL REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_full_report(df: pd.DataFrame, config: Dict = None) -> str:
    """
    Generates a Markdown report with all three objectives.
    Returns the report as a string and saves to reports/
    """
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    cfg  = config or {}

    # ── 1. ROI Global ─────────────────────────────────────────────────────────
    metrics  = compute_roi_metrics(df)
    # ── 2. Gate Calibration ───────────────────────────────────────────────────
    calib    = gate_calibration_report(df)
    best_cfg = calib.iloc[0].to_dict() if not calib.empty else {}
    # ── 3. Market Heatmap ─────────────────────────────────────────────────────
    pivot    = market_league_heatmap(df)
    # ── 4. Season breakdown ───────────────────────────────────────────────────
    seasons  = season_breakdown(df)

    # ── Build markdown ────────────────────────────────────────────────────────
    lines = [
        "# APEX-TSS — Walk-Forward Backtest Report",
        f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "---",
        "",
        "## 1. ROI Global",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
    ]
    for k, v in metrics.items():
        lines.append(f"| {k} | {v} |")

    lines += [
        "",
        "---",
        "",
        "## 2. Optimal Gate Configuration",
        "",
    ]
    if best_cfg:
        lines += [
            f"| Parameter | Current | **Optimal** |",
            f"|-----------|---------|-------------|",
            f"| ev_min    | {cfg.get('ev_min',0.03):.2f} | **{best_cfg.get('ev_min','—')}** |",
            f"| edge_min  | {cfg.get('edge_min',0.05):.2f} | **{best_cfg.get('edge_min','—')}** |",
            f"| → ROI     | — | **{best_cfg.get('roi_pct','—')}%** |",
            f"| → N bets  | — | **{best_cfg.get('n_bets','—')}** |",
        ]
    else:
        lines.append("*Insufficient data for calibration.*")

    lines += [
        "",
        "### Top 15 Gate Configs",
        "",
        calib.head(15).to_markdown(index=False) if not calib.empty else "*No data.*",
        "",
        "---",
        "",
        "## 3. Market Profitability (ROI % by League × Market)",
        "",
        pivot.to_markdown() if not pivot.empty else "*Insufficient data.*",
        "",
        "---",
        "",
        "## 4. Season-by-Season Performance",
        "",
        seasons.to_markdown(index=False) if not seasons.empty else "*No data.*",
        "",
        "---",
        "",
        "## 5. Gate Failure Analysis",
        "",
    ]

    # Gate failure breakdown
    if "reason" in df.columns:
        no_bets = df[df["decision"] == "NO BET"]
        gate_counts = no_bets["reason"].str.extract(r"(Gate-\d)")[0].value_counts()
        lines.append("| Gate | Count | % of NO BETs |")
        lines.append("|------|-------|--------------|")
        for gate, count in gate_counts.items():
            pct = count / len(no_bets) * 100 if len(no_bets) > 0 else 0
            lines.append(f"| {gate} | {count} | {pct:.1f}% |")

    lines += [
        "",
        "---",
        "",
        "## 6. Recommendations",
        "",
    ]

    # Auto-recommendations based on results
    roi_val = metrics.get("roi_pct", 0)
    wr_val  = metrics.get("win_rate", 0)

    if roi_val > 5:
        lines.append(f"✅ **TSS ROI positive** ({roi_val}%) — framework validated on synthetic odds.")
    elif roi_val > 0:
        lines.append(f"⚠️ **TSS ROI marginally positive** ({roi_val}%) — increase edge_min threshold.")
    else:
        lines.append(f"❌ **TSS ROI negative** ({roi_val}%) — review gate thresholds urgently.")

    if best_cfg:
        lines.append(f"\n📌 **Recommended gate update:** "
                     f"`ev_min={best_cfg.get('ev_min')}` / "
                     f"`edge_min={best_cfg.get('edge_min')}`")

    if not pivot.empty:
        # Best market
        flat = pivot.stack().reset_index()
        flat.columns = ["league", "market", "roi_pct"]
        flat = flat.dropna()
        if not flat.empty:
            best_row = flat.loc[flat["roi_pct"].idxmax()]
            worst_row = flat.loc[flat["roi_pct"].idxmin()]
            lines.append(f"\n📈 **Most profitable:** {best_row['league']} × "
                         f"{best_row['market']} (ROI={best_row['roi_pct']}%)")
            lines.append(f"\n📉 **Least profitable:** {worst_row['league']} × "
                         f"{worst_row['market']} (ROI={worst_row['roi_pct']}%) "
                         f"→ consider moratorium")

    report_str = "\n".join(lines)

    # Save
    report_path = REPORTS_DIR / f"backtest_report_{ts}.md"
    report_path.write_text(report_str, encoding="utf-8")
    log.info(f"\n📄 Report saved: {report_path}")

    # Also save raw signals CSV
    csv_path = REPORTS_DIR / f"signals_{ts}.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"📊 Signals CSV: {csv_path}")

    # Save gate calibration JSON
    if not calib.empty:
        calib_path = REPORTS_DIR / f"gate_calibration_{ts}.json"
        calib.head(20).to_json(calib_path, orient="records", indent=2)
        log.info(f"🔧 Gate calibration: {calib_path}")

    return report_str


# ═══════════════════════════════════════════════════════════════════════════════
# QUICK TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Load from CSV if exists, else generate dummy
    import sys
    from pathlib import Path

    csv_files = list(Path("reports").glob("signals_*.csv"))
    if csv_files:
        latest = sorted(csv_files)[-1]
        log.info(f"Loading signals from {latest}")
        df = pd.read_csv(latest)
    else:
        log.warning("No signal CSV found. Run backtest_engine.py first.")
        sys.exit(1)

    report = generate_full_report(df)
    print("\n" + "="*60)
    print(report[:3000])   # Print first 3000 chars
    print("…")
