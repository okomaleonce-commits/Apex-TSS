"""
APEX-TSS Backtesting — Metrics Engine v1.0
Objectifs:
  1. ROI global du TSS (par ligue, par saison, global)
  2. Calibration des gates (DCS threshold x EV threshold → ROI/Sharpe grid)
  3. Marchés profitables (1X2-H/D/A, BTTS, O/U par ligue)
"""

import numpy as np
import pandas as pd
import json
import logging
from typing import Dict, List, Optional, Tuple
from itertools import product

logger = logging.getLogger("APEX-TSS.Metrics")


# ─── 1. ROI Global ─────────────────────────────────────────────────────────

def compute_roi_summary(
    df: pd.DataFrame,
    only_bets: bool = True,
    group_by: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Calcule ROI, Yield, Win Rate, P&L, # bets.
    group_by: ex. ['league'], ['market'], ['league', 'market']
    """
    if only_bets:
        df_bets = df[df["signal"].str.contains("^BET$", na=False)].copy()
    else:
        df_bets = df.copy()

    if df_bets.empty:
        return pd.DataFrame()

    def roi_stats(sub: pd.DataFrame) -> Dict:
        total_staked = sub["stake"].sum()
        total_pnl = sub["pnl"].sum()
        wins = (sub["outcome"] == "WIN").sum()
        losses = (sub["outcome"] == "LOSE").sum()
        n = len(sub)
        roi = total_pnl / total_staked if total_staked > 0 else 0.0
        win_rate = wins / n if n > 0 else 0.0
        avg_odds = sub["odds"].mean()
        sharpe = _sharpe_ratio(sub["pnl"])
        return {
            "n_bets": n,
            "total_staked": round(total_staked, 2),
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(roi * 100, 2),
            "win_rate_pct": round(win_rate * 100, 2),
            "avg_odds": round(avg_odds, 3),
            "sharpe": round(sharpe, 3),
            "wins": wins,
            "losses": losses,
        }

    if not group_by:
        stats = roi_stats(df_bets)
        return pd.DataFrame([stats])

    rows = []
    for keys, sub_df in df_bets.groupby(group_by):
        row = roi_stats(sub_df)
        if isinstance(keys, str):
            keys = (keys,)
        for k, v in zip(group_by, keys):
            row[k] = v
        rows.append(row)

    return pd.DataFrame(rows).sort_values("roi_pct", ascending=False)


def _sharpe_ratio(pnl_series: pd.Series, risk_free: float = 0.0) -> float:
    if len(pnl_series) < 2:
        return 0.0
    mean = pnl_series.mean() - risk_free
    std = pnl_series.std()
    return mean / std if std > 0 else 0.0


# ─── 2. Gate Calibration Grid ──────────────────────────────────────────────

def gate_calibration_grid(
    df: pd.DataFrame,
    dcs_thresholds: List[float] = None,
    ev_thresholds: List[float] = None,
) -> pd.DataFrame:
    """
    Grid search: pour chaque (DCS_min, EV_min), calcule ROI + Sharpe + #bets.
    Permet d'identifier les seuils optimaux sans data snooping.
    
    Input: df complet avec colonnes dcs, ev, signal, pnl, stake, outcome.
    """
    if dcs_thresholds is None:
        dcs_thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    if ev_thresholds is None:
        ev_thresholds = [0.01, 0.02, 0.03, 0.05, 0.07, 0.10]

    rows = []
    for dcs_min, ev_min in product(dcs_thresholds, ev_thresholds):
        # Filtrer les matchs qui auraient passé ces seuils
        sub = df[(df["dcs"] >= dcs_min) & (df["ev"] >= ev_min)].copy()

        if sub.empty or sub["stake"].sum() == 0:
            rows.append({
                "dcs_threshold": dcs_min, "ev_threshold": ev_min,
                "n_bets": 0, "roi_pct": 0.0, "sharpe": 0.0,
                "total_pnl": 0.0, "win_rate_pct": 0.0,
            })
            continue

        n = len(sub)
        total_staked = sub["stake"].sum()
        total_pnl = sub["pnl"].sum()
        roi = total_pnl / total_staked if total_staked > 0 else 0.0
        win_rate = (sub["outcome"] == "WIN").mean()
        sharpe = _sharpe_ratio(sub["pnl"])

        rows.append({
            "dcs_threshold": dcs_min,
            "ev_threshold": ev_min,
            "n_bets": n,
            "roi_pct": round(roi * 100, 2),
            "sharpe": round(sharpe, 3),
            "total_pnl": round(total_pnl, 2),
            "win_rate_pct": round(win_rate * 100, 2),
        })

    grid_df = pd.DataFrame(rows).sort_values("sharpe", ascending=False)
    return grid_df


def optimal_gates(grid_df: pd.DataFrame, min_bets: int = 50) -> Dict:
    """Retourne les gates optimaux selon Sharpe (avec plancher de volume)."""
    filtered = grid_df[grid_df["n_bets"] >= min_bets]
    if filtered.empty:
        return {}
    best = filtered.iloc[0]
    return {
        "optimal_dcs_threshold": float(best["dcs_threshold"]),
        "optimal_ev_threshold": float(best["ev_threshold"]),
        "expected_roi_pct": float(best["roi_pct"]),
        "expected_sharpe": float(best["sharpe"]),
        "n_bets_at_optimal": int(best["n_bets"]),
    }


# ─── 3. Market Profitability Analysis ──────────────────────────────────────

def market_profitability_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse la rentabilité par marché + ligue.
    Identifie les marchés systématiquement profitables vs non profitables.
    """
    bets = df[df["signal"] == "BET"].copy() if "BET" in df.get("signal", pd.Series()).values else df.copy()
    if bets.empty:
        return pd.DataFrame()

    report = compute_roi_summary(bets, only_bets=False, group_by=["league", "market"])

    # Ajouter classification
    def classify(row):
        if row["n_bets"] < 10:
            return "INSUFFICIENT_SAMPLE"
        elif row["roi_pct"] > 5 and row["sharpe"] > 0.1:
            return "✅ PROFITABLE"
        elif row["roi_pct"] > 0:
            return "⚠️ MARGINAL"
        elif row["roi_pct"] > -5:
            return "🔴 SLIGHT LOSS"
        else:
            return "❌ AVOID"

    report["classification"] = report.apply(classify, axis=1)
    return report.sort_values(["roi_pct"], ascending=False)


def moratorium_candidates(market_report: pd.DataFrame, loss_threshold: float = -5.0) -> List[Dict]:
    """
    Identifie les combinaisons ligue+marché qui méritent un moratorium.
    Critère: ROI < loss_threshold ET sample >= 20 matchs.
    """
    candidates = market_report[
        (market_report["roi_pct"] < loss_threshold) &
        (market_report["n_bets"] >= 20)
    ]
    result = []
    for _, row in candidates.iterrows():
        result.append({
            "league": row.get("league", "?"),
            "market": row.get("market", "?"),
            "roi_pct": row["roi_pct"],
            "n_bets": row["n_bets"],
            "recommendation": f"MORATORIUM — {row['market']} interdit en {row.get('league', '?')} "
                              f"(ROI={row['roi_pct']}%, n={row['n_bets']})"
        })
    return result


# ─── 4. Equity Curve ───────────────────────────────────────────────────────

def equity_curve(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule la courbe d'équité cumulée sur le temps."""
    bets = df[df["signal"] == "BET"].sort_values("date").copy()
    if bets.empty:
        return pd.DataFrame()
    bets["cumulative_pnl"] = bets["pnl"].cumsum()
    bets["drawdown"] = bets["cumulative_pnl"] - bets["cumulative_pnl"].cummax()
    return bets[["date", "league", "market", "pnl", "cumulative_pnl", "drawdown"]]


def max_drawdown(df: pd.DataFrame) -> Dict:
    """Calcule le max drawdown absolu et relatif."""
    eq = equity_curve(df)
    if eq.empty:
        return {"max_drawdown_abs": 0.0, "max_drawdown_pct": 0.0}
    mdd_abs = float(eq["drawdown"].min())
    peak = float(eq["cumulative_pnl"].max())
    mdd_pct = (mdd_abs / peak * 100) if peak > 0 else 0.0
    return {
        "max_drawdown_abs": round(mdd_abs, 2),
        "max_drawdown_pct": round(mdd_pct, 2),
    }


# ─── 5. Rapport Consolidé ──────────────────────────────────────────────────

def generate_full_report(
    df: pd.DataFrame,
    config: dict,
    run_id: str,
    output_path: str = None
) -> Dict:
    """
    Génère le rapport complet des 3 objectifs + export JSON + CSV.
    """
    report = {
        "run_id": run_id,
        "config": config,
        "timestamp": pd.Timestamp.now().isoformat(),
    }

    # Objectif 1: ROI Global
    roi_global = compute_roi_summary(df, only_bets=True)
    report["roi_global"] = roi_global.to_dict(orient="records")

    roi_by_league = compute_roi_summary(df, only_bets=True, group_by=["league"])
    report["roi_by_league"] = roi_by_league.to_dict(orient="records")

    roi_by_market = compute_roi_summary(df, only_bets=True, group_by=["market"])
    report["roi_by_market"] = roi_by_market.to_dict(orient="records")

    # Objectif 2: Gate Calibration
    grid = gate_calibration_grid(df)
    report["gate_calibration_grid"] = grid.head(20).to_dict(orient="records")
    report["optimal_gates"] = optimal_gates(grid)

    # Objectif 3: Market profitability
    market_report = market_profitability_report(df)
    report["market_report"] = market_report.to_dict(orient="records")
    report["moratorium_candidates"] = moratorium_candidates(market_report)

    # Equity + drawdown
    report["max_drawdown"] = max_drawdown(df)

    # Summary Telegram-ready
    g = report["roi_global"][0] if report["roi_global"] else {}
    opts = report["optimal_gates"]
    mdd = report["max_drawdown"]
    report["telegram_summary"] = (
        f"🏁 *APEX-TSS Backtest [{run_id}]*\n"
        f"📊 ROI Global: *{g.get('roi_pct', 0):.1f}%*\n"
        f"🎯 Bets: {g.get('n_bets', 0)} | WR: {g.get('win_rate_pct', 0):.1f}%\n"
        f"💰 P&L: {g.get('total_pnl', 0):+.0f} | Sharpe: {g.get('sharpe', 0):.2f}\n"
        f"📉 Max DD: {mdd.get('max_drawdown_abs', 0):+.0f} ({mdd.get('max_drawdown_pct', 0):.1f}%)\n"
        f"⚙️ Optimal gates: DCS≥{opts.get('optimal_dcs_threshold', '?')} | "
        f"EV≥{opts.get('optimal_ev_threshold', '?')}\n"
        f"🔴 Moratoriums: {len(report['moratorium_candidates'])} candidats"
    )

    # Export
    if output_path:
        import os
        os.makedirs(output_path, exist_ok=True)
        with open(f"{output_path}/report_{run_id}.json", "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        df.to_csv(f"{output_path}/bets_{run_id}.csv", index=False)
        grid.to_csv(f"{output_path}/gate_grid_{run_id}.csv", index=False)
        market_report.to_csv(f"{output_path}/markets_{run_id}.csv", index=False)
        logger.info(f"[REPORT] Exporté dans {output_path}/")

    return report
