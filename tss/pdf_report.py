"""
APEX-TSS — PDF Report Generator
=================================
Produces a professional multi-page PDF backtest report:

  Page 1  — Cover + Executive Summary
  Page 2  — Equity Curve + Drawdown
  Page 3  — ROI by League × Market Heatmap
  Page 4  — Gate Calibration Grid (ev_min × edge_min)
  Page 5  — Season-by-Season Performance Table
  Page 6  — Signal Distribution + Odds/EV histograms
  Page 7  — Recommendations + Config Diff

Usage:
  from tss.pdf_report import generate_pdf_report
  generate_pdf_report(signals_df, config, output_path="reports/backtest_report.pdf")

  # Or from CLI:
  python tss/pdf_report.py --signals reports/signals_<ts>.csv
"""

import io
import sys
import logging
import argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")   # no display needed
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

from reportlab.lib                  import colors
from reportlab.lib.pagesizes        import A4
from reportlab.lib.styles           import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units            import cm, mm
from reportlab.lib.enums            import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus             import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
from reportlab.platypus.flowables   import BalancedColumns

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PDF] %(message)s")
log = logging.getLogger("pdf_report")

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# ── Brand palette ──────────────────────────────────────────────────────────────
C_DARK    = colors.HexColor("#0D1117")
C_ACCENT  = colors.HexColor("#00D4AA")   # TSS teal
C_WARN    = colors.HexColor("#F59E0B")
C_DANGER  = colors.HexColor("#EF4444")
C_GREEN   = colors.HexColor("#10B981")
C_GREY    = colors.HexColor("#6B7280")
C_LIGHT   = colors.HexColor("#F3F4F6")
C_WHITE   = colors.white
C_NAVY    = colors.HexColor("#1E3A5F")

MPL_BG    = "#0D1117"
MPL_FG    = "#E5E7EB"
MPL_TEAL  = "#00D4AA"
MPL_WARN  = "#F59E0B"
MPL_RED   = "#EF4444"
MPL_GRID  = "#1F2937"

W, H = A4   # 595.3 x 841.9 pts


# ═══════════════════════════════════════════════════════════════════════════════
# STYLE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _styles():
    base = getSampleStyleSheet()
    custom = {
        "cover_title": ParagraphStyle(
            "cover_title", fontSize=32, textColor=C_ACCENT,
            fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=6
        ),
        "cover_sub": ParagraphStyle(
            "cover_sub", fontSize=14, textColor=C_WHITE,
            fontName="Helvetica", alignment=TA_CENTER, spaceAfter=4
        ),
        "section_h": ParagraphStyle(
            "section_h", fontSize=14, textColor=C_ACCENT,
            fontName="Helvetica-Bold", spaceBefore=12, spaceAfter=6,
            borderPad=4
        ),
        "metric_label": ParagraphStyle(
            "metric_label", fontSize=9, textColor=C_GREY,
            fontName="Helvetica", alignment=TA_CENTER
        ),
        "metric_value": ParagraphStyle(
            "metric_value", fontSize=22, textColor=C_WHITE,
            fontName="Helvetica-Bold", alignment=TA_CENTER
        ),
        "body": ParagraphStyle(
            "body", fontSize=9, textColor=colors.HexColor("#D1D5DB"),
            fontName="Helvetica", spaceAfter=4, leading=13
        ),
        "caption": ParagraphStyle(
            "caption", fontSize=7.5, textColor=C_GREY,
            fontName="Helvetica-Oblique", alignment=TA_CENTER, spaceBefore=2
        ),
        "warn_box": ParagraphStyle(
            "warn_box", fontSize=8.5, textColor=C_WARN,
            fontName="Helvetica-Bold", alignment=TA_CENTER
        ),
    }
    return {**{k: base[k] for k in base.byName}, **custom}


# ═══════════════════════════════════════════════════════════════════════════════
# CHART GENERATORS (return ReportLab Image objects)
# ═══════════════════════════════════════════════════════════════════════════════

def _mpl_to_rl(fig, width_cm: float = 16, height_cm: float = 7) -> Image:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=MPL_BG, edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width_cm*cm, height=height_cm*cm)


def chart_equity_drawdown(df: pd.DataFrame) -> Image:
    bets = df[df["decision"] == "BET"].copy()
    bets["date"] = pd.to_datetime(bets["date"])
    bets = bets.sort_values("date")
    bets["cum_pnl"]   = bets["pnl_units"].cumsum()
    bets["peak"]      = bets["cum_pnl"].cummax()
    bets["drawdown"]  = bets["cum_pnl"] - bets["peak"]

    fig = plt.figure(figsize=(11, 6), facecolor=MPL_BG)
    gs  = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.08)

    # Equity curve
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor(MPL_BG)
    ax1.plot(bets["date"], bets["cum_pnl"], color=MPL_TEAL, lw=1.8, zorder=3)
    ax1.fill_between(bets["date"], 0, bets["cum_pnl"],
                     where=bets["cum_pnl"] >= 0, alpha=0.15, color=MPL_TEAL)
    ax1.fill_between(bets["date"], 0, bets["cum_pnl"],
                     where=bets["cum_pnl"] < 0,  alpha=0.15, color=MPL_RED)
    ax1.axhline(0, color=MPL_FG, lw=0.5, ls="--", alpha=0.4)
    ax1.set_ylabel("Cumulative PnL (units)", color=MPL_FG, fontsize=9)
    ax1.tick_params(colors=MPL_FG, labelsize=8)
    ax1.set_xticklabels([])
    ax1.grid(True, color=MPL_GRID, lw=0.4)
    for sp in ax1.spines.values(): sp.set_color(MPL_GRID)

    final_pnl = bets["cum_pnl"].iloc[-1] if not bets.empty else 0
    color_ann  = MPL_TEAL if final_pnl >= 0 else MPL_RED
    ax1.annotate(f"Final: {final_pnl:+.3f}u",
                 xy=(bets["date"].iloc[-1], final_pnl),
                 xytext=(-60, 10), textcoords="offset points",
                 color=color_ann, fontsize=8, fontweight="bold",
                 arrowprops=dict(arrowstyle="->", color=color_ann, lw=0.8))

    # Drawdown
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor(MPL_BG)
    ax2.fill_between(bets["date"], bets["drawdown"], 0,
                     color=MPL_RED, alpha=0.6)
    ax2.set_ylabel("Drawdown", color=MPL_FG, fontsize=8)
    ax2.tick_params(colors=MPL_FG, labelsize=7)
    ax2.grid(True, color=MPL_GRID, lw=0.4)
    for sp in ax2.spines.values(): sp.set_color(MPL_GRID)

    max_dd = bets["drawdown"].min()
    ax2.annotate(f"Max DD: {max_dd:.3f}u",
                 xy=(0.02, 0.15), xycoords="axes fraction",
                 color=MPL_WARN, fontsize=8)

    fig.suptitle("Equity Curve & Drawdown", color=MPL_FG, fontsize=11, y=1.01)
    return _mpl_to_rl(fig, 16, 8)


def chart_heatmap(df: pd.DataFrame) -> Optional[Image]:
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty or "market" not in bets.columns or "league" not in bets.columns:
        return None

    rows = []
    for (lg, mk), g in bets.groupby(["league", "market"]):
        if len(g) < 3: continue
        st = g["stake_pct"].sum()
        roi = g["pnl_units"].sum() / st if st > 0 else 0
        rows.append({"league": lg, "market": mk, "roi": roi * 100, "n": len(g)})

    if not rows:
        return None

    detail = pd.DataFrame(rows)
    pivot  = detail.pivot_table(index="league", columns="market",
                                values="roi", aggfunc="mean").fillna(0)

    # Color map: red → white → green
    cmap = LinearSegmentedColormap.from_list(
        "rwg", [MPL_RED, "#1F2937", MPL_TEAL], N=256
    )

    fig, ax = plt.subplots(figsize=(12, max(4, len(pivot)*0.6 + 1)),
                           facecolor=MPL_BG)
    ax.set_facecolor(MPL_BG)

    vmax = max(abs(pivot.values.max()), abs(pivot.values.min()), 1)
    im = ax.imshow(pivot.values, cmap=cmap, vmin=-vmax, vmax=vmax, aspect="auto")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=35, ha="right",
                       color=MPL_FG, fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, color=MPL_FG, fontsize=8)

    # Annotate cells
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            txt_col = "#000000" if abs(val) < vmax * 0.3 else MPL_FG
            ax.text(j, i, f"{val:+.1f}%", ha="center", va="center",
                    color=txt_col, fontsize=7, fontweight="bold")

    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.ax.tick_params(colors=MPL_FG, labelsize=7)
    cb.set_label("ROI %", color=MPL_FG, fontsize=8)

    ax.set_title("ROI % by League × Market", color=MPL_FG, fontsize=11, pad=10)
    for sp in ax.spines.values(): sp.set_visible(False)

    fig.tight_layout()
    return _mpl_to_rl(fig, 16, max(5, len(pivot)*0.8 + 2))


def chart_gate_calibration(df: pd.DataFrame) -> Optional[Image]:
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty:
        return None

    ev_range   = np.arange(0.01, 0.12, 0.01)
    edge_range = np.arange(0.02, 0.15, 0.01)
    grid = np.zeros((len(ev_range), len(edge_range)))

    for i, ev_t in enumerate(ev_range):
        for j, ed_t in enumerate(edge_range):
            sub = bets[(bets["ev"] >= ev_t) & (bets["edge"] >= ed_t)]
            if len(sub) < 10:
                grid[i, j] = np.nan
                continue
            st  = sub["stake_pct"].sum()
            roi = sub["pnl_units"].sum() / st * 100 if st > 0 else 0
            grid[i, j] = roi

    cmap = LinearSegmentedColormap.from_list(
        "rwg", [MPL_RED, "#111827", MPL_TEAL], N=256
    )
    vmax = np.nanmax(np.abs(grid)) if not np.all(np.isnan(grid)) else 1

    fig, ax = plt.subplots(figsize=(11, 5), facecolor=MPL_BG)
    ax.set_facecolor(MPL_BG)

    im = ax.imshow(grid, cmap=cmap, vmin=-vmax, vmax=vmax,
                   aspect="auto", origin="lower")

    ax.set_xticks(range(len(edge_range)))
    ax.set_xticklabels([f"{v:.2f}" for v in edge_range],
                       rotation=45, ha="right", color=MPL_FG, fontsize=7)
    ax.set_yticks(range(len(ev_range)))
    ax.set_yticklabels([f"{v:.2f}" for v in ev_range], color=MPL_FG, fontsize=7)
    ax.set_xlabel("edge_min", color=MPL_FG, fontsize=9)
    ax.set_ylabel("ev_min",   color=MPL_FG, fontsize=9)

    # Mark best cell
    best_idx = np.unravel_index(np.nanargmax(grid), grid.shape)
    ax.add_patch(mpatches.Rectangle(
        (best_idx[1]-0.5, best_idx[0]-0.5), 1, 1,
        linewidth=2, edgecolor=MPL_WARN, facecolor="none"
    ))

    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.ax.tick_params(colors=MPL_FG, labelsize=7)
    cb.set_label("ROI %", color=MPL_FG, fontsize=8)

    ax.set_title("Gate Calibration — ROI% by ev_min × edge_min  (★ = optimal)",
                 color=MPL_FG, fontsize=10, pad=8)
    for sp in ax.spines.values(): sp.set_visible(False)

    # Annotate optimal
    best_ev  = ev_range[best_idx[0]]
    best_ed  = edge_range[best_idx[1]]
    best_roi = grid[best_idx]
    ax.annotate(
        f"Optimal\nev={best_ev:.2f}\nedge={best_ed:.2f}\nROI={best_roi:.1f}%",
        xy=(best_idx[1], best_idx[0]),
        xytext=(best_idx[1]+1.5, best_idx[0]+1.5),
        color=MPL_WARN, fontsize=7.5,
        arrowprops=dict(arrowstyle="->", color=MPL_WARN, lw=0.8),
    )

    fig.tight_layout()
    return _mpl_to_rl(fig, 16, 7)


def chart_distributions(df: pd.DataFrame) -> Image:
    bets = df[df["decision"] == "BET"].copy()

    fig, axes = plt.subplots(1, 3, figsize=(13, 4), facecolor=MPL_BG)

    def _hist(ax, data, title, color, xlabel, bins=20):
        ax.set_facecolor(MPL_BG)
        ax.hist(data.dropna(), bins=bins, color=color, alpha=0.85, edgecolor="none")
        ax.set_title(title, color=MPL_FG, fontsize=9)
        ax.set_xlabel(xlabel, color=MPL_FG, fontsize=8)
        ax.tick_params(colors=MPL_FG, labelsize=7)
        ax.grid(True, color=MPL_GRID, lw=0.4, axis="y")
        for sp in ax.spines.values(): sp.set_color(MPL_GRID)
        mu = data.mean()
        ax.axvline(mu, color=MPL_WARN, lw=1.2, ls="--")
        ax.text(mu, ax.get_ylim()[1]*0.9, f"μ={mu:.3f}",
                color=MPL_WARN, fontsize=7, ha="left")

    _hist(axes[0], bets["ev"],   "EV Distribution",   MPL_TEAL, "Expected Value")
    _hist(axes[1], bets["odds"], "Odds Distribution",  "#818CF8", "Odds")
    _hist(axes[2], bets["edge"], "Edge Distribution",  "#FB923C", "Edge (P_synth - P_book)")

    fig.suptitle("Signal Distribution Analysis", color=MPL_FG, fontsize=11, y=1.01)
    fig.tight_layout()
    return _mpl_to_rl(fig, 16, 5.5)


def chart_market_bar(df: pd.DataFrame) -> Image:
    bets = df[df["decision"] == "BET"].copy()

    rows = []
    for market, g in bets.groupby("market"):
        st  = g["stake_pct"].sum()
        roi = g["pnl_units"].sum() / st * 100 if st > 0 else 0
        rows.append({"market": market, "roi": roi, "n": len(g)})
    if not rows:
        fig, ax = plt.subplots(figsize=(10, 3), facecolor=MPL_BG)
        ax.text(0.5, 0.5, "No BET signals to display", ha="center", va="center",
                color=MPL_FG, fontsize=12, transform=ax.transAxes)
        ax.set_facecolor(MPL_BG)
        return _mpl_to_rl(fig, 14, 3)
    mdf = pd.DataFrame(rows).sort_values("roi", ascending=True)

    fig, ax = plt.subplots(figsize=(10, max(3, len(mdf)*0.5 + 1)),
                           facecolor=MPL_BG)
    ax.set_facecolor(MPL_BG)

    bar_colors = [MPL_TEAL if v >= 0 else MPL_RED for v in mdf["roi"]]
    bars = ax.barh(mdf["market"], mdf["roi"], color=bar_colors,
                   edgecolor="none", height=0.6)

    ax.axvline(0, color=MPL_FG, lw=0.7, ls="--", alpha=0.5)
    ax.set_xlabel("ROI %", color=MPL_FG, fontsize=9)
    ax.tick_params(colors=MPL_FG, labelsize=8)
    ax.grid(True, color=MPL_GRID, lw=0.4, axis="x")
    for sp in ax.spines.values(): sp.set_color(MPL_GRID)

    for bar, (_, row) in zip(bars, mdf.iterrows()):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                f"n={row['n']}", va="center", color=MPL_FG, fontsize=7)

    ax.set_title("ROI % by Market", color=MPL_FG, fontsize=11, pad=8)
    fig.tight_layout()
    return _mpl_to_rl(fig, 14, max(3.5, len(mdf)*0.55 + 1.5))


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_TABLE_STYLE_BASE = TableStyle([
    ("BACKGROUND",   (0, 0), (-1, 0),  C_NAVY),
    ("TEXTCOLOR",    (0, 0), (-1, 0),  C_ACCENT),
    ("FONTNAME",     (0, 0), (-1, 0),  "Helvetica-Bold"),
    ("FONTSIZE",     (0, 0), (-1, 0),  8),
    ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
    ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.HexColor("#111827"),
                                        colors.HexColor("#1A2332")]),
    ("TEXTCOLOR",    (0, 1), (-1, -1), colors.HexColor("#D1D5DB")),
    ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
    ("FONTSIZE",     (0, 1), (-1, -1), 8),
    ("GRID",         (0, 0), (-1, -1), 0.3, colors.HexColor("#374151")),
    ("TOPPADDING",   (0, 0), (-1, -1), 4),
    ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
    ("LEFTPADDING",  (0, 0), (-1, -1), 6),
    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
])

def _color_row_style(table_style: TableStyle, df: pd.DataFrame, col: str,
                     good_thresh: float = 0, row_offset: int = 1):
    """Colour rows green/red based on a numeric column value."""
    for i, val in enumerate(df[col]):
        try:
            v = float(val)
            bg = colors.HexColor("#064E3B") if v > good_thresh else colors.HexColor("#7F1D1D")
            table_style.add("BACKGROUND", (0, i+row_offset), (-1, i+row_offset), bg)
        except (ValueError, TypeError):
            pass


def metrics_summary_table(metrics: Dict, st) -> Table:
    pairs = [
        ("Total Signals",    metrics.get("n_signals_total", "—")),
        ("Bets Placed",      metrics.get("n_bets", "—")),
        ("Bet Rate",         f"{metrics.get('bet_rate_pct', 0):.1f}%"),
        ("Win Rate",         f"{metrics.get('win_rate', 0)*100:.1f}%"),
        ("Total PnL",        f"{metrics.get('total_pnl', 0):+.4f} u"),
        ("ROI",              f"{metrics.get('roi_pct', 0):+.2f}%"),
        ("Avg Odds",         f"{metrics.get('avg_odds', 0):.2f}"),
        ("Sharpe (ann.)",    f"{metrics.get('sharpe_annualised', 0):.3f}"),
        ("Max Drawdown",     f"{metrics.get('max_drawdown', 0):.4f} u"),
        ("Yield / bet",      f"{metrics.get('yield_per_bet', 0):+.4f} u"),
    ]
    data = [["Metric", "Value"]]
    data += [[k, str(v)] for k, v in pairs]

    tbl_style = TableStyle(_TABLE_STYLE_BASE._cmds[:])
    # Color ROI row
    roi_row = next((i+1 for i, (k,_) in enumerate(pairs) if k == "ROI"), None)
    if roi_row:
        roi_val = metrics.get("roi_pct", 0)
        bg = colors.HexColor("#064E3B") if roi_val > 0 else colors.HexColor("#7F1D1D")
        tbl_style.add("BACKGROUND", (0, roi_row), (-1, roi_row), bg)
        tbl_style.add("TEXTCOLOR",  (1, roi_row), (1, roi_row),
                      C_GREEN if roi_val > 0 else C_DANGER)
        tbl_style.add("FONTNAME",   (1, roi_row), (1, roi_row), "Helvetica-Bold")

    return Table(data, colWidths=[7*cm, 5.5*cm], style=tbl_style,
                 hAlign="LEFT", repeatRows=1)


def season_table(df: pd.DataFrame, st) -> Optional[Table]:
    bets = df[df["decision"] == "BET"].copy()
    rows = []
    for season, g in bets.groupby("season"):
        st_   = g["stake_pct"].sum()
        roi   = g["pnl_units"].sum() / st_ * 100 if st_ > 0 else 0
        rows.append([
            season, len(g),
            f"{(g['outcome']=='WIN').mean()*100:.1f}%",
            f"{roi:+.2f}%",
            f"{g['pnl_units'].sum():+.4f}",
        ])
    if not rows:
        return None

    data = [["Season", "Bets", "Win Rate", "ROI %", "Total PnL"]] + rows
    tbl  = TableStyle(_TABLE_STYLE_BASE._cmds[:])
    _color_row_style(tbl, pd.DataFrame(rows, columns=["s","n","wr","roi","pnl"]),
                     col="roi", good_thresh=0)
    return Table(data, colWidths=[3.5*cm, 2*cm, 2.5*cm, 2.5*cm, 3*cm],
                 style=tbl, hAlign="LEFT", repeatRows=1)


def gate_top_table(df: pd.DataFrame) -> Optional[Table]:
    bets = df[df["decision"] == "BET"].copy()
    rows_out = []
    combos   = []
    for ev_t in np.arange(0.01, 0.12, 0.02):
        for ed_t in np.arange(0.02, 0.15, 0.02):
            sub = bets[(bets["ev"] >= ev_t) & (bets["edge"] >= ed_t)]
            if len(sub) < 10: continue
            st  = sub["stake_pct"].sum()
            roi = sub["pnl_units"].sum() / st * 100 if st > 0 else 0
            combos.append((roi, ev_t, ed_t, len(sub),
                           (sub["outcome"]=="WIN").mean()*100))

    combos.sort(reverse=True)
    for roi, ev_t, ed_t, n, wr in combos[:10]:
        rows_out.append([f"{ev_t:.2f}", f"{ed_t:.2f}", n,
                         f"{wr:.1f}%", f"{roi:+.2f}%"])
    if not rows_out: return None

    data = [["ev_min","edge_min","N bets","Win Rate","ROI %"]] + rows_out
    tbl  = TableStyle(_TABLE_STYLE_BASE._cmds[:])
    _color_row_style(
        tbl,
        pd.DataFrame(rows_out, columns=["ev","ed","n","wr","roi"]),
        col="roi"
    )
    return Table(data, colWidths=[2.5*cm]*5,
                 style=tbl, hAlign="LEFT", repeatRows=1)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE BACKGROUND CANVAS
# ═══════════════════════════════════════════════════════════════════════════════

def _on_page(canvas_obj, doc):
    """Dark background + header bar + page number."""
    canvas_obj.saveState()
    # Dark fill
    canvas_obj.setFillColor(C_DARK)
    canvas_obj.rect(0, 0, W, H, fill=1, stroke=0)
    # Top accent bar
    canvas_obj.setFillColor(C_ACCENT)
    canvas_obj.rect(0, H - 8*mm, W, 8*mm, fill=1, stroke=0)
    canvas_obj.setFillColor(C_DARK)
    canvas_obj.setFont("Helvetica-Bold", 8)
    canvas_obj.drawString(1.5*cm, H - 5.5*mm, "APEX-TSS  ·  Walk-Forward Backtest Report")
    canvas_obj.setFillColor(C_WHITE)
    canvas_obj.drawRightString(W - 1.5*cm, H - 5.5*mm,
                                datetime.utcnow().strftime("%Y-%m-%d"))
    # Footer
    canvas_obj.setFillColor(C_GREY)
    canvas_obj.setFont("Helvetica", 7)
    canvas_obj.drawCentredString(W/2, 0.6*cm,
                                  f"Page {doc.page}  ·  APEX-TSS Proprietary Analytics")
    canvas_obj.restoreState()


def _on_cover_page(canvas_obj, doc):
    """Full dark cover without header bar."""
    canvas_obj.saveState()
    canvas_obj.setFillColor(C_DARK)
    canvas_obj.rect(0, 0, W, H, fill=1, stroke=0)
    # Side accent
    canvas_obj.setFillColor(C_ACCENT)
    canvas_obj.rect(0, 0, 6*mm, H, fill=1, stroke=0)
    canvas_obj.restoreState()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_pdf_report(
    df:          pd.DataFrame,
    config:      Dict = None,
    output_path: str  = None,
) -> str:
    config = config or {}
    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if output_path is None:
        output_path = str(REPORTS_DIR / f"APEX_TSS_Backtest_{ts}.pdf")

    log.info(f"\n📄 Generating PDF report: {output_path}")

    st = _styles()

    # ── Compute metrics ───────────────────────────────────────────────────────
    from tss.results_analyzer import compute_roi_metrics
    metrics = compute_roi_metrics(df)
    bets    = df[df["decision"] == "BET"].copy()

    # ── Build document ────────────────────────────────────────────────────────
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=1.5*cm,  bottomMargin=1.5*cm,
        title="APEX-TSS Walk-Forward Backtest Report",
        author="APEX-TSS Analytics Engine",
    )

    story = []

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 1 — COVER
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 5*cm))
    story.append(Paragraph("APEX-TSS", st["cover_title"]))
    story.append(Paragraph("Walk-Forward Backtest Report", st["cover_sub"]))
    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="60%", thickness=1, color=C_ACCENT,
                             hAlign="CENTER"))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph(
        f"Generated: {datetime.utcnow().strftime('%d %B %Y — %H:%M UTC')}",
        st["cover_sub"]
    ))

    leagues  = sorted(df["league"].unique()) if "league" in df.columns else []
    seasons  = sorted(df["season"].unique()) if "season" in df.columns else []
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(f"Leagues: {' · '.join(leagues)}", st["body"]))
    story.append(Paragraph(f"Seasons: {' · '.join(seasons)}", st["body"]))

    # KPI cards row (3-column table)
    roi_val  = metrics.get("roi_pct", 0)
    roi_col  = "#10B981" if roi_val >= 0 else "#EF4444"
    kpi_data = [[
        Paragraph("ROI",            st["metric_label"]),
        Paragraph("Win Rate",       st["metric_label"]),
        Paragraph("Sharpe",         st["metric_label"]),
    ],[
        Paragraph(f'<font color="{roi_col}">{roi_val:+.2f}%</font>', st["metric_value"]),
        Paragraph(f"{metrics.get('win_rate',0)*100:.1f}%", st["metric_value"]),
        Paragraph(f"{metrics.get('sharpe_annualised',0):.3f}", st["metric_value"]),
    ],[
        Paragraph(f"n={metrics.get('n_bets',0)} bets", st["metric_label"]),
        Paragraph(f"n={metrics.get('n_wins',0)} wins", st["metric_label"]),
        Paragraph("annualised",     st["metric_label"]),
    ]]
    kpi_tbl = Table(kpi_data, colWidths=[5.5*cm]*3,
                    style=TableStyle([
                        ("BACKGROUND", (0,0),(-1,-1), colors.HexColor("#111827")),
                        ("BOX",        (0,0),(-1,-1), 0.5, C_ACCENT),
                        ("INNERGRID",  (0,0),(-1,-1), 0.3, colors.HexColor("#374151")),
                        ("ALIGN",      (0,0),(-1,-1), "CENTER"),
                        ("VALIGN",     (0,0),(-1,-1), "MIDDLE"),
                        ("TOPPADDING", (0,0),(-1,-1), 8),
                        ("BOTTOMPADDING",(0,0),(-1,-1),8),
                    ]))
    story.append(Spacer(1, 2*cm))
    story.append(kpi_tbl)

    # Synthetic odds warning
    if "odds_source" in df.columns:
        n_synth = (df["odds_source"] == "synthetic_DC").sum()
        if n_synth > 0:
            story.append(Spacer(1, 0.5*cm))
            story.append(Paragraph(
                f"⚠  {n_synth} matches used synthetic odds (Dixon-Coles). "
                f"These signals are LOW CONFIDENCE and flagged separately.",
                st["warn_box"]
            ))

    story.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 2 — EXECUTIVE SUMMARY + EQUITY CURVE
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("1. Executive Summary", st["section_h"]))
    story.append(metrics_summary_table(metrics, st))
    story.append(Spacer(1, 0.5*cm))

    if not bets.empty and "date" in bets.columns:
        story.append(Paragraph("2. Equity Curve & Drawdown", st["section_h"]))
        eq_chart = chart_equity_drawdown(df)
        story.append(eq_chart)
        story.append(Paragraph(
            "Cumulative PnL in units staked. Drawdown = distance from rolling peak.",
            st["caption"]
        ))

    story.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 3 — HEATMAP + MARKET BAR
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("3. Profitability by League × Market", st["section_h"]))
    hmap = chart_heatmap(df)
    if hmap:
        story.append(hmap)
        story.append(Paragraph(
            "Green = positive ROI. Red = negative ROI. Min 3 bets per cell.",
            st["caption"]
        ))
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("4. ROI by Market (all leagues combined)", st["section_h"]))
    story.append(chart_market_bar(df))
    story.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 4 — GATE CALIBRATION
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("5. Gate Calibration Grid", st["section_h"]))
    story.append(Paragraph(
        "Grid search over ev_min × edge_min. Orange box = optimal configuration. "
        "Grey cells = fewer than 10 bets (excluded).",
        st["body"]
    ))
    gcal = chart_gate_calibration(df)
    if gcal:
        story.append(gcal)

    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Top 10 Gate Configurations", st["section_h"]))
    gt = gate_top_table(df)
    if gt:
        story.append(gt)
    story.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 5 — SEASON TABLE + SIGNAL DISTRIBUTIONS
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("6. Season-by-Season Performance", st["section_h"]))
    st_tbl = season_table(df, st)
    if st_tbl:
        story.append(st_tbl)

    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("7. Signal Distribution Analysis", st["section_h"]))
    story.append(chart_distributions(df))
    story.append(Paragraph(
        "Dashed yellow line = mean. Distributions across all BET signals.",
        st["caption"]
    ))
    story.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    # PAGE 6 — RECOMMENDATIONS + CONFIG
    # ─────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("8. Recommendations", st["section_h"]))

    roi_val  = metrics.get("roi_pct", 0)
    wr_val   = metrics.get("win_rate", 0) * 100
    sh_val   = metrics.get("sharpe_annualised", 0)
    dd_val   = metrics.get("max_drawdown", 0)

    recs = []
    if roi_val > 5:
        recs.append("✅  ROI >5% — framework validated. Consider increasing max_stake_pct slightly.")
    elif roi_val > 0:
        recs.append("⚠   ROI marginally positive. Raise edge_min threshold by +0.01 increments.")
    else:
        recs.append("❌  ROI negative. Review gate thresholds and market moratoriums urgently.")

    if wr_val < 45:
        recs.append("⚠   Win rate <45% — check if markets with low P_synth are leaking through Gate-2.")
    if sh_val < 0.5:
        recs.append("⚠   Sharpe <0.5 — high variance relative to returns. Reduce max_stake_pct.")
    if abs(dd_val) > 0.15:
        recs.append("❌  Max drawdown >15% of bankroll — tighten Kelly fraction (try 0.15).")

    recs.append(f"📌  Current config: ev_min={config.get('ev_min',0.03):.2f} / "
                f"edge_min={config.get('edge_min',0.05):.2f} / "
                f"kelly={config.get('kelly_fraction',0.25):.2f}")

    for rec in recs:
        story.append(Paragraph(rec, st["body"]))
        story.append(Spacer(1, 0.2*cm))

    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("9. Active Configuration", st["section_h"]))
    cfg_rows = [["Parameter", "Value"]]
    for k, v in config.items():
        cfg_rows.append([str(k), str(v)])
    cfg_tbl = Table(cfg_rows, colWidths=[6*cm, 5*cm],
                    style=_TABLE_STYLE_BASE, hAlign="LEFT", repeatRows=1)
    story.append(cfg_tbl)

    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_GREY))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        "APEX-TSS Analytics Engine  ·  Dixon-Coles + Monte Carlo + Shin Demarg  ·  "
        "Confidential — Internal Use Only",
        st["caption"]
    ))

    # ── Build PDF ─────────────────────────────────────────────────────────────
    doc.build(story,
              onFirstPage=_on_cover_page,
              onLaterPages=_on_page)

    log.info(f"✅ PDF saved: {output_path}  ({Path(output_path).stat().st_size//1024} KB)")
    return output_path


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="APEX-TSS PDF Report Generator")
    parser.add_argument("--signals", required=True,
                        help="Path to signals CSV (output of backtesting.py)")
    parser.add_argument("--config",  default="config.json")
    parser.add_argument("--output",  default=None)
    args = parser.parse_args()

    df = pd.read_csv(args.signals)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    config = {}
    if Path(args.config).exists():
        import json
        with open(args.config) as f:
            config = json.load(f)

    path = generate_pdf_report(df, config=config, output_path=args.output)
    print(f"\n✅ Report: {path}")
