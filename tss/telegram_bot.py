"""
APEX-TSS — Telegram Report Bot
================================
Envoie un résumé structuré après chaque backtest.

Format du message:
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🤖 APEX-TSS | BACKTEST REPORT
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📅 Date · Ligues · Saisons
  
  📊 PERFORMANCE GLOBALE
  ROI / Win Rate / Sharpe / Drawdown / N bets
  
  🏆 TOP 3 MARCHÉS
  Meilleurs ROI par marché
  
  ❌ PIRES 3 MARCHÉS
  (→ moratoriums suggérés)
  
  ⚙️ GATE OPTIMAL
  ev_min / edge_min → ROI optimal
  
  🔔 ALERTES
  Diagnostics automatiques
  
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━

Usage:
  from tss.telegram_bot import send_backtest_report
  send_backtest_report(signals_df, metrics, config)

  # Ou CLI:
  python tss/telegram_bot.py --signals reports/signals_<ts>.csv
"""

import json
import logging
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TG-BOT] %(message)s")
log = logging.getLogger("telegram_bot")

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN = "8317486741:AAGvBTv-Id5Qr48JBaq-RXyUAGZQfw7Z5dE"
TG_API    = f"https://api.telegram.org/bot{BOT_TOKEN}"

CONFIG_PATH = Path("config.json")


def _load_chat_id() -> Optional[str]:
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text())
        cid = cfg.get("telegram", {}).get("chat_id") or cfg.get("chat_id")
        if cid:
            return str(cid)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# CORE SEND FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def send_message(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
    try:
        r = requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=15
        )
        if r.status_code == 200 and r.json().get("ok"):
            log.info(f"✅ Message sent to {chat_id}")
            return True
        log.error(f"Telegram error: {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Send failed: {e}")
        return False


def send_document(chat_id: str, file_path: str, caption: str = "") -> bool:
    try:
        with open(file_path, "rb") as f:
            r = requests.post(
                f"{TG_API}/sendDocument",
                data={"chat_id": chat_id, "caption": caption,
                      "parse_mode": "HTML"},
                files={"document": f},
                timeout=60
            )
        if r.status_code == 200 and r.json().get("ok"):
            log.info(f"✅ Document sent: {file_path}")
            return True
        log.error(f"Document error: {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Document send failed: {e}")
        return False


def send_photo(chat_id: str, image_bytes: bytes, caption: str = "") -> bool:
    try:
        r = requests.post(
            f"{TG_API}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("chart.png", image_bytes, "image/png")},
            timeout=30
        )
        if r.status_code == 200 and r.json().get("ok"):
            log.info("✅ Photo sent")
            return True
        log.error(f"Photo error: {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Photo send failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def _roi_emoji(roi: float) -> str:
    if roi > 8:  return "🚀"
    if roi > 3:  return "✅"
    if roi > 0:  return "🟡"
    if roi > -5: return "⚠️"
    return "❌"


def _verdict(metrics: Dict) -> str:
    roi = metrics.get("roi_pct", 0)
    wr  = metrics.get("win_rate", 0) * 100
    sh  = metrics.get("sharpe_annualised", 0)
    if roi > 5 and sh > 0.5:   return "✅ FRAMEWORK VALIDÉ"
    if roi > 0:                 return "🟡 MARGINALEMENT POSITIF"
    if roi > -3:                return "⚠️ NÉGATIF — RÉVISION GATES"
    return "❌ ÉCHEC — MORATORIUMS REQUIS"


def _top_markets(df: pd.DataFrame, n: int = 3, worst: bool = False) -> List[Dict]:
    bets = df[df["decision"] == "BET"].copy()
    rows = []
    for market, g in bets.groupby("market"):
        if len(g) < 5: continue
        st  = g["stake_pct"].sum()
        roi = g["pnl_units"].sum() / st * 100 if st > 0 else 0
        rows.append({"market": market, "roi": roi, "n": len(g),
                     "wr": (g["outcome"] == "WIN").mean() * 100})
    rows.sort(key=lambda x: x["roi"], reverse=not worst)
    return rows[:n]


def _gate_optimal(df: pd.DataFrame) -> Optional[Dict]:
    bets = df[df["decision"] == "BET"].copy()
    if bets.empty: return None
    best = {"roi": -999}
    for ev_t in np.arange(0.01, 0.12, 0.01):
        for ed_t in np.arange(0.02, 0.15, 0.01):
            sub = bets[(bets["ev"] >= ev_t) & (bets["edge"] >= ed_t)]
            if len(sub) < 10: continue
            st  = sub["stake_pct"].sum()
            roi = sub["pnl_units"].sum() / st * 100 if st > 0 else 0
            if roi > best["roi"]:
                best = {"ev_min": round(ev_t, 2), "edge_min": round(ed_t, 2),
                        "roi": round(roi, 2), "n": len(sub)}
    return best if best["roi"] > -999 else None


def _alerts(metrics: Dict, df: pd.DataFrame) -> List[str]:
    alerts = []
    roi = metrics.get("roi_pct", 0)
    wr  = metrics.get("win_rate", 0) * 100
    sh  = metrics.get("sharpe_annualised", 0)
    dd  = abs(metrics.get("max_drawdown", 0))

    if roi < 0:
        alerts.append("🔴 ROI négatif → revoir ev_min / edge_min")
    if wr < 43:
        alerts.append("🔴 Win rate <43% → Gate-2 edge trop bas")
    if sh < 0.3:
        alerts.append("🟡 Sharpe faible → réduire max_stake_pct")
    if dd > 0.15:
        alerts.append("🔴 Drawdown >15% → Kelly fraction à baisser")

    # Markets with negative ROI → moratorium candidates
    worst = _top_markets(df, n=3, worst=True)
    for m in worst:
        if m["roi"] < -5:
            alerts.append(f"⛔ Moratorium suggéré: {m['market']} (ROI={m['roi']:.1f}%)")

    if not alerts:
        alerts.append("✅ Aucune alerte critique")

    return alerts


def _season_lines(df: pd.DataFrame) -> str:
    bets = df[df["decision"] == "BET"].copy()
    lines = []
    for season, g in sorted(bets.groupby("season")):
        st  = g["stake_pct"].sum()
        roi = g["pnl_units"].sum() / st * 100 if st > 0 else 0
        em  = "✅" if roi > 0 else "❌"
        lines.append(f"  {em} {season}: ROI={roi:+.1f}%  n={len(g)}")
    return "\n".join(lines) if lines else "  —"


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN REPORT COMPOSER
# ═══════════════════════════════════════════════════════════════════════════════

def build_report_message(
    df:      pd.DataFrame,
    metrics: Dict,
    config:  Dict = None,
) -> str:
    config   = config or {}
    ts       = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")
    leagues  = sorted(df["league"].unique()) if "league" in df.columns else []
    seasons  = sorted(df["season"].unique()) if "season" in df.columns else []
    verdict  = _verdict(metrics)
    top3     = _top_markets(df, 3, worst=False)
    worst3   = _top_markets(df, 3, worst=True)
    gate_opt = _gate_optimal(df)
    alerts   = _alerts(metrics, df)

    roi_val  = metrics.get("roi_pct", 0)
    roi_em   = _roi_emoji(roi_val)
    n_bets   = metrics.get("n_bets", 0)
    n_total  = metrics.get("n_signals_total", 0)
    bet_rate = metrics.get("bet_rate_pct", 0)

    # Synthetic odds warning
    synth_warn = ""
    if "odds_source" in df.columns:
        n_synth = (df["odds_source"] == "synthetic_DC").sum()
        if n_synth > 0:
            synth_warn = f"\n⚠️ <b>{n_synth} matchs avec cotes synthétiques</b> (DC) — faible confiance\n"

    # Top markets block
    def market_block(markets, header):
        if not markets:
            return f"{header}\n  —"
        lines = [header]
        for m in markets:
            em = "📈" if m["roi"] >= 0 else "📉"
            lines.append(
                f"  {em} <b>{m['market']}</b>: ROI={m['roi']:+.1f}%"
                f"  WR={m['wr']:.0f}%  n={m['n']}"
            )
        return "\n".join(lines)

    # Gate optimal block
    if gate_opt and gate_opt.get("roi", -999) > -999:
        gate_block = (
            f"⚙️ <b>GATE OPTIMAL</b>\n"
            f"  ev_min=<code>{gate_opt['ev_min']}</code>  "
            f"edge_min=<code>{gate_opt['edge_min']}</code>\n"
            f"  → ROI={gate_opt['roi']:+.1f}%  n={gate_opt['n']} bets\n"
            f"  (actuel: ev={config.get('ev_min',0.03):.2f} / "
            f"edge={config.get('edge_min',0.05):.2f})"
        )
    else:
        gate_block = "⚙️ <b>GATE OPTIMAL</b>\n  Données insuffisantes"

    msg = f"""━━━━━━━━━━━━━━━━━━━━━━━━━
🤖 <b>APEX-TSS | BACKTEST REPORT</b>
━━━━━━━━━━━━━━━━━━━━━━━━━
📅 {ts}
🌍 {' · '.join(leagues)}
📆 {' · '.join(seasons)}
{synth_warn}
<b>{verdict}</b>

📊 <b>PERFORMANCE GLOBALE</b>
  {roi_em} ROI: <b>{roi_val:+.2f}%</b>
  🎯 Win Rate: <b>{metrics.get('win_rate',0)*100:.1f}%</b>
  📐 Sharpe: <b>{metrics.get('sharpe_annualised',0):.3f}</b>
  📉 Max DD: <b>{metrics.get('max_drawdown',0):.4f} u</b>
  🎲 Bets: <b>{n_bets}</b> / {n_total} signaux ({bet_rate:.1f}%)
  💰 PnL total: <b>{metrics.get('total_pnl',0):+.4f} u</b>
  📊 Yield/bet: <b>{metrics.get('yield_per_bet',0):+.4f} u</b>

{market_block(top3,  "🏆 <b>TOP 3 MARCHÉS</b>")}

{market_block(worst3,"❌ <b>PIRES 3 MARCHÉS</b>")}

📆 <b>PAR SAISON</b>
{_season_lines(df)}

{gate_block}

🔔 <b>ALERTES</b>
{chr(10).join("  " + a for a in alerts)}

━━━━━━━━━━━━━━━━━━━━━━━━━
<i>APEX-TSS Analytics Engine</i>
<i>Dixon-Coles + Shin Demarg + Kelly</i>"""

    return msg.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

def send_backtest_report(
    df:       pd.DataFrame,
    metrics:  Dict,
    config:   Dict     = None,
    chat_id:  str      = None,
    pdf_path: str      = None,
) -> bool:
    """
    Sends full backtest report to Telegram.

    Parameters
    ----------
    df       : signals DataFrame (output of BacktestRunner)
    metrics  : dict from compute_roi_metrics()
    config   : active config dict
    chat_id  : Telegram chat_id (reads from config.json if None)
    pdf_path : path to PDF report file (sent as document if provided)
    """
    cid = chat_id or _load_chat_id()
    if not cid:
        log.error("No chat_id found. Set in config.json → telegram.chat_id")
        return False

    config = config or {}

    # 1. Text summary
    msg = build_report_message(df, metrics, config)
    ok  = send_message(cid, msg)

    # 2. PDF document (optional)
    if pdf_path and Path(pdf_path).exists():
        roi_val = metrics.get("roi_pct", 0)
        caption = (f"📄 APEX-TSS Backtest Report\n"
                   f"ROI: {roi_val:+.2f}% | "
                   f"Bets: {metrics.get('n_bets',0)}")
        send_document(cid, pdf_path, caption=caption)

    return ok


def get_chat_id(timeout: int = 30) -> Optional[str]:
    """
    Helper: fetches your chat_id by reading /getUpdates.
    Send any message to your bot first, then call this.
    """
    try:
        r = requests.get(f"{TG_API}/getUpdates", timeout=timeout)
        data = r.json()
        if data.get("ok") and data["result"]:
            last = data["result"][-1]
            cid  = str(last["message"]["chat"]["id"])
            name = last["message"]["chat"].get("first_name", "")
            log.info(f"Found chat_id: {cid} ({name})")
            return cid
        log.warning("No updates found. Send a message to your bot first.")
        return None
    except Exception as e:
        log.error(f"getUpdates failed: {e}")
        return None


def test_connection(chat_id: str) -> bool:
    """Sends a test ping to verify bot + chat_id."""
    msg = (
        "🤖 <b>APEX-TSS Bot — Connexion OK</b>\n\n"
        f"✅ Bot actif\n"
        f"📅 {datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}\n\n"
        "<i>Le rapport backtest sera envoyé ici après chaque run.</i>"
    )
    return send_message(chat_id, msg)


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO-HOOK: patch backtesting.py pipeline to call this automatically
# ═══════════════════════════════════════════════════════════════════════════════

def hook_into_pipeline(results_df: pd.DataFrame, config: Dict,
                       pdf_path: str = None, chat_id: str = None):
    """
    Drop-in call at end of run_complete_pipeline() or run_with_real_odds().
    Computes metrics internally — no extra work needed in backtesting.py.
    """
    try:
        from tss.results_analyzer import compute_roi_metrics
        metrics = compute_roi_metrics(results_df)
        send_backtest_report(
            df=results_df,
            metrics=metrics,
            config=config,
            chat_id=chat_id,
            pdf_path=pdf_path,
        )
    except Exception as e:
        log.error(f"hook_into_pipeline failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="APEX-TSS Telegram Report Bot")
    parser.add_argument("--signals",    help="Path to signals CSV")
    parser.add_argument("--pdf",        help="Path to PDF report (optional)")
    parser.add_argument("--chat-id",    help="Telegram chat_id")
    parser.add_argument("--get-chat-id",action="store_true",
                        help="Fetch your chat_id (send a message to bot first)")
    parser.add_argument("--test",       action="store_true",
                        help="Send a test ping to verify connection")
    parser.add_argument("--config",     default="config.json")
    args = parser.parse_args()

    # Load config
    cfg = {}
    if Path(args.config).exists():
        cfg = json.loads(Path(args.config).read_text())

    cid = args.chat_id or cfg.get("telegram", {}).get("chat_id") or cfg.get("chat_id")

    # ── get-chat-id ───────────────────────────────────────────────────────────
    if args.get_chat_id:
        print("\nSend any message to your bot, then press Enter...")
        input()
        cid = get_chat_id()
        if cid:
            print(f"\n✅ chat_id: {cid}")
            print(f"Add to config.json:")
            print(json.dumps({"telegram": {"chat_id": cid}}, indent=2))
        sys.exit(0)

    # ── test ping ─────────────────────────────────────────────────────────────
    if args.test:
        if not cid:
            print("❌ No chat_id. Run: python tss/telegram_bot.py --get-chat-id")
            sys.exit(1)
        ok = test_connection(cid)
        print("✅ Test sent!" if ok else "❌ Failed — check token / chat_id")
        sys.exit(0)

    # ── send report ───────────────────────────────────────────────────────────
    if not args.signals:
        # Try latest signals CSV automatically
        csvs = sorted(Path("reports").glob("signals_*.csv"))
        if not csvs:
            print("❌ No signals CSV. Run backtesting.py first.")
            sys.exit(1)
        args.signals = str(csvs[-1])
        print(f"Auto-selected: {args.signals}")

    if not cid:
        print("❌ No chat_id. Run --get-chat-id first.")
        sys.exit(1)

    df = pd.read_csv(args.signals)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    from tss.results_analyzer import compute_roi_metrics
    metrics = compute_roi_metrics(df)

    ok = send_backtest_report(
        df=df, metrics=metrics, config=cfg,
        chat_id=cid, pdf_path=args.pdf
    )
    print("✅ Report sent!" if ok else "❌ Send failed.")
