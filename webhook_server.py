"""
APEX-TSS — Telegram Webhook Server (Render.com)
"""

import re
import os
import json
import logging
import threading
import subprocess
from pathlib import Path
from datetime import datetime

import requests
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVER] %(message)s")
log = logging.getLogger("webhook_server")

# ── Config ─────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN",   "8317486741:AAGvBTv-Id5Qr48JBaq-RXyUAGZQfw7Z5dE")
CHAT_ID     = os.environ.get("CHAT_ID",     "5484281251")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://apex-tss.onrender.com")
PORT        = int(os.environ.get("PORT",    10000))
TG_API      = f"https://api.telegram.org/bot{BOT_TOKEN}"

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# ── Deduplication: track processed update_ids ──────────────────────────────────
_processed_updates = set()

# ── Flask app — MUST be before any @app.route ──────────────────────────────────
app = Flask(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def tg_send(chat_id, text, parse_mode="HTML"):
    try:
        r = requests.post(f"{TG_API}/sendMessage", json={
            "chat_id": chat_id, "text": text,
            "parse_mode": parse_mode, "disable_web_page_preview": True
        }, timeout=10)
        return r.status_code == 200 and r.json().get("ok")
    except Exception as e:
        log.error(f"tg_send: {e}")
        return False

def tg_send_doc(chat_id, file_path, caption=""):
    try:
        with open(file_path, "rb") as f:
            r = requests.post(f"{TG_API}/sendDocument",
                data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"document": f}, timeout=60)
        return r.status_code == 200 and r.json().get("ok")
    except Exception as e:
        log.error(f"tg_send_doc: {e}")
        return False

def register_webhook():
    url = f"{WEBHOOK_URL}/webhook/{BOT_TOKEN}"
    try:
        r = requests.post(f"{TG_API}/setWebhook", json={"url": url}, timeout=10)
        ok = r.status_code == 200 and r.json().get("ok")
        log.info(f"Webhook {'OK' if ok else 'FAIL'}: {url}")
        return ok
    except Exception as e:
        log.error(f"register_webhook: {e}")
        return False

def _is_match(text):
    return bool(re.search(
        r"\s+vs\.?\s+|\s+v\.?\s+|\s+contre\s+|\w\s*[-]\s*\w",
        text, re.IGNORECASE
    ))


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_start(cid):
    tg_send(cid,
        "🤖 <b>APEX-TSS Bot actif</b>\n\n"
        "Envoie un match pour l'analyser:\n"
        "<code>PSG vs Lyon</code>\n"
        "<code>/analyse 11/04 PL Arsenal Bournemouth</code>\n\n"
        "/help pour toutes les commandes"
    )

def cmd_help(cid):
    tg_send(cid,
        "📖 <b>APEX-TSS — Commandes</b>\n\n"
        "/start — Démarrer\n"
        "/status — Statut système\n"
        "/report — Dernier rapport backtest\n"
        "/backtest — Lancer un backtest rapide\n"
        "/gates — Gates actives\n"
        "/analyse [match] — Analyser un match\n"
        "/help — Ce message\n\n"
        "💡 <b>Formats acceptés:</b>\n"
        "<code>PSG vs Lyon</code>\n"
        "<code>/analyse 11/04 11:30 PL Arsenal Bournemouth</code>\n"
        "<code>20/04 20:45 Serie A Napoli Lazio</code>"
    )

def cmd_status(cid):
    csvs = sorted(REPORTS_DIR.glob("signals_*.csv"))
    if csvs:
        mtime = datetime.utcfromtimestamp(csvs[-1].stat().st_mtime)
        age   = (datetime.utcnow() - mtime).total_seconds() / 3600
        tg_send(cid,
            f"⚡ <b>Statut APEX-TSS</b>\n\n"
            f"🟢 En ligne\n"
            f"📊 Dernier backtest: {mtime.strftime('%d/%m/%Y %H:%M')} ({age:.1f}h)\n"
            f"📄 Rapports PDF: {len(list(REPORTS_DIR.glob('*.pdf')))}\n\n"
            f"<i>{datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}</i>"
        )
    else:
        tg_send(cid, "🟢 En ligne — Aucun backtest encore. Envoie /backtest")

def cmd_report(cid):
    csvs = sorted(REPORTS_DIR.glob("signals_*.csv"))
    if not csvs:
        tg_send(cid, "❌ Aucun rapport. Lance /backtest d'abord.")
        return
    try:
        import pandas as pd
        from tss.results_analyzer import compute_roi_metrics
        from tss.telegram_bot import build_report_message
        df = pd.read_csv(str(csvs[-1]))
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        metrics = compute_roi_metrics(df)
        tg_send(cid, build_report_message(df, metrics))
        pdfs = sorted(REPORTS_DIR.glob("*.pdf"))
        if pdfs:
            tg_send_doc(cid, str(pdfs[-1]),
                        caption=f"📄 ROI: {metrics.get('roi_pct',0):+.2f}%")
    except Exception as e:
        tg_send(cid, f"❌ Erreur: {e}")

def cmd_gates(cid):
    try:
        cfg = json.loads(Path("config.json").read_text())
        bk  = cfg.get("backtest", cfg.get("gates", {}))
        tg_send(cid,
            "⚙️ <b>Gates actives</b>\n\n"
            f"Gate-0 DCS:  <code>{bk.get('dcs_min', 0.60)}</code>\n"
            f"Gate-1 EV:   <code>{bk.get('ev_min',  0.03)}</code>\n"
            f"Gate-2 Edge: <code>{bk.get('edge_min',0.05)}</code>\n"
            f"Gate-3 Odds: <code>[{bk.get('odds_min',1.40)} – {bk.get('odds_max',4.50)}]</code>\n\n"
            f"Kelly:     <code>{bk.get('kelly_fraction',0.25)}</code>\n"
            f"Max stake: <code>{bk.get('max_stake_pct',0.03)*100:.1f}%</code>"
        )
    except Exception as e:
        tg_send(cid, f"❌ config.json: {e}")

def cmd_backtest(cid):
    tg_send(cid, "⏳ <b>Backtest lancé (~2 min)...</b>")
    def run():
        try:
            res = subprocess.run(["python","backtesting.py","--smoke-test"],
                                 capture_output=True, text=True, timeout=300)
            tg_send(cid, "✅ Backtest terminé." if res.returncode==0
                    else f"❌ Erreur:\n<code>{res.stderr[-400:]}</code>")
        except Exception as e:
            tg_send(cid, f"❌ {e}")
    threading.Thread(target=run, daemon=True).start()

def cmd_analyze(cid, text):
    match_text = re.sub(
        r"^/(analys[ei]|analyze|match|tss)\s*", "", text, flags=re.IGNORECASE
    ).strip()
    if not match_text:
        tg_send(cid, "⚽ Exemple: <code>/analyse 11/04 PL Arsenal Bournemouth</code>")
        return
    tg_send(cid, "⏳ Analyse TSS en cours...")
    try:
        from tss.match_analyzer import analyze_match_text
        result = analyze_match_text(match_text)
        # Split if too long (Telegram max 4096 chars)
        if len(result) <= 4000:
            ok = tg_send(cid, result)
            if not ok:
                log.error(f"tg_send failed for analysis result (len={len(result)})")
                tg_send(cid, "❌ Erreur envoi du résultat. Réessaie.")
        else:
            # Send in two parts
            mid = result.rfind("\n", 0, 3800)
            tg_send(cid, result[:mid])
            tg_send(cid, result[mid:])
    except Exception as e:
        log.error(f"cmd_analyze error: {e}", exc_info=True)
        tg_send(cid, f"❌ Erreur analyse: <code>{str(e)[:200]}</code>")


# ═══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "online", "service": "APEX-TSS",
                    "timestamp": datetime.utcnow().isoformat()})

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"ok": True})

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    # ── Return 200 IMMEDIATELY — prevents Telegram retry loop ─────────────────
    update_id = data.get("update_id")
    if update_id in _processed_updates:
        return jsonify({"ok": True})   # duplicate — ignore silently
    _processed_updates.add(update_id)
    # Keep set small
    if len(_processed_updates) > 500:
        _processed_updates.clear()

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return jsonify({"ok": True})

    cid  = str(msg["chat"]["id"])
    text = msg.get("text", "").strip()

    # Process in background so we return 200 before Telegram's 5s timeout
    threading.Thread(
        target=_handle_message,
        args=(cid, text),
        daemon=True
    ).start()

    return jsonify({"ok": True})  # Immediate 200


def _handle_message(cid: str, text: str):
    """Process message in background thread."""
    if cid != CHAT_ID:
        tg_send(cid, "⛔ Accès non autorisé.")
        return

    log.info(f"CMD: {text!r}")
    cmd = text.split()[0].lower().split("@")[0] if text else ""

    if   cmd == "/start":    cmd_start(cid)
    elif cmd == "/help":     cmd_help(cid)
    elif cmd == "/status":   cmd_status(cid)
    elif cmd == "/report":   cmd_report(cid)
    elif cmd == "/backtest": cmd_backtest(cid)
    elif cmd == "/gates":    cmd_gates(cid)
    elif cmd in ("/analyze", "/analyse", "/match", "/tss"):
        cmd_analyze(cid, text)
    elif _is_match(text):
        cmd_analyze(cid, text)
    else:
        tg_send(cid, "❓ /help pour la liste. Ou envoie: <code>PSG vs Lyon</code>")


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info(f"Starting APEX-TSS on port {PORT}")

    # Pre-load DC models at startup (avoids fitting delay on first request)
    def _preload_models():
        try:
            from tss.match_analyzer import _get_dc_model
            for lg in ["EPL", "Serie A", "La Liga", "Bundesliga", "Ligue 1"]:
                _get_dc_model(lg)
            log.info("✅ DC models pre-loaded")
        except Exception as e:
            log.warning(f"Pre-load skipped: {e}")
    threading.Thread(target=_preload_models, daemon=True).start()

    if WEBHOOK_URL:
        if register_webhook():
            tg_send(CHAT_ID,
                "🟢 <b>APEX-TSS — Redémarrage OK</b>\n"
                f"📅 {datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}\n"
                "✅ Webhook actif · Dixon-Coles chargé\n\n"
                "💡 Test:\n<code>/analyse 11/04 PL Arsenal Bournemouth</code>"
            )
    app.run(host="0.0.0.0", port=PORT, debug=False)
# APEX-TSS Webhook Server — rebuilt Sat Apr 11 10:02:38 UTC 2026
