"""
APEX-TSS — Telegram Webhook Server
====================================
Déployé sur Render.com en mode web service permanent.

Commandes disponibles via Telegram:
  /start        — Message de bienvenue
  /status       — Statut du bot + derniers signaux
  /report       — Envoie le dernier rapport backtest disponible
  /backtest     — Lance un backtest smoke-test rapide
  /gates        — Affiche la config des gates actuelle
  /help         — Liste des commandes

Variables d'environnement requises sur Render:
  BOT_TOKEN     — Token Telegram bot
  CHAT_ID       — Ton chat_id Telegram
  WEBHOOK_URL   — URL publique Render (ex: https://apex-tss.onrender.com)
"""

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

app = Flask(__name__)

# ── Config depuis variables d'environnement ───────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN",   "8798739431:AAH4BhkUL9f1O7GpdKBPm7UZreuuLVd4H9s")
CHAT_ID     = os.environ.get("CHAT_ID",     "5484281251")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")   # set on Render dashboard
PORT        = int(os.environ.get("PORT",    10000))
TG_API      = f"https://api.telegram.org/bot{BOT_TOKEN}"

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def tg_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
    try:
        r = requests.post(f"{TG_API}/sendMessage", json={
            "chat_id": chat_id, "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }, timeout=10)
        return r.status_code == 200 and r.json().get("ok")
    except Exception as e:
        log.error(f"tg_send error: {e}")
        return False


def tg_send_doc(chat_id: str, file_path: str, caption: str = "") -> bool:
    try:
        with open(file_path, "rb") as f:
            r = requests.post(f"{TG_API}/sendDocument", data={
                "chat_id": chat_id, "caption": caption, "parse_mode": "HTML"
            }, files={"document": f}, timeout=60)
        return r.status_code == 200 and r.json().get("ok")
    except Exception as e:
        log.error(f"tg_send_doc error: {e}")
        return False


def register_webhook(url: str) -> bool:
    webhook_url = f"{url}/webhook/{BOT_TOKEN}"
    r = requests.post(f"{TG_API}/setWebhook", json={"url": webhook_url}, timeout=10)
    ok = r.status_code == 200 and r.json().get("ok")
    log.info(f"Webhook {'✅ registered' if ok else '❌ failed'}: {webhook_url}")
    return ok


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_start(chat_id: str):
    msg = (
        "🤖 <b>APEX-TSS Bot actif</b>\n\n"
        "Walk-Forward Backtesting Engine en ligne.\n\n"
        "<b>Commandes disponibles:</b>\n"
        "/report — Dernier rapport backtest\n"
        "/backtest — Lancer un backtest rapide\n"
        "/gates — Config des gates actuelle\n"
        "/status — Statut système\n"
        "/help — Aide complète\n\n"
        f"<i>Serveur actif depuis {datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}</i>"
    )
    tg_send(chat_id, msg)


def cmd_help(chat_id: str):
    msg = (
        "📖 <b>APEX-TSS — Commandes</b>\n\n"
        "/start — Démarrer le bot\n"
        "/status — Statut + dernières stats\n"
        "/report — Envoyer dernier rapport (texte)\n"
        "/backtest — Smoke-test backtest (2 min)\n"
        "/gates — Afficher gates actives\n"
        "/help — Ce message\n\n"
        "<i>Le bot envoie automatiquement un rapport après chaque backtest complet.</i>"
    )
    tg_send(chat_id, msg)


def cmd_status(chat_id: str):
    # Find latest signals CSV
    csvs = sorted(REPORTS_DIR.glob("signals_*.csv"))
    pdfs = sorted(REPORTS_DIR.glob("*.pdf"))

    if csvs:
        latest = csvs[-1]
        mtime  = datetime.utcfromtimestamp(latest.stat().st_mtime)
        age    = (datetime.utcnow() - mtime).total_seconds() / 3600

        try:
            import pandas as pd
            df   = pd.read_csv(latest)
            bets = df[df["decision"] == "BET"] if "decision" in df.columns else df
            n_b  = len(bets)
            n_t  = len(df)
            leagues = ", ".join(sorted(df["league"].unique())) if "league" in df.columns else "—"
        except Exception:
            n_b, n_t, leagues = "?", "?", "—"

        status = (
            f"⚡ <b>APEX-TSS — Statut système</b>\n\n"
            f"🟢 Bot: <b>En ligne</b>\n"
            f"📊 Dernier backtest: <b>{mtime.strftime('%d/%m/%Y %H:%M')}</b> "
            f"({age:.1f}h ago)\n"
            f"🎲 Signaux: <b>{n_t}</b> total | <b>{n_b}</b> BETs\n"
            f"🌍 Ligues: {leagues}\n"
            f"📄 Rapports PDF: <b>{len(pdfs)}</b> disponibles\n\n"
            f"<i>Serveur: render.com | {datetime.utcnow().strftime('%H:%M UTC')}</i>"
        )
    else:
        status = (
            "⚡ <b>APEX-TSS — Statut système</b>\n\n"
            "🟢 Bot: <b>En ligne</b>\n"
            "📊 Aucun backtest effectué encore.\n"
            "→ Envoie /backtest pour lancer un test.\n\n"
            f"<i>{datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}</i>"
        )
    tg_send(chat_id, status)


def cmd_report(chat_id: str):
    csvs = sorted(REPORTS_DIR.glob("signals_*.csv"))
    if not csvs:
        tg_send(chat_id, "❌ Aucun rapport disponible. Lance /backtest d'abord.")
        return

    try:
        import pandas as pd
        from tss.results_analyzer import compute_roi_metrics
        from tss.telegram_bot import build_report_message

        df      = pd.read_csv(str(csvs[-1]))
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        metrics = compute_roi_metrics(df)
        msg     = build_report_message(df, metrics)
        tg_send(chat_id, msg)

        # Send PDF if available
        pdfs = sorted(REPORTS_DIR.glob("*.pdf"))
        if pdfs:
            roi = metrics.get("roi_pct", 0)
            tg_send_doc(chat_id, str(pdfs[-1]),
                        caption=f"📄 APEX-TSS Rapport | ROI: {roi:+.2f}%")
    except Exception as e:
        tg_send(chat_id, f"❌ Erreur rapport: {e}")
        log.error(f"cmd_report error: {e}")


def cmd_gates(chat_id: str):
    try:
        cfg = json.loads(Path("config.json").read_text())
        g   = cfg.get("gates", cfg)
        bk  = cfg.get("backtest", {})

        msg = (
            "⚙️ <b>APEX-TSS — Gates actives</b>\n\n"
            f"<b>Gate-0 DCS:</b> <code>{bk.get('dcs_min', cfg.get('dcs_min','—'))}</code>\n"
            f"<b>Gate-1 EV min:</b> <code>{bk.get('ev_min', g.get('ev_min','—'))}</code>\n"
            f"<b>Gate-2 Edge min:</b> <code>{bk.get('edge_min', g.get('edge_min','—'))}</code>\n"
            f"<b>Gate-3 Odds:</b> <code>[{bk.get('odds_min',1.40)} – {bk.get('odds_max',4.50)}]</code>\n\n"
            f"<b>Kelly fraction:</b> <code>{bk.get('kelly_fraction',0.25)}</code>\n"
            f"<b>Max stake:</b> <code>{bk.get('max_stake_pct',0.03)*100:.1f}%</code>\n"
            f"<b>Book margin:</b> <code>{bk.get('book_margin',0.055)*100:.1f}%</code>\n"
            f"<b>Demarg method:</b> <code>{bk.get('demarg_method','shin')}</code>\n"
        )
    except Exception as e:
        msg = f"❌ Impossible de lire config.json: {e}"
    tg_send(chat_id, msg)


def cmd_backtest(chat_id: str):
    tg_send(chat_id, "⏳ <b>Backtest smoke-test lancé...</b>\nRésultat dans ~2 minutes.")

    def run():
        try:
            result = subprocess.run(
                ["python", "backtesting.py", "--smoke-test"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                tg_send(chat_id, "✅ <b>Backtest terminé.</b> Rapport envoyé ci-dessus.")
            else:
                tg_send(chat_id,
                        f"❌ <b>Backtest échoué:</b>\n<code>{result.stderr[-500:]}</code>")
        except subprocess.TimeoutExpired:
            tg_send(chat_id, "⏱ Timeout — backtest trop long pour le mode smoke-test.")
        except Exception as e:
            tg_send(chat_id, f"❌ Erreur: {e}")

    threading.Thread(target=run, daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# WEBHOOK ROUTE
# ═══════════════════════════════════════════════════════════════════════════════

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": False}), 400

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return jsonify({"ok": True})

    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    # Security: only respond to authorised chat
    if chat_id != CHAT_ID:
        log.warning(f"Unauthorized access from chat_id: {chat_id}")
        tg_send(chat_id, "⛔ Accès non autorisé.")
        return jsonify({"ok": True})

    log.info(f"Command received: {text}")

    cmd = text.split()[0].lower().split("@")[0] if text else ""
    if   cmd == "/start":    cmd_start(chat_id)
    elif cmd == "/help":     cmd_help(chat_id)
    elif cmd == "/status":   cmd_status(chat_id)
    elif cmd == "/report":   cmd_report(chat_id)
    elif cmd == "/backtest": cmd_backtest(chat_id)
    elif cmd == "/gates":    cmd_gates(chat_id)
    else:
        tg_send(chat_id,
                "❓ Commande inconnue. Envoie /help pour la liste des commandes.")

    return jsonify({"ok": True})


# ── Health check (Render ping) ─────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "online",
        "service": "APEX-TSS Telegram Bot",
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"ok": True})


# ── Set webhook on startup ─────────────────────────────────────────────────────
@app.before_request
def _once():
    app.before_request_funcs[None].remove(_once)
    if WEBHOOK_URL:
        register_webhook(WEBHOOK_URL)
    else:
        log.warning("WEBHOOK_URL not set — webhook not registered. "
                    "Set it in Render environment variables.")


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info(f"Starting APEX-TSS webhook server on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
