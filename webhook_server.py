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
    # Sanitize: remove any unmatched HTML tags that could break Telegram parser
    import html as html_lib
    # Allowed tags in Telegram HTML: b, i, u, s, code, pre, a
    # Just strip any problematic chars from team names in the text
    safe_text = text
    try:
        r = requests.post(f"{TG_API}/sendMessage", json={
            "chat_id": chat_id, "text": safe_text,
            "parse_mode": parse_mode, "disable_web_page_preview": True
        }, timeout=15)
        if r.status_code == 200 and r.json().get("ok"):
            return True
        # If HTML parse fails, retry as plain text
        log.warning(f"tg_send HTML failed ({r.json().get('description')}), retrying plain")
        r2 = requests.post(f"{TG_API}/sendMessage", json={
            "chat_id": chat_id,
            "text": re.sub(r"<[^>]+>", "", safe_text),
            "disable_web_page_preview": True
        }, timeout=15)
        return r2.status_code == 200 and r2.json().get("ok")
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
        "Analyse un match ou scanne une journée:\n"
        "<code>/analyse 11/04 PL Arsenal Bournemouth</code>\n"
        "<code>/scan today</code> — scanner les matchs du jour\n"
        "<code>/scan 48h</code> — prochaines 48h\n\n"
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
        "/scan [fenêtre] — Scanner tous les matchs\n"
        "/setgates [params] — Modifier les gates\n"
        "/help — Ce message\n\n"
        "💡 <b>Exemples /scan:</b>\n"
        "<code>/scan today</code>\n"
        "<code>/scan 48h</code>\n"
        "<code>/scan week</code>\n"
        "<code>/scan 12/04</code>\n"
        "<code>/scan 12/04-14/04</code>\n\n"
        "💡 <b>Analyse directe:</b>\n"
        "<code>PSG vs Lyon</code>\n"
        "<code>/analyse 11/04 PL Arsenal Bournemouth</code>"
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

def cmd_suspect(cid, text):
    """Scan and return only suspicious matches."""
    window_text = re.sub(r"^/(suspect|suspicious|alerte)\s*", "", text,
                         flags=re.IGNORECASE).strip() or "48h"
    tg_send(cid, f"🚨 <b>Scan matchs suspects...</b> (<code>{window_text}</code>)")
    try:
        from tss.fixture_fetcher import get_fixtures
        from tss.scanner import scan_fixtures, _compute_synthetic_pbook, _run_gates_with_pbook
        from tss.match_analyzer import (_load_gates, _best_team_match,
                                         _get_dc_model, _league_average_probs,
                                         _simulate_odds)
        from tss.suspicion_engine import analyze_suspicion, format_suspect_message

        fixtures, label = get_fixtures(window_text)
        if not fixtures:
            tg_send(cid, f"📭 Aucun match trouvé pour {window_text}.")
            return

        # Enrich with real odds
        try:
            from tss.odds_api import enrich_fixtures_with_odds, demarginalize_odds
            fixtures = enrich_fixtures_with_odds(fixtures)
            has_real = True
        except Exception:
            has_real = False

        gates   = _load_gates()
        suspects = []

        for fix in fixtures:
            home = _best_team_match(fix["home"]) or fix["home"]
            away = _best_team_match(fix["away"]) or fix["away"]
            model = _get_dc_model(fix["league"])
            if model:
                try:
                    probs = model.predict_probs(home, away)
                except Exception:
                    probs = _league_average_probs(home, away)
            else:
                probs = _league_average_probs(home, away)

            odds_dict = {
                "odds_H":        fix.get("odds_H"),
                "odds_D":        fix.get("odds_D"),
                "odds_A":        fix.get("odds_A"),
                "odds_over2.5":  fix.get("odds_over2.5"),
                "odds_under2.5": fix.get("odds_under2.5"),
                "odds_over3.5":  fix.get("odds_over3.5"),
                "odds_under3.5": fix.get("odds_under3.5"),
            }
            synth = _simulate_odds(probs, margin=gates["book_margin"])
            for k, v in odds_dict.items():
                if not v or v <= 1.0:
                    odds_dict[k] = synth.get(k, 2.0)

            if has_real and fix.get("odds_matched"):
                from tss.odds_api import demarginalize_odds
                p_book = demarginalize_odds(fix)
            else:
                p_book = _compute_synthetic_pbook(probs, gates["book_margin"])

            result = analyze_suspicion(fix, probs, odds_dict, p_book)

            if result["score"] >= 30:
                suspects.append({"fix": {**fix,"home":home,"away":away},
                                  "probs": probs, "suspicion": result})

        suspects.sort(key=lambda x: x["suspicion"]["score"], reverse=True)
        msg = format_suspect_message(suspects, label)

        if len(msg) <= 4000:
            tg_send(cid, msg)
        else:
            mid = msg.rfind("\n", 0, 3800)
            tg_send(cid, msg[:mid])
            tg_send(cid, msg[mid:])

    except Exception as e:
        log.error(f"cmd_suspect error: {e}", exc_info=True)
        tg_send(cid, f"❌ Erreur: <code>{str(e)[:200]}</code>")


def cmd_setgates(cid, text):
    import json
    from pathlib import Path
    DEFAULTS = {
        "ev_min": 0.03, "edge_min": 0.05, "odds_min": 1.40, "odds_max": 4.50,
        "kelly_fraction": 0.25, "max_stake_pct": 0.03, "dcs_min": 0.60,
        "book_margin": 0.055,
    }
    ALLOWED = set(DEFAULTS.keys())
    raw = re.sub(r"^/setgates\s*", "", text, flags=re.IGNORECASE).strip()

    # Reset
    if raw.lower() in ("reset", "default", "defaults", "restaurer"):
        try:
            cfg = json.loads(Path("config.json").read_text())
            cfg["backtest"] = {**cfg.get("backtest", {}), **DEFAULTS}
            Path("config.json").write_text(json.dumps(cfg, indent=2))
            tg_send(cid,
                "\u2705 <b>Gates restaur\u00e9es aux valeurs par d\u00e9faut</b>\n\n"
                f"ev_min=<code>{DEFAULTS['ev_min']}</code>  "
                f"edge_min=<code>{DEFAULTS['edge_min']}</code>\n"
                f"odds=[<code>{DEFAULTS['odds_min']}</code>"
                f"\u2013<code>{DEFAULTS['odds_max']}</code>]  "
                f"kelly=<code>{DEFAULTS['kelly_fraction']}</code>"
            )
        except Exception as e:
            tg_send(cid, f"\u274c Erreur reset: <code>{e}</code>")
        return

    # Help
    if not raw:
        tg_send(cid,
            "\u2699\ufe0f <b>Usage /setgates:</b>\n\n"
            "<code>/setgates ev_min=0.01 edge_min=0.01</code>\n"
            "<code>/setgates odds_min=1.30 odds_max=5.00</code>\n"
            "<code>/setgates kelly_fraction=0.15</code>\n"
            "<code>/setgates reset</code> \u2014 valeurs par d\u00e9faut\n\n"
            "Param\u00e8tres: ev_min | edge_min | odds_min | odds_max\n"
            "kelly_fraction | max_stake_pct | dcs_min | book_margin"
        )
        return

    # Parse key=value
    updates, errors = {}, []
    for token in raw.split():
        if "=" not in token:
            errors.append(f"Format invalide: <code>{token}</code>")
            continue
        k, v = token.split("=", 1)
        k = k.strip().lower()
        if k not in ALLOWED:
            errors.append(f"Cl\u00e9 inconnue: <code>{k}</code>")
            continue
        try:
            updates[k] = float(v)
        except ValueError:
            errors.append(f"Valeur invalide: <code>{v}</code> pour {k}")

    if errors:
        tg_send(cid, "\u274c Erreurs:\n" + "\n".join(errors))
        return
    if not updates:
        tg_send(cid, "\u274c Aucun param\u00e8tre valide.")
        return

    # Apply to config.json + runtime override file
    try:
        cfg = json.loads(Path("config.json").read_text())
        if "backtest" not in cfg:
            cfg["backtest"] = {}
        cfg["backtest"].update(updates)
        Path("config.json").write_text(json.dumps(cfg, indent=2))
        # Also write to override file (survives within session)
        Path("data/gates_override.json").write_text(json.dumps(updates))
        lines = ["\u2705 <b>Gates mises \u00e0 jour</b>\n"]
        for k, v in updates.items():
            default = DEFAULTS.get(k, "?")
            arrow   = "\U0001f4c9" if v < default else ("\U0001f4c8" if v > default else "\u27a1\ufe0f")
            lines.append(f"  {arrow} {k} = <code>{v}</code>  (d\u00e9faut: {default})")
        ev  = cfg["backtest"].get("ev_min", 0.03)
        ed  = cfg["backtest"].get("edge_min", 0.05)
        if ev < 0.02 or ed < 0.02:
            lines.append("\n\u26a0\ufe0f <b>Gates basses \u2014 mode diagnostic</b>")
            lines.append("Utilise <code>/setgates reset</code> apr\u00e8s validation.")
        tg_send(cid, "\n".join(lines))
    except Exception as e:
        tg_send(cid, f"\u274c Erreur: <code>{e}</code>")


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

def cmd_scan(cid, text):
    """Scan upcoming fixtures through TSS gates."""
    # Parse window from command: /scan today | /scan 48h | /scan 12/04
    window_text = text.strip()
    window_text = re.sub(r"^/(scan)\s*", "", window_text, flags=re.IGNORECASE).strip()
    if not window_text:
        tg_send(cid,
            "🔭 <b>Usage /scan:</b>\n\n"
            "<code>/scan today</code> — matchs du jour\n"
            "<code>/scan 48h</code> — prochaines 48h\n"
            "<code>/scan week</code> — cette semaine\n"
            "<code>/scan 12/04</code> — date spécifique\n"
            "<code>/scan 12/04-14/04</code> — plage de dates"
        )
        return

    tg_send(cid, f"🔭 <b>Scan en cours...</b> (<code>{window_text}</code>)\n⏳ Récupération des fixtures...")

    try:
        from tss.fixture_fetcher import get_fixtures
        from tss.scanner import scan_fixtures, format_scan_message

        # 1. Fetch fixtures
        fixtures, label = get_fixtures(window_text)
        total = len(fixtures)

        if total == 0:
            tg_send(cid,
                f"🔭 <b>Scan {label}</b>\n\n"
                f"📭 Aucun match trouvé pour cette période.\n"
                f"Vérifie la fenêtre ou réessaie plus tard."
            )
            return

        tg_send(cid, f"⏳ {total} matchs trouvés — analyse TSS en cours...")

        # 2. Scan through TSS
        results = scan_fixtures(fixtures, min_stars=2, min_ev=0.03)

        # 3. Top-5 + multi-message
        from tss.scanner import format_scan_messages
        for chunk in format_scan_messages(results, label, total, top_n=5):
            tg_send(cid, chunk)

    except Exception as e:
        log.error(f"cmd_scan error: {e}", exc_info=True)
        tg_send(cid, f"❌ Erreur scan: <code>{str(e)[:200]}</code>")


def _split_scan_message(msg: str, max_len: int = 3800) -> list:
    """Split long scan message at match boundaries (═══ separator)."""
    parts   = msg.split("═" * 25)
    chunks  = []
    current = parts[0]  # header

    for part in parts[1:]:
        block = "═" * 25 + part
        if len(current) + len(block) <= max_len:
            current += block
        else:
            chunks.append(current)
            current = block

    if current:
        chunks.append(current)
    return chunks


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
    elif cmd == "/scan":     cmd_scan(cid, text)
    elif cmd in ("/suspect", "/suspicious", "/alerte"): cmd_suspect(cid, text)
    elif cmd in ("/setgates", "/gates_set", "/set"): cmd_setgates(cid, text)
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
