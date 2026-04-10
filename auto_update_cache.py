"""
APEX-TSS — Auto Cache Updater
================================
Pipeline automatique :
  1. Scrape FBref (toutes les ligues actives)
  2. Commit data/fbref_cache.db sur GitHub
  3. Render détecte le push → redéploie → Dixon-Coles rechargé

Usage:
  python auto_update_cache.py                     # toutes ligues
  python auto_update_cache.py --leagues EPL "Serie A"
  python auto_update_cache.py --seasons 2024-2025  # saison en cours seulement

Planification (cron) — ajouter dans crontab :
  # Toutes les semaines le lundi à 3h
  0 3 * * 1 cd /path/to/Apex-TSS && python auto_update_cache.py >> logs/auto_update.log 2>&1
"""

import os
import sys
import json
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AUTO-UPDATE] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/auto_update.log", mode="a"),
    ]
)
log = logging.getLogger("auto_update")
Path("logs").mkdir(exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_URL_TPL = "https://{token}@github.com/okomaleonce-commits/Apex-TSS.git"

DEFAULT_LEAGUES = [
    "EPL", "Serie A", "La Liga", "Bundesliga", "Ligue 1",
    "Eredivisie", "Belgian Pro",
]
DEFAULT_SEASONS = ["2023-2024", "2024-2025"]

CACHE_DB   = Path("data/fbref_cache.db")
GITIGNORE  = Path(".gitignore")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ENSURE CACHE IS TRACKED BY GIT
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_cache_tracked():
    """Remove fbref_cache.db from .gitignore if present, so it gets committed."""
    if not GITIGNORE.exists():
        return

    lines = GITIGNORE.read_text().splitlines()
    filtered = [l for l in lines
                if "fbref_cache" not in l and "*.db" not in l]

    if len(filtered) != len(lines):
        GITIGNORE.write_text("\n".join(filtered) + "\n")
        log.info(".gitignore updated — fbref_cache.db now tracked")

    # Ensure data/ exists and has a .gitkeep so dir is tracked
    Path("data").mkdir(exist_ok=True)
    Path("data/.gitkeep").touch()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SCRAPE FBREF
# ═══════════════════════════════════════════════════════════════════════════════

def scrape(leagues: list, seasons: list) -> bool:
    log.info(f"Scraping FBref: {leagues} | {seasons}")
    try:
        from tss.fbref_scraper import FBrefScraper, FBrefCache
        cache   = FBrefCache()
        scraper = FBrefScraper(cache)
        df      = scraper.scrape_all(leagues=leagues, seasons=seasons, save_cache=True)

        if df.empty:
            log.error("Scraping returned no data.")
            return False

        log.info(f"✅ Scraped {len(df)} matches → {CACHE_DB}")
        return True

    except Exception as e:
        log.error(f"Scraping failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# 3. GIT COMMIT + PUSH
# ═══════════════════════════════════════════════════════════════════════════════

def _run(cmd: list, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


def git_commit_and_push(token: str = "") -> bool:
    ts     = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    size   = CACHE_DB.stat().st_size // 1024 if CACHE_DB.exists() else 0

    # Config git identity
    _run(["git", "config", "user.email", "apex-tss-bot@render.com"])
    _run(["git", "config", "user.name",  "APEX-TSS AutoBot"])

    # Stage cache + gitignore
    files = [str(CACHE_DB), ".gitignore", "data/.gitkeep"]
    _run(["git", "add"] + files)

    # Check if there's anything to commit
    status = _run(["git", "status", "--porcelain"])
    if not status.stdout.strip():
        log.info("Nothing to commit — cache unchanged.")
        return True

    msg = (
        f"data: auto-update FBref cache {ts}\n\n"
        f"Size: {size} KB | "
        f"Leagues: {', '.join(DEFAULT_LEAGUES)}"
    )
    commit = _run(["git", "commit", "-m", msg])
    if commit.returncode != 0:
        log.error(f"Git commit failed:\n{commit.stderr}")
        return False

    # Push (with token if provided)
    if token:
        remote_url = REPO_URL_TPL.format(token=token)
        _run(["git", "remote", "set-url", "origin", remote_url])

    push = _run(["git", "push", "origin", "main"])
    if push.returncode != 0:
        log.error(f"Git push failed:\n{push.stderr}")
        return False

    log.info(f"✅ Cache pushed to GitHub ({size} KB) → Render redeploy triggered")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TELEGRAM NOTIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def notify_telegram(success: bool, leagues: list, n_matches: int = 0):
    try:
        cfg   = json.loads(Path("config.json").read_text())
        token = cfg.get("telegram", {}).get("bot_token",
                os.environ.get("BOT_TOKEN", "8798739431:AAH4BhkUL9f1O7GpdKBPm7UZreuuLVd4H9s"))
        cid   = cfg.get("telegram", {}).get("chat_id", "5484281251")

        import requests
        ts  = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")
        msg = (
            f"{'✅' if success else '❌'} <b>APEX-TSS — Mise à jour cache</b>\n\n"
            f"📅 {ts}\n"
            f"🌍 Ligues: {', '.join(leagues)}\n"
            f"📊 Matchs en cache: <b>{n_matches}</b>\n"
            f"{'🚀 Render redéploie — Dixon-Coles rechargé dans ~2 min.' if success else '⚠️ Échec scraping — ancien cache conservé.'}"
        )
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.warning(f"Telegram notify failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. COUNT CACHED MATCHES
# ═══════════════════════════════════════════════════════════════════════════════

def count_cached_matches() -> int:
    try:
        import sqlite3
        conn = sqlite3.connect(str(CACHE_DB))
        n    = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# 6. MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="APEX-TSS Auto Cache Updater")
    parser.add_argument("--leagues", nargs="+", default=DEFAULT_LEAGUES)
    parser.add_argument("--seasons", nargs="+", default=DEFAULT_SEASONS)
    parser.add_argument("--push-only", action="store_true",
                        help="Skip scraping, just commit+push existing cache")
    parser.add_argument("--no-push",  action="store_true",
                        help="Scrape only, don't push to GitHub")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("APEX-TSS AUTO CACHE UPDATE — START")
    log.info(f"Leagues : {args.leagues}")
    log.info(f"Seasons : {args.seasons}")
    log.info("=" * 60)

    # Ensure DB is tracked by git
    ensure_cache_tracked()

    # Step 1: Scrape
    scrape_ok = True
    if not args.push_only:
        scrape_ok = scrape(args.leagues, args.seasons)

    n_matches = count_cached_matches()
    log.info(f"Cache total: {n_matches} matches")

    # Step 2: Commit + push
    push_ok = True
    if not args.no_push and CACHE_DB.exists():
        token    = GITHUB_TOKEN or ""
        push_ok  = git_commit_and_push(token)

    # Step 3: Notify
    success = scrape_ok and push_ok
    notify_telegram(success, args.leagues, n_matches)

    log.info(f"\n{'✅ DONE' if success else '❌ FAILED'} — {n_matches} matches in cache")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
