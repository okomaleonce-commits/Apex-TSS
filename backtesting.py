"""
APEX-TSS — Backtesting Master Orchestrator
===========================================
Usage:
  python backtesting.py --leagues EPL "Serie A" --seasons 2022-2023 2023-2024
  python backtesting.py --all          # All 10 leagues, all 4 seasons
  python backtesting.py --smoke-test   # Fast synthetic test
  python backtesting.py --calibrate    # Gate calibration only (requires existing signals)
"""

import sys
import json
import logging
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Internal imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from tss.fbref_scraper  import FBrefScraper, FBrefCache, LEAGUES, SEASONS
from tss.backtest_engine import BacktestRunner, GateCalibrator
from tss.results_analyzer import generate_full_report, compute_roi_metrics
from tss.telegram_bot import hook_into_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/backtest_run.log", mode="a")
    ]
)
log = logging.getLogger("main")
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG LOADER
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_CONFIG = {
    # Gate thresholds (will be calibrated)
    "ev_min":           0.03,
    "edge_min":         0.05,
    "odds_min":         1.40,
    "odds_max":         4.50,
    # Staking
    "kelly_fraction":   0.25,
    "max_stake_pct":    0.030,
    "min_stake_pct":    0.005,
    # DCS gate
    "dcs_min":          0.60,
    # Model
    "book_margin":      0.055,
    "demarg_method":    "shin",
    "xi":               0.0065,
}

def load_config(path: str = "config.json") -> dict:
    if Path(path).exists():
        with open(path) as f:
            cfg = json.load(f)
        merged = {**DEFAULT_CONFIG, **cfg.get("backtest", cfg)}
        log.info(f"Config loaded from {path}")
        return merged
    log.warning(f"No config.json found, using defaults.")
    return DEFAULT_CONFIG


# ═══════════════════════════════════════════════════════════════════════════════
# SMOKE TEST (no network required)
# ═══════════════════════════════════════════════════════════════════════════════

def run_smoke_test(config: dict):
    """Synthetic data smoke test — validates full pipeline in ~10 seconds."""
    log.info("🔥 SMOKE TEST MODE — synthetic data")
    import hashlib
    from itertools import permutations

    np.random.seed(42)
    teams   = [f"Team_{i}" for i in range(18)]
    seasons = ["2021-2022", "2022-2023", "2023-2024"]
    rows    = []

    for season in seasons:
        pairs = list(permutations(teams, 2))[:306]   # 18-team league full season
        base_date = pd.Timestamp(season[:4] + "-08-01")
        for i, (h, a) in enumerate(pairs):
            hg  = np.random.poisson(1.4)
            ag  = np.random.poisson(1.1)
            res = "H" if hg > ag else ("A" if ag > hg else "D")
            mid = hashlib.md5(f"{season}{i}{h}{a}".encode()).hexdigest()[:12]
            rows.append({
                "match_id": mid, "league": "EPL", "season": season,
                "date":     base_date + pd.Timedelta(days=i % 270),
                "home": h, "away": a,
                "home_goals": hg, "away_goals": ag,
                "xg_home":  round(hg + np.random.normal(0, 0.3), 2),
                "xg_away":  round(ag + np.random.normal(0, 0.3), 2),
                "result": res,
            })

    df = pd.DataFrame(rows)
    log.info(f"Synthetic: {len(df)} matches / {df['season'].nunique()} seasons")

    runner  = BacktestRunner(config)
    results = runner.run(df)

    if results.empty:
        log.error("No signals generated — check model fitting.")
        return

    report = generate_full_report(results, config)
    print("\n" + "="*70)
    print(report)

    log.info("✅ Smoke test complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# FULL BACKTEST
# ═══════════════════════════════════════════════════════════════════════════════

def run_full_backtest(leagues: list, seasons: list, config: dict, use_cache: bool = True):
    log.info(f"\n{'='*70}")
    log.info(f"APEX-TSS WALK-FORWARD BACKTEST")
    log.info(f"Leagues : {leagues}")
    log.info(f"Seasons : {seasons}")
    log.info(f"{'='*70}\n")

    cache   = FBrefCache()
    scraper = FBrefScraper(cache)

    # Try loading from cache first
    cached_df = cache.load_matches(leagues=leagues, seasons=seasons)
    if use_cache and not cached_df.empty:
        log.info(f"✅ Loaded {len(cached_df)} matches from cache")
        df = cached_df
    else:
        log.info("🌐 Scraping FBref …")
        df = scraper.scrape_all(leagues=leagues, seasons=seasons, save_cache=True)

    if df.empty:
        log.error("No data available. Aborting.")
        return

    # Per-league season coverage report
    log.info("\n── Data coverage ──")
    coverage = df.groupby(["league", "season"]).size().reset_index(name="n_matches")
    log.info("\n" + coverage.to_string(index=False))

    # Run backtest
    runner  = BacktestRunner(config)
    results = runner.run(df)

    if results.empty:
        log.error("No signals generated.")
        return

    # Summary
    metrics = compute_roi_metrics(results)
    log.info("\n" + "="*70)
    log.info("BACKTEST COMPLETE — SUMMARY")
    log.info("="*70)
    for k, v in metrics.items():
        log.info(f"  {k:30s}: {v}")

    # Full report
    generate_full_report(results, config)
    log.info("\n✅ All reports saved to ./reports/")


# ═══════════════════════════════════════════════════════════════════════════════
# CALIBRATE ONLY (from existing CSV)
# ═══════════════════════════════════════════════════════════════════════════════

def run_calibration_only():
    csv_files = sorted(Path("reports").glob("signals_*.csv"))
    if not csv_files:
        log.error("No signal CSV found. Run a backtest first.")
        return
    latest = csv_files[-1]
    log.info(f"Loading signals from {latest}")
    df = pd.read_csv(latest)

    calibrator = GateCalibrator()
    calib = calibrator.calibrate(df, min_bets=15)

    if not calib.empty:
        print("\n── TOP 20 GATE CONFIGURATIONS ──")
        print(calib.head(20).to_string(index=False))
        best = calib.iloc[0]
        print(f"\n✅ OPTIMAL: ev_min={best['ev_min']}  "
              f"edge_min={best['edge_min']}  "
              f"ROI={best['roi_pct']}%  n_bets={best['n_bets']}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(description="APEX-TSS Walk-Forward Backtester")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all",         action="store_true", help="All leagues & seasons")
    group.add_argument("--smoke-test",  action="store_true", help="Fast synthetic test")
    group.add_argument("--calibrate",   action="store_true", help="Gate calibration on existing signals")
    group.add_argument("--leagues",     nargs="+",           help="League keys e.g. EPL 'Serie A'")

    parser.add_argument("--seasons",    nargs="+", default=SEASONS, help="Seasons e.g. 2023-2024")
    parser.add_argument("--no-cache",   action="store_true",  help="Force re-scrape (ignore cache)")
    parser.add_argument("--config",     default="config.json",help="Config file path")
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    config = load_config(args.config)

    if args.smoke_test:
        run_smoke_test(config)

    elif args.calibrate:
        run_calibration_only()

    elif args.all:
        run_full_backtest(
            leagues=list(LEAGUES.keys()),
            seasons=SEASONS,
            config=config,
            use_cache=not args.no_cache
        )

    elif args.leagues:
        valid = [lg for lg in args.leagues if lg in LEAGUES]
        invalid = [lg for lg in args.leagues if lg not in LEAGUES]
        if invalid:
            log.warning(f"Unknown leagues (skipped): {invalid}")
            log.warning(f"Valid options: {list(LEAGUES.keys())}")
        if valid:
            run_full_backtest(
                leagues=valid,
                seasons=args.seasons,
                config=config,
                use_cache=not args.no_cache
            )


# ═══════════════════════════════════════════════════════════════════════════════
# REAL-ODDS BACKTEST (added by odds_loader integration)
# ═══════════════════════════════════════════════════════════════════════════════

def run_with_real_odds(leagues: list, seasons: list, config: dict, force_dl: bool = False):
    """Full pipeline: FBref scrape + FDCO odds download + real-odds backtest."""
    from tss.odds_loader import build_unified_dataset, run_real_odds_backtest

    unified = build_unified_dataset(
        leagues=leagues,
        seasons=seasons,
        force_download=force_dl
    )

    if unified.empty:
        log.error("No unified dataset built. Aborting.")
        return

    results = run_real_odds_backtest(unified, config)

    if results.empty:
        log.error("No signals from real-odds backtest.")
        return

    metrics = compute_roi_metrics(results)
    log.info("\n" + "="*60)
    log.info("REAL-ODDS BACKTEST — SUMMARY")
    log.info("="*60)
    for k, v in metrics.items():
        log.info(f"  {k:30s}: {v}")

    pdf = generate_full_report(results, config)
    hook_into_pipeline(results, config, pdf_path=pdf)
    log.info("\n✅ Real-odds backtest complete. Reports in ./reports/")


# ═══════════════════════════════════════════════════════════════════════════════
# FULL PIPELINE incl. ALT LEAGUES (Brazil, A-League, AFC CL)
# ═══════════════════════════════════════════════════════════════════════════════

FDCO_LEAGUES = ["EPL","La Liga","Bundesliga","Serie A","Ligue 1","Eredivisie","Belgian Pro"]
ALT_LEAGUES_LIST = ["Brazil Serie A", "A-League", "AFC CL"]

def run_complete_pipeline(seasons: list, config: dict, force_dl: bool = False):
    """
    Full 10-league pipeline:
      1. Scrape FBref (all leagues)
      2. Download FDCO odds (7 leagues)
      3. Load alternative odds (3 leagues) via AlternativeOddsLoader
      4. Merge + real-odds backtest
      5. Generate full report
    """
    from tss.fbref_scraper import FBrefScraper, FBrefCache, LEAGUES as ALL_LEAGUES
    from tss.odds_loader import build_unified_dataset
    from tss.alternative_odds_loader import extend_unified_dataset
    from tss.odds_loader import run_real_odds_backtest

    all_league_keys = list(ALL_LEAGUES.keys())

    # Step 1: FBref scrape (all leagues)
    log.info("\n📡 Step 1: Scraping FBref for all leagues …")
    cache   = FBrefCache()
    scraper = FBrefScraper(cache)
    fbref_cached = cache.load_matches(leagues=all_league_keys, seasons=seasons)
    if fbref_cached.empty or force_dl:
        fbref_df = scraper.scrape_all(leagues=all_league_keys, seasons=seasons)
    else:
        fbref_df = fbref_cached
        log.info(f"  FBref cache: {len(fbref_df)} matches")

    # Step 2: FDCO odds (7 leagues)
    log.info("\n📥 Step 2: Building FDCO unified dataset …")
    fdco_unified = build_unified_dataset(
        leagues=FDCO_LEAGUES,
        seasons=seasons,
        fbref_df=fbref_df[fbref_df["league"].isin(FDCO_LEAGUES)],
        force_download=force_dl
    )

    # Step 3: Alternative leagues (3 leagues)
    log.info("\n🔄 Step 3: Loading alternative odds (Brazil / A-League / AFC CL) …")
    full_unified = extend_unified_dataset(
        unified_df=fdco_unified,
        alt_leagues=ALT_LEAGUES_LIST,
        seasons=seasons,
        fbref_df=fbref_df
    )

    if full_unified.empty:
        log.error("No unified dataset. Aborting.")
        return

    log.info(f"\n✅ Total unified dataset: {len(full_unified)} matches")

    # Step 4: Backtest
    log.info("\n🎯 Step 4: Running real-odds walk-forward backtest …")
    results = run_real_odds_backtest(full_unified, config)

    if results.empty:
        log.error("No signals generated.")
        return

    # Step 5: Report
    generate_full_report(results, config)
    pdf = list(sorted(__import__('pathlib').Path('reports').glob('*.pdf')))[-1] if list(__import__('pathlib').Path('reports').glob('*.pdf')) else None
    hook_into_pipeline(results, config, pdf_path=str(pdf) if pdf else None)
    log.info("\n✅ Complete pipeline finished. Reports in ./reports/")
