"""
APEX-TSS — Historical Odds Loader
===================================
Source: football-data.co.uk (free, no API key required)
Covers: EPL, La Liga, Bundesliga, Serie A, Ligue 1,
        Eredivisie, Belgian Pro, Brazil Serie A, A-League

Workflow:
  1. OddsDownloader   → fetch CSV files from football-data.co.uk
  2. OddsParser       → standardise columns + extract B365/Bet365 odds
  3. OddsMatchMerger  → fuzzy-join with FBref DataFrame on (date, home, away)
  4. UnifiedDataset   → final DataFrame ready for BacktestRunner

Usage:
  from tss.odds_loader import build_unified_dataset
  df = build_unified_dataset(leagues=["EPL","Serie A"], seasons=["2023-2024"])
  # df has all columns needed by BacktestRunner + real book odds
"""

import io
import re
import time
import logging
import hashlib
import sqlite3
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ODDS] %(message)s")
log = logging.getLogger("odds_loader")

ODDS_CACHE_DIR = Path("data/odds_csv")
ODDS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

RATE_LIMIT = 2.0   # seconds between downloads


# ═══════════════════════════════════════════════════════════════════════════════
# 1. LEAGUE → URL MAP (football-data.co.uk)
# ═══════════════════════════════════════════════════════════════════════════════

# Season code format: "2324" for 2023-2024
FDCO_LEAGUE_MAP = {
    "EPL":            "E0",    # England Premier League
    "Championship":   "E1",    # England Championship
    "La Liga":        "SP1",   # Spain La Liga
    "Bundesliga":     "D1",    # Germany Bundesliga
    "Serie A":        "I1",    # Italy Serie A
    "Ligue 1":        "F1",    # France Ligue 1
    "Eredivisie":     "N1",    # Netherlands Eredivisie
    "Belgian Pro":    "B1",    # Belgium Pro League
    "Brazil Serie A": None,    # Not on FDCO → use alternative
    "A-League":       None,    # Not on FDCO → use alternative
    "AFC CL":         None,    # Not on FDCO
}

# Leagues not on FDCO — alternative CSV sources
ALTERNATIVE_SOURCES = {
    "Brazil Serie A": "https://github.com/martj42/international_results/raw/master/results.csv",
    "A-League":       None,   # Manual import only
    "AFC CL":         None,   # Manual import only
}

FDCO_BASE = "https://www.football-data.co.uk/mmz4281"

def _season_code(season_str: str) -> str:
    """'2023-2024' → '2324'"""
    parts = season_str.split("-")
    if len(parts) == 2:
        return parts[0][-2:] + parts[1][-2:]
    raise ValueError(f"Invalid season format: {season_str}")

def build_fdco_url(league_key: str, season: str) -> Optional[str]:
    code = FDCO_LEAGUE_MAP.get(league_key)
    if code is None:
        return None
    sc = _season_code(season)
    return f"{FDCO_BASE}/{sc}/{code}.csv"


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DOWNLOADER (with local file cache)
# ═══════════════════════════════════════════════════════════════════════════════

class OddsDownloader:
    """Downloads and caches CSV files from football-data.co.uk."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Referer": "https://www.football-data.co.uk/",
    }

    def __init__(self, cache_dir: Path = ODDS_CACHE_DIR):
        self.cache_dir = cache_dir
        self.session   = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _cache_path(self, url: str) -> Path:
        fn = hashlib.md5(url.encode()).hexdigest()[:10] + ".csv"
        return self.cache_dir / fn

    def download(self, url: str, force: bool = False) -> Optional[str]:
        cache = self._cache_path(url)
        if cache.exists() and not force:
            log.debug(f"Cache hit: {url}")
            return cache.read_text(encoding="utf-8", errors="replace")

        log.info(f"Downloading: {url}")
        time.sleep(RATE_LIMIT)
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            text = r.content.decode("latin-1")
            cache.write_text(text, encoding="utf-8")
            return text
        except Exception as e:
            log.error(f"Download failed [{url}]: {e}")
            return None

    def download_all(
        self,
        leagues: List[str],
        seasons: List[str],
        force: bool = False
    ) -> Dict[Tuple[str, str], Optional[str]]:
        results = {}
        for league in leagues:
            for season in seasons:
                url = build_fdco_url(league, season)
                if url is None:
                    log.warning(f"No FDCO source for {league} — skipping download")
                    results[(league, season)] = None
                    continue
                csv_text = self.download(url, force=force)
                results[(league, season)] = csv_text
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# 3. ODDS PARSER (standardise FDCO columns)
# ═══════════════════════════════════════════════════════════════════════════════

# Priority order of bookmakers to use (first available wins)
BOOKMAKER_PRIORITY = [
    "B365",   # Bet365 (most complete, highest coverage)
    "BW",     # Betway
    "IW",     # Interwetten
    "LB",     # Ladbrokes
    "PS",     # Pinnacle (sharpest)
    "WH",     # William Hill
    "VC",     # VC Bet
    "Avg",    # Market average
    "Max",    # Market max
]

MARKET_SUFFIXES = {
    "1x2":    {"H": "H", "D": "D", "A": "A"},
    "ou25":   {"over": ">2.5", "under": "<2.5"},
    "btts":   {"yes": "AHH", "no": None},   # FDCO BTTS is limited
}


class OddsParser:
    """
    Parses raw FDCO CSV text into a standardised DataFrame.

    Output columns (per match):
        date, home, away, home_goals, away_goals, result
        odds_H, odds_D, odds_A
        odds_over2.5, odds_under2.5
        odds_over3.5, odds_under3.5  (if available)
        odds_btts_yes, odds_btts_no  (if available)
        bookie_used
    """

    def parse(self, csv_text: str, league: str, season: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                io.StringIO(csv_text),
                encoding="utf-8",
                on_bad_lines="skip",
                low_memory=False
            )
        except Exception as e:
            log.error(f"CSV parse error [{league} {season}]: {e}")
            return pd.DataFrame()

        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all")

        # ── Date ─────────────────────────────────────────────────────────────
        date_col = next((c for c in df.columns if c.lower() in ("date",)), None)
        if date_col is None:
            log.warning(f"No date column in {league} {season}")
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])

        # ── Teams ─────────────────────────────────────────────────────────────
        home_col = next((c for c in df.columns if c in ("HomeTeam", "Home")), None)
        away_col = next((c for c in df.columns if c in ("AwayTeam", "Away")), None)
        if not home_col or not away_col:
            log.warning(f"No team columns in {league} {season}")
            return pd.DataFrame()
        df["home"] = df[home_col].str.strip()
        df["away"] = df[away_col].str.strip()

        # ── Goals ─────────────────────────────────────────────────────────────
        hg_col = next((c for c in df.columns if c in ("FTHG", "HG")), None)
        ag_col = next((c for c in df.columns if c in ("FTAG", "AG")), None)
        if not hg_col or not ag_col:
            log.warning(f"No goal columns in {league} {season}")
            return pd.DataFrame()
        df["home_goals"] = pd.to_numeric(df[hg_col], errors="coerce")
        df["away_goals"] = pd.to_numeric(df[ag_col], errors="coerce")
        df = df.dropna(subset=["home_goals", "away_goals"])
        df["home_goals"] = df["home_goals"].astype(int)
        df["away_goals"] = df["away_goals"].astype(int)

        # ── Full-time result ───────────────────────────────────────────────────
        ftr_col = next((c for c in df.columns if c in ("FTR", "Res")), None)
        if ftr_col:
            df["result"] = df[ftr_col].str.strip()
        else:
            df["result"] = df.apply(
                lambda r: "H" if r["home_goals"] > r["away_goals"]
                         else ("A" if r["away_goals"] > r["home_goals"] else "D"),
                axis=1
            )

        # ── 1X2 Odds ──────────────────────────────────────────────────────────
        odds_H, odds_D, odds_A, bookie = self._extract_1x2(df)
        df["odds_H"] = odds_H
        df["odds_D"] = odds_D
        df["odds_A"] = odds_A
        df["bookie_used"] = bookie

        # ── Over/Under 2.5 ────────────────────────────────────────────────────
        df["odds_over2.5"],  df["odds_under2.5"]  = self._extract_ou(df, "2.5")
        df["odds_over3.5"],  df["odds_under3.5"]  = self._extract_ou(df, "3.5")

        # ── BTTS ──────────────────────────────────────────────────────────────
        df["odds_btts_yes"], df["odds_btts_no"] = self._extract_btts(df)

        # ── Metadata ──────────────────────────────────────────────────────────
        df["league"]  = league
        df["season"]  = season
        df["match_id"] = df.apply(
            lambda r: hashlib.md5(
                f"{r['date'].date()}_{r['home']}_{r['away']}_{league}".encode()
            ).hexdigest()[:12],
            axis=1
        )

        cols = [
            "match_id", "league", "season", "date", "home", "away",
            "home_goals", "away_goals", "result",
            "odds_H", "odds_D", "odds_A",
            "odds_over2.5", "odds_under2.5",
            "odds_over3.5", "odds_under3.5",
            "odds_btts_yes", "odds_btts_no",
            "bookie_used",
        ]
        existing = [c for c in cols if c in df.columns]
        result_df = df[existing].reset_index(drop=True)
        log.info(f"  Parsed {len(result_df)} matches — {league} {season} "
                 f"(bookie: {bookie})")
        return result_df

    def _extract_1x2(self, df: pd.DataFrame) -> Tuple:
        for bk in BOOKMAKER_PRIORITY:
            hc, dc, ac = f"{bk}H", f"{bk}D", f"{bk}A"
            if all(c in df.columns for c in [hc, dc, ac]):
                H = pd.to_numeric(df[hc], errors="coerce")
                D = pd.to_numeric(df[dc], errors="coerce")
                A = pd.to_numeric(df[ac], errors="coerce")
                coverage = H.notna().mean()
                if coverage > 0.5:
                    return H.round(3), D.round(3), A.round(3), bk
        return pd.Series([None]*len(df)), pd.Series([None]*len(df)), pd.Series([None]*len(df)), "none"

    def _extract_ou(self, df: pd.DataFrame, line: str) -> Tuple:
        tag = line.replace(".", "")   # "25" or "35"
        # Try Bet365 first: B365>2.5, B365<2.5
        for bk in ["B365", "BW", "Max", "Avg"]:
            oc = f"{bk}>{line}"
            uc = f"{bk}<{line}"
            # Some seasons use slightly different column names
            if oc not in df.columns:
                oc = f"{bk}O{tag}" if f"{bk}O{tag}" in df.columns else None
            if uc not in df.columns:
                uc = f"{bk}U{tag}" if f"{bk}U{tag}" in df.columns else None
            if oc and uc and oc in df.columns and uc in df.columns:
                O = pd.to_numeric(df[oc], errors="coerce")
                U = pd.to_numeric(df[uc], errors="coerce")
                if O.notna().mean() > 0.4:
                    return O.round(3), U.round(3)
        return pd.Series([None]*len(df)), pd.Series([None]*len(df))

    def _extract_btts(self, df: pd.DataFrame) -> Tuple:
        # FDCO has BTTS only from ~2019 onwards in some leagues
        for col_y, col_n in [("BTSHY", "BTSHN"), ("BbAv>2.5", None)]:
            if col_y in df.columns:
                Y = pd.to_numeric(df[col_y], errors="coerce")
                N = pd.to_numeric(df.get(col_n, pd.Series([None]*len(df))), errors="coerce")
                return Y.round(3), N.round(3)
        return pd.Series([None]*len(df)), pd.Series([None]*len(df))


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TEAM NAME NORMALISER (FBref vs FDCO name differences)
# ═══════════════════════════════════════════════════════════════════════════════

# Known manual mappings (FBref name → FDCO name)
TEAM_NAME_MAP = {
    # EPL
    "Manchester City":      "Man City",
    "Manchester United":    "Man United",
    "Newcastle United":     "Newcastle",
    "Tottenham Hotspur":    "Tottenham",
    "West Ham United":      "West Ham",
    "Wolverhampton Wanderers": "Wolves",
    "Nottingham Forest":    "Nott'm Forest",
    "Brighton & Hove Albion": "Brighton",
    "AFC Bournemouth":      "Bournemouth",
    "Leeds United":         "Leeds",
    "Brentford":            "Brentford",
    # Serie A
    "Inter Milan":          "Inter",
    "AC Milan":             "Milan",
    "Hellas Verona":        "Verona",
    "Genoa":                "Genoa",
    "Monza":                "Monza",
    # La Liga
    "Athletic Club":        "Ath Bilbao",
    "Atlético Madrid":      "Ath Madrid",
    "Real Betis":           "Betis",
    "Celta Vigo":           "Celta",
    "Deportivo Alavés":     "Alaves",
    "Rayo Vallecano":       "Vallecano",
    # Bundesliga
    "Borussia Dortmund":    "Dortmund",
    "Borussia M'gladbach":  "Ein Frankfurt",
    "Eintracht Frankfurt":  "Ein Frankfurt",
    "RB Leipzig":           "RB Leipzig",
    "Bayer 04 Leverkusen":  "Leverkusen",
    "TSG 1899 Hoffenheim":  "Hoffenheim",
    "FC Augsburg":          "Augsburg",
    "VfL Wolfsburg":        "Wolfsburg",
    "VfB Stuttgart":        "Stuttgart",
    "SC Freiburg":          "Freiburg",
    "1. FC Köln":           "FC Koln",
    "1. FC Union Berlin":   "Union Berlin",
    "FC Heidenheim 1846":   "Heidenheim",
    # Ligue 1
    "Paris Saint-Germain":  "Paris SG",
    "Olympique Marseille":  "Marseille",
    "Olympique Lyonnais":   "Lyon",
    "Stade Rennais":        "Rennes",
    "Girondins Bordeaux":   "Bordeaux",
    "AS Monaco":            "Monaco",
    "OGC Nice":             "Nice",
    "RC Lens":              "Lens",
    "Stade de Reims":       "Reims",
    "Montpellier HSC":      "Montpellier",
    "RC Strasbourg Alsace": "Strasbourg",
    "Toulouse FC":          "Toulouse",
    # Eredivisie
    "Ajax":                 "Ajax",
    "Feyenoord":            "Feyenoord",
    "PSV Eindhoven":        "PSV",
    "AZ Alkmaar":           "AZ",
}

def normalise_team(name: str) -> str:
    """Returns standardised team name for fuzzy matching."""
    name = str(name).strip()
    # Apply manual map first
    mapped = TEAM_NAME_MAP.get(name, name)
    # Lower + remove common suffixes for fuzzy matching
    clean = re.sub(r"\b(fc|sc|ac|cf|united|city|town|athletic|sporting|club)\b",
                   "", mapped.lower(), flags=re.IGNORECASE)
    clean = re.sub(r"[^a-z0-9\s]", "", clean).strip()
    return clean


def team_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalise_team(a), normalise_team(b)).ratio()


# ═══════════════════════════════════════════════════════════════════════════════
# 5. MERGER (FBref DataFrame + Odds DataFrame)
# ═══════════════════════════════════════════════════════════════════════════════

class OddsMatchMerger:
    """
    Fuzzy-joins FBref match records with FDCO odds records.
    Matching key: (date ±1 day) × (home similarity > 0.75) × (away similarity > 0.75)
    """

    def __init__(self, date_tolerance_days: int = 1, name_threshold: float = 0.72):
        self.date_tol  = date_tolerance_days
        self.name_thr  = name_threshold

    def merge(
        self,
        fbref_df: pd.DataFrame,
        odds_df:  pd.DataFrame
    ) -> pd.DataFrame:
        if odds_df.empty:
            log.warning("Empty odds DataFrame — returning FBref only.")
            return fbref_df

        fbref_df = fbref_df.copy()
        odds_df  = odds_df.copy()

        fbref_df["date"] = pd.to_datetime(fbref_df["date"])
        odds_df["date"]  = pd.to_datetime(odds_df["date"])

        odds_cols = [c for c in odds_df.columns
                     if c.startswith("odds_") or c == "bookie_used"]

        # Pre-build odds lookup by date for speed
        odds_by_date = {}
        for _, row in odds_df.iterrows():
            d = row["date"].date()
            for delta in range(-self.date_tol, self.date_tol + 1):
                key = d + pd.Timedelta(days=delta)
                odds_by_date.setdefault(key, []).append(row)

        matched     = 0
        not_matched = 0
        result_rows = []

        for _, fb_row in fbref_df.iterrows():
            candidates = odds_by_date.get(fb_row["date"].date(), [])
            best_match = None
            best_score = 0.0

            for od_row in candidates:
                sh = team_similarity(fb_row["home"], od_row["home"])
                sa = team_similarity(fb_row["away"], od_row["away"])
                score = (sh + sa) / 2
                if score > best_score and sh >= self.name_thr and sa >= self.name_thr:
                    best_score = score
                    best_match = od_row

            row_dict = fb_row.to_dict()
            if best_match is not None:
                for col in odds_cols:
                    row_dict[col] = best_match.get(col, None)
                row_dict["odds_matched"] = True
                row_dict["match_score"]  = round(best_score, 3)
                matched += 1
            else:
                for col in odds_cols:
                    row_dict[col] = None
                row_dict["odds_matched"] = False
                row_dict["match_score"]  = 0.0
                not_matched += 1

            result_rows.append(row_dict)

        total = matched + not_matched
        log.info(f"  Merge: {matched}/{total} matched "
                 f"({matched/total*100:.1f}%) | "
                 f"unmatched={not_matched}")

        return pd.DataFrame(result_rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. UNIFIED DATASET BUILDER (main entry point)
# ═══════════════════════════════════════════════════════════════════════════════

def build_unified_dataset(
    leagues:     List[str],
    seasons:     List[str],
    fbref_df:    Optional[pd.DataFrame] = None,
    force_download: bool = False,
) -> pd.DataFrame:
    """
    Full pipeline:
      1. Download FDCO CSVs
      2. Parse all odds
      3. Load or accept FBref data (xG)
      4. Merge on (date, home, away)
      5. Return unified DataFrame

    Parameters
    ----------
    leagues      : list of league keys (same as LEAGUES in fbref_scraper.py)
    seasons      : list of season strings e.g. ["2022-2023","2023-2024"]
    fbref_df     : if None, will try to load from SQLite cache (data/fbref_cache.db)
    force_download: re-download even if cached

    Returns
    -------
    pd.DataFrame with columns:
        match_id, league, season, date, home, away,
        home_goals, away_goals, result, xg_home, xg_away,
        odds_H, odds_D, odds_A,
        odds_over2.5, odds_under2.5,
        odds_over3.5, odds_under3.5,
        odds_btts_yes, odds_btts_no,
        odds_matched, bookie_used
    """
    log.info(f"\n{'='*60}")
    log.info(f"APEX-TSS Unified Dataset Builder")
    log.info(f"Leagues : {leagues}")
    log.info(f"Seasons : {seasons}")
    log.info(f"{'='*60}")

    downloader = OddsDownloader()
    parser     = OddsParser()
    merger     = OddsMatchMerger()

    # ── Step 1: FBref data ────────────────────────────────────────────────────
    if fbref_df is None:
        log.info("Loading FBref data from SQLite cache …")
        try:
            from tss.fbref_scraper import FBrefCache
            cache    = FBrefCache()
            fbref_df = cache.load_matches(leagues=leagues, seasons=seasons)
            log.info(f"  FBref: {len(fbref_df)} matches loaded from cache")
        except Exception as e:
            log.warning(f"Could not load FBref cache: {e}")
            fbref_df = pd.DataFrame()

    if fbref_df is None or fbref_df.empty:
        log.warning("No FBref data — scraping needed first. "
                    "Run: python backtesting.py --all")

    # ── Step 2: Download + parse odds ─────────────────────────────────────────
    all_odds_frames = []
    csv_data = downloader.download_all(leagues, seasons, force=force_download)

    for (league, season), csv_text in csv_data.items():
        if csv_text is None:
            log.warning(f"No CSV for {league} {season} — skipping odds")
            continue
        parsed = parser.parse(csv_text, league, season)
        if not parsed.empty:
            all_odds_frames.append(parsed)

    if not all_odds_frames:
        log.warning("No odds data downloaded. "
                    "Check network access to football-data.co.uk")
        return fbref_df if fbref_df is not None else pd.DataFrame()

    odds_df = pd.concat(all_odds_frames, ignore_index=True)
    log.info(f"\nTotal odds records: {len(odds_df)}")

    # ── Step 3: Merge ─────────────────────────────────────────────────────────
    if fbref_df is None or fbref_df.empty:
        log.info("No FBref xG data — using FDCO results only")
        return odds_df

    log.info("\nMerging FBref + odds …")
    unified = []
    for league in leagues:
        fb_lg  = fbref_df[fbref_df["league"] == league].copy()
        od_lg  = odds_df[odds_df["league"]  == league].copy()
        if fb_lg.empty:
            log.warning(f"No FBref data for {league}")
            continue
        if od_lg.empty:
            log.warning(f"No odds data for {league}")
            unified.append(fb_lg)
            continue
        log.info(f"\n── {league}: {len(fb_lg)} FBref + {len(od_lg)} odds")
        merged = merger.merge(fb_lg, od_lg)
        unified.append(merged)

    if not unified:
        return pd.DataFrame()

    final = pd.concat(unified, ignore_index=True)

    # ── Step 4: Quality report ────────────────────────────────────────────────
    _print_quality_report(final)

    return final


def _print_quality_report(df: pd.DataFrame):
    log.info("\n" + "="*60)
    log.info("UNIFIED DATASET — QUALITY REPORT")
    log.info("="*60)
    log.info(f"Total matches  : {len(df)}")

    if "odds_matched" in df.columns:
        n_matched = df["odds_matched"].sum()
        log.info(f"Odds matched   : {n_matched} ({n_matched/len(df)*100:.1f}%)")

    for col in ["odds_H", "odds_over2.5", "odds_btts_yes", "xg_home"]:
        if col in df.columns:
            filled = df[col].notna().sum()
            log.info(f"  {col:20s}: {filled}/{len(df)} ({filled/len(df)*100:.0f}%)")

    if "league" in df.columns:
        log.info("\nPer-league breakdown:")
        breakdown = df.groupby("league").agg(
            matches=("match_id", "count"),
            odds_rate=("odds_matched", lambda x: f"{x.mean()*100:.0f}%" if "odds_matched" in df.columns else "N/A")
        )
        log.info("\n" + breakdown.to_string())


# ═══════════════════════════════════════════════════════════════════════════════
# 7. BACKTEST ADAPTER (plug odds into BacktestRunner)
# ═══════════════════════════════════════════════════════════════════════════════

class RealOddsSignalEngine:
    """
    Variant of TSSSignalEngine that uses REAL book odds instead of simulated ones.
    Drop-in replacement: takes unified DataFrame row, runs gate logic with actual odds.
    """

    MARKETS = ["H", "D", "A", "btts_yes", "btts_no",
               "over2.5", "under2.5", "over3.5", "under3.5"]

    ODDS_COLUMNS = {
        "H":         "odds_H",
        "D":         "odds_D",
        "A":         "odds_A",
        "over2.5":   "odds_over2.5",
        "under2.5":  "odds_under2.5",
        "over3.5":   "odds_over3.5",
        "under3.5":  "odds_under3.5",
        "btts_yes":  "odds_btts_yes",
        "btts_no":   "odds_btts_no",
    }

    def __init__(self, config: Dict, dc_model, odds_sim):
        from tss.backtest_engine import TSSSignalEngine, OddsSimulator
        self.gate_engine = TSSSignalEngine(config)
        self.odds_sim    = odds_sim
        self.dc_model    = dc_model

    def analyze_row(self, row: pd.Series) -> List:
        from tss.backtest_engine import Signal

        home, away = row["home"], row["away"]
        try:
            probs = self.dc_model.predict_probs(home, away)
        except Exception:
            return []

        # Build odds dict: use REAL odds where available, else simulate
        odds_dict = {}
        for market, col in self.ODDS_COLUMNS.items():
            real_odds = row.get(col, None)
            if pd.notna(real_odds) and float(real_odds) > 1.0:
                odds_dict[f"odds_{market}"] = float(real_odds)
            else:
                # Fallback to simulated odds
                sim = self.odds_sim.simulate_odds(probs)
                odds_dict[f"odds_{market}"] = sim.get(f"odds_{market}", 2.0)

        p_book = self.odds_sim.demarginalize(odds_dict)

        signals = self.gate_engine.analyze_match(
            match_id=row.get("match_id", ""),
            date=str(pd.to_datetime(row["date"]).date()),
            home=home, away=away,
            league=row.get("league", ""),
            season=row.get("season", ""),
            probs=probs,
            odds_dict=odds_dict,
            p_book_dict=p_book,
        )

        # Resolve signals
        for sig in signals:
            sig.resolve(
                match_result=str(row.get("result", "")),
                home_goals=int(row.get("home_goals", 0)),
                away_goals=int(row.get("away_goals", 0)),
            )

        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# 8. FULL REAL-ODDS BACKTEST (entry point from backtesting.py)
# ═══════════════════════════════════════════════════════════════════════════════

def run_real_odds_backtest(unified_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """
    Walk-forward backtest using REAL historical odds.
    Uses Dixon-Coles for P_synth, real FDCO odds for P_book via Shin demarg.
    """
    from tss.backtest_engine import (
        WalkForwardSplitter, DixonColesModel,
        OddsSimulator, BacktestRunner
    )
    from dataclasses import asdict

    log.info("\n🎯 Starting REAL-ODDS walk-forward backtest …")

    splitter = WalkForwardSplitter(min_train_seasons=2)
    odds_sim  = OddsSimulator(
        margin=config.get("book_margin", 0.055),
        method=config.get("demarg_method", "shin")
    )

    # Filter to matches with real odds
    has_odds = unified_df["odds_H"].notna() if "odds_H" in unified_df.columns else pd.Series([False]*len(unified_df))
    df_with_odds = unified_df[has_odds].copy()
    df_no_odds   = unified_df[~has_odds].copy()

    log.info(f"  Matches with real odds : {len(df_with_odds)}")
    log.info(f"  Matches without odds   : {len(df_no_odds)} (will use simulated odds)")

    all_signals = []

    try:
        folds = splitter.split(unified_df)
    except ValueError as e:
        log.error(f"Not enough seasons: {e}")
        return pd.DataFrame()

    for fold_idx, (train_df, test_df, label) in enumerate(folds, 1):
        log.info(f"\n{'='*50}")
        log.info(f"FOLD {fold_idx}: {label}")

        for league in test_df["league"].unique():
            train_lg = train_df[train_df["league"] == league].copy()
            test_lg  = test_df[test_df["league"]  == league].copy()

            if len(train_lg) < 50:
                continue

            model = DixonColesModel(xi=config.get("xi", 0.0065))
            try:
                model.fit(train_lg, reference_date=test_lg["date"].min())
            except Exception as e:
                log.error(f"DC fit failed [{league}]: {e}")
                continue

            engine = RealOddsSignalEngine(config, model, odds_sim)

            for _, row in test_lg.iterrows():
                sigs = engine.analyze_row(row)
                all_signals.extend(sigs)

        log.info(f"  Fold BETs so far: {sum(1 for s in all_signals if s.decision=='BET')}")

    if not all_signals:
        log.warning("No signals generated.")
        return pd.DataFrame()

    return pd.DataFrame([asdict(s) for s in all_signals])


# ═══════════════════════════════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    TARGET_LEAGUES  = ["EPL", "Serie A", "La Liga", "Bundesliga", "Ligue 1",
                       "Eredivisie", "Belgian Pro"]
    TARGET_SEASONS  = ["2021-2022", "2022-2023", "2023-2024"]

    unified = build_unified_dataset(
        leagues=TARGET_LEAGUES,
        seasons=TARGET_SEASONS,
        force_download=False
    )

    if not unified.empty:
        print(f"\n✅ Unified dataset: {len(unified)} matches")
        print(unified[["league","season","home","away",
                        "odds_H","odds_D","odds_A","odds_over2.5"]].head(20).to_string())

        # Save for inspection
        out = Path("data/unified_dataset.csv")
        unified.to_csv(out, index=False)
        print(f"\n💾 Saved to {out}")
    else:
        print("❌ No data — check network access to football-data.co.uk")
        print("   Alternatively, place CSV files manually in data/odds_csv/")
        print("   File naming: E0_2324.csv (EPL 2023-24), I1_2223.csv (Serie A 22-23), etc.")
