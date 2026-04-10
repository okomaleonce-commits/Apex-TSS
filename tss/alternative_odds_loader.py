"""
APEX-TSS — Alternative Odds Loader
=====================================
Covers leagues NOT on football-data.co.uk:
  - Brazil Serie A
  - Australia A-League
  - Asia AFC Champions League

3-layer strategy:
  Layer 1 — OddsPortal Selenium scraper (full odds history, requires Chrome)
  Layer 2 — Manual CSV/JSON import (drop files in data/manual_odds/)
  Layer 3 — Synthetic fallback (Dixon-Coles generates P_synth, no real edge — clearly flagged)

Usage:
  from tss.alternative_odds_loader import AlternativeOddsLoader
  loader = AlternativeOddsLoader()
  df = loader.load("Brazil Serie A", ["2022-2023","2023-2024"])
"""

import io
import re
import json
import time
import logging
import hashlib
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [ALT-ODDS] %(message)s")
log = logging.getLogger("alt_odds")

MANUAL_DIR = Path("data/manual_odds")
MANUAL_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR  = Path("data/alt_odds_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# LEAGUE CONFIG FOR ALTERNATIVE SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

ALT_LEAGUES = {
    "Brazil Serie A": {
        "oddsportal_slug": "football/brazil/serie-a",
        "betexplorer_slug": "soccer/brazil/serie-a",
        "manual_prefix":  "brazil_serie_a",
        "season_format":  "calendar",   # Brazil uses calendar year (2023, not 2022-23)
        "typical_teams":  [
            "Atletico Mineiro", "Flamengo", "Palmeiras", "Gremio",
            "Internacional", "Santos", "Sao Paulo", "Corinthians",
            "Botafogo", "Cruzeiro", "Vasco", "Fluminense",
            "America Mineiro", "Goias", "Cuiaba", "Bahia",
            "Athletico-PR", "Bragantino", "Coritiba", "Fortaleza",
        ],
    },
    "A-League": {
        "oddsportal_slug": "football/australia/a-league",
        "betexplorer_slug": "soccer/australia/a-league",
        "manual_prefix":  "a_league",
        "season_format":  "split",      # e.g. 2023-2024
        "typical_teams":  [
            "Melbourne City", "Melbourne Victory", "Sydney FC",
            "Western Sydney Wanderers", "Brisbane Roar", "Adelaide United",
            "Perth Glory", "Wellington Phoenix", "Macarthur FC",
            "Central Coast Mariners", "Western United", "Newcastle Jets",
        ],
    },
    "AFC CL": {
        "oddsportal_slug": "football/asia/afc-champions-league",
        "betexplorer_slug": "soccer/asia/afc-champions-league",
        "manual_prefix":  "afc_champions_league",
        "season_format":  "calendar",
        "typical_teams":  [],           # Varies by year
    },
}

# Season year extraction
def season_years(season_str: str, fmt: str) -> List[int]:
    """Returns calendar years covered by this season."""
    if fmt == "calendar":
        y = int(season_str.split("-")[0])
        return [y]
    else:
        parts = season_str.split("-")
        return [int(parts[0]), int(parts[1])]


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — ODDSPORTAL SELENIUM SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

ODDSPORTAL_SCRAPER_SCRIPT = '''#!/usr/bin/env python3
"""
APEX-TSS — OddsPortal Selenium Scraper
Run this on your local machine (NOT in Claude sandbox).

Requirements:
  pip install selenium webdriver-manager pandas

Usage:
  python oddsportal_scraper.py --league "Brazil Serie A" --season 2023-2024
  python oddsportal_scraper.py --league "A-League"       --season 2023-2024
  python oddsportal_scraper.py --league "AFC CL"         --season 2024

Output: data/manual_odds/<prefix>_<season>.csv
"""

import time
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

LEAGUE_URLS = {
    "Brazil Serie A": "https://www.oddsportal.com/football/brazil/serie-a/results/",
    "A-League":       "https://www.oddsportal.com/football/australia/a-league/results/",
    "AFC CL":         "https://www.oddsportal.com/football/asia/afc-champions-league/results/",
}

LEAGUE_PREFIXES = {
    "Brazil Serie A": "brazil_serie_a",
    "A-League":       "a_league",
    "AFC CL":         "afc_champions_league",
}

OUTPUT_DIR = Path("data/manual_odds")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def init_driver(headless: bool = True) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def scrape_results_page(driver, url: str, page: int = 1) -> list:
    """Scrapes one page of results. Returns list of match dicts."""
    page_url = url if page == 1 else f"{url}#/page/{page}/"
    driver.get(page_url)
    time.sleep(3)

    # Accept cookies if dialog appears
    try:
        btn = driver.find_element(By.XPATH, "//button[contains(text(),'Accept')]")
        btn.click()
        time.sleep(1)
    except Exception:
        pass

    rows = []
    try:
        # Wait for match rows
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='eventRow']"))
        )
        match_rows = driver.find_elements(By.CSS_SELECTOR, "[class*='eventRow']")

        for row in match_rows:
            try:
                # Date/time
                date_el = row.find_elements(By.CSS_SELECTOR, "[class*='date']")
                teams   = row.find_elements(By.CSS_SELECTOR, "[class*='participant']")
                score   = row.find_elements(By.CSS_SELECTOR, "[class*='score']")
                odds_els = row.find_elements(By.CSS_SELECTOR, "[class*='odds-wrap'] span")

                if len(teams) < 2: continue
                home = teams[0].text.strip()
                away = teams[1].text.strip()
                if not home or not away: continue

                score_txt = score[0].text.strip() if score else ""
                goals = _parse_score(score_txt)
                if goals is None: continue

                odds_vals = []
                for el in odds_els[:3]:
                    try:
                        v = float(el.text.strip())
                        if v > 1.0:
                            odds_vals.append(v)
                    except ValueError:
                        pass

                date_str = date_el[0].text.strip() if date_el else ""

                rows.append({
                    "date_raw": date_str,
                    "home": home, "away": away,
                    "score": score_txt,
                    "home_goals": goals[0], "away_goals": goals[1],
                    "odds_H": odds_vals[0] if len(odds_vals) > 0 else None,
                    "odds_D": odds_vals[1] if len(odds_vals) > 1 else None,
                    "odds_A": odds_vals[2] if len(odds_vals) > 2 else None,
                })
            except Exception:
                continue
    except Exception as e:
        print(f"  Page scrape error: {e}")

    return rows


def _parse_score(txt: str):
    for sep in [":", "-", "–"]:
        if sep in txt:
            parts = txt.split(sep)
            try:
                return int(parts[0].strip()), int(parts[1].strip().split()[0])
            except (ValueError, IndexError):
                pass
    return None


def scrape_league(league: str, season: str, max_pages: int = 20) -> pd.DataFrame:
    url    = LEAGUE_URLS[league]
    prefix = LEAGUE_PREFIXES[league]

    print(f"\\nScraping {league} — {season}")
    print(f"URL: {url}")

    driver = init_driver(headless=True)
    all_rows = []

    try:
        for page in range(1, max_pages + 1):
            print(f"  Page {page}...", end=" ")
            rows = scrape_results_page(driver, url, page)
            if not rows:
                print("empty — stopping.")
                break
            all_rows.extend(rows)
            print(f"{len(rows)} matches")
            time.sleep(2)
    finally:
        driver.quit()

    if not all_rows:
        print("No data scraped.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Parse dates (OddsPortal shows relative dates like "10 Apr 2024")
    df["date"] = pd.to_datetime(df["date_raw"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["date"])

    # Add metadata
    df["league"]  = league
    df["season"]  = season
    df["source"]  = "OddsPortal"
    df["result"]  = df.apply(
        lambda r: "H" if r["home_goals"] > r["away_goals"]
                 else ("A" if r["away_goals"] > r["home_goals"] else "D"),
        axis=1
    )

    import hashlib
    df["match_id"] = df.apply(
        lambda r: hashlib.md5(f"{r['date'].date()}_{r['home']}_{r['away']}_{league}".encode()).hexdigest()[:12],
        axis=1
    )

    # Save
    out_path = OUTPUT_DIR / f"{prefix}_{season.replace('-','_')}_oddsportal.csv"
    df.to_csv(out_path, index=False)
    print(f"\\n✅ Saved {len(df)} matches → {out_path}")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--league",  required=True, choices=list(LEAGUE_URLS.keys()))
    parser.add_argument("--season",  required=True)
    parser.add_argument("--pages",   type=int, default=20)
    parser.add_argument("--visible", action="store_true", help="Show browser window")
    args = parser.parse_args()

    df = scrape_league(args.league, args.season, max_pages=args.pages)
    if not df.empty:
        print(df[["date","home","away","home_goals","away_goals","odds_H","odds_D","odds_A"]].head(10).to_string())


if __name__ == "__main__":
    main()
'''


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — MANUAL CSV/JSON IMPORT
# ═══════════════════════════════════════════════════════════════════════════════

MANUAL_FORMAT_SPEC = """
APEX-TSS — Manual Odds Import Format
======================================

Place files in:  data/manual_odds/

ACCEPTED FORMATS:

A) Standard CSV (football-data.co.uk compatible):
   Filename: brazil_serie_a_2023_2024.csv
   Required columns: Date, HomeTeam, AwayTeam, FTHG, FTAG, FTR, B365H, B365D, B365A
   Optional:         B365>2.5, B365<2.5, BTSHY, BTSHN

B) Generic CSV (minimal):
   Filename: a_league_2023_2024_generic.csv
   Required columns: date, home, away, home_goals, away_goals
   Optional:         odds_H, odds_D, odds_A, odds_over2.5, odds_under2.5

C) JSON (APEX-TSS native):
   Filename: afc_champions_league_2024.json
   Format:
   [
     {
       "date": "2024-04-10",
       "home": "Al Hilal",
       "away": "Urawa Reds",
       "home_goals": 2,
       "away_goals": 1,
       "odds_H": 1.75,
       "odds_D": 3.40,
       "odds_A": 5.00,
       "odds_over2.5": 1.85,
       "odds_under2.5": 2.00
     },
     ...
   ]

NAMING CONVENTION:
  brazil_serie_a_2023*.csv        → Brazil Serie A 2023-2024
  brazil_serie_a_2022*.csv        → Brazil Serie A 2022-2023
  a_league_2023_2024*.csv         → A-League 2023-2024
  afc_champions_league_2024*.csv  → AFC CL 2024
"""

class ManualImportParser:
    """
    Parses manually placed CSV/JSON files from data/manual_odds/.
    Auto-detects format (FDCO-style, generic, JSON).
    """

    LEAGUE_PATTERNS = {
        "Brazil Serie A": r"brazil.*serie.*a",
        "A-League":       r"a.?league",
        "AFC CL":         r"afc.*(champions|cl)",
    }

    def scan_and_load(
        self, league: str, seasons: List[str]
    ) -> pd.DataFrame:
        """Find and load all matching files for a league+seasons combo."""
        pattern = self.LEAGUE_PATTERNS.get(league, "")
        frames  = []

        files = list(MANUAL_DIR.glob("*.csv")) + list(MANUAL_DIR.glob("*.json"))
        for f in files:
            if not re.search(pattern, f.name, re.IGNORECASE):
                continue
            log.info(f"  Found manual file: {f.name}")
            df = self._parse_file(f, league)
            if df.empty:
                continue
            # Filter to requested seasons
            df = self._filter_seasons(df, seasons, league)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        result = result.drop_duplicates(subset=["match_id"])
        log.info(f"  Manual import: {len(result)} matches for {league}")
        return result

    def _parse_file(self, path: Path, league: str) -> pd.DataFrame:
        suffix = path.suffix.lower()
        try:
            if suffix == ".json":
                return self._parse_json(path, league)
            elif suffix == ".csv":
                return self._parse_csv(path, league)
        except Exception as e:
            log.error(f"  Parse error [{path.name}]: {e}")
        return pd.DataFrame()

    def _parse_json(self, path: Path, league: str) -> pd.DataFrame:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data = data.get("matches", data.get("data", []))
        df = pd.DataFrame(data)
        return self._standardise(df, league, source=path.name)

    def _parse_csv(self, path: Path, league: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, encoding="utf-8", on_bad_lines="skip", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin-1", on_bad_lines="skip", low_memory=False)

        df.columns = [str(c).strip() for c in df.columns]

        # Detect format
        if "HomeTeam" in df.columns:
            return self._parse_fdco_style(df, league, path.name)
        else:
            return self._standardise(df, league, source=path.name)

    def _parse_fdco_style(self, df: pd.DataFrame, league: str, source: str) -> pd.DataFrame:
        """Handle FDCO-compatible CSV columns."""
        from tss.odds_loader import OddsParser
        parser = OddsParser()
        season = self._guess_season_from_data(df)
        return parser.parse(df.to_csv(index=False), league, season)

    def _standardise(self, df: pd.DataFrame, league: str, source: str) -> pd.DataFrame:
        """Standardise a generic DataFrame into APEX-TSS format."""
        col_map = {}
        for c in df.columns:
            cl = c.lower().strip()
            if cl in ("date",):                            col_map[c] = "date"
            elif cl in ("home", "home_team", "hometeam"): col_map[c] = "home"
            elif cl in ("away", "away_team", "awayteam"): col_map[c] = "away"
            elif cl in ("home_goals", "fthg", "hg", "score_home"): col_map[c] = "home_goals"
            elif cl in ("away_goals", "ftag", "ag", "score_away"): col_map[c] = "away_goals"
            elif cl in ("odds_h", "b365h", "odd_h", "home_odds"):  col_map[c] = "odds_H"
            elif cl in ("odds_d", "b365d", "odd_d", "draw_odds"):  col_map[c] = "odds_D"
            elif cl in ("odds_a", "b365a", "odd_a", "away_odds"):  col_map[c] = "odds_A"
            elif "over" in cl and "2.5" in cl:                     col_map[c] = "odds_over2.5"
            elif "under" in cl and "2.5" in cl:                    col_map[c] = "odds_under2.5"
            elif "over" in cl and "3.5" in cl:                     col_map[c] = "odds_over3.5"
            elif "under" in cl and "3.5" in cl:                    col_map[c] = "odds_under3.5"
            elif "btts" in cl and ("yes" in cl or "y" == cl[-1]):  col_map[c] = "odds_btts_yes"
            elif "btts" in cl and ("no" in cl  or "n" == cl[-1]):  col_map[c] = "odds_btts_no"
            elif cl in ("result", "ftr", "res"):                   col_map[c] = "result"
            elif cl in ("season",):                                col_map[c] = "season"

        df = df.rename(columns=col_map)

        needed = {"date", "home", "away", "home_goals", "away_goals"}
        missing = needed - set(df.columns)
        if missing:
            log.warning(f"  Missing required columns: {missing}")
            return pd.DataFrame()

        df["date"]       = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        df["home_goals"] = pd.to_numeric(df.get("home_goals"), errors="coerce")
        df["away_goals"] = pd.to_numeric(df.get("away_goals"), errors="coerce")
        df = df.dropna(subset=["date","home_goals","away_goals"])
        df["home_goals"] = df["home_goals"].astype(int)
        df["away_goals"] = df["away_goals"].astype(int)

        if "result" not in df.columns:
            df["result"] = df.apply(
                lambda r: "H" if r["home_goals"] > r["away_goals"]
                         else ("A" if r["away_goals"] > r["home_goals"] else "D"),
                axis=1
            )

        if "season" not in df.columns:
            df["season"] = df["date"].apply(self._infer_season)

        df["league"]  = league
        df["source"]  = source
        df["bookie_used"] = "manual"

        # Numeric odds columns
        for col in ["odds_H","odds_D","odds_A","odds_over2.5","odds_under2.5",
                    "odds_over3.5","odds_under3.5","odds_btts_yes","odds_btts_no"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(3)
            else:
                df[col] = None

        df["match_id"] = df.apply(
            lambda r: hashlib.md5(
                f"{r['date'].date()}_{r['home']}_{r['away']}_{league}".encode()
            ).hexdigest()[:12],
            axis=1
        )

        return df.reset_index(drop=True)

    @staticmethod
    def _infer_season(dt: pd.Timestamp) -> str:
        """Infer football season from date."""
        if dt.month >= 7:
            return f"{dt.year}-{dt.year+1}"
        else:
            return f"{dt.year-1}-{dt.year}"

    @staticmethod
    def _guess_season_from_data(df: pd.DataFrame) -> str:
        try:
            dates = pd.to_datetime(df.get("Date", df.get("date", pd.Series())), dayfirst=True, errors="coerce")
            mid   = dates.dropna().median()
            if mid.month >= 7:
                return f"{mid.year}-{mid.year+1}"
            return f"{mid.year-1}-{mid.year}"
        except Exception:
            return "unknown"

    def _filter_seasons(self, df: pd.DataFrame, seasons: List[str], league: str) -> pd.DataFrame:
        cfg = ALT_LEAGUES.get(league, {})
        fmt = cfg.get("season_format", "split")

        if "season" not in df.columns:
            return df

        # Build set of all acceptable season strings
        acceptable = set(seasons)
        # Also add calendar-year aliases (Brazil: "2023" ↔ "2022-2023"/"2023-2024")
        for s in seasons:
            for y in season_years(s, fmt):
                acceptable.add(str(y))
                acceptable.add(f"{y}-{y+1}")
                acceptable.add(f"{y-1}-{y}")

        return df[df["season"].isin(acceptable)].copy()


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — SYNTHETIC FALLBACK (clearly flagged)
# ═══════════════════════════════════════════════════════════════════════════════

class SyntheticOddsFallback:
    """
    When NO real odds are available, generates synthetic odds from Dixon-Coles.
    Clearly flags these rows: odds_source = 'synthetic_DC'
    IMPORTANT: synthetic odds create near-zero real edge (both P_synth and P_book
    come from same model), so BET signals from synthetic odds are filtered by
    a stricter edge gate (edge_min_synthetic > edge_min_real).
    """

    def __init__(self, margin: float = 0.055):
        self.margin = margin

    def generate(self, probs: Dict) -> Dict:
        """probs: output of DixonColesModel.predict_probs()"""
        from tss.backtest_engine import OddsSimulator
        sim = OddsSimulator(margin=self.margin, method="shin")
        odds = sim.simulate_odds(probs)
        odds["odds_source"] = "synthetic_DC"
        odds["bookie_used"] = "synthetic"
        return odds

    def flag_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add odds_source column to mark synthetic rows."""
        if "odds_source" not in df.columns:
            df["odds_source"] = "real"
        has_odds = df["odds_H"].notna()
        df.loc[~has_odds, "odds_source"] = "synthetic_DC"
        return df


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOADER CLASS (orchestrates all 3 layers)
# ═══════════════════════════════════════════════════════════════════════════════

class AlternativeOddsLoader:
    """
    Primary interface. Tries layers in order:
      1. OddsPortal cached CSV (already scraped by oddsportal_scraper.py)
      2. Manual file import (data/manual_odds/)
      3. Synthetic fallback
    """

    def __init__(self):
        self.manual_parser = ManualImportParser()
        self.synthetic     = SyntheticOddsFallback()

    def load(
        self,
        league:  str,
        seasons: List[str],
        fbref_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        log.info(f"\n── AlternativeOddsLoader: {league} | {seasons}")

        if league not in ALT_LEAGUES:
            log.error(f"League not in ALT_LEAGUES: {league}")
            return pd.DataFrame()

        # Layer 1 + 2: check data/manual_odds/ for OddsPortal output OR manual files
        manual_df = self.manual_parser.scan_and_load(league, seasons)

        if not manual_df.empty:
            log.info(f"  ✅ Layer 1/2: {len(manual_df)} matches from manual/OddsPortal files")
            # Merge with FBref xG if available
            if fbref_df is not None and not fbref_df.empty:
                manual_df = self._enrich_with_xg(manual_df, fbref_df, league)
            manual_df["odds_source"] = "real"
            return manual_df

        # Layer 3: Synthetic fallback from FBref
        if fbref_df is not None and not fbref_df.empty:
            log.warning(f"  ⚠️  Layer 3: No real odds found for {league}. "
                        f"Using SYNTHETIC odds (flagged). "
                        f"BETs from these rows are LOW CONFIDENCE.")
            return self._generate_synthetic(league, seasons, fbref_df)

        log.error(f"  ❌ No data available for {league} {seasons}. "
                  f"Run oddsportal_scraper.py or place files in data/manual_odds/")
        return pd.DataFrame()

    def _enrich_with_xg(
        self, odds_df: pd.DataFrame, fbref_df: pd.DataFrame, league: str
    ) -> pd.DataFrame:
        """Add xG columns from FBref to odds DataFrame via fuzzy match."""
        from tss.alternative_odds_loader import ManualImportParser
        from tss.odds_loader import OddsMatchMerger

        fb_lg = fbref_df[fbref_df["league"] == league][["date","home","away","xg_home","xg_away"]].copy()
        if fb_lg.empty:
            return odds_df

        # Simple date+team merge
        merger = OddsMatchMerger(date_tolerance_days=1, name_threshold=0.65)
        # Use the merger to attach xG — swap role: odds is "fbref", fbref is "odds"
        # Build a pseudo-odds-df with just xg columns
        xg_as_odds = fb_lg.copy()
        xg_as_odds["odds_H"]       = None
        xg_as_odds["bookie_used"]  = "xg_only"
        xg_as_odds["league"]       = league

        enriched = merger.merge(odds_df, xg_as_odds)
        # Only pick up xg columns if present
        if "xg_home" not in enriched.columns:
            enriched["xg_home"] = None
            enriched["xg_away"] = None

        return enriched

    def _generate_synthetic(
        self, league: str, seasons: List[str], fbref_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Generate synthetic odds using Dixon-Coles for the entire dataset."""
        from tss.backtest_engine import (
            WalkForwardSplitter, DixonColesModel, OddsSimulator
        )

        df = fbref_df[
            (fbref_df["league"] == league) &
            (fbref_df["season"].isin(seasons))
        ].copy()

        if df.empty:
            return pd.DataFrame()

        all_seasons = sorted(df["season"].unique())
        if len(all_seasons) < 2:
            log.warning(f"Need ≥2 seasons for synthetic, have {len(all_seasons)}")
            # Use all data for fit
            train = df.copy()
        else:
            train = df[df["season"] != all_seasons[-1]].copy()

        model = DixonColesModel()
        try:
            model.fit(train)
        except Exception as e:
            log.error(f"DC fit failed: {e}")
            return df

        sim = OddsSimulator(margin=0.055, method="shin")
        odds_cols = []

        for idx, row in df.iterrows():
            try:
                probs = model.predict_probs(row["home"], row["away"])
                odds  = sim.simulate_odds(probs)
                for col, val in odds.items():
                    df.at[idx, col] = val
            except Exception:
                pass

        df["odds_source"] = "synthetic_DC"
        df["bookie_used"] = "synthetic_DC"

        log.warning(f"  Generated synthetic odds for {len(df)} matches in {league}. "
                    f"Edge validation will be near-zero — treat as calibration only.")
        return df

    def generate_readme(self) -> str:
        """Returns the manual format specification doc."""
        return MANUAL_FORMAT_SPEC


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION WITH MAIN ODDS LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def extend_unified_dataset(
    unified_df:  pd.DataFrame,
    alt_leagues: List[str],
    seasons:     List[str],
    fbref_df:    Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Adds alternative-source leagues to an existing unified_df
    (which was built from FDCO leagues).

    Called from backtesting.py after build_unified_dataset().
    """
    loader = AlternativeOddsLoader()
    frames = [unified_df] if not unified_df.empty else []

    for league in alt_leagues:
        fb_lg = None
        if fbref_df is not None and not fbref_df.empty:
            fb_lg = fbref_df[fbref_df["league"] == league]

        alt_df = loader.load(league, seasons, fbref_df=fb_lg)
        if not alt_df.empty:
            # Align columns
            for col in unified_df.columns:
                if col not in alt_df.columns:
                    alt_df[col] = None
            frames.append(alt_df[unified_df.columns])

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Quality summary
    if "odds_source" in result.columns:
        src_counts = result["odds_source"].value_counts()
        log.info("\n── Odds Source Breakdown ──")
        for src, cnt in src_counts.items():
            pct = cnt / len(result) * 100
            icon = "✅" if src == "real" else "⚠️ "
            log.info(f"  {icon} {src:20s}: {cnt:5d} ({pct:.1f}%)")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SAVE ODDSPORTAL SCRAPER SCRIPT
# ═══════════════════════════════════════════════════════════════════════════════

def save_scraper_script(output_path: str = "oddsportal_scraper.py"):
    """Writes the standalone OddsPortal Selenium scraper to disk."""
    Path(output_path).write_text(ODDSPORTAL_SCRAPER_SCRIPT, encoding="utf-8")
    log.info(f"✅ OddsPortal scraper saved: {output_path}")
    return output_path


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="APEX-TSS Alternative Odds Loader"
    )
    parser.add_argument("--league",  choices=list(ALT_LEAGUES.keys()))
    parser.add_argument("--seasons", nargs="+", default=["2022-2023","2023-2024"])
    parser.add_argument("--save-scraper", action="store_true",
                        help="Save the OddsPortal Selenium scraper to disk")
    parser.add_argument("--readme",  action="store_true",
                        help="Print manual import format specification")
    args = parser.parse_args()

    if args.save_scraper:
        save_scraper_script()
        print("\nNext step:")
        print("  pip install selenium webdriver-manager")
        print("  python oddsportal_scraper.py --league 'Brazil Serie A' --season 2023-2024")
        print("  python oddsportal_scraper.py --league 'A-League'       --season 2023-2024")
        print("  python oddsportal_scraper.py --league 'AFC CL'         --season 2024")

    elif args.readme:
        loader = AlternativeOddsLoader()
        print(loader.generate_readme())

    elif args.league:
        loader = AlternativeOddsLoader()
        df = loader.load(args.league, args.seasons)
        print(f"\nLoaded: {len(df)} matches")
        if not df.empty:
            print(df[["date","home","away","odds_H","odds_D","odds_A","odds_source"]].head(20).to_string())
    else:
        parser.print_help()
