#!/usr/bin/env python3
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

    print(f"\nScraping {league} — {season}")
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
    print(f"\n✅ Saved {len(df)} matches → {out_path}")

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
