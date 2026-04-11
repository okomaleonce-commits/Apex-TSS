"""
APEX-TSS — Fixture Fetcher
============================
Fetches upcoming fixtures from football-data.org v4 API.

Supported commands:
  /scan today          → matchs du jour
  /scan 48h            → prochaines 48 heures
  /scan week           → 7 prochains jours
  /scan 12/04          → date spécifique
  /scan 12/04-14/04    → plage de dates
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

log = logging.getLogger("fixture_fetcher")

FDORG_TOKEN = os.environ.get("FDORG_TOKEN", "b8b980d46849a1fc55c8bd271bcad18c")
FDORG_BASE  = "https://api.football-data.org/v4"

# league code → competition id on football-data.org
COMPETITION_MAP = {
    "EPL":        "PL",    # Premier League
    "Serie A":    "SA",    # Serie A
    "La Liga":    "PD",    # Primera Division
    "Bundesliga": "BL1",   # Bundesliga
    "Ligue 1":    "FL1",   # Ligue 1
    "Eredivisie": "DED",   # Eredivisie
    "Belgian Pro":"BSA",   # Belgian First Division A
}

CACHED_LEAGUES = list(COMPETITION_MAP.keys())


def _headers() -> Dict:
    return {"X-Auth-Token": FDORG_TOKEN}


def _fetch_fixtures(date_from: str, date_to: str) -> List[Dict]:
    """Fetch all matches in date range across cached leagues."""
    matches = []
    for league, comp_id in COMPETITION_MAP.items():
        try:
            url = f"{FDORG_BASE}/competitions/{comp_id}/matches"
            r   = requests.get(url, headers=_headers(),
                               params={"dateFrom": date_from, "dateTo": date_to},
                               timeout=15)
            if r.status_code == 200:
                data = r.json()
                for m in data.get("matches", []):
                    if m.get("status") in ("SCHEDULED", "TIMED", "IN_PLAY", "LIVE"):
                        matches.append({
                            "league":    league,
                            "home":      m["homeTeam"]["name"],
                            "away":      m["awayTeam"]["name"],
                            "date":      m["utcDate"][:10],
                            "time":      m["utcDate"][11:16],
                            "status":    m["status"],
                            "match_id":  str(m["id"]),
                        })
            elif r.status_code == 403:
                log.warning(f"API 403 for {league} — plan may not include this competition")
            else:
                log.warning(f"API {r.status_code} for {league}: {r.text[:100]}")
        except Exception as e:
            log.error(f"Fixture fetch error [{league}]: {e}")

    return matches


def parse_scan_window(text: str) -> Tuple[str, str, str]:
    """
    Parse time window from command text.
    Returns: (date_from, date_to, label)

    Examples:
      'today'       → today only
      '48h'         → next 48 hours
      'week'        → next 7 days
      '12/04'       → April 12
      '12/04-14/04' → April 12–14
    """
    text  = text.strip().lower()
    today = datetime.utcnow().date()

    if text in ("today", "aujourd'hui", "auj", "ce soir", "jour"):
        d_from = today
        d_to   = today
        label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"

    elif text in ("48h", "48"):
        d_from = today
        d_to   = today + timedelta(days=2)
        label  = f"Prochaines 48h ({today.strftime('%d/%m')} → {d_to.strftime('%d/%m')})"

    elif text in ("week", "semaine", "7j", "7d"):
        d_from = today
        d_to   = today + timedelta(days=7)
        label  = f"Cette semaine ({today.strftime('%d/%m')} → {d_to.strftime('%d/%m')})"

    elif text in ("tomorrow", "demain"):
        d_from = today + timedelta(days=1)
        d_to   = d_from
        label  = f"Demain {d_from.strftime('%d/%m/%Y')}"

    elif "-" in text and "/" in text:
        # Range: 12/04-14/04
        try:
            parts  = text.split("-")
            d_from = _parse_date(parts[0].strip(), today.year)
            d_to   = _parse_date(parts[1].strip(), today.year)
            label  = f"{d_from.strftime('%d/%m')} → {d_to.strftime('%d/%m')}"
        except Exception:
            d_from = today
            d_to   = today + timedelta(days=2)
            label  = "48h (défaut)"

    elif "/" in text or len(text) in (4, 5):
        # Single date: 12/04 or 1204
        try:
            d_from = _parse_date(text, today.year)
            d_to   = d_from
            label  = d_from.strftime("%d/%m/%Y")
        except Exception:
            d_from = today
            d_to   = today
            label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"
    else:
        # Default: today
        d_from = today
        d_to   = today
        label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"

    return str(d_from), str(d_to), label


def _parse_date(text: str, year: int):
    """Parse 'dd/mm' or 'dd/mm/yyyy' into date object."""
    import re
    text = text.strip()
    m = re.match(r"(\d{1,2})[/\-\.](\d{1,2})(?:[/\-\.](\d{2,4}))?", text)
    if m:
        d  = int(m.group(1))
        mo = int(m.group(2))
        y  = int(m.group(3)) if m.group(3) else year
        y  = 2000 + y if y < 100 else y
        from datetime import date
        return date(y, mo, d)
    raise ValueError(f"Cannot parse date: {text}")


def get_fixtures(window_text: str) -> Tuple[List[Dict], str]:
    """
    Main entry point.
    Returns: (list of fixture dicts, window label)
    """
    date_from, date_to, label = parse_scan_window(window_text)
    log.info(f"Fetching fixtures: {date_from} → {date_to}")
    fixtures = _fetch_fixtures(date_from, date_to)
    log.info(f"Found {len(fixtures)} fixtures in window")
    return fixtures, label
