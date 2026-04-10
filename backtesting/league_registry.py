"""
APEX-TSS Backtesting — League Registry v1.0
Mapping: 30 leagues → FBref IDs + football-data.co.uk codes + metadata
"""

LEAGUE_REGISTRY = {
    # ─────────────── TIER P0 — UEFA Big 5 ───────────────
    "serie_a": {
        "label": "Italy - Serie A",
        "tier": "P0",
        "fbref_id": 11,
        "fbref_slug": "Serie-A",
        "fdco_code": "I1",          # football-data.co.uk code
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "la_liga": {
        "label": "Spain - La Liga",
        "tier": "P0",
        "fbref_id": 12,
        "fbref_slug": "La-Liga",
        "fdco_code": "SP1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "bundesliga": {
        "label": "Germany - Bundesliga",
        "tier": "P0",
        "fbref_id": 20,
        "fbref_slug": "Fussball-Bundesliga",
        "fdco_code": "D1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "epl": {
        "label": "England - Premier League",
        "tier": "P0",
        "fbref_id": 9,
        "fbref_slug": "Premier-League",
        "fdco_code": "E0",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "ligue_1": {
        "label": "France - Ligue 1",
        "tier": "P0",
        "fbref_id": 13,
        "fbref_slug": "Ligue-1",
        "fdco_code": "F1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },

    # ─────────────── TIER P1 — Secondaires forts ───────────────
    "eredivisie": {
        "label": "Netherlands - Eredivisie",
        "tier": "P1",
        "fbref_id": 23,
        "fbref_slug": "Eredivisie",
        "fdco_code": "N1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "liga_portugal": {
        "label": "Portugal - Primeira Liga",
        "tier": "P1",
        "fbref_id": 32,
        "fbref_slug": "Primeira-Liga",
        "fdco_code": "P1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "scottish_premiership": {
        "label": "Scotland - Premiership",
        "tier": "P1",
        "fbref_id": 40,
        "fbref_slug": "Scottish-Premiership",
        "fdco_code": "SC0",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "turkey_super_lig": {
        "label": "Turkey - Süper Lig",
        "tier": "P1",
        "fbref_id": 26,
        "fbref_slug": "Super-Lig",
        "fdco_code": "T1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "belgium_pro_league": {
        "label": "Belgium - Pro League",
        "tier": "P1",
        "fbref_id": 37,
        "fbref_slug": "Belgian-First-Division-A",
        "fdco_code": "B1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "austria_bundesliga": {
        "label": "Austria - Bundesliga",
        "tier": "P1",
        "fbref_id": 18,
        "fbref_slug": "Austrian-Football-Bundesliga",
        "fdco_code": "A1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "greece_super_league": {
        "label": "Greece - Super League",
        "tier": "P1",
        "fbref_id": 27,
        "fbref_slug": "Super-League-1",
        "fdco_code": "G1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },

    # ─────────────── TIER P2 — Secondaires élargis ───────────────
    "england_championship": {
        "label": "England - Championship",
        "tier": "P2",
        "fbref_id": 10,
        "fbref_slug": "Championship",
        "fdco_code": "E1",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "efl_league_one": {
        "label": "England - EFL League One",
        "tier": "P2",
        "fbref_id": 15,
        "fbref_slug": "EFL-League-One",
        "fdco_code": "E2",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "bundesliga_2": {
        "label": "Germany - 2. Bundesliga",
        "tier": "P2",
        "fbref_id": 33,
        "fbref_slug": "2-Bundesliga",
        "fdco_code": "D2",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "ligue_2": {
        "label": "France - Ligue 2",
        "tier": "P2",
        "fbref_id": 60,
        "fbref_slug": "Ligue-2",
        "fdco_code": "F2",
        "fdco_available": True,
        "seasons_available": ["2021-22", "2022-23", "2023-24", "2024-25"],
    },
    "denmark_superliga": {
        "label": "Denmark - Superliga",
        "tier": "P2",
        "fbref_id": 50,
        "fbref_slug": "Danish-Superliga",
        "fdco_code": None,
        "fdco_available": False,   # Odds manquantes — flags ODDS_MISSING
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "ukraine_premier_league": {
        "label": "Ukraine - Premier League",
        "tier": "P2",
        "fbref_id": 39,
        "fbref_slug": "Ukrainian-Premier-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "israel_premier_league": {
        "label": "Israel - Premier League",
        "tier": "P2",
        "fbref_id": 55,
        "fbref_slug": "Israeli-Premier-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },

    # ─────────────── TIER P3 — Régionaux / Intercontinentaux ───────────────
    "champions_league": {
        "label": "Europe - UEFA Champions League",
        "tier": "P3",
        "fbref_id": 8,
        "fbref_slug": "Champions-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "europa_league": {
        "label": "Europe - UEFA Europa League",
        "tier": "P3",
        "fbref_id": 19,
        "fbref_slug": "Europa-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "conference_league": {
        "label": "Europe - UEFA Europa Conference League",
        "tier": "P3",
        "fbref_id": 882,
        "fbref_slug": "Europa-Conference-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "russia_premier_league": {
        "label": "Russia - Premier League",
        "tier": "P3",
        "fbref_id": 30,
        "fbref_slug": "Russian-Premier-League",
        "fdco_code": None,
        "fdco_available": False,   # Sanctionné — données partielles
        "seasons_available": ["2021-22", "2022-23"],
        "warning": "Coverage degraded post-2022 due to sanctions",
    },
    "saudi_pro_league": {
        "label": "Saudi Arabia - Pro League",
        "tier": "P3",
        "fbref_id": 70,
        "fbref_slug": "Saudi-Professional-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "argentina_primera": {
        "label": "Argentina - Primera División",
        "tier": "P3",
        "fbref_id": 21,
        "fbref_slug": "Primera-Division",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022", "2023", "2024"],
    },
    "brazil_serie_a": {
        "label": "Brazil - Serie A",
        "tier": "P3",
        "fbref_id": 24,
        "fbref_slug": "Serie-A",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022", "2023", "2024"],
    },
    "chile_primera": {
        "label": "Chile - Primera División",
        "tier": "P3",
        "fbref_id": 35,
        "fbref_slug": "Primera-Division",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022", "2023", "2024"],
    },

    # ─────────────── TIER N5 — Surveillance only ───────────────
    "afc_champions_league": {
        "label": "Asia - AFC Champions League",
        "tier": "N5",
        "fbref_id": 36,
        "fbref_slug": "AFC-Champions-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2023", "2024"],
    },
    "australia_aleague": {
        "label": "Australia - A-League",
        "tier": "N5",
        "fbref_id": 53,
        "fbref_slug": "A-League-Men",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022-23", "2023-24", "2024-25"],
    },
    "china_csl": {
        "label": "China - Chinese Super League",
        "tier": "N5",
        "fbref_id": 57,
        "fbref_slug": "Chinese-Super-League",
        "fdco_code": None,
        "fdco_available": False,
        "seasons_available": ["2022", "2023", "2024"],
    },
}

# ─── Helpers ───────────────────────────────────────────────────────────────

def get_leagues_by_tier(tier: str) -> dict:
    return {k: v for k, v in LEAGUE_REGISTRY.items() if v["tier"] == tier}

def get_leagues_with_odds() -> dict:
    return {k: v for k, v in LEAGUE_REGISTRY.items() if v["fdco_available"]}

def get_leagues_without_odds() -> dict:
    return {k: v for k, v in LEAGUE_REGISTRY.items() if not v["fdco_available"]}

def get_fdco_url(fdco_code: str, season: str) -> str:
    """
    football-data.co.uk CSV URL.
    season format: '2122' pour 2021-22, '2324' pour 2023-24
    """
    s = season.replace("-", "").replace("/", "")
    if len(s) == 6:
        s = s[2:4] + s[4:6]   # '202122' → '2122'
    return f"https://www.football-data.co.uk/mmz4281/{s}/{fdco_code}.csv"

def get_fbref_schedule_url(fbref_id: int, fbref_slug: str, season: str) -> str:
    """
    FBref schedule URL.
    season format: '2021-2022'
    """
    return f"https://fbref.com/en/comps/{fbref_id}/{season}/schedule/{season}-{fbref_slug}-Scores-and-Fixtures"

if __name__ == "__main__":
    print(f"Total leagues registered: {len(LEAGUE_REGISTRY)}")
    print(f"Leagues WITH odds (football-data.co.uk): {len(get_leagues_with_odds())}")
    print(f"Leagues WITHOUT odds (ODDS_MISSING flag): {len(get_leagues_without_odds())}")
    for tier in ["P0", "P1", "P2", "P3", "N5"]:
        leagues = get_leagues_by_tier(tier)
        print(f"  {tier}: {[v['label'] for v in leagues.values()]}")
