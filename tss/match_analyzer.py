"""
APEX-TSS — Live Match Analyzer
================================
Reçoit un texte libre ("PSG vs Lyon", "Naples - Lazio 13/04"),
détecte la ligue, charge le modèle Dixon-Coles depuis le cache FBref,
génère les cotes et retourne les marchés avec signal fort.

Flow:
  1. parse_match_text()     → extrait home, away, date
  2. detect_league()        → identifie la ligue via fuzzy matching
  3. load_or_fit_dc_model() → charge ou entraîne Dixon-Coles
  4. run_tss_gates()        → filtre Gate-0/1/2/3
  5. format_telegram_msg()  → message structuré pour le bot
"""

import re
import json
import logging
import hashlib
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("match_analyzer")

CACHE_DB   = Path("data/fbref_cache.db")
MODEL_DIR  = Path("data/dc_models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# ── Gate config (lit config.json si dispo) ────────────────────────────────────
def _load_gates() -> Dict:
    try:
        cfg = json.loads(Path("config.json").read_text())
        base = {
            "ev_min":         cfg.get("backtest", cfg.get("gates", {})).get("ev_min",   0.01),
            "edge_min":       cfg.get("backtest", cfg.get("gates", {})).get("edge_min", 0.02),
            "odds_min":       cfg.get("backtest", {}).get("odds_min",       1.30),
            "odds_max":       cfg.get("backtest", {}).get("odds_max",       5.50),
            "kelly_fraction": cfg.get("backtest", {}).get("kelly_fraction", 0.20),
            "max_stake_pct":  cfg.get("backtest", {}).get("max_stake_pct",  0.025),
            "dcs_min":        cfg.get("backtest", {}).get("dcs_min",        0.50),
            "book_margin":    cfg.get("backtest", {}).get("book_margin",    0.055),
        }
        # Runtime override (from /setgates command)
        override_path = Path("data/gates_override.json")
        if override_path.exists():
            overrides = json.loads(override_path.read_text())
            base.update(overrides)
        return base
    except Exception:
        return {
            "ev_min": 0.01, "edge_min": 0.02, "odds_min": 1.30, "odds_max": 5.50,
            "kelly_fraction": 0.20, "max_stake_pct": 0.025, "dcs_min": 0.50,
            "book_margin": 0.055,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 1. TEAM → LEAGUE DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

TEAM_LEAGUE_MAP = {
    # Serie A
    "Napoli":"Serie A","Inter":"Serie A","Milan":"Serie A","Juventus":"Serie A",
    "Roma":"Serie A","Lazio":"Serie A","Atalanta":"Serie A","Fiorentina":"Serie A",
    "Bologna":"Serie A","Torino":"Serie A","Monza":"Serie A","Lecce":"Serie A",
    "Genoa":"Serie A","Verona":"Serie A","Cagliari":"Serie A","Empoli":"Serie A",
    "Udinese":"Serie A","Frosinone":"Serie A","Salernitana":"Serie A","Sassuolo":"Serie A",
    "Como":"Serie A","Venezia":"Serie A","Parma":"Serie A",

    # EPL
    "Arsenal":"EPL","Chelsea":"EPL","Liverpool":"EPL","Manchester City":"EPL",
    "Manchester United":"EPL","Tottenham":"EPL","Newcastle":"EPL","Brighton":"EPL",
    "Aston Villa":"EPL","West Ham":"EPL","Wolves":"EPL","Everton":"EPL",
    "Fulham":"EPL","Brentford":"EPL","Crystal Palace":"EPL","Bournemouth":"EPL",
    "Nottingham Forest":"EPL","Leicester":"EPL","Southampton":"EPL","Ipswich":"EPL",
    "Man City":"EPL","Man United":"EPL","Spurs":"EPL",

    # La Liga
    "Real Madrid":"La Liga","Barcelona":"La Liga","Atletico Madrid":"La Liga",
    "Sevilla":"La Liga","Athletic Club":"La Liga","Villarreal":"La Liga",
    "Real Sociedad":"La Liga","Betis":"La Liga","Valencia":"La Liga",
    "Celta Vigo":"La Liga","Getafe":"La Liga","Osasuna":"La Liga",
    "Girona":"La Liga","Las Palmas":"La Liga","Alaves":"La Liga",
    "Rayo Vallecano":"La Liga","Mallorca":"La Liga","Espanyol":"La Liga",

    # Bundesliga
    "Bayern Munich":"Bundesliga","Dortmund":"Bundesliga","Leverkusen":"Bundesliga",
    "RB Leipzig":"Bundesliga","Frankfurt":"Bundesliga","Wolfsburg":"Bundesliga",
    "Stuttgart":"Bundesliga","Freiburg":"Bundesliga","Hoffenheim":"Bundesliga",
    "Augsburg":"Bundesliga","Mainz":"Bundesliga","Werder Bremen":"Bundesliga",
    "Union Berlin":"Bundesliga","Heidenheim":"Bundesliga","Bochum":"Bundesliga",
    "Holstein Kiel":"Bundesliga","St. Pauli":"Bundesliga",

    # Ligue 1
    "PSG":"Ligue 1","Paris Saint-Germain":"Ligue 1","Marseille":"Ligue 1",
    "Lyon":"Ligue 1","Monaco":"Ligue 1","Nice":"Ligue 1","Lens":"Ligue 1",
    "Lille":"Ligue 1","Rennes":"Ligue 1","Montpellier":"Ligue 1",
    "Strasbourg":"Ligue 1","Toulouse":"Ligue 1","Nantes":"Ligue 1",
    "Reims":"Ligue 1","Brest":"Ligue 1","Le Havre":"Ligue 1",
    "Auxerre":"Ligue 1","Angers":"Ligue 1","Saint-Etienne":"Ligue 1",

    # Eredivisie
    "Ajax":"Eredivisie","PSV":"Eredivisie","Feyenoord":"Eredivisie",
    "AZ":"Eredivisie","Twente":"Eredivisie","Utrecht":"Eredivisie",

    # Belgian Pro
    "Club Bruges":"Belgian Pro","Anderlecht":"Belgian Pro","Genk":"Belgian Pro",
    "Gent":"Belgian Pro","Standard":"Belgian Pro","Antwerp":"Belgian Pro",

    # Brazil Serie A
    "Flamengo":"Brazil Serie A","Palmeiras":"Brazil Serie A",
    "Atletico Mineiro":"Brazil Serie A","Fluminense":"Brazil Serie A",
    "Botafogo":"Brazil Serie A","Corinthians":"Brazil Serie A",
    "Internacional":"Brazil Serie A","Gremio":"Brazil Serie A",
    "Santos":"Brazil Serie A","Sao Paulo":"Brazil Serie A",

    # A-League
    "Melbourne City":"A-League","Melbourne Victory":"A-League",
    "Sydney FC":"A-League","Western Sydney":"A-League",
    "Brisbane Roar":"A-League","Adelaide United":"A-League",
    "Perth Glory":"A-League","Wellington Phoenix":"A-League",

    # Eliteserien (Norway)
    "Bodø/Glimt":"Eliteserien","Bodo/Glimt":"Eliteserien","Glimt":"Eliteserien",
    "Viking FK":"Eliteserien","Viking":"Eliteserien",
    "Molde":"Eliteserien","Rosenborg":"Eliteserien","Brann":"Eliteserien",
    "Tromsø":"Eliteserien","Tromso":"Eliteserien","Vålerenga":"Eliteserien",
    "Valerenga":"Eliteserien","Fredrikstad":"Eliteserien","Lillestrøm":"Eliteserien",
    "Lillestrom":"Eliteserien","Odd":"Eliteserien","Sandefjord":"Eliteserien",
    "Haugesund":"Eliteserien","Aalesund":"Eliteserien","Stabæk":"Eliteserien",
    "HamKam":"Eliteserien","Sarpsborg":"Eliteserien","Kristiansund":"Eliteserien",

    # Allsvenskan (Sweden)
    "Malmö FF":"Allsvenskan","Malmo":"Allsvenskan","AIK":"Allsvenskan",
    "Djurgården":"Allsvenskan","Djurgarden":"Allsvenskan",
    "Hammarby":"Allsvenskan","IFK Göteborg":"Allsvenskan","Göteborg":"Allsvenskan",
    "Helsingborg":"Allsvenskan","BK Häcken":"Allsvenskan","Hacken":"Allsvenskan",
    "Elfsborg":"Allsvenskan","Kalmar":"Allsvenskan","Sirius":"Allsvenskan",

    # Superligaen (Denmark)
    "FC Copenhagen":"Superligaen","Copenhagen":"Superligaen",
    "Brøndby":"Superligaen","Brondby":"Superligaen",
    "FC Midtjylland":"Superligaen","Midtjylland":"Superligaen",
    "AGF":"Superligaen","Silkeborg":"Superligaen","Randers":"Superligaen",
    "Viborg":"Superligaen","OB":"Superligaen","Lyngby":"Superligaen",

    # Primeira Liga (Portugal)
    "Benfica":"Primeira Liga","Porto":"Primeira Liga","Sporting CP":"Primeira Liga",
    "Braga":"Primeira Liga","Vitória SC":"Primeira Liga","Guimarães":"Primeira Liga",
    "Rio Ave":"Primeira Liga","Famalicão":"Primeira Liga","Estoril":"Primeira Liga",
    "Moreirense":"Primeira Liga","Boavista":"Primeira Liga","Casa Pia":"Primeira Liga",
}

# Aliases pour le parsing libre
TEAM_ALIASES = {
    "psg": "PSG", "barca": "Barcelona", "bayer": "Leverkusen",
    "inter milan": "Inter", "ac milan": "Milan", "napoles": "Napoli",
    "atletico": "Atletico Madrid", "atm": "Atletico Madrid",
    "real": "Real Madrid", "rm": "Real Madrid",
    "mancity": "Manchester City", "mcfc": "Manchester City",
    "manutd": "Manchester United", "mufc": "Manchester United",
    "juve": "Juventus", "juventus": "Juventus",
    "dortmund": "Dortmund", "bvb": "Dortmund",
    "frankfurt": "Frankfurt", "ein frankfurt": "Frankfurt",
    "spurs": "Tottenham",
}


def _normalise(name: str) -> str:
    return re.sub(r"[^a-z0-9\s]", "", name.lower().strip())


def _best_team_match(raw: str) -> Optional[str]:
    """Fuzzy match raw name → canonical team name."""
    raw_lower = raw.lower().strip()

    # Direct alias check
    if raw_lower in TEAM_ALIASES:
        return TEAM_ALIASES[raw_lower]

    # Exact match (case-insensitive)
    for team in TEAM_LEAGUE_MAP:
        if team.lower() == raw_lower:
            return team

    # Fuzzy match
    best_score, best_team = 0.0, None
    for team in list(TEAM_LEAGUE_MAP.keys()) + list(TEAM_ALIASES.values()):
        score = SequenceMatcher(None, _normalise(raw), _normalise(team)).ratio()
        if score > best_score:
            best_score, best_team = score, team

    return best_team if best_score >= 0.65 else None


def _detect_league(home: str, away: str) -> Optional[str]:
    """Detect league from team names."""
    h_team = _best_team_match(home)
    a_team = _best_team_match(away)

    h_league = TEAM_LEAGUE_MAP.get(h_team) if h_team else None
    a_league = TEAM_LEAGUE_MAP.get(a_team) if a_team else None

    if h_league and h_league == a_league:
        return h_league
    if h_league:
        return h_league
    if a_league:
        return a_league
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 2. FREE TEXT PARSER
# ═══════════════════════════════════════════════════════════════════════════════

SEPARATORS = re.compile(r"\s+vs\.?\s+|\s+v\.?\s+|\s*[-–—]\s*|\s+contre\s+", re.IGNORECASE)
DATE_PATTERNS = [
    (re.compile(r"(\d{1,2})[/\-\.](\d{1,2})(?:[/\-\.](\d{2,4}))?"), "dmy"),
    (re.compile(r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})"),          "ymd"),
]
MONTHS_FR = {
    "jan":1,"fev":2,"mar":3,"avr":4,"mai":5,"juin":6,
    "juil":7,"aou":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,
}


def parse_match_text(text: str) -> Tuple[str, str, Optional[datetime], Optional[str]]:
    """
    Parse free text → (home, away, date, league_hint)
    Handles multiple formats:
      "PSG vs Lyon 15/04"
      "11/04 11:30 PL Arsenal Bournemouth"
      "/analyze Arsenal vs Bournemouth 11/04"
      "Arsenal contre Chelsea 20/04/2026 EPL"
    """
    text = text.strip()

    # Strip command prefix
    text = re.sub(r"^/(analys[ei]|analyze|match|tss)\s*", "", text, flags=re.IGNORECASE).strip()

    # ── League hint ───────────────────────────────────────────────────────────
    LEAGUE_ALIASES = {
        "pl": "EPL", "epl": "EPL", "premier league": "EPL", "premier": "EPL",
        "serie a": "Serie A", "seriea": "Serie A", "italy": "Serie A",
        "la liga": "La Liga", "laliga": "La Liga", "spain": "La Liga",
        "bundesliga": "Bundesliga", "germany": "Bundesliga", "buli": "Bundesliga",
        "ligue 1": "Ligue 1", "ligue1": "Ligue 1", "france": "Ligue 1",
        "eredivisie": "Eredivisie", "netherlands": "Eredivisie",
        "belgian pro": "Belgian Pro", "belgium": "Belgian Pro",
        "brazil": "Brazil Serie A", "brasileirao": "Brazil Serie A",
        "a-league": "A-League", "aleague": "A-League", "australia": "A-League",
        "afc": "AFC CL", "afc cl": "AFC CL",
        "eliteserien": "Eliteserien", "norway": "Eliteserien", "norvège": "Eliteserien",
        "allsvenskan": "Allsvenskan", "sweden": "Allsvenskan", "suède": "Allsvenskan",
        "superligaen": "Superligaen", "denmark": "Superligaen", "danemark": "Superligaen",
        "primeira liga": "Primeira Liga", "portugal": "Primeira Liga",
        "ucl": "EPL",
    }

    league_hint = None
    for alias, lg in sorted(LEAGUE_ALIASES.items(), key=lambda x: -len(x[0])):
        if re.search(rf"\b{re.escape(alias)}\b", text, re.IGNORECASE):
            league_hint = lg
            text = re.sub(rf"\b{re.escape(alias)}\b", "", text, flags=re.IGNORECASE).strip()
            break

    for lg in ["Serie A","EPL","La Liga","Bundesliga","Ligue 1",
               "Eredivisie","Belgian Pro","Brazil Serie A","A-League","AFC CL"]:
        if re.search(re.escape(lg), text, re.IGNORECASE):
            league_hint = lg
            text = re.sub(re.escape(lg), "", text, flags=re.IGNORECASE).strip()
            break

    # ── Time (HH:MM) — strip it, not used for prediction ─────────────────────
    text = re.sub(r"\b\d{1,2}:\d{2}\b", "", text).strip()

    # ── Date ──────────────────────────────────────────────────────────────────
    match_date = None
    for pattern, fmt in DATE_PATTERNS:
        m = pattern.search(text)
        if m:
            try:
                if fmt == "dmy":
                    d, mo, y = int(m.group(1)), int(m.group(2)), m.group(3)
                    y = int(y) if y else datetime.utcnow().year
                    y = 2000 + y if y < 100 else y
                    match_date = datetime(y, mo, d)
                else:
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    match_date = datetime(y, mo, d)
                text = text[:m.start()] + text[m.end():]
                break
            except ValueError:
                pass

    # French month names
    if not match_date:
        for fr, num in MONTHS_FR.items():
            pattern = re.compile(rf"(\d{{1,2}})\s+{fr}", re.IGNORECASE)
            m = pattern.search(text)
            if m:
                try:
                    match_date = datetime(datetime.utcnow().year, num, int(m.group(1)))
                    text = text[:m.start()] + text[m.end():]
                    break
                except ValueError:
                    pass

    text = re.sub(r"\s+", " ", text).strip()

    # ── Split teams ───────────────────────────────────────────────────────────
    # Try explicit separators first
    parts = SEPARATORS.split(text)
    if len(parts) >= 2:
        home_raw = parts[0].strip()
        away_raw = parts[1].strip()
    else:
        # No separator — try matching known team names from left and right
        home_raw, away_raw = _split_by_team_names(text)

    # Clean
    home_raw = re.sub(r"\s+", " ", home_raw).strip(" -–")
    away_raw = re.sub(r"\s+", " ", away_raw).strip(" -–")

    return home_raw, away_raw, match_date, league_hint


def _split_by_team_names(text: str) -> Tuple[str, str]:
    """
    When no separator (vs/v/-) found, try to split by identifying
    two consecutive known team names.
    Example: "Arsenal Bournemouth" → "Arsenal" / "Bournemouth"
    """
    words = text.split()
    all_teams = list(TEAM_LEAGUE_MAP.keys()) + list(TEAM_ALIASES.keys())

    # Try all split points
    best = (0.0, "", "")
    for i in range(1, len(words)):
        left  = " ".join(words[:i])
        right = " ".join(words[i:])
        sl    = max((SequenceMatcher(None, _normalise(left),  _normalise(t)).ratio()
                     for t in all_teams), default=0)
        sr    = max((SequenceMatcher(None, _normalise(right), _normalise(t)).ratio()
                     for t in all_teams), default=0)
        score = sl + sr
        if score > best[0]:
            best = (score, left, right)

    if best[1] and best[2]:
        return best[1], best[2]

    # Fallback: split in half
    mid = len(words) // 2
    return " ".join(words[:mid]), " ".join(words[mid:])


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DC MODEL LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def _load_fbref_data(league: str) -> pd.DataFrame:
    """Load historical match data from SQLite cache."""
    if not CACHE_DB.exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(str(CACHE_DB))
        df   = pd.read_sql(
            "SELECT * FROM matches WHERE league=? ORDER BY date DESC LIMIT 1000",
            conn, params=(league,)
        )
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        log.warning(f"Cache load failed [{league}]: {e}")
        return pd.DataFrame()


# In-memory model cache (avoids re-fitting on every request)
_MODEL_CACHE: Dict = {}
_PICKLE_DIR = Path("data/models")
_PICKLE_DIR.mkdir(parents=True, exist_ok=True)


def _get_dc_model(league: str):
    """Load pre-fitted DC model from pickle (instant), or fit if missing."""
    if league in _MODEL_CACHE:
        return _MODEL_CACHE[league]

    # 1. Try loading pre-fitted pickle (<1 second)
    pkl_name = league.replace(" ", "_").replace("/", "_") + ".pkl"
    pkl_path = _PICKLE_DIR / pkl_name
    if pkl_path.exists():
        try:
            import pickle
            with open(pkl_path, "rb") as f:
                model = pickle.load(f)
            _MODEL_CACHE[league] = model
            log.info(f"DC model loaded from pickle: {league}")
            return model
        except Exception as e:
            log.warning(f"Pickle load failed [{league}]: {e}")

    # 2. Fallback: fit from scratch (slow on free CPU)
    try:
        from tss.backtest_engine import DixonColesModel
    except ImportError:
        return None

    df = _load_fbref_data(league)
    if df.empty or len(df) < 100:
        return None

    model = DixonColesModel(xi=0.0065)
    try:
        model.fit(df, reference_date=pd.Timestamp(datetime.utcnow()))
        import pickle
        with open(pkl_path, "wb") as f:
            pickle.dump(model, f)
        _MODEL_CACHE[league] = model
        log.info(f"DC model fitted+saved: {league} ({len(df)} matches)")
        return model
    except Exception as e:
        log.error(f"DC fit failed [{league}]: {e}")
        return None


def _league_average_probs(home: str, away: str) -> Dict:
    """
    Fallback when no historical data: return league-average probabilities
    with a slight home advantage. Clearly flagged as LOW CONFIDENCE.
    """
    return {
        "xg_home": 1.35, "xg_away": 1.05,
        "H": 0.45, "D": 0.27, "A": 0.28,
        "btts_yes": 0.52, "btts_no": 0.48,
        "over2.5": 0.53, "under2.5": 0.47,
        "over3.5": 0.28, "under3.5": 0.72,
        "_fallback": True,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TSS GATE ENGINE (inline, no file dependency)
# ═══════════════════════════════════════════════════════════════════════════════

MARKET_LABELS = {
    "H": "Victoire Domicile (1)",
    "D": "Match Nul (X)",
    "A": "Victoire Extérieur (2)",
    "over2.5":  "Plus de 2.5 buts",
    "under2.5": "Moins de 2.5 buts",
    "over3.5":  "Plus de 3.5 buts",
    "under3.5": "Moins de 3.5 buts",
    "btts_yes": "BTTS Oui",
    "btts_no":  "BTTS Non",
}

MARKET_ODDS_KEY = {
    "H": "odds_H", "D": "odds_D", "A": "odds_A",
    "over2.5": "odds_over2.5", "under2.5": "odds_under2.5",
    "over3.5": "odds_over3.5", "under3.5": "odds_under3.5",
    "btts_yes": "odds_btts_yes", "btts_no": "odds_btts_no",
}


def _shin_demarg(odds_list: List[float]) -> List[float]:
    raw   = [1/o for o in odds_list]
    total = sum(raw)
    n     = len(raw)
    z     = (total - 1) / (total * (n-1)/n + total - 1 + 1e-9)
    z     = max(0.001, min(z, 0.5))
    return [(np.sqrt(z**2 + 4*(1-z)*p**2/total) - z) / (2*(1-z)) for p in raw]


def _simulate_odds(probs: Dict, margin: float = 0.055) -> Dict:
    """Add book margin to true probabilities → synthetic odds."""
    def inflate(ps):
        total = sum(ps) * (1 + margin)
        adj   = [p * total / sum(ps) for p in ps]
        return [max(1.01, 1/p) for p in adj]

    h, d, a   = probs["H"], probs["D"], probs["A"]
    ov, un    = probs["over2.5"],  probs["under2.5"]
    ov3, un3  = probs["over3.5"],  probs["under3.5"]
    by, bn    = probs["btts_yes"], probs["btts_no"]

    o1x2 = inflate([h, d, a])
    oou  = inflate([ov, un])
    oou3 = inflate([ov3, un3])
    obt  = inflate([by, bn])

    return {
        "odds_H": round(o1x2[0],3), "odds_D": round(o1x2[1],3),
        "odds_A": round(o1x2[2],3),
        "odds_over2.5": round(oou[0],3),  "odds_under2.5": round(oou[1],3),
        "odds_over3.5": round(oou3[0],3), "odds_under3.5": round(oou3[1],3),
        "odds_btts_yes": round(obt[0],3), "odds_btts_no": round(obt[1],3),
    }


def _run_gates(probs: Dict, odds: Dict, gates: Dict) -> List[Dict]:
    """Run all 4 gates, return list of signals that pass."""
    # Demarginalize book odds → P_book
    p1x2 = _shin_demarg([odds["odds_H"], odds["odds_D"], odds["odds_A"]])
    pou  = _shin_demarg([odds["odds_over2.5"], odds["odds_under2.5"]])
    pou3 = _shin_demarg([odds["odds_over3.5"], odds["odds_under3.5"]])
    pbt  = _shin_demarg([odds["odds_btts_yes"], odds["odds_btts_no"]])

    p_book = {
        "H": p1x2[0], "D": p1x2[1], "A": p1x2[2],
        "over2.5": pou[0], "under2.5": pou[1],
        "over3.5": pou3[0], "under3.5": pou3[1],
        "btts_yes": pbt[0], "btts_no": pbt[1],
    }

    signals = []
    for market in MARKET_LABELS:
        p_s   = probs.get(market, 0)
        p_b   = p_book.get(market, 0)
        odd   = odds.get(MARKET_ODDS_KEY[market], 0)
        ev    = round(p_s * odd - 1, 4)
        edge  = round(p_s - p_b, 4)

        # Gate checks
        fails = []
        if ev   < gates["ev_min"]:   fails.append(f"EV={ev:.3f}<{gates['ev_min']}")
        if edge < gates["edge_min"]: fails.append(f"Edge={edge:.3f}<{gates['edge_min']}")
        if not (gates["odds_min"] <= odd <= gates["odds_max"]):
            fails.append(f"Odds={odd} hors [{gates['odds_min']},{gates['odds_max']}]")

        # Kelly
        b = odd - 1
        kelly = max(0, (b*p_s - (1-p_s))/b * gates["kelly_fraction"]) if b > 0 else 0
        stake = min(kelly, gates["max_stake_pct"])

        # Signal strength (0-5 stars)
        stars = 0
        if not fails:
            if ev >= 0.10:   stars += 2
            elif ev >= 0.05: stars += 1
            if edge >= 0.10: stars += 2
            elif edge >= 0.07: stars += 1
            stars = min(stars, 5)

        signals.append({
            "market":   market,
            "label":    MARKET_LABELS[market],
            "p_synth":  round(p_s, 4),
            "p_book":   round(p_b, 4),
            "odds":     round(odd, 3),
            "ev":       ev,
            "edge":     edge,
            "kelly":    round(kelly, 4),
            "stake":    round(stake, 4),
            "bet":      len(fails) == 0,
            "fails":    fails,
            "stars":    stars,
        })

    return sorted(signals, key=lambda x: x["ev"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. TELEGRAM MESSAGE FORMATTER
# ═══════════════════════════════════════════════════════════════════════════════

def _stars(n: int) -> str:
    return "⭐" * n + "☆" * (5-n)


def format_analysis_message(
    home: str, away: str,
    league: str, match_date: Optional[datetime],
    probs: Dict, signals: List[Dict],
    fallback: bool = False,
) -> str:

    date_str = match_date.strftime("%d/%m/%Y") if match_date else "Date inconnue"
    ts       = datetime.utcnow().strftime("%H:%M UTC")

    bets  = [s for s in signals if s["bet"]]
    nobets= [s for s in signals if not s["bet"]]

    # Header
    conf_tag = "⚠️ <i>DONNÉES INSUFFISANTES — probabilités moyennes ligue</i>" if fallback else ""
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🔍 <b>APEX-TSS | ANALYSE MATCH</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"⚽ <b>{home}  vs  {away}</b>",
        f"🌍 {league}  ·  📅 {date_str}  ·  🕐 {ts}",
        conf_tag if fallback else "",
        "",
        f"📐 <b>PROBABILITÉS DIXON-COLES</b>",
        f"  1️⃣  Domicile : <b>{probs['H']*100:.1f}%</b>  "
        f"(xG {probs['xg_home']:.2f})",
        f"  ➖  Nul      : <b>{probs['D']*100:.1f}%</b>",
        f"  2️⃣  Extérieur: <b>{probs['A']*100:.1f}%</b>  "
        f"(xG {probs['xg_away']:.2f})",
        f"  🔵 BTTS Oui  : <b>{probs['btts_yes']*100:.1f}%</b>  "
        f"  ⚪ BTTS Non: <b>{probs['btts_no']*100:.1f}%</b>",
        f"  📈 Over 2.5  : <b>{probs['over2.5']*100:.1f}%</b>  "
        f"  📉 Under 2.5: <b>{probs['under2.5']*100:.1f}%</b>",
        "",
    ]

    # BET signals
    if bets:
        lines.append(f"✅ <b>SIGNAUX BET ({len(bets)})</b>")
        for s in bets:
            stake_pct = s['stake'] * 100
            lines.append(
                f"\n  {_stars(s['stars'])}  <b>{s['label']}</b>\n"
                f"  📊 Cote: <code>{s['odds']}</code>  "
                f"EV: <code>{s['ev']:+.3f}</code>  "
                f"Edge: <code>{s['edge']:+.3f}</code>\n"
                f"  🎯 P_synth: <code>{s['p_synth']*100:.1f}%</code>  "
                f"P_book: <code>{s['p_book']*100:.1f}%</code>\n"
                f"  💰 Mise Kelly: <code>{stake_pct:.2f}%</code> bankroll"
            )
    else:
        lines.append("🚫 <b>AUCUN SIGNAL BET</b>")
        lines.append("  Tous les marchés échouent aux gates TSS.")
        lines.append("")
        lines.append("  <i>Raisons principales:</i>")
        # Show why best market failed
        for s in signals[:3]:
            fail_str = ' | '.join(s['fails']).replace('<', '&lt;').replace('>', '&gt;')
            lines.append(f"  • {s['label']}: {fail_str}")
        lines.append("")  # blank line before footer

    # NO BET summary (top 3 closest to passing)
    if bets and nobets:
        lines.append("")
        lines.append("📋 <b>MARCHÉS REJETÉS (top 3)</b>")
        for s in nobets[:3]:
            lines.append(
                f"  ❌ {s['label']}: "
                f"EV={s['ev']:+.3f} | Edge={s['edge']:+.3f}"
            )

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>APEX-TSS · Dixon-Coles + Shin · NO BET par défaut</i>",
    ]

    return "\n".join(l for l in lines if l is not None)
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_match_text(text: str) -> str:
    """
    Full pipeline: free text → Telegram message.
    Returns formatted string ready to send via tg_send().
    """
    gates = _load_gates()

    # Step 1: Parse
    home_raw, away_raw, match_date, league_hint = parse_match_text(text)
    if not home_raw or not away_raw:
        return (
            "❌ <b>Format non reconnu.</b>\n\n"
            "Exemples valides:\n"
            "  • <code>PSG vs Lyon</code>\n"
            "  • <code>Naples - Lazio 15/04</code>\n"
            "  • <code>Arsenal contre Chelsea 20/04/2026</code>"
        )

    # Step 2: Resolve team names
    home_canon = _best_team_match(home_raw) or home_raw
    away_canon = _best_team_match(away_raw) or away_raw

    # Step 3: Detect league
    league = league_hint or _detect_league(home_raw, away_raw) or "Ligue inconnue"

    log.info(f"Analyzing: {home_canon} vs {away_canon} | {league}")

    # Step 4: Load DC model
    model    = _get_dc_model(league)
    fallback = False

    if model:
        try:
            probs = model.predict_probs(home_canon, away_canon)
        except Exception:
            # Team not in training data → fallback
            probs    = _league_average_probs(home_canon, away_canon)
            fallback = True
    else:
        probs    = _league_average_probs(home_canon, away_canon)
        fallback = True

    # Step 5: Simulate odds
    odds = _simulate_odds(probs, margin=gates["book_margin"])

    # Step 6: Run gates
    signals = _run_gates(probs, odds, gates)

    # Step 7: Format message
    return format_analysis_message(
        home=home_canon, away=away_canon,
        league=league, match_date=match_date,
        probs=probs, signals=signals,
        fallback=fallback,
    )
