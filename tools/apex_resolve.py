#!/usr/bin/env python3
"""APEX-LEAD — résolution de la requête d'entrée.

Le protocole accepte trois formes de requête, et une seule sortie :
la liste canonique des matchs à instruire.

    1. MATCH  — "Arsenal vs Manchester City"       → 1 pipeline
    2. LIGUE  — "Premier League" (+ fenêtre)       → N pipelines
    3. DATE   — "2026-09-27", "demain", "weekend"  → N pipelines

Source : The Odds API. Les endpoints /v4/sports et /v4/sports/<k>/events
sont gratuits (ils ne décomptent pas le quota de 500 req/mois), ce qui
permet de résoudre une ligue ou une date entière sans consommer le budget
réservé aux cotes réelles de A1/A6.

Règle anti-hallucination n°1 : aucune affiche n'est inventée. Un match qui
ne ressort pas du catalogue est renvoyé dans `unresolved`, jamais deviné.
"""

from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "runs", ".cache")
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
CATALOGUE_TTL = 12 * 3600
EVENTS_TTL = 30 * 60

# Mapping factuel sport_key → moteur APEX. Établi par correspondance entre
# les clés actives de The Odds API et la table de routage
# .claude/agents/apex-league-router.md. Une clé absente d'ici part en
# fallback (apex-pronostic-football, DCS −5, Kelly ×0.70).
ENGINE_BY_SPORT_KEY = {
    "soccer_epl":                          "apex-engine-epl",
    "soccer_spain_la_liga":                "apex-engine-la-liga",
    "soccer_spain_segunda_division":       "apex-engine-laliga2",
    "soccer_germany_bundesliga":           "apex-engine-bundesliga",
    "soccer_italy_serie_a":                "apex-engine-serie-a",
    "soccer_france_ligue_one":             "apex-engine-ligue1",
    "soccer_netherlands_eredivisie":       "apex-engine-eredivisie",
    "soccer_spl":                          "apex-engine-scottish-prem",
    "soccer_brazil_campeonato":            "apex-engine-brasileirao",
    "soccer_conmebol_copa_libertadores":   "apex-engine-libertadores",
    "soccer_norway_eliteserien":           "apex-engine-eliteserien",
    "soccer_denmark_superliga":            "apex-engine-superligaen",
    "soccer_uefa_champs_league":           "apex-engine-uefa",
    "soccer_uefa_europa_league":           "apex-engine-uefa",
    "soccer_uefa_europa_conference_league": "apex-engine-uefa",
    "soccer_fifa_world_cup":               "apex-engine-worldcup-2026",
    # Moteurs dédiés dont la ligue n'est pas (ou pas encore) exposée par
    # The Odds API : Ligue 2, Jupiler Pro League, Liga Portugal, Süper Lig,
    # RPL, Saudi Pro League, Allsvenskan, Swiss SL, Arménie, IPL, Oman,
    # Superliga roumaine. Ces matchs doivent être fournis en forme MATCH
    # explicite, avec leur compétition.
}

# Moteurs sans clé Odds API : routables uniquement sur requête explicite.
ENGINE_WITHOUT_FEED = {
    "apex-engine-ligue2": "Ligue 2 BKT",
    "apex-engine-jpl": "Jupiler Pro League",
    "apex-engine-liga-portugal": "Liga Portugal",
    "apex-engine-super-lig": "Süper Lig",
    "apex-engine-rpl": "Premier League russe",
    "apex-engine-saudi-pro-league": "Saudi Pro League",
    "apex-engine-allsvenskan": "Allsvenskan",
    "apex-engine-ssl": "Swiss Super League",
    "apex-engine-armenia-premier-league": "Armenia Premier League",
    "apex-engine-ipl": "Ligat Ha'Al",
    "apex-engine-oman-pro-league": "Oman Pro League",
    "apex-engine-romania-superliga": "Superliga (Roumanie)",
}

# Alias francophones / usuels → sport_key.
LEAGUE_ALIASES = {
    "premier league": "soccer_epl", "epl": "soccer_epl",
    "angleterre": "soccer_epl", "prem": "soccer_epl",
    "la liga": "soccer_spain_la_liga", "liga": "soccer_spain_la_liga",
    "espagne": "soccer_spain_la_liga",
    "liga 2": "soccer_spain_segunda_division",
    "segunda": "soccer_spain_segunda_division",
    "hypermotion": "soccer_spain_segunda_division",
    "bundesliga": "soccer_germany_bundesliga", "allemagne": "soccer_germany_bundesliga",
    "serie a": "soccer_italy_serie_a", "italie": "soccer_italy_serie_a",
    "ligue 1": "soccer_france_ligue_one", "france": "soccer_france_ligue_one",
    "eredivisie": "soccer_netherlands_eredivisie", "pays-bas": "soccer_netherlands_eredivisie",
    "scottish premiership": "soccer_spl", "ecosse": "soccer_spl",
    "brasileirao": "soccer_brazil_campeonato", "bresil": "soccer_brazil_campeonato",
    "libertadores": "soccer_conmebol_copa_libertadores",
    "eliteserien": "soccer_norway_eliteserien", "norvege": "soccer_norway_eliteserien",
    "superligaen": "soccer_denmark_superliga", "danemark": "soccer_denmark_superliga",
    "ligue des champions": "soccer_uefa_champs_league",
    "champions league": "soccer_uefa_champs_league", "ucl": "soccer_uefa_champs_league",
    "europa league": "soccer_uefa_europa_league", "uel": "soccer_uefa_europa_league",
    "conference league": "soccer_uefa_europa_conference_league",
    "uecl": "soccer_uefa_europa_conference_league",
    "championship": "soccer_efl_champ",
    "liga mx": "soccer_mexico_ligamx", "mls": "soccer_usa_mls",
}

FALLBACK_ENGINE = "apex-pronostic-football"
FALLBACK_ADJUSTMENTS = {"dcs_delta": -5, "kelly_multiplier": 0.70}


# --------------------------------------------------------------------------
# Accès réseau (avec cache disque)
# --------------------------------------------------------------------------

def _api_key() -> str:
    return os.environ.get("ODDS_API_KEY", "4556cbebcaea0e8301f1c176bdb64e31")


def _cache_path(name: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{name}.json")


def _cached(name: str, ttl: int):
    path = _cache_path(name)
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < ttl:
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            return None
    return None


def _store(name: str, data) -> None:
    with open(_cache_path(name), "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)


def catalogue(refresh: bool = False) -> list[dict]:
    """Compétitions football actives. Endpoint gratuit (hors quota)."""
    if not refresh:
        hit = _cached("catalogue", CATALOGUE_TTL)
        if hit is not None:
            return hit
    resp = requests.get(f"{ODDS_API_BASE}/sports",
                        params={"apiKey": _api_key()}, timeout=25)
    resp.raise_for_status()
    soccer = [s for s in resp.json()
              if s.get("group") == "Soccer" and s.get("active")]
    _store("catalogue", soccer)
    return soccer


def events(sport_key: str, refresh: bool = False) -> list[dict]:
    """Affiches à venir d'une compétition. Endpoint gratuit (hors quota)."""
    name = f"events_{sport_key}"
    if not refresh:
        hit = _cached(name, EVENTS_TTL)
        if hit is not None:
            return hit
    try:
        resp = requests.get(f"{ODDS_API_BASE}/sports/{sport_key}/events",
                            params={"apiKey": _api_key(), "dateFormat": "iso"},
                            timeout=25)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except requests.RequestException:
        return []
    _store(name, data)
    return data


def _source_url(sport_key: str) -> str:
    return f"{ODDS_API_BASE}/sports/{sport_key}/events"


# --------------------------------------------------------------------------
# Normalisation & détection de forme
# --------------------------------------------------------------------------

def norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9 ]+", " ", text.lower()).strip()


VS_SPLIT = re.compile(r"\s+(?:vs\.?|v|contre|[-–—:])\s+", re.IGNORECASE)
ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
EU_DATE = re.compile(r"^(\d{1,2})[/.](\d{1,2})(?:[/.](\d{2,4}))?$")
WINDOW = re.compile(r"^(\d+)\s*(h|d|j)$", re.IGNORECASE)

RELATIVE_DAYS = {
    "today": 0, "aujourd'hui": 0, "aujourdhui": 0, "auj": 0, "ce soir": 0,
    "tomorrow": 1, "demain": 1,
    "apres-demain": 2, "après-demain": 2,
}


def detect_form(text: str) -> tuple[str, str]:
    """Renvoie ('match'|'league'|'date', valeur normalisée)."""
    raw = (text or "").strip()
    low = raw.lower()

    if low in RELATIVE_DAYS or low in ("weekend", "week-end", "semaine", "week"):
        return "date", low
    if ISO_DATE.match(raw) or EU_DATE.match(raw) or WINDOW.match(raw):
        return "date", raw
    parts = VS_SPLIT.split(raw)
    if len(parts) == 2 and all(p.strip() for p in parts):
        return "match", raw
    return "league", raw


def date_window(spec: str, today: datetime | None = None) -> tuple[str, str, str]:
    """Traduit une expression de date en (date_from, date_to, libellé)."""
    today = (today or datetime.now(timezone.utc)).date()
    low = (spec or "").strip().lower()

    if low in RELATIVE_DAYS:
        day = today + timedelta(days=RELATIVE_DAYS[low])
        return str(day), str(day), str(day)
    if low in ("weekend", "week-end"):
        # samedi et dimanche de la semaine courante (ou à venir).
        saturday = today + timedelta(days=(5 - today.weekday()) % 7)
        return str(saturday), str(saturday + timedelta(days=1)), "week-end"
    if low in ("semaine", "week", "7d", "7j"):
        return str(today), str(today + timedelta(days=7)), "7 jours"

    win = WINDOW.match(low)
    if win:
        n, unit = int(win.group(1)), win.group(2).lower()
        days = max(1, (n + 23) // 24) if unit == "h" else n
        return str(today), str(today + timedelta(days=days)), f"{n}{unit}"

    iso = ISO_DATE.match(spec.strip())
    if iso:
        return spec.strip(), spec.strip(), spec.strip()

    eur = EU_DATE.match(spec.strip())
    if eur:
        day, month = int(eur.group(1)), int(eur.group(2))
        year = int(eur.group(3) or today.year)
        year = year + 2000 if year < 100 else year
        iso_day = f"{year:04d}-{month:02d}-{day:02d}"
        return iso_day, iso_day, iso_day

    return str(today), str(today), str(today)


MIN_LEAGUE_SIMILARITY = 0.80   # en dessous : ambigu, donc non résolu
MIN_LEAGUE_MARGIN = 0.05       # écart minimal avec le second candidat


def _tokens(text: str) -> list[str]:
    return [t for t in norm(text).split() if t]


def _contains_sequence(haystack: list[str], needle: list[str]) -> bool:
    """Les tokens de `needle` apparaissent-ils contigus dans `haystack` ?"""
    if not needle or len(needle) > len(haystack):
        return False
    return any(haystack[i:i + len(needle)] == needle
               for i in range(len(haystack) - len(needle) + 1))


def no_feed_engine(text: str) -> tuple[str, str] | None:
    """Ligue disposant d'un moteur APEX mais absente du flux d'affiches."""
    q = _tokens(text)
    for engine, label in ENGINE_WITHOUT_FEED.items():
        lab = _tokens(label)
        if _contains_sequence(q, lab) or _contains_sequence(lab, q):
            return engine, label
    return None


def match_league(text: str, cat: list[dict]) -> tuple[str | None, float]:
    """Résout un nom de ligue vers un sport_key.

    Trois verrous, dans cet ordre, contre le mis-routage silencieux observé
    en test (« Liga Portugal » → La Liga, « Süper Lig » → Superliga danoise,
    « Saudi Pro League » → Europa League) :

    1. les ligues à moteur dédié mais sans flux d'affiches sont interceptées
       avant toute comparaison floue ;
    2. un alias ne s'applique que sur une séquence de tokens complète, pas
       sur une sous-chaîne (« liga » ne capture plus « Liga Portugal ») ;
    3. le meilleur candidat doit dépasser 0,80 de similarité ET devancer le
       second d'au moins 0,05 — sinon la requête est ambiguë, et une
       ambiguïté de routage arrête la chaîne au lieu de la mal calibrer.
    """
    key = norm(text)
    if key in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[key], 1.0

    if no_feed_engine(text):
        return None, 0.0

    q = _tokens(text)
    for alias, sport_key in sorted(LEAGUE_ALIASES.items(),
                                   key=lambda kv: -len(kv[0])):
        if _contains_sequence(q, _tokens(alias)):
            return sport_key, 0.95

    scored: list[tuple[float, str]] = []
    for sport in cat:
        best_for_sport = max(
            SequenceMatcher(None, key, norm(candidate)).ratio()
            for candidate in (sport.get("title", ""),
                              sport.get("key", "").replace("_", " ")))
        scored.append((best_for_sport, sport["key"]))
    scored.sort(reverse=True)

    if not scored:
        return None, 0.0
    best_score, best_key = scored[0]
    runner_up = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= MIN_LEAGUE_SIMILARITY and (best_score - runner_up) >= MIN_LEAGUE_MARGIN:
        return best_key, best_score
    return None, best_score


# --------------------------------------------------------------------------
# Construction des matchs
# --------------------------------------------------------------------------

def engine_for(sport_key: str) -> tuple[str, bool]:
    engine = ENGINE_BY_SPORT_KEY.get(sport_key)
    return (engine, False) if engine else (FALLBACK_ENGINE, True)


def to_match(event: dict, sport_key: str, title: str, origin: str = "feed") -> dict:
    engine, fallback = engine_for(sport_key)
    return {
        "origin": origin,
        "home": event["home_team"],
        "away": event["away_team"],
        "competition": event.get("sport_title") or title,
        "kickoff_utc": event["commence_time"],
        "sport_key": sport_key,
        "event_id": event.get("id"),
        # Indication de routage, PAS la décision : A0 (apex-league-router)
        # reste seul juge et écrit 00_routing.json.
        "routing_hint": {
            "engine_skill": engine,
            "fallback_mode": fallback,
            "fallback_adjustments": FALLBACK_ADJUSTMENTS if fallback else {},
        },
        "source_url": _source_url(sport_key),
        "retrieved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def in_window(iso_ts: str, date_from: str, date_to: str) -> bool:
    return date_from <= (iso_ts or "")[:10] <= date_to


MATCHDAY_SPAN_DAYS = 3  # vendredi → lundi : une journée de championnat


def next_matchday(evs: list[dict]) -> tuple[str, str] | None:
    """Fenêtre de la prochaine journée : du 1er match à +3 jours."""
    dates = sorted(e.get("commence_time", "")[:10] for e in evs if e.get("commence_time"))
    if not dates:
        return None
    first = datetime.strptime(dates[0], "%Y-%m-%d").date()
    return str(first), str(first + timedelta(days=MATCHDAY_SPAN_DAYS))


def resolve_league(name: str, window: str = "next", cat=None) -> dict:
    cat = cat if cat is not None else catalogue()
    sport_key, score = match_league(name, cat)
    notes, unresolved = [], []
    if not sport_key:
        no_feed = no_feed_engine(name)
        if no_feed:
            engine, label = no_feed
            reason = (f"« {label} » dispose du moteur {engine}, mais cette ligue "
                      f"n'est pas exposée par le flux d'affiches : fournir les "
                      f"matchs en forme MATCH explicite (home, away, competition, "
                      f"kickoff_utc).")
        else:
            reason = (f"ligue non reconnue ou ambiguë (meilleure similarité "
                      f"{score:.2f} < {MIN_LEAGUE_SIMILARITY}) — préciser le nom exact.")
        unresolved.append({"query": name, "reason": reason})
        return {"form": "league", "matches": [], "unresolved": unresolved, "notes": notes}

    title = next((s.get("title", sport_key) for s in cat if s["key"] == sport_key), sport_key)
    evs = events(sport_key)

    if (window or "next").lower() in ("next", "prochaine", "journee", "journée", "matchday"):
        span = next_matchday(evs)
        if span is None:
            unresolved.append({"query": name, "reason": f"aucune affiche au flux pour {title}"})
            return {"form": "league", "matches": [], "unresolved": unresolved, "notes": notes}
        date_from, date_to = span
        label = f"prochaine journée ({date_from} → {date_to})"
    else:
        date_from, date_to, label = date_window(window)

    found = [to_match(e, sport_key, title, "league") for e in evs
             if in_window(e.get("commence_time", ""), date_from, date_to)]
    found.sort(key=lambda m: m["kickoff_utc"])
    notes.append(f"Ligue « {title} » ({sport_key}) — fenêtre {label} : "
                 f"{len(found)} affiche(s).")
    if not found:
        upcoming = next_matchday(evs)
        extra = (f" Prochaines affiches disponibles à partir du {upcoming[0]}."
                 if upcoming else " Aucune affiche au flux pour cette compétition.")
        unresolved.append({"query": name,
                           "reason": f"aucune affiche pour {title} dans la fenêtre {label}.{extra}"})
    return {"form": "league", "matches": found, "unresolved": unresolved, "notes": notes}


def resolve_date(spec: str, leagues: list[str] | None = None, cat=None) -> dict:
    cat = cat if cat is not None else catalogue()
    date_from, date_to, label = date_window(spec)

    if leagues:
        keys, unresolved = [], []
        for name in leagues:
            sport_key, score = match_league(name, cat)
            if sport_key:
                keys.append(sport_key)
            else:
                unresolved.append({"query": name, "reason": f"ligue non reconnue (score {score:.2f})"})
    else:
        keys, unresolved = [s["key"] for s in cat], []

    titles = {s["key"]: s.get("title", s["key"]) for s in cat}
    found = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sport_key, evs in zip(keys, pool.map(events, keys)):
            found += [to_match(e, sport_key, titles.get(sport_key, sport_key), "date")
                      for e in evs
                      if in_window(e.get("commence_time", ""), date_from, date_to)]

    found.sort(key=lambda m: (m["kickoff_utc"], m["competition"]))
    scope = f"{len(keys)} compétition(s)" if leagues else "toutes compétitions actives"
    notes = [f"Date « {label} » ({date_from} → {date_to}) sur {scope} : "
             f"{len(found)} affiche(s)."]
    if not found:
        horizon = sorted(e.get("commence_time", "")[:10]
                         for key in keys for e in events(key) if e.get("commence_time"))
        extra = f" Prochaines affiches à partir du {horizon[0]}." if horizon else ""
        unresolved.append({"query": spec,
                           "reason": f"aucune affiche entre {date_from} et {date_to}.{extra}"})
    return {"form": "date", "matches": found, "unresolved": unresolved, "notes": notes}


def resolve_match(text: str, competition: str | None = None,
                  kickoff_utc: str | None = None, cat=None) -> dict:
    """Confirme une affiche précise contre le catalogue réel."""
    cat = cat if cat is not None else catalogue()
    parts = VS_SPLIT.split(text.strip())
    if len(parts) != 2:
        return {"form": "match", "matches": [], "notes": [],
                "unresolved": [{"query": text, "reason": "format attendu « Équipe A vs Équipe B »"}]}
    home_q, away_q = norm(parts[0]), norm(parts[1])

    keys = [s["key"] for s in cat]
    if competition:
        sport_key, _ = match_league(competition, cat)
        if sport_key:
            keys = [sport_key]
    titles = {s["key"]: s.get("title", s["key"]) for s in cat}

    best, best_score, best_key = None, 0.0, None
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sport_key, evs in zip(keys, pool.map(events, keys)):
            for ev in evs:
                home, away = norm(ev.get("home_team", "")), norm(ev.get("away_team", ""))
                direct = (SequenceMatcher(None, home_q, home).ratio()
                          + SequenceMatcher(None, away_q, away).ratio()) / 2
                # Affiche inversée : on la retient mais on le signale.
                swapped = (SequenceMatcher(None, home_q, away).ratio()
                           + SequenceMatcher(None, away_q, home).ratio()) / 2
                score = max(direct, swapped)
                if score > best_score:
                    best, best_score, best_key = ev, score, sport_key

    if best_score < 0.70 or best is None:
        return {"form": "match", "matches": [], "notes": [],
                "unresolved": [{"query": text,
                                "reason": f"affiche introuvable au catalogue "
                                          f"(meilleure similarité {best_score:.2f}) — "
                                          f"fournir competition + kickoff_utc explicitement"}]}

    resolved = to_match(best, best_key, titles.get(best_key, best_key), "match")
    notes = [f"Affiche confirmée : {resolved['home']} vs {resolved['away']} "
             f"({resolved['competition']}, {resolved['kickoff_utc']}) — "
             f"similarité {best_score:.2f}."]
    if kickoff_utc and kickoff_utc[:10] != resolved["kickoff_utc"][:10]:
        notes.append(f"ATTENTION : kickoff fourni {kickoff_utc} ≠ catalogue "
                     f"{resolved['kickoff_utc']} — le catalogue fait foi.")
    return {"form": "match", "matches": [resolved], "unresolved": [], "notes": notes}


def resolve(spec: dict) -> dict:
    """Point d'entrée unique. `spec` accepte les trois formes + l'héritée."""
    cat = catalogue()
    out = {"matches": [], "unresolved": [], "notes": []}

    def merge(res):
        out["matches"] += res["matches"]
        out["unresolved"] += res["unresolved"]
        out["notes"] += res["notes"]

    # Forme libre : "Arsenal vs Chelsea", "Premier League", "2026-09-27".
    if spec.get("query"):
        form, value = detect_form(spec["query"])
        out["notes"].append(f"Requête « {spec['query']} » détectée comme forme {form.upper()}.")
        if form == "match":
            merge(resolve_match(value, spec.get("competition"), spec.get("kickoff_utc"), cat))
        elif form == "league":
            merge(resolve_league(value, spec.get("window", "next"), cat))
        else:
            merge(resolve_date(value, spec.get("leagues"), cat))

    if spec.get("league"):
        merge(resolve_league(spec["league"], spec.get("window", "next"), cat))

    if spec.get("date"):
        merge(resolve_date(spec["date"], spec.get("leagues"), cat))

    # Forme MATCH structurée, et compat ascendante avec `matches:`.
    explicit = list(spec.get("matches") or [])
    if spec.get("match"):
        explicit.append(spec["match"])
    for item in explicit:
        if item.get("kickoff_utc") and item.get("competition"):
            # Match entièrement spécifié : on le prend tel quel (seule voie
            # pour les ligues sans flux d'affiches).
            sport_key, _ = match_league(item["competition"], cat)
            no_feed = no_feed_engine(item["competition"])
            if no_feed:
                # Ligue à moteur dédié mais absente du flux : c'est précisément
                # la voie prescrite pour elle — surtout pas un fallback.
                engine, fallback = no_feed[0], False
            else:
                engine, fallback = engine_for(sport_key or "")
            out["matches"].append({
                "origin": "explicit",
                **item,
                "sport_key": sport_key,
                "routing_hint": {"engine_skill": engine, "fallback_mode": fallback,
                                 "fallback_adjustments": FALLBACK_ADJUSTMENTS if fallback else {}},
                "source_url": item.get("source_url", "fourni par l'utilisateur"),
                "retrieved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
        else:
            merge(resolve_match(f"{item.get('home','')} vs {item.get('away','')}",
                                item.get("competition"), item.get("kickoff_utc"), cat))

    # Dédoublonnage (une ligue et une date peuvent renvoyer la même affiche).
    # Les affiches demandées nommément passent en tête : un --limit ne doit
    # jamais écarter un match explicite au profit d'un match auto-résolu.
    priority = {"explicit": 0, "match": 1, "league": 2, "date": 3}
    out["matches"].sort(key=lambda m: (priority.get(m.get("origin"), 9),
                                       m.get("kickoff_utc", "")))
    seen, unique = set(), []
    for m in out["matches"]:
        sig = (norm(m["home"]), norm(m["away"]), m["kickoff_utc"][:10])
        if sig not in seen:
            seen.add(sig)
            unique.append(m)
    if len(unique) < len(out["matches"]):
        out["notes"].append(f"{len(out['matches']) - len(unique)} doublon(s) écarté(s).")
    out["matches"] = unique
    return out


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Résolution de requête APEX")
    parser.add_argument("query", help="ligue, date ou match")
    parser.add_argument("--window", default="next")
    args = parser.parse_args()
    print(json.dumps(resolve({"query": args.query, "window": args.window}),
                     ensure_ascii=False, indent=2))
