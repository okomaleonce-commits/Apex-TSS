#!/usr/bin/env python3
"""APEX-MI — moteur mécanique du swarm « Market & Behavioral Intelligence ».

Cette cellule est SÉPARÉE de la cellule statistique (BSM / chaîne S1-S8 / moteurs
apex-engine-*). Elle ne recalcule ni xG, ni tirs, ni forme : elle capte le BRUIT
informationnel et comportemental du marché AVANT le coup d'envoi, puis le score de
façon reproductible pour que les agents interprètent au lieu de calculer de tête.

Principe directeur (comme APEX-WORM) : « le moteur calcule, l'agent interprète ».
Chaque agent enregistre une OBSERVATION (famille de signal, source, vague temporelle,
direction, ampleur) ; c'est CE moteur qui attribue les quatre dimensions
(SOURCE_RELIABILITY, TIMING_RELEVANCE, CROSS_SOURCE_CONFIRMATION, MARKET_IMPACT),
le MARKET_SIGNAL_SCORE et le MARKET_NOISE_SCORE. La valeur vient de la CONVERGENCE
entre agents, jamais d'un signal isolé.

Règle d'intégration non négociable :
    BEHAVIORAL seul            -> WATCH
    BEHAVIORAL + MARKET        -> CANDIDATE
    BEHAVIORAL + MARKET + DATA  -> CONFIRMED (hors de cette cellule : c'est le moteur
                                  statistique APEX qui apporte la brique DATA)
Cette cellule ne produit donc JAMAIS un pari à elle seule. Elle émet de la
« market intelligence » qui alimente le moteur statistique.

Sous-commandes :
  window      Affiche la fenêtre APEX (08:00 → 07:59) et le fuseau.
  init        Résout les matchs (inline ou depuis les snapshots API-Football) et crée
              un dossier par match dans runs_mi/<run>/<match_id>/.
  oddsflow    Calcule mécaniquement la trajectoire de ligne, le steam, la dispersion,
              Pinnacle-vs-médiane et le RLM (honnête : non calculable sans % public).
  signal      Enregistre une observation de marché scorée (append-only signals.jsonl).
  behavioral  Enregistre un indice comportemental scoré (append-only behavioral.jsonl).
  check       État des agents d'un match + prochaine étape (comme un lead).
  score       Agrège signaux + indices -> synthèse marché + bloc comportemental.
  finalize    SYNTHESE.md, telegram.txt et ligne de journal append-only.

Aucune donnée inventée. Si une donnée manque, elle est écrite comme manquante.
"""
from __future__ import annotations

import argparse
import html as _html
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from apex_common import (as_float, load_input, match_id_of, now_utc,  # noqa: E402
                         read_json, slug, write_json)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNS = os.path.join(ROOT, "runs_mi")
SNAPSHOTS = os.path.join(ROOT, "data", "apifootball", "snapshots")
JOURNAL = os.path.join(ROOT, "journal", "apex_mi_journal.csv")
# Pont avec APEX-WORM : le scanner écrit ses relevés horodatés ici ; l'activation H-60
# d'APEX-MI les lit, produit un rapport orienté UPSET et le dépose pour l'email WORM.
WORM_SNAP = os.path.join(ROOT, "data", "worm", "snapshots")
WORM_MI = os.path.join(ROOT, "data", "worm", "mi")
WORM_REP = os.path.join(ROOT, "reports", "worm")

# ───────────────────────── Hiérarchie des sources ─────────────────────────
# Fiabilité 0..100. Un déplacement Pinnacle et un post Telegram n'ont pas le même poids.
SOURCE_TIERS = {
    "sharp": 100,          # book sharp (Pinnacle), marché d'échange réputé informatif
    "exchange": 100,       # Betfair / exchange — volume réel
    "official": 95,        # source officielle club / compétition / ligue
    "local_reliable": 75,  # journaliste local fiable, suiveur de club identifié
    "specialist": 60,      # presse spécialisée
    "aggregator": 45,      # agrégateurs de cotes / de news
    "social": 30,          # réseaux sociaux (comptes non officiels)
    "forum": 20,           # forums
    "tipster": 12,         # tipsters
}

# ───────────────────────── Familles de signaux ─────────────────────────
# classe → gouverne la pertinence temporelle (une compo tardive = or ; un
# emballement sentiment tardif = bruit).
FAMILY_CLASS = {
    "SHARP_MOVE": "MARKET", "STEAM_MOVE": "MARKET", "RLM": "MARKET",
    "MARKET_RESISTANCE": "MARKET", "BOOKMAKER_DIVERGENCE": "MARKET",
    "LIQUIDITY_SPIKE": "MARKET", "LATE_MONEY": "MARKET", "EARLY_MONEY": "MARKET",
    "PRICE_COMPRESSION": "MARKET", "PRICE_DRIFT": "MARKET", "MARKET_FLIP": "MARKET",
    "LINEUP_SHOCK": "LINEUP",
    "NEWS_SHOCK": "NEWS",
    "SENTIMENT_OVERLOAD": "SENTIMENT",
}
VALID_FAMILIES = set(FAMILY_CLASS)

# Vagues temporelles (règle du protocole : distinguer structurel, information, tardif).
WAVES = {
    "EARLY": "T-24h → T-6h (marché structurel)",
    "INFORMATION": "T-6h → T-1h (marché d'information)",
    "LATE": "T-60min → KO (marché tardif / compositions)",
}

# TIMING_RELEVANCE[classe][vague]
TIMING_MATRIX = {
    "MARKET":    {"EARLY": 65, "INFORMATION": 85, "LATE": 95},
    "NEWS":      {"EARLY": 45, "INFORMATION": 80, "LATE": 100},
    "LINEUP":    {"EARLY": 20, "INFORMATION": 70, "LATE": 100},
    "SENTIMENT": {"EARLY": 55, "INFORMATION": 48, "LATE": 35},
}

# Indices comportementaux (0..100) — des INDICES, pas des diagnostics.
BEHAVIORAL_INDICES = [
    "PRESSURE_INDEX", "MOTIVATION_INDEX", "COHESION_INDEX", "INSTABILITY_INDEX",
    "CONFIDENCE_PROXY", "FATIGUE_CONTEXT", "PUBLIC_PRESSURE", "COACH_PRESSURE",
    "SUPPORTER_PRESSURE", "MEDIA_PRESSURE", "NARRATIVE_STRENGTH",
]

KIND_CAP = {"fact": 100, "observation": 90, "interpretation": 55}
CONFIDENCE_W = {"high": 1.0, "medium": 0.75, "low": 0.5}


def band(score):
    """Bandes imposées par le protocole."""
    if score is None:
        return "INDISPONIBLE"
    if score < 40:
        return "bruit faible / non exploitable"
    if score < 60:
        return "information à surveiller"
    if score < 75:
        return "signal crédible"
    if score < 90:
        return "signal fort"
    return "anomalie majeure"


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


# ───────────────────────── Fenêtre APEX ─────────────────────────
def apex_window(ref=None):
    ref = ref or datetime.now(timezone.utc)
    start = ref.replace(hour=8, minute=0, second=0, microsecond=0)
    if ref.hour < 8:
        start -= timedelta(days=1)
    return start, start + timedelta(days=1) - timedelta(seconds=1)


def cmd_window(a):
    s, e = apex_window()
    print(f"Fenêtre APEX (UTC) : {s.isoformat()} → {e.isoformat()}")
    print("Vagues : " + " | ".join(f"{k} = {v}" for k, v in WAVES.items()))
    return 0


# ───────────────────────── Résolution des matchs ─────────────────────────
def _fixtures_from_snapshot(date, leagues):
    path = os.path.join(SNAPSHOTS, f"{date}.jsonl")
    if not os.path.exists(path):
        return []
    want = {int(x) for x in leagues.split(",")} if leagues else None
    # Dédup par fixture_id : le snapshot contient plusieurs relevés par match sur la journée ;
    # on ne garde que le dernier (par retrieved_at_utc) pour n'avoir qu'un dossier par match.
    latest = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if want and r.get("league_id") not in want:
            continue
        fid = r.get("fixture_id")
        prev = latest.get(fid)
        if prev is None or (r.get("retrieved_at_utc") or "") >= (prev.get("retrieved_at_utc") or ""):
            latest[fid] = r
    return [{"home": r["home"], "away": r["away"], "kickoff_utc": r["kickoff_utc"],
             "competition": r.get("league"), "fixture_id": r.get("fixture_id")}
            for r in latest.values()]


def cmd_init(a):
    spec = {}
    if a.input:
        spec = load_input(a.input) if os.path.exists(a.input) else json.loads(a.input)
    fixtures = spec.get("fixtures") or []
    date = a.date or spec.get("date")
    if not fixtures and date:
        fixtures = _fixtures_from_snapshot(date, a.leagues or spec.get("leagues"))
    if not fixtures:
        print("INIT EMPTY : aucun match résolu (fournir fixtures inline ou une --date avec snapshot existant).")
        print("Astuce : python3 tools/apex_apifootball.py snapshot --date <J> crée le snapshot.")
        return 2

    run_id = a.run_id or spec.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(RUNS, run_id)
    created = []
    for fx in fixtures:
        home, away = fx["home"], fx["away"]
        ko = fx.get("kickoff_utc") or ""
        mid = match_id_of(home, away, ko)
        mdir = os.path.join(run_dir, mid)
        meta = {"match_id": mid, "home": home, "away": away, "kickoff_utc": ko,
                "competition": fx.get("competition"), "fixture_id": fx.get("fixture_id"),
                "created_at_utc": now_utc()}
        write_json(os.path.join(mdir, "meta.json"), meta)
        # amorce les journaux append-only
        for fn in ("signals.jsonl", "behavioral.jsonl"):
            p = os.path.join(mdir, fn)
            if not os.path.exists(p):
                open(p, "a", encoding="utf-8").close()
        created.append((mid, home, away, ko, fx.get("fixture_id")))

    s, e = apex_window()
    write_json(os.path.join(run_dir, "request.json"),
               {"run_id": run_id, "window_utc": [s.isoformat(), e.isoformat()],
                "n_matches": len(created), "created_at_utc": now_utc()})
    print(f"Run {run_id} — {len(created)} match(s) · {run_dir}")
    for mid, h, aw, ko, fid in created:
        print(f"  {mid}  {h} – {aw}  KO {ko or '?'}  fixture_id={fid or '—'}")
    print("\nProchaine étape par match : oddsflow (si fixture_id), puis les agents collecteurs, puis score.")
    return 0


# ───────────────────────── Odds-flow mécanique ─────────────────────────
def _demargin_1x2(triple):
    o = [as_float(x) for x in (triple or [])]
    if len(o) != 3 or any(x is None or x <= 1.0 for x in o):
        return None
    inv = [1.0 / x for x in o]
    s = sum(inv)
    return [round(x / s, 4) for x in inv]


def _snapshot_series(fixture_id):
    """Tous les relevés d'un fixture, triés par retrieved_at_utc."""
    recs = []
    if not os.path.isdir(SNAPSHOTS):
        return recs
    for fn in sorted(os.listdir(SNAPSHOTS)):
        if not fn.endswith(".jsonl"):
            continue
        for line in open(os.path.join(SNAPSHOTS, fn), encoding="utf-8"):
            if f'"fixture_id": {fixture_id},' in line:
                try:
                    recs.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    recs.sort(key=lambda r: r.get("retrieved_at_utc", ""))
    return recs


def cmd_oddsflow(a):
    mdir = a.match_dir
    meta = read_json(os.path.join(mdir, "meta.json")) or {}
    fid = a.fixture or meta.get("fixture_id")
    out = {"match_id": meta.get("match_id"), "agent": "oddsflow_engine",
           "fixture_id": fid, "generated_at_utc": now_utc(),
           "status": "OK", "missing": [], "sources": []}

    if not fid:
        out["status"] = "UPSTREAM_MISSING"
        out["missing"].append("fixture_id (pas de snapshot API-Football appariable)")
        write_json(os.path.join(mdir, "00_oddsflow.json"), out)
        print("oddsflow : pas de fixture_id — métriques de marché NON calculables. Les agents marché "
              "devront s'appuyer sur des cotes sourcées manuellement.")
        return 0

    series = _snapshot_series(fid)
    if not series:
        out["status"] = "UPSTREAM_MISSING"
        out["missing"].append(f"aucun relevé snapshot pour fixture_id={fid}")
        write_json(os.path.join(mdir, "00_oddsflow.json"), out)
        print(f"oddsflow : aucun relevé pour fixture_id={fid}. Lancer d'abord apex_apifootball snapshot.")
        return 0

    first, last = series[0], series[-1]
    for r in (first, last):
        out["sources"].append({"url": f"API-Football/odds#fixture={fid}",
                               "retrieved_at_utc": r.get("retrieved_at_utc")})

    def pin_or_median(odds):
        books = {k: v for k, v in (odds or {}).items() if not k.startswith("_") and "1X2" in v}
        if not books:
            return None, None
        if "Pinnacle" in books:
            return "Pinnacle", _demargin_1x2(books["Pinnacle"]["1X2"])
        # médiane
        med = [sorted(b["1X2"][i] for b in books.values())[len(books) // 2] for i in range(3)]
        return f"médiane/{len(books)}", _demargin_1x2(med)

    bk0, p0 = pin_or_median(first.get("odds"))
    bk1, p1 = pin_or_median(last.get("odds"))
    out["opening"] = {"book": bk0, "fair_prob_1x2": p0, "at": first.get("retrieved_at_utc")}
    out["current"] = {"book": bk1, "fair_prob_1x2": p1, "at": last.get("retrieved_at_utc")}

    if p0 and p1:
        labels = ["home", "draw", "away"]
        delta = [round(p1[i] - p0[i], 4) for i in range(3)]
        out["prob_delta_1x2"] = dict(zip(labels, delta))
        idx = max(range(3), key=lambda i: abs(delta[i]))
        out["dominant_move"] = {"outcome": labels[idx], "delta_prob": delta[idx],
                                "direction": "shortening" if delta[idx] > 0 else "drifting"}
        # vitesse (points de proba par heure)
        try:
            t0 = datetime.fromisoformat(first["retrieved_at_utc"].replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(last["retrieved_at_utc"].replace("Z", "+00:00"))
            hrs = max((t1 - t0).total_seconds() / 3600.0, 1e-6)
            out["velocity_pp_per_h"] = round(abs(delta[idx]) * 100 / hrs, 3)
            out["span_hours"] = round(hrs, 2)
        except (KeyError, ValueError):
            out["velocity_pp_per_h"] = None
        # magnitude normalisée (0..1) pour alimenter MARKET_IMPACT
        out["move_magnitude"] = round(min(abs(delta[idx]) / 0.08, 1.0), 3)  # 8 pts de proba = mouvement majeur
    else:
        out["missing"].append("1X2 démarginable absent sur ouverture et/ou relevé courant")

    # dispersion inter-books au dernier relevé (proxy de consensus / désaccord)
    books_last = {k: v for k, v in (last.get("odds") or {}).items() if not k.startswith("_") and "1X2" in v}
    if len(books_last) >= 2:
        fair = [_demargin_1x2(b["1X2"]) for b in books_last.values()]
        fair = [f for f in fair if f]
        if fair:
            disp = [round(max(f[i] for f in fair) - min(f[i] for f in fair), 4) for i in range(3)]
            out["dispersion_1x2"] = dict(zip(["home", "draw", "away"], disp))
            out["n_books"] = len(fair)
            out["dispersion_max"] = max(disp)
        # Pinnacle vs médiane (proxy sharp)
        if "Pinnacle" in books_last and len(books_last) >= 3:
            pin = _demargin_1x2(books_last["Pinnacle"]["1X2"])
            others = [_demargin_1x2(b["1X2"]) for k, b in books_last.items() if k != "Pinnacle"]
            others = [o for o in others if o]
            if pin and others:
                med = [sorted(o[i] for o in others)[len(others) // 2] for i in range(3)]
                out["pinnacle_vs_median"] = {lab: round(pin[i] - med[i], 4)
                                             for i, lab in enumerate(["home", "draw", "away"])}
    else:
        out["missing"].append("un seul book : dispersion / Pinnacle-vs-médiane non calculables")

    # RLM : NON calculable sans % de parieurs publics (honnêteté WORM §4)
    out["rlm"] = "UNAVAILABLE"
    out["rlm_note"] = ("Reverse Line Movement NON calculable : le % de parieurs publics n'est pas "
                       "disponible via API-Football. Ne jamais affirmer un RLM sans ce côté public.")
    # Steam : nécessite plusieurs books mobiles entre relevés successifs — calcul simple si ≥3 relevés
    out["steam"] = _steam(series)

    write_json(os.path.join(mdir, "00_oddsflow.json"), out)
    dm = out.get("dominant_move")
    print(f"oddsflow {meta.get('match_id')} : "
          + (f"{dm['outcome']} {dm['direction']} Δ{dm['delta_prob']:+.3f} "
             f"(mag {out.get('move_magnitude')}, {out.get('velocity_pp_per_h')} pp/h)" if dm else "mouvement non calculable")
          + f" · dispersion_max {out.get('dispersion_max', '—')} · RLM {out['rlm']}")
    return 0


def _steam(series):
    """Proxy steam : plusieurs books se raccourcissent simultanément sur la même issue entre deux relevés."""
    if len(series) < 2:
        return {"status": "INSUFFICIENT_SNAPSHOTS", "note": "≥2 relevés requis"}
    prev, cur = series[-2], series[-1]
    moves = {"home": 0, "away": 0}
    n = 0
    pb = {k: v for k, v in (prev.get("odds") or {}).items() if not k.startswith("_") and "1X2" in v}
    cb = {k: v for k, v in (cur.get("odds") or {}).items() if not k.startswith("_") and "1X2" in v}
    for bk in set(pb) & set(cb):
        p0, p1 = _demargin_1x2(pb[bk]["1X2"]), _demargin_1x2(cb[bk]["1X2"])
        if not p0 or not p1:
            continue
        n += 1
        if p1[0] - p0[0] > 0.005:
            moves["home"] += 1
        if p1[2] - p0[2] > 0.005:
            moves["away"] += 1
    if n < 2:
        return {"status": "INSUFFICIENT_BOOKS", "books_compared": n}
    side = max(moves, key=moves.get)
    frac = moves[side] / n
    return {"status": "COMPUTED", "books_compared": n, "aligned_side": side,
            "aligned_books": moves[side], "aligned_fraction": round(frac, 2),
            "is_steam": frac >= 0.6 and moves[side] >= 2}


# ───────────────────────── Scoring d'un signal ─────────────────────────
def _confirmation_from_count(cross):
    return {0: 20, 1: 45, 2: 65, 3: 80}.get(min(cross, 3), 92) if cross >= 3 else {0: 20, 1: 45, 2: 65}[cross]


def _score_signal(family, source_tier, wave, magnitude, cross, kind):
    cls = FAMILY_CLASS[family]
    source = SOURCE_TIERS[source_tier]
    timing = TIMING_MATRIX[cls][wave]
    confirm = _confirmation_from_count(int(cross))
    mag = clamp(float(magnitude) * 100.0)
    # une famille non-marché ne peut pas s'offrir un fort MARKET_IMPACT sans confirmation marché :
    if cls in ("NEWS", "LINEUP", "SENTIMENT"):
        impact = clamp(mag * 0.6)
    else:
        impact = mag
    cap = KIND_CAP.get(kind, 90)
    signal = clamp(0.30 * source + 0.20 * timing + 0.25 * confirm + 0.25 * impact)
    signal = min(signal, cap)
    noise = clamp(0.50 * (100 - source) + 0.30 * (100 - confirm) + 0.20 * (100 - impact))
    # un emballement sentiment de source faible est surtout du bruit
    if cls == "SENTIMENT" and source < 45:
        noise = clamp(noise + 10)
    return {
        "SOURCE_RELIABILITY": round(source),
        "TIMING_RELEVANCE": round(timing),
        "CROSS_SOURCE_CONFIRMATION": round(confirm),
        "MARKET_IMPACT": round(impact),
        "MARKET_SIGNAL_SCORE": round(signal),
        "MARKET_NOISE_SCORE": round(noise),
        "band": band(signal),
        "family_class": cls,
    }


def cmd_signal(a):
    mdir = a.match_dir
    meta = read_json(os.path.join(mdir, "meta.json")) or {}
    if a.family not in VALID_FAMILIES:
        print(f"ERREUR : famille '{a.family}' hors nomenclature. Choisir parmi : {', '.join(sorted(VALID_FAMILIES))}")
        return 2
    if a.source_tier not in SOURCE_TIERS:
        print(f"ERREUR : source-tier '{a.source_tier}' inconnu. Choisir parmi : {', '.join(SOURCE_TIERS)}")
        return 2
    if a.wave not in WAVES:
        print(f"ERREUR : wave '{a.wave}' inconnue. Choisir parmi : {', '.join(WAVES)}")
        return 2
    kind = a.kind or "observation"
    sources = []
    if a.url:
        sources.append({"url": a.url, "retrieved_at_utc": a.retrieved or now_utc()})
    elif kind in ("fact", "observation"):
        print("ERREUR anti-hallucination : un fait/observation exige --url (source datée). "
              "Sans source, utiliser --kind interpretation.")
        return 2
    scored = _score_signal(a.family, a.source_tier, a.wave, a.magnitude, a.cross, kind)
    rec = {"id": f"{a.agent}:{a.family}:{now_utc()}:{uuid.uuid4().hex[:8]}", "match_id": meta.get("match_id"),
           "agent": a.agent, "family": a.family, "source_tier": a.source_tier,
           "wave": a.wave, "direction": a.direction, "magnitude": float(a.magnitude),
           "cross_cited": int(a.cross), "kind": kind, "note": a.note,
           "sources": sources, "generated_at_utc": now_utc(), **scored}
    with open(os.path.join(mdir, "signals.jsonl"), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"signal enregistré : {a.family} [{scored['family_class']}] dir={a.direction} "
          f"-> SIGNAL {scored['MARKET_SIGNAL_SCORE']} / NOISE {scored['MARKET_NOISE_SCORE']} "
          f"({scored['band']})")
    return 0


def cmd_behavioral(a):
    mdir = a.match_dir
    meta = read_json(os.path.join(mdir, "meta.json")) or {}
    if a.index not in BEHAVIORAL_INDICES:
        print(f"ERREUR : indice '{a.index}' inconnu. Choisir parmi : {', '.join(BEHAVIORAL_INDICES)}")
        return 2
    kind = a.kind or "observation"
    sources = []
    if a.url:
        sources.append({"url": a.url, "retrieved_at_utc": a.retrieved or now_utc()})
    elif kind in ("fact", "observation"):
        print("ERREUR anti-hallucination : un fait/observation exige --url. Sans source -> --kind interpretation.")
        return 2
    val = clamp(float(a.value))
    rec = {"id": f"{a.agent}:{a.index}:{now_utc()}:{uuid.uuid4().hex[:8]}", "match_id": meta.get("match_id"),
           "agent": a.agent, "level": a.level, "index": a.index, "value": round(val),
           "kind": kind, "weight": CONFIDENCE_W.get(a.confidence or "medium", 0.75) * (KIND_CAP.get(kind, 90) / 100.0),
           "confidence": a.confidence or "medium", "note": a.note, "team": a.team,
           "sources": sources, "generated_at_utc": now_utc()}
    with open(os.path.join(mdir, "behavioral.jsonl"), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"indice comportemental : {a.index}={rec['value']} ({a.level}, conf={rec['confidence']}, poids={rec['weight']:.2f})")
    return 0


# ───────────────────────── Lecture des journaux ─────────────────────────
def _read_jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


MARKET_AGENTS = [
    "odds-flow", "sharp-books", "exchange-flow", "steam-detector", "reverse-line",
    "news-pulse", "lineup-watch", "local-intel", "sentiment", "book-disagreement",
    "liquidity-timing",
]
BEHAVIOR_AGENTS = [
    "player", "squad", "team-identity", "coach", "league-culture",
    "country-context", "crowd", "motivation", "narrative",
]


def cmd_check(a):
    mdir = a.match_dir
    sig = _read_jsonl(os.path.join(mdir, "signals.jsonl"))
    beh = _read_jsonl(os.path.join(mdir, "behavioral.jsonl"))
    reported = {s.get("agent") for s in sig} | {b.get("agent") for b in beh}
    oddsflow = os.path.exists(os.path.join(mdir, "00_oddsflow.json"))
    scored = os.path.exists(os.path.join(mdir, "90_market_synthesis.json"))
    print(f"check {os.path.basename(mdir)} :")
    print(f"  oddsflow_engine : {'OK' if oddsflow else 'manquant'}")
    print(f"  signaux marché : {len(sig)} · indices comportementaux : {len(beh)}")
    print(f"  agents ayant reporté : {', '.join(sorted(reported)) or '—'}")
    missing_m = [x for x in MARKET_AGENTS if x not in reported]
    missing_b = [x for x in BEHAVIOR_AGENTS if x not in reported]
    if missing_m:
        print(f"  agents marché sans report : {', '.join(missing_m)}")
    if missing_b:
        print(f"  agents comportement sans report : {', '.join(missing_b)}")
    if not oddsflow:
        nxt = "oddsflow"
    elif not (sig or beh):
        nxt = "collecteurs (agents marché + comportement)"
    elif not scored:
        nxt = "score (synthétiseurs)"
    else:
        nxt = "finalize"
    print(f"  prochaine étape : {nxt}")
    return 0


# ───────────────────────── Agrégation (score) ─────────────────────────
DIR_GROUP = {  # direction normalisée -> camp pour convergence
    "home": "home", "away": "away", "draw": "draw",
    "over": "over", "under": "under", "btts_yes": "over", "btts_no": "under",
}


def _best_per_agent(signals):
    best = {}
    for s in signals:
        ag = s.get("agent")
        if ag not in best or s["MARKET_SIGNAL_SCORE"] > best[ag]["MARKET_SIGNAL_SCORE"]:
            best[ag] = s
    return best


def cmd_score(a):
    mdir = a.match_dir
    meta = read_json(os.path.join(mdir, "meta.json")) or {}
    signals = _read_jsonl(os.path.join(mdir, "signals.jsonl"))
    behav = _read_jsonl(os.path.join(mdir, "behavioral.jsonl"))

    # ---- Synthèse marché ----
    out = {"match_id": meta.get("match_id"), "home": meta.get("home"), "away": meta.get("away"),
           "competition": meta.get("competition"), "kickoff_utc": meta.get("kickoff_utc"),
           "generated_at_utc": now_utc(), "n_signals": len(signals)}

    if not signals:
        out.update({"market_state": "CALM", "dominant_signal": None, "market_signal_score": 0,
                    "status": "WATCH", "note": "aucun signal marché collecté"})
    else:
        best = _best_per_agent(signals)
        ranked = sorted(best.values(), key=lambda s: s["MARKET_SIGNAL_SCORE"], reverse=True)
        top = ranked[0]
        dom_dir = DIR_GROUP.get(top.get("direction"), top.get("direction"))
        # convergence : signaux ≥60 cohérents avec la direction dominante, d'agents distincts
        convergent = [s for s in ranked if s["MARKET_SIGNAL_SCORE"] >= 60
                      and DIR_GROUP.get(s.get("direction"), s.get("direction")) == dom_dir]
        contra = [s for s in ranked if s["MARKET_SIGNAL_SCORE"] >= 60
                  and DIR_GROUP.get(s.get("direction"), s.get("direction")) not in (dom_dir, None)]
        # score match : plus fort + bonus de convergence à rendements géométriquement
        # décroissants (plafonné) ; les 90-100 restent réservés à une convergence exceptionnelle.
        match_signal = float(top["MARKET_SIGNAL_SCORE"])
        bonus = 0.0
        for k, s in enumerate(convergent[1:]):
            bonus += (0.16 * s["MARKET_SIGNAL_SCORE"]) * (0.55 ** k)
        match_signal += min(bonus, 16.0)
        match_signal -= 0.12 * sum(s["MARKET_SIGNAL_SCORE"] for s in contra)
        match_signal = round(clamp(match_signal))

        disp = (read_json(os.path.join(mdir, "00_oddsflow.json")) or {}).get("dispersion_max")
        # état de marché
        market_driven = top["family_class"] == "MARKET"
        if match_signal >= 85:
            state = "SHARP" if market_driven and len(convergent) >= 2 else "DISLOCATED"
        elif match_signal >= 60:
            state = "ACTIVE"
        else:
            state = "CALM"
        if contra and disp and disp >= 0.06:
            state = "DISLOCATED"

        dom_family = top["family"]
        dom_map = {"MARKET": {"SHARP_MOVE": "SHARP", "STEAM_MOVE": "STEAM", "RLM": "RLM",
                              "LIQUIDITY_SPIKE": "LIQUIDITY"}, }
        dominant = dom_map.get("MARKET", {}).get(dom_family) or top["family_class"]

        src_q = "HIGH" if any(s["SOURCE_RELIABILITY"] >= 90 for s in convergent) else (
                "MEDIUM" if any(s["SOURCE_RELIABILITY"] >= 60 for s in convergent) else "LOW")

        has_market = any(s["family_class"] == "MARKET" and s["MARKET_SIGNAL_SCORE"] >= 60 for s in ranked)
        has_news = any(s["family_class"] in ("NEWS", "LINEUP") and s["MARKET_SIGNAL_SCORE"] >= 60 for s in ranked)
        if has_market and has_news:
            interp = "Information d'équipe probablement en cours d'absorption par le marché"
        elif has_market and src_q == "HIGH":
            interp = "Possible argent informé (informed money)"
        elif top["family_class"] == "SENTIMENT":
            interp = "Bruit public dominant — probablement déjà intégré / non exploitable"
        else:
            interp = "Mouvement à surveiller, confirmation insuffisante"

        # statut de LECTURE marché (format du synthétiseur) :
        if match_signal >= 75 and src_q in ("HIGH", "MEDIUM") and len(convergent) >= 2:
            status = "CONFIRMED"
        elif contra and match_signal < 60:
            status = "INVALIDATED"
        else:
            status = "WATCH"

        out.update({
            "market_state": state,
            "dominant_signal": dominant,
            "dominant_family": dom_family,
            "dominant_direction": top.get("direction"),
            "market_signal_score": match_signal,
            "market_signal_band": band(match_signal),
            "main_observation": top.get("note"),
            "confirmations": len(convergent) - 1 if convergent else 0,
            "contradictions": len(contra),
            "source_quality": src_q,
            "interpretation": interp,
            "status": status,
            "top_signals": [{"agent": s["agent"], "family": s["family"], "dir": s.get("direction"),
                             "signal": s["MARKET_SIGNAL_SCORE"], "noise": s["MARKET_NOISE_SCORE"]}
                            for s in ranked[:6]],
        })
    write_json(os.path.join(mdir, "90_market_synthesis.json"), out)

    # ---- Bloc comportemental ----
    bout = {"match_id": meta.get("match_id"), "generated_at_utc": now_utc(), "n_observations": len(behav)}
    idx_vals = {}
    for b in behav:
        idx_vals.setdefault(b["index"], []).append(b)
    agg = {}
    for idx, items in idx_vals.items():
        wsum = sum(i["weight"] for i in items) or 1.0
        agg[idx] = round(sum(i["value"] * i["weight"] for i in items) / wsum)
    bout["indices"] = agg

    narrative = agg.get("NARRATIVE_STRENGTH", 0)
    # priced-in : la narration est-elle DÉJÀ payée par le marché ? Deux voies indépendantes :
    #  (a) un signal marché fort la valide (argent informé déjà entré), OU
    #  (b) le marché est DÉJÀ tranché — favori court et ligne figée — donc l'histoire est
    #      dans le prix même sans mouvement frais. NE PAS confondre « pas de mouvement » avec
    #      « pas intégré » : un favori à 1.45 stable = narration pleinement pricée (piège du faux edge).
    ms = out.get("market_signal_score", 0)
    of = read_json(os.path.join(mdir, "00_oddsflow.json")) or {}
    cur_fair = (of.get("current") or {}).get("fair_prob_1x2") or []
    fav_prob = max(cur_fair) if cur_fair else None
    move_mag = of.get("move_magnitude")
    market_decided = fav_prob is not None and fav_prob >= 0.60 and (move_mag is None or move_mag < 0.30)
    if ms >= 65 or market_decided:
        priced_in = "LIKELY"
    elif ms >= 40 or (fav_prob is not None and fav_prob >= 0.50):
        priced_in = "PARTIAL"
    else:
        priced_in = "UNLIKELY"
    priced_val = {"LIKELY": 70, "PARTIAL": 40, "UNLIKELY": 10}[priced_in]
    net_edge_val = clamp(narrative - priced_val, -100, 100)
    net_edge = "HIGH" if net_edge_val >= 40 else ("MED" if net_edge_val >= 15 else "LOW")

    pressures = [agg.get(k, 0) for k in ("PRESSURE_INDEX", "MOTIVATION_INDEX", "INSTABILITY_INDEX",
                                         "COACH_PRESSURE", "SUPPORTER_PRESSURE", "MEDIA_PRESSURE")]
    beh_signal = "HIGH" if (max(pressures + [0]) >= 75 or narrative >= 75) else (
                 "MED" if max(pressures + [0]) >= 50 else "LOW")

    bout.update({
        "behavioral_signal": beh_signal,
        "narrative_strength": narrative,
        "market_priced_in": priced_in,
        "net_behavioral_edge": net_edge,
        "net_behavioral_edge_value": round(net_edge_val),
        "rule": "BEHAVIORAL seul -> WATCH ; + MARKET -> CANDIDATE ; + DATA (moteur stat) -> CONFIRMED",
        "behavioral_only_status": "WATCH",
    })
    write_json(os.path.join(mdir, "91_behavioral.json"), bout)

    # ---- Statut d'intégration (jamais un pari seul) ----
    has_market = out.get("market_signal_score", 0) >= 60
    has_behav = beh_signal in ("HIGH", "MED") or narrative >= 60
    if has_market and (has_behav or out.get("confirmations", 0) >= 1):
        integ = "CANDIDATE"
    elif has_market or has_behav:
        integ = "WATCH"
    else:
        integ = "WATCH"
    integration = {"match_id": meta.get("match_id"), "integration_status": integ,
                   "bet_authority": False,
                   "requires_statistical_convergence": True,
                   "note": ("Cette cellule n'émet jamais un pari seule. Transmettre au moteur statistique "
                            "APEX (BSM / S1-S8) pour la brique DATA avant toute décision."),
                   "generated_at_utc": now_utc()}
    write_json(os.path.join(mdir, "92_integration.json"), integration)

    print(f"score {meta.get('match_id')} : MARKET_STATE {out.get('market_state')} · "
          f"dominant {out.get('dominant_signal')} · MARKET_SIGNAL {out.get('market_signal_score')} "
          f"({out.get('market_signal_band', '—')}) · status {out.get('status')}")
    print(f"  BEHAVIORAL {beh_signal} · narrative {narrative} · priced-in {priced_in} · net edge {net_edge}")
    print(f"  INTEGRATION {integ} (bet_authority=False, requires_statistical_convergence=True)")
    return 0


# ───────────────────────── Synthèse finale ─────────────────────────
def cmd_finalize(a):
    run_dir = a.run
    req = read_json(os.path.join(run_dir, "request.json")) or {}
    mdirs = sorted(d for d in (os.path.join(run_dir, x) for x in os.listdir(run_dir))
                   if os.path.isdir(d) and os.path.exists(os.path.join(d, "meta.json")))
    lines = ["# APEX-MI — Synthèse Market & Behavioral Intelligence", ""]
    lines.append(f"Run `{req.get('run_id', os.path.basename(run_dir))}` · fenêtre {req.get('window_utc')}")
    lines.append(f"{len(mdirs)} match(s) · généré {now_utc()}")
    lines.append("")
    lines.append("> Cette cellule capte le BRUIT du marché et le CONTEXTE comportemental **avant** le "
                 "coup d'envoi. Elle ne price pas et **n'émet jamais un pari seule** : ses sorties "
                 "alimentent le moteur statistique APEX (brique DATA) pour la décision finale.")
    lines.append("")
    tg = []
    journal_rows = []
    for mdir in mdirs:
        meta = read_json(os.path.join(mdir, "meta.json")) or {}
        ms = read_json(os.path.join(mdir, "90_market_synthesis.json"))
        bh = read_json(os.path.join(mdir, "91_behavioral.json"))
        itg = read_json(os.path.join(mdir, "92_integration.json")) or {}
        title = f"{meta.get('home')} – {meta.get('away')}"
        lines.append(f"## {title}")
        lines.append(f"*{meta.get('competition') or '—'} · KO {meta.get('kickoff_utc') or '?'}*")
        lines.append("")
        if not ms:
            lines.append("_Non scoré (score non lancé)._\n")
            continue
        lines.append("```")
        lines.append(f"MARKET STATE          {ms.get('market_state')}")
        lines.append(f"DOMINANT SIGNAL       {ms.get('dominant_signal')}")
        lines.append(f"MARKET_SIGNAL_SCORE   {ms.get('market_signal_score')}/100  ({ms.get('market_signal_band')})")
        lines.append(f"MAIN OBSERVATION      {ms.get('main_observation') or '—'}")
        lines.append(f"CONFIRMATIONS         {ms.get('confirmations')}")
        lines.append(f"CONTRADICTIONS        {ms.get('contradictions')}")
        lines.append(f"SOURCE QUALITY        {ms.get('source_quality')}")
        lines.append(f"INTERPRETATION        {ms.get('interpretation')}")
        lines.append(f"STATUS                {ms.get('status')}")
        lines.append("")
        if bh:
            lines.append("BEHAVIORAL CONTEXT")
            for k in BEHAVIORAL_INDICES:
                if k in (bh.get("indices") or {}):
                    lines.append(f"  {k:<20} {bh['indices'][k]}/100")
            lines.append(f"  BEHAVIORAL SIGNAL    {bh.get('behavioral_signal')}")
            lines.append(f"  MARKET PRICED-IN     {bh.get('market_priced_in')}")
            lines.append(f"  NET BEHAVIORAL EDGE  {bh.get('net_behavioral_edge')}")
        lines.append("")
        lines.append(f"INTEGRATION           {itg.get('integration_status')}  "
                     f"(bet_authority={itg.get('bet_authority')}, needs DATA=stat engine)")
        lines.append("```")
        lines.append("")
        tg.append(f"• {title} — {ms.get('market_state')}/{ms.get('dominant_signal')} "
                  f"{ms.get('market_signal_score')} [{itg.get('integration_status')}]")
        journal_rows.append([
            (meta.get("kickoff_utc") or "")[:10], meta.get("match_id"), meta.get("competition") or "",
            ms.get("market_state") or "", ms.get("dominant_signal") or "",
            ms.get("market_signal_score"), ms.get("confirmations"), ms.get("contradictions"),
            ms.get("source_quality") or "", ms.get("status") or "",
            (bh or {}).get("behavioral_signal") or "", (bh or {}).get("net_behavioral_edge") or "",
            itg.get("integration_status") or "", now_utc(),
        ])

    synth_path = os.path.join(run_dir, "SYNTHESE.md")
    with open(synth_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    with open(os.path.join(run_dir, "telegram.txt"), "w", encoding="utf-8") as fh:
        fh.write("APEX-MI — bruit marché & contexte (pré-match)\n" + "\n".join(tg)
                 + "\n\n⚠ Signaux à confirmer par le moteur statistique APEX avant toute mise.\n")

    # journal append-only
    os.makedirs(os.path.dirname(JOURNAL), exist_ok=True)
    new = not os.path.exists(JOURNAL)
    import csv
    with open(JOURNAL, "a", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["date", "match_id", "competition", "market_state", "dominant_signal",
                        "market_signal_score", "confirmations", "contradictions", "source_quality",
                        "market_status", "behavioral_signal", "net_behavioral_edge",
                        "integration_status", "logged_at_utc"])
        for row in journal_rows:
            w.writerow(row)

    print(f"Synthèse : {os.path.relpath(synth_path, ROOT)}")
    print(f"Journal : +{len(journal_rows)} ligne(s) -> {os.path.relpath(JOURNAL, ROOT)}")
    print("\n".join(tg) if tg else "(aucun match scoré)")

    # Email OBLIGATOIRE à chaque passage : finalize construit toujours l'artefact du digest.
    # L'envoi se fait ensuite via le connecteur Gmail (mcp__Gmail__send_message), SMTP absent.
    subject, html_body, text_body = build_email_html(run_dir)
    for fn, body in (("email.html", html_body), ("email.txt", text_body), ("email.subject.txt", subject)):
        with open(os.path.join(run_dir, fn), "w", encoding="utf-8") as fh:
            fh.write(body)
    print("\n" + "=" * 70)
    print("⚠ ENVOI EMAIL OBLIGATOIRE (règle : tout passage se termine par l'email).")
    print(f"  SUJET : {subject}")
    print(f"  Envoyer via mcp__Gmail__send_message → to=okoma.leonce@gmail.com")
    print(f"  htmlBody = {os.path.relpath(os.path.join(run_dir, 'email.html'), ROOT)} · "
          f"body = {os.path.relpath(os.path.join(run_dir, 'email.txt'), ROOT)}")
    print("  SMTP non configuré → canal Gmail MCP (recharger via ToolSearch si déconnecté). Preuve = threadId.")
    print("=" * 70)
    return 0


# ───────────────────── Email du passage (digest HTML) ─────────────────────
def build_email_html(run_dir):
    """(sujet, html, texte) du passage. Même esprit que apex_worm.build_email_html :
    le moteur produit le digest, l'envoi réel se fait via le connecteur Gmail (mcp__Gmail__send_message),
    SMTP non configuré. Ne price pas, n'émet aucun pari."""
    req = read_json(os.path.join(run_dir, "request.json")) or {}
    rows = []
    for name in sorted(os.listdir(run_dir)):
        d = os.path.join(run_dir, name)
        ms = read_json(os.path.join(d, "90_market_synthesis.json"))
        if not ms:
            continue
        meta = read_json(os.path.join(d, "meta.json")) or {}
        bh = read_json(os.path.join(d, "91_behavioral.json")) or {}
        itg = read_json(os.path.join(d, "92_integration.json")) or {}
        rows.append((meta, ms, bh, itg))
    rows.sort(key=lambda r: (r[3].get("integration_status") != "CANDIDATE",
                             -(r[1].get("market_signal_score") or 0)))
    now = now_utc().replace("T", " ")
    nc = sum(1 for r in rows if r[3].get("integration_status") == "CANDIDATE")
    subject = f"APEX-MI — bruit marché pré-match · {nc} CANDIDATE / {len(rows)} matchs · {now}"

    def esc(x):
        return _html.escape(str(x if x is not None else "—"))

    css = ("body{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#1a1a2e;background:#f4f5f7;margin:0;padding:16px}"
           ".card{background:#fff;border-radius:12px;padding:16px 18px;max-width:860px;margin:0 auto 14px;box-shadow:0 1px 4px rgba(0,0,0,.08)}"
           "h1{font-size:19px;margin:0 0 4px}h2{font-size:15px;margin:16px 0 8px;color:#3b3b58}.muted{color:#6b7280;font-size:12px}"
           "table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:6px 8px;border-bottom:1px solid #eee;text-align:left}"
           "th{background:#fafafe;color:#555}.cand{background:#e7f6ec;color:#137333;font-weight:700;border-radius:6px;padding:1px 7px}"
           ".watch{color:#6b7280}.r{text-align:right}.tag{color:#4338ca}")
    H = [f"<html><head><meta charset='utf-8'><style>{css}</style></head><body><div class='card'>",
         "<h1>APEX-MI — bruit de marché &amp; comportement (pré-match)</h1>",
         f"<div class='muted'>Run <b>{esc(req.get('run_id') or os.path.basename(run_dir))}</b> · {len(rows)} matchs · "
         f"{nc} CANDIDATE · généré {esc(now)}</div>",
         "<div class='muted'>Cellule séparée de la cellule statistique : capte le bruit marché/comportement, "
         "<b>ne price pas</b> et <b>n'émet aucun pari</b> (bet_authority=false). Les CANDIDATE sont à transmettre "
         "au moteur statistique APEX (brique DATA).</div>",
         "<h2>Synthèse de la slate</h2>",
         "<table><tr><th>Match</th><th>État</th><th>Dominant</th><th class='r'>Signal</th><th>Source</th>"
         "<th>Behav.</th><th>Priced-in</th><th>Net edge</th><th>Intégration</th></tr>"]
    tg = [f"APEX-MI — {nc} CANDIDATE / {len(rows)} matchs ({now})", ""]
    for meta, S, B, I in rows:
        integ = I.get("integration_status", "WATCH")
        badge = (f"<span class='cand'>{esc(integ)}</span>" if integ == "CANDIDATE"
                 else f"<span class='watch'>{esc(integ)}</span>")
        H.append("<tr>"
                 f"<td><b>{esc(meta.get('home'))} – {esc(meta.get('away'))}</b></td>"
                 f"<td>{esc(S.get('market_state'))}</td><td class='tag'>{esc(S.get('dominant_signal'))}</td>"
                 f"<td class='r'>{esc(S.get('market_signal_score'))}</td><td>{esc(S.get('source_quality'))}</td>"
                 f"<td>{esc(B.get('behavioral_signal'))}</td><td>{esc(B.get('market_priced_in'))}</td>"
                 f"<td>{esc(B.get('net_behavioral_edge'))}</td><td>{badge}</td></tr>")
        tg.append(f"- {meta.get('home')} - {meta.get('away')}: {S.get('market_state')}/{S.get('dominant_signal')} "
                  f"{S.get('market_signal_score')} [{integ}]")
    H.append("</table>")
    cands = [r for r in rows if r[3].get("integration_status") == "CANDIDATE"]
    if cands:
        H.append("<h2>Détail des signaux CANDIDATE</h2>")
        for meta, S, B, I in cands:
            H.append("<div style='margin:10px 0;padding:10px 12px;background:#f8fbf9;border-left:3px solid #137333;border-radius:6px'>"
                     f"<b>{esc(meta.get('home'))} – {esc(meta.get('away'))}</b> · {esc(meta.get('competition'))} · "
                     f"KO {esc((meta.get('kickoff_utc') or '')[11:16])}Z<br>"
                     f"<span class='muted'>{esc(S.get('main_observation'))}</span><br>"
                     f"<b>{esc(S.get('market_state'))} / {esc(S.get('dominant_signal'))} {esc(S.get('market_signal_score'))}/100</b> · "
                     f"source {esc(S.get('source_quality'))} · {esc(S.get('interpretation'))}<br>"
                     f"Behavioral {esc(B.get('behavioral_signal'))} · priced-in {esc(B.get('market_priced_in'))} · "
                     f"net edge {esc(B.get('net_behavioral_edge'))}</div>")
    H.append("<div class='muted'>RLM non calculable sans % public ; volume réel seulement si exchange actif. "
             "Convergence = agents indépendants (un même mouvement vu deux fois ne compte qu'une fois). "
             "Rien d'inventé : donnée absente = écrite comme absente.</div></div></body></html>")
    tg.append("\nCellule APEX-MI : ne price pas, n'émet aucun pari. À transmettre au moteur statistique.")
    return subject, "\n".join(H), "\n".join(tg)


def cmd_email(a):
    run_dir = a.run
    subject, html_body, text_body = build_email_html(run_dir)
    hp = os.path.join(run_dir, "email.html")
    tp = os.path.join(run_dir, "email.txt")
    with open(hp, "w", encoding="utf-8") as fh:
        fh.write(html_body)
    with open(tp, "w", encoding="utf-8") as fh:
        fh.write(text_body)
    # le sujet est écrit à part pour un envoi scripté/automatique
    with open(os.path.join(run_dir, "email.subject.txt"), "w", encoding="utf-8") as fh:
        fh.write(subject)
    print(f"SUBJECT: {subject}")
    print(f"HTML: {os.path.relpath(hp, ROOT)} · TXT: {os.path.relpath(tp, ROOT)}")
    print("Envoi : via le connecteur Gmail (mcp__Gmail__send_message), to=okoma.leonce@gmail.com, "
          "htmlBody=email.html, body=email.txt. SMTP non configuré.")
    return 0


# ───────────────────── Pont APEX-WORM — activation H-60 (focus UPSET) ─────────────────────
def _worm_series(day):
    """Relevés WORM du jour groupés par fixture_id et triés chronologiquement."""
    path = os.path.join(WORM_SNAP, f"{day}.jsonl")
    series = {}
    if not os.path.exists(path):
        return series, path
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        series.setdefault(r.get("fixture_id"), []).append(r)
    for fid in series:
        series[fid].sort(key=lambda r: r.get("scan_time_utc", ""))
    return series, path


def _mins_to_ko(kickoff, now):
    try:
        k = datetime.fromisoformat((kickoff or "").replace("Z", "+00:00"))
        return (k - now).total_seconds() / 60.0
    except (ValueError, AttributeError):
        return None


def _mi_from_worm(fid, recs, cur, mk):
    """Lecture APEX-MI d'un match imminent à partir des relevés WORM. Focus UPSET.

    WORM a déjà démarginé le marché (`market_prob_1x2`) et calculé le score `upset`
    (outsider structurellement sous-évalué). APEX-MI ajoute la LECTURE DE MOUVEMENT :
    l'argent se dirige-t-il vers l'outsider dans la fenêtre H-60 (confirmation), ou s'en
    éloigne-t-il (upset qui refroidit) ? La valeur vient du croisement des deux.
    """
    missing = []
    fair_now = cur.get("market_prob_1x2")
    first_fair = next((r["market_prob_1x2"] for r in recs if r.get("market_prob_1x2")), None)
    up = cur.get("upset")
    upc = cur.get("upset_components") or {}

    dog_idx, dog_side = None, None
    if upc.get("dog_at_home") is True:
        dog_idx, dog_side = 0, "home"
    elif upc.get("dog") in ("extérieur", "exterieur"):
        dog_idx, dog_side = 2, "away"
    elif fair_now:
        dog_idx = 0 if fair_now[0] < fair_now[2] else 2
        dog_side = "home" if dog_idx == 0 else "away"

    dog_open = first_fair[dog_idx] if (first_fair and dog_idx is not None) else None
    dog_now = fair_now[dog_idx] if (fair_now and dog_idx is not None) else None
    dog_delta = round(dog_now - dog_open, 4) if (dog_open is not None and dog_now is not None) else None

    mi_signal = None
    if dog_delta is not None:
        shortening = dog_delta > 0  # l'outsider se raccourcit = argent vers le dog
        family = "PRICE_COMPRESSION" if shortening else "PRICE_DRIFT"
        magnitude = min(abs(dog_delta) / 0.05, 1.0)  # 5 pts de proba = mouvement fort en H-60
        tier = "sharp" if "Pinnacle" in (cur.get("odds") or {}) else "aggregator"
        mi_signal = _score_signal(family, tier, "LATE", magnitude, 0, "observation")
        mi_signal.update({"family": family, "direction": dog_side, "dog_delta": dog_delta,
                          "source_tier": tier})
    else:
        missing.append("trajectoire outsider non calculable (cote juste absente)")

    # confirmation d'upset = mouvement DU MARCHÉ VERS l'outsider seulement
    confirm = mi_signal["MARKET_SIGNAL_SCORE"] if (mi_signal and dog_delta and dog_delta > 0) else 0
    if up is not None:
        watch = round(0.55 * up + 0.45 * confirm)
    elif dog_delta is not None:
        watch = round(confirm)
        missing.append("score UPSET WORM absent (classement indisponible)")
    else:
        watch = None

    if up is not None and dog_delta is not None and dog_delta > 0 and up >= 50:
        status = "LIVE_UPSET_WATCH"      # upset structurel + argent qui va vers l'outsider
    elif dog_delta is not None and dog_delta < 0:
        status = "UPSET_FADING"          # le marché s'éloigne de l'outsider
    else:
        status = "WATCH"

    exch = cur.get("exchange")
    volume = exch.get("total_matched") if isinstance(exch, dict) else None
    if volume is None:
        missing.append("volume réel indisponible (excapper/--money inactif)")

    return {
        "fixture_id": fid,
        "match": f"{cur.get('home')} – {cur.get('away')}",
        "league": cur.get("league"), "country": cur.get("country"),
        "kickoff": cur.get("kickoff"), "minutes_to_ko": round(mk),
        "dog_side": dog_side,
        "worm_upset": up, "worm_upset_components": upc,
        "dog_prob_open": dog_open, "dog_prob_now": dog_now, "dog_delta": dog_delta,
        "mi_move_family": mi_signal.get("family") if mi_signal else None,
        "mi_move_signal_score": mi_signal["MARKET_SIGNAL_SCORE"] if mi_signal else None,
        "dispersion_max": cur.get("market_dispersion"),
        "exchange_volume": volume,
        "upset_watch_score": watch,
        "upset_watch_band": band(watch),
        "status": status,
        "rlm": "UNAVAILABLE (pas de % public)",
        "bet_authority": False,
        "missing": missing,
    }


def _write_mi_md(day, artifact):
    L = [f"## APEX-MI — bruit de marché H-60 (focus UPSET) · {day}", "",
         f"_{artifact['n_selected']} match(s) imminent(s) · généré {artifact['generated_at_utc']} · "
         "cette cellule n'émet aucun pari, elle alimente le moteur statistique._", ""]
    items = artifact["items"]
    if not items:
        L.append("Aucun match dans la fenêtre H-60 à ce passage.")
    else:
        L.append("| Dans | Match | Compét. | Outsider | ΔProb dog | Mouvement | UPSET WORM | UPSET WATCH | Statut |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for it in items:
            dd = it["dog_delta"]
            dd_s = f"{dd:+.3f}" if dd is not None else "—"
            L.append(f"| {it['minutes_to_ko']}′ | {it['match']} | {(it.get('country') or '')[:3]} "
                     f"{(it.get('league') or '')[:16]} | {it['dog_side'] or '—'} | {dd_s} | "
                     f"{it['mi_move_family'] or '—'} | {it['worm_upset'] if it['worm_upset'] is not None else '—'} | "
                     f"**{it['upset_watch_score'] if it['upset_watch_score'] is not None else '—'}** "
                     f"({it['upset_watch_band']}) | {it['status']} |")
        L += ["", "> UPSET WATCH = 0.55·UPSET structurel (WORM) + 0.45·confirmation de mouvement vers "
              "l'outsider (APEX-MI). `LIVE_UPSET_WATCH` = outsider sous-évalué ET argent qui va vers lui. "
              "RLM non calculable sans % public ; volume réel seulement si excapper actif."]
    os.makedirs(WORM_REP, exist_ok=True)
    with open(os.path.join(WORM_REP, f"{day}.mi.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(L))


def run_worm_hook(day, within=60.0, write=True):
    """Activation H-60 : sélectionne les matchs WORM en PREMATCH dont le coup d'envoi est
    dans `within` minutes, en produit une lecture APEX-MI orientée UPSET, et dépose un
    artefact que l'email WORM joint. Retourne l'artefact (dict)."""
    series, path = _worm_series(day)
    now = datetime.now(timezone.utc)
    selected = []
    for fid, recs in series.items():
        cur = recs[-1]
        if cur.get("phase") != "PREMATCH":
            continue
        mk = _mins_to_ko(cur.get("kickoff"), now)
        if mk is None or not (0 <= mk <= within):
            continue
        selected.append((mk, fid, recs, cur))
    selected.sort(key=lambda x: x[0])
    items = [_mi_from_worm(fid, recs, cur, mk) for mk, fid, recs, cur in selected]
    items.sort(key=lambda it: (it.get("upset_watch_score") if it.get("upset_watch_score") is not None else -1),
               reverse=True)
    artifact = {"day": day, "generated_at_utc": now_utc(), "within_min": within,
                "focus": "UPSET", "snapshot": os.path.relpath(path, ROOT),
                "n_selected": len(items), "items": items,
                "note": "APEX-MI ne price pas et n'émet aucun pari (bet_authority=false) ; "
                        "ses signaux alimentent le moteur statistique APEX."}
    if write:
        write_json(os.path.join(WORM_MI, f"{day}.json"), artifact)
        _write_mi_md(day, artifact)
    return artifact


def cmd_worm_hook(a):
    day = a.day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    art = run_worm_hook(day, float(a.within), write=not a.dry_run)
    print(f"APEX-MI worm-hook {day} : {art['n_selected']} match(s) H-{int(a.within)} (focus UPSET)")
    for it in art["items"][:15]:
        print(f"  {it['minutes_to_ko']:>3}′ {it['match'][:42]:42s} UPSET WATCH "
              f"{it['upset_watch_score'] if it['upset_watch_score'] is not None else '—'} "
              f"[{it['status']}] dogΔ {it['dog_delta'] if it['dog_delta'] is not None else '—'}")
    if not a.dry_run:
        print(f"Artefact : data/worm/mi/{day}.json · reports/worm/{day}.mi.md")
    return 0


# ───────────────────────── CLI ─────────────────────────
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)

    sp.add_parser("window", help="Fenêtre APEX et vagues temporelles")

    pi = sp.add_parser("init", help="Résout les matchs et crée les dossiers de run")
    pi.add_argument("--input", help="fichier YAML/JSON ou JSON inline (fixtures / date)")
    pi.add_argument("--date", help="date (lit data/apifootball/snapshots/<date>.jsonl)")
    pi.add_argument("--leagues", help="ids de ligue séparés par des virgules (filtre snapshot)")
    pi.add_argument("--run-id", dest="run_id")

    po = sp.add_parser("oddsflow", help="Métriques de marché mécaniques depuis les snapshots")
    po.add_argument("--match-dir", required=True)
    po.add_argument("--fixture", type=int, help="override du fixture_id")

    ps = sp.add_parser("signal", help="Enregistre une observation de marché scorée")
    ps.add_argument("--match-dir", required=True)
    ps.add_argument("--agent", required=True)
    ps.add_argument("--family", required=True, help=", ".join(sorted(VALID_FAMILIES)))
    ps.add_argument("--source-tier", required=True, dest="source_tier", help=", ".join(SOURCE_TIERS))
    ps.add_argument("--wave", required=True, help=", ".join(WAVES))
    ps.add_argument("--direction", required=True, help="home/draw/away/over/under/btts_yes/btts_no…")
    ps.add_argument("--magnitude", required=True, help="ampleur observée 0..1")
    ps.add_argument("--cross", default=0, help="nombre de sources indépendantes corroborantes")
    ps.add_argument("--kind", choices=["fact", "observation", "interpretation"], default="observation")
    ps.add_argument("--note", default="")
    ps.add_argument("--url", help="URL source datée (obligatoire pour fact/observation)")
    ps.add_argument("--retrieved", help="retrieved_at_utc de la source")

    pb = sp.add_parser("behavioral", help="Enregistre un indice comportemental scoré")
    pb.add_argument("--match-dir", required=True)
    pb.add_argument("--agent", required=True)
    pb.add_argument("--level", required=True, choices=["player", "squad", "team", "coach", "league", "country", "crowd", "match", "market"])
    pb.add_argument("--index", required=True, help=", ".join(BEHAVIORAL_INDICES))
    pb.add_argument("--value", required=True, help="valeur d'indice 0..100")
    pb.add_argument("--team", help="équipe concernée (si applicable)")
    pb.add_argument("--kind", choices=["fact", "observation", "interpretation"], default="observation")
    pb.add_argument("--confidence", choices=["low", "medium", "high"], default="medium")
    pb.add_argument("--note", default="")
    pb.add_argument("--url")
    pb.add_argument("--retrieved")

    pc = sp.add_parser("check", help="État des agents d'un match + prochaine étape")
    pc.add_argument("--match-dir", required=True)

    psc = sp.add_parser("score", help="Agrège signaux + indices -> synthèse")
    psc.add_argument("--match-dir", required=True)

    pf = sp.add_parser("finalize", help="SYNTHESE.md + telegram + journal")
    pf.add_argument("--run", required=True)

    pe = sp.add_parser("email", help="Construit le digest HTML du passage (à envoyer via Gmail MCP)")
    pe.add_argument("--run", required=True)

    pw = sp.add_parser("worm-hook", help="Activation H-60 depuis un scan WORM, rapport UPSET pour l'email")
    pw.add_argument("--day", help="jour de la fenêtre WORM (défaut aujourd'hui UTC)")
    pw.add_argument("--within", default=60, help="fenêtre H-N minutes avant le coup d'envoi (défaut 60)")
    pw.add_argument("--dry-run", action="store_true", help="n'écrit pas l'artefact (affiche seulement)")

    a = p.parse_args()
    return {
        "window": cmd_window, "init": cmd_init, "oddsflow": cmd_oddsflow,
        "signal": cmd_signal, "behavioral": cmd_behavioral, "check": cmd_check,
        "score": cmd_score, "finalize": cmd_finalize, "email": cmd_email,
        "worm-hook": cmd_worm_hook,
    }[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
