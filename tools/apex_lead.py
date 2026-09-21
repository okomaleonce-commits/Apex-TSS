#!/usr/bin/env python3
"""APEX-LEAD — outillage mécanique du protocole d'équipe d'agents.

Ce module ne fait AUCUNE analyse sportive. Il rend mécaniques les trois
points du protocole qui ne doivent jamais dépendre du raisonnement du
conducteur :

  1. la validation de l'enveloppe JSON commune de chaque agent,
  2. l'évaluation des gates G0-G7 (lecture du champ `status`, pas du texte),
  3. le recalcul des edges, le contrôle de bankroll et la production des
     livrables (SYNTHESE.md, telegram.txt, synthese.json, journal CSV).

Usage :
    python3 tools/apex_lead.py init      --input <input.yaml|json>
    python3 tools/apex_lead.py check     --match-dir runs/<date>/<match_id>
    python3 tools/apex_lead.py finalize  --date <YYYY-MM-DD>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOURNAL = os.path.join(ROOT, "journal", "apex_journal.csv")

JOURNAL_COLUMNS = [
    "date", "match_id", "competition", "engine", "drs", "vs", "market",
    "selection", "p_model", "fair_odds", "market_odds", "edge_pct",
    "stake_units", "verdict", "blocking_gate", "result", "pnl_units",
]

# Fichier attendu par agent, dans l'ordre strict du pipeline.
STEPS = [
    ("A0_ROUTER",     "00_routing.json"),
    ("A1_DATA",       "01_scraper.json"),
    ("A1_DATA",       "02_s1_integrity.json"),
    ("A2_CONTEXTE",   "03_s2_context.json"),
    ("A3_TACTIQUE",   "04_s3_tactical.json"),
    ("A4_PRICING",    "05_s4_pricing.json"),
    ("A5_RISQUE",     "06_s5_volatility.json"),
    ("A6_MARCHE",     "07_s6_market.json"),
    ("A7_DECISION",   "08_s7_decision.json"),
    ("A8_CONSEIL",    "09_s8_council.json"),
    ("A9_PREFLIGHT",  "10_preflight.json"),
]

ENVELOPE_KEYS = [
    "match_id", "agent", "skill_versions", "status", "flags", "confidence",
    "payload", "missing_fields", "sources", "generated_at_utc",
]

VALID_STATUS = {
    "OK", "ABORT", "NO_BET", "WAIT_LINEUPS", "LIVE_ONLY", "DATA_REQUEST",
    "UPSTREAM_MISSING",
}

DEFAULT_DRS_THRESHOLD = 60.0
PROB_SUM_TOLERANCE = 0.005
EDGE_TOLERANCE_PTS = 0.1


# --------------------------------------------------------------------------
# Utilitaires
# --------------------------------------------------------------------------

def slug(text: str) -> str:
    """Snake_case ASCII : 'Bodø/Glimt' -> 'bodo_glimt'."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace("ø", "o").replace("Ø", "O").replace("ß", "ss")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    return text


def match_id_of(home: str, away: str, kickoff_utc: str) -> str:
    day = kickoff_utc[:10].replace("-", "")
    return f"{slug(home)}_{slug(away)}_{day}"


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_input(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        raw = fh.read()
    if path.endswith((".yaml", ".yml")):
        import yaml  # dépendance déjà présente dans l'environnement
        return yaml.safe_load(raw)
    return json.loads(raw)


def read_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        text = fh.read().strip()
    if not text:
        return None
    return json.loads(text)


def dig(payload, *keys, default=None):
    """Lecture tolérante : renvoie la première clé présente et non nulle."""
    if not isinstance(payload, dict):
        return default
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return default


def as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# Validation d'enveloppe
# --------------------------------------------------------------------------

def validate_envelope(obj, filename: str) -> list[str]:
    errors = []
    if not isinstance(obj, dict):
        return [f"{filename}: racine JSON non-objet"]
    for key in ENVELOPE_KEYS:
        if key not in obj:
            errors.append(f"{filename}: clé d'enveloppe manquante '{key}'")
    status = obj.get("status")
    if status is not None and status not in VALID_STATUS:
        errors.append(f"{filename}: status '{status}' hors nomenclature")
    for src in obj.get("sources") or []:
        if not isinstance(src, dict) or not src.get("url"):
            errors.append(f"{filename}: source sans url")
        elif not src.get("retrieved_at_utc"):
            errors.append(f"{filename}: source sans retrieved_at_utc ({src.get('url')})")
    return errors


def validate_probabilities(pricing) -> list[str]:
    """Règle anti-hallucination n°5 : somme 1X2 = 1 ± 0,005."""
    if not isinstance(pricing, dict):
        return []
    payload = pricing.get("payload") or {}
    probs = dig(payload, "probabilities", "p_1x2", "probs_1x2")
    if not isinstance(probs, dict):
        return []
    values = [as_float(probs.get(k)) for k in ("home", "draw", "away", "1", "X", "2")]
    values = [v for v in values if v is not None]
    if len(values) < 3:
        return ["05_s4_pricing.json: probabilités 1X2 incomplètes"]
    total = sum(values[:3])
    if abs(total - 1.0) > PROB_SUM_TOLERANCE:
        return [f"05_s4_pricing.json: somme 1X2 = {total:.4f} hors tolérance ±{PROB_SUM_TOLERANCE}"]
    return []


def recompute_edge(decision) -> dict:
    """edge_pct = (p_model × market_odds − 1) × 100, calculé ici et non de tête."""
    payload = (decision or {}).get("payload") or {}
    p_model = as_float(payload.get("p_model"))
    market_odds = as_float(payload.get("market_odds"))
    declared = as_float(payload.get("edge_pct"))
    out = {"p_model": p_model, "market_odds": market_odds,
           "declared_edge_pct": declared, "computed_edge_pct": None,
           "delta": None, "mismatch": False}
    if p_model is None or market_odds is None:
        return out
    computed = (p_model * market_odds - 1.0) * 100.0
    out["computed_edge_pct"] = round(computed, 4)
    if declared is not None:
        out["delta"] = round(abs(computed - declared), 4)
        out["mismatch"] = out["delta"] > EDGE_TOLERANCE_PTS
    return out


# --------------------------------------------------------------------------
# Gates G0-G7
# --------------------------------------------------------------------------

def evaluate_gates(match_dir: str) -> dict:
    """Évalue les gates dans l'ordre. S'arrête à la première bloquante."""
    def f(name):
        return read_json(os.path.join(match_dir, name))

    errors, gate_log = [], []
    routing = f("00_routing.json")
    drs_threshold = DEFAULT_DRS_THRESHOLD
    if routing:
        errors += validate_envelope(routing, "00_routing.json")
        adj = dig(routing.get("payload") or {}, "fallback_adjustments", default={}) or {}
        if dig(routing.get("payload") or {}, "fallback_mode", default=False):
            # Fallback ligue : DCS −5 sur le seuil de fiabilité.
            delta = as_float(adj.get("dcs_delta"))
            drs_threshold += delta if delta is not None else -5.0

    def blocked(gate, verdict, reason):
        return {"blocking_gate": gate, "verdict": verdict, "reason": reason,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": None, "complete": True}

    # --- G0 : scraper absent ou vide -------------------------------------
    scraper = f("01_scraper.json")
    if scraper is None:
        gate_log.append("G0 PENDING (01_scraper.json absent)")
        return {"blocking_gate": None, "verdict": "PENDING", "reason": None,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": next_step(match_dir), "complete": False}
    errors += validate_envelope(scraper, "01_scraper.json")
    if not (scraper.get("payload") or {}) or scraper.get("status") == "DATA_REQUEST":
        return blocked("G0", "DATA_REQUEST", "01_scraper.json vide ou statut DATA_REQUEST")
    gate_log.append("G0 PASS")

    # --- G1 : intégrité des données --------------------------------------
    s1 = f("02_s1_integrity.json")
    if s1 is None:
        return pending(gate_log, errors, match_dir, "G1")
    errors += validate_envelope(s1, "02_s1_integrity.json")
    drs = as_float(dig(s1.get("payload") or {}, "drs", "data_reliability_score"))
    if s1.get("status") == "ABORT":
        return blocked("G1", "NO_BET (data)", "S1 status = ABORT")
    if drs is not None and drs < drs_threshold:
        return blocked("G1", "NO_BET (data)", f"DRS {drs} < seuil {drs_threshold}")
    gate_log.append(f"G1 PASS (DRS={drs}, seuil={drs_threshold})")

    # --- G2 : contexte / dead rubber -------------------------------------
    s2 = f("03_s2_context.json")
    if s2 is None:
        return pending(gate_log, errors, match_dir, "G2")
    errors += validate_envelope(s2, "03_s2_context.json")
    p2 = s2.get("payload") or {}
    dead = "DEAD_RUBBER" in (s2.get("flags") or []) or dig(p2, "flag") == "DEAD_RUBBER"
    compensated = bool(dig(p2, "dead_rubber_compensated", "compensated", default=False))
    if dead and not compensated:
        return blocked("G2", "NO_BET (context)", "DEAD_RUBBER non compensé")
    gate_log.append("G2 PASS")

    # --- pas de gate sur A3/A4, mais contrôle de calibration --------------
    s3 = f("04_s3_tactical.json")
    if s3 is None:
        return pending(gate_log, errors, match_dir, "A3")
    errors += validate_envelope(s3, "04_s3_tactical.json")

    s4 = f("05_s4_pricing.json")
    if s4 is None:
        return pending(gate_log, errors, match_dir, "A4")
    errors += validate_envelope(s4, "05_s4_pricing.json")
    errors += validate_probabilities(s4)

    # --- G3 : volatilité --------------------------------------------------
    s5 = f("06_s5_volatility.json")
    if s5 is None:
        return pending(gate_log, errors, match_dir, "G3")
    errors += validate_envelope(s5, "06_s5_volatility.json")
    if s5.get("status") in {"NO_BET", "WAIT_LINEUPS", "LIVE_ONLY"}:
        return blocked("G3", s5["status"], f"S5 status = {s5['status']} (propagé tel quel)")
    gate_log.append("G3 PASS")

    # --- G4 : marché ------------------------------------------------------
    s6 = f("07_s6_market.json")
    if s6 is None:
        return pending(gate_log, errors, match_dir, "G4")
    errors += validate_envelope(s6, "07_s6_market.json")
    p6 = s6.get("payload") or {}
    allowed = dig(p6, "markets_with_value", "allowed_markets_with_value",
                  "value_markets", default=[]) or []
    if s6.get("status") == "NO_BET" or not allowed:
        return blocked("G4", "NO_BET (market)", "Aucun marché autorisé avec value confirmée")
    gate_log.append(f"G4 PASS ({len(allowed)} marché(s) avec value)")

    # --- A7 : décision ----------------------------------------------------
    s7 = f("08_s7_decision.json")
    if s7 is None:
        return pending(gate_log, errors, match_dir, "A7")
    errors += validate_envelope(s7, "08_s7_decision.json")
    edge = recompute_edge(s7)
    if edge["mismatch"]:
        errors.append(
            f"08_s7_decision.json: edge déclaré {edge['declared_edge_pct']} vs recalculé "
            f"{edge['computed_edge_pct']} (écart {edge['delta']} > {EDGE_TOLERANCE_PTS} pt)")
    verdict = dig(s7.get("payload") or {}, "verdict", default=s7.get("status"))

    # --- G5 : conseil (conditionnel, uniquement si BET) -------------------
    if verdict == "BET":
        s8 = f("09_s8_council.json")
        if s8 is None:
            return pending(gate_log, errors, match_dir, "G5")
        errors += validate_envelope(s8, "09_s8_council.json")
        council = dig(s8.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT")
        if council == "VETO":
            return blocked("G5", "NO_BET (council)", "COUNCIL_VERDICT = VETO — override S7")
        gate_log.append(f"G5 PASS (council={council})")
    else:
        gate_log.append(f"G5 SKIP (S7 verdict={verdict}, A8 non déclenché)")

    # --- G6 / G7 : preflight ---------------------------------------------
    pre = f("10_preflight.json")
    if pre is None:
        return pending(gate_log, errors, match_dir, "A9")
    errors += validate_envelope(pre, "10_preflight.json")
    pre_flags = set(pre.get("flags") or [])
    if pre.get("status") == "NO_BET" or pre_flags & {
            "NULL_MARGIN", "ABERRANT_MARGIN", "BRIER_FAIL"}:
        return blocked("G6", "NO_BET (preflight)",
                       "Marge bookmaker nulle/aberrante ou Brier ≤ baseline naïve")
    gate_log.append("G6 PASS")

    if "EARLY_SEASON_SAMPLE" in pre_flags or dig(pre.get("payload") or {},
                                                 "early_season_insufficient",
                                                 default=False):
        gate_log.append("G7 RETROGRADE (échantillon début de saison insuffisant)")
        verdict = "INDICATIF" if verdict == "BET" else verdict
    else:
        gate_log.append("G7 PASS")

    return {"blocking_gate": None, "verdict": verdict, "reason": None,
            "gate_log": gate_log, "schema_errors": errors, "edge": edge,
            "next_step": None, "complete": True}


def pending(gate_log, errors, match_dir, label):
    gate_log.append(f"{label} PENDING (fichier amont absent)")
    return {"blocking_gate": None, "verdict": "PENDING", "reason": None,
            "gate_log": gate_log, "schema_errors": errors,
            "next_step": next_step(match_dir), "complete": False}


def next_step(match_dir: str):
    """Premier fichier manquant = prochain agent à lancer."""
    for agent, filename in STEPS:
        if read_json(os.path.join(match_dir, filename)) is None:
            return {"agent": agent, "expected_file": filename}
    return None


# --------------------------------------------------------------------------
# Commandes
# --------------------------------------------------------------------------

def cmd_init(args) -> int:
    cfg = load_input(args.input)
    matches = cfg.get("matches") or []
    if not matches:
        print("ERREUR: aucun match dans le bloc INPUT.", file=sys.stderr)
        return 2
    day = args.date or matches[0]["kickoff_utc"][:10]
    run_dir = os.path.join(ROOT, "runs", day)
    created = []
    for m in matches:
        mid = match_id_of(m["home"], m["away"], m["kickoff_utc"])
        mdir = os.path.join(run_dir, mid)
        os.makedirs(mdir, exist_ok=True)
        sources = os.path.join(mdir, "sources.md")
        if not os.path.exists(sources):
            with open(sources, "w", encoding="utf-8") as fh:
                fh.write(f"# Sources — {m['home']} vs {m['away']}\n\n"
                         f"| URL | retrieved_at_utc | agent |\n|---|---|---|\n")
        with open(os.path.join(mdir, "_match.json"), "w", encoding="utf-8") as fh:
            json.dump({**m, "match_id": mid}, fh, ensure_ascii=False, indent=2)
        created.append(mid)
    with open(os.path.join(run_dir, "_input.json"), "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)
    ensure_journal()
    print(json.dumps({"run_dir": os.path.relpath(run_dir, ROOT),
                      "match_ids": created}, ensure_ascii=False, indent=2))
    return 0


def cmd_check(args) -> int:
    result = evaluate_gates(args.match_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["schema_errors"] else 0


def ensure_journal() -> None:
    os.makedirs(os.path.dirname(JOURNAL), exist_ok=True)
    if not os.path.exists(JOURNAL):
        with open(JOURNAL, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(JOURNAL_COLUMNS)


def append_journal(rows: list[dict]) -> int:
    """Append-only : on n'ouvre jamais le journal autrement qu'en 'a'."""
    ensure_journal()
    with open(JOURNAL, encoding="utf-8") as fh:
        existing = {r["match_id"] for r in csv.DictReader(fh) if r.get("match_id")}
    new = [r for r in rows if r["match_id"] not in existing]
    with open(JOURNAL, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=JOURNAL_COLUMNS)
        for row in new:
            writer.writerow({c: row.get(c, "") for c in JOURNAL_COLUMNS})
    return len(new)


def collect(day: str) -> list[dict]:
    run_dir = os.path.join(ROOT, "runs", day)
    out = []
    for mid in sorted(os.listdir(run_dir)):
        mdir = os.path.join(run_dir, mid)
        if not os.path.isdir(mdir):
            continue
        meta = read_json(os.path.join(mdir, "_match.json")) or {}
        gates = evaluate_gates(mdir)
        routing = read_json(os.path.join(mdir, "00_routing.json")) or {}
        s1 = read_json(os.path.join(mdir, "02_s1_integrity.json")) or {}
        s5 = read_json(os.path.join(mdir, "06_s5_volatility.json")) or {}
        s7 = read_json(os.path.join(mdir, "08_s7_decision.json")) or {}
        s8 = read_json(os.path.join(mdir, "09_s8_council.json")) or {}
        p7 = s7.get("payload") or {}
        out.append({
            "match_id": mid,
            "meta": meta,
            "engine": dig(routing.get("payload") or {}, "engine_skill", default=""),
            "fallback_mode": bool(dig(routing.get("payload") or {}, "fallback_mode", default=False)),
            "drs": as_float(dig(s1.get("payload") or {}, "drs", "data_reliability_score")),
            "vs": as_float(dig(s5.get("payload") or {}, "vs", "volatility_score")),
            "council": dig(s8.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT"),
            "verdict": gates["verdict"],
            "blocking_gate": gates["blocking_gate"] or "",
            "reason": gates["reason"] or "",
            "gate_log": gates["gate_log"],
            "schema_errors": gates["schema_errors"],
            "decision": p7,
            "edge": recompute_edge(s7),
            "confidence": as_float(s7.get("confidence")) or 0.0,
        })
    return out


def apply_bankroll(rows: list[dict], cfg: dict) -> list[str]:
    """Cap par pari, exposition journalière, dédoublonnage des corrélés."""
    notes = []
    max_stake = as_float(cfg.get("max_stake_per_bet")) or 2.0
    max_daily = as_float(cfg.get("max_daily_exposure")) or 5.0
    bets = [r for r in rows if r["verdict"] == "BET"]

    # 1. Cap par pari.
    for r in bets:
        stake = as_float(r["decision"].get("stake_units")) or 0.0
        r["stake_final"] = min(stake, max_stake)
        if stake > max_stake:
            notes.append(f"{r['match_id']}: mise {stake}u plafonnée à {max_stake}u")

    # 2. Paris corrélés (même match, ou même équipe) → meilleur edge ajusté.
    seen: dict[str, dict] = {}
    for r in sorted(bets, key=lambda x: -(x["edge"]["computed_edge_pct"] or -999)):
        teams = {slug(r["meta"].get("home", "")), slug(r["meta"].get("away", ""))}
        clash = next((k for k, v in seen.items()
                      if v["match_id"] == r["match_id"] or (v["teams"] & teams)), None)
        if clash:
            r["stake_final"] = 0.0
            r["verdict"] = "NO_BET (corrélé)"
            notes.append(f"{r['match_id']}: écarté — corrélé à {seen[clash]['match_id']} (edge inférieur)")
        else:
            seen[r["match_id"]] = {"match_id": r["match_id"], "teams": teams}

    # 3. Exposition journalière : réduction proportionnelle, plus basses confiances d'abord.
    kept = [r for r in bets if r.get("stake_final", 0) > 0]
    total = sum(r["stake_final"] for r in kept)
    if total > max_daily:
        excess = total - max_daily
        for r in sorted(kept, key=lambda x: x["confidence"]):
            if excess <= 1e-9:
                break
            cut = min(r["stake_final"], excess)
            r["stake_final"] = round(r["stake_final"] - cut, 3)
            excess -= cut
            notes.append(f"{r['match_id']}: mise réduite de {cut}u (exposition > {max_daily}u)")
        notes.append(f"Exposition ramenée de {total:.2f}u à "
                     f"{sum(r['stake_final'] for r in kept):.2f}u")
    return notes


def fmt(value, digits=2):
    return "—" if value is None else f"{value:.{digits}f}"


def cmd_finalize(args) -> int:
    day = args.date
    run_dir = os.path.join(ROOT, "runs", day)
    cfg = read_json(os.path.join(run_dir, "_input.json")) or {}
    rows = collect(day)
    notes = apply_bankroll(rows, cfg)

    # --- SYNTHESE.md ------------------------------------------------------
    lines = [f"# APEX — Synthèse du {day}", "",
             "| Match | Moteur | DRS | VS | Marché | p_model | Fair | Cote | Edge % | Mise (u) | Verdict | Gate bloquante |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        d = r["decision"]
        m = r["meta"]
        label = f"{m.get('home','?')} – {m.get('away','?')}"
        engine = r["engine"] + (" (fallback)" if r["fallback_mode"] else "")
        lines.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            label, engine or "—", fmt(r["drs"], 1), fmt(r["vs"], 1),
            d.get("market", "—") or "—", fmt(as_float(d.get("p_model")), 3),
            fmt(as_float(d.get("fair_odds"))), fmt(as_float(d.get("market_odds"))),
            fmt(r["edge"]["computed_edge_pct"]), fmt(r.get("stake_final")),
            r["verdict"], r["blocking_gate"] or "—"))

    bets = [r for r in rows if r["verdict"] == "BET" and r.get("stake_final", 0) > 0]
    if bets:
        lines += ["", "## Conditions d'invalidation"]
        for r in bets:
            d = r["decision"]
            lines += [
                "", f"### {r['meta'].get('home')} – {r['meta'].get('away')} — "
                    f"{d.get('market')} / {d.get('selection')}",
                f"- Cote minimale acceptable : **{fmt(as_float(d.get('min_acceptable_odds')))}** "
                f"({d.get('bookmaker','?')})",
                f"- Mise retenue : **{fmt(r.get('stake_final'))}u**",
            ]
            for cond in d.get("invalidation_conditions") or []:
                lines.append(f"- {cond}")
    if notes:
        lines += ["", "## Contrôle de bankroll"] + [f"- {n}" for n in notes]

    errors = [e for r in rows for e in r["schema_errors"]]
    if errors:
        lines += ["", "## Anomalies de schéma"] + [f"- {e}" for e in errors]

    with open(os.path.join(run_dir, "SYNTHESE.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    # --- telegram.txt (≤ 12 lignes par pari) ------------------------------
    tg = []
    for r in bets:
        d = r["decision"]
        tg += [
            f"APEX {day} — {r['meta'].get('home')} vs {r['meta'].get('away')}",
            f"Competition: {r['meta'].get('competition','?')}",
            f"Marche: {d.get('market')} — {d.get('selection')}",
            f"Cote: {fmt(as_float(d.get('market_odds')))} ({d.get('bookmaker','?')})",
            f"Cote mini: {fmt(as_float(d.get('min_acceptable_odds')))}",
            f"p_model: {fmt(as_float(d.get('p_model')), 3)} | Fair: {fmt(as_float(d.get('fair_odds')))}",
            f"Edge: {fmt(r['edge']['computed_edge_pct'])}%",
            f"Mise: {fmt(r.get('stake_final'))}u",
            "Analyse probabiliste, aucun gain garanti.",
            "",
        ]
    if not bets:
        tg = [f"APEX {day} — 0 pari retenu.",
              "Analyse probabiliste, aucun gain garanti."]
    with open(os.path.join(run_dir, "telegram.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(tg).rstrip() + "\n")

    # --- synthese.json ----------------------------------------------------
    payload = {"date": day, "generated_at_utc": now_utc(), "bankroll": cfg,
               "bankroll_notes": notes, "matches": []}
    for r in rows:
        mdir = os.path.join(run_dir, r["match_id"])
        payload["matches"].append({
            "match_id": r["match_id"],
            "a7_envelope": read_json(os.path.join(mdir, "08_s7_decision.json")),
            "s8_verdict": r["council"],
            "preflight": read_json(os.path.join(mdir, "10_preflight.json")),
            "final_verdict": r["verdict"],
            "blocking_gate": r["blocking_gate"],
            "stake_final": r.get("stake_final", 0.0),
            "edge_recomputed_pct": r["edge"]["computed_edge_pct"],
            "gate_log": r["gate_log"],
        })
    with open(os.path.join(run_dir, "synthese.json"), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    # --- journal ----------------------------------------------------------
    journal_rows = []
    for r in rows:
        d = r["decision"]
        journal_rows.append({
            "date": day, "match_id": r["match_id"],
            "competition": r["meta"].get("competition", ""),
            "engine": r["engine"], "drs": r["drs"] if r["drs"] is not None else "",
            "vs": r["vs"] if r["vs"] is not None else "",
            "market": d.get("market", ""), "selection": d.get("selection", ""),
            "p_model": d.get("p_model", ""), "fair_odds": d.get("fair_odds", ""),
            "market_odds": d.get("market_odds", ""),
            "edge_pct": r["edge"]["computed_edge_pct"] if r["edge"]["computed_edge_pct"] is not None else "",
            "stake_units": r.get("stake_final", 0.0),
            "verdict": r["verdict"], "blocking_gate": r["blocking_gate"],
            "result": "", "pnl_units": "",
        })
    added = append_journal(journal_rows)

    counts = {}
    for r in rows:
        key = r["verdict"].split(" ")[0]
        counts[key] = counts.get(key, 0) + 1
    missing = sorted({m for r in rows for m in
                      (read_json(os.path.join(run_dir, r["match_id"], "02_s1_integrity.json")) or {}
                       ).get("missing_fields", []) or []})
    print(json.dumps({
        "matches_analysed": len(rows), "counts": counts,
        "total_exposure_units": round(sum(r.get("stake_final", 0.0) for r in bets), 3),
        "journal_rows_added": added, "missing_data": missing,
        "schema_errors": errors,
    }, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="APEX-LEAD — outillage de protocole")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Crée l'arborescence runs/<date>/<match_id>/")
    p_init.add_argument("--input", required=True)
    p_init.add_argument("--date")
    p_init.set_defaults(func=cmd_init)

    p_check = sub.add_parser("check", help="Évalue les gates G0-G7 pour un match")
    p_check.add_argument("--match-dir", required=True)
    p_check.set_defaults(func=cmd_check)

    p_fin = sub.add_parser("finalize", help="Bankroll + livrables + journal")
    p_fin.add_argument("--date", required=True)
    p_fin.set_defaults(func=cmd_finalize)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
