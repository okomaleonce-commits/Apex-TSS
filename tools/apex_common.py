#!/usr/bin/env python3
"""Primitives partagées par les chaînes APEX football et APEX-HOCKEY.

Rien de sportif ici : uniquement l'identité des matchs, les entrées/sorties
JSON et la validation de l'enveloppe commune. Les deux chaînes restent
volontairement séparées pour tout le reste — le pricing football
(Dixon-Coles, taux de nuls, base de buts) ne se transpose pas au hockey.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import datetime, timezone

VALID_STATUS = {
    "OK", "ABORT", "NO_BET", "WAIT_LINEUPS", "WAIT_GOALIE", "LIVE_ONLY",
    "DATA_REQUEST", "UPSTREAM_MISSING", "EMPTY", "AMBIGUOUS",
}

ENVELOPE_KEYS = [
    "match_id", "agent", "status", "flags", "confidence", "payload",
    "missing_fields", "sources", "generated_at_utc",
]


def slug(text: str) -> str:
    """Snake_case ASCII : 'Montréal Canadiens' -> 'montreal_canadiens'."""
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace("ø", "o").replace("Ø", "O").replace("ß", "ss")
    text = text.replace("æ", "ae").replace("Æ", "AE").replace("ð", "d")
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()


def match_id_of(home: str, away: str, start_utc: str) -> str:
    return f"{slug(home)}_{slug(away)}_{(start_utc or '')[:10].replace('-', '')}"


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        text = fh.read().strip()
    return json.loads(text) if text else None


def write_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def load_input(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        raw = fh.read()
    if path.endswith((".yaml", ".yml")):
        import yaml
        return yaml.safe_load(raw)
    return json.loads(raw)


def dig(payload, *keys, default=None):
    """Première clé présente et non nulle, sinon `default`."""
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


def validate_envelope(obj, filename: str, required=None) -> list[str]:
    """Règles anti-hallucination 1 et 3 : toute donnée chiffrée est sourcée."""
    errors = []
    if not isinstance(obj, dict):
        return [f"{filename}: racine JSON non-objet"]
    for key in (required or ENVELOPE_KEYS):
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


def recompute_edge(p_model, market_odds, declared=None, tolerance=0.1) -> dict:
    """edge_pct = (p_model × market_odds − 1) × 100, calculé, jamais de tête."""
    p_model, market_odds = as_float(p_model), as_float(market_odds)
    declared = as_float(declared)
    out = {"p_model": p_model, "market_odds": market_odds,
           "declared_edge_pct": declared, "computed_edge_pct": None,
           "delta": None, "mismatch": False}
    if p_model is None or market_odds is None:
        return out
    computed = (p_model * market_odds - 1.0) * 100.0
    out["computed_edge_pct"] = round(computed, 4)
    if declared is not None:
        out["delta"] = round(abs(computed - declared), 4)
        out["mismatch"] = out["delta"] > tolerance
    return out
