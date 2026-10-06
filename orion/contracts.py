"""Strict contracts. Agent statements are data, never execution instructions."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from typing import Any

ROLES = ("ORION-Core", "SENSOR", "MEMORY", "PATTERN", "FORECAST", "CONTEXT",
         "SKEPTIC", "SIMULATOR", "RED TEAM", "RISK", "DECISION", "AUDITOR",
         "META", "EXECUTOR", "LEARNER")
SCHEMA_VERSION = "orion-1"


def utc(value: str) -> dt.datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("A timezone-aware ISO timestamp is required")
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Naive timestamps are forbidden")
    return result.astimezone(dt.timezone.utc)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def finite_number(value: Any, name: str, low: float | None = None,
                  high: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise ValueError(f"INVALID_NUMBER: {name}")
    if (low is not None and value < low) or (high is not None and value > high):
        raise ValueError(f"INVALID_NUMBER: {name}")
    return float(value)


@dataclass(frozen=True)
class Finding:
    code: str
    message: str
    severity: str = "block"
    evidence_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Evidence:
    evidence_id: str
    fact_key: str
    value: Any
    statement: str
    source_uri: str
    origin_ids: tuple[str, ...]
    content: str
    content_hash: str
    available_at: str
    retrieved_at: str
    valid_until: str | None = None
    kind: str = "document"
    critical: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> "Evidence":
        required = ("evidence_id", "fact_key", "value", "statement", "source_uri",
                    "content", "available_at", "retrieved_at")
        missing = [key for key in required if key not in data]
        if missing:
            raise ValueError(f"Evidence missing fields: {', '.join(missing)}")
        for key in ("evidence_id", "fact_key", "statement", "source_uri", "available_at", "retrieved_at"):
            if not isinstance(data[key], str) or not data[key].strip():
                raise ValueError(f"Invalid evidence field {key}")
        if not isinstance(data["content"], str):
            raise ValueError("Evidence content must be text")
        origins = data.get("origin_ids", [])
        if not isinstance(origins, (list, tuple)) or any(not isinstance(x, str) or not x for x in origins):
            raise ValueError("origin_ids must be a list of known primary origins")
        if data.get("kind", "document") not in ("document", "observation", "rumor", "inference"):
            raise ValueError("Invalid evidence kind")
        if "critical" in data and not isinstance(data["critical"], bool):
            raise ValueError("critical must be boolean")
        utc(data["available_at"]); utc(data["retrieved_at"])
        if data.get("valid_until"):
            utc(data["valid_until"])
        actual_hash = hashlib.sha256(data["content"].encode("utf-8")).hexdigest()
        canonical(data["value"])
        return cls(**{key: data[key] for key in required},
                   origin_ids=tuple(origins), content_hash=data.get("content_hash", actual_hash),
                   valid_until=data.get("valid_until"), kind=data.get("kind", "document"),
                   critical=data.get("critical", True))


@dataclass(frozen=True)
class Claim:
    claim_id: str
    statement: str
    evidence_ids: tuple[str, ...]
    kind: str = "fact"
    critical: bool = True
    psychology: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "Claim":
        if not data.get("claim_id") or not isinstance(data.get("statement"), str):
            raise ValueError("Claim requires claim_id and statement")
        if data.get("kind", "fact") not in ("fact", "inference", "assumption", "unknown"):
            raise ValueError("Invalid claim kind")
        ids = data.get("evidence_ids", [])
        if not isinstance(ids, (list, tuple)) or any(not isinstance(x, str) for x in ids):
            raise ValueError("Invalid evidence references")
        for flag in ("critical", "psychology"):
            if flag in data and not isinstance(data[flag], bool):
                raise ValueError(f"{flag} must be boolean")
        return cls(data["claim_id"], data["statement"], tuple(ids), data.get("kind", "fact"),
                   data.get("critical", True), data.get("psychology", False))


@dataclass(frozen=True)
class Forecast:
    event_id: str
    event: str
    probability: float
    method: str
    method_version: str
    validation_status: str = "unvalidated"
    validation_reference: str | None = None
    odds: float | None = None
    odds_at: str | None = None
    kickoff: str | None = None
    sensitivity_probabilities: tuple[float, ...] = ()
    settlement: str = "binary"
    synthetic: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "Forecast":
        for key in ("event_id", "event", "method", "method_version"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                raise ValueError(f"Forecast requires {key}")
        p = finite_number(data.get("probability"), "probability", 0, 1)
        odds = None if data.get("odds") is None else finite_number(data["odds"], "odds", 1.000000001)
        sensitivity = data.get("sensitivity_probabilities", [])
        if not isinstance(sensitivity, (list, tuple)):
            raise ValueError("Invalid sensitivity probabilities")
        sensitivity = tuple(finite_number(x, "sensitivity probability", 0, 1) for x in sensitivity)
        status = data.get("validation_status", "unvalidated")
        if status not in ("validated", "unvalidated", "unknown", "quarantined"):
            raise ValueError("Invalid validation status")
        if "synthetic" in data and not isinstance(data["synthetic"], bool):
            raise ValueError("synthetic must be boolean")
        for key in ("odds_at", "kickoff"):
            if data.get(key):
                utc(data[key])
        return cls(data["event_id"], data["event"], p, data["method"], data["method_version"],
                   status, data.get("validation_reference"), odds, data.get("odds_at"),
                   data.get("kickoff"), sensitivity, data.get("settlement", "binary"),
                   data.get("synthetic", False))


@dataclass(frozen=True)
class Mission:
    task_id: str
    objective: str
    as_of: str
    evidence: tuple[Evidence, ...] = ()
    claims: tuple[Claim, ...] = ()
    forecast: Forecast | None = None
    requirements: dict[str, int] = field(default_factory=dict)
    mode: str = "auto"
    proposed_action: str = "report"
    resume_condition: str | None = None
    metadata: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "Mission":
        for key in ("task_id", "objective", "as_of"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                raise ValueError(f"Mission requires {key}")
        utc(data["as_of"])
        if data.get("mode", "auto") not in ("auto", "reflex", "analysis", "deep"):
            raise ValueError("Unknown analysis mode")
        for key in ("evidence", "claims"):
            if not isinstance(data.get(key, []), (list, tuple)):
                raise ValueError(f"{key} must be a list")
        requirements = data.get("requirements", {})
        if not isinstance(requirements, dict) or any(not isinstance(k, str) or not k or isinstance(v, bool)
               or not isinstance(v, int) or v < 1 for k, v in requirements.items()):
            raise ValueError("requirements maps fact keys to positive minimum-origin counts")
        if not isinstance(data.get("metadata", {}), dict):
            raise ValueError("metadata must be an object")
        evidence = tuple(Evidence.from_dict(x) for x in data.get("evidence", []))
        claims = tuple(Claim.from_dict(x) for x in data.get("claims", []))
        if len({x.evidence_id for x in evidence}) != len(evidence):
            raise ValueError("Duplicate evidence_id")
        if len({x.claim_id for x in claims}) != len(claims):
            raise ValueError("Duplicate claim_id")
        result = cls(data["task_id"], data["objective"], data["as_of"], evidence, claims,
                     Forecast.from_dict(data["forecast"]) if data.get("forecast") else None,
                     dict(requirements), data.get("mode", "auto"), data.get("proposed_action", "report"),
                     data.get("resume_condition"), dict(data.get("metadata", {})))
        canonical(result.to_dict())
        return result

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class AgentResult:
    role: str
    status: str = "complete"
    findings: tuple[Finding, ...] = ()
    metrics: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)
