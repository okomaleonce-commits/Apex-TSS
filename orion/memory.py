"""Append-only, point-in-time memory and explicitly settled binary forecasts.

Recording an old ``as_of`` does not turn a retrospective forecast into a
prospective one: learning also checks when the decision was actually saved.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .contracts import canonical, digest, finite_number, utc

MEMORY_CATEGORIES = ("working", "episodic", "semantic", "failure")


def _stamp(value: str | dt.datetime) -> str:
    moment = utc(value) if isinstance(value, str) else value
    if not isinstance(moment, dt.datetime) or moment.tzinfo is None:
        raise ValueError("A timezone-aware timestamp is required")
    return moment.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _now() -> str:
    return _stamp(dt.datetime.now(dt.timezone.utc))


class MemoryStore:
    """One locked SQLite connection; immutable records and explicit outcomes."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path) if str(path) == ":memory:" else str(Path(path).expanduser())
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA busy_timeout = 5000")
        if self.path != ":memory:":
            self._db.execute("PRAGMA journal_mode = WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                run_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                as_of TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                mission_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS decisions (
                run_id TEXT PRIMARY KEY REFERENCES snapshots(run_id),
                recorded_at TEXT NOT NULL,
                result_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outcomes (
                run_id TEXT PRIMARY KEY REFERENCES decisions(run_id),
                available_at TEXT NOT NULL,
                occurred_at TEXT,
                recorded_at TEXT NOT NULL,
                value INTEGER NOT NULL CHECK (value IN (0, 1)),
                outcome_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS memory_entries (
                entry_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES snapshots(run_id),
                task_id TEXT NOT NULL,
                category TEXT NOT NULL CHECK (category IN ('working','episodic','semantic','failure')),
                available_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS memory_available ON memory_entries(available_at, task_id);
        """)
        for table in ("snapshots", "decisions", "outcomes", "memory_entries"):
            for operation in ("UPDATE", "DELETE"):
                self._db.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} "
                    f"BEFORE {operation} ON {table} BEGIN "
                    "SELECT RAISE(ABORT, 'ORION memory is append-only'); END"
                )
        self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def __enter__(self) -> "MemoryStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _entry(self, run_id: str, task_id: str, category: str, available_at: str, payload: dict) -> None:
        entry_id = digest({"run_id": run_id, "category": category, "payload": payload})
        self._db.execute(
            "INSERT INTO memory_entries VALUES (?, ?, ?, ?, ?, ?)",
            (entry_id, run_id, task_id, category, available_at, canonical(payload)),
        )

    def save_run(self, run_id: str, mission_payload: dict, result: dict) -> dict:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id is required")
        if not isinstance(mission_payload, dict) or not isinstance(result, dict):
            raise ValueError("Mission and result must be JSON objects")
        task_id = mission_payload.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("Mission task_id is required")
        as_of = _stamp(mission_payload.get("as_of"))
        if result.get("run_id", run_id) != run_id or result.get("task_id", task_id) != task_id:
            raise ValueError("Decision identity does not match the snapshot")
        mission_json, result_json = canonical(mission_payload), canonical(result)
        recorded_at = _now()
        available_at = max(as_of, recorded_at)
        with self._lock, self._db:
            existing = self._db.execute(
                "SELECT s.mission_json, d.result_json FROM snapshots s "
                "JOIN decisions d USING(run_id) WHERE run_id = ?", (run_id,),
            ).fetchone()
            if existing:
                if existing["mission_json"] != mission_json or existing["result_json"] != result_json:
                    raise ValueError("Conflicting data for an existing run_id")
                return {"run_id": run_id, "created": False}
            self._db.execute("INSERT INTO snapshots VALUES (?, ?, ?, ?, ?)",
                             (run_id, task_id, as_of, recorded_at, mission_json))
            self._db.execute("INSERT INTO decisions VALUES (?, ?, ?)",
                             (run_id, recorded_at, result_json))
            self._entry(run_id, task_id, "working", available_at, {"mission": mission_payload})
            self._entry(run_id, task_id, "episodic", available_at,
                        {"verdict": result.get("verdict"), "reason_codes": result.get("reason_codes", [])})
            forecast = mission_payload.get("forecast")
            if isinstance(forecast, dict):
                self._entry(run_id, task_id, "semantic", available_at, {
                    "kind": "model_metadata", "method": forecast.get("method"),
                    "method_version": forecast.get("method_version"),
                    "validation_status": forecast.get("validation_status", "unknown"),
                    "validation_reference": forecast.get("validation_reference"),
                    "automatic_promotion": False,
                })
            blockers = [item for item in result.get("findings", [])
                        if isinstance(item, dict) and item.get("severity") == "block"]
            if blockers:
                self._entry(run_id, task_id, "failure", available_at,
                            {"kind": "process_or_input_block", "findings": blockers})
        return {"run_id": run_id, "created": True}

    def load_run(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT s.*, d.result_json FROM snapshots s JOIN decisions d USING(run_id) "
                "WHERE run_id = ?", (run_id,),
            ).fetchone()
            if not row:
                return None
            outcome = self._db.execute("SELECT * FROM outcomes WHERE run_id = ?", (run_id,)).fetchone()
        return {
            "run_id": row["run_id"], "task_id": row["task_id"], "as_of": row["as_of"],
            "recorded_at": row["recorded_at"], "mission": json.loads(row["mission_json"]),
            "result": json.loads(row["result_json"]), "outcome": self._outcome(outcome) if outcome else None,
        }

    def recall(self, as_of: str, task_id: str | None = None, limit: int = 20) -> list[dict]:
        cutoff = _stamp(as_of)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("Memory recall limit must be an integer from 1 to 1000")
        query = "SELECT * FROM memory_entries WHERE available_at <= ?"
        params: list[Any] = [cutoff]
        if task_id is not None:
            query += " AND task_id = ?"
            params.append(task_id)
        query += " ORDER BY available_at DESC, entry_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._db.execute(query, params).fetchall()
        return [{"entry_id": row["entry_id"], "run_id": row["run_id"], "task_id": row["task_id"],
                 "category": row["category"], "available_at": row["available_at"],
                 "payload": json.loads(row["payload_json"])} for row in rows]

    @staticmethod
    def _outcome(row: sqlite3.Row) -> dict:
        return {"run_id": row["run_id"], "value": row["value"], "available_at": row["available_at"],
                "occurred_at": row["occurred_at"], "recorded_at": row["recorded_at"],
                "outcome": json.loads(row["outcome_json"])}

    def settle(self, run_id: str, outcome: bool | int | dict, available_at: str,
               now: str | dt.datetime | None = None) -> dict:
        if isinstance(outcome, dict):
            payload = json.loads(canonical(outcome))
            value = payload.get("value")
        else:
            value = outcome
            payload = {"value": outcome}
        if not isinstance(value, (bool, int)) or value not in (0, 1):
            raise ValueError("Binary outcome must be bool, 0 or 1")
        value = int(value)
        payload["value"] = value
        available = _stamp(available_at)
        observed_now = _stamp(now) if now is not None else _now()
        if available > observed_now:
            raise ValueError("Cannot settle a result that is not yet available")
        occurred = _stamp(payload["occurred_at"]) if payload.get("occurred_at") else None
        if occurred and occurred > available:
            raise ValueError("Outcome occurrence cannot be after its availability")
        recorded_at = _now()
        with self._lock, self._db:
            run = self._db.execute("SELECT * FROM snapshots WHERE run_id = ?", (run_id,)).fetchone()
            if not run:
                raise ValueError("Unknown run_id")
            if available <= run["as_of"]:
                raise ValueError("Outcome must become available after the forecast snapshot")
            existing = self._db.execute("SELECT * FROM outcomes WHERE run_id = ?", (run_id,)).fetchone()
            if existing:
                if (existing["value"] != value or existing["available_at"] != available
                        or existing["occurred_at"] != occurred or existing["outcome_json"] != canonical(payload)):
                    raise ValueError("Conflicting outcome for an already settled run")
                return self._outcome(existing)
            self._db.execute("INSERT INTO outcomes VALUES (?, ?, ?, ?, ?, ?)",
                             (run_id, available, occurred, recorded_at, value, canonical(payload)))
            mission = json.loads(run["mission_json"])
            forecast = mission.get("forecast")
            self._entry(run_id, run["task_id"], "episodic", max(recorded_at, available),
                        {"kind": "outcome", "value": value, "available_at": available, "occurred_at": occurred})
            if isinstance(forecast, dict):
                p = finite_number(forecast.get("probability"), "probability", 0, 1)
                if (p > 0.5 and value == 0) or (p < 0.5 and value == 1):
                    self._entry(run_id, run["task_id"], "failure", max(recorded_at, available), {
                        "kind": "unexpected_binary_outcome", "probability": p, "value": value,
                        "brier": (p - value) ** 2, "automatic_correction": False,
                    })
            row = self._db.execute("SELECT * FROM outcomes WHERE run_id = ?", (run_id,)).fetchone()
        return self._outcome(row)

    def learning_report(self, as_of: str) -> dict:
        cutoff = _stamp(as_of)
        with self._lock:
            rows = self._db.execute(
                "SELECT s.run_id, s.as_of, s.mission_json, d.recorded_at AS decision_recorded_at, "
                "o.value, o.available_at, o.occurred_at, o.recorded_at AS outcome_recorded_at "
                "FROM snapshots s JOIN decisions d USING(run_id) JOIN outcomes o USING(run_id)",
            ).fetchall()
        excluded: list[dict] = []
        excluded_counts: dict[str, int] = {}
        samples: list[dict] = []
        epsilon = 1e-15
        for row in rows:
            reason = None
            forecast = json.loads(row["mission_json"]).get("forecast")
            if (row["available_at"] > cutoff or row["outcome_recorded_at"] > cutoff
                    or row["decision_recorded_at"] > cutoff):
                reason = "not_known_at_as_of"
            elif not isinstance(forecast, dict):
                reason = "no_forecast"
            elif forecast.get("settlement", "binary") != "binary":
                reason = "non_binary_forecast"
            elif forecast.get("synthetic", False):
                reason = "synthetic_forecast"
            elif forecast.get("kickoff") and (row["as_of"] >= _stamp(forecast["kickoff"])
                                               or row["decision_recorded_at"] >= _stamp(forecast["kickoff"])):
                reason = "prematch_cutoff_passed"
            elif (row["decision_recorded_at"] >= row["available_at"]
                  or (row["occurred_at"] is not None
                      and (row["decision_recorded_at"] >= row["occurred_at"] or row["as_of"] >= row["occurred_at"]))):
                reason = "retrospective_forecast"
            if reason:
                excluded.append({"run_id": row["run_id"], "reason": reason})
                excluded_counts[reason] = excluded_counts.get(reason, 0) + 1
                continue
            try:
                p = finite_number(forecast.get("probability"), "probability", 0, 1)
            except ValueError:
                excluded.append({"run_id": row["run_id"], "reason": "invalid_probability"})
                excluded_counts["invalid_probability"] = excluded_counts.get("invalid_probability", 0) + 1
                continue
            value = row["value"]
            log_loss = -math.log(max(epsilon, p if value else 1 - p))
            samples.append({"run_id": row["run_id"], "event_id": forecast.get("event_id"),
                            "method": forecast.get("method"), "method_version": forecast.get("method_version"),
                            "validation_status": forecast.get("validation_status", "unknown"),
                            "probability": p, "value": value, "brier": (p - value) ** 2,
                            "log_loss": log_loss})
        groups: dict[str, list[dict]] = {}
        for sample in samples:
            key = f"{sample['method']}:{sample['method_version']}"
            groups.setdefault(key, []).append(sample)

        def summary(items: list[dict]) -> dict:
            return {"n_scored": len(items), "brier_score": sum(x["brier"] for x in items) / len(items) if items else None,
                    "log_loss": sum(x["log_loss"] for x in items) / len(items) if items else None}

        return {"as_of": cutoff, **summary(samples), "by_model": {key: summary(items) for key, items in groups.items()},
                "excluded": excluded, "excluded_counts": excluded_counts, "observations": samples,
                "log_loss_probability_floor": epsilon,
                "automatic_weights_updated": False, "automatic_model_promotion": False,
                "outcome_verification": "supplied_outcomes_not_independently_verified"}
