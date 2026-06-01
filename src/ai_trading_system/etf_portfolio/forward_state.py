from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION = "etf_shadow_candidate_registry_v2"
LEGACY_SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION = "etf_shadow_candidate_registry_v1"
DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH = (
    PROJECT_ROOT / "data" / "simulation" / "etf_shadow_candidates.json"
)

OPEN_SHADOW_STATUSES = frozenset(
    {"active", "needs_more_data", "watch", "reject_pending_review"}
)
TERMINAL_SHADOW_STATUSES = frozenset({"rejected", "archived"})
ALLOWED_SHADOW_STATUSES = OPEN_SHADOW_STATUSES | TERMINAL_SHADOW_STATUSES
LEGACY_STATUS_MAP = {
    "active_shadow_observation": "active",
    "continue_shadow": "active",
    "reject_candidate": "reject_pending_review",
    "promote_to_longer_observation": "active",
}
PROHIBITED_SHADOW_STATUSES = frozenset({"production", "live", "broker_enabled"})
REQUIRED_CANDIDATE_FIELDS = (
    "shadow_id",
    "candidate_id",
    "experiment_id",
    "source_run_id",
    "source_pack_id",
    "enrolled_at",
    "enrollment_date",
    "model_version",
    "config_hash",
    "data_hash",
    "ranking_score",
    "ranking_summary",
    "selection_gate_status",
    "status",
    "observe_only",
    "production_effect",
    "broker_action",
    "manual_review_required",
    "evaluation_schedule",
    "last_evaluated_at",
    "last_evaluated_date",
    "notes",
)


def empty_shadow_candidate_registry(updated_at: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_shadow_candidates",
        "updated_at": updated_at,
        "candidate_count": 0,
        "candidates": [],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def load_shadow_candidate_registry(
    path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    *,
    validate: bool = True,
) -> dict[str, Any]:
    if not path.exists():
        return empty_shadow_candidate_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"ETF shadow candidate registry must be a JSON object: {path}")
    normalized = normalize_shadow_candidate_registry(payload)
    if validate:
        validate_shadow_candidate_registry(normalized)
    return normalized


def write_shadow_candidate_registry(
    payload: Mapping[str, Any],
    path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
) -> dict[str, Any]:
    normalized = normalize_shadow_candidate_registry(payload)
    validate_shadow_candidate_registry(normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return normalized


def normalize_shadow_candidate_registry(payload: Mapping[str, Any]) -> dict[str, Any]:
    _reject_explicit_top_level_unsafe_values(payload)
    candidates = payload.get("candidates")
    if candidates is None:
        candidates = []
    if not isinstance(candidates, list):
        raise ValueError("ETF shadow candidate registry missing candidates list")
    normalized_candidates = [
        normalize_shadow_candidate_record(candidate)
        for candidate in candidates
        if isinstance(candidate, Mapping)
    ]
    normalized_candidates = sorted(
        normalized_candidates,
        key=lambda item: (str(item.get("shadow_id")), str(item.get("candidate_id"))),
    )
    updated_at = payload.get("updated_at")
    return {
        "schema_version": SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_shadow_candidates",
        "updated_at": None if updated_at is None else str(updated_at),
        "candidate_count": len(normalized_candidates),
        "candidates": normalized_candidates,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def normalize_shadow_candidate_record(candidate: Mapping[str, Any]) -> dict[str, Any]:
    status = _canonical_status(candidate.get("status"))
    enrolled_at = _string(candidate.get("enrolled_at"))
    enrollment_date = _string(candidate.get("enrollment_date")) or _date_from_timestamp(enrolled_at)
    start_date = _string(candidate.get("start_date")) or enrollment_date
    notes = _string_list(candidate.get("notes"))
    if candidate.get("status") in LEGACY_STATUS_MAP:
        notes = [*notes, f"legacy_status_migrated:{candidate.get('status')}->{status}"]
    source_pack_id = _string(candidate.get("source_pack_id")) or _string(
        candidate.get("source_pack")
    )
    if not source_pack_id:
        source_pack_id = "unknown_pack"
        notes = [*notes, "source_pack_id_missing_in_legacy_state"]
    data_hash = _string(candidate.get("data_hash"))
    if not data_hash:
        data_hash = "legacy_unknown_data_hash"
        notes = [*notes, "data_hash_missing_in_legacy_state"]
    ranking_summary = candidate.get("ranking_summary")
    if not isinstance(ranking_summary, Mapping):
        ranking_summary = {
            "ranking_reason": _string_list(candidate.get("ranking_reason")),
            "selection_status": _string(candidate.get("selection_status")),
        }
    evaluation_schedule = candidate.get("evaluation_schedule")
    if not isinstance(evaluation_schedule, Mapping):
        evaluation_schedule = {}
    schedule = {
        "cadence": _string(evaluation_schedule.get("cadence"), "daily"),
        "start_date": _string(evaluation_schedule.get("start_date")) or start_date,
        "weekly_review_task": _string(
            evaluation_schedule.get("weekly_review_task"),
            "TRADING-065F",
        ),
    }
    return {
        "shadow_id": _string(candidate.get("shadow_id")),
        "candidate_id": _string(candidate.get("candidate_id")),
        "experiment_id": _string(candidate.get("experiment_id")),
        "source_run_id": _string(candidate.get("source_run_id")),
        "source_pack_id": source_pack_id,
        "enrolled_at": enrolled_at,
        "enrollment_date": enrollment_date,
        "model_version": _string(candidate.get("model_version")),
        "config_hash": _string(candidate.get("config_hash")),
        "data_hash": data_hash,
        "ranking_score": _optional_float(
            candidate.get("ranking_score", candidate.get("candidate_score"))
        ),
        "ranking_summary": dict(ranking_summary),
        "selection_gate_status": _string(
            candidate.get("selection_gate_status"),
            _string(candidate.get("selection_status")),
        ),
        "status": status,
        "observe_only": candidate.get("observe_only") is not False,
        "production_effect": _string(candidate.get("production_effect"), "none"),
        "broker_action": _string(candidate.get("broker_action"), "none"),
        "manual_review_required": candidate.get("manual_review_required") is not False,
        "production_promotion_allowed": candidate.get("production_promotion_allowed") is True,
        "evaluation_schedule": schedule,
        "last_evaluated_at": _none_or_string(candidate.get("last_evaluated_at")),
        "last_evaluated_date": _none_or_string(candidate.get("last_evaluated_date")),
        "notes": notes,
    }


def validate_shadow_candidate_registry(payload: Mapping[str, Any]) -> None:
    _validate_top_level_safety(payload)
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("ETF shadow candidate registry candidates must be a list")
    shadow_ids: set[str] = set()
    active_candidate_ids: set[str] = set()
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, Mapping):
            raise ValueError(f"ETF shadow candidate at index {index} must be a mapping")
        validate_shadow_candidate_record(candidate)
        shadow_id = str(candidate["shadow_id"])
        if shadow_id in shadow_ids:
            raise ValueError(f"ETF shadow candidate duplicate shadow_id: {shadow_id}")
        shadow_ids.add(shadow_id)
        status = str(candidate["status"])
        candidate_id = str(candidate["candidate_id"])
        if status in OPEN_SHADOW_STATUSES:
            if candidate_id in active_candidate_ids:
                raise ValueError(
                    "ETF shadow candidate duplicate active candidate_id: "
                    f"{candidate_id}"
                )
            active_candidate_ids.add(candidate_id)
    expected_count = len(candidates)
    if int(payload.get("candidate_count") or 0) != expected_count:
        raise ValueError("ETF shadow candidate registry candidate_count mismatch")


def validate_shadow_candidate_record(candidate: Mapping[str, Any]) -> None:
    missing = [field for field in REQUIRED_CANDIDATE_FIELDS if field not in candidate]
    if missing:
        raise ValueError(
            "ETF shadow candidate missing required field(s): " + ", ".join(missing)
        )
    for field in (
        "shadow_id",
        "candidate_id",
        "experiment_id",
        "source_run_id",
        "source_pack_id",
        "enrolled_at",
        "enrollment_date",
        "model_version",
        "config_hash",
        "data_hash",
        "selection_gate_status",
    ):
        if not _string(candidate.get(field)):
            raise ValueError(f"ETF shadow candidate field must be non-empty: {field}")
    _validate_date_text(candidate.get("enrollment_date"), "enrollment_date")
    if candidate.get("last_evaluated_date") is not None:
        _validate_date_text(candidate.get("last_evaluated_date"), "last_evaluated_date")
    status = _string(candidate.get("status"))
    if status in PROHIBITED_SHADOW_STATUSES:
        raise ValueError(f"ETF shadow candidate prohibited status: {status}")
    if status not in ALLOWED_SHADOW_STATUSES:
        raise ValueError(f"ETF shadow candidate invalid status: {status}")
    if candidate.get("observe_only") is not True:
        raise ValueError("ETF shadow candidate must keep observe_only=true")
    if candidate.get("production_effect") != "none":
        raise ValueError("ETF shadow candidate must keep production_effect=none")
    if candidate.get("broker_action") != "none":
        raise ValueError("ETF shadow candidate must keep broker_action=none")
    if candidate.get("manual_review_required") is not True:
        raise ValueError("ETF shadow candidate must keep manual_review_required=true")
    if candidate.get("production_promotion_allowed") is not False:
        raise ValueError(
            "ETF shadow candidate must keep production_promotion_allowed=false"
        )
    if not isinstance(candidate.get("ranking_summary"), Mapping):
        raise ValueError("ETF shadow candidate ranking_summary must be a mapping")
    if not isinstance(candidate.get("evaluation_schedule"), Mapping):
        raise ValueError("ETF shadow candidate evaluation_schedule must be a mapping")
    if not isinstance(candidate.get("notes"), list):
        raise ValueError("ETF shadow candidate notes must be a list")


def update_shadow_candidate_evaluation_state(
    registry: Mapping[str, Any],
    *,
    updates: Mapping[str, Mapping[str, Any]],
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    timestamp = (updated_at or datetime.now(tz=UTC)).isoformat()
    normalized = normalize_shadow_candidate_registry(registry)
    candidates: list[dict[str, Any]] = []
    for candidate in normalized["candidates"]:
        shadow_id = str(candidate.get("shadow_id"))
        update = updates.get(shadow_id, {})
        if update:
            candidate = {
                **candidate,
                "status": _canonical_status(update.get("status", candidate.get("status"))),
                "last_evaluated_at": _none_or_string(
                    update.get("last_evaluated_at", timestamp)
                ),
                "last_evaluated_date": _none_or_string(update.get("last_evaluated_date")),
                "notes": _string_list(update.get("notes", candidate.get("notes"))),
            }
        candidates.append(candidate)
    return normalize_shadow_candidate_registry(
        {
            **normalized,
            "updated_at": timestamp,
            "candidates": candidates,
        }
    )


def active_shadow_candidates(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    normalized = normalize_shadow_candidate_registry(registry)
    return [
        dict(candidate)
        for candidate in normalized.get("candidates", [])
        if str(candidate.get("status")) in OPEN_SHADOW_STATUSES
    ]


def _validate_top_level_safety(payload: Mapping[str, Any]) -> None:
    if payload.get("observe_only") is not True:
        raise ValueError("ETF shadow candidate registry must keep observe_only=true")
    if payload.get("production_effect") != "none":
        raise ValueError("ETF shadow candidate registry must keep production_effect=none")
    if payload.get("broker_action") != "none":
        raise ValueError("ETF shadow candidate registry must keep broker_action=none")
    if payload.get("manual_review_required") is not True:
        raise ValueError(
            "ETF shadow candidate registry must keep manual_review_required=true"
        )
    if payload.get("production_promotion_allowed") is not False:
        raise ValueError(
            "ETF shadow candidate registry must keep production_promotion_allowed=false"
        )


def _reject_explicit_top_level_unsafe_values(payload: Mapping[str, Any]) -> None:
    if "observe_only" in payload and payload.get("observe_only") is not True:
        raise ValueError("ETF shadow candidate registry must keep observe_only=true")
    if "production_effect" in payload and payload.get("production_effect") != "none":
        raise ValueError("ETF shadow candidate registry must keep production_effect=none")
    if "broker_action" in payload and payload.get("broker_action") != "none":
        raise ValueError("ETF shadow candidate registry must keep broker_action=none")
    if "manual_review_required" in payload and payload.get("manual_review_required") is not True:
        raise ValueError(
            "ETF shadow candidate registry must keep manual_review_required=true"
        )
    if (
        "production_promotion_allowed" in payload
        and payload.get("production_promotion_allowed") is not False
    ):
        raise ValueError(
            "ETF shadow candidate registry must keep production_promotion_allowed=false"
        )


def _canonical_status(value: object) -> str:
    status = _string(value, "active")
    return LEGACY_STATUS_MAP.get(status, status)


def _validate_date_text(value: object, field: str) -> None:
    try:
        date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"ETF shadow candidate invalid date field {field}: {value}") from exc


def _date_from_timestamp(value: str) -> str:
    if not value:
        return datetime.now(tz=UTC).date().isoformat()
    try:
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError:
        return str(value)[:10]


def _string(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _none_or_string(value: object) -> str | None:
    text = _string(value)
    return text or None


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [_string(item) for item in value if _string(item)]
    text = _string(value)
    return [] if not text else [text]


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
