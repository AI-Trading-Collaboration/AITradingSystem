from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    FALSE_SAFETY_FIELDS,
    FORBIDDEN_EMIT_FIELDS,
    FORBIDDEN_TRUE_FIELDS,
    REAL_SCHEDULER_FIELDS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
)
from ai_trading_system.post_2085_research_common import mapping

MUST_NOT_ACTIONS = [
    "enable_scheduler",
    "create_cron_job",
    "create_windows_task",
    "create_github_actions_schedule",
    "execute_real_manual_run",
    "append_historical_event_log",
    "mutate_event_log",
    "bind_outcome",
    "mutate_outcome_store",
    "enable_paper_shadow",
    "create_paper_trade",
    "create_shadow_position",
    "enable_production",
    "call_broker_api",
    "send_order",
    "read_fresh_market_data",
    "generate_new_signal",
    "run_backtest",
    "generate_daily_report",
]


class HighIntensityGuardrailClosureError(ValueError):
    pass


def load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityGuardrailClosureError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = read_json_object(path)
    return payloads


def read_json_object(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityGuardrailClosureError(f"{path}: expected JSON object")
    return payload


def string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def markdown_table_from_mapping(payload: object) -> list[str]:
    values = mapping(payload)
    lines = ["|Field|Value|", "|---|---|"]
    for key, value in values.items():
        lines.append(f"|`{key}`|`{value}`|")
    return lines


def validate_generated_payloads(
    payloads: Mapping[str, Mapping[str, Any]],
    label: str,
) -> None:
    for key, payload in payloads.items():
        validate_safety_payload(f"{label} generated {key}", payload)


def validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    unsafe = collect_unsafe_fields(
        payload,
        false_fields=FALSE_SAFETY_FIELDS,
        forbidden_emit_fields=FORBIDDEN_EMIT_FIELDS,
    )
    if unsafe:
        raise HighIntensityGuardrailClosureError(
            f"{label} has unsafe fields: {sorted(set(unsafe))}"
        )
    scheduler = collect_real_scheduler_creation_fields(
        payload,
        real_scheduler_fields=REAL_SCHEDULER_FIELDS,
    )
    if scheduler:
        raise HighIntensityGuardrailClosureError(
            f"{label} has real scheduler creation fields: {sorted(set(scheduler))}"
        )
    forbidden_true = collect_forbidden_true_fields(payload)
    if forbidden_true:
        raise HighIntensityGuardrailClosureError(
            f"{label} has forbidden true fields: {sorted(set(forbidden_true))}"
        )
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensityGuardrailClosureError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensityGuardrailClosureError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensityGuardrailClosureError(
            f"{label} requires broker_action=none"
        )


def collect_forbidden_true_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in FORBIDDEN_TRUE_FIELDS and item is True:
                violations.append(path)
            violations.extend(collect_forbidden_true_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                collect_forbidden_true_fields(item, f"{prefix}[{index}]")
            )
    return violations


def validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensityGuardrailClosureError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensityGuardrailClosureError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def require_equal(
    payload: Mapping[str, Any],
    field: str,
    expected: object,
    label: str,
) -> None:
    if payload.get(field) != expected:
        raise HighIntensityGuardrailClosureError(
            f"{label} requires {field}={expected}"
        )


def require_true(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not True:
        raise HighIntensityGuardrailClosureError(f"{label} requires {field}=true")


def require_false(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not False:
        raise HighIntensityGuardrailClosureError(f"{label} requires {field}=false")


def source_evidence_rows_with_previous(
    previous_source_review: Mapping[str, Any],
    *,
    task: str,
    status: object,
    evidence: str,
) -> list[dict[str, Any]]:
    inherited = list(previous_source_review.get("source_task_evidence", []))
    return [
        *inherited,
        {
            "task": task,
            "status": status,
            "evidence": evidence,
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
