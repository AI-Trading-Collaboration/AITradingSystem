from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_target_baseline_preparation import (
    DEFAULT_DIAGNOSTICS_ROOT,
    DEFAULT_SIMULATION_POLICY_ROOT,
    DEFAULT_SOURCE_BINDING_ROOT,
    DynamicTargetBaselinePreparationError,
    load_trading_2323_policy_outputs,
    load_trading_2324_source_binding_outputs,
    load_trading_2327_diagnostics_outputs,
)
from ai_trading_system.dynamic_target_baseline_preparation import (
    _validate_no_unsafe_fields as validate_no_unsafe_fields,
)
from ai_trading_system.dynamic_target_baseline_source_remediation import (
    DEFAULT_DYNAMIC_PREPARATION_ROOT,
    load_trading_2328_dynamic_preparation_outputs,
)
from ai_trading_system.dynamic_target_baseline_source_remediation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SOURCE_REMEDIATION_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    mapping,
    records,
    round_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2330_DYNAMIC_TARGET_BASELINE_TIMESTAMP_REMEDIATION"
REPORT_TYPE = "dynamic_target_baseline_timestamp_remediation"
ARTIFACT_ROLE = "dynamic_target_baseline_timestamp_remediation"
MODE = "timestamp_remediation"
STATUS = "DYNAMIC_TARGET_BASELINE_TIMESTAMP_REMEDIATION_READY_PROMOTION_BLOCKED"
BLOCKED_STATUS = "DYNAMIC_TARGET_BASELINE_TIMESTAMP_REMEDIATION_BLOCKED_NO_WRAPPER"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_TIMESTAMP_REMEDIATION_ONLY"
POLICY_ID = "dynamic_target_timestamp_remediation_policy_v1"
POLICY_VERSION = "2026-07-03"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

NEXT_RECHECK_TASK = "TRADING-2331_Dynamic_Target_Baseline_Wrapper_Readiness_Recheck"
NEXT_PIT_CAVEAT_TASK = (
    "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat"
)
NEXT_SOURCE_GENERATION_TASK = "TRADING-2331_Dynamic_Target_Baseline_Source_Generation"
NEXT_STATIC_ONLY_TASK = "TRADING-2331_Continue_Static_Baseline_Only"

SOURCE_REMEDIATION_NEXT_TASK = "TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation"

WRAPPER_REQUIRED_IDENTITY_FIELDS = (
    "baseline_id",
    "source_id",
    "source_family",
    "source_path",
    "source_hash",
    "baseline_schema_version",
    "source_artifact_hash",
)
TIMESTAMP_FIELDS = (
    "as_of_timestamp",
    "decision_timestamp",
    "valid_from",
    "valid_until",
    "rebalance_timestamp",
    "generated_at",
)
WRAPPER_REQUIRED_OUTPUT_FIELDS = (
    "baseline_id",
    "source_id",
    "source_family",
    "date",
    "target_asset",
    "target_exposure",
    "risk_asset_exposure",
    "asset_weight",
    "cash_weight",
    "as_of_timestamp",
    "decision_timestamp",
    "valid_from",
    "valid_until",
    "rebalance_flag",
    "rebalance_timestamp",
    "source_artifact_hash",
    "source_path",
    "baseline_schema_version",
    "adapter_id",
    "timestamp_remediation_policy_id",
    "timestamp_derivation_mode",
    "pit_policy",
    "known_at_policy",
    "latency_policy",
    "rebalance_policy",
    "replayability_status",
    "simulation_ready_candidate",
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
)


class DynamicTargetBaselineTimestampRemediationError(ValueError):
    pass


def run_dynamic_target_baseline_timestamp_remediation(
    *,
    source_remediation_dir: Path = DEFAULT_SOURCE_REMEDIATION_ROOT,
    dynamic_preparation_dir: Path = DEFAULT_DYNAMIC_PREPARATION_ROOT,
    diagnostics_dir: Path = DEFAULT_DIAGNOSTICS_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicTargetBaselineTimestampRemediationError(
            f"dynamic target baseline timestamp remediation only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_target_timestamp_remediation_inputs(
        source_remediation_dir=source_remediation_dir,
        dynamic_preparation_dir=dynamic_preparation_dir,
        diagnostics_dir=diagnostics_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    wrapper_rows = _rows_from_payload(inputs["source_remediation"].get("wrapper", {}))
    selected_source = _selected_source_identity(wrapper_rows)

    priority_rows = build_dynamic_target_timestamp_source_priority_matrix()
    policy = build_dynamic_target_timestamp_remediation_policy(generated_at)
    gap_rows = build_dynamic_target_timestamp_gap_matrix(wrapper_rows)
    derivation_rows = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=wrapper_rows,
        policy=policy,
    )
    remediated_rows = build_dynamic_target_timestamp_remediated_wrapper_artifact(
        wrapper_rows=wrapper_rows,
        derivation_rows=derivation_rows,
        generated_at=generated_at,
    )
    known_at_report = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=wrapper_rows,
        derivation_rows=derivation_rows,
        selected_source=selected_source,
    )
    validity_report = build_dynamic_target_validity_window_remediation_report(
        wrapper_rows=wrapper_rows,
        derivation_rows=derivation_rows,
        selected_source=selected_source,
    )
    latency_report = build_dynamic_target_latency_policy_report(
        selected_source=selected_source,
        known_at_report=known_at_report,
    )
    rebalance_report = build_dynamic_target_rebalance_timing_report(
        wrapper_rows=wrapper_rows,
        derivation_rows=derivation_rows,
        selected_source=selected_source,
    )
    wrapper_validation = build_dynamic_target_timestamp_wrapper_validation_summary(
        remediated_rows=remediated_rows,
        derivation_rows=derivation_rows,
        known_at_report=known_at_report,
    )
    pit_caveat_report = build_dynamic_target_timestamp_pit_caveat_report(
        wrapper_rows=wrapper_rows,
        derivation_rows=derivation_rows,
        known_at_report=known_at_report,
        selected_source=selected_source,
    )
    risk_cap_alignment = build_dynamic_target_risk_cap_timestamp_alignment_report(
        remediated_rows=remediated_rows,
        source_alignment=mapping(inputs["source_remediation"].get("alignment")),
        wrapper_validation=wrapper_validation,
        selected_source=selected_source,
    )
    readiness = build_dynamic_target_2331_readiness_matrix(
        selected_source=selected_source,
        wrapper_validation=wrapper_validation,
        known_at_report=known_at_report,
        latency_report=latency_report,
        rebalance_report=rebalance_report,
        risk_cap_alignment=risk_cap_alignment,
        wrapper_generated=bool(remediated_rows),
    )
    task_route = build_dynamic_target_2331_task_route(readiness)
    safety_boundary = build_dynamic_target_timestamp_remediation_safety_boundary(
        generated_at
    )
    blocked_report = build_dynamic_target_timestamp_remediation_blocked_report(
        selected_source=selected_source,
        wrapper_rows=wrapper_rows,
        readiness=readiness,
    )
    generation_requirements = build_dynamic_target_timestamp_source_generation_requirements(
        readiness=readiness,
        selected_source=selected_source,
    )
    summary = build_dynamic_target_timestamp_remediation_summary(
        generated_at=generated_at,
        source_remediation_dir=source_remediation_dir,
        dynamic_preparation_dir=dynamic_preparation_dir,
        diagnostics_dir=diagnostics_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        wrapper_rows=wrapper_rows,
        remediated_rows=remediated_rows,
        gap_rows=gap_rows,
        derivation_rows=derivation_rows,
        known_at_report=known_at_report,
        wrapper_validation=wrapper_validation,
        readiness=readiness,
        task_route=task_route,
        mode=mode,
    )
    paths = write_dynamic_target_timestamp_remediation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        gap_rows=gap_rows,
        priority_rows=priority_rows,
        policy=policy,
        derivation_rows=derivation_rows,
        known_at_report=known_at_report,
        validity_report=validity_report,
        latency_report=latency_report,
        rebalance_report=rebalance_report,
        remediated_rows=remediated_rows,
        wrapper_validation=wrapper_validation,
        pit_caveat_report=pit_caveat_report,
        risk_cap_alignment=risk_cap_alignment,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
        blocked_report=blocked_report,
        generation_requirements=generation_requirements,
    )
    return {**summary, "output_paths": paths}


def load_dynamic_target_timestamp_remediation_inputs(
    *,
    source_remediation_dir: Path,
    dynamic_preparation_dir: Path,
    diagnostics_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    source_payloads = load_trading_2329_source_remediation_outputs(source_remediation_dir)
    dynamic_preparation = load_trading_2328_dynamic_preparation_outputs(
        dynamic_preparation_dir
    )
    try:
        diagnostics = load_trading_2327_diagnostics_outputs(diagnostics_dir)
        source_binding = load_trading_2324_source_binding_outputs(source_binding_dir)
        policy = load_trading_2323_policy_outputs(simulation_policy_dir)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetBaselineTimestampRemediationError(str(exc)) from exc
    return {
        "source_remediation": source_payloads,
        "dynamic_preparation": dynamic_preparation,
        "diagnostics": diagnostics,
        "source_binding": source_binding,
        "policy": policy,
    }


def load_trading_2329_source_remediation_outputs(source_remediation_dir: Path) -> dict[str, Any]:
    required_paths = {
        "summary": source_remediation_dir / "dynamic_target_source_remediation_summary.json",
        "wrapper_validation": source_remediation_dir
        / "dynamic_target_wrapper_validation_summary.json",
        "pit_caveat": source_remediation_dir / "dynamic_target_wrapper_pit_caveat_report.json",
        "alignment": source_remediation_dir / "dynamic_target_wrapper_alignment_readiness.json",
        "readiness": source_remediation_dir / "dynamic_target_2330_readiness_matrix.json",
        "task_route": source_remediation_dir / "dynamic_target_2330_task_route.json",
        "safety_boundary": source_remediation_dir
        / "dynamic_target_source_remediation_safety_boundary.json",
    }
    optional_paths = {
        "schema_adapter": source_remediation_dir / "dynamic_target_schema_adapter_spec.json",
        "wrapper": source_remediation_dir / "dynamic_target_baseline_wrapper_artifact.json",
    }
    payloads = _load_required_payloads(required_paths, "TRADING-2329 source remediation")
    for key, path in optional_paths.items():
        payloads[key] = _load_optional_payload(path, key)
    _validate_source_remediation_inputs(payloads)
    return payloads


def build_dynamic_target_timestamp_gap_matrix(
    wrapper_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not wrapper_rows:
        return []
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in wrapper_rows:
        key = (str(row.get("baseline_id", "")), str(row.get("source_id", "")))
        grouped.setdefault(key, []).append(row)

    result: list[dict[str, Any]] = []
    for (baseline_id, source_id), rows_for_source in grouped.items():
        counts = {
            "missing_as_of_timestamp_count": _missing_count(rows_for_source, "as_of_timestamp"),
            "missing_decision_timestamp_count": _missing_count(
                rows_for_source, "decision_timestamp"
            ),
            "missing_valid_from_count": _missing_count(rows_for_source, "valid_from"),
            "missing_valid_until_count": _missing_count(rows_for_source, "valid_until"),
            "missing_rebalance_timestamp_count": _missing_count(
                rows_for_source, "rebalance_timestamp"
            ),
            "missing_generated_at_count": _missing_count(rows_for_source, "generated_at"),
            "missing_known_at_semantics_count": _missing_count(
                rows_for_source, "known_at_semantics"
            ),
        }
        blocking_fields = [
            field.replace("missing_", "").replace("_count", "")
            for field, count in counts.items()
            if count and field in {"missing_decision_timestamp_count"}
        ]
        policy_available = _timestamp_derivation_policy_available(rows_for_source)
        warning_fields = _timestamp_warning_fields(rows_for_source)
        severity = _timestamp_gap_severity(
            counts=counts,
            warning_fields=warning_fields,
            policy_available=policy_available,
        )
        result.append(
            {
                "baseline_id": baseline_id,
                "source_id": source_id,
                "record_count": len(rows_for_source),
                **counts,
                "timestamp_gap_severity": severity,
                "strict_pit_blocked": severity != "NONE",
                "pit_approximation_possible": policy_available and severity != "BLOCKING",
                "remediation_possible": policy_available and severity != "BLOCKING",
                "blocking_fields": blocking_fields if not policy_available else [],
                "warning_fields": warning_fields,
                **_safety_subset(),
            }
        )
    return result


def build_dynamic_target_timestamp_source_priority_matrix() -> list[dict[str, Any]]:
    return [
        {
            "source_priority": 1,
            "timestamp_source_type": "native_source_timestamp_fields",
            "fields": [
                "as_of_timestamp",
                "decision_timestamp",
                "valid_from",
                "valid_until",
                "rebalance_timestamp",
            ],
            "allowed_for_strict_pit": True,
            "allowed_for_pit_approximation": True,
            "allowed_for_research_only": True,
            "requires_caveat": False,
            "derivation_confidence": "HIGH",
        },
        {
            "source_priority": 2,
            "timestamp_source_type": "source_artifact_metadata",
            "fields": ["generated_at", "report_date", "artifact_as_of"],
            "allowed_for_strict_pit": False,
            "allowed_for_pit_approximation": True,
            "allowed_for_research_only": True,
            "requires_caveat": True,
            "derivation_confidence": "MEDIUM",
        },
        {
            "source_priority": 3,
            "timestamp_source_type": "registry_metadata",
            "fields": [
                "report_registry_as_of",
                "artifact_catalog_timestamp",
                "task_run_timestamp",
            ],
            "allowed_for_strict_pit": False,
            "allowed_for_pit_approximation": True,
            "allowed_for_research_only": True,
            "requires_caveat": True,
            "derivation_confidence": "MEDIUM",
        },
        {
            "source_priority": 4,
            "timestamp_source_type": "filename_or_path_date",
            "fields": ["date extracted from filename", "date extracted from run directory"],
            "allowed_for_strict_pit": False,
            "allowed_for_pit_approximation": True,
            "allowed_for_research_only": True,
            "requires_caveat": True,
            "derivation_confidence": "LOW",
        },
        {
            "source_priority": 5,
            "timestamp_source_type": "deterministic_policy_derivation",
            "fields": [
                "decision_timestamp = next_trading_session_after_as_of",
                "valid_from = decision_timestamp",
                "valid_until = next decision timestamp or decision_timestamp + horizon",
            ],
            "allowed_for_strict_pit": False,
            "allowed_for_pit_approximation": True,
            "allowed_for_research_only": True,
            "requires_caveat": True,
            "derivation_confidence": "LOW",
        },
    ]


def build_dynamic_target_timestamp_remediation_policy(
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC).replace(microsecond=0)
    return {
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "generated_at": generated.isoformat(),
        "allowed_derivation_modes": [
            "native_timestamp_copy",
            "artifact_metadata_mapping",
            "registry_metadata_mapping",
            "filename_date_mapping",
            "deterministic_latency_policy",
            "validity_window_policy",
        ],
        "blocked_derivation_modes": [
            "future_outcome_inference",
            "market_result_based_timestamp",
            "manual_undocumented_timestamp",
        ],
        "strict_pit_rules": [
            "strict PIT requires native source timestamp fields and documented known-at semantics",
            "filename / path date and deterministic derivation are not strict PIT sources",
        ],
        "pit_approximation_rules": [
            "date-level timestamps may be remediated only as PIT approximation",
            "non-native timestamp fields must carry derivation mode and caveat",
        ],
        "research_only_rules": [
            "timestamp-remediated wrapper may be used only for dynamic target dry-run diagnostics",
            "target_exposure remains a research baseline field, not trading target weight",
        ],
        "fallback_policy": (
            "missing native intraday known-at timestamp uses next trading day decision "
            "policy with PIT caveat"
        ),
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_derivation_matrix(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not wrapper_rows:
        return []
    next_decisions = _next_decision_by_record_key(wrapper_rows)
    result: list[dict[str, Any]] = []
    for index, row in enumerate(wrapper_rows):
        source_date = _row_date(row)
        native = _native_known_at_ready(row)
        as_of_original = _string(row.get("as_of_timestamp"))
        decision_original = _string(row.get("decision_timestamp"))
        valid_from_original = _string(row.get("valid_from"))
        valid_until_original = _string(row.get("valid_until"))
        rebalance_original = _string(row.get("rebalance_timestamp"))

        as_of_dt = _parse_datetime(as_of_original) or _date_to_utc_midnight(source_date)
        if native and decision_original:
            decision_dt = _parse_datetime(decision_original) or as_of_dt
            decision_mode = "native_timestamp_copy"
            confidence = "HIGH"
            caveat_required = False
            status = "REMEDIATED_STRICT_NATIVE"
        elif as_of_dt:
            decision_dt = _next_business_day(as_of_dt)
            decision_mode = "deterministic_latency_policy"
            confidence = "MEDIUM" if as_of_original else "LOW"
            caveat_required = True
            status = "REMEDIATED_PIT_APPROXIMATION"
        else:
            decision_dt = None
            decision_mode = "unresolved"
            confidence = "NONE"
            caveat_required = True
            status = "BLOCKED_TIMESTAMP_UNRESOLVED"

        valid_from_dt = decision_dt
        record_key = _record_key(row, index)
        valid_until_dt = next_decisions.get(record_key)
        if not valid_until_dt and decision_dt:
            valid_until_dt = _next_business_day(decision_dt)

        if native and valid_until_original:
            valid_until_mode = "native_timestamp_copy"
        elif valid_until_dt:
            valid_until_mode = "validity_window_policy"
        else:
            valid_until_mode = "unresolved"
            if status != "BLOCKED_TIMESTAMP_UNRESOLVED":
                status = "REMEDIATED_RESEARCH_ONLY"

        result.append(
            {
                "record_id": _record_id(row, index),
                "baseline_id": _string(row.get("baseline_id")),
                "source_id": _string(row.get("source_id")),
                "target_asset": _string(row.get("target_asset")),
                "source_date": source_date.isoformat() if source_date else "",
                "as_of_timestamp_original": as_of_original,
                "as_of_timestamp_remediated": _format_datetime(as_of_dt),
                "as_of_timestamp_derivation_mode": (
                    "native_timestamp_copy"
                    if native and as_of_original
                    else _as_of_derivation_mode(row)
                ),
                "decision_timestamp_original": decision_original,
                "decision_timestamp_remediated": _format_datetime(decision_dt),
                "decision_timestamp_derivation_mode": decision_mode,
                "valid_from_original": valid_from_original,
                "valid_from_remediated": _format_datetime(valid_from_dt),
                "valid_from_derivation_mode": (
                    "native_timestamp_copy"
                    if native and valid_from_original
                    else "deterministic_latency_policy"
                ),
                "valid_until_original": valid_until_original,
                "valid_until_remediated": _format_datetime(valid_until_dt),
                "valid_until_derivation_mode": valid_until_mode,
                "rebalance_timestamp_original": rebalance_original,
                "rebalance_timestamp_remediated": _format_datetime(decision_dt),
                "rebalance_timestamp_derivation_mode": (
                    "native_timestamp_copy"
                    if native and rebalance_original
                    else "deterministic_latency_policy"
                ),
                "derivation_confidence": confidence,
                "pit_caveat_required": caveat_required,
                "remediation_status": status,
            }
        )
    return result


def build_dynamic_target_known_at_semantics_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    if not wrapper_rows:
        return {
            "baseline_id": "",
            "source_id": "",
            "known_at_policy": "UNKNOWN_BLOCKED",
            "known_at_source": "none",
            "as_of_semantics": "missing_wrapper",
            "decision_semantics": "missing_wrapper",
            "rebalance_semantics": "missing_wrapper",
            "latency_assumption": "none",
            "timezone_policy": "UTC",
            "trading_calendar_policy": "not_evaluated",
            "known_at_confidence": "NONE",
            "strict_pit_ready": False,
            "pit_approximation_ready": False,
            "research_only_ready": False,
            "known_at_caveats": ["wrapper artifact missing"],
            **_safety_subset(),
        }
    strict_ready = all(_native_known_at_ready(row) for row in wrapper_rows)
    unresolved = any(
        row.get("remediation_status") == "BLOCKED_TIMESTAMP_UNRESOLVED"
        for row in derivation_rows
    )
    if strict_ready:
        policy = "NATIVE_KNOWN_AT"
        confidence = "HIGH"
        caveats: list[str] = []
    elif unresolved:
        policy = "UNKNOWN_BLOCKED"
        confidence = "NONE"
        caveats = ["one or more records have unresolved decision timestamp"]
    else:
        policy = "NEXT_SESSION_DECISION_POLICY"
        confidence = "MEDIUM"
        caveats = [
            "native intraday known-at timestamp missing",
            "decision timestamp uses next trading day policy",
            "timestamp remediation is PIT approximation and research-only",
        ]
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "known_at_policy": policy,
        "known_at_source": (
            "native_source_timestamp_fields"
            if strict_ready
            else "wrapper_date_fields_plus_deterministic_latency_policy"
        ),
        "as_of_semantics": (
            "native source as_of timestamp"
            if strict_ready
            else "date-level source observation timestamp with PIT caveat"
        ),
        "decision_semantics": (
            "native source decision timestamp"
            if strict_ready
            else "next trading day after as_of timestamp"
        ),
        "rebalance_semantics": (
            "native source rebalance timestamp"
            if strict_ready
            else "rebalance no earlier than remediated decision timestamp"
        ),
        "latency_assumption": "native" if strict_ready else "next trading day decision",
        "timezone_policy": "UTC",
        "trading_calendar_policy": (
            "weekday calendar approximation; exchange holiday validation deferred to 2331"
        ),
        "known_at_confidence": confidence,
        "strict_pit_ready": strict_ready,
        "pit_approximation_ready": not unresolved,
        "research_only_ready": not unresolved,
        "known_at_caveats": caveats,
        **_safety_subset(),
    }


def build_dynamic_target_validity_window_remediation_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    if not wrapper_rows:
        policy = "BLOCKED_VALIDITY_UNKNOWN"
    elif all(_native_known_at_ready(row) and row.get("valid_until") for row in wrapper_rows):
        policy = "NATIVE_VALIDITY_WINDOW"
    elif derivation_rows and all(row.get("valid_until_remediated") for row in derivation_rows):
        policy = "NEXT_DECISION_UNTIL_REPLACED"
    else:
        policy = "BLOCKED_VALIDITY_UNKNOWN"
    gap_count = sum(
        1
        for row in wrapper_rows
        if _is_missing(row.get("valid_from")) or _is_missing(row.get("valid_until"))
    )
    blocked_count = sum(
        1
        for row in derivation_rows
        if row.get("remediation_status") == "BLOCKED_TIMESTAMP_UNRESOLVED"
    )
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "validity_policy": policy,
        "horizon_source": "not_native",
        "rebalance_frequency_source": "observed_wrapper_dates",
        "valid_from_policy": (
            "native_timestamp_copy" if policy == "NATIVE_VALIDITY_WINDOW" else "decision_timestamp"
        ),
        "valid_until_policy": (
            "native_timestamp_copy"
            if policy == "NATIVE_VALIDITY_WINDOW"
            else "next decision timestamp or next business day"
        ),
        "validity_gap_count": gap_count,
        "validity_remediated_count": max(0, len(derivation_rows) - blocked_count),
        "validity_blocked_count": blocked_count,
        "validity_caveats": (
            []
            if policy == "NATIVE_VALIDITY_WINDOW"
            else [
                "validity window is derived for research-only diagnostics",
                "2331 must recheck exchange calendar and dry-run alignment",
            ]
        ),
        **_safety_subset(),
    }


def build_dynamic_target_latency_policy_report(
    *,
    selected_source: Mapping[str, Any],
    known_at_report: Mapping[str, Any],
) -> dict[str, Any]:
    strict_ready = known_at_report.get("strict_pit_ready") is True
    blocked = known_at_report.get("known_at_policy") == "UNKNOWN_BLOCKED"
    if strict_ready:
        policy = "NATIVE_LATENCY"
        caveats: list[str] = []
    elif blocked:
        policy = "BLOCKED_LATENCY_UNKNOWN"
        caveats = ["known-at semantics unresolved"]
    else:
        policy = "NEXT_TRADING_DAY_DECISION"
        caveats = [
            "same-day tradability is not assumed without native intraday known-at timestamp"
        ]
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "latency_policy": policy,
        "input_availability_assumption": (
            "native timestamp availability" if strict_ready else "available after source date"
        ),
        "decision_delay": "native" if strict_ready else "next trading day",
        "rebalance_delay": "native" if strict_ready else "not before decision timestamp",
        "execution_delay": "not_applicable_research_only",
        "calendar_policy": "weekday calendar approximation",
        "timezone_policy": "UTC",
        "latency_caveats": caveats,
        **_safety_subset(),
    }


def build_dynamic_target_rebalance_timing_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    if not wrapper_rows:
        policy = "BLOCKED_REBALANCE_UNKNOWN"
    elif all(
        _native_known_at_ready(row) and row.get("rebalance_timestamp")
        for row in wrapper_rows
    ):
        policy = "NATIVE_REBALANCE_TIMING"
    elif derivation_rows and all(
        row.get("rebalance_timestamp_remediated") for row in derivation_rows
    ):
        policy = "DAILY_DECISION_REBALANCE"
    else:
        policy = "BLOCKED_REBALANCE_UNKNOWN"
    available = sum(1 for row in wrapper_rows if not _is_missing(row.get("rebalance_timestamp")))
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "rebalance_policy": policy,
        "rebalance_frequency": "daily_observed_wrapper_dates",
        "rebalance_timestamp_available": bool(available),
        "rebalance_timestamp_derivation_mode": (
            "native_timestamp_copy"
            if policy == "NATIVE_REBALANCE_TIMING"
            else "deterministic_latency_policy"
        ),
        "rebalance_flag_coverage": round_float(available / len(wrapper_rows))
        if wrapper_rows
        else 0.0,
        "rebalance_timing_caveats": (
            []
            if policy == "NATIVE_REBALANCE_TIMING"
            else ["rebalance timing is research-only approximation"]
        ),
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_remediated_wrapper_artifact(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    generated = generated_at or datetime.now(tz=UTC).replace(microsecond=0)
    derivation_by_id = {row["record_id"]: row for row in derivation_rows}
    result: list[dict[str, Any]] = []
    for index, row in enumerate(wrapper_rows):
        derivation = derivation_by_id.get(_record_id(row, index), {})
        blocked = derivation.get("remediation_status") == "BLOCKED_TIMESTAMP_UNRESOLVED"
        native = derivation.get("remediation_status") == "REMEDIATED_STRICT_NATIVE"
        pit_policy = "STRICT_PIT_READY" if native else "PIT_APPROXIMATION_READY"
        if blocked:
            pit_policy = "PIT_BLOCKED"
        result.append(
            {
                "baseline_id": _string(row.get("baseline_id")),
                "source_id": _string(row.get("source_id")),
                "source_family": _string(row.get("source_family")),
                "date": _string(row.get("date")),
                "target_asset": _string(row.get("target_asset")),
                "target_exposure": row.get("target_exposure"),
                "risk_asset_exposure": row.get("risk_asset_exposure"),
                "asset_weight": row.get("asset_weight"),
                "cash_weight": row.get("cash_weight"),
                "as_of_timestamp": _string(derivation.get("as_of_timestamp_remediated")),
                "decision_timestamp": _string(
                    derivation.get("decision_timestamp_remediated")
                ),
                "valid_from": _string(derivation.get("valid_from_remediated")),
                "valid_until": _string(derivation.get("valid_until_remediated")),
                "rebalance_flag": bool(row.get("rebalance_flag")),
                "rebalance_timestamp": _string(
                    derivation.get("rebalance_timestamp_remediated")
                ),
                "source_artifact_hash": _string(row.get("source_artifact_hash")),
                "source_hash": _string(row.get("source_hash")),
                "source_path": _string(row.get("source_path")),
                "baseline_schema_version": _string(row.get("baseline_schema_version")),
                "adapter_id": _string(row.get("adapter_id")),
                "generated_at": generated.isoformat(),
                "timestamp_remediation_policy_id": POLICY_ID,
                "timestamp_derivation_mode": _timestamp_derivation_mode_summary(
                    derivation
                ),
                "pit_policy": pit_policy,
                "known_at_policy": "NATIVE_KNOWN_AT"
                if native
                else "NEXT_SESSION_DECISION_POLICY",
                "latency_policy": "NATIVE_LATENCY"
                if native
                else "NEXT_TRADING_DAY_DECISION",
                "rebalance_policy": "NATIVE_REBALANCE_TIMING"
                if native
                else "DAILY_DECISION_REBALANCE",
                "replayability_status": _string(
                    row.get("replayability_status") or "REPLAYABLE_WITH_CAVEAT"
                ),
                "simulation_ready_candidate": not blocked,
                "allowed_usage": ["dynamic_target_dry_run_diagnostics"] if not blocked else [],
                "blocked_usage": ["promotion", "paper_shadow", "production", "broker_action"],
                "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
                **_safety_subset(),
            }
        )
    return result


def build_dynamic_target_timestamp_wrapper_validation_summary(
    *,
    remediated_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    known_at_report: Mapping[str, Any],
) -> dict[str, Any]:
    fail_count = 0
    pass_count = 0
    for row in remediated_rows:
        for field in WRAPPER_REQUIRED_OUTPUT_FIELDS:
            if field in {"promotion_allowed", "paper_shadow_allowed", "production_allowed"}:
                pass_count += row.get(field) is False
            elif field == "broker_action":
                pass_count += str(row.get(field, "")).lower() == "none"
            elif _is_missing(row.get(field)):
                fail_count += 1
            else:
                pass_count += 1
    blocked = any(
        row.get("remediation_status") == "BLOCKED_TIMESTAMP_UNRESOLVED"
        for row in derivation_rows
    )
    warnings = []
    if known_at_report.get("strict_pit_ready") is not True and remediated_rows:
        warnings.append("pit_or_known_at_caveat_required")
    if blocked:
        warnings.append("timestamp_unresolved")
    validation_status = "FAIL" if fail_count or blocked else "PASS_WITH_WARNINGS"
    if validation_status == "PASS_WITH_WARNINGS" and not warnings:
        validation_status = "PASS"
    return {
        "artifact_role": "dynamic_target_timestamp_wrapper_validation_summary",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.wrapper_validation.v1",
        "wrapper_generated": bool(remediated_rows),
        "wrapper_record_count": len(remediated_rows),
        "timestamp_required_field_pass_count": int(pass_count),
        "timestamp_required_field_fail_count": int(fail_count),
        "as_of_timestamp_status": _field_status(remediated_rows, "as_of_timestamp"),
        "decision_timestamp_status": _field_status(remediated_rows, "decision_timestamp"),
        "valid_from_status": _field_status(remediated_rows, "valid_from"),
        "valid_until_status": _field_status(remediated_rows, "valid_until"),
        "rebalance_timestamp_status": _field_status(remediated_rows, "rebalance_timestamp"),
        "known_at_semantics_status": known_at_report.get("known_at_policy", "UNKNOWN"),
        "latency_policy_status": _field_status(remediated_rows, "latency_policy"),
        "rebalance_policy_status": _field_status(remediated_rows, "rebalance_policy"),
        "simulation_ready_candidate": bool(remediated_rows) and not blocked,
        "validation_status": validation_status,
        "validation_errors": ["timestamp required field missing"] if fail_count else [],
        "validation_warnings": warnings,
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_pit_caveat_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    known_at_report: Mapping[str, Any],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    strict_ready = known_at_report.get("strict_pit_ready") is True
    pit_ready = known_at_report.get("pit_approximation_ready") is True
    missing_native = []
    if not strict_ready and wrapper_rows:
        missing_native = [
            "native_intraday_known_at_timestamp",
            "native_decision_timestamp_semantics",
            "native_validity_window_semantics",
        ]
    derived_fields = sorted(
        {
            field
            for row in derivation_rows
            for field in (
                "as_of_timestamp",
                "decision_timestamp",
                "valid_from",
                "valid_until",
                "rebalance_timestamp",
            )
            if str(row.get(f"{field}_derivation_mode")) != "native_timestamp_copy"
        }
    )
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "strict_pit_ready": strict_ready,
        "pit_approximation_ready": pit_ready,
        "research_only_ready": known_at_report.get("research_only_ready") is True,
        "missing_native_timestamp_fields": missing_native,
        "derived_timestamp_fields": derived_fields,
        "known_at_caveats": list(known_at_report.get("known_at_caveats", [])),
        "lookahead_risk": "MITIGATED_BY_NEXT_SESSION_POLICY" if pit_ready else "BLOCKED",
        "revision_risk": "SOURCE_ARTIFACT_VERSION_DEPENDENT",
        "allowed_usage": ["dynamic_target_dry_run_diagnostics"] if pit_ready else [],
        "blocked_usage": ["promotion", "paper_shadow", "production", "broker_action"],
        **_safety_subset(),
    }


def build_dynamic_target_risk_cap_timestamp_alignment_report(
    *,
    remediated_rows: Sequence[Mapping[str, Any]],
    source_alignment: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    validation_status = wrapper_validation.get("validation_status")
    risk_cap_available = source_alignment.get("risk_cap_trigger_series_available") is True
    blocked = validation_status == "FAIL" or not remediated_rows
    date_values = sorted(
        {_string(row.get("date")) for row in remediated_rows if row.get("date")}
    )
    assets = sorted(
        {
            _string(row.get("target_asset"))
            for row in remediated_rows
            if row.get("target_asset")
        }
    )
    warnings = []
    if source_alignment.get("alignment_readiness_status") == "WRAPPER_ALIGNMENT_BLOCKED":
        warnings.append("source remediation alignment blockers carried forward for 2331")
    if not risk_cap_available:
        warnings.append("risk_cap_trigger_series_not_available")
    if blocked:
        status = "TIMESTAMP_ALIGNMENT_BLOCKED"
        blockers = ["timestamp_wrapper_validation_failed"]
    elif warnings:
        status = "TIMESTAMP_ALIGNMENT_READY_WITH_WARNINGS"
        blockers = []
    else:
        status = "TIMESTAMP_ALIGNMENT_READY"
        blockers = []
    return {
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "risk_cap_trigger_series_available": risk_cap_available,
        "dynamic_target_timestamp_ready": bool(remediated_rows) and not blocked,
        "date_overlap_start": source_alignment.get("date_overlap_start")
        or (date_values[0] if date_values else ""),
        "date_overlap_end": source_alignment.get("date_overlap_end")
        or (date_values[-1] if date_values else ""),
        "overlap_record_count": len(remediated_rows),
        "decision_timestamp_alignment_status": "READY_WITH_PIT_CAVEAT"
        if not blocked
        else "BLOCKED",
        "validity_window_alignment_status": "READY_WITH_WARNINGS"
        if not blocked
        else "BLOCKED",
        "rebalance_timing_alignment_status": "READY_WITH_WARNINGS"
        if not blocked
        else "BLOCKED",
        "asset_overlap_status": "PARTIAL_OR_REQUIRES_2331_RECHECK"
        if assets
        else "BLOCKED_NO_ASSETS",
        "horizon_overlap_status": "REQUIRES_2331_RECHECK",
        "alignment_readiness_status": status,
        "alignment_blockers": blockers,
        "alignment_warnings": warnings,
        **_safety_subset(),
    }


def build_dynamic_target_2331_readiness_matrix(
    *,
    selected_source: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
    known_at_report: Mapping[str, Any],
    latency_report: Mapping[str, Any],
    rebalance_report: Mapping[str, Any],
    risk_cap_alignment: Mapping[str, Any],
    wrapper_generated: bool,
) -> dict[str, Any]:
    validation_status = str(wrapper_validation.get("validation_status", "FAIL"))
    alignment_status = str(
        risk_cap_alignment.get("alignment_readiness_status", "TIMESTAMP_ALIGNMENT_BLOCKED")
    )
    blockers: list[str] = []
    warnings: list[str] = []
    if not wrapper_generated:
        readiness_status = "TIMESTAMP_REMEDIATION_SOURCE_GENERATION_REQUIRED"
        blockers.append("timestamp_remediated_wrapper_not_generated")
    elif validation_status == "FAIL" or alignment_status == "TIMESTAMP_ALIGNMENT_BLOCKED":
        readiness_status = "TIMESTAMP_REMEDIATION_BLOCKED"
        blockers.extend(wrapper_validation.get("validation_errors", []))
        blockers.extend(risk_cap_alignment.get("alignment_blockers", []))
    elif (
        known_at_report.get("strict_pit_ready") is True
        and alignment_status == "TIMESTAMP_ALIGNMENT_READY"
    ):
        readiness_status = "TIMESTAMP_REMEDIATED_READY_FOR_2331"
    else:
        readiness_status = "TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331"
        warnings.extend(wrapper_validation.get("validation_warnings", []))
        warnings.extend(risk_cap_alignment.get("alignment_warnings", []))
        warnings.extend(known_at_report.get("known_at_caveats", []))
    return {
        "readiness_status": readiness_status,
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "wrapper_generated": wrapper_generated,
        "wrapper_validation_status": validation_status,
        "timestamp_remediation_status": "REMEDIATED"
        if readiness_status.startswith("TIMESTAMP_REMEDIATED")
        else "BLOCKED",
        "pit_policy": "STRICT_PIT_READY"
        if known_at_report.get("strict_pit_ready") is True
        else ("PIT_APPROXIMATION_READY" if wrapper_generated else "PIT_BLOCKED"),
        "known_at_policy": known_at_report.get("known_at_policy", "UNKNOWN_BLOCKED"),
        "latency_policy": latency_report.get("latency_policy", "BLOCKED_LATENCY_UNKNOWN"),
        "rebalance_policy": rebalance_report.get(
            "rebalance_policy", "BLOCKED_REBALANCE_UNKNOWN"
        ),
        "alignment_readiness_status": alignment_status,
        "2331_allowed": readiness_status
        in {
            "TIMESTAMP_REMEDIATED_READY_FOR_2331",
            "TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331",
        },
        "2331_blockers": sorted(set(blockers)),
        "2331_warnings": sorted(set(warnings)),
        **_safety_subset(),
    }


def build_dynamic_target_2331_task_route(readiness: Mapping[str, Any]) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", "TIMESTAMP_REMEDIATION_BLOCKED"))
    if status == "TIMESTAMP_REMEDIATED_READY_FOR_2331":
        next_task = NEXT_RECHECK_TASK
    elif status == "TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331":
        next_task = NEXT_PIT_CAVEAT_TASK
    elif status == "TIMESTAMP_REMEDIATION_SOURCE_GENERATION_REQUIRED":
        next_task = NEXT_SOURCE_GENERATION_TASK
    else:
        next_task = NEXT_STATIC_ONLY_TASK
    return {
        "artifact_role": "dynamic_target_2331_task_route",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.2331_task_route.v1",
        "readiness_status": status,
        "next_task": next_task,
        "caveat": _route_caveat(status),
        "simulation_executed": False,
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_remediation_safety_boundary(
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC).replace(microsecond=0)
    return {
        "artifact_role": "dynamic_target_timestamp_remediation_safety_boundary",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "generated_at": generated.isoformat(),
        "research_only": True,
        "timestamp_remediation_only": True,
        "simulation_executed": False,
        "portfolio_effect": "none",
        "production_effect": "none",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "manual_review_only": True,
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        "forbidden_outputs": [
            "target_weight_action",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
            "production_decision",
            "paper_shadow_ready",
        ],
    }


def build_dynamic_target_timestamp_remediation_blocked_report(
    *,
    selected_source: Mapping[str, Any],
    wrapper_rows: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    blocked = not wrapper_rows or readiness.get("2331_allowed") is not True
    reasons = []
    if not wrapper_rows:
        reasons.append("wrapper artifact missing")
    reasons.extend(readiness.get("2331_blockers", []))
    return {
        "artifact_role": "dynamic_target_timestamp_remediation_blocked_report",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.blocked_report.v1",
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "timestamp_remediation_blocked": blocked,
        "blocked_reasons": sorted(set(reasons)),
        "next_task": NEXT_SOURCE_GENERATION_TASK
        if not wrapper_rows
        else readiness.get("readiness_status"),
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_source_generation_requirements(
    *,
    readiness: Mapping[str, Any],
    selected_source: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_role": "dynamic_target_timestamp_source_generation_requirements",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.source_generation_requirements.v1",
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "source_generation_required": readiness.get("readiness_status")
        == "TIMESTAMP_REMEDIATION_SOURCE_GENERATION_REQUIRED",
        "required_native_fields": [
            "as_of_timestamp",
            "decision_timestamp",
            "valid_from",
            "valid_until",
            "rebalance_timestamp",
            "known_at_semantics",
            "source_artifact_hash",
        ],
        "required_policies": [
            "native known-at semantics",
            "latency policy",
            "rebalance timing policy",
            "risk-cap trigger timestamp alignment policy",
        ],
        "acceptance_condition": (
            "new source can re-enter TRADING-2330 or TRADING-2331 only after native "
            "timestamp fields are present or caveat is explicitly accepted"
        ),
        **_safety_subset(),
    }


def build_dynamic_target_timestamp_remediation_summary(
    *,
    generated_at: datetime,
    source_remediation_dir: Path,
    dynamic_preparation_dir: Path,
    diagnostics_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    wrapper_rows: Sequence[Mapping[str, Any]],
    remediated_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    derivation_rows: Sequence[Mapping[str, Any]],
    known_at_report: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "title": "Dynamic Target Baseline Timestamp Remediation",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "schema_version": f"{REPORT_TYPE}.v1",
        "mode": mode,
        "status": STATUS if wrapper_rows else BLOCKED_STATUS,
        "generated_at": generated_at.isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": _iso_date_value(ANCHOR_DATE),
        "default_backtest_start": _iso_date_value(DEFAULT_BACKTEST_START),
        "source_remediation_dir": str(source_remediation_dir),
        "dynamic_preparation_dir": str(dynamic_preparation_dir),
        "diagnostics_dir": str(diagnostics_dir),
        "source_binding_dir": str(source_binding_dir),
        "simulation_policy_dir": str(simulation_policy_dir),
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_gate_required": False,
        "data_quality_gate_executed": False,
        "aits_validate_data_executed": False,
        "data_quality_gate_rationale": (
            "aits validate-data not applicable because TRADING-2330 only reads "
            "prior research outputs / static config / registry / candidate artifacts"
        ),
        "dynamic_target_timestamp_remediation_cli": True,
        "timestamp_gap_matrix_generated": True,
        "timestamp_source_priority_matrix_generated": True,
        "timestamp_remediation_policy_generated": True,
        "timestamp_derivation_matrix_generated": True,
        "known_at_semantics_report_generated": True,
        "validity_window_remediation_report_generated": True,
        "latency_policy_report_generated": True,
        "rebalance_timing_report_generated": True,
        "timestamp_remediated_wrapper_generated": bool(remediated_rows),
        "timestamp_wrapper_validation_summary_generated": True,
        "timestamp_pit_caveat_report_generated": True,
        "risk_cap_timestamp_alignment_report_generated": True,
        "dynamic_target_2331_readiness_matrix_generated": True,
        "dynamic_target_2331_task_route_generated": True,
        "wrapper_input_record_count": len(wrapper_rows),
        "timestamp_remediated_wrapper_record_count": len(remediated_rows),
        "timestamp_gap_row_count": len(gap_rows),
        "timestamp_derivation_row_count": len(derivation_rows),
        "known_at_policy": known_at_report.get("known_at_policy", "UNKNOWN_BLOCKED"),
        "wrapper_validation_status": wrapper_validation.get("validation_status"),
        "readiness_status": readiness.get("readiness_status"),
        "2331_allowed": readiness.get("2331_allowed", False),
        "next_task": task_route.get("next_task"),
        "simulation_executed": False,
        "research_only": True,
        "timestamp_remediation_only": True,
        "manual_review_only": True,
        "portfolio_effect": "none",
        "production_effect": "none",
        **_safety_subset(),
    }


def write_dynamic_target_timestamp_remediation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    gap_rows: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    derivation_rows: Sequence[Mapping[str, Any]],
    known_at_report: Mapping[str, Any],
    validity_report: Mapping[str, Any],
    latency_report: Mapping[str, Any],
    rebalance_report: Mapping[str, Any],
    remediated_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    pit_caveat_report: Mapping[str, Any],
    risk_cap_alignment: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    blocked_report: Mapping[str, Any],
    generation_requirements: Mapping[str, Any],
) -> dict[str, str]:
    output_payloads = [
        summary,
        *gap_rows,
        *priority_rows,
        policy,
        *derivation_rows,
        known_at_report,
        validity_report,
        latency_report,
        rebalance_report,
        *remediated_rows,
        wrapper_validation,
        pit_caveat_report,
        risk_cap_alignment,
        readiness,
        task_route,
        safety_boundary,
        blocked_report,
        generation_requirements,
    ]
    for index, payload in enumerate(output_payloads):
        _validate_safe_output(f"TRADING-2330 output {index}", payload)

    paths: dict[str, Path] = {
        "summary": output_dir / "dynamic_target_timestamp_remediation_summary.json",
        "gap_json": output_dir / "dynamic_target_timestamp_gap_matrix.json",
        "gap_csv": output_dir / "dynamic_target_timestamp_gap_matrix.csv",
        "priority_json": output_dir / "dynamic_target_timestamp_source_priority_matrix.json",
        "priority_csv": output_dir / "dynamic_target_timestamp_source_priority_matrix.csv",
        "policy": output_dir / "dynamic_target_timestamp_remediation_policy.json",
        "derivation_json": output_dir / "dynamic_target_timestamp_derivation_matrix.json",
        "derivation_csv": output_dir / "dynamic_target_timestamp_derivation_matrix.csv",
        "known_at": output_dir / "dynamic_target_known_at_semantics_report.json",
        "validity": output_dir / "dynamic_target_validity_window_remediation_report.json",
        "latency": output_dir / "dynamic_target_latency_policy_report.json",
        "rebalance": output_dir / "dynamic_target_rebalance_timing_report.json",
        "wrapper_json": output_dir / "dynamic_target_timestamp_remediated_wrapper_artifact.json",
        "wrapper_csv": output_dir / "dynamic_target_timestamp_remediated_wrapper_artifact.csv",
        "wrapper_validation": output_dir
        / "dynamic_target_timestamp_wrapper_validation_summary.json",
        "pit_caveat": output_dir / "dynamic_target_timestamp_pit_caveat_report.json",
        "risk_cap_alignment": output_dir
        / "dynamic_target_risk_cap_timestamp_alignment_report.json",
        "readiness": output_dir / "dynamic_target_2331_readiness_matrix.json",
        "task_route": output_dir / "dynamic_target_2331_task_route.json",
        "safety_boundary": output_dir
        / "dynamic_target_timestamp_remediation_safety_boundary.json",
        "blocked_report": output_dir
        / "dynamic_target_timestamp_remediation_blocked_report.json",
        "generation_requirements": output_dir
        / "dynamic_target_timestamp_source_generation_requirements.json",
        "report_doc": docs_root / "dynamic_target_baseline_timestamp_remediation_report.md",
        "gap_doc": docs_root / "dynamic_target_timestamp_gap_matrix.md",
        "known_at_doc": docs_root / "dynamic_target_known_at_semantics_report.md",
        "validity_doc": docs_root
        / "dynamic_target_validity_window_remediation_report.md",
        "route_doc": docs_root / "dynamic_target_2331_readiness_route.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["gap_json"], {**dict(summary), "rows": list(gap_rows)})
    write_csv_rows(paths["gap_csv"], gap_rows)
    write_json(paths["priority_json"], {**dict(summary), "rows": list(priority_rows)})
    write_csv_rows(paths["priority_csv"], priority_rows)
    write_json(paths["policy"], dict(policy))
    write_json(paths["derivation_json"], {**dict(summary), "rows": list(derivation_rows)})
    write_csv_rows(paths["derivation_csv"], derivation_rows)
    write_json(paths["known_at"], dict(known_at_report))
    write_json(paths["validity"], dict(validity_report))
    write_json(paths["latency"], dict(latency_report))
    write_json(paths["rebalance"], dict(rebalance_report))
    write_json(paths["wrapper_json"], {**dict(summary), "rows": list(remediated_rows)})
    write_csv_rows(paths["wrapper_csv"], remediated_rows)
    write_json(paths["wrapper_validation"], dict(wrapper_validation))
    write_json(paths["pit_caveat"], dict(pit_caveat_report))
    write_json(paths["risk_cap_alignment"], dict(risk_cap_alignment))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_json(paths["blocked_report"], dict(blocked_report))
    write_json(paths["generation_requirements"], dict(generation_requirements))

    write_markdown(
        paths["report_doc"],
        _render_timestamp_remediation_report(
            summary,
            gap_rows,
            wrapper_validation,
            pit_caveat_report,
            readiness,
            task_route,
        ),
    )
    write_markdown(paths["gap_doc"], _render_gap_matrix_doc(summary, gap_rows))
    write_markdown(paths["known_at_doc"], _render_known_at_doc(known_at_report))
    write_markdown(paths["validity_doc"], _render_validity_doc(validity_report))
    write_markdown(paths["route_doc"], _render_2331_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicTargetBaselineTimestampRemediationError(
            f"{label} missing required artifacts: {missing}"
        )
    for key, path in paths.items():
        payloads[key] = _load_json_object(path, f"{label} {key}")
    return payloads


def _load_optional_payload(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _load_json_object(path, label)


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DynamicTargetBaselineTimestampRemediationError(
            f"{label} artifact is not valid JSON: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise DynamicTargetBaselineTimestampRemediationError(
            f"{label} artifact must be a JSON object: {path}"
        )
    return dict(payload)


def _validate_source_remediation_inputs(payloads: Mapping[str, Any]) -> None:
    for key, payload in payloads.items():
        if payload:
            _validate_safe_input(f"TRADING-2329 {key}", payload)
    summary = mapping(payloads.get("summary"))
    task_route = mapping(payloads.get("task_route"))
    safety = mapping(payloads.get("safety_boundary"))
    if str(task_route.get("next_task")) != SOURCE_REMEDIATION_NEXT_TASK:
        raise DynamicTargetBaselineTimestampRemediationError(
            "TRADING-2329 route is not timestamp remediation"
        )
    if str(summary.get("next_task")) != SOURCE_REMEDIATION_NEXT_TASK:
        raise DynamicTargetBaselineTimestampRemediationError(
            "TRADING-2329 summary is not routed to timestamp remediation"
        )
    if summary.get("wrapper_generated") is False:
        return
    wrapper_rows = _rows_from_payload(payloads.get("wrapper", {}))
    if not wrapper_rows:
        return
    for index, row in enumerate(wrapper_rows):
        missing = [
            field
            for field in WRAPPER_REQUIRED_IDENTITY_FIELDS
            if _is_missing(row.get(field))
        ]
        if missing:
            raise DynamicTargetBaselineTimestampRemediationError(
                f"TRADING-2329 wrapper row {index} missing source identity fields: {missing}"
            )
    if not safety:
        raise DynamicTargetBaselineTimestampRemediationError(
            "TRADING-2329 safety boundary missing"
        )


def _validate_safe_input(name: str, payload: Mapping[str, Any]) -> None:
    try:
        validate_no_unsafe_fields(name, payload)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetBaselineTimestampRemediationError(str(exc)) from exc


def _validate_safe_output(name: str, payload: Mapping[str, Any]) -> None:
    try:
        validate_no_unsafe_fields(name, payload)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetBaselineTimestampRemediationError(str(exc)) from exc


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in records(mapping(payload).get("rows"))]


def _selected_source_identity(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    first = rows[0]
    return {
        "baseline_id": _string(first.get("baseline_id")),
        "source_id": _string(first.get("source_id")),
        "source_family": _string(first.get("source_family")),
        "source_path": _string(first.get("source_path")),
        "source_hash": _string(first.get("source_hash")),
        "source_artifact_hash": _string(first.get("source_artifact_hash")),
        "baseline_schema_version": _string(first.get("baseline_schema_version")),
    }


def _missing_count(rows: Sequence[Mapping[str, Any]], field: str) -> int:
    return sum(1 for row in rows if _is_missing(row.get(field)))


def _timestamp_derivation_policy_available(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        not _is_missing(row.get("date")) or not _is_missing(row.get("as_of_timestamp"))
        for row in rows
    )


def _timestamp_warning_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    warnings: set[str] = set()
    for row in rows:
        if not _native_known_at_ready(row):
            warnings.add("native_known_at_semantics")
            warnings.add("native_intraday_timestamp")
        if str(row.get("pit_policy")) != "STRICT_PIT_READY":
            warnings.add("strict_pit_policy")
        if str(row.get("known_at_semantics", "")).lower().startswith("pit approximation"):
            warnings.add("known_at_semantics_native_missing")
    return sorted(warnings)


def _timestamp_gap_severity(
    *,
    counts: Mapping[str, int],
    warning_fields: Sequence[str],
    policy_available: bool,
) -> str:
    if counts.get("missing_decision_timestamp_count", 0) and not policy_available:
        return "BLOCKING"
    if warning_fields and counts.get("missing_decision_timestamp_count", 0):
        return "HIGH"
    if warning_fields:
        return "HIGH"
    if counts.get("missing_valid_until_count", 0):
        return "MEDIUM"
    if any(counts.values()):
        return "LOW"
    return "NONE"


def _native_known_at_ready(row: Mapping[str, Any]) -> bool:
    semantics = str(row.get("known_at_semantics", "")).lower()
    pit_policy = str(row.get("pit_policy", "")).upper()
    return (
        "native" in semantics
        and "pit approximation" not in semantics
        and pit_policy == "STRICT_PIT_READY"
    )


def _next_decision_by_record_key(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, int], datetime]:
    grouped: dict[tuple[str, str], list[tuple[int, Mapping[str, Any], datetime]]] = {}
    for index, row in enumerate(rows):
        source_id = _string(row.get("source_id"))
        asset = _string(row.get("target_asset"))
        source_date = _row_date(row)
        if not source_date:
            continue
        as_of_dt = _parse_datetime(row.get("as_of_timestamp")) or _date_to_utc_midnight(
            source_date
        )
        if not as_of_dt:
            continue
        decision_dt = _next_business_day(as_of_dt)
        grouped.setdefault((source_id, asset), []).append((index, row, decision_dt))
    result: dict[tuple[str, str, int], datetime] = {}
    for (_source_id, _asset), values in grouped.items():
        ordered = sorted(values, key=lambda item: item[2])
        for pos, (index, row, decision_dt) in enumerate(ordered):
            if pos + 1 < len(ordered):
                result[_record_key(row, index)] = ordered[pos + 1][2]
            else:
                result[_record_key(row, index)] = _next_business_day(decision_dt)
    return result


def _row_date(row: Mapping[str, Any]) -> date | None:
    value = _string(row.get("date")) or _string(row.get("valid_from"))
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _date_to_utc_midnight(value: date | None) -> datetime | None:
    if not value:
        return None
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _parse_datetime(value: Any) -> datetime | None:
    text = _string(value)
    if not text:
        return None
    if len(text) == 10:
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError:
            return None
        return _date_to_utc_midnight(parsed_date)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _next_business_day(value: datetime) -> datetime:
    candidate = value + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate.replace(hour=0, minute=0, second=0, microsecond=0)


def _format_datetime(value: datetime | None) -> str:
    if not value:
        return ""
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _record_id(row: Mapping[str, Any], index: int) -> str:
    return "|".join(
        [
            _string(row.get("baseline_id")),
            _string(row.get("source_id")),
            _string(row.get("date")),
            _string(row.get("target_asset")),
            str(index),
        ]
    )


def _record_key(row: Mapping[str, Any], index: int) -> tuple[str, str, int]:
    return (_string(row.get("source_id")), _string(row.get("target_asset")), index)


def _as_of_derivation_mode(row: Mapping[str, Any]) -> str:
    if not _is_missing(row.get("as_of_timestamp")):
        return "artifact_metadata_mapping"
    if not _is_missing(row.get("date")):
        return "filename_date_mapping"
    if not _is_missing(row.get("generated_at")):
        return "artifact_metadata_mapping"
    return "unresolved"


def _timestamp_derivation_mode_summary(derivation: Mapping[str, Any]) -> str:
    modes = {
        str(derivation.get("as_of_timestamp_derivation_mode")),
        str(derivation.get("decision_timestamp_derivation_mode")),
        str(derivation.get("valid_from_derivation_mode")),
        str(derivation.get("valid_until_derivation_mode")),
        str(derivation.get("rebalance_timestamp_derivation_mode")),
    }
    modes.discard("")
    if modes == {"native_timestamp_copy"}:
        return "native_timestamp_copy"
    if "unresolved" in modes:
        return "unresolved"
    return "deterministic_latency_policy_with_validity_window_policy"


def _field_status(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    if not rows:
        return "FAIL"
    missing = _missing_count(rows, field)
    if missing == 0:
        return "PASS"
    if missing < len(rows):
        return "PASS_WITH_WARNINGS"
    return "FAIL"


def _route_caveat(status: str) -> str:
    if status == "TIMESTAMP_REMEDIATED_READY_FOR_2331":
        return "STRICT_PIT_TIMESTAMP_RECHECK_REQUIRED"
    if status == "TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331":
        return "PIT_APPROXIMATION_AND_ALIGNMENT_RECHECK_REQUIRED"
    if status == "TIMESTAMP_REMEDIATION_SOURCE_GENERATION_REQUIRED":
        return "NATIVE_DYNAMIC_TARGET_SOURCE_GENERATION_REQUIRED"
    return "CONTINUE_STATIC_BASELINE_ONLY"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in {"none", "null", "nan"}
    return False


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _iso_date_value(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return _string(value)


def _safety_subset() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_timestamp_remediation_report(
    summary: Mapping[str, Any],
    gap_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    pit_caveat_report: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Target Baseline Timestamp Remediation Report",
        "",
        f"- market_regime: `{summary.get('market_regime')}`",
        "- actual_requested_date_range: `prior research outputs only`",
        f"- source wrapper records: `{summary.get('wrapper_input_record_count')}`",
        (
            "- timestamp-remediated wrapper records: "
            f"`{summary.get('timestamp_remediated_wrapper_record_count')}`"
        ),
        f"- wrapper_validation_status: `{wrapper_validation.get('validation_status')}`",
        f"- known_at_policy: `{summary.get('known_at_policy')}`",
        f"- readiness_status: `{readiness.get('readiness_status')}`",
        f"- 2331_allowed: `{readiness.get('2331_allowed')}`",
        f"- next_task: `{task_route.get('next_task')}`",
        "",
        (
            "TRADING-2329 found 4 remediable sources, but timestamp / known-at / "
            "validity semantics still blocked direct dynamic dry-run entry."
        ),
        "",
        "## Timestamp Gaps",
        "",
    ]
    for row in gap_rows:
        lines.append(
            f"- `{row.get('source_id')}` severity=`{row.get('timestamp_gap_severity')}` "
            f"strict_pit_blocked=`{row.get('strict_pit_blocked')}`"
        )
    lines.extend(
        [
            "",
            "## PIT Caveat",
            "",
            f"- strict_pit_ready: `{pit_caveat_report.get('strict_pit_ready')}`",
            f"- pit_approximation_ready: `{pit_caveat_report.get('pit_approximation_ready')}`",
            f"- blocked_usage: `{', '.join(pit_caveat_report.get('blocked_usage', []))}`",
            "",
            "No promotion, paper-shadow, production or broker action is allowed.",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_gap_matrix_doc(
    summary: Mapping[str, Any],
    gap_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Target Timestamp Gap Matrix",
        "",
        f"- row_count: `{len(gap_rows)}`",
        f"- wrapper_record_count: `{summary.get('wrapper_input_record_count')}`",
        "",
    ]
    for row in gap_rows:
        lines.append(
            f"- `{row.get('baseline_id')}`: severity=`{row.get('timestamp_gap_severity')}`, "
            f"warnings=`{', '.join(row.get('warning_fields', []))}`"
        )
    return "\n".join(lines) + "\n"


def _render_known_at_doc(known_at_report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Target Known-At Semantics Report",
            "",
            f"- baseline_id: `{known_at_report.get('baseline_id')}`",
            f"- source_id: `{known_at_report.get('source_id')}`",
            f"- known_at_policy: `{known_at_report.get('known_at_policy')}`",
            f"- strict_pit_ready: `{known_at_report.get('strict_pit_ready')}`",
            f"- pit_approximation_ready: `{known_at_report.get('pit_approximation_ready')}`",
            f"- latency_assumption: `{known_at_report.get('latency_assumption')}`",
            "",
            "Non-native known-at semantics remain research-only and require 2331 recheck.",
        ]
    ) + "\n"


def _render_validity_doc(validity_report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Target Validity Window Remediation Report",
            "",
            f"- baseline_id: `{validity_report.get('baseline_id')}`",
            f"- source_id: `{validity_report.get('source_id')}`",
            f"- validity_policy: `{validity_report.get('validity_policy')}`",
            f"- validity_gap_count: `{validity_report.get('validity_gap_count')}`",
            f"- validity_remediated_count: `{validity_report.get('validity_remediated_count')}`",
            f"- validity_blocked_count: `{validity_report.get('validity_blocked_count')}`",
            "",
            "Derived validity windows are not trading instructions.",
        ]
    ) + "\n"


def _render_2331_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Target 2331 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- 2331_allowed: `{readiness.get('2331_allowed')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            f"- caveat: `{task_route.get('caveat')}`",
            "",
            (
                "TRADING-2331 must recheck wrapper readiness before any dynamic target "
                "dry-run. Promotion, paper-shadow, production and broker action remain "
                "blocked."
            ),
        ]
    ) + "\n"
