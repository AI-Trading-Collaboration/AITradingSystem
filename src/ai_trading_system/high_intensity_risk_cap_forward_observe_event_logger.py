from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
)
from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_READINESS_ROOT,
)
from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
)
from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DYNAMIC_DRY_RUN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_THRESHOLD_SELECTION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    load_trading_2330_timestamp_context,
    load_trading_2331_readiness_context,
    load_trading_2332_dynamic_dry_run_context,
    load_trading_2333_dynamic_diagnostics_context,
    load_trading_2334_forward_observe_plan_outputs,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2336_HIGH_INTENSITY_RISK_CAP_FORWARD_OBSERVE_EVENT_LOGGER"
REPORT_TYPE = "high_intensity_risk_cap_forward_observe_event_logger"
ARTIFACT_ROLE = "high_intensity_risk_cap_forward_observe_event_logger"
MODE = "observe_event_logger"
STATUS = "HIGH_INTENSITY_EVENT_LOGGER_READY_PROMOTION_BLOCKED"
STATUS_WITH_WARNINGS = (
    "HIGH_INTENSITY_EVENT_LOGGER_READY_WITH_WARNINGS_PROMOTION_BLOCKED"
)
ZERO_EVENT_STATUS = "EVENT_LOGGER_READY_ZERO_EVENTS"
BLOCKED_STATUS = "HIGH_INTENSITY_EVENT_LOGGER_BLOCKED_PROMOTION_BLOCKED"
DATA_VALIDATION_POLICY = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_RESEARCH_ARTIFACTS_ONLY_NO_OUTCOME_BINDING"
)

EXPECTED_SELECTED_RULE_ID = "COMPOSITE_HIGH_INTENSITY_RULE"
EXPECTED_2336_TASK = (
    "TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger"
)
NEXT_OUTCOME_BINDER_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder"
)
NEXT_ZERO_EVENT_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Zero_Event_Readiness_Review"
)
NEXT_DATA_CONTRACT_REMEDIATION_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Data_Contract_Remediation"
)
NEXT_TRIGGER_SOURCE_REMEDIATION_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Trigger_Source_Remediation"
)
NEXT_CLUSTERING_REMEDIATION_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Event_Clustering_Remediation"
)
NEXT_ARCHIVE_TASK = "TRADING-2337_Archive_High_Intensity_Risk_Cap_Observe_Line"

KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
LATENCY_POLICY = "NEXT_TRADING_DAY_DECISION"
PIT_POLICY = "PIT_APPROXIMATION_READY"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_SELECTED_RULE_INPUT_FIELDS = [
    "risk_cap_triggered",
    "risk_cap_intensity",
    "risk_cap_score",
    "scope_active",
    "signal_direction",
    "as_of_timestamp",
    "decision_timestamp",
    "known_at_policy",
    "pit_policy",
]

# TRADING-2335 density guardrail uses a monthly concentration cap of three
# historical events; 2336 inherits the same review boundary for cluster tagging.
MONTHLY_EVENT_GUARDRAIL = 3
CLUSTER_CONTINUATION_GAP_DAYS = 3
OUTCOME_HORIZON_DAYS = {"1d": 1, "5d": 5, "10d": 10, "20d": 20}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_event_logger_only": True,
    "forward_observe_line": True,
    "manual_review_only": True,
    "runtime_observe_started": False,
    "runtime_scheduler_enabled": False,
    "outcome_binding_executed": False,
    "automatic_exposure_cap_allowed": False,
    "target_weight_action_allowed": False,
    "rebalance_instruction_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "real_portfolio_effect": "none",
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}


class HighIntensityEventLoggerError(ValueError):
    pass


def run_high_intensity_risk_cap_forward_observe_event_logger(
    *,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    dynamic_diagnostics_dir: Path = DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
    readiness_dir: Path = DEFAULT_READINESS_ROOT,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityEventLoggerError(
            f"high-intensity event logger only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_event_logger_inputs(
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
    )
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    trigger_source_rows = records(
        mapping(inputs["dynamic_dry_run"]["trigger_alignment"]).get("rows")
    )
    if not trigger_source_rows:
        raise HighIntensityEventLoggerError(
            "BLOCKED_NO_TRIGGER_SOURCE: dynamic trigger alignment source is empty"
        )

    field_coverage = validate_selected_rule_input_fields(
        trigger_source_rows=trigger_source_rows,
        selected_rule=selected_rule,
        dynamic_dry_run=inputs["dynamic_dry_run"],
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    trigger_day_rows = build_high_intensity_observe_trigger_day_log(
        trigger_source_rows=trigger_source_rows,
        selected_rule=selected_rule,
        dynamic_dry_run=inputs["dynamic_dry_run"],
    )
    cluster_registry, event_log, trigger_day_rows = (
        build_high_intensity_observe_event_log(
            trigger_day_rows=trigger_day_rows,
            selected_rule=selected_rule,
        )
    )
    monthly_report = build_high_intensity_monthly_concentration_report(
        cluster_registry=cluster_registry,
        threshold_selection=inputs["threshold_selection"],
    )
    pending_registry = build_high_intensity_pending_outcome_registry(
        event_log=event_log,
    )
    outcome_schedule = build_high_intensity_outcome_collection_schedule(
        pending_registry=pending_registry,
        as_of_date=generated_at.date(),
    )
    manual_review_queue = build_high_intensity_manual_review_event_queue(
        event_log=event_log,
        cluster_registry=cluster_registry,
        monthly_report=monthly_report,
    )
    execution_report = build_high_intensity_selected_rule_execution_report(
        selected_rule=selected_rule,
        threshold_selection=inputs["threshold_selection"],
        trigger_source_rows=trigger_source_rows,
        field_coverage=field_coverage,
        trigger_day_rows=trigger_day_rows,
        event_log=event_log,
        cluster_registry=cluster_registry,
        monthly_report=monthly_report,
    )
    data_quality_report = build_high_intensity_event_logger_data_quality_report(
        execution_report=execution_report,
        monthly_report=monthly_report,
        trigger_day_rows=trigger_day_rows,
        event_log=event_log,
        cluster_registry=cluster_registry,
    )
    interpretation_boundary = (
        build_high_intensity_event_logger_interpretation_boundary(
            execution_report=execution_report,
            data_quality_report=data_quality_report,
        )
    )
    readiness = build_high_intensity_2337_readiness_checklist(
        execution_report=execution_report,
        data_quality_report=data_quality_report,
        monthly_report=monthly_report,
    )
    task_route = build_high_intensity_2337_task_route(readiness)
    safety_boundary = build_high_intensity_event_logger_safety_boundary(
        generated_at=generated_at,
        execution_report=execution_report,
        readiness=readiness,
        task_route=task_route,
    )
    summary = build_high_intensity_event_logger_summary(
        generated_at=generated_at,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        execution_report=execution_report,
        monthly_report=monthly_report,
        data_quality_report=data_quality_report,
        readiness=readiness,
        task_route=task_route,
    )
    artifact_paths = write_high_intensity_event_logger_outputs(
        paths=paths,
        summary=summary,
        execution_report=execution_report,
        trigger_day_rows=trigger_day_rows,
        event_log=event_log,
        cluster_registry=cluster_registry,
        monthly_report=monthly_report,
        pending_registry=pending_registry,
        outcome_schedule=outcome_schedule,
        manual_review_queue=manual_review_queue,
        data_quality_report=data_quality_report,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_event_logger_inputs(
    *,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    dynamic_diagnostics_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
) -> dict[str, Any]:
    return {
        "threshold_selection": load_trading_2335_threshold_selection_outputs(
            threshold_selection_dir
        ),
        "forward_observe_plan": load_trading_2334_forward_observe_plan_outputs(
            forward_observe_plan_dir
        ),
        "dynamic_diagnostics": load_trading_2333_dynamic_diagnostics_context(
            dynamic_diagnostics_dir
        ),
        "dynamic_dry_run": load_trading_2332_dynamic_dry_run_context(
            dynamic_dry_run_dir
        ),
        "readiness": load_trading_2331_readiness_context(readiness_dir),
        "timestamp_remediation": load_trading_2330_timestamp_context(
            timestamp_remediation_dir
        ),
    }


def load_trading_2335_threshold_selection_outputs(
    threshold_selection_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": threshold_selection_dir
        / "high_intensity_threshold_selection_summary.json",
        "candidate_scoring": threshold_selection_dir
        / "high_intensity_threshold_candidate_scoring_matrix.json",
        "density_guardrail": threshold_selection_dir
        / "high_intensity_trigger_density_guardrail.json",
        "decision_matrix": threshold_selection_dir
        / "high_intensity_threshold_selection_decision_matrix.json",
        "selected_rule": threshold_selection_dir
        / "high_intensity_selected_trigger_rule.json",
        "selected_contract": threshold_selection_dir
        / "high_intensity_selected_trigger_contract.json",
        "caveat_report": threshold_selection_dir
        / "high_intensity_threshold_selection_caveat_report.json",
        "event_logger_input_contract": threshold_selection_dir
        / "high_intensity_event_logger_input_contract.json",
        "backtest_context": threshold_selection_dir
        / "high_intensity_selected_rule_backtest_context.json",
        "false_warning_context": threshold_selection_dir
        / "high_intensity_selected_rule_false_warning_context.json",
        "missed_stress_context": threshold_selection_dir
        / "high_intensity_selected_rule_missed_stress_context.json",
        "manual_review_boundary": threshold_selection_dir
        / "high_intensity_selected_rule_manual_review_boundary.json",
        "readiness": threshold_selection_dir
        / "high_intensity_2336_readiness_checklist.json",
        "task_route": threshold_selection_dir / "high_intensity_2336_task_route.json",
        "safety_boundary": threshold_selection_dir
        / "high_intensity_threshold_selection_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2335 threshold selection")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2335 {key}", payload)

    summary = mapping(payloads["summary"])
    selected_rule = mapping(payloads["selected_rule"])
    density_guardrail = mapping(payloads["density_guardrail"])
    event_logger_contract = mapping(payloads["event_logger_input_contract"])
    route = mapping(payloads["task_route"])
    readiness = mapping(payloads["readiness"])

    if selected_rule.get("selected_rule_id") != EXPECTED_SELECTED_RULE_ID:
        raise HighIntensityEventLoggerError(
            f"TRADING-2336 requires selected_rule_id={EXPECTED_SELECTED_RULE_ID}"
        )
    if summary.get("next_task") != EXPECTED_2336_TASK:
        raise HighIntensityEventLoggerError(
            f"TRADING-2336 requires threshold summary next_task {EXPECTED_2336_TASK}"
        )
    if route.get("next_task") != EXPECTED_2336_TASK:
        raise HighIntensityEventLoggerError(
            f"TRADING-2336 requires threshold route next_task {EXPECTED_2336_TASK}"
        )
    if summary.get("runtime_observe_started") is True:
        raise HighIntensityEventLoggerError(
            "TRADING-2336 requires runtime_observe_started=false from TRADING-2335"
        )
    if event_logger_contract.get("runtime_observe_allowed") is not True:
        raise HighIntensityEventLoggerError(
            "TRADING-2336 requires TRADING-2335 event logger contract"
        )
    if density_guardrail.get("density_guardrail_status") not in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }:
        raise HighIntensityEventLoggerError(
            "TRADING-2336 requires PASS/PASS_WITH_WARNINGS density guardrail"
        )
    if readiness.get("readiness_status") not in {
        "READY_FOR_2336_EVENT_LOGGER",
        "READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT",
    }:
        raise HighIntensityEventLoggerError(
            "TRADING-2336 requires threshold readiness for event logger"
        )
    return {
        "source_dir": str(threshold_selection_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def validate_selected_rule_input_fields(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    del selected_rule
    dry_summary = mapping(dynamic_dry_run.get("summary"))
    pit_boundary = mapping(dynamic_dry_run.get("pit_boundary"))
    coverage: list[dict[str, Any]] = []
    missing: list[str] = []
    row_count = len(trigger_source_rows)
    for field in REQUIRED_SELECTED_RULE_INPUT_FIELDS:
        present_count = sum(
            1 for row in trigger_source_rows if field in row and row.get(field) != ""
        )
        derived_source = ""
        status = "PRESENT"
        if present_count == 0:
            derived_source = _derived_field_source(field, dry_summary, pit_boundary)
            if derived_source:
                status = "PRESENT_DERIVED"
            else:
                status = "MISSING"
                missing.append(field)
        coverage.append(
            {
                "field": field,
                "status": status,
                "present_count": present_count,
                "source_record_count": row_count,
                "derived_source": derived_source,
            }
        )
    missing_base = [
        field
        for field in ("date", "target_asset")
        if not all(field in row and row.get(field) not in {"", None} for row in trigger_source_rows)
    ]
    if missing or missing_base:
        missing_text = ", ".join([*missing_base, *missing])
        raise HighIntensityEventLoggerError(
            "BLOCKED_MISSING_SELECTED_RULE_INPUT_FIELDS: "
            f"missing/underived fields {missing_text}"
        )
    return coverage


def build_high_intensity_observe_trigger_day_log(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    dry_summary = mapping(dynamic_dry_run.get("summary"))
    pit_boundary = mapping(dynamic_dry_run.get("pit_boundary"))
    source_path = mapping(dynamic_dry_run.get("paths")).get("trigger_alignment", "")
    source_artifact_hash = _file_hash(Path(str(source_path)))
    selected_rule_id = str(selected_rule.get("selected_rule_id", ""))
    selected_rule_version = str(selected_rule.get("selected_rule_version", ""))
    selected_rule_hash = _hash_payload(selected_rule)
    threshold = _selected_rule_threshold(selected_rule)
    rows: list[dict[str, Any]] = []
    for index, source_row in enumerate(trigger_source_rows):
        if not selected_rule_matches(source_row, selected_rule):
            continue
        source_record_id = str(
            source_row.get("source_record_id")
            or f"dynamic_trigger_alignment_row_{index:06d}"
        )
        event_date = str(source_row.get("date", ""))
        target_asset = str(source_row.get("target_asset", ""))
        row = {
            "schema_version": f"{REPORT_TYPE}.trigger_day.v1",
            "task_id": TASK_ID,
            "trigger_day_id": _short_id(
                "hitd",
                selected_rule_id,
                target_asset,
                event_date,
                source_record_id,
            ),
            "date": event_date,
            "target_asset": target_asset,
            "selected_rule_id": selected_rule_id,
            "selected_rule_version": selected_rule_version,
            "selected_rule_hash": selected_rule_hash,
            "risk_cap_triggered": _truthy(source_row.get("risk_cap_triggered")),
            "risk_cap_intensity": source_row.get("risk_cap_intensity", ""),
            "risk_cap_score": round_float(source_row.get("risk_cap_score")),
            "scope_active": _truthy(source_row.get("scope_active")),
            "signal_direction": source_row.get("signal_direction", ""),
            "high_intensity_triggered": True,
            "high_intensity_reason": (
                f"{selected_rule_id}: risk_cap_score >= {round_float(threshold)} "
                "with active risk-cap defensive direction"
            ),
            "as_of_timestamp": _as_of_timestamp(source_row),
            "decision_timestamp": _decision_timestamp(source_row),
            "known_at_policy": _known_at_policy(selected_rule, dry_summary, pit_boundary),
            "latency_policy": _latency_policy(selected_rule),
            "pit_policy": _pit_policy(selected_rule, dry_summary, pit_boundary),
            "source_record_id": source_record_id,
            "source_artifact_hash": source_artifact_hash,
            "trigger_day_status": "TRIGGER_DAY_ACTIVE",
            "event_status": "OBSERVE_PENDING",
            "manual_review_observation_flag": True,
            **SAFETY_FIELDS,
        }
        rows.append(clean_for_yaml(row))
    return sorted(rows, key=lambda item: (str(item["date"]), str(item["target_asset"])))


def selected_rule_matches(
    source_row: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
) -> bool:
    threshold = _selected_rule_threshold(selected_rule)
    return (
        _truthy(source_row.get("risk_cap_triggered"))
        and _truthy(source_row.get("scope_active"))
        and to_float(source_row.get("risk_cap_score")) >= threshold
        and _is_defensive_risk_cap_direction(source_row.get("signal_direction"))
    )


def build_high_intensity_observe_event_log(
    *,
    trigger_day_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    del selected_rule
    if not trigger_day_rows:
        return [], [], []

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trigger_day_rows:
        key = (str(row.get("selected_rule_id", "")), str(row.get("target_asset", "")))
        grouped[key].append(dict(row))

    clusters: list[list[dict[str, Any]]] = []
    for rows in grouped.values():
        current: list[dict[str, Any]] = []
        previous_date: date | None = None
        for row in sorted(rows, key=lambda item: str(item.get("date", ""))):
            row_date = _parse_date(str(row.get("date", "")))
            if (
                previous_date is None
                or (row_date - previous_date).days > CLUSTER_CONTINUATION_GAP_DAYS
            ):
                if current:
                    clusters.append(current)
                current = [row]
            else:
                current.append(row)
            previous_date = row_date
        if current:
            clusters.append(current)

    cluster_registry: list[dict[str, Any]] = []
    event_log: list[dict[str, Any]] = []
    enriched_trigger_rows: list[dict[str, Any]] = []
    cluster_records = [_cluster_record(cluster) for cluster in clusters]
    monthly_counts = Counter(record["monthly_bucket"] for record in cluster_records)

    for cluster, cluster_record in zip(clusters, cluster_records, strict=True):
        monthly_count = monthly_counts[cluster_record["monthly_bucket"]]
        warning = (
            "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
            if monthly_count > MONTHLY_EVENT_GUARDRAIL
            else ""
        )
        cluster_record = {
            **cluster_record,
            "monthly_event_count": monthly_count,
            "cluster_warning": warning,
            **SAFETY_FIELDS,
        }
        cluster_registry.append(clean_for_yaml(cluster_record))

        event_log.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.observe_event.v1",
                    "task_id": TASK_ID,
                    "event_id": cluster_record["primary_event_id"],
                    "event_date": cluster_record["cluster_start_date"],
                    "target_asset": cluster_record["target_asset"],
                    "selected_rule_id": cluster_record["selected_rule_id"],
                    "selected_rule_version": cluster[0].get(
                        "selected_rule_version",
                        "",
                    ),
                    "event_cluster_id": cluster_record["event_cluster_id"],
                    "cluster_start_date": cluster_record["cluster_start_date"],
                    "cluster_end_date": cluster_record["cluster_end_date"],
                    "cluster_active_days": cluster_record["cluster_active_days"],
                    "trigger_day_count": cluster_record["trigger_day_count"],
                    "consecutive_trigger_days": cluster_record[
                        "consecutive_trigger_days"
                    ],
                    "monthly_bucket": cluster_record["monthly_bucket"],
                    "monthly_event_count": monthly_count,
                    "high_intensity_triggered": True,
                    "high_intensity_reason": cluster[0].get(
                        "high_intensity_reason",
                        "",
                    ),
                    "risk_cap_score_max": cluster_record["risk_cap_score_max"],
                    "risk_cap_score_avg": cluster_record["risk_cap_score_avg"],
                    "risk_cap_intensity_max": cluster_record[
                        "risk_cap_intensity_max"
                    ],
                    "is_new_event": True,
                    "is_existing_cluster_continuation": False,
                    "event_status": "OBSERVE_PENDING",
                    "manual_review_observation_flag": True,
                    "allowed_usage": [
                        "research_only_forward_observe_event_logger",
                        "manual_review_observation_context",
                    ],
                    "blocked_usage": [
                        "target_weight_action",
                        "rebalance_instruction",
                        "paper_shadow",
                        "production",
                        "broker_action",
                    ],
                    "cluster_warning": warning,
                    **SAFETY_FIELDS,
                }
            )
        )

        for index, row in enumerate(cluster):
            enriched_trigger_rows.append(
                clean_for_yaml(
                    {
                        **row,
                        "event_cluster_id": cluster_record["event_cluster_id"],
                        "cluster_start_date": cluster_record["cluster_start_date"],
                        "cluster_active_days": cluster_record["cluster_active_days"],
                        "is_new_event": index == 0,
                        "is_existing_cluster_continuation": index > 0,
                        "monthly_event_count": monthly_count,
                        "consecutive_trigger_days": cluster_record[
                            "consecutive_trigger_days"
                        ],
                    }
                )
            )

    return (
        sorted(cluster_registry, key=lambda item: str(item["cluster_start_date"])),
        sorted(event_log, key=lambda item: str(item["event_date"])),
        sorted(enriched_trigger_rows, key=lambda item: str(item["trigger_day_id"])),
    )


def build_high_intensity_monthly_concentration_report(
    *,
    cluster_registry: Sequence[Mapping[str, Any]],
    threshold_selection: Mapping[str, Any],
) -> dict[str, Any]:
    guardrail = mapping(threshold_selection.get("density_guardrail"))
    inherited_warnings = list(guardrail.get("density_guardrail_warnings", []))
    monthly_counts = Counter(str(row.get("monthly_bucket", "")) for row in cluster_registry)
    monthly_counts.pop("", None)
    max_monthly_event_count = max(monthly_counts.values(), default=0)
    generated_warning = (
        "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
        if max_monthly_event_count > MONTHLY_EVENT_GUARDRAIL
        else ""
    )
    warnings = sorted({warning for warning in [*inherited_warnings, generated_warning] if warning})
    status = "PASS_WITH_WARNINGS" if warnings else "PASS"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.monthly_concentration_report.v1",
            "task_id": TASK_ID,
            "monthly_event_counts": dict(sorted(monthly_counts.items())),
            "monthly_event_guardrail": MONTHLY_EVENT_GUARDRAIL,
            "observed_monthly_event_count_max": max_monthly_event_count,
            "inherited_2335_monthly_event_counts": guardrail.get(
                "monthly_event_counts",
                {},
            ),
            "inherited_2335_observed_monthly_event_count_max": guardrail.get(
                "observed_monthly_event_count_max",
                0,
            ),
            "inherited_2335_warning": "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
            in inherited_warnings,
            "monthly_concentration_status": status,
            "monthly_concentration_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_pending_outcome_registry(
    *,
    event_log: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in event_log:
        for horizon, horizon_days in OUTCOME_HORIZON_DAYS.items():
            event_date = _parse_date(str(event.get("event_date", "")))
            due_date = _add_business_days(event_date, horizon_days)
            rows.append(
                clean_for_yaml(
                    {
                        "schema_version": f"{REPORT_TYPE}.pending_outcome.v1",
                        "task_id": TASK_ID,
                        "pending_outcome_id": _short_id(
                            "hiout",
                            str(event.get("event_id", "")),
                            horizon,
                        ),
                        "event_id": event.get("event_id", ""),
                        "event_cluster_id": event.get("event_cluster_id", ""),
                        "event_date": event.get("event_date", ""),
                        "target_asset": event.get("target_asset", ""),
                        "selected_rule_id": event.get("selected_rule_id", ""),
                        "horizon": horizon,
                        "horizon_trading_days": horizon_days,
                        "outcome_due_date": due_date.isoformat(),
                        "outcome_status": "OUTCOME_PENDING",
                        "outcome_quality_status": "OUTCOME_PENDING",
                        "required_metrics": [
                            "forward_return",
                            "forward_max_drawdown",
                            "forward_min_return",
                            "forward_max_return",
                            "realized_volatility",
                            "stress_detected",
                            "rebound_detected",
                            "false_warning_candidate",
                            "missed_stress_candidate",
                            "missed_upside_candidate",
                            "downside_capture_candidate",
                            "manual_review_would_have_helped",
                        ],
                        "outcome_binding_executed": False,
                        "outcome_payload_bound": False,
                        "next_binding_task": NEXT_OUTCOME_BINDER_TASK,
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_high_intensity_outcome_collection_schedule(
    *,
    pending_registry: Sequence[Mapping[str, Any]],
    as_of_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pending in pending_registry:
        due_date = _parse_date(str(pending.get("outcome_due_date", "")))
        if due_date < as_of_date:
            schedule_status = "HISTORICAL_DUE"
        elif due_date == as_of_date:
            schedule_status = "DUE"
        else:
            schedule_status = "NOT_DUE"
        rows.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.outcome_schedule.v1",
                    "task_id": TASK_ID,
                    "pending_outcome_id": pending.get("pending_outcome_id", ""),
                    "event_id": pending.get("event_id", ""),
                    "event_cluster_id": pending.get("event_cluster_id", ""),
                    "event_date": pending.get("event_date", ""),
                    "target_asset": pending.get("target_asset", ""),
                    "horizon": pending.get("horizon", ""),
                    "outcome_due_date": pending.get("outcome_due_date", ""),
                    "schedule_status": schedule_status,
                    "outcome_status": pending.get("outcome_status", ""),
                    "collection_policy": (
                        "OUTCOME_BINDING_DEFERRED_TO_2337_NO_OUTCOME_DATA_BOUND"
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_manual_review_event_queue(
    *,
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    monthly_report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    cluster_by_id = {
        str(cluster.get("event_cluster_id", "")): cluster for cluster in cluster_registry
    }
    rows: list[dict[str, Any]] = []
    for event in event_log:
        cluster = cluster_by_id.get(str(event.get("event_cluster_id", "")), {})
        rows.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.manual_review_queue.v1",
                    "task_id": TASK_ID,
                    "manual_review_queue_id": _short_id(
                        "himrq",
                        str(event.get("event_id", "")),
                    ),
                    "event_id": event.get("event_id", ""),
                    "event_cluster_id": event.get("event_cluster_id", ""),
                    "event_date": event.get("event_date", ""),
                    "target_asset": event.get("target_asset", ""),
                    "selected_rule_id": event.get("selected_rule_id", ""),
                    "manual_review_observation_flag": True,
                    "manual_review_reason": (
                        "high-intensity observe event; review context only"
                    ),
                    "risk_warning_context": event.get("high_intensity_reason", ""),
                    "cluster_context": (
                        f"cluster_active_days={cluster.get('cluster_active_days', 0)}; "
                        f"trigger_day_count={cluster.get('trigger_day_count', 0)}; "
                        f"monthly_event_count={cluster.get('monthly_event_count', 0)}"
                    ),
                    "monthly_concentration_status": monthly_report.get(
                        "monthly_concentration_status",
                        "",
                    ),
                    "forbidden_outputs": [
                        "target_weight",
                        "rebalance_instruction",
                        "buy_signal",
                        "sell_signal",
                        "paper_shadow_ready",
                        "production_ready",
                        "broker_action",
                    ],
                    "position_instruction_generated": False,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_selected_rule_execution_report(
    *,
    selected_rule: Mapping[str, Any],
    threshold_selection: Mapping[str, Any],
    trigger_source_rows: Sequence[Mapping[str, Any]],
    field_coverage: Sequence[Mapping[str, Any]],
    trigger_day_rows: Sequence[Mapping[str, Any]],
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    monthly_report: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule_id = str(selected_rule.get("selected_rule_id", ""))
    monthly_warnings = list(monthly_report.get("monthly_concentration_warnings", []))
    if not event_log:
        status = "ZERO_EVENTS"
    elif monthly_warnings:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    threshold_guardrail = mapping(threshold_selection.get("density_guardrail"))
    trigger_source_record_count = len(trigger_source_rows)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.selected_rule_execution_report.v1",
            "task_id": TASK_ID,
            "selected_rule_id": selected_rule_id,
            "selected_rule_version": selected_rule.get("selected_rule_version", ""),
            "selected_rule_type": mapping(selected_rule.get("trigger_rule")).get(
                "threshold_type",
                "",
            ),
            "selected_rule_hash": _hash_payload(selected_rule),
            "source_signal_family": selected_rule.get("source_signal_family", ""),
            "required_input_fields": REQUIRED_SELECTED_RULE_INPUT_FIELDS,
            "input_field_coverage": list(field_coverage),
            "trigger_source_record_count": trigger_source_record_count,
            "eligible_record_count": trigger_source_record_count,
            "trigger_day_count": len(trigger_day_rows),
            "trigger_day_density": _rate(len(trigger_day_rows), trigger_source_record_count),
            "event_count_after_dedup": len(event_log),
            "event_density_after_dedup": _rate(len(event_log), trigger_source_record_count),
            "cluster_count": len(cluster_registry),
            "selected_threshold_density_from_2335": threshold_guardrail.get(
                "selected_threshold_density",
                0,
            ),
            "monthly_concentration_warning": bool(monthly_warnings),
            "monthly_concentration_warnings": monthly_warnings,
            "execution_status": status,
            "status": status,
            "zero_event_review_required": not event_log,
            "outcome_binding_executed": False,
            "target_weight_action_generated": False,
            "rebalance_instruction_generated": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_event_logger_data_quality_report(
    *,
    execution_report: Mapping[str, Any],
    monthly_report: Mapping[str, Any],
    trigger_day_rows: Sequence[Mapping[str, Any]],
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    duplicate_event_ids = _duplicate_count(row.get("event_id", "") for row in event_log)
    duplicate_cluster_ids = _duplicate_count(
        row.get("event_cluster_id", "") for row in cluster_registry
    )
    invalid_date_count = sum(
        1 for row in trigger_day_rows if not _date_is_valid(str(row.get("date", "")))
    )
    missing_cluster_link_count = sum(
        1 for row in trigger_day_rows if not row.get("event_cluster_id")
    )
    error_count = (
        duplicate_event_ids
        + duplicate_cluster_ids
        + invalid_date_count
        + missing_cluster_link_count
    )
    warnings = list(monthly_report.get("monthly_concentration_warnings", []))
    if execution_report.get("execution_status") == "ZERO_EVENTS":
        warnings.append("ZERO_EVENT_REVIEW_REQUIRED")
    warning_count = len(warnings)
    status = "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.data_quality_report.v1",
            "task_id": TASK_ID,
            "data_quality_status": status,
            "error_count": error_count,
            "warning_count": warning_count,
            "warnings": sorted(set(warnings)),
            "duplicate_event_id_count": duplicate_event_ids,
            "duplicate_cluster_id_count": duplicate_cluster_ids,
            "invalid_date_count": invalid_date_count,
            "missing_cluster_link_count": missing_cluster_link_count,
            "trigger_day_count": len(trigger_day_rows),
            "observe_event_count": len(event_log),
            "cluster_count": len(cluster_registry),
            "data_validation_policy": DATA_VALIDATION_POLICY,
            "aits_validate_data_executed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_event_logger_interpretation_boundary(
    *,
    execution_report: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "boundary_id": "HIGH_INTENSITY_EVENT_LOGGER_INTERPRETATION_BOUNDARY_V1",
            "logger_status": execution_report.get("execution_status", ""),
            "data_quality_status": data_quality_report.get("data_quality_status", ""),
            "observe_only": True,
            "outcome_binding_executed": False,
            "future_outcome_used_for_trigger_creation": False,
            "not_signal_validation": True,
            "not_target_weight_advice": True,
            "not_rebalance_instruction": True,
            "not_paper_shadow_ready": True,
            "not_production_ready": True,
            "schedule_due_date_policy": (
                "WEEKDAY_BUSINESS_DAY_APPROXIMATION_FOR_COLLECTION_QUEUE_ONLY"
            ),
            "next_required_step": NEXT_OUTCOME_BINDER_TASK,
            "allowed_usage": [
                "research_only_event_inventory",
                "manual_review_observation_context",
                "pending_outcome_registry",
            ],
            "blocked_usage": [
                "target_weight_action",
                "rebalance_instruction",
                "paper_shadow",
                "production",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2337_readiness_checklist(
    *,
    execution_report: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    monthly_report: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if data_quality_report.get("data_quality_status") == "FAIL":
        blockers.append("EVENT_LOGGER_DATA_QUALITY_FAIL")
    if execution_report.get("execution_status") == "ZERO_EVENTS":
        warnings.append("ZERO_EVENT_REVIEW_REQUIRED")
    warnings.extend(monthly_report.get("monthly_concentration_warnings", []))

    if blockers:
        readiness_status = "EVENT_LOGGER_BLOCKED"
    elif execution_report.get("execution_status") == "ZERO_EVENTS":
        readiness_status = "ZERO_EVENT_REVIEW_REQUIRED"
    elif warnings:
        readiness_status = "READY_FOR_2337_OUTCOME_BINDER_WITH_WARNINGS"
    else:
        readiness_status = "READY_FOR_2337_OUTCOME_BINDER"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2337_readiness_checklist.v1",
            "task_id": TASK_ID,
            "readiness_status": readiness_status,
            "blockers": blockers,
            "warnings": sorted(set(warnings)),
            "event_logger_ready": readiness_status.startswith("READY_FOR_2337"),
            "zero_event_review_required": readiness_status
            == "ZERO_EVENT_REVIEW_REQUIRED",
            "data_contract_remediation_required": False,
            "clustering_remediation_required": False,
            "outcome_binding_executed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2337_task_route(
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    if status in {
        "READY_FOR_2337_OUTCOME_BINDER",
        "READY_FOR_2337_OUTCOME_BINDER_WITH_WARNINGS",
    }:
        next_task = NEXT_OUTCOME_BINDER_TASK
    elif status == "ZERO_EVENT_REVIEW_REQUIRED":
        next_task = NEXT_ZERO_EVENT_TASK
    elif status == "DATA_CONTRACT_REMEDIATION_REQUIRED":
        next_task = NEXT_DATA_CONTRACT_REMEDIATION_TASK
    elif status == "EVENT_CLUSTERING_REMEDIATION_REQUIRED":
        next_task = NEXT_CLUSTERING_REMEDIATION_TASK
    elif status == "EVENT_LOGGER_BLOCKED":
        next_task = NEXT_ARCHIVE_TASK
    else:
        next_task = NEXT_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2337_task_route.v1",
            "task_id": TASK_ID,
            "readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                NEXT_OUTCOME_BINDER_TASK,
                NEXT_ZERO_EVENT_TASK,
                NEXT_DATA_CONTRACT_REMEDIATION_TASK,
                NEXT_CLUSTERING_REMEDIATION_TASK,
                NEXT_ARCHIVE_TASK,
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "promotion_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "broker_action",
                }
            },
        }
    )


def build_high_intensity_event_logger_safety_boundary(
    *,
    generated_at: datetime,
    execution_report: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "execution_status": execution_report.get("execution_status", ""),
            "readiness_status": readiness.get("readiness_status", ""),
            "next_task": task_route.get("next_task", ""),
            "manual_review_observation_flag_allowed": True,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "outcome_binding_executed": False,
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "target_weight_action_allowed",
                    "rebalance_instruction_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "broker_action",
                    "outcome_binding_executed",
                }
            },
        }
    )


def build_high_intensity_event_logger_summary(
    *,
    generated_at: datetime,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    dynamic_diagnostics_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    execution_report: Mapping[str, Any],
    monthly_report: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    if execution_report.get("execution_status") == "ZERO_EVENTS":
        status = ZERO_EVENT_STATUS
    elif data_quality_report.get("data_quality_status") == "PASS_WITH_WARNINGS":
        status = STATUS_WITH_WARNINGS
    elif data_quality_report.get("data_quality_status") == "FAIL":
        status = BLOCKED_STATUS
    else:
        status = STATUS
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Forward Observe Event Logger",
            "mode": MODE,
            "status": status,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "dynamic_diagnostics_dir": str(dynamic_diagnostics_dir),
            "readiness_dir": str(readiness_dir),
            "timestamp_remediation_dir": str(timestamp_remediation_dir),
            "selected_rule_id": execution_report.get("selected_rule_id", ""),
            "selected_rule_version": execution_report.get(
                "selected_rule_version",
                "",
            ),
            "trigger_source_record_count": execution_report.get(
                "trigger_source_record_count",
                0,
            ),
            "trigger_day_count": execution_report.get("trigger_day_count", 0),
            "trigger_day_density": execution_report.get("trigger_day_density", 0),
            "event_count_after_dedup": execution_report.get(
                "event_count_after_dedup",
                0,
            ),
            "event_density_after_dedup": execution_report.get(
                "event_density_after_dedup",
                0,
            ),
            "cluster_count": execution_report.get("cluster_count", 0),
            "monthly_concentration_status": monthly_report.get(
                "monthly_concentration_status",
                "",
            ),
            "monthly_concentration_warnings": monthly_report.get(
                "monthly_concentration_warnings",
                [],
            ),
            "data_quality_status": data_quality_report.get(
                "data_quality_status",
                "",
            ),
            "data_validation_policy": DATA_VALIDATION_POLICY,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_applicability": "not_applicable",
            "aits_validate_data_executed": False,
            "event_logger_status": execution_report.get("execution_status", ""),
            "readiness_status": readiness.get("readiness_status", ""),
            "next_task": task_route.get("next_task", ""),
            "runtime_observe_started": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_observation_flag_generated": True,
            "outcome_binding_executed": False,
            "target_weight_action_generated": False,
            "rebalance_instruction_generated": False,
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "runtime_observe_started",
                    "promotion_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "broker_action",
                    "outcome_binding_executed",
                    "target_weight_generated",
                    "rebalance_instruction_generated",
                }
            },
        }
    )


def write_high_intensity_event_logger_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    execution_report: Mapping[str, Any],
    trigger_day_rows: Sequence[Mapping[str, Any]],
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    monthly_report: Mapping[str, Any],
    pending_registry: Sequence[Mapping[str, Any]],
    outcome_schedule: Sequence[Mapping[str, Any]],
    manual_review_queue: Sequence[Mapping[str, Any]],
    data_quality_report: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    outputs: list[Any] = [
        summary,
        execution_report,
        *trigger_day_rows,
        *event_log,
        *cluster_registry,
        monthly_report,
        *pending_registry,
        *outcome_schedule,
        *manual_review_queue,
        data_quality_report,
        interpretation_boundary,
        readiness,
        task_route,
        safety_boundary,
    ]
    for index, payload in enumerate(outputs):
        _validate_no_unsafe_fields(f"TRADING-2336 output {index}", payload)

    write_json(paths["summary"], dict(summary))
    write_json(paths["execution_report"], dict(execution_report))
    _write_rows_artifacts(
        paths["trigger_day_log_json"],
        paths["trigger_day_log_csv"],
        f"{REPORT_TYPE}.trigger_day_log.v1",
        trigger_day_rows,
    )
    _write_rows_artifacts(
        paths["event_log_json"],
        paths["event_log_csv"],
        f"{REPORT_TYPE}.observe_event_log.v1",
        event_log,
    )
    _write_rows_artifacts(
        paths["cluster_registry_json"],
        paths["cluster_registry_csv"],
        f"{REPORT_TYPE}.cluster_registry.v1",
        cluster_registry,
    )
    write_json(paths["monthly_report"], dict(monthly_report))
    _write_rows_artifacts(
        paths["pending_registry_json"],
        paths["pending_registry_csv"],
        f"{REPORT_TYPE}.pending_outcome_registry.v1",
        pending_registry,
    )
    _write_rows_artifacts(
        paths["outcome_schedule_json"],
        paths["outcome_schedule_csv"],
        f"{REPORT_TYPE}.outcome_collection_schedule.v1",
        outcome_schedule,
    )
    _write_rows_artifacts(
        paths["manual_review_queue_json"],
        paths["manual_review_queue_csv"],
        f"{REPORT_TYPE}.manual_review_event_queue.v1",
        manual_review_queue,
    )
    write_json(paths["data_quality_report"], dict(data_quality_report))
    write_json(paths["interpretation_boundary"], dict(interpretation_boundary))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))

    write_markdown(paths["main_doc"], _render_main_doc(summary, execution_report))
    write_markdown(paths["schema_doc"], _render_schema_usage_doc())
    write_markdown(
        paths["clustering_doc"],
        _render_clustering_doc(monthly_report=monthly_report),
    )
    write_markdown(
        paths["pending_registry_doc"],
        _render_pending_registry_doc(summary=summary),
    )
    write_markdown(
        paths["readiness_route_doc"],
        _render_readiness_route_doc(readiness=readiness, task_route=task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _write_rows_artifacts(
    json_path: Path,
    csv_path: Path,
    schema_version: str,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    write_json(
        json_path,
        {
            "schema_version": schema_version,
            "task_id": TASK_ID,
            "row_count": len(rows),
            "rows": list(rows),
            **SAFETY_FIELDS,
        },
    )
    write_csv_rows(csv_path, rows)


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_event_logger_summary.json",
        "execution_report": output_dir
        / "high_intensity_selected_rule_execution_report.json",
        "trigger_day_log_json": output_dir
        / "high_intensity_observe_trigger_day_log.json",
        "trigger_day_log_csv": output_dir
        / "high_intensity_observe_trigger_day_log.csv",
        "event_log_json": output_dir / "high_intensity_observe_event_log.json",
        "event_log_csv": output_dir / "high_intensity_observe_event_log.csv",
        "cluster_registry_json": output_dir
        / "high_intensity_observe_event_cluster_registry.json",
        "cluster_registry_csv": output_dir
        / "high_intensity_observe_event_cluster_registry.csv",
        "monthly_report": output_dir
        / "high_intensity_monthly_concentration_report.json",
        "pending_registry_json": output_dir
        / "high_intensity_pending_outcome_registry.json",
        "pending_registry_csv": output_dir
        / "high_intensity_pending_outcome_registry.csv",
        "outcome_schedule_json": output_dir
        / "high_intensity_outcome_collection_schedule.json",
        "outcome_schedule_csv": output_dir
        / "high_intensity_outcome_collection_schedule.csv",
        "manual_review_queue_json": output_dir
        / "high_intensity_manual_review_event_queue.json",
        "manual_review_queue_csv": output_dir
        / "high_intensity_manual_review_event_queue.csv",
        "data_quality_report": output_dir
        / "high_intensity_event_logger_data_quality_report.json",
        "interpretation_boundary": output_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "readiness": output_dir / "high_intensity_2337_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2337_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_event_logger_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_forward_observe_event_logger.md",
        "schema_doc": docs_root / "high_intensity_observe_event_schema_usage.md",
        "clustering_doc": docs_root
        / "high_intensity_observe_event_clustering_and_dedup.md",
        "pending_registry_doc": docs_root
        / "high_intensity_pending_outcome_registry.md",
        "readiness_route_doc": docs_root / "high_intensity_2337_readiness_route.md",
    }


def _render_main_doc(
    summary: Mapping[str, Any],
    execution_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Forward Observe Event Logger",
            "",
            "TRADING-2336 承接 TRADING-2335 `COMPOSITE_HIGH_INTENSITY_RULE`，"
            "只生成 observe-only event inventory、cluster registry 和 pending "
            "outcome registry。本任务不绑定 future outcome，不输出仓位建议。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['market_regime']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_validation_policy: `{summary['data_validation_policy']}`",
            "- aits validate-data: `not applicable`，因为本任务只读取 prior "
            "validated research artifacts，且不绑定 actual-path outcome。",
            f"- selected_rule_id: `{summary['selected_rule_id']}`",
            f"- trigger_day_count: `{summary['trigger_day_count']}`",
            f"- event_count_after_dedup: `{summary['event_count_after_dedup']}`",
            f"- cluster_count: `{summary['cluster_count']}`",
            f"- monthly_concentration_status: "
            f"`{summary['monthly_concentration_status']}`",
            f"- readiness_status: `{summary['readiness_status']}`",
            f"- next_task: `{summary['next_task']}`",
            "- runtime_observe_started: `False`",
            "- promotion_allowed / paper_shadow_allowed / production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Execution Contract",
            "",
            f"- execution_status: `{execution_report['execution_status']}`",
            f"- trigger_day_density: `{execution_report['trigger_day_density']}`",
            f"- event_density_after_dedup: "
            f"`{execution_report['event_density_after_dedup']}`",
            "- manual_review_observation_flag: `True` only as review context",
            "- outcome_binding_executed: `False`",
            "",
        ]
    )


def _render_schema_usage_doc() -> str:
    return "\n".join(
        [
            "# High-Intensity Observe Event Schema Usage",
            "",
            "`high_intensity_observe_trigger_day_log` 保存 selected rule 命中的"
            "日级/资产级记录；`high_intensity_observe_event_log` 只保存事件簇的"
            "primary observe event。所有事件初始 `event_status=OBSERVE_PENDING`。",
            "",
            "核心字段包括 `event_id`、`event_cluster_id`、`event_date`、"
            "`target_asset`、`selected_rule_id`、`risk_cap_score`、"
            "`manual_review_observation_flag`、`monthly_event_count` 和 "
            "`consecutive_trigger_days`。",
            "",
            "这些字段只能用于 research-only forward observe 和 manual review "
            "context，不能解释为 target weight、rebalance instruction、"
            "paper-shadow 或 production signal。",
            "",
        ]
    )


def _render_clustering_doc(*, monthly_report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Observe Event Clustering and Dedup",
            "",
            "TRADING-2336 对同一 `selected_rule_id + target_asset` 的连续触发"
            f"按 `{CLUSTER_CONTINUATION_GAP_DAYS}` 个 calendar days 内 continuation "
            "合并为同一 event cluster；每个 cluster 只生成一个 primary observe event。",
            "",
            f"- monthly_event_guardrail: `{MONTHLY_EVENT_GUARDRAIL}`",
            f"- monthly_concentration_status: "
            f"`{monthly_report['monthly_concentration_status']}`",
            f"- monthly_concentration_warnings: "
            f"`{monthly_report['monthly_concentration_warnings']}`",
            "",
            "聚类和去重是后续 actual-path review 的样本控制，不是信号有效性证明。",
            "",
        ]
    )


def _render_pending_registry_doc(*, summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Pending Outcome Registry",
            "",
            "Pending outcome registry 为每个 de-duplicated observe event 建立 "
            "`1d / 5d / 10d / 20d` outcome slot，全部初始为 "
            "`OUTCOME_PENDING`。TRADING-2336 不填充 forward return、drawdown、"
            "stress 或 false-warning label。",
            "",
            f"- event_count_after_dedup: `{summary['event_count_after_dedup']}`",
            f"- next_task: `{summary['next_task']}`",
            "- outcome_binding_executed: `False`",
            "",
        ]
    )


def _render_readiness_route_doc(
    *,
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2337 Readiness Route",
            "",
            f"- readiness_status: `{readiness['readiness_status']}`",
            f"- next_task: `{task_route['next_task']}`",
            f"- warnings: `{readiness.get('warnings', [])}`",
            f"- blockers: `{readiness.get('blockers', [])}`",
            "",
            "只有 `TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder` "
            "可以在未来任务中绑定 actual-path outcome；2336 输出仍然保持 "
            "observe-only。",
            "",
        ]
    )


def _cluster_record(cluster: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    first = cluster[0]
    last = cluster[-1]
    selected_rule_id = str(first.get("selected_rule_id", ""))
    target_asset = str(first.get("target_asset", ""))
    start_date = _parse_date(str(first.get("date", "")))
    end_date = _parse_date(str(last.get("date", "")))
    cluster_id = _short_id(
        "hicl",
        selected_rule_id,
        target_asset,
        start_date.isoformat(),
    )
    event_id = _short_id(
        "hievt",
        selected_rule_id,
        target_asset,
        cluster_id,
        start_date.isoformat(),
    )
    scores = [to_float(row.get("risk_cap_score")) for row in cluster]
    intensities = [str(row.get("risk_cap_intensity", "")) for row in cluster]
    monthly_bucket = start_date.isoformat()[:7]
    active_days = (end_date - start_date).days + 1
    return {
        "schema_version": f"{REPORT_TYPE}.cluster_registry.v1",
        "task_id": TASK_ID,
        "event_cluster_id": cluster_id,
        "selected_rule_id": selected_rule_id,
        "target_asset": target_asset,
        "cluster_start_date": start_date.isoformat(),
        "cluster_end_date": end_date.isoformat(),
        "cluster_active_days": active_days,
        "trigger_day_count": len(cluster),
        "first_event_id": event_id,
        "primary_event_id": event_id,
        "risk_cap_intensity_max": _max_intensity(intensities),
        "risk_cap_score_max": round_float(max(scores) if scores else 0.0),
        "risk_cap_score_avg": round_float(sum(scores) / len(scores) if scores else 0.0),
        "monthly_bucket": monthly_bucket,
        "monthly_event_count": 0,
        "consecutive_trigger_days": _max_consecutive_trigger_days(cluster),
        "cluster_status": "HISTORICAL_CLOSED",
        "cluster_warning": "",
    }


def _derived_field_source(
    field: str,
    dry_summary: Mapping[str, Any],
    pit_boundary: Mapping[str, Any],
) -> str:
    if field == "as_of_timestamp":
        return "risk_cap_decision_timestamp_or_decision_timestamp"
    if field == "known_at_policy" and (
        dry_summary.get("known_at_policy") == KNOWN_AT_POLICY
        or pit_boundary.get("known_at_policy") == KNOWN_AT_POLICY
    ):
        return "TRADING-2332_pit_boundary"
    if field == "pit_policy" and (
        dry_summary.get("pit_policy") == PIT_POLICY
        or pit_boundary.get("pit_approximation_ready") is True
    ):
        return "TRADING-2332_pit_boundary"
    return ""


def _selected_rule_threshold(selected_rule: Mapping[str, Any]) -> float:
    trigger_rule = mapping(selected_rule.get("trigger_rule"))
    return to_float(trigger_rule.get("threshold_value"))


def _is_defensive_risk_cap_direction(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {
        "defensive",
        "risk_cap",
        "risk_off",
        "defensive_risk_cap",
        "portfolio_level_risk_cap",
    }


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _as_of_timestamp(row: Mapping[str, Any]) -> str:
    return str(
        row.get("as_of_timestamp")
        or row.get("risk_cap_decision_timestamp")
        or row.get("decision_timestamp")
        or ""
    )


def _decision_timestamp(row: Mapping[str, Any]) -> str:
    return str(row.get("decision_timestamp") or row.get("risk_cap_decision_timestamp") or "")


def _known_at_policy(
    selected_rule: Mapping[str, Any],
    dry_summary: Mapping[str, Any],
    pit_boundary: Mapping[str, Any],
) -> str:
    return str(
        mapping(selected_rule.get("trigger_rule")).get("known_at_policy")
        or dry_summary.get("known_at_policy")
        or pit_boundary.get("known_at_policy")
        or KNOWN_AT_POLICY
    )


def _latency_policy(selected_rule: Mapping[str, Any]) -> str:
    return str(
        mapping(selected_rule.get("trigger_rule")).get("latency_policy")
        or LATENCY_POLICY
    )


def _pit_policy(
    selected_rule: Mapping[str, Any],
    dry_summary: Mapping[str, Any],
    pit_boundary: Mapping[str, Any],
) -> str:
    if mapping(selected_rule.get("trigger_rule")).get("pit_policy"):
        return str(mapping(selected_rule.get("trigger_rule")).get("pit_policy"))
    if dry_summary.get("pit_policy"):
        return str(dry_summary.get("pit_policy"))
    if pit_boundary.get("pit_approximation_ready") is True:
        return PIT_POLICY
    return PIT_POLICY


def _max_intensity(values: Sequence[str]) -> str:
    ranked = sorted(values, key=_intensity_rank, reverse=True)
    return ranked[0] if ranked else ""


def _intensity_rank(value: str) -> int:
    ranks = {"none": 0, "low": 1, "medium": 2, "high": 3}
    return ranks.get(str(value).lower(), 0)


def _max_consecutive_trigger_days(cluster: Sequence[Mapping[str, Any]]) -> int:
    dates = sorted(_parse_date(str(row.get("date", ""))) for row in cluster)
    best = 0
    current = 0
    previous: date | None = None
    for item in dates:
        if previous is None or (item - previous).days == 1:
            current += 1
        else:
            current = 1
        best = max(best, current)
        previous = item
    return best


def _add_business_days(start: date, days: int) -> date:
    current = start
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _date_is_valid(value: str) -> bool:
    try:
        _parse_date(value)
    except ValueError:
        return False
    return True


def _rate(numerator: int, denominator: int) -> float:
    return round_float(numerator / denominator) if denominator else 0.0


def _duplicate_count(values: Sequence[Any] | Any) -> int:
    counter = Counter(str(value) for value in values if value)
    return sum(count - 1 for count in counter.values() if count > 1)


def _short_id(prefix: str, *parts: str) -> str:
    encoded = "|".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(encoded).hexdigest()[:16]}"


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        clean_for_yaml(dict(payload)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_hash(path: Path) -> str:
    if not path.exists():
        raise HighIntensityEventLoggerError(f"required artifact missing for hash: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityEventLoggerError(
                f"{label} required artifact missing: {path}"
            )
        payloads[key] = _load_json(path)
    return payloads


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HighIntensityEventLoggerError(f"invalid JSON artifact: {path}") from exc


def _validate_no_unsafe_fields(label: str, payload: Any) -> None:
    false_required_keys = {
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "paper_shadow_ready",
        "production_ready",
        "paper_shadow_started",
        "production_started",
        "automatic_exposure_cap_allowed",
        "target_weight_action_allowed",
        "rebalance_instruction_allowed",
        "target_weight_generated",
        "rebalance_instruction_generated",
        "broker_order_generated",
        "paper_shadow_order_generated",
        "production_decision_generated",
    }
    blocked_value_keys = {
        "target_weight_action",
        "target_weight",
        "rebalance_instruction",
        "reduce_position_instruction",
        "increase_cash_instruction",
        "buy_signal",
        "sell_signal",
        "automatic_exposure_cap",
    }
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in false_required_keys and value is True:
            raise HighIntensityEventLoggerError(
                f"{label} unsafe field {'.'.join(path)}=true"
            )
        if key == "broker_action" and str(value).lower() not in {"", "none"}:
            raise HighIntensityEventLoggerError(
                f"{label} unsafe broker_action={value}"
            )
        if key in blocked_value_keys and value not in {False, None, "", "none", "NONE"}:
            raise HighIntensityEventLoggerError(
                f"{label} unsafe field {'.'.join(path)}={value}"
            )


def _walk_payload(
    payload: Any,
    prefix: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], Any]]:
    items: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            items.extend(_walk_payload(value, (*prefix, str(key))))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            items.extend(_walk_payload(value, (*prefix, str(index))))
    else:
        items.append((prefix, payload))
    return items
