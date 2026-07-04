from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_OUTCOME_BINDER_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2339_HIGH_INTENSITY_RISK_CAP_PARTIAL_OUTCOME_READINESS_REVIEW"
REPORT_TYPE = "high_intensity_risk_cap_partial_outcome_readiness_review"
ARTIFACT_ROLE = "high_intensity_risk_cap_partial_outcome_readiness_review"
MODE = "partial_outcome_readiness_review"

EXPECTED_2337_STATUS = "PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS"
EXPECTED_2337_ROUTE = "TRADING-2339_High_Intensity_Risk_Cap_Partial_Outcome_Readiness_Review"

NEXT_2340_FORWARD_REVIEW_TASK = "TRADING-2340_High_Intensity_Risk_Cap_Forward_Outcome_Review"
NEXT_2340_FORWARD_REVIEW_WITH_CAVEAT_TASK = (
    "TRADING-2340_High_Intensity_Risk_Cap_Forward_Outcome_Review_With_Partial_Coverage_Caveat"
)
NEXT_2340_WAIT_TASK = "TRADING-2340_High_Intensity_Risk_Cap_Wait_For_Not_Due_Horizons"
NEXT_2340_PARTIAL_ONLY_TASK = "TRADING-2340_High_Intensity_Risk_Cap_Partial_Outcome_Review_Only"
NEXT_2340_DATA_REMEDIATION_TASK = "TRADING-2340_High_Intensity_Risk_Cap_Outcome_Data_Remediation"
NEXT_2340_ARCHIVE_TASK = "TRADING-2340_Archive_High_Intensity_Risk_Cap_Observe_Line"

OUTCOME_HORIZONS = ("1d", "5d", "10d", "20d")

# TRADING-2339 requirement baseline: >=95% partial coverage may enter 2340 with
# a caveat when there are no data gaps or critical not-due clusters.
HIGH_COVERAGE_RATIO = 0.95
# TRADING-2339 requirement baseline: >=90% coverage is usable only as a partial
# review floor; below this, the outcome review should not proceed.
ACCEPTABLE_PARTIAL_COVERAGE_RATIO = 0.90
# Recent event window used only to distinguish immature forward horizons from
# stale missing outcomes; it does not affect investment interpretation.
RECENT_EVENT_CALENDAR_DAYS = 30
# A multi-day cluster with multiple not-due horizons is treated as critical
# because it can dominate the cluster-level sample if reviewed too early.
CRITICAL_CLUSTER_ACTIVE_DAY_FLOOR = 3

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "partial_outcome_readiness_only": True,
    "outcome_binding_executed": False,
    "original_event_log_mutated": False,
    "runtime_scheduler_enabled": False,
    "automatic_exposure_cap_allowed": False,
    "target_weight_action_allowed": False,
    "rebalance_instruction_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

INPUT_SAFETY_FALSE_FIELDS = {
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "runtime_scheduler_enabled",
    "target_weight_action_allowed",
    "rebalance_instruction_allowed",
    "target_weight_generated",
    "rebalance_instruction_generated",
    "broker_order_generated",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
}
FORBIDDEN_EMIT_FIELDS = {
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
}


class HighIntensityPartialOutcomeReadinessError(ValueError):
    pass


def run_high_intensity_risk_cap_partial_outcome_readiness_review(
    *,
    outcome_binder_dir: Path = DEFAULT_OUTCOME_BINDER_ROOT,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityPartialOutcomeReadinessError(
            f"high-intensity partial outcome readiness review only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_partial_outcome_readiness_inputs(
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
    )
    outcome = inputs["outcome_binder"]
    event_matrix = records(outcome["event_matrix"].get("rows"))
    cluster_matrix = records(outcome["cluster_matrix"].get("rows"))
    event_logger_clusters = records(inputs["event_logger"]["cluster_registry"].get("rows"))
    source_as_of = _parse_date(
        str(
            outcome["summary"].get("validate_data_as_of")
            or outcome["data_quality_report"].get("validate_data_as_of")
        )
    )

    cluster_readiness = build_high_intensity_cluster_readiness_matrix(
        cluster_matrix=cluster_matrix,
        event_logger_clusters=event_logger_clusters,
        source_as_of=source_as_of,
    )
    not_due_matrix = build_high_intensity_not_due_horizon_matrix(
        event_matrix=event_matrix,
        cluster_readiness=cluster_readiness,
        source_as_of=source_as_of,
    )
    coverage_matrix = build_high_intensity_partial_outcome_coverage_matrix(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        not_due_matrix=not_due_matrix,
    )
    not_due_impact = build_high_intensity_not_due_cluster_impact_report(
        cluster_readiness=cluster_readiness,
        not_due_matrix=not_due_matrix,
    )
    distribution = build_high_intensity_not_due_asset_horizon_distribution(
        not_due_matrix=not_due_matrix,
        source_as_of=source_as_of,
    )
    horizon_readiness = build_high_intensity_horizon_readiness_matrix(
        cluster_matrix=cluster_matrix,
    )
    sufficiency = build_high_intensity_partial_outcome_sufficiency_report(
        coverage_matrix=coverage_matrix,
        cluster_readiness=cluster_readiness,
        not_due_impact=not_due_impact,
    )
    decision = build_high_intensity_wait_vs_review_decision_matrix(
        sufficiency_report=sufficiency,
        not_due_distribution=distribution,
        not_due_impact_report=not_due_impact,
    )
    input_contract = build_high_intensity_partial_review_input_contract()
    interpretation_boundary = build_high_intensity_partial_outcome_interpretation_boundary(
        generated_at=generated_at,
        source_as_of=source_as_of,
    )
    readiness = build_high_intensity_2340_readiness_checklist(
        source_data_quality_report=outcome["data_quality_report"],
        sufficiency_report=sufficiency,
        decision_matrix=decision,
    )
    task_route = build_high_intensity_2340_task_route(readiness)
    safety_boundary = build_high_intensity_partial_outcome_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_partial_outcome_readiness_summary(
        generated_at=generated_at,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        source_summary=outcome["summary"],
        source_coverage_report=outcome["coverage_report"],
        source_data_quality_report=outcome["data_quality_report"],
        coverage_matrix=coverage_matrix,
        not_due_matrix=not_due_matrix,
        not_due_impact_report=not_due_impact,
        horizon_readiness=horizon_readiness,
        cluster_readiness=cluster_readiness,
        sufficiency_report=sufficiency,
        decision_matrix=decision,
        readiness=readiness,
        task_route=task_route,
    )

    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_partial_outcome_readiness_outputs(
        paths=paths,
        summary=summary,
        coverage_matrix=coverage_matrix,
        not_due_matrix=not_due_matrix,
        not_due_impact_report=not_due_impact,
        distribution=distribution,
        horizon_readiness=horizon_readiness,
        cluster_readiness=cluster_readiness,
        sufficiency_report=sufficiency,
        decision_matrix=decision,
        input_contract=input_contract,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_partial_outcome_readiness_inputs(
    *,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    return {
        "outcome_binder": load_trading_2337_outcome_binder_outputs(outcome_binder_dir),
        "event_logger": load_trading_2336_lineage_outputs(event_logger_dir),
        "threshold_selection": load_trading_2335_partial_review_context(threshold_selection_dir),
        "forward_observe_plan": load_trading_2334_partial_review_context(forward_observe_plan_dir),
    }


def load_trading_2337_outcome_binder_outputs(outcome_binder_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": outcome_binder_dir / "high_intensity_outcome_binder_summary.json",
        "event_matrix": outcome_binder_dir / "high_intensity_event_actual_path_outcome_matrix.json",
        "cluster_matrix": outcome_binder_dir
        / "high_intensity_cluster_actual_path_outcome_matrix.json",
        "trigger_context": outcome_binder_dir
        / "high_intensity_trigger_day_actual_path_context.json",
        "coverage_report": outcome_binder_dir / "high_intensity_outcome_coverage_report.json",
        "horizon_quality_report": outcome_binder_dir
        / "high_intensity_horizon_outcome_quality_report.json",
        "false_warning_report": outcome_binder_dir
        / "high_intensity_false_warning_classification_report.json",
        "missed_upside_report": outcome_binder_dir
        / "high_intensity_missed_upside_classification_report.json",
        "downside_capture_report": outcome_binder_dir
        / "high_intensity_downside_capture_classification_report.json",
        "manual_review_report": outcome_binder_dir
        / "high_intensity_manual_review_usefulness_proxy_report.json",
        "cluster_policy": outcome_binder_dir / "high_intensity_cluster_weighting_policy.json",
        "data_quality_report": outcome_binder_dir
        / "high_intensity_actual_path_data_quality_report.json",
        "interpretation_boundary": outcome_binder_dir
        / "high_intensity_outcome_binder_interpretation_boundary.json",
        "readiness": outcome_binder_dir / "high_intensity_2339_readiness_checklist.json",
        "task_route": outcome_binder_dir / "high_intensity_2339_task_route.json",
        "safety_boundary": outcome_binder_dir
        / "high_intensity_outcome_binder_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2337 outcome binder")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2337 {key}", payload)

    summary = payloads["summary"]
    route = payloads["task_route"]
    coverage = payloads["coverage_report"]
    data_quality = payloads["data_quality_report"]
    if summary.get("status") != EXPECTED_2337_STATUS:
        raise HighIntensityPartialOutcomeReadinessError(
            f"TRADING-2339 requires 2337 status {EXPECTED_2337_STATUS}"
        )
    if summary.get("next_task") != EXPECTED_2337_ROUTE:
        raise HighIntensityPartialOutcomeReadinessError(
            f"TRADING-2339 requires 2337 summary next_task {EXPECTED_2337_ROUTE}"
        )
    if route.get("next_task") != EXPECTED_2337_ROUTE:
        raise HighIntensityPartialOutcomeReadinessError(
            f"TRADING-2339 requires 2337 route next_task {EXPECTED_2337_ROUTE}"
        )
    if summary.get("validate_data_executed") is not True:
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires source validate_data_executed=true"
        )
    if str(summary.get("validate_data_status", "")) not in {"PASS", "PASS_WITH_WARNINGS"}:
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires source validate_data_status PASS/PASS_WITH_WARNINGS"
        )
    if int(summary.get("validate_data_error_count") or 0) != 0:
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires source validate_data_error_count=0"
        )
    if str(data_quality.get("data_quality_status", "")) == "FAIL":
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 blocks source data_quality_status=FAIL"
        )
    if not records(payloads["event_matrix"].get("rows")):
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires non-empty event outcome matrix"
        )
    if not records(payloads["cluster_matrix"].get("rows")):
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires non-empty cluster outcome matrix"
        )
    expected = int(coverage.get("pending_outcome_count") or 0)
    if expected <= 0:
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires positive pending_outcome_count"
        )
    return {"source_dir": str(outcome_binder_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2336_lineage_outputs(event_logger_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": event_logger_dir / "high_intensity_event_logger_summary.json",
        "event_log": event_logger_dir / "high_intensity_observe_event_log.json",
        "cluster_registry": event_logger_dir / "high_intensity_observe_event_cluster_registry.json",
        "pending_registry": event_logger_dir / "high_intensity_pending_outcome_registry.json",
        "outcome_schedule": event_logger_dir / "high_intensity_outcome_collection_schedule.json",
        "monthly_report": event_logger_dir / "high_intensity_monthly_concentration_report.json",
        "interpretation_boundary": event_logger_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "safety_boundary": event_logger_dir / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger lineage")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)
    if not records(payloads["event_log"].get("rows")):
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires non-empty 2336 event log"
        )
    if not records(payloads["cluster_registry"].get("rows")):
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires non-empty 2336 cluster registry"
        )
    return {"source_dir": str(event_logger_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2335_partial_review_context(
    threshold_selection_dir: Path,
) -> dict[str, Any]:
    paths = {
        "selected_rule": threshold_selection_dir / "high_intensity_selected_trigger_rule.json",
        "caveat_report": threshold_selection_dir
        / "high_intensity_threshold_selection_caveat_report.json",
        "manual_review_boundary": threshold_selection_dir
        / "high_intensity_selected_rule_manual_review_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2335 threshold context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2335 {key}", payload)
    selected_rule = str(payloads["selected_rule"].get("selected_rule_id", ""))
    if selected_rule != "COMPOSITE_HIGH_INTENSITY_RULE":
        raise HighIntensityPartialOutcomeReadinessError(
            "TRADING-2339 requires selected_rule_id=COMPOSITE_HIGH_INTENSITY_RULE"
        )
    return {
        "source_dir": str(threshold_selection_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def load_trading_2334_partial_review_context(
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    paths = {
        "false_warning_framework": forward_observe_plan_dir
        / "high_intensity_false_warning_missed_stress_framework.json",
        "stop_continue_archive_rules": forward_observe_plan_dir
        / "high_intensity_stop_continue_archive_rules.json",
        "manual_review_boundary": forward_observe_plan_dir
        / "high_intensity_manual_review_boundary.json",
        "safety_boundary": forward_observe_plan_dir
        / "high_intensity_forward_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2334 forward observe context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2334 {key}", payload)
    return {
        "source_dir": str(forward_observe_plan_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def build_high_intensity_partial_outcome_coverage_matrix(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    not_due_matrix: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    critical_missing = any(row.get("is_critical_cluster") is True for row in not_due_matrix)
    rows: list[dict[str, Any]] = []
    for analysis_level, source_rows, status_key in (
        ("event", event_matrix, "outcome_binding_status"),
        ("cluster", cluster_matrix, "cluster_outcome_binding_status"),
    ):
        for horizon in OUTCOME_HORIZONS:
            horizon_rows = [row for row in source_rows if row.get("horizon") == horizon]
            expected = len(horizon_rows)
            bound = sum(1 for row in horizon_rows if row.get(status_key) == "OUTCOME_BOUND")
            not_due = sum(1 for row in horizon_rows if _is_not_due_status(row, status_key))
            blocked = sum(1 for row in horizon_rows if _is_blocked_status(row, status_key))
            partial = max(expected - bound - not_due - blocked, 0)
            coverage_ratio = bound / expected if expected else 0.0
            not_due_ratio = not_due / expected if expected else 0.0
            blocked_ratio = blocked / expected if expected else 0.0
            rows.append(
                clean_for_yaml(
                    {
                        "analysis_level": analysis_level,
                        "horizon": horizon,
                        "expected_outcome_count": expected,
                        "bound_outcome_count": bound,
                        "not_due_outcome_count": not_due,
                        "blocked_outcome_count": blocked,
                        "partial_outcome_count": partial,
                        "coverage_ratio": round_float(coverage_ratio),
                        "not_due_ratio": round_float(not_due_ratio),
                        "blocked_ratio": round_float(blocked_ratio),
                        "coverage_status": _coverage_status(
                            coverage_ratio=coverage_ratio,
                            blocked_ratio=blocked_ratio,
                            critical_missing=critical_missing,
                        ),
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_high_intensity_not_due_horizon_matrix(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_readiness: Sequence[Mapping[str, Any]],
    source_as_of: date,
) -> list[dict[str, Any]]:
    cluster_by_id = {str(row.get("event_cluster_id")): row for row in cluster_readiness}
    rows: list[dict[str, Any]] = []
    for outcome in event_matrix:
        if not _is_not_due_status(outcome, "outcome_binding_status"):
            continue
        cluster = cluster_by_id.get(str(outcome.get("event_cluster_id")), {})
        due_date = _parse_date(str(outcome.get("outcome_due_date") or source_as_of))
        event_date = _parse_date(str(outcome.get("event_date") or source_as_of))
        is_recent = (source_as_of - event_date).days <= RECENT_EVENT_CALENDAR_DAYS
        is_critical = cluster.get("cluster_importance_label") == "HIGH_INTENSITY_CRITICAL"
        rows.append(
            clean_for_yaml(
                {
                    "pending_outcome_id": outcome.get("pending_outcome_id", ""),
                    "event_id": outcome.get("event_id", ""),
                    "event_cluster_id": outcome.get("event_cluster_id", ""),
                    "target_asset": outcome.get("target_asset", ""),
                    "event_date": event_date.isoformat(),
                    "horizon": outcome.get("horizon", ""),
                    "outcome_due_date": due_date.isoformat(),
                    "outcome_due_status": outcome.get("outcome_due_status", ""),
                    "is_not_due": True,
                    "days_until_due": max((due_date - source_as_of).days, 0),
                    "is_recent_event": is_recent,
                    "is_critical_cluster": is_critical,
                    "criticality_reason": cluster.get("cluster_readiness_warning", ""),
                    "not_due_impact_level": _not_due_impact_level(
                        horizon=str(outcome.get("horizon", "")),
                        is_critical=is_critical,
                        cluster_not_due_count=int(cluster.get("not_due_horizon_count") or 0),
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_not_due_cluster_impact_report(
    *,
    cluster_readiness: Sequence[Mapping[str, Any]],
    not_due_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    clusters_with_not_due = {
        str(row.get("event_cluster_id"))
        for row in cluster_readiness
        if int(row.get("not_due_horizon_count") or 0) > 0
    }
    only_20d = {
        str(row.get("event_cluster_id"))
        for row in cluster_readiness
        if row.get("cluster_readiness_status") == "CLUSTER_READY_WITH_20D_NOT_DUE"
    }
    multiple = {
        str(row.get("event_cluster_id"))
        for row in cluster_readiness
        if int(row.get("not_due_horizon_count") or 0) > 1
    }
    critical = {
        str(row.get("event_cluster_id"))
        for row in not_due_matrix
        if row.get("is_critical_cluster") is True
    }
    cluster_count = len(cluster_readiness)
    not_due_cluster_ratio = len(clusters_with_not_due) / cluster_count if cluster_count else 0.0
    critical_ratio = len(critical) / cluster_count if cluster_count else 0.0
    label = _not_due_cluster_impact_label(
        critical_count=len(critical),
        multiple_count=len(multiple),
        not_due_cluster_ratio=not_due_cluster_ratio,
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.not_due_cluster_impact_report.v1",
            "task_id": TASK_ID,
            "cluster_count": cluster_count,
            "clusters_with_not_due_outcomes": len(clusters_with_not_due),
            "clusters_with_only_20d_not_due": len(only_20d),
            "clusters_with_multiple_not_due_horizons": len(multiple),
            "critical_clusters_with_not_due": len(critical),
            "not_due_cluster_ratio": round_float(not_due_cluster_ratio),
            "critical_not_due_cluster_ratio": round_float(critical_ratio),
            "not_due_cluster_impact_label": label,
            "not_due_cluster_impact_summary": _not_due_cluster_impact_summary(label),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_not_due_asset_horizon_distribution(
    *,
    not_due_matrix: Sequence[Mapping[str, Any]],
    source_as_of: date,
) -> dict[str, Any]:
    by_asset = Counter(str(row.get("target_asset", "")) for row in not_due_matrix)
    by_horizon = Counter(str(row.get("horizon", "")) for row in not_due_matrix)
    by_month = Counter(str(row.get("event_date", ""))[:7] for row in not_due_matrix)
    by_cluster_age = Counter(
        _cluster_age_bucket(source_as_of, _parse_date(str(row.get("event_date"))))
        for row in not_due_matrix
    )
    total = len(not_due_matrix)
    dominant_asset = _dominant_key(by_asset)
    dominant_horizon = _dominant_key(by_horizon)
    critical_count = sum(1 for row in not_due_matrix if row.get("is_critical_cluster") is True)
    horizon_20d_share = by_horizon.get("20d", 0) / total if total else 0.0
    asset_share = (by_asset.get(dominant_asset, 0) / total) if total and dominant_asset else 0.0
    if critical_count:
        label = "NOT_DUE_CRITICAL_CLUSTER_CONCENTRATION"
    elif horizon_20d_share >= 0.5:
        label = "NOT_DUE_RECENT_20D_CONCENTRATION"
    elif asset_share > 0.5:
        label = "NOT_DUE_ASSET_CONCENTRATION"
    elif total:
        label = "NOT_DUE_DISTRIBUTED_LOW_RISK"
    else:
        label = "NOT_DUE_INCONCLUSIVE"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.not_due_asset_horizon_distribution.v1",
            "task_id": TASK_ID,
            "not_due_by_asset": dict(sorted(by_asset.items())),
            "not_due_by_horizon": dict(sorted(by_horizon.items())),
            "not_due_by_month": dict(sorted(by_month.items())),
            "not_due_by_cluster_age": dict(sorted(by_cluster_age.items())),
            "dominant_missing_asset": dominant_asset or "none",
            "dominant_missing_horizon": dominant_horizon or "none",
            "not_due_concentration_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_horizon_readiness_matrix(
    *,
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon in OUTCOME_HORIZONS:
        horizon_rows = [row for row in cluster_matrix if row.get("horizon") == horizon]
        expected = len(horizon_rows)
        bound = sum(
            1
            for row in horizon_rows
            if row.get("cluster_outcome_binding_status") == "OUTCOME_BOUND"
        )
        not_due = sum(
            1 for row in horizon_rows if _is_not_due_status(row, "cluster_outcome_binding_status")
        )
        blocked = sum(
            1 for row in horizon_rows if _is_blocked_status(row, "cluster_outcome_binding_status")
        )
        coverage_ratio = bound / expected if expected else 0.0
        status, warnings, blockers = _horizon_readiness_status(
            coverage_ratio=coverage_ratio,
            not_due=not_due,
            blocked=blocked,
        )
        metric_ready = status in {
            "HORIZON_READY",
            "HORIZON_READY_WITH_NOT_DUE_CAVEAT",
            "HORIZON_PARTIAL",
        }
        rows.append(
            clean_for_yaml(
                {
                    "horizon": horizon,
                    "expected_cluster_outcome_count": expected,
                    "bound_cluster_outcome_count": bound,
                    "not_due_cluster_outcome_count": not_due,
                    "coverage_ratio": round_float(coverage_ratio),
                    "false_warning_ready": metric_ready,
                    "missed_upside_ready": metric_ready,
                    "downside_capture_ready": metric_ready,
                    "manual_review_proxy_ready": metric_ready,
                    "horizon_readiness_status": status,
                    "horizon_readiness_warnings": warnings,
                    "horizon_readiness_blockers": blockers,
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_cluster_readiness_matrix(
    *,
    cluster_matrix: Sequence[Mapping[str, Any]],
    event_logger_clusters: Sequence[Mapping[str, Any]] = (),
    source_as_of: date | None = None,
) -> list[dict[str, Any]]:
    logger_by_cluster = {str(row.get("event_cluster_id", "")): row for row in event_logger_clusters}
    by_cluster: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in cluster_matrix:
        by_cluster[str(row.get("event_cluster_id", ""))].append(row)
    readiness_rows: list[dict[str, Any]] = []
    effective_as_of = source_as_of or date.today()
    for cluster_id, rows in sorted(by_cluster.items()):
        first = rows[0]
        logger = logger_by_cluster.get(cluster_id, {})
        statuses = {
            str(row.get("horizon", "")): str(row.get("cluster_outcome_binding_status", ""))
            for row in rows
        }
        bound_count = sum(
            1 for row in rows if row.get("cluster_outcome_binding_status") == "OUTCOME_BOUND"
        )
        not_due_count = sum(
            1 for row in rows if _is_not_due_status(row, "cluster_outcome_binding_status")
        )
        blocked_count = sum(
            1 for row in rows if _is_blocked_status(row, "cluster_outcome_binding_status")
        )
        horizons_not_due = {
            str(row.get("horizon", ""))
            for row in rows
            if _is_not_due_status(row, "cluster_outcome_binding_status")
        }
        active_days = int(
            logger.get("cluster_active_days") or first.get("cluster_active_days") or 0
        )
        cluster_start = _parse_date(
            str(logger.get("cluster_start_date") or first.get("cluster_start_date"))
        )
        recent = (effective_as_of - cluster_start).days <= RECENT_EVENT_CALENDAR_DAYS
        is_critical = _is_critical_not_due_cluster(
            not_due_horizons=horizons_not_due,
            blocked_count=blocked_count,
            active_days=active_days,
        )
        readiness_status = _cluster_readiness_status(
            bound_count=bound_count,
            not_due_horizons=horizons_not_due,
            blocked_count=blocked_count,
        )
        importance = _cluster_importance_label(
            is_critical=is_critical,
            not_due_count=not_due_count,
            recent=recent,
        )
        readiness_rows.append(
            clean_for_yaml(
                {
                    "event_cluster_id": cluster_id,
                    "target_asset": first.get("target_asset", ""),
                    "cluster_start_date": cluster_start.isoformat(),
                    "cluster_end_date": str(
                        first.get("cluster_end_date") or logger.get("cluster_end_date") or ""
                    ),
                    "cluster_active_days": active_days,
                    "horizon_1d_status": statuses.get("1d", "MISSING"),
                    "horizon_5d_status": statuses.get("5d", "MISSING"),
                    "horizon_10d_status": statuses.get("10d", "MISSING"),
                    "horizon_20d_status": statuses.get("20d", "MISSING"),
                    "bound_horizon_count": bound_count,
                    "not_due_horizon_count": not_due_count,
                    "blocked_horizon_count": blocked_count,
                    "cluster_readiness_status": readiness_status,
                    "cluster_importance_label": importance,
                    "cluster_readiness_warning": _cluster_readiness_warning(
                        is_critical=is_critical,
                        not_due_horizons=horizons_not_due,
                        blocked_count=blocked_count,
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return readiness_rows


def build_high_intensity_partial_outcome_sufficiency_report(
    *,
    coverage_matrix: Sequence[Mapping[str, Any]],
    cluster_readiness: Sequence[Mapping[str, Any]],
    not_due_impact: Mapping[str, Any],
) -> dict[str, Any]:
    cluster_rows = [row for row in coverage_matrix if row.get("analysis_level") == "cluster"]
    expected = sum(int(row.get("expected_outcome_count") or 0) for row in cluster_rows)
    bound = sum(int(row.get("bound_outcome_count") or 0) for row in cluster_rows)
    not_due = sum(int(row.get("not_due_outcome_count") or 0) for row in cluster_rows)
    blocked = sum(int(row.get("blocked_outcome_count") or 0) for row in cluster_rows)
    coverage_ratio = bound / expected if expected else 0.0
    full_ready = sum(
        1
        for row in cluster_readiness
        if row.get("cluster_readiness_status") == "CLUSTER_FULLY_READY"
    )
    partial_ready = sum(
        1
        for row in cluster_readiness
        if row.get("cluster_readiness_status")
        in {"CLUSTER_READY_WITH_20D_NOT_DUE", "CLUSTER_PARTIAL_NOT_DUE"}
    )
    blocked_clusters = sum(
        1 for row in cluster_readiness if row.get("cluster_readiness_status") == "CLUSTER_BLOCKED"
    )
    critical_missing = int(not_due_impact.get("critical_clusters_with_not_due") or 0)
    if blocked or blocked_clusters:
        status = "DATA_REMEDIATION_REQUIRED"
        label = "DATA_GAPS_BLOCK_REVIEW"
        partial_allowed = False
    elif coverage_ratio < ACCEPTABLE_PARTIAL_COVERAGE_RATIO:
        status = "INSUFFICIENT_COVERAGE"
        label = "COVERAGE_INSUFFICIENT_FOR_20D_REVIEW"
        partial_allowed = False
    elif critical_missing:
        status = "WAIT_FOR_NOT_DUE_HORIZONS"
        label = "CRITICAL_CLUSTER_OUTCOMES_MISSING"
        partial_allowed = False
    elif not_due:
        status = "SUFFICIENT_WITH_PARTIAL_COVERAGE_CAVEAT"
        label = "HIGH_COVERAGE_BUT_RECENT_20D_INCOMPLETE"
        partial_allowed = True
    else:
        status = "SUFFICIENT_FOR_FORWARD_OUTCOME_REVIEW"
        label = "HIGH_COVERAGE_NOT_DUE_LOW_IMPACT"
        partial_allowed = True
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.partial_outcome_sufficiency_report.v1",
            "task_id": TASK_ID,
            "event_count": _analysis_expected_count(coverage_matrix, "event"),
            "cluster_count": len(cluster_readiness),
            "expected_outcome_count": expected,
            "bound_outcome_count": bound,
            "not_due_outcome_count": not_due,
            "blocked_outcome_count": blocked,
            "coverage_ratio": round_float(coverage_ratio),
            "cluster_full_ready_count": full_ready,
            "cluster_partial_ready_count": partial_ready,
            "cluster_blocked_count": blocked_clusters,
            "primary_analysis_level": "cluster",
            "trigger_day_level_usage": "context_only",
            "partial_review_allowed": partial_allowed,
            "partial_review_caveat": "not_due_horizons_must_be_carried_into_2340"
            if not_due
            else "",
            "sufficiency_status": status,
            "sufficiency_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_wait_vs_review_decision_matrix(
    *,
    sufficiency_report: Mapping[str, Any],
    not_due_distribution: Mapping[str, Any],
    not_due_impact_report: Mapping[str, Any],
) -> dict[str, Any]:
    coverage_ratio = to_float(sufficiency_report.get("coverage_ratio"))
    data_gap_count = int(sufficiency_report.get("blocked_outcome_count") or 0)
    critical_count = int(not_due_impact_report.get("critical_clusters_with_not_due") or 0)
    not_due_count = int(sufficiency_report.get("not_due_outcome_count") or 0)
    by_horizon = dict(not_due_distribution.get("not_due_by_horizon") or {})
    count_20d = int(by_horizon.get("20d", 0) or 0)
    if data_gap_count:
        decision = "DATA_REMEDIATION_REQUIRED"
        next_task = NEXT_2340_DATA_REMEDIATION_TASK
        wait_benefit = "data_gap_must_be_fixed_before_review"
        review_risk = "review_would_confuse_missing_data_with_not_due_horizons"
    elif critical_count:
        decision = "WAIT_FOR_NOT_DUE_HORIZONS"
        next_task = NEXT_2340_WAIT_TASK
        wait_benefit = "critical_cluster_outcomes_can_materially_change_review"
        review_risk = "critical_cluster_not_due_horizons_can_distort_downside_capture"
    elif coverage_ratio >= HIGH_COVERAGE_RATIO:
        decision = "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT"
        next_task = NEXT_2340_FORWARD_REVIEW_WITH_CAVEAT_TASK
        wait_benefit = "limited_to_recent_horizon_completion"
        review_risk = "2340_must_carry_partial_coverage_caveat"
    elif coverage_ratio >= ACCEPTABLE_PARTIAL_COVERAGE_RATIO:
        decision = "PARTIAL_OUTCOME_REVIEW_ONLY"
        next_task = NEXT_2340_PARTIAL_ONLY_TASK
        wait_benefit = "waiting_can_complete_marginal_horizons"
        review_risk = "partial_review_cannot_set_final_signal_verdict"
    else:
        decision = "ARCHIVE_OR_PAUSE"
        next_task = NEXT_2340_ARCHIVE_TASK
        wait_benefit = "coverage_too_low_for_useful_review"
        review_risk = "sample_incomplete"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.wait_vs_review_decision_matrix.v1",
            "task_id": TASK_ID,
            "coverage_ratio": round_float(coverage_ratio),
            "not_due_outcome_count": not_due_count,
            "not_due_horizon_distribution": by_horizon,
            "critical_cluster_not_due_count": critical_count,
            "20d_not_due_count": count_20d,
            "data_gap_count": data_gap_count,
            "wait_benefit": wait_benefit,
            "review_now_benefit": "cluster_level_review_can_start_with_explicit_caveat"
            if decision
            in {
                "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT",
                "PARTIAL_OUTCOME_REVIEW_ONLY",
            }
            else "none",
            "review_now_risk": review_risk,
            "decision": decision,
            "decision_rationale": _decision_rationale(decision),
            "next_task_recommendation": next_task,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_review_input_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.partial_review_input_contract.v1",
            "task_id": TASK_ID,
            "contract_id": "high_intensity_partial_review_input_contract",
            "contract_version": "v1",
            "primary_analysis_level": "cluster",
            "allowed_input_artifacts": [
                "high_intensity_cluster_actual_path_outcome_matrix",
                "high_intensity_false_warning_classification_report",
                "high_intensity_missed_upside_classification_report",
                "high_intensity_downside_capture_classification_report",
                "high_intensity_manual_review_usefulness_proxy_report",
                "high_intensity_partial_outcome_readiness_summary",
            ],
            "coverage_caveat_required": True,
            "not_due_horizon_caveat_required": True,
            "blocked_usage": [
                "automatic_exposure_cap",
                "target_weight_action",
                "rebalance_instruction",
                "paper_shadow",
                "production",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_outcome_interpretation_boundary(
    *,
    generated_at: datetime,
    source_as_of: date,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "source_validate_data_as_of": source_as_of.isoformat(),
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "forbidden_interpretations": [
                "real_account_performance",
                "real_position_advice",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2340_readiness_checklist(
    *,
    source_data_quality_report: Mapping[str, Any],
    sufficiency_report: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    decision = str(decision_matrix.get("decision", ""))
    data_quality_passed = (
        str(source_data_quality_report.get("validate_data_status", ""))
        in {
            "PASS",
            "PASS_WITH_WARNINGS",
        }
        and int(source_data_quality_report.get("validate_data_error_count") or 0) == 0
    )
    blockers: list[str] = []
    warnings: list[str] = []
    if not data_quality_passed:
        blockers.append("SOURCE_DATA_QUALITY_NOT_PASSING")
    if sufficiency_report.get("not_due_outcome_count", 0):
        warnings.append("PARTIAL_COVERAGE_NOT_DUE_HORIZONS_PRESENT")
    if decision == "DATA_REMEDIATION_REQUIRED":
        status = "DATA_REMEDIATION_REQUIRED"
        blockers.append("DATA_GAP_COUNT_POSITIVE")
    elif decision == "WAIT_FOR_NOT_DUE_HORIZONS":
        status = "WAIT_FOR_NOT_DUE_HORIZONS"
        blockers.append("CRITICAL_NOT_DUE_HORIZONS_PRESENT")
    elif decision == "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT":
        status = "READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT"
    elif decision == "PARTIAL_OUTCOME_REVIEW_ONLY":
        status = "PARTIAL_OUTCOME_REVIEW_ONLY"
    elif decision == "ARCHIVE_OR_PAUSE":
        status = "READINESS_BLOCKED"
        blockers.append("COVERAGE_TOO_LOW_FOR_2340_REVIEW")
    else:
        status = "READY_FOR_2340_FORWARD_OUTCOME_REVIEW"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2340_readiness_checklist.v1",
            "task_id": TASK_ID,
            "partial_outcome_coverage_matrix_generated": True,
            "not_due_horizon_matrix_generated": True,
            "not_due_cluster_impact_report_generated": True,
            "horizon_readiness_matrix_generated": True,
            "cluster_readiness_matrix_generated": True,
            "sufficiency_report_generated": True,
            "wait_vs_review_decision_generated": True,
            "partial_review_input_contract_generated": True,
            "data_quality_passed": data_quality_passed,
            "safety_boundary_passed": True,
            "outcome_binding_executed": False,
            "original_event_log_mutated": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2340_task_route(readiness: Mapping[str, Any]) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    if status == "READY_FOR_2340_FORWARD_OUTCOME_REVIEW":
        next_task = NEXT_2340_FORWARD_REVIEW_TASK
    elif status == "READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT":
        next_task = NEXT_2340_FORWARD_REVIEW_WITH_CAVEAT_TASK
    elif status == "WAIT_FOR_NOT_DUE_HORIZONS":
        next_task = NEXT_2340_WAIT_TASK
    elif status == "PARTIAL_OUTCOME_REVIEW_ONLY":
        next_task = NEXT_2340_PARTIAL_ONLY_TASK
    elif status == "DATA_REMEDIATION_REQUIRED":
        next_task = NEXT_2340_DATA_REMEDIATION_TASK
    else:
        next_task = NEXT_2340_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2340_task_route.v1",
            "task_id": TASK_ID,
            "readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                NEXT_2340_FORWARD_REVIEW_TASK,
                NEXT_2340_FORWARD_REVIEW_WITH_CAVEAT_TASK,
                NEXT_2340_WAIT_TASK,
                NEXT_2340_PARTIAL_ONLY_TASK,
                NEXT_2340_DATA_REMEDIATION_TASK,
                NEXT_2340_ARCHIVE_TASK,
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_outcome_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task", ""),
            "forbidden_outputs": [
                "target_weight_action",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "reduce_position_instruction",
                "increase_cash_instruction",
                "paper_shadow_ready",
                "production_ready",
                "broker_action",
                "automatic_exposure_cap",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_outcome_readiness_summary(
    *,
    generated_at: datetime,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    source_summary: Mapping[str, Any],
    source_coverage_report: Mapping[str, Any],
    source_data_quality_report: Mapping[str, Any],
    coverage_matrix: Sequence[Mapping[str, Any]],
    not_due_matrix: Sequence[Mapping[str, Any]],
    not_due_impact_report: Mapping[str, Any],
    horizon_readiness: Sequence[Mapping[str, Any]],
    cluster_readiness: Sequence[Mapping[str, Any]],
    sufficiency_report: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Partial Outcome Readiness Review",
            "mode": MODE,
            "status": readiness.get("readiness_status", ""),
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "outcome_binder_dir": str(outcome_binder_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "source_2337_status": source_summary.get("status", ""),
            "source_validate_data_executed": source_summary.get("validate_data_executed", False),
            "source_validate_data_as_of": source_summary.get("validate_data_as_of", ""),
            "source_validate_data_status": source_summary.get("validate_data_status", ""),
            "source_validate_data_error_count": source_summary.get("validate_data_error_count", 0),
            "source_data_quality_status": source_data_quality_report.get("data_quality_status", ""),
            "source_coverage_status": source_coverage_report.get("coverage_status", ""),
            "event_count": source_summary.get("event_count", 0),
            "cluster_count": source_summary.get("cluster_count", 0),
            "expected_outcome_count": sufficiency_report.get("expected_outcome_count", 0),
            "bound_outcome_count": sufficiency_report.get("bound_outcome_count", 0),
            "not_due_outcome_count": sufficiency_report.get("not_due_outcome_count", 0),
            "blocked_outcome_count": sufficiency_report.get("blocked_outcome_count", 0),
            "coverage_ratio": sufficiency_report.get("coverage_ratio", 0.0),
            "not_due_horizon_count": len(not_due_matrix),
            "critical_clusters_with_not_due": not_due_impact_report.get(
                "critical_clusters_with_not_due", 0
            ),
            "horizon_readiness_row_count": len(horizon_readiness),
            "cluster_readiness_row_count": len(cluster_readiness),
            "sufficiency_status": sufficiency_report.get("sufficiency_status", ""),
            "decision": decision_matrix.get("decision", ""),
            "readiness_status": readiness.get("readiness_status", ""),
            "next_task": task_route.get("next_task", ""),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "not rerun because TRADING-2339 only reads prior validated "
                "TRADING-2337 outcome artifacts and does not consume market data directly"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_partial_outcome_readiness_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    coverage_matrix: Sequence[Mapping[str, Any]],
    not_due_matrix: Sequence[Mapping[str, Any]],
    not_due_impact_report: Mapping[str, Any],
    distribution: Mapping[str, Any],
    horizon_readiness: Sequence[Mapping[str, Any]],
    cluster_readiness: Sequence[Mapping[str, Any]],
    sufficiency_report: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    _write_rows_artifacts(
        paths["coverage_matrix_json"],
        paths["coverage_matrix_csv"],
        f"{REPORT_TYPE}.partial_outcome_coverage_matrix.v1",
        coverage_matrix,
    )
    _write_rows_artifacts(
        paths["not_due_matrix_json"],
        paths["not_due_matrix_csv"],
        f"{REPORT_TYPE}.not_due_horizon_matrix.v1",
        not_due_matrix,
    )
    write_json(paths["not_due_impact_report"], not_due_impact_report)
    write_json(paths["distribution"], distribution)
    _write_rows_artifacts(
        paths["horizon_readiness"],
        None,
        f"{REPORT_TYPE}.horizon_readiness_matrix.v1",
        horizon_readiness,
    )
    _write_rows_artifacts(
        paths["cluster_readiness"],
        None,
        f"{REPORT_TYPE}.cluster_readiness_matrix.v1",
        cluster_readiness,
    )
    write_json(paths["sufficiency_report"], sufficiency_report)
    write_json(paths["decision_matrix"], decision_matrix)
    write_json(paths["input_contract"], input_contract)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary))
    write_markdown(paths["coverage_doc"], _render_coverage_doc(summary, coverage_matrix))
    write_markdown(paths["not_due_doc"], _render_not_due_doc(summary, not_due_impact_report))
    write_markdown(paths["decision_doc"], _render_decision_doc(decision_matrix))
    write_markdown(paths["route_doc"], _render_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_partial_outcome_readiness_summary.json",
        "coverage_matrix_json": output_dir / "high_intensity_partial_outcome_coverage_matrix.json",
        "coverage_matrix_csv": output_dir / "high_intensity_partial_outcome_coverage_matrix.csv",
        "not_due_matrix_json": output_dir / "high_intensity_not_due_horizon_matrix.json",
        "not_due_matrix_csv": output_dir / "high_intensity_not_due_horizon_matrix.csv",
        "not_due_impact_report": output_dir / "high_intensity_not_due_cluster_impact_report.json",
        "distribution": output_dir / "high_intensity_not_due_asset_horizon_distribution.json",
        "horizon_readiness": output_dir / "high_intensity_horizon_readiness_matrix.json",
        "cluster_readiness": output_dir / "high_intensity_cluster_readiness_matrix.json",
        "sufficiency_report": output_dir / "high_intensity_partial_outcome_sufficiency_report.json",
        "decision_matrix": output_dir / "high_intensity_wait_vs_review_decision_matrix.json",
        "input_contract": output_dir / "high_intensity_partial_review_input_contract.json",
        "interpretation_boundary": output_dir
        / "high_intensity_partial_outcome_interpretation_boundary.json",
        "readiness": output_dir / "high_intensity_2340_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2340_task_route.json",
        "safety_boundary": output_dir / "high_intensity_partial_outcome_safety_boundary.json",
        "main_doc": docs_root / "high_intensity_risk_cap_partial_outcome_readiness_review.md",
        "coverage_doc": docs_root / "high_intensity_partial_outcome_coverage.md",
        "not_due_doc": docs_root / "high_intensity_not_due_horizon_impact.md",
        "decision_doc": docs_root / "high_intensity_wait_vs_review_decision.md",
        "route_doc": docs_root / "high_intensity_2340_readiness_route.md",
    }


def _write_rows_artifacts(
    json_path: Path,
    csv_path: Path | None,
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
    if csv_path is not None:
        write_csv_rows(csv_path, rows)


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityPartialOutcomeReadinessError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityPartialOutcomeReadinessError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensityPartialOutcomeReadinessError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _collect_unsafe_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in INPUT_SAFETY_FALSE_FIELDS and _truthy(item):
                violations.append(path)
            if key_text == "broker_action" and str(item).lower() not in {"", "none"}:
                violations.append(path)
            if key_text in FORBIDDEN_EMIT_FIELDS and _emits_action(item):
                violations.append(path)
            violations.extend(_collect_unsafe_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_unsafe_fields(item, f"{prefix}[{index}]"))
    return violations


def _is_not_due_status(row: Mapping[str, Any], status_key: str) -> bool:
    return (
        str(row.get(status_key, "")).upper() == "OUTCOME_NOT_DUE"
        or str(row.get("outcome_due_status", "")).upper() == "NOT_DUE"
    )


def _is_blocked_status(row: Mapping[str, Any], status_key: str) -> bool:
    status = str(row.get(status_key, "")).upper()
    quality = str(
        row.get("outcome_quality_status") or row.get("cluster_outcome_quality_status") or ""
    ).upper()
    return status.startswith("OUTCOME_BLOCKED") or "BLOCKED" in quality


def _coverage_status(
    *,
    coverage_ratio: float,
    blocked_ratio: float,
    critical_missing: bool,
) -> str:
    if blocked_ratio > 0:
        return "COVERAGE_BLOCKED"
    if coverage_ratio >= 1.0:
        return "FULL_COVERAGE"
    if coverage_ratio >= HIGH_COVERAGE_RATIO:
        return "HIGH_COVERAGE_WITH_NOT_DUE"
    if coverage_ratio >= ACCEPTABLE_PARTIAL_COVERAGE_RATIO and not critical_missing:
        return "PARTIAL_COVERAGE_ACCEPTABLE"
    return "PARTIAL_COVERAGE_LOW"


def _horizon_readiness_status(
    *,
    coverage_ratio: float,
    not_due: int,
    blocked: int,
) -> tuple[str, list[str], list[str]]:
    warnings: list[str] = []
    blockers: list[str] = []
    if blocked:
        blockers.append("BLOCKED_OUTCOMES_PRESENT")
        return "HORIZON_BLOCKED", warnings, blockers
    if coverage_ratio >= 1.0:
        return "HORIZON_READY", warnings, blockers
    if coverage_ratio >= HIGH_COVERAGE_RATIO:
        warnings.append("NOT_DUE_HORIZONS_PRESENT")
        return "HORIZON_READY_WITH_NOT_DUE_CAVEAT", warnings, blockers
    if coverage_ratio >= ACCEPTABLE_PARTIAL_COVERAGE_RATIO:
        warnings.append("PARTIAL_NOT_DUE_HORIZONS_PRESENT")
        return "HORIZON_PARTIAL", warnings, blockers
    blockers.append("LOW_HORIZON_COVERAGE")
    return "HORIZON_BLOCKED", warnings, blockers


def _cluster_readiness_status(
    *,
    bound_count: int,
    not_due_horizons: set[str],
    blocked_count: int,
) -> str:
    if blocked_count:
        return "CLUSTER_BLOCKED"
    if bound_count >= len(OUTCOME_HORIZONS):
        return "CLUSTER_FULLY_READY"
    if not_due_horizons == {"20d"}:
        return "CLUSTER_READY_WITH_20D_NOT_DUE"
    if not_due_horizons:
        return "CLUSTER_PARTIAL_NOT_DUE"
    return "CLUSTER_BLOCKED"


def _is_critical_not_due_cluster(
    *,
    not_due_horizons: set[str],
    blocked_count: int,
    active_days: int,
) -> bool:
    if blocked_count:
        return True
    if "5d" in not_due_horizons:
        return True
    return len(not_due_horizons) > 1 and active_days >= CRITICAL_CLUSTER_ACTIVE_DAY_FLOOR


def _cluster_importance_label(
    *,
    is_critical: bool,
    not_due_count: int,
    recent: bool,
) -> str:
    if is_critical:
        return "HIGH_INTENSITY_CRITICAL"
    if not_due_count and recent:
        return "RECENT_INCOMPLETE"
    if not_due_count:
        return "INCONCLUSIVE"
    return "NORMAL_HIGH_INTENSITY"


def _cluster_readiness_warning(
    *,
    is_critical: bool,
    not_due_horizons: set[str],
    blocked_count: int,
) -> str:
    if blocked_count:
        return "BLOCKED_CLUSTER_OUTCOME"
    if is_critical:
        return "MULTI_HORIZON_ACTIVE_CLUSTER_NOT_DUE"
    if not_due_horizons == {"20d"}:
        return "RECENT_20D_ONLY_NOT_DUE"
    if not_due_horizons:
        return "RECENT_MULTI_HORIZON_NOT_DUE"
    return ""


def _not_due_impact_level(
    *,
    horizon: str,
    is_critical: bool,
    cluster_not_due_count: int,
) -> str:
    if is_critical:
        return "HIGH"
    if cluster_not_due_count > 1:
        return "MODERATE"
    if horizon == "20d":
        return "LOW"
    return "MODERATE"


def _not_due_cluster_impact_label(
    *,
    critical_count: int,
    multiple_count: int,
    not_due_cluster_ratio: float,
) -> str:
    if critical_count:
        return "NOT_DUE_IMPACT_HIGH"
    if multiple_count:
        return "NOT_DUE_IMPACT_MODERATE"
    if not_due_cluster_ratio > 0:
        return "NOT_DUE_IMPACT_LOW"
    return "NOT_DUE_IMPACT_LOW"


def _not_due_cluster_impact_summary(label: str) -> str:
    if label == "NOT_DUE_IMPACT_HIGH":
        return "critical not-due clusters require waiting before full review"
    if label == "NOT_DUE_IMPACT_MODERATE":
        return "recent multi-horizon not-due clusters require explicit partial caveat"
    return "not-due horizons are limited and low impact"


def _analysis_expected_count(
    coverage_matrix: Sequence[Mapping[str, Any]], analysis_level: str
) -> int:
    total = sum(
        int(row.get("expected_outcome_count") or 0)
        for row in coverage_matrix
        if row.get("analysis_level") == analysis_level
    )
    return total // len(OUTCOME_HORIZONS) if total else 0


def _cluster_age_bucket(source_as_of: date, event_date: date) -> str:
    days = max((source_as_of - event_date).days, 0)
    if days <= 7:
        return "0_7_days"
    if days <= RECENT_EVENT_CALENDAR_DAYS:
        return "8_30_days"
    return "over_30_days"


def _dominant_key(counter: Counter[str]) -> str:
    if not counter:
        return ""
    top_count = max(counter.values())
    top = sorted(key for key, value in counter.items() if value == top_count)
    if len(top) > 1:
        return "MIXED"
    return top[0]


def _decision_rationale(decision: str) -> str:
    if decision == "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT":
        return "coverage_ratio_is_high_and_not_due_horizons_are_noncritical"
    if decision == "WAIT_FOR_NOT_DUE_HORIZONS":
        return "critical_not_due_horizons_can_change_cluster_level_interpretation"
    if decision == "PARTIAL_OUTCOME_REVIEW_ONLY":
        return "coverage_is_acceptable_but_not_enough_for_full_review"
    if decision == "DATA_REMEDIATION_REQUIRED":
        return "blocked_outcomes_or_data_gaps_require_remediation"
    return "coverage_or_safety_state_blocks_forward_review"


def _parse_date(value: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise HighIntensityPartialOutcomeReadinessError("missing required date")
    return date.fromisoformat(text[:10])


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "yes", "1", "enabled", "ready"}


def _emits_action(value: object) -> bool:
    if value in (None, False, "", "none", "NONE"):
        return False
    if isinstance(value, Mapping):
        return bool(value)
    if isinstance(value, Sequence) and not isinstance(value, str):
        return bool(value)
    return True


def _render_main_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Partial Outcome Readiness Review",
            "",
            f"- status: `{summary.get('status')}`",
            f"- source_2337_status: `{summary.get('source_2337_status')}`",
            f"- coverage_ratio: `{summary.get('coverage_ratio')}`",
            f"- bound_outcome_count: `{summary.get('bound_outcome_count')}`",
            f"- not_due_outcome_count: `{summary.get('not_due_outcome_count')}`",
            f"- decision: `{summary.get('decision')}`",
            f"- next_task: `{summary.get('next_task')}`",
            "",
            (
                "本报告只做 partial outcome readiness review；不重新绑定 outcome，"
                "不读取 market data，不输出仓位建议。"
            ),
        ]
    )


def _render_coverage_doc(
    summary: Mapping[str, Any],
    coverage_matrix: Sequence[Mapping[str, Any]],
) -> str:
    cluster_rows = [row for row in coverage_matrix if row.get("analysis_level") == "cluster"]
    return "\n".join(
        [
            "# High-Intensity Partial Outcome Coverage",
            "",
            f"- cluster_count: `{summary.get('cluster_count')}`",
            f"- expected_outcome_count: `{summary.get('expected_outcome_count')}`",
            f"- bound_outcome_count: `{summary.get('bound_outcome_count')}`",
            f"- coverage_ratio: `{summary.get('coverage_ratio')}`",
            "",
            "|horizon|coverage_status|bound|not_due|blocked|",
            "|---|---|---:|---:|---:|",
            *[
                "|{horizon}|{status}|{bound}|{not_due}|{blocked}|".format(
                    horizon=row.get("horizon", ""),
                    status=row.get("coverage_status", ""),
                    bound=row.get("bound_outcome_count", 0),
                    not_due=row.get("not_due_outcome_count", 0),
                    blocked=row.get("blocked_outcome_count", 0),
                )
                for row in cluster_rows
            ],
        ]
    )


def _render_not_due_doc(
    summary: Mapping[str, Any],
    not_due_impact_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Not-Due Horizon Impact",
            "",
            f"- not_due_outcome_count: `{summary.get('not_due_outcome_count')}`",
            (
                "- clusters_with_not_due_outcomes: "
                f"`{not_due_impact_report.get('clusters_with_not_due_outcomes')}`"
            ),
            (
                "- critical_clusters_with_not_due: "
                f"`{not_due_impact_report.get('critical_clusters_with_not_due')}`"
            ),
            f"- impact_label: `{not_due_impact_report.get('not_due_cluster_impact_label')}`",
            "",
            (
                "Not-due horizons are carried as a caveat into the next review; "
                "they are not interpreted as signal success or failure."
            ),
        ]
    )


def _render_decision_doc(decision_matrix: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Wait vs Review Decision",
            "",
            f"- decision: `{decision_matrix.get('decision')}`",
            f"- rationale: `{decision_matrix.get('decision_rationale')}`",
            f"- next_task: `{decision_matrix.get('next_task_recommendation')}`",
            f"- review_now_risk: `{decision_matrix.get('review_now_risk')}`",
            "",
            "该决策只决定 TRADING-2340 route，不批准 paper-shadow、production 或 broker action。",
        ]
    )


def _render_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2340 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            f"- blockers: `{readiness.get('readiness_blockers')}`",
            f"- warnings: `{readiness.get('readiness_warnings')}`",
            "",
            "TRADING-2340 才能解读 cluster-level outcome；TRADING-2339 不做最终有效性判断。",
        ]
    )
