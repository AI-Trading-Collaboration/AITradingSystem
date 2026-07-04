from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
    DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
    load_trading_2335_threshold_selection_outputs,
)
from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EVENT_LOGGER_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    load_trading_2332_dynamic_dry_run_context,
    load_trading_2333_dynamic_diagnostics_context,
    load_trading_2334_forward_observe_plan_outputs,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    load_adjusted_price_matrix,
    mapping,
    max_price_date,
    rate,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2337_HIGH_INTENSITY_RISK_CAP_ACTUAL_PATH_OUTCOME_BINDER"
REPORT_TYPE = "high_intensity_risk_cap_actual_path_outcome_binder"
ARTIFACT_ROLE = "high_intensity_risk_cap_actual_path_outcome_binder"
MODE = "actual_path_outcome_binder"

EXPECTED_2337_TASK = (
    "TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder"
)
NEXT_2339_FORWARD_REVIEW_TASK = (
    "TRADING-2339_High_Intensity_Risk_Cap_Forward_Outcome_Review"
)
NEXT_2339_PARTIAL_REVIEW_TASK = (
    "TRADING-2339_High_Intensity_Risk_Cap_Partial_Outcome_Readiness_Review"
)
NEXT_2339_DATA_REMEDIATION_TASK = (
    "TRADING-2339_High_Intensity_Risk_Cap_Outcome_Data_Remediation"
)
NEXT_2339_CLUSTERING_REMEDIATION_TASK = (
    "TRADING-2339_High_Intensity_Risk_Cap_Event_Clustering_Remediation"
)
NEXT_2339_ZERO_EVENT_TASK = (
    "TRADING-2339_High_Intensity_Risk_Cap_Zero_Event_Readiness_Review"
)
NEXT_2339_ARCHIVE_TASK = "TRADING-2339_Archive_High_Intensity_Risk_Cap_Observe_Line"

OUTCOME_HORIZON_DAYS = {"1d": 1, "5d": 5, "10d": 10, "20d": 20}
CLASSIFICATION_POLICY_ID = "high_intensity_actual_path_research_candidate_v1"
CLASSIFICATION_POLICY_VERSION = "v1"
ANNUALIZED_VOLATILITY_TRADING_DAYS = 252

STRESS_THRESHOLD_BY_HORIZON = {
    "1d": -0.015,
    "5d": -0.035,
    "10d": -0.050,
    "20d": -0.070,
}
REBOUND_THRESHOLD_BY_HORIZON = {
    "1d": 0.015,
    "5d": 0.035,
    "10d": 0.050,
    "20d": 0.070,
}
MISSED_UPSIDE_THRESHOLD_BY_HORIZON = dict(REBOUND_THRESHOLD_BY_HORIZON)
DOWNSIDE_CAPTURE_THRESHOLD_BY_HORIZON = dict(STRESS_THRESHOLD_BY_HORIZON)

MODERATE_RATE_THRESHOLD = 0.25
HIGH_RATE_THRESHOLD = 0.50

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "actual_path_outcome_binding_only": True,
    "outcome_binding_executed": True,
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


class HighIntensityOutcomeBinderError(ValueError):
    pass


def run_high_intensity_risk_cap_actual_path_outcome_binder(
    *,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    dynamic_diagnostics_dir: Path = DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
    market_data_source: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityOutcomeBinderError(
            f"high-intensity outcome binder only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_outcome_binder_inputs(
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
    )
    event_rows = records(mapping(inputs["event_logger"]["event_log"]).get("rows"))
    cluster_rows = records(mapping(inputs["event_logger"]["cluster_registry"]).get("rows"))
    pending_rows = records(mapping(inputs["event_logger"]["pending_registry"]).get("rows"))
    schedule_rows = records(mapping(inputs["event_logger"]["outcome_schedule"]).get("rows"))
    trigger_day_rows = records(mapping(inputs["event_logger"]["trigger_day_log"]).get("rows"))

    prices_path = _resolve_market_data_source(market_data_source)
    required_assets = _target_assets([event_rows, cluster_rows, trigger_day_rows])
    resolved_quality_as_of = quality_as_of or max_price_date(prices_path)
    quality_report, quality_report_path = run_actual_path_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_assets=required_assets,
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise HighIntensityOutcomeBinderError(
            f"cached data quality gate failed: {quality_report.status}"
        )

    price_matrix = load_adjusted_price_matrix(prices_path, required_assets)
    outcome_as_of = resolved_quality_as_of
    classification_policy = build_high_intensity_outcome_classification_policy()
    cluster_policy = build_high_intensity_cluster_weighting_policy()

    event_matrix = build_high_intensity_event_actual_path_outcome_matrix(
        event_log=event_rows,
        pending_registry=pending_rows,
        outcome_schedule=schedule_rows,
        price_matrix=price_matrix,
        outcome_as_of_date=outcome_as_of,
        classification_policy=classification_policy,
    )
    cluster_matrix = build_high_intensity_cluster_actual_path_outcome_matrix(
        cluster_registry=cluster_rows,
        price_matrix=price_matrix,
        outcome_as_of_date=outcome_as_of,
        classification_policy=classification_policy,
    )
    trigger_context = build_high_intensity_trigger_day_actual_path_context(
        trigger_day_log=trigger_day_rows,
        price_matrix=price_matrix,
        outcome_as_of_date=outcome_as_of,
        classification_policy=classification_policy,
    )
    coverage_report = build_high_intensity_outcome_coverage_report(
        event_log=event_rows,
        cluster_registry=cluster_rows,
        pending_registry=pending_rows,
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        latest_market_data_date=outcome_as_of,
        price_matrix=price_matrix,
    )
    horizon_quality_report = build_high_intensity_horizon_outcome_quality_report(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        coverage_report=coverage_report,
    )
    rebound_stress_matrix = build_high_intensity_rebound_stress_classification_matrix(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        classification_policy=classification_policy,
    )
    false_warning_report = build_high_intensity_false_warning_classification_report(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
    )
    missed_upside_report = build_high_intensity_missed_upside_classification_report(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
    )
    downside_capture_report = (
        build_high_intensity_downside_capture_classification_report(
            event_matrix=event_matrix,
            cluster_matrix=cluster_matrix,
        )
    )
    manual_review_report = build_high_intensity_manual_review_usefulness_proxy_report(
        event_log=event_rows,
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
    )
    actual_path_data_quality_report = build_high_intensity_actual_path_data_quality_report(
        event_log=event_rows,
        cluster_registry=cluster_rows,
        pending_registry=pending_rows,
        outcome_schedule=schedule_rows,
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        trigger_context=trigger_context,
        price_matrix=price_matrix,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
    )
    interpretation_boundary = build_high_intensity_outcome_binder_interpretation_boundary(
        generated_at=generated_at,
        quality_report=quality_report,
    )
    readiness = build_high_intensity_2339_readiness_checklist(
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        coverage_report=coverage_report,
        actual_path_data_quality_report=actual_path_data_quality_report,
    )
    task_route = build_high_intensity_2339_task_route(readiness)
    safety_boundary = build_high_intensity_outcome_binder_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_outcome_binder_summary(
        generated_at=generated_at,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_report=quality_report,
        event_log=event_rows,
        cluster_registry=cluster_rows,
        pending_registry=pending_rows,
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        trigger_context=trigger_context,
        coverage_report=coverage_report,
        actual_path_data_quality_report=actual_path_data_quality_report,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_outcome_binder_outputs(
        paths=paths,
        summary=summary,
        event_matrix=event_matrix,
        cluster_matrix=cluster_matrix,
        trigger_context=trigger_context,
        coverage_report=coverage_report,
        horizon_quality_report=horizon_quality_report,
        rebound_stress_matrix=rebound_stress_matrix,
        false_warning_report=false_warning_report,
        missed_upside_report=missed_upside_report,
        downside_capture_report=downside_capture_report,
        manual_review_report=manual_review_report,
        cluster_policy=cluster_policy,
        actual_path_data_quality_report=actual_path_data_quality_report,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_outcome_binder_inputs(
    *,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    dynamic_diagnostics_dir: Path,
) -> dict[str, Any]:
    return {
        "event_logger": load_trading_2336_event_logger_outputs(event_logger_dir),
        "threshold_selection": load_trading_2335_threshold_selection_outputs(
            threshold_selection_dir
        ),
        "forward_observe_plan": load_trading_2334_forward_observe_plan_outputs(
            forward_observe_plan_dir
        ),
        "dynamic_dry_run": load_trading_2332_dynamic_dry_run_context(
            dynamic_dry_run_dir
        ),
        "dynamic_diagnostics": load_trading_2333_dynamic_diagnostics_context(
            dynamic_diagnostics_dir
        ),
    }


def load_trading_2336_event_logger_outputs(event_logger_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": event_logger_dir / "high_intensity_event_logger_summary.json",
        "execution_report": event_logger_dir
        / "high_intensity_selected_rule_execution_report.json",
        "trigger_day_log": event_logger_dir
        / "high_intensity_observe_trigger_day_log.json",
        "event_log": event_logger_dir / "high_intensity_observe_event_log.json",
        "cluster_registry": event_logger_dir
        / "high_intensity_observe_event_cluster_registry.json",
        "monthly_report": event_logger_dir
        / "high_intensity_monthly_concentration_report.json",
        "pending_registry": event_logger_dir
        / "high_intensity_pending_outcome_registry.json",
        "outcome_schedule": event_logger_dir
        / "high_intensity_outcome_collection_schedule.json",
        "manual_review_queue": event_logger_dir
        / "high_intensity_manual_review_event_queue.json",
        "data_quality_report": event_logger_dir
        / "high_intensity_event_logger_data_quality_report.json",
        "interpretation_boundary": event_logger_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "readiness": event_logger_dir / "high_intensity_2337_readiness_checklist.json",
        "task_route": event_logger_dir / "high_intensity_2337_task_route.json",
        "safety_boundary": event_logger_dir
        / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)

    summary = mapping(payloads["summary"])
    route = mapping(payloads["task_route"])
    event_log = records(mapping(payloads["event_log"]).get("rows"))
    cluster_registry = records(mapping(payloads["cluster_registry"]).get("rows"))
    pending_registry = records(mapping(payloads["pending_registry"]).get("rows"))
    outcome_schedule = records(mapping(payloads["outcome_schedule"]).get("rows"))

    if summary.get("next_task") != EXPECTED_2337_TASK:
        raise HighIntensityOutcomeBinderError(
            f"TRADING-2337 requires 2336 summary next_task {EXPECTED_2337_TASK}"
        )
    if route.get("next_task") != EXPECTED_2337_TASK:
        raise HighIntensityOutcomeBinderError(
            f"TRADING-2337 requires 2336 route next_task {EXPECTED_2337_TASK}"
        )
    if summary.get("outcome_binding_executed") is not False:
        raise HighIntensityOutcomeBinderError(
            "TRADING-2337 requires 2336 outcome_binding_executed=false"
        )
    if summary.get("runtime_scheduler_enabled") is not False:
        raise HighIntensityOutcomeBinderError(
            "TRADING-2337 requires 2336 runtime_scheduler_enabled=false"
        )
    if not event_log:
        return {
            "source_dir": str(event_logger_dir),
            "paths": {key: str(path) for key, path in paths.items()},
            **payloads,
        }
    if not cluster_registry:
        raise HighIntensityOutcomeBinderError(
            "TRADING-2337 requires non-empty 2336 cluster registry"
        )
    if not pending_registry:
        raise HighIntensityOutcomeBinderError(
            "TRADING-2337 requires non-empty 2336 pending outcome registry"
        )
    if not outcome_schedule:
        raise HighIntensityOutcomeBinderError(
            "TRADING-2337 requires non-empty 2336 outcome collection schedule"
        )
    return {
        "source_dir": str(event_logger_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def run_actual_path_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    required_assets: Sequence[str],
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(required_assets),
        expected_rate_series=[],
        quality_config=load_data_quality(),
        as_of=quality_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, quality_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def build_high_intensity_event_actual_path_outcome_matrix(
    *,
    event_log: Sequence[Mapping[str, Any]],
    pending_registry: Sequence[Mapping[str, Any]],
    outcome_schedule: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    outcome_as_of_date: date,
    classification_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    events_by_id = {str(row.get("event_id", "")): row for row in event_log}
    schedule_by_key = {
        (str(row.get("event_id", "")), str(row.get("horizon", ""))): row
        for row in outcome_schedule
    }
    rows: list[dict[str, Any]] = []
    for pending in pending_registry:
        event_id = str(pending.get("event_id", ""))
        event = events_by_id.get(event_id, {})
        horizon = str(pending.get("horizon", ""))
        horizon_days = int(OUTCOME_HORIZON_DAYS.get(horizon, 0))
        event_date = _parse_date(str(pending.get("event_date") or event.get("event_date", "")))
        due_date = _parse_date(str(pending.get("outcome_due_date", "")))
        target_asset = str(pending.get("target_asset") or event.get("target_asset", ""))
        schedule = schedule_by_key.get((event_id, horizon), {})
        due_status = _price_calendar_due_status(
            price_matrix=price_matrix,
            target_asset=target_asset,
            start_date=event_date,
            horizon_days=horizon_days,
            fallback_due_date=due_date,
            outcome_as_of_date=outcome_as_of_date,
        )
        outcome = _price_outcome(
            price_matrix=price_matrix,
            target_asset=target_asset,
            start_date=event_date,
            horizon_days=horizon_days,
            due_status=due_status,
            outcome_as_of_date=outcome_as_of_date,
        )
        classified = _classify_outcome(
            outcome=outcome,
            horizon=horizon,
            classification_policy=classification_policy,
        )
        rows.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.event_actual_path_outcome.v1",
                    "task_id": TASK_ID,
                    "outcome_id": _short_id("hiapo", event_id, horizon),
                    "pending_outcome_id": pending.get("pending_outcome_id", ""),
                    "event_id": event_id,
                    "event_date": event_date.isoformat(),
                    "target_asset": target_asset,
                    "selected_rule_id": pending.get(
                        "selected_rule_id",
                        event.get("selected_rule_id", ""),
                    ),
                    "event_cluster_id": pending.get(
                        "event_cluster_id",
                        event.get("event_cluster_id", ""),
                    ),
                    "horizon": horizon,
                    "outcome_as_of_date": outcome_as_of_date.isoformat(),
                    "outcome_due_date": due_date.isoformat(),
                    "outcome_due_status": due_status,
                    "source_schedule_status": schedule.get("schedule_status", ""),
                    "market_data_start_date": outcome.get("market_data_start_date"),
                    "market_data_end_date": outcome.get("market_data_end_date"),
                    "forward_return": outcome.get("forward_return"),
                    "forward_max_drawdown": outcome.get("forward_max_drawdown"),
                    "forward_min_return": outcome.get("forward_min_return"),
                    "forward_max_return": outcome.get("forward_max_return"),
                    "realized_volatility": outcome.get("realized_volatility"),
                    **classified,
                    "outcome_quality_status": outcome["outcome_quality_status"],
                    "outcome_binding_status": outcome["outcome_binding_status"],
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_cluster_actual_path_outcome_matrix(
    *,
    cluster_registry: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    outcome_as_of_date: date,
    classification_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cluster in cluster_registry:
        start_date = _parse_date(str(cluster.get("cluster_start_date", "")))
        for horizon, horizon_days in OUTCOME_HORIZON_DAYS.items():
            due_date = _add_business_days(start_date, horizon_days)
            due_status = _price_calendar_due_status(
                price_matrix=price_matrix,
                target_asset=str(cluster.get("target_asset", "")),
                start_date=start_date,
                horizon_days=horizon_days,
                fallback_due_date=due_date,
                outcome_as_of_date=outcome_as_of_date,
            )
            outcome = _price_outcome(
                price_matrix=price_matrix,
                target_asset=str(cluster.get("target_asset", "")),
                start_date=start_date,
                horizon_days=horizon_days,
                due_status=due_status,
                outcome_as_of_date=outcome_as_of_date,
            )
            classified = _classify_outcome(
                outcome=outcome,
                horizon=horizon,
                classification_policy=classification_policy,
            )
            rows.append(
                clean_for_yaml(
                    {
                        "schema_version": f"{REPORT_TYPE}.cluster_actual_path_outcome.v1",
                        "task_id": TASK_ID,
                        "cluster_outcome_id": _short_id(
                            "hicapo",
                            str(cluster.get("event_cluster_id", "")),
                            horizon,
                        ),
                        "event_cluster_id": cluster.get("event_cluster_id", ""),
                        "primary_event_id": cluster.get("primary_event_id", ""),
                        "cluster_start_date": cluster.get("cluster_start_date", ""),
                        "cluster_end_date": cluster.get("cluster_end_date", ""),
                        "target_asset": cluster.get("target_asset", ""),
                        "selected_rule_id": cluster.get("selected_rule_id", ""),
                        "cluster_active_days": cluster.get("cluster_active_days", 0),
                        "trigger_day_count": cluster.get("trigger_day_count", 0),
                        "horizon": horizon,
                        "cluster_outcome_as_of_date": outcome_as_of_date.isoformat(),
                        "outcome_due_date": due_date.isoformat(),
                        "outcome_due_status": due_status,
                        "market_data_start_date": outcome.get("market_data_start_date"),
                        "market_data_end_date": outcome.get("market_data_end_date"),
                        "cluster_forward_return": outcome.get("forward_return"),
                        "cluster_forward_max_drawdown": outcome.get(
                            "forward_max_drawdown"
                        ),
                        "cluster_forward_min_return": outcome.get("forward_min_return"),
                        "cluster_forward_max_return": outcome.get("forward_max_return"),
                        "cluster_realized_volatility": outcome.get("realized_volatility"),
                        "cluster_stress_detected": classified["stress_detected"],
                        "cluster_rebound_detected": classified["rebound_detected"],
                        "cluster_false_warning_candidate": classified[
                            "false_warning_candidate"
                        ],
                        "cluster_missed_upside_candidate": classified[
                            "missed_upside_candidate"
                        ],
                        "cluster_downside_capture_candidate": classified[
                            "downside_capture_candidate"
                        ],
                        "cluster_manual_review_would_have_helped_candidate": classified[
                            "manual_review_would_have_helped_candidate"
                        ],
                        "cluster_outcome_quality_status": outcome[
                            "outcome_quality_status"
                        ],
                        "cluster_outcome_binding_status": outcome[
                            "outcome_binding_status"
                        ],
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_high_intensity_trigger_day_actual_path_context(
    *,
    trigger_day_log: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    outcome_as_of_date: date,
    classification_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trigger_day in trigger_day_log:
        start_date = _parse_date(str(trigger_day.get("date", "")))
        for horizon, horizon_days in OUTCOME_HORIZON_DAYS.items():
            due_date = _add_business_days(start_date, horizon_days)
            due_status = _price_calendar_due_status(
                price_matrix=price_matrix,
                target_asset=str(trigger_day.get("target_asset", "")),
                start_date=start_date,
                horizon_days=horizon_days,
                fallback_due_date=due_date,
                outcome_as_of_date=outcome_as_of_date,
            )
            outcome = _price_outcome(
                price_matrix=price_matrix,
                target_asset=str(trigger_day.get("target_asset", "")),
                start_date=start_date,
                horizon_days=horizon_days,
                due_status=due_status,
                outcome_as_of_date=outcome_as_of_date,
            )
            classified = _classify_outcome(
                outcome=outcome,
                horizon=horizon,
                classification_policy=classification_policy,
            )
            rows.append(
                clean_for_yaml(
                    {
                        "schema_version": f"{REPORT_TYPE}.trigger_day_context.v1",
                        "task_id": TASK_ID,
                        "trigger_day_context_id": _short_id(
                            "hitdctx",
                            str(trigger_day.get("trigger_day_id", "")),
                            horizon,
                        ),
                        "trigger_day_id": trigger_day.get("trigger_day_id", ""),
                        "event_cluster_id": trigger_day.get("event_cluster_id", ""),
                        "event_id": trigger_day.get("event_id", ""),
                        "date": trigger_day.get("date", ""),
                        "target_asset": trigger_day.get("target_asset", ""),
                        "horizon": horizon,
                        "outcome_due_status": due_status,
                        "trigger_day_forward_return": outcome.get("forward_return"),
                        "trigger_day_forward_max_drawdown": outcome.get(
                            "forward_max_drawdown"
                        ),
                        "trigger_day_forward_min_return": outcome.get(
                            "forward_min_return"
                        ),
                        "trigger_day_forward_max_return": outcome.get(
                            "forward_max_return"
                        ),
                        "trigger_day_realized_volatility": outcome.get(
                            "realized_volatility"
                        ),
                        "trigger_day_stress_detected": classified["stress_detected"],
                        "trigger_day_rebound_detected": classified["rebound_detected"],
                        "trigger_day_context_only": True,
                        "excluded_from_primary_sample": True,
                        "context_reason": (
                            "trigger-day actual path is context only; "
                            "cluster-level matrix is the primary sample"
                        ),
                        **SAFETY_FIELDS,
                    }
                )
            )
    return rows


def build_high_intensity_outcome_classification_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.classification_policy.v1",
            "task_id": TASK_ID,
            "classification_policy_id": CLASSIFICATION_POLICY_ID,
            "classification_policy_version": CLASSIFICATION_POLICY_VERSION,
            "classification_policy_status": "RESEARCH_CANDIDATE_NOT_PRODUCTION",
            "stress_threshold_by_horizon": STRESS_THRESHOLD_BY_HORIZON,
            "rebound_threshold_by_horizon": REBOUND_THRESHOLD_BY_HORIZON,
            "missed_upside_threshold_by_horizon": MISSED_UPSIDE_THRESHOLD_BY_HORIZON,
            "downside_capture_threshold_by_horizon": DOWNSIDE_CAPTURE_THRESHOLD_BY_HORIZON,
            "volatility_threshold_policy": {
                "realized_volatility_role": "context_only_not_decision_gate",
                "annualization_trading_days": ANNUALIZED_VOLATILITY_TRADING_DAYS,
            },
            "report_label_rate_thresholds": {
                "moderate_rate_threshold": MODERATE_RATE_THRESHOLD,
                "high_rate_threshold": HIGH_RATE_THRESHOLD,
            },
            "classification_source": "high_intensity_false_warning_missed_stress_framework",
            "uses_future_return_for_rule_selection": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_cluster_weighting_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.cluster_weighting_policy.v1",
            "task_id": TASK_ID,
            "primary_analysis_level": "cluster",
            "secondary_analysis_level": "event",
            "trigger_day_level_usage": "context_only",
            "original_event_log_mutated": False,
            "outcome_binding_derivative_only": True,
            "future_outcome_used_for_event_creation": False,
            "future_outcome_used_for_rule_selection": False,
            "cluster_weighting_rule": "one_cluster_horizon_one_primary_sample",
            "cluster_start_policy": "primary_event_id_or_cluster_start_date",
            "continuous_trigger_handling": (
                "continuous trigger days stay in trigger-day context and do not "
                "increase primary sample count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_outcome_coverage_report(
    *,
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    pending_registry: Sequence[Mapping[str, Any]],
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    latest_market_data_date: date,
    price_matrix: pd.DataFrame,
) -> dict[str, Any]:
    statuses = Counter(str(row.get("outcome_binding_status", "")) for row in event_matrix)
    cluster_statuses = Counter(
        str(row.get("cluster_outcome_binding_status", "")) for row in cluster_matrix
    )
    blocked_count = statuses.get("OUTCOME_BLOCKED_MARKET_DATA", 0) + cluster_statuses.get(
        "OUTCOME_BLOCKED_MARKET_DATA", 0
    )
    not_due_count = statuses.get("OUTCOME_NOT_DUE", 0) + cluster_statuses.get(
        "OUTCOME_NOT_DUE", 0
    )
    warnings: list[str] = []
    errors: list[str] = []
    if not_due_count:
        warnings.append("NOT_DUE_HORIZONS_PRESENT")
    if blocked_count:
        errors.append("MARKET_DATA_COVERAGE_GAP")
    if not event_log:
        errors.append("ZERO_EVENTS")
    missing_assets = [
        asset
        for asset in _target_assets([event_log, cluster_registry])
        if asset not in price_matrix.columns or price_matrix[asset].dropna().empty
    ]
    if missing_assets:
        errors.append("PRICE_CACHE_MISSING_TARGET_ASSET")
    if errors:
        status = "BLOCKED" if "ZERO_EVENTS" in errors else "PARTIAL_COVERAGE_WITH_DATA_GAPS"
    elif not_due_count:
        status = "PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS"
    else:
        status = "FULL_COVERAGE"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.outcome_coverage_report.v1",
            "task_id": TASK_ID,
            "event_count": len(event_log),
            "cluster_count": len(cluster_registry),
            "pending_outcome_count": len(pending_registry),
            "outcome_bound_count": statuses.get("OUTCOME_BOUND", 0),
            "outcome_partial_count": statuses.get("OUTCOME_PARTIAL", 0),
            "outcome_not_due_count": statuses.get("OUTCOME_NOT_DUE", 0),
            "outcome_blocked_count": statuses.get("OUTCOME_BLOCKED_MARKET_DATA", 0),
            "cluster_outcome_bound_count": cluster_statuses.get("OUTCOME_BOUND", 0),
            "coverage_by_horizon": _coverage_by(event_matrix, "horizon"),
            "coverage_by_asset": _coverage_by(event_matrix, "target_asset"),
            "coverage_by_cluster": _coverage_by(event_matrix, "event_cluster_id"),
            "latest_market_data_date": latest_market_data_date.isoformat(),
            "missing_asset_count": len(missing_assets),
            "missing_assets": missing_assets,
            "minimum_required_coverage_met": not errors,
            "coverage_status": status,
            "coverage_warnings": warnings,
            "coverage_errors": errors,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_horizon_outcome_quality_report(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    coverage_report: Mapping[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for horizon in OUTCOME_HORIZON_DAYS:
        event_rows = [row for row in event_matrix if row.get("horizon") == horizon]
        cluster_rows = [row for row in cluster_matrix if row.get("horizon") == horizon]
        event_statuses = Counter(str(row.get("outcome_binding_status", "")) for row in event_rows)
        cluster_statuses = Counter(
            str(row.get("cluster_outcome_binding_status", "")) for row in cluster_rows
        )
        rows.append(
            clean_for_yaml(
                {
                    "horizon": horizon,
                    "event_outcome_count": len(event_rows),
                    "cluster_outcome_count": len(cluster_rows),
                    "event_bound_count": event_statuses.get("OUTCOME_BOUND", 0),
                    "cluster_bound_count": cluster_statuses.get("OUTCOME_BOUND", 0),
                    "event_not_due_count": event_statuses.get("OUTCOME_NOT_DUE", 0),
                    "cluster_not_due_count": cluster_statuses.get("OUTCOME_NOT_DUE", 0),
                    "event_blocked_count": event_statuses.get(
                        "OUTCOME_BLOCKED_MARKET_DATA",
                        0,
                    ),
                    "cluster_blocked_count": cluster_statuses.get(
                        "OUTCOME_BLOCKED_MARKET_DATA",
                        0,
                    ),
                }
            )
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.horizon_quality_report.v1",
            "task_id": TASK_ID,
            "coverage_status": coverage_report.get("coverage_status", ""),
            "rows": rows,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_rebound_stress_classification_matrix(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    classification_policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in event_matrix:
        rows.append(
            {
                "analysis_level": "event",
                "sample_id": row.get("event_id", ""),
                "cluster_id": row.get("event_cluster_id", ""),
                "target_asset": row.get("target_asset", ""),
                "horizon": row.get("horizon", ""),
                "stress_detected": row.get("stress_detected", False),
                "rebound_detected": row.get("rebound_detected", False),
                "false_warning_candidate": row.get("false_warning_candidate", False),
                "missed_upside_candidate": row.get("missed_upside_candidate", False),
                "downside_capture_candidate": row.get("downside_capture_candidate", False),
                "outcome_binding_status": row.get("outcome_binding_status", ""),
            }
        )
    for row in cluster_matrix:
        rows.append(
            {
                "analysis_level": "cluster",
                "sample_id": row.get("event_cluster_id", ""),
                "cluster_id": row.get("event_cluster_id", ""),
                "target_asset": row.get("target_asset", ""),
                "horizon": row.get("horizon", ""),
                "stress_detected": row.get("cluster_stress_detected", False),
                "rebound_detected": row.get("cluster_rebound_detected", False),
                "false_warning_candidate": row.get(
                    "cluster_false_warning_candidate",
                    False,
                ),
                "missed_upside_candidate": row.get(
                    "cluster_missed_upside_candidate",
                    False,
                ),
                "downside_capture_candidate": row.get(
                    "cluster_downside_capture_candidate",
                    False,
                ),
                "outcome_binding_status": row.get("cluster_outcome_binding_status", ""),
            }
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.rebound_stress_matrix.v1",
            "task_id": TASK_ID,
            "classification_policy": classification_policy,
            "row_count": len(rows),
            "rows": rows,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_false_warning_classification_report(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_ready = _bound_rows(event_matrix, "outcome_binding_status")
    cluster_ready = _bound_rows(cluster_matrix, "cluster_outcome_binding_status")
    false_event = [row for row in event_ready if row.get("false_warning_candidate") is True]
    false_cluster = [
        row for row in cluster_ready if row.get("cluster_false_warning_candidate") is True
    ]
    event_ready_ids = _unique_values(event_ready, "event_id")
    cluster_ready_ids = _unique_values(cluster_ready, "event_cluster_id")
    false_event_ids = _unique_values(false_event, "event_id")
    false_cluster_ids = _unique_values(false_cluster, "event_cluster_id")
    cluster_rate = rate(len(false_cluster_ids), len(cluster_ready_ids))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.false_warning_report.v1",
            "task_id": TASK_ID,
            "event_count": len(_unique_values(event_matrix, "event_id")),
            "cluster_count": len(_unique_values(cluster_matrix, "event_cluster_id")),
            "outcome_ready_event_count": len(event_ready_ids),
            "outcome_ready_cluster_count": len(cluster_ready_ids),
            "false_warning_event_count": len(false_event_ids),
            "false_warning_cluster_count": len(false_cluster_ids),
            "false_warning_event_rate": rate(len(false_event_ids), len(event_ready_ids)),
            "false_warning_cluster_rate": cluster_rate,
            "false_warning_by_horizon": _candidate_by(false_cluster, "horizon"),
            "false_warning_by_asset": _candidate_by(false_cluster, "target_asset"),
            "false_warning_label": _rate_label(
                cluster_rate,
                prefix="FALSE_WARNING",
                inconclusive=not cluster_ready,
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_missed_upside_classification_report(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_ready = _bound_rows(event_matrix, "outcome_binding_status")
    cluster_ready = _bound_rows(cluster_matrix, "cluster_outcome_binding_status")
    missed_event = [row for row in event_ready if row.get("missed_upside_candidate") is True]
    missed_cluster = [
        row for row in cluster_ready if row.get("cluster_missed_upside_candidate") is True
    ]
    event_ready_ids = _unique_values(event_ready, "event_id")
    cluster_ready_ids = _unique_values(cluster_ready, "event_cluster_id")
    missed_event_ids = _unique_values(missed_event, "event_id")
    missed_cluster_ids = _unique_values(missed_cluster, "event_cluster_id")
    cluster_rate = rate(len(missed_cluster_ids), len(cluster_ready_ids))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.missed_upside_report.v1",
            "task_id": TASK_ID,
            "event_count": len(_unique_values(event_matrix, "event_id")),
            "cluster_count": len(_unique_values(cluster_matrix, "event_cluster_id")),
            "missed_upside_event_count": len(missed_event_ids),
            "missed_upside_cluster_count": len(missed_cluster_ids),
            "missed_upside_event_rate": rate(len(missed_event_ids), len(event_ready_ids)),
            "missed_upside_cluster_rate": cluster_rate,
            "missed_upside_return_context": _return_context(missed_cluster),
            "missed_upside_by_horizon": _candidate_by(missed_cluster, "horizon"),
            "missed_upside_by_asset": _candidate_by(missed_cluster, "target_asset"),
            "missed_upside_label": _rate_label(
                cluster_rate,
                prefix="MISSED_UPSIDE",
                inconclusive=not cluster_ready,
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_downside_capture_classification_report(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_ready = _bound_rows(event_matrix, "outcome_binding_status")
    cluster_ready = _bound_rows(cluster_matrix, "cluster_outcome_binding_status")
    downside_event = [row for row in event_ready if row.get("downside_capture_candidate") is True]
    downside_cluster = [
        row
        for row in cluster_ready
        if row.get("cluster_downside_capture_candidate") is True
    ]
    event_ready_ids = _unique_values(event_ready, "event_id")
    cluster_ready_ids = _unique_values(cluster_ready, "event_cluster_id")
    downside_event_ids = _unique_values(downside_event, "event_id")
    downside_cluster_ids = _unique_values(downside_cluster, "event_cluster_id")
    cluster_rate = rate(len(downside_cluster_ids), len(cluster_ready_ids))
    drawdowns = [
        to_float(row.get("cluster_forward_max_drawdown"))
        for row in cluster_ready
        if row.get("cluster_forward_max_drawdown") is not None
    ]
    label = "DOWNSIDE_CAPTURE_INCONCLUSIVE"
    if cluster_ready:
        if cluster_rate >= HIGH_RATE_THRESHOLD:
            label = "DOWNSIDE_CAPTURE_STRONG"
        elif cluster_rate >= MODERATE_RATE_THRESHOLD:
            label = "DOWNSIDE_CAPTURE_MODERATE"
        else:
            label = "DOWNSIDE_CAPTURE_WEAK"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.downside_capture_report.v1",
            "task_id": TASK_ID,
            "event_count": len(_unique_values(event_matrix, "event_id")),
            "cluster_count": len(_unique_values(cluster_matrix, "event_cluster_id")),
            "downside_capture_event_count": len(downside_event_ids),
            "downside_capture_cluster_count": len(downside_cluster_ids),
            "downside_capture_event_rate": rate(
                len(downside_event_ids),
                len(event_ready_ids),
            ),
            "downside_capture_cluster_rate": cluster_rate,
            "stress_detected_event_count": len(
                _unique_values(
                    [row for row in event_ready if row.get("stress_detected") is True],
                    "event_id",
                )
            ),
            "stress_detected_cluster_count": len(
                _unique_values(
                    [
                        row
                        for row in cluster_ready
                        if row.get("cluster_stress_detected") is True
                    ],
                    "event_cluster_id",
                )
            ),
            "average_forward_max_drawdown_after_warning": round_float(
                sum(drawdowns) / len(drawdowns) if drawdowns else 0.0
            ),
            "worst_forward_max_drawdown_after_warning": round_float(
                min(drawdowns) if drawdowns else 0.0
            ),
            "downside_capture_by_horizon": _candidate_by(downside_cluster, "horizon"),
            "downside_capture_by_asset": _candidate_by(downside_cluster, "target_asset"),
            "downside_capture_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_manual_review_usefulness_proxy_report(
    *,
    event_log: Sequence[Mapping[str, Any]],
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_ready = _bound_rows(event_matrix, "outcome_binding_status")
    cluster_ready = _bound_rows(cluster_matrix, "cluster_outcome_binding_status")
    helpful_events = [
        row for row in event_ready if row.get("manual_review_would_have_helped_candidate") is True
    ]
    helpful_clusters = [
        row
        for row in cluster_ready
        if row.get("cluster_manual_review_would_have_helped_candidate") is True
    ]
    cluster_ready_ids = _unique_values(cluster_ready, "event_cluster_id")
    helpful_event_ids = _unique_values(helpful_events, "event_id")
    helpful_cluster_ids = _unique_values(helpful_clusters, "event_cluster_id")
    false_warning_count = len(
        _unique_values(
            [row for row in event_ready if row.get("false_warning_candidate") is True],
            "event_id",
        )
    )
    missed_upside_count = len(
        _unique_values(
            [row for row in event_ready if row.get("missed_upside_candidate") is True],
            "event_id",
        )
    )
    usefulness = rate(len(helpful_cluster_ids), len(cluster_ready_ids))
    if not cluster_ready:
        label = "MANUAL_REVIEW_CONTEXT_INCONCLUSIVE"
    elif usefulness >= HIGH_RATE_THRESHOLD:
        label = "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY"
    elif usefulness >= MODERATE_RATE_THRESHOLD:
        label = "MANUAL_REVIEW_CONTEXT_MIXED_PROXY"
    else:
        label = "MANUAL_REVIEW_CONTEXT_WEAK_PROXY"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_usefulness_proxy.v1",
            "task_id": TASK_ID,
            "manual_review_event_count": len(event_log),
            "outcome_ready_manual_review_event_count": len(
                {row.get("event_id", "") for row in event_ready}
            ),
            "manual_review_would_have_helped_event_count": len(helpful_event_ids),
            "manual_review_would_have_helped_cluster_count": len(helpful_cluster_ids),
            "manual_review_false_warning_count": false_warning_count,
            "manual_review_missed_upside_count": missed_upside_count,
            "manual_review_usefulness_proxy": usefulness,
            "manual_review_usefulness_label": label,
            "proxy_boundary": (
                "manual_review_would_have_helped is a research proxy, not a "
                "historical position instruction"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_actual_path_data_quality_report(
    *,
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    pending_registry: Sequence[Mapping[str, Any]],
    outcome_schedule: Sequence[Mapping[str, Any]],
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    trigger_context: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    quality_report: DataQualityReport,
    quality_report_path: Path,
) -> dict[str, Any]:
    invalid_horizon_count = sum(
        1 for row in event_matrix if str(row.get("horizon", "")) not in OUTCOME_HORIZON_DAYS
    )
    duplicate_outcome_id_count = _duplicate_count(
        [str(row.get("outcome_id", "")) for row in event_matrix]
    )
    safety_violation_count = sum(
        _safety_violation_count(row)
        for collection in (event_matrix, cluster_matrix, trigger_context)
        for row in collection
    )
    missing_market_data_count = sum(
        1
        for row in event_matrix
        if row.get("outcome_binding_status") == "OUTCOME_BLOCKED_MARKET_DATA"
    )
    required_assets = _target_assets([event_log, cluster_registry])
    missing_assets = [
        asset
        for asset in required_assets
        if asset not in price_matrix.columns or price_matrix[asset].dropna().empty
    ]
    errors = [
        count
        for count in (
            invalid_horizon_count,
            duplicate_outcome_id_count,
            safety_violation_count,
            missing_market_data_count,
            len(missing_assets),
            0 if quality_report.passed else quality_report.error_count,
        )
        if count
    ]
    warning_count = quality_report.warning_count + sum(
        1
        for row in event_matrix
        if row.get("outcome_binding_status") == "OUTCOME_NOT_DUE"
    )
    error_count = sum(errors)
    status = "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.actual_path_data_quality_report.v1",
            "task_id": TASK_ID,
            "market_data_status": quality_report.status,
            "event_log_status": "PASS" if event_log else "FAIL",
            "cluster_registry_status": "PASS" if cluster_registry else "FAIL",
            "pending_outcome_registry_status": "PASS" if pending_registry else "FAIL",
            "outcome_schedule_status": "PASS" if outcome_schedule else "FAIL",
            "event_outcome_matrix_status": "PASS" if event_matrix else "FAIL",
            "cluster_outcome_matrix_status": "PASS" if cluster_matrix else "FAIL",
            "record_count": len(event_matrix),
            "event_count": len(event_log),
            "cluster_count": len(cluster_registry),
            "missing_market_data_count": missing_market_data_count,
            "missing_asset_count": len(missing_assets),
            "missing_assets": missing_assets,
            "invalid_horizon_count": invalid_horizon_count,
            "invalid_timestamp_count": 0,
            "duplicate_outcome_id_count": duplicate_outcome_id_count,
            "safety_violation_count": safety_violation_count,
            "warning_count": warning_count,
            "error_count": error_count,
            "data_quality_status": status,
            "validate_data_executed": True,
            "validate_data_as_of": quality_report.as_of.isoformat(),
            "validate_data_status": quality_report.status,
            "validate_data_error_count": quality_report.error_count,
            "validate_data_warning_count": quality_report.warning_count,
            "validate_data_report_path": str(quality_report_path),
            "price_row_count": quality_report.price_summary.rows,
            "rate_row_count": quality_report.rate_summary.rows,
            "price_checksum": quality_report.price_summary.sha256,
            "rate_checksum": quality_report.rate_summary.sha256,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_outcome_binder_interpretation_boundary(
    *,
    generated_at: datetime,
    quality_report: DataQualityReport,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "event_logging_source": "TRADING-2336",
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "actual_path_outcome_binding_only": True,
            "data_quality_status": quality_report.status,
            "validate_data_executed": True,
            "validate_data_as_of": quality_report.as_of.isoformat(),
            "forbidden_interpretations": [
                "real_account_performance",
                "position_advice",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2339_readiness_checklist(
    *,
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    coverage_report: Mapping[str, Any],
    actual_path_data_quality_report: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not event_matrix:
        blockers.append("EVENT_OUTCOME_MATRIX_MISSING")
    if not cluster_matrix:
        blockers.append("CLUSTER_OUTCOME_MATRIX_MISSING")
    if coverage_report.get("coverage_status") == "BLOCKED":
        blockers.append("OUTCOME_COVERAGE_BLOCKED")
    if coverage_report.get("coverage_status") == "PARTIAL_COVERAGE_WITH_DATA_GAPS":
        blockers.append("MARKET_DATA_COVERAGE_GAP")
    if coverage_report.get("coverage_status") == "PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS":
        warnings.append("NOT_DUE_HORIZONS_PRESENT")
    if actual_path_data_quality_report.get("data_quality_status") == "FAIL":
        blockers.append("ACTUAL_PATH_DATA_QUALITY_FAIL")
    elif actual_path_data_quality_report.get("data_quality_status") == "PASS_WITH_WARNINGS":
        warnings.append("ACTUAL_PATH_DATA_QUALITY_WARNINGS")

    if blockers:
        status = "DATA_REMEDIATION_REQUIRED"
        if "EVENT_OUTCOME_MATRIX_MISSING" in blockers:
            status = "OUTCOME_BINDER_BLOCKED"
    elif warnings:
        if "NOT_DUE_HORIZONS_PRESENT" in warnings:
            status = "PARTIAL_OUTCOME_REVIEW_REQUIRED"
        else:
            status = "READY_FOR_2339_FORWARD_OUTCOME_REVIEW_WITH_WARNINGS"
    else:
        status = "READY_FOR_2339_FORWARD_OUTCOME_REVIEW"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2339_readiness_checklist.v1",
            "task_id": TASK_ID,
            "event_outcome_matrix_generated": bool(event_matrix),
            "cluster_outcome_matrix_generated": bool(cluster_matrix),
            "cluster_level_primary_analysis_ready": bool(cluster_matrix)
            and not blockers,
            "false_warning_report_generated": True,
            "missed_upside_report_generated": True,
            "downside_capture_report_generated": True,
            "manual_review_usefulness_report_generated": True,
            "outcome_coverage_report_generated": True,
            "data_quality_passed": actual_path_data_quality_report.get(
                "data_quality_status"
            )
            in {"PASS", "PASS_WITH_WARNINGS"},
            "safety_boundary_passed": True,
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


def build_high_intensity_2339_task_route(
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    caveat = ""
    if status == "READY_FOR_2339_FORWARD_OUTCOME_REVIEW":
        next_task = NEXT_2339_FORWARD_REVIEW_TASK
    elif status == "READY_FOR_2339_FORWARD_OUTCOME_REVIEW_WITH_WARNINGS":
        next_task = NEXT_2339_FORWARD_REVIEW_TASK
        caveat = "DATA_QUALITY_OR_PIT_CAVEAT"
    elif status == "PARTIAL_OUTCOME_REVIEW_REQUIRED":
        next_task = NEXT_2339_PARTIAL_REVIEW_TASK
    elif status == "DATA_REMEDIATION_REQUIRED":
        blockers = set(records(readiness.get("readiness_blockers")))
        del blockers
        next_task = NEXT_2339_DATA_REMEDIATION_TASK
    elif status == "OUTCOME_BINDER_BLOCKED":
        next_task = NEXT_2339_ARCHIVE_TASK
    else:
        next_task = NEXT_2339_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2339_task_route.v1",
            "task_id": TASK_ID,
            "readiness_status": status,
            "next_task": next_task,
            "caveat": caveat,
            "allowed_routes": [
                NEXT_2339_FORWARD_REVIEW_TASK,
                NEXT_2339_PARTIAL_REVIEW_TASK,
                NEXT_2339_DATA_REMEDIATION_TASK,
                NEXT_2339_CLUSTERING_REMEDIATION_TASK,
                NEXT_2339_ZERO_EVENT_TASK,
                NEXT_2339_ARCHIVE_TASK,
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_outcome_binder_safety_boundary(
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


def build_high_intensity_outcome_binder_summary(
    *,
    generated_at: datetime,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    dynamic_diagnostics_dir: Path,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_report: DataQualityReport,
    event_log: Sequence[Mapping[str, Any]],
    cluster_registry: Sequence[Mapping[str, Any]],
    pending_registry: Sequence[Mapping[str, Any]],
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    trigger_context: Sequence[Mapping[str, Any]],
    coverage_report: Mapping[str, Any],
    actual_path_data_quality_report: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    status = _summary_status(readiness, coverage_report)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Actual Path Outcome Binder",
            "mode": MODE,
            "status": status,
            "outcome_binder_status": status,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "dynamic_diagnostics_dir": str(dynamic_diagnostics_dir),
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "event_count": len(event_log),
            "cluster_count": len(cluster_registry),
            "pending_outcome_count": len(pending_registry),
            "event_actual_path_outcome_count": len(event_matrix),
            "cluster_actual_path_outcome_count": len(cluster_matrix),
            "trigger_day_context_count": len(trigger_context),
            "coverage_status": coverage_report.get("coverage_status", ""),
            "data_quality_status": actual_path_data_quality_report.get(
                "data_quality_status",
                "",
            ),
            "validate_data_executed": True,
            "validate_data_as_of": quality_report.as_of.isoformat(),
            "validate_data_status": quality_report.status,
            "validate_data_error_count": quality_report.error_count,
            "validate_data_warning_count": quality_report.warning_count,
            "readiness_status": readiness.get("readiness_status", ""),
            "next_task": task_route.get("next_task", ""),
            "original_event_log_mutated": False,
            "future_outcome_used_for_rule_selection": False,
            "future_outcome_used_for_event_creation": False,
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_outcome_binder_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    event_matrix: Sequence[Mapping[str, Any]],
    cluster_matrix: Sequence[Mapping[str, Any]],
    trigger_context: Sequence[Mapping[str, Any]],
    coverage_report: Mapping[str, Any],
    horizon_quality_report: Mapping[str, Any],
    rebound_stress_matrix: Mapping[str, Any],
    false_warning_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    downside_capture_report: Mapping[str, Any],
    manual_review_report: Mapping[str, Any],
    cluster_policy: Mapping[str, Any],
    actual_path_data_quality_report: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    _write_rows_artifacts(
        paths["event_matrix_json"],
        paths["event_matrix_csv"],
        f"{REPORT_TYPE}.event_actual_path_outcome_matrix.v1",
        event_matrix,
    )
    _write_rows_artifacts(
        paths["cluster_matrix_json"],
        paths["cluster_matrix_csv"],
        f"{REPORT_TYPE}.cluster_actual_path_outcome_matrix.v1",
        cluster_matrix,
    )
    _write_rows_artifacts(
        paths["trigger_context_json"],
        paths["trigger_context_csv"],
        f"{REPORT_TYPE}.trigger_day_actual_path_context.v1",
        trigger_context,
    )
    write_json(paths["coverage_report"], coverage_report)
    write_json(paths["horizon_quality_report"], horizon_quality_report)
    write_json(paths["rebound_stress_json"], rebound_stress_matrix)
    write_csv_rows(
        paths["rebound_stress_csv"],
        records(rebound_stress_matrix.get("rows")),
    )
    write_json(paths["false_warning_report"], false_warning_report)
    write_json(paths["missed_upside_report"], missed_upside_report)
    write_json(paths["downside_capture_report"], downside_capture_report)
    write_json(paths["manual_review_report"], manual_review_report)
    write_json(paths["cluster_policy"], cluster_policy)
    write_json(paths["actual_path_data_quality_report"], actual_path_data_quality_report)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary=summary))
    write_markdown(paths["event_doc"], _render_event_outcome_doc(summary=summary))
    write_markdown(paths["cluster_doc"], _render_cluster_outcome_doc(summary=summary))
    write_markdown(
        paths["classification_doc"],
        _render_classification_doc(
            false_warning_report=false_warning_report,
            missed_upside_report=missed_upside_report,
            downside_capture_report=downside_capture_report,
        ),
    )
    write_markdown(
        paths["readiness_doc"],
        _render_2339_readiness_doc(readiness=readiness, task_route=task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _price_outcome(
    *,
    price_matrix: pd.DataFrame,
    target_asset: str,
    start_date: date,
    horizon_days: int,
    due_status: str,
    outcome_as_of_date: date,
) -> dict[str, Any]:
    if due_status == "NOT_DUE":
        return _empty_outcome("OUTCOME_NOT_DUE", "OUTCOME_PENDING_NOT_DUE")
    if horizon_days <= 0:
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "INVALID_HORIZON")
    if target_asset not in price_matrix.columns:
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "TARGET_ASSET_MISSING")
    series = price_matrix[target_asset].dropna().astype(float).sort_index()
    if series.empty:
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "TARGET_ASSET_NO_PRICES")
    start_ts = pd.Timestamp(start_date)
    start_pos = int(series.index.searchsorted(start_ts, side="left"))
    if start_pos >= len(series):
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "START_PRICE_MISSING")
    end_pos = start_pos + horizon_days
    if end_pos >= len(series):
        return _empty_outcome(
            "OUTCOME_NOT_DUE",
            "OUTCOME_PENDING_TRADING_CALENDAR_NOT_MATURED",
        )
    end_ts = pd.Timestamp(series.index[end_pos])
    if end_ts.date() > outcome_as_of_date:
        return _empty_outcome("OUTCOME_NOT_DUE", "OUTCOME_PENDING_NOT_DUE")
    window = series.iloc[start_pos : end_pos + 1].dropna()
    if len(window) < 2:
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "PRICE_WINDOW_INCOMPLETE")
    start_price = float(window.iloc[0])
    end_price = float(window.iloc[-1])
    if start_price <= 0.0 or end_price <= 0.0:
        return _empty_outcome("OUTCOME_BLOCKED_MARKET_DATA", "NON_POSITIVE_PRICE")
    path_returns = window / start_price - 1.0
    daily_returns = window.pct_change().dropna()
    realized_volatility = 0.0
    if not daily_returns.empty:
        realized_volatility = float(daily_returns.std(ddof=0)) * math.sqrt(
            ANNUALIZED_VOLATILITY_TRADING_DAYS
        )
    return {
        "market_data_start_date": pd.Timestamp(window.index[0]).date().isoformat(),
        "market_data_end_date": pd.Timestamp(window.index[-1]).date().isoformat(),
        "forward_return": round_float(end_price / start_price - 1.0),
        "forward_max_drawdown": round_float(float(path_returns.min())),
        "forward_min_return": round_float(float(path_returns.min())),
        "forward_max_return": round_float(float(path_returns.max())),
        "realized_volatility": round_float(realized_volatility),
        "outcome_quality_status": "PASS",
        "outcome_binding_status": "OUTCOME_BOUND",
    }


def _classify_outcome(
    *,
    outcome: Mapping[str, Any],
    horizon: str,
    classification_policy: Mapping[str, Any],
) -> dict[str, Any]:
    if outcome.get("outcome_binding_status") != "OUTCOME_BOUND":
        return {
            "stress_detected": False,
            "rebound_detected": False,
            "false_warning_candidate": False,
            "missed_upside_candidate": False,
            "downside_capture_candidate": False,
            "manual_review_would_have_helped_candidate": False,
        }
    stress_threshold = to_float(
        mapping(classification_policy.get("stress_threshold_by_horizon")).get(horizon)
    )
    rebound_threshold = to_float(
        mapping(classification_policy.get("rebound_threshold_by_horizon")).get(horizon)
    )
    missed_upside_threshold = to_float(
        mapping(classification_policy.get("missed_upside_threshold_by_horizon")).get(
            horizon
        )
    )
    downside_threshold = to_float(
        mapping(classification_policy.get("downside_capture_threshold_by_horizon")).get(
            horizon
        )
    )
    forward_return = to_float(outcome.get("forward_return"))
    forward_min_return = to_float(outcome.get("forward_min_return"))
    forward_max_return = to_float(outcome.get("forward_max_return"))
    forward_max_drawdown = to_float(outcome.get("forward_max_drawdown"))
    stress = min(forward_min_return, forward_max_drawdown) <= stress_threshold
    rebound = max(forward_return, forward_max_return) >= rebound_threshold
    downside_capture = min(forward_min_return, forward_max_drawdown) <= downside_threshold
    missed_upside = max(forward_return, forward_max_return) >= missed_upside_threshold
    false_warning = (not stress) and (rebound or missed_upside)
    manual_review_helped = stress or downside_capture or false_warning or missed_upside
    return {
        "stress_detected": stress,
        "rebound_detected": rebound,
        "false_warning_candidate": false_warning,
        "missed_upside_candidate": missed_upside,
        "downside_capture_candidate": downside_capture,
        "manual_review_would_have_helped_candidate": manual_review_helped,
    }


def _empty_outcome(status: str, quality_status: str) -> dict[str, Any]:
    return {
        "market_data_start_date": None,
        "market_data_end_date": None,
        "forward_return": None,
        "forward_max_drawdown": None,
        "forward_min_return": None,
        "forward_max_return": None,
        "realized_volatility": None,
        "outcome_quality_status": quality_status,
        "outcome_binding_status": status,
    }


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_outcome_binder_summary.json",
        "event_matrix_json": output_dir
        / "high_intensity_event_actual_path_outcome_matrix.json",
        "event_matrix_csv": output_dir
        / "high_intensity_event_actual_path_outcome_matrix.csv",
        "cluster_matrix_json": output_dir
        / "high_intensity_cluster_actual_path_outcome_matrix.json",
        "cluster_matrix_csv": output_dir
        / "high_intensity_cluster_actual_path_outcome_matrix.csv",
        "trigger_context_json": output_dir
        / "high_intensity_trigger_day_actual_path_context.json",
        "trigger_context_csv": output_dir
        / "high_intensity_trigger_day_actual_path_context.csv",
        "coverage_report": output_dir / "high_intensity_outcome_coverage_report.json",
        "horizon_quality_report": output_dir
        / "high_intensity_horizon_outcome_quality_report.json",
        "rebound_stress_json": output_dir
        / "high_intensity_rebound_stress_classification_matrix.json",
        "rebound_stress_csv": output_dir
        / "high_intensity_rebound_stress_classification_matrix.csv",
        "false_warning_report": output_dir
        / "high_intensity_false_warning_classification_report.json",
        "missed_upside_report": output_dir
        / "high_intensity_missed_upside_classification_report.json",
        "downside_capture_report": output_dir
        / "high_intensity_downside_capture_classification_report.json",
        "manual_review_report": output_dir
        / "high_intensity_manual_review_usefulness_proxy_report.json",
        "cluster_policy": output_dir / "high_intensity_cluster_weighting_policy.json",
        "actual_path_data_quality_report": output_dir
        / "high_intensity_actual_path_data_quality_report.json",
        "interpretation_boundary": output_dir
        / "high_intensity_outcome_binder_interpretation_boundary.json",
        "readiness": output_dir / "high_intensity_2339_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2339_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_outcome_binder_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_actual_path_outcome_binder.md",
        "event_doc": docs_root / "high_intensity_event_actual_path_outcomes.md",
        "cluster_doc": docs_root / "high_intensity_cluster_actual_path_outcomes.md",
        "classification_doc": docs_root
        / "high_intensity_false_warning_missed_upside_downside_capture.md",
        "readiness_doc": docs_root / "high_intensity_2339_readiness_route.md",
    }


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


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityOutcomeBinderError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityOutcomeBinderError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensityOutcomeBinderError(
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


def _safety_violation_count(row: Mapping[str, Any]) -> int:
    count = 0
    for key, expected in SAFETY_FIELDS.items():
        if key in row and row.get(key) != expected:
            count += 1
    return count


def _resolve_market_data_source(path: Path) -> Path:
    if path.is_file():
        return path
    candidate = path / "prices_daily.csv"
    if candidate.exists():
        return candidate
    raise HighIntensityOutcomeBinderError(f"market data price cache missing: {path}")


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _target_assets(collections: Sequence[Sequence[Mapping[str, Any]]]) -> list[str]:
    assets: list[str] = []
    for collection in collections:
        for row in collection:
            asset = str(row.get("target_asset", ""))
            if asset and asset not in assets:
                assets.append(asset)
    return sorted(assets)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise HighIntensityOutcomeBinderError(f"date must use YYYY-MM-DD: {value}") from exc


def _add_business_days(start: date, days: int) -> date:
    current = start
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _outcome_due_status(due_date: date, as_of: date) -> str:
    if due_date > as_of:
        return "NOT_DUE"
    if due_date == as_of:
        return "DUE"
    return "HISTORICAL_DUE"


def _price_calendar_due_status(
    *,
    price_matrix: pd.DataFrame,
    target_asset: str,
    start_date: date,
    horizon_days: int,
    fallback_due_date: date,
    outcome_as_of_date: date,
) -> str:
    fallback = _outcome_due_status(fallback_due_date, outcome_as_of_date)
    if fallback == "NOT_DUE" or target_asset not in price_matrix.columns:
        return fallback
    series = price_matrix[target_asset].dropna().sort_index()
    if series.empty:
        return fallback
    start_pos = int(series.index.searchsorted(pd.Timestamp(start_date), side="left"))
    end_pos = start_pos + horizon_days
    if start_pos >= len(series) or end_pos >= len(series):
        latest_price_date = pd.Timestamp(series.index[-1]).date()
        return "NOT_DUE" if latest_price_date >= outcome_as_of_date else fallback
    end_date = pd.Timestamp(series.index[end_pos]).date()
    if end_date > outcome_as_of_date:
        return "NOT_DUE"
    if end_date == outcome_as_of_date:
        return "DUE"
    return "HISTORICAL_DUE"


def _coverage_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, dict[str, int]]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        grouped[str(row.get(key, ""))][str(row.get("outcome_binding_status", ""))] += 1
    return {group: dict(counter) for group, counter in sorted(grouped.items())}


def _candidate_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key, "")) for row in rows).items()))


def _unique_values(rows: Sequence[Mapping[str, Any]], key: str) -> set[str]:
    return {str(row.get(key, "")) for row in rows if str(row.get(key, ""))}


def _bound_rows(rows: Sequence[Mapping[str, Any]], status_key: str) -> list[Mapping[str, Any]]:
    return [row for row in rows if row.get(status_key) == "OUTCOME_BOUND"]


def _rate_label(rate_value: float, *, prefix: str, inconclusive: bool) -> str:
    if inconclusive:
        return f"{prefix}_INCONCLUSIVE"
    if rate_value >= HIGH_RATE_THRESHOLD:
        return f"{prefix}_HIGH"
    if rate_value >= MODERATE_RATE_THRESHOLD:
        return f"{prefix}_MODERATE"
    return f"{prefix}_LOW"


def _return_context(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [
        to_float(row.get("cluster_forward_return"))
        for row in rows
        if row.get("cluster_forward_return") is not None
    ]
    return {
        "sample_count": len(returns),
        "average_forward_return": round_float(
            sum(returns) / len(returns) if returns else 0.0
        ),
        "max_forward_return": round_float(max(returns) if returns else 0.0),
    }


def _duplicate_count(values: Sequence[str]) -> int:
    counts = Counter(values)
    return sum(count - 1 for value, count in counts.items() if value and count > 1)


def _summary_status(readiness: Mapping[str, Any], coverage: Mapping[str, Any]) -> str:
    readiness_status = str(readiness.get("readiness_status", ""))
    coverage_status = str(coverage.get("coverage_status", ""))
    if coverage_status == "BLOCKED":
        return "BLOCKED_ZERO_EVENTS"
    if readiness_status == "PARTIAL_OUTCOME_REVIEW_REQUIRED":
        return "PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS"
    if readiness_status == "DATA_REMEDIATION_REQUIRED":
        return "BLOCKED_MARKET_DATA_COVERAGE"
    if readiness_status == "READY_FOR_2339_FORWARD_OUTCOME_REVIEW_WITH_WARNINGS":
        return "HIGH_INTENSITY_OUTCOME_BINDER_READY_WITH_WARNINGS_PROMOTION_BLOCKED"
    return "HIGH_INTENSITY_OUTCOME_BINDER_READY_PROMOTION_BLOCKED"


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text not in {"", "false", "0", "none", "no"}


def _emits_action(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "none", "false", "no"}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return len(value) > 0
    if isinstance(value, Mapping):
        return bool(value)
    return bool(value)


def _short_id(prefix: str, *parts: str) -> str:
    payload = "|".join(parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _render_main_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Actual Path Outcome Binder",
            "",
            "TRADING-2337 只绑定 TRADING-2336 observe-only event 的 actual-path "
            "outcome，不修改原始 event log，不输出仓位建议。",
            "",
            f"- status: `{summary.get('status', '')}`",
            f"- selected_market_regime: `{summary.get('market_regime', '')}`",
            f"- event_count: `{summary.get('event_count', 0)}`",
            f"- cluster_count: `{summary.get('cluster_count', 0)}`",
            f"- event_actual_path_outcome_count: "
            f"`{summary.get('event_actual_path_outcome_count', 0)}`",
            f"- cluster_actual_path_outcome_count: "
            f"`{summary.get('cluster_actual_path_outcome_count', 0)}`",
            f"- coverage_status: `{summary.get('coverage_status', '')}`",
            f"- validate_data_status: `{summary.get('validate_data_status', '')}`",
            f"- validate_data_as_of: `{summary.get('validate_data_as_of', '')}`",
            f"- readiness_status: `{summary.get('readiness_status', '')}`",
            f"- next_task: `{summary.get('next_task', '')}`",
            "- primary_analysis_level: `cluster`",
            "- trigger_day_level_usage: `context_only`",
            "- promotion_allowed / paper_shadow_allowed / production_allowed: `False`",
            "- broker_action: `none`",
            "",
        ]
    )


def _render_event_outcome_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Event Actual Path Outcomes",
            "",
            "Event-level matrix 为每个 de-duplicated observe event 保存 "
            "`1d / 5d / 10d / 20d` actual-path outcome。该层级用于保留事件细节，"
            "后续主统计仍以 cluster-level 为准。",
            "",
            f"- event_actual_path_outcome_count: "
            f"`{summary.get('event_actual_path_outcome_count', 0)}`",
            "- original_event_log_mutated: `False`",
            "",
        ]
    )


def _render_cluster_outcome_doc(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Cluster Actual Path Outcomes",
            "",
            "Cluster-level matrix 以 `primary_event_id / cluster_start_date` 为起点，"
            "连续 trigger days 不重复计入主样本。这是 TRADING-2339 forward outcome "
            "review 的主要分析单位。",
            "",
            f"- cluster_actual_path_outcome_count: "
            f"`{summary.get('cluster_actual_path_outcome_count', 0)}`",
            "- primary_analysis_level: `cluster`",
            "",
        ]
    )


def _render_classification_doc(
    *,
    false_warning_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    downside_capture_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity False Warning / Missed Upside / Downside Capture",
            "",
            "Classification thresholds 是 research candidate，不是 production "
            "threshold，也不会反向修改 trigger rule。",
            "",
            f"- false_warning_label: "
            f"`{false_warning_report.get('false_warning_label', '')}`",
            f"- missed_upside_label: "
            f"`{missed_upside_report.get('missed_upside_label', '')}`",
            f"- downside_capture_label: "
            f"`{downside_capture_report.get('downside_capture_label', '')}`",
            "",
        ]
    )


def _render_2339_readiness_doc(
    *,
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2339 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status', '')}`",
            f"- next_task: `{task_route.get('next_task', '')}`",
            f"- warnings: `{readiness.get('readiness_warnings', [])}`",
            f"- blockers: `{readiness.get('readiness_blockers', [])}`",
            "",
            "TRADING-2339 才能解读 actual-path outcome；TRADING-2337 不做 "
            "owner final decision，不进入 paper-shadow、production 或 broker。",
            "",
        ]
    )
