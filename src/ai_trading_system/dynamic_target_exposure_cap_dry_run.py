from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import DataQualityReport
from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DRY_RUN_READINESS_ROOT,
)
from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    DEFAULT_SOURCE_REMEDIATION_ROOT,
    DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    NEXT_DRY_RUN_TASK,
    load_trading_2330_timestamp_remediation_outputs,
    validate_no_unsafe_fields,
)
from ai_trading_system.dynamic_target_baseline_preparation import (
    DEFAULT_SIMULATION_POLICY_ROOT,
    DEFAULT_SOURCE_BINDING_ROOT,
    DEFAULT_STATIC_DRY_RUN_ROOT,
    DynamicTargetBaselinePreparationError,
    load_trading_2323_policy_outputs,
    load_trading_2326_static_dry_run_outputs,
)
from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    load_trading_2329_source_remediation_outputs,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    load_adjusted_price_matrix,
    mapping,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.source_bound_static_etf_dry_run import (
    DEFAULT_POLICY_PATH,
    SAFETY_FIELDS,
    build_portfolio_level_trigger_map,
    load_exposure_cap_policy,
    load_source_binding_outputs,
    run_data_quality_gate,
)

TASK_ID = "TRADING-2332_SOURCE_BOUND_EXPOSURE_CAP_DRY_RUN_WITH_DYNAMIC_TARGET_BASELINE"
REPORT_TYPE = "source_bound_exposure_cap_dynamic_target_dry_run"
ARTIFACT_ROLE = "source_bound_dynamic_target_exposure_cap_dry_run"
MODE = "dynamic_target_baseline_dry_run"
STATUS = "SOURCE_BOUND_DYNAMIC_TARGET_EXPOSURE_CAP_DRY_RUN_READY_PROMOTION_BLOCKED"
BLOCKED_STATUS = "SOURCE_BOUND_DYNAMIC_TARGET_EXPOSURE_CAP_DRY_RUN_DATA_QUALITY_BLOCKED"
PORTFOLIO_SOURCE_MODE = "dynamic_target_baseline_wrapper_with_pit_caveat"
EXPECTED_READINESS = {
    "DYNAMIC_DRY_RUN_READY_FOR_2332_WITH_PIT_CAVEAT",
    "DYNAMIC_DRY_RUN_READY_FOR_2332_WITH_WARNINGS",
}
EXPECTED_KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
NEXT_DIAGNOSTICS_TASK = "TRADING-2333_Dynamic_Exposure_Cap_vs_No_Cap_Diagnostics_Review"
NEXT_POLICY_TASK = "TRADING-2333_Dynamic_Exposure_Cap_Policy_Refinement_Plan"
NEXT_DATA_TASK = "TRADING-2333_Dynamic_Target_Baseline_Data_Remediation"
NEXT_ARCHIVE_TASK = "TRADING-2333_Archive_Dynamic_Exposure_Cap_Dry_Run"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"


class DynamicTargetExposureCapDryRunError(ValueError):
    pass


def run_dynamic_target_exposure_cap_dry_run(
    *,
    dry_run_readiness_dir: Path = DEFAULT_DRY_RUN_READINESS_ROOT,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    source_remediation_dir: Path = DEFAULT_SOURCE_REMEDIATION_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    static_dry_run_dir: Path = DEFAULT_STATIC_DRY_RUN_ROOT,
    market_data_source: Path = DEFAULT_PRICES_PATH,
    rates_source: Path = DEFAULT_RATES_PATH,
    marketstack_prices_source: Path | None = None,
    policy_path: Path | None = None,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicTargetExposureCapDryRunError(
            f"dynamic target exposure-cap dry-run only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_target_exposure_cap_dry_run_inputs(
        dry_run_readiness_dir=dry_run_readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        source_remediation_dir=source_remediation_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        static_dry_run_dir=static_dry_run_dir,
    )
    wrapper_rows = _rows_from_payload(inputs["timestamp_remediation"]["wrapper"])
    assets = dynamic_target_assets_from_wrapper(wrapper_rows)
    if not assets:
        raise DynamicTargetExposureCapDryRunError(
            "TRADING-2332 requires timestamp-remediated wrapper target assets"
        )

    source_binding = mapping(inputs["source_binding"])
    selected_policy_path = _policy_path(policy_path, source_binding)
    policy = load_exposure_cap_policy(selected_policy_path)
    trigger_frame = load_portfolio_level_risk_cap_trigger_frame_from_source_binding(
        source_binding
    )
    price_matrix = load_adjusted_price_matrix(market_data_source, assets)
    all_schedule_rows = build_dynamic_target_baseline_exposure_schedule(
        wrapper_rows=wrapper_rows,
    )
    simulation_dates = build_dynamic_target_simulation_calendar_from_sources(
        trigger_frame=trigger_frame,
        price_matrix=price_matrix,
        schedule_rows=all_schedule_rows,
        target_assets=assets,
    )
    if not simulation_dates:
        raise DynamicTargetExposureCapDryRunError(
            "no overlapping dynamic target dry-run calendar"
        )
    schedule_rows = [
        row
        for row in all_schedule_rows
        if date.fromisoformat(str(row["date"])) in set(simulation_dates)
    ]

    quality_report, quality_report_path = run_data_quality_gate(
        market_data_source=market_data_source,
        rates_source=rates_source,
        marketstack_prices_source=marketstack_prices_source,
        target_assets=assets,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
    )
    date_trigger_map = build_portfolio_level_trigger_map(trigger_frame)
    alignment_rows = build_dynamic_target_risk_cap_trigger_alignment_matrix(
        simulation_dates=simulation_dates,
        target_assets=assets,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        date_trigger_map=date_trigger_map,
        trigger_source_hash=str(trigger_frame.attrs.get("source_hash", "")),
        risk_cap_trigger_source_available=not trigger_frame.empty,
    )
    data_quality_report = build_dynamic_target_data_quality_report(
        quality_report=quality_report,
        alignment_rows=alignment_rows,
        schedule_rows=schedule_rows,
        wrapper_rows=wrapper_rows,
        trigger_frame=trigger_frame,
        policy=policy,
        wrapper_validation=mapping(
            inputs["timestamp_remediation"]["wrapper_validation"]
        ),
    )
    full_dry_run_allowed = data_quality_report["data_quality_status"] != "FAIL"
    dry_run_rows: list[dict[str, Any]] = []
    if full_dry_run_allowed:
        dry_run_rows = build_dynamic_target_exposure_cap_dry_run_rows(
            policy=policy,
            simulation_dates=simulation_dates,
            schedule_rows=schedule_rows,
            price_matrix=price_matrix,
            alignment_rows=alignment_rows,
            date_trigger_map=date_trigger_map,
            target_assets=assets,
            data_quality_status=data_quality_report["data_quality_status"],
        )

    comparison = build_dynamic_target_cap_vs_no_cap_comparison(
        dry_run_rows=dry_run_rows,
        data_quality_status=data_quality_report["data_quality_status"],
    )
    binding_rows = build_dynamic_target_cap_binding_day_matrix(dry_run_rows)
    exposure_reduction = build_dynamic_target_exposure_reduction_report(dry_run_rows)
    return_drawdown = build_dynamic_target_return_drawdown_proxy_report(dry_run_rows)
    turnover_report = build_dynamic_target_turnover_impact_report(dry_run_rows)
    cooldown_report = build_dynamic_target_cooldown_impact_report(dry_run_rows)
    false_cost_report = build_dynamic_target_false_risk_cap_cost_report(dry_run_rows)
    missed_upside_report = build_dynamic_target_missed_upside_cost_report(dry_run_rows)
    downside_report = build_dynamic_target_downside_protection_proxy_report(dry_run_rows)
    overlap_report = build_dynamic_target_strategy_overlap_report(dry_run_rows)
    static_dynamic = build_dynamic_target_static_vs_dynamic_comparison(
        static_dry_run=mapping(inputs["static_dry_run"]),
        dynamic_comparison=comparison,
        dynamic_false_cost=false_cost_report,
        dynamic_downside=downside_report,
        overlap_report=overlap_report,
    )
    interpretation_boundary = build_dynamic_target_pit_caveat_interpretation_boundary(
        generated_at=generated_at,
        data_quality_status=data_quality_report["data_quality_status"],
    )
    task_route = build_dynamic_target_2333_task_route(
        comparison=comparison,
        data_quality_report=data_quality_report,
        false_cost_report=false_cost_report,
    )
    source_report = build_dynamic_target_baseline_source_report(
        wrapper_rows=wrapper_rows,
        schedule_rows=schedule_rows,
        inputs=inputs,
    )
    summary = build_dynamic_target_exposure_cap_dry_run_summary(
        generated_at=generated_at,
        dry_run_readiness_dir=dry_run_readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        source_remediation_dir=source_remediation_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        static_dry_run_dir=static_dry_run_dir,
        market_data_source=market_data_source,
        rates_source=rates_source,
        policy_path=selected_policy_path,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        target_assets=assets,
        schedule_rows=schedule_rows,
        alignment_rows=alignment_rows,
        dry_run_rows=dry_run_rows,
        comparison=comparison,
        data_quality_report=data_quality_report,
        task_route=task_route,
        full_dry_run_allowed=full_dry_run_allowed,
    )
    paths = write_dynamic_target_exposure_cap_dry_run_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        source_report=source_report,
        schedule_rows=schedule_rows,
        alignment_rows=alignment_rows,
        dry_run_rows=dry_run_rows,
        comparison=comparison,
        binding_rows=binding_rows,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        turnover_report=turnover_report,
        cooldown_report=cooldown_report,
        false_cost_report=false_cost_report,
        missed_upside_report=missed_upside_report,
        downside_report=downside_report,
        overlap_report=overlap_report,
        static_dynamic=static_dynamic,
        data_quality_report=data_quality_report,
        interpretation_boundary=interpretation_boundary,
        task_route=task_route,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_dynamic_target_exposure_cap_dry_run_inputs(
    *,
    dry_run_readiness_dir: Path,
    timestamp_remediation_dir: Path,
    source_remediation_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    static_dry_run_dir: Path,
) -> dict[str, Any]:
    readiness = load_trading_2331_dry_run_readiness_outputs(dry_run_readiness_dir)
    timestamp_remediation = load_trading_2330_timestamp_remediation_outputs(
        timestamp_remediation_dir
    )
    source_remediation = load_trading_2329_source_remediation_outputs(
        source_remediation_dir
    )
    source_binding = load_source_binding_outputs(source_binding_dir)
    try:
        policy_context = load_trading_2323_policy_outputs(simulation_policy_dir)
        static_dry_run = load_trading_2326_static_dry_run_outputs(static_dry_run_dir)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetExposureCapDryRunError(str(exc)) from exc
    static_dry_run = {
        **static_dry_run,
        "turnover_report": _load_optional_json(
            static_dry_run_dir / "exposure_cap_turnover_impact_report.json"
        ),
        "cooldown_report": _load_optional_json(
            static_dry_run_dir / "exposure_cap_cooldown_impact_report.json"
        ),
        "false_cost_report": _load_optional_json(
            static_dry_run_dir / "exposure_cap_false_risk_cap_cost_report.json"
        ),
        "missed_upside_report": _load_optional_json(
            static_dry_run_dir / "exposure_cap_missed_upside_cost_report.json"
        ),
        "downside_report": _load_optional_json(
            static_dry_run_dir / "exposure_cap_downside_protection_proxy_report.json"
        ),
    }
    return {
        "dry_run_readiness": readiness,
        "timestamp_remediation": timestamp_remediation,
        "source_remediation": source_remediation,
        "source_binding": source_binding,
        "policy_context": policy_context,
        "static_dry_run": static_dry_run,
    }


def load_trading_2331_dry_run_readiness_outputs(
    dry_run_readiness_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": dry_run_readiness_dir / "dynamic_dry_run_readiness_summary.json",
        "gate_checklist": dry_run_readiness_dir / "dynamic_dry_run_gate_checklist.json",
        "pit_acceptance": dry_run_readiness_dir
        / "dynamic_dry_run_pit_caveat_acceptance_report.json",
        "wrapper_field_validation": dry_run_readiness_dir
        / "dynamic_dry_run_wrapper_field_validation_matrix.json",
        "timestamp_alignment": dry_run_readiness_dir
        / "dynamic_dry_run_timestamp_alignment_matrix.json",
        "risk_cap_alignment": dry_run_readiness_dir
        / "dynamic_dry_run_risk_cap_alignment_matrix.json",
        "market_data_alignment": dry_run_readiness_dir
        / "dynamic_dry_run_market_data_alignment_matrix.json",
        "policy_compatibility": dry_run_readiness_dir
        / "dynamic_dry_run_policy_compatibility_matrix.json",
        "input_contract": dry_run_readiness_dir / "dynamic_dry_run_input_contract.json",
        "data_quality_precheck": dry_run_readiness_dir
        / "dynamic_dry_run_data_quality_precheck.json",
        "interpretation_boundary": dry_run_readiness_dir
        / "dynamic_dry_run_interpretation_boundary.json",
        "readiness": dry_run_readiness_dir / "dynamic_dry_run_2332_readiness_matrix.json",
        "task_route": dry_run_readiness_dir / "dynamic_dry_run_2332_task_route.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2331 dry-run readiness")
    for key, payload in payloads.items():
        _validate_safe(f"TRADING-2331 {key}", payload)
    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    if summary.get("2332_allowed") is not True or readiness.get("2332_allowed") is not True:
        raise DynamicTargetExposureCapDryRunError(
            "TRADING-2332 requires TRADING-2331 2332_allowed=true"
        )
    if str(summary.get("readiness_status")) not in EXPECTED_READINESS:
        raise DynamicTargetExposureCapDryRunError(
            "TRADING-2332 requires TRADING-2331 dynamic dry-run readiness status"
        )
    if str(summary.get("next_task")) != NEXT_DRY_RUN_TASK or str(
        task_route.get("next_task")
    ) != NEXT_DRY_RUN_TASK:
        raise DynamicTargetExposureCapDryRunError(
            "TRADING-2332 requires TRADING-2331 route to dynamic dry-run"
        )
    if summary.get("simulation_executed") is not False:
        raise DynamicTargetExposureCapDryRunError(
            "TRADING-2331 must not have executed dynamic dry-run"
        )
    return {
        "source_dir": str(dry_run_readiness_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def dynamic_target_assets_from_wrapper(
    wrapper_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    return sorted(
        {
            str(row.get("target_asset", "")).upper()
            for row in wrapper_rows
            if str(row.get("target_asset", "")).strip()
        }
    )


def load_portfolio_level_risk_cap_trigger_frame_from_source_binding(
    source_binding: Mapping[str, Any],
) -> pd.DataFrame:
    trigger_report = mapping(source_binding.get("risk_cap_trigger_binding"))
    source_path_value = trigger_report.get("source_path")
    if not source_path_value:
        raise DynamicTargetExposureCapDryRunError("risk-cap trigger source_path missing")
    source_path = Path(str(source_path_value))
    if not source_path.exists():
        raise DynamicTargetExposureCapDryRunError(
            f"risk-cap trigger series missing: {source_path}"
        )
    frame = pd.read_csv(source_path)
    required = {
        "target_asset",
        "horizon",
        "source_date",
        "scope_active",
        "usage_role",
        "risk_cap_score",
        "risk_cap_intensity",
    }
    missing = required - set(frame.columns)
    if missing:
        raise DynamicTargetExposureCapDryRunError(
            f"risk-cap trigger series missing columns: {sorted(missing)}"
        )
    frame = frame.copy()
    frame["scope_active_bool"] = frame["scope_active"].map(_to_bool)
    frame = frame.loc[
        (frame["scope_active_bool"])
        & (frame["usage_role"].astype(str) == "risk_cap_only")
    ].copy()
    if frame.empty:
        raise DynamicTargetExposureCapDryRunError(
            "risk-cap trigger series has no active rows"
        )
    frame["trigger_date"] = pd.to_datetime(frame["source_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["trigger_date"])
    frame["risk_cap_score_numeric"] = pd.to_numeric(
        frame["risk_cap_score"],
        errors="coerce",
    ).fillna(0.0)
    frame.attrs["source_path"] = str(source_path)
    frame.attrs["source_hash"] = _file_hash(source_path)
    return frame


def build_dynamic_target_baseline_exposure_schedule(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for index, row in enumerate(wrapper_rows):
        _validate_safe(f"dynamic target wrapper row {index}", row)
        target_asset = str(row.get("target_asset", "")).upper()
        effective_date = _timestamp_date(row.get("valid_from")) or _timestamp_date(
            row.get("date")
        )
        if not target_asset or effective_date is None:
            continue
        key = (effective_date.isoformat(), target_asset)
        schedule = {
            "date": effective_date.isoformat(),
            "source_signal_date": str(row.get("date", "")),
            "target_asset": target_asset,
            "baseline_id": str(row.get("baseline_id", "")),
            "source_id": str(row.get("source_id", "")),
            "source_family": str(row.get("source_family", "")),
            "target_exposure": round_float(row.get("target_exposure")),
            "risk_asset_exposure": round_float(row.get("risk_asset_exposure")),
            "asset_weight": round_float(row.get("asset_weight")),
            "cash_weight": round_float(row.get("cash_weight")),
            "as_of_timestamp": str(row.get("as_of_timestamp", "")),
            "decision_timestamp": str(row.get("decision_timestamp", "")),
            "valid_from": str(row.get("valid_from", "")),
            "valid_until": str(row.get("valid_until", "")),
            "rebalance_flag": bool(row.get("rebalance_flag", False)),
            "rebalance_timestamp": str(row.get("rebalance_timestamp", "")),
            "known_at_policy": str(row.get("known_at_policy", "")),
            "pit_policy": str(row.get("pit_policy", "")),
            "latency_policy": str(row.get("latency_policy", "")),
            "source_artifact_hash": str(row.get("source_artifact_hash", "")),
            "source_hash": str(row.get("source_hash", "")),
            "wrapper_record_id": f"{row.get('baseline_id', '')}:{row.get('source_id', '')}:{index}",
            **SAFETY_FIELDS,
        }
        existing = latest_by_key.get(key)
        if existing is None or str(schedule["decision_timestamp"]) >= str(
            existing.get("decision_timestamp", "")
        ):
            latest_by_key[key] = schedule
    return clean_for_yaml(
        [latest_by_key[key] for key in sorted(latest_by_key)]
    )


def build_dynamic_target_simulation_calendar_from_sources(
    *,
    trigger_frame: pd.DataFrame,
    price_matrix: pd.DataFrame,
    schedule_rows: Sequence[Mapping[str, Any]],
    target_assets: Sequence[str],
) -> list[date]:
    trigger_dates = sorted(set(trigger_frame["trigger_date"]))
    complete_prices = price_matrix.loc[:, list(target_assets)].dropna(how="any")
    schedule_assets: dict[date, set[str]] = defaultdict(set)
    for row in schedule_rows:
        current = _parse_date(str(row.get("date", "")))
        if current is not None:
            schedule_assets[current].add(str(row.get("target_asset", "")).upper())
    complete_schedule_dates = {
        current
        for current, assets in schedule_assets.items()
        if set(target_assets).issubset(assets)
    }
    if not trigger_dates or complete_prices.empty or not complete_schedule_dates:
        return []
    start = max(
        trigger_dates[0],
        date.fromisoformat(DEFAULT_BACKTEST_START),
        min(complete_schedule_dates),
    )
    end = min(
        trigger_dates[-1],
        pd.Timestamp(complete_prices.index.max()).date(),
        max(complete_schedule_dates),
    )
    if end < start:
        return []
    mask = (complete_prices.index.date >= start) & (complete_prices.index.date <= end)
    return [
        pd.Timestamp(ts).date()
        for ts in complete_prices.loc[mask].index
        if pd.Timestamp(ts).date() in complete_schedule_dates
    ]


def build_dynamic_target_risk_cap_trigger_alignment_matrix(
    *,
    simulation_dates: Sequence[date],
    target_assets: Sequence[str],
    schedule_rows: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    date_trigger_map: Mapping[str, Mapping[str, Any]],
    trigger_source_hash: str,
    risk_cap_trigger_source_available: bool,
) -> list[dict[str, Any]]:
    schedule_by_key = {
        (str(row.get("date")), str(row.get("target_asset"))): row
        for row in schedule_rows
    }
    rows: list[dict[str, Any]] = []
    for current in simulation_dates:
        current_key = current.isoformat()
        trigger = mapping(date_trigger_map.get(current_key))
        for asset in target_assets:
            schedule = mapping(schedule_by_key.get((current_key, asset)))
            market_available = _market_data_available(price_matrix, asset, current)
            target_available = bool(schedule)
            timestamp_status = _timestamp_alignment_status(schedule, current)
            ineligible: list[str] = []
            if not target_available:
                ineligible.append("missing_dynamic_target")
            if not market_available:
                ineligible.append("missing_market_data")
            if timestamp_status != "NEXT_SESSION_ALIGNED_WITH_PIT_CAVEAT":
                ineligible.append("timestamp_alignment_not_eligible")
            rows.append(
                {
                    "date": current_key,
                    "target_asset": asset,
                    "baseline_id": str(schedule.get("baseline_id", "")),
                    "target_exposure_available": target_available,
                    "risk_cap_trigger_available": risk_cap_trigger_source_available,
                    "market_data_available": market_available,
                    "risk_cap_triggered": bool(trigger.get("risk_cap_triggered", False)),
                    "risk_cap_intensity": str(trigger.get("risk_cap_intensity", "none")),
                    "risk_cap_score": round_float(trigger.get("risk_cap_score", 0.0)),
                    "scope_active": bool(trigger.get("scope_active", False)),
                    "signal_direction": str(trigger.get("signal_direction", "none")),
                    "decision_timestamp": str(schedule.get("decision_timestamp", "")),
                    "risk_cap_decision_timestamp": f"{current_key}T00:00:00+00:00"
                    if trigger
                    else "",
                    "timestamp_alignment_status": timestamp_status,
                    "trigger_source_hash": trigger_source_hash,
                    "simulation_eligible": not ineligible,
                    "ineligible_reason": ";".join(ineligible),
                    **SAFETY_FIELDS,
                }
            )
    return clean_for_yaml(rows)


def build_dynamic_target_exposure_cap_dry_run_rows(
    *,
    policy: Mapping[str, Any],
    simulation_dates: Sequence[date],
    schedule_rows: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    alignment_rows: Sequence[Mapping[str, Any]],
    date_trigger_map: Mapping[str, Mapping[str, Any]],
    target_assets: Sequence[str],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    cap_policy = mapping(policy.get("cap_policy"))
    cooldown_policy = mapping(policy.get("cooldown_policy"))
    cap_by_intensity = mapping(cap_policy.get("max_allowed_exposure_by_intensity"))
    default_cap = to_float(cap_policy.get("default_max_allowed_exposure"), 1.0)
    cooldown_by_intensity = mapping(cooldown_policy.get("cooldown_days_by_intensity"))
    default_cooldown = int(to_float(cooldown_policy.get("default_cooldown_days"), 3.0))
    schedule_by_key = {
        (str(row.get("date")), str(row.get("target_asset"))): row
        for row in schedule_rows
    }
    eligible = {
        (str(row.get("date")), str(row.get("target_asset"))): bool(
            row.get("simulation_eligible")
        )
        for row in alignment_rows
    }
    date_index = {pd.Timestamp(ts).date(): pos for pos, ts in enumerate(price_matrix.index)}
    first_date = simulation_dates[0].isoformat()
    previous_final = {
        asset: to_float(schedule_by_key.get((first_date, asset), {}).get("target_exposure"))
        for asset in target_assets
    }
    cooldown_remaining = 0
    cooldown_cap = default_cap
    previous_cap_active = False
    rows: list[dict[str, Any]] = []
    for current in simulation_dates:
        current_key = current.isoformat()
        trigger = mapping(date_trigger_map.get(current_key))
        triggered = bool(trigger.get("risk_cap_triggered", False))
        intensity = str(trigger.get("risk_cap_intensity") or "none")
        if triggered:
            max_total_allowed = to_float(cap_by_intensity.get(intensity), default_cap)
            cooldown_cap = max_total_allowed
            cooldown_remaining = max(
                cooldown_remaining,
                int(to_float(cooldown_by_intensity.get(intensity), default_cooldown)),
            )
        elif cooldown_remaining > 0:
            max_total_allowed = cooldown_cap
        else:
            max_total_allowed = default_cap
            cooldown_cap = default_cap
        baseline_total_risk = sum(
            to_float(schedule_by_key.get((current_key, asset), {}).get("risk_asset_exposure"))
            for asset in target_assets
        )
        capped_total_risk = min(baseline_total_risk, max_total_allowed)
        cap_active = capped_total_risk < baseline_total_risk
        dynamic_already_derisked = bool(triggered and baseline_total_risk <= max_total_allowed)
        transition = (
            "entry"
            if cap_active and not previous_cap_active
            else "exit"
            if previous_cap_active and not cap_active
            else "active"
            if cap_active
            else "inactive"
        )
        scale = capped_total_risk / baseline_total_risk if baseline_total_risk > 0.0 else 1.0
        for asset in target_assets:
            schedule = mapping(schedule_by_key.get((current_key, asset)))
            if not schedule:
                continue
            dynamic_exposure = to_float(schedule.get("target_exposure"))
            risk_exposure = to_float(schedule.get("risk_asset_exposure"))
            market_eligible = bool(eligible.get((current_key, asset)))
            risk_scaled_exposure = risk_exposure * scale if risk_exposure > 0.0 else 0.0
            non_risk_exposure = max(0.0, dynamic_exposure - risk_exposure)
            final_exposure = non_risk_exposure + risk_scaled_exposure
            exposure_delta = dynamic_exposure - final_exposure
            asset_return = (
                _asset_return(price_matrix, asset, current, date_index)
                if market_eligible
                else 0.0
            )
            turnover_proxy = abs(final_exposure - previous_final.get(asset, dynamic_exposure))
            incremental_binding = bool(
                cap_active and risk_exposure > 0.0 and not dynamic_already_derisked
            )
            rows.append(
                {
                    "date": current_key,
                    "target_asset": asset,
                    "baseline_id": str(schedule.get("baseline_id", "")),
                    "dynamic_target_exposure": round_float(dynamic_exposure),
                    "dynamic_risk_asset_exposure": round_float(risk_exposure),
                    "risk_cap_triggered": triggered,
                    "risk_cap_intensity": intensity if triggered else "none",
                    "risk_cap_score": round_float(trigger.get("risk_cap_score", 0.0)),
                    "simulated_max_allowed_exposure": round_float(final_exposure),
                    "simulated_max_allowed_total_risk_exposure": round_float(
                        max_total_allowed
                    ),
                    "simulated_final_exposure_after_cap": round_float(final_exposure),
                    "simulated_exposure_delta": round_float(exposure_delta),
                    "simulated_cap_binding_active": bool(cap_active and risk_exposure > 0.0),
                    "simulated_cap_transition": transition,
                    "simulated_cooldown_state": (
                        "active" if cooldown_remaining > 0 else "inactive"
                    ),
                    "simulated_cooldown_days_remaining": cooldown_remaining,
                    "simulated_turnover_proxy": round_float(turnover_proxy),
                    "asset_return": round_float(asset_return),
                    "dynamic_no_cap_return_contribution_proxy": round_float(
                        dynamic_exposure * asset_return
                    ),
                    "dynamic_capped_return_contribution_proxy": round_float(
                        final_exposure * asset_return
                    ),
                    "manual_review_required": triggered or (cap_active and risk_exposure > 0.0),
                    "dynamic_strategy_already_de_risked": dynamic_already_derisked,
                    "risk_cap_incremental_binding": incremental_binding,
                    "data_quality_status": data_quality_status,
                    **SAFETY_FIELDS,
                }
            )
            previous_final[asset] = final_exposure
        previous_cap_active = cap_active
        if cooldown_remaining > 0:
            cooldown_remaining -= 1
    return clean_for_yaml(rows)


def build_dynamic_target_cap_vs_no_cap_comparison(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> dict[str, Any]:
    record_count = len(dry_run_rows)
    dates = sorted({str(row.get("date")) for row in dry_run_rows})
    cap_dates = sorted(
        {
            str(row.get("date"))
            for row in dry_run_rows
            if row.get("simulated_cap_binding_active") is True
        }
    )
    reductions = [to_float(row.get("simulated_exposure_delta")) for row in dry_run_rows]
    no_cap_return = sum(
        to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
        for row in dry_run_rows
    )
    capped_return = sum(
        to_float(row.get("dynamic_capped_return_contribution_proxy"))
        for row in dry_run_rows
    )
    daily_returns = _dynamic_daily_return_proxy(dry_run_rows)
    no_cap_drawdown = _max_drawdown(
        [values["no_cap"] for _, values in sorted(daily_returns.items())]
    )
    capped_drawdown = _max_drawdown(
        [values["capped"] for _, values in sorted(daily_returns.items())]
    )
    missed_upside = _missed_upside_cost(dry_run_rows)
    downside = _downside_protection(dry_run_rows)
    return clean_for_yaml(
        {
            "simulation_mode": MODE,
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "record_count": record_count,
            "simulation_start": dates[0] if dates else "",
            "simulation_end": dates[-1] if dates else "",
            "cap_binding_days": len(cap_dates),
            "cap_binding_rate": round_float(len(cap_dates) / len(dates) if dates else 0.0),
            "average_exposure_reduction": round_float(
                sum(reductions) / record_count if record_count else 0.0
            ),
            "max_exposure_reduction": round_float(max(reductions) if reductions else 0.0),
            "turnover_proxy_total": round_float(
                sum(to_float(row.get("simulated_turnover_proxy")) for row in dry_run_rows)
            ),
            "turnover_proxy_average": round_float(
                sum(to_float(row.get("simulated_turnover_proxy")) for row in dry_run_rows)
                / record_count
                if record_count
                else 0.0
            ),
            "dynamic_no_cap_return_proxy": round_float(no_cap_return),
            "dynamic_capped_return_proxy": round_float(capped_return),
            "return_proxy_delta": round_float(capped_return - no_cap_return),
            "dynamic_no_cap_max_drawdown_proxy": round_float(no_cap_drawdown),
            "dynamic_capped_max_drawdown_proxy": round_float(capped_drawdown),
            "drawdown_proxy_delta": round_float(capped_drawdown - no_cap_drawdown),
            "false_risk_cap_cost_proxy": round_float(missed_upside),
            "missed_upside_cost_proxy": round_float(missed_upside),
            "downside_protection_proxy": round_float(downside),
            "manual_review_trigger_count": sum(
                1 for row in dry_run_rows if row.get("manual_review_required") is True
            ),
            "data_quality_status": data_quality_status,
            "pit_policy": "PIT_APPROXIMATION_READY",
            "known_at_policy": EXPECTED_KNOWN_AT_POLICY,
            "interpretation_boundary": "dynamic_target_pit_caveat_research_proxy_only",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_cap_binding_day_matrix(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in dry_run_rows:
        by_date[str(row.get("date"))].append(row)
    rows: list[dict[str, Any]] = []
    for current, date_rows in sorted(by_date.items()):
        cap_rows = [
            row for row in date_rows if row.get("simulated_cap_binding_active") is True
        ]
        rows.append(
            {
                "date": current,
                "cap_binding_active_any_asset": bool(cap_rows),
                "cap_binding_assets": [str(row.get("target_asset")) for row in cap_rows],
                "cap_binding_asset_count": len(cap_rows),
                "risk_cap_intensity_max": _max_intensity(
                    str(row.get("risk_cap_intensity", "none")) for row in date_rows
                ),
                "risk_cap_intensity_average": _average_intensity(date_rows),
                "dynamic_target_risk_exposure_total": round_float(
                    sum(to_float(row.get("dynamic_risk_asset_exposure")) for row in date_rows)
                ),
                "capped_risk_exposure_total": round_float(
                    sum(
                        min(
                            to_float(row.get("dynamic_risk_asset_exposure")),
                            to_float(row.get("simulated_final_exposure_after_cap")),
                        )
                        for row in date_rows
                    )
                ),
                "exposure_reduction_total": round_float(
                    sum(to_float(row.get("simulated_exposure_delta")) for row in date_rows)
                ),
                "cooldown_active": any(
                    row.get("simulated_cooldown_state") == "active" for row in date_rows
                ),
                "manual_review_required": any(
                    row.get("manual_review_required") is True for row in date_rows
                ),
                "dynamic_strategy_already_de_risked": any(
                    row.get("dynamic_strategy_already_de_risked") is True
                    for row in date_rows
                ),
                "risk_cap_incremental_binding": any(
                    row.get("risk_cap_incremental_binding") is True for row in date_rows
                ),
                **SAFETY_FIELDS,
            }
        )
    return clean_for_yaml(rows)


def build_dynamic_target_exposure_reduction_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reductions = [to_float(row.get("simulated_exposure_delta")) for row in dry_run_rows]
    by_asset: dict[str, float] = defaultdict(float)
    by_period: dict[str, float] = defaultdict(float)
    for row in dry_run_rows:
        value = to_float(row.get("simulated_exposure_delta"))
        by_asset[str(row.get("target_asset"))] += value
        by_period[str(row.get("date", ""))[:7]] += value
    cap_rows = [
        row for row in dry_run_rows if row.get("simulated_cap_binding_active") is True
    ]
    incremental_rows = [
        row for row in cap_rows if row.get("risk_cap_incremental_binding") is True
    ]
    non_incremental_rows = [
        row for row in cap_rows if row.get("risk_cap_incremental_binding") is not True
    ]
    incremental_rate = len(incremental_rows) / len(cap_rows) if cap_rows else 0.0
    return clean_for_yaml(
        {
            "average_exposure_reduction": round_float(
                sum(reductions) / len(reductions) if reductions else 0.0
            ),
            "median_exposure_reduction": round_float(median(reductions) if reductions else 0.0),
            "max_exposure_reduction": round_float(max(reductions) if reductions else 0.0),
            "total_exposure_reduction": round_float(sum(reductions)),
            "exposure_reduction_by_asset": {
                key: round_float(value) for key, value in sorted(by_asset.items())
            },
            "exposure_reduction_by_period": {
                key: round_float(value) for key, value in sorted(by_period.items())
            },
            "incremental_exposure_reduction_after_dynamic_derisk": round_float(
                sum(to_float(row.get("simulated_exposure_delta")) for row in incremental_rows)
            ),
            "non_incremental_binding_count": len(non_incremental_rows),
            "incremental_binding_count": len(incremental_rows),
            "incremental_binding_rate": round_float(incremental_rate),
            "exposure_reduction_label": _exposure_reduction_label(
                total=sum(reductions),
                incremental_count=len(incremental_rows),
                non_incremental_count=len(non_incremental_rows),
                cap_count=len(cap_rows),
            ),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_return_drawdown_proxy_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    comparison = build_dynamic_target_cap_vs_no_cap_comparison(
        dry_run_rows=dry_run_rows,
        data_quality_status=str(
            dry_run_rows[0].get("data_quality_status", "UNKNOWN")
        )
        if dry_run_rows
        else "UNKNOWN",
    )
    return_delta = to_float(comparison.get("return_proxy_delta"))
    drawdown_delta = to_float(comparison.get("drawdown_proxy_delta"))
    return clean_for_yaml(
        {
            "dynamic_no_cap_return_proxy": comparison["dynamic_no_cap_return_proxy"],
            "dynamic_capped_return_proxy": comparison["dynamic_capped_return_proxy"],
            "return_proxy_delta": comparison["return_proxy_delta"],
            "dynamic_no_cap_max_drawdown_proxy": comparison[
                "dynamic_no_cap_max_drawdown_proxy"
            ],
            "dynamic_capped_max_drawdown_proxy": comparison[
                "dynamic_capped_max_drawdown_proxy"
            ],
            "drawdown_proxy_delta": comparison["drawdown_proxy_delta"],
            "return_drawdown_tradeoff_label": _return_drawdown_label(
                return_delta=return_delta,
                drawdown_delta=drawdown_delta,
            ),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_turnover_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_date: dict[str, float] = defaultdict(float)
    for row in dry_run_rows:
        by_date[str(row.get("date"))] += to_float(row.get("simulated_turnover_proxy"))
    total = sum(by_date.values())
    max_turnover = max(by_date.values()) if by_date else 0.0
    return clean_for_yaml(
        {
            "turnover_proxy_total": round_float(total),
            "turnover_proxy_average": round_float(
                total / len(dry_run_rows) if dry_run_rows else 0.0
            ),
            "turnover_proxy_from_cap_entry": round_float(
                sum(
                    to_float(row.get("simulated_turnover_proxy"))
                    for row in dry_run_rows
                    if row.get("simulated_cap_transition") == "entry"
                )
            ),
            "turnover_proxy_from_cap_exit": round_float(
                sum(
                    to_float(row.get("simulated_turnover_proxy"))
                    for row in dry_run_rows
                    if row.get("simulated_cap_transition") == "exit"
                )
            ),
            "turnover_proxy_from_cooldown": round_float(
                sum(
                    to_float(row.get("simulated_turnover_proxy"))
                    for row in dry_run_rows
                    if row.get("simulated_cooldown_state") == "active"
                    and row.get("risk_cap_triggered") is False
                )
            ),
            "turnover_spike_days": [
                day for day, value in sorted(by_date.items()) if value == max_turnover
            ],
            "turnover_impact_label": "LOW_TURNOVER_IMPACT"
            if total == 0.0
            else "TURNOVER_IMPACT_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_cooldown_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    cooldown_rows = [
        row for row in dry_run_rows if row.get("simulated_cooldown_state") == "active"
    ]
    cooldown_dates = {str(row.get("date")) for row in cooldown_rows}
    trigger_dates = {
        str(row.get("date")) for row in dry_run_rows if row.get("risk_cap_triggered") is True
    }
    cooldown_delta = sum(
        to_float(row.get("dynamic_capped_return_contribution_proxy"))
        - to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
        for row in cooldown_rows
    )
    false_cost = sum(
        max(
            0.0,
            to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
            - to_float(row.get("dynamic_capped_return_contribution_proxy")),
        )
        for row in cooldown_rows
        if to_float(row.get("asset_return")) > 0.0
    )
    return clean_for_yaml(
        {
            "cooldown_trigger_count": len(trigger_dates),
            "cooldown_active_days": len(cooldown_dates),
            "average_cooldown_length": round_float(
                len(cooldown_dates) / len(trigger_dates) if trigger_dates else 0.0
            ),
            "cooldown_prevented_reentry_days": len(
                {
                    row.get("date")
                    for row in cooldown_rows
                    if row.get("risk_cap_triggered") is False
                    and row.get("simulated_cap_binding_active") is True
                }
            ),
            "cooldown_return_proxy_delta": round_float(cooldown_delta),
            "cooldown_false_cost_proxy": round_float(false_cost),
            "cooldown_impact_label": "COOLDOWN_HELPFUL_PROXY"
            if cooldown_delta > 0.0
            else "COOLDOWN_COSTLY_PROXY"
            if cooldown_delta < 0.0
            else "COOLDOWN_NEUTRAL_PROXY",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_false_risk_cap_cost_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    false_rows = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    ]
    missed = _missed_upside_cost(dry_run_rows)
    downside = _downside_protection(dry_run_rows)
    return clean_for_yaml(
        {
            "false_risk_cap_count": len(false_rows),
            "false_risk_cap_days": len({row.get("date") for row in false_rows}),
            "false_risk_cap_cost_proxy": round_float(missed),
            "strong_upside_after_cap_count": len(false_rows),
            "mild_drawdown_after_cap_count": len(
                [
                    row
                    for row in dry_run_rows
                    if row.get("simulated_cap_binding_active") is True
                    and to_float(row.get("asset_return")) < 0.0
                ]
            ),
            "false_risk_cap_cost_label": "FALSE_COST_ACCEPTABLE"
            if missed == 0.0
            else "FALSE_COST_BLOCKING"
            if missed > downside
            else "FALSE_COST_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_missed_upside_cost_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    missed = _missed_upside_cost(dry_run_rows)
    incremental = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and row.get("risk_cap_incremental_binding") is True
        and to_float(row.get("asset_return")) > 0.0
    ]
    redundant = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and row.get("risk_cap_incremental_binding") is not True
        and to_float(row.get("asset_return")) > 0.0
    ]
    return clean_for_yaml(
        {
            "missed_upside_cost_proxy": round_float(missed),
            "missed_upside_days": len({row.get("date") for row in incremental + redundant}),
            "missed_upside_after_incremental_cap_count": len(incremental),
            "missed_upside_after_redundant_cap_count": len(redundant),
            "missed_upside_label": "MISSED_UPSIDE_NONE"
            if missed == 0.0
            else "MISSED_UPSIDE_AFTER_INCREMENTAL_CAP"
            if len(incremental) >= len(redundant)
            else "MISSED_UPSIDE_AFTER_REDUNDANT_CAP",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_downside_protection_proxy_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    protection = _downside_protection(dry_run_rows)
    drawdown_rows = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) < 0.0
    ]
    incremental_protection = sum(
        max(
            0.0,
            to_float(row.get("dynamic_capped_return_contribution_proxy"))
            - to_float(row.get("dynamic_no_cap_return_contribution_proxy")),
        )
        for row in drawdown_rows
        if row.get("risk_cap_incremental_binding") is True
    )
    return clean_for_yaml(
        {
            "risk_cap_trigger_count": len(
                {
                    row.get("date")
                    for row in dry_run_rows
                    if row.get("risk_cap_triggered") is True
                }
            ),
            "post_trigger_drawdown_capture_count": len(drawdown_rows),
            "post_trigger_stress_capture_count": len(drawdown_rows),
            "downside_tail_capture_count": len(drawdown_rows),
            "downside_protection_proxy": round_float(protection),
            "drawdown_reduction_proxy": round_float(protection),
            "stress_window_exposure_reduction": round_float(
                sum(to_float(row.get("simulated_exposure_delta")) for row in drawdown_rows)
            ),
            "incremental_downside_protection_proxy": round_float(incremental_protection),
            "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
            if protection > 0.0
            else "DOWNSIDE_PROTECTION_NEGATIVE_PROXY"
            if protection < 0.0
            else "DOWNSIDE_PROTECTION_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_strategy_overlap_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    triggered = [row for row in dry_run_rows if row.get("risk_cap_triggered") is True]
    derisked = [
        row
        for row in dry_run_rows
        if row.get("dynamic_strategy_already_de_risked") is True
    ]
    overlap = [
        row
        for row in dry_run_rows
        if row.get("risk_cap_triggered") is True
        and row.get("dynamic_strategy_already_de_risked") is True
    ]
    incremental = [
        row for row in dry_run_rows if row.get("risk_cap_incremental_binding") is True
    ]
    redundant = [
        row
        for row in dry_run_rows
        if row.get("risk_cap_triggered") is True
        and row.get("dynamic_strategy_already_de_risked") is True
    ]
    binding_without_dynamic = [
        row
        for row in dry_run_rows
        if row.get("risk_cap_triggered") is True
        and row.get("simulated_cap_binding_active") is True
        and row.get("dynamic_strategy_already_de_risked") is not True
    ]
    derisk_without_trigger = [
        row
        for row in derisked
        if row.get("risk_cap_triggered") is not True
    ]
    trigger_count = len(triggered)
    return clean_for_yaml(
        {
            "record_count": len(dry_run_rows),
            "risk_cap_trigger_count": trigger_count,
            "dynamic_strategy_derisked_count": len(derisked),
            "risk_cap_and_dynamic_derisk_overlap_count": len(overlap),
            "risk_cap_incremental_binding_count": len(incremental),
            "risk_cap_redundant_binding_count": len(redundant),
            "risk_cap_binding_without_dynamic_derisk_count": len(binding_without_dynamic),
            "dynamic_derisk_without_risk_cap_count": len(derisk_without_trigger),
            "overlap_rate": round_float(len(overlap) / trigger_count if trigger_count else 0.0),
            "incremental_binding_rate": round_float(
                len(incremental) / trigger_count if trigger_count else 0.0
            ),
            "redundant_binding_rate": round_float(
                len(redundant) / trigger_count if trigger_count else 0.0
            ),
            "overlap_label": _overlap_label(
                trigger_count=trigger_count,
                incremental_count=len(incremental),
                redundant_count=len(redundant),
                binding_without_dynamic_count=len(binding_without_dynamic),
            ),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_static_vs_dynamic_comparison(
    *,
    static_dry_run: Mapping[str, Any],
    dynamic_comparison: Mapping[str, Any],
    dynamic_false_cost: Mapping[str, Any],
    dynamic_downside: Mapping[str, Any],
    overlap_report: Mapping[str, Any],
) -> dict[str, Any]:
    static_comparison = mapping(static_dry_run.get("comparison"))
    static_false_cost = mapping(static_dry_run.get("false_cost_report"))
    static_downside = mapping(static_dry_run.get("downside_report"))
    static_binding = to_float(static_comparison.get("cap_binding_rate"))
    dynamic_binding = to_float(dynamic_comparison.get("cap_binding_rate"))
    static_return_delta = to_float(static_comparison.get("return_proxy_delta"))
    dynamic_return_delta = to_float(dynamic_comparison.get("return_proxy_delta"))
    static_drawdown_delta = to_float(static_comparison.get("drawdown_proxy_delta"))
    dynamic_drawdown_delta = to_float(dynamic_comparison.get("drawdown_proxy_delta"))
    static_false = to_float(static_false_cost.get("false_risk_cap_cost_proxy"))
    if static_false == 0.0:
        static_false = to_float(static_comparison.get("false_risk_cap_cost_proxy"))
    dynamic_false = to_float(dynamic_false_cost.get("false_risk_cap_cost_proxy"))
    static_protection = to_float(static_downside.get("downside_protection_proxy"))
    if static_protection == 0.0:
        static_protection = to_float(static_comparison.get("downside_protection_proxy"))
    dynamic_protection = to_float(dynamic_downside.get("downside_protection_proxy"))
    return clean_for_yaml(
        {
            "static_cap_binding_rate": round_float(static_binding),
            "dynamic_cap_binding_rate": round_float(dynamic_binding),
            "cap_binding_rate_delta": round_float(dynamic_binding - static_binding),
            "static_return_proxy_delta": round_float(static_return_delta),
            "dynamic_return_proxy_delta": round_float(dynamic_return_delta),
            "return_cost_delta": round_float(dynamic_return_delta - static_return_delta),
            "static_drawdown_proxy_delta": round_float(static_drawdown_delta),
            "dynamic_drawdown_proxy_delta": round_float(dynamic_drawdown_delta),
            "drawdown_protection_delta": round_float(
                dynamic_drawdown_delta - static_drawdown_delta
            ),
            "static_false_cost_proxy": round_float(static_false),
            "dynamic_false_cost_proxy": round_float(dynamic_false),
            "false_cost_delta": round_float(dynamic_false - static_false),
            "static_downside_protection_proxy": round_float(static_protection),
            "dynamic_downside_protection_proxy": round_float(dynamic_protection),
            "comparison_label": _static_dynamic_label(
                static_binding=static_binding,
                dynamic_binding=dynamic_binding,
                false_delta=dynamic_false - static_false,
                protection_delta=dynamic_protection - static_protection,
                overlap_label=str(overlap_report.get("overlap_label", "")),
            ),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_data_quality_report(
    *,
    quality_report: DataQualityReport,
    alignment_rows: Sequence[Mapping[str, Any]],
    schedule_rows: Sequence[Mapping[str, Any]],
    wrapper_rows: Sequence[Mapping[str, Any]],
    trigger_frame: pd.DataFrame,
    policy: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
) -> dict[str, Any]:
    missing_market = sum(1 for row in alignment_rows if not row.get("market_data_available"))
    missing_dynamic = sum(
        1 for row in alignment_rows if not row.get("target_exposure_available")
    )
    timestamp_warnings = sum(
        1
        for row in alignment_rows
        if row.get("timestamp_alignment_status") != "NEXT_SESSION_ALIGNED_WITH_PIT_CAVEAT"
    )
    pit_warnings = sum(
        1
        for row in wrapper_rows
        if str(row.get("pit_policy")) != "STRICT_PIT_READY"
    )
    missing_trigger = 0 if not trigger_frame.empty else len(alignment_rows)
    errors = (
        quality_report.error_count
        + missing_market
        + missing_dynamic
        + missing_trigger
    )
    warnings = quality_report.warning_count + timestamp_warnings + pit_warnings
    status = "FAIL" if errors > 0 else "PASS_WITH_WARNINGS" if warnings > 0 else "PASS"
    return clean_for_yaml(
        {
            "wrapper_data_quality_status": str(
                wrapper_validation.get("wrapper_validation_status", "UNKNOWN")
            ),
            "market_data_status": quality_report.status,
            "risk_cap_trigger_data_status": "PASS" if not trigger_frame.empty else "FAIL",
            "policy_data_status": "PASS"
            if policy.get("cap_policy") and policy.get("cooldown_policy")
            else "FAIL",
            "record_count": len(alignment_rows),
            "eligible_record_count": sum(
                1 for row in alignment_rows if row.get("simulation_eligible") is True
            ),
            "schedule_record_count": len(schedule_rows),
            "wrapper_record_count": len(wrapper_rows),
            "missing_market_data_count": missing_market,
            "missing_dynamic_target_count": missing_dynamic,
            "missing_trigger_count": missing_trigger,
            "timestamp_warning_count": timestamp_warnings,
            "pit_caveat_warning_count": pit_warnings,
            "coverage_ratio": round_float(
                sum(1 for row in alignment_rows if row.get("simulation_eligible") is True)
                / len(alignment_rows)
                if alignment_rows
                else 0.0
            ),
            "warning_count": warnings,
            "error_count": errors,
            "data_quality_status": status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_pit_caveat_interpretation_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.pit_caveat_interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "simulation_executed": data_quality_status != "FAIL",
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "known_at_policy": EXPECTED_KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "real_portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_2333_task_route(
    *,
    comparison: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    false_cost_report: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(data_quality_report.get("data_quality_status", "FAIL"))
    record_count = int(to_float(comparison.get("record_count"), 0.0))
    if status == "FAIL":
        next_task = NEXT_DATA_TASK
        route_reason = "data_quality_failed"
    elif record_count > 0:
        next_task = NEXT_DIAGNOSTICS_TASK
        route_reason = "dynamic_dry_run_ready_for_diagnostics_review"
    elif str(false_cost_report.get("false_risk_cap_cost_label")) == "FALSE_COST_BLOCKING":
        next_task = NEXT_POLICY_TASK
        route_reason = "false_cost_blocking"
    else:
        next_task = NEXT_ARCHIVE_TASK
        route_reason = "dynamic_dry_run_not_meaningful"
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "next_task": next_task,
            "route_reason": route_reason,
            "data_quality_status": status,
            "record_count": record_count,
            "allowed_routes": [
                NEXT_DIAGNOSTICS_TASK,
                NEXT_POLICY_TASK,
                NEXT_DATA_TASK,
                NEXT_ARCHIVE_TASK,
            ],
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_baseline_source_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    schedule_rows: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    first = mapping(wrapper_rows[0]) if wrapper_rows else {}
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "wrapper_record_count": len(wrapper_rows),
            "schedule_record_count": len(schedule_rows),
            "baseline_id": str(first.get("baseline_id", "")),
            "source_id": str(first.get("source_id", "")),
            "source_family": str(first.get("source_family", "")),
            "source_path": str(first.get("source_path", "")),
            "source_hash": str(first.get("source_hash", "")),
            "source_artifact_hash": str(first.get("source_artifact_hash", "")),
            "target_assets": dynamic_target_assets_from_wrapper(wrapper_rows),
            "pit_policy": str(first.get("pit_policy", "")),
            "known_at_policy": str(first.get("known_at_policy", "")),
            "latency_policy": str(first.get("latency_policy", "")),
            "readiness_status": mapping(
                mapping(inputs.get("dry_run_readiness")).get("summary")
            ).get("readiness_status"),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_exposure_cap_dry_run_summary(
    *,
    generated_at: datetime,
    dry_run_readiness_dir: Path,
    timestamp_remediation_dir: Path,
    source_remediation_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    static_dry_run_dir: Path,
    market_data_source: Path,
    rates_source: Path,
    policy_path: Path,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    target_assets: Sequence[str],
    schedule_rows: Sequence[Mapping[str, Any]],
    alignment_rows: Sequence[Mapping[str, Any]],
    dry_run_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    task_route: Mapping[str, Any],
    full_dry_run_allowed: bool,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "status": STATUS if full_dry_run_allowed else BLOCKED_STATUS,
            "generated_at": generated_at.isoformat(),
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "mode": MODE,
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "dry_run_readiness_dir": str(dry_run_readiness_dir),
            "timestamp_remediation_dir": str(timestamp_remediation_dir),
            "source_remediation_dir": str(source_remediation_dir),
            "source_binding_dir": str(source_binding_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "static_dry_run_dir": str(static_dry_run_dir),
            "market_data_source": str(market_data_source),
            "rates_source": str(rates_source),
            "policy_path": str(policy_path),
            "target_assets": list(target_assets),
            "schedule_record_count": len(schedule_rows),
            "alignment_record_count": len(alignment_rows),
            "record_count": len(dry_run_rows),
            "simulation_executed": full_dry_run_allowed,
            "full_dry_run_allowed": full_dry_run_allowed,
            "data_quality_gate_required": True,
            "data_quality_gate_executed": True,
            "aits_validate_data_equivalent": True,
            "quality_as_of": quality_report.as_of.isoformat(),
            "data_quality_report_path": str(quality_report_path),
            "data_quality_status": data_quality_report["data_quality_status"],
            "market_data_status": quality_report.status,
            "warning_count": data_quality_report["warning_count"],
            "error_count": data_quality_report["error_count"],
            "simulation_start": comparison.get("simulation_start", ""),
            "simulation_end": comparison.get("simulation_end", ""),
            "cap_binding_days": comparison.get("cap_binding_days", 0),
            "cap_binding_rate": comparison.get("cap_binding_rate", 0.0),
            "return_proxy_delta": comparison.get("return_proxy_delta", 0.0),
            "drawdown_proxy_delta": comparison.get("drawdown_proxy_delta", 0.0),
            "pit_policy": "PIT_APPROXIMATION_READY",
            "known_at_policy": EXPECTED_KNOWN_AT_POLICY,
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def write_dynamic_target_exposure_cap_dry_run_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    source_report: Mapping[str, Any],
    schedule_rows: Sequence[Mapping[str, Any]],
    alignment_rows: Sequence[Mapping[str, Any]],
    dry_run_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    binding_rows: Sequence[Mapping[str, Any]],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
    false_cost_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    downside_report: Mapping[str, Any],
    overlap_report: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, str]:
    output_payloads: list[Any] = [
        summary,
        source_report,
        *schedule_rows,
        *alignment_rows,
        *dry_run_rows,
        comparison,
        *binding_rows,
        exposure_reduction,
        return_drawdown,
        turnover_report,
        cooldown_report,
        false_cost_report,
        missed_upside_report,
        downside_report,
        overlap_report,
        static_dynamic,
        data_quality_report,
        interpretation_boundary,
        task_route,
    ]
    for index, payload in enumerate(output_payloads):
        _validate_safe(f"TRADING-2332 output {index}", payload)

    paths = {
        "summary": output_dir / "dynamic_target_exposure_cap_dry_run_summary.json",
        "source_report": output_dir / "dynamic_target_baseline_source_report.json",
        "schedule_json": output_dir / "dynamic_target_baseline_exposure_schedule.json",
        "schedule_csv": output_dir / "dynamic_target_baseline_exposure_schedule.csv",
        "alignment_json": output_dir
        / "dynamic_target_risk_cap_trigger_alignment_matrix.json",
        "alignment_csv": output_dir
        / "dynamic_target_risk_cap_trigger_alignment_matrix.csv",
        "dry_run_json": output_dir / "dynamic_target_exposure_cap_dry_run_result.json",
        "dry_run_csv": output_dir / "dynamic_target_exposure_cap_dry_run_result.csv",
        "comparison_json": output_dir / "dynamic_target_cap_vs_no_cap_comparison.json",
        "comparison_csv": output_dir / "dynamic_target_cap_vs_no_cap_comparison.csv",
        "binding_json": output_dir / "dynamic_target_cap_binding_day_matrix.json",
        "binding_csv": output_dir / "dynamic_target_cap_binding_day_matrix.csv",
        "exposure_reduction": output_dir / "dynamic_target_exposure_reduction_report.json",
        "return_drawdown": output_dir / "dynamic_target_return_drawdown_proxy_report.json",
        "turnover": output_dir / "dynamic_target_turnover_impact_report.json",
        "cooldown": output_dir / "dynamic_target_cooldown_impact_report.json",
        "false_cost": output_dir / "dynamic_target_false_risk_cap_cost_report.json",
        "missed_upside": output_dir / "dynamic_target_missed_upside_cost_report.json",
        "downside": output_dir / "dynamic_target_downside_protection_proxy_report.json",
        "overlap": output_dir / "dynamic_target_strategy_overlap_report.json",
        "static_dynamic": output_dir / "dynamic_target_static_vs_dynamic_comparison.json",
        "data_quality": output_dir / "dynamic_target_data_quality_report.json",
        "boundary": output_dir / "dynamic_target_pit_caveat_interpretation_boundary.json",
        "task_route": output_dir / "dynamic_target_2333_task_route.json",
        "report_doc": docs_root / "dynamic_target_exposure_cap_dry_run_report.md",
        "comparison_doc": docs_root / "dynamic_target_cap_vs_no_cap_comparison.md",
        "overlap_doc": docs_root / "dynamic_target_strategy_overlap_report.md",
        "false_downside_doc": docs_root
        / "dynamic_target_false_cost_downside_protection_review.md",
        "static_dynamic_doc": docs_root / "dynamic_target_static_vs_dynamic_comparison.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["source_report"], dict(source_report))
    write_json(paths["schedule_json"], {**dict(summary), "rows": list(schedule_rows)})
    write_csv_rows(paths["schedule_csv"], schedule_rows)
    write_json(paths["alignment_json"], {**dict(summary), "rows": list(alignment_rows)})
    write_csv_rows(paths["alignment_csv"], alignment_rows)
    write_json(paths["dry_run_json"], {**dict(summary), "rows": list(dry_run_rows)})
    write_csv_rows(paths["dry_run_csv"], dry_run_rows)
    write_json(paths["comparison_json"], dict(comparison))
    write_csv_rows(paths["comparison_csv"], [comparison])
    write_json(paths["binding_json"], {**dict(summary), "rows": list(binding_rows)})
    write_csv_rows(paths["binding_csv"], binding_rows)
    write_json(paths["exposure_reduction"], dict(exposure_reduction))
    write_json(paths["return_drawdown"], dict(return_drawdown))
    write_json(paths["turnover"], dict(turnover_report))
    write_json(paths["cooldown"], dict(cooldown_report))
    write_json(paths["false_cost"], dict(false_cost_report))
    write_json(paths["missed_upside"], dict(missed_upside_report))
    write_json(paths["downside"], dict(downside_report))
    write_json(paths["overlap"], dict(overlap_report))
    write_json(paths["static_dynamic"], dict(static_dynamic))
    write_json(paths["data_quality"], dict(data_quality_report))
    write_json(paths["boundary"], dict(interpretation_boundary))
    write_json(paths["task_route"], dict(task_route))
    write_markdown(
        paths["report_doc"],
        _render_main_doc(
            summary,
            comparison,
            exposure_reduction,
            return_drawdown,
            overlap_report,
            task_route,
        ),
    )
    write_markdown(paths["comparison_doc"], _render_comparison_doc(comparison))
    write_markdown(paths["overlap_doc"], _render_overlap_doc(overlap_report))
    write_markdown(
        paths["false_downside_doc"],
        _render_false_downside_doc(false_cost_report, missed_upside_report, downside_report),
    )
    write_markdown(paths["static_dynamic_doc"], _render_static_dynamic_doc(static_dynamic))
    return {key: str(path) for key, path in paths.items()}


def _render_main_doc(
    summary: Mapping[str, Any],
    comparison: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    overlap_report: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Target Exposure-Cap Dry-Run Report",
            "",
            "TRADING-2332 使用 TRADING-2331 允许的 timestamp-remediated dynamic target "
            "baseline wrapper 执行 source-bound exposure-cap dry-run。wrapper 仍为 "
            "`PIT_APPROXIMATION_READY`，并使用 `NEXT_SESSION_DECISION_POLICY`；本报告只"
            "提供 research-only proxy diagnostics。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- target_assets: `{', '.join(summary['target_assets'])}`",
            f"- data_quality_gate_executed: `{summary['data_quality_gate_executed']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- warning_count: `{summary['warning_count']}`",
            f"- error_count: `{summary['error_count']}`",
            f"- simulation_executed: `{summary['simulation_executed']}`",
            f"- cap_binding_days: `{comparison['cap_binding_days']}`",
            f"- cap_binding_rate: `{comparison['cap_binding_rate']}`",
            f"- average_exposure_reduction: `{comparison['average_exposure_reduction']}`",
            f"- exposure_reduction_label: `{exposure_reduction['exposure_reduction_label']}`",
            f"- return_proxy_delta: `{comparison['return_proxy_delta']}`",
            f"- drawdown_proxy_delta: `{comparison['drawdown_proxy_delta']}`",
            f"- return_drawdown_tradeoff_label: "
            f"`{return_drawdown['return_drawdown_tradeoff_label']}`",
            f"- overlap_label: `{overlap_report['overlap_label']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## 解释边界",
            "",
            "本报告不得解释为真实账户表现、真实仓位建议、paper-shadow signal、"
            "production strategy 或 broker action；dynamic no-cap / capped 差异只"
            "是 PIT caveat wrapper 下的 proxy diagnostics。",
            "",
        ]
    )


def _render_comparison_doc(comparison: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Target Cap vs No-Cap Comparison",
            "",
            "本比较只衡量 dynamic target baseline wrapper with PIT caveat 下的 cap 与 "
            "no-cap proxy 差异，不代表真实收益、真实回撤保护或生产结论。",
            "",
            f"- simulation_mode: `{comparison['simulation_mode']}`",
            f"- record_count: `{comparison['record_count']}`",
            f"- cap_binding_days: `{comparison['cap_binding_days']}`",
            f"- cap_binding_rate: `{comparison['cap_binding_rate']}`",
            f"- dynamic_no_cap_return_proxy: `{comparison['dynamic_no_cap_return_proxy']}`",
            f"- dynamic_capped_return_proxy: `{comparison['dynamic_capped_return_proxy']}`",
            f"- return_proxy_delta: `{comparison['return_proxy_delta']}`",
            f"- dynamic_no_cap_max_drawdown_proxy: "
            f"`{comparison['dynamic_no_cap_max_drawdown_proxy']}`",
            f"- dynamic_capped_max_drawdown_proxy: "
            f"`{comparison['dynamic_capped_max_drawdown_proxy']}`",
            f"- drawdown_proxy_delta: `{comparison['drawdown_proxy_delta']}`",
            f"- false_risk_cap_cost_proxy: `{comparison['false_risk_cap_cost_proxy']}`",
            f"- downside_protection_proxy: `{comparison['downside_protection_proxy']}`",
            "",
        ]
    )


def _render_overlap_doc(overlap_report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Target Strategy Overlap Report",
            "",
            "本报告检查 risk-cap trigger 与 dynamic strategy 自身降仓行为的重叠程度，"
            "用于判断 cap 是边际约束还是重复约束。",
            "",
            f"- record_count: `{overlap_report['record_count']}`",
            f"- risk_cap_trigger_count: `{overlap_report['risk_cap_trigger_count']}`",
            f"- dynamic_strategy_derisked_count: "
            f"`{overlap_report['dynamic_strategy_derisked_count']}`",
            f"- risk_cap_incremental_binding_count: "
            f"`{overlap_report['risk_cap_incremental_binding_count']}`",
            f"- risk_cap_redundant_binding_count: "
            f"`{overlap_report['risk_cap_redundant_binding_count']}`",
            f"- overlap_rate: `{overlap_report['overlap_rate']}`",
            f"- incremental_binding_rate: `{overlap_report['incremental_binding_rate']}`",
            f"- redundant_binding_rate: `{overlap_report['redundant_binding_rate']}`",
            f"- overlap_label: `{overlap_report['overlap_label']}`",
            "",
        ]
    )


def _render_false_downside_doc(
    false_cost_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    downside_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Target False Cost and Downside Protection Review",
            "",
            "本报告只披露 false risk-cap cost、missed upside 和 downside protection "
            "proxy，不代表真实机会成本或实盘保护效果。",
            "",
            f"- false_risk_cap_count: `{false_cost_report['false_risk_cap_count']}`",
            f"- false_risk_cap_cost_proxy: "
            f"`{false_cost_report['false_risk_cap_cost_proxy']}`",
            f"- false_risk_cap_cost_label: "
            f"`{false_cost_report['false_risk_cap_cost_label']}`",
            f"- missed_upside_cost_proxy: "
            f"`{missed_upside_report['missed_upside_cost_proxy']}`",
            f"- missed_upside_label: `{missed_upside_report['missed_upside_label']}`",
            f"- downside_protection_proxy: "
            f"`{downside_report['downside_protection_proxy']}`",
            f"- incremental_downside_protection_proxy: "
            f"`{downside_report['incremental_downside_protection_proxy']}`",
            f"- downside_protection_label: "
            f"`{downside_report['downside_protection_label']}`",
            "",
        ]
    )


def _render_static_dynamic_doc(static_dynamic: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Target Static vs Dynamic Comparison",
            "",
            "本文件把 TRADING-2326 static ETF baseline dry-run reference 与 TRADING-2332 "
            "dynamic target baseline dry-run proxy 做对照，不重新解释 static baseline 为"
            "真实账户表现。",
            "",
            f"- static_cap_binding_rate: `{static_dynamic['static_cap_binding_rate']}`",
            f"- dynamic_cap_binding_rate: `{static_dynamic['dynamic_cap_binding_rate']}`",
            f"- cap_binding_rate_delta: `{static_dynamic['cap_binding_rate_delta']}`",
            f"- static_return_proxy_delta: `{static_dynamic['static_return_proxy_delta']}`",
            f"- dynamic_return_proxy_delta: `{static_dynamic['dynamic_return_proxy_delta']}`",
            f"- return_cost_delta: `{static_dynamic['return_cost_delta']}`",
            f"- static_drawdown_proxy_delta: "
            f"`{static_dynamic['static_drawdown_proxy_delta']}`",
            f"- dynamic_drawdown_proxy_delta: "
            f"`{static_dynamic['dynamic_drawdown_proxy_delta']}`",
            f"- false_cost_delta: `{static_dynamic['false_cost_delta']}`",
            f"- comparison_label: `{static_dynamic['comparison_label']}`",
            "",
        ]
    )


def _policy_path(policy_path: Path | None, source_binding: Mapping[str, Any]) -> Path:
    if policy_path is not None:
        return policy_path
    value = mapping(source_binding.get("summary")).get("policy_path")
    return Path(str(value)) if value else DEFAULT_POLICY_PATH


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicTargetExposureCapDryRunError(
            f"{label} required artifacts missing: " + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicTargetExposureCapDryRunError(f"JSON must be object: {path}")
    return payload


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _load_json(path)


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_safe(name: str, payload: Mapping[str, Any]) -> None:
    try:
        validate_no_unsafe_fields(name, payload)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetExposureCapDryRunError(str(exc)) from exc


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in records(mapping(payload).get("rows"))]


def _timestamp_alignment_status(schedule: Mapping[str, Any], current: date) -> str:
    if not schedule:
        return "MISSING_DYNAMIC_TARGET"
    if str(schedule.get("known_at_policy")) != EXPECTED_KNOWN_AT_POLICY:
        return "UNSUPPORTED_KNOWN_AT_POLICY"
    decision_date = _timestamp_date(schedule.get("decision_timestamp"))
    valid_from = _timestamp_date(schedule.get("valid_from"))
    valid_until = _timestamp_date(schedule.get("valid_until"))
    if decision_date is None or valid_from is None:
        return "MISSING_TIMESTAMP"
    if decision_date > current:
        return "DECISION_AFTER_SIMULATION_DATE"
    if valid_from > current:
        return "VALIDITY_NOT_STARTED"
    if valid_until is not None and valid_until < current:
        return "VALIDITY_EXPIRED"
    return "NEXT_SESSION_ALIGNED_WITH_PIT_CAVEAT"


def _timestamp_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    return _parse_date(text[:10])


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _market_data_available(price_matrix: pd.DataFrame, asset: str, current: date) -> bool:
    if asset not in price_matrix.columns:
        return False
    rows_for_date = price_matrix.loc[price_matrix.index.date == current]
    if rows_for_date.empty:
        return False
    return pd.notna(rows_for_date[asset].iloc[0])


def _asset_return(
    price_matrix: pd.DataFrame,
    asset: str,
    current: date,
    date_index: Mapping[date, int],
) -> float:
    position = date_index.get(current)
    if asset not in price_matrix.columns or position is None or position <= 0:
        return 0.0
    current_price = to_float(price_matrix[asset].iloc[position])
    previous_price = to_float(price_matrix[asset].iloc[position - 1])
    if previous_price <= 0.0:
        return 0.0
    return current_price / previous_price - 1.0


def _dynamic_daily_return_proxy(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    daily: dict[str, dict[str, float]] = defaultdict(lambda: {"no_cap": 0.0, "capped": 0.0})
    for row in dry_run_rows:
        day = str(row.get("date"))
        daily[day]["no_cap"] += to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
        daily[day]["capped"] += to_float(row.get("dynamic_capped_return_contribution_proxy"))
    return daily


def _max_drawdown(returns: Sequence[float]) -> float:
    value = 1.0
    peak = 1.0
    drawdown = 0.0
    for item in returns:
        value *= 1.0 + to_float(item)
        peak = max(peak, value)
        if peak > 0.0:
            drawdown = min(drawdown, value / peak - 1.0)
    return drawdown


def _missed_upside_cost(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
            - to_float(row.get("dynamic_capped_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    )


def _downside_protection(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("dynamic_capped_return_contribution_proxy"))
            - to_float(row.get("dynamic_no_cap_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) < 0.0
    )


def _max_intensity(values: Sequence[str]) -> str:
    rank = {"none": 0, "low": 1, "medium": 2, "high": 3}
    normalized = [value.lower() for value in values if value]
    if not normalized:
        return "none"
    return max(normalized, key=lambda item: rank.get(item, 0))


def _average_intensity(rows: Sequence[Mapping[str, Any]]) -> float:
    rank = {"none": 0.0, "low": 1.0, "medium": 2.0, "high": 3.0}
    values = [
        rank.get(str(row.get("risk_cap_intensity", "none")).lower(), 0.0)
        for row in rows
    ]
    return round_float(sum(values) / len(values) if values else 0.0)


def _exposure_reduction_label(
    *,
    total: float,
    incremental_count: int,
    non_incremental_count: int,
    cap_count: int,
) -> str:
    if cap_count == 0:
        return "EXPOSURE_REDUCTION_INCONCLUSIVE"
    if total == 0.0 or incremental_count == 0:
        return "MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"
    if incremental_count > non_incremental_count:
        return "INCREMENTAL_EXPOSURE_REDUCTION_MATERIAL"
    return "INCREMENTAL_EXPOSURE_REDUCTION_MODEST"


def _return_drawdown_label(*, return_delta: float, drawdown_delta: float) -> str:
    if drawdown_delta > 0.0 and return_delta >= 0.0:
        return "DRAWDOWN_IMPROVED_RETURN_ACCEPTABLE"
    if drawdown_delta > 0.0 and return_delta < 0.0:
        return "DRAWDOWN_IMPROVED_RETURN_COSTLY"
    if drawdown_delta <= 0.0 and return_delta < 0.0:
        return "DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY"
    if return_delta > 0.0:
        return "RETURN_IMPROVED_WITH_DRAWDOWN_ACCEPTABLE"
    if return_delta == 0.0 and drawdown_delta == 0.0:
        return "NO_MATERIAL_DIFFERENCE"
    return "INCONCLUSIVE"


def _overlap_label(
    *,
    trigger_count: int,
    incremental_count: int,
    redundant_count: int,
    binding_without_dynamic_count: int,
) -> str:
    if trigger_count == 0:
        return "OVERLAP_INCONCLUSIVE"
    if binding_without_dynamic_count > redundant_count:
        return "RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK"
    if incremental_count > redundant_count:
        return "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"
    if redundant_count > 0 and incremental_count == 0:
        return "DYNAMIC_STRATEGY_ALREADY_HANDLES_RISK"
    if redundant_count >= incremental_count and redundant_count > 0:
        return "RISK_CAP_MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"
    return "OVERLAP_INCONCLUSIVE"


def _static_dynamic_label(
    *,
    static_binding: float,
    dynamic_binding: float,
    false_delta: float,
    protection_delta: float,
    overlap_label: str,
) -> str:
    if dynamic_binding < static_binding and false_delta <= 0.0:
        return "DYNAMIC_BASELINE_REDUCES_OVERBINDING"
    if protection_delta > 0.0 and false_delta <= 0.0:
        return "DYNAMIC_BASELINE_CONFIRMS_RISK_CAP_VALUE"
    if "REDUNDANT" in overlap_label or "ALREADY_HANDLES" in overlap_label:
        return "DYNAMIC_BASELINE_SHOWS_RISK_CAP_REDUNDANT"
    if false_delta > 0.0:
        return "DYNAMIC_BASELINE_SHOWS_FALSE_COST_WORSE"
    return "DYNAMIC_BASELINE_INCONCLUSIVE"
