from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
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
    max_price_date,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2326_SOURCE_BOUND_EXPOSURE_CAP_DRY_RUN_STATIC_ETF_BASELINE"
REPORT_TYPE = "source_bound_exposure_cap_dry_run_static_etf_baseline"
ARTIFACT_ROLE = "source_bound_static_etf_exposure_cap_dry_run"
MODE = "static_etf_baseline_dry_run"
STATUS = "SOURCE_BOUND_STATIC_ETF_EXPOSURE_CAP_DRY_RUN_READY_PROMOTION_BLOCKED"
SELECTED_BASELINE = "static_etf_allocation_baseline"
RISK_CAP_CANDIDATE_ID = "volatility_regime_scope_narrowed_risk_cap_v1"

DEFAULT_SOURCE_BINDING_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_simulation_source_binding"
)
DEFAULT_BASELINE_DECISION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "portfolio_baseline_source_decision"
)
DEFAULT_SIMULATION_POLICY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_mechanics_simulation"
)
DEFAULT_PORTFOLIO_CONFIG_ROOT = PROJECT_ROOT / "config" / "etf_portfolio"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "exposure_cap_simulation_source_binding_policy.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")

WEIGHT_SUM_TOLERANCE = 0.001  # Floating-point YAML sum tolerance, not a policy gate.

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "dry_run_only": True,
    "manual_review_only": True,
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


class SourceBoundStaticEtfDryRunError(ValueError):
    pass


def run_source_bound_static_etf_dry_run(
    *,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    baseline_decision_dir: Path = DEFAULT_BASELINE_DECISION_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    portfolio_config_dir: Path = DEFAULT_PORTFOLIO_CONFIG_ROOT,
    market_data_source: Path = DEFAULT_PRICES_PATH,
    rates_source: Path = DEFAULT_RATES_PATH,
    marketstack_prices_source: Path | None = None,
    policy_path: Path | None = None,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise SourceBoundStaticEtfDryRunError(
            f"source-bound exposure-cap dry-run only supports {MODE} mode"
        )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    assets = _normalize_list(target_assets) or list(DEFAULT_TARGET_ASSETS)
    baseline_decision = load_baseline_decision_outputs(baseline_decision_dir)
    source_binding = load_source_binding_outputs(source_binding_dir)
    load_simulation_policy_outputs(simulation_policy_dir)
    selected_policy_path = _policy_path(policy_path, source_binding)
    policy = load_exposure_cap_policy(selected_policy_path)
    _validate_policy(policy, assets)

    static_config = load_static_etf_config(portfolio_config_dir, assets)
    trigger_frame = load_risk_cap_trigger_frame_from_source_binding(
        source_binding,
        assets,
    )
    price_matrix = load_adjusted_price_matrix(market_data_source, assets)
    simulation_dates = build_simulation_calendar_from_sources(
        trigger_frame=trigger_frame,
        price_matrix=price_matrix,
        target_assets=assets,
    )
    if not simulation_dates:
        raise SourceBoundStaticEtfDryRunError(
            "no overlapping simulation calendar for static ETF dry-run"
        )
    quality_report, quality_report_path = run_data_quality_gate(
        market_data_source=market_data_source,
        rates_source=rates_source,
        marketstack_prices_source=marketstack_prices_source,
        target_assets=assets,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise SourceBoundStaticEtfDryRunError(
            f"cached data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )

    schedule_rows = build_static_etf_baseline_exposure_schedule(
        static_config=static_config,
        simulation_dates=simulation_dates,
    )
    date_trigger_map = build_portfolio_level_trigger_map(trigger_frame)
    alignment_rows = build_risk_cap_trigger_alignment_matrix(
        simulation_dates=simulation_dates,
        target_assets=assets,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        date_trigger_map=date_trigger_map,
        trigger_source_hash=str(trigger_frame.attrs.get("source_hash", "")),
    )
    dry_run_rows = build_source_bound_static_etf_dry_run_rows(
        policy=policy,
        simulation_dates=simulation_dates,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        alignment_rows=alignment_rows,
        date_trigger_map=date_trigger_map,
        target_assets=assets,
        data_quality_status=quality_report.status,
    )
    comparison = build_exposure_cap_vs_no_cap_static_etf_comparison(
        dry_run_rows=dry_run_rows,
        data_quality_status=quality_report.status,
    )
    binding_rows = build_exposure_cap_binding_day_matrix(dry_run_rows)
    turnover_report = build_turnover_impact_report(dry_run_rows)
    cooldown_report = build_cooldown_impact_report(dry_run_rows)
    false_cost_report = build_false_risk_cap_cost_report(dry_run_rows)
    missed_upside_report = build_missed_upside_cost_report(dry_run_rows)
    downside_report = build_downside_protection_proxy_report(dry_run_rows)
    data_quality_report = build_exposure_cap_data_quality_report(
        quality_report=quality_report,
        alignment_rows=alignment_rows,
        schedule_rows=schedule_rows,
        trigger_frame=trigger_frame,
        policy=policy,
    )
    interpretation_boundary = build_interpretation_boundary(
        generated_at=generated_at,
        data_quality_status=quality_report.status,
    )
    task_route = build_exposure_cap_2327_task_route(
        comparison=comparison,
        data_quality_report=data_quality_report,
    )
    baseline_report = build_static_etf_baseline_source_report(
        static_config=static_config,
        schedule_rows=schedule_rows,
        baseline_decision=baseline_decision,
    )
    summary = build_source_bound_static_etf_dry_run_summary(
        generated_at=generated_at,
        source_binding_dir=source_binding_dir,
        baseline_decision_dir=baseline_decision_dir,
        simulation_policy_dir=simulation_policy_dir,
        portfolio_config_dir=portfolio_config_dir,
        market_data_source=market_data_source,
        rates_source=rates_source,
        policy_path=selected_policy_path,
        target_assets=assets,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        schedule_rows=schedule_rows,
        alignment_rows=alignment_rows,
        dry_run_rows=dry_run_rows,
        comparison=comparison,
        task_route=task_route,
    )
    paths = write_source_bound_static_etf_dry_run_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        baseline_report=baseline_report,
        schedule_rows=schedule_rows,
        alignment_rows=alignment_rows,
        dry_run_rows=dry_run_rows,
        comparison=comparison,
        binding_rows=binding_rows,
        turnover_report=turnover_report,
        cooldown_report=cooldown_report,
        false_cost_report=false_cost_report,
        missed_upside_report=missed_upside_report,
        downside_report=downside_report,
        data_quality_report=data_quality_report,
        interpretation_boundary=interpretation_boundary,
        task_route=task_route,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_baseline_decision_outputs(baseline_decision_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": baseline_decision_dir
        / "portfolio_baseline_source_decision_summary.json",
        "candidate_matrix": baseline_decision_dir / "portfolio_baseline_candidate_matrix.json",
        "feasibility_matrix": baseline_decision_dir
        / "portfolio_baseline_source_feasibility_matrix.json",
        "pit_audit": baseline_decision_dir
        / "portfolio_baseline_pit_reproducibility_audit.json",
        "risk_matrix": baseline_decision_dir / "portfolio_baseline_risk_matrix.json",
        "field_requirement_matrix": baseline_decision_dir
        / "portfolio_baseline_field_requirement_matrix.json",
        "recommendation": baseline_decision_dir / "portfolio_baseline_recommendation.json",
        "recommended_baseline": baseline_decision_dir
        / "recommended_exposure_cap_simulation_baseline.json",
        "task_route": baseline_decision_dir / "exposure_cap_2326_task_route.json",
        "safety_boundary": baseline_decision_dir
        / "portfolio_baseline_source_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2325 baseline decision")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2325 {key}", payload)
    selected = str(mapping(payloads["recommended_baseline"]).get("selected_for_2326"))
    if selected != SELECTED_BASELINE:
        raise SourceBoundStaticEtfDryRunError(
            f"TRADING-2326 requires {SELECTED_BASELINE}; got {selected}"
        )
    if mapping(payloads["summary"]).get("simulation_executed") is not False:
        raise SourceBoundStaticEtfDryRunError(
            "TRADING-2325 baseline decision must not have executed simulation"
        )
    return {"source_dir": str(baseline_decision_dir), **payloads}


def load_source_binding_outputs(source_binding_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": source_binding_dir / "exposure_cap_source_binding_summary.json",
        "source_inventory": source_binding_dir / "exposure_cap_source_inventory.json",
        "source_gap_matrix": source_binding_dir / "exposure_cap_source_gap_matrix.json",
        "risk_cap_trigger_binding": source_binding_dir
        / "risk_cap_trigger_series_binding_report.json",
        "market_data_binding": source_binding_dir / "market_data_binding_report.json",
        "portfolio_baseline_binding": source_binding_dir
        / "portfolio_baseline_binding_report.json",
        "turnover_rebalance_assumption": source_binding_dir
        / "turnover_rebalance_assumption_report.json",
        "dry_run_readiness": source_binding_dir
        / "source_bound_dry_run_simulation_readiness.json",
        "safety_boundary": source_binding_dir / "source_bound_dry_run_safety_boundary.json",
        "next_task_route": source_binding_dir / "exposure_cap_simulation_next_task_route.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2324 source binding")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2324 {key}", payload)
    return {"source_dir": str(source_binding_dir), **payloads}


def load_simulation_policy_outputs(simulation_policy_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": simulation_policy_dir / "exposure_cap_mechanics_simulation_summary.json",
        "readiness": simulation_policy_dir / "exposure_cap_simulation_readiness_matrix.json",
        "metric_contract": simulation_policy_dir / "exposure_cap_simulation_metric_contract.json",
        "safety_boundary": simulation_policy_dir
        / "exposure_cap_simulation_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2323 simulation policy")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return {"source_dir": str(simulation_policy_dir), **payloads}


def load_exposure_cap_policy(policy_path: Path) -> dict[str, Any]:
    if not policy_path.exists():
        raise SourceBoundStaticEtfDryRunError(f"exposure-cap policy missing: {policy_path}")
    payload = safe_load_yaml_path(policy_path)
    if not isinstance(payload, dict):
        raise SourceBoundStaticEtfDryRunError(f"exposure-cap policy must be object: {policy_path}")
    _validate_no_unsafe_fields("exposure-cap policy", payload)
    if not mapping(payload).get("cap_policy"):
        raise SourceBoundStaticEtfDryRunError("exposure-cap policy missing cap_policy")
    if not mapping(payload).get("cooldown_policy"):
        raise SourceBoundStaticEtfDryRunError("exposure-cap policy missing cooldown_policy")
    return payload


def load_static_etf_config(
    portfolio_config_dir: Path,
    target_assets: Sequence[str],
) -> dict[str, Any]:
    assets_path = portfolio_config_dir / "assets.yaml"
    if not assets_path.exists():
        raise SourceBoundStaticEtfDryRunError(f"static ETF config missing: {assets_path}")
    payload = safe_load_yaml_path(assets_path)
    if not isinstance(payload, dict):
        raise SourceBoundStaticEtfDryRunError("static ETF config must be object")
    asset_map = mapping(payload.get("assets"))
    rows: list[dict[str, Any]] = []
    total_weight = 0.0
    for symbol, raw in asset_map.items():
        item = mapping(raw)
        weight = to_float(item.get("default_weight"), 0.0)
        if weight <= 0.0:
            continue
        asset = str(symbol).upper()
        asset_type = str(item.get("asset_type", "")).upper()
        risk_group = str(item.get("risk_group", "")).lower()
        sleeve = str(item.get("sleeve", "")).lower()
        cash_like = asset == "CASH" or asset_type == "CASH" or risk_group == "cash"
        defensive = cash_like or sleeve == "defense"
        risk_asset = not cash_like and risk_group != "cash"
        total_weight += weight
        rows.append(
            {
                "asset": asset,
                "baseline_weight": round_float(weight),
                "risk_asset_flag": risk_asset,
                "defensive_asset_flag": defensive,
                "cash_like_flag": cash_like,
                "risk_group": risk_group,
                "asset_type": asset_type or "ETF",
            }
        )
    missing_targets = sorted(set(target_assets) - {row["asset"] for row in rows})
    if missing_targets:
        raise SourceBoundStaticEtfDryRunError(
            f"static ETF config missing target assets: {missing_targets}"
        )
    if abs(total_weight - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise SourceBoundStaticEtfDryRunError(
            f"static ETF baseline weights must sum to 1.0; got {round_float(total_weight)}"
        )
    return {
        "source_path": str(assets_path),
        "source_hash": _file_hash(assets_path),
        "source_version": str(
            mapping(payload.get("policy_metadata")).get("version", "")
        ),
        "assets": rows,
        "total_weight": round_float(total_weight),
        "target_assets": list(target_assets),
    }


def load_risk_cap_trigger_frame_from_source_binding(
    source_binding: Mapping[str, Any],
    target_assets: Sequence[str],
) -> pd.DataFrame:
    trigger_report = mapping(source_binding.get("risk_cap_trigger_binding"))
    source_path_value = trigger_report.get("source_path")
    if not source_path_value:
        raise SourceBoundStaticEtfDryRunError("risk-cap trigger source_path missing")
    source_path = Path(str(source_path_value))
    if not source_path.exists():
        raise SourceBoundStaticEtfDryRunError(
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
        raise SourceBoundStaticEtfDryRunError(
            f"risk-cap trigger series missing columns: {sorted(missing)}"
        )
    frame = frame.copy()
    frame["scope_active_bool"] = frame["scope_active"].map(_to_bool)
    frame = frame.loc[
        (frame["scope_active_bool"])
        & (frame["usage_role"].astype(str) == "risk_cap_only")
        & (frame["target_asset"].astype(str).isin(set(target_assets)))
    ].copy()
    if frame.empty:
        raise SourceBoundStaticEtfDryRunError("risk-cap trigger series has no active rows")
    frame["trigger_date"] = pd.to_datetime(frame["source_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["trigger_date"])
    frame["risk_cap_score_numeric"] = pd.to_numeric(
        frame["risk_cap_score"],
        errors="coerce",
    ).fillna(0.0)
    frame.attrs["source_path"] = str(source_path)
    frame.attrs["source_hash"] = _file_hash(source_path)
    return frame


def build_simulation_calendar_from_sources(
    *,
    trigger_frame: pd.DataFrame,
    price_matrix: pd.DataFrame,
    target_assets: Sequence[str],
) -> list[date]:
    trigger_dates = sorted(set(trigger_frame["trigger_date"]))
    complete_prices = price_matrix.loc[:, list(target_assets)].dropna(how="any")
    if not trigger_dates or complete_prices.empty:
        return []
    start = max(trigger_dates[0], date.fromisoformat(DEFAULT_BACKTEST_START))
    end = min(trigger_dates[-1], pd.Timestamp(complete_prices.index.max()).date())
    if end < start:
        return []
    mask = (complete_prices.index.date >= start) & (complete_prices.index.date <= end)
    return [pd.Timestamp(ts).date() for ts in complete_prices.loc[mask].index]


def run_data_quality_gate(
    *,
    market_data_source: Path,
    rates_source: Path,
    marketstack_prices_source: Path | None,
    target_assets: Sequence[str],
    quality_as_of: str | date | None,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    resolved_as_of = _parse_optional_date(quality_as_of) or max_price_date(
        market_data_source
    )
    report = validate_data_cache(
        prices_path=market_data_source,
        rates_path=rates_source,
        expected_price_tickers=list(target_assets),
        expected_rate_series=[],
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        secondary_prices_path=marketstack_prices_source
        if marketstack_prices_source is not None and marketstack_prices_source.exists()
        else None,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, resolved_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def build_static_etf_baseline_exposure_schedule(
    *,
    static_config: Mapping[str, Any],
    simulation_dates: Sequence[date],
) -> list[dict[str, Any]]:
    assets = records(static_config.get("assets"))
    first_by_month: set[str] = set()
    for current in simulation_dates:
        month_key = f"{current.year:04d}-{current.month:02d}"
        if month_key not in first_by_month:
            first_by_month.add(month_key)
    first_dates = {
        min(current for current in simulation_dates if current.strftime("%Y-%m") == month)
        for month in first_by_month
    }
    source_path = str(static_config.get("source_path", ""))
    source_hash = str(static_config.get("source_hash", ""))
    rows: list[dict[str, Any]] = []
    for current in simulation_dates:
        rebalance_flag = current in first_dates
        timestamp = f"{current.isoformat()}T00:00:00+00:00"
        for asset in assets:
            risk_flag = bool(asset.get("risk_asset_flag"))
            weight = to_float(asset.get("baseline_weight"))
            rows.append(
                {
                    "date": current.isoformat(),
                    "asset": str(asset.get("asset")),
                    "baseline_weight": round_float(weight),
                    "baseline_weight_for_dry_run": round_float(weight),
                    "risk_asset_flag": risk_flag,
                    "defensive_asset_flag": bool(asset.get("defensive_asset_flag")),
                    "cash_like_flag": bool(asset.get("cash_like_flag")),
                    "baseline_risk_asset_exposure": round_float(weight if risk_flag else 0.0),
                    "rebalance_flag": rebalance_flag,
                    "baseline_source": source_path,
                    "baseline_source_hash": source_hash,
                    "as_of_timestamp": timestamp,
                    "decision_timestamp": timestamp,
                    **SAFETY_FIELDS,
                }
            )
    return clean_for_yaml(rows)


def build_portfolio_level_trigger_map(
    trigger_frame: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in trigger_frame.to_dict(orient="records"):
        grouped[str(row["trigger_date"])].append(row)
    result: dict[str, dict[str, Any]] = {}
    for current, rows_for_date in grouped.items():
        max_score = max(to_float(row.get("risk_cap_score_numeric")) for row in rows_for_date)
        intensity = _max_intensity(
            str(row.get("risk_cap_intensity", "")) for row in rows_for_date
        )
        result[current] = {
            "risk_cap_triggered": True,
            "risk_cap_intensity": intensity,
            "risk_cap_score": round_float(max_score),
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "source_candidate_id": RISK_CAP_CANDIDATE_ID,
            "source_trigger_assets": sorted(
                {str(row.get("target_asset", "")) for row in rows_for_date}
            ),
        }
    return result


def build_risk_cap_trigger_alignment_matrix(
    *,
    simulation_dates: Sequence[date],
    target_assets: Sequence[str],
    schedule_rows: Sequence[Mapping[str, Any]],
    price_matrix: pd.DataFrame,
    date_trigger_map: Mapping[str, Mapping[str, Any]],
    trigger_source_hash: str,
) -> list[dict[str, Any]]:
    baseline_keys = {
        (str(row.get("date")), str(row.get("asset")))
        for row in schedule_rows
        if row.get("risk_asset_flag") is True
    }
    rows: list[dict[str, Any]] = []
    for current in simulation_dates:
        current_key = current.isoformat()
        trigger = mapping(date_trigger_map.get(current_key))
        for asset in target_assets:
            market_available = _market_data_available(price_matrix, asset, current)
            baseline_available = (current_key, asset) in baseline_keys
            ineligible: list[str] = []
            if not market_available:
                ineligible.append("missing_market_data")
            if not baseline_available:
                ineligible.append("missing_baseline_exposure")
            rows.append(
                {
                    "date": current_key,
                    "target_asset": asset,
                    "risk_cap_triggered": bool(trigger.get("risk_cap_triggered", False)),
                    "risk_cap_intensity": str(trigger.get("risk_cap_intensity", "none")),
                    "risk_cap_score": round_float(trigger.get("risk_cap_score", 0.0)),
                    "scope_active": bool(trigger.get("scope_active", False)),
                    "signal_direction": str(trigger.get("signal_direction", "none")),
                    "source_candidate_id": RISK_CAP_CANDIDATE_ID,
                    "trigger_source_hash": trigger_source_hash,
                    "market_data_available": market_available,
                    "baseline_exposure_available": baseline_available,
                    "simulation_eligible": not ineligible,
                    "ineligible_reason": ";".join(ineligible),
                    **SAFETY_FIELDS,
                }
            )
    return clean_for_yaml(rows)


def build_source_bound_static_etf_dry_run_rows(
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
        (str(row.get("date")), str(row.get("asset"))): row for row in schedule_rows
    }
    baseline_assets = sorted({str(row.get("asset")) for row in schedule_rows})
    risk_assets = [
        asset
        for asset in target_assets
        if any(
            row.get("asset") == asset and row.get("risk_asset_flag") is True
            for row in schedule_rows
        )
    ]
    eligible = {
        (str(row.get("date")), str(row.get("target_asset"))): bool(
            row.get("simulation_eligible")
        )
        for row in alignment_rows
    }
    date_index = {
        pd.Timestamp(ts).date(): pos for pos, ts in enumerate(price_matrix.index)
    }
    first_date = simulation_dates[0].isoformat()
    previous_final = {
        asset: to_float(schedule_by_key.get((first_date, asset), {}).get("baseline_weight"))
        for asset in baseline_assets
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
            to_float(schedule_by_key[(current_key, asset)].get("baseline_weight"))
            for asset in risk_assets
            if (current_key, asset) in schedule_by_key
        )
        capped_total_risk = min(baseline_total_risk, max_total_allowed)
        cap_active = capped_total_risk < baseline_total_risk
        transition = (
            "entry"
            if cap_active and not previous_cap_active
            else "exit"
            if previous_cap_active and not cap_active
            else "active"
            if cap_active
            else "inactive"
        )
        scale = (
            capped_total_risk / baseline_total_risk
            if baseline_total_risk > 0.0
            else 1.0
        )
        for asset in baseline_assets:
            schedule = mapping(schedule_by_key.get((current_key, asset)))
            baseline = to_float(schedule.get("baseline_weight"))
            risk_asset = bool(schedule.get("risk_asset_flag"))
            market_eligible = (current_key, asset) in eligible and eligible[(current_key, asset)]
            final_exposure = baseline * scale if risk_asset else baseline
            max_allowed_asset = baseline * scale if risk_asset else baseline
            exposure_delta = baseline - final_exposure
            asset_return = (
                _asset_return(price_matrix, asset, current, date_index)
                if market_eligible
                else 0.0
            )
            turnover_proxy = abs(final_exposure - previous_final.get(asset, baseline))
            rows.append(
                {
                    "date": current_key,
                    "asset": asset,
                    "target_asset": asset,
                    "baseline_weight_for_dry_run": round_float(baseline),
                    "baseline_risk_asset_exposure": round_float(
                        baseline if risk_asset else 0.0
                    ),
                    "risk_cap_triggered": triggered,
                    "risk_cap_intensity": intensity if triggered else "none",
                    "risk_cap_score": round_float(trigger.get("risk_cap_score", 0.0)),
                    "simulated_max_allowed_exposure": round_float(max_allowed_asset),
                    "simulated_max_allowed_total_risk_exposure": round_float(
                        max_total_allowed
                    ),
                    "simulated_final_exposure_after_cap": round_float(final_exposure),
                    "simulated_exposure_delta": round_float(exposure_delta),
                    "simulated_cap_binding_active": cap_active and risk_asset,
                    "simulated_cap_transition": transition,
                    "simulated_cooldown_state": (
                        "active" if cooldown_remaining > 0 else "inactive"
                    ),
                    "simulated_cooldown_days_remaining": cooldown_remaining,
                    "simulated_turnover_proxy": round_float(turnover_proxy),
                    "asset_return": round_float(asset_return),
                    "no_cap_return_contribution_proxy": round_float(
                        baseline * asset_return
                    ),
                    "capped_return_contribution_proxy": round_float(
                        final_exposure * asset_return
                    ),
                    "manual_review_required": triggered or (cap_active and risk_asset),
                    "data_quality_status": data_quality_status,
                    **SAFETY_FIELDS,
                }
            )
            previous_final[asset] = final_exposure
        previous_cap_active = cap_active
        if cooldown_remaining > 0:
            cooldown_remaining -= 1
    return clean_for_yaml(rows)


def build_exposure_cap_vs_no_cap_static_etf_comparison(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> dict[str, Any]:
    record_count = len(dry_run_rows)
    dates = sorted({str(row.get("date")) for row in dry_run_rows})
    cap_rows = [row for row in dry_run_rows if row.get("simulated_cap_binding_active") is True]
    cap_dates = sorted({str(row.get("date")) for row in cap_rows})
    reductions = [to_float(row.get("simulated_exposure_delta")) for row in dry_run_rows]
    no_cap_return = sum(
        to_float(row.get("no_cap_return_contribution_proxy")) for row in dry_run_rows
    )
    capped_return = sum(
        to_float(row.get("capped_return_contribution_proxy")) for row in dry_run_rows
    )
    daily_returns = _daily_return_proxy(dry_run_rows)
    no_cap_drawdown = _max_drawdown(
        [values["no_cap"] for _, values in sorted(daily_returns.items())]
    )
    capped_drawdown = _max_drawdown(
        [values["capped"] for _, values in sorted(daily_returns.items())]
    )
    missed_upside = _missed_upside_cost(dry_run_rows)
    downside_protection = _downside_protection(dry_run_rows)
    return clean_for_yaml(
        {
            "simulation_mode": MODE,
            "portfolio_source_mode": SELECTED_BASELINE,
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
            "no_cap_return_proxy": round_float(no_cap_return),
            "capped_return_proxy": round_float(capped_return),
            "return_proxy_delta": round_float(capped_return - no_cap_return),
            "no_cap_max_drawdown_proxy": round_float(no_cap_drawdown),
            "capped_max_drawdown_proxy": round_float(capped_drawdown),
            "drawdown_proxy_delta": round_float(capped_drawdown - no_cap_drawdown),
            "false_risk_cap_cost_proxy": round_float(missed_upside),
            "missed_upside_cost_proxy": round_float(missed_upside),
            "downside_protection_proxy": round_float(downside_protection),
            "manual_review_trigger_count": sum(
                1 for row in dry_run_rows if row.get("manual_review_required") is True
            ),
            "data_quality_status": data_quality_status,
            "interpretation_boundary": "static_etf_baseline_dry_run_proxy_diagnostics_only",
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_binding_day_matrix(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in dry_run_rows:
        by_date[str(row.get("date"))].append(row)
    rows: list[dict[str, Any]] = []
    for current, date_rows in sorted(by_date.items()):
        cap_rows = [row for row in date_rows if row.get("simulated_cap_binding_active") is True]
        rows.append(
            {
                "date": current,
                "cap_binding_active_any_asset": bool(cap_rows),
                "cap_binding_assets": [str(row.get("asset")) for row in cap_rows],
                "cap_binding_asset_count": len(cap_rows),
                "risk_cap_intensity_max": _max_intensity(
                    str(row.get("risk_cap_intensity", "none")) for row in date_rows
                ),
                "risk_cap_intensity_average": _average_intensity(date_rows),
                "baseline_risk_exposure_total": round_float(
                    sum(to_float(row.get("baseline_risk_asset_exposure")) for row in date_rows)
                ),
                "capped_risk_exposure_total": round_float(
                    sum(
                        to_float(row.get("simulated_final_exposure_after_cap"))
                        for row in date_rows
                        if to_float(row.get("baseline_risk_asset_exposure")) > 0.0
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
                **SAFETY_FIELDS,
            }
        )
    return clean_for_yaml(rows)


def build_turnover_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_date: dict[str, float] = defaultdict(float)
    for row in dry_run_rows:
        by_date[str(row.get("date"))] += to_float(row.get("simulated_turnover_proxy"))
    total = sum(by_date.values())
    return clean_for_yaml(
        {
            "turnover_proxy_total": round_float(total),
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
            "average_daily_turnover_proxy": round_float(
                total / len(by_date) if by_date else 0.0
            ),
            "max_daily_turnover_proxy": round_float(max(by_date.values()) if by_date else 0.0),
            "turnover_spike_days": [
                day for day, value in by_date.items() if value == max(by_date.values())
            ]
            if by_date
            else [],
            "turnover_impact_label": "LOW_TURNOVER_IMPACT"
            if total == 0.0
            else "TURNOVER_IMPACT_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_cooldown_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    cooldown_rows = [
        row for row in dry_run_rows if row.get("simulated_cooldown_state") == "active"
    ]
    cooldown_dates = {str(row.get("date")) for row in cooldown_rows}
    cooldown_delta = sum(
        to_float(row.get("capped_return_contribution_proxy"))
        - to_float(row.get("no_cap_return_contribution_proxy"))
        for row in cooldown_rows
    )
    trigger_dates = {
        row.get("date") for row in dry_run_rows if row.get("risk_cap_triggered") is True
    }
    false_cost = sum(
        max(
            0.0,
            to_float(row.get("no_cap_return_contribution_proxy"))
            - to_float(row.get("capped_return_contribution_proxy")),
        )
        for row in cooldown_rows
        if to_float(row.get("asset_return")) > 0.0
    )
    return clean_for_yaml(
        {
            "cooldown_trigger_count": len(
                trigger_dates
            ),
            "cooldown_active_days": len(cooldown_dates),
            "average_cooldown_length": round_float(
                len(cooldown_dates)
                / len(trigger_dates)
                if trigger_dates
                else 0.0
            ),
            "cooldown_prevented_reentry_days": len(
                {
                    row.get("date")
                    for row in cooldown_rows
                    if row.get("risk_cap_triggered") is False
                    and row.get("simulated_cap_binding_active") is True
                }
            ),
            "cooldown_exposure_reduction_total": round_float(
                sum(to_float(row.get("simulated_exposure_delta")) for row in cooldown_rows)
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


def build_false_risk_cap_cost_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    false_rows = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    ]
    missed = _missed_upside_cost(dry_run_rows)
    return clean_for_yaml(
        {
            "false_risk_cap_count": len(false_rows),
            "false_risk_cap_days": len({row.get("date") for row in false_rows}),
            "false_risk_cap_cost_proxy": round_float(missed),
            "missed_upside_cost_proxy": round_float(missed),
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
            else "FALSE_COST_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_missed_upside_cost_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    missed = _missed_upside_cost(dry_run_rows)
    return clean_for_yaml(
        {
            "missed_upside_cost_proxy": round_float(missed),
            "missed_upside_days": len(
                {
                    row.get("date")
                    for row in dry_run_rows
                    if row.get("simulated_cap_binding_active") is True
                    and to_float(row.get("asset_return")) > 0.0
                }
            ),
            "interpretation_boundary": "missed_upside_proxy_not_real_opportunity_cost",
            **SAFETY_FIELDS,
        }
    )


def build_downside_protection_proxy_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    protection = _downside_protection(dry_run_rows)
    drawdown_rows = [
        row
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) < 0.0
    ]
    return clean_for_yaml(
        {
            "risk_cap_trigger_count": len(
                {row.get("date") for row in dry_run_rows if row.get("risk_cap_triggered") is True}
            ),
            "post_trigger_drawdown_capture_count": len(drawdown_rows),
            "post_trigger_stress_capture_count": len(drawdown_rows),
            "downside_tail_capture_count": len(drawdown_rows),
            "downside_protection_proxy": round_float(protection),
            "drawdown_reduction_proxy": round_float(protection),
            "stress_window_exposure_reduction": round_float(
                sum(to_float(row.get("simulated_exposure_delta")) for row in drawdown_rows)
            ),
            "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
            if protection > 0.0
            else "DOWNSIDE_PROTECTION_INCONCLUSIVE",
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_data_quality_report(
    *,
    quality_report: DataQualityReport,
    alignment_rows: Sequence[Mapping[str, Any]],
    schedule_rows: Sequence[Mapping[str, Any]],
    trigger_frame: pd.DataFrame,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    missing_market = sum(1 for row in alignment_rows if not row.get("market_data_available"))
    missing_baseline = sum(
        1 for row in alignment_rows if not row.get("baseline_exposure_available")
    )
    errors = quality_report.error_count + missing_market + missing_baseline
    warnings = quality_report.warning_count
    status = "FAIL" if errors > 0 else "PASS_WITH_WARNINGS" if warnings > 0 else "PASS"
    return clean_for_yaml(
        {
            "market_data_status": quality_report.status,
            "baseline_data_status": "PASS" if schedule_rows else "FAIL",
            "risk_cap_trigger_data_status": "PASS" if not trigger_frame.empty else "FAIL",
            "policy_data_status": "PASS" if policy.get("cap_policy") else "FAIL",
            "record_count": len(alignment_rows),
            "eligible_record_count": sum(
                1 for row in alignment_rows if row.get("simulation_eligible") is True
            ),
            "missing_market_data_count": missing_market,
            "missing_baseline_exposure_count": missing_baseline,
            "missing_trigger_count": 0 if not trigger_frame.empty else len(alignment_rows),
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


def build_interpretation_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
        "task_id": TASK_ID,
        "generated_at": generated_at.isoformat(),
        "portfolio_source_mode": SELECTED_BASELINE,
        "data_quality_status": data_quality_status,
        "forbidden_interpretations": [
            "real_account_performance",
            "real_position_advice",
            "paper_shadow_signal",
            "production_strategy",
            "broker_action",
        ],
        **SAFETY_FIELDS,
    }


def build_exposure_cap_2327_task_route(
    *,
    comparison: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(data_quality_report.get("data_quality_status", "FAIL"))
    record_count = int(to_float(comparison.get("record_count"), 0.0))
    if status == "FAIL":
        next_task = "TRADING-2327_Exposure_Cap_Static_Baseline_Data_Remediation"
    elif record_count > 0:
        next_task = "TRADING-2327_Exposure_Cap_vs_No_Cap_Diagnostics_Review"
    else:
        next_task = "TRADING-2327_Exposure_Cap_Dynamic_Target_Baseline_Preparation"
    return {
        "schema_version": f"{REPORT_TYPE}.task_route.v1",
        "task_id": TASK_ID,
        "next_task": next_task,
        "route_reason": (
            "data quality passed and static ETF dry-run produced eligible records"
            if next_task == "TRADING-2327_Exposure_Cap_vs_No_Cap_Diagnostics_Review"
            else "source remediation required before diagnostics review"
        ),
        "record_count": record_count,
        "data_quality_status": status,
        **SAFETY_FIELDS,
    }


def build_static_etf_baseline_source_report(
    *,
    static_config: Mapping[str, Any],
    schedule_rows: Sequence[Mapping[str, Any]],
    baseline_decision: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.baseline_source.v1",
            "baseline_source_mode": SELECTED_BASELINE,
            "source_path": static_config.get("source_path"),
            "source_hash": static_config.get("source_hash"),
            "source_version": static_config.get("source_version"),
            "baseline_weight_sum": static_config.get("total_weight"),
            "schedule_record_count": len(schedule_rows),
            "selected_for_2326": mapping(baseline_decision.get("recommended_baseline")).get(
                "selected_for_2326"
            ),
            "interpretation_boundary": "static_config_baseline_not_real_account",
            **SAFETY_FIELDS,
        }
    )


def build_source_bound_static_etf_dry_run_summary(
    *,
    generated_at: datetime,
    source_binding_dir: Path,
    baseline_decision_dir: Path,
    simulation_policy_dir: Path,
    portfolio_config_dir: Path,
    market_data_source: Path,
    rates_source: Path,
    policy_path: Path,
    target_assets: Sequence[str],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    schedule_rows: Sequence[Mapping[str, Any]],
    alignment_rows: Sequence[Mapping[str, Any]],
    dry_run_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "title": "Source-Bound Exposure-Cap Dry-Run With Static ETF Baseline",
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_binding_dir": str(source_binding_dir),
            "baseline_decision_dir": str(baseline_decision_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "portfolio_config_dir": str(portfolio_config_dir),
            "market_data_source": str(market_data_source),
            "rates_source": str(rates_source),
            "policy_path": str(policy_path),
            "target_assets": list(target_assets),
            "selected_baseline": SELECTED_BASELINE,
            "portfolio_source_mode": SELECTED_BASELINE,
            "data_quality_status": quality_report.status,
            "data_quality_gate_required": True,
            "data_quality_gate_executed": True,
            "data_quality_report_path": str(quality_report_path),
            "source_bound_static_etf_dry_run_cli": True,
            "static_etf_baseline_schedule_generated": bool(schedule_rows),
            "risk_cap_trigger_alignment_generated": bool(alignment_rows),
            "dry_run_result_generated": bool(dry_run_rows),
            "exposure_cap_vs_no_cap_comparison_generated": True,
            "turnover_impact_report_generated": True,
            "cooldown_impact_report_generated": True,
            "false_risk_cap_cost_report_generated": True,
            "downside_protection_proxy_report_generated": True,
            "data_quality_report_generated": True,
            "record_count": len(dry_run_rows),
            "simulation_start": comparison.get("simulation_start", ""),
            "simulation_end": comparison.get("simulation_end", ""),
            "cap_binding_days": comparison.get("cap_binding_days", 0),
            "cap_binding_rate": comparison.get("cap_binding_rate", 0.0),
            "return_proxy_delta": comparison.get("return_proxy_delta", 0.0),
            "drawdown_proxy_delta": comparison.get("drawdown_proxy_delta", 0.0),
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def write_source_bound_static_etf_dry_run_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    baseline_report: Mapping[str, Any],
    schedule_rows: Sequence[Mapping[str, Any]],
    alignment_rows: Sequence[Mapping[str, Any]],
    dry_run_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    binding_rows: Sequence[Mapping[str, Any]],
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
    false_cost_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    downside_report: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "source_bound_static_etf_dry_run_summary.json",
        "baseline_source_report": output_dir / "static_etf_baseline_source_report.json",
        "schedule_json": output_dir / "static_etf_baseline_exposure_schedule.json",
        "schedule_csv": output_dir / "static_etf_baseline_exposure_schedule.csv",
        "alignment_json": output_dir / "risk_cap_trigger_alignment_matrix.json",
        "alignment_csv": output_dir / "risk_cap_trigger_alignment_matrix.csv",
        "dry_run_json": output_dir
        / "source_bound_static_etf_exposure_cap_dry_run_result.json",
        "dry_run_csv": output_dir
        / "source_bound_static_etf_exposure_cap_dry_run_result.csv",
        "comparison_json": output_dir / "exposure_cap_vs_no_cap_static_etf_comparison.json",
        "comparison_csv": output_dir / "exposure_cap_vs_no_cap_static_etf_comparison.csv",
        "binding_json": output_dir / "exposure_cap_binding_day_matrix.json",
        "binding_csv": output_dir / "exposure_cap_binding_day_matrix.csv",
        "turnover": output_dir / "exposure_cap_turnover_impact_report.json",
        "cooldown": output_dir / "exposure_cap_cooldown_impact_report.json",
        "false_cost": output_dir / "exposure_cap_false_risk_cap_cost_report.json",
        "missed_upside": output_dir / "exposure_cap_missed_upside_cost_report.json",
        "downside": output_dir / "exposure_cap_downside_protection_proxy_report.json",
        "data_quality": output_dir / "exposure_cap_data_quality_report.json",
        "boundary": output_dir / "exposure_cap_simulation_interpretation_boundary.json",
        "task_route": output_dir / "exposure_cap_2327_task_route.json",
        "report_doc": docs_root
        / "source_bound_exposure_cap_dry_run_static_etf_report.md",
        "schedule_doc": docs_root / "static_etf_baseline_exposure_schedule.md",
        "comparison_doc": docs_root / "exposure_cap_vs_no_cap_static_etf_comparison.md",
        "turnover_cooldown_doc": docs_root
        / "exposure_cap_turnover_and_cooldown_impact.md",
        "false_downside_doc": docs_root
        / "exposure_cap_false_cost_and_downside_protection.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["baseline_source_report"], dict(baseline_report))
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
    write_json(paths["turnover"], dict(turnover_report))
    write_json(paths["cooldown"], dict(cooldown_report))
    write_json(paths["false_cost"], dict(false_cost_report))
    write_json(paths["missed_upside"], dict(missed_upside_report))
    write_json(paths["downside"], dict(downside_report))
    write_json(paths["data_quality"], dict(data_quality_report))
    write_json(paths["boundary"], dict(interpretation_boundary))
    write_json(paths["task_route"], dict(task_route))
    write_markdown(
        paths["report_doc"],
        _render_main_report(
            summary,
            comparison,
            turnover_report,
            cooldown_report,
            false_cost_report,
            downside_report,
        ),
    )
    write_markdown(paths["schedule_doc"], _render_schedule_doc(summary, schedule_rows))
    write_markdown(paths["comparison_doc"], _render_comparison_doc(comparison))
    write_markdown(
        paths["turnover_cooldown_doc"],
        _render_turnover_cooldown_doc(turnover_report, cooldown_report),
    )
    write_markdown(
        paths["false_downside_doc"],
        _render_false_downside_doc(false_cost_report, downside_report),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_main_report(
    summary: Mapping[str, Any],
    comparison: Mapping[str, Any],
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
    false_cost_report: Mapping[str, Any],
    downside_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Static ETF Baseline Source-Bound Exposure-Cap Dry-Run",
            "",
            "TRADING-2326 使用 TRADING-2325 选定的 `static_etf_allocation_baseline` "
            "执行 source-bound exposure-cap dry-run simulation。这里的 cap 解释为"
            "组合层面的总风险资产敞口上限，再按 QQQ / SPY / SMH baseline 权重等比例"
            "缩放风险资产；`CASH` 保持 static baseline 权重。static ETF baseline 不是"
            "实际账户，所有 return / drawdown / turnover 数值都是 proxy diagnostics。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- selected_baseline: `{summary['selected_baseline']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_gate_executed: `{summary['data_quality_gate_executed']}`",
            f"- simulation_start: `{comparison['simulation_start']}`",
            f"- simulation_end: `{comparison['simulation_end']}`",
            f"- cap_binding_days: `{comparison['cap_binding_days']}`",
            f"- cap_binding_rate: `{comparison['cap_binding_rate']}`",
            f"- average_exposure_reduction: `{comparison['average_exposure_reduction']}`",
            f"- return_proxy_delta: `{comparison['return_proxy_delta']}`",
            f"- drawdown_proxy_delta: `{comparison['drawdown_proxy_delta']}`",
            f"- turnover_impact_label: `{turnover_report['turnover_impact_label']}`",
            f"- cooldown_impact_label: `{cooldown_report['cooldown_impact_label']}`",
            f"- false_risk_cap_cost_proxy: `{false_cost_report['false_risk_cap_cost_proxy']}`",
            f"- downside_protection_proxy: `{downside_report['downside_protection_proxy']}`",
            f"- next_task: `{summary['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## 解释边界",
            "",
            "本报告不得解释为真实账户表现、真实仓位建议、paper-shadow signal、"
            "production strategy 或 broker action；也不得把 proxy 改善/损失直接解释为"
            "exposure-cap 可实盘使用。",
            "",
        ]
    )


def _render_schedule_doc(
    summary: Mapping[str, Any],
    schedule_rows: Sequence[Mapping[str, Any]],
) -> str:
    assets = sorted({str(row.get("asset")) for row in schedule_rows})
    return "\n".join(
        [
            "# Static ETF Baseline Exposure Schedule",
            "",
            "本文件记录 TRADING-2326 使用的 static ETF baseline 每日 dry-run exposure "
            "schedule。它只用于 source-bound simulation，不是 target weight 或 rebalance "
            "instruction。",
            "",
            f"- status: `{summary['status']}`",
            f"- record_count: `{len(schedule_rows)}`",
            f"- assets: `{', '.join(assets)}`",
            "- baseline_source_mode: `static_etf_allocation_baseline`",
            "- target_weight_generated: `False`",
            "- rebalance_instruction_generated: `False`",
            "",
        ]
    )


def _render_comparison_doc(comparison: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Exposure-Cap vs No-Cap Static ETF Comparison",
            "",
            "本比较只衡量 static ETF baseline dry-run 下 cap 与 no-cap 的 proxy 差异，"
            "不得解释为真实策略收益、真实回撤保护或生产结论。",
            "",
            f"- simulation_mode: `{comparison['simulation_mode']}`",
            f"- record_count: `{comparison['record_count']}`",
            f"- cap_binding_days: `{comparison['cap_binding_days']}`",
            f"- cap_binding_rate: `{comparison['cap_binding_rate']}`",
            f"- no_cap_return_proxy: `{comparison['no_cap_return_proxy']}`",
            f"- capped_return_proxy: `{comparison['capped_return_proxy']}`",
            f"- return_proxy_delta: `{comparison['return_proxy_delta']}`",
            f"- no_cap_max_drawdown_proxy: `{comparison['no_cap_max_drawdown_proxy']}`",
            f"- capped_max_drawdown_proxy: `{comparison['capped_max_drawdown_proxy']}`",
            f"- drawdown_proxy_delta: `{comparison['drawdown_proxy_delta']}`",
            f"- interpretation_boundary: `{comparison['interpretation_boundary']}`",
            "",
        ]
    )


def _render_turnover_cooldown_doc(
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Turnover and Cooldown Impact",
            "",
            "本报告只披露 simulated exposure delta 产生的 turnover proxy 与 cooldown proxy，"
            "不代表真实换手、真实交易成本或执行建议。",
            "",
            f"- turnover_proxy_total: `{turnover_report['turnover_proxy_total']}`",
            f"- average_daily_turnover_proxy: "
            f"`{turnover_report['average_daily_turnover_proxy']}`",
            f"- turnover_impact_label: `{turnover_report['turnover_impact_label']}`",
            f"- cooldown_trigger_count: `{cooldown_report['cooldown_trigger_count']}`",
            f"- cooldown_active_days: `{cooldown_report['cooldown_active_days']}`",
            f"- cooldown_return_proxy_delta: "
            f"`{cooldown_report['cooldown_return_proxy_delta']}`",
            f"- cooldown_impact_label: `{cooldown_report['cooldown_impact_label']}`",
            "",
        ]
    )


def _render_false_downside_doc(
    false_cost_report: Mapping[str, Any],
    downside_report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap False Cost and Downside Protection",
            "",
            "本报告只披露 cap 触发后 missed upside / downside protection 的 proxy "
            "diagnostics，不能作为实盘保护效果或机会成本结论。",
            "",
            f"- false_risk_cap_count: `{false_cost_report['false_risk_cap_count']}`",
            f"- missed_upside_cost_proxy: "
            f"`{false_cost_report['missed_upside_cost_proxy']}`",
            f"- false_risk_cap_cost_label: "
            f"`{false_cost_report['false_risk_cap_cost_label']}`",
            f"- downside_protection_proxy: "
            f"`{downside_report['downside_protection_proxy']}`",
            f"- downside_tail_capture_count: "
            f"`{downside_report['downside_tail_capture_count']}`",
            f"- downside_protection_label: "
            f"`{downside_report['downside_protection_label']}`",
            "",
        ]
    )


def _policy_path(policy_path: Path | None, source_binding: Mapping[str, Any]) -> Path:
    if policy_path is not None:
        return policy_path
    value = mapping(source_binding.get("summary")).get("policy_path")
    return Path(str(value)) if value else DEFAULT_POLICY_PATH


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise SourceBoundStaticEtfDryRunError(
            f"{label} required artifacts missing: " + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def _validate_policy(policy: Mapping[str, Any], assets: Sequence[str]) -> None:
    required_symbols = set(
        mapping(policy.get("source_requirements")).get("required_market_symbols", [])
    )
    if required_symbols and not set(assets).issubset(required_symbols):
        raise SourceBoundStaticEtfDryRunError(
            "target assets must be covered by policy required_market_symbols"
        )


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    banned_values = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
    }
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise SourceBoundStaticEtfDryRunError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise SourceBoundStaticEtfDryRunError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise SourceBoundStaticEtfDryRunError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise SourceBoundStaticEtfDryRunError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise SourceBoundStaticEtfDryRunError(f"{name} opens {forbidden}")
        for value in item.values():
            if isinstance(value, str) and value in banned_values:
                raise SourceBoundStaticEtfDryRunError(
                    f"{name} emits banned value {value}"
                )


def _walk_mappings(payload: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        found.append(payload)
        for value in payload.values():
            found.extend(_walk_mappings(value))
    elif isinstance(payload, list | tuple):
        for value in payload:
            found.extend(_walk_mappings(value))
    return found


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


def _daily_return_proxy(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    daily: dict[str, dict[str, float]] = defaultdict(lambda: {"no_cap": 0.0, "capped": 0.0})
    for row in dry_run_rows:
        day = str(row.get("date"))
        daily[day]["no_cap"] += to_float(row.get("no_cap_return_contribution_proxy"))
        daily[day]["capped"] += to_float(row.get("capped_return_contribution_proxy"))
    return daily


def _max_drawdown(returns: Sequence[float]) -> float:
    value = 1.0
    peak = 1.0
    drawdown = 0.0
    for item in returns:
        value *= 1.0 + to_float(item)
        peak = max(peak, value)
        if peak > 0:
            drawdown = min(drawdown, value / peak - 1.0)
    return drawdown


def _missed_upside_cost(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("no_cap_return_contribution_proxy"))
            - to_float(row.get("capped_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    )


def _downside_protection(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("capped_return_contribution_proxy"))
            - to_float(row.get("no_cap_return_contribution_proxy")),
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
    values = [rank.get(str(row.get("risk_cap_intensity", "none")).lower(), 0.0) for row in rows]
    return round_float(sum(values) / len(values) if values else 0.0)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _normalize_list(value: str | Sequence[str]) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def _parse_optional_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SourceBoundStaticEtfDryRunError(f"required JSON missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SourceBoundStaticEtfDryRunError(f"JSON must be object: {path}")
    return payload


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
