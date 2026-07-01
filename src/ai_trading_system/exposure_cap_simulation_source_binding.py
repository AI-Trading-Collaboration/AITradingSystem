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
from ai_trading_system.exposure_cap_mechanics_simulation import (
    ARTIFACT_ROLE as SOURCE_2323_ARTIFACT_ROLE,
)
from ai_trading_system.exposure_cap_mechanics_simulation import (
    STATUS as SOURCE_2323_STATUS,
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
    data_quality_payload,
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
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_VALIDATION_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    RISK_CAP_CANDIDATE_ID,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_GENERATOR_ROOT,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2324_EXPOSURE_CAP_SIMULATION_SOURCE_BINDING"
REPORT_TYPE = "exposure_cap_simulation_source_binding"
ARTIFACT_ROLE = "exposure_cap_simulation_source_binding"
MODE = "source_bound_dry_run_readiness"
STATUS = "EXPOSURE_CAP_SIMULATION_SOURCE_BOUND_DRY_RUN_READY_PROMOTION_BLOCKED"
FULL_SIMULATION_STATUS = "FULL_SIMULATION_BLOCKED"

READY = "SOURCE_BOUND_DRY_RUN_READY"
READY_WITH_WARNINGS = "SOURCE_BOUND_DRY_RUN_READY_WITH_WARNINGS"
READY_WITH_SYNTHETIC_BASELINE = "SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE"
BLOCKED = "SOURCE_BOUND_DRY_RUN_BLOCKED"

DEFAULT_SIMULATION_POLICY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_mechanics_simulation"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "exposure_cap_simulation_source_binding_policy.yaml"
)
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")

SOURCE_CATEGORIES = (
    "risk_cap_trigger_series",
    "market_price_history",
    "portfolio_baseline",
    "rebalance_calendar",
    "turnover_assumption",
    "cooldown_policy",
    "exposure_cap_policy",
    "simulation_calendar",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "manual_review_only": True,
    "dry_run_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}


class ExposureCapSimulationSourceBindingError(ValueError):
    pass


def run_exposure_cap_simulation_source_binding(
    *,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    risk_cap_source_dir: Path = DEFAULT_SCOPE_GENERATOR_ROOT,
    scope_validation_dir: Path = DEFAULT_SCOPE_VALIDATION_ROOT,
    market_data_source: Path = DEFAULT_PRICES_PATH,
    rates_source: Path = DEFAULT_RATES_PATH,
    marketstack_prices_source: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    portfolio_baseline_source: Path | None = None,
    policy_path: Path = DEFAULT_POLICY_PATH,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise ExposureCapSimulationSourceBindingError(
            f"exposure-cap simulation source binding only supports {MODE} mode"
        )
    assets = _normalize_list(target_assets) or list(DEFAULT_TARGET_ASSETS)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    policy = _load_policy(policy_path)
    _validate_policy(policy, assets)
    source_2323 = load_trading_2323_source_artifacts(simulation_policy_dir)
    scope_validation = load_scope_validation_artifacts(scope_validation_dir)
    _validate_upstream_sources(source_2323, scope_validation, policy)

    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=market_data_source,
        rates_path=rates_source,
        marketstack_prices_path=marketstack_prices_source,
        target_assets=assets,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise ExposureCapSimulationSourceBindingError(
            f"cached data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )

    trigger_frame = load_risk_cap_trigger_frame(risk_cap_source_dir, assets)
    price_matrix = load_adjusted_price_matrix(market_data_source, assets)
    simulation_dates = build_simulation_calendar(
        trigger_frame=trigger_frame,
        price_matrix=price_matrix,
        target_assets=assets,
    )
    if not simulation_dates:
        raise ExposureCapSimulationSourceBindingError(
            "no overlapping simulation calendar after binding risk-cap triggers "
            "and market data"
        )
    trigger_map = build_trigger_binding_map(trigger_frame)
    portfolio_rows, portfolio_mode, real_portfolio_bound = build_portfolio_baseline(
        portfolio_baseline_source=portfolio_baseline_source,
        policy=policy,
        simulation_dates=simulation_dates,
        target_assets=assets,
    )
    portfolio_map = {
        (row["date"], row["target_asset"]): to_float(row["baseline_exposure"])
        for row in portfolio_rows
    }
    dry_run_rows = build_source_bound_dry_run_rows(
        policy=policy,
        simulation_dates=simulation_dates,
        target_assets=assets,
        price_matrix=price_matrix,
        trigger_map=trigger_map,
        portfolio_map=portfolio_map,
    )
    comparison = build_exposure_cap_vs_no_cap_comparison(
        dry_run_rows=dry_run_rows,
        portfolio_source_mode=portfolio_mode,
        data_quality_status=quality_report.status,
    )
    turnover_report = build_turnover_impact_report(dry_run_rows, policy)
    cooldown_report = build_cooldown_impact_report(dry_run_rows, policy)
    risk_cap_report = build_risk_cap_trigger_series_binding_report(
        trigger_frame=trigger_frame,
        risk_cap_source_dir=risk_cap_source_dir,
        scope_validation=scope_validation,
    )
    market_report = build_market_data_binding_report(
        prices_path=market_data_source,
        rates_path=rates_source,
        marketstack_prices_path=marketstack_prices_source,
        price_matrix=price_matrix,
        target_assets=assets,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        simulation_dates=simulation_dates,
    )
    portfolio_report = build_portfolio_baseline_binding_report(
        portfolio_baseline_source=portfolio_baseline_source,
        portfolio_rows=portfolio_rows,
        portfolio_source_mode=portfolio_mode,
        real_portfolio_bound=real_portfolio_bound,
    )
    turnover_assumption_report = build_turnover_rebalance_assumption_report(
        policy=policy,
        simulation_dates=simulation_dates,
    )
    inventory_rows = build_exposure_cap_source_inventory(
        policy_path=policy_path,
        risk_cap_source_dir=risk_cap_source_dir,
        market_data_source=market_data_source,
        portfolio_baseline_source=portfolio_baseline_source,
        risk_cap_report=risk_cap_report,
        market_report=market_report,
        portfolio_report=portfolio_report,
        turnover_assumption_report=turnover_assumption_report,
        simulation_dates=simulation_dates,
        data_quality_status=quality_report.status,
    )
    gap_rows = build_exposure_cap_source_gap_matrix(
        inventory_rows=inventory_rows,
        portfolio_source_mode=portfolio_mode,
    )
    readiness = build_source_bound_dry_run_simulation_readiness(
        inventory_rows=inventory_rows,
        portfolio_source_mode=portfolio_mode,
        real_portfolio_bound=real_portfolio_bound,
    )
    safety_boundary = build_source_bound_dry_run_safety_boundary(
        generated_at=generated_at,
        readiness=readiness,
        data_quality_status=quality_report.status,
    )
    next_task_route = build_exposure_cap_simulation_next_task_route(readiness)
    summary = build_exposure_cap_source_binding_summary(
        generated_at=generated_at,
        policy_path=policy_path,
        simulation_policy_dir=simulation_policy_dir,
        risk_cap_source_dir=risk_cap_source_dir,
        scope_validation_dir=scope_validation_dir,
        market_data_source=market_data_source,
        rates_source=rates_source,
        marketstack_prices_source=marketstack_prices_source,
        portfolio_baseline_source=portfolio_baseline_source,
        target_assets=assets,
        simulation_dates=simulation_dates,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        readiness=readiness,
        risk_cap_report=risk_cap_report,
        market_report=market_report,
        portfolio_report=portfolio_report,
        comparison=comparison,
        next_task_route=next_task_route,
    )
    paths = write_exposure_cap_simulation_source_binding_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        inventory_rows=inventory_rows,
        gap_rows=gap_rows,
        risk_cap_report=risk_cap_report,
        market_report=market_report,
        portfolio_report=portfolio_report,
        turnover_assumption_report=turnover_assumption_report,
        readiness=readiness,
        safety_boundary=safety_boundary,
        next_task_route=next_task_route,
        dry_run_rows=dry_run_rows,
        comparison=comparison,
        turnover_report=turnover_report,
        cooldown_report=cooldown_report,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_trading_2323_source_artifacts(source_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": source_dir / "exposure_cap_mechanics_simulation_summary.json",
        "readiness": source_dir / "exposure_cap_simulation_readiness_matrix.json",
        "metric_contract": source_dir / "exposure_cap_simulation_metric_contract.json",
        "safety_boundary": source_dir / "exposure_cap_simulation_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2324 requires TRADING-2323 exposure-cap mechanics artifacts: "
            + ", ".join(missing)
        )
    payloads = {key: _load_json(path) for key, path in paths.items()}
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return payloads


def load_scope_validation_artifacts(scope_validation_dir: Path) -> dict[str, Any]:
    paths = {
        "risk_cap_scorecard": scope_validation_dir / "risk_cap_only_validation_scorecard.json",
        "state_recommendation": scope_validation_dir
        / "scope_narrowed_state_recommendation_matrix.json",
        "active_actual_path": scope_validation_dir / "scope_narrowed_active_actual_path_matrix.csv",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2324 requires TRADING-2292 risk-cap validation artifacts: "
            + ", ".join(missing)
        )
    scorecard = _load_json(paths["risk_cap_scorecard"])
    state = _load_json(paths["state_recommendation"])
    _validate_no_unsafe_fields("TRADING-2292 risk_cap_scorecard", scorecard)
    _validate_no_unsafe_fields("TRADING-2292 state_recommendation", state)
    active_path = paths["active_actual_path"]
    return {
        "risk_cap_scorecard": scorecard,
        "state_recommendation": state,
        "active_actual_path": {
            "path": str(active_path),
            "row_count": _csv_row_count(active_path),
            "source_hash": _file_hash(active_path),
        },
    }


def load_risk_cap_trigger_frame(
    risk_cap_source_dir: Path,
    target_assets: Sequence[str],
) -> pd.DataFrame:
    candidate_dir = risk_cap_source_dir / RISK_CAP_CANDIDATE_ID
    paths = {
        "signal_series": candidate_dir / "scope_narrowed_candidate_signal_series.csv",
        "prediction_artifact": candidate_dir / "scope_narrowed_candidate_prediction_artifact.json",
        "scope_filter": candidate_dir / "scope_filter_report.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2324 requires TRADING-2291 risk-cap source artifacts: "
            + ", ".join(missing)
        )
    prediction_artifact = _load_json(paths["prediction_artifact"])
    scope_filter = _load_json(paths["scope_filter"])
    _validate_no_unsafe_fields("TRADING-2291 prediction_artifact", prediction_artifact)
    _validate_no_unsafe_fields("TRADING-2291 scope_filter", scope_filter)
    frame = pd.read_csv(paths["signal_series"])
    if frame.empty:
        raise ExposureCapSimulationSourceBindingError("risk-cap trigger series is empty")
    required = {
        "target_asset",
        "horizon",
        "source_date",
        "scope_active",
        "usage_role",
        "risk_cap_score",
        "risk_cap_intensity",
    }
    missing_columns = required - set(frame.columns)
    if missing_columns:
        raise ExposureCapSimulationSourceBindingError(
            f"risk-cap trigger series missing columns: {sorted(missing_columns)}"
        )
    frame = frame.copy()
    frame["scope_active_bool"] = frame["scope_active"].map(_to_bool)
    frame = frame.loc[
        (frame["scope_active_bool"])
        & (frame["usage_role"].astype(str) == "risk_cap_only")
        & (frame["target_asset"].astype(str).isin(set(target_assets)))
    ].copy()
    if frame.empty:
        raise ExposureCapSimulationSourceBindingError(
            "no active risk-cap trigger records after binding target assets"
        )
    frame["trigger_date"] = pd.to_datetime(frame["source_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["trigger_date"])
    if frame.empty:
        raise ExposureCapSimulationSourceBindingError(
            "active risk-cap trigger records have no bindable trigger_date"
        )
    frame["risk_cap_score_numeric"] = pd.to_numeric(
        frame["risk_cap_score"],
        errors="coerce",
    ).fillna(0.0)
    frame["source_path"] = str(paths["signal_series"])
    frame["source_hash"] = _file_hash(paths["signal_series"])
    return frame


def build_simulation_calendar(
    *,
    trigger_frame: pd.DataFrame,
    price_matrix: pd.DataFrame,
    target_assets: Sequence[str],
) -> list[date]:
    trigger_dates = sorted(set(trigger_frame["trigger_date"]))
    if not trigger_dates:
        return []
    complete_prices = price_matrix.loc[:, list(target_assets)].dropna(how="any")
    if complete_prices.empty:
        return []
    start = max(trigger_dates[0], date.fromisoformat(DEFAULT_BACKTEST_START))
    end = min(trigger_dates[-1], pd.Timestamp(complete_prices.index.max()).date())
    if end < start:
        return []
    mask = (complete_prices.index.date >= start) & (complete_prices.index.date <= end)
    return [pd.Timestamp(ts).date() for ts in complete_prices.loc[mask].index]


def build_trigger_binding_map(
    trigger_frame: pd.DataFrame,
) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in trigger_frame.to_dict(orient="records"):
        grouped[(str(row["trigger_date"]), str(row["target_asset"]))].append(row)

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for key, rows_for_key in grouped.items():
        max_score = max(to_float(row.get("risk_cap_score_numeric")) for row in rows_for_key)
        intensity = _max_intensity(
            str(row.get("risk_cap_intensity", "")) for row in rows_for_key
        )
        result[key] = {
            "risk_cap_triggered": True,
            "risk_cap_score": round_float(max_score),
            "risk_cap_intensity": intensity,
            "triggered_horizons": sorted({str(row.get("horizon", "")) for row in rows_for_key}),
            "risk_cap_reason": ";".join(
                sorted({str(row.get("risk_cap_reason", "")) for row in rows_for_key})
            ),
            "source_signal_records": len(rows_for_key),
        }
    return result


def build_portfolio_baseline(
    *,
    portfolio_baseline_source: Path | None,
    policy: Mapping[str, Any],
    simulation_dates: Sequence[date],
    target_assets: Sequence[str],
) -> tuple[list[dict[str, Any]], str, bool]:
    if portfolio_baseline_source is not None and portfolio_baseline_source.exists():
        rows = _load_portfolio_baseline_rows(portfolio_baseline_source, target_assets)
        if rows:
            return rows, "provided_portfolio_baseline", True
    dry_run_policy = mapping(policy.get("dry_run_policy"))
    if dry_run_policy.get("synthetic_baseline_source_mode") != "synthetic_observe_only":
        raise ExposureCapSimulationSourceBindingError(
            "synthetic observe-only baseline is not allowed by policy"
        )
    exposures = mapping(dry_run_policy.get("synthetic_baseline_exposure"))
    rows: list[dict[str, Any]] = []
    for current in simulation_dates:
        for asset in target_assets:
            rows.append(
                {
                    "date": current.isoformat(),
                    "target_asset": asset,
                    "baseline_exposure": round_float(exposures.get(asset, 1.0)),
                    "portfolio_source_mode": "synthetic_observe_only",
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
            )
    return rows, "synthetic_observe_only", False


def build_source_bound_dry_run_rows(
    *,
    policy: Mapping[str, Any],
    simulation_dates: Sequence[date],
    target_assets: Sequence[str],
    price_matrix: pd.DataFrame,
    trigger_map: Mapping[tuple[str, str], Mapping[str, Any]],
    portfolio_map: Mapping[tuple[str, str], float],
) -> list[dict[str, Any]]:
    cap_policy = mapping(policy.get("cap_policy"))
    cooldown_policy = mapping(policy.get("cooldown_policy"))
    cap_by_intensity = mapping(cap_policy.get("max_allowed_exposure_by_intensity"))
    default_cap = to_float(cap_policy.get("default_max_allowed_exposure"), 1.0)
    cooldown_by_intensity = mapping(cooldown_policy.get("cooldown_days_by_intensity"))
    default_cooldown = int(to_float(cooldown_policy.get("default_cooldown_days"), 3.0))
    rows: list[dict[str, Any]] = []
    first_date = simulation_dates[0].isoformat()
    previous_final: dict[str, float] = {
        asset: to_float(portfolio_map.get((first_date, asset), 1.0))
        for asset in target_assets
    }
    cooldown_remaining: dict[str, int] = {asset: 0 for asset in target_assets}
    cooldown_cap: dict[str, float] = {asset: default_cap for asset in target_assets}
    date_index = {pd.Timestamp(ts).date(): pos for pos, ts in enumerate(price_matrix.index)}
    for current in simulation_dates:
        current_key = current.isoformat()
        for asset in target_assets:
            trigger = mapping(trigger_map.get((current_key, asset)))
            triggered = bool(trigger.get("risk_cap_triggered", False))
            intensity = str(trigger.get("risk_cap_intensity") or "none")
            if triggered:
                max_allowed = to_float(cap_by_intensity.get(intensity), default_cap)
                cooldown_cap[asset] = max_allowed
                cooldown_remaining[asset] = max(
                    cooldown_remaining[asset],
                    int(to_float(cooldown_by_intensity.get(intensity), default_cooldown)),
                )
            elif cooldown_remaining[asset] > 0:
                max_allowed = cooldown_cap[asset]
            else:
                max_allowed = default_cap
                cooldown_cap[asset] = default_cap
            baseline = to_float(portfolio_map.get((current_key, asset), 1.0))
            final_exposure = min(baseline, max_allowed)
            exposure_delta = baseline - final_exposure
            asset_return = _asset_return(price_matrix, asset, current, date_index)
            turnover_proxy = abs(final_exposure - previous_final.get(asset, baseline))
            row = {
                "date": current_key,
                "target_asset": asset,
                "baseline_exposure": round_float(baseline),
                "risk_cap_triggered": triggered,
                "risk_cap_intensity": intensity if triggered else "none",
                "triggered_horizons": trigger.get("triggered_horizons", []),
                "risk_cap_score": round_float(trigger.get("risk_cap_score", 0.0)),
                "max_allowed_exposure": round_float(max_allowed),
                "final_exposure_after_cap": round_float(final_exposure),
                "exposure_delta": round_float(exposure_delta),
                "cooldown_state": "active" if cooldown_remaining[asset] > 0 else "inactive",
                "cooldown_days_remaining": cooldown_remaining[asset],
                "turnover_proxy": round_float(turnover_proxy),
                "asset_return": round_float(asset_return),
                "baseline_return_contribution": round_float(baseline * asset_return),
                "capped_return_contribution": round_float(final_exposure * asset_return),
                "cap_binding_active": final_exposure < baseline,
                "manual_review_required": triggered or final_exposure < baseline,
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
            rows.append(row)
            previous_final[asset] = final_exposure
            if cooldown_remaining[asset] > 0:
                cooldown_remaining[asset] -= 1
    return clean_for_yaml(rows)


def build_exposure_cap_vs_no_cap_comparison(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    portfolio_source_mode: str,
    data_quality_status: str,
) -> dict[str, Any]:
    record_count = len(dry_run_rows)
    cap_rows = [row for row in dry_run_rows if row.get("cap_binding_active") is True]
    exposure_reductions = [to_float(row.get("exposure_delta")) for row in dry_run_rows]
    baseline_return = sum(to_float(row.get("baseline_return_contribution")) for row in dry_run_rows)
    capped_return = sum(to_float(row.get("capped_return_contribution")) for row in dry_run_rows)
    date_returns: dict[str, dict[str, float]] = defaultdict(
        lambda: {"baseline": 0.0, "capped": 0.0}
    )
    for row in dry_run_rows:
        date_key = str(row.get("date"))
        date_returns[date_key]["baseline"] += to_float(row.get("baseline_return_contribution"))
        date_returns[date_key]["capped"] += to_float(row.get("capped_return_contribution"))
    baseline_drawdown = _max_drawdown(
        [values["baseline"] for _, values in sorted(date_returns.items())]
    )
    capped_drawdown = _max_drawdown(
        [values["capped"] for _, values in sorted(date_returns.items())]
    )
    missed_upside = sum(
        max(
            0.0,
            to_float(row.get("baseline_return_contribution"))
            - to_float(row.get("capped_return_contribution")),
        )
        for row in cap_rows
        if to_float(row.get("asset_return")) > 0.0
    )
    downside_protection = sum(
        max(
            0.0,
            to_float(row.get("capped_return_contribution"))
            - to_float(row.get("baseline_return_contribution")),
        )
        for row in cap_rows
        if to_float(row.get("asset_return")) < 0.0
    )
    return clean_for_yaml(
        {
            "simulation_mode": MODE,
            "portfolio_source_mode": portfolio_source_mode,
            "record_count": record_count,
            "cap_binding_days": len({row.get("date") for row in cap_rows}),
            "cap_binding_rate": round_float(len(cap_rows) / record_count if record_count else 0.0),
            "average_exposure_reduction": round_float(
                sum(exposure_reductions) / record_count if record_count else 0.0
            ),
            "max_exposure_reduction": round_float(
                max(exposure_reductions) if exposure_reductions else 0.0
            ),
            "turnover_proxy_total": round_float(
                sum(to_float(row.get("turnover_proxy")) for row in dry_run_rows)
            ),
            "baseline_return_proxy": round_float(baseline_return),
            "capped_return_proxy": round_float(capped_return),
            "return_proxy_delta": round_float(capped_return - baseline_return),
            "baseline_max_drawdown_proxy": round_float(baseline_drawdown),
            "capped_max_drawdown_proxy": round_float(capped_drawdown),
            "drawdown_proxy_delta": round_float(capped_drawdown - baseline_drawdown),
            "false_risk_cap_cost_proxy": round_float(missed_upside),
            "missed_upside_cost_proxy": round_float(missed_upside),
            "downside_protection_proxy": round_float(downside_protection),
            "manual_review_trigger_count": sum(
                1 for row in dry_run_rows if row.get("manual_review_required") is True
            ),
            "data_quality_status": data_quality_status,
            "interpretation_boundary": (
                "proxy_diagnostics_only_synthetic_observe_baseline"
                if portfolio_source_mode == "synthetic_observe_only"
                else "source_bound_dry_run_proxy_diagnostics_only"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_turnover_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    assumption = mapping(policy.get("turnover_assumption"))
    return clean_for_yaml(
        {
            "turnover_proxy_method": assumption.get("turnover_proxy_method", ""),
            "transaction_cost_model": assumption.get("transaction_cost_model", ""),
            "real_turnover_history_bound": bool(assumption.get("real_turnover_history_bound")),
            "turnover_proxy_total": round_float(
                sum(to_float(row.get("turnover_proxy")) for row in dry_run_rows)
            ),
            "cap_binding_turnover_proxy_total": round_float(
                sum(
                    to_float(row.get("turnover_proxy"))
                    for row in dry_run_rows
                    if row.get("cap_binding_active") is True
                )
            ),
            "interpretation_boundary": "turnover_proxy_not_real_turnover_history",
            **SAFETY_FIELDS,
        }
    )


def build_cooldown_impact_report(
    dry_run_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    active_rows = [row for row in dry_run_rows if row.get("cooldown_state") == "active"]
    return clean_for_yaml(
        {
            "cooldown_policy": mapping(policy.get("cooldown_policy")),
            "cooldown_active_record_count": len(active_rows),
            "cooldown_active_date_count": len({row.get("date") for row in active_rows}),
            "cooldown_cap_binding_record_count": sum(
                1 for row in active_rows if row.get("cap_binding_active") is True
            ),
            "manual_review_record_count": sum(
                1 for row in active_rows if row.get("manual_review_required") is True
            ),
            "interpretation_boundary": "cooldown_proxy_state_not_runtime_state_history",
            **SAFETY_FIELDS,
        }
    )


def build_risk_cap_trigger_series_binding_report(
    *,
    trigger_frame: pd.DataFrame,
    risk_cap_source_dir: Path,
    scope_validation: Mapping[str, Any],
) -> dict[str, Any]:
    source_path = (
        risk_cap_source_dir
        / RISK_CAP_CANDIDATE_ID
        / "scope_narrowed_candidate_signal_series.csv"
    )
    return clean_for_yaml(
        {
            "source_category": "risk_cap_trigger_series",
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "source_path": str(source_path),
            "source_hash": _file_hash(source_path),
            "source_row_count": _csv_row_count(source_path),
            "active_trigger_record_count": int(len(trigger_frame)),
            "bound_asset_count": int(trigger_frame["target_asset"].nunique()),
            "bound_horizon_count": int(trigger_frame["horizon"].nunique()),
            "coverage_start": min(trigger_frame["trigger_date"]).isoformat(),
            "coverage_end": max(trigger_frame["trigger_date"]).isoformat(),
            "scope_state": _risk_cap_scope_state(scope_validation),
            "source_mode": "TRADING_2291_scope_narrowed_candidate_signal_series",
            "data_quality_status": "RESEARCH_OUTPUT_VALIDATED_BY_2292",
            **SAFETY_FIELDS,
        }
    )


def build_market_data_binding_report(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    price_matrix: pd.DataFrame,
    target_assets: Sequence[str],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    simulation_dates: Sequence[date],
) -> dict[str, Any]:
    quality = data_quality_payload(
        quality_report,
        prices_path,
        rates_path,
        marketstack_prices_path,
    )
    quality["required_command"] = "aits validate-data"
    quality["report_path"] = str(quality_report_path)
    bound = price_matrix.loc[
        [pd.Timestamp(item) for item in simulation_dates],
        list(target_assets),
    ]
    non_null = int(bound.notna().sum().sum())
    total = int(bound.shape[0] * bound.shape[1])
    return clean_for_yaml(
        {
            "source_category": "market_price_history",
            "source_path": str(prices_path),
            "source_hash": _file_hash(prices_path),
            "row_count": _csv_row_count(prices_path),
            "coverage_start": simulation_dates[0].isoformat(),
            "coverage_end": simulation_dates[-1].isoformat(),
            "coverage_ratio": round_float(non_null / total if total else 0.0),
            "target_assets": list(target_assets),
            "data_quality_status": quality_report.status,
            "data_quality_gate": quality,
            "source_mode": "validated_cached_market_price_history",
            "pit_status": "cached_daily_adjusted_close",
            **SAFETY_FIELDS,
        }
    )


def build_portfolio_baseline_binding_report(
    *,
    portfolio_baseline_source: Path | None,
    portfolio_rows: Sequence[Mapping[str, Any]],
    portfolio_source_mode: str,
    real_portfolio_bound: bool,
) -> dict[str, Any]:
    dates = sorted({str(row.get("date")) for row in portfolio_rows})
    source_path = str(portfolio_baseline_source) if portfolio_baseline_source else ""
    return clean_for_yaml(
        {
            "source_category": "portfolio_baseline",
            "source_path": source_path,
            "source_hash": _file_hash(portfolio_baseline_source)
            if portfolio_baseline_source and portfolio_baseline_source.exists()
            else "",
            "row_count": len(portfolio_rows),
            "coverage_start": dates[0] if dates else "",
            "coverage_end": dates[-1] if dates else "",
            "portfolio_source_mode": portfolio_source_mode,
            "real_portfolio_baseline_bound": real_portfolio_bound,
            "synthetic_observe_only": portfolio_source_mode == "synthetic_observe_only",
            "data_quality_status": (
                "NOT_APPLICABLE_SYNTHETIC_OBSERVE_ONLY_BASELINE"
                if portfolio_source_mode == "synthetic_observe_only"
                else "PORTFOLIO_BASELINE_BOUND_NO_RUNTIME_MUTATION"
            ),
            "interpretation_boundary": (
                "synthetic baseline outputs are proxy diagnostics only"
                if portfolio_source_mode == "synthetic_observe_only"
                else "provided baseline remains dry-run only"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_turnover_rebalance_assumption_report(
    *,
    policy: Mapping[str, Any],
    simulation_dates: Sequence[date],
) -> dict[str, Any]:
    assumption = mapping(policy.get("turnover_assumption"))
    return clean_for_yaml(
        {
            "source_category": "turnover_assumption",
            "rebalance_calendar_mode": assumption.get("rebalance_calendar_mode", ""),
            "turnover_proxy_method": assumption.get("turnover_proxy_method", ""),
            "transaction_cost_model": assumption.get("transaction_cost_model", ""),
            "real_turnover_history_bound": bool(assumption.get("real_turnover_history_bound")),
            "simulation_calendar_start": simulation_dates[0].isoformat(),
            "simulation_calendar_end": simulation_dates[-1].isoformat(),
            "simulation_trading_day_count": len(simulation_dates),
            "source_mode": "policy_governed_proxy_assumption",
            "interpretation_boundary": "not real turnover or rebalance history",
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_source_inventory(
    *,
    policy_path: Path,
    risk_cap_source_dir: Path,
    market_data_source: Path,
    portfolio_baseline_source: Path | None,
    risk_cap_report: Mapping[str, Any],
    market_report: Mapping[str, Any],
    portfolio_report: Mapping[str, Any],
    turnover_assumption_report: Mapping[str, Any],
    simulation_dates: Sequence[date],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    rows = [
        _inventory_row(
            "risk_cap_trigger_series",
            required=True,
            available=True,
            source_path=str(
                risk_cap_source_dir
                / RISK_CAP_CANDIDATE_ID
                / "scope_narrowed_candidate_signal_series.csv"
            ),
            source_hash=str(risk_cap_report.get("source_hash", "")),
            coverage_start=str(risk_cap_report.get("coverage_start", "")),
            coverage_end=str(risk_cap_report.get("coverage_end", "")),
            coverage_ratio=1.0,
            data_quality_status=str(risk_cap_report.get("data_quality_status", "")),
            pit_status="TRADING_2291_candidate_bound_research_output",
            source_mode=str(risk_cap_report.get("source_mode", "")),
            usage="risk_cap_trigger_series_for_dry_run",
            blocking_if_missing=True,
        ),
        _inventory_row(
            "market_price_history",
            required=True,
            available=True,
            source_path=str(market_data_source),
            source_hash=str(market_report.get("source_hash", "")),
            coverage_start=str(market_report.get("coverage_start", "")),
            coverage_end=str(market_report.get("coverage_end", "")),
            coverage_ratio=to_float(market_report.get("coverage_ratio")),
            data_quality_status=data_quality_status,
            pit_status="validated_cached_daily_price_history",
            source_mode=str(market_report.get("source_mode", "")),
            usage="asset_return_proxy_and_simulation_calendar",
            blocking_if_missing=True,
        ),
        _inventory_row(
            "portfolio_baseline",
            required=True,
            available=True,
            source_path=str(portfolio_baseline_source) if portfolio_baseline_source else "",
            source_hash=str(portfolio_report.get("source_hash", "")),
            coverage_start=str(portfolio_report.get("coverage_start", "")),
            coverage_end=str(portfolio_report.get("coverage_end", "")),
            coverage_ratio=1.0,
            data_quality_status=str(portfolio_report.get("data_quality_status", "")),
            pit_status="synthetic_observe_only"
            if portfolio_report.get("synthetic_observe_only")
            else "provided_portfolio_baseline",
            source_mode=str(portfolio_report.get("portfolio_source_mode", "")),
            usage="baseline_exposure_for_dry_run_only",
            blocking_if_missing=False,
        ),
        _inventory_row(
            "rebalance_calendar",
            required=True,
            available=True,
            source_path=str(market_data_source),
            source_hash=str(market_report.get("source_hash", "")),
            coverage_start=simulation_dates[0].isoformat(),
            coverage_end=simulation_dates[-1].isoformat(),
            coverage_ratio=1.0,
            data_quality_status=data_quality_status,
            pit_status="derived_from_validated_market_trading_days",
            source_mode=str(turnover_assumption_report.get("rebalance_calendar_mode", "")),
            usage="dry_run_calendar_only",
            blocking_if_missing=True,
        ),
        _inventory_row(
            "turnover_assumption",
            required=True,
            available=True,
            source_path=str(policy_path),
            source_hash=_file_hash(policy_path),
            coverage_start=simulation_dates[0].isoformat(),
            coverage_end=simulation_dates[-1].isoformat(),
            coverage_ratio=1.0,
            data_quality_status="POLICY_GOVERNED_PROXY_ASSUMPTION",
            pit_status="not_real_turnover_history",
            source_mode=str(turnover_assumption_report.get("turnover_proxy_method", "")),
            usage="turnover_proxy_diagnostics_only",
            blocking_if_missing=False,
        ),
        _inventory_row(
            "cooldown_policy",
            required=True,
            available=True,
            source_path=str(policy_path),
            source_hash=_file_hash(policy_path),
            coverage_start=simulation_dates[0].isoformat(),
            coverage_end=simulation_dates[-1].isoformat(),
            coverage_ratio=1.0,
            data_quality_status="POLICY_GOVERNED_DRY_RUN_COOLDOWN",
            pit_status="policy_governed_proxy",
            source_mode="source_binding_policy",
            usage="cooldown_state_proxy_for_dry_run",
            blocking_if_missing=True,
        ),
        _inventory_row(
            "exposure_cap_policy",
            required=True,
            available=True,
            source_path=str(policy_path),
            source_hash=_file_hash(policy_path),
            coverage_start=simulation_dates[0].isoformat(),
            coverage_end=simulation_dates[-1].isoformat(),
            coverage_ratio=1.0,
            data_quality_status="POLICY_GOVERNED_DRY_RUN_CAP",
            pit_status="policy_governed_proxy",
            source_mode="source_binding_policy",
            usage="max_allowed_exposure_proxy_for_dry_run",
            blocking_if_missing=True,
        ),
        _inventory_row(
            "simulation_calendar",
            required=True,
            available=True,
            source_path=str(market_data_source),
            source_hash=str(market_report.get("source_hash", "")),
            coverage_start=simulation_dates[0].isoformat(),
            coverage_end=simulation_dates[-1].isoformat(),
            coverage_ratio=1.0,
            data_quality_status=data_quality_status,
            pit_status="derived_from_validated_market_trading_days",
            source_mode="risk_cap_trigger_market_overlap_calendar",
            usage="source_bound_dry_run_calendar",
            blocking_if_missing=True,
        ),
    ]
    return clean_for_yaml(rows)


def build_exposure_cap_source_gap_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    portfolio_source_mode: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in inventory_rows:
        category = str(item.get("source_category"))
        available = item.get("available") is True
        synthetic_portfolio = (
            category == "portfolio_baseline"
            and portfolio_source_mode == "synthetic_observe_only"
        )
        rows.append(
            clean_for_yaml(
                {
                    "source_category": category,
                    "required_for_dry_run": True,
                    "required_for_full_simulation": True,
                    "available": available,
                    "gap_type": (
                        "REAL_PORTFOLIO_BASELINE_MISSING_SYNTHETIC_OBSERVE_ONLY_USED"
                        if synthetic_portfolio
                        else ("NONE_FOR_DRY_RUN" if available else "MISSING_REQUIRED_SOURCE")
                    ),
                    "gap_severity": (
                        "FULL_SIMULATION_BLOCKER"
                        if synthetic_portfolio
                        else ("NONE" if available else "DRY_RUN_BLOCKER")
                    ),
                    "fallback_allowed": synthetic_portfolio
                    or category in {"turnover_assumption", "rebalance_calendar"},
                    "fallback_mode": (
                        "synthetic_observe_only"
                        if synthetic_portfolio
                        else (
                            str(item.get("source_mode", ""))
                            if category in {"turnover_assumption", "rebalance_calendar"}
                            else ""
                        )
                    ),
                    "simulation_status_if_missing": (
                        READY_WITH_SYNTHETIC_BASELINE
                        if synthetic_portfolio
                        else (READY if available else BLOCKED)
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_source_bound_dry_run_simulation_readiness(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    portfolio_source_mode: str,
    real_portfolio_bound: bool,
) -> dict[str, Any]:
    by_category = {str(row.get("source_category")): row for row in inventory_rows}
    required_available = all(
        by_category.get(category, {}).get("available") is True
        for category in SOURCE_CATEGORIES
    )
    if not required_available:
        dry_status = BLOCKED
        source_status = "SOURCE_BINDING_BLOCKED"
        dry_run_allowed = False
    elif portfolio_source_mode == "synthetic_observe_only":
        dry_status = READY_WITH_SYNTHETIC_BASELINE
        source_status = "SOURCE_BOUND_WITH_SYNTHETIC_BASELINE"
        dry_run_allowed = True
    elif not real_portfolio_bound:
        dry_status = READY_WITH_WARNINGS
        source_status = "SOURCE_BOUND_WITH_WARNINGS"
        dry_run_allowed = True
    else:
        dry_status = READY
        source_status = "SOURCE_BOUND"
        dry_run_allowed = True
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "source_binding_status": source_status,
            "dry_run_readiness_status": dry_status,
            "full_simulation_readiness_status": FULL_SIMULATION_STATUS,
            "risk_cap_trigger_series_bound": by_category.get(
                "risk_cap_trigger_series", {}
            ).get("available")
            is True,
            "market_data_bound": by_category.get("market_price_history", {}).get("available")
            is True,
            "portfolio_baseline_bound": by_category.get("portfolio_baseline", {}).get("available")
            is True,
            "real_portfolio_baseline_bound": real_portfolio_bound,
            "portfolio_source_mode": portfolio_source_mode,
            "turnover_assumption_bound": by_category.get("turnover_assumption", {}).get("available")
            is True,
            "cooldown_policy_bound": by_category.get("cooldown_policy", {}).get("available")
            is True,
            "simulation_calendar_bound": by_category.get("simulation_calendar", {}).get("available")
            is True,
            "dry_run_allowed": dry_run_allowed,
            "full_simulation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_source_bound_dry_run_safety_boundary(
    *,
    generated_at: datetime,
    readiness: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "dry_run_readiness_status": readiness.get("dry_run_readiness_status"),
            "research_only": True,
            "observe_only": True,
            "portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_only": True,
            "dry_run_only": True,
            "forbidden_outputs": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
                "production_decision",
                "paper_shadow_ready",
            ],
            "interpretation_boundary": (
                "Source-bound dry-run outputs are proxy diagnostics only and cannot "
                "support promotion, paper-shadow, production, broker action or real "
                "portfolio mutation."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_exposure_cap_simulation_next_task_route(
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(readiness.get("dry_run_readiness_status", ""))
    if status == READY:
        next_task = "TRADING-2325_Source_Bound_Exposure_Cap_Dry_Run_Simulation"
    elif status == READY_WITH_SYNTHETIC_BASELINE:
        next_task = "TRADING-2325_Portfolio_Baseline_Source_Decision"
    elif status == BLOCKED:
        next_task = "TRADING-2325_Exposure_Cap_Source_Gap_Remediation"
    else:
        next_task = "TRADING-2325_Source_Bound_Exposure_Cap_Dry_Run_Simulation"
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "dry_run_readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                "TRADING-2325_Source_Bound_Exposure_Cap_Dry_Run_Simulation",
                "TRADING-2325_Portfolio_Baseline_Source_Decision",
                "TRADING-2325_Exposure_Cap_Source_Gap_Remediation",
                "TRADING-2325_Archive_Exposure_Cap_Simulation_Until_Source_Ready",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_exposure_cap_source_binding_summary(
    *,
    generated_at: datetime,
    policy_path: Path,
    simulation_policy_dir: Path,
    risk_cap_source_dir: Path,
    scope_validation_dir: Path,
    market_data_source: Path,
    rates_source: Path,
    marketstack_prices_source: Path | None,
    portfolio_baseline_source: Path | None,
    target_assets: Sequence[str],
    simulation_dates: Sequence[date],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    readiness: Mapping[str, Any],
    risk_cap_report: Mapping[str, Any],
    market_report: Mapping[str, Any],
    portfolio_report: Mapping[str, Any],
    comparison: Mapping[str, Any],
    next_task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "Exposure-Cap Simulation Source Binding",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "policy_path": str(policy_path),
            "simulation_policy_dir": str(simulation_policy_dir),
            "risk_cap_source_dir": str(risk_cap_source_dir),
            "scope_validation_dir": str(scope_validation_dir),
            "market_data_source": str(market_data_source),
            "rates_source": str(rates_source),
            "marketstack_prices_source": str(marketstack_prices_source or ""),
            "portfolio_baseline_source": str(portfolio_baseline_source or ""),
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "target_assets": list(target_assets),
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": (
                f"{simulation_dates[0].isoformat()}..{simulation_dates[-1].isoformat()}"
            ),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "data_quality_gate_required": True,
            "data_quality_gate_executed": True,
            "source_binding_status": readiness.get("source_binding_status"),
            "dry_run_readiness_status": readiness.get("dry_run_readiness_status"),
            "full_simulation_readiness_status": readiness.get(
                "full_simulation_readiness_status"
            ),
            "dry_run_allowed": readiness.get("dry_run_allowed"),
            "dry_run_executed": readiness.get("dry_run_allowed"),
            "dry_run_result_generated": readiness.get("dry_run_allowed"),
            "full_simulation_allowed": False,
            "portfolio_source_mode": readiness.get("portfolio_source_mode"),
            "real_portfolio_baseline_bound": readiness.get(
                "real_portfolio_baseline_bound"
            ),
            "risk_cap_active_trigger_record_count": risk_cap_report.get(
                "active_trigger_record_count"
            ),
            "market_coverage_ratio": market_report.get("coverage_ratio"),
            "portfolio_baseline_row_count": portfolio_report.get("row_count"),
            "dry_run_record_count": comparison.get("record_count"),
            "cap_binding_days": comparison.get("cap_binding_days"),
            "next_task": next_task_route.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def write_exposure_cap_simulation_source_binding_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    risk_cap_report: Mapping[str, Any],
    market_report: Mapping[str, Any],
    portfolio_report: Mapping[str, Any],
    turnover_assumption_report: Mapping[str, Any],
    readiness: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    next_task_route: Mapping[str, Any],
    dry_run_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "exposure_cap_source_binding_summary.json",
        "source_inventory": output_dir / "exposure_cap_source_inventory.json",
        "source_gap_matrix": output_dir / "exposure_cap_source_gap_matrix.json",
        "risk_cap_trigger_binding": output_dir / "risk_cap_trigger_series_binding_report.json",
        "market_data_binding": output_dir / "market_data_binding_report.json",
        "portfolio_baseline_binding": output_dir / "portfolio_baseline_binding_report.json",
        "turnover_rebalance_assumption": output_dir
        / "turnover_rebalance_assumption_report.json",
        "dry_run_readiness": output_dir / "source_bound_dry_run_simulation_readiness.json",
        "safety_boundary": output_dir / "source_bound_dry_run_safety_boundary.json",
        "next_task_route": output_dir / "exposure_cap_simulation_next_task_route.json",
        "dry_run_result_json": output_dir / "source_bound_exposure_cap_dry_run_result.json",
        "dry_run_result_csv": output_dir / "source_bound_exposure_cap_dry_run_result.csv",
        "comparison": output_dir / "exposure_cap_vs_no_cap_comparison.json",
        "turnover_impact": output_dir / "exposure_cap_turnover_impact_report.json",
        "cooldown_impact": output_dir / "exposure_cap_cooldown_impact_report.json",
        "report_doc": docs_root / "exposure_cap_simulation_source_binding_report.md",
        "source_inventory_doc": docs_root / "exposure_cap_source_inventory.md",
        "dry_run_readiness_doc": docs_root
        / "source_bound_exposure_cap_dry_run_readiness.md",
        "next_task_route_doc": docs_root / "exposure_cap_simulation_next_task_route.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["source_inventory"], {**dict(summary), "sources": inventory_rows})
    write_json(paths["source_gap_matrix"], {**dict(summary), "rows": gap_rows})
    write_json(paths["risk_cap_trigger_binding"], dict(risk_cap_report))
    write_json(paths["market_data_binding"], dict(market_report))
    write_json(paths["portfolio_baseline_binding"], dict(portfolio_report))
    write_json(paths["turnover_rebalance_assumption"], dict(turnover_assumption_report))
    write_json(paths["dry_run_readiness"], dict(readiness))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_json(paths["next_task_route"], dict(next_task_route))
    write_json(paths["dry_run_result_json"], {**dict(summary), "rows": dry_run_rows})
    write_csv_rows(paths["dry_run_result_csv"], dry_run_rows)
    write_json(paths["comparison"], dict(comparison))
    write_json(paths["turnover_impact"], dict(turnover_report))
    write_json(paths["cooldown_impact"], dict(cooldown_report))
    write_markdown(paths["report_doc"], _render_main_report(summary, comparison))
    write_markdown(
        paths["source_inventory_doc"],
        _render_source_inventory_doc(summary, inventory_rows, gap_rows),
    )
    write_markdown(
        paths["dry_run_readiness_doc"],
        _render_readiness_doc(summary, readiness, comparison),
    )
    write_markdown(
        paths["next_task_route_doc"],
        _render_next_task_doc(summary, next_task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_main_report(
    summary: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Simulation Source Binding",
            "",
            "TRADING-2324 将 TRADING-2323 source-blocked simulation readiness 绑定到 "
            "risk-cap trigger series、cached market price history、portfolio baseline "
            "和 policy-governed turnover / cooldown / exposure-cap assumptions。本报告只"
            "是 source-bound dry-run readiness 与 proxy diagnostics，不产生 target weight、"
            "rebalance instruction、paper-shadow、production 或 broker action。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_gate_executed: `{summary['data_quality_gate_executed']}`",
            f"- dry_run_readiness_status: `{summary['dry_run_readiness_status']}`",
            f"- portfolio_source_mode: `{summary['portfolio_source_mode']}`",
            f"- dry_run_record_count: `{summary['dry_run_record_count']}`",
            f"- cap_binding_days: `{summary['cap_binding_days']}`",
            f"- next_task: `{summary['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Proxy Comparison",
            "",
            f"- baseline_return_proxy: `{comparison['baseline_return_proxy']}`",
            f"- capped_return_proxy: `{comparison['capped_return_proxy']}`",
            f"- return_proxy_delta: `{comparison['return_proxy_delta']}`",
            f"- turnover_proxy_total: `{comparison['turnover_proxy_total']}`",
            f"- interpretation_boundary: `{comparison['interpretation_boundary']}`",
            "",
        ]
    )


def _render_source_inventory_doc(
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Exposure-Cap Source Inventory",
        "",
        f"- status: `{summary['status']}`",
        f"- data_quality_status: `{summary['data_quality_status']}`",
        "",
        "|source_category|available|source_mode|data_quality_status|coverage|",
        "|---|---|---|---|---|",
    ]
    for row in inventory_rows:
        lines.append(
            f"|`{row['source_category']}`|`{row['available']}`|"
            f"`{row['source_mode']}`|`{row['data_quality_status']}`|"
            f"`{row['coverage_start']}..{row['coverage_end']}`|"
        )
    lines.extend(["", "## Source Gaps", "", "|source_category|gap_type|gap_severity|"])
    lines.append("|---|---|---|")
    for row in gap_rows:
        lines.append(
            f"|`{row['source_category']}`|`{row['gap_type']}`|"
            f"`{row['gap_severity']}`|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_readiness_doc(
    summary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Source-Bound Exposure-Cap Dry-Run Readiness",
            "",
            f"- dry_run_readiness_status: `{readiness['dry_run_readiness_status']}`",
            "- full_simulation_readiness_status: "
            f"`{readiness['full_simulation_readiness_status']}`",
            f"- portfolio_source_mode: `{readiness['portfolio_source_mode']}`",
            f"- real_portfolio_baseline_bound: `{readiness['real_portfolio_baseline_bound']}`",
            f"- dry_run_allowed: `{readiness['dry_run_allowed']}`",
            f"- full_simulation_allowed: `{readiness['full_simulation_allowed']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- dry_run_record_count: `{comparison['record_count']}`",
            "",
            "Synthetic observe-only baseline 下，return / drawdown / turnover "
            "只能作为 proxy diagnostics。",
            "",
        ]
    )


def _render_next_task_doc(
    summary: Mapping[str, Any],
    next_task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Simulation Next Task Route",
            "",
            f"- dry_run_readiness_status: `{next_task_route['dry_run_readiness_status']}`",
            f"- next_task: `{next_task_route['next_task']}`",
            f"- portfolio_source_mode: `{summary['portfolio_source_mode']}`",
            "",
            "该 route 不打开 promotion、paper-shadow、production 或 broker gates。",
            "",
        ]
    )


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    target_assets: Sequence[str],
    quality_as_of: str | date | None,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    resolved_as_of = _parse_optional_date(quality_as_of) or max_price_date(prices_path)
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(target_assets),
        expected_rate_series=[],
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        secondary_prices_path=marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, resolved_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def _validate_upstream_sources(
    source_2323: Mapping[str, Any],
    scope_validation: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    summary = mapping(source_2323.get("summary"))
    summary_payload = mapping(summary.get("summary")) or summary
    requirements = mapping(policy.get("source_requirements"))
    if _payload_value(summary_payload, "status") != SOURCE_2323_STATUS:
        raise ExposureCapSimulationSourceBindingError("TRADING-2323 status mismatch")
    if _payload_value(summary_payload, "artifact_role") != SOURCE_2323_ARTIFACT_ROLE:
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2323 artifact_role mismatch"
        )
    if _payload_value(summary_payload, "source_blocked_no_simulation") is not True:
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2323 must remain source_blocked_no_simulation"
        )
    if _risk_cap_scope_state(scope_validation) != requirements.get("required_scope_state"):
        raise ExposureCapSimulationSourceBindingError(
            "TRADING-2292 risk-cap state is not forward observe candidate"
        )


def _validate_policy(policy: Mapping[str, Any], assets: Sequence[str]) -> None:
    required = (
        "policy_id",
        "version",
        "status",
        "owner",
        "task_id",
        "market_regime",
        "source_requirements",
        "dry_run_policy",
        "cap_policy",
        "cooldown_policy",
        "turnover_assumption",
        "safety",
    )
    missing = [field for field in required if not policy.get(field)]
    if missing:
        raise ExposureCapSimulationSourceBindingError(f"policy missing fields: {missing}")
    if policy.get("policy_id") != "exposure_cap_simulation_source_binding_policy":
        raise ExposureCapSimulationSourceBindingError("unexpected policy_id")
    if policy.get("task_id") != TASK_ID:
        raise ExposureCapSimulationSourceBindingError("policy task_id mismatch")
    if policy.get("market_regime") != MARKET_REGIME:
        raise ExposureCapSimulationSourceBindingError("policy market_regime mismatch")
    requirements = mapping(policy.get("source_requirements"))
    if requirements.get("required_candidate_id") != RISK_CAP_CANDIDATE_ID:
        raise ExposureCapSimulationSourceBindingError("policy candidate mismatch")
    required_symbols = set(requirements.get("required_market_symbols", []))
    if not set(assets).issubset(required_symbols):
        raise ExposureCapSimulationSourceBindingError(
            "target assets must be covered by policy required_market_symbols"
        )
    if requirements.get("required_2323_status") != SOURCE_2323_STATUS:
        raise ExposureCapSimulationSourceBindingError("policy 2323 status mismatch")
    if requirements.get("required_2323_artifact_role") != SOURCE_2323_ARTIFACT_ROLE:
        raise ExposureCapSimulationSourceBindingError("policy 2323 role mismatch")
    for field, expected in SAFETY_FIELDS.items():
        if mapping(policy.get("safety")).get(field) != expected:
            raise ExposureCapSimulationSourceBindingError(
                f"policy safety.{field} must be {expected}"
            )


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    banned_recommendations = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
    }
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise ExposureCapSimulationSourceBindingError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise ExposureCapSimulationSourceBindingError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise ExposureCapSimulationSourceBindingError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise ExposureCapSimulationSourceBindingError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise ExposureCapSimulationSourceBindingError(
                    f"{name} opens {forbidden}"
                )
        for value in item.values():
            if isinstance(value, str) and value in banned_recommendations:
                raise ExposureCapSimulationSourceBindingError(
                    f"{name} emits banned recommendation {value}"
                )


def _risk_cap_scope_state(scope_validation: Mapping[str, Any]) -> str:
    state_payload = mapping(scope_validation.get("state_recommendation"))
    for row in records(state_payload.get("candidate_rows")):
        if row.get("scope_narrowed_candidate_id") == RISK_CAP_CANDIDATE_ID:
            return str(row.get("recommended_research_status", ""))
    return ""


def _inventory_row(
    source_category: str,
    *,
    required: bool,
    available: bool,
    source_path: str,
    source_hash: str,
    coverage_start: str,
    coverage_end: str,
    coverage_ratio: float,
    data_quality_status: str,
    pit_status: str,
    source_mode: str,
    usage: str,
    blocking_if_missing: bool,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "source_name": source_category,
            "source_category": source_category,
            "required": required,
            "available": available,
            "source_path": source_path,
            "source_hash": source_hash,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
            "coverage_ratio": round_float(coverage_ratio),
            "data_quality_status": data_quality_status,
            "pit_status": pit_status,
            "source_mode": source_mode,
            "usage": usage,
            "blocking_if_missing": blocking_if_missing,
            **SAFETY_FIELDS,
        }
    )


def _asset_return(
    price_matrix: pd.DataFrame,
    asset: str,
    current: date,
    date_index: Mapping[date, int],
) -> float:
    position = date_index.get(current)
    if position is None or position <= 0:
        return 0.0
    series = price_matrix[asset]
    current_price = to_float(series.iloc[position])
    previous_price = to_float(series.iloc[position - 1])
    if previous_price <= 0.0:
        return 0.0
    return current_price / previous_price - 1.0


def _max_drawdown(returns: Sequence[float]) -> float:
    value = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for item in returns:
        value *= 1.0 + to_float(item)
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = min(max_drawdown, value / peak - 1.0)
    return max_drawdown


def _max_intensity(values: Sequence[str]) -> str:
    rank = {"none": 0, "low": 1, "medium": 2, "high": 3}
    normalized = [value.lower() for value in values if value]
    if not normalized:
        return "medium"
    return max(normalized, key=lambda item: rank.get(item, 2))


def _load_portfolio_baseline_rows(
    path: Path,
    target_assets: Sequence[str],
) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = _load_json(path)
        rows = records(payload.get("rows"))
    else:
        rows = pd.read_csv(path).to_dict(orient="records")
    required = {"date", "target_asset", "baseline_exposure"}
    result: list[dict[str, Any]] = []
    for row in rows:
        if not required.issubset(row):
            continue
        if str(row.get("target_asset")) not in set(target_assets):
            continue
        result.append(
            clean_for_yaml(
                {
                    "date": str(row.get("date")),
                    "target_asset": str(row.get("target_asset")),
                    "baseline_exposure": round_float(row.get("baseline_exposure")),
                    "portfolio_source_mode": "provided_portfolio_baseline",
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
            )
        )
    return result


def _payload_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    return mapping(payload.get("summary")).get(key)


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ExposureCapSimulationSourceBindingError(f"policy file missing: {path}")
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise ExposureCapSimulationSourceBindingError(f"policy must be object: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ExposureCapSimulationSourceBindingError(f"required JSON missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ExposureCapSimulationSourceBindingError(f"JSON must be object: {path}")
    return payload


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        found.append(value)
        for child in value.values():
            found.extend(_walk_mappings(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_mappings(child))
    return found


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


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _file_hash(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


__all__ = [
    "ARTIFACT_ROLE",
    "BLOCKED",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_SIMULATION_POLICY_ROOT",
    "DEFAULT_TARGET_ASSETS",
    "MODE",
    "READY",
    "READY_WITH_SYNTHETIC_BASELINE",
    "READY_WITH_WARNINGS",
    "REPORT_TYPE",
    "SAFETY_FIELDS",
    "STATUS",
    "TASK_ID",
    "ExposureCapSimulationSourceBindingError",
    "build_exposure_cap_source_gap_matrix",
    "build_exposure_cap_source_inventory",
    "build_risk_cap_trigger_series_binding_report",
    "build_source_bound_dry_run_simulation_readiness",
    "load_risk_cap_trigger_frame",
    "load_scope_validation_artifacts",
    "load_trading_2323_source_artifacts",
    "run_exposure_cap_simulation_source_binding",
]
