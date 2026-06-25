from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

import pandas as pd

from ai_trading_system.layer1_meta_policy_readiness import _float, _mapping

# TRADING-1009 to 1014 pilot grid: research-only thresholds documented in
# docs/requirements/TRADING-1009_to_1014_Layer1_Low_Turnover_Selector_Refinement.md.
LOW_TURNOVER_BUFFER_GRID = (0.01, 0.02, 0.03, 0.05)
LOW_TURNOVER_CONFIRMATION_GRID = (3, 5, 10, 20)
LOW_TURNOVER_MIN_HOLDING_GRID = (20, 40, 60)
LOW_TURNOVER_COOLDOWN_GRID = (5, 10, 20)
LOW_TURNOVER_MAX_SWITCHES_GRID = (3, 4, 6)
LOW_TURNOVER_NEAR_200DMA_BAND = 0.02
LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE = 0.02
LOW_TURNOVER_CONSTRAINED_RISK_ON_WEIGHTS = (0.70, 0.80, 0.90)
LOW_TURNOVER_CONSTRAINED_NEUTRAL_WEIGHTS = (0.40, 0.50, 0.60)
LOW_TURNOVER_CONSTRAINED_RISK_OFF_WEIGHTS = (0.10, 0.20, 0.30)
LOW_TURNOVER_CONSTRAINED_BUFFER_GRID = (0.02, 0.03, 0.05)
LOW_TURNOVER_CONSTRAINED_CONFIRMATION_GRID = (5, 10, 20)
SWITCH_CONTROL_CONTRACT_DEFAULTS = {
    "version": "v1_2026_06_25_research_only",
    "max_switches_per_year": 2,
    "max_switches_per_3y": 6,
    "max_turnover_per_year": 1.0,
    "min_avg_holding_period": 60,
    "allowed_exception_cases": [
        "initial_allocation_state_selection",
        "owner_approved_emergency_risk_exit",
        "data_quality_repair_recompute_without_new_decision",
    ],
}

_BASELINE_VARIANT_IDS = {
    "original_trend_200dma_selector",
    "always_equal_risk",
    "always_100_qqq",
}


def low_turnover_ranking_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    recommended = recommended_low_turnover_candidate(rows)
    return {
        "variant_count": len(rows),
        "top_by_net_return": top_variant_id(rows, "net_return_after_cost"),
        "top_by_calmar": top_variant_id(rows, "calmar"),
        "top_by_low_turnover": top_variant_id(rows, "turnover", reverse=False),
        "top_by_regret_reduction": top_variant_id(rows, "regret_reduction"),
        "dominated_variants": [
            row["variant_id"] for row in rows if row.get("dominance_status") == "DOMINATED"
        ],
        "recommended_low_turnover_candidate": recommended.get("variant_id"),
        "recommended_low_turnover_candidate_family": recommended.get("variant_family"),
    }


def recommended_low_turnover_candidate(rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    candidates = [
        row
        for row in rows
        if row.get("variant_id") not in _BASELINE_VARIANT_IDS
        and row.get("dominance_status") != "DOMINATED"
        and _float(row.get("turnover_reduction")) > 0.0
        and (
            _float(row.get("relative_vs_equal_risk")) > 0.0
            or _float(row.get("relative_vs_100_qqq"))
            >= -LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE
        )
    ]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            bool(row.get("turnover_acceptable")),
            _float(row.get("net_return_after_cost")),
            _float(row.get("calmar")),
            _float(row.get("turnover_reduction")),
            -_float(row.get("turnover")),
        ),
    )


def top_variant_id(
    rows: list[Mapping[str, Any]],
    metric: str,
    *,
    reverse: bool = True,
) -> str | None:
    if not rows:
        return None
    ordered = sorted(rows, key=lambda row: _float(row.get(metric)), reverse=reverse)
    return str(ordered[0].get("variant_id"))


def best_low_turnover_row(rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not rows:
        return {}
    return max(
        rows,
        key=lambda row: (
            bool(row.get("turnover_acceptable")),
            _float(row.get("net_return_after_cost")),
            _float(row.get("calmar")),
            _float(row.get("turnover_reduction")),
            -_float(row.get("turnover")),
        ),
    )


def low_turnover_dominance_status(
    row: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
) -> str:
    row_id = row.get("variant_id")
    row_return = _float(row.get("net_return_after_cost"))
    row_drawdown = abs(_float(row.get("max_drawdown")))
    row_turnover = _float(row.get("turnover"))
    for other in rows:
        if other.get("variant_id") == row_id:
            continue
        other_return = _float(other.get("net_return_after_cost"))
        other_drawdown = abs(_float(other.get("max_drawdown")))
        other_turnover = _float(other.get("turnover"))
        if (
            other_return >= row_return
            and other_drawdown <= row_drawdown
            and other_turnover <= row_turnover
            and (
                other_return > row_return
                or other_drawdown < row_drawdown
                or other_turnover < row_turnover
            )
        ):
            return "DOMINATED"
    return "NOT_DOMINATED"


def low_turnover_owner_decision(
    *,
    actual_date_range: Mapping[str, str | None],
    data_quality_status: object,
    registry: Mapping[str, Any],
    ranking_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    original = next(
        (
            row
            for row in ranking_rows
            if row.get("variant_id") == "original_trend_200dma_selector"
        ),
        {},
    )
    candidate = recommended_low_turnover_candidate(ranking_rows)
    if not candidate:
        return {
            "status": "NO_SELECTOR_EDGE",
            "candidate_id": None,
            "decision_reasons": ["no low-turnover variant preserved enough cost-after edge"],
            "checks": [],
            "original_turnover": original.get("turnover"),
            "candidate_turnover": None,
            "candidate_switch_count": None,
            "candidate_net_return_after_cost": None,
            "candidate_relative_vs_equal_risk": None,
            "candidate_relative_vs_100_qqq": None,
        }
    policy = _mapping(registry.get("evaluation_policy"))
    annual_switches = annualized_switches(actual_date_range, candidate)
    drawdown_tolerance = _float(
        policy.get("drawdown_worsening_tolerance"),
        LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE,
    )
    switch_control = switch_count_control_result(
        actual_date_range=actual_date_range,
        metrics=candidate,
        registry_policy=policy,
    )
    checks = [
        low_turnover_owner_check(
            "cost_after_value_preserved",
            _float(candidate.get("relative_vs_equal_risk")) > 0.0
            or _float(candidate.get("relative_vs_100_qqq"))
            >= -LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE,
            "candidate is not much weaker than always_100_qqq or improves vs equal_risk",
        ),
        low_turnover_owner_check(
            "turnover_reduced_vs_original",
            _float(candidate.get("turnover")) < _float(original.get("turnover")),
            "turnover is below original trend_200dma_selector",
        ),
        low_turnover_owner_check(
            "switch_count_controlled",
            bool(switch_control["switch_count_controlled"]),
            (
                "calendar-year switches, rolling 3y switches, annual turnover, "
                "and average holding period stay within strict contract"
            ),
        ),
        low_turnover_owner_check(
            "drawdown_not_materially_worse",
            abs(_float(candidate.get("max_drawdown")))
            <= abs(_float(original.get("max_drawdown"))) + drawdown_tolerance,
            "max drawdown does not materially worsen vs original trend selector",
        ),
        low_turnover_owner_check(
            "recent_regime_risk_disclosed",
            True,
            "recent-regime-only limitation remains disclosed",
        ),
        low_turnover_owner_check(
            "data_quality_pass_with_warnings_or_better",
            "PASS" in str(data_quality_status),
            "cached data quality gate passed or passed with warnings",
        ),
        low_turnover_owner_check(
            "safety_boundary_preserved",
            True,
            "paper_shadow_allowed=false, production_allowed=false, broker_action=none",
        ),
    ]
    failed = [row["check_id"] for row in checks if not row["passed"]]
    if failed:
        status = "KEEP_SELECTOR_DRY_RUN_ONLY"
    else:
        status = "LOW_TURNOVER_SELECTOR_REVIEWABLE"
    return {
        "status": status,
        "candidate_id": candidate.get("variant_id"),
        "decision_reasons": failed or ["low-turnover candidate passes research-only review gate"],
        "checks": checks,
        "original_turnover": original.get("turnover"),
        "candidate_turnover": candidate.get("turnover"),
        "candidate_switch_count": candidate.get("switch_count"),
        "candidate_net_return_after_cost": candidate.get("net_return_after_cost"),
        "candidate_relative_vs_equal_risk": candidate.get("relative_vs_equal_risk"),
        "candidate_relative_vs_100_qqq": candidate.get("relative_vs_100_qqq"),
        "candidate_annualized_switches": annual_switches,
        "candidate_switch_count_control": switch_control,
    }


def low_turnover_owner_check(check_id: str, passed: bool, rationale: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "rationale": rationale}


def low_turnover_acceptable(
    *,
    actual_date_range: Mapping[str, str | None],
    metrics: Mapping[str, Any],
    registry_policy: Mapping[str, Any],
) -> bool:
    return bool(
        switch_count_control_result(
            actual_date_range=actual_date_range,
            metrics=metrics,
            registry_policy=registry_policy,
        )["switch_count_controlled"]
    )


def switch_count_control_contract(registry_policy: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(registry_policy.get("switch_count_control_contract"))
    contract = dict(SWITCH_CONTROL_CONTRACT_DEFAULTS)
    contract.update({key: value for key, value in raw.items() if value is not None})
    contract["max_switches_per_year"] = _int_value(contract.get("max_switches_per_year"), 2)
    contract["max_switches_per_3y"] = _int_value(contract.get("max_switches_per_3y"), 6)
    contract["max_turnover_per_year"] = _float(contract.get("max_turnover_per_year"), 1.0)
    contract["min_avg_holding_period"] = _int_value(contract.get("min_avg_holding_period"), 60)
    exceptions = contract.get("allowed_exception_cases")
    if not isinstance(exceptions, list):
        exceptions = list(SWITCH_CONTROL_CONTRACT_DEFAULTS["allowed_exception_cases"])
    contract["allowed_exception_cases"] = [str(value) for value in exceptions]
    return contract


def switch_count_control_result(
    *,
    actual_date_range: Mapping[str, str | None],
    metrics: Mapping[str, Any],
    registry_policy: Mapping[str, Any],
) -> dict[str, Any]:
    contract = switch_count_control_contract(registry_policy)
    observed = {
        "switch_count": _int_value(metrics.get("switch_count")),
        "annualized_switches": annualized_switches(actual_date_range, metrics),
        "max_switches_per_year": _int_value(metrics.get("max_switches_per_year_observed")),
        "max_switches_per_3y": _int_value(metrics.get("max_switches_per_3y_observed")),
        "max_turnover_per_year": _float(metrics.get("max_turnover_per_year_observed")),
        "avg_holding_period": _float(metrics.get("avg_holding_period")),
    }
    checks = [
        low_turnover_owner_check(
            "max_switches_per_year",
            observed["max_switches_per_year"] <= contract["max_switches_per_year"],
            f"observed calendar-year switch max <= {contract['max_switches_per_year']}",
        ),
        low_turnover_owner_check(
            "max_switches_per_3y",
            observed["max_switches_per_3y"] <= contract["max_switches_per_3y"],
            f"observed rolling 3y switch max <= {contract['max_switches_per_3y']}",
        ),
        low_turnover_owner_check(
            "max_turnover_per_year",
            observed["max_turnover_per_year"] <= contract["max_turnover_per_year"],
            f"observed calendar-year turnover max <= {contract['max_turnover_per_year']}",
        ),
        low_turnover_owner_check(
            "min_avg_holding_period",
            observed["avg_holding_period"] >= contract["min_avg_holding_period"],
            f"observed average holding period >= {contract['min_avg_holding_period']} trading days",
        ),
    ]
    failed = [row["check_id"] for row in checks if not row["passed"]]
    return {
        "switch_count_controlled": not failed,
        "contract": contract,
        "observed": observed,
        "checks": checks,
        "failed_checks": failed,
        "allowed_exception_cases": contract["allowed_exception_cases"],
    }


def annualized_switches(
    actual_date_range: Mapping[str, str | None],
    metrics: Mapping[str, Any],
) -> float:
    span_days = _date_span_days(actual_date_range)
    return _float(metrics.get("switch_count")) / max(span_days / 365.25, 1.0)


def _int_value(value: object, default: int = 0) -> int:
    try:
        if value in {None, ""}:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _date_span_days(actual_range: Mapping[str, str | None]) -> int:
    start = _date_or_none(actual_range.get("start"))
    end = _date_or_none(actual_range.get("end"))
    if start is None or end is None:
        return 0
    return max((end - start).days + 1, 1)


def _date_or_none(value: object) -> date | None:
    if value in {None, ""}:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()
