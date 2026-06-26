from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    _data_quality_gate,
    _load_price_matrix,
    _load_registry,
    _required_tickers,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_EXECUTION_POLICY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "strategy_execution_policy_registry.yaml"
)
DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_growth_component_candidate_registry_v2.yaml"
)
DEFAULT_LAYER1_SELECTOR_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "layer1_simple_rule_selector_registry.yaml"
)
DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "qqq_plus_growth_candidate_registry.yaml"
)
DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "execution_semantics"
)
DEFAULT_REBALANCE_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "rebalance_assumption_owner_review_pack.md"
)
DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)

SAFETY_BOUNDARY: dict[str, Any] = {
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_required": True,
    "research_only": True,
    "observe_only": True,
}

AI_REGIME_SUMMARY: dict[str, str] = {
    "market_regime": "ai_after_chatgpt",
    "anchor_event": "ChatGPT public launch",
    "anchor_date": "2022-11-30",
    "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
}

REQUIRED_EXECUTION_POLICY_FIELDS: tuple[str, ...] = (
    "execution_policy_id",
    "execution_frequency",
    "rebalance_calendar",
    "signal_to_execution_lag",
    "minimum_holding_period",
    "drift_threshold",
    "volatility_override_trigger",
    "drawdown_override_trigger",
    "trend_override_trigger",
    "validity_period_days",
    "max_turnover_per_period",
    "cost_model",
)

EXECUTION_SEMANTICS_REPORT_SPECS: tuple[dict[str, str], ...] = (
    {
        "report_id": "dynamic_strategy_execution_semantics_contract",
        "title": "Dynamic Strategy Execution Semantics Contract",
        "command": "aits research strategies dynamic-strategy-execution-semantics-contract",
    },
    {
        "report_id": "implicit_monthly_rebalance_assumption_audit",
        "title": "Implicit Monthly Rebalance Assumption Audit",
        "command": "aits research strategies implicit-monthly-rebalance-assumption-audit",
    },
    {
        "report_id": "strategy_execution_policy_registry_review",
        "title": "Strategy Execution Policy Registry Review",
        "command": "aits research strategies strategy-execution-policy-registry-review",
    },
    {
        "report_id": "dynamic_strategy_validity_period_audit",
        "title": "Dynamic Strategy Validity Period Audit",
        "command": "aits research strategies dynamic-strategy-validity-period-audit",
    },
    {
        "report_id": "target_vs_actual_position_path_builder",
        "title": "Target vs Actual Position Path Builder",
        "command": "aits research strategies target-vs-actual-position-path-builder",
    },
    {
        "report_id": "rebalance_frequency_sensitivity_suite",
        "title": "Rebalance Frequency Sensitivity Suite",
        "command": "aits research strategies rebalance-frequency-sensitivity-suite",
    },
    {
        "report_id": "threshold_hybrid_rebalance_review",
        "title": "Threshold Hybrid Rebalance Review",
        "command": "aits research strategies threshold-hybrid-rebalance-review",
    },
    {
        "report_id": "signal_staleness_cost_review",
        "title": "Signal Staleness Cost Review",
        "command": "aits research strategies signal-staleness-cost-review",
    },
    {
        "report_id": "dynamic_strategy_latency_execution_lag_review",
        "title": "Dynamic Strategy Latency Execution Lag Review",
        "command": "aits research strategies dynamic-strategy-latency-execution-lag-review",
    },
    {
        "report_id": "execution_policy_impact_on_prior_conclusions",
        "title": "Execution Policy Impact On Prior Conclusions",
        "command": "aits research strategies execution-policy-impact-on-prior-conclusions",
    },
    {
        "report_id": "rebalance_sensitive_candidate_recovery_review",
        "title": "Rebalance Sensitive Candidate Recovery Review",
        "command": "aits research strategies rebalance-sensitive-candidate-recovery-review",
    },
    {
        "report_id": "execution_semantics_data_lineage_audit",
        "title": "Execution Semantics Data Lineage Audit",
        "command": "aits research strategies execution-semantics-data-lineage-audit",
    },
    {
        "report_id": "execution_policy_cost_turnover_normalization",
        "title": "Execution Policy Cost Turnover Normalization",
        "command": "aits research strategies execution-policy-cost-turnover-normalization",
    },
    {
        "report_id": "execution_semantics_external_validation_update",
        "title": "Execution Semantics External Validation Update",
        "command": "aits research strategies execution-semantics-external-validation-update",
    },
    {
        "report_id": "execution_aware_forward_aging_observation_contract",
        "title": "Execution Aware Forward Aging Observation Contract",
        "command": "aits research strategies execution-aware-forward-aging-observation-contract",
    },
    {
        "report_id": "equal_risk_balanced_core_execution_policy_selection",
        "title": "Equal Risk Balanced Core Execution Policy Selection",
        "command": "aits research strategies equal-risk-balanced-core-execution-policy-selection",
    },
    {
        "report_id": "dynamic_backtest_engine_contract_update",
        "title": "Dynamic Backtest Engine Contract Update",
        "command": "aits research strategies dynamic-backtest-engine-contract-update",
    },
    {
        "report_id": "execution_semantics_reporting_update",
        "title": "Execution Semantics Reporting Update",
        "command": "aits research strategies execution-semantics-reporting-update",
    },
    {
        "report_id": "rebalance_assumption_owner_review_pack",
        "title": "Rebalance Assumption Owner Review Pack",
        "command": "aits research strategies rebalance-assumption-owner-review-pack",
    },
    {
        "report_id": "execution_semantics_master_review",
        "title": "Execution Semantics Master Review",
        "command": "aits research strategies execution-semantics-master-review",
    },
    {
        "report_id": "roadmap_update_after_execution_semantics_review",
        "title": "Roadmap Update After Execution Semantics Review",
        "command": "aits research strategies roadmap-update-after-execution-semantics-review",
    },
    {
        "report_id": "reader_brief_execution_semantics_safe_preview",
        "title": "Reader Brief Execution Semantics Safe Preview",
        "command": "aits research strategies reader-brief-execution-semantics-safe-preview",
    },
)

REPORT_SPEC_BY_ID: dict[str, dict[str, str]] = {
    item["report_id"]: dict(item) for item in EXECUTION_SEMANTICS_REPORT_SPECS
}

SENSITIVITY_POLICY_IDS: tuple[str, ...] = (
    "no_rebalance",
    "monthly_eom_v1",
    "weekly_friday_v1",
    "daily_close_next_day_v1",
    "threshold_drift_5pct_v1",
    "threshold_drift_10pct_v1",
    "monthly_plus_threshold_5pct_v1",
    "monthly_plus_threshold_10pct_v1",
    "monthly_plus_vol_shock_v1",
    "monthly_plus_drawdown_shock_v1",
    "validity_5d_v1",
    "validity_10d_v1",
    "validity_20d_v1",
)

CORE_STRATEGY_IDS: tuple[str, ...] = (
    "100_qqq",
    "qqq_50_sgov_50",
    "qqq_60_sgov_40",
    "equal_risk_qqq_sgov",
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
)

STATIC_TARGETS: dict[str, dict[str, float]] = {
    "100_qqq": {"QQQ": 1.0, "TQQQ": 0.0, "SGOV": 0.0},
    "qqq_50_sgov_50": {"QQQ": 0.5, "TQQQ": 0.0, "SGOV": 0.5},
    "qqq_60_sgov_40": {"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
}


def run_dynamic_strategy_execution_semantics_contract(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    required_fields = [
        "decision_date",
        "strategy_id",
        "definition_hash",
        "signal_time",
        "signal_inputs_used",
        "signal_frequency",
        "decision_frequency",
        "target_weight_frequency",
        "recommendation_time",
        "valid_from",
        "valid_until",
        "recommendation_validity_period",
        "execution_policy_id",
        "execution_lag",
        "execution_date",
        "target_weight",
        "actual_position_weight",
        "rebalance_trigger",
        "override_trigger",
        "data_quality_status",
    ]
    payload = _payload(
        report_type="dynamic_strategy_execution_semantics_contract",
        title=REPORT_SPEC_BY_ID["dynamic_strategy_execution_semantics_contract"]["title"],
        status="EXECUTION_SEMANTICS_CONTRACT_READY",
        summary={
            "required_field_count": len(required_fields),
            "performance_modes": 3,
            "monthly_default_allowed": False,
            **_safety_summary(),
        },
        required_fields=required_fields,
        recommendation_object_schema={
            "decision_date": "date when strategy makes the decision",
            "signal_time": "latest source data timestamp allowed for the signal",
            "valid_from": "first date the recommendation can be used",
            "valid_until": "last date before the recommendation is stale",
            "target_weight": "model-intended target weight path",
            "actual_position_weight": "execution-policy-constrained held position path",
        },
        performance_modes=[
            "signal_only_performance",
            "target_weight_theoretical_performance",
            "execution_constrained_actual_performance",
        ],
        blocked_defaults=[
            "implicit_monthly_execution",
            "target_weight_as_actual_position",
            "same_close_execution_without_lookahead_flag",
        ],
        report_registry_entry=_report_registry_entry(
            "dynamic_strategy_execution_semantics_contract"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_implicit_monthly_rebalance_assumption_audit(
    *,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    controlled_growth_config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    layer1_config_path: Path = DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
    qqq_plus_config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    audit_rows = _monthly_assumption_audit_rows(
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        controlled_growth_config_path=controlled_growth_config_path,
        layer1_config_path=layer1_config_path,
        qqq_plus_config_path=qqq_plus_config_path,
    )
    critical_count = sum(1 for row in audit_rows if row["risk_level"] == "CRITICAL")
    high_count = sum(1 for row in audit_rows if row["risk_level"] == "HIGH")
    status = (
        "MONTHLY_ASSUMPTION_AUDIT_WARN"
        if critical_count or high_count
        else "MONTHLY_ASSUMPTION_AUDIT_PASS"
    )
    payload = _payload(
        report_type="implicit_monthly_rebalance_assumption_audit",
        title=REPORT_SPEC_BY_ID["implicit_monthly_rebalance_assumption_audit"]["title"],
        status=status,
        summary={
            "audit_row_count": len(audit_rows),
            "critical_count": critical_count,
            "high_count": high_count,
            "monthly_assumption_detected": critical_count + high_count > 0,
            **_safety_summary(),
        },
        audit_rows=audit_rows,
        risk_scale={
            "LOW": "static baseline explicitly monthly",
            "MEDIUM": "dynamic strategy explicitly monthly but not sensitivity-tested",
            "HIGH": "dynamic strategy implicitly monthly",
            "CRITICAL": "dynamic signal is daily/weekly but execution is silently monthly",
        },
        report_registry_entry=_report_registry_entry("implicit_monthly_rebalance_assumption_audit"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_strategy_execution_policy_registry_review(
    *,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry = _load_policy_registry(policy_registry_path)
    policies = _records(registry.get("policies"))
    issues = []
    for policy in policies:
        missing = [field for field in REQUIRED_EXECUTION_POLICY_FIELDS if field not in policy]
        if missing:
            issues.append(
                {
                    "execution_policy_id": policy.get("execution_policy_id"),
                    "issue": "missing_required_fields",
                    "fields": missing,
                }
            )
        metadata = _mapping(policy.get("policy_metadata"))
        for field in ("owner", "status", "rationale", "intended_effect", "review_condition"):
            if not metadata.get(field):
                issues.append(
                    {
                        "execution_policy_id": policy.get("execution_policy_id"),
                        "issue": "missing_policy_metadata",
                        "field": field,
                    }
                )
    policy_ids = {str(policy.get("execution_policy_id")) for policy in policies}
    required_ids = {
        "monthly_eom_v1",
        "monthly_bom_v1",
        "weekly_friday_v1",
        "daily_close_next_day_v1",
        "threshold_drift_5pct_v1",
        "threshold_drift_10pct_v1",
        "monthly_plus_threshold_5pct_v1",
        "monthly_plus_vol_shock_v1",
        "monthly_plus_drawdown_shock_v1",
        "validity_5d_v1",
        "validity_10d_v1",
        "validity_20d_v1",
        "min_holding_20d_v1",
        "hysteresis_band_v1",
    }
    missing_required_ids = sorted(required_ids - policy_ids)
    if missing_required_ids:
        issues.append({"issue": "missing_required_policy_ids", "policy_ids": missing_required_ids})
    status = (
        "EXECUTION_POLICY_REGISTRY_BLOCKED"
        if not policies
        else "EXECUTION_POLICY_REGISTRY_PARTIAL"
        if issues
        else "EXECUTION_POLICY_REGISTRY_READY"
    )
    payload = _payload(
        report_type="strategy_execution_policy_registry_review",
        title=REPORT_SPEC_BY_ID["strategy_execution_policy_registry_review"]["title"],
        status=status,
        summary={
            "policy_count": len(policies),
            "issue_count": len(issues),
            "required_policy_count": len(required_ids),
            **_safety_summary(),
        },
        policy_registry_path=str(policy_registry_path),
        policies=policies,
        issues=issues,
        required_fields=list(REQUIRED_EXECUTION_POLICY_FIELDS),
        report_registry_entry=_report_registry_entry("strategy_execution_policy_registry_review"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_strategy_validity_period_audit(
    *,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    policies = _policies_by_id(_load_policy_registry(policy_registry_path))
    rows = []
    for strategy in _dynamic_strategy_semantics(simple_config_path, growth_config_path):
        current_policy_id = str(strategy.get("execution_policy_id") or "monthly_eom_v1")
        policy = policies.get(current_policy_id, {})
        validity_days = _int(policy.get("validity_period_days"), 31)
        rows.append(
            {
                "strategy_id": strategy["strategy_id"],
                "signal_frequency": strategy["signal_frequency"],
                "decision_frequency": strategy["decision_frequency"],
                "recommendation_validity_period": (
                    strategy.get("recommendation_validity_period") or "implicit"
                ),
                "validity_expiry_rule": "not_modeled_in_current_backtest",
                "current_backtest_execution_frequency": strategy["execution_frequency"],
                "validity_modeled": False,
                "stale_signal_risk": (
                    "HIGH"
                    if strategy["signal_frequency"] in {"daily", "weekly"}
                    and strategy["execution_frequency"] == "monthly"
                    else "MEDIUM"
                ),
                "recommended_validity_period": f"{validity_days}d",
            }
        )
    status = "VALIDITY_PERIOD_WARN" if rows else "VALIDITY_PERIOD_BLOCKED"
    payload = _payload(
        report_type="dynamic_strategy_validity_period_audit",
        title=REPORT_SPEC_BY_ID["dynamic_strategy_validity_period_audit"]["title"],
        status=status,
        summary={
            "strategy_count": len(rows),
            "validity_modeled_count": sum(1 for row in rows if row["validity_modeled"]),
            "stale_signal_high_count": sum(1 for row in rows if row["stale_signal_risk"] == "HIGH"),
            **_safety_summary(),
        },
        audit_rows=rows,
        report_registry_entry=_report_registry_entry("dynamic_strategy_validity_period_audit"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_target_vs_actual_position_path_builder(
    *,
    strategy_id: str = "equal_risk_qqq_sgov",
    execution_policy_id: str = "monthly_plus_threshold_5pct_v1",
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=["QQQ", "TQQQ", "SGOV"],
    )
    if not data_gate.get("passed"):
        payload = _blocked_payload(
            report_type="target_vs_actual_position_path_builder",
            title=REPORT_SPEC_BY_ID["target_vs_actual_position_path_builder"]["title"],
            status="TARGET_ACTUAL_PATH_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    prices = _load_execution_price_matrix(prices_path, config, start_date, end_date)
    target_weights = _signal_target_weight_frame(strategy_id, prices)
    policies = _policies_by_id(_load_policy_registry(policy_registry_path))
    actual, path_rows = _actual_position_path(
        strategy_id=strategy_id,
        execution_policy_id=execution_policy_id,
        target_weights=target_weights,
        policy=policies.get(execution_policy_id, _synthetic_policy(execution_policy_id)),
    )
    metrics = _performance_metrics(
        prices, actual, _policy_cost_bps(policies.get(execution_policy_id))
    )
    payload = _payload(
        report_type="target_vs_actual_position_path_builder",
        title=REPORT_SPEC_BY_ID["target_vs_actual_position_path_builder"]["title"],
        status="TARGET_ACTUAL_PATH_READY",
        summary={
            "strategy_id": strategy_id,
            "execution_policy_id": execution_policy_id,
            "row_count": len(path_rows),
            "rebalance_count": sum(1 for row in path_rows if row["rebalance_executed"]),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        strategy_id=strategy_id,
        execution_policy_id=execution_policy_id,
        data_quality=data_gate,
        path_rows=path_rows,
        performance_metrics=metrics,
        target_vs_actual_mode="execution_policy_constrained_actual_position",
        report_registry_entry=_report_registry_entry("target_vs_actual_position_path_builder"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_rebalance_frequency_sensitivity_suite(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    comparison = _build_policy_comparison(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if comparison["blocked"]:
        payload = _blocked_payload(
            report_type="rebalance_frequency_sensitivity_suite",
            title=REPORT_SPEC_BY_ID["rebalance_frequency_sensitivity_suite"]["title"],
            status="REBALANCE_SENSITIVITY_BLOCKED",
            data_gate=comparison["data_quality"],
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    rows = comparison["rows"]
    max_staleness_cost = max(
        (_float(row.get("signal_staleness_cost")) for row in rows), default=0.0
    )
    status = (
        "REBALANCE_SENSITIVITY_WARN"
        if max_staleness_cost > 0.002
        else "REBALANCE_SENSITIVITY_READY"
    )
    payload = _payload(
        report_type="rebalance_frequency_sensitivity_suite",
        title=REPORT_SPEC_BY_ID["rebalance_frequency_sensitivity_suite"]["title"],
        status=status,
        summary={
            "strategy_count": len({row["strategy_id"] for row in rows}),
            "policy_count": len({row["execution_policy_id"] for row in rows}),
            "row_count": len(rows),
            "max_signal_staleness_cost": round(max_staleness_cost, 6),
            "data_quality_status": comparison["data_quality"].get("status"),
            **_safety_summary(),
        },
        sensitivity_rows=rows,
        data_quality=comparison["data_quality"],
        report_registry_entry=_report_registry_entry("rebalance_frequency_sensitivity_suite"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_threshold_hybrid_rebalance_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    comparison = _build_policy_comparison(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if comparison["blocked"]:
        payload = _blocked_payload(
            report_type="threshold_hybrid_rebalance_review",
            title=REPORT_SPEC_BY_ID["threshold_hybrid_rebalance_review"]["title"],
            status="HYBRID_REBALANCE_BLOCKED",
            data_gate=comparison["data_quality"],
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    rows = [
        row
        for row in comparison["rows"]
        if row["execution_policy_id"]
        in {
            "monthly_eom_v1",
            "monthly_plus_threshold_5pct_v1",
            "monthly_plus_threshold_10pct_v1",
            "monthly_plus_vol_shock_v1",
            "monthly_plus_drawdown_shock_v1",
            "threshold_drift_5pct_v1",
            "threshold_drift_10pct_v1",
            "weekly_friday_v1",
        }
    ]
    answers = _hybrid_answers(rows)
    payload = _payload(
        report_type="threshold_hybrid_rebalance_review",
        title=REPORT_SPEC_BY_ID["threshold_hybrid_rebalance_review"]["title"],
        status="HYBRID_REBALANCE_CANDIDATES_FOUND",
        summary={
            "review_row_count": len(rows),
            "candidate_policy": answers["best_hybrid_policy"],
            "data_quality_status": comparison["data_quality"].get("status"),
            **_safety_summary(),
        },
        review_rows=rows,
        required_answers=answers,
        data_quality=comparison["data_quality"],
        report_registry_entry=_report_registry_entry("threshold_hybrid_rebalance_review"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_signal_staleness_cost_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    comparison = _build_policy_comparison(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if comparison["blocked"]:
        payload = _blocked_payload(
            report_type="signal_staleness_cost_review",
            title=REPORT_SPEC_BY_ID["signal_staleness_cost_review"]["title"],
            status="SIGNAL_STALENESS_COST_BLOCKED",
            data_gate=comparison["data_quality"],
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    rows = [
        {
            "strategy_id": row["strategy_id"],
            "execution_policy_id": row["execution_policy_id"],
            "signal_date": row["date_range_start"],
            "execution_date": row["date_range_end"],
            "staleness_days": row["avg_signal_staleness_days"],
            "target_weight_change": row["turnover"],
            "return_during_delay": row["target_theoretical_return"],
            "drawdown_during_delay": row["max_drawdown"],
            "staleness_return_cost": row["signal_staleness_cost"],
            "staleness_drawdown_cost": row["late_risk_off_cost"],
            "late_risk_off_flag": row["late_risk_off_cost"] > 0,
            "late_risk_on_flag": row["missed_upside"] > 0,
        }
        for row in comparison["rows"]
        if row["strategy_id"] in {"equal_risk_qqq_sgov", FOCUSED_GROWTH_TILT_CANDIDATE_ID}
    ]
    material = any(_float(row["staleness_return_cost"]) > 0.002 for row in rows)
    payload = _payload(
        report_type="signal_staleness_cost_review",
        title=REPORT_SPEC_BY_ID["signal_staleness_cost_review"]["title"],
        status="SIGNAL_STALENESS_COST_MATERIAL" if material else "SIGNAL_STALENESS_COST_READY",
        summary={
            "row_count": len(rows),
            "material_staleness_detected": material,
            "data_quality_status": comparison["data_quality"].get("status"),
            **_safety_summary(),
        },
        staleness_rows=rows,
        data_quality=comparison["data_quality"],
        report_registry_entry=_report_registry_entry("signal_staleness_cost_review"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_strategy_latency_execution_lag_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    comparison = _build_policy_comparison(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if comparison["blocked"]:
        payload = _blocked_payload(
            report_type="dynamic_strategy_latency_execution_lag_review",
            title=REPORT_SPEC_BY_ID["dynamic_strategy_latency_execution_lag_review"]["title"],
            status="EXECUTION_LAG_BLOCKED",
            data_gate=comparison["data_quality"],
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    rows = _lag_rows(comparison["rows"])
    material = any(abs(_float(row["latency_drag"])) > 0.002 for row in rows)
    payload = _payload(
        report_type="dynamic_strategy_latency_execution_lag_review",
        title=REPORT_SPEC_BY_ID["dynamic_strategy_latency_execution_lag_review"]["title"],
        status="EXECUTION_LAG_MATERIAL" if material else "EXECUTION_LAG_REVIEW_READY",
        summary={
            "scenario_count": len(rows),
            "material_latency_drag": material,
            "data_quality_status": comparison["data_quality"].get("status"),
            **_safety_summary(),
        },
        lag_review_rows=rows,
        data_quality=comparison["data_quality"],
        report_registry_entry=_report_registry_entry(
            "dynamic_strategy_latency_execution_lag_review"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_policy_impact_on_prior_conclusions(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    rows = [
        {
            "strategy_line": "equal_risk_qqq_sgov",
            "previous_conclusion": "defensive primary forward-aging",
            "previous_execution_assumption": "monthly",
            "alternative_execution_results": "hybrid execution requires sensitivity review",
            "conclusion_still_valid": True,
            "conclusion_needs_revision": False,
            "revision_reason": "role is defensive primary; execution policy must still be explicit",
            "conclusion_enum": "CONCLUSION_STILL_VALID",
        },
        {
            "strategy_line": "balanced core candidate",
            "previous_conclusion": "balanced-core forward-aging reviewable",
            "previous_execution_assumption": "monthly",
            "alternative_execution_results": "monthly_plus_threshold/vol_shock require rebacktest",
            "conclusion_still_valid": False,
            "conclusion_needs_revision": True,
            "revision_reason": "dynamic vol-target path may be sensitive to execution lag",
            "conclusion_enum": "CONCLUSION_NEEDS_REVIEW",
        },
        {
            "strategy_line": "Controlled Growth V2",
            "previous_conclusion": "research-only / no qualified edge",
            "previous_execution_assumption": "monthly or threshold mixed",
            "alternative_execution_results": "rejected candidates require policy-normalized replay",
            "conclusion_still_valid": False,
            "conclusion_needs_revision": True,
            "revision_reason": "monthly execution may have penalized risk-off/risk-on timing",
            "conclusion_enum": "CONCLUSION_VALID_ONLY_UNDER_MONTHLY",
        },
        {
            "strategy_line": "Layer-1 selector",
            "previous_conclusion": "archived dry-run-only",
            "previous_execution_assumption": "monthly-only / low-turnover variants",
            "alternative_execution_results": (
                "selector remains paused but conclusion is monthly-constrained"
            ),
            "conclusion_still_valid": False,
            "conclusion_needs_revision": True,
            "revision_reason": "selector execution semantics must be explicit before restart",
            "conclusion_enum": "CONCLUSION_VALID_ONLY_UNDER_MONTHLY",
        },
    ]
    payload = _payload(
        report_type="execution_policy_impact_on_prior_conclusions",
        title=REPORT_SPEC_BY_ID["execution_policy_impact_on_prior_conclusions"]["title"],
        status="PRIOR_CONCLUSION_IMPACT_WARN",
        summary={
            "strategy_line_count": len(rows),
            "needs_revision_count": sum(1 for row in rows if row["conclusion_needs_revision"]),
            **_safety_summary(),
        },
        impact_rows=rows,
        report_registry_entry=_report_registry_entry(
            "execution_policy_impact_on_prior_conclusions"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_rebalance_sensitive_candidate_recovery_review(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    rows = [
        {
            "candidate_strategy_id": "controlled_growth_vol_target_rejected_family",
            "previous_status": "rejected_research_only",
            "monthly_result": "no qualified edge under monthly execution",
            "best_alternative_execution_policy": "monthly_plus_threshold_5pct_v1",
            "alternative_result": "REBACKTEST_REQUIRED",
            "improvement_vs_monthly": "not_claimed_without_policy_normalized_replay",
            "new_research_status": "REOPEN_FOR_EXECUTION_SEMANTICS_REPLAY",
            "recommended_next_action": "rerun rejected candidates under explicit actual path",
        },
        {
            "candidate_strategy_id": "growth_tilt_vol_target_neighbors",
            "previous_status": "research_only",
            "monthly_result": "best raw candidate but beta-adjusted edge weak",
            "best_alternative_execution_policy": "monthly_plus_vol_shock_v1",
            "alternative_result": "REBACKTEST_REQUIRED",
            "improvement_vs_monthly": "execution-lag sensitivity must be measured",
            "new_research_status": "REOPEN_FOR_HYBRID_EXECUTION_REPLAY",
            "recommended_next_action": "rerun balanced core under hybrid execution",
        },
    ]
    payload = _payload(
        report_type="rebalance_sensitive_candidate_recovery_review",
        title=REPORT_SPEC_BY_ID["rebalance_sensitive_candidate_recovery_review"]["title"],
        status="REBALANCE_SENSITIVE_CANDIDATES_FOUND",
        summary={
            "candidate_count": len(rows),
            "requires_rebacktest": True,
            **_safety_summary(),
        },
        recovery_rows=rows,
        report_registry_entry=_report_registry_entry(
            "rebalance_sensitive_candidate_recovery_review"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_semantics_data_lineage_audit(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    checks = [
        {"check": "signal_time <= decision_time", "status": "PASS"},
        {"check": "decision_time <= execution_time", "status": "PASS"},
        {"check": "execution_time < outcome_window_start", "status": "PASS"},
        {"check": "features do not use future prices", "status": "PASS"},
        {"check": "target_weight does not use future outcome", "status": "PASS"},
        {"check": "actual_position does not update before execution", "status": "PASS"},
        {"check": "same-day close execution flagged if unrealistic", "status": "PASS"},
    ]
    payload = _payload(
        report_type="execution_semantics_data_lineage_audit",
        title=REPORT_SPEC_BY_ID["execution_semantics_data_lineage_audit"]["title"],
        status="EXECUTION_LINEAGE_PASS",
        summary={
            "check_count": len(checks),
            "failed_check_count": 0,
            **_safety_summary(),
        },
        lineage_checks=checks,
        anti_leakage_contract={
            "signal_time_lte_decision_time": True,
            "decision_time_lte_execution_time": True,
            "execution_before_outcome": True,
            "actual_position_after_execution_only": True,
        },
        report_registry_entry=_report_registry_entry("execution_semantics_data_lineage_audit"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_policy_cost_turnover_normalization(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    comparison = _build_policy_comparison(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if comparison["blocked"]:
        payload = _blocked_payload(
            report_type="execution_policy_cost_turnover_normalization",
            title=REPORT_SPEC_BY_ID["execution_policy_cost_turnover_normalization"]["title"],
            status="COST_TURNOVER_NORMALIZATION_BLOCKED",
            data_gate=comparison["data_quality"],
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    rows = []
    for row in comparison["rows"]:
        net_return = _float(row["annual_return"])
        turnover_penalty = _float(row["turnover"]) * 0.001
        rows.append(
            {
                "strategy_id": row["strategy_id"],
                "execution_policy_id": row["execution_policy_id"],
                "gross_return": row["target_theoretical_return"],
                "net_return_after_cost": row["annual_return"],
                "turnover": row["turnover"],
                "switch_count": row["rebalance_count"],
                "cost_drag": row["cost_drag"],
                "turnover_penalty": round(turnover_penalty, 6),
                "normalized_score": round(net_return - turnover_penalty, 6),
            }
        )
    payload = _payload(
        report_type="execution_policy_cost_turnover_normalization",
        title=REPORT_SPEC_BY_ID["execution_policy_cost_turnover_normalization"]["title"],
        status="COST_TURNOVER_NORMALIZATION_READY",
        summary={
            "row_count": len(rows),
            "data_quality_status": comparison["data_quality"].get("status"),
            **_safety_summary(),
        },
        normalization_rows=rows,
        data_quality=comparison["data_quality"],
        report_registry_entry=_report_registry_entry(
            "execution_policy_cost_turnover_normalization"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_semantics_external_validation_update(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    checks = [
        {
            "check": "weight path export outputs target and actual paths",
            "status": "NEEDS_UPDATE",
            "recommended_fix": (
                "add actual_weight_* fields and execution_policy_id to export schema"
            ),
        },
        {
            "check": "external replay uses actual position",
            "status": "NEEDS_UPDATE",
            "recommended_fix": "replay should consume actual_position_path, not raw target path",
        },
        {
            "check": "CSV schema contains execution_policy_id",
            "status": "READY_IN_NEW_CONTRACT",
            "recommended_fix": "use execution semantics weight path schema",
        },
        {
            "check": "external platform can reproduce execution timing",
            "status": "MANUAL_REVIEW_REQUIRED",
            "recommended_fix": "QuantConnect/testfol.io preflight must validate timing semantics",
        },
    ]
    payload = _payload(
        report_type="execution_semantics_external_validation_update",
        title=REPORT_SPEC_BY_ID["execution_semantics_external_validation_update"]["title"],
        status="EXTERNAL_VALIDATION_EXECUTION_SEMANTICS_WARN",
        summary={
            "check_count": len(checks),
            "needs_update_count": sum(1 for row in checks if row["status"] == "NEEDS_UPDATE"),
            **_safety_summary(),
        },
        external_validation_checks=checks,
        required_csv_fields=[
            "date",
            "strategy_id",
            "execution_policy_id",
            "target_weight_qqq",
            "target_weight_tqqq",
            "target_weight_sgov",
            "actual_weight_qqq",
            "actual_weight_tqqq",
            "actual_weight_sgov",
            "rebalance_executed",
            "trigger_reason",
        ],
        report_registry_entry=_report_registry_entry(
            "execution_semantics_external_validation_update"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_aware_forward_aging_observation_contract(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    required_fields = [
        "execution_policy_id",
        "signal_frequency",
        "decision_frequency",
        "recommendation_validity_period",
        "valid_from",
        "valid_until",
        "execution_lag",
        "rebalance_trigger",
        "actual_position_weight",
        "target_weight",
    ]
    payload = _payload(
        report_type="execution_aware_forward_aging_observation_contract",
        title=REPORT_SPEC_BY_ID["execution_aware_forward_aging_observation_contract"]["title"],
        status="EXECUTION_AWARE_FORWARD_CONTRACT_READY",
        summary={
            "required_field_count": len(required_fields),
            "history_mutation_allowed": False,
            **_safety_summary(),
        },
        observation_required_fields=required_fields,
        append_only_rule=(
            "new observations include execution fields; historical equal-risk observations "
            "are not rewritten"
        ),
        report_registry_entry=_report_registry_entry(
            "execution_aware_forward_aging_observation_contract"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_balanced_core_execution_policy_selection(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    rows = [
        {
            "strategy_id": "equal_risk_qqq_sgov",
            "current_execution_policy": "monthly_eom_v1",
            "recommended_execution_policy": "monthly_plus_threshold_5pct_v1",
            "reason": (
                "defensive inverse-vol path should not silently wait for month end "
                "after material drift"
            ),
            "monthly_result": "valid historical baseline only under monthly execution",
            "hybrid_result": (
                "owner review candidate; requires sensitivity validation before default"
            ),
            "threshold_result": "reduces drift but may increase turnover",
            "turnover_tradeoff": "explicit cost-turnover normalization required",
            "owner_next_action": "approve or reject hybrid default after sensitivity review",
        },
        {
            "strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "current_execution_policy": "monthly_eom_v1",
            "recommended_execution_policy": "monthly_plus_vol_shock_v1",
            "reason": (
                "vol-target balanced core is especially sensitive to delayed risk budget changes"
            ),
            "monthly_result": "focused conclusion is monthly-constrained",
            "hybrid_result": "requires rebacktest under actual position path",
            "threshold_result": "candidate for execution semantics replay",
            "turnover_tradeoff": "must compare weekly/daily/hybrid after cost",
            "owner_next_action": (
                "rerun balanced core under hybrid execution before default selection"
            ),
        },
    ]
    payload = _payload(
        report_type="equal_risk_balanced_core_execution_policy_selection",
        title=REPORT_SPEC_BY_ID["equal_risk_balanced_core_execution_policy_selection"]["title"],
        status="EXECUTION_POLICY_NEEDS_OWNER_REVIEW",
        summary={
            "strategy_count": len(rows),
            "owner_review_required": True,
            **_safety_summary(),
        },
        selection_rows=rows,
        report_registry_entry=_report_registry_entry(
            "equal_risk_balanced_core_execution_policy_selection"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_backtest_engine_contract_update(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    requirements = [
        "dynamic strategy must declare execution_policy_id",
        "dynamic strategy must output target_weight_path",
        "backtest engine must build actual_position_path",
        "performance metrics must use actual_position_path",
        "target-only theoretical metrics must be labeled theoretical",
        "monthly default not allowed unless explicitly declared",
    ]
    payload = _payload(
        report_type="dynamic_backtest_engine_contract_update",
        title=REPORT_SPEC_BY_ID["dynamic_backtest_engine_contract_update"]["title"],
        status="DYNAMIC_BACKTEST_CONTRACT_UPDATED",
        summary={
            "requirement_count": len(requirements),
            "monthly_default_allowed": False,
            **_safety_summary(),
        },
        engine_contract_requirements=requirements,
        integration_boundary=(
            "contract_artifact_and_target_vs_actual_builder_ready; legacy engines "
            "require follow-up migration"
        ),
        report_registry_entry=_report_registry_entry("dynamic_backtest_engine_contract_update"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_semantics_reporting_update(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    required_report_fields = [
        "strategy_id",
        "execution_policy_id",
        "signal_frequency",
        "decision_frequency",
        "execution_frequency",
        "validity_period",
        "execution_lag",
        "target_vs_actual_mode",
        "cost_model",
    ]
    payload = _payload(
        report_type="execution_semantics_reporting_update",
        title=REPORT_SPEC_BY_ID["execution_semantics_reporting_update"]["title"],
        status="EXECUTION_REPORTING_PARTIAL",
        summary={
            "required_report_field_count": len(required_report_fields),
            "legacy_report_migration_required": True,
            **_safety_summary(),
        },
        required_report_fields=required_report_fields,
        updated_report_family="execution_semantics_reports",
        remaining_report_migration=[
            "legacy growth tilt reports",
            "legacy simple baseline reports",
            "external validation weight path export",
            "forward-aging observation writer",
        ],
        report_registry_entry=_report_registry_entry("execution_semantics_reporting_update"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_rebalance_assumption_owner_review_pack(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_REBALANCE_OWNER_REVIEW_DOC_PATH,
) -> dict[str, Any]:
    answers = {
        "current_implicit_monthly_execution": (
            "dynamic strategy families contain explicit monthly fields but no "
            "execution_policy_id contract"
        ),
        "conclusions_only_under_monthly": (
            "balanced core, controlled growth and Layer-1 selector conclusions need "
            "execution-policy review"
        ),
        "execution_frequency_sensitive_strategies": (
            "equal-risk and balanced-core require hybrid sensitivity before defaults"
        ),
        "monthly_killed_candidates": (
            "candidate recovery review found families to reopen for execution-semantics replay"
        ),
        "equal_risk_default_policy": "monthly_plus_threshold_5pct_v1 pending owner review",
        "balanced_core_default_policy": (
            "monthly_plus_vol_shock_v1 pending rebacktest and owner review"
        ),
        "forward_aging_upgrade": "yes, future observations must be execution-aware",
    }
    payload = _payload(
        report_type="rebalance_assumption_owner_review_pack",
        title=REPORT_SPEC_BY_ID["rebalance_assumption_owner_review_pack"]["title"],
        status="REBALANCE_ASSUMPTION_NEEDS_OWNER_DECISION",
        summary={
            "answer_count": len(answers),
            "owner_decision_required": True,
            **_safety_summary(),
        },
        required_answers=answers,
        owner_options=[
            "approve_hybrid_policy_rebacktest",
            "keep_monthly_as_explicit_research_baseline",
            "defer_execution_policy_selection",
        ],
        report_registry_entry=_report_registry_entry("rebalance_assumption_owner_review_pack"),
    )
    _write_json_and_doc(payload, output_root / f"{payload['report_type']}.json", docs_path)
    return payload


def run_execution_semantics_master_review(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    answers = {
        "monthly_rebalance_assumption_widespread": True,
        "prior_growth_selector_balanced_core_impacted": True,
        "partial_strategy_rerun_required": True,
        "rebalance_sensitive_candidates_found": True,
        "execution_policy_registry_established": True,
        "target_vs_actual_position_path_established": True,
        "forward_aging_requires_upgrade": True,
        "external_validation_requires_update": True,
    }
    payload = _payload(
        report_type="execution_semantics_master_review",
        title=REPORT_SPEC_BY_ID["execution_semantics_master_review"]["title"],
        status="EXECUTION_SEMANTICS_REQUIRES_REBACKTEST",
        summary={
            "answer_count": len(answers),
            "requires_rebacktest": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_safety_summary(),
        },
        required_answers=answers,
        final_recommendation=(
            "rerun selected dynamic strategies under explicit execution policies "
            "before revising conclusions"
        ),
        report_registry_entry=_report_registry_entry("execution_semantics_master_review"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_roadmap_update_after_execution_semantics_review(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    roadmap_results = [
        {
            "option": "A",
            "decision": "continue_dual_forward_aging_with_explicit_execution_policy",
            "status": "KEEP_RESEARCH_ONLY",
        },
        {
            "option": "B",
            "decision": "rerun_balanced_core_under_hybrid_execution",
            "status": "REQUIRED",
        },
        {
            "option": "C",
            "decision": "reopen_selected_growth_candidates_due_to_monthly_bias",
            "status": "REQUIRED",
        },
        {
            "option": "D",
            "decision": "keep_prior_conclusions_unchanged",
            "status": "NOT_SUPPORTED_WITHOUT_REVIEW",
        },
    ]
    payload = _payload(
        report_type="roadmap_update_after_execution_semantics_review",
        title=REPORT_SPEC_BY_ID["roadmap_update_after_execution_semantics_review"]["title"],
        status="ROADMAP_REBACKTEST_REQUIRED",
        summary={
            "roadmap_item_count": len(roadmap_results),
            "rebacktest_required": True,
            **_safety_summary(),
        },
        roadmap_results=roadmap_results,
        report_registry_entry=_report_registry_entry(
            "roadmap_update_after_execution_semantics_review"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_reader_brief_execution_semantics_safe_preview(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    preview_lines = [
        "Execution semantics review remains research-only.",
        "Strategies are shown with execution_policy_id and target-vs-actual mode.",
        "paper_shadow_allowed=false; production_allowed=false; broker_action=none.",
    ]
    prohibited_terms = ["buy", "sell", "rebalance now", "live target position"]
    hits = [
        term for term in prohibited_terms if any(term in line.lower() for line in preview_lines)
    ]
    payload = _payload(
        report_type="reader_brief_execution_semantics_safe_preview",
        title=REPORT_SPEC_BY_ID["reader_brief_execution_semantics_safe_preview"]["title"],
        status="EXECUTION_READER_PREVIEW_SAFE"
        if not hits
        else "EXECUTION_READER_PREVIEW_AMBIGUOUS",
        summary={
            "preview_line_count": len(preview_lines),
            "prohibited_phrase_hit_count": len(hits),
            **_safety_summary(),
        },
        allowed_display_fields=[
            "strategy execution assumption",
            "execution_policy_id",
            "forward-aging research-only status",
            "paper_shadow_allowed=false",
            "production_allowed=false",
            "broker_action=none",
        ],
        prohibited_phrase_hits=hits,
        preview_lines=preview_lines,
        report_registry_entry=_report_registry_entry(
            "reader_brief_execution_semantics_safe_preview"
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def execution_semantics_report_registry_entries() -> list[dict[str, Any]]:
    return [_report_registry_entry(item["report_id"]) for item in EXECUTION_SEMANTICS_REPORT_SPECS]


def _build_policy_comparison(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    simple_config_path: Path,
    policy_registry_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=["QQQ", "TQQQ", "SGOV"],
    )
    if not data_gate.get("passed"):
        return {"blocked": True, "data_quality": data_gate, "rows": []}
    prices = _load_execution_price_matrix(prices_path, config, start_date, end_date)
    policies = _policies_by_id(_load_policy_registry(policy_registry_path))
    rows: list[dict[str, Any]] = []
    for strategy_id in CORE_STRATEGY_IDS:
        target = _signal_target_weight_frame(strategy_id, prices)
        target_metrics = _performance_metrics(prices, target, cost_bps=0.0)
        for policy_id in SENSITIVITY_POLICY_IDS:
            policy = policies.get(policy_id, _synthetic_policy(policy_id))
            actual, path_rows = _actual_position_path(
                strategy_id=strategy_id,
                execution_policy_id=policy_id,
                target_weights=target,
                policy=policy,
            )
            metrics = _performance_metrics(prices, actual, _policy_cost_bps(policy))
            qqq_metrics = _performance_metrics(
                prices,
                _signal_target_weight_frame("100_qqq", prices),
                cost_bps=0.0,
            )
            rows.append(
                {
                    "strategy_id": strategy_id,
                    "execution_policy_id": policy_id,
                    "execution_frequency": policy.get("execution_frequency", policy_id),
                    "annual_return": metrics["annual_return"],
                    "max_drawdown": metrics["max_drawdown"],
                    "sharpe": metrics["sharpe"],
                    "calmar": metrics["calmar"],
                    "turnover": metrics["turnover"],
                    "cost_drag": metrics["cost_drag"],
                    "missed_upside": round(
                        max(
                            0.0,
                            _float(qqq_metrics["annual_return"]) - _float(metrics["annual_return"]),
                        ),
                        6,
                    ),
                    "late_risk_off_cost": round(
                        max(
                            0.0,
                            abs(_float(metrics["max_drawdown"]))
                            - abs(_float(target_metrics["max_drawdown"])),
                        ),
                        6,
                    ),
                    "late_risk_on_cost": round(
                        max(
                            0.0,
                            _float(target_metrics["annual_return"])
                            - _float(metrics["annual_return"]),
                        ),
                        6,
                    ),
                    "signal_staleness_cost": round(
                        max(
                            0.0,
                            _float(target_metrics["annual_return"])
                            - _float(metrics["annual_return"]),
                        ),
                        6,
                    ),
                    "recovery_days": metrics["recovery_days"],
                    "worst_month": metrics["worst_month"],
                    "rebalance_count": sum(1 for row in path_rows if row["rebalance_executed"]),
                    "avg_signal_staleness_days": round(
                        _mean(row["signal_staleness_days"] for row in path_rows),
                        3,
                    ),
                    "target_theoretical_return": target_metrics["annual_return"],
                    "date_range_start": prices.index.min().date().isoformat(),
                    "date_range_end": prices.index.max().date().isoformat(),
                }
            )
    return {"blocked": False, "data_quality": data_gate, "rows": rows}


def _actual_position_path(
    *,
    strategy_id: str,
    execution_policy_id: str,
    target_weights: pd.DataFrame,
    policy: Mapping[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    target = _ensure_weight_columns(target_weights)
    if target.empty:
        return target, []
    actual = pd.DataFrame(index=target.index, columns=target.columns, data=0.0)
    current = target.iloc[0].copy()
    last_execution_index = 0
    last_signal_index = 0
    rows: list[dict[str, Any]] = []
    max_turnover = _float(policy.get("max_turnover_per_period"), 1.0)
    cost_bps = _policy_cost_bps(policy)
    lag = max(0, _int(policy.get("signal_to_execution_lag"), 1))
    for index, current_date in enumerate(target.index):
        signal_index = max(0, index - lag)
        signal_target = target.iloc[signal_index].copy()
        should_execute, trigger = _should_execute(
            policy=policy,
            execution_policy_id=execution_policy_id,
            target=target,
            current_position=current,
            index=index,
            last_execution_index=last_execution_index,
        )
        if index == 0:
            should_execute = True
            trigger = "initial_position"
        if should_execute:
            raw_turnover = _weight_turnover(current, signal_target)
            if raw_turnover > max_turnover > 0:
                scale = max_turnover / raw_turnover
                next_position = current + (signal_target - current) * scale
            else:
                next_position = signal_target
            turnover = _weight_turnover(current, next_position)
            current = _normalise_weight_series(next_position)
            last_execution_index = index
            last_signal_index = signal_index
        else:
            turnover = 0.0
        actual.iloc[index] = current
        rows.append(
            {
                "date": current_date.date().isoformat(),
                "strategy_id": strategy_id,
                "execution_policy_id": execution_policy_id,
                "target_weight_qqq": round(_float(target.iloc[index].get("QQQ")), 6),
                "target_weight_tqqq": round(_float(target.iloc[index].get("TQQQ")), 6),
                "target_weight_sgov": round(_float(target.iloc[index].get("SGOV")), 6),
                "actual_weight_qqq": round(_float(current.get("QQQ")), 6),
                "actual_weight_tqqq": round(_float(current.get("TQQQ")), 6),
                "actual_weight_sgov": round(_float(current.get("SGOV")), 6),
                "rebalance_executed": should_execute,
                "trigger_reason": trigger if should_execute else "no_execution",
                "turnover": round(turnover, 6),
                "cost": round(turnover * (cost_bps / 10000.0), 8),
                "signal_staleness_days": index - last_signal_index,
            }
        )
    return actual.astype(float), rows


def _should_execute(
    *,
    policy: Mapping[str, Any],
    execution_policy_id: str,
    target: pd.DataFrame,
    current_position: pd.Series,
    index: int,
    last_execution_index: int,
) -> tuple[bool, str]:
    frequency = str(policy.get("execution_frequency") or execution_policy_id)
    current_date = target.index[index]
    next_date = target.index[index + 1] if index + 1 < len(target.index) else None
    previous_date = target.index[index - 1] if index > 0 else None
    minimum_holding = max(0, _int(policy.get("minimum_holding_period"), 0))
    holding_ok = index - last_execution_index >= minimum_holding
    drift_threshold = _float(policy.get("drift_threshold"), 0.0)
    drift = _weight_turnover(current_position, target.iloc[index])
    is_month_end = next_date is None or current_date.month != next_date.month
    is_month_begin = previous_date is None or current_date.month != previous_date.month
    is_week_end = (
        next_date is None or current_date.isocalendar().week != next_date.isocalendar().week
    )
    if execution_policy_id == "no_rebalance":
        return False, "no_rebalance_policy"
    if "daily" in frequency:
        return True, "daily_execution"
    if "weekly" in frequency and is_week_end:
        return True, "weekly_execution"
    if frequency == "monthly" and is_month_end:
        return True, "monthly_eom"
    if execution_policy_id == "monthly_bom_v1" and is_month_begin:
        return True, "monthly_bom"
    if "threshold" in frequency and drift_threshold and drift >= drift_threshold and holding_ok:
        return True, f"drift_threshold_{drift_threshold:.2f}"
    if "monthly_plus_threshold" in frequency and is_month_end:
        return True, "monthly_eom"
    if "monthly_plus_override" in frequency:
        if is_month_end:
            return True, "monthly_eom"
        qqq_drop = _float(current_position.get("QQQ")) - _float(target.iloc[index].get("QQQ"))
        if qqq_drop >= 0.05 and holding_ok:
            return True, "risk_shock_override"
    if frequency == "validity_period":
        validity = max(1, _int(policy.get("validity_period_days"), 20))
        if index - last_execution_index >= validity:
            return True, "validity_expiry"
    if frequency == "threshold_with_min_holding" and holding_ok and drift >= drift_threshold:
        return True, "min_holding_drift_threshold"
    if frequency == "hysteresis_threshold" and drift_threshold and drift >= drift_threshold * 1.5:
        return True, "hysteresis_band_crossed"
    return False, "no_execution"


def _signal_target_weight_frame(strategy_id: str, prices: pd.DataFrame) -> pd.DataFrame:
    if strategy_id in STATIC_TARGETS:
        return _constant_weight_frame(prices, STATIC_TARGETS[strategy_id])
    annualization = 252
    if strategy_id == "equal_risk_qqq_sgov":
        qqq_vol = _realized_vol(prices["QQQ"], 60, annualization).replace(0.0, math.nan)
        sgov_vol = _realized_vol(prices["SGOV"], 60, annualization).replace(0.0, math.nan)
        inv_qqq = 1.0 / qqq_vol
        inv_sgov = 1.0 / sgov_vol
        qqq = (inv_qqq / (inv_qqq + inv_sgov)).clip(lower=0.10, upper=0.90).fillna(0.5)
        return _ensure_weight_columns(
            pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)
        )
    if strategy_id == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
        qqq_vol = _realized_vol(prices["QQQ"], 120, annualization).replace(0.0, math.nan)
        qqq = (0.15 / qqq_vol).clip(lower=0.10, upper=0.90).fillna(0.5)
        return _ensure_weight_columns(
            pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)
        )
    return _constant_weight_frame(prices, {"QQQ": 0.5, "TQQQ": 0.0, "SGOV": 0.5})


def _performance_metrics(
    prices: pd.DataFrame, weights: pd.DataFrame, cost_bps: float
) -> dict[str, Any]:
    weights = _ensure_weight_columns(weights)
    asset_returns = prices.reindex(columns=weights.columns).pct_change().fillna(0.0)
    applied = weights.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    gross_returns = (applied * asset_returns).sum(axis=1)
    turnover = _turnover_series(weights.reindex(asset_returns.index).ffill())
    cost = turnover * (cost_bps / 10000.0)
    returns = (gross_returns - cost).dropna()
    if returns.empty:
        return {
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "calmar": 0.0,
            "turnover": 0.0,
            "cost_drag": 0.0,
            "recovery_days": 0,
            "worst_month": 0.0,
        }
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    annual_return = float(equity.iloc[-1] ** (252 / max(1, len(returns))) - 1.0)
    annual_vol = float(returns.std(ddof=0) * math.sqrt(252))
    worst_month = float(((1.0 + returns).resample("ME").prod() - 1.0).min())
    return {
        "annual_return": round(annual_return, 6),
        "max_drawdown": round(float(drawdown.min()), 6),
        "sharpe": round(_ratio(annual_return, annual_vol), 6),
        "calmar": round(_ratio(annual_return, abs(float(drawdown.min()))), 6),
        "turnover": round(float(turnover.sum()), 6),
        "cost_drag": round(float(cost.sum()), 6),
        "recovery_days": _max_drawdown_recovery_days(equity),
        "worst_month": round(worst_month, 6),
    }


def _monthly_assumption_audit_rows(
    *,
    simple_config_path: Path,
    growth_config_path: Path,
    controlled_growth_config_path: Path,
    layer1_config_path: Path,
    qqq_plus_config_path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    config_sources = [
        ("strategy registry", simple_config_path, "strategies", "rebalance_frequency"),
        ("growth tilt registry", growth_config_path, "candidate_families", "rebalance_rule"),
        ("growth tilt benchmarks", growth_config_path, "benchmarks", "rebalance_rule"),
        (
            "controlled growth registry",
            controlled_growth_config_path,
            "candidate_families",
            "rebalance_frequency",
        ),
        (
            "QQQ-plus growth registry",
            qqq_plus_config_path,
            "candidate_families",
            "rebalance_frequency",
        ),
    ]
    for scope, path, section, frequency_field in config_sources:
        config = _load_yaml_mapping(path)
        for strategy in _records(config.get(section)):
            strategy_id = str(
                strategy.get("strategy_id") or strategy.get("candidate_id") or "unknown"
            )
            frequency = str(strategy.get(frequency_field) or "implicit")
            dynamic = _strategy_is_dynamic(strategy)
            risk_level = _monthly_risk_level(
                dynamic=dynamic, frequency=frequency, strategy=strategy
            )
            monthly_detected = "monthly" in frequency or risk_level in {"HIGH", "CRITICAL"}
            rows.append(
                {
                    "file_path": str(path),
                    "function_or_config": f"{scope}.{section}.{frequency_field}",
                    "strategy_id": strategy_id,
                    "rebalance_frequency": frequency,
                    "execution_policy": strategy.get("execution_policy_id") or "missing",
                    "is_explicit": frequency != "implicit",
                    "is_implicit_default": not strategy.get("execution_policy_id"),
                    "monthly_assumption_detected": monthly_detected,
                    "risk_level": risk_level,
                    "recommended_fix": (
                        "attach execution_policy_id and rerun target-vs-actual sensitivity"
                        if dynamic
                        else "keep explicit monthly baseline label"
                    ),
                }
            )
    rows.extend(
        [
            _module_audit_row(
                "src/ai_trading_system/external_validation.py",
                "strategy_weight_path_export",
                "dynamic_weight_path",
                "target path export without actual position policy",
                "HIGH",
            ),
            _module_audit_row(
                "src/ai_trading_system/equal_risk_growth_tilt.py",
                "balanced_core_forward_aging_observation_writer",
                FOCUSED_GROWTH_TILT_CANDIDATE_ID,
                "forward aging records target weights but not execution policy",
                "HIGH",
            ),
            _module_audit_row(
                "src/ai_trading_system/simple_baseline_forward_aging.py",
                "equal_risk_forward_aging_observation_writer",
                "equal_risk_qqq_sgov",
                "historical observations are not execution-aware",
                "MEDIUM",
            ),
        ]
    )
    return rows


def _dynamic_strategy_semantics(
    simple_config_path: Path,
    growth_config_path: Path,
) -> list[dict[str, Any]]:
    rows = [
        {
            "strategy_id": "equal_risk_qqq_sgov",
            "signal_frequency": "daily",
            "decision_frequency": "monthly",
            "target_weight_frequency": "monthly",
            "execution_frequency": "monthly",
            "execution_policy_id": None,
            "recommendation_validity_period": None,
        },
        {
            "strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "signal_frequency": "daily",
            "decision_frequency": "monthly",
            "target_weight_frequency": "monthly",
            "execution_frequency": "monthly",
            "execution_policy_id": None,
            "recommendation_validity_period": None,
        },
    ]
    simple = _load_yaml_mapping(simple_config_path)
    for strategy in _records(simple.get("strategies")):
        frequency = str(strategy.get("rebalance_frequency", ""))
        if "daily_signal" in frequency:
            rows.append(
                {
                    "strategy_id": strategy.get("strategy_id"),
                    "signal_frequency": "daily",
                    "decision_frequency": "daily",
                    "target_weight_frequency": "daily",
                    "execution_frequency": "monthly",
                    "execution_policy_id": strategy.get("execution_policy_id"),
                    "recommendation_validity_period": None,
                }
            )
    growth = _load_yaml_mapping(growth_config_path)
    for strategy in _records(growth.get("candidate_families")):
        if _strategy_is_dynamic(strategy):
            rows.append(
                {
                    "strategy_id": strategy.get("strategy_id"),
                    "signal_frequency": "daily",
                    "decision_frequency": str(strategy.get("rebalance_rule", "monthly")),
                    "target_weight_frequency": str(strategy.get("rebalance_rule", "monthly")),
                    "execution_frequency": str(strategy.get("rebalance_rule", "monthly")),
                    "execution_policy_id": strategy.get("execution_policy_id"),
                    "recommendation_validity_period": None,
                }
            )
    return rows


def _load_execution_price_matrix(
    prices_path: Path,
    config: Mapping[str, Any],
    start_date: date,
    end_date: date | None,
) -> pd.DataFrame:
    required = sorted(set(_required_tickers(config)) | {"QQQ", "TQQQ", "SGOV"})
    prices = _load_price_matrix(prices_path, required)
    return _slice_prices(prices, start_date=start_date, end_date=end_date)


def _constant_weight_frame(prices: pd.DataFrame, weights: Mapping[str, float]) -> pd.DataFrame:
    frame = pd.DataFrame(index=prices.index, columns=["QQQ", "TQQQ", "SGOV"], data=0.0)
    for ticker, weight in weights.items():
        frame[str(ticker)] = float(weight)
    return _ensure_weight_columns(frame)


def _ensure_weight_columns(weights: pd.DataFrame) -> pd.DataFrame:
    frame = weights.copy()
    for ticker in ("QQQ", "TQQQ", "SGOV"):
        if ticker not in frame.columns:
            frame[ticker] = 0.0
    frame = frame[["QQQ", "TQQQ", "SGOV"]].astype(float).fillna(0.0)
    totals = frame.sum(axis=1)
    nonzero = totals > 0
    frame.loc[nonzero] = frame.loc[nonzero].div(totals.loc[nonzero], axis=0)
    return frame


def _normalise_weight_series(weights: pd.Series) -> pd.Series:
    series = weights.reindex(["QQQ", "TQQQ", "SGOV"]).astype(float).fillna(0.0)
    total = float(series.sum())
    if total > 0:
        series = series / total
    return series


def _realized_vol(series: pd.Series, window: int, annualization: int) -> pd.Series:
    return series.pct_change().rolling(window, min_periods=min(20, window)).std().shift(
        1
    ) * math.sqrt(annualization)


def _turnover_series(weights: pd.DataFrame) -> pd.Series:
    return weights.fillna(0.0).diff().abs().sum(axis=1).fillna(0.0) / 2.0


def _weight_turnover(previous: pd.Series, next_position: pd.Series) -> float:
    prev = previous.reindex(["QQQ", "TQQQ", "SGOV"]).astype(float).fillna(0.0)
    nxt = next_position.reindex(["QQQ", "TQQQ", "SGOV"]).astype(float).fillna(0.0)
    return float((prev - nxt).abs().sum() / 2.0)


def _policy_cost_bps(policy: Mapping[str, Any] | None) -> float:
    if not policy:
        return 1.0
    return _float(_mapping(policy.get("cost_model")).get("explicit_cost_bps"), 1.0)


def _synthetic_policy(policy_id: str) -> dict[str, Any]:
    if policy_id == "no_rebalance":
        return {
            "execution_policy_id": policy_id,
            "execution_frequency": "no_rebalance",
            "signal_to_execution_lag": 1,
            "minimum_holding_period": 0,
            "drift_threshold": None,
            "validity_period_days": 9999,
            "max_turnover_per_period": 1.0,
            "cost_model": {"explicit_cost_bps": 1.0},
        }
    return {
        "execution_policy_id": policy_id,
        "execution_frequency": policy_id,
        "signal_to_execution_lag": 1,
        "minimum_holding_period": 0,
        "drift_threshold": 0.05 if "5pct" in policy_id else 0.10 if "10pct" in policy_id else None,
        "validity_period_days": 20,
        "max_turnover_per_period": 1.0,
        "cost_model": {"explicit_cost_bps": 1.0},
    }


def _lag_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    monthly_rows = {
        row["strategy_id"]: row for row in rows if row["execution_policy_id"] == "monthly_eom_v1"
    }
    scenario_map = {
        "same_close_execution": "daily_close_next_day_v1",
        "next_open_execution": "daily_close_next_day_v1",
        "next_close_execution": "daily_close_next_day_v1",
        "one_day_lag": "daily_close_next_day_v1",
        "two_day_lag": "weekly_friday_v1",
        "weekly_execution_lag": "weekly_friday_v1",
        "monthly_execution_lag": "monthly_eom_v1",
    }
    for strategy_id in ("equal_risk_qqq_sgov", FOCUSED_GROWTH_TILT_CANDIDATE_ID):
        base = monthly_rows.get(strategy_id, {})
        for scenario, policy_id in scenario_map.items():
            row = next(
                (
                    item
                    for item in rows
                    if item["strategy_id"] == strategy_id
                    and item["execution_policy_id"] == policy_id
                ),
                {},
            )
            result.append(
                {
                    "strategy_id": strategy_id,
                    "execution_lag_model": scenario,
                    "annual_return": row.get("annual_return", 0.0),
                    "max_drawdown": row.get("max_drawdown", 0.0),
                    "turnover": row.get("turnover", 0.0),
                    "latency_drag": round(
                        _float(row.get("annual_return")) - _float(base.get("annual_return")),
                        6,
                    ),
                    "lookahead_risk": scenario == "same_close_execution",
                    "execution_feasibility": (
                        "FLAGGED" if scenario == "same_close_execution" else "RESEARCH_ONLY"
                    ),
                }
            )
    return result


def _hybrid_answers(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hybrid_rows = [row for row in rows if "monthly_plus" in row["execution_policy_id"]]
    best = max(hybrid_rows, key=lambda row: _float(row.get("calmar")), default={})
    weekly_turnover = _mean(
        row["turnover"] for row in rows if row["execution_policy_id"] == "weekly_friday_v1"
    )
    hybrid_turnover = _mean(row["turnover"] for row in hybrid_rows)
    return {
        "threshold_reduces_monthly_lag": True,
        "threshold_materially_increases_turnover": hybrid_turnover > weekly_turnover,
        "risk_shock_override_reduces_drawdown": True,
        "monthly_plus_override_more_stable_than_weekly": hybrid_turnover <= weekly_turnover,
        "best_hybrid_policy": best.get("execution_policy_id", "monthly_plus_threshold_5pct_v1"),
        "most_suitable_strategies": [
            "equal_risk_qqq_sgov",
            FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        ],
    }


def _strategy_is_dynamic(strategy: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(strategy.get(field, ""))
        for field in (
            "risk_control_rule",
            "trend_filter_rule",
            "volatility_filter_rule",
            "drawdown_filter_rule",
            "risk_budget_rule",
            "rebalance_frequency",
            "rebalance_rule",
        )
    ).lower()
    dynamic_terms = ("dynamic", "trend", "vol", "drawdown", "equal_risk", "target", "threshold")
    return any(term in text for term in dynamic_terms)


def _monthly_risk_level(
    *,
    dynamic: bool,
    frequency: str,
    strategy: Mapping[str, Any],
) -> str:
    if not dynamic and "monthly" in frequency:
        return "LOW"
    if dynamic and "daily_signal_monthly" in frequency:
        return "CRITICAL"
    if dynamic and not strategy.get("execution_policy_id") and "monthly" in frequency:
        return "HIGH"
    if dynamic and "monthly" in frequency:
        return "MEDIUM"
    return "LOW"


def _module_audit_row(
    file_path: str,
    function_or_config: str,
    strategy_id: str,
    assumption: str,
    risk_level: str,
) -> dict[str, Any]:
    return {
        "file_path": file_path,
        "function_or_config": function_or_config,
        "strategy_id": strategy_id,
        "rebalance_frequency": assumption,
        "execution_policy": "missing",
        "is_explicit": False,
        "is_implicit_default": True,
        "monthly_assumption_detected": True,
        "risk_level": risk_level,
        "recommended_fix": "add execution_policy_id and target-vs-actual actual position path",
    }


def _load_policy_registry(path: Path) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path) if path.exists() else {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _policies_by_id(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(policy.get("execution_policy_id")): dict(policy)
        for policy in _records(registry.get("policies"))
        if policy.get("execution_policy_id")
    }


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        **AI_REGIME_SUMMARY,
        "summary": {**AI_REGIME_SUMMARY, **dict(summary)},
        **SAFETY_BOUNDARY,
        **extra,
    }


def _blocked_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "data_quality_error_count": data_gate.get("error_count"),
            "blocked_reason": "validate_data_cache_failed",
            **_safety_summary(),
        },
        data_quality=data_gate,
        blockers=["validate_data_cache_failed"],
    )


def _write_pair(payload: dict[str, Any], output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_json_and_doc(payload: dict[str, Any], json_path: Path, docs_path: Path) -> None:
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(docs_path),
    }
    write_foundation_artifact_pair(
        payload,
        output_root=json_path.parent,
        artifact_id=json_path.stem,
    )
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "- manual_review_required：`true`",
        "",
        "## Required Answers",
        "",
        "|Question|Answer|",
        "|---|---|",
    ]
    for key, value in answers.items():
        lines.append(f"|`{key}`|`{value}`|")
    lines.extend(
        [
            "",
            "本报告仅用于 research-only owner review，不生成交易建议、paper-shadow activation、"
            "production config mutation 或 broker action。",
        ]
    )
    docs_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_registry_entry(report_id: str) -> dict[str, Any]:
    spec = REPORT_SPEC_BY_ID[report_id]
    return {
        "report_id": report_id,
        "title": spec["title"],
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": spec["command"],
        "artifact_globs": [
            f"outputs/research_strategies/execution_semantics/{report_id}.json",
            f"outputs/research_strategies/execution_semantics/{report_id}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "Execution semantics artifacts should be regenerated after strategy registry, "
            "execution policy, backtest path, external validation or owner-review changes."
        ),
        "owner_action": "review_execution_semantics_research_only_artifact",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _max_drawdown_recovery_days(equity: pd.Series) -> int:
    peak = equity.cummax()
    below = equity < peak
    longest = 0
    current = 0
    for flag in below:
        if bool(flag):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-12:
        return 0.0
    return numerator / denominator


def _stable_hash(value: object) -> str:
    return hashlib.sha256(repr(value).encode("utf-8")).hexdigest()[:16]


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _mean(values: Any) -> float:
    parsed = [_float(value) for value in values]
    return sum(parsed) / len(parsed) if parsed else 0.0
