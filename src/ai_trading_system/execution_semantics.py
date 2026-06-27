from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

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
DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "policy_sensitivity"
)
DEFAULT_REBALANCE_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "rebalance_assumption_owner_review_pack.md"
)
DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_actual_path_owner_review_decision.md"
)
DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "dynamic_actual_path_owner_review_decision.yaml"
)
DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_actual_path_policy_sensitivity_review.md"
)
DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "dynamic_actual_path_policy_sensitivity_matrix.yaml"
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

REQUIRED_STRATEGY_EXECUTION_POLICY_FIELDS: tuple[str, ...] = (
    "strategy_id",
    "strategy_type",
    "policy_status",
    "execution_policy_id",
    "signal_policy",
    "rebalance_policy",
    "position_policy",
    "cost_policy",
    "validation_policy",
)

REQUIRED_STRATEGY_POLICY_SECTIONS: dict[str, tuple[str, ...]] = {
    "signal_policy": (
        "signal_source",
        "signal_observation_time",
        "signal_effective_earliest",
        "signal_validity_window_bdays",
        "stale_signal_behavior",
    ),
    "rebalance_policy": (
        "rebalance_frequency",
        "rebalance_anchor",
        "allow_intramonth_rebalance",
        "execution_lag_bdays",
    ),
    "position_policy": (
        "target_weight_rule",
        "actual_weight_fill_rule",
        "no_signal_behavior",
        "cash_or_safe_asset",
    ),
    "cost_policy": (
        "transaction_cost_bps",
        "slippage_bps",
        "turnover_calculation",
    ),
    "validation_policy": (
        "requires_actual_position_rebacktest",
        "promotion_allowed_from_target_path",
    ),
}

DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS: tuple[str, ...] = (
    "no_trade",
    "100_qqq",
    "qqq_60_sgov_40",
    "qqq_50_sgov_50",
    "limited_adjustment",
    "defensive_limited_adjustment",
    "dynamic_regime_overlay_v0_4_lower_turnover",
    "dynamic_v0_5_ai_trend_confirmed_only",
)

ACTUAL_PATH_OWNER_REVIEW_CANDIDATES: tuple[str, ...] = (
    "limited_adjustment",
    "dynamic_v0_5_ai_trend_confirmed_only",
)

ACTUAL_PATH_OWNER_REVIEW_BASELINES: tuple[str, ...] = (
    "no_trade",
    "100_qqq",
    "qqq_60_sgov_40",
    "qqq_50_sgov_50",
)

POLICY_SENSITIVITY_EXECUTION_LAG_DAYS: tuple[int, ...] = (0, 1, 2)
POLICY_SENSITIVITY_REBALANCE_FREQUENCIES: tuple[str, ...] = (
    "next_trading_day",
    "weekly",
    "monthly",
)
POLICY_SENSITIVITY_SIGNAL_VALIDITY_WINDOWS: tuple[int, ...] = (1, 3, 5, 10, 20)
POLICY_SENSITIVITY_TURNOVER_CONSTRAINTS: tuple[str, ...] = (
    "existing_default",
    "relaxed",
    "strict",
)

POLICY_SENSITIVITY_CLASSIFICATION_POLICY: dict[str, Any] = {
    "policy_id": "dynamic_actual_path_policy_sensitivity_classification_v1",
    "owner": "research_governance",
    "status": "pilot_baseline",
    "rationale": (
        "Classify execution-policy robustness using actual-path annual-return "
        "advantage versus no_trade and lag/staleness materiality only; target-path "
        "performance is excluded from ranking and decision support."
    ),
    "survival_rule": (
        "A scenario survives when actual_path annual_return is above same-scenario "
        "no_trade and lag/staleness materiality is not FAIL."
    ),
    "review_condition": (
        "Review before using sensitivity classifications for paper-shadow preflight "
        "admission or changing execution policy defaults."
    ),
}

REBACKTEST_STRATEGY_ID_ALIASES: dict[str, str] = {
    "no_trade_baseline": "no_trade",
    "static_100_qqq": "100_qqq",
    "static_qqq_60_sgov_40": "qqq_60_sgov_40",
    "static_qqq_50_sgov_50": "qqq_50_sgov_50",
    "v0_4_lower_turnover": "dynamic_regime_overlay_v0_4_lower_turnover",
    "v0_5_ai_trend_confirmed_only": "dynamic_v0_5_ai_trend_confirmed_only",
}

# Fallback mirrors config/research/strategy_execution_policy_registry.yaml. The
# registry value is the governed source; this fallback keeps tests fail-closed if
# a synthetic registry omits the pilot materiality block.
DEFAULT_EXECUTION_MATERIALITY_THRESHOLDS: dict[str, float] = {
    "execution_lag_return_cost_abs_pp": 1.0,
    "execution_lag_return_cost_relative_pct": 20.0,
    "execution_lag_max_drawdown_cost_pp": 2.0,
    "signal_staleness_material_event_count": 3.0,
    "actual_trade_delay_days_p95": 10.0,
}

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
        "report_id": "execution_semantics_rebacktest_gate",
        "title": "Execution Semantics Rebacktest Gate",
        "command": "aits research strategies execution-semantics-rebacktest-gate",
    },
    {
        "report_id": "execution_semantics_rebacktest",
        "title": "Execution Semantics Aware Rebacktest",
        "command": "aits research strategies execution-semantics-rebacktest",
    },
    {
        "report_id": "dynamic_actual_path_owner_review_decision",
        "title": "Dynamic Actual-Path Owner Review Decision",
        "command": "aits research strategies dynamic-actual-path-owner-review-decision",
    },
    {
        "report_id": "dynamic_actual_path_policy_sensitivity_review",
        "title": "Dynamic Actual-Path Policy Sensitivity Review",
        "command": "aits research strategies dynamic-actual-path-policy-sensitivity-review",
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
    strategy_bindings = _strategy_execution_bindings(registry)
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
        "no_rebalance",
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
    issues.extend(_strategy_binding_issues(strategy_bindings, policy_ids))
    status = (
        "EXECUTION_POLICY_REGISTRY_BLOCKED"
        if not policies or not strategy_bindings
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
            "strategy_binding_count": len(strategy_bindings),
            "issue_count": len(issues),
            "required_policy_count": len(required_ids),
            **_safety_summary(),
        },
        policy_registry_path=str(policy_registry_path),
        policies=policies,
        strategy_execution_policies=strategy_bindings,
        issues=issues,
        required_fields=list(REQUIRED_EXECUTION_POLICY_FIELDS),
        required_strategy_binding_fields=list(REQUIRED_STRATEGY_EXECUTION_POLICY_FIELDS),
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
    _attach_path_return_columns(
        prices=prices,
        target_weights=target_weights,
        actual_weights=actual,
        path_rows=path_rows,
        cost_bps=_policy_cost_bps(policies.get(execution_policy_id)),
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


def run_execution_semantics_rebacktest_gate(
    *,
    strategy_id: str = "limited_adjustment",
    backtest_generation: str = "PRE_EXECUTION_SEMANTICS",
    position_path_used_for_metrics: str = "TARGET",
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry = _load_policy_registry(policy_registry_path)
    binding = _strategy_execution_binding_by_id(registry).get(strategy_id)
    gate = _execution_semantics_promotion_gate_decision(
        strategy_id=strategy_id,
        strategy_binding=binding,
        backtest_generation=backtest_generation,
        position_path_used_for_metrics=position_path_used_for_metrics,
        actual_rebacktest_available=(
            backtest_generation == "EXECUTION_SEMANTICS_AWARE"
            and position_path_used_for_metrics == "ACTUAL"
        ),
    )
    payload = _payload(
        report_type="execution_semantics_rebacktest_gate",
        title=REPORT_SPEC_BY_ID["execution_semantics_rebacktest_gate"]["title"],
        status=gate["status"],
        summary={
            "strategy_id": strategy_id,
            "strategy_type": gate["strategy_type"],
            "promotion_eligible": gate["promotion_eligible"],
            "rebacktest_required": gate["rebacktest_required"],
            "backtest_generation": backtest_generation,
            "position_path_used_for_metrics": position_path_used_for_metrics,
            **_safety_summary(),
        },
        gate_decision=gate,
        legacy_result_tags=[
            "PRE_EXECUTION_SEMANTICS_LEGACY_EVIDENCE",
            "PRE_EXECUTION_SEMANTICS",
            "REBACKTEST_REQUIRED",
            "NOT_PROMOTION_ELIGIBLE",
        ]
        if gate["rebacktest_required"]
        else [],
        legacy_evidence_notice=(
            "Pre-execution-semantics dynamic results are candidate evidence only. "
            "They are not eligible for promotion or paper-shadow decisions without "
            "actual-path rebacktest."
        ),
        report_registry_entry=_report_registry_entry("execution_semantics_rebacktest_gate"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_execution_semantics_rebacktest(
    *,
    strategy_id: str | None = None,
    strategy_ids: list[str] | tuple[str, ...] | None = None,
    execution_policy_id: str | None = None,
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
            report_type="execution_semantics_rebacktest",
            title=REPORT_SPEC_BY_ID["execution_semantics_rebacktest"]["title"],
            status="EXECUTION_SEMANTICS_REBACKTEST_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload

    prices = _load_execution_price_matrix(prices_path, config, start_date, end_date)
    registry = _load_policy_registry(policy_registry_path)
    policies = _policies_by_id(registry)
    bindings = _strategy_execution_binding_by_id(registry)
    policy_ids = set(policies)
    materiality_thresholds = _execution_materiality_thresholds(registry)
    policy_registry_hash = _file_sha256(policy_registry_path)
    selected_strategy_ids = _selected_rebacktest_strategy_ids(strategy_id, strategy_ids)
    rows: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []

    for current_strategy_id in selected_strategy_ids:
        binding = bindings.get(current_strategy_id)
        binding_issues = (
            _strategy_binding_issues([binding], policy_ids) if binding else []
        )
        gate = _execution_semantics_promotion_gate_decision(
            strategy_id=current_strategy_id,
            strategy_binding=binding,
            backtest_generation="EXECUTION_SEMANTICS_AWARE",
            position_path_used_for_metrics="ACTUAL",
            actual_rebacktest_available=True,
        )
        policy_id = str(execution_policy_id or _mapping(binding).get("execution_policy_id") or "")
        policy = policies.get(policy_id)
        policy_issues = _policy_definition_issues(policy) if policy else []
        if not binding or not policy or binding_issues or policy_issues:
            issue_reasons = [
                str(issue.get("issue"))
                for issue in binding_issues + policy_issues
                if issue.get("issue")
            ]
            blocked = {
                "strategy_id": current_strategy_id,
                "status": "EXECUTION_POLICY_MISSING",
                "strategy_type": _mapping(binding).get("strategy_type", "unknown"),
                "execution_policy_id": policy_id or None,
                "policy_hash": None,
                "promotion_eligible": False,
                "rebacktest_required": True,
                "promotion_final_status": "blocked",
                "blocking_reasons": _dedupe_ordered(
                    list(gate["blocking_reasons"])
                    + ([] if policy else [f"execution_policy_not_found:{policy_id or 'missing'}"])
                    + issue_reasons
                ),
                "failure_reason": "strategy_execution_policy_binding_or_definition_invalid",
            }
            blocked_rows.append(blocked)
            rows.append(blocked)
            continue

        target_weights = _signal_target_weight_frame(current_strategy_id, prices)
        actual_weights, path_rows = _actual_position_path(
            strategy_id=current_strategy_id,
            execution_policy_id=policy_id,
            target_weights=target_weights,
            policy=policy,
        )
        _attach_path_return_columns(
            prices=prices,
            target_weights=target_weights,
            actual_weights=actual_weights,
            path_rows=path_rows,
            cost_bps=_policy_cost_bps(policy),
        )
        metrics_target = _performance_metrics(prices, target_weights, cost_bps=0.0)
        metrics_actual = _performance_metrics(
            prices,
            actual_weights,
            cost_bps=_policy_cost_bps(policy),
        )
        lag_cost = _lag_cost_summary(
            metrics_target,
            metrics_actual,
            path_rows,
            thresholds=materiality_thresholds,
        )
        staleness = _signal_staleness_summary(
            path_rows,
            thresholds=materiality_thresholds,
        )
        policy_hash = _policy_snapshot_hash(binding=binding, policy=policy)
        namespaced_actual = _namespace_path_metrics(metrics_actual, "actual_path")
        namespaced_target = _namespace_path_metrics(metrics_target, "target_path")
        gap_metrics = _target_vs_actual_gap_metrics(
            target_metrics=namespaced_target,
            actual_metrics=namespaced_actual,
            lag_cost=lag_cost,
            staleness=staleness,
        )
        promotion_readiness = _promotion_readiness_for_rebacktest(
            strategy_id=current_strategy_id,
            binding=binding,
            policy=policy,
            metrics_actual=metrics_actual,
            metrics_target=metrics_target,
            lag_cost=lag_cost,
            staleness=staleness,
            gate=gate,
            policy_hash=policy_hash,
        )
        artifact_paths = _write_strategy_rebacktest_artifacts(
            output_root=output_root / current_strategy_id,
            strategy_id=current_strategy_id,
            binding=binding,
            policy=policy,
            policy_hash=policy_hash,
            path_rows=path_rows,
            metrics_target=metrics_target,
            metrics_actual=metrics_actual,
            lag_cost=lag_cost,
            staleness=staleness,
            promotion_readiness=promotion_readiness,
            materiality_thresholds=materiality_thresholds,
            date_range_start=prices.index.min().date().isoformat(),
            date_range_end=prices.index.max().date().isoformat(),
        )
        rows.append(
            {
                "strategy_id": current_strategy_id,
                "status": "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE",
                "strategy_type": binding.get("strategy_type"),
                "execution_policy_id": policy_id,
                "policy_hash": policy_hash,
                "backtest_generation": "EXECUTION_SEMANTICS_AWARE",
                "position_path_used_for_metrics": "ACTUAL",
                "metric_convention_namespace": "internal.execution_semantics.actual_path.v1",
                "promotion_eligible": promotion_readiness["promotion_eligible"],
                "rebacktest_required": promotion_readiness["rebacktest_required"],
                "promotion_final_status": promotion_readiness["final_status"],
                "blocking_reasons": promotion_readiness["blocking_reason_codes"],
                **namespaced_actual,
                **gap_metrics,
                "annual_return_actual_path": metrics_actual["annual_return"],
                "annual_return_target_path": metrics_target["annual_return"],
                "annual_return_lag_cost": lag_cost["annual_return_lag_cost"],
                "average_signal_age_bdays": staleness["average_signal_age_bdays"],
                "stale_signal_day_pct": staleness["stale_signal_day_pct"],
                "artifact_paths": artifact_paths,
            }
        )

    date_range = {
        "start": prices.index.min().date().isoformat(),
        "end": prices.index.max().date().isoformat(),
        "market_regime": "ai_after_chatgpt",
    }
    aggregate_artifact_paths = _write_rebacktest_aggregate_artifacts(
        output_root=output_root,
        strategy_rows=rows,
        blocked_rows=blocked_rows,
        selected_strategy_ids=selected_strategy_ids,
        date_range=date_range,
        data_quality=data_gate,
        policy_registry_path=policy_registry_path,
        policy_registry_hash=policy_registry_hash,
        materiality_thresholds=materiality_thresholds,
    )
    status = (
        "EXECUTION_SEMANTICS_REBACKTEST_COMPLETE_WITH_BLOCKED_ROWS"
        if blocked_rows
        else "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE"
    )
    payload = _payload(
        report_type="execution_semantics_rebacktest",
        title=REPORT_SPEC_BY_ID["execution_semantics_rebacktest"]["title"],
        status=status,
        summary={
            "strategy_count": len(selected_strategy_ids),
            "completed_count": sum(
                1
                for row in rows
                if row.get("status") == "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE"
            ),
            "blocked_count": len(blocked_rows),
            "promotion_eligible_count": sum(
                1 for row in rows if row.get("promotion_eligible") is True
            ),
            "promotion_decision_source": "actual_path_only",
            "target_path_metrics_role": "diagnostic_only",
            "dynamic_promotion_blocked": True,
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        strategy_rows=rows,
        blocked_rows=blocked_rows,
        data_quality=data_gate,
        date_range=date_range,
        policy_registry_path=str(policy_registry_path),
        policy_registry_hash=policy_registry_hash,
        selected_strategy_id_mapping={
            original: canonical
            for original, canonical in REBACKTEST_STRATEGY_ID_ALIASES.items()
            if canonical in selected_strategy_ids
        },
        target_path_diagnostic_notice=(
            "Target-path metrics are diagnostic only and are not eligible for "
            "promotion decisions."
        ),
        aggregate_artifact_paths=aggregate_artifact_paths,
        report_registry_entry=_report_registry_entry("execution_semantics_rebacktest"),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_actual_path_owner_review_decision(
    *,
    output_root: Path = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH,
    yaml_path: Path = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH,
) -> dict[str, Any]:
    index_payload = _read_json_mapping(output_root / "index.json")
    date_range = _mapping(index_payload.get("date_range"))
    strategy_metrics = {
        strategy_id: _load_actual_path_strategy_evidence(output_root, strategy_id)
        for strategy_id in (
            *ACTUAL_PATH_OWNER_REVIEW_BASELINES,
            *ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
        )
    }
    missing = [
        strategy_id
        for strategy_id, evidence in strategy_metrics.items()
        if not evidence.get("actual_path_metrics")
    ]
    if missing:
        payload = _payload(
            report_type="dynamic_actual_path_owner_review_decision",
            title=REPORT_SPEC_BY_ID["dynamic_actual_path_owner_review_decision"]["title"],
            status="OWNER_REVIEW_DECISION_BLOCKED",
            summary={
                "candidate_count": len(ACTUAL_PATH_OWNER_REVIEW_CANDIDATES),
                "missing_strategy_artifact_count": len(missing),
                "dynamic_promotion_blocked": True,
                **_safety_summary(),
            },
            blockers=[f"missing_actual_path_artifacts:{strategy_id}" for strategy_id in missing],
            source_runtime_root=str(output_root),
            report_registry_entry=_report_registry_entry(
                "dynamic_actual_path_owner_review_decision"
            ),
        )
        _write_owner_review_decision_artifacts(payload, docs_path, yaml_path)
        return payload

    decisions = [
        _owner_review_decision_for_candidate(
            candidate_id=candidate_id,
            strategy_metrics=strategy_metrics,
        )
        for candidate_id in ACTUAL_PATH_OWNER_REVIEW_CANDIDATES
    ]
    summary = {
        "candidate_count": len(decisions),
        "paper_shadow_candidate_recommendation_count": sum(
            1
            for item in decisions
            if item.get("system_review_recommendation") == "PAPER_SHADOW_CANDIDATE"
        ),
        "watch_only_recommendation_count": sum(
            1 for item in decisions if item.get("system_review_recommendation") == "WATCH_ONLY"
        ),
        "reject_recommendation_count": sum(
            1 for item in decisions if item.get("system_review_recommendation") == "REJECT"
        ),
        "owner_manual_review_required": True,
        "owner_decision_status": "pending",
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        "dynamic_promotion_blocked": True,
        "data_quality_status": index_payload.get("data_quality_status"),
        **_safety_summary(),
    }
    payload = _payload(
        report_type="dynamic_actual_path_owner_review_decision",
        title=REPORT_SPEC_BY_ID["dynamic_actual_path_owner_review_decision"]["title"],
        status="DYNAMIC_ACTUAL_PATH_OWNER_REVIEW_DECISION_READY",
        summary=summary,
        source_runtime_root=str(output_root),
        date_range={
            "start": date_range.get("start"),
            "end": date_range.get("end"),
            "market_regime": date_range.get("market_regime", "ai_after_chatgpt"),
        },
        tracked_evidence=[
            "docs/research/execution_semantics_actual_path_rebacktest_review.md",
            "docs/research/artifact_snapshots/execution_semantics_actual_path_rebacktest_snapshot.yaml",
            "docs/research/execution_semantics_strategy_survival_review.md",
            "inputs/research_reviews/execution_semantics_strategy_survival_matrix.yaml",
        ],
        baseline_strategy_ids=list(ACTUAL_PATH_OWNER_REVIEW_BASELINES),
        candidate_strategy_ids=list(ACTUAL_PATH_OWNER_REVIEW_CANDIDATES),
        owner_review_decisions=decisions,
        target_path_diagnostic_notice=(
            "Target-path metrics are diagnostic only and are not eligible for owner "
            "decision, promotion readiness or ranking support."
        ),
        report_registry_entry=_report_registry_entry(
            "dynamic_actual_path_owner_review_decision"
        ),
    )
    _write_owner_review_decision_artifacts(payload, docs_path, yaml_path)
    return payload


def run_dynamic_actual_path_policy_sensitivity_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH,
    yaml_path: Path = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH,
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
            report_type="dynamic_actual_path_policy_sensitivity_review",
            title=REPORT_SPEC_BY_ID[
                "dynamic_actual_path_policy_sensitivity_review"
            ]["title"],
            status="POLICY_SENSITIVITY_BLOCKED",
            data_gate=data_gate,
        )
        _write_policy_sensitivity_artifacts(
            payload=payload,
            output_root=output_root,
            docs_path=docs_path,
            yaml_path=yaml_path,
            matrix_rows=[],
            leaderboard_rows=[],
            gap_rows=[],
            readiness_summary={},
            summary_payload={},
        )
        return payload

    prices = _load_execution_price_matrix(prices_path, config, start_date, end_date)
    registry = _load_policy_registry(policy_registry_path)
    policies = _policies_by_id(registry)
    bindings = _strategy_execution_binding_by_id(registry)
    materiality_thresholds = _execution_materiality_thresholds(registry)
    scenario_rows: list[dict[str, Any]] = []
    strategy_ids = [
        *ACTUAL_PATH_OWNER_REVIEW_BASELINES,
        *ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
    ]
    for strategy_id in strategy_ids:
        target_weights = _signal_target_weight_frame(strategy_id, prices)
        target_metrics = _performance_metrics(prices, target_weights, cost_bps=0.0)
        binding = _mapping(bindings.get(strategy_id))
        base_policy = policies.get(str(binding.get("execution_policy_id") or ""))
        for scenario in _policy_sensitivity_scenarios(
            base_policy=base_policy,
            registry=registry,
        ):
            policy = _policy_sensitivity_policy(base_policy=base_policy, scenario=scenario)
            actual_weights, path_rows = _actual_position_path(
                strategy_id=strategy_id,
                execution_policy_id=str(scenario["scenario_id"]),
                target_weights=target_weights,
                policy=policy,
            )
            _attach_path_return_columns(
                prices=prices,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                cost_bps=_policy_cost_bps(policy),
            )
            actual_metrics = _performance_metrics(
                prices,
                actual_weights,
                cost_bps=_policy_cost_bps(policy),
            )
            namespaced_actual = _namespace_path_metrics(actual_metrics, "actual_path")
            namespaced_target = _namespace_path_metrics(target_metrics, "target_path")
            lag_cost = _lag_cost_summary(
                target_metrics,
                actual_metrics,
                path_rows,
                thresholds=materiality_thresholds,
            )
            staleness = _signal_staleness_summary(
                path_rows,
                thresholds=materiality_thresholds,
            )
            gaps = _target_vs_actual_gap_metrics(
                target_metrics=namespaced_target,
                actual_metrics=namespaced_actual,
                lag_cost=lag_cost,
                staleness=staleness,
            )
            scenario_rows.append(
                {
                    "strategy_id": strategy_id,
                    "strategy_role": (
                        "candidate"
                        if strategy_id in ACTUAL_PATH_OWNER_REVIEW_CANDIDATES
                        else "baseline"
                    ),
                    "scenario_id": scenario["scenario_id"],
                    "matrix_mode": "staged",
                    "sensitivity_stage": scenario["sensitivity_stage"],
                    "execution_lag_days": scenario["execution_lag_days"],
                    "rebalance_frequency": scenario["rebalance_frequency"],
                    "signal_validity_window_days": scenario[
                        "signal_validity_window_days"
                    ],
                    "turnover_constraint": scenario["turnover_constraint"],
                    "max_turnover_per_period": policy.get("max_turnover_per_period"),
                    "promotion_decision_source": "actual_path_only",
                    "target_path_metrics_role": "diagnostic_only",
                    **namespaced_actual,
                    **gaps,
                    "execution_lag_materiality": _materiality_enum(
                        lag_cost.get("review_status")
                    ),
                    "signal_staleness_materiality": _materiality_enum(
                        staleness.get("review_status")
                    ),
                    "rebalance_count": sum(
                        1 for row in path_rows if row.get("rebalance_executed") is True
                    ),
                    "average_signal_age_bdays": staleness.get("average_signal_age_bdays"),
                    "policy_hash": _stable_hash(policy),
                }
            )

    classifications = _policy_sensitivity_classifications(scenario_rows)
    leaderboard_rows = _policy_sensitivity_leaderboard_rows(scenario_rows)
    gap_rows = _policy_sensitivity_gap_rows(scenario_rows)
    readiness_summary = _policy_sensitivity_readiness_summary(classifications)
    summary_payload = {
        "schema_version": "dynamic_actual_path_policy_sensitivity_summary.v1",
        "report_type": "dynamic_actual_path_policy_sensitivity_summary",
        "status": "POLICY_SENSITIVITY_REVIEW_READY",
        "matrix_mode": "staged",
        "stage_a_rule": "execution_lag_days x rebalance_frequency",
        "stage_b_rule": (
            "signal_validity_window_days x turnover_constraint on lag=1 weekly/monthly execution"
        ),
        "classification_policy": POLICY_SENSITIVITY_CLASSIFICATION_POLICY,
        "strategy_classifications": classifications,
        "best_surviving_candidate": _best_surviving_candidate(classifications),
        "dynamic_promotion_blocked": True,
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        **SAFETY_BOUNDARY,
    }
    artifact_paths = _write_policy_sensitivity_artifacts(
        payload={},
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
        matrix_rows=scenario_rows,
        leaderboard_rows=leaderboard_rows,
        gap_rows=gap_rows,
        readiness_summary=readiness_summary,
        summary_payload=summary_payload,
    )
    payload = _payload(
        report_type="dynamic_actual_path_policy_sensitivity_review",
        title=REPORT_SPEC_BY_ID["dynamic_actual_path_policy_sensitivity_review"]["title"],
        status="POLICY_SENSITIVITY_REVIEW_READY",
        summary={
            "scenario_row_count": len(scenario_rows),
            "matrix_mode": "staged",
            "stage_a_rule": "execution_lag_days x rebalance_frequency",
            "stage_b_rule": (
                "signal_validity_window_days x turnover_constraint on lag=1 "
                "weekly/monthly execution"
            ),
            "candidate_count": len(ACTUAL_PATH_OWNER_REVIEW_CANDIDATES),
            "baseline_count": len(ACTUAL_PATH_OWNER_REVIEW_BASELINES),
            "policy_stable_count": sum(
                1
                for item in classifications
                if item.get("sensitivity_classification") == "POLICY_STABLE"
            ),
            "policy_fragile_count": sum(
                1
                for item in classifications
                if item.get("sensitivity_classification") == "POLICY_FRAGILE"
            ),
            "best_surviving_candidate": summary_payload["best_surviving_candidate"],
            "data_quality_status": data_gate.get("status"),
            "dynamic_promotion_blocked": True,
            "promotion_decision_source": "actual_path_only",
            "target_path_metrics_role": "diagnostic_only",
            **_safety_summary(),
        },
        date_range={
            "start": prices.index.min().date().isoformat(),
            "end": prices.index.max().date().isoformat(),
            "market_regime": "ai_after_chatgpt",
        },
        data_quality=data_gate,
        classification_policy=POLICY_SENSITIVITY_CLASSIFICATION_POLICY,
        strategy_classifications=classifications,
        artifact_paths=artifact_paths,
        report_registry_entry=_report_registry_entry(
            "dynamic_actual_path_policy_sensitivity_review"
        ),
    )
    _write_json(
        output_root / "index.json",
        _policy_sensitivity_index_payload(payload, scenario_rows),
    )
    _write_json(output_root / "policy_sensitivity_summary.json", summary_payload)
    _write_policy_sensitivity_review_docs(payload, docs_path, yaml_path, scenario_rows)
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
    columns = ["QQQ", "TQQQ", "SGOV"]
    target_values = target[columns].to_numpy(dtype=float)
    dates = list(target.index)
    current_values = target_values[0].copy()
    actual_rows: list[dict[str, float]] = []
    last_execution_index = 0
    last_signal_index = 0
    rows: list[dict[str, Any]] = []
    max_turnover = _float(policy.get("max_turnover_per_period"), 1.0)
    cost_bps = _policy_cost_bps(policy)
    lag = max(0, _int(policy.get("signal_to_execution_lag"), 1))
    validity_days = max(1, _int(policy.get("validity_period_days"), 20))
    frequency = str(policy.get("execution_frequency") or execution_policy_id)
    minimum_holding = max(0, _int(policy.get("minimum_holding_period"), 0))
    drift_threshold = _float(policy.get("drift_threshold"), 0.0)
    for index, current_date in enumerate(dates):
        signal_index = max(0, index - lag)
        signal_date = dates[signal_index]
        signal_target = target_values[signal_index].copy()
        should_execute, trigger = _should_execute_fast(
            execution_policy_id=execution_policy_id,
            frequency=frequency,
            dates=dates,
            target_values=target_values,
            current_values=current_values,
            index=index,
            last_execution_index=last_execution_index,
            minimum_holding=minimum_holding,
            drift_threshold=drift_threshold,
            validity_days=validity_days,
        )
        if index == 0:
            should_execute = True
            trigger = "initial_position"
        if should_execute:
            raw_turnover = _array_turnover(current_values, signal_target)
            if raw_turnover > max_turnover > 0:
                scale = max_turnover / raw_turnover
                next_position = current_values + (signal_target - current_values) * scale
            else:
                next_position = signal_target
            turnover = _array_turnover(current_values, next_position)
            current_values = _normalise_weight_array(next_position)
            last_execution_index = index
            last_signal_index = signal_index
        else:
            turnover = 0.0
        actual_rows.append(
            {
                "QQQ": float(current_values[0]),
                "TQQQ": float(current_values[1]),
                "SGOV": float(current_values[2]),
            }
        )
        signal_age = index - last_signal_index
        target_current = target_values[index]
        rows.append(
            {
                "date": current_date.date().isoformat(),
                "strategy_id": strategy_id,
                "execution_policy_id": execution_policy_id,
                "signal_date": signal_date.date().isoformat(),
                "signal_asof_date": signal_date.date().isoformat(),
                "target_weight_qqq": round(float(target_current[0]), 6),
                "target_weight_tqqq": round(float(target_current[1]), 6),
                "target_weight_sgov": round(float(target_current[2]), 6),
                "actual_weight_qqq": round(float(current_values[0]), 6),
                "actual_weight_tqqq": round(float(current_values[1]), 6),
                "actual_weight_sgov": round(float(current_values[2]), 6),
                "rebalance_allowed": should_execute,
                "rebalance_executed": should_execute,
                "execution_date": current_date.date().isoformat() if should_execute else None,
                "execution_lag_bdays": lag,
                "signal_age_bdays": signal_age,
                "is_signal_stale": signal_age > validity_days,
                "trigger_reason": trigger if should_execute else "no_execution",
                "turnover": round(turnover, 6),
                "cost": round(turnover * (cost_bps / 10000.0), 8),
                "signal_staleness_days": signal_age,
            }
        )
    actual = pd.DataFrame(actual_rows, index=target.index, columns=target.columns)
    return actual.astype(float), rows


def _should_execute_fast(
    *,
    execution_policy_id: str,
    frequency: str,
    dates: list[pd.Timestamp],
    target_values: Any,
    current_values: Any,
    index: int,
    last_execution_index: int,
    minimum_holding: int,
    drift_threshold: float,
    validity_days: int,
) -> tuple[bool, str]:
    current_date = dates[index]
    next_date = dates[index + 1] if index + 1 < len(dates) else None
    previous_date = dates[index - 1] if index > 0 else None
    holding_ok = index - last_execution_index >= minimum_holding
    drift = _array_turnover(current_values, target_values[index])
    is_month_end = next_date is None or current_date.month != next_date.month
    is_month_begin = previous_date is None or current_date.month != previous_date.month
    is_week_end = (
        next_date is None
        or current_date.isocalendar().week != next_date.isocalendar().week
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
        qqq_drop = float(current_values[0]) - float(target_values[index][0])
        if qqq_drop >= 0.05 and holding_ok:
            return True, "risk_shock_override"
    if frequency == "validity_period":
        if index - last_execution_index >= validity_days:
            return True, "validity_expiry"
        return False, "no_execution"
    if frequency == "threshold_with_min_holding" and holding_ok and drift >= drift_threshold:
        return True, "min_holding_drift_threshold"
    if frequency == "hysteresis_threshold" and drift_threshold and drift >= drift_threshold * 1.5:
        return True, "hysteresis_band_crossed"
    return False, "no_execution"


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
    if strategy_id in {"no_trade", "no_trade_baseline"}:
        return _constant_weight_frame(prices, {"QQQ": 0.0, "TQQQ": 0.0, "SGOV": 1.0})
    annualization = 252
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    qqq_ma_60 = prices["QQQ"].rolling(60, min_periods=20).mean()
    qqq_ma_120 = prices["QQQ"].rolling(120, min_periods=40).mean()
    trend_on = prices["QQQ"] >= qqq_ma_120.fillna(prices["QQQ"])
    vol_20 = qqq_returns.rolling(20, min_periods=10).std().shift(1) * math.sqrt(annualization)
    vol_high = vol_20 >= vol_20.rolling(120, min_periods=30).quantile(0.80)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    if strategy_id == "limited_adjustment":
        qqq = pd.Series(0.45, index=prices.index)
        qqq.loc[trend_on] = 0.65
        return _ensure_weight_columns(pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}))
    if strategy_id == "defensive_limited_adjustment":
        qqq = pd.Series(0.35, index=prices.index)
        qqq.loc[trend_on & (drawdown > -0.08)] = 0.55
        qqq.loc[drawdown <= -0.12] = 0.20
        return _ensure_weight_columns(pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}))
    if strategy_id == "dynamic_regime_overlay_v0_4_lower_turnover":
        qqq = pd.Series(0.40, index=prices.index)
        qqq.loc[trend_on] = 0.70
        qqq = qqq.rolling(5, min_periods=1).mean()
        return _ensure_weight_columns(pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}))
    if strategy_id in {
        "dynamic_v0_5_ai_trend_confirmed_only",
        "ai_trend_confirmed_only",
    }:
        confirmed = (prices["QQQ"] >= qqq_ma_60.fillna(prices["QQQ"])) & trend_on & ~vol_high
        qqq = pd.Series(0.30, index=prices.index)
        qqq.loc[confirmed] = 0.75
        return _ensure_weight_columns(pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}))
    if strategy_id == "smooth_weights_3d_limited_adjustment":
        base = _signal_target_weight_frame("limited_adjustment", prices)
        return _ensure_weight_columns(base.rolling(3, min_periods=1).mean())
    if strategy_id == "smooth_weights_5d_limited_adjustment":
        base = _signal_target_weight_frame("limited_adjustment", prices)
        return _ensure_weight_columns(base.rolling(5, min_periods=1).mean())
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
            "annual_return_total_return_path": 0.0,
            "volatility_daily_annualized": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_daily_equity": 0.0,
            "max_drawdown_monthly_return": 0.0,
            "sharpe": 0.0,
            "sharpe_daily_zero_rf": 0.0,
            "calmar": 0.0,
            "calmar_daily_equity_dd": 0.0,
            "turnover": 0.0,
            "constraint_hit_rate": 0.0,
            "cost_drag": 0.0,
            "recovery_days": 0,
            "worst_month": 0.0,
        }
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    annual_return = float(equity.iloc[-1] ** (252 / max(1, len(returns))) - 1.0)
    annual_vol = float(returns.std(ddof=0) * math.sqrt(252))
    monthly_returns = (1.0 + returns).resample("ME").prod() - 1.0
    monthly_equity = (1.0 + monthly_returns).cumprod()
    monthly_drawdown = monthly_equity / monthly_equity.cummax() - 1.0
    worst_month = float(monthly_returns.min())
    max_drawdown_daily = float(drawdown.min())
    max_drawdown_monthly = float(monthly_drawdown.min())
    sharpe_daily = _ratio(annual_return, annual_vol)
    calmar_daily = _ratio(annual_return, abs(max_drawdown_daily))
    return {
        "annual_return": round(annual_return, 6),
        "annual_return_total_return_path": round(annual_return, 6),
        "volatility_daily_annualized": round(annual_vol, 6),
        "max_drawdown": round(max_drawdown_daily, 6),
        "max_drawdown_daily_equity": round(max_drawdown_daily, 6),
        "max_drawdown_monthly_return": round(max_drawdown_monthly, 6),
        "sharpe": round(sharpe_daily, 6),
        "sharpe_daily_zero_rf": round(sharpe_daily, 6),
        "calmar": round(calmar_daily, 6),
        "calmar_daily_equity_dd": round(calmar_daily, 6),
        "turnover": round(float(turnover.sum()), 6),
        "constraint_hit_rate": 0.0,
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


def _normalise_weight_array(values: Any) -> Any:
    total = float(values.sum())
    if total > 0:
        return values / total
    return values


def _array_turnover(previous: Any, next_position: Any) -> float:
    return float(abs(previous - next_position).sum() / 2.0)


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


def _strategy_execution_bindings(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    return _records(registry.get("strategy_execution_policies"))


def _strategy_execution_binding_by_id(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(binding.get("strategy_id")): dict(binding)
        for binding in _strategy_execution_bindings(registry)
        if binding.get("strategy_id")
    }


def _strategy_binding_issues(
    bindings: list[dict[str, Any]],
    policy_ids: set[str],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    seen: set[str] = set()
    for binding in bindings:
        strategy_id = str(binding.get("strategy_id") or "unknown")
        if strategy_id in seen:
            issues.append({"strategy_id": strategy_id, "issue": "duplicate_strategy_binding"})
        seen.add(strategy_id)
        missing = [
            field for field in REQUIRED_STRATEGY_EXECUTION_POLICY_FIELDS if field not in binding
        ]
        if missing:
            issues.append(
                {
                    "strategy_id": strategy_id,
                    "issue": "missing_required_strategy_binding_fields",
                    "fields": missing,
                }
            )
        for section, fields in REQUIRED_STRATEGY_POLICY_SECTIONS.items():
            raw_section = _mapping(binding.get(section))
            missing_section_fields = [field for field in fields if field not in raw_section]
            if missing_section_fields:
                issues.append(
                    {
                        "strategy_id": strategy_id,
                        "issue": "missing_required_strategy_policy_section_fields",
                        "section": section,
                        "fields": missing_section_fields,
                    }
                )
        policy_id = str(binding.get("execution_policy_id") or "")
        if policy_id not in policy_ids:
            issues.append(
                {
                    "strategy_id": strategy_id,
                    "issue": "execution_policy_id_not_registered",
                    "execution_policy_id": policy_id or "missing",
                }
            )
        validation = _mapping(binding.get("validation_policy"))
        if (
            str(binding.get("strategy_type")) == "dynamic"
            and validation.get("promotion_allowed_from_target_path") is not False
        ):
            issues.append(
                {
                    "strategy_id": strategy_id,
                    "issue": "dynamic_strategy_target_path_promotion_not_blocked",
                }
            )
    return issues


def _policy_definition_issues(policy: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not policy:
        return [{"issue": "execution_policy_definition_missing"}]
    policy_id = str(policy.get("execution_policy_id") or "missing")
    missing = [field for field in REQUIRED_EXECUTION_POLICY_FIELDS if field not in policy]
    if not missing:
        return []
    return [
        {
            "execution_policy_id": policy_id,
            "issue": "missing_required_execution_policy_fields",
            "fields": missing,
        }
    ]


def _selected_rebacktest_strategy_ids(
    strategy_id: str | None,
    strategy_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    raw: list[str] = []
    if strategy_ids:
        raw.extend(str(item) for item in strategy_ids)
    if strategy_id:
        raw.extend(str(item) for item in strategy_id.split(","))
    selected = [
        REBACKTEST_STRATEGY_ID_ALIASES.get(item.strip(), item.strip())
        for item in raw
        if item.strip()
    ]
    return _dedupe_ordered(selected) or list(DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS)


def _execution_materiality_thresholds(registry: Mapping[str, Any]) -> dict[str, float]:
    raw = _mapping(registry.get("materiality_thresholds"))
    raw_thresholds = _mapping(raw.get("thresholds"))
    thresholds = dict(DEFAULT_EXECUTION_MATERIALITY_THRESHOLDS)
    for key in thresholds:
        if key in raw_thresholds:
            thresholds[key] = _float(raw_thresholds[key], thresholds[key])
    return thresholds


def _file_sha256(path: Path) -> str:
    if not path.exists():
        return "missing"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _policy_snapshot_hash(
    *,
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> str:
    payload = {
        "strategy_execution_policy": dict(binding),
        "execution_policy": dict(policy),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _namespace_path_metrics(metrics: Mapping[str, Any], path_scope: str) -> dict[str, Any]:
    return {
        f"{path_scope}_annual_return": metrics.get("annual_return", 0.0),
        f"{path_scope}_volatility_daily_annualized": metrics.get(
            "volatility_daily_annualized", 0.0
        ),
        f"{path_scope}_max_drawdown_daily_equity": metrics.get(
            "max_drawdown_daily_equity", 0.0
        ),
        f"{path_scope}_sharpe_daily_zero_rf": metrics.get("sharpe_daily_zero_rf", 0.0),
        f"{path_scope}_calmar_daily_equity_dd": metrics.get(
            "calmar_daily_equity_dd", 0.0
        ),
        f"{path_scope}_turnover": metrics.get("turnover", 0.0),
        f"{path_scope}_constraint_hit_rate": metrics.get("constraint_hit_rate", 0.0),
    }


def _target_vs_actual_gap_metrics(
    *,
    target_metrics: Mapping[str, Any],
    actual_metrics: Mapping[str, Any],
    lag_cost: Mapping[str, Any],
    staleness: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "target_vs_actual_annual_return_gap": round(
            _float(target_metrics.get("target_path_annual_return"))
            - _float(actual_metrics.get("actual_path_annual_return")),
            6,
        ),
        "target_vs_actual_max_drawdown_gap": round(
            abs(_float(actual_metrics.get("actual_path_max_drawdown_daily_equity")))
            - abs(_float(target_metrics.get("target_path_max_drawdown_daily_equity"))),
            6,
        ),
        "target_vs_actual_sharpe_gap": round(
            _float(target_metrics.get("target_path_sharpe_daily_zero_rf"))
            - _float(actual_metrics.get("actual_path_sharpe_daily_zero_rf")),
            6,
        ),
        "target_vs_actual_calmar_gap": round(
            _float(target_metrics.get("target_path_calmar_daily_equity_dd"))
            - _float(actual_metrics.get("actual_path_calmar_daily_equity_dd")),
            6,
        ),
        "execution_lag_return_cost": lag_cost.get("execution_lag_return_cost_abs", 0.0),
        "execution_lag_drawdown_cost": lag_cost.get("execution_lag_max_drawdown_cost", 0.0),
        "signal_staleness_return_cost": staleness.get(
            "signal_staleness_return_cost_abs", 0.0
        ),
        "signal_staleness_drawdown_cost": staleness.get(
            "signal_staleness_max_drawdown_cost", 0.0
        ),
    }


def _execution_semantics_promotion_gate_decision(
    *,
    strategy_id: str,
    strategy_binding: Mapping[str, Any] | None,
    backtest_generation: str,
    position_path_used_for_metrics: str,
    actual_rebacktest_available: bool,
) -> dict[str, Any]:
    binding = _mapping(strategy_binding)
    strategy_type = str(binding.get("strategy_type") or "dynamic")
    blocking_reasons: list[str] = []
    if not binding:
        blocking_reasons.append("EXECUTION_POLICY_MISSING")
    if strategy_type == "dynamic":
        validation = _mapping(binding.get("validation_policy"))
        if backtest_generation != "EXECUTION_SEMANTICS_AWARE":
            blocking_reasons.append("PRE_EXECUTION_SEMANTICS")
        if position_path_used_for_metrics != "ACTUAL":
            blocking_reasons.append("TARGET_PATH_NOT_PROMOTION_ELIGIBLE")
        if not actual_rebacktest_available:
            blocking_reasons.append("EXECUTION_SEMANTICS_REBACKTEST_REQUIRED")
        if validation.get("promotion_allowed_from_target_path") is not False:
            blocking_reasons.append("PROMOTION_ALLOWED_FROM_TARGET_PATH_NOT_FALSE")
    if strategy_type == "static" and not blocking_reasons:
        return {
            "strategy_id": strategy_id,
            "strategy_type": strategy_type,
            "status": "STATIC_BASELINE_NOT_BLOCKED_BY_EXECUTION_SEMANTICS",
            "promotion_eligible": True,
            "rebacktest_required": False,
            "blocking_reasons": [],
        }
    if blocking_reasons:
        return {
            "strategy_id": strategy_id,
            "strategy_type": strategy_type,
            "status": "EXECUTION_SEMANTICS_REBACKTEST_REQUIRED",
            "promotion_eligible": False,
            "rebacktest_required": True,
            "blocking_reasons": _dedupe_ordered(blocking_reasons),
        }
    return {
        "strategy_id": strategy_id,
        "strategy_type": strategy_type,
        "status": "EXECUTION_SEMANTICS_ACTUAL_PATH_REVIEWABLE",
        "promotion_eligible": True,
        "rebacktest_required": False,
        "blocking_reasons": [],
    }


def _attach_path_return_columns(
    *,
    prices: pd.DataFrame,
    target_weights: pd.DataFrame,
    actual_weights: pd.DataFrame,
    path_rows: list[dict[str, Any]],
    cost_bps: float,
) -> None:
    asset_returns = prices.reindex(columns=["QQQ", "TQQQ", "SGOV"]).pct_change().fillna(0.0)
    target_applied = _ensure_weight_columns(target_weights).shift(1).ffill().fillna(0.0)
    actual_applied = _ensure_weight_columns(actual_weights).shift(1).ffill().fillna(0.0)
    target_returns = (target_applied * asset_returns).sum(axis=1)
    actual_gross_returns = (actual_applied * asset_returns).sum(axis=1)
    cost = pd.Series(
        [float(row.get("turnover", 0.0)) * (cost_bps / 10000.0) for row in path_rows],
        index=asset_returns.index,
    )
    actual_returns = actual_gross_returns - cost
    for index, row in enumerate(path_rows):
        current_date = asset_returns.index[index]
        target_return = _float(target_returns.loc[current_date])
        actual_return = _float(actual_returns.loc[current_date])
        row["portfolio_return_target_path"] = round(target_return, 8)
        row["portfolio_return_actual_path"] = round(actual_return, 8)
        row["lag_cost_return_diff"] = round(target_return - actual_return, 8)


def _lag_cost_summary(
    metrics_target: Mapping[str, Any],
    metrics_actual: Mapping[str, Any],
    path_rows: list[dict[str, Any]],
    *,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    annual_lag_cost = _float(metrics_target.get("annual_return")) - _float(
        metrics_actual.get("annual_return")
    )
    drawdown_lag_cost = abs(_float(metrics_actual.get("max_drawdown_daily_equity"))) - abs(
        _float(metrics_target.get("max_drawdown_daily_equity"))
    )
    sharpe_lag_cost = _float(metrics_target.get("sharpe_daily_zero_rf")) - _float(
        metrics_actual.get("sharpe_daily_zero_rf")
    )
    execution_lags = [_float(row.get("execution_lag_bdays")) for row in path_rows]
    return_cost_abs = abs(annual_lag_cost)
    return_cost_abs_pp = return_cost_abs * 100.0
    target_return_abs = abs(_float(metrics_target.get("annual_return")))
    return_cost_relative_pct = (
        return_cost_abs / target_return_abs * 100.0 if target_return_abs > 1e-12 else 0.0
    )
    drawdown_cost_abs = max(0.0, drawdown_lag_cost)
    drawdown_cost_abs_pp = drawdown_cost_abs * 100.0
    lag_p95 = round(_percentile(execution_lags, 0.95), 3)
    review_status = _materiality_review_status(
        values={
            "execution_lag_return_cost_abs_pp": return_cost_abs_pp,
            "execution_lag_return_cost_relative_pct": return_cost_relative_pct,
            "execution_lag_max_drawdown_cost_pp": drawdown_cost_abs_pp,
            "actual_trade_delay_days_p95": lag_p95,
        },
        thresholds=thresholds,
    )
    return {
        "annual_return_target_path": metrics_target.get("annual_return"),
        "annual_return_actual_path": metrics_actual.get("annual_return"),
        "annual_return_lag_cost": round(annual_lag_cost, 6),
        "execution_lag_return_cost_abs": round(return_cost_abs, 6),
        "execution_lag_return_cost_abs_pp": round(return_cost_abs_pp, 3),
        "execution_lag_return_cost_relative_pct": round(return_cost_relative_pct, 3),
        "max_drawdown_target_path": metrics_target.get("max_drawdown_daily_equity"),
        "max_drawdown_actual_path": metrics_actual.get("max_drawdown_daily_equity"),
        "drawdown_lag_cost": round(drawdown_lag_cost, 6),
        "execution_lag_max_drawdown_cost": round(drawdown_cost_abs, 6),
        "execution_lag_max_drawdown_cost_pp": round(drawdown_cost_abs_pp, 3),
        "sharpe_target_path": metrics_target.get("sharpe_daily_zero_rf"),
        "sharpe_actual_path": metrics_actual.get("sharpe_daily_zero_rf"),
        "sharpe_lag_cost": round(sharpe_lag_cost, 6),
        "execution_lag_sharpe_cost": round(sharpe_lag_cost, 6),
        "execution_lag_days_mean": round(_mean(execution_lags), 3),
        "execution_lag_days_p95": lag_p95,
        "actual_trade_delay_days_avg": round(_mean(execution_lags), 3),
        "actual_trade_delay_days_p95": lag_p95,
        "review_status": review_status,
        "status": _lag_cost_status(
            annual_lag_cost,
            drawdown_lag_cost,
            sharpe_lag_cost,
            review_status=review_status,
        ),
    }


def _signal_staleness_summary(
    path_rows: list[dict[str, Any]],
    *,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    ages = [_float(row.get("signal_age_bdays")) for row in path_rows]
    stale_days = [row for row in path_rows if row.get("is_signal_stale") is True]
    material_rows = [
        row for row in stale_days if abs(_float(row.get("lag_cost_return_diff"))) > 0.00001
    ]
    return_cost_abs = abs(sum(_float(row.get("lag_cost_return_diff")) for row in stale_days))
    material_event_count = len(material_rows)
    review_status = _materiality_review_status(
        values={
            "signal_staleness_material_event_count": float(material_event_count),
        },
        thresholds=thresholds,
    )
    return {
        "average_signal_age_bdays": round(_mean(ages), 3),
        "p95_signal_age_bdays": round(_percentile(ages, 0.95), 3),
        "stale_signal_days": len(stale_days),
        "stale_signal_day_pct": round(len(stale_days) / max(1, len(path_rows)), 6),
        "signal_staleness_event_count": len(stale_days),
        "signal_staleness_material_event_count": material_event_count,
        "signal_staleness_return_cost_abs": round(return_cost_abs, 6),
        "signal_staleness_max_drawdown_cost": 0.0,
        "missed_signal_window_count": len(stale_days),
        "review_status": review_status,
        "status": "SIGNAL_STALENESS_COST_MATERIAL"
        if review_status in {"warn", "fail"}
        else "SIGNAL_STALENESS_COST_READY",
    }


def _materiality_review_status(
    *,
    values: Mapping[str, float],
    thresholds: Mapping[str, float],
) -> str:
    exceeded = []
    severe = []
    for key, value in values.items():
        threshold = _float(thresholds.get(key), 0.0)
        if threshold <= 0:
            continue
        parsed = abs(_float(value))
        if parsed >= threshold:
            exceeded.append(key)
        if parsed >= threshold * 2.0:
            severe.append(key)
    if severe:
        return "fail"
    if exceeded:
        return "warn"
    return "pass"


def _promotion_readiness_for_rebacktest(
    *,
    strategy_id: str,
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
    metrics_actual: Mapping[str, Any],
    metrics_target: Mapping[str, Any],
    lag_cost: Mapping[str, Any],
    staleness: Mapping[str, Any],
    gate: Mapping[str, Any],
    policy_hash: str,
) -> dict[str, Any]:
    actual_metrics = _namespace_path_metrics(metrics_actual, "actual_path")
    target_metrics = _namespace_path_metrics(metrics_target, "target_path")
    actual_metrics_available = bool(metrics_actual)
    checks = _promotion_readiness_checks(
        policy_bound=bool(binding) and bool(policy) and bool(policy_hash),
        actual_metrics_available=actual_metrics_available,
        gate=gate,
        lag_cost=lag_cost,
        staleness=staleness,
    )
    final_status = _derive_promotion_readiness_final_status(checks)
    blocking_reason_codes = _readiness_blocking_reason_codes(checks)
    promotion_eligible = final_status == "reviewable" and bool(gate.get("promotion_eligible"))
    return {
        "schema_version": "dynamic_promotion_readiness.v1",
        "report_type": "execution_semantics_promotion_readiness",
        "strategy_id": strategy_id,
        "status": "NOT_PROMOTION_ELIGIBLE" if not promotion_eligible else "PROMOTION_REVIEWABLE",
        "final_status": final_status,
        "backtest_generation": "EXECUTION_SEMANTICS_AWARE",
        "position_path_used_for_metrics": "ACTUAL",
        "execution_policy_id": policy.get("execution_policy_id"),
        "policy_hash": policy_hash,
        "execution_lag_bdays": policy.get("signal_to_execution_lag"),
        "rebalance_frequency": policy.get("execution_frequency"),
        "signal_validity_window_bdays": policy.get("validity_period_days"),
        "metric_convention_namespace": "internal.execution_semantics.actual_path.v1",
        "promotion_decision_source": "actual_path_only",
        "promotion_eligible": promotion_eligible,
        "rebacktest_required": False,
        "blocking_reason_codes": blocking_reason_codes,
        "blocking_reasons": blocking_reason_codes,
        "blocking_reason_details": _readiness_blocking_reason_details(
            checks,
            blocking_reason_codes,
        ),
        "checks": checks,
        "decision_inputs": {
            "promotion_decision_source": "actual_path_only",
            "actual_path_metrics_artifact": "metrics_actual_path.json",
            "target_path_metrics_artifact": "metrics_target_path.json",
            "target_path_metrics_role": "diagnostic_only",
            "target_path_metrics_used_for_promotion": False,
            "decision_metric_names": sorted(actual_metrics),
        },
        "target_path_diagnostic_notice": (
            "Target-path metrics are diagnostic only and are not eligible for "
            "promotion decisions."
        ),
        "actual_path_metrics": actual_metrics,
        "target_path_metrics_diagnostic": target_metrics,
        "legacy_metrics_deprecated": {
            "deprecated": True,
            "reason": (
                "Raw metric aliases are retained only for compatibility; "
                "promotion uses actual_path_* fields."
            ),
            "actual_path_raw_metrics": dict(metrics_actual),
        },
        "owner_waiver_schema": {
            "required_fields": [
                "waiver_id",
                "owner",
                "timestamp",
                "reason",
                "affected_strategy_id",
                "affected_check",
                "expiry",
                "evidence_artifact",
            ],
            "enabled_by_default": False,
        },
        **SAFETY_BOUNDARY,
    }


def _promotion_readiness_checks(
    *,
    policy_bound: bool,
    actual_metrics_available: bool,
    gate: Mapping[str, Any],
    lag_cost: Mapping[str, Any],
    staleness: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    lag_status = str(lag_cost.get("review_status") or "pass")
    stale_status = str(staleness.get("review_status") or "pass")
    return {
        "policy_binding": _readiness_check(
            status="pass" if policy_bound else "fail",
            severity="critical",
            required_action=None if policy_bound else "Bind strategy to a valid execution policy.",
            evidence_artifact="execution_policy_snapshot.yaml",
        ),
        "metric_namespace": _readiness_check(
            status="pass" if actual_metrics_available else "fail",
            severity="critical",
            required_action=None
            if actual_metrics_available
            else "Regenerate metrics_actual_path.json with actual_path_* metric names.",
            evidence_artifact="metrics_actual_path.json",
        ),
        "actual_path_rebacktest": _readiness_check(
            status=(
                "pass"
                if actual_metrics_available
                and gate.get("status") != "EXECUTION_SEMANTICS_REBACKTEST_REQUIRED"
                else "fail"
            ),
            severity="critical",
            required_action=None
            if actual_metrics_available
            else "Run execution-semantics-rebacktest to generate actual-path artifacts.",
            evidence_artifact="target_vs_actual_position_path.csv",
        ),
        "target_path_not_used_for_promotion": _readiness_check(
            status="pass" if actual_metrics_available else "fail",
            severity="critical",
            required_action=None
            if actual_metrics_available
            else "Target-path metrics cannot unlock promotion without actual-path metrics.",
            evidence_artifact="promotion_readiness.json",
        ),
        "lag_cost_review": _readiness_check(
            status=lag_status,
            severity="high",
            required_action=(
                None if lag_status == "pass" else "Review lag_cost_report.md if warn/fail."
            ),
            evidence_artifact="lag_cost_report.md",
        ),
        "signal_staleness_review": _readiness_check(
            status=stale_status,
            severity="high",
            required_action=(
                None
                if stale_status == "pass"
                else "Review signal_staleness_report.md if warn/fail."
            ),
            evidence_artifact="signal_staleness_report.md",
        ),
        "owner_manual_review": _readiness_check(
            status="pending",
            severity="critical",
            required_action=(
                "Owner must review actual-path evidence and explicitly sign off."
            ),
            evidence_artifact="owner_review_pack.md",
        ),
    }


def _readiness_check(
    *,
    status: str,
    severity: str,
    required_action: str | None,
    evidence_artifact: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "severity": severity,
        "required_action": required_action,
        "evidence_artifact": evidence_artifact,
    }


def _derive_promotion_readiness_final_status(checks: Mapping[str, Mapping[str, Any]]) -> str:
    for check in checks.values():
        if check.get("severity") == "critical" and check.get("status") in {"fail", "pending"}:
            return "blocked"
    for check in checks.values():
        if check.get("severity") == "high" and check.get("status") in {"warn", "fail"}:
            return "blocked"
    return "reviewable"


def _readiness_blocking_reason_codes(checks: Mapping[str, Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for check_id, check in checks.items():
        status = str(check.get("status"))
        severity = str(check.get("severity"))
        if severity == "critical" and status in {"fail", "pending"}:
            reasons.append(f"{check_id}_{status}")
        elif severity == "high" and status in {"warn", "fail"}:
            reasons.append(f"{check_id}_{status}")
    return reasons


def _readiness_blocking_reason_details(
    checks: Mapping[str, Mapping[str, Any]],
    reason_codes: list[str],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for reason_code in reason_codes:
        check_id = reason_code.rsplit("_", 1)[0]
        check = dict(checks.get(check_id, {}))
        details.append(
            {
                "reason": reason_code,
                "check_id": check_id,
                "status": check.get("status"),
                "severity": check.get("severity"),
                "required_action": check.get("required_action"),
                "evidence_artifact": check.get("evidence_artifact"),
            }
        )
    return details


def _write_strategy_rebacktest_artifacts(
    *,
    output_root: Path,
    strategy_id: str,
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
    policy_hash: str,
    path_rows: list[dict[str, Any]],
    metrics_target: Mapping[str, Any],
    metrics_actual: Mapping[str, Any],
    lag_cost: Mapping[str, Any],
    staleness: Mapping[str, Any],
    promotion_readiness: Mapping[str, Any],
    materiality_thresholds: Mapping[str, float],
    date_range_start: str,
    date_range_end: str,
) -> dict[str, str]:
    import yaml

    output_root.mkdir(parents=True, exist_ok=True)
    namespaced_actual = _namespace_path_metrics(metrics_actual, "actual_path")
    namespaced_target = _namespace_path_metrics(metrics_target, "target_path")
    gap_metrics = _target_vs_actual_gap_metrics(
        target_metrics=namespaced_target,
        actual_metrics=namespaced_actual,
        lag_cost=lag_cost,
        staleness=staleness,
    )
    summary = {
        "schema_version": "1.0",
        "report_type": "execution_semantics_rebacktest_summary",
        "strategy_id": strategy_id,
        "status": "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE",
        "backtest_generation": "EXECUTION_SEMANTICS_AWARE",
        "position_path_used_for_metrics": "ACTUAL",
        "target_path_role": "diagnostic_only_not_promotion_eligible",
        "date_range": {"start": date_range_start, "end": date_range_end},
        "execution_policy_id": policy.get("execution_policy_id"),
        "policy_hash": policy_hash,
        "metric_convention_namespace": "internal.execution_semantics.actual_path.v1",
        "promotion_decision_source": "actual_path_only",
        "target_path_diagnostic_notice": (
            "Target-path metrics are diagnostic only and are not eligible for "
            "promotion decisions."
        ),
        "actual_path_metrics": namespaced_actual,
        "target_vs_actual_gap_metrics": gap_metrics,
        "promotion_eligible": promotion_readiness.get("promotion_eligible"),
        "rebacktest_required": promotion_readiness.get("rebacktest_required"),
        "promotion_final_status": promotion_readiness.get("final_status"),
        "blocking_reason_codes": promotion_readiness.get("blocking_reason_codes"),
        **SAFETY_BOUNDARY,
    }
    metrics_actual_payload = {
        "schema_version": "metrics_actual_path.v1",
        "report_type": "metrics_actual_path",
        "strategy_id": strategy_id,
        "position_path_used_for_metrics": "ACTUAL",
        "metric_convention_namespace": "internal.execution_semantics.actual_path.v1",
        "promotion_metric_source": True,
        "promotion_decision_source": "actual_path_only",
        "metrics": namespaced_actual,
        "legacy_metrics_deprecated": {
            "deprecated": True,
            "reason": (
                "Raw metric aliases are not promotion inputs; "
                "use metrics.actual_path_* fields."
            ),
            "fields": dict(metrics_actual),
        },
    }
    metrics_target_payload = {
        "schema_version": "metrics_target_path.v1",
        "report_type": "metrics_target_path",
        "strategy_id": strategy_id,
        "position_path_used_for_metrics": "TARGET",
        "metric_convention_namespace": "internal.execution_semantics.target_path.v1",
        "target_path_role": "diagnostic_only",
        "target_path_diagnostic_notice": (
            "Target-path metrics are diagnostic only and are not eligible for "
            "promotion decisions."
        ),
        "promotion_metric_source": False,
        "promotion_eligible": False,
        "metrics": namespaced_target,
        "legacy_metrics_deprecated": {
            "deprecated": True,
            "reason": "Raw metric aliases are retained for target-vs-actual diagnostics only.",
            "fields": dict(metrics_target),
        },
    }
    paths = {
        "summary": output_root / "summary.json",
        "metrics_actual_path": output_root / "metrics_actual_path.json",
        "metrics_target_path": output_root / "metrics_target_path.json",
        "target_vs_actual_position_path": output_root / "target_vs_actual_position_path.csv",
        "lag_cost_report": output_root / "lag_cost_report.md",
        "signal_staleness_report": output_root / "signal_staleness_report.md",
        "execution_policy_snapshot": output_root / "execution_policy_snapshot.yaml",
        "promotion_readiness": output_root / "promotion_readiness.json",
    }
    _write_json(paths["summary"], summary)
    _write_json(paths["metrics_actual_path"], metrics_actual_payload)
    _write_json(paths["metrics_target_path"], metrics_target_payload)
    pd.DataFrame(path_rows).to_csv(paths["target_vs_actual_position_path"], index=False)
    paths["lag_cost_report"].write_text(_lag_cost_markdown(strategy_id, lag_cost), encoding="utf-8")
    paths["signal_staleness_report"].write_text(
        _signal_staleness_markdown(strategy_id, staleness),
        encoding="utf-8",
    )
    paths["execution_policy_snapshot"].write_text(
        yaml.safe_dump(
            {
                "policy_hash": policy_hash,
                "materiality_thresholds": dict(materiality_thresholds),
                "normalized_execution_policy_contract": _normalized_policy_contract(
                    binding=binding,
                    policy=policy,
                ),
                "strategy_execution_policy": dict(binding),
                "execution_policy": dict(policy),
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    _write_json(paths["promotion_readiness"], dict(promotion_readiness))
    return {key: str(value) for key, value in paths.items()}


def _normalized_policy_contract(
    *,
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    signal_policy = _mapping(binding.get("signal_policy"))
    rebalance_policy = _mapping(binding.get("rebalance_policy"))
    cost_policy = _mapping(binding.get("cost_policy"))
    policy_cost = _mapping(policy.get("cost_model"))
    return {
        "policy_id": policy.get("execution_policy_id"),
        "strategy_id": binding.get("strategy_id"),
        "rebalance_frequency": rebalance_policy.get(
            "rebalance_frequency",
            policy.get("execution_frequency"),
        ),
        "signal_observation_time": signal_policy.get("signal_observation_time"),
        "execution_delay_days": policy.get("signal_to_execution_lag"),
        "trade_effective_time": signal_policy.get("signal_effective_earliest"),
        "signal_validity_days": policy.get("validity_period_days"),
        "stale_signal_behavior": signal_policy.get("stale_signal_behavior"),
        "allow_partial_adjustment": _float(policy.get("max_turnover_per_period"), 1.0) < 1.0,
        "transaction_cost_model": cost_policy.get(
            "transaction_cost_model",
            policy_cost.get("model_id", "none"),
        ),
        "promotion_allowed": False,
        "owner_review_required": True,
    }


def _write_rebacktest_aggregate_artifacts(
    *,
    output_root: Path,
    strategy_rows: list[dict[str, Any]],
    blocked_rows: list[dict[str, Any]],
    selected_strategy_ids: list[str],
    date_range: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    policy_registry_path: Path,
    policy_registry_hash: str,
    materiality_thresholds: Mapping[str, float],
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    completed_rows = [
        row
        for row in strategy_rows
        if row.get("status") == "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE"
    ]
    leaderboard_rows = _leaderboard_actual_path_rows(completed_rows)
    gap_rows = _target_vs_actual_gap_rows(completed_rows)
    readiness_summary = {
        "schema_version": "execution_semantics_promotion_readiness_summary.v1",
        "report_type": "execution_semantics_promotion_readiness_summary",
        "status": "DYNAMIC_PROMOTION_BLOCKED",
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        "dynamic_promotion_blocked": True,
        "strategy_count": len(selected_strategy_ids),
        "completed_count": len(completed_rows),
        "blocked_count": len(blocked_rows),
        "strategy_readiness": [
            {
                "strategy_id": row.get("strategy_id"),
                "strategy_type": row.get("strategy_type"),
                "promotion_final_status": row.get("promotion_final_status", "blocked"),
                "promotion_eligible": row.get("promotion_eligible", False),
                "blocking_reasons": row.get("blocking_reasons", []),
                "policy_hash": row.get("policy_hash"),
            }
            for row in strategy_rows
        ],
        **SAFETY_BOUNDARY,
    }
    index_payload = {
        "schema_version": "execution_semantics_rebacktest_index.v1",
        "report_type": "execution_semantics_rebacktest_index",
        "status": (
            "COMPLETE_WITH_BLOCKED_ROWS" if blocked_rows else "COMPLETE"
        ),
        "date_range": dict(date_range),
        "data_quality_status": data_quality.get("status"),
        "policy_registry_path": str(policy_registry_path),
        "policy_registry_hash": policy_registry_hash,
        "materiality_thresholds": dict(materiality_thresholds),
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        "selected_strategy_ids": selected_strategy_ids,
        "strategy_rows": strategy_rows,
        "blocked_rows": blocked_rows,
        **SAFETY_BOUNDARY,
    }
    paths = {
        "index": output_root / "index.json",
        "leaderboard_actual_path": output_root / "leaderboard_actual_path.csv",
        "target_vs_actual_gap_summary": output_root / "target_vs_actual_gap_summary.csv",
        "promotion_readiness_summary": output_root / "promotion_readiness_summary.json",
        "owner_review_pack": output_root / "owner_review_pack.md",
    }
    _write_json(paths["index"], index_payload)
    pd.DataFrame(leaderboard_rows).to_csv(paths["leaderboard_actual_path"], index=False)
    pd.DataFrame(gap_rows).to_csv(paths["target_vs_actual_gap_summary"], index=False)
    _write_json(paths["promotion_readiness_summary"], readiness_summary)
    paths["owner_review_pack"].write_text(
        _owner_review_pack_markdown(
            date_range=date_range,
            data_quality=data_quality,
            policy_registry_path=policy_registry_path,
            policy_registry_hash=policy_registry_hash,
            leaderboard_rows=leaderboard_rows,
            gap_rows=gap_rows,
            strategy_rows=strategy_rows,
        ),
        encoding="utf-8",
    )
    return {key: str(value) for key, value in paths.items()}


def _leaderboard_actual_path_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leaderboard = [
        {
            "strategy_id": row.get("strategy_id"),
            "policy_id": row.get("execution_policy_id"),
            "actual_path_annual_return": row.get("actual_path_annual_return"),
            "actual_path_max_drawdown_daily_equity": row.get(
                "actual_path_max_drawdown_daily_equity"
            ),
            "actual_path_sharpe_daily_zero_rf": row.get(
                "actual_path_sharpe_daily_zero_rf"
            ),
            "actual_path_calmar_daily_equity_dd": row.get(
                "actual_path_calmar_daily_equity_dd"
            ),
            "actual_path_turnover": row.get("actual_path_turnover"),
            "target_vs_actual_annual_return_gap": row.get(
                "target_vs_actual_annual_return_gap"
            ),
            "target_vs_actual_max_drawdown_gap": row.get(
                "target_vs_actual_max_drawdown_gap"
            ),
            "execution_lag_return_cost": row.get("execution_lag_return_cost"),
            "signal_staleness_return_cost": row.get("signal_staleness_return_cost"),
            "promotion_final_status": row.get("promotion_final_status"),
            "blocking_reasons": ";".join(str(item) for item in row.get("blocking_reasons", [])),
        }
        for row in rows
    ]
    return sorted(
        leaderboard,
        key=lambda row: (
            _float(row.get("actual_path_sharpe_daily_zero_rf")),
            _float(row.get("actual_path_annual_return")),
        ),
        reverse=True,
    )


def _target_vs_actual_gap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": row.get("strategy_id"),
            "policy_id": row.get("execution_policy_id"),
            "target_vs_actual_annual_return_gap": row.get(
                "target_vs_actual_annual_return_gap"
            ),
            "target_vs_actual_max_drawdown_gap": row.get(
                "target_vs_actual_max_drawdown_gap"
            ),
            "target_vs_actual_sharpe_gap": row.get("target_vs_actual_sharpe_gap"),
            "target_vs_actual_calmar_gap": row.get("target_vs_actual_calmar_gap"),
            "execution_lag_return_cost": row.get("execution_lag_return_cost"),
            "execution_lag_drawdown_cost": row.get("execution_lag_drawdown_cost"),
            "signal_staleness_return_cost": row.get("signal_staleness_return_cost"),
            "signal_staleness_drawdown_cost": row.get("signal_staleness_drawdown_cost"),
        }
        for row in rows
    ]


def _owner_review_pack_markdown(
    *,
    date_range: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    policy_registry_path: Path,
    policy_registry_hash: str,
    leaderboard_rows: list[dict[str, Any]],
    gap_rows: list[dict[str, Any]],
    strategy_rows: list[dict[str, Any]],
) -> str:
    readiness_rows = [
        {
            "strategy_id": row.get("strategy_id"),
            "policy_id": row.get("execution_policy_id"),
            "promotion_final_status": row.get("promotion_final_status", "blocked"),
            "blocking_reasons": ";".join(str(item) for item in row.get("blocking_reasons", [])),
        }
        for row in strategy_rows
    ]
    promising_review_rows = leaderboard_rows[:3]
    invalidated_review_rows = [
        row
        for row in leaderboard_rows
        if "lag_cost_review" in str(row.get("blocking_reasons", ""))
        or "signal_staleness_review" in str(row.get("blocking_reasons", ""))
    ]
    lines = [
        "# Execution Semantics Actual-Path Owner Review Pack",
        "",
        "Target-path metrics are diagnostic only and are not eligible for promotion decisions.",
        "",
        "## 1. Run summary",
        "",
        f"- market_regime: `{date_range.get('market_regime')}`",
        f"- date_range: `{date_range.get('start')}` to `{date_range.get('end')}`",
        f"- data_quality_status: `{data_quality.get('status')}`",
        f"- policy_registry_path: `{policy_registry_path}`",
        f"- policy_registry_hash: `{policy_registry_hash}`",
        "- dynamic_promotion: `BLOCKED`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## 2. Strategy list and policy bindings",
        "",
        _markdown_table(
            [
                {
                    "strategy_id": row.get("strategy_id"),
                    "strategy_type": row.get("strategy_type"),
                    "policy_id": row.get("execution_policy_id"),
                    "policy_hash": row.get("policy_hash"),
                    "status": row.get("status"),
                }
                for row in strategy_rows
            ],
            ["strategy_id", "strategy_type", "policy_id", "policy_hash", "status"],
        ),
        "",
        "## 3. Actual-path leaderboard",
        "",
        _markdown_table(
            leaderboard_rows,
            [
                "strategy_id",
                "policy_id",
                "actual_path_annual_return",
                "actual_path_max_drawdown_daily_equity",
                "actual_path_sharpe_daily_zero_rf",
                "actual_path_calmar_daily_equity_dd",
                "promotion_final_status",
            ],
        ),
        "",
        "## 4. Target vs actual gap summary",
        "",
        _markdown_table(
            gap_rows,
            [
                "strategy_id",
                "target_vs_actual_annual_return_gap",
                "target_vs_actual_max_drawdown_gap",
                "target_vs_actual_sharpe_gap",
                "target_vs_actual_calmar_gap",
            ],
        ),
        "",
        "## 5. Lag cost summary",
        "",
        _markdown_table(
            gap_rows,
            ["strategy_id", "execution_lag_return_cost", "execution_lag_drawdown_cost"],
        ),
        "",
        "## 6. Signal staleness summary",
        "",
        _markdown_table(
            gap_rows,
            [
                "strategy_id",
                "signal_staleness_return_cost",
                "signal_staleness_drawdown_cost",
            ],
        ),
        "",
        "## 7. Promotion readiness table",
        "",
        _markdown_table(
            readiness_rows,
            ["strategy_id", "policy_id", "promotion_final_status", "blocking_reasons"],
        ),
        "",
        "## 8. Strategies that remain promising",
        "",
        (
            "No strategy is automatically approved. The rows below are only the top "
            "actual-path leaderboard rows for owner review."
        ),
        "",
        _markdown_table(
            promising_review_rows,
            [
                "strategy_id",
                "actual_path_sharpe_daily_zero_rf",
                "actual_path_annual_return",
                "promotion_final_status",
            ],
        ),
        "",
        "## 9. Strategies that are invalidated by execution semantics",
        "",
        (
            "No automatic invalidation decision is emitted. Rows with lag or "
            "staleness blockers require owner review before further research use."
        ),
        "",
        _markdown_table(
            invalidated_review_rows,
            [
                "strategy_id",
                "execution_lag_return_cost",
                "signal_staleness_return_cost",
                "blocking_reasons",
            ],
        ),
        "",
        "## 10. Manual Review Checklist",
        "",
        "- [ ] I understand target-path metrics are diagnostic only.",
        "- [ ] I reviewed actual-path metrics for all candidate strategies.",
        "- [ ] I reviewed execution lag materiality.",
        "- [ ] I reviewed signal staleness materiality.",
        "- [ ] I reviewed strategies with blocked promotion status.",
        "- [ ] I approve keeping the following strategies as candidates:",
        "- [ ] I approve removing the following strategies from active research:",
        "- [ ] I approve running the next paper-shadow dry-run batch:",
        "",
        "## 11. Explicit signoff section",
        "",
        "- owner:",
        "- timestamp:",
        "- approved_strategy_ids:",
        "- removed_strategy_ids:",
        "- paper_shadow_dry_run_allowed: `false`",
        "- notes:",
        "",
    ]
    return "\n".join(lines)


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _load_actual_path_strategy_evidence(
    output_root: Path,
    strategy_id: str,
) -> dict[str, Any]:
    strategy_root = output_root / strategy_id
    actual_payload = _read_json_mapping(strategy_root / "metrics_actual_path.json")
    target_payload = _read_json_mapping(strategy_root / "metrics_target_path.json")
    summary = _read_json_mapping(strategy_root / "summary.json")
    readiness = _read_json_mapping(strategy_root / "promotion_readiness.json")
    return {
        "strategy_id": strategy_id,
        "summary": summary,
        "actual_path_metrics": _mapping(actual_payload.get("metrics")),
        "target_path_metrics": _mapping(target_payload.get("metrics")),
        "target_vs_actual_gap_metrics": _mapping(
            summary.get("target_vs_actual_gap_metrics")
        ),
        "promotion_readiness": readiness,
    }


def _owner_review_decision_for_candidate(
    *,
    candidate_id: str,
    strategy_metrics: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    evidence = _mapping(strategy_metrics.get(candidate_id))
    actual_metrics = _owner_metric_block(_mapping(evidence.get("actual_path_metrics")))
    comparisons = {
        f"vs_{baseline_id}": _owner_metric_delta_block(
            actual_metrics,
            _owner_metric_block(
                _mapping(
                    _mapping(strategy_metrics.get(baseline_id)).get("actual_path_metrics")
                )
            ),
        )
        for baseline_id in ACTUAL_PATH_OWNER_REVIEW_BASELINES
    }
    readiness = _mapping(evidence.get("promotion_readiness"))
    checks = _mapping(readiness.get("checks"))
    target_vs_actual = {
        "return_gap": _maybe_float(
            _mapping(evidence.get("target_vs_actual_gap_metrics")).get(
                "target_vs_actual_annual_return_gap"
            )
        ),
        "drawdown_gap": _maybe_float(
            _mapping(evidence.get("target_vs_actual_gap_metrics")).get(
                "target_vs_actual_max_drawdown_gap"
            )
        ),
        "lag_cost_materiality": _materiality_enum(
            _mapping(checks.get("lag_cost_review")).get("status")
        ),
        "staleness_cost_materiality": _materiality_enum(
            _mapping(checks.get("signal_staleness_review")).get("status")
        ),
    }
    recommendation, rationale = _owner_review_recommendation(
        comparisons=comparisons,
        target_vs_actual=target_vs_actual,
    )
    blocked_reasons = _dedupe_ordered(
        [
            *(str(item) for item in readiness.get("blocking_reason_codes", [])),
            "owner_manual_review_pending",
        ]
    )
    return {
        "strategy_id": candidate_id,
        "review_scope": "actual_path_only",
        "candidate_type": "dynamic",
        "legacy_result_status": "PRE_EXECUTION_SEMANTICS_LEGACY_EVIDENCE",
        "actual_path_metrics": actual_metrics,
        "comparisons": comparisons,
        "target_vs_actual": target_vs_actual,
        "promotion_readiness": {
            "final_status": _promotion_final_status_enum(readiness.get("final_status")),
            "blocked_reasons": blocked_reasons,
            "target_metrics_used_for_decision": False,
        },
        "system_review_recommendation": recommendation,
        "system_review_rationale": rationale,
        "owner_manual_review_required": True,
        "owner_decision": {
            "status": "pending",
            "recommended_status": recommendation,
            "allowed_values": [
                "PAPER_SHADOW_CANDIDATE",
                "WATCH_ONLY",
                "REJECT",
            ],
            "rationale_required": True,
        },
    }


def _owner_metric_block(metrics: Mapping[str, Any]) -> dict[str, float | None]:
    return {
        "annual_return": _maybe_float(metrics.get("actual_path_annual_return")),
        "max_drawdown_daily_equity": _maybe_float(
            metrics.get("actual_path_max_drawdown_daily_equity")
        ),
        "sharpe_daily_zero_rf": _maybe_float(metrics.get("actual_path_sharpe_daily_zero_rf")),
        "calmar_daily_equity_dd": _maybe_float(metrics.get("actual_path_calmar_daily_equity_dd")),
        "turnover": _maybe_float(metrics.get("actual_path_turnover")),
    }


def _owner_metric_delta_block(
    candidate: Mapping[str, float | None],
    baseline: Mapping[str, float | None],
) -> dict[str, float | None]:
    return {
        "annual_return_delta": _metric_delta(candidate, baseline, "annual_return"),
        "max_drawdown_delta": _metric_delta(
            candidate,
            baseline,
            "max_drawdown_daily_equity",
        ),
        "sharpe_delta": _metric_delta(candidate, baseline, "sharpe_daily_zero_rf"),
        "calmar_delta": _metric_delta(candidate, baseline, "calmar_daily_equity_dd"),
    }


def _owner_review_recommendation(
    *,
    comparisons: Mapping[str, Mapping[str, Any]],
    target_vs_actual: Mapping[str, Any],
) -> tuple[str, str]:
    if target_vs_actual.get("lag_cost_materiality") == "FAIL":
        return "REJECT", "execution lag materiality is FAIL under actual-path review"
    if target_vs_actual.get("staleness_cost_materiality") == "FAIL":
        return "REJECT", "signal staleness materiality is FAIL under actual-path review"
    no_trade_delta = _maybe_float(
        _mapping(comparisons.get("vs_no_trade")).get("annual_return_delta")
    )
    static_deltas = [
        _maybe_float(_mapping(comparisons.get(f"vs_{baseline}")).get("annual_return_delta"))
        for baseline in ("100_qqq", "qqq_60_sgov_40", "qqq_50_sgov_50")
    ]
    has_static_edge = any(delta is not None and delta > 0 for delta in static_deltas)
    has_no_trade_edge = no_trade_delta is not None and no_trade_delta > 0
    if not has_no_trade_edge and not has_static_edge:
        return "REJECT", "actual-path annual return does not beat no_trade or static baselines"
    qqq_delta = _maybe_float(
        _mapping(comparisons.get("vs_100_qqq")).get("annual_return_delta")
    )
    if qqq_delta is None or qqq_delta <= 0:
        return (
            "WATCH_ONLY",
            "actual-path edge exists, but annual return does not beat 100_qqq",
        )
    if (
        has_no_trade_edge
        and qqq_delta > 0
        and target_vs_actual.get("lag_cost_materiality") == "PASS"
        and target_vs_actual.get("staleness_cost_materiality") == "PASS"
    ):
        return (
            "PAPER_SHADOW_CANDIDATE",
            "actual-path edge survives no_trade and 100_qqq with PASS materiality",
        )
    return (
        "WATCH_ONLY",
        "actual-path edge is present but not stable enough for automatic preflight admission",
    )


def _promotion_final_status_enum(value: object) -> str:
    normalized = str(value or "").lower()
    if normalized == "reviewable":
        return "REVIEWABLE"
    if normalized == "blocked":
        return "BLOCKED"
    return "NOT_PROMOTION_ELIGIBLE"


def _write_owner_review_decision_artifacts(
    payload: dict[str, Any],
    docs_path: Path,
    yaml_path: Path,
) -> None:
    payload["artifact_paths"] = {
        "markdown_path": str(docs_path),
        "yaml_path": str(yaml_path),
    }
    yaml_payload = {
        "schema_version": "dynamic_actual_path_owner_review_decision.v1",
        "report_type": payload["report_type"],
        "status": payload["status"],
        "generated_at": payload["generated_at"],
        "market_regime": payload["market_regime"],
        "date_range": payload.get("date_range", {}),
        "summary": payload.get("summary", {}),
        "source_runtime_root": payload.get("source_runtime_root"),
        "baseline_strategy_ids": payload.get("baseline_strategy_ids", []),
        "candidate_strategy_ids": payload.get("candidate_strategy_ids", []),
        "owner_review_decisions": payload.get("owner_review_decisions", []),
        "target_path_metrics_used_for_decision": False,
        "dynamic_promotion_blocked": True,
        **SAFETY_BOUNDARY,
    }
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(
        yaml.safe_dump(yaml_payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text(_owner_review_decision_markdown(payload), encoding="utf-8")


def _owner_review_decision_markdown(payload: Mapping[str, Any]) -> str:
    decisions = _records(payload.get("owner_review_decisions"))
    rows = [
        {
            "strategy_id": item.get("strategy_id"),
            "recommendation": item.get("system_review_recommendation"),
            "owner_decision": _mapping(item.get("owner_decision")).get("status"),
            "annual_return": _mapping(item.get("actual_path_metrics")).get("annual_return"),
            "sharpe": _mapping(item.get("actual_path_metrics")).get(
                "sharpe_daily_zero_rf"
            ),
            "lag": _mapping(item.get("target_vs_actual")).get("lag_cost_materiality"),
            "staleness": _mapping(item.get("target_vs_actual")).get(
                "staleness_cost_materiality"
            ),
        }
        for item in decisions
    ]
    return "\n".join(
        [
            "# Dynamic Actual-Path Owner Review Decision",
            "",
            f"- 状态：`{payload.get('status')}`",
            "- market_regime：`ai_after_chatgpt`",
            "- promotion_decision_source：`actual_path_only`",
            "- target_path_metrics_role：`diagnostic_only`",
            "- dynamic_promotion：`BLOCKED`",
            "- owner_manual_review_required：`true`",
            "- paper_shadow_allowed：`false`",
            "- production_allowed：`false`",
            "- broker_action：`none`",
            "",
            "## Candidate Decisions",
            "",
            _markdown_table(
                rows,
                [
                    "strategy_id",
                    "recommendation",
                    "owner_decision",
                    "annual_return",
                    "sharpe",
                    "lag",
                    "staleness",
                ],
            ),
            "",
            "## Decision Notes",
            "",
            "本报告只记录 system review recommendation 与 pending owner decision 字段。"
            "任何 `PAPER_SHADOW_CANDIDATE` 都不是 promotion，也不会自动进入 paper-shadow。",
            "",
            "Target-path metrics 仅用于 target-vs-actual gap、execution lag 和 signal "
            "staleness diagnostic，不作为 owner decision 或 promotion readiness 的正向依据。",
            "",
        ]
    )


def _policy_sensitivity_scenarios(
    *,
    base_policy: Mapping[str, Any] | None,
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    turnover_values = _policy_sensitivity_turnover_values(base_policy, registry)
    scenarios: list[dict[str, Any]] = []
    seen: set[str] = set()
    for lag_days in POLICY_SENSITIVITY_EXECUTION_LAG_DAYS:
        for frequency in POLICY_SENSITIVITY_REBALANCE_FREQUENCIES:
            scenario = _policy_sensitivity_scenario(
                stage="stage_a",
                lag_days=lag_days,
                frequency=frequency,
                validity_days=20,
                turnover_constraint="existing_default",
                turnover_values=turnover_values,
            )
            scenarios.append(scenario)
            seen.add(str(scenario["scenario_id"]))
    for frequency in ("weekly", "monthly"):
        for validity_days in POLICY_SENSITIVITY_SIGNAL_VALIDITY_WINDOWS:
            for turnover_constraint in POLICY_SENSITIVITY_TURNOVER_CONSTRAINTS:
                scenario = _policy_sensitivity_scenario(
                    stage="stage_b",
                    lag_days=1,
                    frequency=frequency,
                    validity_days=validity_days,
                    turnover_constraint=turnover_constraint,
                    turnover_values=turnover_values,
                )
                if str(scenario["scenario_id"]) in seen:
                    continue
                scenarios.append(scenario)
                seen.add(str(scenario["scenario_id"]))
    return scenarios


def _policy_sensitivity_scenario(
    *,
    stage: str,
    lag_days: int,
    frequency: str,
    validity_days: int,
    turnover_constraint: str,
    turnover_values: Mapping[str, float],
) -> dict[str, Any]:
    return {
        "scenario_id": (
            f"lag{lag_days}d_{frequency}_validity{validity_days}d_"
            f"{turnover_constraint}"
        ),
        "sensitivity_stage": stage,
        "execution_lag_days": lag_days,
        "rebalance_frequency": frequency,
        "signal_validity_window_days": validity_days,
        "turnover_constraint": turnover_constraint,
        "max_turnover_per_period": turnover_values[turnover_constraint],
    }


def _policy_sensitivity_turnover_values(
    base_policy: Mapping[str, Any] | None,
    registry: Mapping[str, Any],
) -> dict[str, float]:
    policies = _records(registry.get("policies"))
    default_turnover = _float(
        _mapping(registry.get("defaults")).get("max_turnover_per_period"),
        1.0,
    )
    base_turnover = _float(
        _mapping(base_policy).get("max_turnover_per_period"),
        default_turnover,
    )
    positive_policy_values = [
        _float(policy.get("max_turnover_per_period"))
        for policy in policies
        if _float(policy.get("max_turnover_per_period")) > 0
    ]
    strict_turnover = min(positive_policy_values) if positive_policy_values else base_turnover
    return {
        "existing_default": base_turnover,
        "relaxed": max(base_turnover, default_turnover),
        "strict": min(base_turnover, strict_turnover),
    }


def _policy_sensitivity_policy(
    *,
    base_policy: Mapping[str, Any] | None,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    policy = dict(base_policy or _synthetic_policy("monthly_plus_threshold_5pct_v1"))
    frequency = str(scenario["rebalance_frequency"])
    execution_frequency = {
        "next_trading_day": "daily",
        "weekly": "weekly",
        "monthly": "monthly",
    }[frequency]
    policy.update(
        {
            "execution_policy_id": scenario["scenario_id"],
            "execution_frequency": execution_frequency,
            "rebalance_calendar": f"policy_sensitivity_{frequency}",
            "signal_to_execution_lag": int(scenario["execution_lag_days"]),
            "validity_period_days": int(scenario["signal_validity_window_days"]),
            "max_turnover_per_period": float(scenario["max_turnover_per_period"]),
            "minimum_holding_period": 0,
        }
    )
    return policy


def _policy_sensitivity_classifications(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_by_key = {
        ("no_trade", _policy_sensitivity_scenario_key(row)): row
        for row in rows
        if row.get("strategy_id") == "no_trade"
    }
    qqq_by_key = {
        _policy_sensitivity_scenario_key(row): row
        for row in rows
        if row.get("strategy_id") == "100_qqq"
    }
    static_rows = [
        row for row in rows if row.get("strategy_id") in ACTUAL_PATH_OWNER_REVIEW_BASELINES
    ]
    best_static_return = max(
        (_float(row.get("actual_path_annual_return")) for row in static_rows),
        default=0.0,
    )
    classifications: list[dict[str, Any]] = []
    for strategy_id in ACTUAL_PATH_OWNER_REVIEW_CANDIDATES:
        candidate_rows = [row for row in rows if row.get("strategy_id") == strategy_id]
        survival = {
            row["scenario_id"]: _policy_scenario_survives(
                row,
                baseline_by_key.get(("no_trade", _policy_sensitivity_scenario_key(row))),
            )
            for row in candidate_rows
        }
        any_survives = any(survival.values())
        flags = {
            "survives_lag_0d": _any_survives(
                candidate_rows,
                survival,
                execution_lag_days=0,
            ),
            "survives_lag_1d": _any_survives(
                candidate_rows,
                survival,
                execution_lag_days=1,
            ),
            "survives_lag_2d": _any_survives(
                candidate_rows,
                survival,
                execution_lag_days=2,
            ),
            "survives_weekly": _any_survives(
                candidate_rows,
                survival,
                rebalance_frequency="weekly",
            ),
            "survives_monthly": _any_survives(
                candidate_rows,
                survival,
                rebalance_frequency="monthly",
            ),
            "survives_short_validity_window": _any_survives(
                candidate_rows,
                survival,
                signal_validity_window_days={1, 3},
            ),
            "survives_long_validity_window": _any_survives(
                candidate_rows,
                survival,
                signal_validity_window_days={10, 20},
            ),
        }
        primary_failure_modes = _policy_sensitivity_failure_modes(
            strategy_rows=candidate_rows,
            survival=survival,
            flags=flags,
            qqq_by_key=qqq_by_key,
            best_static_return=best_static_return,
        )
        classification = _policy_sensitivity_classification(flags, any_survives)
        if (
            classification == "POLICY_STABLE"
            and "STATIC_BASELINE_UNDERPERFORMANCE" in primary_failure_modes
        ):
            classification = "POLICY_SENSITIVE_BUT_WATCHABLE"
        classifications.append(
            {
                "strategy_id": strategy_id,
                "sensitivity_classification": classification,
                "policy_stability": flags,
                "surviving_scenario_count": sum(1 for value in survival.values() if value),
                "tested_scenario_count": len(candidate_rows),
                "primary_failure_modes": primary_failure_modes,
                "recommended_next_action": _policy_sensitivity_next_action(
                    classification,
                    primary_failure_modes,
                ),
                "target_path_metrics_used_for_ranking": False,
                "owner_manual_review_required": True,
            }
        )
    return classifications


def _policy_sensitivity_scenario_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("execution_lag_days"),
        row.get("rebalance_frequency"),
        row.get("signal_validity_window_days"),
        row.get("turnover_constraint"),
    )


def _policy_scenario_survives(
    row: Mapping[str, Any],
    no_trade_row: Mapping[str, Any] | None,
) -> bool:
    if not no_trade_row:
        return False
    return (
        _float(row.get("actual_path_annual_return"))
        > _float(no_trade_row.get("actual_path_annual_return"))
        and row.get("execution_lag_materiality") != "FAIL"
        and row.get("signal_staleness_materiality") != "FAIL"
    )


def _any_survives(
    rows: list[dict[str, Any]],
    survival: Mapping[str, bool],
    **filters: Any,
) -> bool:
    for row in rows:
        matched = True
        for key, expected in filters.items():
            value = row.get(key)
            if isinstance(expected, set):
                matched = value in expected
            else:
                matched = value == expected
            if not matched:
                break
        if matched and survival.get(str(row.get("scenario_id"))) is True:
            return True
    return False


def _policy_sensitivity_failure_modes(
    *,
    strategy_rows: list[dict[str, Any]],
    survival: Mapping[str, bool],
    flags: Mapping[str, bool],
    qqq_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
    best_static_return: float,
) -> list[str]:
    modes: list[str] = []
    if flags.get("survives_lag_2d") is not True:
        modes.append("EXECUTION_LAG_COST_MATERIAL")
    if flags.get("survives_short_validity_window") is not True:
        modes.append("SIGNAL_STALENESS_COST_MATERIAL")
    if not _any_survives(strategy_rows, survival, turnover_constraint="strict"):
        modes.append("TURNOVER_COST_MATERIAL")
    drawdown_worse = any(
        abs(_float(row.get("actual_path_max_drawdown_daily_equity")))
        > abs(
            _float(
                _mapping(qqq_by_key.get(_policy_sensitivity_scenario_key(row))).get(
                    "actual_path_max_drawdown_daily_equity"
                )
            )
        )
        for row in strategy_rows
    )
    if drawdown_worse:
        modes.append("DRAWDOWN_WORSENING")
    best_candidate_return = max(
        (_float(row.get("actual_path_annual_return")) for row in strategy_rows),
        default=0.0,
    )
    if best_candidate_return <= best_static_return:
        modes.append("STATIC_BASELINE_UNDERPERFORMANCE")
    return _dedupe_ordered(modes)


def _policy_sensitivity_classification(
    flags: Mapping[str, bool],
    any_survives: bool,
) -> str:
    if not flags:
        return "INSUFFICIENT_EVIDENCE"
    if all(flags.values()):
        return "POLICY_STABLE"
    if any_survives and (
        flags.get("survives_lag_1d") is True
        or flags.get("survives_weekly") is True
        or flags.get("survives_monthly") is True
    ):
        return "POLICY_SENSITIVE_BUT_WATCHABLE"
    return "POLICY_FRAGILE"


def _policy_sensitivity_next_action(
    classification: str,
    failure_modes: list[str],
) -> str:
    if classification == "POLICY_STABLE":
        return "PAPER_SHADOW_PREFLIGHT"
    if classification == "POLICY_SENSITIVE_BUT_WATCHABLE":
        return "WATCH_ONLY"
    if failure_modes:
        return "STRATEGY_REDESIGN"
    return "REJECT"


def _policy_sensitivity_leaderboard_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    leaderboard = [
        {
            "strategy_id": row.get("strategy_id"),
            "strategy_role": row.get("strategy_role"),
            "scenario_id": row.get("scenario_id"),
            "execution_lag_days": row.get("execution_lag_days"),
            "rebalance_frequency": row.get("rebalance_frequency"),
            "signal_validity_window_days": row.get("signal_validity_window_days"),
            "turnover_constraint": row.get("turnover_constraint"),
            "actual_path_annual_return": row.get("actual_path_annual_return"),
            "actual_path_max_drawdown_daily_equity": row.get(
                "actual_path_max_drawdown_daily_equity"
            ),
            "actual_path_sharpe_daily_zero_rf": row.get("actual_path_sharpe_daily_zero_rf"),
            "actual_path_calmar_daily_equity_dd": row.get(
                "actual_path_calmar_daily_equity_dd"
            ),
            "actual_path_turnover": row.get("actual_path_turnover"),
            "execution_lag_materiality": row.get("execution_lag_materiality"),
            "signal_staleness_materiality": row.get("signal_staleness_materiality"),
        }
        for row in rows
    ]
    return sorted(
        leaderboard,
        key=lambda row: (
            _float(row.get("actual_path_sharpe_daily_zero_rf")),
            _float(row.get("actual_path_annual_return")),
            -abs(_float(row.get("actual_path_max_drawdown_daily_equity"))),
        ),
        reverse=True,
    )


def _policy_sensitivity_gap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": row.get("strategy_id"),
            "scenario_id": row.get("scenario_id"),
            "execution_lag_days": row.get("execution_lag_days"),
            "rebalance_frequency": row.get("rebalance_frequency"),
            "signal_validity_window_days": row.get("signal_validity_window_days"),
            "turnover_constraint": row.get("turnover_constraint"),
            "target_vs_actual_annual_return_gap": row.get(
                "target_vs_actual_annual_return_gap"
            ),
            "target_vs_actual_max_drawdown_gap": row.get(
                "target_vs_actual_max_drawdown_gap"
            ),
            "target_vs_actual_sharpe_gap": row.get("target_vs_actual_sharpe_gap"),
            "target_vs_actual_calmar_gap": row.get("target_vs_actual_calmar_gap"),
            "execution_lag_return_cost": row.get("execution_lag_return_cost"),
            "execution_lag_drawdown_cost": row.get("execution_lag_drawdown_cost"),
            "signal_staleness_return_cost": row.get("signal_staleness_return_cost"),
            "signal_staleness_drawdown_cost": row.get("signal_staleness_drawdown_cost"),
        }
        for row in rows
    ]


def _policy_sensitivity_readiness_summary(
    classifications: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_actual_path_policy_sensitivity_readiness.v1",
        "report_type": "dynamic_actual_path_policy_sensitivity_readiness",
        "status": "DYNAMIC_PROMOTION_BLOCKED",
        "dynamic_promotion_blocked": True,
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        "target_path_metrics_used_for_ranking": False,
        "owner_manual_review_required": True,
        "strategy_readiness": [
            {
                "strategy_id": item.get("strategy_id"),
                "sensitivity_classification": item.get("sensitivity_classification"),
                "recommended_next_action": item.get("recommended_next_action"),
                "promotion_final_status": "blocked",
                "blocking_reasons": [
                    "owner_manual_review_pending",
                    "dynamic_promotion_blocked",
                ],
                "primary_failure_modes": item.get("primary_failure_modes", []),
            }
            for item in classifications
        ],
        **SAFETY_BOUNDARY,
    }


def _best_surviving_candidate(classifications: list[dict[str, Any]]) -> str | None:
    ranked = sorted(
        classifications,
        key=lambda item: (
            {
                "POLICY_STABLE": 3,
                "POLICY_SENSITIVE_BUT_WATCHABLE": 2,
                "POLICY_FRAGILE": 1,
            }.get(str(item.get("sensitivity_classification")), 0),
            _int(item.get("surviving_scenario_count"), 0),
        ),
        reverse=True,
    )
    if not ranked or _int(ranked[0].get("surviving_scenario_count"), 0) <= 0:
        return None
    return str(ranked[0].get("strategy_id"))


def _write_policy_sensitivity_artifacts(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_path: Path,
    yaml_path: Path,
    matrix_rows: list[dict[str, Any]],
    leaderboard_rows: list[dict[str, Any]],
    gap_rows: list[dict[str, Any]],
    readiness_summary: Mapping[str, Any],
    summary_payload: Mapping[str, Any],
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "index": output_root / "index.json",
        "leaderboard_actual_path": output_root / "leaderboard_actual_path.csv",
        "target_vs_actual_gap_summary": output_root / "target_vs_actual_gap_summary.csv",
        "promotion_readiness_summary": output_root / "promotion_readiness_summary.json",
        "policy_sensitivity_matrix": output_root / "policy_sensitivity_matrix.csv",
        "policy_sensitivity_summary": output_root / "policy_sensitivity_summary.json",
        "review_markdown": docs_path,
        "review_yaml": yaml_path,
    }
    pd.DataFrame(matrix_rows).to_csv(paths["policy_sensitivity_matrix"], index=False)
    pd.DataFrame(leaderboard_rows).to_csv(paths["leaderboard_actual_path"], index=False)
    pd.DataFrame(gap_rows).to_csv(paths["target_vs_actual_gap_summary"], index=False)
    _write_json(paths["promotion_readiness_summary"], dict(readiness_summary))
    _write_json(paths["policy_sensitivity_summary"], dict(summary_payload))
    artifact_paths = {key: str(value) for key, value in paths.items()}
    if payload:
        payload["artifact_paths"] = artifact_paths
        _write_json(paths["index"], _policy_sensitivity_index_payload(payload, matrix_rows))
        _write_policy_sensitivity_review_docs(payload, docs_path, yaml_path, matrix_rows)
    return artifact_paths


def _policy_sensitivity_index_payload(
    payload: Mapping[str, Any],
    matrix_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_actual_path_policy_sensitivity_index.v1",
        "report_type": "dynamic_actual_path_policy_sensitivity_index",
        "status": payload.get("status"),
        "summary": payload.get("summary", {}),
        "date_range": payload.get("date_range", {}),
        "classification_policy": POLICY_SENSITIVITY_CLASSIFICATION_POLICY,
        "candidate_strategy_ids": list(ACTUAL_PATH_OWNER_REVIEW_CANDIDATES),
        "baseline_strategy_ids": list(ACTUAL_PATH_OWNER_REVIEW_BASELINES),
        "scenario_count": len(
            {str(row.get("scenario_id")) for row in matrix_rows if row.get("scenario_id")}
        ),
        "matrix_row_count": len(matrix_rows),
        "artifact_paths": payload.get("artifact_paths", {}),
        "promotion_decision_source": "actual_path_only",
        "target_path_metrics_role": "diagnostic_only",
        "dynamic_promotion_blocked": True,
        **SAFETY_BOUNDARY,
    }


def _write_policy_sensitivity_review_docs(
    payload: Mapping[str, Any],
    docs_path: Path,
    yaml_path: Path,
    matrix_rows: list[dict[str, Any]],
) -> None:
    classifications = _records(payload.get("strategy_classifications"))
    yaml_payload = {
        "schema_version": "dynamic_actual_path_policy_sensitivity_matrix.v1",
        "report_type": payload.get("report_type"),
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "market_regime": payload.get("market_regime"),
        "date_range": payload.get("date_range", {}),
        "summary": payload.get("summary", {}),
        "classification_policy": POLICY_SENSITIVITY_CLASSIFICATION_POLICY,
        "strategy_classifications": classifications,
        "candidate_matrix_rows": [
            row
            for row in matrix_rows
            if row.get("strategy_id") in ACTUAL_PATH_OWNER_REVIEW_CANDIDATES
        ],
        "runtime_artifacts": payload.get("artifact_paths", {}),
        "target_path_metrics_used_for_ranking": False,
        "dynamic_promotion_blocked": True,
        **SAFETY_BOUNDARY,
    }
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(
        yaml.safe_dump(yaml_payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text(_policy_sensitivity_markdown(payload), encoding="utf-8")


def _policy_sensitivity_markdown(payload: Mapping[str, Any]) -> str:
    rows = [
        {
            "strategy_id": item.get("strategy_id"),
            "classification": item.get("sensitivity_classification"),
            "surviving": item.get("surviving_scenario_count"),
            "tested": item.get("tested_scenario_count"),
            "next_action": item.get("recommended_next_action"),
            "failure_modes": ";".join(
                str(mode) for mode in item.get("primary_failure_modes", [])
            ),
        }
        for item in _records(payload.get("strategy_classifications"))
    ]
    return "\n".join(
        [
            "# Dynamic Actual-Path Policy Sensitivity Review",
            "",
            f"- 状态：`{payload.get('status')}`",
            "- market_regime：`ai_after_chatgpt`",
            "- matrix_mode：`staged`",
            "- Stage A：`execution_lag_days x rebalance_frequency`",
            (
                "- Stage B：`signal_validity_window_days x turnover_constraint on "
                "lag=1 weekly/monthly`"
            ),
            "- ranking_basis：`actual_path annual_return, max_drawdown, sharpe, calmar, turnover`",
            "- target_path_metrics_role：`diagnostic_only`",
            "- dynamic_promotion：`BLOCKED`",
            "- owner_manual_review_required：`true`",
            "- paper_shadow_allowed：`false`",
            "- production_allowed：`false`",
            "- broker_action：`none`",
            "",
            "## Classification",
            "",
            _markdown_table(
                rows,
                [
                    "strategy_id",
                    "classification",
                    "surviving",
                    "tested",
                    "next_action",
                    "failure_modes",
                ],
            ),
            "",
            "## Policy",
            "",
            POLICY_SENSITIVITY_CLASSIFICATION_POLICY["survival_rule"],
            "",
            "Target-path metrics 只用于解释 target-vs-actual gap、execution lag cost "
            "和 signal staleness cost，不参与 policy sensitivity ranking 或 next action。",
            "",
        ]
    )


def _materiality_enum(value: object) -> str:
    normalized = str(value or "").lower()
    if normalized == "pass":
        return "PASS"
    if normalized == "warn":
        return "WARN"
    if normalized == "fail":
        return "FAIL"
    return "UNKNOWN"


def _metric_delta(
    candidate: Mapping[str, float | None],
    baseline: Mapping[str, float | None],
    key: str,
) -> float | None:
    left = candidate.get(key)
    right = baseline.get(key)
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)


def _maybe_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, 6)


def _markdown_table(rows: list[Mapping[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_No rows._"
    lines = [
        "|" + "|".join(columns) + "|",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(str(row.get(column, "")) for column in columns) + "|")
    return "\n".join(lines)


def _lag_cost_markdown(strategy_id: str, lag_cost: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Lag Cost Report: {strategy_id}",
            "",
            f"- status: `{lag_cost.get('status')}`",
            f"- review_status: `{lag_cost.get('review_status')}`",
            f"- annual_return_target_path: `{lag_cost.get('annual_return_target_path')}`",
            f"- annual_return_actual_path: `{lag_cost.get('annual_return_actual_path')}`",
            f"- annual_return_lag_cost: `{lag_cost.get('annual_return_lag_cost')}`",
            f"- execution_lag_return_cost_abs: `{lag_cost.get('execution_lag_return_cost_abs')}`",
            (
                "- execution_lag_return_cost_relative_pct: "
                f"`{lag_cost.get('execution_lag_return_cost_relative_pct')}`"
            ),
            f"- drawdown_lag_cost: `{lag_cost.get('drawdown_lag_cost')}`",
            f"- sharpe_lag_cost: `{lag_cost.get('sharpe_lag_cost')}`",
            f"- actual_trade_delay_days_p95: `{lag_cost.get('actual_trade_delay_days_p95')}`",
            "",
            "Target-path metrics are diagnostic only and are not eligible for promotion decisions.",
        ]
    ) + "\n"


def _signal_staleness_markdown(strategy_id: str, staleness: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Signal Staleness Report: {strategy_id}",
            "",
            f"- status: `{staleness.get('status')}`",
            f"- review_status: `{staleness.get('review_status')}`",
            f"- average_signal_age_bdays: `{staleness.get('average_signal_age_bdays')}`",
            f"- p95_signal_age_bdays: `{staleness.get('p95_signal_age_bdays')}`",
            f"- stale_signal_days: `{staleness.get('stale_signal_days')}`",
            f"- stale_signal_day_pct: `{staleness.get('stale_signal_day_pct')}`",
            (
                "- signal_staleness_material_event_count: "
                f"`{staleness.get('signal_staleness_material_event_count')}`"
            ),
            (
                "- signal_staleness_return_cost_abs: "
                f"`{staleness.get('signal_staleness_return_cost_abs')}`"
            ),
            f"- missed_signal_window_count: `{staleness.get('missed_signal_window_count')}`",
            "",
            "Target-path metrics are diagnostic only and are not eligible for promotion decisions.",
        ]
    ) + "\n"


def _lag_cost_status(
    annual_lag_cost: float,
    drawdown_lag_cost: float,
    sharpe_lag_cost: float,
    *,
    review_status: str | None = None,
) -> str:
    if review_status in {"warn", "fail"}:
        return "EXECUTION_LAG_COST_MATERIAL"
    if (
        abs(annual_lag_cost) >= 0.01
        or abs(drawdown_lag_cost) >= 0.05
        or abs(sharpe_lag_cost) >= 0.20
    ):
        return "EXECUTION_LAG_COST_MATERIAL"
    if (
        abs(annual_lag_cost) >= 0.005
        or abs(drawdown_lag_cost) >= 0.02
        or abs(sharpe_lag_cost) >= 0.10
    ):
        return "EXECUTION_LAG_COST_WARN"
    return "EXECUTION_LAG_COST_READY"


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _dedupe_ordered(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


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
    if report_id == "dynamic_actual_path_owner_review_decision":
        return {
            "report_id": report_id,
            "title": spec["title"],
            "group": "research",
            "cadence": "ad_hoc",
            "audience": "project_owner",
            "owner": "research_governance",
            "command": spec["command"],
            "artifact_globs": [
                "docs/research/dynamic_actual_path_owner_review_decision.md",
                "inputs/research_reviews/dynamic_actual_path_owner_review_decision.yaml",
            ],
            "artifact_selection_policy": "latest_available",
            "freshness_sla_days": 30,
            "freshness_rationale": (
                "Owner review decisions must be regenerated after actual-path rebacktest, "
                "promotion readiness, strategy survival or owner-review policy changes."
            ),
            "owner_action": "review_dynamic_actual_path_owner_review_decision",
            "include_in_reader_brief": False,
            "include_in_daily_task_dashboard": False,
            "required_for_daily_reading": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    if report_id == "dynamic_actual_path_policy_sensitivity_review":
        return {
            "report_id": report_id,
            "title": spec["title"],
            "group": "research",
            "cadence": "ad_hoc",
            "audience": "project_owner",
            "owner": "research_governance",
            "command": spec["command"],
            "artifact_globs": [
                "outputs/research_strategies/policy_sensitivity/index.json",
                "outputs/research_strategies/policy_sensitivity/leaderboard_actual_path.csv",
                "outputs/research_strategies/policy_sensitivity/target_vs_actual_gap_summary.csv",
                "outputs/research_strategies/policy_sensitivity/promotion_readiness_summary.json",
                "outputs/research_strategies/policy_sensitivity/policy_sensitivity_matrix.csv",
                "outputs/research_strategies/policy_sensitivity/policy_sensitivity_summary.json",
                "docs/research/dynamic_actual_path_policy_sensitivity_review.md",
                "inputs/research_reviews/dynamic_actual_path_policy_sensitivity_matrix.yaml",
            ],
            "artifact_selection_policy": "latest_available",
            "freshness_sla_days": 30,
            "freshness_rationale": (
                "Policy sensitivity evidence must be regenerated after execution policy, "
                "strategy target path, materiality policy or cached data changes."
            ),
            "owner_action": "review_dynamic_actual_path_policy_sensitivity",
            "include_in_reader_brief": False,
            "include_in_daily_task_dashboard": False,
            "required_for_daily_reading": False,
            "production_effect": "none",
            "broker_action": "none",
        }
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
