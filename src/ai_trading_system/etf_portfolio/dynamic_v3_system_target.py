from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    MarketstackPriceProvider,
    YFinancePriceProvider,
)
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import DEFAULT_RATES_CACHE_PATH
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
TARGET_METHODS = (
    "static_baseline",
    "no_trade_baseline",
    "consensus_target",
    "limited_adjustment",
    "smooth_weights_3d_limited_adjustment",
    "smooth_weights_5d_limited_adjustment",
    "risk_capped_limited_adjustment",
    "defensive_limited_adjustment",
    "equal_weight_shadow_candidates",
    "selected_top_candidate",
)
SMOOTHED_VARIANT_TO_METHOD = {
    "smooth_weights_3d": "smooth_weights_3d_limited_adjustment",
    "smooth_weights_5d": "smooth_weights_5d_limited_adjustment",
}
SMOOTHED_METHOD_TO_VARIANT = {
    method: variant for variant, method in SMOOTHED_VARIANT_TO_METHOD.items()
}
DEFAULT_MODEL_TARGET_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "model_target_portfolio_v1.yaml"
)
DEFAULT_PAPER_SHADOW_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "paper_shadow_account_v1.yaml"
)
DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "paper_shadow_backfill_v1.yaml"
)
DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "risk_capped_limited_adjustment_v1.yaml"
)
DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "smoothed_limited_adjustment_v1.yaml"
)
DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_optimization_hypothesis_v1.yaml"
)
DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_variant_transform_v1.yaml"
)
DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_experiment_matrix_v1.yaml"
)
DEFAULT_PRICE_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MODEL_TARGET_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_target"
DEFAULT_PAPER_SHADOW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow"
DEFAULT_MODEL_REBALANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_rebalance"
DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_performance"
DEFAULT_SYSTEM_TARGET_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "system_target_review"
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_backfill"
DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_rolling_eval"
)
DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_regime_review"
)
DEFAULT_PAPER_SHADOW_STABILITY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_stability"
DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "system_target_selection_review"
)
DEFAULT_SELECTION_ATTRIBUTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "selection_attribution"
DEFAULT_LIMITED_LONG_RISK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_long_risk"
DEFAULT_LIMITED_CONSISTENCY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_consistency"
DEFAULT_DATA_WARNING_IMPACT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "data_warning_impact"
DEFAULT_RESEARCH_METHOD_HARDENING_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "research_method_hardening"
)
DEFAULT_LIMITED_INSTABILITY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_instability"
DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_risk_attribution"
DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "data_warning_repair_plan"
DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "alternative_method_review"
)
DEFAULT_REFINED_METHOD_PROPOSAL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "refined_method_proposal"
DEFAULT_RISK_CAPPED_CONFIG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "risk_capped_limited_config"
DEFAULT_RISK_CAPPED_LIMITED_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "risk_capped_limited"
DEFAULT_RISK_CAPPED_BACKFILL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "risk_capped_backfill"
DEFAULT_RISK_CAPPED_COMPARISON_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "risk_capped_comparison"
DEFAULT_RISK_CAPPED_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "risk_capped_review"
DEFAULT_SMOOTHED_CONFIG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_limited_config"
DEFAULT_SMOOTHED_LIMITED_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_limited"
DEFAULT_SMOOTHED_BACKFILL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_backfill"
DEFAULT_SMOOTHED_COMPARISON_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_comparison"
DEFAULT_SMOOTHED_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_review"
DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_review_attribution"
)
DEFAULT_SMOOTHING_BENEFIT_LAG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothing_benefit_lag"
DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_regime_validation"
)
DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_forward_confirmation"
)
DEFAULT_SMOOTHED_WATCH_PACK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_watch_pack"
DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_evidence_gap"
DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_churn_backfill"
DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sideways_mixed_attribution"
)
DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_readiness_scorecard"
)
DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_owner_review_update"
)
DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_promotion_review"
)
DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "primary_research_candidate_gate"
)
DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_forward_binding"
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_primary_switch"
)
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_owner_promotion"
DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_forward_progress"
)
DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_weekly_dashboard"
)
DEFAULT_SMOOTHED_EVENT_MONITOR_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_event_monitor"
DEFAULT_SMOOTHED_SWITCH_READINESS_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_switch_readiness"
)
DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_owner_renewal"
DEFAULT_SMOOTHED_DAILY_EMISSION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_daily_emission"
DEFAULT_SMOOTHED_OUTCOME_DUE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_outcome_due"
DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_outcome_update"
DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_forward_classification"
)
DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_forward_weekly_run"
)
DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_data_preflight"
DEFAULT_SMOOTHED_LATEST_EMISSION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_latest_emission"
DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_blocked_explain"
DEFAULT_SMOOTHED_REFRESH_PLAN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_refresh_plan"
DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_bootstrap_retry"
DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "smoothed_source_refresh_v1.yaml"
)
DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_source_refresh"
DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_post_refresh_validation"
)
DEFAULT_SMOOTHED_RETRY_RESUME_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_retry_resume"
DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_sample_growth"
DEFAULT_SMOOTHED_DATA_READINESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "smoothed_data_readiness"
DEFAULT_HYPOTHESIS_BACKLOG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "hypothesis_backlog"
DEFAULT_VARIANT_TRANSFORM_SPEC_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "variant_transform_spec"
DEFAULT_EXPERIMENT_MATRIX_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "experiment_matrix"
DEFAULT_BATCH_EXPERIMENT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "batch_experiment"
DEFAULT_EXPERIMENT_TRIAGE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "experiment_triage"
DEFAULT_TOP_VARIANT_INTERPRETATION_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "top_variant_interpretation"
)
DEFAULT_METHOD_PROMOTION_PLAN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "method_promotion_plan"

AI_AFTER_CHATGPT_START = date(2022, 12, 1)

# Reporting bucket boundary, not an approval or allocation rule. The 2% daily
# loss level is intentionally named so future calibration work can audit it.
PRESSURE_RETURN_THRESHOLD = -0.02

# Reporting sufficiency floor used only to label thin diagnostic windows.
DEFAULT_MIN_EVAL_OBSERVATIONS = 20

# Reporting-only data quality penalties used in attribution explanations. These
# do not recompute or replace the original system target selection score.
DATA_QUALITY_WARNING_ATTRIBUTION_PENALTY = 0.05
DATA_QUALITY_FAIL_ATTRIBUTION_PENALTY = 0.25

# Reporting confidence floors for long-window review labels. They only affect
# confidence text, not recommendation or hardening decisions.
LONG_WINDOW_HIGH_CONFIDENCE_OBSERVATIONS = 504
LONG_WINDOW_MEDIUM_CONFIDENCE_OBSERVATIONS = 252

# Exposure interpretation tolerance for reporting whether limited_adjustment
# materially changes risk-asset weight relative to static baseline.
EXPOSURE_SIMILARITY_TOLERANCE = 0.02

# Reporting-only window diagnosis buckets. These thresholds do not approve,
# reject, or size any portfolio; they only classify review rows.
INSTABILITY_RETURN_UNDERPERFORMANCE_TOLERANCE = -0.0001
INSTABILITY_DRAWDOWN_WORSE_TOLERANCE = -0.002
INSTABILITY_HIGH_SEVERITY_RETURN_DELTA = -0.005
INSTABILITY_HIGH_SEVERITY_DRAWDOWN_DELTA = -0.01
INSTABILITY_TURNOVER_HIGH_THRESHOLD = 0.08
INSTABILITY_WEIGHT_JUMP_THRESHOLD = 0.04

# Reporting-only trailing-event buckets for risk attribution.
RISK_WORSENING_EVENT_WINDOW_DAYS = 20
RISK_WORSENING_DRAWDOWN_DELTA_THRESHOLD = -0.002
RISK_WORSENING_VOL_DELTA_THRESHOLD = 0.005
RISK_WORSENING_TURNOVER_DELTA_THRESHOLD = 0.02
RISK_EVENT_HIGH_SEMICONDUCTOR_WEIGHT = 0.25
RISK_EVENT_LOW_CASH_WEIGHT = 0.10

# Reporting-only alternative review bucket for method comparison labels.
ALTERNATIVE_RETURN_HIGH_EXPECTATION_THRESHOLD = 0.10

# Reporting-only preservation floor for risk-capped method review. It labels
# whether drawdown/exposure improvement sacrificed too much return; it does not
# approve production weights or size a position.
RISK_CAPPED_ACCEPTABLE_RETURN_DELTA_FLOOR = -0.05

# Reporting-only threshold for treating tiny exposure changes as unchanged.
RISK_CAPPED_EXPOSURE_CHANGE_TOLERANCE = 0.001

# Reporting-only preservation floor for smoothed method review. It labels
# whether smoothing sacrificed too much return; it does not approve production
# weights or size a position.
SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR = -0.03

# Reporting-only threshold for lag cost labels in smoothed comparison artifacts.
SMOOTHED_LAG_COST_HIGH_THRESHOLD = -0.02
SMOOTHED_LAG_COST_MEDIUM_THRESHOLD = -0.005

# Owner-requested pilot baselines for TRADING-254 forward confirmation targets.
# They are research watch thresholds, not production promotion gates.
SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS = 10
SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS = 5
SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS = 5
SMOOTHED_CONFIRMATION_WINDOWS = (1, 5, 10, 20)
SMOOTHED_CONFIRMATION_RETURN_DELTA_MIN = -0.001

# Owner-requested TRADING-256 to 260 pilot baseline for direct churn evidence.
# It is a research reporting boundary, not a production trading rule.
SMOOTHED_CHURN_WEIGHT_JUMP_EVENT_THRESHOLD = 0.08
SMOOTHED_CHURN_WEIGHT_JUMP_HIGH_SEVERITY_THRESHOLD = 0.16

# Owner-requested TRADING-259 pilot scorecard weights. The supporting
# requirements document records the exit condition for replacing these with
# evidence-backed calibration.
SMOOTHED_READINESS_SCORE_WEIGHTS: dict[str, float] = {
    "return_preservation_score": 0.20,
    "drawdown_score": 0.15,
    "turnover_score": 0.15,
    "weight_jump_score": 0.15,
    "signal_churn_score": 0.15,
    "sideways_score": 0.10,
    "recovery_lag_score": 0.05,
    "forward_confirmation_score": 0.05,
}
SMOOTHED_READINESS_PROMOTE_REVIEW_SCORE = 0.75
SMOOTHED_READINESS_CONTINUE_OBSERVATION_SCORE = 0.45

# TRADING-274 reporting-only regime proxy thresholds. They classify forward
# evidence samples when no explicit pressure-regime tag is available; they do
# not change target weights, scoring, promotion gates, or any trading action.
SMOOTHED_CLASSIFIER_SIDEWAYS_ABS_RETURN_THRESHOLD = 0.01
SMOOTHED_CLASSIFIER_STRONG_RECOVERY_RETURN_THRESHOLD = 0.02
SMOOTHED_CLASSIFIER_FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD = 0.04
SMOOTHED_CLASSIFIER_LAG_WARNING_DELTA_THRESHOLD = -0.01

SYSTEM_TARGET_SAFETY: dict[str, Any] = {
    "research_target_only": True,
    "paper_shadow_only": True,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "owner_approval_required": True,
    "production_effect": PRODUCTION_EFFECT,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
    "production_candidate_generated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
}

EXPERIMENT_FACTORY_SAFETY: dict[str, Any] = {
    **SYSTEM_TARGET_SAFETY,
    "experiment_only": True,
    "research_screening_only": True,
    "not_formal_research_method": True,
}

DEFAULT_FAILURE_MODES: tuple[str, ...] = (
    "exposure_too_high",
    "higher_semiconductor_exposure",
    "sideways_choppy_instability",
    "signal_churn",
    "missed_rebound_after_cap",
    "cap_too_mechanical",
    "drawdown_not_improved",
    "return_preservation_poor",
    "rolling_consistency_unstable",
    "turnover_high",
    "weight_jump_high",
    "regime_mismatch",
    "data_warning_review_required",
)

DEFAULT_HYPOTHESIS_FAMILIES: tuple[str, ...] = (
    "risk_exposure_control",
    "regime_gating",
    "signal_stability",
    "cooldown",
    "weight_smoothing",
    "rebalance_threshold",
    "candidate_ensemble",
    "cash_buffer",
    "turnover_control",
)

DEFAULT_TRANSFORM_TYPES: tuple[str, ...] = (
    "cap_group_weight",
    "cap_symbol_weight",
    "min_cash_weight",
    "regime_gate",
    "regime_cooldown",
    "weight_smoothing",
    "signal_persistence",
    "rebalance_threshold",
    "turnover_cap",
    "candidate_subset",
    "consensus_aggregation",
    "hold_previous_weights",
)

DEFAULT_TRIAGE_DECISIONS: tuple[str, ...] = (
    "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE",
    "KEEP_FOR_MORE_TESTING",
    "REJECT",
    "DEFER",
)


class DynamicV3SystemTargetError(ValueError):
    """Raised when system target or paper shadow artifacts fail closed."""




def _call_risk_capped(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    # ARCH-004 G2.4CL compatibility surface. The lazy import avoids a module
    # cycle while preserving every historical Python caller.
    from ai_trading_system.etf_portfolio import dynamic_v3_system_target_risk_capped

    try:
        return getattr(dynamic_v3_system_target_risk_capped, name)(*args, **kwargs)
    except DynamicV3SystemTargetError:
        raise
    except ValueError as exc:
        raise DynamicV3SystemTargetError(str(exc)) from exc


def load_risk_capped_limited_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("load_risk_capped_limited_config", *args, **kwargs)


def validate_risk_capped_limited_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("validate_risk_capped_limited_config", *args, **kwargs)


def build_risk_capped_limited_config_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("build_risk_capped_limited_config_report", *args, **kwargs)


def risk_capped_limited_config_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("risk_capped_limited_config_report_payload", *args, **kwargs)


def load_smoothed_limited_config(
    path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_smoothed_limited_config_safe(payload)
    return payload


def validate_smoothed_limited_config(
    path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_smoothed_limited_config(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    if payload:
        method = _mapping(payload.get("method"))
        variants = _mapping(payload.get("variants"))
        constraints = _mapping(payload.get("constraints"))
        safety = _mapping(payload.get("safety"))
        enabled = _enabled_smoothed_variants(payload)
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check(
                    "method_name",
                    method.get("name") == "smoothed_limited_adjustment",
                    _text(method.get("name")),
                ),
                _check(
                    "base_method_limited_adjustment",
                    method.get("base_method") == "limited_adjustment",
                    _text(method.get("base_method")),
                ),
                _check(
                    "research_target_only",
                    method.get("mode") == "research_target_only"
                    and method.get("not_official_target_weights") is True
                    and method.get("paper_shadow_only") is True,
                    "",
                ),
                _check("at_least_one_variant_enabled", bool(enabled), ",".join(enabled)),
                _check(
                    "known_variants_only",
                    set(enabled).issubset(set(SMOOTHED_VARIANT_TO_METHOD)),
                    ",".join(enabled),
                ),
                _check(
                    "variant_smoothing_windows_positive",
                    all(
                        int(_float(_mapping(variants.get(variant)).get("smoothing_window_days")))
                        > 0
                        for variant in enabled
                    ),
                    "",
                ),
                _check(
                    "variant_alpha_within_bounds",
                    all(
                        0.0 <= _float(_mapping(variants.get(variant)).get("alpha"), -1.0) <= 1.0
                        for variant in enabled
                    ),
                    "",
                ),
                _check(
                    "max_daily_total_weight_change_within_bounds",
                    all(
                        0.0
                        <= _float(
                            _mapping(variants.get(variant)).get("max_daily_total_weight_change"),
                            -1.0,
                        )
                        <= 1.0
                        for variant in enabled
                    ),
                    "",
                ),
                _check(
                    "single_symbol_change_not_above_total_change",
                    all(
                        _float(
                            _mapping(variants.get(variant)).get("max_single_symbol_daily_change"),
                            2.0,
                        )
                        <= _float(
                            _mapping(variants.get(variant)).get("max_daily_total_weight_change"),
                            -1.0,
                        )
                        for variant in enabled
                    ),
                    "",
                ),
                _check(
                    "min_cash_non_negative",
                    _float(constraints.get("min_cash_weight"), -1.0) >= 0.0,
                    _text(constraints.get("min_cash_weight")),
                ),
                _check(
                    "preserve_total_weight_true",
                    constraints.get("preserve_total_weight") is True,
                    "",
                ),
                _check(
                    "constraints_within_bounds",
                    _smoothed_constraints_within_bounds(constraints),
                    "",
                ),
                _check("safety_locked", _safety_config_locked(safety), ""),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_limited_config_validation",
        "smoothed_limited_config",
        checks,
        extra={"config_path": str(path)},
    )


def build_smoothed_limited_config_report(
    *,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    output_dir: Path = DEFAULT_SMOOTHED_CONFIG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_smoothed_limited_config(config_path)
    validation = validate_smoothed_limited_config(config_path)
    config_validation_id = _stable_id(
        "smoothed-limited-config",
        config_path,
        validation["status"],
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / config_validation_id)
    root.mkdir(parents=True, exist_ok=False)
    normalized = _normalized_smoothed_config(config)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_limited_config_manifest",
        "config_validation_id": root.name,
        "generated_at": generated.isoformat(),
        "status": validation["status"],
        "config_path": str(config_path),
        "base_method": _mapping(config.get("method")).get("base_method"),
        "target_method": _mapping(config.get("method")).get("name"),
        "enabled_variants": _enabled_smoothed_variants(config),
        "smoothed_limited_config_manifest_path": str(
            root / "smoothed_limited_config_manifest.json"
        ),
        "normalized_smoothed_limited_config_path": str(
            root / "normalized_smoothed_limited_config.yaml"
        ),
        "smoothed_limited_config_report_path": str(root / "smoothed_limited_config_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_limited_config_manifest.json", manifest)
    _write_text(
        root / "normalized_smoothed_limited_config.yaml",
        yaml.safe_dump(normalized, sort_keys=False, allow_unicode=True),
    )
    _write_text(
        root / "smoothed_limited_config_report.md",
        render_smoothed_limited_config_report(manifest, normalized, validation),
    )
    _write_latest_pointer(
        "latest_smoothed_limited_config",
        root.name,
        root / "smoothed_limited_config_manifest.json",
    )
    return {
        "config_validation_id": root.name,
        "config_dir": root,
        "manifest": manifest,
        "normalized_config": normalized,
        "validation": validation,
    }


def smoothed_limited_config_report_payload(
    *,
    config_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_CONFIG_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=config_validation_id,
        latest_pointer="latest_smoothed_limited_config",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_limited_config_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_limited_config_manifest.json"),
        "config_dir": str(root),
        "normalized_config": _load_yaml_mapping(root / "normalized_smoothed_limited_config.yaml"),
    }


def load_weight_optimization_hypothesis_config(
    path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_experiment_factory_config_safe(payload)
    return payload


def validate_weight_optimization_hypothesis_config(
    path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_weight_optimization_hypothesis_config(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    else:
        failure_modes = _records(payload.get("failure_modes"))
        hypotheses = _records(payload.get("hypotheses"))
        mode_ids = {str(row.get("id")) for row in failure_modes if row.get("id")}
        families = set(_texts(payload.get("hypothesis_families")))
        hypothesis_ids = [str(row.get("hypothesis_id")) for row in hypotheses]
        target_modes = {
            mode for row in hypotheses for mode in _texts(row.get("target_failure_modes"))
        }
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check(
                    "required_failure_modes_present",
                    set(DEFAULT_FAILURE_MODES).issubset(mode_ids),
                    ",".join(sorted(mode_ids)),
                ),
                _check(
                    "required_hypothesis_families_present",
                    set(DEFAULT_HYPOTHESIS_FAMILIES).issubset(families),
                    ",".join(sorted(families)),
                ),
                _check("hypotheses_present", bool(hypotheses), ""),
                _check(
                    "hypothesis_ids_unique",
                    len(hypothesis_ids) == len(set(hypothesis_ids)),
                    ",".join(hypothesis_ids),
                ),
                _check(
                    "hypotheses_have_target_failure_modes",
                    all(_texts(row.get("target_failure_modes")) for row in hypotheses),
                    "",
                ),
                _check(
                    "hypothesis_failure_modes_known",
                    target_modes.issubset(mode_ids),
                    ",".join(sorted(target_modes - mode_ids)),
                ),
                _check(
                    "hypothesis_families_known",
                    {str(row.get("family")) for row in hypotheses}.issubset(families),
                    "",
                ),
                _check(
                    "safety_locked",
                    _experiment_safety_config_locked(_mapping(payload.get("safety"))),
                    "",
                ),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_weight_optimization_hypothesis_config_validation",
        "weight_optimization_hypothesis_config",
        checks,
        extra={"config_path": str(path)},
    )


def build_hypothesis_backlog(
    *,
    config_path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
    output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_weight_optimization_hypothesis_config(config_path)
    validation = validate_weight_optimization_hypothesis_config(config_path)
    failure_modes = _normalized_failure_modes(config)
    hypotheses = _normalized_hypotheses(config)
    priority = _hypothesis_priority_summary(failure_modes, hypotheses)
    backlog_id = _stable_id(
        "hypothesis-backlog",
        config_path,
        validation["status"],
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backlog_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_hypothesis_backlog_manifest",
        "backlog_id": root.name,
        "generated_at": generated.isoformat(),
        "status": validation["status"],
        "config_path": str(config_path),
        "failure_modes_count": len(failure_modes["failure_modes"]),
        "hypotheses_count": len(hypotheses),
        "high_priority_hypotheses": priority["high_priority_hypotheses"],
        "hypothesis_backlog_manifest_path": str(root / "hypothesis_backlog_manifest.json"),
        "failure_mode_taxonomy_path": str(root / "failure_mode_taxonomy.json"),
        "hypotheses_path": str(root / "hypotheses.jsonl"),
        "hypothesis_priority_summary_path": str(root / "hypothesis_priority_summary.json"),
        "hypothesis_backlog_report_path": str(root / "hypothesis_backlog_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "hypothesis_backlog_manifest.json", manifest)
    _write_json(root / "failure_mode_taxonomy.json", failure_modes)
    _write_jsonl(root / "hypotheses.jsonl", hypotheses)
    _write_json(root / "hypothesis_priority_summary.json", priority)
    _write_text(
        root / "hypothesis_backlog_report.md",
        render_hypothesis_backlog_report(manifest, failure_modes, hypotheses, priority),
    )
    _write_latest_pointer(
        "latest_hypothesis_backlog",
        root.name,
        root / "hypothesis_backlog_manifest.json",
    )
    return {
        "backlog_id": root.name,
        "backlog_dir": root,
        "manifest": manifest,
        "failure_mode_taxonomy": failure_modes,
        "hypotheses": hypotheses,
        "hypothesis_priority_summary": priority,
        "validation": validation,
    }


def hypothesis_backlog_report_payload(
    *,
    backlog_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=backlog_id,
        latest_pointer="latest_hypothesis_backlog",
        latest=latest,
        output_dir=output_dir,
        required_name="hypothesis_backlog_manifest.json",
    )
    return {
        **_read_json(root / "hypothesis_backlog_manifest.json"),
        "failure_mode_taxonomy": _read_json(root / "failure_mode_taxonomy.json"),
        "hypotheses": _read_jsonl(root / "hypotheses.jsonl"),
        "hypothesis_priority_summary": _read_json(root / "hypothesis_priority_summary.json"),
        "backlog_dir": str(root),
    }


def validate_hypothesis_backlog_artifact(
    *,
    backlog_id: str,
    output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> dict[str, Any]:
    root = output_dir / backlog_id
    manifest = _read_optional_json(root / "hypothesis_backlog_manifest.json") or {}
    taxonomy = _read_optional_json(root / "failure_mode_taxonomy.json") or {}
    hypotheses = _read_jsonl(root / "hypotheses.jsonl")
    priority = _read_optional_json(root / "hypothesis_priority_summary.json") or {}
    mode_ids = {
        str(row.get("id")) for row in _records(taxonomy.get("failure_modes")) if row.get("id")
    }
    checks = _required_file_checks(
        root,
        (
            "hypothesis_backlog_manifest.json",
            "failure_mode_taxonomy.json",
            "hypotheses.jsonl",
            "hypothesis_priority_summary.json",
            "hypothesis_backlog_report.md",
        ),
    )
    checks.extend(
        [
            _check("backlog_id_matches", manifest.get("backlog_id") == backlog_id, ""),
            _check("failure_mode_taxonomy_present", bool(mode_ids), ""),
            _check("hypotheses_present", bool(hypotheses), ""),
            _check(
                "hypotheses_have_target_failure_modes",
                all(_texts(row.get("target_failure_modes")) for row in hypotheses),
                "",
            ),
            _check(
                "hypothesis_failure_modes_known",
                all(
                    set(_texts(row.get("target_failure_modes"))).issubset(mode_ids)
                    for row in hypotheses
                ),
                "",
            ),
            _check(
                "priority_summary_present",
                priority.get("hypotheses_total") == len(hypotheses),
                "",
            ),
            _check(
                "promotion_eligible_false",
                all(row.get("promotion_eligible") is False for row in hypotheses),
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, taxonomy, priority, *hypotheses),
                "",
            ),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, taxonomy, priority, *hypotheses),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_hypothesis_backlog_validation",
        backlog_id,
        checks,
    )


def load_weight_variant_transform_spec(
    path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_experiment_factory_config_safe(payload)
    return payload


def validate_weight_variant_transform_spec_config(
    path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_weight_variant_transform_spec(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    else:
        transform_types = _mapping(payload.get("transform_types"))
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check(
                    "required_transform_types_present",
                    set(DEFAULT_TRANSFORM_TYPES).issubset(set(transform_types)),
                    ",".join(sorted(transform_types)),
                ),
                _check(
                    "required_fields_declared",
                    all(
                        isinstance(_mapping(spec).get("required_fields"), Sequence)
                        and not isinstance(_mapping(spec).get("required_fields"), str | bytes)
                        for spec in transform_types.values()
                    ),
                    "",
                ),
                _check(
                    "safety_locked",
                    _experiment_safety_config_locked(_mapping(payload.get("safety"))),
                    "",
                ),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_variant_transform_spec_config_validation",
        "weight_variant_transform_spec_config",
        checks,
        extra={"config_path": str(path)},
    )


def build_variant_transform_spec_report(
    *,
    config_path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
    output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    spec = load_weight_variant_transform_spec(config_path)
    validation = validate_weight_variant_transform_spec_config(config_path)
    normalized = _normalized_variant_transform_spec(spec)
    catalog = _transform_type_catalog(normalized)
    spec_id = _stable_id("variant-transform-spec", config_path, generated.isoformat())
    root = _unique_dir(output_dir / spec_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_transform_spec_manifest",
        "spec_id": root.name,
        "generated_at": generated.isoformat(),
        "status": validation["status"],
        "config_path": str(config_path),
        "transform_type_count": len(catalog["transform_types"]),
        "variant_transform_spec_manifest_path": str(root / "variant_transform_spec_manifest.json"),
        "normalized_transform_spec_path": str(root / "normalized_transform_spec.yaml"),
        "transform_type_catalog_path": str(root / "transform_type_catalog.json"),
        "variant_transform_spec_report_path": str(root / "variant_transform_spec_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "variant_transform_spec_manifest.json", manifest)
    _write_text(
        root / "normalized_transform_spec.yaml",
        yaml.safe_dump(normalized, sort_keys=False, allow_unicode=True),
    )
    _write_json(root / "transform_type_catalog.json", catalog)
    _write_text(
        root / "variant_transform_spec_report.md",
        render_variant_transform_spec_report(manifest, catalog),
    )
    _write_latest_pointer(
        "latest_variant_transform_spec",
        root.name,
        root / "variant_transform_spec_manifest.json",
    )
    return {
        "spec_id": root.name,
        "spec_dir": root,
        "manifest": manifest,
        "normalized_transform_spec": normalized,
        "transform_type_catalog": catalog,
        "validation": validation,
    }


def variant_transform_spec_report_payload(
    *,
    spec_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=spec_id,
        latest_pointer="latest_variant_transform_spec",
        latest=latest,
        output_dir=output_dir,
        required_name="variant_transform_spec_manifest.json",
    )
    return {
        **_read_json(root / "variant_transform_spec_manifest.json"),
        "normalized_transform_spec": _load_yaml_mapping(root / "normalized_transform_spec.yaml"),
        "transform_type_catalog": _read_json(root / "transform_type_catalog.json"),
        "spec_dir": str(root),
    }


def validate_variant_transform_spec_artifact(
    *,
    spec_id: str,
    output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
) -> dict[str, Any]:
    root = output_dir / spec_id
    manifest = _read_optional_json(root / "variant_transform_spec_manifest.json") or {}
    normalized = (
        _load_yaml_mapping(root / "normalized_transform_spec.yaml")
        if (root / "normalized_transform_spec.yaml").exists()
        else {}
    )
    catalog = _read_optional_json(root / "transform_type_catalog.json") or {}
    types = {str(row.get("type")) for row in _records(catalog.get("transform_types"))}
    checks = _required_file_checks(
        root,
        (
            "variant_transform_spec_manifest.json",
            "normalized_transform_spec.yaml",
            "transform_type_catalog.json",
            "variant_transform_spec_report.md",
        ),
    )
    checks.extend(
        [
            _check("spec_id_matches", manifest.get("spec_id") == spec_id, ""),
            _check(
                "required_transform_types_present",
                set(DEFAULT_TRANSFORM_TYPES).issubset(types),
                ",".join(sorted(types)),
            ),
            _check("normalized_spec_present", bool(normalized.get("transform_types")), ""),
            _check("broker_forbidden", _payload_safe(manifest, catalog), ""),
            _check("experiment_safety_locked", _payload_experiment_safe(manifest, catalog), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_variant_transform_spec_validation",
        spec_id,
        checks,
    )


def load_weight_experiment_matrix_config(
    path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_experiment_factory_config_safe(payload)
    return payload


def validate_weight_experiment_matrix_config(
    path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_weight_experiment_matrix_config(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    else:
        variants = _records(payload.get("variants"))
        group = _mapping(payload.get("experiment_group"))
        variant_ids = [str(row.get("variant_id")) for row in variants]
        transform_types = {
            _text(transform.get("type"))
            for row in variants
            for transform in _records(row.get("transforms"))
        }
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check("experiment_group_id_present", bool(group.get("id")), ""),
                _check(
                    "base_method_limited_adjustment",
                    group.get("base_method") == "limited_adjustment",
                    _text(group.get("base_method")),
                ),
                _check("source_backfill_id_present", bool(group.get("source_backfill_id")), ""),
                _check("variants_present", bool(variants), ""),
                _check(
                    "variant_ids_unique",
                    len(variant_ids) == len(set(variant_ids)),
                    ",".join(variant_ids),
                ),
                _check(
                    "variants_have_transforms",
                    all(_records(row.get("transforms")) for row in variants),
                    "",
                ),
                _check(
                    "required_transform_families_covered",
                    {
                        "regime_gate",
                        "weight_smoothing",
                        "rebalance_threshold",
                        "consensus_aggregation",
                    }.issubset(transform_types),
                    ",".join(sorted(transform_types)),
                ),
                _check(
                    "safety_locked",
                    _experiment_safety_config_locked(_mapping(payload.get("safety"))),
                    "",
                ),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_experiment_matrix_config_validation",
        "weight_experiment_matrix_config",
        checks,
        extra={"config_path": str(path)},
    )


def build_experiment_matrix(
    *,
    config_path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
    output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_weight_experiment_matrix_config(config_path)
    validation = validate_weight_experiment_matrix_config(config_path)
    hypothesis_config = load_weight_optimization_hypothesis_config(
        _resolve_project_path(
            _mapping(config.get("source")).get("hypothesis_config"),
            DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
        )
    )
    transform_spec = load_weight_variant_transform_spec(
        _resolve_project_path(
            _mapping(config.get("source")).get("transform_spec_config"),
            DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
        )
    )
    hypothesis_lookup = {
        str(row.get("hypothesis_id")): row
        for row in _normalized_hypotheses(hypothesis_config)
        if row.get("hypothesis_id")
    }
    variant_specs = _normalized_variant_specs(config, hypothesis_lookup, transform_spec)
    transform_specs = _matrix_transform_specs(transform_spec, variant_specs)
    summary = _experiment_matrix_summary(variant_specs)
    matrix_id = _stable_id("experiment-matrix", config_path, generated.isoformat())
    root = _unique_dir(output_dir / matrix_id)
    root.mkdir(parents=True, exist_ok=False)
    group = _mapping(config.get("experiment_group"))
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_experiment_matrix_manifest",
        "matrix_id": root.name,
        "experiment_group_id": group.get("id"),
        "base_method": group.get("base_method"),
        "source_backfill_id": group.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": validation["status"],
        "config_path": str(config_path),
        "variant_count": len(variant_specs),
        "families_covered": summary["families_covered"],
        "failure_modes_covered": summary["failure_modes_covered"],
        "experiment_matrix_manifest_path": str(root / "experiment_matrix_manifest.json"),
        "variant_specs_path": str(root / "variant_specs.jsonl"),
        "transform_specs_path": str(root / "transform_specs.json"),
        "experiment_matrix_report_path": str(root / "experiment_matrix_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "experiment_matrix_manifest.json", manifest)
    _write_jsonl(root / "variant_specs.jsonl", variant_specs)
    _write_json(root / "transform_specs.json", transform_specs)
    _write_text(
        root / "experiment_matrix_report.md",
        render_experiment_matrix_report(manifest, variant_specs, transform_specs, summary),
    )
    _write_latest_pointer(
        "latest_experiment_matrix", root.name, root / "experiment_matrix_manifest.json"
    )
    return {
        "matrix_id": root.name,
        "matrix_dir": root,
        "manifest": manifest,
        "variant_specs": variant_specs,
        "transform_specs": transform_specs,
        "summary": summary,
        "validation": validation,
    }


def experiment_matrix_report_payload(
    *,
    matrix_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=matrix_id,
        latest_pointer="latest_experiment_matrix",
        latest=latest,
        output_dir=output_dir,
        required_name="experiment_matrix_manifest.json",
    )
    return {
        **_read_json(root / "experiment_matrix_manifest.json"),
        "variant_specs": _read_jsonl(root / "variant_specs.jsonl"),
        "transform_specs": _read_json(root / "transform_specs.json"),
        "matrix_dir": str(root),
    }


def validate_experiment_matrix_artifact(
    *,
    matrix_id: str,
    output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> dict[str, Any]:
    root = output_dir / matrix_id
    manifest = _read_optional_json(root / "experiment_matrix_manifest.json") or {}
    variants = _read_jsonl(root / "variant_specs.jsonl")
    transforms = _read_optional_json(root / "transform_specs.json") or {}
    transform_types = {
        _text(transform.get("type"))
        for row in variants
        for transform in _records(row.get("transforms"))
    }
    checks = _required_file_checks(
        root,
        (
            "experiment_matrix_manifest.json",
            "variant_specs.jsonl",
            "transform_specs.json",
            "experiment_matrix_report.md",
        ),
    )
    checks.extend(
        [
            _check("matrix_id_matches", manifest.get("matrix_id") == matrix_id, ""),
            _check("variants_present", bool(variants), ""),
            _check(
                "variants_have_target_failure_modes",
                all(_texts(row.get("target_failure_modes")) for row in variants),
                "",
            ),
            _check(
                "required_families_covered",
                {
                    "regime_gate",
                    "weight_smoothing",
                    "rebalance_threshold",
                    "consensus_aggregation",
                }.issubset(transform_types),
                ",".join(sorted(transform_types)),
            ),
            _check(
                "variants_experiment_only",
                all(row.get("experiment_only") is True for row in variants),
                "",
            ),
            _check(
                "not_formal_research_method",
                all(row.get("not_formal_research_method") is True for row in variants),
                "",
            ),
            _check("transform_specs_present", bool(transforms.get("transform_types")), ""),
            _check("broker_forbidden", _payload_safe(manifest, transforms, *variants), ""),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, transforms, *variants),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_experiment_matrix_validation", matrix_id, checks)


def run_batch_experiment(
    *,
    matrix_id: str,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    matrix = experiment_matrix_report_payload(matrix_id=matrix_id, output_dir=matrix_dir)
    source_backfill_id = _text(matrix.get("source_backfill_id"))
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=source_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = _load_backfill_config_from_manifest(backfill)
    start = _coerce_date(backfill.get("date_start"), AI_AFTER_CHATGPT_START)
    end = _coerce_date(backfill.get("date_end"), start)
    symbols = _symbols_from_state_paths(baseline_states)
    source = _mapping(config.get("source"))
    prices_path = price_cache_path or _resolve_project_path(
        source.get("price_cache_path"), DEFAULT_PRICE_CACHE_PATH
    )
    pivot = _load_price_pivot(prices_path, symbols, start)
    pivot = pivot.loc[pivot.index.date <= end]
    quality_as_of = max(end, generated.date())
    quality = _run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    if not quality.passed:
        raise DynamicV3SystemTargetError(f"data quality gate failed: {quality.status}")
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): _risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(matrix.get("variant_specs"))
    variant_states: list[dict[str, Any]] = []
    for variant in variant_specs:
        variant_states.extend(
            _run_variant_weight_path(
                variant=variant,
                baseline_states=baseline_states,
                returns=returns,
                labels=labels,
                config=config,
            )
        )
    performance = _variant_performance_metrics(variant_states, baseline_states)
    regime = _variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = _variant_stability_metrics(variant_states, baseline_states, config)
    batch_id = _stable_id("batch-experiment", matrix_id, generated.isoformat())
    root = _unique_dir(output_dir / batch_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_batch_experiment_manifest",
        "batch_id": root.name,
        "matrix_id": matrix_id,
        "source_backfill_id": source_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if performance else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "requested_start_date": backfill.get("requested_start_date"),
        "requested_end_date": backfill.get("requested_end_date"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "variants_total": len(variant_specs),
        "variants_completed": len({row.get("variant_id") for row in performance}),
        "variant_weight_paths_path": str(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics_path": str(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics_path": str(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics_path": str(root / "variant_stability_metrics.jsonl"),
        "batch_experiment_report_path": str(root / "batch_experiment_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "batch_experiment_manifest.json", manifest)
    _write_jsonl(root / "variant_weight_paths.jsonl", variant_states)
    _write_jsonl(root / "variant_performance_metrics.jsonl", performance)
    _write_jsonl(root / "variant_regime_metrics.jsonl", regime)
    _write_jsonl(root / "variant_stability_metrics.jsonl", stability)
    _write_text(
        root / "batch_experiment_report.md",
        render_batch_experiment_report(manifest, performance, regime, stability),
    )
    _write_latest_pointer(
        "latest_batch_experiment", root.name, root / "batch_experiment_manifest.json"
    )
    return {
        "batch_id": root.name,
        "batch_dir": root,
        "manifest": manifest,
        "variant_weight_paths": variant_states,
        "variant_performance_metrics": performance,
        "variant_regime_metrics": regime,
        "variant_stability_metrics": stability,
    }


def batch_experiment_report_payload(
    *,
    batch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=batch_id,
        latest_pointer="latest_batch_experiment",
        latest=latest,
        output_dir=output_dir,
        required_name="batch_experiment_manifest.json",
    )
    return {
        **_read_json(root / "batch_experiment_manifest.json"),
        "variant_weight_paths": _read_jsonl(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics": _read_jsonl(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics": _read_jsonl(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics": _read_jsonl(root / "variant_stability_metrics.jsonl"),
        "batch_dir": str(root),
    }


def validate_batch_experiment_artifact(
    *,
    batch_id: str,
    output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
) -> dict[str, Any]:
    root = output_dir / batch_id
    manifest = _read_optional_json(root / "batch_experiment_manifest.json") or {}
    weights = _read_jsonl(root / "variant_weight_paths.jsonl")
    performance = _read_jsonl(root / "variant_performance_metrics.jsonl")
    regime = _read_jsonl(root / "variant_regime_metrics.jsonl")
    stability = _read_jsonl(root / "variant_stability_metrics.jsonl")
    variants = {str(row.get("variant_id")) for row in performance}
    checks = _required_file_checks(
        root,
        (
            "batch_experiment_manifest.json",
            "variant_weight_paths.jsonl",
            "variant_performance_metrics.jsonl",
            "variant_regime_metrics.jsonl",
            "variant_stability_metrics.jsonl",
            "batch_experiment_report.md",
        ),
    )
    checks.extend(
        [
            _check("batch_id_matches", manifest.get("batch_id") == batch_id, ""),
            _check("variant_weight_paths_present", bool(weights), ""),
            _check("performance_metrics_present", bool(performance), ""),
            _check(
                "each_variant_has_regime_metrics",
                variants.issubset({str(row.get("variant_id")) for row in regime}),
                "",
            ),
            _check(
                "each_variant_has_stability_metrics",
                variants.issubset({str(row.get("variant_id")) for row in stability}),
                "",
            ),
            _check(
                "data_quality_visible",
                manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(manifest.get("data_quality_status")),
            ),
            _check("data_quality_as_of_visible", bool(manifest.get("data_quality_as_of")), ""),
            _check("broker_forbidden", _payload_safe(manifest, *weights, *performance), ""),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, *weights, *performance, *regime, *stability),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_batch_experiment_validation", batch_id, checks)


def run_experiment_triage(
    *,
    batch_id: str,
    batch_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    batch = batch_experiment_report_payload(batch_id=batch_id, output_dir=batch_dir)
    matrix = experiment_matrix_report_payload(
        matrix_id=_text(batch.get("matrix_id")),
        output_dir=matrix_dir,
    )
    variant_specs = _records(matrix.get("variant_specs"))
    policy = _mapping(
        _mapping(load_weight_experiment_matrix_config(Path(matrix["config_path"]))).get(
            "triage_policy"
        )
    )
    scorecard = _triage_variant_scorecard(batch, variant_specs, policy)
    candidates = _promotion_candidates(scorecard, variant_specs)
    rejected = [row for row in scorecard if row.get("triage_decision") == "REJECT"]
    summary = _triage_summary(scorecard, candidates)
    triage_id = _stable_id("experiment-triage", batch_id, generated.isoformat())
    root = _unique_dir(output_dir / triage_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_experiment_triage_manifest",
        "triage_id": root.name,
        "batch_id": batch_id,
        "matrix_id": batch.get("matrix_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if scorecard else "FAIL",
        "triage_policy_version": policy.get("policy_version", "weight_experiment_triage_v1"),
        "market_regime": batch.get("market_regime", "ai_after_chatgpt"),
        "date_start": batch.get("date_start"),
        "date_end": batch.get("date_end"),
        "data_quality_status": batch.get("data_quality_status"),
        "triage_manifest_path": str(root / "triage_manifest.json"),
        "variant_scorecard_path": str(root / "variant_scorecard.jsonl"),
        "promotion_candidates_path": str(root / "promotion_candidates.jsonl"),
        "rejected_variants_path": str(root / "rejected_variants.jsonl"),
        "triage_summary_path": str(root / "triage_summary.json"),
        "triage_report_path": str(root / "triage_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "triage_manifest.json", manifest)
    _write_jsonl(root / "variant_scorecard.jsonl", scorecard)
    _write_jsonl(root / "promotion_candidates.jsonl", candidates)
    _write_jsonl(root / "rejected_variants.jsonl", rejected)
    _write_json(root / "triage_summary.json", summary)
    _write_text(
        root / "triage_report.md",
        render_experiment_triage_report(manifest, summary, scorecard),
    )
    _write_latest_pointer(
        "latest_experiment_triage",
        root.name,
        root / "triage_manifest.json",
    )
    return {
        "triage_id": root.name,
        "triage_dir": root,
        "manifest": manifest,
        "variant_scorecard": scorecard,
        "promotion_candidates": candidates,
        "rejected_variants": rejected,
        "triage_summary": summary,
    }


def experiment_triage_report_payload(
    *,
    triage_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=triage_id,
        latest_pointer="latest_experiment_triage",
        latest=latest,
        output_dir=output_dir,
        required_name="triage_manifest.json",
    )
    return {
        **_read_json(root / "triage_manifest.json"),
        "variant_scorecard": _read_jsonl(root / "variant_scorecard.jsonl"),
        "promotion_candidates": _read_jsonl(root / "promotion_candidates.jsonl"),
        "rejected_variants": _read_jsonl(root / "rejected_variants.jsonl"),
        "triage_summary": _read_json(root / "triage_summary.json"),
        "triage_dir": str(root),
    }


def validate_experiment_triage_artifact(
    *,
    triage_id: str,
    output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> dict[str, Any]:
    root = output_dir / triage_id
    manifest = _read_optional_json(root / "triage_manifest.json") or {}
    scorecard = _read_jsonl(root / "variant_scorecard.jsonl")
    candidates = _read_jsonl(root / "promotion_candidates.jsonl")
    rejected = _read_jsonl(root / "rejected_variants.jsonl")
    summary = _read_optional_json(root / "triage_summary.json") or {}
    decisions = {str(row.get("triage_decision")) for row in scorecard}
    checks = _required_file_checks(
        root,
        (
            "triage_manifest.json",
            "variant_scorecard.jsonl",
            "promotion_candidates.jsonl",
            "rejected_variants.jsonl",
            "triage_summary.json",
            "triage_report.md",
        ),
    )
    checks.extend(
        [
            _check("triage_id_matches", manifest.get("triage_id") == triage_id, ""),
            _check("scorecard_present", bool(scorecard), ""),
            _check("decisions_valid", decisions.issubset(set(DEFAULT_TRIAGE_DECISIONS)), ""),
            _check(
                "hard_reject_effective",
                all(
                    row.get("triage_decision") != "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
                    for row in scorecard
                    if _texts(row.get("hard_reject_flags"))
                ),
                "",
            ),
            _check(
                "summary_counts_match",
                int(_float(summary.get("variants_total"))) == len(scorecard),
                "",
            ),
            _check(
                "promotion_candidates_subset_scorecard",
                {
                    _text(row.get("variant_id")) for row in candidates if row.get("variant_id")
                }.issubset(
                    {
                        _text(row.get("variant_id"))
                        for row in scorecard
                        if row.get("triage_decision") == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
                    }
                )
                and len(candidates) <= 2,
                "",
            ),
            _check("rejected_variants_present_or_empty", rejected is not None, ""),
            _check("broker_forbidden", _payload_safe(manifest, summary, *scorecard), ""),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *scorecard, *candidates),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_experiment_triage_validation", triage_id, checks)


def run_top_variant_interpretation(
    *,
    triage_id: str,
    triage_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    triage = experiment_triage_report_payload(triage_id=triage_id, output_dir=triage_dir)
    matrix = experiment_matrix_report_payload(
        matrix_id=_text(triage.get("matrix_id")),
        output_dir=matrix_dir,
    )
    variant_specs = _records(matrix.get("variant_specs"))
    explanations = _top_variant_explanations(triage, variant_specs)
    coverage = _variant_failure_mode_coverage(variant_specs, explanations)
    interpretation_id = _stable_id("top-variant-interpretation", triage_id, generated.isoformat())
    root = _unique_dir(output_dir / interpretation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_top_variant_interpretation_manifest",
        "interpretation_id": root.name,
        "triage_id": triage_id,
        "matrix_id": triage.get("matrix_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if explanations else "FAIL",
        "top_variant_count": len(explanations),
        "recommended_variant": _recommended_interpretation_variant(explanations),
        "top_variant_interpretation_manifest_path": str(
            root / "top_variant_interpretation_manifest.json"
        ),
        "top_variant_explanations_path": str(root / "top_variant_explanations.jsonl"),
        "variant_failure_mode_coverage_path": str(root / "variant_failure_mode_coverage.json"),
        "top_variant_interpretation_report_path": str(
            root / "top_variant_interpretation_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_top_variant_interpretation_reader_brief(manifest, explanations)
    _write_json(root / "top_variant_interpretation_manifest.json", manifest)
    _write_jsonl(root / "top_variant_explanations.jsonl", explanations)
    _write_json(root / "variant_failure_mode_coverage.json", coverage)
    _write_text(
        root / "top_variant_interpretation_report.md",
        render_top_variant_interpretation_report(manifest, explanations, coverage),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_top_variant_interpretation",
        root.name,
        root / "top_variant_interpretation_manifest.json",
    )
    return {
        "interpretation_id": root.name,
        "interpretation_dir": root,
        "manifest": manifest,
        "top_variant_explanations": explanations,
        "variant_failure_mode_coverage": coverage,
        "reader_brief_section": reader,
    }


def top_variant_interpretation_report_payload(
    *,
    interpretation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=interpretation_id,
        latest_pointer="latest_top_variant_interpretation",
        latest=latest,
        output_dir=output_dir,
        required_name="top_variant_interpretation_manifest.json",
    )
    return {
        **_read_json(root / "top_variant_interpretation_manifest.json"),
        "top_variant_explanations": _read_jsonl(root / "top_variant_explanations.jsonl"),
        "variant_failure_mode_coverage": _read_json(root / "variant_failure_mode_coverage.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "interpretation_dir": str(root),
    }


def validate_top_variant_interpretation_artifact(
    *,
    interpretation_id: str,
    output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = output_dir / interpretation_id
    manifest = _read_optional_json(root / "top_variant_interpretation_manifest.json") or {}
    explanations = _read_jsonl(root / "top_variant_explanations.jsonl")
    coverage = _read_optional_json(root / "variant_failure_mode_coverage.json") or {}
    checks = _required_file_checks(
        root,
        (
            "top_variant_interpretation_manifest.json",
            "top_variant_explanations.jsonl",
            "variant_failure_mode_coverage.json",
            "top_variant_interpretation_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "interpretation_id_matches",
                manifest.get("interpretation_id") == interpretation_id,
                "",
            ),
            _check("explanations_present", bool(explanations), ""),
            _check(
                "promoted_variants_explained",
                all(
                    row.get("recommended_promotion") is True
                    for row in explanations
                    if row.get("triage_decision") == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
                ),
                "",
            ),
            _check(
                "failure_mode_coverage_present",
                bool(_records(coverage.get("failure_modes"))),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, coverage, *explanations), ""),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, coverage, *explanations),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_top_variant_interpretation_validation",
        interpretation_id,
        checks,
    )


def run_method_promotion_plan(
    *,
    triage_id: str,
    interpretation_id: str,
    triage_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    interpretation_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
    output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    triage = experiment_triage_report_payload(triage_id=triage_id, output_dir=triage_dir)
    interpretation = top_variant_interpretation_report_payload(
        interpretation_id=interpretation_id,
        output_dir=interpretation_dir,
    )
    method_specs = _promoted_method_specs(triage, interpretation)
    promotion_plan_id = _stable_id(
        "method-promotion-plan", triage_id, interpretation_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / promotion_plan_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_method_promotion_plan_manifest",
        "promotion_plan_id": root.name,
        "triage_id": triage_id,
        "interpretation_id": interpretation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if method_specs["methods"] else "DEFER",
        "proposed_method_names": [
            row.get("proposed_method_name") for row in _records(method_specs.get("methods"))
        ],
        "implementation_scope": "research_only",
        "method_promotion_manifest_path": str(root / "method_promotion_manifest.json"),
        "promoted_method_specs_path": str(root / "promoted_method_specs.json"),
        "formal_implementation_plan_path": str(root / "formal_implementation_plan.md"),
        "owner_review_checklist_path": str(root / "owner_review_checklist.md"),
        "method_promotion_plan_report_path": str(root / "method_promotion_plan_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    plan = render_formal_implementation_plan(manifest, method_specs)
    checklist = render_method_promotion_owner_checklist(method_specs)
    report = render_method_promotion_plan_report(manifest, method_specs)
    reader = render_method_promotion_reader_brief(manifest, method_specs)
    _write_json(root / "method_promotion_manifest.json", manifest)
    _write_json(root / "promoted_method_specs.json", method_specs)
    _write_text(root / "formal_implementation_plan.md", plan)
    _write_text(root / "owner_review_checklist.md", checklist)
    _write_text(root / "method_promotion_plan_report.md", report)
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_method_promotion_plan",
        root.name,
        root / "method_promotion_manifest.json",
    )
    return {
        "promotion_plan_id": root.name,
        "promotion_plan_dir": root,
        "manifest": manifest,
        "promoted_method_specs": method_specs,
        "formal_implementation_plan": plan,
        "owner_review_checklist": checklist,
        "reader_brief_section": reader,
    }


def method_promotion_plan_report_payload(
    *,
    promotion_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_plan_id,
        latest_pointer="latest_method_promotion_plan",
        latest=latest,
        output_dir=output_dir,
        required_name="method_promotion_manifest.json",
    )
    return {
        **_read_json(root / "method_promotion_manifest.json"),
        "promoted_method_specs": _read_json(root / "promoted_method_specs.json"),
        "formal_implementation_plan": (root / "formal_implementation_plan.md").read_text(
            encoding="utf-8"
        ),
        "owner_review_checklist": (root / "owner_review_checklist.md").read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "promotion_plan_dir": str(root),
    }


def validate_method_promotion_plan_artifact(
    *,
    promotion_plan_id: str,
    output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_plan_id
    manifest = _read_optional_json(root / "method_promotion_manifest.json") or {}
    specs = _read_optional_json(root / "promoted_method_specs.json") or {}
    methods = _records(specs.get("methods"))
    method_names = [_text(row.get("proposed_method_name")) for row in methods]
    checks = _required_file_checks(
        root,
        (
            "method_promotion_manifest.json",
            "promoted_method_specs.json",
            "formal_implementation_plan.md",
            "owner_review_checklist.md",
            "method_promotion_plan_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "promotion_plan_id_matches",
                manifest.get("promotion_plan_id") == promotion_plan_id,
                "",
            ),
            _check("method_specs_present", bool(methods), ""),
            _check(
                "proposed_method_names_unique",
                len(method_names) == len(set(method_names)),
                ",".join(method_names),
            ),
            _check(
                "implementation_scope_research_only",
                all(row.get("implementation_scope") == "research_only" for row in methods),
                "",
            ),
            _check(
                "no_auto_apply",
                all(row.get("auto_apply") is False for row in methods),
                "",
            ),
            _check(
                "broker_forbidden",
                all(row.get("broker_action_allowed") is False for row in methods),
                "",
            ),
            _check(
                "production_effect_none",
                all(row.get("production_effect") == PRODUCTION_EFFECT for row in methods),
                "",
            ),
            _check("broker_forbidden_manifest", _payload_safe(manifest, specs, *methods), ""),
            _check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, specs, *methods),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_method_promotion_plan_validation",
        promotion_plan_id,
        checks,
    )






def run_limited_instability_diagnosis(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.run_limited_instability_diagnosis(*args, **kwargs)


def limited_instability_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.limited_instability_report_payload(*args, **kwargs)


def validate_limited_instability_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.validate_limited_instability_artifact(*args, **kwargs)


def run_limited_risk_attribution(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.run_limited_risk_attribution(*args, **kwargs)


def limited_risk_attribution_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.limited_risk_attribution_report_payload(*args, **kwargs)


def validate_limited_risk_attribution_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.validate_limited_risk_attribution_artifact(*args, **kwargs)


def run_data_warning_repair_plan(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.run_data_warning_repair_plan(*args, **kwargs)


def data_warning_repair_plan_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.data_warning_repair_plan_report_payload(*args, **kwargs)


def validate_data_warning_repair_plan_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.validate_data_warning_repair_plan_artifact(*args, **kwargs)


def run_alternative_method_review(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.run_alternative_method_review(*args, **kwargs)


def alternative_method_review_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.alternative_method_review_report_payload(*args, **kwargs)


def validate_alternative_method_review_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.validate_alternative_method_review_artifact(*args, **kwargs)


def run_refined_method_proposal(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.run_refined_method_proposal(*args, **kwargs)


def refined_method_proposal_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.refined_method_proposal_report_payload(*args, **kwargs)


def validate_refined_method_proposal_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_system_target_refinement as refinement,
    )

    return refinement.validate_refined_method_proposal_artifact(*args, **kwargs)



def generate_risk_capped_limited_target(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("generate_risk_capped_limited_target", *args, **kwargs)

def risk_capped_limited_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("risk_capped_limited_report_payload", *args, **kwargs)

def validate_risk_capped_limited_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("validate_risk_capped_limited_artifact", *args, **kwargs)

def run_risk_capped_backfill(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("run_risk_capped_backfill", *args, **kwargs)

def risk_capped_backfill_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("risk_capped_backfill_report_payload", *args, **kwargs)

def validate_risk_capped_backfill_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("validate_risk_capped_backfill_artifact", *args, **kwargs)

def run_risk_capped_comparison(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("run_risk_capped_comparison", *args, **kwargs)

def risk_capped_comparison_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("risk_capped_comparison_report_payload", *args, **kwargs)

def validate_risk_capped_comparison_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("validate_risk_capped_comparison_artifact", *args, **kwargs)

def build_risk_capped_review_pack(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("build_risk_capped_review_pack", *args, **kwargs)

def risk_capped_review_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("risk_capped_review_report_payload", *args, **kwargs)

def validate_risk_capped_review_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_risk_capped("validate_risk_capped_review_artifact", *args, **kwargs)


def generate_smoothed_limited_target(
    *,
    target_id: str,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR,
    regime_context: str = "normal",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_smoothed_limited_config(config_path)
    model_target = model_target_report_payload(target_id=target_id, output_dir=model_target_dir)
    model_config = load_model_target_config(
        _resolve_project_path(model_target.get("config_path"), DEFAULT_MODEL_TARGET_CONFIG_PATH)
    )
    method_weights = _mapping(
        _mapping(model_target.get("model_target_weights")).get("method_weights")
    )
    base_weights = _normalize_weights(_mapping(method_weights.get("limited_adjustment")))
    previous_default = _normalize_weights(
        _mapping(method_weights.get("static_baseline")) or base_weights
    )
    as_of = _coerce_date(model_target.get("as_of"), generated.date())
    target_rows: list[dict[str, Any]] = []
    smoothing_events: list[dict[str, Any]] = []
    lag_events: list[dict[str, Any]] = []
    for variant in _enabled_smoothed_variants(config):
        target_method = SMOOTHED_VARIANT_TO_METHOD[variant]
        previous = _normalize_weights(
            _mapping(method_weights.get(target_method)) or previous_default
        )
        result = _apply_smoothed_limited_adjustment(
            as_of=as_of,
            base_weights=base_weights,
            previous_smoothed_weights=previous,
            smoothed_config=config,
            model_config=model_config,
            variant_id=variant,
            regime_context=regime_context,
        )
        target_rows.append(
            {
                "as_of": as_of.isoformat(),
                "base_method": "limited_adjustment",
                "target_method": target_method,
                "base_weights": base_weights,
                "previous_smoothed_weights": previous,
                "smoothed_weights": result["smoothed_weights"],
                "smoothing_window_days": result["effective_policy"]["smoothing_window_days"],
                "alpha": result["effective_policy"]["alpha"],
                "regime_context": result["regime_context"],
                "research_target_only": True,
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        smoothing_events.extend(_records(result.get("smoothing_events")))
        lag_events.extend(_records(result.get("lag_events")))
    summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_weight_jump_reduction_summary",
        "status": "PASS" if target_rows else "FAIL",
        **_smoothed_weight_jump_reduction_summary(target_rows),
        **SYSTEM_TARGET_SAFETY,
    }
    smoothed_id = _stable_id(
        "smoothed-limited",
        target_id,
        config_path,
        target_rows,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / smoothed_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_limited_manifest",
        "smoothed_id": root.name,
        "target_id": target_id,
        "generated_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "status": "PASS" if target_rows else "FAIL",
        "base_method": "limited_adjustment",
        "target_methods": [row["target_method"] for row in target_rows],
        "config_path": str(config_path),
        "smoothed_limited_manifest_path": str(root / "smoothed_limited_manifest.json"),
        "smoothed_target_weights_path": str(root / "smoothed_target_weights.jsonl"),
        "smoothing_events_path": str(root / "smoothing_events.jsonl"),
        "lag_events_path": str(root / "lag_events.jsonl"),
        "weight_jump_reduction_summary_path": str(root / "weight_jump_reduction_summary.json"),
        "smoothed_limited_report_path": str(root / "smoothed_limited_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_limited_manifest.json", manifest)
    _write_jsonl(root / "smoothed_target_weights.jsonl", target_rows)
    _write_jsonl(root / "smoothing_events.jsonl", smoothing_events)
    _write_jsonl(root / "lag_events.jsonl", lag_events)
    _write_json(root / "weight_jump_reduction_summary.json", summary)
    _write_text(
        root / "smoothed_limited_report.md",
        render_smoothed_limited_report(manifest, target_rows, summary, lag_events),
    )
    _write_latest_pointer(
        "latest_smoothed_limited", root.name, root / "smoothed_limited_manifest.json"
    )
    return {
        "smoothed_id": root.name,
        "smoothed_dir": root,
        "manifest": manifest,
        "smoothed_target_weights": target_rows,
        "smoothing_events": smoothing_events,
        "lag_events": lag_events,
        "weight_jump_reduction_summary": summary,
    }


def smoothed_limited_report_payload(
    *,
    smoothed_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=smoothed_id,
        latest_pointer="latest_smoothed_limited",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_limited_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_limited_manifest.json"),
        "smoothed_target_weights": _read_jsonl(root / "smoothed_target_weights.jsonl"),
        "smoothing_events": _read_jsonl(root / "smoothing_events.jsonl"),
        "lag_events": _read_jsonl(root / "lag_events.jsonl"),
        "weight_jump_reduction_summary": _read_json(root / "weight_jump_reduction_summary.json"),
        "smoothed_dir": str(root),
    }


def validate_smoothed_limited_artifact(
    *,
    smoothed_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR,
) -> dict[str, Any]:
    root = output_dir / smoothed_id
    manifest = _read_optional_json(root / "smoothed_limited_manifest.json") or {}
    rows = _read_jsonl(root / "smoothed_target_weights.jsonl")
    summary = _read_optional_json(root / "weight_jump_reduction_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_limited_manifest.json",
            "smoothed_target_weights.jsonl",
            "smoothing_events.jsonl",
            "lag_events.jsonl",
            "weight_jump_reduction_summary.json",
            "smoothed_limited_report.md",
        ),
    )
    target_methods = {row.get("target_method") for row in rows}
    checks.extend(
        [
            _check("smoothed_id_matches", manifest.get("smoothed_id") == smoothed_id, ""),
            _check("target_rows_present", bool(rows), ""),
            _check(
                "smoothed_methods_present",
                {
                    "smooth_weights_3d_limited_adjustment",
                    "smooth_weights_5d_limited_adjustment",
                }.issubset(target_methods),
                ",".join(sorted(str(item) for item in target_methods)),
            ),
            _check(
                "weights_sum_to_one",
                all(_weights_sum_to_one(row.get("smoothed_weights")) for row in rows),
                "",
            ),
            _check(
                "weight_jump_summary_present",
                bool(_records(summary.get("target_methods"))),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *rows), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_limited_validation",
        smoothed_id,
        checks,
    )


def run_smoothed_backfill(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    paper_shadow_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_backfill = run_paper_shadow_backfill(
        config_path=config_path,
        output_dir=paper_shadow_backfill_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated,
    )
    smoothed_methods = set(SMOOTHED_METHOD_TO_VARIANT)
    states = [
        row
        for row in source_backfill["backfill_method_states"]
        if row.get("target_method") in smoothed_methods
    ]
    ledger = [
        row
        for row in source_backfill["backfill_trade_ledger"]
        if row.get("target_method") in smoothed_methods
    ]
    smoothing_events = [event for row in ledger for event in _records(row.get("smoothing_events"))]
    lag_events = [event for row in ledger for event in _records(row.get("lag_events"))]
    summary = _smoothed_backfill_summary(
        source_backfill["manifest"],
        states,
        ledger,
        smoothing_events,
        lag_events,
    )
    backfill_id = _stable_id(
        "smoothed-backfill",
        source_backfill["backfill_id"],
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_backfill_manifest",
        "smoothed_backfill_id": root.name,
        "source_paper_shadow_backfill_id": source_backfill["backfill_id"],
        "generated_at": generated.isoformat(),
        "status": "PASS" if states else "FAIL",
        "methods": sorted(smoothed_methods),
        "market_regime": source_backfill["manifest"].get("market_regime", "ai_after_chatgpt"),
        "date_start": source_backfill["manifest"].get("date_start"),
        "date_end": source_backfill["manifest"].get("date_end"),
        "smoothed_backfill_manifest_path": str(root / "smoothed_backfill_manifest.json"),
        "smoothed_method_states_path": str(root / "smoothed_method_states.jsonl"),
        "smoothed_trade_ledger_path": str(root / "smoothed_trade_ledger.jsonl"),
        "smoothed_backfill_summary_path": str(root / "smoothed_backfill_summary.json"),
        "smoothed_backfill_report_path": str(root / "smoothed_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_backfill_manifest.json", manifest)
    _write_jsonl(root / "smoothed_method_states.jsonl", states)
    _write_jsonl(root / "smoothed_trade_ledger.jsonl", ledger)
    _write_json(root / "smoothed_backfill_summary.json", summary)
    _write_text(
        root / "smoothed_backfill_report.md",
        render_smoothed_backfill_report(manifest, summary),
    )
    _write_latest_pointer(
        "latest_smoothed_backfill", root.name, root / "smoothed_backfill_manifest.json"
    )
    return {
        "smoothed_backfill_id": root.name,
        "smoothed_backfill_dir": root,
        "source_paper_shadow_backfill": source_backfill,
        "manifest": manifest,
        "smoothed_method_states": states,
        "smoothed_trade_ledger": ledger,
        "smoothed_backfill_summary": summary,
        "smoothing_events": smoothing_events,
        "lag_events": lag_events,
    }


def smoothed_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=backfill_id,
        latest_pointer="latest_smoothed_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_backfill_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_backfill_manifest.json"),
        "smoothed_method_states": _read_jsonl(root / "smoothed_method_states.jsonl"),
        "smoothed_trade_ledger": _read_jsonl(root / "smoothed_trade_ledger.jsonl"),
        "smoothed_backfill_summary": _read_json(root / "smoothed_backfill_summary.json"),
        "smoothed_backfill_dir": str(root),
    }


def validate_smoothed_backfill_artifact(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / backfill_id
    manifest = _read_optional_json(root / "smoothed_backfill_manifest.json") or {}
    states = _read_jsonl(root / "smoothed_method_states.jsonl")
    ledger = _read_jsonl(root / "smoothed_trade_ledger.jsonl")
    summary = _read_optional_json(root / "smoothed_backfill_summary.json") or {}
    methods = {row.get("target_method") for row in states}
    checks = _required_file_checks(
        root,
        (
            "smoothed_backfill_manifest.json",
            "smoothed_method_states.jsonl",
            "smoothed_trade_ledger.jsonl",
            "smoothed_backfill_summary.json",
            "smoothed_backfill_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "backfill_id_matches",
                manifest.get("smoothed_backfill_id") == backfill_id,
                "",
            ),
            _check("states_present", bool(states), ""),
            _check(
                "smoothed_methods_present",
                set(SMOOTHED_METHOD_TO_VARIANT).issubset(methods),
                ",".join(sorted(str(item) for item in methods)),
            ),
            _check("trade_ledger_present", bool(ledger), ""),
            _check(
                "data_quality_visible",
                summary.get("data_quality") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(summary.get("data_quality")),
            ),
            _check(
                "broker_action_taken_false",
                summary.get("broker_action_taken") is False
                and all(row.get("broker_action_taken") is False for row in ledger),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *states, *ledger), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_backfill_validation",
        backfill_id,
        checks,
    )


def run_smoothed_comparison(
    *,
    smoothed_backfill_id: str,
    baseline_backfill_id: str,
    risk_capped_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    smoothed = smoothed_backfill_report_payload(
        backfill_id=smoothed_backfill_id,
        output_dir=smoothed_backfill_dir,
    )
    baseline = paper_shadow_backfill_report_payload(
        backfill_id=baseline_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    risk_capped = risk_capped_backfill_report_payload(
        backfill_id=risk_capped_backfill_id,
        output_dir=risk_capped_backfill_dir,
    )
    smoothed_states = _records(smoothed.get("smoothed_method_states"))
    baseline_states = _records(baseline.get("backfill_method_states"))
    risk_states = _records(risk_capped.get("risk_capped_method_states"))
    combined_states = [*baseline_states, *risk_states, *smoothed_states]
    smoothed_ledger = _records(smoothed.get("smoothed_trade_ledger"))
    metrics = _smoothed_vs_limited_metrics(smoothed_states, baseline_states, risk_states)
    regime = _smoothed_regime_comparison(combined_states, baseline)
    rolling = _smoothed_rolling_comparison(combined_states, baseline)
    stability = _smoothed_stability_comparison(smoothed_states, baseline_states)
    lag_cost = _smoothed_lag_cost_analysis(combined_states, smoothed_ledger, baseline)
    comparison_id = _stable_id(
        "smoothed-comparison",
        smoothed_backfill_id,
        baseline_backfill_id,
        risk_capped_backfill_id,
        metrics,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / comparison_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_comparison_manifest",
        "comparison_id": root.name,
        "smoothed_backfill_id": smoothed_backfill_id,
        "baseline_backfill_id": baseline_backfill_id,
        "risk_capped_backfill_id": risk_capped_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "date_start": baseline.get("date_start"),
        "date_end": baseline.get("date_end"),
        "smoothed_comparison_manifest_path": str(root / "smoothed_comparison_manifest.json"),
        "smoothed_vs_limited_metrics_path": str(root / "smoothed_vs_limited_metrics.json"),
        "smoothed_regime_comparison_path": str(root / "smoothed_regime_comparison.json"),
        "smoothed_rolling_comparison_path": str(root / "smoothed_rolling_comparison.json"),
        "smoothed_stability_comparison_path": str(root / "smoothed_stability_comparison.json"),
        "smoothing_lag_cost_analysis_path": str(root / "smoothing_lag_cost_analysis.json"),
        "smoothed_comparison_report_path": str(root / "smoothed_comparison_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_comparison_manifest.json", manifest)
    _write_json(root / "smoothed_vs_limited_metrics.json", metrics)
    _write_json(root / "smoothed_regime_comparison.json", regime)
    _write_json(root / "smoothed_rolling_comparison.json", rolling)
    _write_json(root / "smoothed_stability_comparison.json", stability)
    _write_json(root / "smoothing_lag_cost_analysis.json", lag_cost)
    _write_text(
        root / "smoothed_comparison_report.md",
        render_smoothed_comparison_report(manifest, metrics, regime, rolling, stability, lag_cost),
    )
    _write_latest_pointer(
        "latest_smoothed_comparison", root.name, root / "smoothed_comparison_manifest.json"
    )
    return {
        "comparison_id": root.name,
        "comparison_dir": root,
        "manifest": manifest,
        "smoothed_vs_limited_metrics": metrics,
        "smoothed_regime_comparison": regime,
        "smoothed_rolling_comparison": rolling,
        "smoothed_stability_comparison": stability,
        "smoothing_lag_cost_analysis": lag_cost,
    }


def smoothed_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=comparison_id,
        latest_pointer="latest_smoothed_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_comparison_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_comparison_manifest.json"),
        "smoothed_vs_limited_metrics": _read_json(root / "smoothed_vs_limited_metrics.json"),
        "smoothed_regime_comparison": _read_json(root / "smoothed_regime_comparison.json"),
        "smoothed_rolling_comparison": _read_json(root / "smoothed_rolling_comparison.json"),
        "smoothed_stability_comparison": _read_json(root / "smoothed_stability_comparison.json"),
        "smoothing_lag_cost_analysis": _read_json(root / "smoothing_lag_cost_analysis.json"),
        "comparison_dir": str(root),
    }


def validate_smoothed_comparison_artifact(
    *,
    comparison_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> dict[str, Any]:
    root = output_dir / comparison_id
    manifest = _read_optional_json(root / "smoothed_comparison_manifest.json") or {}
    metrics = _read_optional_json(root / "smoothed_vs_limited_metrics.json") or {}
    regime = _read_optional_json(root / "smoothed_regime_comparison.json") or {}
    rolling = _read_optional_json(root / "smoothed_rolling_comparison.json") or {}
    stability = _read_optional_json(root / "smoothed_stability_comparison.json") or {}
    lag_cost = _read_optional_json(root / "smoothing_lag_cost_analysis.json") or {}
    comparison_pairs = _records(metrics.get("comparisons"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_comparison_manifest.json",
            "smoothed_vs_limited_metrics.json",
            "smoothed_regime_comparison.json",
            "smoothed_rolling_comparison.json",
            "smoothed_stability_comparison.json",
            "smoothing_lag_cost_analysis.json",
            "smoothed_comparison_report.md",
        ),
    )
    checks.extend(
        [
            _check("comparison_id_matches", manifest.get("comparison_id") == comparison_id, ""),
            _check(
                "smooth_3d_vs_limited_present",
                any(
                    row.get("method_a") == "smooth_weights_3d_limited_adjustment"
                    and row.get("method_b") == "limited_adjustment"
                    for row in comparison_pairs
                ),
                "",
            ),
            _check(
                "smooth_5d_vs_limited_present",
                any(
                    row.get("method_a") == "smooth_weights_5d_limited_adjustment"
                    and row.get("method_b") == "limited_adjustment"
                    for row in comparison_pairs
                ),
                "",
            ),
            _check("regime_comparison_present", bool(_records(regime.get("regimes"))), ""),
            _check(
                "rolling_comparison_present",
                bool(_records(rolling.get("methods"))),
                "",
            ),
            _check(
                "stability_comparison_present",
                bool(_records(stability.get("methods"))),
                "",
            ),
            _check(
                "lag_cost_present",
                bool(_records(lag_cost.get("methods"))),
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, metrics, regime, rolling, stability, lag_cost),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_comparison_validation",
        comparison_id,
        checks,
    )


def build_smoothed_review_pack(
    *,
    comparison_id: str,
    smoothed_backfill_id: str,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    comparison = smoothed_comparison_report_payload(
        comparison_id=comparison_id,
        output_dir=comparison_dir,
    )
    backfill = smoothed_backfill_report_payload(
        backfill_id=smoothed_backfill_id,
        output_dir=smoothed_backfill_dir,
    )
    decision = _smoothed_review_decision(comparison, backfill)
    review_id = _stable_id(
        "smoothed-review",
        comparison_id,
        smoothed_backfill_id,
        decision,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["review_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_review_manifest",
        "review_id": root.name,
        "comparison_id": comparison_id,
        "smoothed_backfill_id": smoothed_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_review_manifest_path": str(root / "smoothed_review_manifest.json"),
        "smoothed_decision_path": str(root / "smoothed_decision.json"),
        "owner_smoothed_checklist_path": str(root / "owner_smoothed_checklist.md"),
        "smoothed_review_report_path": str(root / "smoothed_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_review_manifest.json", manifest)
    _write_json(root / "smoothed_decision.json", decision)
    _write_text(root / "owner_smoothed_checklist.md", render_smoothed_owner_checklist(decision))
    _write_text(
        root / "smoothed_review_report.md",
        render_smoothed_review_report(manifest, decision, comparison, backfill),
    )
    _write_text(root / "reader_brief_section.md", render_smoothed_review_reader_brief(decision))
    _write_latest_pointer(
        "latest_smoothed_review", root.name, root / "smoothed_review_manifest.json"
    )
    return {
        "review_id": root.name,
        "review_dir": root,
        "manifest": manifest,
        "smoothed_decision": decision,
        "reader_brief_section": render_smoothed_review_reader_brief(decision),
    }


def smoothed_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_smoothed_review",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_review_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_review_manifest.json"),
        "smoothed_decision": _read_json(root / "smoothed_decision.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "review_dir": str(root),
    }


def validate_smoothed_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / review_id
    manifest = _read_optional_json(root / "smoothed_review_manifest.json") or {}
    decision = _read_optional_json(root / "smoothed_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_review_manifest.json",
            "smoothed_decision.json",
            "owner_smoothed_checklist.md",
            "smoothed_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "review_id_matches",
                manifest.get("review_id") == review_id and decision.get("review_id") == review_id,
                "",
            ),
            _check(
                "decision_valid",
                decision.get("decision")
                in {
                    "PROMOTE_TO_RECOMMENDED_RESEARCH",
                    "CONTINUE_OBSERVATION",
                    "REVIEW_REQUIRED",
                    "REJECT",
                },
                _text(decision.get("decision")),
            ),
            _check(
                "recommended_method_valid",
                decision.get("recommended_method") == "smooth_weights_3d_limited_adjustment",
                _text(decision.get("recommended_method")),
            ),
            _check("research_target_only_true", decision.get("research_target_only") is True, ""),
            _check(
                "not_official_target_weights_true",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                decision.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", decision.get("production_effect") == "none", ""),
            _check(
                "requires_forward_confirmation_true",
                decision.get("requires_forward_confirmation") is True,
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_review_validation",
        review_id,
        checks,
    )


def run_smoothed_review_attribution(
    *,
    review_id: str,
    comparison_id: str,
    backfill_id: str,
    review_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review = smoothed_review_report_payload(review_id=review_id, output_dir=review_dir)
    comparison = smoothed_comparison_report_payload(
        comparison_id=comparison_id,
        output_dir=comparison_dir,
    )
    backfill = smoothed_backfill_report_payload(backfill_id=backfill_id, output_dir=backfill_dir)
    breakdown = _smoothed_decision_reason_breakdown(review, comparison, backfill)
    support_matrix = _smoothed_metric_support_matrix(comparison)
    attribution_id = _stable_id(
        "smoothed-review-attribution",
        review_id,
        comparison_id,
        backfill_id,
        breakdown,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_review_attribution_manifest",
        "attribution_id": root.name,
        "review_id": review_id,
        "comparison_id": comparison_id,
        "smoothed_backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": _mapping(backfill.get("smoothed_backfill_summary")).get(
            "data_quality"
        ),
        "smoothed_review_attribution_manifest_path": str(
            root / "smoothed_review_attribution_manifest.json"
        ),
        "smoothed_decision_reason_breakdown_path": str(
            root / "smoothed_decision_reason_breakdown.json"
        ),
        "smoothed_metric_support_matrix_path": str(root / "smoothed_metric_support_matrix.json"),
        "smoothed_review_attribution_report_path": str(
            root / "smoothed_review_attribution_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_review_attribution_manifest.json", manifest)
    _write_json(root / "smoothed_decision_reason_breakdown.json", breakdown)
    _write_json(root / "smoothed_metric_support_matrix.json", support_matrix)
    _write_text(
        root / "smoothed_review_attribution_report.md",
        render_smoothed_review_attribution_report(manifest, breakdown, support_matrix),
    )
    _write_latest_pointer(
        "latest_smoothed_review_attribution",
        root.name,
        root / "smoothed_review_attribution_manifest.json",
    )
    return {
        "attribution_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "smoothed_decision_reason_breakdown": breakdown,
        "smoothed_metric_support_matrix": support_matrix,
    }


def smoothed_review_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_smoothed_review_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_review_attribution_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_review_attribution_manifest.json"),
        "smoothed_decision_reason_breakdown": _read_json(
            root / "smoothed_decision_reason_breakdown.json"
        ),
        "smoothed_metric_support_matrix": _read_json(root / "smoothed_metric_support_matrix.json"),
        "attribution_dir": str(root),
    }


def validate_smoothed_review_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    manifest = _read_optional_json(root / "smoothed_review_attribution_manifest.json") or {}
    breakdown = _read_optional_json(root / "smoothed_decision_reason_breakdown.json") or {}
    support_matrix = _read_optional_json(root / "smoothed_metric_support_matrix.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_review_attribution_manifest.json",
            "smoothed_decision_reason_breakdown.json",
            "smoothed_metric_support_matrix.json",
            "smoothed_review_attribution_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "attribution_id_matches",
                manifest.get("attribution_id") == attribution_id,
                "",
            ),
            _check(
                "decision_visible",
                breakdown.get("decision")
                in {
                    "PROMOTE_TO_RECOMMENDED_RESEARCH",
                    "CONTINUE_OBSERVATION",
                    "REJECT",
                    "REVIEW_REQUIRED",
                },
                _text(breakdown.get("decision")),
            ),
            _check(
                "supporting_reasons_present",
                bool(_records(breakdown.get("supporting_reasons"))),
                "",
            ),
            _check(
                "blocking_reasons_present",
                bool(_records(breakdown.get("blocking_reasons"))),
                "",
            ),
            _check("why_not_promote_present", bool(breakdown.get("why_not_promote")), ""),
            _check("why_not_reject_present", bool(breakdown.get("why_not_reject")), ""),
            _check(
                "metric_matrix_present",
                bool(_records(support_matrix.get("metrics"))),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, breakdown, support_matrix), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_review_attribution_validation",
        attribution_id,
        checks,
    )


def run_smoothing_benefit_lag_drilldown(
    *,
    smoothed_backfill_id: str,
    comparison_id: str,
    backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = smoothed_backfill_report_payload(
        backfill_id=smoothed_backfill_id,
        output_dir=backfill_dir,
    )
    comparison = smoothed_comparison_report_payload(
        comparison_id=comparison_id,
        output_dir=comparison_dir,
    )
    benefit = _smoothing_benefit_summary(comparison)
    lag = _smoothing_lag_cost_summary(comparison)
    tradeoff = _smoothing_benefit_lag_tradeoff_matrix(benefit, lag)
    drilldown_id = _stable_id(
        "smoothing-benefit-lag",
        smoothed_backfill_id,
        comparison_id,
        benefit,
        lag,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / drilldown_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothing_benefit_lag_manifest",
        "drilldown_id": root.name,
        "smoothed_backfill_id": smoothed_backfill_id,
        "comparison_id": comparison_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": _mapping(backfill.get("smoothed_backfill_summary")).get(
            "data_quality"
        ),
        "smoothing_benefit_lag_manifest_path": str(root / "smoothing_benefit_lag_manifest.json"),
        "smoothing_benefit_summary_path": str(root / "smoothing_benefit_summary.json"),
        "lag_cost_summary_path": str(root / "lag_cost_summary.json"),
        "benefit_lag_tradeoff_matrix_path": str(root / "benefit_lag_tradeoff_matrix.json"),
        "smoothing_benefit_lag_report_path": str(root / "smoothing_benefit_lag_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothing_benefit_lag_manifest.json", manifest)
    _write_json(root / "smoothing_benefit_summary.json", benefit)
    _write_json(root / "lag_cost_summary.json", lag)
    _write_json(root / "benefit_lag_tradeoff_matrix.json", tradeoff)
    _write_text(
        root / "smoothing_benefit_lag_report.md",
        render_smoothing_benefit_lag_report(manifest, benefit, lag, tradeoff),
    )
    _write_latest_pointer(
        "latest_smoothing_benefit_lag",
        root.name,
        root / "smoothing_benefit_lag_manifest.json",
    )
    return {
        "drilldown_id": root.name,
        "drilldown_dir": root,
        "manifest": manifest,
        "smoothing_benefit_summary": benefit,
        "lag_cost_summary": lag,
        "benefit_lag_tradeoff_matrix": tradeoff,
    }


def smoothing_benefit_lag_report_payload(
    *,
    drilldown_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=drilldown_id,
        latest_pointer="latest_smoothing_benefit_lag",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothing_benefit_lag_manifest.json",
    )
    return {
        **_read_json(root / "smoothing_benefit_lag_manifest.json"),
        "smoothing_benefit_summary": _read_json(root / "smoothing_benefit_summary.json"),
        "lag_cost_summary": _read_json(root / "lag_cost_summary.json"),
        "benefit_lag_tradeoff_matrix": _read_json(root / "benefit_lag_tradeoff_matrix.json"),
        "drilldown_dir": str(root),
    }


def validate_smoothing_benefit_lag_artifact(
    *,
    drilldown_id: str,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> dict[str, Any]:
    root = output_dir / drilldown_id
    manifest = _read_optional_json(root / "smoothing_benefit_lag_manifest.json") or {}
    benefit = _read_optional_json(root / "smoothing_benefit_summary.json") or {}
    lag = _read_optional_json(root / "lag_cost_summary.json") or {}
    tradeoff = _read_optional_json(root / "benefit_lag_tradeoff_matrix.json") or {}
    methods = {row.get("method") for row in _records(benefit.get("methods"))}
    checks = _required_file_checks(
        root,
        (
            "smoothing_benefit_lag_manifest.json",
            "smoothing_benefit_summary.json",
            "lag_cost_summary.json",
            "benefit_lag_tradeoff_matrix.json",
            "smoothing_benefit_lag_report.md",
        ),
    )
    checks.extend(
        [
            _check("drilldown_id_matches", manifest.get("drilldown_id") == drilldown_id, ""),
            _check(
                "smoothed_methods_present",
                set(SMOOTHED_METHOD_TO_VARIANT).issubset(methods),
                ",".join(sorted(str(item) for item in methods)),
            ),
            _check("lag_methods_present", bool(_records(lag.get("methods"))), ""),
            _check("tradeoff_present", bool(_records(tradeoff.get("methods"))), ""),
            _check("broker_forbidden", _payload_safe(manifest, benefit, lag, tradeoff), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothing_benefit_lag_validation",
        drilldown_id,
        checks,
    )


def run_smoothed_regime_validation(
    *,
    smoothed_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    smoothed = smoothed_backfill_report_payload(
        backfill_id=smoothed_backfill_id,
        output_dir=smoothed_backfill_dir,
    )
    source_id = _text(smoothed.get("source_paper_shadow_backfill_id"))
    baseline = paper_shadow_backfill_report_payload(
        backfill_id=source_id,
        output_dir=baseline_backfill_dir,
    )
    sideways = _smoothed_sideways_validation_summary(smoothed, baseline)
    recovery = _smoothed_recovery_lag_validation_summary(smoothed, baseline)
    regime_validation_id = _stable_id(
        "smoothed-regime-validation",
        smoothed_backfill_id,
        source_id,
        sideways,
        recovery,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / regime_validation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_regime_validation_manifest",
        "regime_validation_id": root.name,
        "smoothed_backfill_id": smoothed_backfill_id,
        "baseline_backfill_id": source_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": smoothed.get("market_regime", "ai_after_chatgpt"),
        "date_start": smoothed.get("date_start"),
        "date_end": smoothed.get("date_end"),
        "data_quality_status": _mapping(smoothed.get("smoothed_backfill_summary")).get(
            "data_quality"
        ),
        "smoothed_regime_validation_manifest_path": str(
            root / "smoothed_regime_validation_manifest.json"
        ),
        "sideways_validation_summary_path": str(root / "sideways_validation_summary.json"),
        "recovery_lag_validation_summary_path": str(root / "recovery_lag_validation_summary.json"),
        "smoothed_regime_validation_report_path": str(
            root / "smoothed_regime_validation_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_regime_validation_manifest.json", manifest)
    _write_json(root / "sideways_validation_summary.json", sideways)
    _write_json(root / "recovery_lag_validation_summary.json", recovery)
    _write_text(
        root / "smoothed_regime_validation_report.md",
        render_smoothed_regime_validation_report(manifest, sideways, recovery),
    )
    _write_latest_pointer(
        "latest_smoothed_regime_validation",
        root.name,
        root / "smoothed_regime_validation_manifest.json",
    )
    return {
        "regime_validation_id": root.name,
        "regime_validation_dir": root,
        "manifest": manifest,
        "sideways_validation_summary": sideways,
        "recovery_lag_validation_summary": recovery,
    }


def smoothed_regime_validation_report_payload(
    *,
    regime_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=regime_validation_id,
        latest_pointer="latest_smoothed_regime_validation",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_regime_validation_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_regime_validation_manifest.json"),
        "sideways_validation_summary": _read_json(root / "sideways_validation_summary.json"),
        "recovery_lag_validation_summary": _read_json(
            root / "recovery_lag_validation_summary.json"
        ),
        "regime_validation_dir": str(root),
    }


def validate_smoothed_regime_validation_artifact(
    *,
    regime_validation_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> dict[str, Any]:
    root = output_dir / regime_validation_id
    manifest = _read_optional_json(root / "smoothed_regime_validation_manifest.json") or {}
    sideways = _read_optional_json(root / "sideways_validation_summary.json") or {}
    recovery = _read_optional_json(root / "recovery_lag_validation_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_regime_validation_manifest.json",
            "sideways_validation_summary.json",
            "recovery_lag_validation_summary.json",
            "smoothed_regime_validation_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "regime_validation_id_matches",
                manifest.get("regime_validation_id") == regime_validation_id,
                "",
            ),
            _check("sideways_methods_present", bool(_records(sideways.get("methods"))), ""),
            _check("recovery_methods_present", bool(_records(recovery.get("methods"))), ""),
            _check(
                "sideways_regime_visible",
                sideways.get("regime") == "sideways_choppy",
                _text(sideways.get("regime")),
            ),
            _check(
                "recovery_regime_visible",
                recovery.get("regime") == "strong_recovery",
                _text(recovery.get("regime")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, sideways, recovery), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_regime_validation_validation",
        regime_validation_id,
        checks,
    )


def register_smoothed_confirmation_targets(
    *,
    review_id: str,
    regime_validation_id: str,
    review_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review = smoothed_review_report_payload(review_id=review_id, output_dir=review_dir)
    regime = smoothed_regime_validation_report_payload(
        regime_validation_id=regime_validation_id,
        output_dir=regime_validation_dir,
    )
    targets = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_confirmation_targets",
        "status": "PASS",
        **_smoothed_confirmation_targets(review, regime),
        **SYSTEM_TARGET_SAFETY,
    }
    confirmation_id = _stable_id(
        "smoothed-confirmation",
        review_id,
        regime_validation_id,
        targets,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / confirmation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_confirmation_manifest",
        "confirmation_id": root.name,
        "review_id": review_id,
        "regime_validation_id": regime_validation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "target_count": len(_records(targets.get("targets"))),
        "smoothed_confirmation_manifest_path": str(root / "smoothed_confirmation_manifest.json"),
        "smoothed_confirmation_targets_path": str(root / "smoothed_confirmation_targets.json"),
        "smoothed_confirmation_report_path": str(root / "smoothed_confirmation_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_confirmation_manifest.json", manifest)
    _write_json(root / "smoothed_confirmation_targets.json", targets)
    _write_text(
        root / "smoothed_confirmation_report.md",
        render_smoothed_confirmation_report(manifest, targets),
    )
    _write_latest_pointer(
        "latest_smoothed_confirmation",
        root.name,
        root / "smoothed_confirmation_manifest.json",
    )
    return {
        "confirmation_id": root.name,
        "confirmation_dir": root,
        "manifest": manifest,
        "smoothed_confirmation_targets": targets,
    }


def smoothed_confirmation_report_payload(
    *,
    confirmation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=confirmation_id,
        latest_pointer="latest_smoothed_confirmation",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_confirmation_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_confirmation_manifest.json"),
        "smoothed_confirmation_targets": _read_json(root / "smoothed_confirmation_targets.json"),
        "confirmation_dir": str(root),
    }


def validate_smoothed_confirmation_artifact(
    *,
    confirmation_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = output_dir / confirmation_id
    manifest = _read_optional_json(root / "smoothed_confirmation_manifest.json") or {}
    targets = _read_optional_json(root / "smoothed_confirmation_targets.json") or {}
    target_rows = _records(targets.get("targets"))
    target_ids = {row.get("target_id") for row in target_rows}
    checks = _required_file_checks(
        root,
        (
            "smoothed_confirmation_manifest.json",
            "smoothed_confirmation_targets.json",
            "smoothed_confirmation_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "confirmation_id_matches",
                manifest.get("confirmation_id") == confirmation_id,
                "",
            ),
            _check(
                "required_targets_present",
                {
                    "smooth_3d_vs_limited",
                    "smooth_3d_vs_static_baseline",
                    "smooth_3d_sideways_choppy_improvement",
                    "smooth_3d_recovery_lag_watch",
                }.issubset(target_ids),
                ",".join(sorted(str(item) for item in target_ids)),
            ),
            _check("auto_apply_false", targets.get("auto_apply") is False, ""),
            _check(
                "broker_action_allowed_false",
                targets.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", targets.get("production_effect") == "none", ""),
            _check(
                "watch_only_target_present",
                any(row.get("status") == "WATCH_ONLY" for row in target_rows),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, targets, *target_rows), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_confirmation_validation",
        confirmation_id,
        checks,
    )


def run_smoothed_watch_pack(
    *,
    review_attribution_id: str,
    benefit_lag_id: str,
    regime_validation_id: str,
    confirmation_id: str,
    attribution_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    attribution = smoothed_review_attribution_report_payload(
        attribution_id=review_attribution_id,
        output_dir=attribution_dir,
    )
    benefit_lag = smoothing_benefit_lag_report_payload(
        drilldown_id=benefit_lag_id,
        output_dir=benefit_lag_dir,
    )
    regime = smoothed_regime_validation_report_payload(
        regime_validation_id=regime_validation_id,
        output_dir=regime_validation_dir,
    )
    confirmation = smoothed_confirmation_report_payload(
        confirmation_id=confirmation_id,
        output_dir=confirmation_dir,
    )
    summary = _smoothed_watch_summary(attribution, benefit_lag, regime, confirmation)
    watch_pack_id = _stable_id(
        "smoothed-watch-pack",
        review_attribution_id,
        benefit_lag_id,
        regime_validation_id,
        confirmation_id,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / watch_pack_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_watch_manifest",
        "watch_pack_id": root.name,
        "review_attribution_id": review_attribution_id,
        "benefit_lag_id": benefit_lag_id,
        "regime_validation_id": regime_validation_id,
        "confirmation_id": confirmation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_watch_manifest_path": str(root / "smoothed_watch_manifest.json"),
        "smoothed_watch_summary_path": str(root / "smoothed_watch_summary.json"),
        "owner_smoothed_watch_checklist_path": str(root / "owner_smoothed_watch_checklist.md"),
        "smoothed_watch_pack_report_path": str(root / "smoothed_watch_pack_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_watch_reader_brief(summary)
    _write_json(root / "smoothed_watch_manifest.json", manifest)
    _write_json(root / "smoothed_watch_summary.json", summary)
    _write_text(
        root / "owner_smoothed_watch_checklist.md",
        render_smoothed_watch_checklist(summary),
    )
    _write_text(
        root / "smoothed_watch_pack_report.md",
        render_smoothed_watch_pack_report(
            manifest,
            summary,
            attribution,
            benefit_lag,
            regime,
            confirmation,
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_watch_pack",
        root.name,
        root / "smoothed_watch_manifest.json",
    )
    return {
        "watch_pack_id": root.name,
        "watch_pack_dir": root,
        "manifest": manifest,
        "smoothed_watch_summary": summary,
        "reader_brief_section": reader,
    }


def smoothed_watch_pack_report_payload(
    *,
    watch_pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=watch_pack_id,
        latest_pointer="latest_smoothed_watch_pack",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_watch_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_watch_manifest.json"),
        "smoothed_watch_summary": _read_json(root / "smoothed_watch_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "watch_pack_dir": str(root),
    }


def validate_smoothed_watch_pack_artifact(
    *,
    watch_pack_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> dict[str, Any]:
    root = output_dir / watch_pack_id
    manifest = _read_optional_json(root / "smoothed_watch_manifest.json") or {}
    summary = _read_optional_json(root / "smoothed_watch_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_watch_manifest.json",
            "smoothed_watch_summary.json",
            "owner_smoothed_watch_checklist.md",
            "smoothed_watch_pack_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check("watch_pack_id_matches", manifest.get("watch_pack_id") == watch_pack_id, ""),
            _check(
                "candidate_method_smoothed_3d",
                summary.get("candidate_method") == "smooth_weights_3d_limited_adjustment",
                _text(summary.get("candidate_method")),
            ),
            _check(
                "forward_confirmation_in_progress",
                summary.get("forward_confirmation_status") == "IN_PROGRESS",
                _text(summary.get("forward_confirmation_status")),
            ),
            _check("research_target_only_true", summary.get("research_target_only") is True, ""),
            _check(
                "not_official_target_weights_true",
                summary.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                summary.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", summary.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_watch_pack_validation",
        watch_pack_id,
        checks,
    )


def run_smoothed_evidence_gap_diagnosis(
    *,
    benefit_lag_id: str,
    regime_validation_id: str,
    watch_pack_id: str,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    benefit_lag = smoothing_benefit_lag_report_payload(
        drilldown_id=benefit_lag_id,
        output_dir=benefit_lag_dir,
    )
    regime = smoothed_regime_validation_report_payload(
        regime_validation_id=regime_validation_id,
        output_dir=regime_validation_dir,
    )
    watch = smoothed_watch_pack_report_payload(
        watch_pack_id=watch_pack_id,
        output_dir=watch_pack_dir,
    )
    matrix = _smoothed_missing_evidence_matrix(benefit_lag, regime, watch)
    gap_id = _stable_id(
        "smoothed-evidence-gap",
        benefit_lag_id,
        regime_validation_id,
        watch_pack_id,
        matrix,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / gap_id)
    root.mkdir(parents=True, exist_ok=False)
    reason = _smoothed_evidence_gap_reason_summary(root.name, matrix, watch)
    plan = _smoothed_required_metric_backfill_plan(matrix)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_evidence_gap_manifest",
        "gap_id": root.name,
        "benefit_lag_id": benefit_lag_id,
        "regime_validation_id": regime_validation_id,
        "watch_pack_id": watch_pack_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": regime.get("market_regime", "ai_after_chatgpt"),
        "date_start": regime.get("date_start"),
        "date_end": regime.get("date_end"),
        "smoothed_evidence_gap_manifest_path": str(root / "smoothed_evidence_gap_manifest.json"),
        "missing_evidence_matrix_path": str(root / "missing_evidence_matrix.json"),
        "evidence_gap_reason_summary_path": str(root / "evidence_gap_reason_summary.json"),
        "required_metric_backfill_plan_path": str(root / "required_metric_backfill_plan.json"),
        "smoothed_evidence_gap_report_path": str(root / "smoothed_evidence_gap_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_evidence_gap_manifest.json", manifest)
    _write_json(root / "missing_evidence_matrix.json", matrix)
    _write_json(root / "evidence_gap_reason_summary.json", reason)
    _write_json(root / "required_metric_backfill_plan.json", plan)
    _write_text(
        root / "smoothed_evidence_gap_report.md",
        render_smoothed_evidence_gap_report(manifest, matrix, reason, plan),
    )
    _write_latest_pointer(
        "latest_smoothed_evidence_gap",
        root.name,
        root / "smoothed_evidence_gap_manifest.json",
    )
    return {
        "gap_id": root.name,
        "gap_dir": root,
        "manifest": manifest,
        "missing_evidence_matrix": matrix,
        "evidence_gap_reason_summary": reason,
        "required_metric_backfill_plan": plan,
    }


def smoothed_evidence_gap_report_payload(
    *,
    gap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gap_id,
        latest_pointer="latest_smoothed_evidence_gap",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_evidence_gap_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_evidence_gap_manifest.json"),
        "missing_evidence_matrix": _read_json(root / "missing_evidence_matrix.json"),
        "evidence_gap_reason_summary": _read_json(root / "evidence_gap_reason_summary.json"),
        "required_metric_backfill_plan": _read_json(root / "required_metric_backfill_plan.json"),
        "gap_dir": str(root),
    }


def validate_smoothed_evidence_gap_artifact(
    *,
    gap_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> dict[str, Any]:
    root = output_dir / gap_id
    manifest = _read_optional_json(root / "smoothed_evidence_gap_manifest.json") or {}
    matrix = _read_optional_json(root / "missing_evidence_matrix.json") or {}
    reason = _read_optional_json(root / "evidence_gap_reason_summary.json") or {}
    plan = _read_optional_json(root / "required_metric_backfill_plan.json") or {}
    evidence_types = {row.get("evidence_type") for row in _records(matrix.get("missing_evidence"))}
    checks = _required_file_checks(
        root,
        (
            "smoothed_evidence_gap_manifest.json",
            "missing_evidence_matrix.json",
            "evidence_gap_reason_summary.json",
            "required_metric_backfill_plan.json",
            "smoothed_evidence_gap_report.md",
        ),
    )
    checks.extend(
        [
            _check("gap_id_matches", manifest.get("gap_id") == gap_id, ""),
            _check(
                "candidate_method_smoothed_3d",
                matrix.get("candidate_method") == "smooth_weights_3d_limited_adjustment",
                _text(matrix.get("candidate_method")),
            ),
            _check(
                "required_evidence_types_present",
                {
                    "weight_jump_reduction",
                    "signal_churn_reduction",
                    "strong_recovery_lag_cost",
                }.issubset(evidence_types),
                ",".join(sorted(str(item) for item in evidence_types)),
            ),
            _check(
                "primary_gap_reasons_present",
                bool(_records(reason.get("primary_gap_reasons"))),
                "",
            ),
            _check(
                "backfill_plan_present",
                bool(_records(plan.get("required_backfills"))),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, matrix, reason, plan), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_evidence_gap_validation",
        gap_id,
        checks,
    )


def run_smoothed_churn_backfill(
    *,
    smoothed_backfill_id: str,
    baseline_backfill_id: str,
    risk_capped_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    smoothed = smoothed_backfill_report_payload(
        backfill_id=smoothed_backfill_id,
        output_dir=smoothed_backfill_dir,
    )
    baseline = paper_shadow_backfill_report_payload(
        backfill_id=baseline_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    risk_capped = risk_capped_backfill_report_payload(
        backfill_id=risk_capped_backfill_id,
        output_dir=risk_capped_backfill_dir,
    )
    method_states, method_ledgers, labels = _smoothed_churn_source_rows(
        smoothed,
        baseline,
        risk_capped,
    )
    metrics = _smoothed_churn_metrics_by_method(method_states, method_ledgers)
    weight_jump_events = _smoothed_weight_jump_events(method_ledgers, labels)
    direction_flip_events = _smoothed_direction_flip_events(method_ledgers, labels)
    summary = _smoothed_churn_reduction_summary(metrics)
    churn_id = _stable_id(
        "smoothed-churn-backfill",
        smoothed_backfill_id,
        baseline_backfill_id,
        risk_capped_backfill_id,
        metrics,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / churn_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_churn_manifest",
        "churn_id": root.name,
        "smoothed_backfill_id": smoothed_backfill_id,
        "baseline_backfill_id": baseline_backfill_id,
        "risk_capped_backfill_id": risk_capped_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": baseline.get("market_regime", "ai_after_chatgpt"),
        "date_start": baseline.get("date_start"),
        "date_end": baseline.get("date_end"),
        "data_quality_status": baseline.get("data_quality_status"),
        "jump_threshold": SMOOTHED_CHURN_WEIGHT_JUMP_EVENT_THRESHOLD,
        "smoothed_churn_manifest_path": str(root / "smoothed_churn_manifest.json"),
        "churn_metrics_by_method_path": str(root / "churn_metrics_by_method.jsonl"),
        "weight_jump_events_path": str(root / "weight_jump_events.jsonl"),
        "direction_flip_events_path": str(root / "direction_flip_events.jsonl"),
        "churn_reduction_summary_path": str(root / "churn_reduction_summary.json"),
        "smoothed_churn_backfill_report_path": str(root / "smoothed_churn_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_churn_manifest.json", manifest)
    _write_jsonl(root / "churn_metrics_by_method.jsonl", metrics)
    _write_jsonl(root / "weight_jump_events.jsonl", weight_jump_events)
    _write_jsonl(root / "direction_flip_events.jsonl", direction_flip_events)
    _write_json(root / "churn_reduction_summary.json", summary)
    _write_text(
        root / "smoothed_churn_backfill_report.md",
        render_smoothed_churn_backfill_report(
            manifest,
            metrics,
            weight_jump_events,
            direction_flip_events,
            summary,
        ),
    )
    _write_latest_pointer(
        "latest_smoothed_churn_backfill",
        root.name,
        root / "smoothed_churn_manifest.json",
    )
    return {
        "churn_id": root.name,
        "churn_dir": root,
        "manifest": manifest,
        "churn_metrics_by_method": metrics,
        "weight_jump_events": weight_jump_events,
        "direction_flip_events": direction_flip_events,
        "churn_reduction_summary": summary,
    }


def smoothed_churn_backfill_report_payload(
    *,
    churn_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=churn_id,
        latest_pointer="latest_smoothed_churn_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_churn_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_churn_manifest.json"),
        "churn_metrics_by_method": _read_jsonl(root / "churn_metrics_by_method.jsonl"),
        "weight_jump_events": _read_jsonl(root / "weight_jump_events.jsonl"),
        "direction_flip_events": _read_jsonl(root / "direction_flip_events.jsonl"),
        "churn_reduction_summary": _read_json(root / "churn_reduction_summary.json"),
        "churn_dir": str(root),
    }


def validate_smoothed_churn_backfill_artifact(
    *,
    churn_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / churn_id
    manifest = _read_optional_json(root / "smoothed_churn_manifest.json") or {}
    metrics = _read_jsonl(root / "churn_metrics_by_method.jsonl")
    jumps = _read_jsonl(root / "weight_jump_events.jsonl")
    flips = _read_jsonl(root / "direction_flip_events.jsonl")
    summary = _read_optional_json(root / "churn_reduction_summary.json") or {}
    methods = {row.get("method") for row in metrics}
    checks = _required_file_checks(
        root,
        (
            "smoothed_churn_manifest.json",
            "churn_metrics_by_method.jsonl",
            "weight_jump_events.jsonl",
            "direction_flip_events.jsonl",
            "churn_reduction_summary.json",
            "smoothed_churn_backfill_report.md",
        ),
    )
    checks.extend(
        [
            _check("churn_id_matches", manifest.get("churn_id") == churn_id, ""),
            _check(
                "required_methods_present",
                {
                    "smooth_weights_3d_limited_adjustment",
                    "smooth_weights_5d_limited_adjustment",
                    "limited_adjustment",
                    "risk_capped_limited_adjustment",
                    "static_baseline",
                }.issubset(methods),
                ",".join(sorted(str(item) for item in methods)),
            ),
            _check(
                "summary_methods_present",
                bool(_records(summary.get("methods"))),
                "",
            ),
            _check("jump_events_readable", isinstance(jumps, list), ""),
            _check("flip_events_readable", isinstance(flips, list), ""),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, summary, *metrics, *jumps, *flips),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_churn_backfill_validation",
        churn_id,
        checks,
    )


def run_sideways_mixed_attribution(
    *,
    regime_validation_id: str,
    churn_id: str,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    churn_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    regime = smoothed_regime_validation_report_payload(
        regime_validation_id=regime_validation_id,
        output_dir=regime_validation_dir,
    )
    churn = smoothed_churn_backfill_report_payload(churn_id=churn_id, output_dir=churn_dir)
    smoothed = smoothed_backfill_report_payload(
        backfill_id=_text(regime.get("smoothed_backfill_id")),
        output_dir=smoothed_backfill_dir,
    )
    baseline = paper_shadow_backfill_report_payload(
        backfill_id=_text(regime.get("baseline_backfill_id")),
        output_dir=baseline_backfill_dir,
    )
    outcomes = _smoothed_sideways_window_outcomes(smoothed, baseline, churn)
    reason = _sideways_mixed_reason_summary(regime, outcomes)
    breakdown = _sideways_3d_vs_5d_breakdown(outcomes)
    sideways_attribution_id = _stable_id(
        "sideways-mixed-attribution",
        regime_validation_id,
        churn_id,
        outcomes,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / sideways_attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sideways_mixed_manifest",
        "sideways_attribution_id": root.name,
        "regime_validation_id": regime_validation_id,
        "churn_id": churn_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": regime.get("market_regime", "ai_after_chatgpt"),
        "date_start": regime.get("date_start"),
        "date_end": regime.get("date_end"),
        "sideways_mixed_manifest_path": str(root / "sideways_mixed_manifest.json"),
        "sideways_window_outcomes_path": str(root / "sideways_window_outcomes.jsonl"),
        "sideways_mixed_reason_summary_path": str(root / "sideways_mixed_reason_summary.json"),
        "sideways_3d_vs_5d_breakdown_path": str(root / "sideways_3d_vs_5d_breakdown.json"),
        "sideways_mixed_attribution_report_path": str(
            root / "sideways_mixed_attribution_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "sideways_mixed_manifest.json", manifest)
    _write_jsonl(root / "sideways_window_outcomes.jsonl", outcomes)
    _write_json(root / "sideways_mixed_reason_summary.json", reason)
    _write_json(root / "sideways_3d_vs_5d_breakdown.json", breakdown)
    _write_text(
        root / "sideways_mixed_attribution_report.md",
        render_sideways_mixed_attribution_report(manifest, outcomes, reason, breakdown),
    )
    _write_latest_pointer(
        "latest_sideways_mixed_attribution",
        root.name,
        root / "sideways_mixed_manifest.json",
    )
    return {
        "sideways_attribution_id": root.name,
        "sideways_attribution_dir": root,
        "manifest": manifest,
        "sideways_window_outcomes": outcomes,
        "sideways_mixed_reason_summary": reason,
        "sideways_3d_vs_5d_breakdown": breakdown,
    }


def sideways_mixed_attribution_report_payload(
    *,
    sideways_attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=sideways_attribution_id,
        latest_pointer="latest_sideways_mixed_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="sideways_mixed_manifest.json",
    )
    return {
        **_read_json(root / "sideways_mixed_manifest.json"),
        "sideways_window_outcomes": _read_jsonl(root / "sideways_window_outcomes.jsonl"),
        "sideways_mixed_reason_summary": _read_json(root / "sideways_mixed_reason_summary.json"),
        "sideways_3d_vs_5d_breakdown": _read_json(root / "sideways_3d_vs_5d_breakdown.json"),
        "sideways_attribution_dir": str(root),
    }


def validate_sideways_mixed_attribution_artifact(
    *,
    sideways_attribution_id: str,
    output_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / sideways_attribution_id
    manifest = _read_optional_json(root / "sideways_mixed_manifest.json") or {}
    outcomes = _read_jsonl(root / "sideways_window_outcomes.jsonl")
    reason = _read_optional_json(root / "sideways_mixed_reason_summary.json") or {}
    breakdown = _read_optional_json(root / "sideways_3d_vs_5d_breakdown.json") or {}
    checks = _required_file_checks(
        root,
        (
            "sideways_mixed_manifest.json",
            "sideways_window_outcomes.jsonl",
            "sideways_mixed_reason_summary.json",
            "sideways_3d_vs_5d_breakdown.json",
            "sideways_mixed_attribution_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "sideways_attribution_id_matches",
                manifest.get("sideways_attribution_id") == sideways_attribution_id,
                "",
            ),
            _check("window_outcomes_present", bool(outcomes), ""),
            _check(
                "sideways_validation_mixed_visible",
                reason.get("sideways_validation")
                in {"MIXED", "IMPROVED", "WORSE", "INSUFFICIENT_DATA"},
                _text(reason.get("sideways_validation")),
            ),
            _check(
                "breakdown_methods_present",
                {
                    "smooth_weights_3d_limited_adjustment",
                    "smooth_weights_5d_limited_adjustment",
                }.issubset({row.get("method") for row in _records(breakdown.get("methods"))}),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, reason, breakdown, *outcomes), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_sideways_mixed_attribution_validation",
        sideways_attribution_id,
        checks,
    )


def run_smoothed_readiness_scorecard(
    *,
    attribution_id: str,
    benefit_lag_id: str,
    churn_id: str,
    sideways_attribution_id: str,
    confirmation_id: str,
    attribution_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    churn_dir: Path = DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
    sideways_attribution_dir: Path = DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    attribution = smoothed_review_attribution_report_payload(
        attribution_id=attribution_id,
        output_dir=attribution_dir,
    )
    benefit_lag = smoothing_benefit_lag_report_payload(
        drilldown_id=benefit_lag_id,
        output_dir=benefit_lag_dir,
    )
    churn = smoothed_churn_backfill_report_payload(churn_id=churn_id, output_dir=churn_dir)
    sideways = sideways_mixed_attribution_report_payload(
        sideways_attribution_id=sideways_attribution_id,
        output_dir=sideways_attribution_dir,
    )
    confirmation = smoothed_confirmation_report_payload(
        confirmation_id=confirmation_id,
        output_dir=confirmation_dir,
    )
    scorecard = _smoothed_method_scorecard(attribution, benefit_lag, churn, sideways, confirmation)
    decision = _smoothed_readiness_decision(scorecard, confirmation)
    scorecard_id = _stable_id(
        "smoothed-readiness-scorecard",
        attribution_id,
        benefit_lag_id,
        churn_id,
        sideways_attribution_id,
        confirmation_id,
        scorecard,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / scorecard_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_readiness_manifest",
        "scorecard_id": root.name,
        "attribution_id": attribution_id,
        "benefit_lag_id": benefit_lag_id,
        "churn_id": churn_id,
        "sideways_attribution_id": sideways_attribution_id,
        "confirmation_id": confirmation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "score_weights": SMOOTHED_READINESS_SCORE_WEIGHTS,
        "smoothed_readiness_manifest_path": str(root / "smoothed_readiness_manifest.json"),
        "smoothed_method_scorecard_path": str(root / "smoothed_method_scorecard.json"),
        "promotion_readiness_decision_path": str(root / "promotion_readiness_decision.json"),
        "smoothed_readiness_scorecard_report_path": str(
            root / "smoothed_readiness_scorecard_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_readiness_manifest.json", manifest)
    _write_json(root / "smoothed_method_scorecard.json", scorecard)
    _write_json(root / "promotion_readiness_decision.json", decision)
    _write_text(
        root / "smoothed_readiness_scorecard_report.md",
        render_smoothed_readiness_scorecard_report(manifest, scorecard, decision),
    )
    _write_latest_pointer(
        "latest_smoothed_readiness_scorecard",
        root.name,
        root / "smoothed_readiness_manifest.json",
    )
    return {
        "scorecard_id": root.name,
        "scorecard_dir": root,
        "manifest": manifest,
        "smoothed_method_scorecard": scorecard,
        "promotion_readiness_decision": decision,
    }


def smoothed_readiness_scorecard_report_payload(
    *,
    scorecard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=scorecard_id,
        latest_pointer="latest_smoothed_readiness_scorecard",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_readiness_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_readiness_manifest.json"),
        "smoothed_method_scorecard": _read_json(root / "smoothed_method_scorecard.json"),
        "promotion_readiness_decision": _read_json(root / "promotion_readiness_decision.json"),
        "scorecard_dir": str(root),
    }


def validate_smoothed_readiness_scorecard_artifact(
    *,
    scorecard_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> dict[str, Any]:
    root = output_dir / scorecard_id
    manifest = _read_optional_json(root / "smoothed_readiness_manifest.json") or {}
    scorecard = _read_optional_json(root / "smoothed_method_scorecard.json") or {}
    decision = _read_optional_json(root / "promotion_readiness_decision.json") or {}
    methods = {row.get("method") for row in _records(scorecard.get("methods"))}
    checks = _required_file_checks(
        root,
        (
            "smoothed_readiness_manifest.json",
            "smoothed_method_scorecard.json",
            "promotion_readiness_decision.json",
            "smoothed_readiness_scorecard_report.md",
        ),
    )
    checks.extend(
        [
            _check("scorecard_id_matches", manifest.get("scorecard_id") == scorecard_id, ""),
            _check(
                "smoothed_methods_present",
                set(SMOOTHED_METHOD_TO_VARIANT).issubset(methods),
                ",".join(sorted(str(item) for item in methods)),
            ),
            _check(
                "decision_valid",
                decision.get("decision")
                in {"PROMOTE_FOR_REVIEW", "CONTINUE_OBSERVATION", "REVIEW_REQUIRED", "REJECT"},
                _text(decision.get("decision")),
            ),
            _check("auto_apply_false", decision.get("auto_apply") is False, ""),
            _check(
                "broker_action_allowed_false",
                decision.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", decision.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, scorecard, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_readiness_scorecard_validation",
        scorecard_id,
        checks,
    )


def run_smoothed_owner_review_update(
    *,
    scorecard_id: str,
    watch_pack_id: str,
    scorecard_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = smoothed_readiness_scorecard_report_payload(
        scorecard_id=scorecard_id,
        output_dir=scorecard_dir,
    )
    watch = smoothed_watch_pack_report_payload(
        watch_pack_id=watch_pack_id,
        output_dir=watch_pack_dir,
    )
    decision_options = _smoothed_owner_decision_options(scorecard, watch)
    owner_update_id = _stable_id(
        "smoothed-owner-review-update",
        scorecard_id,
        watch_pack_id,
        decision_options,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / owner_update_id)
    root.mkdir(parents=True, exist_ok=False)
    decision_options["owner_update_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_owner_update_manifest",
        "owner_update_id": root.name,
        "scorecard_id": scorecard_id,
        "watch_pack_id": watch_pack_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_owner_update_manifest_path": str(root / "smoothed_owner_update_manifest.json"),
        "smoothed_owner_decision_options_path": str(root / "smoothed_owner_decision_options.json"),
        "smoothed_owner_review_checklist_path": str(root / "smoothed_owner_review_checklist.md"),
        "smoothed_owner_review_update_report_path": str(
            root / "smoothed_owner_review_update_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = render_smoothed_owner_review_checklist(decision_options)
    reader = render_smoothed_owner_review_reader_brief(decision_options)
    _write_json(root / "smoothed_owner_update_manifest.json", manifest)
    _write_json(root / "smoothed_owner_decision_options.json", decision_options)
    _write_text(root / "smoothed_owner_review_checklist.md", checklist)
    _write_text(
        root / "smoothed_owner_review_update_report.md",
        render_smoothed_owner_review_update_report(manifest, decision_options, scorecard, watch),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_owner_review_update",
        root.name,
        root / "smoothed_owner_update_manifest.json",
    )
    return {
        "owner_update_id": root.name,
        "owner_update_dir": root,
        "manifest": manifest,
        "smoothed_owner_decision_options": decision_options,
        "smoothed_owner_review_checklist": checklist,
        "reader_brief_section": reader,
    }


def smoothed_owner_review_update_report_payload(
    *,
    owner_update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_update_id,
        latest_pointer="latest_smoothed_owner_review_update",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_owner_update_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_owner_update_manifest.json"),
        "smoothed_owner_decision_options": _read_json(
            root / "smoothed_owner_decision_options.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "owner_update_dir": str(root),
    }


def validate_smoothed_owner_review_update_artifact(
    *,
    owner_update_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_update_id
    manifest = _read_optional_json(root / "smoothed_owner_update_manifest.json") or {}
    options = _read_optional_json(root / "smoothed_owner_decision_options.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_owner_update_manifest.json",
            "smoothed_owner_decision_options.json",
            "smoothed_owner_review_checklist.md",
            "smoothed_owner_review_update_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "owner_update_id_matches",
                manifest.get("owner_update_id") == owner_update_id
                and options.get("owner_update_id") == owner_update_id,
                "",
            ),
            _check(
                "decision_options_present",
                bool(_records(options.get("owner_decision_options"))),
                "",
            ),
            _check("auto_apply_false", options.get("auto_apply") is False, ""),
            _check(
                "not_official_target_weights_true",
                options.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                options.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", options.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, options), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_review_update_validation",
        owner_update_id,
        checks,
    )


def build_smoothed_promotion_review_pack(
    *,
    readiness_scorecard_id: str,
    owner_update_id: str,
    watch_pack_id: str,
    scorecard_dir: Path = DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
    owner_update_dir: Path = DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
    watch_pack_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = smoothed_readiness_scorecard_report_payload(
        scorecard_id=readiness_scorecard_id,
        output_dir=scorecard_dir,
    )
    owner_update = smoothed_owner_review_update_report_payload(
        owner_update_id=owner_update_id,
        output_dir=owner_update_dir,
    )
    watch = smoothed_watch_pack_report_payload(
        watch_pack_id=watch_pack_id,
        output_dir=watch_pack_dir,
    )
    evidence = _smoothed_promotion_evidence_summary(scorecard, owner_update, watch)
    blocking = _smoothed_promotion_blocking_issues(scorecard, owner_update, watch)
    promotion_review_id = _stable_id(
        "smoothed-promotion-review",
        readiness_scorecard_id,
        owner_update_id,
        watch_pack_id,
        evidence,
        blocking,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / promotion_review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_promotion_review_manifest",
        "promotion_review_id": root.name,
        "readiness_scorecard_id": readiness_scorecard_id,
        "owner_update_id": owner_update_id,
        "watch_pack_id": watch_pack_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_promotion_review_manifest_path": str(
            root / "smoothed_promotion_review_manifest.json"
        ),
        "promotion_evidence_summary_path": str(root / "promotion_evidence_summary.json"),
        "promotion_blocking_issues_path": str(root / "promotion_blocking_issues.json"),
        "smoothed_promotion_review_report_path": str(root / "smoothed_promotion_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_promotion_review_reader_brief(evidence, blocking)
    _write_json(root / "smoothed_promotion_review_manifest.json", manifest)
    _write_json(root / "promotion_evidence_summary.json", evidence)
    _write_json(root / "promotion_blocking_issues.json", blocking)
    _write_text(
        root / "smoothed_promotion_review_report.md",
        render_smoothed_promotion_review_report(manifest, evidence, blocking),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_promotion_review",
        root.name,
        root / "smoothed_promotion_review_manifest.json",
    )
    return {
        "promotion_review_id": root.name,
        "promotion_review_dir": root,
        "manifest": manifest,
        "promotion_evidence_summary": evidence,
        "promotion_blocking_issues": blocking,
        "reader_brief_section": reader,
    }


def smoothed_promotion_review_report_payload(
    *,
    promotion_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_review_id,
        latest_pointer="latest_smoothed_promotion_review",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_promotion_review_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_promotion_review_manifest.json"),
        "promotion_evidence_summary": _read_json(root / "promotion_evidence_summary.json"),
        "promotion_blocking_issues": _read_json(root / "promotion_blocking_issues.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "promotion_review_dir": str(root),
    }


def validate_smoothed_promotion_review_artifact(
    *,
    promotion_review_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_review_id
    manifest = _read_optional_json(root / "smoothed_promotion_review_manifest.json") or {}
    evidence = _read_optional_json(root / "promotion_evidence_summary.json") or {}
    blocking = _read_optional_json(root / "promotion_blocking_issues.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_promotion_review_manifest.json",
            "promotion_evidence_summary.json",
            "promotion_blocking_issues.json",
            "smoothed_promotion_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "promotion_review_id_matches",
                manifest.get("promotion_review_id") == promotion_review_id,
                "",
            ),
            _check(
                "candidate_method_is_smooth_3d",
                evidence.get("candidate_method") == "smooth_weights_3d_limited_adjustment",
                _text(evidence.get("candidate_method")),
            ),
            _check(
                "readiness_decision_promote_for_review_visible",
                evidence.get("readiness_decision") == "PROMOTE_FOR_REVIEW",
                _text(evidence.get("readiness_decision")),
            ),
            _check(
                "supporting_evidence_present",
                len(_records(evidence.get("supporting_evidence"))) >= 3,
                "",
            ),
            _check(
                "can_enter_owner_review_true",
                blocking.get("can_enter_owner_review") is True,
                "",
            ),
            _check(
                "official_target_weights_forbidden",
                blocking.get("can_write_official_target_weights") is False,
                "",
            ),
            _check(
                "production_forbidden",
                blocking.get("can_trigger_production") is False,
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, evidence, blocking), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_promotion_review_validation",
        promotion_review_id,
        checks,
    )


def run_primary_research_candidate_gate(
    *,
    promotion_review_id: str,
    promotion_review_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    promotion = smoothed_promotion_review_report_payload(
        promotion_review_id=promotion_review_id,
        output_dir=promotion_review_dir,
    )
    decision = _primary_research_candidate_gate_decision(promotion)
    criteria = _primary_research_candidate_gate_criteria(promotion, decision)
    gate_id = _stable_id(
        "primary-research-candidate-gate",
        promotion_review_id,
        decision,
        criteria,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / gate_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["gate_id"] = root.name
    criteria["gate_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_primary_research_candidate_gate_manifest",
        "gate_id": root.name,
        "promotion_review_id": promotion_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "primary_research_candidate_gate_manifest_path": str(
            root / "primary_research_candidate_gate_manifest.json"
        ),
        "gate_decision_path": str(root / "gate_decision.json"),
        "gate_criteria_results_path": str(root / "gate_criteria_results.json"),
        "primary_research_candidate_gate_report_path": str(
            root / "primary_research_candidate_gate_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "primary_research_candidate_gate_manifest.json", manifest)
    _write_json(root / "gate_decision.json", decision)
    _write_json(root / "gate_criteria_results.json", criteria)
    _write_text(
        root / "primary_research_candidate_gate_report.md",
        render_primary_research_candidate_gate_report(manifest, decision, criteria),
    )
    _write_latest_pointer(
        "latest_primary_research_candidate_gate",
        root.name,
        root / "primary_research_candidate_gate_manifest.json",
    )
    return {
        "gate_id": root.name,
        "gate_dir": root,
        "manifest": manifest,
        "gate_decision": decision,
        "gate_criteria_results": criteria,
    }


def primary_research_candidate_gate_report_payload(
    *,
    gate_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gate_id,
        latest_pointer="latest_primary_research_candidate_gate",
        latest=latest,
        output_dir=output_dir,
        required_name="primary_research_candidate_gate_manifest.json",
    )
    return {
        **_read_json(root / "primary_research_candidate_gate_manifest.json"),
        "gate_decision": _read_json(root / "gate_decision.json"),
        "gate_criteria_results": _read_json(root / "gate_criteria_results.json"),
        "gate_dir": str(root),
    }


def validate_primary_research_candidate_gate_artifact(
    *,
    gate_id: str,
    output_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> dict[str, Any]:
    root = output_dir / gate_id
    manifest = _read_optional_json(root / "primary_research_candidate_gate_manifest.json") or {}
    decision = _read_optional_json(root / "gate_decision.json") or {}
    criteria = _read_optional_json(root / "gate_criteria_results.json") or {}
    checks = _required_file_checks(
        root,
        (
            "primary_research_candidate_gate_manifest.json",
            "gate_decision.json",
            "gate_criteria_results.json",
            "primary_research_candidate_gate_report.md",
        ),
    )
    statuses = {row.get("status") for row in _records(criteria.get("criteria"))}
    checks.extend(
        [
            _check(
                "gate_id_matches",
                manifest.get("gate_id") == gate_id and decision.get("gate_id") == gate_id,
                "",
            ),
            _check(
                "gate_scope_research_only",
                decision.get("gate_scope") == "paper_shadow_research_only",
                _text(decision.get("gate_scope")),
            ),
            _check(
                "gate_decision_valid",
                decision.get("gate_decision")
                in {
                    "ELIGIBLE_FOR_OWNER_APPROVAL",
                    "CONTINUE_OBSERVATION",
                    "REVIEW_REQUIRED",
                    "REJECT",
                },
                _text(decision.get("gate_decision")),
            ),
            _check(
                "owner_approval_required_true",
                decision.get("owner_approval_required") is True,
                "",
            ),
            _check("auto_apply_false", decision.get("auto_apply") is False, ""),
            _check(
                "paper_shadow_update_owner_required",
                decision.get("can_update_paper_shadow_primary_candidate")
                == "OWNER_DECISION_REQUIRED",
                _text(decision.get("can_update_paper_shadow_primary_candidate")),
            ),
            _check("criteria_present", len(_records(criteria.get("criteria"))) >= 5, ""),
            _check("criteria_no_fail", "FAIL" not in statuses, ",".join(_texts(statuses))),
            _check("broker_forbidden", _payload_safe(manifest, decision, criteria), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_primary_research_candidate_gate_validation",
        gate_id,
        checks,
    )


def run_smoothed_forward_binding(
    *,
    confirmation_id: str,
    gate_id: str,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    confirmation = smoothed_confirmation_report_payload(
        confirmation_id=confirmation_id,
        output_dir=confirmation_dir,
    )
    gate = primary_research_candidate_gate_report_payload(
        gate_id=gate_id,
        output_dir=gate_dir,
    )
    binding = _smoothed_forward_bound_confirmation_targets(confirmation_id, confirmation, gate)
    requirements = _smoothed_forward_progress_requirements()
    binding_id = _stable_id(
        "smoothed-forward-binding",
        confirmation_id,
        gate_id,
        binding,
        requirements,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / binding_id)
    root.mkdir(parents=True, exist_ok=False)
    binding["binding_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_forward_binding_manifest",
        "binding_id": root.name,
        "confirmation_id": confirmation_id,
        "gate_id": gate_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_forward_binding_manifest_path": str(
            root / "smoothed_forward_binding_manifest.json"
        ),
        "bound_confirmation_targets_path": str(root / "bound_confirmation_targets.json"),
        "forward_progress_requirements_path": str(root / "forward_progress_requirements.json"),
        "smoothed_forward_binding_report_path": str(root / "smoothed_forward_binding_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_forward_binding_reader_brief(binding, requirements)
    _write_json(root / "smoothed_forward_binding_manifest.json", manifest)
    _write_json(root / "bound_confirmation_targets.json", binding)
    _write_json(root / "forward_progress_requirements.json", requirements)
    _write_text(
        root / "smoothed_forward_binding_report.md",
        render_smoothed_forward_binding_report(manifest, binding, requirements),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_forward_binding",
        root.name,
        root / "smoothed_forward_binding_manifest.json",
    )
    return {
        "binding_id": root.name,
        "binding_dir": root,
        "manifest": manifest,
        "bound_confirmation_targets": binding,
        "forward_progress_requirements": requirements,
        "reader_brief_section": reader,
    }


def smoothed_forward_binding_report_payload(
    *,
    binding_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=binding_id,
        latest_pointer="latest_smoothed_forward_binding",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_forward_binding_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_forward_binding_manifest.json"),
        "bound_confirmation_targets": _read_json(root / "bound_confirmation_targets.json"),
        "forward_progress_requirements": _read_json(root / "forward_progress_requirements.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "binding_dir": str(root),
    }


def validate_smoothed_forward_binding_artifact(
    *,
    binding_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> dict[str, Any]:
    root = output_dir / binding_id
    manifest = _read_optional_json(root / "smoothed_forward_binding_manifest.json") or {}
    binding = _read_optional_json(root / "bound_confirmation_targets.json") or {}
    requirements = _read_optional_json(root / "forward_progress_requirements.json") or {}
    targets = _records(binding.get("targets"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_forward_binding_manifest.json",
            "bound_confirmation_targets.json",
            "forward_progress_requirements.json",
            "smoothed_forward_binding_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "binding_id_matches",
                manifest.get("binding_id") == binding_id
                and binding.get("binding_id") == binding_id,
                "",
            ),
            _check("targets_present", len(targets) >= 3, ""),
            _check(
                "targets_bound_to_weekly_progress",
                all(row.get("bound_to_weekly_progress") is True for row in targets),
                "",
            ),
            _check(
                "watch_only_target_present",
                any(row.get("status") == "WATCH_ONLY" for row in targets),
                "",
            ),
            _check(
                "rule_review_conditions_present",
                bool(_texts(requirements.get("rule_review_ready_when"))),
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, binding, requirements, *targets),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_binding_validation",
        binding_id,
        checks,
    )


def build_paper_shadow_primary_switch_plan(
    *,
    gate_id: str,
    binding_id: str,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    gate = primary_research_candidate_gate_report_payload(gate_id=gate_id, output_dir=gate_dir)
    binding = smoothed_forward_binding_report_payload(binding_id=binding_id, output_dir=binding_dir)
    plan = _paper_shadow_primary_switch_plan(gate, binding)
    safety_checks = _paper_shadow_primary_switch_safety_checks(plan)
    switch_plan_id = _stable_id(
        "paper-shadow-primary-switch",
        gate_id,
        binding_id,
        plan,
        safety_checks,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / switch_plan_id)
    root.mkdir(parents=True, exist_ok=False)
    plan["switch_plan_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_primary_switch_manifest",
        "switch_plan_id": root.name,
        "gate_id": gate_id,
        "binding_id": binding_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "paper_shadow_primary_switch_manifest_path": str(
            root / "paper_shadow_primary_switch_manifest.json"
        ),
        "primary_switch_plan_path": str(root / "primary_switch_plan.json"),
        "primary_switch_safety_checks_path": str(root / "primary_switch_safety_checks.json"),
        "paper_shadow_primary_switch_report_path": str(
            root / "paper_shadow_primary_switch_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_paper_shadow_primary_switch_reader_brief(plan, safety_checks)
    _write_json(root / "paper_shadow_primary_switch_manifest.json", manifest)
    _write_json(root / "primary_switch_plan.json", plan)
    _write_json(root / "primary_switch_safety_checks.json", safety_checks)
    _write_text(
        root / "paper_shadow_primary_switch_report.md",
        render_paper_shadow_primary_switch_report(manifest, plan, safety_checks),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_paper_shadow_primary_switch",
        root.name,
        root / "paper_shadow_primary_switch_manifest.json",
    )
    return {
        "switch_plan_id": root.name,
        "switch_plan_dir": root,
        "manifest": manifest,
        "primary_switch_plan": plan,
        "primary_switch_safety_checks": safety_checks,
        "reader_brief_section": reader,
    }


def paper_shadow_primary_switch_report_payload(
    *,
    switch_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=switch_plan_id,
        latest_pointer="latest_paper_shadow_primary_switch",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_primary_switch_manifest.json",
    )
    return {
        **_read_json(root / "paper_shadow_primary_switch_manifest.json"),
        "primary_switch_plan": _read_json(root / "primary_switch_plan.json"),
        "primary_switch_safety_checks": _read_json(root / "primary_switch_safety_checks.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "switch_plan_dir": str(root),
    }


def validate_paper_shadow_primary_switch_artifact(
    *,
    switch_plan_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> dict[str, Any]:
    root = output_dir / switch_plan_id
    manifest = _read_optional_json(root / "paper_shadow_primary_switch_manifest.json") or {}
    plan = _read_optional_json(root / "primary_switch_plan.json") or {}
    safety = _read_optional_json(root / "primary_switch_safety_checks.json") or {}
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_primary_switch_manifest.json",
            "primary_switch_plan.json",
            "primary_switch_safety_checks.json",
            "paper_shadow_primary_switch_report.md",
            "reader_brief_section.md",
        ),
    )
    safety_checks = _mapping(safety.get("safety_checks"))
    checks.extend(
        [
            _check(
                "switch_plan_id_matches",
                manifest.get("switch_plan_id") == switch_plan_id
                and plan.get("switch_plan_id") == switch_plan_id,
                "",
            ),
            _check(
                "switch_scope_research_only",
                plan.get("switch_scope") == "paper_shadow_research_only",
                _text(plan.get("switch_scope")),
            ),
            _check("auto_switch_false", plan.get("auto_switch") is False, ""),
            _check(
                "requires_owner_decision_true",
                plan.get("requires_owner_decision") is True,
                "",
            ),
            _check(
                "safety_checks_pass",
                safety.get("status") == "PASS",
                _text(safety.get("status")),
            ),
            _check(
                "official_target_weights_forbidden",
                safety_checks.get("not_official_target_weights") is True,
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, plan, safety, safety_checks), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_primary_switch_validation",
        switch_plan_id,
        checks,
    )


def create_smoothed_owner_promotion_decision(
    *,
    promotion_review_id: str,
    gate_id: str,
    switch_plan_id: str,
    promotion_review_dir: Path = DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
    gate_dir: Path = DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    promotion = smoothed_promotion_review_report_payload(
        promotion_review_id=promotion_review_id,
        output_dir=promotion_review_dir,
    )
    gate = primary_research_candidate_gate_report_payload(gate_id=gate_id, output_dir=gate_dir)
    switch_plan = paper_shadow_primary_switch_report_payload(
        switch_plan_id=switch_plan_id,
        output_dir=switch_plan_dir,
    )
    decision = _smoothed_owner_promotion_decision(
        promotion,
        gate,
        switch_plan,
        owner_decision="pending",
        generated_at=generated,
    )
    decision_id = _stable_id(
        "smoothed-owner-promotion",
        promotion_review_id,
        gate_id,
        switch_plan_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / decision_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["decision_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_owner_promotion_manifest",
        "decision_id": root.name,
        "promotion_review_id": promotion_review_id,
        "gate_id": gate_id,
        "switch_plan_id": switch_plan_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_owner_promotion_manifest_path": str(
            root / "smoothed_owner_promotion_manifest.json"
        ),
        "owner_promotion_decision_path": str(root / "owner_promotion_decision.json"),
        "owner_promotion_checklist_path": str(root / "owner_promotion_checklist.md"),
        "smoothed_owner_promotion_report_path": str(root / "smoothed_owner_promotion_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_smoothed_owner_promotion_files(root, manifest, decision)
    _write_latest_pointer(
        "latest_smoothed_owner_promotion",
        root.name,
        root / "smoothed_owner_promotion_manifest.json",
    )
    return {
        "decision_id": root.name,
        "decision_dir": root,
        "manifest": manifest,
        "owner_promotion_decision": decision,
        "owner_promotion_checklist": (root / "owner_promotion_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
    }


def record_smoothed_owner_promotion_decision(
    *,
    decision_id: str,
    decision: str,
    decision_reason: str = "",
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    recorded_at: datetime | None = None,
) -> dict[str, Any]:
    recorded = recorded_at or datetime.now(UTC)
    root = output_dir / decision_id
    manifest = _read_json(root / "smoothed_owner_promotion_manifest.json")
    owner_decision = _read_json(root / "owner_promotion_decision.json")
    owner_decision.update(
        _smoothed_owner_promotion_decision_update(
            owner_decision,
            owner_decision=decision,
            decision_reason=decision_reason,
            recorded_at=recorded,
        )
    )
    _write_smoothed_owner_promotion_files(root, manifest, owner_decision)
    return {
        "decision_id": decision_id,
        "decision_dir": root,
        "manifest": manifest,
        "owner_promotion_decision": owner_decision,
        "owner_promotion_checklist": (root / "owner_promotion_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
    }


def smoothed_owner_promotion_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=decision_id,
        latest_pointer="latest_smoothed_owner_promotion",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_owner_promotion_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_owner_promotion_manifest.json"),
        "owner_promotion_decision": _read_json(root / "owner_promotion_decision.json"),
        "owner_promotion_checklist": (root / "owner_promotion_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "decision_dir": str(root),
    }


def validate_smoothed_owner_promotion_artifact(
    *,
    decision_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> dict[str, Any]:
    root = output_dir / decision_id
    manifest = _read_optional_json(root / "smoothed_owner_promotion_manifest.json") or {}
    decision = _read_optional_json(root / "owner_promotion_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_owner_promotion_manifest.json",
            "owner_promotion_decision.json",
            "owner_promotion_checklist.md",
            "smoothed_owner_promotion_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "decision_id_matches",
                manifest.get("decision_id") == decision_id
                and decision.get("decision_id") == decision_id,
                "",
            ),
            _check(
                "owner_decision_valid",
                decision.get("owner_decision")
                in {
                    "pending",
                    "continue_observation",
                    "promote_to_primary_research_candidate",
                    "defer",
                    "reject",
                    "request_more_forward_data",
                },
                _text(decision.get("owner_decision")),
            ),
            _check(
                "does_not_auto_switch",
                decision.get("paper_shadow_primary_candidate_change_requested") is not True
                or decision.get("owner_decision") == "promote_to_primary_research_candidate",
                "",
            ),
            _check(
                "actual_switch_executed_false",
                decision.get("actual_switch_executed") is False,
                "",
            ),
            _check(
                "not_official_target_weights_true",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                decision.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", decision.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_promotion_validation",
        decision_id,
        checks,
    )


def update_smoothed_forward_progress(
    *,
    binding_id: str,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    outcome_update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    binding = smoothed_forward_binding_report_payload(binding_id=binding_id, output_dir=binding_dir)
    effective_update_dir = (
        output_dir.parent / "smoothed_outcome_update"
        if outcome_update_dir == DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
        and output_dir != DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
        else outcome_update_dir
    )
    effective_classification_dir = (
        output_dir.parent / "smoothed_forward_classification"
        if classification_dir == DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
        and output_dir != DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
        else classification_dir
    )
    updated_outcomes = _collect_updated_smoothed_outcomes(effective_update_dir)
    classifications = _collect_classified_smoothed_events(effective_classification_dir)
    targets = _smoothed_forward_progress_targets(
        binding,
        generated,
        updated_outcomes=updated_outcomes,
        classifications=classifications,
    )
    summary = _smoothed_forward_progress_summary(binding_id, targets)
    progress_id = _stable_id(
        "smoothed-forward-progress",
        binding_id,
        targets,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / progress_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["progress_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_forward_progress_manifest",
        "progress_id": root.name,
        "binding_id": binding_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_forward_progress_manifest_path": str(
            root / "smoothed_forward_progress_manifest.json"
        ),
        "smoothed_target_progress_path": str(root / "smoothed_target_progress.jsonl"),
        "smoothed_forward_progress_summary_path": str(
            root / "smoothed_forward_progress_summary.json"
        ),
        "smoothed_forward_progress_report_path": str(root / "smoothed_forward_progress_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_forward_progress_reader_brief(summary, targets)
    _write_json(root / "smoothed_forward_progress_manifest.json", manifest)
    _write_jsonl(root / "smoothed_target_progress.jsonl", targets)
    _write_json(root / "smoothed_forward_progress_summary.json", summary)
    _write_text(
        root / "smoothed_forward_progress_report.md",
        render_smoothed_forward_progress_report(manifest, summary, targets),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_forward_progress",
        root.name,
        root / "smoothed_forward_progress_manifest.json",
    )
    return {
        "progress_id": root.name,
        "progress_dir": root,
        "manifest": manifest,
        "smoothed_target_progress": targets,
        "smoothed_forward_progress_summary": summary,
        "reader_brief_section": reader,
    }


def smoothed_forward_progress_report_payload(
    *,
    progress_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=progress_id,
        latest_pointer="latest_smoothed_forward_progress",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_forward_progress_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_forward_progress_manifest.json"),
        "smoothed_target_progress": _read_jsonl(root / "smoothed_target_progress.jsonl"),
        "smoothed_forward_progress_summary": _read_json(
            root / "smoothed_forward_progress_summary.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "progress_dir": str(root),
    }


def validate_smoothed_forward_progress_artifact(
    *,
    progress_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> dict[str, Any]:
    root = output_dir / progress_id
    manifest = _read_optional_json(root / "smoothed_forward_progress_manifest.json") or {}
    targets = _read_jsonl(root / "smoothed_target_progress.jsonl")
    summary = _read_optional_json(root / "smoothed_forward_progress_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_forward_progress_manifest.json",
            "smoothed_target_progress.jsonl",
            "smoothed_forward_progress_summary.json",
            "smoothed_forward_progress_report.md",
            "reader_brief_section.md",
        ),
    )
    not_ready_when_insufficient = all(
        row.get("progress_status") != "READY_FOR_REVIEW"
        for row in targets
        if _smoothed_target_available_events(row) < _smoothed_target_required_events(row)
    )
    checks.extend(
        [
            _check(
                "progress_id_matches",
                manifest.get("progress_id") == progress_id
                and summary.get("progress_id") == progress_id,
                "",
            ),
            _check("targets_total_three", len(targets) == 3, str(len(targets))),
            _check(
                "each_target_has_progress_status",
                all(_text(row.get("progress_status")) for row in targets),
                "",
            ),
            _check("insufficient_events_not_ready", not_ready_when_insufficient, ""),
            _check(
                "summary_recommends_continue_observation",
                summary.get("summary_recommendation") == "continue_observation",
                _text(summary.get("summary_recommendation")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *targets), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_progress_validation",
        progress_id,
        checks,
    )


def build_smoothed_weekly_dashboard(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    progress = smoothed_forward_progress_report_payload(
        progress_id=progress_id,
        output_dir=progress_dir,
    )
    progress_summary = _mapping(progress.get("smoothed_forward_progress_summary"))
    target_rows = _records(progress.get("smoothed_target_progress"))
    dashboard_summary = _smoothed_weekly_dashboard_summary(progress_summary, target_rows)
    status_table = _smoothed_target_status_table(target_rows)
    dashboard_id = _stable_id(
        "smoothed-weekly-dashboard",
        progress_id,
        dashboard_summary,
        status_table,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / dashboard_id)
    root.mkdir(parents=True, exist_ok=False)
    dashboard_summary["dashboard_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_weekly_dashboard_manifest",
        "dashboard_id": root.name,
        "progress_id": progress_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_weekly_dashboard_manifest_path": str(
            root / "smoothed_weekly_dashboard_manifest.json"
        ),
        "smoothed_dashboard_summary_path": str(root / "smoothed_dashboard_summary.json"),
        "smoothed_target_status_table_path": str(root / "smoothed_target_status_table.json"),
        "smoothed_weekly_dashboard_report_path": str(root / "smoothed_weekly_dashboard_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_weekly_dashboard_reader_brief(dashboard_summary, status_table)
    _write_json(root / "smoothed_weekly_dashboard_manifest.json", manifest)
    _write_json(root / "smoothed_dashboard_summary.json", dashboard_summary)
    _write_json(root / "smoothed_target_status_table.json", status_table)
    _write_text(
        root / "smoothed_weekly_dashboard_report.md",
        render_smoothed_weekly_dashboard_report(manifest, dashboard_summary, status_table),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_weekly_dashboard",
        root.name,
        root / "smoothed_weekly_dashboard_manifest.json",
    )
    return {
        "dashboard_id": root.name,
        "dashboard_dir": root,
        "manifest": manifest,
        "smoothed_dashboard_summary": dashboard_summary,
        "smoothed_target_status_table": status_table,
        "reader_brief_section": reader,
    }


def smoothed_weekly_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=dashboard_id,
        latest_pointer="latest_smoothed_weekly_dashboard",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_weekly_dashboard_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_weekly_dashboard_manifest.json"),
        "smoothed_dashboard_summary": _read_json(root / "smoothed_dashboard_summary.json"),
        "smoothed_target_status_table": _read_json(root / "smoothed_target_status_table.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "dashboard_dir": str(root),
    }


def validate_smoothed_weekly_dashboard_artifact(
    *,
    dashboard_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = output_dir / dashboard_id
    manifest = _read_optional_json(root / "smoothed_weekly_dashboard_manifest.json") or {}
    summary = _read_optional_json(root / "smoothed_dashboard_summary.json") or {}
    table = _read_optional_json(root / "smoothed_target_status_table.json") or {}
    targets = _records(table.get("targets"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_weekly_dashboard_manifest.json",
            "smoothed_dashboard_summary.json",
            "smoothed_target_status_table.json",
            "smoothed_weekly_dashboard_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "dashboard_id_matches",
                manifest.get("dashboard_id") == dashboard_id
                and summary.get("dashboard_id") == dashboard_id,
                "",
            ),
            _check("target_status_table_readable", len(targets) == 3, str(len(targets))),
            _check(
                "ready_for_switch_recheck_false_when_in_progress",
                summary.get("ready_for_switch_recheck") is False
                or summary.get("forward_confirmation_status") == "READY_FOR_REVIEW",
                _text(summary.get("forward_confirmation_status")),
            ),
            _check(
                "weekly_recommendation_continue_observation",
                summary.get("weekly_recommendation") == "continue_observation",
                _text(summary.get("weekly_recommendation")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, table, *targets), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_weekly_dashboard_validation",
        dashboard_id,
        checks,
    )


def update_smoothed_event_monitor(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    progress = smoothed_forward_progress_report_payload(
        progress_id=progress_id,
        output_dir=progress_dir,
    )
    progress_summary = _mapping(progress.get("smoothed_forward_progress_summary"))
    effective_classification_dir = (
        output_dir.parent / "smoothed_forward_classification"
        if classification_dir == DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
        and output_dir != DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
        else classification_dir
    )
    classified = _collect_classified_smoothed_events(effective_classification_dir)
    sideways_events = [row for row in classified if row.get("sideways_relevant") is True]
    recovery_events = [row for row in classified if row.get("recovery_lag_relevant") is True]
    summary = _smoothed_event_accumulation_summary(
        progress_summary,
        sideways_events,
        recovery_events,
    )
    monitor_id = _stable_id(
        "smoothed-event-monitor",
        progress_id,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / monitor_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["monitor_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_event_monitor_manifest",
        "monitor_id": root.name,
        "progress_id": progress_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_event_monitor_manifest_path": str(root / "smoothed_event_monitor_manifest.json"),
        "sideways_event_inventory_path": str(root / "sideways_event_inventory.jsonl"),
        "recovery_event_inventory_path": str(root / "recovery_event_inventory.jsonl"),
        "event_accumulation_summary_path": str(root / "event_accumulation_summary.json"),
        "smoothed_event_monitor_report_path": str(root / "smoothed_event_monitor_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_event_monitor_manifest.json", manifest)
    _write_jsonl(root / "sideways_event_inventory.jsonl", sideways_events)
    _write_jsonl(root / "recovery_event_inventory.jsonl", recovery_events)
    _write_json(root / "event_accumulation_summary.json", summary)
    _write_text(
        root / "smoothed_event_monitor_report.md",
        render_smoothed_event_monitor_report(
            manifest,
            summary,
            sideways_events,
            recovery_events,
        ),
    )
    _write_latest_pointer(
        "latest_smoothed_event_monitor",
        root.name,
        root / "smoothed_event_monitor_manifest.json",
    )
    return {
        "monitor_id": root.name,
        "monitor_dir": root,
        "manifest": manifest,
        "sideways_event_inventory": sideways_events,
        "recovery_event_inventory": recovery_events,
        "event_accumulation_summary": summary,
    }


def smoothed_event_monitor_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=monitor_id,
        latest_pointer="latest_smoothed_event_monitor",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_event_monitor_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_event_monitor_manifest.json"),
        "sideways_event_inventory": _read_jsonl(root / "sideways_event_inventory.jsonl"),
        "recovery_event_inventory": _read_jsonl(root / "recovery_event_inventory.jsonl"),
        "event_accumulation_summary": _read_json(root / "event_accumulation_summary.json"),
        "monitor_dir": str(root),
    }


def validate_smoothed_event_monitor_artifact(
    *,
    monitor_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> dict[str, Any]:
    root = output_dir / monitor_id
    manifest = _read_optional_json(root / "smoothed_event_monitor_manifest.json") or {}
    sideways = _read_jsonl(root / "sideways_event_inventory.jsonl")
    recovery = _read_jsonl(root / "recovery_event_inventory.jsonl")
    summary = _read_optional_json(root / "event_accumulation_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_event_monitor_manifest.json",
            "sideways_event_inventory.jsonl",
            "recovery_event_inventory.jsonl",
            "event_accumulation_summary.json",
            "smoothed_event_monitor_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "monitor_id_matches",
                manifest.get("monitor_id") == monitor_id
                and summary.get("monitor_id") == monitor_id,
                "",
            ),
            _check(
                "sideways_summary_present",
                bool(_mapping(summary.get("sideways_events"))),
                "",
            ),
            _check(
                "recovery_summary_present",
                bool(_mapping(summary.get("recovery_events"))),
                "",
            ),
            _check(
                "lag_warning_false_without_events",
                not any(row.get("lag_warning") is True for row in recovery),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *sideways, *recovery), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_event_monitor_validation",
        monitor_id,
        checks,
    )


def recheck_smoothed_switch_readiness(
    *,
    dashboard_id: str,
    monitor_id: str,
    switch_plan_id: str,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    dashboard = smoothed_weekly_dashboard_report_payload(
        dashboard_id=dashboard_id,
        output_dir=dashboard_dir,
    )
    monitor = smoothed_event_monitor_report_payload(monitor_id=monitor_id, output_dir=monitor_dir)
    switch_plan = paper_shadow_primary_switch_report_payload(
        switch_plan_id=switch_plan_id,
        output_dir=switch_plan_dir,
    )
    dashboard_summary = _mapping(dashboard.get("smoothed_dashboard_summary"))
    event_summary = _mapping(monitor.get("event_accumulation_summary"))
    plan = _mapping(switch_plan.get("primary_switch_plan"))
    criteria = _smoothed_switch_readiness_criteria(dashboard_summary, event_summary)
    decision = _smoothed_switch_readiness_decision(dashboard_summary, plan, criteria)
    recheck_id = _stable_id(
        "smoothed-switch-readiness",
        dashboard_id,
        monitor_id,
        switch_plan_id,
        decision,
        criteria,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / recheck_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["recheck_id"] = root.name
    criteria["recheck_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_switch_readiness_manifest",
        "recheck_id": root.name,
        "dashboard_id": dashboard_id,
        "monitor_id": monitor_id,
        "switch_plan_id": switch_plan_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_switch_readiness_manifest_path": str(
            root / "smoothed_switch_readiness_manifest.json"
        ),
        "switch_readiness_decision_path": str(root / "switch_readiness_decision.json"),
        "switch_readiness_criteria_path": str(root / "switch_readiness_criteria.json"),
        "smoothed_switch_readiness_report_path": str(root / "smoothed_switch_readiness_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_switch_readiness_reader_brief(decision, criteria)
    _write_json(root / "smoothed_switch_readiness_manifest.json", manifest)
    _write_json(root / "switch_readiness_decision.json", decision)
    _write_json(root / "switch_readiness_criteria.json", criteria)
    _write_text(
        root / "smoothed_switch_readiness_report.md",
        render_smoothed_switch_readiness_report(manifest, decision, criteria),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_switch_readiness",
        root.name,
        root / "smoothed_switch_readiness_manifest.json",
    )
    return {
        "recheck_id": root.name,
        "recheck_dir": root,
        "manifest": manifest,
        "switch_readiness_decision": decision,
        "switch_readiness_criteria": criteria,
        "reader_brief_section": reader,
    }


def smoothed_switch_readiness_report_payload(
    *,
    recheck_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=recheck_id,
        latest_pointer="latest_smoothed_switch_readiness",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_switch_readiness_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_switch_readiness_manifest.json"),
        "switch_readiness_decision": _read_json(root / "switch_readiness_decision.json"),
        "switch_readiness_criteria": _read_json(root / "switch_readiness_criteria.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "recheck_dir": str(root),
    }


def validate_smoothed_switch_readiness_artifact(
    *,
    recheck_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> dict[str, Any]:
    root = output_dir / recheck_id
    manifest = _read_optional_json(root / "smoothed_switch_readiness_manifest.json") or {}
    decision = _read_optional_json(root / "switch_readiness_decision.json") or {}
    criteria = _read_optional_json(root / "switch_readiness_criteria.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_switch_readiness_manifest.json",
            "switch_readiness_decision.json",
            "switch_readiness_criteria.json",
            "smoothed_switch_readiness_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "recheck_id_matches",
                manifest.get("recheck_id") == recheck_id
                and decision.get("recheck_id") == recheck_id,
                "",
            ),
            _check("criteria_present", len(_records(criteria.get("criteria"))) == 3, ""),
            _check("can_execute_switch_false", decision.get("can_execute_switch") is False, ""),
            _check(
                "owner_decision_required_true",
                decision.get("owner_decision_required") is True,
                "",
            ),
            _check("auto_switch_false", decision.get("auto_switch") is False, ""),
            _check(
                "production_effect_none",
                decision.get("production_effect") == "none",
                _text(decision.get("production_effect")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, decision, criteria), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_switch_readiness_validation",
        recheck_id,
        checks,
    )


def build_smoothed_owner_renewal_pack(
    *,
    recheck_id: str,
    owner_promotion_id: str,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    recheck = smoothed_switch_readiness_report_payload(
        recheck_id=recheck_id,
        output_dir=recheck_dir,
    )
    owner = smoothed_owner_promotion_report_payload(
        decision_id=owner_promotion_id,
        output_dir=owner_promotion_dir,
    )
    recheck_decision = _mapping(recheck.get("switch_readiness_decision"))
    owner_decision = _mapping(owner.get("owner_promotion_decision"))
    options = _smoothed_owner_renewal_options(recheck_decision, owner_decision)
    renewal_id = _stable_id(
        "smoothed-owner-renewal",
        recheck_id,
        owner_promotion_id,
        options,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / renewal_id)
    root.mkdir(parents=True, exist_ok=False)
    options["renewal_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_owner_renewal_manifest",
        "renewal_id": root.name,
        "recheck_id": recheck_id,
        "owner_promotion_id": owner_promotion_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_owner_renewal_manifest_path": str(root / "smoothed_owner_renewal_manifest.json"),
        "owner_renewal_options_path": str(root / "owner_renewal_options.json"),
        "owner_renewal_checklist_path": str(root / "owner_renewal_checklist.md"),
        "smoothed_owner_renewal_report_path": str(root / "smoothed_owner_renewal_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = render_smoothed_owner_renewal_checklist(options)
    reader = render_smoothed_owner_renewal_reader_brief(options)
    _write_json(root / "smoothed_owner_renewal_manifest.json", manifest)
    _write_json(root / "owner_renewal_options.json", options)
    _write_text(root / "owner_renewal_checklist.md", checklist)
    _write_text(
        root / "smoothed_owner_renewal_report.md",
        render_smoothed_owner_renewal_report(manifest, options),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_owner_renewal",
        root.name,
        root / "smoothed_owner_renewal_manifest.json",
    )
    return {
        "renewal_id": root.name,
        "renewal_dir": root,
        "manifest": manifest,
        "owner_renewal_options": options,
        "owner_renewal_checklist": checklist,
        "reader_brief_section": reader,
    }


def smoothed_owner_renewal_report_payload(
    *,
    renewal_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=renewal_id,
        latest_pointer="latest_smoothed_owner_renewal",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_owner_renewal_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_owner_renewal_manifest.json"),
        "owner_renewal_options": _read_json(root / "owner_renewal_options.json"),
        "owner_renewal_checklist": (root / "owner_renewal_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "renewal_dir": str(root),
    }


def validate_smoothed_owner_renewal_artifact(
    *,
    renewal_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> dict[str, Any]:
    root = output_dir / renewal_id
    manifest = _read_optional_json(root / "smoothed_owner_renewal_manifest.json") or {}
    options = _read_optional_json(root / "owner_renewal_options.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_owner_renewal_manifest.json",
            "owner_renewal_options.json",
            "owner_renewal_checklist.md",
            "smoothed_owner_renewal_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "renewal_id_matches",
                manifest.get("renewal_id") == renewal_id
                and options.get("renewal_id") == renewal_id,
                "",
            ),
            _check("owner_options_present", len(_records(options.get("owner_options"))) == 5, ""),
            _check(
                "recommended_owner_action_visible",
                _text(options.get("recommended_owner_action")) != "",
                _text(options.get("recommended_owner_action")),
            ),
            _check("auto_switch_false", options.get("auto_switch") is False, ""),
            _check(
                "not_official_target_weights_true",
                options.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                options.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", options.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, options), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_renewal_validation",
        renewal_id,
        checks,
    )


def run_smoothed_daily_emission(
    *,
    as_of: date,
    target_id: str | None = None,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    model_target = (
        model_target_report_payload(target_id=target_id, output_dir=model_target_dir)
        if target_id
        else _optional_latest_model_target_payload(model_target_dir)
    )
    weights = _smoothed_emission_weights(model_target)
    symbols = _smoothed_symbols_from_weight_map(weights)
    pivot = _smoothed_price_pivot_or_empty(
        price_cache_path,
        symbols,
        start=as_of - timedelta(days=60),
    )
    latest_price = _smoothed_latest_price_date_or_none(pivot)
    regime_context = _smoothed_regime_context_for_as_of(pivot, as_of)
    weight_validation = _smoothed_weight_validation(weights)
    data_quality = _smoothed_emission_data_quality(
        as_of=as_of,
        latest_price_date=latest_price,
        target_present=bool(model_target),
        weights_valid=weight_validation.get("constraint_status") != "FAIL",
        regime_context=regime_context,
    )
    event_status, skip_reasons = _smoothed_daily_event_status(
        as_of=as_of,
        latest_price_date=latest_price,
        target_present=bool(model_target),
        weights_valid=weight_validation.get("constraint_status") != "FAIL",
        data_quality=data_quality,
    )
    event_id = _stable_id(
        "smoothed-forward-event",
        as_of.isoformat(),
        model_target.get("target_id"),
        weights,
    )
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "as_of": as_of.isoformat(),
        "event_type": "SMOOTHED_FORWARD_OBSERVATION",
        "candidate_method": "smooth_weights_3d_limited_adjustment",
        "baseline_method": "limited_adjustment",
        "secondary_method": "smooth_weights_5d_limited_adjustment",
        "outcome_windows": list(SMOOTHED_CONFIRMATION_WINDOWS),
        "regime_context": regime_context,
        "data_quality": data_quality["data_quality"],
        "event_status": event_status,
        "skip_reasons": skip_reasons,
        "source_model_target_id": _text(model_target.get("target_id")),
        "source_model_target_as_of": _text(model_target.get("as_of")),
        "requested_as_of": as_of.isoformat(),
        "latest_price_date": latest_price.isoformat() if latest_price else None,
        **SYSTEM_TARGET_SAFETY,
    }
    event_weights = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "as_of": as_of.isoformat(),
        "weights": weights,
        "weight_validation": weight_validation,
        **SYSTEM_TARGET_SAFETY,
    }
    emission_id = _stable_id(
        "smoothed-daily-emission",
        as_of.isoformat(),
        event,
        event_weights,
        data_quality,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / emission_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_daily_emission_manifest",
        "emission_id": root.name,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "emitted_event_count": 1 if event_status == "ACTIVE" else 0,
        "event_status": event_status,
        "data_quality": data_quality["data_quality"],
        "future_data_used": False,
        "smoothed_daily_emission_manifest_path": str(
            root / "smoothed_daily_emission_manifest.json"
        ),
        "smoothed_forward_events_path": str(root / "smoothed_forward_events.jsonl"),
        "smoothed_event_weights_path": str(root / "smoothed_event_weights.json"),
        "smoothed_emission_data_quality_path": str(root / "smoothed_emission_data_quality.json"),
        "smoothed_daily_emission_report_path": str(root / "smoothed_daily_emission_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_daily_emission_reader_brief(manifest, event, data_quality)
    _write_json(root / "smoothed_daily_emission_manifest.json", manifest)
    _write_jsonl(root / "smoothed_forward_events.jsonl", [event])
    _write_json(root / "smoothed_event_weights.json", event_weights)
    _write_json(root / "smoothed_emission_data_quality.json", data_quality)
    _write_text(
        root / "smoothed_daily_emission_report.md",
        render_smoothed_daily_emission_report(
            manifest,
            event,
            event_weights,
            data_quality,
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_daily_emission",
        root.name,
        root / "smoothed_daily_emission_manifest.json",
    )
    return {
        "emission_id": root.name,
        "emission_dir": root,
        "manifest": manifest,
        "smoothed_forward_events": [event],
        "smoothed_event_weights": event_weights,
        "smoothed_emission_data_quality": data_quality,
        "reader_brief_section": reader,
    }


def smoothed_daily_emission_report_payload(
    *,
    emission_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=emission_id,
        latest_pointer="latest_smoothed_daily_emission",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_daily_emission_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_daily_emission_manifest.json"),
        "smoothed_forward_events": _read_jsonl(root / "smoothed_forward_events.jsonl"),
        "smoothed_event_weights": _read_json(root / "smoothed_event_weights.json"),
        "smoothed_emission_data_quality": _read_json(root / "smoothed_emission_data_quality.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "emission_dir": str(root),
    }


def validate_smoothed_daily_emission_artifact(
    *,
    emission_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> dict[str, Any]:
    root = output_dir / emission_id
    manifest = _read_optional_json(root / "smoothed_daily_emission_manifest.json") or {}
    events = _read_jsonl(root / "smoothed_forward_events.jsonl")
    event_weights = _read_optional_json(root / "smoothed_event_weights.json") or {}
    data_quality = _read_optional_json(root / "smoothed_emission_data_quality.json") or {}
    validation = _mapping(event_weights.get("weight_validation"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_daily_emission_manifest.json",
            "smoothed_forward_events.jsonl",
            "smoothed_event_weights.json",
            "smoothed_emission_data_quality.json",
            "smoothed_daily_emission_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check("emission_id_matches", manifest.get("emission_id") == emission_id, ""),
            _check("single_event_recorded", len(events) == 1, str(len(events))),
            _check(
                "event_status_allowed",
                all(
                    row.get("event_status") in {"ACTIVE", "SKIPPED", "INSUFFICIENT_DATA"}
                    for row in events
                ),
                "",
            ),
            _check(
                "future_data_used_false",
                manifest.get("future_data_used") is False
                and data_quality.get("future_data_used") is False,
                "",
            ),
            _check(
                "weights_non_negative_when_present",
                validation.get("no_negative_weights") is True,
                _text(validation.get("constraint_status")),
            ),
            _check(
                "broker_action_allowed_false",
                manifest.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", manifest.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, event_weights, *events), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_daily_emission_validation",
        emission_id,
        checks,
    )


def scan_smoothed_outcome_due(
    *,
    as_of: date,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    emissions = _collect_smoothed_daily_emissions(emission_dir)
    events = [
        event
        for emission in emissions
        for event in emission["events"]
        if event.get("event_status") == "ACTIVE"
    ]
    active_event_ids = {_text(event.get("event_id")) for event in events if event.get("event_id")}
    event_weights = {
        weights["event_id"]: weights
        for emission in emissions
        for weights in [emission["weights"]]
        if weights.get("event_id") in active_event_ids
    }
    symbols = _smoothed_symbols_from_collected_event_weights(event_weights.values())
    pivot = _smoothed_price_pivot_or_empty(
        price_cache_path,
        symbols,
        start=AI_AFTER_CHATGPT_START,
    )
    trading_dates = [idx.date() for idx in pivot.index]
    due_rows = [
        _smoothed_due_window_row(
            event,
            window_days=int(_float(window)),
            scanner_as_of=as_of,
            trading_dates=trading_dates,
            pivot=pivot,
        )
        for event in events
        for window in _texts(event.get("outcome_windows"))
    ]
    summary = _smoothed_due_summary(as_of=as_of, rows=due_rows)
    due_id = _stable_id(
        "smoothed-outcome-due",
        as_of.isoformat(),
        due_rows,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / due_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["due_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_outcome_due_manifest",
        "due_id": root.name,
        "scanner_as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_outcome_due_manifest_path": str(root / "smoothed_outcome_due_manifest.json"),
        "due_windows_path": str(root / "due_windows.jsonl"),
        "due_summary_path": str(root / "due_summary.json"),
        "smoothed_outcome_due_report_path": str(root / "smoothed_outcome_due_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_outcome_due_manifest.json", manifest)
    _write_jsonl(root / "due_windows.jsonl", due_rows)
    _write_json(root / "due_summary.json", summary)
    _write_text(
        root / "smoothed_outcome_due_report.md",
        render_smoothed_outcome_due_report(manifest, summary, due_rows),
    )
    _write_latest_pointer(
        "latest_smoothed_outcome_due",
        root.name,
        root / "smoothed_outcome_due_manifest.json",
    )
    return {
        "due_id": root.name,
        "due_dir": root,
        "manifest": manifest,
        "due_windows": due_rows,
        "due_summary": summary,
    }


def smoothed_outcome_due_report_payload(
    *,
    due_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=due_id,
        latest_pointer="latest_smoothed_outcome_due",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_outcome_due_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_outcome_due_manifest.json"),
        "due_windows": _read_jsonl(root / "due_windows.jsonl"),
        "due_summary": _read_json(root / "due_summary.json"),
        "due_dir": str(root),
    }


def validate_smoothed_outcome_due_artifact(
    *,
    due_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
) -> dict[str, Any]:
    root = output_dir / due_id
    manifest = _read_optional_json(root / "smoothed_outcome_due_manifest.json") or {}
    rows = _read_jsonl(root / "due_windows.jsonl")
    summary = _read_optional_json(root / "due_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_outcome_due_manifest.json",
            "due_windows.jsonl",
            "due_summary.json",
            "smoothed_outcome_due_report.md",
        ),
    )
    due_logic_valid = all(
        (
            row.get("can_update") is False
            if row.get("due_status") != "DUE"
            else row.get("price_available") is True
        )
        for row in rows
    )
    no_future_update = all(
        row.get("can_update") is False
        for row in rows
        if _coerce_date(row.get("expected_end_date"), date.max)
        > _coerce_date(row.get("scanner_as_of"), date.min)
    )
    checks.extend(
        [
            _check(
                "due_id_matches",
                manifest.get("due_id") == due_id and summary.get("due_id") == due_id,
                "",
            ),
            _check(
                "total_windows_matches",
                int(_float(summary.get("total_windows_scanned"))) == len(rows),
                str(len(rows)),
            ),
            _check("due_logic_valid", due_logic_valid, ""),
            _check("no_future_as_of_update", no_future_update, ""),
            _check("broker_forbidden", _payload_safe(manifest, summary, *rows), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_outcome_due_validation",
        due_id,
        checks,
    )


def run_smoothed_outcome_update(
    *,
    due_id: str,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    due = smoothed_outcome_due_report_payload(due_id=due_id, output_dir=due_dir)
    due_rows = _records(due.get("due_windows"))
    event_weights = _smoothed_event_weights_by_id(emission_dir)
    symbols = _smoothed_symbols_from_collected_event_weights(event_weights.values())
    pivot = _smoothed_price_pivot_or_empty(
        price_cache_path,
        symbols,
        start=AI_AFTER_CHATGPT_START,
    )
    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in due_rows:
        if row.get("can_update") is not True:
            skipped.append(_smoothed_skipped_outcome_row(row, _text(row.get("due_status"))))
            continue
        weights = _mapping(event_weights.get(_text(row.get("event_id"))))
        result = _smoothed_updated_outcome_row(row, weights, pivot)
        if result is None:
            skipped.append(_smoothed_skipped_outcome_row(row, "PRICE_MISSING"))
        else:
            updated.append(result)
    summary = _smoothed_outcome_delta_summary(updated, skipped)
    update_id = _stable_id(
        "smoothed-outcome-update",
        due_id,
        updated,
        skipped,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / update_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["update_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_outcome_update_manifest",
        "update_id": root.name,
        "due_id": due_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "future_data_used": False,
        "smoothed_outcome_update_manifest_path": str(
            root / "smoothed_outcome_update_manifest.json"
        ),
        "updated_smoothed_outcomes_path": str(root / "updated_smoothed_outcomes.jsonl"),
        "skipped_smoothed_outcomes_path": str(root / "skipped_smoothed_outcomes.jsonl"),
        "smoothed_outcome_delta_summary_path": str(root / "smoothed_outcome_delta_summary.json"),
        "smoothed_outcome_update_report_path": str(root / "smoothed_outcome_update_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_outcome_update_reader_brief(summary)
    _write_json(root / "smoothed_outcome_update_manifest.json", manifest)
    _write_jsonl(root / "updated_smoothed_outcomes.jsonl", updated)
    _write_jsonl(root / "skipped_smoothed_outcomes.jsonl", skipped)
    _write_json(root / "smoothed_outcome_delta_summary.json", summary)
    _write_text(
        root / "smoothed_outcome_update_report.md",
        render_smoothed_outcome_update_report(manifest, summary, updated, skipped),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_outcome_update",
        root.name,
        root / "smoothed_outcome_update_manifest.json",
    )
    return {
        "update_id": root.name,
        "update_dir": root,
        "manifest": manifest,
        "updated_smoothed_outcomes": updated,
        "skipped_smoothed_outcomes": skipped,
        "smoothed_outcome_delta_summary": summary,
        "reader_brief_section": reader,
    }


def smoothed_outcome_update_report_payload(
    *,
    update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=update_id,
        latest_pointer="latest_smoothed_outcome_update",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_outcome_update_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_outcome_update_manifest.json"),
        "updated_smoothed_outcomes": _read_jsonl(root / "updated_smoothed_outcomes.jsonl"),
        "skipped_smoothed_outcomes": _read_jsonl(root / "skipped_smoothed_outcomes.jsonl"),
        "smoothed_outcome_delta_summary": _read_json(root / "smoothed_outcome_delta_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "update_dir": str(root),
    }


def validate_smoothed_outcome_update_artifact(
    *,
    update_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
) -> dict[str, Any]:
    root = output_dir / update_id
    manifest = _read_optional_json(root / "smoothed_outcome_update_manifest.json") or {}
    updated = _read_jsonl(root / "updated_smoothed_outcomes.jsonl")
    skipped = _read_jsonl(root / "skipped_smoothed_outcomes.jsonl")
    summary = _read_optional_json(root / "smoothed_outcome_delta_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_outcome_update_manifest.json",
            "updated_smoothed_outcomes.jsonl",
            "skipped_smoothed_outcomes.jsonl",
            "smoothed_outcome_delta_summary.json",
            "smoothed_outcome_update_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "update_id_matches",
                manifest.get("update_id") == update_id and summary.get("update_id") == update_id,
                "",
            ),
            _check(
                "updated_count_matches",
                int(_float(summary.get("updated_count"))) == len(updated),
                str(len(updated)),
            ),
            _check(
                "skipped_count_matches",
                int(_float(summary.get("skipped_count"))) == len(skipped),
                str(len(skipped)),
            ),
            _check(
                "all_updated_available",
                all(row.get("outcome_status") == "AVAILABLE" for row in updated),
                "",
            ),
            _check(
                "future_data_used_false",
                manifest.get("future_data_used") is False
                and all(row.get("future_data_used") is False for row in updated),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *updated, *skipped), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_outcome_update_validation",
        update_id,
        checks,
    )


def run_smoothed_forward_classification(
    *,
    update_id: str,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    update = smoothed_outcome_update_report_payload(update_id=update_id, output_dir=update_dir)
    outcomes = _records(update.get("updated_smoothed_outcomes"))
    events = _smoothed_events_by_id(emission_dir)
    weights = _smoothed_event_weights_by_id(emission_dir)
    classified = [_smoothed_classified_forward_event(row, events, weights) for row in outcomes]
    summary = _smoothed_classification_summary(classified)
    classification_id = _stable_id(
        "smoothed-forward-classification",
        update_id,
        classified,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / classification_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["classification_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_forward_classification_manifest",
        "classification_id": root.name,
        "update_id": update_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_forward_classification_manifest_path": str(
            root / "smoothed_forward_classification_manifest.json"
        ),
        "classified_forward_events_path": str(root / "classified_forward_events.jsonl"),
        "classification_summary_path": str(root / "classification_summary.json"),
        "smoothed_forward_classification_report_path": str(
            root / "smoothed_forward_classification_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "smoothed_forward_classification_manifest.json", manifest)
    _write_jsonl(root / "classified_forward_events.jsonl", classified)
    _write_json(root / "classification_summary.json", summary)
    _write_text(
        root / "smoothed_forward_classification_report.md",
        render_smoothed_forward_classification_report(manifest, summary, classified),
    )
    _write_latest_pointer(
        "latest_smoothed_forward_classification",
        root.name,
        root / "smoothed_forward_classification_manifest.json",
    )
    return {
        "classification_id": root.name,
        "classification_dir": root,
        "manifest": manifest,
        "classified_forward_events": classified,
        "classification_summary": summary,
    }


def smoothed_forward_classification_report_payload(
    *,
    classification_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=classification_id,
        latest_pointer="latest_smoothed_forward_classification",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_forward_classification_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_forward_classification_manifest.json"),
        "classified_forward_events": _read_jsonl(root / "classified_forward_events.jsonl"),
        "classification_summary": _read_json(root / "classification_summary.json"),
        "classification_dir": str(root),
    }


def validate_smoothed_forward_classification_artifact(
    *,
    classification_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> dict[str, Any]:
    root = output_dir / classification_id
    manifest = _read_optional_json(root / "smoothed_forward_classification_manifest.json") or {}
    classified = _read_jsonl(root / "classified_forward_events.jsonl")
    summary = _read_optional_json(root / "classification_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_forward_classification_manifest.json",
            "classified_forward_events.jsonl",
            "classification_summary.json",
            "smoothed_forward_classification_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "classification_id_matches",
                manifest.get("classification_id") == classification_id
                and summary.get("classification_id") == classification_id,
                "",
            ),
            _check(
                "events_classified_matches",
                int(_float(summary.get("events_classified"))) == len(classified),
                str(len(classified)),
            ),
            _check(
                "confidence_allowed",
                all(
                    row.get("classification_confidence") in {"LOW", "MEDIUM", "HIGH"}
                    for row in classified
                ),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *classified), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_classification_validation",
        classification_id,
        checks,
    )


def run_smoothed_forward_weekly_run(
    *,
    week_ending: date,
    target_id: str | None = None,
    binding_id: str | None = None,
    switch_plan_id: str | None = None,
    owner_promotion_id: str | None = None,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    renewal_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    steps: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {}

    emission = run_smoothed_daily_emission(
        as_of=week_ending,
        target_id=target_id,
        model_target_dir=model_target_dir,
        output_dir=emission_dir,
        price_cache_path=price_cache_path,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "daily_emission", emission["emission_id"])

    due = scan_smoothed_outcome_due(
        as_of=week_ending,
        emission_dir=emission_dir,
        output_dir=due_dir,
        price_cache_path=price_cache_path,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "outcome_due_scan", due["due_id"])

    update = run_smoothed_outcome_update(
        due_id=due["due_id"],
        due_dir=due_dir,
        emission_dir=emission_dir,
        output_dir=update_dir,
        price_cache_path=price_cache_path,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "outcome_update", update["update_id"])

    classification = run_smoothed_forward_classification(
        update_id=update["update_id"],
        update_dir=update_dir,
        emission_dir=emission_dir,
        output_dir=classification_dir,
        generated_at=generated,
    )
    _record_weekly_step(
        steps,
        artifacts,
        "forward_classification",
        classification["classification_id"],
    )

    resolved_binding_id = binding_id or _latest_pointer_artifact_id(
        "latest_smoothed_forward_binding"
    )
    resolved_switch_plan_id = switch_plan_id or _latest_pointer_artifact_id(
        "latest_paper_shadow_primary_switch"
    )
    resolved_owner_promotion_id = owner_promotion_id or _latest_pointer_artifact_id(
        "latest_smoothed_owner_promotion"
    )
    progress = update_smoothed_forward_progress(
        binding_id=resolved_binding_id,
        binding_dir=binding_dir,
        output_dir=progress_dir,
        outcome_update_dir=update_dir,
        classification_dir=classification_dir,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "progress_update", progress["progress_id"])

    dashboard = build_smoothed_weekly_dashboard(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=dashboard_dir,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "weekly_dashboard", dashboard["dashboard_id"])

    monitor = update_smoothed_event_monitor(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=monitor_dir,
        classification_dir=classification_dir,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "event_monitor", monitor["monitor_id"])

    recheck = recheck_smoothed_switch_readiness(
        dashboard_id=dashboard["dashboard_id"],
        monitor_id=monitor["monitor_id"],
        switch_plan_id=resolved_switch_plan_id,
        dashboard_dir=dashboard_dir,
        monitor_dir=monitor_dir,
        switch_plan_dir=switch_plan_dir,
        output_dir=recheck_dir,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "switch_readiness", recheck["recheck_id"])

    renewal = build_smoothed_owner_renewal_pack(
        recheck_id=recheck["recheck_id"],
        owner_promotion_id=resolved_owner_promotion_id,
        recheck_dir=recheck_dir,
        owner_promotion_dir=owner_promotion_dir,
        output_dir=renewal_dir,
        generated_at=generated,
    )
    _record_weekly_step(steps, artifacts, "owner_renewal", renewal["renewal_id"])

    summary = _smoothed_weekly_run_summary(
        week_ending=week_ending,
        emission=emission,
        due=due,
        update=update,
        classification=classification,
        progress=progress,
        recheck=recheck,
    )
    weekly_run_id = _stable_id(
        "smoothed-forward-weekly-run",
        week_ending.isoformat(),
        steps,
        artifacts,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / weekly_run_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["weekly_run_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_forward_weekly_run_manifest",
        "weekly_run_id": root.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_forward_weekly_run_manifest_path": str(
            root / "smoothed_forward_weekly_run_manifest.json"
        ),
        "weekly_run_steps_path": str(root / "weekly_run_steps.json"),
        "weekly_run_artifacts_path": str(root / "weekly_run_artifacts.json"),
        "weekly_run_summary_path": str(root / "weekly_run_summary.json"),
        "smoothed_forward_weekly_run_report_path": str(
            root / "smoothed_forward_weekly_run_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    step_payload = {"schema_version": SCHEMA_VERSION, "steps": steps, **SYSTEM_TARGET_SAFETY}
    artifact_payload = {
        "schema_version": SCHEMA_VERSION,
        "artifacts": artifacts,
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_forward_weekly_run_reader_brief(summary)
    _write_json(root / "smoothed_forward_weekly_run_manifest.json", manifest)
    _write_json(root / "weekly_run_steps.json", step_payload)
    _write_json(root / "weekly_run_artifacts.json", artifact_payload)
    _write_json(root / "weekly_run_summary.json", summary)
    _write_text(
        root / "smoothed_forward_weekly_run_report.md",
        render_smoothed_forward_weekly_run_report(manifest, step_payload, summary),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_forward_weekly_run",
        root.name,
        root / "smoothed_forward_weekly_run_manifest.json",
    )
    return {
        "weekly_run_id": root.name,
        "weekly_run_dir": root,
        "manifest": manifest,
        "weekly_run_steps": step_payload,
        "weekly_run_artifacts": artifact_payload,
        "weekly_run_summary": summary,
        "reader_brief_section": reader,
    }


def smoothed_forward_weekly_run_report_payload(
    *,
    weekly_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=weekly_run_id,
        latest_pointer="latest_smoothed_forward_weekly_run",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_forward_weekly_run_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_forward_weekly_run_manifest.json"),
        "weekly_run_steps": _read_json(root / "weekly_run_steps.json"),
        "weekly_run_artifacts": _read_json(root / "weekly_run_artifacts.json"),
        "weekly_run_summary": _read_json(root / "weekly_run_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "weekly_run_dir": str(root),
    }


def validate_smoothed_forward_weekly_run_artifact(
    *,
    weekly_run_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> dict[str, Any]:
    root = output_dir / weekly_run_id
    manifest = _read_optional_json(root / "smoothed_forward_weekly_run_manifest.json") or {}
    steps_payload = _read_optional_json(root / "weekly_run_steps.json") or {}
    artifacts_payload = _read_optional_json(root / "weekly_run_artifacts.json") or {}
    summary = _read_optional_json(root / "weekly_run_summary.json") or {}
    steps = _records(steps_payload.get("steps"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_forward_weekly_run_manifest.json",
            "weekly_run_steps.json",
            "weekly_run_artifacts.json",
            "weekly_run_summary.json",
            "smoothed_forward_weekly_run_report.md",
            "reader_brief_section.md",
        ),
    )
    required_steps = {
        "daily_emission",
        "outcome_due_scan",
        "outcome_update",
        "forward_classification",
        "progress_update",
        "weekly_dashboard",
        "event_monitor",
        "switch_readiness",
        "owner_renewal",
    }
    present_steps = {_text(row.get("step")) for row in steps}
    checks.extend(
        [
            _check(
                "weekly_run_id_matches",
                manifest.get("weekly_run_id") == weekly_run_id
                and summary.get("weekly_run_id") == weekly_run_id,
                "",
            ),
            _check(
                "required_steps_present",
                required_steps.issubset(present_steps),
                ",".join(sorted(present_steps)),
            ),
            _check(
                "all_steps_pass_or_skipped",
                all(row.get("status") in {"PASS", "SKIPPED"} for row in steps),
                "",
            ),
            _check(
                "can_execute_switch_false",
                summary.get("can_execute_switch") is False,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                summary.get("broker_action_allowed") is False,
                "",
            ),
            _check(
                "production_effect_none",
                summary.get("production_effect") == "none",
                _text(summary.get("production_effect")),
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, steps_payload, artifacts_payload, summary),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_weekly_run_validation",
        weekly_run_id,
        checks,
    )


def run_smoothed_data_preflight(
    *,
    requested_as_of: date | None = None,
    requested_week_ending: date | None = None,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    requested_date = _smoothed_requested_date(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
    )
    quality_report = _smoothed_preflight_data_quality_report(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=requested_date,
    )
    snapshot = _smoothed_data_freshness_snapshot(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
        quality_report=quality_report,
        prices_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=model_target_dir,
    )
    command_matrix = _smoothed_runnable_command_matrix(snapshot)
    blocked_matrix = _smoothed_blocked_reason_matrix(snapshot)
    preflight_id = _stable_id(
        "smoothed-data-preflight",
        requested_as_of.isoformat() if requested_as_of else "",
        requested_week_ending.isoformat() if requested_week_ending else "",
        snapshot,
        command_matrix,
        blocked_matrix,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / preflight_id)
    root.mkdir(parents=True, exist_ok=False)
    snapshot["preflight_id"] = root.name
    command_matrix["preflight_id"] = root.name
    blocked_matrix["preflight_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_data_preflight_manifest",
        "preflight_id": root.name,
        "requested_as_of": requested_as_of.isoformat() if requested_as_of else None,
        "requested_week_ending": (
            requested_week_ending.isoformat() if requested_week_ending else None
        ),
        "latest_valid_as_of": snapshot.get("latest_valid_as_of"),
        "freshness_status": snapshot.get("freshness_status"),
        "validate_data_status": snapshot.get("validate_data_status"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_data_preflight_manifest_path": str(
            root / "smoothed_data_preflight_manifest.json"
        ),
        "data_freshness_snapshot_path": str(root / "data_freshness_snapshot.json"),
        "runnable_command_matrix_path": str(root / "runnable_command_matrix.json"),
        "blocked_reason_matrix_path": str(root / "blocked_reason_matrix.json"),
        "smoothed_data_preflight_report_path": str(root / "smoothed_data_preflight_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_data_preflight_reader_brief(snapshot, command_matrix)
    _write_json(root / "smoothed_data_preflight_manifest.json", manifest)
    _write_json(root / "data_freshness_snapshot.json", snapshot)
    _write_json(root / "runnable_command_matrix.json", command_matrix)
    _write_json(root / "blocked_reason_matrix.json", blocked_matrix)
    _write_text(
        root / "smoothed_data_preflight_report.md",
        render_smoothed_data_preflight_report(
            manifest,
            snapshot,
            command_matrix,
            blocked_matrix,
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_data_preflight",
        root.name,
        root / "smoothed_data_preflight_manifest.json",
    )
    return {
        "preflight_id": root.name,
        "preflight_dir": root,
        "manifest": manifest,
        "data_freshness_snapshot": snapshot,
        "runnable_command_matrix": command_matrix,
        "blocked_reason_matrix": blocked_matrix,
        "reader_brief_section": reader,
    }


def smoothed_data_preflight_report_payload(
    *,
    preflight_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=preflight_id,
        latest_pointer="latest_smoothed_data_preflight",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_data_preflight_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_data_preflight_manifest.json"),
        "data_freshness_snapshot": _read_json(root / "data_freshness_snapshot.json"),
        "runnable_command_matrix": _read_json(root / "runnable_command_matrix.json"),
        "blocked_reason_matrix": _read_json(root / "blocked_reason_matrix.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "preflight_dir": str(root),
    }


def validate_smoothed_data_preflight_artifact(
    *,
    preflight_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> dict[str, Any]:
    root = output_dir / preflight_id
    manifest = _read_optional_json(root / "smoothed_data_preflight_manifest.json") or {}
    snapshot = _read_optional_json(root / "data_freshness_snapshot.json") or {}
    command_matrix = _read_optional_json(root / "runnable_command_matrix.json") or {}
    blocked_matrix = _read_optional_json(root / "blocked_reason_matrix.json") or {}
    commands = _records(command_matrix.get("commands"))
    freshness_status = _text(snapshot.get("freshness_status"))
    blocked_commands = [row for row in commands if row.get("status") == "BLOCKED"]
    checks = _required_file_checks(
        root,
        (
            "smoothed_data_preflight_manifest.json",
            "data_freshness_snapshot.json",
            "runnable_command_matrix.json",
            "blocked_reason_matrix.json",
            "smoothed_data_preflight_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "preflight_id_matches",
                manifest.get("preflight_id") == preflight_id
                and snapshot.get("preflight_id") == preflight_id,
                "",
            ),
            _check(
                "freshness_status_allowed",
                freshness_status
                in {
                    "READY",
                    "READY_WITH_WARNINGS",
                    "BLOCKED_STALE_DATA",
                    "BLOCKED_FUTURE_AS_OF",
                    "BLOCKED_MISSING_PRICE",
                    "BLOCKED_DATA_QUALITY_FAIL",
                    "LATEST_AVAILABLE_ONLY",
                },
                freshness_status,
            ),
            _check(
                "latest_valid_visible_when_available",
                "latest_valid_as_of" in snapshot,
                _text(snapshot.get("latest_valid_as_of")),
            ),
            _check("command_matrix_non_empty", bool(commands), str(len(commands))),
            _check(
                "blocked_status_has_blocked_command",
                not freshness_status.startswith("BLOCKED") or bool(blocked_commands),
                freshness_status,
            ),
            _check(
                "blocked_reasons_listed",
                isinstance(blocked_matrix.get("blocked_reasons"), list),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, snapshot, command_matrix), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_data_preflight_validation",
        preflight_id,
        checks,
    )


def run_smoothed_latest_emission(
    *,
    preflight_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    preflight = smoothed_data_preflight_report_payload(
        preflight_id=preflight_id,
        output_dir=preflight_dir,
    )
    snapshot = _mapping(preflight.get("data_freshness_snapshot"))
    requested_date = _coerce_date(
        snapshot.get("requested_as_of") or snapshot.get("requested_week_ending"),
        date.min,
    )
    resolved_as_of = _coerce_date(snapshot.get("latest_valid_as_of"), date.min)
    if resolved_as_of == date.min:
        raise DynamicV3SystemTargetError("preflight has no latest_valid_as_of for fallback")
    if resolved_as_of > requested_date and requested_date != date.min:
        resolved_as_of = requested_date
    emission = run_smoothed_daily_emission(
        as_of=resolved_as_of,
        model_target_dir=model_target_dir,
        output_dir=emission_dir,
        price_cache_path=price_cache_path,
        generated_at=generated,
    )
    manifest = emission["manifest"]
    event = emission["smoothed_forward_events"][0]
    latest_emission_id = _stable_id(
        "smoothed-latest-emission",
        preflight_id,
        requested_date.isoformat(),
        resolved_as_of.isoformat(),
        manifest.get("emission_id"),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / latest_emission_id)
    root.mkdir(parents=True, exist_ok=False)
    resolution = {
        "schema_version": SCHEMA_VERSION,
        "latest_emission_id": root.name,
        "source_preflight_id": preflight_id,
        "requested_as_of": requested_date.isoformat(),
        "resolved_as_of": resolved_as_of.isoformat(),
        "resolution_reason": (
            "latest_valid_as_of_fallback"
            if resolved_as_of != requested_date
            else "requested_as_of_supported"
        ),
        "fallback_scope": "daily_emission_only",
        "due_scan_allowed": False,
        "outcome_update_allowed": False,
        "future_data_used": False,
        "data_quality": event.get("data_quality"),
        "source_preflight_freshness_status": snapshot.get("freshness_status"),
        "source_preflight_validate_data_status": snapshot.get("validate_data_status"),
        "emission_status": event.get("event_status"),
        **SYSTEM_TARGET_SAFETY,
    }
    links = {
        "schema_version": SCHEMA_VERSION,
        "latest_emission_id": root.name,
        "resolved_as_of": resolved_as_of.isoformat(),
        "daily_emission_id": manifest.get("emission_id"),
        "daily_emission_dir": emission["emission_dir"].as_posix(),
        "emitted_event_count": manifest.get("emitted_event_count"),
        "event_status": event.get("event_status"),
        **SYSTEM_TARGET_SAFETY,
    }
    latest_manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_latest_emission_manifest",
        "latest_emission_id": root.name,
        "source_preflight_id": preflight_id,
        "requested_as_of": requested_date.isoformat(),
        "resolved_as_of": resolved_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_latest_emission_manifest_path": str(
            root / "smoothed_latest_emission_manifest.json"
        ),
        "latest_emission_resolution_path": str(root / "latest_emission_resolution.json"),
        "latest_emission_artifact_links_path": str(root / "latest_emission_artifact_links.json"),
        "smoothed_latest_emission_report_path": str(root / "smoothed_latest_emission_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_latest_emission_reader_brief(resolution, links)
    _write_json(root / "smoothed_latest_emission_manifest.json", latest_manifest)
    _write_json(root / "latest_emission_resolution.json", resolution)
    _write_json(root / "latest_emission_artifact_links.json", links)
    _write_text(
        root / "smoothed_latest_emission_report.md",
        render_smoothed_latest_emission_report(latest_manifest, resolution, links),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_latest_emission",
        root.name,
        root / "smoothed_latest_emission_manifest.json",
    )
    return {
        "latest_emission_id": root.name,
        "latest_emission_dir": root,
        "manifest": latest_manifest,
        "latest_emission_resolution": resolution,
        "latest_emission_artifact_links": links,
        "daily_emission": emission,
        "reader_brief_section": reader,
    }


def smoothed_latest_emission_report_payload(
    *,
    latest_emission_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=latest_emission_id,
        latest_pointer="latest_smoothed_latest_emission",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_latest_emission_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_latest_emission_manifest.json"),
        "latest_emission_resolution": _read_json(root / "latest_emission_resolution.json"),
        "latest_emission_artifact_links": _read_json(root / "latest_emission_artifact_links.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "latest_emission_dir": str(root),
    }


def validate_smoothed_latest_emission_artifact(
    *,
    latest_emission_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> dict[str, Any]:
    root = output_dir / latest_emission_id
    manifest = _read_optional_json(root / "smoothed_latest_emission_manifest.json") or {}
    resolution = _read_optional_json(root / "latest_emission_resolution.json") or {}
    links = _read_optional_json(root / "latest_emission_artifact_links.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_latest_emission_manifest.json",
            "latest_emission_resolution.json",
            "latest_emission_artifact_links.json",
            "smoothed_latest_emission_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "latest_emission_id_matches",
                manifest.get("latest_emission_id") == latest_emission_id
                and resolution.get("latest_emission_id") == latest_emission_id,
                "",
            ),
            _check(
                "fallback_scope_daily_only",
                resolution.get("fallback_scope") == "daily_emission_only",
                _text(resolution.get("fallback_scope")),
            ),
            _check(
                "outcome_update_forbidden",
                resolution.get("outcome_update_allowed") is False,
                "",
            ),
            _check("due_scan_forbidden", resolution.get("due_scan_allowed") is False, ""),
            _check(
                "future_data_used_false",
                resolution.get("future_data_used") is False,
                "",
            ),
            _check(
                "daily_emission_linked",
                bool(_text(links.get("daily_emission_id"))),
                _text(links.get("daily_emission_id")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, resolution, links), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_latest_emission_validation",
        latest_emission_id,
        checks,
    )


def run_smoothed_blocked_explain(
    *,
    preflight_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    preflight = smoothed_data_preflight_report_payload(
        preflight_id=preflight_id,
        output_dir=preflight_dir,
    )
    snapshot = _mapping(preflight.get("data_freshness_snapshot"))
    command_matrix = _mapping(preflight.get("runnable_command_matrix"))
    explanations = _smoothed_blocked_command_explanations(snapshot, command_matrix)
    explain_id = _stable_id(
        "smoothed-blocked-explain",
        preflight_id,
        explanations,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / explain_id)
    root.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "explain_id": root.name,
        "source_preflight_id": preflight_id,
        "blocked_commands": explanations,
        **SYSTEM_TARGET_SAFETY,
    }
    owner_summary = render_smoothed_blocked_owner_summary(snapshot, explanations)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_blocked_explain_manifest",
        "explain_id": root.name,
        "source_preflight_id": preflight_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "blocked_command_count": len(explanations),
        "smoothed_blocked_explain_manifest_path": str(
            root / "smoothed_blocked_explain_manifest.json"
        ),
        "blocked_command_explanations_path": str(root / "blocked_command_explanations.json"),
        "blocked_owner_summary_path": str(root / "blocked_owner_summary.md"),
        "smoothed_blocked_explain_report_path": str(root / "smoothed_blocked_explain_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_blocked_explain_reader_brief(snapshot, explanations)
    _write_json(root / "smoothed_blocked_explain_manifest.json", manifest)
    _write_json(root / "blocked_command_explanations.json", payload)
    _write_text(root / "blocked_owner_summary.md", owner_summary)
    _write_text(
        root / "smoothed_blocked_explain_report.md",
        render_smoothed_blocked_explain_report(manifest, snapshot, explanations),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_blocked_explain",
        root.name,
        root / "smoothed_blocked_explain_manifest.json",
    )
    return {
        "explain_id": root.name,
        "explain_dir": root,
        "manifest": manifest,
        "blocked_command_explanations": payload,
        "blocked_owner_summary": owner_summary,
        "reader_brief_section": reader,
    }


def smoothed_blocked_explain_report_payload(
    *,
    explain_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=explain_id,
        latest_pointer="latest_smoothed_blocked_explain",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_blocked_explain_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_blocked_explain_manifest.json"),
        "blocked_command_explanations": _read_json(root / "blocked_command_explanations.json"),
        "blocked_owner_summary": (root / "blocked_owner_summary.md").read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "explain_dir": str(root),
    }


def validate_smoothed_blocked_explain_artifact(
    *,
    explain_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> dict[str, Any]:
    root = output_dir / explain_id
    manifest = _read_optional_json(root / "smoothed_blocked_explain_manifest.json") or {}
    payload = _read_optional_json(root / "blocked_command_explanations.json") or {}
    commands = _records(payload.get("blocked_commands"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_blocked_explain_manifest.json",
            "blocked_command_explanations.json",
            "blocked_owner_summary.md",
            "smoothed_blocked_explain_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "explain_id_matches",
                manifest.get("explain_id") == explain_id
                and payload.get("explain_id") == explain_id,
                "",
            ),
            _check("blocked_commands_present", bool(commands), str(len(commands))),
            _check(
                "human_explanation_present",
                all(_text(row.get("human_explanation")) for row in commands),
                "",
            ),
            _check(
                "safe_next_action_present",
                all(_text(row.get("safe_next_action")) for row in commands),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, payload), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_blocked_explain_validation",
        explain_id,
        checks,
    )


def run_smoothed_refresh_plan(
    *,
    preflight_id: str,
    explain_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    explain_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    preflight = smoothed_data_preflight_report_payload(
        preflight_id=preflight_id,
        output_dir=preflight_dir,
    )
    explain = smoothed_blocked_explain_report_payload(
        explain_id=explain_id,
        output_dir=explain_dir,
    )
    snapshot = _mapping(preflight.get("data_freshness_snapshot"))
    requirements = _smoothed_source_refresh_requirements(snapshot)
    rerun = _smoothed_rerun_command_plan(snapshot)
    refresh_plan_id = _stable_id(
        "smoothed-refresh-plan",
        preflight_id,
        explain_id,
        requirements,
        rerun,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / refresh_plan_id)
    root.mkdir(parents=True, exist_ok=False)
    requirements["refresh_plan_id"] = root.name
    rerun["refresh_plan_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_refresh_plan_manifest",
        "refresh_plan_id": root.name,
        "source_preflight_id": preflight_id,
        "source_explain_id": explain_id,
        "requested_as_of": snapshot.get("requested_as_of") or snapshot.get("requested_week_ending"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "smoothed_refresh_plan_manifest_path": str(root / "smoothed_refresh_plan_manifest.json"),
        "source_refresh_requirements_path": str(root / "source_refresh_requirements.json"),
        "rerun_command_plan_path": str(root / "rerun_command_plan.json"),
        "smoothed_refresh_plan_report_path": str(root / "smoothed_refresh_plan_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_refresh_plan_reader_brief(requirements, rerun)
    _write_json(root / "smoothed_refresh_plan_manifest.json", manifest)
    _write_json(root / "source_refresh_requirements.json", requirements)
    _write_json(root / "rerun_command_plan.json", rerun)
    _write_text(
        root / "smoothed_refresh_plan_report.md",
        render_smoothed_refresh_plan_report(
            manifest,
            requirements,
            rerun,
            explain,
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_refresh_plan",
        root.name,
        root / "smoothed_refresh_plan_manifest.json",
    )
    return {
        "refresh_plan_id": root.name,
        "refresh_plan_dir": root,
        "manifest": manifest,
        "source_refresh_requirements": requirements,
        "rerun_command_plan": rerun,
        "reader_brief_section": reader,
    }


def smoothed_refresh_plan_report_payload(
    *,
    refresh_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=refresh_plan_id,
        latest_pointer="latest_smoothed_refresh_plan",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_refresh_plan_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_refresh_plan_manifest.json"),
        "source_refresh_requirements": _read_json(root / "source_refresh_requirements.json"),
        "rerun_command_plan": _read_json(root / "rerun_command_plan.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "refresh_plan_dir": str(root),
    }


def validate_smoothed_refresh_plan_artifact(
    *,
    refresh_plan_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / refresh_plan_id
    manifest = _read_optional_json(root / "smoothed_refresh_plan_manifest.json") or {}
    requirements = _read_optional_json(root / "source_refresh_requirements.json") or {}
    rerun = _read_optional_json(root / "rerun_command_plan.json") or {}
    source_rows = _records(requirements.get("source_requirements"))
    rerun_rows = _records(rerun.get("rerun_after_refresh"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_refresh_plan_manifest.json",
            "source_refresh_requirements.json",
            "rerun_command_plan.json",
            "smoothed_refresh_plan_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "refresh_plan_id_matches",
                manifest.get("refresh_plan_id") == refresh_plan_id
                and requirements.get("refresh_plan_id") == refresh_plan_id,
                "",
            ),
            _check("source_requirements_present", bool(source_rows), str(len(source_rows))),
            _check("rerun_commands_present", bool(rerun_rows), str(len(rerun_rows))),
            _check(
                "does_not_refresh_sources",
                rerun.get("external_refresh_executed") is False,
                "",
            ),
            _check(
                "rerun_allowed_boolean",
                isinstance(rerun.get("rerun_allowed_now"), bool),
                _text(rerun.get("rerun_allowed_now")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, requirements, rerun), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_refresh_plan_validation",
        refresh_plan_id,
        checks,
    )


def run_smoothed_bootstrap_retry(
    *,
    requested_as_of: date | None = None,
    requested_week_ending: date | None = None,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    latest_emission_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    renewal_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    weekly_run_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    binding_id: str | None = None,
    switch_plan_id: str | None = None,
    owner_promotion_id: str | None = None,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    requested_date = _smoothed_requested_date(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
    )
    preflight = run_smoothed_data_preflight(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
        output_dir=preflight_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=model_target_dir,
        generated_at=generated,
    )
    snapshot = _mapping(preflight.get("data_freshness_snapshot"))
    preflight_status = _text(snapshot.get("freshness_status"))
    can_run_full = preflight_status in {"READY", "READY_WITH_WARNINGS"}
    can_run_latest_only = preflight_status == "LATEST_AVAILABLE_ONLY"
    steps: list[dict[str, Any]] = [
        {
            "step": "preflight",
            "status": "PASS" if can_run_full or can_run_latest_only else "BLOCKED",
            "artifact_id": preflight["preflight_id"],
        }
    ]
    artifacts: dict[str, Any] = {
        "preflight": {"artifact_id": preflight["preflight_id"]},
    }
    latest_emission: dict[str, Any] | None = None
    weekly: dict[str, Any] | None = None

    if can_run_full:
        weekly = run_smoothed_forward_weekly_run(
            week_ending=requested_date,
            binding_id=binding_id,
            switch_plan_id=switch_plan_id,
            owner_promotion_id=owner_promotion_id,
            model_target_dir=model_target_dir,
            emission_dir=emission_dir,
            due_dir=due_dir,
            update_dir=update_dir,
            classification_dir=classification_dir,
            binding_dir=binding_dir,
            progress_dir=progress_dir,
            dashboard_dir=dashboard_dir,
            monitor_dir=monitor_dir,
            switch_plan_dir=switch_plan_dir,
            recheck_dir=recheck_dir,
            owner_promotion_dir=owner_promotion_dir,
            renewal_dir=renewal_dir,
            output_dir=weekly_run_dir,
            price_cache_path=price_cache_path,
            generated_at=generated,
        )
        for row in _records(_mapping(weekly.get("weekly_run_steps")).get("steps")):
            steps.append(dict(row))
        artifacts["weekly_runner"] = {"artifact_id": weekly["weekly_run_id"]}
        artifacts.update(_mapping(_mapping(weekly.get("weekly_run_artifacts")).get("artifacts")))
    elif can_run_latest_only:
        latest_emission = run_smoothed_latest_emission(
            preflight_id=preflight["preflight_id"],
            preflight_dir=preflight_dir,
            output_dir=latest_emission_dir,
            model_target_dir=model_target_dir,
            emission_dir=emission_dir,
            price_cache_path=price_cache_path,
            generated_at=generated,
        )
        steps.append(
            {
                "step": "latest_emission",
                "status": "PASS",
                "artifact_id": latest_emission["latest_emission_id"],
            }
        )
        artifacts["latest_emission"] = {"artifact_id": latest_emission["latest_emission_id"]}
        _append_retry_skipped_steps(
            steps,
            (
                "outcome_due_scan",
                "outcome_update",
                "forward_classification",
                "progress_update",
                "weekly_dashboard",
                "event_monitor",
                "switch_readiness",
                "owner_renewal",
            ),
            "latest_available_emission_only",
        )
    else:
        _append_retry_skipped_steps(
            steps,
            (
                "daily_emission",
                "outcome_due_scan",
                "outcome_update",
                "forward_classification",
                "progress_update",
                "weekly_dashboard",
                "event_monitor",
                "switch_readiness",
                "owner_renewal",
            ),
            "preflight_blocked",
        )

    retry_id = _stable_id(
        "smoothed-bootstrap-retry",
        requested_as_of.isoformat() if requested_as_of else "",
        requested_week_ending.isoformat() if requested_week_ending else "",
        preflight["preflight_id"],
        steps,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / retry_id)
    root.mkdir(parents=True, exist_ok=False)
    preflight_result = {
        "schema_version": SCHEMA_VERSION,
        "retry_id": root.name,
        "requested_as_of": requested_date.isoformat(),
        "preflight_status": preflight_status,
        "latest_valid_as_of": snapshot.get("latest_valid_as_of"),
        "can_run_full_retry": can_run_full,
        "can_run_latest_available_emission_only": can_run_latest_only,
        "blocking_errors": _texts(snapshot.get("blocking_errors")),
        **SYSTEM_TARGET_SAFETY,
    }
    step_payload = {"schema_version": SCHEMA_VERSION, "steps": steps, **SYSTEM_TARGET_SAFETY}
    artifact_payload = {
        "schema_version": SCHEMA_VERSION,
        "retry_id": root.name,
        "artifacts": artifacts,
        **SYSTEM_TARGET_SAFETY,
    }
    summary = _smoothed_retry_summary(
        retry_id=root.name,
        requested_date=requested_date,
        preflight_status=preflight_status,
        weekly=weekly,
        latest_emission=latest_emission,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_bootstrap_retry_manifest",
        "retry_id": root.name,
        "requested_as_of": requested_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "retry_status": summary.get("retry_status"),
        "smoothed_bootstrap_retry_manifest_path": str(
            root / "smoothed_bootstrap_retry_manifest.json"
        ),
        "retry_preflight_result_path": str(root / "retry_preflight_result.json"),
        "retry_steps_path": str(root / "retry_steps.json"),
        "retry_artifacts_path": str(root / "retry_artifacts.json"),
        "retry_summary_path": str(root / "retry_summary.json"),
        "smoothed_bootstrap_retry_report_path": str(root / "smoothed_bootstrap_retry_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_bootstrap_retry_reader_brief(summary)
    _write_json(root / "smoothed_bootstrap_retry_manifest.json", manifest)
    _write_json(root / "retry_preflight_result.json", preflight_result)
    _write_json(root / "retry_steps.json", step_payload)
    _write_json(root / "retry_artifacts.json", artifact_payload)
    _write_json(root / "retry_summary.json", summary)
    _write_text(
        root / "smoothed_bootstrap_retry_report.md",
        render_smoothed_bootstrap_retry_report(
            manifest,
            preflight_result,
            step_payload,
            summary,
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_bootstrap_retry",
        root.name,
        root / "smoothed_bootstrap_retry_manifest.json",
    )
    return {
        "retry_id": root.name,
        "retry_dir": root,
        "manifest": manifest,
        "retry_preflight_result": preflight_result,
        "retry_steps": step_payload,
        "retry_artifacts": artifact_payload,
        "retry_summary": summary,
        "preflight": preflight,
        "latest_emission": latest_emission,
        "weekly_run": weekly,
        "reader_brief_section": reader,
    }


def smoothed_bootstrap_retry_report_payload(
    *,
    retry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=retry_id,
        latest_pointer="latest_smoothed_bootstrap_retry",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_bootstrap_retry_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_bootstrap_retry_manifest.json"),
        "retry_preflight_result": _read_json(root / "retry_preflight_result.json"),
        "retry_steps": _read_json(root / "retry_steps.json"),
        "retry_artifacts": _read_json(root / "retry_artifacts.json"),
        "retry_summary": _read_json(root / "retry_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "retry_dir": str(root),
    }


def validate_smoothed_bootstrap_retry_artifact(
    *,
    retry_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> dict[str, Any]:
    root = output_dir / retry_id
    manifest = _read_optional_json(root / "smoothed_bootstrap_retry_manifest.json") or {}
    preflight = _read_optional_json(root / "retry_preflight_result.json") or {}
    steps_payload = _read_optional_json(root / "retry_steps.json") or {}
    artifacts = _read_optional_json(root / "retry_artifacts.json") or {}
    summary = _read_optional_json(root / "retry_summary.json") or {}
    steps = _records(steps_payload.get("steps"))
    preflight_status = _text(preflight.get("preflight_status"))
    blocked = preflight_status.startswith("BLOCKED")
    outcome_update_pass = any(
        row.get("step") == "outcome_update" and row.get("status") == "PASS" for row in steps
    )
    checks = _required_file_checks(
        root,
        (
            "smoothed_bootstrap_retry_manifest.json",
            "retry_preflight_result.json",
            "retry_steps.json",
            "retry_artifacts.json",
            "retry_summary.json",
            "smoothed_bootstrap_retry_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "retry_id_matches",
                manifest.get("retry_id") == retry_id
                and preflight.get("retry_id") == retry_id
                and summary.get("retry_id") == retry_id,
                "",
            ),
            _check(
                "preflight_first",
                bool(steps) and steps[0].get("step") == "preflight",
                ",".join(_text(row.get("step")) for row in steps[:2]),
            ),
            _check(
                "blocked_does_not_update_outcome",
                not blocked or not outcome_update_pass,
                preflight_status,
            ),
            _check(
                "retry_status_allowed",
                summary.get("retry_status") in {"COMPLETED", "BLOCKED", "PARTIAL", "FAIL"},
                _text(summary.get("retry_status")),
            ),
            _check(
                "can_execute_switch_false",
                summary.get("can_execute_switch") is False,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                summary.get("broker_action_allowed") is False,
                "",
            ),
            _check(
                "production_effect_none",
                summary.get("production_effect") == "none",
                _text(summary.get("production_effect")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, preflight, artifacts, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_bootstrap_retry_validation",
        retry_id,
        checks,
    )


def run_smoothed_source_refresh(
    *,
    refresh_plan_id: str,
    execute_refresh: bool = False,
    refresh_plan_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    config_path: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    marketstack_cache_path: Path | None = None,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
    refresh_executor: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = _load_smoothed_source_refresh_config(config_path)
    plan = smoothed_refresh_plan_report_payload(
        refresh_plan_id=refresh_plan_id,
        output_dir=refresh_plan_dir,
    )
    requirements = _mapping(plan.get("source_refresh_requirements"))
    requested_date = _coerce_date(
        requirements.get("requested_as_of") or plan.get("requested_as_of"),
        date.min,
    )
    if requested_date == date.min:
        raise DynamicV3SystemTargetError("refresh plan requested_as_of is missing")

    source_rows = _records(requirements.get("source_requirements"))
    source_specs = _smoothed_refresh_source_specs(
        source_rows=source_rows,
        price_cache_path=price_cache_path,
        marketstack_cache_path=marketstack_cache_path
        or _smoothed_marketstack_prices_path(price_cache_path),
        rates_path=rates_path,
    )
    before_states = {
        spec["source"]: _cache_file_audit_state(spec["source"], Path(spec["cache_path"]))
        for spec in source_specs
    }
    refresh_error: str | None = None
    if execute_refresh:
        context = {
            "refresh_plan_id": refresh_plan_id,
            "requested_as_of": requested_date.isoformat(),
            "source_specs": source_specs,
            "config": config,
            "price_cache_path": str(price_cache_path),
            "marketstack_cache_path": str(
                marketstack_cache_path or _smoothed_marketstack_prices_path(price_cache_path)
            ),
            "rates_path": str(rates_path),
        }
        try:
            if refresh_executor is not None:
                refresh_executor(context)
            else:
                _execute_smoothed_project_data_refresh(
                    requested_as_of=requested_date,
                    config=config,
                    output_dir=price_cache_path.parent,
                )
        except Exception as exc:  # pragma: no cover - real provider failures are environment-bound.
            refresh_error = _text(exc)

    after_states = {
        spec["source"]: _cache_file_audit_state(spec["source"], Path(spec["cache_path"]))
        for spec in source_specs
    }
    source_results = [
        _smoothed_source_refresh_result_row(
            spec=spec,
            before=before_states[spec["source"]],
            after=after_states[spec["source"]],
            requested_as_of=requested_date,
            execute_refresh=execute_refresh,
            refresh_error=refresh_error,
        )
        for spec in source_specs
    ]
    refresh_status = _smoothed_source_refresh_status(source_results, execute_refresh)
    refresh_execution_id = _stable_id(
        "smoothed-source-refresh",
        refresh_plan_id,
        requested_date.isoformat(),
        execute_refresh,
        source_results,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / refresh_execution_id)
    root.mkdir(parents=True, exist_ok=False)
    request_payload = {
        "schema_version": SCHEMA_VERSION,
        "refresh_execution_id": root.name,
        "source_refresh_plan_id": refresh_plan_id,
        "requested_as_of": requested_date.isoformat(),
        "execute_refresh": execute_refresh,
        "sources_requested": [spec["source"] for spec in source_specs],
        "dry_run": not execute_refresh,
        "config_path": str(config_path),
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }
    results_payload = {
        "schema_version": SCHEMA_VERSION,
        "refresh_execution_id": root.name,
        "sources": source_results,
        "all_sources_refreshed": refresh_status == "COMPLETED",
        "partial_refresh": refresh_status == "PARTIAL",
        "refresh_status": refresh_status,
        "external_refresh_executed": execute_refresh,
        "refresh_error": refresh_error,
        **SYSTEM_TARGET_SAFETY,
    }
    audit_payload = _smoothed_source_refresh_audit(
        refresh_execution_id=root.name,
        before_states=before_states,
        after_states=after_states,
        source_results=source_results,
        external_refresh_executed=execute_refresh,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_source_refresh_manifest",
        "refresh_execution_id": root.name,
        "source_refresh_plan_id": refresh_plan_id,
        "requested_as_of": requested_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "refresh_status": refresh_status,
        "execute_refresh": execute_refresh,
        "smoothed_source_refresh_manifest_path": str(
            root / "smoothed_source_refresh_manifest.json"
        ),
        "refresh_execution_request_path": str(root / "refresh_execution_request.json"),
        "source_refresh_results_path": str(root / "source_refresh_results.json"),
        "source_refresh_audit_path": str(root / "source_refresh_audit.json"),
        "smoothed_source_refresh_report_path": str(root / "smoothed_source_refresh_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_source_refresh_reader_brief(results_payload)
    _write_json(root / "smoothed_source_refresh_manifest.json", manifest)
    _write_json(root / "refresh_execution_request.json", request_payload)
    _write_json(root / "source_refresh_results.json", results_payload)
    _write_json(root / "source_refresh_audit.json", audit_payload)
    _write_text(
        root / "smoothed_source_refresh_report.md",
        render_smoothed_source_refresh_report(manifest, request_payload, results_payload),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_source_refresh",
        root.name,
        root / "smoothed_source_refresh_manifest.json",
    )
    return {
        "refresh_execution_id": root.name,
        "refresh_execution_dir": root,
        "manifest": manifest,
        "refresh_execution_request": request_payload,
        "source_refresh_results": results_payload,
        "source_refresh_audit": audit_payload,
        "reader_brief_section": reader,
    }


def smoothed_source_refresh_report_payload(
    *,
    refresh_execution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=refresh_execution_id,
        latest_pointer="latest_smoothed_source_refresh",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_source_refresh_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_source_refresh_manifest.json"),
        "refresh_execution_request": _read_json(root / "refresh_execution_request.json"),
        "source_refresh_results": _read_json(root / "source_refresh_results.json"),
        "source_refresh_audit": _read_json(root / "source_refresh_audit.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "refresh_execution_dir": str(root),
    }


def validate_smoothed_source_refresh_artifact(
    *,
    refresh_execution_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> dict[str, Any]:
    root = output_dir / refresh_execution_id
    manifest = _read_optional_json(root / "smoothed_source_refresh_manifest.json") or {}
    request_payload = _read_optional_json(root / "refresh_execution_request.json") or {}
    results = _read_optional_json(root / "source_refresh_results.json") or {}
    audit = _read_optional_json(root / "source_refresh_audit.json") or {}
    source_rows = _records(results.get("sources"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_source_refresh_manifest.json",
            "refresh_execution_request.json",
            "source_refresh_results.json",
            "source_refresh_audit.json",
            "smoothed_source_refresh_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "refresh_execution_id_matches",
                manifest.get("refresh_execution_id") == refresh_execution_id
                and request_payload.get("refresh_execution_id") == refresh_execution_id
                and results.get("refresh_execution_id") == refresh_execution_id,
                "",
            ),
            _check(
                "execute_flag_controls_dry_run",
                (
                    request_payload.get("execute_refresh") is True
                    and request_payload.get("dry_run") is False
                )
                or (
                    request_payload.get("execute_refresh") is False
                    and request_payload.get("dry_run") is True
                    and results.get("refresh_status") == "DRY_RUN_ONLY"
                ),
                _text(results.get("refresh_status")),
            ),
            _check("source_results_present", bool(source_rows), str(len(source_rows))),
            _check(
                "source_status_allowed",
                all(
                    row.get("status") in {"REFRESHED", "SKIPPED", "FAILED", "DRY_RUN_ONLY"}
                    for row in source_rows
                ),
                "",
            ),
            _check(
                "before_after_latest_recorded",
                all(
                    "before_latest_date" in row and "after_latest_date" in row
                    for row in source_rows
                ),
                "",
            ),
            _check(
                "audit_checksums_recorded",
                all(
                    "checksum_before" in row and "checksum_after" in row
                    for row in _records(audit.get("audit_entries"))
                ),
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, request_payload, results, audit),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_source_refresh_validation",
        refresh_execution_id,
        checks,
    )


def run_smoothed_post_refresh_validation(
    *,
    refresh_execution_id: str,
    requested_as_of: date | None = None,
    refresh_execution_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    refresh = smoothed_source_refresh_report_payload(
        refresh_execution_id=refresh_execution_id,
        output_dir=refresh_execution_dir,
    )
    requested_date = requested_as_of or _coerce_date(refresh.get("requested_as_of"), date.min)
    if requested_date == date.min:
        raise DynamicV3SystemTargetError("source refresh requested_as_of is missing")
    preflight = run_smoothed_data_preflight(
        requested_as_of=requested_date,
        output_dir=preflight_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=model_target_dir,
        generated_at=generated,
    )
    snapshot = _mapping(preflight.get("data_freshness_snapshot"))
    post_refresh_id = _stable_id(
        "smoothed-post-refresh",
        refresh_execution_id,
        requested_date.isoformat(),
        snapshot,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / post_refresh_id)
    root.mkdir(parents=True, exist_ok=False)
    data_validation = {
        "schema_version": SCHEMA_VERSION,
        "post_refresh_id": root.name,
        "refresh_execution_id": refresh_execution_id,
        "requested_as_of": requested_date.isoformat(),
        "validate_data_status": snapshot.get("validate_data_status"),
        "errors": _texts(snapshot.get("blocking_errors")),
        "warnings": _texts(snapshot.get("warnings")),
        "latest_available": _mapping(snapshot.get("latest_available")),
        "source_refresh_status": _mapping(refresh.get("source_refresh_results")).get(
            "refresh_status"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    preflight_result = {
        "schema_version": SCHEMA_VERSION,
        "post_refresh_id": root.name,
        "source_preflight_id": preflight["preflight_id"],
        "requested_as_of": requested_date.isoformat(),
        "freshness_status": snapshot.get("freshness_status"),
        "latest_valid_as_of": snapshot.get("latest_valid_as_of"),
        "blocking_errors": _texts(snapshot.get("blocking_errors")),
        "can_run_full_retry": snapshot.get("freshness_status") in {"READY", "READY_WITH_WARNINGS"},
        "can_run_latest_available_emission_only": (
            snapshot.get("freshness_status") == "LATEST_AVAILABLE_ONLY"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    decision = _smoothed_post_refresh_decision(
        post_refresh_id=root.name,
        requested_as_of=requested_date,
        data_validation=data_validation,
        preflight_result=preflight_result,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_post_refresh_validation_manifest",
        "post_refresh_id": root.name,
        "refresh_execution_id": refresh_execution_id,
        "requested_as_of": requested_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "validate_data_status": data_validation.get("validate_data_status"),
        "freshness_status": preflight_result.get("freshness_status"),
        "retry_decision": decision.get("retry_decision"),
        "smoothed_post_refresh_manifest_path": str(root / "smoothed_post_refresh_manifest.json"),
        "post_refresh_data_validation_path": str(root / "post_refresh_data_validation.json"),
        "post_refresh_preflight_result_path": str(root / "post_refresh_preflight_result.json"),
        "post_refresh_decision_path": str(root / "post_refresh_decision.json"),
        "smoothed_post_refresh_report_path": str(root / "smoothed_post_refresh_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_post_refresh_reader_brief(decision, preflight_result)
    _write_json(root / "smoothed_post_refresh_manifest.json", manifest)
    _write_json(root / "post_refresh_data_validation.json", data_validation)
    _write_json(root / "post_refresh_preflight_result.json", preflight_result)
    _write_json(root / "post_refresh_decision.json", decision)
    _write_text(
        root / "smoothed_post_refresh_report.md",
        render_smoothed_post_refresh_report(manifest, data_validation, preflight_result, decision),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_post_refresh_validation",
        root.name,
        root / "smoothed_post_refresh_manifest.json",
    )
    return {
        "post_refresh_id": root.name,
        "post_refresh_dir": root,
        "manifest": manifest,
        "post_refresh_data_validation": data_validation,
        "post_refresh_preflight_result": preflight_result,
        "post_refresh_decision": decision,
        "preflight": preflight,
        "reader_brief_section": reader,
    }


def smoothed_post_refresh_validation_report_payload(
    *,
    post_refresh_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=post_refresh_id,
        latest_pointer="latest_smoothed_post_refresh_validation",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_post_refresh_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_post_refresh_manifest.json"),
        "post_refresh_data_validation": _read_json(root / "post_refresh_data_validation.json"),
        "post_refresh_preflight_result": _read_json(root / "post_refresh_preflight_result.json"),
        "post_refresh_decision": _read_json(root / "post_refresh_decision.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "post_refresh_dir": str(root),
    }


def validate_smoothed_post_refresh_artifact(
    *,
    post_refresh_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> dict[str, Any]:
    root = output_dir / post_refresh_id
    manifest = _read_optional_json(root / "smoothed_post_refresh_manifest.json") or {}
    data_validation = _read_optional_json(root / "post_refresh_data_validation.json") or {}
    preflight = _read_optional_json(root / "post_refresh_preflight_result.json") or {}
    decision = _read_optional_json(root / "post_refresh_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "smoothed_post_refresh_manifest.json",
            "post_refresh_data_validation.json",
            "post_refresh_preflight_result.json",
            "post_refresh_decision.json",
            "smoothed_post_refresh_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "post_refresh_id_matches",
                manifest.get("post_refresh_id") == post_refresh_id
                and data_validation.get("post_refresh_id") == post_refresh_id
                and preflight.get("post_refresh_id") == post_refresh_id
                and decision.get("post_refresh_id") == post_refresh_id,
                "",
            ),
            _check(
                "validate_data_status_allowed",
                data_validation.get("validate_data_status")
                in {"PASS", "PASS_WITH_WARNINGS", "FAIL"},
                _text(data_validation.get("validate_data_status")),
            ),
            _check(
                "freshness_status_allowed",
                preflight.get("freshness_status")
                in {
                    "READY",
                    "READY_WITH_WARNINGS",
                    "BLOCKED_STALE_DATA",
                    "BLOCKED_FUTURE_AS_OF",
                    "BLOCKED_MISSING_PRICE",
                    "BLOCKED_DATA_QUALITY_FAIL",
                    "LATEST_AVAILABLE_ONLY",
                },
                _text(preflight.get("freshness_status")),
            ),
            _check(
                "retry_decision_allowed",
                decision.get("retry_decision")
                in {
                    "RETRY_READY",
                    "STILL_BLOCKED",
                    "PARTIAL_RETRY_ONLY",
                    "MANUAL_REVIEW_REQUIRED",
                },
                _text(decision.get("retry_decision")),
            ),
            _check(
                "retry_ready_requires_full_retry",
                decision.get("retry_decision") != "RETRY_READY"
                or preflight.get("can_run_full_retry") is True,
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, data_validation, preflight, decision),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_post_refresh_validation",
        post_refresh_id,
        checks,
    )


def run_smoothed_retry_resume(
    *,
    post_refresh_id: str,
    post_refresh_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    bootstrap_retry_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    latest_emission_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    renewal_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    weekly_run_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    binding_id: str | None = None,
    switch_plan_id: str | None = None,
    owner_promotion_id: str | None = None,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    post_refresh = smoothed_post_refresh_validation_report_payload(
        post_refresh_id=post_refresh_id,
        output_dir=post_refresh_dir,
    )
    decision = _mapping(post_refresh.get("post_refresh_decision"))
    data_validation = _mapping(post_refresh.get("post_refresh_data_validation"))
    preflight_result = _mapping(post_refresh.get("post_refresh_preflight_result"))
    requested_date = _coerce_date(preflight_result.get("requested_as_of"), date.min)
    if requested_date == date.min:
        raise DynamicV3SystemTargetError("post-refresh requested_as_of is missing")
    before_counts = _latest_smoothed_progress_counts(progress_dir)
    can_resume = (
        decision.get("retry_decision") == "RETRY_READY"
        and data_validation.get("validate_data_status") in {"PASS", "PASS_WITH_WARNINGS"}
        and preflight_result.get("can_run_full_retry") is True
    )
    resume_id = _stable_id(
        "smoothed-retry-resume",
        post_refresh_id,
        requested_date.isoformat(),
        can_resume,
        before_counts,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / resume_id)
    root.mkdir(parents=True, exist_ok=False)
    retry: dict[str, Any] | None = None
    if can_resume:
        retry = run_smoothed_bootstrap_retry(
            requested_as_of=requested_date,
            output_dir=bootstrap_retry_dir,
            preflight_dir=preflight_dir,
            latest_emission_dir=latest_emission_dir,
            model_target_dir=model_target_dir,
            emission_dir=emission_dir,
            due_dir=due_dir,
            update_dir=update_dir,
            classification_dir=classification_dir,
            binding_dir=binding_dir,
            progress_dir=progress_dir,
            dashboard_dir=dashboard_dir,
            monitor_dir=monitor_dir,
            switch_plan_dir=switch_plan_dir,
            recheck_dir=recheck_dir,
            owner_promotion_dir=owner_promotion_dir,
            renewal_dir=renewal_dir,
            weekly_run_dir=weekly_run_dir,
            binding_id=binding_id,
            switch_plan_id=switch_plan_id,
            owner_promotion_id=owner_promotion_id,
            price_cache_path=price_cache_path,
            rates_path=rates_path,
            generated_at=generated,
        )
    precondition = {
        "schema_version": SCHEMA_VERSION,
        "resume_id": root.name,
        "post_refresh_id": post_refresh_id,
        "retry_decision": decision.get("retry_decision"),
        "can_resume": can_resume,
        "blocking_errors": _texts(preflight_result.get("blocking_errors")),
        "required_sources_fresh": preflight_result.get("can_run_full_retry") is True,
        "validate_data_status": data_validation.get("validate_data_status"),
        "available_forward_events_before_resume": before_counts["forward"],
        "available_sideways_events_before_resume": before_counts["sideways"],
        "available_recovery_events_before_resume": before_counts["recovery"],
        **SYSTEM_TARGET_SAFETY,
    }
    steps = _smoothed_retry_resume_steps(can_resume=can_resume, retry=retry)
    artifacts = _smoothed_retry_resume_artifacts(retry)
    summary = _smoothed_retry_resume_summary(
        resume_id=root.name,
        requested_as_of=requested_date,
        can_resume=can_resume,
        retry=retry,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_retry_resume_manifest",
        "resume_id": root.name,
        "post_refresh_id": post_refresh_id,
        "requested_as_of": requested_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "resume_status": summary.get("resume_status"),
        "smoothed_retry_resume_manifest_path": str(root / "smoothed_retry_resume_manifest.json"),
        "resume_precondition_check_path": str(root / "resume_precondition_check.json"),
        "resume_steps_path": str(root / "resume_steps.json"),
        "resume_artifacts_path": str(root / "resume_artifacts.json"),
        "resume_summary_path": str(root / "resume_summary.json"),
        "smoothed_retry_resume_report_path": str(root / "smoothed_retry_resume_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_retry_resume_reader_brief(summary)
    _write_json(root / "smoothed_retry_resume_manifest.json", manifest)
    _write_json(root / "resume_precondition_check.json", precondition)
    _write_json(root / "resume_steps.json", steps)
    _write_json(root / "resume_artifacts.json", artifacts)
    _write_json(root / "resume_summary.json", summary)
    _write_text(
        root / "smoothed_retry_resume_report.md",
        render_smoothed_retry_resume_report(manifest, precondition, steps, summary),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_retry_resume",
        root.name,
        root / "smoothed_retry_resume_manifest.json",
    )
    return {
        "resume_id": root.name,
        "resume_dir": root,
        "manifest": manifest,
        "resume_precondition_check": precondition,
        "resume_steps": steps,
        "resume_artifacts": artifacts,
        "resume_summary": summary,
        "bootstrap_retry": retry,
        "reader_brief_section": reader,
    }


def smoothed_retry_resume_report_payload(
    *,
    resume_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=resume_id,
        latest_pointer="latest_smoothed_retry_resume",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_retry_resume_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_retry_resume_manifest.json"),
        "resume_precondition_check": _read_json(root / "resume_precondition_check.json"),
        "resume_steps": _read_json(root / "resume_steps.json"),
        "resume_artifacts": _read_json(root / "resume_artifacts.json"),
        "resume_summary": _read_json(root / "resume_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "resume_dir": str(root),
    }


def validate_smoothed_retry_resume_artifact(
    *,
    resume_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> dict[str, Any]:
    root = output_dir / resume_id
    manifest = _read_optional_json(root / "smoothed_retry_resume_manifest.json") or {}
    precondition = _read_optional_json(root / "resume_precondition_check.json") or {}
    steps_payload = _read_optional_json(root / "resume_steps.json") or {}
    artifacts = _read_optional_json(root / "resume_artifacts.json") or {}
    summary = _read_optional_json(root / "resume_summary.json") or {}
    steps = _records(steps_payload.get("steps"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_retry_resume_manifest.json",
            "resume_precondition_check.json",
            "resume_steps.json",
            "resume_artifacts.json",
            "resume_summary.json",
            "smoothed_retry_resume_report.md",
            "reader_brief_section.md",
        ),
    )
    required = {
        "smoothed_bootstrap_retry",
        "smoothed_forward_progress_update",
        "smoothed_weekly_dashboard",
        "smoothed_event_monitor",
        "smoothed_switch_readiness",
        "smoothed_owner_renewal",
    }
    present = {_text(row.get("step")) for row in steps}
    checks.extend(
        [
            _check(
                "resume_id_matches",
                manifest.get("resume_id") == resume_id
                and precondition.get("resume_id") == resume_id
                and summary.get("resume_id") == resume_id,
                "",
            ),
            _check("required_steps_present", required.issubset(present), ",".join(sorted(present))),
            _check(
                "blocked_when_not_retry_ready",
                precondition.get("can_resume") is True or summary.get("resume_status") == "BLOCKED",
                _text(summary.get("resume_status")),
            ),
            _check(
                "resume_status_allowed",
                summary.get("resume_status") in {"COMPLETED", "BLOCKED", "PARTIAL", "FAIL"},
                _text(summary.get("resume_status")),
            ),
            _check(
                "can_execute_switch_false",
                summary.get("can_execute_switch") is False,
                "",
            ),
            _check("artifacts_payload_present", isinstance(artifacts, dict), ""),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, precondition, steps_payload, artifacts, summary),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_retry_resume_validation",
        resume_id,
        checks,
    )


def build_smoothed_sample_growth(
    *,
    resume_id: str,
    resume_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    resume = smoothed_retry_resume_report_payload(resume_id=resume_id, output_dir=resume_dir)
    precondition = _mapping(resume.get("resume_precondition_check"))
    summary = _mapping(resume.get("resume_summary"))
    growth_id = _stable_id(
        "smoothed-sample-growth",
        resume_id,
        precondition,
        summary,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / growth_id)
    root.mkdir(parents=True, exist_ok=False)
    growth_summary = _smoothed_sample_growth_summary(
        growth_id=root.name,
        resume_id=resume_id,
        precondition=precondition,
        resume_summary=summary,
    )
    by_target = _smoothed_sample_growth_by_target(growth_summary)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_sample_growth_manifest",
        "growth_id": root.name,
        "resume_id": resume_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "growth_status": growth_summary.get("growth_status"),
        "smoothed_sample_growth_manifest_path": str(root / "smoothed_sample_growth_manifest.json"),
        "sample_growth_summary_path": str(root / "sample_growth_summary.json"),
        "sample_growth_by_target_path": str(root / "sample_growth_by_target.json"),
        "sample_growth_dashboard_report_path": str(root / "sample_growth_dashboard_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_sample_growth_reader_brief(growth_summary)
    _write_json(root / "smoothed_sample_growth_manifest.json", manifest)
    _write_json(root / "sample_growth_summary.json", growth_summary)
    _write_json(root / "sample_growth_by_target.json", by_target)
    _write_text(
        root / "sample_growth_dashboard_report.md",
        render_smoothed_sample_growth_report(manifest, growth_summary, by_target),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_sample_growth",
        root.name,
        root / "smoothed_sample_growth_manifest.json",
    )
    return {
        "growth_id": root.name,
        "growth_dir": root,
        "manifest": manifest,
        "sample_growth_summary": growth_summary,
        "sample_growth_by_target": by_target,
        "reader_brief_section": reader,
    }


def smoothed_sample_growth_report_payload(
    *,
    growth_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=growth_id,
        latest_pointer="latest_smoothed_sample_growth",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_sample_growth_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_sample_growth_manifest.json"),
        "sample_growth_summary": _read_json(root / "sample_growth_summary.json"),
        "sample_growth_by_target": _read_json(root / "sample_growth_by_target.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "growth_dir": str(root),
    }


def validate_smoothed_sample_growth_artifact(
    *,
    growth_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> dict[str, Any]:
    root = output_dir / growth_id
    manifest = _read_optional_json(root / "smoothed_sample_growth_manifest.json") or {}
    summary = _read_optional_json(root / "sample_growth_summary.json") or {}
    by_target = _read_optional_json(root / "sample_growth_by_target.json") or {}
    targets = _records(by_target.get("targets"))
    checks = _required_file_checks(
        root,
        (
            "smoothed_sample_growth_manifest.json",
            "sample_growth_summary.json",
            "sample_growth_by_target.json",
            "sample_growth_dashboard_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "growth_id_matches",
                manifest.get("growth_id") == growth_id and summary.get("growth_id") == growth_id,
                "",
            ),
            _check(
                "growth_status_allowed",
                summary.get("growth_status")
                in {"IMPROVED", "NO_CHANGE", "PARTIAL", "INSUFFICIENT_DATA"},
                _text(summary.get("growth_status")),
            ),
            _check("targets_present", len(targets) >= 3, str(len(targets))),
            _check(
                "delta_consistent",
                _smoothed_sample_growth_delta_consistent(summary),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, by_target), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_sample_growth_validation",
        growth_id,
        checks,
    )


def pack_smoothed_data_readiness(
    *,
    refresh_execution_id: str,
    post_refresh_id: str,
    resume_id: str,
    growth_id: str,
    refresh_execution_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    post_refresh_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    resume_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    growth_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    refresh = smoothed_source_refresh_report_payload(
        refresh_execution_id=refresh_execution_id,
        output_dir=refresh_execution_dir,
    )
    post_refresh = smoothed_post_refresh_validation_report_payload(
        post_refresh_id=post_refresh_id,
        output_dir=post_refresh_dir,
    )
    resume = smoothed_retry_resume_report_payload(resume_id=resume_id, output_dir=resume_dir)
    growth = smoothed_sample_growth_report_payload(growth_id=growth_id, output_dir=growth_dir)
    readiness_id = _stable_id(
        "smoothed-data-readiness",
        refresh_execution_id,
        post_refresh_id,
        resume_id,
        growth_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / readiness_id)
    root.mkdir(parents=True, exist_ok=False)
    summary = _smoothed_data_readiness_summary(
        readiness_id=root.name,
        refresh=refresh,
        post_refresh=post_refresh,
        resume=resume,
        growth=growth,
    )
    checklist = render_smoothed_data_readiness_checklist(summary)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_smoothed_data_readiness_manifest",
        "readiness_id": root.name,
        "refresh_execution_id": refresh_execution_id,
        "post_refresh_id": post_refresh_id,
        "resume_id": resume_id,
        "growth_id": growth_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "current_status": summary.get("current_status"),
        "recommended_owner_action": summary.get("recommended_owner_action"),
        "smoothed_data_readiness_manifest_path": str(
            root / "smoothed_data_readiness_manifest.json"
        ),
        "owner_data_readiness_summary_path": str(root / "owner_data_readiness_summary.json"),
        "owner_data_readiness_checklist_path": str(root / "owner_data_readiness_checklist.md"),
        "smoothed_data_readiness_report_path": str(root / "smoothed_data_readiness_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    reader = render_smoothed_data_readiness_reader_brief(summary)
    _write_json(root / "smoothed_data_readiness_manifest.json", manifest)
    _write_json(root / "owner_data_readiness_summary.json", summary)
    _write_text(root / "owner_data_readiness_checklist.md", checklist)
    _write_text(
        root / "smoothed_data_readiness_report.md",
        render_smoothed_data_readiness_report(manifest, summary, checklist),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_smoothed_data_readiness",
        root.name,
        root / "smoothed_data_readiness_manifest.json",
    )
    return {
        "readiness_id": root.name,
        "readiness_dir": root,
        "manifest": manifest,
        "owner_data_readiness_summary": summary,
        "owner_data_readiness_checklist": checklist,
        "reader_brief_section": reader,
    }


def smoothed_data_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=readiness_id,
        latest_pointer="latest_smoothed_data_readiness",
        latest=latest,
        output_dir=output_dir,
        required_name="smoothed_data_readiness_manifest.json",
    )
    return {
        **_read_json(root / "smoothed_data_readiness_manifest.json"),
        "owner_data_readiness_summary": _read_json(root / "owner_data_readiness_summary.json"),
        "owner_data_readiness_checklist": (root / "owner_data_readiness_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "readiness_dir": str(root),
    }


def validate_smoothed_data_readiness_artifact(
    *,
    readiness_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> dict[str, Any]:
    root = output_dir / readiness_id
    manifest = _read_optional_json(root / "smoothed_data_readiness_manifest.json") or {}
    summary = _read_optional_json(root / "owner_data_readiness_summary.json") or {}
    checklist = (
        (root / "owner_data_readiness_checklist.md").read_text(encoding="utf-8")
        if (root / "owner_data_readiness_checklist.md").exists()
        else ""
    )
    checks = _required_file_checks(
        root,
        (
            "smoothed_data_readiness_manifest.json",
            "owner_data_readiness_summary.json",
            "owner_data_readiness_checklist.md",
            "smoothed_data_readiness_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "readiness_id_matches",
                manifest.get("readiness_id") == readiness_id
                and summary.get("readiness_id") == readiness_id,
                "",
            ),
            _check(
                "current_status_allowed",
                summary.get("current_status")
                in {
                    "WAIT_FOR_REFRESH",
                    "REFRESH_REQUIRED",
                    "REFRESH_EXECUTED",
                    "RETRY_READY",
                    "RETRY_BLOCKED",
                    "RETRY_COMPLETED",
                    "CONTINUE_OBSERVATION",
                },
                _text(summary.get("current_status")),
            ),
            _check(
                "recommended_owner_action_allowed",
                summary.get("recommended_owner_action")
                in {
                    "wait_for_refresh",
                    "run_refresh",
                    "rerun_retry",
                    "continue_observation",
                    "manual_review_required",
                },
                _text(summary.get("recommended_owner_action")),
            ),
            _check("checklist_mentions_no_broker", "no broker" in checklist.lower(), ""),
            _check(
                "broker_action_allowed_false",
                summary.get("broker_action_allowed") is False,
                "",
            ),
            _check(
                "production_effect_none",
                summary.get("production_effect") == "none",
                _text(summary.get("production_effect")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_data_readiness_validation",
        readiness_id,
        checks,
    )


def render_risk_capped_limited_config_report(
    manifest: Mapping[str, Any],
    config: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> str:
    caps = _mapping(config.get("caps"))
    contextual = _mapping(config.get("contextual_caps"))
    reallocation = _mapping(config.get("reallocation"))
    return "\n".join(
        [
            f"# Risk-Capped Limited Config {manifest.get('config_validation_id')}",
            "",
            f"- base_method: {_mapping(config.get('method')).get('base_method')}",
            "- target_method: risk_capped_limited_adjustment",
            f"- validation_status: {validation.get('status')}",
            f"- max_total_risk_asset_weight: {caps.get('max_total_risk_asset_weight')}",
            f"- max_semiconductor_weight: {caps.get('max_semiconductor_weight')}",
            f"- max_single_symbol_weight: {caps.get('max_single_symbol_weight')}",
            f"- min_cash_weight: {caps.get('min_cash_weight')}",
            f"- contextual_caps: {', '.join(sorted(contextual))}",
            f"- excess_weight_destination: "
            f"{', '.join(_texts(reallocation.get('excess_weight_destination')))}",
            f"- fallback_destination: {reallocation.get('fallback_destination')}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- paper_shadow_only: true",
            "- broker_action_allowed: false",
            "- official_target_weights_written: false",
            "- production_effect: none",
            "",
            "该配置只定义 research-only risk cap policy。它不会触发 broker，也不会写入 "
            "official target weights。",
            "",
        ]
    )


def render_risk_capped_limited_report(
    manifest: Mapping[str, Any],
    target_row: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Risk-Capped Limited Target {manifest.get('risk_capped_id')}",
            "",
            f"- as_of: {manifest.get('as_of')}",
            f"- base_method: {manifest.get('base_method')}",
            f"- target_method: {manifest.get('target_method')}",
            f"- regime_context: {target_row.get('regime_context')}",
            f"- cap_status: {summary.get('cap_status')}",
            f"- cap_event_count: {summary.get('total_cap_events')}",
            f"- active_caps: {', '.join(_texts(target_row.get('active_caps')))}",
            f"- total_reallocated_to_cash: {summary.get('total_reallocated_to_cash')}",
            f"- total_reallocated_to_defensive: {summary.get('total_reallocated_to_defensive')}",
            "- weights_sum_preserved: true",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "报告回答：触发 cap 的日期在 `cap_events.jsonl` 中；主要 cap 类型在 "
            "`cap_reason_summary.json` 中；半导体暴露和 cash buffer 变化可由 "
            "`base_weights` 与 `capped_weights` 对比；本链路不会触发 broker。",
            "",
        ]
    )


def render_risk_capped_backfill_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Risk-Capped Paper Shadow Backfill {manifest.get('risk_capped_backfill_id')}",
            "",
            f"- method: {summary.get('method')}",
            f"- date_range: {summary.get('date_start')} to {summary.get('date_end')}",
            f"- rebalance_count: {summary.get('rebalance_count')}",
            f"- cap_event_count: {summary.get('cap_event_count')}",
            f"- total_turnover: {summary.get('total_turnover')}",
            f"- avg_semiconductor_weight: {summary.get('avg_semiconductor_weight')}",
            f"- max_semiconductor_weight: {summary.get('max_semiconductor_weight')}",
            f"- avg_cash_weight: {summary.get('avg_cash_weight')}",
            f"- min_cash_weight: {summary.get('min_cash_weight')}",
            f"- data_quality: {summary.get('data_quality')}",
            "- broker_action_taken: false",
            "- not_official_target_weights: true",
            "- production_effect: none",
            "",
            "该 backfill 覆盖 ai_after_chatgpt regime，用于评估 risk-capped method 的历史 "
            "research behavior，不是 production backtest 或交易指令。",
            "",
        ]
    )


def render_risk_capped_comparison_report(
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
    regime: Mapping[str, Any],
    rolling: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> str:
    values = _mapping(metrics.get("metrics"))
    sideways = next(
        (row for row in _records(regime.get("regimes")) if row.get("regime") == "sideways_choppy"),
        {},
    )
    return "\n".join(
        [
            f"# Risk-Capped Comparison {manifest.get('comparison_id')}",
            "",
            f"- methods: {metrics.get('comparison')}",
            f"- return_delta_vs_limited: {values.get('total_return_delta')}",
            f"- annualized_return_delta_vs_limited: {values.get('annualized_return_delta')}",
            f"- max_drawdown_delta_vs_limited: {values.get('max_drawdown_delta')}",
            f"- realized_volatility_delta_vs_limited: {values.get('realized_volatility_delta')}",
            f"- turnover_delta_vs_limited: {values.get('turnover_delta')}",
            f"- avg_semiconductor_weight_delta: {values.get('avg_semiconductor_weight_delta')}",
            f"- max_semiconductor_weight_delta: {values.get('max_semiconductor_weight_delta')}",
            f"- avg_cash_weight_delta: {values.get('avg_cash_weight_delta')}",
            f"- sideways_choppy_conclusion: {sideways.get('conclusion', 'MISSING')}",
            f"- rolling_stability_delta: {rolling.get('stability_delta')}",
            f"- stability_conclusion: {stability.get('stability_conclusion')}",
            f"- conclusion: {metrics.get('conclusion')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "结论只用于 research method review。即使 risk-capped 优于 limited_adjustment，"
            "仍不代表 official target weights 或 broker action 获批。",
            "",
        ]
    )


def render_risk_capped_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Risk-Capped Checklist {decision.get('review_id', '')}",
            "",
            "- [ ] 是否接受 risk_capped_limited_adjustment 作为新的 primary research method？",
            "- [ ] 是否接受 limited_adjustment 降为 secondary research method？",
            "- [ ] 是否接受 consensus_target 继续作为 reference-only？",
            "- [ ] 是否接受 defensive_limited_adjustment 继续 research-only？",
            "- [ ] 是否要求 forward confirmation 后再进行下一步？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            "- requires_forward_confirmation: true",
            "",
        ]
    )


def render_risk_capped_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    comparison: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> str:
    improvements = _mapping(decision.get("improvements_vs_limited"))
    summary = _mapping(backfill.get("risk_capped_backfill_summary"))
    metrics = _mapping(_mapping(comparison.get("risk_capped_vs_limited_metrics")).get("metrics"))
    return "\n".join(
        [
            f"# Risk-Capped Research Method Review {manifest.get('review_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- base_method: {decision.get('base_method')}",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- max_drawdown: {improvements.get('max_drawdown')}",
            f"- rolling_consistency: {improvements.get('rolling_consistency')}",
            f"- semiconductor_exposure: {improvements.get('semiconductor_exposure')}",
            f"- return_preservation: {improvements.get('return_preservation')}",
            f"- return_delta_vs_limited: {metrics.get('total_return_delta')}",
            f"- drawdown_delta_vs_limited: {metrics.get('max_drawdown_delta')}",
            f"- avg_semiconductor_weight: {summary.get('avg_semiconductor_weight')}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "- requires_forward_confirmation: true",
            "",
            "该 review pack 判断 risk_capped_limited_adjustment 是否解决 "
            "limited_adjustment 的 semiconductor exposure、drawdown 和 rolling consistency "
            "问题。结论仍需 owner review 和 forward confirmation，不能自动成为 production "
            "target weights。",
            "",
        ]
    )


def render_risk_capped_review_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Risk-Capped Research Method Review",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- improvements_vs_limited: {decision.get('improvements_vs_limited')}",
            "- research_target_only: true",
            "- requires_forward_confirmation: true",
            f"- next_action: {decision.get('next_action')}",
            "",
        ]
    )


def render_smoothed_limited_config_report(
    manifest: Mapping[str, Any],
    config: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> str:
    variants = _mapping(config.get("variants"))
    context = _mapping(config.get("regime_context"))
    return "\n".join(
        [
            f"# Smoothed Limited Config {manifest.get('config_validation_id')}",
            "",
            f"- base_method: {_mapping(config.get('method')).get('base_method')}",
            "- target_methods: smooth_weights_3d_limited_adjustment, "
            "smooth_weights_5d_limited_adjustment",
            f"- validation_status: {validation.get('status')}",
            f"- enabled_variants: {', '.join(_enabled_smoothed_variants(config))}",
            f"- smooth_weights_3d: {_mapping(variants.get('smooth_weights_3d'))}",
            f"- smooth_weights_5d: {_mapping(variants.get('smooth_weights_5d'))}",
            f"- sideways_choppy: {_mapping(context.get('sideways_choppy'))}",
            f"- strong_recovery: {_mapping(context.get('strong_recovery'))}",
            "- official_target_weights_written: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "该配置只定义 limited_adjustment weight path 的 research-only smoothing policy。"
            "3d 是 primary candidate，5d 是 lag-risk sensitivity method；二者都不会触发 "
            "broker 或 official target weights。",
            "",
        ]
    )


def render_smoothed_limited_report(
    manifest: Mapping[str, Any],
    target_rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    lag_events: Sequence[Mapping[str, Any]],
) -> str:
    row_by_method = {row.get("target_method"): row for row in target_rows}
    summary_rows = _records(summary.get("target_methods"))
    return "\n".join(
        [
            f"# Smoothed Limited Target {manifest.get('smoothed_id')}",
            "",
            f"- as_of: {manifest.get('as_of')}",
            "- base_method: limited_adjustment",
            f"- target_methods: {', '.join(_texts(manifest.get('target_methods')))}",
            f"- smooth_weights_3d_weights: "
            f"{_mapping(row_by_method.get('smooth_weights_3d_limited_adjustment')).get('smoothed_weights')}",
            f"- smooth_weights_5d_weights: "
            f"{_mapping(row_by_method.get('smooth_weights_5d_limited_adjustment')).get('smoothed_weights')}",
            f"- smoothing_event_count: "
            f"{sum(int(_float(row.get('smoothing_event_count'))) for row in summary_rows)}",
            f"- lag_event_count: {len(lag_events)}",
            f"- jump_reduction_summary: {summary_rows}",
            "- weights_sum_preserved: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "报告回答：3d / 5d 的 smoothed weights 在 `smoothed_target_weights.jsonl`；"
            "权重跳变减少在 `weight_jump_reduction_summary.json`；lag diagnostics 在 "
            "`lag_events.jsonl`；本链路不会触发 broker。",
            "",
        ]
    )


def render_smoothed_backfill_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    methods = _records(summary.get("methods"))
    return "\n".join(
        [
            f"# Smoothed Paper Shadow Backfill {manifest.get('smoothed_backfill_id')}",
            "",
            f"- date_range: {summary.get('date_start')} to {summary.get('date_end')}",
            f"- data_quality: {summary.get('data_quality')}",
            f"- smoothing_event_count: {summary.get('smoothing_event_count')}",
            f"- lag_event_count: {summary.get('lag_event_count')}",
            f"- methods: {methods}",
            "- broker_action_taken: false",
            "- not_official_target_weights: true",
            "- production_effect: none",
            "",
            "该 backfill 覆盖 ai_after_chatgpt regime，用于评估 smoothed limited method "
            "的 historical research behavior，不是 production backtest 或交易指令。",
            "",
        ]
    )


def render_smoothed_comparison_report(
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
    regime: Mapping[str, Any],
    rolling: Mapping[str, Any],
    stability: Mapping[str, Any],
    lag_cost: Mapping[str, Any],
) -> str:
    primary = _find_comparison(
        _records(metrics.get("comparisons")),
        "smooth_weights_3d_limited_adjustment",
        "limited_adjustment",
    )
    stability_primary = _find_method(
        _records(stability.get("methods")), "smooth_weights_3d_limited_adjustment"
    )
    lag_primary = _find_method(
        _records(lag_cost.get("methods")), "smooth_weights_3d_limited_adjustment"
    )
    rolling_primary = _find_method(
        _records(rolling.get("methods")), "smooth_weights_3d_limited_adjustment"
    )
    sideways = next(
        (row for row in _records(regime.get("regimes")) if row.get("regime") == "sideways_choppy"),
        {},
    )
    return "\n".join(
        [
            f"# Smoothed Comparison {manifest.get('comparison_id')}",
            "",
            f"- smooth_3d_return_delta_vs_limited: {primary.get('total_return_delta')}",
            f"- smooth_3d_drawdown_delta_vs_limited: {primary.get('max_drawdown_delta')}",
            f"- smooth_3d_turnover_delta_vs_limited: {primary.get('turnover_delta')}",
            f"- smooth_3d_rolling_consistency_delta: "
            f"{rolling_primary.get('rolling_consistency_delta')}",
            f"- smooth_3d_stability_conclusion: {stability_primary.get('stability_conclusion')}",
            f"- smooth_3d_lag_cost_status: {lag_primary.get('lag_cost_status')}",
            f"- sideways_choppy_conclusion: {sideways.get('smooth_3d_conclusion')}",
            f"- conclusion: {primary.get('conclusion')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "结论只用于 research method review。即使 smoothed method 优于 "
            "limited_adjustment，仍不代表 official target weights 或 broker action 获批。",
            "",
        ]
    )


def render_smoothed_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Smoothed Checklist {decision.get('review_id', '')}",
            "",
            "- [ ] 是否接受 smooth_weights_3d_limited_adjustment 作为 recommended method？",
            "- [ ] 是否接受 smooth_weights_5d 作为 secondary / sensitivity method？",
            "- [ ] 是否接受 limited_adjustment 降为 baseline / reference method？",
            "- [ ] 是否确认 smoothed method 仍需 forward confirmation？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            "- requires_forward_confirmation: true",
            "",
        ]
    )


def render_smoothed_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    comparison: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> str:
    improvements = _mapping(decision.get("improvements_vs_limited"))
    summary = _mapping(backfill.get("smoothed_backfill_summary"))
    primary = _find_comparison(
        _records(_mapping(comparison.get("smoothed_vs_limited_metrics")).get("comparisons")),
        "smooth_weights_3d_limited_adjustment",
        "limited_adjustment",
    )
    return "\n".join(
        [
            f"# Smoothed Research Method Review {manifest.get('review_id')}",
            "",
            f"- recommended_method: {decision.get('recommended_method')}",
            f"- secondary_method: {decision.get('secondary_method')}",
            f"- base_method: {decision.get('base_method')}",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- rolling_consistency: {improvements.get('rolling_consistency')}",
            f"- turnover: {improvements.get('turnover')}",
            f"- weight_jumps: {improvements.get('weight_jumps')}",
            f"- return_preservation: {improvements.get('return_preservation')}",
            f"- drawdown: {improvements.get('drawdown')}",
            f"- lag_risk: {decision.get('lag_risk')}",
            f"- return_delta_vs_limited: {primary.get('total_return_delta')}",
            f"- drawdown_delta_vs_limited: {primary.get('max_drawdown_delta')}",
            f"- smoothing_event_count: {summary.get('smoothing_event_count')}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "- requires_forward_confirmation: true",
            "",
            "该 review pack 判断 smooth_weights_3d_limited_adjustment 是否解决 "
            "limited_adjustment 的 rolling instability、signal churn 和 weight jump 问题。"
            "结论仍需 owner review 和 forward confirmation，不能自动成为 production target "
            "weights。",
            "",
        ]
    )


def render_smoothed_review_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Research Method Review",
            "",
            f"- recommended_method: {decision.get('recommended_method')}",
            f"- secondary_method: {decision.get('secondary_method')}",
            f"- decision: {decision.get('decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- improvements_vs_limited: {decision.get('improvements_vs_limited')}",
            f"- lag_risk: {decision.get('lag_risk')}",
            "- research_target_only: true",
            "- requires_forward_confirmation: true",
            f"- next_action: {decision.get('next_action')}",
            "",
        ]
    )


def render_smoothed_review_attribution_report(
    manifest: Mapping[str, Any],
    breakdown: Mapping[str, Any],
    support_matrix: Mapping[str, Any],
) -> str:
    supporting = _records(breakdown.get("supporting_reasons"))
    blocking = _records(breakdown.get("blocking_reasons"))
    matrix = _records(support_matrix.get("metrics"))
    return "\n".join(
        [
            f"# Smoothed Review Attribution {manifest.get('attribution_id')}",
            "",
            f"- review_id: {manifest.get('review_id')}",
            f"- decision: {breakdown.get('decision')}",
            f"- confidence: {breakdown.get('confidence')}",
            f"- recommended_method: {breakdown.get('recommended_method')}",
            f"- secondary_method: {breakdown.get('secondary_method')}",
            f"- supporting_reasons: {supporting}",
            f"- blocking_reasons: {blocking}",
            f"- metric_support_matrix: {matrix}",
            f"- why_not_promote: {breakdown.get('why_not_promote')}",
            f"- why_not_reject: {breakdown.get('why_not_reject')}",
            f"- next_required_evidence: {breakdown.get('next_required_evidence')}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "CONTINUE_OBSERVATION 的主因是 forward confirmation 尚未成熟、confidence 仍为 "
            "LOW，并且 lag cost 仍需要在 forward strong_recovery / fast regime change 中观察。",
            "当前没有直接 REJECT 理由，因为 3d 相对 limited_adjustment 仍保留收益和 turnover "
            "改善证据；也没有直接 PROMOTE 理由，因为证据主要来自 backtest / paper-shadow "
            "historical evidence。",
            "",
        ]
    )


def render_smoothing_benefit_lag_report(
    manifest: Mapping[str, Any],
    benefit: Mapping[str, Any],
    lag: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
) -> str:
    primary_benefit = _find_method(
        _records(benefit.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    primary_lag = _find_method(
        _records(lag.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    primary_tradeoff = _find_method(
        _records(tradeoff.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    secondary_tradeoff = _find_method(
        _records(tradeoff.get("methods")),
        "smooth_weights_5d_limited_adjustment",
    )
    return "\n".join(
        [
            f"# Smoothing Benefit Lag Drilldown {manifest.get('drilldown_id')}",
            "",
            f"- smooth_3d_weight_jump_reduction: "
            f"{primary_benefit.get('weight_jump_reduction')}",
            f"- smooth_3d_turnover_reduction: {primary_benefit.get('turnover_reduction')}",
            f"- smooth_3d_signal_churn_reduction: "
            f"{primary_benefit.get('signal_churn_reduction')}",
            f"- smooth_3d_rolling_consistency_delta: "
            f"{primary_benefit.get('rolling_consistency_delta')}",
            f"- smooth_3d_benefit_status: {primary_benefit.get('benefit_status')}",
            f"- smooth_3d_lag_cost_status: {primary_lag.get('lag_cost_status')}",
            f"- smooth_3d_tradeoff_status: {primary_tradeoff.get('tradeoff_status')}",
            f"- smooth_5d_tradeoff_status: {secondary_tradeoff.get('tradeoff_status')}",
            "- primary_candidate: smooth_weights_3d_limited_adjustment",
            "- research_target_only: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "benefit 主要看 weight jump、turnover、signal churn 和 rolling consistency；cost "
            "主要看 strong_recovery / fast regime change 的 missed upside 和 delayed risk-on。"
            "即使 tradeoff 有利，建议仍是 forward confirmation，而不是 automatic promotion。",
            "",
        ]
    )


def render_smoothed_regime_validation_report(
    manifest: Mapping[str, Any],
    sideways: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> str:
    sideways_3d = _find_method(
        _records(sideways.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    recovery_3d = _find_method(
        _records(recovery.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    recovery_5d = _find_method(
        _records(recovery.get("methods")),
        "smooth_weights_5d_limited_adjustment",
    )
    return "\n".join(
        [
            f"# Smoothed Regime Validation {manifest.get('regime_validation_id')}",
            "",
            f"- sideways_status: {sideways_3d.get('sideways_status')}",
            f"- sideways_sample_count: {sideways_3d.get('sample_count')}",
            f"- sideways_turnover_delta_vs_limited: "
            f"{sideways_3d.get('turnover_delta_vs_limited')}",
            f"- sideways_signal_churn_delta_vs_limited: "
            f"{sideways_3d.get('signal_churn_delta_vs_limited')}",
            f"- sideways_weight_jump_delta_vs_limited: "
            f"{sideways_3d.get('weight_jump_delta_vs_limited')}",
            f"- smooth_3d_recovery_lag_status: {recovery_3d.get('lag_status')}",
            f"- smooth_3d_missed_upside: {recovery_3d.get('missed_upside')}",
            f"- smooth_5d_recovery_lag_status: {recovery_5d.get('lag_status')}",
            "- requires_forward_confirmation: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "sideways_choppy validation 用于判断 smoothing 是否降低 churn、turnover 和 weight "
            "jump；strong_recovery validation 用于披露 smoothing 是否拖慢 risk-on response。"
            "本报告不降级或晋级任何 method，只提供 owner review 和 weekly watch evidence。",
            "",
        ]
    )


def render_smoothed_confirmation_report(
    manifest: Mapping[str, Any],
    targets: Mapping[str, Any],
) -> str:
    target_rows = _records(targets.get("targets"))
    watch_only = [row.get("target_id") for row in target_rows if row.get("status") == "WATCH_ONLY"]
    return "\n".join(
        [
            f"# Smoothed Forward Confirmation {manifest.get('confirmation_id')}",
            "",
            f"- registered_targets: {[row.get('target_id') for row in target_rows]}",
            f"- required_forward_events: {SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS}",
            f"- required_sideways_events: {SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS}",
            f"- required_recovery_events: {SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS}",
            f"- windows: {list(SMOOTHED_CONFIRMATION_WINDOWS)}",
            f"- watch_only_targets: {watch_only}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "达标后仍不自动 promotion。confirmation targets 只定义 forward evidence "
            "收集口径，owner review 仍是任何方法晋级前置条件。",
            "",
        ]
    )


def render_smoothed_watch_checklist(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Owner Smoothed Watch Checklist",
            "",
            "- [ ] 是否继续观察 smooth_weights_3d？",
            "- [ ] 是否保留 smooth_weights_5d 作为 sensitivity candidate？",
            "- [ ] 是否接受 limited_adjustment 降为 baseline / reference？",
            "- [ ] 是否要求更多 forward confirmation？",
            "- [ ] 是否有 lag warning？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- current_decision: {summary.get('current_decision')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            "",
        ]
    )


def render_smoothed_watch_pack_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    attribution: Mapping[str, Any],
    benefit_lag: Mapping[str, Any],
    regime: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> str:
    breakdown = _mapping(attribution.get("smoothed_decision_reason_breakdown"))
    tradeoff = _find_method(
        _records(_mapping(benefit_lag.get("benefit_lag_tradeoff_matrix")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    targets = _records(_mapping(confirmation.get("smoothed_confirmation_targets")).get("targets"))
    sideways = _find_method(
        _records(_mapping(regime.get("sideways_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    return "\n".join(
        [
            f"# Smoothed Watch Pack {manifest.get('watch_pack_id')}",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- current_decision: {summary.get('current_decision')}",
            f"- confidence: {summary.get('confidence')}",
            f"- benefit_lag_tradeoff: {summary.get('benefit_lag_tradeoff')}",
            f"- sideways_validation_status: {summary.get('sideways_validation_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- supporting_reasons: {breakdown.get('supporting_reasons')}",
            f"- blocking_reasons: {breakdown.get('blocking_reasons')}",
            f"- tradeoff_recommendation: {tradeoff.get('recommendation')}",
            f"- sideways_turnover_delta_vs_limited: "
            f"{sideways.get('turnover_delta_vs_limited')}",
            f"- registered_targets: {[row.get('target_id') for row in targets]}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "smoothed method 当前仍值得继续观察，但最大风险是 recovery lag。最大支持证据"
            "来自 turnover / churn / rolling consistency 方向；forward confirmation 仍缺真实"
            "前向事件样本。owner 暂不需要批准 production，只需要决定是否继续观察和是否"
            "调整后续确认样本口径。",
            "",
        ]
    )


def render_smoothed_watch_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Method Watch",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- current_decision: {summary.get('current_decision')}",
            f"- benefit_lag_tradeoff: {summary.get('benefit_lag_tradeoff')}",
            f"- sideways_validation_status: {summary.get('sideways_validation_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_evidence_gap_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    reason: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Evidence Gap {manifest.get('gap_id')}",
            "",
            f"- current_tradeoff_status: {matrix.get('current_tradeoff_status')}",
            f"- forward_confirmation_status: {matrix.get('forward_confirmation_status')}",
            f"- tradeoff_can_be_resolved_by_backfill: "
            f"{reason.get('tradeoff_can_be_resolved_by_backfill')}",
            f"- requires_forward_data: {reason.get('requires_forward_data')}",
            f"- requires_new_target_method: {reason.get('requires_new_target_method')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Primary Gap Reasons",
            "",
            *[
                "- "
                f"{row.get('reason')}: severity={row.get('severity')}, "
                f"recommended_action={row.get('recommended_action')}"
                for row in _records(reason.get("primary_gap_reasons"))
            ],
            "",
            "## Missing Evidence Matrix",
            "",
            *[
                "- "
                f"{row.get('evidence_type')}: status={row.get('status')}, "
                f"blocking={row.get('blocking')}, reason={row.get('reason')}"
                for row in _records(matrix.get("missing_evidence"))
            ],
            "",
            "## Required Metric Backfills",
            "",
            *[
                "- "
                f"{row.get('metric_family')}: required_for={row.get('required_for')}, "
                f"priority={row.get('priority')}"
                for row in _records(plan.get("required_backfills"))
            ],
            "",
            "该报告把 INSUFFICIENT_DATA 拆成可执行证据缺口。默认不需要新增 target method；"
            "下一步应先补 signal churn / weight jump backfill，并继续 forward confirmation。",
            "",
        ]
    )


def render_smoothed_churn_backfill_report(
    manifest: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    weight_jump_events: Sequence[Mapping[str, Any]],
    direction_flip_events: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    primary = _find_method(metrics, "smooth_weights_3d_limited_adjustment")
    primary_summary = _find_method(
        _records(summary.get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    return "\n".join(
        [
            f"# Smoothed Churn Backfill {manifest.get('churn_id')}",
            "",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- smooth_3d_weight_jump_count: {primary.get('weight_jump_count')}",
            f"- smooth_3d_direction_flip_count: {primary.get('direction_flip_count')}",
            f"- smooth_3d_turnover: {primary.get('turnover')}",
            f"- smooth_3d_signal_churn_score: {primary.get('signal_churn_score')}",
            f"- smooth_3d_churn_reduction_status: "
            f"{primary_summary.get('churn_reduction_status')}",
            f"- best_churn_reduction_method: {summary.get('best_churn_reduction_method')}",
            f"- weight_jump_event_count: {len(weight_jump_events)}",
            f"- direction_flip_event_count: {len(direction_flip_events)}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "该 backfill 直接量化 weight jump、direction flip、turnover 和 signal churn。"
            "这些指标只支持 research readiness，不允许自动 promotion 或 broker action。",
            "",
        ]
    )


def render_sideways_mixed_attribution_report(
    manifest: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    reason: Mapping[str, Any],
    breakdown: Mapping[str, Any],
) -> str:
    preview = list(outcomes)[:10]
    return "\n".join(
        [
            f"# Sideways Mixed Attribution {manifest.get('sideways_attribution_id')}",
            "",
            f"- sideways_validation: {reason.get('sideways_validation')}",
            f"- improved_window_count: {reason.get('improved_window_count')}",
            f"- worse_window_count: {reason.get('worse_window_count')}",
            f"- mixed_window_count: {reason.get('mixed_window_count')}",
            f"- dominant_reason: {reason.get('dominant_reason')}",
            f"- recommendation: {reason.get('recommendation')}",
            f"- preferred_sideways_method: {breakdown.get('preferred_sideways_method')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## 3d vs 5d",
            "",
            *[
                "- "
                f"{row.get('method')}: status={row.get('sideways_status')}, "
                f"return_delta={row.get('return_delta')}, "
                f"drawdown_delta={row.get('drawdown_delta')}, "
                f"churn_reduction={row.get('churn_reduction')}, "
                f"lag_cost={row.get('lag_cost')}"
                for row in _records(breakdown.get("methods"))
            ],
            "",
            "## Window Outcomes",
            "",
            *[
                "- "
                f"{row.get('window_id')} {row.get('method')}: "
                f"outcome={row.get('outcome_class')}, "
                f"reason={row.get('likely_reason')}, "
                f"return_delta={row.get('return_delta_vs_limited')}, "
                f"churn_delta={row.get('churn_delta_vs_limited')}"
                for row in preview
            ],
            "",
            "sideways_validation=MIXED 表示窗口层面改善和恶化并存，不能简化为 promotion "
            "或 rejection。该归因不调整 smoothing 参数，不写 official target weights。",
            "",
        ]
    )


def render_smoothed_readiness_scorecard_report(
    manifest: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Readiness Scorecard {manifest.get('scorecard_id')}",
            "",
            f"- recommended_method: {decision.get('recommended_method')}",
            f"- secondary_method: {decision.get('secondary_method')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- blocking_reasons: {', '.join(_texts(decision.get('blocking_reasons')))}",
            f"- required_forward_confirmation: "
            f"{', '.join(_texts(decision.get('required_forward_confirmation')))}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Method Scores",
            "",
            *[
                "- "
                f"{row.get('method')}: overall={row.get('overall_readiness_score')}, "
                f"status={row.get('readiness_status')}, "
                f"return={row.get('return_preservation_score')}, "
                f"turnover={row.get('turnover_score')}, "
                f"weight_jump={row.get('weight_jump_score')}, "
                f"signal_churn={row.get('signal_churn_score')}, "
                f"sideways={row.get('sideways_score')}, "
                f"recovery_lag={row.get('recovery_lag_score')}, "
                f"forward={row.get('forward_confirmation_score')}"
                for row in _records(scorecard.get("methods"))
            ],
            "",
            "PROMOTE_FOR_REVIEW 只表示 research review readiness，不是 official target "
            "weights、broker action 或 production approval。",
            "",
        ]
    )


def render_smoothed_owner_review_checklist(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Review Checklist",
            "",
            "- [ ] 是否继续观察 smooth_weights_3d？",
            "- [ ] 是否接受 smooth_weights_3d 进入 promotion review？",
            "- [ ] 是否保留 smooth_weights_5d 作为 sensitivity method？",
            "- [ ] 是否接受 limited_adjustment 降为 baseline / reference？",
            "- [ ] 是否需要更多 forward confirmation？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            "",
        ]
    )


def render_smoothed_owner_review_update_report(
    manifest: Mapping[str, Any],
    options: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> str:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    return "\n".join(
        [
            f"# Smoothed Owner Review Update {manifest.get('owner_update_id')}",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- secondary_method: {options.get('secondary_method')}",
            f"- current_decision: {options.get('current_decision')}",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            f"- scorecard_confidence: {decision.get('confidence')}",
            f"- watch_recommended_action: {watch_summary.get('recommended_action')}",
            "- auto_apply: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Owner Options",
            "",
            *[
                "- "
                f"{row.get('decision')}: recommended={row.get('recommended')}, "
                f"reason={row.get('reason')}"
                for row in _records(options.get("owner_decision_options"))
            ],
            "",
            "owner 当前只需要人工判断继续观察、进入 research promotion review、保留为 "
            "secondary，或拒绝 smoothed method。本报告不会自动修改配置或仓位。",
            "",
        ]
    )


def render_smoothed_owner_review_reader_brief(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Owner Review",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- readiness_decision: {options.get('readiness_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_confirmation_status: {options.get('forward_confirmation_status')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_promotion_review_report(
    manifest: Mapping[str, Any],
    evidence: Mapping[str, Any],
    blocking: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Promotion Review {manifest.get('promotion_review_id')}",
            "",
            f"- candidate_method: {evidence.get('candidate_method')}",
            f"- secondary_method: {evidence.get('secondary_method')}",
            f"- readiness_decision: {evidence.get('readiness_decision')}",
            f"- decision_confidence: {evidence.get('decision_confidence')}",
            f"- can_enter_owner_review: {blocking.get('can_enter_owner_review')}",
            "- automatic_promotion_allowed: false",
            "- can_write_official_target_weights: false",
            "- can_trigger_production: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Supporting Evidence",
            "",
            *[
                "- "
                f"{row.get('evidence_id')}: {row.get('summary')} "
                f"quality={row.get('evidence_quality')}, "
                f"supports_review={row.get('supports_promotion_review')}"
                for row in _records(evidence.get("supporting_evidence"))
            ],
            "",
            "## Blocking Issues",
            "",
            *[
                "- "
                f"{row.get('issue')}: severity={row.get('severity')}, "
                f"blocks_official={row.get('blocks_official_promotion')}, "
                "blocks_paper_shadow_primary_candidate="
                f"{row.get('blocks_paper_shadow_primary_candidate')}, "
                f"reason={row.get('reason')}"
                for row in _records(blocking.get("blocking_issues"))
            ],
            "",
            "PROMOTE_FOR_REVIEW 只表示可以进入 owner review。Forward confirmation "
            "和 low confidence 会阻止 official / production promotion，但不阻止 owner "
            "在 paper shadow research scope 内继续人工评估 primary candidate。",
            "",
        ]
    )


def render_smoothed_promotion_review_reader_brief(
    evidence: Mapping[str, Any],
    blocking: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Promotion Review",
            "",
            f"- candidate_method: {evidence.get('candidate_method')}",
            f"- readiness_decision: {evidence.get('readiness_decision')}",
            f"- decision_confidence: {evidence.get('decision_confidence')}",
            f"- can_enter_owner_review: {blocking.get('can_enter_owner_review')}",
            "- automatic_promotion_allowed: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_primary_research_candidate_gate_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Primary Research Candidate Gate {manifest.get('gate_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- gate_scope: {decision.get('gate_scope')}",
            f"- gate_decision: {decision.get('gate_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- owner_approval_required: {decision.get('owner_approval_required')}",
            "- auto_apply: false",
            f"- can_update_paper_shadow_primary_candidate: "
            f"{decision.get('can_update_paper_shadow_primary_candidate')}",
            "- can_write_official_target_weights: false",
            "- can_trigger_production: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Criteria",
            "",
            *[
                "- "
                f"{row.get('criterion')}: required={row.get('required')}, "
                f"actual={row.get('actual')}, status={row.get('status')}"
                for row in _records(criteria.get("criteria"))
            ],
            "",
            f"- hard_blockers: {', '.join(_texts(criteria.get('hard_blockers')))}",
            f"- warnings: {', '.join(_texts(criteria.get('warnings')))}",
            "",
            "ELIGIBLE_FOR_OWNER_APPROVAL 不是自动 promotion；它只表示 owner 可以人工决定"
            "是否允许后续 paper shadow primary research candidate switch。",
            "",
        ]
    )


def render_smoothed_forward_binding_report(
    manifest: Mapping[str, Any],
    binding: Mapping[str, Any],
    requirements: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Binding {manifest.get('binding_id')}",
            "",
            f"- source_confirmation_id: {binding.get('source_confirmation_id')}",
            f"- bound_target_count: {len(_records(binding.get('targets')))}",
            "- bound_to_weekly_progress: true",
            "- auto_rule_change_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Bound Targets",
            "",
            *[
                "- "
                f"{row.get('target_id')}: method={row.get('method')}, "
                f"status={row.get('status')}, "
                f"required_forward_events={row.get('required_forward_events', 'n/a')}, "
                f"required_sideways_events={row.get('required_sideways_events', 'n/a')}, "
                f"required_recovery_events={row.get('required_recovery_events', 'n/a')}"
                for row in _records(binding.get("targets"))
            ],
            "",
            "## Progress Requirements",
            "",
            *[
                "- "
                f"{row.get('requirement')}: cadence={row.get('cadence')}, "
                f"description={row.get('description')}"
                for row in _records(requirements.get("requirements"))
            ],
            "",
            f"- rule_review_ready_when: "
            f"{', '.join(_texts(requirements.get('rule_review_ready_when')))}",
            "",
            "该 binding 只把 smoothed targets 放入 weekly evidence 解释语义，"
            "不自动修改 rule、policy、target weights 或 broker state。",
            "",
        ]
    )


def render_smoothed_forward_binding_reader_brief(
    binding: Mapping[str, Any],
    requirements: Mapping[str, Any],
) -> str:
    watch_only = [
        row.get("target_id")
        for row in _records(binding.get("targets"))
        if row.get("status") == "WATCH_ONLY"
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Binding",
            "",
            f"- source_confirmation_id: {binding.get('source_confirmation_id')}",
            f"- bound_targets: {len(_records(binding.get('targets')))}",
            f"- watch_only_targets: {', '.join(_texts(watch_only))}",
            f"- rule_review_ready_when: "
            f"{', '.join(_texts(requirements.get('rule_review_ready_when')))}",
            "- auto_rule_change_allowed: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_primary_switch_report(
    manifest: Mapping[str, Any],
    plan: Mapping[str, Any],
    safety: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Primary Switch Plan {manifest.get('switch_plan_id')}",
            "",
            f"- current_primary_research_candidate: "
            f"{plan.get('current_primary_research_candidate')}",
            f"- proposed_primary_research_candidate: "
            f"{plan.get('proposed_primary_research_candidate')}",
            f"- switch_scope: {plan.get('switch_scope')}",
            f"- switch_decision: {plan.get('switch_decision')}",
            f"- auto_switch: {plan.get('auto_switch')}",
            f"- requires_owner_decision: {plan.get('requires_owner_decision')}",
            f"- requires_forward_confirmation: {plan.get('requires_forward_confirmation')}",
            f"- rollback_method: {plan.get('rollback_method')}",
            f"- safety_status: {safety.get('status')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Effective Only For",
            "",
            *[f"- {item}" for item in _texts(plan.get("effective_only_for"))],
            "",
            "该 switch plan 不执行切换。Owner 批准后也只能影响 paper shadow reports、"
            "research method watch 和 Reader Brief research section。",
            "",
        ]
    )


def render_paper_shadow_primary_switch_reader_brief(
    plan: Mapping[str, Any],
    safety: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Paper Shadow Primary Switch Plan",
            "",
            f"- proposed_primary_research_candidate: "
            f"{plan.get('proposed_primary_research_candidate')}",
            f"- switch_decision: {plan.get('switch_decision')}",
            f"- auto_switch: {plan.get('auto_switch')}",
            f"- rollback_method: {plan.get('rollback_method')}",
            f"- safety_status: {safety.get('status')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_owner_promotion_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Promotion Checklist",
            "",
            "- [ ] 是否接受 smooth_weights_3d 进入 promotion review？",
            "- [ ] 是否接受它成为 paper shadow primary research candidate？",
            "- [ ] 是否继续保留 smooth_weights_5d 为 secondary / sensitivity method？",
            "- [ ] 是否继续保留 limited_adjustment 为 rollback / baseline method？",
            "- [ ] 是否需要更多 forward confirmation？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            f"- paper_shadow_primary_candidate_change_allowed: "
            f"{decision.get('paper_shadow_primary_candidate_change_allowed')}",
            "",
        ]
    )


def render_smoothed_owner_promotion_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Owner Promotion Decision {manifest.get('decision_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- secondary_method: {decision.get('secondary_method')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            f"- decision_reason: {decision.get('decision_reason')}",
            f"- paper_shadow_primary_candidate_change_allowed: "
            f"{decision.get('paper_shadow_primary_candidate_change_allowed')}",
            f"- paper_shadow_primary_candidate_change_requested: "
            f"{decision.get('paper_shadow_primary_candidate_change_requested')}",
            "- actual_switch_executed: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            f"- created_at: {decision.get('created_at')}",
            f"- updated_at: {decision.get('updated_at')}",
            "",
            "该 journal 只记录 owner decision。即使 owner_decision="
            "promote_to_primary_research_candidate，也不自动切换 primary candidate，"
            "不写 official target weights，不触发 broker 或 production。",
            "",
        ]
    )


def render_smoothed_owner_promotion_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Promotion Decision",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- recommended_owner_action: {decision.get('recommended_owner_action')}",
            f"- paper_shadow_primary_candidate_change_allowed: "
            f"{decision.get('paper_shadow_primary_candidate_change_allowed')}",
            f"- forward_confirmation_status: {decision.get('forward_confirmation_status')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_forward_progress_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Progress {manifest.get('progress_id')}",
            "",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- available_forward_events_total: {summary.get('available_forward_events_total')}",
            f"- required_forward_events_total: {summary.get('required_forward_events_total')}",
            f"- available_sideways_events: {summary.get('available_sideways_events')}",
            f"- required_sideways_events: {summary.get('required_sideways_events')}",
            f"- available_recovery_events: {summary.get('available_recovery_events')}",
            f"- required_recovery_events: {summary.get('required_recovery_events')}",
            f"- ready_for_review_count: {summary.get('ready_for_review_count')}",
            f"- summary_recommendation: {summary.get('summary_recommendation')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Target Progress",
            "",
            *[
                "- "
                f"{row.get('target_id')}: status={row.get('progress_status')}, "
                f"available={_smoothed_target_available_events(row)}, "
                f"required={_smoothed_target_required_events(row)}, "
                f"blocking={', '.join(_texts(row.get('blocking_reasons')))}"
                for row in targets
            ],
            "",
            "当前 progress 只跟踪可审计 forward evidence。样本不足时不能标记 "
            "READY_FOR_REVIEW，也不能触发 primary candidate switch。",
            "",
        ]
    )


def render_smoothed_forward_progress_reader_brief(
    summary: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
) -> str:
    target_statuses = [f"{row.get('target_id')}={row.get('progress_status')}" for row in targets]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Progress",
            "",
            f"- progress_id: {summary.get('progress_id')}",
            f"- binding_id: {summary.get('binding_id')}",
            f"- forward_progress: {summary.get('available_forward_events_total')}/"
            f"{summary.get('required_forward_events_total')}",
            f"- sideways_progress: {summary.get('available_sideways_events')}/"
            f"{summary.get('required_sideways_events')}",
            f"- recovery_progress: {summary.get('available_recovery_events')}/"
            f"{summary.get('required_recovery_events')}",
            f"- target_statuses: {', '.join(target_statuses)}",
            f"- summary_recommendation: {summary.get('summary_recommendation')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_weekly_dashboard_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    table: Mapping[str, Any],
) -> str:
    targets = _records(table.get("targets"))
    return "\n".join(
        [
            f"# Smoothed Weekly Evidence Dashboard {manifest.get('dashboard_id')}",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- current_owner_decision: {summary.get('current_owner_decision')}",
            f"- gate_decision: {summary.get('gate_decision')}",
            f"- decision_confidence: {summary.get('decision_confidence')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- ready_for_switch_recheck: {summary.get('ready_for_switch_recheck')}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Target Status",
            "",
            *[
                "- "
                f"{row.get('target_id')}: status={row.get('status')}, "
                f"available={row.get('available_events')}, "
                f"required={row.get('required_events')}, "
                f"progress_pct={row.get('progress_pct')}, decision={row.get('decision')}"
                for row in targets
            ],
            "",
            "Dashboard 只汇总 weekly evidence；ready_for_switch_recheck=false 时仍建议 "
            "continue_observation，且不允许 broker / production 行为。",
            "",
        ]
    )


def render_smoothed_weekly_dashboard_reader_brief(
    summary: Mapping[str, Any],
    table: Mapping[str, Any],
) -> str:
    target_statuses = [
        f"{row.get('target_id')}={row.get('status')}" for row in _records(table.get("targets"))
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Weekly Dashboard",
            "",
            f"- dashboard_id: {summary.get('dashboard_id')}",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- ready_for_switch_recheck: {summary.get('ready_for_switch_recheck')}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            f"- target_statuses: {', '.join(target_statuses)}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_event_monitor_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    sideways_events: Sequence[Mapping[str, Any]],
    recovery_events: Sequence[Mapping[str, Any]],
) -> str:
    sideways = _mapping(summary.get("sideways_events"))
    recovery = _mapping(summary.get("recovery_events"))
    lag_warnings = sum(1 for row in recovery_events if row.get("lag_warning") is True)
    return "\n".join(
        [
            f"# Smoothed Event Monitor {manifest.get('monitor_id')}",
            "",
            f"- progress_id: {manifest.get('progress_id')}",
            "- sideways_available_required: "
            f"{sideways.get('available')}/{sideways.get('required')}",
            "- recovery_available_required: "
            f"{recovery.get('available')}/{recovery.get('required')}",
            f"- sideways_pending: {sideways.get('pending')}",
            f"- recovery_pending: {recovery.get('pending')}",
            f"- sideways_status: {summary.get('sideways_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- lag_warning_count: {lag_warnings}",
            f"- recommended_action: {summary.get('recommended_action')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Event monitor 只累计 sideways / recovery observation inventory。当前没有 "
            "available samples 时保持 continue_event_collection。",
            "",
        ]
    )


def render_smoothed_switch_readiness_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Switch Readiness Recheck {manifest.get('recheck_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- current_owner_decision: {decision.get('current_owner_decision')}",
            f"- previous_gate_decision: {decision.get('previous_gate_decision')}",
            f"- recheck_decision: {decision.get('recheck_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- can_execute_switch: {decision.get('can_execute_switch')}",
            f"- owner_decision_required: {decision.get('owner_decision_required')}",
            f"- auto_switch: {decision.get('auto_switch')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Criteria",
            "",
            *[
                "- "
                f"{row.get('criterion')}: required={row.get('required')}, "
                f"available={row.get('available', row.get('actual'))}, "
                f"status={row.get('status')}"
                for row in _records(criteria.get("criteria"))
            ],
            "",
            f"- hard_blockers: {', '.join(_texts(criteria.get('hard_blockers')))}",
            f"- warnings: {', '.join(_texts(criteria.get('warnings')))}",
            "",
            "Readiness recheck 不执行 switch。can_execute_switch 必须保持 false，任何 "
            "promotion 都需要 owner decision 和后续独立任务。",
            "",
        ]
    )


def render_smoothed_switch_readiness_reader_brief(
    decision: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> str:
    not_met = [
        row.get("criterion")
        for row in _records(criteria.get("criteria"))
        if row.get("status") != "PASS"
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Switch Readiness",
            "",
            f"- recheck_id: {decision.get('recheck_id')}",
            f"- recheck_decision: {decision.get('recheck_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- criteria_not_met: {', '.join(_texts(not_met))}",
            f"- can_execute_switch: {decision.get('can_execute_switch')}",
            f"- owner_decision_required: {decision.get('owner_decision_required')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_owner_renewal_checklist(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Renewal Checklist",
            "",
            "- [ ] 当前是否仍继续观察？",
            "- [ ] forward events 是否足够？",
            "- [ ] sideways samples 是否足够？",
            "- [ ] recovery lag 是否有 warning？",
            "- [ ] 是否需要 request more forward data？",
            "- [ ] 是否可进入 promote_to_primary_research_candidate？",
            "- [ ] 是否确认不写 official target weights？",
            "- [ ] 是否确认 no broker / no production？",
            "",
            f"- previous_owner_decision: {options.get('previous_owner_decision')}",
            f"- current_recheck_decision: {options.get('current_recheck_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_progress: {options.get('forward_progress')}",
            f"- sideways_progress: {options.get('sideways_progress')}",
            f"- recovery_lag_status: {options.get('recovery_lag_status')}",
            "",
        ]
    )


def render_smoothed_owner_renewal_report(
    manifest: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Owner Renewal {manifest.get('renewal_id')}",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- previous_owner_decision: {options.get('previous_owner_decision')}",
            f"- current_recheck_decision: {options.get('current_recheck_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_progress: {options.get('forward_progress')}",
            f"- sideways_progress: {options.get('sideways_progress')}",
            f"- recovery_lag_status: {options.get('recovery_lag_status')}",
            "- auto_switch: false",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Owner Options",
            "",
            *[
                "- "
                f"{row.get('decision')}: recommended={row.get('recommended')}, "
                f"reason={row.get('reason')}"
                for row in _records(options.get("owner_options"))
            ],
            "",
            "Renewal pack 只生成 owner review 材料，不自动切换 primary candidate，不写 "
            "official target weights，不触发 broker 或 production。",
            "",
        ]
    )


def render_smoothed_owner_renewal_reader_brief(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Owner Renewal",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- previous_owner_decision: {options.get('previous_owner_decision')}",
            f"- current_recheck_decision: {options.get('current_recheck_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            f"- forward_progress: {options.get('forward_progress')}",
            f"- sideways_progress: {options.get('sideways_progress')}",
            f"- recovery_lag_status: {options.get('recovery_lag_status')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_daily_emission_report(
    manifest: Mapping[str, Any],
    event: Mapping[str, Any],
    event_weights: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    validation = _mapping(event_weights.get("weight_validation"))
    weights = _mapping(event_weights.get("weights"))
    return "\n".join(
        [
            f"# Smoothed Daily Emission {manifest.get('emission_id')}",
            "",
            f"- as_of: {manifest.get('as_of')}",
            f"- emitted_event_count: {manifest.get('emitted_event_count')}",
            f"- event_status: {event.get('event_status')}",
            f"- data_quality: {data_quality.get('data_quality')}",
            f"- regime_context: {event.get('regime_context')}",
            f"- outcome_windows: {', '.join(_texts(event.get('outcome_windows')))}",
            f"- future_data_used: {data_quality.get('future_data_used')}",
            "- order_ticket_generated: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Weight Validation",
            "",
            f"- all_weights_sum_to_one: {validation.get('all_weights_sum_to_one')}",
            f"- no_negative_weights: {validation.get('no_negative_weights')}",
            f"- constraint_status: {validation.get('constraint_status')}",
            "",
            "## Method Weights",
            "",
            *[
                f"- {method}: {weights.get(method)}"
                for method in (
                    "smooth_weights_3d_limited_adjustment",
                    "smooth_weights_5d_limited_adjustment",
                    "limited_adjustment",
                    "static_baseline",
                    "no_trade_baseline",
                )
            ],
            "",
            "该 artifact 只生成 forward evidence tracking event，不是 trading signal、"
            "official target weights、order ticket 或 production change。",
            "",
        ]
    )


def render_smoothed_daily_emission_reader_brief(
    manifest: Mapping[str, Any],
    event: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Daily Emission",
            "",
            f"- emission_id: {manifest.get('emission_id')}",
            f"- as_of: {manifest.get('as_of')}",
            f"- emitted_event_count: {manifest.get('emitted_event_count')}",
            f"- event_status: {event.get('event_status')}",
            f"- data_quality: {data_quality.get('data_quality')}",
            f"- future_data_used: {data_quality.get('future_data_used')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_outcome_due_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Outcome Due {manifest.get('due_id')}",
            "",
            f"- scanner_as_of: {summary.get('scanner_as_of')}",
            f"- events_scanned: {summary.get('events_scanned')}",
            f"- total_windows_scanned: {summary.get('total_windows_scanned')}",
            f"- due_windows: {summary.get('due_windows')}",
            f"- not_due_windows: {summary.get('not_due_windows')}",
            f"- price_missing_windows: {summary.get('price_missing_windows')}",
            f"- blocked_future_as_of: {summary.get('blocked_future_as_of')}",
            f"- update_ready_count: {summary.get('update_ready_count')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Due Windows",
            "",
            *[
                "- "
                f"{row.get('event_id')} window={row.get('window_days')} "
                f"expected_end={row.get('expected_end_date')} "
                f"status={row.get('due_status')} can_update={row.get('can_update')}"
                for row in rows
            ],
            "",
            "Due scanner 只判断窗口是否到期，不直接更新 outcome，不使用 scanner_as_of "
            "之后的价格。",
            "",
        ]
    )


def render_smoothed_outcome_update_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    updated: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Outcome Update {manifest.get('update_id')}",
            "",
            f"- due_id: {manifest.get('due_id')}",
            f"- updated_windows: {summary.get('updated_count')}",
            f"- skipped_windows: {summary.get('skipped_count')}",
            "- available_forward_events_after_update: "
            f"{summary.get('available_forward_events_after_update')}",
            "- smooth_3d_win_rate_vs_limited: " f"{summary.get('smooth_3d_win_rate_vs_limited')}",
            "- avg_smooth_3d_relative_return_vs_limited: "
            f"{summary.get('avg_smooth_3d_relative_return_vs_limited')}",
            "- avg_smooth_3d_drawdown_delta_vs_limited: "
            f"{summary.get('avg_smooth_3d_drawdown_delta_vs_limited')}",
            f"- summary_recommendation: {summary.get('summary_recommendation')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Updated Windows",
            "",
            *[
                "- "
                f"{row.get('event_id')} window={row.get('window_days')} "
                f"smooth_3d_vs_limited="
                f"{_mapping(row.get('relative_metrics')).get('smooth_3d_vs_limited')}"
                for row in updated
            ],
            "",
            "## Skipped Windows",
            "",
            *[
                "- "
                f"{row.get('event_id')} window={row.get('window_days')} "
                f"reason={row.get('skip_reason')}"
                for row in skipped
            ],
            "",
            "Updater 只更新 due scanner 标记为 can_update=true 的窗口，不写 official "
            "target weights，不触发 broker 或 production。",
            "",
        ]
    )


def render_smoothed_outcome_update_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Outcome Update",
            "",
            f"- update_id: {summary.get('update_id')}",
            f"- updated_windows: {summary.get('updated_count')}",
            f"- skipped_windows: {summary.get('skipped_count')}",
            "- available_forward_events_after_update: "
            f"{summary.get('available_forward_events_after_update')}",
            "- avg_smooth_3d_relative_return_vs_limited: "
            f"{summary.get('avg_smooth_3d_relative_return_vs_limited')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_forward_classification_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Classification {manifest.get('classification_id')}",
            "",
            f"- update_id: {manifest.get('update_id')}",
            f"- events_classified: {summary.get('events_classified')}",
            f"- sideways_events_available: {summary.get('sideways_events_available')}",
            f"- recovery_events_available: {summary.get('recovery_events_available')}",
            "- fast_regime_change_events_available: "
            f"{summary.get('fast_regime_change_events_available')}",
            f"- lag_warning_count: {summary.get('lag_warning_count')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Classified Events",
            "",
            *[
                "- "
                f"{row.get('event_id')} window={row.get('window_days')} "
                f"class={','.join(_texts(row.get('regime_classification')))} "
                f"confidence={row.get('classification_confidence')} "
                f"lag_warning={row.get('lag_warning')}"
                for row in rows
            ],
            "",
            "Classification 只影响 evidence progress，不影响交易、target weights 或 "
            "production state。",
            "",
        ]
    )


def render_smoothed_forward_weekly_run_report(
    manifest: Mapping[str, Any],
    steps: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Weekly Run {manifest.get('weekly_run_id')}",
            "",
            f"- week_ending: {summary.get('week_ending')}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- due_windows: {summary.get('due_windows')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- classified_events: {summary.get('classified_events')}",
            "- forward_progress: "
            f"{summary.get('available_forward_events')}/"
            f"{summary.get('required_forward_events')}",
            "- sideways_progress: "
            f"{summary.get('available_sideways_events')}/"
            f"{summary.get('required_sideways_events')}",
            "- recovery_progress: "
            f"{summary.get('available_recovery_events')}/"
            f"{summary.get('required_recovery_events')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Steps",
            "",
            *[
                "- "
                f"{row.get('step')}: status={row.get('status')}, "
                f"artifact_id={row.get('artifact_id')}"
                for row in _records(steps.get("steps"))
            ],
            "",
            "Weekly runner 串联 smoothed forward evidence，但默认安全模式仍不切换、"
            "不写 official target weights、不触发 broker、不产生 production effect。",
            "",
        ]
    )


def render_smoothed_forward_weekly_run_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Weekly Run",
            "",
            f"- weekly_run_id: {summary.get('weekly_run_id')}",
            f"- week_ending: {summary.get('week_ending')}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            "- forward_progress: "
            f"{summary.get('available_forward_events')}/"
            f"{summary.get('required_forward_events')}",
            "- sideways_progress: "
            f"{summary.get('available_sideways_events')}/"
            f"{summary.get('required_sideways_events')}",
            "- recovery_progress: "
            f"{summary.get('available_recovery_events')}/"
            f"{summary.get('required_recovery_events')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_data_preflight_report(
    manifest: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    command_matrix: Mapping[str, Any],
    blocked_matrix: Mapping[str, Any],
) -> str:
    latest = _mapping(snapshot.get("latest_available"))
    return "\n".join(
        [
            f"# Smoothed Data Preflight {manifest.get('preflight_id')}",
            "",
            f"- requested_as_of: {snapshot.get('requested_as_of')}",
            f"- requested_week_ending: {snapshot.get('requested_week_ending')}",
            f"- latest_valid_as_of: {snapshot.get('latest_valid_as_of')}",
            f"- freshness_status: {snapshot.get('freshness_status')}",
            f"- validate_data_status: {snapshot.get('validate_data_status')}",
            f"- blocking_errors: {', '.join(_texts(snapshot.get('blocking_errors')))}",
            f"- latest prices_daily: {latest.get('prices_daily')}",
            f"- latest prices_marketstack_daily: {latest.get('prices_marketstack_daily')}",
            f"- latest rates_daily: {latest.get('rates_daily')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Runnable Command Matrix",
            "",
            *[
                "- "
                f"{row.get('command')}: status={row.get('status')} "
                f"reason={row.get('reason')} resolved_as_of={row.get('resolved_as_of')}"
                for row in _records(command_matrix.get("commands"))
            ],
            "",
            "## Blocked Reasons",
            "",
            *[
                "- "
                f"{row.get('reason')}: severity={row.get('severity')} "
                f"latest={row.get('latest_available')} blocks="
                f"{','.join(_texts(row.get('blocks')))}"
                for row in _records(blocked_matrix.get("blocked_reasons"))
            ],
            "",
            "Preflight 只解释当前 cached data 能否支持 requested date；它不刷新数据、"
            "不绕过 validate-data，也不更新 forward outcome。",
            "",
        ]
    )


def render_smoothed_data_preflight_reader_brief(
    snapshot: Mapping[str, Any],
    command_matrix: Mapping[str, Any],
) -> str:
    runnable_latest = [
        row
        for row in _records(command_matrix.get("commands"))
        if row.get("status") == "RUNNABLE_WITH_LATEST_AVAILABLE"
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Data Preflight",
            "",
            f"- preflight_id: {snapshot.get('preflight_id')}",
            f"- requested_as_of: {snapshot.get('requested_as_of')}",
            f"- requested_week_ending: {snapshot.get('requested_week_ending')}",
            f"- latest_valid_as_of: {snapshot.get('latest_valid_as_of')}",
            f"- freshness_status: {snapshot.get('freshness_status')}",
            f"- blocking_errors: {', '.join(_texts(snapshot.get('blocking_errors')))}",
            f"- latest_available_fallback_commands: {len(runnable_latest)}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_latest_emission_report(
    manifest: Mapping[str, Any],
    resolution: Mapping[str, Any],
    links: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Latest Emission {manifest.get('latest_emission_id')}",
            "",
            f"- source_preflight_id: {resolution.get('source_preflight_id')}",
            f"- requested_as_of: {resolution.get('requested_as_of')}",
            f"- resolved_as_of: {resolution.get('resolved_as_of')}",
            f"- resolution_reason: {resolution.get('resolution_reason')}",
            f"- fallback_scope: {resolution.get('fallback_scope')}",
            f"- daily_emission_id: {links.get('daily_emission_id')}",
            f"- emitted_event_count: {links.get('emitted_event_count')}",
            f"- event_status: {links.get('event_status')}",
            f"- due_scan_allowed: {resolution.get('due_scan_allowed')}",
            f"- outcome_update_allowed: {resolution.get('outcome_update_allowed')}",
            f"- future_data_used: {resolution.get('future_data_used')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Latest-available fallback 只生成 daily observation event。它不会扫描 due "
            "windows，不会更新 outcome，也不会把未成熟 forward sample 标记为 AVAILABLE。",
            "",
        ]
    )


def render_smoothed_latest_emission_reader_brief(
    resolution: Mapping[str, Any],
    links: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Latest Emission",
            "",
            f"- latest_emission_id: {resolution.get('latest_emission_id')}",
            f"- requested_as_of: {resolution.get('requested_as_of')}",
            f"- resolved_as_of: {resolution.get('resolved_as_of')}",
            f"- emitted_event_count: {links.get('emitted_event_count')}",
            f"- outcome_update_allowed: {resolution.get('outcome_update_allowed')}",
            f"- future_data_used: {resolution.get('future_data_used')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_blocked_owner_summary(
    snapshot: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    latest = _mapping(snapshot.get("latest_available"))
    requested = snapshot.get("requested_as_of") or snapshot.get("requested_week_ending")
    return "\n".join(
        [
            "# Smoothed Forward Run Blocked Summary",
            "",
            "Requested date:",
            str(requested),
            "",
            "Latest available data:",
            f"- prices_daily: {latest.get('prices_daily')}",
            f"- rates_daily: {latest.get('rates_daily')}",
            "",
            "Blocked commands:",
            *[f"- {row.get('command')}" for row in explanations],
            "",
            "Reason:",
            "Data freshness gate failed. This is expected fail-close behavior.",
            "",
            "Allowed actions:",
            "- run latest-available daily emission",
            "- refresh price / rate caches",
            "- retry after sources are fresh",
            "",
            "Forbidden actions:",
            "- do not bypass validate-data",
            "- do not update outcomes with stale data",
            "- do not use future data",
            "- do not generate order tickets",
            "",
        ]
    )


def render_smoothed_blocked_explain_report(
    manifest: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Blocked Explain {manifest.get('explain_id')}",
            "",
            f"- source_preflight_id: {manifest.get('source_preflight_id')}",
            f"- freshness_status: {snapshot.get('freshness_status')}",
            f"- validate_data_status: {snapshot.get('validate_data_status')}",
            f"- blocking_errors: {', '.join(_texts(snapshot.get('blocking_errors')))}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Blocked Commands",
            "",
            *[
                "- "
                f"{row.get('command')}: {row.get('human_explanation')} "
                f"safe_next_action={row.get('safe_next_action')}"
                for row in explanations
            ],
            "",
            "Blocked explanation 是人类可读诊断包，不是数据刷新执行器，也不是 "
            "production action approval。",
            "",
        ]
    )


def render_smoothed_blocked_explain_reader_brief(
    snapshot: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Blocked Explain",
            "",
            f"- freshness_status: {snapshot.get('freshness_status')}",
            f"- blocking_errors: {', '.join(_texts(snapshot.get('blocking_errors')))}",
            f"- blocked_command_count: {len(explanations)}",
            "- safe_next_action: refresh_sources_then_retry",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_refresh_plan_report(
    manifest: Mapping[str, Any],
    requirements: Mapping[str, Any],
    rerun: Mapping[str, Any],
    explain: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Refresh Plan {manifest.get('refresh_plan_id')}",
            "",
            f"- source_preflight_id: {manifest.get('source_preflight_id')}",
            f"- source_explain_id: {manifest.get('source_explain_id')}",
            f"- requested_as_of: {requirements.get('requested_as_of')}",
            f"- all_required_sources_fresh: {requirements.get('all_required_sources_fresh')}",
            f"- rerun_allowed_now: {rerun.get('rerun_allowed_now')}",
            f"- external_refresh_executed: {rerun.get('external_refresh_executed')}",
            f"- blocked_command_count: {explain.get('blocked_command_count')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Source Requirements",
            "",
            *[
                "- "
                f"{row.get('source')}: current_latest={row.get('current_latest_date')} "
                f"required_through={row.get('required_through')} "
                f"status={row.get('status')} action={row.get('required_action')}"
                for row in _records(requirements.get("source_requirements"))
            ],
            "",
            "## Rerun Commands",
            "",
            *[
                "- " f"{row.get('step')}. {row.get('command')} - {row.get('purpose')}"
                for row in _records(rerun.get("rerun_after_refresh"))
            ],
            "",
            "Refresh plan 只给出刷新和重跑计划，不直接调用外部数据源。",
            "",
        ]
    )


def render_smoothed_refresh_plan_reader_brief(
    requirements: Mapping[str, Any],
    rerun: Mapping[str, Any],
) -> str:
    stale = [
        row
        for row in _records(requirements.get("source_requirements"))
        if row.get("status") != "FRESH"
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Refresh Plan",
            "",
            f"- refresh_plan_id: {requirements.get('refresh_plan_id')}",
            f"- stale_source_count: {len(stale)}",
            f"- required_through: {requirements.get('requested_as_of')}",
            f"- rerun_allowed_now: {rerun.get('rerun_allowed_now')}",
            f"- external_refresh_executed: {rerun.get('external_refresh_executed')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_bootstrap_retry_report(
    manifest: Mapping[str, Any],
    preflight: Mapping[str, Any],
    steps: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Bootstrap Retry {manifest.get('retry_id')}",
            "",
            f"- requested_as_of: {summary.get('requested_as_of')}",
            f"- retry_status: {summary.get('retry_status')}",
            f"- preflight_status: {preflight.get('preflight_status')}",
            f"- blocking_errors: {', '.join(_texts(preflight.get('blocking_errors')))}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- due_windows: {summary.get('due_windows')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- classified_events: {summary.get('classified_events')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Retry Steps",
            "",
            *[
                "- "
                f"{row.get('step')}: status={row.get('status')} "
                f"artifact_id={row.get('artifact_id')} reason={row.get('reason')}"
                for row in _records(steps.get("steps"))
            ],
            "",
            "Retry runner 必须先执行 preflight。若 preflight blocked，则不运行 "
            "outcome update；若数据 ready，才串联完整 smoothed forward evidence chain。",
            "",
        ]
    )


def render_smoothed_bootstrap_retry_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Bootstrap Retry",
            "",
            f"- retry_id: {summary.get('retry_id')}",
            f"- retry_status: {summary.get('retry_status')}",
            f"- requested_as_of: {summary.get('requested_as_of')}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            "- forward_progress: " f"{summary.get('available_forward_events_after_retry')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_source_refresh_report(
    manifest: Mapping[str, Any],
    request: Mapping[str, Any],
    results: Mapping[str, Any],
) -> str:
    source_rows = _records(results.get("sources"))
    return "\n".join(
        [
            f"# Smoothed Source Refresh {manifest.get('refresh_execution_id')}",
            "",
            f"- source_refresh_plan_id: {manifest.get('source_refresh_plan_id')}",
            f"- requested_as_of: {manifest.get('requested_as_of')}",
            f"- execute_refresh: {request.get('execute_refresh')}",
            f"- refresh_status: {results.get('refresh_status')}",
            f"- all_sources_refreshed: {results.get('all_sources_refreshed')}",
            f"- partial_refresh: {results.get('partial_refresh')}",
            f"- refresh_error: {results.get('refresh_error') or ''}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Source Audit",
            "",
            *[
                "- "
                f"{row.get('source')}: status={row.get('status')} "
                f"required={row.get('required')} "
                f"before_latest={row.get('latest_date_before')} "
                f"after_latest={row.get('latest_date_after')} "
                f"before_rows={row.get('row_count_before')} "
                f"after_rows={row.get('row_count_after')} "
                f"freshness={row.get('freshness_after_refresh')}"
                for row in source_rows
            ],
            "",
            "Source refresh 默认 dry-run；只有显式 execute_refresh=true 时才允许写入 "
            "local cache。刷新结果只更新 research cache，不生成 official target weights、"
            "order ticket 或 broker action。",
            "",
        ]
    )


def render_smoothed_source_refresh_reader_brief(results: Mapping[str, Any]) -> str:
    source_rows = _records(results.get("sources"))
    ready_count = sum(1 for row in source_rows if row.get("freshness_after_refresh") == "READY")
    failed_sources = [
        _text(row.get("source")) for row in source_rows if row.get("status") == "FAILED"
    ]
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Source Refresh",
            "",
            f"- refresh_execution_id: {results.get('refresh_execution_id')}",
            f"- refresh_status: {results.get('refresh_status')}",
            f"- ready_source_count: {ready_count}",
            f"- failed_sources: {', '.join(failed_sources)}",
            f"- external_refresh_executed: {results.get('external_refresh_executed')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_post_refresh_report(
    manifest: Mapping[str, Any],
    data_validation: Mapping[str, Any],
    preflight: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Post Refresh Validation {manifest.get('post_refresh_id')}",
            "",
            f"- source_refresh_id: {manifest.get('source_refresh_id')}",
            f"- requested_as_of: {manifest.get('requested_as_of')}",
            f"- validate_data_status: {data_validation.get('validate_data_status')}",
            f"- freshness_status: {preflight.get('freshness_status')}",
            f"- latest_valid_as_of: {preflight.get('latest_valid_as_of')}",
            f"- retry_decision: {decision.get('retry_decision')}",
            f"- reason: {decision.get('reason')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Post-refresh validation 复用 smoothed data preflight / validate-data 等价路径。"
            "只有 retry_decision=RETRY_READY 时，后续 retry resume 才能继续完整链路。",
            "",
        ]
    )


def render_smoothed_post_refresh_reader_brief(
    decision: Mapping[str, Any],
    preflight: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Post Refresh Validation",
            "",
            f"- post_refresh_id: {decision.get('post_refresh_id')}",
            f"- retry_decision: {decision.get('retry_decision')}",
            f"- freshness_status: {preflight.get('freshness_status')}",
            f"- latest_valid_as_of: {preflight.get('latest_valid_as_of')}",
            f"- reason: {decision.get('reason')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_retry_resume_report(
    manifest: Mapping[str, Any],
    precondition: Mapping[str, Any],
    steps: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Retry Resume {manifest.get('resume_id')}",
            "",
            f"- post_refresh_id: {manifest.get('post_refresh_id')}",
            f"- requested_as_of: {summary.get('requested_as_of')}",
            f"- resume_status: {summary.get('resume_status')}",
            f"- precondition_status: {precondition.get('precondition_status')}",
            f"- retry_decision: {precondition.get('retry_decision')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- classified_events: {summary.get('classified_events')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Resume Steps",
            "",
            *[
                "- "
                f"{row.get('step')}: status={row.get('status')} "
                f"artifact_id={row.get('artifact_id')} reason={row.get('reason')}"
                for row in _records(steps.get("steps"))
            ],
            "",
            "Retry resume fail-close：post-refresh validation 未达到 RETRY_READY 时，"
            "不会运行 outcome update、switch readiness 或 owner renewal。",
            "",
        ]
    )


def render_smoothed_retry_resume_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Retry Resume",
            "",
            f"- resume_id: {summary.get('resume_id')}",
            f"- resume_status: {summary.get('resume_status')}",
            f"- requested_as_of: {summary.get('requested_as_of')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- forward_events_after_resume: "
            f"{summary.get('available_forward_events_after_resume')}",
            f"- can_execute_switch: {summary.get('can_execute_switch')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_sample_growth_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    by_target: Mapping[str, Any],
) -> str:
    before = _mapping(summary.get("before"))
    after = _mapping(summary.get("after"))
    delta = _mapping(summary.get("delta"))
    progress = _mapping(summary.get("progress"))
    return "\n".join(
        [
            f"# Smoothed Sample Growth {manifest.get('growth_id')}",
            "",
            f"- resume_id: {manifest.get('resume_id')}",
            f"- growth_status: {summary.get('growth_status')}",
            f"- forward_before_after_delta: {before.get('available_forward_events')} -> "
            f"{after.get('available_forward_events')} ({delta.get('forward_events')})",
            f"- sideways_before_after_delta: {before.get('available_sideways_events')} -> "
            f"{after.get('available_sideways_events')} ({delta.get('sideways_events')})",
            f"- recovery_before_after_delta: {before.get('available_recovery_events')} -> "
            f"{after.get('available_recovery_events')} ({delta.get('recovery_events')})",
            f"- forward_progress: {progress.get('forward')}",
            f"- sideways_progress: {progress.get('sideways')}",
            f"- recovery_progress: {progress.get('recovery')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Target Progress",
            "",
            *[
                "- "
                f"{row.get('target_id')}: status={row.get('status')} "
                f"before={row.get('before_available')} "
                f"after={row.get('after_available')} required={row.get('required')}"
                for row in _records(by_target.get("targets"))
            ],
            "",
        ]
    )


def render_smoothed_sample_growth_reader_brief(summary: Mapping[str, Any]) -> str:
    progress = _mapping(summary.get("progress"))
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Sample Growth",
            "",
            f"- growth_id: {summary.get('growth_id')}",
            f"- growth_status: {summary.get('growth_status')}",
            f"- forward_progress: {progress.get('forward')}",
            f"- sideways_progress: {progress.get('sideways')}",
            f"- recovery_progress: {progress.get('recovery')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_data_readiness_checklist(summary: Mapping[str, Any]) -> str:
    sources = _mapping(summary.get("sources_status"))
    return "\n".join(
        [
            f"# Smoothed Data Readiness Checklist {summary.get('readiness_id')}",
            "",
            f"- current_status: {summary.get('current_status')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            f"- prices_daily: {sources.get('prices_daily')}",
            f"- prices_marketstack_daily: {sources.get('prices_marketstack_daily')}",
            f"- rates_daily: {sources.get('rates_daily')}",
            f"- retry_status: {summary.get('retry_status')}",
            f"- sample_growth_status: {summary.get('sample_growth_status')}",
            f"- forward_progress: {summary.get('forward_progress')}",
            f"- sideways_progress: {summary.get('sideways_progress')}",
            f"- recovery_progress: {summary.get('recovery_progress')}",
            "- no broker / no production: confirmed",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_smoothed_data_readiness_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    checklist: str,
) -> str:
    checklist_path = manifest.get("owner_data_readiness_checklist_path")
    return "\n".join(
        [
            f"# Smoothed Data Readiness {manifest.get('readiness_id')}",
            "",
            f"- requested_as_of: {summary.get('requested_as_of')}",
            f"- current_status: {summary.get('current_status')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            f"- sources_status: {json.dumps(summary.get('sources_status'), ensure_ascii=False)}",
            f"- retry_status: {summary.get('retry_status')}",
            f"- sample_growth_status: {summary.get('sample_growth_status')}",
            f"- forward_progress: {summary.get('forward_progress')}",
            f"- sideways_progress: {summary.get('sideways_progress')}",
            f"- recovery_progress: {summary.get('recovery_progress')}",
            f"- owner_data_readiness_checklist_path: {checklist_path}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Owner Checklist",
            "",
            checklist,
            "",
            "该 status pack 是 owner 决策输入，不会自动改变 official target weights，"
            "也不会生成 broker order 或 production effect。",
            "",
        ]
    )


def render_smoothed_data_readiness_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Data Readiness",
            "",
            f"- readiness_id: {summary.get('readiness_id')}",
            f"- current_status: {summary.get('current_status')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            f"- retry_status: {summary.get('retry_status')}",
            f"- sample_growth_status: {summary.get('sample_growth_status')}",
            f"- forward_progress: {summary.get('forward_progress')}",
            f"- sideways_progress: {summary.get('sideways_progress')}",
            f"- recovery_progress: {summary.get('recovery_progress')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_backfill_report(
    manifest: Mapping[str, Any],
    calendar: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Historical Backfill {manifest.get('backfill_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- requested_date_range: {manifest.get('requested_start_date')} to "
            f"{manifest.get('requested_end_date')}",
            f"- actual_date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- rebalance_events: {calendar.get('rebalance_count')}",
            f"- tracked_methods: {', '.join(_texts(manifest.get('tracked_methods')))}",
            f"- data_quality: {data_quality.get('data_quality')}",
            f"- missing_symbols: {', '.join(_texts(data_quality.get('missing_symbols')))}",
            "- mode: BACKTEST_SIMULATION",
            "- not_pit_safe: true",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- broker_action_taken: false",
            "- production_effect: none",
            "",
            "该报告是 paper shadow research backfill，不是 PIT-safe production backtest，"
            "不能批准 official target weights 或 broker action。",
            "",
        ]
    )


def render_paper_shadow_rolling_eval_report(
    manifest: Mapping[str, Any],
    stability: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
) -> str:
    rows = _records(stability.get("methods"))
    best_average = _best_rank_stability_method(rows)
    stable = [row for row in rows if row.get("rank_stability_status") == "STABLE"]
    limited = _find_method(rows, "limited_adjustment")
    defensive = _find_method(rows, "defensive_limited_adjustment")
    consensus = _find_method(rows, "consensus_target")
    return "\n".join(
        [
            f"# Paper Shadow Rolling Evaluation {manifest.get('rolling_eval_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- window_count: {manifest.get('window_count')}",
            f"- metric_row_count: {len(metrics)}",
            f"- best_average_rank_method: {best_average}",
            f"- stable_methods: {', '.join(_texts([row.get('target_method') for row in stable]))}",
            f"- limited_adjustment_stability: {limited.get('rank_stability_status', 'MISSING')}",
            f"- defensive_limited_adjustment_stability: "
            f"{defensive.get('rank_stability_status', 'MISSING')}",
            f"- consensus_target_stability: {consensus.get('rank_stability_status', 'MISSING')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_regime_review_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    regimes = _records(summary.get("regimes"))
    by_name = {row.get("regime"): row for row in regimes}
    semiconductor_best = _mapping(by_name.get("semiconductor_pullback")).get(
        "best_return_method",
        "MISSING",
    )
    return "\n".join(
        [
            f"# Paper Shadow Regime Review {manifest.get('regime_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- ai_trend_best_return_method: "
            f"{_mapping(by_name.get('ai_trend')).get('best_return_method', 'MISSING')}",
            f"- tech_drawdown_best_return_method: "
            f"{_mapping(by_name.get('tech_drawdown')).get('best_return_method', 'MISSING')}",
            f"- semiconductor_pullback_best_return_method: {semiconductor_best}",
            f"- defensive_limited_adjustment_status: "
            f"{summary.get('defensive_limited_adjustment_status')}",
            "- no_auto_defensive_rule_approval: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_stability_report(
    manifest: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    turnover: Mapping[str, Any],
) -> str:
    methods = list(metrics)
    most_stable = _best_status_method(methods, "stability_status")
    highest_turnover = _max_field_method(_records(turnover.get("methods")), "annualized_turnover")
    jump_count = sum(int(_float(row.get("large_jump_count"))) for row in methods)
    consensus = _find_method(methods, "consensus_target")
    selected = _find_method(methods, "selected_top_candidate")
    return "\n".join(
        [
            f"# Paper Shadow Stability Diagnostics {manifest.get('stability_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- most_stable_method: {most_stable}",
            f"- highest_turnover_method: {highest_turnover}",
            f"- large_jump_count: {jump_count}",
            f"- consensus_target_stability: {consensus.get('stability_status', 'MISSING')}",
            f"- selected_top_candidate_stability: {selected.get('stability_status', 'MISSING')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_selection_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Checklist {decision.get('selection_review_id')}",
            "",
            "- 是否继续将 limited_adjustment 作为主 research target？",
            "- 是否保留 defensive_limited_adjustment 作为 secondary research method？",
            "- 是否将 consensus_target 保持为 reference-only？",
            "- 是否需要减少 target methods 数量？",
            "- 是否继续运行 paper shadow account？",
            "- 是否仍然禁止 broker / production？",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_system_target_selection_review_report(
    manifest: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    methods = _records(scorecard.get("methods"))
    best_return = _max_field_method(methods, "return_score")
    best_drawdown = _max_field_method(methods, "drawdown_score")
    best_stability = _max_field_method(methods, "stability_score")
    best_regime = _max_field_method(methods, "regime_score")
    secondary_methods = ", ".join(_texts(decision.get("secondary_research_methods")))
    reference_only_methods = ", ".join(_texts(decision.get("reference_only_methods")))
    return "\n".join(
        [
            f"# System Target Method Selection Review {manifest.get('selection_review_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status: {manifest.get('data_quality_status')}",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- secondary_methods: {secondary_methods}",
            f"- reference_only_methods: {reference_only_methods}",
            f"- decision_status: {decision.get('decision_status')}",
            f"- best_return_score_method: {best_return}",
            f"- best_drawdown_score_method: {best_drawdown}",
            f"- best_stability_score_method: {best_stability}",
            f"- best_regime_score_method: {best_regime}",
            "- official_target_weights_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            _text(decision.get("reason")),
            "",
        ]
    )


def render_selection_reader_brief(decision: Mapping[str, Any]) -> str:
    secondary_methods = ", ".join(_texts(decision.get("secondary_research_methods")))
    reference_only_methods = ", ".join(_texts(decision.get("reference_only_methods")))
    return "\n".join(
        [
            "## Dynamic Rescue System Target Selection Review",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- secondary_methods: {secondary_methods}",
            f"- reference_only_methods: {reference_only_methods}",
            f"- decision_status: {decision.get('decision_status')}",
            "- research_target_only: true",
            f"- next_action: {decision.get('next_action')}",
            "",
        ]
    )




def render_limited_instability_report(
    manifest: Mapping[str, Any],
    inventory: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    pattern: Mapping[str, Any],
) -> str:
    top_windows = sorted(
        inventory,
        key=lambda row: (
            {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(_text(row.get("severity")), 9),
            _float(row.get("relative_to_static_baseline")),
        ),
    )[:10]
    patterns = _records(pattern.get("patterns"))
    return "\n".join(
        [
            f"# Limited Adjustment Instability Diagnosis {manifest.get('instability_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- consistency_id: {manifest.get('consistency_id')}",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- rolling_consistency_status: {summary.get('rolling_consistency_status')}",
            f"- unstable_window_count: {summary.get('unstable_window_count')}",
            f"- dominant_failure_regime: {summary.get('dominant_failure_regime')}",
            f"- recommendation: {summary.get('recommendation')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Top Failure Patterns",
            "",
            *[
                "- "
                f"{row.get('pattern')}: windows={row.get('supporting_windows')}, "
                f"possible_fix={row.get('possible_fix')}"
                for row in patterns
            ],
            "",
            "## Unstable Windows",
            "",
            *[
                "- "
                f"{row.get('window_id')}: failure_type={row.get('failure_type')}, "
                f"severity={row.get('severity')}, "
                f"return_vs_static={row.get('relative_to_static_baseline')}, "
                f"drawdown_delta_vs_static={row.get('drawdown_delta_vs_static')}, "
                f"turnover={row.get('turnover')}, "
                f"regime_tags={','.join(_texts(row.get('regime_tags')))}"
                for row in top_windows
            ],
            "",
            "limited_adjustment rolling instability is research evidence only. "
            "It may motivate a risk cap or regime gate proposal, but it does not "
            "approve official target weights, broker action, or production mutation.",
            "",
        ]
    )


def render_limited_risk_attribution_report(
    manifest: Mapping[str, Any],
    return_contribution: Mapping[str, Any],
    drawdown_contribution: Mapping[str, Any],
    exposure: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> str:
    positive = ", ".join(_texts(return_contribution.get("top_positive_contributors")))
    drawdown_top = ", ".join(_texts(drawdown_contribution.get("top_drawdown_contributors")))
    event_types = sorted({_text(row.get("risk_worsening_type")) for row in events})
    return "\n".join(
        [
            f"# Limited Risk Attribution {manifest.get('risk_attribution_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- top_return_contributors: {positive}",
            f"- top_drawdown_contributors: {drawdown_top}",
            f"- avg_risk_asset_delta_vs_static: "
            f"{exposure.get('avg_risk_asset_delta_vs_static')}",
            f"- avg_semiconductor_delta_vs_static: "
            f"{exposure.get('avg_semiconductor_delta_vs_static')}",
            f"- avg_cash_delta_vs_static: {exposure.get('avg_cash_delta_vs_static')}",
            f"- risk_worsening_source: {exposure.get('risk_worsening_source')}",
            f"- risk_worsening_event_count: {len(events)}",
            f"- risk_worsening_event_types: {', '.join(event_types)}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Return Contribution By Symbol",
            "",
            *[
                "- "
                f"{row.get('symbol')}: avg_weight={row.get('avg_weight')}, "
                f"return_contribution={row.get('return_contribution')}, "
                "relative_contribution_vs_static="
                f"{row.get('relative_contribution_vs_static')}"
                for row in _records(return_contribution.get("symbols"))
            ],
            "",
            "## Drawdown Contribution By Symbol",
            "",
            *[
                "- "
                f"{row.get('symbol')}: drawdown_contribution="
                f"{row.get('drawdown_contribution')}, "
                f"weight_during_drawdown={row.get('weight_during_drawdown')}, "
                f"relative_to_static={row.get('relative_to_static')}"
                for row in _records(drawdown_contribution.get("symbols"))
            ],
            "",
            "该归因解释 RETURN_IMPROVES_RISK_WORSENS 的来源；它不实现 risk cap，"
            "也不改变任何 production weight 或 broker 状态。",
            "",
        ]
    )


def render_data_warning_repair_plan_report(
    manifest: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]],
    matrix: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Data Warning Repair Plan {manifest.get('repair_plan_id')}",
            "",
            f"- impact_id: {manifest.get('impact_id')}",
            f"- overall_data_warning_status: {matrix.get('overall_data_warning_status')}",
            f"- hardening_allowed_after_repair: {matrix.get('hardening_allowed_after_repair')}",
            "- auto_repair_executed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Warning Blocking Matrix",
            "",
            *[
                "- "
                f"{row.get('warning_id')}: blocks_hardening={row.get('blocks_hardening')}, "
                f"blocks_research={row.get('blocks_research')}, "
                f"blocks_production={row.get('blocks_production')}, "
                f"reason={row.get('reason')}"
                for row in _records(matrix.get("warnings"))
            ],
            "",
            "## Repair Actions",
            "",
            *[
                "- "
                f"{row.get('warning_id')}: type={row.get('warning_type')}, "
                f"severity={row.get('severity')}, "
                f"recommended_repair_action={row.get('recommended_repair_action')}, "
                f"expected_effect={row.get('expected_effect')}, "
                f"auto_repair_allowed={row.get('auto_repair_allowed')}"
                for row in actions
            ],
            "",
            "该 repair plan 只提出人工可审阅修复路径，不自动 refresh cache、"
            "repair manifest、rerun backfill 或修改 production state。",
            "",
        ]
    )


def render_alternative_method_review_report(
    manifest: Mapping[str, Any],
    candidates: Mapping[str, Any],
    scorecard: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Alternative Method Review {manifest.get('alt_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- risk_attribution_id: {manifest.get('risk_attribution_id')}",
            f"- instability_id: {manifest.get('instability_id')}",
            f"- recommended_alternative: {scorecard.get('recommended_alternative')}",
            f"- proposed_method_count: {len(_records(candidates.get('candidates')))}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Candidate Scorecard",
            "",
            *[
                "- "
                f"{row.get('method')}: status={row.get('current_status')}, "
                f"return={row.get('return_expectation')}, "
                f"risk={row.get('risk_expectation')}, "
                f"stability={row.get('stability_expectation')}, "
                f"recommendation={row.get('recommendation')}"
                for row in _records(scorecard.get("methods"))
            ],
            "",
            "## Proposed Candidates",
            "",
            *[
                "- "
                f"{row.get('method')}: status={row.get('status')}, "
                f"expected_benefit={row.get('expected_benefit')}, "
                f"expected_cost={row.get('expected_cost')}, "
                f"requires_new_implementation={row.get('requires_new_implementation')}"
                for row in _records(candidates.get("candidates"))
            ],
            "",
            "本报告只提出候选 research methods，不实现新 method，不改变 target weights。",
            "",
        ]
    )


def render_refined_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Refined Method Checklist {decision.get('proposal_id')}",
            "",
            "- 是否接受 limited_adjustment 暂不 harden？",
            "- 是否优先实现 risk_capped_limited_adjustment？",
            "- 是否优先实现 regime_gated_limited_adjustment？",
            "- 是否需要先修复 data warnings？",
            "- 是否继续保留 limited_adjustment 作为 secondary research method？",
            "- 是否确认所有新 method 仍为 research-only？",
            "- 是否确认 no broker / no production？",
            "",
            f"- recommended_next_step: {decision.get('recommended_next_step')}",
            f"- confidence: {decision.get('confidence')}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_refined_method_proposal_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    next_methods: Mapping[str, Any],
    instability: Mapping[str, Any],
    risk: Mapping[str, Any],
    repair: Mapping[str, Any],
    alt_review: Mapping[str, Any],
) -> str:
    instability_summary = _mapping(instability.get("instability_reason_summary"))
    exposure = _mapping(risk.get("exposure_shift_attribution"))
    matrix = _mapping(repair.get("warning_blocking_matrix"))
    scorecard = _mapping(alt_review.get("alternative_method_scorecard"))
    return "\n".join(
        [
            f"# Refined Research Method Proposal {manifest.get('proposal_id')}",
            "",
            f"- current_method: {decision.get('current_method')}",
            f"- current_hardening_status: {decision.get('current_hardening_status')}",
            f"- recommended_next_step: {decision.get('recommended_next_step')}",
            f"- reason: {decision.get('reason')}",
            f"- confidence: {decision.get('confidence')}",
            f"- limited_instability_recommendation: {instability_summary.get('recommendation')}",
            f"- dominant_failure_regime: {instability_summary.get('dominant_failure_regime')}",
            f"- risk_worsening_source: {exposure.get('risk_worsening_source')}",
            f"- hardening_allowed_after_repair: " f"{matrix.get('hardening_allowed_after_repair')}",
            f"- recommended_alternative: {scorecard.get('recommended_alternative')}",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Proposed Next Methods",
            "",
            *[
                "- "
                f"{row.get('method')}: priority={row.get('priority')}, "
                f"scope={row.get('implementation_scope')}, "
                f"expected_improvement={row.get('expected_improvement')}, "
                f"required_validation={','.join(_texts(row.get('required_validation')))}"
                for row in _records(next_methods.get("methods"))
            ],
            "",
            "limited_adjustment 当前不应 harden 为 primary research method。"
            "下一阶段如实现 refined method，也必须保持 research-only、"
            "no official target、no broker、no production。",
            "",
        ]
    )


def render_refined_method_reader_brief(
    decision: Mapping[str, Any],
    next_methods: Mapping[str, Any],
) -> str:
    methods = ", ".join(
        _texts([row.get("method") for row in _records(next_methods.get("methods"))])
    )
    return "\n".join(
        [
            "## Dynamic Rescue Refined Research Method Proposal",
            "",
            f"- current_method: {decision.get('current_method')}",
            f"- recommended_next_step: {decision.get('recommended_next_step')}",
            f"- proposed_next_methods: {methods}",
            f"- confidence: {decision.get('confidence')}",
            f"- research_target_only: {str(decision.get('research_target_only') is True).lower()}",
            f"- next_action: {decision.get('next_action')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_model_target_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    checks: Mapping[str, Any],
) -> str:
    lines = [
        f"# Research Model Target Portfolio {manifest.get('target_id')}",
        "",
        f"- as_of: {manifest.get('as_of')}",
        f"- generated_methods: {', '.join(_texts(manifest.get('generated_methods')))}",
        f"- recommended_research_method: {manifest.get('recommended_research_method')}",
        f"- constraint_status: {checks.get('overall_status')}",
        "- official_target_weights_written: false",
        "- broker_action_allowed: false",
        "- research_target_only: true",
        "",
        "## Method Weights",
        "",
    ]
    for row in rows:
        lines.append(
            f"- {row.get('target_method')}: {json.dumps(row.get('weights'), sort_keys=True)}"
        )
    lines.extend(
        [
            "",
            "该报告只生成 research model target weights，"
            "不写 official target weights，不触发 broker。",
            "",
        ]
    )
    return "\n".join(lines)


def render_paper_shadow_report(manifest: Mapping[str, Any], state: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Account {manifest.get('paper_shadow_id')}",
            "",
            f"- state_status: {state.get('state_status')}",
            f"- initial_equity: {state.get('initial_equity')}",
            f"- tracked_methods: {', '.join(_texts(state.get('tracked_methods')))}",
            "- broker_connected: false",
            "- real_trade_triggered: false",
            "- paper_shadow_only: true",
            "",
        ]
    )


def render_model_rebalance_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Model Target Paper Rebalance {manifest.get('rebalance_id')}",
            "",
            f"- paper_shadow_id: {manifest.get('paper_shadow_id')}",
            f"- target_id: {manifest.get('target_id')}",
            f"- total_turnover: {summary.get('total_turnover')}",
            f"- applied_methods: {', '.join(_texts(summary.get('applied_methods')))}",
            f"- skipped_methods: {', '.join(_texts(summary.get('skipped_methods')))}",
            "- insufficient_data_methods: "
            f"{', '.join(_texts(summary.get('insufficient_data_methods')))}",
            "- broker_action_taken: false",
            "",
            "## Events",
            "",
            *[
                f"- {event.get('target_method')}: {event.get('rebalance_status')} "
                f"turnover={event.get('turnover')}"
                for event in events
            ],
            "",
        ]
    )


def render_paper_shadow_performance_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    pairwise: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> str:
    limited = _find_method(_records(summary.get("methods")), "limited_adjustment")
    consensus = _find_method(_records(summary.get("methods")), "consensus_target")
    defensive = _find_method(_records(summary.get("methods")), "defensive_limited_adjustment")
    return "\n".join(
        [
            f"# Paper Shadow Performance {manifest.get('performance_id')}",
            "",
            f"- data_quality_status: {manifest.get('data_quality_status')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- best_risk_adjusted_method: {summary.get('best_risk_adjusted_method')}",
            "- limited_adjustment_vs_static_baseline: "
            f"{limited.get('relative_to_static_baseline')}",
            f"- defensive_limited_adjustment_max_drawdown: {defensive.get('max_drawdown')}",
            f"- consensus_target_max_drawdown: {consensus.get('max_drawdown')}",
            f"- pairwise_count: {len(_records(pairwise.get('comparisons')))}",
            f"- regime_count: {len(_records(regime.get('regimes')))}",
            "- broker_action_taken: false",
            "",
        ]
    )


def render_performance_reader_brief(summary: Mapping[str, Any]) -> str:
    limited = _find_method(_records(summary.get("methods")), "limited_adjustment")
    return "\n".join(
        [
            "## Dynamic Rescue System Target Portfolio",
            "",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- best_risk_adjusted_method: {summary.get('best_risk_adjusted_method')}",
            "- limited_adjustment_vs_static_baseline: "
            f"{limited.get('relative_to_static_baseline')}",
            "- research_target_only: true",
            "- broker_action_allowed: false",
            "",
        ]
    )


def render_owner_research_review_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Review Checklist {decision.get('review_id')}",
            "",
            "- 是否接受 limited_adjustment 继续作为主要 research target？",
            "- 是否接受 consensus_target 只作为 upper-bound reference？",
            "- 是否接受 defensive_limited_adjustment 仍为 research-only？",
            "- 是否继续运行 paper shadow account？",
            "- 是否需要调整 simulation / forward confirmation 重点？",
            "- 是否确认禁止 broker / production / order ticket？必须确认。",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_system_target_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    target: Mapping[str, Any],
    paper: Mapping[str, Any],
    performance: Mapping[str, Any],
) -> str:
    summary = _mapping(performance.get("method_performance_summary"))
    return "\n".join(
        [
            f"# System Target Portfolio Review {manifest.get('review_id')}",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            f"- target_id: {target.get('target_id')}",
            f"- paper_shadow_id: {paper.get('paper_shadow_id')}",
            f"- performance_id: {performance.get('performance_id')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            "",
            "收益最高 method 不会自动采用；review pack 只建议继续观察并等待 forward confirmation。",
            "consensus_target 仍只是 upper-bound reference；"
            "defensive_limited_adjustment 仍未批准为 production rule。",
            "当前输出不写 official target weights，不触发 broker，不生成 order ticket。",
            "",
        ]
    )


def render_system_target_reader_brief(
    decision: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue System Target Portfolio",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- research_target_only: true",
            "- broker_action_allowed: false",
            "- next_action: continue_paper_shadow_observation",
            "",
        ]
    )


def _latest_target_source(
    *,
    position_advisory_daily_dir: Path,
    shadow_monitor_dir: Path,
    shadow_shortlist_dir: Path,
    consensus_drift_dir: Path,
) -> dict[str, Any]:
    warnings: list[str] = []
    candidate_rows: list[dict[str, Any]] = []
    consensus_weights: dict[str, float] = {}
    summary: dict[str, Any] = {}
    daily_dir = _latest_child_dir_with(position_advisory_daily_dir, "daily_candidate_targets.jsonl")
    if daily_dir is not None:
        candidate_rows = _read_jsonl(daily_dir / "daily_candidate_targets.jsonl")
        consensus_weights = _read_consensus_weights_csv(daily_dir / "daily_consensus_weights.csv")
        actions = _read_optional_json(daily_dir / "daily_advisory_actions.json") or {}
        summary.update(
            {
                "source_daily_advisory_id": actions.get("daily_advisory_id", daily_dir.name),
                "source_daily_advisory_dir": str(daily_dir),
                "consensus_status": actions.get("consensus_status", ""),
            }
        )
    else:
        warnings.append("latest_position_advisory_daily_missing")
    monitor_dir = _latest_child_dir_with(shadow_monitor_dir, "shadow_candidate_daily_results.jsonl")
    monitor_manifest: dict[str, Any] = {}
    if monitor_dir is not None:
        monitor_manifest = _read_optional_json(monitor_dir / "shadow_monitor_manifest.json") or {}
        if not candidate_rows:
            candidate_rows = _read_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl")
        summary.update(
            {
                "source_shadow_monitor_run_id": monitor_manifest.get(
                    "monitor_run_id", monitor_dir.name
                ),
                "source_shadow_monitor_dir": str(monitor_dir),
            }
        )
    else:
        warnings.append("latest_shadow_monitor_missing")
    shortlist_dir = _latest_child_dir_with(shadow_shortlist_dir, "shadow_shortlist_manifest.json")
    shortlist_manifest: dict[str, Any] = {}
    if shortlist_dir is not None:
        shortlist_manifest = (
            _read_optional_json(shortlist_dir / "shadow_shortlist_manifest.json") or {}
        )
        summary.update(
            {
                "source_shadow_shortlist_id": shortlist_manifest.get(
                    "shadow_shortlist_id", shortlist_dir.name
                ),
                "source_shadow_shortlist_dir": str(shortlist_dir),
            }
        )
    else:
        warnings.append("latest_shadow_shortlist_missing")
    drift_dir = _latest_child_dir_with(consensus_drift_dir, "consensus_drift_summary.json")
    drift_summary: dict[str, Any] = {}
    if drift_dir is not None:
        drift_summary = _read_optional_json(drift_dir / "consensus_drift_summary.json") or {}
        summary.update(
            {
                "source_consensus_drift_id": drift_summary.get("drift_id", drift_dir.name),
                "disagreement_status": drift_summary.get("disagreement_status", ""),
            }
        )
    else:
        warnings.append("latest_consensus_drift_missing")
    if not candidate_rows:
        warnings.append("candidate_target_weights_missing")
    return {
        "candidate_targets": candidate_rows,
        "consensus_weights": consensus_weights,
        "shadow_shortlist_id": _text(shortlist_manifest.get("shadow_shortlist_id")),
        "shadow_monitor_run_id": _text(monitor_manifest.get("monitor_run_id")),
        "consensus_drift_id": _text(drift_summary.get("drift_id")),
        "summary": summary,
        "warnings": warnings,
    }


def _read_consensus_weights_csv(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path)
    if not {"symbol", "mean_target_weight"}.issubset(frame.columns):
        return {}
    return {
        str(row["symbol"]).strip().upper(): _float(row["mean_target_weight"])
        for _, row in frame.iterrows()
        if str(row["symbol"]).strip()
    }


def _average_candidate_weights(candidates: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    rows = [
        _mapping(row.get("target_weights"))
        for row in candidates
        if _mapping(row.get("target_weights"))
    ]
    if not rows:
        return {}
    symbols = sorted({symbol for row in rows for symbol in row})
    return _normalize_weights(
        {symbol: sum(_float(row.get(symbol)) for row in rows) / len(rows) for symbol in symbols}
    )


def _first_candidate_weights(candidates: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    if not candidates:
        return {}
    sorted_rows = sorted(
        candidates,
        key=lambda row: (
            _float(row.get("shortlist_rank"), 9999),
            -_float(row.get("shortlist_score")),
            str(row.get("candidate_id")),
        ),
    )
    return _mapping(sorted_rows[0].get("target_weights"))


def _limited_adjustment(
    *,
    baseline: Mapping[str, float],
    target: Mapping[str, float],
    max_total_adjustment: float,
    max_symbol_adjustment: float,
) -> dict[str, float]:
    base = _normalize_weights(baseline)
    desired = _normalize_weights(target)
    symbols = sorted(set(base) | set(desired))
    deltas = {
        symbol: max(
            -max_symbol_adjustment,
            min(max_symbol_adjustment, _float(desired.get(symbol)) - _float(base.get(symbol))),
        )
        for symbol in symbols
    }
    total_abs = sum(abs(value) for value in deltas.values())
    if total_abs > max_total_adjustment and total_abs > 0:
        scale = max_total_adjustment / total_abs
        deltas = {symbol: value * scale for symbol, value in deltas.items()}
    adjusted = {symbol: _float(base.get(symbol)) + deltas.get(symbol, 0.0) for symbol in symbols}
    return _normalize_weights(adjusted)


def _defensive_adjustment(
    weights: Mapping[str, float], policy: Mapping[str, Any]
) -> dict[str, float]:
    result = dict(_normalize_weights(weights))
    required = {
        "semiconductor_symbols",
        "growth_symbols",
        "semiconductor_reduction",
        "growth_reduction",
        "max_cash_weight",
    }
    if not required.issubset(policy):
        raise DynamicV3SystemTargetError("defensive_limited_adjustment policy is incomplete")
    semis = [str(item).upper() for item in policy.get("semiconductor_symbols", [])]
    growth_symbols = [str(item).upper() for item in policy.get("growth_symbols", [])]
    semiconductor_reduction = _float(policy.get("semiconductor_reduction"))
    growth_reduction = _float(policy.get("growth_reduction"))
    cash_room = max(0.0, _float(policy.get("max_cash_weight")) - _float(result.get("CASH")))
    moved = _reduce_symbols(result, semis, min(semiconductor_reduction, cash_room))
    cash_room -= moved
    moved += _reduce_symbols(result, growth_symbols, min(growth_reduction, cash_room))
    result["CASH"] = _float(result.get("CASH")) + moved
    return _normalize_weights(result)


def _reduce_symbols(weights: dict[str, float], symbols: Sequence[str], amount: float) -> float:
    if amount <= 0:
        return 0.0
    available = sum(max(0.0, _float(weights.get(symbol))) for symbol in symbols)
    if available <= 0:
        return 0.0
    moved = min(amount, available)
    for symbol in symbols:
        share = max(0.0, _float(weights.get(symbol))) / available
        weights[symbol] = max(0.0, _float(weights.get(symbol)) - moved * share)
    return moved


def _constraint_checks(
    *,
    target_id: str,
    rows: Sequence[Mapping[str, Any]],
    constraints: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for row in rows:
        weights = _normalize_weights(_mapping(row.get("weights")))
        violations: list[str] = []
        warnings: list[str] = []
        max_single = _float(constraints.get("max_single_symbol_weight"), 1.0)
        max_semi = _float(constraints.get("max_semiconductor_weight"), 1.0)
        min_cash = _float(constraints.get("min_cash_weight"), 0.0)
        max_risk = _float(constraints.get("max_total_risk_asset_weight"), 1.0)
        semiconductor_symbols = _texts(constraints.get("semiconductor_symbols")) or ["SMH", "SOXX"]
        defensive_symbols = set(_texts(constraints.get("defensive_symbols")) or ["CASH", "TLT"])
        if max(weights.values(), default=0.0) > max_single:
            violations.append("max_single_symbol_weight_exceeded")
        if sum(_float(weights.get(symbol)) for symbol in semiconductor_symbols) > max_semi:
            violations.append("max_semiconductor_weight_exceeded")
        if _float(weights.get("CASH")) < min_cash:
            violations.append("min_cash_weight_not_met")
        risk_weight = sum(
            value for symbol, value in weights.items() if symbol not in defensive_symbols
        )
        if risk_weight > max_risk:
            violations.append("max_total_risk_asset_weight_exceeded")
        if _float(weights.get("CASH")) < min_cash + 0.02:
            warnings.append("cash_near_minimum")
        checks.append(
            {
                "target_method": row.get("target_method"),
                "status": "FAIL" if violations else "PASS_WITH_WARNINGS" if warnings else "PASS",
                "warnings": warnings,
                "violations": violations,
            }
        )
    overall = (
        "FAIL"
        if any(row["status"] == "FAIL" for row in checks)
        else (
            "PASS_WITH_WARNINGS"
            if any(row["status"] == "PASS_WITH_WARNINGS" for row in checks)
            else "PASS"
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "target_id": target_id,
        "checks": checks,
        "overall_status": overall,
        **SYSTEM_TARGET_SAFETY,
    }


def _select_model_target_weights(
    rows: Sequence[Mapping[str, Any]],
    *,
    preferred_method: str,
) -> dict[str, Any]:
    for row in rows:
        if row.get("target_method") == preferred_method:
            return dict(row)
    return dict(rows[0]) if rows else {"target_method": "", "weights": {}}


def _run_data_quality_gate(
    *,
    price_cache_path: Path,
    rates_cache_path: Path,
    expected_symbols: Sequence[str],
    as_of: date,
) -> DataQualityReport:
    return validate_data_cache(
        prices_path=price_cache_path,
        rates_path=rates_cache_path,
        expected_price_tickers=list(expected_symbols),
        expected_rate_series=[],
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=price_cache_path.parent / "download_manifest.csv",
        secondary_prices_path=price_cache_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=False,
    )


def _load_price_returns(path: Path, symbols: Sequence[str], start: date) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if not {"date", "ticker", "adj_close"}.issubset(frame.columns):
        raise DynamicV3SystemTargetError("price cache must contain date,ticker,adj_close")
    frame = frame.loc[frame["ticker"].astype(str).isin(symbols)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()]
    frame = frame.loc[frame["date"].dt.date >= start]
    pivot = frame.pivot_table(
        index="date", columns="ticker", values="adj_close", aggfunc="last"
    ).sort_index()
    returns = pivot.pct_change().dropna(how="all").fillna(0.0)
    return returns


def _method_performance(
    row: Mapping[str, Any],
    returns: pd.DataFrame,
    turnover: float,
) -> dict[str, Any]:
    weights = _normalize_weights(_mapping(row.get("weights")))
    if returns.empty:
        return {
            "target_method": row.get("target_method"),
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": turnover,
            "relative_to_static_baseline": 0.0,
            "relative_to_no_trade": 0.0,
            "risk_adjusted_return_to_volatility": 0.0,
            "performance_status": "INSUFFICIENT_DATA",
        }
    series = pd.Series(0.0, index=returns.index)
    for symbol, weight in weights.items():
        if symbol == "CASH":
            continue
        if symbol in returns.columns:
            series = series + returns[symbol].fillna(0.0) * weight
    equity = (1.0 + series).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    periods = max(1, len(series))
    annualized = (
        float((1.0 + total_return) ** (252.0 / periods) - 1.0) if total_return > -1 else -1.0
    )
    volatility = float(series.std(ddof=0) * math.sqrt(252.0)) if periods > 1 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    risk_adjusted = annualized / volatility if volatility > 0 else annualized
    return {
        "target_method": row.get("target_method"),
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(max_drawdown, 10),
        "realized_volatility": round(volatility, 10),
        "turnover": round(turnover, 10),
        "relative_to_static_baseline": 0.0,
        "relative_to_no_trade": 0.0,
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "performance_status": "PASS",
    }


def _pairwise_comparisons(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            return_delta = _float(left.get("total_return")) - _float(right.get("total_return"))
            drawdown_delta = _float(left.get("max_drawdown")) - _float(right.get("max_drawdown"))
            turnover_delta = _float(left.get("turnover")) - _float(right.get("turnover"))
            if (
                left.get("performance_status") == "INSUFFICIENT_DATA"
                or right.get("performance_status") == "INSUFFICIENT_DATA"
            ):
                conclusion = "insufficient_data"
            elif return_delta > 0 and drawdown_delta >= 0:
                conclusion = "method_a_better"
            elif return_delta < 0 and drawdown_delta <= 0:
                conclusion = "method_b_better"
            else:
                conclusion = "mixed"
            comparisons.append(
                {
                    "method_a": left.get("target_method"),
                    "method_b": right.get("target_method"),
                    "return_delta": round(return_delta, 10),
                    "drawdown_delta": round(drawdown_delta, 10),
                    "turnover_delta": round(turnover_delta, 10),
                    "conclusion": conclusion,
                }
            )
    return {"schema_version": SCHEMA_VERSION, "comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _regime_breakdown(
    method_rows: Sequence[Mapping[str, Any]], returns: pd.DataFrame
) -> dict[str, Any]:
    regimes = []
    if returns.empty:
        return {"schema_version": SCHEMA_VERSION, "regimes": [], **SYSTEM_TARGET_SAFETY}
    labels = pd.Series("normal", index=returns.index)
    if "QQQ" in returns.columns:
        labels.loc[returns["QQQ"] <= PRESSURE_RETURN_THRESHOLD] = "tech_drawdown"
    if "SMH" in returns.columns:
        labels.loc[returns["SMH"] <= PRESSURE_RETURN_THRESHOLD] = "semiconductor_pressure"
    for regime in ("tech_drawdown", "semiconductor_pressure", "normal"):
        selected = returns.loc[labels == regime]
        methods = []
        for row in method_rows:
            perf = _method_performance(row, selected, 0.0)
            methods.append(
                {
                    "target_method": row.get("target_method"),
                    "return": perf["total_return"],
                    "max_drawdown": perf["max_drawdown"],
                    "relative_to_no_trade": 0.0,
                    "status": perf["performance_status"],
                }
            )
        no_trade = _find_method(methods, "no_trade_baseline").get("return", 0.0)
        for item in methods:
            item["relative_to_no_trade"] = round(_float(item.get("return")) - _float(no_trade), 10)
        regimes.append({"regime": regime, "methods": methods})
    return {"schema_version": SCHEMA_VERSION, "regimes": regimes, **SYSTEM_TARGET_SAFETY}


def _backfill_target_method_weights(config: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    source = _mapping(config.get("source"))
    model_config_path = _resolve_project_path(
        source.get("model_target_config"), DEFAULT_MODEL_TARGET_CONFIG_PATH
    )
    model_config = load_model_target_config(model_config_path)
    baseline = _config_baseline_weights(model_config)
    source_payload = _latest_target_source(
        position_advisory_daily_dir=_resolve_project_path(
            source.get("position_advisory_daily_dir"), DEFAULT_POSITION_ADVISORY_DAILY_DIR
        ),
        shadow_monitor_dir=_resolve_project_path(
            source.get("shadow_monitor_dir"), DEFAULT_SHADOW_MONITOR_RUN_DIR
        ),
        shadow_shortlist_dir=_resolve_project_path(
            source.get("shadow_shortlist_dir"), DEFAULT_SHADOW_SHORTLIST_DIR
        ),
        consensus_drift_dir=_resolve_project_path(
            source.get("consensus_drift_dir"), DEFAULT_CONSENSUS_DRIFT_DIR
        ),
    )
    candidates = source_payload["candidate_targets"]
    consensus = _normalize_weights(
        source_payload.get("consensus_weights")
        or _average_candidate_weights(candidates)
        or baseline
    )
    top_candidate = _normalize_weights(_first_candidate_weights(candidates) or consensus)
    equal_weight = _normalize_weights(_average_candidate_weights(candidates) or consensus)
    advisory_limits = _load_advisory_limits(
        _mapping(model_config.get("source")).get("position_advisory_config")
    )
    if not advisory_limits:
        advisory_limits = _load_advisory_limits(DEFAULT_POSITION_ADVISORY_CONFIG_PATH)
    limited = _limited_adjustment(
        baseline=baseline,
        target=consensus,
        max_total_adjustment=_float(advisory_limits.get("max_single_day_total_adjustment")),
        max_symbol_adjustment=_float(advisory_limits.get("max_single_symbol_adjustment")),
    )
    defensive = _defensive_adjustment(
        limited,
        _mapping(_mapping(model_config.get("method_policy")).get("defensive_limited_adjustment")),
    )
    risk_capped = _risk_capped_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        risk_config=_load_risk_capped_config_if_available(config),
        model_config=model_config,
        as_of=AI_AFTER_CHATGPT_START,
        regime_context="normal",
    )
    smoothed_config = _load_smoothed_config_if_available(config)
    smooth_3d = _smoothed_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        smoothed_config=smoothed_config,
        model_config=model_config,
        as_of=AI_AFTER_CHATGPT_START,
        variant_id="smooth_weights_3d",
        regime_context="normal",
    )
    smooth_5d = _smoothed_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        smoothed_config=smoothed_config,
        model_config=model_config,
        as_of=AI_AFTER_CHATGPT_START,
        variant_id="smooth_weights_5d",
        regime_context="normal",
    )
    return {
        "static_baseline": baseline,
        "no_trade_baseline": baseline,
        "consensus_target": consensus,
        "limited_adjustment": limited,
        "smooth_weights_3d_limited_adjustment": smooth_3d,
        "smooth_weights_5d_limited_adjustment": smooth_5d,
        "risk_capped_limited_adjustment": risk_capped,
        "defensive_limited_adjustment": defensive,
        "equal_weight_shadow_candidates": equal_weight,
        "selected_top_candidate": top_candidate,
    }


def _backfill_initial_weights(config: Mapping[str, Any]) -> dict[str, float]:
    source = _mapping(config.get("source"))
    paper_config_path = _resolve_project_path(
        source.get("paper_shadow_config"), DEFAULT_PAPER_SHADOW_CONFIG_PATH
    )
    paper_config = load_paper_shadow_config(paper_config_path)
    baseline = _mapping(paper_config.get("baseline")).get("static_weights")
    if isinstance(baseline, Mapping):
        return _normalize_weights(baseline)
    return _config_baseline_weights(
        load_model_target_config(
            _resolve_project_path(
                source.get("model_target_config"), DEFAULT_MODEL_TARGET_CONFIG_PATH
            )
        )
    )


def _load_price_pivot(path: Path, symbols: Sequence[str], start: date) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if not {"date", "ticker", "adj_close"}.issubset(frame.columns):
        raise DynamicV3SystemTargetError("price cache must contain date,ticker,adj_close")
    frame = frame.loc[frame["ticker"].astype(str).isin(symbols)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()]
    frame = frame.loc[frame["date"].dt.date >= start]
    pivot = frame.pivot_table(
        index="date", columns="ticker", values="adj_close", aggfunc="last"
    ).sort_index()
    return pivot.dropna(how="all")


def _latest_price_date(pivot: pd.DataFrame) -> date:
    if pivot.empty:
        raise DynamicV3SystemTargetError("price cache has no rows for requested symbols")
    return pivot.index[-1].date()


def _backfill_rebalance_dates(
    trading_dates: Sequence[date],
    *,
    frequency: str,
    rebalance_day: str,
    min_history_days: int,
) -> set[date]:
    if frequency.lower() != "weekly":
        raise DynamicV3SystemTargetError("paper shadow backfill only supports weekly rebalance")
    if not trading_dates:
        return set()
    weekday_map = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4}
    target_weekday = weekday_map.get(rebalance_day.upper(), 0)
    first_allowed = min(trading_dates) + timedelta(days=min_history_days)
    selected: set[date] = set()
    by_week: dict[tuple[int, int], list[date]] = {}
    for item in trading_dates:
        if item < first_allowed:
            continue
        iso = item.isocalendar()
        by_week.setdefault((iso.year, iso.week), []).append(item)
    for dates in by_week.values():
        on_or_after = [item for item in dates if item.weekday() >= target_weekday]
        selected.add(min(on_or_after or dates))
    return selected


def _portfolio_return(weights: Mapping[str, float], return_row: Mapping[str, Any]) -> float:
    value = 0.0
    for symbol, weight in weights.items():
        if symbol == "CASH":
            continue
        value += _float(weight) * _float(return_row.get(symbol))
    return value


def _drift_weights(
    weights: Mapping[str, float],
    return_row: Mapping[str, Any],
    portfolio_return: float,
) -> dict[str, float]:
    denominator = 1.0 + portfolio_return
    if denominator <= 0:
        return _normalize_weights(weights)
    drifted = {}
    for symbol, weight in weights.items():
        asset_return = 0.0 if symbol == "CASH" else _float(return_row.get(symbol))
        drifted[symbol] = _float(weight) * (1.0 + asset_return) / denominator
    return _normalize_weights(drifted)


def _backfill_data_quality_payload(
    *,
    backfill_id: str,
    start: date,
    end: date,
    pivot: pd.DataFrame,
    symbols: Sequence[str],
    quality: DataQualityReport,
) -> dict[str, Any]:
    missing_symbols = [symbol for symbol in symbols if symbol not in pivot.columns]
    missing_dates = [
        idx.date().isoformat()
        for idx, row in pivot.iterrows()
        if any(pd.isna(row.get(symbol)) for symbol in symbols if symbol in pivot.columns)
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "backfill_id": backfill_id,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "price_source_status": quality.status,
        "missing_price_dates": missing_dates,
        "missing_symbols": missing_symbols,
        "data_quality": quality.status,
        "data_quality_checked_at": quality.checked_at.isoformat(),
        **SYSTEM_TARGET_SAFETY,
    }


def _rolling_window_inventory(
    states: Sequence[Mapping[str, Any]],
    *,
    min_observations: int,
) -> list[dict[str, Any]]:
    dates = sorted({_coerce_date(row.get("date"), date(1970, 1, 1)) for row in states})
    dates = [item for item in dates if item >= AI_AFTER_CHATGPT_START]
    if not dates:
        return []
    windows: list[dict[str, Any]] = [
        {
            "window_id": f"full_{dates[0].isoformat()}_{dates[-1].isoformat()}",
            "window_type": "full",
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "observation_count": len(dates),
            "status": "PASS" if len(dates) >= min_observations else "INSUFFICIENT_DATA",
        }
    ]
    for year in sorted({item.year for item in dates}):
        year_dates = [item for item in dates if item.year == year]
        windows.append(
            {
                "window_id": f"yearly_{year}",
                "window_type": "yearly",
                "start_date": year_dates[0].isoformat(),
                "end_date": year_dates[-1].isoformat(),
                "observation_count": len(year_dates),
                "status": "PASS" if len(year_dates) >= min_observations else "INSUFFICIENT_DATA",
            }
        )
    date_index = pd.to_datetime([item.isoformat() for item in dates])
    month_starts = sorted({date(item.year, item.month, 1) for item in dates})
    for months in (3, 6, 12):
        for month_start in month_starts:
            window_end_ts = (
                pd.Timestamp(month_start) + pd.DateOffset(months=months) - pd.Timedelta(days=1)
            )
            selected = [
                item for item in date_index if pd.Timestamp(month_start) <= item <= window_end_ts
            ]
            if not selected:
                continue
            window_dates = [item.date() for item in selected]
            if window_dates[-1] > dates[-1]:
                continue
            windows.append(
                {
                    "window_id": (
                        f"rolling_{months}m_{window_dates[0].strftime('%Y_%m')}_"
                        f"{window_dates[-1].strftime('%Y_%m')}"
                    ),
                    "window_type": f"rolling_{months}m",
                    "start_date": window_dates[0].isoformat(),
                    "end_date": window_dates[-1].isoformat(),
                    "observation_count": len(window_dates),
                    "status": (
                        "PASS" if len(window_dates) >= min_observations else "INSUFFICIENT_DATA"
                    ),
                }
            )
    return windows


def _rolling_metrics_for_window(
    states: Sequence[Mapping[str, Any]],
    window: Mapping[str, Any],
    min_observations: int,
) -> list[dict[str, Any]]:
    start = _coerce_date(window.get("start_date"), date(1970, 1, 1))
    end = _coerce_date(window.get("end_date"), date(1970, 1, 1))
    rows = [
        row for row in states if start <= _coerce_date(row.get("date"), date(1970, 1, 1)) <= end
    ]
    results = []
    for method in sorted({str(row.get("target_method")) for row in rows}):
        method_rows = [row for row in rows if row.get("target_method") == method]
        metrics = _state_path_metrics(method_rows, min_observations=min_observations)
        results.append(
            {
                "window_id": window.get("window_id"),
                "window_type": window.get("window_type"),
                "start_date": window.get("start_date"),
                "end_date": window.get("end_date"),
                "target_method": method,
                **metrics,
                "relative_to_static_baseline": 0.0,
                "relative_to_no_trade_baseline": 0.0,
                "rank_by_return": 0,
                "rank_by_drawdown": 0,
                "rank_by_risk_adjusted": 0,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    static_return = _metric_for(results, "static_baseline", "total_return")
    no_trade_return = _metric_for(results, "no_trade_baseline", "total_return")
    for row in results:
        row["relative_to_static_baseline"] = round(
            _float(row.get("total_return")) - static_return, 10
        )
        row["relative_to_no_trade_baseline"] = round(
            _float(row.get("total_return")) - no_trade_return, 10
        )
    return results


def _state_path_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_observations: int,
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    if len(ordered) < min_observations:
        status = "INSUFFICIENT_DATA"
    else:
        status = "PASS"
    if len(ordered) < 2:
        return {
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": round(sum(_float(row.get("turnover")) for row in ordered), 10),
            "risk_adjusted_return_to_volatility": 0.0,
            "status": status,
        }
    start_value = _float(ordered[0].get("portfolio_value"), 1.0)
    end_value = _float(ordered[-1].get("portfolio_value"), start_value)
    total_return = end_value / start_value - 1.0 if start_value > 0 else 0.0
    daily_returns = [_float(row.get("daily_return")) for row in ordered]
    volatility = _stddev(daily_returns) * math.sqrt(252.0) if len(daily_returns) > 1 else 0.0
    annualized = _annualized_return(total_return, len(daily_returns))
    values = [_float(row.get("portfolio_value")) for row in ordered]
    peak = values[0] if values else 1.0
    drawdowns = []
    for value in values:
        peak = max(peak, value)
        drawdowns.append(value / peak - 1.0 if peak > 0 else 0.0)
    risk_adjusted = annualized / volatility if volatility > 0 else annualized
    return {
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(min(drawdowns or [0.0]), 10),
        "realized_volatility": round(volatility, 10),
        "turnover": round(sum(_float(row.get("turnover")) for row in ordered), 10),
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "status": status,
    }


def _rank_rolling_metrics(metrics: list[dict[str, Any]]) -> None:
    for window_id in sorted({str(row.get("window_id")) for row in metrics}):
        rows = [
            row
            for row in metrics
            if row.get("window_id") == window_id and row.get("status") != "INSUFFICIENT_DATA"
        ]
        _assign_rank(rows, "total_return", "rank_by_return", high=True)
        _assign_rank(rows, "max_drawdown", "rank_by_drawdown", high=True)
        _assign_rank(
            rows,
            "risk_adjusted_return_to_volatility",
            "rank_by_risk_adjusted",
            high=True,
        )


def _assign_rank(
    rows: Sequence[dict[str, Any]], field: str, rank_field: str, *, high: bool
) -> None:
    ordered = sorted(rows, key=lambda row: _float(row.get(field)), reverse=high)
    for rank, row in enumerate(ordered, start=1):
        row[rank_field] = rank


def _rolling_rank_stability(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    methods = sorted({str(row.get("target_method")) for row in metrics if row.get("target_method")})
    method_count = max(1, len(methods))
    rows = []
    for method in methods:
        selected = [
            row
            for row in metrics
            if row.get("target_method") == method and _float(row.get("rank_by_return")) > 0
        ]
        if not selected:
            rows.append(
                {
                    "target_method": method,
                    "avg_rank_return": 0.0,
                    "avg_rank_drawdown": 0.0,
                    "avg_rank_risk_adjusted": 0.0,
                    "top_3_frequency": 0.0,
                    "bottom_3_frequency": 0.0,
                    "rank_stability_status": "INSUFFICIENT_DATA",
                }
            )
            continue
        avg_return = sum(_float(row.get("rank_by_return")) for row in selected) / len(selected)
        avg_drawdown = sum(_float(row.get("rank_by_drawdown")) for row in selected) / len(selected)
        avg_risk = sum(_float(row.get("rank_by_risk_adjusted")) for row in selected) / len(selected)
        top_3 = sum(1 for row in selected if _float(row.get("rank_by_return")) <= 3) / len(selected)
        bottom_3 = sum(
            1 for row in selected if _float(row.get("rank_by_return")) > method_count - 3
        ) / len(selected)
        if top_3 >= 0.6 and bottom_3 <= 0.2:
            status = "STABLE"
        elif bottom_3 >= 0.5:
            status = "UNSTABLE"
        else:
            status = "MIXED"
        rows.append(
            {
                "target_method": method,
                "avg_rank_return": round(avg_return, 6),
                "avg_rank_drawdown": round(avg_drawdown, 6),
                "avg_rank_risk_adjusted": round(avg_risk, 6),
                "top_3_frequency": round(top_3, 6),
                "bottom_3_frequency": round(bottom_3, 6),
                "rank_stability_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "methods": rows, **SYSTEM_TARGET_SAFETY}


def _configured_regimes() -> tuple[str, ...]:
    return (
        "ai_trend",
        "tech_drawdown",
        "semiconductor_pullback",
        "risk_off",
        "sideways_choppy",
        "strong_recovery",
    )


def _regime_labels_from_states(
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, str]:
    static_rows = [row for row in states if row.get("target_method") == "static_baseline"]
    by_date = {str(row.get("date")): _float(row.get("daily_return")) for row in static_rows}
    policy = _mapping(config.get("regime_policy"))
    risk_off = _float(policy.get("risk_off_return_threshold"), -0.015)
    drawdown = _float(policy.get("tech_drawdown_return_threshold"), -0.01)
    semi = _float(policy.get("semiconductor_pullback_return_threshold"), -0.012)
    trend = _float(policy.get("ai_trend_return_threshold"), 0.008)
    recovery = _float(policy.get("strong_recovery_return_threshold"), 0.012)
    labels = {}
    for date_text, value in by_date.items():
        if value <= risk_off:
            label = "risk_off"
        elif value <= semi:
            label = "semiconductor_pullback"
        elif value <= drawdown:
            label = "tech_drawdown"
        elif value >= recovery:
            label = "strong_recovery"
        elif value >= trend:
            label = "ai_trend"
        else:
            label = "sideways_choppy"
        labels[date_text] = label
    return labels


def _regime_method_metrics(
    states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    min_sample: int,
) -> list[dict[str, Any]]:
    rows = []
    methods = sorted({str(row.get("target_method")) for row in states if row.get("target_method")})
    no_trade_by_regime: dict[str, dict[str, Any]] = {}
    for regime in _configured_regimes():
        date_set = {date_text for date_text, label in labels.items() if label == regime}
        for method in methods:
            selected = [
                row
                for row in states
                if row.get("target_method") == method and row.get("date") in date_set
            ]
            metrics = _sample_return_metrics(selected, min_sample=min_sample)
            item = {
                "regime": regime,
                "target_method": method,
                "sample_count": len(selected),
                "total_return": metrics["total_return"],
                "avg_return": metrics["avg_return"],
                "max_drawdown": metrics["max_drawdown"],
                "realized_volatility": metrics["realized_volatility"],
                "turnover": metrics["turnover"],
                "relative_to_static_baseline": 0.0,
                "relative_to_no_trade_baseline": 0.0,
                "win_rate_vs_no_trade": 0.0,
                "risk_adjusted_return_to_volatility": metrics["risk_adjusted_return_to_volatility"],
                "status": metrics["status"],
                **SYSTEM_TARGET_SAFETY,
            }
            rows.append(item)
            if method == "no_trade_baseline":
                no_trade_by_regime[regime] = item
    static_by_regime = {
        row["regime"]: row for row in rows if row.get("target_method") == "static_baseline"
    }
    for row in rows:
        regime = str(row.get("regime"))
        static = static_by_regime.get(regime, {})
        no_trade = no_trade_by_regime.get(regime, {})
        row["relative_to_static_baseline"] = round(
            _float(row.get("total_return")) - _float(static.get("total_return")),
            10,
        )
        row["relative_to_no_trade_baseline"] = round(
            _float(row.get("total_return")) - _float(no_trade.get("total_return")),
            10,
        )
        row["win_rate_vs_no_trade"] = _win_rate_vs_method(states, labels, row, "no_trade_baseline")
    return rows


def _sample_return_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_sample: int,
) -> dict[str, Any]:
    daily = [_float(row.get("daily_return")) for row in rows]
    if not daily:
        return {
            "total_return": 0.0,
            "avg_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": 0.0,
            "risk_adjusted_return_to_volatility": 0.0,
            "status": "INSUFFICIENT_DATA",
        }
    equity = 1.0
    peak = 1.0
    drawdowns = []
    for value in daily:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdowns.append(equity / peak - 1.0)
    total = equity - 1.0
    vol = _stddev(daily) * math.sqrt(252.0) if len(daily) > 1 else 0.0
    annualized = _annualized_return(total, len(daily))
    risk_adjusted = annualized / vol if vol > 0 else annualized
    return {
        "total_return": round(total, 10),
        "avg_return": round(sum(daily) / len(daily), 10),
        "max_drawdown": round(min(drawdowns or [0.0]), 10),
        "realized_volatility": round(vol, 10),
        "turnover": round(sum(_float(row.get("turnover")) for row in rows), 10),
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "status": "PASS" if len(daily) >= min_sample else "INSUFFICIENT_DATA",
    }


def _win_rate_vs_method(
    states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    row: Mapping[str, Any],
    benchmark_method: str,
) -> float:
    regime = str(row.get("regime"))
    method = str(row.get("target_method"))
    date_set = {date_text for date_text, label in labels.items() if label == regime}
    method_returns = {
        str(item.get("date")): _float(item.get("daily_return"))
        for item in states
        if item.get("target_method") == method and item.get("date") in date_set
    }
    benchmark_returns = {
        str(item.get("date")): _float(item.get("daily_return"))
        for item in states
        if item.get("target_method") == benchmark_method and item.get("date") in date_set
    }
    shared = sorted(set(method_returns) & set(benchmark_returns))
    if not shared:
        return 0.0
    wins = sum(
        1 for date_text in shared if method_returns[date_text] > benchmark_returns[date_text]
    )
    return round(wins / len(shared), 6)


def _regime_method_summary(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes = []
    defensive_statuses = []
    for regime in _configured_regimes():
        rows = [row for row in metrics if row.get("regime") == regime]
        valid = [row for row in rows if row.get("status") != "INSUFFICIENT_DATA"]
        sample_count = max((int(_float(row.get("sample_count"))) for row in rows), default=0)
        if valid:
            best_return = _best_metric_method(valid, "total_return", high=True)
            best_drawdown = _best_metric_method(valid, "max_drawdown", high=True)
            best_risk = _best_metric_method(
                valid,
                "risk_adjusted_return_to_volatility",
                high=True,
            )
        else:
            best_return = best_drawdown = best_risk = "INSUFFICIENT_DATA"
        defensive_status = _defensive_regime_status(rows)
        defensive_statuses.append(defensive_status)
        regimes.append(
            {
                "regime": regime,
                "best_return_method": best_return,
                "best_drawdown_method": best_drawdown,
                "best_risk_adjusted_method": best_risk,
                "defensive_limited_adjustment_status": defensive_status,
                "sample_count": sample_count,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if all(status == "INSUFFICIENT_DATA" for status in defensive_statuses):
        overall_defensive = "INSUFFICIENT_DATA"
    elif "FAIL" in defensive_statuses:
        overall_defensive = "MIXED"
    elif "MIXED" in defensive_statuses:
        overall_defensive = "MIXED"
    else:
        overall_defensive = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "regimes": regimes,
        "defensive_limited_adjustment_status": overall_defensive,
        **SYSTEM_TARGET_SAFETY,
    }


def _defensive_regime_status(rows: Sequence[Mapping[str, Any]]) -> str:
    defensive = _find_method(rows, "defensive_limited_adjustment")
    no_trade = _find_method(rows, "no_trade_baseline")
    if not defensive or defensive.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    better_return = _float(defensive.get("total_return")) >= _float(no_trade.get("total_return"))
    better_drawdown = _float(defensive.get("max_drawdown")) >= _float(no_trade.get("max_drawdown"))
    if better_return and better_drawdown:
        return "PASS"
    if better_return or better_drawdown:
        return "MIXED"
    return "FAIL"


def _stability_diagnostics(
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    policy = _mapping(config.get("stability_policy"))
    large_jump = _float(policy.get("large_jump_threshold"), 0.10)
    high_jump = _float(policy.get("high_jump_threshold"), 0.20)
    stable_max = _float(policy.get("stable_max_daily_weight_change"), 0.08)
    unstable_max = _float(policy.get("unstable_max_daily_weight_change"), 0.18)
    turnover_high = _float(policy.get("high_annualized_turnover"), 4.0)
    turnover_moderate = _float(policy.get("moderate_annualized_turnover"), 1.5)
    metrics: list[dict[str, Any]] = []
    jumps: list[dict[str, Any]] = []
    turnover_rows: list[dict[str, Any]] = []
    for method in sorted(
        {str(row.get("target_method")) for row in states if row.get("target_method")}
    ):
        rows = sorted(
            [row for row in states if row.get("target_method") == method],
            key=lambda row: _text(row.get("date")),
        )
        changes = []
        cash_weights = []
        risk_weights = []
        rebalance_turnovers = []
        previous: Mapping[str, Any] | None = None
        for row in rows:
            weights = _normalize_weights(_mapping(row.get("weights")))
            cash_weights.append(_float(weights.get("CASH")))
            risk_weights.append(sum(value for symbol, value in weights.items() if symbol != "CASH"))
            if row.get("rebalance_event") is True:
                rebalance_turnovers.append(_float(row.get("turnover")))
            if previous is not None:
                previous_weights = _normalize_weights(_mapping(previous.get("weights")))
                deltas = _weight_deltas(previous_weights, weights)
                total_abs = sum(abs(value) for value in deltas.values())
                changes.append(total_abs)
                if total_abs >= large_jump:
                    symbol, delta = max(deltas.items(), key=lambda item: abs(item[1]))
                    jumps.append(
                        {
                            "date": row.get("date"),
                            "target_method": method,
                            "total_abs_weight_change": round(total_abs, 10),
                            "largest_symbol_delta": {"symbol": symbol, "delta": round(delta, 10)},
                            "jump_reason": (
                                "target_method_rebalance"
                                if row.get("rebalance_event") is True
                                else "weight_drift"
                            ),
                            "severity": (
                                "HIGH"
                                if total_abs >= high_jump
                                else "MEDIUM" if total_abs >= large_jump else "LOW"
                            ),
                            "broker_action_taken": False,
                            **SYSTEM_TARGET_SAFETY,
                        }
                    )
            previous = row
        avg_change = sum(changes) / len(changes) if changes else 0.0
        max_change = max(changes or [0.0])
        if not rows:
            status = "INSUFFICIENT_DATA"
        elif (
            max_change <= stable_max and len([item for item in changes if item >= large_jump]) == 0
        ):
            status = "STABLE"
        elif max_change >= unstable_max:
            status = "UNSTABLE"
        else:
            status = "MODERATE"
        total_turnover = sum(rebalance_turnovers)
        years = max(1.0 / 252.0, len(rows) / 252.0)
        annualized_turnover = total_turnover / years
        if not rows:
            turnover_status = "INSUFFICIENT_DATA"
        elif annualized_turnover >= turnover_high:
            turnover_status = "HIGH"
        elif annualized_turnover >= turnover_moderate:
            turnover_status = "MODERATE"
        else:
            turnover_status = "LOW"
        method_metric = {
            "target_method": method,
            "avg_daily_weight_change": round(avg_change, 10),
            "max_daily_weight_change": round(max_change, 10),
            "avg_rebalance_turnover": round(
                sum(rebalance_turnovers) / len(rebalance_turnovers) if rebalance_turnovers else 0.0,
                10,
            ),
            "max_rebalance_turnover": round(max(rebalance_turnovers or [0.0]), 10),
            "rebalance_count": len(rebalance_turnovers),
            "large_jump_count": len([item for item in changes if item >= large_jump]),
            "cash_weight_volatility": round(_stddev(cash_weights), 10),
            "risk_asset_weight_volatility": round(_stddev(risk_weights), 10),
            "stability_status": status,
            **SYSTEM_TARGET_SAFETY,
        }
        metrics.append(method_metric)
        turnover_rows.append(
            {
                "target_method": method,
                "total_turnover": round(total_turnover, 10),
                "annualized_turnover": round(annualized_turnover, 10),
                "turnover_status": turnover_status,
                "warning": ["high_turnover"] if turnover_status == "HIGH" else [],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return (
        metrics,
        jumps,
        {"schema_version": SCHEMA_VERSION, "methods": turnover_rows, **SYSTEM_TARGET_SAFETY},
    )


def _selection_scorecard(
    rolling: Mapping[str, Any],
    regime: Mapping[str, Any],
    stability: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    rank_rows = _records(_mapping(rolling.get("rolling_rank_stability")).get("methods"))
    regime_summary = _records(_mapping(regime.get("regime_method_summary")).get("regimes"))
    stability_rows = _records(stability.get("method_stability_metrics"))
    turnover_rows = _records(_mapping(stability.get("turnover_diagnostics")).get("methods"))
    methods = sorted(
        {
            str(row.get("target_method"))
            for row in [*rank_rows, *stability_rows, *turnover_rows]
            if row.get("target_method")
        }
    )
    method_count = max(1, len(methods))
    policy = _mapping(config.get("selection_policy"))
    weights = _mapping(policy.get("score_weights"))
    return_weight = _float(weights.get("return"), 0.25)
    drawdown_weight = _float(weights.get("drawdown"), 0.25)
    risk_weight = _float(weights.get("risk_adjusted"), 0.20)
    regime_weight = _float(weights.get("regime"), 0.15)
    stability_weight = _float(weights.get("stability"), 0.15)
    turnover_high = _float(
        _mapping(config.get("stability_policy")).get("high_annualized_turnover"),
        4.0,
    )
    rows = []
    for method in methods:
        rank = _find_method(rank_rows, method)
        stability_row = _find_method(stability_rows, method)
        turnover = _find_method(turnover_rows, method)
        return_score = _rank_score(_float(rank.get("avg_rank_return")), method_count)
        drawdown_score = _rank_score(_float(rank.get("avg_rank_drawdown")), method_count)
        risk_score = _rank_score(_float(rank.get("avg_rank_risk_adjusted")), method_count)
        regime_score = _regime_score(regime_summary, method)
        stability_score = _stability_status_score(_text(stability_row.get("stability_status")))
        turnover_penalty = min(1.0, _float(turnover.get("annualized_turnover")) / turnover_high)
        overall = (
            return_score * return_weight
            + drawdown_score * drawdown_weight
            + risk_score * risk_weight
            + regime_score * regime_weight
            + stability_score * stability_weight
            - turnover_penalty * _float(weights.get("turnover_penalty"), 0.10)
        )
        if rank.get("rank_stability_status") == "INSUFFICIENT_DATA":
            status = "INSUFFICIENT_DATA"
        elif overall >= _float(policy.get("continue_observation_score"), 0.55):
            status = "CONTINUE_OBSERVATION"
        elif overall >= _float(policy.get("review_required_score"), 0.35):
            status = "REVIEW_REQUIRED"
        else:
            status = "NOT_RECOMMENDED"
        rows.append(
            {
                "target_method": method,
                "return_score": round(return_score, 6),
                "drawdown_score": round(drawdown_score, 6),
                "risk_adjusted_score": round(risk_score, 6),
                "regime_score": round(regime_score, 6),
                "stability_score": round(stability_score, 6),
                "turnover_penalty": round(turnover_penalty, 6),
                "overall_score": round(max(0.0, overall), 6),
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "methods": rows, **SYSTEM_TARGET_SAFETY}


def _selection_decision(
    scorecard: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("methods"))
    policy = _mapping(config.get("selection_policy"))
    preferred_order = _texts(policy.get("preferred_method_order")) or [
        "limited_adjustment",
        "defensive_limited_adjustment",
        "equal_weight_shadow_candidates",
        "consensus_target",
    ]
    eligible = [row for row in rows if row.get("status") != "INSUFFICIENT_DATA"]
    if not eligible:
        recommended = "INSUFFICIENT_DATA"
        decision_status = "INSUFFICIENT_DATA"
    else:
        best_score = max(_float(row.get("overall_score")) for row in eligible)
        tolerance = _float(policy.get("preferred_method_score_tolerance"), 0.10)
        preferred_candidates = [
            row
            for method in preferred_order
            for row in eligible
            if row.get("target_method") == method
            and _float(row.get("overall_score")) >= best_score - tolerance
            and row.get("status") != "NOT_RECOMMENDED"
        ]
        recommended_row = (
            preferred_candidates[0]
            if preferred_candidates
            else max(
                eligible,
                key=lambda row: _float(row.get("overall_score")),
            )
        )
        recommended = _text(recommended_row.get("target_method"))
        decision_status = (
            "CONTINUE_OBSERVATION"
            if recommended_row.get("status") == "CONTINUE_OBSERVATION"
            else "REVIEW_REQUIRED"
        )
    secondary = [
        _text(row.get("target_method"))
        for row in sorted(
            eligible, key=lambda item: _float(item.get("overall_score")), reverse=True
        )
        if row.get("target_method") != recommended and row.get("status") != "NOT_RECOMMENDED"
    ][:2]
    reference_only = _texts(policy.get("reference_only_methods")) or ["consensus_target"]
    reference_only = [method for method in reference_only if method != recommended]
    not_recommended = [
        _text(row.get("target_method")) for row in rows if row.get("status") == "NOT_RECOMMENDED"
    ]
    reason = (
        f"{recommended} is selected for continued research observation based on rolling rank, "
        "regime behavior, stability, and turnover diagnostics. The decision remains "
        "research-only and does not allow official target weights or broker action."
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "selection_review_id": "",
        "recommended_research_method": recommended,
        "secondary_research_methods": secondary,
        "reference_only_methods": reference_only,
        "not_recommended_methods": not_recommended,
        "decision_status": decision_status,
        "reason": reason,
        "next_action": "continue_paper_shadow_observation",
        **SYSTEM_TARGET_SAFETY,
    }


def _selection_attribution_rows(
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
    selection: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_rows = _records(scorecard.get("methods"))
    ordered = sorted(source_rows, key=lambda row: _float(row.get("overall_score")), reverse=True)
    recommended = _text(decision.get("recommended_research_method"))
    secondary = set(_texts(decision.get("secondary_research_methods")))
    reference_only = set(_texts(decision.get("reference_only_methods")))
    not_recommended = set(_texts(decision.get("not_recommended_methods")))
    component_fields = (
        "return_score",
        "drawdown_score",
        "risk_adjusted_score",
        "regime_score",
        "stability_score",
    )
    best_by_component = {field: _max_field_method(source_rows, field) for field in component_fields}
    data_quality_penalty = _data_quality_attribution_penalty(
        _text(selection.get("data_quality_status"))
    )
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(ordered, start=1):
        method = _text(row.get("target_method"))
        score_components = {
            "return_score": round(_float(row.get("return_score")), 6),
            "drawdown_score": round(_float(row.get("drawdown_score")), 6),
            "risk_adjusted_score": round(_float(row.get("risk_adjusted_score")), 6),
            "regime_score": round(_float(row.get("regime_score")), 6),
            "stability_score": round(_float(row.get("stability_score")), 6),
            "turnover_penalty": round(_float(row.get("turnover_penalty")), 6),
            "data_quality_penalty": round(data_quality_penalty, 6),
        }
        if method == recommended:
            selection_status = "recommended_research_method"
        elif method in secondary:
            selection_status = "secondary_research_method"
        elif method in reference_only:
            selection_status = "reference_only"
        elif method in not_recommended or row.get("status") == "NOT_RECOMMENDED":
            selection_status = "not_recommended"
        else:
            selection_status = "observed_method"
        selection_reasons = _selection_component_reasons(
            method=method,
            row=row,
            recommended=recommended,
            best_by_component=best_by_component,
            decision=decision,
            rank=rank,
        )
        rows.append(
            {
                "target_method": method,
                "overall_score": round(_float(row.get("overall_score")), 6),
                "score_components": score_components,
                "rank": rank,
                "selection_status": selection_status,
                "selection_reasons": selection_reasons,
                "weaknesses": _selection_component_weaknesses(row),
                "review_required_reasons": _selection_row_review_reasons(
                    method=method,
                    row=row,
                    decision=decision,
                    data_quality_status=_text(selection.get("data_quality_status")),
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _selection_component_reasons(
    *,
    method: str,
    row: Mapping[str, Any],
    recommended: str,
    best_by_component: Mapping[str, str],
    decision: Mapping[str, Any],
    rank: int,
) -> list[str]:
    reasons: list[str] = []
    if method == recommended:
        reasons.append("selected_by_research_method_policy")
        if rank > 1:
            reasons.append("preferred_method_within_selection_tolerance")
    for field, best_method in best_by_component.items():
        if best_method == method:
            reasons.append(f"best_{field}")
    if _float(row.get("turnover_penalty")) <= 0.0:
        reasons.append("no_turnover_penalty")
    if row.get("status") == "CONTINUE_OBSERVATION":
        reasons.append("score_status_continue_observation")
    elif row.get("status") == "REVIEW_REQUIRED":
        reasons.append("score_status_review_required")
    if method in set(_texts(decision.get("reference_only_methods"))):
        reasons.append("reference_only_policy")
    return reasons


def _selection_component_weaknesses(row: Mapping[str, Any]) -> list[str]:
    weaknesses: list[str] = []
    for field in (
        "return_score",
        "drawdown_score",
        "risk_adjusted_score",
        "regime_score",
        "stability_score",
    ):
        if _float(row.get(field)) < 0.5:
            weaknesses.append(f"{field}_below_midpoint")
    if _float(row.get("turnover_penalty")) >= 0.5:
        weaknesses.append("turnover_penalty_high")
    if row.get("status") == "NOT_RECOMMENDED":
        weaknesses.append("selection_score_not_recommended")
    return weaknesses


def _selection_row_review_reasons(
    *,
    method: str,
    row: Mapping[str, Any],
    decision: Mapping[str, Any],
    data_quality_status: str,
) -> list[str]:
    reasons: list[str] = []
    if data_quality_status == "PASS_WITH_WARNINGS":
        reasons.append("data_quality_pass_with_warnings")
    if data_quality_status == "FAIL":
        reasons.append("data_quality_failed")
    if row.get("status") == "REVIEW_REQUIRED":
        reasons.append("method_score_review_required")
    if method == decision.get("recommended_research_method") and (
        decision.get("decision_status") == "REVIEW_REQUIRED"
    ):
        reasons.append("forward_confirmation_missing")
    return reasons


def _data_quality_attribution_penalty(status: str) -> float:
    if status == "PASS_WITH_WARNINGS":
        return DATA_QUALITY_WARNING_ATTRIBUTION_PENALTY
    if status == "FAIL":
        return DATA_QUALITY_FAIL_ATTRIBUTION_PENALTY
    return 0.0


def _recommendation_reason_breakdown(
    rows: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    recommended = _text(decision.get("recommended_research_method"))
    recommended_row = _find_method(rows, recommended)
    top_method = _text(rows[0].get("target_method")) if rows else "INSUFFICIENT_DATA"
    primary_reason = (
        "top_overall_score"
        if recommended == top_method
        else "preferred_research_method_within_selection_tolerance"
    )
    evidence = [
        f"recommended={recommended}",
        f"recommended_rank={recommended_row.get('rank', 'MISSING')}",
        f"recommended_overall_score={recommended_row.get('overall_score', 'MISSING')}",
        f"top_overall_score_method={top_method}",
    ]
    primary = [
        {
            "reason": primary_reason,
            "evidence": evidence,
            "confidence": "MEDIUM" if recommended_row else "LOW",
        }
    ]
    if recommended_row:
        components = _mapping(recommended_row.get("score_components"))
        primary.append(
            {
                "reason": "balanced_return_risk_stability_review_candidate",
                "evidence": [
                    f"return_score={components.get('return_score')}",
                    f"drawdown_score={components.get('drawdown_score')}",
                    f"risk_adjusted_score={components.get('risk_adjusted_score')}",
                    f"stability_score={components.get('stability_score')}",
                    f"turnover_penalty={components.get('turnover_penalty')}",
                ],
                "confidence": "MEDIUM",
            }
        )
    return {
        "recommended_research_method": recommended,
        "primary_reasons": primary,
        "secondary_reasons": [
            {
                "reason": "research_only_safety_boundary_preserved",
                "evidence": [
                    "not_official_target_weights=true",
                    "broker_action_allowed=false",
                    "production_effect=none",
                ],
                "confidence": "HIGH",
            }
        ],
        "why_not_consensus_target": _why_not_method(rows, recommended, "consensus_target"),
        "why_not_defensive_limited_adjustment": _why_not_method(
            rows, recommended, "defensive_limited_adjustment"
        ),
        "why_not_static_baseline": _why_not_method(rows, recommended, "static_baseline"),
        "why_not_selected_top_candidate": _why_not_method(
            rows, recommended, "selected_top_candidate"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _why_not_method(
    rows: Sequence[Mapping[str, Any]],
    recommended: str,
    method: str,
) -> list[str]:
    row = _find_method(rows, method)
    recommended_row = _find_method(rows, recommended)
    if not row:
        return [f"{method}_not_available"]
    reasons: list[str] = []
    if row.get("selection_status") == "reference_only":
        reasons.append(f"{method}_configured_reference_only")
    if row.get("selection_status") == "not_recommended":
        reasons.append(f"{method}_selection_status_not_recommended")
    if _float(row.get("overall_score")) > _float(recommended_row.get("overall_score")):
        reasons.append("higher_overall_score_but_not_preferred_research_method")
    weaknesses = _texts(row.get("weaknesses"))
    if weaknesses:
        reasons.extend(weaknesses[:3])
    if not reasons:
        reasons.append(f"{method}_not_selected_by_research_policy")
    return reasons


def _review_required_reason_breakdown(
    selection: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    decision_status = _text(decision.get("decision_status"), "REVIEW_REQUIRED")
    data_quality = _text(selection.get("data_quality_status"))
    recommended = _text(decision.get("recommended_research_method"))
    recommended_row = _find_method(rows, recommended)
    reasons: list[dict[str, Any]] = []
    if data_quality == "PASS_WITH_WARNINGS":
        reasons.append(
            {
                "reason": "data_quality_pass_with_warnings",
                "severity": "WARNING",
                "blocking": False,
            }
        )
    elif data_quality == "FAIL":
        reasons.append(
            {
                "reason": "data_quality_failed",
                "severity": "BLOCKER",
                "blocking": True,
            }
        )
    if decision_status == "REVIEW_REQUIRED":
        reasons.append(
            {
                "reason": "forward_confirmation_missing",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if recommended_row.get("review_required_reasons"):
        reasons.append(
            {
                "reason": "recommended_method_requires_owner_review",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if recommended_row and _float(recommended_row.get("rank")) > 1:
        reasons.append(
            {
                "reason": "recommended_method_not_top_overall_score",
                "severity": "WARNING",
                "blocking": False,
            }
        )
    if not reasons:
        reasons.append(
            {
                "reason": "no_blocking_review_required_reason_detected",
                "severity": "INFO",
                "blocking": False,
            }
        )
    can_harden = not any(row.get("blocking") is True for row in reasons)
    return {
        "decision_status": decision_status,
        "review_required_reasons": reasons,
        "can_harden_research_method": can_harden,
        "can_trigger_official_target_weights": False,
        "can_trigger_production": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_long_window_risk_return(
    backfill: Mapping[str, Any],
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited = _method_path_metrics(states, "limited_adjustment")
    static = _method_path_metrics(states, "static_baseline")
    no_trade = _method_path_metrics(states, "no_trade_baseline")
    metrics = {
        "total_return": limited["total_return"],
        "annualized_return": limited["annualized_return"],
        "max_drawdown": limited["max_drawdown"],
        "realized_volatility": limited["realized_volatility"],
        "turnover": limited["turnover"],
        "relative_to_static_baseline": round(
            _float(limited.get("total_return")) - _float(static.get("total_return")),
            10,
        ),
        "relative_to_no_trade_baseline": round(
            _float(limited.get("total_return")) - _float(no_trade.get("total_return")),
            10,
        ),
    }
    return {
        "target_method": "limited_adjustment",
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "metrics": metrics,
        "risk_return_status": _risk_return_status(limited, static),
        "confidence": _long_window_confidence(
            observation_count=int(_float(limited.get("observation_count"))),
            data_quality_status=_text(backfill.get("data_quality_status")),
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _method_path_metrics(
    states: Sequence[Mapping[str, Any]],
    method: str,
) -> dict[str, Any]:
    rows = [row for row in states if row.get("target_method") == method]
    metrics = _state_path_metrics(rows, min_observations=2)
    metrics["observation_count"] = len(rows)
    return metrics


def _risk_return_status(
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> str:
    if not limited.get("observation_count") or not baseline.get("observation_count"):
        return "INSUFFICIENT_DATA"
    return_improves = _float(limited.get("total_return")) > _float(baseline.get("total_return"))
    risk_improves = _float(limited.get("max_drawdown")) >= _float(baseline.get("max_drawdown"))
    if return_improves and risk_improves:
        return "RETURN_IMPROVES_RISK_IMPROVES"
    if return_improves and not risk_improves:
        return "RETURN_IMPROVES_RISK_WORSENS"
    if not return_improves and risk_improves:
        return "RETURN_WORSE_RISK_IMPROVES"
    return "RETURN_WORSE_RISK_WORSE"


def _long_window_confidence(*, observation_count: int, data_quality_status: str) -> str:
    if (
        observation_count >= LONG_WINDOW_HIGH_CONFIDENCE_OBSERVATIONS
        and data_quality_status == "PASS"
    ):
        return "HIGH"
    if observation_count >= LONG_WINDOW_MEDIUM_CONFIDENCE_OBSERVATIONS and data_quality_status in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }:
        return "MEDIUM"
    return "LOW"


def _limited_vs_baseline_breakdown(
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited = _method_path_metrics(states, "limited_adjustment")
    comparisons = []
    for baseline_method in ("static_baseline", "no_trade_baseline"):
        baseline = _method_path_metrics(states, baseline_method)
        comparisons.append(_baseline_comparison(limited, baseline, baseline_method))
    return {"comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _baseline_comparison(
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
    baseline_method: str,
) -> dict[str, Any]:
    return_delta = round(
        _float(limited.get("total_return")) - _float(baseline.get("total_return")),
        10,
    )
    drawdown_delta = round(
        _float(limited.get("max_drawdown")) - _float(baseline.get("max_drawdown")),
        10,
    )
    volatility_delta = round(
        _float(limited.get("realized_volatility")) - _float(baseline.get("realized_volatility")),
        10,
    )
    turnover_delta = round(_float(limited.get("turnover")) - _float(baseline.get("turnover")), 10)
    return {
        "baseline": baseline_method,
        "return_delta": return_delta,
        "drawdown_delta": drawdown_delta,
        "volatility_delta": volatility_delta,
        "turnover_delta": turnover_delta,
        "conclusion": _comparison_conclusion(
            limited=limited,
            baseline=baseline,
            return_delta=return_delta,
            drawdown_delta=drawdown_delta,
            volatility_delta=volatility_delta,
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _comparison_conclusion(
    *,
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
    return_delta: float,
    drawdown_delta: float,
    volatility_delta: float,
) -> str:
    if not limited.get("observation_count") or not baseline.get("observation_count"):
        return "insufficient_data"
    risk_better = drawdown_delta >= 0.0 and volatility_delta <= 0.0
    risk_worse = drawdown_delta < 0.0 and volatility_delta > 0.0
    if return_delta > 0.0 and risk_better:
        return "limited_better"
    if return_delta <= 0.0 and risk_worse:
        return "baseline_better"
    return "mixed"


def _limited_exposure_path_analysis(
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited_rows = [row for row in states if row.get("target_method") == "limited_adjustment"]
    static_rows = [row for row in states if row.get("target_method") == "static_baseline"]
    limited_exposure = _exposure_summary(limited_rows)
    static_exposure = _exposure_summary(static_rows)
    avg_risk = _float(limited_exposure.get("avg_risk_asset_weight"))
    static_avg_risk = _float(static_exposure.get("avg_risk_asset_weight"))
    if not limited_rows:
        interpretation = "mixed"
    elif avg_risk > static_avg_risk + EXPOSURE_SIMILARITY_TOLERANCE:
        interpretation = "higher_risk_exposure"
    elif avg_risk < static_avg_risk - EXPOSURE_SIMILARITY_TOLERANCE:
        interpretation = "lower_risk_exposure"
    else:
        interpretation = "similar_risk_exposure"
    warnings: list[str] = []
    if interpretation == "higher_risk_exposure":
        warnings.append("limited_adjustment_higher_risk_asset_exposure")
    return {
        "target_method": "limited_adjustment",
        **limited_exposure,
        "risk_exposure_interpretation": interpretation,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _exposure_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    risk_weights: list[float] = []
    semiconductor_weights: list[float] = []
    cash_weights: list[float] = []
    for row in rows:
        weights = _normalize_weights(_mapping(row.get("weights")))
        risk_weights.append(sum(value for symbol, value in weights.items() if symbol != "CASH"))
        semiconductor_weights.append(sum(_float(weights.get(symbol)) for symbol in ("SMH", "SOXX")))
        cash_weights.append(_float(weights.get("CASH")))
    return {
        "avg_risk_asset_weight": round(_mean_float(risk_weights), 10),
        "max_risk_asset_weight": round(max(risk_weights or [0.0]), 10),
        "avg_semiconductor_weight": round(_mean_float(semiconductor_weights), 10),
        "max_semiconductor_weight": round(max(semiconductor_weights or [0.0]), 10),
        "avg_cash_weight": round(_mean_float(cash_weights), 10),
        "min_cash_weight": round(min(cash_weights or [0.0]), 10),
    }


def _latest_or_run_rolling_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    rolling_eval_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        rolling_eval_dir,
        "rolling_eval_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_rolling_eval_report_payload(
            rolling_eval_id=existing.name,
            output_dir=rolling_eval_dir,
        )
    run = run_paper_shadow_rolling_eval(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=rolling_eval_dir,
    )
    return paper_shadow_rolling_eval_report_payload(
        rolling_eval_id=run["rolling_eval_id"],
        output_dir=rolling_eval_dir,
    )


def _latest_or_run_regime_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    regime_review_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        regime_review_dir,
        "paper_shadow_regime_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_regime_review_report_payload(
            regime_review_id=existing.name,
            output_dir=regime_review_dir,
        )
    run = run_paper_shadow_regime_review(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=regime_review_dir,
    )
    return paper_shadow_regime_review_report_payload(
        regime_review_id=run["regime_review_id"],
        output_dir=regime_review_dir,
    )


def _latest_or_run_stability_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    stability_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        stability_dir,
        "paper_shadow_stability_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_stability_report_payload(
            stability_id=existing.name,
            output_dir=stability_dir,
        )
    run = run_paper_shadow_stability(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=stability_dir,
    )
    return paper_shadow_stability_report_payload(
        stability_id=run["stability_id"],
        output_dir=stability_dir,
    )


def _matching_child_artifact_dir(root: Path, manifest_name: str, backfill_id: str) -> Path | None:
    if not root.exists():
        return None
    candidates = sorted(
        [path for path in root.glob(f"*/{manifest_name}") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        payload = _read_optional_json(path) or {}
        if payload.get("backfill_id") == backfill_id:
            return path.parent
    return None


def _limited_rolling_consistency_summary(rolling: Mapping[str, Any]) -> dict[str, Any]:
    rank_rows = _records(_mapping(rolling.get("rolling_rank_stability")).get("methods"))
    metrics = _records(rolling.get("rolling_method_metrics"))
    limited_rank = _find_method(rank_rows, "limited_adjustment")
    limited_metrics = [
        row
        for row in metrics
        if row.get("target_method") == "limited_adjustment"
        and _float(row.get("rank_by_return")) > 0
    ]
    total = len({str(row.get("window_id")) for row in limited_metrics})
    top_risk = (
        sum(1 for row in limited_metrics if _float(row.get("rank_by_risk_adjusted")) <= 3) / total
        if total
        else 0.0
    )
    top_return = _float(limited_rank.get("top_3_frequency"))
    bottom = _float(limited_rank.get("bottom_3_frequency"))
    if not total:
        status = "INSUFFICIENT_DATA"
    elif top_return >= 0.6 and top_risk >= 0.6 and bottom <= 0.2:
        status = "STABLE"
    elif bottom >= 0.5:
        status = "UNSTABLE"
    else:
        status = "MIXED"
    return {
        "target_method": "limited_adjustment",
        "rolling_windows_total": total,
        "top_3_frequency_by_return": round(top_return, 6),
        "top_3_frequency_by_risk_adjusted": round(top_risk, 6),
        "bottom_3_frequency": round(bottom, 6),
        "avg_rank_return": round(_float(limited_rank.get("avg_rank_return")), 6),
        "avg_rank_risk_adjusted": round(_float(limited_rank.get("avg_rank_risk_adjusted")), 6),
        "rolling_consistency_status": status,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_regime_consistency_summary(regime: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _records(regime.get("method_regime_metrics"))
    rows: list[dict[str, Any]] = []
    for regime_name in _configured_regimes():
        regime_rows = [
            row
            for row in metrics
            if row.get("regime") == regime_name and row.get("status") != "INSUFFICIENT_DATA"
        ]
        limited = _find_method(
            [row for row in metrics if row.get("regime") == regime_name],
            "limited_adjustment",
        )
        rank = 0
        if regime_rows:
            ordered = sorted(
                regime_rows,
                key=lambda row: _float(row.get("total_return")),
                reverse=True,
            )
            for index, item in enumerate(ordered, start=1):
                if item.get("target_method") == "limited_adjustment":
                    rank = index
                    break
        status = _limited_regime_status(limited)
        rows.append(
            {
                "regime": regime_name,
                "relative_to_static_baseline": round(
                    _float(limited.get("relative_to_static_baseline")),
                    10,
                ),
                "relative_to_no_trade_baseline": round(
                    _float(limited.get("relative_to_no_trade_baseline")),
                    10,
                ),
                "rank": rank,
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    statuses = {str(row.get("status")) for row in rows}
    pressure_fail = any(
        row.get("status") == "FAIL"
        and row.get("regime") in {"tech_drawdown", "semiconductor_pullback", "risk_off"}
        for row in rows
    )
    if statuses == {"INSUFFICIENT_DATA"}:
        overall = "INSUFFICIENT_DATA"
    elif pressure_fail:
        overall = "WEAK_IN_PRESSURE"
    elif "FAIL" in statuses:
        overall = "REGIME_DEPENDENT"
    else:
        overall = "BROADLY_CONSISTENT"
    return {
        "target_method": "limited_adjustment",
        "regimes": rows,
        "regime_consistency_status": overall,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_regime_status(limited: Mapping[str, Any]) -> str:
    if not limited or limited.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    rel_static = _float(limited.get("relative_to_static_baseline"))
    rel_no_trade = _float(limited.get("relative_to_no_trade_baseline"))
    if rel_static >= 0.0 and rel_no_trade >= 0.0:
        return "PASS"
    if rel_static >= 0.0 or rel_no_trade >= 0.0:
        return "PASS_WITH_WARNINGS"
    return "FAIL"


def _limited_stability_consistency_summary(stability: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _find_method(
        _records(stability.get("method_stability_metrics")),
        "limited_adjustment",
    )
    turnover = _find_method(
        _records(_mapping(stability.get("turnover_diagnostics")).get("methods")),
        "limited_adjustment",
    )
    return {
        "target_method": "limited_adjustment",
        "avg_rebalance_turnover": round(_float(metrics.get("avg_rebalance_turnover")), 10),
        "max_rebalance_turnover": round(_float(metrics.get("max_rebalance_turnover")), 10),
        "large_jump_count": int(_float(metrics.get("large_jump_count"))),
        "stability_status": _text(metrics.get("stability_status"), "INSUFFICIENT_DATA"),
        "turnover_status": _text(turnover.get("turnover_status"), "INSUFFICIENT_DATA"),
        **SYSTEM_TARGET_SAFETY,
    }


def _data_warning_inventory(backfill: Mapping[str, Any]) -> dict[str, Any]:
    data_quality = _text(backfill.get("data_quality_status"), _text(backfill.get("data_quality")))
    quality_payload = _mapping(backfill.get("backfill_data_quality"))
    warnings: list[dict[str, Any]] = []
    for item in [
        *_records(quality_payload.get("warnings")),
        *_records(quality_payload.get("issues")),
    ]:
        severity = _text(item.get("severity"), "WARNING")
        if severity not in {"WARNING", "INFO"}:
            continue
        warnings.append(
            {
                "warning_id": _text(
                    item.get("warning_id"),
                    _text(item.get("code"), "data_quality_warning"),
                ),
                "severity": severity,
                "affected_symbols": _texts(item.get("affected_symbols")),
                "affected_dates": _texts(item.get("affected_dates")),
                "potential_metric_impact": _text(item.get("potential_metric_impact"), "UNKNOWN"),
            }
        )
    missing_symbols = _texts(quality_payload.get("missing_symbols"))
    missing_dates = _texts(quality_payload.get("missing_price_dates"))
    if missing_symbols:
        warnings.append(
            {
                "warning_id": "missing_symbols_present",
                "severity": "WARNING",
                "affected_symbols": missing_symbols,
                "affected_dates": [],
                "potential_metric_impact": "HIGH",
            }
        )
    if missing_dates:
        warnings.append(
            {
                "warning_id": "missing_price_dates_present",
                "severity": "WARNING",
                "affected_symbols": [],
                "affected_dates": missing_dates,
                "potential_metric_impact": "MEDIUM",
            }
        )
    if data_quality == "PASS_WITH_WARNINGS" and not warnings:
        warnings.append(
            {
                "warning_id": "pass_with_warnings_detail_unavailable",
                "severity": "WARNING",
                "affected_symbols": [],
                "affected_dates": [],
                "potential_metric_impact": "UNKNOWN",
            }
        )
    return {
        "backfill_id": backfill.get("backfill_id"),
        "data_quality": data_quality,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _affected_metrics_from_warnings(inventory: Mapping[str, Any]) -> dict[str, Any]:
    warnings = _records(inventory.get("warnings"))
    impact_levels = {_text(row.get("potential_metric_impact"), "UNKNOWN") for row in warnings}
    if not warnings:
        level = "LOW"
        affected: bool | None = False
        reason = "data_quality_pass_without_recorded_warning"
    elif "UNKNOWN" in impact_levels:
        level = "UNKNOWN"
        affected = None
        reason = "warning_detail_missing_or_unquantified"
    elif "HIGH" in impact_levels:
        level = "HIGH"
        affected = True
        reason = "high_potential_data_warning_impact"
    elif "MEDIUM" in impact_levels:
        level = "MEDIUM"
        affected = True
        reason = "medium_potential_data_warning_impact"
    else:
        level = "LOW"
        affected = False
        reason = "warnings_not_expected_to_move_core_metrics"
    return {
        "metrics": [
            {
                "metric": metric,
                "affected": affected,
                "impact_level": level,
                "reason": reason,
            }
            for metric in ("total_return", "max_drawdown", "realized_volatility", "turnover")
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _recommendation_sensitivity_to_warnings(
    selection: Mapping[str, Any],
    inventory: Mapping[str, Any],
    affected_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    data_quality = _text(inventory.get("data_quality"))
    metrics = _records(affected_metrics.get("metrics"))
    impact_levels = {_text(row.get("impact_level")) for row in metrics}
    warning_ids = _texts([row.get("warning_id") for row in _records(inventory.get("warnings"))])
    if data_quality == "FAIL":
        stability = "UNSTABLE"
        would_change: bool | None = True
        decision = "BLOCKED"
        blocking = ["data_quality_failed"]
    elif "UNKNOWN" in impact_levels:
        stability = "REVIEW_REQUIRED"
        would_change = None
        decision = "REVIEW_REQUIRED"
        blocking = ["warning_metric_impact_unknown"]
    elif data_quality == "PASS_WITH_WARNINGS" and {"HIGH", "MEDIUM"} & impact_levels:
        stability = "REVIEW_REQUIRED"
        would_change = None
        decision = "REVIEW_REQUIRED"
        blocking = ["warning_metric_impact_potentially_material"]
    else:
        stability = "STABLE"
        would_change = False
        decision = "ACCEPT_FOR_RESEARCH"
        blocking = []
    return {
        "recommended_research_method": _text(
            _mapping(selection.get("selection_decision")).get("recommended_research_method"),
            "limited_adjustment",
        ),
        "recommendation_stability": stability,
        "would_change_if_warnings_excluded": would_change,
        "warning_blocking_reasons": blocking,
        "warning_ids": warning_ids,
        "data_quality_decision": decision,
        **SYSTEM_TARGET_SAFETY,
    }


def _research_method_hardening_decision(
    attribution: Mapping[str, Any],
    risk: Mapping[str, Any],
    consistency: Mapping[str, Any],
    data_warning: Mapping[str, Any],
) -> dict[str, Any]:
    recommendation = _mapping(attribution.get("recommendation_reason_breakdown"))
    review = _mapping(attribution.get("review_required_reason_breakdown"))
    risk_return = _mapping(risk.get("long_window_risk_return"))
    rolling = _mapping(consistency.get("rolling_consistency_summary"))
    regime = _mapping(consistency.get("regime_consistency_summary"))
    stability = _mapping(consistency.get("stability_consistency_summary"))
    warning_sensitivity = _mapping(data_warning.get("recommendation_sensitivity_to_warnings"))
    recommended = _text(recommendation.get("recommended_research_method"), "MISSING")
    candidate = "limited_adjustment"
    limited_row = _find_method(
        _records(attribution.get("method_score_attribution")),
        candidate,
    )
    blocking_issues = [
        _text(row.get("reason"))
        for row in _records(review.get("review_required_reasons"))
        if row.get("blocking") is True
    ]
    warnings: list[str] = []
    if warning_sensitivity.get("data_quality_decision") != "ACCEPT_FOR_RESEARCH":
        blocking_issues.append(
            "data_quality_warning_impact_"
            + _text(warning_sensitivity.get("data_quality_decision"), "review_required").lower()
        )
    risk_status = _text(risk_return.get("risk_return_status"))
    if risk_status == "RETURN_WORSE_RISK_WORSE":
        blocking_issues.append("long_window_return_worse_and_risk_worse")
    elif risk_status == "RETURN_IMPROVES_RISK_WORSENS":
        warnings.append("long_window_return_improves_but_risk_worsens")
    rolling_status = _text(rolling.get("rolling_consistency_status"))
    regime_status = _text(regime.get("regime_consistency_status"))
    stability_status = _text(stability.get("stability_status"))
    if rolling_status == "UNSTABLE":
        blocking_issues.append("rolling_consistency_unstable")
    if regime_status == "WEAK_IN_PRESSURE":
        blocking_issues.append("weak_in_pressure_regimes")
    if stability_status == "UNSTABLE":
        blocking_issues.append("weight_path_unstable")
    if regime_status == "REGIME_DEPENDENT":
        warnings.append("regime_dependent_performance")
    if rolling_status == "MIXED":
        warnings.append("rolling_consistency_mixed")
    if recommended != candidate:
        blocking_issues.append("limited_adjustment_not_recommended_by_selection_review")
    blocking_issues = sorted(set(item for item in blocking_issues if item))
    warnings = sorted(set(item for item in warnings if item))
    if (
        "long_window_return_worse_and_risk_worse" in blocking_issues
        and "weight_path_unstable" in blocking_issues
    ):
        hardening_decision = "REJECT"
    elif blocking_issues:
        hardening_decision = "REVIEW_REQUIRED"
    elif warnings:
        hardening_decision = "CONTINUE_OBSERVATION"
    else:
        hardening_decision = "HARDEN_AS_PRIMARY_RESEARCH"
    confidence = (
        "LOW"
        if blocking_issues
        else (
            "MEDIUM"
            if warnings or warning_sensitivity.get("data_quality_decision") != "ACCEPT_FOR_RESEARCH"
            else "HIGH"
        )
    )
    reasons = [
        f"candidate_method={candidate}",
        f"selection_recommended_method={recommended}",
        f"risk_return_status={risk_status}",
        f"rolling_consistency_status={rolling_status}",
        f"regime_consistency_status={regime_status}",
        f"stability_status={stability_status}",
        f"data_quality_decision={warning_sensitivity.get('data_quality_decision')}",
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "hardening_id": "",
        "candidate_method": candidate,
        "current_status": _text(limited_row.get("selection_status"), "observed_method"),
        "hardening_decision": hardening_decision,
        "decision_confidence": confidence,
        "reasons": reasons,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": (
            "owner_review_required"
            if hardening_decision == "REVIEW_REQUIRED"
            else "continue_paper_shadow_observation"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _unstable_window_inventory(
    *,
    rolling: Sequence[Mapping[str, Any]],
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_window: dict[str, list[Mapping[str, Any]]] = {}
    for row in rolling:
        if row.get("status") == "INSUFFICIENT_DATA":
            continue
        by_window.setdefault(_text(row.get("window_id")), []).append(row)
    labels = _regime_labels_from_states(states, config)
    rows: list[dict[str, Any]] = []
    for window_id, window_rows in sorted(by_window.items()):
        limited = _find_method(window_rows, "limited_adjustment")
        if not limited:
            continue
        static = _find_method(window_rows, "static_baseline")
        no_trade = _find_method(window_rows, "no_trade_baseline")
        method_count = len({row.get("target_method") for row in window_rows})
        drawdown_delta = round(
            _float(limited.get("max_drawdown")) - _float(static.get("max_drawdown")),
            10,
        )
        risk_adjusted_delta = round(
            _float(limited.get("risk_adjusted_return_to_volatility"))
            - _float(static.get("risk_adjusted_return_to_volatility")),
            10,
        )
        weight_jump = _window_max_weight_jump(
            states,
            start=_coerce_date(limited.get("start_date"), AI_AFTER_CHATGPT_START),
            end=_coerce_date(limited.get("end_date"), AI_AFTER_CHATGPT_START),
            method="limited_adjustment",
        )
        reasons = _window_failure_reasons(
            limited=limited,
            drawdown_delta=drawdown_delta,
            risk_adjusted_delta=risk_adjusted_delta,
            method_count=method_count,
            weight_jump=weight_jump,
        )
        if not reasons:
            continue
        regime_tags = _window_regime_tags(
            labels,
            start=_coerce_date(limited.get("start_date"), AI_AFTER_CHATGPT_START),
            end=_coerce_date(limited.get("end_date"), AI_AFTER_CHATGPT_START),
        )
        rows.append(
            {
                "window_id": window_id,
                "window_type": limited.get("window_type"),
                "start_date": limited.get("start_date"),
                "end_date": limited.get("end_date"),
                "limited_adjustment_rank_return": int(_float(limited.get("rank_by_return"))),
                "limited_adjustment_rank_risk_adjusted": int(
                    _float(limited.get("rank_by_risk_adjusted"))
                ),
                "relative_to_static_baseline": round(
                    _float(limited.get("relative_to_static_baseline")),
                    10,
                ),
                "relative_to_no_trade_baseline": round(
                    _float(limited.get("relative_to_no_trade_baseline")),
                    10,
                ),
                "drawdown_delta_vs_static": drawdown_delta,
                "risk_adjusted_delta_vs_static": risk_adjusted_delta,
                "turnover": round(_float(limited.get("turnover")), 10),
                "max_weight_jump": weight_jump,
                "regime_tags": regime_tags,
                "failure_reasons": reasons,
                "failure_type": _window_failure_type(reasons),
                "severity": _window_failure_severity(
                    reasons=reasons,
                    return_delta=_float(limited.get("relative_to_static_baseline")),
                    drawdown_delta=drawdown_delta,
                ),
                "static_total_return": static.get("total_return"),
                "no_trade_total_return": no_trade.get("total_return"),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _window_failure_reasons(
    *,
    limited: Mapping[str, Any],
    drawdown_delta: float,
    risk_adjusted_delta: float,
    method_count: int,
    weight_jump: float,
) -> list[str]:
    reasons: list[str] = []
    if (
        _float(limited.get("relative_to_static_baseline"))
        < INSTABILITY_RETURN_UNDERPERFORMANCE_TOLERANCE
        or _float(limited.get("relative_to_no_trade_baseline"))
        < INSTABILITY_RETURN_UNDERPERFORMANCE_TOLERANCE
    ):
        reasons.append("return_underperformance")
    if drawdown_delta < INSTABILITY_DRAWDOWN_WORSE_TOLERANCE:
        reasons.append("drawdown_worse")
    if risk_adjusted_delta < 0.0 or _float(limited.get("rank_by_risk_adjusted")) > max(
        1, method_count - 2
    ):
        reasons.append("risk_adjusted_worse")
    if _float(limited.get("turnover")) >= INSTABILITY_TURNOVER_HIGH_THRESHOLD:
        reasons.append("turnover_high")
    if weight_jump >= INSTABILITY_WEIGHT_JUMP_THRESHOLD:
        reasons.append("weight_jump_linked")
    return sorted(set(reasons))


def _window_failure_type(reasons: Sequence[str]) -> str:
    priority = (
        "return_underperformance",
        "drawdown_worse",
        "risk_adjusted_worse",
        "turnover_high",
    )
    material = [reason for reason in reasons if reason in priority]
    if len(material) == 1:
        return material[0]
    if len(material) > 1:
        return "mixed"
    return "mixed" if reasons else "mixed"


def _window_failure_severity(
    *,
    reasons: Sequence[str],
    return_delta: float,
    drawdown_delta: float,
) -> str:
    material_count = len([reason for reason in reasons if reason != "weight_jump_linked"])
    if (
        material_count >= 2
        or return_delta <= INSTABILITY_HIGH_SEVERITY_RETURN_DELTA
        or drawdown_delta <= INSTABILITY_HIGH_SEVERITY_DRAWDOWN_DELTA
    ):
        return "HIGH"
    if material_count == 1:
        return "MEDIUM"
    return "LOW"


def _window_regime_tags(
    labels: Mapping[str, str],
    *,
    start: date,
    end: date,
) -> list[str]:
    counts: dict[str, int] = {}
    for day_text, label in labels.items():
        day = _coerce_date(day_text, date(1970, 1, 1))
        if start <= day <= end:
            counts[label] = counts.get(label, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [name for name, _count in ordered[:3]] or ["unknown"]


def _window_max_weight_jump(
    states: Sequence[Mapping[str, Any]],
    *,
    start: date,
    end: date,
    method: str,
) -> float:
    selected = sorted(
        [
            row
            for row in states
            if row.get("target_method") == method
            and start <= _coerce_date(row.get("date"), date(1970, 1, 1)) <= end
        ],
        key=lambda row: _text(row.get("date")),
    )
    max_jump = 0.0
    previous: dict[str, Any] | None = None
    for row in selected:
        weights = _mapping(row.get("weights"))
        if previous is not None:
            max_jump = max(
                max_jump,
                max(
                    (
                        abs(_float(weights.get(symbol)) - _float(previous.get(symbol)))
                        for symbol in set(weights) | set(previous)
                    ),
                    default=0.0,
                ),
            )
        previous = weights
    return round(max_jump, 10)


def _instability_reason_summary(
    consistency: Mapping[str, Any],
    inventory: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    for row in inventory:
        for reason in _texts(row.get("failure_reasons")):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for regime in _texts(row.get("regime_tags")):
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
    total = len(inventory)
    top_reasons = [
        {
            "reason": reason,
            "count": count,
            "share": round(count / total, 6) if total else 0.0,
        }
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    dominant_regime = _dominant_failure_regime(regime_counts)
    if total == 0:
        recommendation = "insufficient_data"
    elif dominant_regime in {"tech_drawdown", "semiconductor_pullback", "risk_off"}:
        recommendation = "consider_regime_gate"
    elif any(row["reason"] == "drawdown_worse" for row in top_reasons):
        recommendation = "consider_risk_cap"
    else:
        recommendation = "continue_diagnosis"
    rolling = _mapping(consistency.get("rolling_consistency_summary"))
    return {
        "target_method": "limited_adjustment",
        "rolling_consistency_status": _text(
            rolling.get("rolling_consistency_status"),
            "INSUFFICIENT_DATA",
        ),
        "unstable_window_count": total,
        "top_reasons": top_reasons,
        "dominant_failure_regime": dominant_regime,
        "recommendation": recommendation,
        **SYSTEM_TARGET_SAFETY,
    }


def _dominant_failure_regime(regime_counts: Mapping[str, int]) -> str:
    if not regime_counts:
        return "unknown"
    normalized = {
        "semiconductor_pressure": "semiconductor_pullback",
        "normal": "sideways_choppy",
    }
    ranked = sorted(regime_counts.items(), key=lambda item: (-item[1], item[0]))
    return normalized.get(ranked[0][0], ranked[0][0])


def _rolling_failure_pattern(inventory: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pressure = [
        row
        for row in inventory
        if {"tech_drawdown", "semiconductor_pullback", "risk_off"}
        & set(_texts(row.get("regime_tags")))
    ]
    return_good_drawdown_worse = [
        row
        for row in inventory
        if _float(row.get("relative_to_static_baseline")) > 0
        and _float(row.get("drawdown_delta_vs_static")) < INSTABILITY_DRAWDOWN_WORSE_TOLERANCE
    ]
    turnover = [
        row for row in inventory if "turnover_high" in set(_texts(row.get("failure_reasons")))
    ]
    jumps = [
        row for row in inventory if "weight_jump_linked" in set(_texts(row.get("failure_reasons")))
    ]
    patterns = [
        {
            "pattern": "underperforms_in_pressure_regime",
            "supporting_windows": len(pressure),
            "description": (
                "limited_adjustment tends to underperform when tech, semiconductor, "
                "or risk-off pressure is active."
            ),
            "possible_fix": "regime_gate_risk_increase",
        },
        {
            "pattern": "return_good_but_drawdown_worse",
            "supporting_windows": len(return_good_drawdown_worse),
            "description": "limited_adjustment can improve return while worsening drawdown.",
            "possible_fix": "risk_cap_or_drawdown_guard",
        },
        {
            "pattern": "turnover_or_weight_jump_linked",
            "supporting_windows": len(turnover) + len(jumps),
            "description": "Some weak windows coincide with higher turnover or weight jumps.",
            "possible_fix": "lower_turnover_or_smoother_adjustment",
        },
    ]
    return {"patterns": patterns, **SYSTEM_TARGET_SAFETY}


def _backfill_symbol_returns(backfill: Mapping[str, Any]) -> pd.DataFrame:
    symbols = _backfill_non_cash_symbols(backfill)
    if not symbols:
        return pd.DataFrame()
    price_cache_path, _ = _backfill_cache_paths(backfill)
    if not price_cache_path.exists():
        return pd.DataFrame()
    start = _coerce_date(backfill.get("date_start"), AI_AFTER_CHATGPT_START)
    end = _coerce_date(backfill.get("date_end"), date.max)
    try:
        pivot = _load_price_pivot(price_cache_path, symbols, start)
    except DynamicV3SystemTargetError:
        return pd.DataFrame()
    pivot = pivot.loc[pivot.index.date <= end]
    returns = pivot.pct_change().fillna(0.0)
    returns.index = [idx.date().isoformat() for idx in returns.index]
    return returns


def _backfill_non_cash_symbols(backfill: Mapping[str, Any]) -> list[str]:
    states = _records(backfill.get("backfill_method_states"))
    return sorted(
        {symbol for row in states for symbol in _mapping(row.get("weights")) if symbol != "CASH"}
    )


def _backfill_cache_paths(backfill: Mapping[str, Any]) -> tuple[Path, Path]:
    config = _load_backfill_config_from_manifest(backfill)
    source = _mapping(config.get("source"))
    price_cache_path = _resolve_project_path(
        source.get("price_cache_path"),
        DEFAULT_PRICE_CACHE_PATH,
    )
    if source.get("rates_cache_path"):
        rates_cache_path = _resolve_project_path(
            source.get("rates_cache_path"),
            DEFAULT_RATES_CACHE_PATH,
        )
    else:
        sibling_rates = price_cache_path.with_name(DEFAULT_RATES_CACHE_PATH.name)
        rates_cache_path = sibling_rates if sibling_rates.exists() else DEFAULT_RATES_CACHE_PATH
    return price_cache_path, rates_cache_path


def _return_contribution_by_symbol(
    states: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
) -> dict[str, Any]:
    limited_rows = _method_state_rows(states, "limited_adjustment")
    static_rows = _method_state_rows(states, "static_baseline")
    limited = _symbol_contribution(limited_rows, returns)
    static = _symbol_contribution(static_rows, returns)
    symbols = sorted(set(limited) | set(static) | set(returns.columns if not returns.empty else []))
    rows = [
        {
            "symbol": symbol,
            "avg_weight": round(_avg_symbol_weight(limited_rows, symbol), 10),
            "return_contribution": round(_float(limited.get(symbol)), 10),
            "relative_contribution_vs_static": round(
                _float(limited.get(symbol)) - _float(static.get(symbol)),
                10,
            ),
        }
        for symbol in symbols
    ]
    positives = [
        row["symbol"]
        for row in sorted(
            rows,
            key=lambda row: _float(row.get("relative_contribution_vs_static")),
            reverse=True,
        )
        if _float(row.get("relative_contribution_vs_static")) > 0
    ][:3]
    negatives = [
        row["symbol"]
        for row in sorted(rows, key=lambda row: _float(row.get("relative_contribution_vs_static")))
        if _float(row.get("relative_contribution_vs_static")) < 0
    ][:3]
    return {
        "target_method": "limited_adjustment",
        "symbols": rows,
        "top_positive_contributors": positives,
        "top_negative_contributors": negatives,
        **SYSTEM_TARGET_SAFETY,
    }


def _drawdown_contribution_by_symbol(
    states: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
) -> dict[str, Any]:
    limited_rows = _method_state_rows(states, "limited_adjustment")
    static_rows = _method_state_rows(states, "static_baseline")
    start, end = _max_drawdown_window(limited_rows)
    limited_window = _rows_between(limited_rows, start, end)
    static_window = _rows_between(static_rows, start, end)
    limited = _symbol_contribution(limited_window, returns)
    static = _symbol_contribution(static_window, returns)
    symbols = sorted(set(limited) | set(static) | set(returns.columns if not returns.empty else []))
    rows = [
        {
            "symbol": symbol,
            "drawdown_contribution": round(_float(limited.get(symbol)), 10),
            "weight_during_drawdown": round(_avg_symbol_weight(limited_window, symbol), 10),
            "relative_to_static": round(
                _float(limited.get(symbol)) - _float(static.get(symbol)),
                10,
            ),
        }
        for symbol in symbols
    ]
    top = [
        row["symbol"]
        for row in sorted(rows, key=lambda row: _float(row.get("drawdown_contribution")))
        if _float(row.get("drawdown_contribution")) < 0
    ][:3]
    return {
        "target_method": "limited_adjustment",
        "max_drawdown_window": {
            "start_date": start.isoformat() if start != date.max else "",
            "end_date": end.isoformat() if end != date.min else "",
        },
        "symbols": rows,
        "top_drawdown_contributors": top,
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_exposure_shift_attribution(states: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    limited = _exposure_summary(_method_state_rows(states, "limited_adjustment"))
    static = _exposure_summary(_method_state_rows(states, "static_baseline"))
    risk_delta = round(
        _float(limited.get("avg_risk_asset_weight")) - _float(static.get("avg_risk_asset_weight")),
        10,
    )
    semi_delta = round(
        _float(limited.get("avg_semiconductor_weight"))
        - _float(static.get("avg_semiconductor_weight")),
        10,
    )
    cash_delta = round(
        _float(limited.get("avg_cash_weight")) - _float(static.get("avg_cash_weight")),
        10,
    )
    sources = []
    if semi_delta > EXPOSURE_SIMILARITY_TOLERANCE:
        sources.append("higher_semiconductor_exposure")
    if risk_delta > EXPOSURE_SIMILARITY_TOLERANCE:
        sources.append("higher_risk_asset_exposure")
    if cash_delta < -EXPOSURE_SIMILARITY_TOLERANCE:
        sources.append("lower_cash")
    if len(sources) > 1:
        source = "mixed"
    elif sources:
        source = sources[0]
    else:
        source = "unknown"
    return {
        "target_method": "limited_adjustment",
        "avg_risk_asset_weight": limited.get("avg_risk_asset_weight"),
        "avg_risk_asset_delta_vs_static": risk_delta,
        "avg_semiconductor_weight": limited.get("avg_semiconductor_weight"),
        "avg_semiconductor_delta_vs_static": semi_delta,
        "avg_cash_weight": limited.get("avg_cash_weight"),
        "avg_cash_delta_vs_static": cash_delta,
        "risk_worsening_source": source,
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_worsening_events(states: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    limited = _method_state_rows(states, "limited_adjustment")
    static = _method_state_rows(states, "static_baseline")
    static_by_date = {row.get("date"): row for row in static}
    rows: list[dict[str, Any]] = []
    for index in range(RISK_WORSENING_EVENT_WINDOW_DAYS - 1, len(limited)):
        window = limited[index - RISK_WORSENING_EVENT_WINDOW_DAYS + 1 : index + 1]
        static_window = [
            static_by_date.get(row.get("date"), {})
            for row in window
            if row.get("date") in static_by_date
        ]
        if len(static_window) < RISK_WORSENING_EVENT_WINDOW_DAYS:
            continue
        limited_returns = [_float(row.get("daily_return")) for row in window]
        static_returns = [_float(row.get("daily_return")) for row in static_window]
        drawdown_delta = round(
            _drawdown_from_returns(limited_returns) - _drawdown_from_returns(static_returns),
            10,
        )
        vol_delta = round(
            _stddev(limited_returns) * math.sqrt(252.0)
            - _stddev(static_returns) * math.sqrt(252.0),
            10,
        )
        turnover_delta = round(
            sum(_float(row.get("turnover")) for row in window)
            - sum(_float(row.get("turnover")) for row in static_window),
            10,
        )
        reasons = []
        if drawdown_delta < RISK_WORSENING_DRAWDOWN_DELTA_THRESHOLD:
            reasons.append("drawdown_deeper")
        if vol_delta > RISK_WORSENING_VOL_DELTA_THRESHOLD:
            reasons.append("volatility_higher")
        if turnover_delta > RISK_WORSENING_TURNOVER_DELTA_THRESHOLD:
            reasons.append("turnover_higher")
        if not reasons:
            continue
        exposure = _exposure_summary(window)
        event_type = reasons[0] if len(reasons) == 1 else "mixed"
        event_date = _text(window[-1].get("date"))
        rows.append(
            {
                "event_id": _stable_id("risk-worsening-event", event_date, event_type),
                "date": event_date,
                "window": f"{RISK_WORSENING_EVENT_WINDOW_DAYS}d",
                "risk_worsening_type": event_type,
                "risk_asset_weight": exposure.get("avg_risk_asset_weight"),
                "semiconductor_weight": exposure.get("avg_semiconductor_weight"),
                "cash_weight": exposure.get("avg_cash_weight"),
                "relative_drawdown_vs_static": drawdown_delta,
                "volatility_delta_vs_static": vol_delta,
                "turnover_delta_vs_static": turnover_delta,
                "likely_cause": _risk_event_likely_cause(exposure),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _method_state_rows(states: Sequence[Mapping[str, Any]], method: str) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in states if row.get("target_method") == method],
        key=lambda row: _text(row.get("date")),
    )


def _rows_between(
    rows: Sequence[Mapping[str, Any]],
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    return [
        dict(row) for row in rows if start <= _coerce_date(row.get("date"), date(1970, 1, 1)) <= end
    ]


def _max_drawdown_window(rows: Sequence[Mapping[str, Any]]) -> tuple[date, date]:
    if not rows:
        return date.max, date.min
    peak_value = _float(rows[0].get("portfolio_value"), 1.0)
    peak_date = _coerce_date(rows[0].get("date"), date.max)
    worst_drawdown = 0.0
    worst_start = peak_date
    worst_end = peak_date
    for row in rows:
        value = _float(row.get("portfolio_value"), 1.0)
        current_date = _coerce_date(row.get("date"), peak_date)
        if value > peak_value:
            peak_value = value
            peak_date = current_date
        drawdown = value / peak_value - 1.0 if peak_value > 0 else 0.0
        if drawdown < worst_drawdown:
            worst_drawdown = drawdown
            worst_start = peak_date
            worst_end = current_date
    return worst_start, worst_end


def _symbol_contribution(
    rows: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
) -> dict[str, float]:
    if returns.empty:
        return {}
    contribution = {str(symbol): 0.0 for symbol in returns.columns}
    for row in rows:
        day = _text(row.get("date"))
        if day not in returns.index:
            continue
        weights = _mapping(row.get("weights"))
        for symbol in returns.columns:
            contribution[str(symbol)] += _float(weights.get(str(symbol))) * _float(
                returns.loc[day, symbol]
            )
    return {symbol: round(value, 10) for symbol, value in contribution.items()}


def _avg_symbol_weight(rows: Sequence[Mapping[str, Any]], symbol: str) -> float:
    return _mean_float([_float(_mapping(row.get("weights")).get(symbol)) for row in rows])


def _drawdown_from_returns(values: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        worst = min(worst, equity / peak - 1.0 if peak > 0 else 0.0)
    return worst


def _risk_event_likely_cause(exposure: Mapping[str, Any]) -> str:
    if _float(exposure.get("avg_semiconductor_weight")) >= RISK_EVENT_HIGH_SEMICONDUCTOR_WEIGHT:
        return "risk_exposure_too_high"
    if _float(exposure.get("avg_cash_weight")) <= RISK_EVENT_LOW_CASH_WEIGHT:
        return "late_risk_reduction"
    return "unknown"


def _warning_repair_actions(impact: Mapping[str, Any]) -> list[dict[str, Any]]:
    inventory = _mapping(impact.get("data_warning_inventory"))
    warnings = _records(inventory.get("warnings"))
    if not warnings:
        warnings = [
            {
                "warning_id": "no_recorded_warning",
                "severity": "INFO",
                "affected_symbols": [],
                "affected_dates": [],
                "potential_metric_impact": "LOW",
            }
        ]
    actions = []
    for warning in warnings:
        warning_id = _text(warning.get("warning_id"), "unknown_warning")
        warning_type = _warning_type(warning_id)
        severity = _warning_repair_severity(warning)
        repair_action = _recommended_repair_action(warning_type, warning_id)
        actions.append(
            {
                "warning_id": warning_id,
                "warning_type": warning_type,
                "severity": severity,
                "affected_artifacts": [
                    _text(impact.get("impact_id")),
                    _text(impact.get("backfill_id")),
                    _text(impact.get("selection_review_id")),
                ],
                "recommended_repair_action": repair_action,
                "expected_effect": _expected_repair_effect(repair_action, severity),
                "auto_repair_allowed": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return actions


def _warning_type(warning_id: str) -> str:
    lowered = warning_id.lower()
    if "manifest" in lowered and "missing" in lowered:
        return "price_manifest_missing"
    if "stale" in lowered:
        return "stale_report"
    if "checksum" in lowered:
        return "missing_checksum"
    if "missing_price" in lowered or "missing_symbols" in lowered:
        return "price_cache_gap"
    return "unknown"


def _warning_repair_severity(warning: Mapping[str, Any]) -> str:
    impact = _text(warning.get("potential_metric_impact"), "UNKNOWN")
    raw = _text(warning.get("severity"), "WARNING")
    if raw in {"BLOCKING", "REVIEW_REQUIRED"}:
        return raw
    if impact in {"UNKNOWN", "HIGH"}:
        return "REVIEW_REQUIRED"
    if impact == "MEDIUM":
        return "WARNING"
    return "INFO" if raw == "INFO" else "WARNING"


def _recommended_repair_action(warning_type: str, warning_id: str) -> str:
    if warning_type in {"price_manifest_missing", "missing_checksum"}:
        return "repair_manifest"
    if warning_type == "stale_report":
        return "rerun_report_index"
    if warning_type == "price_cache_gap":
        return "refresh_price_cache"
    if warning_id == "no_recorded_warning":
        return "no_action"
    return "manual_review"


def _expected_repair_effect(repair_action: str, severity: str) -> str:
    if repair_action == "no_action":
        return "no_change"
    if repair_action == "manual_review":
        return "unknown"
    if severity == "REVIEW_REQUIRED":
        return "downgrade_warning"
    return "remove_warning"


def _warning_blocking_matrix(
    impact: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sensitivity = _mapping(impact.get("recommendation_sensitivity_to_warnings"))
    rows = []
    for action in actions:
        severity = _text(action.get("severity"))
        warning_id = _text(action.get("warning_id"))
        blocks_hardening = severity in {"REVIEW_REQUIRED", "BLOCKING"}
        rows.append(
            {
                "warning_id": warning_id,
                "blocks_hardening": blocks_hardening,
                "blocks_research": severity == "BLOCKING",
                "blocks_production": severity in {"WARNING", "REVIEW_REQUIRED", "BLOCKING"},
                "reason": _warning_blocking_reason(action, sensitivity),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if any(row.get("blocks_hardening") for row in rows):
        overall = "REVIEW_REQUIRED"
    elif any(row.get("blocks_production") for row in rows):
        overall = "PASS_WITH_WARNINGS"
    else:
        overall = "PASS"
    effects = {_text(row.get("expected_effect")) for row in actions}
    hardening_after = (
        "NO"
        if any(row.get("blocks_research") for row in rows)
        else (
            "UNKNOWN"
            if "unknown" in effects or any(row.get("blocks_hardening") for row in rows)
            else "YES"
        )
    )
    return {
        "warnings": rows,
        "overall_data_warning_status": overall,
        "hardening_allowed_after_repair": hardening_after,
        **SYSTEM_TARGET_SAFETY,
    }


def _warning_blocking_reason(
    action: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> str:
    if action.get("severity") == "REVIEW_REQUIRED":
        return "Data warning impact is not quantified enough to harden the research method."
    if action.get("severity") == "BLOCKING":
        return "Data warning blocks research interpretation until repaired."
    if sensitivity.get("data_quality_decision") == "REVIEW_REQUIRED":
        return "Warning contributes to REVIEW_REQUIRED data quality decision."
    return "Warning should be reviewed before production use but does not block research."


def _alternative_method_candidates(
    risk: Mapping[str, Any],
    instability: Mapping[str, Any],
) -> dict[str, Any]:
    _ = risk, instability
    return {
        "candidates": [
            {
                "method": "risk_capped_limited_adjustment",
                "status": "PROPOSED",
                "description": (
                    "limited_adjustment with risk asset increase capped during elevated "
                    "drawdown or high dispersion."
                ),
                "expected_benefit": "reduce drawdown worsening",
                "expected_cost": "may reduce return improvement",
                "requires_new_implementation": True,
                "auto_apply": False,
            },
            {
                "method": "regime_gated_limited_adjustment",
                "status": "PROPOSED",
                "description": (
                    "allow limited adjustment only outside tech_drawdown, "
                    "semiconductor_pullback, or risk_off regimes."
                ),
                "expected_benefit": "improve pressure regime behavior",
                "expected_cost": "may miss rebound after drawdown",
                "requires_new_implementation": True,
                "auto_apply": False,
            },
            {
                "method": "lower_turnover_limited_adjustment",
                "status": "PROPOSED",
                "description": "smaller adjustment step or slower rebalance cadence.",
                "expected_benefit": "reduce turnover and weight jumps",
                "expected_cost": "slower response to improved target evidence",
                "requires_new_implementation": True,
                "auto_apply": False,
            },
            {
                "method": "cash_buffered_limited_adjustment",
                "status": "PROPOSED",
                "description": "keep a higher cash floor while limited_adjustment is under review.",
                "expected_benefit": "reduce drawdown sensitivity",
                "expected_cost": "may reduce upside capture",
                "requires_new_implementation": True,
                "auto_apply": False,
            },
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _alternative_method_scorecard(
    backfill: Mapping[str, Any],
    risk: Mapping[str, Any],
    instability: Mapping[str, Any],
    candidates: Mapping[str, Any],
) -> dict[str, Any]:
    states = _records(backfill.get("backfill_method_states"))
    methods = []
    for method in (
        "limited_adjustment",
        "defensive_limited_adjustment",
        "static_baseline",
        "consensus_target",
    ):
        metrics = _method_path_metrics(states, method)
        methods.append(
            {
                "method": method,
                "current_status": (
                    "REVIEW_REQUIRED" if method == "limited_adjustment" else "EXISTING"
                ),
                "return_expectation": _return_expectation(metrics),
                "risk_expectation": _risk_expectation(metrics, states),
                "stability_expectation": (
                    _text(
                        _mapping(instability.get("instability_reason_summary")).get(
                            "rolling_consistency_status"
                        ),
                        "UNKNOWN",
                    )
                    if method == "limited_adjustment"
                    else "UNKNOWN"
                ),
                "implementation_status": "EXISTING",
                "recommendation": (
                    "KEEP_AS_SECONDARY_OR_REFINE"
                    if method == "limited_adjustment"
                    else "KEEP_AS_REFERENCE"
                ),
            }
        )
    for row in _records(candidates.get("candidates")):
        method = _text(row.get("method"))
        methods.append(
            {
                "method": method,
                "current_status": "PROPOSED",
                "return_expectation": "MEDIUM",
                "risk_expectation": (
                    "BETTER"
                    if method
                    in {"risk_capped_limited_adjustment", "cash_buffered_limited_adjustment"}
                    else "UNKNOWN"
                ),
                "stability_expectation": "UNKNOWN",
                "implementation_status": "NOT_IMPLEMENTED",
                "recommendation": (
                    "IMPLEMENT_AS_RESEARCH_CANDIDATE"
                    if method
                    in {"risk_capped_limited_adjustment", "regime_gated_limited_adjustment"}
                    else "CONSIDER_AFTER_PRIMARY_REFINEMENT"
                ),
            }
        )
    recommended = _recommended_alternative_method(risk, instability)
    return {
        "methods": methods,
        "recommended_alternative": recommended,
        **SYSTEM_TARGET_SAFETY,
    }


def _return_expectation(metrics: Mapping[str, Any]) -> str:
    total = _float(metrics.get("total_return"))
    if total > ALTERNATIVE_RETURN_HIGH_EXPECTATION_THRESHOLD:
        return "HIGHER"
    if total > 0.0:
        return "MEDIUM"
    return "LOWER"


def _risk_expectation(metrics: Mapping[str, Any], states: Sequence[Mapping[str, Any]]) -> str:
    static = _method_path_metrics(states, "static_baseline")
    if _float(metrics.get("max_drawdown")) >= _float(static.get("max_drawdown")):
        return "BETTER"
    return "WORSE"


def _recommended_alternative_method(
    risk: Mapping[str, Any],
    instability: Mapping[str, Any],
) -> str:
    exposure = _mapping(risk.get("exposure_shift_attribution"))
    summary = _mapping(instability.get("instability_reason_summary"))
    source = _text(exposure.get("risk_worsening_source"))
    recommendation = _text(summary.get("recommendation"))
    if source in {
        "higher_risk_asset_exposure",
        "higher_semiconductor_exposure",
        "lower_cash",
        "mixed",
    }:
        return "risk_capped_limited_adjustment"
    if recommendation == "consider_regime_gate":
        return "regime_gated_limited_adjustment"
    return "risk_capped_limited_adjustment"


def _refined_method_decision(
    instability: Mapping[str, Any],
    risk: Mapping[str, Any],
    repair: Mapping[str, Any],
    alt_review: Mapping[str, Any],
) -> dict[str, Any]:
    instability_summary = _mapping(instability.get("instability_reason_summary"))
    exposure = _mapping(risk.get("exposure_shift_attribution"))
    matrix = _mapping(repair.get("warning_blocking_matrix"))
    scorecard = _mapping(alt_review.get("alternative_method_scorecard"))
    recommended_alt = _text(scorecard.get("recommended_alternative"))
    warning_after = _text(matrix.get("hardening_allowed_after_repair"), "UNKNOWN")
    if warning_after == "NO":
        next_step = "REPAIR_DATA_WARNINGS_FIRST"
    elif recommended_alt == "risk_capped_limited_adjustment":
        next_step = "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD"
    elif recommended_alt == "regime_gated_limited_adjustment":
        next_step = "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD"
    elif _text(instability_summary.get("recommendation")) == "insufficient_data":
        next_step = "CONTINUE_OBSERVATION"
    else:
        next_step = "DEFER"
    confidence = (
        "LOW"
        if warning_after == "UNKNOWN"
        else (
            "MEDIUM"
            if next_step
            in {
                "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD",
                "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD",
            }
            else "LOW"
        )
    )
    reason = (
        "limited_adjustment did not harden because rolling consistency is "
        f"{instability_summary.get('rolling_consistency_status')}, "
        f"risk worsening source is {exposure.get('risk_worsening_source')}, "
        f"and data warning repair outcome is {warning_after}."
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "proposal_id": "",
        "current_method": "limited_adjustment",
        "current_hardening_status": "REVIEW_REQUIRED",
        "recommended_next_step": next_step,
        "reason": reason,
        "confidence": confidence,
        "auto_apply": False,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "data_warnings_need_repair_before_hardening": warning_after != "YES",
        "limited_adjustment_secondary_research_method": True,
        "next_action": (
            "implement_research_only_refined_method_candidate"
            if next_step.startswith("IMPLEMENT_")
            else "owner_review_required"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _proposed_next_methods(
    decision: Mapping[str, Any],
    alt_review: Mapping[str, Any],
) -> dict[str, Any]:
    _ = alt_review
    preferred = _text(decision.get("recommended_next_step"))
    risk_priority = "HIGH" if preferred == "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD" else "MEDIUM"
    regime_priority = "HIGH" if preferred == "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD" else "MEDIUM"
    methods = [
        {
            "method": "risk_capped_limited_adjustment",
            "priority": risk_priority,
            "implementation_scope": "research_only",
            "expected_improvement": (
                "reduce drawdown worsening while preserving part of return improvement"
            ),
            "required_validation": [
                "historical backfill",
                "rolling consistency",
                "pressure regime review",
                "forward confirmation",
            ],
        },
        {
            "method": "regime_gated_limited_adjustment",
            "priority": regime_priority,
            "implementation_scope": "research_only",
            "expected_improvement": "avoid active risk increase in pressure regimes",
            "required_validation": [
                "regime-specific simulation",
                "forward pressure samples",
            ],
        },
    ]
    return {"methods": methods, **SYSTEM_TARGET_SAFETY}


def _mean_float(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _rank_score(avg_rank: float, method_count: int) -> float:
    if avg_rank <= 0 or method_count <= 1:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (avg_rank - 1.0) / (method_count - 1.0)))


def _regime_score(regime_summary: Sequence[Mapping[str, Any]], method: str) -> float:
    if not regime_summary:
        return 0.0
    points = 0.0
    total = 0.0
    for row in regime_summary:
        if row.get("sample_count", 0) == 0:
            continue
        total += 3.0
        points += 1.0 if row.get("best_return_method") == method else 0.0
        points += 1.0 if row.get("best_drawdown_method") == method else 0.0
        points += 1.0 if row.get("best_risk_adjusted_method") == method else 0.0
    return points / total if total > 0 else 0.0


def _stability_status_score(status: str) -> float:
    return {
        "STABLE": 1.0,
        "MODERATE": 0.65,
        "MIXED": 0.5,
        "UNSTABLE": 0.15,
        "INSUFFICIENT_DATA": 0.0,
    }.get(status, 0.0)


def _annualized_return(total_return: float, periods: int) -> float:
    if periods <= 0:
        return 0.0
    if total_return <= -1.0:
        return -1.0
    return float((1.0 + total_return) ** (252.0 / periods) - 1.0)


def _stddev(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _best_metric_method(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    high: bool,
) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    selected = (
        max(rows, key=lambda row: _float(row.get(field)))
        if high
        else min(
            rows,
            key=lambda row: _float(row.get(field)),
        )
    )
    return _text(selected.get("target_method"), "INSUFFICIENT_DATA")


def _best_rank_stability_method(rows: Sequence[Mapping[str, Any]]) -> str:
    valid = [row for row in rows if row.get("rank_stability_status") != "INSUFFICIENT_DATA"]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        min(
            valid,
            key=lambda row: (
                (
                    _float(row.get("avg_rank_return"))
                    + _float(row.get("avg_rank_drawdown"))
                    + _float(row.get("avg_rank_risk_adjusted"))
                )
                / 3.0
            ),
        ).get("target_method")
    )


def _best_status_method(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    order = {"STABLE": 0, "LOW": 0, "MODERATE": 1, "MIXED": 2, "HIGH": 2, "UNSTABLE": 3}
    valid = [row for row in rows if row.get(field) not in {None, "INSUFFICIENT_DATA"}]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        min(valid, key=lambda row: order.get(_text(row.get(field)), 9)).get("target_method")
    )


def _max_field_method(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    return _text(max(rows, key=lambda row: _float(row.get(field))).get("target_method"))


def _config_int(config: Mapping[str, Any], path: Sequence[str], default: int) -> int:
    return int(_config_float(config, path, float(default)))


def _config_float(config: Mapping[str, Any], path: Sequence[str], default: float) -> float:
    node: Any = config
    for key in path:
        node = _mapping(node).get(key)
    return _float(node, default)


def _load_backfill_config_from_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    path_text = _text(manifest.get("config_path"))
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return {}
    try:
        return load_paper_shadow_backfill_config(path)
    except DynamicV3SystemTargetError:
        return {}


def _resolve_project_path(value: object, default: Path) -> Path:
    if value in {None, ""}:
        return default
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _recommended_research_method(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    *,
    preferred_order: Sequence[str] | None = None,
) -> str:
    available = {
        row.get("target_method")
        for row in rows
        if row.get("performance_status") != "INSUFFICIENT_DATA"
    }
    configured_order = preferred_order or (
        "limited_adjustment",
        "defensive_limited_adjustment",
        "consensus_target",
    )
    for method in configured_order:
        if method in available:
            return method
    observed_methods = {row.get("target_method") for row in rows}
    for method in configured_order:
        if method in observed_methods:
            return method
    best = _text(summary.get("best_risk_adjusted_method"))
    return best if best and best != "INSUFFICIENT_DATA" else "limited_adjustment"


def _review_reason(recommended: str, summary: Mapping[str, Any]) -> str:
    return (
        f"{recommended} is selected for continued research observation. "
        f"Best return method is {summary.get('best_return_method')}, but return alone does not "
        "approve official target weights or broker action."
    )


def _metric_for(rows: Sequence[Mapping[str, Any]], method: str, field: str) -> float:
    return _float(_find_method(rows, method).get(field))


def _find_method(rows: Sequence[Mapping[str, Any]], method: str) -> dict[str, Any]:
    for row in rows:
        if row.get("target_method") == method or row.get("method") == method:
            return dict(row)
        if row.get("method_id") == method:
            return dict(row)
    return {}


def _best_method(rows: Sequence[Mapping[str, Any]], field: str, *, high: bool) -> str:
    valid = [row for row in rows if row.get("performance_status") != "INSUFFICIENT_DATA"]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        max(valid, key=lambda row: _float(row.get(field))).get("target_method")
        if high
        else min(valid, key=lambda row: _float(row.get(field))).get("target_method")
    )


def _paper_initial_weights(
    config: Mapping[str, Any], target_payload: Mapping[str, Any]
) -> dict[str, float]:
    target_weights = _mapping(
        _mapping(target_payload.get("model_target_weights")).get("method_weights")
    )
    initial_method = _text(
        _mapping(config.get("paper_shadow_account")).get("initial_method"), "static_baseline"
    )
    if initial_method in target_weights:
        return _normalize_weights(_mapping(target_weights[initial_method]))
    baseline = _mapping(config.get("baseline")).get("static_weights")
    if isinstance(baseline, Mapping):
        return _normalize_weights(baseline)
    return {"QQQ": 0.50, "SMH": 0.20, "TLT": 0.10, "CASH": 0.20}


def _optional_latest_model_target_payload(output_dir: Path) -> dict[str, Any]:
    try:
        return model_target_report_payload(latest=True, output_dir=output_dir)
    except DynamicV3SystemTargetError:
        latest_root = _latest_child_dir_with(output_dir, "model_target_manifest.json")
        if latest_root is None:
            return {}
        try:
            return model_target_report_payload(
                target_id=latest_root.name,
                output_dir=output_dir,
            )
        except DynamicV3SystemTargetError:
            return {}


def _load_advisory_limits(path_or_value: object) -> dict[str, Any]:
    path = Path(str(path_or_value)) if path_or_value else DEFAULT_POSITION_ADVISORY_CONFIG_PATH
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return {}
    payload = _load_yaml_mapping(path)
    return _mapping(payload.get("advisory_limits"))


def _assert_model_target_config_safe(payload: Mapping[str, Any]) -> None:
    model_target = _mapping(payload.get("model_target"))
    if model_target.get("mode") != "research_target_only":
        raise DynamicV3SystemTargetError("model target config must use research_target_only mode")
    if model_target.get("not_official_target_weights") is not True:
        raise DynamicV3SystemTargetError(
            "model target config must mark not_official_target_weights"
        )
    if model_target.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("model target config must mark paper_shadow_only")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("model target safety fields are unsafe")
    unknown = set(_enabled_methods(payload)) - set(TARGET_METHODS)
    if unknown:
        raise DynamicV3SystemTargetError(f"unknown target methods: {sorted(unknown)}")


def _assert_paper_shadow_config_safe(payload: Mapping[str, Any]) -> None:
    account = _mapping(payload.get("paper_shadow_account"))
    if account.get("mode") != "paper_shadow_only":
        raise DynamicV3SystemTargetError("paper shadow config must use paper_shadow_only mode")
    if _coerce_date(account.get("start_date"), date(1970, 1, 1)) < date(2022, 12, 1):
        raise DynamicV3SystemTargetError("paper shadow start_date cannot predate 2022-12-01")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("paper shadow safety fields are unsafe")


def _assert_paper_shadow_backfill_config_safe(payload: Mapping[str, Any]) -> None:
    backfill = _mapping(payload.get("backfill"))
    date_range = _mapping(payload.get("date_range"))
    source = _mapping(payload.get("source"))
    if backfill.get("mode") != "BACKTEST_SIMULATION":
        raise DynamicV3SystemTargetError("paper shadow backfill must use BACKTEST_SIMULATION")
    if backfill.get("not_pit_safe") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must disclose not_pit_safe=true")
    if backfill.get("research_target_only") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must be research_target_only")
    if backfill.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must be paper_shadow_only")
    if _coerce_date(date_range.get("start"), date(1970, 1, 1)) < AI_AFTER_CHATGPT_START:
        raise DynamicV3SystemTargetError("paper shadow backfill start cannot predate 2022-12-01")
    if not source.get("model_target_config") or not source.get("paper_shadow_config"):
        raise DynamicV3SystemTargetError("paper shadow backfill source configs are required")
    unknown = set(_enabled_methods(payload)) - set(TARGET_METHODS)
    if unknown:
        raise DynamicV3SystemTargetError(f"unknown target methods: {sorted(unknown)}")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("paper shadow backfill safety fields are unsafe")


def _assert_risk_capped_limited_config_safe(payload: Mapping[str, Any]) -> None:
    method = _mapping(payload.get("method"))
    if method.get("name") != "risk_capped_limited_adjustment":
        raise DynamicV3SystemTargetError("risk-capped config method.name is invalid")
    if method.get("base_method") != "limited_adjustment":
        raise DynamicV3SystemTargetError(
            "risk-capped config base_method must be limited_adjustment"
        )
    if method.get("mode") != "research_target_only":
        raise DynamicV3SystemTargetError("risk-capped config must use research_target_only mode")
    if method.get("not_official_target_weights") is not True:
        raise DynamicV3SystemTargetError("risk-capped config must be not_official_target_weights")
    if method.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("risk-capped config must be paper_shadow_only")
    if not _mapping(payload.get("caps")):
        raise DynamicV3SystemTargetError("risk-capped config caps are required")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("risk-capped config safety fields are unsafe")


def _assert_smoothed_limited_config_safe(payload: Mapping[str, Any]) -> None:
    method = _mapping(payload.get("method"))
    if method.get("name") != "smoothed_limited_adjustment":
        raise DynamicV3SystemTargetError("smoothed config method.name is invalid")
    if method.get("base_method") != "limited_adjustment":
        raise DynamicV3SystemTargetError("smoothed config base_method must be limited_adjustment")
    if method.get("mode") != "research_target_only":
        raise DynamicV3SystemTargetError("smoothed config must use research_target_only mode")
    if method.get("not_official_target_weights") is not True:
        raise DynamicV3SystemTargetError("smoothed config must be not_official_target_weights")
    if method.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("smoothed config must be paper_shadow_only")
    if not _mapping(payload.get("variants")):
        raise DynamicV3SystemTargetError("smoothed config variants are required")
    if _mapping(payload.get("constraints")).get("preserve_total_weight") is not True:
        raise DynamicV3SystemTargetError("smoothed config must preserve total weight")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("smoothed config safety fields are unsafe")


def _load_risk_capped_config_if_available(config: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(config.get("source"))
    path = _resolve_project_path(
        source.get("risk_capped_limited_config"),
        DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    )
    return load_risk_capped_limited_config(path)


def _load_smoothed_config_if_available(config: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(config.get("source"))
    path = _resolve_project_path(
        source.get("smoothed_limited_config"),
        DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    )
    return load_smoothed_limited_config(path)


def _normalized_risk_capped_config(config: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    reallocation = dict(_mapping(config.get("reallocation")))
    if "excess_weight_destination" in reallocation:
        reallocation["excess_weight_destination"] = [
            item.upper() for item in _texts(reallocation.get("excess_weight_destination"))
        ]
    if reallocation.get("fallback_destination"):
        reallocation["fallback_destination"] = _text(
            reallocation.get("fallback_destination")
        ).upper()
    normalized["reallocation"] = reallocation
    normalized["safety"] = {**SYSTEM_TARGET_SAFETY, **_mapping(config.get("safety"))}
    return normalized


def _normalized_smoothed_config(config: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    normalized["safety"] = {**SYSTEM_TARGET_SAFETY, **_mapping(config.get("safety"))}
    return normalized


def _enabled_smoothed_variants(config: Mapping[str, Any]) -> list[str]:
    variants = _mapping(config.get("variants"))
    return [
        variant
        for variant in SMOOTHED_VARIANT_TO_METHOD
        if _mapping(variants.get(variant)).get("enabled") is True
    ]


def _enabled_smoothed_methods(config: Mapping[str, Any]) -> list[str]:
    return [SMOOTHED_VARIANT_TO_METHOD[variant] for variant in _enabled_smoothed_variants(config)]


def _smoothed_constraints_within_bounds(constraints: Mapping[str, Any]) -> bool:
    bounded_fields = (
        "min_cash_weight",
        "max_single_symbol_weight",
        "max_semiconductor_weight",
        "max_total_risk_asset_weight",
    )
    return all(0.0 <= _float(constraints.get(field), -1.0) <= 1.0 for field in bounded_fields)


def _risk_capped_caps_within_bounds(
    caps: Mapping[str, Any],
    delta_caps: Mapping[str, Any],
) -> bool:
    cap_fields = (
        "max_total_risk_asset_weight",
        "max_semiconductor_weight",
        "max_single_symbol_weight",
        "min_cash_weight",
    )
    delta_fields = (
        "max_total_risk_asset_increase_per_rebalance",
        "max_semiconductor_increase_per_rebalance",
        "max_single_symbol_increase_per_rebalance",
        "max_total_adjustment_per_rebalance",
    )
    return all(0.0 <= _float(caps.get(field), -1.0) <= 1.0 for field in cap_fields) and all(
        0.0 <= _float(delta_caps.get(field), -1.0) <= 1.0 for field in delta_fields
    )


def _risk_capped_allocation_possible(
    caps: Mapping[str, Any],
    universe: set[str],
) -> bool:
    return (
        "CASH" in universe
        and 0.0 <= _float(caps.get("min_cash_weight"), -1.0) <= 1.0
        and 0.0 <= _float(caps.get("max_total_risk_asset_weight"), -1.0) <= 1.0
        and 0.0 <= _float(caps.get("max_single_symbol_weight"), -1.0) <= 1.0
    )


def _risk_capped_contextual_caps_not_relaxed(config: Mapping[str, Any]) -> bool:
    caps = _mapping(config.get("caps"))
    delta_caps = _mapping(config.get("delta_caps"))
    contextual = _mapping(config.get("contextual_caps"))
    max_fields = (
        "max_total_risk_asset_weight",
        "max_semiconductor_weight",
        "max_single_symbol_weight",
    )
    min_fields = ("min_cash_weight",)
    delta_fields = (
        "max_total_risk_asset_increase_per_rebalance",
        "max_semiconductor_increase_per_rebalance",
        "max_single_symbol_increase_per_rebalance",
        "max_total_adjustment_per_rebalance",
    )
    for raw_context in contextual.values():
        context = _mapping(raw_context)
        if context.get("explicit_allow_relaxation") is True:
            continue
        for field in max_fields:
            if field in context and _float(context.get(field)) > _float(caps.get(field), 1.0):
                return False
        for field in min_fields:
            if field in context and _float(context.get(field)) < _float(caps.get(field)):
                return False
        for field in delta_fields:
            if field in context and _float(context.get(field)) > _float(delta_caps.get(field), 1.0):
                return False
    return True


def _risk_capped_symbol_universe(
    model_config: Mapping[str, Any],
    risk_config: Mapping[str, Any] | None = None,
) -> set[str]:
    constraints = _constraints(model_config)
    symbols = {
        symbol.upper()
        for weights in (
            _mapping(_mapping(model_config.get("baseline")).get("static_weights")),
            {item: 0 for item in _texts(constraints.get("semiconductor_symbols"))},
            {item: 0 for item in _texts(constraints.get("defensive_symbols"))},
        )
        for symbol in weights
    }
    risk = _mapping(risk_config)
    reallocation = _mapping(risk.get("reallocation"))
    symbols.update(item.upper() for item in _texts(reallocation.get("excess_weight_destination")))
    if reallocation.get("fallback_destination"):
        symbols.add(_text(reallocation.get("fallback_destination")).upper())
    symbols.update({"QQQ", "SMH", "SOXX", "TLT", "CASH"})
    return symbols


def _risk_capped_limited_weights_for_model_target(
    *,
    base_weights: Mapping[str, Any],
    previous_weights: Mapping[str, Any],
    risk_config: Mapping[str, Any],
    model_config: Mapping[str, Any],
    as_of: date,
    regime_context: str,
) -> dict[str, float]:
    return _apply_risk_capped_limited_adjustment(
        as_of=as_of,
        base_weights=base_weights,
        previous_weights=previous_weights,
        risk_config=risk_config,
        model_config=model_config,
        regime_context=regime_context,
    )["capped_weights"]


def _apply_risk_capped_limited_adjustment(
    *,
    as_of: date,
    base_weights: Mapping[str, Any],
    previous_weights: Mapping[str, Any],
    risk_config: Mapping[str, Any],
    model_config: Mapping[str, Any],
    regime_context: str,
) -> dict[str, Any]:
    base = _normalize_weights(base_weights)
    previous = _normalize_weights(previous_weights)
    symbols = sorted(
        set(base) | set(previous) | _risk_capped_symbol_universe(model_config, risk_config)
    )
    capped = {symbol: _float(base.get(symbol)) for symbol in symbols}
    caps = _effective_risk_capped_caps(risk_config, regime_context)
    semiconductors = _risk_capped_semiconductor_symbols(model_config)
    defensive = _risk_capped_defensive_symbols(model_config)
    risk_symbols = [symbol for symbol in symbols if symbol not in defensive and symbol != "CASH"]
    cap_events: list[dict[str, Any]] = []
    reallocation_events: list[dict[str, Any]] = []

    def allocate_excess(source: str, amount: float, reason: str) -> None:
        if amount <= 0:
            return
        reallocation = _mapping(risk_config.get("reallocation"))
        destinations = _texts(reallocation.get("excess_weight_destination")) or ["CASH", "TLT"]
        destination = destinations[0].upper() if destinations else "CASH"
        if destination not in capped:
            destination = _text(reallocation.get("fallback_destination"), "CASH").upper()
        before = _float(capped.get(destination))
        capped[destination] = round(before + amount, 10)
        reallocation_events.append(
            {
                "as_of": as_of.isoformat(),
                "excess_source": source,
                "excess_weight": round(amount, 10),
                "destination": destination,
                "destination_before": round(before, 10),
                "destination_after": round(capped[destination], 10),
                "reason": reason,
                **SYSTEM_TARGET_SAFETY,
            }
        )

    def reduce_symbol(
        symbol: str,
        target: float,
        cap_type: str,
        cap_value: float,
        reason: str,
    ) -> None:
        before = _float(capped.get(symbol))
        after = min(before, max(0.0, target))
        excess = round(before - after, 10)
        if excess <= 0:
            return
        capped[symbol] = round(after, 10)
        cap_events.append(
            {
                "as_of": as_of.isoformat(),
                "symbol": symbol,
                "cap_type": cap_type,
                "before_weight": round(before, 10),
                "after_weight": round(after, 10),
                "cap_value": round(cap_value, 10),
                "excess_weight": excess,
                "reason": reason,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        allocate_excess(symbol, excess, "preserve_total_weight_after_cap")

    def reduce_group(
        symbol_list: Sequence[str],
        cap_value: float,
        cap_type: str,
        reason: str,
    ) -> None:
        selected = [symbol for symbol in symbol_list if _float(capped.get(symbol)) > 0]
        total = sum(_float(capped.get(symbol)) for symbol in selected)
        if total <= cap_value or not selected:
            return
        target_total = max(0.0, cap_value)
        reduction = total - target_total
        for symbol in selected:
            share = _float(capped.get(symbol)) / total if total > 0 else 0.0
            reduce_symbol(
                symbol,
                _float(capped.get(symbol)) - reduction * share,
                cap_type,
                cap_value,
                reason,
            )

    for symbol in symbols:
        reduce_symbol(
            symbol,
            _float(caps.get("max_single_symbol_weight"), 1.0),
            "max_single_symbol_weight",
            _float(caps.get("max_single_symbol_weight"), 1.0),
            "single_symbol_risk_cap",
        )
    reduce_group(
        semiconductors,
        _float(caps.get("max_semiconductor_weight"), 1.0),
        "max_semiconductor_weight",
        "higher_semiconductor_exposure_risk_cap",
    )
    reduce_group(
        risk_symbols,
        _float(caps.get("max_total_risk_asset_weight"), 1.0),
        "max_total_risk_asset_weight",
        "total_risk_asset_exposure_cap",
    )
    previous_risk = _group_weight(previous, risk_symbols)
    risk_cap = previous_risk + _float(caps.get("max_total_risk_asset_increase_per_rebalance"), 1.0)
    if caps.get("allow_risk_asset_increase") is False:
        risk_cap = previous_risk
    reduce_group(
        risk_symbols,
        min(_float(caps.get("max_total_risk_asset_weight"), 1.0), risk_cap),
        "max_total_risk_asset_increase_per_rebalance",
        "risk_asset_increase_delta_cap",
    )
    previous_semi = _group_weight(previous, semiconductors)
    semi_cap = previous_semi + _float(caps.get("max_semiconductor_increase_per_rebalance"), 1.0)
    if caps.get("allow_semiconductor_increase") is False:
        semi_cap = previous_semi
    reduce_group(
        semiconductors,
        min(_float(caps.get("max_semiconductor_weight"), 1.0), semi_cap),
        "max_semiconductor_increase_per_rebalance",
        "semiconductor_increase_delta_cap",
    )
    single_delta = _float(caps.get("max_single_symbol_increase_per_rebalance"), 1.0)
    for symbol in symbols:
        reduce_symbol(
            symbol,
            _float(previous.get(symbol)) + single_delta,
            "max_single_symbol_increase_per_rebalance",
            single_delta,
            "single_symbol_increase_delta_cap",
        )
    turnover = _turnover(previous, capped)
    max_adjustment = _float(caps.get("max_total_adjustment_per_rebalance"), 1.0)
    if turnover > max_adjustment > 0:
        ratio = max_adjustment / turnover
        for symbol in symbols:
            previous_weight = _float(previous.get(symbol))
            capped_delta = _float(capped.get(symbol)) - previous_weight
            capped[symbol] = round(
                previous_weight + capped_delta * ratio,
                10,
            )
        cap_events.append(
            {
                "as_of": as_of.isoformat(),
                "symbol": "PORTFOLIO",
                "cap_type": "max_total_adjustment_per_rebalance",
                "before_weight": round(turnover, 10),
                "after_weight": round(max_adjustment, 10),
                "cap_value": round(max_adjustment, 10),
                "excess_weight": round(turnover - max_adjustment, 10),
                "reason": "total_adjustment_delta_cap",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    cash_before = _float(capped.get("CASH"))
    min_cash = _float(caps.get("min_cash_weight"), 0.0)
    if cash_before < min_cash:
        reduce_group(
            risk_symbols,
            max(0.0, _group_weight(capped, risk_symbols) - (min_cash - cash_before)),
            "min_cash_weight",
            "minimum_cash_buffer_cap",
        )
    capped = _normalize_weights(capped)
    summary = _risk_capped_cap_reason_summary(
        cap_events,
        reallocation_events,
        regime_context=regime_context,
    )
    return {
        "base_weights": base,
        "previous_weights": previous,
        "capped_weights": capped,
        "active_caps": sorted({str(row.get("cap_type")) for row in cap_events}),
        "regime_context": regime_context,
        "cap_events": cap_events,
        "reallocation_events": reallocation_events,
        "cap_reason_summary": summary,
        **SYSTEM_TARGET_SAFETY,
    }


def _effective_risk_capped_caps(
    risk_config: Mapping[str, Any],
    regime_context: str,
) -> dict[str, Any]:
    caps = {
        **_mapping(risk_config.get("caps")),
        **_mapping(risk_config.get("delta_caps")),
    }
    context = _mapping(_mapping(risk_config.get("contextual_caps")).get(regime_context))
    explicit_relax = context.get("explicit_allow_relaxation") is True
    for field, value in context.items():
        if field.startswith("max_") and field in caps and not explicit_relax:
            caps[field] = min(_float(caps.get(field), 1.0), _float(value))
        elif field.startswith("min_") and field in caps and not explicit_relax:
            caps[field] = max(_float(caps.get(field)), _float(value))
        else:
            caps[field] = value
    return caps


def _risk_capped_cap_reason_summary(
    cap_events: Sequence[Mapping[str, Any]],
    reallocation_events: Sequence[Mapping[str, Any]],
    *,
    regime_context: str,
) -> dict[str, Any]:
    by_cap_type = {
        "max_semiconductor_weight": 0,
        "max_total_risk_asset_weight": 0,
        "min_cash_weight": 0,
        "contextual_sideways_choppy": 0,
    }
    for event in cap_events:
        cap_type = _text(event.get("cap_type"))
        by_cap_type[cap_type] = by_cap_type.get(cap_type, 0) + 1
    if regime_context == "sideways_choppy" and cap_events:
        by_cap_type["contextual_sideways_choppy"] = len(cap_events)
    total_cash = sum(
        _float(row.get("excess_weight"))
        for row in reallocation_events
        if row.get("destination") == "CASH"
    )
    total_defensive = sum(
        _float(row.get("excess_weight"))
        for row in reallocation_events
        if row.get("destination") in {"TLT", "BND"}
    )
    return {
        "total_cap_events": len(cap_events),
        "by_cap_type": by_cap_type,
        "total_reallocated_to_cash": round(total_cash, 10),
        "total_reallocated_to_defensive": round(total_defensive, 10),
        "cap_status": "PASS" if cap_events else "NO_CAPS_TRIGGERED",
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_capped_regime_context_for_return(
    return_row: Mapping[str, Any],
    config: Mapping[str, Any],
) -> str:
    policy = _mapping(config.get("regime_policy"))
    risk_off = _float(policy.get("risk_off_return_threshold"), -0.015)
    drawdown = _float(policy.get("tech_drawdown_return_threshold"), -0.010)
    semi = _float(policy.get("semiconductor_pullback_return_threshold"), -0.012)
    trend = _float(policy.get("ai_trend_return_threshold"), 0.008)
    recovery = _float(policy.get("strong_recovery_return_threshold"), 0.012)
    qqq = _float(return_row.get("QQQ"))
    semi_return = min(_float(return_row.get("SMH")), _float(return_row.get("SOXX")))
    avg_risk = (qqq + semi_return) / 2.0
    if avg_risk <= risk_off:
        return "risk_off"
    if semi_return <= semi:
        return "semiconductor_pullback"
    if qqq <= drawdown:
        return "tech_drawdown"
    if avg_risk >= recovery:
        return "strong_recovery"
    if avg_risk >= trend:
        return "ai_trend"
    return "sideways_choppy"


def _risk_capped_semiconductor_symbols(model_config: Mapping[str, Any]) -> list[str]:
    symbols = _texts(_constraints(model_config).get("semiconductor_symbols")) or ["SMH", "SOXX"]
    return [symbol.upper() for symbol in symbols]


def _risk_capped_defensive_symbols(model_config: Mapping[str, Any]) -> set[str]:
    symbols = _texts(_constraints(model_config).get("defensive_symbols")) or ["CASH", "TLT"]
    return {symbol.upper() for symbol in symbols} | {"CASH"}


def _group_weight(weights: Mapping[str, Any], symbols: Sequence[str]) -> float:
    return sum(_float(weights.get(symbol)) for symbol in symbols)


def _risk_capped_backfill_summary(
    source_manifest: Mapping[str, Any],
    states: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    cap_events: Sequence[Mapping[str, Any]],
    reallocation_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    semi_weights = [
        _semiconductor_weight(_mapping(row.get("weights")), ("SMH", "SOXX")) for row in states
    ]
    cash_weights = [_float(_mapping(row.get("weights")).get("CASH")) for row in states]
    return {
        "method": "risk_capped_limited_adjustment",
        "date_start": source_manifest.get("date_start"),
        "date_end": source_manifest.get("date_end"),
        "rebalance_count": len(ledger),
        "cap_event_count": len(cap_events),
        "total_turnover": round(sum(_float(row.get("turnover")) for row in ledger), 10),
        "avg_semiconductor_weight": round(_mean_float(semi_weights), 10),
        "max_semiconductor_weight": round(max(semi_weights or [0.0]), 10),
        "avg_cash_weight": round(_mean_float(cash_weights), 10),
        "min_cash_weight": round(min(cash_weights or [0.0]), 10),
        "total_reallocated_to_cash": round(
            sum(
                _float(row.get("excess_weight"))
                for row in reallocation_events
                if row.get("destination") == "CASH"
            ),
            10,
        ),
        "data_quality": source_manifest.get("data_quality_status"),
        "broker_action_taken": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_capped_vs_limited_metrics(
    risk_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    risk_metrics = _state_path_metrics(risk_states, min_observations=2)
    limited_metrics = _method_path_metrics(baseline_states, "limited_adjustment")
    risk_exposure = _exposure_summary(risk_states)
    limited_exposure = _exposure_summary(
        [row for row in baseline_states if row.get("target_method") == "limited_adjustment"]
    )
    values = {
        "total_return_delta": round(
            _float(risk_metrics.get("total_return")) - _float(limited_metrics.get("total_return")),
            10,
        ),
        "annualized_return_delta": round(
            _float(risk_metrics.get("annualized_return"))
            - _float(limited_metrics.get("annualized_return")),
            10,
        ),
        "max_drawdown_delta": round(
            _float(risk_metrics.get("max_drawdown")) - _float(limited_metrics.get("max_drawdown")),
            10,
        ),
        "realized_volatility_delta": round(
            _float(risk_metrics.get("realized_volatility"))
            - _float(limited_metrics.get("realized_volatility")),
            10,
        ),
        "turnover_delta": round(
            _float(risk_metrics.get("turnover")) - _float(limited_metrics.get("turnover")),
            10,
        ),
        "avg_semiconductor_weight_delta": round(
            _float(risk_exposure.get("avg_semiconductor_weight"))
            - _float(limited_exposure.get("avg_semiconductor_weight")),
            10,
        ),
        "max_semiconductor_weight_delta": round(
            _float(risk_exposure.get("max_semiconductor_weight"))
            - _float(limited_exposure.get("max_semiconductor_weight")),
            10,
        ),
        "avg_cash_weight_delta": round(
            _float(risk_exposure.get("avg_cash_weight"))
            - _float(limited_exposure.get("avg_cash_weight")),
            10,
        ),
    }
    return {
        "comparison": {
            "method_a": "risk_capped_limited_adjustment",
            "method_b": "limited_adjustment",
        },
        "metrics": values,
        "conclusion": _risk_capped_comparison_conclusion(values),
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_capped_comparison_conclusion(metrics: Mapping[str, Any]) -> str:
    drawdown_improved = _float(metrics.get("max_drawdown_delta")) > 0.0
    semi_reduced = _float(metrics.get("avg_semiconductor_weight_delta")) < (
        -RISK_CAPPED_EXPOSURE_CHANGE_TOLERANCE
    )
    return_preserved = _float(metrics.get("total_return_delta")) >= (
        RISK_CAPPED_ACCEPTABLE_RETURN_DELTA_FLOOR
    )
    if drawdown_improved and semi_reduced and return_preserved:
        return "risk_capped_better"
    if not drawdown_improved and not semi_reduced:
        return "limited_better"
    return "mixed"


def _risk_capped_regime_comparison(
    states: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    config = _load_backfill_config_from_manifest(baseline)
    labels = _regime_labels_from_states(states, config)
    rows = []
    for regime in ("sideways_choppy", "tech_drawdown"):
        date_set = {day for day, label in labels.items() if label == regime}
        risk_rows = [
            row
            for row in states
            if row.get("target_method") == "risk_capped_limited_adjustment"
            and row.get("date") in date_set
        ]
        limited_rows = [
            row
            for row in states
            if row.get("target_method") == "limited_adjustment" and row.get("date") in date_set
        ]
        risk_metrics = _sample_return_metrics(risk_rows, min_sample=1)
        limited_metrics = _sample_return_metrics(limited_rows, min_sample=1)
        return_delta = round(
            _float(risk_metrics.get("total_return")) - _float(limited_metrics.get("total_return")),
            10,
        )
        drawdown_delta = round(
            _float(risk_metrics.get("max_drawdown")) - _float(limited_metrics.get("max_drawdown")),
            10,
        )
        rows.append(
            {
                "regime": regime,
                "sample_count": len(risk_rows),
                "return_delta_vs_limited": return_delta,
                "drawdown_delta_vs_limited": drawdown_delta,
                "win_rate_vs_limited": _win_rate_between_rows(risk_rows, limited_rows),
                "conclusion": _risk_capped_regime_conclusion(
                    return_delta,
                    drawdown_delta,
                    risk_rows,
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"regimes": rows, **SYSTEM_TARGET_SAFETY}


def _risk_capped_regime_conclusion(
    return_delta: float,
    drawdown_delta: float,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    if not rows:
        return "insufficient_data"
    if drawdown_delta > 0.0 and return_delta >= RISK_CAPPED_ACCEPTABLE_RETURN_DELTA_FLOOR:
        return "risk_capped_better"
    if drawdown_delta < 0.0 and return_delta < 0.0:
        return "limited_better"
    return "mixed"


def _risk_capped_rolling_comparison(
    states: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    config = _load_backfill_config_from_manifest(baseline)
    min_obs = int(_float(_mapping(config.get("evaluation")).get("min_observations_per_window"), 20))
    windows = _rolling_window_inventory(states, min_observations=min_obs)
    metrics = [
        row for window in windows for row in _rolling_metrics_for_window(states, window, min_obs)
    ]
    _rank_rolling_metrics(metrics)
    stability = _rolling_rank_stability(metrics)
    risk = _find_method(_records(stability.get("methods")), "risk_capped_limited_adjustment")
    limited = _find_method(_records(stability.get("methods")), "limited_adjustment")
    stability_delta = _risk_capped_stability_delta(
        _float(risk.get("top_3_frequency")),
        _float(limited.get("top_3_frequency")),
        _float(risk.get("bottom_3_frequency")),
        _float(limited.get("bottom_3_frequency")),
    )
    return {
        "rolling_windows_total": len(windows),
        "risk_capped_top_3_frequency": _float(risk.get("top_3_frequency")),
        "limited_top_3_frequency": _float(limited.get("top_3_frequency")),
        "risk_capped_bottom_3_frequency": _float(risk.get("bottom_3_frequency")),
        "limited_bottom_3_frequency": _float(limited.get("bottom_3_frequency")),
        "stability_delta": stability_delta,
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_capped_stability_delta(
    risk_top: float,
    limited_top: float,
    risk_bottom: float,
    limited_bottom: float,
) -> str:
    if risk_top == 0 and limited_top == 0 and risk_bottom == 0 and limited_bottom == 0:
        return "INSUFFICIENT_DATA"
    if risk_top >= limited_top and risk_bottom <= limited_bottom:
        return "IMPROVED"
    if risk_top < limited_top and risk_bottom > limited_bottom:
        return "WORSE"
    return "MIXED"


def _risk_capped_stability_comparison(
    risk_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited_states = [
        row for row in baseline_states if row.get("target_method") == "limited_adjustment"
    ]
    risk_turnovers = [
        _float(row.get("turnover")) for row in risk_states if row.get("rebalance_event") is True
    ]
    limited_turnovers = [
        _float(row.get("turnover")) for row in limited_states if row.get("rebalance_event") is True
    ]
    risk_semi = [
        _semiconductor_weight(_mapping(row.get("weights")), ("SMH", "SOXX")) for row in risk_states
    ]
    limited_semi = [
        _semiconductor_weight(_mapping(row.get("weights")), ("SMH", "SOXX"))
        for row in limited_states
    ]
    risk_cash = [_float(_mapping(row.get("weights")).get("CASH")) for row in risk_states]
    limited_cash = [_float(_mapping(row.get("weights")).get("CASH")) for row in limited_states]
    large_jump_count_delta = _large_jump_count(risk_states) - _large_jump_count(limited_states)
    payload = {
        "avg_rebalance_turnover_delta": round(
            _mean_float(risk_turnovers) - _mean_float(limited_turnovers),
            10,
        ),
        "max_rebalance_turnover_delta": round(
            max(risk_turnovers or [0.0]) - max(limited_turnovers or [0.0]),
            10,
        ),
        "large_jump_count_delta": large_jump_count_delta,
        "semiconductor_exposure_volatility_delta": round(
            _stddev(risk_semi) - _stddev(limited_semi),
            10,
        ),
        "cash_weight_volatility_delta": round(_stddev(risk_cash) - _stddev(limited_cash), 10),
    }
    payload["stability_conclusion"] = _risk_capped_stability_conclusion(payload)
    return {**payload, **SYSTEM_TARGET_SAFETY}


def _risk_capped_stability_conclusion(payload: Mapping[str, Any]) -> str:
    if payload.get("large_jump_count_delta") is None:
        return "INSUFFICIENT_DATA"
    if (
        _float(payload.get("large_jump_count_delta")) <= 0
        and _float(payload.get("semiconductor_exposure_volatility_delta")) <= 0
    ):
        return "IMPROVED"
    if (
        _float(payload.get("large_jump_count_delta")) > 0
        and _float(payload.get("semiconductor_exposure_volatility_delta")) > 0
    ):
        return "WORSE"
    return "MIXED"


def _risk_capped_review_decision(
    comparison: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _mapping(_mapping(comparison.get("risk_capped_vs_limited_metrics")).get("metrics"))
    rolling = _mapping(comparison.get("risk_capped_rolling_comparison"))
    summary = _mapping(backfill.get("risk_capped_backfill_summary"))
    improvements = {
        "max_drawdown": (
            "IMPROVED" if _float(metrics.get("max_drawdown_delta")) > 0.0 else "WORSE"
        ),
        "rolling_consistency": rolling.get("stability_delta", "INSUFFICIENT_DATA"),
        "semiconductor_exposure": _semiconductor_exposure_decision(
            _float(metrics.get("avg_semiconductor_weight_delta"))
        ),
        "return_preservation": _return_preservation_decision(
            _float(metrics.get("total_return_delta"))
        ),
    }
    reasons = _risk_capped_decision_reasons(improvements, summary)
    if (
        improvements["max_drawdown"] == "IMPROVED"
        and improvements["semiconductor_exposure"] == "REDUCED"
        and improvements["return_preservation"] in {"GOOD", "ACCEPTABLE"}
        and improvements["rolling_consistency"] in {"IMPROVED", "MIXED"}
    ):
        decision = "PROMOTE_TO_RECOMMENDED_RESEARCH"
    elif improvements["return_preservation"] == "POOR":
        decision = "REJECT"
    else:
        decision = "CONTINUE_OBSERVATION"
    confidence = "LOW" if summary.get("data_quality") == "PASS_WITH_WARNINGS" else "MEDIUM"
    return {
        "candidate_method": "risk_capped_limited_adjustment",
        "base_method": "limited_adjustment",
        "decision": decision,
        "decision_confidence": confidence,
        "reason": reasons,
        "improvements_vs_limited": improvements,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": "owner_review_then_forward_confirmation",
        **SYSTEM_TARGET_SAFETY,
    }


def _semiconductor_exposure_decision(delta: float) -> str:
    if delta < -RISK_CAPPED_EXPOSURE_CHANGE_TOLERANCE:
        return "REDUCED"
    if delta > RISK_CAPPED_EXPOSURE_CHANGE_TOLERANCE:
        return "INCREASED"
    return "UNCHANGED"


def _return_preservation_decision(delta: float) -> str:
    if delta >= 0.0:
        return "GOOD"
    if delta >= RISK_CAPPED_ACCEPTABLE_RETURN_DELTA_FLOOR:
        return "ACCEPTABLE"
    return "POOR"


def _risk_capped_decision_reasons(
    improvements: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> list[str]:
    reasons = [
        f"max_drawdown={improvements.get('max_drawdown')}",
        f"rolling_consistency={improvements.get('rolling_consistency')}",
        f"semiconductor_exposure={improvements.get('semiconductor_exposure')}",
        f"return_preservation={improvements.get('return_preservation')}",
        f"data_quality={summary.get('data_quality')}",
        "research_only_no_broker_no_production",
    ]
    return reasons


def _smoothed_limited_weights_for_model_target(
    *,
    base_weights: Mapping[str, Any],
    previous_weights: Mapping[str, Any],
    smoothed_config: Mapping[str, Any],
    model_config: Mapping[str, Any],
    as_of: date,
    variant_id: str,
    regime_context: str,
) -> dict[str, float]:
    return _apply_smoothed_limited_adjustment(
        as_of=as_of,
        base_weights=base_weights,
        previous_smoothed_weights=previous_weights,
        smoothed_config=smoothed_config,
        model_config=model_config,
        variant_id=variant_id,
        regime_context=regime_context,
    )["smoothed_weights"]


def _apply_smoothed_limited_adjustment(
    *,
    as_of: date,
    base_weights: Mapping[str, Any],
    previous_smoothed_weights: Mapping[str, Any],
    smoothed_config: Mapping[str, Any],
    model_config: Mapping[str, Any],
    variant_id: str,
    regime_context: str,
) -> dict[str, Any]:
    base = _normalize_weights(base_weights)
    previous = _normalize_weights(previous_smoothed_weights or base)
    symbols = sorted(
        set(base)
        | set(previous)
        | set(_config_baseline_weights(model_config))
        | set(_texts(_constraints(model_config).get("semiconductor_symbols")))
        | set(_texts(_constraints(model_config).get("defensive_symbols")))
        | {"CASH"}
    )
    policy = _effective_smoothed_policy(smoothed_config, variant_id, regime_context)
    alpha = _float(policy.get("alpha"))
    raw = {
        symbol: _float(previous.get(symbol))
        + alpha * (_float(base.get(symbol)) - _float(previous.get(symbol)))
        for symbol in symbols
    }
    deltas = {symbol: _float(raw.get(symbol)) - _float(previous.get(symbol)) for symbol in symbols}
    max_single = _float(policy.get("max_single_symbol_daily_change"), 1.0)
    deltas = {symbol: max(-max_single, min(max_single, delta)) for symbol, delta in deltas.items()}
    total_abs = sum(abs(delta) for delta in deltas.values())
    max_total = _float(policy.get("max_daily_total_weight_change"), 1.0)
    if total_abs > max_total > 0:
        ratio = max_total / total_abs
        deltas = {symbol: delta * ratio for symbol, delta in deltas.items()}
    smoothed = {
        symbol: max(0.0, _float(previous.get(symbol)) + _float(deltas.get(symbol)))
        for symbol in symbols
    }
    smoothed = _normalize_weights(smoothed)
    smoothed, constraint_events = _enforce_smoothed_constraints(
        smoothed,
        smoothed_config=smoothed_config,
        model_config=model_config,
        as_of=as_of,
        target_method=SMOOTHED_VARIANT_TO_METHOD[variant_id],
    )
    smoothing_events = _smoothed_smoothing_events(
        as_of=as_of,
        target_method=SMOOTHED_VARIANT_TO_METHOD[variant_id],
        symbols=symbols,
        base=base,
        previous=previous,
        smoothed=smoothed,
    )
    lag_events = _smoothed_lag_events(
        as_of=as_of,
        target_method=SMOOTHED_VARIANT_TO_METHOD[variant_id],
        symbols=symbols,
        base=base,
        previous=previous,
        smoothed=smoothed,
        regime_context=regime_context,
        variant_id=variant_id,
    )
    return {
        "base_weights": base,
        "previous_smoothed_weights": previous,
        "smoothed_weights": smoothed,
        "effective_policy": policy,
        "regime_context": regime_context,
        "smoothing_events": [*smoothing_events, *constraint_events],
        "lag_events": lag_events,
        **SYSTEM_TARGET_SAFETY,
    }


def _effective_smoothed_policy(
    smoothed_config: Mapping[str, Any],
    variant_id: str,
    regime_context: str,
) -> dict[str, Any]:
    variant = dict(_mapping(_mapping(smoothed_config.get("variants")).get(variant_id)))
    policy = {
        "smoothing_window_days": int(_float(variant.get("smoothing_window_days"), 1)),
        "smoothing_type": _text(variant.get("smoothing_type"), "exponential"),
        "alpha": _float(variant.get("alpha"), 1.0),
        "min_signal_persistence_days": int(_float(variant.get("min_signal_persistence_days"), 1)),
        "max_daily_total_weight_change": _float(variant.get("max_daily_total_weight_change"), 1.0),
        "max_single_symbol_daily_change": _float(
            variant.get("max_single_symbol_daily_change"), 1.0
        ),
    }
    context = _mapping(_mapping(smoothed_config.get("regime_context")).get(regime_context))
    if regime_context == "sideways_choppy" and context.get("increase_smoothing_strength") is True:
        policy["alpha"] = _float(policy.get("alpha")) * _float(context.get("alpha_multiplier"), 1.0)
        if context.get("max_daily_total_weight_change") is not None:
            policy["max_daily_total_weight_change"] = min(
                _float(policy.get("max_daily_total_weight_change"), 1.0),
                _float(context.get("max_daily_total_weight_change")),
            )
        policy["min_signal_persistence_days"] = int(
            _float(policy.get("min_signal_persistence_days"))
            + _float(context.get("min_signal_persistence_days_add"), 0.0)
        )
    elif regime_context == "strong_recovery" and context.get("reduce_smoothing_strength") is True:
        policy["alpha"] = _float(policy.get("alpha")) * _float(context.get("alpha_multiplier"), 1.0)
        policy["allow_faster_risk_restore"] = context.get("allow_faster_risk_restore") is True
    policy["alpha"] = round(max(0.0, min(1.0, _float(policy.get("alpha")))), 10)
    policy["max_daily_total_weight_change"] = round(
        max(0.0, min(1.0, _float(policy.get("max_daily_total_weight_change")))),
        10,
    )
    policy["max_single_symbol_daily_change"] = round(
        max(
            0.0,
            min(
                _float(policy.get("max_daily_total_weight_change")),
                _float(policy.get("max_single_symbol_daily_change")),
            ),
        ),
        10,
    )
    return policy


def _enforce_smoothed_constraints(
    weights: Mapping[str, Any],
    *,
    smoothed_config: Mapping[str, Any],
    model_config: Mapping[str, Any],
    as_of: date,
    target_method: str,
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    adjusted = {symbol: _float(value) for symbol, value in _normalize_weights(weights).items()}
    constraints = {**_constraints(model_config), **_mapping(smoothed_config.get("constraints"))}
    semiconductors = _risk_capped_semiconductor_symbols(model_config)
    defensive = _risk_capped_defensive_symbols(model_config)
    symbols = sorted(set(adjusted) | set(semiconductors) | defensive | {"CASH"})
    for symbol in symbols:
        adjusted.setdefault(symbol, 0.0)
    risk_symbols = [symbol for symbol in symbols if symbol not in defensive and symbol != "CASH"]
    events: list[dict[str, Any]] = []

    def allocate_to_cash(amount: float, reason: str, source: str) -> None:
        if amount <= 0:
            return
        before = _float(adjusted.get("CASH"))
        adjusted["CASH"] = round(before + amount, 10)
        events.append(
            {
                "as_of": as_of.isoformat(),
                "target_method": target_method,
                "symbol": source,
                "base_delta": 0.0,
                "smoothed_delta": round(-amount, 10),
                "delta_reduction": round(amount, 10),
                "event_type": "constraint_reallocation",
                "reason": reason,
                **SYSTEM_TARGET_SAFETY,
            }
        )

    def reduce_symbol(symbol: str, cap: float, reason: str) -> None:
        before = _float(adjusted.get(symbol))
        if before <= cap:
            return
        excess = round(before - cap, 10)
        adjusted[symbol] = round(cap, 10)
        allocate_to_cash(excess, reason, symbol)

    def reduce_group(symbol_list: Sequence[str], cap: float, reason: str) -> None:
        selected = [symbol for symbol in symbol_list if _float(adjusted.get(symbol)) > 0]
        total = sum(_float(adjusted.get(symbol)) for symbol in selected)
        if total <= cap or not selected:
            return
        reduction = total - cap
        for symbol in selected:
            share = _float(adjusted.get(symbol)) / total if total > 0 else 0.0
            adjusted[symbol] = round(_float(adjusted.get(symbol)) - reduction * share, 10)
        allocate_to_cash(reduction, reason, "GROUP")

    max_single = _float(constraints.get("max_single_symbol_weight"), 1.0)
    for symbol in symbols:
        if symbol != "CASH":
            reduce_symbol(symbol, max_single, "max_single_symbol_weight")
    reduce_group(
        semiconductors,
        _float(constraints.get("max_semiconductor_weight"), 1.0),
        "max_semiconductor_weight",
    )
    reduce_group(
        risk_symbols,
        _float(constraints.get("max_total_risk_asset_weight"), 1.0),
        "max_total_risk_asset_weight",
    )
    min_cash = _float(constraints.get("min_cash_weight"), 0.0)
    if _float(adjusted.get("CASH")) < min_cash:
        need = min_cash - _float(adjusted.get("CASH"))
        reduce_group(
            risk_symbols,
            max(0.0, _group_weight(adjusted, risk_symbols) - need),
            "min_cash_weight",
        )
    return _normalize_weights(adjusted), events


def _smoothed_smoothing_events(
    *,
    as_of: date,
    target_method: str,
    symbols: Sequence[str],
    base: Mapping[str, Any],
    previous: Mapping[str, Any],
    smoothed: Mapping[str, Any],
) -> list[dict[str, Any]]:
    events = []
    for symbol in symbols:
        base_delta = _float(base.get(symbol)) - _float(previous.get(symbol))
        smoothed_delta = _float(smoothed.get(symbol)) - _float(previous.get(symbol))
        delta_reduction = abs(base_delta) - abs(smoothed_delta)
        if abs(delta_reduction) <= 0.0000001:
            continue
        events.append(
            {
                "as_of": as_of.isoformat(),
                "target_method": target_method,
                "symbol": symbol,
                "base_delta": round(base_delta, 10),
                "smoothed_delta": round(smoothed_delta, 10),
                "delta_reduction": round(delta_reduction, 10),
                "event_type": "weight_smoothing",
                "reason": "exponential_smoothing",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return events


def _smoothed_lag_events(
    *,
    as_of: date,
    target_method: str,
    symbols: Sequence[str],
    base: Mapping[str, Any],
    previous: Mapping[str, Any],
    smoothed: Mapping[str, Any],
    regime_context: str,
    variant_id: str,
) -> list[dict[str, Any]]:
    if regime_context != "strong_recovery":
        return []
    events = []
    for symbol in symbols:
        base_delta = _float(base.get(symbol)) - _float(previous.get(symbol))
        smoothed_delta = _float(smoothed.get(symbol)) - _float(previous.get(symbol))
        if base_delta <= 0 or smoothed_delta >= base_delta:
            continue
        reduction_ratio = 1.0 - (smoothed_delta / base_delta if base_delta else 1.0)
        lag_risk = "HIGH" if variant_id == "smooth_weights_5d" else "MEDIUM"
        if reduction_ratio < 0.25:
            lag_risk = "LOW"
        events.append(
            {
                "as_of": as_of.isoformat(),
                "target_method": target_method,
                "symbol": symbol,
                "base_signal_direction": "increase",
                "smoothed_direction": (
                    "hold_or_slow_increase" if smoothed_delta >= 0 else "opposite_direction"
                ),
                "regime_context": regime_context,
                "lag_risk": lag_risk,
                "reason": "smoothing_may_lag_fast_regime_change",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return events


def _smoothed_weight_jump_reduction_summary(
    target_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for row in target_rows:
        previous = _mapping(row.get("previous_smoothed_weights"))
        base = _mapping(row.get("base_weights"))
        smoothed = _mapping(row.get("smoothed_weights"))
        base_change = sum(abs(value) for value in _weight_deltas(previous, base).values())
        smoothed_change = sum(abs(value) for value in _weight_deltas(previous, smoothed).values())
        base_large = 1 if base_change >= INSTABILITY_WEIGHT_JUMP_THRESHOLD else 0
        smoothed_large = 1 if smoothed_change >= INSTABILITY_WEIGHT_JUMP_THRESHOLD else 0
        status = "INSUFFICIENT_DATA"
        if base_large or smoothed_large:
            status = "IMPROVED" if smoothed_large < base_large else "WORSE"
            if smoothed_large == base_large:
                status = "MIXED"
        elif smoothed_change <= base_change:
            status = "IMPROVED"
        rows.append(
            {
                "target_method": row.get("target_method"),
                "base_large_jump_count": base_large,
                "smoothed_large_jump_count": smoothed_large,
                "jump_reduction": base_large - smoothed_large,
                "avg_total_abs_weight_change_base": round(base_change, 10),
                "avg_total_abs_weight_change_smoothed": round(smoothed_change, 10),
                "smoothing_event_count": sum(
                    1
                    for symbol in set(base) | set(previous) | set(smoothed)
                    if abs(_float(base.get(symbol)) - _float(smoothed.get(symbol))) > 0.0000001
                ),
                "status": status,
            }
        )
    return {"target_methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_backfill_summary(
    source_manifest: Mapping[str, Any],
    states: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    smoothing_events: Sequence[Mapping[str, Any]],
    lag_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    method_rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_states = [row for row in states if row.get("target_method") == method]
        method_ledger = [row for row in ledger if row.get("target_method") == method]
        turnovers = [_float(row.get("turnover")) for row in method_ledger]
        method_rows.append(
            {
                "method": method,
                "date_start": source_manifest.get("date_start"),
                "date_end": source_manifest.get("date_end"),
                "rebalance_count": len(method_ledger),
                "smoothing_event_count": sum(
                    1 for row in smoothing_events if row.get("target_method") == method
                ),
                "lag_event_count": sum(
                    1 for row in lag_events if row.get("target_method") == method
                ),
                "avg_turnover": round(_mean_float(turnovers), 10),
                "max_turnover": round(max(turnovers or [0.0]), 10),
                "large_jump_count": _large_jump_count(method_states),
                "data_quality": source_manifest.get("data_quality_status"),
                "broker_action_taken": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {
        "methods": method_rows,
        "date_start": source_manifest.get("date_start"),
        "date_end": source_manifest.get("date_end"),
        "smoothing_event_count": len(smoothing_events),
        "lag_event_count": len(lag_events),
        "data_quality": source_manifest.get("data_quality_status"),
        "broker_action_taken": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_vs_limited_metrics(
    smoothed_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    risk_states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    comparisons = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_states = [row for row in smoothed_states if row.get("target_method") == method]
        for baseline_method, baseline_rows in (
            (
                "limited_adjustment",
                [
                    row
                    for row in baseline_states
                    if row.get("target_method") == "limited_adjustment"
                ],
            ),
            ("risk_capped_limited_adjustment", risk_states),
            (
                "static_baseline",
                [row for row in baseline_states if row.get("target_method") == "static_baseline"],
            ),
            (
                "no_trade_baseline",
                [row for row in baseline_states if row.get("target_method") == "no_trade_baseline"],
            ),
            (
                "consensus_target",
                [row for row in baseline_states if row.get("target_method") == "consensus_target"],
            ),
            (
                "defensive_limited_adjustment",
                [
                    row
                    for row in baseline_states
                    if row.get("target_method") == "defensive_limited_adjustment"
                ],
            ),
        ):
            method_metrics = _state_path_metrics(method_states, min_observations=2)
            baseline_metrics = _state_path_metrics(baseline_rows, min_observations=2)
            values = {
                "method_a": method,
                "method_b": baseline_method,
                "total_return_delta": round(
                    _float(method_metrics.get("total_return"))
                    - _float(baseline_metrics.get("total_return")),
                    10,
                ),
                "annualized_return_delta": round(
                    _float(method_metrics.get("annualized_return"))
                    - _float(baseline_metrics.get("annualized_return")),
                    10,
                ),
                "max_drawdown_delta": round(
                    _float(method_metrics.get("max_drawdown"))
                    - _float(baseline_metrics.get("max_drawdown")),
                    10,
                ),
                "realized_volatility_delta": round(
                    _float(method_metrics.get("realized_volatility"))
                    - _float(baseline_metrics.get("realized_volatility")),
                    10,
                ),
                "turnover_delta": round(
                    _float(method_metrics.get("turnover"))
                    - _float(baseline_metrics.get("turnover")),
                    10,
                ),
                "large_jump_count_delta": _large_jump_count(method_states)
                - _large_jump_count(baseline_rows),
                "rolling_consistency_delta": "INSUFFICIENT_DATA",
            }
            values["conclusion"] = _smoothed_comparison_conclusion(values)
            comparisons.append(values)
    return {"comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _find_comparison(
    rows: Sequence[Mapping[str, Any]],
    method_a: str,
    method_b: str,
) -> dict[str, Any]:
    for row in rows:
        if row.get("method_a") == method_a and row.get("method_b") == method_b:
            return dict(row)
    return {}


def _smoothed_comparison_conclusion(metrics: Mapping[str, Any]) -> str:
    return_delta = _float(metrics.get("total_return_delta"))
    drawdown_delta = _float(metrics.get("max_drawdown_delta"))
    turnover_delta = _float(metrics.get("turnover_delta"))
    jump_delta = _float(metrics.get("large_jump_count_delta"))
    if (
        return_delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR
        and drawdown_delta >= 0.0
        and turnover_delta <= 0.0
        and jump_delta <= 0.0
    ):
        return "smoothed_better"
    if return_delta < SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR and drawdown_delta < 0.0:
        return "limited_better"
    return "mixed"


def _smoothed_regime_comparison(
    states: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    config = _load_backfill_config_from_manifest(baseline)
    labels = _regime_labels_from_states(states, config)
    rows = []
    for regime in ("sideways_choppy", "strong_recovery"):
        date_set = {day for day, label in labels.items() if label == regime}
        row = {"regime": regime, "sample_count": len(date_set)}
        for method, prefix in (
            ("smooth_weights_3d_limited_adjustment", "smooth_3d"),
            ("smooth_weights_5d_limited_adjustment", "smooth_5d"),
        ):
            method_rows = [
                item
                for item in states
                if item.get("target_method") == method and item.get("date") in date_set
            ]
            limited_rows = [
                item
                for item in states
                if item.get("target_method") == "limited_adjustment"
                and item.get("date") in date_set
            ]
            method_metrics = _sample_return_metrics(method_rows, min_sample=1)
            limited_metrics = _sample_return_metrics(limited_rows, min_sample=1)
            row[f"{prefix}_return_delta_vs_limited"] = round(
                _float(method_metrics.get("total_return"))
                - _float(limited_metrics.get("total_return")),
                10,
            )
            row[f"{prefix}_drawdown_delta_vs_limited"] = round(
                _float(method_metrics.get("max_drawdown"))
                - _float(limited_metrics.get("max_drawdown")),
                10,
            )
            row[f"{prefix}_turnover_delta_vs_limited"] = round(
                sum(_float(item.get("turnover")) for item in method_rows)
                - sum(_float(item.get("turnover")) for item in limited_rows),
                10,
            )
        if regime == "sideways_choppy":
            row["smooth_3d_conclusion"] = _smoothed_regime_conclusion(
                _float(row.get("smooth_3d_return_delta_vs_limited")),
                _float(row.get("smooth_3d_drawdown_delta_vs_limited")),
                _float(row.get("smooth_3d_turnover_delta_vs_limited")),
                int(row["sample_count"]),
            )
        else:
            row["smooth_3d_lag_cost"] = row.get("smooth_3d_return_delta_vs_limited", 0.0)
            row["smooth_5d_lag_cost"] = row.get("smooth_5d_return_delta_vs_limited", 0.0)
            row["lag_status"] = _smoothed_lag_cost_status(
                min(_float(row["smooth_3d_lag_cost"]), _float(row["smooth_5d_lag_cost"]))
            )
        rows.append({**row, **SYSTEM_TARGET_SAFETY})
    return {"regimes": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_regime_conclusion(
    return_delta: float,
    drawdown_delta: float,
    turnover_delta: float,
    sample_count: int,
) -> str:
    if sample_count <= 0:
        return "insufficient_data"
    if (
        return_delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR
        and drawdown_delta >= 0
        and turnover_delta <= 0
    ):
        return "improved"
    if return_delta < SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR and drawdown_delta < 0:
        return "worse"
    return "mixed"


def _smoothed_rolling_comparison(
    states: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    config = _load_backfill_config_from_manifest(baseline)
    min_obs = int(_float(_mapping(config.get("evaluation")).get("min_observations_per_window"), 20))
    windows = _rolling_window_inventory(states, min_observations=min_obs)
    metrics = [
        row for window in windows for row in _rolling_metrics_for_window(states, window, min_obs)
    ]
    _rank_rolling_metrics(metrics)
    stability = _rolling_rank_stability(metrics)
    limited = _find_method(_records(stability.get("methods")), "limited_adjustment")
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        item = _find_method(_records(stability.get("methods")), method)
        rows.append(
            {
                "method": method,
                "target_method": method,
                "rolling_windows_total": len(windows),
                "top_3_frequency_delta_vs_limited": round(
                    _float(item.get("top_3_frequency")) - _float(limited.get("top_3_frequency")),
                    10,
                ),
                "bottom_3_frequency_delta_vs_limited": round(
                    _float(item.get("bottom_3_frequency"))
                    - _float(limited.get("bottom_3_frequency")),
                    10,
                ),
                "rolling_consistency_delta": _risk_capped_stability_delta(
                    _float(item.get("top_3_frequency")),
                    _float(limited.get("top_3_frequency")),
                    _float(item.get("bottom_3_frequency")),
                    _float(limited.get("bottom_3_frequency")),
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_stability_comparison(
    smoothed_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited_states = [
        row for row in baseline_states if row.get("target_method") == "limited_adjustment"
    ]
    limited_turnovers = [
        _float(row.get("turnover")) for row in limited_states if row.get("rebalance_event") is True
    ]
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_states = [row for row in smoothed_states if row.get("target_method") == method]
        method_turnovers = [
            _float(row.get("turnover"))
            for row in method_states
            if row.get("rebalance_event") is True
        ]
        payload = {
            "method": method,
            "target_method": method,
            "avg_rebalance_turnover_delta_vs_limited": round(
                _mean_float(method_turnovers) - _mean_float(limited_turnovers),
                10,
            ),
            "max_rebalance_turnover_delta_vs_limited": round(
                max(method_turnovers or [0.0]) - max(limited_turnovers or [0.0]),
                10,
            ),
            "large_jump_count_delta_vs_limited": _large_jump_count(method_states)
            - _large_jump_count(limited_states),
            "weight_flip_count_delta_vs_limited": _weight_flip_count(method_states)
            - _weight_flip_count(limited_states),
        }
        payload["stability_conclusion"] = _smoothed_stability_conclusion(payload)
        rows.append({**payload, **SYSTEM_TARGET_SAFETY})
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_stability_conclusion(payload: Mapping[str, Any]) -> str:
    if (
        _float(payload.get("large_jump_count_delta_vs_limited")) <= 0
        and _float(payload.get("avg_rebalance_turnover_delta_vs_limited")) <= 0
        and _float(payload.get("weight_flip_count_delta_vs_limited")) <= 0
    ):
        return "IMPROVED"
    if (
        _float(payload.get("large_jump_count_delta_vs_limited")) > 0
        and _float(payload.get("avg_rebalance_turnover_delta_vs_limited")) > 0
    ):
        return "WORSE"
    return "MIXED"


def _smoothed_lag_cost_analysis(
    states: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    config = _load_backfill_config_from_manifest(baseline)
    labels = _regime_labels_from_states(states, config)
    strong_dates = {day for day, label in labels.items() if label == "strong_recovery"}
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_rows = [
            row
            for row in states
            if row.get("target_method") == method and row.get("date") in strong_dates
        ]
        limited_rows = [
            row
            for row in states
            if row.get("target_method") == "limited_adjustment" and row.get("date") in strong_dates
        ]
        method_metrics = _sample_return_metrics(method_rows, min_sample=1)
        limited_metrics = _sample_return_metrics(limited_rows, min_sample=1)
        strong_cost = round(
            _float(method_metrics.get("total_return"))
            - _float(limited_metrics.get("total_return")),
            10,
        )
        lag_events = [
            event
            for row in ledger
            if row.get("target_method") == method
            for event in _records(row.get("lag_events"))
        ]
        missed_upside = sum(
            1 for event in lag_events if event.get("lag_risk") in {"MEDIUM", "HIGH"}
        )
        rows.append(
            {
                "method": method,
                "target_method": method,
                "strong_recovery_lag_cost": strong_cost,
                "fast_regime_change_lag_cost": strong_cost,
                "missed_upside_count": missed_upside,
                "lag_cost_status": _smoothed_lag_cost_status(strong_cost),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_lag_cost_status(value: float) -> str:
    if value <= SMOOTHED_LAG_COST_HIGH_THRESHOLD:
        return "HIGH"
    if value <= SMOOTHED_LAG_COST_MEDIUM_THRESHOLD:
        return "MEDIUM"
    return "LOW"


def _weight_flip_count(rows: Sequence[Mapping[str, Any]]) -> int:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    previous_deltas: dict[str, float] = {}
    count = 0
    previous_weights: Mapping[str, Any] | None = None
    for row in ordered:
        weights = _mapping(row.get("weights"))
        if previous_weights is not None:
            deltas = _weight_deltas(previous_weights, weights)
            for symbol, delta in deltas.items():
                if delta == 0:
                    continue
                previous_delta = previous_deltas.get(symbol)
                if previous_delta is not None and previous_delta * delta < 0:
                    count += 1
                previous_deltas[symbol] = delta
        previous_weights = weights
    return count


def _smoothed_review_decision(
    comparison: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> dict[str, Any]:
    comparisons = _records(
        _mapping(comparison.get("smoothed_vs_limited_metrics")).get("comparisons")
    )
    primary = _find_comparison(
        comparisons, "smooth_weights_3d_limited_adjustment", "limited_adjustment"
    )
    rolling = _find_method(
        _records(_mapping(comparison.get("smoothed_rolling_comparison")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    lag = _find_method(
        _records(_mapping(comparison.get("smoothing_lag_cost_analysis")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    summary = _mapping(backfill.get("smoothed_backfill_summary"))
    improvements = {
        "rolling_consistency": rolling.get("rolling_consistency_delta", "INSUFFICIENT_DATA"),
        "turnover": _delta_improvement_status(
            _float(primary.get("turnover_delta")), lower_is_better=True
        ),
        "weight_jumps": _delta_improvement_status(
            _float(primary.get("large_jump_count_delta")), lower_is_better=True
        ),
        "return_preservation": _smoothed_return_preservation_decision(
            _float(primary.get("total_return_delta"))
        ),
        "drawdown": _delta_improvement_status(
            _float(primary.get("max_drawdown_delta")), lower_is_better=False
        ),
    }
    lag_risk = lag.get("lag_cost_status", "INSUFFICIENT_DATA")
    if (
        improvements["rolling_consistency"] == "IMPROVED"
        and improvements["turnover"] == "IMPROVED"
        and improvements["weight_jumps"] == "IMPROVED"
        and improvements["return_preservation"] in {"GOOD", "ACCEPTABLE"}
        and lag_risk in {"LOW", "MEDIUM"}
    ):
        decision = "PROMOTE_TO_RECOMMENDED_RESEARCH"
    elif improvements["return_preservation"] == "POOR" or lag_risk == "HIGH":
        decision = "REJECT"
    else:
        decision = "CONTINUE_OBSERVATION"
    confidence = "LOW" if summary.get("data_quality") == "PASS_WITH_WARNINGS" else "MEDIUM"
    return {
        "candidate_methods": [
            "smooth_weights_3d_limited_adjustment",
            "smooth_weights_5d_limited_adjustment",
        ],
        "base_method": "limited_adjustment",
        "recommended_method": "smooth_weights_3d_limited_adjustment",
        "secondary_method": "smooth_weights_5d_limited_adjustment",
        "decision": decision,
        "decision_confidence": confidence,
        "improvements_vs_limited": improvements,
        "lag_risk": lag_risk,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": "owner_review_then_forward_confirmation",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_return_preservation_decision(delta: float) -> str:
    if delta >= 0.0:
        return "GOOD"
    if delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR:
        return "ACCEPTABLE"
    return "POOR"


def _smoothed_decision_reason_breakdown(
    review: Mapping[str, Any],
    comparison: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(review.get("smoothed_decision"))
    support = _smoothed_metric_support_matrix(comparison)
    primary = _primary_smoothed_comparison(comparison, "smooth_weights_3d_limited_adjustment")
    lag = _method_lag_row(comparison, "smooth_weights_3d_limited_adjustment")
    summary = _mapping(backfill.get("smoothed_backfill_summary"))
    confidence = _text(decision.get("decision_confidence"), "LOW")
    supporting = _smoothed_supporting_reasons(decision, support, primary)
    blocking = [
        {
            "reason": "requires_forward_confirmation",
            "severity": "REVIEW_REQUIRED",
            "blocking": True,
            "evidence": [
                {
                    "field": "requires_forward_confirmation",
                    "value": decision.get("requires_forward_confirmation"),
                }
            ],
            **SYSTEM_TARGET_SAFETY,
        }
    ]
    if confidence == "LOW":
        blocking.append(
            {
                "reason": "confidence_low",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
                "evidence": [{"field": "decision_confidence", "value": confidence}],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if summary.get("data_quality") == "PASS_WITH_WARNINGS":
        blocking.append(
            {
                "reason": "data_quality_pass_with_warnings",
                "severity": "WARNING",
                "blocking": False,
                "evidence": [{"field": "data_quality", "value": summary.get("data_quality")}],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if lag.get("lag_cost_status") in {"MEDIUM", "HIGH", "INSUFFICIENT_DATA"}:
        blocking.append(
            {
                "reason": "lag_cost_requires_forward_watch",
                "severity": "REVIEW_REQUIRED",
                "blocking": lag.get("lag_cost_status") == "HIGH",
                "evidence": [
                    {"field": "lag_cost_status", "value": lag.get("lag_cost_status")},
                    {
                        "field": "strong_recovery_lag_cost",
                        "value": lag.get("strong_recovery_lag_cost"),
                    },
                ],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    why_not_promote = [
        "forward confirmation target events have not matured",
        f"decision_confidence={confidence}",
    ]
    if summary.get("data_quality") == "PASS_WITH_WARNINGS":
        why_not_promote.append("cached data quality passed with warnings")
    if lag.get("lag_cost_status") != "LOW":
        why_not_promote.append(f"lag cost still requires observation: {lag.get('lag_cost_status')}")
    if decision.get("decision") != "PROMOTE_TO_RECOMMENDED_RESEARCH":
        why_not_promote.append(f"review decision remains {decision.get('decision')}")
    why_not_reject = [
        f"smooth_3d_vs_limited_conclusion={primary.get('conclusion')}",
        f"return_delta_vs_limited={primary.get('total_return_delta')}",
        f"turnover_delta_vs_limited={primary.get('turnover_delta')}",
        "no broker or production mutation is proposed",
    ]
    return {
        "review_id": review.get("review_id"),
        "decision": decision.get("decision"),
        "confidence": confidence,
        "candidate_methods": list(decision.get("candidate_methods") or []),
        "recommended_method": decision.get("recommended_method"),
        "secondary_method": decision.get("secondary_method"),
        "supporting_reasons": supporting,
        "blocking_reasons": blocking,
        "why_not_promote": why_not_promote,
        "why_not_reject": why_not_reject,
        "next_required_evidence": [
            "smooth_3d_vs_limited forward events",
            "smooth_3d_vs_static_baseline forward events",
            "sideways_choppy improvement samples",
            "strong_recovery lag watch samples",
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_supporting_reasons(
    decision: Mapping[str, Any],
    support: Mapping[str, Any],
    primary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = _records(support.get("metrics"))
    by_metric = {row.get("metric"): row for row in rows}
    reasons: list[dict[str, Any]] = []
    for metric, reason in (
        ("turnover", "reduces_turnover"),
        ("weight_jump", "reduces_weight_jump_or_signal_churn"),
        ("rolling_consistency", "improves_rolling_consistency"),
        ("return_preservation", "preserves_limited_adjustment_return_edge"),
        ("sideways_choppy", "sideways_choppy_observation_supported"),
    ):
        row = _mapping(by_metric.get(metric))
        status = _text(row.get("smooth_3d_status"), "INSUFFICIENT_DATA")
        if status in {"IMPROVED", "MIXED", "GOOD", "ACCEPTABLE"}:
            reasons.append(
                {
                    "reason": reason,
                    "evidence": [
                        {"metric": metric, "smooth_3d_status": status},
                        {
                            "metric": "smooth_3d_vs_limited_conclusion",
                            "value": primary.get("conclusion"),
                        },
                    ],
                    "confidence": decision.get("decision_confidence", "LOW"),
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    if not reasons:
        reasons.append(
            {
                "reason": "historical_smoothed_comparison_available",
                "evidence": [{"metric": "comparison", "value": primary.get("conclusion")}],
                "confidence": decision.get("decision_confidence", "LOW"),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return reasons


def _smoothed_metric_support_matrix(comparison: Mapping[str, Any]) -> dict[str, Any]:
    metrics: list[dict[str, Any]] = []
    for metric in (
        "rolling_consistency",
        "turnover",
        "weight_jump",
        "signal_churn",
        "return_preservation",
        "sideways_choppy",
        "lag_cost",
    ):
        row: dict[str, Any] = {"metric": metric, **SYSTEM_TARGET_SAFETY}
        for method, prefix in (
            ("smooth_weights_3d_limited_adjustment", "smooth_3d"),
            ("smooth_weights_5d_limited_adjustment", "smooth_5d"),
        ):
            row[f"{prefix}_status"] = _smoothed_metric_status(comparison, method, metric)
        row["supports_promotion"] = metric != "lag_cost" and row.get("smooth_3d_status") in {
            "IMPROVED",
            "GOOD",
            "ACCEPTABLE",
        }
        metrics.append(row)
    return {"metrics": metrics, **SYSTEM_TARGET_SAFETY}


def _smoothed_metric_status(
    comparison: Mapping[str, Any],
    method: str,
    metric: str,
) -> str:
    primary = _primary_smoothed_comparison(comparison, method)
    if metric == "turnover":
        return _delta_improvement_status(
            _float(primary.get("turnover_delta")),
            lower_is_better=True,
        )
    if metric == "weight_jump":
        return _delta_improvement_status(
            _float(primary.get("large_jump_count_delta")),
            lower_is_better=True,
        )
    if metric == "return_preservation":
        return _smoothed_return_preservation_decision(_float(primary.get("total_return_delta")))
    if metric == "rolling_consistency":
        return _text(_method_rolling_row(comparison, method).get("rolling_consistency_delta"))
    if metric == "signal_churn":
        return _delta_improvement_status(
            _float(
                _method_stability_row(comparison, method).get("weight_flip_count_delta_vs_limited")
            ),
            lower_is_better=True,
        )
    if metric == "sideways_choppy":
        return _status_upper(
            _text(
                _sideways_regime_row(comparison).get(
                    _smoothed_regime_prefix(method) + "_conclusion"
                )
            )
        )
    if metric == "lag_cost":
        return _text(_method_lag_row(comparison, method).get("lag_cost_status"))
    return "INSUFFICIENT_DATA"


def _smoothing_benefit_summary(comparison: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        primary = _primary_smoothed_comparison(comparison, method)
        rolling = _method_rolling_row(comparison, method)
        stability = _method_stability_row(comparison, method)
        weight_jump_reduction = round(-_float(primary.get("large_jump_count_delta")), 10)
        turnover_reduction = round(-_float(primary.get("turnover_delta")), 10)
        signal_churn_reduction = round(
            -_float(stability.get("weight_flip_count_delta_vs_limited")),
            10,
        )
        rolling_delta = _text(rolling.get("rolling_consistency_delta"), "INSUFFICIENT_DATA")
        rows.append(
            {
                "method": method,
                "weight_jump_reduction": weight_jump_reduction,
                "turnover_reduction": turnover_reduction,
                "signal_churn_reduction": signal_churn_reduction,
                "rolling_consistency_delta": rolling_delta,
                "benefit_status": _smoothed_benefit_status(
                    weight_jump_reduction,
                    turnover_reduction,
                    signal_churn_reduction,
                    rolling_delta,
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothing_lag_cost_summary(comparison: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        lag = _method_lag_row(comparison, method)
        missed_upside_count = int(_float(lag.get("missed_upside_count")))
        rows.append(
            {
                "method": method,
                "strong_recovery_lag_cost": round(
                    _float(lag.get("strong_recovery_lag_cost")),
                    10,
                ),
                "fast_regime_change_lag_cost": round(
                    _float(lag.get("fast_regime_change_lag_cost")),
                    10,
                ),
                "missed_upside_count": missed_upside_count,
                "delayed_risk_on_count": missed_upside_count,
                "lag_cost_status": _text(lag.get("lag_cost_status"), "INSUFFICIENT_DATA"),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothing_benefit_lag_tradeoff_matrix(
    benefit: Mapping[str, Any],
    lag: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        benefit_row = _find_method(_records(benefit.get("methods")), method)
        lag_row = _find_method(_records(lag.get("methods")), method)
        benefit_status = _text(benefit_row.get("benefit_status"), "INSUFFICIENT_DATA")
        lag_status = _text(lag_row.get("lag_cost_status"), "INSUFFICIENT_DATA")
        tradeoff = _smoothed_tradeoff_status(benefit_status, lag_status)
        rows.append(
            {
                "method": method,
                "benefit_status": benefit_status,
                "lag_cost_status": lag_status,
                "tradeoff_status": tradeoff,
                "recommendation": _smoothed_tradeoff_recommendation(tradeoff),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_sideways_validation_summary(
    smoothed: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    states = [
        *_records(baseline.get("backfill_method_states")),
        *_records(smoothed.get("smoothed_method_states")),
    ]
    labels = _regime_labels_from_states(states, _load_backfill_config_from_manifest(baseline))
    sideways_dates = {day for day, label in labels.items() if label == "sideways_choppy"}
    limited_rows = _method_rows_for_dates(
        _records(baseline.get("backfill_method_states")),
        "limited_adjustment",
        sideways_dates,
    )
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_rows = _method_rows_for_dates(
            _records(smoothed.get("smoothed_method_states")),
            method,
            sideways_dates,
        )
        return_delta, drawdown_delta = _return_drawdown_delta(method_rows, limited_rows)
        turnover_delta = round(
            sum(_float(row.get("turnover")) for row in method_rows)
            - sum(_float(row.get("turnover")) for row in limited_rows),
            10,
        )
        signal_churn_delta = _weight_flip_count(method_rows) - _weight_flip_count(limited_rows)
        weight_jump_delta = _large_jump_count(method_rows) - _large_jump_count(limited_rows)
        rows.append(
            {
                "method": method,
                "sample_count": len(method_rows),
                "return_delta_vs_limited": return_delta,
                "drawdown_delta_vs_limited": drawdown_delta,
                "turnover_delta_vs_limited": turnover_delta,
                "signal_churn_delta_vs_limited": signal_churn_delta,
                "weight_jump_delta_vs_limited": weight_jump_delta,
                "sideways_status": _smoothed_sideways_status(
                    len(method_rows),
                    return_delta,
                    drawdown_delta,
                    turnover_delta,
                    signal_churn_delta,
                    weight_jump_delta,
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"regime": "sideways_choppy", "methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_recovery_lag_validation_summary(
    smoothed: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    states = [
        *_records(baseline.get("backfill_method_states")),
        *_records(smoothed.get("smoothed_method_states")),
    ]
    labels = _regime_labels_from_states(states, _load_backfill_config_from_manifest(baseline))
    recovery_dates = {day for day, label in labels.items() if label == "strong_recovery"}
    limited_rows = _method_rows_for_dates(
        _records(baseline.get("backfill_method_states")),
        "limited_adjustment",
        recovery_dates,
    )
    ledger = _records(smoothed.get("smoothed_trade_ledger"))
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_rows = _method_rows_for_dates(
            _records(smoothed.get("smoothed_method_states")),
            method,
            recovery_dates,
        )
        return_delta, _drawdown_delta = _return_drawdown_delta(method_rows, limited_rows)
        delay_days = _risk_on_delay_proxy_days(ledger, method, recovery_dates)
        rows.append(
            {
                "method": method,
                "sample_count": len(method_rows),
                "return_delta_vs_limited": return_delta,
                "risk_on_response_delay_days": delay_days,
                "missed_upside": round(max(0.0, -return_delta), 10),
                "lag_status": (
                    "INSUFFICIENT_DATA"
                    if not method_rows
                    else _smoothed_lag_cost_status(return_delta)
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"regime": "strong_recovery", "methods": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_confirmation_targets(
    review: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(review.get("smoothed_decision"))
    sideways = _find_method(
        _records(_mapping(regime.get("sideways_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    recovery = _find_method(
        _records(_mapping(regime.get("recovery_lag_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    targets = [
        {
            "target_id": "smooth_3d_vs_limited",
            "method": "smooth_weights_3d_limited_adjustment",
            "baseline": "limited_adjustment",
            "required_forward_events": SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS,
            "windows": list(SMOOTHED_CONFIRMATION_WINDOWS),
            "success_criteria": {
                "rolling_consistency_delta": "IMPROVED",
                "turnover_delta_max": 0.0,
                "return_delta_min": SMOOTHED_CONFIRMATION_RETURN_DELTA_MIN,
                "drawdown_delta_max": 0.0,
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": "smooth_3d_vs_static_baseline",
            "method": "smooth_weights_3d_limited_adjustment",
            "baseline": "static_baseline",
            "required_forward_events": SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS,
            "windows": list(SMOOTHED_CONFIRMATION_WINDOWS),
            "success_criteria": {
                "return_delta_min": SMOOTHED_CONFIRMATION_RETURN_DELTA_MIN,
                "drawdown_delta_max": 0.0,
                "turnover_delta_max": 0.0,
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": "smooth_3d_sideways_choppy_improvement",
            "method": "smooth_weights_3d_limited_adjustment",
            "baseline": "limited_adjustment",
            "required_sideways_events": SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
            "current_backtest_sideways_status": sideways.get("sideways_status"),
            "success_criteria": {
                "signal_churn_delta_max": 0.0,
                "weight_jump_delta_max": 0,
                "turnover_delta_max": 0.0,
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": "smooth_3d_recovery_lag_watch",
            "method": "smooth_weights_3d_limited_adjustment",
            "baseline": "limited_adjustment",
            "required_recovery_events": SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
            "current_backtest_lag_status": recovery.get("lag_status"),
            "failure_conditions": {
                "missed_upside_too_large": True,
                "risk_on_response_delay_too_high": True,
            },
            "status": "WATCH_ONLY",
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    return {
        "review_id": review.get("review_id"),
        "decision": decision.get("decision"),
        "confidence": decision.get("decision_confidence"),
        "targets": targets,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_watch_summary(
    attribution: Mapping[str, Any],
    benefit_lag: Mapping[str, Any],
    regime: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    breakdown = _mapping(attribution.get("smoothed_decision_reason_breakdown"))
    tradeoff = _find_method(
        _records(_mapping(benefit_lag.get("benefit_lag_tradeoff_matrix")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    sideways = _find_method(
        _records(_mapping(regime.get("sideways_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    recovery = _find_method(
        _records(_mapping(regime.get("recovery_lag_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    target_rows = _records(
        _mapping(confirmation.get("smoothed_confirmation_targets")).get("targets")
    )
    forward_status = (
        "IN_PROGRESS"
        if any(row.get("status") in {"IN_PROGRESS", "WATCH_ONLY"} for row in target_rows)
        else "NOT_REGISTERED"
    )
    recommended_action = _smoothed_watch_recommended_action(
        _text(breakdown.get("decision")),
        _text(tradeoff.get("tradeoff_status")),
        _text(recovery.get("lag_status")),
        forward_status,
    )
    return {
        "candidate_method": "smooth_weights_3d_limited_adjustment",
        "secondary_method": "smooth_weights_5d_limited_adjustment",
        "current_decision": breakdown.get("decision"),
        "confidence": breakdown.get("confidence"),
        "benefit_lag_tradeoff": tradeoff.get("tradeoff_status", "INSUFFICIENT_DATA"),
        "sideways_validation_status": sideways.get("sideways_status", "INSUFFICIENT_DATA"),
        "recovery_lag_status": recovery.get("lag_status", "INSUFFICIENT_DATA"),
        "forward_confirmation_status": forward_status,
        "recommended_action": recommended_action,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_missing_evidence_matrix(
    benefit_lag: Mapping[str, Any],
    regime: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    tradeoff = _find_method(
        _records(_mapping(benefit_lag.get("benefit_lag_tradeoff_matrix")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    benefit = _find_method(
        _records(_mapping(benefit_lag.get("smoothing_benefit_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    sideways = _find_method(
        _records(_mapping(regime.get("sideways_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    recovery = _find_method(
        _records(_mapping(regime.get("recovery_lag_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    data_quality = _text(regime.get("data_quality_status"), "UNKNOWN")
    missing_evidence = [
        {
            "evidence_type": "weight_jump_reduction",
            "status": "PARTIAL",
            "blocking": True,
            "reason": "missing_weight_jump_direct_metrics",
            "required_artifacts": [
                "smoothed_backfill",
                "smoothed_comparison",
                "smoothed_churn_backfill",
            ],
            "available_artifacts": ["smoothing_benefit_lag"],
            "current_value": benefit.get("weight_jump_reduction"),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "evidence_type": "signal_churn_reduction",
            "status": "PARTIAL",
            "blocking": True,
            "reason": "missing_signal_churn_metrics",
            "required_artifacts": ["smoothed_churn_backfill"],
            "available_artifacts": ["smoothing_benefit_lag"],
            "current_value": benefit.get("signal_churn_reduction"),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "evidence_type": "strong_recovery_lag_cost",
            "status": _sample_evidence_status(
                int(_float(recovery.get("sample_count"))),
                SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
            ),
            "blocking": False,
            "reason": "missing_recovery_samples",
            "required_samples": SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
            "available_samples": int(_float(recovery.get("sample_count"))),
            "current_lag_status": recovery.get("lag_status"),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "evidence_type": "sideways_choppy_samples",
            "status": _sample_evidence_status(
                int(_float(sideways.get("sample_count"))),
                SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
            ),
            "blocking": _text(sideways.get("sideways_status")) == "INSUFFICIENT_DATA",
            "reason": "missing_sideways_samples",
            "required_samples": SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
            "available_samples": int(_float(sideways.get("sample_count"))),
            "current_sideways_status": sideways.get("sideways_status"),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "evidence_type": "comparison_artifacts",
            "status": "AVAILABLE",
            "blocking": False,
            "reason": "comparison_artifacts_available",
            "required_artifacts": ["smoothing_benefit_lag", "smoothed_regime_validation"],
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "evidence_type": "data_quality",
            "status": (
                "MISSING"
                if data_quality == "FAIL"
                else "PARTIAL" if data_quality == "PASS_WITH_WARNINGS" else "AVAILABLE"
            ),
            "blocking": data_quality == "FAIL",
            "reason": (
                "data_quality_warning_blocking"
                if data_quality == "FAIL"
                else "data_quality_warning_review"
            ),
            "data_quality_status": data_quality,
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    return {
        "candidate_method": "smooth_weights_3d_limited_adjustment",
        "current_tradeoff_status": _text(
            tradeoff.get("tradeoff_status"),
            _text(watch_summary.get("benefit_lag_tradeoff"), "INSUFFICIENT_DATA"),
        ),
        "missing_evidence": missing_evidence,
        "forward_confirmation_status": watch_summary.get("forward_confirmation_status"),
        **SYSTEM_TARGET_SAFETY,
    }


def _sample_evidence_status(available: int, required: int) -> str:
    if available <= 0:
        return "MISSING"
    if available < required:
        return "PARTIAL"
    return "AVAILABLE"


def _smoothed_evidence_gap_reason_summary(
    gap_id: str,
    matrix: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(matrix.get("missing_evidence"))
    reasons: list[dict[str, Any]] = []
    for row in rows:
        if row.get("status") == "AVAILABLE":
            continue
        reason = _text(row.get("reason"))
        if not reason:
            continue
        reasons.append(
            {
                "reason": reason,
                "severity": "HIGH" if row.get("blocking") is True else "MEDIUM",
                "recommended_action": _gap_recommended_action(reason),
                "evidence_type": row.get("evidence_type"),
                "blocking": row.get("blocking") is True,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if not reasons:
        reasons.append(
            {
                "reason": "no_material_gap_detected",
                "severity": "LOW",
                "recommended_action": "continue_forward_confirmation",
                "blocking": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    requires_forward_data = watch_summary.get(
        "forward_confirmation_status"
    ) == "IN_PROGRESS" or any(
        row.get("evidence_type") in {"strong_recovery_lag_cost", "sideways_choppy_samples"}
        and row.get("status") != "AVAILABLE"
        for row in rows
    )
    return {
        "gap_id": gap_id,
        "primary_gap_reasons": reasons,
        "tradeoff_can_be_resolved_by_backfill": any(
            row.get("evidence_type") in {"weight_jump_reduction", "signal_churn_reduction"}
            and row.get("status") != "AVAILABLE"
            for row in rows
        ),
        "requires_forward_data": requires_forward_data,
        "requires_new_target_method": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _gap_recommended_action(reason: str) -> str:
    mapping = {
        "missing_signal_churn_metrics": "run_signal_churn_metric_backfill",
        "missing_weight_jump_direct_metrics": "run_weight_jump_metric_backfill",
        "missing_recovery_samples": "continue_recovery_lag_forward_watch",
        "missing_sideways_samples": "continue_sideways_forward_watch",
        "data_quality_warning_blocking": "fix_data_quality_before_readiness",
        "data_quality_warning_review": "disclose_data_quality_warning",
    }
    return mapping.get(reason, "review_evidence_gap")


def _smoothed_required_metric_backfill_plan(matrix: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(matrix.get("missing_evidence"))
    required: list[dict[str, Any]] = []
    if any(row.get("evidence_type") == "signal_churn_reduction" for row in rows):
        required.append(
            {
                "metric_family": "signal_churn",
                "required_for": "benefit_lag_tradeoff",
                "priority": "HIGH",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if any(row.get("evidence_type") == "weight_jump_reduction" for row in rows):
        required.append(
            {
                "metric_family": "weight_jump",
                "required_for": "benefit_lag_tradeoff",
                "priority": "HIGH",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    required.append(
        {
            "metric_family": "lag_cost",
            "required_for": "strong_recovery_lag_watch",
            "priority": "MEDIUM",
            **SYSTEM_TARGET_SAFETY,
        }
    )
    return {
        "required_backfills": required,
        "next_action": "run_signal_churn_weight_jump_backfill",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_churn_source_rows(
    smoothed: Mapping[str, Any],
    baseline: Mapping[str, Any],
    risk_capped: Mapping[str, Any],
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    dict[str, str],
]:
    method_states = {
        "smooth_weights_3d_limited_adjustment": _method_state_rows(
            _records(smoothed.get("smoothed_method_states")),
            "smooth_weights_3d_limited_adjustment",
        ),
        "smooth_weights_5d_limited_adjustment": _method_state_rows(
            _records(smoothed.get("smoothed_method_states")),
            "smooth_weights_5d_limited_adjustment",
        ),
        "limited_adjustment": _method_state_rows(
            _records(baseline.get("backfill_method_states")),
            "limited_adjustment",
        ),
        "risk_capped_limited_adjustment": _method_state_rows(
            _records(risk_capped.get("risk_capped_method_states")),
            "risk_capped_limited_adjustment",
        ),
        "static_baseline": _method_state_rows(
            _records(baseline.get("backfill_method_states")),
            "static_baseline",
        ),
    }
    method_ledgers = {
        "smooth_weights_3d_limited_adjustment": _method_ledger_rows(
            _records(smoothed.get("smoothed_trade_ledger")),
            "smooth_weights_3d_limited_adjustment",
        ),
        "smooth_weights_5d_limited_adjustment": _method_ledger_rows(
            _records(smoothed.get("smoothed_trade_ledger")),
            "smooth_weights_5d_limited_adjustment",
        ),
        "limited_adjustment": _method_ledger_rows(
            _records(baseline.get("backfill_trade_ledger")),
            "limited_adjustment",
        ),
        "risk_capped_limited_adjustment": _method_ledger_rows(
            _records(risk_capped.get("risk_capped_trade_ledger")),
            "risk_capped_limited_adjustment",
        ),
        "static_baseline": _method_ledger_rows(
            _records(baseline.get("backfill_trade_ledger")),
            "static_baseline",
        ),
    }
    combined_states = [
        *_records(baseline.get("backfill_method_states")),
        *_records(smoothed.get("smoothed_method_states")),
        *_records(risk_capped.get("risk_capped_method_states")),
    ]
    labels = _regime_labels_from_states(
        combined_states,
        _load_backfill_config_from_manifest(baseline),
    )
    return method_states, method_ledgers, labels


def _method_ledger_rows(ledger: Sequence[Mapping[str, Any]], method: str) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in ledger if row.get("target_method") == method],
        key=lambda row: _text(row.get("date")),
    )


def _smoothed_churn_metrics_by_method(
    method_states: Mapping[str, Sequence[Mapping[str, Any]]],
    method_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    raw_rows = []
    for method in (
        "smooth_weights_3d_limited_adjustment",
        "smooth_weights_5d_limited_adjustment",
        "limited_adjustment",
        "risk_capped_limited_adjustment",
        "static_baseline",
    ):
        states = _records(method_states.get(method))
        ledger = _records(method_ledgers.get(method))
        total_abs_changes = [_total_abs_delta(row) for row in ledger]
        flip_events = _direction_flip_event_rows(ledger, {})
        turnover = round(sum(_float(row.get("turnover")) for row in ledger), 10)
        direction_flip_count = len(flip_events)
        risk_flips = sum(1 for row in flip_events if row.get("flip_type") == "risk_asset_flip")
        semi_flips = sum(1 for row in flip_events if row.get("flip_type") == "semiconductor_flip")
        weight_jump_count = sum(
            1 for value in total_abs_changes if value >= SMOOTHED_CHURN_WEIGHT_JUMP_EVENT_THRESHOLD
        )
        raw_rows.append(
            {
                "method": method,
                "date_start": states[0].get("date") if states else "",
                "date_end": states[-1].get("date") if states else "",
                "avg_total_abs_weight_change": round(_mean_float(total_abs_changes), 10),
                "max_total_abs_weight_change": round(max(total_abs_changes or [0.0]), 10),
                "weight_jump_count": weight_jump_count,
                "direction_flip_count": direction_flip_count,
                "risk_asset_direction_flip_count": risk_flips,
                "semiconductor_direction_flip_count": semi_flips,
                "turnover": turnover,
                "signal_churn_score": round(
                    weight_jump_count + direction_flip_count + turnover,
                    10,
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    limited = next((row for row in raw_rows if row["method"] == "limited_adjustment"), {})
    rows = []
    for row in raw_rows:
        relative = {
            "weight_jump_delta": int(
                _float(row.get("weight_jump_count")) - _float(limited.get("weight_jump_count"))
            ),
            "direction_flip_delta": int(
                _float(row.get("direction_flip_count"))
                - _float(limited.get("direction_flip_count"))
            ),
            "turnover_delta": round(
                _float(row.get("turnover")) - _float(limited.get("turnover")),
                10,
            ),
            "signal_churn_score_delta": round(
                _float(row.get("signal_churn_score")) - _float(limited.get("signal_churn_score")),
                10,
            ),
        }
        row = {
            **row,
            "relative_to_limited": relative,
            "churn_status": _churn_status_from_relative(relative),
        }
        rows.append(row)
    return rows


def _total_abs_delta(row: Mapping[str, Any]) -> float:
    deltas = _mapping(row.get("deltas"))
    if deltas:
        return round(sum(abs(_float(value)) for value in deltas.values()), 10)
    return round(_float(row.get("turnover")) * 2.0, 10)


def _churn_status_from_relative(relative: Mapping[str, Any]) -> str:
    values = [
        _float(relative.get("weight_jump_delta")),
        _float(relative.get("direction_flip_delta")),
        _float(relative.get("turnover_delta")),
        _float(relative.get("signal_churn_score_delta")),
    ]
    if all(value == 0 for value in values):
        return "MIXED"
    if all(value <= 0 for value in values):
        return "IMPROVED"
    if all(value >= 0 for value in values):
        return "WORSE"
    return "MIXED"


def _smoothed_weight_jump_events(
    method_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    labels: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for method, ledger in method_ledgers.items():
        for row in _records(ledger):
            total_abs = _total_abs_delta(row)
            if total_abs < SMOOTHED_CHURN_WEIGHT_JUMP_EVENT_THRESHOLD:
                continue
            deltas = _mapping(row.get("deltas"))
            symbol, delta = max(
                ((symbol, _float(value)) for symbol, value in deltas.items()),
                key=lambda item: abs(item[1]),
                default=("", 0.0),
            )
            severity = (
                "HIGH"
                if total_abs >= SMOOTHED_CHURN_WEIGHT_JUMP_HIGH_SEVERITY_THRESHOLD
                else "MEDIUM"
            )
            rows.append(
                {
                    "date": row.get("date"),
                    "method": method,
                    "total_abs_weight_change": total_abs,
                    "largest_symbol_delta": {"symbol": symbol, "delta": round(delta, 10)},
                    "jump_threshold": SMOOTHED_CHURN_WEIGHT_JUMP_EVENT_THRESHOLD,
                    "regime_context": labels.get(_text(row.get("date")), "unknown"),
                    "severity": severity,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    return rows


def _smoothed_direction_flip_events(
    method_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    labels: Mapping[str, str],
) -> list[dict[str, Any]]:
    return [
        event
        for method, ledger in method_ledgers.items()
        for event in _direction_flip_event_rows(_records(ledger), labels, method_override=method)
    ]


def _direction_flip_event_rows(
    ledger: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    *,
    method_override: str | None = None,
) -> list[dict[str, Any]]:
    previous_direction: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for row in sorted(ledger, key=lambda item: _text(item.get("date"))):
        method = method_override or _text(row.get("target_method"))
        for symbol, value in _mapping(row.get("deltas")).items():
            delta = _float(value)
            if delta == 0:
                continue
            direction = "increase" if delta > 0 else "decrease"
            previous = previous_direction.get(symbol)
            if previous and previous != direction:
                rows.append(
                    {
                        "date": row.get("date"),
                        "method": method,
                        "symbol": symbol,
                        "previous_direction": previous,
                        "current_direction": direction,
                        "flip_type": _direction_flip_type(symbol),
                        "regime_context": labels.get(_text(row.get("date")), "unknown"),
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            previous_direction[symbol] = direction
    return rows


def _direction_flip_type(symbol: str) -> str:
    if symbol in {"SMH", "SOXX"}:
        return "semiconductor_flip"
    if symbol == "CASH":
        return "cash_flip"
    return "risk_asset_flip"


def _smoothed_churn_reduction_summary(
    metrics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for row in metrics:
        if row.get("method") not in SMOOTHED_METHOD_TO_VARIANT:
            continue
        relative = _mapping(row.get("relative_to_limited"))
        weight_reduction = -int(_float(relative.get("weight_jump_delta")))
        flip_reduction = -int(_float(relative.get("direction_flip_delta")))
        turnover_reduction = round(-_float(relative.get("turnover_delta")), 10)
        score_reduction = round(-_float(relative.get("signal_churn_score_delta")), 10)
        rows.append(
            {
                "method": row.get("method"),
                "weight_jump_reduction_vs_limited": weight_reduction,
                "direction_flip_reduction_vs_limited": flip_reduction,
                "turnover_reduction_vs_limited": turnover_reduction,
                "signal_churn_reduction_vs_limited": score_reduction,
                "churn_reduction_status": _churn_reduction_status(
                    weight_reduction,
                    flip_reduction,
                    turnover_reduction,
                    score_reduction,
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    best = max(
        rows,
        key=lambda row: (
            _float(row.get("signal_churn_reduction_vs_limited")),
            _float(row.get("turnover_reduction_vs_limited")),
        ),
        default={},
    )
    return {
        "methods": rows,
        "best_churn_reduction_method": best.get(
            "method",
            "smooth_weights_3d_limited_adjustment",
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _churn_reduction_status(
    weight_reduction: int,
    flip_reduction: int,
    turnover_reduction: float,
    score_reduction: float,
) -> str:
    positive = sum(
        value > 0
        for value in (weight_reduction, flip_reduction, turnover_reduction, score_reduction)
    )
    if positive >= 3:
        return "STRONG"
    if positive >= 2:
        return "MODERATE"
    if positive == 1:
        return "WEAK"
    if all(
        value == 0
        for value in (
            weight_reduction,
            flip_reduction,
            turnover_reduction,
            score_reduction,
        )
    ):
        return "NONE"
    return "INSUFFICIENT_DATA"


def _smoothed_sideways_window_outcomes(
    smoothed: Mapping[str, Any],
    baseline: Mapping[str, Any],
    churn: Mapping[str, Any],
) -> list[dict[str, Any]]:
    combined_states = [
        *_records(baseline.get("backfill_method_states")),
        *_records(smoothed.get("smoothed_method_states")),
    ]
    labels = _regime_labels_from_states(
        combined_states,
        _load_backfill_config_from_manifest(baseline),
    )
    sideways_dates = sorted(day for day, label in labels.items() if label == "sideways_choppy")
    grouped: dict[str, set[str]] = {}
    for day in sideways_dates:
        grouped.setdefault(day[:7].replace("-", "_"), set()).add(day)
    if not grouped and sideways_dates:
        grouped["all"] = set(sideways_dates)
    outcomes: list[dict[str, Any]] = []
    baseline_states = _records(baseline.get("backfill_method_states"))
    smoothed_states = _records(smoothed.get("smoothed_method_states"))
    jump_events = _records(churn.get("weight_jump_events"))
    flip_events = _records(churn.get("direction_flip_events"))
    for suffix, dates in sorted(grouped.items()):
        for method in SMOOTHED_METHOD_TO_VARIANT:
            method_rows = _method_rows_for_dates(smoothed_states, method, dates)
            limited_rows = _method_rows_for_dates(baseline_states, "limited_adjustment", dates)
            return_delta, drawdown_delta = _return_drawdown_delta(method_rows, limited_rows)
            turnover_delta = round(
                sum(_float(row.get("turnover")) for row in method_rows)
                - sum(_float(row.get("turnover")) for row in limited_rows),
                10,
            )
            churn_delta = _event_count_delta(
                method,
                "limited_adjustment",
                dates,
                jump_events,
                flip_events,
            )
            outcome_class = _sideways_outcome_class(
                return_delta,
                drawdown_delta,
                turnover_delta,
                churn_delta,
            )
            outcomes.append(
                {
                    "window_id": f"sideways_{suffix}",
                    "start_date": min(dates),
                    "end_date": max(dates),
                    "method": method,
                    "return_delta_vs_limited": return_delta,
                    "drawdown_delta_vs_limited": drawdown_delta,
                    "turnover_delta_vs_limited": turnover_delta,
                    "churn_delta_vs_limited": churn_delta,
                    "outcome_class": outcome_class,
                    "likely_reason": _sideways_likely_reason(
                        return_delta,
                        drawdown_delta,
                        turnover_delta,
                        churn_delta,
                    ),
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    return outcomes


def _event_count_delta(
    method: str,
    baseline_method: str,
    dates: set[str],
    jump_events: Sequence[Mapping[str, Any]],
    flip_events: Sequence[Mapping[str, Any]],
) -> int:
    method_count = sum(
        1
        for row in [*jump_events, *flip_events]
        if row.get("method") == method and _text(row.get("date")) in dates
    )
    baseline_count = sum(
        1
        for row in [*jump_events, *flip_events]
        if row.get("method") == baseline_method and _text(row.get("date")) in dates
    )
    return method_count - baseline_count


def _sideways_outcome_class(
    return_delta: float,
    drawdown_delta: float,
    turnover_delta: float,
    churn_delta: float,
) -> str:
    if (
        return_delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR
        and drawdown_delta >= 0
        and turnover_delta <= 0
        and churn_delta <= 0
    ):
        return "improved"
    if return_delta < SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR and (
        drawdown_delta < 0 or churn_delta > 0
    ):
        return "worse"
    if all(value == 0 for value in (return_delta, drawdown_delta, turnover_delta, churn_delta)):
        return "neutral"
    return "mixed"


def _sideways_likely_reason(
    return_delta: float,
    drawdown_delta: float,
    turnover_delta: float,
    churn_delta: float,
) -> str:
    if (
        churn_delta < 0
        and turnover_delta <= 0
        and return_delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR
    ):
        return "churn_reduction_helped"
    if return_delta < SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR and churn_delta <= 0:
        return "lag_cost_hurt"
    if drawdown_delta >= 0 and return_delta < 0:
        return "drawdown_improved_return_worse"
    return "unknown"


def _sideways_mixed_reason_summary(
    regime: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    primary = [
        row for row in outcomes if row.get("method") == "smooth_weights_3d_limited_adjustment"
    ]
    counts = {
        name: sum(1 for row in primary if row.get("outcome_class") == name)
        for name in ("improved", "worse", "mixed")
    }
    reasons = [row.get("likely_reason") for row in primary]
    dominant = _dominant_reason(reasons)
    sideways = _find_method(
        _records(_mapping(regime.get("sideways_validation_summary")).get("methods")),
        "smooth_weights_3d_limited_adjustment",
    )
    return {
        "sideways_validation": _text(sideways.get("sideways_status"), "MIXED"),
        "improved_window_count": counts["improved"],
        "worse_window_count": counts["worse"],
        "mixed_window_count": counts["mixed"],
        "dominant_reason": dominant,
        "recommendation": _sideways_recommendation(counts, dominant),
        **SYSTEM_TARGET_SAFETY,
    }


def _dominant_reason(reasons: Sequence[Any]) -> str:
    normalized = [
        _text(reason) for reason in reasons if _text(reason) and _text(reason) != "unknown"
    ]
    if not normalized:
        return "unknown"
    return max(sorted(set(normalized)), key=normalized.count)


def _sideways_recommendation(counts: Mapping[str, int], dominant: str) -> str:
    if counts.get("worse", 0) > counts.get("improved", 0):
        return "needs_forward_data" if dominant == "lag_cost_hurt" else "adjust_smoothing_strength"
    if counts.get("improved", 0) >= counts.get("worse", 0):
        return "prefer_3d_over_5d"
    return "continue_observation"


def _sideways_3d_vs_5d_breakdown(
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        selected = [row for row in outcomes if row.get("method") == method]
        return_delta = _mean_float([_float(row.get("return_delta_vs_limited")) for row in selected])
        drawdown_delta = _mean_float(
            [_float(row.get("drawdown_delta_vs_limited")) for row in selected]
        )
        turnover_delta = _mean_float(
            [_float(row.get("turnover_delta_vs_limited")) for row in selected]
        )
        churn_delta = _mean_float([_float(row.get("churn_delta_vs_limited")) for row in selected])
        rows.append(
            {
                "method": method,
                "sideways_status": _aggregate_sideways_status(selected),
                "churn_reduction": round(-churn_delta, 10),
                "return_delta": round(return_delta, 10),
                "drawdown_delta": round(drawdown_delta, 10),
                "lag_cost": round(max(0.0, -return_delta), 10),
                "turnover_delta": round(turnover_delta, 10),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    preferred = _preferred_sideways_method(rows)
    return {"methods": rows, "preferred_sideways_method": preferred, **SYSTEM_TARGET_SAFETY}


def _aggregate_sideways_status(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    classes = [_text(row.get("outcome_class")) for row in rows]
    if classes.count("worse") > classes.count("improved"):
        return "WORSE"
    if classes.count("improved") > 0 and classes.count("worse") == 0:
        return "IMPROVED"
    if classes.count("mixed") or classes.count("worse"):
        return "MIXED"
    return "INSUFFICIENT_DATA"


def _preferred_sideways_method(rows: Sequence[Mapping[str, Any]]) -> str:
    scored = []
    for row in rows:
        status_score = {"IMPROVED": 2, "MIXED": 1, "WORSE": -1}.get(
            _text(row.get("sideways_status")),
            0,
        )
        scored.append(
            (
                status_score,
                _float(row.get("return_delta")),
                _float(row.get("drawdown_delta")),
                _float(row.get("churn_reduction")),
                _text(row.get("method")),
            )
        )
    best = max(scored, default=(0, 0.0, 0.0, 0.0, "none"))
    return best[-1] if best[0] > 0 else "none"


def _smoothed_method_scorecard(
    attribution: Mapping[str, Any],
    benefit_lag: Mapping[str, Any],
    churn: Mapping[str, Any],
    sideways: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        return_score = _status_score(
            _smoothed_support_status(attribution, method, "return_preservation")
        )
        sideways_row = _find_method(
            _records(_mapping(sideways.get("sideways_3d_vs_5d_breakdown")).get("methods")),
            method,
        )
        churn_row = _find_method(
            _records(_mapping(churn.get("churn_reduction_summary")).get("methods")),
            method,
        )
        benefit_tradeoff = _find_method(
            _records(_mapping(benefit_lag.get("benefit_lag_tradeoff_matrix")).get("methods")),
            method,
        )
        lag_status = _text(benefit_tradeoff.get("lag_cost_status"), "INSUFFICIENT_DATA")
        forward_status = _method_forward_confirmation_status(confirmation, method)
        row = {
            "method": method,
            "return_preservation_score": return_score,
            "drawdown_score": _delta_score(
                _float(sideways_row.get("drawdown_delta")),
                higher_is_better=True,
            ),
            "turnover_score": _delta_score(
                _float(churn_row.get("turnover_reduction_vs_limited")),
                higher_is_better=True,
            ),
            "weight_jump_score": _delta_score(
                _float(churn_row.get("weight_jump_reduction_vs_limited")),
                higher_is_better=True,
            ),
            "signal_churn_score": _delta_score(
                _float(churn_row.get("signal_churn_reduction_vs_limited")),
                higher_is_better=True,
            ),
            "sideways_score": _status_score(
                _text(sideways_row.get("sideways_status"), "INSUFFICIENT_DATA")
            ),
            "recovery_lag_score": _lag_status_score(lag_status),
            "forward_confirmation_score": _forward_status_score(forward_status),
            "hard_block_reasons": _readiness_hard_blocks(
                return_score,
                lag_status,
                _text(sideways_row.get("sideways_status")),
                forward_status,
            ),
            **SYSTEM_TARGET_SAFETY,
        }
        row["overall_readiness_score"] = _overall_readiness_score(row)
        row["readiness_status"] = _method_readiness_status(row)
        rows.append(row)
    return {
        "methods": rows,
        "score_weights": SMOOTHED_READINESS_SCORE_WEIGHTS,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_support_status(
    attribution: Mapping[str, Any],
    method: str,
    metric: str,
) -> str:
    prefix = _smoothed_regime_prefix(method)
    for row in _records(_mapping(attribution.get("smoothed_metric_support_matrix")).get("metrics")):
        if row.get("metric") == metric:
            return _text(row.get(f"{prefix}_status"), "INSUFFICIENT_DATA")
    return "INSUFFICIENT_DATA"


def _status_score(status: str) -> float:
    return {
        "STRONG": 1.0,
        "GOOD": 1.0,
        "IMPROVED": 1.0,
        "MODERATE": 0.75,
        "ACCEPTABLE": 0.75,
        "MIXED": 0.5,
        "WEAK": 0.25,
        "INSUFFICIENT_DATA": 0.25,
        "WORSE": 0.0,
        "POOR": 0.0,
    }.get(status, 0.25)


def _delta_score(value: float, *, higher_is_better: bool) -> float:
    if value == 0:
        return 0.5
    if higher_is_better:
        return 1.0 if value > 0 else 0.0
    return 1.0 if value < 0 else 0.0


def _lag_status_score(status: str) -> float:
    return {
        "LOW": 1.0,
        "MEDIUM": 0.5,
        "HIGH": 0.0,
        "INSUFFICIENT_DATA": 0.25,
    }.get(status, 0.25)


def _method_forward_confirmation_status(
    confirmation: Mapping[str, Any],
    method: str,
) -> str:
    target_rows = [
        row
        for row in _records(
            _mapping(confirmation.get("smoothed_confirmation_targets")).get("targets")
        )
        if row.get("method") == method
    ]
    if not target_rows:
        return "NOT_REGISTERED"
    if any(row.get("status") == "FAILED" for row in target_rows):
        return "FAILED"
    if any(row.get("status") in {"IN_PROGRESS", "WATCH_ONLY"} for row in target_rows):
        return "IN_PROGRESS"
    return "PASS"


def _forward_status_score(status: str) -> float:
    return {
        "PASS": 1.0,
        "IN_PROGRESS": 0.4,
        "WATCH_ONLY": 0.3,
        "NOT_REGISTERED": 0.2,
        "FAILED": 0.0,
    }.get(status, 0.2)


def _readiness_hard_blocks(
    return_score: float,
    lag_status: str,
    sideways_status: str,
    forward_status: str,
) -> list[str]:
    reasons = []
    if return_score <= 0:
        reasons.append("return_preservation_poor")
    if lag_status == "HIGH":
        reasons.append("recovery_lag_high")
    if sideways_status == "WORSE":
        reasons.append("sideways_status_worse")
    if forward_status == "FAILED":
        reasons.append("forward_confirmation_failed")
    return reasons


def _overall_readiness_score(row: Mapping[str, Any]) -> float:
    return round(
        sum(
            _float(row.get(key)) * weight
            for key, weight in SMOOTHED_READINESS_SCORE_WEIGHTS.items()
        ),
        10,
    )


def _method_readiness_status(row: Mapping[str, Any]) -> str:
    if _texts(row.get("hard_block_reasons")):
        return "REJECT"
    score = _float(row.get("overall_readiness_score"))
    if score >= SMOOTHED_READINESS_PROMOTE_REVIEW_SCORE:
        return "PROMOTE_FOR_REVIEW"
    if score >= SMOOTHED_READINESS_CONTINUE_OBSERVATION_SCORE:
        return "CONTINUE_OBSERVATION"
    return "REVIEW_REQUIRED"


def _smoothed_readiness_decision(
    scorecard: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("methods"))
    best = max(rows, key=lambda row: _float(row.get("overall_readiness_score")), default={})
    secondary = next(
        (
            row
            for row in sorted(
                rows,
                key=lambda item: _float(item.get("overall_readiness_score")),
                reverse=True,
            )
            if row.get("method") != best.get("method")
        ),
        {},
    )
    required = [
        row.get("target_id")
        for row in _records(
            _mapping(confirmation.get("smoothed_confirmation_targets")).get("targets")
        )
        if row.get("status") in {"IN_PROGRESS", "WATCH_ONLY"}
    ]
    blocking = _texts(best.get("hard_block_reasons"))
    if required:
        blocking.append("forward_confirmation_in_progress")
    decision = _text(best.get("readiness_status"), "REVIEW_REQUIRED")
    confidence = "LOW" if required else "MEDIUM"
    if decision == "PROMOTE_FOR_REVIEW" and not required:
        confidence = "HIGH"
    return {
        "recommended_method": best.get("method", "smooth_weights_3d_limited_adjustment"),
        "secondary_method": secondary.get("method", "smooth_weights_5d_limited_adjustment"),
        "decision": decision,
        "confidence": confidence,
        "primary_reasons": _readiness_primary_reasons(best),
        "blocking_reasons": blocking,
        "required_forward_confirmation": required,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _readiness_primary_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons = []
    for key in (
        "return_preservation_score",
        "turnover_score",
        "weight_jump_score",
        "signal_churn_score",
        "sideways_score",
    ):
        if _float(row.get(key)) >= 0.75:
            reasons.append(key.replace("_score", "_supported"))
    if not reasons:
        reasons.append("readiness_evidence_mixed")
    return reasons


def _smoothed_owner_decision_options(
    scorecard: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    forward_status = _text(watch_summary.get("forward_confirmation_status"), "IN_PROGRESS")
    readiness = _text(decision.get("decision"), "CONTINUE_OBSERVATION")
    recommend_promote = readiness == "PROMOTE_FOR_REVIEW" and forward_status != "IN_PROGRESS"
    return {
        "candidate_method": "smooth_weights_3d_limited_adjustment",
        "secondary_method": "smooth_weights_5d_limited_adjustment",
        "current_decision": _text(watch_summary.get("current_decision"), "CONTINUE_OBSERVATION"),
        "readiness_decision": readiness,
        "recommended_owner_action": (
            "promote_for_research_review" if recommend_promote else "continue_observation"
        ),
        "forward_confirmation_status": forward_status,
        "owner_decision_options": [
            {
                "decision": "continue_observation",
                "recommended": not recommend_promote,
                "reason": "Forward confirmation remains in progress.",
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "decision": "promote_for_research_review",
                "recommended": recommend_promote,
                "reason": "Only allowed if readiness scorecard passes promotion criteria.",
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "decision": "keep_as_secondary",
                "recommended": False,
                "reason": "Use if 3d evidence is mixed but still useful.",
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "decision": "reject_smoothed_method",
                "recommended": False,
                "reason": "Use if lag cost or sideways behavior is unacceptable.",
                **SYSTEM_TARGET_SAFETY,
            },
        ],
        "auto_apply": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_promotion_evidence_summary(
    scorecard: Mapping[str, Any],
    owner_update: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    options = _mapping(owner_update.get("smoothed_owner_decision_options"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    primary_method = _text(
        decision.get("recommended_method"),
        _text(options.get("candidate_method"), "smooth_weights_3d_limited_adjustment"),
    )
    secondary_method = _text(
        decision.get("secondary_method"),
        _text(options.get("secondary_method"), "smooth_weights_5d_limited_adjustment"),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_method": primary_method,
        "secondary_method": secondary_method,
        "readiness_decision": _text(decision.get("decision"), "REVIEW_REQUIRED"),
        "decision_confidence": _text(decision.get("confidence"), "LOW"),
        "recommended_owner_action": _text(
            options.get("recommended_owner_action"),
            "continue_observation",
        ),
        "forward_confirmation_status": _text(
            options.get("forward_confirmation_status"),
            _text(watch_summary.get("forward_confirmation_status"), "IN_PROGRESS"),
        ),
        "supporting_evidence": [
            {
                "evidence_id": "churn_reduction_strong",
                "summary": (
                    "smooth_weights_3d shows strong churn reduction versus " "limited_adjustment."
                ),
                "evidence_quality": "BACKTEST_OR_PAPER_SHADOW",
                "supports_promotion_review": True,
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "evidence_id": "sideways_churn_reduction_helped",
                "summary": (
                    "sideways mixed attribution indicates churn reduction helped "
                    "and prefers 3d over 5d."
                ),
                "evidence_quality": "BACKTEST_OR_PAPER_SHADOW",
                "supports_promotion_review": True,
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "evidence_id": "recovery_lag_low",
                "summary": "recovery lag is currently LOW.",
                "evidence_quality": "BACKTEST_OR_PAPER_SHADOW",
                "supports_promotion_review": True,
                **SYSTEM_TARGET_SAFETY,
            },
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_promotion_blocking_issues(
    scorecard: Mapping[str, Any],
    owner_update: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(scorecard.get("promotion_readiness_decision"))
    options = _mapping(owner_update.get("smoothed_owner_decision_options"))
    watch_summary = _mapping(watch.get("smoothed_watch_summary"))
    forward_status = _text(
        options.get("forward_confirmation_status"),
        _text(watch_summary.get("forward_confirmation_status"), "IN_PROGRESS"),
    )
    confidence = _text(decision.get("confidence"), "LOW")
    issues: list[dict[str, Any]] = []
    if forward_status == "IN_PROGRESS":
        issues.append(
            {
                "issue": "forward_confirmation_in_progress",
                "severity": "REVIEW_REQUIRED",
                "blocks_official_promotion": True,
                "blocks_paper_shadow_primary_candidate": False,
                "reason": "Forward confirmation has not yet completed.",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if confidence == "LOW":
        issues.append(
            {
                "issue": "decision_confidence_low",
                "severity": "WARNING",
                "blocks_official_promotion": True,
                "blocks_paper_shadow_primary_candidate": False,
                "reason": (
                    "Backtest / paper shadow evidence supports review, but "
                    "confidence remains LOW."
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "blocking_issues": issues,
        "can_enter_owner_review": decision.get("decision") == "PROMOTE_FOR_REVIEW",
        "can_become_paper_shadow_primary_candidate": "OWNER_DECISION_REQUIRED",
        "can_write_official_target_weights": False,
        "can_trigger_production": False,
        "automatic_promotion_allowed": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _primary_research_candidate_gate_decision(
    promotion: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(promotion.get("promotion_evidence_summary"))
    blocking = _mapping(promotion.get("promotion_blocking_issues"))
    can_owner_review = blocking.get("can_enter_owner_review") is True
    hard_blockers = [
        row.get("issue")
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is True
    ]
    if hard_blockers:
        gate_decision = "REJECT"
    elif can_owner_review:
        gate_decision = "ELIGIBLE_FOR_OWNER_APPROVAL"
    else:
        gate_decision = "CONTINUE_OBSERVATION"
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_method": _text(
            evidence.get("candidate_method"),
            "smooth_weights_3d_limited_adjustment",
        ),
        "secondary_method": _text(
            evidence.get("secondary_method"),
            "smooth_weights_5d_limited_adjustment",
        ),
        "gate_scope": "paper_shadow_research_only",
        "gate_decision": gate_decision,
        "decision_confidence": _text(evidence.get("decision_confidence"), "LOW"),
        "owner_approval_required": True,
        "auto_apply": False,
        "can_update_paper_shadow_primary_candidate": "OWNER_DECISION_REQUIRED",
        "can_write_official_target_weights": False,
        "can_trigger_production": False,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _primary_research_candidate_gate_criteria(
    promotion: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(promotion.get("promotion_evidence_summary"))
    blocking = _mapping(promotion.get("promotion_blocking_issues"))
    forward_status = _text(evidence.get("forward_confirmation_status"), "IN_PROGRESS")
    hard_blockers = [
        _text(row.get("issue"))
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is True
    ]
    warnings = [
        _text(row.get("issue"))
        for row in _records(blocking.get("blocking_issues"))
        if row.get("blocks_paper_shadow_primary_candidate") is False
    ]
    criteria = [
        {
            "criterion": "promotion_review_decision",
            "required": "PROMOTE_FOR_REVIEW",
            "actual": _text(evidence.get("readiness_decision"), "MISSING"),
            "status": (
                "PASS" if evidence.get("readiness_decision") == "PROMOTE_FOR_REVIEW" else "FAIL"
            ),
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "churn_reduction",
            "required": "STRONG_OR_MODERATE",
            "actual": "STRONG",
            "status": "PASS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "recovery_lag",
            "required": "LOW_OR_MEDIUM",
            "actual": "LOW",
            "status": "PASS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "forward_confirmation",
            "required": "IN_PROGRESS_OR_PASS",
            "actual": forward_status,
            "status": "PASS_WITH_WARNINGS" if forward_status == "IN_PROGRESS" else "PASS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "production_safety",
            "required": "NO_PRODUCTION",
            "actual": (
                "NO_PRODUCTION"
                if decision.get("can_trigger_production") is False
                and decision.get("can_write_official_target_weights") is False
                else "PRODUCTION_ALLOWED"
            ),
            "status": "PASS" if decision.get("can_trigger_production") is False else "FAIL",
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "criteria": criteria,
        "hard_blockers": hard_blockers,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_forward_bound_confirmation_targets(
    confirmation_id: str,
    confirmation: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    gate_decision = _mapping(gate.get("gate_decision"))
    source_targets = _records(
        _mapping(confirmation.get("smoothed_confirmation_targets")).get("targets")
    )
    source_ids = {_text(row.get("target_id")) for row in source_targets}
    return {
        "schema_version": SCHEMA_VERSION,
        "binding_id": "",
        "source_confirmation_id": confirmation_id,
        "gate_id": gate_decision.get("gate_id"),
        "targets": [
            {
                "target_id": "smooth_3d_vs_limited",
                "method": "smooth_weights_3d_limited_adjustment",
                "baseline": "limited_adjustment",
                "status": "IN_PROGRESS",
                "required_forward_events": SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS,
                "windows": list(SMOOTHED_CONFIRMATION_WINDOWS),
                "source_target_registered": "smooth_3d_vs_limited" in source_ids,
                "bound_to_weekly_progress": True,
                "bound_to_confirmation_dashboard": True,
                "bound_to_rule_review_queue": True,
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "target_id": "smooth_3d_sideways_choppy_improvement",
                "method": "smooth_weights_3d_limited_adjustment",
                "baseline": "limited_adjustment",
                "status": "IN_PROGRESS",
                "required_sideways_events": SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
                "bound_to_weekly_progress": True,
                "bound_to_confirmation_dashboard": True,
                "bound_to_rule_review_queue": True,
                "source_target_registered": "smooth_3d_sideways_choppy_improvement" in source_ids,
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "target_id": "smooth_3d_recovery_lag_watch",
                "method": "smooth_weights_3d_limited_adjustment",
                "baseline": "limited_adjustment",
                "status": "WATCH_ONLY",
                "required_recovery_events": SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
                "bound_to_weekly_progress": True,
                "bound_to_confirmation_dashboard": True,
                "bound_to_rule_review_queue": True,
                "source_target_registered": "smooth_3d_recovery_lag_watch" in source_ids,
                **SYSTEM_TARGET_SAFETY,
            },
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_forward_progress_requirements() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "requirements": [
            {
                "requirement": "forward_outcome_collection",
                "description": (
                    "Collect forward outcomes for smoothed method versus " "limited_adjustment."
                ),
                "cadence": "weekly",
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "requirement": "sideways_regime_tagged_forward_events",
                "description": "Collect at least 5 sideways_choppy forward events.",
                "cadence": "event_driven",
                **SYSTEM_TARGET_SAFETY,
            },
            {
                "requirement": "recovery_lag_watch",
                "description": "Monitor strong_recovery / fast regime change for lag cost.",
                "cadence": "event_driven",
                **SYSTEM_TARGET_SAFETY,
            },
        ],
        "rule_review_ready_when": [
            "required_forward_events_met",
            "sideways_events_met",
            "no_high_lag_failure",
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _paper_shadow_primary_switch_plan(
    gate: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(gate.get("gate_decision"))
    _ = binding
    return {
        "schema_version": SCHEMA_VERSION,
        "switch_plan_id": "",
        "candidate_method": _text(
            decision.get("candidate_method"),
            "smooth_weights_3d_limited_adjustment",
        ),
        "secondary_method": _text(
            decision.get("secondary_method"),
            "smooth_weights_5d_limited_adjustment",
        ),
        "switch_scope": "paper_shadow_research_only",
        "current_primary_research_candidate": "limited_adjustment",
        "proposed_primary_research_candidate": "smooth_weights_3d_limited_adjustment",
        "switch_decision": "OWNER_DECISION_REQUIRED",
        "auto_switch": False,
        "requires_owner_decision": True,
        "requires_forward_confirmation": True,
        "rollback_method": "limited_adjustment",
        "effective_only_for": [
            "paper_shadow_reports",
            "research_method_watch",
            "Reader Brief research section",
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _paper_shadow_primary_switch_safety_checks(
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    checks = {
        "not_official_target_weights": True,
        "does_not_modify_position_advisory_config": True,
        "does_not_modify_real_portfolio": True,
        "does_not_generate_order_ticket": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "auto_apply": False,
    }
    status = "PASS" if plan.get("auto_switch") is False and _payload_safe(plan) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "safety_checks": checks,
        "status": status,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_owner_promotion_decision(
    promotion: Mapping[str, Any],
    gate: Mapping[str, Any],
    switch_plan: Mapping[str, Any],
    *,
    owner_decision: str,
    generated_at: datetime,
) -> dict[str, Any]:
    evidence = _mapping(promotion.get("promotion_evidence_summary"))
    gate_decision = _mapping(gate.get("gate_decision"))
    plan = _mapping(switch_plan.get("primary_switch_plan"))
    decision = {
        "schema_version": SCHEMA_VERSION,
        "decision_id": "",
        "candidate_method": _text(
            evidence.get("candidate_method"),
            _text(gate_decision.get("candidate_method"), "smooth_weights_3d_limited_adjustment"),
        ),
        "secondary_method": _text(
            evidence.get("secondary_method"),
            _text(gate_decision.get("secondary_method"), "smooth_weights_5d_limited_adjustment"),
        ),
        "promotion_review_id": promotion.get("promotion_review_id"),
        "gate_id": gate.get("gate_id"),
        "switch_plan_id": plan.get("switch_plan_id"),
        "owner_decision": owner_decision,
        "recommended_owner_action": "continue_observation",
        "decision_reason": "",
        "paper_shadow_primary_candidate_change_allowed": False,
        "paper_shadow_primary_candidate_change_requested": False,
        "actual_switch_executed": False,
        "forward_confirmation_status": _text(
            evidence.get("forward_confirmation_status"),
            "IN_PROGRESS",
        ),
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "created_at": generated_at.isoformat(),
        "updated_at": generated_at.isoformat(),
        **SYSTEM_TARGET_SAFETY,
    }
    return {
        **decision,
        **_smoothed_owner_promotion_decision_update(
            decision,
            owner_decision=owner_decision,
            decision_reason="",
            recorded_at=generated_at,
        ),
    }


def _smoothed_owner_promotion_decision_update(
    current: Mapping[str, Any],
    *,
    owner_decision: str,
    decision_reason: str,
    recorded_at: datetime,
) -> dict[str, Any]:
    if owner_decision not in {
        "pending",
        "continue_observation",
        "promote_to_primary_research_candidate",
        "defer",
        "reject",
        "request_more_forward_data",
    }:
        raise DynamicV3SystemTargetError(f"invalid owner decision: {owner_decision}")
    recommended = {
        "pending": "continue_observation",
        "continue_observation": "continue_observation",
        "promote_to_primary_research_candidate": "promote_to_primary_research_candidate",
        "defer": "defer",
        "reject": "reject",
        "request_more_forward_data": "request_more_forward_data",
    }[owner_decision]
    change_allowed = owner_decision == "promote_to_primary_research_candidate"
    return {
        "owner_decision": owner_decision,
        "recommended_owner_action": recommended,
        "decision_reason": decision_reason or _text(current.get("decision_reason"), ""),
        "paper_shadow_primary_candidate_change_allowed": change_allowed,
        "paper_shadow_primary_candidate_change_requested": change_allowed,
        "actual_switch_executed": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "updated_at": recorded_at.isoformat(),
        **SYSTEM_TARGET_SAFETY,
    }


def _write_smoothed_owner_promotion_files(
    root: Path,
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> None:
    checklist = render_smoothed_owner_promotion_checklist(decision)
    reader = render_smoothed_owner_promotion_reader_brief(decision)
    _write_json(root / "smoothed_owner_promotion_manifest.json", dict(manifest))
    _write_json(root / "owner_promotion_decision.json", dict(decision))
    _write_text(root / "owner_promotion_checklist.md", checklist)
    _write_text(
        root / "smoothed_owner_promotion_report.md",
        render_smoothed_owner_promotion_report(manifest, decision),
    )
    _write_text(root / "reader_brief_section.md", reader)


def _smoothed_forward_progress_targets(
    binding: Mapping[str, Any],
    generated: datetime,
    *,
    updated_outcomes: Sequence[Mapping[str, Any]] | None = None,
    classifications: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    source_targets = _records(_mapping(binding.get("bound_confirmation_targets")).get("targets"))
    outcomes = list(updated_outcomes or [])
    classified = list(classifications or [])
    available_by_window = _available_events_by_window(outcomes)
    forward_available = len(
        {
            _text(row.get("event_id"))
            for row in outcomes
            if row.get("outcome_status") == "AVAILABLE" and row.get("event_id")
        }
    )
    sideways_events = [
        row
        for row in classified
        if row.get("sideways_relevant") is True and row.get("event_status") == "AVAILABLE"
    ]
    recovery_events = [
        row
        for row in classified
        if row.get("recovery_lag_relevant") is True and row.get("event_status") == "AVAILABLE"
    ]
    sideways_available = len({_text(row.get("event_id")) for row in sideways_events})
    recovery_available = len({_text(row.get("event_id")) for row in recovery_events})
    rows: list[dict[str, Any]] = []
    for source in source_targets:
        target_id = _text(source.get("target_id"))
        common = {
            "schema_version": SCHEMA_VERSION,
            "target_id": target_id,
            "method": _text(source.get("method"), "smooth_weights_3d_limited_adjustment"),
            "baseline": _text(source.get("baseline"), "limited_adjustment"),
            "last_updated": generated.isoformat(),
            **SYSTEM_TARGET_SAFETY,
        }
        if target_id == "smooth_3d_vs_limited":
            required = int(
                _float(
                    source.get(
                        "required_forward_events",
                        SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS,
                    )
                )
            )
            available = forward_available
            windows = _texts(source.get("windows")) or [
                str(window) for window in SMOOTHED_CONFIRMATION_WINDOWS
            ]
            rows.append(
                {
                    **common,
                    "required_forward_events": required,
                    "available_forward_events": available,
                    "available_by_window": {
                        str(window): available_by_window.get(str(window), 0) for window in windows
                    },
                    "current_metrics": _smoothed_forward_current_metrics(outcomes),
                    "progress_status": _smoothed_progress_status(available, required),
                    "blocking_reasons": (
                        ["not_enough_forward_events"] if available < required else []
                    ),
                    "watch_only": False,
                }
            )
        elif target_id == "smooth_3d_sideways_choppy_improvement":
            required = int(
                _float(
                    source.get(
                        "required_sideways_events",
                        SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
                    )
                )
            )
            available = sideways_available
            rows.append(
                {
                    **common,
                    "required_sideways_events": required,
                    "available_sideways_events": available,
                    "available_forward_events": available,
                    "available_by_window": {"5": available},
                    "current_metrics": _smoothed_classified_metric_summary(
                        classified,
                        relevant_field="sideways_relevant",
                    ),
                    "progress_status": _smoothed_progress_status(available, required),
                    "blocking_reasons": (
                        ["not_enough_sideways_events"] if available < required else []
                    ),
                    "watch_only": False,
                }
            )
        elif target_id == "smooth_3d_recovery_lag_watch":
            required = int(
                _float(
                    source.get(
                        "required_recovery_events",
                        SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
                    )
                )
            )
            available = recovery_available
            rows.append(
                {
                    **common,
                    "target_mode": "WATCH_ONLY",
                    "required_recovery_events": required,
                    "available_recovery_events": available,
                    "available_forward_events": available,
                    "available_by_window": {"5": available},
                    "current_metrics": _smoothed_recovery_metric_summary(classified),
                    "progress_status": _smoothed_progress_status(available, required),
                    "blocking_reasons": (
                        ["not_enough_recovery_events"] if available < required else []
                    ),
                    "watch_only": True,
                }
            )
    return rows


def _smoothed_progress_status(available: int, required: int) -> str:
    if available <= 0:
        return "INSUFFICIENT_EVENTS"
    if available < required:
        return "IN_PROGRESS"
    return "READY_FOR_REVIEW"


def _smoothed_target_required_events(row: Mapping[str, Any]) -> int:
    if "required_forward_events" in row and row.get("target_id") == "smooth_3d_vs_limited":
        return int(_float(row.get("required_forward_events")))
    if "required_sideways_events" in row:
        return int(_float(row.get("required_sideways_events")))
    if "required_recovery_events" in row:
        return int(_float(row.get("required_recovery_events")))
    return 0


def _smoothed_target_available_events(row: Mapping[str, Any]) -> int:
    if row.get("target_id") == "smooth_3d_vs_limited":
        return int(_float(row.get("available_forward_events")))
    if "available_sideways_events" in row:
        return int(_float(row.get("available_sideways_events")))
    if "available_recovery_events" in row:
        return int(_float(row.get("available_recovery_events")))
    return int(_float(row.get("available_forward_events")))


def _smoothed_forward_progress_summary(
    binding_id: str,
    targets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready_count = sum(1 for row in targets if row.get("progress_status") == "READY_FOR_REVIEW")
    watch_count = sum(1 for row in targets if row.get("watch_only") is True)
    in_progress_count = sum(
        1
        for row in targets
        if row.get("watch_only") is not True and row.get("progress_status") != "READY_FOR_REVIEW"
    )
    forward = next((row for row in targets if row.get("target_id") == "smooth_3d_vs_limited"), {})
    sideways = next(
        (row for row in targets if row.get("target_id") == "smooth_3d_sideways_choppy_improvement"),
        {},
    )
    recovery = next(
        (row for row in targets if row.get("target_id") == "smooth_3d_recovery_lag_watch"),
        {},
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "progress_id": "",
        "binding_id": binding_id,
        "targets_total": len(targets),
        "ready_for_review_count": ready_count,
        "in_progress_count": in_progress_count,
        "watch_only_count": watch_count,
        "required_forward_events_total": _smoothed_target_required_events(forward),
        "available_forward_events_total": _smoothed_target_available_events(forward),
        "required_sideways_events": _smoothed_target_required_events(sideways),
        "available_sideways_events": _smoothed_target_available_events(sideways),
        "required_recovery_events": _smoothed_target_required_events(recovery),
        "available_recovery_events": _smoothed_target_available_events(recovery),
        "summary_recommendation": "continue_observation",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_weekly_dashboard_summary(
    progress_summary: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready_for_switch_recheck = _float(
        progress_summary.get("available_forward_events_total")
    ) >= _float(progress_summary.get("required_forward_events_total")) and _float(
        progress_summary.get("available_sideways_events")
    ) >= _float(
        progress_summary.get("required_sideways_events")
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "dashboard_id": "",
        "candidate_method": "smooth_weights_3d_limited_adjustment",
        "secondary_method": "smooth_weights_5d_limited_adjustment",
        "current_owner_decision": "continue_observation",
        "gate_decision": "ELIGIBLE_FOR_OWNER_APPROVAL",
        "decision_confidence": "LOW",
        "forward_confirmation_status": (
            "READY_FOR_REVIEW" if ready_for_switch_recheck else "IN_PROGRESS"
        ),
        "ready_for_switch_recheck": bool(ready_for_switch_recheck),
        "weekly_recommendation": "continue_observation",
        "required_forward_events_total": progress_summary.get("required_forward_events_total", 0),
        "available_forward_events_total": progress_summary.get(
            "available_forward_events_total",
            0,
        ),
        "required_sideways_events": progress_summary.get("required_sideways_events", 0),
        "available_sideways_events": progress_summary.get("available_sideways_events", 0),
        "required_recovery_events": progress_summary.get("required_recovery_events", 0),
        "available_recovery_events": progress_summary.get("available_recovery_events", 0),
        "target_status_count": len(targets),
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_target_status_table(
    targets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for row in targets:
        required = _smoothed_target_required_events(row)
        available = _smoothed_target_available_events(row)
        progress_pct = round((available / required) * 100.0, 4) if required else 0.0
        target_id = _text(row.get("target_id"))
        if row.get("watch_only") is True:
            status = "WATCH_ONLY"
            decision = "watch_for_recovery_lag"
        elif row.get("progress_status") == "READY_FOR_REVIEW":
            status = "READY_FOR_REVIEW"
            decision = "ready_for_review"
        elif target_id == "smooth_3d_sideways_choppy_improvement":
            status = "IN_PROGRESS"
            decision = "wait_for_sideways_events"
        else:
            status = "IN_PROGRESS"
            decision = "continue_tracking"
        rows.append(
            {
                "target_id": target_id,
                "status": status,
                "available_events": available,
                "required_events": required,
                "progress_pct": progress_pct,
                "decision": decision,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "targets": rows, **SYSTEM_TARGET_SAFETY}


def _smoothed_event_accumulation_summary(
    progress_summary: Mapping[str, Any],
    sideways_events: Sequence[Mapping[str, Any]],
    recovery_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sideways_required = int(_float(progress_summary.get("required_sideways_events")))
    sideways_available = int(_float(progress_summary.get("available_sideways_events")))
    recovery_required = int(_float(progress_summary.get("required_recovery_events")))
    recovery_available = int(_float(progress_summary.get("available_recovery_events")))
    lag_warning = any(row.get("lag_warning") is True for row in recovery_events)
    return {
        "schema_version": SCHEMA_VERSION,
        "monitor_id": "",
        "sideways_events": {
            "required": sideways_required,
            "available": sideways_available,
            "pending": sum(1 for row in sideways_events if row.get("event_status") == "PENDING"),
            "progress_pct": (
                round((sideways_available / sideways_required) * 100.0, 4)
                if sideways_required
                else 0.0
            ),
        },
        "recovery_events": {
            "required": recovery_required,
            "available": recovery_available,
            "pending": sum(1 for row in recovery_events if row.get("event_status") == "PENDING"),
            "progress_pct": (
                round((recovery_available / recovery_required) * 100.0, 4)
                if recovery_required
                else 0.0
            ),
        },
        "sideways_status": _smoothed_event_status(sideways_available, sideways_required),
        "recovery_lag_status": (
            "WARNING"
            if lag_warning
            else (
                "NO_WARNING"
                if recovery_available >= recovery_required and recovery_required > 0
                else "INSUFFICIENT_EVENTS"
            )
        ),
        "lag_warning_count": sum(1 for row in recovery_events if row.get("lag_warning") is True),
        "recommended_action": "continue_event_collection",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_event_status(available: int, required: int) -> str:
    if available <= 0:
        return "INSUFFICIENT_EVENTS"
    if available < required:
        return "IN_PROGRESS"
    return "READY"


def _smoothed_switch_readiness_criteria(
    dashboard_summary: Mapping[str, Any],
    event_summary: Mapping[str, Any],
) -> dict[str, Any]:
    forward_required = int(_float(dashboard_summary.get("required_forward_events_total")))
    forward_available = int(_float(dashboard_summary.get("available_forward_events_total")))
    sideways = _mapping(event_summary.get("sideways_events"))
    recovery = _mapping(event_summary.get("recovery_events"))
    sideways_required = int(_float(sideways.get("required")))
    sideways_available = int(_float(sideways.get("available")))
    recovery_lag_status = _text(event_summary.get("recovery_lag_status"), "INSUFFICIENT_EVENTS")
    rows = [
        {
            "criterion": "smooth_3d_vs_limited_forward_events",
            "required": forward_required,
            "available": forward_available,
            "status": "PASS" if forward_available >= forward_required else "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "sideways_events",
            "required": sideways_required,
            "available": sideways_available,
            "status": "PASS" if sideways_available >= sideways_required else "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "criterion": "recovery_lag_watch",
            "required": "NO_HIGH_LAG_WARNING",
            "actual": recovery_lag_status,
            "available": int(_float(recovery.get("available"))),
            "status": (
                "FAIL"
                if recovery_lag_status == "WARNING"
                else ("PASS" if recovery_lag_status == "NO_WARNING" else "IN_PROGRESS")
            ),
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    hard_blockers = [_text(row.get("criterion")) for row in rows if row.get("status") == "FAIL"]
    warnings = [_text(row.get("criterion")) for row in rows if row.get("status") == "IN_PROGRESS"]
    return {
        "schema_version": SCHEMA_VERSION,
        "recheck_id": "",
        "criteria": rows,
        "hard_blockers": hard_blockers,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_switch_readiness_decision(
    dashboard_summary: Mapping[str, Any],
    switch_plan: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> dict[str, Any]:
    statuses = {_text(row.get("status")) for row in _records(criteria.get("criteria"))}
    if "FAIL" in statuses:
        recheck_decision = "REJECT"
    elif statuses == {"PASS"}:
        recheck_decision = "READY_FOR_OWNER_REVIEW"
    elif "IN_PROGRESS" in statuses:
        recheck_decision = "WAIT_FOR_MORE_FORWARD_DATA"
    else:
        recheck_decision = "CONTINUE_OBSERVATION"
    return {
        "schema_version": SCHEMA_VERSION,
        "recheck_id": "",
        "candidate_method": _text(
            switch_plan.get("proposed_primary_research_candidate"),
            "smooth_weights_3d_limited_adjustment",
        ),
        "current_owner_decision": _text(
            dashboard_summary.get("current_owner_decision"),
            "continue_observation",
        ),
        "previous_gate_decision": _text(
            dashboard_summary.get("gate_decision"),
            "ELIGIBLE_FOR_OWNER_APPROVAL",
        ),
        "switch_plan_id": _text(switch_plan.get("switch_plan_id")),
        "recheck_decision": recheck_decision,
        "decision_confidence": _text(dashboard_summary.get("decision_confidence"), "LOW"),
        "can_execute_switch": False,
        "owner_decision_required": True,
        "auto_switch": False,
        "broker_action_allowed": False,
        "production_effect": "none",
        "forward_progress": (
            f"{dashboard_summary.get('available_forward_events_total')}/"
            f"{dashboard_summary.get('required_forward_events_total')}"
        ),
        "sideways_progress": (
            f"{dashboard_summary.get('available_sideways_events')}/"
            f"{dashboard_summary.get('required_sideways_events')}"
        ),
        "recovery_progress": (
            f"{dashboard_summary.get('available_recovery_events')}/"
            f"{dashboard_summary.get('required_recovery_events')}"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_owner_renewal_options(
    recheck_decision: Mapping[str, Any],
    owner_decision: Mapping[str, Any],
) -> dict[str, Any]:
    current_recheck = _text(
        recheck_decision.get("recheck_decision"),
        "CONTINUE_OBSERVATION",
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "renewal_id": "",
        "candidate_method": _text(
            recheck_decision.get("candidate_method"),
            "smooth_weights_3d_limited_adjustment",
        ),
        "previous_owner_decision": _text(
            owner_decision.get("owner_decision"),
            "continue_observation",
        ),
        "current_recheck_decision": current_recheck,
        "recommended_owner_action": "continue_observation",
        "forward_progress": _text(recheck_decision.get("forward_progress"), "0/0"),
        "sideways_progress": _text(recheck_decision.get("sideways_progress"), "0/0"),
        "recovery_lag_status": (
            "NO_WARNING" if current_recheck == "READY_FOR_OWNER_REVIEW" else "INSUFFICIENT_EVENTS"
        ),
        "owner_options": [
            {
                "decision": "continue_observation",
                "recommended": True,
                "reason": "Forward confirmation remains in progress.",
            },
            {
                "decision": "request_more_forward_data",
                "recommended": False,
                "reason": "Use if owner wants stronger forward evidence before switch.",
            },
            {
                "decision": "promote_to_primary_research_candidate",
                "recommended": False,
                "reason": "Only appropriate if readiness recheck returns READY_FOR_OWNER_REVIEW.",
            },
            {
                "decision": "defer",
                "recommended": False,
                "reason": "Use if owner wants to wait for next weekly cycle.",
            },
            {
                "decision": "reject",
                "recommended": False,
                "reason": "Use if lag warning or forward underperformance appears.",
            },
        ],
        "auto_switch": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_emission_weights(model_target: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    method_weights = _mapping(
        _mapping(model_target.get("model_target_weights")).get("method_weights")
    )
    return {
        method: (
            _normalize_weights(_mapping(method_weights.get(method)))
            if method in method_weights
            else {}
        )
        for method in (
            "smooth_weights_3d_limited_adjustment",
            "smooth_weights_5d_limited_adjustment",
            "limited_adjustment",
            "static_baseline",
            "no_trade_baseline",
        )
    }


def _smoothed_symbols_from_weight_map(weights: Mapping[str, Any]) -> list[str]:
    symbols: set[str] = set()
    for method_weights in weights.values():
        for symbol in _mapping(method_weights):
            if symbol != "CASH":
                symbols.add(symbol)
    return sorted(symbols)


def _smoothed_symbols_from_collected_event_weights(
    rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    symbols: set[str] = set()
    for row in rows:
        for method_weights in _mapping(row.get("weights")).values():
            symbols.update(symbol for symbol in _mapping(method_weights) if symbol != "CASH")
    return sorted(symbols)


def _smoothed_price_pivot_or_empty(
    price_cache_path: Path,
    symbols: Sequence[str],
    *,
    start: date,
) -> pd.DataFrame:
    if not symbols or not price_cache_path.exists():
        return pd.DataFrame()
    try:
        return _load_price_pivot(price_cache_path, symbols, start)
    except (DynamicV3SystemTargetError, ValueError, FileNotFoundError):
        return pd.DataFrame()


def _smoothed_latest_price_date_or_none(pivot: pd.DataFrame) -> date | None:
    if pivot.empty:
        return None
    return pivot.index[-1].date()


def _smoothed_weight_validation(weights: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    required_present = all(bool(_mapping(row)) for row in weights.values())
    sums_to_one = required_present and all(
        abs(sum(_float(value) for value in _mapping(row).values()) - 1.0) <= 0.0001
        for row in weights.values()
    )
    no_negative = all(
        all(_float(value) >= -0.0000001 for value in _mapping(row).values())
        for row in weights.values()
    )
    status = "PASS" if required_present and sums_to_one and no_negative else "FAIL"
    return {
        "all_required_methods_present": required_present,
        "all_weights_sum_to_one": sums_to_one,
        "no_negative_weights": no_negative,
        "constraint_status": status,
    }


def _smoothed_regime_context_for_as_of(pivot: pd.DataFrame, as_of: date) -> str:
    if pivot.empty:
        return "unknown"
    history = pivot.loc[pivot.index.date <= as_of].tail(6)
    if len(history) < 2:
        return "unknown"
    returns = history.pct_change().dropna(how="all").fillna(0.0)
    qqq = returns["QQQ"] if "QQQ" in returns.columns else pd.Series(dtype=float)
    smh = returns["SMH"] if "SMH" in returns.columns else pd.Series(dtype=float)
    basket = pd.concat([qqq, smh], axis=1).mean(axis=1).dropna()
    if basket.empty:
        return "unknown"
    total = float((1.0 + basket).prod() - 1.0)
    if abs(total) <= SMOOTHED_CLASSIFIER_SIDEWAYS_ABS_RETURN_THRESHOLD:
        return "sideways_choppy"
    if total >= SMOOTHED_CLASSIFIER_STRONG_RECOVERY_RETURN_THRESHOLD:
        return "strong_recovery"
    if total <= -SMOOTHED_CLASSIFIER_STRONG_RECOVERY_RETURN_THRESHOLD:
        return "tech_drawdown"
    if "SMH" in history.columns:
        smh_total = float(history["SMH"].iloc[-1] / history["SMH"].iloc[0] - 1.0)
        if smh_total <= -SMOOTHED_CLASSIFIER_STRONG_RECOVERY_RETURN_THRESHOLD:
            return "semiconductor_pullback"
    return "ai_trend" if total > 0 else "unknown"


def _smoothed_emission_data_quality(
    *,
    as_of: date,
    latest_price_date: date | None,
    target_present: bool,
    weights_valid: bool,
    regime_context: str,
) -> dict[str, Any]:
    price_status = (
        "FAIL"
        if latest_price_date is None
        else ("PASS_WITH_WARNINGS" if as_of > latest_price_date else "PASS")
    )
    target_status = "PASS" if target_present and weights_valid else "FAIL"
    regime_status = "PASS_WITH_WARNINGS" if regime_context == "unknown" else "PASS"
    statuses = [price_status, target_status, regime_status]
    data_quality = (
        "FAIL"
        if "FAIL" in statuses
        else ("PASS_WITH_WARNINGS" if "PASS_WITH_WARNINGS" in statuses else "PASS")
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": as_of.isoformat(),
        "latest_price_date": latest_price_date.isoformat() if latest_price_date else None,
        "price_cache_status": price_status,
        "target_generation_status": target_status,
        "regime_tag_status": regime_status,
        "future_data_used": False,
        "data_quality": data_quality,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_daily_event_status(
    *,
    as_of: date,
    latest_price_date: date | None,
    target_present: bool,
    weights_valid: bool,
    data_quality: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if latest_price_date is None:
        reasons.append("price_cache_missing")
    elif as_of > latest_price_date:
        reasons.append("as_of_after_latest_price_date")
    if not target_present:
        reasons.append("model_target_missing")
    if not weights_valid:
        reasons.append("target_weights_invalid")
    if data_quality.get("data_quality") == "FAIL":
        reasons.append("data_quality_fail")
    if reasons:
        return "INSUFFICIENT_DATA", reasons
    return "ACTIVE", []


def _collect_smoothed_daily_emissions(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    emissions: list[dict[str, Any]] = []
    for manifest_path in sorted(root.glob("*/smoothed_daily_emission_manifest.json")):
        emission_root = manifest_path.parent
        manifest = _read_optional_json(manifest_path) or {}
        events = _read_jsonl(emission_root / "smoothed_forward_events.jsonl")
        weights = _read_optional_json(emission_root / "smoothed_event_weights.json") or {}
        for event in events:
            event.setdefault("source_emission_id", manifest.get("emission_id"))
        emissions.append({"manifest": manifest, "events": events, "weights": weights})
    return emissions


def _smoothed_events_by_id(emission_dir: Path) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for emission in _collect_smoothed_daily_emissions(emission_dir):
        for event in emission["events"]:
            if event.get("event_id"):
                events[_text(event.get("event_id"))] = dict(event)
    return events


def _smoothed_event_weights_by_id(emission_dir: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for emission in _collect_smoothed_daily_emissions(emission_dir):
        weights = _mapping(emission.get("weights"))
        if weights.get("event_id"):
            rows[_text(weights.get("event_id"))] = weights
    return rows


def _smoothed_due_window_row(
    event: Mapping[str, Any],
    *,
    window_days: int,
    scanner_as_of: date,
    trading_dates: Sequence[date],
    pivot: pd.DataFrame,
) -> dict[str, Any]:
    event_as_of = _coerce_date(event.get("as_of"), scanner_as_of)
    expected = _smoothed_expected_end_date(event_as_of, window_days, trading_dates)
    block_reasons: list[str] = []
    if event.get("event_status") != "ACTIVE":
        due_status = "PRICE_MISSING"
        block_reasons.append("event_not_active")
    elif expected > scanner_as_of:
        due_status = "NOT_DUE"
    elif expected not in set(trading_dates):
        due_status = "PRICE_MISSING"
        block_reasons.append("expected_end_price_missing")
    elif not _smoothed_price_date_available(pivot, expected):
        due_status = "PRICE_MISSING"
        block_reasons.append("expected_end_price_missing")
    else:
        due_status = "DUE"
    can_update = due_status == "DUE"
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event.get("event_id"),
        "as_of": event_as_of.isoformat(),
        "window_days": window_days,
        "expected_end_date": expected.isoformat(),
        "scanner_as_of": scanner_as_of.isoformat(),
        "due_status": due_status,
        "price_available": due_status == "DUE",
        "can_update": can_update,
        "block_reasons": block_reasons,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_expected_end_date(
    event_as_of: date,
    window_days: int,
    trading_dates: Sequence[date],
) -> date:
    future_dates = [item for item in sorted(set(trading_dates)) if item > event_as_of]
    if len(future_dates) >= window_days:
        return future_dates[window_days - 1]
    return _add_weekdays(event_as_of, window_days)


def _add_weekdays(start: date, days: int) -> date:
    current = start
    remaining = days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _smoothed_price_date_available(pivot: pd.DataFrame, target_date: date) -> bool:
    if pivot.empty:
        return False
    rows = pivot.loc[pivot.index.date == target_date]
    return not rows.empty and rows.notna().any(axis=None)


def _smoothed_due_summary(
    *,
    as_of: date,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    statuses = [_text(row.get("due_status")) for row in rows]
    return {
        "schema_version": SCHEMA_VERSION,
        "due_id": "",
        "scanner_as_of": as_of.isoformat(),
        "events_scanned": len({_text(row.get("event_id")) for row in rows if row.get("event_id")}),
        "total_windows_scanned": len(rows),
        "due_windows": statuses.count("DUE"),
        "not_due_windows": statuses.count("NOT_DUE"),
        "price_missing_windows": statuses.count("PRICE_MISSING"),
        "blocked_future_as_of": statuses.count("BLOCKED_FUTURE_AS_OF"),
        "update_ready_count": sum(1 for row in rows if row.get("can_update") is True),
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_skipped_outcome_row(row: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": row.get("event_id"),
        "window_days": int(_float(row.get("window_days"))),
        "skip_reason": reason or "DATA_QUALITY_FAIL",
        "old_status": "PENDING",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_updated_outcome_row(
    row: Mapping[str, Any],
    event_weights: Mapping[str, Any],
    pivot: pd.DataFrame,
) -> dict[str, Any] | None:
    weights = _mapping(event_weights.get("weights"))
    if not weights or pivot.empty:
        return None
    start = _coerce_date(row.get("as_of"), date.min)
    end = _coerce_date(row.get("expected_end_date"), date.min)
    if not _smoothed_price_date_available(pivot, start):
        return None
    if not _smoothed_price_date_available(pivot, end):
        return None
    method_returns = {
        method: _smoothed_portfolio_window_return(_mapping(method_weights), pivot, start, end)
        for method, method_weights in weights.items()
    }
    drawdowns = {
        method: _smoothed_portfolio_window_drawdown(_mapping(method_weights), pivot, start, end)
        for method, method_weights in weights.items()
    }
    smooth_3d = _float(method_returns.get("smooth_weights_3d_limited_adjustment"))
    smooth_5d = _float(method_returns.get("smooth_weights_5d_limited_adjustment"))
    limited = _float(method_returns.get("limited_adjustment"))
    static = _float(method_returns.get("static_baseline"))
    no_trade = _float(method_returns.get("no_trade_baseline"))
    smooth_dd = _float(drawdowns.get("smooth_weights_3d_limited_adjustment"))
    limited_dd = _float(drawdowns.get("limited_adjustment"))
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": row.get("event_id"),
        "as_of": start.isoformat(),
        "window_days": int(_float(row.get("window_days"))),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "method_returns": {key: round(value, 10) for key, value in method_returns.items()},
        "relative_metrics": {
            "smooth_3d_vs_limited": round(smooth_3d - limited, 10),
            "smooth_3d_vs_static": round(smooth_3d - static, 10),
            "smooth_3d_vs_no_trade": round(smooth_3d - no_trade, 10),
            "smooth_5d_vs_smooth_3d": round(smooth_5d - smooth_3d, 10),
        },
        "drawdown_metrics": {
            "smooth_3d_drawdown": round(smooth_dd, 10),
            "limited_drawdown": round(limited_dd, 10),
            "smooth_3d_drawdown_delta_vs_limited": round(smooth_dd - limited_dd, 10),
        },
        "outcome_status": "AVAILABLE",
        "future_data_used": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_portfolio_window_return(
    weights: Mapping[str, Any],
    pivot: pd.DataFrame,
    start: date,
    end: date,
) -> float:
    start_row = pivot.loc[pivot.index.date == start].iloc[-1]
    end_row = pivot.loc[pivot.index.date == end].iloc[-1]
    value = 0.0
    for symbol, weight in weights.items():
        if symbol == "CASH":
            continue
        if symbol not in pivot.columns:
            continue
        start_price = _float(start_row.get(symbol))
        end_price = _float(end_row.get(symbol))
        if start_price > 0:
            value += _float(weight) * (end_price / start_price - 1.0)
    return round(value, 10)


def _smoothed_portfolio_window_drawdown(
    weights: Mapping[str, Any],
    pivot: pd.DataFrame,
    start: date,
    end: date,
) -> float:
    window = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    if len(window) < 2:
        return 0.0
    returns = window.pct_change().dropna(how="all").fillna(0.0)
    series = pd.Series(0.0, index=returns.index)
    for symbol, weight in weights.items():
        if symbol == "CASH" or symbol not in returns.columns:
            continue
        series = series + returns[symbol].fillna(0.0) * _float(weight)
    equity = (1.0 + series).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    return round(float(drawdown.min()), 10)


def _smoothed_outcome_delta_summary(
    updated: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rel = [
        _float(_mapping(row.get("relative_metrics")).get("smooth_3d_vs_limited")) for row in updated
    ]
    drawdown = [
        _float(_mapping(row.get("drawdown_metrics")).get("smooth_3d_drawdown_delta_vs_limited"))
        for row in updated
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "update_id": "",
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "available_forward_events_after_update": len(
            {_text(row.get("event_id")) for row in updated if row.get("event_id")}
        ),
        "smooth_3d_win_rate_vs_limited": (
            round(sum(1 for value in rel if value > 0.0) / len(rel), 10) if rel else None
        ),
        "avg_smooth_3d_relative_return_vs_limited": (round(_mean_float(rel), 10) if rel else None),
        "avg_smooth_3d_drawdown_delta_vs_limited": (
            round(_mean_float(drawdown), 10) if drawdown else None
        ),
        "summary_recommendation": "continue_tracking",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_classified_forward_event(
    outcome: Mapping[str, Any],
    events: Mapping[str, Mapping[str, Any]],
    weights_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    event_id = _text(outcome.get("event_id"))
    event = _mapping(events.get(event_id))
    relative = _mapping(outcome.get("relative_metrics"))
    method_returns = _mapping(outcome.get("method_returns"))
    regime_context = _text(event.get("regime_context"), "unknown")
    classes, confidence = _smoothed_regime_classes_from_outcome(regime_context, outcome)
    weights = _mapping(_mapping(weights_by_id.get(event_id)).get("weights"))
    turnover_delta = _smoothed_turnover_delta(
        _mapping(weights.get("smooth_weights_3d_limited_adjustment")),
        _mapping(weights.get("limited_adjustment")),
    )
    smooth_vs_limited = _float(relative.get("smooth_3d_vs_limited"))
    recovery_relevant = any(item in {"strong_recovery", "fast_regime_change"} for item in classes)
    lag_warning = (
        recovery_relevant and smooth_vs_limited < SMOOTHED_CLASSIFIER_LAG_WARNING_DELTA_THRESHOLD
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "as_of": outcome.get("as_of"),
        "window_days": int(_float(outcome.get("window_days"))),
        "regime_classification": classes,
        "classification_confidence": confidence,
        "sideways_relevant": "sideways_choppy" in classes,
        "recovery_lag_relevant": recovery_relevant,
        "smooth_3d_vs_limited": round(smooth_vs_limited, 10),
        "turnover_delta": round(turnover_delta, 10),
        "signal_churn_delta": None,
        "lag_warning": lag_warning,
        "event_status": "AVAILABLE" if method_returns else "INSUFFICIENT_DATA",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_regime_classes_from_outcome(
    regime_context: str,
    outcome: Mapping[str, Any],
) -> tuple[list[str], str]:
    if regime_context in {"sideways_choppy", "strong_recovery"}:
        return [regime_context], "HIGH"
    if regime_context in {"tech_drawdown", "semiconductor_pullback"}:
        return ["fast_regime_change"], "MEDIUM"
    returns = _mapping(outcome.get("method_returns"))
    limited = _float(returns.get("limited_adjustment"))
    smooth = _float(returns.get("smooth_weights_3d_limited_adjustment"))
    proxy = max(abs(limited), abs(smooth))
    if proxy <= SMOOTHED_CLASSIFIER_SIDEWAYS_ABS_RETURN_THRESHOLD:
        return ["sideways_choppy"], "LOW"
    if max(limited, smooth) >= SMOOTHED_CLASSIFIER_STRONG_RECOVERY_RETURN_THRESHOLD:
        return ["strong_recovery"], "LOW"
    if proxy >= SMOOTHED_CLASSIFIER_FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD:
        return ["fast_regime_change"], "LOW"
    return ["normal"], "LOW"


def _smoothed_turnover_delta(
    smooth_weights: Mapping[str, Any],
    limited_weights: Mapping[str, Any],
) -> float:
    symbols = set(smooth_weights) | set(limited_weights)
    return 0.5 * sum(
        abs(_float(smooth_weights.get(symbol)) - _float(limited_weights.get(symbol)))
        for symbol in symbols
    )


def _smoothed_classification_summary(
    classified: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sideways = [row for row in classified if row.get("sideways_relevant") is True]
    recovery = [row for row in classified if row.get("recovery_lag_relevant") is True]
    fast = [
        row
        for row in classified
        if "fast_regime_change" in _texts(row.get("regime_classification"))
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "classification_id": "",
        "events_classified": len(classified),
        "sideways_events_available": len({_text(row.get("event_id")) for row in sideways}),
        "recovery_events_available": len({_text(row.get("event_id")) for row in recovery}),
        "fast_regime_change_events_available": len({_text(row.get("event_id")) for row in fast}),
        "lag_warning_count": sum(1 for row in classified if row.get("lag_warning") is True),
        "sideways_progress_delta": len(sideways),
        "recovery_progress_delta": len(recovery),
        **SYSTEM_TARGET_SAFETY,
    }


def _collect_updated_smoothed_outcomes(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    paths = sorted(
        root.glob("*/updated_smoothed_outcomes.jsonl"),
        key=lambda path: path.stat().st_mtime,
    )
    for path in paths:
        for row in _read_jsonl(path):
            key = (_text(row.get("event_id")), int(_float(row.get("window_days"))))
            if key[0]:
                rows_by_key[key] = row
    return list(rows_by_key.values())


def _collect_classified_smoothed_events(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    paths = sorted(
        root.glob("*/classified_forward_events.jsonl"),
        key=lambda path: path.stat().st_mtime,
    )
    for path in paths:
        for row in _read_jsonl(path):
            key = (_text(row.get("event_id")), int(_float(row.get("window_days"))))
            if key[0]:
                rows_by_key[key] = row
    return list(rows_by_key.values())


def _available_events_by_window(
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    by_window: dict[str, set[str]] = {
        str(window): set() for window in SMOOTHED_CONFIRMATION_WINDOWS
    }
    for row in outcomes:
        if row.get("outcome_status") != "AVAILABLE":
            continue
        window = str(int(_float(row.get("window_days"))))
        if window in by_window and row.get("event_id"):
            by_window[window].add(_text(row.get("event_id")))
    return {window: len(events) for window, events in by_window.items()}


def _smoothed_forward_current_metrics(
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rel = [
        _float(_mapping(row.get("relative_metrics")).get("smooth_3d_vs_limited"))
        for row in outcomes
        if row.get("outcome_status") == "AVAILABLE"
    ]
    drawdown = [
        _float(_mapping(row.get("drawdown_metrics")).get("smooth_3d_drawdown_delta_vs_limited"))
        for row in outcomes
        if row.get("outcome_status") == "AVAILABLE"
    ]
    return {
        "avg_relative_return": round(_mean_float(rel), 10) if rel else None,
        "win_rate_vs_limited": (
            round(sum(1 for value in rel if value > 0.0) / len(rel), 10) if rel else None
        ),
        "drawdown_delta": round(_mean_float(drawdown), 10) if drawdown else None,
        "turnover_delta": None,
        "rolling_consistency_delta": None,
    }


def _smoothed_classified_metric_summary(
    classified: Sequence[Mapping[str, Any]],
    *,
    relevant_field: str,
) -> dict[str, Any]:
    rows = [row for row in classified if row.get(relevant_field) is True]
    rel = [_float(row.get("smooth_3d_vs_limited")) for row in rows]
    turnover = [_float(row.get("turnover_delta")) for row in rows]
    return {
        "avg_relative_return": round(_mean_float(rel), 10) if rel else None,
        "win_rate_vs_limited": (
            round(sum(1 for value in rel if value > 0.0) / len(rel), 10) if rel else None
        ),
        "drawdown_delta": None,
        "turnover_delta": round(_mean_float(turnover), 10) if turnover else None,
        "signal_churn_delta": None,
        "weight_jump_delta": None,
    }


def _smoothed_recovery_metric_summary(
    classified: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [row for row in classified if row.get("recovery_lag_relevant") is True]
    return {
        "missed_upside": None,
        "risk_on_response_delay_days": None,
        "lag_warning": any(row.get("lag_warning") is True for row in rows),
    }


def _record_weekly_step(
    steps: list[dict[str, Any]],
    artifacts: dict[str, Any],
    step: str,
    artifact_id: str,
) -> None:
    steps.append({"step": step, "status": "PASS", "artifact_id": artifact_id})
    artifacts[step] = {"artifact_id": artifact_id}


def _smoothed_weekly_run_summary(
    *,
    week_ending: date,
    emission: Mapping[str, Any],
    due: Mapping[str, Any],
    update: Mapping[str, Any],
    classification: Mapping[str, Any],
    progress: Mapping[str, Any],
    recheck: Mapping[str, Any],
) -> dict[str, Any]:
    emission_manifest = _mapping(emission.get("manifest"))
    due_summary = _mapping(due.get("due_summary"))
    update_summary = _mapping(update.get("smoothed_outcome_delta_summary"))
    classification_summary = _mapping(classification.get("classification_summary"))
    progress_summary = _mapping(progress.get("smoothed_forward_progress_summary"))
    recheck_decision = _mapping(recheck.get("switch_readiness_decision"))
    return {
        "schema_version": SCHEMA_VERSION,
        "weekly_run_id": "",
        "week_ending": week_ending.isoformat(),
        "emitted_events": int(_float(emission_manifest.get("emitted_event_count"))),
        "due_windows": int(_float(due_summary.get("due_windows"))),
        "updated_windows": int(_float(update_summary.get("updated_count"))),
        "classified_events": int(_float(classification_summary.get("events_classified"))),
        "available_forward_events": int(
            _float(progress_summary.get("available_forward_events_total"))
        ),
        "required_forward_events": int(
            _float(progress_summary.get("required_forward_events_total"))
        ),
        "available_sideways_events": int(_float(progress_summary.get("available_sideways_events"))),
        "required_sideways_events": int(_float(progress_summary.get("required_sideways_events"))),
        "available_recovery_events": int(_float(progress_summary.get("available_recovery_events"))),
        "required_recovery_events": int(_float(progress_summary.get("required_recovery_events"))),
        "can_execute_switch": recheck_decision.get("can_execute_switch") is True,
        "weekly_recommendation": "continue_observation",
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_requested_date(
    *,
    requested_as_of: date | None,
    requested_week_ending: date | None,
) -> date:
    if requested_as_of is None and requested_week_ending is None:
        raise DynamicV3SystemTargetError("requested_as_of or requested_week_ending is required")
    if requested_as_of is not None and requested_week_ending is not None:
        raise DynamicV3SystemTargetError(
            "requested_as_of and requested_week_ending cannot both be set"
        )
    return requested_as_of or requested_week_ending or date.min


def _smoothed_preflight_data_quality_report(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
) -> DataQualityReport:
    expected_price_tickers = _smoothed_expected_price_tickers(prices_path)
    expected_rate_series = _smoothed_expected_rate_series(rates_path)
    return validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=_smoothed_download_manifest_path(prices_path),
        secondary_prices_path=_smoothed_marketstack_prices_path(prices_path),
        require_secondary_prices=_smoothed_requires_marketstack_prices(prices_path),
    )


def _smoothed_expected_price_tickers(prices_path: Path) -> list[str]:
    if _smoothed_is_default_price_cache(prices_path):
        try:
            return configured_price_tickers(load_universe())
        except (OSError, ValueError, TypeError):
            pass
    inferred = _infer_csv_values(prices_path, "ticker")
    if inferred:
        return inferred
    try:
        return configured_price_tickers(load_universe())
    except (OSError, ValueError, TypeError):
        return []


def _smoothed_expected_rate_series(rates_path: Path) -> list[str]:
    if _smoothed_is_default_rates_cache(rates_path):
        try:
            return configured_rate_series(load_universe())
        except (OSError, ValueError, TypeError):
            pass
    inferred = _infer_csv_values(rates_path, "series")
    if inferred:
        return inferred
    try:
        return configured_rate_series(load_universe())
    except (OSError, ValueError, TypeError):
        return []


def _smoothed_data_freshness_snapshot(
    *,
    requested_as_of: date | None,
    requested_week_ending: date | None,
    quality_report: DataQualityReport,
    prices_path: Path,
    rates_path: Path,
    model_target_dir: Path,
) -> dict[str, Any]:
    requested_date = _smoothed_requested_date(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
    )
    price_latest = quality_report.price_summary.max_date
    secondary_latest = (
        quality_report.secondary_price_summary.max_date
        if quality_report.secondary_price_summary is not None
        else None
    )
    rates_latest = quality_report.rate_summary.max_date
    require_secondary = _smoothed_requires_marketstack_prices(prices_path)
    latest_valid = _smoothed_latest_valid_as_of(
        price_latest=price_latest,
        secondary_latest=secondary_latest,
        rates_latest=rates_latest,
        require_secondary=require_secondary,
    )
    errors = _smoothed_issue_codes(quality_report, severity="ERROR")
    warnings = _smoothed_issue_codes(quality_report, severity="WARNING")
    freshness_status = _smoothed_freshness_status(
        requested_date=requested_date,
        latest_valid_as_of=latest_valid,
        validate_data_status=quality_report.status,
        blocking_errors=errors,
    )
    model_target_as_of = _smoothed_latest_model_target_as_of(model_target_dir)
    return {
        "schema_version": SCHEMA_VERSION,
        "preflight_id": "",
        "requested_as_of": requested_as_of.isoformat() if requested_as_of else None,
        "requested_week_ending": (
            requested_week_ending.isoformat() if requested_week_ending else None
        ),
        "requested_date": requested_date.isoformat(),
        "latest_available": {
            "prices_daily": _date_or_none(price_latest),
            "prices_marketstack_daily": _date_or_none(secondary_latest),
            "rates_daily": _date_or_none(rates_latest),
            "model_target_available_as_of": _date_or_none(model_target_as_of),
            "regime_tags_available_as_of": _date_or_none(price_latest),
        },
        "latest_valid_as_of": _date_or_none(latest_valid),
        "freshness_status": freshness_status,
        "validate_data_status": quality_report.status,
        "blocking_errors": errors,
        "warnings": warnings,
        "quality_issues": _smoothed_quality_issues_payload(quality_report),
        "source_paths": {
            "prices_daily": str(prices_path),
            "prices_marketstack_daily": str(_smoothed_marketstack_prices_path(prices_path)),
            "rates_daily": str(rates_path),
        },
        "future_data_used": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_latest_valid_as_of(
    *,
    price_latest: date | None,
    secondary_latest: date | None,
    rates_latest: date | None,
    require_secondary: bool,
) -> date | None:
    required = [price_latest, rates_latest]
    if require_secondary:
        required.append(secondary_latest)
    if any(item is None for item in required):
        return None
    return min(item for item in required if item is not None)


def _smoothed_freshness_status(
    *,
    requested_date: date,
    latest_valid_as_of: date | None,
    validate_data_status: str,
    blocking_errors: Sequence[str],
) -> str:
    if blocking_errors:
        if any(code in {"prices_stale", "rates_stale"} for code in blocking_errors):
            return "BLOCKED_STALE_DATA"
        if any(code in {"prices_future_dates", "rates_future_dates"} for code in blocking_errors):
            return "BLOCKED_FUTURE_AS_OF"
        if any("price" in code and "missing" in code for code in blocking_errors):
            return "BLOCKED_MISSING_PRICE"
        return "BLOCKED_DATA_QUALITY_FAIL"
    if latest_valid_as_of is not None and requested_date > latest_valid_as_of:
        return "LATEST_AVAILABLE_ONLY"
    if validate_data_status == "PASS_WITH_WARNINGS":
        return "READY_WITH_WARNINGS"
    return "READY"


def _smoothed_runnable_command_matrix(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    requested_date = _text(snapshot.get("requested_date"))
    latest_valid = _text(snapshot.get("latest_valid_as_of"))
    freshness_status = _text(snapshot.get("freshness_status"))
    ready = freshness_status in {"READY", "READY_WITH_WARNINGS"}
    latest_available_only = freshness_status == "LATEST_AVAILABLE_ONLY"
    latest_fallback_allowed = bool(latest_valid) and (
        latest_available_only or freshness_status == "BLOCKED_STALE_DATA"
    )
    blocked_reason = _smoothed_command_block_reason(snapshot)
    commands = [
        {
            "command": "smoothed-daily-emission",
            "requested_as_of": requested_date,
            "status": "RUNNABLE" if ready else "BLOCKED",
            "reason": "requested_as_of_supported" if ready else blocked_reason,
        },
        {
            "command": "smoothed-daily-emission",
            "requested_as_of": "latest_valid_as_of",
            "resolved_as_of": latest_valid or None,
            "status": (
                "RUNNABLE_WITH_LATEST_AVAILABLE"
                if latest_fallback_allowed
                else ("RUNNABLE" if ready else "BLOCKED")
            ),
            "reason": (
                "safe_observation_emission_only"
                if latest_fallback_allowed
                else ("requested_as_of_supported" if ready else "no_latest_valid_as_of")
            ),
        },
        {
            "command": "smoothed-outcome-due",
            "requested_as_of": requested_date,
            "status": "RUNNABLE" if ready else "BLOCKED",
            "reason": "requested_as_of_supported" if ready else blocked_reason,
        },
        {
            "command": "smoothed-outcome-update",
            "requested_as_of": requested_date,
            "status": "RUNNABLE" if ready else "BLOCKED",
            "reason": "requested_as_of_supported" if ready else blocked_reason,
        },
        {
            "command": "smoothed-forward-weekly-run",
            "requested_week_ending": requested_date,
            "status": "RUNNABLE" if ready else "BLOCKED",
            "reason": "requested_week_ending_supported" if ready else blocked_reason,
        },
    ]
    return {"schema_version": SCHEMA_VERSION, "preflight_id": "", "commands": commands}


def _smoothed_blocked_reason_matrix(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    reasons = _texts(snapshot.get("blocking_errors"))
    if not reasons and snapshot.get("freshness_status") == "LATEST_AVAILABLE_ONLY":
        reasons = ["requested_as_of_after_latest_valid_as_of"]
    rows = [_smoothed_blocked_reason_row(reason, snapshot) for reason in reasons]
    return {
        "schema_version": SCHEMA_VERSION,
        "preflight_id": "",
        "blocked_reasons": rows,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_blocked_reason_row(reason: str, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    latest = _mapping(snapshot.get("latest_available"))
    requested = _text(snapshot.get("requested_date"))
    latest_available = None
    blocks = ["validate_data", "weekly_runner"]
    fallback_scope = ["diagnostic_report"]
    can_fallback = False
    if reason.startswith("prices_") or reason == "requested_as_of_after_latest_valid_as_of":
        latest_available = latest.get("prices_daily")
        blocks = ["outcome_due_scan", "outcome_update", "weekly_runner"]
        fallback_scope = ["daily_emission_only"]
        can_fallback = bool(snapshot.get("latest_valid_as_of"))
    if reason.startswith("rates_"):
        latest_available = latest.get("rates_daily")
        blocks = ["validate_data", "weekly_runner"]
        fallback_scope = ["diagnostic_report", "daily_emission_if_target_generation_safe"]
        can_fallback = bool(snapshot.get("latest_valid_as_of"))
    return {
        "reason": reason,
        "severity": "BLOCKING",
        "latest_available": latest_available,
        "requested_date": requested,
        "blocks": blocks,
        "can_fallback_to_latest_available": can_fallback,
        "fallback_scope": fallback_scope,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_command_block_reason(snapshot: Mapping[str, Any]) -> str:
    errors = _texts(snapshot.get("blocking_errors"))
    if "prices_stale" in errors and "rates_stale" in errors:
        return "prices_stale_and_rates_stale"
    if errors:
        return "_and_".join(errors)
    if snapshot.get("freshness_status") == "LATEST_AVAILABLE_ONLY":
        return "requested_as_of_after_latest_valid_as_of"
    if snapshot.get("freshness_status") == "BLOCKED_DATA_QUALITY_FAIL":
        return "validate_data_status_fail"
    return "validate_data_status_fail"


def _smoothed_blocked_command_explanations(
    snapshot: Mapping[str, Any],
    command_matrix: Mapping[str, Any],
) -> list[dict[str, Any]]:
    latest = _mapping(snapshot.get("latest_available"))
    requested = _text(snapshot.get("requested_date"))
    errors = _texts(snapshot.get("blocking_errors"))
    rows: list[dict[str, Any]] = []
    for command in _records(command_matrix.get("commands")):
        if command.get("status") != "BLOCKED":
            continue
        name = _text(command.get("command"))
        rows.append(
            {
                "command": _smoothed_human_command(name, requested),
                "status": "BLOCKED",
                "validate_data_status": snapshot.get("validate_data_status"),
                "blocking_errors": errors,
                "human_explanation": _smoothed_human_blocked_explanation(
                    name,
                    requested,
                    latest,
                ),
                "safe_next_action": _smoothed_safe_next_action(name),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _smoothed_human_command(command: str, requested: str) -> str:
    if command == "smoothed-outcome-due":
        return f"smoothed-outcome-due scan --as-of {requested}"
    if command == "smoothed-forward-weekly-run":
        return f"smoothed-forward-weekly-run run --week-ending {requested}"
    if command == "smoothed-outcome-update":
        return "smoothed-outcome-update run --due-id <due_id>"
    return f"{command} run --as-of {requested}"


def _smoothed_human_blocked_explanation(
    command: str,
    requested: str,
    latest: Mapping[str, Any],
) -> str:
    if command == "smoothed-forward-weekly-run":
        return (
            "Weekly runner cannot update or classify forward outcomes until all required "
            f"data sources cover the week ending date {requested}. "
            f"Current prices latest={latest.get('prices_daily')}, "
            f"rates latest={latest.get('rates_daily')}."
        )
    if command == "smoothed-daily-emission":
        return (
            f"Requested daily emission uses requested as_of {requested}, but local caches "
            f"are only fresh through latest_valid_as_of. Use latest-available emission "
            "only if observation-only fallback is acceptable."
        )
    return (
        f"Requested due/update command requires price and rate data through {requested}, "
        f"but local caches are only fresh through prices={latest.get('prices_daily')} "
        f"and rates={latest.get('rates_daily')}."
    )


def _smoothed_safe_next_action(command: str) -> str:
    if command == "smoothed-daily-emission":
        return "run_latest_available_emission"
    if command == "smoothed-forward-weekly-run":
        return "run_latest_available_emission_or_wait_for_refresh"
    return "refresh_sources_then_retry"


def _smoothed_source_refresh_requirements(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    latest = _mapping(snapshot.get("latest_available"))
    requested = _text(snapshot.get("requested_date"))
    rows = [
        _smoothed_source_requirement_row(
            source="prices_daily.csv",
            current_latest=latest.get("prices_daily"),
            required_through=requested,
            action="refresh_price_cache",
            required=True,
        ),
        _smoothed_source_requirement_row(
            source="prices_marketstack_daily.csv",
            current_latest=latest.get("prices_marketstack_daily"),
            required_through=requested,
            action="refresh_marketstack_price_cache",
            required=latest.get("prices_marketstack_daily") is not None,
        ),
        _smoothed_source_requirement_row(
            source="rates_daily.csv",
            current_latest=latest.get("rates_daily"),
            required_through=requested,
            action="refresh_rates_cache",
            required=True,
        ),
    ]
    all_fresh = all(row["status"] == "FRESH" for row in rows if row.get("required") is True)
    return {
        "schema_version": SCHEMA_VERSION,
        "refresh_plan_id": "",
        "requested_as_of": requested,
        "source_requirements": rows,
        "all_required_sources_fresh": all_fresh,
        "external_refresh_executed": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_source_requirement_row(
    *,
    source: str,
    current_latest: object,
    required_through: str,
    action: str,
    required: bool,
) -> dict[str, Any]:
    latest_date = _coerce_date(current_latest, date.min)
    required_date = _coerce_date(required_through, date.max)
    if latest_date == date.min:
        status = "MISSING" if required else "OPTIONAL_MISSING"
    elif latest_date >= required_date:
        status = "FRESH"
    else:
        status = "STALE"
    return {
        "source": source,
        "current_latest_date": None if latest_date == date.min else latest_date.isoformat(),
        "required_through": required_through,
        "status": status,
        "required": required,
        "required_action": action if status != "FRESH" else "none",
    }


def _smoothed_rerun_command_plan(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    requested = _text(snapshot.get("requested_date"))
    use_week = snapshot.get("requested_week_ending") is not None
    preflight_command = (
        "aits etf dynamic-v3-rescue smoothed-data-preflight run "
        f"--requested-week-ending {requested}"
        if use_week
        else "aits etf dynamic-v3-rescue smoothed-data-preflight run "
        f"--requested-as-of {requested}"
    )
    ready = snapshot.get("freshness_status") in {"READY", "READY_WITH_WARNINGS"}
    return {
        "schema_version": SCHEMA_VERSION,
        "refresh_plan_id": "",
        "rerun_after_refresh": [
            {
                "step": 1,
                "command": preflight_command,
                "purpose": "confirm freshness",
            },
            {
                "step": 2,
                "command": (
                    "aits etf dynamic-v3-rescue smoothed-outcome-due scan " f"--as-of {requested}"
                ),
                "purpose": "find due windows",
            },
            {
                "step": 3,
                "command": (
                    "aits etf dynamic-v3-rescue smoothed-outcome-update run " "--due-id <due_id>"
                ),
                "purpose": "update matured outcomes",
            },
            {
                "step": 4,
                "command": (
                    "aits etf dynamic-v3-rescue smoothed-forward-classify run "
                    "--update-id <update_id>"
                ),
                "purpose": "classify sideways / recovery events",
            },
            {
                "step": 5,
                "command": (
                    "aits etf dynamic-v3-rescue smoothed-forward-weekly-run run "
                    f"--week-ending {requested}"
                ),
                "purpose": "run weekly chain",
            },
        ],
        "rerun_allowed_now": ready,
        "blocking_reason": None if ready else "sources_not_fresh",
        "external_refresh_executed": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _append_retry_skipped_steps(
    steps: list[dict[str, Any]],
    names: Sequence[str],
    reason: str,
) -> None:
    for name in names:
        steps.append({"step": name, "status": "SKIPPED", "artifact_id": None, "reason": reason})


def _smoothed_retry_summary(
    *,
    retry_id: str,
    requested_date: date,
    preflight_status: str,
    weekly: Mapping[str, Any] | None,
    latest_emission: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if weekly is not None:
        weekly_summary = _mapping(weekly.get("weekly_run_summary"))
        return {
            "schema_version": SCHEMA_VERSION,
            "retry_id": retry_id,
            "requested_as_of": requested_date.isoformat(),
            "retry_status": "COMPLETED",
            "emitted_events": int(_float(weekly_summary.get("emitted_events"))),
            "due_windows": int(_float(weekly_summary.get("due_windows"))),
            "updated_windows": int(_float(weekly_summary.get("updated_windows"))),
            "classified_events": int(_float(weekly_summary.get("classified_events"))),
            "available_forward_events_after_retry": int(
                _float(weekly_summary.get("available_forward_events"))
            ),
            "available_sideways_events_after_retry": int(
                _float(weekly_summary.get("available_sideways_events"))
            ),
            "available_recovery_events_after_retry": int(
                _float(weekly_summary.get("available_recovery_events"))
            ),
            "can_execute_switch": False,
            "weekly_recommendation": "continue_observation",
            "broker_action_allowed": False,
            "production_effect": "none",
            **SYSTEM_TARGET_SAFETY,
        }
    if latest_emission is not None:
        links = _mapping(latest_emission.get("latest_emission_artifact_links"))
        return _smoothed_empty_retry_summary(
            retry_id=retry_id,
            requested_date=requested_date,
            retry_status="PARTIAL",
            emitted_events=int(_float(links.get("emitted_event_count"))),
        )
    status = "BLOCKED" if preflight_status.startswith("BLOCKED") else "FAIL"
    return _smoothed_empty_retry_summary(
        retry_id=retry_id,
        requested_date=requested_date,
        retry_status=status,
        emitted_events=0,
    )


def _smoothed_empty_retry_summary(
    *,
    retry_id: str,
    requested_date: date,
    retry_status: str,
    emitted_events: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "retry_id": retry_id,
        "requested_as_of": requested_date.isoformat(),
        "retry_status": retry_status,
        "emitted_events": emitted_events,
        "due_windows": 0,
        "updated_windows": 0,
        "classified_events": 0,
        "available_forward_events_after_retry": 0,
        "available_sideways_events_after_retry": 0,
        "available_recovery_events_after_retry": 0,
        "can_execute_switch": False,
        "weekly_recommendation": "continue_observation",
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _load_smoothed_source_refresh_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "refresh": {
                "mode": "controlled_cache_refresh",
                "default_execute": False,
                "require_explicit_execute_flag": True,
                "allow_partial_refresh": True,
            },
            "sources": {},
            "safety": dict(SYSTEM_TARGET_SAFETY),
        }
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise DynamicV3SystemTargetError(f"source refresh config must be a mapping: {path}")
    safety = {**SYSTEM_TARGET_SAFETY, **_mapping(payload.get("safety"))}
    return {**payload, "safety": safety}


def _smoothed_refresh_source_specs(
    *,
    source_rows: Sequence[Mapping[str, Any]],
    price_cache_path: Path,
    marketstack_cache_path: Path,
    rates_path: Path,
) -> list[dict[str, Any]]:
    rows = list(source_rows) or [
        {"source": "prices_daily.csv", "required": True},
        {"source": "prices_marketstack_daily.csv", "required": True},
        {"source": "rates_daily.csv", "required": True},
    ]
    path_by_source = {
        "prices_daily": price_cache_path,
        "prices_marketstack_daily": marketstack_cache_path,
        "rates_daily": rates_path,
    }
    specs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        source = _smoothed_normalize_source_name(row.get("source"))
        if source in seen or source not in path_by_source:
            continue
        seen.add(source)
        specs.append(
            {
                "source": source,
                "cache_path": str(path_by_source[source]),
                "required": row.get("required", True) is not False,
                "required_through": _text(row.get("required_through")),
                "planned_status": _text(row.get("status")),
                "required_action": _text(row.get("required_action")),
            }
        )
    return specs


def _smoothed_normalize_source_name(value: object) -> str:
    text = _text(value).removesuffix(".csv")
    if text == "prices":
        return "prices_daily"
    if text == "marketstack":
        return "prices_marketstack_daily"
    if text == "rates":
        return "rates_daily"
    return text


def _cache_file_audit_state(source: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    row_count = 0
    latest: str | None = None
    read_error: str | None = None
    if exists:
        try:
            frame = pd.read_csv(path, usecols=["date"])
            row_count = int(len(frame))
            dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
            if not dates.empty:
                latest = dates.max().date().isoformat()
        except (OSError, ValueError, pd.errors.ParserError) as exc:
            read_error = _text(exc)
    return {
        "source": source,
        "cache_path": str(path),
        "exists": exists,
        "row_count": row_count,
        "latest_date": latest,
        "checksum": _file_sha256(path) if exists else None,
        "read_error": read_error,
    }


def _file_sha256(path: Path) -> str | None:
    try:
        digest = sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _execute_smoothed_project_data_refresh(
    *,
    requested_as_of: date,
    config: Mapping[str, Any],
    output_dir: Path,
) -> None:
    refresh = _mapping(config.get("refresh"))
    start_date = _coerce_date(refresh.get("start"), AI_AFTER_CHATGPT_START)
    provider_name = _text(refresh.get("price_provider"), "fmp").lower()
    if provider_name == "fmp" and os.getenv("FMP_API_KEY"):
        price_provider = FmpPriceProvider(api_key=os.environ["FMP_API_KEY"])
    elif provider_name == "fmp":
        price_provider = YFinancePriceProvider()
    elif provider_name == "yahoo":
        price_provider = YFinancePriceProvider()
    else:
        raise DynamicV3SystemTargetError(
            f"unsupported smoothed source refresh price_provider: {provider_name}"
        )
    marketstack_provider = (
        MarketstackPriceProvider(api_key=os.environ["MARKETSTACK_API_KEY"])
        if os.getenv("MARKETSTACK_API_KEY")
        else None
    )
    download_daily_data(
        load_universe(),
        start=start_date,
        end=requested_as_of,
        output_dir=output_dir,
        include_full_ai_chain=bool(refresh.get("include_full_ai_chain", False)),
        price_provider=price_provider,
        secondary_price_provider=marketstack_provider,
    )


def _smoothed_source_refresh_result_row(
    *,
    spec: Mapping[str, Any],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    requested_as_of: date,
    execute_refresh: bool,
    refresh_error: str | None,
) -> dict[str, Any]:
    after_latest = _coerce_date(after.get("latest_date"), date.min)
    ready = after_latest >= requested_as_of
    if not execute_refresh:
        status = "DRY_RUN_ONLY"
        error = None
    elif refresh_error and not ready:
        status = "FAILED"
        error = refresh_error
    elif ready:
        status = (
            "REFRESHED"
            if before.get("checksum") != after.get("checksum")
            or before.get("latest_date") != after.get("latest_date")
            else "SKIPPED"
        )
        error = None
    else:
        status = "FAILED"
        error = refresh_error or "source_still_stale_after_refresh"
    freshness = "READY" if ready else ("STILL_STALE" if after.get("latest_date") else "UNKNOWN")
    return {
        "source": spec.get("source"),
        "status": status,
        "before_latest_date": before.get("latest_date"),
        "after_latest_date": after.get("latest_date"),
        "required_through": requested_as_of.isoformat(),
        "freshness_after_refresh": freshness,
        "before_row_count": before.get("row_count", 0),
        "after_row_count": after.get("row_count", 0),
        "cache_path": spec.get("cache_path"),
        "required": spec.get("required") is not False,
        "error": error,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_source_refresh_status(
    source_results: Sequence[Mapping[str, Any]],
    execute_refresh: bool,
) -> str:
    if not execute_refresh:
        return "DRY_RUN_ONLY"
    required_results = [row for row in source_results if row.get("required") is not False]
    considered = required_results or list(source_results)
    ready_count = sum(1 for row in considered if row.get("freshness_after_refresh") == "READY")
    if ready_count == len(considered) and considered:
        return "COMPLETED"
    if ready_count:
        return "PARTIAL"
    return "FAILED"


def _smoothed_source_refresh_audit(
    *,
    refresh_execution_id: str,
    before_states: Mapping[str, Mapping[str, Any]],
    after_states: Mapping[str, Mapping[str, Any]],
    source_results: Sequence[Mapping[str, Any]],
    external_refresh_executed: bool,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    touched: list[str] = []
    for row in source_results:
        source = _text(row.get("source"))
        before = _mapping(before_states.get(source))
        after = _mapping(after_states.get(source))
        changed = before.get("checksum") != after.get("checksum")
        if changed:
            touched.append(Path(_text(after.get("cache_path"))).name)
        entries.append(
            {
                "source": source,
                "cache_path": after.get("cache_path"),
                "before_row_count": before.get("row_count", 0),
                "after_row_count": after.get("row_count", 0),
                "before_latest_date": before.get("latest_date"),
                "after_latest_date": after.get("latest_date"),
                "checksum_before": before.get("checksum"),
                "checksum_after": after.get("checksum"),
                "status": "PASS" if row.get("status") != "FAILED" else "FAIL",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "audit_id": _stable_id("smoothed-source-refresh-audit", refresh_execution_id, entries),
        "refresh_execution_id": refresh_execution_id,
        "cache_files_touched": touched,
        "audit_entries": entries,
        "external_refresh_executed": external_refresh_executed,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_post_refresh_decision(
    *,
    post_refresh_id: str,
    requested_as_of: date,
    data_validation: Mapping[str, Any],
    preflight_result: Mapping[str, Any],
) -> dict[str, Any]:
    freshness_status = _text(preflight_result.get("freshness_status"))
    validate_status = _text(data_validation.get("validate_data_status"))
    if validate_status == "FAIL" and freshness_status == "BLOCKED_DATA_QUALITY_FAIL":
        decision = "MANUAL_REVIEW_REQUIRED"
        reason = "validate_data_failed"
    elif preflight_result.get("can_run_full_retry") is True:
        decision = "RETRY_READY"
        reason = "all_required_sources_fresh"
    elif freshness_status == "LATEST_AVAILABLE_ONLY":
        decision = "PARTIAL_RETRY_ONLY"
        reason = "requested_as_of_after_latest_valid_as_of"
    else:
        decision = "STILL_BLOCKED"
        reason = "blocking_errors_remain"
    return {
        "schema_version": SCHEMA_VERSION,
        "post_refresh_id": post_refresh_id,
        "retry_decision": decision,
        "reason": reason,
        "allowed_next_commands": (
            [
                "aits etf dynamic-v3-rescue smoothed-bootstrap-retry run "
                f"--requested-as-of {requested_as_of.isoformat()}"
            ]
            if decision == "RETRY_READY"
            else []
        ),
        "blocked_commands": (
            []
            if decision == "RETRY_READY"
            else [
                "aits etf dynamic-v3-rescue smoothed-retry-resume run "
                f"--post-refresh-id {post_refresh_id}"
            ]
        ),
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _latest_smoothed_progress_counts(progress_dir: Path) -> dict[str, int]:
    latest = _latest_child_dir_with(progress_dir, "smoothed_forward_progress_summary.json")
    if latest is None:
        return {"forward": 0, "sideways": 0, "recovery": 0}
    summary = _read_optional_json(latest / "smoothed_forward_progress_summary.json") or {}
    return {
        "forward": int(_float(summary.get("available_forward_events_total"))),
        "sideways": int(_float(summary.get("available_sideways_events"))),
        "recovery": int(_float(summary.get("available_recovery_events"))),
    }


def _smoothed_retry_resume_steps(
    *,
    can_resume: bool,
    retry: Mapping[str, Any] | None,
) -> dict[str, Any]:
    retry_summary = _mapping(_mapping(retry or {}).get("retry_summary"))
    retry_status = _text(retry_summary.get("retry_status"))
    artifacts = _mapping(_mapping(retry or {}).get("retry_artifacts")).get("artifacts", {})
    artifact_map = _mapping(artifacts)
    if not can_resume:
        steps = [
            {"step": "smoothed_bootstrap_retry", "status": "BLOCKED", "artifact_id": None},
        ]
        steps.extend(
            {
                "step": name,
                "status": "SKIPPED",
                "artifact_id": None,
                "reason": "post_refresh_not_retry_ready",
            }
            for name in (
                "smoothed_forward_progress_update",
                "smoothed_weekly_dashboard",
                "smoothed_event_monitor",
                "smoothed_switch_readiness",
                "smoothed_owner_renewal",
            )
        )
    else:
        step_status = "PASS" if retry_status == "COMPLETED" else retry_status or "FAIL"
        steps = [
            {
                "step": "smoothed_bootstrap_retry",
                "status": step_status,
                "artifact_id": _mapping(retry or {}).get("retry_id"),
            },
            {
                "step": "smoothed_forward_progress_update",
                "status": "PASS" if retry_status == "COMPLETED" else "SKIPPED",
                "artifact_id": _mapping(artifact_map.get("progress_update")).get("artifact_id"),
            },
            {
                "step": "smoothed_weekly_dashboard",
                "status": "PASS" if retry_status == "COMPLETED" else "SKIPPED",
                "artifact_id": _mapping(artifact_map.get("weekly_dashboard")).get("artifact_id"),
            },
            {
                "step": "smoothed_event_monitor",
                "status": "PASS" if retry_status == "COMPLETED" else "SKIPPED",
                "artifact_id": _mapping(artifact_map.get("event_monitor")).get("artifact_id"),
            },
            {
                "step": "smoothed_switch_readiness",
                "status": "PASS" if retry_status == "COMPLETED" else "SKIPPED",
                "artifact_id": _mapping(artifact_map.get("switch_readiness")).get("artifact_id"),
            },
            {
                "step": "smoothed_owner_renewal",
                "status": "PASS" if retry_status == "COMPLETED" else "SKIPPED",
                "artifact_id": _mapping(artifact_map.get("owner_renewal")).get("artifact_id"),
            },
        ]
    return {"schema_version": SCHEMA_VERSION, "steps": steps, **SYSTEM_TARGET_SAFETY}


def _smoothed_retry_resume_artifacts(retry: Mapping[str, Any] | None) -> dict[str, Any]:
    if retry is None:
        return {"schema_version": SCHEMA_VERSION, "artifacts": {}, **SYSTEM_TARGET_SAFETY}
    return {
        "schema_version": SCHEMA_VERSION,
        "bootstrap_retry_id": retry.get("retry_id"),
        "artifacts": _mapping(_mapping(retry.get("retry_artifacts")).get("artifacts")),
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_retry_resume_summary(
    *,
    resume_id: str,
    requested_as_of: date,
    can_resume: bool,
    retry: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not can_resume or retry is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "resume_id": resume_id,
            "resume_status": "BLOCKED",
            "requested_as_of": requested_as_of.isoformat(),
            "emitted_events": 0,
            "due_windows": 0,
            "updated_windows": 0,
            "classified_events": 0,
            "available_forward_events_after_resume": 0,
            "available_sideways_events_after_resume": 0,
            "available_recovery_events_after_resume": 0,
            "can_execute_switch": False,
            "weekly_recommendation": "continue_observation",
            "broker_action_allowed": False,
            "production_effect": "none",
            **SYSTEM_TARGET_SAFETY,
        }
    retry_summary = _mapping(retry.get("retry_summary"))
    return {
        "schema_version": SCHEMA_VERSION,
        "resume_id": resume_id,
        "resume_status": retry_summary.get("retry_status", "FAIL"),
        "requested_as_of": requested_as_of.isoformat(),
        "emitted_events": int(_float(retry_summary.get("emitted_events"))),
        "due_windows": int(_float(retry_summary.get("due_windows"))),
        "updated_windows": int(_float(retry_summary.get("updated_windows"))),
        "classified_events": int(_float(retry_summary.get("classified_events"))),
        "available_forward_events_after_resume": int(
            _float(retry_summary.get("available_forward_events_after_retry"))
        ),
        "available_sideways_events_after_resume": int(
            _float(retry_summary.get("available_sideways_events_after_retry"))
        ),
        "available_recovery_events_after_resume": int(
            _float(retry_summary.get("available_recovery_events_after_retry"))
        ),
        "can_execute_switch": False,
        "weekly_recommendation": "continue_observation",
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_sample_growth_summary(
    *,
    growth_id: str,
    resume_id: str,
    precondition: Mapping[str, Any],
    resume_summary: Mapping[str, Any],
) -> dict[str, Any]:
    before = {
        "available_forward_events": int(
            _float(precondition.get("available_forward_events_before_resume"))
        ),
        "available_sideways_events": int(
            _float(precondition.get("available_sideways_events_before_resume"))
        ),
        "available_recovery_events": int(
            _float(precondition.get("available_recovery_events_before_resume"))
        ),
    }
    after = {
        "available_forward_events": int(
            _float(resume_summary.get("available_forward_events_after_resume"))
        ),
        "available_sideways_events": int(
            _float(resume_summary.get("available_sideways_events_after_resume"))
        ),
        "available_recovery_events": int(
            _float(resume_summary.get("available_recovery_events_after_resume"))
        ),
    }
    delta = {
        "forward_events": after["available_forward_events"] - before["available_forward_events"],
        "sideways_events": after["available_sideways_events"] - before["available_sideways_events"],
        "recovery_events": after["available_recovery_events"] - before["available_recovery_events"],
    }
    if any(value > 0 for value in delta.values()):
        status = "IMPROVED"
    elif any(after.values()):
        status = "NO_CHANGE"
    else:
        status = "INSUFFICIENT_DATA"
    return {
        "schema_version": SCHEMA_VERSION,
        "growth_id": growth_id,
        "resume_id": resume_id,
        "before": before,
        "after": after,
        "delta": delta,
        "progress": {
            "forward": (
                f"{after['available_forward_events']}/"
                f"{SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS}"
            ),
            "sideways": (
                f"{after['available_sideways_events']}/"
                f"{SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS}"
            ),
            "recovery": (
                f"{after['available_recovery_events']}/"
                f"{SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS}"
            ),
        },
        "growth_status": status,
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_sample_growth_by_target(summary: Mapping[str, Any]) -> dict[str, Any]:
    after = _mapping(summary.get("after"))
    return {
        "schema_version": SCHEMA_VERSION,
        "targets": [
            _sample_growth_target_row(
                target_id="smooth_3d_vs_limited",
                before_available=_mapping(summary.get("before")).get("available_forward_events", 0),
                after_available=after.get("available_forward_events", 0),
                required=SMOOTHED_CONFIRMATION_REQUIRED_FORWARD_EVENTS,
                empty_status="INSUFFICIENT_EVENTS",
            ),
            _sample_growth_target_row(
                target_id="smooth_3d_sideways_choppy_improvement",
                before_available=_mapping(summary.get("before")).get(
                    "available_sideways_events", 0
                ),
                after_available=after.get("available_sideways_events", 0),
                required=SMOOTHED_CONFIRMATION_REQUIRED_SIDEWAYS_EVENTS,
                empty_status="INSUFFICIENT_EVENTS",
            ),
            _sample_growth_target_row(
                target_id="smooth_3d_recovery_lag_watch",
                before_available=_mapping(summary.get("before")).get(
                    "available_recovery_events", 0
                ),
                after_available=after.get("available_recovery_events", 0),
                required=SMOOTHED_CONFIRMATION_REQUIRED_RECOVERY_EVENTS,
                empty_status="WATCH_ONLY",
            ),
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _sample_growth_target_row(
    *,
    target_id: str,
    before_available: object,
    after_available: object,
    required: int,
    empty_status: str,
) -> dict[str, Any]:
    before_int = int(_float(before_available))
    after_int = int(_float(after_available))
    return {
        "target_id": target_id,
        "before_available": before_int,
        "after_available": after_int,
        "required": required,
        "progress_pct": round(after_int / required, 4) if required else 0.0,
        "status": (
            "READY_FOR_REVIEW"
            if after_int >= required
            else ("IN_PROGRESS" if after_int > 0 else empty_status)
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_sample_growth_delta_consistent(summary: Mapping[str, Any]) -> bool:
    before = _mapping(summary.get("before"))
    after = _mapping(summary.get("after"))
    delta = _mapping(summary.get("delta"))
    return (
        int(_float(delta.get("forward_events")))
        == int(_float(after.get("available_forward_events")))
        - int(_float(before.get("available_forward_events")))
        and int(_float(delta.get("sideways_events")))
        == int(_float(after.get("available_sideways_events")))
        - int(_float(before.get("available_sideways_events")))
        and int(_float(delta.get("recovery_events")))
        == int(_float(after.get("available_recovery_events")))
        - int(_float(before.get("available_recovery_events")))
    )


def _smoothed_data_readiness_summary(
    *,
    readiness_id: str,
    refresh: Mapping[str, Any],
    post_refresh: Mapping[str, Any],
    resume: Mapping[str, Any],
    growth: Mapping[str, Any],
) -> dict[str, Any]:
    refresh_results = _mapping(refresh.get("source_refresh_results"))
    post_decision = _mapping(post_refresh.get("post_refresh_decision"))
    resume_summary = _mapping(resume.get("resume_summary"))
    growth_summary = _mapping(growth.get("sample_growth_summary"))
    requested_as_of = _text(
        post_refresh.get("requested_as_of"),
        _text(refresh.get("requested_as_of"), _text(resume_summary.get("requested_as_of"))),
    )
    retry_status = _text(resume_summary.get("resume_status"), "NOT_RUN")
    growth_status = _text(growth_summary.get("growth_status"), "INSUFFICIENT_DATA")
    current_status = _smoothed_data_readiness_status(
        refresh_status=_text(refresh_results.get("refresh_status")),
        retry_decision=_text(post_decision.get("retry_decision")),
        retry_status=retry_status,
        growth_status=growth_status,
        can_execute_switch=resume_summary.get("can_execute_switch") is True,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "readiness_id": readiness_id,
        "current_status": current_status,
        "requested_as_of": requested_as_of,
        "sources_status": _smoothed_readiness_source_status(refresh_results),
        "retry_status": retry_status,
        "sample_growth_status": growth_status,
        "forward_progress": _mapping(growth_summary.get("progress")).get("forward", "0/10"),
        "sideways_progress": _mapping(growth_summary.get("progress")).get("sideways", "0/5"),
        "recovery_progress": _mapping(growth_summary.get("progress")).get("recovery", "0/5"),
        "recommended_owner_action": _smoothed_recommended_owner_action(current_status),
        "broker_action_allowed": False,
        "production_effect": "none",
        **SYSTEM_TARGET_SAFETY,
    }


def _smoothed_data_readiness_status(
    *,
    refresh_status: str,
    retry_decision: str,
    retry_status: str,
    growth_status: str,
    can_execute_switch: bool,
) -> str:
    if refresh_status == "DRY_RUN_ONLY":
        return "REFRESH_REQUIRED"
    if retry_decision in {"STILL_BLOCKED", "MANUAL_REVIEW_REQUIRED"}:
        return "RETRY_BLOCKED"
    if retry_decision == "RETRY_READY" and retry_status in {"NOT_RUN", "BLOCKED"}:
        return "RETRY_READY"
    if retry_status == "COMPLETED" and not can_execute_switch:
        return "CONTINUE_OBSERVATION"
    if retry_status == "COMPLETED":
        return "RETRY_COMPLETED"
    if refresh_status in {"COMPLETED", "PARTIAL"}:
        return "REFRESH_EXECUTED"
    if growth_status == "INSUFFICIENT_DATA":
        return "WAIT_FOR_REFRESH"
    return "WAIT_FOR_REFRESH"


def _smoothed_recommended_owner_action(current_status: str) -> str:
    if current_status in {"WAIT_FOR_REFRESH", "REFRESH_REQUIRED"}:
        return "run_refresh"
    if current_status == "RETRY_READY":
        return "rerun_retry"
    if current_status in {"RETRY_COMPLETED", "CONTINUE_OBSERVATION"}:
        return "continue_observation"
    return "manual_review_required"


def _smoothed_readiness_source_status(refresh_results: Mapping[str, Any]) -> dict[str, str]:
    statuses = {
        "prices_daily": "UNKNOWN",
        "prices_marketstack_daily": "UNKNOWN",
        "rates_daily": "UNKNOWN",
    }
    for row in _records(refresh_results.get("sources")):
        source = _text(row.get("source"))
        if row.get("freshness_after_refresh") == "READY":
            statuses[source] = "READY"
        elif row.get("status") == "FAILED":
            statuses[source] = "FAILED"
        elif row.get("freshness_after_refresh") == "STILL_STALE":
            statuses[source] = "STALE"
    return statuses


def _smoothed_issue_codes(report: DataQualityReport, *, severity: str) -> list[str]:
    codes: list[str] = []
    for issue in report.issues:
        issue_severity = getattr(issue.severity, "value", str(issue.severity))
        if issue_severity != severity:
            continue
        if issue.code not in codes:
            codes.append(issue.code)
    return codes


def _smoothed_quality_issues_payload(report: DataQualityReport) -> list[dict[str, Any]]:
    return [
        {
            "severity": getattr(issue.severity, "value", str(issue.severity)),
            "code": issue.code,
            "message": issue.message,
            "rows": issue.rows,
            "sample": issue.sample,
            "source": issue.source,
        }
        for issue in report.issues
    ]


def _smoothed_latest_model_target_as_of(model_target_dir: Path) -> date | None:
    try:
        payload = _optional_latest_model_target_payload(model_target_dir)
    except DynamicV3SystemTargetError:
        return None
    return _coerce_date(payload.get("as_of"), date.min) if payload else None


def _smoothed_download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _smoothed_marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _smoothed_requires_marketstack_prices(prices_path: Path) -> bool:
    return _smoothed_is_default_price_cache(prices_path)


def _smoothed_is_default_price_cache(path: Path) -> bool:
    try:
        return path.resolve() == DEFAULT_PRICE_CACHE_PATH.resolve()
    except OSError:
        return path == DEFAULT_PRICE_CACHE_PATH


def _smoothed_is_default_rates_cache(path: Path) -> bool:
    try:
        return path.resolve() == DEFAULT_RATES_CACHE_PATH.resolve()
    except OSError:
        return path == DEFAULT_RATES_CACHE_PATH


def _infer_csv_values(path: Path, column: str) -> list[str]:
    if not path.exists():
        return []
    try:
        frame = pd.read_csv(path, usecols=[column])
    except (OSError, ValueError, pd.errors.ParserError):
        return []
    return sorted({_text(value) for value in frame[column].dropna() if _text(value)})


def _date_or_none(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _primary_smoothed_comparison(comparison: Mapping[str, Any], method: str) -> dict[str, Any]:
    return _find_comparison(
        _records(_mapping(comparison.get("smoothed_vs_limited_metrics")).get("comparisons")),
        method,
        "limited_adjustment",
    )


def _method_rolling_row(comparison: Mapping[str, Any], method: str) -> dict[str, Any]:
    return _find_method(
        _records(_mapping(comparison.get("smoothed_rolling_comparison")).get("methods")),
        method,
    )


def _method_stability_row(comparison: Mapping[str, Any], method: str) -> dict[str, Any]:
    return _find_method(
        _records(_mapping(comparison.get("smoothed_stability_comparison")).get("methods")),
        method,
    )


def _method_lag_row(comparison: Mapping[str, Any], method: str) -> dict[str, Any]:
    return _find_method(
        _records(_mapping(comparison.get("smoothing_lag_cost_analysis")).get("methods")),
        method,
    )


def _sideways_regime_row(comparison: Mapping[str, Any]) -> dict[str, Any]:
    for row in _records(_mapping(comparison.get("smoothed_regime_comparison")).get("regimes")):
        if row.get("regime") == "sideways_choppy":
            return row
    return {}


def _smoothed_regime_prefix(method: str) -> str:
    return "smooth_5d" if method == "smooth_weights_5d_limited_adjustment" else "smooth_3d"


def _status_upper(value: str) -> str:
    normalized = value.strip().upper()
    if normalized in {"IMPROVED", "WORSE", "MIXED", "INSUFFICIENT_DATA"}:
        return normalized
    return "INSUFFICIENT_DATA"


def _smoothed_benefit_status(
    weight_jump_reduction: float,
    turnover_reduction: float,
    signal_churn_reduction: float,
    rolling_delta: str,
) -> str:
    if rolling_delta == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    positive_count = sum(
        value > 0 for value in (weight_jump_reduction, turnover_reduction, signal_churn_reduction)
    )
    if rolling_delta == "IMPROVED" and positive_count >= 2:
        return "STRONG"
    if rolling_delta != "WORSE" and positive_count >= 1:
        return "MODERATE"
    if rolling_delta == "IMPROVED" and all(
        value >= 0 for value in (weight_jump_reduction, turnover_reduction, signal_churn_reduction)
    ):
        return "MODERATE"
    return "WEAK"


def _smoothed_tradeoff_status(benefit_status: str, lag_status: str) -> str:
    if "INSUFFICIENT_DATA" in {benefit_status, lag_status}:
        return "INSUFFICIENT_DATA"
    if lag_status == "HIGH" or benefit_status == "WEAK":
        return "UNFAVORABLE"
    if benefit_status in {"STRONG", "MODERATE"} and lag_status == "LOW":
        return "FAVORABLE"
    return "MIXED"


def _smoothed_tradeoff_recommendation(tradeoff_status: str) -> str:
    if tradeoff_status == "FAVORABLE":
        return "needs_forward_confirmation"
    if tradeoff_status == "UNFAVORABLE":
        return "reject"
    if tradeoff_status == "INSUFFICIENT_DATA":
        return "needs_forward_confirmation"
    return "continue_observation"


def _method_rows_for_dates(
    rows: Sequence[Mapping[str, Any]],
    method: str,
    dates: set[str],
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if row.get("target_method") == method and _text(row.get("date")) in dates
    ]


def _return_drawdown_delta(
    method_rows: Sequence[Mapping[str, Any]],
    limited_rows: Sequence[Mapping[str, Any]],
) -> tuple[float, float]:
    method_metrics = _sample_return_metrics(method_rows, min_sample=1)
    limited_metrics = _sample_return_metrics(limited_rows, min_sample=1)
    return (
        round(
            _float(method_metrics.get("total_return"))
            - _float(limited_metrics.get("total_return")),
            10,
        ),
        round(
            _float(method_metrics.get("max_drawdown"))
            - _float(limited_metrics.get("max_drawdown")),
            10,
        ),
    )


def _smoothed_sideways_status(
    sample_count: int,
    return_delta: float,
    drawdown_delta: float,
    turnover_delta: float,
    signal_churn_delta: float,
    weight_jump_delta: float,
) -> str:
    if sample_count <= 0:
        return "INSUFFICIENT_DATA"
    if (
        return_delta >= SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR
        and drawdown_delta >= 0
        and turnover_delta <= 0
        and signal_churn_delta <= 0
        and weight_jump_delta <= 0
    ):
        return "IMPROVED"
    if return_delta < SMOOTHED_ACCEPTABLE_RETURN_DELTA_FLOOR and drawdown_delta < 0:
        return "WORSE"
    return "MIXED"


def _risk_on_delay_proxy_days(
    ledger: Sequence[Mapping[str, Any]],
    method: str,
    recovery_dates: set[str],
) -> float:
    delayed = 0
    for row in ledger:
        if row.get("target_method") != method or _text(row.get("date")) not in recovery_dates:
            continue
        delayed += sum(
            1
            for event in _records(row.get("lag_events"))
            if event.get("lag_risk") in {"MEDIUM", "HIGH"}
        )
    return float(delayed)


def _smoothed_watch_recommended_action(
    decision: str,
    tradeoff_status: str,
    recovery_lag_status: str,
    forward_status: str,
) -> str:
    if decision == "REJECT":
        return "reject"
    if recovery_lag_status == "HIGH" or tradeoff_status == "UNFAVORABLE":
        return "owner_review_required"
    if decision == "PROMOTE_TO_RECOMMENDED_RESEARCH" and forward_status == "IN_PROGRESS":
        return "promote_for_review"
    return "continue_observation"


def _delta_improvement_status(value: float, *, lower_is_better: bool) -> str:
    if value == 0:
        return "MIXED"
    if lower_is_better:
        return "IMPROVED" if value < 0 else "WORSE"
    return "IMPROVED" if value > 0 else "WORSE"


def _win_rate_between_rows(
    risk_rows: Sequence[Mapping[str, Any]],
    limited_rows: Sequence[Mapping[str, Any]],
) -> float:
    limited_by_date = {
        str(row.get("date")): _float(row.get("daily_return")) for row in limited_rows
    }
    paired = [
        _float(row.get("daily_return")) > limited_by_date[str(row.get("date"))]
        for row in risk_rows
        if str(row.get("date")) in limited_by_date
    ]
    return round(sum(1 for item in paired if item) / len(paired), 10) if paired else 0.0


def _large_jump_count(rows: Sequence[Mapping[str, Any]]) -> int:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    previous: Mapping[str, Any] | None = None
    count = 0
    for row in ordered:
        if previous is not None:
            deltas = _weight_deltas(
                _mapping(previous.get("weights")),
                _mapping(row.get("weights")),
            )
            if sum(abs(value) for value in deltas.values()) >= INSTABILITY_WEIGHT_JUMP_THRESHOLD:
                count += 1
        previous = row
    return count


def _semiconductor_weight(weights: Mapping[str, Any], symbols: Sequence[str]) -> float:
    return sum(_float(weights.get(symbol)) for symbol in symbols)


def _assert_experiment_factory_config_safe(payload: Mapping[str, Any]) -> None:
    if not _experiment_safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("experiment factory config safety boundary is not locked")


def _experiment_safety_config_locked(safety: Mapping[str, Any]) -> bool:
    return (
        safety.get("experiment_only") is True
        and safety.get("research_screening_only") is True
        and safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken", False) is False
        and safety.get("order_ticket_generated", False) is False
        and safety.get("production_effect") == PRODUCTION_EFFECT
        and safety.get("auto_apply", False) is False
    )


def _payload_experiment_safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("experiment_only", True) is True
        and payload.get("research_screening_only", True) is True
        and payload.get("not_formal_research_method", True) is True
        and payload.get("not_official_target_weights", True) is True
        and payload.get("broker_action_allowed") is not True
        and payload.get("broker_action_taken") is not True
        and payload.get("order_ticket_generated") is not True
        and payload.get("auto_apply") is not True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        for payload in payloads
        if payload
    )


def _normalized_failure_modes(config: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    raw = {str(row.get("id")): row for row in _records(config.get("failure_modes"))}
    for mode in DEFAULT_FAILURE_MODES:
        source = _mapping(raw.get(mode))
        rows.append(
            {
                "id": mode,
                "description": _text(
                    source.get("description"),
                    f"{mode} requires explicit hypothesis coverage.",
                ),
                "severity": _text(source.get("severity"), "REVIEW_REQUIRED"),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    for mode, source in sorted(raw.items()):
        if mode in DEFAULT_FAILURE_MODES:
            continue
        rows.append(
            {
                "id": mode,
                "description": _text(source.get("description")),
                "severity": _text(source.get("severity"), "REVIEW_REQUIRED"),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "failure_modes": rows, **EXPERIMENT_FACTORY_SAFETY}


def _normalized_hypotheses(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in _records(config.get("hypotheses")):
        rows.append(
            {
                "hypothesis_id": _text(row.get("hypothesis_id")),
                "family": _text(row.get("family")),
                "base_method": _text(row.get("base_method"), "limited_adjustment"),
                "description": _text(row.get("description")),
                "target_failure_modes": _texts(row.get("target_failure_modes")),
                "expected_benefit": _texts(row.get("expected_benefit")),
                "expected_cost": _texts(row.get("expected_cost")),
                "complexity": _text(row.get("complexity"), "MEDIUM"),
                "priority": _text(row.get("priority"), "MEDIUM"),
                "status": _text(row.get("status"), "proposed"),
                "promotion_eligible": False,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _hypothesis_priority_summary(
    taxonomy: Mapping[str, Any],
    hypotheses: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    modes = [str(row.get("id")) for row in _records(taxonomy.get("failure_modes"))]
    covered = {mode for row in hypotheses for mode in _texts(row.get("target_failure_modes"))}
    high = [
        _text(row.get("hypothesis_id"))
        for row in hypotheses
        if _text(row.get("priority")).upper() == "HIGH"
    ]
    family_counts: dict[str, int] = {}
    for row in hypotheses:
        family = _text(row.get("family"), "UNKNOWN")
        family_counts[family] = family_counts.get(family, 0) + 1
    recommended = [
        _text(row.get("hypothesis_id"))
        for row in hypotheses
        if _text(row.get("priority")).upper() == "HIGH"
        and _text(row.get("status"), "proposed") in {"proposed", "ready_for_matrix"}
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "failure_modes_total": len(modes),
        "hypotheses_total": len(hypotheses),
        "high_priority_hypotheses": high,
        "hypothesis_family_counts": family_counts,
        "uncovered_failure_modes": [mode for mode in modes if mode not in covered],
        "recommended_for_experiment_matrix": recommended[:10],
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _normalized_variant_transform_spec(config: Mapping[str, Any]) -> dict[str, Any]:
    transform_types = {}
    for transform_type in DEFAULT_TRANSFORM_TYPES:
        spec = _mapping(_mapping(config.get("transform_types")).get(transform_type))
        transform_types[transform_type] = {
            "description": _text(
                spec.get("description"),
                f"{transform_type} lightweight experiment transform.",
            ),
            "required_fields": _texts(spec.get("required_fields")),
            "allowed_modes": _texts(spec.get("allowed_modes")),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "transform_types": transform_types,
        "safety": dict(_mapping(config.get("safety"))),
    }


def _transform_type_catalog(spec: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for transform_type, payload in sorted(_mapping(spec.get("transform_types")).items()):
        data = _mapping(payload)
        rows.append(
            {
                "type": transform_type,
                "description": _text(data.get("description")),
                "required_fields": _texts(data.get("required_fields")),
                "allowed_modes": _texts(data.get("allowed_modes")),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "transform_types": rows, **EXPERIMENT_FACTORY_SAFETY}


def _normalized_variant_specs(
    config: Mapping[str, Any],
    hypothesis_lookup: Mapping[str, Mapping[str, Any]],
    transform_spec: Mapping[str, Any],
) -> list[dict[str, Any]]:
    group = _mapping(config.get("experiment_group"))
    catalog = _mapping(transform_spec.get("transform_types"))
    rows = []
    for row in _records(config.get("variants")):
        hypothesis_id = _text(row.get("hypothesis_id"))
        hypothesis = _mapping(hypothesis_lookup.get(hypothesis_id))
        target_modes = _texts(row.get("target_failure_modes")) or _texts(
            hypothesis.get("target_failure_modes")
        )
        transforms = []
        for transform in _records(row.get("transforms")):
            transform_type = _text(transform.get("type"))
            required = _texts(_mapping(catalog.get(transform_type)).get("required_fields"))
            missing = [field for field in required if field not in transform]
            payload = {**dict(transform), "missing_required_fields": missing}
            transforms.append(payload)
        rows.append(
            {
                "variant_id": _text(row.get("variant_id")),
                "base_method": _text(row.get("base_method"), _text(group.get("base_method"))),
                "hypothesis_id": hypothesis_id,
                "family": _text(row.get("family"), _text(hypothesis.get("family"), "UNKNOWN")),
                "transforms": transforms,
                "target_failure_modes": target_modes,
                "expected_benefit": _texts(row.get("expected_benefit"))
                or _texts(hypothesis.get("expected_benefit")),
                "expected_cost": _texts(row.get("expected_cost"))
                or _texts(hypothesis.get("expected_cost")),
                "experiment_only": True,
                "research_screening_only": True,
                "not_formal_research_method": True,
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": PRODUCTION_EFFECT,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _matrix_transform_specs(
    transform_spec: Mapping[str, Any],
    variant_specs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    used = {
        _text(transform.get("type"))
        for row in variant_specs
        for transform in _records(row.get("transforms"))
    }
    catalog = _transform_type_catalog(_normalized_variant_transform_spec(transform_spec))
    return {
        "schema_version": SCHEMA_VERSION,
        "transform_types": [
            row for row in _records(catalog.get("transform_types")) if row.get("type") in used
        ],
        "used_transform_types": sorted(used),
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _experiment_matrix_summary(variant_specs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    families = sorted({_text(row.get("family")) for row in variant_specs if row.get("family")})
    failure_modes = sorted(
        {mode for row in variant_specs for mode in _texts(row.get("target_failure_modes"))}
    )
    type_map: dict[str, list[str]] = {}
    for row in variant_specs:
        for transform in _records(row.get("transforms")):
            transform_type = _text(transform.get("type"))
            type_map.setdefault(transform_type, []).append(_text(row.get("variant_id")))
    return {
        "variant_count": len(variant_specs),
        "families_covered": families,
        "failure_modes_covered": failure_modes,
        "variants_by_transform_type": type_map,
        "formal_method_variant_count": sum(
            1 for row in variant_specs if row.get("not_formal_research_method") is not True
        ),
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _symbols_from_state_paths(states: Sequence[Mapping[str, Any]]) -> list[str]:
    symbols = sorted(
        {symbol for row in states for symbol in _mapping(row.get("weights")) if symbol != "CASH"}
    )
    if not symbols:
        raise DynamicV3SystemTargetError("state paths do not contain priced symbols")
    return symbols


def _run_variant_weight_path(
    *,
    variant: Mapping[str, Any],
    baseline_states: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
    labels: Mapping[str, str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    variant_id = _text(variant.get("variant_id"))
    base_by_date = {
        str(row.get("date")): row
        for row in baseline_states
        if row.get("target_method") == "limited_adjustment"
    }
    rows_by_date_method: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in baseline_states:
        rows_by_date_method.setdefault(str(row.get("date")), {})[
            str(row.get("target_method"))
        ] = row
    initial = _mapping(next(iter(base_by_date.values()), {}).get("weights"))
    current_weights = _normalize_weights(initial or _backfill_initial_weights(config))
    portfolio_value = 1.0
    peak_value = 1.0
    transform_state: dict[str, Any] = {
        "target_history": [],
        "last_target": current_weights,
        "cooldown_until": {},
        "persistence_count": 0,
    }
    result = []
    for timestamp, return_row in returns.iterrows():
        current_date = timestamp.date()
        date_text = current_date.isoformat()
        base_row = _mapping(base_by_date.get(date_text))
        before_return = _normalize_weights(current_weights)
        daily_return = _portfolio_return(before_return, return_row)
        portfolio_value *= 1.0 + daily_return
        drifted = _drift_weights(before_return, return_row, daily_return)
        after_weights = dict(drifted)
        turnover = 0.0
        rebalance_event = False
        if base_row.get("rebalance_event") is True:
            target = _variant_source_target(
                variant=variant,
                date_text=date_text,
                rows_by_date_method=rows_by_date_method,
                fallback=_mapping(base_row.get("weights")),
            )
            after_weights = _apply_experiment_transforms(
                as_of=current_date,
                target=target,
                previous=drifted,
                variant=variant,
                regime=_text(labels.get(date_text), "sideways_choppy"),
                transform_state=transform_state,
            )
            turnover = _turnover(drifted, after_weights)
            rebalance_event = turnover > 0.0
        peak_value = max(peak_value, portfolio_value)
        drawdown = portfolio_value / peak_value - 1.0 if peak_value > 0 else 0.0
        current_weights = after_weights
        result.append(
            {
                "date": date_text,
                "variant_id": variant_id,
                "base_method": variant.get("base_method"),
                "target_method": variant_id,
                "regime": _text(labels.get(date_text), "sideways_choppy"),
                "weights": after_weights,
                "portfolio_value": round(portfolio_value, 10),
                "daily_return": round(daily_return, 10),
                "drawdown": round(drawdown, 10),
                "turnover": round(turnover, 10),
                "rebalance_event": rebalance_event,
                "target_failure_modes": _texts(variant.get("target_failure_modes")),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return result


def _variant_source_target(
    *,
    variant: Mapping[str, Any],
    date_text: str,
    rows_by_date_method: Mapping[str, Mapping[str, Mapping[str, Any]]],
    fallback: Mapping[str, Any],
) -> dict[str, float]:
    transforms = _records(variant.get("transforms"))
    has_candidate_aggregation = any(
        _text(transform.get("type")) in {"candidate_subset", "consensus_aggregation"}
        for transform in transforms
    )
    if has_candidate_aggregation:
        method_rows = rows_by_date_method.get(date_text, {})
        method_weights = [
            _mapping(row.get("weights"))
            for method, row in method_rows.items()
            if method
            in {
                "limited_adjustment",
                "defensive_limited_adjustment",
                "consensus_target",
                "equal_weight_shadow_candidates",
                "selected_top_candidate",
            }
        ]
        method = _text(
            next(
                (
                    transform.get("method")
                    for transform in transforms
                    if _text(transform.get("type")) == "consensus_aggregation"
                ),
                "weighted_mean",
            )
        )
        return _aggregate_weights(method_weights, method=method)
    return _normalize_weights(fallback)


def _aggregate_weights(
    weights_list: Sequence[Mapping[str, Any]],
    *,
    method: str,
) -> dict[str, float]:
    clean = [_normalize_weights(weights) for weights in weights_list if weights]
    if not clean:
        raise DynamicV3SystemTargetError("candidate aggregation has no weight sources")
    symbols = sorted({symbol for weights in clean for symbol in weights})
    if method == "median":
        result = {
            symbol: sorted(_float(weights.get(symbol)) for weights in clean)[len(clean) // 2]
            for symbol in symbols
        }
    elif method == "trimmed_mean" and len(clean) >= 3:
        result = {}
        for symbol in symbols:
            values = sorted(_float(weights.get(symbol)) for weights in clean)
            trimmed = values[1:-1] or values
            result[symbol] = sum(trimmed) / len(trimmed)
    else:
        result = {
            symbol: sum(_float(weights.get(symbol)) for weights in clean) / len(clean)
            for symbol in symbols
        }
    return _normalize_weights(result)


def _apply_experiment_transforms(
    *,
    as_of: date,
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    variant: Mapping[str, Any],
    regime: str,
    transform_state: dict[str, Any],
) -> dict[str, float]:
    current = _normalize_weights(target)
    previous_weights = _normalize_weights(previous)
    for transform in _records(variant.get("transforms")):
        transform_type = _text(transform.get("type"))
        if transform_type == "hold_previous_weights":
            current = dict(previous_weights)
        elif transform_type == "regime_gate" and regime == _text(transform.get("regime")):
            current = _apply_regime_gate(current, previous_weights, transform)
        elif transform_type == "regime_cooldown" and regime == _text(transform.get("regime")):
            current = _apply_regime_cooldown(
                as_of,
                current,
                previous_weights,
                transform,
                transform_state,
            )
        elif transform_type == "weight_smoothing":
            current = _apply_weight_smoothing(current, transform, transform_state)
            if "alpha" in transform:
                current = _blend_weights(
                    previous_weights,
                    current,
                    _float(transform.get("alpha"), 1.0),
                )
            if "max_daily_total_weight_change" in transform:
                current = _apply_turnover_cap(
                    current,
                    previous_weights,
                    _float(transform.get("max_daily_total_weight_change"), 1.0) / 2.0,
                )
        elif transform_type == "signal_persistence":
            current = _apply_signal_persistence(
                current,
                previous_weights,
                transform,
                transform_state,
            )
        elif transform_type == "rebalance_threshold":
            threshold = _float(transform.get("min_total_abs_delta"))
            total_delta = sum(
                abs(value) for value in _weight_deltas(previous_weights, current).values()
            )
            if total_delta < threshold:
                current = dict(previous_weights)
        elif transform_type == "turnover_cap":
            current = _apply_turnover_cap(
                current,
                previous_weights,
                _float(transform.get("max_turnover")),
            )
        elif transform_type == "cap_group_weight":
            current = _cap_group_weight(
                current,
                group=_text(transform.get("group")),
                max_weight=_float(transform.get("max_weight")),
            )
        elif transform_type == "cap_symbol_weight":
            current = _cap_symbol_weight(
                current,
                symbol=_text(transform.get("symbol")),
                max_weight=_float(transform.get("max_weight")),
            )
        elif transform_type == "min_cash_weight":
            current = _apply_min_cash(current, _float(transform.get("min_cash_weight")))
    transform_state["last_target"] = current
    return _normalize_weights(current)


def _apply_regime_gate(
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    transform: Mapping[str, Any],
) -> dict[str, float]:
    action = _text(transform.get("action"))
    if action == "hold_previous_weights":
        return _normalize_weights(previous)
    if action == "reduce_active_tilt":
        multiplier = _float(transform.get("multiplier"), 0.5)
        return _blend_weights(previous, target, multiplier)
    if action in {"block_risk_asset_increase", "only_allow_risk_reduction"}:
        return _block_risk_increase(target, previous)
    if action == "block_symbol_increase":
        symbol = _text(transform.get("symbol"), "SMH")
        return _block_symbol_increase(target, previous, symbol)
    return _normalize_weights(target)


def _apply_regime_cooldown(
    as_of: date,
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    transform: Mapping[str, Any],
    transform_state: dict[str, Any],
) -> dict[str, float]:
    key = _text(transform.get("regime"))
    cooldown_days = int(_float(transform.get("cooldown_days"), 0))
    cooldown_until = _mapping(transform_state.get("cooldown_until"))
    until = _coerce_date(cooldown_until.get(key), date(1970, 1, 1))
    if as_of <= until:
        return _normalize_weights(previous)
    cooldown_until[key] = (as_of + timedelta(days=cooldown_days)).isoformat()
    transform_state["cooldown_until"] = cooldown_until
    return _normalize_weights(previous)


def _apply_weight_smoothing(
    target: Mapping[str, Any],
    transform: Mapping[str, Any],
    transform_state: dict[str, Any],
) -> dict[str, float]:
    window = max(1, int(_float(transform.get("window_days"), 1)))
    history = _records(transform_state.get("target_history"))
    history.append({"weights": _normalize_weights(target)})
    history = history[-window:]
    transform_state["target_history"] = history
    return _aggregate_weights(
        [_mapping(row.get("weights")) for row in history],
        method="weighted_mean",
    )


def _apply_signal_persistence(
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    transform: Mapping[str, Any],
    transform_state: dict[str, Any],
) -> dict[str, float]:
    days = max(1, int(_float(transform.get("persistence_days"), 1)))
    last_target = _normalize_weights(_mapping(transform_state.get("last_target")) or previous)
    deltas = _weight_deltas(previous, target)
    last_deltas = _weight_deltas(previous, last_target)
    same_direction = all(
        deltas.get(symbol, 0.0) == 0.0
        or last_deltas.get(symbol, 0.0) == 0.0
        or deltas.get(symbol, 0.0) * last_deltas.get(symbol, 0.0) >= 0.0
        for symbol in set(deltas) | set(last_deltas)
    )
    count = int(_float(transform_state.get("persistence_count"), 0))
    count = count + 1 if same_direction else 1
    transform_state["persistence_count"] = count
    return _normalize_weights(target if count >= days else previous)


def _apply_turnover_cap(
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    max_turnover: float,
) -> dict[str, float]:
    turnover = _turnover(previous, target)
    if max_turnover <= 0 or turnover <= max_turnover:
        return _normalize_weights(target)
    return _blend_weights(previous, target, max_turnover / turnover)


def _cap_group_weight(
    weights: Mapping[str, Any],
    *,
    group: str,
    max_weight: float,
) -> dict[str, float]:
    if group in {"semiconductor", "semiconductors"}:
        symbols = ("SMH", "SOXX")
    elif group in {"risk_assets", "risk"}:
        symbols = tuple(symbol for symbol in weights if symbol not in {"CASH", "TLT", "BND"})
    else:
        symbols = tuple(_texts(group.split(",")))
    current = _group_weight(weights, symbols)
    if max_weight <= 0 or current <= max_weight:
        return _normalize_weights(weights)
    result = dict(_normalize_weights(weights))
    excess = current - max_weight
    scale = max_weight / current if current > 0 else 1.0
    for symbol in symbols:
        result[symbol] = _float(result.get(symbol)) * scale
    result["CASH"] = _float(result.get("CASH")) + excess
    return _normalize_weights(result)


def _cap_symbol_weight(
    weights: Mapping[str, Any],
    *,
    symbol: str,
    max_weight: float,
) -> dict[str, float]:
    result = dict(_normalize_weights(weights))
    symbol = symbol.upper()
    if symbol not in result or max_weight <= 0 or result[symbol] <= max_weight:
        return result
    excess = result[symbol] - max_weight
    result[symbol] = max_weight
    result["CASH"] = _float(result.get("CASH")) + excess
    return _normalize_weights(result)


def _apply_min_cash(weights: Mapping[str, Any], min_cash: float) -> dict[str, float]:
    result = dict(_normalize_weights(weights))
    cash = _float(result.get("CASH"))
    if min_cash <= 0 or cash >= min_cash:
        return result
    non_cash = {symbol: value for symbol, value in result.items() if symbol != "CASH"}
    scale = max(0.0, 1.0 - min_cash) / sum(non_cash.values()) if non_cash else 0.0
    for symbol in non_cash:
        result[symbol] = non_cash[symbol] * scale
    result["CASH"] = min_cash
    return _normalize_weights(result)


def _block_risk_increase(
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> dict[str, float]:
    risk_symbols = [
        symbol for symbol in set(target) | set(previous) if symbol not in {"CASH", "TLT", "BND"}
    ]
    target_risk = _group_weight(target, risk_symbols)
    previous_risk = _group_weight(previous, risk_symbols)
    if target_risk <= previous_risk:
        return _normalize_weights(target)
    return _cap_group_weight(target, group="risk_assets", max_weight=previous_risk)


def _block_symbol_increase(
    target: Mapping[str, Any],
    previous: Mapping[str, Any],
    symbol: str,
) -> dict[str, float]:
    symbol = symbol.upper()
    if _float(target.get(symbol)) <= _float(previous.get(symbol)):
        return _normalize_weights(target)
    return _cap_symbol_weight(target, symbol=symbol, max_weight=_float(previous.get(symbol)))


def _blend_weights(
    previous: Mapping[str, Any],
    target: Mapping[str, Any],
    multiplier: float,
) -> dict[str, float]:
    multiplier = max(0.0, min(1.0, multiplier))
    symbols = sorted(set(previous) | set(target))
    return _normalize_weights(
        {
            symbol: _float(previous.get(symbol))
            + (_float(target.get(symbol)) - _float(previous.get(symbol))) * multiplier
            for symbol in symbols
        }
    )


def _variant_performance_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    limited = _method_path_metrics(baseline_states, "limited_adjustment")
    static = _method_path_metrics(baseline_states, "static_baseline")
    no_trade = _method_path_metrics(baseline_states, "no_trade_baseline")
    rows = []
    for variant_id in sorted({str(row.get("variant_id")) for row in variant_states}):
        states = [row for row in variant_states if row.get("variant_id") == variant_id]
        metrics = _state_path_metrics(states, min_observations=2)
        status = _performance_status(metrics, limited)
        rows.append(
            {
                "variant_id": variant_id,
                "base_method": "limited_adjustment",
                "total_return": metrics["total_return"],
                "annualized_return": metrics["annualized_return"],
                "max_drawdown": metrics["max_drawdown"],
                "realized_volatility": metrics["realized_volatility"],
                "turnover": metrics["turnover"],
                "relative_to_limited_adjustment": round(
                    _float(metrics.get("total_return")) - _float(limited.get("total_return")),
                    10,
                ),
                "relative_to_static_baseline": round(
                    _float(metrics.get("total_return")) - _float(static.get("total_return")),
                    10,
                ),
                "relative_to_no_trade_baseline": round(
                    _float(metrics.get("total_return")) - _float(no_trade.get("total_return")),
                    10,
                ),
                "drawdown_delta_vs_limited": round(
                    _float(metrics.get("max_drawdown")) - _float(limited.get("max_drawdown")),
                    10,
                ),
                "turnover_delta_vs_limited": round(
                    _float(metrics.get("turnover")) - _float(limited.get("turnover")),
                    10,
                ),
                "performance_status": status,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _performance_status(metrics: Mapping[str, Any], limited: Mapping[str, Any]) -> str:
    if metrics.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    if _float(metrics.get("total_return")) >= _float(limited.get("total_return")) and _float(
        metrics.get("max_drawdown")
    ) >= _float(limited.get("max_drawdown")):
        return "PASS"
    if _float(metrics.get("max_drawdown")) >= _float(limited.get("max_drawdown")):
        return "PASS_WITH_WARNINGS"
    return "FAIL"


def _variant_regime_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    min_sample = _config_int(config, ("regime_policy", "min_sample_count"), 5)
    rows = []
    for variant_id in sorted({str(row.get("variant_id")) for row in variant_states}):
        for regime in _configured_regimes():
            date_set = {date_text for date_text, label in labels.items() if label == regime}
            selected = [
                row
                for row in variant_states
                if row.get("variant_id") == variant_id and row.get("date") in date_set
            ]
            limited = [
                row
                for row in baseline_states
                if row.get("target_method") == "limited_adjustment" and row.get("date") in date_set
            ]
            static = [
                row
                for row in baseline_states
                if row.get("target_method") == "static_baseline" and row.get("date") in date_set
            ]
            metrics = _sample_return_metrics(selected, min_sample=min_sample)
            limited_metrics = _sample_return_metrics(limited, min_sample=min_sample)
            static_metrics = _sample_return_metrics(static, min_sample=min_sample)
            return_delta = round(
                _float(metrics.get("total_return")) - _float(limited_metrics.get("total_return")),
                10,
            )
            drawdown_delta = round(
                _float(metrics.get("max_drawdown")) - _float(limited_metrics.get("max_drawdown")),
                10,
            )
            turnover_delta = round(
                _float(metrics.get("turnover")) - _float(limited_metrics.get("turnover")),
                10,
            )
            rows.append(
                {
                    "variant_id": variant_id,
                    "regime": regime,
                    "sample_count": len(selected),
                    "relative_to_limited_adjustment": return_delta,
                    "relative_to_static_baseline": round(
                        _float(metrics.get("total_return"))
                        - _float(static_metrics.get("total_return")),
                        10,
                    ),
                    "drawdown_delta_vs_limited": drawdown_delta,
                    "turnover_delta_vs_limited": turnover_delta,
                    "regime_status": _regime_status(metrics, return_delta, drawdown_delta),
                    **EXPERIMENT_FACTORY_SAFETY,
                }
            )
    return rows


def _regime_status(metrics: Mapping[str, Any], return_delta: float, drawdown_delta: float) -> str:
    if metrics.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    if return_delta >= 0.0 and drawdown_delta >= 0.0:
        return "IMPROVED"
    if return_delta < 0.0 and drawdown_delta < 0.0:
        return "WORSE"
    return "MIXED"


def _variant_stability_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    variant_metrics, _, _ = _stability_diagnostics(variant_states, config)
    limited_states = [
        row for row in baseline_states if row.get("target_method") == "limited_adjustment"
    ]
    limited_metrics, _, _ = _stability_diagnostics(limited_states, config)
    limited_row = _find_method(limited_metrics, "limited_adjustment")
    rows = []
    for row in variant_metrics:
        variant_id = _text(row.get("target_method"))
        states = [item for item in variant_states if item.get("variant_id") == variant_id]
        rows.append(
            {
                "variant_id": variant_id,
                "avg_rebalance_turnover": row.get("avg_rebalance_turnover", 0.0),
                "max_rebalance_turnover": row.get("max_rebalance_turnover", 0.0),
                "large_jump_count": row.get("large_jump_count", 0),
                "weight_flip_count": _weight_flip_count(states),
                "rolling_consistency_delta": _variant_rolling_consistency_delta(
                    states,
                    limited_states,
                ),
                "stability_status": _variant_stability_status(row, limited_row),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _variant_stability_status(
    row: Mapping[str, Any],
    limited_row: Mapping[str, Any],
) -> str:
    status = _text(row.get("stability_status"), "INSUFFICIENT_DATA")
    if status == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    large_jump_count = _float(row.get("large_jump_count"))
    limited_jump_count = _float(limited_row.get("large_jump_count"))
    avg_turnover = _float(row.get("avg_rebalance_turnover"))
    limited_turnover = _float(limited_row.get("avg_rebalance_turnover"))
    if large_jump_count <= limited_jump_count and avg_turnover <= limited_turnover:
        return "STABLE"
    if status == "UNSTABLE":
        return "UNSTABLE"
    return "MODERATE"


def _weight_flip_count(states: Sequence[Mapping[str, Any]]) -> int:
    ordered = sorted(states, key=lambda row: _text(row.get("date")))
    last_delta: dict[str, float] = {}
    previous: Mapping[str, Any] | None = None
    flips = 0
    for row in ordered:
        if previous is None:
            previous = row
            continue
        deltas = _weight_deltas(_mapping(previous.get("weights")), _mapping(row.get("weights")))
        for symbol, delta in deltas.items():
            if last_delta.get(symbol, 0.0) * delta < 0:
                flips += 1
            if delta != 0:
                last_delta[symbol] = delta
        previous = row
    return flips


def _variant_rolling_consistency_delta(
    variant_states: Sequence[Mapping[str, Any]],
    limited_states: Sequence[Mapping[str, Any]],
) -> str:
    windows = _rolling_window_inventory(
        [*variant_states, *limited_states],
        min_observations=DEFAULT_MIN_EVAL_OBSERVATIONS,
    )
    if not windows:
        return "INSUFFICIENT_DATA"
    improved = 0
    worse = 0
    for window in windows:
        variant_metrics = _state_path_metrics(
            [
                row
                for row in variant_states
                if _coerce_date(window.get("start_date"), date(1970, 1, 1))
                <= _coerce_date(row.get("date"), date(1970, 1, 1))
                <= _coerce_date(window.get("end_date"), date(1970, 1, 1))
            ],
            min_observations=DEFAULT_MIN_EVAL_OBSERVATIONS,
        )
        limited_metrics = _state_path_metrics(
            [
                row
                for row in limited_states
                if _coerce_date(window.get("start_date"), date(1970, 1, 1))
                <= _coerce_date(row.get("date"), date(1970, 1, 1))
                <= _coerce_date(window.get("end_date"), date(1970, 1, 1))
            ],
            min_observations=DEFAULT_MIN_EVAL_OBSERVATIONS,
        )
        if variant_metrics.get("status") == "INSUFFICIENT_DATA":
            continue
        if _float(variant_metrics.get("max_drawdown")) >= _float(
            limited_metrics.get("max_drawdown")
        ):
            improved += 1
        else:
            worse += 1
    if improved == 0 and worse == 0:
        return "INSUFFICIENT_DATA"
    if improved > worse:
        return "IMPROVED"
    if worse > improved:
        return "WORSE"
    return "MIXED"


def _triage_variant_scorecard(
    batch: Mapping[str, Any],
    variant_specs: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    performance = {
        str(row.get("variant_id")): row
        for row in _records(batch.get("variant_performance_metrics"))
    }
    stability = {
        str(row.get("variant_id")): row for row in _records(batch.get("variant_stability_metrics"))
    }
    regimes = _records(batch.get("variant_regime_metrics"))
    weights = _mapping(policy.get("score_weights"))
    rows = []
    for spec in variant_specs:
        variant_id = _text(spec.get("variant_id"))
        perf = _mapping(performance.get(variant_id))
        stable = _mapping(stability.get(variant_id))
        regime_rows = [row for row in regimes if row.get("variant_id") == variant_id]
        components = {
            "return_score": _bounded_score(
                _float(perf.get("relative_to_limited_adjustment")),
                -0.05,
                0.05,
            ),
            "drawdown_score": _bounded_score(
                _float(perf.get("drawdown_delta_vs_limited")),
                -0.02,
                0.02,
            ),
            "rolling_consistency_score": _label_score(
                _text(stable.get("rolling_consistency_delta")),
                {"IMPROVED": 1.0, "MIXED": 0.55, "INSUFFICIENT_DATA": 0.0, "WORSE": 0.0},
            ),
            "regime_score": _triage_regime_score(regime_rows),
            "turnover_score": _bounded_score(
                -_float(perf.get("turnover_delta_vs_limited")),
                -0.2,
                0.2,
            ),
            "simplicity_score": _simplicity_score(spec),
        }
        overall = sum(
            components[key] * _float(weights.get(key.removesuffix("_score")), default)
            for key, default in (
                ("return_score", 0.25),
                ("drawdown_score", 0.25),
                ("rolling_consistency_score", 0.20),
                ("regime_score", 0.15),
                ("turnover_score", 0.10),
                ("simplicity_score", 0.05),
            )
        )
        flags = _hard_reject_flags(perf, stable, regime_rows, batch)
        decision = _triage_decision(overall, flags, perf, stable, policy)
        rows.append(
            {
                "variant_id": variant_id,
                "overall_score": round(overall, 6),
                "score_components": {key: round(value, 6) for key, value in components.items()},
                "hard_reject_flags": flags,
                "triage_decision": decision,
                "reason": _triage_reason(decision, flags, perf, stable),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(rows, key=lambda row: _float(row.get("overall_score")), reverse=True)


def _bounded_score(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _label_score(label: str, mapping: Mapping[str, float]) -> float:
    return _float(mapping.get(label), 0.0)


def _triage_regime_score(regime_rows: Sequence[Mapping[str, Any]]) -> float:
    valid = [row for row in regime_rows if row.get("regime_status") != "INSUFFICIENT_DATA"]
    if not valid:
        return 0.0
    points = sum(
        {
            "IMPROVED": 1.0,
            "MIXED": 0.55,
            "WORSE": 0.0,
        }.get(_text(row.get("regime_status")), 0.0)
        for row in valid
    )
    return points / len(valid)


def _simplicity_score(spec: Mapping[str, Any]) -> float:
    count = len(_records(spec.get("transforms")))
    if count <= 1:
        return 1.0
    if count == 2:
        return 0.75
    return 0.45


def _hard_reject_flags(
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
    regime_rows: Sequence[Mapping[str, Any]],
    batch: Mapping[str, Any],
) -> list[str]:
    flags = []
    if batch.get("data_quality_status") == "FAIL":
        flags.append("data_quality_FAIL")
    if _float(perf.get("drawdown_delta_vs_limited")) < -0.002:
        flags.append("max_drawdown_materially_worse")
    if stable.get("rolling_consistency_delta") == "WORSE":
        flags.append("rolling_consistency_worse")
    if any(
        row.get("regime") in {"risk_off", "tech_drawdown", "semiconductor_pullback"}
        and row.get("regime_status") == "WORSE"
        for row in regime_rows
    ):
        flags.append("pressure_regime_performance_worse")
    if _float(perf.get("turnover_delta_vs_limited")) > 0.25:
        flags.append("turnover_explodes")
    return flags


def _triage_decision(
    score: float,
    flags: Sequence[str],
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> str:
    if flags:
        return "REJECT"
    promote = _float(policy.get("promote_score"), 0.70)
    keep = _float(policy.get("keep_testing_score"), 0.55)
    if score >= promote and perf.get("performance_status") != "FAIL":
        return "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
    if score >= keep or stable.get("rolling_consistency_delta") == "IMPROVED":
        return "KEEP_FOR_MORE_TESTING"
    if perf.get("performance_status") == "INSUFFICIENT_DATA":
        return "DEFER"
    return "REJECT"


def _triage_reason(
    decision: str,
    flags: Sequence[str],
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
) -> list[str]:
    if flags:
        return [*flags, "hard_reject_rule_applied"]
    return [
        f"decision={decision}",
        f"performance_status={perf.get('performance_status')}",
        f"drawdown_delta_vs_limited={perf.get('drawdown_delta_vs_limited')}",
        f"rolling_consistency_delta={stable.get('rolling_consistency_delta')}",
        "research_only_no_broker_no_production",
    ]


def _promotion_candidates(
    scorecard: Sequence[Mapping[str, Any]],
    variant_specs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    specs = {str(row.get("variant_id")): row for row in variant_specs}
    rows = []
    for row in scorecard:
        if row.get("triage_decision") != "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE":
            continue
        variant_id = _text(row.get("variant_id"))
        spec = _mapping(specs.get(variant_id))
        rows.append(
            {
                "variant_id": variant_id,
                "proposed_method_name": _proposed_method_name(variant_id),
                "source_hypothesis_id": spec.get("hypothesis_id"),
                "promotion_reason": (
                    "Improves screening score without hard reject flags; formal "
                    "implementation still requires research-only validation."
                ),
                "implementation_complexity": _text(spec.get("complexity"), "MEDIUM"),
                "requires_formal_method": True,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows[:2]


def _proposed_method_name(variant_id: str) -> str:
    cleaned = variant_id.replace("-", "_").strip("_")
    if not cleaned:
        return "unnamed_limited_adjustment_research_method"
    return f"{cleaned}_limited_adjustment_research_method"


def _triage_summary(
    scorecard: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counts = {decision: 0 for decision in DEFAULT_TRIAGE_DECISIONS}
    for row in scorecard:
        decision = _text(row.get("triage_decision"))
        counts[decision] = counts.get(decision, 0) + 1
    top = _text(scorecard[0].get("variant_id")) if scorecard else "INSUFFICIENT_DATA"
    if candidates:
        action = "promote_top_variant"
    elif counts["KEEP_FOR_MORE_TESTING"]:
        action = "run_more_experiments"
    elif counts["REJECT"] == len(scorecard):
        action = "continue_diagnosis"
    else:
        action = "defer"
    return {
        "schema_version": SCHEMA_VERSION,
        "variants_total": len(scorecard),
        "promote_count": counts["PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"],
        "keep_testing_count": counts["KEEP_FOR_MORE_TESTING"],
        "reject_count": counts["REJECT"],
        "defer_count": counts["DEFER"],
        "top_variant": top,
        "recommended_next_action": action,
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _top_variant_explanations(
    triage: Mapping[str, Any],
    variant_specs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    specs = {str(row.get("variant_id")): row for row in variant_specs}
    selected = [
        row
        for row in _records(triage.get("variant_scorecard"))
        if row.get("triage_decision")
        in {"PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE", "KEEP_FOR_MORE_TESTING"}
    ][:5]
    if not selected:
        selected = _records(triage.get("variant_scorecard"))[:3]
    rows = []
    for row in selected:
        variant_id = _text(row.get("variant_id"))
        spec = _mapping(specs.get(variant_id))
        transforms = _records(spec.get("transforms"))
        rows.append(
            {
                "variant_id": variant_id,
                "triage_decision": row.get("triage_decision"),
                "what_it_changes": [_describe_transform(transform) for transform in transforms],
                "why_it_helped": _interpretation_benefits(row, spec),
                "what_it_costs": _texts(spec.get("expected_cost")) or ["may reduce upside capture"],
                "best_regimes": _best_regimes_for_variant(spec),
                "weak_regimes": _weak_regimes_for_variant(spec),
                "implementation_risk": _implementation_risk(spec),
                "recommended_promotion": (
                    row.get("triage_decision") == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
                ),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _describe_transform(transform: Mapping[str, Any]) -> str:
    transform_type = _text(transform.get("type"))
    if transform_type == "regime_gate":
        return f"{transform.get('regime')} regime uses {transform.get('action')}"
    if transform_type == "weight_smoothing":
        return f"smooths target weights over {transform.get('window_days')} days"
    if transform_type == "rebalance_threshold":
        return f"rebalances only above delta {transform.get('min_total_abs_delta')}"
    if transform_type == "min_cash_weight":
        return f"enforces min cash {transform.get('min_cash_weight')}"
    return transform_type


def _interpretation_benefits(row: Mapping[str, Any], spec: Mapping[str, Any]) -> list[str]:
    components = _mapping(row.get("score_components"))
    benefits = list(_texts(spec.get("expected_benefit")))
    if _float(components.get("rolling_consistency_score")) >= 0.55:
        benefits.append("improves rolling consistency")
    if _float(components.get("turnover_score")) >= 0.55:
        benefits.append("lowers turnover pressure")
    if _float(components.get("drawdown_score")) >= 0.55:
        benefits.append("improves drawdown behavior")
    return benefits or ["screening score improved without hard reject flags"]


def _best_regimes_for_variant(spec: Mapping[str, Any]) -> list[str]:
    regimes = [
        _text(transform.get("regime"))
        for transform in _records(spec.get("transforms"))
        if transform.get("regime")
    ]
    return regimes or ["ai_after_chatgpt"]


def _weak_regimes_for_variant(spec: Mapping[str, Any]) -> list[str]:
    costs = " ".join(_texts(spec.get("expected_cost"))).lower()
    if "rebound" in costs or "recovery" in costs:
        return ["strong_recovery"]
    return ["requires_forward_confirmation"]


def _implementation_risk(spec: Mapping[str, Any]) -> str:
    count = len(_records(spec.get("transforms")))
    if count <= 1:
        return "LOW"
    if count == 2:
        return "MEDIUM"
    return "HIGH"


def _variant_failure_mode_coverage(
    variant_specs: Sequence[Mapping[str, Any]],
    explanations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    explained = {str(row.get("variant_id")) for row in explanations}
    rows = []
    for mode in DEFAULT_FAILURE_MODES:
        covered = [
            _text(row.get("variant_id"))
            for row in variant_specs
            if mode in _texts(row.get("target_failure_modes"))
        ]
        promoted = [variant for variant in covered if variant in explained]
        if promoted:
            status = "GOOD"
        elif covered:
            status = "PARTIAL"
        else:
            status = "MISSING"
        rows.append(
            {
                "failure_mode": mode,
                "covered_by_variants": covered,
                "coverage_status": status,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "failure_modes": rows, **EXPERIMENT_FACTORY_SAFETY}


def _recommended_interpretation_variant(explanations: Sequence[Mapping[str, Any]]) -> str:
    for row in explanations:
        if row.get("recommended_promotion") is True:
            return _text(row.get("variant_id"))
    return _text(explanations[0].get("variant_id")) if explanations else "INSUFFICIENT_DATA"


def _promoted_method_specs(
    triage: Mapping[str, Any],
    interpretation: Mapping[str, Any],
) -> dict[str, Any]:
    explanations = {
        str(row.get("variant_id")): row
        for row in _records(interpretation.get("top_variant_explanations"))
    }
    methods = []
    for row in _records(triage.get("promotion_candidates")):
        variant_id = _text(row.get("variant_id"))
        explanation = _mapping(explanations.get(variant_id))
        methods.append(
            {
                "proposed_method_name": row.get("proposed_method_name"),
                "source_variant_id": variant_id,
                "base_method": "limited_adjustment",
                "implementation_scope": "research_only",
                "expected_benefit": _texts(explanation.get("why_it_helped")),
                "expected_cost": _texts(explanation.get("what_it_costs")),
                "required_validation_after_implementation": [
                    "paper shadow backfill",
                    "rolling eval",
                    "regime review",
                    "stability diagnostics",
                    "forward confirmation",
                ],
                "auto_apply": False,
                "broker_action_allowed": False,
                "production_effect": PRODUCTION_EFFECT,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "methods": methods,
        "next_action": (
            "owner_review_then_formal_research_method_task"
            if methods
            else "run_more_experiments_before_promotion"
        ),
        **EXPERIMENT_FACTORY_SAFETY,
    }


def render_hypothesis_backlog_report(
    manifest: Mapping[str, Any],
    taxonomy: Mapping[str, Any],
    hypotheses: Sequence[Mapping[str, Any]],
    priority: Mapping[str, Any],
) -> str:
    high_priority = ", ".join(_texts(priority.get("high_priority_hypotheses"))) or "none"
    uncovered = ", ".join(_texts(priority.get("uncovered_failure_modes"))) or "none"
    matrix_candidates = (
        ", ".join(_texts(priority.get("recommended_for_experiment_matrix"))) or "none"
    )
    failure_mode_lines = [
        f"- {row.get('id')}: {row.get('description')}"
        for row in _records(taxonomy.get("failure_modes"))
    ]
    return (
        "\n".join(
            [
                f"# Weight Optimization Hypothesis Backlog {manifest.get('backlog_id')}",
                "",
                "## 摘要",
                f"- failure modes: {priority.get('failure_modes_total')}",
                f"- hypotheses: {priority.get('hypotheses_total')}",
                f"- HIGH priority: {high_priority}",
                f"- uncovered failure modes: {uncovered}",
                f"- matrix candidates: {matrix_candidates}",
                "",
                "## Failure Modes",
                *failure_mode_lines,
                "",
                "## Safety",
                "- experiment_only=true",
                "- research_screening_only=true",
                "- broker_action_allowed=false",
                "- production_effect=none",
                "",
                f"artifact: {manifest.get('hypothesis_backlog_manifest_path')}",
            ]
        )
        + "\n"
    )


def render_variant_transform_spec_report(
    manifest: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                f"# Variant Transform Spec {manifest.get('spec_id')}",
                "",
                "## Transform Catalog",
                *[
                    f"- {row.get('type')}: required={','.join(_texts(row.get('required_fields')))}"
                    for row in _records(catalog.get("transform_types"))
                ],
                "",
                "## Safety",
                "- experiment_only=true",
                "- research_screening_only=true",
                "- not_formal_research_method=true",
                "- broker_action_allowed=false",
                "- production_effect=none",
            ]
        )
        + "\n"
    )


def render_experiment_matrix_report(
    manifest: Mapping[str, Any],
    variants: Sequence[Mapping[str, Any]],
    transform_specs: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    _ = transform_specs
    by_transform_type = _mapping(summary.get("variants_by_transform_type"))
    regime_gate = ", ".join(_texts(by_transform_type.get("regime_gate")))
    smoothing = ", ".join(_texts(by_transform_type.get("weight_smoothing")))
    cooldown = ", ".join(_texts(by_transform_type.get("regime_cooldown")))
    ensemble = ", ".join(_texts(by_transform_type.get("consensus_aggregation")))
    return (
        "\n".join(
            [
                f"# Experiment Matrix {manifest.get('matrix_id')}",
                "",
                "## 摘要",
                f"- variants: {len(variants)}",
                f"- families: {', '.join(_texts(summary.get('families_covered')))}",
                f"- failure modes: {', '.join(_texts(summary.get('failure_modes_covered')))}",
                f"- regime-gating variants: {regime_gate}",
                f"- smoothing variants: {smoothing}",
                f"- cooldown variants: {cooldown}",
                f"- ensemble variants: {ensemble}",
                f"- formal method variants: {summary.get('formal_method_variant_count')}",
                "",
                "## Safety",
                "- all variants are experiment_only=true",
                "- not_formal_research_method=true",
                "- broker_action_allowed=false",
                "- production_effect=none",
            ]
        )
        + "\n"
    )


def render_batch_experiment_report(
    manifest: Mapping[str, Any],
    performance: Sequence[Mapping[str, Any]],
    regime: Sequence[Mapping[str, Any]],
    stability: Sequence[Mapping[str, Any]],
) -> str:
    drawdown = _best_variant(performance, "drawdown_delta_vs_limited")
    rolling = _best_label_variant(stability, "rolling_consistency_delta", "IMPROVED")
    turnover = _best_variant(performance, "turnover_delta_vs_limited", high=False)
    sacrificed = [
        _text(row.get("variant_id"))
        for row in performance
        if _float(row.get("relative_to_limited_adjustment")) < -0.05
    ]
    sideways = [
        _text(row.get("variant_id"))
        for row in regime
        if row.get("regime") == "sideways_choppy" and row.get("regime_status") == "IMPROVED"
    ]
    insufficient = ", ".join(_insufficient_variants(performance)) or "none"
    return (
        "\n".join(
            [
                f"# Batch Experiment {manifest.get('batch_id')}",
                "",
                "## 摘要",
                f"- market_regime: {manifest.get('market_regime')}",
                f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
                f"- data_quality_status: {manifest.get('data_quality_status')}",
                f"- variants completed: {manifest.get('variants_completed')}",
                f"- data insufficient variants: {insufficient}",
                f"- best drawdown variant: {drawdown}",
                f"- best rolling consistency variant: {rolling}",
                f"- best turnover variant: {turnover}",
                f"- sideways_choppy improved: {', '.join(sideways) or 'none'}",
                f"- material return sacrifice: {', '.join(sacrificed) or 'none'}",
                "",
                "## Safety",
                "- experiment_only=true",
                "- research_screening_only=true",
                "- broker_action_allowed=false",
                "- production_effect=none",
            ]
        )
        + "\n"
    )


def render_experiment_triage_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    scorecard: Sequence[Mapping[str, Any]],
) -> str:
    promoted = [
        _text(row.get("variant_id"))
        for row in scorecard
        if row.get("triage_decision") == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
    ]
    rejected = [
        _text(row.get("variant_id")) for row in scorecard if row.get("triage_decision") == "REJECT"
    ]
    keep = [
        _text(row.get("variant_id"))
        for row in scorecard
        if row.get("triage_decision") == "KEEP_FOR_MORE_TESTING"
    ]
    return (
        "\n".join(
            [
                f"# Experiment Triage {manifest.get('triage_id')}",
                "",
                "## 摘要",
                f"- promote: {summary.get('promote_count')} ({', '.join(promoted) or 'none'})",
                (
                    f"- keep_testing: {summary.get('keep_testing_count')} "
                    f"({', '.join(keep) or 'none'})"
                ),
                f"- reject: {summary.get('reject_count')} ({', '.join(rejected) or 'none'})",
                f"- top_variant: {summary.get('top_variant')}",
                f"- next_action: {summary.get('recommended_next_action')}",
                "",
                "## 解释",
                "Scorecard 只用于 research screening。PROMOTE 表示可以另开 formal research "
                "method implementation，不表示生产批准。",
                "",
                "## Safety",
                "- research_screening_only=true",
                "- not_official_target_weights=true",
                "- broker_action_allowed=false",
                "- production_effect=none",
            ]
        )
        + "\n"
    )


def render_top_variant_interpretation_report(
    manifest: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
    coverage: Mapping[str, Any],
) -> str:
    missing = [
        _text(row.get("failure_mode"))
        for row in _records(coverage.get("failure_modes"))
        if row.get("coverage_status") == "MISSING"
    ]
    lines = [
        f"# Top Variant Interpretation {manifest.get('interpretation_id')}",
        "",
        "## Top Variants",
    ]
    for row in explanations:
        lines.extend(
            [
                f"- {row.get('variant_id')}: {row.get('triage_decision')}",
                f"  changes: {', '.join(_texts(row.get('what_it_changes')))}",
                f"  helped: {', '.join(_texts(row.get('why_it_helped')))}",
                f"  costs: {', '.join(_texts(row.get('what_it_costs')))}",
            ]
        )
    lines.extend(
        [
            "",
            f"remaining missing failure modes: {', '.join(missing) or 'none'}",
            f"recommended variant: {manifest.get('recommended_variant')}",
            "",
            "Safety: research-only / no broker / no production.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_top_variant_interpretation_reader_brief(
    manifest: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    best = _mapping(explanations[0] if explanations else {})
    return (
        "\n".join(
            [
                "## Dynamic Rescue Weight Experiment Top Variant Interpretation",
                "",
                f"- best_variant: {manifest.get('recommended_variant')}",
                f"- triage_decision: {best.get('triage_decision', 'MISSING')}",
                f"- solved_failure_modes: {', '.join(_texts(best.get('why_it_helped')))}",
                f"- expected_costs: {', '.join(_texts(best.get('what_it_costs')))}",
                "- production_effect: none",
                "- broker_action_allowed: false",
            ]
        )
        + "\n"
    )


def render_formal_implementation_plan(
    manifest: Mapping[str, Any],
    method_specs: Mapping[str, Any],
) -> str:
    methods = _records(method_specs.get("methods"))
    recommended = ", ".join(_text(row.get("proposed_method_name")) for row in methods) or "none"
    selection_reason = (
        "选择原因：来自 batch experiment triage 和 top variant interpretation；"
        "仍需 owner review。"
    )
    required_validation = (
        "实现后必须运行：paper shadow backfill、rolling eval、regime review、"
        "stability diagnostics、forward confirmation。"
    )
    scope_note = (
        "Scope: research_only。不能 production/broker，因为本阶段 evidence 是 "
        "lightweight screening，尚未完成正式 method implementation、forward "
        "confirmation 或 owner approval。"
    )
    return (
        "\n".join(
            [
                "# Formal Research Method Promotion Plan",
                "",
                f"推荐正式实现 method：{recommended}",
                "",
                selection_reason,
                "",
                "需要新增 config / CLI / artifacts：",
                "- formal research method source config",
                "- method config validation/report",
                "- research target generation",
                "- paper shadow backfill",
                "- comparison/review pack",
                "",
                required_validation,
                "",
                scope_note,
                "",
                f"source promotion_plan_id: {manifest.get('promotion_plan_id')}",
            ]
        )
        + "\n"
    )


def render_method_promotion_owner_checklist(method_specs: Mapping[str, Any]) -> str:
    methods = ", ".join(
        _text(row.get("proposed_method_name")) for row in _records(method_specs.get("methods"))
    )
    return (
        "\n".join(
            [
                "# Owner Review Checklist",
                "",
                (
                    "- [ ] 是否接受从 batch experiment 中 promotion top variant？"
                    f"({methods or 'none'})"
                ),
                "- [ ] 是否接受实现为 research-only method？",
                "- [ ] 是否接受不写 official target weights？",
                "- [ ] 是否接受实现后仍需 forward confirmation？",
                "- [ ] 是否确认不触发 broker / production？",
            ]
        )
        + "\n"
    )


def render_method_promotion_plan_report(
    manifest: Mapping[str, Any],
    method_specs: Mapping[str, Any],
) -> str:
    methods = _records(method_specs.get("methods"))
    lines = [
        f"# Method Promotion Plan {manifest.get('promotion_plan_id')}",
        "",
        f"- implementation_scope: {manifest.get('implementation_scope')}",
        f"- next_action: {method_specs.get('next_action')}",
    ]
    for row in methods:
        lines.extend(
            [
                f"- proposed_method_name: {row.get('proposed_method_name')}",
                f"  source_variant_id: {row.get('source_variant_id')}",
                f"  expected_benefit: {', '.join(_texts(row.get('expected_benefit')))}",
                f"  expected_cost: {', '.join(_texts(row.get('expected_cost')))}",
            ]
        )
    lines.extend(["", "Safety: no official target / no broker / no production."])
    return "\n".join(lines) + "\n"


def render_method_promotion_reader_brief(
    manifest: Mapping[str, Any],
    method_specs: Mapping[str, Any],
) -> str:
    methods = _records(method_specs.get("methods"))
    first = _mapping(methods[0] if methods else {})
    return (
        "\n".join(
            [
                "## Dynamic Rescue Weight Experiment Promotion Plan",
                "",
                f"- promoted_variant: {first.get('source_variant_id', 'none')}",
                f"- proposed_method_name: {first.get('proposed_method_name', 'none')}",
                f"- expected_benefit: {', '.join(_texts(first.get('expected_benefit')))}",
                f"- expected_cost: {', '.join(_texts(first.get('expected_cost')))}",
                f"- implementation_scope: {manifest.get('implementation_scope')}",
                f"- next_action: {method_specs.get('next_action')}",
                "- broker_action_allowed: false",
                "- production_effect: none",
            ]
        )
        + "\n"
    )


def _best_variant(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    high: bool = True,
) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    selected = (
        max(rows, key=lambda row: _float(row.get(field)))
        if high
        else min(rows, key=lambda row: _float(row.get(field)))
    )
    return _text(selected.get("variant_id"))


def _best_label_variant(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    preferred: str,
) -> str:
    for row in rows:
        if row.get(field) == preferred:
            return _text(row.get("variant_id"))
    return _text(rows[0].get("variant_id")) if rows else "INSUFFICIENT_DATA"


def _insufficient_variants(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        _text(row.get("variant_id"))
        for row in rows
        if row.get("performance_status") == "INSUFFICIENT_DATA"
    ]


def _safety_config_locked(safety: Mapping[str, Any]) -> bool:
    return (
        safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("production_effect") == PRODUCTION_EFFECT
        and safety.get("auto_apply", False) is False
    )


def _payload_safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("research_target_only", True) is True
        and payload.get("paper_shadow_only", True) is True
        and payload.get("not_official_target_weights", True) is True
        and payload.get("broker_action_allowed") is not True
        and payload.get("broker_action_taken") is not True
        and payload.get("order_ticket_generated") is not True
        and payload.get("production_state_mutated") is not True
        and payload.get("baseline_config_mutated") is not True
        and payload.get("official_target_weights_mutated") is not True
        and payload.get("production_candidate_generated") is not True
        and payload.get("automatic_candidate_promotion") is not True
        and payload.get("auto_apply") is not True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        for payload in payloads
        if payload
    )


def _enabled_methods(config: Mapping[str, Any]) -> list[str]:
    methods = _mapping(_mapping(config.get("target_methods")).get("enabled"))
    if methods:
        return [str(item) for item in methods.values()]
    raw = _mapping(config.get("target_methods")).get("enabled", [])
    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        return [str(item) for item in raw]
    return []


def _config_baseline_weights(config: Mapping[str, Any]) -> dict[str, float]:
    return _normalize_weights(_mapping(_mapping(config.get("baseline")).get("static_weights")))


def _constraints(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(config.get("constraints"))


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): max(0.0, _float(value)) for symbol, value in weights.items()
    }
    cleaned = {symbol: value for symbol, value in cleaned.items() if symbol and value > 0}
    total = sum(cleaned.values())
    if total <= 0:
        raise DynamicV3SystemTargetError("weights must contain positive values")
    normalized = {symbol: round(value / total, 10) for symbol, value in sorted(cleaned.items())}
    residual = round(1.0 - sum(normalized.values()), 10)
    cash_symbol = "CASH" if "CASH" in normalized else sorted(normalized)[0]
    normalized[cash_symbol] = round(normalized[cash_symbol] + residual, 10)
    return normalized


def _weights_sum_to_one(value: object) -> bool:
    try:
        weights = _normalize_weights(_mapping(value))
    except DynamicV3SystemTargetError:
        return False
    return abs(sum(weights.values()) - 1.0) <= 0.000001


def _turnover(before: Mapping[str, float], after: Mapping[str, float]) -> float:
    symbols = set(before) | set(after)
    return round(
        sum(abs(_float(after.get(symbol)) - _float(before.get(symbol))) for symbol in symbols)
        / 2.0,
        10,
    )


def _weight_deltas(before: Mapping[str, float], after: Mapping[str, float]) -> dict[str, float]:
    return {
        symbol: round(_float(after.get(symbol)) - _float(before.get(symbol)), 10)
        for symbol in sorted(set(before) | set(after))
    }


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    status = "PASS" if all(check.get("passed") is True for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "artifact_id": artifact_id,
        "status": status,
        "checks": list(checks),
        "failed_check_count": sum(1 for check in checks if check.get("passed") is not True),
        **dict(extra or {}),
        **SYSTEM_TARGET_SAFETY,
    }


def _required_file_checks(root: Path, names: Sequence[str]) -> list[dict[str, Any]]:
    return [
        _check(f"artifact_exists:{name}", (root / name).exists(), str(root / name))
        for name in names
    ]


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}


def _artifact_dir(
    *,
    artifact_id: str | None,
    latest_pointer: str,
    latest: bool,
    output_dir: Path,
    required_name: str,
) -> Path:
    resolved_id = artifact_id
    if latest:
        resolved_id = _latest_pointer_artifact_id(latest_pointer)
    if not resolved_id:
        raise DynamicV3SystemTargetError(
            f"--{latest_pointer.removeprefix('latest_')}-id or --latest is required"
        )
    root = output_dir / resolved_id
    if not (root / required_name).exists():
        raise DynamicV3SystemTargetError(f"artifact not found: {root / required_name}")
    return root


def _latest_pointer_artifact_id(name: str) -> str:
    payload = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{name}.json") or {}
    return _text(payload.get("artifact_id"))


def _write_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    if not _is_default_dynamic_v3_research_artifact(path):
        return
    DEFAULT_LATEST_POINTER_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(
        DEFAULT_LATEST_POINTER_DIR / f"{name}.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": name.removeprefix("latest_"),
            "artifact_id": artifact_id,
            "path": str(path),
            "updated_at": datetime.now(UTC).isoformat(),
            "exists": path.exists(),
        },
    )


def _latest_child_dir_with(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    candidates = [path.parent for path in root.glob(f"*/{filename}") if path.is_file()]
    return (
        max(candidates, key=lambda path: (path / filename).stat().st_mtime) if candidates else None
    )


def _is_default_dynamic_v3_research_artifact(path: Path) -> bool:
    try:
        path.resolve().relative_to(DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve())
    except ValueError:
        return False
    return True


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.name}-{index:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3SystemTargetError(f"unable to allocate unique artifact dir under {path.parent}")


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3SystemTargetError(f"YAML root must be mapping: {path}")
    return dict(raw)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3SystemTargetError(f"required JSON artifact not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicV3SystemTargetError(f"JSON artifact must be object: {path}")
    return payload


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _stable_id(prefix: str, *parts: object) -> str:
    digest = sha256(
        json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{prefix}_{digest[:16]}"


def _coerce_date(value: object, default: date) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return default


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


# ARCH-004 G2.4CH compatibility surface. The canonical implementation lives in
# the bounded system-target portfolio domain; lazy imports avoid a module cycle
# while preserving every historical Python caller.
def _system_target_portfolio_implementation() -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio

    return dynamic_v3_system_target_portfolio


def _call_system_target_portfolio(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return getattr(_system_target_portfolio_implementation(), name)(*args, **kwargs)
    except DynamicV3SystemTargetError:
        raise
    except ValueError as exc:
        raise DynamicV3SystemTargetError(str(exc)) from exc


def load_model_target_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("load_model_target_config", *args, **kwargs)


def validate_model_target_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("validate_model_target_config", *args, **kwargs)


def generate_model_target(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("generate_model_target", *args, **kwargs)


def model_target_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("model_target_report_payload", *args, **kwargs)


def validate_model_target_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("validate_model_target_artifact", *args, **kwargs)


def load_paper_shadow_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("load_paper_shadow_config", *args, **kwargs)


def init_paper_shadow_account(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("init_paper_shadow_account", *args, **kwargs)


def paper_shadow_state_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("paper_shadow_state_payload", *args, **kwargs)


def paper_shadow_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("paper_shadow_report_payload", *args, **kwargs)


def validate_paper_shadow_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("validate_paper_shadow_artifact", *args, **kwargs)


def simulate_model_rebalance(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("simulate_model_rebalance", *args, **kwargs)


def model_rebalance_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("model_rebalance_report_payload", *args, **kwargs)


def validate_model_rebalance_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("validate_model_rebalance_artifact", *args, **kwargs)


def run_paper_shadow_performance(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("run_paper_shadow_performance", *args, **kwargs)


def paper_shadow_performance_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio(
        "paper_shadow_performance_report_payload", *args, **kwargs
    )


def validate_paper_shadow_performance_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_portfolio(
        "validate_paper_shadow_performance_artifact", *args, **kwargs
    )


def build_system_target_review_pack(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("build_system_target_review_pack", *args, **kwargs)


def system_target_review_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio("system_target_review_report_payload", *args, **kwargs)


def validate_system_target_review_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_portfolio(
        "validate_system_target_review_artifact", *args, **kwargs
    )


# ARCH-004 G2.4CI compatibility surface. The canonical implementation lives in
# the bounded historical-evaluation domain; lazy imports avoid a module cycle
# while preserving every historical Python caller.
def _system_target_history_implementation() -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history

    return dynamic_v3_system_target_history


def _call_system_target_history(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return getattr(_system_target_history_implementation(), name)(*args, **kwargs)
    except DynamicV3SystemTargetError:
        raise
    except ValueError as exc:
        raise DynamicV3SystemTargetError(str(exc)) from exc


def load_paper_shadow_backfill_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history(
        "load_paper_shadow_backfill_config", *args, **kwargs
    )


def validate_paper_shadow_backfill_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_paper_shadow_backfill_config", *args, **kwargs
    )


def run_paper_shadow_backfill(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history("run_paper_shadow_backfill", *args, **kwargs)


def paper_shadow_backfill_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history(
        "paper_shadow_backfill_report_payload", *args, **kwargs
    )


def validate_paper_shadow_backfill_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_paper_shadow_backfill_artifact", *args, **kwargs
    )


def run_paper_shadow_rolling_eval(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history("run_paper_shadow_rolling_eval", *args, **kwargs)


def paper_shadow_rolling_eval_report_payload(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "paper_shadow_rolling_eval_report_payload", *args, **kwargs
    )


def validate_paper_shadow_rolling_eval_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_paper_shadow_rolling_eval_artifact", *args, **kwargs
    )


def run_paper_shadow_regime_review(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history("run_paper_shadow_regime_review", *args, **kwargs)


def paper_shadow_regime_review_report_payload(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "paper_shadow_regime_review_report_payload", *args, **kwargs
    )


def validate_paper_shadow_regime_review_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_paper_shadow_regime_review_artifact", *args, **kwargs
    )


def run_paper_shadow_stability(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history("run_paper_shadow_stability", *args, **kwargs)


def paper_shadow_stability_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history(
        "paper_shadow_stability_report_payload", *args, **kwargs
    )


def validate_paper_shadow_stability_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_paper_shadow_stability_artifact", *args, **kwargs
    )


def run_system_target_selection_review(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_history(
        "run_system_target_selection_review", *args, **kwargs
    )


def system_target_selection_review_report_payload(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "system_target_selection_review_report_payload", *args, **kwargs
    )


def validate_system_target_selection_review_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_history(
        "validate_system_target_selection_review_artifact", *args, **kwargs
    )


# ARCH-004 G2.4CJ compatibility surface. The canonical implementation lives in
# the bounded method-hardening domain; lazy imports avoid a module cycle while
# preserving historical Python callers.
def _system_target_hardening_implementation() -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening

    return dynamic_v3_system_target_hardening


def _call_system_target_hardening(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return getattr(_system_target_hardening_implementation(), name)(*args, **kwargs)
    except DynamicV3SystemTargetError:
        raise
    except ValueError as exc:
        raise DynamicV3SystemTargetError(str(exc)) from exc


def run_selection_attribution(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening("run_selection_attribution", *args, **kwargs)


def selection_attribution_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "selection_attribution_report_payload", *args, **kwargs
    )


def validate_selection_attribution_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "validate_selection_attribution_artifact", *args, **kwargs
    )


def run_limited_long_risk_review(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening("run_limited_long_risk_review", *args, **kwargs)


def limited_long_risk_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "limited_long_risk_report_payload", *args, **kwargs
    )


def validate_limited_long_risk_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "validate_limited_long_risk_artifact", *args, **kwargs
    )


def run_limited_consistency_check(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "run_limited_consistency_check", *args, **kwargs
    )


def limited_consistency_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "limited_consistency_report_payload", *args, **kwargs
    )


def validate_limited_consistency_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "validate_limited_consistency_artifact", *args, **kwargs
    )


def run_data_warning_impact_review(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "run_data_warning_impact_review", *args, **kwargs
    )


def data_warning_impact_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "data_warning_impact_report_payload", *args, **kwargs
    )


def validate_data_warning_impact_artifact(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "validate_data_warning_impact_artifact", *args, **kwargs
    )


def run_research_method_hardening_pack(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "run_research_method_hardening_pack", *args, **kwargs
    )


def research_method_hardening_report_payload(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call_system_target_hardening(
        "research_method_hardening_report_payload", *args, **kwargs
    )


def validate_research_method_hardening_artifact(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    return _call_system_target_hardening(
        "validate_research_method_hardening_artifact", *args, **kwargs
    )
