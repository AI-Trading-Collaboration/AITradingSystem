from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_ALLOCATION_POLICY_SCHEMA_VERSION = "etf_dynamic_allocation_policy_v1"
DYNAMIC_ALLOCATION_DECISION_SCHEMA_VERSION = "etf_dynamic_allocation_decision_record_v1"
DYNAMIC_ALLOCATION_REPORT_SCHEMA_VERSION = "etf_dynamic_allocation_report_v1"
DYNAMIC_ALLOCATION_REGISTRY_SCHEMA_VERSION = "etf_dynamic_allocation_policy_registry_v1"
DYNAMIC_ALLOCATION_VALIDATION_SCHEMA_VERSION = "etf_dynamic_allocation_validation_v1"

DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_allocation_policy.yaml"
)
DEFAULT_DYNAMIC_ALLOCATION_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_allocation"
DEFAULT_DYNAMIC_ALLOCATION_DECISION_DIR = DEFAULT_DYNAMIC_ALLOCATION_ROOT / "decisions"
DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR = DEFAULT_DYNAMIC_ALLOCATION_ROOT / "reports"
DEFAULT_DYNAMIC_ALLOCATION_REGISTRY_DIR = DEFAULT_DYNAMIC_ALLOCATION_ROOT / "registry"
DEFAULT_DYNAMIC_ALLOCATION_VALIDATION_DIR = DEFAULT_DYNAMIC_ALLOCATION_ROOT / "validation"

SAFETY_FIELDS: dict[str, Any] = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
}
FORBIDDEN_OUTPUT_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "automatic_candidate_promotion",
    "auto_enrollment_without_owner_approval",
}
WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")


class DynamicAllocationError(RuntimeError):
    """Raised when dynamic allocation inputs or outputs are unsafe."""


class DynamicAllocationMarketRegime(BaseModel):
    regime_id: str = Field(min_length=1)
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_decision_start: date

    @model_validator(mode="after")
    def validate_ai_regime_start(self) -> Self:
        if self.regime_id != "ai_after_chatgpt":
            raise ValueError("TRADING-084 default market regime must be ai_after_chatgpt")
        if self.default_decision_start < date(2022, 12, 1):
            raise ValueError("dynamic allocation default decision start cannot predate 2022-12-01")
        return self


class DynamicAllocationSafety(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: bool
    production_state_mutated: bool
    baseline_config_mutated: bool
    official_target_weights_mutated: bool

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.model_dump() != SAFETY_FIELDS:
            raise ValueError("dynamic allocation safety fields are unsafe")
        return self


class RegimeWeightTarget(BaseModel):
    weights: dict[str, float] = Field(min_length=1)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        _validate_weight_map(self.weights, context="regime weight target")
        return self


class RegimeSelectionRuleConfig(BaseModel):
    score_id: str | None = None
    threshold: float | None = None
    risk_regime_score_max: float | None = None
    composite_trend_score_max: float | None = None
    growth_leadership_score_max: float | None = None
    semiconductor_leadership_score_min: float | None = None
    composite_trend_score_min: float | None = None
    risk_regime_score_min: float | None = None
    rationale: str = Field(min_length=1)


class TrendOverlayRuleConfig(BaseModel):
    rule_id: str = Field(min_length=1)
    score_id: str = Field(min_length=1)
    comparator: Literal[">=", "<", "<=", ">"]
    threshold: float
    adjustments: dict[str, float] = Field(min_length=1)
    reason_code: str = Field(min_length=1)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_adjustments(self) -> Self:
        missing = sorted(set(self.adjustments) - set(WEIGHT_SYMBOLS))
        if missing:
            raise ValueError(
                f"trend overlay rule {self.rule_id} references unknown symbols: "
                + ", ".join(missing)
            )
        if abs(sum(self.adjustments.values())) > 1e-8:
            raise ValueError(f"trend overlay rule {self.rule_id} adjustments must sum to 0")
        return self


class EventRiskOverlayConfig(BaseModel):
    enabled: bool
    score_id: str = Field(min_length=1)
    high_threshold: float
    cash_increase: float = Field(ge=0, le=1)
    reductions: dict[str, float] = Field(min_length=1)
    reason_code: str = Field(min_length=1)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_reductions(self) -> Self:
        if "CASH" in self.reductions:
            raise ValueError("event risk reductions must not reduce CASH")
        missing = sorted(set(self.reductions) - set(WEIGHT_SYMBOLS))
        if missing:
            raise ValueError(
                "event risk reductions reference unknown symbols: " + ", ".join(missing)
            )
        if abs(sum(self.reductions.values()) - self.cash_increase) > 1e-8:
            raise ValueError("event risk reductions must sum to cash_increase")
        return self


class ExposureConstraintsConfig(BaseModel):
    asset_caps: dict[str, float] = Field(min_length=1)
    asset_floors: dict[str, float] = Field(min_length=1)
    semiconductor_sleeve_max: float = Field(ge=0, le=1)
    cash_max: float = Field(ge=0, le=1)
    cash_min: float = Field(ge=0, le=1)
    long_only: bool
    allow_leverage: bool

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        if set(self.asset_caps) != set(WEIGHT_SYMBOLS):
            raise ValueError("asset_caps must define SPY/QQQ/SMH/SOXX/CASH")
        if set(self.asset_floors) != set(WEIGHT_SYMBOLS):
            raise ValueError("asset_floors must define SPY/QQQ/SMH/SOXX/CASH")
        for symbol in WEIGHT_SYMBOLS:
            floor = self.asset_floors[symbol]
            cap = self.asset_caps[symbol]
            if floor < 0 or cap > 1 or floor > cap:
                raise ValueError(f"invalid asset floor/cap for {symbol}")
        if self.cash_min > self.cash_max:
            raise ValueError("cash_min cannot exceed cash_max")
        if not self.long_only:
            raise ValueError("TRADING-084 requires long_only=true")
        if self.allow_leverage:
            raise ValueError("TRADING-084 requires allow_leverage=false")
        return self


class RebalancePolicyConfig(BaseModel):
    score_change_threshold: float = Field(ge=0)
    regime_confirmation_days: int = Field(ge=1)
    minimum_holding_days: int = Field(ge=1)
    max_single_rebalance_delta: float = Field(ge=0, le=1)
    weekly_turnover_cap: float = Field(ge=0, le=1)
    min_rebalance_weight_delta: float = Field(ge=0, le=1)
    rationale: str = Field(min_length=1)


class DynamicAllocationPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_allocation_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicAllocationMarketRegime
    safety: DynamicAllocationSafety
    default_policy_id: str = Field(min_length=1)
    base_weights: dict[str, float] = Field(min_length=1)
    regime_weight_targets: dict[str, RegimeWeightTarget] = Field(min_length=1)
    regime_selection_rules: dict[str, RegimeSelectionRuleConfig] = Field(min_length=1)
    trend_overlay_rules: list[TrendOverlayRuleConfig]
    event_risk_overlay: EventRiskOverlayConfig
    exposure_constraints: ExposureConstraintsConfig
    rebalance_policy: RebalancePolicyConfig
    sample_score_profiles: dict[str, dict[str, float]] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        _validate_weight_map(self.base_weights, context="base weights")
        expected_regimes = {
            "risk_on",
            "neutral",
            "risk_off",
            "growth_underperformance",
            "semiconductor_leadership",
            "event_risk_high",
        }
        missing = expected_regimes - set(self.regime_weight_targets)
        if missing:
            raise ValueError(
                "dynamic allocation policy missing regime targets: " + ", ".join(sorted(missing))
            )
        missing_rules = expected_regimes - set(self.regime_selection_rules)
        if missing_rules:
            raise ValueError(
                "dynamic allocation policy missing regime selection rules: "
                + ", ".join(sorted(missing_rules))
            )
        return self


def load_dynamic_allocation_policy_config(
    path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
) -> DynamicAllocationPolicyConfig:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise DynamicAllocationError("dynamic allocation policy must be a mapping")
    try:
        return DynamicAllocationPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicAllocationError(f"invalid dynamic allocation policy: {exc}") from exc


def build_dynamic_allocation_decision_record(
    *,
    policy: DynamicAllocationPolicyConfig,
    decision_date: date,
    input_scores: dict[str, float],
    previous_weights: dict[str, float] | None = None,
    previous_scores: dict[str, float] | None = None,
    days_since_last_rebalance: int | None = None,
    confirmed_regime_days: int | None = None,
    source_trend_report: str | None = None,
    data_quality_status: str = "UNKNOWN",
) -> dict[str, Any]:
    if decision_date < policy.market_regime.default_decision_start:
        raise DynamicAllocationError("dynamic allocation decision date predates AI regime start")
    _validate_scores(input_scores)
    previous = _normalise_weights(previous_weights or policy.base_weights)
    selected_regime, regime_reason = select_dynamic_regime_state(input_scores, policy)
    template = policy.regime_weight_targets[selected_regime].weights
    proposed = _normalise_weights(template)
    reason_codes = [f"REGIME_{selected_regime.upper()}"]
    overlay_details: list[dict[str, Any]] = []
    proposed, trend_overlays = apply_trend_overlay_adjustments(
        proposed,
        input_scores,
        policy,
    )
    overlay_details.extend(trend_overlays)
    reason_codes.extend(str(item["reason_code"]) for item in trend_overlays)
    proposed, event_overlay = apply_event_risk_overlay(proposed, input_scores, policy)
    if event_overlay:
        overlay_details.append(event_overlay)
        reason_codes.append(str(event_overlay["reason_code"]))
    constrained, constraints_applied, diagnostics = apply_dynamic_allocation_constraints(
        proposed,
        previous,
        policy,
    )
    reason_codes.extend(constraints_applied)
    rebalance = build_rebalance_decision(
        proposed_weights=constrained,
        previous_weights=previous,
        input_scores=input_scores,
        previous_scores=previous_scores,
        selected_regime=selected_regime,
        policy=policy,
        days_since_last_rebalance=days_since_last_rebalance,
        confirmed_regime_days=confirmed_regime_days,
    )
    final_weights = constrained
    if rebalance["decision"] == "hold":
        final_weights = previous
    trade_deltas = {
        symbol: round(final_weights.get(symbol, 0.0) - previous.get(symbol, 0.0), 10)
        for symbol in WEIGHT_SYMBOLS
    }
    record = {
        "schema_version": DYNAMIC_ALLOCATION_DECISION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_allocation_decision_record",
        "decision_id": _stable_id(
            "dynamic-allocation-decision",
            decision_date.isoformat(),
            policy.default_policy_id,
            _stable_hash(input_scores),
        ),
        "date": decision_date.isoformat(),
        "policy_id": policy.default_policy_id,
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "source_trend_report": source_trend_report or "",
        "data_quality_status": data_quality_status,
        "input_scores": _round_weights(input_scores),
        "previous_scores": _round_weights(previous_scores or {}),
        "regime_state": {
            "selected_regime": selected_regime,
            "reason": regime_reason,
            "template_rationale": policy.regime_weight_targets[selected_regime].rationale,
        },
        "base_weights": _round_weights(policy.base_weights),
        "regime_template_weights": _round_weights(template),
        "overlay_adjustments": overlay_details,
        "proposed_candidate_weights": _round_weights(proposed),
        "candidate_target_weights": _round_weights(final_weights),
        "pre_rebalance_candidate_weights": _round_weights(constrained),
        "previous_weights": _round_weights(previous),
        "trade_deltas": trade_deltas,
        "turnover_estimate": round(_turnover(final_weights, previous), 10),
        "pre_rebalance_turnover_estimate": round(_turnover(constrained, previous), 10),
        "constraints_applied": constraints_applied,
        "constraint_diagnostics": diagnostics,
        "rebalance_decision": rebalance,
        "reason_codes": sorted(set(reason_codes + list(rebalance["reason_codes"]))),
        "safety": policy.safety.model_dump(mode="json"),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "commands_executed": False,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    _assert_dynamic_allocation_output_safe(record)
    return record


def select_dynamic_regime_state(
    input_scores: dict[str, float],
    policy: DynamicAllocationPolicyConfig,
) -> tuple[str, str]:
    rules = policy.regime_selection_rules
    event_rule = rules["event_risk_high"]
    if _score(input_scores, event_rule.score_id or "EventRiskScore") >= (event_rule.threshold or 0):
        return "event_risk_high", event_rule.rationale
    risk_off = rules["risk_off"]
    if _score(input_scores, "RiskRegimeScore") <= (risk_off.risk_regime_score_max or 0) or _score(
        input_scores, "CompositeTrendScore"
    ) <= (risk_off.composite_trend_score_max or 0):
        return "risk_off", risk_off.rationale
    growth = rules["growth_underperformance"]
    if _score(input_scores, "GrowthLeadershipScore") <= (growth.growth_leadership_score_max or 0):
        return "growth_underperformance", growth.rationale
    semi = rules["semiconductor_leadership"]
    if _score(input_scores, "SemiconductorLeadershipScore") >= (
        semi.semiconductor_leadership_score_min or 100
    ):
        return "semiconductor_leadership", semi.rationale
    risk_on = rules["risk_on"]
    if _score(input_scores, "CompositeTrendScore") >= (
        risk_on.composite_trend_score_min or 100
    ) and _score(input_scores, "RiskRegimeScore") >= (risk_on.risk_regime_score_min or 100):
        return "risk_on", risk_on.rationale
    return "neutral", rules["neutral"].rationale


def apply_trend_overlay_adjustments(
    weights: dict[str, float],
    input_scores: dict[str, float],
    policy: DynamicAllocationPolicyConfig,
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    adjusted = dict(weights)
    overlays: list[dict[str, Any]] = []
    for rule in policy.trend_overlay_rules:
        value = _score(input_scores, rule.score_id)
        if not _comparison_passes(value, rule.comparator, rule.threshold):
            continue
        before = dict(adjusted)
        for symbol, delta in rule.adjustments.items():
            adjusted[symbol] = adjusted.get(symbol, 0.0) + delta
        adjusted = _normalise_weights(adjusted)
        overlays.append(
            {
                "rule_id": rule.rule_id,
                "score_id": rule.score_id,
                "score_value": round(value, 6),
                "threshold": rule.threshold,
                "adjustments": _round_weights(rule.adjustments),
                "before_weights": _round_weights(before),
                "after_weights": _round_weights(adjusted),
                "reason_code": rule.reason_code,
                "rationale": rule.rationale,
            }
        )
    return adjusted, overlays


def apply_event_risk_overlay(
    weights: dict[str, float],
    input_scores: dict[str, float],
    policy: DynamicAllocationPolicyConfig,
) -> tuple[dict[str, float], dict[str, Any] | None]:
    overlay = policy.event_risk_overlay
    if not overlay.enabled:
        return weights, None
    value = _score(input_scores, overlay.score_id)
    if value < overlay.high_threshold:
        return weights, None
    adjusted = dict(weights)
    before = dict(adjusted)
    adjusted["CASH"] = adjusted.get("CASH", 0.0) + overlay.cash_increase
    for symbol, reduction in overlay.reductions.items():
        adjusted[symbol] = adjusted.get(symbol, 0.0) - reduction
    adjusted = _normalise_weights(adjusted)
    return adjusted, {
        "rule_id": "event_risk_overlay",
        "score_id": overlay.score_id,
        "score_value": round(value, 6),
        "threshold": overlay.high_threshold,
        "cash_increase": overlay.cash_increase,
        "reductions": _round_weights(overlay.reductions),
        "before_weights": _round_weights(before),
        "after_weights": _round_weights(adjusted),
        "reason_code": overlay.reason_code,
        "rationale": overlay.rationale,
    }


def apply_dynamic_allocation_constraints(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    policy: DynamicAllocationPolicyConfig,
) -> tuple[dict[str, float], list[str], list[dict[str, Any]]]:
    constrained = _normalise_weights(weights)
    applied: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    constraints = policy.exposure_constraints
    constrained, floor_cap_applied, floor_cap_diagnostics = _apply_asset_floors_caps(
        constrained,
        constraints,
    )
    applied.extend(floor_cap_applied)
    diagnostics.extend(floor_cap_diagnostics)
    constrained, semi_applied, semi_diagnostics = _apply_semiconductor_cap(
        constrained,
        constraints.semiconductor_sleeve_max,
    )
    applied.extend(semi_applied)
    diagnostics.extend(semi_diagnostics)
    constrained, cash_applied, cash_diagnostics = _apply_cash_bounds(constrained, constraints)
    applied.extend(cash_applied)
    diagnostics.extend(cash_diagnostics)
    constrained, rebalance_applied, rebalance_diagnostics = _apply_rebalance_caps(
        constrained,
        previous_weights,
        policy.rebalance_policy,
    )
    applied.extend(rebalance_applied)
    diagnostics.extend(rebalance_diagnostics)
    constrained = _normalise_weights(constrained)
    return constrained, sorted(set(applied)), _dedupe_diagnostics(diagnostics)


def build_rebalance_decision(
    *,
    proposed_weights: dict[str, float],
    previous_weights: dict[str, float],
    input_scores: dict[str, float],
    previous_scores: dict[str, float] | None,
    selected_regime: str,
    policy: DynamicAllocationPolicyConfig,
    days_since_last_rebalance: int | None,
    confirmed_regime_days: int | None,
) -> dict[str, Any]:
    rebalance = policy.rebalance_policy
    reason_codes: list[str] = []
    blockers: list[str] = []
    score_change = _score_change(input_scores, previous_scores)
    if previous_scores is not None and score_change < rebalance.score_change_threshold:
        blockers.append("score_change_below_threshold")
        reason_codes.append("SCORE_CHANGE_BELOW_THRESHOLD")
    days_since = (
        rebalance.minimum_holding_days
        if days_since_last_rebalance is None
        else days_since_last_rebalance
    )
    if days_since < rebalance.minimum_holding_days:
        blockers.append("minimum_holding_period_not_satisfied")
        reason_codes.append("MINIMUM_HOLDING_PERIOD_NOT_SATISFIED")
    confirmed_days = (
        rebalance.regime_confirmation_days
        if confirmed_regime_days is None
        else confirmed_regime_days
    )
    if confirmed_days < rebalance.regime_confirmation_days:
        blockers.append("regime_confirmation_window_not_satisfied")
        reason_codes.append("REGIME_CONFIRMATION_WINDOW_NOT_SATISFIED")
    max_delta = max(
        abs(proposed_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
        for symbol in WEIGHT_SYMBOLS
    )
    turnover = _turnover(proposed_weights, previous_weights)
    if max_delta < rebalance.min_rebalance_weight_delta:
        blockers.append("weight_delta_below_minimum")
        reason_codes.append("WEIGHT_DELTA_BELOW_MINIMUM")
    if turnover > rebalance.weekly_turnover_cap + 1e-8:
        blockers.append("weekly_turnover_cap_exceeded_after_constraints")
        reason_codes.append("WEEKLY_TURNOVER_CAP_EXCEEDED")
    decision = "rebalance_candidate" if not blockers else "hold"
    if decision == "rebalance_candidate":
        reason_codes.append("REBALANCE_POLICY_GATES_PASSED")
    return {
        "decision": decision,
        "selected_regime": selected_regime,
        "blockers": blockers,
        "reason_codes": reason_codes,
        "score_change": round(score_change, 6),
        "score_change_threshold": rebalance.score_change_threshold,
        "days_since_last_rebalance": days_since,
        "minimum_holding_days": rebalance.minimum_holding_days,
        "confirmed_regime_days": confirmed_days,
        "required_regime_confirmation_days": rebalance.regime_confirmation_days,
        "max_weight_delta": round(max_delta, 10),
        "turnover_estimate": round(turnover, 10),
        "weekly_turnover_cap": rebalance.weekly_turnover_cap,
        "min_rebalance_weight_delta": rebalance.min_rebalance_weight_delta,
    }


def build_dynamic_allocation_policy_registry(
    policy: DynamicAllocationPolicyConfig,
    *,
    latest_report_path: str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": DYNAMIC_ALLOCATION_REGISTRY_SCHEMA_VERSION,
        "report_type": "etf_dynamic_allocation_policy_registry",
        "generated_at": datetime.now(UTC).isoformat(),
        "policy_count": 1,
        "policies": [
            {
                "policy_id": policy.default_policy_id,
                "policy_version": policy.policy_metadata.version,
                "config_hash": _stable_hash(policy.model_dump(mode="json")),
                "status": policy.policy_metadata.status,
                "base_weights": _round_weights(policy.base_weights),
                "regime_count": len(policy.regime_weight_targets),
                "trend_overlay_rule_count": len(policy.trend_overlay_rules),
                "source_report_path": latest_report_path or "",
                "safety": policy.safety.model_dump(mode="json"),
            }
        ],
        "safety": policy.safety.model_dump(mode="json"),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "commands_executed": False,
    }
    _assert_dynamic_allocation_output_safe(payload)
    return payload


def build_dynamic_allocation_report(
    *,
    policy: DynamicAllocationPolicyConfig,
    decision_records: list[dict[str, Any]],
    source_trend_report: str | None = None,
) -> dict[str, Any]:
    if not decision_records:
        raise DynamicAllocationError("dynamic allocation report requires at least one decision")
    latest = decision_records[-1]
    summary = {
        "status": "PASS",
        "policy_id": policy.default_policy_id,
        "selected_regime": latest["regime_state"]["selected_regime"],
        "rebalance_decision": latest["rebalance_decision"]["decision"],
        "candidate_target_weights": latest["candidate_target_weights"],
        "constraint_count": len(latest["constraints_applied"]),
        "data_quality_status": latest.get("data_quality_status", "UNKNOWN"),
        "source_trend_report": source_trend_report or "",
    }
    payload = {
        "schema_version": DYNAMIC_ALLOCATION_REPORT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_allocation_report",
        "dynamic_allocation_report_id": _stable_id(
            "dynamic-allocation-report",
            latest["date"],
            policy.default_policy_id,
            _stable_hash(latest),
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "PASS",
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "requested_decision_date": latest["date"],
        "source_trend_report": source_trend_report or "",
        "summary": summary,
        "policy_summary": {
            "policy_id": policy.default_policy_id,
            "base_weights": _round_weights(policy.base_weights),
            "regime_weight_targets": {
                regime: {
                    "weights": _round_weights(target.weights),
                    "rationale": target.rationale,
                }
                for regime, target in policy.regime_weight_targets.items()
            },
            "trend_overlay_rules": [
                rule.model_dump(mode="json") for rule in policy.trend_overlay_rules
            ],
            "event_risk_overlay": policy.event_risk_overlay.model_dump(mode="json"),
            "exposure_constraints": policy.exposure_constraints.model_dump(mode="json"),
            "rebalance_policy": policy.rebalance_policy.model_dump(mode="json"),
        },
        "decision_records": decision_records,
        "sample_decision_count": len(decision_records),
        "candidate_target_weights": latest["candidate_target_weights"],
        "constraints_applied": latest["constraints_applied"],
        "constraint_diagnostics": latest["constraint_diagnostics"],
        "reason_codes": latest["reason_codes"],
        "safety": policy.safety.model_dump(mode="json"),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "commands_executed": False,
    }
    _assert_dynamic_allocation_output_safe(payload)
    return payload


def build_dynamic_allocation_validation_report(
    *,
    policy_config_path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    generated = datetime.now(UTC)
    policy: DynamicAllocationPolicyConfig | None = None
    try:
        policy = load_dynamic_allocation_policy_config(policy_config_path)
        _append_check(checks, "policy_config_valid", True, "dynamic allocation policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "policy_config_valid", False, str(exc))
    sample_records: list[dict[str, Any]] = []
    if policy is not None:
        try:
            for profile_id, scores in policy.sample_score_profiles.items():
                record = build_dynamic_allocation_decision_record(
                    policy=policy,
                    decision_date=max(
                        policy.market_regime.default_decision_start,
                        date(2024, 1, 2),
                    ),
                    input_scores=dict(scores),
                    previous_weights=policy.base_weights,
                    previous_scores=policy.sample_score_profiles.get("neutral"),
                    source_trend_report=f"validation_profile:{profile_id}",
                    data_quality_status="VALIDATION_SAMPLE",
                )
                sample_records.append(record)
            _append_check(
                checks,
                "decision_engine_available",
                True,
                f"built {len(sample_records)} sample dynamic allocation decisions",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "decision_engine_available", False, str(exc))
        try:
            report = build_dynamic_allocation_report(
                policy=policy,
                decision_records=sample_records[:1]
                or [
                    build_dynamic_allocation_decision_record(
                        policy=policy,
                        decision_date=date(2024, 1, 2),
                        input_scores=policy.sample_score_profiles["neutral"],
                    )
                ],
            )
            _append_check(
                checks,
                "report_generator_available",
                report.get("status") == "PASS",
                "dynamic allocation report generated",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "report_generator_available", False, str(exc))
        try:
            registry = build_dynamic_allocation_policy_registry(policy)
            _append_check(
                checks,
                "policy_registry_available",
                registry.get("policy_count") == 1,
                "dynamic allocation policy registry generated",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "policy_registry_available", False, str(exc))
    registry_text = (PROJECT_ROOT / "config" / "report_registry.yaml").read_text(encoding="utf-8")
    _append_check(
        checks,
        "report_registry_visibility",
        "etf_dynamic_allocation_report" in registry_text
        and "etf_dynamic_allocation_validation" in registry_text,
        "report registry includes dynamic allocation report and validation",
    )
    reader_brief_text = (
        PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "reader_brief.py"
    ).read_text(encoding="utf-8")
    _append_check(
        checks,
        "reader_brief_visibility",
        "Dynamic Allocation Candidate" in reader_brief_text
        and "_etf_dynamic_allocation_summary" in reader_brief_text,
        "Reader Brief includes dynamic allocation section",
    )
    if policy is not None:
        safety_payload = {
            **SAFETY_FIELDS,
            "safety": policy.safety.model_dump(mode="json"),
            "commands_executed": False,
        }
        try:
            _assert_dynamic_allocation_output_safe(safety_payload)
            safety_ok = True
            safety_detail = "dynamic allocation safety boundary preserved"
        except Exception as exc:  # noqa: BLE001
            safety_ok = False
            safety_detail = str(exc)
        _append_check(checks, "safety_boundary", safety_ok, safety_detail)
    failed = [check for check in checks if not check["passed"]]
    payload = {
        "schema_version": DYNAMIC_ALLOCATION_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_allocation_validation",
        "validation_id": _stable_id("dynamic-allocation-validation", generated.date().isoformat()),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "checks": checks,
        "failed_check_count": len(failed),
        "source_schema_versions": {
            "policy": DYNAMIC_ALLOCATION_POLICY_SCHEMA_VERSION,
            "decision": DYNAMIC_ALLOCATION_DECISION_SCHEMA_VERSION,
            "report": DYNAMIC_ALLOCATION_REPORT_SCHEMA_VERSION,
            "registry": DYNAMIC_ALLOCATION_REGISTRY_SCHEMA_VERSION,
        },
        "safety": SAFETY_FIELDS,
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "commands_executed": False,
        "production_weight_update_blocked": True,
        "broker_order_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "official_target_weights_write_blocked": True,
    }
    _assert_dynamic_allocation_output_safe(payload)
    return payload


def write_dynamic_allocation_decision_record(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_ALLOCATION_DECISION_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    decision_id = str(payload["decision_id"])
    json_path = output_dir / f"{decision_id}.json"
    markdown_path = output_dir / f"{decision_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_allocation_decision_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_allocation_report(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_id = str(payload["dynamic_allocation_report_id"])
    json_path = output_dir / f"{report_id}.json"
    markdown_path = output_dir / f"{report_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_allocation_report_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_allocation_policy_registry(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_ALLOCATION_REGISTRY_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "etf_dynamic_allocation_policy_registry.json"
    markdown_path = output_dir / "etf_dynamic_allocation_policy_registry.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_allocation_registry_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_allocation_validation_report(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_ALLOCATION_VALIDATION_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_id = str(payload["validation_id"])
    json_path = output_dir / f"{validation_id}.json"
    markdown_path = output_dir / f"{validation_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_allocation_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def latest_dynamic_allocation_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR,
) -> Path | None:
    return _latest_file(report_dir, "dynamic-allocation-report_*.json")


def render_dynamic_allocation_decision_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Dynamic Allocation Decision {payload.get('decision_id')}",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "- official_target_weights_mutated=false",
        "",
        "## Decision",
        f"- Date: {payload.get('date')}",
        f"- Policy: {payload.get('policy_id')}",
        f"- Regime: {_mapping(payload.get('regime_state')).get('selected_regime')}",
        f"- Rebalance: {_mapping(payload.get('rebalance_decision')).get('decision')}",
        f"- Turnover estimate: {payload.get('turnover_estimate')}",
        "",
        "## Candidate Target Weights",
        _markdown_mapping(payload.get("candidate_target_weights")),
        "",
        "## Constraints",
        _markdown_list(payload.get("constraints_applied")),
    ]
    return "\n".join(lines) + "\n"


def render_dynamic_allocation_report_markdown(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Dynamic Allocation Report {payload.get('dynamic_allocation_report_id')}",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "- official_target_weights_mutated=false",
        "",
        "## Summary",
        f"- Status: {payload.get('status')}",
        f"- Policy: {summary.get('policy_id')}",
        f"- Selected regime: {summary.get('selected_regime')}",
        f"- Rebalance decision: {summary.get('rebalance_decision')}",
        f"- Data quality status: {summary.get('data_quality_status')}",
        f"- Source trend report: {summary.get('source_trend_report')}",
        "",
        "## Candidate Target Weights",
        _markdown_mapping(summary.get("candidate_target_weights")),
        "",
        "## Constraints Applied",
        _markdown_list(payload.get("constraints_applied")),
        "",
        "## Reason Codes",
        _markdown_list(payload.get("reason_codes")),
    ]
    return "\n".join(lines) + "\n"


def render_dynamic_allocation_registry_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Dynamic Allocation Policy Registry",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "",
    ]
    for policy in payload.get("policies", []):
        mapping = _mapping(policy)
        lines.extend(
            [
                f"## {mapping.get('policy_id')}",
                f"- Version: {mapping.get('policy_version')}",
                f"- Status: {mapping.get('status')}",
                f"- Config hash: {mapping.get('config_hash')}",
                f"- Regimes: {mapping.get('regime_count')}",
                f"- Overlay rules: {mapping.get('trend_overlay_rule_count')}",
                "",
            ]
        )
    return "\n".join(lines)


def render_dynamic_allocation_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Dynamic Allocation Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Failed checks: {payload.get('failed_check_count')}",
        "- production_effect=none",
        "- broker_action=none",
        "- official_target_weights_write_blocked=true",
        "",
        "## Checks",
    ]
    for check in payload.get("checks", []):
        mapping = _mapping(check)
        status = "PASS" if mapping.get("passed") else "FAIL"
        lines.append(f"- {status}: {mapping.get('check_id')} - {mapping.get('detail')}")
    return "\n".join(lines) + "\n"


def _apply_asset_floors_caps(
    weights: dict[str, float],
    constraints: ExposureConstraintsConfig,
) -> tuple[dict[str, float], list[str], list[dict[str, Any]]]:
    adjusted = dict(weights)
    applied: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    for symbol in WEIGHT_SYMBOLS:
        before = adjusted.get(symbol, 0.0)
        after = min(max(before, constraints.asset_floors[symbol]), constraints.asset_caps[symbol])
        if abs(after - before) <= 1e-12:
            continue
        adjusted[symbol] = after
        applied.append(f"{symbol}_ASSET_FLOOR_CAP")
        diagnostics.append(
            _constraint_diagnostic(
                "asset_floor_cap",
                symbol,
                before,
                after,
                f"Configured asset floor/cap applied for {symbol}.",
            )
        )
    adjusted = _normalise_weights(adjusted)
    return adjusted, applied, diagnostics


def _apply_semiconductor_cap(
    weights: dict[str, float],
    cap: float,
) -> tuple[dict[str, float], list[str], list[dict[str, Any]]]:
    adjusted = dict(weights)
    before = adjusted.get("SMH", 0.0) + adjusted.get("SOXX", 0.0)
    if before <= cap + 1e-12:
        return weights, [], []
    factor = cap / before if before > 0 else 0
    adjusted["SMH"] = adjusted.get("SMH", 0.0) * factor
    adjusted["SOXX"] = adjusted.get("SOXX", 0.0) * factor
    adjusted["CASH"] = adjusted.get("CASH", 0.0) + (before - cap)
    adjusted = _normalise_weights(adjusted)
    after = adjusted.get("SMH", 0.0) + adjusted.get("SOXX", 0.0)
    return (
        adjusted,
        ["SEMICONDUCTOR_SLEEVE_CAP"],
        [
            _constraint_diagnostic(
                "semiconductor_sleeve_cap",
                "SMH+SOXX",
                before,
                after,
                "Configured semiconductor sleeve cap applied.",
            )
        ],
    )


def _apply_cash_bounds(
    weights: dict[str, float],
    constraints: ExposureConstraintsConfig,
) -> tuple[dict[str, float], list[str], list[dict[str, Any]]]:
    adjusted = dict(weights)
    applied: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    cash = adjusted.get("CASH", 0.0)
    if cash < constraints.cash_min - 1e-12:
        before = cash
        needed = constraints.cash_min - cash
        _reduce_non_cash_for_cash(adjusted, needed)
        applied.append("CASH_MIN")
        diagnostics.append(
            _constraint_diagnostic(
                "cash_min",
                "CASH",
                before,
                adjusted.get("CASH", 0.0),
                "Configured cash minimum applied.",
            )
        )
    cash = adjusted.get("CASH", 0.0)
    if cash > constraints.cash_max + 1e-12:
        before = cash
        excess = cash - constraints.cash_max
        adjusted["CASH"] = constraints.cash_max
        adjusted["SPY"] = adjusted.get("SPY", 0.0) + excess
        applied.append("CASH_MAX")
        diagnostics.append(
            _constraint_diagnostic(
                "cash_max",
                "CASH",
                before,
                constraints.cash_max,
                "Configured cash maximum applied; excess allocated to SPY candidate sleeve.",
            )
        )
    adjusted = _normalise_weights(adjusted)
    return adjusted, applied, diagnostics


def _apply_rebalance_caps(
    weights: dict[str, float],
    previous: dict[str, float],
    rebalance_policy: RebalancePolicyConfig,
) -> tuple[dict[str, float], list[str], list[dict[str, Any]]]:
    adjusted = dict(weights)
    diagnostics: list[dict[str, Any]] = []
    for symbol in WEIGHT_SYMBOLS:
        before = adjusted.get(symbol, 0.0)
        prior = previous.get(symbol, 0.0)
        delta = before - prior
        clipped_delta = max(
            -rebalance_policy.max_single_rebalance_delta,
            min(rebalance_policy.max_single_rebalance_delta, delta),
        )
        if abs(clipped_delta - delta) <= 1e-12:
            continue
        adjusted[symbol] = prior + clipped_delta
        diagnostics.append(
            _constraint_diagnostic(
                "max_single_rebalance_delta",
                symbol,
                before,
                adjusted[symbol],
                "Configured maximum single rebalance delta applied.",
            )
        )
    adjusted = _normalise_weights(adjusted)
    turnover = _turnover(adjusted, previous)
    if turnover <= rebalance_policy.weekly_turnover_cap + 1e-12:
        return adjusted, ["MAX_SINGLE_REBALANCE_DELTA"] if diagnostics else [], diagnostics
    before_turnover = turnover
    factor = rebalance_policy.weekly_turnover_cap / turnover if turnover > 0 else 0
    for symbol in WEIGHT_SYMBOLS:
        adjusted[symbol] = (
            previous.get(symbol, 0.0)
            + (adjusted.get(symbol, 0.0) - previous.get(symbol, 0.0)) * factor
        )
    adjusted = _normalise_weights(adjusted)
    diagnostics.append(
        _constraint_diagnostic(
            "weekly_turnover_cap",
            "portfolio",
            before_turnover,
            _turnover(adjusted, previous),
            "Configured weekly turnover cap applied.",
        )
    )
    applied = ["WEEKLY_TURNOVER_CAP"]
    if len(diagnostics) > 1:
        applied.append("MAX_SINGLE_REBALANCE_DELTA")
    return adjusted, sorted(set(applied)), diagnostics


def _validate_weight_map(weights: dict[str, float], *, context: str) -> None:
    if set(weights) != set(WEIGHT_SYMBOLS):
        raise ValueError(f"{context} must define SPY/QQQ/SMH/SOXX/CASH")
    for symbol, value in weights.items():
        if not isinstance(value, int | float) or value < 0 or value > 1:
            raise ValueError(f"{context} invalid weight for {symbol}")
    if abs(sum(weights.values()) - 1.0) > 1e-6:
        raise ValueError(f"{context} weights must sum to 1.0")


def _validate_scores(input_scores: dict[str, float]) -> None:
    required = {
        "CompositeTrendScore",
        "RiskRegimeScore",
        "GrowthLeadershipScore",
        "SemiconductorLeadershipScore",
        "EventRiskAdjustedTrendScore",
        "EventRiskScore",
    }
    missing = sorted(required - set(input_scores))
    if missing:
        raise DynamicAllocationError(
            "dynamic allocation missing input scores: " + ", ".join(missing)
        )
    for key, value in input_scores.items():
        if not isinstance(value, int | float) or value < 0 or value > 100:
            raise DynamicAllocationError(f"dynamic allocation score {key} must be in 0..100")


def _normalise_weights(weights: dict[str, float]) -> dict[str, float]:
    adjusted = {symbol: max(0.0, float(weights.get(symbol, 0.0))) for symbol in WEIGHT_SYMBOLS}
    total = sum(adjusted.values())
    if total <= 0:
        raise DynamicAllocationError("dynamic allocation weights cannot sum to zero")
    adjusted = {symbol: value / total for symbol, value in adjusted.items()}
    drift = 1.0 - sum(adjusted.values())
    adjusted["CASH"] += drift
    return adjusted


def _reduce_non_cash_for_cash(weights: dict[str, float], amount: float) -> None:
    non_cash = ("SPY", "QQQ", "SMH", "SOXX")
    total = sum(weights.get(symbol, 0.0) for symbol in non_cash)
    if total <= 0:
        weights["CASH"] = weights.get("CASH", 0.0) + amount
        return
    for symbol in non_cash:
        reduction = amount * (weights.get(symbol, 0.0) / total)
        weights[symbol] = max(0.0, weights.get(symbol, 0.0) - reduction)
    weights["CASH"] = weights.get("CASH", 0.0) + amount


def _comparison_passes(value: float, comparator: str, threshold: float) -> bool:
    if comparator == ">=":
        return value >= threshold
    if comparator == ">":
        return value > threshold
    if comparator == "<":
        return value < threshold
    if comparator == "<=":
        return value <= threshold
    return False


def _score(scores: dict[str, float], score_id: str) -> float:
    return float(scores.get(score_id, 50.0))


def _score_change(
    scores: dict[str, float],
    previous_scores: dict[str, float] | None,
) -> float:
    if not previous_scores:
        return 100.0
    keys = set(scores) | set(previous_scores)
    return max(
        abs(float(scores.get(key, 50.0)) - float(previous_scores.get(key, 50.0))) for key in keys
    )


def _turnover(weights: dict[str, float], previous: dict[str, float]) -> float:
    return sum(
        abs(weights.get(symbol, 0.0) - previous.get(symbol, 0.0)) for symbol in WEIGHT_SYMBOLS
    )


def _constraint_diagnostic(
    constraint_id: str,
    asset_or_sleeve: str,
    before: float,
    after: float,
    reason: str,
) -> dict[str, Any]:
    return {
        "constraint_id": constraint_id,
        "asset_or_sleeve": asset_or_sleeve,
        "before_weight": round(float(before), 10),
        "after_weight": round(float(after), 10),
        "reason": reason,
        "severity": "info",
    }


def _dedupe_diagnostics(diagnostics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, Any, Any, Any]] = set()
    deduped: list[dict[str, Any]] = []
    for item in diagnostics:
        key = (
            item.get("constraint_id"),
            item.get("asset_or_sleeve"),
            item.get("before_weight"),
            item.get("after_weight"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _assert_dynamic_allocation_output_safe(payload: dict[str, Any]) -> None:
    for forbidden in FORBIDDEN_OUTPUT_KEYS:
        if forbidden in payload:
            raise DynamicAllocationError(
                f"dynamic allocation output contains forbidden key: {forbidden}"
            )
    safety = _mapping(payload.get("safety"))
    for key, expected in SAFETY_FIELDS.items():
        if key in safety and safety.get(key) != expected:
            raise DynamicAllocationError(f"dynamic allocation safety field {key} is unsafe")
        if key in payload and payload.get(key) != expected:
            raise DynamicAllocationError(f"dynamic allocation output field {key} is unsafe")


def _stable_hash(payload: Any) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    digest = _stable_hash({"prefix": prefix, "parts": parts})[:12]
    return f"{prefix}_{digest}"


def _round_weights(weights: dict[str, float]) -> dict[str, float]:
    return {str(key): round(float(value), 10) for key, value in weights.items()}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, detail: str) -> None:
    checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})


def _markdown_mapping(value: Any) -> str:
    mapping = _mapping(value)
    if not mapping:
        return "- none"
    return "\n".join(f"- {key}: {val}" for key, val in mapping.items())


def _markdown_list(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- none"
    return "\n".join(f"- {item}" for item in value)


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.write_text(text, encoding="utf-8")


def _latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    matches = sorted(root.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0] if matches else None
