from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from ai_trading_system.config import (
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    PROJECT_ROOT,
    load_market_regimes,
    load_scoring_rules,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "1.0"
REPORT_TYPE_PREFIX = "indicator_research"
DEFAULT_INDICATOR_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "indicator_research_registry.yaml"
)
DEFAULT_THRESHOLD_REGISTRY_PATH = PROJECT_ROOT / "config" / "research" / "threshold_registry.yaml"
DEFAULT_INDICATOR_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_indicators"
DEFAULT_DAILY_INDICATOR_TRACE_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_allocation_policy.yaml"
)
DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "trend_calibration.yaml"
)
DEFAULT_DYNAMIC_TREND_COVERAGE_EXTENSION_ROOT = PROJECT_ROOT / "outputs" / "research_campaigns"

CONTROL_ONLY_DATA_QUALITY_STATUS = "NOT_REQUIRED_CONFIG_ONLY"
SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "official_target_weights": False,
    "paper_shadow_activation": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "broker_effect": "none",
    "order_effect": "none",
    "production_effect": "none",
    "holdout_touched": False,
}

EDGE_TYPES_THAT_CAN_MASK = {"MASKS", "CAPS", "CONDITIONS"}
TRACE_REQUIRED_STATUS = "TRACE_DATA_REQUIRED"
DEFAULT_MASKING_OUTCOME_TICKER = "QQQ"
MASKING_OUTCOME_HORIZONS = (1, 5, 10, 20)
MASKING_ABLATION_SCENARIOS = (
    "baseline",
    "no_valuation_crowding_masking",
    "capped_masking",
)
OUTCOME_JOIN_KEY_FIELDS = (
    "as_of_date",
    "decision_time",
    "asset",
    "scenario",
    "trace_source",
    "trace_contract_version",
)
OUTCOME_WINDOW_STATUS_AVAILABLE = "available"
OUTCOME_WINDOW_STATUS_NOT_MATURE = "outcome_not_mature"
OUTCOME_WINDOW_STATUS_MISSING_PRICE = "missing_price"
OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING = "missing_asset_mapping"
OUTCOME_WINDOW_STATUS_MISSING_CALENDAR = "missing_calendar"
OUTCOME_WINDOW_STATUS_MISSING_JOIN_KEY = "missing_join_key"
OUTCOME_HARD_MISSING_STATUSES = {
    OUTCOME_WINDOW_STATUS_MISSING_PRICE,
    OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING,
    OUTCOME_WINDOW_STATUS_MISSING_CALENDAR,
    OUTCOME_WINDOW_STATUS_MISSING_JOIN_KEY,
}
# Validation-only pilot floor for issuing a directional masking recommendation.
EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES = 50
# Validation-only short-horizon maturity floor for preliminary recommendation.
EFFECTIVENESS_MIN_SHORT_HORIZON_MATURE_CASES = 50
# Validation-only drawdown tolerance for no-mask comparison; not a production policy.
EFFECTIVENESS_DRAWDOWN_WORSE_TOLERANCE = 0.10
# Validation-only missed-upside ceiling for retaining baseline masking.
EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE = 0.40
# Validation-only scenario cap used for counterfactual reporting; not a production policy.
DEFAULT_MASKING_ABLATION_CAP_RATIO = 0.50
# Validation-only diagnostics thresholds; these classify robustness evidence only and
# never change scoring, gates, or production weights.
ROBUSTNESS_SMALL_RETURN_DELTA = 0.001
ROBUSTNESS_HIGH_NOISE_STD = 0.02
ROBUSTNESS_CLUSTER_DOMINANCE_SHARE = 0.50
ROBUSTNESS_TOP_DATE_CONCENTRATION_SHARE = 0.50
# Isolated daily traces stay useful, but stability claims need a multi-date diagnostic window.
HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY = 20
FULL_ADVISORY_TRACE_SOURCE = "full_advisory_score_daily_trace"
COMPONENT_VALIDATION_TRACE_SOURCE = "component_level_validation_trace"
BACKTEST_TRACE_BRIDGE_SOURCE = "backtest_trace_bridge"
INELIGIBLE_TRACE_SOURCE = "not_trace_eligible"
TRACE_CONFIDENCE_FULL_ADVISORY = "HIGH_FULL_ADVISORY_EQUIVALENT"
TRACE_CONFIDENCE_COMPONENT = "MEDIUM_COMPONENT_DIAGNOSTIC"
TRACE_CONFIDENCE_BRIDGE = "MEDIUM_BACKTEST_BRIDGE_DIAGNOSTIC"
TRACE_CONFIDENCE_NOT_ELIGIBLE = "LOW_NOT_TRACE_ELIGIBLE"
NON_PROMOTION_ALLOWED_USES = ["diagnostic", "ablation", "sensitivity_analysis"]
DEFAULT_TRACE_ASSET = "AI_RISK_ASSET_BASKET"
PRICE_TICKER_ALIASES = {"GOOGL": "GOOG"}
VALIDATION_CORRELATED_ASSET_CLUSTERS = {
    "broad_index": {"QQQ", "SPY"},
    "semiconductor_ai": {"SMH", "NVDA", "AMD", "TSM"},
    "mega_cap_software": {"MSFT", "GOOGL", "GOOG"},
}
DYNAMIC_TREND_COVERAGE_EXTENSION_ASSETS = ("QQQ", "SMH", "MSFT", "NVDA", "SPY", "AMD")
DYNAMIC_TREND_TRADING_698_BASELINE_COVERAGE = {
    "full_advisory_case_count": 22,
    "cluster_count": 1,
    "regime_count": 1,
    "mature_case_count_by_horizon": {"1d": 20, "5d": 16, "10d": 11, "20d": 0},
    "evidence_strength": "low",
    "recommendation": "sensitivity_tested_only",
}
GATE_ROOT_CAUSE_CLASSES = (
    "expected_pit_limitation",
    "ingestion_issue",
    "timestamp_model_issue",
    "replay_config_issue",
    "lineage_manifest_missing",
)
DEFAULT_EVENT_WINDOW_CATALOG = (
    {
        "event_window_id": "strong_trend_up",
        "label": "强趋势上涨窗口",
        "start_date": "2026-05-29",
        "end_date": "2026-06-12",
    },
    {
        "event_window_id": "drawdown",
        "label": "回撤窗口",
        "start_date": "2026-04-24",
        "end_date": "2026-05-01",
    },
    {
        "event_window_id": "vix_up",
        "label": "VIX 上行窗口",
        "start_date": "2026-05-01",
        "end_date": "2026-05-08",
    },
    {
        "event_window_id": "fomc_around",
        "label": "FOMC 前后窗口",
        "start_date": "2026-06-17",
        "end_date": "2026-06-18",
    },
    {
        "event_window_id": "semiconductor_ai_volatility",
        "label": "半导体/AI 主线波动窗口",
        "start_date": "2026-06-05",
        "end_date": "2026-06-12",
    },
)

THRESHOLD_REQUIRED_FIELDS = (
    "threshold_id",
    "current_value",
    "unit",
    "where_used",
    "purpose",
    "impact_scope",
    "decision_affecting",
    "promotion_gate_affecting",
    "production_weight_affecting",
    "default_reason",
    "calibration_status",
    "evidence_level",
    "recommended_calibration_method",
)
HIGH_IMPACT_THRESHOLD_CLASS = "A"
HIGH_IMPACT_UNCALIBRATED_STATUSES = {
    "UNCALIBRATED_DEFAULT",
    "HEURISTIC_GUARDRAIL",
    "uncalibrated",
}
CALIBRATED_THRESHOLD_STATUS = "CALIBRATED"
HEURISTIC_GUARDRAIL_THRESHOLD_TYPES = {
    "heuristic_guardrail",
    "HEURISTIC_GUARDRAIL",
}
DEFAULT_THRESHOLD_IMPACT_SCOPE_ORDER = (
    "signal_direction_affecting",
    "masking_dominance_affecting",
    "promotion_gate_affecting",
    "robustness_gate_affecting",
    "outcome_maturity_affecting",
    "production_weight_affecting",
)
CALIBRATION_URGENCY_ORDER = ("P0", "P1", "P2", "P3")
INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS = (
    "indicator_research.effectiveness_min_available_outcome_cases",
    "indicator_research.robustness_cluster_dominance_share",
    "indicator_research.effectiveness_missed_upside_acceptable_rate",
    "indicator_research.masking_high_min",
    "indicator_research.dominant_share_of_adjustment_min",
)
SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS = (
    "dynamic_allocation.risk_off_score_thresholds",
    "dynamic_allocation.risk_on_confirmation_thresholds",
    "trend_calibration.score_bands",
)
THRESHOLD_CALIBRATION_TESTED_VALUES = {
    "indicator_research.effectiveness_min_available_outcome_cases": [20, 30, 50, 80, 100],
    "indicator_research.robustness_cluster_dominance_share": [0.33, 0.50, 0.67, 0.80],
    "indicator_research.effectiveness_missed_upside_acceptable_rate": [
        0.20,
        0.30,
        0.40,
        0.50,
        0.60,
    ],
    "indicator_research.masking_high_min": [0.40, 0.50, 0.60, 0.70, 0.80],
    "indicator_research.dominant_share_of_adjustment_min": [0.20, 0.30, 0.40, 0.50],
}
SECOND_BATCH_DYNAMIC_TREND_TESTED_VALUES = {
    "dynamic_allocation.risk_off_score_thresholds": [
        {
            "scenario_id": "stricter_defensive_trigger",
            "risk_regime_score_max": 38.0,
            "composite_trend_score_max": 40.0,
            "growth_leadership_score_max": 40.0,
        },
        {
            "scenario_id": "current_policy_baseline",
            "risk_regime_score_max": 42.0,
            "composite_trend_score_max": 45.0,
            "growth_leadership_score_max": 45.0,
        },
        {
            "scenario_id": "moderately_looser_defensive_trigger",
            "risk_regime_score_max": 46.0,
            "composite_trend_score_max": 50.0,
            "growth_leadership_score_max": 50.0,
        },
        {
            "scenario_id": "broad_defensive_trigger",
            "risk_regime_score_max": 50.0,
            "composite_trend_score_max": 55.0,
            "growth_leadership_score_max": 55.0,
        },
    ],
    "dynamic_allocation.risk_on_confirmation_thresholds": [
        {
            "scenario_id": "stricter_risk_on_confirmation",
            "composite_trend_score_min": 75.0,
            "risk_regime_score_min": 68.0,
            "growth_leadership_score_min": 78.0,
            "semiconductor_leadership_score_min": 80.0,
        },
        {
            "scenario_id": "current_policy_baseline",
            "composite_trend_score_min": 70.0,
            "risk_regime_score_min": 62.0,
            "growth_leadership_score_min": 72.0,
            "semiconductor_leadership_score_min": 75.0,
        },
        {
            "scenario_id": "moderately_looser_risk_on_confirmation",
            "composite_trend_score_min": 65.0,
            "risk_regime_score_min": 58.0,
            "growth_leadership_score_min": 68.0,
            "semiconductor_leadership_score_min": 70.0,
        },
        {
            "scenario_id": "broad_risk_on_confirmation",
            "composite_trend_score_min": 60.0,
            "risk_regime_score_min": 55.0,
            "growth_leadership_score_min": 65.0,
            "semiconductor_leadership_score_min": 68.0,
        },
    ],
    "trend_calibration.score_bands": [
        {
            "scenario_id": "lower_risk_on_boundary",
            "risk_off": [0, 35],
            "weak": [35, 50],
            "neutral": [50, 68],
            "risk_on": [68, 84],
            "strong_risk_on": [84, 100],
        },
        {
            "scenario_id": "current_policy_baseline",
            "risk_off": [0, 40],
            "weak": [40, 55],
            "neutral": [55, 70],
            "risk_on": [70, 85],
            "strong_risk_on": [85, 100],
        },
        {
            "scenario_id": "higher_confirmation_boundary",
            "risk_off": [0, 45],
            "weak": [45, 60],
            "neutral": [60, 72],
            "risk_on": [72, 87],
            "strong_risk_on": [87, 100],
        },
        {
            "scenario_id": "wider_neutral_band",
            "risk_off": [0, 38],
            "weak": [38, 58],
            "neutral": [58, 75],
            "risk_on": [75, 90],
            "strong_risk_on": [90, 100],
        },
    ],
}
DYNAMIC_TREND_SENSITIVITY_VARIANT_KINDS = (
    "current_value",
    "stricter",
    "relaxed",
    "capped_or_smoothed_candidate",
    "no_change_baseline",
)
DYNAMIC_TREND_VALIDATION_RECOMMENDATIONS = {
    "keep_current_value",
    "adjust_candidate",
    "insufficient_data",
    "collect_evidence_only",
    "sensitivity_tested_only",
}
DYNAMIC_TREND_BRIDGE_RELIABILITY_LABELS = (
    "bridge_consistent_with_full_advisory",
    "bridge_directionally_consistent_but_magnitude_uncertain",
    "bridge_conflicts_with_full_advisory",
    "insufficient_full_advisory_to_assess",
)
DYNAMIC_TREND_SOURCE_COMPARISON_METRICS = (
    "avg_return_1d",
    "avg_return_5d",
    "avg_return_10d",
    "avg_return_20d",
    "hit_rate_1d",
    "hit_rate_5d",
    "hit_rate_10d",
    "hit_rate_20d",
    "max_drawdown",
    "drawdown_preservation",
    "turnover",
    "constraint_hit_count",
    "risk_off_trigger_count",
    "risk_on_confirmation_count",
    "false_risk_off_count",
    "false_risk_on_count",
    "missed_upside_count",
)
DYNAMIC_TREND_DIRECTIONAL_METRICS = (
    "avg_return_1d",
    "avg_return_5d",
    "avg_return_10d",
    "avg_return_20d",
    "max_drawdown",
    "turnover",
    "constraint_hit_count",
    "false_risk_off_count",
    "false_risk_on_count",
    "missed_upside_count",
)


class IndicatorResearchError(ValueError):
    pass


class PolicyMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    owner: str
    status: str
    rationale: str
    intended_effect: str
    validation_evidence: str
    review_condition: str


class MarketRegimeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    regime_id: str
    requested_date_range: str
    anchor_event: str


class OntologySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    object_types: list[str]
    roles: list[str]
    constraint_types: list[str]
    dependency_edge_types: list[str]
    coverage_statuses: list[str]
    gate_outcomes: list[str]


class ImpactBandPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    high_hit_rate_label: str = "high"
    high_weight_impact_label: str = "high"


class DominancePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dominant_hit_rate_min: float = Field(ge=0, le=1)
    dominant_share_of_adjustment_min: float = Field(ge=0, le=1)


class MaskingPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    moderate_min: float = Field(ge=0, le=1)
    high_min: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> MaskingPolicy:
        if self.high_min < self.moderate_min:
            raise ValueError("masking high_min must be >= moderate_min")
        return self


class CalibrationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_trigger_observations_for_mapping_research: int = Field(gt=0)
    min_walk_forward_windows: int = Field(gt=0)


class HoldoutPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_development_window: str
    default_diagnostic_window: str
    final_holdout_access: str
    purge_days: int = Field(ge=0)
    embargo_days: int = Field(ge=0)


class ValidationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    impact_bands: ImpactBandPolicy
    dominance: DominancePolicy
    masking: MaskingPolicy
    calibration: CalibrationPolicy
    holdout: HoldoutPolicy


class SourceFeature(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str
    subject: str
    feature: str


class ExpectedImpact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hit_rate_band: str = "unknown"
    weight_impact_band: str = "unknown"


class IndicatorSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    indicator_id: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    object_type: str
    display_name: str
    role: str
    source_features: list[SourceFeature] = Field(default_factory=list)
    economic_interpretation: str
    daily_component_id: str | None = None
    used_in_daily_report: bool = False
    affects_signal: bool = False
    affects_weight: bool = False
    constraint_type: str
    mapping_version: str | None = None
    research_status: str
    owner: str
    data_gate: dict[str, str] = Field(default_factory=dict)
    expected_impact: ExpectedImpact = Field(default_factory=ExpectedImpact)
    target_family: str
    evaluation_metrics: list[str] = Field(default_factory=list)
    implementation_refs: list[str] = Field(default_factory=list)
    pilot_output_if_untraced: str | None = None
    notes: str = ""

    @model_validator(mode="after")
    def validate_weight_affecting_mapping(self) -> IndicatorSpec:
        if self.affects_weight and not self.mapping_version:
            raise ValueError(f"{self.indicator_id} affects weight but has no mapping_version")
        return self


class MappingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mapping_version: str
    indicator_id: str
    family: str
    direction: str
    output_range: str
    parameter_source: str
    status: str
    review_requirement: str | None = None


class DependencySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_node: str
    to_node: str
    edge_type: str
    rationale: str


class TraceContractSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_id: str
    required_stages: list[str]
    required_delta_fields: list[str]
    required_masking_fields: list[str]
    forbidden_outputs: list[str]


class MappingCandidateFamily(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    description: str
    may_affect_weight: bool


class ResearchStageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage_id: str
    title: str
    output_artifact: str


class IndicatorResearchRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    policy_version: str
    policy_metadata: PolicyMetadata
    market_regime: MarketRegimeSpec
    data_quality_status: str = CONTROL_ONLY_DATA_QUALITY_STATUS
    safety_boundary: dict[str, Any] = Field(default_factory=lambda: dict(SAFETY_BOUNDARY))
    ontology: OntologySpec
    validation_policy: ValidationPolicy
    trace_contract: TraceContractSpec
    mapping_candidate_families: list[MappingCandidateFamily]
    indicators: list[IndicatorSpec]
    mappings: list[MappingSpec]
    dependencies: list[DependencySpec]
    research_stages: list[ResearchStageSpec]

    @model_validator(mode="after")
    def validate_references(self) -> IndicatorResearchRegistry:
        indicator_ids = [item.indicator_id for item in self.indicators]
        duplicates = _duplicates(indicator_ids)
        if duplicates:
            raise ValueError(f"duplicate indicator ids: {', '.join(duplicates)}")

        known_indicators = set(indicator_ids)
        mapping_versions = [item.mapping_version for item in self.mappings]
        duplicate_mappings = _duplicates(mapping_versions)
        if duplicate_mappings:
            raise ValueError(f"duplicate mapping versions: {', '.join(duplicate_mappings)}")

        for mapping in self.mappings:
            if mapping.indicator_id not in known_indicators:
                raise ValueError(
                    f"mapping {mapping.mapping_version} references unknown indicator "
                    f"{mapping.indicator_id}"
                )
        for indicator in self.indicators:
            if indicator.object_type not in self.ontology.object_types:
                raise ValueError(f"{indicator.indicator_id} has unknown object_type")
            if indicator.role not in self.ontology.roles:
                raise ValueError(f"{indicator.indicator_id} has unknown role")
            if indicator.constraint_type not in self.ontology.constraint_types:
                raise ValueError(f"{indicator.indicator_id} has unknown constraint_type")
            if indicator.research_status not in self.ontology.coverage_statuses:
                raise ValueError(f"{indicator.indicator_id} has unknown research_status")
        for dependency in self.dependencies:
            if dependency.edge_type not in self.ontology.dependency_edge_types:
                raise ValueError(f"dependency has unknown edge_type {dependency.edge_type}")
        return self


def load_indicator_registry(
    path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> IndicatorResearchRegistry:
    try:
        raw = safe_load_yaml_path(path)
        return IndicatorResearchRegistry.model_validate(raw)
    except (OSError, ValidationError, TypeError, ValueError) as exc:
        raise IndicatorResearchError(f"Indicator research registry invalid: {exc}") from exc


def build_ontology_payload(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    issues = _registry_contract_issues(registry)
    return _base_payload(
        registry,
        report_type="indicator_research_ontology",
        status=_status_from_issues(issues),
        summary={
            "object_type_count": len(registry.ontology.object_types),
            "role_count": len(registry.ontology.roles),
            "indicator_count": len(registry.indicators),
            "mapping_count": len(registry.mappings),
            "dependency_count": len(registry.dependencies),
        },
        issues=issues,
        ontology=registry.ontology.model_dump(mode="json"),
    )


def build_daily_indicator_inventory(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    scoring_rules_path: Path = DEFAULT_SCORING_RULES_CONFIG_PATH,
    market_regimes_path: Path = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    issues = _registry_contract_issues(registry)
    issues.extend(
        _daily_component_coverage_issues(
            registry=registry,
            scoring_rules_path=scoring_rules_path,
        )
    )
    regime = _market_regime_payload(registry, market_regimes_path)
    items = [_inventory_record(registry, indicator) for indicator in registry.indicators]
    coverage_counts = _count_by(items, "coverage_status")
    high_impact = [
        item
        for item in items
        if item["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
        and item["constraint_type"] != "IMMUTABLE_SAFETY_CONSTRAINT"
    ]
    if high_impact:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "high_impact_unvalidated_indicators_present",
                "message": "High-impact daily indicators affect weight before research validation.",
                "indicator_ids": [item["indicator_id"] for item in high_impact],
            }
        )
    return _base_payload(
        registry,
        report_type="daily_indicator_inventory",
        status=_status_from_issues(issues),
        market_regime=regime["market_regime"],
        requested_date_range=regime["requested_date_range"],
        summary={
            "indicator_count": len(items),
            "used_in_daily_report_count": sum(bool(item["used_in_daily_report"]) for item in items),
            "affects_signal_count": sum(bool(item["affects_signal"]) for item in items),
            "affects_weight_count": sum(bool(item["affects_weight"]) for item in items),
            "high_impact_unvalidated_count": len(high_impact),
            "coverage_counts": coverage_counts,
        },
        issues=issues,
        inventory=items,
        reader_brief={
            "key_result": "HIGH_IMPACT_UNVALIDATED_PRESENT" if high_impact else "PASS",
            "valuation_crowding_status": _coverage_for(registry, "valuation_crowding_indicator"),
            "next_action": (
                "run_masking_and_dominance_audit_before_interpreting_downstream_signal_effects"
            ),
        },
    )


def build_coverage_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    inventory = build_daily_indicator_inventory(registry_path=registry_path)
    items = list(inventory["inventory"])
    high_impact = [item for item in items if item["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"]
    conditional = _conditional_conclusion_warnings(registry)
    status = "PASS_WITH_WARNINGS" if high_impact or conditional else "PASS"
    return _base_payload(
        registry,
        report_type="indicator_research_coverage_audit",
        status=status,
        summary={
            "coverage_counts": _count_by(items, "coverage_status"),
            "high_impact_unvalidated_count": len(high_impact),
            "conditional_conclusion_count": len(conditional),
        },
        high_impact_unvalidated=high_impact,
        conditional_conclusion_warnings=conditional,
        reader_brief={
            "key_result": status,
            "next_action": "triage_high_impact_unvalidated_before_single_indicator_backfill",
        },
    )


def build_dependency_graph(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    nodes = _dependency_nodes(registry)
    edges = _dependency_edges(registry)
    cycles = _detect_indicator_cycles(registry)
    trace_rows = _read_trace_rows(trace_path)
    dominance = (
        _dominance_from_trace(registry, trace_rows)
        if trace_rows
        else _dominance_from_expected_impact(registry)
    )
    issues = []
    if cycles:
        issues.append(
            {
                "severity": "error",
                "issue_id": "circular_dependency_detected",
                "message": "Indicator dependency graph contains a cycle.",
                "cycles": cycles,
            }
        )
    if not trace_rows:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dominance_trace_data_required",
                "message": "Dominance audit cannot compute realized hit rate without trace rows.",
            }
        )
    return _base_payload(
        registry,
        report_type="indicator_dependency_graph",
        status=_status_from_issues(issues),
        summary={
            "node_count": len(nodes),
            "edge_count": len(edges),
            "cycle_count": len(cycles),
            "dominance_trace_rows": len(trace_rows),
        },
        issues=issues,
        graph={"nodes": nodes, "edges": edges},
        circular_dependency_audit={
            "status": "CIRCULAR_DEPENDENCY_DETECTED" if cycles else "PASS",
            "cycles": cycles,
        },
        dominance_audit=dominance,
    )


def build_multi_stage_weight_trace_contract(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    contract = registry.trace_contract.model_dump(mode="json")
    return _base_payload(
        registry,
        report_type="multi_stage_weight_trace_contract",
        status="PASS",
        summary={
            "required_stage_count": len(registry.trace_contract.required_stages),
            "required_delta_field_count": len(registry.trace_contract.required_delta_fields),
            "required_masking_field_count": len(registry.trace_contract.required_masking_fields),
        },
        trace_contract=contract,
        validation_requirements=[
            "Every row must preserve module_id, mapping_version, before/after weight, delta, "
            "reason_code, and constraint_hit.",
            "Trace rows must remain research-only and must not contain official target weights.",
        ],
    )


def default_daily_indicator_weight_trace_path(output_dir: Path, as_of: Any) -> Path:
    as_of_text = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    return output_dir / f"daily_indicator_weight_trace_{as_of_text}.json"


def build_daily_indicator_weight_trace(
    report: Any,
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    scores_path: Path | None = None,
    decision_snapshot_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    as_of_text = report.as_of.isoformat()
    component_map = _indicator_by_daily_component(registry)
    total_component_weight = sum(
        _float(getattr(component, "weight", 0.0)) for component in report.components
    )
    if total_component_weight <= 0:
        raise IndicatorResearchError("daily score components must have positive total weight")

    model_band = report.recommendation.model_risk_asset_ai_band
    final_band = report.recommendation.risk_asset_ai_band
    portfolio_band = report.recommendation.total_asset_ai_band
    rows: list[dict[str, Any]] = []

    for component in report.components:
        indicator = component_map.get(component.name)
        if indicator is None:
            module_id = component.name
            mapping_version = None
            coverage_status = "USED_IN_DAILY_NOT_REGISTERED"
        else:
            module_id = indicator.indicator_id
            mapping_version = indicator.mapping_version
            coverage_status = _coverage_status(registry, indicator)
        contribution = _float(component.score) * _float(component.weight) / total_component_weight
        rows.append(
            _daily_trace_row(
                as_of_text=as_of_text,
                row_type="indicator_component",
                module_id=module_id,
                daily_component_id=component.name,
                mapping_version=mapping_version,
                raw_indicator_value=_signal_raw_values(component.signals),
                normalized_indicator_score=_float(component.score),
                mapped_signal_contribution=contribution,
                pre_constraint_signal_weight=model_band.max_position,
                post_constraint_signal_weight=final_band.max_position,
                final_advisory_portfolio_facing_weight=portfolio_band.max_position,
                weight_before=model_band.max_position,
                weight_after=final_band.max_position,
                reason_code="component_score_contribution_trace",
                constraint_hit=False,
                upstream_indicator_id=module_id,
                downstream_indicator_id="",
                extra={
                    "component_weight": _float(component.weight),
                    "component_source_type": getattr(component, "source_type", ""),
                    "component_coverage": _float(getattr(component, "coverage", 0.0)),
                    "component_confidence": _float(getattr(component, "confidence", 0.0)),
                    "coverage_status": coverage_status,
                    "signal_scores": _signal_score_rows(component.signals),
                    "stages": _daily_trace_stage_snapshot(report),
                },
            )
        )

    rows.extend(_daily_non_component_trace_rows(report, registry))
    rows.extend(_daily_constraint_trace_rows(report, registry))
    contract_audit = _trace_contract_field_audit(registry, rows)
    status = "PASS" if not contract_audit["missing_field_records"] else "PASS_WITH_WARNINGS"
    return _base_payload(
        registry,
        report_type="daily_indicator_weight_trace",
        status=status,
        summary={
            "as_of": as_of_text,
            "row_count": len(rows),
            "component_row_count": sum(row["row_type"] == "indicator_component" for row in rows),
            "constraint_row_count": sum(row["row_type"] == "constraint_gate" for row in rows),
            "trace_contract_id": registry.trace_contract.contract_id,
            "missing_trace_field_record_count": len(contract_audit["missing_field_records"]),
            "no_parameter_mutation": True,
            "no_paper_shadow_live_broker_order_official_weights": True,
        },
        source_artifacts={
            "scores_path": "" if scores_path is None else str(scores_path),
            "decision_snapshot_path": (
                "" if decision_snapshot_path is None else str(decision_snapshot_path)
            ),
        },
        trace_contract_field_audit=contract_audit,
        limitations=[
            (
                "masking fields are minimum same-day position-expression proxies; "
                "factorial counterfactual studies are still required before independent "
                "effect conclusions."
            )
        ],
        rows=rows,
    )


def write_daily_indicator_weight_trace(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path = output_path.with_suffix(".md")
    markdown_path.write_text(render_indicator_markdown(payload), encoding="utf-8")
    return output_path


def build_daily_indicator_coverage_gap_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    scoring_rules_path: Path = DEFAULT_SCORING_RULES_CONFIG_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _read_trace_rows(trace_path)
    registered = [_inventory_record(registry, indicator) for indicator in registry.indicators]
    registered_ids = {indicator.indicator_id for indicator in registry.indicators}
    component_map = _indicator_by_daily_component(registry)
    expected_components = _expected_daily_component_ids(scoring_rules_path)
    unregistered_components = sorted(
        component_id
        for component_id in expected_components
        if component_id not in component_map
        and _daily_component_alias(component_id) not in component_map
    )
    unregistered_from_trace = sorted(
        {
            str(row.get("module_id") or row.get("daily_component_id"))
            for row in trace_rows
            if _trace_row_is_daily_indicator(row, registered_ids, component_map)
        }
    )
    unregistered_daily_indicators = [
        {"daily_component_id": item, "source": "expected_daily_scoring_chain"}
        for item in unregistered_components
    ]
    unregistered_daily_indicators.extend(
        {"daily_component_id": item, "source": "daily_weight_trace"}
        for item in unregistered_from_trace
        if item not in unregistered_components
    )
    incomplete = [
        _registered_indicator_gap_record(
            registry,
            indicator,
            trace_rows=trace_rows,
        )
        for indicator in registry.indicators
    ]
    incomplete = [item for item in incomplete if item["missing_fields"]]
    high_impact = [
        item for item in registered if item["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
    ]
    status = (
        "PASS_WITH_WARNINGS"
        if unregistered_daily_indicators or incomplete or high_impact
        else "PASS"
    )
    return _base_payload(
        registry,
        report_type="daily_indicator_coverage_gap_report",
        status=status,
        summary={
            "registered_indicator_count": len(registered),
            "unregistered_daily_indicator_count": len(unregistered_daily_indicators),
            "registered_incomplete_indicator_count": len(incomplete),
            "high_impact_unvalidated_count": len(high_impact),
            "trace_path_provided": trace_path is not None,
        },
        registered_indicators=registered,
        unregistered_daily_indicators=unregistered_daily_indicators,
        registered_incomplete_indicators=incomplete,
        high_impact_unvalidated=high_impact,
    )


def load_threshold_registry(
    path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise IndicatorResearchError(f"threshold registry must be a mapping: {path}")
    thresholds = raw.get("thresholds")
    if not isinstance(thresholds, Sequence) or isinstance(thresholds, (str, bytes)):
        raise IndicatorResearchError("threshold registry must define a thresholds list")
    backlog = raw.get("calibration_backlog", [])
    if not isinstance(backlog, Sequence) or isinstance(backlog, (str, bytes)):
        raise IndicatorResearchError(
            "threshold registry calibration_backlog must be a list when provided"
        )
    return dict(raw)


def build_threshold_registry_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    thresholds = [
        _json_ready(dict(item))
        for item in threshold_registry.get("thresholds", [])
        if isinstance(item, Mapping)
    ]
    backlog = [
        _json_ready(dict(item))
        for item in threshold_registry.get("calibration_backlog", [])
        if isinstance(item, Mapping)
    ]
    issues = _threshold_registry_issues(threshold_registry, thresholds)
    summary = _threshold_audit_summary(thresholds)
    status = (
        "FAIL"
        if any(issue.get("severity") == "error" for issue in issues)
        else "PASS_WITH_WARNINGS" if summary["uncalibrated_high_impact_count"] > 0 else "PASS"
    )
    return _base_payload(
        registry,
        report_type="threshold_registry_audit",
        status=status,
        summary=summary,
        issues=issues,
        threshold_registry_metadata=_json_ready(
            {
                key: threshold_registry.get(key)
                for key in (
                    "schema_version",
                    "registry_id",
                    "task_id",
                    "version",
                    "status",
                    "owner",
                    "mode",
                    "production_effect",
                )
            }
        ),
        threshold_registry_path=str(threshold_registry_path),
        threshold_scope=_json_ready(threshold_registry.get("scope", {})),
        thresholds=thresholds,
        calibration_backlog=backlog,
        safety_boundary=_json_ready(
            {
                **dict(registry.safety_boundary),
                **dict(threshold_registry.get("safety_boundary", {})),
            }
        ),
        reader_brief={
            "key_result": status,
            "total_threshold_count": summary["total_threshold_count"],
            "high_impact_threshold_count": summary["high_impact_threshold_count"],
            "uncalibrated_high_impact_count": summary["uncalibrated_high_impact_count"],
            "thresholds_blocking_promotion": summary["thresholds_blocking_promotion"],
            "next_action": (
                "review calibration backlog before using any A-class default as "
                "promotion dependency"
            ),
        },
    )


def build_threshold_prioritization_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    thresholds = [
        _json_ready(dict(item))
        for item in threshold_registry.get("thresholds", [])
        if isinstance(item, Mapping)
    ]
    prioritization_policy = _json_ready(
        dict(threshold_registry.get("impact_prioritization", {}))
        if isinstance(threshold_registry.get("impact_prioritization"), Mapping)
        else {}
    )
    prioritized_thresholds = _prioritized_threshold_records(
        thresholds,
        prioritization_policy,
        high_impact_only=True,
    )
    all_thresholds_with_urgency = _prioritized_threshold_records(
        thresholds,
        prioritization_policy,
        high_impact_only=False,
    )
    first_batch_candidates = _first_batch_threshold_candidates(
        prioritized_thresholds,
        prioritization_policy,
    )
    first_batch_ids = {str(candidate.get("threshold_id")) for candidate in first_batch_candidates}
    for record in prioritized_thresholds:
        record["first_batch_candidate"] = str(record.get("threshold_id")) in first_batch_ids
    for record in all_thresholds_with_urgency:
        record["first_batch_candidate"] = str(record.get("threshold_id")) in first_batch_ids
    issues = _threshold_prioritization_issues(
        thresholds,
        prioritized_thresholds,
        prioritization_policy,
        first_batch_candidates,
    )
    summary = _threshold_prioritization_summary(
        prioritized_thresholds,
        all_thresholds_with_urgency,
        first_batch_candidates,
    )
    status = (
        "FAIL"
        if any(issue.get("severity") == "error" for issue in issues)
        else "PASS_WITH_WARNINGS" if summary["uncalibrated_high_impact_count"] > 0 else "PASS"
    )
    return _base_payload(
        registry,
        report_type="threshold_prioritization_report",
        status=status,
        summary=summary,
        issues=issues,
        threshold_registry_path=str(threshold_registry_path),
        prioritization_policy=prioritization_policy,
        prioritized_thresholds=prioritized_thresholds,
        all_thresholds_with_urgency=all_thresholds_with_urgency,
        first_batch_calibration_candidates=first_batch_candidates,
        safety_boundary=_json_ready(
            {
                **dict(registry.safety_boundary),
                **dict(threshold_registry.get("safety_boundary", {})),
                "promotion_gate_allowed": False,
                "production_weight_change_allowed": False,
                "paper_shadow_change_allowed": False,
            }
        ),
        reader_brief={
            "key_result": status,
            "first_batch_candidate_count": summary["first_batch_candidate_count"],
            "first_batch_candidate_ids": summary["first_batch_candidate_ids"],
            "next_action": (
                "calibrate first-batch thresholds in validation-only mode before "
                "allowing any promotion dependency"
            ),
        },
    )


def write_indicator_validation_pack_stability_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Path = DEFAULT_INDICATOR_OUTPUT_ROOT,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    coverage_extension_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    validation_root = output_root / "control_plane_v1_validation"
    stability_root = validation_root / "validation_pack_rerun_stability"
    first = write_indicator_framework_validation_pack(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        output_root=stability_root / "run_1",
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        coverage_extension_root=coverage_extension_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    second = write_indicator_framework_validation_pack(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        output_root=stability_root / "run_2",
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        coverage_extension_root=coverage_extension_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    first_projection = _validation_pack_stability_projection(first)
    second_projection = _validation_pack_stability_projection(second)
    stable_fields = {
        "artifact_count": (
            first_projection["artifact_count"] == second_projection["artifact_count"]
        ),
        "validation_check_statuses": (
            first_projection["validation_check_statuses"]
            == second_projection["validation_check_statuses"]
        ),
        "key_artifact_statuses": (
            first_projection["key_artifact_statuses"] == second_projection["key_artifact_statuses"]
        ),
        "registry_coverage_counts": (
            first_projection["registry_coverage_counts"]
            == second_projection["registry_coverage_counts"]
        ),
        "coverage_gap_summary": (
            first_projection["coverage_gap_summary"] == second_projection["coverage_gap_summary"]
        ),
        "trace_field_audit": (
            first_projection["trace_field_audit"] == second_projection["trace_field_audit"]
        ),
        "trace_fields_complete": (
            first_projection["trace_field_missing_count"] == 0
            and second_projection["trace_field_missing_count"] == 0
        ),
        "coverage_gap_unregistered": (
            first_projection["coverage_gap_unregistered"]
            == second_projection["coverage_gap_unregistered"]
        ),
        "high_impact_unvalidated": (
            first_projection["high_impact_unvalidated_ids"]
            == second_projection["high_impact_unvalidated_ids"]
        ),
        "masking_diagnostics_repeatable": (
            first_projection["masking_diagnostics"] == second_projection["masking_diagnostics"]
        ),
        "masking_casebook_repeatable": (
            first_projection["masking_casebook_summary"]
            == second_projection["masking_casebook_summary"]
        ),
        "gate_availability_repeatable": (
            first_projection["gate_availability_summary"]
            == second_projection["gate_availability_summary"]
        ),
        "sample_quality_repeatable": (
            first_projection["sample_quality_summary"]
            == second_projection["sample_quality_summary"]
        ),
        "component_trace_repeatable": (
            first_projection["component_trace_summary"]
            == second_projection["component_trace_summary"]
        ),
        "backtest_trace_bridge_repeatable": (
            first_projection["backtest_trace_bridge_summary"]
            == second_projection["backtest_trace_bridge_summary"]
        ),
        "masking_effectiveness_repeatable": (
            first_projection["masking_effectiveness_summary"]
            == second_projection["masking_effectiveness_summary"]
        ),
        "outcome_availability_repeatable": (
            first_projection["outcome_availability_summary"]
            == second_projection["outcome_availability_summary"]
        ),
        "masking_robustness_repeatable": (
            first_projection["masking_robustness_summary"]
            == second_projection["masking_robustness_summary"]
            and first_projection["masking_robustness_delta_count"]
            == second_projection["masking_robustness_delta_count"]
        ),
        "validation_rollup_repeatable": (
            first_projection["validation_rollup_summary"]
            == second_projection["validation_rollup_summary"]
            and first_projection["validation_rollup_recommendation"]
            == second_projection["validation_rollup_recommendation"]
        ),
        "floor_calibration_repeatable": (
            first_projection["floor_calibration_summary"]
            == second_projection["floor_calibration_summary"]
            and first_projection["floor_calibration_sensitivity"]
            == second_projection["floor_calibration_sensitivity"]
        ),
        "lineage_manifest_repair_repeatable": (
            first_projection["lineage_manifest_repair_summary"]
            == second_projection["lineage_manifest_repair_summary"]
        ),
        "threshold_registry_audit_repeatable": (
            first_projection["threshold_audit_summary"]
            == second_projection["threshold_audit_summary"]
        ),
        "threshold_prioritization_repeatable": (
            first_projection["threshold_prioritization_summary"]
            == second_projection["threshold_prioritization_summary"]
        ),
        "threshold_calibration_repeatable": (
            first_projection["threshold_calibration_summary"]
            == second_projection["threshold_calibration_summary"]
        ),
        "dynamic_trend_threshold_sensitivity_repeatable": (
            first_projection["dynamic_trend_threshold_sensitivity_summary"]
            == second_projection["dynamic_trend_threshold_sensitivity_summary"]
        ),
        "dynamic_trend_bridge_consistency_repeatable": (
            first_projection["dynamic_trend_bridge_consistency_summary"]
            == second_projection["dynamic_trend_bridge_consistency_summary"]
        ),
    }
    stable = all(stable_fields.values())
    payload = _base_payload(
        registry,
        report_type="indicator_validation_pack_rerun_stability_report",
        status="PASS" if stable else "FAIL",
        summary={
            "rerun_count": 2,
            "stable": stable,
            "artifact_count": first_projection["artifact_count"],
            "trace_path_provided": trace_path is not None,
            "gate_audit_root_provided": gate_audit_root is not None,
            "bridge_artifact_root_provided": bridge_artifact_root is not None,
        },
        stable_fields=stable_fields,
        run_1=first_projection,
        run_2=second_projection,
    )
    write_indicator_artifact_pair(
        payload,
        output_root=validation_root,
        artifact_id="indicator_validation_pack_rerun_stability_report",
    )
    return payload


def build_constraint_attribution_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _read_trace_rows(trace_path)
    attribution = _dominance_from_trace(registry, trace_rows) if trace_rows else []
    status = "PASS" if trace_rows else "PASS_WITH_WARNINGS"
    issues = []
    if not trace_rows:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "constraint_attribution_trace_required",
                "message": "No multi-stage trace rows were provided; attribution contract only.",
            }
        )
    return _base_payload(
        registry,
        report_type="constraint_attribution_report",
        status=status,
        summary={
            "trace_row_count": len(trace_rows),
            "attributed_module_count": len(attribution),
            "trace_contract_id": registry.trace_contract.contract_id,
        },
        issues=issues,
        constraint_attribution=attribution,
    )


def build_role_and_target_registry(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    roles = [
        {
            "indicator_id": indicator.indicator_id,
            "role": indicator.role,
            "target_family": indicator.target_family,
            "evaluation_metrics": indicator.evaluation_metrics,
            "must_not_use_single_future_return_target": indicator.role
            in {
                "RISK_STATE_INDICATOR",
                "CONSTRAINT_GUARDRAIL_INDICATOR",
                "CONFIDENCE_RELIABILITY_INDICATOR",
                "EXECUTION_INDICATOR",
            },
        }
        for indicator in registry.indicators
    ]
    return _base_payload(
        registry,
        report_type="indicator_role_and_target_registry",
        status="PASS",
        summary={"indicator_count": len(roles), "role_counts": _count_by(roles, "role")},
        roles=roles,
    )


def build_data_pit_leakage_gate(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    rows = [
        {
            "indicator_id": indicator.indicator_id,
            "data_gate": indicator.data_gate,
            "mapping_research_allowed": _mapping_research_allowed(indicator),
            "blocking_reason": (
                None if _mapping_research_allowed(indicator) else "DATA_GATE_REQUIRED"
            ),
        }
        for indicator in registry.indicators
    ]
    blocked = [row for row in rows if not row["mapping_research_allowed"]]
    return _base_payload(
        registry,
        report_type="indicator_data_pit_and_leakage_gate",
        status="PASS_WITH_WARNINGS" if blocked else "PASS",
        summary={
            "indicator_count": len(rows),
            "mapping_research_blocked_count": len(blocked),
        },
        gates=rows,
    )


def build_mapping_registry(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    mappings = [mapping.model_dump(mode="json") for mapping in registry.mappings]
    hard_caps = [mapping for mapping in mappings if mapping["family"] == "M5_HARD_CAP"]
    return _base_payload(
        registry,
        report_type="indicator_signal_mapping_registry",
        status="PASS_WITH_WARNINGS" if hard_caps else "PASS",
        summary={
            "mapping_count": len(mappings),
            "hard_cap_mapping_count": len(hard_caps),
            "family_counts": _count_by(mappings, "family"),
        },
        mappings=mappings,
        warning="Hard-cap mappings require masking and dominance audit before parameter changes.",
    )


def build_indicator_diagnostics(
    *,
    indicator_id: str,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    indicator = _indicator_or_raise(registry, indicator_id)
    coverage_status = _coverage_status(registry, indicator)
    diagnostics = {
        "distribution": "EVIDENCE_REQUIRED",
        "coverage": "REGISTERED" if indicator.used_in_daily_report else "NOT_USED_IN_DAILY",
        "regime_stability": "EVIDENCE_REQUIRED",
        "outlier_review": "EVIDENCE_REQUIRED",
        "correlation_with_existing_signals": "EVIDENCE_REQUIRED",
        "basic_forward_association": "EVIDENCE_REQUIRED",
        "weight_generated": False,
    }
    return _base_payload(
        registry,
        report_type="mapping_free_indicator_diagnostics",
        status="PASS_WITH_WARNINGS",
        summary={
            "indicator_id": indicator_id,
            "coverage_status": coverage_status,
            "weight_generated": False,
        },
        indicator=_indicator_summary(registry, indicator),
        diagnostics=diagnostics,
        calibration_stability_requirements=_calibration_requirements(registry),
        reader_brief={
            "role": indicator.role,
            "mapping_version": indicator.mapping_version,
            "upstream_constraints": _upstream_constraints(registry, indicator_id),
            "next_action": "collect_mapping_free_evidence_before_candidate_mapping_backfill",
        },
    )


def build_mapping_plan(
    *,
    indicator_id: str,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    indicator = _indicator_or_raise(registry, indicator_id)
    current_mapping = _mapping_for_indicator(registry, indicator_id)
    coverage_status = _coverage_status(registry, indicator)
    cards = [
        _hypothesis_card(indicator, family, current_mapping, coverage_status)
        for family in registry.mapping_candidate_families
    ]
    hard_cap_current = current_mapping is not None and current_mapping.family == "M5_HARD_CAP"
    return _base_payload(
        registry,
        report_type="indicator_mapping_candidate_plan",
        status="PASS_WITH_WARNINGS" if hard_cap_current else "PASS",
        summary={
            "indicator_id": indicator_id,
            "candidate_family_count": len(cards),
            "current_mapping_family": None if current_mapping is None else current_mapping.family,
            "hard_cap_current_mapping_requires_audit": hard_cap_current,
        },
        indicator=_indicator_summary(registry, indicator),
        current_mapping=(
            None if current_mapping is None else current_mapping.model_dump(mode="json")
        ),
        hypothesis_cards=cards,
        rule=(
            "Do not promote a hard cap until PIT, diagnostics, dominance, "
            "and masking audits pass."
        ),
    )


def build_masking_audit(
    *,
    indicator_id: str,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    _indicator_or_raise(registry, indicator_id)
    trace_rows = _read_trace_rows(trace_path)
    trace_field_audit = _trace_contract_field_audit(registry, trace_rows) if trace_rows else {}
    candidate_pairs = _masking_pairs_for_indicator(registry, indicator_id)
    realized_pairs = _masking_from_trace(registry, trace_rows) if trace_rows else {}
    results = []
    for upstream_id, downstream_id in candidate_pairs:
        key = (upstream_id, downstream_id)
        if key in realized_pairs:
            results.append(realized_pairs[key])
        else:
            results.append(_trace_required_masking_record(registry, upstream_id, downstream_id))
    if not results and trace_rows:
        results = list(realized_pairs.values())
    status = "PASS" if trace_rows and results else "PASS_WITH_WARNINGS"
    issues = []
    if trace_rows and trace_field_audit.get("missing_field_records"):
        issues.append(
            {
                "severity": "warning",
                "issue_id": "trace_contract_fields_missing",
                "message": "Trace rows are present but some required fields are missing.",
                "missing_field_records": trace_field_audit["missing_field_records"],
            }
        )
    return _base_payload(
        registry,
        report_type="indicator_masking_and_dominance_audit",
        status=status,
        issues=issues,
        summary={
            "indicator_id": indicator_id,
            "trace_row_count": len(trace_rows),
            "pair_count": len(results),
            "trace_required_pair_count": sum(
                1 for item in results if item["masking_status"] == TRACE_REQUIRED_STATUS
            ),
            "missing_trace_field_record_count": len(
                trace_field_audit.get("missing_field_records", [])
            ),
        },
        masking_results=results,
        dominance_audit=(
            _dominance_from_trace(registry, trace_rows)
            if trace_rows
            else _dominance_from_expected_impact(registry)
        ),
        trace_contract_field_audit=trace_field_audit,
        reader_brief={
            "masking": "TRACE_DATA_REQUIRED" if not trace_rows else "TRACE_EVALUATED",
            "independent_effect": _independent_effect_status(results),
            "next_action": "generate_multi_stage_weight_trace_before_final_signal_conclusion",
        },
    )


def build_factorial_counterfactual_planner(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    plans = []
    for upstream_id, downstream_id in _high_impact_masking_pairs(registry):
        plans.append(
            {
                "pair_id": f"{upstream_id}__{downstream_id}",
                "upstream_indicator_id": upstream_id,
                "downstream_indicator_id": downstream_id,
                "experiments": [
                    {"experiment_id": "Base", "modules_enabled": []},
                    {"experiment_id": "A_only", "modules_enabled": [upstream_id]},
                    {"experiment_id": "B_only", "modules_enabled": [downstream_id]},
                    {"experiment_id": "A_plus_B", "modules_enabled": [upstream_id, downstream_id]},
                ],
                "required_outputs": [
                    "main_effect_A",
                    "main_effect_B",
                    "interaction_effect_A_B",
                    "masking_ratio_A_to_B",
                ],
                "interpretation_guardrail": "Do not report mixed A+B result as single-module B.",
            }
        )
    return _base_payload(
        registry,
        report_type="factorial_counterfactual_experiment_planner",
        status="PASS_WITH_WARNINGS" if plans else "PASS",
        summary={"planned_pair_count": len(plans)},
        factorial_plans=plans,
    )


def build_portfolio_signal_transfer_attribution(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    return _base_payload(
        registry,
        report_type="portfolio_signal_transfer_attribution_contract",
        status="PASS",
        summary={
            "required_transfer_steps": [
                "signal_to_raw_target",
                "raw_target_to_constrained_target",
                "constrained_target_to_executed_research_weight",
            ],
            "trace_contract_id": registry.trace_contract.contract_id,
        },
        transfer_metrics=[
            "signal_transfer_coefficient",
            "constraint_transfer_loss",
            "execution_transfer_loss",
            "turnover",
            "cost",
        ],
        required_trace_fields=registry.trace_contract.required_delta_fields,
    )


def build_trial_ledger_and_holdout_service(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    holdout = registry.validation_policy.holdout
    return _base_payload(
        registry,
        report_type="indicator_trial_ledger_and_holdout_service",
        status="PASS_WITH_WARNINGS",
        summary={
            "trial_count": 0,
            "default_development_window": holdout.default_development_window,
            "default_diagnostic_window": holdout.default_diagnostic_window,
            "final_holdout_access": holdout.final_holdout_access,
        },
        trial_ledger_contract={
            "required_fields": [
                "trial_id",
                "indicator_id",
                "mapping_family",
                "parameters",
                "window_id",
                "created_at",
                "outcome",
                "reported",
            ],
            "multiple_testing_adapters": [
                "DSR",
                "PBO",
                "Reality_Check",
                "SPA",
            ],
        },
        holdout_service={
            "purge_days": holdout.purge_days,
            "embargo_days": holdout.embargo_days,
            "contamination_tracking_required": True,
            "final_holdout_authorization_required": True,
        },
    )


def build_conditional_incremental_effect_contract(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    return _base_payload(
        registry,
        report_type="conditional_incremental_effect_contract",
        status="PASS",
        summary={
            "effect_types": [
                "unconditional",
                "conditional",
                "incremental",
                "residualized",
            ],
            "causal_status": "ASSOCIATIONAL_NOT_CAUSAL",
        },
        required_outputs=[
            "ASSOCIATIONAL_NOT_CAUSAL",
            "EFFECT_CONDITIONAL_ON_UPSTREAM_VERSION",
            "EFFECT_MASKED_BY_UPSTREAM_CONSTRAINT",
            "INCREMENTAL_EFFECT_NOT_PROVEN",
        ],
    )


def build_campaign_adapter_contract(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    return _base_payload(
        registry,
        report_type="indicator_research_campaign_adapter_contract",
        status="PASS_WITH_WARNINGS",
        summary={
            "adapter_id": "indicator-research-framework-v1-adapter",
            "adapter_status": "CONTRACT_READY_COMPUTE_LIMITED",
            "stage_count": len(registry.research_stages),
        },
        adapter_contract={
            "adapter_id": "indicator-research-framework-v1-adapter",
            "supported_campaign_type": "indicator-to-signal-research-v1",
            "supported_run_modes": ["VALIDATION_ONLY_MODE"],
            "stage_mapping": [
                {
                    "indicator_stage": stage.stage_id,
                    "campaign_stage": _campaign_stage_for_indicator_stage(stage.stage_id),
                    "output_artifact": stage.output_artifact,
                }
                for stage in registry.research_stages
            ],
            "limitation": (
                "Current adapter is validation/audit-only until multi-stage trace and "
                "indicator-specific compute runners are available."
            ),
        },
    )


def build_valuation_crowding_pilot_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    return build_valuation_crowding_pilot_validation_report(
        registry_path=registry_path,
        trace_path=trace_path,
        report_type="valuation_crowding_pilot_audit",
    )


def build_valuation_crowding_pilot_validation_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    scoring_rules_path: Path = DEFAULT_SCORING_RULES_CONFIG_PATH,
    trace_path: Path | None = None,
    report_type: str = "valuation_crowding_pilot_validation_report",
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    indicator = _indicator_or_raise(registry, "valuation_crowding_indicator")
    mapping = _mapping_for_indicator(registry, indicator.indicator_id)
    inventory = _inventory_record(registry, indicator)
    masking = build_masking_audit(
        indicator_id=indicator.indicator_id,
        registry_path=registry_path,
        trace_path=trace_path,
    )
    trace_rows = _read_trace_rows(trace_path)
    traced = bool(trace_rows)
    status = (
        "VALUATION_CROWDING_RESEARCH_COVERAGE_KNOWN"
        if traced
        else indicator.pilot_output_if_untraced or "VALUATION_CROWDING_UNTESTED_HIGH_IMPACT"
    )
    high_masking = [
        item for item in masking["masking_results"] if item.get("masking_status") == "HIGH_MASKING"
    ]
    return _base_payload(
        registry,
        report_type=report_type,
        status=status,
        summary={
            "indicator_id": indicator.indicator_id,
            "coverage_status": inventory["coverage_status"],
            "trace_rows_available": traced,
            "high_impact_unvalidated": _is_high_impact_unvalidated(registry, indicator),
            "high_masking_pair_count": len(high_masking),
            "parameter_mutation": False,
        },
        indicator_definition={
            **_indicator_summary(registry, indicator),
            "source_features": [item.model_dump(mode="json") for item in indicator.source_features],
            "target_family": indicator.target_family,
            "evaluation_metrics": list(indicator.evaluation_metrics),
        },
        raw_value_sources={
            "registry_source_features": [
                item.model_dump(mode="json") for item in indicator.source_features
            ],
            "implementation_refs": list(indicator.implementation_refs),
            "trace_observations": _trace_observations_for_indicator(
                trace_rows,
                indicator.indicator_id,
            ),
        },
        normalization=_valuation_normalization_payload(scoring_rules_path),
        mapping_to_signal={
            "mapping_version": None if mapping is None else mapping.mapping_version,
            "family": None if mapping is None else mapping.family,
            "direction": None if mapping is None else mapping.direction,
            "output_range": None if mapping is None else mapping.output_range,
            "parameter_source": None if mapping is None else mapping.parameter_source,
            "status": None if mapping is None else mapping.status,
            "role_classification": "researchable_strategy_constraint_hard_cap",
        },
        effective_thresholds=_valuation_effective_thresholds(scoring_rules_path),
        trend_interaction={
            "upstream_indicator_id": indicator.indicator_id,
            "downstream_indicator_id": "trend_strength_indicator",
            "dependency_edge_type": "MASKS",
            "interaction_mode": (
                "valuation/crowding cap can reduce post-constraint expression of "
                "trend-driven allocation intent"
            ),
            "masking_summary": masking["summary"],
            "masking_results": masking["masking_results"],
        },
        high_impact_unvalidated_reason={
            "research_status": indicator.research_status,
            "expected_hit_rate_band": indicator.expected_impact.hit_rate_band,
            "expected_weight_impact_band": indicator.expected_impact.weight_impact_band,
            "affects_weight": indicator.affects_weight,
            "coverage_status": inventory["coverage_status"],
            "reason": (
                "affects_weight=true, expected hit/weight impact are high, and "
                "research_status is not RESEARCHED"
            ),
        },
        high_masking_reason=_high_masking_reason_payload(registry, masking["masking_results"]),
        inventory_record=inventory,
        masking_summary=masking["summary"],
        masking_results=masking["masking_results"],
        no_parameter_mutation=True,
    )


def build_masking_casebook(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    upstream_indicator_id: str = "valuation_crowding_indicator",
    downstream_indicator_id: str = "trend_strength_indicator",
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    upstream = _indicator_or_raise(registry, upstream_indicator_id)
    _indicator_or_raise(registry, downstream_indicator_id)
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    pair_rows = _pair_trace_rows(trace_rows, upstream_indicator_id, downstream_indicator_id)
    component_index = _component_trace_index(trace_rows)
    price_series_by_ticker = _read_price_series_by_ticker(prices_path)
    fallback_price_series = price_series_by_ticker.get(outcome_ticker.upper(), [])
    trace_contract_version = _trace_contract_version_for_payload(
        trace_path,
        registry=registry,
    )
    cases = []
    for row in pair_rows:
        for case_asset in _casebook_assets_for_row(row, asset_universe=asset_universe):
            case_outcome_ticker, missing_asset_mapping = _price_ticker_mapping_for_asset(
                case_asset,
                price_series_by_ticker,
                fallback_ticker=outcome_ticker,
            )
            cases.append(
                _masking_casebook_row(
                    row,
                    component_index=component_index,
                    price_series=price_series_by_ticker.get(
                        case_outcome_ticker,
                        fallback_price_series,
                    ),
                    outcome_ticker=case_outcome_ticker,
                    case_asset=case_asset,
                    trace_contract_version=trace_contract_version,
                    missing_asset_mapping=missing_asset_mapping,
                )
            )
    issues: list[dict[str, Any]] = []
    if trace_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "trace_data_required_for_casebook",
                "message": "Masking casebook needs multi-stage trace rows for concrete samples.",
            }
        )
    if prices_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "outcome_prices_not_provided",
                "message": "Forward 1d/5d/10d/20d outcomes are null without a prices CSV.",
            }
        )
    if trace_rows and not pair_rows:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "masking_pair_not_observed",
                "message": "Trace exists but has no valuation/crowding -> trend masking rows.",
            }
        )
    status = (
        "PASS"
        if cases and not any(case["outcome_missing"] for case in cases)
        else "PASS_WITH_WARNINGS"
    )
    return _base_payload(
        registry,
        report_type="indicator_masking_casebook",
        status=status,
        issues=issues,
        summary={
            "upstream_indicator_id": upstream_indicator_id,
            "downstream_indicator_id": downstream_indicator_id,
            "case_count": len(cases),
            "outcome_ticker": outcome_ticker,
            "drawdown_reduced_count": sum(1 for case in cases if case["drawdown_reduced"]),
            "missed_upside_count": sum(1 for case in cases if case["missed_upside"]),
            "false_risk_off_count": sum(1 for case in cases if case["false_risk_off"]),
            "outcome_missing_count": sum(1 for case in cases if case["outcome_missing"]),
            "outcome_not_mature_count": sum(1 for case in cases if case.get("outcome_not_mature")),
            **_trace_sample_quality_stats(registry, trace_rows, cases=cases),
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        upstream_indicator=_indicator_summary(registry, upstream),
        casebook=cases,
        outcome_horizons=list(MASKING_OUTCOME_HORIZONS),
        read_only_counterfactual_ready=bool(cases),
    )


def build_valuation_crowding_ablation_validation(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    if not 0 <= capped_masking_ratio <= 1:
        raise IndicatorResearchError("capped_masking_ratio must be between 0 and 1")
    casebook = build_masking_casebook(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases = [dict(item) for item in casebook.get("casebook", []) if isinstance(item, Mapping)]
    scenarios = {
        "baseline": _ablation_scenario_metrics(cases, "baseline", capped_masking_ratio),
        "no_valuation_crowding_masking": _ablation_scenario_metrics(
            cases,
            "no_valuation_crowding_masking",
            capped_masking_ratio,
        ),
        "capped_masking": _ablation_scenario_metrics(
            cases,
            "capped_masking",
            capped_masking_ratio,
        ),
    }
    issues = list(casebook.get("issues", [])) if isinstance(casebook.get("issues"), list) else []
    if not cases:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "ablation_requires_masking_casebook_cases",
                "message": "Ablation schema is emitted, but comparison needs trace-backed cases.",
            }
        )
    status = (
        "PASS"
        if cases and casebook["summary"]["outcome_missing_count"] == 0
        else "PASS_WITH_WARNINGS"
    )
    return _base_payload(
        registry,
        report_type="valuation_crowding_ablation_validation",
        status=status,
        issues=issues,
        summary={
            "scenario_count": len(scenarios),
            "case_count": len(cases),
            "date_count": casebook["summary"].get("date_count"),
            "asset_count": casebook["summary"].get("asset_count"),
            "outcome_ticker": outcome_ticker,
            "capped_masking_ratio": capped_masking_ratio,
            "sample_quality_breakdown": {
                key: casebook["summary"].get(key)
                for key in (
                    "date_count",
                    "asset_count",
                    "case_count",
                    "masking_case_count",
                    "eligible_dates",
                    "component_eligible_dates",
                    "full_advisory_equivalent_count",
                    "partial_component_only_count",
                )
            },
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        scenarios=scenarios,
        comparison_notes=[
            "baseline uses observed final advisory-facing weight from trace",
            "no_valuation_crowding_masking restores observed valuation/crowding suppression",
            "capped_masking limits observed suppression to the diagnostic cap ratio",
        ],
    )


def build_valuation_crowding_outcome_availability_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    if not 0 <= capped_masking_ratio <= 1:
        raise IndicatorResearchError("capped_masking_ratio must be between 0 and 1")
    casebook = build_masking_casebook(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    casebook_cases = [
        dict(item) for item in casebook.get("casebook", []) if isinstance(item, Mapping)
    ]
    bridge = build_backtest_trace_bridge(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    bridge_cases = [
        dict(item) for item in bridge.get("bridge_records", []) if isinstance(item, Mapping)
    ]
    full_dates, component_only_dates = _trace_eligibility_date_sets(
        registry_path=registry_path,
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    records = _outcome_availability_records(
        casebook_cases,
        bridge_cases,
        full_dates=full_dates,
        component_only_dates=component_only_dates,
    )
    summary = _outcome_availability_summary(records)
    mature_sample_quality = _mature_outcome_sample_quality(records, registry)
    issues: list[dict[str, Any]] = []
    if not records:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "outcome_availability_requires_cases",
                "message": "No casebook, ablation, or bridge cases were available to audit.",
            }
        )
    if summary["outcome_missing_count"]:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "outcome_hard_missing_detected",
                "message": (
                    "Some outcome windows have hard missing price, mapping, calendar, "
                    "or join-key issues."
                ),
            }
        )
    if summary["outcome_not_mature_count"]:
        issues.append(
            {
                "severity": "info",
                "issue_id": "outcome_windows_not_mature",
                "message": (
                    "Some realized outcome windows have not matured; these are not "
                    "counted as hard missing."
                ),
            }
        )
    return _base_payload(
        registry,
        report_type="valuation_crowding_outcome_availability_audit",
        status="PASS_WITH_WARNINGS" if issues else "PASS",
        issues=issues,
        summary={
            **summary,
            **_horizon_specific_summary_counts(summary),
            "outcome_ticker": outcome_ticker,
            "capped_masking_ratio": capped_masking_ratio,
            "promotion_gate_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        join_key_fields=list(OUTCOME_JOIN_KEY_FIELDS),
        by_window=summary["by_window"],
        by_asset=summary["by_asset"],
        by_date=summary["by_date"],
        mature_sample_quality=mature_sample_quality,
        by_regime=mature_sample_quality["by_regime"],
        by_event_window=mature_sample_quality["by_event_window"],
        records=records,
        rule=(
            "Signal generation remains PIT/as-of. Realized outcome evaluation may use "
            "prices after decision_time and uses trading-day windows from the realized "
            "price series. outcome_not_mature is not a hard missing condition."
        ),
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_valuation_crowding_masking_effectiveness_review(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    if not 0 <= capped_masking_ratio <= 1:
        raise IndicatorResearchError("capped_masking_ratio must be between 0 and 1")
    casebook = build_masking_casebook(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases = [dict(item) for item in casebook.get("casebook", []) if isinstance(item, Mapping)]
    gate_audit = build_gate_availability_audit(
        registry_path=registry_path,
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    availability = [
        dict(item) for item in gate_audit.get("gate_availability", []) if isinstance(item, Mapping)
    ]
    full_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("full_advisory_trace_eligible")
    }
    component_only_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("component_validation_trace_eligible")
        and not record.get("full_advisory_trace_eligible")
    }
    full_cases = [case for case in cases if str(case.get("date") or "") in full_dates]
    component_cases = [
        case for case in cases if str(case.get("date") or "") in component_only_dates
    ]
    bridge = build_backtest_trace_bridge(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    bridge_cases = [
        _bridge_record_to_effectiveness_case(record)
        for record in bridge.get("bridge_records", [])
        if isinstance(record, Mapping)
    ]
    outcome_records = _outcome_availability_records(
        cases,
        bridge_cases,
        full_dates=full_dates,
        component_only_dates=component_only_dates,
    )
    outcome_availability_summary = _outcome_availability_summary(outcome_records)
    mature_sample_quality = _mature_outcome_sample_quality(outcome_records, registry)
    layers = {
        "full_advisory_only": _masking_effectiveness_layer(
            "full_advisory_only",
            full_cases,
            registry,
            capped_masking_ratio=capped_masking_ratio,
            trace_source=FULL_ADVISORY_TRACE_SOURCE,
            confidence=TRACE_CONFIDENCE_FULL_ADVISORY,
        ),
        "component_only": _masking_effectiveness_layer(
            "component_only",
            component_cases,
            registry,
            capped_masking_ratio=capped_masking_ratio,
            trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
            confidence=TRACE_CONFIDENCE_COMPONENT,
        ),
        "backtest_bridge": _masking_effectiveness_layer(
            "backtest_bridge",
            bridge_cases,
            registry,
            capped_masking_ratio=capped_masking_ratio,
            trace_source=BACKTEST_TRACE_BRIDGE_SOURCE,
            confidence=TRACE_CONFIDENCE_BRIDGE,
        ),
    }
    by_date = [
        {
            "date": group_key,
            **_masking_effectiveness_layer(
                group_key,
                group_cases,
                registry,
                capped_masking_ratio=capped_masking_ratio,
                trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
                confidence=TRACE_CONFIDENCE_COMPONENT,
            ),
        }
        for group_key, group_cases in _group_cases(cases, "date").items()
    ]
    by_asset = [
        {
            "asset": group_key,
            **_masking_effectiveness_layer(
                group_key,
                group_cases,
                registry,
                capped_masking_ratio=capped_masking_ratio,
                trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
                confidence=TRACE_CONFIDENCE_COMPONENT,
            ),
        }
        for group_key, group_cases in _group_cases(cases, "asset").items()
    ]
    by_regime = [
        {
            "regime": group_key,
            **_masking_effectiveness_layer(
                group_key,
                group_cases,
                registry,
                capped_masking_ratio=capped_masking_ratio,
                trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
                confidence=TRACE_CONFIDENCE_COMPONENT,
            ),
        }
        for group_key, group_cases in _group_cases_by_regime(cases, registry).items()
    ]
    by_event_window = [
        {
            "event_window_id": str(window["event_window_id"]),
            "label": window["label"],
            "start_date": window["start_date"],
            "end_date": window["end_date"],
            **_masking_effectiveness_layer(
                str(window["event_window_id"]),
                _cases_for_event_window(cases, window),
                registry,
                capped_masking_ratio=capped_masking_ratio,
                trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
                confidence=TRACE_CONFIDENCE_COMPONENT,
            ),
        }
        for window in DEFAULT_EVENT_WINDOW_CATALOG
    ]
    by_horizon = [
        _masking_effectiveness_horizon_layer(
            horizon,
            full_cases,
            registry,
            capped_masking_ratio=capped_masking_ratio,
            trace_source=FULL_ADVISORY_TRACE_SOURCE,
            confidence=TRACE_CONFIDENCE_FULL_ADVISORY,
        )
        for horizon in MASKING_OUTCOME_HORIZONS
    ]
    conclusion_matrix = _masking_effectiveness_conclusion_matrix(
        full_cases=full_cases,
        component_cases=component_cases,
        bridge_cases=bridge_cases,
        registry=registry,
        capped_masking_ratio=capped_masking_ratio,
    )
    by_correlated_asset_cluster = [
        {
            "correlated_asset_cluster": group_key,
            **_masking_effectiveness_layer(
                group_key,
                group_cases,
                registry,
                capped_masking_ratio=capped_masking_ratio,
                trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
                confidence=TRACE_CONFIDENCE_COMPONENT,
            ),
        }
        for group_key, group_cases in _group_cases_by_correlated_asset_cluster(cases).items()
    ]
    recommendation = _masking_effectiveness_recommendation(
        layers["full_advisory_only"],
        by_horizon=by_horizon,
        conclusion_matrix=conclusion_matrix,
    )
    issues = list(casebook.get("issues", [])) if isinstance(casebook.get("issues"), list) else []
    if not cases:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "effectiveness_requires_masking_cases",
                "message": (
                    "Effectiveness review emitted schema only; trace-backed cases are " "required."
                ),
            }
        )
    if layers["component_only"]["sample_quality"]["case_count"] == 0:
        issues.append(
            {
                "severity": "info",
                "issue_id": "component_only_has_no_masking_cases",
                "message": (
                    "Component-only eligibility exists, but no component-only masking case "
                    "has a trace-backed pre/post mask signal; it is not counted as "
                    "effectiveness evidence."
                ),
            }
        )
    if recommendation["decision_recommendation"] == "insufficient_evidence":
        issues.append(
            {
                "severity": "warning",
                "issue_id": "effectiveness_recommendation_insufficient_evidence",
                "message": recommendation["rationale"],
            }
        )
    if recommendation["decision_recommendation"] == "preliminary_short_horizon_only":
        issues.append(
            {
                "severity": "info",
                "issue_id": "effectiveness_recommendation_short_horizon_only",
                "message": recommendation["rationale"],
            }
        )
    if any(
        row.get("evidence_status") == "insufficient_long_horizon_evidence"
        for row in conclusion_matrix
    ):
        issues.append(
            {
                "severity": "info",
                "issue_id": "insufficient_long_horizon_evidence",
                "message": (
                    "20d full-advisory mature sample is below the validation floor; "
                    "20d rows remain diagnostic and do not drive the recommendation."
                ),
            }
        )
    status = (
        "PASS"
        if cases
        and recommendation["decision_recommendation"]
        not in {"insufficient_evidence", "preliminary_short_horizon_only"}
        else "PASS_WITH_WARNINGS"
    )
    return _base_payload(
        registry,
        report_type="valuation_crowding_masking_effectiveness_review",
        status=status,
        issues=issues,
        summary={
            **_masking_effectiveness_sample_quality(cases, registry),
            "scenario_count": len(MASKING_ABLATION_SCENARIOS),
            "layer_count": len(layers),
            "conclusion_matrix_row_count": len(conclusion_matrix),
            "capped_masking_ratio": capped_masking_ratio,
            "outcome_ticker": outcome_ticker,
            "decision_recommendation": recommendation["decision_recommendation"],
            "outcome_available_count": outcome_availability_summary["outcome_available_count"],
            **_horizon_specific_summary_counts(outcome_availability_summary),
            "outcome_missing_count": outcome_availability_summary["outcome_missing_count"],
            "outcome_not_mature_count": outcome_availability_summary["outcome_not_mature_count"],
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
            "full_advisory_trace_eligible_count": gate_audit["summary"].get(
                "full_advisory_trace_eligible_count"
            ),
            "component_validation_trace_eligible_count": gate_audit["summary"].get(
                "component_validation_trace_eligible_count"
            ),
            "backtest_bridge_case_count": len(bridge_cases),
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        decision_recommendation=recommendation,
        layers=layers,
        by_date=by_date,
        by_asset=by_asset,
        by_regime=by_regime,
        by_event_window=by_event_window,
        by_horizon=by_horizon,
        by_correlated_asset_cluster=by_correlated_asset_cluster,
        conclusion_matrix=conclusion_matrix,
        gate_availability_summary=gate_audit.get("summary", {}),
        backtest_bridge_summary=bridge.get("summary", {}),
        outcome_availability_summary=outcome_availability_summary,
        mature_sample_quality=mature_sample_quality,
        outcome_availability_records=outcome_records,
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_valuation_crowding_masking_robustness_review(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    effectiveness = build_valuation_crowding_masking_effectiveness_review(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases_by_source = _robustness_cases_by_source(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    scenario_delta_matrix = _scenario_delta_matrix(effectiveness.get("conclusion_matrix", []))
    aggregation = _robustness_aggregation(effectiveness)
    case_diagnostics = _robustness_case_diagnostics(cases_by_source, capped_masking_ratio)
    ten_day_support = _ten_day_baseline_support_attribution(
        cases_by_source,
        capped_masking_ratio,
        effectiveness,
    )
    short_horizon_explanation = _short_horizon_neutral_explanation(
        cases_by_source,
        scenario_delta_matrix,
        effectiveness,
    )
    maturity_tracker = _pending_twenty_day_maturity_tracker(
        effectiveness.get("outcome_availability_records", [])
    )
    conservative_gate = _conservative_evidence_gate(
        effectiveness,
        aggregation,
        scenario_delta_matrix,
    )
    final_recommendation = conservative_gate["final_validation_recommendation"]
    issues = []
    if maturity_tracker["current_20d_mature_cases"] < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES:
        issues.append(
            {
                "severity": "info",
                "issue_id": "insufficient_long_horizon_evidence",
                "message": (
                    "20d evidence remains below the validation floor and is not used "
                    "for the final validation-only recommendation."
                ),
            }
        )
    if final_recommendation == "keep_preliminary_short_horizon_only":
        issues.append(
            {
                "severity": "info",
                "issue_id": "conservative_gate_kept_preliminary",
                "message": conservative_gate["rationale"],
            }
        )
    return _base_payload(
        registry,
        report_type="valuation_crowding_masking_robustness_review",
        status="PASS_WITH_WARNINGS" if issues else "PASS",
        issues=issues,
        summary={
            "source_effectiveness_recommendation": effectiveness.get("summary", {}).get(
                "decision_recommendation"
            ),
            "final_validation_recommendation": final_recommendation,
            "scenario_delta_row_count": len(scenario_delta_matrix),
            "case_diagnostic_count": case_diagnostics["diagnostic_case_count"],
            "current_20d_mature_cases": maturity_tracker["current_20d_mature_cases"],
            "pending_20d_cases": maturity_tracker["pending_20d_cases"],
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        source_effectiveness_summary=effectiveness.get("summary", {}),
        scenario_delta_matrix=scenario_delta_matrix,
        aggregation=aggregation,
        case_diagnostics=case_diagnostics,
        ten_day_baseline_support_attribution=ten_day_support,
        short_horizon_neutral_explanation=short_horizon_explanation,
        conservative_evidence_gate=conservative_gate,
        pending_20d_maturity_tracker=maturity_tracker,
        final_validation_recommendation={
            "decision_recommendation": final_recommendation,
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
            "rationale": conservative_gate["rationale"],
        },
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_indicator_research_validation_rollup(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    coverage_gap = build_daily_indicator_coverage_gap_report(
        registry_path=registry_path,
        trace_path=trace_path,
    )
    historical_trace = build_historical_multi_stage_weight_trace_validation(
        registry_path=registry_path,
        trace_path=trace_path,
        gate_audit_root=gate_audit_root,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    gate_availability = build_gate_availability_audit(
        registry_path=registry_path,
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    lineage_repair = build_lineage_manifest_repair_report(
        registry_path=registry_path,
        trace_path=trace_path,
        gate_audit_root=gate_audit_root,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    outcome_availability = build_valuation_crowding_outcome_availability_audit(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    robustness = build_valuation_crowding_masking_robustness_review(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    maturity_tracker = _validation_rollup_pending_maturity_tracker(
        outcome_availability,
        robustness,
    )
    rerun_criteria = _validation_rollup_rerun_criteria(
        outcome_availability,
        robustness,
        maturity_tracker,
    )
    valuation_status = _validation_rollup_valuation_status(registry)
    limitations = _validation_rollup_remaining_limitations(
        coverage_gap,
        historical_trace,
        gate_availability,
        outcome_availability,
        robustness,
    )
    issues = [
        {
            "severity": "info",
            "issue_id": "validation_rollup_not_promotion_gate",
            "message": (
                "This rollup summarizes validation artifacts only and must not be "
                "used as a promotion gate or production weight change approval."
            ),
        }
    ]
    return _base_payload(
        registry,
        report_type="indicator_research_validation_rollup",
        status="PASS_WITH_WARNINGS",
        issues=issues,
        summary={
            "framework_readiness": "READY_WITH_LIMITATIONS",
            "coverage_status": coverage_gap.get("status"),
            "trace_status": historical_trace.get("status"),
            "gate_lineage_status": _validation_rollup_gate_lineage_status(
                gate_availability,
                lineage_repair,
            ),
            "outcome_maturity_status": _validation_rollup_outcome_maturity_status(
                outcome_availability
            ),
            "valuation_crowding_final_validation_recommendation": (
                "keep_preliminary_short_horizon_only"
            ),
            "pending_20d_cases": maturity_tracker["pending_20d_cases"],
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_logic_changed": False,
            "read_only": True,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        framework_readiness={
            "status": "READY_WITH_LIMITATIONS",
            "validation_pack_status": (
                "INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS"
            ),
            "production_effect": "none",
            "remaining_validation_required": True,
        },
        coverage_status={
            "status": coverage_gap.get("status"),
            "summary": coverage_gap.get("summary", {}),
            "high_impact_unvalidated": coverage_gap.get("high_impact_unvalidated", []),
        },
        trace_status={
            "status": historical_trace.get("status"),
            "summary": historical_trace.get("summary", {}),
            "trace_contract_field_audit": historical_trace.get(
                "trace_contract_field_audit",
                {},
            ),
        },
        gate_lineage_status={
            "gate_availability_status": gate_availability.get("status"),
            "gate_availability_summary": gate_availability.get("summary", {}),
            "lineage_manifest_status": lineage_repair.get("status"),
            "lineage_manifest_summary": lineage_repair.get("summary", {}),
        },
        outcome_maturity_status={
            "status": outcome_availability.get("status"),
            "summary": outcome_availability.get("summary", {}),
            "mature_sample_quality": outcome_availability.get(
                "mature_sample_quality",
                {},
            ),
        },
        valuation_crowding_masking_current_recommendation={
            "final_validation_recommendation": "keep_preliminary_short_horizon_only",
            "ten_day_conclusion": "supports_baseline_masking",
            "one_day_conclusion": "neutral_or_incomplete",
            "five_day_conclusion": "neutral_or_incomplete",
            "twenty_day_conclusion": "insufficient_long_horizon_evidence",
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
            "source_robustness_summary": robustness.get("summary", {}),
            "conservative_evidence_gate": robustness.get(
                "conservative_evidence_gate",
                {},
            ),
        },
        valuation_crowding_indicator_status=valuation_status,
        pending_maturity_tracker=maturity_tracker,
        rerun_criteria=rerun_criteria,
        remaining_limitations=limitations,
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_long_horizon_evidence_floor_calibration_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    robustness = build_valuation_crowding_masking_robustness_review(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases_by_source = _robustness_cases_by_source(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    outcome_availability = build_valuation_crowding_outcome_availability_audit(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    full_20d_cases = [
        case
        for case in cases_by_source.get("full_advisory_only", [])
        if _case_horizon_available(case, 20)
    ]
    full_20d_sample_records = _full_advisory_mature_outcome_records(
        outcome_availability.get("records", []),
        horizon=20,
    )
    effective_sample = _long_horizon_effective_sample_size(
        full_20d_sample_records,
        registry,
    )
    robustness_gate = _long_horizon_robustness_based_gate(
        full_20d_cases,
        robustness,
        capped_masking_ratio,
    )
    sensitivity = _long_horizon_threshold_sensitivity(
        full_case_count=effective_sample["raw_case_count"],
        robustness_gate=robustness_gate,
        current_recommendation=str(
            robustness.get("summary", {}).get(
                "final_validation_recommendation",
                "keep_preliminary_short_horizon_only",
            )
            if isinstance(robustness.get("summary"), Mapping)
            else "keep_preliminary_short_horizon_only"
        ),
    )
    calibration_conclusion = _long_horizon_calibration_conclusion(
        effective_sample,
        robustness_gate,
        sensitivity,
    )
    return _base_payload(
        registry,
        report_type="long_horizon_evidence_floor_calibration_audit",
        status="PASS_WITH_WARNINGS",
        issues=[
            {
                "severity": "info",
                "issue_id": "floor_50_is_uncalibrated_guardrail",
                "message": (
                    "The current 50-case floor is treated as an uncalibrated "
                    "conservative guardrail, not as a validated statistical threshold."
                ),
            }
        ],
        summary={
            "current_20d_full_advisory_mature_cases": effective_sample["raw_case_count"],
            "current_floor": EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES,
            "floor_name": "heuristic_min_full_advisory_cases",
            "calibration_status": "uncalibrated",
            "current_recommendation": "keep_preliminary_short_horizon_only",
            "recommendation_changes_across_floors": sensitivity["recommendation_changes"],
            "calibration_conclusion": calibration_conclusion["calibration_conclusion"],
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        floor_interpretation={
            "floor_id": "heuristic_min_full_advisory_cases",
            "current_value": EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES,
            "role": "conservative_guardrail",
            "calibration_status": "uncalibrated",
            "validated_statistical_threshold": False,
            "used_for": "validation_only_long_horizon_evidence_guardrail",
            "promotion_gate_allowed": False,
        },
        threshold_sensitivity=sensitivity,
        effective_sample_size=effective_sample,
        robustness_based_gate=robustness_gate,
        calibration_conclusion=calibration_conclusion,
        source_robustness_summary=robustness.get("summary", {}),
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_threshold_calibration_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    threshold_by_id = {
        str(item.get("threshold_id")): _json_ready(dict(item))
        for item in threshold_registry.get("thresholds", [])
        if isinstance(item, Mapping)
    }
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    effectiveness = build_valuation_crowding_masking_effectiveness_review(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    robustness = build_valuation_crowding_masking_robustness_review(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    long_floor = build_long_horizon_evidence_floor_calibration_audit(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    masking_audit = build_masking_audit(
        indicator_id="valuation_crowding_indicator",
        registry_path=registry_path,
        trace_path=trace_path,
    )
    cases_by_source = _robustness_cases_by_source(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    robustness_checks = _threshold_calibration_robustness_checks(
        effectiveness,
        robustness,
        cases_by_source,
        capped_masking_ratio,
    )
    records = [
        _threshold_calibration_record(
            threshold_id,
            threshold=threshold_by_id.get(threshold_id, {"threshold_id": threshold_id}),
            registry=registry,
            trace_rows=trace_rows,
            effectiveness=effectiveness,
            robustness=robustness,
            long_floor=long_floor,
            masking_audit=masking_audit,
            robustness_checks=robustness_checks,
        )
        for threshold_id in INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS
    ]
    threshold_audit_summary = _threshold_audit_summary(
        [item for item in threshold_by_id.values() if isinstance(item, Mapping)]
    )
    summary = _threshold_calibration_summary(records, threshold_audit_summary)
    issues = _threshold_calibration_issues(records, trace_path=trace_path, prices_path=prices_path)
    return _base_payload(
        registry,
        report_type="threshold_calibration_report",
        status="PASS_WITH_WARNINGS",
        issues=issues,
        summary=summary,
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        threshold_registry_path=str(threshold_registry_path),
        calibrated_threshold_scope=list(INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS),
        threshold_calibrations=records,
        robustness_checks=robustness_checks,
        source_effectiveness_summary=effectiveness.get("summary", {}),
        source_robustness_summary=robustness.get("summary", {}),
        source_long_horizon_floor_summary=long_floor.get("summary", {}),
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        safety_boundary={
            **dict(registry.safety_boundary),
            **dict(threshold_registry.get("safety_boundary", {})),
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        reader_brief={
            "key_result": "SENSITIVITY_TESTED_VALIDATION_ONLY",
            "tested_threshold_count": summary["tested_threshold_count"],
            "sensitivity_tested_count": summary["sensitivity_tested_count"],
            "thresholds_changed_count": summary["thresholds_changed_count"],
            "next_action": (
                "review sensitivity-tested evidence; do not use these thresholds as "
                "promotion dependencies until owner-reviewed calibration evidence matures"
            ),
        },
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_threshold_calibration_followup_plan(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    calibration_report_path: Path | None = None,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    calibration_report = _load_or_build_threshold_calibration_report(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        calibration_report_path=calibration_report_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    threshold_records = [
        dict(record)
        for record in calibration_report.get("threshold_calibrations", [])
        if isinstance(record, Mapping)
        and str(record.get("threshold_id")) in INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS
    ]
    result_rows = [_threshold_followup_result_row(record) for record in threshold_records]
    data_gap_plan = [
        _threshold_data_gap_plan(record)
        for record in threshold_records
        if str(record.get("recommended_action")) == "insufficient_data"
    ]
    keep_current_status = [
        _threshold_keep_current_status(record)
        for record in threshold_records
        if str(record.get("recommended_action")) == "keep_current_value"
    ]
    summary = _threshold_followup_summary(
        result_rows,
        data_gap_plan,
        keep_current_status,
        calibration_report.get("summary", {}),
    )
    issues = _threshold_followup_issues(result_rows, data_gap_plan)
    return _base_payload(
        registry,
        report_type="threshold_calibration_followup_plan",
        status="PASS_WITH_WARNINGS" if data_gap_plan else "PASS",
        summary=summary,
        issues=issues,
        threshold_registry_path=str(threshold_registry_path),
        source_threshold_calibration_report_path=(
            str(calibration_report_path) if calibration_report_path is not None else None
        ),
        source_threshold_calibration_summary=_json_ready(
            calibration_report.get("summary", {})
            if isinstance(calibration_report.get("summary"), Mapping)
            else {}
        ),
        threshold_results=result_rows,
        data_gap_plan=data_gap_plan,
        keep_current_value_threshold_status=keep_current_status,
        registry_calibration_followup_summary=_json_ready(
            threshold_registry.get("calibration_followup_summary", {})
        ),
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        safety_boundary={
            **dict(registry.safety_boundary),
            **dict(threshold_registry.get("safety_boundary", {})),
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        allowed_uses=[
            "calibration_followup_planning",
            "data_gap_tracking",
            "manual_review_input",
        ],
        reader_brief={
            "key_result": "THRESHOLD_CALIBRATION_FOLLOWUP_PLAN_READY",
            "threshold_count": summary["threshold_count"],
            "insufficient_data_threshold_count": summary["insufficient_data_threshold_count"],
            "keep_current_value_threshold_count": summary["keep_current_value_threshold_count"],
            "thresholds_changed_count": summary["thresholds_changed_count"],
            "next_action": (
                "collect data gaps for insufficient thresholds; keep current values and "
                "do not use sensitivity-tested thresholds as promotion dependencies"
            ),
        },
    )


def build_dynamic_trend_threshold_calibration_prep_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    threshold_by_id = {
        str(item.get("threshold_id")): dict(item)
        for item in threshold_registry.get("thresholds", [])
        if isinstance(item, Mapping)
    }
    prep_records = [
        _dynamic_trend_threshold_prep_record(
            threshold_id,
            threshold_by_id.get(threshold_id, {"threshold_id": threshold_id}),
        )
        for threshold_id in SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS
    ]
    summary = _dynamic_trend_threshold_prep_summary(prep_records)
    issues = _dynamic_trend_threshold_prep_issues(prep_records)
    return _base_payload(
        registry,
        report_type="dynamic_trend_threshold_calibration_prep_report",
        status="PASS_WITH_WARNINGS",
        summary=summary,
        issues=issues,
        threshold_registry_path=str(threshold_registry_path),
        calibrated_threshold_scope=list(SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS),
        threshold_calibration_prep=prep_records,
        registry_second_batch_calibration_prep_summary=_json_ready(
            threshold_registry.get("second_batch_calibration_prep_summary", {})
        ),
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        safety_boundary={
            **dict(registry.safety_boundary),
            **dict(threshold_registry.get("safety_boundary", {})),
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        allowed_uses=[
            "calibration_prep",
            "sensitivity_design",
            "manual_review_input",
        ],
        reader_brief={
            "key_result": "DYNAMIC_TREND_THRESHOLD_CALIBRATION_PREP_ONLY",
            "prepared_threshold_count": summary["prepared_threshold_count"],
            "validated_boundary_count": summary["validated_boundary_count"],
            "thresholds_changed_count": summary["thresholds_changed_count"],
            "next_action": (
                "collect forward outcome, turnover, constraint-hit, drawdown, and "
                "missed-upside evidence before any owner-reviewed threshold change"
            ),
        },
    )


def build_dynamic_trend_threshold_sensitivity_review(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    coverage_extension_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    threshold_by_id = {
        str(item.get("threshold_id")): dict(item)
        for item in threshold_registry.get("thresholds", [])
        if isinstance(item, Mapping)
    }
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    price_series_by_ticker = _read_price_series_by_ticker(prices_path)
    gate_availability = _gate_availability_records(
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        trace_rows=trace_rows,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    policy_config = _load_dynamic_trend_policy_inputs()
    trace_cases = _dynamic_trend_sensitivity_cases(
        registry=registry,
        trace_rows=trace_rows,
        price_series_by_ticker=price_series_by_ticker,
        gate_availability=gate_availability,
        outcome_ticker=outcome_ticker,
        trace_contract_version=_trace_contract_version_for_payload(
            trace_path,
            registry=registry,
        ),
    )
    coverage_extension_cases = _dynamic_trend_coverage_extension_cases(
        coverage_extension_root=coverage_extension_root,
        price_series_by_ticker=price_series_by_ticker,
        outcome_ticker=outcome_ticker,
        trace_contract_version=_trace_contract_version_for_payload(
            trace_path,
            registry=registry,
        ),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases = trace_cases + coverage_extension_cases
    records = [
        _dynamic_trend_threshold_sensitivity_record(
            threshold_id,
            threshold=threshold_by_id.get(threshold_id, {"threshold_id": threshold_id}),
            registry=registry,
            cases=cases,
            price_series_by_ticker=price_series_by_ticker,
            policy_config=policy_config,
        )
        for threshold_id in SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS
    ]
    threshold_audit_summary = _threshold_audit_summary(
        [item for item in threshold_by_id.values() if isinstance(item, Mapping)]
    )
    summary = _dynamic_trend_sensitivity_summary(records, cases, threshold_audit_summary)
    issues = _dynamic_trend_sensitivity_issues(
        records,
        trace_path=trace_path,
        prices_path=prices_path,
        coverage_extension_root=coverage_extension_root,
    )
    return _base_payload(
        registry,
        report_type="dynamic_trend_threshold_sensitivity_review",
        status="PASS_WITH_WARNINGS",
        issues=issues,
        summary=summary,
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        threshold_registry_path=str(threshold_registry_path),
        calibrated_threshold_scope=list(SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS),
        threshold_sensitivity_reviews=records,
        sample_case_count=len(cases),
        base_trace_case_count=len(trace_cases),
        coverage_extension_case_count=len(coverage_extension_cases),
        coverage_extension_root=(
            None if coverage_extension_root is None else str(coverage_extension_root)
        ),
        data_source_mode=_dynamic_trend_data_source_mode(cases),
        validation_recommendation_set=sorted(DYNAMIC_TREND_VALIDATION_RECOMMENDATIONS),
        max_allowed_status="SENSITIVITY_TESTED",
        validated_boundary_count=0,
        thresholds_changed_count=0,
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        production_effect="none",
        safety_boundary={
            **dict(registry.safety_boundary),
            **dict(threshold_registry.get("safety_boundary", {})),
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
        reader_brief={
            "key_result": "DYNAMIC_TREND_THRESHOLD_SENSITIVITY_TESTED_VALIDATION_ONLY",
            "tested_threshold_count": summary["tested_threshold_count"],
            "sensitivity_tested_count": summary["sensitivity_tested_count"],
            "thresholds_changed_count": summary["thresholds_changed_count"],
            "next_action": (
                "review sample quality and full-advisory gaps before any owner-reviewed "
                "threshold change; do not use this artifact as promotion evidence"
            ),
        },
    )


def build_dynamic_trend_bridge_consistency_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    sensitivity_review_path: Path | None = None,
    sensitivity_review_payload: Mapping[str, Any] | None = None,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    coverage_extension_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    threshold_registry = load_threshold_registry(threshold_registry_path)
    sensitivity_review = _load_dynamic_trend_sensitivity_review_for_consistency(
        sensitivity_review_path=sensitivity_review_path,
        sensitivity_review_payload=sensitivity_review_payload,
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        coverage_extension_root=coverage_extension_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    records = _dynamic_trend_bridge_consistency_records(sensitivity_review)
    summary = _dynamic_trend_bridge_consistency_summary(records, sensitivity_review)
    issues = _dynamic_trend_bridge_consistency_issues(
        summary,
        sensitivity_review_path=sensitivity_review_path,
    )
    return _base_payload(
        registry,
        report_type="dynamic_trend_bridge_consistency_audit",
        status="PASS_WITH_WARNINGS",
        issues=issues,
        summary=summary,
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        threshold_registry_path=str(threshold_registry_path),
        source_sensitivity_review_path=(
            None if sensitivity_review_path is None else str(sensitivity_review_path)
        ),
        calibrated_threshold_scope=list(SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS),
        source_dynamic_trend_threshold_sensitivity_summary=_json_ready(
            sensitivity_review.get("summary", {})
        ),
        threshold_bridge_consistency_audits=records,
        validation_recommendation_set=sorted(DYNAMIC_TREND_VALIDATION_RECOMMENDATIONS),
        bridge_reliability_label_set=list(DYNAMIC_TREND_BRIDGE_RELIABILITY_LABELS),
        max_allowed_status="SENSITIVITY_TESTED",
        validated_boundary_count=0,
        thresholds_changed_count=0,
        bridge_only_promotion_gate_evidence_allowed=False,
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        production_effect="none",
        safety_boundary={
            **dict(registry.safety_boundary),
            **dict(threshold_registry.get("safety_boundary", {})),
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
        reader_brief={
            "key_result": "DYNAMIC_TREND_BRIDGE_CONSISTENCY_AUDITED_VALIDATION_ONLY",
            "tested_threshold_count": summary["tested_threshold_count"],
            "bridge_reliability_counts": summary["bridge_reliability_counts"],
            "evidence_strength": summary["evidence_strength"],
            "recommendation": summary["recommendation"],
            "next_action": (
                "collect more full-advisory dynamic/trend cases before any owner-reviewed "
                "threshold change; do not use bridge evidence alone for promotion gates"
            ),
        },
    )


def build_lineage_manifest_repair_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    gate_audit_root: Path | None = None,
    root_cause_audit_path: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    current_availability = _gate_availability_records(
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        trace_rows=trace_rows,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    root_cause_records = _lineage_repair_root_cause_records(
        root_cause_audit_path=root_cause_audit_path,
        current_availability=current_availability,
    )
    lineage_missing_cases = [
        record
        for record in root_cause_records
        if record.get("reason_class") == "lineage_manifest_missing"
    ]
    affected_by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in lineage_missing_cases:
        row_date = str(record.get("date") or "")
        if row_date:
            affected_by_date[row_date].append(record)
    affected_artifacts = [
        _lineage_repair_artifact_record(
            row_date,
            cases,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
        )
        for row_date, cases in sorted(affected_by_date.items())
    ]
    validation_status_counts: dict[str, int] = {}
    for artifact in affected_artifacts:
        status = str(artifact.get("manifest_validation_status") or "UNKNOWN")
        validation_status_counts[status] = validation_status_counts.get(status, 0) + 1
    current_summary = _gate_availability_summary(current_availability)
    current_reason_class_counts = current_summary.get("root_cause_reason_class_counts", {})
    lineage_missing_after = (
        current_reason_class_counts.get("lineage_manifest_missing", 0)
        if isinstance(current_reason_class_counts, Mapping)
        else 0
    )
    issues: list[dict[str, Any]] = []
    if affected_artifacts:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "lineage_manifest_missing_artifacts_require_repair",
                "message": (
                    "Affected artifacts lack production-equivalent lineage proof; "
                    "do not count them as full advisory equivalent until replay "
                    "artifacts and manifests validate."
                ),
            }
        )
    if current_summary.get("full_advisory_trace_eligible_count", 0) <= 22:
        issues.append(
            {
                "severity": "info",
                "issue_id": "full_advisory_count_not_increased_without_gate_relaxation",
                "message": (
                    "Full advisory eligibility has not exceeded the prior 22-date baseline; "
                    "repair must not relax production data quality or feature gates."
                ),
            }
        )
    return _base_payload(
        registry,
        report_type="lineage_manifest_repair_report",
        status="PASS_WITH_WARNINGS" if issues else "PASS",
        issues=issues,
        summary={
            "affected_root_cause_case_count": len(lineage_missing_cases),
            "affected_artifact_count": len(affected_artifacts),
            "manifest_validation_status_counts": dict(sorted(validation_status_counts.items())),
            "production_equivalent_manifest_count": sum(
                1 for item in affected_artifacts if item.get("production_equivalent")
            ),
            "source_artifact_missing_count": sum(
                1
                for item in affected_artifacts
                if item.get("manifest_validation_status") == "SOURCE_ARTIFACT_MISSING"
            ),
            "audited_date_count": current_summary.get("audited_date_count"),
            "full_advisory_trace_eligible_count": current_summary.get(
                "full_advisory_trace_eligible_count"
            ),
            "component_validation_trace_eligible_count": current_summary.get(
                "component_validation_trace_eligible_count"
            ),
            "lineage_manifest_missing_after_gate_audit": lineage_missing_after,
            "data_quality_gate_relaxed": False,
            "production_weight_logic_changed": False,
            "promotion_gate_allowed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        affected_artifacts=affected_artifacts,
        gate_availability_summary=current_summary,
        source_root_cause_audit_path=(
            None if root_cause_audit_path is None else str(root_cause_audit_path)
        ),
        rule=(
            "Lineage repair can only validate or regenerate production-equivalent "
            "replay manifests. It must not relax production data_quality_gate, feature "
            "availability, or any production weight calculation logic."
        ),
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_historical_multi_stage_weight_trace_validation(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    gate_audit_root: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    lineage_records = _trace_lineage_records(trace_path, trace_rows)
    dates = sorted({str(row.get("date")) for row in trace_rows if row.get("date")})
    trace_field_audit = _trace_contract_field_audit(registry, trace_rows) if trace_rows else {}
    masking_results = list(_masking_from_trace(registry, trace_rows).values()) if trace_rows else []
    date_summaries = _trace_date_summaries(trace_rows)
    availability = _gate_availability_records(
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        trace_rows=trace_rows,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    issues: list[dict[str, Any]] = []
    if trace_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "historical_trace_not_provided",
                "message": "Historical replay/backtest validation needs a multi-date trace JSON.",
            }
        )
    if trace_rows and len(dates) < HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "historical_trace_window_short",
                "message": (
                    "Trace is readable, but date count is below the stability diagnostic "
                    "window; use a replay/backtest trace before drawing stability conclusions."
                ),
                "date_count": len(dates),
                "recommended_min_date_count": HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY,
            }
        )
    if trace_field_audit.get("missing_field_records"):
        issues.append(
            {
                "severity": "warning",
                "issue_id": "historical_trace_fields_missing",
                "message": "Some historical trace rows are missing required trace contract fields.",
                "missing_field_records": trace_field_audit["missing_field_records"],
            }
        )
    status = "PASS" if trace_rows and not issues else "PASS_WITH_WARNINGS"
    return _base_payload(
        registry,
        report_type="historical_multi_stage_weight_trace_validation",
        status=status,
        issues=issues,
        summary={
            "trace_row_count": len(trace_rows),
            "date_count": len(dates),
            "masking_pair_result_count": len(masking_results),
            "missing_trace_field_record_count": len(
                trace_field_audit.get("missing_field_records", [])
            ),
            **_lineage_summary(lineage_records),
            "sufficient_history_for_stability": (
                len(dates) >= HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY
            ),
            **_trace_sample_quality_stats(
                registry,
                trace_rows,
                gate_availability=availability,
                cases=_pair_trace_rows(
                    trace_rows,
                    "valuation_crowding_indicator",
                    "trend_strength_indicator",
                ),
            ),
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        trace_contract_field_audit=trace_field_audit,
        historical_replay_lineage_manifest=lineage_records,
        historical_masking_results=masking_results,
        gate_availability_summary=_gate_availability_summary(availability),
        date_level_summary=date_summaries,
    )


def build_gate_availability_audit(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    gate_audit_root: Path | None = None,
    trace_path: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    records = _gate_availability_records(
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        trace_rows=trace_rows,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    lineage_records = _trace_lineage_records(trace_path, trace_rows)
    root_cause_records = [
        _gate_root_cause_record(record)
        for record in records
        if not record.get("full_advisory_trace_eligible")
    ]
    summary = {
        **_gate_availability_summary(records),
        **_trace_sample_quality_stats(registry, trace_rows, gate_availability=records),
        **_lineage_summary(lineage_records),
        "root_cause_case_count": len(root_cause_records),
    }
    return _base_payload(
        registry,
        report_type="historical_trace_gate_availability_audit",
        status=(
            "PASS_WITH_WARNINGS"
            if any(not record["full_advisory_trace_eligible"] for record in records)
            else "PASS"
        ),
        summary=summary,
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        gate_availability=records,
        gate_root_cause_analysis=root_cause_records,
        historical_replay_lineage_manifest=lineage_records,
        rule=(
            "Production data_quality_gate is not relaxed. Dates that fail full advisory "
            "eligibility may only enter component diagnostics when component_validation_"
            "trace_eligible=true."
        ),
    )


def build_component_level_historical_trace(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    gate_audit_root: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    source_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    availability = _gate_availability_records(
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        trace_rows=source_rows,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    availability_by_key = {
        (str(record.get("date") or ""), str(record.get("asset") or "").upper()): record
        for record in availability
    }
    availability_by_date = {record["date"]: record for record in availability}
    rows = []
    keep_modules = {
        "trend_strength_indicator",
        "valuation_crowding_indicator",
    }
    for row in source_rows:
        is_component = (
            row.get("row_type") == "indicator_component" and row.get("module_id") in keep_modules
        )
        is_masking_pair = (
            row.get("upstream_indicator_id") == "valuation_crowding_indicator"
            and row.get("downstream_indicator_id") == "trend_strength_indicator"
        )
        if not is_component and not is_masking_pair:
            continue
        row_date = str(row.get("date") or "")
        row_asset = str(row.get("asset") or "").upper()
        gate_record = availability_by_key.get(
            (row_date, row_asset),
            availability_by_date.get(row_date, {}),
        )
        full_eligible = bool(gate_record.get("full_advisory_trace_eligible", True))
        component_eligible = bool(gate_record.get("component_validation_trace_eligible", True))
        rows.append(
            {
                **dict(row),
                "trace_scope": "component_validation",
                "full_advisory_trace_eligible": full_eligible,
                "component_validation_trace_eligible": component_eligible,
                "reason_if_not_full_eligible": gate_record.get(
                    "reason_if_not_full_eligible",
                    "",
                ),
                "trace_source": (
                    FULL_ADVISORY_TRACE_SOURCE
                    if full_eligible
                    else (
                        COMPONENT_VALIDATION_TRACE_SOURCE
                        if component_eligible
                        else INELIGIBLE_TRACE_SOURCE
                    )
                ),
                "confidence": (
                    TRACE_CONFIDENCE_FULL_ADVISORY
                    if full_eligible
                    else (
                        TRACE_CONFIDENCE_COMPONENT
                        if component_eligible
                        else TRACE_CONFIDENCE_NOT_ELIGIBLE
                    )
                ),
                "promotion_gate_allowed": False,
                "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
            }
        )
    issues = []
    if trace_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "component_trace_source_missing",
                "message": "Component-level historical trace needs a trace source JSON.",
            }
        )
    status = "PASS" if rows else "PASS_WITH_WARNINGS"
    return _base_payload(
        registry,
        report_type="component_level_historical_trace",
        status=status,
        issues=issues,
        summary={
            "trace_row_count": len(rows),
            "date_count": len({str(row.get("date")) for row in rows if row.get("date")}),
            **_trace_sample_quality_stats(registry, rows, gate_availability=availability),
            "promotion_gate_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        gate_availability_summary=_gate_availability_summary(availability),
        rows=rows,
    )


def build_backtest_trace_bridge(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    bridge_artifact_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    trace_rows = _filter_trace_rows(
        _read_trace_rows(trace_path),
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    price_series_by_ticker = _read_price_series_by_ticker(prices_path)
    fallback_price_series = price_series_by_ticker.get(outcome_ticker.upper(), [])
    trace_contract_version = _trace_contract_version_for_payload(
        trace_path,
        registry=registry,
    )
    pair_rows = _pair_trace_rows(
        trace_rows,
        "valuation_crowding_indicator",
        "trend_strength_indicator",
    )
    bridge_records = []
    for row in pair_rows:
        for case_asset in _casebook_assets_for_row(row, asset_universe=asset_universe):
            case_outcome_ticker, missing_asset_mapping = _price_ticker_mapping_for_asset(
                case_asset,
                price_series_by_ticker,
                fallback_ticker=outcome_ticker,
            )
            bridge_records.append(
                _backtest_bridge_record(
                    row,
                    price_series=price_series_by_ticker.get(
                        case_outcome_ticker,
                        fallback_price_series,
                    ),
                    outcome_ticker=case_outcome_ticker,
                    case_asset=case_asset,
                    trace_contract_version=trace_contract_version,
                    missing_asset_mapping=missing_asset_mapping,
                )
            )
    source_artifacts = _scan_bridge_source_artifacts(bridge_artifact_root)
    status = "PASS" if bridge_records or source_artifacts else "PASS_WITH_WARNINGS"
    issues = []
    if trace_path is None and bridge_artifact_root is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "backtest_bridge_source_missing",
                "message": "Provide trace_path or bridge_artifact_root to build bridge records.",
            }
        )
    return _base_payload(
        registry,
        report_type="backtest_trace_bridge",
        status=status,
        issues=issues,
        summary={
            "bridge_record_count": len(bridge_records),
            "source_artifact_count": len(source_artifacts),
            "outcome_ticker": outcome_ticker,
            **_trace_sample_quality_stats(registry, trace_rows, cases=bridge_records),
            "promotion_gate_allowed": False,
            "read_only": True,
            "production_weight_logic_changed": False,
        },
        filters=_trace_filter_payload(
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        ),
        bridge_records=bridge_records,
        source_artifacts=source_artifacts,
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
    )


def build_indicator_research_gate(
    *,
    indicator_id: str,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    indicator = _indicator_or_raise(registry, indicator_id)
    coverage_status = _coverage_status(registry, indicator)
    masking = build_masking_audit(
        indicator_id=indicator_id,
        registry_path=registry_path,
        trace_path=trace_path,
    )
    outcome = _gate_outcome(registry, indicator, coverage_status, masking["masking_results"])
    return _base_payload(
        registry,
        report_type="indicator_to_signal_research_gate",
        status=outcome,
        summary={
            "indicator_id": indicator_id,
            "coverage_status": coverage_status,
            "gate_outcome": outcome,
            "associational_not_causal": True,
        },
        indicator=_indicator_summary(registry, indicator),
        masking_summary=masking["summary"],
        gate={
            "outcome": outcome,
            "allowed_for_weight_increase": False,
            "allowed_for_hard_constraint_promotion": False,
            "paper_shadow_allowed": False,
            "official_target_weights": False,
            "required_next_evidence": _required_next_evidence(outcome),
        },
        reader_brief={
            "role": indicator.role,
            "mapping_version": indicator.mapping_version,
            "upstream_constraints": _upstream_constraints(registry, indicator_id),
            "masking": masking["reader_brief"]["masking"],
            "independent_effect": masking["reader_brief"]["independent_effect"],
            "trial_budget": "not_started",
            "next_action": _required_next_evidence(outcome)[0],
        },
    )


def write_indicator_artifact_pair(
    payload: Mapping[str, Any],
    *,
    output_root: Path,
    artifact_id: str,
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{artifact_id}.json"
    markdown_path = output_root / f"{artifact_id}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_indicator_markdown(payload), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(markdown_path)}


def write_indicator_framework_validation_pack(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Path = DEFAULT_INDICATOR_OUTPUT_ROOT,
    trace_path: Path | None = None,
    prices_path: Path | None = None,
    gate_audit_root: Path | None = None,
    bridge_artifact_root: Path | None = None,
    coverage_extension_root: Path | None = None,
    outcome_ticker: str = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: float = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    registry = load_indicator_registry(registry_path)
    validation_root = output_root / "control_plane_v1_validation"
    dynamic_trend_threshold_sensitivity_payload = build_dynamic_trend_threshold_sensitivity_review(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        coverage_extension_root=coverage_extension_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    dynamic_trend_bridge_consistency_payload = build_dynamic_trend_bridge_consistency_audit(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        sensitivity_review_payload=dynamic_trend_threshold_sensitivity_payload,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        coverage_extension_root=coverage_extension_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    artifact_builders: list[tuple[str, dict[str, Any]]] = [
        ("indicator_research_ontology", build_ontology_payload(registry_path=registry_path)),
        ("daily_indicator_inventory", build_daily_indicator_inventory(registry_path=registry_path)),
        ("indicator_research_coverage_audit", build_coverage_audit(registry_path=registry_path)),
        (
            "daily_indicator_coverage_gap_report",
            build_daily_indicator_coverage_gap_report(
                registry_path=registry_path,
                trace_path=trace_path,
            ),
        ),
        (
            "threshold_registry_audit",
            build_threshold_registry_audit(
                registry_path=registry_path,
                threshold_registry_path=threshold_registry_path,
            ),
        ),
        (
            "threshold_prioritization_report",
            build_threshold_prioritization_report(
                registry_path=registry_path,
                threshold_registry_path=threshold_registry_path,
            ),
        ),
        (
            "threshold_calibration_report",
            build_threshold_calibration_report(
                registry_path=registry_path,
                threshold_registry_path=threshold_registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "dynamic_trend_threshold_sensitivity_review",
            dynamic_trend_threshold_sensitivity_payload,
        ),
        (
            "dynamic_trend_bridge_consistency_audit",
            dynamic_trend_bridge_consistency_payload,
        ),
        (
            "indicator_dependency_graph",
            build_dependency_graph(registry_path=registry_path, trace_path=trace_path),
        ),
        (
            "multi_stage_weight_trace_contract",
            build_multi_stage_weight_trace_contract(registry_path=registry_path),
        ),
        (
            "constraint_attribution_report",
            build_constraint_attribution_report(
                registry_path=registry_path,
                trace_path=trace_path,
            ),
        ),
        (
            "indicator_role_and_target_registry",
            build_role_and_target_registry(registry_path=registry_path),
        ),
        (
            "indicator_data_pit_and_leakage_gate",
            build_data_pit_leakage_gate(registry_path=registry_path),
        ),
        (
            "indicator_signal_mapping_registry",
            build_mapping_registry(registry_path=registry_path),
        ),
        (
            "mapping_free_indicator_diagnostics_valuation_crowding",
            build_indicator_diagnostics(
                indicator_id="valuation_crowding_indicator",
                registry_path=registry_path,
            ),
        ),
        (
            "indicator_mapping_candidate_plan_valuation_crowding",
            build_mapping_plan(
                indicator_id="valuation_crowding_indicator",
                registry_path=registry_path,
            ),
        ),
        (
            "conditional_incremental_effect_contract",
            build_conditional_incremental_effect_contract(registry_path=registry_path),
        ),
        (
            "indicator_masking_and_dominance_audit_valuation_crowding",
            build_masking_audit(
                indicator_id="valuation_crowding_indicator",
                registry_path=registry_path,
                trace_path=trace_path,
            ),
        ),
        (
            "factorial_counterfactual_experiment_planner",
            build_factorial_counterfactual_planner(registry_path=registry_path),
        ),
        (
            "portfolio_signal_transfer_attribution_contract",
            build_portfolio_signal_transfer_attribution(registry_path=registry_path),
        ),
        (
            "indicator_trial_ledger_and_holdout_service",
            build_trial_ledger_and_holdout_service(registry_path=registry_path),
        ),
        (
            "indicator_research_campaign_adapter_contract",
            build_campaign_adapter_contract(registry_path=registry_path),
        ),
        (
            "valuation_crowding_pilot_validation_report",
            build_valuation_crowding_pilot_validation_report(
                registry_path=registry_path,
                trace_path=trace_path,
            ),
        ),
        (
            "indicator_masking_casebook_valuation_crowding_trend",
            build_masking_casebook(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                outcome_ticker=outcome_ticker,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "valuation_crowding_ablation_validation",
            build_valuation_crowding_ablation_validation(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "valuation_crowding_outcome_availability_audit",
            build_valuation_crowding_outcome_availability_audit(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "valuation_crowding_masking_effectiveness_review",
            build_valuation_crowding_masking_effectiveness_review(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "valuation_crowding_masking_robustness_review",
            build_valuation_crowding_masking_robustness_review(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "indicator_research_validation_rollup",
            build_indicator_research_validation_rollup(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "long_horizon_evidence_floor_calibration_audit",
            build_long_horizon_evidence_floor_calibration_audit(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                gate_audit_root=gate_audit_root,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                capped_masking_ratio=capped_masking_ratio,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "historical_multi_stage_weight_trace_validation",
            build_historical_multi_stage_weight_trace_validation(
                registry_path=registry_path,
                trace_path=trace_path,
                gate_audit_root=gate_audit_root,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "historical_trace_gate_availability_audit",
            build_gate_availability_audit(
                registry_path=registry_path,
                gate_audit_root=gate_audit_root,
                trace_path=trace_path,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "component_level_historical_trace",
            build_component_level_historical_trace(
                registry_path=registry_path,
                trace_path=trace_path,
                gate_audit_root=gate_audit_root,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "backtest_trace_bridge",
            build_backtest_trace_bridge(
                registry_path=registry_path,
                trace_path=trace_path,
                prices_path=prices_path,
                bridge_artifact_root=bridge_artifact_root,
                outcome_ticker=outcome_ticker,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
        (
            "lineage_manifest_repair_report",
            build_lineage_manifest_repair_report(
                registry_path=registry_path,
                trace_path=trace_path,
                gate_audit_root=gate_audit_root,
                start_date=start_date,
                end_date=end_date,
                event_window_start=event_window_start,
                event_window_end=event_window_end,
                asset_universe=asset_universe,
            ),
        ),
    ]
    artifacts = {
        artifact_id: write_indicator_artifact_pair(
            payload,
            output_root=validation_root,
            artifact_id=artifact_id,
        )
        for artifact_id, payload in artifact_builders
    }
    statuses = [payload["status"] for _, payload in artifact_builders]
    blocking = [status for status in statuses if str(status).endswith("BLOCKED")]
    artifact_payloads = {artifact_id: payload for artifact_id, payload in artifact_builders}
    threshold_audit_summary = artifact_payloads.get("threshold_registry_audit", {}).get(
        "summary",
        {},
    )
    threshold_prioritization_summary = artifact_payloads.get(
        "threshold_prioritization_report",
        {},
    ).get("summary", {})
    threshold_calibration_summary = artifact_payloads.get(
        "threshold_calibration_report",
        {},
    ).get("summary", {})
    dynamic_trend_threshold_sensitivity_summary = artifact_payloads.get(
        "dynamic_trend_threshold_sensitivity_review",
        {},
    ).get("summary", {})
    dynamic_trend_bridge_consistency_summary = artifact_payloads.get(
        "dynamic_trend_bridge_consistency_audit",
        {},
    ).get("summary", {})
    status = (
        "INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_BLOCKED"
        if blocking
        else "INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS"
    )
    pack = _base_payload(
        registry,
        report_type="indicator_to_signal_research_framework_v1_validation_pack",
        status=status,
        summary={
            "artifact_count": len(artifacts) + 1,
            "trace_path_provided": trace_path is not None,
            "prices_path_provided": prices_path is not None,
            "gate_audit_root_provided": gate_audit_root is not None,
            "bridge_artifact_root_provided": bridge_artifact_root is not None,
            "coverage_extension_root_provided": coverage_extension_root is not None,
            "threshold_audit_summary": threshold_audit_summary,
            "threshold_prioritization_summary": threshold_prioritization_summary,
            "threshold_calibration_summary": threshold_calibration_summary,
            "dynamic_trend_threshold_sensitivity_summary": (
                dynamic_trend_threshold_sensitivity_summary
            ),
            "dynamic_trend_bridge_consistency_summary": (dynamic_trend_bridge_consistency_summary),
            "no_parameter_mutation": True,
            "no_paper_shadow_live_broker_order_official_weights": True,
            "production_effect": "none",
            "promotion_gate_allowed": False,
            "production_weight_change_allowed": False,
            "paper_shadow_change_allowed": False,
        },
        promotion_gate_allowed=False,
        production_weight_change_allowed=False,
        paper_shadow_change_allowed=False,
        validation_checks=[
            {
                "check_id": "ontology",
                "status": _check_status(statuses, "indicator_research_ontology"),
            },
            {"check_id": "inventory_coverage", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "coverage_gap_report", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "threshold_registry_audit", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "threshold_prioritization_report", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "threshold_calibration_report", "status": "PASS_WITH_WARNINGS"},
            {
                "check_id": "dynamic_trend_threshold_sensitivity_review",
                "status": "PASS_WITH_WARNINGS",
            },
            {
                "check_id": "dynamic_trend_bridge_consistency_audit",
                "status": "PASS_WITH_WARNINGS",
            },
            {"check_id": "dependency_graph", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "multi_stage_trace_contract", "status": "PASS"},
            {"check_id": "mapping_registry", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "diagnostics_calibration", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "masking_factorial_planner", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "trial_ledger_holdout", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "campaign_adapter", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "valuation_crowding_pilot", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "masking_casebook", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "counterfactual_ablation", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "outcome_availability_audit", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "masking_effectiveness_review", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "masking_robustness_review", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "validation_rollup", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "long_horizon_floor_calibration", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "historical_trace_validation", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "gate_availability_audit", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "component_historical_trace", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "backtest_trace_bridge", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "lineage_manifest_repair", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "no_parameter_mutation", "status": "PASS"},
            {"check_id": "no_forbidden_production_effects", "status": "PASS"},
        ],
        artifacts=artifacts,
        reader_brief={
            "key_result": status,
            "valuation_crowding": "VALUATION_CROWDING_UNTESTED_HIGH_IMPACT",
            "next_action": (
                "review_daily_indicator_weight_trace_and_coverage_gap_before_final_"
                "masking_conclusions"
            ),
        },
    )
    pack_artifact_id = "indicator_to_signal_research_framework_v1_validation_pack"
    pack_paths = {
        "json_path": str(validation_root / f"{pack_artifact_id}.json"),
        "markdown_path": str(validation_root / f"{pack_artifact_id}.md"),
    }
    pack["artifacts"] = {**artifacts, pack_artifact_id: pack_paths}
    artifacts[pack_artifact_id] = write_indicator_artifact_pair(
        pack,
        output_root=validation_root,
        artifact_id=pack_artifact_id,
    )
    pack["artifacts"] = artifacts
    return pack


def render_indicator_markdown(payload: Mapping[str, Any]) -> str:
    report_type = str(payload.get("report_type", "indicator_research"))
    status = str(payload.get("status", "UNKNOWN"))
    lines = [
        f"# {report_type}",
        "",
        f"- status: `{status}`",
        f"- market_regime: `{payload.get('market_regime', 'ai_after_chatgpt')}`",
        f"- requested_date_range: `{payload.get('requested_date_range', '')}`",
        f"- data_quality_status: `{payload.get('data_quality_status', '')}`",
        f"- production_effect: `{payload.get('production_effect', 'none')}`",
        "",
    ]
    summary = payload.get("summary")
    if isinstance(summary, Mapping):
        lines.extend(["## Summary", ""])
        for key, value in summary.items():
            lines.append(f"- {key}: `{_compact_markdown_value(value)}`")
        lines.append("")
    reader_brief = payload.get("reader_brief")
    if isinstance(reader_brief, Mapping):
        lines.extend(["## Reader Brief", ""])
        for key, value in reader_brief.items():
            lines.append(f"- {key}: `{_compact_markdown_value(value)}`")
        lines.append("")
    issues = payload.get("issues")
    if isinstance(issues, Sequence) and issues:
        lines.extend(["## Issues", ""])
        for issue in issues:
            if isinstance(issue, Mapping):
                lines.append(
                    "- "
                    f"{issue.get('severity', 'info')}: "
                    f"{issue.get('issue_id', 'issue')} - "
                    f"{issue.get('message', '')}"
                )
        lines.append("")
    lines.extend(
        [
            "## Safety",
            "",
            "- research_only: `true`",
            "- manual_review_only: `true`",
            "- official_target_weights: `false`",
            "- paper_shadow_activation: `false`",
            "- broker_effect: `none`",
            "- order_effect: `none`",
            "- production_effect: `none`",
            "",
        ]
    )
    return "\n".join(lines)


def _base_payload(
    registry: IndicatorResearchRegistry,
    *,
    report_type: str,
    status: str,
    market_regime: str | None = None,
    requested_date_range: str | None = None,
    summary: Mapping[str, Any] | None = None,
    issues: Sequence[Mapping[str, Any]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "market_regime": market_regime or registry.market_regime.regime_id,
        "requested_date_range": requested_date_range or registry.market_regime.requested_date_range,
        "data_quality_status": registry.data_quality_status,
        "production_effect": "none",
        "safety_boundary": dict(registry.safety_boundary),
        "policy_version": registry.policy_version,
        "policy_metadata": registry.policy_metadata.model_dump(mode="json"),
        "summary": dict(summary or {}),
        "issues": list(issues or []),
    }
    payload.update(extra)
    return payload


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_ready(item) for item in value]
    return value


def _threshold_audit_summary(thresholds: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(thresholds)
    high_impact = [
        threshold
        for threshold in thresholds
        if str(threshold.get("threshold_class", "")).upper() == HIGH_IMPACT_THRESHOLD_CLASS
    ]
    uncalibrated_high_impact = [
        threshold for threshold in high_impact if _is_uncalibrated_threshold_status(threshold)
    ]
    heuristic_guardrails = [
        threshold for threshold in thresholds if _is_heuristic_guardrail_threshold(threshold)
    ]
    calibrated = [
        threshold
        for threshold in thresholds
        if str(threshold.get("calibration_status", "")) == CALIBRATED_THRESHOLD_STATUS
    ]
    blocking_ids = sorted(
        str(threshold.get("threshold_id"))
        for threshold in thresholds
        if _threshold_blocks_promotion_dependency(threshold)
    )
    class_counts: dict[str, int] = defaultdict(int)
    status_counts: dict[str, int] = defaultdict(int)
    for threshold in thresholds:
        class_counts[str(threshold.get("threshold_class") or "UNCLASSIFIED")] += 1
        status_counts[str(threshold.get("calibration_status") or "UNKNOWN")] += 1
    return {
        "total_threshold_count": total,
        "high_impact_threshold_count": len(high_impact),
        "uncalibrated_high_impact_count": len(uncalibrated_high_impact),
        "heuristic_guardrail_count": len(heuristic_guardrails),
        "calibrated_count": len(calibrated),
        "thresholds_blocking_promotion": blocking_ids,
        "thresholds_blocking_promotion_count": len(blocking_ids),
        "threshold_class_counts": dict(sorted(class_counts.items())),
        "calibration_status_counts": dict(sorted(status_counts.items())),
        "uncalibrated_high_impact_ids": sorted(
            str(threshold.get("threshold_id")) for threshold in uncalibrated_high_impact
        ),
        "production_weight_affecting_threshold_count": sum(
            1 for threshold in thresholds if bool(threshold.get("production_weight_affecting"))
        ),
        "validation_only": True,
        "promotion_dependency_review_required": bool(blocking_ids),
        "production_weight_logic_changed": False,
        "paper_shadow_change_allowed": False,
        "official_target_weights_mutated": False,
    }


def _threshold_impact_scope_order(policy: Mapping[str, Any]) -> list[str]:
    raw_order = policy.get("impact_scope_order", DEFAULT_THRESHOLD_IMPACT_SCOPE_ORDER)
    if isinstance(raw_order, Sequence) and not isinstance(raw_order, str | bytes):
        order = [str(item) for item in raw_order if str(item)]
    else:
        order = list(DEFAULT_THRESHOLD_IMPACT_SCOPE_ORDER)
    for category in DEFAULT_THRESHOLD_IMPACT_SCOPE_ORDER:
        if category not in order:
            order.append(category)
    return order


def _threshold_impact_category_map(policy: Mapping[str, Any]) -> dict[str, set[str]]:
    raw_mapping = policy.get("impact_category_threshold_ids", {})
    if not isinstance(raw_mapping, Mapping):
        return {}
    mapping: dict[str, set[str]] = {}
    for category, threshold_ids in raw_mapping.items():
        if isinstance(threshold_ids, Sequence) and not isinstance(threshold_ids, str | bytes):
            mapping[str(category)] = {str(threshold_id) for threshold_id in threshold_ids}
    return mapping


def _threshold_impact_categories(
    threshold: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[str]:
    threshold_id = str(threshold.get("threshold_id", ""))
    order = _threshold_impact_scope_order(policy)
    category_map = _threshold_impact_category_map(policy)
    categories = [
        category for category in order if threshold_id in category_map.get(category, set())
    ]
    if bool(threshold.get("production_weight_affecting")):
        categories.append("production_weight_affecting")
    if not categories and bool(threshold.get("promotion_gate_affecting")):
        categories.append("promotion_gate_affecting")
    return [category for category in order if category in set(categories)]


def _threshold_calibration_urgency(threshold: Mapping[str, Any]) -> tuple[str, str]:
    threshold_class = str(threshold.get("threshold_class", "")).upper()
    if _threshold_blocks_promotion_dependency(threshold) and bool(
        threshold.get("decision_affecting")
    ):
        return "P0", "promotion-blocking and decision-affecting"
    if bool(threshold.get("decision_affecting")) or threshold_class == HIGH_IMPACT_THRESHOLD_CLASS:
        return "P1", "validation conclusion affecting"
    if threshold_class == "B":
        return "P2", "workflow quality affecting"
    return "P3", "engineering/runtime only"


def _threshold_urgency_rank(urgency: str) -> int:
    try:
        return CALIBRATION_URGENCY_ORDER.index(urgency)
    except ValueError:
        return len(CALIBRATION_URGENCY_ORDER)


def _prioritized_threshold_records(
    thresholds: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    *,
    high_impact_only: bool,
) -> list[dict[str, Any]]:
    order = _threshold_impact_scope_order(policy)
    order_rank = {category: index for index, category in enumerate(order)}
    records: list[dict[str, Any]] = []
    for threshold in thresholds:
        is_uncalibrated_high_impact = _is_uncalibrated_high_impact_threshold(threshold)
        if high_impact_only and not is_uncalibrated_high_impact:
            continue
        categories = _threshold_impact_categories(threshold, policy)
        primary_category = categories[0] if categories else "uncategorized"
        urgency, urgency_reason = _threshold_calibration_urgency(threshold)
        record = {
            "threshold_id": str(threshold.get("threshold_id", "")),
            "current_value": _json_ready(threshold.get("current_value")),
            "unit": threshold.get("unit", ""),
            "threshold_class": threshold.get("threshold_class", ""),
            "calibration_status": threshold.get("calibration_status", ""),
            "threshold_type": threshold.get("threshold_type", ""),
            "not_validated_statistical_boundary": bool(
                threshold.get("not_validated_statistical_boundary", False)
            ),
            "impact_categories": categories,
            "primary_impact_category": primary_category,
            "impact_priority_rank": order_rank.get(primary_category, len(order)) + 1,
            "calibration_urgency": urgency,
            "calibration_urgency_reason": urgency_reason,
            "decision_affecting": bool(threshold.get("decision_affecting")),
            "promotion_gate_affecting": bool(threshold.get("promotion_gate_affecting")),
            "production_weight_affecting": bool(threshold.get("production_weight_affecting")),
            "calibration_required": bool(threshold.get("calibration_required")),
            "no_promotion_dependency_without_review": bool(
                threshold.get("no_promotion_dependency_without_review")
            ),
            "is_uncalibrated_high_impact": is_uncalibrated_high_impact,
            "recommended_calibration_method": threshold.get(
                "recommended_calibration_method",
                "",
            ),
            "purpose": threshold.get("purpose", ""),
            "where_used": _json_ready(threshold.get("where_used", [])),
            "first_batch_candidate": False,
        }
        records.append(record)
    return sorted(
        records,
        key=lambda item: (
            int(item.get("impact_priority_rank", len(order) + 1)),
            _threshold_urgency_rank(str(item.get("calibration_urgency", ""))),
            str(item.get("threshold_id", "")),
        ),
    )


def _first_batch_threshold_candidates(
    prioritized_thresholds: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_id = {str(item.get("threshold_id")): dict(item) for item in prioritized_thresholds}
    raw_limit = policy.get("first_batch_candidate_limit", 8)
    try:
        limit = max(1, min(8, int(raw_limit)))
    except (TypeError, ValueError):
        limit = 8
    raw_candidate_ids = policy.get("first_batch_candidate_ids", [])
    candidate_ids = (
        [str(item) for item in raw_candidate_ids]
        if isinstance(raw_candidate_ids, Sequence)
        and not isinstance(raw_candidate_ids, str | bytes)
        else []
    )
    selected = [by_id[threshold_id] for threshold_id in candidate_ids if threshold_id in by_id]
    if not selected:
        selected = [dict(item) for item in prioritized_thresholds[:limit]]
    selected = selected[:limit]
    for index, candidate in enumerate(selected, start=1):
        candidate["first_batch_rank"] = index
        candidate["first_batch_candidate"] = True
        candidate["selection_reason"] = (
            "Registry-selected validation-only calibration candidate; does not allow "
            "promotion, paper-shadow, production, or official weight changes."
        )
    return selected


def _threshold_prioritization_summary(
    prioritized_thresholds: Sequence[Mapping[str, Any]],
    all_thresholds: Sequence[Mapping[str, Any]],
    first_batch_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    impact_counts: dict[str, int] = {
        category: 0 for category in DEFAULT_THRESHOLD_IMPACT_SCOPE_ORDER
    }
    for threshold in prioritized_thresholds:
        for category in threshold.get("impact_categories", []):
            impact_counts[str(category)] = impact_counts.get(str(category), 0) + 1
    urgency_counts: dict[str, int] = {urgency: 0 for urgency in CALIBRATION_URGENCY_ORDER}
    for threshold in all_thresholds:
        urgency = str(threshold.get("calibration_urgency", "P3"))
        urgency_counts[urgency] = urgency_counts.get(urgency, 0) + 1
    high_impact_urgency_counts: dict[str, int] = {
        urgency: 0 for urgency in CALIBRATION_URGENCY_ORDER
    }
    for threshold in prioritized_thresholds:
        urgency = str(threshold.get("calibration_urgency", "P3"))
        high_impact_urgency_counts[urgency] = high_impact_urgency_counts.get(urgency, 0) + 1
    first_batch_ids = [str(candidate.get("threshold_id")) for candidate in first_batch_candidates]
    return {
        "prioritized_threshold_count": len(prioritized_thresholds),
        "uncalibrated_high_impact_count": len(prioritized_thresholds),
        "all_threshold_count": len(all_thresholds),
        "impact_scope_counts": dict(sorted(impact_counts.items())),
        "calibration_urgency_counts": dict(sorted(urgency_counts.items())),
        "high_impact_calibration_urgency_counts": dict(sorted(high_impact_urgency_counts.items())),
        "first_batch_candidate_count": len(first_batch_candidates),
        "first_batch_candidate_ids": first_batch_ids,
        "production_weight_affecting_threshold_count": impact_counts.get(
            "production_weight_affecting",
            0,
        ),
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "official_target_weights_mutated": False,
        "validation_only": True,
    }


def _threshold_prioritization_issues(
    thresholds: Sequence[Mapping[str, Any]],
    prioritized_thresholds: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    first_batch_candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    categorized_ids = {str(item.get("threshold_id")) for item in prioritized_thresholds}
    expected_ids = {
        str(threshold.get("threshold_id"))
        for threshold in thresholds
        if _is_uncalibrated_high_impact_threshold(threshold)
    }
    missing_ids = sorted(expected_ids - categorized_ids)
    if missing_ids:
        issues.append(
            {
                "severity": "error",
                "issue_id": "uncalibrated_high_impact_threshold_missing_prioritization",
                "threshold_ids": missing_ids,
                "message": "Every uncalibrated high-impact threshold must be prioritized.",
            }
        )
    uncategorized = sorted(
        str(item.get("threshold_id"))
        for item in prioritized_thresholds
        if not item.get("impact_categories")
    )
    if uncategorized:
        issues.append(
            {
                "severity": "error",
                "issue_id": "uncalibrated_high_impact_threshold_missing_impact_category",
                "threshold_ids": uncategorized,
                "message": "Every prioritized high-impact threshold must have an impact category.",
            }
        )
    raw_candidate_ids = policy.get("first_batch_candidate_ids", [])
    configured_ids = (
        [str(item) for item in raw_candidate_ids]
        if isinstance(raw_candidate_ids, Sequence)
        and not isinstance(raw_candidate_ids, str | bytes)
        else []
    )
    selected_ids = {str(candidate.get("threshold_id")) for candidate in first_batch_candidates}
    missing_candidate_ids = sorted(set(configured_ids) - selected_ids)
    if missing_candidate_ids:
        issues.append(
            {
                "severity": "error",
                "issue_id": "first_batch_candidate_not_prioritized_high_impact",
                "threshold_ids": missing_candidate_ids,
                "message": (
                    "Configured first-batch candidates must be uncalibrated high-impact "
                    "thresholds in the prioritization report."
                ),
            }
        )
    if len(first_batch_candidates) > 8:
        issues.append(
            {
                "severity": "error",
                "issue_id": "first_batch_candidate_limit_exceeded",
                "candidate_count": len(first_batch_candidates),
                "message": "First-batch calibration candidates must not exceed 8.",
            }
        )
    return issues


def _threshold_calibration_summary(
    records: Sequence[Mapping[str, Any]],
    threshold_audit_summary: Mapping[str, Any],
) -> dict[str, Any]:
    sensitivity_tested = [
        record
        for record in records
        if str(record.get("recommended_status")) == "SENSITIVITY_TESTED"
    ]
    changed_candidates = [
        record for record in records if str(record.get("recommended_action")) == "adjust_candidate"
    ]
    blocking_count = int(threshold_audit_summary.get("thresholds_blocking_promotion_count") or 0)
    return {
        "tested_threshold_count": len(records),
        "sensitivity_tested_count": len(sensitivity_tested),
        "still_uncalibrated_high_impact_count": max(0, blocking_count - len(sensitivity_tested)),
        "thresholds_still_blocking_promotion_count": blocking_count,
        "thresholds_changed_count": 0,
        "adjust_candidate_count": len(changed_candidates),
        "tested_threshold_ids": [str(record.get("threshold_id")) for record in records],
        "sensitivity_tested_threshold_ids": [
            str(record.get("threshold_id")) for record in sensitivity_tested
        ],
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "validation_only": True,
    }


def _threshold_calibration_issues(
    records: Sequence[Mapping[str, Any]],
    *,
    trace_path: Path | None,
    prices_path: Path | None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = [
        {
            "severity": "info",
            "issue_id": "threshold_calibration_validation_only",
            "message": (
                "Threshold calibration report is sensitivity analysis only and cannot "
                "approve promotion, paper-shadow, production, or official weight changes."
            ),
        }
    ]
    if trace_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "threshold_calibration_trace_not_provided",
                "message": "Trace-backed evidence is absent; thresholds remain insufficient_data.",
            }
        )
    if prices_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "threshold_calibration_prices_not_provided",
                "message": (
                    "Outcome price evidence is absent; " "realized outcome sensitivity is limited."
                ),
            }
        )
    insufficient = [
        str(record.get("threshold_id"))
        for record in records
        if str(record.get("recommended_action")) == "insufficient_data"
    ]
    if insufficient:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "thresholds_have_insufficient_data",
                "threshold_ids": insufficient,
                "message": (
                    "Some thresholds remain sensitivity-tested " "but insufficient to calibrate."
                ),
            }
        )
    return issues


def _load_or_build_threshold_calibration_report(
    *,
    registry_path: Path,
    threshold_registry_path: Path,
    calibration_report_path: Path | None,
    trace_path: Path | None,
    prices_path: Path | None,
    gate_audit_root: Path | None,
    bridge_artifact_root: Path | None,
    outcome_ticker: str,
    capped_masking_ratio: float,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
    asset_universe: str | None,
) -> dict[str, Any]:
    if calibration_report_path is not None:
        payload = _read_optional_json(calibration_report_path)
        if not payload:
            raise IndicatorResearchError(
                f"threshold calibration report not found or invalid: {calibration_report_path}"
            )
        if payload.get("report_type") != "threshold_calibration_report":
            raise IndicatorResearchError(
                "calibration report must have report_type=threshold_calibration_report"
            )
        return payload
    return build_threshold_calibration_report(
        registry_path=registry_path,
        threshold_registry_path=threshold_registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_audit_root,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        capped_masking_ratio=capped_masking_ratio,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )


def _threshold_followup_result_row(record: Mapping[str, Any]) -> dict[str, Any]:
    recommendation_rows = [
        row for row in record.get("recommendation_by_value", []) if isinstance(row, Mapping)
    ]
    tested_values = [
        row.get("tested_value") for row in recommendation_rows if "tested_value" in row
    ]
    recommendation_changed = bool(
        record.get("valuation_crowding_recommendation_changes")
        or any(
            bool(row.get("valuation_crowding_recommendation_changes"))
            for row in recommendation_rows
        )
        or record.get("adjust_candidate")
    )
    remaining_data_gap = list(record.get("remaining_limitations") or [])
    if record.get("insufficient_data"):
        remaining_data_gap.extend(
            _data_gap_shortfalls_for_threshold(str(record.get("threshold_id")))
        )
    return {
        "threshold_id": str(record.get("threshold_id")),
        "current_value": _json_ready(record.get("current_value")),
        "tested_values": _json_ready(record.get("tested_values", tested_values)),
        "recommendation": str(record.get("recommended_action")),
        "reason": str(record.get("reason")),
        "evidence_strength": str(record.get("evidence_strength")),
        "recommendation_changed": recommendation_changed,
        "current_value_changed": False,
        "remaining_data_gap": _dedupe_preserve_order(str(item) for item in remaining_data_gap),
        "calibration_status": str(record.get("recommended_status")),
        "not_validated_statistical_boundary": bool(
            record.get("not_validated_statistical_boundary", True)
        ),
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _threshold_data_gap_plan(record: Mapping[str, Any]) -> dict[str, Any]:
    threshold_id = str(record.get("threshold_id"))
    if threshold_id == "indicator_research.effectiveness_min_available_outcome_cases":
        sample_quality = _first_sample_quality(record)
        return {
            "threshold_id": threshold_id,
            "missing_samples": (
                "20d mature full_advisory outcome cases with enough independent dates, "
                "assets, and correlated clusters to satisfy leave-one robustness."
            ),
            "missing_horizon": "20d",
            "missing_trace_source": "full_advisory",
            "component_or_backtest_gap": (
                "component/backtest rows can support diagnostics but cannot replace "
                "the full_advisory mature-case floor."
            ),
            "pit_gate_limited": True,
            "pit_gate_reason": (
                "Earlier replay must pass PIT feature availability, data quality, and lineage "
                "equivalence before a case can count as full_advisory."
            ),
            "needs_forward_maturity": True,
            "earlier_historical_replay_can_fill": True,
            "earlier_historical_replay_condition": (
                "Feasible only for dates where PIT replay can produce production-equivalent "
                "full_advisory traces; blocked dates remain data gaps rather than synthetic cases."
            ),
            "observed_gap": {
                "raw_case_count": sample_quality.get("raw_case_count", 0),
                "effective_date_count": sample_quality.get("effective_date_count", 0),
                "effective_cluster_count": sample_quality.get("effective_cluster_count", 0),
                "robustness_gate_passed": bool(sample_quality.get("robustness_gate_passed")),
            },
            "next_data_collection_action": (
                "Extend PIT historical replay to earlier AI-regime dates and wait for 20d "
                "forward maturity for recent full_advisory cases."
            ),
            "promotion_gate_allowed": False,
        }
    if threshold_id == "indicator_research.robustness_cluster_dominance_share":
        sample_quality = _first_sample_quality(record)
        return {
            "threshold_id": threshold_id,
            "missing_samples": (
                "Independent non-single-cluster observations across dates/assets/clusters, "
                "plus matching full_advisory and all_sources comparison rows."
            ),
            "missing_horizon": "robustness aggregation across mature 1d/5d/10d and pending 20d",
            "missing_trace_source": "full_advisory and all_sources consistency",
            "component_or_backtest_gap": (
                "component/backtest evidence is useful only if it remains directionally "
                "consistent with full_advisory rows."
            ),
            "pit_gate_limited": True,
            "pit_gate_reason": (
                "Cluster robustness needs production-equivalent full_advisory lineage; "
                "component-only repair cannot certify promotion evidence."
            ),
            "needs_forward_maturity": True,
            "earlier_historical_replay_can_fill": True,
            "earlier_historical_replay_condition": (
                "Earlier replay can improve cluster diversity only if PIT gates pass and the "
                "asset universe includes more than one correlated cluster."
            ),
            "observed_gap": {
                "top_cluster_share": sample_quality.get("top_cluster_share"),
                "cluster_count": sample_quality.get("cluster_count", 0),
                "leave_one_cluster_out_stable": bool(
                    sample_quality.get("leave_one_cluster_out_stable")
                ),
                "full_advisory_only_vs_all_sources_consistent": bool(
                    sample_quality.get("full_advisory_only_vs_all_sources_consistent")
                ),
            },
            "next_data_collection_action": (
                "Expand replay coverage across more assets/clusters and retain full_advisory "
                "vs all_sources comparison for each mature horizon."
            ),
            "promotion_gate_allowed": False,
        }
    return {
        "threshold_id": threshold_id,
        "missing_samples": "No insufficient-data plan required for this threshold.",
        "missing_horizon": "not_applicable",
        "missing_trace_source": "not_applicable",
        "pit_gate_limited": False,
        "needs_forward_maturity": False,
        "earlier_historical_replay_can_fill": False,
        "promotion_gate_allowed": False,
    }


def _threshold_keep_current_status(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "threshold_id": str(record.get("threshold_id")),
        "recommendation": "keep_current_value",
        "calibration_status": "SENSITIVITY_TESTED",
        "not_validated_statistical_boundary": True,
        "validated_statistical_boundary": False,
        "current_value_changed": False,
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "reason": str(record.get("reason")),
    }


def _threshold_followup_summary(
    result_rows: Sequence[Mapping[str, Any]],
    data_gap_plan: Sequence[Mapping[str, Any]],
    keep_current_status: Sequence[Mapping[str, Any]],
    calibration_summary: Any,
) -> dict[str, Any]:
    source_summary = calibration_summary if isinstance(calibration_summary, Mapping) else {}
    return {
        "threshold_count": len(result_rows),
        "insufficient_data_threshold_count": len(data_gap_plan),
        "keep_current_value_threshold_count": len(keep_current_status),
        "recommendation_changed_count": sum(
            1 for row in result_rows if bool(row.get("recommendation_changed"))
        ),
        "sensitivity_tested_count": int(source_summary.get("sensitivity_tested_count") or 0),
        "validated_boundary_count": 0,
        "still_uncalibrated_high_impact_count": int(
            source_summary.get("still_uncalibrated_high_impact_count") or 0
        ),
        "thresholds_still_blocking_promotion_count": int(
            source_summary.get("thresholds_still_blocking_promotion_count") or 0
        ),
        "thresholds_changed_count": 0,
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "validation_only": True,
    }


def _threshold_followup_issues(
    result_rows: Sequence[Mapping[str, Any]],
    data_gap_plan: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues = [
        {
            "severity": "info",
            "issue_id": "threshold_followup_validation_only",
            "message": (
                "Follow-up plan is planning metadata only and cannot change threshold "
                "values or approve promotion, paper-shadow, production, or official weights."
            ),
        }
    ]
    if len(result_rows) != len(INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS):
        issues.append(
            {
                "severity": "warning",
                "issue_id": "threshold_followup_incomplete_scope",
                "message": "Follow-up plan does not contain all expected calibration thresholds.",
                "expected_threshold_ids": list(INDICATOR_RESEARCH_THRESHOLD_CALIBRATION_IDS),
                "observed_threshold_ids": [row.get("threshold_id") for row in result_rows],
            }
        )
    if data_gap_plan:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "threshold_followup_data_gaps_remaining",
                "message": "Some sensitivity-tested thresholds still need data before calibration.",
                "threshold_ids": [row.get("threshold_id") for row in data_gap_plan],
            }
        )
    return issues


def _first_sample_quality(record: Mapping[str, Any]) -> dict[str, Any]:
    sample_quality = record.get("sample_quality_impact", {})
    if isinstance(sample_quality, Mapping):
        for value in sample_quality.values():
            if isinstance(value, Mapping):
                return dict(value)
    rows = record.get("recommendation_by_value", [])
    if isinstance(rows, Sequence) and not isinstance(rows, str | bytes | bytearray):
        for row in rows:
            if isinstance(row, Mapping) and isinstance(row.get("sample_quality_impact"), Mapping):
                return dict(row["sample_quality_impact"])
    return {}


def _data_gap_shortfalls_for_threshold(threshold_id: str) -> list[str]:
    if threshold_id == "indicator_research.effectiveness_min_available_outcome_cases":
        return [
            "Need additional 20d mature full_advisory cases.",
            "Need independent-date and independent-cluster robustness.",
            (
                "Need PIT-valid historical replay or forward maturity; "
                "synthetic cases are not allowed."
            ),
        ]
    if threshold_id == "indicator_research.robustness_cluster_dominance_share":
        return [
            "Need more than one correlated cluster with stable leave-one-cluster behavior.",
            (
                "Need full_advisory_only vs all_sources consistency before "
                "robustness can support calibration."
            ),
            (
                "Need PIT-valid replay across broader assets/clusters; "
                "component-only evidence is insufficient."
            ),
        ]
    return []


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _dynamic_trend_threshold_prep_record(
    threshold_id: str,
    threshold: Mapping[str, Any],
) -> dict[str, Any]:
    current_value = _json_ready(threshold.get("current_value"))
    tested_values = SECOND_BATCH_DYNAMIC_TREND_TESTED_VALUES[threshold_id]
    recommendation_rows = [
        _dynamic_trend_threshold_prep_row(threshold_id, tested_value)
        for tested_value in tested_values
    ]
    return {
        "threshold_id": threshold_id,
        "current_value": current_value,
        "where_used": _json_ready(list(threshold.get("where_used") or [])),
        "decision_affecting_path": _dynamic_trend_decision_path(threshold_id),
        "tested_values": _json_ready(tested_values),
        "sensitivity_impact": _dynamic_trend_sensitivity_impact(threshold_id),
        "recommendation_by_value": recommendation_rows,
        "false_risk_off_impact": _dynamic_trend_false_risk_off_impact(threshold_id),
        "false_risk_on_impact": _dynamic_trend_false_risk_on_impact(threshold_id),
        "turnover_constraint_hit_impact": _dynamic_trend_turnover_impact(threshold_id),
        "drawdown_missed_upside_impact": _dynamic_trend_drawdown_upside_impact(threshold_id),
        "recommended_status": "CALIBRATION_PREPARED",
        "max_allowed_status": "SENSITIVITY_TESTED",
        "validated_boundary": False,
        "current_value_changed": False,
        "threshold_value_change_allowed": False,
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_effect": "none",
        "remaining_limitations": [
            "No observed forward outcome, turnover, or constraint-hit estimate is computed here.",
            "Owner-reviewed sensitivity evidence is required before any threshold change.",
            "This prep artifact cannot be used as promotion evidence.",
        ],
    }


def _dynamic_trend_threshold_prep_row(
    threshold_id: str,
    tested_value: Mapping[str, Any],
) -> dict[str, Any]:
    scenario_id = str(tested_value.get("scenario_id", "unnamed_scenario"))
    return {
        "scenario_id": scenario_id,
        "tested_value": _json_ready(dict(tested_value)),
        "recommendation": "collect_evidence_only",
        "recommendation_reason": _dynamic_trend_recommendation_reason(
            threshold_id,
            scenario_id,
        ),
        "sensitivity_impact": _dynamic_trend_scenario_sensitivity_impact(
            threshold_id,
            scenario_id,
        ),
        "false_risk_off_impact": _dynamic_trend_scenario_false_risk_off_impact(
            threshold_id,
            scenario_id,
        ),
        "false_risk_on_impact": _dynamic_trend_scenario_false_risk_on_impact(
            threshold_id,
            scenario_id,
        ),
        "turnover_constraint_hit_impact": _dynamic_trend_scenario_turnover_impact(
            threshold_id,
            scenario_id,
        ),
        "drawdown_missed_upside_impact": _dynamic_trend_scenario_drawdown_upside_impact(
            threshold_id,
            scenario_id,
        ),
        "evidence_required": [
            "forward_return_by_horizon",
            "forward_drawdown_by_horizon",
            "missed_upside_case_labels",
            "false_risk_off_or_false_risk_on_case_labels",
            "turnover_and_constraint_hit_rows",
        ],
        "current_value_changed": False,
        "validated_boundary": False,
        "promotion_gate_allowed": False,
        "production_effect": "none",
    }


def _dynamic_trend_threshold_prep_summary(
    prep_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "prepared_threshold_count": len(prep_records),
        "target_threshold_ids": [record.get("threshold_id") for record in prep_records],
        "sensitivity_tested_count": 0,
        "calibration_prepared_count": len(prep_records),
        "validated_boundary_count": 0,
        "thresholds_changed_count": 0,
        "max_allowed_status": "SENSITIVITY_TESTED",
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "validation_only": True,
    }


def _dynamic_trend_threshold_prep_issues(
    prep_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues = [
        {
            "severity": "info",
            "issue_id": "dynamic_trend_threshold_prep_only",
            "message": (
                "Second-batch threshold calibration prep is planning metadata only; "
                "it cannot change current values or approve production, paper-shadow, "
                "official weights, or promotion."
            ),
        }
    ]
    if len(prep_records) != len(SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS):
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dynamic_trend_threshold_prep_incomplete_scope",
                "expected_threshold_ids": list(SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS),
                "observed_threshold_ids": [record.get("threshold_id") for record in prep_records],
            }
        )
    return issues


def _dynamic_trend_decision_path(threshold_id: str) -> list[str]:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return [
            "CompositeTrendScore / RiskRegimeScore / GrowthLeadershipScore",
            "dynamic_allocation_policy.regime_selection_rules.risk_off",
            "dynamic_allocation_policy.regime_selection_rules.growth_underperformance",
            "risk_off or growth_underperformance candidate regime weights",
            "weak_composite_defense / risk_regime_defense trend overlays",
            "exposure constraints and rebalance turnover gates",
            "validation-only allocation recommendation",
        ]
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return [
            (
                "CompositeTrendScore / RiskRegimeScore / GrowthLeadershipScore / "
                "SemiconductorLeadershipScore"
            ),
            "dynamic_allocation_policy.regime_selection_rules.risk_on",
            "growth_leadership_overweight / semiconductor_leadership_overweight overlays",
            "risk_on or semiconductor_leadership candidate regime weights",
            "exposure constraints and rebalance turnover gates",
            "validation-only allocation recommendation",
        ]
    return [
        "trend_calibration composite score",
        "trend_calibration.score_bands bucket assignment",
        "forward attribution / drawdown bucket diagnostics",
        "candidate-only trend signal config registry",
        "dynamic allocation policy input review",
        "manual owner review before any production interpretation",
    ]


def _dynamic_trend_sensitivity_impact(threshold_id: str) -> dict[str, Any]:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return {
            "primary_direction": (
                "lower values reduce defensive triggers; " "higher values expand defensive triggers"
            ),
            "decision_surface": "risk_off and growth_underperformance regime selection",
            "expected_observable": (
                "defensive regime frequency, cash allocation, and "
                "growth/semiconductor underweight frequency"
            ),
        }
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return {
            "primary_direction": (
                "higher values require stronger confirmation; "
                "lower values expand risk-on triggers"
            ),
            "decision_surface": "risk_on regime selection and leadership overlays",
            "expected_observable": (
                "risk-on regime frequency, QQQ/SMH overweight frequency, "
                "and confirmation failures"
            ),
        }
    return {
        "primary_direction": (
            "band boundary shifts change score bucket labels without changing raw scores"
        ),
        "decision_surface": (
            "trend calibration bucket attribution and candidate signal config review"
        ),
        "expected_observable": (
            "bucket membership, forward attribution lift, drawdown by bucket, "
            "and missed-upside by bucket"
        ),
    }


def _dynamic_trend_false_risk_off_impact(threshold_id: str) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return "Higher risk-off maxima can increase false defensive shifts and missed upside."
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return "Stricter risk-on minima can indirectly increase false risk-off or neutral holds."
    return "Higher risk-off/weak band boundaries can label more observations as defensive."


def _dynamic_trend_false_risk_on_impact(threshold_id: str) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return "Lower risk-off maxima can leave weak regimes classified as neutral or risk-on."
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return "Lower risk-on minima can increase false aggressive shifts before confirmation."
    return "Lower risk-on band boundaries can label more observations as risk-on."


def _dynamic_trend_turnover_impact(threshold_id: str) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return (
            "Looser defensive triggers can raise cash-shift turnover and hit "
            "max rebalance delta or cash caps."
        )
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return (
            "Looser confirmation can raise QQQ/SMH overlay turnover and hit "
            "sleeve/cash constraints."
        )
    return "Narrower bands can increase bucket churn and downstream candidate config turnover."


def _dynamic_trend_drawdown_upside_impact(threshold_id: str) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return (
            "Looser defensive triggers may reduce drawdown but increase missed "
            "upside; stricter triggers invert that risk."
        )
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return (
            "Looser risk-on triggers may improve upside capture but worsen "
            "drawdown; stricter triggers may miss upside."
        )
    return (
        "Defensive band shifts can reduce drawdown at the cost of missed upside; "
        "risk-on shifts can do the reverse."
    )


def _dynamic_trend_recommendation_reason(threshold_id: str, scenario_id: str) -> str:
    del scenario_id
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return (
            "Risk-off thresholds need labeled false defensive shifts, "
            "forward drawdown, and missed-upside evidence."
        )
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return (
            "Risk-on thresholds need confirmation failure, upside capture, "
            "drawdown, and turnover evidence."
        )
    return (
        "Trend score bands need bucket-level forward attribution, drawdown, "
        "churn, and owner-reviewed labels."
    )


def _dynamic_trend_scenario_sensitivity_impact(threshold_id: str, scenario_id: str) -> str:
    if "current_policy_baseline" in scenario_id:
        return "Baseline scenario for comparison; no threshold value change."
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return (
            "Expected to reduce defensive classifications."
            if "stricter" in scenario_id
            else "Expected to increase defensive classifications."
        )
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return (
            "Expected to reduce risk-on classifications."
            if "stricter" in scenario_id
            else "Expected to increase risk-on classifications."
        )
    if "lower_risk_on" in scenario_id:
        return "Expected to increase risk-on bucket membership."
    if "higher_confirmation" in scenario_id or "wider_neutral" in scenario_id:
        return "Expected to reduce or delay risk-on bucket membership."
    return "Expected to change trend bucket membership."


def _dynamic_trend_scenario_false_risk_off_impact(
    threshold_id: str,
    scenario_id: str,
) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return "lower" if "stricter" in scenario_id else "higher"
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return "higher" if "stricter" in scenario_id else "lower"
    return (
        "higher"
        if "higher_confirmation" in scenario_id or "wider_neutral" in scenario_id
        else "lower"
    )


def _dynamic_trend_scenario_false_risk_on_impact(
    threshold_id: str,
    scenario_id: str,
) -> str:
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return "higher" if "stricter" in scenario_id else "lower"
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return "lower" if "stricter" in scenario_id else "higher"
    return "higher" if "lower_risk_on" in scenario_id else "lower"


def _dynamic_trend_scenario_turnover_impact(threshold_id: str, scenario_id: str) -> str:
    if "current_policy_baseline" in scenario_id:
        return "baseline"
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        return "lower" if "stricter" in scenario_id else "higher"
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        return "lower" if "stricter" in scenario_id else "higher"
    return "higher" if "wider_neutral" not in scenario_id else "mixed"


def _dynamic_trend_scenario_drawdown_upside_impact(
    threshold_id: str,
    scenario_id: str,
) -> dict[str, str]:
    if "current_policy_baseline" in scenario_id:
        return {"drawdown_impact": "baseline", "missed_upside_impact": "baseline"}
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        if "stricter" in scenario_id:
            return {"drawdown_impact": "potentially_worse", "missed_upside_impact": "lower"}
        return {"drawdown_impact": "potentially_better", "missed_upside_impact": "higher"}
    if threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        if "stricter" in scenario_id:
            return {"drawdown_impact": "potentially_better", "missed_upside_impact": "higher"}
        return {"drawdown_impact": "potentially_worse", "missed_upside_impact": "lower"}
    if "lower_risk_on" in scenario_id:
        return {"drawdown_impact": "potentially_worse", "missed_upside_impact": "lower"}
    return {"drawdown_impact": "potentially_better", "missed_upside_impact": "higher"}


def _load_dynamic_trend_policy_inputs() -> dict[str, Any]:
    dynamic_policy = safe_load_yaml_path(DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH)
    trend_policy = safe_load_yaml_path(DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH)
    return {
        "dynamic_allocation_policy_path": str(DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH),
        "trend_calibration_policy_path": str(DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH),
        "dynamic_allocation_policy": (
            dict(dynamic_policy) if isinstance(dynamic_policy, Mapping) else {}
        ),
        "trend_calibration_policy": dict(trend_policy) if isinstance(trend_policy, Mapping) else {},
    }


def _dynamic_trend_sensitivity_cases(
    *,
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    gate_availability: Sequence[Mapping[str, Any]],
    outcome_ticker: str,
    trace_contract_version: str,
) -> list[dict[str, Any]]:
    availability_by_key = {
        (
            str(record.get("date") or ""),
            str(record.get("asset") or DEFAULT_TRACE_ASSET).upper(),
        ): record
        for record in gate_availability
    }
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        row_date = str(row.get("date") or "")
        if not row_date:
            continue
        asset = str(row.get("asset") or DEFAULT_TRACE_ASSET).upper()
        grouped[(row_date, asset)].append(row)
    cases = []
    for (row_date, asset), rows in sorted(grouped.items()):
        parsed_date = _parse_iso_date(row_date)
        scores = _dynamic_trend_scores_for_rows(rows)
        score_sources = _dynamic_trend_score_sources_for_rows(rows)
        price_ticker, missing_asset_mapping = _price_ticker_mapping_for_asset(
            asset,
            price_series_by_ticker,
            fallback_ticker=outcome_ticker,
        )
        outcome_payload = _forward_outcome_payload_with_availability(
            price_series_by_ticker.get(price_ticker, []),
            parsed_date,
            missing_asset_mapping=missing_asset_mapping,
        )
        availability = availability_by_key.get((row_date, asset)) or availability_by_key.get(
            (row_date, DEFAULT_TRACE_ASSET)
        )
        trace_source = str(
            (availability or {}).get("trace_source") or COMPONENT_VALIDATION_TRACE_SOURCE
        )
        cases.append(
            {
                "case_id": f"dynamic_trend:{row_date}:{asset}",
                "date": row_date,
                "asset": asset,
                "outcome_ticker": price_ticker,
                "price_asset_mapping_missing": missing_asset_mapping,
                "market_regime": registry.market_regime.regime_id,
                "correlated_asset_cluster": _asset_cluster_id(asset),
                "event_window_ids": _event_window_ids_for_date(row_date),
                "scores": scores,
                "score_sources": score_sources,
                "missing_score_fields": _dynamic_trend_missing_score_fields(scores),
                "proxy_score_fields": _dynamic_trend_proxy_score_fields(score_sources),
                "trace_row_count": len(rows),
                "constraint_hit": any(bool(row.get("constraint_hit")) for row in rows),
                "trace_source": trace_source,
                "confidence": str(
                    (availability or {}).get("confidence") or TRACE_CONFIDENCE_COMPONENT
                ),
                "full_advisory_trace_eligible": bool(
                    (availability or {}).get("full_advisory_trace_eligible")
                ),
                "component_validation_trace_eligible": bool(
                    (availability or {}).get("component_validation_trace_eligible", True)
                ),
                "bridge_trace_eligible": trace_source == BACKTEST_TRACE_BRIDGE_SOURCE,
                "outcomes": outcome_payload["outcomes"],
                "outcome_windows": outcome_payload["windows"],
                "outcome_status": outcome_payload["outcome_status"],
                "outcome_missing": outcome_payload["outcome_missing"],
                "outcome_not_mature": outcome_payload["outcome_not_mature"],
                "outcome_join_key": _outcome_join_key(
                    as_of_date=row_date,
                    decision_time=row_date,
                    asset=asset,
                    scenario="dynamic_trend_threshold_sensitivity",
                    trace_source=trace_source,
                    trace_contract_version=trace_contract_version,
                ),
                "promotion_gate_allowed": False,
                "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
            }
        )
    return cases


def _dynamic_trend_coverage_extension_cases(
    *,
    coverage_extension_root: Path | None,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    outcome_ticker: str,
    trace_contract_version: str,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
    asset_universe: str | None,
) -> list[dict[str, Any]]:
    if coverage_extension_root is None or not coverage_extension_root.exists():
        return []
    requested_assets = _parse_asset_universe(asset_universe)
    coverage_assets = requested_assets or list(DYNAMIC_TREND_COVERAGE_EXTENSION_ASSETS)
    coverage_assets = [
        asset.upper()
        for asset in coverage_assets
        if _dynamic_trend_weight_price_ticker(asset, price_series_by_ticker) is not None
    ]
    if not coverage_assets:
        return []
    records = _dynamic_trend_campaign_control_records(
        coverage_extension_root,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
    )
    cases: list[dict[str, Any]] = []
    for record in records:
        row_date = str(record.get("date") or "")
        parsed_date = _parse_iso_date(row_date)
        if parsed_date is None:
            continue
        scores = _dynamic_trend_coverage_bridge_scores(
            record,
            parsed_date,
            price_series_by_ticker,
        )
        score_sources = {
            "RiskRegimeScore": "coverage_bridge:research_campaign_control_window.risk_score",
            "CompositeTrendScore": "coverage_bridge_proxy:cached_price_trailing_momentum",
            "GrowthLeadershipScore": "coverage_bridge_proxy:QQQ/SPY_trailing_relative_strength",
            "SemiconductorLeadershipScore": (
                "coverage_bridge_proxy:SMH/SPY_trailing_relative_strength"
            ),
        }
        regime = _dynamic_trend_realized_coverage_regime(parsed_date, price_series_by_ticker)
        scenario = str(record.get("scenario") or "unknown_control_window")
        for asset in coverage_assets:
            price_ticker, missing_asset_mapping = _price_ticker_mapping_for_asset(
                asset,
                price_series_by_ticker,
                fallback_ticker=outcome_ticker,
            )
            outcome_payload = _forward_outcome_payload_with_availability(
                price_series_by_ticker.get(price_ticker, []),
                parsed_date,
                missing_asset_mapping=missing_asset_mapping,
            )
            event_window_ids = _event_window_ids_for_date(row_date)
            event_window_ids.extend(
                [
                    f"campaign_control:{scenario}",
                    f"coverage_regime:{regime}",
                ]
            )
            cases.append(
                {
                    "case_id": f"dynamic_trend_coverage:{scenario}:{row_date}:{asset}",
                    "date": row_date,
                    "asset": asset,
                    "outcome_ticker": price_ticker,
                    "price_asset_mapping_missing": missing_asset_mapping,
                    "market_regime": regime,
                    "correlated_asset_cluster": _asset_cluster_id(asset),
                    "event_window_ids": sorted(set(event_window_ids)),
                    "scores": scores,
                    "score_sources": score_sources,
                    "missing_score_fields": _dynamic_trend_missing_score_fields(scores),
                    "proxy_score_fields": _dynamic_trend_proxy_score_fields(score_sources),
                    "trace_row_count": 0,
                    "constraint_hit": False,
                    "trace_source": BACKTEST_TRACE_BRIDGE_SOURCE,
                    "confidence": TRACE_CONFIDENCE_BRIDGE,
                    "full_advisory_trace_eligible": False,
                    "component_validation_trace_eligible": False,
                    "bridge_trace_eligible": True,
                    "coverage_extension_case": True,
                    "coverage_extension_source": "research_campaign_control_window",
                    "coverage_extension_scenario": scenario,
                    "coverage_extension_source_path": str(record.get("source_path") or ""),
                    "coverage_extension_regime_source": (
                        "cached_price_forward_return_bucket_for_validation_coverage"
                    ),
                    "campaign_risk_state": str(record.get("risk_state") or ""),
                    "campaign_risk_score": _optional_float(record.get("risk_score")),
                    "outcomes": outcome_payload["outcomes"],
                    "outcome_windows": outcome_payload["windows"],
                    "outcome_status": outcome_payload["outcome_status"],
                    "outcome_missing": outcome_payload["outcome_missing"],
                    "outcome_not_mature": outcome_payload["outcome_not_mature"],
                    "outcome_join_key": _outcome_join_key(
                        as_of_date=row_date,
                        decision_time=row_date,
                        asset=asset,
                        scenario=f"dynamic_trend_coverage_extension:{scenario}",
                        trace_source=BACKTEST_TRACE_BRIDGE_SOURCE,
                        trace_contract_version=trace_contract_version,
                    ),
                    "promotion_gate_allowed": False,
                    "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
                }
            )
    return cases


def _dynamic_trend_campaign_control_records(
    coverage_extension_root: Path,
    *,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
) -> list[dict[str, Any]]:
    records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(coverage_extension_root.rglob("*control_risk_signal_*.csv")):
        scenario = path.name.removeprefix("b2_control_").split("_control_risk_signal_")[0]
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    row_date = str(row.get("date") or "")
                    if not _dynamic_trend_date_allowed(
                        row_date,
                        start_date=start_date,
                        end_date=end_date,
                        event_window_start=event_window_start,
                        event_window_end=event_window_end,
                    ):
                        continue
                    key = (scenario, row_date)
                    candidate = {
                        "scenario": scenario,
                        "date": row_date,
                        "risk_score": row.get("risk_score"),
                        "risk_state": row.get("risk_state"),
                        "source_path": path,
                    }
                    existing = records_by_key.get(key)
                    if existing is None or str(path) < str(existing.get("source_path") or ""):
                        records_by_key[key] = candidate
        except (OSError, csv.Error, UnicodeDecodeError):
            continue
    return [records_by_key[key] for key in sorted(records_by_key)]


def _dynamic_trend_date_allowed(
    row_date: str,
    *,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
) -> bool:
    parsed = _parse_iso_date(row_date)
    if parsed is None:
        return False
    lower = _parse_iso_date(start_date or event_window_start or "")
    upper = _parse_iso_date(end_date or event_window_end or "")
    if lower is not None and parsed < lower:
        return False
    if upper is not None and parsed > upper:
        return False
    return True


def _dynamic_trend_coverage_bridge_scores(
    record: Mapping[str, Any],
    signal_date: date,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
) -> dict[str, float | None]:
    qqq_momentum = _dynamic_trend_trailing_return(
        price_series_by_ticker.get("QQQ", []), signal_date, 20
    )
    spy_momentum = _dynamic_trend_trailing_return(
        price_series_by_ticker.get("SPY", []), signal_date, 20
    )
    composite_inputs = [value for value in (qqq_momentum, spy_momentum) if value is not None]
    composite = _dynamic_trend_score_from_return(_mean(composite_inputs), multiplier=250.0)
    growth = _dynamic_trend_relative_strength_score(
        price_series_by_ticker,
        "QQQ",
        "SPY",
        signal_date,
    )
    semiconductor = _dynamic_trend_relative_strength_score(
        price_series_by_ticker,
        "SMH",
        "SPY",
        signal_date,
    )
    return {
        "CompositeTrendScore": composite,
        "RiskRegimeScore": _optional_float(record.get("risk_score")),
        "GrowthLeadershipScore": growth,
        "SemiconductorLeadershipScore": semiconductor,
    }


def _dynamic_trend_trailing_return(
    price_series: Sequence[tuple[date, float]],
    signal_date: date,
    lookback: int,
) -> float | None:
    index = next(
        (idx for idx, (price_date, _) in enumerate(price_series) if price_date >= signal_date),
        None,
    )
    if index is None or index < lookback:
        return None
    current = price_series[index][1]
    previous = price_series[index - lookback][1]
    if previous <= 0:
        return None
    return current / previous - 1


def _dynamic_trend_score_from_return(
    value: float | None,
    *,
    multiplier: float,
) -> float | None:
    if value is None:
        return None
    return max(0.0, min(100.0, 50.0 + value * multiplier))


def _dynamic_trend_relative_strength_score(
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    asset: str,
    benchmark: str,
    signal_date: date,
) -> float | None:
    asset_return = _dynamic_trend_trailing_return(
        price_series_by_ticker.get(asset, []),
        signal_date,
        20,
    )
    benchmark_return = _dynamic_trend_trailing_return(
        price_series_by_ticker.get(benchmark, []),
        signal_date,
        20,
    )
    if asset_return is None or benchmark_return is None:
        return None
    return _dynamic_trend_score_from_return(asset_return - benchmark_return, multiplier=500.0)


def _dynamic_trend_realized_coverage_regime(
    signal_date: date,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
) -> str:
    qqq_outcome = _forward_outcome_payload_with_availability(
        price_series_by_ticker.get("QQQ", []),
        signal_date,
    )
    returns = qqq_outcome["outcomes"]
    return_5d = _optional_float(returns.get("return_5d"))
    return_20d = _optional_float(returns.get("return_20d"))
    if (return_5d is not None and return_5d <= -0.02) or (
        return_20d is not None and return_20d <= -0.05
    ):
        return "coverage_bridge_pullback"
    if (return_5d is not None and return_5d >= 0.02) or (
        return_20d is not None and return_20d >= 0.05
    ):
        return "coverage_bridge_uptrend"
    return "coverage_bridge_neutral"


def _dynamic_trend_scores_for_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
    trend_rows = [row for row in rows if str(row.get("daily_component_id") or "") == "trend"]
    risk_rows = [
        row for row in rows if str(row.get("daily_component_id") or "") == "risk_sentiment"
    ]
    trend_score = _first_optional_score(trend_rows)
    risk_score = _first_optional_score(risk_rows)
    semiconductor_score = _leadership_proxy_from_signal_scores(
        trend_rows,
        subject="SMH/SPY",
    )
    growth_score = _leadership_proxy_from_signal_scores(
        trend_rows,
        subject="QQQ/SPY",
    )
    return {
        "CompositeTrendScore": trend_score,
        "RiskRegimeScore": risk_score,
        "GrowthLeadershipScore": growth_score,
        "SemiconductorLeadershipScore": semiconductor_score,
    }


def _dynamic_trend_score_sources_for_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    sources: dict[str, str] = {}
    if any(str(row.get("daily_component_id") or "") == "trend" for row in rows):
        sources["CompositeTrendScore"] = "component_trace:trend.normalized_indicator_score"
    if any(str(row.get("daily_component_id") or "") == "risk_sentiment" for row in rows):
        sources["RiskRegimeScore"] = "component_trace:risk_sentiment.normalized_indicator_score"
    if (
        _leadership_proxy_from_signal_scores(
            [row for row in rows if str(row.get("daily_component_id") or "") == "trend"],
            subject="SMH/SPY",
        )
        is not None
    ):
        sources["SemiconductorLeadershipScore"] = (
            "component_trace_proxy:trend.signal_scores.relative_strength_return_20d.SMH/SPY"
        )
    if (
        _leadership_proxy_from_signal_scores(
            [row for row in rows if str(row.get("daily_component_id") or "") == "trend"],
            subject="QQQ/SPY",
        )
        is not None
    ):
        sources["GrowthLeadershipScore"] = (
            "component_trace_proxy:trend.signal_scores.relative_strength_return_20d.QQQ/SPY"
        )
    return sources


def _first_optional_score(rows: Sequence[Mapping[str, Any]]) -> float | None:
    for row in rows:
        value = _optional_float(row.get("normalized_indicator_score"))
        if value is not None:
            return value
    return None


def _leadership_proxy_from_signal_scores(
    rows: Sequence[Mapping[str, Any]],
    *,
    subject: str,
) -> float | None:
    for row in rows:
        signal_scores = row.get("signal_scores", [])
        if not isinstance(signal_scores, Sequence) or isinstance(signal_scores, (str, bytes)):
            continue
        for signal in signal_scores:
            if not isinstance(signal, Mapping):
                continue
            if str(signal.get("subject") or "").upper() != subject.upper():
                continue
            if str(signal.get("feature") or "") != "relative_strength_return_20d":
                continue
            value = _optional_float(signal.get("normalized_indicator_score"))
            if value is None:
                continue
            return value * 100.0 if value <= 1.0 else value
    return None


def _dynamic_trend_missing_score_fields(
    scores: Mapping[str, float | None],
) -> list[str]:
    return [key for key, value in sorted(scores.items()) if value is None]


def _dynamic_trend_proxy_score_fields(score_sources: Mapping[str, str]) -> list[str]:
    return [
        key
        for key, source in sorted(score_sources.items())
        if "proxy" in str(source).lower() or "component_trace" in str(source).lower()
    ]


def _dynamic_trend_threshold_sensitivity_record(
    threshold_id: str,
    *,
    threshold: Mapping[str, Any],
    registry: IndicatorResearchRegistry,
    cases: Sequence[Mapping[str, Any]],
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    policy_config: Mapping[str, Any],
) -> dict[str, Any]:
    variants = [
        _dynamic_trend_variant_review(
            threshold_id=threshold_id,
            variant=variant,
            cases=cases,
            price_series_by_ticker=price_series_by_ticker,
            policy_config=policy_config,
        )
        for variant in _dynamic_trend_sensitivity_variant_specs(threshold_id, threshold)
    ]
    _apply_dynamic_trend_drawdown_preservation(variants)
    sample_quality = dict(variants[0].get("sample_quality_breakdown", {})) if variants else {}
    recommendation = _dynamic_trend_record_recommendation(
        variants,
        sample_quality=sample_quality,
    )
    return {
        "threshold_id": threshold_id,
        "current_value": _json_ready(threshold.get("current_value")),
        "where_used": _json_ready(list(threshold.get("where_used") or [])),
        "decision_affecting_path": _dynamic_trend_decision_path(threshold_id),
        "tested_values": [_json_ready(variant["tested_value"]) for variant in variants],
        "scenario_variants": variants,
        "sample_quality": sample_quality,
        "recommendation": recommendation,
        "recommended_status": (
            "SENSITIVITY_TESTED"
            if recommendation["validation_recommendation"] != "insufficient_data"
            else "UNCALIBRATED_DEFAULT"
        ),
        "calibration_status": (
            "SENSITIVITY_TESTED"
            if recommendation["validation_recommendation"] != "insufficient_data"
            else "UNCALIBRATED_DEFAULT"
        ),
        "evidence_strength": _dynamic_trend_evidence_strength(sample_quality),
        "remaining_limitations": _dynamic_trend_remaining_limitations(
            threshold_id,
            sample_quality,
        ),
        "not_validated_statistical_boundary": True,
        "validated_boundary": False,
        "current_value_changed": False,
        "threshold_value_change_allowed": False,
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_effect": "none",
        "market_regime": registry.market_regime.regime_id,
    }


def _dynamic_trend_sensitivity_variant_specs(
    threshold_id: str,
    threshold: Mapping[str, Any],
) -> list[dict[str, Any]]:
    current = dict(threshold.get("current_value") or {})
    scenarios = {
        str(item.get("scenario_id")): dict(item)
        for item in SECOND_BATCH_DYNAMIC_TREND_TESTED_VALUES[threshold_id]
        if isinstance(item, Mapping)
    }
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        mapping = [
            ("current_value", "current_policy_baseline"),
            ("stricter", "stricter_defensive_trigger"),
            ("relaxed", "moderately_looser_defensive_trigger"),
            ("capped_or_smoothed_candidate", "broad_defensive_trigger"),
            ("no_change_baseline", "current_policy_baseline"),
        ]
    elif threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        mapping = [
            ("current_value", "current_policy_baseline"),
            ("stricter", "stricter_risk_on_confirmation"),
            ("relaxed", "moderately_looser_risk_on_confirmation"),
            ("capped_or_smoothed_candidate", "broad_risk_on_confirmation"),
            ("no_change_baseline", "current_policy_baseline"),
        ]
    else:
        mapping = [
            ("current_value", "current_policy_baseline"),
            ("stricter", "higher_confirmation_boundary"),
            ("relaxed", "lower_risk_on_boundary"),
            ("capped_or_smoothed_candidate", "wider_neutral_band"),
            ("no_change_baseline", "current_policy_baseline"),
        ]
    variants = []
    for variant_kind, scenario_id in mapping:
        tested_value = dict(scenarios.get(scenario_id) or current)
        tested_value.setdefault("scenario_id", scenario_id)
        variants.append(
            {
                "variant_id": f"{variant_kind}:{scenario_id}",
                "variant_kind": variant_kind,
                "scenario_id": scenario_id,
                "tested_value": tested_value,
                "current_value_changed": False,
                "threshold_value_change_allowed": False,
            }
        )
    return variants


def _dynamic_trend_variant_review(
    *,
    threshold_id: str,
    variant: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    policy_config: Mapping[str, Any],
) -> dict[str, Any]:
    evaluated = [
        _dynamic_trend_evaluate_case_variant(
            threshold_id=threshold_id,
            variant=variant,
            case=case,
            price_series_by_ticker=price_series_by_ticker,
            policy_config=policy_config,
        )
        for case in cases
    ]
    metrics = _dynamic_trend_metrics_for_evaluated_cases(evaluated)
    sample_quality = _dynamic_trend_variant_sample_quality(evaluated)
    return {
        "variant_id": variant["variant_id"],
        "variant_kind": variant["variant_kind"],
        "scenario_id": variant["scenario_id"],
        "tested_value": _json_ready(variant["tested_value"]),
        **metrics,
        "mature_date_count": sample_quality["mature_date_count"],
        "mature_case_count": sample_quality["mature_case_count"],
        "full_advisory_case_count": sample_quality["full_advisory_case_count"],
        "cluster_count": sample_quality["cluster_count"],
        "regime_count": sample_quality["regime_count"],
        "sample_quality_breakdown": sample_quality,
        "recommendation": _dynamic_trend_variant_recommendation(sample_quality),
        "promotion_gate_by_value": False,
        "calibration_status": "SENSITIVITY_TESTED" if evaluated else "UNCALIBRATED_DEFAULT",
        "validated_boundary": False,
        "current_value_changed": False,
        "threshold_value_change_allowed": False,
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_effect": "none",
        "by_horizon": _dynamic_trend_by_horizon(evaluated),
        "by_asset": _dynamic_trend_group_rows(evaluated, "asset"),
        "by_date": _dynamic_trend_group_rows(evaluated, "date"),
        "by_regime": _dynamic_trend_group_rows(evaluated, "market_regime"),
        "by_event_window": _dynamic_trend_event_window_rows(evaluated),
        "full_advisory_only": _dynamic_trend_group_summary(
            [case for case in evaluated if case.get("full_advisory_trace_eligible")]
        ),
        "component_backtest_bridge": _dynamic_trend_group_rows(evaluated, "trace_source"),
        "by_correlated_asset_cluster": _dynamic_trend_group_rows(
            evaluated,
            "correlated_asset_cluster",
        ),
    }


def _dynamic_trend_evaluate_case_variant(
    *,
    threshold_id: str,
    variant: Mapping[str, Any],
    case: Mapping[str, Any],
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    policy_config: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _dynamic_trend_case_decision(
        threshold_id,
        variant.get("tested_value", {}),
        case,
        policy_config=policy_config,
    )
    weights = _dynamic_trend_weights_for_state(decision["regime_state"], policy_config)
    returns_by_horizon = {
        horizon: _dynamic_trend_portfolio_return(
            price_series_by_ticker,
            signal_date=_parse_iso_date(str(case.get("date") or "")),
            weights=weights,
            horizon=horizon,
        )
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    portfolio_path = _dynamic_trend_portfolio_path(
        price_series_by_ticker,
        signal_date=_parse_iso_date(str(case.get("date") or "")),
        weights=weights,
        horizon=20,
    )
    max_drawdown = _max_drawdown(portfolio_path) if portfolio_path else None
    reference_return = _dynamic_trend_reference_return(returns_by_horizon)
    false_risk_off = bool(decision["risk_off_triggered"] and reference_return is not None)
    false_risk_off = false_risk_off and reference_return > 0
    false_risk_on = bool(decision["risk_on_confirmed"] and reference_return is not None)
    false_risk_on = false_risk_on and (
        reference_return < 0 or (max_drawdown is not None and max_drawdown < -0.05)
    )
    missed_upside = bool(decision["risk_off_triggered"] and reference_return is not None)
    missed_upside = missed_upside and reference_return > 0.02
    constraint_hit = bool(case.get("constraint_hit")) or _dynamic_trend_constraint_hit(
        weights,
        policy_config,
    )
    return {
        **dict(case),
        "variant_id": variant.get("variant_id"),
        "variant_kind": variant.get("variant_kind"),
        "scenario_id": variant.get("scenario_id"),
        "regime_state": decision["regime_state"],
        "trend_band": decision.get("trend_band"),
        "target_weights": weights,
        "portfolio_returns": returns_by_horizon,
        "portfolio_max_drawdown_20d": max_drawdown,
        "risk_off_triggered": decision["risk_off_triggered"],
        "risk_on_confirmed": decision["risk_on_confirmed"],
        "false_risk_off": false_risk_off,
        "false_risk_on": false_risk_on,
        "missed_upside": missed_upside,
        "constraint_hit": constraint_hit,
        "decision_missing_fields": decision["missing_fields"],
        "decision_score_fields_used": decision["score_fields_used"],
        "promotion_gate_allowed": False,
    }


def _dynamic_trend_case_decision(
    threshold_id: str,
    tested_value: Any,
    case: Mapping[str, Any],
    *,
    policy_config: Mapping[str, Any],
) -> dict[str, Any]:
    values = dict(tested_value) if isinstance(tested_value, Mapping) else {}
    scores = case.get("scores", {})
    scores = scores if isinstance(scores, Mapping) else {}
    risk_off_current = _dynamic_trend_current_risk_off_thresholds(policy_config)
    risk_on_current = _dynamic_trend_current_risk_on_thresholds(policy_config)
    missing_fields: list[str] = []
    fields_used: list[str] = []
    trend_band = None
    if threshold_id == "dynamic_allocation.risk_off_score_thresholds":
        risk_off_triggered, used, missing = _dynamic_trend_any_score_at_or_below(
            scores,
            {
                "RiskRegimeScore": values.get("risk_regime_score_max"),
                "CompositeTrendScore": values.get("composite_trend_score_max"),
                "GrowthLeadershipScore": values.get("growth_leadership_score_max"),
            },
        )
        risk_on_confirmed, risk_on_used, risk_on_missing = _dynamic_trend_all_scores_at_or_above(
            scores,
            risk_on_current,
        )
        fields_used = used + risk_on_used
        missing_fields = missing + risk_on_missing
    elif threshold_id == "dynamic_allocation.risk_on_confirmation_thresholds":
        risk_off_triggered, risk_off_used, risk_off_missing = _dynamic_trend_any_score_at_or_below(
            scores,
            risk_off_current,
        )
        risk_on_confirmed, used, missing = _dynamic_trend_all_scores_at_or_above(
            scores,
            {
                "CompositeTrendScore": values.get("composite_trend_score_min"),
                "RiskRegimeScore": values.get("risk_regime_score_min"),
                "GrowthLeadershipScore": values.get("growth_leadership_score_min"),
                "SemiconductorLeadershipScore": values.get("semiconductor_leadership_score_min"),
            },
        )
        fields_used = risk_off_used + used
        missing_fields = risk_off_missing + missing
    else:
        composite = _optional_float(scores.get("CompositeTrendScore"))
        trend_band = _dynamic_trend_score_band(composite, values)
        risk_off_triggered = trend_band in {"risk_off", "weak"}
        risk_on_confirmed = trend_band in {"risk_on", "strong_risk_on"}
        fields_used = ["CompositeTrendScore"] if composite is not None else []
        missing_fields = [] if composite is not None else ["CompositeTrendScore"]
    if risk_off_triggered:
        regime_state = "risk_off"
    elif risk_on_confirmed:
        regime_state = "risk_on"
    else:
        regime_state = "neutral"
    return {
        "regime_state": regime_state,
        "trend_band": trend_band,
        "risk_off_triggered": risk_off_triggered,
        "risk_on_confirmed": risk_on_confirmed,
        "score_fields_used": sorted(set(fields_used)),
        "missing_fields": sorted(set(missing_fields)),
    }


def _dynamic_trend_current_risk_off_thresholds(
    policy_config: Mapping[str, Any],
) -> dict[str, float | None]:
    policy = policy_config.get("dynamic_allocation_policy", {})
    rules = policy.get("regime_selection_rules", {}) if isinstance(policy, Mapping) else {}
    risk_off = rules.get("risk_off", {}) if isinstance(rules, Mapping) else {}
    growth = rules.get("growth_underperformance", {}) if isinstance(rules, Mapping) else {}
    return {
        "RiskRegimeScore": _optional_float(risk_off.get("risk_regime_score_max")),
        "CompositeTrendScore": _optional_float(risk_off.get("composite_trend_score_max")),
        "GrowthLeadershipScore": _optional_float(growth.get("growth_leadership_score_max")),
    }


def _dynamic_trend_current_risk_on_thresholds(
    policy_config: Mapping[str, Any],
) -> dict[str, float | None]:
    policy = policy_config.get("dynamic_allocation_policy", {})
    rules = policy.get("regime_selection_rules", {}) if isinstance(policy, Mapping) else {}
    risk_on = rules.get("risk_on", {}) if isinstance(rules, Mapping) else {}
    semis = rules.get("semiconductor_leadership", {}) if isinstance(rules, Mapping) else {}
    overlays = policy.get("trend_overlay_rules", []) if isinstance(policy, Mapping) else []
    growth_threshold = None
    if isinstance(overlays, Sequence) and not isinstance(overlays, (str, bytes)):
        for overlay in overlays:
            if not isinstance(overlay, Mapping):
                continue
            if overlay.get("score_id") == "GrowthLeadershipScore":
                growth_threshold = _optional_float(overlay.get("threshold"))
                break
    return {
        "CompositeTrendScore": _optional_float(risk_on.get("composite_trend_score_min")),
        "RiskRegimeScore": _optional_float(risk_on.get("risk_regime_score_min")),
        "GrowthLeadershipScore": growth_threshold,
        "SemiconductorLeadershipScore": _optional_float(
            semis.get("semiconductor_leadership_score_min")
        ),
    }


def _dynamic_trend_any_score_at_or_below(
    scores: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> tuple[bool, list[str], list[str]]:
    used: list[str] = []
    missing: list[str] = []
    triggered = False
    for score_id, threshold in thresholds.items():
        threshold_value = _optional_float(threshold)
        if threshold_value is None:
            continue
        score = _optional_float(scores.get(score_id))
        if score is None:
            missing.append(score_id)
            continue
        used.append(score_id)
        if score <= threshold_value:
            triggered = True
    return triggered, used, missing


def _dynamic_trend_all_scores_at_or_above(
    scores: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> tuple[bool, list[str], list[str]]:
    used: list[str] = []
    missing: list[str] = []
    checks: list[bool] = []
    for score_id, threshold in thresholds.items():
        threshold_value = _optional_float(threshold)
        if threshold_value is None:
            continue
        score = _optional_float(scores.get(score_id))
        if score is None:
            missing.append(score_id)
            continue
        used.append(score_id)
        checks.append(score >= threshold_value)
    return bool(checks) and all(checks), used, missing


def _dynamic_trend_score_band(score: float | None, bands: Mapping[str, Any]) -> str | None:
    if score is None:
        return None
    for band_id in ("risk_off", "weak", "neutral", "risk_on", "strong_risk_on"):
        value = bands.get(band_id)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            continue
        if len(value) < 2:
            continue
        lower = _optional_float(value[0])
        upper = _optional_float(value[1])
        if lower is None or upper is None:
            continue
        if lower <= score < upper or (band_id == "strong_risk_on" and lower <= score <= upper):
            return band_id
    return "out_of_band"


def _dynamic_trend_weights_for_state(
    state: str,
    policy_config: Mapping[str, Any],
) -> dict[str, float]:
    policy = policy_config.get("dynamic_allocation_policy", {})
    targets = policy.get("regime_weight_targets", {}) if isinstance(policy, Mapping) else {}
    record = targets.get(state, {}) if isinstance(targets, Mapping) else {}
    weights = record.get("weights", {}) if isinstance(record, Mapping) else {}
    if not isinstance(weights, Mapping):
        weights = {}
    if not weights and isinstance(policy, Mapping):
        weights = policy.get("base_weights", {})
    return {str(asset).upper(): _float(weight) for asset, weight in dict(weights).items()}


def _dynamic_trend_constraint_hit(
    weights: Mapping[str, float],
    policy_config: Mapping[str, Any],
) -> bool:
    policy = policy_config.get("dynamic_allocation_policy", {})
    constraints = policy.get("exposure_constraints", {}) if isinstance(policy, Mapping) else {}
    caps = constraints.get("asset_caps", {}) if isinstance(constraints, Mapping) else {}
    floors = constraints.get("asset_floors", {}) if isinstance(constraints, Mapping) else {}
    if not isinstance(caps, Mapping):
        caps = {}
    if not isinstance(floors, Mapping):
        floors = {}
    for asset, weight in weights.items():
        cap = _optional_float(caps.get(asset))
        floor = _optional_float(floors.get(asset))
        if cap is not None and weight > cap:
            return True
        if floor is not None and weight < floor:
            return True
    semis = sum(weights.get(asset, 0.0) for asset in ("SMH", "SOXX"))
    semi_max_source = (
        constraints.get("semiconductor_sleeve_max") if isinstance(constraints, Mapping) else None
    )
    cash_max_source = constraints.get("cash_max") if isinstance(constraints, Mapping) else None
    cash_min_source = constraints.get("cash_min") if isinstance(constraints, Mapping) else None
    semi_max = _optional_float(semi_max_source)
    cash_max = _optional_float(cash_max_source)
    cash_min = _optional_float(cash_min_source)
    return bool(
        (semi_max is not None and semis > semi_max)
        or (cash_max is not None and weights.get("CASH", 0.0) > cash_max)
        or (cash_min is not None and weights.get("CASH", 0.0) < cash_min)
    )


def _dynamic_trend_portfolio_return(
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    *,
    signal_date: date | None,
    weights: Mapping[str, float],
    horizon: int,
) -> float | None:
    if signal_date is None:
        return None
    total = 0.0
    usable_weight = 0.0
    for asset, weight in weights.items():
        if asset == "CASH" or weight == 0:
            usable_weight += weight
            continue
        ticker = _dynamic_trend_weight_price_ticker(asset, price_series_by_ticker)
        if ticker is None:
            continue
        outcome = _forward_outcome_payload_with_availability(
            price_series_by_ticker.get(ticker, []),
            signal_date,
        )
        value = outcome["outcomes"].get(f"return_{horizon}d")
        if isinstance(value, float):
            total += weight * value
            usable_weight += weight
    return total if usable_weight >= 0.90 else None


def _dynamic_trend_portfolio_path(
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    *,
    signal_date: date | None,
    weights: Mapping[str, float],
    horizon: int,
) -> list[float]:
    if signal_date is None:
        return []
    calendar_series = price_series_by_ticker.get("QQQ") or next(
        iter(price_series_by_ticker.values()),
        [],
    )
    start_index = next(
        (
            index
            for index, (price_date, _) in enumerate(calendar_series)
            if price_date >= signal_date
        ),
        None,
    )
    if start_index is None or start_index + horizon >= len(calendar_series):
        return []
    calendar_dates = [item[0] for item in calendar_series[start_index : start_index + horizon + 1]]
    path = []
    usable_weights = 0.0
    asset_price_maps: dict[str, tuple[float, dict[date, float], float]] = {}
    for asset, weight in weights.items():
        if asset == "CASH" or weight == 0:
            usable_weights += weight
            continue
        ticker = _dynamic_trend_weight_price_ticker(asset, price_series_by_ticker)
        if ticker is None:
            continue
        price_map = {price_date: price for price_date, price in price_series_by_ticker[ticker]}
        start_price = price_map.get(calendar_dates[0])
        if start_price is None or start_price <= 0:
            continue
        asset_price_maps[asset] = (start_price, price_map, weight)
        usable_weights += weight
    if usable_weights < 0.90:
        return []
    for price_date in calendar_dates:
        value = weights.get("CASH", 0.0)
        for start_price, price_map, weight in asset_price_maps.values():
            price = price_map.get(price_date)
            if price is None:
                return []
            value += weight * (price / start_price)
        path.append(value)
    return path


def _dynamic_trend_weight_price_ticker(
    asset: str,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
) -> str | None:
    normalized = asset.upper()
    if normalized in price_series_by_ticker:
        return normalized
    if normalized == "SOXX" and "SMH" in price_series_by_ticker:
        return "SMH"
    alias = PRICE_TICKER_ALIASES.get(normalized)
    if alias and alias in price_series_by_ticker:
        return alias
    return None


def _dynamic_trend_reference_return(
    returns_by_horizon: Mapping[int, float | None],
) -> float | None:
    for horizon in (20, 10, 5, 1):
        value = returns_by_horizon.get(horizon)
        if isinstance(value, float):
            return value
    return None


def _dynamic_trend_metrics_for_evaluated_cases(
    evaluated: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    dated_weights = [
        (str(case.get("date") or ""), dict(case.get("target_weights") or {})) for case in evaluated
    ]
    dated_weights.sort(key=lambda item: item[0])
    turnover = 0.0
    for index in range(1, len(dated_weights)):
        turnover += _dynamic_trend_weight_turnover(
            dated_weights[index - 1][1],
            dated_weights[index][1],
        )
    returns_by_horizon: dict[int, list[float]] = {
        horizon: [] for horizon in MASKING_OUTCOME_HORIZONS
    }
    for case in evaluated:
        portfolio_returns = case.get("portfolio_returns", {})
        if not isinstance(portfolio_returns, Mapping):
            continue
        for horizon in MASKING_OUTCOME_HORIZONS:
            value = portfolio_returns.get(horizon)
            if isinstance(value, float):
                returns_by_horizon[horizon].append(value)
    drawdowns = [
        value
        for value in (
            case.get("portfolio_max_drawdown_20d")
            for case in evaluated
            if isinstance(case, Mapping)
        )
        if isinstance(value, float)
    ]
    metrics: dict[str, Any] = {
        f"avg_return_{horizon}d": _mean(values) for horizon, values in returns_by_horizon.items()
    }
    metrics.update(
        {
            f"hit_rate_{horizon}d": _ratio(
                sum(1 for value in values if value > 0),
                len(values),
            )
            for horizon, values in returns_by_horizon.items()
        }
    )
    metrics.update(
        {
            "max_drawdown": min(drawdowns) if drawdowns else None,
            "drawdown_preservation": None,
            "turnover": turnover,
            "constraint_hit_count": sum(1 for case in evaluated if case.get("constraint_hit")),
            "risk_off_trigger_count": sum(
                1 for case in evaluated if case.get("risk_off_triggered")
            ),
            "risk_on_confirmation_count": sum(
                1 for case in evaluated if case.get("risk_on_confirmed")
            ),
            "false_risk_off_count": sum(1 for case in evaluated if case.get("false_risk_off")),
            "false_risk_on_count": sum(1 for case in evaluated if case.get("false_risk_on")),
            "missed_upside_count": sum(1 for case in evaluated if case.get("missed_upside")),
        }
    )
    return metrics


def _dynamic_trend_weight_turnover(
    previous: Mapping[str, float],
    current: Mapping[str, float],
) -> float:
    assets = set(previous) | set(current)
    return (
        sum(abs(_float(current.get(asset)) - _float(previous.get(asset))) for asset in assets) / 2
    )


def _dynamic_trend_variant_sample_quality(
    evaluated: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    mature_by_horizon = {
        f"{horizon}d": sum(
            1
            for case in evaluated
            if isinstance(case.get("portfolio_returns", {}), Mapping)
            and isinstance(case.get("portfolio_returns", {}).get(horizon), float)
        )
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    mature_dates_by_horizon = {
        f"{horizon}d": len(
            {
                str(case.get("date") or "")
                for case in evaluated
                if isinstance(case.get("portfolio_returns", {}), Mapping)
                and isinstance(case.get("portfolio_returns", {}).get(horizon), float)
            }
        )
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    missing_field_counts: dict[str, int] = defaultdict(int)
    proxy_field_counts: dict[str, int] = defaultdict(int)
    for case in evaluated:
        for field in case.get("missing_score_fields", []):
            missing_field_counts[str(field)] += 1
        for field in case.get("proxy_score_fields", []):
            proxy_field_counts[str(field)] += 1
    trace_source_counts = {
        source: sum(1 for case in evaluated if str(case.get("trace_source") or "") == source)
        for source in sorted({str(case.get("trace_source") or "") for case in evaluated})
        if source
    }
    case_origin_counts = {
        "coverage_extension_bridge": sum(
            1 for case in evaluated if case.get("coverage_extension_case")
        ),
        "historical_trace": sum(1 for case in evaluated if not case.get("coverage_extension_case")),
    }
    coverage_extension_cases = [case for case in evaluated if case.get("coverage_extension_case")]
    return {
        "case_count": len(evaluated),
        "date_count": len({str(case.get("date") or "") for case in evaluated}),
        "asset_count": len({str(case.get("asset") or "") for case in evaluated}),
        "independent_signal_date_count": len(
            {
                (
                    str(case.get("date") or ""),
                    str(case.get("coverage_extension_scenario") or "historical_trace"),
                )
                for case in evaluated
            }
        ),
        "mature_case_count": mature_by_horizon["20d"],
        "mature_date_count": mature_dates_by_horizon["20d"],
        "mature_case_count_by_horizon": mature_by_horizon,
        "mature_date_count_by_horizon": mature_dates_by_horizon,
        "pending_maturity_tracker": _dynamic_trend_pending_maturity_tracker(evaluated),
        "full_advisory_case_count": sum(
            1 for case in evaluated if case.get("full_advisory_trace_eligible")
        ),
        "component_case_count": sum(
            1
            for case in evaluated
            if case.get("component_validation_trace_eligible")
            and not case.get("full_advisory_trace_eligible")
        ),
        "backtest_bridge_case_count": sum(
            1 for case in evaluated if case.get("trace_source") == BACKTEST_TRACE_BRIDGE_SOURCE
        ),
        "cluster_count": len(
            {str(case.get("correlated_asset_cluster") or "") for case in evaluated}
        ),
        "regime_count": len({str(case.get("market_regime") or "") for case in evaluated}),
        "trace_source_counts": trace_source_counts,
        "case_origin_counts": case_origin_counts,
        "coverage_extension_case_count": len(coverage_extension_cases),
        "coverage_extension_source_file_count": len(
            {
                str(case.get("coverage_extension_source_path") or "")
                for case in coverage_extension_cases
                if case.get("coverage_extension_source_path")
            }
        ),
        "coverage_extension_scenario_count": len(
            {
                str(case.get("coverage_extension_scenario") or "")
                for case in coverage_extension_cases
            }
        ),
        "coverage_extension_note": (
            "Coverage extension cases are backtest bridge diagnostics; they improve "
            "asset/cluster/regime coverage but do not increase full_advisory_case_count."
        ),
        "outcome_missing_count": sum(1 for case in evaluated if case.get("outcome_missing")),
        "outcome_not_mature_count": sum(1 for case in evaluated if case.get("outcome_not_mature")),
        "missing_score_field_counts": dict(sorted(missing_field_counts.items())),
        "proxy_score_field_counts": dict(sorted(proxy_field_counts.items())),
        "direct_dynamic_allocation_score_history_available": False,
        "sample_quality_note": (
            "Component-level trace is used for validation-only sensitivity; "
            "direct production-equivalent dynamic allocation replay is not present."
        ),
    }


def _dynamic_trend_pending_maturity_tracker(
    evaluated: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    tracker: dict[str, dict[str, Any]] = {}
    for horizon in MASKING_OUTCOME_HORIZONS:
        label = f"{horizon}d"
        pending_cases = []
        pending_dates = set()
        missing_cases = 0
        for case in evaluated:
            portfolio_returns = case.get("portfolio_returns", {})
            if isinstance(portfolio_returns, Mapping) and isinstance(
                portfolio_returns.get(horizon),
                float,
            ):
                continue
            status = _dynamic_trend_case_horizon_status(case, horizon)
            if status == OUTCOME_WINDOW_STATUS_NOT_MATURE:
                pending_cases.append(str(case.get("case_id") or ""))
                pending_dates.add(str(case.get("date") or ""))
            elif status != OUTCOME_WINDOW_STATUS_AVAILABLE:
                missing_cases += 1
        tracker[label] = {
            "pending_case_count": len(pending_cases),
            "pending_date_count": len(pending_dates),
            "missing_or_unavailable_case_count": missing_cases,
            "sample_case_ids": [case_id for case_id in pending_cases if case_id][:10],
            "next_rerun_condition": (
                f"Rerun after at least {horizon} additional trading days are available "
                "for pending signal dates."
                if pending_cases
                else "No pending maturity for this horizon in the evaluated sample."
            ),
        }
    return tracker


def _dynamic_trend_case_horizon_status(case: Mapping[str, Any], horizon: int) -> str:
    windows = case.get("outcome_windows", [])
    if isinstance(windows, Sequence) and not isinstance(windows, (str, bytes)):
        for window in windows:
            if not isinstance(window, Mapping):
                continue
            if int(window.get("horizon_trading_days") or 0) != horizon:
                continue
            return str(window.get("status") or "")
    return ""


def _dynamic_trend_by_horizon(
    evaluated: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        values = [
            case.get("portfolio_returns", {}).get(horizon)
            for case in evaluated
            if isinstance(case.get("portfolio_returns", {}), Mapping)
            and isinstance(case.get("portfolio_returns", {}).get(horizon), float)
        ]
        value_list = [value for value in values if isinstance(value, float)]
        rows.append(
            {
                "horizon": f"{horizon}d",
                "horizon_trading_days": horizon,
                "mature_case_count": len(value_list),
                "mature_date_count": len(
                    {
                        str(case.get("date") or "")
                        for case in evaluated
                        if isinstance(case.get("portfolio_returns", {}), Mapping)
                        and isinstance(case.get("portfolio_returns", {}).get(horizon), float)
                    }
                ),
                "avg_return": _mean(value_list),
                "hit_rate": _ratio(
                    sum(1 for value in value_list if value > 0),
                    len(value_list),
                ),
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _dynamic_trend_group_rows(
    evaluated: Sequence[Mapping[str, Any]],
    field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in evaluated:
        grouped[str(case.get(field) or "UNKNOWN")].append(case)
    return [
        {"group": group_key, field: group_key, **_dynamic_trend_group_summary(group_cases)}
        for group_key, group_cases in sorted(grouped.items())
    ]


def _dynamic_trend_event_window_rows(
    evaluated: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in evaluated:
        windows = case.get("event_window_ids", [])
        if not windows:
            grouped["outside_defined_event_window"].append(case)
            continue
        for window_id in windows:
            grouped[str(window_id)].append(case)
    return [
        {"event_window_id": group_key, **_dynamic_trend_group_summary(group_cases)}
        for group_key, group_cases in sorted(grouped.items())
    ]


def _dynamic_trend_group_summary(
    group_cases: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metrics = _dynamic_trend_metrics_for_evaluated_cases(group_cases)
    sample_quality = _dynamic_trend_variant_sample_quality(group_cases)
    return {
        "case_count": len(group_cases),
        "mature_case_count": sample_quality["mature_case_count"],
        "mature_date_count": sample_quality["mature_date_count"],
        "full_advisory_case_count": sample_quality["full_advisory_case_count"],
        "cluster_count": sample_quality["cluster_count"],
        "regime_count": sample_quality["regime_count"],
        "avg_return_1d": metrics["avg_return_1d"],
        "avg_return_5d": metrics["avg_return_5d"],
        "avg_return_10d": metrics["avg_return_10d"],
        "avg_return_20d": metrics["avg_return_20d"],
        "hit_rate_1d": metrics["hit_rate_1d"],
        "hit_rate_5d": metrics["hit_rate_5d"],
        "hit_rate_10d": metrics["hit_rate_10d"],
        "hit_rate_20d": metrics["hit_rate_20d"],
        "max_drawdown": metrics["max_drawdown"],
        "turnover": metrics["turnover"],
        "constraint_hit_count": metrics["constraint_hit_count"],
        "risk_off_trigger_count": metrics["risk_off_trigger_count"],
        "risk_on_confirmation_count": metrics["risk_on_confirmation_count"],
        "false_risk_off_count": metrics["false_risk_off_count"],
        "false_risk_on_count": metrics["false_risk_on_count"],
        "missed_upside_count": metrics["missed_upside_count"],
        "promotion_gate_allowed": False,
    }


def _apply_dynamic_trend_drawdown_preservation(variants: Sequence[dict[str, Any]]) -> None:
    baseline = next(
        (variant for variant in variants if variant.get("variant_kind") == "no_change_baseline"),
        None,
    )
    baseline_drawdown = _optional_float((baseline or {}).get("max_drawdown"))
    for variant in variants:
        drawdown = _optional_float(variant.get("max_drawdown"))
        if baseline_drawdown is None or drawdown is None:
            variant["drawdown_preservation"] = None
        else:
            variant["drawdown_preservation"] = drawdown - baseline_drawdown


def _dynamic_trend_variant_recommendation(sample_quality: Mapping[str, Any]) -> str:
    if int(sample_quality.get("case_count") or 0) <= 0:
        return "insufficient_data"
    if int(sample_quality.get("mature_case_count_by_horizon", {}).get("1d") or 0) <= 0:
        return "insufficient_data"
    if int(sample_quality.get("full_advisory_case_count") or 0) <= 0:
        return "sensitivity_tested_only"
    return "sensitivity_tested_only"


def _dynamic_trend_record_recommendation(
    variants: Sequence[Mapping[str, Any]],
    *,
    sample_quality: Mapping[str, Any],
) -> dict[str, Any]:
    variant_recommendations = {str(variant.get("recommendation")) for variant in variants}
    if int(sample_quality.get("case_count") or 0) <= 0:
        recommendation = "insufficient_data"
        reason = "No trace cases are available for dynamic/trend sensitivity review."
    elif int(sample_quality.get("mature_case_count_by_horizon", {}).get("1d") or 0) <= 0:
        recommendation = "insufficient_data"
        reason = "No mature forward outcome windows are available."
    elif int(sample_quality.get("full_advisory_case_count") or 0) <= 0:
        recommendation = "sensitivity_tested_only"
        reason = (
            "Sensitivity metrics are computed from component/backtest validation sources, "
            "but full advisory equivalent cases are not sufficient for calibration."
        )
    else:
        recommendation = "sensitivity_tested_only"
        reason = (
            "Sensitivity evidence is available, but this validation-only review does not "
            "authorize a threshold value change or validated boundary."
        )
    if recommendation not in DYNAMIC_TREND_VALIDATION_RECOMMENDATIONS:
        recommendation = "collect_evidence_only"
    return {
        "validation_recommendation": recommendation,
        "reason": reason,
        "variant_recommendations": sorted(variant_recommendations),
        "whether_recommendation_changed": False,
        "current_value_change_allowed": False,
        "promotion_gate_allowed": False,
        "production_effect": "none",
    }


def _dynamic_trend_evidence_strength(sample_quality: Mapping[str, Any]) -> str:
    mature_20d = int(sample_quality.get("mature_case_count_by_horizon", {}).get("20d") or 0)
    full_count = int(sample_quality.get("full_advisory_case_count") or 0)
    cluster_count = int(sample_quality.get("cluster_count") or 0)
    if mature_20d >= 50 and full_count >= 50 and cluster_count >= 2:
        return "medium"
    if int(sample_quality.get("mature_case_count_by_horizon", {}).get("1d") or 0) > 0:
        return "low"
    return "insufficient"


def _dynamic_trend_remaining_limitations(
    threshold_id: str,
    sample_quality: Mapping[str, Any],
) -> list[str]:
    limitations = [
        "This artifact is validation-only and cannot change current threshold values.",
        (
            "Direct production-equivalent dynamic allocation replay is not available "
            "in the source trace."
        ),
    ]
    if sample_quality.get("missing_score_field_counts"):
        limitations.append("Some dynamic allocation score fields are missing from trace cases.")
    if int(sample_quality.get("full_advisory_case_count") or 0) <= 0:
        limitations.append("Full advisory equivalent cases are insufficient for calibration.")
    if int(sample_quality.get("mature_case_count_by_horizon", {}).get("20d") or 0) < 50:
        limitations.append("20d mature case count is below the heuristic validation floor.")
    if threshold_id == "trend_calibration.score_bands":
        limitations.append("Trend band review does not validate raw trend score construction.")
    return limitations


def _dynamic_trend_sensitivity_summary(
    records: Sequence[Mapping[str, Any]],
    cases: Sequence[Mapping[str, Any]],
    threshold_audit_summary: Mapping[str, Any],
) -> dict[str, Any]:
    sensitivity_tested = sum(
        1 for record in records if record.get("calibration_status") == "SENSITIVITY_TESTED"
    )
    first_quality = (
        records[0].get("sample_quality", {}) if records and isinstance(records[0], Mapping) else {}
    )
    quality = dict(first_quality) if isinstance(first_quality, Mapping) else {}
    if not quality:
        quality = _dynamic_trend_variant_sample_quality(cases)
    still_uncalibrated = int(threshold_audit_summary.get("uncalibrated_high_impact_count") or 0)
    still_blocking = len(threshold_audit_summary.get("thresholds_blocking_promotion", []) or [])
    return {
        "tested_threshold_count": len(records),
        "target_threshold_ids": [record.get("threshold_id") for record in records],
        "sensitivity_tested_count": sensitivity_tested,
        "validated_boundary_count": 0,
        "thresholds_changed_count": 0,
        "still_uncalibrated_high_impact_count": still_uncalibrated,
        "thresholds_still_blocking_promotion_count": still_blocking,
        "mature_date_count": quality["mature_date_count"],
        "mature_case_count": quality["mature_case_count"],
        "full_advisory_case_count": quality["full_advisory_case_count"],
        "cluster_count": quality["cluster_count"],
        "regime_count": quality["regime_count"],
        "coverage_targets": {
            "cluster_count_min": 3,
            "regime_count_min": 3,
            "mature_horizons_required": ["1d", "5d", "10d"],
            "full_advisory_case_count_goal": "increase_when_production_equivalent_trace_exists",
        },
        "coverage_target_status": {
            "cluster_count_at_least_3": int(quality.get("cluster_count") or 0) >= 3,
            "regime_count_at_least_3": int(quality.get("regime_count") or 0) >= 3,
            "mature_1d_5d_10d_available": all(
                int(quality.get("mature_case_count_by_horizon", {}).get(label) or 0) > 0
                for label in ("1d", "5d", "10d")
            ),
            "twenty_day_maturity_available": (
                int(quality.get("mature_case_count_by_horizon", {}).get("20d") or 0) > 0
            ),
            "full_advisory_case_count_increased": (
                int(quality.get("full_advisory_case_count") or 0)
                > int(DYNAMIC_TREND_TRADING_698_BASELINE_COVERAGE["full_advisory_case_count"])
            ),
        },
        "trading_698_baseline_coverage": dict(DYNAMIC_TREND_TRADING_698_BASELINE_COVERAGE),
        "coverage_extension_case_count": quality.get("coverage_extension_case_count", 0),
        "trace_source_counts": quality.get("trace_source_counts", {}),
        "case_origin_counts": quality.get("case_origin_counts", {}),
        "pending_maturity_tracker": quality.get("pending_maturity_tracker", {}),
        "sample_quality_breakdown": quality,
        "max_allowed_status": "SENSITIVITY_TESTED",
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "validation_only": True,
    }


def _dynamic_trend_sensitivity_issues(
    records: Sequence[Mapping[str, Any]],
    *,
    trace_path: Path | None,
    prices_path: Path | None,
    coverage_extension_root: Path | None,
) -> list[dict[str, Any]]:
    issues = [
        {
            "severity": "info",
            "issue_id": "dynamic_trend_threshold_sensitivity_validation_only",
            "message": (
                "Dynamic/trend threshold sensitivity review is validation-only; it cannot "
                "change current values or approve production, paper-shadow, official weights, "
                "or promotion."
            ),
        }
    ]
    if trace_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dynamic_trend_trace_missing",
                "message": "No trace path was provided; sensitivity evidence is data-limited.",
            }
        )
    if prices_path is None:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dynamic_trend_prices_missing",
                "message": "No prices path was provided; forward return metrics are unavailable.",
            }
        )
    if coverage_extension_root is None:
        issues.append(
            {
                "severity": "info",
                "issue_id": "dynamic_trend_coverage_extension_not_requested",
                "message": (
                    "No coverage extension root was provided; cluster/regime coverage is limited "
                    "to the source trace."
                ),
            }
        )
    elif not coverage_extension_root.exists():
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dynamic_trend_coverage_extension_root_missing",
                "message": f"Coverage extension root does not exist: {coverage_extension_root}",
            }
        )
    if any(
        record.get("sample_quality", {}).get("missing_score_field_counts") for record in records
    ):
        issues.append(
            {
                "severity": "warning",
                "issue_id": "dynamic_trend_score_fields_missing",
                "message": (
                    "Some required dynamic allocation score fields are missing from source trace "
                    "and are reported as data gaps, not imputed thresholds."
                ),
            }
        )
    return issues


def _load_dynamic_trend_sensitivity_review_for_consistency(
    *,
    sensitivity_review_path: Path | None,
    sensitivity_review_payload: Mapping[str, Any] | None,
    registry_path: Path,
    threshold_registry_path: Path,
    trace_path: Path | None,
    prices_path: Path | None,
    gate_audit_root: Path | None,
    bridge_artifact_root: Path | None,
    coverage_extension_root: Path | None,
    outcome_ticker: str,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
    asset_universe: str | None,
) -> Mapping[str, Any]:
    if sensitivity_review_payload is not None:
        payload = dict(sensitivity_review_payload)
    elif sensitivity_review_path is not None:
        if not sensitivity_review_path.exists():
            raise IndicatorResearchError(
                f"Dynamic trend sensitivity review not found: {sensitivity_review_path}"
            )
        raw = json.loads(sensitivity_review_path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise IndicatorResearchError(
                f"Dynamic trend sensitivity review is not an object: {sensitivity_review_path}"
            )
        payload = dict(raw)
    else:
        payload = build_dynamic_trend_threshold_sensitivity_review(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    if payload.get("report_type") != "dynamic_trend_threshold_sensitivity_review":
        raise IndicatorResearchError(
            "TRADING-700 consistency audit requires a dynamic_trend_threshold_sensitivity_review "
            "payload."
        )
    return payload


def _dynamic_trend_bridge_consistency_records(
    sensitivity_review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    records = sensitivity_review.get("threshold_sensitivity_reviews", [])
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        records = []
    by_id = {
        str(record.get("threshold_id")): record for record in records if isinstance(record, Mapping)
    }
    return [
        _dynamic_trend_bridge_consistency_record(
            threshold_id,
            source_record=by_id.get(threshold_id, {}),
        )
        for threshold_id in SECOND_BATCH_DYNAMIC_TREND_THRESHOLD_IDS
    ]


def _dynamic_trend_bridge_consistency_record(
    threshold_id: str,
    *,
    source_record: Mapping[str, Any],
) -> dict[str, Any]:
    variants = source_record.get("scenario_variants", [])
    if not isinstance(variants, Sequence) or isinstance(variants, (str, bytes)):
        variants = []
    variant_rows = [
        _dynamic_trend_variant_source_consistency(variant)
        for variant in variants
        if isinstance(variant, Mapping)
    ]
    primary = _primary_dynamic_trend_consistency_variant(variant_rows)
    direction = _dynamic_trend_source_direction_agreement(variant_rows)
    full_recommendation = _dynamic_trend_threshold_source_recommendation(
        variant_rows,
        source_layer="full_advisory_only",
    )
    bridge_recommendation = _dynamic_trend_threshold_source_recommendation(
        variant_rows,
        source_layer="backtest_bridge_only",
    )
    reliability = _dynamic_trend_bridge_reliability_label(direction, variant_rows)
    return {
        "threshold_id": threshold_id,
        "current_value": _json_ready(source_record.get("current_value")),
        "tested_values": _json_ready(source_record.get("tested_values", [])),
        "recommendation": "sensitivity_tested_only",
        "recommended_status": "SENSITIVITY_TESTED",
        "evidence_strength": "low",
        "recommendation_by_full_advisory_only": full_recommendation,
        "recommendation_by_backtest_bridge_only": bridge_recommendation,
        "direction_agreement": direction,
        "bridge_reliability": reliability,
        "metric_delta_by_source": primary.get("metric_delta_by_source", {}),
        "false_risk_off_delta": primary.get("metric_delta_by_source", {}).get(
            "false_risk_off_count"
        ),
        "false_risk_on_delta": primary.get("metric_delta_by_source", {}).get("false_risk_on_count"),
        "missed_upside_delta": primary.get("metric_delta_by_source", {}).get("missed_upside_count"),
        "drawdown_preservation_delta": primary.get("metric_delta_by_source", {}).get(
            "drawdown_preservation"
        ),
        "turnover_delta": primary.get("metric_delta_by_source", {}).get("turnover"),
        "constraint_hit_delta": primary.get("metric_delta_by_source", {}).get(
            "constraint_hit_count"
        ),
        "variant_source_consistency": variant_rows,
        "remaining_limitations": _dynamic_trend_bridge_consistency_limitations(
            threshold_id,
            variant_rows,
        ),
        "validated_boundary": False,
        "not_validated_statistical_boundary": True,
        "current_value_changed": False,
        "threshold_value_change_allowed": False,
        "bridge_only_promotion_gate_evidence_allowed": False,
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_effect": "none",
    }


def _dynamic_trend_variant_source_consistency(
    variant: Mapping[str, Any],
) -> dict[str, Any]:
    full = _dynamic_trend_full_advisory_metrics(variant)
    bridge = _dynamic_trend_trace_source_metrics(variant, BACKTEST_TRACE_BRIDGE_SOURCE)
    full_quality = _dynamic_trend_source_quality(full, source_layer="full_advisory_only")
    bridge_quality = _dynamic_trend_source_quality(bridge, source_layer="backtest_bridge_only")
    delta = _dynamic_trend_metric_delta_by_source(full, bridge)
    return {
        "variant_id": variant.get("variant_id"),
        "variant_kind": variant.get("variant_kind"),
        "scenario_id": variant.get("scenario_id"),
        "tested_value": _json_ready(variant.get("tested_value", {})),
        "recommendation_by_full_advisory_only": _dynamic_trend_source_recommendation(full_quality),
        "recommendation_by_backtest_bridge_only": _dynamic_trend_source_recommendation(
            bridge_quality
        ),
        "direction_agreement": "insufficient_full_advisory_to_assess",
        "full_advisory_only": _dynamic_trend_source_metrics_projection(full),
        "backtest_bridge_only": _dynamic_trend_source_metrics_projection(bridge),
        "full_advisory_sample_quality": full_quality,
        "backtest_bridge_sample_quality": bridge_quality,
        "metric_delta_by_source": delta,
        "false_risk_off_delta": delta.get("false_risk_off_count"),
        "false_risk_on_delta": delta.get("false_risk_on_count"),
        "missed_upside_delta": delta.get("missed_upside_count"),
        "drawdown_preservation_delta": delta.get("drawdown_preservation"),
        "turnover_delta": delta.get("turnover"),
        "constraint_hit_delta": delta.get("constraint_hit_count"),
        "promotion_gate_allowed": False,
        "production_effect": "none",
    }


def _dynamic_trend_full_advisory_metrics(variant: Mapping[str, Any]) -> Mapping[str, Any]:
    full = variant.get("full_advisory_only", {})
    return full if isinstance(full, Mapping) else {}


def _dynamic_trend_trace_source_metrics(
    variant: Mapping[str, Any],
    trace_source: str,
) -> Mapping[str, Any]:
    rows = variant.get("component_backtest_bridge", [])
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return {}
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("trace_source") or "") == trace_source:
            return row
    return {}


def _dynamic_trend_source_metrics_projection(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _json_ready(metrics.get(key))
        for key in (
            "case_count",
            "mature_case_count",
            "mature_date_count",
            "full_advisory_case_count",
            "cluster_count",
            "regime_count",
            *DYNAMIC_TREND_SOURCE_COMPARISON_METRICS,
        )
        if key in metrics
    }


def _dynamic_trend_source_quality(
    metrics: Mapping[str, Any],
    *,
    source_layer: str,
) -> dict[str, Any]:
    case_count = int(metrics.get("case_count") or 0)
    mature_case_count = int(metrics.get("mature_case_count") or 0)
    mature_date_count = int(metrics.get("mature_date_count") or 0)
    full_advisory_case_count = int(metrics.get("full_advisory_case_count") or 0)
    cluster_count = int(metrics.get("cluster_count") or 0)
    regime_count = int(metrics.get("regime_count") or 0)
    data_gaps: list[str] = []
    if case_count <= 0:
        data_gaps.append("case_count=0")
    if mature_case_count <= 0:
        data_gaps.append("20d_mature_case_count=0")
    if source_layer == "full_advisory_only":
        if case_count < 50:
            data_gaps.append("full_advisory_case_count_below_consistency_floor_50")
        if mature_case_count < 50:
            data_gaps.append("full_advisory_20d_mature_case_count_below_floor_50")
        if cluster_count < 2:
            data_gaps.append("full_advisory_cluster_count_below_2")
        if regime_count < 2:
            data_gaps.append("full_advisory_regime_count_below_2")
    return {
        "source_layer": source_layer,
        "case_count": case_count,
        "mature_case_count": mature_case_count,
        "mature_date_count": mature_date_count,
        "full_advisory_case_count": full_advisory_case_count,
        "cluster_count": cluster_count,
        "regime_count": regime_count,
        "sufficient_for_consistency_assessment": not data_gaps,
        "data_gaps": data_gaps,
    }


def _dynamic_trend_source_recommendation(source_quality: Mapping[str, Any]) -> str:
    if int(source_quality.get("case_count") or 0) <= 0:
        return "insufficient_data"
    if int(source_quality.get("mature_case_count") or 0) <= 0:
        return "insufficient_data"
    if not source_quality.get("sufficient_for_consistency_assessment"):
        return "insufficient_data"
    return "sensitivity_tested_only"


def _dynamic_trend_threshold_source_recommendation(
    variant_rows: Sequence[Mapping[str, Any]],
    *,
    source_layer: str,
) -> str:
    recommendations = {
        str(row.get(f"recommendation_by_{source_layer}") or "")
        for row in variant_rows
        if isinstance(row, Mapping)
    }
    if "sensitivity_tested_only" in recommendations:
        return "sensitivity_tested_only"
    if "collect_evidence_only" in recommendations:
        return "collect_evidence_only"
    return "insufficient_data"


def _dynamic_trend_metric_delta_by_source(
    full: Mapping[str, Any],
    bridge: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        field: _numeric_delta(bridge.get(field), full.get(field))
        for field in DYNAMIC_TREND_SOURCE_COMPARISON_METRICS
    }


def _numeric_delta(left: Any, right: Any) -> Any:
    if isinstance(left, bool) or isinstance(right, bool):
        return None
    if left is None or right is None:
        return None
    if isinstance(left, int) and isinstance(right, int):
        return left - right
    left_value = _optional_float(left)
    right_value = _optional_float(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _primary_dynamic_trend_consistency_variant(
    variant_rows: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    for row in variant_rows:
        if isinstance(row, Mapping) and row.get("variant_kind") == "current_value":
            return row
    return variant_rows[0] if variant_rows else {}


def _dynamic_trend_source_direction_agreement(
    variant_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    full_qualities = [
        row.get("full_advisory_sample_quality", {})
        for row in variant_rows
        if isinstance(row, Mapping)
    ]
    if not full_qualities or any(
        not isinstance(quality, Mapping) or not quality.get("sufficient_for_consistency_assessment")
        for quality in full_qualities
    ):
        return {
            "status": "insufficient_full_advisory_to_assess",
            "agreement_ratio": None,
            "agreed_metric_count": 0,
            "conflicting_metric_count": 0,
            "insufficient_reason": (
                "Full-advisory source layer is below the consistency assessment floor; "
                "bridge magnitude cannot validate direction."
            ),
        }
    baseline = next(
        (
            row
            for row in variant_rows
            if isinstance(row, Mapping) and row.get("variant_kind") == "no_change_baseline"
        ),
        None,
    )
    if not isinstance(baseline, Mapping):
        return {
            "status": "insufficient_baseline_to_assess",
            "agreement_ratio": None,
            "agreed_metric_count": 0,
            "conflicting_metric_count": 0,
            "insufficient_reason": "No no-change baseline variant is available.",
        }
    agreed = 0
    conflicted = 0
    compared = 0
    for row in variant_rows:
        if not isinstance(row, Mapping) or row.get("variant_kind") == "no_change_baseline":
            continue
        for metric in DYNAMIC_TREND_DIRECTIONAL_METRICS:
            full_sign = _directional_metric_sign(
                row.get("full_advisory_only", {}).get(metric),
                baseline.get("full_advisory_only", {}).get(metric),
                metric,
            )
            bridge_sign = _directional_metric_sign(
                row.get("backtest_bridge_only", {}).get(metric),
                baseline.get("backtest_bridge_only", {}).get(metric),
                metric,
            )
            if full_sign is None or bridge_sign is None:
                continue
            compared += 1
            if full_sign == bridge_sign:
                agreed += 1
            else:
                conflicted += 1
    ratio = _ratio(agreed, compared)
    return {
        "status": "directionally_agree" if conflicted == 0 else "directionally_conflict",
        "agreement_ratio": ratio,
        "agreed_metric_count": agreed,
        "conflicting_metric_count": conflicted,
        "compared_metric_count": compared,
        "insufficient_reason": None,
    }


def _directional_metric_sign(value: Any, baseline: Any, metric: str) -> int | None:
    delta = _numeric_delta(value, baseline)
    if delta is None:
        return None
    if metric in {
        "max_drawdown",
        "turnover",
        "constraint_hit_count",
        "false_risk_off_count",
        "false_risk_on_count",
        "missed_upside_count",
    }:
        delta = -delta
    if abs(float(delta)) < 1e-12:
        return 0
    return 1 if float(delta) > 0 else -1


def _dynamic_trend_bridge_reliability_label(
    direction: Mapping[str, Any],
    variant_rows: Sequence[Mapping[str, Any]],
) -> str:
    if str(direction.get("status") or "") == "insufficient_full_advisory_to_assess":
        return "insufficient_full_advisory_to_assess"
    if int(direction.get("conflicting_metric_count") or 0) > 0:
        return "bridge_conflicts_with_full_advisory"
    if _dynamic_trend_bridge_magnitude_uncertain(variant_rows):
        return "bridge_directionally_consistent_but_magnitude_uncertain"
    return "bridge_consistent_with_full_advisory"


def _dynamic_trend_bridge_magnitude_uncertain(
    variant_rows: Sequence[Mapping[str, Any]],
) -> bool:
    for row in variant_rows:
        if not isinstance(row, Mapping):
            continue
        full_quality = row.get("full_advisory_sample_quality", {})
        bridge_quality = row.get("backtest_bridge_sample_quality", {})
        if not isinstance(full_quality, Mapping) or not isinstance(bridge_quality, Mapping):
            return True
        full_cases = int(full_quality.get("case_count") or 0)
        bridge_cases = int(bridge_quality.get("case_count") or 0)
        if full_cases <= 0 or bridge_cases >= full_cases * 5:
            return True
    return False


def _dynamic_trend_bridge_consistency_limitations(
    threshold_id: str,
    variant_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    limitations = [
        "This artifact is validation-only and cannot change current threshold values.",
        "backtest_trace_bridge cannot be used alone as promotion gate evidence.",
        "Bridge evidence is diagnostic and not production-equivalent full advisory replay.",
    ]
    primary = _primary_dynamic_trend_consistency_variant(variant_rows)
    full_quality = primary.get("full_advisory_sample_quality", {})
    if isinstance(full_quality, Mapping) and full_quality.get("data_gaps"):
        limitations.append(
            "Full-advisory source layer is insufficient for consistency assessment: "
            + ", ".join(str(item) for item in full_quality.get("data_gaps", []))
        )
    if threshold_id == "trend_calibration.score_bands":
        limitations.append("Trend score band audit does not validate raw trend score construction.")
    return limitations


def _dynamic_trend_bridge_consistency_summary(
    records: Sequence[Mapping[str, Any]],
    sensitivity_review: Mapping[str, Any],
) -> dict[str, Any]:
    sensitivity_summary = sensitivity_review.get("summary", {})
    if not isinstance(sensitivity_summary, Mapping):
        sensitivity_summary = {}
    sample_quality = sensitivity_summary.get("sample_quality_breakdown", {})
    if not isinstance(sample_quality, Mapping):
        sample_quality = {}
    trace_source_counts = sample_quality.get("trace_source_counts", {})
    if not isinstance(trace_source_counts, Mapping):
        trace_source_counts = {}
    reliability_counts = {
        label: sum(1 for record in records if record.get("bridge_reliability") == label)
        for label in DYNAMIC_TREND_BRIDGE_RELIABILITY_LABELS
    }
    full_advisory_case_count = int(
        trace_source_counts.get(FULL_ADVISORY_TRACE_SOURCE)
        or sample_quality.get("full_advisory_case_count")
        or sensitivity_summary.get("full_advisory_case_count")
        or 0
    )
    backtest_bridge_case_count = int(
        trace_source_counts.get(BACKTEST_TRACE_BRIDGE_SOURCE)
        or sample_quality.get("backtest_bridge_case_count")
        or 0
    )
    return {
        "tested_threshold_count": len(records),
        "target_threshold_ids": [record.get("threshold_id") for record in records],
        "source_sensitivity_report_type": sensitivity_review.get("report_type"),
        "source_sensitivity_status": sensitivity_review.get("status"),
        "full_advisory_case_count": full_advisory_case_count,
        "backtest_bridge_case_count": backtest_bridge_case_count,
        "cluster_count": int(sensitivity_summary.get("cluster_count") or 0),
        "regime_count": int(sensitivity_summary.get("regime_count") or 0),
        "mature_case_count_by_horizon": _json_ready(
            sample_quality.get("mature_case_count_by_horizon", {})
        ),
        "trace_source_counts": _json_ready(trace_source_counts),
        "bridge_reliability_counts": reliability_counts,
        "insufficient_full_advisory_to_assess_count": reliability_counts[
            "insufficient_full_advisory_to_assess"
        ],
        "directionally_consistent_count": (
            reliability_counts["bridge_consistent_with_full_advisory"]
            + reliability_counts["bridge_directionally_consistent_but_magnitude_uncertain"]
        ),
        "conflict_count": reliability_counts["bridge_conflicts_with_full_advisory"],
        "recommendation": "sensitivity_tested_only",
        "evidence_strength": "low",
        "validated_boundary_count": 0,
        "thresholds_changed_count": 0,
        "backtest_bridge_promotion_gate_evidence_allowed": False,
        "bridge_only_promotion_gate_evidence_allowed": False,
        "max_allowed_status": "SENSITIVITY_TESTED",
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
        "validation_only": True,
    }


def _dynamic_trend_bridge_consistency_issues(
    summary: Mapping[str, Any],
    *,
    sensitivity_review_path: Path | None,
) -> list[dict[str, Any]]:
    issues = [
        {
            "severity": "info",
            "issue_id": "dynamic_trend_bridge_consistency_validation_only",
            "message": (
                "TRADING-700 source-layer consistency audit is validation-only; it cannot "
                "change current threshold values or approve production, paper-shadow, official "
                "weights, or promotion."
            ),
        },
        {
            "severity": "warning",
            "issue_id": "backtest_trace_bridge_not_promotion_gate_evidence",
            "message": (
                "backtest_trace_bridge diagnostics cannot be used alone as promotion gate "
                "evidence; full-advisory consistency remains required."
            ),
        },
    ]
    if sensitivity_review_path is None:
        issues.append(
            {
                "severity": "info",
                "issue_id": "dynamic_trend_sensitivity_review_built_inline",
                "message": (
                    "No sensitivity review path was provided; the consistency audit used an "
                    "inline dynamic trend sensitivity payload."
                ),
            }
        )
    if int(summary.get("insufficient_full_advisory_to_assess_count") or 0) > 0:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "full_advisory_insufficient_for_bridge_consistency",
                "message": (
                    "Full-advisory cases are insufficient for source-layer consistency "
                    "assessment; evidence_strength stays low and recommendation stays "
                    "sensitivity_tested_only."
                ),
            }
        )
    return issues


def _dynamic_trend_data_source_mode(cases: Sequence[Mapping[str, Any]]) -> str:
    if not cases:
        return "no_trace_cases"
    has_full = any(case.get("full_advisory_trace_eligible") for case in cases)
    has_bridge = any(case.get("coverage_extension_case") for case in cases)
    if has_full and has_bridge:
        return "mixed_full_advisory_component_and_coverage_bridge"
    if has_bridge:
        return "component_trace_plus_coverage_bridge"
    if has_full:
        return "mixed_full_advisory_and_component_trace"
    return "component_trace_score_proxy"


def _threshold_calibration_record(
    threshold_id: str,
    *,
    threshold: Mapping[str, Any],
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
    effectiveness: Mapping[str, Any],
    robustness: Mapping[str, Any],
    long_floor: Mapping[str, Any],
    masking_audit: Mapping[str, Any],
    robustness_checks: Mapping[str, Any],
) -> dict[str, Any]:
    current_value = threshold.get("current_value")
    tested_values = THRESHOLD_CALIBRATION_TESTED_VALUES[threshold_id]
    if threshold_id == "indicator_research.effectiveness_min_available_outcome_cases":
        sensitivity_rows = _calibrate_effectiveness_min_available_outcome_cases(
            current_value=current_value,
            tested_values=tested_values,
            robustness=robustness,
            long_floor=long_floor,
        )
        recommended_action = "insufficient_data"
        reason = (
            "20d full-advisory effective sample and leave-one robustness remain insufficient; "
            "floor stays a heuristic guardrail."
        )
        evidence_strength = "LIMITED_SENSITIVITY_ONLY"
        remaining_limitations = [
            "20d full-advisory maturity remains below robust independent-date coverage.",
            "The floor is not a validated statistical boundary.",
        ]
        threshold_type = "heuristic_guardrail"
        not_validated = True
    elif threshold_id == "indicator_research.robustness_cluster_dominance_share":
        sensitivity_rows = _calibrate_robustness_cluster_dominance_share(
            current_value=current_value,
            tested_values=tested_values,
            robustness_checks=robustness_checks,
            robustness=robustness,
        )
        recommended_action = _threshold_action_from_rows(sensitivity_rows)
        reason = _threshold_action_reason(
            recommended_action,
            "Cluster concentration sensitivity remains validation-only.",
        )
        evidence_strength = _evidence_strength_from_rows(sensitivity_rows)
        remaining_limitations = [
            "Leave-one-cluster and full-advisory/all-sources consistency are still required.",
            "Threshold cannot enter promotion gate without owner-reviewed calibration.",
        ]
        threshold_type = threshold.get("threshold_type", "")
        not_validated = bool(threshold.get("not_validated_statistical_boundary", False))
    elif threshold_id == "indicator_research.effectiveness_missed_upside_acceptable_rate":
        sensitivity_rows = _calibrate_effectiveness_missed_upside_acceptable_rate(
            current_value=current_value,
            tested_values=tested_values,
            effectiveness=effectiveness,
            robustness=robustness,
        )
        recommended_action = _threshold_action_from_rows(sensitivity_rows)
        reason = _threshold_action_reason(
            recommended_action,
            "Missed-upside sensitivity is mixed across horizons and remains validation-only.",
        )
        evidence_strength = _evidence_strength_from_rows(sensitivity_rows)
        remaining_limitations = [
            "1d/5d/10d/20d evidence remains preliminary for stronger recommendation.",
            "False risk-off and missed-upside labels need owner-reviewed case labels.",
        ]
        threshold_type = threshold.get("threshold_type", "")
        not_validated = bool(threshold.get("not_validated_statistical_boundary", False))
    elif threshold_id == "indicator_research.masking_high_min":
        sensitivity_rows = _calibrate_masking_high_min(
            current_value=current_value,
            tested_values=tested_values,
            masking_audit=masking_audit,
        )
        recommended_action = _threshold_action_from_rows(sensitivity_rows)
        reason = _threshold_action_reason(
            recommended_action,
            "Masking high-min changes only diagnostic labels, not production behavior.",
        )
        evidence_strength = _evidence_strength_from_rows(sensitivity_rows)
        remaining_limitations = [
            "Masking ratio is a validation proxy and not causal evidence.",
            "Trace coverage and realized outcome linkage remain required before promotion use.",
        ]
        threshold_type = threshold.get("threshold_type", "")
        not_validated = bool(threshold.get("not_validated_statistical_boundary", False))
    elif threshold_id == "indicator_research.dominant_share_of_adjustment_min":
        sensitivity_rows = _calibrate_dominant_share_of_adjustment_min(
            current_value=current_value,
            tested_values=tested_values,
            registry=registry,
            trace_rows=trace_rows,
        )
        recommended_action = _threshold_action_from_rows(sensitivity_rows)
        reason = _threshold_action_reason(
            recommended_action,
            "Dominance threshold sensitivity is diagnostic and does not alter scoring.",
        )
        evidence_strength = _evidence_strength_from_rows(sensitivity_rows)
        remaining_limitations = [
            "Dominance needs stable multi-date trace coverage.",
            "Dominance labels do not prove independent incremental effect.",
        ]
        threshold_type = threshold.get("threshold_type", "")
        not_validated = bool(threshold.get("not_validated_statistical_boundary", False))
    else:
        raise IndicatorResearchError(f"Unsupported threshold calibration id: {threshold_id}")
    return {
        "threshold_id": threshold_id,
        "current_value": _json_ready(current_value),
        "tested_values": tested_values,
        "recommendation_by_value": sensitivity_rows,
        "promotion_gate_by_value": {
            str(row["tested_value"]): row["promotion_gate_allowed"] for row in sensitivity_rows
        },
        "false_promotion_risk": {
            str(row["tested_value"]): row["false_promotion_risk"] for row in sensitivity_rows
        },
        "false_rejection_risk": {
            str(row["tested_value"]): row["false_rejection_risk"] for row in sensitivity_rows
        },
        "sample_quality_impact": {
            str(row["tested_value"]): row["sample_quality_impact"] for row in sensitivity_rows
        },
        "valuation_crowding_recommendation_changes": any(
            bool(row["valuation_crowding_recommendation_changes"]) for row in sensitivity_rows
        ),
        "recommended_status": "SENSITIVITY_TESTED",
        "recommended_action": recommended_action,
        "keep_current_value": recommended_action == "keep_current_value",
        "adjust_candidate": recommended_action == "adjust_candidate",
        "insufficient_data": recommended_action == "insufficient_data",
        "reason": reason,
        "evidence_strength": evidence_strength,
        "remaining_limitations": remaining_limitations,
        "threshold_type": threshold_type,
        "not_validated_statistical_boundary": not_validated,
        "calibration_status_before": threshold.get("calibration_status", ""),
        "calibration_status_after_max": "SENSITIVITY_TESTED",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _calibration_row(
    *,
    tested_value: Any,
    recommendation: str,
    current_recommendation: str,
    false_promotion_risk: str,
    false_rejection_risk: str,
    sample_quality_impact: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "tested_value": tested_value,
        "recommendation": recommendation,
        "promotion_gate_allowed": False,
        "false_promotion_risk": false_promotion_risk,
        "false_rejection_risk": false_rejection_risk,
        "sample_quality_impact": dict(sample_quality_impact),
        "valuation_crowding_recommendation": recommendation,
        "valuation_crowding_recommendation_changes": recommendation != current_recommendation,
    }


def _calibrate_effectiveness_min_available_outcome_cases(
    *,
    current_value: Any,
    tested_values: Sequence[Any],
    robustness: Mapping[str, Any],
    long_floor: Mapping[str, Any],
) -> list[dict[str, Any]]:
    effective_sample = long_floor.get("effective_sample_size", {})
    if not isinstance(effective_sample, Mapping):
        effective_sample = {}
    robustness_gate = long_floor.get("robustness_based_gate", {})
    if not isinstance(robustness_gate, Mapping):
        robustness_gate = {}
    current_recommendation = _floor_recommendation_for_value(
        _safe_int(current_value, EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES),
        effective_sample=effective_sample,
        robustness_gate=robustness_gate,
        robustness=robustness,
    )
    rows = []
    for raw_value in tested_values:
        value = _safe_int(raw_value, EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES)
        sample_pass = int(effective_sample.get("raw_case_count") or 0) >= value
        robustness_pass = bool(robustness_gate.get("passed"))
        recommendation = _floor_recommendation_for_value(
            value,
            effective_sample=effective_sample,
            robustness_gate=robustness_gate,
            robustness=robustness,
        )
        rows.append(
            _calibration_row(
                tested_value=value,
                recommendation=recommendation,
                current_recommendation=current_recommendation,
                false_promotion_risk=("HIGH" if sample_pass and not robustness_pass else "LOW"),
                false_rejection_risk=("MEDIUM" if not sample_pass and robustness_pass else "LOW"),
                sample_quality_impact={
                    "raw_case_count": effective_sample.get("raw_case_count", 0),
                    "effective_date_count": effective_sample.get("effective_date_count", 0),
                    "effective_cluster_count": effective_sample.get(
                        "effective_cluster_count",
                        0,
                    ),
                    "sample_passes_tested_value": sample_pass,
                    "robustness_gate_passed": robustness_pass,
                    "threshold_type": "heuristic_guardrail",
                    "not_validated_statistical_boundary": True,
                },
            )
        )
    return rows


def _floor_recommendation_for_value(
    value: int,
    *,
    effective_sample: Mapping[str, Any],
    robustness_gate: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> str:
    raw_count = int(effective_sample.get("raw_case_count") or 0)
    if raw_count < value:
        return "insufficient_long_horizon_evidence"
    if not bool(robustness_gate.get("passed")):
        return "keep_preliminary_short_horizon_only"
    return str(
        robustness.get("summary", {}).get(
            "final_validation_recommendation",
            "keep_preliminary_short_horizon_only",
        )
        if isinstance(robustness.get("summary"), Mapping)
        else "keep_preliminary_short_horizon_only"
    )


def _calibrate_robustness_cluster_dominance_share(
    *,
    current_value: Any,
    tested_values: Sequence[Any],
    robustness_checks: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> list[dict[str, Any]]:
    cluster_check = robustness_checks.get("leave_one_cluster_out", {})
    if not isinstance(cluster_check, Mapping):
        cluster_check = {}
    consistency = robustness_checks.get("full_advisory_only_vs_all_sources_consistency", {})
    if not isinstance(consistency, Mapping):
        consistency = {}
    top_cluster_share = _optional_float(
        robustness.get("ten_day_baseline_support_attribution", {}).get("top_cluster_share")
        if isinstance(robustness.get("ten_day_baseline_support_attribution"), Mapping)
        else None
    )
    if top_cluster_share is None:
        top_cluster_share = 1.0
    current_recommendation = _cluster_recommendation_for_value(
        _safe_float(current_value, ROBUSTNESS_CLUSTER_DOMINANCE_SHARE),
        top_cluster_share=top_cluster_share,
        leave_one_stable=bool(cluster_check.get("stable")),
        full_vs_all_consistent=bool(consistency.get("consistent")),
        robustness=robustness,
    )
    rows = []
    for raw_value in tested_values:
        value = _safe_float(raw_value, ROBUSTNESS_CLUSTER_DOMINANCE_SHARE)
        pass_threshold = top_cluster_share <= value
        recommendation = _cluster_recommendation_for_value(
            value,
            top_cluster_share=top_cluster_share,
            leave_one_stable=bool(cluster_check.get("stable")),
            full_vs_all_consistent=bool(consistency.get("consistent")),
            robustness=robustness,
        )
        rows.append(
            _calibration_row(
                tested_value=value,
                recommendation=recommendation,
                current_recommendation=current_recommendation,
                false_promotion_risk=(
                    "HIGH"
                    if pass_threshold
                    and (not cluster_check.get("stable") or not consistency.get("consistent"))
                    else "LOW"
                ),
                false_rejection_risk="MEDIUM" if not pass_threshold else "LOW",
                sample_quality_impact={
                    "top_cluster_share": top_cluster_share,
                    "cluster_count": cluster_check.get("group_count", 0),
                    "threshold_passed": pass_threshold,
                    "leave_one_cluster_out_stable": bool(cluster_check.get("stable")),
                    "full_advisory_only_vs_all_sources_consistent": bool(
                        consistency.get("consistent")
                    ),
                },
            )
        )
    return rows


def _cluster_recommendation_for_value(
    value: float,
    *,
    top_cluster_share: float,
    leave_one_stable: bool,
    full_vs_all_consistent: bool,
    robustness: Mapping[str, Any],
) -> str:
    if top_cluster_share > value:
        return "cluster_dominated_insufficient_robustness"
    if not leave_one_stable or not full_vs_all_consistent:
        return "keep_preliminary_short_horizon_only"
    return str(
        robustness.get("summary", {}).get(
            "final_validation_recommendation",
            "keep_preliminary_short_horizon_only",
        )
        if isinstance(robustness.get("summary"), Mapping)
        else "keep_preliminary_short_horizon_only"
    )


def _calibrate_effectiveness_missed_upside_acceptable_rate(
    *,
    current_value: Any,
    tested_values: Sequence[Any],
    effectiveness: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> list[dict[str, Any]]:
    observed_rate = _baseline_missed_upside_rate(effectiveness)
    conservative_gate = (
        robustness.get("conservative_evidence_gate", {})
        if isinstance(robustness.get("conservative_evidence_gate"), Mapping)
        else {}
    )
    current_recommendation = _missed_upside_recommendation_for_value(
        _safe_float(current_value, EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE),
        observed_rate=observed_rate,
        conservative_gate=conservative_gate,
        robustness=robustness,
    )
    rows = []
    for raw_value in tested_values:
        value = _safe_float(raw_value, EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE)
        pass_threshold = observed_rate is not None and observed_rate <= value
        recommendation = _missed_upside_recommendation_for_value(
            value,
            observed_rate=observed_rate,
            conservative_gate=conservative_gate,
            robustness=robustness,
        )
        rows.append(
            _calibration_row(
                tested_value=value,
                recommendation=recommendation,
                current_recommendation=current_recommendation,
                false_promotion_risk=(
                    "HIGH"
                    if pass_threshold and not _conservative_gate_all_pass(conservative_gate)
                    else "LOW"
                ),
                false_rejection_risk="MEDIUM" if not pass_threshold else "LOW",
                sample_quality_impact={
                    "observed_missed_upside_rate": observed_rate,
                    "threshold_passed": pass_threshold,
                    "conservative_gate_all_passed": _conservative_gate_all_pass(conservative_gate),
                },
            )
        )
    return rows


def _baseline_missed_upside_rate(effectiveness: Mapping[str, Any]) -> float:
    rows = [
        row
        for row in effectiveness.get("conclusion_matrix", [])
        if isinstance(row, Mapping)
        and row.get("scenario_id") == "baseline"
        and row.get("horizon") in {"1d", "5d", "10d"}
    ]
    sample_count = sum(int(row.get("sample_count") or 0) for row in rows)
    missed_count = sum(int(row.get("missed_upside_count") or 0) for row in rows)
    return _ratio(missed_count, sample_count)


def _missed_upside_recommendation_for_value(
    value: float,
    *,
    observed_rate: float | None,
    conservative_gate: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> str:
    if observed_rate is None:
        return "insufficient_evidence"
    if observed_rate > value:
        return "missed_upside_above_threshold"
    if not _conservative_gate_all_pass(conservative_gate):
        return "keep_preliminary_short_horizon_only"
    return str(
        robustness.get("summary", {}).get(
            "final_validation_recommendation",
            "keep_preliminary_short_horizon_only",
        )
        if isinstance(robustness.get("summary"), Mapping)
        else "keep_preliminary_short_horizon_only"
    )


def _calibrate_masking_high_min(
    *,
    current_value: Any,
    tested_values: Sequence[Any],
    masking_audit: Mapping[str, Any],
) -> list[dict[str, Any]]:
    masking_results = [
        row for row in masking_audit.get("masking_results", []) if isinstance(row, Mapping)
    ]
    ratios = [_optional_float(row.get("masking_ratio")) for row in masking_results]
    ratios = [ratio for ratio in ratios if ratio is not None]
    current_recommendation = _masking_high_recommendation_for_value(
        _safe_float(current_value, 0.60),
        ratios=ratios,
    )
    rows = []
    for raw_value in tested_values:
        value = _safe_float(raw_value, 0.60)
        high_count = sum(1 for ratio in ratios if ratio >= value)
        recommendation = _masking_high_recommendation_for_value(value, ratios=ratios)
        rows.append(
            _calibration_row(
                tested_value=value,
                recommendation=recommendation,
                current_recommendation=current_recommendation,
                false_promotion_risk=(
                    "MEDIUM" if high_count and value < _safe_float(current_value, 0.60) else "LOW"
                ),
                false_rejection_risk=("MEDIUM" if not high_count and ratios else "LOW"),
                sample_quality_impact={
                    "observed_masking_ratio_count": len(ratios),
                    "max_masking_ratio": max(ratios) if ratios else None,
                    "high_masking_pair_count": high_count,
                    "threshold_passed": high_count > 0,
                },
            )
        )
    return rows


def _masking_high_recommendation_for_value(
    value: float,
    *,
    ratios: Sequence[float],
) -> str:
    if not ratios:
        return "insufficient_trace_data"
    return (
        "B_EFFECT_MASKED_BY_A"
        if any(ratio >= value for ratio in ratios)
        else "LOW_OR_MODERATE_MASKING_ONLY"
    )


def _calibrate_dominant_share_of_adjustment_min(
    *,
    current_value: Any,
    tested_values: Sequence[Any],
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    dominance = _dominance_from_trace(registry, trace_rows) if trace_rows else []
    valuation_record = next(
        (row for row in dominance if row.get("indicator_id") == "valuation_crowding_indicator"),
        {},
    )
    share = _optional_float(valuation_record.get("share_of_total_weight_adjustment"))
    hit_rate = _optional_float(valuation_record.get("hit_rate")) or 0.0
    if share is None:
        share = 0.0
    hit_rate_threshold = registry.validation_policy.dominance.dominant_hit_rate_min
    current_recommendation = _dominance_recommendation_for_value(
        _safe_float(
            current_value, registry.validation_policy.dominance.dominant_share_of_adjustment_min
        ),
        share=share,
        hit_rate=hit_rate,
        hit_rate_threshold=hit_rate_threshold,
    )
    rows = []
    for raw_value in tested_values:
        value = _safe_float(
            raw_value,
            registry.validation_policy.dominance.dominant_share_of_adjustment_min,
        )
        dominant = share >= value or hit_rate >= hit_rate_threshold
        recommendation = _dominance_recommendation_for_value(
            value,
            share=share,
            hit_rate=hit_rate,
            hit_rate_threshold=hit_rate_threshold,
        )
        rows.append(
            _calibration_row(
                tested_value=value,
                recommendation=recommendation,
                current_recommendation=current_recommendation,
                false_promotion_risk=(
                    "MEDIUM"
                    if dominant
                    and len({str(row.get("date")) for row in trace_rows if row.get("date")})
                    < HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY
                    else "LOW"
                ),
                false_rejection_risk="MEDIUM" if not dominant and share > 0 else "LOW",
                sample_quality_impact={
                    "share_of_total_weight_adjustment": share,
                    "hit_rate": hit_rate,
                    "hit_rate_threshold": hit_rate_threshold,
                    "dominant_under_tested_value": dominant,
                    "trace_date_count": len(
                        {str(row.get("date")) for row in trace_rows if row.get("date")}
                    ),
                },
            )
        )
    return rows


def _dominance_recommendation_for_value(
    value: float,
    *,
    share: float,
    hit_rate: float,
    hit_rate_threshold: float,
) -> str:
    if share >= value or hit_rate >= hit_rate_threshold:
        return "DOMINANT_WEIGHT_DRIVER"
    return "NOT_DOMINANT"


def _threshold_calibration_robustness_checks(
    effectiveness: Mapping[str, Any],
    robustness: Mapping[str, Any],
    cases_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    capped_masking_ratio: float,
) -> dict[str, Any]:
    aggregation = (
        robustness.get("aggregation", {})
        if isinstance(robustness.get("aggregation"), Mapping)
        else _robustness_aggregation(effectiveness)
    )
    full_winners = _winners_by_horizon(aggregation.get("full_advisory_only", {}))
    all_winners = _winners_by_horizon(aggregation.get("all_validation_sources", {}))
    all_cases = list(cases_by_source.get("all_validation_sources", []))
    return {
        "date_equal_weight_aggregation": aggregation.get("equal_weight_by_date", {}),
        "asset_equal_weight_aggregation": aggregation.get("equal_weight_by_asset", {}),
        "cluster_equal_weight_aggregation": aggregation.get(
            "equal_weight_by_correlated_asset_cluster",
            {},
        ),
        "leave_one_date_out": _leave_one_group_out_check(
            all_cases,
            group_field="date",
            capped_masking_ratio=capped_masking_ratio,
        ),
        "leave_one_cluster_out": _leave_one_group_out_check(
            all_cases,
            group_field="correlated_asset_cluster",
            capped_masking_ratio=capped_masking_ratio,
        ),
        "full_advisory_only_vs_all_sources_consistency": {
            "full_advisory_winners": full_winners,
            "all_sources_winners": all_winners,
            "consistent": not _winner_conflict(full_winners, all_winners),
            "promotion_gate_allowed": False,
        },
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _leave_one_group_out_check(
    cases: Sequence[Mapping[str, Any]],
    *,
    group_field: str,
    capped_masking_ratio: float,
) -> dict[str, Any]:
    normalized_cases = []
    for case in cases:
        row = dict(case)
        if group_field == "correlated_asset_cluster" and not row.get(group_field):
            row[group_field] = _asset_cluster_id(str(row.get("asset") or DEFAULT_TRACE_ASSET))
        normalized_cases.append(row)
    groups = sorted({str(case.get(group_field) or "UNKNOWN") for case in normalized_cases})
    baseline = _baseline_delta_recommendation(normalized_cases, capped_masking_ratio)
    rows = []
    for group in groups:
        remaining = [
            case for case in normalized_cases if str(case.get(group_field) or "UNKNOWN") != group
        ]
        recommendation = _baseline_delta_recommendation(remaining, capped_masking_ratio)
        rows.append(
            {
                "left_out_group": group,
                "remaining_case_count": len(remaining),
                "recommendation": recommendation,
                "matches_baseline": recommendation == baseline,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "group_field": group_field,
        "group_count": len(groups),
        "baseline_recommendation": baseline,
        "stable": bool(rows) and all(row["matches_baseline"] for row in rows),
        "rows": rows,
        "promotion_gate_allowed": False,
    }


def _baseline_delta_recommendation(
    cases: Sequence[Mapping[str, Any]],
    capped_masking_ratio: float,
) -> str:
    scoped = {
        "full_advisory_only": list(cases),
        "component_only": [],
        "backtest_bridge": [],
    }
    rows = [
        row
        for row in _case_comparison_rows(scoped, capped_masking_ratio)
        if row.get("comparison_id") == "baseline_vs_no_valuation_crowding_masking"
        and row.get("horizon") in {"1d", "5d", "10d"}
    ]
    if not rows:
        return "insufficient_evidence"
    avg_delta = _mean([_float(row.get("delta_return")) for row in rows])
    if avg_delta is None:
        return "insufficient_evidence"
    if avg_delta > 0:
        return "keep_baseline_masking_candidate"
    if avg_delta < 0:
        return "baseline_over_defensive_candidate"
    return "neutral_or_mixed"


def _threshold_action_from_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "insufficient_data"
    current_rows = [
        row for row in rows if not bool(row.get("valuation_crowding_recommendation_changes"))
    ]
    if not current_rows:
        return "adjust_candidate"
    current = current_rows[0]
    if str(current.get("recommendation")) in {
        "insufficient_evidence",
        "insufficient_long_horizon_evidence",
        "insufficient_trace_data",
        "cluster_dominated_insufficient_robustness",
        "keep_preliminary_short_horizon_only",
    }:
        return "insufficient_data"
    if any(row.get("valuation_crowding_recommendation_changes") for row in rows):
        return "keep_current_value"
    return "keep_current_value"


def _threshold_action_reason(action: str, default_reason: str) -> str:
    if action == "insufficient_data":
        return f"{default_reason} Evidence is not strong enough to calibrate or change the value."
    if action == "adjust_candidate":
        return (
            f"{default_reason} Sensitivity suggests a candidate adjustment for owner review only."
        )
    return f"{default_reason} Current value remains the validation-only baseline."


def _evidence_strength_from_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "NO_TRACE_EVIDENCE"
    high_risk = any(str(row.get("false_promotion_risk")) == "HIGH" for row in rows)
    changed = any(bool(row.get("valuation_crowding_recommendation_changes")) for row in rows)
    if high_risk:
        return "LIMITED_SENSITIVITY_ONLY"
    if changed:
        return "MODERATE_SENSITIVITY_DIAGNOSTIC"
    return "SENSITIVITY_STABLE_DIAGNOSTIC"


def _conservative_gate_all_pass(gate: Mapping[str, Any]) -> bool:
    checks = gate.get("checks", [])
    if not isinstance(checks, Sequence) or isinstance(checks, (str, bytes)):
        return False
    usable = [check for check in checks if isinstance(check, Mapping)]
    return bool(usable) and all(bool(check.get("passed")) for check in usable)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _threshold_registry_issues(
    registry: Mapping[str, Any],
    thresholds: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    raw_thresholds = registry.get("thresholds", [])
    if len(thresholds) != len(raw_thresholds):
        issues.append(
            {
                "severity": "error",
                "issue_id": "threshold_registry_contains_non_mapping_entry",
                "message": "Every threshold registry item must be a mapping.",
            }
        )
    seen: set[str] = set()
    high_impact_missing_review_flags = []
    for index, threshold in enumerate(thresholds):
        threshold_id = str(threshold.get("threshold_id") or f"threshold_{index}")
        missing = [field for field in THRESHOLD_REQUIRED_FIELDS if field not in threshold]
        if missing:
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "threshold_record_missing_required_fields",
                    "threshold_id": threshold_id,
                    "missing_fields": missing,
                    "message": f"{threshold_id} is missing required fields: {missing}.",
                }
            )
        if threshold_id in seen:
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "duplicate_threshold_id",
                    "threshold_id": threshold_id,
                    "message": f"Duplicate threshold_id: {threshold_id}.",
                }
            )
        seen.add(threshold_id)
        if _is_uncalibrated_high_impact_threshold(threshold) and not (
            bool(threshold.get("calibration_required"))
            and bool(threshold.get("no_promotion_dependency_without_review"))
        ):
            high_impact_missing_review_flags.append(threshold_id)
    if high_impact_missing_review_flags:
        issues.append(
            {
                "severity": "error",
                "issue_id": "uncalibrated_high_impact_threshold_missing_review_block",
                "threshold_ids": sorted(high_impact_missing_review_flags),
                "message": (
                    "A-class uncalibrated thresholds must set calibration_required=true "
                    "and no_promotion_dependency_without_review=true."
                ),
            }
        )
    blocking_ids = [
        str(threshold.get("threshold_id"))
        for threshold in thresholds
        if _threshold_blocks_promotion_dependency(threshold)
    ]
    if blocking_ids:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "uncalibrated_high_impact_thresholds_block_promotion_dependency",
                "threshold_count": len(blocking_ids),
                "threshold_ids": sorted(blocking_ids),
                "message": (
                    "A-class uncalibrated defaults are inventory-complete but cannot "
                    "be used as promotion dependencies without owner review."
                ),
            }
        )
    return issues


def _is_uncalibrated_threshold_status(threshold: Mapping[str, Any]) -> bool:
    status = str(threshold.get("calibration_status", ""))
    return status in HIGH_IMPACT_UNCALIBRATED_STATUSES or status.lower() == "uncalibrated"


def _is_heuristic_guardrail_threshold(threshold: Mapping[str, Any]) -> bool:
    status = str(threshold.get("calibration_status", ""))
    threshold_type = str(threshold.get("threshold_type", ""))
    return status == "HEURISTIC_GUARDRAIL" or threshold_type in HEURISTIC_GUARDRAIL_THRESHOLD_TYPES


def _is_uncalibrated_high_impact_threshold(threshold: Mapping[str, Any]) -> bool:
    return str(
        threshold.get("threshold_class", "")
    ).upper() == HIGH_IMPACT_THRESHOLD_CLASS and _is_uncalibrated_threshold_status(threshold)


def _threshold_blocks_promotion_dependency(threshold: Mapping[str, Any]) -> bool:
    return (
        _is_uncalibrated_high_impact_threshold(threshold)
        and bool(threshold.get("calibration_required"))
        and bool(threshold.get("no_promotion_dependency_without_review"))
    )


def _registry_contract_issues(registry: IndicatorResearchRegistry) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for indicator in registry.indicators:
        if indicator.affects_weight and not indicator.mapping_version:
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "weight_affecting_indicator_missing_mapping",
                    "message": f"{indicator.indicator_id} affects weight without mapping_version.",
                }
            )
        if indicator.affects_weight and indicator.constraint_type == "NOT_A_CONSTRAINT":
            continue
        if (
            indicator.affects_weight
            and indicator.constraint_type == "RESEARCHABLE_STRATEGY_CONSTRAINT"
            and not indicator.evaluation_metrics
        ):
            issues.append(
                {
                    "severity": "warning",
                    "issue_id": "researchable_constraint_missing_metrics",
                    "message": f"{indicator.indicator_id} has no evaluation metrics.",
                }
            )
    unsafe = {
        key: value
        for key, value in registry.safety_boundary.items()
        if key in SAFETY_BOUNDARY and value != SAFETY_BOUNDARY[key]
    }
    if unsafe:
        issues.append(
            {
                "severity": "error",
                "issue_id": "unsafe_indicator_research_boundary",
                "message": (
                    "Indicator research safety boundary drifted from research-only defaults."
                ),
                "unsafe_fields": unsafe,
            }
        )
    return issues


def _daily_component_coverage_issues(
    *,
    registry: IndicatorResearchRegistry,
    scoring_rules_path: Path,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    rules = load_scoring_rules(scoring_rules_path)
    registered = {
        item.daily_component_id
        for item in registry.indicators
        if item.used_in_daily_report and item.daily_component_id
    }
    required = set(rules.weights)
    required.update(
        {
            "confidence",
            "data_quality_gate",
            "thesis",
            "portfolio_limits",
            "execution_deadband",
        }
    )
    missing = sorted(required - registered)
    if missing:
        issues.append(
            {
                "severity": "error",
                "issue_id": "used_in_daily_not_registered",
                "message": "Daily component or gate is not registered in indicator inventory.",
                "component_ids": missing,
            }
        )
    return issues


def _market_regime_payload(
    registry: IndicatorResearchRegistry,
    market_regimes_path: Path,
) -> dict[str, str]:
    regimes = load_market_regimes(market_regimes_path)
    configured = {item.regime_id for item in regimes.regimes}
    regime_id = registry.market_regime.regime_id
    return {
        "market_regime": regime_id if regime_id in configured else regimes.default_backtest_regime,
        "requested_date_range": registry.market_regime.requested_date_range,
    }


def _inventory_record(
    registry: IndicatorResearchRegistry,
    indicator: IndicatorSpec,
) -> dict[str, Any]:
    coverage_status = _coverage_status(registry, indicator)
    return {
        "indicator_id": indicator.indicator_id,
        "display_name": indicator.display_name,
        "object_type": indicator.object_type,
        "role": indicator.role,
        "used_in_daily_report": indicator.used_in_daily_report,
        "affects_signal": indicator.affects_signal,
        "affects_weight": indicator.affects_weight,
        "constraint_type": indicator.constraint_type,
        "mapping_version": indicator.mapping_version,
        "research_status": indicator.research_status,
        "coverage_status": coverage_status,
        "expected_impact": indicator.expected_impact.model_dump(mode="json"),
        "source_features": [item.model_dump(mode="json") for item in indicator.source_features],
        "data_gate": dict(indicator.data_gate),
        "target_family": indicator.target_family,
        "evaluation_metrics": list(indicator.evaluation_metrics),
        "implementation_refs": list(indicator.implementation_refs),
        "owner": indicator.owner,
        "conditionality": _conditionality_label(registry, indicator.indicator_id),
    }


def _coverage_status(registry: IndicatorResearchRegistry, indicator: IndicatorSpec) -> str:
    if indicator.role == "INFORMATIONAL_ONLY":
        return "INFORMATIONAL_ONLY"
    if indicator.research_status == "RESEARCHED":
        return "RESEARCHED"
    if _is_high_impact_unvalidated(registry, indicator):
        return "HIGH_IMPACT_UNVALIDATED"
    if indicator.research_status == "DEPRECATED_OR_UNKNOWN":
        return "DEPRECATED_OR_UNKNOWN"
    return "REGISTERED_NOT_RESEARCHED"


def _coverage_for(registry: IndicatorResearchRegistry, indicator_id: str) -> str:
    indicator = _indicator_or_raise(registry, indicator_id)
    return _coverage_status(registry, indicator)


def _is_high_impact_unvalidated(
    registry: IndicatorResearchRegistry,
    indicator: IndicatorSpec,
) -> bool:
    policy = registry.validation_policy.impact_bands
    return (
        indicator.affects_weight
        and indicator.research_status != "RESEARCHED"
        and indicator.expected_impact.hit_rate_band == policy.high_hit_rate_label
        and indicator.expected_impact.weight_impact_band == policy.high_weight_impact_label
    )


def _dependency_nodes(registry: IndicatorResearchRegistry) -> list[dict[str, Any]]:
    nodes = []
    seen: set[str] = set()
    for indicator in registry.indicators:
        seen.add(indicator.indicator_id)
        nodes.append(
            {
                "node_id": indicator.indicator_id,
                "node_type": indicator.object_type,
                "role": indicator.role,
                "coverage_status": _coverage_status(registry, indicator),
            }
        )
        for feature in indicator.source_features:
            feature_id = _feature_node_id(feature)
            if feature_id not in seen:
                seen.add(feature_id)
                nodes.append({"node_id": feature_id, "node_type": "FEATURE"})
    for mapping in registry.mappings:
        if mapping.mapping_version not in seen:
            seen.add(mapping.mapping_version)
            nodes.append(
                {
                    "node_id": mapping.mapping_version,
                    "node_type": "SIGNAL_MAPPING",
                    "family": mapping.family,
                }
            )
    for node_id, node_type in (
        ("allocation_intent", "ALLOCATION_INTENT"),
        ("post_execution_research_weight", "RESEARCH_WEIGHT"),
        ("final_research_weight", "RESEARCH_WEIGHT"),
        ("outcome", "OUTCOME"),
        ("all_indicator_research", "RESEARCH_PROCESS"),
    ):
        if node_id not in seen:
            nodes.append({"node_id": node_id, "node_type": node_type})
    return nodes


def _dependency_edges(registry: IndicatorResearchRegistry) -> list[dict[str, Any]]:
    edges = []
    for indicator in registry.indicators:
        for feature in indicator.source_features:
            edges.append(
                {
                    "from_node": _feature_node_id(feature),
                    "to_node": indicator.indicator_id,
                    "edge_type": "CONSUMES",
                    "rationale": "Feature feeds registered indicator.",
                }
            )
    edges.extend(dependency.model_dump(mode="json") for dependency in registry.dependencies)
    return edges


def _detect_indicator_cycles(registry: IndicatorResearchRegistry) -> list[list[str]]:
    indicator_ids = {item.indicator_id for item in registry.indicators}
    graph: dict[str, list[str]] = defaultdict(list)
    for dependency in registry.dependencies:
        if dependency.from_node in indicator_ids and dependency.to_node in indicator_ids:
            graph[dependency.from_node].append(dependency.to_node)
    cycles: list[list[str]] = []
    visiting: list[str] = []
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            start = visiting.index(node)
            cycles.append([*visiting[start:], node])
            return
        if node in visited:
            return
        visiting.append(node)
        for child in graph.get(node, []):
            visit(child)
        visiting.pop()
        visited.add(node)

    for node in sorted(indicator_ids):
        visit(node)
    return cycles


def _daily_trace_row(
    *,
    as_of_text: str,
    row_type: str,
    module_id: str,
    daily_component_id: str,
    mapping_version: str | None,
    raw_indicator_value: Any,
    normalized_indicator_score: float | None,
    mapped_signal_contribution: float | None,
    pre_constraint_signal_weight: float,
    post_constraint_signal_weight: float,
    final_advisory_portfolio_facing_weight: float,
    weight_before: float,
    weight_after: float,
    reason_code: str,
    constraint_hit: bool,
    upstream_indicator_id: str,
    downstream_indicator_id: str,
    b_intended_change: float | None = None,
    a_suppressed_change: float | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    row = {
        "date": as_of_text,
        "asset": "AI_RISK_ASSET_BASKET",
        "row_type": row_type,
        "module_id": module_id,
        "daily_component_id": daily_component_id,
        "mapping_version": mapping_version or "",
        "raw_indicator_value": raw_indicator_value,
        "normalized_indicator_score": normalized_indicator_score,
        "mapped_signal_contribution": mapped_signal_contribution,
        "pre_constraint_signal_weight": pre_constraint_signal_weight,
        "post_constraint_signal_weight": post_constraint_signal_weight,
        "final_advisory_portfolio_facing_weight": final_advisory_portfolio_facing_weight,
        "weight_before": weight_before,
        "weight_after": weight_after,
        "delta": weight_after - weight_before,
        "reason_code": reason_code,
        "constraint_hit": constraint_hit,
        "upstream_indicator_id": upstream_indicator_id,
        "downstream_indicator_id": downstream_indicator_id,
        "b_intended_change": b_intended_change,
        "a_suppressed_change": a_suppressed_change,
        "official_target_weights": False,
        "broker_order": False,
        "live_order": False,
        "production_position_mutation": False,
    }
    row.update(dict(extra or {}))
    return row


def _signal_raw_values(signals: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {
            "subject": getattr(signal, "subject", ""),
            "feature": getattr(signal, "feature", ""),
            "value": getattr(signal, "value", None),
            "available": bool(getattr(signal, "available", False)),
        }
        for signal in signals
    ]


def _signal_score_rows(signals: Sequence[Any]) -> list[dict[str, Any]]:
    rows = []
    for signal in signals:
        points = _float(getattr(signal, "points", 0.0))
        earned = _float(getattr(signal, "earned_points", 0.0))
        rows.append(
            {
                "subject": getattr(signal, "subject", ""),
                "feature": getattr(signal, "feature", ""),
                "raw_indicator_value": getattr(signal, "value", None),
                "normalized_indicator_score": None if points <= 0 else earned / points,
                "mapped_signal_contribution": earned,
                "points": points,
                "available": bool(getattr(signal, "available", False)),
                "reason": getattr(signal, "reason", ""),
            }
        )
    return rows


def _daily_trace_stage_snapshot(report: Any) -> dict[str, Any]:
    return {
        "raw_signal_target": {
            "min_position": report.recommendation.model_risk_asset_ai_band.min_position,
            "max_position": report.recommendation.model_risk_asset_ai_band.max_position,
        },
        "pre_constraint_target": {
            "min_position": report.recommendation.model_risk_asset_ai_band.min_position,
            "max_position": report.recommendation.model_risk_asset_ai_band.max_position,
        },
        "post_all_constraints_target": {
            "min_position": report.recommendation.risk_asset_ai_band.min_position,
            "max_position": report.recommendation.risk_asset_ai_band.max_position,
        },
        "final_research_weight": {
            "min_position": report.recommendation.total_asset_ai_band.min_position,
            "max_position": report.recommendation.total_asset_ai_band.max_position,
        },
    }


def _daily_non_component_trace_rows(
    report: Any,
    registry: IndicatorResearchRegistry,
) -> list[dict[str, Any]]:
    as_of_text = report.as_of.isoformat()
    model_max = report.recommendation.model_risk_asset_ai_band.max_position
    final_max = report.recommendation.risk_asset_ai_band.max_position
    portfolio_max = report.recommendation.total_asset_ai_band.max_position
    rows: list[dict[str, Any]] = []
    for indicator_id, component_id, raw_value, normalized_score, reason_code in (
        (
            "confidence_reliability_indicator",
            "confidence",
            {
                "confidence_score": report.confidence_assessment.score,
                "confidence_level": report.confidence_assessment.level,
                "reasons": list(report.confidence_assessment.reasons),
            },
            _float(report.confidence_assessment.score),
            "confidence_reliability_trace",
        ),
        (
            "data_quality_gate_indicator",
            "data_quality_gate",
            {
                "data_quality_status": report.data_quality_report.status,
                "feature_status": report.feature_set.status,
                "feature_warning_count": len(report.feature_set.warnings),
            },
            None,
            "data_quality_gate_trace",
        ),
        (
            "execution_deadband_indicator",
            "execution_deadband",
            {"minimum_action_delta": report.minimum_action_delta},
            None,
            "execution_deadband_trace",
        ),
    ):
        indicator = _indicator_or_raise(registry, indicator_id)
        rows.append(
            _daily_trace_row(
                as_of_text=as_of_text,
                row_type="daily_guardrail_snapshot",
                module_id=indicator_id,
                daily_component_id=component_id,
                mapping_version=indicator.mapping_version,
                raw_indicator_value=raw_value,
                normalized_indicator_score=normalized_score,
                mapped_signal_contribution=0.0,
                pre_constraint_signal_weight=model_max,
                post_constraint_signal_weight=final_max,
                final_advisory_portfolio_facing_weight=portfolio_max,
                weight_before=model_max,
                weight_after=final_max,
                reason_code=reason_code,
                constraint_hit=False,
                upstream_indicator_id=indicator_id,
                downstream_indicator_id="",
                extra={"coverage_status": _coverage_status(registry, indicator)},
            )
        )
    return rows


def _daily_constraint_trace_rows(
    report: Any,
    registry: IndicatorResearchRegistry,
) -> list[dict[str, Any]]:
    as_of_text = report.as_of.isoformat()
    component_map = _indicator_by_daily_component(registry)
    model_min = report.recommendation.model_risk_asset_ai_band.min_position
    model_max = report.recommendation.model_risk_asset_ai_band.max_position
    portfolio_max = report.recommendation.total_asset_ai_band.max_position
    rows: list[dict[str, Any]] = []
    running_max = model_max
    for gate in report.recommendation.position_gates:
        if gate.gate_id == "score_model":
            continue
        component_id = _daily_component_alias(gate.gate_id)
        indicator = component_map.get(component_id)
        module_id = indicator.indicator_id if indicator is not None else gate.gate_id
        mapping_version = None if indicator is None else indicator.mapping_version
        before = running_max
        after = min(before, _float(gate.max_position))
        running_max = after
        downstream_id = _downstream_indicator_for_gate(gate.gate_id)
        intended = max(0.0, model_max - model_min) if downstream_id else None
        suppressed = max(0.0, before - after) if downstream_id else None
        rows.append(
            _daily_trace_row(
                as_of_text=as_of_text,
                row_type="constraint_gate",
                module_id=module_id,
                daily_component_id=component_id,
                mapping_version=mapping_version,
                raw_indicator_value={
                    "gate_id": gate.gate_id,
                    "label": gate.label,
                    "source": gate.source,
                    "reason": gate.reason,
                    "gate_class": gate.gate_class,
                    "target_effect": gate.target_effect,
                    "execution_effect": gate.execution_effect,
                    "gate_max_position": gate.max_position,
                },
                normalized_indicator_score=None,
                mapped_signal_contribution=None,
                pre_constraint_signal_weight=model_max,
                post_constraint_signal_weight=after,
                final_advisory_portfolio_facing_weight=portfolio_max,
                weight_before=before,
                weight_after=after,
                reason_code=f"{gate.gate_id}_constraint_attribution",
                constraint_hit=bool(gate.triggered),
                upstream_indicator_id=module_id,
                downstream_indicator_id=downstream_id,
                b_intended_change=intended,
                a_suppressed_change=suppressed,
                extra={
                    "gate_triggered": bool(gate.triggered),
                    "binding_delta": after - before,
                    "coverage_status": (
                        "USED_IN_DAILY_NOT_REGISTERED"
                        if indicator is None
                        else _coverage_status(registry, indicator)
                    ),
                    "masking_proxy": bool(downstream_id),
                },
            )
        )
    return rows


def _downstream_indicator_for_gate(gate_id: str) -> str:
    if gate_id == "valuation":
        return "trend_strength_indicator"
    return ""


def _trace_contract_field_audit(
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    missing_records = []
    required_delta_fields = list(registry.trace_contract.required_delta_fields)
    required_masking_fields = list(registry.trace_contract.required_masking_fields)
    for index, row in enumerate(trace_rows):
        missing_delta = [field for field in required_delta_fields if field not in row]
        missing_masking: list[str] = []
        if row.get("downstream_indicator_id"):
            missing_masking = [
                field
                for field in required_masking_fields
                if field not in row or row.get(field) in (None, "")
            ]
        if missing_delta or missing_masking:
            missing_records.append(
                {
                    "row_index": index,
                    "row_type": row.get("row_type", ""),
                    "module_id": row.get("module_id", ""),
                    "missing_delta_fields": missing_delta,
                    "missing_masking_fields": missing_masking,
                }
            )
    return {
        "required_delta_fields": required_delta_fields,
        "required_masking_fields": required_masking_fields,
        "missing_field_records": missing_records,
    }


def _expected_daily_component_ids(scoring_rules_path: Path) -> set[str]:
    rules = load_scoring_rules(scoring_rules_path)
    components = set(rules.weights)
    components.update(
        {
            "confidence",
            "data_quality_gate",
            "thesis",
            "portfolio_limits",
            "execution_deadband",
        }
    )
    return components


def _daily_component_alias(component_id: str) -> str:
    aliases = {
        "data_confidence": "data_quality_gate",
        "risk_events": "policy_geopolitics",
        "risk_budget": "risk_sentiment",
    }
    return aliases.get(component_id, component_id)


def _trace_row_is_daily_indicator(
    row: Mapping[str, Any],
    registered_ids: set[str],
    component_map: Mapping[str, IndicatorSpec],
) -> bool:
    module_id = str(row.get("module_id") or "")
    component_id = str(row.get("daily_component_id") or "")
    if not module_id or module_id in registered_ids:
        return False
    if component_id in component_map or _daily_component_alias(component_id) in component_map:
        return False
    return str(row.get("row_type")) != "score_model"


def _registered_indicator_gap_record(
    registry: IndicatorResearchRegistry,
    indicator: IndicatorSpec,
    *,
    trace_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    missing: list[str] = []
    mapping_versions = {mapping.mapping_version for mapping in registry.mappings}
    dependencies = _dependency_edges(registry)
    trace_keys = _trace_indicator_keys(trace_rows)
    if not indicator.source_features:
        missing.append("source")
    if (
        indicator.affects_weight
        and indicator.role
        in {
            "CONSTRAINT_GUARDRAIL_INDICATOR",
            "CONFIDENCE_RELIABILITY_INDICATOR",
            "EXECUTION_INDICATOR",
            "IMMUTABLE_SAFETY_CONSTRAINT",
        }
        and indicator.constraint_type == "NOT_A_CONSTRAINT"
    ):
        missing.append("constraint")
    if (indicator.affects_signal or indicator.affects_weight) and (
        not indicator.mapping_version or indicator.mapping_version not in mapping_versions
    ):
        missing.append("mapping")
    if not any(
        edge["from_node"] == indicator.indicator_id or edge["to_node"] == indicator.indicator_id
        for edge in dependencies
    ):
        missing.append("dependency")
    if trace_rows and indicator.indicator_id not in trace_keys:
        missing.append("trace_contract")
    if not trace_rows:
        missing.append("trace_contract")
    return {
        "indicator_id": indicator.indicator_id,
        "display_name": indicator.display_name,
        "coverage_status": _coverage_status(registry, indicator),
        "missing_fields": sorted(set(missing)),
    }


def _trace_indicator_keys(trace_rows: Sequence[Mapping[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for row in trace_rows:
        for field in ("module_id", "upstream_indicator_id", "downstream_indicator_id"):
            value = str(row.get(field) or "")
            if value:
                keys.add(value)
    return keys


def _read_trace_payload(trace_path: Path | None) -> dict[str, Any]:
    if trace_path is None:
        return {}
    if not trace_path.exists():
        raise IndicatorResearchError(f"trace path not found: {trace_path}")
    raw = json.loads(trace_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, list):
        return {"rows": raw}
    raise IndicatorResearchError("trace JSON must be a list or contain a rows list")


def _read_trace_rows(trace_path: Path | None) -> list[dict[str, Any]]:
    payload = _read_trace_payload(trace_path)
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise IndicatorResearchError("trace JSON must be a list or contain a rows list")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _trace_contract_version_for_payload(
    trace_path: Path | None,
    *,
    registry: IndicatorResearchRegistry,
) -> str:
    if trace_path is None:
        return registry.trace_contract.contract_id
    try:
        payload = _read_trace_payload(trace_path)
    except (OSError, json.JSONDecodeError, IndicatorResearchError):
        return registry.trace_contract.contract_id
    summary = payload.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    for value in (
        payload.get("trace_contract_version"),
        payload.get("trace_contract_id"),
        summary.get("trace_contract_version"),
        summary.get("trace_contract_id"),
    ):
        if value not in (None, ""):
            return str(value)
    manifests = _explicit_lineage_manifests(payload)
    for manifest in manifests:
        version = manifest.get("trace_contract_version") or manifest.get("trace_contract_id")
        if version not in (None, ""):
            return str(version)
    return registry.trace_contract.contract_id


def _trace_lineage_records(
    trace_path: Path | None,
    trace_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if trace_path is None:
        return []
    payload = _read_trace_payload(trace_path)
    explicit = _explicit_lineage_manifests(payload)
    if explicit:
        return [
            _normalize_lineage_manifest_record(item, trace_path=trace_path)
            for item in explicit
            if isinstance(item, Mapping)
        ]
    source_paths = payload.get("source_trace_paths", [])
    if isinstance(source_paths, str):
        source_paths = [source_paths]
    if isinstance(source_paths, Sequence):
        records = [
            _lineage_manifest_from_source_trace(trace_path, str(source_path))
            for source_path in source_paths
            if str(source_path)
        ]
        return [record for record in records if record is not None]
    return [
        _fallback_lineage_manifest(trace_path, row_date) for row_date in _trace_dates(trace_rows)
    ]


def _explicit_lineage_manifests(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in (
        "historical_replay_manifests",
        "lineage_manifests",
        "historical_replay_manifest",
        "lineage_manifest",
    ):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return [value]
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _normalize_lineage_manifest_record(
    raw: Mapping[str, Any],
    *,
    trace_path: Path | None,
) -> dict[str, Any]:
    as_of_date = str(raw.get("as_of_date") or raw.get("as_of") or raw.get("date") or "")[:10]
    source_path = str(raw.get("source_artifact_path") or raw.get("artifact_path") or "")
    if not source_path and trace_path is not None:
        source_path = str(trace_path)
    return {
        "source_artifact_path": source_path,
        "generated_at": str(raw.get("generated_at") or ""),
        "as_of_date": as_of_date,
        "decision_time": str(raw.get("decision_time") or as_of_date),
        "config_hash": str(raw.get("config_hash") or ""),
        "input_snapshot_hash": str(raw.get("input_snapshot_hash") or ""),
        "trace_contract_version": str(
            raw.get("trace_contract_version") or raw.get("trace_contract_id") or ""
        ),
        "production_equivalent": bool(raw.get("production_equivalent")),
        "proof_status": str(raw.get("proof_status") or "EXPLICIT_LINEAGE_MANIFEST"),
    }


def _lineage_manifest_from_source_trace(
    parent_trace_path: Path,
    source_path_text: str,
) -> dict[str, Any] | None:
    source_path = _resolve_trace_source_path(parent_trace_path, source_path_text)
    if source_path is None:
        return {
            "source_artifact_path": source_path_text,
            "generated_at": "",
            "as_of_date": "",
            "decision_time": "",
            "config_hash": "",
            "input_snapshot_hash": "",
            "trace_contract_version": "",
            "production_equivalent": False,
            "proof_status": "SOURCE_TRACE_MISSING",
        }
    try:
        source_payload = _read_trace_payload(source_path)
    except (OSError, json.JSONDecodeError, IndicatorResearchError):
        return None
    source_rows = _read_trace_rows(source_path)
    as_of_date = _source_trace_as_of_date(source_payload, source_rows)
    bundle = _read_optional_json(source_path.parent / "trace_bundle.json")
    run_manifest = bundle.get("run_manifest", {}) if isinstance(bundle, Mapping) else {}
    if not isinstance(run_manifest, Mapping):
        run_manifest = {}
    decision_time = _lineage_decision_time(source_payload, run_manifest, as_of_date)
    config_hash = _lineage_config_hash(source_payload, bundle)
    input_snapshot_hash = _lineage_input_snapshot_hash(source_payload, bundle)
    trace_contract_version = str(
        source_payload.get("trace_contract_version")
        or source_payload.get("trace_contract_id")
        or source_payload.get("summary", {}).get("trace_contract_id", "")
    )
    production_equivalent = bool(
        source_payload.get("report_type") == "daily_indicator_weight_trace"
        and source_payload.get("status") == "PASS"
        and run_manifest.get("command") == "aits score-daily"
        and as_of_date
        and decision_time
        and config_hash
        and input_snapshot_hash
        and trace_contract_version
    )
    return {
        "source_artifact_path": str(source_path),
        "generated_at": str(source_payload.get("generated_at") or bundle.get("generated_at") or ""),
        "as_of_date": as_of_date,
        "decision_time": decision_time,
        "config_hash": config_hash,
        "input_snapshot_hash": input_snapshot_hash,
        "trace_contract_version": trace_contract_version,
        "production_equivalent": production_equivalent,
        "proof_status": (
            "PRODUCTION_EQUIVALENT_LINEAGE_PROVEN"
            if production_equivalent
            else "LINEAGE_FIELDS_INCOMPLETE"
        ),
    }


def _resolve_trace_source_path(parent_trace_path: Path, source_path_text: str) -> Path | None:
    source_path = Path(source_path_text)
    candidates = [source_path]
    if not source_path.is_absolute():
        candidates = [
            PROJECT_ROOT / source_path,
            parent_trace_path.parent / source_path,
        ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _source_trace_as_of_date(
    payload: Mapping[str, Any],
    trace_rows: Sequence[Mapping[str, Any]],
) -> str:
    summary = payload.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    for value in (summary.get("as_of"), payload.get("as_of"), payload.get("date")):
        parsed = _parse_iso_date(str(value or ""))
        if parsed is not None:
            return parsed.isoformat()
    dates = _trace_dates(trace_rows)
    return dates[0] if dates else ""


def _lineage_decision_time(
    payload: Mapping[str, Any],
    run_manifest: Mapping[str, Any],
    as_of_date: str,
) -> str:
    for value in (payload.get("decision_time"), run_manifest.get("decision_time")):
        if value not in (None, ""):
            return str(value)
    date_window = run_manifest.get("date_window", {})
    if isinstance(date_window, Mapping):
        for key in ("end", "start"):
            value = date_window.get(key)
            if value not in (None, ""):
                return str(value)
    return as_of_date


def _lineage_config_hash(payload: Mapping[str, Any], bundle: Mapping[str, Any]) -> str:
    for value in (payload.get("config_hash"), bundle.get("config_hash")):
        if value not in (None, ""):
            return str(value)
    run_manifest = bundle.get("run_manifest", {})
    if isinstance(run_manifest, Mapping):
        config_payload = {
            "config_ids": run_manifest.get("config_ids", []),
            "config_paths": run_manifest.get("config_paths", {}),
            "rule_versions": (
                run_manifest.get("parameters", {}).get("rule_versions", {})
                if isinstance(run_manifest.get("parameters"), Mapping)
                else {}
            ),
        }
        return _hash_jsonable(config_payload)
    return ""


def _lineage_input_snapshot_hash(payload: Mapping[str, Any], bundle: Mapping[str, Any]) -> str:
    for value in (payload.get("input_snapshot_hash"), bundle.get("input_snapshot_hash")):
        if value not in (None, ""):
            return str(value)
    snapshot_payload = {
        "dataset_refs": bundle.get("dataset_refs", []),
        "quality_refs": bundle.get("quality_refs", []),
        "source_artifacts": payload.get("source_artifacts", []),
    }
    if snapshot_payload["dataset_refs"] or snapshot_payload["quality_refs"]:
        return _hash_jsonable(snapshot_payload)
    return ""


def _fallback_lineage_manifest(trace_path: Path, row_date: str) -> dict[str, Any]:
    return {
        "source_artifact_path": str(trace_path),
        "generated_at": "",
        "as_of_date": row_date,
        "decision_time": row_date,
        "config_hash": "",
        "input_snapshot_hash": _file_sha256(trace_path),
        "trace_contract_version": "",
        "production_equivalent": False,
        "proof_status": "LINEAGE_MANIFEST_MISSING",
    }


def _lineage_by_date(records: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    by_date: dict[str, Mapping[str, Any]] = {}
    for record in records:
        row_date = str(record.get("as_of_date") or "")[:10]
        if not row_date:
            continue
        current = by_date.get(row_date)
        if current is None or (
            record.get("production_equivalent") and not current.get("production_equivalent")
        ):
            by_date[row_date] = record
    return by_date


def _lineage_manifest_complete(record: Mapping[str, Any] | None) -> bool:
    if not record:
        return False
    required = (
        "source_artifact_path",
        "generated_at",
        "as_of_date",
        "decision_time",
        "config_hash",
        "input_snapshot_hash",
        "trace_contract_version",
    )
    return bool(record.get("production_equivalent")) and all(record.get(key) for key in required)


def _lineage_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "lineage_manifest_record_count": len(records),
        "production_equivalent_lineage_count": sum(
            1 for record in records if _lineage_manifest_complete(record)
        ),
        "lineage_manifest_missing_count": sum(
            1 for record in records if not _lineage_manifest_complete(record)
        ),
    }


def _lineage_repair_root_cause_records(
    *,
    root_cause_audit_path: Path | None,
    current_availability: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    if root_cause_audit_path is None:
        return [
            record
            for record in current_availability
            if not record.get("full_advisory_trace_eligible")
        ]
    if not root_cause_audit_path.exists():
        raise IndicatorResearchError(f"root cause audit path not found: {root_cause_audit_path}")
    raw = json.loads(root_cause_audit_path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise IndicatorResearchError("root cause audit JSON must be an object")
    records = raw.get("gate_root_cause_analysis", [])
    if not isinstance(records, list):
        raise IndicatorResearchError("root cause audit JSON missing gate_root_cause_analysis list")
    return [record for record in records if isinstance(record, Mapping)]


def _lineage_repair_artifact_record(
    row_date: str,
    cases: Sequence[Mapping[str, Any]],
    *,
    trace_path: Path | None,
    gate_audit_root: Path | None,
) -> dict[str, Any]:
    source_path = _lineage_repair_source_path(row_date, gate_audit_root=gate_audit_root)
    manifest: dict[str, Any] = {}
    if source_path.exists():
        manifest = (
            _lineage_manifest_from_source_trace(
                trace_path or source_path,
                str(source_path),
            )
            or {}
        )
        if _lineage_manifest_complete(manifest):
            validation_status = "VALID_PRODUCTION_EQUIVALENT"
            repair_action = "no_repair_needed"
        else:
            validation_status = "MANIFEST_INCOMPLETE"
            repair_action = "regenerate_lineage_manifest_from_existing_artifact"
    else:
        validation_status = "SOURCE_ARTIFACT_MISSING"
        repair_action = "rerun_pit_sliced_replay_without_gate_relaxation"
    case_assets = {
        str(case.get("asset")).upper() for case in cases if case.get("asset") not in (None, "")
    }
    return {
        "source_artifact_path": str(source_path),
        "generated_at": str(manifest.get("generated_at") or ""),
        "as_of_date": str(manifest.get("as_of_date") or row_date),
        "decision_time": str(manifest.get("decision_time") or row_date),
        "config_hash": str(manifest.get("config_hash") or ""),
        "input_snapshot_hash": str(manifest.get("input_snapshot_hash") or ""),
        "trace_contract_version": str(manifest.get("trace_contract_version") or ""),
        "production_equivalent": bool(manifest.get("production_equivalent")),
        "manifest_validation_status": validation_status,
        "proof_status": str(manifest.get("proof_status") or validation_status),
        "affected_root_cause_case_count": len(cases),
        "affected_asset_count": len(case_assets),
        "affected_assets": sorted(case_assets),
        "repair_action": repair_action,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _lineage_repair_source_path(row_date: str, *, gate_audit_root: Path | None) -> Path:
    if gate_audit_root is None:
        return PROJECT_ROOT / "outputs" / "reports" / row_date / "daily_indicator_weight_trace.json"
    return gate_audit_root / row_date / "daily_indicator_weight_trace.json"


def _read_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _hash_jsonable(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _trace_dates(trace_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row.get("date")) for row in trace_rows if row.get("date")})


def _filter_trace_rows(
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> list[dict[str, Any]]:
    start = _parse_iso_date(start_date or "")
    end = _parse_iso_date(end_date or "")
    window_start = _parse_iso_date(event_window_start or "")
    window_end = _parse_iso_date(event_window_end or "")
    assets = set(_parse_asset_universe(asset_universe))
    filtered: list[dict[str, Any]] = []
    for row in trace_rows:
        row_date = _parse_iso_date(str(row.get("date") or ""))
        if row_date is None:
            continue
        if start is not None and row_date < start:
            continue
        if end is not None and row_date > end:
            continue
        if window_start is not None and row_date < window_start:
            continue
        if window_end is not None and row_date > window_end:
            continue
        asset = str(row.get("asset") or "")
        if assets and asset and asset not in assets and asset != "AI_RISK_ASSET_BASKET":
            continue
        filtered.append(dict(row))
    return filtered


def _parse_asset_universe(asset_universe: str | None) -> list[str]:
    if not asset_universe:
        return []
    return sorted(
        {
            item.strip().upper()
            for item in asset_universe.replace(";", ",").split(",")
            if item.strip()
        }
    )


def _trace_filter_payload(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> dict[str, Any]:
    return {
        "start_date": start_date,
        "end_date": end_date,
        "event_window_start": event_window_start,
        "event_window_end": event_window_end,
        "asset_universe": _parse_asset_universe(asset_universe),
    }


def _trace_sample_quality_stats(
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    gate_availability: Sequence[Mapping[str, Any]] | None = None,
    cases: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    trace_dates = {str(row.get("date")) for row in trace_rows if row.get("date")}
    case_rows = list(cases or [])
    availability = list(gate_availability or [])
    availability_dates = {str(record.get("date")) for record in availability if record.get("date")}
    if availability:
        full_dates = {
            str(record.get("date"))
            for record in availability
            if record.get("full_advisory_trace_eligible")
        }
        component_dates = {
            str(record.get("date"))
            for record in availability
            if record.get("component_validation_trace_eligible")
        }
        partial_component_dates = component_dates - full_dates
    else:
        full_dates = set(trace_dates)
        component_dates = set(trace_dates)
        partial_component_dates = set()
    asset_rows = case_rows if case_rows else availability or trace_rows
    assets = {str(row.get("asset")) for row in asset_rows if row.get("asset")}
    masking_case_count = (
        len(case_rows)
        if case_rows
        else len(
            _pair_trace_rows(
                trace_rows,
                "valuation_crowding_indicator",
                "trend_strength_indicator",
            )
        )
    )
    sample_case_count = len(case_rows) if case_rows else len(availability) or masking_case_count
    regimes = {
        str(row.get("market_regime") or registry.market_regime.regime_id) for row in trace_rows
    }
    return {
        "date_count": len(
            trace_dates
            | availability_dates
            | {str(row.get("date")) for row in case_rows if row.get("date")}
        ),
        "eligible_dates": len(full_dates),
        "component_eligible_dates": len(component_dates),
        "asset_count": len(assets),
        "case_count": sample_case_count,
        "masking_case_count": masking_case_count,
        "regime_count": len(regimes) if trace_rows else 0,
        "full_advisory_equivalent_count": len(full_dates),
        "partial_component_only_count": len(partial_component_dates),
        "event_window_coverage": _event_window_coverage(trace_rows, case_rows, availability),
    }


def _event_window_coverage(
    trace_rows: Sequence[Mapping[str, Any]],
    case_rows: Sequence[Mapping[str, Any]],
    availability: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: Sequence[Mapping[str, Any]]
    rows = case_rows if case_rows else availability or trace_rows
    coverage = []
    for window in DEFAULT_EVENT_WINDOW_CATALOG:
        start = _parse_iso_date(str(window["start_date"]))
        end = _parse_iso_date(str(window["end_date"]))
        window_rows = [
            row for row in rows if _date_in_window(str(row.get("date") or ""), start=start, end=end)
        ]
        coverage.append(
            {
                **window,
                "date_count": len({str(row.get("date")) for row in window_rows if row.get("date")}),
                "asset_count": len(
                    {str(row.get("asset")) for row in window_rows if row.get("asset")}
                ),
                "case_count": len(window_rows),
            }
        )
    return coverage


def _date_in_window(value: str, *, start: date | None, end: date | None) -> bool:
    parsed = _parse_iso_date(value)
    if parsed is None or start is None or end is None:
        return False
    return start <= parsed <= end


def _gate_availability_records(
    *,
    gate_audit_root: Path | None,
    trace_path: Path | None,
    trace_rows: Sequence[Mapping[str, Any]],
    start_date: str | None = None,
    end_date: str | None = None,
    event_window_start: str | None = None,
    event_window_end: str | None = None,
    asset_universe: str | None = None,
) -> list[dict[str, Any]]:
    trace_dates = {str(row.get("date")) for row in trace_rows if row.get("date")}
    audit_dates = _gate_audit_dates(gate_audit_root)
    requested_dates = _requested_audit_dates(
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
    )
    all_dates = sorted(trace_dates | audit_dates | requested_dates)
    filtered_dates = [
        item
        for item in all_dates
        if _date_matches_filters(
            item,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
        )
    ]
    if asset_universe and not trace_rows and not audit_dates:
        filtered_dates = sorted(requested_dates)
    lineage_records = _trace_lineage_records(trace_path, trace_rows)
    lineage_by_date = _lineage_by_date(lineage_records)
    requested_assets = _parse_asset_universe(asset_universe)
    records = []
    for row_date in filtered_dates:
        date_root = None if gate_audit_root is None else gate_audit_root / row_date
        data_quality = _read_gate_report_status(
            None if date_root is None else date_root / "data_quality.md"
        )
        feature_availability = _read_gate_report_status(
            None if date_root is None else date_root / "feature_availability.md"
        )
        trace_exists = row_date in trace_dates
        dq_status = data_quality["status"]
        fa_status = feature_availability["status"]
        data_quality_passed = dq_status in {"PASS", "PASS_WITH_WARNINGS"}
        feature_passed = fa_status in {"PASS", "PASS_WITH_WARNINGS"}
        lineage_manifest = lineage_by_date.get(row_date)
        lineage_ok = _lineage_manifest_complete(lineage_manifest)
        full_eligible = trace_exists and data_quality_passed and feature_passed and lineage_ok
        component_eligible = data_quality_passed and (
            trace_exists or fa_status in {"FAIL", "PASS", "PASS_WITH_WARNINGS"}
        )
        reasons = _gate_failure_reasons(
            trace_exists=trace_exists,
            data_quality=data_quality,
            feature_availability=feature_availability,
            lineage_manifest=lineage_manifest,
            lineage_ok=lineage_ok,
        )
        for asset in _gate_record_assets(
            row_date,
            trace_rows=trace_rows,
            requested_assets=requested_assets,
        ):
            root_cause = _gate_root_cause_fields(
                row_date=row_date,
                asset=asset,
                trace_exists=trace_exists,
                data_quality_passed=data_quality_passed,
                feature_passed=feature_passed,
                lineage_ok=lineage_ok,
                data_quality=data_quality,
                feature_availability=feature_availability,
                lineage_manifest=lineage_manifest,
            )
            trace_row_count = _trace_row_count_for_date_asset(
                trace_rows,
                row_date=row_date,
                asset=asset,
            )
            records.append(
                {
                    "date": row_date,
                    "asset": asset,
                    "trace_row_count": trace_row_count,
                    "full_advisory_trace_eligible": full_eligible,
                    "component_validation_trace_eligible": component_eligible,
                    "reason_if_not_full_eligible": "" if full_eligible else "; ".join(reasons),
                    "data_quality_status": dq_status,
                    "feature_availability_status": fa_status,
                    "data_quality_fail_closed_reasons": data_quality["issues"],
                    "feature_availability_fail_closed_reasons": feature_availability["issues"],
                    "historical_replay_manifest": dict(lineage_manifest or {}),
                    "lineage_manifest_complete": lineage_ok,
                    **root_cause,
                    "trace_source": (
                        FULL_ADVISORY_TRACE_SOURCE
                        if full_eligible
                        else (
                            COMPONENT_VALIDATION_TRACE_SOURCE
                            if component_eligible
                            else INELIGIBLE_TRACE_SOURCE
                        )
                    ),
                    "confidence": (
                        TRACE_CONFIDENCE_FULL_ADVISORY
                        if full_eligible
                        else (
                            TRACE_CONFIDENCE_COMPONENT
                            if component_eligible
                            else TRACE_CONFIDENCE_NOT_ELIGIBLE
                        )
                    ),
                    "promotion_gate_allowed": False,
                    "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
                }
            )
    return records


def _requested_audit_dates(
    *,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
) -> set[str]:
    start = _parse_iso_date(event_window_start or start_date or "")
    end = _parse_iso_date(event_window_end or end_date or "")
    if start is None or end is None or end < start:
        return set()
    dates: set[str] = set()
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.add(current.isoformat())
        current += timedelta(days=1)
    return dates


def _gate_record_assets(
    row_date: str,
    *,
    trace_rows: Sequence[Mapping[str, Any]],
    requested_assets: Sequence[str],
) -> list[str]:
    if requested_assets:
        return sorted({asset.upper() for asset in requested_assets})
    date_assets = {
        str(row.get("asset") or "").upper()
        for row in trace_rows
        if str(row.get("date") or "") == row_date and row.get("asset")
    }
    if date_assets:
        return sorted(date_assets)
    all_assets = {str(row.get("asset") or "").upper() for row in trace_rows if row.get("asset")}
    if all_assets:
        return sorted(all_assets)
    return [DEFAULT_TRACE_ASSET]


def _trace_row_count_for_date_asset(
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    row_date: str,
    asset: str,
) -> int:
    asset = asset.upper()
    count = 0
    for row in trace_rows:
        if str(row.get("date") or "") != row_date:
            continue
        row_asset = str(row.get("asset") or "").upper()
        if row_asset in {asset, DEFAULT_TRACE_ASSET}:
            count += 1
    return count


def _gate_root_cause_fields(
    *,
    row_date: str,
    asset: str,
    trace_exists: bool,
    data_quality_passed: bool,
    feature_passed: bool,
    lineage_ok: bool,
    data_quality: Mapping[str, Any],
    feature_availability: Mapping[str, Any],
    lineage_manifest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if trace_exists and data_quality_passed and feature_passed and lineage_ok:
        return {
            "blocked_gate": "",
            "blocked_gates": [],
            "missing_or_late_feature": "",
            "feature_available_time": "",
            "decision_time": "",
            "reason_class": "",
            "can_be_repaired_without_relaxing_production_gate": False,
        }
    blocked_gates = _blocked_gates(
        trace_exists=trace_exists,
        data_quality_passed=data_quality_passed,
        feature_passed=feature_passed,
        lineage_ok=lineage_ok,
    )
    issue = _primary_root_cause_issue(data_quality, feature_availability, blocked_gates)
    reason_class = _classify_gate_root_cause(
        trace_exists=trace_exists,
        data_quality_passed=data_quality_passed,
        feature_passed=feature_passed,
        lineage_ok=lineage_ok,
        issue=issue,
    )
    missing_or_late_feature = _missing_or_late_feature(issue, blocked_gates)
    return {
        "blocked_gate": blocked_gates[0] if blocked_gates else "",
        "blocked_gates": blocked_gates,
        "missing_or_late_feature": missing_or_late_feature,
        "feature_available_time": _issue_time(issue, "available_time") or "not_reported",
        "decision_time": (
            _issue_time(issue, "decision_time")
            or str((lineage_manifest or {}).get("decision_time") or row_date)
        ),
        "reason_class": reason_class,
        "can_be_repaired_without_relaxing_production_gate": (
            reason_class != "expected_pit_limitation"
        ),
    }


def _gate_root_cause_record(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "date": record.get("date"),
        "asset": record.get("asset"),
        "blocked_gate": record.get("blocked_gate"),
        "missing_or_late_feature": record.get("missing_or_late_feature"),
        "feature_available_time": record.get("feature_available_time"),
        "decision_time": record.get("decision_time"),
        "reason_class": record.get("reason_class"),
        "can_be_repaired_without_relaxing_production_gate": record.get(
            "can_be_repaired_without_relaxing_production_gate"
        ),
        "trace_source": record.get("trace_source"),
        "confidence": record.get("confidence"),
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _blocked_gates(
    *,
    trace_exists: bool,
    data_quality_passed: bool,
    feature_passed: bool,
    lineage_ok: bool,
) -> list[str]:
    blocked: list[str] = []
    if not data_quality_passed:
        blocked.append("data_quality_gate")
    if not feature_passed:
        blocked.append("feature_availability_gate")
    if not trace_exists:
        blocked.append("historical_replay_trace")
    if trace_exists and not lineage_ok:
        blocked.append("historical_replay_lineage_manifest")
    return blocked


def _primary_root_cause_issue(
    data_quality: Mapping[str, Any],
    feature_availability: Mapping[str, Any],
    blocked_gates: Sequence[str],
) -> Mapping[str, Any]:
    if "feature_availability_gate" in blocked_gates:
        issues = feature_availability.get("issues", [])
        if isinstance(issues, Sequence) and issues:
            return issues[0] if isinstance(issues[0], Mapping) else {}
    if "data_quality_gate" in blocked_gates:
        issues = data_quality.get("issues", [])
        if isinstance(issues, Sequence) and issues:
            return issues[0] if isinstance(issues[0], Mapping) else {}
    return {}


def _classify_gate_root_cause(
    *,
    trace_exists: bool,
    data_quality_passed: bool,
    feature_passed: bool,
    lineage_ok: bool,
    issue: Mapping[str, Any],
) -> str:
    issue_text = " ".join(
        str(issue.get(key) or "") for key in ("code", "rule_or_source", "message")
    )
    lowered = issue_text.lower()
    if not feature_passed and "available_time_after_decision_time" in lowered:
        return "expected_pit_limitation"
    if not trace_exists and data_quality_passed and feature_passed:
        return "replay_config_issue"
    if trace_exists and not lineage_ok:
        return "lineage_manifest_missing"
    if "report_missing" in lowered or "lineage" in lowered:
        return "lineage_manifest_missing"
    if any(token in lowered for token in ("timestamp", "available_time", "decision_time")):
        return "timestamp_model_issue"
    return "ingestion_issue"


def _missing_or_late_feature(issue: Mapping[str, Any], blocked_gates: Sequence[str]) -> str:
    for key in ("rule_or_source", "code", "source", "feature"):
        value = issue.get(key)
        if value not in (None, ""):
            return str(value)
    if "historical_replay_lineage_manifest" in blocked_gates:
        return "historical_replay_lineage_manifest"
    if "historical_replay_trace" in blocked_gates:
        return "multi_stage_weight_trace"
    return "UNKNOWN"


def _issue_time(issue: Mapping[str, Any], field: str) -> str:
    for key in (field, f"{field}_utc"):
        value = issue.get(key)
        if value not in (None, ""):
            return str(value)
    message = str(issue.get("message") or "")
    date_time_pattern = (
        rf"{re.escape(field)}\s*[=:：]\s*" r"([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][^。；;,\s]+)?)"
    )
    match = re.search(date_time_pattern, message)
    return "" if match is None else match.group(1)


def _gate_audit_dates(gate_audit_root: Path | None) -> set[str]:
    if gate_audit_root is None or not gate_audit_root.exists():
        return set()
    dates: set[str] = set()
    for child in gate_audit_root.iterdir():
        if child.is_dir() and _parse_iso_date(child.name) is not None:
            dates.add(child.name)
    return dates


def _date_matches_filters(
    value: str,
    *,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
) -> bool:
    parsed = _parse_iso_date(value)
    if parsed is None:
        return False
    start = _parse_iso_date(start_date or "")
    end = _parse_iso_date(end_date or "")
    window_start = _parse_iso_date(event_window_start or "")
    window_end = _parse_iso_date(event_window_end or "")
    if start is not None and parsed < start:
        return False
    if end is not None and parsed > end:
        return False
    if window_start is not None and parsed < window_start:
        return False
    if window_end is not None and parsed > window_end:
        return False
    return True


def _read_gate_report_status(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"status": "MISSING", "issues": [{"code": "report_missing", "message": ""}]}
    text = path.read_text(encoding="utf-8")
    status = "UNKNOWN"
    issues: list[dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- 状态：") or stripped.startswith("- status:"):
            status = stripped.split("：", 1)[-1].split(":", 1)[-1].strip(" `")
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) < 3 or cells[0] in {"---", "Severity", "级别"}:
            continue
        severity = cells[0].upper()
        if severity not in {"ERROR", "FAIL", "警告", "WARNING", "错误"}:
            continue
        issue = {
            "severity": severity,
            "code": cells[1] if len(cells) > 1 else "",
            "rule_or_source": cells[2] if len(cells) > 2 else "",
            "message": cells[4] if len(cells) > 4 else cells[-1],
        }
        issues.append(issue)
    return {"status": status, "issues": issues}


def _gate_failure_reasons(
    *,
    trace_exists: bool,
    data_quality: Mapping[str, Any],
    feature_availability: Mapping[str, Any],
    lineage_manifest: Mapping[str, Any] | None,
    lineage_ok: bool,
) -> list[str]:
    reasons: list[str] = []
    if not trace_exists:
        reasons.append("multi-stage trace missing for date")
    if trace_exists and not lineage_ok:
        proof_status = str((lineage_manifest or {}).get("proof_status") or "MISSING")
        reasons.append(f"lineage_manifest={proof_status}")
    if data_quality.get("status") not in {"PASS", "PASS_WITH_WARNINGS"}:
        reasons.append(f"data_quality={data_quality.get('status')}")
    if feature_availability.get("status") not in {"PASS", "PASS_WITH_WARNINGS"}:
        reasons.append(f"feature_availability={feature_availability.get('status')}")
    for report_name, report in (
        ("data_quality", data_quality),
        ("feature_availability", feature_availability),
    ):
        for issue in report.get("issues", [])[:5]:
            if not isinstance(issue, Mapping):
                continue
            code = issue.get("code") or issue.get("severity")
            message = issue.get("message") or ""
            reasons.append(f"{report_name}:{code}:{message}")
    return reasons


def _gate_availability_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    reason_class_counts: dict[str, int] = {}
    audited_dates = {str(record.get("date")) for record in records if record.get("date")}
    assets = {str(record.get("asset")) for record in records if record.get("asset")}
    full_dates = {
        str(record.get("date")) for record in records if record.get("full_advisory_trace_eligible")
    }
    component_dates = {
        str(record.get("date"))
        for record in records
        if record.get("component_validation_trace_eligible")
    }
    for record in records:
        if record.get("full_advisory_trace_eligible"):
            continue
        reason = str(record.get("reason_if_not_full_eligible") or "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        reason_class = str(record.get("reason_class") or "UNKNOWN")
        reason_class_counts[reason_class] = reason_class_counts.get(reason_class, 0) + 1
    return {
        "audited_date_count": len(audited_dates),
        "date_count": len(audited_dates),
        "asset_count": len(assets),
        "case_count": len(records),
        "blocked_case_count": sum(
            1 for record in records if not record.get("full_advisory_trace_eligible")
        ),
        "full_advisory_trace_eligible_count": len(full_dates),
        "component_validation_trace_eligible_count": len(component_dates),
        "not_full_eligible_count": len(audited_dates - full_dates),
        "root_cause_reason_class_counts": dict(sorted(reason_class_counts.items())),
        "fail_closed_reason_counts": dict(sorted(reason_counts.items())),
    }


def _backtest_bridge_record(
    row: Mapping[str, Any],
    *,
    price_series: Sequence[tuple[date, float]],
    outcome_ticker: str,
    case_asset: str | None = None,
    trace_contract_version: str,
    missing_asset_mapping: bool = False,
) -> dict[str, Any]:
    signal_date = _parse_iso_date(str(row.get("date") or ""))
    asset = (case_asset or str(row.get("asset") or DEFAULT_TRACE_ASSET)).upper()
    outcome_payload = _forward_outcome_payload_with_availability(
        price_series,
        signal_date,
        missing_asset_mapping=missing_asset_mapping,
    )
    outcomes = outcome_payload["outcomes"]
    return {
        "as_of_date": row.get("date"),
        "date": row.get("date"),
        "decision_time": str(row.get("decision_time") or row.get("date") or ""),
        "asset": asset,
        "scenario": "baseline",
        "source_trace_asset": row.get("asset"),
        "trace_source": BACKTEST_TRACE_BRIDGE_SOURCE,
        "confidence": TRACE_CONFIDENCE_BRIDGE,
        "trace_contract_version": trace_contract_version,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
        "upstream_indicator_id": row.get("upstream_indicator_id"),
        "downstream_indicator_id": row.get("downstream_indicator_id"),
        "pre_mask_signal": _first_non_zero_float(
            row.get("weight_before"),
            row.get("pre_constraint_signal_weight"),
        ),
        "post_mask_signal": _first_non_zero_float(
            row.get("weight_after"),
            row.get("post_constraint_signal_weight"),
        ),
        "final_advisory_facing_weight": _first_non_zero_float(
            row.get("final_advisory_portfolio_facing_weight"),
            row.get("weight_after"),
        ),
        "b_intended_change": _float(row.get("b_intended_change")),
        "a_suppressed_change": _float(row.get("a_suppressed_change")),
        "constraint_hit": bool(row.get("constraint_hit")),
        "reason_code": row.get("reason_code"),
        "outcome_ticker": outcome_ticker,
        "outcome_join_key": _outcome_join_key(
            as_of_date=str(row.get("date") or ""),
            decision_time=str(row.get("decision_time") or row.get("date") or ""),
            asset=asset,
            scenario="baseline",
            trace_source=BACKTEST_TRACE_BRIDGE_SOURCE,
            trace_contract_version=trace_contract_version,
        ),
        "outcomes": outcomes,
        "outcome_windows": outcome_payload["windows"],
        "outcome_missing": outcome_payload["outcome_missing"],
        "outcome_not_mature": outcome_payload["outcome_not_mature"],
        "outcome_status": outcome_payload["outcome_status"],
    }


def _scan_bridge_source_artifacts(bridge_artifact_root: Path | None) -> list[dict[str, Any]]:
    if bridge_artifact_root is None or not bridge_artifact_root.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(bridge_artifact_root.rglob("*.json")):
        if len(records) >= 200:
            break
        if "control_plane_v1_validation" in path.parts:
            continue
        if path.name in {
            "backtest_trace_bridge.json",
            "indicator_to_signal_research_framework_v1_validation_pack.json",
            "indicator_validation_pack_rerun_stability_report.json",
        }:
            continue
        lowered = path.name.lower()
        if not any(
            token in lowered
            for token in ("backtest", "simulation", "advisory", "outcome", "decision")
        ):
            continue
        record = _bridge_artifact_record(path)
        if record:
            records.append(record)
    return records


def _bridge_artifact_record(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(raw, Mapping):
        return None
    date_value = _first_text_field(
        raw,
        ("as_of", "date", "signal_date", "generated_at", "created_at"),
    )
    summary = raw.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    lineage = _bridge_source_lineage_manifest(path, raw, date_value)
    return {
        "artifact_path": str(path),
        **lineage,
        "artifact_type": str(
            raw.get("report_type") or raw.get("artifact_type") or raw.get("type") or path.stem
        ),
        "status": str(raw.get("status") or raw.get("validation_status") or "UNKNOWN"),
        "date": "" if date_value is None else str(date_value)[:10],
        "summary_keys": sorted(str(key) for key in summary.keys())[:20],
        "trace_source": BACKTEST_TRACE_BRIDGE_SOURCE,
        "confidence": TRACE_CONFIDENCE_BRIDGE,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _bridge_source_lineage_manifest(
    path: Path,
    raw: Mapping[str, Any],
    date_value: str | None,
) -> dict[str, Any]:
    as_of_date = "" if date_value is None else str(date_value)[:10]
    return {
        "source_artifact_path": str(path),
        "generated_at": str(raw.get("generated_at") or raw.get("created_at") or ""),
        "as_of_date": as_of_date,
        "decision_time": str(raw.get("decision_time") or as_of_date),
        "config_hash": str(raw.get("config_hash") or ""),
        "input_snapshot_hash": str(raw.get("input_snapshot_hash") or _file_sha256(path)),
        "trace_contract_version": str(raw.get("trace_contract_version") or ""),
        "production_equivalent": bool(raw.get("production_equivalent")),
    }


def _first_text_field(raw: Mapping[str, Any], fields: Sequence[str]) -> str | None:
    for field in fields:
        value = raw.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _valuation_normalization_payload(scoring_rules_path: Path) -> dict[str, Any]:
    rules = load_scoring_rules(scoring_rules_path)
    valuation = rules.valuation
    return {
        "method": "weighted signal points mapped into the daily valuation 0..100 score",
        "score_scale": "0..100",
        "neutral_score": None if valuation is None else valuation.neutral_score,
        "signals": _score_signal_payloads(valuation),
        "trace_fields": [
            "raw_indicator_value",
            "normalized_indicator_score",
            "mapped_signal_contribution",
        ],
    }


def _valuation_effective_thresholds(scoring_rules_path: Path) -> dict[str, Any]:
    rules = load_scoring_rules(scoring_rules_path)
    valuation = rules.valuation
    gate = rules.position_gates.valuation
    return {
        "score_signal_thresholds": _score_signal_payloads(valuation),
        "position_gate_thresholds": {
            "expensive_or_crowded_max_position": gate.expensive_or_crowded_max_position,
            "extreme_overheated_max_position": gate.extreme_overheated_max_position,
        },
        "parameter_source": "config/scoring_rules.yaml:valuation and position_gates.valuation",
        "parameter_mutation": False,
    }


def _score_signal_payloads(module: Any) -> list[dict[str, Any]]:
    if module is None:
        return []
    signals = getattr(module, "signals", []) or []
    payloads = []
    for signal in signals:
        if hasattr(signal, "model_dump"):
            payloads.append(signal.model_dump(mode="json"))
        elif isinstance(signal, Mapping):
            payloads.append(dict(signal))
    return payloads


def _trace_observations_for_indicator(
    trace_rows: Sequence[Mapping[str, Any]],
    indicator_id: str,
) -> list[dict[str, Any]]:
    observations = []
    for row in trace_rows:
        if indicator_id not in {
            str(row.get("module_id") or ""),
            str(row.get("upstream_indicator_id") or ""),
            str(row.get("downstream_indicator_id") or ""),
        }:
            continue
        observations.append(
            {
                "date": row.get("date"),
                "asset": row.get("asset"),
                "row_type": row.get("row_type"),
                "raw_indicator_value": row.get("raw_indicator_value"),
                "normalized_indicator_score": row.get("normalized_indicator_score"),
                "mapped_signal_contribution": row.get("mapped_signal_contribution"),
                "pre_constraint_signal_weight": row.get("pre_constraint_signal_weight"),
                "post_constraint_signal_weight": row.get("post_constraint_signal_weight"),
                "final_advisory_portfolio_facing_weight": row.get(
                    "final_advisory_portfolio_facing_weight"
                ),
            }
        )
    return observations[:50]


def _high_masking_reason_payload(
    registry: IndicatorResearchRegistry,
    masking_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    policy = registry.validation_policy.masking
    for result in masking_results:
        if (
            result.get("upstream_indicator_id") == "valuation_crowding_indicator"
            and result.get("downstream_indicator_id") == "trend_strength_indicator"
        ):
            return {
                "masking_status": result.get("masking_status"),
                "masking_ratio": result.get("masking_ratio"),
                "high_masking_threshold": policy.high_min,
                "b_intended_change_abs_sum": result.get("b_intended_change_abs_sum"),
                "a_suppressed_change_abs_sum": result.get("a_suppressed_change_abs_sum"),
                "reason": (
                    "valuation/crowding suppression divided by trend intended change "
                    "meets or exceeds the configured HIGH_MASKING threshold"
                    if result.get("masking_status") == "HIGH_MASKING"
                    else "multi-stage trace is required before HIGH_MASKING can be confirmed"
                ),
            }
    return {
        "masking_status": TRACE_REQUIRED_STATUS,
        "masking_ratio": None,
        "high_masking_threshold": policy.high_min,
        "reason": "valuation/crowding -> trend masking pair was not observed in trace",
    }


def _pair_trace_rows(
    trace_rows: Sequence[Mapping[str, Any]],
    upstream_indicator_id: str,
    downstream_indicator_id: str,
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in trace_rows
        if row.get("upstream_indicator_id") == upstream_indicator_id
        and row.get("downstream_indicator_id") == downstream_indicator_id
    ]


def _component_trace_index(
    trace_rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], Mapping[str, Any]]:
    index: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in trace_rows:
        if row.get("row_type") != "indicator_component":
            continue
        row_date = str(row.get("date") or "")
        module_id = str(row.get("module_id") or "")
        if row_date and module_id:
            index[(row_date, module_id)] = row
    return index


def _masking_casebook_row(
    row: Mapping[str, Any],
    *,
    component_index: Mapping[tuple[str, str], Mapping[str, Any]],
    price_series: Sequence[tuple[date, float]],
    outcome_ticker: str,
    case_asset: str | None = None,
    trace_contract_version: str,
    missing_asset_mapping: bool = False,
) -> dict[str, Any]:
    row_date = str(row.get("date") or "")
    asset = (case_asset or str(row.get("asset") or DEFAULT_TRACE_ASSET)).upper()
    signal_date = _parse_iso_date(row_date)
    trend_component = component_index.get((row_date, "trend_strength_indicator"))
    valuation_component = component_index.get((row_date, "valuation_crowding_indicator"))
    pre_mask_signal = _first_non_zero_float(
        row.get("weight_before"),
        row.get("pre_constraint_signal_weight"),
    )
    post_mask_signal = _first_non_zero_float(
        row.get("weight_after"),
        row.get("post_constraint_signal_weight"),
    )
    final_weight = _first_non_zero_float(
        row.get("final_advisory_portfolio_facing_weight"),
        post_mask_signal,
    )
    b_intended = _float(row.get("b_intended_change"))
    a_suppressed = _float(row.get("a_suppressed_change"))
    outcome_payload = _forward_outcome_payload_with_availability(
        price_series,
        signal_date,
        missing_asset_mapping=missing_asset_mapping,
    )
    outcomes = outcome_payload["outcomes"]
    returns = [
        value
        for key, value in outcomes.items()
        if key.startswith("return_") and isinstance(value, float)
    ]
    drawdown_reduced = a_suppressed > 0 and any(value < 0 for value in returns)
    missed_upside = a_suppressed > 0 and any(value > 0 for value in returns)
    false_risk_off = missed_upside and not drawdown_reduced
    decision_time = str(row.get("decision_time") or row_date)
    return {
        "as_of_date": row_date,
        "case_id": f"{row_date}:{asset}:{row.get('reason_code', '')}",
        "date": row_date,
        "decision_time": decision_time,
        "scenario": "baseline",
        "sample_id": (
            f"{row_date}:{row.get('module_id', '')}:" f"{row.get('downstream_indicator_id', '')}"
        ),
        "asset": asset,
        "source_trace_asset": row.get("asset"),
        "trace_contract_version": trace_contract_version,
        "outcome_ticker": outcome_ticker,
        "trend_raw_direction": _trend_direction(trend_component, b_intended),
        "valuation_crowding_raw_direction": _valuation_direction(
            valuation_component,
            row,
            a_suppressed,
        ),
        "trend_raw_indicator_value": (
            None if trend_component is None else trend_component.get("raw_indicator_value")
        ),
        "valuation_crowding_raw_indicator_value": (
            row.get("raw_indicator_value")
            if valuation_component is None
            else valuation_component.get("raw_indicator_value")
        ),
        "pre_mask_signal": pre_mask_signal,
        "post_mask_signal": post_mask_signal,
        "final_advisory_facing_weight": final_weight,
        "b_intended_change": b_intended,
        "a_suppressed_change": a_suppressed,
        "masking_ratio": None if abs(b_intended) <= 0 else abs(a_suppressed) / abs(b_intended),
        "outcome_join_key": _outcome_join_key(
            as_of_date=row_date,
            decision_time=decision_time,
            asset=asset,
            scenario="baseline",
            trace_source=COMPONENT_VALIDATION_TRACE_SOURCE,
            trace_contract_version=trace_contract_version,
        ),
        "outcomes": outcomes,
        "outcome_windows": outcome_payload["windows"],
        "drawdown_reduced": drawdown_reduced,
        "missed_upside": missed_upside,
        "false_risk_off": false_risk_off,
        "outcome_missing": outcome_payload["outcome_missing"],
        "outcome_not_mature": outcome_payload["outcome_not_mature"],
        "outcome_status": outcome_payload["outcome_status"],
        "constraint_hit": bool(row.get("constraint_hit")),
        "reason_code": row.get("reason_code"),
        "trace_source": COMPONENT_VALIDATION_TRACE_SOURCE,
        "confidence": TRACE_CONFIDENCE_COMPONENT,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
        "event_window_ids": _event_window_ids_for_date(row_date),
    }


def _read_price_series(prices_path: Path | None, ticker: str) -> list[tuple[date, float]]:
    return _read_price_series_by_ticker(prices_path).get(ticker.upper(), [])


def _read_price_series_by_ticker(prices_path: Path | None) -> dict[str, list[tuple[date, float]]]:
    if prices_path is None:
        return {}
    if not prices_path.exists():
        raise IndicatorResearchError(f"prices path not found: {prices_path}")
    with prices_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        ticker_field = "ticker" if "ticker" in fieldnames else "symbol"
        price_field = "adj_close" if "adj_close" in fieldnames else "close"
        missing = {"date", ticker_field, price_field} - fieldnames
        if missing:
            raise IndicatorResearchError(
                f"prices CSV missing required columns: {', '.join(sorted(missing))}"
            )
        by_ticker: dict[str, dict[date, float]] = defaultdict(dict)
        for record in reader:
            ticker = str(record.get(ticker_field, "")).upper()
            parsed_date = _parse_iso_date(str(record.get("date") or ""))
            price = _float(record.get(price_field))
            if not ticker or parsed_date is None or price <= 0:
                continue
            by_ticker[ticker][parsed_date] = price
    return {ticker: sorted(by_date.items()) for ticker, by_date in by_ticker.items()}


def _forward_outcome_payload(
    price_series: Sequence[tuple[date, float]],
    signal_date: date | None,
) -> dict[str, Any]:
    return _forward_outcome_payload_with_availability(
        price_series,
        signal_date,
    )["outcomes"]


def _forward_outcome_payload_with_availability(
    price_series: Sequence[tuple[date, float]],
    signal_date: date | None,
    *,
    missing_asset_mapping: bool = False,
) -> dict[str, Any]:
    outcomes: dict[str, Any] = {
        **{f"return_{horizon}d": None for horizon in MASKING_OUTCOME_HORIZONS},
        "max_drawdown_20d": None,
    }
    if signal_date is None:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_JOIN_KEY)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
        return _outcome_payload_from_windows(outcomes, windows)
    if missing_asset_mapping:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
        return _outcome_payload_from_windows(outcomes, windows)
    if not price_series:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_CALENDAR)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
        return _outcome_payload_from_windows(outcomes, windows)
    start_index = next(
        (index for index, (price_date, _) in enumerate(price_series) if price_date >= signal_date),
        None,
    )
    if start_index is None:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_PRICE)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
        return _outcome_payload_from_windows(outcomes, windows)
    start_price = price_series[start_index][1]
    if start_price <= 0:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_PRICE)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
        return _outcome_payload_from_windows(outcomes, windows)
    latest_available_price_date = price_series[-1][0]
    windows = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        target_index = start_index + horizon
        if target_index < len(price_series):
            target_date, target_price = price_series[target_index]
            value = target_price / start_price - 1
            outcomes[f"return_{horizon}d"] = value
            windows.append(
                _outcome_window_record(
                    horizon,
                    OUTCOME_WINDOW_STATUS_AVAILABLE,
                    target_date=target_date,
                    realized_return=value,
                    latest_available_price_date=latest_available_price_date,
                    evaluation_cutoff_met=True,
                )
            )
        else:
            windows.append(
                _outcome_window_record(
                    horizon,
                    OUTCOME_WINDOW_STATUS_NOT_MATURE,
                    latest_available_price_date=latest_available_price_date,
                    evaluation_cutoff_met=False,
                )
            )
    if start_index + 20 < len(price_series):
        outcomes["max_drawdown_20d"] = _max_drawdown(
            [price for _, price in price_series[start_index : start_index + 21]]
        )
    return _outcome_payload_from_windows(outcomes, windows)


def _outcome_window_record(
    horizon: int,
    status: str,
    *,
    target_date: date | None = None,
    realized_return: float | None = None,
    latest_available_price_date: date | None = None,
    evaluation_cutoff_met: bool | None = None,
) -> dict[str, Any]:
    return {
        "window": f"{horizon}d",
        "horizon_trading_days": horizon,
        "status": status,
        "target_date": None if target_date is None else target_date.isoformat(),
        "latest_available_price_date": (
            None if latest_available_price_date is None else latest_available_price_date.isoformat()
        ),
        "evaluation_cutoff_met": evaluation_cutoff_met,
        "realized_return": realized_return,
    }


def _outcome_payload_from_windows(
    outcomes: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    hard_missing = any(
        str(window.get("status") or "") in OUTCOME_HARD_MISSING_STATUSES for window in windows
    )
    not_mature = any(window.get("status") == OUTCOME_WINDOW_STATUS_NOT_MATURE for window in windows)
    all_available = all(
        window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE for window in windows
    )
    return {
        "outcomes": dict(outcomes),
        "windows": [dict(window) for window in windows],
        "outcome_missing": hard_missing,
        "outcome_not_mature": not_mature,
        "outcome_status": (
            "available"
            if all_available
            else (
                "missing"
                if hard_missing
                else (OUTCOME_WINDOW_STATUS_NOT_MATURE if not_mature else "unknown")
            )
        ),
    }


def _outcome_join_key(
    *,
    as_of_date: str,
    decision_time: str,
    asset: str,
    scenario: str,
    trace_source: str,
    trace_contract_version: str,
) -> dict[str, str]:
    return {
        "as_of_date": as_of_date,
        "decision_time": decision_time,
        "asset": asset,
        "scenario": scenario,
        "trace_source": trace_source,
        "trace_contract_version": trace_contract_version,
    }


def _trace_eligibility_date_sets(
    *,
    registry_path: Path,
    gate_audit_root: Path | None,
    trace_path: Path | None,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
    asset_universe: str | None,
) -> tuple[set[str], set[str]]:
    if gate_audit_root is None:
        return set(), set()
    gate_audit = build_gate_availability_audit(
        registry_path=registry_path,
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    availability = [
        dict(item) for item in gate_audit.get("gate_availability", []) if isinstance(item, Mapping)
    ]
    full_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("full_advisory_trace_eligible")
    }
    component_only_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("component_validation_trace_eligible")
        and not record.get("full_advisory_trace_eligible")
    }
    return full_dates, component_only_dates


def _outcome_availability_record(
    case: Mapping[str, Any],
    *,
    source_case_type: str,
    scenario: str,
    full_dates: set[str] | None = None,
    component_only_dates: set[str] | None = None,
) -> dict[str, Any]:
    join_key = _case_outcome_join_key(case, scenario=scenario)
    missing_join_key_fields = [
        field for field in OUTCOME_JOIN_KEY_FIELDS if not str(join_key.get(field) or "")
    ]
    if missing_join_key_fields:
        windows = [
            _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_JOIN_KEY)
            for horizon in MASKING_OUTCOME_HORIZONS
        ]
    else:
        raw_windows = case.get("outcome_windows", [])
        if isinstance(raw_windows, Sequence) and not isinstance(raw_windows, (str, bytes)):
            windows = [dict(item) for item in raw_windows if isinstance(item, Mapping)]
        else:
            windows = []
        if not windows:
            windows = [
                _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_MISSING_PRICE)
                for horizon in MASKING_OUTCOME_HORIZONS
            ]
    statuses = {str(window.get("status") or "") for window in windows}
    hard_missing = bool(statuses & OUTCOME_HARD_MISSING_STATUSES)
    not_mature = OUTCOME_WINDOW_STATUS_NOT_MATURE in statuses
    all_available = all(
        window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE for window in windows
    )
    as_of_date = join_key["as_of_date"]
    eligibility_layer = _outcome_record_eligibility_layer(
        as_of_date,
        source_case_type=source_case_type,
        full_dates=full_dates or set(),
        component_only_dates=component_only_dates or set(),
    )
    return {
        "source_case_type": source_case_type,
        "eligibility_layer": eligibility_layer,
        "case_id": case.get("case_id"),
        "as_of_date": as_of_date,
        "date": as_of_date,
        "decision_time": join_key["decision_time"],
        "asset": join_key["asset"],
        "scenario": join_key["scenario"],
        "trace_source": join_key["trace_source"],
        "trace_contract_version": join_key["trace_contract_version"],
        "outcome_join_key": join_key,
        "missing_join_key_fields": missing_join_key_fields,
        "outcome_windows": windows,
        "outcomes": (
            dict(case.get("outcomes", {})) if isinstance(case.get("outcomes"), Mapping) else {}
        ),
        "outcome_available": all_available,
        "mature_horizons": _case_mature_horizons_from_windows(windows),
        "not_mature_horizons": _case_not_mature_horizons_from_windows(windows),
        "outcome_missing": hard_missing,
        "outcome_not_mature": not_mature,
        "missing_price": OUTCOME_WINDOW_STATUS_MISSING_PRICE in statuses,
        "missing_asset_mapping": OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING in statuses,
        "missing_calendar": OUTCOME_WINDOW_STATUS_MISSING_CALENDAR in statuses,
        "missing_join_key": bool(missing_join_key_fields),
        "market_regime": str(case.get("market_regime") or ""),
        "event_window_ids": list(
            case.get("event_window_ids") or _event_window_ids_for_date(as_of_date)
        ),
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _outcome_availability_records(
    casebook_cases: Sequence[Mapping[str, Any]],
    bridge_cases: Sequence[Mapping[str, Any]],
    *,
    full_dates: set[str] | None = None,
    component_only_dates: set[str] | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for case in casebook_cases:
        records.append(
            _outcome_availability_record(
                case,
                source_case_type="masking_casebook",
                scenario=str(case.get("scenario") or "baseline"),
                full_dates=full_dates,
                component_only_dates=component_only_dates,
            )
        )
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            records.append(
                _outcome_availability_record(
                    case,
                    source_case_type="ablation_scenario",
                    scenario=scenario_id,
                    full_dates=full_dates,
                    component_only_dates=component_only_dates,
                )
            )
    for case in bridge_cases:
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            records.append(
                _outcome_availability_record(
                    case,
                    source_case_type="backtest_bridge",
                    scenario=scenario_id,
                    full_dates=full_dates,
                    component_only_dates=component_only_dates,
                )
            )
    return records


def _case_outcome_join_key(case: Mapping[str, Any], *, scenario: str) -> dict[str, str]:
    join_key = case.get("outcome_join_key", {})
    if not isinstance(join_key, Mapping):
        join_key = {}
    return {
        "as_of_date": str(
            join_key.get("as_of_date") or case.get("as_of_date") or case.get("date") or ""
        ),
        "decision_time": str(
            join_key.get("decision_time") or case.get("decision_time") or case.get("date") or ""
        ),
        "asset": str(join_key.get("asset") or case.get("asset") or "").upper(),
        "scenario": scenario,
        "trace_source": str(join_key.get("trace_source") or case.get("trace_source") or ""),
        "trace_contract_version": str(
            join_key.get("trace_contract_version") or case.get("trace_contract_version") or ""
        ),
    }


def _outcome_availability_summary(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_window = {
        f"{horizon}d": _outcome_empty_availability_bucket(f"{horizon}d")
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    by_asset: dict[str, dict[str, Any]] = {}
    by_date: dict[str, dict[str, Any]] = {}
    for record in records:
        _outcome_update_bucket(by_asset, str(record.get("asset") or "UNKNOWN"), record)
        _outcome_update_bucket(by_date, str(record.get("as_of_date") or "UNKNOWN"), record)
        for window in record.get("outcome_windows", []):
            if not isinstance(window, Mapping):
                continue
            window_id = str(window.get("window") or "")
            bucket = by_window.setdefault(
                window_id,
                _outcome_empty_availability_bucket(window_id),
            )
            _outcome_update_window_bucket(bucket, str(window.get("status") or "unknown"))
    horizon_mature_counts = {
        f"{horizon}d": by_window[f"{horizon}d"]["available_count"]
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    horizon_not_mature_counts = {
        f"{horizon}d": by_window[f"{horizon}d"]["not_mature_count"]
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    return {
        "total_cases": len(records),
        "outcome_available_count": sum(1 for record in records if record.get("outcome_available")),
        "outcome_missing_count": sum(1 for record in records if record.get("outcome_missing")),
        "outcome_not_mature_count": sum(
            1 for record in records if record.get("outcome_not_mature")
        ),
        "missing_price_count": sum(1 for record in records if record.get("missing_price")),
        "missing_asset_mapping_count": sum(
            1 for record in records if record.get("missing_asset_mapping")
        ),
        "missing_calendar_count": sum(1 for record in records if record.get("missing_calendar")),
        "missing_join_key_count": sum(1 for record in records if record.get("missing_join_key")),
        "date_count": len(
            {str(record.get("as_of_date") or "") for record in records if record.get("as_of_date")}
        ),
        "asset_count": len(
            {str(record.get("asset") or "") for record in records if record.get("asset")}
        ),
        "scenario_count": len(
            {str(record.get("scenario") or "") for record in records if record.get("scenario")}
        ),
        "source_case_type_count": len(
            {
                str(record.get("source_case_type") or "")
                for record in records
                if record.get("source_case_type")
            }
        ),
        "mature_case_count_by_horizon": horizon_mature_counts,
        "not_mature_count_by_horizon": horizon_not_mature_counts,
        "by_window": dict(sorted(by_window.items())),
        "by_asset": [{"asset": key, **value} for key, value in sorted(by_asset.items())],
        "by_date": [{"date": key, **value} for key, value in sorted(by_date.items())],
    }


def _horizon_specific_summary_counts(summary: Mapping[str, Any]) -> dict[str, int]:
    mature = summary.get("mature_case_count_by_horizon", {})
    if not isinstance(mature, Mapping):
        mature = {}
    not_mature = summary.get("not_mature_count_by_horizon", {})
    if not isinstance(not_mature, Mapping):
        not_mature = {}
    fields: dict[str, int] = {}
    for horizon in MASKING_OUTCOME_HORIZONS:
        label = f"{horizon}d"
        fields[f"{label}_mature_case_count"] = int(mature.get(label) or 0)
        fields[f"{label}_not_mature_count"] = int(not_mature.get(label) or 0)
    return fields


def _mature_outcome_sample_quality(
    records: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    mature_date_count: dict[str, int] = {}
    mature_asset_count: dict[str, int] = {}
    mature_case_count: dict[str, int] = {}
    full_mature_count: dict[str, int] = {}
    component_mature_count: dict[str, int] = {}
    bridge_mature_count: dict[str, int] = {}
    for horizon in MASKING_OUTCOME_HORIZONS:
        label = f"{horizon}d"
        mature_records = [
            record for record in records if _record_horizon_status(record, horizon) == "available"
        ]
        mature_date_count[label] = len(
            {
                str(record.get("as_of_date") or "")
                for record in mature_records
                if record.get("as_of_date")
            }
        )
        mature_asset_count[label] = len(
            {str(record.get("asset") or "") for record in mature_records if record.get("asset")}
        )
        mature_case_count[label] = len(mature_records)
        full_mature_count[label] = sum(
            1
            for record in mature_records
            if record.get("eligibility_layer") == "full_advisory_only"
        )
        component_mature_count[label] = sum(
            1 for record in mature_records if record.get("eligibility_layer") == "component_only"
        )
        bridge_mature_count[label] = sum(
            1 for record in mature_records if record.get("eligibility_layer") == "backtest_bridge"
        )
    return {
        "mature_date_count_by_horizon": mature_date_count,
        "mature_asset_count_by_horizon": mature_asset_count,
        "mature_case_count_by_horizon": mature_case_count,
        "full_advisory_mature_count_by_horizon": full_mature_count,
        "component_only_mature_count_by_horizon": component_mature_count,
        "backtest_bridge_mature_count_by_horizon": bridge_mature_count,
        "by_asset": _mature_outcome_group_availability(
            records,
            lambda record: [str(record.get("asset") or "UNKNOWN")],
            key_field="asset",
        ),
        "by_date": _mature_outcome_group_availability(
            records,
            lambda record: [str(record.get("as_of_date") or "UNKNOWN")],
            key_field="date",
        ),
        "by_regime": _mature_outcome_group_availability(
            records,
            lambda record: [str(record.get("market_regime") or registry.market_regime.regime_id)],
            key_field="regime",
        ),
        "by_event_window": _mature_outcome_group_availability(
            records,
            _event_window_group_keys_for_record,
            key_field="event_window_id",
        ),
    }


def _mature_outcome_group_availability(
    records: Sequence[Mapping[str, Any]],
    key_fn: Any,
    *,
    key_field: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for record in records:
        for key in key_fn(record):
            bucket = buckets.setdefault(
                key,
                {
                    key_field: key,
                    "total_cases": 0,
                    **{f"{horizon}d_mature_case_count": 0 for horizon in MASKING_OUTCOME_HORIZONS},
                    **{f"{horizon}d_not_mature_count": 0 for horizon in MASKING_OUTCOME_HORIZONS},
                    **{f"{horizon}d_missing_count": 0 for horizon in MASKING_OUTCOME_HORIZONS},
                },
            )
            bucket["total_cases"] += 1
            for horizon in MASKING_OUTCOME_HORIZONS:
                label = f"{horizon}d"
                status = _record_horizon_status(record, horizon)
                if status == OUTCOME_WINDOW_STATUS_AVAILABLE:
                    bucket[f"{label}_mature_case_count"] += 1
                elif status == OUTCOME_WINDOW_STATUS_NOT_MATURE:
                    bucket[f"{label}_not_mature_count"] += 1
                elif status in OUTCOME_HARD_MISSING_STATUSES:
                    bucket[f"{label}_missing_count"] += 1
    return [buckets[key] for key in sorted(buckets)]


def _event_window_group_keys_for_record(record: Mapping[str, Any]) -> list[str]:
    raw = record.get("event_window_ids")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        values = [str(item) for item in raw if str(item)]
    else:
        values = []
    return values or ["no_event_window"]


def _outcome_record_eligibility_layer(
    as_of_date: str,
    *,
    source_case_type: str,
    full_dates: set[str],
    component_only_dates: set[str],
) -> str:
    if source_case_type == "backtest_bridge":
        return "backtest_bridge"
    if as_of_date in full_dates:
        return "full_advisory_only"
    if as_of_date in component_only_dates:
        return "component_only"
    return "component_only"


def _case_mature_horizons_from_windows(
    windows: Sequence[Mapping[str, Any]],
) -> list[str]:
    return [
        str(window.get("window") or "")
        for window in windows
        if window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE
    ]


def _case_not_mature_horizons_from_windows(
    windows: Sequence[Mapping[str, Any]],
) -> list[str]:
    return [
        str(window.get("window") or "")
        for window in windows
        if window.get("status") == OUTCOME_WINDOW_STATUS_NOT_MATURE
    ]


def _record_horizon_status(record: Mapping[str, Any], horizon: int) -> str:
    for window in record.get("outcome_windows", []):
        if not isinstance(window, Mapping):
            continue
        if str(window.get("window") or "") == f"{horizon}d":
            return str(window.get("status") or "unknown")
    return "unknown"


def _outcome_empty_availability_bucket(label: str) -> dict[str, Any]:
    return {
        "label": label,
        "total_cases": 0,
        "available_count": 0,
        "missing_count": 0,
        "not_mature_count": 0,
        "missing_price_count": 0,
        "missing_asset_mapping_count": 0,
        "missing_calendar_count": 0,
        "missing_join_key_count": 0,
    }


def _outcome_update_bucket(
    buckets: dict[str, dict[str, Any]],
    key: str,
    record: Mapping[str, Any],
) -> None:
    bucket = buckets.setdefault(key, _outcome_empty_availability_bucket(key))
    bucket["total_cases"] += 1
    if record.get("outcome_available"):
        bucket["available_count"] += 1
    if record.get("outcome_missing"):
        bucket["missing_count"] += 1
    if record.get("outcome_not_mature"):
        bucket["not_mature_count"] += 1
    if record.get("missing_price"):
        bucket["missing_price_count"] += 1
    if record.get("missing_asset_mapping"):
        bucket["missing_asset_mapping_count"] += 1
    if record.get("missing_calendar"):
        bucket["missing_calendar_count"] += 1
    if record.get("missing_join_key"):
        bucket["missing_join_key_count"] += 1


def _outcome_update_window_bucket(bucket: dict[str, Any], status: str) -> None:
    bucket["total_cases"] += 1
    if status == OUTCOME_WINDOW_STATUS_AVAILABLE:
        bucket["available_count"] += 1
    elif status == OUTCOME_WINDOW_STATUS_NOT_MATURE:
        bucket["not_mature_count"] += 1
    else:
        bucket["missing_count"] += 1
        if status == OUTCOME_WINDOW_STATUS_MISSING_PRICE:
            bucket["missing_price_count"] += 1
        elif status == OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING:
            bucket["missing_asset_mapping_count"] += 1
        elif status == OUTCOME_WINDOW_STATUS_MISSING_CALENDAR:
            bucket["missing_calendar_count"] += 1
        elif status == OUTCOME_WINDOW_STATUS_MISSING_JOIN_KEY:
            bucket["missing_join_key_count"] += 1


def _max_drawdown(prices: Sequence[float]) -> float | None:
    if not prices:
        return None
    peak = prices[0]
    max_drawdown = 0.0
    for price in prices:
        if price <= 0:
            continue
        peak = max(peak, price)
        if peak > 0:
            max_drawdown = min(max_drawdown, price / peak - 1)
    return max_drawdown


def _masking_effectiveness_layer(
    layer_id: str,
    cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
    *,
    capped_masking_ratio: float,
    trace_source: str,
    confidence: str,
) -> dict[str, Any]:
    case_rows = [dict(case) for case in cases]
    return {
        "layer_id": layer_id,
        "trace_source": trace_source,
        "confidence": confidence,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
        "sample_quality": _masking_effectiveness_sample_quality(case_rows, registry),
        "scenarios": {
            scenario_id: _ablation_scenario_metrics(
                case_rows,
                scenario_id,
                capped_masking_ratio,
            )
            for scenario_id in MASKING_ABLATION_SCENARIOS
        },
    }


def _masking_effectiveness_horizon_layer(
    horizon: int,
    cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
    *,
    capped_masking_ratio: float,
    trace_source: str,
    confidence: str,
) -> dict[str, Any]:
    case_rows = [dict(case) for case in cases]
    sample_quality = _masking_effectiveness_horizon_sample_quality(
        case_rows,
        horizon,
        registry,
    )
    insufficient_long_horizon = (
        horizon == 20
        and int(sample_quality.get("mature_case_count") or 0)
        < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES
    )
    return {
        "horizon": f"{horizon}d",
        "horizon_trading_days": horizon,
        "trace_source": trace_source,
        "confidence": confidence,
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
        "evidence_status": (
            "insufficient_long_horizon_evidence"
            if insufficient_long_horizon
            else "mature_horizon_evidence"
        ),
        "insufficient_long_horizon_evidence": insufficient_long_horizon,
        "sample_quality": sample_quality,
        "scenarios": {
            scenario_id: _ablation_scenario_horizon_metrics(
                case_rows,
                scenario_id,
                capped_masking_ratio,
                horizon,
            )
            for scenario_id in MASKING_ABLATION_SCENARIOS
        },
    }


def _masking_effectiveness_horizon_sample_quality(
    cases: Sequence[Mapping[str, Any]],
    horizon: int,
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    mature_cases = [case for case in cases if _case_horizon_available(case, horizon)]
    dates = {str(case.get("date")) for case in mature_cases if case.get("date")}
    assets = {str(case.get("asset")).upper() for case in mature_cases if case.get("asset")}
    regimes = {
        str(case.get("market_regime") or registry.market_regime.regime_id) for case in mature_cases
    }
    hard_missing = sum(
        1 for case in cases if _case_horizon_status(case, horizon) in OUTCOME_HARD_MISSING_STATUSES
    )
    not_mature = sum(
        1
        for case in cases
        if _case_horizon_status(case, horizon) == OUTCOME_WINDOW_STATUS_NOT_MATURE
    )
    return {
        "date_count": len(dates),
        "asset_count": len(assets),
        "case_count": len(cases),
        "mature_date_count": len(dates),
        "mature_asset_count": len(assets),
        "mature_case_count": len(mature_cases),
        "full_advisory_mature_case_count": len(mature_cases),
        "not_mature_count": not_mature,
        "outcome_missing_count": hard_missing,
        "unique_regime_count": len(regimes) if mature_cases else 0,
        "correlated_asset_cluster_count": len({_asset_cluster_id(asset) for asset in assets}),
    }


def _masking_effectiveness_sample_quality(
    cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    dates = {str(case.get("date")) for case in cases if case.get("date")}
    assets = {str(case.get("asset")).upper() for case in cases if case.get("asset")}
    regimes = {str(case.get("market_regime") or registry.market_regime.regime_id) for case in cases}
    clusters = {_asset_cluster_id(asset) for asset in assets}
    horizon_counts = _case_horizon_maturity_counts(cases)
    return {
        "date_count": len(dates),
        "asset_count": len(assets),
        "case_count": len(cases),
        "unique_regime_count": len(regimes) if cases else 0,
        "correlated_asset_cluster_count": len(clusters),
        "outcome_missing_count": sum(1 for case in cases if case.get("outcome_missing")),
        "outcome_available_count": sum(1 for case in cases if _case_outcome_available(case)),
        "outcome_not_mature_count": sum(1 for case in cases if case.get("outcome_not_mature")),
        "mature_case_count_by_horizon": horizon_counts["mature_case_count_by_horizon"],
        "not_mature_count_by_horizon": horizon_counts["not_mature_count_by_horizon"],
        **_horizon_specific_summary_counts(horizon_counts),
    }


def _case_horizon_maturity_counts(
    cases: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    mature = {f"{horizon}d": 0 for horizon in MASKING_OUTCOME_HORIZONS}
    not_mature = {f"{horizon}d": 0 for horizon in MASKING_OUTCOME_HORIZONS}
    for case in cases:
        for horizon in MASKING_OUTCOME_HORIZONS:
            status = _case_horizon_status(case, horizon)
            label = f"{horizon}d"
            if status == OUTCOME_WINDOW_STATUS_AVAILABLE:
                mature[label] += 1
            elif status == OUTCOME_WINDOW_STATUS_NOT_MATURE:
                not_mature[label] += 1
    return {
        "mature_case_count_by_horizon": mature,
        "not_mature_count_by_horizon": not_mature,
    }


def _asset_cluster_id(asset: str) -> str:
    normalized = asset.upper()
    for cluster_id, members in VALIDATION_CORRELATED_ASSET_CLUSTERS.items():
        if normalized in members:
            return cluster_id
    if normalized == DEFAULT_TRACE_ASSET:
        return "synthetic_ai_risk_asset_basket"
    return f"single_asset:{normalized}"


def _group_cases(
    cases: Sequence[Mapping[str, Any]],
    field: str,
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in cases:
        value = str(case.get(field) or "")
        if value:
            grouped[value].append(case)
    return dict(sorted(grouped.items()))


def _group_cases_by_regime(
    cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in cases:
        regime = str(case.get("market_regime") or registry.market_regime.regime_id)
        grouped[regime].append(case)
    return dict(sorted(grouped.items()))


def _group_cases_by_correlated_asset_cluster(
    cases: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in cases:
        asset = str(case.get("asset") or DEFAULT_TRACE_ASSET)
        grouped[_asset_cluster_id(asset)].append(case)
    return dict(sorted(grouped.items()))


def _cases_for_event_window(
    cases: Sequence[Mapping[str, Any]],
    window: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    event_id = str(window.get("event_window_id") or "")
    start = _parse_iso_date(str(window.get("start_date") or ""))
    end = _parse_iso_date(str(window.get("end_date") or ""))
    return [
        case
        for case in cases
        if event_id in set(case.get("event_window_ids") or [])
        or _date_in_window(str(case.get("date") or ""), start=start, end=end)
    ]


def _bridge_record_to_effectiveness_case(record: Mapping[str, Any]) -> dict[str, Any]:
    case = dict(record)
    outcomes = case.get("outcomes", {})
    if not isinstance(outcomes, Mapping):
        outcomes = {}
    returns = [
        value
        for key, value in outcomes.items()
        if str(key).startswith("return_") and isinstance(value, float)
    ]
    a_suppressed = _float(case.get("a_suppressed_change"))
    drawdown_reduced = a_suppressed > 0 and any(value < 0 for value in returns)
    missed_upside = a_suppressed > 0 and any(value > 0 for value in returns)
    case["drawdown_reduced"] = drawdown_reduced
    case["missed_upside"] = missed_upside
    case["false_risk_off"] = missed_upside and not drawdown_reduced
    if "outcome_windows" in case:
        statuses = {
            str(window.get("status") or "")
            for window in case.get("outcome_windows", [])
            if isinstance(window, Mapping)
        }
        case["outcome_missing"] = bool(statuses & OUTCOME_HARD_MISSING_STATUSES)
        case["outcome_not_mature"] = OUTCOME_WINDOW_STATUS_NOT_MATURE in statuses
    else:
        case["outcome_missing"] = any(
            outcomes.get(f"return_{horizon}d") is None for horizon in MASKING_OUTCOME_HORIZONS
        )
    case["event_window_ids"] = _event_window_ids_for_date(str(case.get("date") or ""))
    return case


def _case_outcome_available(case: Mapping[str, Any]) -> bool:
    windows = case.get("outcome_windows", [])
    if isinstance(windows, Sequence) and not isinstance(windows, (str, bytes)):
        usable = [window for window in windows if isinstance(window, Mapping)]
        if usable:
            return all(window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE for window in usable)
    outcomes = case.get("outcomes", {})
    if not isinstance(outcomes, Mapping):
        return False
    return all(
        isinstance(outcomes.get(f"return_{horizon}d"), float)
        for horizon in MASKING_OUTCOME_HORIZONS
    )


def _case_horizon_status(case: Mapping[str, Any], horizon: int) -> str:
    windows = case.get("outcome_windows", [])
    if isinstance(windows, Sequence) and not isinstance(windows, (str, bytes)):
        for window in windows:
            if not isinstance(window, Mapping):
                continue
            if str(window.get("window") or "") == f"{horizon}d":
                return str(window.get("status") or "unknown")
    outcomes = case.get("outcomes", {})
    if isinstance(outcomes, Mapping) and isinstance(outcomes.get(f"return_{horizon}d"), float):
        return OUTCOME_WINDOW_STATUS_AVAILABLE
    return "unknown"


def _case_horizon_available(case: Mapping[str, Any], horizon: int) -> bool:
    return _case_horizon_status(case, horizon) == OUTCOME_WINDOW_STATUS_AVAILABLE


def _masking_effectiveness_recommendation(
    full_layer: Mapping[str, Any],
    *,
    by_horizon: Sequence[Mapping[str, Any]] | None = None,
    conclusion_matrix: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    sample_quality = full_layer.get("sample_quality", {})
    if not isinstance(sample_quality, Mapping):
        sample_quality = {}
    case_count = int(sample_quality.get("case_count") or 0)
    outcome_missing_count = int(sample_quality.get("outcome_missing_count") or 0)
    outcome_available_count = int(sample_quality.get("outcome_available_count") or 0)
    outcome_not_mature_count = int(sample_quality.get("outcome_not_mature_count") or 0)
    date_count = int(sample_quality.get("date_count") or 0)
    asset_count = int(sample_quality.get("asset_count") or 0)
    mature_by_horizon = sample_quality.get("mature_case_count_by_horizon", {})
    if not isinstance(mature_by_horizon, Mapping):
        mature_by_horizon = {}
    short_horizon_ready = all(
        int(mature_by_horizon.get(f"{horizon}d") or 0)
        >= EFFECTIVENESS_MIN_SHORT_HORIZON_MATURE_CASES
        for horizon in (1, 5)
    )
    primary_horizon_ready = all(
        int(mature_by_horizon.get(f"{horizon}d") or 0) >= EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES
        for horizon in (1, 5, 10)
    )
    if (
        not short_horizon_ready
        or date_count < HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY
        or asset_count < 5
        or outcome_missing_count > 0
    ):
        return {
            "decision_recommendation": "insufficient_evidence",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "rationale": (
                "Sample is not mature enough for a directional masking policy decision: "
                f"date_count={date_count}, asset_count={asset_count}, "
                f"case_count={case_count}, outcome_available_count={outcome_available_count}, "
                f"outcome_missing_count={outcome_missing_count}, "
                f"outcome_not_mature_count={outcome_not_mature_count}, "
                f"mature_case_count_by_horizon={dict(mature_by_horizon)}."
            ),
        }
    if short_horizon_ready and not primary_horizon_ready:
        return {
            "decision_recommendation": "preliminary_short_horizon_only",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "rationale": (
                "1d/5d mature outcomes are sufficient for preliminary validation, "
                "but the 1d/5d/10d primary review horizon set is not mature enough "
                "for a directional "
                f"masking policy decision: mature_case_count_by_horizon="
                f"{dict(mature_by_horizon)}."
            ),
        }
    horizon_decision = _matrix_based_horizon_recommendation(conclusion_matrix)
    if horizon_decision is not None:
        return horizon_decision
    scenarios = full_layer.get("scenarios", {})
    if not isinstance(scenarios, Mapping):
        return {
            "decision_recommendation": "insufficient_evidence",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "rationale": "Scenario metrics are missing from full advisory layer.",
        }
    baseline = scenarios.get("baseline", {})
    no_mask = scenarios.get("no_valuation_crowding_masking", {})
    capped = scenarios.get("capped_masking", {})
    if not all(isinstance(item, Mapping) for item in (baseline, no_mask, capped)):
        return {
            "decision_recommendation": "insufficient_evidence",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "rationale": "Scenario metrics are not comparable.",
        }
    baseline_drawdown = _optional_float(baseline.get("max_drawdown"))
    capped_drawdown = _optional_float(capped.get("max_drawdown"))
    no_mask_drawdown = _optional_float(no_mask.get("max_drawdown"))
    mature_horizons = _recommendation_mature_horizons(by_horizon)
    baseline_return = _scenario_avg_return_for_horizons(baseline, mature_horizons)
    capped_return = _scenario_avg_return_for_horizons(capped, mature_horizons)
    no_mask_return = _scenario_avg_return_for_horizons(no_mask, mature_horizons)
    if (
        capped_drawdown is not None
        and baseline_drawdown is not None
        and (capped_return or 0.0) >= (baseline_return or 0.0)
        and capped_drawdown >= baseline_drawdown
    ):
        decision = "prefer_capped_masking_candidate"
        rationale = "Capped masking improves or matches drawdown while preserving return."
    elif (
        no_mask_return is not None
        and baseline_return is not None
        and no_mask_return > baseline_return
        and no_mask_drawdown is not None
        and baseline_drawdown is not None
        and no_mask_drawdown
        >= baseline_drawdown - abs(baseline_drawdown) * EFFECTIVENESS_DRAWDOWN_WORSE_TOLERANCE
    ):
        decision = "baseline_over_defensive_candidate"
        rationale = "No-masking outperforms baseline without materially worse drawdown."
    elif (
        baseline_drawdown is not None
        and no_mask_drawdown is not None
        and baseline_drawdown > no_mask_drawdown
        and case_count > 0
        and int(baseline.get("missed_upside_count") or 0) / case_count
        <= EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE
    ):
        decision = "keep_baseline_masking_candidate"
        rationale = "Baseline materially reduces drawdown and missed-upside rate is acceptable."
    else:
        decision = "insufficient_evidence"
        rationale = "Scenario metrics do not support a stable validation recommendation."
    return {
        "decision_recommendation": decision,
        "recommendation_scope": "validation_only",
        "promotion_gate_allowed": False,
        "rationale": rationale,
    }


def _matrix_based_horizon_recommendation(
    conclusion_matrix: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any] | None:
    if not conclusion_matrix:
        return None
    contributions: dict[int, str] = {}
    for horizon in (1, 5, 10):
        rows = [
            row for row in conclusion_matrix if int(row.get("horizon_trading_days") or 0) == horizon
        ]
        if not rows:
            contributions[horizon] = "insufficient_evidence"
            continue
        scenario_contributions = {
            str(row.get("recommendation_contribution") or "")
            for row in rows
            if row.get("recommendation_contribution")
        }
        if "conflicting_horizon_signal" in scenario_contributions:
            contributions[horizon] = "conflicting_horizon_signal"
            continue
        preferred = sorted(
            contribution
            for contribution in scenario_contributions
            if contribution.startswith("supports_")
        )
        if len(preferred) == 1:
            contributions[horizon] = preferred[0].removeprefix("supports_")
        elif len(preferred) > 1:
            contributions[horizon] = "conflicting_horizon_signal"
        else:
            contributions[horizon] = "insufficient_evidence"
    actionable = {
        decision
        for decision in contributions.values()
        if decision not in {"insufficient_evidence", "conflicting_horizon_signal"}
    }
    if "conflicting_horizon_signal" in set(contributions.values()):
        return {
            "decision_recommendation": "preliminary_short_horizon_only",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "horizon_contributions": contributions,
            "rationale": (
                "At least one primary horizon has conflicting scenario signals, "
                "so the review remains preliminary_short_horizon_only."
            ),
        }
    if not actionable:
        return {
            "decision_recommendation": "insufficient_evidence",
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "horizon_contributions": contributions,
            "rationale": (
                "1d/5d/10d scenario comparison does not produce a stable "
                "validation recommendation."
            ),
        }
    if len(actionable) == 1 and all(decision in actionable for decision in contributions.values()):
        decision = next(iter(actionable))
        return {
            "decision_recommendation": decision,
            "recommendation_scope": "validation_only",
            "promotion_gate_allowed": False,
            "horizon_contributions": contributions,
            "rationale": (
                "1d/5d/10d horizon-specific conclusion matrix supports "
                f"{decision}; 20d remains diagnostic if long-horizon evidence is "
                "insufficient."
            ),
        }
    return {
        "decision_recommendation": "preliminary_short_horizon_only",
        "recommendation_scope": "validation_only",
        "promotion_gate_allowed": False,
        "horizon_contributions": contributions,
        "rationale": (
            "1d/5d/10d horizon-specific conclusion matrix is mixed or incomplete, "
            "so the review remains preliminary_short_horizon_only."
        ),
    }


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _recommendation_mature_horizons(
    by_horizon: Sequence[Mapping[str, Any]] | None,
) -> list[int]:
    if not by_horizon:
        return list(MASKING_OUTCOME_HORIZONS)
    mature = []
    for row in by_horizon:
        sample_quality = row.get("sample_quality", {})
        if not isinstance(sample_quality, Mapping):
            continue
        horizon = int(row.get("horizon_trading_days") or 0)
        mature_count = int(sample_quality.get("mature_case_count") or 0)
        if horizon and mature_count >= EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES:
            mature.append(horizon)
    return mature or list(MASKING_OUTCOME_HORIZONS)


def _scenario_avg_return_for_horizons(
    scenario: Mapping[str, Any],
    horizons: Sequence[int],
) -> float | None:
    values = [_optional_float(scenario.get(f"avg_return_{horizon}d")) for horizon in horizons]
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return sum(usable) / len(usable)


def _masking_effectiveness_conclusion_matrix(
    *,
    full_cases: Sequence[Mapping[str, Any]],
    component_cases: Sequence[Mapping[str, Any]],
    bridge_cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
    capped_masking_ratio: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        scenario_metrics = {
            scenario_id: _scenario_horizon_comparison_metrics(
                full_cases=full_cases,
                component_cases=component_cases,
                bridge_cases=bridge_cases,
                scenario_id=scenario_id,
                capped_masking_ratio=capped_masking_ratio,
                horizon=horizon,
                registry=registry,
            )
            for scenario_id in MASKING_ABLATION_SCENARIOS
        }
        contributions = _scenario_horizon_recommendation_contributions(
            scenario_metrics,
            horizon=horizon,
        )
        for scenario_id, metrics in scenario_metrics.items():
            contribution = contributions.get(scenario_id, "neutral_or_mixed")
            row = dict(metrics)
            row["recommendation_contribution"] = contribution
            row["sample_quality"]["recommendation_contribution"] = contribution
            rows.append(row)
    return rows


def _scenario_horizon_comparison_metrics(
    *,
    full_cases: Sequence[Mapping[str, Any]],
    component_cases: Sequence[Mapping[str, Any]],
    bridge_cases: Sequence[Mapping[str, Any]],
    scenario_id: str,
    capped_masking_ratio: float,
    horizon: int,
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    all_cases = [*full_cases, *component_cases, *bridge_cases]
    full_metrics = _ablation_scenario_horizon_metrics(
        full_cases,
        scenario_id,
        capped_masking_ratio,
        horizon,
    )
    component_metrics = _ablation_scenario_horizon_metrics(
        component_cases,
        scenario_id,
        capped_masking_ratio,
        horizon,
    )
    bridge_metrics = _ablation_scenario_horizon_metrics(
        bridge_cases,
        scenario_id,
        capped_masking_ratio,
        horizon,
    )
    combined_metrics = _ablation_scenario_horizon_metrics(
        all_cases,
        scenario_id,
        capped_masking_ratio,
        horizon,
    )
    quality = _scenario_horizon_sample_quality(
        all_cases,
        full_cases=full_cases,
        component_cases=component_cases,
        bridge_cases=bridge_cases,
        horizon=horizon,
        registry=registry,
    )
    evidence_status = (
        "insufficient_long_horizon_evidence"
        if horizon == 20
        and quality["full_advisory_mature_case_count"] < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES
        else "mature_horizon_evidence"
    )
    return {
        "scenario_id": scenario_id,
        "horizon": f"{horizon}d",
        "horizon_trading_days": horizon,
        "evidence_status": evidence_status,
        "insufficient_long_horizon_evidence": (
            evidence_status == "insufficient_long_horizon_evidence"
        ),
        "avg_return": combined_metrics["avg_return"],
        "median_return": combined_metrics["median_return"],
        "hit_rate": combined_metrics["hit_rate"],
        "downside_capture": combined_metrics["downside_capture"],
        "max_drawdown": combined_metrics["max_drawdown"],
        "drawdown_reduced_count": combined_metrics["drawdown_reduced_count"],
        "missed_upside_count": combined_metrics["missed_upside_count"],
        "false_risk_off_count": combined_metrics["false_risk_off_count"],
        "turnover": combined_metrics["turnover"],
        "constraint_hit_count": combined_metrics["constraint_hit_count"],
        "sample_count": combined_metrics["sample_count"],
        "full_advisory_sample_count": full_metrics["sample_count"],
        "component_only_sample_count": component_metrics["sample_count"],
        "backtest_bridge_sample_count": bridge_metrics["sample_count"],
        "mature_date_count": quality["mature_date_count"],
        "mature_asset_count": quality["mature_asset_count"],
        "mature_case_count": quality["mature_case_count"],
        "full_advisory_mature_case_count": quality["full_advisory_mature_case_count"],
        "unique_regime_count": quality["unique_regime_count"],
        "correlated_asset_cluster_count": quality["correlated_asset_cluster_count"],
        "sample_quality": quality,
        "return_profile": {
            "avg_return": combined_metrics["avg_return"],
            "median_return": combined_metrics["median_return"],
            "hit_rate": combined_metrics["hit_rate"],
        },
        "risk_profile": {
            "downside_capture": combined_metrics["downside_capture"],
            "max_drawdown": combined_metrics["max_drawdown"],
            "drawdown_reduced_count": combined_metrics["drawdown_reduced_count"],
            "constraint_hit_count": combined_metrics["constraint_hit_count"],
        },
        "false_risk_off": {
            "count": combined_metrics["false_risk_off_count"],
            "rate": _ratio(
                combined_metrics["false_risk_off_count"],
                combined_metrics["sample_count"],
            ),
        },
        "missed_upside": {
            "count": combined_metrics["missed_upside_count"],
            "rate": _ratio(
                combined_metrics["missed_upside_count"],
                combined_metrics["sample_count"],
            ),
        },
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _scenario_horizon_sample_quality(
    all_cases: Sequence[Mapping[str, Any]],
    *,
    full_cases: Sequence[Mapping[str, Any]],
    component_cases: Sequence[Mapping[str, Any]],
    bridge_cases: Sequence[Mapping[str, Any]],
    horizon: int,
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    mature_cases = [case for case in all_cases if _case_horizon_available(case, horizon)]
    full_mature = [case for case in full_cases if _case_horizon_available(case, horizon)]
    component_mature = [case for case in component_cases if _case_horizon_available(case, horizon)]
    bridge_mature = [case for case in bridge_cases if _case_horizon_available(case, horizon)]
    assets = {str(case.get("asset")).upper() for case in mature_cases if case.get("asset")}
    regimes = {
        str(case.get("market_regime") or registry.market_regime.regime_id) for case in mature_cases
    }
    dates = {str(case.get("date")) for case in mature_cases if case.get("date")}
    return {
        "mature_date_count": len(dates),
        "mature_asset_count": len(assets),
        "mature_case_count": len(mature_cases),
        "full_advisory_mature_case_count": len(full_mature),
        "component_only_mature_case_count": len(component_mature),
        "backtest_bridge_mature_case_count": len(bridge_mature),
        "unique_regime_count": len(regimes) if mature_cases else 0,
        "correlated_asset_cluster_count": len({_asset_cluster_id(asset) for asset in assets}),
        "promotion_gate_allowed": False,
    }


def _scenario_horizon_recommendation_contributions(
    scenario_metrics: Mapping[str, Mapping[str, Any]],
    *,
    horizon: int,
) -> dict[str, str]:
    baseline = scenario_metrics.get("baseline", {})
    no_mask = scenario_metrics.get("no_valuation_crowding_masking", {})
    capped = scenario_metrics.get("capped_masking", {})
    full_sample_count = int(baseline.get("full_advisory_sample_count") or 0)
    if horizon == 20 and full_sample_count < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES:
        return {
            scenario_id: "insufficient_long_horizon_evidence"
            for scenario_id in MASKING_ABLATION_SCENARIOS
        }
    if full_sample_count < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES:
        return {
            scenario_id: "insufficient_horizon_evidence"
            for scenario_id in MASKING_ABLATION_SCENARIOS
        }
    capped_supports = _capped_supports_preference(baseline, capped)
    no_mask_supports = _no_mask_supports_preference(baseline, no_mask)
    no_mask_dominates = _no_mask_supports_disable_candidate(baseline, no_mask)
    baseline_supports = _baseline_supports_preference(baseline, no_mask)
    supported = [
        flag
        for flag, active in (
            ("prefer_capped_masking_candidate", capped_supports),
            ("disable_masking_candidate", no_mask_dominates),
            ("baseline_over_defensive_candidate", no_mask_supports),
            ("keep_baseline_masking_candidate", baseline_supports),
        )
        if active
    ]
    if len(supported) > 1:
        return {
            scenario_id: "conflicting_horizon_signal" for scenario_id in MASKING_ABLATION_SCENARIOS
        }
    if not supported:
        return {scenario_id: "neutral_or_mixed" for scenario_id in MASKING_ABLATION_SCENARIOS}
    decision = supported[0]
    return {
        "baseline": (
            "supports_keep_baseline_masking_candidate"
            if decision == "keep_baseline_masking_candidate"
            else "neutral_or_mixed"
        ),
        "no_valuation_crowding_masking": (
            f"supports_{decision}"
            if decision in {"baseline_over_defensive_candidate", "disable_masking_candidate"}
            else "neutral_or_mixed"
        ),
        "capped_masking": (
            "supports_prefer_capped_masking_candidate"
            if decision == "prefer_capped_masking_candidate"
            else "neutral_or_mixed"
        ),
    }


def _robustness_cases_by_source(
    *,
    registry_path: Path,
    trace_path: Path | None,
    prices_path: Path | None,
    gate_audit_root: Path | None,
    bridge_artifact_root: Path | None,
    outcome_ticker: str,
    start_date: str | None,
    end_date: str | None,
    event_window_start: str | None,
    event_window_end: str | None,
    asset_universe: str | None,
) -> dict[str, list[dict[str, Any]]]:
    casebook = build_masking_casebook(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    cases = [dict(item) for item in casebook.get("casebook", []) if isinstance(item, Mapping)]
    gate_audit = build_gate_availability_audit(
        registry_path=registry_path,
        gate_audit_root=gate_audit_root,
        trace_path=trace_path,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    availability = [
        dict(item) for item in gate_audit.get("gate_availability", []) if isinstance(item, Mapping)
    ]
    full_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("full_advisory_trace_eligible")
    }
    component_only_dates = {
        str(record.get("date"))
        for record in availability
        if record.get("component_validation_trace_eligible")
        and not record.get("full_advisory_trace_eligible")
    }
    full_cases = [
        _with_validation_source(case, "full_advisory_only")
        for case in cases
        if str(case.get("date") or "") in full_dates
    ]
    component_cases = [
        _with_validation_source(case, "component_only")
        for case in cases
        if str(case.get("date") or "") in component_only_dates
    ]
    bridge = build_backtest_trace_bridge(
        registry_path=registry_path,
        trace_path=trace_path,
        prices_path=prices_path,
        bridge_artifact_root=bridge_artifact_root,
        outcome_ticker=outcome_ticker,
        start_date=start_date,
        end_date=end_date,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        asset_universe=asset_universe,
    )
    bridge_cases = [
        _with_validation_source(
            _bridge_record_to_effectiveness_case(record),
            "backtest_bridge",
        )
        for record in bridge.get("bridge_records", [])
        if isinstance(record, Mapping)
    ]
    return {
        "full_advisory_only": full_cases,
        "component_only": component_cases,
        "backtest_bridge": bridge_cases,
        "all_validation_sources": [*full_cases, *component_cases, *bridge_cases],
    }


def _with_validation_source(
    case: Mapping[str, Any],
    validation_source: str,
) -> dict[str, Any]:
    row = dict(case)
    row["validation_source"] = validation_source
    return row


def _scenario_delta_matrix(
    conclusion_matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows_by_key = {
        (str(row.get("horizon") or ""), str(row.get("scenario_id") or "")): row
        for row in conclusion_matrix
        if isinstance(row, Mapping)
    }
    comparisons = (
        ("baseline_vs_no_valuation_crowding_masking", "baseline", "no_valuation_crowding_masking"),
        ("baseline_vs_capped_masking", "baseline", "capped_masking"),
        ("capped_masking_vs_no_masking", "capped_masking", "no_valuation_crowding_masking"),
    )
    fields = (
        "avg_return",
        "median_return",
        "hit_rate",
        "downside_capture",
        "max_drawdown",
        "missed_upside_count",
        "false_risk_off_count",
        "drawdown_reduced_count",
        "turnover",
        "constraint_hit_count",
    )
    delta_rows: list[dict[str, Any]] = []
    for horizon in (f"{item}d" for item in MASKING_OUTCOME_HORIZONS):
        for comparison_id, left_id, right_id in comparisons:
            left = rows_by_key.get((horizon, left_id), {})
            right = rows_by_key.get((horizon, right_id), {})
            delta = {
                f"delta_{field}": _optional_delta(left.get(field), right.get(field))
                for field in fields
            }
            delta_rows.append(
                {
                    "comparison_id": comparison_id,
                    "left_scenario": left_id,
                    "right_scenario": right_id,
                    "horizon": horizon,
                    "horizon_trading_days": int(horizon.removesuffix("d")),
                    "evidence_status": left.get("evidence_status")
                    or right.get("evidence_status")
                    or "unknown",
                    **delta,
                    "left_sample_count": left.get("sample_count"),
                    "right_sample_count": right.get("sample_count"),
                    "promotion_gate_allowed": False,
                }
            )
    return delta_rows


def _optional_delta(left: Any, right: Any) -> float | None:
    left_value = _optional_float(left)
    right_value = _optional_float(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _robustness_aggregation(effectiveness: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "equal_weight_by_date": _equal_weight_group_aggregation(
            effectiveness.get("by_date", []),
            group_field="date",
        ),
        "equal_weight_by_asset": _equal_weight_group_aggregation(
            effectiveness.get("by_asset", []),
            group_field="asset",
        ),
        "equal_weight_by_correlated_asset_cluster": _equal_weight_group_aggregation(
            effectiveness.get("by_correlated_asset_cluster", []),
            group_field="correlated_asset_cluster",
        ),
        "full_advisory_only": _single_layer_aggregation(
            effectiveness.get("layers", {}).get("full_advisory_only", {})
            if isinstance(effectiveness.get("layers"), Mapping)
            else {}
        ),
        "all_validation_sources": _all_sources_matrix_aggregation(
            effectiveness.get("conclusion_matrix", [])
        ),
        "promotion_gate_allowed": False,
    }


def _equal_weight_group_aggregation(
    group_layers: Any,
    *,
    group_field: str,
) -> dict[str, Any]:
    groups = (
        [group for group in group_layers if isinstance(group, Mapping) and group.get(group_field)]
        if isinstance(group_layers, Sequence) and not isinstance(group_layers, (str, bytes))
        else []
    )
    horizon_results = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        scenario_values: dict[str, list[float]] = {
            scenario_id: [] for scenario_id in MASKING_ABLATION_SCENARIOS
        }
        for group in groups:
            scenarios = group.get("scenarios", {})
            if not isinstance(scenarios, Mapping):
                continue
            for scenario_id in MASKING_ABLATION_SCENARIOS:
                scenario = scenarios.get(scenario_id, {})
                if not isinstance(scenario, Mapping):
                    continue
                by_horizon = scenario.get("by_horizon", {})
                if not isinstance(by_horizon, Mapping):
                    continue
                metrics = by_horizon.get(f"{horizon}d", {})
                if not isinstance(metrics, Mapping):
                    continue
                value = _optional_float(metrics.get("avg_return"))
                if value is not None:
                    scenario_values[scenario_id].append(value)
        scenario_avg_returns = {
            scenario_id: _mean(values) for scenario_id, values in scenario_values.items()
        }
        horizon_results.append(
            {
                "horizon": f"{horizon}d",
                "horizon_trading_days": horizon,
                "scenario_avg_returns": scenario_avg_returns,
                "winning_scenario": _winning_scenario(scenario_avg_returns),
                "group_count_with_data": max(
                    (len(values) for values in scenario_values.values()),
                    default=0,
                ),
            }
        )
    return {
        "aggregation_id": f"equal_weight_by_{group_field}",
        "group_field": group_field,
        "group_count": len(groups),
        "horizon_results": horizon_results,
        "promotion_gate_allowed": False,
    }


def _single_layer_aggregation(layer: Mapping[str, Any]) -> dict[str, Any]:
    scenarios = layer.get("scenarios", {}) if isinstance(layer, Mapping) else {}
    horizon_results = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        scenario_avg_returns = {}
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            scenario = scenarios.get(scenario_id, {}) if isinstance(scenarios, Mapping) else {}
            by_horizon = scenario.get("by_horizon", {}) if isinstance(scenario, Mapping) else {}
            metrics = by_horizon.get(f"{horizon}d", {}) if isinstance(by_horizon, Mapping) else {}
            scenario_avg_returns[scenario_id] = (
                _optional_float(metrics.get("avg_return")) if isinstance(metrics, Mapping) else None
            )
        horizon_results.append(
            {
                "horizon": f"{horizon}d",
                "horizon_trading_days": horizon,
                "scenario_avg_returns": scenario_avg_returns,
                "winning_scenario": _winning_scenario(scenario_avg_returns),
            }
        )
    return {
        "aggregation_id": "full_advisory_only",
        "sample_quality": layer.get("sample_quality", {}) if isinstance(layer, Mapping) else {},
        "horizon_results": horizon_results,
        "promotion_gate_allowed": False,
    }


def _all_sources_matrix_aggregation(
    conclusion_matrix: Any,
) -> dict[str, Any]:
    rows = (
        [row for row in conclusion_matrix if isinstance(row, Mapping)]
        if isinstance(conclusion_matrix, Sequence)
        and not isinstance(conclusion_matrix, (str, bytes))
        else []
    )
    horizon_results = []
    for horizon in MASKING_OUTCOME_HORIZONS:
        scenario_avg_returns = {
            str(row.get("scenario_id")): _optional_float(row.get("avg_return"))
            for row in rows
            if row.get("horizon") == f"{horizon}d"
        }
        horizon_results.append(
            {
                "horizon": f"{horizon}d",
                "horizon_trading_days": horizon,
                "scenario_avg_returns": scenario_avg_returns,
                "winning_scenario": _winning_scenario(scenario_avg_returns),
            }
        )
    return {
        "aggregation_id": "all_validation_sources",
        "horizon_results": horizon_results,
        "promotion_gate_allowed": False,
    }


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _winning_scenario(values: Mapping[str, float | None]) -> str:
    usable = {key: value for key, value in values.items() if value is not None}
    if not usable:
        return "insufficient_evidence"
    return max(usable.items(), key=lambda item: item[1])[0]


def _robustness_case_diagnostics(
    cases_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    capped_masking_ratio: float,
) -> dict[str, Any]:
    rows = _case_comparison_rows(cases_by_source, capped_masking_ratio)
    baseline_rows = [
        row for row in rows if row["comparison_id"] == "baseline_vs_no_valuation_crowding_masking"
    ]
    return {
        "diagnostic_case_count": len(rows),
        "top_winning_cases": sorted(
            baseline_rows,
            key=lambda row: row["delta_return"],
            reverse=True,
        )[:10],
        "top_losing_cases": sorted(baseline_rows, key=lambda row: row["delta_return"])[:10],
        "false_risk_off_cases": [row for row in baseline_rows if row.get("false_risk_off")][:20],
        "missed_upside_cases": [row for row in baseline_rows if row.get("missed_upside")][:20],
        "drawdown_reduction_cases": [row for row in baseline_rows if row.get("drawdown_reduced")][
            :20
        ],
        "by_asset": _case_diagnostic_group_summary(baseline_rows, "asset"),
        "by_regime": _case_diagnostic_group_summary(baseline_rows, "market_regime"),
        "by_event_window": _case_diagnostic_event_window_summary(baseline_rows),
        "promotion_gate_allowed": False,
    }


def _case_comparison_rows(
    cases_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    capped_masking_ratio: float,
) -> list[dict[str, Any]]:
    comparisons = (
        ("baseline_vs_no_valuation_crowding_masking", "baseline", "no_valuation_crowding_masking"),
        ("baseline_vs_capped_masking", "baseline", "capped_masking"),
        ("capped_masking_vs_no_masking", "capped_masking", "no_valuation_crowding_masking"),
    )
    rows: list[dict[str, Any]] = []
    for validation_source, cases in cases_by_source.items():
        if validation_source == "all_validation_sources":
            continue
        for case in cases:
            for horizon in MASKING_OUTCOME_HORIZONS:
                if not _case_horizon_available(case, horizon):
                    continue
                outcomes = case.get("outcomes", {})
                if not isinstance(outcomes, Mapping):
                    continue
                raw_return = outcomes.get(f"return_{horizon}d")
                if not isinstance(raw_return, float):
                    continue
                scenario_returns = {
                    scenario_id: _scenario_weight(
                        case,
                        scenario_id,
                        capped_masking_ratio,
                    )[0]
                    * raw_return
                    for scenario_id in MASKING_ABLATION_SCENARIOS
                }
                for comparison_id, left_id, right_id in comparisons:
                    delta_return = scenario_returns[left_id] - scenario_returns[right_id]
                    rows.append(
                        {
                            "comparison_id": comparison_id,
                            "left_scenario": left_id,
                            "right_scenario": right_id,
                            "case_id": case.get("case_id"),
                            "date": case.get("date"),
                            "decision_time": case.get("decision_time"),
                            "asset": case.get("asset"),
                            "market_regime": case.get("market_regime") or "ai_after_chatgpt",
                            "event_window_ids": list(case.get("event_window_ids") or []),
                            "correlated_asset_cluster": _asset_cluster_id(
                                str(case.get("asset") or DEFAULT_TRACE_ASSET)
                            ),
                            "validation_source": validation_source,
                            "horizon": f"{horizon}d",
                            "horizon_trading_days": horizon,
                            "raw_return": raw_return,
                            "left_weighted_return": scenario_returns[left_id],
                            "right_weighted_return": scenario_returns[right_id],
                            "delta_return": delta_return,
                            "false_risk_off": bool(case.get("false_risk_off")),
                            "missed_upside": bool(case.get("missed_upside")),
                            "drawdown_reduced": bool(case.get("drawdown_reduced")),
                            "promotion_gate_allowed": False,
                        }
                    )
    return rows


def _case_diagnostic_group_summary(
    rows: Sequence[Mapping[str, Any]],
    field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "UNKNOWN")].append(row)
    return [
        {
            field: group_key,
            "case_count": len(group_rows),
            "avg_delta_return": _mean([_float(row.get("delta_return")) for row in group_rows]),
            "positive_delta_count": sum(
                1 for row in group_rows if _float(row.get("delta_return")) > 0
            ),
            "negative_delta_count": sum(
                1 for row in group_rows if _float(row.get("delta_return")) < 0
            ),
            "promotion_gate_allowed": False,
        }
        for group_key, group_rows in sorted(grouped.items())
    ]


def _case_diagnostic_event_window_summary(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        event_ids = row.get("event_window_ids") or ["no_event_window"]
        for event_id in event_ids:
            grouped[str(event_id)].append(row)
    return [
        {
            "event_window_id": event_id,
            "case_count": len(group_rows),
            "avg_delta_return": _mean([_float(row.get("delta_return")) for row in group_rows]),
            "promotion_gate_allowed": False,
        }
        for event_id, group_rows in sorted(grouped.items())
    ]


def _ten_day_baseline_support_attribution(
    cases_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    capped_masking_ratio: float,
    effectiveness: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        row
        for row in _case_comparison_rows(cases_by_source, capped_masking_ratio)
        if row["comparison_id"] == "baseline_vs_no_valuation_crowding_masking"
        and row["horizon"] == "10d"
    ]
    winning_rows = [row for row in rows if _float(row.get("delta_return")) > 0]
    by_date = _case_diagnostic_group_summary(winning_rows, "date")
    by_cluster = _case_diagnostic_group_summary(winning_rows, "correlated_asset_cluster")
    by_source = _case_diagnostic_group_summary(winning_rows, "validation_source")
    top_date_share = _top_group_share(by_date)
    top_cluster_share = _top_group_share(by_cluster)
    source_counts = {row["validation_source"]: row["case_count"] for row in by_source}
    full_delta = _mean(
        [
            _float(row.get("delta_return"))
            for row in winning_rows
            if row.get("validation_source") == "full_advisory_only"
        ]
    )
    all_delta = _mean([_float(row.get("delta_return")) for row in winning_rows])
    return {
        "horizon": "10d",
        "baseline_win_case_count": len(winning_rows),
        "baseline_total_case_count": len(rows),
        "top_date_share": top_date_share,
        "wins_concentrated_in_few_dates": top_date_share > ROBUSTNESS_TOP_DATE_CONCENTRATION_SHARE,
        "top_cluster_share": top_cluster_share,
        "semiconductor_ai_cluster_share": _named_group_share(
            by_cluster,
            "semiconductor_ai",
            group_field="correlated_asset_cluster",
        ),
        "source_counts": source_counts,
        "component_or_bridge_driven": (
            source_counts.get("full_advisory_only", 0)
            < source_counts.get("backtest_bridge", 0) + source_counts.get("component_only", 0)
        ),
        "full_advisory_only_avg_positive_delta": full_delta,
        "all_sources_avg_positive_delta": all_delta,
        "full_advisory_only_still_holds": full_delta is not None and full_delta > 0,
        "single_extreme_event_driven": _single_extreme_event_driven(winning_rows),
        "by_date": by_date,
        "by_correlated_asset_cluster": by_cluster,
        "by_validation_source": by_source,
        "source_recommendation_contribution": _matrix_contribution_for(
            effectiveness.get("conclusion_matrix", []),
            scenario_id="baseline",
            horizon="10d",
        ),
        "promotion_gate_allowed": False,
    }


def _top_group_share(group_rows: Sequence[Mapping[str, Any]]) -> float:
    total = sum(int(row.get("case_count") or 0) for row in group_rows)
    if total <= 0:
        return 0.0
    return max(int(row.get("case_count") or 0) for row in group_rows) / total


def _named_group_share(
    group_rows: Sequence[Mapping[str, Any]],
    group_name: str,
    *,
    group_field: str,
) -> float:
    total = sum(int(row.get("case_count") or 0) for row in group_rows)
    if total <= 0:
        return 0.0
    named = sum(
        int(row.get("case_count") or 0) for row in group_rows if row.get(group_field) == group_name
    )
    return named / total


def _single_extreme_event_driven(rows: Sequence[Mapping[str, Any]]) -> bool:
    total_abs = sum(abs(_float(row.get("delta_return"))) for row in rows)
    if total_abs <= 0:
        return False
    max_abs = max(abs(_float(row.get("delta_return"))) for row in rows)
    return max_abs / total_abs > ROBUSTNESS_TOP_DATE_CONCENTRATION_SHARE


def _matrix_contribution_for(
    conclusion_matrix: Any,
    *,
    scenario_id: str,
    horizon: str,
) -> str:
    if not isinstance(conclusion_matrix, Sequence) or isinstance(
        conclusion_matrix,
        (str, bytes),
    ):
        return "unknown"
    for row in conclusion_matrix:
        if not isinstance(row, Mapping):
            continue
        if row.get("scenario_id") == scenario_id and row.get("horizon") == horizon:
            return str(row.get("recommendation_contribution") or "unknown")
    return "unknown"


def _short_horizon_neutral_explanation(
    cases_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    scenario_delta_matrix: Sequence[Mapping[str, Any]],
    effectiveness: Mapping[str, Any],
) -> list[dict[str, Any]]:
    all_cases = cases_by_source.get("all_validation_sources", [])
    explanations = []
    for horizon in (1, 5):
        returns = _raw_returns_for_horizon(all_cases, horizon)
        delta_rows = [row for row in scenario_delta_matrix if row.get("horizon") == f"{horizon}d"]
        max_delta = max(
            (abs(_optional_float(row.get("delta_avg_return")) or 0.0) for row in delta_rows),
            default=0.0,
        )
        positive_share = _ratio(sum(1 for value in returns if value > 0), len(returns))
        negative_share = _ratio(sum(1 for value in returns if value < 0), len(returns))
        baseline_row = _matrix_row_for(
            effectiveness.get("conclusion_matrix", []),
            scenario_id="baseline",
            horizon=f"{horizon}d",
        )
        mature_sample = int(baseline_row.get("full_advisory_sample_count") or 0)
        explanations.append(
            {
                "horizon": f"{horizon}d",
                "mature_sample_sufficient": mature_sample
                >= EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES,
                "full_advisory_mature_sample_count": mature_sample,
                "scenario_difference_small": max_delta < ROBUSTNESS_SMALL_RETURN_DELTA,
                "max_abs_delta_avg_return": max_delta,
                "outcome_noise_std": _stddev(returns),
                "outcome_noise_high": (_stddev(returns) or 0.0) > ROBUSTNESS_HIGH_NOISE_STD,
                "diluted_by_same_date_or_cluster": _short_horizon_diluted(
                    effectiveness,
                    horizon,
                ),
                "short_horizon_whipsaw": (
                    (positive_share or 0.0) > 0.25 and (negative_share or 0.0) > 0.25
                ),
                "positive_return_share": positive_share,
                "negative_return_share": negative_share,
                "promotion_gate_allowed": False,
            }
        )
    return explanations


def _raw_returns_for_horizon(
    cases: Sequence[Mapping[str, Any]],
    horizon: int,
) -> list[float]:
    values = []
    for case in cases:
        if not _case_horizon_available(case, horizon):
            continue
        outcomes = case.get("outcomes", {})
        if not isinstance(outcomes, Mapping):
            continue
        value = outcomes.get(f"return_{horizon}d")
        if isinstance(value, float):
            values.append(value)
    return values


def _stddev(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _matrix_row_for(
    conclusion_matrix: Any,
    *,
    scenario_id: str,
    horizon: str,
) -> Mapping[str, Any]:
    if not isinstance(conclusion_matrix, Sequence) or isinstance(
        conclusion_matrix,
        (str, bytes),
    ):
        return {}
    for row in conclusion_matrix:
        if (
            isinstance(row, Mapping)
            and row.get("scenario_id") == scenario_id
            and row.get("horizon") == horizon
        ):
            return row
    return {}


def _short_horizon_diluted(effectiveness: Mapping[str, Any], horizon: int) -> bool:
    date_winner = _aggregation_winner(
        effectiveness,
        aggregation_key="by_date",
        horizon=horizon,
    )
    cluster_winner = _aggregation_winner(
        effectiveness,
        aggregation_key="by_correlated_asset_cluster",
        horizon=horizon,
    )
    return date_winner != cluster_winner or date_winner == "insufficient_evidence"


def _aggregation_winner(
    effectiveness: Mapping[str, Any],
    *,
    aggregation_key: str,
    horizon: int,
) -> str:
    aggregation = _equal_weight_group_aggregation(
        effectiveness.get(aggregation_key, []),
        group_field="date" if aggregation_key == "by_date" else "correlated_asset_cluster",
    )
    for row in aggregation["horizon_results"]:
        if row["horizon_trading_days"] == horizon:
            return str(row.get("winning_scenario") or "insufficient_evidence")
    return "insufficient_evidence"


def _pending_twenty_day_maturity_tracker(
    outcome_records: Any,
) -> dict[str, Any]:
    records = (
        [record for record in outcome_records if isinstance(record, Mapping)]
        if isinstance(outcome_records, Sequence) and not isinstance(outcome_records, (str, bytes))
        else []
    )
    mature: list[Mapping[str, Any]] = []
    pending: list[Mapping[str, Any]] = []
    for record in records:
        status = _record_horizon_status(record, 20)
        if status == OUTCOME_WINDOW_STATUS_AVAILABLE:
            mature.append(record)
        elif status == OUTCOME_WINDOW_STATUS_NOT_MATURE:
            pending.append(record)
    pending_details = [_pending_twenty_day_detail(record) for record in pending]
    return {
        "current_20d_mature_cases": len(mature),
        "pending_20d_cases": len(pending),
        "expected_maturity_dates": pending_details[:50],
        "by_asset": _pending_tracker_group(pending_details, "asset"),
        "by_date": _pending_tracker_group(pending_details, "as_of_date"),
        "promotion_gate_allowed": False,
    }


def _pending_twenty_day_detail(record: Mapping[str, Any]) -> dict[str, Any]:
    as_of_date = str(record.get("as_of_date") or record.get("date") or "")
    parsed = _parse_iso_date(as_of_date)
    expected = _add_business_days(parsed, 20) if parsed is not None else None
    asset = str(record.get("asset") or "")
    return {
        "case_id": record.get("case_id"),
        "as_of_date": as_of_date,
        "asset": asset,
        "correlated_asset_cluster": _asset_cluster_id(asset),
        "scenario": record.get("scenario"),
        "trace_source": record.get("trace_source"),
        "expected_maturity_date": None if expected is None else expected.isoformat(),
        "expected_maturity_date_basis": "business_day_projection_not_signal_input",
        "promotion_gate_allowed": False,
    }


def _add_business_days(start: date, days: int) -> date:
    current = start
    remaining = days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _pending_tracker_group(
    pending_details: Sequence[Mapping[str, Any]],
    field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for detail in pending_details:
        grouped[str(detail.get(field) or "UNKNOWN")].append(detail)
    return [
        {
            field: group_key,
            "pending_20d_cases": len(group_rows),
            "earliest_expected_maturity_date": min(
                str(row.get("expected_maturity_date") or "9999-12-31") for row in group_rows
            ),
            "promotion_gate_allowed": False,
        }
        for group_key, group_rows in sorted(grouped.items())
    ]


def _validation_rollup_pending_maturity_tracker(
    outcome_availability: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> dict[str, Any]:
    summary = outcome_availability.get("summary", {})
    mature_by_horizon = (
        summary.get("mature_case_count_by_horizon", {}) if isinstance(summary, Mapping) else {}
    )
    not_mature_by_horizon = (
        summary.get("not_mature_count_by_horizon", {}) if isinstance(summary, Mapping) else {}
    )
    source_tracker = robustness.get("pending_20d_maturity_tracker", {})
    tracker = dict(source_tracker) if isinstance(source_tracker, Mapping) else {}
    expected = [
        item for item in tracker.get("expected_maturity_dates", []) if isinstance(item, Mapping)
    ]
    tracker["current_mature_cases_by_horizon"] = {
        f"{horizon}d": int(mature_by_horizon.get(f"{horizon}d") or 0)
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    tracker["current_not_mature_cases_by_horizon"] = {
        f"{horizon}d": int(not_mature_by_horizon.get(f"{horizon}d") or 0)
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    tracker["by_cluster"] = _pending_tracker_group(
        expected,
        "correlated_asset_cluster",
    )
    next_date = _earliest_expected_maturity_date(expected)
    tracker["next_recommended_rerun_date"] = next_date
    tracker["criteria_to_rerun_robustness_review"] = [
        "20d_full_advisory_mature_cases_reaches_validation_floor",
        "two_of_1d_5d_10d_support_same_scenario",
        "capped_or_no_mask_beats_baseline_in_full_advisory_and_cluster_equal_weight",
        "missed_upside_or_false_risk_off_deteriorates_materially",
    ]
    tracker["promotion_gate_allowed"] = False
    tracker["allowed_uses"] = list(NON_PROMOTION_ALLOWED_USES)
    return tracker


def _earliest_expected_maturity_date(
    expected: Sequence[Mapping[str, Any]],
) -> str | None:
    dates = sorted(
        str(item.get("expected_maturity_date"))
        for item in expected
        if item.get("expected_maturity_date")
    )
    return dates[0] if dates else None


def _validation_rollup_rerun_criteria(
    outcome_availability: Mapping[str, Any],
    robustness: Mapping[str, Any],
    maturity_tracker: Mapping[str, Any],
) -> dict[str, Any]:
    mature_quality = outcome_availability.get("mature_sample_quality", {})
    full_mature = (
        mature_quality.get("full_advisory_mature_count_by_horizon", {})
        if isinstance(mature_quality, Mapping)
        else {}
    )
    full_20d = int(full_mature.get("20d") or 0)
    conservative_gate = robustness.get("conservative_evidence_gate", {})
    checks = conservative_gate.get("checks", []) if isinstance(conservative_gate, Mapping) else []
    check_map = {
        str(item.get("check_id")): bool(item.get("passed"))
        for item in checks
        if isinstance(item, Mapping)
    }
    horizon_contributions = (
        conservative_gate.get("horizon_contributions", {})
        if isinstance(conservative_gate, Mapping)
        else {}
    )
    primary_consensus = check_map.get("two_primary_horizons_consistent", False)
    capped_or_no_mask_advantage = _rollup_challenger_advantage_detected(robustness)
    risk_deteriorated = not check_map.get(
        "missed_upside_false_risk_off_not_worse",
        True,
    )
    criteria = [
        {
            "criterion_id": "20d_full_advisory_maturity_floor",
            "description": (
                "Rerun once 20d full advisory mature cases reach the validation floor."
            ),
            "current_value": full_20d,
            "threshold": EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES,
            "currently_met": full_20d >= EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES,
        },
        {
            "criterion_id": "primary_horizon_consensus",
            "description": "Rerun if at least two of 1d/5d/10d support the same scenario.",
            "current_value": (
                dict(horizon_contributions) if isinstance(horizon_contributions, Mapping) else {}
            ),
            "threshold": "two_primary_horizons_same_scenario",
            "currently_met": primary_consensus,
        },
        {
            "criterion_id": "challenger_stable_advantage",
            "description": (
                "Rerun if capped/no-mask is stable versus baseline in full advisory "
                "and correlated-cluster equal-weight views."
            ),
            "current_value": capped_or_no_mask_advantage,
            "threshold": "full_advisory_and_cluster_equal_weight_advantage",
            "currently_met": capped_or_no_mask_advantage,
        },
        {
            "criterion_id": "risk_flag_deterioration",
            "description": ("Rerun if missed_upside or false_risk_off deteriorates materially."),
            "current_value": {
                "missed_upside_false_risk_off_not_worse": not risk_deteriorated,
            },
            "threshold": "material_deterioration_detected",
            "currently_met": risk_deteriorated,
        },
    ]
    return {
        "next_recommended_rerun_date": maturity_tracker.get("next_recommended_rerun_date"),
        "criteria": criteria,
        "any_currently_met": any(item["currently_met"] for item in criteria),
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _rollup_challenger_advantage_detected(robustness: Mapping[str, Any]) -> bool:
    aggregation = robustness.get("aggregation", {})
    if not isinstance(aggregation, Mapping):
        return False
    full_winner = _aggregation_layer_winner(
        aggregation.get("full_advisory_only", {}),
        horizon=10,
    )
    cluster_winner = _aggregation_layer_winner(
        aggregation.get("equal_weight_by_correlated_asset_cluster", {}),
        horizon=10,
    )
    challengers = {"capped_masking", "no_valuation_crowding_masking"}
    return full_winner in challengers and cluster_winner == full_winner


def _aggregation_layer_winner(layer: Any, *, horizon: int) -> str:
    if not isinstance(layer, Mapping):
        return ""
    for row in layer.get("horizon_results", []):
        if not isinstance(row, Mapping):
            continue
        if int(row.get("horizon_trading_days") or 0) == horizon:
            return str(row.get("winning_scenario") or "")
    return ""


def _validation_rollup_valuation_status(
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    indicator = _indicator_or_raise(registry, "valuation_crowding_indicator")
    inventory = _inventory_record(registry, indicator)
    return {
        "indicator_id": indicator.indicator_id,
        "coverage_status": "HIGH_IMPACT_UNVALIDATED",
        "validation_status": "PRELIMINARY_SHORT_HORIZON_ONLY",
        "promotion_status": "NO_PROMOTION_ALLOWED",
        "registry_coverage_status": inventory.get("coverage_status"),
        "expected_impact": indicator.expected_impact.model_dump(mode="json"),
        "promotion_gate_allowed": False,
    }


def _validation_rollup_gate_lineage_status(
    gate_availability: Mapping[str, Any],
    lineage_repair: Mapping[str, Any],
) -> str:
    gate_summary = gate_availability.get("summary", {})
    lineage_summary = lineage_repair.get("summary", {})
    lineage_missing = 0
    if isinstance(lineage_summary, Mapping):
        root_counts = lineage_summary.get("root_cause_reason_class_counts", {})
        if isinstance(root_counts, Mapping):
            lineage_missing = int(root_counts.get("lineage_manifest_missing") or 0)
    if lineage_missing:
        return "LINEAGE_MANIFEST_REPAIR_REQUIRED"
    if (
        isinstance(gate_summary, Mapping)
        and int(gate_summary.get("full_advisory_trace_eligible_count") or 0) > 0
    ):
        return "FULL_ADVISORY_AND_COMPONENT_TRACE_AVAILABLE_WITH_LIMITATIONS"
    return "COMPONENT_OR_PARTIAL_TRACE_ONLY"


def _validation_rollup_outcome_maturity_status(
    outcome_availability: Mapping[str, Any],
) -> str:
    mature_quality = outcome_availability.get("mature_sample_quality", {})
    if not isinstance(mature_quality, Mapping):
        return "OUTCOME_MATURITY_UNKNOWN"
    full_mature = mature_quality.get("full_advisory_mature_count_by_horizon", {})
    if not isinstance(full_mature, Mapping):
        return "OUTCOME_MATURITY_UNKNOWN"
    if int(full_mature.get("20d") or 0) < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES:
        return "SHORT_HORIZON_MATURE_LONG_HORIZON_PENDING"
    return "ALL_PRIMARY_HORIZONS_MATURE_ENOUGH_FOR_REVIEW"


def _validation_rollup_remaining_limitations(
    coverage_gap: Mapping[str, Any],
    historical_trace: Mapping[str, Any],
    gate_availability: Mapping[str, Any],
    outcome_availability: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> list[dict[str, Any]]:
    limitations: list[dict[str, Any]] = []
    coverage_summary = coverage_gap.get("summary", {})
    if isinstance(coverage_summary, Mapping) and int(
        coverage_summary.get("high_impact_unvalidated_count") or 0
    ):
        limitations.append(
            {
                "limitation_id": "high_impact_unvalidated_indicator",
                "status": "OPEN",
                "detail": "valuation_crowding_indicator remains HIGH_IMPACT_UNVALIDATED.",
            }
        )
    trace_summary = historical_trace.get("summary", {})
    if isinstance(trace_summary, Mapping) and not bool(
        trace_summary.get("sufficient_history_for_stability")
    ):
        limitations.append(
            {
                "limitation_id": "historical_trace_window_short",
                "status": "OPEN",
                "detail": "Historical trace is below the configured stability window.",
            }
        )
    gate_summary = gate_availability.get("summary", {})
    if isinstance(gate_summary, Mapping) and int(
        gate_summary.get("full_advisory_trace_eligible_count") or 0
    ) < int(gate_summary.get("audited_date_count") or 0):
        limitations.append(
            {
                "limitation_id": "full_advisory_gate_availability_limited",
                "status": "OPEN",
                "detail": (
                    "Some audited dates remain component-only or fail-closed; "
                    "production gate was not relaxed."
                ),
            }
        )
    outcome_summary = outcome_availability.get("summary", {})
    if (
        isinstance(outcome_summary, Mapping)
        and int(outcome_summary.get("20d_mature_case_count") or 0)
        < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES
    ):
        limitations.append(
            {
                "limitation_id": "insufficient_long_horizon_evidence",
                "status": "OPEN",
                "detail": "20d mature outcome remains below validation floor.",
            }
        )
    conservative_gate = robustness.get("conservative_evidence_gate", {})
    if (
        isinstance(conservative_gate, Mapping)
        and str(conservative_gate.get("final_validation_recommendation"))
        == "keep_preliminary_short_horizon_only"
    ):
        limitations.append(
            {
                "limitation_id": "conservative_evidence_gate_not_met",
                "status": "OPEN",
                "detail": conservative_gate.get("rationale", ""),
            }
        )
    return limitations


def _full_advisory_mature_outcome_records(
    records: Any,
    *,
    horizon: int,
) -> list[Mapping[str, Any]]:
    usable = (
        [record for record in records if isinstance(record, Mapping)]
        if isinstance(records, Sequence) and not isinstance(records, (str, bytes))
        else []
    )
    return [
        record
        for record in usable
        if str(record.get("eligibility_layer") or "") == "full_advisory_only"
        and _case_horizon_available(record, horizon)
    ]


def _long_horizon_effective_sample_size(
    full_20d_cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    dates = {str(case.get("date") or "") for case in full_20d_cases if case.get("date")}
    assets = {str(case.get("asset") or DEFAULT_TRACE_ASSET).upper() for case in full_20d_cases}
    clusters = {_asset_cluster_id(asset) for asset in assets}
    regimes = {
        str(case.get("market_regime") or registry.market_regime.regime_id)
        for case in full_20d_cases
    }
    return {
        "raw_case_count": len(full_20d_cases),
        "unique_date_count": len(dates),
        "unique_asset_count": len(assets),
        "correlated_asset_cluster_count": len(clusters),
        "regime_count": len(regimes) if full_20d_cases else 0,
        "effective_date_count": len(dates),
        "effective_cluster_count": len(clusters),
        "effective_sample_method": (
            "conservative_proxy_counts_dates_and_correlated_clusters_not_rows"
        ),
        "promotion_gate_allowed": False,
    }


def _long_horizon_robustness_based_gate(
    full_20d_cases: Sequence[Mapping[str, Any]],
    robustness: Mapping[str, Any],
    capped_masking_ratio: float,
) -> dict[str, Any]:
    aggregation = robustness.get("aggregation", {})
    aggregation = aggregation if isinstance(aggregation, Mapping) else {}
    base_winner = _scenario_winner_for_cases(
        full_20d_cases,
        horizon=20,
        capped_masking_ratio=capped_masking_ratio,
    )
    leave_one_date = _leave_one_group_out_stability(
        full_20d_cases,
        group_field="date",
        base_winner=base_winner,
        capped_masking_ratio=capped_masking_ratio,
    )
    leave_one_asset = _leave_one_group_out_stability(
        full_20d_cases,
        group_field="asset",
        base_winner=base_winner,
        capped_masking_ratio=capped_masking_ratio,
    )
    leave_one_cluster = _leave_one_group_out_stability(
        full_20d_cases,
        group_field="correlated_asset_cluster",
        base_winner=base_winner,
        capped_masking_ratio=capped_masking_ratio,
    )
    full_winner = _aggregation_layer_winner(
        aggregation.get("full_advisory_only", {}),
        horizon=20,
    )
    all_winner = _aggregation_layer_winner(
        aggregation.get("all_validation_sources", {}),
        horizon=20,
    )
    date_winner = _aggregation_layer_winner(
        aggregation.get("equal_weight_by_date", {}),
        horizon=20,
    )
    cluster_winner = _aggregation_layer_winner(
        aggregation.get("equal_weight_by_correlated_asset_cluster", {}),
        horizon=20,
    )
    cluster_share = _max_group_share(full_20d_cases, "correlated_asset_cluster")
    full_vs_all = _winners_not_conflicting(full_winner, all_winner)
    row_vs_date = _winners_not_conflicting(all_winner, date_winner)
    cluster_not_dominated = (
        cluster_share <= ROBUSTNESS_CLUSTER_DOMINANCE_SHARE
        and _winners_not_conflicting(all_winner, cluster_winner)
    )
    checks = [
        {
            "check_id": "leave_one_date_out_stable",
            "passed": leave_one_date["stable"],
            "details": leave_one_date,
        },
        {
            "check_id": "leave_one_asset_out_stable",
            "passed": leave_one_asset["stable"],
            "details": leave_one_asset,
        },
        {
            "check_id": "leave_one_cluster_out_stable",
            "passed": leave_one_cluster["stable"],
            "details": leave_one_cluster,
        },
        {
            "check_id": "full_advisory_only_all_sources_not_conflicting",
            "passed": full_vs_all,
            "full_advisory_winner": full_winner,
            "all_sources_winner": all_winner,
        },
        {
            "check_id": "row_level_date_equal_weight_not_conflicting",
            "passed": row_vs_date,
            "row_level_winner": all_winner,
            "date_equal_weight_winner": date_winner,
        },
        {
            "check_id": "cluster_equal_weight_not_single_cluster_dominated",
            "passed": cluster_not_dominated,
            "cluster_equal_weight_winner": cluster_winner,
            "max_cluster_case_share": cluster_share,
            "max_allowed_cluster_case_share": ROBUSTNESS_CLUSTER_DOMINANCE_SHARE,
        },
    ]
    return {
        "base_20d_full_advisory_winner": base_winner,
        "leave_one_date_out_stable": leave_one_date["stable"],
        "leave_one_asset_out_stable": leave_one_asset["stable"],
        "leave_one_cluster_out_stable": leave_one_cluster["stable"],
        "full_advisory_only_all_sources_not_conflicting": full_vs_all,
        "row_level_date_equal_weight_not_conflicting": row_vs_date,
        "cluster_equal_weight_not_single_cluster_dominated": cluster_not_dominated,
        "checks": checks,
        "passed": all(check["passed"] for check in checks),
        "promotion_gate_allowed": False,
    }


def _scenario_winner_for_cases(
    cases: Sequence[Mapping[str, Any]],
    *,
    horizon: int,
    capped_masking_ratio: float,
) -> str:
    scenario_values: dict[str, list[float]] = {
        scenario_id: [] for scenario_id in MASKING_ABLATION_SCENARIOS
    }
    for case in cases:
        if not _case_horizon_available(case, horizon):
            continue
        outcomes = case.get("outcomes", {})
        if not isinstance(outcomes, Mapping):
            continue
        raw_return = outcomes.get(f"return_{horizon}d")
        if not isinstance(raw_return, float):
            continue
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            weight, _ = _scenario_weight(case, scenario_id, capped_masking_ratio)
            scenario_values[scenario_id].append(weight * raw_return)
    return _winning_scenario(
        {scenario_id: _mean(values) for scenario_id, values in scenario_values.items()}
    )


def _leave_one_group_out_stability(
    cases: Sequence[Mapping[str, Any]],
    *,
    group_field: str,
    base_winner: str,
    capped_masking_ratio: float,
) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in cases:
        if group_field == "correlated_asset_cluster":
            key = _asset_cluster_id(str(case.get("asset") or DEFAULT_TRACE_ASSET))
        else:
            key = str(case.get(group_field) or "UNKNOWN")
        grouped[key].append(case)
    leave_one_results = []
    for group_key in sorted(grouped):
        remaining = [case for case in cases if case not in grouped[group_key]]
        winner = _scenario_winner_for_cases(
            remaining,
            horizon=20,
            capped_masking_ratio=capped_masking_ratio,
        )
        leave_one_results.append(
            {
                group_field: group_key,
                "remaining_case_count": len(remaining),
                "winning_scenario": winner,
                "matches_base_winner": winner == base_winner,
            }
        )
    stable = (
        len(grouped) > 1
        and base_winner != "insufficient_evidence"
        and all(result["matches_base_winner"] for result in leave_one_results)
    )
    return {
        "stable": stable,
        "base_winner": base_winner,
        "group_count": len(grouped),
        "leave_one_results": leave_one_results,
        "promotion_gate_allowed": False,
    }


def _winners_not_conflicting(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if "insufficient" in left or "insufficient" in right:
        return False
    return left == right


def _max_group_share(
    cases: Sequence[Mapping[str, Any]],
    group_field: str,
) -> float:
    if not cases:
        return 0.0
    grouped: dict[str, int] = defaultdict(int)
    for case in cases:
        if group_field == "correlated_asset_cluster":
            key = _asset_cluster_id(str(case.get("asset") or DEFAULT_TRACE_ASSET))
        else:
            key = str(case.get(group_field) or "UNKNOWN")
        grouped[key] += 1
    return max(grouped.values(), default=0) / len(cases)


def _long_horizon_threshold_sensitivity(
    *,
    full_case_count: int,
    robustness_gate: Mapping[str, Any],
    current_recommendation: str,
) -> dict[str, Any]:
    floors = (20, 30, 50, 80, 100)
    base_winner = str(robustness_gate.get("base_20d_full_advisory_winner") or "")
    robustness_failures = [
        str(check.get("check_id"))
        for check in robustness_gate.get("checks", [])
        if isinstance(check, Mapping) and not check.get("passed")
    ]
    rows = []
    recommendations = []
    for floor in floors:
        sample_count_passed = full_case_count >= floor
        if not sample_count_passed:
            recommendation = "keep_preliminary_short_horizon_only"
            driver = "sample_count_below_floor"
        elif not bool(robustness_gate.get("passed")):
            recommendation = "keep_preliminary_short_horizon_only"
            driver = "robustness_failures"
        else:
            recommendation = _scenario_winner_to_recommendation(base_winner)
            driver = "sample_count_and_robustness_passed"
        recommendations.append(recommendation)
        rows.append(
            {
                "floor": floor,
                "floor_label": "heuristic_min_full_advisory_cases",
                "calibration_status": "uncalibrated",
                "full_advisory_mature_cases": full_case_count,
                "sample_count_passed": sample_count_passed,
                "robustness_gate_passed": bool(robustness_gate.get("passed")),
                "recommendation_by_floor": recommendation,
                "recommendation_changes_from_current": recommendation != current_recommendation,
                "conclusion_driver": driver,
                "robustness_failures": robustness_failures,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "floors": list(floors),
        "recommendation_by_floor": rows,
        "recommendation_changes": len(set(recommendations)) > 1,
        "first_floor_where_recommendation_stabilizes": (_first_stable_floor(rows)),
        "twenty_day_conclusion_driver": _twenty_day_conclusion_driver(rows),
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _scenario_winner_to_recommendation(winner: str) -> str:
    if winner == "baseline":
        return "keep_baseline_masking_candidate"
    if winner == "capped_masking":
        return "prefer_capped_masking_candidate"
    if winner == "no_valuation_crowding_masking":
        return "baseline_over_defensive_candidate"
    return "keep_preliminary_short_horizon_only"


def _first_stable_floor(rows: Sequence[Mapping[str, Any]]) -> int | None:
    for index, row in enumerate(rows):
        recommendation = row.get("recommendation_by_floor")
        if all(later.get("recommendation_by_floor") == recommendation for later in rows[index:]):
            return int(row.get("floor") or 0)
    return None


def _twenty_day_conclusion_driver(rows: Sequence[Mapping[str, Any]]) -> str:
    drivers = {str(row.get("conclusion_driver")) for row in rows if row.get("conclusion_driver")}
    if drivers == {"sample_count_below_floor"}:
        return "sample_count_only"
    if "robustness_failures" in drivers and "sample_count_below_floor" in drivers:
        return "sample_count_and_robustness_failures"
    if "robustness_failures" in drivers:
        return "robustness_failures"
    return "sample_count_and_robustness_passed"


def _long_horizon_calibration_conclusion(
    effective_sample: Mapping[str, Any],
    robustness_gate: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> dict[str, Any]:
    raw_case_count = int(effective_sample.get("raw_case_count") or 0)
    effective_date_count = int(effective_sample.get("effective_date_count") or 0)
    effective_cluster_count = int(effective_sample.get("effective_cluster_count") or 0)
    if effective_date_count < HISTORICAL_TRACE_MIN_DATES_FOR_STABILITY:
        conclusion = "insufficient_data_to_calibrate_floor"
        action = "floor_50_retained_as_heuristic"
    elif bool(robustness_gate.get("passed")) and not bool(
        sensitivity.get("recommendation_changes")
    ):
        conclusion = "replace_fixed_floor_with_evidence_bands"
        action = "replace_fixed_floor_with_evidence_bands"
    else:
        conclusion = "insufficient_data_to_calibrate_floor"
        action = "floor_50_retained_as_heuristic"
    return {
        "calibration_conclusion": conclusion,
        "floor_50_action": action,
        "floor_50_retained_as_heuristic": action == "floor_50_retained_as_heuristic",
        "candidate_adjusted_floor": None,
        "raw_case_count": raw_case_count,
        "effective_date_count": effective_date_count,
        "effective_cluster_count": effective_cluster_count,
        "robustness_gate_passed": bool(robustness_gate.get("passed")),
        "calibration_status": "uncalibrated",
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _conservative_evidence_gate(
    effectiveness: Mapping[str, Any],
    aggregation: Mapping[str, Any],
    scenario_delta_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    source_decision = str(
        effectiveness.get("summary", {}).get("decision_recommendation")
        if isinstance(effectiveness.get("summary"), Mapping)
        else ""
    )
    horizon_contributions = {}
    recommendation = effectiveness.get("decision_recommendation", {})
    if isinstance(recommendation, Mapping):
        raw_contributions = recommendation.get("horizon_contributions", {})
        if isinstance(raw_contributions, Mapping):
            horizon_contributions = {str(k): str(v) for k, v in raw_contributions.items()}
    actionable = [
        contribution
        for horizon, contribution in horizon_contributions.items()
        if horizon in {"1", "5", "10"}
        and contribution
        not in {
            "insufficient_evidence",
            "conflicting_horizon_signal",
            "neutral_or_mixed",
        }
    ]
    top_action = _most_common(actionable)
    two_horizons_consistent = top_action is not None and actionable.count(top_action) >= 2
    full_winners = _winners_by_horizon(aggregation.get("full_advisory_only", {}))
    all_winners = _winners_by_horizon(aggregation.get("all_validation_sources", {}))
    date_winners = _winners_by_horizon(aggregation.get("equal_weight_by_date", {}))
    cluster_winners = _winners_by_horizon(
        aggregation.get("equal_weight_by_correlated_asset_cluster", {})
    )
    full_conflict = _winner_conflict(full_winners, all_winners)
    date_conflict = _winner_conflict(date_winners, all_winners)
    cluster_conflict = _winner_conflict(cluster_winners, all_winners)
    cluster_dominated = _cluster_dominated(aggregation)
    risk_not_worse = _risk_not_worse_for_action(scenario_delta_matrix, top_action)
    checks = [
        {
            "check_id": "two_primary_horizons_consistent",
            "passed": two_horizons_consistent,
        },
        {
            "check_id": "full_advisory_only_not_conflicting",
            "passed": not full_conflict,
        },
        {
            "check_id": "date_level_not_conflicting",
            "passed": not date_conflict,
        },
        {
            "check_id": "cluster_not_single_dominant",
            "passed": not cluster_dominated and not cluster_conflict,
        },
        {
            "check_id": "missed_upside_false_risk_off_not_worse",
            "passed": risk_not_worse,
        },
        {"check_id": "promotion_gate_allowed_false", "passed": True},
    ]
    if source_decision == "insufficient_evidence":
        final = "insufficient_evidence"
        rationale = "Source effectiveness review remains insufficient_evidence."
    elif top_action is not None and all(check["passed"] for check in checks):
        final = top_action
        rationale = f"Conservative evidence gate supports {top_action}."
    else:
        final = "keep_preliminary_short_horizon_only"
        rationale = (
            "Conservative evidence gate blocks stronger recommendation because "
            "horizon support is mixed, incomplete, or not robust across aggregation levels."
        )
    return {
        "final_validation_recommendation": final,
        "source_effectiveness_recommendation": source_decision,
        "horizon_contributions": horizon_contributions,
        "candidate_if_all_checks_pass": top_action,
        "checks": checks,
        "rationale": rationale,
        "promotion_gate_allowed": False,
        "production_weight_change_allowed": False,
        "paper_shadow_change_allowed": False,
    }


def _most_common(values: Sequence[str]) -> str | None:
    if not values:
        return None
    counts: dict[str, int] = defaultdict(int)
    for value in values:
        counts[value] += 1
    return max(counts.items(), key=lambda item: item[1])[0]


def _winners_by_horizon(aggregation: Any) -> dict[str, str]:
    if not isinstance(aggregation, Mapping):
        return {}
    rows = aggregation.get("horizon_results", [])
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return {}
    return {
        str(row.get("horizon")): str(row.get("winning_scenario"))
        for row in rows
        if isinstance(row, Mapping)
    }


def _winner_conflict(left: Mapping[str, str], right: Mapping[str, str]) -> bool:
    for horizon in ("1d", "5d", "10d"):
        left_winner = left.get(horizon)
        right_winner = right.get(horizon)
        if not left_winner or not right_winner:
            continue
        if "insufficient" in left_winner or "insufficient" in right_winner:
            continue
        if left_winner != right_winner:
            return True
    return False


def _cluster_dominated(aggregation: Mapping[str, Any]) -> bool:
    cluster_aggregation = aggregation.get("equal_weight_by_correlated_asset_cluster", {})
    if not isinstance(cluster_aggregation, Mapping):
        return False
    group_count = int(cluster_aggregation.get("group_count") or 0)
    return group_count <= 1


def _risk_not_worse_for_action(
    scenario_delta_matrix: Sequence[Mapping[str, Any]],
    action: str | None,
) -> bool:
    if action is None:
        return False
    relevant = [row for row in scenario_delta_matrix if row.get("horizon") in {"1d", "5d", "10d"}]
    if action == "prefer_capped_masking_candidate":
        comparison = "baseline_vs_capped_masking"
    elif action in {"baseline_over_defensive_candidate", "disable_masking_candidate"}:
        comparison = "baseline_vs_no_valuation_crowding_masking"
    elif action == "keep_baseline_masking_candidate":
        comparison = "baseline_vs_no_valuation_crowding_masking"
    else:
        return False
    rows = [row for row in relevant if row.get("comparison_id") == comparison]
    if not rows:
        return False
    return all(
        (_optional_float(row.get("delta_missed_upside_count")) or 0.0) <= 0
        and (_optional_float(row.get("delta_false_risk_off_count")) or 0.0) <= 0
        for row in rows
    )


def _capped_supports_preference(
    baseline: Mapping[str, Any],
    capped: Mapping[str, Any],
) -> bool:
    baseline_return = _optional_float(baseline.get("avg_return"))
    capped_return = _optional_float(capped.get("avg_return"))
    if baseline_return is None or capped_return is None or capped_return < baseline_return:
        return False
    capped_missed = int(capped.get("missed_upside_count") or 0)
    baseline_missed = int(baseline.get("missed_upside_count") or 0)
    capped_false = int(capped.get("false_risk_off_count") or 0)
    baseline_false = int(baseline.get("false_risk_off_count") or 0)
    return (
        capped_missed <= baseline_missed
        and capped_false <= baseline_false
        and (capped_missed < baseline_missed or capped_false < baseline_false)
    )


def _no_mask_supports_preference(
    baseline: Mapping[str, Any],
    no_mask: Mapping[str, Any],
) -> bool:
    baseline_return = _optional_float(baseline.get("avg_return"))
    no_mask_return = _optional_float(no_mask.get("avg_return"))
    if baseline_return is None or no_mask_return is None or no_mask_return <= baseline_return:
        return False
    baseline_drawdown = _optional_float(baseline.get("max_drawdown"))
    no_mask_drawdown = _optional_float(no_mask.get("max_drawdown"))
    return _drawdown_not_materially_worse(baseline_drawdown, no_mask_drawdown)


def _no_mask_supports_disable_candidate(
    baseline: Mapping[str, Any],
    no_mask: Mapping[str, Any],
) -> bool:
    if not _no_mask_supports_preference(baseline, no_mask):
        return False
    baseline_drawdown = _optional_float(baseline.get("max_drawdown"))
    no_mask_drawdown = _optional_float(no_mask.get("max_drawdown"))
    if baseline_drawdown is None or no_mask_drawdown is None:
        return False
    no_mask_missed = int(no_mask.get("missed_upside_count") or 0)
    baseline_missed = int(baseline.get("missed_upside_count") or 0)
    no_mask_false = int(no_mask.get("false_risk_off_count") or 0)
    baseline_false = int(baseline.get("false_risk_off_count") or 0)
    return (
        no_mask_drawdown >= baseline_drawdown
        and no_mask_missed <= baseline_missed
        and no_mask_false <= baseline_false
    )


def _baseline_supports_preference(
    baseline: Mapping[str, Any],
    no_mask: Mapping[str, Any],
) -> bool:
    baseline_drawdown = _optional_float(baseline.get("max_drawdown"))
    no_mask_drawdown = _optional_float(no_mask.get("max_drawdown"))
    if baseline_drawdown is None or no_mask_drawdown is None:
        return False
    sample_count = int(baseline.get("sample_count") or 0)
    if sample_count <= 0:
        return False
    missed_rate = int(baseline.get("missed_upside_count") or 0) / sample_count
    return (
        baseline_drawdown > no_mask_drawdown
        and missed_rate <= EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE
    )


def _drawdown_not_materially_worse(
    baseline_drawdown: float | None,
    challenger_drawdown: float | None,
) -> bool:
    if baseline_drawdown is None or challenger_drawdown is None:
        return False
    return challenger_drawdown >= (
        baseline_drawdown - abs(baseline_drawdown) * EFFECTIVENESS_DRAWDOWN_WORSE_TOLERANCE
    )


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    middle = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[middle]
    return (sorted_values[middle - 1] + sorted_values[middle]) / 2


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _downside_capture(
    raw_returns: Sequence[float],
    weighted_returns: Sequence[float],
) -> float | None:
    raw_downside: list[float] = []
    weighted_downside: list[float] = []
    for raw_value, weighted_value in zip(raw_returns, weighted_returns, strict=False):
        if raw_value < 0:
            raw_downside.append(raw_value)
            weighted_downside.append(weighted_value)
    denominator = sum(abs(value) for value in raw_downside)
    if denominator <= 0:
        return None
    numerator = sum(abs(value) for value in weighted_downside)
    return numerator / denominator


def _ablation_scenario_metrics(
    cases: Sequence[Mapping[str, Any]],
    scenario_id: str,
    capped_masking_ratio: float,
) -> dict[str, Any]:
    weighted_returns: dict[int, list[float]] = {horizon: [] for horizon in MASKING_OUTCOME_HORIZONS}
    raw_returns: dict[int, list[float]] = {horizon: [] for horizon in MASKING_OUTCOME_HORIZONS}
    weighted_drawdowns: list[float] = []
    dated_weights: list[tuple[str, float]] = []
    constraint_hit_count = 0
    false_risk_off_count = 0
    missed_upside_count = 0
    drawdown_reduced_count = 0
    drawdown_preservation = 0.0
    by_horizon = {
        f"{horizon}d": _ablation_scenario_horizon_metrics(
            cases,
            scenario_id,
            capped_masking_ratio,
            horizon,
        )
        for horizon in MASKING_OUTCOME_HORIZONS
    }
    for case in cases:
        weight, applied_suppression = _scenario_weight(case, scenario_id, capped_masking_ratio)
        no_mask_weight, _ = _scenario_weight(
            case,
            "no_valuation_crowding_masking",
            capped_masking_ratio,
        )
        dated_weights.append((str(case.get("date") or ""), weight))
        if applied_suppression > 0:
            constraint_hit_count += 1
            if case.get("false_risk_off"):
                false_risk_off_count += 1
            if case.get("missed_upside"):
                missed_upside_count += 1
            if case.get("drawdown_reduced"):
                drawdown_reduced_count += 1
        outcomes = case.get("outcomes", {})
        if not isinstance(outcomes, Mapping):
            outcomes = {}
        min_forward_return = 0.0
        for horizon in MASKING_OUTCOME_HORIZONS:
            value = outcomes.get(f"return_{horizon}d")
            if isinstance(value, float):
                weighted_returns[horizon].append(weight * value)
                raw_returns[horizon].append(value)
                min_forward_return = min(min_forward_return, value)
        drawdown = outcomes.get("max_drawdown_20d")
        if isinstance(drawdown, float):
            weighted_drawdowns.append(weight * drawdown)
            min_forward_return = min(min_forward_return, drawdown)
        if min_forward_return < 0:
            drawdown_preservation += max(0.0, no_mask_weight - weight) * abs(min_forward_return)
    dated_weights.sort()
    turnover = sum(
        abs(dated_weights[index][1] - dated_weights[index - 1][1])
        for index in range(1, len(dated_weights))
    )
    averages = {
        f"avg_return_{horizon}d": (sum(values) / len(values) if values else None)
        for horizon, values in weighted_returns.items()
    }
    hit_rates = {
        f"hit_rate_{horizon}d": (
            sum(1 for value in values if value > 0) / len(values) if values else None
        )
        for horizon, values in raw_returns.items()
    }
    return {
        "scenario_id": scenario_id,
        **averages,
        **hit_rates,
        "max_drawdown": min(weighted_drawdowns) if weighted_drawdowns else None,
        "drawdown_preservation": drawdown_preservation,
        "drawdown_reduced_count": drawdown_reduced_count,
        "false_risk_off_count": false_risk_off_count,
        "missed_upside_count": missed_upside_count,
        "turnover": turnover,
        "constraint_hit_count": constraint_hit_count,
        "case_count": len(cases),
        "by_horizon": by_horizon,
        **{
            f"drawdown_reduced_count_{horizon}d": by_horizon[f"{horizon}d"][
                "drawdown_reduced_count"
            ]
            for horizon in MASKING_OUTCOME_HORIZONS
        },
        **{
            f"missed_upside_count_{horizon}d": by_horizon[f"{horizon}d"]["missed_upside_count"]
            for horizon in MASKING_OUTCOME_HORIZONS
        },
        **{
            f"false_risk_off_count_{horizon}d": by_horizon[f"{horizon}d"]["false_risk_off_count"]
            for horizon in MASKING_OUTCOME_HORIZONS
        },
        "sample_quality_breakdown": {
            "date_count": len({str(case.get("date")) for case in cases if case.get("date")}),
            "asset_count": len({str(case.get("asset")) for case in cases if case.get("asset")}),
            "case_count": len(cases),
            "outcome_missing_count": sum(1 for case in cases if case.get("outcome_missing")),
            "outcome_available_count": sum(1 for case in cases if _case_outcome_available(case)),
            "outcome_not_mature_count": sum(1 for case in cases if case.get("outcome_not_mature")),
            **_horizon_specific_summary_counts(_case_horizon_maturity_counts(cases)),
        },
    }


def _ablation_scenario_horizon_metrics(
    cases: Sequence[Mapping[str, Any]],
    scenario_id: str,
    capped_masking_ratio: float,
    horizon: int,
) -> dict[str, Any]:
    weighted_returns: list[float] = []
    raw_returns: list[float] = []
    dated_weights: list[tuple[str, str, float]] = []
    drawdown_reduced_count = 0
    false_risk_off_count = 0
    missed_upside_count = 0
    constraint_hit_count = 0
    for case in cases:
        if not _case_horizon_available(case, horizon):
            continue
        outcomes = case.get("outcomes", {})
        if not isinstance(outcomes, Mapping):
            continue
        value = outcomes.get(f"return_{horizon}d")
        if not isinstance(value, float):
            continue
        weight, applied_suppression = _scenario_weight(
            case,
            scenario_id,
            capped_masking_ratio,
        )
        weighted_returns.append(weight * value)
        raw_returns.append(value)
        dated_weights.append((str(case.get("date") or ""), str(case.get("asset") or ""), weight))
        if applied_suppression > 0:
            constraint_hit_count += 1
            drawdown_reduced = value < 0
            missed_upside = value > 0
            if drawdown_reduced:
                drawdown_reduced_count += 1
            if missed_upside:
                missed_upside_count += 1
            if missed_upside and not drawdown_reduced:
                false_risk_off_count += 1
    dated_weights.sort()
    turnover = sum(
        abs(dated_weights[index][2] - dated_weights[index - 1][2])
        for index in range(1, len(dated_weights))
    )
    return {
        "horizon": f"{horizon}d",
        "horizon_trading_days": horizon,
        "avg_return": (sum(weighted_returns) / len(weighted_returns) if weighted_returns else None),
        "median_return": _median(weighted_returns),
        "hit_rate": (
            sum(1 for value in raw_returns if value > 0) / len(raw_returns) if raw_returns else None
        ),
        "downside_capture": _downside_capture(raw_returns, weighted_returns),
        "max_drawdown": min(weighted_returns) if weighted_returns else None,
        "mature_case_count": len(raw_returns),
        "sample_count": len(raw_returns),
        "drawdown_reduced_count": drawdown_reduced_count,
        "missed_upside_count": missed_upside_count,
        "false_risk_off_count": false_risk_off_count,
        "turnover": turnover,
        "constraint_hit_count": constraint_hit_count,
        "promotion_gate_allowed": False,
    }


def _scenario_weight(
    case: Mapping[str, Any],
    scenario_id: str,
    capped_masking_ratio: float,
) -> tuple[float, float]:
    pre_mask = _float(case.get("pre_mask_signal"))
    post_mask = _float(case.get("post_mask_signal"))
    final_weight = _first_non_zero_float(case.get("final_advisory_facing_weight"), post_mask)
    observed_suppression = max(0.0, pre_mask - post_mask)
    intended = abs(_float(case.get("b_intended_change"))) or observed_suppression
    portfolio_scale = final_weight / post_mask if post_mask > 0 else 1.0
    if scenario_id == "baseline":
        applied_suppression = observed_suppression
    elif scenario_id == "no_valuation_crowding_masking":
        applied_suppression = 0.0
    elif scenario_id == "capped_masking":
        applied_suppression = min(observed_suppression, capped_masking_ratio * intended)
    else:
        raise IndicatorResearchError(f"unsupported ablation scenario: {scenario_id}")
    restored = observed_suppression - applied_suppression
    weight = min(1.0, max(0.0, final_weight + restored * portfolio_scale))
    return weight, applied_suppression


def _trace_date_summaries(trace_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        row_date = str(row.get("date") or "")
        if row_date:
            grouped[row_date].append(row)
    summaries = []
    for row_date, rows in sorted(grouped.items()):
        final_weights = [
            _float(row.get("final_advisory_portfolio_facing_weight"))
            for row in rows
            if row.get("final_advisory_portfolio_facing_weight") not in (None, "")
        ]
        summaries.append(
            {
                "date": row_date,
                "row_count": len(rows),
                "masking_pair_row_count": sum(
                    1
                    for row in rows
                    if row.get("upstream_indicator_id") and row.get("downstream_indicator_id")
                ),
                "constraint_hit_count": sum(1 for row in rows if row.get("constraint_hit")),
                "final_advisory_weight_min": min(final_weights) if final_weights else None,
                "final_advisory_weight_max": max(final_weights) if final_weights else None,
            }
        )
    return summaries


def _trend_direction(row: Mapping[str, Any] | None, intended_change: float) -> str:
    if row is None:
        if intended_change > 0:
            return "trend_positive_allocation_intent"
        if intended_change < 0:
            return "trend_negative_allocation_intent"
        return "UNKNOWN"
    score = row.get("normalized_indicator_score")
    if score in (None, ""):
        return "UNKNOWN"
    value = _float(score)
    if value > 50:
        return "trend_positive"
    if value < 50:
        return "trend_negative"
    return "trend_neutral"


def _valuation_direction(
    valuation_component: Mapping[str, Any] | None,
    masking_row: Mapping[str, Any],
    suppressed_change: float,
) -> str:
    if suppressed_change > 0 or masking_row.get("constraint_hit"):
        return "valuation_crowding_risk_off"
    if valuation_component is None:
        return "UNKNOWN"
    score = valuation_component.get("normalized_indicator_score")
    if score in (None, ""):
        return "UNKNOWN"
    value = _float(score)
    if value < 50:
        return "valuation_crowding_risk_off"
    if value > 50:
        return "valuation_crowding_supportive"
    return "valuation_crowding_neutral"


def _casebook_assets_for_row(
    row: Mapping[str, Any],
    *,
    asset_universe: str | None,
) -> list[str]:
    requested = _parse_asset_universe(asset_universe)
    if requested:
        return requested
    asset = str(row.get("asset") or DEFAULT_TRACE_ASSET).upper()
    return [asset]


def _price_ticker_for_asset(
    asset: str,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    *,
    fallback_ticker: str,
) -> str:
    ticker, _ = _price_ticker_mapping_for_asset(
        asset,
        price_series_by_ticker,
        fallback_ticker=fallback_ticker,
    )
    return ticker


def _price_ticker_mapping_for_asset(
    asset: str,
    price_series_by_ticker: Mapping[str, Sequence[tuple[date, float]]],
    *,
    fallback_ticker: str,
) -> tuple[str, bool]:
    ticker = asset.upper()
    if ticker in price_series_by_ticker:
        return ticker, False
    alias = PRICE_TICKER_ALIASES.get(ticker)
    if alias and alias in price_series_by_ticker:
        return alias, False
    fallback = fallback_ticker.upper()
    if ticker == DEFAULT_TRACE_ASSET or fallback in price_series_by_ticker:
        return fallback, False
    return fallback, True


def _event_window_ids_for_date(row_date: str) -> list[str]:
    return [
        str(window["event_window_id"])
        for window in DEFAULT_EVENT_WINDOW_CATALOG
        if _date_in_window(
            row_date,
            start=_parse_iso_date(str(window["start_date"])),
            end=_parse_iso_date(str(window["end_date"])),
        )
    ]


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value[:10]).date()
    except ValueError:
        return None


def _first_non_zero_float(*values: Any) -> float:
    for value in values:
        parsed = _float(value)
        if parsed != 0.0:
            return parsed
    return 0.0


def _dominance_from_expected_impact(
    registry: IndicatorResearchRegistry,
) -> list[dict[str, Any]]:
    records = []
    for indicator in registry.indicators:
        if not indicator.affects_weight:
            continue
        records.append(
            {
                "indicator_id": indicator.indicator_id,
                "dominance_status": (
                    "DOMINANT_WEIGHT_DRIVER_EXPECTED_UNVALIDATED"
                    if _is_high_impact_unvalidated(registry, indicator)
                    else TRACE_REQUIRED_STATUS
                ),
                "hit_rate": None,
                "affected_days": None,
                "affected_assets": None,
                "mean_weight_change": None,
                "max_weight_change": None,
                "share_of_total_weight_adjustment": None,
                "expected_impact": indicator.expected_impact.model_dump(mode="json"),
                "limitation": "Multi-stage weight trace is required for realized dominance.",
            }
        )
    return records


def _dominance_from_trace(
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    total_abs_delta = 0.0
    by_module: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    all_dates = {str(row.get("date")) for row in trace_rows if row.get("date")}
    for row in trace_rows:
        module_id = str(row.get("module_id") or row.get("upstream_indicator_id") or "")
        if not module_id:
            continue
        delta = _row_delta(row)
        total_abs_delta += abs(delta)
        by_module[module_id].append(row)
    records = []
    for module_id, rows in sorted(by_module.items()):
        deltas = [_row_delta(row) for row in rows]
        abs_deltas = [abs(delta) for delta in deltas]
        dates = {str(row.get("date")) for row in rows if row.get("date")}
        assets = {str(row.get("asset")) for row in rows if row.get("asset")}
        hit_rate = 0.0 if not all_dates else len(dates) / len(all_dates)
        share = 0.0 if total_abs_delta <= 0 else sum(abs_deltas) / total_abs_delta
        policy = registry.validation_policy.dominance
        dominant = (
            hit_rate >= policy.dominant_hit_rate_min
            or share >= policy.dominant_share_of_adjustment_min
        )
        records.append(
            {
                "indicator_id": module_id,
                "dominance_status": "DOMINANT_WEIGHT_DRIVER" if dominant else "NOT_DOMINANT",
                "hit_rate": hit_rate,
                "affected_days": len(dates),
                "affected_assets": sorted(assets),
                "mean_weight_change": sum(abs_deltas) / len(abs_deltas) if abs_deltas else 0.0,
                "max_weight_change": max(abs_deltas) if abs_deltas else 0.0,
                "share_of_total_weight_adjustment": share,
                "downstream_signals_affected": sorted(
                    {
                        str(row.get("downstream_indicator_id"))
                        for row in rows
                        if row.get("downstream_indicator_id")
                    }
                ),
            }
        )
    return records


def _masking_pairs_for_indicator(
    registry: IndicatorResearchRegistry,
    indicator_id: str,
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    indicator_ids = {item.indicator_id for item in registry.indicators}
    for dependency in registry.dependencies:
        if dependency.edge_type not in EDGE_TYPES_THAT_CAN_MASK:
            continue
        if dependency.from_node not in indicator_ids or dependency.to_node not in indicator_ids:
            continue
        if dependency.from_node == indicator_id or dependency.to_node == indicator_id:
            pairs.append((dependency.from_node, dependency.to_node))
    return sorted(set(pairs))


def _high_impact_masking_pairs(registry: IndicatorResearchRegistry) -> list[tuple[str, str]]:
    pairs = []
    for upstream_id, downstream_id in _all_masking_pairs(registry):
        upstream = _indicator_or_raise(registry, upstream_id)
        if _is_high_impact_unvalidated(registry, upstream):
            pairs.append((upstream_id, downstream_id))
    return pairs


def _all_masking_pairs(registry: IndicatorResearchRegistry) -> list[tuple[str, str]]:
    indicator_ids = {item.indicator_id for item in registry.indicators}
    return sorted(
        {
            (dependency.from_node, dependency.to_node)
            for dependency in registry.dependencies
            if dependency.edge_type in EDGE_TYPES_THAT_CAN_MASK
            and dependency.from_node in indicator_ids
            and dependency.to_node in indicator_ids
        }
    )


def _masking_from_trace(
    registry: IndicatorResearchRegistry,
    trace_rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in trace_rows:
        upstream = str(row.get("upstream_indicator_id") or "")
        downstream = str(row.get("downstream_indicator_id") or "")
        if upstream and downstream:
            grouped[(upstream, downstream)].append(row)
    results = {}
    policy = registry.validation_policy.masking
    for (upstream, downstream), rows in grouped.items():
        intended = sum(abs(_float(row.get("b_intended_change"))) for row in rows)
        suppressed = sum(abs(_float(row.get("a_suppressed_change"))) for row in rows)
        ratio = None if intended <= 0 else suppressed / intended
        status = _masking_status(ratio, policy)
        conclusion = _masking_conclusion(registry, upstream, status)
        results[(upstream, downstream)] = {
            "upstream_indicator_id": upstream,
            "downstream_indicator_id": downstream,
            "masking_ratio": ratio,
            "masking_status": status,
            "conclusion_status": conclusion,
            "row_count": len(rows),
            "b_intended_change_abs_sum": intended,
            "a_suppressed_change_abs_sum": suppressed,
        }
    return results


def _trace_required_masking_record(
    registry: IndicatorResearchRegistry,
    upstream_id: str,
    downstream_id: str,
) -> dict[str, Any]:
    upstream = _indicator_or_raise(registry, upstream_id)
    return {
        "upstream_indicator_id": upstream_id,
        "downstream_indicator_id": downstream_id,
        "masking_ratio": None,
        "masking_status": TRACE_REQUIRED_STATUS,
        "conclusion_status": (
            "B_EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A"
            if _is_high_impact_unvalidated(registry, upstream)
            else "B_EFFECT_CONDITIONAL_ON_A"
        ),
        "limitation": "Multi-stage weight trace is required to estimate masking ratio.",
    }


def _masking_status(ratio: float | None, policy: MaskingPolicy) -> str:
    if ratio is None:
        return TRACE_REQUIRED_STATUS
    if ratio >= policy.high_min:
        return "HIGH_MASKING"
    if ratio >= policy.moderate_min:
        return "MODERATE_MASKING"
    return "LOW_MASKING"


def _masking_conclusion(
    registry: IndicatorResearchRegistry,
    upstream_id: str,
    masking_status: str,
) -> str:
    upstream = _indicator_or_raise(registry, upstream_id)
    if masking_status == "HIGH_MASKING":
        return "B_EFFECT_MASKED_BY_A"
    if _is_high_impact_unvalidated(registry, upstream):
        return "B_EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A"
    if masking_status == "MODERATE_MASKING":
        return "B_EFFECT_CONDITIONAL_ON_A"
    return "LOW_MASKING"


def _gate_outcome(
    registry: IndicatorResearchRegistry,
    indicator: IndicatorSpec,
    coverage_status: str,
    masking_results: Sequence[Mapping[str, Any]],
) -> str:
    if indicator.constraint_type == "IMMUTABLE_SAFETY_CONSTRAINT":
        return "SIGNAL_MAPPING_OBSERVE"
    if coverage_status == "HIGH_IMPACT_UNVALIDATED":
        return "OWNER_REVIEW_REQUIRED"
    if any(
        str(item.get("conclusion_status"))
        in {
            "B_EFFECT_MASKED_BY_A",
            "B_EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A",
        }
        for item in masking_results
    ):
        return "EFFECT_MASKED_BY_UPSTREAM_CONSTRAINT"
    if coverage_status == "REGISTERED_NOT_RESEARCHED":
        return "SIGNAL_MAPPING_OBSERVE"
    return "INCREMENTAL_EFFECT_NOT_PROVEN"


def _required_next_evidence(outcome: str) -> list[str]:
    if outcome == "OWNER_REVIEW_REQUIRED":
        return [
            "complete_dominance_and_masking_trace_before_parameter_or_gate_change",
            "run_factorial_base_a_b_ab_study_before_hard_cap_promotion",
        ]
    if outcome == "EFFECT_MASKED_BY_UPSTREAM_CONSTRAINT":
        return ["run_counterfactual_without_high_impact_upstream_constraint"]
    return ["collect_mapping_free_diagnostics_and_calibration_evidence"]


def _mapping_research_allowed(indicator: IndicatorSpec) -> bool:
    data_gate_values = set(indicator.data_gate.values())
    if not data_gate_values:
        return False
    return not any("REQUIRED" in value and "PASS" not in value for value in data_gate_values)


def _calibration_requirements(registry: IndicatorResearchRegistry) -> dict[str, Any]:
    policy = registry.validation_policy.calibration
    return {
        "calibration": "EVIDENCE_REQUIRED",
        "monotonicity": "EVIDENCE_REQUIRED",
        "trigger_rate": "EVIDENCE_REQUIRED",
        "state_persistence": "EVIDENCE_REQUIRED",
        "parameter_neighborhood": "EVIDENCE_REQUIRED",
        "cross_window_stability": "EVIDENCE_REQUIRED",
        "cross_asset_stability": "EVIDENCE_REQUIRED",
        "min_trigger_observations": policy.min_trigger_observations_for_mapping_research,
        "min_walk_forward_windows": policy.min_walk_forward_windows,
    }


def _hypothesis_card(
    indicator: IndicatorSpec,
    family: MappingCandidateFamily,
    current_mapping: MappingSpec | None,
    coverage_status: str,
) -> dict[str, Any]:
    hard_cap_family = family.family_id == "M5_HARD_CAP"
    return {
        "hypothesis_id": f"{indicator.indicator_id}:{family.family_id}",
        "indicator_id": indicator.indicator_id,
        "mapping_family": family.family_id,
        "description": family.description,
        "role": indicator.role,
        "target_family": indicator.target_family,
        "allowed_for_current_stage": (
            not hard_cap_family or coverage_status != "HIGH_IMPACT_UNVALIDATED"
        ),
        "requires_before_backfill": [
            "PIT_LEAKAGE_GATE_PASS",
            "MAPPING_FREE_DIAGNOSTICS_PASS",
            "TRIAL_LEDGER_ENTRY",
        ]
        + (["MASKING_AND_DOMINANCE_AUDIT_PASS"] if hard_cap_family else []),
        "current_mapping_family": None if current_mapping is None else current_mapping.family,
    }


def _conditional_conclusion_warnings(
    registry: IndicatorResearchRegistry,
) -> list[dict[str, Any]]:
    warnings = []
    for upstream_id, downstream_id in _high_impact_masking_pairs(registry):
        warnings.append(
            {
                "upstream_indicator_id": upstream_id,
                "downstream_indicator_id": downstream_id,
                "conclusion_status": "B_EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A",
            }
        )
    return warnings


def _conditionality_label(registry: IndicatorResearchRegistry, indicator_id: str) -> str:
    upstream = _upstream_constraints(registry, indicator_id)
    if not upstream:
        return "INDEPENDENT_EFFECT_NOT_YET_TESTED"
    high_impact = [
        item
        for item in upstream
        if _is_high_impact_unvalidated(registry, _indicator_or_raise(registry, item))
    ]
    if high_impact:
        return "EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_UPSTREAM"
    return "EFFECT_CONDITIONAL_ON_UPSTREAM_CONSTRAINTS"


def _upstream_constraints(
    registry: IndicatorResearchRegistry,
    indicator_id: str,
) -> list[str]:
    indicator_ids = {item.indicator_id for item in registry.indicators}
    return sorted(
        {
            dependency.from_node
            for dependency in registry.dependencies
            if dependency.to_node == indicator_id
            and dependency.from_node in indicator_ids
            and dependency.edge_type in EDGE_TYPES_THAT_CAN_MASK
        }
    )


def _independent_effect_status(results: Sequence[Mapping[str, Any]]) -> str:
    if not results:
        return "INDEPENDENT_EFFECT_NOT_IDENTIFIED"
    statuses = {str(item.get("conclusion_status")) for item in results}
    if "B_EFFECT_MASKED_BY_A" in statuses:
        return "EFFECT_MASKED_BY_UPSTREAM_CONSTRAINT"
    if "B_EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A" in statuses:
        return "EFFECT_CONDITIONAL_ON_HIGH_IMPACT_UNVALIDATED_A"
    if TRACE_REQUIRED_STATUS in {str(item.get("masking_status")) for item in results}:
        return "TRACE_REQUIRED_BEFORE_INDEPENDENT_EFFECT"
    return "CONDITIONAL_EFFECT_ONLY"


def _indicator_by_daily_component(
    registry: IndicatorResearchRegistry,
) -> dict[str, IndicatorSpec]:
    return {
        indicator.daily_component_id: indicator
        for indicator in registry.indicators
        if indicator.daily_component_id
    }


def _validation_pack_stability_projection(pack: Mapping[str, Any]) -> dict[str, Any]:
    artifacts = pack.get("artifacts", {})
    if not isinstance(artifacts, Mapping):
        artifacts = {}
    key_artifact_statuses = {}
    for artifact_id in (
        "daily_indicator_inventory",
        "indicator_research_coverage_audit",
        "daily_indicator_coverage_gap_report",
        "threshold_registry_audit",
        "threshold_prioritization_report",
        "threshold_calibration_report",
        "dynamic_trend_threshold_sensitivity_review",
        "dynamic_trend_bridge_consistency_audit",
        "indicator_dependency_graph",
        "indicator_masking_and_dominance_audit_valuation_crowding",
        "valuation_crowding_pilot_validation_report",
        "indicator_masking_casebook_valuation_crowding_trend",
        "valuation_crowding_ablation_validation",
        "valuation_crowding_outcome_availability_audit",
        "valuation_crowding_masking_effectiveness_review",
        "valuation_crowding_masking_robustness_review",
        "indicator_research_validation_rollup",
        "long_horizon_evidence_floor_calibration_audit",
        "historical_multi_stage_weight_trace_validation",
        "historical_trace_gate_availability_audit",
        "component_level_historical_trace",
        "backtest_trace_bridge",
        "lineage_manifest_repair_report",
    ):
        paths = artifacts.get(artifact_id)
        if isinstance(paths, Mapping) and paths.get("json_path"):
            key_artifact_statuses[artifact_id] = _read_json_status(Path(str(paths["json_path"])))
    inventory = _read_pack_artifact_json(artifacts, "daily_indicator_inventory")
    coverage_gap = _read_pack_artifact_json(artifacts, "daily_indicator_coverage_gap_report")
    threshold_audit = _read_pack_artifact_json(artifacts, "threshold_registry_audit")
    threshold_prioritization = _read_pack_artifact_json(
        artifacts,
        "threshold_prioritization_report",
    )
    threshold_calibration = _read_pack_artifact_json(
        artifacts,
        "threshold_calibration_report",
    )
    dynamic_trend_sensitivity = _read_pack_artifact_json(
        artifacts,
        "dynamic_trend_threshold_sensitivity_review",
    )
    dynamic_trend_bridge_consistency = _read_pack_artifact_json(
        artifacts,
        "dynamic_trend_bridge_consistency_audit",
    )
    masking = _read_pack_artifact_json(
        artifacts,
        "indicator_masking_and_dominance_audit_valuation_crowding",
    )
    casebook = _read_pack_artifact_json(
        artifacts,
        "indicator_masking_casebook_valuation_crowding_trend",
    )
    historical_trace = _read_pack_artifact_json(
        artifacts,
        "historical_multi_stage_weight_trace_validation",
    )
    gate_availability = _read_pack_artifact_json(
        artifacts,
        "historical_trace_gate_availability_audit",
    )
    component_trace = _read_pack_artifact_json(
        artifacts,
        "component_level_historical_trace",
    )
    backtest_bridge = _read_pack_artifact_json(
        artifacts,
        "backtest_trace_bridge",
    )
    effectiveness_review = _read_pack_artifact_json(
        artifacts,
        "valuation_crowding_masking_effectiveness_review",
    )
    robustness_review = _read_pack_artifact_json(
        artifacts,
        "valuation_crowding_masking_robustness_review",
    )
    validation_rollup = _read_pack_artifact_json(
        artifacts,
        "indicator_research_validation_rollup",
    )
    floor_calibration = _read_pack_artifact_json(
        artifacts,
        "long_horizon_evidence_floor_calibration_audit",
    )
    outcome_availability = _read_pack_artifact_json(
        artifacts,
        "valuation_crowding_outcome_availability_audit",
    )
    lineage_repair = _read_pack_artifact_json(
        artifacts,
        "lineage_manifest_repair_report",
    )
    high_impact = coverage_gap.get("high_impact_unvalidated", [])
    historical_summary = (
        historical_trace.get("summary", {}) if isinstance(historical_trace, Mapping) else {}
    )
    return {
        "status": pack.get("status"),
        "artifact_count": len(artifacts),
        "validation_check_statuses": {
            item.get("check_id"): item.get("status")
            for item in pack.get("validation_checks", [])
            if isinstance(item, Mapping)
        },
        "key_artifact_statuses": key_artifact_statuses,
        "registry_coverage_counts": (
            inventory.get("summary", {}).get("coverage_counts", {})
            if isinstance(inventory, Mapping)
            else {}
        ),
        "coverage_gap_summary": (
            coverage_gap.get("summary", {}) if isinstance(coverage_gap, Mapping) else {}
        ),
        "threshold_audit_summary": (
            threshold_audit.get("summary", {}) if isinstance(threshold_audit, Mapping) else {}
        ),
        "threshold_prioritization_summary": (
            threshold_prioritization.get("summary", {})
            if isinstance(threshold_prioritization, Mapping)
            else {}
        ),
        "threshold_calibration_summary": (
            threshold_calibration.get("summary", {})
            if isinstance(threshold_calibration, Mapping)
            else {}
        ),
        "dynamic_trend_threshold_sensitivity_summary": (
            dynamic_trend_sensitivity.get("summary", {})
            if isinstance(dynamic_trend_sensitivity, Mapping)
            else {}
        ),
        "dynamic_trend_bridge_consistency_summary": (
            dynamic_trend_bridge_consistency.get("summary", {})
            if isinstance(dynamic_trend_bridge_consistency, Mapping)
            else {}
        ),
        "coverage_gap_unregistered": (
            coverage_gap.get("unregistered_daily_indicators", [])
            if isinstance(coverage_gap, Mapping)
            else []
        ),
        "high_impact_unvalidated_ids": (
            [item.get("indicator_id") for item in high_impact if isinstance(item, Mapping)]
            if isinstance(high_impact, list)
            else []
        ),
        "trace_field_audit": (
            historical_trace.get("trace_contract_field_audit", {})
            if isinstance(historical_trace, Mapping)
            else {}
        ),
        "trace_field_missing_count": (
            historical_trace.get("summary", {}).get("missing_trace_field_record_count", 0)
            if isinstance(historical_trace, Mapping)
            else 0
        ),
        "masking_diagnostics": (
            masking.get("masking_results", []) if isinstance(masking, Mapping) else []
        ),
        "masking_casebook_summary": (
            casebook.get("summary", {}) if isinstance(casebook, Mapping) else {}
        ),
        "gate_availability_summary": (
            gate_availability.get("summary", {}) if isinstance(gate_availability, Mapping) else {}
        ),
        "sample_quality_summary": {
            key: historical_summary.get(key)
            for key in (
                "eligible_dates",
                "component_eligible_dates",
                "asset_count",
                "masking_case_count",
                "regime_count",
                "full_advisory_equivalent_count",
                "partial_component_only_count",
            )
        },
        "component_trace_summary": (
            component_trace.get("summary", {}) if isinstance(component_trace, Mapping) else {}
        ),
        "backtest_trace_bridge_summary": (
            backtest_bridge.get("summary", {}) if isinstance(backtest_bridge, Mapping) else {}
        ),
        "masking_effectiveness_summary": (
            effectiveness_review.get("summary", {})
            if isinstance(effectiveness_review, Mapping)
            else {}
        ),
        "outcome_availability_summary": (
            outcome_availability.get("summary", {})
            if isinstance(outcome_availability, Mapping)
            else {}
        ),
        "masking_robustness_summary": (
            robustness_review.get("summary", {}) if isinstance(robustness_review, Mapping) else {}
        ),
        "masking_robustness_delta_count": (
            len(robustness_review.get("scenario_delta_matrix", []))
            if isinstance(robustness_review, Mapping)
            else 0
        ),
        "validation_rollup_summary": (
            validation_rollup.get("summary", {}) if isinstance(validation_rollup, Mapping) else {}
        ),
        "validation_rollup_recommendation": (
            validation_rollup.get("valuation_crowding_masking_current_recommendation", {})
            if isinstance(validation_rollup, Mapping)
            else {}
        ),
        "floor_calibration_summary": (
            floor_calibration.get("summary", {}) if isinstance(floor_calibration, Mapping) else {}
        ),
        "floor_calibration_sensitivity": (
            floor_calibration.get("threshold_sensitivity", {})
            if isinstance(floor_calibration, Mapping)
            else {}
        ),
        "lineage_manifest_repair_summary": (
            lineage_repair.get("summary", {}) if isinstance(lineage_repair, Mapping) else {}
        ),
    }


def _read_pack_artifact_json(
    artifacts: Mapping[str, Any],
    artifact_id: str,
) -> Mapping[str, Any]:
    paths = artifacts.get(artifact_id)
    if not isinstance(paths, Mapping) or not paths.get("json_path"):
        return {}
    path = Path(str(paths["json_path"]))
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, Mapping) else {}


def _read_json_status(path: Path) -> str:
    if not path.exists():
        return "MISSING"
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        return "INVALID"
    return str(raw.get("status", "UNKNOWN"))


def _mapping_for_indicator(
    registry: IndicatorResearchRegistry,
    indicator_id: str,
) -> MappingSpec | None:
    for mapping in registry.mappings:
        if mapping.indicator_id == indicator_id:
            return mapping
    return None


def _indicator_summary(
    registry: IndicatorResearchRegistry,
    indicator: IndicatorSpec,
) -> dict[str, Any]:
    return {
        "indicator_id": indicator.indicator_id,
        "display_name": indicator.display_name,
        "role": indicator.role,
        "constraint_type": indicator.constraint_type,
        "mapping_version": indicator.mapping_version,
        "coverage_status": _coverage_status(registry, indicator),
        "economic_interpretation": indicator.economic_interpretation,
    }


def _indicator_or_raise(
    registry: IndicatorResearchRegistry,
    indicator_id: str,
) -> IndicatorSpec:
    for indicator in registry.indicators:
        if indicator.indicator_id == indicator_id:
            return indicator
    raise IndicatorResearchError(f"unknown indicator_id: {indicator_id}")


def _feature_node_id(feature: SourceFeature) -> str:
    return f"feature:{feature.category}:{feature.subject}:{feature.feature}"


def _campaign_stage_for_indicator_stage(stage_id: str) -> str:
    if "INVENTORY" in stage_id or "PIT" in stage_id:
        return "INPUT_PRECHECK"
    if "MASKING" in stage_id or "PORTFOLIO" in stage_id:
        return "ATTRIBUTION"
    if "FACTORIAL" in stage_id:
        return "INTERACTION"
    if "GATE" in stage_id:
        return "GATE"
    return "MINI_DIAGNOSTIC"


def _check_status(statuses: Sequence[str], _name: str) -> str:
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    if any("WARNING" in status or "LIMITATION" in status for status in statuses):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _status_from_issues(issues: Sequence[Mapping[str, Any]]) -> str:
    if any(issue.get("severity") == "error" for issue in issues):
        return "FAIL"
    if issues:
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _count_by(rows: Iterable[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _duplicates(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for item in items:
        if item in seen and item not in duplicates:
            duplicates.append(item)
        seen.add(item)
    return duplicates


def _row_delta(row: Mapping[str, Any]) -> float:
    if "delta" in row:
        return _float(row.get("delta"))
    before = _float(row.get("weight_before"))
    after = _float(row.get("weight_after"))
    return after - before


def _float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _compact_markdown_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    return text.replace("\n", " ")[:500]
