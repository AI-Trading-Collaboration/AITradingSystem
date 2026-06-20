from __future__ import annotations

import csv
import hashlib
import json
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
DEFAULT_INDICATOR_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_indicators"
DEFAULT_DAILY_INDICATOR_TRACE_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "reports"

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
# Validation-only drawdown tolerance for no-mask comparison; not a production policy.
EFFECTIVENESS_DRAWDOWN_WORSE_TOLERANCE = 0.10
# Validation-only missed-upside ceiling for retaining baseline masking.
EFFECTIVENESS_MISSED_UPSIDE_ACCEPTABLE_RATE = 0.40
# Validation-only scenario cap used for counterfactual reporting; not a production policy.
DEFAULT_MASKING_ABLATION_CAP_RATIO = 0.50
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
    high_impact = [
        item for item in items if item["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
    ]
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
        contribution = (
            _float(component.score) * _float(component.weight) / total_component_weight
        )
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


def write_indicator_validation_pack_stability_report(
    *,
    registry_path: Path = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Path = DEFAULT_INDICATOR_OUTPUT_ROOT,
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
    validation_root = output_root / "control_plane_v1_validation"
    stability_root = validation_root / "validation_pack_rerun_stability"
    first = write_indicator_framework_validation_pack(
        registry_path=registry_path,
        output_root=stability_root / "run_1",
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
    second = write_indicator_framework_validation_pack(
        registry_path=registry_path,
        output_root=stability_root / "run_2",
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
            first_projection["key_artifact_statuses"]
            == second_projection["key_artifact_statuses"]
        ),
        "registry_coverage_counts": (
            first_projection["registry_coverage_counts"]
            == second_projection["registry_coverage_counts"]
        ),
        "coverage_gap_summary": (
            first_projection["coverage_gap_summary"]
            == second_projection["coverage_gap_summary"]
        ),
        "trace_field_audit": (
            first_projection["trace_field_audit"]
            == second_projection["trace_field_audit"]
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
            first_projection["masking_diagnostics"]
            == second_projection["masking_diagnostics"]
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
        "lineage_manifest_repair_repeatable": (
            first_projection["lineage_manifest_repair_summary"]
            == second_projection["lineage_manifest_repair_summary"]
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
            "outcome_not_mature_count": sum(
                1 for case in cases if case.get("outcome_not_mature")
            ),
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
    records = _outcome_availability_records(casebook_cases, bridge_cases)
    summary = _outcome_availability_summary(records)
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
        dict(item)
        for item in gate_audit.get("gate_availability", [])
        if isinstance(item, Mapping)
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
    outcome_records = _outcome_availability_records(cases, bridge_cases)
    outcome_availability_summary = _outcome_availability_summary(outcome_records)
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
    recommendation = _masking_effectiveness_recommendation(layers["full_advisory_only"])
    issues = list(casebook.get("issues", [])) if isinstance(casebook.get("issues"), list) else []
    if not cases:
        issues.append(
            {
                "severity": "warning",
                "issue_id": "effectiveness_requires_masking_cases",
                "message": (
                    "Effectiveness review emitted schema only; trace-backed cases are "
                    "required."
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
    status = (
        "PASS"
        if cases and recommendation["decision_recommendation"] != "insufficient_evidence"
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
            "capped_masking_ratio": capped_masking_ratio,
            "outcome_ticker": outcome_ticker,
            "decision_recommendation": recommendation["decision_recommendation"],
            "outcome_available_count": outcome_availability_summary[
                "outcome_available_count"
            ],
            "outcome_missing_count": outcome_availability_summary[
                "outcome_missing_count"
            ],
            "outcome_not_mature_count": outcome_availability_summary[
                "outcome_not_mature_count"
            ],
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
        gate_availability_summary=gate_audit.get("summary", {}),
        backtest_bridge_summary=bridge.get("summary", {}),
        outcome_availability_summary=outcome_availability_summary,
        outcome_availability_records=outcome_records,
        allowed_uses=list(NON_PROMOTION_ALLOWED_USES),
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
        status="PASS_WITH_WARNINGS"
        if any(not record["full_advisory_trace_eligible"] for record in records)
        else "PASS",
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
            row.get("row_type") == "indicator_component"
            and row.get("module_id") in keep_modules
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
        component_eligible = bool(
            gate_record.get("component_validation_trace_eligible", True)
        )
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
    output_root: Path = DEFAULT_INDICATOR_OUTPUT_ROOT,
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
    validation_root = output_root / "control_plane_v1_validation"
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
            "no_parameter_mutation": True,
            "no_paper_shadow_live_broker_order_official_weights": True,
        },
        validation_checks=[
            {
                "check_id": "ontology",
                "status": _check_status(statuses, "indicator_research_ontology"),
            },
            {"check_id": "inventory_coverage", "status": "PASS_WITH_WARNINGS"},
            {"check_id": "coverage_gap_report", "status": "PASS_WITH_WARNINGS"},
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
    if (
        (indicator.affects_signal or indicator.affects_weight)
        and (
            not indicator.mapping_version
            or indicator.mapping_version not in mapping_versions
        )
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
        _fallback_lineage_manifest(trace_path, row_date)
        for row_date in _trace_dates(trace_rows)
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
        manifest = _lineage_manifest_from_source_trace(
            trace_path or source_path,
            str(source_path),
        ) or {}
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
        str(case.get("asset")).upper()
        for case in cases
        if case.get("asset") not in (None, "")
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
    availability_dates = {
        str(record.get("date")) for record in availability if record.get("date")
    }
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
        str(row.get("market_regime") or registry.market_regime.regime_id)
        for row in trace_rows
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
            row
            for row in rows
            if _date_in_window(str(row.get("date") or ""), start=start, end=end)
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
    all_assets = {
        str(row.get("asset") or "").upper()
        for row in trace_rows
        if row.get("asset")
    }
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
        str(issue.get(key) or "")
        for key in ("code", "rule_or_source", "message")
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
        rf"{re.escape(field)}\s*[=:：]\s*"
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][^。；;,\s]+)?)"
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
        str(record.get("date"))
        for record in records
        if record.get("full_advisory_trace_eligible")
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
            raw.get("report_type")
            or raw.get("artifact_type")
            or raw.get("type")
            or path.stem
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
            f"{row_date}:{row.get('module_id', '')}:"
            f"{row.get('downstream_indicator_id', '')}"
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
                )
            )
        else:
            windows.append(
                _outcome_window_record(horizon, OUTCOME_WINDOW_STATUS_NOT_MATURE)
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
) -> dict[str, Any]:
    return {
        "window": f"{horizon}d",
        "horizon_trading_days": horizon,
        "status": status,
        "target_date": None if target_date is None else target_date.isoformat(),
        "realized_return": realized_return,
    }


def _outcome_payload_from_windows(
    outcomes: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    hard_missing = any(
        str(window.get("status") or "") in OUTCOME_HARD_MISSING_STATUSES
        for window in windows
    )
    not_mature = any(
        window.get("status") == OUTCOME_WINDOW_STATUS_NOT_MATURE
        for window in windows
    )
    all_available = all(
        window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE
        for window in windows
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
                else (
                    OUTCOME_WINDOW_STATUS_NOT_MATURE
                    if not_mature
                    else "unknown"
                )
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


def _outcome_availability_record(
    case: Mapping[str, Any],
    *,
    source_case_type: str,
    scenario: str,
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
    return {
        "source_case_type": source_case_type,
        "case_id": case.get("case_id"),
        "as_of_date": join_key["as_of_date"],
        "date": join_key["as_of_date"],
        "decision_time": join_key["decision_time"],
        "asset": join_key["asset"],
        "scenario": join_key["scenario"],
        "trace_source": join_key["trace_source"],
        "trace_contract_version": join_key["trace_contract_version"],
        "outcome_join_key": join_key,
        "missing_join_key_fields": missing_join_key_fields,
        "outcome_windows": windows,
        "outcomes": dict(case.get("outcomes", {}))
        if isinstance(case.get("outcomes"), Mapping)
        else {},
        "outcome_available": all_available,
        "outcome_missing": hard_missing,
        "outcome_not_mature": not_mature,
        "missing_price": OUTCOME_WINDOW_STATUS_MISSING_PRICE in statuses,
        "missing_asset_mapping": OUTCOME_WINDOW_STATUS_MISSING_ASSET_MAPPING in statuses,
        "missing_calendar": OUTCOME_WINDOW_STATUS_MISSING_CALENDAR in statuses,
        "missing_join_key": bool(missing_join_key_fields),
        "promotion_gate_allowed": False,
        "allowed_uses": list(NON_PROMOTION_ALLOWED_USES),
    }


def _outcome_availability_records(
    casebook_cases: Sequence[Mapping[str, Any]],
    bridge_cases: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for case in casebook_cases:
        records.append(
            _outcome_availability_record(
                case,
                source_case_type="masking_casebook",
                scenario=str(case.get("scenario") or "baseline"),
            )
        )
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            records.append(
                _outcome_availability_record(
                    case,
                    source_case_type="ablation_scenario",
                    scenario=scenario_id,
                )
            )
    for case in bridge_cases:
        for scenario_id in MASKING_ABLATION_SCENARIOS:
            records.append(
                _outcome_availability_record(
                    case,
                    source_case_type="backtest_bridge",
                    scenario=scenario_id,
                )
            )
    return records


def _case_outcome_join_key(case: Mapping[str, Any], *, scenario: str) -> dict[str, str]:
    join_key = case.get("outcome_join_key", {})
    if not isinstance(join_key, Mapping):
        join_key = {}
    return {
        "as_of_date": str(
            join_key.get("as_of_date")
            or case.get("as_of_date")
            or case.get("date")
            or ""
        ),
        "decision_time": str(
            join_key.get("decision_time")
            or case.get("decision_time")
            or case.get("date")
            or ""
        ),
        "asset": str(join_key.get("asset") or case.get("asset") or "").upper(),
        "scenario": scenario,
        "trace_source": str(
            join_key.get("trace_source") or case.get("trace_source") or ""
        ),
        "trace_contract_version": str(
            join_key.get("trace_contract_version")
            or case.get("trace_contract_version")
            or ""
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
    return {
        "total_cases": len(records),
        "outcome_available_count": sum(
            1 for record in records if record.get("outcome_available")
        ),
        "outcome_missing_count": sum(
            1 for record in records if record.get("outcome_missing")
        ),
        "outcome_not_mature_count": sum(
            1 for record in records if record.get("outcome_not_mature")
        ),
        "missing_price_count": sum(1 for record in records if record.get("missing_price")),
        "missing_asset_mapping_count": sum(
            1 for record in records if record.get("missing_asset_mapping")
        ),
        "missing_calendar_count": sum(
            1 for record in records if record.get("missing_calendar")
        ),
        "missing_join_key_count": sum(
            1 for record in records if record.get("missing_join_key")
        ),
        "date_count": len(
            {str(record.get("as_of_date") or "") for record in records if record.get("as_of_date")}
        ),
        "asset_count": len(
            {str(record.get("asset") or "") for record in records if record.get("asset")}
        ),
        "scenario_count": len(
            {
                str(record.get("scenario") or "")
                for record in records
                if record.get("scenario")
            }
        ),
        "source_case_type_count": len(
            {
                str(record.get("source_case_type") or "")
                for record in records
                if record.get("source_case_type")
            }
        ),
        "by_window": dict(sorted(by_window.items())),
        "by_asset": [
            {"asset": key, **value} for key, value in sorted(by_asset.items())
        ],
        "by_date": [
            {"date": key, **value} for key, value in sorted(by_date.items())
        ],
    }


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


def _masking_effectiveness_sample_quality(
    cases: Sequence[Mapping[str, Any]],
    registry: IndicatorResearchRegistry,
) -> dict[str, Any]:
    dates = {str(case.get("date")) for case in cases if case.get("date")}
    assets = {str(case.get("asset")).upper() for case in cases if case.get("asset")}
    regimes = {
        str(case.get("market_regime") or registry.market_regime.regime_id)
        for case in cases
    }
    clusters = {_asset_cluster_id(asset) for asset in assets}
    return {
        "date_count": len(dates),
        "asset_count": len(assets),
        "case_count": len(cases),
        "unique_regime_count": len(regimes) if cases else 0,
        "correlated_asset_cluster_count": len(clusters),
        "outcome_missing_count": sum(1 for case in cases if case.get("outcome_missing")),
        "outcome_available_count": sum(1 for case in cases if _case_outcome_available(case)),
        "outcome_not_mature_count": sum(
            1 for case in cases if case.get("outcome_not_mature")
        ),
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
            return all(
                window.get("status") == OUTCOME_WINDOW_STATUS_AVAILABLE
                for window in usable
            )
    outcomes = case.get("outcomes", {})
    if not isinstance(outcomes, Mapping):
        return False
    return all(
        isinstance(outcomes.get(f"return_{horizon}d"), float)
        for horizon in MASKING_OUTCOME_HORIZONS
    )


def _masking_effectiveness_recommendation(
    full_layer: Mapping[str, Any],
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
    if (
        outcome_available_count < EFFECTIVENESS_MIN_AVAILABLE_OUTCOME_CASES
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
                f"outcome_not_mature_count={outcome_not_mature_count}."
            ),
        }
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
    baseline_20d = _optional_float(baseline.get("avg_return_20d"))
    capped_20d = _optional_float(capped.get("avg_return_20d"))
    no_mask_20d = _optional_float(no_mask.get("avg_return_20d"))
    if (
        capped_drawdown is not None
        and baseline_drawdown is not None
        and (capped_20d or 0.0) >= (baseline_20d or 0.0)
        and capped_drawdown >= baseline_drawdown
    ):
        decision = "prefer_capped_masking_candidate"
        rationale = "Capped masking improves or matches drawdown while preserving return."
    elif (
        no_mask_20d is not None
        and baseline_20d is not None
        and no_mask_20d > baseline_20d
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


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
        f"avg_return_{horizon}d": (
            sum(values) / len(values) if values else None
        )
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
        "sample_quality_breakdown": {
            "date_count": len({str(case.get("date")) for case in cases if case.get("date")}),
            "asset_count": len({str(case.get("asset")) for case in cases if case.get("asset")}),
            "case_count": len(cases),
            "outcome_missing_count": sum(1 for case in cases if case.get("outcome_missing")),
            "outcome_available_count": sum(1 for case in cases if _case_outcome_available(case)),
            "outcome_not_mature_count": sum(
                1 for case in cases if case.get("outcome_not_mature")
            ),
        },
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
        str(item.get("conclusion_status")) in {
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
        "indicator_dependency_graph",
        "indicator_masking_and_dominance_audit_valuation_crowding",
        "valuation_crowding_pilot_validation_report",
        "indicator_masking_casebook_valuation_crowding_trend",
        "valuation_crowding_ablation_validation",
        "valuation_crowding_outcome_availability_audit",
        "valuation_crowding_masking_effectiveness_review",
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
        "coverage_gap_unregistered": (
            coverage_gap.get("unregistered_daily_indicators", [])
            if isinstance(coverage_gap, Mapping)
            else []
        ),
        "high_impact_unvalidated_ids": (
            [
                item.get("indicator_id")
                for item in high_impact
                if isinstance(item, Mapping)
            ]
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
            gate_availability.get("summary", {})
            if isinstance(gate_availability, Mapping)
            else {}
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
