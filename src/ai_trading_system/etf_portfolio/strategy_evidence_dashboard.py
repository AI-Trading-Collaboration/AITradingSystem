from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata, load_etf_config_bundle
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "evidence_dashboard.yaml"
)
DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "evidence_dashboard"
)
DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR = (
    DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR / "aggregation"
)
DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR = (
    DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR / "validation"
)

STRATEGY_EVIDENCE_REGISTRY_SCHEMA_VERSION = (
    "etf_strategy_evidence_dashboard_registry_v1"
)
STRATEGY_EVIDENCE_DASHBOARD_SCHEMA_VERSION = "etf_strategy_evidence_dashboard_v1"
STRATEGY_EVIDENCE_AGGREGATION_SCHEMA_VERSION = "etf_strategy_evidence_aggregation_v1"
STRATEGY_EVIDENCE_VALIDATION_SCHEMA_VERSION = "etf_strategy_evidence_validation_v1"
STRATEGY_EVIDENCE_REPORT_TYPE = "etf_strategy_evidence_dashboard"
STRATEGY_EVIDENCE_VALIDATION_REPORT_TYPE = "etf_strategy_evidence_dashboard_validation"
STRATEGY_EVIDENCE_REPORT_REGISTRY_ID = "etf_strategy_evidence_dashboard"
STRATEGY_EVIDENCE_VALIDATION_REGISTRY_ID = "etf_strategy_evidence_dashboard_validation"

STRATEGY_EVIDENCE_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

EvidenceCategory = Literal[
    "baseline_allocation",
    "weight_calibration",
    "forward_simulation",
    "ai_confirmation",
    "ai_attribution",
    "satellite_replacement",
    "satellite_attribution",
    "parameter_review",
    "weekly_review",
    "dynamic_shadow",
    "decision_journal",
    "data_quality",
    "operations_health",
    "validation_gates",
]
EvidenceStatus = Literal[
    "strong_support",
    "supportive",
    "mixed",
    "needs_more_data",
    "weak",
    "blocked",
    "stale",
    "invalid",
]
EvidenceConfidence = Literal["high", "medium", "low", "none"]
CandidateType = Literal[
    "weight_calibration_candidate",
    "forward_shadow_candidate",
    "AI_overlay_candidate",
    "satellite_replacement_candidate",
    "parameter_review_proposal",
    "dynamic_shadow_candidate",
]
ConflictSeverity = Literal["critical", "high", "medium", "low"]
ManualReviewPriorityLevel = Literal["critical", "high", "medium", "low"]
ManualReviewAction = Literal[
    "review_blocker",
    "review_candidate",
    "continue_observation",
    "defer_decision",
    "request_more_data",
    "reject_after_review",
    "start_new_experiment",
]
SourceLoadStatus = Literal["loaded", "missing", "stale", "blocked", "optional_missing"]

_ALLOWED_CATEGORIES = set(EvidenceCategory.__args__)  # type: ignore[attr-defined]
_UNSAFE_MANUAL_ACTIONS = {
    "place_order",
    "promote_to_production",
    "change_production_weights",
    "apply_weight_change",
}


class StrategyEvidenceDashboardError(ValueError):
    """Raised when the strategy evidence dashboard contract is unsafe."""


class StrategyEvidenceSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class StrategyEvidenceSourceConfig(BaseModel):
    category: EvidenceCategory
    title: str = Field(min_length=1)
    source_module: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    required: bool
    max_age_days: int = Field(ge=0)
    source_metric_paths: list[str] = Field(default_factory=list)
    sample_count_paths: list[str] = Field(default_factory=list)
    validation_report_id: str | None = None
    expected_artifact_globs: list[str] = Field(default_factory=list)
    manual_review_action: ManualReviewAction = "continue_observation"
    minimum_sample_count: int = Field(default=1, ge=0)

    @model_validator(mode="after")
    def normalize_lists(self) -> Self:
        self.source_metric_paths = _unique_strings(self.source_metric_paths)
        self.sample_count_paths = _unique_strings(self.sample_count_paths)
        self.expected_artifact_globs = _unique_strings(self.expected_artifact_globs)
        return self


class StrategyEvidenceCategoryConfig(BaseModel):
    title: str = Field(min_length=1)
    required: bool = True
    default_manual_review_action: ManualReviewAction = "continue_observation"


class StrategyEvidenceFreshnessRequirement(BaseModel):
    max_age_days: int = Field(ge=0)
    stale_required_blocks: bool = True


class StrategyEvidenceQualityRequirement(BaseModel):
    require_data_quality_context: Literal[True] = True
    require_validation_context: Literal[True] = True
    require_source_links: Literal[True] = True
    minimum_sample_count_default: int = Field(default=1, ge=0)


class StrategyEvidenceManualReviewPriorityRules(BaseModel):
    critical_statuses: list[str] = Field(default_factory=lambda: ["blocked", "invalid"])
    stale_gate_priority: ManualReviewPriorityLevel = "high"
    conflict_priority: ManualReviewPriorityLevel = "high"
    needs_more_data_priority: ManualReviewPriorityLevel = "medium"
    optional_missing_priority: ManualReviewPriorityLevel = "low"


class StrategyEvidenceDashboardConfig(BaseModel):
    schema_version: Literal["etf_strategy_evidence_dashboard_registry_v1"]
    policy_metadata: PolicyMetadata
    safety: StrategyEvidenceSafety
    categories: dict[EvidenceCategory, StrategyEvidenceCategoryConfig]
    sources: dict[str, StrategyEvidenceSourceConfig]
    freshness_requirements: StrategyEvidenceFreshnessRequirement
    quality_requirements: StrategyEvidenceQualityRequirement
    manual_review_priority_rules: StrategyEvidenceManualReviewPriorityRules

    @model_validator(mode="after")
    def validate_registry(self) -> Self:
        if not self.sources:
            raise ValueError("evidence dashboard registry must contain sources")
        missing_categories = sorted(
            {source.category for source in self.sources.values()} - set(self.categories)
        )
        if missing_categories:
            raise ValueError(
                "evidence dashboard sources reference missing categories: "
                + ", ".join(missing_categories)
            )
        for source_id, source in self.sources.items():
            if not source_id.strip():
                raise ValueError("evidence dashboard source ID cannot be empty")
            if source.required and not source.report_id.strip():
                raise ValueError(f"{source_id}: required source must declare report_id")
            if source.max_age_days < 0:
                raise ValueError(f"{source_id}: max_age_days must be >= 0")
        return self


class StrategyEvidenceItem(BaseModel):
    evidence_id: str = Field(min_length=1)
    source_module: str = Field(min_length=1)
    source_report_path: str = Field(min_length=1)
    source_metric: str = Field(min_length=1)
    as_of_date: date
    freshness_status: str = Field(min_length=1)
    data_quality_status: str = Field(min_length=1)
    sample_count_if_applicable: int | None = Field(default=None, ge=0)
    summary: str = Field(min_length=1)
    value: Any = None


class StrategyEvidenceCard(BaseModel):
    card_id: str = Field(min_length=1)
    category: EvidenceCategory
    title: str = Field(min_length=1)
    status: EvidenceStatus
    confidence: EvidenceConfidence
    summary: str = Field(min_length=1)
    supporting_evidence: list[StrategyEvidenceItem] = Field(default_factory=list)
    blocking_evidence: list[StrategyEvidenceItem] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    sample_count: int = Field(ge=0)
    freshness_status: str = Field(min_length=1)
    data_quality_status: str = Field(min_length=1)
    validation_status: str = Field(min_length=1)
    source_report_paths: list[str] = Field(min_length=1)
    manual_review_action: ManualReviewAction

    @model_validator(mode="after")
    def validate_traceability(self) -> Self:
        self.source_report_paths = _unique_strings(self.source_report_paths)
        if not self.source_report_paths:
            raise ValueError(f"{self.card_id}: source_report_paths is required")
        evidence = [*self.supporting_evidence, *self.blocking_evidence]
        if not evidence:
            raise ValueError(f"{self.card_id}: at least one evidence item is required")
        for item in evidence:
            if item.source_report_path not in self.source_report_paths:
                self.source_report_paths.append(item.source_report_path)
        return self


class StrategyCandidateEvidenceRanking(BaseModel):
    candidate_id: str = Field(min_length=1)
    candidate_type: CandidateType
    rank: int = Field(ge=1)
    evidence_score: float = Field(ge=0, le=100)
    status: EvidenceStatus
    supporting_evidence: list[StrategyEvidenceItem] = Field(default_factory=list)
    blocking_evidence: list[StrategyEvidenceItem] = Field(default_factory=list)
    manual_review_priority: ManualReviewPriorityLevel
    dimension_scores: dict[str, float] = Field(default_factory=dict)


class StrategyEvidenceConflict(BaseModel):
    conflict_id: str = Field(min_length=1)
    affected_candidate_or_component: str = Field(min_length=1)
    conflict_type: str = Field(min_length=1)
    supporting_side: str = Field(min_length=1)
    blocking_side: str = Field(min_length=1)
    severity: ConflictSeverity
    source_links: list[str] = Field(min_length=1)
    manual_review_action: ManualReviewAction

    @model_validator(mode="after")
    def reject_unsafe_action(self) -> Self:
        if self.manual_review_action in _UNSAFE_MANUAL_ACTIONS:
            raise ValueError("unsafe conflict manual_review_action")
        return self


class StrategyManualReviewPriority(BaseModel):
    priority_id: str = Field(min_length=1)
    priority_level: ManualReviewPriorityLevel
    source_component: str = Field(min_length=1)
    issue: str = Field(min_length=1)
    recommended_review_action: ManualReviewAction
    evidence_links: list[str] = Field(default_factory=list)
    created_at: datetime
    status: Literal["open", "deferred", "resolved"] = "open"

    @model_validator(mode="after")
    def reject_unsafe_action(self) -> Self:
        if self.recommended_review_action in _UNSAFE_MANUAL_ACTIONS:
            raise ValueError("unsafe manual review action")
        return self


class StrategyEvidenceSourceReport(BaseModel):
    source_id: str = Field(min_length=1)
    category: EvidenceCategory
    title: str = Field(min_length=1)
    source_module: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    required: bool
    load_status: SourceLoadStatus
    source_report_path: str = Field(min_length=1)
    freshness_status: str = Field(min_length=1)
    data_quality_status: str = Field(min_length=1)
    validation_status: str = Field(min_length=1)
    artifact_status: str = Field(min_length=1)
    sample_count: int = Field(ge=0)
    age_days: int | None = None
    source_metrics: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)


class StrategyEvidenceDashboard(BaseModel):
    schema_version: Literal["etf_strategy_evidence_dashboard_v1"] = (
        STRATEGY_EVIDENCE_DASHBOARD_SCHEMA_VERSION
    )
    report_type: Literal["etf_strategy_evidence_dashboard"] = STRATEGY_EVIDENCE_REPORT_TYPE
    dashboard_id: str = Field(min_length=1)
    as_of_date: date
    generated_at: datetime
    model_version: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    overall_status: EvidenceStatus
    evidence_cards: list[StrategyEvidenceCard] = Field(default_factory=list)
    candidate_rankings: list[StrategyCandidateEvidenceRanking] = Field(default_factory=list)
    conflicts: list[StrategyEvidenceConflict] = Field(default_factory=list)
    data_quality_overlay: dict[str, Any] = Field(default_factory=dict)
    manual_review_priorities: list[StrategyManualReviewPriority] = Field(default_factory=list)
    source_reports: list[StrategyEvidenceSourceReport] = Field(default_factory=list)
    source_schema_versions: dict[str, str] = Field(default_factory=dict)
    safety: StrategyEvidenceSafety
    commands_executed: Literal[False] = False
    production_state_mutated: Literal[False] = False
    observe_only: Literal[True] = True
    candidate_only: Literal[True] = True
    production_effect: Literal["none"] = "none"
    broker_action: Literal["none"] = "none"
    manual_review_required: Literal[True] = True

    @model_validator(mode="after")
    def validate_dashboard(self) -> Self:
        if not self.evidence_cards:
            raise ValueError("strategy evidence dashboard requires evidence_cards")
        if self.safety.model_dump(mode="json") != STRATEGY_EVIDENCE_SAFETY:
            raise ValueError("strategy evidence dashboard safety boundary is unsafe")
        if self.commands_executed:
            raise ValueError("strategy evidence dashboard must not execute commands")
        if self.production_state_mutated:
            raise ValueError("strategy evidence dashboard must not mutate production state")
        return self


def load_strategy_evidence_dashboard_config(
    path: Path | str = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
) -> StrategyEvidenceDashboardConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise StrategyEvidenceDashboardError("evidence dashboard config must be a mapping")
    try:
        return StrategyEvidenceDashboardConfig.model_validate(raw)
    except ValueError as exc:
        raise StrategyEvidenceDashboardError(str(exc)) from exc


def build_strategy_evidence_aggregation(
    *,
    as_of: date | str,
    config: StrategyEvidenceDashboardConfig | None = None,
    config_path: Path | str = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_index: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of)
    registry_config = config or load_strategy_evidence_dashboard_config(config_path)
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    index_payload = _resolve_report_index(
        as_of=run_date,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    registry = _safe_report_registry(report_registry_path)
    source_reports = [
        _source_report_from_config(
            source_id=source_id,
            source=source,
            as_of=run_date,
            report_index=index_payload,
            report_registry=registry,
            report_registry_path=report_registry_path,
        )
        for source_id, source in sorted(registry_config.sources.items())
    ]
    missing_sources = [
        source.source_id
        for source in source_reports
        if source.load_status in {"missing", "optional_missing"}
    ]
    stale_sources = [
        source.source_id for source in source_reports if source.load_status == "stale"
    ]
    blocked_sources = [
        source.source_id for source in source_reports if source.load_status == "blocked"
    ]
    loaded_sources = [
        source.source_id for source in source_reports if source.load_status == "loaded"
    ]
    warnings = [
        warning
        for source in source_reports
        for warning in source.warnings
    ]
    aggregation_status = (
        "blocked" if blocked_sources else ("warning" if warnings else "loaded")
    )
    return {
        "schema_version": STRATEGY_EVIDENCE_AGGREGATION_SCHEMA_VERSION,
        "aggregation_id": _stable_id("strategy-evidence-aggregation", run_date.isoformat()),
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "aggregation_status": aggregation_status,
        "loaded_sources": loaded_sources,
        "missing_sources": missing_sources,
        "stale_sources": stale_sources,
        "blocked_sources": blocked_sources,
        "warnings": warnings,
        "source_report_paths": [
            source.source_report_path for source in source_reports
        ],
        "source_reports": [source.model_dump(mode="json") for source in source_reports],
        "safety": dict(STRATEGY_EVIDENCE_SAFETY),
        **STRATEGY_EVIDENCE_SAFETY,
    }


def build_strategy_evidence_cards(
    aggregation_payload: Mapping[str, Any],
    *,
    config: StrategyEvidenceDashboardConfig | None = None,
) -> list[StrategyEvidenceCard]:
    source_reports = [
        StrategyEvidenceSourceReport.model_validate(item)
        for item in _records(aggregation_payload.get("source_reports"))
    ]
    categories = _category_config(config)
    cards: list[StrategyEvidenceCard] = []
    for category in sorted(_ALLOWED_CATEGORIES):
        category_sources = [source for source in source_reports if source.category == category]
        if not category_sources and category not in categories:
            continue
        title = categories.get(category, {}).get("title") or _category_title(category)
        card = _card_for_category(
            category=category,  # type: ignore[arg-type]
            title=title,
            sources=category_sources,
            as_of=_parse_date(aggregation_payload.get("as_of_date")),
            default_manual_review_action=categories.get(category, {}).get(
                "manual_review_action",
                "continue_observation",
            ),
        )
        cards.append(card)
    return cards


def build_candidate_evidence_rankings(
    cards: Sequence[StrategyEvidenceCard],
) -> list[StrategyCandidateEvidenceRanking]:
    by_category = {card.category: card for card in cards}
    journal_bonus = 5.0 if by_category.get("decision_journal", None) and (
        by_category["decision_journal"].status in {"supportive", "strong_support"}
    ) else 0.0
    candidates: list[StrategyCandidateEvidenceRanking] = []
    for category, candidate_type in _CANDIDATE_CATEGORY_TYPES.items():
        card = by_category.get(category)
        if card is None:
            continue
        base_score = _status_score(card.status)
        freshness_score = _freshness_score(card.freshness_status)
        quality_score = _quality_score(card.data_quality_status)
        sample_score = _sample_score(card.sample_count)
        evidence_score = min(base_score, freshness_score, quality_score)
        evidence_score = min(100.0, max(0.0, evidence_score + sample_score + journal_bonus))
        status = card.status
        if quality_score == 0:
            status = "blocked"
            evidence_score = 0.0
        elif card.sample_count < 1 and status not in {"blocked", "invalid", "stale"}:
            status = "needs_more_data"
            evidence_score = min(evidence_score, 40.0)
        candidates.append(
            StrategyCandidateEvidenceRanking(
                candidate_id=f"{category}:aggregate",
                candidate_type=candidate_type,
                rank=1,
                evidence_score=round(evidence_score, 3),
                status=status,
                supporting_evidence=card.supporting_evidence,
                blocking_evidence=card.blocking_evidence,
                manual_review_priority=_ranking_priority(status),
                dimension_scores={
                    "forward_performance": round(base_score, 3),
                    "drawdown_control": round(min(base_score, quality_score), 3),
                    "turnover": round(freshness_score, 3),
                    "stability": round((base_score + sample_score) / 2.0, 3),
                    "attribution_support": round(base_score, 3),
                    "journal_support": journal_bonus,
                    "data_quality": round(quality_score, 3),
                    "freshness": round(freshness_score, 3),
                    "sample_size": round(sample_score, 3),
                },
            )
        )
    ordered = sorted(candidates, key=lambda item: (-item.evidence_score, item.candidate_id))
    return [
        item.model_copy(update={"rank": index})
        for index, item in enumerate(ordered, start=1)
    ]


def build_evidence_conflicts(
    cards: Sequence[StrategyEvidenceCard],
) -> list[StrategyEvidenceConflict]:
    by_category = {card.category: card for card in cards}
    conflicts: list[StrategyEvidenceConflict] = []
    _maybe_add_conflict(
        conflicts,
        by_category,
        support_category="weight_calibration",
        blocking_category="forward_simulation",
        conflict_type="historical_backtest_strong_forward_weak",
        support_statuses={"supportive", "strong_support"},
        blocking_statuses={"weak", "blocked", "needs_more_data", "stale", "invalid"},
        severity="high",
        action="review_candidate",
    )
    _maybe_add_conflict(
        conflicts,
        by_category,
        support_category="ai_attribution",
        blocking_category="data_quality",
        conflict_type="ai_attribution_support_data_quality_blocked",
        support_statuses={"supportive", "strong_support"},
        blocking_statuses={"weak", "blocked", "stale", "invalid"},
        severity="high",
        action="review_blocker",
    )
    _maybe_add_conflict(
        conflicts,
        by_category,
        support_category="satellite_replacement",
        blocking_category="satellite_attribution",
        conflict_type="satellite_candidate_positive_attribution_negative",
        support_statuses={"supportive", "strong_support"},
        blocking_statuses={"weak", "blocked", "mixed", "invalid"},
        severity="medium",
        action="continue_observation",
    )
    _maybe_add_conflict(
        conflicts,
        by_category,
        support_category="parameter_review",
        blocking_category="decision_journal",
        conflict_type="parameter_proposal_journal_rejected_or_deferred",
        support_statuses={"supportive", "strong_support"},
        blocking_statuses={"weak", "blocked", "mixed", "invalid"},
        severity="medium",
        action="defer_decision",
    )
    _maybe_add_conflict(
        conflicts,
        by_category,
        support_category="operations_health",
        blocking_category="validation_gates",
        conflict_type="operations_health_pass_validation_gate_stale",
        support_statuses={"supportive", "strong_support"},
        blocking_statuses={"stale", "blocked", "invalid"},
        severity="medium",
        action="review_blocker",
    )
    return conflicts


def build_manual_review_priorities(
    *,
    cards: Sequence[StrategyEvidenceCard],
    conflicts: Sequence[StrategyEvidenceConflict],
    rankings: Sequence[StrategyCandidateEvidenceRanking],
    generated_at: datetime | None = None,
) -> list[StrategyManualReviewPriority]:
    created = _coerce_datetime(generated_at or datetime.now(UTC))
    priorities: list[StrategyManualReviewPriority] = []
    for card in cards:
        if card.status in {"blocked", "invalid"}:
            priorities.append(
                _manual_priority(
                    f"card:{card.card_id}:blocker",
                    "critical",
                    card.category,
                    f"{card.title}: status={card.status}",
                    "review_blocker",
                    card.source_report_paths,
                    created,
                )
            )
        elif card.status == "stale":
            priorities.append(
                _manual_priority(
                    f"card:{card.card_id}:stale",
                    "high",
                    card.category,
                    f"{card.title}: report freshness is stale",
                    "review_blocker",
                    card.source_report_paths,
                    created,
                )
            )
        elif card.status == "needs_more_data":
            priorities.append(
                _manual_priority(
                    f"card:{card.card_id}:needs_more_data",
                    "medium",
                    card.category,
                    f"{card.title}: sample_count={card.sample_count}",
                    "request_more_data",
                    card.source_report_paths,
                    created,
                )
            )
    for conflict in conflicts:
        priorities.append(
            _manual_priority(
                f"conflict:{conflict.conflict_id}",
                "high" if conflict.severity in {"critical", "high"} else "medium",
                conflict.affected_candidate_or_component,
                conflict.conflict_type,
                conflict.manual_review_action,
                conflict.source_links,
                created,
            )
        )
    for ranking in rankings:
        if ranking.status in {"blocked", "invalid", "needs_more_data", "weak"}:
            links = [
                item.source_report_path
                for item in [*ranking.supporting_evidence, *ranking.blocking_evidence]
            ]
            action: ManualReviewAction = (
                "request_more_data"
                if ranking.status == "needs_more_data"
                else "review_candidate"
            )
            priorities.append(
                _manual_priority(
                    f"candidate:{ranking.candidate_id}",
                    ranking.manual_review_priority,
                    ranking.candidate_id,
                    f"candidate status={ranking.status}; evidence_score={ranking.evidence_score}",
                    action,
                    links,
                    created,
                )
            )
    return _dedupe_priorities(priorities)


def build_strategy_evidence_dashboard(
    *,
    as_of: date | str,
    config: StrategyEvidenceDashboardConfig | None = None,
    config_path: Path | str = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_index: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> StrategyEvidenceDashboard:
    run_date = _parse_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    registry_config = config or load_strategy_evidence_dashboard_config(config_path)
    aggregation = build_strategy_evidence_aggregation(
        as_of=run_date,
        config=registry_config,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
        generated_at=generated,
    )
    cards = build_strategy_evidence_cards(aggregation, config=registry_config)
    rankings = build_candidate_evidence_rankings(cards)
    conflicts = build_evidence_conflicts(cards)
    priorities = build_manual_review_priorities(
        cards=cards,
        conflicts=conflicts,
        rankings=rankings,
        generated_at=generated,
    )
    config_bundle = _safe_config_bundle()
    data_quality_overlay = _data_quality_overlay(cards)
    return StrategyEvidenceDashboard(
        dashboard_id=_stable_id("strategy-evidence-dashboard", run_date.isoformat()),
        as_of_date=run_date,
        generated_at=generated,
        model_version=config_bundle["model_version"],
        config_hash=config_bundle["config_hash"],
        overall_status=_overall_status(cards),
        evidence_cards=cards,
        candidate_rankings=rankings,
        conflicts=conflicts,
        data_quality_overlay=data_quality_overlay,
        manual_review_priorities=priorities,
        source_reports=[
            StrategyEvidenceSourceReport.model_validate(item)
            for item in _records(aggregation.get("source_reports"))
        ],
        source_schema_versions={
            "registry": registry_config.schema_version,
            "aggregation": STRATEGY_EVIDENCE_AGGREGATION_SCHEMA_VERSION,
        },
        safety=registry_config.safety,
    )


def render_strategy_evidence_dashboard_markdown(
    dashboard: StrategyEvidenceDashboard,
) -> str:
    payload = dashboard.model_dump(mode="json")
    data_quality_overlay = payload["data_quality_overlay"]
    lines = [
        "# ETF Strategy Evidence Dashboard",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
        "| observe_only | true |",
        "| candidate_only | true |",
        "| production_effect | none |",
        "| broker_action | none |",
        "| manual_review_required | true |",
        "",
        "## Dashboard Metadata / Dashboard 元数据",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| dashboard_id | `{dashboard.dashboard_id}` |",
        f"| as_of_date | `{dashboard.as_of_date.isoformat()}` |",
        f"| generated_at | `{dashboard.generated_at.isoformat()}` |",
        f"| model_version | `{_escape_md(dashboard.model_version)}` |",
        f"| config_hash | `{_escape_md(dashboard.config_hash)}` |",
        f"| overall_status | `{dashboard.overall_status}` |",
        "",
        "## Strategy Component Evidence Cards / 策略组件证据卡",
        "",
        (
            "| Category | Status | Confidence | Sample | Freshness | Data Quality | "
            "Validation | Manual Review | Source Links |"
        ),
        "|---|---|---|---:|---|---|---|---|---|",
    ]
    for card in dashboard.evidence_cards:
        lines.append(
            f"| `{card.category}` | `{card.status}` | `{card.confidence}` | "
            f"{card.sample_count} | `{_escape_md(card.freshness_status)}` | "
            f"`{_escape_md(card.data_quality_status)}` | "
            f"`{_escape_md(card.validation_status)}` | "
            f"`{card.manual_review_action}` | {_markdown_links(card.source_report_paths)} |"
        )
    lines.extend(
        [
            "",
            "## Candidate Evidence Ranking / 候选证据排序",
            "",
            "| Rank | Candidate | Type | Score | Status | Manual Priority |",
            "|---:|---|---|---:|---|---|",
        ]
    )
    if not dashboard.candidate_rankings:
        lines.append("| 0 | none | none | 0 | none | none |")
    for item in dashboard.candidate_rankings:
        lines.append(
            f"| {item.rank} | `{_escape_md(item.candidate_id)}` | "
            f"`{item.candidate_type}` | {item.evidence_score:.3f} | "
            f"`{item.status}` | `{item.manual_review_priority}` |"
        )
    lines.extend(
        [
            "",
            "## Evidence Conflicts / 证据冲突",
            "",
            "| Conflict | Component | Type | Severity | Action | Sources |",
            "|---|---|---|---|---|---|",
        ]
    )
    if not dashboard.conflicts:
        lines.append("| none | none | none | none | none | none |")
    for conflict in dashboard.conflicts:
        lines.append(
            f"| `{_escape_md(conflict.conflict_id)}` | "
            f"`{_escape_md(conflict.affected_candidate_or_component)}` | "
            f"`{_escape_md(conflict.conflict_type)}` | `{conflict.severity}` | "
            f"`{conflict.manual_review_action}` | {_markdown_links(conflict.source_links)} |"
        )
    lines.extend(
        [
            "",
            "## Data Quality Overlay / 数据质量叠加",
            "",
            f"- Status: `{data_quality_overlay.get('status', 'unknown')}`",
            f"- Blocked card count: `{data_quality_overlay.get('blocked_card_count', 0)}`",
            f"- Stale card count: `{data_quality_overlay.get('stale_card_count', 0)}`",
            "",
            "## Validation Gate Summary / Validation Gate 摘要",
            "",
            "| Source | Validation Status | Freshness | Data Quality |",
            "|---|---|---|---|",
        ]
    )
    for source in dashboard.source_reports:
        lines.append(
            f"| `{source.source_id}` | `{_escape_md(source.validation_status)}` | "
            f"`{_escape_md(source.freshness_status)}` | "
            f"`{_escape_md(source.data_quality_status)}` |"
        )
    lines.extend(
        [
            "",
            "## Manual Review Priority Queue / 人工复核优先队列",
            "",
            "| Priority | Level | Component | Issue | Action | Sources |",
            "|---|---|---|---|---|---|",
        ]
    )
    if not dashboard.manual_review_priorities:
        lines.append("| none | none | none | none | none | none |")
    for item in dashboard.manual_review_priorities:
        lines.append(
            f"| `{_escape_md(item.priority_id)}` | `{item.priority_level}` | "
            f"`{_escape_md(item.source_component)}` | "
            f"{_escape_md(item.issue)} | `{item.recommended_review_action}` | "
            f"{_markdown_links(item.evidence_links)} |"
        )
    lines.extend(
        [
            "",
            "## Source Report Links / Source Report Links",
            "",
            "| Source | Status | Required | Path |",
            "|---|---|---|---|",
        ]
    )
    for source in dashboard.source_reports:
        lines.append(
            f"| `{source.source_id}` | `{source.load_status}` | "
            f"`{str(source.required).lower()}` | `{_escape_md(source.source_report_path)}` |"
        )
    lines.extend(
        [
            "",
            "## Next Steps / 下一步",
            "",
            "- 优先处理 `critical` / `high` manual review priority。",
            "- 对 `needs_more_data` 组件继续 observe-only 样本积累。",
            "- 对 `blocked` / `stale` evidence 先修复 data quality、freshness 或 validation gate。",
            "- 不得把 dashboard 输出解释为自动 promotion、baseline replacement 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def write_strategy_evidence_dashboard_report(
    dashboard: StrategyEvidenceDashboard,
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(dashboard.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_strategy_evidence_dashboard_markdown(dashboard),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def build_strategy_evidence_validation_report(
    *,
    as_of: date | str | None = None,
    config_path: Path | str = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of or date.today())
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    config: StrategyEvidenceDashboardConfig | None = None
    try:
        config = load_strategy_evidence_dashboard_config(config_path)
        _append_check(checks, "source_registry_valid", True, "source registry loads")
    except Exception as exc:  # noqa: BLE001 - validation gate records fail-closed issue.
        _append_check(
            checks,
            "source_registry_valid",
            False,
            "source registry failed",
            {"error": str(exc), "error_type": type(exc).__name__},
        )
    if config is not None:
        _append_check(
            checks,
            "dashboard_schema_valid",
            _schema_probe(run_date, generated),
            "dashboard schema validates complete sample",
        )
        _append_check(
            checks,
            "aggregator_available",
            _aggregation_probe(config, run_date, generated),
            "aggregator creates stable source report set",
        )
        _append_check(
            checks,
            "evidence_cards_available",
            _cards_probe(config, run_date, generated),
            "component evidence cards are available",
        )
        _append_check(
            checks,
            "candidate_ranking_available",
            _ranking_probe(run_date),
            "candidate ranking is deterministic",
        )
        _append_check(
            checks,
            "conflict_overlay_available",
            _conflict_probe(run_date),
            "conflict overlay detects expected conflicts",
        )
        _append_check(
            checks,
            "manual_review_queue_available",
            _manual_queue_probe(run_date, generated),
            "manual review priority queue is available",
        )
        _append_check(
            checks,
            "report_generator_available",
            _report_probe(config, run_date, generated),
            "JSON and Markdown report generator is available",
        )
        _append_check(
            checks,
            "reader_brief_integration_available",
            _registry_has_strategy_evidence_dashboard(report_registry_path),
            "Reader Brief can discover strategy evidence dashboard registry entry",
        )
        _append_check(
            checks,
            "safety_boundary_safe",
            config.safety.model_dump(mode="json") == STRATEGY_EVIDENCE_SAFETY,
            "production_effect=none; broker_action=none; manual_review_required=true",
        )
    failed = [check for check in checks if check["status"] == "FAIL"]
    return {
        "schema_version": STRATEGY_EVIDENCE_VALIDATION_SCHEMA_VERSION,
        "report_type": STRATEGY_EVIDENCE_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id("strategy-evidence-validation", run_date.isoformat()),
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "checks": checks,
        "failed_check_count": len(failed),
        "warning_check_count": len([check for check in checks if check["status"] == "WARNING"]),
        "source_schema_versions": {
            "registry": STRATEGY_EVIDENCE_REGISTRY_SCHEMA_VERSION,
            "dashboard": STRATEGY_EVIDENCE_DASHBOARD_SCHEMA_VERSION,
            "aggregation": STRATEGY_EVIDENCE_AGGREGATION_SCHEMA_VERSION,
        },
        "safety": dict(STRATEGY_EVIDENCE_SAFETY),
        "commands_executed": False,
        "production_state_mutated": False,
        **STRATEGY_EVIDENCE_SAFETY,
    }


def render_strategy_evidence_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Strategy Evidence Dashboard Validation Gate",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
        "| observe_only | true |",
        "| candidate_only | true |",
        "| production_effect | none |",
        "| broker_action | none |",
        "| manual_review_required | true |",
        "",
        "## Status / 状态",
        "",
        f"- Status: `{_text(payload.get('status'), 'UNKNOWN')}`",
        f"- Failed checks: `{payload.get('failed_check_count', 0)}`",
        "",
        "## Checks / 校验项",
        "",
        "| Check | Status | Summary |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| `{_escape_md(_text(check.get('check_id')))}` | "
            f"`{_escape_md(_text(check.get('status')))}` | "
            f"{_escape_md(_text(check.get('summary')))} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_strategy_evidence_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_strategy_evidence_validation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def _source_report_from_config(
    *,
    source_id: str,
    source: StrategyEvidenceSourceConfig,
    as_of: date,
    report_index: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    report_registry_path: Path,
) -> StrategyEvidenceSourceReport:
    index_record = _report_index_record(report_index, source.report_id)
    registry_record = _registry_record(report_registry, source.report_id)
    path = _source_path(index_record, registry_record, report_registry_path)
    exists = bool(index_record.get("exists")) and not path.startswith("config/")
    payload = _read_json_object(Path(path)) if exists and path.endswith(".json") else {}
    freshness_status = _text(index_record.get("freshness_status"), "MISSING")
    artifact_status = _text(index_record.get("artifact_status"), "MISSING")
    age_days = _int_or_none(index_record.get("age_days"))
    data_quality_status = _extract_data_quality_status(payload)
    validation_status = _validation_status(
        source=source,
        report_index=report_index,
        artifact_status=artifact_status,
        payload=payload,
    )
    raw_sample_count = _sample_count(payload, source.sample_count_paths)
    sample_count = (
        raw_sample_count
        if raw_sample_count > 0
        else (1 if exists and source.minimum_sample_count == 0 else 0)
    )
    source_metrics = {
        metric_path: _get_path(payload, metric_path)
        for metric_path in source.source_metric_paths
        if _get_path(payload, metric_path) not in (None, "")
    }
    warnings: list[str] = []
    blockers: list[str] = []
    if not exists:
        status: SourceLoadStatus = "missing" if source.required else "optional_missing"
        warning = f"{source_id}:source_missing:{source.report_id}"
        (blockers if source.required else warnings).append(warning)
    elif freshness_status.upper() == "STALE" or (
        age_days is not None and age_days > source.max_age_days
    ):
        status = "stale"
        message = f"{source_id}:source_stale:age_days={age_days};max_age_days={source.max_age_days}"
        (blockers if source.required else warnings).append(message)
    elif _is_blocked_status(artifact_status) or _is_blocked_status(data_quality_status):
        status = "blocked"
        blockers.append(f"{source_id}:source_blocked:{artifact_status}/{data_quality_status}")
    else:
        status = "loaded"
    return StrategyEvidenceSourceReport(
        source_id=source_id,
        category=source.category,
        title=source.title,
        source_module=source.source_module,
        report_id=source.report_id,
        required=source.required,
        load_status=status,
        source_report_path=path,
        freshness_status=freshness_status,
        data_quality_status=data_quality_status,
        validation_status=validation_status,
        artifact_status=artifact_status,
        sample_count=sample_count,
        age_days=age_days,
        source_metrics=source_metrics,
        warnings=warnings,
        blockers=blockers,
    )


def _card_for_category(
    *,
    category: EvidenceCategory,
    title: str,
    sources: Sequence[StrategyEvidenceSourceReport],
    as_of: date,
    default_manual_review_action: ManualReviewAction,
) -> StrategyEvidenceCard:
    if not sources:
        synthetic_path = f"config/report_registry.yaml#{category}"
        item = StrategyEvidenceItem(
            evidence_id=f"{category}:missing_source",
            source_module="report_registry",
            source_report_path=synthetic_path,
            source_metric="source_availability",
            as_of_date=as_of,
            freshness_status="MISSING",
            data_quality_status="UNKNOWN",
            sample_count_if_applicable=0,
            summary=f"{title}: no configured source report found.",
            value="MISSING",
        )
        return StrategyEvidenceCard(
            card_id=f"{category}:card",
            category=category,
            title=title,
            status="blocked",
            confidence="none",
            summary=f"{title}: no source report available.",
            supporting_evidence=[],
            blocking_evidence=[item],
            metrics={},
            sample_count=0,
            freshness_status="MISSING",
            data_quality_status="UNKNOWN",
            validation_status="UNKNOWN",
            source_report_paths=[synthetic_path],
            manual_review_action="review_blocker",
        )

    blockers = [source for source in sources if source.blockers or source.load_status == "blocked"]
    stale = [source for source in sources if source.load_status == "stale"]
    missing_required = [
        source for source in sources if source.required and source.load_status == "missing"
    ]
    sample_count = max([source.sample_count for source in sources] or [0])
    status = _category_status(
        sources=sources,
        blockers=blockers,
        stale=stale,
        missing_required=missing_required,
        sample_count=sample_count,
    )
    confidence = _confidence_for_status(status, sample_count)
    evidence_items = [
        _evidence_item_from_source(
            source,
            as_of=as_of,
            evidence_role="supporting" if source.load_status == "loaded" else "blocking",
        )
        for source in sources
    ]
    supporting = [
        item
        for item, source in zip(evidence_items, sources, strict=True)
        if source.load_status == "loaded"
    ]
    blocking = [
        item
        for item, source in zip(evidence_items, sources, strict=True)
        if source.load_status != "loaded"
    ]
    if not blocking and status in {"needs_more_data", "mixed", "weak"}:
        blocking = [
            _evidence_item_from_source(
                sources[0],
                as_of=as_of,
                evidence_role="blocking",
                summary_override=f"{title}: evidence remains {status}.",
            )
        ]
    if not supporting and evidence_items:
        supporting = [evidence_items[0]]
    return StrategyEvidenceCard(
        card_id=f"{category}:card",
        category=category,
        title=title,
        status=status,
        confidence=confidence,
        summary=_card_summary(
            title=title,
            status=status,
            sources=sources,
            sample_count=sample_count,
        ),
        supporting_evidence=supporting,
        blocking_evidence=blocking,
        metrics=_merge_metrics(sources),
        sample_count=sample_count,
        freshness_status=_combined_status([source.freshness_status for source in sources]),
        data_quality_status=_combined_status([source.data_quality_status for source in sources]),
        validation_status=_combined_status([source.validation_status for source in sources]),
        source_report_paths=[source.source_report_path for source in sources],
        manual_review_action=_manual_action_for_status(status, default_manual_review_action),
    )


def _evidence_item_from_source(
    source: StrategyEvidenceSourceReport,
    *,
    as_of: date,
    evidence_role: str,
    summary_override: str | None = None,
) -> StrategyEvidenceItem:
    metric = next(iter(source.source_metrics), "artifact_status")
    value = source.source_metrics.get(metric, source.artifact_status)
    return StrategyEvidenceItem(
        evidence_id=f"{source.source_id}:{evidence_role}",
        source_module=source.source_module,
        source_report_path=source.source_report_path,
        source_metric=metric,
        as_of_date=as_of,
        freshness_status=source.freshness_status,
        data_quality_status=source.data_quality_status,
        sample_count_if_applicable=source.sample_count,
        summary=summary_override
        or (
            f"{source.title}: load_status={source.load_status}; "
            f"artifact_status={source.artifact_status}; sample_count={source.sample_count}"
        ),
        value=value,
    )


def _category_status(
    *,
    sources: Sequence[StrategyEvidenceSourceReport],
    blockers: Sequence[StrategyEvidenceSourceReport],
    stale: Sequence[StrategyEvidenceSourceReport],
    missing_required: Sequence[StrategyEvidenceSourceReport],
    sample_count: int,
) -> EvidenceStatus:
    if missing_required or blockers:
        return "blocked"
    if any(
        _is_blocked_status(source.data_quality_status)
        or _is_blocked_status(source.artifact_status)
        for source in sources
    ):
        return "blocked"
    if stale:
        return "stale"
    statuses = {_text(source.artifact_status).lower() for source in sources}
    metric_text = json.dumps([source.source_metrics for source in sources], sort_keys=True).lower()
    if any(token in metric_text for token in ("blocked", "fail", "rejected", "negative")):
        return "weak"
    if sample_count <= 0 or any("insufficient" in status for status in statuses):
        return "needs_more_data"
    if any(status in {"warning", "pass_with_warnings", "mixed"} for status in statuses):
        return "mixed"
    if any(token in metric_text for token in ("strong", "eligible", "positive", "support")):
        return "strong_support"
    return "supportive"


def _overall_status(cards: Sequence[StrategyEvidenceCard]) -> EvidenceStatus:
    statuses = [card.status for card in cards]
    if any(status in {"blocked", "invalid"} for status in statuses):
        return "blocked"
    if any(status == "stale" for status in statuses):
        return "stale"
    if any(status in {"mixed", "weak"} for status in statuses):
        return "mixed"
    if any(status == "needs_more_data" for status in statuses):
        return "needs_more_data"
    if statuses and all(status == "strong_support" for status in statuses):
        return "strong_support"
    return "supportive"


def _data_quality_overlay(cards: Sequence[StrategyEvidenceCard]) -> dict[str, Any]:
    blocked = [
        card.category for card in cards if _is_blocked_status(card.data_quality_status)
    ]
    stale = [card.category for card in cards if card.status == "stale"]
    unknown = [
        card.category
        for card in cards
        if card.data_quality_status.upper() in {"", "UNKNOWN", "MISSING"}
    ]
    return {
        "status": "blocked" if blocked else ("warning" if stale or unknown else "pass"),
        "blocked_card_count": len(blocked),
        "stale_card_count": len(stale),
        "unknown_quality_card_count": len(unknown),
        "blocked_categories": list(blocked),
        "stale_categories": list(stale),
        "unknown_quality_categories": list(unknown),
    }


def _resolve_report_index(
    *,
    as_of: date,
    report_index: Mapping[str, Any] | None,
    report_index_path: Path | None,
    report_registry_path: Path,
    root_path: Path,
) -> Mapping[str, Any]:
    if report_index is not None:
        return report_index
    if report_index_path is not None and report_index_path.exists():
        payload = _read_json_object(report_index_path)
        if payload:
            return payload
    return build_report_index_payload(
        as_of=as_of,
        project_root=root_path,
        registry_path=report_registry_path,
    )


def _report_index_record(report_index: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(report_index.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return report
    return {
        "report_id": report_id,
        "latest_artifact_path": "",
        "freshness_status": "MISSING",
        "artifact_status": "MISSING",
        "exists": False,
    }


def _registry_record(registry: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(registry.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return report
    return {}


def _source_path(
    index_record: Mapping[str, Any],
    registry_record: Mapping[str, Any],
    report_registry_path: Path,
) -> str:
    latest_path = _text(index_record.get("latest_artifact_path"))
    if latest_path:
        return latest_path
    report_id = _text(index_record.get("report_id"), _text(registry_record.get("report_id")))
    return f"{report_registry_path.as_posix()}#{report_id}"


def _safe_report_registry(path: Path) -> Mapping[str, Any]:
    try:
        return load_report_registry(path)
    except Exception:  # noqa: BLE001 - missing registry is represented in source links.
        return {}


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_data_quality_status(payload: Mapping[str, Any]) -> str:
    candidates = [
        _get_path(payload, "data_quality.status"),
        _get_path(payload, "data_quality_status"),
        _get_path(payload, "data_quality_report.status"),
        _get_path(payload, "run_metadata.data_quality_status"),
        _get_path(payload, "safety_banner.data_quality_status"),
    ]
    for value in candidates:
        text = _text(value)
        if text:
            return text
    if payload:
        return "UNKNOWN"
    return "MISSING"


def _validation_status(
    *,
    source: StrategyEvidenceSourceConfig,
    report_index: Mapping[str, Any],
    artifact_status: str,
    payload: Mapping[str, Any],
) -> str:
    if source.validation_report_id:
        record = _report_index_record(report_index, source.validation_report_id)
        status = _text(record.get("artifact_status")) or _text(record.get("freshness_status"))
        if status:
            return status
    for path in ("validation_status", "gate_status", "status"):
        status = _text(_get_path(payload, path))
        if status:
            return status
    return artifact_status or "UNKNOWN"


def _sample_count(payload: Mapping[str, Any], paths: Sequence[str]) -> int:
    for path in paths:
        parsed = _int_or_none(_get_path(payload, path))
        if parsed is not None:
            return max(parsed, 0)
    for key in (
        "sample_count",
        "available_sample_count",
        "record_count",
        "candidate_count",
        "source_count",
        "entry_count",
        "manual_review_priority_count",
    ):
        parsed = _int_or_none(_first_nested_value(payload, key))
        if parsed is not None:
            return max(parsed, 0)
    return 0


def _get_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _first_nested_value(payload: Any, key: str) -> Any:
    if isinstance(payload, Mapping):
        if key in payload:
            return payload[key]
        for value in payload.values():
            found = _first_nested_value(value, key)
            if found not in (None, ""):
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _first_nested_value(value, key)
            if found not in (None, ""):
                return found
    return None


def _is_blocked_status(value: object) -> bool:
    text = _text(value).upper()
    return text in {"FAIL", "FAILED", "BLOCKED", "CRITICAL", "INVALID"} or text.startswith("FAIL")


def _combined_status(values: Sequence[str]) -> str:
    clean = [_text(value, "UNKNOWN") for value in values if _text(value, "UNKNOWN")]
    if not clean:
        return "UNKNOWN"
    upper = [value.upper() for value in clean]
    if any(value in {"FAIL", "FAILED", "BLOCKED", "CRITICAL", "INVALID"} for value in upper):
        return "BLOCKED"
    if any(value == "STALE" for value in upper):
        return "STALE"
    if any(value in {"WARNING", "PASS_WITH_WARNINGS", "MIXED"} for value in upper):
        return "WARNING"
    if any(value in {"MISSING", "UNKNOWN"} for value in upper):
        return "UNKNOWN"
    if all(value in {"PASS", "FRESH", "AVAILABLE", "LOADED"} for value in upper):
        return "PASS"
    return clean[0]


def _confidence_for_status(status: EvidenceStatus, sample_count: int) -> EvidenceConfidence:
    if status in {"blocked", "invalid", "stale"}:
        return "none"
    if status == "needs_more_data" or sample_count < 1:
        return "low"
    if status in {"mixed", "weak"}:
        return "medium"
    return "high"


def _manual_action_for_status(
    status: EvidenceStatus,
    default_action: ManualReviewAction,
) -> ManualReviewAction:
    return {
        "blocked": "review_blocker",
        "invalid": "review_blocker",
        "stale": "review_blocker",
        "needs_more_data": "request_more_data",
        "weak": "reject_after_review",
        "mixed": "review_candidate",
    }.get(status, default_action)


def _card_summary(
    *,
    title: str,
    status: EvidenceStatus,
    sources: Sequence[StrategyEvidenceSourceReport],
    sample_count: int,
) -> str:
    data_quality = _combined_status([source.data_quality_status for source in sources])
    freshness = _combined_status([source.freshness_status for source in sources])
    return (
        f"{title}: status={status}; source_count={len(sources)}; "
        f"sample_count={sample_count}; data_quality={data_quality}; "
        f"freshness={freshness}."
    )


def _merge_metrics(sources: Sequence[StrategyEvidenceSourceReport]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for source in sources:
        for key, value in source.source_metrics.items():
            metrics[f"{source.source_id}.{key}"] = value
        metrics[f"{source.source_id}.sample_count"] = source.sample_count
        metrics[f"{source.source_id}.artifact_status"] = source.artifact_status
    return metrics


def _status_score(status: EvidenceStatus) -> float:
    return {
        "strong_support": 90.0,
        "supportive": 75.0,
        "mixed": 55.0,
        "needs_more_data": 35.0,
        "weak": 25.0,
        "stale": 20.0,
        "blocked": 0.0,
        "invalid": 0.0,
    }[status]


def _freshness_score(value: str) -> float:
    text = value.upper()
    if text in {"FRESH", "PASS", "AVAILABLE", "LOADED"}:
        return 100.0
    if text in {"WARNING", "AVAILABLE_DATE_UNKNOWN", "UNKNOWN"}:
        return 60.0
    if text == "STALE":
        return 30.0
    return 10.0


def _quality_score(value: str) -> float:
    text = value.upper()
    if _is_blocked_status(text):
        return 0.0
    if text in {"PASS", "FRESH", "AVAILABLE"}:
        return 100.0
    if "WARNING" in text:
        return 65.0
    if text in {"UNKNOWN", "MISSING"}:
        return 45.0
    return 60.0


def _sample_score(sample_count: int) -> float:
    if sample_count <= 0:
        return -10.0
    if sample_count < 3:
        return 0.0
    if sample_count < 10:
        return 5.0
    return 10.0


def _ranking_priority(status: EvidenceStatus) -> ManualReviewPriorityLevel:
    if status in {"blocked", "invalid"}:
        return "critical"
    if status in {"stale", "weak"}:
        return "high"
    if status in {"needs_more_data", "mixed"}:
        return "medium"
    return "low"


def _maybe_add_conflict(
    conflicts: list[StrategyEvidenceConflict],
    by_category: Mapping[EvidenceCategory, StrategyEvidenceCard],
    *,
    support_category: EvidenceCategory,
    blocking_category: EvidenceCategory,
    conflict_type: str,
    support_statuses: set[str],
    blocking_statuses: set[str],
    severity: ConflictSeverity,
    action: ManualReviewAction,
) -> None:
    support = by_category.get(support_category)
    blocking = by_category.get(blocking_category)
    if support is None or blocking is None:
        return
    if support.status not in support_statuses or blocking.status not in blocking_statuses:
        return
    source_links = _unique_strings([*support.source_report_paths, *blocking.source_report_paths])
    conflicts.append(
        StrategyEvidenceConflict(
            conflict_id=f"{conflict_type}:{support_category}:{blocking_category}",
            affected_candidate_or_component=f"{support_category}/{blocking_category}",
            conflict_type=conflict_type,
            supporting_side=f"{support_category}:{support.status}",
            blocking_side=f"{blocking_category}:{blocking.status}",
            severity=severity,
            source_links=source_links,
            manual_review_action=action,
        )
    )


def _manual_priority(
    priority_id: str,
    priority_level: ManualReviewPriorityLevel,
    source_component: str,
    issue: str,
    action: ManualReviewAction,
    evidence_links: Sequence[str],
    created_at: datetime,
) -> StrategyManualReviewPriority:
    return StrategyManualReviewPriority(
        priority_id=priority_id,
        priority_level=priority_level,
        source_component=source_component,
        issue=issue,
        recommended_review_action=action,
        evidence_links=(
            _unique_strings(evidence_links)
            or [f"config/report_registry.yaml#{source_component}"]
        ),
        created_at=created_at,
    )


def _dedupe_priorities(
    priorities: Sequence[StrategyManualReviewPriority],
) -> list[StrategyManualReviewPriority]:
    priority_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    seen: set[str] = set()
    result: list[StrategyManualReviewPriority] = []
    for item in sorted(
        priorities,
        key=lambda value: (priority_rank[value.priority_level], value.priority_id),
    ):
        if item.priority_id in seen:
            continue
        seen.add(item.priority_id)
        result.append(item)
    return result


def _safe_config_bundle() -> dict[str, str]:
    try:
        config = load_etf_config_bundle()
        return {
            "model_version": config.strategy.model.version,
            "config_hash": config.config_hash,
        }
    except Exception:  # noqa: BLE001 - validation/report still stays auditable.
        return {"model_version": "unknown", "config_hash": "unknown"}


def _schema_probe(run_date: date, generated_at: datetime) -> bool:
    try:
        source = _sample_source("baseline_source", "baseline_allocation", run_date)
        card = _card_for_category(
            category="baseline_allocation",
            title="ETF Baseline Allocation",
            sources=[source],
            as_of=run_date,
            default_manual_review_action="continue_observation",
        )
        StrategyEvidenceDashboard(
            dashboard_id="schema-probe",
            as_of_date=run_date,
            generated_at=generated_at,
            model_version="probe",
            config_hash="probe",
            overall_status="supportive",
            evidence_cards=[card],
            source_reports=[source],
            safety=StrategyEvidenceSafety(**STRATEGY_EVIDENCE_SAFETY),
        )
    except Exception:
        return False
    return True


def _aggregation_probe(
    config: StrategyEvidenceDashboardConfig,
    run_date: date,
    generated_at: datetime,
) -> bool:
    try:
        report_index = _sample_report_index(run_date)
        payload = build_strategy_evidence_aggregation(
            as_of=run_date,
            config=config,
            report_index=report_index,
            generated_at=generated_at,
        )
        return bool(payload["source_reports"])
    except Exception:
        return False


def _cards_probe(
    config: StrategyEvidenceDashboardConfig,
    run_date: date,
    generated_at: datetime,
) -> bool:
    try:
        payload = build_strategy_evidence_aggregation(
            as_of=run_date,
            config=config,
            report_index=_sample_report_index(run_date),
            generated_at=generated_at,
        )
        cards = build_strategy_evidence_cards(payload, config=config)
        return bool(cards) and all(card.source_report_paths for card in cards)
    except Exception:
        return False


def _ranking_probe(run_date: date) -> bool:
    source = _sample_source("forward_source", "forward_simulation", run_date)
    card = _card_for_category(
        category="forward_simulation",
        title="Forward Simulation Candidates",
        sources=[source],
        as_of=run_date,
        default_manual_review_action="continue_observation",
    )
    first = build_candidate_evidence_rankings([card])
    second = build_candidate_evidence_rankings([card])
    return [item.model_dump(mode="json") for item in first] == [
        item.model_dump(mode="json") for item in second
    ]


def _conflict_probe(run_date: date) -> bool:
    support = _card_for_category(
        category="weight_calibration",
        title="Weight Calibration Candidates",
        sources=[_sample_source("weight", "weight_calibration", run_date)],
        as_of=run_date,
        default_manual_review_action="continue_observation",
    )
    blocker_source = _sample_source("forward", "forward_simulation", run_date).model_copy(
        update={"sample_count": 0, "source_metrics": {"status": "insufficient"}}
    )
    blocking = _card_for_category(
        category="forward_simulation",
        title="Forward Simulation Candidates",
        sources=[blocker_source],
        as_of=run_date,
        default_manual_review_action="continue_observation",
    )
    return bool(build_evidence_conflicts([support, blocking]))


def _manual_queue_probe(run_date: date, generated_at: datetime) -> bool:
    source = _sample_source("data_quality", "data_quality", run_date).model_copy(
        update={"data_quality_status": "FAIL", "artifact_status": "FAIL"}
    )
    card = _card_for_category(
        category="data_quality",
        title="Data Quality",
        sources=[source],
        as_of=run_date,
        default_manual_review_action="review_blocker",
    )
    return bool(
        build_manual_review_priorities(
            cards=[card],
            conflicts=[],
            rankings=[],
            generated_at=generated_at,
        )
    )


def _report_probe(
    config: StrategyEvidenceDashboardConfig,
    run_date: date,
    generated_at: datetime,
) -> bool:
    try:
        dashboard = build_strategy_evidence_dashboard(
            as_of=run_date,
            config=config,
            report_index=_sample_report_index(run_date),
            generated_at=generated_at,
        )
        with TemporaryDirectory() as temp_dir:
            paths = write_strategy_evidence_dashboard_report(
                dashboard,
                json_path=Path(temp_dir) / "dashboard.json",
                markdown_path=Path(temp_dir) / "dashboard.md",
            )
            return paths["json"].exists() and paths["markdown"].exists()
    except Exception:
        return False


def _sample_source(
    source_id: str,
    category: EvidenceCategory,
    run_date: date,
) -> StrategyEvidenceSourceReport:
    return StrategyEvidenceSourceReport(
        source_id=source_id,
        category=category,
        title=_category_title(category),
        source_module=f"sample.{category}",
        report_id=f"sample_{category}",
        required=True,
        load_status="loaded",
        source_report_path=f"sample/{category}_{run_date.isoformat()}.json",
        freshness_status="FRESH",
        data_quality_status="PASS",
        validation_status="PASS",
        artifact_status="PASS",
        sample_count=5,
        source_metrics={"status": "supportive", "sample_count": 5},
    )


def _sample_report_index(run_date: date) -> dict[str, Any]:
    return {
        "reports": [
            {
                "report_id": report_id,
                "latest_artifact_path": f"sample/{report_id}_{run_date.isoformat()}.json",
                "freshness_status": "FRESH",
                "artifact_status": "PASS",
                "exists": True,
                "age_days": 0,
            }
            for report_id in (
                "etf_portfolio_brief",
                "etf_weight_dual_track_calibration_report",
                "etf_forward_dashboard",
                "etf_ai_confirmation_report",
                "etf_ai_attribution_report",
                "etf_satellite_replacement_report",
                "etf_satellite_attribution_report",
                "etf_parameter_review_report",
                "etf_weekly_review",
                "etf_decision_journal_report",
                "etf_data_quality_governance_report",
                "etf_operations_health_report",
                "etf_forward_validation",
            )
        ]
    }


def _registry_has_strategy_evidence_dashboard(path: Path) -> bool:
    try:
        registry = load_report_registry(path)
    except Exception:
        return False
    reports = {
        _text(report.get("report_id")): report
        for report in _records(registry.get("reports"))
    }
    report = reports.get(STRATEGY_EVIDENCE_REPORT_REGISTRY_ID)
    validation = reports.get(STRATEGY_EVIDENCE_VALIDATION_REGISTRY_ID)
    return bool(
        report
        and validation
        and report.get("include_in_reader_brief") is True
        and validation.get("include_in_reader_brief") is True
    )


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    summary: str,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "summary": summary,
            "evidence": dict(evidence or {}),
            "production_effect": "none",
        }
    )


def _category_config(
    config: StrategyEvidenceDashboardConfig | None,
) -> dict[str, dict[str, str]]:
    if config is None:
        return {}
    return {
        str(category): {
            "title": value.title,
            "manual_review_action": value.default_manual_review_action,
        }
        for category, value in config.categories.items()
    }


def _category_title(category: str) -> str:
    return {
        "baseline_allocation": "ETF Baseline Allocation",
        "weight_calibration": "Weight Calibration Candidates",
        "forward_simulation": "Forward Simulation Candidates",
        "ai_confirmation": "AI Confirmation",
        "ai_attribution": "AI Attribution",
        "satellite_replacement": "Satellite Replacement",
        "satellite_attribution": "Satellite Attribution",
        "parameter_review": "Parameter Review",
        "weekly_review": "Weekly Review",
        "dynamic_shadow": "Dynamic Shadow Review",
        "decision_journal": "Decision Journal",
        "data_quality": "Data Quality",
        "operations_health": "Operations Health",
        "validation_gates": "Validation Gates",
    }.get(category, category.replace("_", " ").title())


def _parse_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        raise StrategyEvidenceDashboardError("as_of date is required")
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise StrategyEvidenceDashboardError("date must use YYYY-MM-DD") from exc


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _int_or_none(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _unique_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _stable_id(*parts: str) -> str:
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"{parts[0]}:{digest}"


def _markdown_links(paths: Sequence[str]) -> str:
    values = _unique_strings(paths)
    if not values:
        return "none"
    return ", ".join(f"`{_escape_md(value)}`" for value in values)


def _escape_md(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


_CANDIDATE_CATEGORY_TYPES: dict[EvidenceCategory, CandidateType] = {
    "weight_calibration": "weight_calibration_candidate",
    "forward_simulation": "forward_shadow_candidate",
    "ai_confirmation": "AI_overlay_candidate",
    "satellite_replacement": "satellite_replacement_candidate",
    "parameter_review": "parameter_review_proposal",
    "dynamic_shadow": "dynamic_shadow_candidate",
}
