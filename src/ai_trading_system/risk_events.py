from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError

from ai_trading_system.config import (
    IndustryChainConfig,
    RiskEventLevelConfig,
    RiskEventRuleConfig,
    RiskEventsConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
)
from ai_trading_system.source_policy import (
    evidence_grade_allows_automatic_scoring,
    evidence_grade_allows_position_gate,
    evidence_grade_is_report_only,
    source_type_allows_automatic_scoring,
)

RiskEventOccurrenceStatus = Literal["active", "watch", "resolved", "dismissed"]
RiskEventLifecycleState = Literal[
    "extracted",
    "pending_review",
    "confirmed_low",
    "confirmed_medium",
    "confirmed_high",
    "confirmed_thesis_break",
    "resolved",
    "expired",
    "rejected",
]
RiskEventOccurrenceSourceType = Literal[
    "primary_source",
    "paid_vendor",
    "manual_input",
    "public_convenience",
    "llm_extracted",
]
RiskEventReviewAttestationSourceType = Literal[
    "primary_source",
    "paid_vendor",
    "manual_input",
    "public_convenience",
    "llm_extracted",
]
RiskEventEvidenceGrade = Literal["S", "A", "B", "C", "D", "X"]
RiskEventSeverity = Literal["low", "medium", "high", "critical", "unknown"]
RiskEventProbability = Literal["low", "medium", "high", "confirmed", "unknown"]
RiskEventScope = Literal[
    "single_ticker",
    "industry_chain_node",
    "ai_bucket",
    "market_wide",
    "unknown",
]
RiskEventTimeSensitivity = Literal["low", "medium", "high", "immediate", "unknown"]
RiskEventReversibility = Literal[
    "reversible",
    "partly_reversible",
    "hard_to_reverse",
    "unknown",
]
RiskEventActionClass = Literal[
    "monitor_only",
    "manual_review",
    "score_eligible",
    "position_gate_eligible",
]
RiskEventReviewDecision = Literal[
    "confirmed_active",
    "confirmed_watch",
    "confirmed_resolved",
    "dismissed",
    "needs_more_evidence",
]
RiskEventReviewAttestationDecision = Literal[
    "confirmed_no_unrecorded_material_events",
    "needs_more_evidence",
]


class RiskEventIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class RiskEventEvidenceSource(BaseModel):
    source_name: str = Field(min_length=1)
    source_type: RiskEventOccurrenceSourceType
    source_url: str = ""
    published_at: date | None = None
    captured_at: date
    notes: str = ""


class RiskEventOccurrence(BaseModel):
    occurrence_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    event_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    status: RiskEventOccurrenceStatus
    triggered_at: date
    last_confirmed_at: date
    resolved_at: date | None = None
    evidence_grade: RiskEventEvidenceGrade = "B"
    severity: RiskEventSeverity = "unknown"
    probability: RiskEventProbability = "unknown"
    scope: RiskEventScope = "unknown"
    time_sensitivity: RiskEventTimeSensitivity = "unknown"
    reversibility: RiskEventReversibility = "unknown"
    action_class: RiskEventActionClass = "manual_review"
    lifecycle_state: RiskEventLifecycleState | None = None
    dedup_group: str = ""
    primary_channel: str = ""
    used_in_alpha: bool = False
    used_in_gate: bool = False
    decay_half_life_days: int | None = Field(default=None, ge=1)
    expiry_time: date | None = None
    resolution_reason: str = ""
    reviewer: str = ""
    reviewed_at: date | None = None
    review_decision: RiskEventReviewDecision | str = ""
    rationale: str = ""
    next_review_due: date | None = None
    evidence_sources: list[RiskEventEvidenceSource] = Field(min_length=1)
    summary: str = Field(min_length=1)
    notes: str = ""


class RiskEventReviewAttestationSource(BaseModel):
    source_name: str = Field(min_length=1)
    source_type: RiskEventReviewAttestationSourceType
    source_url: str = ""
    captured_at: date
    notes: str = ""


class RiskEventReviewAttestation(BaseModel):
    attestation_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    review_date: date
    coverage_start: date
    coverage_end: date
    reviewer: str = Field(min_length=1)
    reviewed_at: date
    review_decision: RiskEventReviewAttestationDecision
    rationale: str = Field(min_length=1)
    next_review_due: date
    review_scope: list[str] = Field(min_length=1)
    checked_sources: list[RiskEventReviewAttestationSource] = Field(min_length=1)
    notes: str = ""


@dataclass(frozen=True)
class LoadedRiskEventOccurrence:
    occurrence: RiskEventOccurrence
    path: Path


@dataclass(frozen=True)
class LoadedRiskEventReviewAttestation:
    attestation: RiskEventReviewAttestation
    path: Path


@dataclass(frozen=True)
class RiskEventOccurrenceLoadError:
    path: Path
    message: str


@dataclass(frozen=True)
class RiskEventOccurrenceStore:
    input_path: Path
    loaded: tuple[LoadedRiskEventOccurrence, ...] = field(default_factory=tuple)
    review_attestations: tuple[LoadedRiskEventReviewAttestation, ...] = field(
        default_factory=tuple
    )
    load_errors: tuple[RiskEventOccurrenceLoadError, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RiskEventIssue:
    severity: RiskEventIssueSeverity
    code: str
    message: str
    event_id: str | None = None
    level: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class RiskEventsValidationReport:
    as_of: date
    config: RiskEventsConfig
    issues: tuple[RiskEventIssue, ...] = field(default_factory=tuple)

    @property
    def active_rule_count(self) -> int:
        return sum(1 for rule in self.config.event_rules if rule.active)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class RiskEventOccurrenceReviewItem:
    occurrence_id: str
    event_id: str
    level: str
    status: RiskEventOccurrenceStatus
    evidence_grade: RiskEventEvidenceGrade
    severity: RiskEventSeverity
    probability: RiskEventProbability
    scope: RiskEventScope
    time_sensitivity: RiskEventTimeSensitivity
    reversibility: RiskEventReversibility
    action_class: RiskEventActionClass
    reviewer: str
    reviewed_at: date | None
    review_decision: str
    next_review_due: date | None
    triggered_at: date
    last_confirmed_at: date
    source_types: tuple[RiskEventOccurrenceSourceType, ...]
    target_ai_exposure_multiplier: float
    score_eligible: bool
    position_gate_eligible: bool
    health: str
    reason: str
    lifecycle_state: str = "pending_review"
    dedup_group: str = ""
    primary_channel: str = ""
    used_in_alpha: bool = False
    used_in_gate: bool = False
    decay_half_life_days: int | None = None
    expiry_time: date | None = None
    resolution_reason: str = ""


@dataclass(frozen=True)
class RiskEventOccurrenceValidationReport:
    as_of: date
    input_path: Path
    config: RiskEventsConfig
    occurrences: tuple[LoadedRiskEventOccurrence, ...]
    review_attestations: tuple[LoadedRiskEventReviewAttestation, ...] = field(
        default_factory=tuple
    )
    issues: tuple[RiskEventIssue, ...] = field(default_factory=tuple)

    @property
    def occurrence_count(self) -> int:
        return len(self.occurrences)

    @property
    def active_occurrence_count(self) -> int:
        return sum(
            1
            for loaded in self.occurrences
            if loaded.occurrence.status in {"active", "watch"}
        )

    @property
    def review_attestation_count(self) -> int:
        return len(self.review_attestations)

    @property
    def current_review_attestations(
        self,
    ) -> tuple[LoadedRiskEventReviewAttestation, ...]:
        return tuple(
            loaded
            for loaded in self.review_attestations
            if _review_attestation_is_current(loaded.attestation, self.as_of)
        )

    @property
    def current_review_attestation_count(self) -> int:
        return len(self.current_review_attestations)

    @property
    def has_current_review_attestation(self) -> bool:
        return self.current_review_attestation_count > 0

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class RiskEventOccurrenceReviewReport:
    as_of: date
    validation_report: RiskEventOccurrenceValidationReport
    items: tuple[RiskEventOccurrenceReviewItem, ...]

    @property
    def active_items(self) -> tuple[RiskEventOccurrenceReviewItem, ...]:
        return tuple(item for item in self.items if item.status in {"active", "watch"})

    @property
    def score_eligible_active_items(self) -> tuple[RiskEventOccurrenceReviewItem, ...]:
        return tuple(item for item in self.active_items if item.score_eligible)

    @property
    def position_gate_eligible_active_items(
        self,
    ) -> tuple[RiskEventOccurrenceReviewItem, ...]:
        return tuple(item for item in self.active_items if item.position_gate_eligible)

    @property
    def status(self) -> str:
        if self.validation_report.error_count:
            return "FAIL"
        if self.validation_report.warning_count:
            return "PASS_WITH_WARNINGS"
        if any(item.level in {"L2", "L3"} for item in self.active_items):
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def has_current_review_attestation(self) -> bool:
        return self.validation_report.has_current_review_attestation


def load_risk_event_occurrence_store(input_path: Path | str) -> RiskEventOccurrenceStore:
    path = Path(input_path)
    loaded: list[LoadedRiskEventOccurrence] = []
    attestations: list[LoadedRiskEventReviewAttestation] = []
    load_errors: list[RiskEventOccurrenceLoadError] = []

    for yaml_path in _occurrence_yaml_paths(path):
        try:
            raw = _load_yaml(yaml_path)
        except OSError as exc:
            load_errors.append(RiskEventOccurrenceLoadError(path=yaml_path, message=str(exc)))
            continue
        except yaml.YAMLError as exc:
            load_errors.append(
                RiskEventOccurrenceLoadError(path=yaml_path, message=f"YAML 解析失败：{exc}")
            )
            continue

        for raw_item in _raw_occurrence_items(raw):
            try:
                occurrence = RiskEventOccurrence.model_validate(raw_item)
            except ValidationError as exc:
                load_errors.append(
                    RiskEventOccurrenceLoadError(
                        path=yaml_path,
                        message=_compact_validation_error(exc),
                    )
                )
                continue
            loaded.append(
                LoadedRiskEventOccurrence(occurrence=occurrence, path=yaml_path)
            )

        for raw_item in _raw_review_attestation_items(raw):
            try:
                attestation = RiskEventReviewAttestation.model_validate(raw_item)
            except ValidationError as exc:
                load_errors.append(
                    RiskEventOccurrenceLoadError(
                        path=yaml_path,
                        message=_compact_validation_error(exc),
                    )
                )
                continue
            attestations.append(
                LoadedRiskEventReviewAttestation(
                    attestation=attestation,
                    path=yaml_path,
                )
            )

    return RiskEventOccurrenceStore(
        input_path=path,
        loaded=tuple(loaded),
        review_attestations=tuple(attestations),
        load_errors=tuple(load_errors),
    )


def validate_risk_event_occurrence_store(
    store: RiskEventOccurrenceStore,
    risk_events: RiskEventsConfig,
    as_of: date,
    max_active_age_days: int = 14,
) -> RiskEventOccurrenceValidationReport:
    issues: list[RiskEventIssue] = []
    rules_by_id = {rule.event_id: rule for rule in risk_events.event_rules}

    for load_error in store.load_errors:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_occurrence_load_error",
                path=load_error.path,
                message=load_error.message,
            )
        )

    visible_occurrences: list[LoadedRiskEventOccurrence] = []
    for loaded in store.loaded:
        if _occurrence_has_future_input(loaded.occurrence, as_of):
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="risk_event_occurrence_excluded_future_as_of",
                    event_id=loaded.occurrence.event_id,
                    path=loaded.path,
                    message=(
                        "风险事件发生记录包含晚于评估日期的触发、确认、复核、解除或证据日期，"
                        "已从本次历史 as-of 评分和复核中排除。"
                    ),
                )
            )
            continue
        visible_occurrences.append(loaded)

    visible_attestations: list[LoadedRiskEventReviewAttestation] = []
    for loaded in store.review_attestations:
        if _review_attestation_has_future_input(loaded.attestation, as_of):
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="risk_event_review_attestation_excluded_future_as_of",
                    event_id=loaded.attestation.attestation_id,
                    path=loaded.path,
                    message=(
                        "风险事件复核声明包含晚于评估日期的复核、覆盖窗口或来源日期，"
                        "已从本次历史 as-of 复核声明中排除。"
                    ),
                )
            )
            continue
        visible_attestations.append(loaded)

    visible_occurrence_tuple = tuple(visible_occurrences)
    visible_attestation_tuple = tuple(visible_attestations)

    current_attestations = tuple(
        loaded
        for loaded in visible_attestation_tuple
        if _review_attestation_is_current(loaded.attestation, as_of)
    )

    if not store.input_path.exists():
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_occurrence_path_missing",
                path=store.input_path,
                message=(
                    "风险事件发生记录目录或文件不存在；政策/地缘模块不能证明当前没有风险。"
                ),
            )
        )
    elif not store.loaded and not store.review_attestations and not store.load_errors:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="no_risk_event_occurrences",
                path=store.input_path,
                message=(
                    "未发现风险事件发生记录 YAML；评分不会把监控规则配置当作实际风险或无风险证明。"
                ),
            )
        )
    elif (
        not any(
            loaded.occurrence.status in {"active", "watch"}
            for loaded in visible_occurrence_tuple
        )
        and not current_attestations
    ):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_current_review_attestation_missing",
                path=store.input_path,
                message=(
                    "未发现覆盖评估日且未过期的风险事件复核声明；空发生记录不能证明当前没有"
                    "政策或地缘风险。"
                ),
            )
        )

    _check_duplicate_occurrence_ids(visible_occurrence_tuple, issues)
    for loaded in visible_occurrence_tuple:
        _check_occurrence(
            loaded=loaded,
            rules_by_id=rules_by_id,
            as_of=as_of,
            max_active_age_days=max_active_age_days,
            issues=issues,
        )

    _check_duplicate_attestation_ids(visible_attestation_tuple, issues)
    for loaded in visible_attestation_tuple:
        _check_review_attestation(
            loaded=loaded,
            as_of=as_of,
            issues=issues,
        )

    return RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=store.input_path,
        config=risk_events,
        occurrences=visible_occurrence_tuple,
        review_attestations=visible_attestation_tuple,
        issues=tuple(issues),
    )


def build_risk_event_occurrence_review_report(
    validation_report: RiskEventOccurrenceValidationReport,
) -> RiskEventOccurrenceReviewReport:
    rules_by_id = {rule.event_id: rule for rule in validation_report.config.event_rules}
    levels_by_id: dict[str, RiskEventLevelConfig] = {
        str(level.level): level for level in validation_report.config.levels
    }

    return RiskEventOccurrenceReviewReport(
        as_of=validation_report.as_of,
        validation_report=validation_report,
        items=tuple(
            _occurrence_review_item(
                loaded=loaded,
                rules_by_id=rules_by_id,
                levels_by_id=levels_by_id,
                as_of=validation_report.as_of,
            )
            for loaded in validation_report.occurrences
        ),
    )


def validate_risk_events_config(
    risk_events: RiskEventsConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    universe: UniverseConfig,
    as_of: date,
) -> RiskEventsValidationReport:
    issues: list[RiskEventIssue] = []
    node_ids = {node.node_id for node in industry_chain.nodes}
    known_tickers = set(configured_price_tickers(universe, include_full_ai_chain=True))
    known_tickers.update(item.ticker for item in watchlist.items)

    _check_level_actions(risk_events, issues)
    for rule in risk_events.event_rules:
        _check_rule_references(rule, node_ids, known_tickers, issues)
        _check_rule_action_design(rule, issues)

    return RiskEventsValidationReport(
        as_of=as_of,
        config=risk_events,
        issues=tuple(issues),
    )


def render_risk_events_validation_report(report: RiskEventsValidationReport) -> str:
    lines = [
        "# 风险事件分级校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 风险等级数：{len(report.config.levels)}",
        f"- 风险事件规则数：{len(report.config.event_rules)}",
        f"- 活跃规则数：{report.active_rule_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 风险等级",
        "",
        "| 等级 | 名称 | AI 仓位乘数 | 人工复核 | 默认动作 |",
        "|---|---|---:|---|---|",
    ]

    for level in sorted(report.config.levels, key=lambda item: item.level):
        lines.append(
            "| "
            f"{level.level} | "
            f"{_escape_markdown_table(level.name)} | "
            f"{level.target_ai_exposure_multiplier:.0%} | "
            f"{'需要' if level.requires_manual_review else '不需要'} | "
            f"{_escape_markdown_table(level.default_action)} |"
        )

    lines.extend(
        [
            "",
            "## 事件规则",
            "",
            "| 事件 | 名称 | 等级 | 活跃 | 影响节点 | 相关标的 |",
            "|---|---|---|---|---|---|",
        ]
    )
    for rule in sorted(report.config.event_rules, key=lambda item: item.event_id):
        lines.append(
            "| "
            f"{rule.event_id} | "
            f"{_escape_markdown_table(rule.name)} | "
            f"{rule.level} | "
            f"{'是' if rule.active else '否'} | "
            f"{', '.join(rule.affected_nodes)} | "
            f"{', '.join(rule.related_tickers)} |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 等级 | 事件 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.level or ''} | "
                f"{issue.event_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 风险事件规则不直接触发交易，只改变风险评估、仓位折扣或人工复核状态。",
            "- L2/L3 事件必须进入人工复核；仓位动作仍需受组合上限和 thesis 约束。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_risk_events_validation_report(
    report: RiskEventsValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_risk_events_validation_report(report), encoding="utf-8")
    return output_path


def default_risk_events_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_events_validation_{as_of.isoformat()}.md"


def render_risk_event_occurrence_review_report(
    report: RiskEventOccurrenceReviewReport,
) -> str:
    validation = report.validation_report
    lines = [
        "# 风险事件发生记录校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 校验状态：{validation.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{validation.input_path}`",
        f"- 发生记录数：{validation.occurrence_count}",
        f"- 活跃/观察记录数：{validation.active_occurrence_count}",
        f"- 可进入普通评分的活跃记录数：{len(report.score_eligible_active_items)}",
        f"- 可触发仓位闸门的活跃记录数：{len(report.position_gate_eligible_active_items)}",
        f"- 风险事件复核声明数：{validation.review_attestation_count}",
        f"- 当前有效复核声明数：{validation.current_review_attestation_count}",
        f"- 错误数：{validation.error_count}",
        f"- 警告数：{validation.warning_count}",
        "",
        "## 发生记录",
        "",
    ]
    if not report.items:
        lines.append("未发现可读取的风险事件发生记录。")
    else:
        lines.extend(
            [
                "| Occurrence | 事件 | 等级 | 状态 | 证据等级 | 严重性 | 概率 | "
                "影响范围 | 动作等级 | 生命周期 | Dedup group | Alpha | Gate | "
                "触发日期 | 最近确认 | 过期 | 来源 | 评分 | 仓位闸门 | 复核人 | 下次复核 | 结论 |",
                "|" + "|".join(["---"] * 22) + "|",
            ]
        )
        for item in sorted(report.items, key=lambda value: value.occurrence_id):
            lines.append(
                "| "
                f"{item.occurrence_id} | "
                f"{item.event_id} | "
                f"{item.level} | "
                f"{_occurrence_status_label(item.status)} | "
                f"{item.evidence_grade} | "
                f"{_risk_severity_label(item.severity)} | "
                f"{_risk_probability_label(item.probability)} | "
                f"{_risk_scope_label(item.scope)} | "
                f"{_risk_action_class_label(item.action_class)} | "
                f"{item.lifecycle_state} | "
                f"{_escape_markdown_table(item.dedup_group)} | "
                f"{'是' if item.used_in_alpha else '否'} | "
                f"{'是' if item.used_in_gate else '否'} | "
                f"{item.triggered_at.isoformat()} | "
                f"{item.last_confirmed_at.isoformat()} | "
                f"{item.expiry_time.isoformat() if item.expiry_time else ''} | "
                f"{', '.join(_source_type_label(value) for value in item.source_types)} | "
                f"{'可用' if item.score_eligible else '不可用'} | "
                f"{'可用' if item.position_gate_eligible else '不可用'} | "
                f"{_escape_markdown_table(item.reviewer)} | "
                f"{item.next_review_due.isoformat() if item.next_review_due else ''} | "
                f"{_escape_markdown_table(item.reason)} |"
            )

    lines.extend(["", "## 复核声明", ""])
    if not validation.review_attestations:
        lines.append("未发现风险事件复核声明。")
    else:
        lines.extend(
            [
                "| Attestation | 复核日期 | 覆盖窗口 | 复核人 | 结论 | 下次复核 | "
                "来源范围 | 当前有效 | 说明 |",
                "|---|---|---|---|---|---|---|---|---|",
            ]
        )
        current_ids = {
            loaded.attestation.attestation_id
            for loaded in validation.current_review_attestations
        }
        for loaded in sorted(
            validation.review_attestations,
            key=lambda value: value.attestation.attestation_id,
        ):
            attestation = loaded.attestation
            source_names = ", ".join(
                source.source_name for source in attestation.checked_sources
            )
            lines.append(
                "| "
                f"{attestation.attestation_id} | "
                f"{attestation.review_date.isoformat()} | "
                f"{attestation.coverage_start.isoformat()} 至 "
                f"{attestation.coverage_end.isoformat()} | "
                f"{_escape_markdown_table(attestation.reviewer)} | "
                f"{_review_attestation_decision_label(attestation.review_decision)} | "
                f"{attestation.next_review_due.isoformat()} | "
                f"{_escape_markdown_table(source_names)} | "
                f"{'是' if attestation.attestation_id in current_ids else '否'} | "
                f"{_escape_markdown_table(attestation.rationale)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not validation.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 事件 | 文件 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in validation.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.event_id or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- `config/risk_events.yaml` 只定义需要监控的风险规则，不代表风险事件已经发生。",
            "- 政策/地缘评分只读取本报告中已通过校验的发生记录；没有发生记录时显示数据不足。",
            "- 保守 source policy：`S/A` 级 active 风险证据可进入普通评分并支持仓位闸门；"
            "`B` 级 active 风险证据只能进入普通评分；`C/D/X` 只能报告或人工复核。",
            "- 仅 `primary_source`、`paid_vendor` 或 `manual_input` 证据可进入评分；"
            "`public_convenience` 只能作为辅助线索。",
            "- `watch` 状态默认只进入报告和人工复核；只有 `active` 且证据来源、证据等级和 "
            "`action_class` 满足条件时，才进入自动评分和仓位闸门。",
            "- 复核声明只表示复核人在指定覆盖窗口内检查了列出的来源范围；它不是自动风险消除"
            "证明，也不会触发仓位闸门或覆盖已记录的 active/watch 风险事件。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_risk_event_occurrence_review_report(
    report: RiskEventOccurrenceReviewReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_risk_event_occurrence_review_report(report),
        encoding="utf-8",
    )
    return output_path


def default_risk_event_occurrence_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_event_occurrences_{as_of.isoformat()}.md"


def build_risk_event_review_attestation(
    *,
    as_of: date,
    reviewer: str,
    rationale: str,
    checked_source_names: tuple[str, ...],
    coverage_start: date | None = None,
    coverage_end: date | None = None,
    reviewed_at: date | None = None,
    next_review_due: date | None = None,
    review_scope: tuple[str, ...] = (
        "policy_event_occurrences",
        "geopolitical_event_occurrences",
        "risk_event_prereview_queue",
    ),
    source_type: RiskEventReviewAttestationSourceType = "manual_input",
    review_decision: RiskEventReviewAttestationDecision = (
        "confirmed_no_unrecorded_material_events"
    ),
    notes: str = "",
) -> RiskEventReviewAttestation:
    checked_at = reviewed_at or as_of
    return RiskEventReviewAttestation(
        attestation_id=f"risk_event_review_attestation_{as_of.isoformat()}",
        review_date=as_of,
        coverage_start=coverage_start or as_of,
        coverage_end=coverage_end or as_of,
        reviewer=reviewer,
        reviewed_at=checked_at,
        review_decision=review_decision,
        rationale=rationale,
        next_review_due=next_review_due or as_of,
        review_scope=list(review_scope),
        checked_sources=[
            RiskEventReviewAttestationSource(
                source_name=source_name,
                source_type=source_type,
                captured_at=checked_at,
            )
            for source_name in checked_source_names
        ],
        notes=notes,
    )


def write_risk_event_review_attestation(
    attestation: RiskEventReviewAttestation,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{attestation.attestation_id}.yaml"
    payload = {
        "review_attestation": attestation.model_dump(mode="json"),
    }
    with output_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(payload, file, allow_unicode=True, sort_keys=False)
    return output_path


def _occurrence_yaml_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _raw_occurrence_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "occurrences" in raw:
        occurrences = raw["occurrences"]
        if isinstance(occurrences, list):
            return occurrences
        return [occurrences]
    if isinstance(raw, dict) and (
        "review_attestation" in raw
        or "review_attestations" in raw
        or "no_material_events_attestation" in raw
        or "no_material_events_attestations" in raw
        or ("attestation_id" in raw and "event_id" not in raw)
    ):
        return []
    return [raw]


def _raw_review_attestation_items(raw: Any) -> list[Any]:
    if raw is None or not isinstance(raw, dict):
        return []
    for key in (
        "review_attestations",
        "review_attestation",
        "no_material_events_attestations",
        "no_material_events_attestation",
    ):
        if key not in raw:
            continue
        value = raw[key]
        if isinstance(value, list):
            return value
        return [value]
    if "attestation_id" in raw and "event_id" not in raw:
        return [raw]
    return []


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "risk event occurrence schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _check_duplicate_occurrence_ids(
    occurrences: tuple[LoadedRiskEventOccurrence, ...],
    issues: list[RiskEventIssue],
) -> None:
    paths_by_id: dict[str, list[Path]] = defaultdict(list)
    for loaded in occurrences:
        paths_by_id[loaded.occurrence.occurrence_id].append(loaded.path)

    for occurrence_id, paths in sorted(paths_by_id.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="duplicate_risk_event_occurrence_id",
                event_id=occurrence_id,
                path=paths[0],
                message="风险事件 occurrence_id 重复，后续评分无法可靠引用。",
            )
        )


def _check_duplicate_attestation_ids(
    attestations: tuple[LoadedRiskEventReviewAttestation, ...],
    issues: list[RiskEventIssue],
) -> None:
    paths_by_id: dict[str, list[Path]] = defaultdict(list)
    for loaded in attestations:
        paths_by_id[loaded.attestation.attestation_id].append(loaded.path)

    for attestation_id, paths in sorted(paths_by_id.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="duplicate_risk_event_review_attestation_id",
                event_id=attestation_id,
                path=paths[0],
                message="风险事件复核声明 attestation_id 重复，后续评分无法可靠引用。",
            )
        )


def _occurrence_has_future_input(occurrence: RiskEventOccurrence, as_of: date) -> bool:
    if occurrence.triggered_at > as_of or occurrence.last_confirmed_at > as_of:
        return True
    if occurrence.resolved_at is not None and occurrence.resolved_at > as_of:
        return True
    if occurrence.reviewed_at is not None and occurrence.reviewed_at > as_of:
        return True
    return any(
        source.captured_at > as_of
        or (source.published_at is not None and source.published_at > as_of)
        for source in occurrence.evidence_sources
    )


def _review_attestation_has_future_input(
    attestation: RiskEventReviewAttestation,
    as_of: date,
) -> bool:
    if attestation.review_date > as_of or attestation.reviewed_at > as_of:
        return True
    if attestation.coverage_start > as_of or attestation.coverage_end > as_of:
        return True
    return any(source.captured_at > as_of for source in attestation.checked_sources)


def _check_occurrence(
    loaded: LoadedRiskEventOccurrence,
    rules_by_id: dict[str, RiskEventRuleConfig],
    as_of: date,
    max_active_age_days: int,
    issues: list[RiskEventIssue],
) -> None:
    occurrence = loaded.occurrence
    path = loaded.path
    rule = rules_by_id.get(occurrence.event_id)
    if rule is None:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="unknown_risk_event_id",
                event_id=occurrence.event_id,
                path=path,
                message="发生记录引用了未配置的风险事件规则。",
            )
        )
    elif not rule.active:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="inactive_risk_event_rule",
                event_id=occurrence.event_id,
                path=path,
                message="发生记录引用的风险事件规则已关闭监控，需要确认是否仍应保留记录。",
            )
        )

    if occurrence.triggered_at > as_of or occurrence.last_confirmed_at > as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_occurrence_date_in_future",
                event_id=occurrence.event_id,
                path=path,
                message="触发日期或最近确认日期晚于评估日期。",
            )
        )
    if occurrence.resolved_at is not None and occurrence.resolved_at > as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_resolved_date_in_future",
                event_id=occurrence.event_id,
                path=path,
                message="解除日期晚于评估日期。",
            )
        )
    if (
        occurrence.expiry_time is not None
        and occurrence.expiry_time < as_of
        and occurrence.status in {"active", "watch"}
    ):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_occurrence_expired",
                event_id=occurrence.event_id,
                path=path,
                message=(
                    "风险事件已超过 expiry_time；默认不应继续作为 active/watch "
                    "风险压制评分或仓位，需要人工更新状态或重新确认。"
                ),
            )
        )
    if occurrence.last_confirmed_at < occurrence.triggered_at:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="last_confirmed_before_triggered",
                event_id=occurrence.event_id,
                path=path,
                message="最近确认日期不能早于触发日期。",
            )
        )
    if occurrence.resolved_at is not None and occurrence.resolved_at < occurrence.triggered_at:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="resolved_before_triggered",
                event_id=occurrence.event_id,
                path=path,
                message="解除日期不能早于触发日期。",
            )
        )
    if occurrence.status in {"active", "watch"} and occurrence.resolved_at is not None:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="active_occurrence_has_resolved_at",
                event_id=occurrence.event_id,
                path=path,
                message="active/watch 状态不能同时填写 resolved_at。",
            )
        )
    if occurrence.status in {"resolved", "dismissed"} and occurrence.resolved_at is None:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="resolved_occurrence_missing_resolved_at",
                event_id=occurrence.event_id,
                path=path,
                message="resolved/dismissed 状态建议填写 resolved_at，便于复盘时间线。",
            )
        )
    if (
        occurrence.status in {"active", "watch"}
        and (as_of - occurrence.last_confirmed_at).days > max_active_age_days
    ):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="active_risk_event_occurrence_stale",
                event_id=occurrence.event_id,
                path=path,
                message="活跃/观察风险事件超过新鲜度阈值未确认，需要更新证据。",
                )
            )

    missing_review_fields = _missing_occurrence_review_fields(occurrence)
    if missing_review_fields:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_occurrence_missing_review_metadata",
                event_id=occurrence.event_id,
                path=path,
                message=(
                    "活跃/观察风险事件发生记录必须包含人工复核元数据："
                    f"{', '.join(missing_review_fields)}。"
                ),
            )
        )
    if occurrence.reviewed_at is not None and occurrence.reviewed_at > as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_review_date_in_future",
                event_id=occurrence.event_id,
                path=path,
                message="人工复核日期晚于评估日期，不能作为 point-in-time 输入。",
            )
        )

    if occurrence.status == "watch" and occurrence.action_class in {
        "score_eligible",
        "position_gate_eligible",
    }:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="watch_risk_event_not_auto_scored",
                event_id=occurrence.event_id,
                path=path,
                message=(
                    "watch 状态不会直接进入自动评分；如已确认风险，请先将状态升级为 active。"
                ),
            )
        )
    if evidence_grade_is_report_only(occurrence.evidence_grade) and occurrence.action_class in {
        "score_eligible",
        "position_gate_eligible",
    }:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="low_grade_risk_event_not_auto_scored",
                event_id=occurrence.event_id,
                path=path,
                message=(
                    "C/D/X 级证据只能进入人工复核；即使 action_class 较高，也不会自动评分。"
                ),
            )
        )
    if occurrence.evidence_grade == "B" and occurrence.action_class == "position_gate_eligible":
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="b_grade_risk_event_not_position_gate_eligible",
                event_id=occurrence.event_id,
                path=path,
                message=(
                    "B 级风险证据可在 active 后进入普通评分，但不能单独触发仓位闸门。"
                ),
            )
        )

    for source in occurrence.evidence_sources:
        if (
            source.source_type in {"primary_source", "paid_vendor", "public_convenience"}
            and not source.source_url
        ):
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="missing_risk_event_evidence_url",
                    event_id=occurrence.event_id,
                    path=path,
                    message="非手工证据建议提供 source_url，便于复核来源和发布时间。",
                )
            )
        if source.captured_at > as_of or (
            source.published_at is not None and source.published_at > as_of
        ):
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="risk_event_evidence_date_in_future",
                    event_id=occurrence.event_id,
                    path=path,
                    message="证据发布日期或采集日期晚于评估日期。",
                )
            )
        if source.source_type == "public_convenience":
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="public_convenience_risk_event_source",
                    event_id=occurrence.event_id,
                    path=path,
                    message="公开便利来源只能作为辅助，不能单独进入政策/地缘评分。",
                )
            )


def _risk_event_lifecycle_state(occurrence: RiskEventOccurrence) -> str:
    if occurrence.lifecycle_state is not None:
        return occurrence.lifecycle_state
    if occurrence.expiry_time is not None and occurrence.status in {"active", "watch"}:
        return "confirmed_medium"
    if occurrence.status == "active":
        if occurrence.action_class == "position_gate_eligible":
            return "confirmed_high"
        if occurrence.action_class == "score_eligible":
            return "confirmed_medium"
        return "confirmed_low"
    if occurrence.status == "watch":
        return "pending_review"
    if occurrence.status == "resolved":
        return "resolved"
    return "rejected"


def _occurrence_review_item(
    loaded: LoadedRiskEventOccurrence,
    rules_by_id: dict[str, RiskEventRuleConfig],
    levels_by_id: Mapping[str, RiskEventLevelConfig],
    as_of: date,
) -> RiskEventOccurrenceReviewItem:
    occurrence = loaded.occurrence
    rule = rules_by_id.get(occurrence.event_id)
    level_id = rule.level if rule is not None else "UNKNOWN"
    level_config = levels_by_id.get(level_id)
    multiplier = (
        float(level_config.target_ai_exposure_multiplier)
        if level_config is not None
        else 1.0
    )
    source_types = tuple(
        sorted({source.source_type for source in occurrence.evidence_sources})
    )
    has_score_source = any(
        source_type_allows_automatic_scoring(source_type) for source_type in source_types
    )
    action_allows_scoring = occurrence.action_class in {
        "score_eligible",
        "position_gate_eligible",
    }
    evidence_allows_scoring = evidence_grade_allows_automatic_scoring(
        occurrence.evidence_grade
    )
    action_allows_position_gate = occurrence.action_class == "position_gate_eligible"
    evidence_allows_position_gate = evidence_grade_allows_position_gate(
        occurrence.evidence_grade
    )
    expired = occurrence.expiry_time is not None and occurrence.expiry_time < as_of
    score_eligible = (
        rule is not None
        and occurrence.status == "active"
        and not expired
        and has_score_source
        and action_allows_scoring
        and evidence_allows_scoring
    )
    position_gate_eligible = (
        score_eligible
        and action_allows_position_gate
        and evidence_allows_position_gate
    )

    if expired:
        health = "EXPIRED"
        reason = (
            "风险事件已超过 expiry_time；默认只保留审计和复核提示，不进入自动评分或仓位闸门。"
        )
    elif rule is None:
        health = "UNKNOWN_RULE"
        reason = "发生记录引用了未知风险规则，不能进入评分。"
    elif not has_score_source:
        health = "INELIGIBLE_SOURCE"
        reason = "只有 public_convenience 证据，不能进入自动评分。"
    elif not evidence_allows_scoring:
        health = "LOW_EVIDENCE_GRADE"
        reason = "证据等级为 C/D/X，只进入人工复核，不进入自动评分。"
    elif occurrence.action_class in {"monitor_only", "manual_review"}:
        health = "MANUAL_REVIEW_ONLY"
        reason = "action_class 要求只监控或人工复核，不进入自动评分。"
    elif occurrence.status == "watch":
        health = "WATCH"
        reason = f"{rule.level} 风险处于观察状态，只进入报告和人工复核。"
    elif score_eligible and not position_gate_eligible and occurrence.evidence_grade == "B":
        health = f"ACTIVE_{rule.level}_SCORE_ONLY"
        reason = (
            f"{rule.level} 风险已触发且 B 级证据可进入普通评分；"
            "保守 source policy 下不能单独触发仓位闸门。"
        )
    elif occurrence.status == "active":
        health = f"ACTIVE_{rule.level}"
        reason = f"{rule.level} 风险已触发，AI 仓位乘数参考 {multiplier:.0%}。"
    elif occurrence.status == "resolved":
        health = "RESOLVED"
        reason = "风险事件已解除，只保留复盘记录。"
    else:
        health = "DISMISSED"
        reason = "风险事件已排除，只保留审计记录。"

    return RiskEventOccurrenceReviewItem(
        occurrence_id=occurrence.occurrence_id,
        event_id=occurrence.event_id,
        level=level_id,
        status=occurrence.status,
        evidence_grade=occurrence.evidence_grade,
        severity=occurrence.severity,
        probability=occurrence.probability,
        scope=occurrence.scope,
        time_sensitivity=occurrence.time_sensitivity,
        reversibility=occurrence.reversibility,
        action_class=occurrence.action_class,
        lifecycle_state="expired" if expired else _risk_event_lifecycle_state(occurrence),
        dedup_group=occurrence.dedup_group,
        primary_channel=occurrence.primary_channel,
        used_in_alpha=occurrence.used_in_alpha,
        used_in_gate=occurrence.used_in_gate,
        decay_half_life_days=occurrence.decay_half_life_days,
        expiry_time=occurrence.expiry_time,
        resolution_reason=occurrence.resolution_reason,
        reviewer=occurrence.reviewer,
        reviewed_at=occurrence.reviewed_at,
        review_decision=str(occurrence.review_decision),
        next_review_due=occurrence.next_review_due,
        triggered_at=occurrence.triggered_at,
        last_confirmed_at=occurrence.last_confirmed_at,
        source_types=source_types,
        target_ai_exposure_multiplier=multiplier,
        score_eligible=score_eligible,
        position_gate_eligible=position_gate_eligible,
        health=health,
        reason=reason,
            )


def _check_review_attestation(
    loaded: LoadedRiskEventReviewAttestation,
    as_of: date,
    issues: list[RiskEventIssue],
) -> None:
    attestation = loaded.attestation
    path = loaded.path

    if attestation.coverage_start > attestation.coverage_end:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_review_attestation_invalid_window",
                event_id=attestation.attestation_id,
                path=path,
                message="复核声明 coverage_start 不能晚于 coverage_end。",
            )
        )
    if attestation.review_date > as_of or attestation.reviewed_at > as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_review_attestation_date_in_future",
                event_id=attestation.attestation_id,
                path=path,
                message=(
                    "复核声明 review_date 或 reviewed_at 晚于评估日期，"
                    "不能作为 point-in-time 输入。"
                ),
            )
        )
    if attestation.coverage_end > as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="risk_event_review_attestation_future_coverage",
                event_id=attestation.attestation_id,
                path=path,
                message="复核声明不能覆盖评估日之后的未来窗口。",
            )
        )
    if attestation.next_review_due < as_of:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_review_attestation_stale",
                event_id=attestation.attestation_id,
                path=path,
                message="风险事件复核声明已超过 next_review_due，需要重新复核。",
            )
        )
    if attestation.review_decision != "confirmed_no_unrecorded_material_events":
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_review_attestation_not_confirmed",
                event_id=attestation.attestation_id,
                path=path,
                message="复核声明未确认没有未记录重大风险事件，不能解除政策/地缘模块数据不足。",
            )
        )
    if not _review_attestation_has_authoritative_source(attestation):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="risk_event_review_attestation_no_authoritative_source",
                event_id=attestation.attestation_id,
                path=path,
                message=(
                    "复核声明缺少 primary_source、paid_vendor 或 manual_input 来源范围，"
                    "不能作为完整复核输入。"
                ),
            )
        )

    for source in attestation.checked_sources:
        if source.captured_at > as_of:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="risk_event_review_attestation_source_in_future",
                    event_id=attestation.attestation_id,
                    path=path,
                    message="复核声明 checked_sources 中存在晚于评估日期的 captured_at。",
                )
            )
        if (
            source.source_type in {"primary_source", "paid_vendor", "public_convenience"}
            and not source.source_url
        ):
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="risk_event_review_attestation_missing_source_url",
                    event_id=attestation.attestation_id,
                    path=path,
                    message="非人工来源建议提供 source_url，便于复核来源范围。",
                )
            )


def _review_attestation_is_current(
    attestation: RiskEventReviewAttestation,
    as_of: date,
) -> bool:
    return (
        attestation.review_decision == "confirmed_no_unrecorded_material_events"
        and attestation.coverage_start <= as_of
        and as_of <= attestation.coverage_end
        and attestation.coverage_end <= as_of
        and attestation.review_date <= as_of
        and attestation.reviewed_at <= as_of
        and attestation.next_review_due >= as_of
        and _review_attestation_has_authoritative_source(attestation)
    )


def _review_attestation_has_authoritative_source(
    attestation: RiskEventReviewAttestation,
) -> bool:
    return any(
        source.source_type in {"primary_source", "paid_vendor", "manual_input"}
        for source in attestation.checked_sources
    )


def _missing_occurrence_review_fields(occurrence: RiskEventOccurrence) -> list[str]:
    if occurrence.status not in {"active", "watch"}:
        return []
    missing: list[str] = []
    if not occurrence.reviewer.strip():
        missing.append("reviewer")
    if occurrence.reviewed_at is None:
        missing.append("reviewed_at")
    if not str(occurrence.review_decision).strip():
        missing.append("review_decision")
    if not occurrence.rationale.strip():
        missing.append("rationale")
    if occurrence.next_review_due is None:
        missing.append("next_review_due")
    return missing


def _check_level_actions(
    risk_events: RiskEventsConfig,
    issues: list[RiskEventIssue],
) -> None:
    levels = {level.level: level for level in risk_events.levels}
    if (
        levels["L1"].target_ai_exposure_multiplier
        < levels["L2"].target_ai_exposure_multiplier
        or levels["L2"].target_ai_exposure_multiplier
        < levels["L3"].target_ai_exposure_multiplier
    ):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="non_monotonic_exposure_multiplier",
                message="风险等级越高，AI 仓位乘数不能更高。",
            )
        )

    for level_id in ("L2", "L3"):
        if not levels[level_id].requires_manual_review:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="high_level_without_manual_review",
                    level=level_id,
                    message="L2/L3 风险事件必须要求人工复核。",
                )
            )


def _check_rule_references(
    rule: RiskEventRuleConfig,
    node_ids: set[str],
    known_tickers: set[str],
    issues: list[RiskEventIssue],
) -> None:
    for node_id in rule.affected_nodes:
        if node_id not in node_ids:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="unknown_affected_node",
                    event_id=rule.event_id,
                    message=f"风险事件引用了不存在的产业链节点：{node_id}",
                )
            )

    for ticker in rule.related_tickers:
        if ticker not in known_tickers:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="unknown_related_ticker",
                    event_id=rule.event_id,
                    message=f"风险事件引用了未配置的数据或观察池标的：{ticker}",
                )
            )


def _check_rule_action_design(
    rule: RiskEventRuleConfig,
    issues: list[RiskEventIssue],
) -> None:
    if not rule.active:
        return

    if rule.level in {"L2", "L3"} and not rule.escalation_conditions:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="missing_escalation_conditions",
                event_id=rule.event_id,
                message="活跃 L2/L3 风险事件建议配置升级条件，避免临时主观加码。",
            )
        )

    if rule.level in {"L2", "L3"} and not rule.deescalation_conditions:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="missing_deescalation_conditions",
                event_id=rule.event_id,
                message="活跃 L2/L3 风险事件建议配置解除条件，避免风险消失后无法复位。",
            )
        )


def _severity_label(severity: RiskEventIssueSeverity) -> str:
    if severity == RiskEventIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _occurrence_status_label(value: str) -> str:
    return {
        "active": "活跃",
        "watch": "观察",
        "resolved": "已解除",
        "dismissed": "已排除",
    }.get(value, value)


def _source_type_label(value: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "manual_input": "人工审计",
        "public_convenience": "公开便利源",
        "llm_extracted": "LLM 抽取/评估",
    }.get(value, value)


def _risk_severity_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
        "unknown": "未知",
    }.get(value, value)


def _risk_probability_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "confirmed": "已确认",
        "unknown": "未知",
    }.get(value, value)


def _risk_scope_label(value: str) -> str:
    return {
        "single_ticker": "单一标的",
        "industry_chain_node": "产业链节点",
        "ai_bucket": "AI 组合",
        "market_wide": "全市场",
        "unknown": "未知",
    }.get(value, value)


def _risk_action_class_label(value: str) -> str:
    return {
        "monitor_only": "仅监控",
        "manual_review": "人工复核",
        "score_eligible": "可评分",
        "position_gate_eligible": "可触发仓位闸门",
    }.get(value, value)


def _review_attestation_decision_label(value: str) -> str:
    return {
        "confirmed_no_unrecorded_material_events": "确认无未记录重大事件",
        "needs_more_evidence": "需要更多证据",
    }.get(value, value)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
