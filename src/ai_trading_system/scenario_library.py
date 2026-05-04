from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import (
    DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    IndustryChainConfig,
    RiskEventsConfig,
    WatchlistConfig,
)

ScenarioStatus = Literal["active", "candidate", "retired"]
ScenarioType = Literal["hypothetical_shock", "historical_stress", "active_risk_event_linked"]
ShockDirection = Literal["negative", "positive", "mixed"]
ScenarioSeverity = Literal["low", "medium", "high", "critical"]
GateImpactEffect = Literal[
    "cap_lower",
    "no_new_position",
    "manual_review",
    "confidence_downgrade",
]

ALLOWED_POSITION_GATE_IDS = frozenset(
    {
        "score_model",
        "portfolio_limits",
        "risk_events",
        "valuation",
        "thesis",
        "data_confidence",
    }
)

SCENARIO_TYPE_LABELS = {
    "hypothetical_shock": "假设冲击",
    "historical_stress": "历史压力",
    "active_risk_event_linked": "风险事件映射",
}


class ScenarioLibraryIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class ScenarioPositionGateImpact(BaseModel):
    gate_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_effect: GateImpactEffect
    max_ai_exposure_hint: float | None = Field(default=None, ge=0, le=1)
    rationale: str = Field(min_length=1)


class ScenarioDefinition(BaseModel):
    scenario_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    name: str = Field(min_length=1)
    status: ScenarioStatus
    scenario_type: ScenarioType
    shock_direction: ShockDirection
    severity: ScenarioSeverity
    affected_nodes: list[str] = Field(min_length=1)
    affected_tickers: list[str] = Field(min_length=1)
    linked_risk_event_ids: list[str] = Field(default_factory=list)
    linked_thesis_ids: list[str] = Field(default_factory=list)
    position_gate_impacts: list[ScenarioPositionGateImpact] = Field(min_length=1)
    observation_conditions: list[str] = Field(min_length=1)
    review_requirements: list[str] = Field(min_length=1)
    evidence_requirements: list[str] = Field(min_length=1)
    interpretation_boundary: str = Field(min_length=1)
    not_probability_forecast: bool = True

    @model_validator(mode="after")
    def validate_linked_risk_event_type(self) -> Self:
        if self.scenario_type == "active_risk_event_linked" and not self.linked_risk_event_ids:
            raise ValueError("active_risk_event_linked scenario requires linked_risk_event_ids")
        return self


class ScenarioLibrary(BaseModel):
    library_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1, pattern=r"^v[0-9]+([.][0-9]+)*$")
    status: Literal["production", "candidate", "retired"]
    owner: str = Field(min_length=1)
    description: str = Field(min_length=1)
    last_reviewed_at: date
    next_review_due: date
    scenarios: list[ScenarioDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_review_dates(self) -> Self:
        if self.next_review_due < self.last_reviewed_at:
            raise ValueError("next_review_due must not be before last_reviewed_at")
        return self


@dataclass(frozen=True)
class ScenarioLibraryIssue:
    severity: ScenarioLibraryIssueSeverity
    code: str
    message: str
    scenario_id: str | None = None


@dataclass(frozen=True)
class ScenarioLibraryStore:
    input_path: Path
    library: ScenarioLibrary | None
    load_errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ScenarioLibraryValidationReport:
    as_of: date
    store: ScenarioLibraryStore
    issues: tuple[ScenarioLibraryIssue, ...]

    @property
    def scenario_count(self) -> int:
        if self.store.library is None:
            return 0
        return len(self.store.library.scenarios)

    @property
    def active_count(self) -> int:
        if self.store.library is None:
            return 0
        return sum(1 for scenario in self.store.library.scenarios if scenario.status == "active")

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == ScenarioLibraryIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == ScenarioLibraryIssueSeverity.WARNING
        )

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


def load_scenario_library(
    input_path: Path | str = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
) -> ScenarioLibraryStore:
    path = Path(input_path)
    if not path.exists():
        return ScenarioLibraryStore(
            input_path=path,
            library=None,
            load_errors=(f"文件不存在：{path}",),
        )
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
    except (OSError, yaml.YAMLError) as exc:
        return ScenarioLibraryStore(input_path=path, library=None, load_errors=(str(exc),))

    try:
        library = ScenarioLibrary.model_validate(raw)
    except ValidationError as exc:
        return ScenarioLibraryStore(
            input_path=path,
            library=None,
            load_errors=(_compact_validation_error(exc),),
        )
    return ScenarioLibraryStore(input_path=path, library=library)


def validate_scenario_library(
    store: ScenarioLibraryStore,
    *,
    as_of: date,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    risk_events: RiskEventsConfig,
) -> ScenarioLibraryValidationReport:
    issues: list[ScenarioLibraryIssue] = []
    for error in store.load_errors:
        issues.append(
            ScenarioLibraryIssue(
                severity=ScenarioLibraryIssueSeverity.ERROR,
                code="scenario_library_load_error",
                message=error,
            )
        )
    library = store.library
    if library is not None:
        _check_duplicate_scenario_ids(library, issues)
        _check_review_due(library, as_of, issues)
        known_nodes = {node.node_id for node in industry_chain.nodes}
        known_tickers = _known_tickers(industry_chain, watchlist, risk_events)
        known_risk_events = {event.event_id for event in risk_events.event_rules}
        for scenario in library.scenarios:
            _check_scenario(
                scenario,
                known_nodes=known_nodes,
                known_tickers=known_tickers,
                known_risk_events=known_risk_events,
                issues=issues,
            )
    return ScenarioLibraryValidationReport(
        as_of=as_of,
        store=store,
        issues=tuple(issues),
    )


def default_scenario_library_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"scenario_library_{as_of.isoformat()}.md"


def write_scenario_library_report(
    report: ScenarioLibraryValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_scenario_library_report(report), encoding="utf-8")
    return output_path


def lookup_scenario(input_path: Path | str, scenario_id: str) -> ScenarioDefinition:
    store = load_scenario_library(input_path)
    if store.load_errors:
        raise ValueError("; ".join(store.load_errors))
    if store.library is None:
        raise KeyError(f"scenario library not found: {input_path}")
    for scenario in store.library.scenarios:
        if scenario.scenario_id == scenario_id:
            return scenario
    raise KeyError(f"scenario not found: {scenario_id}")


def render_scenario_lookup(scenario: ScenarioDefinition) -> str:
    lines = [
        f"Scenario：{scenario.scenario_id}",
        f"名称：{scenario.name}",
        f"状态：{scenario.status}",
        f"类型：{_scenario_type_label(scenario.scenario_type)}",
        f"方向：{scenario.shock_direction}",
        f"严重度：{scenario.severity}",
        f"影响节点：{', '.join(scenario.affected_nodes)}",
        f"影响 ticker：{', '.join(scenario.affected_tickers)}",
        f"关联风险事件：{', '.join(scenario.linked_risk_event_ids) or '无'}",
        f"仓位 gate 影响：{_gate_summary(scenario)}",
        f"观察条件：{'; '.join(scenario.observation_conditions)}",
        f"人工复核要求：{'; '.join(scenario.review_requirements)}",
        f"解释边界：{scenario.interpretation_boundary}",
    ]
    return "\n".join(lines) + "\n"


def render_scenario_library_report(report: ScenarioLibraryValidationReport) -> str:
    library = report.store.library
    lines = [
        "# AI 产业链情景压力测试库校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.store.input_path}`",
        f"- 情景数量：{report.scenario_count}",
        f"- Active 情景：{report.active_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "- 治理边界：情景用于压力测试、脆弱点识别和人工复核提示；"
        "不得被解释为概率预测，也不得直接修改 production 评分、仓位闸门或回测仓位。",
    ]
    if library is not None:
        type_counts = Counter(scenario.scenario_type for scenario in library.scenarios)
        severity_counts = Counter(scenario.severity for scenario in library.scenarios)
        lines.extend(
            [
                f"- Library：`{library.library_id}` `{library.version}`",
                f"- 下次复核：{library.next_review_due.isoformat()}",
                "",
                "## 类型摘要",
                "",
                "| 类型 | 数量 |",
                "|---|---:|",
            ]
        )
        for scenario_type, count in sorted(type_counts.items()):
            lines.append(f"| {_scenario_type_label(scenario_type)} | {count} |")
        lines.extend(["", "## 严重度摘要", "", "| 严重度 | 数量 |", "|---|---:|"])
        for severity, count in sorted(severity_counts.items()):
            lines.append(f"| {severity} | {count} |")

        lines.extend(
            [
                "",
                "## 情景映射",
                "",
                "| Scenario | 类型 | 严重度 | 节点 | Ticker | Risk event | Gate impact |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for scenario in sorted(library.scenarios, key=lambda item: item.scenario_id):
            lines.append(
                "| "
                f"`{scenario.scenario_id}` | "
                f"{_scenario_type_label(scenario.scenario_type)} | "
                f"{scenario.severity} | "
                f"{_escape_markdown_table(', '.join(scenario.affected_nodes))} | "
                f"{_escape_markdown_table(', '.join(scenario.affected_tickers))} | "
                f"{_escape_markdown_table(', '.join(scenario.linked_risk_event_ids) or '无')} | "
                f"{_escape_markdown_table(_gate_summary(scenario))} |"
            )

        lines.extend(
            [
                "",
                "## 复核要求",
                "",
                "| Scenario | 主要观察条件 | 人工复核要求 | 解释边界 |",
                "|---|---|---|---|",
            ]
        )
        for scenario in sorted(library.scenarios, key=lambda item: item.scenario_id):
            lines.append(
                "| "
                f"`{scenario.scenario_id}` | "
                f"{_escape_markdown_table('; '.join(scenario.observation_conditions))} | "
                f"{_escape_markdown_table('; '.join(scenario.review_requirements))} | "
                f"{_escape_markdown_table(scenario.interpretation_boundary)} |"
            )

    lines.extend(_issue_section(report.issues))
    return "\n".join(lines).rstrip() + "\n"


def _check_duplicate_scenario_ids(
    library: ScenarioLibrary,
    issues: list[ScenarioLibraryIssue],
) -> None:
    counts = Counter(scenario.scenario_id for scenario in library.scenarios)
    for scenario_id, count in counts.items():
        if count > 1:
            issues.append(
                ScenarioLibraryIssue(
                    severity=ScenarioLibraryIssueSeverity.ERROR,
                    code="duplicate_scenario_id",
                    scenario_id=scenario_id,
                    message="scenario_id 必须唯一。",
                )
            )


def _check_review_due(
    library: ScenarioLibrary,
    as_of: date,
    issues: list[ScenarioLibraryIssue],
) -> None:
    if library.next_review_due < as_of:
        issues.append(
            ScenarioLibraryIssue(
                severity=ScenarioLibraryIssueSeverity.WARNING,
                code="scenario_library_review_overdue",
                message=(
                    "scenario library 已超过 next_review_due："
                    f"{library.next_review_due.isoformat()}"
                ),
            )
        )


def _check_scenario(
    scenario: ScenarioDefinition,
    *,
    known_nodes: set[str],
    known_tickers: set[str],
    known_risk_events: set[str],
    issues: list[ScenarioLibraryIssue],
) -> None:
    if not scenario.not_probability_forecast:
        issues.append(
            ScenarioLibraryIssue(
                severity=ScenarioLibraryIssueSeverity.ERROR,
                code="scenario_probability_forecast_not_allowed",
                scenario_id=scenario.scenario_id,
                message="情景必须声明 not_probability_forecast=true。",
            )
        )
    for node_id in scenario.affected_nodes:
        if node_id not in known_nodes:
            issues.append(
                ScenarioLibraryIssue(
                    severity=ScenarioLibraryIssueSeverity.ERROR,
                    code="unknown_affected_node",
                    scenario_id=scenario.scenario_id,
                    message=f"affected_nodes 引用了未知产业链节点：{node_id}",
                )
            )
    for ticker in scenario.affected_tickers:
        if ticker not in known_tickers:
            issues.append(
                ScenarioLibraryIssue(
                    severity=ScenarioLibraryIssueSeverity.ERROR,
                    code="unknown_affected_ticker",
                    scenario_id=scenario.scenario_id,
                    message=f"affected_tickers 引用了未知 ticker：{ticker}",
                )
            )
    for event_id in scenario.linked_risk_event_ids:
        if event_id not in known_risk_events:
            issues.append(
                ScenarioLibraryIssue(
                    severity=ScenarioLibraryIssueSeverity.ERROR,
                    code="unknown_linked_risk_event",
                    scenario_id=scenario.scenario_id,
                    message=f"linked_risk_event_ids 引用了未知风险事件：{event_id}",
                )
            )
    for impact in scenario.position_gate_impacts:
        if impact.gate_id not in ALLOWED_POSITION_GATE_IDS:
            issues.append(
                ScenarioLibraryIssue(
                    severity=ScenarioLibraryIssueSeverity.ERROR,
                    code="unknown_position_gate",
                    scenario_id=scenario.scenario_id,
                    message=f"position_gate_impacts 引用了未知 gate：{impact.gate_id}",
                )
            )

def _known_tickers(
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    risk_events: RiskEventsConfig,
) -> set[str]:
    tickers = {item.ticker for item in watchlist.items}
    for node in industry_chain.nodes:
        tickers.update(node.related_tickers)
    for event in risk_events.event_rules:
        tickers.update(event.related_tickers)
    return tickers


def _gate_summary(scenario: ScenarioDefinition) -> str:
    return "; ".join(
        f"{impact.gate_id}:{impact.expected_effect}"
        for impact in scenario.position_gate_impacts
    )


def _issue_section(issues: tuple[ScenarioLibraryIssue, ...]) -> list[str]:
    lines = ["", "## 校验事项", ""]
    if not issues:
        lines.append("未发现错误或警告。")
        return lines
    lines.extend(["| Severity | Code | Scenario | Message |", "|---|---|---|---|"])
    for issue in issues:
        lines.append(
            "| "
            f"{issue.severity.value} | "
            f"`{issue.code}` | "
            f"{issue.scenario_id or ''} | "
            f"{_escape_markdown_table(issue.message)} |"
        )
    return lines


def _scenario_type_label(scenario_type: str) -> str:
    return SCENARIO_TYPE_LABELS.get(scenario_type, scenario_type)


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _compact_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error.get("loc", ())) or "<root>"
        parts.append(f"{location}: {error.get('msg', '')}")
    return "; ".join(parts)
