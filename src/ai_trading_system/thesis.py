from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import IndustryChainConfig, WatchlistConfig, WatchlistItem

ThesisDirection = Literal["long", "short", "hedge", "watch"]
ThesisTimeHorizon = Literal["short", "medium", "long"]
ThesisReviewFrequency = Literal["daily", "weekly", "monthly", "quarterly", "event_driven"]
ThesisStatus = Literal["draft", "active", "paused", "closed", "invalidated"]
ValidationMetricStatus = Literal["pending", "confirmed", "weakened", "falsified", "not_applicable"]
FalsificationSeverity = Literal["medium", "high", "critical"]
RiskEventLevel = Literal["L1", "L2", "L3"]


class ThesisIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class ValidationMetric(BaseModel):
    metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    description: str = Field(min_length=1)
    evidence_source: str = Field(min_length=1)
    expected_direction: str = Field(min_length=1)
    latest_status: ValidationMetricStatus = "pending"
    updated_at: date | None = None
    notes: str = ""


class FalsificationCondition(BaseModel):
    condition_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    description: str = Field(min_length=1)
    severity: FalsificationSeverity
    triggered: bool = False
    triggered_at: date | None = None
    evidence_source: str = ""
    notes: str = ""

    @model_validator(mode="after")
    def validate_trigger_date(self) -> Self:
        if self.triggered and self.triggered_at is None:
            raise ValueError("triggered_at is required when condition is triggered")
        return self


class ThesisRiskEvent(BaseModel):
    risk_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    level: RiskEventLevel
    description: str = Field(min_length=1)
    action: str = Field(min_length=1)
    active: bool = False
    updated_at: date | None = None
    notes: str = ""


class TradeThesis(BaseModel):
    thesis_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    ticker: str = Field(min_length=1)
    direction: ThesisDirection
    created_at: date
    time_horizon: ThesisTimeHorizon
    position_scope: str = Field(min_length=1)
    entry_reason: list[str] = Field(min_length=1)
    ai_chain_nodes: list[str] = Field(min_length=1)
    validation_metrics: list[ValidationMetric] = Field(min_length=1)
    falsification_conditions: list[FalsificationCondition] = Field(min_length=1)
    risk_events: list[ThesisRiskEvent] = Field(default_factory=list)
    review_frequency: ThesisReviewFrequency
    status: ThesisStatus
    notes: str = ""

    @model_validator(mode="after")
    def normalize_ticker(self) -> Self:
        self.ticker = self.ticker.upper()
        return self


@dataclass(frozen=True)
class LoadedTradeThesis:
    thesis: TradeThesis
    path: Path


@dataclass(frozen=True)
class TradeThesisLoadError:
    path: Path
    message: str


@dataclass(frozen=True)
class TradeThesisStore:
    input_path: Path
    loaded: tuple[LoadedTradeThesis, ...]
    load_errors: tuple[TradeThesisLoadError, ...]


@dataclass(frozen=True)
class ThesisIssue:
    severity: ThesisIssueSeverity
    code: str
    message: str
    thesis_id: str | None = None
    ticker: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class ThesisValidationReport:
    as_of: date
    input_path: Path
    theses: tuple[LoadedTradeThesis, ...]
    issues: tuple[ThesisIssue, ...] = field(default_factory=tuple)

    @property
    def thesis_count(self) -> int:
        return len(self.theses)

    @property
    def active_count(self) -> int:
        return sum(1 for loaded in self.theses if loaded.thesis.status == "active")

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ThesisIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ThesisIssueSeverity.WARNING)

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
class ThesisReviewItem:
    thesis_id: str
    ticker: str
    status: ThesisStatus
    health: str
    health_reason: str
    confirmed_metrics: int
    pending_metrics: int
    weakened_metrics: int
    falsified_metrics: int
    stale_metric_ids: tuple[str, ...]
    triggered_condition_ids: tuple[str, ...]
    active_risk_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class ThesisReviewReport:
    as_of: date
    validation_report: ThesisValidationReport
    items: tuple[ThesisReviewItem, ...]

    @property
    def status(self) -> str:
        if self.validation_report.error_count:
            return "FAIL"
        if self.validation_report.warning_count:
            return "PASS_WITH_WARNINGS"
        if any(item.health in {"WATCH", "INVALIDATED"} for item in self.items):
            return "PASS_WITH_WARNINGS"
        return "PASS"


def load_trade_thesis_store(input_path: Path | str) -> TradeThesisStore:
    path = Path(input_path)
    loaded: list[LoadedTradeThesis] = []
    load_errors: list[TradeThesisLoadError] = []

    for yaml_path in _thesis_yaml_paths(path):
        try:
            raw = _load_yaml(yaml_path)
        except OSError as exc:
            load_errors.append(TradeThesisLoadError(path=yaml_path, message=str(exc)))
            continue
        except yaml.YAMLError as exc:
            load_errors.append(
                TradeThesisLoadError(path=yaml_path, message=f"YAML 解析失败：{exc}")
            )
            continue

        for raw_item in _raw_thesis_items(raw):
            try:
                thesis = TradeThesis.model_validate(raw_item)
            except ValidationError as exc:
                load_errors.append(
                    TradeThesisLoadError(
                        path=yaml_path,
                        message=_compact_validation_error(exc),
                    )
                )
                continue
            loaded.append(LoadedTradeThesis(thesis=thesis, path=yaml_path))

    return TradeThesisStore(
        input_path=path,
        loaded=tuple(loaded),
        load_errors=tuple(load_errors),
    )


def validate_trade_thesis_store(
    store: TradeThesisStore,
    watchlist: WatchlistConfig,
    industry_chain: IndustryChainConfig,
    as_of: date,
) -> ThesisValidationReport:
    issues: list[ThesisIssue] = []
    active_watchlist = {item.ticker: item for item in watchlist.items if item.active}
    all_watchlist = {item.ticker: item for item in watchlist.items}
    industry_node_ids = {node.node_id for node in industry_chain.nodes}

    for load_error in store.load_errors:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="thesis_load_error",
                path=load_error.path,
                message=load_error.message,
            )
        )

    if not store.input_path.exists():
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.WARNING,
                code="thesis_path_missing",
                path=store.input_path,
                message="交易 thesis 目录或文件不存在；当前没有可复核的交易假设。",
            )
        )
    elif not store.loaded and not store.load_errors:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.WARNING,
                code="no_trade_thesis_files",
                path=store.input_path,
                message="未发现交易 thesis YAML 文件；主动交易不能被报告标记为高置信度。",
            )
        )

    _check_duplicate_thesis_ids(store.loaded, issues)

    active_thesis_by_ticker: dict[str, list[TradeThesis]] = defaultdict(list)
    for loaded in store.loaded:
        thesis = loaded.thesis
        if thesis.status == "active":
            active_thesis_by_ticker[thesis.ticker].append(thesis)

        _check_thesis_dates(thesis, loaded.path, as_of, issues)
        _check_thesis_references(
            thesis=thesis,
            path=loaded.path,
            active_watchlist=active_watchlist,
            all_watchlist=all_watchlist,
            industry_node_ids=industry_node_ids,
            issues=issues,
        )
        _check_internal_ids(thesis, loaded.path, issues)
        _check_active_thesis_consistency(thesis, loaded.path, issues)
        _check_review_freshness(thesis, loaded.path, as_of, issues)

    _check_watchlist_thesis_expectations(
        watchlist=watchlist,
        active_thesis_by_ticker=active_thesis_by_ticker,
        issues=issues,
    )

    return ThesisValidationReport(
        as_of=as_of,
        input_path=store.input_path,
        theses=store.loaded,
        issues=tuple(issues),
    )


def build_thesis_review_report(validation_report: ThesisValidationReport) -> ThesisReviewReport:
    items = tuple(
        _review_item(loaded.thesis, validation_report.as_of) for loaded in validation_report.theses
    )
    return ThesisReviewReport(
        as_of=validation_report.as_of,
        validation_report=validation_report,
        items=items,
    )


def render_thesis_validation_report(report: ThesisValidationReport) -> str:
    lines = [
        "# 交易 Thesis 校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.input_path}`",
        f"- Thesis 数量：{report.thesis_count}",
        f"- 活跃 Thesis 数量：{report.active_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## Thesis 概览",
        "",
    ]

    if not report.theses:
        lines.append("未发现可读取的交易 thesis。")
    else:
        lines.extend(
            [
                "| Thesis | Ticker | 方向 | 状态 | 创建日期 | 周期 | 复核频率 | "
                "验证指标 | 证伪条件 |",
                "|---|---|---|---|---|---|---|---:|---:|",
            ]
        )
        for loaded in sorted(report.theses, key=lambda item: item.thesis.thesis_id):
            thesis = loaded.thesis
            lines.append(
                "| "
                f"{thesis.thesis_id} | "
                f"{thesis.ticker} | "
                f"{_direction_label(thesis.direction)} | "
                f"{_status_label(thesis.status)} | "
                f"{thesis.created_at.isoformat()} | "
                f"{_horizon_label(thesis.time_horizon)} | "
                f"{_review_frequency_label(thesis.review_frequency)} | "
                f"{len(thesis.validation_metrics)} | "
                f"{len(thesis.falsification_conditions)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Thesis | Ticker | 文件 | 说明 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.thesis_id or ''} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def render_thesis_review_report(report: ThesisReviewReport) -> str:
    validation = report.validation_report
    lines = [
        "# 交易 Thesis 复核报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{validation.input_path}`",
        f"- Thesis 数量：{validation.thesis_count}",
        f"- 活跃 Thesis 数量：{validation.active_count}",
        f"- 校验错误数：{validation.error_count}",
        f"- 校验警告数：{validation.warning_count}",
        "",
        "## 复核结论",
        "",
    ]

    if not report.items:
        lines.append("未发现可复核的交易 thesis。")
    else:
        lines.extend(
            [
                "| Thesis | Ticker | 状态 | 复核结论 | 原因 | 已确认 | 待确认 | "
                "转弱 | 证伪 | 过期指标 | 触发条件 | 活跃风险 |",
                "|---|---|---|---|---|---:|---:|---:|---:|---|---|---|",
            ]
        )
        for item in sorted(report.items, key=lambda value: value.thesis_id):
            lines.append(
                "| "
                f"{item.thesis_id} | "
                f"{item.ticker} | "
                f"{_status_label(item.status)} | "
                f"{_health_label(item.health)} | "
                f"{_escape_markdown_table(item.health_reason)} | "
                f"{item.confirmed_metrics} | "
                f"{item.pending_metrics} | "
                f"{item.weakened_metrics} | "
                f"{item.falsified_metrics} | "
                f"{', '.join(item.stale_metric_ids)} | "
                f"{', '.join(item.triggered_condition_ids)} | "
                f"{', '.join(item.active_risk_event_ids)} |"
            )

    lines.extend(
        [
            "",
            "## 校验问题",
            "",
        ]
    )
    if not validation.issues:
        lines.append("未发现校验问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Thesis | Ticker | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in validation.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.thesis_id or ''} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告只复核结构化 thesis 的状态，不自动替代人工交易判断。",
            "- `WATCH` 表示存在待确认、转弱、过期指标或活跃风险事件，需要人工复核。",
            "- `证伪触发` 表示至少一个验证指标或证伪条件已经破坏原始假设。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_thesis_validation_report(report: ThesisValidationReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_thesis_validation_report(report), encoding="utf-8")
    return output_path


def write_thesis_review_report(report: ThesisReviewReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_thesis_review_report(report), encoding="utf-8")
    return output_path


def default_thesis_validation_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"thesis_validation_{as_of.isoformat()}.md"


def default_thesis_review_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"thesis_review_{as_of.isoformat()}.md"


def _thesis_yaml_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _raw_thesis_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "theses" in raw:
        theses = raw["theses"]
        if isinstance(theses, list):
            return theses
        return [theses]
    return [raw]


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "thesis schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _check_duplicate_thesis_ids(
    loaded_theses: tuple[LoadedTradeThesis, ...],
    issues: list[ThesisIssue],
) -> None:
    paths_by_id: dict[str, list[Path]] = defaultdict(list)
    for loaded in loaded_theses:
        paths_by_id[loaded.thesis.thesis_id].append(loaded.path)

    for thesis_id, paths in sorted(paths_by_id.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="duplicate_thesis_id",
                thesis_id=thesis_id,
                path=paths[0],
                message="交易 thesis_id 重复，后续复核和归因无法可靠引用。",
            )
        )


def _check_thesis_dates(
    thesis: TradeThesis,
    path: Path,
    as_of: date,
    issues: list[ThesisIssue],
) -> None:
    if thesis.created_at > as_of:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="created_at_in_future",
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message="created_at 晚于评估日期。",
            )
        )

    for metric in thesis.validation_metrics:
        if metric.updated_at and metric.updated_at > as_of:
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.ERROR,
                    code="metric_updated_at_in_future",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message=f"验证指标 {metric.metric_id} 的 updated_at 晚于评估日期。",
                )
            )

    for condition in thesis.falsification_conditions:
        if condition.triggered_at and condition.triggered_at > as_of:
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.ERROR,
                    code="condition_triggered_at_in_future",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message=f"证伪条件 {condition.condition_id} 的 triggered_at 晚于评估日期。",
                )
            )

    for risk_event in thesis.risk_events:
        if risk_event.updated_at and risk_event.updated_at > as_of:
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.ERROR,
                    code="risk_event_updated_at_in_future",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message=f"风险事件 {risk_event.risk_id} 的 updated_at 晚于评估日期。",
                )
            )


def _check_thesis_references(
    thesis: TradeThesis,
    path: Path,
    active_watchlist: dict[str, WatchlistItem],
    all_watchlist: dict[str, WatchlistItem],
    industry_node_ids: set[str],
    issues: list[ThesisIssue],
) -> None:
    active_item = active_watchlist.get(thesis.ticker)
    known_item = all_watchlist.get(thesis.ticker)

    if thesis.status in {"active", "paused"} and active_item is None:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="active_thesis_ticker_not_in_active_watchlist",
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message="活跃或暂停 thesis 的 ticker 必须在活跃观察池中。",
            )
        )
    elif known_item is not None and not known_item.active:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.WARNING,
                code="thesis_ticker_in_inactive_watchlist",
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message="该 ticker 在观察池中被标记为非活跃，复核结论不能作为当前交易依据。",
            )
        )

    for node_id in thesis.ai_chain_nodes:
        if node_id not in industry_node_ids:
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.ERROR,
                    code="unknown_ai_chain_node",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message=f"交易 thesis 引用了不存在的产业链节点：{node_id}",
                )
            )

    if active_item is not None:
        thesis_nodes = set(thesis.ai_chain_nodes)
        watchlist_nodes = set(active_item.ai_chain_nodes)
        if thesis_nodes and watchlist_nodes and thesis_nodes.isdisjoint(watchlist_nodes):
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.WARNING,
                    code="thesis_nodes_do_not_overlap_watchlist_nodes",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message="thesis 的产业链节点与观察池节点没有交集，需要确认映射是否合理。",
                )
            )


def _check_internal_ids(
    thesis: TradeThesis,
    path: Path,
    issues: list[ThesisIssue],
) -> None:
    _append_duplicate_child_id_issues(
        ids=[metric.metric_id for metric in thesis.validation_metrics],
        child_name="验证指标",
        code="duplicate_validation_metric_id",
        thesis=thesis,
        path=path,
        issues=issues,
    )
    _append_duplicate_child_id_issues(
        ids=[condition.condition_id for condition in thesis.falsification_conditions],
        child_name="证伪条件",
        code="duplicate_falsification_condition_id",
        thesis=thesis,
        path=path,
        issues=issues,
    )
    _append_duplicate_child_id_issues(
        ids=[risk_event.risk_id for risk_event in thesis.risk_events],
        child_name="风险事件",
        code="duplicate_risk_event_id",
        thesis=thesis,
        path=path,
        issues=issues,
    )


def _append_duplicate_child_id_issues(
    ids: list[str],
    child_name: str,
    code: str,
    thesis: TradeThesis,
    path: Path,
    issues: list[ThesisIssue],
) -> None:
    counts: dict[str, int] = defaultdict(int)
    for item_id in ids:
        counts[item_id] += 1
    for item_id, count in counts.items():
        if count <= 1:
            continue
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code=code,
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message=f"{child_name} ID 重复：{item_id}",
            )
        )


def _check_active_thesis_consistency(
    thesis: TradeThesis,
    path: Path,
    issues: list[ThesisIssue],
) -> None:
    if thesis.status != "active":
        return

    triggered_conditions = [
        condition
        for condition in thesis.falsification_conditions
        if condition.triggered and condition.severity in {"high", "critical"}
    ]
    for condition in triggered_conditions:
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="triggered_falsification_condition_still_active",
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message=(
                    f"高强度证伪条件 {condition.condition_id} 已触发，"
                    "但 thesis 仍是 active。"
                ),
            )
        )

    if all(metric.latest_status == "falsified" for metric in thesis.validation_metrics):
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.ERROR,
                code="all_validation_metrics_falsified",
                thesis_id=thesis.thesis_id,
                ticker=thesis.ticker,
                path=path,
                message="所有验证指标均已证伪，但 thesis 仍是 active。",
            )
        )


def _check_review_freshness(
    thesis: TradeThesis,
    path: Path,
    as_of: date,
    issues: list[ThesisIssue],
) -> None:
    if thesis.status not in {"active", "paused"}:
        return

    for metric in thesis.validation_metrics:
        if _is_metric_stale(metric, thesis.review_frequency, as_of):
            issues.append(
                ThesisIssue(
                    severity=ThesisIssueSeverity.WARNING,
                    code="validation_metric_stale",
                    thesis_id=thesis.thesis_id,
                    ticker=thesis.ticker,
                    path=path,
                    message=f"验证指标 {metric.metric_id} 已超过复核频率，需要更新证据。",
                )
            )


def _check_watchlist_thesis_expectations(
    watchlist: WatchlistConfig,
    active_thesis_by_ticker: dict[str, list[TradeThesis]],
    issues: list[ThesisIssue],
) -> None:
    for item in watchlist.items:
        if not item.active or not item.thesis_required:
            continue
        if item.ticker in active_thesis_by_ticker:
            continue
        issues.append(
            ThesisIssue(
                severity=ThesisIssueSeverity.WARNING,
                code="watchlist_ticker_requires_thesis_but_has_no_active_thesis",
                ticker=item.ticker,
                message=(
                    "观察池标记该 ticker 需要 thesis；在没有活跃 thesis 前，"
                    "报告不能把主动交易标记为高置信度。"
                ),
            )
        )


def _review_item(thesis: TradeThesis, as_of: date) -> ThesisReviewItem:
    confirmed = sum(
        1 for metric in thesis.validation_metrics if metric.latest_status == "confirmed"
    )
    pending = sum(1 for metric in thesis.validation_metrics if metric.latest_status == "pending")
    weakened = sum(1 for metric in thesis.validation_metrics if metric.latest_status == "weakened")
    falsified = sum(
        1 for metric in thesis.validation_metrics if metric.latest_status == "falsified"
    )
    stale_metric_ids = tuple(
        metric.metric_id
        for metric in thesis.validation_metrics
        if _is_metric_stale(metric, thesis.review_frequency, as_of)
    )
    triggered_conditions = tuple(
        condition.condition_id
        for condition in thesis.falsification_conditions
        if condition.triggered
    )
    active_risks = tuple(risk.risk_id for risk in thesis.risk_events if risk.active)

    if thesis.status == "invalidated" or falsified or triggered_conditions:
        health = "INVALIDATED"
        reason = "至少一个验证指标或证伪条件已经破坏原始假设。"
    elif thesis.status in {"closed", "draft"}:
        health = "INACTIVE"
        reason = "该 thesis 当前不是活跃交易假设。"
    elif weakened or pending or stale_metric_ids or active_risks or thesis.status == "paused":
        health = "WATCH"
        reason = "存在待确认、转弱、过期指标、暂停状态或活跃风险事件。"
    else:
        health = "INTACT"
        reason = "当前结构化验证指标未破坏原始假设。"

    return ThesisReviewItem(
        thesis_id=thesis.thesis_id,
        ticker=thesis.ticker,
        status=thesis.status,
        health=health,
        health_reason=reason,
        confirmed_metrics=confirmed,
        pending_metrics=pending,
        weakened_metrics=weakened,
        falsified_metrics=falsified,
        stale_metric_ids=stale_metric_ids,
        triggered_condition_ids=triggered_conditions,
        active_risk_event_ids=active_risks,
    )


def _is_metric_stale(
    metric: ValidationMetric,
    review_frequency: ThesisReviewFrequency,
    as_of: date,
) -> bool:
    max_age_days = {
        "daily": 1,
        "weekly": 7,
        "monthly": 31,
        "quarterly": 100,
        "event_driven": None,
    }[review_frequency]
    if max_age_days is None:
        return False
    if metric.updated_at is None:
        return True
    return (as_of - metric.updated_at).days > max_age_days


def _direction_label(value: str) -> str:
    return {
        "long": "做多",
        "short": "做空",
        "hedge": "对冲",
        "watch": "观察",
    }.get(value, value)


def _status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "active": "活跃",
        "paused": "暂停",
        "closed": "已关闭",
        "invalidated": "已证伪",
    }.get(value, value)


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _review_frequency_label(value: str) -> str:
    return {
        "daily": "每日",
        "weekly": "每周",
        "monthly": "每月",
        "quarterly": "每季",
        "event_driven": "事件驱动",
    }.get(value, value)


def _health_label(value: str) -> str:
    return {
        "INTACT": "假设仍成立",
        "WATCH": "需要复核",
        "INVALIDATED": "证伪触发",
        "INACTIVE": "非活跃",
    }.get(value, value)


def _severity_label(severity: ThesisIssueSeverity) -> str:
    if severity == ThesisIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
