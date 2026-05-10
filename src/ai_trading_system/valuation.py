from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError

from ai_trading_system.config import (
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
)

ValuationSourceType = Literal[
    "primary_filing",
    "paid_vendor",
    "manual_input",
    "public_convenience",
]
CrowdingStatus = Literal["normal", "elevated", "extreme", "unknown"]
ValuationAssessment = Literal["cheap", "reasonable", "expensive", "extreme", "unknown"]
ValuationPointInTimeClass = Literal[
    "true_point_in_time",
    "captured_snapshot",
    "backfilled_history_distribution",
    "unknown",
]
ValuationHistorySourceClass = Literal[
    "vendor_archive",
    "captured_snapshot_history",
    "vendor_historical_endpoint",
    "vendor_current_trend",
    "manual_backfill",
    "none",
    "unknown",
]
ValuationConfidenceLevel = Literal["high", "medium", "low"]
ValuationBacktestUse = Literal[
    "strict_point_in_time",
    "captured_at_forward_only",
    "auxiliary_current_only",
    "not_for_backtest",
]


class ValuationIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class SnapshotMetric(BaseModel):
    metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    value: float
    unit: str = Field(min_length=1)
    period: str = Field(min_length=1)
    source_field: str = ""
    notes: str = ""


class CrowdingSignal(BaseModel):
    signal_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1)
    status: CrowdingStatus
    evidence_source: str = Field(min_length=1)
    updated_at: date
    notes: str = ""


class ValuationSnapshot(BaseModel):
    snapshot_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    ticker: str = Field(min_length=1)
    as_of: date
    source_type: ValuationSourceType
    source_name: str = Field(min_length=1)
    source_url: str = ""
    captured_at: date
    point_in_time_class: ValuationPointInTimeClass = "captured_snapshot"
    history_source_class: ValuationHistorySourceClass = "unknown"
    confidence_level: ValuationConfidenceLevel = "medium"
    confidence_reason: str = ""
    backtest_use: ValuationBacktestUse = "captured_at_forward_only"
    valuation_metrics: list[SnapshotMetric] = Field(min_length=1)
    expectation_metrics: list[SnapshotMetric] = Field(default_factory=list)
    crowding_signals: list[CrowdingSignal] = Field(default_factory=list)
    valuation_percentile: float | None = Field(default=None, ge=0, le=100)
    overall_assessment: ValuationAssessment = "unknown"
    notes: str = ""


@dataclass(frozen=True)
class LoadedValuationSnapshot:
    snapshot: ValuationSnapshot
    path: Path


@dataclass(frozen=True)
class ValuationLoadError:
    path: Path
    message: str


@dataclass(frozen=True)
class ValuationSnapshotStore:
    input_path: Path
    loaded: tuple[LoadedValuationSnapshot, ...]
    load_errors: tuple[ValuationLoadError, ...]


@dataclass(frozen=True)
class ValuationIssue:
    severity: ValuationIssueSeverity
    code: str
    message: str
    snapshot_id: str | None = None
    ticker: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class ValuationValidationReport:
    as_of: date
    input_path: Path
    snapshots: tuple[LoadedValuationSnapshot, ...]
    issues: tuple[ValuationIssue, ...] = field(default_factory=tuple)

    @property
    def snapshot_count(self) -> int:
        return len(self.snapshots)

    @property
    def ticker_count(self) -> int:
        return len({loaded.snapshot.ticker for loaded in self.snapshots})

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

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
class ValuationReviewItem:
    snapshot_id: str
    ticker: str
    source_type: ValuationSourceType
    as_of: date
    point_in_time_class: ValuationPointInTimeClass
    history_source_class: ValuationHistorySourceClass
    confidence_level: ValuationConfidenceLevel
    confidence_reason: str
    backtest_use: ValuationBacktestUse
    health: str
    reason: str
    valuation_percentile: float | None
    overall_assessment: ValuationAssessment
    extreme_crowding_signals: tuple[str, ...]
    elevated_crowding_signals: tuple[str, ...]


@dataclass(frozen=True)
class ValuationReviewReport:
    as_of: date
    validation_report: ValuationValidationReport
    items: tuple[ValuationReviewItem, ...]

    @property
    def status(self) -> str:
        if self.validation_report.error_count:
            return "FAIL"
        if self.validation_report.warning_count:
            return "PASS_WITH_WARNINGS"
        if any(
            item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
            for item in self.items
        ):
            return "PASS_WITH_WARNINGS"
        return "PASS"


def load_valuation_snapshot_store(input_path: Path | str) -> ValuationSnapshotStore:
    path = Path(input_path)
    loaded: list[LoadedValuationSnapshot] = []
    load_errors: list[ValuationLoadError] = []

    for yaml_path in _snapshot_yaml_paths(path):
        try:
            raw = _load_yaml(yaml_path)
        except OSError as exc:
            load_errors.append(ValuationLoadError(path=yaml_path, message=str(exc)))
            continue
        except yaml.YAMLError as exc:
            load_errors.append(
                ValuationLoadError(path=yaml_path, message=f"YAML 解析失败：{exc}")
            )
            continue

        for raw_item in _raw_snapshot_items(raw):
            try:
                snapshot = ValuationSnapshot.model_validate(raw_item)
            except ValidationError as exc:
                load_errors.append(
                    ValuationLoadError(
                        path=yaml_path,
                        message=_compact_validation_error(exc),
                    )
                )
                continue
            loaded.append(LoadedValuationSnapshot(snapshot=snapshot, path=yaml_path))

    return ValuationSnapshotStore(
        input_path=path,
        loaded=tuple(loaded),
        load_errors=tuple(load_errors),
    )


def validate_valuation_snapshot_store(
    store: ValuationSnapshotStore,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    as_of: date,
    max_snapshot_age_days: int = 45,
) -> ValuationValidationReport:
    issues: list[ValuationIssue] = []
    known_tickers = set(configured_price_tickers(universe, include_full_ai_chain=True))
    known_tickers.update(item.ticker for item in watchlist.items)

    for load_error in store.load_errors:
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="valuation_load_error",
                path=load_error.path,
                message=load_error.message,
            )
        )

    if not store.input_path.exists():
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="valuation_path_missing",
                path=store.input_path,
                message="估值快照目录或文件不存在；估值模块无法提供仓位折扣依据。",
            )
        )
    elif not store.loaded and not store.load_errors:
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="no_valuation_snapshots",
                path=store.input_path,
                message="未发现估值快照 YAML 文件；估值模块不能进入自动评分。",
            )
        )

    visible_snapshots: list[LoadedValuationSnapshot] = []
    for loaded in store.loaded:
        snapshot = loaded.snapshot
        if _snapshot_is_future(snapshot, as_of):
            issues.append(
                ValuationIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="valuation_snapshot_excluded_future_as_of",
                    snapshot_id=snapshot.snapshot_id,
                    ticker=snapshot.ticker,
                    path=loaded.path,
                    message=(
                        "估值快照日期或采集日期晚于评估日期，已从本次历史 as-of "
                        "评分和复核中排除。"
                    ),
                )
            )
            continue
        visible_snapshots.append(loaded)

    visible_snapshot_tuple = tuple(visible_snapshots)
    _check_duplicate_snapshot_ids(visible_snapshot_tuple, issues)
    current_snapshot_ids = {
        id(loaded)
        for loaded in _latest_visible_snapshots_by_ticker(
            visible_snapshot_tuple,
            as_of=as_of,
        )
    }
    for loaded in visible_snapshot_tuple:
        _check_snapshot(
            loaded=loaded,
            known_tickers=known_tickers,
            as_of=as_of,
            max_snapshot_age_days=max_snapshot_age_days,
            check_current_readiness=id(loaded) in current_snapshot_ids,
            issues=issues,
        )

    return ValuationValidationReport(
        as_of=as_of,
        input_path=store.input_path,
        snapshots=visible_snapshot_tuple,
        issues=tuple(issues),
    )


def build_valuation_review_report(
    validation_report: ValuationValidationReport,
) -> ValuationReviewReport:
    review_snapshots = _latest_visible_snapshots_by_ticker(
        validation_report.snapshots,
        as_of=validation_report.as_of,
    )
    return ValuationReviewReport(
        as_of=validation_report.as_of,
        validation_report=validation_report,
        items=tuple(
            _review_item(loaded.snapshot, validation_report.as_of)
            for loaded in review_snapshots
        ),
    )


def render_valuation_validation_report(report: ValuationValidationReport) -> str:
    lines = [
        "# 估值与拥挤度校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.input_path}`",
        f"- 快照数量：{report.snapshot_count}",
        f"- 覆盖标的数：{report.ticker_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 当前评分指标覆盖",
        "",
        *_current_valuation_metric_coverage_lines(report),
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未发现可读取的估值快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 日期 | 来源类型 | PIT 等级 | 历史来源 | 可信度 | "
                "回测用途 | 来源 | 估值分位 | 评估 | 指标数 | 拥挤度信号 |",
                "|---|---|---|---|---|---|---|---|---|---:|---|---:|---:|",
            ]
        )
        for loaded in sorted(report.snapshots, key=lambda item: item.snapshot.snapshot_id):
            snapshot = loaded.snapshot
            percentile = (
                "n/a"
                if snapshot.valuation_percentile is None
                else f"{snapshot.valuation_percentile:.0f}"
            )
            lines.append(
                "| "
                f"{snapshot.snapshot_id} | "
                f"{snapshot.ticker} | "
                f"{snapshot.as_of.isoformat()} | "
                f"{_source_type_label(snapshot.source_type)} | "
                f"{_point_in_time_class_label(snapshot.point_in_time_class)} | "
                f"{_history_source_class_label(snapshot.history_source_class)} | "
                f"{_confidence_level_label(snapshot.confidence_level)} | "
                f"{_backtest_use_label(snapshot.backtest_use)} | "
                f"{_escape_markdown_table(snapshot.source_name)} | "
                f"{percentile} | "
                f"{_assessment_label(snapshot.overall_assessment)} | "
                f"{len(snapshot.valuation_metrics) + len(snapshot.expectation_metrics)} | "
                f"{len(snapshot.crowding_signals)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Snapshot | Ticker | 文件 | 说明 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.snapshot_id or ''} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def render_valuation_review_report(report: ValuationReviewReport) -> str:
    validation = report.validation_report
    lines = [
        "# 估值与拥挤度复核报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{validation.input_path}`",
        f"- 快照数量：{validation.snapshot_count}",
        f"- 当前复核快照数：{len(report.items)}",
        f"- 覆盖标的数：{validation.ticker_count}",
        f"- 校验错误数：{validation.error_count}",
        f"- 校验警告数：{validation.warning_count}",
        "",
        "## 复核结论",
        "",
    ]
    if not report.items:
        lines.append("未发现可复核的估值快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 来源 | 日期 | PIT 等级 | 可信度 | 回测用途 | "
                "复核结论 | 原因 | 估值分位 | 评估 | 极端拥挤 | 偏热信号 |",
                "|---|---|---|---|---|---|---|---|---|---:|---|---|---|",
            ]
        )
        for item in sorted(report.items, key=lambda value: value.snapshot_id):
            percentile = (
                "n/a"
                if item.valuation_percentile is None
                else f"{item.valuation_percentile:.0f}"
            )
            lines.append(
                "| "
                f"{item.snapshot_id} | "
                f"{item.ticker} | "
                f"{_source_type_label(item.source_type)} | "
                f"{item.as_of.isoformat()} | "
                f"{_point_in_time_class_label(item.point_in_time_class)} | "
                f"{_confidence_level_label(item.confidence_level)} | "
                f"{_backtest_use_label(item.backtest_use)} | "
                f"{_health_label(item.health)} | "
                f"{_escape_markdown_table(_valuation_reason_with_confidence(item))} | "
                f"{percentile} | "
                f"{_assessment_label(item.overall_assessment)} | "
                f"{', '.join(item.extreme_crowding_signals)} | "
                f"{', '.join(item.elevated_crowding_signals)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 估值和拥挤度只用于仓位折扣和人工复核，不直接触发买卖。",
            "- `public_convenience` 来源只能作为辅助，不能直接进入自动评分。",
            "- PIT 等级区分真实 point-in-time、采集日快照和回填历史分布；"
            "低可信回填分布不能被解释成历史时点当时可见的 vendor archive。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_valuation_validation_report(
    report: ValuationValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_valuation_validation_report(report), encoding="utf-8")
    return output_path


def write_valuation_review_report(report: ValuationReviewReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_valuation_review_report(report), encoding="utf-8")
    return output_path


def _current_valuation_metric_coverage_lines(report: ValuationValidationReport) -> list[str]:
    current_snapshots = _latest_visible_snapshots_by_ticker(
        report.snapshots,
        as_of=report.as_of,
    )
    total = len(current_snapshots)
    if total == 0:
        return [
            "- 当前可见快照数：0",
            "- 没有可用于当日评分的估值快照。",
        ]

    percentile_count = sum(
        1 for loaded in current_snapshots if loaded.snapshot.valuation_percentile is not None
    )
    eps_revision_count = sum(
        1
        for loaded in current_snapshots
        if _snapshot_has_metric(loaded.snapshot, "eps_revision_90d_pct")
    )
    return [
        f"- 当前可见快照数：{total}（按 ticker 选择不晚于评估日的最新快照）",
        "",
        "| 指标 | 覆盖 | 评分/复核用途 | 缺口说明 |",
        "|---|---:|---|---|",
        (
            "| valuation_percentile | "
            f"{percentile_count}/{total} | "
            "估值模块自动评分 | "
            "需要每个估值 metric 至少 3 个 point-in-time 历史点 |"
        ),
        (
            "| eps_revision_90d_pct | "
            f"{eps_revision_count}/{total} | "
            "预期变化复核 | "
            "需要约 90 日前同一 fiscal estimate date 的 analyst-estimates 历史快照 |"
        ),
    ]


def default_valuation_validation_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"valuation_validation_{as_of.isoformat()}.md"


def default_valuation_review_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"valuation_review_{as_of.isoformat()}.md"


def _snapshot_yaml_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _raw_snapshot_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "snapshots" in raw:
        snapshots = raw["snapshots"]
        if isinstance(snapshots, list):
            return snapshots
        return [snapshots]
    return [raw]


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "valuation snapshot schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _check_duplicate_snapshot_ids(
    snapshots: tuple[LoadedValuationSnapshot, ...],
    issues: list[ValuationIssue],
) -> None:
    paths_by_id: dict[str, list[Path]] = defaultdict(list)
    for loaded in snapshots:
        paths_by_id[loaded.snapshot.snapshot_id].append(loaded.path)

    for snapshot_id, paths in sorted(paths_by_id.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="duplicate_snapshot_id",
                snapshot_id=snapshot_id,
                path=paths[0],
                message="估值 snapshot_id 重复，后续复核无法可靠引用。",
            )
        )


def _latest_visible_snapshots_by_ticker(
    snapshots: tuple[LoadedValuationSnapshot, ...],
    *,
    as_of: date,
) -> tuple[LoadedValuationSnapshot, ...]:
    latest_by_ticker: dict[str, LoadedValuationSnapshot] = {}
    for loaded in snapshots:
        snapshot = loaded.snapshot
        if snapshot.as_of > as_of or snapshot.captured_at > as_of:
            continue
        current = latest_by_ticker.get(snapshot.ticker)
        if current is None or _valuation_snapshot_sort_key(loaded) > (
            _valuation_snapshot_sort_key(current)
        ):
            latest_by_ticker[snapshot.ticker] = loaded
    return tuple(
        sorted(
            latest_by_ticker.values(),
            key=lambda loaded: (loaded.snapshot.ticker, loaded.snapshot.snapshot_id),
        )
    )


def _valuation_snapshot_sort_key(loaded: LoadedValuationSnapshot) -> tuple[date, date, str]:
    snapshot = loaded.snapshot
    return (snapshot.as_of, snapshot.captured_at, snapshot.snapshot_id)


def _snapshot_is_future(snapshot: ValuationSnapshot, as_of: date) -> bool:
    return snapshot.as_of > as_of or snapshot.captured_at > as_of


def _check_snapshot(
    loaded: LoadedValuationSnapshot,
    known_tickers: set[str],
    as_of: date,
    max_snapshot_age_days: int,
    check_current_readiness: bool,
    issues: list[ValuationIssue],
) -> None:
    snapshot = loaded.snapshot
    path = loaded.path
    if snapshot.ticker not in known_tickers:
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="unknown_ticker",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="估值快照 ticker 未出现在数据 universe 或观察池中。",
            )
        )

    if _snapshot_is_future(snapshot, as_of):
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="valuation_date_in_future",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="估值快照日期或采集日期晚于评估日期。",
            )
        )

    if check_current_readiness and (as_of - snapshot.as_of).days > max_snapshot_age_days:
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="valuation_snapshot_stale",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="估值快照超过新鲜度阈值，需要更新后再用于仓位折扣。",
            )
        )

    if snapshot.source_type == "public_convenience":
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="public_convenience_source",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="公开便利来源只能作为辅助，不得直接进入自动评分。",
            )
        )

    if snapshot.point_in_time_class == "backfilled_history_distribution":
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="backfilled_valuation_not_strict_point_in_time",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message=(
                    "该估值快照来自回填历史分布，只能作为采集日之后的辅助参考，"
                    "不能当作严格 point-in-time 历史估值输入。"
                ),
            )
        )
    if (
        snapshot.confidence_level == "low"
        and snapshot.backtest_use == "strict_point_in_time"
    ):
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="low_confidence_strict_backtest_use",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="低可信估值快照不能声明为 strict_point_in_time 回测输入。",
            )
        )

    if check_current_readiness and snapshot.valuation_percentile is None:
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_valuation_percentile",
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message="缺少估值历史分位，无法判断当前价格透支程度。",
            )
        )

    _check_duplicate_child_ids(snapshot, path, issues)
    _check_metric_values(snapshot, path, issues)


def _check_duplicate_child_ids(
    snapshot: ValuationSnapshot,
    path: Path,
    issues: list[ValuationIssue],
) -> None:
    metric_ids = [metric.metric_id for metric in snapshot.valuation_metrics]
    metric_ids.extend(metric.metric_id for metric in snapshot.expectation_metrics)
    signal_ids = [signal.signal_id for signal in snapshot.crowding_signals]
    _append_duplicate_id_issue(
        metric_ids,
        "duplicate_metric_id",
        "估值或预期指标",
        snapshot,
        path,
        issues,
    )
    _append_duplicate_id_issue(
        signal_ids,
        "duplicate_crowding_signal_id",
        "拥挤度信号",
        snapshot,
        path,
        issues,
    )


def _append_duplicate_id_issue(
    ids: list[str],
    code: str,
    item_name: str,
    snapshot: ValuationSnapshot,
    path: Path,
    issues: list[ValuationIssue],
) -> None:
    counts: dict[str, int] = defaultdict(int)
    for item_id in ids:
        counts[item_id] += 1
    for item_id, count in counts.items():
        if count <= 1:
            continue
        issues.append(
            ValuationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code=code,
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                path=path,
                message=f"{item_name} ID 重复：{item_id}",
            )
        )


def _check_metric_values(
    snapshot: ValuationSnapshot,
    path: Path,
    issues: list[ValuationIssue],
) -> None:
    non_negative_keywords = ("pe", "p_e", "ps", "p_s", "ev_sales", "peg", "multiple")
    for metric in (*snapshot.valuation_metrics, *snapshot.expectation_metrics):
        metric_key = metric.metric_id.lower()
        if any(keyword in metric_key for keyword in non_negative_keywords) and metric.value < 0:
            issues.append(
                ValuationIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="negative_valuation_multiple",
                    snapshot_id=snapshot.snapshot_id,
                    ticker=snapshot.ticker,
                    path=path,
                    message=f"估值倍数指标 {metric.metric_id} 不能为负数。",
                )
            )


def _snapshot_has_metric(snapshot: ValuationSnapshot, metric_id: str) -> bool:
    return any(
        metric.metric_id == metric_id
        for metric in (*snapshot.valuation_metrics, *snapshot.expectation_metrics)
    )


def _review_item(snapshot: ValuationSnapshot, as_of: date) -> ValuationReviewItem:
    extreme_signals = tuple(
        signal.signal_id for signal in snapshot.crowding_signals if signal.status == "extreme"
    )
    elevated_signals = tuple(
        signal.signal_id for signal in snapshot.crowding_signals if signal.status == "elevated"
    )
    percentile = snapshot.valuation_percentile

    if (as_of - snapshot.as_of).days > 45:
        health = "STALE"
        reason = "估值快照已过期，不能作为当前仓位折扣依据。"
    elif snapshot.overall_assessment == "extreme" or extreme_signals or (
        percentile is not None and percentile >= 90
    ):
        health = "EXTREME_OVERHEATED"
        reason = "估值或拥挤度处于极端区间，应显著降低仓位置信度。"
    elif snapshot.overall_assessment == "expensive" or elevated_signals or (
        percentile is not None and percentile >= 75
    ):
        health = "EXPENSIVE_OR_CROWDED"
        reason = "估值偏贵或拥挤度偏高，只能作为仓位折扣信号。"
    elif snapshot.overall_assessment == "cheap" or (
        percentile is not None and percentile <= 30
    ):
        health = "POTENTIAL_VALUE"
        reason = "估值分位较低，但仍需结合基本面和风险事件确认。"
    else:
        health = "NEUTRAL"
        reason = "估值和拥挤度没有给出明确折扣信号。"

    return ValuationReviewItem(
        snapshot_id=snapshot.snapshot_id,
        ticker=snapshot.ticker,
        source_type=snapshot.source_type,
        as_of=snapshot.as_of,
        point_in_time_class=snapshot.point_in_time_class,
        history_source_class=snapshot.history_source_class,
        confidence_level=snapshot.confidence_level,
        confidence_reason=snapshot.confidence_reason,
        backtest_use=snapshot.backtest_use,
        health=health,
        reason=reason,
        valuation_percentile=percentile,
        overall_assessment=snapshot.overall_assessment,
        extreme_crowding_signals=extreme_signals,
        elevated_crowding_signals=elevated_signals,
    )


def _source_type_label(value: str) -> str:
    return {
        "primary_filing": "一手披露",
        "paid_vendor": "付费供应商",
        "manual_input": "手工录入",
        "public_convenience": "公开便利源",
    }.get(value, value)


def _assessment_label(value: str) -> str:
    return {
        "cheap": "偏便宜",
        "reasonable": "合理",
        "expensive": "偏贵",
        "extreme": "极端",
        "unknown": "未知",
    }.get(value, value)


def _health_label(value: str) -> str:
    return {
        "STALE": "数据过期",
        "EXTREME_OVERHEATED": "极端过热",
        "EXPENSIVE_OR_CROWDED": "偏贵或拥挤",
        "POTENTIAL_VALUE": "可能有性价比",
        "NEUTRAL": "中性",
    }.get(value, value)


def _point_in_time_class_label(value: str) -> str:
    return {
        "true_point_in_time": "真实 PIT",
        "captured_snapshot": "采集日快照",
        "backfilled_history_distribution": "回填历史分布",
        "unknown": "未知",
    }.get(value, value)


def _history_source_class_label(value: str) -> str:
    return {
        "vendor_archive": "供应商历史档案",
        "captured_snapshot_history": "本地采集快照历史",
        "vendor_historical_endpoint": "供应商 historical 接口",
        "vendor_current_trend": "供应商当前趋势",
        "manual_backfill": "人工回填",
        "none": "无",
        "unknown": "未知",
    }.get(value, value)


def _confidence_level_label(value: str) -> str:
    return {
        "high": "高",
        "medium": "中",
        "low": "低",
    }.get(value, value)


def _backtest_use_label(value: str) -> str:
    return {
        "strict_point_in_time": "严格 PIT",
        "captured_at_forward_only": "采集日后可见",
        "auxiliary_current_only": "仅辅助参考",
        "not_for_backtest": "不用于回测",
    }.get(value, value)


def _valuation_reason_with_confidence(item: ValuationReviewItem) -> str:
    if item.confidence_reason:
        return f"{item.reason} 可信度限制：{item.confidence_reason}"
    return item.reason


def _severity_label(severity: ValuationIssueSeverity) -> str:
    if severity == ValuationIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
