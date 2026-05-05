from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.pit_snapshots import (
    PitSnapshotManifestRecord,
    PitSnapshotValidationReport,
)


@dataclass(frozen=True)
class BacktestPitCoverageSourceSummary:
    module_family: str
    source_id: str
    snapshot_count: int
    row_count: int
    ticker_count: int
    first_available_date: date | None
    latest_available_date: date | None
    unique_available_dates: int
    latest_staleness_days: int | None
    point_in_time_class_counts: dict[str, int]
    backtest_use_counts: dict[str, int]
    confidence_level_counts: dict[str, int]


@dataclass(frozen=True)
class BacktestPitCoverageReadiness:
    module_family: str
    current_grade: str
    first_b_grade_date: date | None
    first_a_grade_date: date | None
    covered_dates: int
    strict_point_in_time_dates: int
    captured_snapshot_dates: int
    latest_available_date: date | None
    reason: str
    exit_condition: str


@dataclass(frozen=True)
class BacktestPitCoverageReport:
    as_of: date
    manifest_path: Path
    manifest_status: str
    snapshot_count: int
    row_count: int
    min_forward_days: int
    max_staleness_days: int
    source_summaries: tuple[BacktestPitCoverageSourceSummary, ...]
    readiness: tuple[BacktestPitCoverageReadiness, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if self.manifest_status == "FAIL":
            return "FAIL"
        if not self.snapshot_count:
            return "PASS_WITH_WARNINGS"
        if any(item.current_grade == "C" for item in self.readiness):
            return "PASS_WITH_WARNINGS"
        if any(
            item.latest_staleness_days is not None
            and item.latest_staleness_days > self.max_staleness_days
            for item in self.source_summaries
        ):
            return "PASS_WITH_WARNINGS"
        return "PASS"


def default_backtest_pit_coverage_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"backtest_pit_coverage_{as_of.isoformat()}.md"


def build_backtest_pit_coverage_report(
    validation_report: PitSnapshotValidationReport,
    *,
    min_forward_days: int = 60,
    max_staleness_days: int = 3,
) -> BacktestPitCoverageReport:
    if min_forward_days <= 0:
        raise ValueError("min_forward_days must be positive")
    if max_staleness_days < 0:
        raise ValueError("max_staleness_days must be non-negative")

    records = validation_report.records if validation_report.passed else tuple()
    return BacktestPitCoverageReport(
        as_of=validation_report.as_of,
        manifest_path=validation_report.input_path,
        manifest_status=validation_report.status,
        snapshot_count=len(records),
        row_count=sum(record.row_count for record in records),
        min_forward_days=min_forward_days,
        max_staleness_days=max_staleness_days,
        source_summaries=_source_summaries(
            records,
            as_of=validation_report.as_of,
        ),
        readiness=_readiness_summaries(
            records,
            as_of=validation_report.as_of,
            min_forward_days=min_forward_days,
            max_staleness_days=max_staleness_days,
        ),
    )


def render_backtest_pit_coverage_report(report: BacktestPitCoverageReport) -> str:
    lines = [
        "# Forward-only PIT 覆盖持续验证报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Manifest 状态：{report.manifest_status}",
        f"- Manifest：`{report.manifest_path}`",
        f"- 快照数量：{report.snapshot_count}",
        f"- 原始记录数：{report.row_count}",
        f"- B 级最小 forward-only 覆盖日期数：{report.min_forward_days}",
        f"- 最新快照最大允许日龄：{report.max_staleness_days}",
        f"- production_effect={report.production_effect}",
        "",
        "## 方法边界",
        "",
        (
            "本报告只评估已经通过 PIT manifest 校验的 forward-only 自建快照覆盖，"
            "不补造历史估值、盈利预期或风险事件输入。"
        ),
        (
            "B 级表示自建 captured snapshot 已自然积累到最小覆盖日期数；"
            "A 级仍需要 `strict_point_in_time` vendor archive 或等价一手可见时间证明。"
        ),
        (
            "首个自建快照日期之前的历史回测仍应保持 C 级或探索性解释，"
            "不得把采集日之后才可见的供应商视图回填到过去。"
        ),
        "",
        "## 升级日期判断",
        "",
        (
            "| 输入族 | 当前等级 | 覆盖日期 | captured 日期 | strict PIT 日期 | "
            "B 级起点 | A 级起点 | 最新日期 | 原因 | 解除条件 |"
        ),
        "|---|---|---:|---:|---:|---|---|---|---|---|",
    ]
    for item in report.readiness:
        lines.append(_readiness_row(item))
    if not report.readiness:
        lines.append(
            "| valuation_expectations | C | 0 | 0 | 0 | n/a | n/a | n/a | "
            "没有可用 PIT 快照 | 运行 forward-only PIT 抓取并通过 manifest 校验 |"
        )

    lines.extend(
        [
            "",
            "## Source 覆盖摘要",
            "",
            (
                "| 输入族 | Source | 快照数 | Row count | Ticker | 覆盖日期 | "
                "最早日期 | 最新日期 | 最新日龄 | PIT class | Backtest use | Confidence |"
            ),
            "|---|---|---:|---:|---:|---:|---|---|---:|---|---|---|",
        ]
    )
    for summary in report.source_summaries:
        lines.append(_source_row(summary))
    if not report.source_summaries:
        lines.append(
            "| valuation_expectations | n/a | 0 | 0 | 0 | 0 | n/a | n/a | n/a | "
            "n/a | n/a | n/a |"
        )

    lines.extend(
        [
            "",
            "## 历史 C 级原因",
            "",
            *(_historical_c_grade_reason_lines(report)),
        ]
    )
    return "\n".join(lines) + "\n"


def write_backtest_pit_coverage_report(
    report: BacktestPitCoverageReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_backtest_pit_coverage_report(report), encoding="utf-8")
    return output_path


def _source_summaries(
    records: tuple[PitSnapshotManifestRecord, ...],
    *,
    as_of: date,
) -> tuple[BacktestPitCoverageSourceSummary, ...]:
    grouped: dict[tuple[str, str], list[PitSnapshotManifestRecord]] = defaultdict(list)
    for record in records:
        grouped[(_module_family(record), record.source_id)].append(record)

    summaries: list[BacktestPitCoverageSourceSummary] = []
    for (module_family, source_id), source_records in sorted(grouped.items()):
        available_dates = sorted(
            {
                available_date
                for record in source_records
                if (available_date := _available_date(record)) is not None
            }
        )
        latest_available_date = available_dates[-1] if available_dates else None
        summaries.append(
            BacktestPitCoverageSourceSummary(
                module_family=module_family,
                source_id=source_id,
                snapshot_count=len(source_records),
                row_count=sum(record.row_count for record in source_records),
                ticker_count=len(
                    {
                        record.canonical_ticker
                        for record in source_records
                        if record.canonical_ticker
                    }
                ),
                first_available_date=available_dates[0] if available_dates else None,
                latest_available_date=latest_available_date,
                unique_available_dates=len(available_dates),
                latest_staleness_days=(
                    None
                    if latest_available_date is None
                    else (as_of - latest_available_date).days
                ),
                point_in_time_class_counts=dict(
                    Counter(record.point_in_time_class for record in source_records)
                ),
                backtest_use_counts=dict(
                    Counter(record.backtest_use for record in source_records)
                ),
                confidence_level_counts=dict(
                    Counter(record.confidence_level for record in source_records)
                ),
            )
        )
    return tuple(summaries)


def _readiness_summaries(
    records: tuple[PitSnapshotManifestRecord, ...],
    *,
    as_of: date,
    min_forward_days: int,
    max_staleness_days: int,
) -> tuple[BacktestPitCoverageReadiness, ...]:
    grouped: dict[str, list[PitSnapshotManifestRecord]] = defaultdict(list)
    for record in records:
        grouped[_module_family(record)].append(record)

    readiness: list[BacktestPitCoverageReadiness] = []
    for module_family, module_records in sorted(grouped.items()):
        all_dates = _record_dates(module_records)
        captured_dates = _record_dates(
            [
                record
                for record in module_records
                if record.point_in_time_class == "captured_snapshot"
                and record.backtest_use == "captured_at_forward_only"
            ]
        )
        strict_dates = _record_dates(
            [
                record
                for record in module_records
                if record.point_in_time_class == "true_point_in_time"
                and record.backtest_use == "strict_point_in_time"
            ]
        )
        latest_date = all_dates[-1] if all_dates else None
        latest_stale = (
            latest_date is not None
            and (as_of - latest_date).days > max_staleness_days
        )
        first_b = _first_eligible_date(captured_dates, min_forward_days)
        first_a = _first_eligible_date(strict_dates, min_forward_days)
        if first_a is not None and not latest_stale:
            grade = "A"
            reason = "已有足够 strict PIT 覆盖且最新快照未过期。"
        elif first_b is not None and not latest_stale:
            grade = "B"
            reason = "已有足够 captured snapshot forward-only 覆盖且最新快照未过期。"
        elif latest_stale:
            grade = "C"
            reason = "forward-only 快照已过期，不能升级历史解释等级。"
        else:
            grade = "C"
            reason = "forward-only 覆盖日期不足，历史回测仍为探索性输入。"
        readiness.append(
            BacktestPitCoverageReadiness(
                module_family=module_family,
                current_grade=grade,
                first_b_grade_date=first_b,
                first_a_grade_date=first_a,
                covered_dates=len(all_dates),
                strict_point_in_time_dates=len(strict_dates),
                captured_snapshot_dates=len(captured_dates),
                latest_available_date=latest_date,
                reason=reason,
                exit_condition=_exit_condition(
                    first_b=first_b,
                    first_a=first_a,
                    min_forward_days=min_forward_days,
                ),
            )
        )
    return tuple(readiness)


def _historical_c_grade_reason_lines(report: BacktestPitCoverageReport) -> list[str]:
    if report.manifest_status == "FAIL":
        return ["- PIT manifest 校验失败，不能把任何快照计入回测覆盖。"]
    if not report.snapshot_count:
        return ["- 尚无通过校验的 forward-only PIT 快照，历史回测保持 C 级。"]
    first_dates = [
        summary.first_available_date
        for summary in report.source_summaries
        if summary.first_available_date is not None
    ]
    lines: list[str] = []
    if first_dates:
        first_date = min(first_dates)
        lines.append(
            f"- {first_date.isoformat()} 之前没有自建可见时间证明，历史输入保持 C 级。"
        )
    if not any(item.first_a_grade_date for item in report.readiness):
        lines.append("- 没有 strict PIT vendor archive 或等价证明，不能升级为 A 级。")
    stale_sources = [
        summary
        for summary in report.source_summaries
        if summary.latest_staleness_days is not None
        and summary.latest_staleness_days > report.max_staleness_days
    ]
    for summary in stale_sources:
        lines.append(
            f"- {summary.source_id} 最新快照日龄 {summary.latest_staleness_days} 天，"
            "需要恢复日常抓取。"
        )
    return lines or ["- 当前没有额外 C 级原因，但生产信任仍需样本长度和 owner 审批。"]


def _module_family(record: PitSnapshotManifestRecord) -> str:
    source_id = record.source_id.lower()
    endpoint = record.endpoint.lower()
    if "risk" in source_id or "risk" in endpoint:
        return "risk_events"
    if "sec" in source_id or "companyfacts" in endpoint:
        return "fundamentals"
    if (
        "valuation" in source_id
        or "earnings_trends" in source_id
        or "analyst" in endpoint
        or "estimate" in endpoint
        or "price-target" in endpoint
        or "rating" in endpoint
        or "earnings" in endpoint
    ):
        return "valuation_expectations"
    return "other"


def _record_dates(records: list[PitSnapshotManifestRecord]) -> list[date]:
    return sorted(
        {
            available_date
            for record in records
            if (available_date := _available_date(record)) is not None
        }
    )


def _available_date(record: PitSnapshotManifestRecord) -> date | None:
    try:
        return datetime.fromisoformat(record.available_time).date()
    except ValueError:
        return None


def _first_eligible_date(dates: list[date], min_forward_days: int) -> date | None:
    if len(dates) < min_forward_days:
        return None
    return dates[min_forward_days - 1]


def _exit_condition(
    *,
    first_b: date | None,
    first_a: date | None,
    min_forward_days: int,
) -> str:
    if first_a is not None:
        return "已具备 A 级日期；继续保持 daily archive 和 manifest 校验。"
    if first_b is not None:
        return "如需 A 级，接入 strict PIT vendor archive 或一手可见时间证明。"
    return f"至少积累 {min_forward_days} 个通过校验的 forward-only 可见日期。"


def _readiness_row(item: BacktestPitCoverageReadiness) -> str:
    return (
        "| "
        f"{item.module_family} | "
        f"{item.current_grade} | "
        f"{item.covered_dates} | "
        f"{item.captured_snapshot_dates} | "
        f"{item.strict_point_in_time_dates} | "
        f"{_format_date(item.first_b_grade_date)} | "
        f"{_format_date(item.first_a_grade_date)} | "
        f"{_format_date(item.latest_available_date)} | "
        f"{_escape(item.reason)} | "
        f"{_escape(item.exit_condition)} |"
    )


def _source_row(summary: BacktestPitCoverageSourceSummary) -> str:
    return (
        "| "
        f"{summary.module_family} | "
        f"{summary.source_id} | "
        f"{summary.snapshot_count} | "
        f"{summary.row_count} | "
        f"{summary.ticker_count} | "
        f"{summary.unique_available_dates} | "
        f"{_format_date(summary.first_available_date)} | "
        f"{_format_date(summary.latest_available_date)} | "
        f"{_format_optional_int(summary.latest_staleness_days)} | "
        f"{_escape(_counts(summary.point_in_time_class_counts))} | "
        f"{_escape(_counts(summary.backtest_use_counts))} | "
        f"{_escape(_counts(summary.confidence_level_counts))} |"
    )


def _counts(values: dict[str, int]) -> str:
    return ", ".join(f"{key}={values[key]}" for key in sorted(values)) or "n/a"


def _format_date(value: date | None) -> str:
    return "n/a" if value is None else value.isoformat()


def _format_optional_int(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _escape(value: str) -> str:
    return value.replace("|", "\\|")
