from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    DataSourceConfig,
    DataSourcesConfig,
)

PIT_SNAPSHOT_MANIFEST_COLUMNS = (
    "snapshot_id",
    "source_id",
    "source_name",
    "source_type",
    "source_quality_tier",
    "endpoint",
    "request_params",
    "provider_symbol",
    "canonical_ticker",
    "provider_symbol_alias",
    "http_status",
    "content_type",
    "response_headers",
    "raw_payload_path",
    "raw_payload_sha256",
    "raw_payload_bytes",
    "snapshot_time",
    "ingested_at",
    "vendor_timestamp",
    "available_time",
    "row_count",
    "parser_version",
    "schema_version",
    "license_use_class",
    "redistribution_allowed",
    "llm_processing_allowed",
    "point_in_time_class",
    "history_source_class",
    "backtest_use",
    "confidence_level",
    "confidence_reason",
    "validation_status",
    "validation_report_path",
)

REQUIRED_VALUE_COLUMNS = (
    "snapshot_id",
    "source_id",
    "source_name",
    "source_type",
    "source_quality_tier",
    "endpoint",
    "request_params",
    "canonical_ticker",
    "raw_payload_path",
    "raw_payload_sha256",
    "raw_payload_bytes",
    "snapshot_time",
    "ingested_at",
    "available_time",
    "row_count",
    "parser_version",
    "schema_version",
    "license_use_class",
    "redistribution_allowed",
    "llm_processing_allowed",
    "point_in_time_class",
    "history_source_class",
    "backtest_use",
    "confidence_level",
    "confidence_reason",
    "validation_status",
)

PIT_POINT_IN_TIME_CLASSES = {
    "true_point_in_time",
    "captured_snapshot",
    "backfilled_history_distribution",
    "unknown",
}
PIT_HISTORY_SOURCE_CLASSES = {
    "vendor_archive",
    "captured_snapshot_history",
    "vendor_historical_endpoint",
    "vendor_current_trend",
    "manual_backfill",
    "none",
    "unknown",
}
PIT_BACKTEST_USES = {
    "strict_point_in_time",
    "captured_at_forward_only",
    "auxiliary_current_only",
    "not_for_backtest",
}
PIT_CONFIDENCE_LEVELS = {"high", "medium", "low"}
PIT_VALIDATION_STATUSES = {"PASS", "PASS_WITH_WARNINGS", "FAIL"}
PIT_SOURCE_TYPES = {
    "primary_source",
    "paid_vendor",
    "public_convenience",
    "manual_input",
}

DEFAULT_PIT_SNAPSHOT_DIR = PROJECT_ROOT / "data" / "raw" / "pit_snapshots"
DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH = DEFAULT_PIT_SNAPSHOT_DIR / "manifest.csv"
PIT_SNAPSHOT_SCHEMA_VERSION = "1"
PIT_SNAPSHOT_PARSER_VERSION = "pit_snapshot_manifest_v1"


class PitSnapshotIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class PitSnapshotManifestRecord:
    snapshot_id: str
    source_id: str
    source_name: str
    source_type: str
    source_quality_tier: str
    endpoint: str
    request_params: str
    provider_symbol: str
    canonical_ticker: str
    provider_symbol_alias: str
    http_status: str
    content_type: str
    response_headers: str
    raw_payload_path: str
    raw_payload_sha256: str
    raw_payload_bytes: int
    snapshot_time: str
    ingested_at: str
    vendor_timestamp: str
    available_time: str
    row_count: int
    parser_version: str
    schema_version: str
    license_use_class: str
    redistribution_allowed: bool
    llm_processing_allowed: bool
    point_in_time_class: str
    history_source_class: str
    backtest_use: str
    confidence_level: str
    confidence_reason: str
    validation_status: str
    validation_report_path: str


@dataclass(frozen=True)
class PitSnapshotValidationIssue:
    severity: PitSnapshotIssueSeverity
    code: str
    message: str
    snapshot_id: str | None = None
    source_id: str | None = None
    path: Path | None = None
    row_number: int | None = None


@dataclass(frozen=True)
class PitSnapshotValidationReport:
    as_of: date
    input_path: Path
    generated_at: datetime
    records: tuple[PitSnapshotManifestRecord, ...]
    issues: tuple[PitSnapshotValidationIssue, ...] = field(default_factory=tuple)
    production_effect: str = "none"

    @property
    def snapshot_count(self) -> int:
        return len(self.records)

    @property
    def source_count(self) -> int:
        return len({record.source_id for record in self.records})

    @property
    def row_count(self) -> int:
        return sum(record.row_count for record in self.records)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == PitSnapshotIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == PitSnapshotIssueSeverity.WARNING
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


def validate_pit_snapshot_manifest(
    *,
    input_path: Path,
    as_of: date,
    data_sources: DataSourcesConfig | None = None,
    project_root: Path = PROJECT_ROOT,
) -> PitSnapshotValidationReport:
    issues: list[PitSnapshotValidationIssue] = []
    records: list[PitSnapshotManifestRecord] = []
    if not input_path.exists():
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_manifest_missing",
                message=f"PIT snapshot manifest 不存在：{input_path}",
                path=input_path,
            )
        )
        return PitSnapshotValidationReport(
            as_of=as_of,
            input_path=input_path,
            generated_at=datetime.now(tz=UTC),
            records=tuple(),
            issues=tuple(issues),
        )
    try:
        frame = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    except Exception as exc:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_manifest_unreadable",
                message=f"PIT snapshot manifest 无法读取：{exc}",
                path=input_path,
            )
        )
        return PitSnapshotValidationReport(
            as_of=as_of,
            input_path=input_path,
            generated_at=datetime.now(tz=UTC),
            records=tuple(),
            issues=tuple(issues),
        )

    missing_columns = [
        column for column in PIT_SNAPSHOT_MANIFEST_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_manifest_missing_columns",
                message=f"manifest 缺少字段：{', '.join(missing_columns)}",
                path=input_path,
            )
        )
        return PitSnapshotValidationReport(
            as_of=as_of,
            input_path=input_path,
            generated_at=datetime.now(tz=UTC),
            records=tuple(),
            issues=tuple(issues),
        )

    source_catalog = {
        source.source_id: source for source in data_sources.sources
    } if data_sources is not None else {}
    seen_snapshot_ids: set[str] = set()
    for index, row in frame.iterrows():
        row_number = int(index) + 2
        record = _record_from_manifest_row(row)
        records.append(record)
        _validate_required_values(record, issues, row_number)
        if record.snapshot_id in seen_snapshot_ids:
            issues.append(
                PitSnapshotValidationIssue(
                    severity=PitSnapshotIssueSeverity.ERROR,
                    code="duplicate_pit_snapshot_id",
                    message=f"重复 snapshot_id：{record.snapshot_id}",
                    snapshot_id=record.snapshot_id,
                    source_id=record.source_id,
                    row_number=row_number,
                )
            )
        seen_snapshot_ids.add(record.snapshot_id)
        _validate_record_enums(record, issues, row_number)
        _validate_record_json_fields(record, issues, row_number)
        _validate_record_dates(record, issues, row_number)
        _validate_payload(record, input_path=input_path, project_root=project_root, issues=issues)
        _validate_source_catalog(record, source_catalog, issues, row_number)

    return PitSnapshotValidationReport(
        as_of=as_of,
        input_path=input_path,
        generated_at=datetime.now(tz=UTC),
        records=tuple(records),
        issues=tuple(issues),
    )


def discover_existing_pit_raw_snapshots(
    *,
    fmp_analyst_history_dir: Path,
    fmp_historical_valuation_dir: Path,
    eodhd_earnings_trends_dir: Path,
    data_sources: DataSourcesConfig | None = None,
    project_root: Path = PROJECT_ROOT,
) -> tuple[PitSnapshotManifestRecord, ...]:
    source_catalog = {
        source.source_id: source for source in data_sources.sources
    } if data_sources is not None else {}
    records: list[PitSnapshotManifestRecord] = []
    records.extend(
        _discover_raw_json_records(
            input_dir=fmp_analyst_history_dir,
            source_id="fmp_valuation_expectations",
            snapshot_kind="fmp_analyst_estimates",
            point_in_time_class="captured_snapshot",
            history_source_class="captured_snapshot_history",
            confidence_level="medium",
            confidence_reason=(
                "本地 forward-only analyst estimates raw cache；available_time "
                "使用 downloaded_at，不能回填为采集日前可见。"
            ),
            source_catalog=source_catalog,
            project_root=project_root,
        )
    )
    records.extend(
        _discover_raw_json_records(
            input_dir=fmp_historical_valuation_dir,
            source_id="fmp_valuation_expectations",
            snapshot_kind="fmp_historical_valuation",
            point_in_time_class="backfilled_history_distribution",
            history_source_class="vendor_historical_endpoint",
            confidence_level="low",
            confidence_reason=(
                "FMP historical endpoint 返回当前供应商历史视图；只用于采集日后 "
                "可见的分布参考，不是严格 PIT vendor archive。"
            ),
            source_catalog=source_catalog,
            project_root=project_root,
        )
    )
    records.extend(
        _discover_raw_json_records(
            input_dir=eodhd_earnings_trends_dir,
            source_id="eodhd_earnings_trends",
            snapshot_kind="eodhd_earnings_trends",
            point_in_time_class="captured_snapshot",
            history_source_class="vendor_current_trend",
            confidence_level="medium",
            confidence_reason=(
                "EODHD current trend raw cache；eps revision 只代表采集成功后 "
                "可见的供应商摘要，不是严格 PIT estimates archive。"
            ),
            source_catalog=source_catalog,
            project_root=project_root,
        )
    )
    return tuple(sorted(records, key=lambda record: record.snapshot_id))


def write_pit_snapshot_manifest(
    records: tuple[PitSnapshotManifestRecord, ...] | list[PitSnapshotManifestRecord],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [{column: asdict(record)[column] for column in PIT_SNAPSHOT_MANIFEST_COLUMNS}
         for record in records]
    )
    if frame.empty:
        frame = pd.DataFrame(columns=PIT_SNAPSHOT_MANIFEST_COLUMNS)
    frame.to_csv(output_path, index=False)
    return output_path


def render_pit_snapshot_validation_report(report: PitSnapshotValidationReport) -> str:
    lines = [
        "# PIT 快照归档质量报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Manifest：`{report.input_path}`",
        f"- 快照数量：{report.snapshot_count}",
        f"- 来源数量：{report.source_count}",
        f"- 原始记录数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- 生产影响：{report.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本报告只校验 forward-only PIT raw snapshot manifest、raw payload 和来源授权字段。",
        "- 通过校验不代表该数据已经进入评分；下游仍必须通过 "
        "`available_time <= decision_time` 查询。",
        "- 缺跑的历史日期不能事后补写为 strict PIT，只能作为缺口或降级项记录。",
        "",
        "## 来源摘要",
        "",
        "| Source | 类型 | 快照数 | Row count | PIT class | Backtest use |",
        "|---|---|---:|---:|---|---|",
    ]
    for source_id in sorted({record.source_id for record in report.records}):
        records = [record for record in report.records if record.source_id == source_id]
        source_type = records[0].source_type if records else ""
        pit_classes = ", ".join(sorted({record.point_in_time_class for record in records}))
        backtest_uses = ", ".join(sorted({record.backtest_use for record in records}))
        lines.append(
            "| "
            f"{source_id} | "
            f"{_escape_markdown_table(source_type)} | "
            f"{len(records)} | "
            f"{sum(record.row_count for record in records)} | "
            f"{_escape_markdown_table(pit_classes)} | "
            f"{_escape_markdown_table(backtest_uses)} |"
        )

    lines.extend(
        [
            "",
            "## 快照样例",
            "",
            "| Snapshot | Source | Ticker | Available time | Row count | Checksum | Payload |",
            "|---|---|---|---|---:|---|---|",
        ]
    )
    for record in sorted(report.records, key=lambda item: item.snapshot_id)[:25]:
        lines.append(
            "| "
            f"{record.snapshot_id} | "
            f"{record.source_id} | "
            f"{_escape_markdown_table(record.canonical_ticker)} | "
            f"{record.available_time} | "
            f"{record.row_count} | "
            f"`{record.raw_payload_sha256[:12]}` | "
            f"`{_escape_markdown_table(record.raw_payload_path)}` |"
        )
    if report.snapshot_count > 25:
        lines.append(
            f"| ... | ... | ... | ... | ... | ... | 仅展示前 25 条，共 {report.snapshot_count} 条 |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Snapshot | Source | Row | Path | 说明 |",
                      "|---|---|---|---|---:|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.snapshot_id or ''} | "
                f"{issue.source_id or ''} | "
                f"{'' if issue.row_number is None else issue.row_number} | "
                f"`{issue.path or ''}` | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def write_pit_snapshot_validation_report(
    report: PitSnapshotValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_pit_snapshot_validation_report(report), encoding="utf-8")
    return output_path


def default_pit_snapshot_validation_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"pit_snapshots_validation_{as_of.isoformat()}.md"


def _discover_raw_json_records(
    *,
    input_dir: Path,
    source_id: str,
    snapshot_kind: str,
    point_in_time_class: str,
    history_source_class: str,
    confidence_level: str,
    confidence_reason: str,
    source_catalog: dict[str, DataSourceConfig],
    project_root: Path,
) -> tuple[PitSnapshotManifestRecord, ...]:
    if not input_dir.exists():
        return tuple()
    source = source_catalog.get(source_id)
    records: list[PitSnapshotManifestRecord] = []
    for path in sorted(input_dir.rglob("*.json")):
        raw = _read_json_payload(path)
        if not isinstance(raw, dict):
            continue
        downloaded_at = str(raw.get("downloaded_at") or _mtime_iso(path))
        ticker = _canonical_ticker(raw)
        provider_symbol = _provider_symbol(raw)
        row_count = _raw_payload_row_count(raw)
        checksum = _file_sha256(path)
        request_params = _request_params(raw)
        records.append(
            PitSnapshotManifestRecord(
                snapshot_id=_snapshot_id(snapshot_kind, ticker, downloaded_at, path, checksum),
                source_id=source_id,
                source_name=str(raw.get("provider") or (source.provider if source else "")),
                source_type=str(raw.get("source_type") or (source.source_type if source else "")),
                source_quality_tier=str(
                    raw.get("source_type") or (source.source_type if source else "unknown")
                ),
                endpoint=_endpoint(raw),
                request_params=_json_dumps(request_params),
                provider_symbol=provider_symbol,
                canonical_ticker=ticker,
                provider_symbol_alias=(
                    f"{ticker}->{provider_symbol}"
                    if ticker and provider_symbol and ticker != provider_symbol
                    else "none"
                ),
                http_status="not_recorded",
                content_type="application/json",
                response_headers="not_recorded",
                raw_payload_path=_display_path(path, project_root),
                raw_payload_sha256=checksum,
                raw_payload_bytes=path.stat().st_size,
                snapshot_time=downloaded_at,
                ingested_at=downloaded_at,
                vendor_timestamp=str(raw.get("vendor_timestamp") or "not_recorded"),
                available_time=downloaded_at,
                row_count=row_count,
                parser_version=PIT_SNAPSHOT_PARSER_VERSION,
                schema_version=PIT_SNAPSHOT_SCHEMA_VERSION,
                license_use_class=(
                    source.llm_permission.license_scope if source is not None else "unknown"
                ),
                redistribution_allowed=(
                    source.llm_permission.redistribution_allowed if source is not None else False
                ),
                llm_processing_allowed=(
                    source.llm_permission.external_llm_allowed if source is not None else False
                ),
                point_in_time_class=point_in_time_class,
                history_source_class=history_source_class,
                backtest_use="captured_at_forward_only",
                confidence_level=confidence_level,
                confidence_reason=confidence_reason,
                validation_status="PASS",
                validation_report_path="not_recorded",
            )
        )
    return tuple(records)


def _record_from_manifest_row(row: pd.Series) -> PitSnapshotManifestRecord:
    return PitSnapshotManifestRecord(
        snapshot_id=_cell(row, "snapshot_id"),
        source_id=_cell(row, "source_id"),
        source_name=_cell(row, "source_name"),
        source_type=_cell(row, "source_type"),
        source_quality_tier=_cell(row, "source_quality_tier"),
        endpoint=_cell(row, "endpoint"),
        request_params=_cell(row, "request_params"),
        provider_symbol=_cell(row, "provider_symbol"),
        canonical_ticker=_cell(row, "canonical_ticker"),
        provider_symbol_alias=_cell(row, "provider_symbol_alias"),
        http_status=_cell(row, "http_status"),
        content_type=_cell(row, "content_type"),
        response_headers=_cell(row, "response_headers"),
        raw_payload_path=_cell(row, "raw_payload_path"),
        raw_payload_sha256=_cell(row, "raw_payload_sha256"),
        raw_payload_bytes=_int_cell(row, "raw_payload_bytes"),
        snapshot_time=_cell(row, "snapshot_time"),
        ingested_at=_cell(row, "ingested_at"),
        vendor_timestamp=_cell(row, "vendor_timestamp"),
        available_time=_cell(row, "available_time"),
        row_count=_int_cell(row, "row_count"),
        parser_version=_cell(row, "parser_version"),
        schema_version=_cell(row, "schema_version"),
        license_use_class=_cell(row, "license_use_class"),
        redistribution_allowed=_bool_cell(row, "redistribution_allowed"),
        llm_processing_allowed=_bool_cell(row, "llm_processing_allowed"),
        point_in_time_class=_cell(row, "point_in_time_class"),
        history_source_class=_cell(row, "history_source_class"),
        backtest_use=_cell(row, "backtest_use"),
        confidence_level=_cell(row, "confidence_level"),
        confidence_reason=_cell(row, "confidence_reason"),
        validation_status=_cell(row, "validation_status"),
        validation_report_path=_cell(row, "validation_report_path"),
    )


def _validate_required_values(
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    raw = asdict(record)
    for column in REQUIRED_VALUE_COLUMNS:
        value = raw[column]
        if value is None or str(value).strip() == "":
            issues.append(
                PitSnapshotValidationIssue(
                    severity=PitSnapshotIssueSeverity.ERROR,
                    code="pit_snapshot_required_value_missing",
                    message=f"manifest 字段 `{column}` 缺少值。",
                    snapshot_id=record.snapshot_id or None,
                    source_id=record.source_id or None,
                    row_number=row_number,
                )
            )


def _validate_record_enums(
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    _check_allowed(
        record.source_type,
        PIT_SOURCE_TYPES,
        "source_type",
        record,
        issues,
        row_number,
    )
    _check_allowed(
        record.point_in_time_class,
        PIT_POINT_IN_TIME_CLASSES,
        "point_in_time_class",
        record,
        issues,
        row_number,
    )
    _check_allowed(
        record.history_source_class,
        PIT_HISTORY_SOURCE_CLASSES,
        "history_source_class",
        record,
        issues,
        row_number,
    )
    _check_allowed(
        record.backtest_use,
        PIT_BACKTEST_USES,
        "backtest_use",
        record,
        issues,
        row_number,
    )
    _check_allowed(
        record.confidence_level,
        PIT_CONFIDENCE_LEVELS,
        "confidence_level",
        record,
        issues,
        row_number,
    )
    _check_allowed(
        record.validation_status,
        PIT_VALIDATION_STATUSES,
        "validation_status",
        record,
        issues,
        row_number,
    )
    if record.validation_status == "FAIL":
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_row_validation_failed",
                message="manifest 记录声明 validation_status=FAIL，下游不得使用。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if record.backtest_use == "strict_point_in_time" and (
        record.point_in_time_class != "true_point_in_time"
    ):
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="strict_backtest_without_true_pit",
                message="只有 true_point_in_time 快照才能声明 strict_point_in_time。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if record.backtest_use == "strict_point_in_time" and record.confidence_level == "low":
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="low_confidence_strict_pit_snapshot",
                message="低可信 PIT 快照不能声明 strict_point_in_time。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )


def _validate_record_json_fields(
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    _check_json_field(record.request_params, "request_params", record, issues, row_number)
    if record.response_headers and record.response_headers != "not_recorded":
        _check_json_field(record.response_headers, "response_headers", record, issues, row_number)


def _validate_record_dates(
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    snapshot_time = _parse_datetime(record.snapshot_time)
    ingested_at = _parse_datetime(record.ingested_at)
    available_time = _parse_datetime(record.available_time)
    if snapshot_time is None:
        _date_issue("snapshot_time", record, issues, row_number)
    if ingested_at is None:
        _date_issue("ingested_at", record, issues, row_number)
    if available_time is None:
        _date_issue("available_time", record, issues, row_number)
    if ingested_at is None or available_time is None:
        return
    if available_time > ingested_at:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="available_time_after_ingested_at",
                message="available_time 晚于 ingested_at，不能作为已可见 PIT 输入。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if (
        available_time < ingested_at
        and record.point_in_time_class != "true_point_in_time"
        and record.history_source_class != "vendor_archive"
    ):
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="available_time_before_ingested_without_archive",
                message=(
                    "available_time 早于 ingested_at，但快照不是 true_point_in_time "
                    "或 vendor_archive，不能提前声明历史可见性。"
                ),
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )


def _validate_payload(
    record: PitSnapshotManifestRecord,
    *,
    input_path: Path,
    project_root: Path,
    issues: list[PitSnapshotValidationIssue],
) -> None:
    payload_path = _resolve_payload_path(record.raw_payload_path, input_path, project_root)
    if not payload_path.exists():
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_raw_payload_missing",
                message=f"raw payload 不存在：{payload_path}",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                path=payload_path,
            )
        )
        return
    actual_bytes = payload_path.stat().st_size
    if actual_bytes != record.raw_payload_bytes:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_raw_payload_bytes_mismatch",
                message=(
                    f"raw_payload_bytes={record.raw_payload_bytes} 与实际 "
                    f"{actual_bytes} 不一致。"
                ),
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                path=payload_path,
            )
        )
    actual_checksum = _file_sha256(payload_path)
    if actual_checksum != record.raw_payload_sha256:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_raw_payload_checksum_mismatch",
                message="raw_payload_sha256 与当前文件 checksum 不一致。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                path=payload_path,
            )
        )
    if payload_path.suffix.lower() != ".json":
        return
    raw = _read_json_payload(payload_path)
    if raw is None:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_raw_payload_json_unreadable",
                message="JSON raw payload 无法读取。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                path=payload_path,
            )
        )
        return
    payload_row_count = _raw_payload_row_count(raw)
    if payload_row_count != record.row_count:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_row_count_mismatch",
                message=(
                    f"manifest row_count={record.row_count} 与 raw payload "
                    f"row_count={payload_row_count} 不一致。"
                ),
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                path=payload_path,
            )
        )


def _validate_source_catalog(
    record: PitSnapshotManifestRecord,
    source_catalog: dict[str, DataSourceConfig],
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    if not source_catalog:
        return
    source = source_catalog.get(record.source_id)
    if source is None:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.WARNING,
                code="pit_snapshot_source_not_in_catalog",
                message="source_id 未在 config/data_sources.yaml 中登记。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
        return
    if source.provider != record.source_name:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.WARNING,
                code="pit_snapshot_provider_mismatch",
                message=(
                    f"manifest source_name `{record.source_name}` 与数据源目录 "
                    f"`{source.provider}` 不一致。"
                ),
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if source.source_type != record.source_type:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.WARNING,
                code="pit_snapshot_source_type_mismatch",
                message=(
                    f"manifest source_type `{record.source_type}` 与数据源目录 "
                    f"`{source.source_type}` 不一致。"
                ),
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if record.redistribution_allowed and not source.llm_permission.redistribution_allowed:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_redistribution_permission_overstated",
                message="manifest 声明允许再分发，但数据源目录未批准。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )
    if record.llm_processing_allowed and not source.llm_permission.external_llm_allowed:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code="pit_snapshot_llm_permission_overstated",
                message="manifest 声明允许外部 LLM 处理，但数据源目录未批准。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )


def _check_allowed(
    value: str,
    allowed: set[str],
    column: str,
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    if value not in allowed:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code=f"invalid_pit_snapshot_{column}",
                message=f"`{column}` 值 `{value}` 不在允许范围内。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )


def _check_json_field(
    value: str,
    column: str,
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    try:
        json.loads(value)
    except json.JSONDecodeError:
        issues.append(
            PitSnapshotValidationIssue(
                severity=PitSnapshotIssueSeverity.ERROR,
                code=f"invalid_pit_snapshot_{column}_json",
                message=f"`{column}` 不是有效 JSON。",
                snapshot_id=record.snapshot_id,
                source_id=record.source_id,
                row_number=row_number,
            )
        )


def _date_issue(
    column: str,
    record: PitSnapshotManifestRecord,
    issues: list[PitSnapshotValidationIssue],
    row_number: int,
) -> None:
    issues.append(
        PitSnapshotValidationIssue(
            severity=PitSnapshotIssueSeverity.ERROR,
            code=f"invalid_pit_snapshot_{column}",
            message=f"`{column}` 不是有效 ISO datetime。",
            snapshot_id=record.snapshot_id,
            source_id=record.source_id,
            row_number=row_number,
        )
    )


def _resolve_payload_path(value: str, input_path: Path, project_root: Path) -> Path:
    raw_path = Path(value)
    if raw_path.is_absolute():
        return raw_path
    manifest_relative = input_path.parent / raw_path
    if manifest_relative.exists():
        return manifest_relative
    return project_root / raw_path


def _display_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _cell(row: pd.Series, column: str) -> str:
    return str(row.get(column, "")).strip()


def _int_cell(row: pd.Series, column: str) -> int:
    value = _cell(row, column)
    if not value:
        return 0
    try:
        return int(float(value))
    except ValueError:
        return 0


def _bool_cell(row: pd.Series, column: str) -> bool:
    return str(row.get(column, "")).strip().lower() in {"true", "1", "yes", "y"}


def _parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _read_json_payload(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _raw_payload_row_count(raw: Any) -> int:
    if not isinstance(raw, dict):
        return 0
    row_count = raw.get("row_count")
    if isinstance(row_count, int):
        return row_count
    if isinstance(row_count, float):
        return int(row_count)
    records = raw.get("records")
    if isinstance(records, list):
        return len(records)
    records_by_endpoint = raw.get("records_by_endpoint")
    if isinstance(records_by_endpoint, dict):
        return sum(len(value) for value in records_by_endpoint.values() if isinstance(value, list))
    records_by_ticker = raw.get("records_by_ticker")
    if isinstance(records_by_ticker, dict):
        return sum(len(value) for value in records_by_ticker.values() if isinstance(value, list))
    return 0


def _canonical_ticker(raw: dict[str, Any]) -> str:
    ticker = raw.get("ticker")
    if isinstance(ticker, str) and ticker.strip():
        return ticker.strip().upper()
    requested = raw.get("requested_tickers")
    if isinstance(requested, list):
        return ",".join(str(item).upper() for item in requested)
    return "UNKNOWN"


def _provider_symbol(raw: dict[str, Any]) -> str:
    request_parameters = raw.get("request_parameters")
    if isinstance(request_parameters, dict) and request_parameters.get("symbol"):
        return str(request_parameters["symbol"]).upper()
    by_endpoint = raw.get("request_parameters_by_endpoint")
    if isinstance(by_endpoint, dict):
        symbols = {
            str(params.get("symbol")).upper()
            for params in by_endpoint.values()
            if isinstance(params, dict) and params.get("symbol")
        }
        if symbols:
            return ",".join(sorted(symbols))
    provider_symbols = raw.get("provider_symbols")
    if isinstance(provider_symbols, list):
        return ",".join(str(item).upper() for item in provider_symbols)
    return _canonical_ticker(raw)


def _request_params(raw: dict[str, Any]) -> Any:
    if isinstance(raw.get("request_parameters"), dict):
        return raw["request_parameters"]
    if isinstance(raw.get("request_parameters_by_endpoint"), dict):
        return raw["request_parameters_by_endpoint"]
    return {}


def _endpoint(raw: dict[str, Any]) -> str:
    endpoint = raw.get("endpoint")
    if isinstance(endpoint, str) and endpoint.strip():
        return endpoint.strip()
    endpoints = raw.get("endpoints")
    if isinstance(endpoints, list):
        return ",".join(str(item) for item in endpoints)
    return "not_recorded"


def _snapshot_id(
    snapshot_kind: str,
    ticker: str,
    downloaded_at: str,
    path: Path,
    checksum: str,
) -> str:
    parsed = _parse_datetime(downloaded_at)
    time_token = parsed.strftime("%Y_%m_%dT%H_%M_%S") if parsed is not None else path.stem
    return _id_token(f"{snapshot_kind}_{ticker}_{time_token}_{checksum[:12]}")


def _id_token(value: str) -> str:
    token = "".join(character.lower() if character.isalnum() else "_" for character in value)
    return "_".join(part for part in token.split("_") if part) or "unknown"


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _severity_label(severity: PitSnapshotIssueSeverity) -> str:
    return "错误" if severity == PitSnapshotIssueSeverity.ERROR else "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
