from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.trading_engine.market_data_refresh import (
    REFRESH_BLOCKED,
    REFRESH_FAILED,
    REFRESH_NOT_NEEDED,
    REFRESH_OK,
    REFRESH_PARTIAL,
    REFRESH_PLANNED,
    REFRESH_RUNNING,
    REFRESH_SOURCE_DELAYED,
    latest_market_data_refresh_path_on_or_before,
    load_market_data_refresh_payload,
    market_data_refresh_payload_date,
)

DATA_REFRESH_AUDIT_SCHEMA_VERSION = 1
DATA_REFRESH_AUDIT_REPORT_TYPE = "data_refresh_audit"
DATA_REFRESH_AUDIT_VALIDATION_REPORT_TYPE = "data_refresh_audit_validation"
DATA_REFRESH_AUDIT_RECORD_TYPE = "data_refresh_audit_record"
DATA_REFRESH_AUDIT_POLICY_VERSION = "data_refresh_audit_policy_v1"
PRODUCTION_EFFECT = "none"

DEFAULT_DATA_REFRESH_AUDIT_DIR = (
    PROJECT_ROOT / "reports" / "data_governance" / "data_refresh_audit"
)
DEFAULT_VALIDATION_AUDIT_DIR = PROJECT_ROOT / "artifacts" / "data_refresh_audit" / "validation"
LATEST_POINTER_NAME = "latest_data_refresh_audit.json"
LATEST_VALIDATE_DATA_AUDIT_POINTER_NAME = "latest_validate_data_audit.json"

AUDIT_STATUS_SUCCESS = "SUCCESS"
AUDIT_STATUS_SUCCESS_WITH_WARNINGS = "SUCCESS_WITH_WARNINGS"
AUDIT_STATUS_FAILED = "FAILED"
AUDIT_STATUS_SKIPPED_MARKET_CLOSED = "SKIPPED_MARKET_CLOSED"
AUDIT_STATUS_SKIPPED_NO_NEW_DATA = "SKIPPED_NO_NEW_DATA"
DATA_REFRESH_AUDIT_RECORD_STATUSES = frozenset(
    {
        AUDIT_STATUS_SUCCESS,
        AUDIT_STATUS_SUCCESS_WITH_WARNINGS,
        AUDIT_STATUS_FAILED,
        AUDIT_STATUS_SKIPPED_MARKET_CLOSED,
        AUDIT_STATUS_SKIPPED_NO_NEW_DATA,
    }
)
DATA_REFRESH_AUDIT_REPORT_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS", "FAIL"})
DATA_REFRESH_AUDIT_REQUIRED_RECORD_FIELDS = (
    "audit_record_id",
    "attempt_type",
    "data_type",
    "source",
    "start_time",
    "end_time",
    "as_of",
    "status",
    "checksum",
    "record_count",
    "warning_count",
    "error_count",
)

DATA_REFRESH_AUDIT_POLICY = {
    "policy_version": DATA_REFRESH_AUDIT_POLICY_VERSION,
    "owner": "system",
    "status": "pilot_baseline",
    "rationale": (
        "Expose refresh and validation evidence used by paper-shadow workflows "
        "without re-running refreshes or weakening the validate-data gate."
    ),
    "intended_effect": (
        "Make refresh attempts, skip reasons, cache checksum, row count and "
        "validation warning/error counts visible before data-dependent "
        "paper-shadow conclusions are interpreted."
    ),
    "validation_evidence": (
        "Focused tests cover validate-data sidecar writing, audit report "
        "generation, invalid status validation and Reader Brief summary."
    ),
    "review_condition": (
        "Review before audit status is used as a promotion gate or before "
        "additional data refresh providers are added."
    ),
}

DATA_REFRESH_AUDIT_SAFETY = {
    "read_only": True,
    "data_refresh_allowed": False,
    "data_downloaded_by_audit": False,
    "pipelines_executed_by_audit": False,
    "cache_fabricated": False,
    "score_or_backtest_allowed": False,
    "broker_action_allowed": False,
    "trading_action_allowed": False,
    "production_effect": PRODUCTION_EFFECT,
    "boundary": (
        "Data refresh audit report only; reads existing refresh and validation "
        "artifacts, does not refresh data, mutate cache, run scoring/backtests, "
        "or create broker/order actions."
    ),
}


class DataRefreshAuditIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class DataRefreshAuditIssue:
    severity: DataRefreshAuditIssueSeverity
    code: str
    message: str
    record_id: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class DataRefreshAuditValidationReport:
    as_of: date
    generated_at: datetime
    audit_id: str
    audit_path: Path
    audit_record_count: int
    issues: tuple[DataRefreshAuditIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == DataRefreshAuditIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == DataRefreshAuditIssueSeverity.WARNING
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


def write_validate_data_audit_sidecar(
    *,
    report: DataQualityReport,
    report_path: Path,
    started_at: datetime,
    ended_at: datetime,
    output_dir: Path = DEFAULT_VALIDATION_AUDIT_DIR,
) -> Path:
    record = build_validate_data_audit_record(
        report=report,
        report_path=report_path,
        started_at=started_at,
        ended_at=ended_at,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    record_path = (
        output_dir / f"validate_data_{report.as_of.isoformat()}_{_hash_record(record)}.json"
    )
    _write_json(record_path, record)
    _write_json(
        output_dir / LATEST_VALIDATE_DATA_AUDIT_POINTER_NAME,
        {
            "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
            "record_type": DATA_REFRESH_AUDIT_RECORD_TYPE,
            "audit_record_id": record["audit_record_id"],
            "record_path": str(record_path),
            "as_of": record["as_of"],
            "status": record["status"],
            "production_effect": PRODUCTION_EFFECT,
        },
    )
    return record_path


def build_validate_data_audit_record(
    *,
    report: DataQualityReport,
    report_path: Path,
    started_at: datetime,
    ended_at: datetime,
) -> dict[str, Any]:
    summaries = _data_quality_summaries(report)
    checksum = _combined_checksum(
        [
            _text(summary.get("sha256"))
            for summary in summaries.values()
            if _text(summary.get("sha256"))
        ]
    )
    row_counts = {
        name: int(summary.get("rows") or 0)
        for name, summary in summaries.items()
    }
    record_count = sum(
        row_counts[name]
        for name in ("price_data", "secondary_price_data", "macro_rate_data")
        if name in row_counts
    )
    status = _data_quality_status_to_audit_status(report.status)
    record = {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "record_type": DATA_REFRESH_AUDIT_RECORD_TYPE,
        "audit_record_id": "",
        "attempt_type": "DATA_VALIDATION",
        "data_type": "cached_market_macro_data",
        "source": "aits validate-data",
        "start_time": _iso_datetime(started_at),
        "end_time": _iso_datetime(ended_at),
        "as_of": report.as_of.isoformat(),
        "status": status,
        "raw_status": report.status,
        "checksum": checksum,
        "input_checksums": {
            name: _text(summary.get("sha256"))
            for name, summary in summaries.items()
            if _text(summary.get("sha256"))
        },
        "record_count": record_count,
        "row_counts": row_counts,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "info_count": report.info_count,
        "report_path": str(report_path),
        "source_artifacts": [str(report_path)],
        "reason": _validation_reason(report),
        "quality_gate": {
            "command": "aits validate-data",
            "passed": report.passed,
            "data_quality_status": report.status,
            "error_count": report.error_count,
            "warning_count": report.warning_count,
            "info_count": report.info_count,
        },
        "file_summaries": summaries,
        "production_effect": PRODUCTION_EFFECT,
        "safety_boundary": DATA_REFRESH_AUDIT_SAFETY,
    }
    record["audit_record_id"] = _audit_record_id(record)
    return record


def build_data_refresh_audit_payload(
    *,
    as_of: date,
    validation_audit_dir: Path = DEFAULT_VALIDATION_AUDIT_DIR,
    market_refresh_root: Path | None = None,
    price_cache_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    build_issues: list[DataRefreshAuditIssue] = []
    records: list[dict[str, Any]] = []

    validation_record = latest_validate_data_audit_record_on_or_before(
        as_of,
        validation_audit_dir=validation_audit_dir,
    )
    if validation_record is None:
        records.append(_missing_validation_record(as_of=as_of, generated_at=generated))
        build_issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="validate_data_audit_sidecar_missing",
                message=(
                    "没有找到 as_of 当日或之前的 validate-data audit sidecar；"
                    "数据依赖 workflow 不能把 quality gate 视为已通过。"
                ),
                record_id="validate_data_missing",
            )
        )
    else:
        records.append(validation_record)

    market_record, market_issue = _market_refresh_record(
        as_of=as_of,
        generated_at=generated,
        market_refresh_root=market_refresh_root,
        price_cache_path=price_cache_path,
    )
    records.append(market_record)
    if market_issue is not None:
        build_issues.append(market_issue)

    summary = _summary(records=records, build_issues=build_issues)
    audit_id = _audit_id(as_of=as_of, generated_at=generated, records=records)
    payload: dict[str, Any] = {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "report_type": DATA_REFRESH_AUDIT_REPORT_TYPE,
        "audit_id": audit_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": summary["status"],
        "validation_status": summary["status"],
        "production_effect": PRODUCTION_EFFECT,
        "policy": DATA_REFRESH_AUDIT_POLICY,
        "safety_boundary": DATA_REFRESH_AUDIT_SAFETY,
        "summary": summary,
        "records": records,
        "build_issues": [_issue_dict(issue) for issue in build_issues],
        "methodology": {
            "record_statuses": sorted(DATA_REFRESH_AUDIT_RECORD_STATUSES),
            "required_record_fields": list(DATA_REFRESH_AUDIT_REQUIRED_RECORD_FIELDS),
            "status_policy": (
                "FAIL if any refresh/validation record fails; PASS_WITH_WARNINGS "
                "when warnings or trading-day SKIPPED_NO_NEW_DATA are present; "
                "otherwise PASS."
            ),
        },
    }
    return _with_validation_summary(payload, audit_path=Path(""))


def write_data_refresh_audit_artifact(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
) -> dict[str, Path]:
    audit_id = _text(payload.get("audit_id"))
    if not audit_id:
        raise ValueError("data refresh audit payload missing audit_id")
    artifact_dir = output_dir / audit_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    audit_path = artifact_dir / "data_refresh_audit.json"
    markdown_path = artifact_dir / "data_refresh_audit.md"
    validation_json_path = artifact_dir / "data_refresh_audit_validation.json"
    validation_md_path = artifact_dir / "data_refresh_audit_validation.md"
    reader_brief_path = artifact_dir / "reader_brief_section.md"

    payload_with_paths = dict(payload)
    payload_with_paths["artifact_paths"] = {
        "artifact_dir": str(artifact_dir),
        "audit_json": str(audit_path),
        "audit_markdown": str(markdown_path),
        "validation_json": str(validation_json_path),
        "validation_markdown": str(validation_md_path),
        "reader_brief_section": str(reader_brief_path),
    }
    validation = validate_data_refresh_audit_payload(payload_with_paths, audit_path=audit_path)
    payload_with_paths = _with_validation_summary(
        payload_with_paths,
        audit_path=audit_path,
        validation=validation,
    )
    _write_json(audit_path, payload_with_paths)
    _write_text(markdown_path, render_data_refresh_audit_markdown(payload_with_paths))
    _write_json(validation_json_path, validation_report_to_payload(validation))
    _write_text(validation_md_path, render_data_refresh_audit_validation_markdown(validation))
    _write_text(reader_brief_path, render_data_refresh_audit_reader_brief(payload_with_paths))
    _write_latest_pointer(output_dir=output_dir, payload=payload_with_paths, audit_path=audit_path)
    return {
        "artifact_dir": artifact_dir,
        "audit_json": audit_path,
        "audit_markdown": markdown_path,
        "validation_json": validation_json_path,
        "validation_markdown": validation_md_path,
        "reader_brief_section": reader_brief_path,
    }


def build_and_write_data_refresh_audit(
    *,
    as_of: date,
    output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    validation_audit_dir: Path = DEFAULT_VALIDATION_AUDIT_DIR,
    market_refresh_root: Path | None = None,
    price_cache_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
) -> tuple[dict[str, Any], dict[str, Path]]:
    payload = build_data_refresh_audit_payload(
        as_of=as_of,
        validation_audit_dir=validation_audit_dir,
        market_refresh_root=market_refresh_root,
        price_cache_path=price_cache_path,
    )
    paths = write_data_refresh_audit_artifact(payload, output_dir=output_dir)
    return load_data_refresh_audit_payload(paths["audit_json"]), paths


def validate_data_refresh_audit_payload(
    payload: Mapping[str, Any],
    *,
    audit_path: Path,
) -> DataRefreshAuditValidationReport:
    issues = [_issue_from_mapping(item) for item in _records(payload.get("build_issues"))]
    as_of = _parse_date(_text(payload.get("as_of"))) or date.today()
    audit_id = _text(payload.get("audit_id"), "UNKNOWN")
    records = _records(payload.get("records"))

    _check_top_level_contract(payload, issues)
    _check_safety_boundary(payload, issues)
    _check_records(records, issues)
    return DataRefreshAuditValidationReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        audit_id=audit_id,
        audit_path=audit_path,
        audit_record_count=len(records),
        issues=tuple(issues),
    )


def validate_data_refresh_audit_artifact(
    *,
    audit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
) -> tuple[DataRefreshAuditValidationReport, Path]:
    audit_path = resolve_data_refresh_audit_path(
        audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    payload = load_data_refresh_audit_payload(audit_path)
    validation = validate_data_refresh_audit_payload(payload, audit_path=audit_path)
    artifact_dir = audit_path.parent
    _write_json(
        artifact_dir / "data_refresh_audit_validation.json",
        validation_report_to_payload(validation),
    )
    _write_text(
        artifact_dir / "data_refresh_audit_validation.md",
        render_data_refresh_audit_validation_markdown(validation),
    )
    updated = _with_validation_summary(payload, audit_path=audit_path, validation=validation)
    _write_json(audit_path, updated)
    return validation, audit_path


def resolve_data_refresh_audit_path(
    *,
    audit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
) -> Path:
    if audit_id:
        candidate = output_dir / audit_id / "data_refresh_audit.json"
        if not candidate.exists():
            raise FileNotFoundError(f"Data refresh audit not found: {candidate}")
        return candidate
    if latest:
        latest_path = _latest_audit_from_pointer(output_dir)
        if latest_path is not None:
            return latest_path
    candidates = sorted(
        output_dir.glob("*/data_refresh_audit.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No data refresh audit artifacts found under {output_dir}")
    return candidates[0]


def load_data_refresh_audit_payload(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Data refresh audit must be a JSON object: {path}")
    return raw


def latest_validate_data_audit_record_on_or_before(
    as_of: date,
    *,
    validation_audit_dir: Path = DEFAULT_VALIDATION_AUDIT_DIR,
) -> dict[str, Any] | None:
    candidates: list[tuple[date, str, Path, dict[str, Any]]] = []
    for path in validation_audit_dir.glob("validate_data_*.json"):
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        if _text(payload.get("record_type")) != DATA_REFRESH_AUDIT_RECORD_TYPE:
            continue
        record_date = _parse_date(_text(payload.get("as_of")))
        if record_date is None or record_date > as_of:
            continue
        candidates.append((record_date, _text(payload.get("end_time")), path, payload))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1], item[2].stat().st_mtime), reverse=True)
    return candidates[0][3]


def validation_report_to_payload(report: DataRefreshAuditValidationReport) -> dict[str, Any]:
    return {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "report_type": DATA_REFRESH_AUDIT_VALIDATION_REPORT_TYPE,
        "audit_id": report.audit_id,
        "audit_path": str(report.audit_path),
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "audit_record_count": report.audit_record_count,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "production_effect": PRODUCTION_EFFECT,
        "issues": [_issue_dict(issue) for issue in report.issues],
    }


def render_data_refresh_audit_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    paths = _mapping(payload.get("artifact_paths"))
    records = _records(payload.get("records"))
    lines = [
        "# Data Refresh Audit",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        f"- Audit ID：`{_text(payload.get('audit_id'), 'UNKNOWN')}`",
        f"- 评估日期：{_text(payload.get('as_of'), 'UNKNOWN')}",
        f"- 生成时间：{_text(payload.get('generated_at'), 'UNKNOWN')}",
        f"- Audit record count：{_text(summary.get('audit_record_count'), '0')}",
        f"- Failed record count：{_text(summary.get('failed_record_count'), '0')}",
        f"- Skipped record count：{_text(summary.get('skipped_record_count'), '0')}",
        f"- Warning count：{_text(summary.get('warning_count'), '0')}",
        f"- Error count：{_text(summary.get('error_count'), '0')}",
        f"- Next action：{_text(summary.get('next_action'), 'UNKNOWN')}",
        f"- Production effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- Validation report：`{_text(paths.get('validation_markdown'), 'UNKNOWN')}`",
        "",
        "## 治理边界",
        "",
        _text(_mapping(payload.get("safety_boundary")).get("boundary")),
        "",
        "## Audit Records",
        "",
        "| Attempt | Data type | Source | As of | Status | Rows | Warnings | Errors | Checksum |",
        "|---|---|---|---|---|---:|---:|---:|---|",
    ]
    for record in records:
        lines.append(
            "| "
            f"{_escape_table(_text(record.get('attempt_type')))} | "
            f"{_escape_table(_text(record.get('data_type')))} | "
            f"{_escape_table(_text(record.get('source')))} | "
            f"{_escape_table(_text(record.get('as_of')))} | "
            f"{_escape_table(_text(record.get('status')))} | "
            f"{_int(record.get('record_count'))} | "
            f"{_int(record.get('warning_count'))} | "
            f"{_int(record.get('error_count'))} | "
            f"{_checksum_prefix(_text(record.get('checksum')))} |"
        )
    issues = _records(payload.get("validation_issues"))
    lines.extend(["", "## 校验问题", ""])
    if not issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| Severity | Code | Record | Field | Message |", "|---|---|---|---|---|"])
        for issue in issues:
            lines.append(
                "| "
                f"{_escape_table(_text(issue.get('severity')))} | "
                f"{_escape_table(_text(issue.get('code')))} | "
                f"{_escape_table(_text(issue.get('record_id')))} | "
                f"{_escape_table(_text(issue.get('field')))} | "
                f"{_escape_table(_text(issue.get('message')))} |"
            )
    return "\n".join(lines) + "\n"


def render_data_refresh_audit_validation_markdown(
    report: DataRefreshAuditValidationReport,
) -> str:
    lines = [
        "# Data Refresh Audit Validation",
        "",
        f"- 状态：{report.status}",
        f"- Audit ID：`{report.audit_id}`",
        f"- Audit path：`{report.audit_path}`",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Audit record count：{report.audit_record_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- Production effect：{PRODUCTION_EFFECT}",
        "",
        "## Issues",
        "",
    ]
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| Severity | Code | Record | Field | Message |", "|---|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"{_escape_table(issue.code)} | "
                f"{_escape_table(issue.record_id or '')} | "
                f"{_escape_table(issue.field or '')} | "
                f"{_escape_table(issue.message)} |"
            )
    return "\n".join(lines) + "\n"


def render_data_refresh_audit_reader_brief(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "## Data Refresh Audit",
            "",
            "- availability: AVAILABLE",
            f"- status: {_text(payload.get('status'), 'UNKNOWN')}",
            f"- audit_id: `{_text(payload.get('audit_id'), 'UNKNOWN')}`",
            f"- as_of: {_text(payload.get('as_of'), 'UNKNOWN')}",
            f"- audit_record_count: {_text(summary.get('audit_record_count'), '0')}",
            f"- failed_record_count: {_text(summary.get('failed_record_count'), '0')}",
            f"- skipped_record_count: {_text(summary.get('skipped_record_count'), '0')}",
            f"- warning_count: {_text(summary.get('warning_count'), '0')}",
            f"- error_count: {_text(summary.get('error_count'), '0')}",
            f"- next_action: {_text(summary.get('next_action'), 'UNKNOWN')}",
            f"- production_effect: {_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
            "",
        ]
    )


def _market_refresh_record(
    *,
    as_of: date,
    generated_at: datetime,
    market_refresh_root: Path | None,
    price_cache_path: Path,
) -> tuple[dict[str, Any], DataRefreshAuditIssue | None]:
    refresh_path = latest_market_data_refresh_path_on_or_before(
        as_of,
        output_root=market_refresh_root,
    )
    if refresh_path is None:
        return _missing_market_refresh_record(
            as_of=as_of,
            generated_at=generated_at,
            price_cache_path=price_cache_path,
        )
    payload = load_market_data_refresh_payload(refresh_path)
    metadata = _mapping(payload.get("metadata"))
    actions = _mapping(payload.get("actions"))
    diagnostics = _mapping(payload.get("diagnostics"))
    remaining_limitations = _texts(payload.get("remaining_limitations"))
    raw_status = _text(metadata.get("status"), "UNKNOWN")
    status = _market_refresh_status_to_audit_status(raw_status, remaining_limitations)
    refresh_date = market_data_refresh_payload_date(payload, refresh_path)
    generated = _parse_datetime(_text(metadata.get("generated_at"))) or generated_at
    asset_results = _records(payload.get("asset_results"))
    row_count = _csv_row_count(price_cache_path)
    checksum = (
        _text(diagnostics.get("price_cache_after_sha256"))
        or _file_sha256(price_cache_path)
        or _text(diagnostics.get("price_cache_before_sha256"))
    )
    error_count = _market_refresh_error_count(raw_status, asset_results)
    warning_count = _market_refresh_warning_count(
        raw_status=raw_status,
        asset_results=asset_results,
        remaining_limitations=remaining_limitations,
    )
    record = {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "record_type": DATA_REFRESH_AUDIT_RECORD_TYPE,
        "audit_record_id": "",
        "attempt_type": "MARKET_DATA_REFRESH",
        "data_type": "price_data",
        "source": _text(actions.get("source")) or _combined_asset_sources(asset_results),
        "start_time": generated.isoformat(),
        "end_time": generated.isoformat(),
        "as_of": refresh_date.isoformat(),
        "requested_as_of": as_of.isoformat(),
        "status": status,
        "raw_status": raw_status,
        "checksum": checksum,
        "input_checksums": {
            "price_cache_before_sha256": _text(diagnostics.get("price_cache_before_sha256")),
            "price_cache_after_sha256": _text(diagnostics.get("price_cache_after_sha256")),
        },
        "record_count": row_count,
        "row_counts": {"price_cache_rows": row_count},
        "warning_count": warning_count,
        "error_count": error_count,
        "source_artifacts": [str(refresh_path)],
        "report_path": str(refresh_path),
        "reason": _text(metadata.get("reason"), "market_data_refresh_summary_available"),
        "refresh_actions": {
            "target_date": _text(actions.get("target_date")),
            "fetched_assets": _texts(actions.get("fetched_assets")),
            "updated_price_cache": actions.get("updated_price_cache") is True,
            "refreshed_backtest_manifest": actions.get("refreshed_backtest_manifest") is True,
        },
        "remaining_limitations": remaining_limitations,
        "production_effect": PRODUCTION_EFFECT,
        "safety_boundary": DATA_REFRESH_AUDIT_SAFETY,
    }
    if not record["source"]:
        record["source"] = "market_data_refresh"
    record["audit_record_id"] = _audit_record_id(record)
    return record, None


def _missing_market_refresh_record(
    *,
    as_of: date,
    generated_at: datetime,
    price_cache_path: Path,
) -> tuple[dict[str, Any], DataRefreshAuditIssue | None]:
    session = us_equity_market_session(as_of)
    market_closed = not session.is_trading_day
    status = (
        AUDIT_STATUS_SKIPPED_MARKET_CLOSED
        if market_closed
        else AUDIT_STATUS_SKIPPED_NO_NEW_DATA
    )
    reason = (
        f"US equity market closed: {session.reason}"
        if market_closed
        else "No market_data_refresh_summary.json found on or before requested as_of."
    )
    warning_count = 0 if market_closed else 1
    issue = None
    if not market_closed:
        issue = DataRefreshAuditIssue(
            severity=DataRefreshAuditIssueSeverity.WARNING,
            code="market_refresh_summary_missing",
            message=(
                "请求日期为交易日但没有找到当日或之前的 market refresh summary；"
                "请确认是否已通过 documented refresh path 生成证据。"
            ),
            record_id="market_data_refresh_missing",
        )
    row_count = _csv_row_count(price_cache_path)
    checksum = _file_sha256(price_cache_path)
    record = {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "record_type": DATA_REFRESH_AUDIT_RECORD_TYPE,
        "audit_record_id": "",
        "attempt_type": "MARKET_DATA_REFRESH",
        "data_type": "price_data",
        "source": "market_calendar" if market_closed else "market_data_refresh_artifact_scan",
        "start_time": generated_at.isoformat(),
        "end_time": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "status": status,
        "raw_status": "NO_REFRESH_SUMMARY",
        "checksum": checksum,
        "input_checksums": {"price_cache_sha256": checksum},
        "record_count": row_count,
        "row_counts": {"price_cache_rows": row_count},
        "warning_count": warning_count,
        "error_count": 0,
        "source_artifacts": [],
        "report_path": "",
        "reason": reason,
        "market_session": {
            "market": session.market,
            "session_status": session.session_status,
            "session_kind": session.session_kind,
            "reason": session.reason,
            "previous_trading_day": session.previous_trading_day.isoformat(),
        },
        "production_effect": PRODUCTION_EFFECT,
        "safety_boundary": DATA_REFRESH_AUDIT_SAFETY,
    }
    record["audit_record_id"] = _audit_record_id(record)
    return record, issue


def _missing_validation_record(*, as_of: date, generated_at: datetime) -> dict[str, Any]:
    record = {
        "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
        "record_type": DATA_REFRESH_AUDIT_RECORD_TYPE,
        "audit_record_id": "",
        "attempt_type": "DATA_VALIDATION",
        "data_type": "cached_market_macro_data",
        "source": "aits validate-data",
        "start_time": generated_at.isoformat(),
        "end_time": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "status": AUDIT_STATUS_FAILED,
        "raw_status": "VALIDATION_AUDIT_SIDECAR_MISSING",
        "checksum": "",
        "input_checksums": {},
        "record_count": 0,
        "row_counts": {},
        "warning_count": 0,
        "error_count": 1,
        "source_artifacts": [],
        "report_path": "",
        "reason": "validate-data audit sidecar missing; quality gate cannot be treated as passed.",
        "production_effect": PRODUCTION_EFFECT,
        "safety_boundary": DATA_REFRESH_AUDIT_SAFETY,
    }
    record["audit_record_id"] = _audit_record_id(record)
    return record


def _summary(
    *,
    records: Sequence[Mapping[str, Any]],
    build_issues: Sequence[DataRefreshAuditIssue],
) -> dict[str, Any]:
    failed_count = sum(
        1 for record in records if _text(record.get("status")) == AUDIT_STATUS_FAILED
    )
    skipped_market_closed_count = sum(
        1 for record in records if _text(record.get("status")) == AUDIT_STATUS_SKIPPED_MARKET_CLOSED
    )
    skipped_no_new_data_count = sum(
        1 for record in records if _text(record.get("status")) == AUDIT_STATUS_SKIPPED_NO_NEW_DATA
    )
    warning_count = sum(_int(record.get("warning_count")) for record in records) + sum(
        1 for issue in build_issues if issue.severity == DataRefreshAuditIssueSeverity.WARNING
    )
    error_count = sum(_int(record.get("error_count")) for record in records) + sum(
        1 for issue in build_issues if issue.severity == DataRefreshAuditIssueSeverity.ERROR
    )
    if failed_count or error_count:
        status = "FAIL"
        next_action = "investigate_failed_refresh_or_validation_before_paper_shadow_use"
    elif warning_count or skipped_no_new_data_count:
        status = "PASS_WITH_WARNINGS"
        next_action = "review_refresh_skip_or_warning_evidence_before_interpretation"
    else:
        status = "PASS"
        next_action = "no_owner_action_required"
    return {
        "status": status,
        "audit_record_count": len(records),
        "failed_record_count": failed_count,
        "skipped_record_count": skipped_market_closed_count + skipped_no_new_data_count,
        "skipped_market_closed_count": skipped_market_closed_count,
        "skipped_no_new_data_count": skipped_no_new_data_count,
        "success_record_count": sum(
            1 for record in records if _text(record.get("status")) == AUDIT_STATUS_SUCCESS
        ),
        "success_with_warnings_record_count": sum(
            1
            for record in records
            if _text(record.get("status")) == AUDIT_STATUS_SUCCESS_WITH_WARNINGS
        ),
        "warning_count": warning_count,
        "error_count": error_count,
        "next_action": next_action,
    }


def _data_quality_summaries(report: DataQualityReport) -> dict[str, dict[str, Any]]:
    output = {
        "price_data": _file_summary_payload(report.price_summary),
        "macro_rate_data": _file_summary_payload(report.rate_summary),
    }
    if report.secondary_price_summary is not None:
        output["secondary_price_data"] = _file_summary_payload(report.secondary_price_summary)
    if report.manifest_summary is not None:
        output["download_manifest"] = _file_summary_payload(report.manifest_summary)
    return output


def _file_summary_payload(summary: DataFileSummary) -> dict[str, Any]:
    return {
        "path": str(summary.path),
        "exists": summary.exists,
        "rows": summary.rows,
        "sha256": summary.sha256 or "",
        "min_date": "" if summary.min_date is None else summary.min_date.isoformat(),
        "max_date": "" if summary.max_date is None else summary.max_date.isoformat(),
    }


def _data_quality_status_to_audit_status(status: str) -> str:
    if status == "PASS":
        return AUDIT_STATUS_SUCCESS
    if status == "PASS_WITH_WARNINGS":
        return AUDIT_STATUS_SUCCESS_WITH_WARNINGS
    return AUDIT_STATUS_FAILED


def _market_refresh_status_to_audit_status(
    status: str,
    remaining_limitations: Sequence[str],
) -> str:
    if status == REFRESH_OK:
        return (
            AUDIT_STATUS_SUCCESS_WITH_WARNINGS
            if remaining_limitations
            else AUDIT_STATUS_SUCCESS
        )
    if status == REFRESH_PARTIAL:
        return AUDIT_STATUS_SUCCESS_WITH_WARNINGS
    if status in {REFRESH_NOT_NEEDED, REFRESH_SOURCE_DELAYED}:
        return AUDIT_STATUS_SKIPPED_NO_NEW_DATA
    if status in {REFRESH_FAILED, REFRESH_BLOCKED, REFRESH_PLANNED, REFRESH_RUNNING}:
        return AUDIT_STATUS_FAILED
    return AUDIT_STATUS_FAILED


def _market_refresh_error_count(
    raw_status: str,
    asset_results: Sequence[Mapping[str, Any]],
) -> int:
    if raw_status not in {REFRESH_FAILED, REFRESH_BLOCKED, REFRESH_PLANNED, REFRESH_RUNNING}:
        return 0
    asset_errors = sum(
        1
        for item in asset_results
        if _text(item.get("status")) in {REFRESH_FAILED, REFRESH_BLOCKED}
    )
    return max(1, asset_errors)


def _market_refresh_warning_count(
    *,
    raw_status: str,
    asset_results: Sequence[Mapping[str, Any]],
    remaining_limitations: Sequence[str],
) -> int:
    warning_statuses = {REFRESH_PARTIAL, REFRESH_SOURCE_DELAYED, REFRESH_NOT_NEEDED}
    warnings = len(remaining_limitations)
    warnings += sum(
        1
        for item in asset_results
        if _text(item.get("status")) not in {"FETCHED", "ALREADY_CURRENT", ""}
    )
    if raw_status in warning_statuses and warnings == 0:
        warnings = 1
    return warnings


def _validation_reason(report: DataQualityReport) -> str:
    if report.status == "PASS":
        return "validate-data quality gate passed without warnings."
    if report.status == "PASS_WITH_WARNINGS":
        return "validate-data quality gate passed with warnings requiring disclosure."
    return "validate-data quality gate failed; downstream data-dependent workflow must stop."


def _check_top_level_contract(
    payload: Mapping[str, Any],
    issues: list[DataRefreshAuditIssue],
) -> None:
    expected = {
        "schema_version",
        "report_type",
        "audit_id",
        "as_of",
        "generated_at",
        "production_effect",
        "policy",
        "safety_boundary",
        "summary",
        "records",
    }
    for field_name in sorted(expected):
        if field_name not in payload:
            issues.append(
                DataRefreshAuditIssue(
                    severity=DataRefreshAuditIssueSeverity.ERROR,
                    code="audit_missing_top_level_field",
                    field=field_name,
                    message=f"Data refresh audit 缺少顶层字段：{field_name}",
                )
            )
    if payload.get("schema_version") != DATA_REFRESH_AUDIT_SCHEMA_VERSION:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="audit_schema_version_invalid",
                field="schema_version",
                message=(
                    "Data refresh audit schema_version 必须为 "
                    f"{DATA_REFRESH_AUDIT_SCHEMA_VERSION}。"
                ),
            )
        )
    if _text(payload.get("report_type")) != DATA_REFRESH_AUDIT_REPORT_TYPE:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="audit_report_type_invalid",
                field="report_type",
                message=f"Data refresh audit report_type 必须为 {DATA_REFRESH_AUDIT_REPORT_TYPE}。",
            )
        )
    if _text(payload.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="audit_production_effect_invalid",
                field="production_effect",
                message="Data refresh audit 必须固定 production_effect=none。",
            )
        )
    status = _text(payload.get("status"))
    if status and status not in DATA_REFRESH_AUDIT_REPORT_STATUSES:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="audit_report_status_invalid",
                field="status",
                message="Data refresh audit status 必须为 PASS、PASS_WITH_WARNINGS 或 FAIL。",
            )
        )


def _check_safety_boundary(
    payload: Mapping[str, Any],
    issues: list[DataRefreshAuditIssue],
) -> None:
    safety = _mapping(payload.get("safety_boundary"))
    expected_false = (
        "data_refresh_allowed",
        "data_downloaded_by_audit",
        "pipelines_executed_by_audit",
        "cache_fabricated",
        "score_or_backtest_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    if safety.get("read_only") is not True:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="safety_boundary_not_read_only",
                field="safety_boundary.read_only",
                message="Data refresh audit safety boundary 必须声明 read_only=true。",
            )
        )
    for field_name in expected_false:
        if safety.get(field_name) is not False:
            issues.append(
                DataRefreshAuditIssue(
                    severity=DataRefreshAuditIssueSeverity.ERROR,
                    code="safety_boundary_forbidden_action_allowed",
                    field=f"safety_boundary.{field_name}",
                    message=f"Safety boundary 必须声明 {field_name}=false。",
                )
            )
    if _text(safety.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="safety_boundary_production_effect_invalid",
                field="safety_boundary.production_effect",
                message="Safety boundary 必须固定 production_effect=none。",
            )
        )


def _check_records(
    records: Sequence[Mapping[str, Any]],
    issues: list[DataRefreshAuditIssue],
) -> None:
    if not records:
        issues.append(
            DataRefreshAuditIssue(
                severity=DataRefreshAuditIssueSeverity.ERROR,
                code="audit_records_empty",
                message="Data refresh audit records 不能为空。",
            )
        )
        return
    seen: set[str] = set()
    for index, record in enumerate(records):
        record_id = _text(record.get("audit_record_id"), f"record_{index + 1}")
        if record_id in seen:
            issues.append(
                DataRefreshAuditIssue(
                    severity=DataRefreshAuditIssueSeverity.ERROR,
                    code="duplicate_audit_record_id",
                    record_id=record_id,
                    field="audit_record_id",
                    message="Audit record id 重复。",
                )
            )
        seen.add(record_id)
        for field_name in DATA_REFRESH_AUDIT_REQUIRED_RECORD_FIELDS:
            if field_name not in record:
                issues.append(
                    DataRefreshAuditIssue(
                        severity=DataRefreshAuditIssueSeverity.ERROR,
                        code="audit_record_missing_required_field",
                        record_id=record_id,
                        field=field_name,
                        message=f"Audit record 缺少必填字段：{field_name}",
                    )
                )
        status = _text(record.get("status"))
        if status not in DATA_REFRESH_AUDIT_RECORD_STATUSES:
            issues.append(
                DataRefreshAuditIssue(
                    severity=DataRefreshAuditIssueSeverity.ERROR,
                    code="audit_record_invalid_status",
                    record_id=record_id,
                    field="status",
                    message=(
                        "Audit record status 必须为 "
                        f"{', '.join(sorted(DATA_REFRESH_AUDIT_RECORD_STATUSES))}。"
                    ),
                )
            )
        for field_name in ("record_count", "warning_count", "error_count"):
            value = _int(record.get(field_name), default=-1)
            if value < 0:
                issues.append(
                    DataRefreshAuditIssue(
                        severity=DataRefreshAuditIssueSeverity.ERROR,
                        code="audit_record_negative_count",
                        record_id=record_id,
                        field=field_name,
                        message=f"Audit record {field_name} 必须为非负整数。",
                    )
                )
        if not _text(record.get("checksum")):
            severity = (
                DataRefreshAuditIssueSeverity.WARNING
                if status
                in {AUDIT_STATUS_SKIPPED_MARKET_CLOSED, AUDIT_STATUS_SKIPPED_NO_NEW_DATA}
                else DataRefreshAuditIssueSeverity.ERROR
            )
            issues.append(
                DataRefreshAuditIssue(
                    severity=severity,
                    code="audit_record_checksum_missing",
                    record_id=record_id,
                    field="checksum",
                    message="Audit record 缺少 cache checksum；需要人工复核数据证据。",
                )
            )
        if _text(record.get("production_effect")) != PRODUCTION_EFFECT:
            issues.append(
                DataRefreshAuditIssue(
                    severity=DataRefreshAuditIssueSeverity.ERROR,
                    code="audit_record_production_effect_invalid",
                    record_id=record_id,
                    field="production_effect",
                    message="Audit record 必须固定 production_effect=none。",
                )
            )


def _with_validation_summary(
    payload: Mapping[str, Any],
    *,
    audit_path: Path,
    validation: DataRefreshAuditValidationReport | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    report = validation or validate_data_refresh_audit_payload(output, audit_path=audit_path)
    audit_status = _text(output.get("status"), report.status)
    summary = dict(_mapping(output.get("summary")))
    summary["status"] = audit_status
    summary["validation_status"] = report.status
    summary["validation_error_count"] = report.error_count
    summary["validation_warning_count"] = report.warning_count
    output["summary"] = summary
    output["status"] = audit_status
    output["validation_status"] = report.status
    output["error_count"] = _int(summary.get("error_count"))
    output["warning_count"] = _int(summary.get("warning_count"))
    output["validation_error_count"] = report.error_count
    output["validation_warning_count"] = report.warning_count
    output["validation_issues"] = [_issue_dict(issue) for issue in report.issues]
    return output


def _write_latest_pointer(
    *,
    output_dir: Path,
    payload: Mapping[str, Any],
    audit_path: Path,
) -> None:
    _write_json(
        output_dir / LATEST_POINTER_NAME,
        {
            "schema_version": DATA_REFRESH_AUDIT_SCHEMA_VERSION,
            "audit_id": _text(payload.get("audit_id")),
            "audit_path": str(audit_path),
            "generated_at": _text(payload.get("generated_at")),
            "status": _text(payload.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
    )


def _latest_audit_from_pointer(output_dir: Path) -> Path | None:
    pointer_path = output_dir / LATEST_POINTER_NAME
    if not pointer_path.exists():
        return None
    pointer = _read_json(pointer_path)
    if not isinstance(pointer, dict):
        return None
    audit_path = Path(_text(pointer.get("audit_path")))
    if audit_path.exists():
        return audit_path
    return None


def _audit_id(
    *,
    as_of: date,
    generated_at: datetime,
    records: Sequence[Mapping[str, Any]],
) -> str:
    digest = sha256()
    digest.update(as_of.isoformat().encode("utf-8"))
    digest.update(generated_at.isoformat().encode("utf-8"))
    for record in sorted(records, key=lambda item: _text(item.get("audit_record_id"))):
        digest.update(_text(record.get("attempt_type")).encode("utf-8"))
        digest.update(_text(record.get("status")).encode("utf-8"))
        digest.update(_text(record.get("checksum")).encode("utf-8"))
    return f"data_refresh_audit_{as_of.isoformat()}_{digest.hexdigest()[:16]}"


def _audit_record_id(record: Mapping[str, Any]) -> str:
    return (
        f"{_text(record.get('attempt_type'), 'UNKNOWN').lower()}_"
        f"{_text(record.get('as_of'), 'unknown')}_"
        f"{_hash_record(record)}"
    )


def _hash_record(record: Mapping[str, Any]) -> str:
    material = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(material.encode("utf-8")).hexdigest()[:16]


def _combined_checksum(values: Sequence[str]) -> str:
    clean = [value for value in values if value]
    if not clean:
        return ""
    digest = sha256()
    for value in sorted(clean):
        digest.update(value.encode("utf-8"))
    return digest.hexdigest()


def _combined_asset_sources(asset_results: Sequence[Mapping[str, Any]]) -> str:
    sources = sorted(
        {
            _text(item.get("source"))
            for item in asset_results
            if _text(item.get("source")) and _text(item.get("source")) != "UNKNOWN"
        }
    )
    return ", ".join(sources)


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            rows = sum(1 for _ in reader)
    except OSError:
        return 0
    return max(0, rows - 1)


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    try:
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return ""
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _issue_dict(issue: DataRefreshAuditIssue) -> dict[str, Any]:
    return {
        "severity": issue.severity.value,
        "code": issue.code,
        "message": issue.message,
        "record_id": issue.record_id or "",
        "field": issue.field or "",
    }


def _issue_from_mapping(raw: Mapping[str, Any]) -> DataRefreshAuditIssue:
    severity = _text(raw.get("severity"), DataRefreshAuditIssueSeverity.WARNING.value)
    if severity not in {item.value for item in DataRefreshAuditIssueSeverity}:
        severity = DataRefreshAuditIssueSeverity.WARNING.value
    return DataRefreshAuditIssue(
        severity=DataRefreshAuditIssueSeverity(severity),
        code=_text(raw.get("code"), "unknown_issue"),
        message=_text(raw.get("message")),
        record_id=_text(raw.get("record_id")) or None,
        field=_text(raw.get("field")) or None,
    )


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        parsed = _parse_datetime(value)
        return None if parsed is None else parsed.date()


def _iso_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()


def _records(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _texts(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _checksum_prefix(value: str) -> str:
    if not value:
        return "MISSING"
    return value[:12]


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
