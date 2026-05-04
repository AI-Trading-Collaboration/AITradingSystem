from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ValidationError

from ai_trading_system.risk_events import (
    RiskEventActionClass,
    RiskEventEvidenceGrade,
    RiskEventEvidenceSource,
    RiskEventIssueSeverity,
    RiskEventOccurrence,
    RiskEventOccurrenceSourceType,
    RiskEventOccurrenceStatus,
    RiskEventProbability,
    RiskEventReversibility,
    RiskEventScope,
    RiskEventSeverity,
    RiskEventTimeSensitivity,
)

REQUIRED_COLUMNS = frozenset(
    {
        "occurrence_id",
        "event_id",
        "status",
        "triggered_at",
        "last_confirmed_at",
        "source_name",
        "source_type",
        "captured_at",
        "summary",
        "reviewer",
        "reviewed_at",
        "review_decision",
        "rationale",
        "next_review_due",
    }
)
OPTIONAL_COLUMNS = frozenset(
    {
        "resolved_at",
        "evidence_grade",
        "severity",
        "probability",
        "scope",
        "time_sensitivity",
        "reversibility",
        "action_class",
        "source_url",
        "published_at",
        "evidence_notes",
        "notes",
    }
)
ALLOWED_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS
ALLOWED_STATUSES = frozenset({"active", "watch", "resolved", "dismissed"})
ALLOWED_SOURCE_TYPES = frozenset(
    {"primary_source", "paid_vendor", "manual_input", "public_convenience"}
)


@dataclass(frozen=True)
class RiskEventOccurrenceImportIssue:
    severity: RiskEventIssueSeverity
    code: str
    message: str
    row_number: int | None = None
    occurrence_id: str | None = None


@dataclass(frozen=True)
class RiskEventOccurrenceImportReport:
    input_path: Path
    row_count: int
    checksum_sha256: str
    occurrences: tuple[RiskEventOccurrence, ...]
    issues: tuple[RiskEventOccurrenceImportIssue, ...] = field(default_factory=tuple)

    @property
    def occurrence_count(self) -> int:
        return len(self.occurrences)

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


@dataclass
class _OccurrenceBuilder:
    occurrence_id: str
    event_id: str
    status: RiskEventOccurrenceStatus
    triggered_at: date
    last_confirmed_at: date
    resolved_at: date | None
    evidence_grade: RiskEventEvidenceGrade
    severity: RiskEventSeverity
    probability: RiskEventProbability
    scope: RiskEventScope
    time_sensitivity: RiskEventTimeSensitivity
    reversibility: RiskEventReversibility
    action_class: RiskEventActionClass
    reviewer: str
    reviewed_at: date
    review_decision: str
    rationale: str
    next_review_due: date
    summary: str
    notes: str
    evidence_sources: list[RiskEventEvidenceSource] = field(default_factory=list)

    def consistency_signature(self) -> tuple[Any, ...]:
        return (
            self.event_id,
            self.status,
            self.triggered_at,
            self.last_confirmed_at,
            self.resolved_at,
            self.evidence_grade,
            self.severity,
            self.probability,
            self.scope,
            self.time_sensitivity,
            self.reversibility,
            self.action_class,
            self.reviewer,
            self.reviewed_at,
            self.review_decision,
            self.rationale,
            self.next_review_due,
            self.summary,
        )


def import_risk_event_occurrences_csv(
    input_path: Path | str,
) -> RiskEventOccurrenceImportReport:
    path = Path(input_path)
    raw_bytes = path.read_bytes()
    checksum = hashlib.sha256(raw_bytes).hexdigest()
    issues: list[RiskEventOccurrenceImportIssue] = []
    builders: dict[str, _OccurrenceBuilder] = {}

    text = raw_bytes.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    fieldnames = tuple(reader.fieldnames or ())
    _check_schema(fieldnames, issues)

    if any(issue.severity == RiskEventIssueSeverity.ERROR for issue in issues):
        return RiskEventOccurrenceImportReport(
            input_path=path,
            row_count=0,
            checksum_sha256=checksum,
            occurrences=(),
            issues=tuple(issues),
        )

    row_count = 0
    for row_number, raw_row in enumerate(reader, start=2):
        row_count += 1
        row = {key: _cell(value) for key, value in raw_row.items() if key is not None}
        parsed = _parse_row(row, row_number, issues)
        if parsed is None:
            continue

        builder, evidence = parsed
        existing = builders.get(builder.occurrence_id)
        if existing is None:
            builder.evidence_sources.append(evidence)
            builders[builder.occurrence_id] = builder
            continue

        if existing.consistency_signature() != builder.consistency_signature():
            issues.append(
                RiskEventOccurrenceImportIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="inconsistent_duplicate_occurrence",
                    row_number=row_number,
                    occurrence_id=builder.occurrence_id,
                    message=(
                        "同一 occurrence_id 的 event_id/status/dates/summary 不一致，"
                        "不能合并证据来源。"
                    ),
                )
            )
            continue
        if existing.notes != builder.notes:
            issues.append(
                RiskEventOccurrenceImportIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="duplicate_occurrence_notes_ignored",
                    row_number=row_number,
                    occurrence_id=builder.occurrence_id,
                    message="同一 occurrence_id 的 notes 不一致；保留首次出现的 notes。",
                )
            )
        existing.evidence_sources.append(evidence)

    occurrences = _build_occurrences(builders, issues)
    if any(issue.severity == RiskEventIssueSeverity.ERROR for issue in issues):
        occurrences = []

    return RiskEventOccurrenceImportReport(
        input_path=path,
        row_count=row_count,
        checksum_sha256=checksum,
        occurrences=tuple(sorted(occurrences, key=lambda item: item.occurrence_id)),
        issues=tuple(issues),
    )


def render_risk_event_occurrence_import_report(
    report: RiskEventOccurrenceImportReport,
) -> str:
    lines = [
        "# 风险事件发生记录 CSV 导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 输入路径：`{report.input_path}`",
        f"- CSV 行数：{report.row_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 导入发生记录数：{report.occurrence_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 导入记录",
        "",
    ]

    if report.occurrences:
        lines.extend(
            [
                "| Occurrence | 事件 | 状态 | 触发日期 | 最近确认 | 复核人 | 下次复核 | 证据数 |",
                "|---|---|---|---|---|---|---|---:|",
            ]
        )
        for occurrence in report.occurrences:
            lines.append(
                "| "
                f"{occurrence.occurrence_id} | "
                f"{occurrence.event_id} | "
                f"{occurrence.status} | "
                f"{occurrence.triggered_at.isoformat()} | "
                f"{occurrence.last_confirmed_at.isoformat()} | "
                f"{_escape_markdown_table(occurrence.reviewer)} | "
                f"{occurrence.next_review_due.isoformat() if occurrence.next_review_due else ''} | "
                f"{len(occurrence.evidence_sources)} |"
            )
    else:
        lines.append("未导入可写入的发生记录。")

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | 行 | Occurrence | 说明 |", "|---|---|---:|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.row_number or ''} | "
                f"{issue.occurrence_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本导入器只接受人工复核后的结构化事件发生记录 CSV。",
            "- 导入结果仅生成 `RiskEventOccurrence` 审计记录，不把原始新闻转换为交易动作。",
            "- 多行相同 `occurrence_id` 只用于合并多个证据来源；关键字段不一致会失败。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_risk_event_occurrence_import_report(
    report: RiskEventOccurrenceImportReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_risk_event_occurrence_import_report(report),
        encoding="utf-8",
    )
    return output_path


def write_risk_event_occurrences_yaml(
    report: RiskEventOccurrenceImportReport,
    output_dir: Path,
) -> tuple[Path, ...]:
    if not report.passed:
        raise ValueError("CSV 导入存在错误，不能写入风险事件发生记录 YAML。")

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for occurrence in report.occurrences:
        output_path = output_dir / f"{occurrence.occurrence_id}.yaml"
        output_path.write_text(
            yaml.safe_dump(
                occurrence.model_dump(mode="json", exclude_none=False),
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        paths.append(output_path)
    return tuple(paths)


def _check_schema(
    fieldnames: tuple[str, ...],
    issues: list[RiskEventOccurrenceImportIssue],
) -> None:
    if not fieldnames:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="missing_csv_header",
                message="CSV 缺少表头。",
            )
        )
        return

    columns = set(fieldnames)
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="missing_required_csv_columns",
                message=f"CSV 缺少必填列：{', '.join(missing)}。",
            )
        )

    unknown = sorted(columns - ALLOWED_COLUMNS)
    if unknown:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="unknown_csv_columns",
                message=f"CSV 包含未使用列：{', '.join(unknown)}。",
            )
        )


def _parse_row(
    row: dict[str, str],
    row_number: int,
    issues: list[RiskEventOccurrenceImportIssue],
) -> tuple[_OccurrenceBuilder, RiskEventEvidenceSource] | None:
    occurrence_id = row["occurrence_id"]
    event_id = row["event_id"]
    status = row["status"]
    source_type = row["source_type"]
    row_errors = 0

    for column in REQUIRED_COLUMNS:
        if row[column] == "":
            issues.append(
                RiskEventOccurrenceImportIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="missing_required_csv_value",
                    row_number=row_number,
                    occurrence_id=occurrence_id or None,
                    message=f"必填列 {column} 不能为空。",
                )
            )
            row_errors += 1

    if status and status not in ALLOWED_STATUSES:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="invalid_risk_event_occurrence_status",
                row_number=row_number,
                occurrence_id=occurrence_id or None,
                message=f"status 必须是：{', '.join(sorted(ALLOWED_STATUSES))}。",
            )
        )
        row_errors += 1
    if source_type and source_type not in ALLOWED_SOURCE_TYPES:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="invalid_risk_event_source_type",
                row_number=row_number,
                occurrence_id=occurrence_id or None,
                message=f"source_type 必须是：{', '.join(sorted(ALLOWED_SOURCE_TYPES))}。",
            )
        )
        row_errors += 1

    issue_count_before_dates = len(issues)
    triggered_at = _parse_required_date(row, "triggered_at", row_number, occurrence_id, issues)
    last_confirmed_at = _parse_required_date(
        row, "last_confirmed_at", row_number, occurrence_id, issues
    )
    captured_at = _parse_required_date(row, "captured_at", row_number, occurrence_id, issues)
    reviewed_at = _parse_required_date(row, "reviewed_at", row_number, occurrence_id, issues)
    next_review_due = _parse_required_date(
        row,
        "next_review_due",
        row_number,
        occurrence_id,
        issues,
    )
    resolved_at = _parse_optional_date(row, "resolved_at", row_number, occurrence_id, issues)
    published_at = _parse_optional_date(row, "published_at", row_number, occurrence_id, issues)

    if None in (triggered_at, last_confirmed_at, captured_at, reviewed_at, next_review_due):
        row_errors += 1
    if len(issues) > issue_count_before_dates:
        row_errors += 1
    if source_type != "manual_input" and not row.get("source_url", ""):
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="missing_risk_event_evidence_url",
                row_number=row_number,
                occurrence_id=occurrence_id or None,
                message="非 manual_input 证据建议填写 source_url，便于审计复核。",
            )
        )

    if row_errors:
        return None
    if (
        triggered_at is None
        or last_confirmed_at is None
        or captured_at is None
        or reviewed_at is None
        or next_review_due is None
    ):
        return None

    try:
        evidence = RiskEventEvidenceSource(
            source_name=row["source_name"],
            source_type=cast(RiskEventOccurrenceSourceType, source_type),
            source_url=row.get("source_url", ""),
            published_at=published_at,
            captured_at=captured_at,
            notes=row.get("evidence_notes", ""),
        )
    except ValidationError as exc:
        issues.append(_validation_issue(exc, row_number, occurrence_id))
        return None

    return (
        _OccurrenceBuilder(
            occurrence_id=occurrence_id,
            event_id=event_id,
            status=cast(RiskEventOccurrenceStatus, status),
            triggered_at=triggered_at,
            last_confirmed_at=last_confirmed_at,
            resolved_at=resolved_at,
            evidence_grade=cast(
                RiskEventEvidenceGrade,
                row.get("evidence_grade", "") or "B",
            ),
            severity=cast(RiskEventSeverity, row.get("severity", "") or "unknown"),
            probability=cast(
                RiskEventProbability,
                row.get("probability", "") or "unknown",
            ),
            scope=cast(RiskEventScope, row.get("scope", "") or "unknown"),
            time_sensitivity=cast(
                RiskEventTimeSensitivity,
                row.get("time_sensitivity", "") or "unknown",
            ),
            reversibility=cast(
                RiskEventReversibility,
                row.get("reversibility", "") or "unknown",
            ),
            action_class=cast(
                RiskEventActionClass,
                row.get("action_class", "") or "manual_review",
            ),
            reviewer=row["reviewer"],
            reviewed_at=reviewed_at,
            review_decision=row["review_decision"],
            rationale=row["rationale"],
            next_review_due=next_review_due,
            summary=row["summary"],
            notes=row.get("notes", ""),
        ),
        evidence,
    )


def _build_occurrences(
    builders: dict[str, _OccurrenceBuilder],
    issues: list[RiskEventOccurrenceImportIssue],
) -> list[RiskEventOccurrence]:
    occurrences: list[RiskEventOccurrence] = []
    for builder in builders.values():
        try:
            occurrences.append(
                RiskEventOccurrence(
                    occurrence_id=builder.occurrence_id,
                    event_id=builder.event_id,
                    status=builder.status,
                    triggered_at=builder.triggered_at,
                    last_confirmed_at=builder.last_confirmed_at,
                    resolved_at=builder.resolved_at,
                    evidence_grade=builder.evidence_grade,
                    severity=builder.severity,
                    probability=builder.probability,
                    scope=builder.scope,
                    time_sensitivity=builder.time_sensitivity,
                    reversibility=builder.reversibility,
                    action_class=builder.action_class,
                    reviewer=builder.reviewer,
                    reviewed_at=builder.reviewed_at,
                    review_decision=builder.review_decision,
                    rationale=builder.rationale,
                    next_review_due=builder.next_review_due,
                    evidence_sources=builder.evidence_sources,
                    summary=builder.summary,
                    notes=builder.notes,
                )
            )
        except ValidationError as exc:
            issues.append(_validation_issue(exc, None, builder.occurrence_id))
    return occurrences


def _parse_required_date(
    row: dict[str, str],
    column: str,
    row_number: int,
    occurrence_id: str,
    issues: list[RiskEventOccurrenceImportIssue],
) -> date | None:
    value = row[column]
    if not value:
        return None
    return _parse_date(value, column, row_number, occurrence_id, issues)


def _parse_optional_date(
    row: dict[str, str],
    column: str,
    row_number: int,
    occurrence_id: str,
    issues: list[RiskEventOccurrenceImportIssue],
) -> date | None:
    value = row.get(column, "")
    if not value:
        return None
    return _parse_date(value, column, row_number, occurrence_id, issues)


def _parse_date(
    value: str,
    column: str,
    row_number: int,
    occurrence_id: str,
    issues: list[RiskEventOccurrenceImportIssue],
) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        issues.append(
            RiskEventOccurrenceImportIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="invalid_csv_date",
                row_number=row_number,
                occurrence_id=occurrence_id or None,
                message=f"{column} 必须使用 YYYY-MM-DD 日期格式。",
            )
        )
        return None


def _validation_issue(
    exc: ValidationError,
    row_number: int | None,
    occurrence_id: str,
) -> RiskEventOccurrenceImportIssue:
    first_error: Any = exc.errors()[0] if exc.errors() else {}
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return RiskEventOccurrenceImportIssue(
        severity=RiskEventIssueSeverity.ERROR,
        code="risk_event_occurrence_schema_error",
        row_number=row_number,
        occurrence_id=occurrence_id or None,
        message=f"{location}: {message}" if location else message,
    )


def _cell(value: str | None) -> str:
    return "" if value is None else value.strip()


def _severity_label(severity: RiskEventIssueSeverity) -> str:
    if severity == RiskEventIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
