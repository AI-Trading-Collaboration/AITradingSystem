from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import RiskEventsConfig

RISK_EVENT_PREREVIEW_SCHEMA_VERSION = "risk_event_prereview_queue.v1"
RISK_EVENT_PREREVIEW_PROMPT_VERSION = "risk_event_prereview_v1"

RiskEventPreReviewSourceType = Literal["llm_extracted"]
RiskEventPreReviewManualReviewStatus = Literal["pending_review"]
RiskEventPreReviewOriginalSourceType = Literal[
    "primary_source",
    "paid_vendor",
    "manual_input",
    "public_convenience",
]
RiskEventPreReviewStatusSuggestion = Literal[
    "irrelevant",
    "candidate",
    "watch",
    "active_candidate",
    "resolved_candidate",
]
RiskEventPreReviewLevelSuggestion = Literal["none", "L1", "L2", "L3"]
RiskEventPreReviewEvidenceGradeSuggestion = Literal["S", "A", "B", "C", "D", "X"]

REQUIRED_CSV_COLUMNS = frozenset(
    {
        "precheck_id",
        "source_url",
        "source_name",
        "captured_at",
        "model",
        "prompt_version",
        "request_id",
        "request_timestamp",
        "input_checksum_sha256",
        "output_checksum_sha256",
        "status_suggestion",
        "level_suggestion",
        "raw_summary",
        "human_review_questions",
        "prohibited_actions_ack",
    }
)
OPTIONAL_CSV_COLUMNS = frozenset(
    {
        "source_title",
        "published_at",
        "original_source_type",
        "external_llm_permitted",
        "source_type",
        "manual_review_status",
        "matched_risk_ids",
        "affected_tickers",
        "affected_nodes",
        "evidence_grade_suggestion",
        "confidence",
        "uncertainty_reasons",
        "dedupe_key",
        "notes",
    }
)
ALLOWED_CSV_COLUMNS = REQUIRED_CSV_COLUMNS | OPTIONAL_CSV_COLUMNS

OPENAI_RISK_EVENT_PREREVIEW_SCHEMA: dict[str, Any] = {
    "name": "risk_event_prereview_v1",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "precheck_id",
            "source_url",
            "source_name",
            "captured_at",
            "model",
            "prompt_version",
            "request_id",
            "request_timestamp",
            "input_checksum_sha256",
            "output_checksum_sha256",
            "status_suggestion",
            "level_suggestion",
            "raw_summary",
            "human_review_questions",
            "prohibited_actions_ack",
        ],
        "properties": {
            "precheck_id": {"type": "string"},
            "source_url": {"type": "string"},
            "source_name": {"type": "string"},
            "source_title": {"type": "string"},
            "published_at": {"type": ["string", "null"], "format": "date"},
            "captured_at": {"type": "string", "format": "date"},
            "original_source_type": {
                "type": "string",
                "enum": [
                    "primary_source",
                    "paid_vendor",
                    "manual_input",
                    "public_convenience",
                ],
            },
            "external_llm_permitted": {"type": "boolean"},
            "source_type": {"const": "llm_extracted"},
            "manual_review_status": {"const": "pending_review"},
            "model": {"type": "string"},
            "prompt_version": {"type": "string"},
            "request_id": {"type": "string"},
            "request_timestamp": {"type": "string", "format": "date-time"},
            "input_checksum_sha256": {"type": "string", "pattern": "^[a-fA-F0-9]{64}$"},
            "output_checksum_sha256": {"type": "string", "pattern": "^[a-fA-F0-9]{64}$"},
            "matched_risk_ids": {"type": "array", "items": {"type": "string"}},
            "status_suggestion": {
                "type": "string",
                "enum": [
                    "irrelevant",
                    "candidate",
                    "watch",
                    "active_candidate",
                    "resolved_candidate",
                ],
            },
            "level_suggestion": {"type": "string", "enum": ["none", "L1", "L2", "L3"]},
            "affected_tickers": {"type": "array", "items": {"type": "string"}},
            "affected_nodes": {"type": "array", "items": {"type": "string"}},
            "evidence_grade_suggestion": {
                "type": "string",
                "enum": ["S", "A", "B", "C", "D", "X"],
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "uncertainty_reasons": {"type": "array", "items": {"type": "string"}},
            "human_review_questions": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string"},
            },
            "dedupe_key": {"type": "string"},
            "prohibited_actions_ack": {"type": "boolean", "const": True},
            "raw_summary": {"type": "string"},
            "notes": {"type": "string"},
        },
    },
}


class RiskEventPreReviewIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class RiskEventPreReviewRecord(BaseModel):
    precheck_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    source_url: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    source_title: str = ""
    published_at: date | None = None
    captured_at: date
    original_source_type: RiskEventPreReviewOriginalSourceType = "public_convenience"
    external_llm_permitted: bool = False
    source_type: RiskEventPreReviewSourceType = "llm_extracted"
    manual_review_status: RiskEventPreReviewManualReviewStatus = "pending_review"
    model: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    request_timestamp: datetime
    input_checksum_sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    output_checksum_sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    matched_risk_ids: list[str] = Field(default_factory=list)
    status_suggestion: RiskEventPreReviewStatusSuggestion
    level_suggestion: RiskEventPreReviewLevelSuggestion
    affected_tickers: list[str] = Field(default_factory=list)
    affected_nodes: list[str] = Field(default_factory=list)
    evidence_grade_suggestion: RiskEventPreReviewEvidenceGradeSuggestion = "C"
    confidence: float = Field(default=0.5, ge=0, le=1)
    uncertainty_reasons: list[str] = Field(default_factory=list)
    human_review_questions: list[str] = Field(min_length=1)
    dedupe_key: str = ""
    prohibited_actions_ack: bool
    raw_summary: str = Field(min_length=1)
    notes: str = ""

    @model_validator(mode="after")
    def enforce_prereview_boundary(self) -> RiskEventPreReviewRecord:
        self.affected_tickers = [ticker.upper() for ticker in self.affected_tickers]
        self.matched_risk_ids = [risk_id for risk_id in self.matched_risk_ids if risk_id]
        self.affected_nodes = [node for node in self.affected_nodes if node]
        self.uncertainty_reasons = [reason for reason in self.uncertainty_reasons if reason]
        self.human_review_questions = [
            question for question in self.human_review_questions if question
        ]
        if not self.prohibited_actions_ack:
            raise ValueError("prohibited_actions_ack must be true")
        if (
            self.original_source_type == "paid_vendor"
            and not self.external_llm_permitted
        ):
            raise ValueError("paid_vendor source requires external_llm_permitted=true")
        return self

    @property
    def automatic_score_eligible(self) -> bool:
        return False

    @property
    def position_gate_eligible(self) -> bool:
        return False


@dataclass(frozen=True)
class RiskEventPreReviewIssue:
    severity: RiskEventPreReviewIssueSeverity
    code: str
    message: str
    row_number: int | None = None
    precheck_id: str | None = None


@dataclass(frozen=True)
class RiskEventPreReviewImportReport:
    input_path: Path
    row_count: int
    checksum_sha256: str
    records: tuple[RiskEventPreReviewRecord, ...]
    issues: tuple[RiskEventPreReviewIssue, ...] = field(default_factory=tuple)

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def pending_review_count(self) -> int:
        return sum(
            1 for record in self.records if record.manual_review_status == "pending_review"
        )

    @property
    def high_level_candidate_count(self) -> int:
        return sum(1 for record in self.records if record.level_suggestion in {"L2", "L3"})

    @property
    def active_candidate_count(self) -> int:
        return sum(1 for record in self.records if record.status_suggestion == "active_candidate")

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == RiskEventPreReviewIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == RiskEventPreReviewIssueSeverity.WARNING
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


def import_risk_event_prereview_csv(
    input_path: Path | str,
    *,
    risk_events: RiskEventsConfig | None = None,
    as_of: date | None = None,
) -> RiskEventPreReviewImportReport:
    path = Path(input_path)
    raw_bytes = path.read_bytes()
    checksum = hashlib.sha256(raw_bytes).hexdigest()
    issues: list[RiskEventPreReviewIssue] = []
    reader = csv.DictReader(raw_bytes.decode("utf-8-sig").splitlines())
    fieldnames = tuple(reader.fieldnames or ())
    _check_csv_schema(fieldnames, issues)
    if any(issue.severity == RiskEventPreReviewIssueSeverity.ERROR for issue in issues):
        return RiskEventPreReviewImportReport(
            input_path=path,
            row_count=0,
            checksum_sha256=checksum,
            records=(),
            issues=tuple(issues),
        )

    known_risk_ids = (
        {rule.event_id for rule in risk_events.event_rules}
        if risk_events is not None
        else set()
    )
    records: list[RiskEventPreReviewRecord] = []
    row_count = 0
    for row_number, raw_row in enumerate(reader, start=2):
        row_count += 1
        row = {key: _cell(value) for key, value in raw_row.items() if key is not None}
        try:
            record = _record_from_csv_row(row)
        except (ValidationError, ValueError) as exc:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.ERROR,
                    code="risk_event_prereview_row_invalid",
                    row_number=row_number,
                    precheck_id=row.get("precheck_id") or None,
                    message=_error_message(exc),
                )
            )
            continue
        _check_record(record, row_number, known_risk_ids, as_of, issues)
        records.append(record)

    _check_duplicate_records(records, issues)
    has_error = any(issue.severity == RiskEventPreReviewIssueSeverity.ERROR for issue in issues)
    return RiskEventPreReviewImportReport(
        input_path=path,
        row_count=row_count,
        checksum_sha256=checksum,
        records=tuple(sorted(records, key=lambda item: item.precheck_id)) if not has_error else (),
        issues=tuple(issues),
    )


def render_risk_event_prereview_import_report(
    report: RiskEventPreReviewImportReport,
) -> str:
    lines = [
        "# 风险事件 OpenAI 预审导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 输入路径：`{report.input_path}`",
        f"- CSV 行数：{report.row_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 预审记录数：{report.record_count}",
        f"- 待人工复核：{report.pending_review_count}",
        f"- L2/L3 候选：{report.high_level_candidate_count}",
        f"- active 候选：{report.active_candidate_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 预审队列",
        "",
    ]
    if report.records:
        lines.extend(
            [
                "| Precheck | Source | Model | Request | Status | Level | Risk IDs | "
                "Tickers | Nodes | Confidence | Policy |",
                "|---|---|---|---|---|---|---|---|---|---:|---|",
            ]
        )
        for record in report.records:
            lines.append(
                "| "
                f"{record.precheck_id} | "
                f"{_escape_markdown_table(record.source_name)} | "
                f"{_escape_markdown_table(record.model)} | "
                f"{_escape_markdown_table(record.request_id)} | "
                f"{_status_suggestion_label(record.status_suggestion)} | "
                f"{record.level_suggestion} | "
                f"{', '.join(record.matched_risk_ids)} | "
                f"{', '.join(record.affected_tickers)} | "
                f"{', '.join(record.affected_nodes)} | "
                f"{record.confidence:.2f} | "
                "仅待人工复核，不得评分/不得触发仓位闸门 |"
            )
    else:
        lines.append("未导入可写入的预审记录。")

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | 行 | Precheck | 说明 |", "|---|---|---:|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.row_number or ''} | "
                f"{issue.precheck_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本命令导入固定结构化输出，不在本地发起 OpenAI API 请求。",
            "- 每条预审记录强制为 `source_type=llm_extracted` 和 "
            "`manual_review_status=pending_review`。",
            "- 预审只做抽取、分类、去重、ticker/产业链节点映射和人工复核问题生成。",
            "- 预审记录不是 `risk_event_occurrence`，不得直接进入评分、仓位闸门或回测。",
            "- 付费供应商内容只有在 `external_llm_permitted=true` 时才允许进入外部 LLM 预审。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_risk_event_prereview_import_report(
    report: RiskEventPreReviewImportReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_risk_event_prereview_import_report(report),
        encoding="utf-8",
    )
    return output_path


def write_risk_event_prereview_queue(
    report: RiskEventPreReviewImportReport,
    output_path: Path,
    *,
    generated_at: datetime | None = None,
) -> Path:
    if not report.passed:
        raise ValueError("预审导入存在错误，不能写入风险事件预审队列。")
    timestamp = generated_at or datetime.now(tz=UTC)
    payload = {
        "schema_version": RISK_EVENT_PREREVIEW_SCHEMA_VERSION,
        "generated_at": timestamp.isoformat(),
        "source_csv_path": str(report.input_path),
        "source_csv_checksum_sha256": report.checksum_sha256,
        "row_count": report.row_count,
        "record_count": report.record_count,
        "records": [record.model_dump(mode="json") for record in report.records],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def default_risk_event_prereview_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_event_prereview_import_{as_of.isoformat()}.md"


def _check_csv_schema(
    fieldnames: tuple[str, ...],
    issues: list[RiskEventPreReviewIssue],
) -> None:
    if not fieldnames:
        issues.append(
            RiskEventPreReviewIssue(
                severity=RiskEventPreReviewIssueSeverity.ERROR,
                code="missing_csv_header",
                message="CSV 缺少表头。",
            )
        )
        return
    columns = set(fieldnames)
    missing = sorted(REQUIRED_CSV_COLUMNS - columns)
    if missing:
        issues.append(
            RiskEventPreReviewIssue(
                severity=RiskEventPreReviewIssueSeverity.ERROR,
                code="missing_required_csv_columns",
                message=f"CSV 缺少必填列：{', '.join(missing)}。",
            )
        )
    unknown = sorted(columns - ALLOWED_CSV_COLUMNS)
    if unknown:
        issues.append(
            RiskEventPreReviewIssue(
                severity=RiskEventPreReviewIssueSeverity.WARNING,
                code="unknown_csv_columns",
                message=f"CSV 包含未使用列：{', '.join(unknown)}。",
            )
        )


def _record_from_csv_row(row: dict[str, str]) -> RiskEventPreReviewRecord:
    return RiskEventPreReviewRecord(
        precheck_id=row["precheck_id"],
        source_url=row["source_url"],
        source_name=row["source_name"],
        source_title=row.get("source_title", ""),
        published_at=_parse_optional_date(row.get("published_at", "")),
        captured_at=_parse_required_date(row["captured_at"], "captured_at"),
        original_source_type=row.get("original_source_type") or "public_convenience",
        external_llm_permitted=_parse_bool(
            row.get("external_llm_permitted", ""),
            default=False,
        ),
        source_type=row.get("source_type") or "llm_extracted",
        manual_review_status=row.get("manual_review_status") or "pending_review",
        model=row["model"],
        prompt_version=row["prompt_version"],
        request_id=row["request_id"],
        request_timestamp=_parse_datetime(row["request_timestamp"]),
        input_checksum_sha256=row["input_checksum_sha256"],
        output_checksum_sha256=row["output_checksum_sha256"],
        matched_risk_ids=_split_items(row.get("matched_risk_ids", "")),
        status_suggestion=row["status_suggestion"],
        level_suggestion=row["level_suggestion"],
        affected_tickers=_split_items(row.get("affected_tickers", "")),
        affected_nodes=_split_items(row.get("affected_nodes", "")),
        evidence_grade_suggestion=row.get("evidence_grade_suggestion") or "C",
        confidence=float(row["confidence"]) if row.get("confidence") else 0.5,
        uncertainty_reasons=_split_items(row.get("uncertainty_reasons", "")),
        human_review_questions=_split_items(row["human_review_questions"]),
        dedupe_key=row.get("dedupe_key", ""),
        prohibited_actions_ack=_parse_bool(row["prohibited_actions_ack"], default=False),
        raw_summary=row["raw_summary"],
        notes=row.get("notes", ""),
    )


def _check_record(
    record: RiskEventPreReviewRecord,
    row_number: int,
    known_risk_ids: set[str],
    as_of: date | None,
    issues: list[RiskEventPreReviewIssue],
) -> None:
    if as_of is not None:
        if record.captured_at > as_of or (
            record.published_at is not None and record.published_at > as_of
        ):
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.ERROR,
                    code="risk_event_prereview_date_in_future",
                    row_number=row_number,
                    precheck_id=record.precheck_id,
                    message="预审来源发布日期或采集日期晚于评估日期。",
                )
            )
        if record.request_timestamp.date() > as_of:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.ERROR,
                    code="risk_event_prereview_request_in_future",
                    row_number=row_number,
                    precheck_id=record.precheck_id,
                    message="OpenAI 请求时间晚于评估日期。",
                )
            )
    if known_risk_ids:
        unknown_ids = sorted(set(record.matched_risk_ids) - known_risk_ids)
        if unknown_ids:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.WARNING,
                    code="unknown_matched_risk_id",
                    row_number=row_number,
                    precheck_id=record.precheck_id,
                    message=f"预审匹配了未配置的 risk_id：{', '.join(unknown_ids)}。",
                )
            )
    if record.level_suggestion in {"L2", "L3"} or record.status_suggestion == "active_candidate":
        issues.append(
            RiskEventPreReviewIssue(
                severity=RiskEventPreReviewIssueSeverity.WARNING,
                code="high_impact_prereview_requires_human_confirmation",
                row_number=row_number,
                precheck_id=record.precheck_id,
                message="L2/L3 或 active 候选只能进入人工复核，不能自动写成 active 发生记录。",
            )
        )


def _check_duplicate_records(
    records: list[RiskEventPreReviewRecord],
    issues: list[RiskEventPreReviewIssue],
) -> None:
    seen_precheck: set[str] = set()
    seen_dedupe: dict[str, str] = {}
    for record in records:
        if record.precheck_id in seen_precheck:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.ERROR,
                    code="duplicate_prereview_precheck_id",
                    precheck_id=record.precheck_id,
                    message="precheck_id 重复，无法稳定追踪预审结果。",
                )
            )
        seen_precheck.add(record.precheck_id)
        if not record.dedupe_key:
            continue
        existing = seen_dedupe.get(record.dedupe_key)
        if existing is not None:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.WARNING,
                    code="duplicate_prereview_dedupe_key",
                    precheck_id=record.precheck_id,
                    message=f"dedupe_key 与 {existing} 重复，需要人工判断是否同一事件。",
                )
            )
            continue
        seen_dedupe[record.dedupe_key] = record.precheck_id


def _parse_required_date(value: str, column: str) -> date:
    if not value:
        raise ValueError(f"{column} 不能为空")
    return _parse_date(value, column)


def _parse_optional_date(value: str) -> date | None:
    if not value:
        return None
    return _parse_date(value, "published_at")


def _parse_date(value: str, column: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{column} 必须使用 YYYY-MM-DD 日期格式") from exc


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        raise ValueError("request_timestamp 不能为空")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("request_timestamp 必须使用 ISO datetime 格式") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_bool(value: str, *, default: bool) -> bool:
    if not value:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "是"}:
        return True
    if normalized in {"0", "false", "no", "n", "否"}:
        return False
    raise ValueError(f"布尔字段无法解析：{value}")


def _split_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def _cell(value: str | None) -> str:
    return "" if value is None else value.strip()


def _error_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        first_error: Any = exc.errors()[0] if exc.errors() else {}
        location = ".".join(str(part) for part in first_error.get("loc", ()))
        message = str(first_error.get("msg", "schema validation failed"))
        return f"{location}: {message}" if location else message
    return str(exc)


def _severity_label(severity: RiskEventPreReviewIssueSeverity) -> str:
    if severity == RiskEventPreReviewIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _status_suggestion_label(value: str) -> str:
    return {
        "irrelevant": "无关",
        "candidate": "候选",
        "watch": "观察候选",
        "active_candidate": "active 候选",
        "resolved_candidate": "解除候选",
    }.get(value, value)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
