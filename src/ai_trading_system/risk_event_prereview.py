from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import DataSourcesConfig, RiskEventsConfig
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_HTTP_CLIENT,
    DEFAULT_OPENAI_LLM_MODEL,
    DEFAULT_OPENAI_REASONING_EFFORT,
    DEFAULT_OPENAI_RESPONSES_ENDPOINT,
    DEFAULT_OPENAI_TIMEOUT_SECONDS,
    HttpPostJson,
    LlmClaimPrecheckInput,
    LlmClaimPrecheckRecord,
    LlmClaimPrecheckReport,
    LlmPrecheckIssueSeverity,
    OpenAIReasoningEffort,
    run_openai_claim_precheck,
)
from ai_trading_system.official_policy_sources import OfficialPolicyCandidate

RISK_EVENT_PREREVIEW_SCHEMA_VERSION = "risk_event_prereview_queue.v2"
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
RiskEventPreReviewSourceKind = Literal["csv_import", "openai_live"]

REQUIRED_CSV_COLUMNS = frozenset(
    {
        "precheck_id",
        "source_url",
        "source_name",
        "captured_at",
        "model",
        "reasoning_effort",
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
            "reasoning_effort",
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
            "reasoning_effort": {
                "type": "string",
                "enum": ["none", "minimal", "low", "medium", "high", "xhigh"],
            },
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
    reasoning_effort: OpenAIReasoningEffort
    prompt_version: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    response_id: str = ""
    client_request_id: str = ""
    request_timestamp: datetime
    input_checksum_sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    output_checksum_sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    source_permission: dict[str, Any] = Field(default_factory=dict)
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
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RiskEventPreReviewImportReport:
    input_path: Path
    row_count: int
    checksum_sha256: str
    records: tuple[RiskEventPreReviewRecord, ...]
    source_kind: RiskEventPreReviewSourceKind = "csv_import"
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


def run_openai_risk_event_prereview(
    input_packet: LlmClaimPrecheckInput,
    *,
    api_key: str,
    data_sources: DataSourcesConfig | None = None,
    risk_events: RiskEventsConfig | None = None,
    input_path: Path | str = Path("<memory>"),
    as_of: date | None = None,
    model: str = DEFAULT_OPENAI_LLM_MODEL,
    reasoning_effort: str = DEFAULT_OPENAI_REASONING_EFFORT,
    endpoint: str = DEFAULT_OPENAI_RESPONSES_ENDPOINT,
    timeout_seconds: float = DEFAULT_OPENAI_TIMEOUT_SECONDS,
    http_client: str = DEFAULT_OPENAI_HTTP_CLIENT,
    generated_at: datetime | None = None,
    http_post_json: HttpPostJson | None = None,
) -> RiskEventPreReviewImportReport:
    claim_report = run_openai_claim_precheck(
        input_packet,
        api_key=api_key,
        data_sources=data_sources,
        input_path=input_path,
        model=model,
        reasoning_effort=reasoning_effort,
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        http_client=http_client,
        generated_at=generated_at,
        http_post_json=http_post_json,
    )
    return build_risk_event_prereview_from_llm_claim_report(
        claim_report,
        risk_events=risk_events,
        as_of=as_of,
    )


def run_openai_risk_event_prereview_for_official_candidates(
    candidates: tuple[OfficialPolicyCandidate, ...],
    *,
    api_key: str,
    data_sources: DataSourcesConfig | None = None,
    risk_events: RiskEventsConfig | None = None,
    input_path: Path | str = Path("<official_policy_candidates>"),
    as_of: date | None = None,
    model: str = DEFAULT_OPENAI_LLM_MODEL,
    reasoning_effort: str = DEFAULT_OPENAI_REASONING_EFFORT,
    endpoint: str = DEFAULT_OPENAI_RESPONSES_ENDPOINT,
    timeout_seconds: float = DEFAULT_OPENAI_TIMEOUT_SECONDS,
    http_client: str = DEFAULT_OPENAI_HTTP_CLIENT,
    generated_at: datetime | None = None,
    http_post_json: HttpPostJson | None = None,
    max_candidates: int | None = None,
) -> RiskEventPreReviewImportReport:
    records: list[RiskEventPreReviewRecord] = []
    issues: list[RiskEventPreReviewIssue] = []
    row_count = 0
    packet_checksums: list[dict[str, str]] = []
    selected_candidates = sorted(candidates, key=_official_candidate_priority_key)
    if max_candidates is not None:
        if max_candidates < 0:
            raise ValueError("max_candidates must be non-negative")
        if len(selected_candidates) > max_candidates:
            issues.append(
                RiskEventPreReviewIssue(
                    severity=RiskEventPreReviewIssueSeverity.WARNING,
                    code="risk_event_prereview_candidate_limit_applied",
                    message=(
                        f"官方候选数 {len(selected_candidates)} 超过本次 OpenAI 预审上限 "
                        f"{max_candidates}；仅预审优先级排序后的前 {max_candidates} 条。"
                    ),
                )
            )
            selected_candidates = selected_candidates[:max_candidates]

    for candidate in selected_candidates:
        packet = official_policy_candidate_to_llm_input(candidate)
        packet_checksum = _packet_checksum(packet)
        packet_checksums.append(
            {
                "candidate_id": candidate.candidate_id,
                "packet_checksum_sha256": packet_checksum,
            }
        )
        report = run_openai_risk_event_prereview(
            packet,
            api_key=api_key,
            data_sources=data_sources,
            risk_events=risk_events,
            input_path=input_path,
            as_of=as_of,
            model=model,
            reasoning_effort=reasoning_effort,
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
            http_client=http_client,
            generated_at=generated_at,
            http_post_json=http_post_json,
        )
        row_count += report.row_count
        records.extend(report.records)
        issues.extend(
            issue
            for issue in report.issues
            if issue.code != "risk_event_prereview_no_risk_event_candidates"
        )
        if report.error_count:
            break

    _check_duplicate_records(records, issues)
    has_error = any(issue.severity == RiskEventPreReviewIssueSeverity.ERROR for issue in issues)
    return RiskEventPreReviewImportReport(
        input_path=Path(input_path),
        row_count=row_count,
        checksum_sha256=_official_candidate_batch_checksum(packet_checksums),
        records=tuple(sorted(records, key=lambda item: item.precheck_id)) if not has_error else (),
        source_kind="openai_live",
        issues=tuple(issues),
    )


def official_policy_candidate_to_llm_input(
    candidate: OfficialPolicyCandidate,
) -> LlmClaimPrecheckInput:
    return LlmClaimPrecheckInput(
        precheck_id=f"precheck:{_safe_id_segment(candidate.candidate_id)}",
        source_id=candidate.source_id,
        source_url=candidate.source_url,
        source_name=candidate.source_name,
        source_title=candidate.source_title,
        published_at=candidate.published_at,
        captured_at=candidate.captured_at,
        content_sent_level="metadata_only",
        content_text=_official_candidate_content(candidate),
        notes=(
            "official_policy_candidate_auto_precheck; metadata_only; "
            "do_not_create_occurrence_or_attestation"
        ),
    )


def _official_candidate_priority_key(candidate: OfficialPolicyCandidate) -> tuple[object, ...]:
    topic_weights = {
        "export_controls": 0,
        "ai_policy": 1,
        "trade_policy": 2,
        "sanctions": 3,
        "taiwan_geopolitics": 4,
        "china_technology": 5,
        "russia_geopolitics": 6,
    }
    source_weights = {
        "official_bis_federal_register_notices": 0,
        "official_federal_register_policy_documents": 1,
        "official_govinfo_federal_register": 2,
        "official_ustr_press_releases": 3,
        "official_congress_bills": 4,
        "official_trade_csl_json": 5,
        "official_ofac_sdn_xml": 6,
        "official_ofac_consolidated_xml": 7,
    }
    topic_rank = min(
        (topic_weights.get(topic, 99) for topic in candidate.matched_topics),
        default=99,
    )
    published_rank = -candidate.published_at.toordinal() if candidate.published_at else 0
    return (
        0 if candidate.matched_risk_ids else 1,
        topic_rank,
        source_weights.get(candidate.source_id, 50),
        0 if candidate.affected_tickers else 1,
        0 if candidate.affected_nodes else 1,
        published_rank,
        candidate.candidate_id,
    )


def build_risk_event_prereview_from_llm_claim_report(
    claim_report: LlmClaimPrecheckReport,
    *,
    risk_events: RiskEventsConfig | None = None,
    as_of: date | None = None,
) -> RiskEventPreReviewImportReport:
    issues = [_issue_from_llm_precheck(issue) for issue in claim_report.issues]
    if claim_report.error_count:
        return RiskEventPreReviewImportReport(
            input_path=claim_report.input_path,
            row_count=claim_report.claim_count,
            checksum_sha256=_claim_report_checksum(claim_report),
            records=(),
            source_kind="openai_live",
            issues=tuple(issues),
        )

    known_risk_ids = (
        {rule.event_id for rule in risk_events.event_rules}
        if risk_events is not None
        else set()
    )
    records: list[RiskEventPreReviewRecord] = []
    for llm_record in claim_report.records:
        for claim_index, claim in enumerate(llm_record.claims, start=1):
            candidate = claim.risk_event_candidate
            if not _claim_has_risk_event_candidate(claim.claim_type, candidate):
                continue
            row_number = len(records) + 1
            try:
                record = _record_from_llm_claim(llm_record, claim_index)
            except (ValidationError, ValueError) as exc:
                issues.append(
                    RiskEventPreReviewIssue(
                        severity=RiskEventPreReviewIssueSeverity.ERROR,
                        code="risk_event_prereview_llm_claim_invalid",
                        row_number=row_number,
                        precheck_id=llm_record.precheck_id,
                        message=_error_message(exc),
                    )
                )
                continue
            _check_record(record, row_number, known_risk_ids, as_of, issues)
            records.append(record)

    if claim_report.record_count and not records:
        issues.append(
            RiskEventPreReviewIssue(
                severity=RiskEventPreReviewIssueSeverity.WARNING,
                code="risk_event_prereview_no_risk_event_candidates",
                message="OpenAI 预审未输出可写入风险事件待复核队列的候选。",
            )
        )

    _check_duplicate_records(records, issues)
    has_error = any(issue.severity == RiskEventPreReviewIssueSeverity.ERROR for issue in issues)
    return RiskEventPreReviewImportReport(
        input_path=claim_report.input_path,
        row_count=claim_report.claim_count,
        checksum_sha256=_claim_report_checksum(claim_report),
        records=tuple(sorted(records, key=lambda item: item.precheck_id)) if not has_error else (),
        source_kind="openai_live",
        issues=tuple(issues),
    )


def render_risk_event_prereview_import_report(
    report: RiskEventPreReviewImportReport,
) -> str:
    is_live = report.source_kind == "openai_live"
    lines = [
        "# 风险事件 OpenAI 预审报告" if is_live else "# 风险事件 OpenAI 预审导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 输入路径：`{report.input_path}`",
        f"- {'LLM claim 数' if is_live else 'CSV 行数'}：{report.row_count}",
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
                "| Precheck | Source | Model | Reasoning | Request | Status | Level | Risk IDs | "
                "Tickers | Nodes | Confidence | Policy |",
                "|---|---|---|---|---|---|---|---|---|---|---:|---|",
            ]
        )
        for record in report.records:
            lines.append(
                "| "
                f"{record.precheck_id} | "
                f"{_escape_markdown_table(record.source_name)} | "
                f"{_escape_markdown_table(record.model)} | "
                f"{record.reasoning_effort} | "
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
        diagnostic_lines = _render_issue_diagnostics(report.issues)
        if diagnostic_lines:
            lines.extend(["", "## 请求诊断", "", *diagnostic_lines])

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            (
                "- 本命令在本地调用 OpenAI Responses API 固定结构化输出，"
                "请求使用 `store=false`。"
                if is_live
                else "- 本命令导入固定结构化输出，不在本地发起 OpenAI API 请求。"
            ),
            (
                "- 单个 OpenAI 请求遇到超时、429 或 5xx 等瞬时失败时最多重试 2 次；"
                "第 3 次仍失败则整批 fail closed。"
                if is_live
                else "- 导入模式不会重试外部 API 请求。"
            ),
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
        "source_kind": report.source_kind,
        "source_input_path": str(report.input_path),
        "source_input_checksum_sha256": report.checksum_sha256,
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


def default_risk_event_openai_prereview_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_event_prereview_openai_{as_of.isoformat()}.md"


def _record_from_llm_claim(
    llm_record: LlmClaimPrecheckRecord,
    claim_index: int,
) -> RiskEventPreReviewRecord:
    claim = llm_record.claims[claim_index - 1]
    candidate = claim.risk_event_candidate
    review_questions = _unique_nonempty(
        [
            *claim.required_review_questions,
            *candidate.review_questions,
            *[f"缺失确认：{item}" for item in candidate.missing_confirmations],
        ]
    )
    if not review_questions:
        review_questions = ["人工确认该 OpenAI 线索是否构成风险事件。"]

    matched_risk_ids = _unique_nonempty(candidate.risk_id_candidate)
    uncertainty_reasons = _unique_nonempty(
        [*claim.conflicts_or_uncertainties, *candidate.missing_confirmations]
    )
    notes = "; ".join(
        _unique_nonempty(
            [
                f"derived_from={llm_record.precheck_id}",
                f"source_span_ref={claim.source_span_ref}",
                f"severity_candidate={candidate.severity_candidate}",
                f"probability_candidate={candidate.probability_candidate}",
                f"scope_candidate={candidate.scope_candidate}",
                f"time_sensitivity_candidate={candidate.time_sensitivity_candidate}",
                f"action_class_candidate={candidate.action_class_candidate}",
                (
                    "thesis_signal_match="
                    f"{','.join(claim.thesis_signal_match)}"
                    if claim.thesis_signal_match
                    else ""
                ),
                llm_record.notes,
            ]
        )
    )
    return RiskEventPreReviewRecord(
        precheck_id=(
            f"{_safe_id_segment(llm_record.precheck_id)}:"
            f"{_safe_id_segment(claim.claim_id or str(claim_index))}"
        ),
        source_url=llm_record.source_url,
        source_name=llm_record.source_name,
        source_title=llm_record.source_title,
        published_at=llm_record.published_at,
        captured_at=llm_record.captured_at,
        original_source_type=llm_record.source_permission.source_type,
        external_llm_permitted=llm_record.source_permission.external_llm_allowed,
        source_type="llm_extracted",
        manual_review_status="pending_review",
        model=llm_record.model,
        reasoning_effort=llm_record.reasoning_effort,
        prompt_version=llm_record.prompt_version,
        request_id=llm_record.request_id,
        response_id=llm_record.response_id,
        client_request_id=llm_record.client_request_id,
        request_timestamp=llm_record.request_timestamp,
        input_checksum_sha256=llm_record.input_checksum_sha256,
        output_checksum_sha256=llm_record.output_checksum_sha256,
        source_permission=llm_record.source_permission.model_dump(mode="json"),
        matched_risk_ids=matched_risk_ids,
        status_suggestion=_status_suggestion_from_candidate(
            str(claim.claim_type),
            candidate.status_candidate,
        ),
        level_suggestion=_level_suggestion_from_candidate(candidate.level_candidate),
        affected_tickers=claim.affected_tickers,
        affected_nodes=claim.affected_nodes,
        evidence_grade_suggestion=claim.evidence_grade_suggestion,
        confidence=claim.confidence,
        uncertainty_reasons=uncertainty_reasons,
        human_review_questions=review_questions,
        dedupe_key=_dedupe_key(llm_record, matched_risk_ids, claim.source_span_ref),
        prohibited_actions_ack=claim.prohibited_actions_ack,
        raw_summary=claim.claim_text_zh,
        notes=notes,
    )


def _claim_has_risk_event_candidate(claim_type: str, candidate: Any) -> bool:
    if (
        candidate.status_candidate in {"none", "irrelevant"}
        and candidate.level_candidate == "none"
        and candidate.action_class_candidate == "none"
    ):
        return False
    return (
        claim_type == "risk_event"
        or candidate.status_candidate not in {"none", "irrelevant"}
        or candidate.level_candidate != "none"
        or candidate.action_class_candidate != "none"
        or bool(candidate.risk_id_candidate)
    )


def _status_suggestion_from_candidate(
    claim_type: str,
    status_candidate: str,
) -> RiskEventPreReviewStatusSuggestion:
    if status_candidate in {
        "irrelevant",
        "candidate",
        "watch",
        "active_candidate",
        "resolved_candidate",
    }:
        return status_candidate  # type: ignore[return-value]
    if claim_type == "risk_event":
        return "candidate"
    return "irrelevant"


def _level_suggestion_from_candidate(
    level_candidate: str,
) -> RiskEventPreReviewLevelSuggestion:
    if level_candidate in {"none", "L1", "L2", "L3"}:
        return level_candidate  # type: ignore[return-value]
    return "none"


def _issue_from_llm_precheck(issue: Any) -> RiskEventPreReviewIssue:
    severity = (
        RiskEventPreReviewIssueSeverity.ERROR
        if issue.severity == LlmPrecheckIssueSeverity.ERROR
        else RiskEventPreReviewIssueSeverity.WARNING
    )
    return RiskEventPreReviewIssue(
        severity=severity,
        code=issue.code,
        message=issue.message,
        precheck_id=issue.precheck_id,
        diagnostics=getattr(issue, "diagnostics", {}),
    )


def _claim_report_checksum(report: LlmClaimPrecheckReport) -> str:
    payload = {
        "input_path": str(report.input_path),
        "generated_at": report.generated_at.isoformat(),
        "records": [
            {
                "precheck_id": record.precheck_id,
                "model": record.model,
                "reasoning_effort": record.reasoning_effort,
                "request_id": record.request_id,
                "response_id": record.response_id,
                "input_checksum_sha256": record.input_checksum_sha256,
                "output_checksum_sha256": record.output_checksum_sha256,
                "claim_count": record.claim_count,
            }
            for record in report.records
        ],
        "issues": [
            {
                "severity": str(issue.severity),
                "code": issue.code,
                "precheck_id": issue.precheck_id,
                "message": issue.message,
            }
            for issue in report.issues
        ],
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _render_issue_diagnostics(issues: tuple[RiskEventPreReviewIssue, ...]) -> list[str]:
    lines: list[str] = []
    for issue in issues:
        diagnostics = issue.diagnostics
        if not diagnostics:
            continue
        attempts = diagnostics.get("attempts")
        if not isinstance(attempts, list):
            continue
        lines.extend(
            [
                f"### {issue.code} / {issue.precheck_id or ''}",
                "",
                "| Attempt | Client request id | Endpoint | Payload bytes | HTTP | "
                "Client | OpenAI request | Exception | Retryable | Elapsed |",
                "|---:|---|---|---:|---:|---|---|---|---|---:|",
            ]
        )
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                continue
            lines.append(
                "| "
                f"{attempt.get('attempt', '')} | "
                f"{_escape_markdown_table(str(attempt.get('client_request_id', '')))} | "
                f"{_escape_markdown_table(str(attempt.get('endpoint_host', '')))} | "
                f"{attempt.get('payload_bytes', '')} | "
                f"{attempt.get('http_status', '')} | "
                f"{_escape_markdown_table(str(attempt.get('http_client') or ''))} | "
                f"{_escape_markdown_table(str(attempt.get('openai_request_id') or ''))} | "
                f"{_escape_markdown_table(_attempt_exception_label(attempt))} | "
                f"{attempt.get('retryable', '')} | "
                f"{attempt.get('elapsed_seconds', '')} |"
            )
        final_attempt = diagnostics.get("final_attempt")
        if isinstance(final_attempt, Mapping) and final_attempt.get("input_checksum_sha256"):
            lines.extend(
                [
                    "",
                    f"- input_checksum_sha256：`{final_attempt['input_checksum_sha256']}`",
                    "",
                ]
            )
    return lines


def _attempt_exception_label(attempt: Mapping[str, Any]) -> str:
    exception_type = str(attempt.get("exception_type") or "")
    if not exception_type:
        return ""
    reason = str(attempt.get("exception_reason") or "")
    errno = attempt.get("errno")
    errno_text = f" errno={errno}" if errno is not None else ""
    return f"{exception_type}{errno_text}: {reason}"


def _dedupe_key(
    llm_record: LlmClaimPrecheckRecord,
    matched_risk_ids: list[str],
    source_span_ref: str,
) -> str:
    return "|".join(
        _unique_nonempty(
            [
                llm_record.source_url,
                ",".join(matched_risk_ids),
                source_span_ref,
            ]
        )
    )


def _safe_id_segment(value: str) -> str:
    normalized = "".join(
        character if character.isalnum() or character in {"_", ".", ":", "-"} else "-"
        for character in value.strip()
    ).strip("-")
    return normalized or "item"


def _official_candidate_content(candidate: OfficialPolicyCandidate) -> str:
    pieces = [
        "官方政策/地缘候选元数据。",
        f"source_id: {candidate.source_id}",
        f"provider: {candidate.provider}",
        f"source_type: {candidate.source_type}",
        f"source_url: {candidate.source_url}",
        f"source_title: {candidate.source_title}",
        (
            "published_at: "
            f"{candidate.published_at.isoformat() if candidate.published_at else ''}"
        ),
        f"captured_at: {candidate.captured_at.isoformat()}",
        f"matched_topics: {', '.join(candidate.matched_topics)}",
        f"matched_risk_ids: {', '.join(candidate.matched_risk_ids)}",
        f"affected_tickers: {', '.join(candidate.affected_tickers)}",
        f"affected_nodes: {', '.join(candidate.affected_nodes)}",
        f"evidence_grade_floor: {candidate.evidence_grade_floor}",
        f"review_questions: {'; '.join(candidate.review_questions)}",
        f"raw_payload_sha256: {candidate.raw_payload_sha256}",
        "请只判断这条候选是否可能构成需要人工复核的政策/地缘风险事件。",
        "如果只是正常公告、无关条目或证据不足，请输出 irrelevant/none，避免增加人工队列。",
    ]
    return "\n".join(piece for piece in pieces if piece)


def _packet_checksum(packet: LlmClaimPrecheckInput) -> str:
    serialized = json.dumps(
        packet.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _official_candidate_batch_checksum(packet_checksums: list[dict[str, str]]) -> str:
    serialized = json.dumps(
        packet_checksums,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _unique_nonempty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


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
        reasoning_effort=row["reasoning_effort"],
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
