from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import DataSourceConfig, DataSourcesConfig

OpenAIReasoningEffort = Literal["none", "minimal", "low", "medium", "high", "xhigh"]

LLM_CLAIM_PREREVIEW_SCHEMA_VERSION = "llm_claim_prereview_queue.v2"
LLM_CLAIM_PREREVIEW_PROMPT_VERSION = "llm_claim_precheck_v1"
DEFAULT_OPENAI_RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_LLM_MODEL = "gpt-5.5-pro"
DEFAULT_OPENAI_REASONING_EFFORT: OpenAIReasoningEffort = "xhigh"
_SUPPORTED_OPENAI_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}

ContentSentLevel = Literal["metadata_only", "short_excerpt", "summary_only", "full_text"]
SourceType = Literal["primary_source", "paid_vendor", "manual_input", "public_convenience"]
ClaimType = Literal[
    "risk_event",
    "thesis_signal",
    "catalyst",
    "fundamental",
    "valuation",
    "supply_chain",
    "macro",
    "other",
]
Novelty = Literal["new", "confirming", "duplicate", "conflicting", "unclear"]
ImpactHorizon = Literal["intraday", "short_term", "medium_term", "long_term", "unclear"]
EvidenceGradeSuggestion = Literal["S", "A", "B", "C", "D", "X"]
ManualReviewStatus = Literal["pending_review"]
LlmSourceType = Literal["llm_extracted"]

_CONTENT_LEVEL_ORDER: dict[str, int] = {
    "metadata_only": 0,
    "summary_only": 1,
    "short_excerpt": 2,
    "full_text": 3,
}


OPENAI_LLM_CLAIM_RESPONSE_FORMAT: dict[str, Any] = {
    "type": "json_schema",
    "name": LLM_CLAIM_PREREVIEW_PROMPT_VERSION,
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["overall_summary_zh", "claims", "prohibited_actions_ack"],
        "properties": {
            "overall_summary_zh": {"type": "string"},
            "prohibited_actions_ack": {"type": "boolean", "const": True},
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "claim_id",
                        "claim_text_zh",
                        "source_span_ref",
                        "affected_tickers",
                        "affected_nodes",
                        "claim_type",
                        "novelty",
                        "impact_horizon",
                        "evidence_grade_suggestion",
                        "confidence",
                        "conflicts_or_uncertainties",
                        "required_review_questions",
                        "risk_event_candidate",
                        "thesis_signal_match",
                        "manual_review_status",
                        "prohibited_actions_ack",
                    ],
                    "properties": {
                        "claim_id": {"type": "string"},
                        "claim_text_zh": {"type": "string"},
                        "source_span_ref": {"type": "string"},
                        "affected_tickers": {"type": "array", "items": {"type": "string"}},
                        "affected_nodes": {"type": "array", "items": {"type": "string"}},
                        "claim_type": {
                            "type": "string",
                            "enum": [
                                "risk_event",
                                "thesis_signal",
                                "catalyst",
                                "fundamental",
                                "valuation",
                                "supply_chain",
                                "macro",
                                "other",
                            ],
                        },
                        "novelty": {
                            "type": "string",
                            "enum": [
                                "new",
                                "confirming",
                                "duplicate",
                                "conflicting",
                                "unclear",
                            ],
                        },
                        "impact_horizon": {
                            "type": "string",
                            "enum": [
                                "intraday",
                                "short_term",
                                "medium_term",
                                "long_term",
                                "unclear",
                            ],
                        },
                        "evidence_grade_suggestion": {
                            "type": "string",
                            "enum": ["S", "A", "B", "C", "D", "X"],
                        },
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "conflicts_or_uncertainties": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "required_review_questions": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string"},
                        },
                        "risk_event_candidate": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "risk_id_candidate",
                                "status_candidate",
                                "level_candidate",
                                "severity_candidate",
                                "probability_candidate",
                                "scope_candidate",
                                "time_sensitivity_candidate",
                                "action_class_candidate",
                                "missing_confirmations",
                                "review_questions",
                            ],
                            "properties": {
                                "risk_id_candidate": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "status_candidate": {
                                    "type": "string",
                                    "enum": [
                                        "none",
                                        "irrelevant",
                                        "candidate",
                                        "watch",
                                        "active_candidate",
                                        "resolved_candidate",
                                    ],
                                },
                                "level_candidate": {
                                    "type": "string",
                                    "enum": ["none", "L1", "L2", "L3"],
                                },
                                "severity_candidate": {
                                    "type": "string",
                                    "enum": [
                                        "none",
                                        "low",
                                        "medium",
                                        "high",
                                        "critical",
                                        "unclear",
                                    ],
                                },
                                "probability_candidate": {
                                    "type": "string",
                                    "enum": ["none", "low", "medium", "high", "unclear"],
                                },
                                "scope_candidate": {
                                    "type": "string",
                                    "enum": [
                                        "none",
                                        "single_ticker",
                                        "node",
                                        "sector",
                                        "market",
                                        "unclear",
                                    ],
                                },
                                "time_sensitivity_candidate": {
                                    "type": "string",
                                    "enum": [
                                        "none",
                                        "intraday",
                                        "short_term",
                                        "medium_term",
                                        "long_term",
                                        "unclear",
                                    ],
                                },
                                "action_class_candidate": {
                                    "type": "string",
                                    "enum": [
                                        "none",
                                        "manual_review",
                                        "score_only_candidate",
                                        "position_gate_candidate",
                                    ],
                                },
                                "missing_confirmations": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "review_questions": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                        "thesis_signal_match": {"type": "array", "items": {"type": "string"}},
                        "manual_review_status": {"type": "string", "const": "pending_review"},
                        "prohibited_actions_ack": {"type": "boolean", "const": True},
                    },
                },
            },
        },
    },
}


class LlmPrecheckIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class SourcePermissionEnvelope(BaseModel):
    provider: str = Field(min_length=1)
    source_type: SourceType
    license_scope: str = "unknown"
    personal_use_only: bool = True
    external_llm_allowed: bool = False
    cache_allowed: bool = False
    redistribution_allowed: bool = False
    content_sent_level: ContentSentLevel = "metadata_only"
    approval_ref: str = ""
    source_id: str = ""
    reviewed_at: date | None = None

    @model_validator(mode="after")
    def validate_permission(self) -> SourcePermissionEnvelope:
        if self.external_llm_allowed and not self.approval_ref:
            raise ValueError("external_llm_allowed=true requires approval_ref")
        if self.external_llm_allowed and self.reviewed_at is None:
            raise ValueError("external_llm_allowed=true requires reviewed_at")
        if self.source_type == "paid_vendor" and self.external_llm_allowed:
            if self.license_scope == "unknown":
                raise ValueError("paid_vendor LLM permission requires explicit license_scope")
            if not self.approval_ref:
                raise ValueError("paid_vendor LLM permission requires approval_ref")
        return self


class LlmClaimPrecheckInput(BaseModel):
    precheck_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    source_id: str = ""
    source_url: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    source_title: str = ""
    published_at: date | None = None
    captured_at: date
    content_text: str = Field(min_length=1)
    content_sent_level: ContentSentLevel = "metadata_only"
    source_permission: SourcePermissionEnvelope | None = None
    notes: str = ""


class RiskEventCandidatePayload(BaseModel):
    risk_id_candidate: list[str] = Field(default_factory=list)
    status_candidate: Literal[
        "none",
        "irrelevant",
        "candidate",
        "watch",
        "active_candidate",
        "resolved_candidate",
    ]
    level_candidate: Literal["none", "L1", "L2", "L3"]
    severity_candidate: Literal["none", "low", "medium", "high", "critical", "unclear"]
    probability_candidate: Literal["none", "low", "medium", "high", "unclear"]
    scope_candidate: Literal["none", "single_ticker", "node", "sector", "market", "unclear"]
    time_sensitivity_candidate: Literal[
        "none",
        "intraday",
        "short_term",
        "medium_term",
        "long_term",
        "unclear",
    ]
    action_class_candidate: Literal[
        "none",
        "manual_review",
        "score_only_candidate",
        "position_gate_candidate",
    ]
    missing_confirmations: list[str] = Field(default_factory=list)
    review_questions: list[str] = Field(default_factory=list)


class LlmExtractedClaim(BaseModel):
    claim_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    claim_text_zh: str = Field(min_length=1)
    source_span_ref: str = Field(min_length=1)
    affected_tickers: list[str] = Field(default_factory=list)
    affected_nodes: list[str] = Field(default_factory=list)
    claim_type: ClaimType
    novelty: Novelty
    impact_horizon: ImpactHorizon
    evidence_grade_suggestion: EvidenceGradeSuggestion
    confidence: float = Field(ge=0, le=1)
    conflicts_or_uncertainties: list[str] = Field(default_factory=list)
    required_review_questions: list[str] = Field(min_length=1)
    risk_event_candidate: RiskEventCandidatePayload
    thesis_signal_match: list[str] = Field(default_factory=list)
    manual_review_status: ManualReviewStatus = "pending_review"
    prohibited_actions_ack: bool

    @model_validator(mode="after")
    def normalize_claim(self) -> LlmExtractedClaim:
        self.affected_tickers = [ticker.upper() for ticker in self.affected_tickers if ticker]
        self.affected_nodes = [node for node in self.affected_nodes if node]
        self.conflicts_or_uncertainties = [
            item for item in self.conflicts_or_uncertainties if item
        ]
        self.required_review_questions = [
            item for item in self.required_review_questions if item
        ]
        if not self.required_review_questions:
            raise ValueError("required_review_questions must contain at least one item")
        if not self.prohibited_actions_ack:
            raise ValueError("prohibited_actions_ack must be true")
        return self

    @property
    def automatic_score_eligible(self) -> bool:
        return False

    @property
    def position_gate_eligible(self) -> bool:
        return False


class OpenAIClaimExtractionOutput(BaseModel):
    overall_summary_zh: str = Field(min_length=1)
    claims: list[LlmExtractedClaim] = Field(default_factory=list)
    prohibited_actions_ack: bool

    @model_validator(mode="after")
    def enforce_boundary(self) -> OpenAIClaimExtractionOutput:
        if not self.prohibited_actions_ack:
            raise ValueError("prohibited_actions_ack must be true")
        return self


class LlmClaimPrecheckRecord(BaseModel):
    precheck_id: str
    source_url: str
    source_name: str
    source_title: str = ""
    published_at: date | None = None
    captured_at: date
    source_permission: SourcePermissionEnvelope
    source_type: LlmSourceType = "llm_extracted"
    manual_review_status: ManualReviewStatus = "pending_review"
    model: str
    reasoning_effort: OpenAIReasoningEffort
    prompt_version: str
    request_id: str
    response_id: str = ""
    client_request_id: str
    request_timestamp: datetime
    input_checksum_sha256: str
    output_checksum_sha256: str
    claim_count: int
    overall_summary_zh: str
    claims: list[LlmExtractedClaim]
    notes: str = ""

    @property
    def automatic_score_eligible(self) -> bool:
        return False

    @property
    def position_gate_eligible(self) -> bool:
        return False


@dataclass(frozen=True)
class LlmPrecheckIssue:
    severity: LlmPrecheckIssueSeverity
    code: str
    message: str
    precheck_id: str | None = None


@dataclass(frozen=True)
class LlmClaimPrecheckReport:
    input_path: Path
    generated_at: datetime
    records: tuple[LlmClaimPrecheckRecord, ...]
    issues: tuple[LlmPrecheckIssue, ...] = field(default_factory=tuple)

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def claim_count(self) -> int:
        return sum(record.claim_count for record in self.records)

    @property
    def pending_review_count(self) -> int:
        return sum(len(record.claims) for record in self.records)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == LlmPrecheckIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == LlmPrecheckIssueSeverity.WARNING)

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
class OpenAIJsonResponse:
    status_code: int
    headers: Mapping[str, str]
    body: dict[str, Any]


HttpPostJson = Callable[
    [str, Mapping[str, str], Mapping[str, Any], float],
    OpenAIJsonResponse,
]


def load_llm_claim_precheck_input(input_path: Path | str) -> LlmClaimPrecheckInput:
    path = Path(input_path)
    raw_text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        raw = yaml.safe_load(raw_text)
    else:
        raw = json.loads(raw_text)
    return LlmClaimPrecheckInput.model_validate(raw)


def run_openai_claim_precheck(
    input_packet: LlmClaimPrecheckInput,
    *,
    api_key: str,
    data_sources: DataSourcesConfig | None = None,
    input_path: Path | str = Path("<memory>"),
    model: str = DEFAULT_OPENAI_LLM_MODEL,
    reasoning_effort: str = DEFAULT_OPENAI_REASONING_EFFORT,
    endpoint: str = DEFAULT_OPENAI_RESPONSES_ENDPOINT,
    timeout_seconds: float = 60.0,
    generated_at: datetime | None = None,
    http_post_json: HttpPostJson | None = None,
) -> LlmClaimPrecheckReport:
    timestamp = generated_at or datetime.now(tz=UTC)
    issues: list[LlmPrecheckIssue] = []
    try:
        permission = resolve_source_permission(input_packet, data_sources=data_sources)
        _check_send_allowed(input_packet, permission)
    except ValueError as exc:
        return LlmClaimPrecheckReport(
            input_path=Path(input_path),
            generated_at=timestamp,
            records=(),
            issues=(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.ERROR,
                    code="llm_precheck_permission_denied",
                    precheck_id=input_packet.precheck_id,
                    message=str(exc),
                ),
            ),
        )

    if not api_key:
        return LlmClaimPrecheckReport(
            input_path=Path(input_path),
            generated_at=timestamp,
            records=(),
            issues=(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.ERROR,
                    code="openai_api_key_missing",
                    precheck_id=input_packet.precheck_id,
                    message="未找到 OpenAI API key 环境变量，已停止 LLM 预审。",
                ),
            ),
        )

    try:
        normalized_reasoning_effort = _validate_reasoning_effort(reasoning_effort)
    except ValueError as exc:
        return LlmClaimPrecheckReport(
            input_path=Path(input_path),
            generated_at=timestamp,
            records=(),
            issues=(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.ERROR,
                    code="openai_reasoning_effort_invalid",
                    precheck_id=input_packet.precheck_id,
                    message=str(exc),
                ),
            ),
        )

    client_request_id = f"aits-llm-precheck-{uuid4()}"
    request_payload = _build_openai_request_payload(
        input_packet=input_packet,
        permission=permission,
        model=model,
        reasoning_effort=normalized_reasoning_effort,
    )
    input_checksum = _sha256_json(
        {
            "prompt_version": LLM_CLAIM_PREREVIEW_PROMPT_VERSION,
            "source": _source_payload(input_packet, permission),
            "content_text": input_packet.content_text,
            "request_payload": request_payload,
        }
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Client-Request-Id": client_request_id,
    }
    post_json = http_post_json or _post_json_stdlib
    response = post_json(endpoint, headers, request_payload, timeout_seconds)
    if response.status_code >= 400:
        return LlmClaimPrecheckReport(
            input_path=Path(input_path),
            generated_at=timestamp,
            records=(),
            issues=(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.ERROR,
                    code="openai_responses_api_error",
                    precheck_id=input_packet.precheck_id,
                    message=(
                        f"OpenAI Responses API 返回 HTTP {response.status_code}，"
                        "已停止写入队列。"
                    ),
                ),
            ),
        )

    try:
        output_text = _extract_output_text(response.body)
        output_checksum = sha256(output_text.encode("utf-8")).hexdigest()
        parsed = OpenAIClaimExtractionOutput.model_validate_json(output_text)
    except (ValueError, ValidationError) as exc:
        return LlmClaimPrecheckReport(
            input_path=Path(input_path),
            generated_at=timestamp,
            records=(),
            issues=(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.ERROR,
                    code="openai_structured_output_invalid",
                    precheck_id=input_packet.precheck_id,
                    message=_error_message(exc),
                ),
            ),
        )

    request_id = _response_header(response.headers, "x-request-id") or str(
        response.body.get("id", "")
    )
    response_id = str(response.body.get("id", ""))
    record = LlmClaimPrecheckRecord(
        precheck_id=input_packet.precheck_id,
        source_url=input_packet.source_url,
        source_name=input_packet.source_name,
        source_title=input_packet.source_title,
        published_at=input_packet.published_at,
        captured_at=input_packet.captured_at,
        source_permission=permission,
        model=model,
        reasoning_effort=normalized_reasoning_effort,
        prompt_version=LLM_CLAIM_PREREVIEW_PROMPT_VERSION,
        request_id=request_id or client_request_id,
        response_id=response_id,
        client_request_id=client_request_id,
        request_timestamp=timestamp,
        input_checksum_sha256=input_checksum,
        output_checksum_sha256=output_checksum,
        claim_count=len(parsed.claims),
        overall_summary_zh=parsed.overall_summary_zh,
        claims=parsed.claims,
        notes=input_packet.notes,
    )
    issues.extend(_claim_review_warnings(record))
    return LlmClaimPrecheckReport(
        input_path=Path(input_path),
        generated_at=timestamp,
        records=(record,),
        issues=tuple(issues),
    )


def resolve_source_permission(
    input_packet: LlmClaimPrecheckInput,
    *,
    data_sources: DataSourcesConfig | None,
) -> SourcePermissionEnvelope:
    if input_packet.source_permission is not None:
        return input_packet.source_permission
    if not input_packet.source_id:
        raise ValueError("必须提供 source_id 或 source_permission envelope。")
    if data_sources is None:
        raise ValueError("使用 source_id 时必须提供 data source catalog。")

    source = _source_by_id(data_sources, input_packet.source_id)
    permission = source.llm_permission
    _check_content_level_allowed(
        requested=input_packet.content_sent_level,
        maximum=permission.max_content_sent_level,
        source_id=source.source_id,
    )
    return SourcePermissionEnvelope(
        provider=source.provider,
        source_type=source.source_type,
        license_scope=permission.license_scope,
        personal_use_only=permission.personal_use_only,
        external_llm_allowed=permission.external_llm_allowed,
        cache_allowed=permission.cache_allowed,
        redistribution_allowed=permission.redistribution_allowed,
        content_sent_level=input_packet.content_sent_level,
        approval_ref=permission.approval_ref,
        source_id=source.source_id,
        reviewed_at=permission.reviewed_at,
    )


def render_llm_claim_precheck_report(report: LlmClaimPrecheckReport) -> str:
    lines = [
        "# LLM 证据预审报告",
        "",
        f"- 状态：{report.status}",
        f"- 输入路径：`{report.input_path}`",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 预审记录数：{report.record_count}",
        f"- 待复核 claim：{report.pending_review_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 预审队列",
        "",
    ]
    if report.records:
        lines.extend(
            [
                "| Precheck | Provider | Source | Model | Reasoning | Request | Content | "
                "Claims | Policy |",
                "|---|---|---|---|---|---|---|---:|---|",
            ]
        )
        for record in report.records:
            permission = record.source_permission
            lines.append(
                "| "
                f"{record.precheck_id} | "
                f"{_escape_markdown_table(permission.provider)} | "
                f"{_escape_markdown_table(record.source_name)} | "
                f"{_escape_markdown_table(record.model)} | "
                f"{record.reasoning_effort} | "
                f"{_escape_markdown_table(record.request_id)} | "
                f"{permission.content_sent_level} | "
                f"{record.claim_count} | "
                "llm_extracted / pending_review；不得评分/不得触发仓位闸门 |"
            )
        lines.extend(["", "## Claim 摘要", ""])
        lines.extend(
            [
                "| Claim | Type | Tickers | Nodes | Confidence | Review |",
                "|---|---|---|---|---:|---|",
            ]
        )
        for record in report.records:
            for claim in record.claims:
                lines.append(
                    "| "
                    f"{claim.claim_id} | "
                    f"{claim.claim_type} | "
                    f"{', '.join(claim.affected_tickers)} | "
                    f"{_escape_markdown_table(', '.join(claim.affected_nodes))} | "
                    f"{claim.confidence:.2f} | "
                    "pending_review |"
                )
    else:
        lines.append("未写入可复核记录。")

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Precheck | 说明 |", "|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.precheck_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本命令使用 OpenAI Responses API 固定 JSON schema 做结构化预审，"
            "请求默认 `store=false`。",
            "- API key 只从环境变量读取，不写入报告、队列或错误信息。",
            "- provider 授权未知或 `external_llm_allowed=false` 时 fail closed，不发起 API 请求。",
            "- 输出只作为 `llm_extracted` / `pending_review` 证据分类结果，"
            "不进入自动评分、thesis 状态迁移、仓位闸门或交易建议。",
            "- 本地队列保存 source permission、request id、model、reasoning effort、"
            "prompt version、输入/输出 checksum 和结构化 claim，不保存未授权全文。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_llm_claim_precheck_report(
    report: LlmClaimPrecheckReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_llm_claim_precheck_report(report), encoding="utf-8")
    return output_path


def write_llm_claim_prereview_queue(
    report: LlmClaimPrecheckReport,
    output_path: Path,
    *,
    generated_at: datetime | None = None,
) -> Path:
    if not report.passed:
        raise ValueError("LLM 预审存在错误，不能写入待复核队列。")
    timestamp = generated_at or report.generated_at
    payload = {
        "schema_version": LLM_CLAIM_PREREVIEW_SCHEMA_VERSION,
        "generated_at": timestamp.isoformat(),
        "source_input_path": str(report.input_path),
        "record_count": report.record_count,
        "claim_count": report.claim_count,
        "records": [record.model_dump(mode="json") for record in report.records],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def default_llm_claim_precheck_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"llm_claim_prereview_{as_of.isoformat()}.md"


def _source_by_id(data_sources: DataSourcesConfig, source_id: str) -> DataSourceConfig:
    for source in data_sources.sources:
        if source.source_id == source_id:
            return source
    raise ValueError(f"data source catalog 未找到 source_id：{source_id}")


def _check_content_level_allowed(
    *,
    requested: ContentSentLevel,
    maximum: ContentSentLevel,
    source_id: str,
) -> None:
    if _CONTENT_LEVEL_ORDER[requested] > _CONTENT_LEVEL_ORDER[maximum]:
        raise ValueError(
            f"{source_id} 只允许发送到 {maximum}，本次请求为 {requested}。"
        )


def _check_send_allowed(
    input_packet: LlmClaimPrecheckInput,
    permission: SourcePermissionEnvelope,
) -> None:
    if not permission.external_llm_allowed:
        provider = permission.provider
        raise ValueError(
            f"{provider} 的 external_llm_allowed=false，不能发送给外部 LLM API。"
        )
    if permission.source_type == "paid_vendor" and permission.content_sent_level == "full_text":
        if not permission.approval_ref:
            raise ValueError("paid_vendor full_text 发送必须记录 approval_ref。")
    if permission.content_sent_level != input_packet.content_sent_level:
        raise ValueError("source_permission.content_sent_level 与输入请求不一致。")


def _build_openai_request_payload(
    *,
    input_packet: LlmClaimPrecheckInput,
    permission: SourcePermissionEnvelope,
    model: str,
    reasoning_effort: str,
) -> dict[str, Any]:
    return {
        "model": model,
        "reasoning": {"effort": reasoning_effort},
        "store": False,
        "input": [
            {
                "role": "system",
                "content": (
                    "你是投资决策支持系统的证据分类器。只能抽取和分类事实断言，"
                    "不得给出看多、看空、买入、卖出、加仓、减仓或仓位建议。"
                    "所有输出必须保持 pending_review，不能写成正式评分或仓位闸门输入。"
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    _source_payload(input_packet, permission),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            },
        ],
        "text": {"format": OPENAI_LLM_CLAIM_RESPONSE_FORMAT},
    }


def _validate_reasoning_effort(reasoning_effort: str) -> str:
    normalized = reasoning_effort.strip()
    if normalized not in _SUPPORTED_OPENAI_REASONING_EFFORTS:
        supported = ", ".join(sorted(_SUPPORTED_OPENAI_REASONING_EFFORTS))
        raise ValueError(
            f"OpenAI reasoning.effort={reasoning_effort!r} 不受支持；允许值：{supported}。"
        )
    return normalized


def _source_payload(
    input_packet: LlmClaimPrecheckInput,
    permission: SourcePermissionEnvelope,
) -> dict[str, Any]:
    return {
        "precheck_id": input_packet.precheck_id,
        "source_url": input_packet.source_url,
        "source_name": input_packet.source_name,
        "source_title": input_packet.source_title,
        "published_at": None
        if input_packet.published_at is None
        else input_packet.published_at.isoformat(),
        "captured_at": input_packet.captured_at.isoformat(),
        "source_permission": permission.model_dump(mode="json"),
        "content_sent_level": input_packet.content_sent_level,
        "content_text": input_packet.content_text,
        "output_language": "zh",
        "required_manual_review_status": "pending_review",
        "prohibited_actions": [
            "直接输出看多/看空",
            "直接输出买入/卖出/加仓/减仓",
            "直接改变评分",
            "直接触发仓位闸门",
        ],
    }


def _claim_review_warnings(record: LlmClaimPrecheckRecord) -> list[LlmPrecheckIssue]:
    issues: list[LlmPrecheckIssue] = []
    for claim in record.claims:
        candidate = claim.risk_event_candidate
        if (
            candidate.level_candidate in {"L2", "L3"}
            or candidate.action_class_candidate == "position_gate_candidate"
            or candidate.status_candidate == "active_candidate"
        ):
            issues.append(
                LlmPrecheckIssue(
                    severity=LlmPrecheckIssueSeverity.WARNING,
                    code="high_impact_llm_claim_requires_human_confirmation",
                    precheck_id=record.precheck_id,
                    message=(
                        f"{claim.claim_id} 是高影响或仓位闸门候选，只能进入人工复核。"
                    ),
                )
            )
    return issues


def _post_json_stdlib(
    url: str,
    headers: Mapping[str, str],
    payload: Mapping[str, Any],
    timeout_seconds: float,
) -> OpenAIJsonResponse:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=dict(headers),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
            response_headers = dict(response.headers.items())
            return OpenAIJsonResponse(
                status_code=response.status,
                headers=response_headers,
                body=json.loads(raw_body),
            )
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            body = {"error": {"message": raw_body}}
        return OpenAIJsonResponse(
            status_code=exc.code,
            headers=dict(exc.headers.items()),
            body=body,
        )


def _extract_output_text(body: Mapping[str, Any]) -> str:
    output_text = body.get("output_text")
    if isinstance(output_text, str) and output_text:
        return output_text
    output = body.get("output")
    if not isinstance(output, list):
        raise ValueError("OpenAI response missing output text")
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") in {"output_text", "text"} and isinstance(part.get("text"), str):
                return str(part["text"])
            refusal = part.get("refusal")
            if isinstance(refusal, str) and refusal:
                raise ValueError(f"OpenAI structured output refusal: {refusal}")
    raise ValueError("OpenAI response missing output text")


def _response_header(headers: Mapping[str, str], name: str) -> str:
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return ""


def _sha256_json(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(serialized.encode("utf-8")).hexdigest()


def _error_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        first_error: Any = exc.errors()[0] if exc.errors() else {}
        location = ".".join(str(part) for part in first_error.get("loc", ()))
        message = str(first_error.get("msg", "schema validation failed"))
        return f"{location}: {message}" if location else message
    return str(exc)


def _severity_label(severity: LlmPrecheckIssueSeverity) -> str:
    if severity == LlmPrecheckIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
