from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import RiskEventsConfig
from ai_trading_system.risk_event_prereview import (
    RISK_EVENT_PREREVIEW_SCHEMA_VERSION,
    RiskEventPreReviewRecord,
)
from ai_trading_system.risk_events import (
    RiskEventEvidenceSource,
    RiskEventOccurrence,
    RiskEventReviewAttestation,
    RiskEventReviewAttestationSource,
)

LLM_FORMAL_ASSESSMENT_SCHEMA_VERSION = "risk_event_llm_formal_assessment.v1"


class LlmFormalAssessmentIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class LlmFormalAssessmentIssue:
    severity: LlmFormalAssessmentIssueSeverity
    code: str
    message: str
    precheck_id: str | None = None


@dataclass(frozen=True)
class LlmFormalAssessmentReport:
    input_path: Path
    input_checksum_sha256: str
    as_of: date
    generated_at: datetime
    records: tuple[RiskEventPreReviewRecord, ...]
    occurrences: tuple[RiskEventOccurrence, ...]
    attestation: RiskEventReviewAttestation | None
    issues: tuple[LlmFormalAssessmentIssue, ...] = field(default_factory=tuple)

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def occurrence_count(self) -> int:
        return len(self.occurrences)

    @property
    def active_occurrence_count(self) -> int:
        return sum(1 for occurrence in self.occurrences if occurrence.status == "active")

    @property
    def watch_occurrence_count(self) -> int:
        return sum(1 for occurrence in self.occurrences if occurrence.status == "watch")

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == LlmFormalAssessmentIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == LlmFormalAssessmentIssueSeverity.WARNING
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


def build_llm_formal_assessment_report(
    queue_path: Path,
    *,
    as_of: date,
    risk_events: RiskEventsConfig | None = None,
    include_attestation: bool = True,
    next_review_days: int = 1,
    min_confidence: float = 0.0,
    generated_at: datetime | None = None,
) -> LlmFormalAssessmentReport:
    if next_review_days < 0:
        raise ValueError("next_review_days must be non-negative")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    input_path = Path(queue_path)
    generated_time = generated_at or datetime.now(tz=UTC)
    issues: list[LlmFormalAssessmentIssue] = []
    raw_bytes = b""
    payload: dict[str, Any] = {}
    try:
        raw_bytes = input_path.read_bytes()
        loaded = json.loads(raw_bytes.decode("utf-8"))
        if isinstance(loaded, dict):
            payload = loaded
        else:
            issues.append(
                LlmFormalAssessmentIssue(
                    severity=LlmFormalAssessmentIssueSeverity.ERROR,
                    code="llm_formal_assessment_invalid_queue",
                    message="风险事件预审队列 JSON 顶层必须是 object。",
                )
            )
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        issues.append(
            LlmFormalAssessmentIssue(
                severity=LlmFormalAssessmentIssueSeverity.ERROR,
                code="llm_formal_assessment_queue_unreadable",
                message=f"无法读取风险事件预审队列：{exc}",
            )
        )

    checksum = hashlib.sha256(raw_bytes).hexdigest() if raw_bytes else ""
    if issues:
        return LlmFormalAssessmentReport(
            input_path=input_path,
            input_checksum_sha256=checksum,
            as_of=as_of,
            generated_at=generated_time,
            records=(),
            occurrences=(),
            attestation=None,
            issues=tuple(issues),
        )

    if payload.get("schema_version") != RISK_EVENT_PREREVIEW_SCHEMA_VERSION:
        issues.append(
            LlmFormalAssessmentIssue(
                severity=LlmFormalAssessmentIssueSeverity.ERROR,
                code="llm_formal_assessment_unknown_queue_schema",
                message=(
                    "风险事件预审队列 schema_version 不匹配："
                    f"{payload.get('schema_version')}"
                ),
            )
        )
    if payload.get("source_kind") != "openai_live":
        issues.append(
            LlmFormalAssessmentIssue(
                severity=LlmFormalAssessmentIssueSeverity.ERROR,
                code="llm_formal_assessment_requires_openai_live_queue",
                message="只有 openai_live 风险事件预审队列可以转为 LLM 正式评估。",
            )
        )

    known_risk_ids = (
        {rule.event_id for rule in risk_events.event_rules}
        if risk_events is not None
        else set()
    )
    records: list[RiskEventPreReviewRecord] = []
    occurrences: list[RiskEventOccurrence] = []
    for index, raw_record in enumerate(payload.get("records", []), start=1):
        try:
            record = RiskEventPreReviewRecord.model_validate(raw_record)
        except ValueError as exc:
            issues.append(
                LlmFormalAssessmentIssue(
                    severity=LlmFormalAssessmentIssueSeverity.ERROR,
                    code="llm_formal_assessment_record_invalid",
                    message=str(exc),
                )
            )
            continue
        if record.confidence < min_confidence:
            issues.append(
                LlmFormalAssessmentIssue(
                    severity=LlmFormalAssessmentIssueSeverity.WARNING,
                    code="llm_formal_assessment_record_below_confidence_threshold",
                    precheck_id=record.precheck_id,
                    message=(
                        f"{record.precheck_id} confidence={record.confidence:.2f} "
                        f"低于阈值 {min_confidence:.2f}，未写入正式 occurrence。"
                    ),
                )
            )
            continue
        event_id = _select_event_id(record, known_risk_ids)
        if not event_id:
            issues.append(
                LlmFormalAssessmentIssue(
                    severity=LlmFormalAssessmentIssueSeverity.WARNING,
                    code="llm_formal_assessment_record_without_risk_id",
                    precheck_id=record.precheck_id,
                    message="LLM 预审记录缺少 matched_risk_ids，未写入正式 occurrence。",
                )
            )
            continue
        records.append(record)
        occurrence = _occurrence_from_record(
            record=record,
            event_id=event_id,
            as_of=as_of,
            next_review_due=as_of + timedelta(days=next_review_days),
            index=index,
        )
        occurrences.append(occurrence)
        if record.level_suggestion in {"L2", "L3"}:
            issues.append(
                LlmFormalAssessmentIssue(
                    severity=LlmFormalAssessmentIssueSeverity.WARNING,
                    code="llm_formal_high_impact_assessment_without_human_review",
                    precheck_id=record.precheck_id,
                    message=(
                        f"{record.precheck_id} 为 {record.level_suggestion} LLM formal "
                        "评估；owner 已允许跳过人工复核，但必须在报告中显式标注。"
                    ),
                )
            )

    attestation = (
        _build_llm_formal_attestation(
            records=tuple(records),
            as_of=as_of,
            next_review_due=as_of + timedelta(days=next_review_days),
            input_path=input_path,
            checksum_sha256=checksum,
            source_input_path=str(payload.get("source_input_path") or ""),
            source_input_checksum_sha256=str(
                payload.get("source_input_checksum_sha256") or ""
            ),
        )
        if include_attestation
        else None
    )
    return LlmFormalAssessmentReport(
        input_path=input_path,
        input_checksum_sha256=checksum,
        as_of=as_of,
        generated_at=generated_time,
        records=tuple(records),
        occurrences=tuple(sorted(occurrences, key=lambda item: item.occurrence_id)),
        attestation=attestation,
        issues=tuple(issues),
    )


def write_llm_formal_assessment_outputs(
    report: LlmFormalAssessmentReport,
    output_dir: Path,
    *,
    overwrite: bool = False,
) -> tuple[Path, ...]:
    if not report.passed:
        raise ValueError("LLM formal assessment report has errors")
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for occurrence in report.occurrences:
        output_path = output_dir / f"{occurrence.occurrence_id}.yaml"
        _write_yaml_if_allowed(
            output_path,
            occurrence.model_dump(mode="json", exclude_none=False),
            overwrite=overwrite,
        )
        paths.append(output_path)
    if report.attestation is not None:
        output_path = output_dir / f"{report.attestation.attestation_id}.yaml"
        _write_yaml_if_allowed(
            output_path,
            {"review_attestation": report.attestation.model_dump(mode="json")},
            overwrite=overwrite,
        )
        paths.append(output_path)
    return tuple(paths)


def write_llm_formal_assessment_report(
    report: LlmFormalAssessmentReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_llm_formal_assessment_report(report), encoding="utf-8")
    return output_path


def default_llm_formal_assessment_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_event_llm_formal_assessment_{as_of.isoformat()}.md"


def render_llm_formal_assessment_report(report: LlmFormalAssessmentReport) -> str:
    lines = [
        "# 风险事件 LLM 正式评估导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 输入队列：`{report.input_path}`",
        f"- 输入 SHA256：`{report.input_checksum_sha256}`",
        f"- LLM 预审记录数：{report.record_count}",
        f"- 写入 occurrence 数：{report.occurrence_count}",
        f"- active occurrence 数：{report.active_occurrence_count}",
        f"- watch occurrence 数：{report.watch_occurrence_count}",
        f"- 写入 LLM formal attestation：{'是' if report.attestation else '否'}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 方法边界",
        "",
        "- 本报告把 LLM 预审结果作为正式风险评估输入，但不是人工复核。",
        "- `reviewer` 使用 `llm_formal_assessment:<model>`，不得解释为人工 reviewer。",
        "- LLM formal evidence 默认最高按 B 级处理，可进入普通评分，但不能单独触发 position gate。",
        "- `watch/candidate` 默认写入 watch；只有 `active_candidate` 写入 active。",
        "",
        "## 写入记录",
        "",
    ]
    if report.occurrences:
        lines.extend(
            [
                "| Occurrence | Event | Status | Evidence | Action | Reviewer | Summary |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for occurrence in report.occurrences:
            lines.append(
                "| "
                f"{occurrence.occurrence_id} | "
                f"{occurrence.event_id} | "
                f"{occurrence.status} | "
                f"{occurrence.evidence_grade} | "
                f"{occurrence.action_class} | "
                f"{_escape_markdown_table(occurrence.reviewer)} | "
                f"{_escape_markdown_table(occurrence.summary)[:180]} |"
            )
    else:
        lines.append("未写入正式 occurrence。")

    lines.extend(["", "## 问题", ""])
    if report.issues:
        lines.extend(["| 级别 | Code | Precheck | 说明 |", "|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"`{issue.code}` | "
                f"{issue.precheck_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    else:
        lines.append("未发现问题。")
    return "\n".join(lines) + "\n"


def _occurrence_from_record(
    *,
    record: RiskEventPreReviewRecord,
    event_id: str,
    as_of: date,
    next_review_due: date,
    index: int,
) -> RiskEventOccurrence:
    status = _occurrence_status(record.status_suggestion)
    triggered_at = record.published_at or record.captured_at
    action_class = _action_class(record, status)
    occurrence_id = _occurrence_id(event_id, as_of, record.precheck_id, index)
    notes = (
        "llm_formal_assessment=true; human_review_skipped_by_owner_decision=true; "
        f"precheck_id={record.precheck_id}; request_id={record.request_id}; "
        f"response_id={record.response_id}; model={record.model}; "
        f"reasoning_effort={record.reasoning_effort}; "
        f"status_suggestion={record.status_suggestion}; "
        f"level_suggestion={record.level_suggestion}; confidence={record.confidence:.2f}; "
        f"input_checksum={record.input_checksum_sha256}; "
        f"output_checksum={record.output_checksum_sha256}"
    )
    return RiskEventOccurrence(
        occurrence_id=occurrence_id,
        event_id=event_id,
        status=status,
        triggered_at=triggered_at,
        last_confirmed_at=min(record.captured_at, as_of),
        resolved_at=record.captured_at if status in {"resolved", "dismissed"} else None,
        evidence_grade="B",
        severity=_severity(record),
        probability=_probability(record, status),
        scope=_scope(record),
        time_sensitivity=_time_sensitivity(record),
        reversibility="unknown",
        action_class=action_class,
        lifecycle_state=_lifecycle_state(record, status, action_class),
        dedup_group=record.dedupe_key or event_id,
        primary_channel="llm_formal_assessment",
        used_in_alpha=False,
        used_in_gate=False,
        decay_half_life_days=3,
        expiry_time=next_review_due,
        reviewer=f"llm_formal_assessment:{record.model}",
        reviewed_at=min(record.request_timestamp.date(), as_of),
        review_decision=_review_decision(status),
        rationale=(
            "Owner 决策允许本阶段以 LLM 复核结果作为正式风险评估；"
            f"LLM 输出 status={record.status_suggestion}, "
            f"level={record.level_suggestion}, confidence={record.confidence:.2f}。"
        ),
        next_review_due=next_review_due,
        evidence_sources=_evidence_sources(record),
        summary=record.raw_summary,
        notes=notes,
    )


def _build_llm_formal_attestation(
    *,
    records: tuple[RiskEventPreReviewRecord, ...],
    as_of: date,
    next_review_due: date,
    input_path: Path,
    checksum_sha256: str,
    source_input_path: str = "",
    source_input_checksum_sha256: str = "",
) -> RiskEventReviewAttestation:
    checked_sources: list[RiskEventReviewAttestationSource] = [
        RiskEventReviewAttestationSource(
            source_name="OpenAI risk_event_prereview_queue formal assessment",
            source_type="llm_extracted",
            source_url=str(input_path),
            captured_at=as_of,
            notes=f"queue_checksum_sha256={checksum_sha256}",
        )
    ]
    if source_input_path:
        checked_sources.append(
            RiskEventReviewAttestationSource(
                source_name="official_policy_source_candidates_from_primary_sources",
                source_type="primary_source",
                source_url=source_input_path,
                captured_at=as_of,
                notes=(
                    "candidate_set_used_by_llm_formal_assessment=true; "
                    f"source_input_checksum_sha256={source_input_checksum_sha256}"
                ),
            )
        )
    for record in records[:50]:
        checked_sources.append(
            RiskEventReviewAttestationSource(
                source_name=record.source_name,
                source_type=record.original_source_type,
                source_url=record.source_url,
                captured_at=record.captured_at,
                notes=f"precheck_id={record.precheck_id}; request_id={record.request_id}",
            )
        )
    return RiskEventReviewAttestation(
        attestation_id=f"llm_formal_risk_event_assessment_{as_of.isoformat()}",
        review_date=as_of,
        coverage_start=as_of,
        coverage_end=as_of,
        reviewer="llm_formal_assessment",
        reviewed_at=as_of,
        review_decision="confirmed_no_unrecorded_material_events",
        rationale=(
            "LLM formal assessment 覆盖本次风险事件预审队列中的高优先级官方候选；"
            "该声明不是人工全量复核，仅表示本队列没有额外 active_candidate 被遗漏。"
        ),
        next_review_due=next_review_due,
        review_scope=[
            "llm_triaged_high_priority_official_candidates",
            "policy_event_occurrences",
            "geopolitical_event_occurrences",
        ],
        checked_sources=checked_sources,
        notes=(
            "human_review_skipped_by_owner_decision=true; "
            f"record_count={len(records)}; queue_checksum_sha256={checksum_sha256}"
        ),
    )


def _evidence_sources(record: RiskEventPreReviewRecord) -> list[RiskEventEvidenceSource]:
    return [
        RiskEventEvidenceSource(
            source_name=record.source_name,
            source_type=record.original_source_type,
            source_url=record.source_url,
            published_at=record.published_at,
            captured_at=record.captured_at,
            notes=f"original official source metadata; precheck_id={record.precheck_id}",
        ),
        RiskEventEvidenceSource(
            source_name=f"OpenAI {record.model} risk event formal assessment",
            source_type="llm_extracted",
            source_url=record.source_url,
            published_at=record.request_timestamp.date(),
            captured_at=record.request_timestamp.date(),
            notes=(
                f"request_id={record.request_id}; response_id={record.response_id}; "
                f"reasoning_effort={record.reasoning_effort}; "
                f"input_checksum={record.input_checksum_sha256}; "
                f"output_checksum={record.output_checksum_sha256}"
            ),
        ),
    ]


def _select_event_id(record: RiskEventPreReviewRecord, known_risk_ids: set[str]) -> str:
    if known_risk_ids:
        for risk_id in record.matched_risk_ids:
            if risk_id in known_risk_ids:
                return risk_id
    return record.matched_risk_ids[0] if record.matched_risk_ids else ""


def _occurrence_status(status_suggestion: str) -> str:
    if status_suggestion == "active_candidate":
        return "active"
    if status_suggestion == "resolved_candidate":
        return "resolved"
    if status_suggestion == "irrelevant":
        return "dismissed"
    return "watch"


def _action_class(record: RiskEventPreReviewRecord, status: str) -> str:
    if status != "active":
        return "manual_review" if record.level_suggestion in {"L2", "L3"} else "monitor_only"
    if record.level_suggestion in {"L2", "L3"}:
        return "score_eligible"
    return "monitor_only"


def _lifecycle_state(
    record: RiskEventPreReviewRecord,
    status: str,
    action_class: str,
) -> str:
    if status == "active":
        return "confirmed_medium" if action_class == "score_eligible" else "confirmed_low"
    if status == "watch":
        return "pending_review"
    if status == "resolved":
        return "resolved"
    if record.status_suggestion == "irrelevant":
        return "rejected"
    return "pending_review"


def _review_decision(status: str) -> str:
    if status == "active":
        return "confirmed_active"
    if status == "resolved":
        return "confirmed_resolved"
    if status == "dismissed":
        return "dismissed"
    return "confirmed_watch"


def _severity(record: RiskEventPreReviewRecord) -> str:
    if record.level_suggestion == "L3":
        return "critical"
    if record.level_suggestion == "L2":
        return "high"
    if record.level_suggestion == "L1":
        return "low"
    return "unknown"


def _probability(record: RiskEventPreReviewRecord, status: str) -> str:
    if status == "active":
        return "confirmed"
    if record.confidence >= 0.75:
        return "high"
    if record.confidence >= 0.5:
        return "medium"
    return "low"


def _scope(record: RiskEventPreReviewRecord) -> str:
    if len(record.affected_tickers) == 1:
        return "single_ticker"
    if record.affected_nodes:
        return "industry_chain_node"
    if record.affected_tickers:
        return "ai_bucket"
    return "unknown"


def _time_sensitivity(record: RiskEventPreReviewRecord) -> str:
    if record.status_suggestion == "active_candidate":
        return "high"
    if record.level_suggestion == "L3":
        return "high"
    if record.level_suggestion == "L2":
        return "medium"
    return "low"


def _occurrence_id(event_id: str, as_of: date, precheck_id: str, index: int) -> str:
    digest = hashlib.sha256(precheck_id.encode("utf-8")).hexdigest()[:10]
    return f"llm_formal_{_safe_token(event_id)}_{as_of.strftime('%Y%m%d')}_{index}_{digest}"


def _safe_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")
    return token or "record"


def _write_yaml_if_allowed(output_path: Path, payload: Any, *, overwrite: bool) -> None:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"输出文件已存在，未覆盖：{output_path}")
    output_path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
