from __future__ import annotations

# HTML/CSS source lines remain readable as one semantic declaration.
# ruff: noqa: E501
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html import escape
from html.parser import HTMLParser
from pathlib import Path

from ai_trading_system.atlas.cited_query import answer_cited_query
from ai_trading_system.atlas.cited_query_validation import (
    CitedQueryValidationResult,
    load_validated_diff_payloads,
    load_validated_snapshot_payload,
    validate_serialized_cited_query_response,
)
from ai_trading_system.atlas.page_effectiveness import (
    build_page_effectiveness_manifest,
    validate_page_effectiveness_manifest,
    write_page_effectiveness_sidecars,
)
from ai_trading_system.atlas.qqq_options_projection import (
    QQQOptionsProjectionValidation,
    build_qqq_options_projection,
    load_qqq_options_projection_policy,
    validate_qqq_options_projection,
)
from ai_trading_system.atlas.reader_accessibility_validation import (
    validate_reader_accessibility,
)
from ai_trading_system.atlas.reader_state_projection import (
    load_reader_state_semantics,
    project_reader_state,
)
from ai_trading_system.atlas.reader_terminology_projection import (
    ReaderTerminologyPolicy,
    load_reader_terminology_policy,
    project_reader_text,
)
from ai_trading_system.atlas.rendered_term_inventory import build_rendered_term_inventory
from ai_trading_system.atlas.status_explanation_projection import (
    StatusExplanationProjectionValidation,
    load_status_explanation_authority_policy,
    project_status_explanations,
    validate_status_explanation_bundle,
)
from ai_trading_system.atlas.work_progress_projection import (
    WorkProgressProjectionValidation,
    load_work_progress_authority_policy,
    project_work_progress,
    validate_work_progress_bundle,
    work_progress_validation_json_bytes,
)
from ai_trading_system.contracts.strategy_research_cited_query import (
    CITED_QUERY_QUESTION_CATALOG,
    CitedQueryAnswerStatus,
    CitedQueryCitation,
    CitedQueryQuestionId,
    StrategyResearchCitedQueryRequest,
    StrategyResearchCitedQueryResponse,
)
from ai_trading_system.contracts.strategy_research_explorer import (
    AssertionKind,
    AttributionDirection,
    ResearchAttribution,
    ResearchResultCard,
)
from ai_trading_system.contracts.strategy_research_page_effectiveness import (
    PageAcceptanceRecord,
    PageAcceptanceStatus,
    PageAcceptanceTrack,
    PageArtifactIdentity,
    PageFreshnessStatus,
    PageTaskCoverage,
    StrategyResearchPageEffectivenessManifest,
    page_task_identity_sort_key,
)
from ai_trading_system.contracts.strategy_research_qqq_options_projection import (
    QQQOptionsProjectionCard,
    StrategyResearchQQQOptionsProjectionBundle,
)
from ai_trading_system.contracts.strategy_research_reader_projection import (
    ReaderCausalEdgeKind,
    ReaderCausalNodeKind,
    StrategyResearchReaderProjectionContract,
)
from ai_trading_system.contracts.strategy_research_reader_state import (
    ReaderChangeKind,
    ReaderStateProjection,
)
from ai_trading_system.contracts.strategy_research_reader_terminology import (
    ReaderTermDefinition,
)
from ai_trading_system.contracts.strategy_research_status_explanation import (
    ATLAS_STATUS_EXPLANATION_STAGE_IDS,
    CitedExplanationFact,
    ExplanationFactKind,
    ExplanationTransitionCondition,
    ExplanationValueState,
    StatusExplanationRecord,
    StrategyResearchStatusExplanationBundle,
)
from ai_trading_system.contracts.strategy_research_work_progress import (
    CapabilityProgress,
    ReaderConcept,
    ResearchEffect,
    StageWorkProgressRecord,
    StrategyResearchWorkProgressBundle,
    build_strategy_research_progress_matrix,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic

_QUESTION_PROMPTS = {
    item.question_id: item.reader_prompt_zh for item in CITED_QUERY_QUESTION_CATALOG
}
_STATUS_LABELS = {
    CitedQueryAnswerStatus.ANSWERED: "证据完整",
    CitedQueryAnswerStatus.LIMITED: "有依据，但上下文有限",
    CitedQueryAnswerStatus.BLOCKED: "无法可靠回答",
}
_RESULT_STATUS_LABELS = {
    "NOT_DUE": "尚未到期",
    "DUE": "待处理",
    "RUNNING": "进行中",
    "PASS": "已通过",
    "LIMITED": "证据有限",
    "SKIPPED": "已跳过",
    "BLOCKED": "已阻断",
    "FAILED": "未通过",
}
_ASSERTION_LABELS = {
    AssertionKind.DATA_FACT: "数据事实",
    AssertionKind.RULE_JUDGMENT: "规则判断",
    AssertionKind.MODEL_RESULT: "模型结果",
    AssertionKind.RESEARCHER_INTERPRETATION: "研究者解释",
    AssertionKind.OWNER_DECISION: "Owner 决策",
}
_ATTRIBUTION_DIRECTION_LABELS = {
    AttributionDirection.SUPPORTS: "支持",
    AttributionDirection.CONTRADICTS: "反对 / 阻断",
    AttributionDirection.MIXED: "影响混合",
    AttributionDirection.NEUTRAL: "中性",
    AttributionDirection.UNKNOWN: "未知",
}
_CAPABILITY_PROGRESS_PRESENTATION = {
    CapabilityProgress.AVAILABLE: ("能力可用", "axis-available"),
    CapabilityProgress.IN_PROGRESS: ("能力建设中", "axis-active"),
    CapabilityProgress.BLOCKED: ("工程受阻", "axis-blocked"),
    CapabilityProgress.NOT_APPLICABLE: ("本页不执行", "axis-neutral"),
}
_RESEARCH_EFFECT_PRESENTATION = {
    ResearchEffect.NO_NEW_RESEARCH_EVIDENCE: ("本页无新增证据", "axis-neutral"),
    ResearchEffect.LIMITED_RESEARCH_EVIDENCE: ("研究证据有限", "axis-limited"),
    ResearchEffect.OWNER_DECISION_ONLY: ("等待人工决策", "axis-review"),
}
_PAGE_ACCEPTANCE_TRACK_LABELS = {
    PageAcceptanceTrack.ENGINEERING_VALIDATION: "工程验收",
    PageAcceptanceTrack.OWNER_VISUAL_REVIEW: "Owner 视觉验收",
    PageAcceptanceTrack.READER_COMPREHENSION_REVIEW: "读者理解验收",
}


@dataclass(frozen=True)
class AtlasCitedQueryRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class AtlasCitedQueryShowcase:
    responses: tuple[StrategyResearchCitedQueryResponse, ...]
    validations: tuple[CitedQueryValidationResult, ...]
    snapshot_payload: Mapping[str, object]
    before_payload: Mapping[str, object]
    after_payload: Mapping[str, object]
    diff_payload: Mapping[str, object]
    status_explanations: StrategyResearchStatusExplanationBundle
    status_explanation_validation: StatusExplanationProjectionValidation
    work_progress: StrategyResearchWorkProgressBundle
    work_progress_validation: WorkProgressProjectionValidation
    qqq_options_projection: StrategyResearchQQQOptionsProjectionBundle
    qqq_options_projection_validation: QQQOptionsProjectionValidation
    reader_terminology: ReaderTerminologyPolicy
    reader_projection_contract: StrategyResearchReaderProjectionContract
    reader_projection_contract_sha256: str
    reader_state: ReaderStateProjection
    page_effectiveness: StrategyResearchPageEffectivenessManifest


@dataclass(frozen=True)
class _FlowStageStatusProvenance:
    stage_id: str
    status_code: str
    status_label: str
    status_tone: str
    source_kind: str
    reason_zh: str
    exact_refs: tuple[str, ...]


@dataclass(frozen=True)
class _ReaderCausalSource:
    source_ref_id: str
    source_locator: str
    source_sha256: str


@dataclass(frozen=True)
class _ReaderCausalNode:
    kind: ReaderCausalNodeKind
    question_zh: str
    answer_zh: str
    sources: tuple[_ReaderCausalSource, ...]


@dataclass(frozen=True)
class _ReaderCausalEdge:
    source_node: ReaderCausalNodeKind
    relation: ReaderCausalEdgeKind
    target_node: ReaderCausalNodeKind
    source: _ReaderCausalSource


def _task_coverage(
    manifest: StrategyResearchPageEffectivenessManifest,
    task_number: str,
) -> PageTaskCoverage:
    prefix = f"TRADING-{task_number}_"
    matches = tuple(item for item in manifest.task_coverage if item.task_id.startswith(prefix))
    if len(matches) != 1:
        raise ValueError(f"ATLAS_READER_TASK_COVERAGE_INVALID:{task_number}:{len(matches)}")
    return matches[0]


def _coverage_source(item: PageTaskCoverage) -> _ReaderCausalSource:
    return _ReaderCausalSource(
        source_ref_id=item.task_id,
        source_locator=item.requirement_path,
        source_sha256=item.requirement_sha256,
    )


def _build_reader_state(
    *,
    manifest: StrategyResearchPageEffectivenessManifest,
    responses: tuple[StrategyResearchCitedQueryResponse, ...],
    snapshot_generated_at: str,
    before_snapshot_id: str,
    before_generated_at: str,
    has_changes: bool,
    policy_root: Path,
) -> ReaderStateProjection:
    readiness = _task_coverage(manifest, "2515")
    evidence = _task_coverage(manifest, "2522")
    next_step = _task_coverage(manifest, "2528")
    change_response = next(
        item
        for item in responses
        if item.request.question_id is CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION
    )
    change_summary = " ".join(
        item.text_zh
        for item in change_response.claims
        if not _contains_raw_reader_identifier(item.text_zh)
    ).strip()
    if not change_summary:
        change_summary = "当前来源没有提供可安全展示的变化说明。"
    citation_dates = tuple(
        citation.as_of.isoformat() for response in responses for citation in response.citations
    )
    return project_reader_state(
        policy=load_reader_state_semantics(repository_root=policy_root),
        status_object_zh="策略研究重新开放状态",
        raw_status="KEEP_CLOSED",
        reason_zh=readiness.reader_summary_zh,
        data_as_of=max(citation_dates) if citation_dates else None,
        evidence_evaluated_at=snapshot_generated_at,
        page_generated_at=snapshot_generated_at,
        next_legal_action_zh=next_step.reader_summary_zh,
        prohibited_inference_zh=(
            "不能把工程校验、页面可读或一次外部运行解释为策略有效、收益稳健或风险可接受。"
        ),
        change_kind=(ReaderChangeKind.CHANGED if has_changes else ReaderChangeKind.UNCHANGED),
        comparison_base_id=before_snapshot_id,
        comparison_base_date=before_generated_at,
        change_explanation_zh=change_summary,
        source_refs=(
            readiness.requirement_path,
            evidence.requirement_path,
            next_step.requirement_path,
        ),
    )


def _build_snapshot_response(
    *,
    question_id: CitedQueryQuestionId,
    target_id: str,
    snapshot_payload: Mapping[str, object],
) -> StrategyResearchCitedQueryResponse:
    snapshot_id = str(snapshot_payload["snapshot_id"])
    request = StrategyResearchCitedQueryRequest.build(
        question_id=question_id,
        target_id=target_id,
        snapshot_id=snapshot_id,
    )
    return answer_cited_query(
        request,
        snapshot_payload=snapshot_payload,
    )


def build_cited_query_showcase(
    *,
    target_ids: Mapping[CitedQueryQuestionId, str],
    snapshot_payload: Mapping[str, object],
    before_payload: Mapping[str, object],
    after_payload: Mapping[str, object],
    diff_payload: Mapping[str, object],
    repository_root: Path | None = None,
    page_engineering_status: PageAcceptanceStatus = PageAcceptanceStatus.NOT_EXECUTED,
    page_engineering_evidence_refs: Sequence[str] = (),
    page_owner_visual_review: PageAcceptanceRecord | None = None,
    page_reader_comprehension_review: PageAcceptanceRecord | None = None,
) -> AtlasCitedQueryShowcase:
    snapshot = load_validated_snapshot_payload(snapshot_payload)
    before, after, diff = load_validated_diff_payloads(
        before_payload=before_payload,
        after_payload=after_payload,
        diff_payload=diff_payload,
    )
    del before, after
    if set(target_ids) != set(CitedQueryQuestionId):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_TARGET_SET_INVALID")
    responses = [
        _build_snapshot_response(
            question_id=CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY,
            target_id=target_ids[CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY],
            snapshot_payload=snapshot_payload,
        ),
        _build_snapshot_response(
            question_id=CitedQueryQuestionId.RESULT_AND_STATUS,
            target_id=target_ids[CitedQueryQuestionId.RESULT_AND_STATUS],
            snapshot_payload=snapshot_payload,
        ),
        _build_snapshot_response(
            question_id=CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS,
            target_id=target_ids[CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS],
            snapshot_payload=snapshot_payload,
        ),
    ]
    diff_request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION,
        target_id=target_ids[CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION],
        diff_id=diff.diff_id,
        before_snapshot_id=diff.before_snapshot_id,
        after_snapshot_id=diff.after_snapshot_id,
    )
    responses.append(
        answer_cited_query(
            diff_request,
            before_payload=before_payload,
            after_payload=after_payload,
            diff_payload=diff_payload,
        )
    )
    responses.append(
        _build_snapshot_response(
            question_id=CitedQueryQuestionId.SOURCE_LINEAGE,
            target_id=target_ids[CitedQueryQuestionId.SOURCE_LINEAGE],
            snapshot_payload=snapshot_payload,
        )
    )
    ordered = tuple(responses)
    validations: list[CitedQueryValidationResult] = []
    for response in ordered:
        if response.request.question_id is CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION:
            validation = validate_serialized_cited_query_response(
                response_payload=response.to_dict(),
                before_payload=before_payload,
                after_payload=after_payload,
                diff_payload=diff_payload,
            )
        else:
            validation = validate_serialized_cited_query_response(
                response_payload=response.to_dict(),
                snapshot_payload=snapshot_payload,
            )
        if validation.status != "PASS":
            raise ValueError(
                "ATLAS_CITED_QUERY_SHOWCASE_VALIDATION_FAILED:" + ",".join(validation.errors)
            )
        validations.append(validation)
    root = (
        Path(__file__).resolve().parents[3]
        if repository_root is None
        else repository_root.resolve()
    )
    explanation_policy = load_status_explanation_authority_policy(repository_root=root)
    status_explanations = project_status_explanations(
        snapshot=snapshot,
        primary_research_start=explanation_policy.primary_research_start,
        policy=explanation_policy,
    )
    status_explanation_validation = validate_status_explanation_bundle(
        snapshot=snapshot,
        bundle=status_explanations,
        policy=explanation_policy,
    )
    if status_explanation_validation.status != "PASS":
        raise ValueError("ATLAS_CITED_QUERY_STATUS_EXPLANATION_VALIDATION_FAILED")
    work_progress_policy = load_work_progress_authority_policy(repository_root=root)
    work_progress = project_work_progress(
        snapshot=snapshot,
        status_explanations=status_explanations,
        policy=work_progress_policy,
    )
    work_progress_validation = validate_work_progress_bundle(
        snapshot=snapshot,
        status_explanations=status_explanations,
        bundle=work_progress,
        policy=work_progress_policy,
    )
    if work_progress_validation.status != "PASS":
        raise ValueError("ATLAS_CITED_QUERY_WORK_PROGRESS_VALIDATION_FAILED")
    qqq_policy = load_qqq_options_projection_policy(repository_root=root)
    qqq_options_projection = build_qqq_options_projection(
        repository_root=root,
        snapshot_id=snapshot.snapshot_id,
        policy=qqq_policy,
    )
    qqq_options_projection_validation = validate_qqq_options_projection(
        repository_root=root,
        bundle=qqq_options_projection,
        policy=qqq_policy,
    )
    if qqq_options_projection_validation.status != "PASS":
        raise ValueError("ATLAS_CITED_QUERY_QQQ_OPTIONS_PROJECTION_VALIDATION_FAILED")
    reader_terminology = load_reader_terminology_policy(repository_root=root)
    reader_projection_contract_bytes = (
        root / "config/atlas/reader_projection_contract.yaml"
    ).read_bytes()
    reader_projection_contract = StrategyResearchReaderProjectionContract.from_yaml_bytes(
        reader_projection_contract_bytes
    )
    page_effectiveness = build_page_effectiveness_manifest(
        repository_root=root,
        engineering_status=page_engineering_status,
        engineering_evidence_refs=page_engineering_evidence_refs,
        owner_visual_review=page_owner_visual_review,
        reader_comprehension_review=page_reader_comprehension_review,
    )
    reader_state = _build_reader_state(
        manifest=page_effectiveness,
        responses=ordered,
        snapshot_generated_at=str(snapshot_payload["generated_at"]),
        before_snapshot_id=str(before_payload["snapshot_id"]),
        before_generated_at=str(before_payload["generated_at"]),
        has_changes=bool(diff_payload["changes"]),
        policy_root=root,
    )
    return AtlasCitedQueryShowcase(
        responses=ordered,
        validations=tuple(validations),
        snapshot_payload=snapshot_payload,
        before_payload=before_payload,
        after_payload=after_payload,
        diff_payload=diff_payload,
        status_explanations=status_explanations,
        status_explanation_validation=status_explanation_validation,
        work_progress=work_progress,
        work_progress_validation=work_progress_validation,
        qqq_options_projection=qqq_options_projection,
        qqq_options_projection_validation=qqq_options_projection_validation,
        reader_terminology=reader_terminology,
        reader_projection_contract=reader_projection_contract,
        reader_projection_contract_sha256=hashlib.sha256(
            reader_projection_contract_bytes
        ).hexdigest(),
        reader_state=reader_state,
        page_effectiveness=page_effectiveness,
    )


def _render_citation(citation: CitedQueryCitation) -> str:
    known_at = "未记录" if citation.known_at is None else citation.known_at.isoformat()
    available_at = "未记录" if citation.available_at is None else citation.available_at.isoformat()
    entity_hashes = (
        f"entity <code>{escape(citation.entity_sha256)}</code>"
        if citation.entity_sha256 is not None
        else (
            f"before <code>{escape(citation.before_entity_sha256 or 'none')}</code>"
            f"<br>after <code>{escape(citation.after_entity_sha256 or 'none')}</code>"
        )
    )
    return (
        '<li class="citation">'
        f"<p><strong>{escape(citation.source_path)}</strong></p>"
        f"<p>source_ref <code>{escape(citation.source_ref_id)}</code><br>"
        f"commit <code>{escape(citation.exact_commit)}</code><br>"
        f"SHA-256 <code>{escape(citation.source_sha256)}</code></p>"
        f"<p>as_of <code>{escape(citation.as_of.isoformat())}</code><br>"
        f"known_at <code>{escape(known_at)}</code><br>"
        f"available_at <code>{escape(available_at)}</code></p>"
        f"<p>{entity_hashes}</p>"
        "</li>"
    )


_RAW_READER_IDENTIFIER = re.compile(
    r"(?:TRADING-\d+(?:_[A-Z0-9]+)*|"
    r"[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+|"
    r"(?:config|docs|inputs|outputs|registry|src|tests)/[A-Za-z0-9_.\-/]+|"
    r"(?<![0-9a-f])[0-9a-f]{40}(?:[0-9a-f]{24})?(?![0-9a-f])|"
    r"[a-z][a-z0-9]+(?:-[a-z0-9]+){2,})"
)


def _contains_raw_reader_identifier(value: str) -> bool:
    return _RAW_READER_IDENTIFIER.search(value) is not None


def _render_response(
    response: StrategyResearchCitedQueryResponse,
    terminology: ReaderTerminologyPolicy,
) -> str:
    prompt = _QUESTION_PROMPTS[response.request.question_id]
    reader_claims = tuple(
        claim.text_zh
        for claim in response.claims
        if not _contains_raw_reader_identifier(claim.text_zh)
    )
    audit_claims = tuple(
        claim.text_zh for claim in response.claims if _contains_raw_reader_identifier(claim.text_zh)
    )
    claims = "".join(
        f'<p class="claim">{escape(project_reader_text(text=item, policy=terminology))}</p>'
        for item in reader_claims
    )
    if audit_claims:
        claims += (
            '<p class="claim">来源身份与校验结果已经核对；原始路径和校验值收在下方审计明细中。</p>'
        )
    if not claims:
        claims = '<p class="claim muted">没有生成未经引用的结论。</p>'
    reader_limitations = tuple(
        item for item in response.limitations if not _contains_raw_reader_identifier(item)
    )
    audit_limitations = tuple(
        item for item in response.limitations if _contains_raw_reader_identifier(item)
    )
    limitations = (
        "<ul>"
        + "".join(
            f"<li>{escape(project_reader_text(text=item, policy=terminology))}</li>"
            for item in reader_limitations
        )
        + "</ul>"
        if reader_limitations
        else '<p class="good">没有额外证据限制。</p>'
    )
    if audit_limitations:
        limitations += "<p>另有机器可读的限制标识，已放入下方审计明细。</p>"
    citations = "".join(_render_citation(item) for item in response.citations)
    raw_claims = "".join(f"<li>{escape(item)}</li>" for item in audit_claims)
    raw_limitations = "".join(f"<li>{escape(item)}</li>" for item in audit_limitations)
    status_class = response.answer_status.value.lower()
    return f"""
    <article class="answer-card" id="{escape(response.request.question_id.value.lower())}" data-reader-card="{escape(response.request.question_id.value)}">
      <div class="answer-head">
        <div>
          <p class="question-id">固定研究问题</p>
          <h2>{escape(prompt)}</h2>
          <p class="target">阅读对象：本页已经绑定并校验的研究记录</p>
        </div>
        <span class="status status-{status_class}">{escape(_STATUS_LABELS[response.answer_status])}</span>
      </div>
      <section class="answer-block">
        <h3>一句话回答</h3>
        {claims}
      </section>
      <section class="answer-block limits">
        <h3>先看限制</h3>
        {limitations}
      </section>
      <details data-reader-layer="audit">
        <summary>查看 {len(response.citations)} 条完整引用与审计标识</summary>
        {f"<h3>原始回答字段</h3><ul>{raw_claims}</ul>" if raw_claims else ""}
        {f"<h3>原始限制字段</h3><ul>{raw_limitations}</ul>" if raw_limitations else ""}
        <p class="reason">机器原因码：<code>{escape(" · ".join(response.reason_codes) or "none")}</code></p>
        <ul class="citations">{citations or "<li>没有通过引用闭包的证据。</li>"}</ul>
        <p class="identity">response <code>{escape(response.response_id)}</code> · request <code>{escape(response.request.request_id)}</code> · target <code>{escape(response.request.target_kind.value)}</code> / <code>{escape(response.request.target_id)}</code></p>
      </details>
    </article>
    """


def _result_status_tone(status_code: str) -> str:
    if status_code == "PASS":
        return "result-pass"
    if status_code == "LIMITED":
        return "result-limited"
    if status_code in {"BLOCKED", "FAILED"}:
        return "result-blocked"
    if status_code == "RUNNING":
        return "result-active"
    return "result-neutral"


def _render_result_attribution(item: ResearchAttribution) -> str:
    source_refs = "".join(
        f"<li><code>{escape(source_ref_id)}</code></li>" for source_ref_id in item.source_ref_ids
    )
    return (
        f'<li class="result-attribution" data-reader-layer="audit" data-attribution-id="{escape(item.attribution_id)}" '
        f'data-attribution-direction="{escape(item.direction.value)}">'
        '<div class="attribution-heading">'
        f'<span class="direction direction-{escape(item.direction.value.lower())}">'
        f"{escape(_ATTRIBUTION_DIRECTION_LABELS[item.direction])}"
        f"<code>{escape(item.direction.value)}</code></span>"
        f'<code class="attribution-id">{escape(item.attribution_id)}</code>'
        "</div>"
        f"<p>{escape(item.explanation)}</p>"
        '<dl class="attribution-meta">'
        f"<div><dt>来源节点</dt><dd><code>{escape(item.source_node_id)}</code></dd></div>"
        f"<div><dt>信息类型</dt><dd>{escape(_ASSERTION_LABELS[item.assertion_kind])} "
        f"<code>{escape(item.assertion_kind.value)}</code></dd></div>"
        "</dl>"
        f'<ul class="result-source-refs">{source_refs}</ul>'
        "</li>"
    )


def _render_result_ledger_card(
    result: ResearchResultCard,
    attributions: tuple[ResearchAttribution, ...],
    terminology: ReaderTerminologyPolicy,
) -> str:
    reader_limitations = tuple(
        item for item in result.limitations if not _contains_raw_reader_identifier(item)
    )
    audit_limitations = tuple(
        item for item in result.limitations if _contains_raw_reader_identifier(item)
    )
    limitations = "".join(
        f"<li>{escape(project_reader_text(text=item, policy=terminology))}</li>"
        for item in reader_limitations
    )
    if audit_limitations:
        limitations += "<li>另有机器限制标识，已保留在本卡审计明细中。</li>"
    audit_limitations_html = "".join(f"<li>{escape(item)}</li>" for item in audit_limitations)
    source_refs = "".join(
        f"<li><code>{escape(source_ref_id)}</code></li>" for source_ref_id in result.source_ref_ids
    )
    attribution_rows = "".join(_render_result_attribution(item) for item in attributions)
    display_status = result.display_status.value
    raw_status = result.raw_status.value
    is_historical = result.source_original_status is not None
    historical_badge = (
        '<span class="historical-result-badge">历史材料 · 非当前结论</span>'
        if is_historical
        else ""
    )
    original_status = (
        ""
        if result.source_original_status is None
        else (
            "<div><dt>来源原始状态</dt>"
            f"<dd><code>{escape(result.source_original_status)}</code></dd></div>"
        )
    )
    mapping_rationale = (
        ""
        if result.status_mapping_rationale is None
        else (
            '<p class="historical-status-rationale"><strong>为什么这样映射：</strong>'
            f"{escape(project_reader_text(text=result.status_mapping_rationale, policy=terminology))}</p>"
        )
    )
    return f"""
    <article class="result-ledger-card{" historical-result-card" if is_historical else ""}" data-result-id="{escape(result.result_id)}" data-raw-status="{escape(raw_status)}" data-display-status="{escape(display_status)}" data-historical-record="{"true" if is_historical else "false"}"{(' data-source-original-status="' + escape(result.source_original_status or "") + '"') if is_historical else ""}>
      <div class="result-ledger-head">
        <div>
          <p class="result-sequence">研究结果</p>
          {historical_badge}
          <h3>{escape(result.title)}</h3>
          <p class="result-node">已绑定到可重放的研究节点；原始编号见审计明细。</p>
        </div>
        <span class="result-status {_result_status_tone(display_status)}">
          <strong>{escape(_RESULT_STATUS_LABELS[display_status])}</strong>
        </span>
      </div>
      <p class="result-summary">{escape(project_reader_text(text=result.reader_summary, policy=terminology))}</p>
      <div class="result-status-pair" aria-label="机器状态与展示状态">
        <span><small>机器原始状态</small><strong>{escape(_RESULT_STATUS_LABELS[raw_status])}</strong></span>
        <span><small>读者展示状态</small><strong>{escape(_RESULT_STATUS_LABELS[display_status])}</strong></span>
        <span><small>信息类型</small><strong>{escape(_ASSERTION_LABELS[result.assertion_kind])}</strong></span>
        <span><small>投资结论</small><strong>不是</strong></span>
      </div>
      {mapping_rationale}
      <details class="result-evidence">
        <summary>查看限制、来源与 {len(attributions)} 条归因</summary>
        <div class="result-evidence-body">
          <section data-reader-layer="audit">
            <h4>审计状态与原始标识</h4>
            <dl>
              <div><dt>result</dt><dd><code>{escape(result.result_id)}</code></dd></div>
              <div><dt>node</dt><dd><code>{escape(result.node_id)}</code></dd></div>
              <div><dt>raw status</dt><dd><code>{escape(raw_status)}</code></dd></div>
              <div><dt>display status</dt><dd><code>{escape(display_status)}</code></dd></div>
              <div><dt>assertion kind</dt><dd><code>{escape(result.assertion_kind.value)}</code></dd></div>
              <div><dt>investment-facing</dt><dd><code>investment_facing=false</code></dd></div>
              {original_status}
            </dl>
            {f"<h4>机器限制标识</h4><ul>{audit_limitations_html}</ul>" if audit_limitations_html else ""}
          </section>
          <section>
            <h4>限制</h4>
            <ul>{limitations or "<li>没有额外限制。</li>"}</ul>
          </section>
          <section data-reader-layer="audit">
            <h4>Canonical source refs</h4>
            <ul class="result-source-refs">{source_refs}</ul>
          </section>
          <section class="result-attribution-section">
            <h4>全部关联归因</h4>
            <ol class="result-attributions">{attribution_rows or "<li>当前没有关联归因。</li>"}</ol>
          </section>
        </div>
      </details>
    </article>
    """


def _render_result_ledger(showcase: AtlasCitedQueryShowcase) -> str:
    snapshot = load_validated_snapshot_payload(showcase.snapshot_payload)
    historical_results = tuple(
        item for item in snapshot.results if item.source_original_status is not None
    )
    if len(historical_results) != 5 or any(
        item.display_status.value != "LIMITED" or item.investment_facing
        for item in historical_results
    ):
        raise ValueError("ATLAS_RESULT_LEDGER_HISTORICAL_PROJECTION_SET_INVALID")
    attributions_by_result: dict[str, list[ResearchAttribution]] = {
        result.result_id: [] for result in snapshot.results
    }
    for attribution in snapshot.attributions:
        try:
            attributions_by_result[attribution.result_id].append(attribution)
        except KeyError as exc:
            raise ValueError(
                "ATLAS_RESULT_LEDGER_ORPHAN_ATTRIBUTION:" + attribution.attribution_id
            ) from exc
    cards = "".join(
        _render_result_ledger_card(
            result,
            tuple(attributions_by_result[result.result_id]),
            showcase.reader_terminology,
        )
        for result in snapshot.results
    )
    status_order = ("PASS", "LIMITED", "BLOCKED", "NOT_DUE", "RUNNING", "DUE", "FAILED", "SKIPPED")
    status_counts = {
        status: sum(result.display_status.value == status for result in snapshot.results)
        for status in status_order
    }
    status_summary = "".join(
        f'<span class="ledger-count {_result_status_tone(status)}" data-ledger-status="{escape(status)}">'
        f"<strong>{count}</strong>{escape(_RESULT_STATUS_LABELS[status])}</span>"
        for status, count in status_counts.items()
        if count
    )
    return f"""
    <section class="result-ledger" id="all-in-scope-results" aria-labelledby="result-ledger-title" data-coverage-scope="ATLAS_V1_3_REPRESENTATIVE_PLUS_REVIEWED_HISTORY" data-historical-repository-coverage-complete="false" data-historical-result-count="{len(historical_results)}">
      <div class="result-ledger-intro">
        <div>
          <p class="section-kicker">RESULT LEDGER · CANONICAL SNAPSHOT ONLY</p>
          <h2 id="result-ledger-title">当前覆盖范围内的全部研究结果</h2>
          <p>这里完整列出 validated Atlas snapshot 中的 {len(snapshot.results)} 个 result 与 {len(snapshot.attributions)} 条 attribution，其中 {len(historical_results)} 个是已审阅历史材料；顺序沿用 canonical snapshot，不做“最好”或“最相关”排序。</p>
        </div>
        <div class="coverage-boundary" aria-label="覆盖范围边界">
          <strong>先看覆盖边界</strong>
          <p>这是 Atlas V1.3 的代表性主线加五份已审阅历史记录，不是全仓历史研究的完整清单；历史工程或治理通过不等于当前策略结论通过。</p>
          <details data-reader-layer="audit">
            <summary>查看覆盖范围审计标识</summary>
            <code>coverage_scope=ATLAS_V1_3_REPRESENTATIVE_PLUS_REVIEWED_HISTORY</code>
            <code>historical_repository_coverage_complete=false</code>
          </details>
        </div>
      </div>
      <div class="ledger-summary" aria-label="结果展示状态分布">{status_summary}</div>
      <p class="ledger-reading-note"><strong>怎样读：</strong>先看标题、摘要和“读者展示状态”；展开后再看限制、来源引用与归因。机器原始状态和读者展示状态均不是投资评级，工程通过也不等于策略结论通过。</p>
      <div class="result-ledger-grid">{cards}</div>
    </section>
    """


def _render_historical_flow_lane(showcase: AtlasCitedQueryShowcase) -> str:
    snapshot = load_validated_snapshot_payload(showcase.snapshot_payload)
    historical_results = tuple(
        item for item in snapshot.results if item.source_original_status is not None
    )
    historical_result_ids = {item.result_id for item in historical_results}
    historical_attributions = tuple(
        item for item in snapshot.attributions if item.result_id in historical_result_ids
    )
    if (
        len(historical_results) != 5
        or len(historical_attributions) != 5
        or any(
            item.direction is not AttributionDirection.NEUTRAL for item in historical_attributions
        )
    ):
        raise ValueError("ATLAS_HISTORICAL_FLOW_LANE_PROVENANCE_INVALID")
    attribution_counts = {
        result_id: sum(item.result_id == result_id for item in historical_attributions)
        for result_id in historical_result_ids
    }
    if set(attribution_counts.values()) != {1}:
        raise ValueError("ATLAS_HISTORICAL_FLOW_LANE_ATTRIBUTION_CARDINALITY_INVALID")
    cards = "".join(
        (
            f'<li class="historical-lane-item" data-historical-result-id="{escape(item.result_id)}">'
            '<span class="historical-lane-status">历史 · 证据有限</span>'
            f"<strong>{escape(item.title)}</strong>"
            '<details data-reader-layer="audit"><summary>查看历史原始状态</summary>'
            f"<code>{escape(item.source_original_status or '')}</code></details>"
            "<small>中性来源关系 · 非当前关注</small>"
            "</li>"
        )
        for item in historical_results
    )
    return f"""
      <section class="historical-flow-lane" aria-labelledby="historical-flow-title" data-historical-lane-active="false" data-historical-result-count="5">
        <div class="historical-lane-head">
          <div>
            <p class="section-kicker">REVIEWED HISTORY · ISOLATED LANE</p>
            <h3 id="historical-flow-title">历史权重研究支线已经纳入证据地图，但不是当前关注</h3>
            <p>这五份材料只补全历史路径和来源。卡片顺序是阅读顺序，不表示因果或优先级；四个原始“通过”只表示历史材料已经形成。</p>
          </div>
          <span class="historical-lane-boundary"><strong>5</strong> 项历史记录<small>全部显示为证据有限</small></span>
        </div>
        <ol class="historical-lane-grid">{cards}</ol>
        <p class="historical-lane-note"><strong>与当前位置的关系：</strong>“你在这里”仍是第 7 阶段 Citation-first 页面；历史支线没有 active marker，也没有改变当前研究、DQ、回测或 Owner 决策状态。</p>
      </section>
    """


_STATUS_PRESENTATION = {
    "NOT_EXECUTED_BY_PAGE": ("本页未执行", "progress-neutral"),
    "RUNNING": ("研究进行中", "progress-active"),
    "LIMITED": ("证据有限", "progress-limited"),
    "VALIDATED": ("已验证", "progress-validated"),
    "PENDING_OWNER_REVIEW": ("待人工复核", "progress-review"),
}
_VALUE_STATE_LABELS = {
    ExplanationValueState.PRESENT: "已有依据",
    ExplanationValueState.NOT_RECORDED: "尚未登记",
    ExplanationValueState.NOT_APPLICABLE: "不适用于本页",
    ExplanationValueState.NOT_YET_DUE: "尚未到期",
    ExplanationValueState.SOURCE_UNAVAILABLE: "来源暂不可用",
    ExplanationValueState.OWNER_DECISION_PENDING: "等待 Owner 决定",
}


def _status_explanation_records_by_stage(
    showcase: AtlasCitedQueryShowcase,
) -> dict[str, StatusExplanationRecord]:
    bundle = showcase.status_explanations
    validation = showcase.status_explanation_validation
    snapshot = load_validated_snapshot_payload(showcase.snapshot_payload)
    snapshot_id = snapshot.snapshot_id
    if (
        validation.status != "PASS"
        or validation.snapshot_id != snapshot_id
        or validation.snapshot_id != bundle.snapshot_id
        or validation.bundle_sha256 != bundle.content_sha256
        or validation.policy_sha256 != bundle.policy_sha256
    ):
        raise ValueError("ATLAS_CITED_QUERY_STATUS_EXPLANATION_BINDING_INVALID")
    stage_ids = tuple(item.stage_id for item in bundle.explanation_records)
    if stage_ids != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise ValueError("ATLAS_CITED_QUERY_STATUS_EXPLANATION_STAGE_SET_INVALID")
    if any(item.status_code not in _STATUS_PRESENTATION for item in bundle.explanation_records):
        raise ValueError("ATLAS_CITED_QUERY_STATUS_EXPLANATION_STATUS_INVALID")
    return {item.stage_id: item for item in bundle.explanation_records}


def _work_progress_records_by_stage(
    showcase: AtlasCitedQueryShowcase,
) -> tuple[dict[str, StageWorkProgressRecord], dict[str, ReaderConcept]]:
    bundle = showcase.work_progress
    validation = showcase.work_progress_validation
    snapshot = load_validated_snapshot_payload(showcase.snapshot_payload)
    if (
        validation.status != "PASS"
        or validation.snapshot_id != snapshot.snapshot_id
        or validation.snapshot_id != bundle.snapshot_id
        or validation.bundle_sha256 != bundle.content_sha256
        or validation.policy_sha256 != bundle.policy_sha256
    ):
        raise ValueError("ATLAS_CITED_QUERY_WORK_PROGRESS_BINDING_INVALID")
    stage_ids = tuple(item.stage_id for item in bundle.stage_records)
    if stage_ids != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise ValueError("ATLAS_CITED_QUERY_WORK_PROGRESS_STAGE_SET_INVALID")
    records = {item.stage_id: item for item in bundle.stage_records}
    concepts = {item.concept_id: item for item in bundle.concepts}
    if len(concepts) != len(bundle.concepts):
        raise ValueError("ATLAS_CITED_QUERY_WORK_PROGRESS_CONCEPT_SET_INVALID")
    if any(
        concept_id not in concepts
        for record in bundle.stage_records
        for concept_id in record.concept_ids
    ):
        raise ValueError("ATLAS_CITED_QUERY_WORK_PROGRESS_CONCEPT_BINDING_INVALID")
    return records, concepts


def _render_stage_axis_summary(record: StageWorkProgressRecord) -> str:
    capability_label, capability_tone = _CAPABILITY_PROGRESS_PRESENTATION[
        record.capability_progress
    ]
    research_label, research_tone = _RESEARCH_EFFECT_PRESENTATION[record.research_effect]
    return (
        '<div class="stage-axis-summary" aria-label="工程与研究进度">'
        f'<span class="stage-axis-chip {escape(capability_tone)}" '
        f'data-stage-axis="capability" '
        f'data-stage-axis-value="{escape(record.capability_progress.value)}">'
        "<small>工程</small>"
        f"<strong>{escape(capability_label)}</strong>"
        "</span>"
        f'<span class="stage-axis-chip {escape(research_tone)}" '
        f'data-stage-axis="research" '
        f'data-stage-axis-value="{escape(record.research_effect.value)}">'
        "<small>研究证据</small>"
        f"<strong>{escape(research_label)}</strong>"
        "</span>"
        "</div>"
    )


def _render_flow_progress_matrix(
    showcase: AtlasCitedQueryShowcase,
    records: Sequence[StageWorkProgressRecord],
) -> str:
    matrix = build_strategy_research_progress_matrix(records)
    manifest = showcase.page_effectiveness
    if tuple(item.track for item in manifest.acceptance) != tuple(PageAcceptanceTrack):
        raise ValueError("ATLAS_PROGRESS_MATRIX_ACCEPTANCE_TRACK_SET_INVALID")
    acceptance_tones = {
        PageAcceptanceStatus.PASS: "axis-available",
        PageAcceptanceStatus.FAIL: "axis-blocked",
        PageAcceptanceStatus.PENDING_REVIEW: "axis-review",
        PageAcceptanceStatus.NOT_EXECUTED: "axis-neutral",
    }
    engineering_counts = (
        ("能力可用", CapabilityProgress.AVAILABLE.value, matrix.capability_available),
        ("能力建设中", CapabilityProgress.IN_PROGRESS.value, matrix.capability_in_progress),
        ("工程受阻", CapabilityProgress.BLOCKED.value, matrix.capability_blocked),
        (
            "本页不执行",
            CapabilityProgress.NOT_APPLICABLE.value,
            matrix.capability_not_applicable,
        ),
    )
    research_counts = (
        (
            "研究证据有限",
            ResearchEffect.LIMITED_RESEARCH_EVIDENCE.value,
            matrix.research_limited_evidence,
        ),
        (
            "本页无新增证据",
            ResearchEffect.NO_NEW_RESEARCH_EVIDENCE.value,
            matrix.research_no_new_evidence,
        ),
        (
            "仅人工决策",
            ResearchEffect.OWNER_DECISION_ONLY.value,
            matrix.research_owner_decision_only,
        ),
    )
    engineering_items = "".join(
        (
            f'<li data-matrix-axis="capability" data-matrix-value="{escape(value)}">'
            f"<span>{escape(label)}</span><strong>{count}</strong></li>"
        )
        for label, value, count in engineering_counts
    )
    research_items = "".join(
        (
            f'<li data-matrix-axis="research" data-matrix-value="{escape(value)}">'
            f"<span>{escape(label)}</span><strong>{count}</strong></li>"
        )
        for label, value, count in research_counts
    )
    acceptance_items = "".join(
        (
            f'<li class="{acceptance_tones[item.status]}" '
            f'data-matrix-axis="page_acceptance" '
            f'data-matrix-value="{escape(item.status.value)}">'
            f"<span>{escape(_PAGE_ACCEPTANCE_TRACK_LABELS[item.track])}</span>"
            f"<strong>{escape(_PAGE_ACCEPTANCE_LABELS[item.status])}</strong></li>"
        )
        for item in manifest.acceptance
    )
    acceptance_pass_count = sum(
        item.status is PageAcceptanceStatus.PASS for item in manifest.acceptance
    )
    strategy_conclusion_pass_count = int(manifest.investment_conclusion_generated)
    return (
        '<section class="progress-matrix" aria-labelledby="progress-matrix-title" '
        f'data-progress-stage-count="{matrix.stage_count}" '
        f'data-page-acceptance-pass-count="{acceptance_pass_count}" '
        f'data-strategy-conclusion-pass-count="{strategy_conclusion_pass_count}">'
        '<div class="progress-matrix-heading">'
        '<div><p class="section-kicker">THREE AXES · DO NOT MERGE</p>'
        '<h3 id="progress-matrix-title">工程、研究、页面验收分别看</h3>'
        "<p>同一个节点可以“工程可用”，同时“研究证据有限”。页面验收只回答这张页面是否可靠、好读，不回答策略是否有效。</p></div>"
        '<div class="strategy-conclusion-count" data-strategy-conclusion="NOT_GENERATED">'
        "<span>策略结论通过</span>"
        f"<strong>{strategy_conclusion_pass_count}</strong>"
        "<small>本页没有生成投资结论</small></div></div>"
        '<div class="progress-matrix-grid">'
        '<article class="progress-matrix-card" data-progress-matrix="capability">'
        f'<div class="progress-matrix-card-head"><span>工程进度</span><strong>{matrix.capability_available} / {matrix.stage_count} 能力可用</strong></div>'
        f"<ul>{engineering_items}</ul><p>回答“工具或合同是否已经做出来”，不回答策略表现。</p></article>"
        '<article class="progress-matrix-card" data-progress-matrix="research">'
        f'<div class="progress-matrix-card-head"><span>策略研究进度</span><strong>{matrix.research_limited_evidence} 个节点只有有限证据</strong></div>'
        f"<ul>{research_items}</ul><p>回答“是否新增支持策略判断的证据”；有限不等于通过。</p></article>"
        '<article class="progress-matrix-card" data-progress-matrix="page_acceptance">'
        f'<div class="progress-matrix-card-head"><span>页面验收</span><strong>{acceptance_pass_count} / {len(manifest.acceptance)} 已通过</strong></div>'
        f"<ul>{acceptance_items}</ul><p>三条验收互不代签，人工验收只能来自真实人工事实。</p></article>"
        "</div>"
        '<p class="progress-matrix-warning"><strong>最重要的边界：</strong>绿色的“能力可用”“已验证”或页面验收通过，都不等于策略结论通过、收益稳健或可以下单。</p>'
        "</section>"
    )


def _validate_showcase_response_bindings(showcase: AtlasCitedQueryShowcase) -> None:
    question_ids = tuple(item.request.question_id for item in showcase.responses)
    if len(set(question_ids)) != len(question_ids) or set(question_ids) != set(
        CitedQueryQuestionId
    ):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_FOCUS_QUESTION_SET_INVALID")
    response_ids = tuple(item.response_id for item in showcase.responses)
    if any(not item for item in response_ids) or len(set(response_ids)) != len(response_ids):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_STATUS_RESPONSE_ID_SET_INVALID")
    validation_by_response: dict[str, CitedQueryValidationResult] = {}
    for validation in showcase.validations:
        if not validation.response_id or validation.response_id in validation_by_response:
            raise ValueError("ATLAS_CITED_QUERY_FLOW_STATUS_VALIDATION_SET_INVALID")
        validation_by_response[validation.response_id] = validation
    if set(validation_by_response) != set(response_ids):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_STATUS_VALIDATION_SET_INVALID")
    for response in showcase.responses:
        validation = validation_by_response[response.response_id]
        if validation.request_id != response.request.request_id or validation.status != "PASS":
            raise ValueError(
                "ATLAS_CITED_QUERY_FLOW_STATUS_VALIDATION_BINDING_INVALID:" + response.response_id
            )


def _build_flow_status_provenance(
    showcase: AtlasCitedQueryShowcase,
) -> tuple[_FlowStageStatusProvenance, ...]:
    records = _status_explanation_records_by_stage(showcase)
    return tuple(
        _FlowStageStatusProvenance(
            stage_id=stage_id,
            status_code=record.status_code,
            status_label=_STATUS_PRESENTATION[record.status_code][0],
            status_tone=_STATUS_PRESENTATION[record.status_code][1],
            source_kind=record.status_object_scope,
            reason_zh=record.plain_summary,
            exact_refs=record.technical_refs,
        )
        for stage_id in ATLAS_STATUS_EXPLANATION_STAGE_IDS
        for record in (records[stage_id],)
    )


def _render_value_state(value_state: ExplanationValueState) -> str:
    return (
        f'<span class="value-state value-state-{escape(value_state.value.lower())}" '
        f'data-value-state="{escape(value_state.value)}">'
        f"{escape(_VALUE_STATE_LABELS[value_state])}</span>"
    )


def _render_fact_group(
    *,
    title: str,
    section_id: str,
    facts: Sequence[CitedExplanationFact],
    emphasis: str = "",
) -> str:
    items = "".join(
        (
            f'<li data-fact-kind="{escape(fact.fact_kind.value)}" '
            f'data-value-state="{escape(fact.value_state.value)}">'
            f"{_render_value_state(fact.value_state)}"
            f"<p>{escape(fact.text_zh)}</p>"
            "</li>"
        )
        for fact in facts
    )
    return (
        f'<section class="reader-section {escape(emphasis)}" '
        f'data-reader-detail="{escape(section_id)}">'
        f"<h4>{escape(title)}</h4>"
        f'<ul class="reader-facts">{items}</ul>'
        "</section>"
    )


def _render_transition_conditions(
    conditions: Sequence[ExplanationTransitionCondition],
) -> str:
    items = "".join(
        (
            f'<li data-value-state="{escape(item.value_state.value)}">'
            f"{_render_value_state(item.value_state)}"
            f"<p>{escape(item.description_zh)}</p>"
            + (
                '<dl class="transition-detail" data-reader-layer="audit">'
                f"<div><dt>当前</dt><dd>{escape(item.current_state or '')}</dd></div>"
                f"<div><dt>可观察变化</dt><dd>{escape(item.observable_event or '')}</dd></div>"
                f"<div><dt>目标状态</dt><dd>{escape(item.target_status or '')}</dd></div>"
                "</dl>"
                if item.value_state is ExplanationValueState.PRESENT
                else ""
            )
            + "</li>"
        )
        for item in conditions
    )
    return (
        '<section class="reader-section reader-transition" '
        'data-reader-detail="what_changes">'
        "<h4>什么会改变当前状态</h4>"
        f'<ul class="reader-facts">{items}</ul>'
        "</section>"
    )


def _render_work_progress_explanation(
    record: StageWorkProgressRecord,
    concepts: Mapping[str, ReaderConcept],
) -> str:
    work_items = "".join(f"<li>{escape(item)}</li>" for item in record.work_items_zh)
    expected_outputs = "".join(f"<li>{escape(item)}</li>" for item in record.expected_outputs_zh)
    concept_links = "".join(
        (
            f'<a class="concept-link" href="#reader-concept-{escape(concept_id)}" '
            f'data-concept-ref="{escape(concept_id)}">'
            f"{escape(concepts[concept_id].display_name_zh)}</a>"
        )
        for concept_id in record.concept_ids
    )
    return (
        '<section class="work-progress-reader" data-reader-detail="work_progress" '
        ">"
        '<div class="work-purpose" data-reader-detail="why_needed">'
        "<h4>为什么需要这一步</h4>"
        f"<p>{escape(record.why_needed_zh)}</p>"
        "</div>"
        '<div class="work-reader-grid">'
        '<section class="work-reader-card" data-reader-detail="work_items">'
        "<h4>具体做什么</h4>"
        f'<ol class="work-item-list">{work_items}</ol>'
        "</section>"
        '<section class="work-reader-card" data-reader-detail="expected_outputs">'
        "<h4>预期产物</h4>"
        f'<ul class="work-output-list">{expected_outputs}</ul>'
        "</section>"
        "</div>"
        '<section class="progress-dimensions" data-reader-detail="progress_dimensions">'
        "<h4>目前进展：三种状态分开看</h4>"
        '<div class="progress-dimension-grid">'
        f'<article data-progress-dimension="capability" data-progress-value="{escape(record.capability_progress.value)}">'
        "<span>工程能力</span>"
        f"<strong>{escape(record.capability_progress_zh)}</strong>"
        "</article>"
        f'<article data-progress-dimension="latest_execution" data-progress-value="{escape(record.latest_execution_status)}">'
        "<span>本次页面所见状态</span>"
        f"<strong>{escape(record.latest_execution_summary_zh)}</strong>"
        "</article>"
        f'<article data-progress-dimension="research_effect" data-progress-value="{escape(record.research_effect.value)}">'
        "<span>对研究结论的影响</span>"
        f"<strong>{escape(record.research_effect_zh)}</strong>"
        "</article>"
        "</div></section>"
        '<section class="work-reader-card" data-reader-detail="downstream_use">'
        "<h4>完成后怎样被使用</h4>"
        f"<p>{escape(record.downstream_use_zh)}</p>"
        "</section>"
        '<section class="work-reader-card work-boundary" data-reader-detail="boundary">'
        "<h4>不能说明什么</h4>"
        f"<p>{escape(record.boundary_zh)}</p>"
        "</section>"
        '<section class="work-reader-card" data-reader-detail="next_trigger">'
        "<h4>什么时候需要再做一次</h4>"
        f"<p>{escape(record.next_trigger_zh)}</p>"
        "</section>"
        '<nav class="concept-links" aria-label="本节点概念解释">'
        "<strong>遇到陌生概念，从这里继续解释</strong>"
        f"<div>{concept_links}</div>"
        "</nav>"
        "</section>"
    )


def _render_reader_status_explanation(
    record: StatusExplanationRecord,
    work_progress: StageWorkProgressRecord,
    concepts: Mapping[str, ReaderConcept],
) -> str:
    facts_by_kind = {
        kind: tuple(item for item in record.facts if item.fact_kind is kind)
        for kind in ExplanationFactKind
    }
    audit_source_refs = tuple(
        dict.fromkeys(
            ref for item in (*record.facts, record.responsible_role) for ref in item.source_ref_ids
        )
    )
    bindings = "".join(
        (
            "<li>"
            f"<code>{escape(item.authority_kind.value)}</code> "
            f"<code>{escape(item.authority_id)}</code>"
            "</li>"
        )
        for item in record.authority_bindings
    )
    refs = "".join(
        f"<li><code>{escape(item)}</code></li>"
        for item in (*record.technical_refs, *audit_source_refs)
    )
    checked_authority_ids = "".join(
        f"<li><code>{escape(item)}</code></li>" for item in record.checked_authority_ids
    )
    status_detail = (
        '<details class="reader-status-detail">'
        "<summary>查看状态限制、责任信息与审计依据</summary>"
        '<div class="reader-status-detail-body">'
        '<div class="reader-explanation">'
        '<section class="reader-conclusion" data-reader-detail="conclusion">'
        "<span>状态限制摘要</span>"
        f"<p>{escape(record.plain_summary)}</p>"
        "</section>"
        + _render_fact_group(
            title="正在做什么",
            section_id="current_work",
            facts=facts_by_kind[ExplanationFactKind.CURRENT_WORK],
        )
        + _render_fact_group(
            title="已完成什么",
            section_id="completed",
            facts=facts_by_kind[ExplanationFactKind.COMPLETED_MILESTONE],
        )
        + _render_fact_group(
            title="还缺什么",
            section_id="remaining_gaps",
            facts=(
                *facts_by_kind[ExplanationFactKind.UNMET_CONDITION],
                *facts_by_kind[ExplanationFactKind.EVIDENCE_GAP],
            ),
            emphasis="reader-gap",
        )
        + _render_fact_group(
            title="为什么重要",
            section_id="reader_impact",
            facts=facts_by_kind[ExplanationFactKind.READER_IMPACT],
        )
        + _render_transition_conditions(record.transition_conditions)
        + '<section class="reader-section reader-owner" data-reader-detail="owner_and_next">'
        "<h4>由谁负责，以及下一步怎么读</h4>"
        '<div class="owner-next-grid">'
        '<div><span class="owner-label">责任信息</span>'
        f"{_render_value_state(record.responsible_role.value_state)}"
        f"<p>{escape(record.responsible_role.text_zh)}</p></div>"
        '<div><span class="owner-label">下一步</span>'
        f"<p>{escape(record.next_reader_action)}</p></div>"
        "</div></section>"
        '<details class="reader-audit" data-reader-layer="audit">'
        "<summary>查看审计依据</summary>"
        '<div class="reader-audit-body">'
        f"<p>状态对象 <code>{escape(record.status_object_scope)}</code> · "
        f"目标 <code>{escape(record.target_id)}</code></p>"
        "<h4>Authority bindings</h4>"
        f"<ul>{bindings}</ul>"
        "<h4>Technical refs / source refs</h4>"
        f"<ul>{refs}</ul>"
        "<h4>已检查范围</h4>"
        f"<p>{escape(' · '.join(record.checked_authority_scope))}</p>"
        "<h4>已检查 authority IDs</h4>"
        f"<ul>{checked_authority_ids}</ul>"
        "</div></details>"
        "</div>"
        "</div></details>"
    )
    return _render_work_progress_explanation(work_progress, concepts) + status_detail


def _render_concept_library(
    records: Mapping[str, StageWorkProgressRecord],
    concepts: Mapping[str, ReaderConcept],
) -> str:
    stages_by_concept: dict[str, list[StageWorkProgressRecord]] = {
        concept_id: [] for concept_id in concepts
    }
    for record in records.values():
        for concept_id in record.concept_ids:
            stages_by_concept[concept_id].append(record)
    cards = "".join(
        (
            f'<article class="concept-card" id="reader-concept-{escape(concept.concept_id)}" '
            f'data-concept-id="{escape(concept.concept_id)}" tabindex="-1">'
            '<div class="concept-card-head">'
            f"<h4>{escape(concept.display_name_zh)}</h4>"
            '<details data-reader-layer="audit"><summary>原始概念编号</summary>'
            f"<code>{escape(concept.concept_id)}</code></details>"
            "</div>"
            '<dl class="concept-explanation">'
            f"<div><dt>一句话解释</dt><dd>{escape(concept.plain_definition_zh)}</dd></div>"
            f"<div><dt>为什么需要</dt><dd>{escape(concept.why_needed_zh)}</dd></div>"
            f"<div><dt>页面里的例子</dt><dd>{escape(concept.example_zh)}</dd></div>"
            "</dl>"
            + (
                '<nav class="related-concepts" aria-label="继续解释相关概念">'
                "<strong>继续解释</strong><div>"
                + "".join(
                    (
                        f'<a href="#reader-concept-{escape(related_id)}" '
                        f'data-related-concept="{escape(related_id)}">'
                        f"{escape(concepts[related_id].display_name_zh)}</a>"
                    )
                    for related_id in concept.related_concept_ids
                )
                + "</div></nav>"
                if concept.related_concept_ids
                else '<p class="concept-leaf">这是当前解释路径的通俗终点，不再引入新概念。</p>'
            )
            + '<nav class="concept-backlinks" aria-label="返回使用该概念的流程节点">'
            "<strong>返回流程节点</strong><div>"
            + "".join(
                (
                    f'<a href="#flow-stage-{escape(stage.stage_id.lower().replace("_", "-"))}">'
                    f"{escape(stage.display_title_zh)}</a>"
                )
                for stage in stages_by_concept[concept.concept_id]
            )
            + "</div></nav>"
            "</article>"
        )
        for concept in concepts.values()
    )
    return (
        '<section class="concept-library" id="reader-concept-library" '
        'aria-labelledby="reader-concept-library-title">'
        '<div class="concept-library-head">'
        '<p class="section-kicker">RECURSIVE EXPLANATION · CLOSED CONCEPT GRAPH</p>'
        '<h3 id="reader-concept-library-title">陌生概念可以继续解释，并能返回原流程节点</h3>'
        "<p>每个概念只用通俗定义、用途和页面实例说明；“继续解释”只指向已登记概念，系统会拒绝缺失引用和循环解释。</p>"
        "</div>"
        f'<div class="concept-grid">{cards}</div>'
        "</section>"
    )


_QQQ_LAYER_LABELS = {
    "A": "主线治理事实",
    "B": "次级证据",
    "C": "机械已实现 / policy 未授权",
}
_QQQ_STATUS_LAYER_LABELS = (
    ("engineering_baseline", "工程底座"),
    ("evidence_quality", "证据质量"),
    ("policy_readiness", "Policy 准备度"),
    ("external_authority", "外部权限"),
    ("strategy_conclusion", "策略结论"),
)


def _validate_qqq_options_projection_binding(showcase: AtlasCitedQueryShowcase) -> None:
    bundle = showcase.qqq_options_projection
    validation = showcase.qqq_options_projection_validation
    if validation.status != "PASS":
        raise ValueError("ATLAS_CITED_QUERY_QQQ_OPTIONS_PROJECTION_NOT_PASS")
    if (
        bundle.snapshot_id != str(showcase.snapshot_payload["snapshot_id"])
        or validation.snapshot_id != bundle.snapshot_id
        or validation.bundle_sha256 != bundle.content_sha256
        or validation.policy_sha256 != bundle.policy_sha256
        or validation.source_set_sha256 != bundle.source_set_sha256
    ):
        raise ValueError("ATLAS_CITED_QUERY_QQQ_OPTIONS_PROJECTION_BINDING_INVALID")


def _render_qqq_projection_card(
    card: QQQOptionsProjectionCard,
    terminology: ReaderTerminologyPolicy,
) -> str:
    layer = card.layer.value
    status_layers = card.status_layers.to_dict()
    status_rows = "".join(
        (
            '<div class="qqq-layer-row">'
            f"<dt>{escape(label)}</dt>"
            f"<dd><code>{escape(str(status_layers[field]))}</code></dd>"
            "</div>"
        )
        for field, label in _QQQ_STATUS_LAYER_LABELS
    )
    reader_priority_facts = tuple(
        fact for fact in card.priority_facts if not _contains_raw_reader_identifier(fact)
    )
    audit_priority_facts = tuple(
        fact for fact in card.priority_facts if _contains_raw_reader_identifier(fact)
    )
    priority_facts = "".join(
        f"<li><span>{index:02d}</span><strong>{escape(project_reader_text(text=fact, policy=terminology))}</strong></li>"
        for index, fact in enumerate(reader_priority_facts, start=1)
    ) or (
        "<li><span>—</span><strong>原始治理顺序已保留在审计明细；读者结论以上方四项为准。</strong></li>"
    )
    audit_priority_facts_html = "".join(f"<li>{escape(fact)}</li>" for fact in audit_priority_facts)
    audit_priority_section = (
        f"<h4>原始优先事实</h4><ol>{audit_priority_facts_html}</ol>"
        if audit_priority_facts_html
        else ""
    )
    mismatch = (
        ""
        if card.source_status_note is None
        else (
            '<p class="qqq-source-warning"><strong>历史来源不一致仍保留：</strong>'
            f"<code>{escape(card.source_status_note)}</code>。页面没有静默修正原任务文件。</p>"
        )
    )
    default_open = " open" if card.task_id in {"TRADING-2492", "TRADING-2493"} else ""
    return (
        f'<details class="qqq-task-card qqq-layer-{escape(layer.lower())}" '
        f'data-qqq-task="{escape(card.task_id)}" '
        f'data-qqq-layer="{escape(layer)}" '
        f'data-strategy-conclusion="{escape(card.status_layers.strategy_conclusion)}"'
        f"{default_open}>"
        '<summary class="qqq-task-summary">'
        '<span class="qqq-task-identity">'
        f'<span class="qqq-layer-badge">{escape(layer)} · {escape(_QQQ_LAYER_LABELS[layer])}</span>'
        "</span>"
        f"<strong>{escape(project_reader_text(text=card.title_zh, policy=terminology))}</strong>"
        f"<span>{escape(project_reader_text(text=card.positioning_zh, policy=terminology))}</span>"
        '<i aria-hidden="true">⌄</i>'
        "</summary>"
        '<div class="qqq-task-body">'
        '<div class="qqq-reader-grid">'
        '<section class="qqq-reader-fact qqq-completed">'
        "<h4>已经做到</h4>"
        f"<p>{escape(project_reader_text(text=card.completed_zh, policy=terminology))}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-not-proven">'
        "<h4>仍不能证明</h4>"
        f"<p>{escape(project_reader_text(text=card.not_proven_zh, policy=terminology))}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-blocker">'
        "<h4>为什么停在这里</h4>"
        f"<p>{escape(project_reader_text(text=card.blocker_zh, policy=terminology))}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-next">'
        "<h4>接下来要看什么</h4>"
        f"<p>{escape(project_reader_text(text=card.next_reader_action_zh, policy=terminology))}</p>"
        "</section>"
        "</div>"
        '<div class="qqq-priority-facts">'
        "<h4>必须按这个顺序理解</h4>"
        f"<ol>{priority_facts}</ol>"
        "</div>"
        '<details class="qqq-audit" data-reader-layer="audit">'
        "<summary>查看五层状态与 exact source</summary>"
        '<div class="qqq-audit-body">'
        f"{mismatch}"
        f"<p>task <code>{escape(card.task_id)}</code></p>"
        f"{audit_priority_section}"
        f'<dl class="qqq-layer-grid">{status_rows}</dl>'
        '<div class="qqq-source-ref">'
        f"<p><strong>source</strong> <code>{escape(card.source.path)}</code></p>"
        f"<p>Git blob <code>{escape(card.source.git_blob)}</code> · "
        f"bytes <code>{card.source.byte_count}</code></p>"
        "</div></div></details>"
        "</div></details>"
    )


def _render_qqq_options_projection(showcase: AtlasCitedQueryShowcase) -> str:
    _validate_qqq_options_projection_binding(showcase)
    bundle = showcase.qqq_options_projection
    by_task = {item.task_id: item for item in bundle.cards}
    group_sections = "".join(
        (
            f'<section class="qqq-group qqq-group-{index}" '
            f'data-qqq-group="{escape(group.group_id)}">'
            '<div class="qqq-group-head">'
            f'<span class="qqq-group-number">{index:02d}</span>'
            '<div><p class="qqq-group-kicker">READER GROUP</p>'
            f"<h3>{escape(project_reader_text(text=group.title_zh, policy=showcase.reader_terminology))}</h3>"
            f"<p>{escape(project_reader_text(text=group.capability_zh, policy=showcase.reader_terminology))}</p></div>"
            "</div>"
            '<div class="qqq-group-boundary">'
            f"<p><strong>还不能证明：</strong>{escape(project_reader_text(text=group.not_proven_zh, policy=showcase.reader_terminology))}</p>"
            f"<p><strong>下一道决定：</strong>{escape(project_reader_text(text=group.owner_need_zh, policy=showcase.reader_terminology))}</p>"
            "</div>"
            '<div class="qqq-task-list">'
            + "".join(
                _render_qqq_projection_card(by_task[task_id], showcase.reader_terminology)
                for task_id in group.task_ids
            )
            + "</div></section>"
        )
        for index, group in enumerate(bundle.groups, start=1)
    )
    return f"""
    <section class="qqq-projection" aria-labelledby="qqq-projection-title">
      <div class="section-kicker">QQQ OPTIONS · READER-FIRST GOVERNED PROJECTION</div>
      <div class="qqq-title-row">
        <div>
          <h2 id="qqq-projection-title">QQQ 期权研究链：做到哪里、还缺什么</h2>
          <p>这里把 13 个研发节点翻译成普通读者能直接判断的四件事：已经做到什么、仍不能证明什么、为什么被阻塞、下一步要看什么。</p>
        </div>
        <div class="qqq-count" aria-label="投影范围"><strong>13</strong><span>个 exact 节点</span><small>4 组 · 5 层状态</small></div>
      </div>
      <div class="qqq-decision" data-aggregate-conclusion="{escape(bundle.aggregate_conclusion)}">
        <div class="qqq-decision-label"><span>当前总判定</span><strong>暂不继续</strong></div>
        <div class="qqq-decision-copy">
          <p>{escape(project_reader_text(text=bundle.aggregate_explanation_zh, policy=showcase.reader_terminology))}</p>
          <details data-reader-layer="audit"><summary>查看治理原始决定</summary><code>{escape(bundle.aggregate_conclusion)}</code></details>
        </div>
      </div>
      <div class="qqq-reader-boundary" aria-label="读者结论边界">
        <div><span>可以确认</span><strong>工程合同、机械与检查工具持续完善</strong></div>
        <div><span>不能推出</span><strong>策略有效、收益稳健或具备部署条件</strong></div>
        <div><span>当前关键原因</span><strong>试点超限，Owner 已签署 aggregate NO-GO</strong></div>
      </div>
      <p class="qqq-layer-guide"><strong>A / B / C 不是成绩：</strong>A 是读者必须先看到的主线治理事实，B 是次级证据，C 是“机械已实现但 policy 未授权”。页面没有绿色成功状态。</p>
      <div class="qqq-groups">{group_sections}</div>
      <p class="qqq-projection-safety"><strong>边界：</strong>primary window 从 <code>{escape(bundle.primary_research_start)}</code> 开始；本区块只重放 Owner 已接受的 projection authority，不执行新研究、回测、外部平台、投资结论、production 或 broker action。</p>
    </section>
    """


def _render_system_flow_map(showcase: AtlasCitedQueryShowcase) -> str:
    by_question = {item.request.question_id: item for item in showcase.responses}
    if set(by_question) != set(CitedQueryQuestionId):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_FOCUS_QUESTION_SET_INVALID")

    focus_rows = (
        (
            "研究主线",
            "第 3 阶段",
            by_question[CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY],
        ),
        (
            "实际结果",
            "第 4 阶段",
            by_question[CitedQueryQuestionId.RESULT_AND_STATUS],
        ),
        (
            "结果归因",
            "第 5 阶段",
            by_question[CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS],
        ),
        (
            "快照变化",
            "第 6 阶段",
            by_question[CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION],
        ),
        (
            "证据来源",
            "第 6 阶段",
            by_question[CitedQueryQuestionId.SOURCE_LINEAGE],
        ),
    )
    focus_ledger = "".join(
        (
            '<li class="focus-item">'
            f"<span><strong>{escape(label)}</strong>"
            f"<small>{escape(stage)}</small></span>"
            "<small>已绑定研究记录</small>"
            f'<span class="focus-status">'
            f"{escape(_STATUS_LABELS[response.answer_status])}</span>"
            "</li>"
        )
        for label, stage, response in focus_rows
    )
    stage_definitions = (
        ("DATA_INPUTS", "flow-context", "上游准备"),
        ("DATA_QUALITY_GATE", "flow-context", "上游准备"),
        ("RESEARCH_MAINLINE", "flow-focus", "当前研究关注"),
        ("BACKTEST_AND_EVALUATION", "flow-focus", "当前研究关注"),
        ("RESULT_ATTRIBUTION", "flow-focus", "当前研究关注"),
        ("ATLAS_SNAPSHOT_DIFF", "flow-context", "页面可靠性检查"),
        ("CITATION_FIRST_QUERY", "flow-current", "你在这里"),
        ("OWNER_DECISION_BOUNDARY", "flow-boundary", "人工验收"),
    )
    status_provenance = _build_flow_status_provenance(showcase)
    status_by_stage = {item.stage_id: item for item in status_provenance}
    explanation_by_stage = _status_explanation_records_by_stage(showcase)
    work_progress_by_stage, concepts = _work_progress_records_by_stage(showcase)
    stage_ids = tuple(item[0] for item in stage_definitions)
    if (
        len(stage_ids) != 8
        or len(set(stage_ids)) != len(stage_ids)
        or set(status_by_stage) != set(stage_ids)
        or set(work_progress_by_stage) != set(stage_ids)
    ):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_STATUS_STAGE_SET_INVALID")
    if any(not value for item in stage_definitions for value in item):
        raise ValueError("ATLAS_CITED_QUERY_FLOW_DRILLDOWN_COPY_INVALID")
    progress_matrix = _render_flow_progress_matrix(
        showcase,
        tuple(work_progress_by_stage[stage_id] for stage_id in stage_ids),
    )
    historical_flow_lane = _render_historical_flow_lane(showcase)
    stage_cards = "".join(
        (
            f'<li class="flow-stage-shell" id="flow-stage-{escape(stage_id.lower().replace("_", "-"))}">'
            f'<details class="flow-stage {escape(role_tone)}" '
            f'data-stage="{escape(stage_id)}" '
            f'data-progress-status="{escape(status_by_stage[stage_id].status_code)}" '
            f'data-drilldown-stage="{escape(stage_id)}"'
            + (' open aria-current="step"' if stage_id == "CITATION_FIRST_QUERY" else "")
            + ">"
            '<summary class="stage-summary">'
            '<span class="stage-top">'
            f'<span class="stage-number">{index:02d}</span>'
            f'<span class="stage-badge">{escape(role_badge)}</span>'
            "</span>"
            f'<span class="stage-title">{escape(work_progress_by_stage[stage_id].display_title_zh)}</span>'
            f'<span class="stage-description">{escape(work_progress_by_stage[stage_id].work_items_zh[0])}</span>'
            f"{_render_stage_axis_summary(work_progress_by_stage[stage_id])}"
            f'<span class="stage-progress {escape(status_by_stage[stage_id].status_tone)}">'
            '<span class="progress-dot" aria-hidden="true"></span>'
            '<span class="stage-progress-copy"><small>本页状态</small>'
            f"<strong>{escape(status_by_stage[stage_id].status_label)}</strong>"
            "</span>"
            "</span>"
            '<span class="stage-disclosure-cue">'
            '<span class="cue-closed">展开读者说明</span>'
            '<span class="cue-open">收起读者说明</span>'
            '<i aria-hidden="true">⌄</i>'
            "</span>"
            "</summary>"
            f'<div class="stage-drilldown" data-drilldown-source="{escape(explanation_by_stage[stage_id].status_object_scope)}">'
            f"{_render_reader_status_explanation(explanation_by_stage[stage_id], work_progress_by_stage[stage_id], concepts)}"
            "</div>"
            "</details>"
            "</li>"
        )
        for index, (
            stage_id,
            role_tone,
            role_badge,
        ) in enumerate(stage_definitions, start=1)
    )
    concept_library = _render_concept_library(work_progress_by_stage, concepts)
    provenance_ledger = "".join(
        (
            f'<li class="provenance-item" data-provenance-stage="{escape(item.stage_id)}" '
            f'data-provenance-source="{escape(item.source_kind)}">'
            '<div class="provenance-head">'
            f'<code class="provenance-stage">{escape(item.stage_id)}</code>'
            f'<span class="provenance-status {escape(item.status_tone)}">'
            '<i aria-hidden="true"></i>'
            f"{escape(item.status_label)}</span>"
            "</div>"
            f'<p class="provenance-reason">{escape(item.reason_zh)}</p>'
            '<details data-reader-layer="audit">'
            "<summary>查看 exact 状态依据</summary>"
            f'<p class="provenance-source">source kind <code>{escape(item.source_kind)}</code></p>'
            '<ul class="provenance-refs">'
            + "".join(f"<li><code>{escape(ref)}</code></li>" for ref in item.exact_refs)
            + "</ul>"
            "</details>"
            "</li>"
        )
        for item in status_provenance
    )
    return f"""
    <section class="flow-map" aria-labelledby="system-flow-title">
      <div class="section-kicker">WHOLE-SYSTEM MAP · READ-ONLY VIEW</div>
      <div class="flow-heading">
        <div>
          <h2 id="system-flow-title">策略系统全流程，以及你现在在哪里</h2>
          <p>先看全局，再进入五个研究问题。本图说明信息怎样走到当前页面，不表示本页重新执行了数据质量、回测或投资决策。</p>
        </div>
        <div class="you-are-here" aria-label="当前页面位置">
          <span>你在这里</span>
          <strong>第 7 / 8 阶段</strong>
          <small>Citation-first 查询与证据展示</small>
        </div>
      </div>
      <div class="flow-legend" aria-label="流程图图例">
        <span><i class="legend-context"></i>上游上下文</span>
        <span><i class="legend-focus"></i>当前研究关注路径</span>
        <span><i class="legend-current"></i>当前页面位置</span>
        <span><i class="legend-boundary"></i>本页以外的决策边界</span>
      </div>
      {progress_matrix}
      <div class="progress-key" aria-label="节点进展状态图例">
        <strong>本页状态图例</strong>
        <span class="progress-neutral"><i aria-hidden="true"></i>本页未执行</span>
        <span class="progress-active"><i aria-hidden="true"></i>研究进行中</span>
        <span class="progress-limited"><i aria-hidden="true"></i>证据有限</span>
        <span class="progress-validated"><i aria-hidden="true"></i>已验证</span>
        <span class="progress-review"><i aria-hidden="true"></i>待人工复核</span>
        <small>这一行只解释“本页状态”；工程能力与研究证据请看上方矩阵和每张节点卡的两个独立标签。</small>
      </div>
      <p class="drilldown-help"><strong>怎样展开：</strong>点击任一节点，先读“为什么需要、具体做什么、目前进展、预期产物”，再看使用方式和边界；遇到陌生概念可继续点开通俗解释，状态限制与技术依据收在最后。当前页面节点默认展开。</p>
      <ol class="system-flow">{stage_cards}</ol>
      {concept_library}
      <div class="focus-panel">
        <div class="focus-copy">
          <p class="section-kicker">当前关注 · 精确对象</p>
          <h3>当前实际关注路径</h3>
          <p>这些节点直接来自本页五个 canonical requests。没有“最相关”排序、模糊匹配或名称推断。</p>
        </div>
        <ul class="focus-ledger">{focus_ledger}</ul>
      </div>
      {historical_flow_lane}
      <section class="provenance-panel" aria-labelledby="status-provenance-title" data-reader-layer="audit">
        <div class="provenance-copy">
          <p class="section-kicker">AUDIT APPENDIX · STRUCTURED FIELDS ONLY</p>
          <h3 id="status-provenance-title">状态审计附录</h3>
          <p>供需要追溯的读者核对状态对象与原始依据；主要阅读结论来自已经验证的状态说明与工作进展 sidecar。</p>
        </div>
        <ol class="provenance-ledger">{provenance_ledger}</ol>
        <p class="provenance-boundary"><strong>怎样理解：</strong><code>VALIDATED</code> 只表示 evidence response/diff 的 validator PASS，不等于 strategy PASS 或投资评级；<code>LIMITED</code> 保留证据限制；<code>NOT_EXECUTED_BY_PAGE</code> 不是 DQ FAIL。</p>
      </section>
      <p class="flow-safety"><strong>边界：</strong>本页只读取已验证的 Atlas snapshot/diff 并展示引用；不会运行 <code>aits validate-data</code>、回测、promotion、production 或 broker action。</p>
    </section>
    """


_PAGE_FRESHNESS_LABELS = {
    PageFreshnessStatus.CURRENT: "与生成时仓库一致",
    PageFreshnessStatus.REPOSITORY_AHEAD_NO_RELEVANT_DRIFT: "仓库已推进，但页面相关内容未漂移",
    PageFreshnessStatus.STALE_REBUILD_REQUIRED: "页面已过期，需要重建",
    PageFreshnessStatus.UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED: "有后继任务尚未完成影响分类",
}
_PAGE_ACCEPTANCE_LABELS = {
    PageAcceptanceStatus.PASS: "已通过",
    PageAcceptanceStatus.FAIL: "未通过",
    PageAcceptanceStatus.PENDING_REVIEW: "等待独立验收",
    PageAcceptanceStatus.NOT_EXECUTED: "尚未执行",
}


@dataclass(frozen=True)
class _InlineTermFrame:
    tag: str
    reader_section: str | None
    blocked: bool


@dataclass(frozen=True)
class _DisclosureFrame:
    source_tag: str
    output_tag: str
    nested_details: bool


class _DisclosureFlatteningParser(HTMLParser):
    _VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []
        self.stack: list[_DisclosureFrame] = []
        self.details_depth = 0

    @staticmethod
    def _attrs(attrs: list[tuple[str, str | None]], *, extra_class: str) -> str:
        rendered: list[str] = []
        class_seen = False
        for key, value in attrs:
            if key == "open":
                continue
            if key == "class":
                class_seen = True
                value = f"{value or ''} {extra_class}".strip()
            rendered.append(f" {key}" if value is None else f' {key}="{escape(value, quote=True)}"')
        if not class_seen:
            rendered.append(f' class="{extra_class}"')
        return "".join(rendered)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        nested_details = tag == "details" and self.details_depth > 0
        flattened_summary = (
            tag == "summary"
            and bool(self.stack)
            and self.stack[-1].source_tag == "details"
            and self.stack[-1].nested_details
        )
        if nested_details:
            output_tag = "section"
            self.parts.append(f"<section{self._attrs(attrs, extra_class='flat-disclosure')}>")
        elif flattened_summary:
            output_tag = "p"
            self.parts.append(f"<p{self._attrs(attrs, extra_class='flat-disclosure-title')}>")
        else:
            output_tag = tag
            self.parts.append(self.get_starttag_text() or f"<{tag}>")
        if tag == "details":
            self.details_depth += 1
        if tag not in self._VOID_TAGS:
            self.stack.append(_DisclosureFrame(tag, output_tag, nested_details))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del tag, attrs
        self.parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].source_tag == tag:
                frame = self.stack[index]
                del self.stack[index:]
                self.parts.append(f"</{frame.output_tag}>")
                if tag == "details":
                    self.details_depth -= 1
                return
        self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.parts.append(f"<!{decl}>")


def _flatten_nested_disclosures(html: str) -> str:
    parser = _DisclosureFlatteningParser()
    parser.feed(html)
    parser.close()
    return "".join(parser.parts)


class _InlineTermParser(HTMLParser):
    _VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
    _BLOCKED_TAGS = {"a", "button", "code", "script", "style", "template"}

    def __init__(self, policy: ReaderTerminologyPolicy) -> None:
        super().__init__(convert_charrefs=False)
        self.policy = policy
        self.parts: list[str] = []
        self.stack: list[_InlineTermFrame] = []
        self.first_seen: set[tuple[str, str]] = set()

    def _matches(self, text: str) -> tuple[tuple[int, int, ReaderTermDefinition, str], ...]:
        candidates: list[tuple[int, int, ReaderTermDefinition, str]] = []
        for term in self.policy.terms:
            for alias in sorted(term.aliases, key=len, reverse=True):
                escaped_alias = re.escape(alias)
                pattern = (
                    re.compile(rf"(?<![A-Za-z0-9_]){escaped_alias}(?![A-Za-z0-9_])")
                    if re.search(r"[A-Za-z0-9]", alias)
                    else re.compile(escaped_alias)
                )
                for match in pattern.finditer(text):
                    candidates.append((match.start(), match.end(), term, match.group(0)))
        selected: list[tuple[int, int, ReaderTermDefinition, str]] = []
        occupied: set[int] = set()
        for candidate in sorted(
            candidates,
            key=lambda item: (item[0], -(item[1] - item[0]), item[2].term_id),
        ):
            start, end, _term, _matched = candidate
            if any(index in occupied for index in range(start, end)):
                continue
            selected.append(candidate)
            occupied.update(range(start, end))
        return tuple(selected)

    def _render_text(self, text: str, section: str) -> str:
        text = project_reader_text(text=text, policy=self.policy)
        matches = self._matches(text)
        if not matches:
            return text
        result: list[str] = []
        cursor = 0
        for start, end, term, matched in matches:
            result.append(text[cursor:start])
            key = (section, term.term_id)
            first = key not in self.first_seen
            if first:
                self.first_seen.add(key)
            description_id = f"term-description-{term.term_id}"
            trigger = (
                f'<span class="term-trigger" data-term-trigger="{escape(term.term_id)}" '
                f'data-term-first="{str(first).lower()}" '
                f'aria-describedby="{escape(description_id)}" '
                f'data-term-short="{escape(term.plain_definition_zh, quote=True)}"'
                + (
                    f' tabindex="0" role="button" aria-expanded="false" '
                    f'aria-controls="reader-term-{escape(term.term_id)}"'
                    if first
                    else ""
                )
                + f">{matched}</span>"
            )
            full_definition_link = (
                f'<a class="term-full-link" href="#reader-term-{escape(term.term_id)}">完整定义</a>'
                if first
                else ""
            )
            result.append(
                '<span class="term-context" data-term-placement="below">'
                + trigger
                + '<span class="term-popover" '
                + f'data-term-short="{escape(term.plain_definition_zh, quote=True)}">'
                + full_definition_link
                + "</span></span>"
            )
            cursor = end
        result.append(text[cursor:])
        return "".join(result)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {key: "" if value is None else value for key, value in attrs}
        parent = self.stack[-1] if self.stack else None
        section = attr_map.get("data-reader-section") or (
            None if parent is None else parent.reader_section
        )
        blocked = bool(
            (parent is not None and parent.blocked)
            or tag in self._BLOCKED_TAGS
            or attr_map.get("data-reader-layer") == "audit"
            or "data-term-definition" in attr_map
            or "data-term-glossary" in attr_map
        )
        self.parts.append(self.get_starttag_text() or f"<{tag}>")
        if tag not in self._VOID_TAGS:
            self.stack.append(_InlineTermFrame(tag, section, blocked))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del tag, attrs
        self.parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag: str) -> None:
        self.parts.append(f"</{tag}>")
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].tag == tag.lower():
                del self.stack[index:]
                break

    def handle_data(self, data: str) -> None:
        frame = self.stack[-1] if self.stack else None
        if frame is None or frame.blocked or frame.reader_section is None:
            self.parts.append(data)
            return
        self.parts.append(self._render_text(data, frame.reader_section))

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.parts.append(f"<!{decl}>")


def _add_inline_term_interactions(html: str, policy: ReaderTerminologyPolicy) -> str:
    parser = _InlineTermParser(policy)
    parser.feed(html)
    parser.close()
    return "".join(parser.parts)


def _render_term_definition_bank(policy: ReaderTerminologyPolicy) -> str:
    definitions = "".join(
        (
            f'<p class="term-description" id="term-description-{escape(term.term_id)}" '
            f'data-term-definition="{escape(term.term_id)}">'
            f"{escape(term.display_name_zh)}：{escape(term.plain_definition_zh)} "
            f"{escape(term.why_needed_zh)}</p>"
        )
        for term in policy.terms
    )
    return f'<div class="term-definition-bank" aria-label="术语短解释">{definitions}</div>'


def _render_reader_terminology_guide(policy: ReaderTerminologyPolicy) -> str:
    cards = "".join(
        (
            f'<article class="terminology-card" id="reader-term-{escape(term.term_id)}" '
            "data-term-glossary-card>"
            '<div class="terminology-card-head">'
            f"<h3>{escape(term.display_name_zh)}</h3>"
            f"<code>{escape(term.aliases[0])}</code>"
            "</div>"
            f"<p><strong>简单说：</strong>{escape(term.plain_definition_zh)}</p>"
            f"<p><strong>为什么页面需要它：</strong>{escape(term.why_needed_zh)}</p>"
            "</article>"
        )
        for term in policy.terms
    )
    return f"""
    <details class="terminology-guide" id="reader-terminology-guide" data-term-glossary data-reader-card="terminology-guide">
      <summary>按需查阅：本页 {len(policy.terms)} 个术语的完整定义</summary>
      <div class="terminology-guide-intro">
        <h2>术语索引与完整定义</h2>
        <p>{escape(policy.reader_profile.audience_zh)}</p>
        <p>日常阅读不需要先记住这张表；在正文中 hover、键盘聚焦或轻触术语即可看到同一短解释。任务、来源、校验值和原因代码的原始编号只保留在审计区。</p>
      </div>
      <div class="terminology-grid">{cards}</div>
    </details>
    """


def _render_page_effectiveness(showcase: AtlasCitedQueryShowcase) -> str:
    manifest = showcase.page_effectiveness
    if not manifest.task_coverage:
        raise ValueError("ATLAS_PAGE_EFFECTIVENESS_TASK_COVERAGE_INVALID")
    acceptance = "".join(
        (
            f'<li class="effectiveness-review-card" data-review-track="{escape(item.track.value)}" '
            f'data-review-status="{escape(item.status.value)}">'
            f"<strong>{escape(_PAGE_ACCEPTANCE_TRACK_LABELS[item.track])}</strong>"
            f"<span>{escape(_PAGE_ACCEPTANCE_LABELS[item.status])}</span>"
            "</li>"
        )
        for item in manifest.acceptance
    )
    acceptance_audit = "".join(
        f"<li><code>{escape(item.track.value)}</code> = "
        f"<code>{escape(item.status.value)}</code></li>"
        for item in manifest.acceptance
    )
    successor_rows = "".join(
        (
            f'<li data-successor-task="{escape(item.task_id)}" '
            f'data-successor-coverage="{escape(item.coverage)}">'
            f"<code>{escape(item.task_id.split('_', 1)[0])}</code>"
            f"<span>{escape(item.reader_summary_zh)}</span>"
            "</li>"
        )
        for item in manifest.task_coverage
        if 2494 <= page_task_identity_sort_key(item.task_id)[0] <= 2532
    )
    return f"""
    <section class="page-effectiveness" id="page-effectiveness" aria-labelledby="page-effectiveness-title" data-page-freshness="{escape(manifest.freshness_status.value)}" data-task-coverage-count="{len(manifest.task_coverage)}">
      <div class="effectiveness-title-row">
        <div>
          <p class="section-kicker">页面有效性 · 四个独立层次</p>
          <h2 id="page-effectiveness-title">先确认：这张页面现在还能不能信</h2>
          <p>页面可打开只说明浏览器读到了 HTML。这里把来源、语义、视觉和读者理解分开验收，避免旧页面继续显示却遗漏新的研究事实。</p>
        </div>
        <div class="freshness-badge" data-freshness-status="{escape(manifest.freshness_status.value)}">
          <span>页面时效性</span>
          <strong>{escape(_PAGE_FRESHNESS_LABELS[manifest.freshness_status])}</strong>
        </div>
      </div>
      <div class="reader-answer-grid" aria-label="读者先回答的六个问题">
        <article><span>01 · 当前主线</span><strong>策略研究重新开放条件已经登记；当前仍须保持研究关闭，而且只允许补齐预先登记的证据，不能进行候选搜索或经验回测。</strong></article>
        <article><span>02 · 最大阻塞</span><strong>18 个 G3 证据槽位尚无通过 DQ/PIT（数据质量与时点可得性）准入的主研究窗口结果，G2 数值政策仍有 0 项获批。</strong></article>
        <article><span>03 · 已做到什么</span><strong>工程合同、数据质量与时点可得性检查、离线工程机制、10 条汇总序列收集器与证据结构均可重放；这只是能力，不是盈利或风险证据。</strong></article>
        <article><span>04 · 不能推出什么</span><strong>不能推出策略有效、收益稳健或风险可接受，也不能把局部工程许可解释成策略结论通过。</strong></article>
        <article><span>05 · 下一步</span><strong>2532 的唯一零订单外部验证已经完成：1202 个交易日中 1201 个最终看到 option chain，只有 1 个全日未见；旧结果中的 1019 个 missing 是首条 Slice 提前结算造成的 collector 混淆。当前只允许审查这份 export-safe aggregate 是否满足 canonical DQ/PIT 证据准入，不重跑 Cloud，也不自动解锁策略或交易。</strong></article>
        <article class="reader-answer-stop"><span>06 · 现在能否投资或下单</span><strong>不能。期权合约选择保持关闭，订单和成交数量均为 0；本页不授权真实策略执行引擎、外部动作或交易。</strong></article>
      </div>
      <div class="effectiveness-boundary">
        <div>
          <h3>三种“通过”互不代签</h3>
          <ul class="effectiveness-review-grid">{acceptance}</ul>
          <p>工程自动化只能更新工程验收；Owner 视觉验收和目标读者理解验收必须来自真实人工事实。</p>
        </div>
        <details class="successor-coverage" data-reader-layer="audit">
          <summary>查看 TRADING-2494–2532（含 2523A/2523B）如何影响当前页面</summary>
          <ul>{successor_rows}</ul>
        </details>
      </div>
      <details class="effectiveness-audit" data-reader-layer="audit">
        <summary>审计信息：exact commit、policy 与 source coverage</summary>
        <dl>
          <div><dt>freshness status</dt><dd><code>{escape(manifest.freshness_status.value)}</code></dd></div>
          <div><dt>repository commit</dt><dd><code>{escape(manifest.repository_commit)}</code></dd></div>
          <div><dt>source snapshot</dt><dd><code>{escape(manifest.source_snapshot_commit)}</code></dd></div>
          <div><dt>policy SHA-256</dt><dd><code>{escape(manifest.policy_sha256)}</code></dd></div>
          <div><dt>覆盖范围</dt><dd><code>TRADING-2481..2504, 2506..2532, 2523A, 2523B</code> · {len(manifest.source_artifacts)} semantic sources</dd></div>
        </dl>
        <h3>验收原始状态</h3><ul>{acceptance_audit}</ul>
      </details>
    </section>
    """


def _build_why_first_projection(
    showcase: AtlasCitedQueryShowcase,
) -> tuple[tuple[_ReaderCausalNode, ...], tuple[_ReaderCausalEdge, ...]]:
    manifest = showcase.page_effectiveness
    calibration = _task_coverage(manifest, "2510")
    readiness = _task_coverage(manifest, "2515")
    observed_evidence = _task_coverage(manifest, "2530")
    collector_fix = _task_coverage(manifest, "2531")
    next_step = _task_coverage(manifest, "2532")
    qqq_source = _ReaderCausalSource(
        source_ref_id="qqq_options_projection",
        source_locator="config/atlas/qqq_options_projection.yaml",
        source_sha256=showcase.qqq_options_projection.policy_sha256,
    )
    nodes = (
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.PROBLEM,
            question_zh="我们真正要回答什么？",
            answer_zh="现有已准入证据，是否足以让策略研究重新开放？",
            sources=(_coverage_source(readiness),),
        ),
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.CONSTRAINT,
            question_zh="为什么不能直接跳到策略结论？",
            answer_zh=calibration.reader_summary_zh,
            sources=(_coverage_source(calibration), _coverage_source(readiness)),
        ),
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.CHOICE,
            question_zh="为什么选择当前研究路径？",
            answer_zh=(
                "先修复 2530 暴露的 session 结算与 underlying 来源混淆，再用严格的一次性准入验证修复后的"
                "整日结果；这样可以先分清 collector 问题和真实 transport 缺口，再决定是否进入 DQ/PIT 审查。"
            ),
            sources=(
                _coverage_source(readiness),
                _coverage_source(collector_fix),
                _coverage_source(next_step),
            ),
        ),
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.EVIDENCE,
            question_zh="这条路径目前拿到了什么证据？",
            answer_zh=next_step.reader_summary_zh,
            sources=(
                _coverage_source(observed_evidence),
                _coverage_source(collector_fix),
                _coverage_source(next_step),
            ),
        ),
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.RESULT,
            question_zh="现有证据只支持什么结论？",
            answer_zh=(
                "2532 证明修复后的整日结算得到 1201 个 chain-present session 和 1 个 never-chain session；"
                "旧结果中的 1019 个 missing 来自首条 Slice 提前结算混淆。它只解决 collector 与 transport 归因，"
                "尚未完成 DQ/PIT 准入，也不能形成策略结论。"
            ),
            sources=(
                qqq_source,
                _coverage_source(observed_evidence),
                _coverage_source(collector_fix),
                _coverage_source(next_step),
            ),
        ),
        _ReaderCausalNode(
            kind=ReaderCausalNodeKind.NEXT_STEP,
            question_zh="当前结果把下一步指向哪里？",
            answer_zh=(
                "唯一零订单外部验证已经完成且不能重跑；下一步只审查这份 export-safe aggregate 是否满足"
                "canonical DQ/PIT 证据准入，再由人工决定是否继续研究。"
            ),
            sources=(
                _coverage_source(next_step),
                _coverage_source(collector_fix),
            ),
        ),
    )
    contract_source = _ReaderCausalSource(
        source_ref_id=showcase.reader_projection_contract.contract_id,
        source_locator="config/atlas/reader_projection_contract.yaml",
        source_sha256=showcase.reader_projection_contract_sha256,
    )
    edges = tuple(
        _ReaderCausalEdge(
            source_node=item.source_node,
            relation=item.relation,
            target_node=item.target_node,
            source=contract_source,
        )
        for item in showcase.reader_projection_contract.causal_edges
    )
    if tuple(item.kind for item in nodes) != showcase.reader_projection_contract.causal_nodes:
        raise ValueError("ATLAS_READER_CAUSAL_NODE_ORDER_INVALID")
    if tuple((item.source_node, item.relation, item.target_node) for item in edges) != tuple(
        (item.source_node, item.relation, item.target_node)
        for item in showcase.reader_projection_contract.causal_edges
    ):
        raise ValueError("ATLAS_READER_CAUSAL_EDGE_SET_INVALID")
    if any(not node.sources for node in nodes) or any(
        not edge.source.source_sha256 for edge in edges
    ):
        raise ValueError("ATLAS_READER_CAUSAL_SOURCE_BINDING_INSUFFICIENT")
    return nodes, edges


_CAUSAL_EDGE_LABELS = {
    ReaderCausalEdgeKind.BOUNDED_BY: "受此约束",
    ReaderCausalEdgeKind.JUSTIFIES: "所以选择",
    ReaderCausalEdgeKind.REQUIRES_EVIDENCE: "需要证据",
    ReaderCausalEdgeKind.SUPPORTS: "目前支持",
    ReaderCausalEdgeKind.LIMITS: "限定范围",
    ReaderCausalEdgeKind.TRIGGERS: "因此触发",
}


def _render_trust_strip(showcase: AtlasCitedQueryShowcase) -> str:
    manifest = showcase.page_effectiveness
    state = showcase.reader_state
    acceptance = {item.track: item for item in manifest.acceptance}
    context_source_refs = " ".join(
        (
            showcase.reader_projection_contract.contract_id,
            _task_coverage(manifest, "2510").task_id,
            _task_coverage(manifest, "2515").task_id,
            _task_coverage(manifest, "2530").task_id,
            _task_coverage(manifest, "2531").task_id,
            _task_coverage(manifest, "2532").task_id,
        )
    )
    return f"""
    <section class="trust-strip" data-reader-section="TRUST_STRIP" data-reader-card="trust-strip" data-page-freshness="{escape(manifest.freshness_status.value)}" data-source-commit="{escape(manifest.repository_commit)}">
      <div class="trust-grid" aria-label="页面身份与安全边界">
        <p data-always-visible="source_commit">代码版本：已锁定，完整值可在页末核对。</p>
        <p><span data-always-visible="evidence_date">数据截至：{escape(state.dates.data_as_of or "未知")}</span><span data-always-visible="page_date">页面生成：{escape(state.dates.page_generated_at)}</span></p>
        <p><span data-always-visible="freshness">页面状态：{escape(_PAGE_FRESHNESS_LABELS[manifest.freshness_status])}</span><span data-always-visible="engineering_validation">工程检查：{escape(_PAGE_ACCEPTANCE_LABELS[acceptance[PageAcceptanceTrack.ENGINEERING_VALIDATION].status])}</span></p>
        <p><span data-always-visible="owner_visual_review">视觉检查：{escape(_PAGE_ACCEPTANCE_LABELS[acceptance[PageAcceptanceTrack.OWNER_VISUAL_REVIEW].status])}</span><span data-always-visible="reader_comprehension_review">理解检查：{escape(_PAGE_ACCEPTANCE_LABELS[acceptance[PageAcceptanceTrack.READER_COMPREHENSION_REVIEW].status])}</span></p>
        <p class="trust-grid-boundary"><span data-always-visible="strategy_conclusion_pass_count">策略结论：尚未形成</span><span data-always-visible="production_effect">生产动作：无</span><span data-always-visible="broker_action">交易动作：无</span></p>
      </div>
      <header>
        <p class="eyebrow">策略研究说明 · 只读页面</p>
        <h1 data-always-visible="current_problem" data-reader-claim-source-refs="{escape(context_source_refs)}">这项策略研究为什么还不能继续？</h1>
        <p class="lead">这套系统不会直接给出买卖答案。它先确认数据可靠，再评价策略，最后由人工决定是否继续。</p>
        <div class="system-orientation" data-reader-overview="SYSTEM_CONTEXT" data-context-source-refs="{escape(context_source_refs)}">
          <p class="system-orientation-title">系统怎样从想法走到行动</p>
          <ol>
            <li data-system-stage="RESEARCH_QUESTION"><span>01 · 提出问题</span><strong>先说明真正想判断什么</strong></li>
            <li data-system-stage="TRUSTED_EVIDENCE"><span>02 · 检查数据</span><strong>确认用于判断的数据完整可靠</strong></li>
            <li data-system-stage="HUMAN_DECISION"><span>03 · 形成结论</span><strong>只说现有数据真正支持的部分</strong></li>
            <li data-system-stage="AUTHORIZED_EXECUTION"><span>04 · 决定行动</span><strong>由人工决定是否继续，页面不会自行执行</strong></li>
          </ol>
          <p class="system-orientation-current"><strong>当前停在第 02 步：</strong>v2 的唯一外部验证已确认 collector 修复后的结果；现在还要判断这些安全汇总是否足够完整、来源和时点是否可信，尚未进入策略评价。</p>
        </div>
      </header>
      <p class="trust-stop" data-always-visible="critical-risk">本页只解释研究状态，不提供投资建议，也不会运行策略、连接外部系统或下单。</p>
    </section>
    """


def _render_why_context(showcase: AtlasCitedQueryShowcase) -> str:
    nodes, edges = _build_why_first_projection(showcase)
    context_source_refs = " ".join(
        sorted({source.source_ref_id for node in nodes for source in node.sources})
    )
    edge_by_pair = {(item.source_node, item.target_node): item for item in edges}
    node_by_kind = {item.kind: item for item in nodes}

    def source_refs(*kinds: ReaderCausalNodeKind) -> str:
        return " ".join(
            sorted(
                {source.source_ref_id for kind in kinds for source in node_by_kind[kind].sources}
            )
        )

    node_cards: list[str] = []
    for index, node in enumerate(nodes):
        answer = project_reader_text(text=node.answer_zh, policy=showcase.reader_terminology)
        source_ids = " ".join(item.source_ref_id for item in node.sources)
        edge_attributes = ""
        edge_marker = ""
        if index:
            previous = nodes[index - 1]
            incoming = edge_by_pair[(previous.kind, node.kind)]
            edge_attributes = (
                f' data-causal-edge="{escape(incoming.relation.value)}"'
                f' data-causal-edge-source="{escape(incoming.source.source_ref_id)}"'
            )
            edge_marker = (
                f'<span class="causal-link">{escape(_CAUSAL_EDGE_LABELS[incoming.relation])}</span>'
            )
        node_cards.append(
            f'<li class="causal-node" data-causal-node="{escape(node.kind.value)}" '
            f'data-causal-source-count="{len(node.sources)}" data-causal-source-refs="{escape(source_ids)}"'
            f"{edge_attributes}>"
            f"{edge_marker}"
            f'<span class="causal-number">{index + 1:02d}</span>'
            f'<p class="causal-question">{escape(node.question_zh)}</p>'
            f'<p class="causal-answer">{escape(answer)}</p></li>'
        )
    audit_sources = {
        (source.source_ref_id, source.source_locator, source.source_sha256)
        for node in nodes
        for source in node.sources
    }
    audit_sources.update(
        (edge.source.source_ref_id, edge.source.source_locator, edge.source.source_sha256)
        for edge in edges
    )
    audit_rows = "".join(
        f"<li><code>{escape(source_ref)}</code><br>{escape(locator)}<br>"
        f"SHA-256 <code>{escape(source_sha)}</code></li>"
        for source_ref, locator, source_sha in sorted(audit_sources)
    )
    return f"""
    <section class="why-context" data-reader-section="WHY_CONTEXT" data-reader-card="why-context" aria-labelledby="why-context-title">
      <div class="reader-section-heading">
        <p class="section-kicker">20 秒先看懂</p>
        <h2 id="why-context-title">当前决定、原因和下一步</h2>
        <p>下面四项是本页默认答案；无需展开，也无需先查看词语说明。</p>
      </div>
      <div class="research-closure" data-reader-layer="reader" data-reader-context="RESEARCH_CLOSURE" data-context-source-refs="{escape(context_source_refs)}">
        <div class="reader-decision-grid" aria-label="当前研究的四个默认答案">
          <article class="reader-decision-card" data-reader-decision="CURRENT_DECISION" data-reader-claim-source-refs="{escape(source_refs(ReaderCausalNodeKind.RESULT))}">
            <span>01 · 当前决定</span><strong data-always-visible="conclusion_boundary">暂不继续形成策略结论。</strong>
          </article>
          <article class="reader-decision-card" data-reader-decision="WHY_PAUSED" data-reader-claim-source-refs="{escape(source_refs(ReaderCausalNodeKind.CONSTRAINT, ReaderCausalNodeKind.EVIDENCE))}">
            <span>02 · 为什么</span><strong data-always-visible="largest_blocker">新的外部结果已分清 collector 混淆和最终 transport 缺口，但还没有确认这份安全汇总是否完整、来源是否明确，以及研究当时能否看到。</strong>
          </article>
          <article class="reader-decision-card" data-reader-decision="CURRENT_WORK" data-reader-claim-source-refs="{escape(source_refs(ReaderCausalNodeKind.CHOICE))}">
            <span>03 · 现在在查什么</span><strong>唯一零订单验证已完成：1201 天最终看到 option chain，1 天全日未见；当前在封存证据并准备独立的数据可信性审查。</strong>
          </article>
          <article class="reader-decision-card" data-reader-decision="NEXT_STEP" data-reader-claim-source-refs="{escape(source_refs(ReaderCausalNodeKind.NEXT_STEP))}">
            <span>04 · 下一步</span><strong data-always-visible="next_legal_action">只对已封存的安全汇总检查来源、完整性和研究当时是否可见；不重跑 Cloud，也不自动解锁策略、引擎或交易。</strong>
          </article>
        </div>
        <p class="reader-safety" data-always-visible="prohibited_inference" data-reader-claim-source-refs="{escape(source_refs(ReaderCausalNodeKind.RESULT))}"><strong>当前不能推出：</strong>这既不能证明策略有效，也不能证明策略无效；更不表示可以投资、部署或交易。</p>
        <ol class="reader-plain-flow" aria-label="当前研究与前后步骤的关系">
          <li><span>已经收集数据</span></li>
          <li><span>离线修复诊断方法</span></li>
          <li><span>完成唯一零订单外部验证</span></li>
          <li class="is-current"><span>当前：检查数据可信性</span></li>
          <li><span>人工决定是否继续</span></li>
        </ol>
      </div>
      <details class="local-research-explanation" data-reader-layer="research">
        <summary>展开：为什么这样判断、当前具体查什么</summary>
        <div class="local-research-explanation-body" data-reader-context="CURRENT_LOCAL_CHAIN">
          <div class="local-why-heading">
            <p class="section-kicker">研究解释 · 来源已绑定</p>
            <h3>在这个整体上下文下，本次研究为什么按现在的顺序推进？</h3>
            <p>下面六步解释当前局部选择；每一步都绑定已校验来源。问题、选择理由或因果边缺失时，页面必须显示信息不足，不能由渲染器补写。</p>
          </div>
          <p class="reader-problem">当前研究问题：现有已准入证据是否足以让策略研究重新开放？</p>
          <ol class="causal-chain">{"".join(node_cards)}</ol>
          <div class="why-boundary-grid">
            <p data-research-detail="largest_blocker">最大阻塞：v2 外部结果已取得，但 export-safe aggregates 还没有成为 canonical DQ/PIT admitted evidence。</p>
            <p data-research-detail="prohibited_inference">禁止推断：不能据此宣称策略有效、收益稳健、风险可接受或可以下单。</p>
            <p data-research-detail="next_legal_action">下一合法动作：只审查已封存 aggregates 的来源、完整性与时点可得性；不授权第二次 Cloud run。</p>
          </div>
        </div>
      </details>
    </section>
    <div class="causal-audit-section" data-reader-card="why-context-audit" data-reader-layer="audit">
      <details class="causal-audit">
        <summary>核对这条因果链的 {len(audit_sources)} 个来源绑定</summary>
        <ul>{audit_rows}</ul>
      </details>
    </div>
    """


def _render_canonical_questions(navigation: str, cards: str) -> str:
    return f"""
    <section class="canonical-questions" data-reader-section="CANONICAL_QUESTIONS" aria-labelledby="canonical-questions-title">
      <div class="reader-section-heading">
        <p class="section-kicker">FIVE QUESTIONS · DEFAULT ANSWERS</p>
        <h2 id="canonical-questions-title">先用五个固定问题读懂当前研究</h2>
        <p>每张卡先给一句话回答与核心限制；完整引用留在卡片自己的审计入口。</p>
      </div>
      <nav aria-label="五个固定问题">{navigation}</nav>
      <div class="answer-grid">{cards}</div>
    </section>
    """


_CHANGE_LABELS = {
    ReaderChangeKind.CHANGED: "已有改变",
    ReaderChangeKind.UNCHANGED: "没有改变",
    ReaderChangeKind.UNKNOWN: "变化未知",
    ReaderChangeKind.NOT_COMPARABLE: "不可比较",
}


def _render_change_summary(showcase: AtlasCitedQueryShowcase) -> str:
    state = showcase.reader_state
    return f"""
    <section class="change-summary" data-reader-section="CHANGE_SUMMARY" data-reader-card="change-summary" aria-labelledby="change-summary-title">
      <div class="reader-section-heading">
        <p class="section-kicker">WHAT CHANGED · OBJECT-QUALIFIED</p>
        <h2 id="change-summary-title">相对上一份研究快照，什么变了？</h2>
      </div>
      <p class="change-state" data-change-kind="{escape(state.change.change_kind.value)}">{escape(_CHANGE_LABELS[state.change.change_kind])}</p>
      <p data-always-visible="change_summary">{escape(project_reader_text(text=state.change.explanation_zh, policy=showcase.reader_terminology))}</p>
      <dl class="date-context">
        <div><dt>数据截至</dt><dd>{escape(state.dates.data_as_of or "未知")}</dd></div>
        <div><dt>证据评估</dt><dd>{escape(state.dates.evidence_evaluated_at or "未知")}</dd></div>
        <div><dt>页面生成</dt><dd>{escape(state.dates.page_generated_at)}</dd></div>
        <div><dt>比较基准日期</dt><dd>{escape(state.change.comparison_base_date or "不适用")}</dd></div>
      </dl>
      <details data-reader-layer="audit"><summary>查看比较基准身份</summary><code>{escape(state.change.comparison_base_id or "none")}</code></details>
    </section>
    """


def _render_conclusion_boundary(showcase: AtlasCitedQueryShowcase) -> str:
    bundle = showcase.qqq_options_projection
    state = showcase.reader_state
    return f"""
    <section class="conclusion-boundary" data-reader-section="CONCLUSION_BOUNDARY" data-reader-card="conclusion-boundary" aria-labelledby="conclusion-boundary-title">
      <div class="reader-section-heading">
        <p class="section-kicker">NARROWEST SUPPORTED RESULT</p>
        <h2 id="conclusion-boundary-title">现有证据把结论限制在哪里？</h2>
      </div>
      <p class="boundary-result" data-always-visible="conclusion_boundary">{escape(project_reader_text(text=bundle.aggregate_explanation_zh, policy=showcase.reader_terminology))}</p>
      <p class="boundary-risk" data-always-visible="critical_risk">关键风险：组合 transport gate 只给出总拒绝，不能唯一定位 quote、Greeks、OI、volume 或交叉条件中的具体失败轴。</p>
      <p data-always-visible="prohibited_inference">{escape(state.prohibited_inference_zh)}</p>
      <p data-always-visible="stop_reason">停止原因：唯一外部授权已经消费；证据准入未通过，数据质量与时点可得性仍未评估。</p>
    </section>
    """


def _render_acceptance_axes(showcase: AtlasCitedQueryShowcase) -> str:
    cards = "".join(
        f'<li data-review-track="{escape(item.track.value)}" data-review-status="{escape(item.status.value)}">'
        f"<strong>{escape(_PAGE_ACCEPTANCE_TRACK_LABELS[item.track])}</strong>"
        f"<span>{escape(_PAGE_ACCEPTANCE_LABELS[item.status])}</span></li>"
        for item in showcase.page_effectiveness.acceptance
    )
    return f"""
    <section class="acceptance-axes" data-reader-section="ACCEPTANCE_AXES" aria-labelledby="acceptance-axes-title">
      <div class="reader-section-heading">
        <p class="section-kicker">THREE AXES · NEVER MERGED</p>
        <h2 id="acceptance-axes-title">工程、页面和读者理解，分别通过了吗？</h2>
        <p>三种“通过”互不代签；页面好读也不能代替策略证据。</p>
      </div>
      <ul class="acceptance-axis-grid">{cards}</ul>
      <p data-always-visible="engineering_validation">工程验收只检查实现与可重放性。</p>
      <p data-always-visible="owner_visual_review">Owner 视觉验收仍必须来自真实人工事实。</p>
      <p data-always-visible="reader_comprehension_review">目标读者理解验收仍必须来自冻结页面上的独立人工记录。</p>
    </section>
    """


def _render_flow_position(showcase: AtlasCitedQueryShowcase) -> str:
    records = _status_explanation_records_by_stage(showcase)
    work_records, _ = _work_progress_records_by_stage(showcase)
    stage_ids = ("ATLAS_SNAPSHOT_DIFF", "CITATION_FIRST_QUERY", "OWNER_DECISION_BOUNDARY")
    labels = ("上一步", "当前位置", "下一步")
    cards = "".join(
        f'<li data-flow-position="{escape(label)}" data-stage="{escape(stage_id)}">'
        f"<span>{escape(label)}</span><strong>{escape(work_records[stage_id].display_title_zh)}</strong>"
        f"<p>{escape(project_reader_text(text=records[stage_id].plain_summary, policy=showcase.reader_terminology))}</p></li>"
        for label, stage_id in zip(labels, stage_ids, strict=True)
    )
    return f"""
    <section class="flow-position" data-reader-section="FLOW_POSITION" aria-labelledby="flow-position-title">
      <div class="reader-section-heading">
        <p class="section-kicker">PREVIOUS → CURRENT → NEXT</p>
        <h2 id="flow-position-title">当前页面位于研究流程的哪里？</h2>
        <p>页面先比较研究快照，再给出引用优先回答，最后把接受与否交回人工决策边界。</p>
      </div>
      <ol class="flow-position-grid">{cards}</ol>
      <p data-always-visible="next_legal_action">下一合法动作仍受当前证据结果约束；页面本身不会推进研究或授权交易。</p>
    </section>
    """


def _render_research_drilldown(
    *,
    system_flow: str,
    qqq_options_projection: str,
    result_ledger: str,
) -> str:
    return f"""
    <section class="research-drilldown" data-reader-section="RESEARCH_DRILLDOWN" aria-labelledby="research-drilldown-title">
      <div class="reader-section-heading">
        <p class="section-kicker">DETAILS ON DEMAND</p>
        <h2 id="research-drilldown-title">需要核对节点、结果和归因时，再进入研究细节</h2>
        <p data-always-visible="drilldown-purpose">以下内容服务于“为什么得到这个结论”的核对，不是新的研究主线。</p>
        <button class="drilldown-toggle" type="button" aria-expanded="false" aria-controls="research-drilldown-body">展开研究流程、QQQ 投影与完整结果</button>
      </div>
      <div id="research-drilldown-body" class="research-drilldown-body" hidden>
        {system_flow}
        {qqq_options_projection}
        {result_ledger}
      </div>
    </section>
    """


def _render_audit_destinations(
    *,
    showcase: AtlasCitedQueryShowcase,
    terminology_guide: str,
    snapshot_id: str,
    diff_id: str,
) -> str:
    manifest = showcase.page_effectiveness
    task_rows = "".join(
        f'<li data-successor-task="{escape(item.task_id)}" '
        f'data-successor-coverage="{escape(item.coverage)}">'
        f"<code>{escape(item.task_id)}</code>"
        f"<span>{escape(item.reader_summary_zh)}</span></li>"
        for item in manifest.task_coverage
        if 2494 <= page_task_identity_sort_key(item.task_id)[0] <= 2532
    )
    return f"""
    <section class="audit-destinations" data-reader-section="AUDIT_DESTINATIONS" data-task-coverage-count="{len(manifest.task_coverage)}" aria-labelledby="audit-destinations-title">
      <div class="reader-section-heading">
        <p class="section-kicker">GLOSSARY + AUDIT DESTINATIONS</p>
        <h2 id="audit-destinations-title">需要完整定义或可重放证据时，到这里核对</h2>
        <p>术语索引、原始身份和 sidecar 都在主线之后；它们保留审计能力，但不占据首屏注意力。</p>
      </div>
      {terminology_guide}
      <details class="task-coverage-audit" data-reader-layer="audit" data-reader-card="task-coverage-audit">
        <summary>查看影响本页的后继任务与来源覆盖</summary>
        <ul>{task_rows}</ul>
      </details>
      <footer>
        <details data-reader-layer="audit" data-reader-card="page-audit">
          <summary>查看页面生成、来源身份与安全审计信息</summary>
          <p>Snapshot <code>{escape(snapshot_id)}</code></p>
          <p>Diff <code>{escape(diff_id)}</code></p>
          <p>repository commit <code>{escape(manifest.repository_commit)}</code></p>
          <p>reader projection contract SHA-256 <code>{escape(showcase.reader_projection_contract_sha256)}</code></p>
          <p>independent_validation=<code>PASS</code> · manual_review_only=<code>true</code> · production_effect=<code>none</code> · broker_action=<code>none</code></p>
        </details>
      </footer>
    </section>
    """


_READER_INTERACTION_SCRIPT = """<script>
(() => {
  const positionTerm = (context) => {
    if (!context) return;
    const trigger = context.querySelector('.term-trigger');
    const popover = context.querySelector('.term-popover');
    if (!trigger || !popover) return;
    const triggerRect = trigger.getBoundingClientRect();
    const popoverWidth = popover.offsetWidth;
    const popoverHeight = popover.offsetHeight;
    const viewportInset = 12;
    const naturalLeft = triggerRect.left + (triggerRect.width / 2) - (popoverWidth / 2);
    const maximumLeft = Math.max(viewportInset, window.innerWidth - popoverWidth - viewportInset);
    const clampedLeft = Math.min(Math.max(viewportInset, naturalLeft), maximumLeft);
    const spaceAbove = triggerRect.top - viewportInset;
    const spaceBelow = window.innerHeight - triggerRect.bottom - viewportInset;
    const placeBelow = spaceBelow >= popoverHeight || spaceBelow >= spaceAbove;
    const naturalTop = placeBelow
      ? triggerRect.bottom + 9
      : triggerRect.top - popoverHeight - 9;
    const maximumTop = Math.max(viewportInset, window.innerHeight - popoverHeight - viewportInset);
    const clampedTop = Math.min(Math.max(viewportInset, naturalTop), maximumTop);
    context.style.setProperty('--term-tooltip-left', `${clampedLeft}px`);
    context.style.setProperty('--term-tooltip-top', `${clampedTop}px`);
    context.dataset.termPlacement = placeBelow ? 'below' : 'above';
  };
  const closeTerms = (except = null) => {
    document.querySelectorAll('.term-context.term-open').forEach((item) => {
      if (item !== except) {
        item.classList.remove('term-open');
        const trigger = item.querySelector('.term-trigger[aria-expanded]');
        if (trigger) trigger.setAttribute('aria-expanded', 'false');
      }
    });
  };
  const toggleTerm = (trigger) => {
    const context = trigger.closest('.term-context');
    if (!context) return;
    const opening = !context.classList.contains('term-open');
    if (opening) positionTerm(context);
    closeTerms(opening ? context : null);
    context.classList.toggle('term-open', opening);
    if (trigger.hasAttribute('aria-expanded')) {
      trigger.setAttribute('aria-expanded', String(opening));
    }
  };
  document.addEventListener('pointerover', (event) => {
    positionTerm(event.target.closest('.term-context'));
  });
  document.addEventListener('focusin', (event) => {
    positionTerm(event.target.closest('.term-context'));
  });
  document.addEventListener('click', (event) => {
    const trigger = event.target.closest('.term-trigger');
    if (trigger) toggleTerm(trigger);
    else if (!event.target.closest('.term-context')) closeTerms();
  });
  document.addEventListener('keydown', (event) => {
    const trigger = event.target.closest('.term-trigger[tabindex="0"]');
    if (trigger && (event.key === 'Enter' || event.key === ' ')) {
      event.preventDefault();
      toggleTerm(trigger);
    }
    if (event.key === 'Escape') {
      const open = document.querySelector('.term-context.term-open');
      const active = open && open.querySelector('.term-trigger[tabindex="0"]');
      closeTerms();
      if (active) active.focus();
    }
  });
  window.addEventListener('resize', () => {
    document.querySelectorAll('.term-context.term-open').forEach(positionTerm);
  });
  window.addEventListener('scroll', () => {
    document.querySelectorAll('.term-context.term-open,.term-context:focus-within,.term-context:hover')
      .forEach(positionTerm);
  }, true);
  const drilldown = document.querySelector('.drilldown-toggle');
  const drilldownBody = document.getElementById('research-drilldown-body');
  if (drilldown && drilldownBody) {
    drilldown.addEventListener('click', () => {
      const expanded = drilldown.getAttribute('aria-expanded') === 'true';
      drilldown.setAttribute('aria-expanded', String(!expanded));
      drilldownBody.hidden = expanded;
      drilldown.textContent = expanded
        ? '展开研究流程、QQQ 投影与完整结果'
        : '收起研究流程、QQQ 投影与完整结果';
    });
  }
})();
</script>"""


def render_cited_query_html(showcase: AtlasCitedQueryShowcase) -> str:
    if len(showcase.responses) != len(CITED_QUERY_QUESTION_CATALOG):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_QUESTION_COUNT_INVALID")
    if any(item.status != "PASS" for item in showcase.validations):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_VALIDATION_NOT_PASS")
    _validate_showcase_response_bindings(showcase)
    _status_explanation_records_by_stage(showcase)
    navigation = "".join(
        f'<a href="#{escape(item.request.question_id.value.lower())}">'
        f"{escape(_QUESTION_PROMPTS[item.request.question_id])}</a>"
        for item in showcase.responses
    )
    cards = "".join(
        _render_response(item, showcase.reader_terminology) for item in showcase.responses
    )
    system_flow = _render_system_flow_map(showcase)
    qqq_options_projection = _render_qqq_options_projection(showcase)
    result_ledger = _render_result_ledger(showcase)
    terminology_guide = _render_reader_terminology_guide(showcase.reader_terminology)
    snapshot_id = str(showcase.snapshot_payload["snapshot_id"])
    diff_id = str(showcase.diff_payload["diff_id"])
    trust_strip = _render_trust_strip(showcase)
    why_context = _render_why_context(showcase)
    canonical_questions = _render_canonical_questions(navigation, cards)
    change_summary = _render_change_summary(showcase)
    conclusion_boundary = _render_conclusion_boundary(showcase)
    acceptance_axes = _render_acceptance_axes(showcase)
    flow_position = _render_flow_position(showcase)
    research_drilldown = _render_research_drilldown(
        system_flow=system_flow,
        qqq_options_projection=qqq_options_projection,
        result_ledger=result_ledger,
    )
    audit_destinations = _render_audit_destinations(
        showcase=showcase,
        terminology_guide=terminology_guide,
        snapshot_id=snapshot_id,
        diff_id=diff_id,
    )
    term_definition_bank = _render_term_definition_bank(showcase.reader_terminology)
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>策略研究页面｜研究主线、证据与边界</title>
  <style>
    :root {{ --ink:#172033; --muted:#697489; --line:#dfe4ec; --paper:#f4f6f9; --panel:#fff; --navy:#132743; --blue:#315fba; --green:#18705b; --teal:#0d7f77; --teal-soft:#e8f6f3; --blue-soft:#eaf1ff; --amber:#9b6b12; --red:#aa3d51; }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior:smooth; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font:16px/1.65 system-ui,-apple-system,"Segoe UI",sans-serif; }}
    header {{ padding:3.2rem max(1.2rem,calc((100vw - 1120px)/2)); color:#fff; background:linear-gradient(135deg,var(--navy),#244b83 70%,#2f6e76); }}
    header h1 {{ max-width:840px; margin:.25rem 0 .7rem; font-size:clamp(2.2rem,6vw,4.6rem); line-height:1.02; letter-spacing:-.035em; }}
    .eyebrow {{ margin:0; font-size:.78rem; font-weight:800; letter-spacing:.16em; }}
    .lead {{ max-width:780px; margin:.8rem 0 0; color:#d9e8ff; font-size:1.08rem; }}
    .system-orientation {{ max-width:980px; margin-top:1.35rem; padding:1rem; border:1px solid #ffffff38; border-radius:.9rem; background:#071a3259; backdrop-filter:blur(5px); }}
    .system-orientation-title {{ margin:0 0 .65rem; color:#b9d8ff; font-size:.72rem; font-weight:900; letter-spacing:.12em; }}
    .system-orientation ol {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.55rem; margin:0; padding:0; list-style:none; }}
    .system-orientation li {{ min-width:0; padding:.65rem .7rem; border:1px solid #ffffff24; border-radius:.62rem; background:#ffffff10; }}
    .system-orientation li span,.system-orientation li strong {{ display:block; }}
    .system-orientation li span {{ color:#b9d8ff; font-size:.61rem; font-weight:900; letter-spacing:.04em; }}
    .system-orientation li strong {{ margin-top:.18rem; color:#fff; font-size:.75rem; line-height:1.45; }}
    .system-orientation-current {{ margin:.7rem 0 0; padding:.62rem .7rem; border-left:4px solid #79d6c5; border-radius:.38rem; color:#e8f7f4; background:#071a326b; font-size:.75rem; line-height:1.5; }}
    .metrics {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.75rem; max-width:720px; margin-top:1.5rem; }}
    .metric {{ padding:.8rem 1rem; border:1px solid #ffffff2b; border-radius:.8rem; background:#ffffff12; }}
    .metric strong {{ display:block; font-size:1.8rem; }}
    .metric span {{ color:#d9e8ff; font-size:.82rem; }}
    main {{ width:min(1120px,calc(100% - 2rem)); margin:0 auto; padding:1.6rem 0 4rem; }}
    .terminology-guide {{ margin:0; border:0; border-bottom:1px solid #cbd8e8; background:#eef5ff; }}
    .terminology-guide > summary {{ padding:.8rem max(1.2rem,calc((100vw - 1120px)/2)); color:#244e80; background:#e7f1ff; font-size:.82rem; font-weight:900; }}
    .terminology-guide-intro,.terminology-grid {{ width:min(1120px,calc(100% - 2rem)); margin:0 auto; }}
    .terminology-guide-intro {{ padding:1rem 0 .4rem; }}
    .terminology-guide-intro h2 {{ margin:0 0 .3rem; font-size:1.15rem; }}
    .terminology-guide-intro p {{ margin:.18rem 0; color:#51627a; font-size:.74rem; }}
    .terminology-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.55rem; padding:.65rem 0 1.2rem; }}
    .terminology-card {{ min-width:0; padding:.7rem .75rem; border:1px solid #cfdaea; border-radius:.62rem; background:#fff; }}
    .terminology-card-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:.6rem; }}
    .terminology-card h3 {{ margin:0; color:#2f4668; font-size:.78rem; }}
    .terminology-card code {{ color:#61728b; font-size:.57rem; overflow-wrap:anywhere; }}
    .terminology-card p {{ margin:.35rem 0 0; color:#526077; font-size:.66rem; line-height:1.5; }}
    .notice {{ margin:0 0 1.4rem; padding:1rem 1.15rem; border-left:5px solid var(--blue); border-radius:.55rem; background:#eaf1ff; }}
    .page-effectiveness {{ margin:0 0 1.5rem; padding:1.35rem; border:1px solid #cfd9ea; border-radius:1rem; background:#fff; box-shadow:0 10px 30px #12213a0a; }}
    .effectiveness-title-row {{ display:grid; grid-template-columns:minmax(0,1.55fr) minmax(250px,.65fr); gap:1rem; align-items:start; }}
    .effectiveness-title-row h2 {{ margin:.2rem 0 .4rem; font-size:clamp(1.45rem,2.8vw,2.15rem); line-height:1.18; }}
    .effectiveness-title-row p:last-child {{ max-width:720px; margin:.2rem 0; color:var(--muted); }}
    .freshness-badge {{ padding:.8rem .9rem; border:1px solid #7fa7d8; border-radius:.72rem; color:#244e80; background:#edf5ff; }}
    .freshness-badge span,.freshness-badge strong,.freshness-badge code {{ display:block; }}
    .freshness-badge span {{ font-size:.64rem; font-weight:900; letter-spacing:.08em; text-transform:uppercase; }}
    .freshness-badge strong {{ margin:.2rem 0; font-size:.83rem; line-height:1.4; }}
    .freshness-badge code {{ color:#55769d; font-size:.6rem; }}
    .reader-answer-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.65rem; margin:1rem 0; }}
    .reader-answer-grid article {{ min-width:0; padding:.72rem .78rem; border:1px solid #dfe5ee; border-radius:.7rem; background:#f8fafc; }}
    .reader-answer-grid span,.reader-answer-grid strong {{ display:block; }}
    .reader-answer-grid span {{ color:#617089; font-size:.62rem; font-weight:900; letter-spacing:.05em; }}
    .reader-answer-grid strong {{ margin-top:.25rem; color:#2e3d55; font-size:.78rem; line-height:1.55; }}
    .reader-answer-grid .reader-answer-stop {{ border-color:#d89aa7; background:#fff6f8; }}
    .reader-answer-stop strong {{ color:#8d3044; }}
    .effectiveness-boundary {{ display:grid; grid-template-columns:minmax(0,1.1fr) minmax(0,.9fr); gap:.8rem; margin-top:.8rem; }}
    .effectiveness-boundary > div,.successor-coverage {{ min-width:0; padding:.8rem; border:1px solid #dfe5ee; border-radius:.72rem; background:#fbfcfe; }}
    .effectiveness-boundary h3 {{ margin:0 0 .55rem; font-size:.88rem; }}
    .effectiveness-boundary p {{ margin:.55rem 0 0; color:var(--muted); font-size:.69rem; }}
    .effectiveness-review-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.45rem; margin:0; padding:0; list-style:none; }}
    .effectiveness-review-card {{ min-width:0; padding:.55rem; border:1px solid #cfd9ea; border-radius:.55rem; background:#fff; }}
    .effectiveness-review-card strong,.effectiveness-review-card span,.effectiveness-review-card code {{ display:block; overflow-wrap:anywhere; }}
    .effectiveness-review-card strong {{ color:#39577d; font-size:.58rem; }}
    .effectiveness-review-card span {{ margin:.15rem 0; font-size:.7rem; font-weight:850; }}
    .effectiveness-review-card code {{ color:#687b96; font-size:.53rem; }}
    .successor-coverage {{ border-top:1px solid #dfe5ee; }}
    .successor-coverage > summary,.effectiveness-audit > summary {{ padding:.2rem; color:var(--blue); font-size:.75rem; }}
    .successor-coverage ul {{ display:grid; gap:.35rem; margin:.6rem 0 0; padding:0; list-style:none; }}
    .successor-coverage li {{ display:grid; grid-template-columns:86px minmax(0,1fr); gap:.45rem; padding:.42rem; border-radius:.4rem; background:#fff; }}
    .successor-coverage li code {{ color:#496a94; font-size:.59rem; }}
    .successor-coverage li span {{ color:#536176; font-size:.65rem; line-height:1.45; }}
    .effectiveness-audit {{ margin-top:.8rem; border:1px solid #dfe5ee; border-radius:.72rem; background:#f8fafc; }}
    .effectiveness-audit > summary {{ padding:.7rem .8rem; }}
    .effectiveness-audit dl {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.45rem; margin:0; padding:0 .8rem .8rem; }}
    .effectiveness-audit dl div {{ min-width:0; padding:.48rem; border-radius:.42rem; background:#fff; }}
    .effectiveness-audit dt {{ color:var(--muted); font-size:.58rem; font-weight:850; }}
    .effectiveness-audit dd {{ margin:.12rem 0 0; font-size:.64rem; overflow-wrap:anywhere; }}
    .qqq-projection {{ margin:0 0 1.5rem; padding:1.35rem; border:1px solid #d9dfe9; border-radius:1rem; background:#fff; box-shadow:0 10px 30px #12213a0a; }}
    .qqq-title-row {{ display:flex; justify-content:space-between; align-items:flex-start; gap:1.2rem; margin:.35rem 0 1rem; }}
    .qqq-title-row h2 {{ margin:0 0 .35rem; font-size:clamp(1.45rem,2.8vw,2.15rem); line-height:1.15; }}
    .qqq-title-row p {{ max-width:760px; margin:0; color:var(--muted); }}
    .qqq-count {{ flex:0 0 142px; padding:.72rem .85rem; border:1px solid #d8dfee; border-radius:.75rem; text-align:center; background:#f7f9fc; }}
    .qqq-count strong,.qqq-count span,.qqq-count small {{ display:block; }}
    .qqq-count strong {{ color:#334b77; font-size:1.65rem; line-height:1; }}
    .qqq-count span {{ margin:.2rem 0; font-size:.72rem; font-weight:850; }}
    .qqq-count small {{ color:var(--muted); font-size:.62rem; }}
    .qqq-decision {{ display:grid; grid-template-columns:180px minmax(0,1fr); overflow:hidden; border:1px solid #d85c70; border-radius:.86rem; background:#fff7f8; }}
    .qqq-decision-label {{ display:flex; flex-direction:column; justify-content:center; padding:1rem; color:#fff; background:#a9364b; }}
    .qqq-decision-label span {{ font-size:.68rem; font-weight:850; letter-spacing:.08em; }}
    .qqq-decision-label strong {{ font-size:1.25rem; }}
    .qqq-decision-copy {{ padding:.9rem 1rem; }}
    .qqq-decision-copy code {{ color:#9b2e43; font-weight:900; }}
    .qqq-decision-copy p {{ margin:.28rem 0 0; color:#563942; font-size:.88rem; }}
    .qqq-reader-boundary {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.65rem; margin:.8rem 0; }}
    .qqq-reader-boundary > div {{ padding:.72rem .78rem; border:1px solid #e0e5ed; border-radius:.68rem; background:#f8fafc; }}
    .qqq-reader-boundary span {{ display:block; color:#6a7485; font-size:.62rem; font-weight:850; letter-spacing:.07em; }}
    .qqq-reader-boundary strong {{ display:block; margin-top:.18rem; color:#29364a; font-size:.78rem; line-height:1.42; }}
    .qqq-reader-boundary > div:nth-child(2) {{ border-color:#e6c278; background:#fff9ed; }}
    .qqq-reader-boundary > div:nth-child(3) {{ border-color:#e0a2ad; background:#fff7f8; }}
    .qqq-layer-guide {{ margin:0 0 1rem; padding:.65rem .75rem; border-left:4px solid #7650a8; color:#535d70; background:#f7f2fc; font-size:.75rem; }}
    .qqq-groups {{ display:grid; gap:1rem; }}
    .qqq-group {{ overflow:hidden; border:1px solid #dfe4ec; border-radius:.82rem; background:#fbfcfe; }}
    .qqq-group-head {{ display:grid; grid-template-columns:42px minmax(0,1fr); gap:.75rem; padding:1rem 1rem .75rem; }}
    .qqq-group-number {{ display:flex; align-items:center; justify-content:center; width:42px; height:42px; border-radius:50%; color:#fff; background:#425a84; font-size:.72rem; font-weight:900; }}
    .qqq-group-kicker {{ margin:0; color:#6d7890; font-size:.58rem!important; font-weight:900; letter-spacing:.1em; }}
    .qqq-group-head h3 {{ margin:.08rem 0 .2rem; font-size:1.05rem; }}
    .qqq-group-head p {{ margin:0; color:#5f6b7e; font-size:.78rem; }}
    .qqq-group-2 .qqq-group-number {{ background:#7650a8; }}
    .qqq-group-3 .qqq-group-number {{ background:#9b6b12; }}
    .qqq-group-4 .qqq-group-number {{ background:#a9364b; }}
    .qqq-group-boundary {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.7rem; padding:0 1rem .85rem; }}
    .qqq-group-boundary p {{ margin:0; padding:.58rem .65rem; border-radius:.55rem; color:#5a6679; background:#eef2f7; font-size:.7rem; }}
    .qqq-group-boundary p:first-child {{ background:#fff6e7; }}
    .qqq-task-list {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.7rem; padding:.85rem; border-top:1px solid #e1e6ee; background:#fff; }}
    .qqq-task-card {{ min-width:0; overflow:hidden; border:1px solid #dfe5ee; border-radius:.7rem; background:#f9fbfd; }}
    .qqq-task-card > summary {{ border:0; background:transparent; }}
    .qqq-task-summary {{ position:relative; display:flex; min-height:154px; padding:.78rem; flex-direction:column; list-style:none; }}
    .qqq-task-summary::-webkit-details-marker {{ display:none; }}
    .qqq-task-summary:focus-visible {{ outline:3px solid #6e9fea; outline-offset:-3px; }}
    .qqq-task-identity {{ display:flex; justify-content:space-between; align-items:center; gap:.5rem; }}
    .qqq-task-identity > code {{ color:#425a84; font-size:.67rem; font-weight:800; }}
    .qqq-layer-badge {{ padding:.15rem .38rem; border-radius:999px; color:#3d5686; background:#eaf0fa; font-size:.55rem; font-weight:850; white-space:nowrap; }}
    .qqq-layer-b .qqq-layer-badge {{ color:#7f5a0b; background:#fff1d2; }}
    .qqq-layer-c .qqq-layer-badge {{ color:#684393; background:#f1e8fb; }}
    .qqq-task-summary > strong {{ margin:.65rem 0 .28rem; font-size:.94rem; }}
    .qqq-task-summary > span:not(.qqq-task-identity) {{ color:#5f6b7e; font-size:.72rem; line-height:1.45; }}
    .qqq-task-summary > i {{ position:absolute; right:.75rem; bottom:.55rem; color:#58709b; font-style:normal; transition:transform .16s ease; }}
    .qqq-task-card[open] .qqq-task-summary > i {{ transform:rotate(180deg); }}
    .qqq-task-body {{ padding:.8rem; border-top:1px solid #dfe5ee; background:#fff; }}
    .qqq-reader-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.55rem; }}
    .qqq-reader-fact {{ min-width:0; padding:.6rem; border:1px solid #e3e7ee; border-radius:.55rem; background:#f8fafc; }}
    .qqq-reader-fact h5,.qqq-priority-facts h5 {{ margin:0 0 .3rem; color:#4a5870; font-size:.65rem; }}
    .qqq-reader-fact p {{ margin:0; color:#536075; font-size:.69rem; line-height:1.48; }}
    .qqq-not-proven {{ border-color:#e6ca91; background:#fffaf1; }}
    .qqq-blocker {{ border-color:#e4acb6; background:#fff7f8; }}
    .qqq-next {{ border-color:#cfc0e2; background:#faf7fd; }}
    .qqq-priority-facts {{ margin-top:.6rem; padding:.6rem; border-left:4px solid #a9364b; border-radius:.45rem; background:#fff7f8; }}
    .qqq-priority-facts ol {{ display:grid; gap:.35rem; margin:0; padding:0; list-style:none; }}
    .qqq-priority-facts li {{ display:grid; grid-template-columns:24px minmax(0,1fr); align-items:start; gap:.4rem; color:#603943; font-size:.67rem; }}
    .qqq-priority-facts li span {{ display:flex; align-items:center; justify-content:center; width:21px; height:21px; border-radius:50%; color:#fff; background:#a9364b; font-size:.5rem; }}
    .qqq-source-warning {{ margin:.6rem 0 0; padding:.55rem; border:1px solid #e0b55b; border-radius:.5rem; color:#6d5117; background:#fff8e9; font-size:.68rem; }}
    .qqq-audit {{ margin-top:.6rem; overflow:hidden; border:1px solid #dfe5ee; border-radius:.52rem; background:#f6f8fb; }}
    .qqq-audit > summary {{ padding:.55rem .65rem; color:#425a84; font-size:.65rem; }}
    .qqq-audit-body {{ padding:.65rem; border-top:1px solid #dfe5ee; }}
    .qqq-layer-grid {{ display:grid; gap:.3rem; margin:0; }}
    .qqq-layer-row {{ display:grid; grid-template-columns:118px minmax(0,1fr); gap:.45rem; align-items:start; }}
    .qqq-layer-row dt {{ color:#677287; font-size:.58rem; font-weight:800; }}
    .qqq-layer-row dd {{ margin:0; font-size:.6rem; overflow-wrap:anywhere; }}
    .qqq-source-ref {{ margin-top:.55rem; padding-top:.5rem; border-top:1px solid #dfe5ee; }}
    .qqq-source-ref p {{ margin:.15rem 0; color:#657086; font-size:.58rem; overflow-wrap:anywhere; }}
    .qqq-projection-safety {{ margin:1rem 0 0; padding:.7rem .8rem; border:1px dashed #9aa5b5; border-radius:.6rem; color:#566276; background:#f8fafc; font-size:.72rem; }}
    .flow-map {{ margin:0 0 1.5rem; padding:1.35rem; border:1px solid var(--line); border-radius:1rem; background:#fff; box-shadow:0 10px 30px #12213a0a; overflow:hidden; }}
    .section-kicker {{ color:var(--teal); font-size:.72rem; font-weight:850; letter-spacing:.14em; }}
    .flow-heading {{ display:flex; align-items:flex-start; justify-content:space-between; gap:1.4rem; margin:.35rem 0 1rem; }}
    .flow-heading h2 {{ margin:0 0 .35rem; font-size:clamp(1.35rem,2.6vw,2rem); line-height:1.18; }}
    .flow-heading p {{ max-width:720px; margin:0; color:var(--muted); }}
    .you-are-here {{ flex:0 0 230px; padding:.8rem 1rem; color:#fff; border-radius:.8rem; background:linear-gradient(135deg,var(--blue),#25477e); }}
    .you-are-here span,.you-are-here small {{ display:block; }}
    .you-are-here span {{ font-size:.72rem; font-weight:850; letter-spacing:.1em; }}
    .you-are-here strong {{ display:block; margin:.08rem 0; font-size:1.25rem; }}
    .you-are-here small {{ color:#dce8ff; }}
    .flow-legend {{ display:flex; flex-wrap:wrap; gap:.45rem 1rem; margin:0 0 1rem; color:var(--muted); font-size:.76rem; }}
    .flow-legend span {{ display:inline-flex; align-items:center; gap:.35rem; }}
    .flow-legend i {{ width:.72rem; height:.72rem; border:2px solid var(--line); border-radius:.18rem; }}
    .flow-legend .legend-focus {{ border-color:var(--teal); background:var(--teal-soft); }}
    .flow-legend .legend-current {{ border-color:var(--blue); background:var(--blue); }}
    .flow-legend .legend-boundary {{ border-style:dashed; border-color:#8b95a6; background:#f8fafc; }}
    .progress-matrix {{ margin:.2rem 0 1rem; padding:1rem; border:1px solid #cbd9ec; border-radius:.82rem; background:#f6f9fe; }}
    .progress-matrix-heading {{ display:grid; grid-template-columns:minmax(0,1fr) 170px; gap:1rem; align-items:start; }}
    .progress-matrix-heading h3 {{ margin:.18rem 0 .3rem; font-size:1.02rem; }}
    .progress-matrix-heading p:last-child {{ max-width:720px; margin:0; color:#5d697d; font-size:.72rem; line-height:1.5; }}
    .strategy-conclusion-count {{ padding:.65rem .75rem; border:1px solid #d79aa7; border-radius:.65rem; color:#8d3044; background:#fff7f8; text-align:center; }}
    .strategy-conclusion-count span,.strategy-conclusion-count strong,.strategy-conclusion-count small {{ display:block; }}
    .strategy-conclusion-count span {{ font-size:.61rem; font-weight:900; }}
    .strategy-conclusion-count strong {{ font-size:1.45rem; line-height:1.15; }}
    .strategy-conclusion-count small {{ color:#835c65; font-size:.57rem; }}
    .progress-matrix-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.6rem; margin-top:.8rem; }}
    .progress-matrix-card {{ min-width:0; padding:.7rem; border:1px solid #dbe3ee; border-radius:.65rem; background:#fff; }}
    .progress-matrix-card-head span,.progress-matrix-card-head strong {{ display:block; }}
    .progress-matrix-card-head span {{ color:#5d6a7f; font-size:.6rem; font-weight:900; letter-spacing:.04em; }}
    .progress-matrix-card-head strong {{ margin:.12rem 0 .45rem; color:#2f4058; font-size:.76rem; }}
    .progress-matrix-card ul {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.3rem; margin:0; padding:0; list-style:none; }}
    .progress-matrix-card li {{ display:flex; align-items:center; justify-content:space-between; gap:.35rem; min-width:0; padding:.32rem .4rem; border-radius:.42rem; color:#4f5e73; background:#f4f7fa; }}
    .progress-matrix-card li span {{ min-width:0; font-size:.58rem; line-height:1.35; }}
    .progress-matrix-card li strong {{ font-size:.72rem; }}
    .progress-matrix-card > p {{ margin:.5rem 0 0; color:#687489; font-size:.61rem; line-height:1.45; }}
    .progress-matrix-warning {{ margin:.72rem 0 0; padding:.58rem .65rem; border-left:4px solid #a9364b; color:#6f3340; background:#fff8f9; font-size:.67rem; line-height:1.5; }}
    .axis-available {{ color:#087a55 !important; background:#eef9f5 !important; }}
    .axis-active {{ color:#1769aa !important; background:#eef6fc !important; }}
    .axis-limited {{ color:#906000 !important; background:#fff7e7 !important; }}
    .axis-review {{ color:#7650a8 !important; background:#f6f0fc !important; }}
    .axis-blocked {{ color:#a9364b !important; background:#fff3f5 !important; }}
    .axis-neutral {{ color:#657187 !important; background:#f1f3f6 !important; }}
    .progress-key {{ display:flex; flex-wrap:wrap; align-items:center; gap:.4rem .75rem; margin:-.15rem 0 1rem; padding:.65rem .75rem; border:1px solid #dde3ec; border-radius:.68rem; color:var(--muted); background:#f8fafc; font-size:.72rem; }}
    .progress-key > strong {{ color:var(--ink); font-size:.75rem; }}
    .progress-key span {{ display:inline-flex; align-items:center; gap:.3rem; white-space:nowrap; }}
    .progress-key i,.progress-dot {{ width:.58rem; height:.58rem; flex:0 0 .58rem; border-radius:50%; background:currentColor; }}
    .progress-key small {{ flex-basis:100%; color:#697589; }}
    .progress-neutral {{ color:#697589; }}
    .progress-active {{ color:#1769aa; }}
    .progress-limited {{ color:#9a6500; }}
    .progress-validated {{ color:#087a55; }}
    .progress-review {{ color:#7650a8; }}
    .drilldown-help {{ margin:0 0 .8rem; color:var(--muted); font-size:.76rem; line-height:1.5; }}
    .system-flow {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); grid-template-areas:"s1 s2" "s4 s3" "s5 s6" "s8 s7"; gap:1.25rem .9rem; margin:0; padding:0; list-style:none; counter-reset:flow; }}
    .flow-stage-shell {{ position:relative; min-width:0; }}
    .flow-stage-shell:nth-child(1) {{ grid-area:s1; }} .flow-stage-shell:nth-child(2) {{ grid-area:s2; }} .flow-stage-shell:nth-child(3) {{ grid-area:s3; }} .flow-stage-shell:nth-child(4) {{ grid-area:s4; }}
    .flow-stage-shell:nth-child(5) {{ grid-area:s5; }} .flow-stage-shell:nth-child(6) {{ grid-area:s6; }} .flow-stage-shell:nth-child(7) {{ grid-area:s7; }} .flow-stage-shell:nth-child(8) {{ grid-area:s8; }}
    .flow-stage-shell::after {{ position:absolute; z-index:2; content:"→"; right:-.78rem; top:98px; width:.65rem; color:#95a0b2; font-size:1.1rem; font-weight:900; text-align:center; transform:translateY(-50%); }}
    .flow-stage-shell:nth-child(2)::after,.flow-stage-shell:nth-child(4)::after,.flow-stage-shell:nth-child(6)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.18rem; transform:translateX(50%); }}
    .flow-stage-shell:nth-child(3)::after,.flow-stage-shell:nth-child(7)::after {{ content:"←"; right:auto; left:-.78rem; }}
    .flow-stage-shell:nth-child(8)::after {{ display:none; }}
    .flow-stage {{ min-width:0; overflow:hidden; border:1px solid var(--line); border-radius:.82rem; background:#f8fafc; }}
    .flow-stage > .stage-summary {{ display:flex; min-height:278px; padding:.82rem; list-style:none; flex-direction:column; }}
    .flow-stage > .stage-summary::-webkit-details-marker {{ display:none; }}
    .flow-stage > .stage-summary:focus-visible {{ outline:3px solid #6e9fea; outline-offset:-3px; }}
    .stage-title {{ margin:.55rem 0 .28rem; font-size:.96rem; font-weight:850; line-height:1.25; }}
    .stage-description {{ margin:0 0 .55rem; color:var(--muted); font-size:.76rem; line-height:1.42; }}
    .flow-stage code {{ display:block; color:#7b8596; font-size:.64rem; }}
    .flow-stage .stage-id {{ margin-bottom:.65rem; }}
    .stage-top {{ display:flex; align-items:center; justify-content:space-between; gap:.4rem; }}
    .stage-number {{ color:var(--muted); font:850 .7rem/1 system-ui,sans-serif; letter-spacing:.08em; }}
    .stage-badge {{ padding:.15rem .4rem; border-radius:999px; color:#667287; background:#e9edf3; font-size:.62rem; font-weight:850; }}
    .stage-axis-summary {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.35rem; margin:0 0 .45rem; }}
    .stage-axis-chip {{ display:block; min-width:0; padding:.38rem .45rem; border:1px solid currentColor; border-radius:.48rem; }}
    .stage-axis-chip small,.stage-axis-chip strong {{ display:block; }}
    .stage-axis-chip small {{ font-size:.53rem; font-weight:900; letter-spacing:.04em; }}
    .stage-axis-chip strong {{ margin-top:.08rem; font-size:.64rem; line-height:1.3; }}
    .stage-progress {{ display:grid; grid-template-columns:auto minmax(0,1fr); align-items:center; gap:.35rem; margin-top:auto; padding:.43rem .5rem; border:1px solid currentColor; border-radius:.55rem; background:#fff; }}
    .stage-progress-copy small,.stage-progress-copy strong,.stage-progress-copy code {{ display:block; }}
    .stage-progress-copy small {{ font-size:.52rem; font-weight:900; letter-spacing:.04em; }}
    .stage-progress-copy strong {{ font-size:.7rem; line-height:1.2; }}
    .stage-progress-copy code {{ color:currentColor; font-size:.56rem; line-height:1.2; overflow-wrap:anywhere; }}
    .stage-disclosure-cue {{ display:flex; align-items:center; justify-content:space-between; gap:.4rem; margin:.58rem 0 -.15rem; padding-top:.5rem; border-top:1px solid #dce2eb; color:#315fba; font-size:.65rem; font-weight:850; }}
    .stage-disclosure-cue i {{ font-style:normal; font-size:.95rem; transition:transform .16s ease; }}
    .cue-open {{ display:none; }}
    .flow-stage[open] .cue-open {{ display:inline; }}
    .flow-stage[open] .cue-closed {{ display:none; }}
    .flow-stage[open] .stage-disclosure-cue i {{ transform:rotate(180deg); }}
    .stage-drilldown {{ padding:.85rem; border-top:1px solid #dbe2ec; color:var(--ink); background:#fff; }}
    .work-progress-reader {{ display:grid; gap:.7rem; }}
    .work-purpose {{ padding:.82rem .88rem; border-left:4px solid var(--blue); border-radius:.62rem; background:var(--blue-soft); }}
    .work-purpose h4,.work-reader-card h4,.progress-dimensions h4 {{ margin:0 0 .38rem; color:#34455e; font-size:.72rem; letter-spacing:.025em; }}
    .work-purpose h4 {{ color:#315fba; }}
    .work-purpose p,.work-reader-card p {{ margin:0; color:#354761; font-size:.74rem; line-height:1.55; }}
    .work-reader-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.6rem; }}
    .work-reader-card {{ min-width:0; padding:.7rem .75rem; border:1px solid #e0e6ee; border-radius:.58rem; background:#f8fafc; }}
    .work-item-list,.work-output-list {{ display:grid; gap:.35rem; margin:0; padding-left:1.05rem; color:#354761; font-size:.7rem; line-height:1.5; }}
    .work-boundary {{ border-color:#e8c880; background:#fffaf0; }}
    .work-boundary h4 {{ color:#825b10; }}
    .progress-dimensions {{ padding:.72rem .75rem; border:1px solid #cbd9ec; border-radius:.62rem; background:#f5f8fd; }}
    .progress-dimension-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.45rem; }}
    .progress-dimension-grid article {{ min-width:0; padding:.58rem; border-radius:.5rem; background:#fff; box-shadow:inset 0 0 0 1px #e0e6ef; }}
    .progress-dimension-grid span {{ display:block; margin-bottom:.24rem; color:#657187; font-size:.57rem; font-weight:850; }}
    .progress-dimension-grid strong {{ display:block; color:#2f4058; font-size:.67rem; line-height:1.48; }}
    .concept-links {{ padding:.68rem .72rem; border:1px dashed #87a9d8; border-radius:.58rem; background:#f6f9fe; }}
    .concept-links > strong {{ display:block; margin-bottom:.4rem; color:#315fba; font-size:.67rem; }}
    .concept-links div,.related-concepts div,.concept-backlinks div {{ display:flex; flex-wrap:wrap; gap:.35rem; }}
    .concept-link,.related-concepts a,.concept-backlinks a {{ display:inline-flex; padding:.2rem .45rem; border:1px solid #b8c9e2; border-radius:999px; color:#254f95; background:#fff; font-size:.6rem; font-weight:750; text-decoration:none; }}
    .concept-link:hover,.related-concepts a:hover,.concept-backlinks a:hover {{ border-color:#4c7cc4; background:#edf4ff; }}
    .reader-status-detail {{ overflow:hidden; margin-top:.75rem; border:1px solid #dce4ee; border-radius:.62rem; background:#f8fafc; }}
    .reader-status-detail > summary {{ padding:.68rem .75rem; color:#536176; font-size:.68rem; font-weight:800; }}
    .reader-status-detail-body {{ padding:.15rem .7rem .7rem; }}
    .concept-library {{ margin:1.15rem 0 0; padding:1rem; border:1px solid #cad9ec; border-radius:.85rem; background:#f6f9fe; }}
    .concept-library-head h3 {{ margin:.25rem 0 .35rem; font-size:1rem; }}
    .concept-library-head p:last-child {{ margin:0; color:#5c687b; font-size:.72rem; line-height:1.5; }}
    .concept-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.65rem; margin-top:.85rem; }}
    .concept-card {{ min-width:0; padding:.72rem; border:1px solid #dde5ef; border-radius:.65rem; background:#fff; scroll-margin-top:1rem; }}
    .concept-card:target,.concept-card:focus {{ outline:3px solid #72a3e7; outline-offset:2px; background:#f8fbff; }}
    .concept-card-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:.5rem; }}
    .concept-card-head h4 {{ margin:0; color:#263d60; font-size:.76rem; }}
    .concept-card-head code {{ color:#7a8596; font-size:.53rem; overflow-wrap:anywhere; }}
    .concept-explanation {{ display:grid; gap:.38rem; margin:.58rem 0; }}
    .concept-explanation div {{ padding:.45rem .5rem; border-radius:.42rem; background:#f7f9fc; }}
    .concept-explanation dt {{ color:#5b687c; font-size:.56rem; font-weight:850; }}
    .concept-explanation dd {{ margin:.15rem 0 0; color:#35445a; font-size:.66rem; line-height:1.48; }}
    .related-concepts,.concept-backlinks {{ margin-top:.5rem; }}
    .related-concepts > strong,.concept-backlinks > strong {{ display:block; margin-bottom:.32rem; color:#5b687c; font-size:.57rem; }}
    .concept-leaf {{ margin:.52rem 0 0; color:#697589; font-size:.61rem; }}
    .reader-explanation {{ display:grid; gap:.65rem; }}
    .reader-conclusion {{ padding:.78rem .82rem; border-left:4px solid var(--blue); border-radius:.58rem; background:var(--blue-soft); }}
    .reader-conclusion span {{ display:block; color:#315fba; font-size:.62rem; font-weight:900; letter-spacing:.08em; }}
    .reader-conclusion p {{ margin:.25rem 0 0; color:#213754; font-size:.86rem; font-weight:750; line-height:1.48; }}
    .reader-section {{ padding:.68rem .72rem; border:1px solid #e0e6ee; border-radius:.58rem; background:#f8fafc; }}
    .reader-section h4 {{ margin:0 0 .45rem; color:#34455e; font-size:.72rem; letter-spacing:.035em; }}
    .reader-gap {{ border-color:#e8c880; background:#fffaf0; }}
    .reader-gap h4 {{ color:#825b10; }}
    .reader-facts {{ display:grid; gap:.42rem; margin:0; padding:0; list-style:none; }}
    .reader-facts > li {{ display:grid; grid-template-columns:auto minmax(0,1fr); align-items:start; gap:.35rem .5rem; min-width:0; }}
    .reader-facts p {{ margin:0; color:#3c4b60; font-size:.72rem; line-height:1.5; }}
    .value-state {{ display:inline-flex; align-items:center; justify-content:center; min-width:5.2rem; padding:.14rem .38rem; border:1px solid currentColor; border-radius:999px; font-size:.56rem; font-weight:850; white-space:nowrap; }}
    .value-state-present {{ color:#087a55; background:#eef9f5; }}
    .value-state-not_recorded,.value-state-source_unavailable {{ color:#9a6500; background:#fff6e4; }}
    .value-state-owner_decision_pending {{ color:#7650a8; background:#f5effd; }}
    .value-state-not_applicable,.value-state-not_yet_due {{ color:#697589; background:#f1f3f6; }}
    .transition-detail {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); grid-column:1/-1; gap:.35rem; margin:.35rem 0 0; }}
    .transition-detail div {{ min-width:0; padding:.35rem; border-radius:.4rem; background:#fff; }}
    .transition-detail dt {{ color:#697589; font-size:.55rem; font-weight:850; }}
    .transition-detail dd {{ margin:.12rem 0 0; font-size:.63rem; overflow-wrap:anywhere; }}
    .owner-next-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.5rem; }}
    .owner-next-grid > div {{ min-width:0; padding:.55rem; border-radius:.48rem; background:#fff; }}
    .owner-next-grid p {{ margin:.35rem 0 0; color:#3c4b60; font-size:.7rem; line-height:1.48; }}
    .owner-label {{ display:block; margin-bottom:.28rem; color:#59677b; font-size:.58rem; font-weight:850; }}
    .reader-audit {{ overflow:hidden; border:1px solid #dce4ee; border-radius:.58rem; background:#f4f7fb; }}
    .reader-audit > summary {{ padding:.62rem .7rem; color:#315fba; font-size:.68rem; }}
    .reader-audit-body {{ padding:0 .72rem .72rem; color:#59677b; font-size:.62rem; }}
    .reader-audit-body h5 {{ margin:.6rem 0 .2rem; color:#45546a; font-size:.58rem; letter-spacing:.05em; text-transform:uppercase; }}
    .reader-audit-body p {{ margin:.2rem 0; overflow-wrap:anywhere; }}
    .reader-audit-body ul {{ margin:.2rem 0; padding-left:1rem; }}
    .reader-audit-body li {{ margin:.16rem 0; overflow-wrap:anywhere; }}
    .drilldown-intro {{ margin:0 0 .65rem; color:#233b61; font-size:.75rem; font-weight:900; letter-spacing:.02em; }}
    .drilldown-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.55rem; margin:0; }}
    .drilldown-grid > div {{ min-width:0; padding:.58rem .62rem; border:1px solid #e0e6ee; border-radius:.58rem; background:#f8fafc; }}
    .drilldown-grid .drilldown-wide {{ grid-column:1/-1; }}
    .drilldown-grid dt {{ color:#536176; font-size:.62rem; font-weight:900; letter-spacing:.04em; text-transform:uppercase; }}
    .drilldown-grid dd {{ margin:.22rem 0 0; color:#354257; font-size:.7rem; line-height:1.48; }}
    .drilldown-evidence {{ margin-top:.65rem; padding:.58rem .62rem; border-left:3px solid #7aa6df; background:#f3f7fd; }}
    .drilldown-evidence p {{ margin:0 0 .3rem; color:#536176; font-size:.65rem; }}
    .drilldown-refs {{ margin:0; padding-left:1rem; }}
    .drilldown-refs li {{ margin:.18rem 0; color:#5b687c; font-size:.6rem; overflow-wrap:anywhere; }}
    .flow-focus {{ border-color:#8bcfc4; background:var(--teal-soft); }}
    .flow-focus .stage-badge {{ color:#08665f; background:#ccebe5; }}
    .flow-current {{ color:#fff; border-color:var(--blue); background:linear-gradient(145deg,var(--blue),#234977); box-shadow:0 8px 20px #315fba2b; }}
    .flow-current .stage-description,.flow-current .stage-id,.flow-current .stage-number {{ color:#dce8ff; }}
    .flow-current .stage-badge {{ color:#173b70; background:#fff; }}
    .flow-current .stage-progress {{ background:#fff; }}
    .flow-current .stage-disclosure-cue {{ border-top-color:#6d8db7; color:#fff; }}
    .flow-boundary {{ border-style:dashed; border-color:#8994a7; background:#f8fafc; }}
    .focus-panel {{ display:grid; grid-template-columns:minmax(220px,.8fr) minmax(0,2.2fr); gap:1rem; margin-top:1.2rem; padding:1rem; border-radius:.82rem; background:#f3f8f7; }}
    .focus-copy h3 {{ margin:.2rem 0 .3rem; font-size:1.1rem; }}
    .focus-copy p {{ margin:.2rem 0; color:var(--muted); font-size:.8rem; }}
    .focus-ledger {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.5rem; margin:0; padding:0; list-style:none; }}
    .focus-item {{ min-width:0; padding:.62rem .7rem; border:1px solid #cfe3df; border-radius:.65rem; background:#fff; }}
    .focus-item span:first-child {{ display:flex; justify-content:space-between; gap:.5rem; }}
    .focus-item small {{ color:var(--muted); }}
    .focus-item code {{ display:block; margin:.28rem 0; font-size:.68rem; overflow-wrap:anywhere; }}
    .focus-status {{ color:var(--amber); font-size:.68rem; font-weight:800; }}
    .historical-flow-lane {{ margin-top:1rem; padding:1rem; border:1px dashed #c59c4b; border-radius:.82rem; background:#fffbf1; }}
    .historical-lane-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:1rem; }}
    .historical-lane-head h3 {{ margin:.2rem 0 .3rem; font-size:1.1rem; }}
    .historical-lane-head p {{ max-width:760px; margin:.2rem 0; color:#705b32; font-size:.78rem; }}
    .historical-lane-boundary {{ flex:0 0 150px; padding:.6rem .7rem; border:1px solid #e1c78e; border-radius:.65rem; color:#715319; background:#fff; text-align:center; }}
    .historical-lane-boundary strong,.historical-lane-boundary small {{ display:block; }}
    .historical-lane-boundary strong {{ font-size:1.3rem; }}
    .historical-lane-boundary small {{ color:#8b7040; font-size:.62rem; }}
    .historical-lane-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:.5rem; margin:.8rem 0 0; padding:0; list-style:none; }}
    .historical-lane-item {{ min-width:0; padding:.65rem; border:1px solid #ead9b5; border-radius:.62rem; background:#fff; }}
    .historical-lane-item strong,.historical-lane-item code,.historical-lane-item small {{ display:block; }}
    .historical-lane-item strong {{ margin:.28rem 0; font-size:.72rem; line-height:1.35; }}
    .historical-lane-item code {{ color:#80663b; font-size:.56rem; overflow-wrap:anywhere; }}
    .historical-lane-item small {{ margin-top:.35rem; color:#8a7653; font-size:.58rem; }}
    .historical-lane-status {{ display:inline-block; padding:.12rem .35rem; border-radius:999px; color:#875c08; background:#fff0ca; font-size:.58rem; font-weight:850; }}
    .historical-lane-note {{ margin:.75rem 0 0; padding:.62rem .7rem; border-left:3px solid #c59c4b; color:#705b32; background:#fff; font-size:.7rem; }}
    .provenance-panel {{ margin-top:1rem; padding:1rem; border:1px solid #dfe5ee; border-radius:.82rem; background:#fafbfc; }}
    .provenance-copy {{ display:flex; align-items:end; justify-content:space-between; gap:1rem; margin-bottom:.75rem; }}
    .provenance-copy h3 {{ margin:.2rem 0 0; font-size:1.1rem; }}
    .provenance-copy > p:last-child {{ max-width:650px; margin:0; color:var(--muted); font-size:.78rem; }}
    .provenance-ledger {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.55rem; margin:0; padding:0; list-style:none; }}
    .provenance-item {{ min-width:0; padding:.7rem .75rem; border:1px solid #dfe5ee; border-radius:.68rem; background:#fff; }}
    .provenance-head {{ display:flex; align-items:center; justify-content:space-between; gap:.6rem; }}
    .provenance-stage {{ color:#526176; font-size:.66rem; font-weight:800; }}
    .provenance-status {{ display:inline-flex; align-items:center; gap:.3rem; white-space:nowrap; font-size:.68rem; font-weight:850; }}
    .provenance-status i {{ width:.52rem; height:.52rem; flex:0 0 .52rem; border-radius:50%; background:currentColor; }}
    .provenance-reason {{ margin:.45rem 0; color:#465368; font-size:.76rem; line-height:1.45; }}
    .provenance-item details {{ border:0; background:transparent; }}
    .provenance-item summary {{ padding:.2rem 0; color:var(--blue); font-size:.7rem; }}
    .provenance-source {{ margin:.35rem 0; color:var(--muted); font-size:.68rem; }}
    .provenance-refs {{ margin:.25rem 0 0; padding-left:1.1rem; }}
    .provenance-refs li {{ margin:.2rem 0; color:var(--muted); font-size:.64rem; overflow-wrap:anywhere; }}
    .provenance-boundary {{ margin:.75rem 0 0; color:var(--muted); font-size:.72rem; }}
    .flow-safety {{ margin:1rem 0 0; padding:.75rem .9rem; border-left:4px solid var(--amber); border-radius:.45rem; color:#6e531b; background:#fff8e9; font-size:.78rem; }}
    .result-ledger {{ margin:0 0 1.5rem; padding:1.35rem; border:1px solid var(--line); border-radius:1rem; background:#fff; box-shadow:0 10px 30px #12213a0a; }}
    .result-ledger-intro {{ display:grid; grid-template-columns:minmax(0,1.65fr) minmax(260px,.85fr); gap:1rem; align-items:start; }}
    .result-ledger-intro h2 {{ margin:.25rem 0 .4rem; font-size:clamp(1.35rem,2.6vw,2rem); line-height:1.18; }}
    .result-ledger-intro > div:first-child > p:last-child {{ max-width:700px; margin:.25rem 0; color:var(--muted); }}
    .coverage-boundary {{ padding:.85rem .9rem; border:1px solid #e8c880; border-radius:.72rem; color:#684a11; background:#fff9e9; }}
    .coverage-boundary strong,.coverage-boundary code {{ display:block; }}
    .coverage-boundary p {{ margin:.3rem 0 .55rem; font-size:.78rem; }}
    .coverage-boundary code {{ color:#80601e; font-size:.62rem; line-height:1.55; overflow-wrap:anywhere; }}
    .ledger-summary {{ display:flex; flex-wrap:wrap; gap:.55rem; margin:1rem 0 .75rem; }}
    .ledger-count {{ display:grid; grid-template-columns:auto auto; align-items:center; gap:0 .35rem; min-width:120px; padding:.55rem .7rem; border:1px solid currentColor; border-radius:.65rem; background:#fff; font-size:.72rem; font-weight:800; }}
    .ledger-count strong {{ grid-row:1/3; font-size:1.35rem; line-height:1; }}
    .ledger-count code {{ color:currentColor; font-size:.56rem; }}
    .ledger-reading-note {{ margin:.65rem 0 1rem; padding:.7rem .8rem; border-left:4px solid var(--teal); color:#425168; background:#f2f8f7; font-size:.78rem; }}
    .result-ledger-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.8rem; }}
    .result-ledger-card {{ min-width:0; overflow:hidden; border:1px solid var(--line); border-radius:.82rem; background:#fbfcfe; }}
    .historical-result-card {{ border-color:#d8bd83; background:#fffdf7; }}
    .result-ledger-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:.75rem; padding:.9rem .95rem .6rem; }}
    .result-ledger-head h3 {{ margin:.18rem 0 .25rem; font-size:1rem; line-height:1.35; }}
    .result-sequence,.result-node {{ margin:0; color:var(--muted); font-size:.64rem; overflow-wrap:anywhere; }}
    .historical-result-badge {{ display:inline-block; margin-top:.35rem; padding:.13rem .42rem; border-radius:999px; color:#805a12; background:#fff0c8; font-size:.61rem; font-weight:850; }}
    .result-status {{ display:grid; flex:0 0 106px; gap:.08rem; padding:.35rem .45rem; border:1px solid currentColor; border-radius:.55rem; background:#fff; text-align:right; }}
    .result-status strong {{ font-size:.7rem; }}
    .result-status code {{ color:currentColor; font-size:.57rem; }}
    .result-pass {{ color:#087a55; }}
    .result-limited {{ color:#9a6500; }}
    .result-blocked {{ color:#aa3d51; }}
    .result-active {{ color:#1769aa; }}
    .result-neutral {{ color:#697589; }}
    .result-summary {{ margin:0; padding:.1rem .95rem .8rem; color:#34445b; font-size:.83rem; }}
    .result-status-pair {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.45rem; padding:0 .95rem .9rem; }}
    .result-status-pair span {{ min-width:0; padding:.5rem .55rem; border:1px solid #e0e5ed; border-radius:.55rem; background:#fff; }}
    .result-status-pair small,.result-status-pair strong,.result-status-pair code {{ display:block; }}
    .result-status-pair small {{ color:var(--muted); font-size:.6rem; font-weight:800; }}
    .result-status-pair strong {{ margin:.12rem 0; font-size:.72rem; }}
    .result-status-pair code {{ color:#68758a; font-size:.56rem; overflow-wrap:anywhere; }}
    .historical-status-rationale {{ margin:0 .95rem .9rem; padding:.62rem .7rem; border-left:3px solid #c59c4b; color:#6e572d; background:#fff9e9; font-size:.72rem; }}
    .result-evidence {{ border-top:1px solid var(--line); background:#fff; }}
    .result-evidence > summary {{ padding:.75rem .95rem; color:var(--blue); font-size:.75rem; }}
    .result-evidence-body {{ padding:0 .95rem .95rem; }}
    .result-evidence-body h4 {{ margin:.75rem 0 .28rem; font-size:.72rem; letter-spacing:.05em; text-transform:uppercase; }}
    .result-evidence-body ul {{ margin:.25rem 0; padding-left:1.15rem; color:#536176; font-size:.7rem; }}
    .result-source-refs {{ display:flex; flex-wrap:wrap; gap:.35rem; padding:0!important; list-style:none; }}
    .result-source-refs li {{ min-width:0; padding:.2rem .35rem; border-radius:.35rem; background:#edf2f8; overflow-wrap:anywhere; }}
    .result-attribution-section {{ margin-top:.75rem; padding-top:.05rem; border-top:1px solid #e4e8ef; }}
    .result-attributions {{ display:grid; gap:.5rem; margin:.35rem 0 0; padding:0; list-style:none; }}
    .result-attribution {{ min-width:0; padding:.65rem .7rem; border:1px solid #e0e5ed; border-radius:.58rem; background:#f8fafc; }}
    .attribution-heading {{ display:flex; align-items:center; justify-content:space-between; gap:.5rem; }}
    .direction {{ display:inline-flex; align-items:center; gap:.3rem; font-size:.66rem; font-weight:850; }}
    .direction code {{ color:currentColor; font-size:.55rem; }}
    .direction-supports {{ color:#087a55; }} .direction-contradicts {{ color:#aa3d51; }} .direction-mixed {{ color:#9a6500; }} .direction-neutral,.direction-unknown {{ color:#697589; }}
    .attribution-id {{ color:#68758a; font-size:.56rem; text-align:right; overflow-wrap:anywhere; }}
    .result-attribution > p {{ margin:.5rem 0; color:#3e4c61; font-size:.72rem; }}
    .attribution-meta {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.35rem; margin:.4rem 0; }}
    .attribution-meta div {{ min-width:0; padding:.4rem .45rem; border-radius:.42rem; background:#fff; }}
    .attribution-meta dt {{ color:var(--muted); font-size:.57rem; font-weight:850; }}
    .attribution-meta dd {{ margin:.12rem 0 0; font-size:.65rem; overflow-wrap:anywhere; }}
    nav {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:.55rem; margin-bottom:1.5rem; }}
    nav a {{ color:var(--navy); padding:.75rem; border:1px solid var(--line); border-radius:.7rem; background:#fff; text-decoration:none; font-size:.82rem; font-weight:750; }}
    nav a:hover {{ border-color:var(--blue); }}
    .answer-grid {{ display:grid; gap:1rem; }}
    .answer-card {{ min-width:0; overflow:hidden; border:1px solid var(--line); border-radius:1rem; background:var(--panel); box-shadow:0 10px 30px #12213a0a; }}
    .answer-head {{ display:flex; justify-content:space-between; gap:1.2rem; padding:1.25rem; border-bottom:1px solid var(--line); }}
    .answer-head h2 {{ margin:.1rem 0 .35rem; font-size:1.35rem; }}
    .question-id,.target,.identity,.reason {{ margin:0; color:var(--muted); font-size:.78rem; overflow-wrap:anywhere; }}
    .status {{ align-self:flex-start; white-space:nowrap; padding:.3rem .65rem; border-radius:999px; font-size:.78rem; font-weight:800; }}
    .status-answered {{ color:var(--green); background:#e4f5ee; }}
    .status-limited {{ color:var(--amber); background:#fff1d8; }}
    .status-blocked {{ color:var(--red); background:#fae8eb; }}
    .answer-block {{ padding:1rem 1.25rem; }}
    .answer-block + .answer-block {{ border-top:1px solid var(--line); }}
    .answer-block h3 {{ margin:0 0 .35rem; font-size:.86rem; text-transform:uppercase; letter-spacing:.07em; }}
    .claim {{ margin:.2rem 0; font-size:1.03rem; }}
    .limits {{ background:#fffaf0; }}
    .limits ul {{ margin:.3rem 0; padding-left:1.25rem; }}
    .good {{ color:var(--green); }}
    details {{ border-top:1px solid var(--line); background:#f8fafc; }}
    summary {{ cursor:pointer; padding:1rem 1.25rem; font-weight:800; }}
    .citations {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.7rem; margin:0; padding:0 1.25rem 1.25rem; list-style:none; }}
    .citation {{ min-width:0; padding:.85rem; border:1px solid var(--line); border-radius:.7rem; background:#fff; }}
    .citation p {{ margin:.15rem 0 .55rem; color:var(--muted); font-size:.76rem; overflow-wrap:anywhere; }}
    .citation strong {{ color:var(--ink); }}
    .identity {{ padding:.7rem 1.25rem; border-top:1px solid var(--line); }}
    [hidden] {{ display:none!important; }}
    .skip-link {{ position:fixed; z-index:1000; top:.5rem; left:.5rem; padding:.55rem .8rem; color:#fff; background:#102a4d; border-radius:.45rem; transform:translateY(-180%); }}
    .skip-link:focus {{ transform:none; }}
    .term-definition-bank {{ position:absolute; width:1px; height:1px; margin:-1px; padding:0; overflow:hidden; clip:rect(0 0 0 0); clip-path:inset(50%); white-space:nowrap; border:0; }}
    .term-context {{ position:relative; display:inline; --term-tooltip-left:12px; --term-tooltip-top:12px; }}
    .term-trigger {{ color:#174f91; border-bottom:1px dashed #4f7fb8; background:#edf5ff; border-radius:.18rem; cursor:help; text-decoration:none; }}
    .term-trigger:focus-visible {{ outline:3px solid #76a8ec; outline-offset:2px; }}
    .term-popover {{ position:fixed; z-index:1001; left:var(--term-tooltip-left); top:var(--term-tooltip-top); display:block; width:min(22rem,calc(100vw - 1.5rem)); padding:.72rem .8rem; color:#fff; background:#142b4c; border:1px solid #365271; border-radius:.6rem; box-shadow:0 10px 28px #10223b40; font-size:.72rem; line-height:1.5; opacity:0; visibility:hidden; pointer-events:none; transform:translateY(-.25rem); transition:opacity .15s ease,transform .15s ease,visibility 0s linear .15s; }}
    .term-context[data-term-placement="above"] .term-popover {{ transform:translateY(.25rem); }}
    .term-context:hover .term-popover,.term-context:focus-within .term-popover,.term-context.term-open .term-popover {{ opacity:1; visibility:visible; pointer-events:auto; transform:translateY(0); transition-delay:0s; }}
    .term-popover::before {{ display:block; content:attr(data-term-short); font-weight:650; overflow-wrap:anywhere; }}
    .term-full-link {{ display:block; margin-top:.52rem; padding-top:.42rem; border-top:1px solid #ffffff2e; color:#b9dbff; font-size:.66rem; font-weight:850; text-decoration:underline; text-underline-offset:.16rem; }}
    .term-full-link:focus-visible {{ outline:2px solid #fff; outline-offset:2px; border-radius:.18rem; }}
    .trust-strip,.why-context,.causal-audit-section,.canonical-questions,.change-summary,.conclusion-boundary,.acceptance-axes,.flow-position,.research-drilldown,.audit-destinations {{ margin:0 0 1.35rem; border:1px solid var(--line); border-radius:1rem; background:#fff; box-shadow:0 10px 30px #12213a0a; overflow:visible; }}
    .trust-strip {{ overflow:hidden; border:0; }}
    .trust-strip > header {{ padding:2.4rem 1.6rem; }}
    .trust-strip > header h1 {{ font-size:clamp(2rem,5vw,3.8rem); }}
    .trust-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:.55rem; padding:1rem 1.2rem; background:#fff; }}
    .trust-grid p {{ margin:0; padding:.62rem .7rem; border:1px solid #dbe4ef; border-radius:.55rem; color:#43526a; background:#f8fafc; font-size:.7rem; line-height:1.4; }}
    .trust-grid p span {{ display:block; }}
    .trust-grid p span + span {{ margin-top:.16rem; }}
    .trust-grid-boundary {{ color:#714155!important; border-color:#ead1da!important; background:#fff7f9!important; }}
    .trust-stop {{ margin:0; padding:.8rem 1.2rem; color:#6f4d0a; background:#fff3d8; border-top:1px solid #efd79f; font-weight:850; }}
    .reader-section-heading {{ padding:1.25rem 1.35rem .9rem; }}
    .reader-section-heading h2 {{ margin:.18rem 0 .35rem; font-size:clamp(1.35rem,2.7vw,2rem); line-height:1.2; }}
    .reader-section-heading > p:last-child {{ max-width:780px; margin:.25rem 0 0; color:var(--muted); }}
    .research-closure {{ margin:0 1.35rem 1rem; padding:1rem; border:1px solid #c5d7ec; border-radius:.82rem; background:#f4f8ff; }}
    .reader-decision-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.65rem; }}
    .reader-decision-card {{ min-width:0; padding:.85rem .9rem; border:1px solid #d6e2f1; border-radius:.7rem; background:#fff; }}
    .reader-decision-card > span,.reader-decision-card > strong {{ display:block; }}
    .reader-decision-card > span {{ color:#657895; font-size:.63rem; font-weight:900; letter-spacing:.04em; }}
    .reader-decision-card > strong {{ margin-top:.3rem; color:#173d70; font-size:.82rem; line-height:1.55; }}
    .reader-decision-card[data-reader-decision="CURRENT_DECISION"] {{ border-color:#a9c5e9; background:#edf4ff; }}
    .reader-decision-card[data-reader-decision="NEXT_STEP"] {{ border-color:#9acfc6; background:#edf8f5; }}
    .reader-safety {{ margin:.75rem 0 0; padding:.72rem .82rem; border-left:4px solid #b34b61; border-radius:.48rem; color:#6f3140; background:#faedf0; font-size:.78rem; line-height:1.55; }}
    .reader-plain-flow {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:1.25rem; margin:.85rem 0 0; padding:0; list-style:none; }}
    .reader-plain-flow li {{ position:relative; min-width:0; padding:.64rem .7rem; border:1px solid #dce4ee; border-radius:.55rem; color:#526177; background:#fff; font-size:.7rem; font-weight:800; line-height:1.45; text-align:center; }}
    .reader-plain-flow li::after {{ position:absolute; right:-.95rem; top:50%; content:"→"; color:#7890ae; font-size:.9rem; transform:translateY(-50%); }}
    .reader-plain-flow li:last-child::after {{ display:none; }}
    .reader-plain-flow .is-current {{ color:#0c6259; border-color:#8dc7be; background:#e9f7f4; }}
    .local-research-explanation {{ margin:0 1.35rem 1rem; border:1px solid #d4dfec; border-radius:.75rem; background:#fbfcfe; overflow:hidden; }}
    .local-research-explanation > summary {{ padding:.85rem 1rem; color:#244f83; background:#eef4fb; font-size:.8rem; font-weight:900; }}
    .local-research-explanation-body {{ padding:.95rem 0 .15rem; }}
    .local-why-heading {{ padding:.25rem 1.35rem .85rem; }}
    .local-why-heading h3 {{ margin:.18rem 0 .3rem; font-size:clamp(1.15rem,2.2vw,1.55rem); line-height:1.25; }}
    .local-why-heading > p:last-child {{ max-width:820px; margin:.2rem 0 0; color:var(--muted); font-size:.78rem; }}
    .reader-problem {{ margin:0 1.35rem .9rem; padding:.9rem 1rem; border-left:5px solid var(--blue); border-radius:.55rem; color:#173b70; background:#edf4ff; font-size:1.05rem; font-weight:850; }}
    .causal-chain {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.75rem; margin:0; padding:0 1.35rem 1rem; list-style:none; }}
    .causal-node {{ position:relative; min-width:0; padding:1rem; border:1px solid #d7e1ee; border-radius:.75rem; background:#fbfcfe; }}
    .causal-node[data-causal-node="RESULT"] {{ border-color:#d9bd75; background:#fff9e8; }}
    .causal-node[data-causal-node="NEXT_STEP"] {{ border-color:#8dc7be; background:#edf8f5; }}
    .causal-number {{ display:block; color:#7a879a; font-size:.64rem; font-weight:900; letter-spacing:.1em; }}
    .causal-link {{ position:absolute; top:.65rem; right:.7rem; padding:.1rem .35rem; color:#53657d; background:#e8eef6; border-radius:999px; font-size:.58rem; font-weight:850; }}
    .causal-question {{ margin:.42rem 0 .3rem; font-weight:900; }}
    .causal-answer {{ margin:0; color:#495870; font-size:.76rem; line-height:1.55; }}
    .why-boundary-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.65rem; padding:0 1.35rem 1rem; }}
    .why-boundary-grid p {{ margin:0; padding:.75rem .8rem; border-radius:.62rem; background:#f4f6f9; font-size:.75rem; }}
    .why-boundary-grid p:nth-child(2) {{ color:#7d2d3e; background:#faedf0; }}
    .why-boundary-grid p:nth-child(3) {{ color:#0b6259; background:#eaf7f3; }}
    .causal-audit > ul {{ display:grid; gap:.5rem; padding:0 1.25rem 1.25rem; list-style:none; }}
    .causal-audit li {{ padding:.6rem; border:1px solid #dfe5ed; border-radius:.45rem; background:#fff; font-size:.65rem; overflow-wrap:anywhere; }}
    .causal-audit-section {{ background:#f8fafc; box-shadow:none; }}
    .causal-audit-section > details > summary {{ padding:.85rem 1.1rem; color:#53657d; font-size:.74rem; font-weight:850; }}
    .canonical-questions > nav {{ padding:0 1.35rem; }}
    .canonical-questions > .answer-grid {{ padding:0 1.35rem 1.35rem; }}
    .change-state {{ display:inline-block; margin:0 1.35rem .7rem; padding:.25rem .65rem; color:#176557; background:#e6f5f0; border-radius:999px; font-weight:850; }}
    .change-summary > p[data-always-visible] {{ margin:.2rem 1.35rem 1rem; }}
    .date-context {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.55rem; margin:0; padding:0 1.35rem 1.1rem; }}
    .date-context div {{ padding:.65rem .7rem; border:1px solid #dfe5ed; border-radius:.55rem; background:#f8fafc; }}
    .date-context dt {{ color:#68758a; font-size:.62rem; font-weight:850; }}
    .date-context dd {{ margin:.15rem 0 0; font-size:.72rem; overflow-wrap:anywhere; }}
    .conclusion-boundary > p {{ margin:.5rem 1.35rem; padding:.8rem .9rem; border-radius:.6rem; background:#f4f6f9; }}
    .conclusion-boundary > p:last-child {{ margin-bottom:1.35rem; }}
    .boundary-result {{ border-left:5px solid #b38526; color:#634910; background:#fff8e6!important; font-weight:850; }}
    .boundary-risk {{ border-left:5px solid var(--red); color:#792f3e; background:#faecef!important; }}
    .acceptance-axis-grid,.flow-position-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.65rem; margin:0; padding:0 1.35rem 1rem; list-style:none; }}
    .acceptance-axis-grid li,.flow-position-grid li {{ padding:.85rem .9rem; border:1px solid #dfe5ed; border-radius:.65rem; background:#f9fbfd; }}
    .acceptance-axis-grid strong,.acceptance-axis-grid span,.flow-position-grid span,.flow-position-grid strong {{ display:block; }}
    .acceptance-axis-grid span,.flow-position-grid span {{ color:#6a7689; font-size:.66rem; }}
    .acceptance-axes > p,.flow-position > p {{ margin:.35rem 1.35rem; color:#56647a; font-size:.75rem; }}
    .acceptance-axes > p:last-child,.flow-position > p:last-child {{ margin-bottom:1.2rem; }}
    .flow-position-grid p {{ margin:.4rem 0 0; color:#516078; font-size:.72rem; }}
    .drilldown-toggle {{ margin-top:.75rem; padding:.7rem 1rem; border:0; border-radius:.55rem; color:#fff; background:#235b9f; font:inherit; font-weight:850; cursor:pointer; }}
    .drilldown-toggle:focus-visible {{ outline:3px solid #8bb8f3; outline-offset:3px; }}
    .research-drilldown-body {{ padding:0 1.1rem 1.1rem; }}
    .flat-disclosure {{ margin:.7rem 0 0; padding:.65rem .75rem; border:1px solid #dfe5ed; border-radius:.55rem; background:#f8fafc; }}
    .flat-disclosure-title {{ margin:0 0 .45rem; color:#385a86; font-size:.7rem; font-weight:850; }}
    .audit-destinations > .terminology-guide {{ margin:0 1.25rem 1rem; border:1px solid #cbd8e8; border-radius:.72rem; overflow:hidden; }}
    .audit-destinations > .terminology-guide > summary {{ padding:.85rem 1rem; }}
    .audit-destinations .terminology-guide-intro,.audit-destinations .terminology-grid {{ width:auto; margin-left:1rem; margin-right:1rem; }}
    code {{ overflow-wrap:anywhere; }}
    footer {{ margin-top:2rem; padding-top:1rem; border-top:1px solid var(--line); color:var(--muted); font-size:.82rem; overflow-wrap:anywhere; }}
    @media (max-width:900px) {{ .trust-grid,.reader-decision-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .system-orientation ol {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .causal-chain {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .date-context {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .progress-matrix-grid,.progress-matrix-heading {{ grid-template-columns:1fr; }} .strategy-conclusion-count {{ width:170px; }} }}
    @media (max-width:900px) {{ .effectiveness-title-row,.effectiveness-boundary,.flow-heading,.qqq-title-row {{ grid-template-columns:1fr; display:grid; }} .you-are-here,.qqq-count {{ margin-top:1rem; }} .qqq-count {{ width:142px; }} .qqq-task-list {{ grid-template-columns:1fr; }} .system-flow {{ grid-template-columns:repeat(2,minmax(0,1fr)); grid-template-areas:"s1 s2" "s4 s3" "s5 s6" "s8 s7"; }} .flow-stage-shell::after,.flow-stage-shell:nth-child(5)::after {{ content:"→"; right:-.78rem; left:auto; top:98px; bottom:auto; transform:translateY(-50%); }} .flow-stage-shell:nth-child(2)::after,.flow-stage-shell:nth-child(4)::after,.flow-stage-shell:nth-child(6)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.2rem; transform:translateX(50%); }} .flow-stage-shell:nth-child(3)::after,.flow-stage-shell:nth-child(7)::after {{ content:"←"; right:auto; left:-.78rem; top:98px; transform:translateY(-50%); }} .flow-stage-shell:nth-child(8)::after {{ display:none; }} .focus-panel,.result-ledger-intro {{ grid-template-columns:1fr; }} .historical-lane-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} nav,.result-ledger-grid {{ grid-template-columns:1fr 1fr; }} .citations {{ grid-template-columns:1fr; }} }}
    @media (max-width:620px) {{ .trust-grid,.system-orientation ol,.reader-decision-grid,.reader-plain-flow,.causal-chain,.why-boundary-grid,.date-context,.acceptance-axis-grid,.flow-position-grid,.terminology-grid,.metrics,nav,.reader-answer-grid,.effectiveness-review-grid,.effectiveness-audit dl,.focus-ledger,.provenance-ledger,.drilldown-grid,.historical-lane-grid,.result-ledger-grid,.result-status-pair,.attribution-meta,.owner-next-grid,.transition-detail,.qqq-reader-boundary,.qqq-group-boundary,.qqq-reader-grid,.work-reader-grid,.progress-dimension-grid,.concept-grid {{ grid-template-columns:1fr; }} .trust-strip > header {{ padding:1.8rem 1rem; }} .trust-grid {{ padding:.8rem; }} .research-closure,.local-research-explanation {{ margin-left:.8rem; margin-right:.8rem; }} .reader-plain-flow {{ gap:1.15rem; }} .reader-plain-flow li::after {{ right:50%; top:auto; bottom:-1.05rem; content:"↓"; transform:translateX(50%); }} .page-effectiveness,.flow-map,.result-ledger,.qqq-projection {{ padding:1rem; }} .successor-coverage li {{ grid-template-columns:1fr; }} .qqq-decision {{ grid-template-columns:1fr; }} .qqq-decision-label {{ padding:.7rem .85rem; }} .qqq-task-identity {{ align-items:flex-start; flex-direction:column; }} .qqq-layer-row {{ grid-template-columns:1fr; gap:.05rem; }} .historical-lane-head {{ display:block; }} .historical-lane-boundary {{ margin-top:.6rem; }} .provenance-copy {{ display:block; }} .provenance-copy > p:last-child {{ margin-top:.4rem; }} .system-flow {{ grid-template-columns:1fr; grid-template-areas:"s1" "s2" "s3" "s4" "s5" "s6" "s7" "s8"; gap:1.15rem; }} .flow-stage > .stage-summary {{ min-height:0; }} .flow-stage-shell::after,.flow-stage-shell:nth-child(n+5):nth-child(-n+7)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.18rem; transform:translateX(50%); }} .flow-stage-shell:nth-child(8)::after {{ display:none; }} .drilldown-grid .drilldown-wide {{ grid-column:auto; }} .reader-facts > li {{ grid-template-columns:1fr; }} .answer-head,.result-ledger-head {{ display:block; }} .status {{ display:inline-block; margin-top:.7rem; }} .result-status {{ margin-top:.55rem; text-align:left; }} .attribution-heading {{ align-items:flex-start; flex-direction:column; }} .attribution-id {{ text-align:left; }} }}
    @media (prefers-reduced-motion:reduce) {{ html {{ scroll-behavior:auto; }} .stage-disclosure-cue i {{ transition:none; }} }}
    @media print {{ body {{ background:#fff; }} nav {{ display:none; }} .stage-drilldown {{ display:block!important; }} .answer-card {{ break-inside:avoid; box-shadow:none; }} }}
  </style>
</head>
<body>
  <a class="skip-link" href="#main-content">跳到主要内容</a>
  {term_definition_bank}
  <main id="main-content">
    {trust_strip}
    {why_context}
    {canonical_questions}
    {change_summary}
    {conclusion_boundary}
    {acceptance_axes}
    {flow_position}
    {research_drilldown}
    {audit_destinations}
  </main>
  {_READER_INTERACTION_SCRIPT}
</body>
</html>
"""
    html = _flatten_nested_disclosures(html)
    html = _add_inline_term_interactions(html, showcase.reader_terminology)
    build_rendered_term_inventory(
        html_bytes=html.encode("utf-8"),
        policy=showcase.reader_terminology,
    )
    return html


def cited_query_responses_json_bytes(
    responses: Sequence[StrategyResearchCitedQueryResponse],
) -> bytes:
    payload = {
        "schema_version": "atlas_cited_query_response_set.v1",
        "response_count": len(responses),
        "responses": [item.to_dict() for item in responses],
        "manual_review_only": True,
        "production_effect": "none",
        "broker_action": "none",
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def cited_query_validations_json_bytes(
    validations: Sequence[CitedQueryValidationResult],
) -> bytes:
    payload = {
        "schema_version": "atlas_cited_query_validation_set.v1",
        "status": ("PASS" if all(item.status == "PASS" for item in validations) else "FAIL"),
        "validation_count": len(validations),
        "validations": [item.to_dict() for item in validations],
        "production_effect": "none",
        "broker_action": "none",
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def status_explanation_validation_json_bytes(
    validation: StatusExplanationProjectionValidation,
) -> bytes:
    return (
        json.dumps(
            validation.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def qqq_options_projection_validation_json_bytes(
    validation: QQQOptionsProjectionValidation,
) -> bytes:
    return (
        json.dumps(
            validation.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def write_cited_query_artifacts(
    showcase: AtlasCitedQueryShowcase,
    output_directory: Path,
) -> tuple[AtlasCitedQueryRenderedArtifact, ...]:
    if any(item.status != "PASS" for item in showcase.validations):
        raise ValueError("ATLAS_CITED_QUERY_ARTIFACT_VALIDATION_FAILED")
    _status_explanation_records_by_stage(showcase)
    _work_progress_records_by_stage(showcase)
    _validate_qqq_options_projection_binding(showcase)
    output_directory.mkdir(parents=True, exist_ok=True)
    html_bytes = render_cited_query_html(showcase).encode("utf-8")
    reader_accessibility = validate_reader_accessibility(html_bytes)
    if reader_accessibility.status != "PASS":
        raise ValueError(
            "ATLAS_READER_ACCESSIBILITY_VALIDATION_FAILED:"
            + ",".join(item.code for item in reader_accessibility.violations)
        )
    reader_terminology_inventory = build_rendered_term_inventory(
        html_bytes=html_bytes,
        policy=showcase.reader_terminology,
    )
    payloads = {
        "index.html": html_bytes,
        "qqq_options_projection.json": showcase.qqq_options_projection.canonical_bytes,
        "qqq_options_projection_validation.json": (
            qqq_options_projection_validation_json_bytes(showcase.qqq_options_projection_validation)
        ),
        "responses.json": cited_query_responses_json_bytes(showcase.responses),
        "reader_accessibility_validation.json": (
            json.dumps(
                reader_accessibility.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8"),
        "reader_terminology_inventory.json": reader_terminology_inventory.canonical_bytes,
        "status_explanation_validation.json": status_explanation_validation_json_bytes(
            showcase.status_explanation_validation
        ),
        "status_explanations.json": showcase.status_explanations.canonical_bytes,
        "validation.json": cited_query_validations_json_bytes(showcase.validations),
        "work_progress_explanation_validation.json": work_progress_validation_json_bytes(
            showcase.work_progress_validation
        ),
        "work_progress_explanations.json": showcase.work_progress.canonical_bytes,
    }
    artifacts: list[AtlasCitedQueryRenderedArtifact] = []
    for name, payload in payloads.items():
        result = write_bytes_atomic(output_directory / name, payload)
        artifacts.append(
            AtlasCitedQueryRenderedArtifact(
                path=result.path.as_posix(),
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        )
    page_prefix = "outputs/atlas/strategy_research_cited_query/trading_2470_v1"
    rendered_identities = tuple(
        PageArtifactIdentity(
            role="ATLAS_PAGE_" + name.upper().replace(".", "_"),
            locator=f"{page_prefix}/{name}",
            sha256=hashlib.sha256(payload).hexdigest(),
            byte_count=len(payload),
        )
        for name, payload in sorted(payloads.items())
    )
    preliminary = showcase.page_effectiveness
    manifest = build_page_effectiveness_manifest(
        repository_root=Path(__file__).resolve().parents[3],
        repository_commit=preliminary.repository_commit,
        source_snapshot_commit=preliminary.source_snapshot_commit,
        rendered_artifacts=rendered_identities,
        engineering_status=preliminary.acceptance[0].status,
        engineering_evidence_refs=preliminary.acceptance[0].evidence_refs,
        owner_visual_review=preliminary.acceptance[1],
        reader_comprehension_review=preliminary.acceptance[2],
    )
    validation = validate_page_effectiveness_manifest(
        repository_root=Path(__file__).resolve().parents[3],
        manifest=manifest,
        current_repository_commit=preliminary.repository_commit,
        rendered_payloads=payloads,
    )
    if validation.status != "PASS":
        raise ValueError(
            "ATLAS_PAGE_EFFECTIVENESS_VALIDATION_FAILED:" + ",".join(validation.errors)
        )
    sidecars = write_page_effectiveness_sidecars(
        output_directory=output_directory,
        manifest=manifest,
        validation=validation,
    )
    for name, identity in zip(
        ("page_effectiveness.json", "page_effectiveness_validation.json"),
        sidecars,
        strict=True,
    ):
        artifacts.append(
            AtlasCitedQueryRenderedArtifact(
                path=(output_directory / name).as_posix(),
                sha256=identity.sha256,
                size_bytes=identity.byte_count,
            )
        )
    return tuple(artifacts)


__all__ = [
    "AtlasCitedQueryRenderedArtifact",
    "AtlasCitedQueryShowcase",
    "build_cited_query_showcase",
    "cited_query_responses_json_bytes",
    "cited_query_validations_json_bytes",
    "qqq_options_projection_validation_json_bytes",
    "render_cited_query_html",
    "status_explanation_validation_json_bytes",
    "write_cited_query_artifacts",
]
