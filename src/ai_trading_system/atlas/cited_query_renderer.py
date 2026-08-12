from __future__ import annotations

# HTML/CSS source lines remain readable as one semantic declaration.
# ruff: noqa: E501
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html import escape
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
    StrategyResearchPageEffectivenessManifest,
)
from ai_trading_system.contracts.strategy_research_qqq_options_projection import (
    QQQOptionsProjectionCard,
    StrategyResearchQQQOptionsProjectionBundle,
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
    page_effectiveness = build_page_effectiveness_manifest(
        repository_root=root,
        engineering_status=page_engineering_status,
        engineering_evidence_refs=page_engineering_evidence_refs,
        owner_visual_review=page_owner_visual_review,
        reader_comprehension_review=page_reader_comprehension_review,
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


def _render_response(response: StrategyResearchCitedQueryResponse) -> str:
    prompt = _QUESTION_PROMPTS[response.request.question_id]
    claims = (
        "".join(f'<p class="claim">{escape(claim.text_zh)}</p>' for claim in response.claims)
        or '<p class="claim muted">没有生成未经引用的 claim。</p>'
    )
    limitations = (
        "<ul>" + "".join(f"<li>{escape(item)}</li>" for item in response.limitations) + "</ul>"
        if response.limitations
        else '<p class="good">没有额外证据限制。</p>'
    )
    citations = "".join(_render_citation(item) for item in response.citations)
    status_class = response.answer_status.value.lower()
    return f"""
    <article class="answer-card" id="{escape(response.request.question_id.value.lower())}">
      <div class="answer-head">
        <div>
          <p class="question-id">{escape(response.request.question_id.value)}</p>
          <h2>{escape(prompt)}</h2>
          <p class="target">目标：<code>{escape(response.request.target_kind.value)}</code> · <code>{escape(response.request.target_id)}</code></p>
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
        <p class="reason">机器原因码：<code>{escape(" · ".join(response.reason_codes) or "none")}</code></p>
      </section>
      <details>
        <summary>查看 {len(response.citations)} 条完整引用与 lineage</summary>
        <ul class="citations">{citations or "<li>没有通过引用闭包的证据。</li>"}</ul>
      </details>
      <p class="identity">response <code>{escape(response.response_id)}</code> · request <code>{escape(response.request.request_id)}</code></p>
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
        f'<li class="result-attribution" data-attribution-id="{escape(item.attribution_id)}" '
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
) -> str:
    limitations = "".join(f"<li>{escape(item)}</li>" for item in result.limitations)
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
            "<span><small>来源原始状态</small><strong>历史 artifact 状态</strong>"
            f"<code>{escape(result.source_original_status)}</code></span>"
        )
    )
    mapping_rationale = (
        ""
        if result.status_mapping_rationale is None
        else (
            '<p class="historical-status-rationale"><strong>为什么这样映射：</strong>'
            f"{escape(result.status_mapping_rationale)}</p>"
        )
    )
    return f"""
    <article class="result-ledger-card{" historical-result-card" if is_historical else ""}" data-result-id="{escape(result.result_id)}" data-raw-status="{escape(raw_status)}" data-display-status="{escape(display_status)}" data-historical-record="{"true" if is_historical else "false"}"{(' data-source-original-status="' + escape(result.source_original_status or "") + '"') if is_historical else ""}>
      <div class="result-ledger-head">
        <div>
          <p class="result-sequence">RESULT · <code>{escape(result.result_id)}</code></p>
          {historical_badge}
          <h3>{escape(result.title)}</h3>
          <p class="result-node">对应节点 <code>{escape(result.node_id)}</code></p>
        </div>
        <span class="result-status {_result_status_tone(display_status)}">
          <strong>{escape(_RESULT_STATUS_LABELS[display_status])}</strong>
          <code>{escape(display_status)}</code>
        </span>
      </div>
      <p class="result-summary">{escape(result.reader_summary)}</p>
      <div class="result-status-pair" aria-label="机器状态与展示状态">
        <span><small>机器原始状态</small><strong>{escape(_RESULT_STATUS_LABELS[raw_status])}</strong><code>{escape(raw_status)}</code></span>
        <span><small>读者展示状态</small><strong>{escape(_RESULT_STATUS_LABELS[display_status])}</strong><code>{escape(display_status)}</code></span>
        <span><small>信息类型</small><strong>{escape(_ASSERTION_LABELS[result.assertion_kind])}</strong><code>{escape(result.assertion_kind.value)}</code></span>
        <span><small>投资结论</small><strong>不是</strong><code>investment_facing=false</code></span>
        {original_status}
      </div>
      {mapping_rationale}
      <details class="result-evidence">
        <summary>查看限制、来源与 {len(attributions)} 条归因</summary>
        <div class="result-evidence-body">
          <section>
            <h4>限制</h4>
            <ul>{limitations or "<li>没有额外限制。</li>"}</ul>
          </section>
          <section>
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
        f"<strong>{count}</strong>{escape(_RESULT_STATUS_LABELS[status])}<code>{escape(status)}</code></span>"
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
          <p>这是 Atlas V1.3 的代表性主线 + 五份已审阅历史记录，不是全仓历史研究的完整清单；历史 PASS 不等于当前策略 PASS。</p>
          <code>coverage_scope=ATLAS_V1_3_REPRESENTATIVE_PLUS_REVIEWED_HISTORY</code>
          <code>historical_repository_coverage_complete=false</code>
        </div>
      </div>
      <div class="ledger-summary" aria-label="结果展示状态分布">{status_summary}</div>
      <p class="ledger-reading-note"><strong>怎样读：</strong>先看标题、摘要和“读者展示状态”；展开后再看限制、source refs 与归因。raw/display status 均不是投资评级，工程 PASS 也不等于 strategy PASS。</p>
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
            '<span class="historical-lane-status">历史 · LIMITED</span>'
            f"<strong>{escape(item.title)}</strong>"
            f"<code>{escape(item.source_original_status or '')}</code>"
            "<small>NEUTRAL provenance · 非当前关注</small>"
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
            <p>这五份材料只补全历史路径和来源。卡片顺序是阅读顺序，不表示因果或优先级；四个 canonical raw PASS 只表示历史 artifact 已形成。</p>
          </div>
          <span class="historical-lane-boundary"><strong>5</strong> 项历史记录<small>全部 display LIMITED</small></span>
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
        '<p class="progress-matrix-warning"><strong>最重要的边界：</strong>绿色的“能力可用”“已验证”或页面验收 PASS，都不等于 strategy PASS、收益稳健或可以下单。</p>'
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
        f'data-reader-section="{escape(section_id)}">'
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
                '<dl class="transition-detail">'
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
        'data-reader-section="what_changes">'
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
        '<section class="work-progress-reader" data-reader-section="work_progress">'
        '<div class="work-purpose" data-reader-section="why_needed">'
        "<h4>为什么需要这一步</h4>"
        f"<p>{escape(record.why_needed_zh)}</p>"
        "</div>"
        '<div class="work-reader-grid">'
        '<section class="work-reader-card" data-reader-section="work_items">'
        "<h4>具体做什么</h4>"
        f'<ol class="work-item-list">{work_items}</ol>'
        "</section>"
        '<section class="work-reader-card" data-reader-section="expected_outputs">'
        "<h4>预期产物</h4>"
        f'<ul class="work-output-list">{expected_outputs}</ul>'
        "</section>"
        "</div>"
        '<section class="progress-dimensions" data-reader-section="progress_dimensions">'
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
        '<section class="work-reader-card" data-reader-section="downstream_use">'
        "<h4>完成后怎样被使用</h4>"
        f"<p>{escape(record.downstream_use_zh)}</p>"
        "</section>"
        '<section class="work-reader-card work-boundary" data-reader-section="boundary">'
        "<h4>不能说明什么</h4>"
        f"<p>{escape(record.boundary_zh)}</p>"
        "</section>"
        '<section class="work-reader-card" data-reader-section="next_trigger">'
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
        '<section class="reader-conclusion" data-reader-section="conclusion">'
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
        + '<section class="reader-section reader-owner" data-reader-section="owner_and_next">'
        "<h4>由谁负责，以及下一步怎么读</h4>"
        '<div class="owner-next-grid">'
        '<div><span class="owner-label">责任信息</span>'
        f"{_render_value_state(record.responsible_role.value_state)}"
        f"<p>{escape(record.responsible_role.text_zh)}</p></div>"
        '<div><span class="owner-label">下一步</span>'
        f"<p>{escape(record.next_reader_action)}</p></div>"
        "</div></section>"
        '<details class="reader-audit">'
        "<summary>查看审计依据</summary>"
        '<div class="reader-audit-body">'
        f"<p>状态对象 <code>{escape(record.status_object_scope)}</code> · "
        f"目标 <code>{escape(record.target_id)}</code></p>"
        "<h5>Authority bindings</h5>"
        f"<ul>{bindings}</ul>"
        "<h5>Technical refs / source refs</h5>"
        f"<ul>{refs}</ul>"
        "<h5>已检查范围</h5>"
        f"<p>{escape(' · '.join(record.checked_authority_scope))}</p>"
        "<h5>已检查 authority IDs</h5>"
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
            f"<code>{escape(concept.concept_id)}</code>"
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


def _render_qqq_projection_card(card: QQQOptionsProjectionCard) -> str:
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
    priority_facts = "".join(
        f"<li><span>{index:02d}</span><strong>{escape(fact)}</strong></li>"
        for index, fact in enumerate(card.priority_facts, start=1)
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
        f"<code>{escape(card.task_id)}</code>"
        f'<span class="qqq-layer-badge">{escape(layer)} · {escape(_QQQ_LAYER_LABELS[layer])}</span>'
        "</span>"
        f"<strong>{escape(card.title_zh)}</strong>"
        f"<span>{escape(card.positioning_zh)}</span>"
        '<i aria-hidden="true">⌄</i>'
        "</summary>"
        '<div class="qqq-task-body">'
        '<div class="qqq-reader-grid">'
        '<section class="qqq-reader-fact qqq-completed">'
        "<h5>已经做到</h5>"
        f"<p>{escape(card.completed_zh)}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-not-proven">'
        "<h5>仍不能证明</h5>"
        f"<p>{escape(card.not_proven_zh)}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-blocker">'
        "<h5>为什么停在这里</h5>"
        f"<p>{escape(card.blocker_zh)}</p>"
        "</section>"
        '<section class="qqq-reader-fact qqq-next">'
        "<h5>接下来要看什么</h5>"
        f"<p>{escape(card.next_reader_action_zh)}</p>"
        "</section>"
        "</div>"
        '<div class="qqq-priority-facts">'
        "<h5>必须按这个顺序理解</h5>"
        f"<ol>{priority_facts}</ol>"
        "</div>"
        f"{mismatch}"
        '<details class="qqq-audit">'
        "<summary>查看五层状态与 exact source</summary>"
        '<div class="qqq-audit-body">'
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
            f"<h3>{escape(group.title_zh)}</h3>"
            f"<p>{escape(group.capability_zh)}</p></div>"
            "</div>"
            '<div class="qqq-group-boundary">'
            f"<p><strong>还不能证明：</strong>{escape(group.not_proven_zh)}</p>"
            f"<p><strong>下一道决定：</strong>{escape(group.owner_need_zh)}</p>"
            "</div>"
            '<div class="qqq-task-list">'
            + "".join(_render_qqq_projection_card(by_task[task_id]) for task_id in group.task_ids)
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
          <code>{escape(bundle.aggregate_conclusion)}</code>
          <p>{escape(bundle.aggregate_explanation_zh)}</p>
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
            f"<code>{escape(response.request.target_id)}</code>"
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
            f'<code class="stage-id">{escape(stage_id)}</code>'
            f"{_render_stage_axis_summary(work_progress_by_stage[stage_id])}"
            f'<span class="stage-progress {escape(status_by_stage[stage_id].status_tone)}">'
            '<span class="progress-dot" aria-hidden="true"></span>'
            '<span class="stage-progress-copy"><small>本页状态</small>'
            f"<strong>{escape(status_by_stage[stage_id].status_label)}</strong>"
            f"<code>{escape(status_by_stage[stage_id].status_code)}</code></span>"
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
            "<details>"
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
          <p class="section-kicker">CURRENT FOCUS · EXACT IDS</p>
          <h3>当前实际关注路径</h3>
          <p>这些节点直接来自本页五个 canonical requests。没有“最相关”排序、模糊匹配或名称推断。</p>
        </div>
        <ul class="focus-ledger">{focus_ledger}</ul>
      </div>
      {historical_flow_lane}
      <section class="provenance-panel" aria-labelledby="status-provenance-title">
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


def _render_page_effectiveness(showcase: AtlasCitedQueryShowcase) -> str:
    manifest = showcase.page_effectiveness
    if len(manifest.task_coverage) != 31:
        raise ValueError("ATLAS_PAGE_EFFECTIVENESS_TASK_COVERAGE_INVALID")
    acceptance = "".join(
        (
            f'<li class="effectiveness-review-card" data-review-track="{escape(item.track.value)}" '
            f'data-review-status="{escape(item.status.value)}">'
            f"<strong>{escape(item.track.value)}</strong>"
            f"<span>{escape(_PAGE_ACCEPTANCE_LABELS[item.status])}</span>"
            f"<code>{escape(item.status.value)}</code>"
            "</li>"
        )
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
        if 2494 <= int(item.task_id.split("-", 1)[1].split("_", 1)[0]) <= 2512
    )
    return f"""
    <section class="page-effectiveness" id="page-effectiveness" aria-labelledby="page-effectiveness-title" data-page-freshness="{escape(manifest.freshness_status.value)}" data-task-coverage-count="{len(manifest.task_coverage)}">
      <div class="effectiveness-title-row">
        <div>
          <p class="section-kicker">PAGE VALIDITY · FOUR INDEPENDENT LAYERS</p>
          <h2 id="page-effectiveness-title">先确认：这张页面现在还能不能信</h2>
          <p>页面可打开只说明浏览器读到了 HTML。这里把来源、语义、视觉和读者理解分开验收，避免旧页面继续显示却遗漏新的研究事实。</p>
        </div>
        <div class="freshness-badge" data-freshness-status="{escape(manifest.freshness_status.value)}">
          <span>页面 freshness</span>
          <strong>{escape(_PAGE_FRESHNESS_LABELS[manifest.freshness_status])}</strong>
          <code>{escape(manifest.freshness_status.value)}</code>
        </div>
      </div>
      <div class="reader-answer-grid" aria-label="读者先回答的六个问题">
        <article><span>01 · 当前主线</span><strong>QQQ Options 的 DAILY 工程、G3 evidence 与 export-safe collector 合同已建立，但真实策略仍保持 NO-GO / policy blocked。</strong></article>
        <article><span>02 · 最大阻塞</span><strong>18 个 G3 slots 尚无经 DQ/PIT admission 的 primary-window evidence，G2 policy value 数仍为 0。</strong></article>
        <article><span>03 · 已做到什么</span><strong>工程合同、DQ/PIT、离线 mechanics、10-series collector 与证据结构可重放；这只是能力，不是盈利或风险证据。</strong></article>
        <article><span>04 · 不能推出什么</span><strong>不能推出策略有效、收益稳健、风险可接受，也不能把局部 capability GO 解释成 strategy PASS。</strong></article>
        <article><span>05 · 下一步</span><strong>先封存 exact run proposal，由 Project Owner 单次授权零订单 collection；经 DQ/PIT 后再审阅 typed G2 policy。</strong></article>
        <article class="reader-answer-stop"><span>06 · 现在能否投资或下单</span><strong>不能。selection=false、orders/fills=0；本页不授权真实 engine、外部动作或交易。</strong></article>
      </div>
      <div class="effectiveness-boundary">
        <div>
          <h3>三种“通过”互不代签</h3>
          <ul class="effectiveness-review-grid">{acceptance}</ul>
          <p>工程自动化只能更新 <code>ENGINEERING_VALIDATION</code>；Owner 视觉验收和目标读者理解验收必须来自真实人工事实。</p>
        </div>
        <details class="successor-coverage">
          <summary>查看 TRADING-2494–2512 如何影响当前页面</summary>
          <ul>{successor_rows}</ul>
        </details>
      </div>
      <details class="effectiveness-audit">
        <summary>审计信息：exact commit、policy 与 source coverage</summary>
        <dl>
          <div><dt>repository commit</dt><dd><code>{escape(manifest.repository_commit)}</code></dd></div>
          <div><dt>source snapshot</dt><dd><code>{escape(manifest.source_snapshot_commit)}</code></dd></div>
          <div><dt>policy SHA-256</dt><dd><code>{escape(manifest.policy_sha256)}</code></dd></div>
          <div><dt>覆盖范围</dt><dd><code>TRADING-2481..2504, 2506..2512</code> · {len(manifest.source_artifacts)} semantic sources</dd></div>
        </dl>
      </details>
    </section>
    """


def render_cited_query_html(showcase: AtlasCitedQueryShowcase) -> str:
    if len(showcase.responses) != len(CITED_QUERY_QUESTION_CATALOG):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_QUESTION_COUNT_INVALID")
    if any(item.status != "PASS" for item in showcase.validations):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_VALIDATION_NOT_PASS")
    _validate_showcase_response_bindings(showcase)
    _status_explanation_records_by_stage(showcase)
    answered = sum(
        item.answer_status is CitedQueryAnswerStatus.ANSWERED for item in showcase.responses
    )
    limited = sum(
        item.answer_status is CitedQueryAnswerStatus.LIMITED for item in showcase.responses
    )
    blocked = sum(
        item.answer_status is CitedQueryAnswerStatus.BLOCKED for item in showcase.responses
    )
    navigation = "".join(
        f'<a href="#{escape(item.request.question_id.value.lower())}">'
        f"{escape(_QUESTION_PROMPTS[item.request.question_id])}</a>"
        for item in showcase.responses
    )
    cards = "".join(_render_response(item) for item in showcase.responses)
    system_flow = _render_system_flow_map(showcase)
    page_effectiveness = _render_page_effectiveness(showcase)
    qqq_options_projection = _render_qqq_options_projection(showcase)
    result_ledger = _render_result_ledger(showcase)
    snapshot_id = str(showcase.snapshot_payload["snapshot_id"])
    diff_id = str(showcase.diff_payload["diff_id"])
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>Atlas 研究问答与证据</title>
  <style>
    :root {{ --ink:#172033; --muted:#697489; --line:#dfe4ec; --paper:#f4f6f9; --panel:#fff; --navy:#132743; --blue:#315fba; --green:#18705b; --teal:#0d7f77; --teal-soft:#e8f6f3; --blue-soft:#eaf1ff; --amber:#9b6b12; --red:#aa3d51; }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior:smooth; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font:16px/1.65 system-ui,-apple-system,"Segoe UI",sans-serif; }}
    header {{ padding:3.2rem max(1.2rem,calc((100vw - 1120px)/2)); color:#fff; background:linear-gradient(135deg,var(--navy),#244b83 70%,#2f6e76); }}
    header h1 {{ max-width:840px; margin:.25rem 0 .7rem; font-size:clamp(2.2rem,6vw,4.6rem); line-height:1.02; letter-spacing:-.035em; }}
    .eyebrow {{ margin:0; font-size:.78rem; font-weight:800; letter-spacing:.16em; }}
    .lead {{ max-width:780px; margin:.8rem 0 0; color:#d9e8ff; font-size:1.08rem; }}
    .metrics {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:.75rem; max-width:720px; margin-top:1.5rem; }}
    .metric {{ padding:.8rem 1rem; border:1px solid #ffffff2b; border-radius:.8rem; background:#ffffff12; }}
    .metric strong {{ display:block; font-size:1.8rem; }}
    .metric span {{ color:#d9e8ff; font-size:.82rem; }}
    main {{ width:min(1120px,calc(100% - 2rem)); margin:0 auto; padding:1.6rem 0 4rem; }}
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
    code {{ overflow-wrap:anywhere; }}
    footer {{ margin-top:2rem; padding-top:1rem; border-top:1px solid var(--line); color:var(--muted); font-size:.82rem; overflow-wrap:anywhere; }}
    @media (max-width:900px) {{ .progress-matrix-grid,.progress-matrix-heading {{ grid-template-columns:1fr; }} .strategy-conclusion-count {{ width:170px; }} }}
    @media (max-width:900px) {{ .effectiveness-title-row,.effectiveness-boundary,.flow-heading,.qqq-title-row {{ grid-template-columns:1fr; display:grid; }} .you-are-here,.qqq-count {{ margin-top:1rem; }} .qqq-count {{ width:142px; }} .qqq-task-list {{ grid-template-columns:1fr; }} .system-flow {{ grid-template-columns:repeat(2,minmax(0,1fr)); grid-template-areas:"s1 s2" "s4 s3" "s5 s6" "s8 s7"; }} .flow-stage-shell::after,.flow-stage-shell:nth-child(5)::after {{ content:"→"; right:-.78rem; left:auto; top:98px; bottom:auto; transform:translateY(-50%); }} .flow-stage-shell:nth-child(2)::after,.flow-stage-shell:nth-child(4)::after,.flow-stage-shell:nth-child(6)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.2rem; transform:translateX(50%); }} .flow-stage-shell:nth-child(3)::after,.flow-stage-shell:nth-child(7)::after {{ content:"←"; right:auto; left:-.78rem; top:98px; transform:translateY(-50%); }} .flow-stage-shell:nth-child(8)::after {{ display:none; }} .focus-panel,.result-ledger-intro {{ grid-template-columns:1fr; }} .historical-lane-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} nav,.result-ledger-grid {{ grid-template-columns:1fr 1fr; }} .citations {{ grid-template-columns:1fr; }} }}
    @media (max-width:620px) {{ .metrics,nav,.reader-answer-grid,.effectiveness-review-grid,.effectiveness-audit dl,.focus-ledger,.provenance-ledger,.drilldown-grid,.historical-lane-grid,.result-ledger-grid,.result-status-pair,.attribution-meta,.owner-next-grid,.transition-detail,.qqq-reader-boundary,.qqq-group-boundary,.qqq-reader-grid,.work-reader-grid,.progress-dimension-grid,.concept-grid {{ grid-template-columns:1fr; }} .page-effectiveness,.flow-map,.result-ledger,.qqq-projection {{ padding:1rem; }} .successor-coverage li {{ grid-template-columns:1fr; }} .qqq-decision {{ grid-template-columns:1fr; }} .qqq-decision-label {{ padding:.7rem .85rem; }} .qqq-task-identity {{ align-items:flex-start; flex-direction:column; }} .qqq-layer-row {{ grid-template-columns:1fr; gap:.05rem; }} .historical-lane-head {{ display:block; }} .historical-lane-boundary {{ margin-top:.6rem; }} .provenance-copy {{ display:block; }} .provenance-copy > p:last-child {{ margin-top:.4rem; }} .system-flow {{ grid-template-columns:1fr; grid-template-areas:"s1" "s2" "s3" "s4" "s5" "s6" "s7" "s8"; gap:1.15rem; }} .flow-stage > .stage-summary {{ min-height:0; }} .flow-stage-shell::after,.flow-stage-shell:nth-child(n+5):nth-child(-n+7)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.18rem; transform:translateX(50%); }} .flow-stage-shell:nth-child(8)::after {{ display:none; }} .drilldown-grid .drilldown-wide {{ grid-column:auto; }} .reader-facts > li {{ grid-template-columns:1fr; }} .answer-head,.result-ledger-head {{ display:block; }} .status {{ display:inline-block; margin-top:.7rem; }} .result-status {{ margin-top:.55rem; text-align:left; }} .attribution-heading {{ align-items:flex-start; flex-direction:column; }} .attribution-id {{ text-align:left; }} }}
    @media (prefers-reduced-motion:reduce) {{ html {{ scroll-behavior:auto; }} .stage-disclosure-cue i {{ transition:none; }} }}
    @media print {{ body {{ background:#fff; }} nav {{ display:none; }} .stage-drilldown {{ display:block!important; }} .answer-card {{ break-inside:avoid; box-shadow:none; }} }}
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">ATLAS · CITATION-FIRST QUERY V1</p>
    <h1>研究主线、结果与归因，一眼看清</h1>
    <p class="lead">这里用五个固定问题解释研究记录。每一句回答都闭包到 repository source、exact commit、SHA-256 与证据时点；信息不完整时明确标为 LIMITED，不补写不存在的事实。</p>
    <div class="metrics" aria-label="回答状态">
      <div class="metric"><strong>{answered}</strong><span>证据完整</span></div>
      <div class="metric"><strong>{limited}</strong><span>有依据但上下文有限</span></div>
      <div class="metric"><strong>{blocked}</strong><span>无法可靠回答</span></div>
    </div>
  </header>
  <main>
    <p class="notice"><strong>怎样阅读：</strong>先看“一句话回答”，再看“限制”；需要审计时展开引用。LIMITED 不等于研究失败，只表示时间、DQ 或研究上下文尚不完整。本页不是投资建议，也不会触发交易。</p>
    {page_effectiveness}
    {system_flow}
    {qqq_options_projection}
    {result_ledger}
    <nav aria-label="五个固定问题">{navigation}</nav>
    <div class="answer-grid">{cards}</div>
    <footer>
      <p>Snapshot <code>{escape(snapshot_id)}</code></p>
      <p>Diff <code>{escape(diff_id)}</code></p>
      <p>independent_validation=<code>PASS</code> · manual_review_only=<code>true</code> · production_effect=<code>none</code> · broker_action=<code>none</code></p>
    </footer>
  </main>
</body>
</html>
"""


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
    payloads = {
        "index.html": render_cited_query_html(showcase).encode("utf-8"),
        "qqq_options_projection.json": showcase.qqq_options_projection.canonical_bytes,
        "qqq_options_projection_validation.json": (
            qqq_options_projection_validation_json_bytes(showcase.qqq_options_projection_validation)
        ),
        "responses.json": cited_query_responses_json_bytes(showcase.responses),
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
