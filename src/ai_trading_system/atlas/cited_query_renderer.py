from __future__ import annotations

# HTML/CSS source lines remain readable as one semantic declaration.
# ruff: noqa: E501
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
from ai_trading_system.contracts.strategy_research_cited_query import (
    CITED_QUERY_QUESTION_CATALOG,
    CitedQueryAnswerStatus,
    CitedQueryQuestionId,
    StrategyResearchCitedQueryRequest,
    StrategyResearchCitedQueryResponse,
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
) -> AtlasCitedQueryShowcase:
    load_validated_snapshot_payload(snapshot_payload)
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
            target_id=target_ids[
                CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS
            ],
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
                "ATLAS_CITED_QUERY_SHOWCASE_VALIDATION_FAILED:"
                + ",".join(validation.errors)
            )
        validations.append(validation)
    return AtlasCitedQueryShowcase(
        responses=ordered,
        validations=tuple(validations),
        snapshot_payload=snapshot_payload,
        before_payload=before_payload,
        after_payload=after_payload,
        diff_payload=diff_payload,
    )


def _render_citation(citation: object) -> str:
    known_at = (
        "未记录"
        if citation.known_at is None
        else citation.known_at.isoformat()
    )
    available_at = (
        "未记录"
        if citation.available_at is None
        else citation.available_at.isoformat()
    )
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
        f'<p><strong>{escape(citation.source_path)}</strong></p>'
        f'<p>source_ref <code>{escape(citation.source_ref_id)}</code><br>'
        f'commit <code>{escape(citation.exact_commit)}</code><br>'
        f'SHA-256 <code>{escape(citation.source_sha256)}</code></p>'
        f'<p>as_of <code>{escape(citation.as_of.isoformat())}</code><br>'
        f'known_at <code>{escape(known_at)}</code><br>'
        f'available_at <code>{escape(available_at)}</code></p>'
        f'<p>{entity_hashes}</p>'
        "</li>"
    )


def _render_response(response: StrategyResearchCitedQueryResponse) -> str:
    prompt = _QUESTION_PROMPTS[response.request.question_id]
    claims = "".join(
        f'<p class="claim">{escape(claim.text_zh)}</p>' for claim in response.claims
    ) or '<p class="claim muted">没有生成未经引用的 claim。</p>'
    limitations = (
        "<ul>"
        + "".join(f"<li>{escape(item)}</li>" for item in response.limitations)
        + "</ul>"
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
        <ul class="citations">{citations or '<li>没有通过引用闭包的证据。</li>'}</ul>
      </details>
      <p class="identity">response <code>{escape(response.response_id)}</code> · request <code>{escape(response.request.request_id)}</code></p>
    </article>
    """


def _render_system_flow_map(showcase: AtlasCitedQueryShowcase) -> str:
    by_question = {
        item.request.question_id: item for item in showcase.responses
    }
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
            f'<span><strong>{escape(label)}</strong>'
            f'<small>{escape(stage)}</small></span>'
            f'<code>{escape(response.request.target_id)}</code>'
            f'<span class="focus-status">'
            f'{escape(_STATUS_LABELS[response.answer_status])}</span>'
            "</li>"
        )
        for label, stage, response in focus_rows
    )
    stages = (
        (
            "DATA_INPUTS",
            "数据与治理输入",
            "市场、宏观、基本面与人工治理信息。",
            "flow-context",
            "上游上下文",
            "NOT_EXECUTED_BY_PAGE",
            "本页未执行",
            "progress-neutral",
        ),
        (
            "DATA_QUALITY_GATE",
            "数据质量门",
            "Schema、完整性、新鲜度、PIT 与 validate-data。",
            "flow-context",
            "上游上下文",
            "NOT_EXECUTED_BY_PAGE",
            "本页未执行",
            "progress-neutral",
        ),
        (
            "RESEARCH_MAINLINE",
            "研究主线",
            "研究问题、策略路径与候选方法。",
            "flow-focus",
            "当前研究关注",
            "IN_PROGRESS",
            "研究进行中",
            "progress-active",
        ),
        (
            "BACKTEST_AND_EVALUATION",
            "回测与评估",
            "Primary window、OOS、stress 与结果状态。",
            "flow-focus",
            "当前研究关注",
            "LIMITED",
            "证据有限",
            "progress-limited",
        ),
        (
            "RESULT_ATTRIBUTION",
            "结果归因",
            "实际结果、驱动因素、限制与失败原因。",
            "flow-focus",
            "当前研究关注",
            "LIMITED",
            "证据有限",
            "progress-limited",
        ),
        (
            "ATLAS_SNAPSHOT_DIFF",
            "Atlas 快照与变化",
            "Validated snapshot、diff、source lineage。",
            "flow-focus",
            "当前研究关注",
            "VALIDATED",
            "已验证",
            "progress-validated",
        ),
        (
            "CITATION_FIRST_QUERY",
            "引用式问答页面",
            "五个固定问题、claim 与 citation closure。",
            "flow-current",
            "你在这里",
            "VALIDATED",
            "已验证",
            "progress-validated",
        ),
        (
            "OWNER_DECISION_BOUNDARY",
            "Owner 决策边界",
            "人工复核、后续任务或明确停止，不自动 promotion。",
            "flow-boundary",
            "本页以外",
            "PENDING_OWNER_REVIEW",
            "待人工复核",
            "progress-review",
        ),
    )
    stage_cards = "".join(
        (
            f'<li class="flow-stage {escape(role_tone)}" '
            f'data-stage="{escape(stage_id)}" '
            f'data-progress-status="{escape(progress_code)}"'
            + (' aria-current="step"' if role_tone == "flow-current" else "")
            + ">"
            '<div class="stage-top">'
            f'<span class="stage-number">{index:02d}</span>'
            f'<span class="stage-badge">{escape(role_badge)}</span>'
            "</div>"
            f"<h3>{escape(title)}</h3>"
            f"<p>{escape(description)}</p>"
            f'<code class="stage-id">{escape(stage_id)}</code>'
            f'<div class="stage-progress {escape(progress_tone)}">'
            '<span class="progress-dot" aria-hidden="true"></span>'
            f"<strong>{escape(progress_label)}</strong>"
            f"<code>{escape(progress_code)}</code>"
            "</div>"
            "</li>"
        )
        for index, (
            stage_id,
            title,
            description,
            role_tone,
            role_badge,
            progress_code,
            progress_label,
            progress_tone,
        ) in enumerate(stages, start=1)
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
      <div class="progress-key" aria-label="节点进展状态图例">
        <strong>进展状态</strong>
        <span class="progress-neutral"><i aria-hidden="true"></i>本页未执行</span>
        <span class="progress-active"><i aria-hidden="true"></i>研究进行中</span>
        <span class="progress-limited"><i aria-hidden="true"></i>证据有限</span>
        <span class="progress-validated"><i aria-hidden="true"></i>已验证</span>
        <span class="progress-review"><i aria-hidden="true"></i>待人工复核</span>
        <small>颜色表示节点在当前 evidence view 中的进展，不代表策略 PASS 或投资评级。</small>
      </div>
      <ol class="system-flow">{stage_cards}</ol>
      <div class="focus-panel">
        <div class="focus-copy">
          <p class="section-kicker">CURRENT FOCUS · EXACT IDS</p>
          <h3>当前实际关注路径</h3>
          <p>这些节点直接来自本页五个 canonical requests。没有“最相关”排序、模糊匹配或名称推断。</p>
        </div>
        <ul class="focus-ledger">{focus_ledger}</ul>
      </div>
      <p class="flow-safety"><strong>边界：</strong>本页只读取已验证的 Atlas snapshot/diff 并展示引用；不会运行 <code>aits validate-data</code>、回测、promotion、production 或 broker action。</p>
    </section>
    """


def render_cited_query_html(showcase: AtlasCitedQueryShowcase) -> str:
    if len(showcase.responses) != len(CITED_QUERY_QUESTION_CATALOG):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_QUESTION_COUNT_INVALID")
    if any(item.status != "PASS" for item in showcase.validations):
        raise ValueError("ATLAS_CITED_QUERY_SHOWCASE_VALIDATION_NOT_PASS")
    answered = sum(
        item.answer_status is CitedQueryAnswerStatus.ANSWERED
        for item in showcase.responses
    )
    limited = sum(
        item.answer_status is CitedQueryAnswerStatus.LIMITED
        for item in showcase.responses
    )
    blocked = sum(
        item.answer_status is CitedQueryAnswerStatus.BLOCKED
        for item in showcase.responses
    )
    navigation = "".join(
        f'<a href="#{escape(item.request.question_id.value.lower())}">'
        f"{escape(_QUESTION_PROMPTS[item.request.question_id])}</a>"
        for item in showcase.responses
    )
    cards = "".join(_render_response(item) for item in showcase.responses)
    system_flow = _render_system_flow_map(showcase)
    snapshot_id = str(showcase.snapshot_payload["snapshot_id"])
    diff_id = str(showcase.diff_payload["diff_id"])
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    .system-flow {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); grid-template-areas:"s1 s2 s3 s4" "s8 s7 s6 s5"; gap:1.25rem .9rem; margin:0; padding:0; list-style:none; counter-reset:flow; }}
    .flow-stage {{ position:relative; display:flex; min-width:0; min-height:196px; padding:.82rem; border:1px solid var(--line); border-radius:.82rem; background:#f8fafc; flex-direction:column; }}
    .flow-stage:nth-child(1) {{ grid-area:s1; }} .flow-stage:nth-child(2) {{ grid-area:s2; }} .flow-stage:nth-child(3) {{ grid-area:s3; }} .flow-stage:nth-child(4) {{ grid-area:s4; }}
    .flow-stage:nth-child(5) {{ grid-area:s5; }} .flow-stage:nth-child(6) {{ grid-area:s6; }} .flow-stage:nth-child(7) {{ grid-area:s7; }} .flow-stage:nth-child(8) {{ grid-area:s8; }}
    .flow-stage::after {{ position:absolute; z-index:2; content:"→"; right:-.78rem; top:50%; width:.65rem; color:#95a0b2; font-size:1.1rem; font-weight:900; text-align:center; transform:translateY(-50%); }}
    .flow-stage:nth-child(4)::after {{ content:"↓"; right:50%; top:auto; bottom:-1.18rem; transform:translateX(50%); }}
    .flow-stage:nth-child(n+5):nth-child(-n+7)::after {{ content:"←"; right:auto; left:-.78rem; }}
    .flow-stage:nth-child(8)::after {{ display:none; }}
    .flow-stage h3 {{ margin:.55rem 0 .28rem; font-size:.96rem; line-height:1.25; }}
    .flow-stage p {{ margin:0 0 .55rem; color:var(--muted); font-size:.76rem; line-height:1.42; }}
    .flow-stage code {{ display:block; color:#7b8596; font-size:.64rem; }}
    .flow-stage .stage-id {{ margin-bottom:.65rem; }}
    .stage-top {{ display:flex; align-items:center; justify-content:space-between; gap:.4rem; }}
    .stage-number {{ color:var(--muted); font:850 .7rem/1 system-ui,sans-serif; letter-spacing:.08em; }}
    .stage-badge {{ padding:.15rem .4rem; border-radius:999px; color:#667287; background:#e9edf3; font-size:.62rem; font-weight:850; }}
    .stage-progress {{ display:grid; grid-template-columns:auto 1fr; align-items:center; gap:.08rem .35rem; margin-top:auto; padding:.43rem .5rem; border:1px solid currentColor; border-radius:.55rem; background:#fff; }}
    .stage-progress strong {{ font-size:.7rem; line-height:1.2; }}
    .stage-progress code {{ grid-column:2; color:currentColor; font-size:.56rem; line-height:1.2; overflow-wrap:anywhere; }}
    .flow-focus {{ border-color:#8bcfc4; background:var(--teal-soft); }}
    .flow-focus .stage-badge {{ color:#08665f; background:#ccebe5; }}
    .flow-current {{ color:#fff; border-color:var(--blue); background:linear-gradient(145deg,var(--blue),#234977); box-shadow:0 8px 20px #315fba2b; }}
    .flow-current p,.flow-current .stage-id,.flow-current .stage-number {{ color:#dce8ff; }}
    .flow-current .stage-badge {{ color:#173b70; background:#fff; }}
    .flow-current .stage-progress {{ background:#fff; }}
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
    .flow-safety {{ margin:1rem 0 0; padding:.75rem .9rem; border-left:4px solid var(--amber); border-radius:.45rem; color:#6e531b; background:#fff8e9; font-size:.78rem; }}
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
    @media (max-width:900px) {{ .flow-heading {{ display:block; }} .you-are-here {{ margin-top:1rem; }} .system-flow {{ grid-template-columns:repeat(2,minmax(0,1fr)); grid-template-areas:"s1 s2" "s4 s3" "s5 s6" "s8 s7"; }} .flow-stage::after,.flow-stage:nth-child(5)::after {{ content:"→"; right:-.78rem; left:auto; top:50%; bottom:auto; transform:translateY(-50%); }} .flow-stage:nth-child(2)::after,.flow-stage:nth-child(4)::after,.flow-stage:nth-child(6)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.2rem; transform:translateX(50%); }} .flow-stage:nth-child(3)::after,.flow-stage:nth-child(7)::after {{ content:"←"; right:auto; left:-.78rem; top:50%; bottom:auto; transform:translateY(-50%); }} .flow-stage:nth-child(8)::after {{ display:none; }} .focus-panel {{ grid-template-columns:1fr; }} nav {{ grid-template-columns:1fr 1fr; }} .citations {{ grid-template-columns:1fr; }} }}
    @media (max-width:620px) {{ .metrics,nav,.focus-ledger {{ grid-template-columns:1fr; }} .flow-map {{ padding:1rem; }} .system-flow {{ grid-template-columns:1fr; grid-template-areas:"s1" "s2" "s3" "s4" "s5" "s6" "s7" "s8"; gap:1.15rem; }} .flow-stage {{ min-height:0; }} .flow-stage::after,.flow-stage:nth-child(n+5):nth-child(-n+7)::after {{ content:"↓"; right:50%; left:auto; top:auto; bottom:-1.18rem; transform:translateX(50%); }} .flow-stage:nth-child(8)::after {{ display:none; }} .answer-head {{ display:block; }} .status {{ display:inline-block; margin-top:.7rem; }} }}
    @media print {{ body {{ background:#fff; }} nav {{ display:none; }} .answer-card {{ break-inside:avoid; box-shadow:none; }} }}
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
    {system_flow}
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
        "status": (
            "PASS" if all(item.status == "PASS" for item in validations) else "FAIL"
        ),
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


def write_cited_query_artifacts(
    showcase: AtlasCitedQueryShowcase,
    output_directory: Path,
) -> tuple[AtlasCitedQueryRenderedArtifact, ...]:
    if any(item.status != "PASS" for item in showcase.validations):
        raise ValueError("ATLAS_CITED_QUERY_ARTIFACT_VALIDATION_FAILED")
    output_directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "index.html": render_cited_query_html(showcase).encode("utf-8"),
        "responses.json": cited_query_responses_json_bytes(showcase.responses),
        "validation.json": cited_query_validations_json_bytes(
            showcase.validations
        ),
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
    return tuple(artifacts)


__all__ = [
    "AtlasCitedQueryRenderedArtifact",
    "AtlasCitedQueryShowcase",
    "build_cited_query_showcase",
    "cited_query_responses_json_bytes",
    "cited_query_validations_json_bytes",
    "render_cited_query_html",
    "write_cited_query_artifacts",
]
