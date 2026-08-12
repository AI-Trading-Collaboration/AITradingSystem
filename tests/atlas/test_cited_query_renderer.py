from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from ai_trading_system.atlas.cited_query_renderer import (
    AtlasCitedQueryShowcase,
    build_cited_query_showcase,
    render_cited_query_html,
    write_cited_query_artifacts,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import build_snapshot_diff
from ai_trading_system.contracts import (
    CitedQueryQuestionId,
    StrategyResearchExplorerSnapshot,
    StrategyResearchStatusExplanationBundle,
    StrategyResearchWorkProgressBundle,
)
from ai_trading_system.contracts.strategy_research_page_effectiveness import (
    PageAcceptanceRecord,
    PageAcceptanceStatus,
    PageAcceptanceTrack,
)
from ai_trading_system.contracts.strategy_research_qqq_options_projection import (
    StrategyResearchQQQOptionsProjectionBundle,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _payloads(
    *, injected_summary: str = ""
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    before = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    selected_node_id = "program-strategy-research"
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " next",
        generated_at=before.generated_at + timedelta(days=1),
        sources=before.sources,
        nodes=tuple(
            (
                replace(
                    item,
                    summary=(injected_summary or item.summary + " 已增加引用式问答入口。"),
                )
                if item.node_id == selected_node_id
                else item
            )
            for item in before.nodes
        ),
        edges=before.edges,
        results=before.results,
        attributions=before.attributions,
    )
    diff = build_snapshot_diff(before, after)
    return (
        before.to_dict(),
        after.to_dict(),
        diff.to_dict(),
    )


def _showcase(
    *,
    injected_summary: str = "",
    page_engineering_status: PageAcceptanceStatus = PageAcceptanceStatus.NOT_EXECUTED,
    page_reader_comprehension_review: PageAcceptanceRecord | None = None,
) -> AtlasCitedQueryShowcase:
    before, after, diff = _payloads(injected_summary=injected_summary)
    return build_cited_query_showcase(
        target_ids={
            CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY: ("program-strategy-research"),
            CitedQueryQuestionId.RESULT_AND_STATUS: "result-restart-r2",
            CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS: ("attr-restart-oos-limits-expansion"),
            CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION: diff["changes"][0]["change_id"],
            CitedQueryQuestionId.SOURCE_LINEAGE: min(
                item["source_ref_id"] for item in after["sources"]
            ),
        },
        snapshot_payload=after,
        before_payload=before,
        after_payload=after,
        diff_payload=diff,
        page_engineering_status=page_engineering_status,
        page_engineering_evidence_refs=(
            (
                "docs/requirements/"
                "TRADING-2508_Atlas_Engineering_Research_Acceptance_Progress_Matrix_V1.md"
            ),
        )
        if page_engineering_status is PageAcceptanceStatus.PASS
        else (),
        page_reader_comprehension_review=page_reader_comprehension_review,
    )


def test_renderer_presents_five_reader_questions_and_lineage() -> None:
    showcase = _showcase()
    html = render_cited_query_html(showcase)
    for expected in (
        "研究主线、结果与归因，一眼看清",
        "这条研究主线在研究什么",
        "这项研究实际得到什么结果",
        "哪些因素解释了结果",
        "两个研究快照之间发生了什么变化",
        "这条信息来自哪里",
        "一句话回答",
        "先看限制",
        "完整引用与 lineage",
        "LIMITED 不等于研究失败",
        "先确认：这张页面现在还能不能信",
        "这张页面现在还能不能信",
        "QQQ Options 的 DAILY 工程合同与 G3 calibration evidence 合同已建立",
        "18 个 G3 slots 尚无 primary-window derived calibration evidence",
        "真实 Owner attestation 已按 canonical admission 合同封存",
        "37-slot v2 catalog、deterministic migration 与 typed evidence admission 已建立",
        "页面把工程能力、研究证据和页面验收分开汇总",
        "三种“通过”互不代签",
        "ENGINEERING_VALIDATION",
        "OWNER_VISUAL_REVIEW",
        "READER_COMPREHENSION_REVIEW",
        'data-page-freshness="CURRENT"',
        "策略系统全流程，以及你现在在哪里",
        "第 7 / 8 阶段",
        "当前实际关注路径",
        "当前研究关注路径",
        "由人工决定是否接受页面解释",
        "检查页面是否仍代表最新研究状态",
        "页面可靠性检查",
        "为什么需要这一步",
        "具体做什么",
        "目前进展：三种状态分开看",
        "预期产物",
        "完成后怎样被使用",
        "不能说明什么",
        "陌生概念可以继续解释，并能返回原流程节点",
        "工程、研究、页面验收分别看",
        "策略结论通过",
        "绿色的“能力可用”“已验证”或页面验收 PASS",
        "本页状态图例",
        "工程能力与研究证据请看上方矩阵",
        "本页未执行",
        "研究进行中",
        "证据有限",
        "已验证",
        "待人工复核",
        "状态审计附录",
        "供需要追溯的读者",
        "怎样展开",
        "展开读者说明",
        "收起读者说明",
        "状态限制摘要",
        "正在做什么",
        "已完成什么",
        "还缺什么",
        "为什么重要",
        "什么会改变当前状态",
        "由谁负责，以及下一步怎么读",
        "查看审计依据",
        "已有依据",
        "尚未登记",
        "不适用于本页",
        "先读普通语言结论；需要核对时再展开 exact citations 与 lineage",
        "当前覆盖范围内的全部研究结果",
        "RESULT LEDGER · CANONICAL SNAPSHOT ONLY",
        "这是 Atlas V1.3 的代表性主线 + 五份已审阅历史记录",
        "不是全仓历史研究的完整清单",
        "coverage_scope=ATLAS_V1_3_REPRESENTATIVE_PLUS_REVIEWED_HISTORY",
        "historical_repository_coverage_complete=false",
        "机器原始状态",
        "读者展示状态",
        "全部关联归因",
        "历史权重研究支线已经纳入证据地图，但不是当前关注",
        "历史材料 · 非当前结论",
        "来源原始状态",
        "为什么这样映射",
        "工程 PASS 也不等于 strategy PASS",
        "NODE_RAW_STATUS",
        "RESULT_DISPLAY_STATUS",
        "CANONICAL_NODE",
        "CANONICAL_RESULT",
        "INDEPENDENT_VALIDATION",
        "INDEPENDENT_VALIDATION_SET",
        "PAGE_EXECUTION_BOUNDARY",
        "OWNER_REVIEW_POLICY",
        "不是 DQ FAIL",
        "不会运行",
        "production_effect",
        "QQQ 期权研究链：做到哪里、还缺什么",
        "当前总判定",
        "暂不继续",
        "NO_GO_KEEP_BLOCKED",
        "可以确认",
        "不能推出",
        "当前关键原因",
        "A / B / C 不是成绩",
        "工程合同、机械与检查工具持续完善",
        "策略有效、收益稳健或具备部署条件",
        "试点超限，Owner 已签署 aggregate NO-GO",
        "研究底座与输入合同",
        "已实现但未授权的策略机械",
        "证据收集、对账与跨层检查",
        "唯一外部试点与治理总判定",
        "已经做到",
        "仍不能证明",
        "为什么停在这里",
        "接下来要看什么",
        "必须按这个顺序理解",
        "734127 &gt; 250000",
        "1 order / 1 fill",
        "SOURCE_STATUS_MISMATCH_REVIEW_REQUIRED",
        "查看五层状态与 exact source",
        "Policy 准备度",
    ):
        assert expected in html
    for stage_id in (
        "DATA_INPUTS",
        "DATA_QUALITY_GATE",
        "RESEARCH_MAINLINE",
        "BACKTEST_AND_EVALUATION",
        "RESULT_ATTRIBUTION",
        "ATLAS_SNAPSHOT_DIFF",
        "CITATION_FIRST_QUERY",
        "OWNER_DECISION_BOUNDARY",
    ):
        assert f'data-stage="{stage_id}"' in html
    for response in showcase.responses:
        assert response.request.target_id in html
    assert html.count('class="flow-stage ') == 8
    assert html.count('class="flow-stage-shell"') == 8
    assert html.count('data-drilldown-stage="') == 8
    assert html.count('class="stage-drilldown"') == 8
    assert html.count('class="reader-explanation"') == 8
    assert html.count('class="work-progress-reader"') == 8
    assert html.count('data-reader-section="why_needed"') == 8
    assert html.count('data-reader-section="work_items"') == 8
    assert html.count('data-reader-section="expected_outputs"') == 8
    assert html.count('data-reader-section="progress_dimensions"') == 8
    assert html.count('data-progress-dimension="') == 24
    assert html.count('data-stage-axis="') == 16
    assert html.count('data-stage-axis="capability"') == 8
    assert html.count('data-stage-axis="research"') == 8
    assert html.count('data-stage-axis-value="AVAILABLE"') == 4
    assert html.count('data-stage-axis-value="IN_PROGRESS"') == 2
    assert html.count('data-stage-axis-value="NOT_APPLICABLE"') == 2
    assert html.count('data-stage-axis-value="NO_NEW_RESEARCH_EVIDENCE"') == 4
    assert html.count('data-stage-axis-value="LIMITED_RESEARCH_EVIDENCE"') == 3
    assert html.count('data-stage-axis-value="OWNER_DECISION_ONLY"') == 1
    assert html.count('data-progress-matrix="') == 3
    assert html.count('data-matrix-axis="capability"') == 4
    assert html.count('data-matrix-axis="research"') == 3
    assert html.count('data-matrix-axis="page_acceptance"') == 3
    assert 'data-progress-stage-count="8"' in html
    assert 'data-page-acceptance-pass-count="0"' in html
    assert 'data-strategy-conclusion-pass-count="0"' in html
    assert 'data-task-coverage-count="29"' in html
    assert (
        'data-successor-task="TRADING-2509_QQQ_OPTIONS_OWNER_DECISION_SLOT_CATALOG_V2_AMENDMENT_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2510_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_EVIDENCE_V1"'
        in html
    )
    assert html.count('data-reader-section="downstream_use"') == 8
    assert html.count('data-reader-section="boundary"') == 8
    assert html.count('data-reader-section="next_trigger"') == 8
    assert html.count('class="concept-card"') == len(showcase.work_progress.concepts)
    assert html.count('data-concept-ref="') == sum(
        len(item.concept_ids) for item in showcase.work_progress.stage_records
    )
    assert html.count('class="reader-conclusion"') == 8
    assert html.count('class="reader-audit"') == 8
    assert html.count('data-reader-section="conclusion"') == 8
    assert html.count('data-reader-section="current_work"') == 8
    assert html.count('data-reader-section="completed"') == 8
    assert html.count('data-reader-section="remaining_gaps"') == 8
    assert html.count('data-reader-section="reader_impact"') == 8
    assert html.count('data-reader-section="what_changes"') == 8
    assert html.count('data-reader-section="owner_and_next"') == 8
    assert html.count('aria-current="step"') == 1
    assert html.count(' open aria-current="step"') == 1
    assert (
        '<details class="flow-stage flow-current" '
        'data-stage="CITATION_FIRST_QUERY" '
        'data-progress-status="VALIDATED" '
        'data-drilldown-stage="CITATION_FIRST_QUERY" open aria-current="step">'
    ) in html
    assert html.count('data-progress-status="NOT_EXECUTED_BY_PAGE"') == 2
    assert html.count('data-progress-status="RUNNING"') == 1
    assert html.count('data-progress-status="LIMITED"') == 2
    assert html.count('data-progress-status="VALIDATED"') == 2
    assert html.count('data-progress-status="PENDING_OWNER_REVIEW"') == 1
    assert html.count('class="provenance-item"') == 8
    assert html.count('data-provenance-stage="') == 8
    assert (
        'data-provenance-stage="RESEARCH_MAINLINE" data-provenance-source="NODE_RAW_STATUS"'
    ) in html
    assert "当前受审阅记录没有把 RUNNING 拆解为一个具体正在执行的研究子任务" in html
    assert "真实样本外、event-risk 与 forward maturity 证据仍不足" in html
    assert "attr-restart-oos-limits-expansion" in html
    assert html.count('class="qqq-group ') == 4
    assert html.count('data-qqq-task="') == 13
    assert html.count('data-qqq-layer="A"') == 5
    assert html.count('data-qqq-layer="B"') == 4
    assert html.count('data-qqq-layer="C"') == 4
    assert html.count('data-strategy-conclusion="PASS"') == 0
    for projected_task_number in range(2481, 2494):
        assert f'data-qqq-task="TRADING-{projected_task_number}"' in html
    pilot_card = html[
        html.index('data-qqq-task="TRADING-2492"') : html.index(
            "</div></details>", html.index('data-qqq-task="TRADING-2492"')
        )
    ]
    pilot_order = (
        "PILOT_NO_GO_LICENSE_OR_EVIDENCE",
        "唯一 scope violation 是 PROCESSED_DATA_POINTS",
        "734127 &gt; 250000",
        "1 order / 1 fill",
    )
    assert tuple(pilot_card.index(item) for item in pilot_order) == tuple(
        sorted(pilot_card.index(item) for item in pilot_order)
    )
    signoff_card = html[
        html.index('data-qqq-task="TRADING-2493"') : html.index(
            "</div></details>", html.index('data-qqq-task="TRADING-2493"')
        )
    ]
    assert signoff_card.index("NO_GO_KEEP_BLOCKED") < signoff_card.index("CONDITIONAL_GO")
    for legacy_primary_label in (
        "状态为什么是这样",
        "可以确认",
        "不能推出",
        "下一合法动作",
    ):
        assert f"<h4>{legacy_primary_label}</h4>" not in html
    first_reader_card = html[
        html.index('<div class="reader-explanation">') : html.index(
            "</div></details></div>", html.index('<div class="reader-explanation">')
        )
    ]
    reader_order = (
        'data-reader-section="conclusion"',
        'data-reader-section="current_work"',
        'data-reader-section="completed"',
        'data-reader-section="remaining_gaps"',
        'data-reader-section="reader_impact"',
        'data-reader-section="what_changes"',
        'data-reader-section="owner_and_next"',
        'class="reader-audit"',
    )
    assert tuple(first_reader_card.index(item) for item in reader_order) == tuple(
        sorted(first_reader_card.index(item) for item in reader_order)
    )
    results = cast(list[dict[str, Any]], showcase.snapshot_payload["results"])
    attributions = cast(list[dict[str, Any]], showcase.snapshot_payload["attributions"])
    assert html.count('data-historical-record="') == len(results) == 13
    assert html.count('data-historical-record="true"') == 5
    assert html.count('data-historical-result-id="') == 5
    assert html.count('class="result-attribution"') == len(attributions) == 17
    assert html.count('data-display-status="PASS"') == 2
    assert html.count('data-display-status="LIMITED"') == 9
    assert html.count('data-display-status="BLOCKED"') == 1
    assert html.count('data-display-status="NOT_DUE"') == 1
    for result in results:
        assert f'data-result-id="{result["result_id"]}"' in html
        for source_ref_id in result["source_ref_ids"]:
            assert source_ref_id in html
    for attribution in attributions:
        assert f'data-attribution-id="{attribution["attribution_id"]}"' in html
    for progress_tone in (
        "progress-neutral",
        "progress-active",
        "progress-limited",
        "progress-validated",
        "progress-review",
    ):
        assert progress_tone in html
    assert "<script" not in html
    assert "<form" not in html
    assert "<iframe" not in html
    assert "http://" not in html
    assert "https://" not in html
    assert 'lang="zh-CN"' in html
    assert len(showcase.responses) == 5
    assert all(item.status == "PASS" for item in showcase.validations)


def test_progress_matrix_uses_independent_page_acceptance_facts() -> None:
    reader_review = PageAcceptanceRecord(
        track=PageAcceptanceTrack.READER_COMPREHENSION_REVIEW,
        status=PageAcceptanceStatus.PASS,
        evidence_refs=(
            "docs/requirements/TRADING-2506_Atlas_Work_Progress_Recursive_Explanation_V1.md",
        ),
        reviewer_id="project-owner",
        reviewed_at="2026-08-10T10:17:40Z",
        decision_id="trading-2506-reader-comprehension-pass-20260810-v1",
    )
    showcase = _showcase(
        page_engineering_status=PageAcceptanceStatus.PASS,
        page_reader_comprehension_review=reader_review,
    )

    html = render_cited_query_html(showcase)

    assert 'data-page-acceptance-pass-count="2"' in html
    assert html.count('class="progress-matrix-card-head"') == 3
    assert '<article class="progress-matrix-card"' in html
    assert (
        '<article class="progress-matrix-card" '
        'data-progress-matrix="capability"><header>' not in html
    )
    assert "2 / 3 已通过" in html
    assert html.count('data-matrix-axis="page_acceptance" data-matrix-value="PASS"') == 2
    assert html.count('data-matrix-axis="page_acceptance" data-matrix-value="PENDING_REVIEW"') == 1
    assert 'data-strategy-conclusion-pass-count="0"' in html


def test_flow_focus_fails_closed_on_duplicate_question_response() -> None:
    showcase = _showcase()
    invalid = replace(
        showcase,
        responses=(*showcase.responses[:-1], showcase.responses[0]),
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_FLOW_FOCUS_QUESTION_SET_INVALID",
    ):
        render_cited_query_html(invalid)


def test_flow_status_fails_closed_on_duplicate_validation_binding() -> None:
    showcase = _showcase()
    invalid = replace(
        showcase,
        validations=(*showcase.validations[:-1], showcase.validations[0]),
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_FLOW_STATUS_VALIDATION_SET_INVALID",
    ):
        render_cited_query_html(invalid)


def test_flow_status_fails_closed_on_unreviewed_research_state() -> None:
    showcase = _showcase()
    snapshot_nodes = cast(list[dict[str, Any]], showcase.snapshot_payload["nodes"])
    nodes = [
        {
            **item,
            "raw_status": (
                "PASS" if item["node_id"] == "program-strategy-research" else item["raw_status"]
            ),
        }
        for item in snapshot_nodes
    ]
    invalid = replace(
        showcase,
        snapshot_payload={**showcase.snapshot_payload, "nodes": nodes},
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_SNAPSHOT_CONTRACT_INVALID",
    ):
        render_cited_query_html(invalid)


def test_flow_status_fails_closed_on_attribution_result_mismatch() -> None:
    showcase = _showcase()
    snapshot_attributions = cast(list[dict[str, Any]], showcase.snapshot_payload["attributions"])
    attributions = [
        {
            **item,
            "result_id": (
                "different-result"
                if item["attribution_id"] == "attr-restart-oos-limits-expansion"
                else item["result_id"]
            ),
        }
        for item in snapshot_attributions
    ]
    invalid = replace(
        showcase,
        snapshot_payload={
            **showcase.snapshot_payload,
            "attributions": attributions,
        },
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_SNAPSHOT_CONTRACT_INVALID",
    ):
        render_cited_query_html(invalid)


def test_result_ledger_fails_closed_when_snapshot_relation_is_orphaned() -> None:
    showcase = _showcase()
    snapshot_results = cast(list[dict[str, Any]], showcase.snapshot_payload["results"])
    invalid = replace(
        showcase,
        snapshot_payload={
            **showcase.snapshot_payload,
            "results": [
                item for item in snapshot_results if item["result_id"] != "result-atlas-contract"
            ],
        },
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_SNAPSHOT_CONTRACT_INVALID",
    ):
        render_cited_query_html(invalid)


def test_renderer_escapes_claim_content() -> None:
    html = render_cited_query_html(_showcase(injected_summary="<script>alert('x')</script>"))
    assert "<script>alert" not in html
    assert "&lt;script&gt;alert" in html


def test_artifact_writer_is_byte_deterministic(tmp_path: Path) -> None:
    showcase = _showcase()
    first = write_cited_query_artifacts(showcase, tmp_path / "first")
    second = write_cited_query_artifacts(showcase, tmp_path / "second")
    assert tuple(Path(item.path).name for item in first) == (
        "index.html",
        "qqq_options_projection.json",
        "qqq_options_projection_validation.json",
        "responses.json",
        "status_explanation_validation.json",
        "status_explanations.json",
        "validation.json",
        "work_progress_explanation_validation.json",
        "work_progress_explanations.json",
        "page_effectiveness.json",
        "page_effectiveness_validation.json",
    )
    assert [item.sha256 for item in first] == [item.sha256 for item in second]
    assert [item.size_bytes for item in first] == [item.size_bytes for item in second]
    for name in (
        "index.html",
        "qqq_options_projection.json",
        "qqq_options_projection_validation.json",
        "responses.json",
        "status_explanation_validation.json",
        "status_explanations.json",
        "validation.json",
        "work_progress_explanation_validation.json",
        "work_progress_explanations.json",
        "page_effectiveness.json",
        "page_effectiveness_validation.json",
    ):
        assert (tmp_path / "first" / name).read_bytes() == (tmp_path / "second" / name).read_bytes()
    sidecar_bytes = (tmp_path / "first" / "status_explanations.json").read_bytes()
    assert sidecar_bytes == showcase.status_explanations.canonical_bytes
    replayed = StrategyResearchStatusExplanationBundle.from_json_bytes(sidecar_bytes)
    assert replayed.canonical_bytes == sidecar_bytes
    assert replayed.content_sha256 == showcase.status_explanations.content_sha256
    projection_bytes = (tmp_path / "first" / "qqq_options_projection.json").read_bytes()
    assert projection_bytes == showcase.qqq_options_projection.canonical_bytes
    projection = StrategyResearchQQQOptionsProjectionBundle.from_json_bytes(projection_bytes)
    assert projection.content_sha256 == showcase.qqq_options_projection.content_sha256
    progress_bytes = (tmp_path / "first" / "work_progress_explanations.json").read_bytes()
    assert progress_bytes == showcase.work_progress.canonical_bytes
    progress = StrategyResearchWorkProgressBundle.from_json_bytes(progress_bytes)
    assert progress.content_sha256 == showcase.work_progress.content_sha256


def test_renderer_rejects_status_explanation_validation_drift() -> None:
    showcase = _showcase()
    invalid = replace(
        showcase,
        status_explanation_validation=replace(
            showcase.status_explanation_validation,
            bundle_sha256="0" * 64,
        ),
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_STATUS_EXPLANATION_BINDING_INVALID",
    ):
        render_cited_query_html(invalid)


def test_renderer_rejects_qqq_options_projection_validation_drift() -> None:
    showcase = _showcase()
    invalid = replace(
        showcase,
        qqq_options_projection_validation=replace(
            showcase.qqq_options_projection_validation,
            bundle_sha256="0" * 64,
        ),
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_QQQ_OPTIONS_PROJECTION_BINDING_INVALID",
    ):
        render_cited_query_html(invalid)


def test_renderer_rejects_work_progress_validation_drift() -> None:
    showcase = _showcase()
    invalid = replace(
        showcase,
        work_progress_validation=replace(
            showcase.work_progress_validation,
            bundle_sha256="0" * 64,
        ),
    )
    with pytest.raises(
        ValueError,
        match="ATLAS_CITED_QUERY_WORK_PROGRESS_BINDING_INVALID",
    ):
        render_cited_query_html(invalid)
