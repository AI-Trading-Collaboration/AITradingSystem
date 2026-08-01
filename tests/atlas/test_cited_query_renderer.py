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


def _showcase(*, injected_summary: str = "") -> AtlasCitedQueryShowcase:
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
        "策略系统全流程，以及你现在在哪里",
        "第 7 / 8 阶段",
        "当前实际关注路径",
        "当前研究关注路径",
        "Owner 决策边界",
        "进展状态",
        "颜色表示节点在当前 evidence view 中的进展",
        "不代表策略 PASS 或投资评级",
        "本页未执行",
        "研究进行中",
        "证据有限",
        "已验证",
        "待人工复核",
        "状态依据台账",
        "为什么是这个状态",
        "怎样展开",
        "展开节点依据",
        "收起节点依据",
        "这个节点现在意味着什么",
        "状态为什么是这样",
        "可以确认",
        "不能推出",
        "下一合法动作",
        "不支持的问题必须通过独立任务扩展合同",
        "当前覆盖范围内的全部研究结果",
        "RESULT LEDGER · CANONICAL SNAPSHOT ONLY",
        "这是 Atlas V1.1 已接入的代表性 campaigns",
        "不是全仓历史研究的完整清单",
        "coverage_scope=ATLAS_V1_1_REPRESENTATIVE_CAMPAIGNS",
        "historical_repository_coverage_complete=false",
        "机器原始状态",
        "读者展示状态",
        "全部关联归因",
        "工程 PASS 也不等于 strategy PASS",
        "CANONICAL_SNAPSHOT_FIELD",
        "CANONICAL_SNAPSHOT_RELATION",
        "INDEPENDENT_VALIDATION",
        "INDEPENDENT_VALIDATION_SET",
        "PAGE_EXECUTION_BOUNDARY",
        "OWNER_REVIEW_POLICY",
        "不是 DQ FAIL",
        "不会运行",
        "production_effect",
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
    assert html.count('class="drilldown-grid"') == 8
    assert html.count('class="drilldown-evidence"') == 8
    assert html.count('aria-current="step"') == 1
    assert html.count(' open aria-current="step"') == 1
    assert (
        '<details class="flow-stage flow-current" '
        'data-stage="CITATION_FIRST_QUERY" '
        'data-progress-status="VALIDATED" '
        'data-drilldown-stage="CITATION_FIRST_QUERY" open aria-current="step">'
    ) in html
    assert html.count('data-progress-status="NOT_EXECUTED_BY_PAGE"') == 2
    assert html.count('data-progress-status="IN_PROGRESS"') == 1
    assert html.count('data-progress-status="LIMITED"') == 2
    assert html.count('data-progress-status="VALIDATED"') == 2
    assert html.count('data-progress-status="PENDING_OWNER_REVIEW"') == 1
    assert html.count('class="provenance-item"') == 8
    assert html.count('data-provenance-stage="') == 8
    assert (
        'data-provenance-stage="RESEARCH_MAINLINE" '
        'data-provenance-source="CANONICAL_SNAPSHOT_FIELD"'
    ) in html
    assert "raw_status=RUNNING" in html
    assert "display_status=LIMITED" in html
    assert "attr-restart-oos-limits-expansion" in html
    for response in showcase.responses:
        assert f"response_id={response.response_id}" in html
    results = cast(list[dict[str, Any]], showcase.snapshot_payload["results"])
    attributions = cast(list[dict[str, Any]], showcase.snapshot_payload["attributions"])
    assert html.count('class="result-ledger-card"') == len(results) == 8
    assert html.count('class="result-attribution"') == len(attributions) == 12
    assert html.count('data-display-status="PASS"') == 2
    assert html.count('data-display-status="LIMITED"') == 4
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
        match="ATLAS_CITED_QUERY_FLOW_STATUS_RESEARCH_STATE_INVALID:PASS",
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
        match="ATLAS_CITED_QUERY_FLOW_STATUS_ATTRIBUTION_RESULT_MISMATCH",
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
    assert [item.sha256 for item in first] == [item.sha256 for item in second]
    assert [item.size_bytes for item in first] == [item.size_bytes for item in second]
    for name in ("index.html", "responses.json", "validation.json"):
        assert (tmp_path / "first" / name).read_bytes() == (tmp_path / "second" / name).read_bytes()
