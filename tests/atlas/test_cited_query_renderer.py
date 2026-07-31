from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from ai_trading_system.atlas.cited_query_renderer import (
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


def _payloads(*, injected_summary: str = ""):
    before = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    selected_node_id = min(item.node_id for item in before.nodes)
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " next",
        generated_at=before.generated_at + timedelta(days=1),
        sources=before.sources,
        nodes=tuple(
            replace(
                item,
                summary=(
                    injected_summary
                    or item.summary + " 已增加引用式问答入口。"
                ),
            )
            if item.node_id == selected_node_id
            else item
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


def _showcase(*, injected_summary: str = ""):
    before, after, diff = _payloads(injected_summary=injected_summary)
    return build_cited_query_showcase(
        target_ids={
            CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY: min(
                item["node_id"]
                for item in after["nodes"]
            ),
            CitedQueryQuestionId.RESULT_AND_STATUS: min(
                item["result_id"]
                for item in after["results"]
            ),
            CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS: min(
                item["attribution_id"]
                for item in after["attributions"]
            ),
            CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION: diff["changes"][0][
                "change_id"
            ],
            CitedQueryQuestionId.SOURCE_LINEAGE: min(
                item["source_ref_id"]
                for item in after["sources"]
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
    assert html.count('aria-current="step"') == 1
    assert "<script" not in html
    assert "<form" not in html
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


def test_renderer_escapes_claim_content() -> None:
    html = render_cited_query_html(
        _showcase(injected_summary="<script>alert('x')</script>")
    )
    assert "<script>alert" not in html
    assert "&lt;script&gt;alert" in html


def test_artifact_writer_is_byte_deterministic(tmp_path: Path) -> None:
    showcase = _showcase()
    first = write_cited_query_artifacts(showcase, tmp_path / "first")
    second = write_cited_query_artifacts(showcase, tmp_path / "second")
    assert [item.sha256 for item in first] == [item.sha256 for item in second]
    assert [item.size_bytes for item in first] == [item.size_bytes for item in second]
    for name in ("index.html", "responses.json", "validation.json"):
        assert (tmp_path / "first" / name).read_bytes() == (
            tmp_path / "second" / name
        ).read_bytes()
