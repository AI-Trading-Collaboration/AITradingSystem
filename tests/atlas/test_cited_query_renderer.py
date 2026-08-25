from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import timedelta
from functools import cache, lru_cache
from html import unescape
from pathlib import Path
from typing import Any, cast

import pytest

from ai_trading_system.atlas.cited_query_renderer import (
    AtlasCitedQueryShowcase,
    build_cited_query_showcase,
    render_cited_query_html,
    write_cited_query_artifacts,
)
from ai_trading_system.atlas.live_snapshot import build_live_snapshot_bundle
from ai_trading_system.atlas.page_effectiveness import repository_head
from ai_trading_system.atlas.reader_accessibility_validation import (
    validate_reader_accessibility,
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
from ai_trading_system.contracts.strategy_research_reader_projection import ReaderSectionId
from ai_trading_system.contracts.strategy_research_reader_terminology import (
    RenderedTermInventory,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@cache
def _payloads(
    *, injected_summary: str = ""
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    before = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=repository_head(PROJECT_ROOT),
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


@cache
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


@lru_cache(maxsize=1)
def _live_showcase() -> AtlasCitedQueryShowcase:
    bundle = build_live_snapshot_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=repository_head(PROJECT_ROOT),
    )
    return build_cited_query_showcase(
        target_ids=bundle.target_ids,
        snapshot_payload=bundle.current_snapshot.to_dict(),
        before_payload=bundle.comparison_snapshot.to_dict(),
        after_payload=bundle.current_snapshot.to_dict(),
        diff_payload=bundle.current_diff.to_dict(),
        repository_root=PROJECT_ROOT,
    )


def test_renderer_presents_five_reader_questions_and_lineage() -> None:
    showcase = _showcase()
    html = render_cited_query_html(showcase)
    rendered_text = unescape(re.sub(r"<[^>]+>", "", html)).replace("完整定义", "")
    for expected in (
        "这项策略研究为什么还不能继续",
        "在这个整体上下文下，本次研究为什么按现在的顺序推进",
        "我们真正要回答什么",
        "为什么选择当前研究路径",
        "现有证据只支持什么结论",
        "当前结果把下一步指向哪里",
        "这条研究主线在研究什么",
        "这项研究实际得到什么结果",
        "哪些因素解释了结果",
        "两个研究快照之间发生了什么变化",
        "这条信息来自哪里",
        "一句话回答",
        "先看限制",
        "完整引用与审计标识",
        "术语索引与完整定义",
        "日常阅读不需要先记住这张表",
        "授权已消费，运行结果无效且不完整",
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
        "绿色的“能力可用”“已验证”或页面验收通过",
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
        "这是 Atlas V1.3 的代表性主线加五份已审阅历史记录",
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
        "工程通过也不等于策略结论通过",
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
        assert expected in html or unescape(expected) in rendered_text
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
    assert html.count('data-reader-detail="why_needed"') == 8
    assert html.count('data-reader-detail="work_items"') == 8
    assert html.count('data-reader-detail="expected_outputs"') == 8
    assert html.count('data-reader-detail="progress_dimensions"') == 8
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
    assert 'data-task-coverage-count="72"' in html
    assert (
        'data-successor-task="TRADING-2509_QQQ_OPTIONS_OWNER_DECISION_SLOT_CATALOG_V2_AMENDMENT_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2510_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_EVIDENCE_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2511_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2512_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2513_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_OWNER_DECISION_PACK_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2514_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2515_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_V1"' in html
    )
    assert (
        'data-successor-task="TRADING-2516_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2517_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_AND_BOUNDED_COLLECTION_LIFECYCLE_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2518_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_AND_REAUTHORIZATION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_RESULT_FAILURE_ADMISSION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2521_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_REVALIDATION_EXECUTION_EVIDENCE_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2523_ATLAS_READER_FACING_TERMINOLOGY_FIRST_USE_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2527_ATLAS_HUMAN_COMPREHENSION_ACCEPTANCE_PILOT_V1"' in html
    )
    assert (
        'data-successor-task="TRADING-2528_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_DIAGNOSTIC_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2529_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_EXPORT_SAFE_AGGREGATE_COLLECTION_PROPOSAL_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2530_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_EXPORT_SAFE_AGGREGATE_COLLECTION_ADMISSION_AND_EXECUTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2531_QC_QQQ_OPTIONS_DAILY_TRANSPORT_SESSION_FINALIZATION_AND_UNDERLYING_PRICE_SOURCE_CONTRACT_FIX_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2532_QC_QQQ_OPTIONS_DAILY_TRANSPORT_SESSION_FINALIZATION_V2_ZERO_ORDER_EXTERNAL_VALIDATION_ADMISSION_AND_EXECUTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2533_QC_QQQ_OPTIONS_SESSION_FINALIZATION_V2_EXPORT_SAFE_DQ_PIT_EVIDENCE_ADMISSION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2534_QQQ_OPTIONS_STAGED_DQ_PIT_RESEARCH_READINESS_AND_EXPORT_SAFE_EVIDENCE_AUTHORITY_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2535_QC_QQQ_OPTIONS_FINAL_NEVER_CHAIN_SESSION_EXPORT_SAFE_PROVIDER_TRANSPORT_ATTRIBUTION_PROPOSAL_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2536_ATLAS_PROVIDER_TRANSPORT_ATTRIBUTION_SUCCESSOR_CLASSIFICATION_SERIAL_CONTRACT_WAVE_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2537_QC_QQQ_OPTIONS_EXACT_DATE_PROVIDER_CATALOG_ATTRIBUTION_CORRECTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2538_QC_QQQ_OPTIONS_EXACT_DATE_PROVIDER_CATALOG_ATTRIBUTION_ADMISSION_AND_EXECUTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2539_QC_CLOUD_FILE_API_EXACT_CONTENT_MUTATION_AND_RETRY_PROPOSAL_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2541_QC_QQQ_OPTIONS_EXACT_DATE_SUBSCRIPTION_MISSING_REMEDIATION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2542B_GROWTH_ACTION_VALUE_CANONICAL_DQ_PIT_SERIAL_CONTRACT_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2542C_GROWTH_ACTION_VALUE_INDEPENDENT_REVIEW_REMEDIATION_AND_FREEZE_READINESS_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2542D_GROWTH_ACTION_VALUE_DQ_PIT_AND_SAMPLE_SEMANTICS_FREEZE_CORRECTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2523A_ATLAS_READER_TERMINOLOGY_INTEGRATION_CORRECTION_V1"'
        in html
    )
    assert (
        'data-successor-task="TRADING-2523B_ATLAS_PAGE_EFFECTIVENESS_SERIAL_CONTRACT_WAVE_V1"'
        in html
    )
    assert "1201 个有 option chain 的 session 全部被组合 transport gate 拒绝" in html
    assert "2528 严格离线逐轴诊断合同已实现" in rendered_text
    assert "只能确认“期权链已出现”" in html
    assert "提案已准备，等待 Owner 最终授权令牌" in html
    assert "尚未执行任何外部动作" in html
    assert "真实一次零订单采集已完成" in html
    assert "只消除了“先有执行证据才能开始研究”的循环依赖" in html
    assert "没有提交 Cloud backtest 或 provider query" in html
    assert "只完成了离线 sealed package、exact hashes 与 replay/tamper 检查" in html
    assert "不能声称该 session 已获得真实归因" in html
    assert "external-collection-unexecuted 边界显式纳入 Atlas freshness 合同" in html
    assert "完整评估 2021-02-22..2025-12-02 的 1202 个 session" in html
    assert "exact-source-date provider probe 返回 1 个 record、6496 个 contracts" in html
    assert "缺口位于 subscription/transport" in html
    assert "V1 existing-clone run、错误 source-date 解释和历史 counters 保持 immutable" in html
    assert "已保留 active QQQ Options primary-window derived aggregate evidence lane" in html
    assert "已完成 pure same-date recovery adapter" in html
    assert "恢复 1 个 exact-date record、6496 个 contracts" in html
    assert "REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE" in html
    assert "V2 exact measurement contract" in html
    assert "只有 182 个看到 option chain，1020 个缺失" in html
    assert "不能据此宣称 DQ/PIT" in html
    assert "旧 collector 在当天第一条 Slice 没有 chain 时就提前把整天判为 missing" in html
    assert "整天合并、最后结算" in html
    assert "真实缺链数量仍需未来一次另行授权的零订单验证" in html
    assert "2532 的唯一零订单外部验证已完成并永久消费授权" in html
    assert "1201 个最终看到 option chain，仅 1 个全日未见" in html
    assert "1019 个是当天先出现无链 Slice、后来恢复的提前结算混淆" in html
    assert "当前：完成整体可信性与参数依据复核" in rendered_text
    assert "无需再次解释缺链日，也不授权新的外部平台运行" in rendered_text
    assert "2516 v2 token 也已签署并在唯一一次 Cloud run 尝试中消费" in html
    assert "9518360aeb329219cd83e78442a1d229" in html
    assert "Option filter 已以显式 list[Symbol] 完成 versioned failure-fix" in html
    assert "KEEP_CLOSED + PREREGISTRATION_ONLY" in html
    assert "Owner 尚未签署" in html
    assert html.count('data-reader-detail="downstream_use"') == 8
    assert html.count('data-reader-detail="boundary"') == 8
    assert html.count('data-reader-detail="next_trigger"') == 8
    assert html.count('class="concept-card"') == len(showcase.work_progress.concepts)
    assert html.count('data-concept-ref="') == sum(
        len(item.concept_ids) for item in showcase.work_progress.stage_records
    )
    assert html.count('class="reader-conclusion"') == 8
    assert html.count('class="reader-audit flat-disclosure"') == 8
    assert html.count('data-reader-detail="conclusion"') == 8
    assert html.count('data-reader-detail="current_work"') == 8
    assert html.count('data-reader-detail="completed"') == 8
    assert html.count('data-reader-detail="remaining_gaps"') == 8
    assert html.count('data-reader-detail="reader_impact"') == 8
    assert html.count('data-reader-detail="what_changes"') == 8
    assert html.count('data-reader-detail="owner_and_next"') == 8
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
        "唯一 scope violation 是 已处理数据点数量",
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
        'data-reader-detail="conclusion"',
        'data-reader-detail="current_work"',
        'data-reader-detail="completed"',
        'data-reader-detail="remaining_gaps"',
        'data-reader-detail="reader_impact"',
        'data-reader-detail="what_changes"',
        'data-reader-detail="owner_and_next"',
        'class="reader-audit flat-disclosure"',
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
    assert html.count("<script>") == 1
    assert "<form" not in html
    assert "<iframe" not in html


def test_renderer_follows_why_first_section_and_term_interaction_contract() -> None:
    showcase = _showcase()
    html = render_cited_query_html(showcase)
    expected_order = tuple(item.value for item in ReaderSectionId)
    positions = tuple(html.index(f'data-reader-section="{item}"') for item in expected_order)

    assert positions == tuple(sorted(positions))
    assert html.count('data-reader-section="') == len(expected_order)
    assert html.index('data-reader-section="CANONICAL_QUESTIONS"') < html.index(
        'data-reader-section="RESEARCH_DRILLDOWN"'
    )
    assert html.index('data-reader-section="WHY_CONTEXT"') < html.index(
        'id="reader-terminology-guide"'
    )
    assert html.count('<li class="causal-node" data-causal-node="') == 6
    assert html.count('data-causal-edge="') == 5
    system_context = html.index('data-reader-overview="SYSTEM_CONTEXT"')
    research_closure = html.index('data-reader-context="RESEARCH_CLOSURE"')
    local_chain = html.index('data-reader-context="CURRENT_LOCAL_CHAIN"')
    first_causal_node = html.index('<li class="causal-node" data-causal-node="')
    assert system_context < research_closure < local_chain < first_causal_node
    assert html.count('data-system-stage="') == 4
    assert "这项策略研究为什么还不能继续？" in html
    assert "系统怎样从想法走到行动" in html
    assert "当前决定、原因和下一步" in html
    assert html.count('class="reader-decision-card"') == 4
    assert 'class="reader-plain-flow"' in html
    assert html.count('class="local-research-explanation"') == 1
    assert "为什么策略研究之前会被关闭" not in html

    l0_start = html.index('data-reader-section="TRUST_STRIP"')
    l1_start = html.index('<details class="local-research-explanation"')
    l0_html = html[l0_start:l1_start]
    l0_text = unescape(re.sub(r"<[^>]+>", " ", l0_html))
    for decision_kind in ("CURRENT_DECISION", "WHY_PAUSED", "CURRENT_WORK", "NEXT_STEP"):
        assert l0_html.count(f'data-reader-decision="{decision_kind}"') == 1
    assert "1201 个 normal session + 1 个 exact-date recovery" in l0_text
    assert "unresolved=0" in l0_text
    assert "合计 1202/1202" in l0_text
    assert "仍有 1 天全日未出现期权链" not in l0_text
    assert "先解释唯一缺链交易日" not in l0_text
    assert 'class="term-trigger"' not in l0_html
    for forbidden_term in (
        "主研究窗口",
        "关键证据",
        "准入门槛",
        "数据质量",
        "时点可得性",
        "来源准入",
        "可信证据",
        "检查轴",
        "严格离线诊断",
        "DQ/PIT",
        "admission",
        "axis",
        "G2",
        "G3",
        "TRADING-",
    ):
        assert forbidden_term not in l0_text
    assert "TRADING-2515_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_V1" in html
    assert 'data-term-first="true"' in html
    assert 'data-term-first="false"' in html
    assert 'class="term-full-link"' in html
    assert html.count('class="term-context"') == html.count('class="term-trigger"')
    assert html.count('class="term-popover"') == html.count('class="term-trigger"')
    assert 'data-term-placement="below"' in html
    assert "const positionTerm = (context) =>" in html
    assert "window.innerHeight - triggerRect.bottom" in html
    assert "--term-tooltip-left" in html
    assert "position:fixed; z-index:1001" in html
    assert ".term-trigger::after" not in html
    assert "title=" not in html

    accessibility = validate_reader_accessibility(html.encode("utf-8"))
    assert accessibility.status == "PASS", [item.to_dict() for item in accessibility.violations]
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
        reviewed_page_sha256="a" * 64,
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
    showcase = _live_showcase()
    first = write_cited_query_artifacts(showcase, tmp_path / "first")
    second = write_cited_query_artifacts(showcase, tmp_path / "second")
    assert tuple(Path(item.path).name for item in first) == (
        "comparison_snapshot.json",
        "current_diff.json",
        "current_snapshot.json",
        "index.html",
        "qqq_options_projection.json",
        "qqq_options_projection_validation.json",
        "responses.json",
        "reader_accessibility_validation.json",
        "reader_state.json",
        "reader_terminology_inventory.json",
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
        "comparison_snapshot.json",
        "current_diff.json",
        "current_snapshot.json",
        "index.html",
        "qqq_options_projection.json",
        "qqq_options_projection_validation.json",
        "responses.json",
        "reader_accessibility_validation.json",
        "reader_state.json",
        "reader_terminology_inventory.json",
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
    terminology_bytes = (tmp_path / "first" / "reader_terminology_inventory.json").read_bytes()
    terminology = RenderedTermInventory.from_json_bytes(terminology_bytes)
    html_identity = next(item for item in first if Path(item.path).name == "index.html")
    assert terminology.html_sha256 == html_identity.sha256
    accessibility_payload = json.loads(
        (tmp_path / "first" / "reader_accessibility_validation.json").read_text(encoding="utf-8")
    )
    assert accessibility_payload["html_sha256"] == html_identity.sha256
    assert accessibility_payload["status"] == "PASS"
    assert accessibility_payload["owner_visual_status"] == "PENDING_REVIEW"
    assert accessibility_payload["reader_comprehension_status"] == "PENDING_REVIEW"


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
