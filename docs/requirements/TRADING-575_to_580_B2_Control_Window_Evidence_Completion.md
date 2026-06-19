# TRADING-575 to 580 B2 Control Window Evidence Completion

最后更新：2026-06-19

## 背景

TRADING-565~574 已完成 B2 full diagnostic completion attempt：

- TRADING-567：`B2_FULL_DIAGNOSTIC_PARTIAL`
- TRADING-572：`B2_ONLY_NEEDS_MORE_EVIDENCE`
- TRADING-573：`B3_SIGNAL_PRECHECK_RESOLUTION_READY`
- TRADING-574：`CONTINUE_B2_ONLY_RESEARCH`
- `B4_retest_allowed=false`
- `b5_allowed=false`
- `b6_allowed=false`
- `v3_allowed=false`

partial 原因：normal / calm control windows 当前只有 normal-market no-trigger reference，尚无独立 B2 control-window signal rerun artifact。因此 B2 full diagnostic 不能标记为 complete。

## 安全边界

本批次固定为 research-only / manual-review-only：

- 不访问 untouched holdout；
- 不调 B2 threshold 或参数；
- 只运行 B2 risk overlay logic；
- 不使用 B3 slow tilt；
- 不运行 B4/B5/B6/v3；
- 不激活 paper-shadow / extended shadow / live trading；
- 不生成 official target weights；
- 不触发 broker/order；
- 不修改 production state；
- 不自动 append owner decision。

所有输出必须披露 `market_regime=ai_after_chatgpt`、requested date range、data quality gate、source artifacts、Reader Brief 和 safety boundary。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-575|B2 control-window rerun contract|VALIDATING|
|TRADING-576|B2 calm/normal control-window rerun|VALIDATING|
|TRADING-577|B2 no-trigger correctness review|VALIDATING|
|TRADING-578|Rerun B2 full diagnostic with control windows|VALIDATING|
|TRADING-579|B2-only research gate v3|VALIDATING|
|TRADING-580|B2 path decision snapshot|VALIDATING|

## 必需输出

- `b2_control_window_rerun_contract.json/md`
- `b2_control_window_rerun.json/md`
- `b2_no_trigger_correctness_review.json/md`
- `b2_full_diagnostic_with_control_windows.json/md`
- `b2_only_research_gate_v3.json/md`
- `b2_path_decision_snapshot.json/md`

## 验收标准

- TRADING-575 必须定义 normal / calm control windows、expected B2 behavior、required outputs、independence requirement、validation 和 Reader Brief。
- TRADING-576 必须生成 per-window independent B2 rerun artifacts；只使用 B2 risk overlay logic，不使用 B3/B4/B5/B6/v3，不访问 holdout，并记录 B2 是否保持 inactive / minimally active。
- TRADING-577 必须评价 no-trigger correctness、false risk-off count、unnecessary exposure reduction、calm-window turnover、missed normal-uptrend exposure、benchmark opportunity cost 和 low-risk signal stability。
- TRADING-578 只有在 risk-heavy evidence 和 calm/normal control evidence 都存在且 validated 时才能输出 `B2_FULL_DIAGNOSTIC_COMPLETE`。
- TRADING-579 必须在完整诊断后重新运行 B2-only gate v3；PROMISING 必须满足 full diagnostic complete、drawdown protection not weak、no-trigger correctness not high-risk、false risk-off acceptable、re-entry lag acceptable、utility not weak、signal robustness not blocked、no holdout used。
- TRADING-580 必须输出 B2 full diagnostic status、B2 gate status、B3 signal status、B4/B5/B6/v3 allowed flags 和 next recommended path；B4 retest 必须依赖 valid B3，B5 必须依赖 valid non-redundant B4，B6 必须依赖 valid B5。

## 状态记录

- 2026-06-19：新增本批次，原因：owner 要求在 TRADING-565~574 后补齐 B2 control-window independent rerun evidence，使 B2 full diagnostic 可以从 partial 进入 complete 或继续说明 blocker；当前不得进入 B3/B4/B5/B6/v3，B4/B5/B6/v3 allowed flags 必须保持 false。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-control-window-research` 输出 TRADING-575=`B2_CONTROL_WINDOW_RERUN_CONTRACT_READY`、576=`B2_CONTROL_RERUN_COMPLETE`、577=`B2_NO_TRIGGER_CORRECTNESS_PASS`、578=`B2_FULL_DIAGNOSTIC_COMPLETE`、579=`B2_ONLY_NEEDS_MORE_EVIDENCE`、580=`CONTINUE_B2_ONLY_RESEARCH`。control rerun 覆盖 normal uptrend control 和 calm market control 两个窗口，均为 independent B2 rerun artifact；aggregate `trigger_count=0`、`false_risk_off_count=0`、`unnecessary_exposure_reduction_count=0`。完整诊断已补齐 control evidence，但 research gate 仍因 re-entry lag、drawdown/utility mixed 和 trigger stability weak 保持 more-evidence；`B4_retest_allowed=false`、`b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`。
