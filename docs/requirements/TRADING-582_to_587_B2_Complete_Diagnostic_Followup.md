# TRADING-582 to 587 B2 Complete Diagnostic Follow-up

最后更新：2026-06-19

## 背景

TRADING-575~580 已补齐 B2 calm / normal control-window independent rerun evidence：

- TRADING-575：`B2_CONTROL_WINDOW_RERUN_CONTRACT_READY`
- TRADING-576：`B2_CONTROL_RERUN_COMPLETE`
- TRADING-577：`B2_NO_TRIGGER_CORRECTNESS_PASS`
- TRADING-578：`B2_FULL_DIAGNOSTIC_COMPLETE`
- TRADING-579：`B2_ONLY_NEEDS_MORE_EVIDENCE`
- TRADING-580：`CONTINUE_B2_ONLY_RESEARCH`

当前解释从“B2 evidence 是否结构性不完整”转为“完整 evidence 为什么仍不足以支持 `B2_ONLY_RESEARCH_PROMISING`”。本批次只做诊断、归因和下一证据计划，不改变 B2 logic 或 threshold。

## 安全边界

- research-only / manual-review-only；
- 不访问 untouched holdout；
- 不调 B2 threshold 或参数；
- 不使用 B3 slow tilt；
- 不运行 B3/B4/B5/B6/v3；
- 不激活 paper-shadow / extended shadow / live trading；
- 不生成 official target weights；
- 不触发 broker/order；
- 不修改 production state；
- 不自动 append owner decision。

所有输出必须披露 `market_regime=ai_after_chatgpt`、requested date range、data quality gate、source artifacts、Reader Brief 和 safety boundary。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-582|B2 needs-more-evidence root cause drilldown|VALIDATING|
|TRADING-583|B2 per-window utility scorecard|VALIDATING|
|TRADING-584|B2 trigger and re-entry design assessment|VALIDATING|
|TRADING-585|B2 next evidence plan|VALIDATING|
|TRADING-586|B2 gate v4 decision|VALIDATING|
|TRADING-587|B2 research branch snapshot|VALIDATING|

## 必需输出

- `b2_needs_more_evidence_root_cause_drilldown.json/md`
- `b2_per_window_utility_scorecard.json/md`
- `b2_trigger_reentry_design_assessment.json/md`
- `b2_next_evidence_plan.json/md`
- `b2_gate_v4_decision.json/md`
- `b2_research_branch_snapshot.json/md`

## 验收标准

- TRADING-582 必须解释 full diagnostic complete 后仍 `B2_ONLY_NEEDS_MORE_EVIDENCE` 的 root cause，并分类 sample size、mixed protection、trigger coverage、re-entry cost、V-shaped opportunity cost、utility、signal robustness、window dispersion 和 no-structural-blocker evidence。
- TRADING-583 必须输出 risk-heavy 与 control windows 的 per-window scorecard，包含 expected/actual behavior、return/drawdown/turnover/cost/benchmark delta、false risk-off、re-entry lag、missed rebound proxy、window-level utility 和 pass/mixed/fail classification。
- TRADING-584 必须逐项回答 trigger timing、false risk-off、re-entry lag、V-shaped recovery、shallow false alarm、threshold sensitivity 和 re-entry conservatism；不得调参。
- TRADING-585 必须定义 additional windows、trigger events、control windows、V-shaped cases、false-risk-off cases、required metrics、kill criteria、promotion criteria 和 estimated minimum evidence count。
- TRADING-586 必须使用 582~585 输出 B2 gate v4；`B2_ONLY_RESEARCH_PROMISING` 必须满足 drawdown protection clear、re-entry cost acceptable、utility acceptable 和 trigger stable。
- TRADING-587 必须输出 B2 full diagnostic status、B2 gate v4 status、next evidence plan、B3 status、B4/B5/B6/v3/paper-shadow allowed flags 和 next recommended research action；B4 retest 仍必须依赖 valid B3，B5 依赖 valid non-redundant B4，B6 依赖 valid B5。

## 状态记录

- 2026-06-19：新增本批次并进入 IN_PROGRESS，原因：TRADING-578 已为 `B2_FULL_DIAGNOSTIC_COMPLETE`，但 TRADING-579 仍为 `B2_ONLY_NEEDS_MORE_EVIDENCE`；需要把剩余 blocker 从结构性缺证据转为可执行的 root-cause、scorecard、design assessment 和 next evidence plan，且继续阻塞 B3/B4/B5/B6/v3 与 paper-shadow。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-followup-research` 输出 TRADING-582=`B2_NEEDS_MORE_EVIDENCE_BUT_NO_STRUCTURAL_BLOCKER`、583=`B2_WINDOW_UTILITY_MIXED`、584=`B2_DESIGN_ACCEPTABLE_NEEDS_MORE_EVIDENCE`、585=`RUN_MORE_B2_RISK_WINDOWS`、586=`B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN`、587=`CONTINUE_B2_ONLY_WITH_TARGETED_EVIDENCE`。结论：B2 full diagnostic complete 且 control behavior clean，但 positive evidence 集中在 slow_drawdown，rapid_drawdown / volatility_spike 未触发，re-entry lag 高，utility mixed；下一步只允许 targeted B2 evidence plan，`B4_retest_allowed=false`、`b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`、`paper_shadow_allowed=false`。
