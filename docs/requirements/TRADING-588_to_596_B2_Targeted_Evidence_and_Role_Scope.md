# TRADING-588 to 596 B2 Targeted Evidence and Role Scope

最后更新：2026-06-19

## 背景

TRADING-575~587 已完成 B2 full diagnostic、control-window independent rerun 和 follow-up decision plan：

- TRADING-578：`B2_FULL_DIAGNOSTIC_COMPLETE`
- TRADING-582：`B2_NEEDS_MORE_EVIDENCE_BUT_NO_STRUCTURAL_BLOCKER`
- TRADING-583：`B2_WINDOW_UTILITY_MIXED`
- TRADING-584：`B2_DESIGN_ACCEPTABLE_NEEDS_MORE_EVIDENCE`
- TRADING-585：`RUN_MORE_B2_RISK_WINDOWS`
- TRADING-586：`B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN`
- TRADING-587：`CONTINUE_B2_ONLY_WITH_TARGETED_EVIDENCE`

当前解释：B2 control behavior clean，未产生 calm / normal false risk-off；positive evidence 集中在 `slow_drawdown`；`rapid_drawdown` / `volatility_spike` 未触发；re-entry lag 高；utility mixed。下一步必须澄清 B2 是否仍可作为 general fast asymmetric risk overlay，还是只能作为候选 slow-drawdown overlay，或应返回设计。

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
|TRADING-588|B2 targeted evidence window lock|VALIDATING|
|TRADING-589|B2 rapid-drawdown / volatility-spike no-trigger audit|VALIDATING|
|TRADING-590|B2 slow-drawdown repeatability study|VALIDATING|
|TRADING-591|B2 re-entry lag root cause review|VALIDATING|
|TRADING-592|B2 role narrowing assessment|VALIDATING|
|TRADING-593|B2 targeted evidence backfill v2|VALIDATING|
|TRADING-594|B2 targeted evidence scorecard|VALIDATING|
|TRADING-595|B2 gate v5|VALIDATING|
|TRADING-596|B2 research branch snapshot v2|VALIDATING|

## 必需输出

- `b2_targeted_evidence_window_lock.json/md`
- `b2_fast_risk_no_trigger_audit.json/md`
- `b2_slow_drawdown_repeatability_study.json/md`
- `b2_reentry_lag_root_cause_review.json/md`
- `b2_role_narrowing_assessment.json/md`
- `b2_targeted_evidence_backfill_v2.json/md`
- `b2_targeted_evidence_scorecard.json/md`
- `b2_gate_v5.json/md`
- `b2_research_branch_snapshot_v2.json/md`

## 验收标准

- TRADING-588 必须按 slow drawdown repeatability、rapid drawdown、volatility spike、V-shaped recovery、false risk-off / shallow pullback、calm / normal control 组冻结 targeted diagnostic windows；未找到独立 repeatability window 时必须显式 `INCOMPLETE`，不得补造窗口或使用 holdout。
- TRADING-589 必须解释 rapid_drawdown / volatility_spike no-trigger：risk signal values before/during drawdown、threshold distance、expected scaler、actual trigger count、first trigger date、onset-to-trigger days、threshold sensitivity、slow-drawdown bias 和 binding issue 判断；不得调 threshold。
- TRADING-590 必须判断 slow_drawdown positive evidence 是否 repeatable；如果只有一个独立 triggered slow drawdown window，不能 classified as promising。
- TRADING-591 必须解释 re-entry lag root cause，区分 signal recovery、threshold、hysteresis、confirmation window、scaler cap 和 implementation delay；不得改逻辑。
- TRADING-592 必须决定 B2 角色：general overlay 是否仍 plausible、fast-risk protection 是否 unsupported、是否可 narrow to slow-drawdown overlay 或需要更多 evidence。
- TRADING-593 必须输出 targeted evidence backfill v2 metrics，包含 B2 vs B0、no-trade/B1 wrapper availability、return/drawdown/turnover/cost/benchmark delta、trigger/false-risk-off/re-entry/missed rebound/window utility、best/worst window；不得调参。
- TRADING-594 必须汇总 targeted evidence 是否改善 B2 research case，包含各 window group、utility/cost/benchmark/signal robustness、role classification 和 remaining uncertainty。
- TRADING-595 必须输出 gate v5，且不得允许 B4/B5/B6/v3 或 paper-shadow。
- TRADING-596 必须输出 branch snapshot v2，包含 B2 role、gate v5、evidence quality、remaining blocker、B3 state、B4/B5/B6/v3/paper-shadow flags 和 next recommended task。

## 状态记录

- 2026-06-19：新增本批次并进入 IN_PROGRESS，原因：TRADING-587 已要求 B2-only targeted evidence；当前必须澄清 B2 的 fast-risk overlay scope、slow_drawdown repeatability 和 re-entry lag root cause，继续保持 B3/B4/B5/B6/v3 与 paper-shadow blocked。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-targeted-evidence-research` 输出 TRADING-588=`B2_TARGETED_EVIDENCE_WINDOWS_INCOMPLETE`、589=`B2_FAST_RISK_NOT_SUPPORTED_BY_CURRENT_DESIGN`、590=`B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY`、591=`B2_REENTRY_LAG_SIGNAL_DRIVEN`、592=`B2_FAST_RISK_OVERLAY_NOT_SUPPORTED`、593=`B2_TARGETED_EVIDENCE_BACKFILL_PARTIAL`、594=`B2_TARGETED_EVIDENCE_MIXED`、595=`B2_ONLY_CONTINUE_WITH_MORE_TARGETED_EVIDENCE`、596=`CONTINUE_B2_ONLY_RESEARCH`。结论：current targeted set 缺独立第二个 slow_drawdown repeatability window；rapid_drawdown / volatility_spike no-trigger 不是 binding issue，而是 fast-risk behavior 当前不受支持；slow_drawdown positive evidence 仍 single-window-only；re-entry lag 为 signal-driven 且窗口内未恢复到 normal exposure；B4/B5/B6/v3 和 paper-shadow 继续 blocked。
