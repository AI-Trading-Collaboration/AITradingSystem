# TRADING-557 to 564 B2 Evidence Expansion and B3 Precheck v2

最后更新：2026-06-19

## 背景

TRADING-537~556 已完成 post-B2/B3 risk and redesign research：

- Final branch：`CONTINUE_B2_ONLY_PATH`
- B2 gate：`B2_ONLY_NEEDS_MORE_EVIDENCE`
- B3 precheck：`B3_SIGNAL_DIRECTION_PRECHECK_MIXED`
- `b5_allowed=false`
- `b6_allowed=false`
- `v3_allowed=false`

当前解释：B2 是唯一仍 active 的研究路线；B3 signal-direction precheck 仍 mixed，不能重新进入组合研究；B4/B5/B6/v3 必须继续 blocked。

## 安全边界

本批次固定为 research-only / manual-review-only：

- 不访问 untouched holdout；
- 不调 B2 threshold；
- 不生成 B3 weights；
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
|TRADING-557|B2 evidence gap ledger|VALIDATING|
|TRADING-558|B2 risk-heavy window expansion v2|VALIDATING|
|TRADING-559|B2 risk trigger sensitivity map|VALIDATING|
|TRADING-560|B2 re-entry and opportunity cost review|VALIDATING|
|TRADING-561|B2-only research gate v2|VALIDATING|
|TRADING-562|B3 signal direction failure taxonomy|VALIDATING|
|TRADING-563|B3 redesign candidate precheck v2|VALIDATING|
|TRADING-564|Branch decision after B2 v2 and B3 precheck v2|VALIDATING|

## 必需输出

- `b2_evidence_gap_ledger.json/md`
- `b2_risk_heavy_window_expansion_v2.json/md`
- `b2_risk_trigger_sensitivity_map.json/md`
- `b2_reentry_opportunity_cost_review.json/md`
- `b2_only_research_gate_v2.json/md`
- `b3_signal_direction_failure_taxonomy.json/md`
- `b3_redesign_candidate_precheck_v2.json/md`
- `branch_decision_after_b2_v2_b3_precheck_v2.json/md`

## 验收标准

- TRADING-557 必须列出 B2 从 `NEEDS_MORE_EVIDENCE` 进入 `PROMISING`、`WEAK` 或 `RETURN_TO_DESIGN` 仍缺什么证据。
- TRADING-558 必须覆盖 rapid drawdown、slow drawdown、volatility spike、high-volatility sideways、semiconductor correction、V-shaped recovery、false risk-off cluster 和 shallow pullback false alarm，且所有窗口都是 diagnostic-only / holdout forbidden。
- TRADING-559 不得改变 threshold，只能判断 trigger coverage、threshold sensitivity、signal binding 或 window set 是否不足。
- TRADING-560 必须披露 risk-off dates、re-entry dates、lower exposure days、missed rebound proxy、V-shaped recovery lag 和 false risk-off opportunity cost。
- TRADING-561 不得允许 B4/B5/v3。
- TRADING-562 必须拆解 B3 mixed precheck 的 failure taxonomy。
- TRADING-563 只能做 signal-only precheck，不生成 weights、不 backfill。
- TRADING-564 必须保持 B4 retest 依赖 valid B3、B5 依赖 non-redundant valid B4、B6 依赖 valid B5。

## 状态记录

- 2026-06-19：新增本批次，原因：owner 要求在 TRADING-537~556 后扩展 B2-only 证据缺口与 B3 signal-direction redesign refinement；当前 B2 仍为 `B2_ONLY_NEEDS_MORE_EVIDENCE`，B3 precheck 仍 mixed，B5/B6/v3 继续 blocked。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-b3-v2-research` 输出 TRADING-557=`B2_EVIDENCE_GAP_LEDGER_READY`、558=`B2_RISK_HEAVY_WINDOW_EXPANSION_V2_READY`、559=`B2_TRIGGER_COVERAGE_TOO_LOW`、560=`B2_REENTRY_LAG_HIGH`、561=`B2_ONLY_NEEDS_MORE_EVIDENCE`、562=`B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY`、563=`B3_PRECHECK_MIXED`、564=`CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC`，且 `B4_retest_allowed=false`、`b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`。
