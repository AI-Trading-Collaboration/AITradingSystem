# TRADING-565 to 574 B2 Full Diagnostic and B3 Resolution

最后更新：2026-06-19

## 背景

TRADING-557~564 已完成 B2-only evidence expansion 和 B3 precheck v2：

- Branch decision：`CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC`
- B2 gate v2：`B2_ONLY_NEEDS_MORE_EVIDENCE`
- B3 precheck v2：`B3_PRECHECK_MIXED`
- `B4_retest_allowed=false`
- `b5_allowed=false`
- `b6_allowed=false`
- `v3_allowed=false`

当前解释：B2 是唯一可继续 full diagnostic 的 active research path；B3 只能进入 signal-resolution plan，不能生成权重或运行 mini-backfill；B4/B5/B6/v3 继续 blocked。

## 安全边界

本批次固定为 research-only / manual-review-only：

- 不访问 untouched holdout；
- 不调 B2 threshold 或参数；
- B2 full diagnostic 只包含 B0 static baseline、fast asymmetric risk overlay、risk-off exposure scaler 和 re-entry logic；
- 不混入 B3 slow tilt、B5 confidence shrinkage、B6 regime information 或 P0 mixed allocator；
- 不生成 B3 weights；
- 不运行 B3 mini-backfill；
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
|TRADING-565|B2 full diagnostic scope freeze|VALIDATING|
|TRADING-566|B2 full diagnostic window finalization|VALIDATING|
|TRADING-567|B2 full diagnostic backfill|VALIDATING|
|TRADING-568|B2 drawdown protection attribution|VALIDATING|
|TRADING-569|B2 false-risk-off and re-entry cost review|VALIDATING|
|TRADING-570|B2 cost benchmark utility review|VALIDATING|
|TRADING-571|B2 signal robustness and trigger stability|VALIDATING|
|TRADING-572|B2-only full diagnostic gate|VALIDATING|
|TRADING-573|B3 signal precheck resolution plan|VALIDATING|
|TRADING-574|B2-B3 branch status snapshot|VALIDATING|

## 必需输出

- `b2_full_diagnostic_scope.json/md`
- `b2_full_diagnostic_windows.json/md`
- `b2_full_diagnostic_backfill.json/md`
- `b2_drawdown_protection_attribution.json/md`
- `b2_false_risk_off_reentry_cost_review.json/md`
- `b2_cost_benchmark_utility_review.json/md`
- `b2_signal_robustness_trigger_stability.json/md`
- `b2_only_full_diagnostic_gate.json/md`
- `b3_signal_precheck_resolution_plan.json/md`
- `b2_b3_branch_status_snapshot.json/md`

## 验收标准

- TRADING-565 必须冻结 B2 full diagnostic scope，明确定义 allowed/forbidden mechanisms、allowed windows、no untouched holdout、validation 和 Reader Brief。
- TRADING-566 必须覆盖 rapid drawdown、slow drawdown、volatility spike、high-volatility sideways、semiconductor correction、V-shaped recovery、false risk-off cluster、shallow pullback false alarm、normal uptrend control 和 calm market control，且每个窗口都有 start/end、regime、expected behavior、risk intensity、diagnostic purpose、holdout_allowed=false 和 data quality status。
- TRADING-567 必须输出 B2 vs B0，并仅在 config 显式启用时允许 B2 vs B1 optional wrapper；不得调 B2 参数。
- TRADING-568 必须区分 successful protection、late protection、unnecessary protection、no trigger 和 harmful trigger。
- TRADING-569 必须披露 false risk-off dates、risk-off duration、re-entry lag、missed rebound proxy、V-shaped recovery cost、shallow pullback false alarm cost、below-baseline exposure time 和 exit/re-entry asymmetry。
- TRADING-570 必须比较 zero / low / medium / high cost 下的 net utility、benchmark-relative、worst-window、turnover、drawdown benefit 和 opportunity cost。
- TRADING-571 必须检查 signal coverage、missing dates、stale inputs、trigger stability、calm-window no-trigger correctness 和 false alarm behavior。
- TRADING-572 不得允许 B4/B5/B6/v3。
- TRADING-573 必须只读 B3 taxonomy 和 precheck v2，输出 redesign/drop recommendation，不生成权重、不运行 mini-backfill。
- TRADING-574 必须输出 B1/B2/B3 状态、B4/B5/B6/v3 allowed flags 和推荐路径；B4 retest 必须依赖 valid B3，B5 必须依赖 non-redundant valid B4，B6 必须依赖 valid B5。

## 状态记录

- 2026-06-19：新增本批次，原因：owner 要求在 TRADING-557~564 后冻结 B2-only full diagnostic scope、完成非 holdout window set、运行 B2 full diagnostic、归因 drawdown/re-entry/cost/signal robustness，并对 B3 mixed precheck 给出 resolution plan 和 branch snapshot；当前 B2 仍为 `B2_ONLY_NEEDS_MORE_EVIDENCE`，B3 为 `B3_PRECHECK_MIXED`，B4/B5/B6/v3 继续 blocked。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-full-diagnostic-research` 输出 TRADING-565=`B2_FULL_DIAGNOSTIC_SCOPE_READY`、566=`B2_FULL_DIAGNOSTIC_WINDOWS_READY`、567=`B2_FULL_DIAGNOSTIC_PARTIAL`、568=`B2_DRAWDOWN_PROTECTION_MIXED`、569=`B2_REENTRY_LAG_HIGH`、570=`B2_UTILITY_MIXED`、571=`B2_TRIGGER_STABILITY_WEAK`、572=`B2_ONLY_NEEDS_MORE_EVIDENCE`、573=`B3_SIGNAL_PRECHECK_RESOLUTION_READY`、574=`CONTINUE_B2_ONLY_RESEARCH`；567 为 PARTIAL，原因是 normal/calm controls 只有 normal-market no-trigger reference，尚无独立 B2 control-window signal rerun artifact；`B4_retest_allowed=false`、`b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`。
