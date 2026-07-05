# TRADING-2387 Dynamic Strategy Observation Gate Threshold Calibration Review

最后更新：2026-07-06

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-06
- 任务登记：`TRADING-2387_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW`
- 目标状态：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- 下一路由：`TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`

## 背景

TRADING-2386 已完成 expanded candidate pool retest and screening，真实 run status=`DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`。2386 当前排名第一候选仍为 `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`，decision=`CONTINUE_OPTIMIZATION`，`observation_ready_candidate_found=false`。

2386 的核心解释不是收益或成本失败：current best 的 `dynamic_vs_static_gap=0.021302`，realistic / conservative / harsh cost 均通过，turnover budget 也通过；阻断来自 `reference candidate` hard block、`time_slice_pass_rate=0.0`、`regime_slice_pass_rate=0.0` 和 `drawdown_not_materially_worse=false`。因此 2387 需要先审计 research-only observation gate 是否过于接近 paper-shadow / production 前置标准。

## 范围

本任务只生成 gate calibration review 和 policy proposal，不批准 observation，不修改 2386 结果，也不修改真实准入规则。

允许动作：

- 读取 TRADING-2365 / 2366 / 2384 / 2385 / 2386 prior validated research artifacts。
- 检查既有 decision rule / gate threshold constants。
- 生成 threshold calibration review、gate policy review、candidate reclassification preview、recommended policy update、owner review recommendation 和 research docs。

禁止动作：

- 启用 scheduler 或创建 scheduled task。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API 或生成 order。
- 生成 daily report。
- 运行新 backtest。
- 生成新 signal 或 scoring。

## 输入

必读 source tasks：

- `TRADING-2365`
- `TRADING-2366`
- `TRADING-2384`
- `TRADING-2385`
- `TRADING-2386`

重点候选：

- `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`：2386 current best，current decision=`CONTINUE_OPTIMIZATION`。
- `dynamic_turnover_budgeted_growth_tilt_v1`：新候选，time slice 约 0.428571，regime slice 0，guarded gap 为负。
- `dynamic_valid_until_expiry_strict_v1`：新候选，time slice 约 0.428571，regime slice 0，guarded gap 为负。
- `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`：robustness reference。
- `equal_risk_growth_tilt_guarded_turnover_v1`：guarded return reference。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_observation_gate_threshold_calibration_review.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-observation-gate-threshold-calibration-review`。
3. fail-closed 校验 2365 / 2366 / 2384 / 2385 / 2386 source status、current best、2386 current decision、安全 side-effect flags 和 source hashes。
4. 生成 current observation gate rules、reference candidate hard block review、time slice threshold review、regime expectation review、drawdown materiality review、research-only vs paper-shadow gate separation。
5. 生成 candidate reclassification preview；current best 在 calibrated-policy preview 下只能进入 `OWNER_REVIEW_REQUIRED`，不得 auto-accept。
6. 生成 recommended gate policy update；标明 proposal 未应用，真实规则未修改。
7. 更新 registry、artifact catalog、system flow、task register 和完成归档文档。
8. 新增 focused tests 并执行验证。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为它只读取 prior validated research artifacts 和既有 threshold constants，不重新读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为重新读取行情、重新 backtest 或生成 signal / scoring，则必须先运行同源 cached-data quality gate 并在输出中披露。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`。
- 输出 JSON 至少包含 `gate_calibration_review_result.json`、`gate_policy_review.json`、`candidate_reclassification_preview.json`、`recommended_gate_policy_update.json`。
- Markdown 报告明确回答 observation gate 是否可能过严、reference candidate 是否应 hard-block、time/regime slice threshold 是否需要分层、drawdown materiality 是否应结合收益补偿、research-only observation 与 paper-shadow 是否应使用不同门槛，以及 current best 是否应进入 owner review preview。
- 所有 safety fields 保持 false / none；不批准 observation、paper-shadow、production 或 broker。
- Registry、artifact catalog、system flow、task register 一致。
- Focused tests、Ruff、compileall、docs gates、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-06：实现完成并归档 `DONE`。真实 CLI run status=`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`；current best=`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`，current decision=`CONTINUE_OPTIMIZATION`，calibrated preview=`OWNER_REVIEW_REQUIRED` / auto_accept=false / owner_review=true；本任务未运行 `aits validate-data`，因为只读取 prior validated artifacts 和既有 threshold constants，不读取 fresh cached market data、不重新 backtest、不生成 signal/scoring。
