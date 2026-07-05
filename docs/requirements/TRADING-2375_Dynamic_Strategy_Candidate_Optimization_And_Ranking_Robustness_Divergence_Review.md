# TRADING-2375 Dynamic Strategy Candidate Optimization And Ranking-Robustness Divergence Review

最后更新：2026-07-05

## 背景

TRADING-2374 owner reassessment checkpoint 已停止 research-only observation 线性推进，
最终 route 为 `OWNER_REASSESSMENT_REQUIRED_BEFORE_TRADING_2375`。项目 owner 已选择
`Return to candidate optimization` + `Compare robustness top vs ranking top deeper`，
因此本任务回到策略候选本身，而不是继续 observation / scheduler / paper-shadow 工程线。

本任务重点解释并处理以下分歧：

|字段|候选|
|---|---|
|ranking top from TRADING-2365|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|
|robustness top from TRADING-2366|`dynamic_regime_overlay_v0_4_lower_turnover`|

## 范围

本任务新增 `aits research strategies dynamic-strategy-candidate-optimization-divergence-review`，
读取 TRADING-2365、TRADING-2366、TRADING-2367 和 TRADING-2374 artifacts，生成收益排序
与稳健性排序分歧解释、候选优化矩阵、fusion candidates 和 TRADING-2376 targeted retest route。

允许：

- 读取既有 market data、strategy configs 和 research artifacts。
- 运行或复用 research-only backtest / sensitivity 结果。
- 计算 cost-adjusted、turnover、cooldown、divergence 和 decision metrics。
- 生成 research-only JSON / Markdown 报告。

禁止：

- 启用 scheduler、event append、outcome binding、paper-shadow、production 或 broker/order。
- 创建 paper trade、shadow position、scheduled task 或 daily report。
- 把月度 rebalance 作为主排名依据。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source validation|DONE|fail-closed 校验 2365 / 2366 / 2367 / 2374 ready status、route、安全字段和 ranking / robustness top。|
|2. Divergence review|DONE|解释 ranking top 收益领先来源、robustness top 稳健来源、turnover / cooldown / cost fragility。|
|3. Optimization matrix|DONE|生成 ranking-top 降风险、robustness-top 增 upside、fusion candidate 的 base / realistic / conservative / harsh stress rows。|
|4. Decision update|DONE|输出 best candidate、recommended decision、monthly rebalance restriction 和 TRADING-2376 route。|
|5. Registry and docs|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc 和 research docs。|
|6. Validation and closeout|DONE|通过 focused parallel pytest、真实 CLI run、`aits validate-data --as-of 2026-07-05`、docs/report/task-register/contract gates、diff check、isolated commit 和 push。|

## 数据质量与回测口径

本任务读取策略 / 行情 / 回测相关数据，必须运行：

```bash
aits validate-data --as-of 2026-07-05
```

主执行口径固定为：

```yaml
primary_execution_cadence: valid_until_window
monthly_rebalance:
  allowed_for_reference: true
  allowed_for_primary_ranking: false
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/divergence_review_result.json
outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/optimization_matrix.json
outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/candidate_decision_update.json
docs/research/dynamic_strategy_candidate_optimization_divergence_review.md
docs/research/dynamic_strategy_ranking_vs_robustness_divergence_review.md
docs/research/dynamic_strategy_candidate_optimization_matrix.md
docs/research/dynamic_strategy_2376_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`。
- 输出包含 `ranking_top_from_2365`、`robustness_top_from_2366`、`ranking_robustness_divergence_detected=true`。
- 输出包含 `optimization_review_ready=true`、`divergence_explanation_ready=true`、
  `fusion_candidates_generated=true`、`candidate_decision_update_ready=true`。
- 输出包含 `best_candidate_after_optimization`、`recommended_decision_after_optimization` 和
  `recommended_next_research_task=TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest`。
- `primary_execution_cadence=valid_until_window`，月度 rebalance 只作为 reference，不作为 primary ranking。
- scheduler、event append、outcome binding、paper-shadow、production、broker、daily report 全部保持 false / none。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务回到 strategy research candidate optimization，不继续 observation 工程线；实现前固定 no scheduler / no event append / no outcome binding / no paper-shadow / no production / no broker / no daily report 边界。
- 2026-07-05: 实现完成并进入 closeout validation。真实 CLI run status=`DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`；`aits validate-data --as-of 2026-07-05` 为 `PASS_WITH_WARNINGS` / errors=0；best candidate after optimization=`dynamic_regime_overlay_v0_4_lower_turnover`，recommended decision=`OWNER_REVIEW_REQUIRED`，next route=`TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest`。Fusion candidates 已生成并压力测试，但尚未优于 ranking top 与 robustness top；paper-shadow、scheduler、event append、outcome binding、production、broker 和 daily report 全部保持 disabled / false / none。
- 2026-07-05: closeout validation 完成并归档 `DONE`。验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、真实 CLI run、`aits validate-data --as-of 2026-07-05` PASS_WITH_WARNINGS / errors=0、docs freshness PASS、documentation contract PASS、task-register consistency run/validate PASS、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260705T125418Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning）。
