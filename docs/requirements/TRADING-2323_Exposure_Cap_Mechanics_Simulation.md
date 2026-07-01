# TRADING-2323 Exposure-Cap Mechanics Simulation

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2321 已定义 risk-cap trigger 后的 design-only execution mechanics：

- `no_add_mode`
- `reduced_max_exposure_mode`
- `manual_review_mode`
- `cooldown_mode`

TRADING-2322 又把这些状态延伸成 signal validity / aging runtime design contract，
覆盖 `valid_from`、`valid_until`、`decay`、`staleness`、`trigger_aging` 和
`release_restore_rule`。两者当前都不是 runtime：没有写入 observe records，没有读取
portfolio weights，没有生成 target weight / rebalance / broker order，也没有校准
validity duration、decay multiplier、release threshold 或 max exposure multiplier。

Owner roadmap 要求 TRADING-2323 在 simulation / observe-only 环境测试 exposure-cap
mechanics。但当前上游数据只支持 source-blocked simulation readiness package，不能执行
真实数值仿真。

## 目标

新增 CLI：

```bash
aits research trends exposure-cap-mechanics-simulation
```

默认读取：

```text
config/research/exposure_cap_mechanics_simulation_policy.yaml
outputs/research_trends/signal_validity_aging_runtime_design/
```

输出 exposure-cap mechanics simulation readiness package，覆盖以下未来仿真问题：

- risk-cap 触发后 `max exposure` 如何变化。
- cooldown 如何影响 turnover。
- risk-cap 解除后是否恢复。
- false risk-cap 成本。

## 当前阻断

真实仿真当前被以下 source gap 阻断：

- 没有 runtime observe records。
- 没有 calibrated cap multiplier policy。
- 没有 portfolio exposure history。
- 没有 turnover history 或 trade intent log。
- 没有 post-trigger return / stress outcome records。
- 没有 release / restore decision records。

不得用手工 fake rows、subjective numeric assumptions 或 2321/2322 design-only fields
替代这些输入。

## 产物

- `exposure_cap_mechanics_simulation_summary.json`
- `exposure_cap_simulation_metric_contract.json`
- `exposure_cap_simulation_readiness_matrix.json`
- `exposure_cap_simulation_readiness_matrix.csv`
- `exposure_cap_simulation_input_requirement_matrix.json`
- `exposure_cap_simulation_input_requirement_matrix.csv`
- `exposure_cap_simulation_blocker_report.json`
- `exposure_cap_simulation_blocker_report.csv`
- `exposure_cap_simulation_safety_boundary.json`
- `docs/research/exposure_cap_mechanics_simulation.md`

## 实施边界

1. Source-blocked simulation readiness。
   - 可以定义未来仿真需要的 objective、metric contract、input requirements 和 blockers。
   - 不得执行 exposure-cap simulation。
   - 不得生成 simulation result、effect claim、max exposure delta、turnover delta、
     restore lag 或 false risk-cap cost 数字。

2. Data-quality gate。
   - 当前命令只读取 TRADING-2322 静态 design-only artifacts，不读取 cached market /
     macro data、runtime observe records、portfolio exposure history 或 turnover records。
   - 因此本批 `aits validate-data` 不作为当前运行门禁。
   - 未来一旦进入真实 simulation、runtime、scoring、report 或 backtest 并消费 cached data，
     必须先执行 `aits validate-data` 或同源 validation code path。

3. 安全边界。
   - `source_blocked_no_simulation=true`
   - `simulation_executed=false`
   - `simulation_result_generated=false`
   - `runtime_records_consumed=false`
   - `portfolio_exposure_history_consumed=false`
   - `turnover_records_consumed=false`
   - `post_trigger_return_outcomes_consumed=false`
   - `release_restore_records_consumed=false`
   - `target_weight_generated=false`
   - `max_exposure_number_generated=false`
   - `rebalance_instruction_generated=false`
   - `broker_order_generated=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends exposure-cap-mechanics-simulation`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、TRADING-2322 source status、
  source runtime flags、simulation objective count、blocked objective count、input
  requirement count、blocker count、metric contract status、data quality status 和 safety
  flags。
- Readiness matrix 覆盖四个 objective，并且全部标记为
  `SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED`。
- Metric contract 只定义未来 metric，不生成任何 metric result 或 effect claim。
- Input requirement matrix 明确 runtime observe、cap multiplier、exposure、turnover、
  post-trigger outcome、release / restore decision 等输入缺口。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2323 source-blocked simulation flow。

## 进展记录

- 2026-07-01: 根据 owner roadmap 和 TRADING-2322 design-only output 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须 selective
  staging，不能混入无关改动。本批只允许实现 source-blocked simulation readiness package，
  不得生成 exposure-cap mechanics simulation result、target weight、rebalance instruction、
  paper-shadow、production 或 broker action。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run：
  `aits research trends exposure-cap-mechanics-simulation`，输出 status=
  `EXPOSURE_CAP_MECHANICS_SIMULATION_SOURCE_BLOCKED_NOT_EXECUTED`，source status=
  `SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED`，
  simulation_objective_count=4，blocked_objective_count=4，input_requirement_count=20，
  blocker_count=24，metric_count=4；所有输出保持 `source_blocked_no_simulation=true`、
  `simulation_executed=false`、`simulation_result_generated=false`、
  `runtime_records_consumed=false`、`portfolio_exposure_history_consumed=false`、
  `turnover_records_consumed=false`、`target_weight_generated=false`、
  `max_exposure_number_generated=false`、`rebalance_instruction_generated=false`、
  `broker_order_generated=false`、promotion / paper-shadow / production / broker false/none。
- 2026-07-01: 验证通过 Ruff、compileall、2323 focused parallel pytest 8 passed、
  2321/2322/2323 adjacent focused parallel pytest 24 passed、docs/registry focused
  parallel pytest 27 passed、真实 CLI run、contract-validation 193 passed
  (runtime artifact=`outputs/validation_runtime/contract-validation_20260701T084410Z/test_runtime_summary.json`)
  和 `git diff --check`。本命令只消费 TRADING-2322 静态 design-only artifacts，不读取 cached
  market data、runtime observe records、portfolio exposure history、turnover records 或
  post-trigger outcomes，因此本批未运行 `aits validate-data`；未来 simulation、runtime、
  scoring、report 或 backtest 一旦消费 cached data，必须先执行 `aits validate-data` 或同源
  data-quality gate。
