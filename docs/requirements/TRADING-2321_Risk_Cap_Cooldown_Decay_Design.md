# TRADING-2321 Risk-Cap Cooldown / Decay Design

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2294 已把 `volatility_regime_scope_narrowed_risk_cap_v1` 的 forward
observe evidence accumulation 设计成 observe-only runtime contract，真实 run
status 为：

```text
FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED
```

2294 明确未启动 runtime：`forward_observe_started=false`、`runtime_started=false`，
daily / weekly report integration 仍为 `design_only`，portfolio / production /
broker effect 全部为 none。

TRADING-2321 承接 owner roadmap 的 execution mechanics 线，只定义 risk-cap 触发后
的执行层状态契约：

- `no_add_mode`
- `reduced_max_exposure_mode`
- `manual_review_mode`
- `cooldown_mode`

本任务不得把这些状态写入真实组合、日报生产建议、paper-shadow、production 或 broker
path。所有 cooldown / decay / cap 规则当前只能作为 design contract 和后续
TRADING-2322 / TRADING-2323 的输入。

## 目标

新增 CLI：

```bash
aits research trends risk-cap-cooldown-decay-design
```

读取 TRADING-2294 artifacts，输出 risk-cap execution mechanics design package。该包
说明 risk-cap trigger 如何映射到 no-add、reduced max exposure、manual review 和
cooldown / decay 状态，以及这些状态为什么当前只能 observe-only / design-only。

## 输入

默认读取：

```text
config/research/risk_cap_cooldown_decay_design_policy.yaml
outputs/research_trends/forward_observe_evidence_accumulation_plan/
```

必须验证 TRADING-2294 summary、runtime contract、daily observe record schema、
trigger follow-up schema、storage layout、minimum observation policy、decision matrix
和 runtime safety boundary。若 2294 已启动 runtime、打开 paper / production / broker
或把 observe-only 变成交易动作，应 fail closed。

## 产物

- `risk_cap_cooldown_decay_design_summary.json`
- `risk_cap_execution_state_contract.json`
- `risk_cap_cooldown_decay_rule_matrix.json`
- `risk_cap_cooldown_decay_rule_matrix.csv`
- `risk_cap_exposure_cap_state_matrix.json`
- `risk_cap_exposure_cap_state_matrix.csv`
- `risk_cap_manual_review_mode_contract.json`
- `risk_cap_execution_transition_matrix.json`
- `risk_cap_execution_transition_matrix.csv`
- `risk_cap_cooldown_decay_safety_boundary.json`
- `docs/research/risk_cap_cooldown_decay_design.md`

## 实施边界

1. Design-only execution mechanics。
   - 可以定义 state contract、transition contract、manual-review contract 和 safety
     boundary。
   - 不得读取或写入 portfolio weights。
   - 不得生成 target weight、rebalance instruction、buy/sell/reduce instruction。
   - 不得启动 paper-shadow、production 或 broker action。

2. Heuristic governance。
   - 所有 state ids、cooldown / decay checkpoint、cap status 和 review condition 必须
     来自 reviewed policy manifest。
   - 当前不得在代码中引入未解释的 numeric threshold 或 cap multiplier。
   - 任何未来可执行 cap multiplier、cooldown duration 或 release threshold 必须在
     TRADING-2323 simulation / owner review 后另行校准。

3. 2322 / 2323 边界。
   - TRADING-2321 不实现 signal validity / aging runtime。
   - TRADING-2321 不模拟 exposure-cap mechanics。
   - 可以为 2322 / 2323 输出 design handoff fields，但不得执行 runtime。

4. 安全边界。
   - `design_only=true`
   - `execution_runtime_started=false`
   - `portfolio_weights_read=false`
   - `portfolio_weights_written=false`
   - `target_weight_generated=false`
   - `rebalance_instruction_generated=false`
   - `broker_order_generated=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends risk-cap-cooldown-decay-design`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、TRADING-2294 source status、
  source runtime flags、state count、transition count、data quality status 和 safety
  flags。
- Execution state contract 覆盖四个 owner roadmap states。
- Cooldown / decay rule matrix 只输出 design-only rules，不输出 target weights 或 broker
  action。
- Exposure cap matrix 明确 cap multiplier 当前未校准、不得执行。
- Manual review contract 明确 manual_review_only，且自动调仓 / broker 动作均禁止。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2321 design-only flow。

## 进展记录

- 2026-07-01: 根据 owner roadmap 和 TRADING-2294 observe-only evidence plan 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。本批只允许实现 design-only execution
  mechanics package，不得启动 forward observe runtime、paper-shadow、production 或 broker。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run：
  `aits research trends risk-cap-cooldown-decay-design`，输出 status=
  `RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED`，source status=
  `FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED`，
  state_count=4，cooldown_decay_rule_count=3，exposure_cap_state_count=4，
  transition_count=5；所有输出保持 `design_only=true`、
  `execution_runtime_started=false`、`portfolio_weights_read=false`、
  `portfolio_weights_written=false`、`target_weight_generated=false`、
  `rebalance_instruction_generated=false`、`broker_order_generated=false`、
  promotion / paper-shadow / production / broker false/none。
- 2026-07-01: 验证通过 Ruff、compileall、2321 focused parallel pytest 8 passed、
  2293/2294/2321 adjacent focused parallel pytest 15 passed、docs/registry focused
  parallel pytest 27 passed、真实 CLI run、contract-validation 193 passed
  (runtime artifact=`outputs/validation_runtime/contract-validation_20260701T075801Z/test_runtime_summary.json`)。
  本命令只消费 TRADING-2294 静态 observe-only artifacts，不读取 cached market / macro data
  或 portfolio data，因此本批未运行 `aits validate-data`；未来 runtime、simulation、
  scoring、report 或 backtest 一旦消费 cached data，必须先执行 `aits validate-data` 或同一
  validation code path。
