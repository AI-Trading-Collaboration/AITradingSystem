# TRADING-2322 Signal Validity / Aging Runtime Design

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2321 已把 TRADING-2294 observe-only evidence plan 延伸为 risk-cap
execution mechanics design package，覆盖：

- `no_add_mode`
- `reduced_max_exposure_mode`
- `manual_review_mode`
- `cooldown_mode`

2321 仍是 design-only：不读取或写入 portfolio weights，不生成 target weight、
rebalance instruction 或 broker order，不启动 paper-shadow、production 或 broker path。

TRADING-2322 承接 owner roadmap 的下一步，目标是把 signal validity / trigger aging
规则定义成可审计 runtime design contract。当前不得实现真实 runtime，也不得把 aging
状态接入日报生产建议、仓位、paper-shadow、production 或 broker。

## 目标

新增 CLI：

```bash
aits research trends signal-validity-aging-runtime-design
```

读取 TRADING-2321 artifacts，输出 signal validity / aging runtime design package。该包
统一描述以下字段和状态：

- `valid_from`
- `valid_until`
- `decay`
- `staleness`
- `trigger_aging`
- `release_restore_rule`

## 输入

默认读取：

```text
config/research/signal_validity_aging_runtime_design_policy.yaml
outputs/research_trends/risk_cap_cooldown_decay_design/
```

必须验证 TRADING-2321 summary、execution state contract、cooldown / decay rule matrix、
exposure-cap state matrix、manual-review mode contract、execution transition matrix 和
safety boundary。若 2321 已启动 runtime、打开 portfolio write / target weight /
rebalance / broker / paper / production，或不再是 design-only，应 fail closed。

## 产物

- `signal_validity_aging_runtime_design_summary.json`
- `signal_validity_lifecycle_contract.json`
- `signal_validity_aging_rule_matrix.json`
- `signal_validity_aging_rule_matrix.csv`
- `signal_validity_trigger_aging_state_matrix.json`
- `signal_validity_trigger_aging_state_matrix.csv`
- `signal_validity_release_restore_rule_matrix.json`
- `signal_validity_release_restore_rule_matrix.csv`
- `signal_validity_runtime_record_schema.json`
- `signal_validity_aging_safety_boundary.json`
- `docs/research/signal_validity_aging_runtime_design.md`

## 实施边界

1. Design-only runtime contract。
   - 可以定义 lifecycle fields、aging state、release / restore rule 和 runtime record schema。
   - 不得启动 runtime 或写入 actual observe records。
   - 不得读取或写入 portfolio weights。
   - 不得生成 target weight、rebalance instruction、buy/sell/reduce instruction。
   - 不得启动 paper-shadow、production 或 broker action。

2. Heuristic governance。
   - 所有 lifecycle fields、aging states、release / restore rule 和 review condition 必须来自
     policy manifest。
   - 当前不得引入未解释的 numeric threshold、validity duration 或 decay multiplier。
   - 5d / 10d / 20d 只能作为继承自 TRADING-2294/2321 follow-up checkpoints 的设计字段，
     不得被解释为可执行 expiry threshold。

3. 2323 边界。
   - TRADING-2322 不模拟 exposure-cap mechanics。
   - TRADING-2322 不校准 max exposure、turnover 或 false-risk-cap cost。
   - 可以为 2323 输出 schema handoff fields，但不得执行 simulation。

4. 安全边界。
   - `design_only=true`
   - `aging_runtime_started=false`
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

- CLI implemented: `aits research trends signal-validity-aging-runtime-design`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、TRADING-2321 source status、
  source runtime flags、lifecycle field count、aging rule count、trigger aging state count、
  release / restore rule count、data quality status 和 safety flags。
- Lifecycle contract 覆盖 `valid_from`、`valid_until`、`decay`、`staleness`、
  `trigger_aging`、`release_restore_rule`。
- Aging rule matrix 只输出 design-only rules，不输出 expiry action、target weight 或 broker
  action。
- Runtime record schema 明确 allowed action 仍是 `observe_only_design_contract`，且禁止
  target weight / rebalance / broker fields。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2322 design-only flow。

## 进展记录

- 2026-07-01: 根据 owner roadmap 和 TRADING-2321 design-only execution mechanics package
  新增并进入 `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。本批只允许实现 signal validity / aging runtime
  design contract，不得启动真实 runtime、paper-shadow、production 或 broker。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run：
  `aits research trends signal-validity-aging-runtime-design`，输出 status=
  `SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED`，source status=
  `RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED`，lifecycle_field_count=6，
  aging_rule_count=5，trigger_aging_state_count=4，release_restore_rule_count=4；
  所有输出保持 `design_only=true`、`aging_runtime_started=false`、
  `signal_validity_runtime_started=false`、`execution_runtime_started=false`、
  `portfolio_weights_read=false`、`portfolio_weights_written=false`、
  `target_weight_generated=false`、`rebalance_instruction_generated=false`、
  `broker_order_generated=false`、promotion / paper-shadow / production / broker false/none。
- 2026-07-01: 验证通过 Ruff、compileall、2322 focused parallel pytest 8 passed、
  2294/2321/2322 adjacent focused parallel pytest 20 passed、docs/registry focused
  parallel pytest 27 passed、真实 CLI run、contract-validation 193 passed
  (runtime artifact=`outputs/validation_runtime/contract-validation_20260701T082153Z/test_runtime_summary.json`)
  和 `git diff --check`。本命令只消费 TRADING-2321 静态 design-only artifacts，不读取 cached
  market / macro data、runtime observe records 或 portfolio data，因此本批未运行
  `aits validate-data`；未来 runtime、simulation、scoring、report 或 backtest 一旦消费 cached
  data，必须先执行 `aits validate-data` 或同一 validation code path。
