# TRADING-2429 Growth Tilt Engine Forward Outcome Binding Boundary

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2429_GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2428 observe-only signal artifact boundary READY 后，定义 Growth Tilt
Engine 未来 observe-only signal 的 forward outcome binding contract。

本任务只定义 outcome 绑定机制和审计边界，不生成真实 signal，不回填真实
outcome，不运行 scoring/backtest/daily report，不读取 fresh market data，不启用
paper-shadow，不进入 production / broker。

## 输入

- TRADING-2428 observe-only signal artifact boundary result
- TRADING-2428 signal artifact schema
- TRADING-2428 valid-until requirements
- TRADING-2428 source traceability requirements
- TRADING-2428 PIT / contract / manual review requirements
- TRADING-2428 no-trading-advice boundary
- TRADING-2428 route doc
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/forward_outcome_binding_boundary_result.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/outcome_horizon_rules.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/valid_until_binding_rules.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/outcome_decision_rules.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/baseline_comparison_rules.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/outcome_artifact_schema.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/signal_to_outcome_linkage.json`
- `outputs/research_strategies/growth_tilt_engine_forward_outcome_binding_boundary/no_effect_boundary.json`
- `docs/research/growth_tilt_engine_forward_outcome_binding_boundary.md`
- `docs/research/growth_tilt_engine_forward_outcome_horizon_rules.md`
- `docs/research/growth_tilt_engine_forward_outcome_valid_until_binding_rules.md`
- `docs/research/growth_tilt_engine_forward_outcome_decision_rules.md`
- `docs/research/growth_tilt_engine_forward_outcome_baseline_comparison_rules.md`
- `docs/research/growth_tilt_engine_forward_outcome_artifact_schema.md`
- `docs/research/growth_tilt_engine_signal_to_outcome_linkage.md`
- `docs/research/growth_tilt_engine_forward_outcome_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2430_route.md`

## CLI

```bash
aits research strategies growth-tilt-engine-forward-outcome-binding-boundary --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY
```

READY payload 必须至少包含：

```yaml
forward_outcome_binding_boundary_ready: true
outcome_schema_ready: true
valid_until_binding_ready: true
baseline_comparison_ready: true
outcome_horizons:
  - 1d
  - 5d
  - 10d
  - 20d
generated_signal: false
outcome_backfilled: false
paper_shadow_enabled: false
production_enabled: false
broker_enabled: false
next_route: TRADING-2430_Growth_Tilt_Engine_Candidate_Promotion_Evidence_Review
```

## 安全边界

本任务不得：

- 生成真实 signal
- 回填真实 outcome
- 读取 fresh cached market data
- 运行 backtest、scoring 或 daily report
- 生成 trading advice
- 生成 actionable allocation change
- 生成 broker order
- 修改实际组合权重
- 启用 paper-shadow
- 启用 schedule 或 scheduler
- 运行 production
- 触发 broker/order

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2429 只读取 prior artifacts/docs、
report registry、artifact catalog 和 system flow，不读取 fresh cached market data，
不运行 backtest/scoring/daily report，不生成 feature/signal，不回填 outcome，也不生成
交易建议。

如果实现阶段引入 fresh cached market/features/signals/outcome/event data 读取，本任务
必须重新引入 `aits validate-data` 或同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- forward outcome binding boundary result、horizon rules、valid-until binding
  rules、decision rules、baseline comparison rules、outcome artifact schema、
  signal-to-outcome linkage、no-effect boundary 和 2430 route 均生成。
- TRADING-2428 READY 状态被读取并继承。
- outcome horizons 明确为 `1d`、`5d`、`10d`、`20d`。
- valid-until 后 outcome 回填规则明确要求 observation window closed、no future data
  at decision time、source traceability preserved。
- pass / fail / inconclusive 判定规则和 baseline comparison 规则可审计。
- 不生成真实 signal、outcome backfill、trading advice、actionable allocation、
  broker order 或 portfolio mutation。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_forward_outcome_binding_boundary.py
aits research strategies growth-tilt-engine-forward-outcome-binding-boundary --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI run 输出
  `GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY`，
  forward_outcome_binding_boundary_ready=true，
  forward_outcome_binding_boundary_gap_count=0，outcome horizons=`1d/5d/10d/20d`，
  outcome_schema_ready=true，valid_until_binding_ready=true，
  outcome_decision_rules_ready=true，baseline_comparison_ready=true，
  signal_to_outcome_linkage_ready=true，no_effect_boundary_ready=true，
  generated_signal=false，outcome_backfilled=false，outcome_binding_executed=false，
  outcome_store_mutated=false，paper_shadow_enabled=false，production_enabled=false，
  broker_enabled=false，next route 指向
  `TRADING-2430_Growth_Tilt_Engine_Candidate_Promotion_Evidence_Review`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts/docs、registry、
  catalog 和 system flow，不读取 fresh cached market/outcome data、不运行 backtest/
  scoring/daily report、不生成 feature/signal 或交易建议。
