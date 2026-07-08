# TRADING-2428 Growth Tilt Engine Observe-Only Signal Artifact Boundary

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2428_GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2427 manual review packet dry-run READY 后，定义 Growth Tilt Engine
未来 observe-only signal artifact 的 contract、字段、安全边界和可审计结构。

本任务不是生成真实策略信号，不生成 trading advice，不运行 scoring/backtest/daily
report，不启用 paper-shadow，不进入 production / broker。

## 输入

- TRADING-2427 manual review packet dry-run result
- TRADING-2427 manual review packet
- TRADING-2427 manual review checklist
- TRADING-2427 no-advice boundary summary
- TRADING-2427 reviewer handoff manifest
- TRADING-2427 route doc
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/observe_only_signal_artifact_boundary_result.json`
- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/signal_artifact_schema.json`
- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/valid_until_requirements.json`
- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/source_traceability_requirements.json`
- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/pit_contract_manual_review_requirements.json`
- `outputs/research_strategies/growth_tilt_engine_observe_only_signal_artifact_boundary/no_trading_advice_boundary.json`
- `docs/research/growth_tilt_engine_observe_only_signal_artifact_boundary.md`
- `docs/research/growth_tilt_engine_observe_only_signal_artifact_schema.md`
- `docs/research/growth_tilt_engine_observe_only_signal_valid_until_requirements.md`
- `docs/research/growth_tilt_engine_observe_only_signal_source_traceability_requirements.md`
- `docs/research/growth_tilt_engine_observe_only_signal_pit_contract_manual_review_requirements.md`
- `docs/research/growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.md`
- `docs/research/dynamic_strategy_2429_route.md`

## CLI

```bash
aits research strategies growth-tilt-engine-observe-only-signal-artifact-boundary --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY
```

READY payload 必须至少包含：

```yaml
observe_only_signal_artifact_boundary_ready: true
signal_artifact_schema_ready: true
valid_until_required: true
source_traceability_required: true
manual_review_required: true
generated_signal: false
generated_trading_advice: false
paper_shadow_enabled: false
production_enabled: false
broker_enabled: false
next_route: TRADING-2429_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary
```

## 安全边界

本任务不得：

- 生成真实 signal
- 生成 trading advice
- 生成 actionable allocation change
- 生成 broker order
- 修改实际组合权重
- 读取 fresh cached market data
- 运行 backtest、scoring 或 daily report
- 启用 paper-shadow
- 启用 schedule 或 scheduler
- 运行 production
- 触发 broker/order

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2428 只读取 prior artifacts/docs、
report registry、artifact catalog 和 system flow，不读取 fresh cached market data，
不运行 backtest/scoring/daily report，不生成 feature/signal 或交易建议。

如果实现阶段引入 fresh cached market/features/signals/event data 读取，本任务必须重新
引入 `aits validate-data` 或同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- observe-only signal artifact boundary result、signal schema、valid-until
  requirements、source traceability requirements、PIT/contract/manual-review
  requirements、no-trading-advice boundary 和 2429 route 均生成。
- TRADING-2427 READY 状态被读取并继承。
- schema 明确要求 `as_of`、`valid_until`、`known_at`、`decision_at`、source
  traceability、PIT readiness、contract readiness、manual review 和 no-advice 字段。
- 不生成真实 signal、trading advice、actionable allocation、broker order 或
  portfolio mutation。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_observe_only_signal_artifact_boundary.py
aits research strategies growth-tilt-engine-observe-only-signal-artifact-boundary --as-of 2026-07-08
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
  `GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY`，
  observe_only_signal_artifact_boundary_ready=true，
  observe_only_signal_artifact_boundary_gap_count=0，
  signal_artifact_schema_ready=true，valid_until_required=true，
  source_traceability_required=true，manual_review_required=true，
  generated_signal=false，generated_trading_advice=false，paper_shadow_enabled=false，
  production_enabled=false，broker_enabled=false，next route 指向
  `TRADING-2429_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts/docs、registry、
  catalog 和 system flow，不读取 fresh cached market data、不运行 backtest/scoring/
  daily report、不生成 feature/signal 或交易建议。
