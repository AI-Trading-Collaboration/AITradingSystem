# TRADING-2419 Growth Tilt Engine PIT Gate Readiness Recheck

最后更新：2026-07-08

## 状态

- 任务登记：`TRADING-2419_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统验证完成；项目 owner 后续复核 TRADING-2420 signal artifact source traceability remediation
- 日期：2026-07-08

## 背景

TRADING-2418 已完成 `valid_until_window` dependency evidence closure，真实状态为
`GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`。2418 只生成
pre-recheck evidence，未标记任何 source feature 为 PIT gate ready 或 contract ready。

当前必须保留并复核的状态：

- `pit_gate_ready_count=0`
- `contract_ready_count=0`
- `pit_gate_blocked_count=10`
- `blocked_by_source_traceability_count=5`
- `blocked_by_valid_until_window_count=1`
- `source_traceability_still_blocked=["growth_tilt_engine_signal_artifact"]`
- `next_route=TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck`

TRADING-2419 的目标是在 2418 evidence closure 后执行一次 Growth Tilt Engine PIT gate
readiness recheck，确认最新 prior artifacts、registry、artifact catalog 和 research docs
是否一致，并明确 remaining blocker 是否仍由 `growth_tilt_engine_signal_artifact` 的 source
traceability gap 阻断。本任务不是 blocker remediation、owner downgrade、paper-shadow 或交易执行任务。

## 范围

允许：

- 读取 TRADING-2418 closure result、valid-until dependency evidence、signal validity contract
  evidence、stale signal policy evidence、growth tilt valid-until alignment evidence、remaining
  blocker summary 和 research docs。
- 读取 TRADING-2417 source traceability / upstream artifact closure evidence、updated mapping
  和 remaining blocker summary。
- 读取 TRADING-2416 remaining blocker matrix、PIT gate evidence requirements 和 closure result。
- 读取 TRADING-2415 PIT gate readiness snapshot / matrix / validation / remaining blocker summary。
- 读取 `config/research/dynamic_strategy_pit_input_registry.yaml`、report registry 和 artifact catalog。
- 生成 PIT gate readiness recheck artifact、blocker classification、remaining blocker summary、
  research docs 和 TRADING-2420 route。

禁止：

- 不标记任何 source feature 为 PIT gate ready 或 contract ready。
- 不解除或降级 `growth_tilt_engine_signal_artifact` blocker。
- 不解除或降级 `growth_tilt_engine` / `valid_until_window` blocker。
- 不恢复 candidate search。
- 不批准 research-only observation、paper-shadow、scheduler、event append、outcome binding、
  production 或 broker/order path。
- 不运行新策略 backtest、不生成新 feature/signal/scoring/daily report。

## 输出

- `outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/readiness_recheck_result.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/pit_gate_recheck_matrix.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/blocker_classification.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md`
- `docs/research/growth_tilt_engine_pit_gate_recheck_matrix.md`
- `docs/research/growth_tilt_engine_signal_artifact_source_traceability_blocker.md`
- `docs/research/dynamic_strategy_2420_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-pit-gate-readiness-recheck`
  返回 `GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY`。
- 输出明确 `pit_gate_ready_count=0`、`contract_ready_count=0`、`pit_gate_blocked_count=10`。
- remaining blocker list 包含且保留 `growth_tilt_engine_signal_artifact`。
- blocker classification 将 `growth_tilt_engine_signal_artifact` 标为 `source_traceability`。
- `blockers_resolved=false`、`blockers_downgraded=false`、`auto_mark_pit_gate_ready=false`、
  `auto_mark_contract_ready=false`。
- paper-shadow、production、broker、scheduler、event append、outcome binding、daily report
  全部保持 false / none。
- next route 为 `TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_Source_Traceability_Remediation`。
- 缺失 required prior artifact 时 fail closed，不 silent pass。
- report registry、artifact catalog、system flow、task register、completed closeout 文档和 focused
  tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_pit_gate_readiness_recheck.py`
- `aits research strategies growth-tilt-engine-pit-gate-readiness-recheck --as-of 2026-07-08`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data` 不在默认验证中运行，因为本任务只读取 prior artifacts、registry、catalog
和 docs，不读取 fresh cached market/macro/features/signals，不运行 backtest/scoring/daily report。

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为 PIT gate readiness
  recheck，不修复 source traceability blocker、不降级 severity、不恢复任何交易或观察路径。
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI status=
  `GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY`；
  `pit_gate_ready_count=0`、`contract_ready_count=0`、`pit_gate_blocked_count=10`，
  remaining blocker 仍为 `growth_tilt_engine_signal_artifact`，blocker classification 为
  `source_traceability`，next route=
  `TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_Source_Traceability_Remediation`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts、registry、catalog 和 docs，
  不读取 fresh cached market data、不运行 backtest/scoring/daily report，也不生成新 signal
  或交易建议。
