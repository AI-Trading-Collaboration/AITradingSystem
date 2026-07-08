# TRADING-2417 Growth Tilt Engine Source Traceability And Upstream Artifact Closure

最后更新：2026-07-08

## 状态

- 任务登记：`TRADING-2417_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统实现
- 日期：2026-07-08

## 背景

TRADING-2416 已生成 growth tilt engine PIT gate remaining blocker closure plan，真实状态为
`GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`。该计划继承
TRADING-2415 readiness snapshot 的 blocker 状态：

- `source_feature_count=10`
- `pit_gate_ready_count=0`
- `contract_ready_count=0`
- `pit_gate_blocked_count=10`
- `blocked_by_source_traceability_count=5`
- `blocked_by_valid_until_window_count=1`

TRADING-2417 的目标是实际整理 5 个 source traceability blockers 的可审计 evidence，
并把可映射的 source config / upstream artifact / owner module / generated_at /
source_data_cutoff / feature_version 显式写入 closure evidence。该 evidence 只为后续 PIT
gate readiness recheck 提供输入，不代表 readiness、contract-ready 或 blocker downgrade。

## 范围

允许：

- 读取 TRADING-2416 closure plan result、remaining blocker matrix、source traceability
  closure plan、as-of evidence closure plan、valid-until dependency closure plan 和 PIT gate
  evidence requirements。
- 读取 TRADING-2415 readiness snapshot / matrix。
- 读取 TRADING-2413 source traceability remediation artifacts、TRADING-2412 as-of mapping
  artifacts、TRADING-2410 source feature contract mapping artifacts。
- 读取 `config/research/dynamic_strategy_pit_input_registry.yaml`、report registry 和
  artifact catalog。
- 生成 source traceability closure evidence、upstream artifact closure evidence、updated
  source feature mapping、remaining blocker summary、research docs 和 TRADING-2418 route。

禁止：

- 不标记任何 source feature 为 PIT gate ready 或 contract ready。
- 不解除或降级 `growth_tilt_engine` blocker。
- 不解除或降级 `valid_until_window` blocker。
- 不恢复 candidate search。
- 不批准 research-only observation、paper-shadow、scheduler、event append、outcome
  binding、production 或 broker/order path。
- 不运行新策略 backtest、不生成新 feature/signal/scoring/daily report。

## 输出

- `outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/closure_result.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/source_traceability_closure_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/upstream_artifact_closure_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/updated_source_feature_mapping.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_source_traceability_upstream_artifact_closure.md`
- `docs/research/growth_tilt_engine_source_traceability_closure_evidence.md`
- `docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md`
- `docs/research/growth_tilt_engine_updated_source_feature_mapping.md`
- `docs/research/dynamic_strategy_2418_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-source-traceability-upstream-artifact-closure`
  返回 `GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY`。
- 输出保留 2415 / 2416 readiness 结论：`source_feature_count=10`、
  `pit_gate_ready_count=0`、`contract_ready_count=0`、`pit_gate_blocked_count=10`、
  `blocked_by_source_traceability_count=5`、`blocked_by_valid_until_window_count=1`。
- 5 个 source traceability blockers 均有 closure evidence record，并区分
  `CLOSED_WITH_EVIDENCE`、`PARTIALLY_CLOSED`、`STILL_BLOCKED` 或 `NOT_APPLICABLE`。
- upstream artifact evidence 显示哪些 mapping 已补齐、哪些仍缺 artifact metadata；缺失项必须
  保留 `blocker_remains=true` 和 required action。
- updated source feature mapping 不改变 PIT gate readiness；只允许
  `UNCHANGED_PENDING_RECHECK`、`TRACEABILITY_EVIDENCE_ADDED_PENDING_RECHECK` 或
  `STILL_BLOCKED`。
- `growth_tilt_engine_blocking_gap_resolved=false`、
  `growth_tilt_engine_severity_downgraded=false`、
  `valid_until_window_blocking_gap_resolved=false`、
  `valid_until_window_severity_downgraded=false`。
- candidate search、observation、paper-shadow、event append、outcome binding、scheduler、
  production、broker 和 daily report 全部保持 false / none。
- report registry、artifact catalog、system flow、task register、completed closeout 文档和
  focused tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_source_traceability_upstream_artifact_closure.py`
- `aits research strategies growth-tilt-engine-source-traceability-upstream-artifact-closure --as-of 2026-07-08`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data --as-of 2026-07-05` 不在默认验证中运行，因为本任务只读取 prior
validated artifacts、registry 和 docs，不读取 fresh cached market/macro/features/signals，不运行
backtest/scoring/daily report。

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为 source
  traceability / upstream artifact closure evidence，不清除 blocker、不降级 severity、不恢复任何交易或观察路径。
- 2026-07-08：实现完成并归档 `DONE`。新增 reusable builder、
  `aits research strategies growth-tilt-engine-source-traceability-upstream-artifact-closure`、
  closure result、source traceability closure evidence、upstream artifact closure evidence、
  updated source feature mapping、remaining blocker summary、research docs、registry、catalog、
  system flow 和 focused tests。真实 run status=
  `GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY`；
  `source_feature_count=10`、`pit_gate_ready_count=0`、`contract_ready_count=0`、
  `pit_gate_blocked_count=10`、`blocked_by_source_traceability_count=5`、
  `blocked_by_valid_until_window_count=1`、source traceability evidence rows=5、
  pre-recheck evidence ready=4、still blocked=1；`growth_tilt_engine_signal_artifact`
  仍缺 standalone upstream signal artifact metadata，`execution_signal_validity_policy`
  的 `valid_until_window` blocker 留给 TRADING-2418。未运行 `aits validate-data`，因为本任务只读取
  prior validated artifacts/docs/registry/catalog 和 PIT input registry，不读取 fresh cached
  market data、不运行新 backtest、不生成 feature/signal/scoring/daily report 或交易建议。
