# TRADING-2418 Valid Until Window Dependency Evidence Closure

最后更新：2026-07-08

## 状态

- 任务登记：`TRADING-2418_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统实现
- 日期：2026-07-08

## 背景

TRADING-2417 已完成 `growth_tilt_engine` source traceability / upstream artifact
closure，真实状态为
`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY`。
该任务只生成 pre-recheck evidence，未标记任何 source feature 为 PIT gate ready
或 contract ready。

当前必须保留的真实状态：

- `source_feature_count=10`
- `pit_gate_ready_count=0`
- `contract_ready_count=0`
- `pit_gate_blocked_count=10`
- `blocked_by_source_traceability_count=5`
- `blocked_by_valid_until_window_count=1`
- `source_traceability_pre_recheck_ready_count_from_2417=4`
- `source_traceability_still_blocked_from_2417=["growth_tilt_engine_signal_artifact"]`

TRADING-2418 的目标是把 `execution_signal_validity_policy` 这一条
`valid_until_window` dependency blocker 转成可审计 evidence，为 TRADING-2419 PIT gate
readiness recheck 提供输入。它不是 readiness 变更、blocker downgrade 或交易执行任务。

## 范围

允许：

- 读取 TRADING-2417 closure result、source traceability closure evidence、upstream artifact
  closure evidence、updated source feature mapping 和 remaining blocker summary。
- 读取 TRADING-2416 closure plan result、remaining blocker matrix、valid-until dependency
  closure plan 和 PIT gate evidence requirements。
- 读取 TRADING-2415 PIT gate readiness snapshot / matrix。
- 读取 TRADING-2414 signal validity dependency remediation artifacts。
- 读取 TRADING-2411 contract gap remediation artifacts。
- 读取 TRADING-2407 valid-until semantics review、stale signal risk audit、signal validity
  contract plan、growth tilt alignment review 和 validation plan。
- 读取 `config/research/dynamic_strategy_pit_input_registry.yaml`、
  `config/research/strategy_execution_policy_registry.yaml`、report registry 和 artifact catalog。
- 生成 valid-until dependency evidence、signal validity contract evidence、stale signal policy
  evidence、growth tilt / valid-until alignment evidence、remaining blocker summary、
  research docs 和 TRADING-2419 route。

禁止：

- 不标记任何 source feature 为 PIT gate ready 或 contract ready。
- 不解除或降级 `growth_tilt_engine` blocker。
- 不解除或降级 `valid_until_window` blocker。
- 不恢复 candidate search。
- 不批准 research-only observation、paper-shadow、scheduler、event append、outcome
  binding、production 或 broker/order path。
- 不运行新策略 backtest、不生成新 feature/signal/scoring/daily report。

## 输出

- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/closure_result.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/signal_validity_contract_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/stale_signal_policy_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/growth_tilt_valid_until_alignment_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_valid_until_dependency_evidence_closure.md`
- `docs/research/growth_tilt_engine_signal_validity_contract_evidence.md`
- `docs/research/growth_tilt_engine_stale_signal_policy_evidence.md`
- `docs/research/growth_tilt_engine_valid_until_alignment_evidence.md`
- `docs/research/dynamic_strategy_2419_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure`
  返回 `GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`。
- 输出明确 `valid_until_window_dependency_blocker_count_from_2415=1`，并定位到
  `execution_signal_validity_policy`。
- valid-until dependency evidence 覆盖 `valid_from`、`valid_until`、`stale_after`、
  `expiry_rule`、`carry_forward_rule`、`signal_to_execution_lag_rule` 和
  `near_expiry_rule` 的来源。
- signal validity contract evidence 覆盖 required fields，并明确哪些字段来自 policy
  plan、哪些仍需后续 signal artifact / PIT recheck 验证。
- stale signal policy evidence 明确：
  `expired_signal_cannot_trigger_new_trade=true`、
  `missing_valid_until_blocks_dependent_strategy_recheck=true`、
  `carry_forward_requires_explicit_rule=true`、
  `owner_review_required_for_carry_forward_in_observation_context=true`。
- growth tilt / valid-until alignment evidence 明确 horizon / expiry / confidence / volatility /
  recovery state 的可追溯状态，并保留
  `growth_tilt_engine_signal_artifact` source traceability blocker。
- 输出保留 2415 / 2416 / 2417 readiness 结论：`pit_gate_ready_count=0`、
  `contract_ready_count=0`、`pit_gate_blocked_count=10`、
  `blocked_by_valid_until_window_count=1`。
- `growth_tilt_engine_blocking_gap_resolved=false`、
  `growth_tilt_engine_severity_downgraded=false`、
  `valid_until_window_blocking_gap_resolved=false`、
  `valid_until_window_severity_downgraded=false`。
- candidate search、observation、paper-shadow、event append、outcome binding、scheduler、
  production、broker 和 daily report 全部保持 false / none。
- next route 为 `TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck`。
- report registry、artifact catalog、system flow、task register、completed closeout 文档和
  focused tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_valid_until_dependency_evidence_closure.py`
- `aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure --as-of 2026-07-08`
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

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为
  `valid_until_window` dependency evidence closure，不清除 blocker、不降级 severity、
  不恢复任何交易或观察路径。
- 2026-07-08：实现完成并归档 `DONE`。新增 reusable builder、
  `aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure`、
  6 个 JSON artifacts、5 个 research docs、registry/catalog/system flow 更新和 focused tests。
  真实 CLI status=`GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`，
  `valid_until_dependency_evidence_ready=true`、`signal_validity_contract_evidence_ready=true`、
  `stale_signal_policy_evidence_ready=true`、`growth_tilt_valid_until_alignment_evidence_ready=true`，
  且 `pit_gate_ready_count=0`、`contract_ready_count=0`、
  `growth_tilt_engine_signal_artifact` 仍保留 source traceability blocker。
