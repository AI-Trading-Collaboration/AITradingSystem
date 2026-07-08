# TRADING-2416 Growth Tilt Engine PIT Gate Remaining Blocker Closure Plan

最后更新：2026-07-08

## 状态

- 任务登记：`TRADING-2416_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统实现
- 日期：2026-07-08

## 背景

TRADING-2415 已生成 growth tilt engine PIT gate readiness snapshot，真实状态为
`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED`。
该 snapshot 明确保留：

- `source_feature_count=10`
- `pit_gate_ready_count=0`
- `contract_ready_count=0`
- `pit_gate_blocked_count=10`
- `blocked_by_source_traceability_count=5`
- `blocked_by_valid_until_window_count=1`

2415 不是 blocker closure，也不是 blocker downgrade。TRADING-2416 的目标是把 2415
暴露出的 remaining blockers 拆成后续可执行 closure plan，并为 TRADING-2417～2420
排序。

本任务完整 task id 与已归档的
`TRADING-2416_DAILY_INCREMENTAL_REFACTOR_GROWTH_TILT_REMEDIATION_REPORT_HELPERS`
不同；这里按 owner 附件继续使用 TRADING-2416 作为 growth tilt PIT gate closure plan
路线编号。

## 范围

允许：

- 读取 TRADING-2415 PIT gate readiness snapshot、matrix、validation 和 remaining blocker
  summary。
- 读取 TRADING-2410～2414 prior validated artifacts、report registry、artifact catalog 和
  `config/research/dynamic_strategy_pit_input_registry.yaml`。
- 生成 remaining blocker matrix、source traceability closure plan、as-of evidence closure
  plan、valid-until dependency closure plan、PIT gate evidence requirements 和 2417 route。
- 更新 report registry、artifact catalog、system flow、task register 和 completed task
  closeout 文档。

禁止：

- 不标记任何 source feature 为 PIT gate ready 或 contract ready。
- 不解除或降级 `growth_tilt_engine` / `valid_until_window` blocker。
- 不恢复 candidate search。
- 不批准 research-only observation、paper-shadow、scheduler、event append、outcome
  binding、production 或 broker/order path。
- 不运行新策略 backtest、不生成新 feature/signal/scoring/daily report。

## 输出

- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/closure_plan_result.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/remaining_blocker_matrix.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/source_traceability_closure_plan.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/as_of_evidence_closure_plan.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/valid_until_dependency_closure_plan.json`
- `outputs/research_strategies/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan/pit_gate_evidence_requirements.json`
- `docs/research/growth_tilt_engine_pit_gate_remaining_blocker_closure_plan.md`
- `docs/research/growth_tilt_engine_remaining_blocker_matrix.md`
- `docs/research/growth_tilt_engine_source_traceability_closure_plan.md`
- `docs/research/growth_tilt_engine_valid_until_dependency_closure_plan.md`
- `docs/research/dynamic_strategy_2417_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-pit-gate-remaining-blocker-closure-plan`
  返回 `GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`。
- 输出保留 2415 readiness 结论：`source_feature_count=10`、
  `pit_gate_ready_count=0`、`contract_ready_count=0`、`pit_gate_blocked_count=10`、
  `blocked_by_source_traceability_count=5`、`blocked_by_valid_until_window_count=1`。
- five source traceability blockers、one valid-until blocker、as-of contract gaps、upstream
  artifact gaps 和 PIT gate evidence gaps 均有 closure evidence requirement。
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
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan.py`
- `aits research strategies growth-tilt-engine-pit-gate-remaining-blocker-closure-plan --as-of 2026-07-08`
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

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为 remaining
  blocker closure plan，不清除 blocker、不降级 severity、不恢复任何交易或观察路径。
- 2026-07-08：实现完成并归档 `DONE`。新增 closure plan builder、CLI、JSON/Markdown
  artifacts、report registry、artifact catalog、system flow 和 focused tests；真实 CLI run 返回
  `GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`，所有 blocker /
  safety gates 保持 unresolved / false / none。验证通过 Ruff、compileall、focused parallel
  pytest、真实 CLI、docs freshness、documentation contract、task-register consistency
  run/validate、contract-validation 和 `git diff --check`。
