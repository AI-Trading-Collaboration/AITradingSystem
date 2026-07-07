# TRADING-2404: Dynamic Strategy PIT Coverage Matrix Implementation Plan

最后更新：2026-07-07

## Context

TRADING-2403 已完成 dynamic strategy PIT coverage matrix and signal construction
review，真实状态为
`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`。
2403 识别 14 行 PIT matrix 和 8 行 remediation matrix，并把
`growth_tilt_engine` 与 `valid_until_window` 标记为 `BLOCKING` gaps。

2404 的目标不是直接实现完整 PIT matrix engine，也不是修复信号，而是将 2403 的
一次性 review 固化为可复用基础设施实现方案：PIT input registry schema、PIT
coverage matrix generator contract、severity gate policy、blocking gap summary 和
remediation routes。

## Scope

This task will:

1. Read TRADING-2403 PIT coverage matrix artifacts.
2. Read TRADING-2403 remediation matrix artifacts.
3. Read TRADING-2402 data / PIT / signal quality gap review artifacts.
4. Read TRADING-2401 plateau decision artifacts.
5. Extract blocking gaps from 2403.
6. Generate PIT matrix implementation plan.
7. Design PIT input registry schema.
8. Design reusable PIT coverage matrix generator contract.
9. Design PIT severity gate contract.
10. Design remediation routing plan.
11. Output TRADING-2405 route.

## Non-Goals

- Do not implement the full reusable PIT matrix engine.
- Do not clear or downgrade blocking gaps without evidence.
- Do not run a new strategy backtest.
- Do not generate a new trading signal or scoring output.
- Do not resume strategy candidate search.
- Do not approve research-only observation for any candidate.
- Do not enable scheduler, event append, outcome binding, paper-shadow,
  paper trade, shadow position, production, broker/order, scheduled task or
  daily report paths.

## Data Quality Gate

2404 only reads prior validated TRADING-2403 / 2402 / 2401 artifacts and designs
infrastructure contracts. It does not read fresh cached market data, generate
features, run scoring, run backtests or generate daily reports. Therefore this
task does not need to rerun `aits validate-data --as-of 2026-07-05`; outputs must
state this reason explicitly.

## Expected Decision

Default next route:

```text
TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation
```

Expected safety result:

- `candidate_search_allowed=false`
- `research_only_observation_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `candidate_search_resumed=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/implementation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/pit_input_registry_schema.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/pit_gate_policy.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/remediation_routes.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/current_blocker_summary.json`
- `docs/research/dynamic_strategy_pit_coverage_matrix_implementation_plan.md`
- `docs/research/dynamic_strategy_pit_input_registry_schema.md`
- `docs/research/dynamic_strategy_pit_gate_policy.md`
- `docs/research/dynamic_strategy_pit_remediation_routes.md`
- `docs/research/dynamic_strategy_2405_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-pit-coverage-matrix-implementation-plan`
  returns `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`.
- Output reads and validates TRADING-2403 route to TRADING-2404.
- Output records blocking gaps `growth_tilt_engine` and `valid_until_window`.
- Output sets PIT matrix implementation plan, PIT input registry schema, PIT gate
  policy, remediation routes and current blocker summary readiness fields to true.
- Output sets candidate_search_allowed=false, research_only_observation_allowed=false,
  paper_shadow_allowed=false and production_allowed=false.
- Output routes to
  `TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation`.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, docs freshness, task-register consistency, report
  contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to PIT coverage matrix infrastructure planning only.
  It must not implement the reusable engine, clear blocking gaps, run strategy
  backtests, generate signals, resume candidate search or touch execution paths.
- 2026-07-07: Implemented
  `aits research strategies dynamic-strategy-pit-coverage-matrix-implementation-plan`,
  `dynamic_strategy_pit_coverage_matrix_implementation_plan.py`, focused tests,
  report registry entry, artifact catalog row and system-flow update. Real CLI
  run returned
  `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`; gate fields
  remain false for candidate search, research-only observation, paper-shadow and
  production; `broker_action=none`.
- 2026-07-07: Focused validation passed:
  `python -m ruff check src\ai_trading_system\dynamic_strategy_pit_coverage_matrix_implementation_plan.py tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_implementation_plan.py src\ai_trading_system\cli_commands\research_execution_semantics.py`,
  `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_implementation_plan.py`
  with 3 passed, and real CLI run.
- 2026-07-07: Full closeout validation passed: `python -m ruff check .`,
  `python -m compileall -q src tests`, docs freshness PASS with 591 docs,
  documentation contract PASS with 1301 reports, task-register consistency
  PASS with active=319 / completed=465, latest task-register validation PASS,
  active-register terminal-status scan returned no matches, and
  `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`
  passed with 197 tests. Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260707T083347Z/test_runtime_summary.json`.
