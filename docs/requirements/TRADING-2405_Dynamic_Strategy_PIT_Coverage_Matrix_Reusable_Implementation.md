# TRADING-2405: Dynamic Strategy PIT Coverage Matrix Reusable Implementation

最后更新：2026-07-07

## Context

TRADING-2404 已完成 dynamic strategy PIT coverage matrix implementation plan，
真实状态为
`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`，并把下一步限定为
`TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation`。

2405 的目标是将 2404 的 plan-only infrastructure contract 落成可复用基础设施：
PIT input registry、registry-backed PIT coverage matrix generator、PIT severity
gate checker、blocker summary 和 remediation matrix。当前 blocking gaps 仍为
`growth_tilt_engine` 与 `valid_until_window`，不得在本任务中清除或降级。

## Scope

This task will:

1. Add `config/research/dynamic_strategy_pit_input_registry.yaml`.
2. Add reusable research-quality modules for PIT input registry loading and schema
   validation.
3. Add reusable PIT coverage matrix generation from registry entries.
4. Add reusable PIT severity gate checker.
5. Add reusable blocker summary and remediation matrix output.
6. Add a dynamic-strategy wrapper command:
   `aits research strategies dynamic-strategy-pit-coverage-matrix-generate`.
7. Read prior validated TRADING-2404 and TRADING-2403 artifacts for source
   validation and continuity.
8. Generate research-strategy and research-quality artifacts.
9. Generate research docs and route to TRADING-2406.

## Non-Goals

- Do not fix `growth_tilt_engine`.
- Do not fix `valid_until_window`.
- Do not clear or downgrade blocking gaps without evidence.
- Do not run a new strategy backtest.
- Do not generate a new trading signal or scoring output.
- Do not resume strategy candidate search.
- Do not approve research-only observation for any candidate.
- Do not enable scheduler, event append, outcome binding, paper-shadow,
  paper trade, shadow position, production, broker/order, scheduled task or
  daily report paths.

## Data Quality Gate

2405 reads prior validated TRADING-2404 / 2403 artifacts and a governed registry
config. It does not read fresh cached market data, generate features, run scoring,
run backtests or generate daily reports. Therefore this task does not need to
rerun `aits validate-data --as-of 2026-07-05`; outputs must state this reason
explicitly.

If implementation is expanded to refresh or consume cached market data directly,
the same validation code path as `aits validate-data` must run first and stop on
failure.

## Expected Decision

Default next route:

```text
TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan
```

Reason: `growth_tilt_engine` is the core return-engine blocking PIT gap.

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

Strategy-level outputs:

- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/implementation_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_input_registry_snapshot.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_coverage_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_gate_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_blocker_summary.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_remediation_routes.json`

Reusable research-quality outputs:

- `outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_coverage_matrix.json`
- `outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_gate_result.json`
- `outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_blocker_summary.json`
- `outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_remediation_matrix.json`

Docs:

- `docs/research/dynamic_strategy_pit_coverage_matrix_reusable_implementation.md`
- `docs/research/dynamic_strategy_pit_input_registry.md`
- `docs/research/dynamic_strategy_pit_gate_result.md`
- `docs/research/dynamic_strategy_pit_remediation_routes.md`
- `docs/research/dynamic_strategy_2406_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-pit-coverage-matrix-generate`
  returns `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`.
- Registry YAML loads and schema validation passes.
- PIT matrix generator outputs rows from registry entries.
- PIT gate checker outputs candidate_search_allowed=false,
  research_only_observation_allowed=false, paper_shadow_allowed=false and
  production_allowed=false.
- Blocking gaps include `growth_tilt_engine` and `valid_until_window`.
- Gate rationale states this is a policy-derived safety gate, not a statistically
  calibrated empirical threshold.
- Outputs route to
  `TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan`.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, docs freshness, task-register consistency, report
  contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to reusable PIT matrix / gate infrastructure only.
- 2026-07-07: Implemented
  `config/research/dynamic_strategy_pit_input_registry.yaml`, reusable
  `research_quality` PIT registry / matrix / gate modules,
  `dynamic_strategy_pit_coverage_matrix_reusable_implementation.py`, the
  `aits research strategies dynamic-strategy-pit-coverage-matrix-generate` CLI,
  research-quality artifacts, research docs, report registry entry, artifact
  catalog row and system-flow update. Real CLI run returned
  `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`; gate
  fields remain false for candidate search, research-only observation,
  paper-shadow and production; `broker_action=none`.
- 2026-07-07: Focused validation passed:
  `python -m ruff check src\ai_trading_system\research_quality\pit_input_registry.py src\ai_trading_system\research_quality\pit_coverage_matrix.py src\ai_trading_system\research_quality\pit_coverage_gate.py src\ai_trading_system\dynamic_strategy_pit_coverage_matrix_reusable_implementation.py tests\research_quality\test_pit_coverage_matrix.py tests\research_quality\test_pit_coverage_gate.py tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_reusable_implementation.py src\ai_trading_system\cli_commands\research_execution_semantics.py`,
  `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_reusable_implementation.py tests\research_quality\test_pit_coverage_matrix.py tests\research_quality\test_pit_coverage_gate.py`
  with 7 passed, and real CLI run.
- 2026-07-07: Full closeout validation passed: `python -m ruff check .`,
  `python -m compileall -q src tests`, docs freshness PASS with 592 docs,
  documentation contract PASS with 1302 reports, task-register consistency
  PASS with active=319 / completed=466, latest task-register validation PASS,
  active-register terminal-status scan returned no matches, and
  `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`
  passed with 197 tests. Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260707T090213Z/test_runtime_summary.json`.
