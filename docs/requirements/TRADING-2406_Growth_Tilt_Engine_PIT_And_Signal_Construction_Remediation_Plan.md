# TRADING-2406: Growth Tilt Engine PIT And Signal Construction Remediation Plan

最后更新：2026-07-07

## Context

TRADING-2405 已完成 dynamic strategy PIT coverage matrix reusable
implementation，真实状态为
`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`。2405 的
PIT gate 仍保留两个 blocking gaps：`growth_tilt_engine` 与
`valid_until_window`，并把下一步限定为
`TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan`。

本任务只处理第一个核心 blocker：`growth_tilt_engine`。它是 dynamic strategy
研究链路中当前最重要的 return engine。若 source features、as-of semantics、
signal horizon、signal confidence、valid-from / valid-until 依赖和 false risk-on
风险不能被明确审计，就不能恢复 candidate search，更不能批准
research-only observation、paper-shadow、production 或 broker path。

## Scope

This task will:

1. Read prior validated TRADING-2405 PIT gate / matrix / blocker artifacts.
2. Read prior TRADING-2403 PIT coverage matrix and signal construction review.
3. Read governed `config/research/dynamic_strategy_pit_input_registry.yaml`.
4. Inspect existing growth tilt config and execution policy config without
   refreshing cached market data or running a new strategy backtest.
5. Preserve the current `growth_tilt_engine` blocker state as unresolved.
6. Build a source feature inventory for the growth tilt engine.
7. Build a PIT risk audit covering lookahead, revision, backfill, stale signal,
   missing as-of metadata and regime confirmation risks.
8. Build signal construction gap analysis covering horizon, lookback, decay,
   confidence, valid-from, valid-until and behavior risks.
9. Produce remediation plan, severity downgrade conditions and validation plan.
10. Generate research docs and route to TRADING-2407.

## Non-Goals

- Do not clear or downgrade `growth_tilt_engine`.
- Do not mark `growth_tilt_engine` as true PIT.
- Do not resume candidate search.
- Do not approve research-only observation.
- Do not enable paper-shadow, paper trade, shadow position, scheduler, event
  append, outcome binding, daily report, production, broker or order paths.
- Do not append historical event logs or mutate outcome stores.
- Do not run a new strategy backtest.
- Do not generate a new trading signal or scoring output.

## Data Quality Gate

2406 reads prior validated 2405 / 2403 artifacts plus governed configs and source
metadata. It does not read fresh cached market data, generate technical features,
run scoring, run backtests or generate daily reports. Therefore this task does
not need to rerun `aits validate-data --as-of 2026-07-05`; outputs must state the
reason explicitly.

If implementation is expanded to consume fresh cached market data, feature
artifacts or live signal artifacts directly, it must run `aits validate-data` or
the same validation code path first and stop on failure.

## Expected Decision

Default next route:

```text
TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan
```

Reason: both current BLOCKING gaps need remediation plans before a unified
implementation task can safely change source signal semantics.

Expected safety result:

- `growth_tilt_engine_blocking_gap_resolved=false`
- `growth_tilt_engine_severity_downgraded=false`
- `candidate_search_allowed=false`
- `candidate_search_resumed=false`
- `research_only_observation_allowed=false`
- `research_only_observation_approved=false`
- `paper_shadow_allowed=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/remediation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/source_feature_inventory.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/pit_risk_audit.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/signal_construction_gap_analysis.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/severity_downgrade_conditions.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/validation_plan.json`
- `docs/research/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.md`
- `docs/research/dynamic_strategy_growth_tilt_engine_source_feature_inventory.md`
- `docs/research/dynamic_strategy_growth_tilt_engine_pit_risk_audit.md`
- `docs/research/dynamic_strategy_growth_tilt_engine_remediation_plan.md`
- `docs/research/dynamic_strategy_2407_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-growth-tilt-engine-pit-signal-remediation-plan`
  returns
  `DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_REMEDIATION_PLAN_READY`.
- Source validation confirms 2405 status and route, current 2405 PIT gate blocks
  candidate search, and `growth_tilt_engine` remains a `BLOCKING` gap.
- Source feature inventory, PIT risk audit, signal construction gap analysis,
  remediation plan, severity downgrade conditions and validation plan are ready.
- `growth_tilt_engine_blocking_gap_resolved=false` and
  `growth_tilt_engine_severity_downgraded=false`.
- Candidate search, observation, paper-shadow, scheduler, event append, outcome
  binding, daily report, production and broker fields remain false/none.
- Output route is
  `TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan`.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, docs freshness, task-register consistency, report
  contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to remediation planning and evidence inventory only.
- 2026-07-07: Implemented
  `dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.py`, the
  `aits research strategies dynamic-strategy-growth-tilt-engine-pit-signal-remediation-plan`
  CLI, research artifacts, research docs, report registry entry, artifact
  catalog row, system-flow update and focused tests. Real CLI run returned
  `DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_REMEDIATION_PLAN_READY`;
  `source_feature_inventory_count=10`, `pit_risk_audit_count=6`,
  `growth_tilt_engine_blocking_gap_resolved=false`,
  `growth_tilt_engine_severity_downgraded=false`, candidate search /
  observation / paper-shadow / production remain false, and `broker_action=none`.
- 2026-07-07: Focused validation passed:
  `python -m ruff check src\ai_trading_system\dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.py src\ai_trading_system\cli_commands\research_execution_semantics.py tests\research_strategies\test_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.py`,
  `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.py`
  with 3 passed, and real CLI run.
- 2026-07-07: Full closeout validation passed before archive update:
  `python -m ruff check .`, `python -m compileall -q src tests`, docs
  freshness PASS with 593 docs, documentation contract PASS with 1303 reports,
  task-register consistency PASS with active=320 / completed=466, latest
  task-register validation PASS, and
  `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`
  passed with 197 tests. Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260707T092001Z/test_runtime_summary.json`.
- 2026-07-07: Archived as `DONE`. Post-archive docs freshness and report
  contract still passed; task-register consistency passed with active=319 /
  completed=467, and the active-register terminal-status scan returned no
  matches.
