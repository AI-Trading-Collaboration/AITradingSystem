# TRADING-2368 Dynamic Strategy Research-Only Shadow Observation Protocol

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2368_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL`
- priority: `P0`
- status: `DONE`
- owner: system implementation; project owner follow-up review
- source route: `TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol`
- source tasks: `TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING`, `TRADING-2366_DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_ANALYSIS`, `TRADING-2367_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE`
- target status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY`
- target next route: `TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run`

## Scope

TRADING-2367 recommended `dynamic_regime_overlay_v0_4_lower_turnover` as the research-only gate candidate while keeping the gate decision at `OWNER_REVIEW_REQUIRED`. This task defines the research-only shadow observation protocol for that candidate, including observation fields, cadence, comparison baselines, owner-review triggers, and safety boundaries.

Research-only shadow observation means report-only signal and portfolio-preview observation. It does not create paper trades, shadow positions, event rows, bound outcomes, production actions, broker calls, scheduled tasks, or daily reports.

## Implementation Plan

- Add `src/ai_trading_system/dynamic_strategy_research_only_shadow_observation_protocol.py`.
- Add CLI `aits research strategies dynamic-strategy-research-only-shadow-observation-protocol`.
- Read and verify TRADING-2367 owner review gate artifacts.
- Read and verify TRADING-2366 sensitivity artifacts.
- Read and verify TRADING-2365 event-driven retest artifacts.
- Extract the primary observation candidate from the 2367 recommended gate candidate.
- Generate research-only observation protocol, field schema, cadence plan, comparison plan, guardrail plan, and review threshold plan.
- Write JSON outputs under `outputs/research_strategies/dynamic_strategy_research_only_shadow_observation_protocol/`.
- Write Chinese research docs:
  - `docs/research/dynamic_strategy_research_only_shadow_observation_protocol.md`
  - `docs/research/dynamic_strategy_shadow_observation_field_schema.md`
  - `docs/research/dynamic_strategy_shadow_observation_review_thresholds.md`
  - `docs/research/dynamic_strategy_2369_route.md`
- Update report registry, artifact catalog, system flow, task register, and completed task archive.
- Add focused tests in `tests/research_strategies/test_dynamic_strategy_research_only_shadow_observation_protocol.py`.

## Acceptance Criteria

- CLI runs and writes JSON / Markdown artifacts.
- `status=DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY`.
- Source TRADING-2367 artifact confirms `DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY` and route to TRADING-2368.
- Source TRADING-2366 artifact confirms `DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY`.
- Source TRADING-2365 artifact confirms `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`.
- Output includes `primary_observation_candidate`, `observation_protocol`, `observation_field_schema`, `review_thresholds`, `research_only_shadow_observation_allowed`, `paper_shadow_enabled=false`, `paper_trade_created=false`, `shadow_position_created=false`, `event_append_enabled=false`, `outcome_binding_enabled=false`, `scheduler_enabled=false`, `production_enabled=false`, `broker_action_enabled=false`, and `recommended_next_research_task`.
- Reports answer what research-only shadow observation records, how it differs from paper-shadow execution, why robustness top is selected over ranking top, which fields are recorded per observation, what triggers owner review, and why event/outcome/production/broker paths remain disabled.

## Safety Boundary

This task is strategy research only. It may read existing TRADING-2365 / 2366 / 2367 artifacts and generate protocol reports. It must not enable scheduler, create scheduled tasks, append historical event logs, bind outcomes, mutate outcome stores, create paper trades, create shadow positions, enable paper-shadow execution, enter production, call broker APIs, send orders, or generate daily reports.

`research_only_shadow_observation_allowed=true` is allowed only as a protocol-planning flag. It is not paper-shadow execution and does not create an observation row.

## Implementation Result

- Added `src/ai_trading_system/dynamic_strategy_research_only_shadow_observation_protocol.py`.
- Added CLI `aits research strategies dynamic-strategy-research-only-shadow-observation-protocol`.
- Added report registry entry `dynamic_strategy_research_only_shadow_observation_protocol`.
- Added artifact catalog and system flow coverage for the 2368 protocol-only branch.
- Added focused tests in `tests/research_strategies/test_dynamic_strategy_research_only_shadow_observation_protocol.py`.
- Generated:
  - `outputs/research_strategies/dynamic_strategy_research_only_shadow_observation_protocol/observation_protocol.json`
  - `outputs/research_strategies/dynamic_strategy_research_only_shadow_observation_protocol/observation_field_schema.json`
  - `outputs/research_strategies/dynamic_strategy_research_only_shadow_observation_protocol/review_thresholds.json`
  - `docs/research/dynamic_strategy_research_only_shadow_observation_protocol.md`
  - `docs/research/dynamic_strategy_shadow_observation_field_schema.md`
  - `docs/research/dynamic_strategy_shadow_observation_review_thresholds.md`
  - `docs/research/dynamic_strategy_2369_route.md`

## Decision Result

- final status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY`
- primary observation candidate: `dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top from 2365: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from 2366: `dynamic_regime_overlay_v0_4_lower_turnover`
- gate decision from 2367: `OWNER_REVIEW_REQUIRED`
- next route: `TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run`
- `data_quality_gate_executed=false` because the CLI only reads prior validated artifacts and does not read fresh cached market data, compute features, run backtests, score, or generate a daily report.
- scheduler, scheduled task, event append, outcome binding, outcome-store mutation, paper-shadow execution, paper trade, shadow position, production, broker action, order generation, and daily report generation all remain disabled / false / none.

## Validation Plan

```bash
python -m ruff check src/ai_trading_system/dynamic_strategy_research_only_shadow_observation_protocol.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_research_only_shadow_observation_protocol.py
python -m compileall src/ai_trading_system/dynamic_strategy_research_only_shadow_observation_protocol.py src/ai_trading_system/cli_commands/research_execution_semantics.py
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_research_only_shadow_observation_protocol.py
python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py
aits research strategies dynamic-strategy-research-only-shadow-observation-protocol --as-of 2026-07-05
aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md
aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json
aits reports task-register-consistency run --as-of 2026-07-05
aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for the implementation CLI because this task does not read cached market data or run a new backtest; it only reads prior artifacts that already carry data quality status from TRADING-2365 / 2366 / 2367.

## Progress Notes

- 2026-07-05: Added as `IN_PROGRESS`. TRADING-2367 completed owner review gate and routed to this research-only shadow observation protocol task. This task must define a protocol only and must not create observations, enable scheduler, event append, outcome binding, paper-shadow execution, production, or broker.
- 2026-07-05: Implemented and moved to `DONE`. Real CLI run wrote protocol, field schema, review thresholds, and 2369 route artifacts with `status=DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY`. Safety flags remained disabled / false / none. Validation passed: Ruff, compileall, focused parallel pytest 3 passed, adjacent execution semantics parallel pytest 3 passed, real CLI run, docs freshness PASS, documentation contract PASS, task-register consistency run/validate PASS, contract-validation 197 passed with runtime artifact `outputs/validation_runtime/contract-validation_20260705T105830Z/test_runtime_summary.json`, and `git diff --check`. `aits validate-data` was not rerun because 2368 reads only prior artifacts and does not read fresh cached market data or run a new backtest.
