# TRADING-2359 Observe-Only Outcome Binding Contract Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2359_OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 系统实现；项目 owner 后续复核 TRADING-2360 paper-shadow scope and no-broker guardrail plan route
- source route: `TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan`

## Background

TRADING-2358 已完成 observe-only event append contract plan。当前 owner decision 仍为：

```text
KEEP_DISABLED_AND_PROMOTION_BLOCKED
```

2359 的目标不是 bind outcome，也不是批准 outcome store mutation，而是定义 future outcome binding 的 schema、idempotency、replay、rollback 和 owner approval requirements。任何 outcome store 写入、outcome row mutation 或 event/outcome reference mutation 必须保持禁用。

## Scope

Implement:

- module: `src/ai_trading_system/high_intensity_risk_cap_outcome_binding_contract_plan.py`
- CLI: `aits research trends high-intensity-risk-cap-observe-only-outcome-binding-contract-plan`
- output root: `outputs/research_trends/high_intensity_risk_cap_observe_only_outcome_binding_contract_plan/`
- research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.md`
  - `docs/research/high_intensity_2360_paper_shadow_scope_route.md`
- focused tests: `tests/research_trends/test_high_intensity_outcome_binding_contract_plan.py`
- registry/catalog/system flow/task register updates.

The owner attachment mentioned `docs/tasks/completed/TRADING-2359_*.md`. The current repository uses `docs/requirements/` and `docs/task_register_completed.md`; TRADING-2359 follows that existing structure.

## Required Status

The real CLI run must return:

```text
OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

Required readiness:

```text
READY_FOR_2360_WITH_CAVEATS
```

Required next route:

```text
TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan
```

## Safety Boundary

All generated artifacts must keep:

```yaml
scheduler_enabled: false
manual_run_only: true
dry_run_only: true
manual_run_executed: false
event_append_enabled: false
event_append_attempted: false
historical_event_log_mutated: false
outcome_binding_enabled: false
outcome_binding_attempted: false
outcome_store_mutated: false
paper_shadow_enabled: false
production_enabled: false
broker_action_enabled: false
promotion_allowed: false
manual_review_required: true
owner_review_required: true
owner_decision: KEEP_DISABLED_AND_PROMOTION_BLOCKED
outcome_binding_contract_ready: true
```

Forbidden actions:

- enable scheduler;
- create cron / Windows Task / GitHub Actions schedule;
- execute real manual run;
- append or mutate historical event log;
- write or mutate event rows;
- bind outcomes;
- write or mutate outcome rows;
- mutate outcome store;
- update outcome references;
- read fresh market data;
- generate new signal, target weight, rebalance instruction, or daily report;
- run backtest;
- enter paper-shadow / production / broker path.

## Implementation Plan

1. Reuse TRADING-2358 loader to validate TRADING-2347 through TRADING-2357 artifacts and load TRADING-2358 event append contract plan outputs.
2. Load TRADING-2358 summary, package, source review, event append contract, event schema append contract, append idempotency contract, duplicate detection contract, append rollback plan, event mutation guardrail, blocked-promotion rationale, 2359 route, interpretation boundary, and safety boundary.
3. Fail closed if 2358 status, readiness, route, evidence_chain_complete, owner_decision, contract readiness, promotion decision, promotion_allowed, safety fields, side-effect assertions, or inherited validate-data error count violates the expected contract.
4. Generate outcome binding schema contract, binding idempotency contract, binding replay contract, outcome store mutation guardrail, and binding rollback plan.
5. Generate blocked promotion decision, 2360 paper-shadow scope route, interpretation boundary, safety boundary, summary JSON, main package JSON, and research Markdown docs.
6. Update registry, artifact catalog, system flow, task register, completed register, and focused tests.

## Acceptance Criteria

- CLI is registered and executable.
- Loader reads TRADING-2358 event append contract artifact.
- Loader reads TRADING-2357 idempotency / replay contract artifact through the inherited source chain.
- Loader reads TRADING-2356 kill-switch plan artifact through the inherited source chain.
- `source_tasks` includes TRADING-2347 through TRADING-2358.
- `evidence_chain_complete=true`.
- `owner_decision=KEEP_DISABLED_AND_PROMOTION_BLOCKED`.
- `outcome_binding_contract_ready=true`.
- `outcome_binding_schema_contract_ready=true`.
- `binding_idempotency_contract_ready=true`.
- `binding_replay_contract_ready=true`.
- `outcome_store_mutation_guardrail_ready=true`.
- `binding_rollback_plan_ready=true`.
- `promotion_decision=BLOCKED`.
- `promotion_allowed=false`.
- `outcome_binding_enabled=false`.
- `outcome_binding_attempted=false`.
- `outcome_store_mutated=false`.
- Event append, historical event log mutation, paper-shadow, production, and broker action remain disabled and unattempted.
- Binding idempotency contract includes stable semantic hash.
- Binding replay contract blocks all side effects.
- Rollback plan is plan-only / no-mutation.
- Next route is `TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan`.

## Validation Plan

Run:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_outcome_binding_contract_plan.py
aits research trends high-intensity-risk-cap-observe-only-outcome-binding-contract-plan
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for TRADING-2359 if the implementation only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 / 2356 / 2357 / 2358 artifacts, does not read fresh market data, does not append event logs, does not bind outcomes, does not mutate outcome store, and does not generate technical features, scoring, backtest, or daily report outputs. The final closeout must state this reason explicitly.

## Progress Notes

- 2026-07-05: Added requirement and moved task into `IN_PROGRESS` before implementation. Scope is outcome binding contract planning only; owner decision remains `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, and no scheduler enablement, manual run execution, event log mutation, outcome binding/store mutation, paper-shadow, production, or broker action is allowed.
- 2026-07-05: Implementation completed and moved to `DONE`. Real CLI run returned status=`OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`, readiness=`READY_FOR_2360_WITH_CAVEATS`, next route=`TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan`, promotion_allowed=false, paper_shadow_allowed=false, production_allowed=false, and broker_action=`none`. Validation passed Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs freshness 547 docs PASS, documentation contract 1256 reports PASS, task-register consistency run/validate PASS, and contract-validation 197 passed with runtime artifact `outputs/validation_runtime/contract-validation_20260705T054859Z/test_runtime_summary.json`.
