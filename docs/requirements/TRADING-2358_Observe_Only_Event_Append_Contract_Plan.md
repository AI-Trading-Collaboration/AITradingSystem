# TRADING-2358 Observe-Only Event Append Contract Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2358_OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 系统实现；项目 owner 后续复核 TRADING-2359 outcome binding contract plan route
- source route: `TRADING-2358_Observe_Only_Event_Append_Contract_Plan`

## Background

TRADING-2357 已完成 observe-only scheduler idempotency and replay contract plan。当前 owner decision 仍为：

```text
KEEP_DISABLED_AND_PROMOTION_BLOCKED
```

2358 的目标不是 append event，也不是批准 event mutation，而是定义 future event append 的 schema、idempotency、duplicate detection、rollback 和 owner approval requirements。任何 historical event log 写入必须保持禁用。

## Scope

Implement:

- module: `src/ai_trading_system/high_intensity_risk_cap_event_append_contract_plan.py`
- CLI: `aits research trends high-intensity-risk-cap-observe-only-event-append-contract-plan`
- output root: `outputs/research_trends/high_intensity_risk_cap_observe_only_event_append_contract_plan/`
- research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_event_append_contract_plan.md`
  - `docs/research/high_intensity_2359_outcome_binding_contract_route.md`
- focused tests: `tests/research_trends/test_high_intensity_event_append_contract_plan.py`
- registry/catalog/system flow/task register updates.

The owner attachment mentioned `docs/tasks/completed/TRADING-2358_*.md`. The current repository uses `docs/requirements/` and `docs/task_register_completed.md`; TRADING-2358 follows that existing structure.

## Required Status

The real CLI run must return:

```text
OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

Required readiness:

```text
READY_FOR_2359_WITH_CAVEATS
```

Required next route:

```text
TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan
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
paper_shadow_enabled: false
production_enabled: false
broker_action_enabled: false
promotion_allowed: false
manual_review_required: true
owner_review_required: true
owner_decision: KEEP_DISABLED_AND_PROMOTION_BLOCKED
event_append_contract_ready: true
```

Forbidden actions:

- enable scheduler;
- create cron / Windows Task / GitHub Actions schedule;
- execute real manual run;
- append or mutate historical event log;
- write event row;
- bind or mutate outcome store;
- read fresh market data;
- generate new signal, target weight, rebalance instruction, or daily report;
- run backtest;
- enter paper-shadow / production / broker path.

## Implementation Plan

1. Reuse TRADING-2357 loader to validate TRADING-2347 through TRADING-2356 artifacts and load TRADING-2357 idempotency / replay contract plan outputs.
2. Load TRADING-2357 summary, package, source review, idempotency contract, stable semantic hash contract, volatile field exclusion rule, duplicate detection plan, replay no-side-effect contract, blocked-promotion rationale, 2358 route, interpretation boundary, and safety boundary.
3. Fail closed if 2357 status, readiness, route, evidence_chain_complete, owner_decision, idempotency/replay readiness, promotion decision, promotion_allowed, safety fields, side-effect assertions, or inherited validate-data error count violates the expected contract.
4. Generate event schema append contract, append idempotency contract, duplicate detection contract, append rollback plan, and event mutation guardrail.
5. Generate blocked promotion decision, 2359 outcome binding contract route, interpretation boundary, safety boundary, summary JSON, main package JSON, and research Markdown docs.
6. Update registry, artifact catalog, system flow, task register, completed register, and focused tests.

## Acceptance Criteria

- CLI is registered and executable.
- Loader reads TRADING-2357 idempotency / replay contract artifact.
- Loader reads TRADING-2356 kill-switch plan artifact through the inherited source chain.
- Loader reads TRADING-2355 hardening backlog artifact through the inherited source chain.
- `source_tasks` includes TRADING-2347 through TRADING-2357.
- `evidence_chain_complete=true`.
- `owner_decision=KEEP_DISABLED_AND_PROMOTION_BLOCKED`.
- `event_append_contract_ready=true`.
- `event_schema_append_contract_ready=true`.
- `append_idempotency_contract_ready=true`.
- `duplicate_detection_contract_ready=true`.
- `append_rollback_plan_ready=true`.
- `event_mutation_guardrail_ready=true`.
- `promotion_decision=BLOCKED`.
- `promotion_allowed=false`.
- `event_append_enabled=false`.
- `event_append_attempted=false`.
- `historical_event_log_mutated=false`.
- Outcome binding, paper-shadow, production, and broker action remain disabled and unattempted.
- Append idempotency contract includes stable semantic hash.
- Duplicate detection contract blocks append and must not mutate an existing event.
- Rollback plan is plan-only / no-mutation.
- Next route is `TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan`.

## Validation Plan

Run:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_event_append_contract_plan.py
aits research trends high-intensity-risk-cap-observe-only-event-append-contract-plan
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for TRADING-2358 if the implementation only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 / 2356 / 2357 artifacts, does not read fresh market data, does not append event logs, does not bind outcomes, and does not generate technical features, scoring, backtest, or daily report outputs. The final closeout must state this reason explicitly.

## Progress Notes

- 2026-07-05: Added requirement and moved task into `IN_PROGRESS` before implementation. Scope is event append contract planning only; owner decision remains `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, and no scheduler enablement, manual run execution, event log mutation, outcome mutation, paper-shadow, production, or broker action is allowed.
- 2026-07-05: Implemented and moved to `DONE`. Real run generated `outputs/research_trends/high_intensity_risk_cap_observe_only_event_append_contract_plan/` and docs under `docs/research/`; status=`OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`, readiness=`READY_FOR_2359_WITH_CAVEATS`, next route=`TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan`, source tasks=`TRADING-2347..TRADING-2357`, source validation inherited `2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0. `aits validate-data` was not rerun because this task only reads prior validated 2347-2357 artifacts and does not read fresh market data, append event logs, bind outcomes, generate technical features, score, backtest, or produce daily reports.
- 2026-07-05: Final validation passed: Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs freshness 546 docs PASS, documentation contract 1255 reports PASS, task-register consistency run/validate PASS, contract-validation 197 passed (`outputs/validation_runtime/contract-validation_20260705T051746Z/test_runtime_summary.json`), and `git diff --check`.
