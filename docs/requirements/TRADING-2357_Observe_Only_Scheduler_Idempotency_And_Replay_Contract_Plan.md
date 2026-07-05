# TRADING-2357 Observe-Only Scheduler Idempotency And Replay Contract Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2357_OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 项目 owner 后续复核 TRADING-2358 event append contract plan route
- source route: `TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan`

## Background

TRADING-2356 已完成 observe-only scheduler kill-switch and disabled-enforcement evidence plan。当前 owner decision 仍为：

```text
KEEP_DISABLED_AND_PROMOTION_BLOCKED
```

2357 的目标不是启用 scheduler，也不执行真实 replay validation，而是定义 future scheduler-like dry-run / manual-run dry-run / artifact replay 的 idempotency and replay contract。该合同约束 stable semantic fields、volatile runtime fields 排除规则、semantic hash 计算、duplicate detection、replay no-side-effect 证据和下一步 event append contract route。

## Scope

Implement:

- module: `src/ai_trading_system/high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan.py`
- CLI: `aits research trends high-intensity-risk-cap-observe-only-scheduler-idempotency-replay-contract-plan`
- output root: `outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan/`
- research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan.md`
  - `docs/research/high_intensity_2358_event_append_contract_route.md`
- focused tests: `tests/research_trends/test_high_intensity_scheduler_idempotency_replay_contract_plan.py`
- registry/catalog/system flow/task register updates.

The owner attachment mentioned `docs/tasks/completed/TRADING-2357_*.md`. The current repository uses `docs/requirements/` and `docs/task_register_completed.md`; TRADING-2357 follows that existing structure.

## Required Status

The real CLI run must return:

```text
OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

Required readiness:

```text
READY_FOR_2358_WITH_CAVEATS
```

Required next route:

```text
TRADING-2358_Observe_Only_Event_Append_Contract_Plan
```

## Safety Boundary

All generated artifacts must keep:

```yaml
scheduler_enabled: false
manual_run_only: true
dry_run_only: true
manual_run_executed: false
event_append_enabled: false
outcome_binding_enabled: false
paper_shadow_enabled: false
production_enabled: false
broker_action_enabled: false
promotion_allowed: false
manual_review_required: true
owner_review_required: true
owner_decision: KEEP_DISABLED_AND_PROMOTION_BLOCKED
idempotency_contract_ready: true
replay_contract_ready: true
```

Forbidden actions:

- enable scheduler;
- create cron / Windows Task / GitHub Actions schedule;
- execute real manual run;
- append or mutate event log;
- bind or mutate outcome store;
- read fresh market data;
- generate new signal, target weight, rebalance instruction, or daily report;
- run backtest;
- enter paper-shadow / production / broker path.

## Implementation Plan

1. Reuse TRADING-2356 loader to validate TRADING-2347 through TRADING-2355 artifacts and load TRADING-2356 kill-switch plan outputs.
2. Load TRADING-2356 summary, package, source review, kill-switch contract, disabled enforcement matrix, no-real-scheduler assertions, manual-review-required assertions, blocked-promotion rationale, 2357 route, interpretation boundary, and safety boundary.
3. Fail closed if 2356 status, readiness, route, evidence_chain_complete, owner_decision, kill-switch readiness, disabled enforcement readiness, no-real-scheduler assertions, manual-review-required assertions, promotion decision, promotion_allowed, safety fields, side-effect assertions, or inherited validate-data error count violates the expected contract.
4. Generate stable semantic hash contract, including semantic fields and a rule that full JSON byte-level equality must not be used.
5. Generate volatile field exclusion rule covering generated timestamps, runtime artifacts, durations, paths, host/machine/process metadata, and git dirty-state metadata.
6. Generate duplicate detection plan and replay no-side-effect contract.
7. Generate blocked promotion decision, 2358 event append contract route, interpretation boundary, safety boundary, summary JSON, main package JSON, and research Markdown docs.
8. Update registry, artifact catalog, system flow, task register, completed register, and focused tests.

## Acceptance Criteria

- CLI is registered and executable.
- Loader reads TRADING-2356 kill-switch plan artifact.
- Loader reads TRADING-2355 hardening backlog artifact through the inherited source chain.
- Loader reads TRADING-2354 gap closure plan artifact through the inherited source chain.
- `source_tasks` includes TRADING-2347 through TRADING-2356.
- `evidence_chain_complete=true`.
- `owner_decision=KEEP_DISABLED_AND_PROMOTION_BLOCKED`.
- `idempotency_contract_ready=true`.
- `stable_semantic_hash_contract_ready=true`.
- `volatile_field_exclusion_rule_ready=true`.
- `duplicate_detection_plan_ready=true`.
- `replay_no_side_effect_contract_ready=true`.
- `promotion_decision=BLOCKED`.
- `promotion_allowed=false`.
- `scheduler_enabled=false`.
- `manual_run_only=true`.
- `dry_run_only=true`.
- `manual_run_executed=false`.
- Event append, outcome binding, paper-shadow, production, and broker action remain disabled and unattempted.
- Stable semantic fields do not overlap volatile runtime fields.
- Volatile exclusion rule covers timestamp, runtime artifact, duration, and path fields.
- Duplicate detection plan has `side_effect_allowed=false`.
- Replay contract explicitly forbids scheduler creation, manual run execution, event append, outcome binding, paper-shadow, production, and broker calls.
- Next route is `TRADING-2358_Observe_Only_Event_Append_Contract_Plan`.

## Validation Plan

Run:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_scheduler_idempotency_replay_contract_plan.py
aits research trends high-intensity-risk-cap-observe-only-scheduler-idempotency-replay-contract-plan
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for TRADING-2357 if the implementation only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 / 2356 artifacts, does not read fresh market data, does not append event logs, does not bind outcomes, and does not generate technical features, scoring, backtest, or daily report outputs. The final closeout must state this reason explicitly.

## Progress Notes

- 2026-07-05: Added requirement and moved task into `IN_PROGRESS` before implementation. Scope is idempotency and replay contract planning only; owner decision remains `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, and no scheduler enablement, manual run execution, event/outcome mutation, paper-shadow, production, or broker action is allowed.
- 2026-07-05: Implementation complete and archived as `DONE`. Real CLI run wrote the idempotency contract, stable semantic hash contract, volatile field exclusion rule, duplicate detection plan, replay no-side-effect contract, blocked-promotion rationale, safety boundary, and TRADING-2358 event append contract route with status `OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`, readiness `READY_FOR_2358_WITH_CAVEATS`, owner decision `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, `promotion_allowed=false`, `scheduler_enabled=false`, `manual_run_executed=false`, event/outcome/paper-shadow/production/broker attempted flags false, and next route `TRADING-2358_Observe_Only_Event_Append_Contract_Plan`. Validation passed Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs freshness 545 docs PASS, documentation contract 1254 reports PASS, task-register consistency run/validate PASS, and contract-validation 197 passed (`outputs/validation_runtime/contract-validation_20260705T045158Z/test_runtime_summary.json`). `aits validate-data` was not rerun because this task only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 / 2356 research artifacts and does not read fresh market data, append events, bind outcomes, or generate technical features, scoring, backtest, or daily report outputs.
