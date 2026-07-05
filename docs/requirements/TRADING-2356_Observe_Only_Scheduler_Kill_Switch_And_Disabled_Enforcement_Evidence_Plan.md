# TRADING-2356 Observe-Only Scheduler Kill-Switch And Disabled-Enforcement Evidence Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2356_OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_ENFORCEMENT_EVIDENCE_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 系统实现；项目 owner 后续复核 TRADING-2357 scheduler idempotency and replay contract plan route
- source route: `TRADING-2356_Observe_Only_Scheduler_Kill_Switch_And_Disabled_Enforcement_Evidence_Plan`

## Background

TRADING-2355 已完成 observe-only scheduler hardening backlog and evidence matrix。当前 owner decision 仍为：

```text
KEEP_DISABLED_AND_PROMOTION_BLOCKED
```

2356 的目标不是启用 scheduler，而是在未来任何 scheduler enablement 讨论之前，先定义 kill-switch、disabled-by-default enforcement、no-real-scheduler creation assertions 和 manual-review-required assertions 的证据计划。

## Scope

Implement:

- module: `src/ai_trading_system/high_intensity_risk_cap_scheduler_kill_switch_plan.py`
- CLI: `aits research trends high-intensity-risk-cap-observe-only-scheduler-kill-switch-plan`
- output root: `outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan/`
- research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.md`
  - `docs/research/high_intensity_2357_scheduler_idempotency_route.md`
- focused tests: `tests/research_trends/test_high_intensity_scheduler_kill_switch_plan.py`
- registry/catalog/system flow/task register updates.

The owner attachment mentioned `docs/tasks/completed/TRADING-2356_*.md`. The current repository uses `docs/requirements/` and `docs/task_register_completed.md`; TRADING-2356 follows that existing structure.

## Required Status

The real CLI run must return:

```text
OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_ENFORCEMENT_EVIDENCE_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

Required readiness:

```text
READY_FOR_2357_WITH_CAVEATS
```

Required next route:

```text
TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan
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
kill_switch_contract_ready: true
disabled_enforcement_evidence_plan_ready: true
```

Forbidden actions:

- enable scheduler;
- create cron / Windows Task / GitHub Actions schedule;
- execute manual run;
- append or mutate event log;
- bind or mutate outcome store;
- read fresh market data;
- generate new signal, target weight, rebalance instruction, or daily report;
- run backtest;
- enter paper-shadow / production / broker path.

## Implementation Plan

1. Reuse TRADING-2355 loader to validate TRADING-2347 through TRADING-2354 artifacts and load TRADING-2355 hardening backlog outputs.
2. Load TRADING-2355 summary, package, source review, backlog items, evidence matrix, blocked-promotion rationale, 2356 route, interpretation boundary, and safety boundary.
3. Fail closed if 2355 status, readiness, route, evidence_chain_complete, owner_decision, backlog/evidence readiness, promotion decision, promotion_allowed, safety fields, side-effect assertions, or inherited validate-data error count violates the expected contract.
4. Generate kill-switch contract covering scheduler global switch, manual-run switch, mutation switch, and trading-path switch.
5. Generate disabled-by-default enforcement matrix covering config default, CLI entry, runtime override, environment override, and artifact replay.
6. Generate no-real-scheduler creation assertion plan and manual-review-required assertion plan.
7. Generate blocked promotion decision, 2357 route, interpretation boundary, safety boundary, summary JSON, main package JSON, and research Markdown docs.
8. Update registry, artifact catalog, system flow, task register, completed register, and focused tests.

## Acceptance Criteria

- CLI is registered and executable.
- Loader reads TRADING-2355 hardening backlog artifact.
- Loader reads TRADING-2354 gap closure plan artifact.
- Loader reads TRADING-2353 owner review decision artifact through the inherited source chain.
- `source_tasks` includes TRADING-2347 through TRADING-2355.
- `evidence_chain_complete=true`.
- `owner_decision=KEEP_DISABLED_AND_PROMOTION_BLOCKED`.
- `kill_switch_contract_ready=true`.
- `disabled_enforcement_evidence_plan_ready=true`.
- `no_real_scheduler_creation_assertions_ready=true`.
- `manual_review_required_assertions_ready=true`.
- `promotion_decision=BLOCKED`.
- `promotion_allowed=false`.
- `scheduler_enabled=false`.
- `manual_run_only=true`.
- `dry_run_only=true`.
- `manual_run_executed=false`.
- Event append, outcome binding, paper-shadow, production, and broker action remain disabled and unattempted.
- Kill-switch contract covers scheduler, manual-run, mutation, and trading path.
- Disabled enforcement matrix covers config, CLI, runtime override, environment override, and artifact replay.
- Every enforcement matrix item has `side_effect_allowed=false`.
- Next route is `TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan`.

## Validation Plan

Run:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_scheduler_kill_switch_plan.py
aits research trends high-intensity-risk-cap-observe-only-scheduler-kill-switch-plan
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for TRADING-2356 if the implementation only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 artifacts, does not read fresh market data, does not append event logs, does not bind outcomes, and does not generate technical features, scoring, backtest, or daily report outputs. The final closeout must state this reason explicitly.

## Progress Notes

- 2026-07-05: Added requirement and moved task into `IN_PROGRESS` before implementation. Scope is kill-switch and disabled-enforcement evidence planning only; owner decision remains `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, and no scheduler enablement, event/outcome mutation, paper-shadow, production, or broker action is allowed.
- 2026-07-05: Implementation complete and archived as `DONE`. Real CLI run wrote the kill-switch contract, disabled enforcement matrix, no-real-scheduler creation assertion plan, manual-review-required assertion plan, blocked-promotion rationale, safety boundary, and TRADING-2357 scheduler idempotency route with status `OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_ENFORCEMENT_EVIDENCE_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`, readiness `READY_FOR_2357_WITH_CAVEATS`, owner decision `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, `promotion_allowed=false`, `scheduler_enabled=false`, `manual_run_executed=false`, event/outcome/paper-shadow/production/broker attempted flags false, and next route `TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan`. Validation passed Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs freshness 544 docs PASS, documentation contract 1253 reports PASS, task-register consistency run/validate PASS, and contract-validation 197 passed (`outputs/validation_runtime/contract-validation_20260705T042635Z/test_runtime_summary.json`). `aits validate-data` was not rerun because this task only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 research artifacts and does not read fresh market data, append events, bind outcomes, or generate technical features, scoring, backtest, or daily report outputs.
