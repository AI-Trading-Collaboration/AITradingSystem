# TRADING-2360 Observe-Only Paper-Shadow Scope And No-Broker Guardrail Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2360_OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 系统实现；项目 owner 后续复核 TRADING-2361 production and broker hard-blocker plan route
- source route: `TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan`

## Background

TRADING-2359 已完成 observe-only outcome binding contract plan，真实状态为：

```text
OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

2359 下一步 route 为：

```text
TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan
```

当前 owner decision 仍为：

```text
KEEP_DISABLED_AND_PROMOTION_BLOCKED
```

2360 的目标不是开启 paper-shadow，也不是批准 broker safety。它只定义 future paper-shadow scope、no-broker guardrail、daily review fields、owner approval requirement 和 blocked promotion rationale。

## Scope

Implement:

- module: `src/ai_trading_system/high_intensity_risk_cap_paper_shadow_scope_plan.py`
- CLI: `aits research trends high-intensity-risk-cap-observe-only-paper-shadow-scope-plan`
- output root: `outputs/research_trends/high_intensity_risk_cap_observe_only_paper_shadow_scope_plan/`
- research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.md`
  - `docs/research/high_intensity_2361_production_broker_hard_blocker_route.md`
- focused tests: `tests/research_trends/test_high_intensity_paper_shadow_scope_plan.py`
- registry/catalog/system flow/task register updates.

The owner attachment mentioned `docs/tasks/completed/TRADING-2360_*.md`. The current repository uses `docs/requirements/` and `docs/task_register_completed.md`; TRADING-2360 follows that existing structure.

## Required Status

The real CLI run must return:

```text
OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED
```

Required readiness:

```text
READY_FOR_2361_WITH_CAVEATS
```

Required next route:

```text
TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan
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
paper_shadow_attempted: false
production_enabled: false
broker_action_enabled: false
broker_action_attempted: false
promotion_allowed: false
manual_review_required: true
owner_review_required: true
owner_decision: KEEP_DISABLED_AND_PROMOTION_BLOCKED
paper_shadow_scope_plan_ready: true
no_broker_guardrail_plan_ready: true
```

Forbidden actions:

- enable scheduler;
- create cron / Windows Task / GitHub Actions schedule;
- execute real manual run;
- append or mutate historical event log;
- bind outcomes;
- mutate outcome store;
- enable paper-shadow;
- create paper trade, paper order, or shadow position;
- enable production;
- call broker API, preview broker order, query account for execution, or sync position for execution;
- put any capital at risk;
- read fresh market data;
- generate new signal, target weight, rebalance instruction, or daily report;
- run backtest.

## Implementation Plan

1. Reuse TRADING-2359 loader to validate TRADING-2347 through TRADING-2358 artifacts.
2. Load TRADING-2359 summary, package, source review, outcome binding contract, schema contract, binding idempotency contract, binding replay contract, outcome store mutation guardrail, binding rollback plan, blocked promotion rationale, 2360 route, interpretation boundary, and safety boundary.
3. Fail closed if 2359 status, readiness, route, evidence_chain_complete, owner_decision, contract readiness, promotion decision, promotion_allowed, safety fields, side-effect assertions, or inherited validate-data error count violates the expected contract.
4. Generate paper-shadow scope definition, no-broker guardrail plan, paper-shadow daily review plan, owner approval requirement, blocked promotion rationale, 2361 route, interpretation boundary, safety boundary, summary JSON, main package JSON, and research Markdown docs.
5. Update registry, artifact catalog, system flow, task register, completed register, and focused tests.

## Acceptance Criteria

- CLI is registered and executable.
- Loader reads TRADING-2359 outcome binding contract artifact.
- Loader reads TRADING-2358 event append contract artifact through the inherited source chain.
- Loader reads TRADING-2357 idempotency / replay contract artifact through the inherited source chain.
- `source_tasks` includes TRADING-2347 through TRADING-2359.
- `evidence_chain_complete=true`.
- `owner_decision=KEEP_DISABLED_AND_PROMOTION_BLOCKED`.
- `paper_shadow_scope_plan_ready=true`.
- `no_broker_guardrail_plan_ready=true`.
- `paper_shadow_daily_review_plan_ready=true`.
- `paper_shadow_owner_approval_requirement_ready=true`.
- `promotion_decision=BLOCKED`.
- `promotion_allowed=false`.
- `paper_shadow_enabled=false`.
- `paper_shadow_attempted=false`.
- `production_enabled=false`.
- `broker_action_enabled=false`.
- `broker_action_attempted=false`.
- Event append and outcome binding remain disabled and unattempted.
- No-broker guardrail blocks broker API import, order creation, order preview to broker, execution account query, execution position sync, and any capital at risk.
- Paper-shadow scope is `DISABLED_SCOPE_PLAN_ONLY`.
- Owner approval requirement exists.
- Next route is `TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan`.

## Validation Plan

Run:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_paper_shadow_scope_plan.py
aits research trends high-intensity-risk-cap-observe-only-paper-shadow-scope-plan
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for TRADING-2360 if the implementation only reads prior validated TRADING-2347 / 2348 / 2349 / 2350 / 2351 / 2352 / 2353 / 2354 / 2355 / 2356 / 2357 / 2358 / 2359 artifacts, does not read fresh market data, does not append event logs, does not bind outcomes, does not mutate outcome store, does not enable paper-shadow, does not call broker APIs, and does not generate technical features, scoring, backtest, or daily report outputs. The final closeout must state this reason explicitly.

## Progress Notes

- 2026-07-05: Added requirement and moved task into `IN_PROGRESS` before implementation. Scope is paper-shadow scope / no-broker guardrail planning only; owner decision remains `KEEP_DISABLED_AND_PROMOTION_BLOCKED`, and no scheduler enablement, manual run execution, event log mutation, outcome binding/store mutation, paper-shadow activation, production, or broker action is allowed.
- 2026-07-05: Implementation completed and moved to `DONE`. Real CLI run returned status=`OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`, readiness=`READY_FOR_2361_WITH_CAVEATS`, next route=`TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan`, promotion_allowed=false, paper_shadow_allowed=false, production_allowed=false, and broker_action=`none`. Validation passed Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs freshness 548 docs PASS, documentation contract 1257 reports PASS, task-register consistency run/validate PASS, and contract-validation 197 passed with runtime artifact `outputs/validation_runtime/contract-validation_20260705T060950Z/test_runtime_summary.json`; `git diff --check` passed with only a line-ending normalization warning.
