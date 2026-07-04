# TRADING-2350 Observe-Only Scheduler Manual-Run Interface Dry-Run

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2350_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN`
- priority: `P0`
- status: `DONE`
- owner: `system`
- last_updated: `2026-07-05`

## Background

TRADING-2347 implemented disabled-by-default observe-only scheduler wiring.
TRADING-2348 proved the disabled wiring can be reached by a CLI smoke dry-run
without creating a real scheduler or trading side effect. TRADING-2349 then
wrapped the 2347/2348 evidence in a manual review promotion gate and kept
promotion blocked.

TRADING-2350 adds the next observe-only step: a manual-run interface dry-run.
It verifies that a human-triggered scheduler entry point can be previewed from
the CLI while still refusing to execute the manual run, enable a scheduler,
append events, bind outcomes, enter paper-shadow, enter production, or call a
broker.

## Scope

1. Read TRADING-2347 disabled wiring artifacts.
2. Read TRADING-2348 smoke dry-run artifacts.
3. Read TRADING-2349 manual review gate artifacts.
4. Fail closed if any source artifact opens scheduler, event append, outcome
   binding, paper-shadow, production, broker action, promotion, real scheduler
   creation, event log mutation, outcome store mutation, or fresh market data
   reads.
5. Generate a manual-run preview package without executing the manual run.
6. Generate dry-run evidence and no-side-effect assertions.
7. Route only to
   `TRADING-2351_Observe_Only_Scheduler_Manual_Run_Replay_No_Side_Effect_Validation`.

## Safety Boundary

Required fixed fields:

- `scheduler_enabled=false`
- `manual_run_only=true`
- `dry_run_only=true`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `paper_shadow_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `promotion_allowed=false`
- `manual_review_required=true`

Forbidden actions:

- enable scheduler, cron, Windows Task, or GitHub Actions schedule
- append or mutate event logs
- bind or mutate outcomes
- read fresh market data
- generate target weights or rebalance instructions
- enter paper-shadow or production
- call broker APIs or send orders

## Artifacts

Default output root:

`outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run/`

Expected JSON artifacts:

- `high_intensity_scheduler_manual_run_dry_run_summary.json`
- `high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.json`
- `high_intensity_scheduler_manual_run_dry_run_source_artifact_review.json`
- `high_intensity_scheduler_manual_run_dry_run_preview.json`
- `high_intensity_scheduler_manual_run_dry_run_evidence.json`
- `high_intensity_scheduler_manual_run_dry_run_side_effect_assertions.json`
- `high_intensity_2351_manual_run_replay_route.json`
- `high_intensity_scheduler_manual_run_dry_run_interpretation_boundary.json`
- `high_intensity_scheduler_manual_run_dry_run_safety_boundary.json`

Expected Markdown artifacts:

- `docs/research/high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.md`
- `docs/research/high_intensity_2351_manual_run_replay_route.md`

## Acceptance Criteria

- CLI
  `aits research trends high-intensity-risk-cap-observe-only-scheduler-manual-run-dry-run`
  runs successfully.
- Status is
  `OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`.
- Source tasks are exactly `TRADING-2347`, `TRADING-2348`, and `TRADING-2349`.
- `manual_run_interface_present=true`.
- `manual_run_preview_generated=true`.
- `manual_run_executed=false`.
- Scheduler, event append, outcome binding, paper-shadow, production, broker
  action, and promotion remain disabled.
- Side-effect assertions prove no cron, Windows Task, GitHub Actions schedule,
  real scheduler, event append, outcome binding, paper-shadow, production, or
  broker action was attempted.
- Readiness is `READY_FOR_2351_WITH_CAVEATS`.
- Next route is
  `TRADING-2351_Observe_Only_Scheduler_Manual_Run_Replay_No_Side_Effect_Validation`.

## Validation Plan

- `ruff check` on touched Python files.
- `python -m compileall -q` on touched Python files and tests.
- Focused parallel pytest for the new TRADING-2350 tests.
- Real CLI run to generate artifacts and research docs.
- Docs freshness, documentation contract, task-register consistency, and
  contract-validation tier.
- `git diff --check`.

`aits validate-data` is not required for TRADING-2350 because the command only
reads prior validated 2347/2348/2349 research artifacts. It does not read fresh
market data, append events, or bind outcomes.

## Progress Notes

- 2026-07-05: Created from owner attachment and entered `IN_PROGRESS`.
- 2026-07-05: Implemented manual-run interface dry-run CLI, source artifact
  loader, preview/evidence/side-effect assertion artifacts, 2351 route,
  registry/catalog/system-flow updates, research docs, and focused tests.
  Completed as `DONE`; `aits validate-data` was not rerun because this task
  reads only prior validated 2347/2348/2349 research artifacts and does not read
  fresh market data, append events, or bind outcomes.
