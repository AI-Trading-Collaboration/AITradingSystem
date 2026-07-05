# TRADING-2351 Observe-Only Scheduler Manual-Run Replay No-Side-Effect Validation

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2351_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION`
- priority: `P0`
- status: `DONE`
- owner: `system`
- last_updated: `2026-07-05`

## Background

TRADING-2350 completed the observe-only scheduler manual-run interface dry-run
and routed to
`TRADING-2351_Observe_Only_Scheduler_Manual_Run_Replay_No_Side_Effect_Validation`.
The prior CLI proved the manual-run interface can be previewed while
`manual_run_executed=false`, `scheduler_enabled=false`, and promotion remains
blocked.

TRADING-2351 does not enable the scheduler. It replays the 2350 dry-run
interface repeatedly and verifies that the stable semantic output stays
unchanged while no event, outcome, paper-shadow, production, broker, cron,
Windows Task, GitHub Actions schedule, or fresh market data side effect is
created.

## Scope

1. Read TRADING-2347 disabled wiring artifacts.
2. Read TRADING-2348 smoke dry-run evidence artifacts.
3. Read TRADING-2349 manual review gate artifacts.
4. Read TRADING-2350 manual-run dry-run artifacts.
5. Execute 3 replay validation rounds without executing manual run logic.
6. Compare only stable semantic fields, excluding timestamp/runtime fields.
7. Hard assert side-effect fields before and after replay.
8. Generate replay no-side-effect evidence and research docs.
9. Route only to
   `TRADING-2352_Observe_Only_Scheduler_Audit_Package_And_Owner_Review_Checklist`.

## Step Breakdown

1. Register and requirement update.
   - Acceptance: task register row links to this document and fixes the safety
     boundary before implementation.
2. Module and CLI implementation.
   - Acceptance: a new replay validation module loads 2347/2348/2349/2350
     artifacts, performs 3 replay rounds, checks stable semantics, and writes
     JSON/Markdown outputs.
3. Focused tests.
   - Acceptance: CLI registration/run, source artifact loading, replay_count,
     stable semantic pass, side-effect pass, blocked promotion, no scheduler
     creation, and registry/catalog/system-flow/task-register references are
     covered.
4. Real artifact generation and docs integration.
   - Acceptance: real CLI returns
     `OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION_PASSED_WITH_CAVEATS_PROMOTION_BLOCKED`
     and writes the documented artifacts.
5. Validation and closeout.
   - Acceptance: focused parallel pytest, docs checks, task consistency,
     contract validation, and diff check pass; task register and completed
     register are updated when the task is complete.

## Stable Semantic Fields

Replay comparison must hash or compare only these stable fields:

- `status`
- `task_id`
- `source_tasks`
- `scheduler_enabled`
- `manual_run_only`
- `dry_run_only`
- `manual_run_executed`
- `promotion_allowed`
- `side_effect_assertions`
- `promotion_blocked_reasons`
- `next_route`

Generated timestamps, runtime paths, durations, and artifact path strings are
not part of the semantic stability hash.

## Safety Boundary

Required fixed fields:

- `scheduler_enabled=false`
- `manual_run_only=true`
- `dry_run_only=true`
- `manual_run_executed=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `paper_shadow_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `promotion_allowed=false`
- `manual_review_required=true`

Forbidden actions:

- enable scheduler, cron, Windows Task, or GitHub Actions schedule
- create or mutate external scheduler entries
- append or mutate event logs
- bind or mutate outcomes
- read fresh market data
- generate target weights or rebalance instructions
- enter paper-shadow or production
- call broker APIs or send orders

## Artifacts

Default output root:

`outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation/`

Expected JSON artifacts:

- `high_intensity_scheduler_manual_run_replay_validation_summary.json`
- `high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.json`
- `high_intensity_scheduler_manual_run_replay_source_artifact_review.json`
- `high_intensity_scheduler_manual_run_replay_semantic_checks.json`
- `high_intensity_scheduler_manual_run_replay_side_effect_assertions.json`
- `high_intensity_scheduler_manual_run_replay_evidence.json`
- `high_intensity_2352_scheduler_audit_package_route.json`
- `high_intensity_scheduler_manual_run_replay_interpretation_boundary.json`
- `high_intensity_scheduler_manual_run_replay_safety_boundary.json`

Expected Markdown artifacts:

- `docs/research/high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.md`
- `docs/research/high_intensity_2352_scheduler_audit_package_route.md`

## Acceptance Criteria

- CLI
  `aits research trends high-intensity-risk-cap-observe-only-scheduler-manual-run-replay-validation`
  runs successfully.
- Status is
  `OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION_PASSED_WITH_CAVEATS_PROMOTION_BLOCKED`.
- Source tasks are exactly `TRADING-2347`, `TRADING-2348`, `TRADING-2349`, and
  `TRADING-2350`.
- `replay_count=3`.
- `stable_semantic_replay_passed=true`.
- `side_effect_assertions_passed=true`.
- `manual_run_executed=false`.
- `scheduler_enabled=false`.
- `manual_run_only=true`.
- `dry_run_only=true`.
- `promotion_allowed=false`.
- event append, outcome binding, paper-shadow, production, broker, cron,
  Windows Task, GitHub Actions schedule, and real scheduler creation all remain
  false.
- Readiness is `READY_FOR_2352_WITH_CAVEATS`.
- Next route is
  `TRADING-2352_Observe_Only_Scheduler_Audit_Package_And_Owner_Review_Checklist`.

## Validation Plan

- `ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_scheduler_manual_run_replay_validation.py`
- Real CLI run:
  `aits research trends high-intensity-risk-cap-observe-only-scheduler-manual-run-replay-validation`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data` is not required for TRADING-2351 because the command only
reads prior validated 2347/2348/2349/2350 research artifacts. It does not read
fresh market data, append events, bind outcomes, produce technical features,
produce scoring outputs, backtest, or generate daily reports.

## Progress Notes

- 2026-07-05: Created from owner attachment and entered `IN_PROGRESS`.
- 2026-07-05: Validation command adjustment recorded. The attachment listed
  `aits tasks consistency run/validate`, but the current CLI has no `tasks`
  root command; the governed repository entry point is
  `aits reports task-register-consistency run` /
  `aits reports task-register-consistency validate --latest`, matching
  `docs/requirements/TRADING-362_Task_Register_Consistency_Check.md` and
  `docs/artifact_catalog.md`.
- 2026-07-05: Implemented replay validation module, CLI, source artifact
  loader, stable semantic checks, side-effect assertions, 2352 route, research
  docs, registry/catalog/system-flow updates, and focused tests. Real CLI run
  returned
  `OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION_PASSED_WITH_CAVEATS_PROMOTION_BLOCKED`
  with replay_count=3, stable_semantic_replay_passed=true,
  side_effect_assertions_passed=true, scheduler_enabled=false,
  manual_run_executed=false, and promotion_allowed=false. Validation passed
  Ruff, compileall, focused parallel pytest 7 passed, real CLI run, docs
  freshness 539 docs PASS, documentation contract 1248 reports PASS,
  task-register consistency run/validate PASS, and contract-validation 197
  passed with runtime artifact
  `outputs/validation_runtime/contract-validation_20260705T024631Z/test_runtime_summary.json`.
  `aits validate-data` was not rerun because TRADING-2351 only reads prior
  validated 2347/2348/2349/2350 research artifacts and does not read fresh
  market data, append events, bind outcomes, produce technical features,
  score, backtest, or generate daily reports.
