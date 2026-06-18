# TRADING-449 to TRADING-459 Next Research Cycle

最后更新：2026-06-18

## Status

Status: DONE.

This batch starts the research-only cycle after
`median_plus_regime_mismatch_filter` was returned to research. The cycle may
prepare intake, freeze a research candidate spec, run or fail-closed a
research-only backfill, review stress/cost/benchmark/signal/window evidence,
and prepare owner review material. It must not create a paper-shadow candidate.

## Context

The latest completed reset is TRADING-439 to TRADING-448. Its final snapshot is
`RETURN_TO_RESEARCH_COMPLETE`, candidate status is `RETURNED_TO_RESEARCH`, and
the owner action is `return_to_research`. Normal paper-shadow is disabled,
extended shadow is forbidden, live trading is forbidden, official target
weights are not generated, broker/order artifacts are not created, and the
candidate is not rejected.

The next cycle uses the reset artifacts as inputs:

- return-to-research governance snapshot;
- failure-mode attribution;
- reusable evidence extraction;
- hypothesis backlog;
- next-candidate spec draft;
- research backfill plan.

## Safety Boundary

- No official target weights.
- No broker integration.
- No order tickets.
- No live trading.
- No production mutation.
- No automatic position control.
- No extended shadow.
- No normal paper-shadow activation.
- Research-only / manual-review-only.
- Backfill outputs must not be interpreted as executable allocation.
- Do not fabricate data.
- Do not weaken promotion gates.

## Task Breakdown

1. TRADING-449 `next-research-cycle-intake`
   - Build `next_research_cycle_intake.json/md`.
   - Summarize reset state, reusable evidence, invalidated/weak evidence, P0
     hypotheses, and the draft spec proposal.
   - Add validation CLI and focused tests.
2. TRADING-450 `next-candidate-spec-freeze`
   - Freeze one research-only spec from the P0 backlog.
   - Assign a stable research candidate id.
   - Freeze signal inputs, regime assumptions, drawdown handling, rotation
     handling, turnover constraints, cost expectations, benchmark
     expectations, validation windows, and stop conditions.
   - Add validation CLI and focused tests.
3. TRADING-451 `next-candidate-backfill-runner`
   - Load the frozen spec and required backfill windows.
   - Run the same `aits validate-data` code path before any data-dependent
     backfill interpretation.
   - If no executable next-candidate signal/weight binding exists, fail closed
     with missing-data rows instead of fabricating return/drawdown/turnover
     metrics.
   - Output candidate backfill JSON/Markdown and validation.
4. TRADING-452 `next-candidate-stress-review`
   - Load frozen spec, backfill artifact, stress scenario library, drawdown
     casebook, and flip/rotation casebook.
   - Output `STRONG`, `MIXED`, `WEAK`, or `FAIL`, with blocking/warning
     scenarios and reusable/failure evidence.
5. TRADING-453 `next-candidate-cost-benchmark-review`
   - Load backfill, cost sensitivity framework, and benchmark baseline control
     pack.
   - Evaluate zero/low/medium/high cost and required baselines as evaluation
     only.
6. TRADING-454 `next-candidate-vs-returned-candidate-comparison`
   - Compare the new candidate evidence against the returned candidate's
     failure attribution and reusable evidence.
   - Output the required comparison status without claiming improvement from
     missing metrics.
7. TRADING-455 `next-candidate-signal-robustness-review`
   - Check missing feature columns, partial/stale series, schema mismatch, and
     market coverage gaps using signal completeness evidence.
   - Do not relax signal completeness rules.
8. TRADING-456 `next-candidate-overfit-and-window-sensitivity`
   - Split backfill into early/middle/recent/stress-heavy/calm windows.
   - Report stability or fragility from available evidence only.
9. TRADING-457 `next-candidate-research-gate`
   - Combine frozen spec, backfill, stress, cost/benchmark, comparison, signal,
     and window reviews.
   - Output one of the research gate decisions and required next action.
   - This gate cannot activate paper-shadow.
10. TRADING-458 `next-candidate-owner-research-review-packet`
    - Prepare owner options: continue research validation, revise hypothesis,
      reject research candidate, or hold for more data.
    - Do not append owner decision automatically.
11. TRADING-459 `next-candidate-research-cycle-snapshot`
    - Collect all cycle artifacts and output final research-cycle readiness
      status.

## Dependencies

- TRADING-439 to TRADING-448 artifacts must be readable.
- TRADING-451 requires cached data validation visibility.
- If no frozen spec exists, stop at TRADING-450.
- If backfill data or executable signal binding is missing, downstream artifacts
  must expose `NEEDS_MORE_EVIDENCE` / blocked research status and must not
  produce paper-shadow, official weights, broker/order, or production outputs.

## Acceptance Criteria

- JSON and Markdown artifacts are generated for TRADING-449 through TRADING-459.
- Validation CLI exists for generated research-cycle artifacts.
- Focused tests cover payload generation, validation, CLI writes, and safety
  invariants.
- Reader Brief, report registry, artifact catalog, system flow, README, task
  register, and this requirement document are updated.
- `aits validate-data --as-of 2026-06-17` or the same validation code path is
  visible before backfill interpretation.
- The final snapshot states the selected market regime and requested date
  range, and keeps all safety boundaries locked.
- Relevant focused pytest, ruff, py_compile, documentation contract, task
  register consistency, and `git diff --check` pass or blockers are recorded.

## Progress Notes

- 2026-06-18: Created this requirement record and task-register row from owner
  attachment. Implementation has not yet changed report logic.
- 2026-06-18: Implemented the next research-cycle report family, CLI commands,
  validation commands, Reader Brief summary, report registry entries, artifact
  catalog coverage, system flow, README, and focused tests. Real 2026-06-17
  artifacts were generated for TRADING-449 through TRADING-459. TRADING-451
  called the same cached-data validation code path before backfill
  interpretation; data quality status was `PASS_WITH_WARNINGS` and the backfill
  correctly failed closed as
  `CANDIDATE_BACKFILL_NEEDS_EXECUTABLE_BINDING` because executable
  next-candidate signal/weight binding is missing. Final snapshot status is
  `NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE`, research gate decision is
  `NEEDS_MORE_EVIDENCE`, market regime is `ai_after_chatgpt`, requested date
  range is `2023-01-03..2025-04-30`, and paper-shadow activation, extended
  shadow, live trading, official target weights, broker/order, owner decision
  append, and production mutation remain disallowed. Report index and Reader
  Brief were regenerated; Reader Brief now reads
  `next_candidate_research_cycle_snapshot` and its validation artifact.
