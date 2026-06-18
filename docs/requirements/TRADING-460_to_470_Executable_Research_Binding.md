# TRADING-460 to TRADING-470 Executable Research Binding

最后更新：2026-06-18

## Status

Status: IN_PROGRESS.

This batch follows TRADING-449 to TRADING-459. The previous next research cycle
correctly stopped at `CANDIDATE_BACKFILL_NEEDS_EXECUTABLE_BINDING` /
`NEEDS_MORE_EVIDENCE` because no executable next-candidate signal or
research-only weight binding existed. This batch must add that executable
research-only binding layer without creating paper-shadow, official target
weights, broker/order artifacts, owner decisions, or production mutations.

## Context

Latest confirmed baseline commit:

- `db8d3e8e Complete TRADING-449 to 459 next research cycle`

Latest confirmed statuses:

- Final snapshot: `NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE`
- Research gate: `NEEDS_MORE_EVIDENCE`
- Backfill: `CANDIDATE_BACKFILL_NEEDS_EXECUTABLE_BINDING`
- Data quality: `PASS_WITH_WARNINGS`, errors=0
- Candidate id: `median_plus_regime_mismatch_filter_research_redesign_v2`
- Market regime: `ai_after_chatgpt`
- Requested date range: `2023-01-03..2025-04-30`

## Safety Boundary

- No official target weights.
- No broker integration.
- No order tickets.
- No live trading.
- No production mutation.
- No automatic position control.
- No paper-shadow activation.
- No extended shadow.
- No owner decision append unless explicitly requested.
- All binding outputs are research-only.
- All hypothetical weights are marked `research_only` and
  `not_official_target_weights`.
- Do not fabricate metrics.
- Do not weaken gates to pass validation.

## Task Breakdown

1. TRADING-460 `next-candidate-executable-binding-contract`
   - Define the executable binding contract for the frozen next research
     candidate.
   - Load the latest frozen spec, next research cycle snapshot, backfill plan,
     and research gate output.
   - Define required signal inputs, feature inputs, allowed research-only state
     outputs, allowed hypothetical allocation outputs, and forbidden outputs.
   - Add report CLI, validation CLI, Reader Brief section, registry/catalog/docs,
     and focused tests.
   - Do not implement strategy behavior yet.
2. TRADING-461 `next-candidate-signal-binding-implementation`
   - Transform validated feature/signal inputs into candidate signal states.
   - Fail closed for missing/stale/schema-mismatched/empty/insufficient inputs.
   - Output research-only candidate signal series, signal state, uncertainty,
     and blocking reason.
3. TRADING-462 `next-candidate-research-weight-binding`
   - Convert signal state into hypothetical research-only weight output.
   - Reject outputs missing `research_only=true`.
4. TRADING-463 `executable-binding-safety-audit`
   - Audit binding code/artifacts for official weights, broker/order, live
     allocation, production mutation, secrets, paper-shadow activation, and owner
     append.
   - Do not run backfill unless safety audit is pass or acceptable warning.
5. TRADING-464 `rerun-next-candidate-backfill-with-binding`
   - Rerun backfill across required windows using executable research-only
     binding and validated inputs.
   - Produce real metrics only; otherwise keep exact blocked reason.
6. TRADING-465 `rerun-next-candidate-stress-cost-benchmark`
   - Rerun stress, drawdown/flip, cost, and benchmark review from real metrics.
7. TRADING-466 `rerun-next-vs-returned-candidate-comparison`
   - Compare new executable metrics against returned-candidate failure evidence.
8. TRADING-467 `rerun-signal-robustness-and-window-sensitivity`
   - Rerun signal robustness and window sensitivity from executable outputs.
9. TRADING-468 `rerun-next-candidate-research-gate`
   - Rerun research gate using executable binding, safety audit, and real metrics.
   - This gate still cannot activate paper-shadow.
10. TRADING-469 `owner-research-review-packet-after-real-metrics`
    - Prepare owner options after real metrics without appending decisions.
11. TRADING-470 `executable-research-cycle-snapshot`
    - Generate final executable research-cycle snapshot from the executable
      binding chain and real metrics.

## Hard Stops

- If executable binding contract cannot be defined, stop at TRADING-460.
- If signal binding cannot consume real validated inputs, stop at TRADING-461.
- If research weight binding risks official target weight semantics, stop at
  TRADING-462.
- If safety audit fails, stop at TRADING-463.
- If backfill cannot produce real metrics, stop at TRADING-464.
- If research gate is not `RESEARCH_PROMISING`, do not proceed toward
  paper-shadow.
- Do not activate paper-shadow in this batch.
- Do not approve extended shadow.
- Do not approve live trading.
- Do not create official target weights.
- Do not create broker/order artifacts.

## Acceptance Criteria

- Each task is implemented and committed separately.
- Each report has JSON/Markdown output, validation CLI, registry/catalog
  coverage, Reader Brief coverage where requested, and focused tests.
- Data-dependent commands run or call the same validate-data code path and stop
  on validation failure.
- Every artifact states market regime, requested date range where applicable,
  `production_effect=none`, and research-only safety metadata.
- Metrics are produced only when computed from executable binding outputs; no
  fabricated return, drawdown, turnover, cost, or benchmark metrics.
- Documentation contract, task-register consistency, focused pytest, ruff,
  compile/py_compile, relevant data quality/report gates, and `git diff --check`
  pass for each task or a hard-stop blocker is recorded.

## Progress Notes

- 2026-06-18: Created this requirement record and task-register row from owner
  request. Implementation has not yet changed executable binding behavior.
- 2026-06-18: TRADING-460 completed. Added
  `next_candidate_executable_binding_contract` and validation artifacts with
  report CLI, validation CLI, Reader Brief section, report registry, artifact
  catalog, system flow, README, and focused tests. The contract is schema-only:
  strategy behavior, signal binding, research weight binding, and real metrics
  remain unimplemented by design. Next task is TRADING-461 signal binding.
- 2026-06-18: TRADING-461 started. Implementation must consume validated cached
  data and the TRADING-460 contract, fail closed for missing/stale/schema-empty
  signal or feature inputs, and output only research-only signal state,
  uncertainty, and blocking reasons. It must not produce research weights,
  official target weights, paper-shadow activation, broker/order artifacts, or
  backfill metrics.
- 2026-06-18: TRADING-461 completed pending commit. Added
  `next_candidate_signal_binding` and validation artifacts with report CLI,
  validation CLI, Reader Brief section, report registry, artifact catalog,
  system flow, README, governed signal-binding policy, and focused tests. The
  real 2026-06-17 artifact consumed the TRADING-460 contract validation and the
  cached-data quality gate (`PASS_WITH_WARNINGS`, errors=0), produced one
  research-only latest signal row dated 2026-06-16, and validated PASS. Status
  is `CANDIDATE_SIGNAL_BINDING_COMPLETE_WITH_WARNINGS`; warnings are data
  quality warnings, latest signal date outside frozen validation windows, and
  signal input completeness warning. No hypothetical weights, official target
  weights, backfill metrics, paper-shadow activation, owner decision,
  broker/order, or production mutation were produced. Next task is TRADING-462
  research-only hypothetical weight binding.
- 2026-06-18: TRADING-462 started. Implementation must consume the validated
  TRADING-461 signal binding and cached-data quality gate, convert signal state
  into hypothetical research-only allocation output, and expose previous
  hypothetical weight, rotation delta, turnover proxy, risk state,
  constraint-hit list, and blocking reason. Every weight output must be marked
  `research_only=true` and `not_official_target_weights=true`; official target
  weights, broker/order artifacts, paper-shadow activation, production mutation,
  and backfill metrics remain forbidden.
- 2026-06-18: TRADING-462 completed pending commit. Added
  `next_candidate_research_weight_binding` and validation artifacts with report
  CLI, validation CLI, Reader Brief section, report registry, artifact catalog,
  system flow, README, governed research-weight policy, and focused tests. The
  real 2026-06-17 artifact consumed validated signal binding and the
  cached-data quality gate (`PASS_WITH_WARNINGS`, errors=0), produced one
  hypothetical research-only weight row dated 2026-06-16, turnover proxy
  `0.85`, and validation PASS. Status is
  `CANDIDATE_RESEARCH_WEIGHT_BINDING_COMPLETE_WITH_WARNINGS`; warnings are data
  quality warnings, initial previous hypothetical weight from policy, latest
  signal date outside frozen validation windows, and signal input completeness
  warning. No official target weights, backfill metrics, paper-shadow
  activation, owner decision, broker/order, or production mutation were
  produced. Next task is TRADING-463 executable binding safety audit.
- 2026-06-18: TRADING-463 started. Implementation must audit the executable
  binding code and artifacts before any backfill rerun, scanning for official
  target weights, broker integration, order tickets, live allocation,
  production mutation, account id/API key exposure, paper-shadow activation,
  and owner decision append. The audit must validate that signal binding is
  research-only and weight binding is hypothetical-only, and must output
  `EXECUTABLE_BINDING_SAFETY_PASS`, `EXECUTABLE_BINDING_SAFETY_WARNING`, or
  `EXECUTABLE_BINDING_SAFETY_BLOCKED`. Backfill remains forbidden unless the
  safety audit is pass or acceptable warning.
- 2026-06-18: TRADING-463 completed pending commit. Added
  `executable_binding_safety_audit` and validation artifacts with report CLI,
  validation CLI, Reader Brief section, report registry, artifact catalog,
  system flow, README, focused tests, artifact-level safety checks, and focused
  static scanning over binding code/policies/artifacts. The real 2026-06-17
  audit consumed validated signal and research weight binding artifacts, found
  artifact failures `0` and static blockers `0`, and returned
  `EXECUTABLE_BINDING_SAFETY_WARNING` with `acceptable_warning=true`; validation
  PASS. Warnings are safe-context forbidden-term mentions in binding code and
  artifacts, not positive production behavior. No backfill was run, no
  paper-shadow was activated, no official target weights, broker/order,
  owner-decision append, or production mutation were produced. Next task is
  TRADING-464 binding-backed research backfill.
- 2026-06-18: TRADING-464 started. Implementation must load the validated
  executable signal binding, research-only weight binding, safety audit, frozen
  candidate spec, validated data gate, and backfill windows. Because the current
  executable binding artifacts contain one latest signal/weight row rather than
  a historical dynamic signal path, any computed backfill metrics must be marked
  partial/static-weight research proxies and must not be presented as a full
  dynamic strategy backfill. Missing price coverage, failed safety audit, failed
  data quality, missing binding rows, or invalid research-only metadata must
  keep the backfill blocked with exact reasons.
- 2026-06-18: TRADING-464 completed pending commit. Updated
  `next_candidate_backfill` to consume validated signal binding, research-only
  weight binding, executable binding safety audit, frozen spec, validate-data
  status, backfill windows, and price cache. The real 2026-06-17 run produced
  `CANDIDATE_BACKFILL_PARTIAL` with real static research-weight proxy metrics
  across all 6 required windows: aggregate return proxy `0.006166`, aggregate
  drawdown proxy `-0.277213`, turnover proxy `0.85`, rotation count `1`, false
  risk-off count `0`, constraint hits `0`, and validation PASS. The partial
  status is required because the current executable binding has one latest
  signal/weight row and no historical dynamic signal series for the required
  windows. No paper-shadow, official target weights, broker/order, owner
  decision append, or production mutation were produced. Next task is
  TRADING-465 stress/cost/benchmark rerun from the real backfill metrics.
- 2026-06-18: TRADING-465 started. Implementation must load the latest complete
  or partial backfill and rerun stress scenario, drawdown casebook,
  flip/rotation casebook, cost sensitivity, and benchmark baseline reviews from
  real metrics. Because the current backfill is partial/static-proxy, stress and
  benchmark conclusions must remain conservative and must expose major blockers
  and warnings instead of forcing `RESEARCH_PROMISING`.
- 2026-06-18: TRADING-465 completed pending commit. `next_candidate_stress_review`
  now consumes TRADING-464 window metrics and produced `WEAK` on the 2026-06-17
  real run: 1 blocking scenario (`slow_drawdown`), 5 warnings, worst drawdown
  proxy `-0.277213`, worst return proxy `-0.150273`, validation PASS.
  `next_candidate_cost_benchmark_review` now computes cost drag, net proxy, and
  benchmark deltas from aggregate return proxy `0.006166` and turnover proxy
  `0.85`; the real run produced `COST_BENCHMARK_REVIEW_WEAK`, cost survival
  `COST_SURVIVAL_WARNING`, benchmark relative status `BENCHMARK_UNDERPERFORMS`,
  1 major blocker and 4 warnings, validation PASS. Reader Brief now exposes a
  combined stress/cost/benchmark summary. All outputs remain research-only and
  no paper-shadow, official weights, broker/order, owner decision append, or
  production mutation were produced. Next task is TRADING-466 vs-returned
  comparison from real metrics.
- 2026-06-18: TRADING-466 started. Implementation must load returned candidate
  failure attribution, reusable/invalidated evidence, TRADING-464 backfill,
  TRADING-465 stress review, and TRADING-465 cost/benchmark review. The
  comparison must explicitly mark repeated failure modes, especially benchmark
  underperformance and partial/static proxy limitations, and must not claim
  `IMPROVED_OVER_RETURNED_CANDIDATE` unless real metrics support it across
  drawdown, turnover, cost survival, benchmark-relative behavior, signal
  robustness, and governance blockers.
- 2026-06-18: TRADING-466 completed pending commit. The real 2026-06-17
  comparison produced `MIXED_VS_RETURNED_CANDIDATE` with real metrics available
  but no measurable improvement established. Row-level results: drawdown
  mismatch `REGRESSED_VS_REUSABLE_EVIDENCE`, flip/rotation `MIXED`, turnover
  `MIXED`, cost survival `MIXED`, benchmark-relative behavior
  `REPEATS_FAILURE_MODE`, signal robustness `NO_IMPROVEMENT`, governance
  blockers `NO_IMPROVEMENT`. Validation PASS. Reader Brief now exposes the
  vs-returned comparison summary. No paper-shadow, official weights,
  broker/order, owner decision append, or production mutation were produced.
  Next task is TRADING-467 signal robustness and window sensitivity rerun.
- 2026-06-18: TRADING-467 started. Implementation must rerun signal robustness
  and window sensitivity from the executable signal binding, TRADING-464 real
  backfill window metrics, and current source signal completeness evidence. It
  must explicitly test missing feature columns, partial signal series, stale
  signal series, schema mismatch, market coverage gap, early/middle/recent/
  stress-heavy/calm windows, overfit risk, and blocking conditions. Partial
  static proxy metrics must remain fragile and must not promote the candidate.
- 2026-06-18: TRADING-467 completed pending commit. The real 2026-06-17 signal
  robustness rerun consumed `next_candidate_signal_binding` and TRADING-464
  backfill metrics, produced `SIGNAL_ROBUSTNESS_BLOCKED`, and validation PASS.
  Blocking checks count is 3: partial signal series, stale latest-only signal
  row outside frozen validation windows, and historical market coverage gaps.
  The window sensitivity rerun consumed real backfill window rows, produced
  `WINDOW_FRAGILE`, and validation PASS. It reported `overfit_risk=HIGH`,
  weak splits=2, partial static proxy splits=3, unavailable splits=0,
  performance dispersion `0.603795`, drawdown dispersion `0.208261`, and no
  false flip dispersion. Reader Brief now exposes the combined signal/window
  summary. No paper-shadow, official weights, broker/order, owner decision
  append, or production mutation were produced. Next task is TRADING-468
  research gate rerun from executable binding and real metrics.
- 2026-06-18: TRADING-468 started. Implementation must load the frozen candidate
  spec, executable binding safety audit, TRADING-464 real backfill, TRADING-465
  stress/cost/benchmark reviews, TRADING-466 vs-returned comparison, and
  TRADING-467 signal/window reviews. The gate must output only
  `RESEARCH_PROMISING`, `NEEDS_MORE_EVIDENCE`,
  `RETURN_TO_HYPOTHESIS_BACKLOG`, or `REJECT_RESEARCH_CANDIDATE`, must include
  strongest positive/negative evidence, blockers, and required next action, and
  still must not allow paper-shadow activation or any official weight/broker/
  order/production side effect.
- 2026-06-18: TRADING-468 completed pending commit. The real 2026-06-17 gate
  loaded executable binding safety audit `EXECUTABLE_BINDING_SAFETY_WARNING`,
  TRADING-464 backfill `CANDIDATE_BACKFILL_PARTIAL`, TRADING-465 stress
  `WEAK`, TRADING-465 cost/benchmark `COST_BENCHMARK_REVIEW_WEAK`, TRADING-466
  comparison `MIXED_VS_RETURNED_CANDIDATE`, TRADING-467 signal
  `SIGNAL_ROBUSTNESS_BLOCKED`, and TRADING-467 window `WINDOW_FRAGILE`.
  Decision remained `NEEDS_MORE_EVIDENCE`, validation PASS, blockers=5:
  stress weak, cost/benchmark weak, repeated failure mode unresolved, signal
  robustness blocked, and window sensitivity fragile. Required next action is
  `repair_signal_window_evidence_before_gate_rerun`. The gate produced no
  paper-shadow activation, official weights, broker/order, owner decision append,
  or production mutation. Next task is TRADING-469 owner research review packet.
