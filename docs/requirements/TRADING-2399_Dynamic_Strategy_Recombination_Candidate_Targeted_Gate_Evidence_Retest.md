# TRADING-2399: Dynamic Strategy Recombination Candidate Targeted Gate Evidence Retest

最后更新：2026-07-07

## Context

TRADING-2398 completed the recombination candidate gate evidence and targeted
improvement plan. The candidate under review remains
`growth_tilt_lower_turnover_guarded_transfer_v1`; TRADING-2396 classified it as
`OWNER_REVIEW_REQUIRED`, and TRADING-2397 kept owner review required without
approving research-only observation.

TRADING-2399 runs the targeted retest planned by TRADING-2398. Unlike TRADING-2398,
this task is cached-data-dependent and must run the same data quality gate as the
dynamic strategy retest path before any backtest result is generated.

## Scope

This task will:

1. Read prior TRADING-2398 gate evidence plan artifacts.
2. Read prior TRADING-2397 owner decision artifacts.
3. Read prior TRADING-2396 recombination retest artifacts.
4. Read prior TRADING-2395 recombination candidate plan artifacts.
5. Run `aits validate-data --as-of 2026-07-05` or the same validation code path
   before the targeted retest.
6. Build and retest the six TRADING-2398 targeted variants using the
   `valid_until_window` primary cadence.
7. Compute variant ranking, gate evidence matrix, time/regime slice evidence,
   cost stress evidence, cadence comparison evidence and decision update.
8. Route any observation-preview or owner-review result to TRADING-2400 owner
   decision without approving observation in this task.

## Targeted Variants

- `growth_tilt_guarded_transfer_time_slice_repair_v1`
- `growth_tilt_guarded_transfer_regime_repair_v1`
- `growth_tilt_guarded_transfer_drawdown_calibrated_v1`
- `growth_tilt_guarded_transfer_return_retention_v1`
- `growth_tilt_guarded_transfer_valid_until_strict_v1`
- `growth_tilt_guarded_transfer_balanced_gate_v1`

## Pilot Construction Governance

The targeted variant construction weights introduced in TRADING-2399 are
research-only pilot baselines for this retest. They are named constants in code,
not production policy. Their purpose is to translate the TRADING-2398 qualitative
variant design into deterministic target-weight series so the gate evidence can
be compared. They must not be reused as investment policy, promotion gate,
position cap, score band or production allocation rule without a separate owner
review, policy manifest entry and validation evidence.

## Non-Goals

- Do not approve research-only observation.
- Do not enable paper-shadow, scheduler, event append, outcome binding,
  production, broker/order, scheduled task or daily report paths.
- Do not write production weights or active shadow weights.
- Do not change the TRADING-2396 / 2398 acceptance thresholds silently.
- Do not treat an observation-preview candidate as owner approval.

## Safety Boundary

- `production_effect=none`
- `broker_action=none`
- `candidate_auto_accept_approved=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest/targeted_gate_evidence_retest_result.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest/targeted_variant_ranking.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest/gate_evidence_matrix.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest/decision_update.json`
- `docs/research/dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest.md`
- `docs/research/dynamic_strategy_targeted_gate_evidence_variant_ranking.md`
- `docs/research/dynamic_strategy_targeted_gate_evidence_matrix.md`
- `docs/research/dynamic_strategy_2400_route.md`

## Acceptance Criteria

- `aits validate-data --as-of 2026-07-05` or the same code path runs before the
  targeted retest and the output discloses data quality status.
- `aits research strategies dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest`
  returns
  `DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_READY`.
- Output identifies candidate under review as
  `growth_tilt_lower_turnover_guarded_transfer_v1`.
- Output confirms primary execution cadence is `valid_until_window`.
- Output tests all six TRADING-2398 targeted variants.
- Output includes `targeted_variant_ranking`, `gate_evidence_matrix` and
  `decision_update`.
- Output includes `best_targeted_variant`,
  `best_targeted_variant_decision` and
  `observation_preview_candidates_count`.
- Recommended next task is
  `TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision`.
- Safety fields remain false/none.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, docs freshness, task-register consistency,
  report contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation will reuse the TRADING-2396 cached-data quality gate and retest
  helper path while substituting the TRADING-2398 targeted variant universe. This
  task uses the full unique ID
  `TRADING-2399_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST`
  to distinguish it from the already archived
  `TRADING-2399_DAILY_INCREMENTAL_REFACTOR_DYNAMIC_STRATEGY_JSON_HELPER_BOUNDARY`.
- 2026-07-07: Implemented
  `aits research strategies dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest`,
  the targeted retest builder, report registry/catalog/system-flow entries,
  research docs and focused tests. The real data gate
  `aits validate-data --as-of 2026-07-05` passed with
  `PASS_WITH_WARNINGS` / errors=0 before the cached-data-dependent retest.
- 2026-07-07: Real CLI run returned
  `DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_READY`.
  Best targeted variant was
  `growth_tilt_guarded_transfer_valid_until_strict_v1`; best targeted variant
  decision was `CONTINUE_TARGETED_IMPROVEMENT`; observation preview candidate
  count stayed 0. The next route is
  `TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision`.
  Safety fields remain false/none, so the task does not approve observation,
  paper-shadow, scheduler, event append, outcome binding, production, broker or
  daily report paths.
- 2026-07-07: Final validation passed: full Ruff, `compileall -q src tests`,
  focused parallel pytest 3 passed, docs freshness 586 docs PASS,
  documentation contract 1296 reports PASS, task-register consistency
  active=319 / completed=460 / failed=0, task-register consistency validation
  checks=5 / failed=0 / warnings=0, contract-validation 197 passed
  (`outputs/validation_runtime/contract-validation_20260707T040233Z/test_runtime_summary.json`),
  active-register terminal status guard and `git diff --check`.
