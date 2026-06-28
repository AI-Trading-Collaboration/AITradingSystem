# TRADING-2269 Norgate Platinum Decision Wording Reconciliation

最后更新：2026-06-28

## Metadata

- Task id: `TRADING-2269_NORGATE_PLATINUM_DECISION_WORDING_RECONCILIATION`
- Priority: P1
- Status: VALIDATING
- Owner: system implementation
- Source: `G:/Download/first_layer_research_status_and_roadmap.docx`
- Market regime: `ai_after_chatgpt`

## Background

TRADING-2268 correctly found that the Norgate 2Y trial did not prove
incremental first-layer trend value. The generated owner memo still exposed
`purchase_platinum_recommendation=yes` with
`purchase_rationale=stress_window_required`. That wording is ambiguous because
it can be read as trial-proven purchase support, while the actual conclusion is
that the trial-based purchase recommendation is no and a paid 2021-2024 stress
window experiment is only conditionally worth owner review.

## Scope

- Update the TRADING-2268 partial evidence review generator so decision outputs
  distinguish trial evidence from an owner-approved paid stress-window
  experiment.
- Regenerate the Norgate 2Y partial evidence review, Platinum decision memo,
  conclusion matrix, and tracked review YAML/JSON artifacts.
- Update docs, report/catalog entries, system flow, task register, and tests so
  downstream readers cannot interpret the 2Y trial as a direct purchase or
  first-layer reopen recommendation.

## Required Fields

The ready-path outputs must include:

- `trial_based_purchase_recommendation = no`
- `stress_window_paid_experiment_recommendation = conditional_yes`
- `owner_decision_required = true`
- `purchase_allowed_without_owner_approval = false`
- `purchase_allowed = false`

The outputs must preserve:

- `local_signal_evidence_reason = no_incremental_value`
- `trial_2y_feature_value = weak`
- `full_history_needed_for_final_answer = true`
- `primary_window_validated = false`
- `model_ready_for_2021_primary_window = false`
- `reopen_gate_allowed = false`
- `promotion_allowed = false`
- `paper_shadow_allowed = false`
- `production_allowed = false`
- `broker_action = none`

## Out Of Scope

- Do not buy or enable Norgate Platinum.
- Do not reopen first-layer, v4, minimal forward diagnostic, promotion,
  paper-shadow, production, or broker paths.
- Do not implement TRADING-2270 through TRADING-2273 in this change.
- Do not mix unrelated workspace cleanup changes into this task.

## Acceptance Criteria

- Generated JSON/YAML/Markdown artifacts expose the split decision wording and
  no longer present the trial result as direct `yes` purchase evidence.
- Guardrail tests assert the split fields and disabled gates.
- Report registry, artifact catalog, system flow, and task register describe the
  new semantics.
- Focused pytest and documentation/task governance tests pass.

## Progress Notes

- 2026-06-28: Created task from attached roadmap and started implementation.
- 2026-06-28: Implementation completed and moved to `VALIDATING`. Updated
  Norgate partial-evidence generator, policy, tests, generated artifacts,
  report registry, artifact catalog, system flow, TRADING-2268 requirement
  notes, and task register. Real run now outputs
  `purchase_platinum_recommendation=no`,
  `trial_based_purchase_recommendation=no`,
  `stress_window_paid_experiment_recommendation=conditional_yes`,
  `purchase_allowed=false`, and all strategy gates false.
- 2026-06-28: Validation passed: Ruff, compileall, focused parallel pytest
  (64 passed), `git diff --check`, task-register terminal status guard, and
  `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  (193 passed). Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260628T114156Z/test_runtime_summary.json`.
