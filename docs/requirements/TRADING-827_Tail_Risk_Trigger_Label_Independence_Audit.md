# TRADING-827: Tail-Risk Trigger/Label Independence Audit
最后更新：2026-06-23

## Context

TRADING-826 identified the highest-severity blocker as TRADING-822
`anti_leakage_critical`: the tail-risk fallback trigger and the validation label may share the
same controlled historical risk definition. The owner originally proposed this audit as
TRADING-826, but that ID is already used by the blocker diagnostic artifact; this task uses the
next unique ID, TRADING-827.

This task answers one question: whether the current strategy uses the same risk definition to
trigger fallback and to validate the fallback. If yes, return metrics and precision/recall evidence
must be treated as temporarily untrustworthy until an independent forward outcome validation exists.

## Safety Boundary

This task is read-only:

- `controlled_only=true`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`

It must not modify strategy rules, policy promotion, paper-shadow state, production weights, or
broker/order state. It must not recommend promotion.

## Scope

TRADING-827 audits the dependency graph around `tail_risk_benchmark_fallback`:

- trigger fields and derived trigger logic
- label/outcome fields and derived validation logic
- forward outcome fields that would be acceptable for independent validation
- decision-time versus forward-window visibility

Required output matrices:

- `overlap_matrix`
- `time_window_matrix`
- `derived_dependency_matrix`

If trigger and label share core fields or derived logic, the artifact status must be `BLOCKED`.

## Acceptance Criteria

- Enumerate every trigger field used by the fallback trigger.
- Enumerate every label, outcome, and forward outcome field used or required by validation.
- Check direct field overlap, derived overlap, and time-window overlap.
- Check that trigger inputs are visible at `decision_time`.
- Check that label/outcome fields are strictly forward-window or evaluation-time fields.
- Output `overlap_matrix`, `time_window_matrix`, and `derived_dependency_matrix`.
- Output `BLOCKED` if trigger and label share core fields or derived logic.
- Explicitly answer whether the same risk definition is used for trigger and validation.
- Keep all safety fields fixed to controlled-only false/none values.
- Register the report in `config/report_registry.yaml` and update artifact catalog/system flow/tests.

## Follow-Up Queue

- TRADING-828: independent forward outcome validation using objective future results:
  `future_5d_max_drawdown`, `future_10d_max_drawdown`, `future_20d_max_drawdown`,
  `future_20d_realized_vol`, `future_20d_underperform_vs_static`,
  `future_20d_recovery_failure`, and `future_gap_down_event`.
- TRADING-829: leakage stress tests covering signal lag, label permutation, timestamp boundary,
  feature availability, forward-window overlap, and trigger-label correlation.
- TRADING-830: regime-stratified independent validation.
- TRADING-831: only after the above pass and owner approval, a read-only promotion gate rerun.

## Progress Notes

- 2026-06-22: Added from owner request and moved TRADING-827 to `IN_PROGRESS`. The owner suggested
  TRADING-826, but TRADING-826 already exists as the tail-risk fallback blocker diagnostic, so the
  implementation uses TRADING-827 and records the mapping.
- 2026-06-22: Implemented and ran
  `aits research strategies tail-risk-trigger-label-independence-audit`. The generated artifact is
  `outputs/research_strategies/value_surface_review/tail_risk_trigger_label_independence_audit.json/md`.
  The audit status is `BLOCKED`: direct overlap fields are `large_loss_case`, `tail_loss_case`, and
  `long_horizon_failure_case`; derived overlap flows through `tail_risk_signal_high`,
  `fallback_triggered`, `trigger_labels`, `trigger_reason`, and `trigger_score`; trigger
  decision-time visibility is not proven for the label-proxy fields. The artifact answers `YES` to
  the same-risk-definition question and marks return metrics temporarily untrustworthy until
  independent forward outcome validation exists.
- 2026-06-22: Validation passed: focused independence pytest 1 passed, focused falsification pytest
  9 passed, focused CLI/registry pytest 3 passed, `fast-unit` 187 passed,
  `contract-validation` 186 passed, `report-validation` 55 passed, `ruff check .`, changed-file
  Black check, `python -m compileall src tests scripts`, and `git diff --check`.
