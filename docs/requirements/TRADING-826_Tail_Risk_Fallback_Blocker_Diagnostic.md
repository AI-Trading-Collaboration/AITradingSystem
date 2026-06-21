# TRADING-826: Tail-Risk Fallback Blocker Diagnostic

## Context

TRADING-821 to TRADING-825 generated controlled-only falsification artifacts for
`tail_risk_benchmark_fallback`, and the latest
`tail_risk_policy_controlled_review_board` can now return `CONTROLLED_RESEARCH_BLOCKED`.
The owner needs a read-only diagnostic artifact that explains which input report caused the
blocked decision and summarizes blocker, warning, and root-cause evidence without changing any
strategy or production state.

## Safety Boundary

This task is read-only:

- `controlled_only=true`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`

It must not suggest promotion, paper-shadow, production weight changes, or broker action.

## Acceptance Criteria

- Read the latest `tail_risk_policy_controlled_review_board` artifact by default.
- Expand TRADING-821 to TRADING-825 input report status, warnings, blockers, and
  `promotion_block_reason`.
- Identify which task or tasks triggered `CONTROLLED_RESEARCH_BLOCKED`.
- Sort blockers by severity:
  1. anti-leakage critical
  2. universe reconciliation incomplete
  3. forward degraded
  4. sensitivity fragile
  5. regime concentrated
  6. forward insufficient / pending
- Output a next recommended action that never recommends promotion.
- Provide artifact path and a report registry entry.

## Progress Notes

- 2026-06-22: Added requirement from owner request and moved task to `IN_PROGRESS`.
- 2026-06-22: Implemented and ran
  `aits research strategies tail-risk-fallback-blocker-diagnostic`. The generated artifact is
  `outputs/research_strategies/value_surface_review/tail_risk_fallback_blocker_diagnostic.json/md`.
  It reads the latest review board, expands all TRADING-821 to TRADING-825 input reports, and
  reports final `CONTROLLED_RESEARCH_BLOCKED` as triggered by TRADING-822
  `anti_leakage_critical`. Severity findings are ordered as TRADING-822 anti-leakage critical,
  TRADING-823 sensitivity fragile, TRADING-824 regime concentrated, and TRADING-825 forward
  pending. The next recommended action is
  `decouple_trigger_and_label_or_add_independent_forward_outcome_validation`; no promotion,
  paper-shadow, production weight, or broker action is recommended.
- 2026-06-22: Validation passed: focused falsification pytest 8 passed, focused CLI/registry
  pytest 11 passed, `fast-unit` 186 passed, `contract-validation` 185 passed,
  `report-validation` 55 passed, `ruff check .`, changed-file Black check,
  `python -m compileall src tests scripts`, and `git diff --check`.
