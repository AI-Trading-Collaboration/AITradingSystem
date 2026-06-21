# TRADING-821 to TRADING-825: Tail-Risk Fallback Falsification Audit

## Context

TRADING-816 to TRADING-820 completed the first controlled-only validation chain for
`tail_risk_benchmark_fallback`. The historical and controlled audit metrics are strong, but
the next stage must deliberately seek evidence of failure, overfit, count-universe confusion,
label coupling, leakage, threshold fragility, regime concentration, and immature forward
evidence.

This document records the implementation plan for the owner-requested TRADING-821 to
TRADING-825 batch. The current task register already has a separate
`TRADING-821_VALIDATION_RUNTIME_CONTROLLED_STRATEGY_SPLIT` item, so the register row for this
batch uses the grouped id `TRADING-821_to_825_TAIL_RISK_FALLBACK_FALSIFICATION_AUDIT` while
preserving the owner-facing task numbers inside this document and generated artifacts.

## Safety Boundary

All outputs remain controlled research only:

- `controlled_only=true`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`

Allowed final review outcomes are limited to:

- `CONTROLLED_RESEARCH_CONTINUE`
- `CONTROLLED_RESEARCH_BLOCKED`
- `NEEDS_MORE_FORWARD_EVIDENCE`
- `PROMOTION_NOT_ALLOWED`

This batch cannot output `PROMOTION_READY`, `PAPER_SHADOW_READY`, or `PRODUCTION_READY`, and it
cannot modify policy config in a way that opens promotion, paper-shadow, production weights, or
broker action.

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-821 | Tail-risk fallback audit universe and count reconciliation | Reconcile TRADING-816, 817, 818, and 819 count universes; missing denominator/date/window fields must prevent `RECONCILED`; unexplained discrepancies block controlled research | VALIDATING |
| TRADING-822 | Anti-leakage audit for fallback trigger, tail-risk label, and outcome | Audit feature availability timestamps, trigger/label overlap, outcome horizon separation, and PIT/revision risk; critical leakage blocks controlled research | VALIDATING |
| TRADING-823 | Threshold sensitivity and perturbation test | Generate baseline plus threshold, lag, horizon, benchmark, and cost/slippage variants; detect cliffs and fragility without enabling promotion | VALIDATING |
| TRADING-824 | Regime-segmented fallback robustness review | Produce calendar, volatility, trend, macro/liquidity availability, and tail-event severity segments; concentrated or low-sample evidence cannot be called robust | VALIDATING |
| TRADING-825 | Forward maturity monitor and promotion-readiness scoreboard | Separate matured, pending, missing, and invalid forward records; insufficient or degraded forward evidence keeps promotion blocked | VALIDATING |
| Review board | Controlled review board integration | Read all five new reports and fail closed on anti-leakage, missing reconciliation, degraded forward evidence, or missing artifacts while preserving controlled-only safety fields | VALIDATING |

## Implementation Plan

1. Extend `config/research/controlled_strategy_next_stage_research.yaml` with blocking-only
   policies for TRADING-821 to TRADING-825. These thresholds are controlled diagnostic
   guardrails, not production or promotion thresholds.
2. Add five CLI commands under `aits research strategies`:
   - `tail-risk-fallback-audit-universe-reconciliation`
   - `tail-risk-fallback-anti-leakage-audit`
   - `tail-risk-fallback-threshold-sensitivity`
   - `tail-risk-fallback-regime-segmented-robustness`
   - `tail-risk-fallback-forward-maturity-scoreboard`
3. Extend `tail-risk-policy-controlled-review-board` with optional inputs for the new reports
   and deterministic fail-closed decision rules.
4. Register all artifacts in `config/report_registry.yaml`, document them in
   `docs/artifact_catalog.md`, and update `docs/system_flow.md`.
5. Add focused tests for the five new builders and review board integration, plus CLI and
   documentation contract checks.
6. Run required validation with parallel pytest by default through
   `scripts/run_validation_tier.py` where applicable.

## Acceptance Criteria

- TRADING-821 explains why 816 `fallback_trigger_count`, 817 `TP+FP`, 818
  `benchmark_upside_case_count`, and 819 `fallback_trigger_count` are different and prevents
  direct comparison across different universes.
- TRADING-822 flags future features, same-source trigger/label coupling, outcome overlap, and
  unknown PIT/revision status. Any critical leakage prevents a continue decision.
- TRADING-823 includes baseline, threshold, lag, horizon, benchmark, and cost perturbations and
  marks cliff behavior as `SENSITIVITY_FRAGILE` when configured small perturbations degrade key
  metrics.
- TRADING-824 outputs at least calendar, volatility, and trend segments. Missing macro/liquidity
  data is reported as `segment_unavailable` with a reason, not fabricated.
- TRADING-825 excludes `pending_maturity` and `missing_outcome` records from matured metrics,
  reports why promotion is blocked, and never emits a promotion-ready assessment.
- The updated review board never opens promotion, paper-shadow, production weights, or broker
  action regardless of favorable controlled evidence.

## Progress Notes

- 2026-06-22: Added this requirement document and moved the grouped task-register item to
  `IN_PROGRESS`. This batch starts from TRADING-816 to TRADING-820 controlled outputs and
  intentionally changes the research question from proof-seeking to falsification.
- 2026-06-22: Implemented the five TRADING-821 to TRADING-825 controlled-only CLI/artifact
  builders, extended the review board with fail-closed inputs, registered artifacts, and updated
  the artifact catalog/system flow. Focused tests passed with `10 passed`; validation tiers
  passed with `fast-unit` 185 passed, `contract-validation` 184 passed, and `report-validation`
  55 passed. `ruff check .`, `python -m compileall .`, `git diff --check`, and Black check on
  changed Python files passed; full `python -m black --check .` still reports pre-existing
  formatting debt outside this task scope, so this batch remains in `VALIDATING` pending owner
  review rather than `DONE`.
- 2026-06-22: Real default CLI artifacts were generated from the existing TRADING-816 to
  TRADING-820 outputs. TRADING-821 returned `PARTIALLY_RECONCILED` with 4 count summaries and
  20 missing-field records; TRADING-822 returned `ANTI_LEAKAGE_BLOCKED` with 1 critical issue;
  TRADING-823 returned `SENSITIVITY_FRAGILE`; TRADING-824 returned `REGIME_CONCENTRATED`;
  TRADING-825 returned `FORWARD_PENDING`; and the updated review board returned
  `CONTROLLED_RESEARCH_BLOCKED` with `next_recommended_action` =
  `resolve_controlled_research_blockers_before_interpreting_fallback`. Safety fields stayed
  controlled-only with no promotion, paper-shadow, production weight, or broker action.
