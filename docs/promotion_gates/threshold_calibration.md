# Promotion Gate Threshold Calibration

最后更新：2026-06-15

TRADING-348 adds a governed threshold policy for the TRADING-346 formal research
method contract. The policy lives at `config/research/promotion_gate_thresholds.yaml`.

## Scope

This calibration is governance-only. It documents how current discrete evidence
statuses are interpreted for research formalization and paper-shadow eligibility.
It does not change the formal contract decision logic, does not fit thresholds
to force `median_plus_regime_mismatch_filter` to pass, and does not create
official target weights, orders, broker actions, or production state.

## Pilot Bands

|Family|Required band|Reason for conservative default|
|---|---|---|
|stress strength|`STRONG`|A candidate should not enter formalization with uneven stress behavior.|
|drawdown mismatch reduction|`IMPROVED`|The candidate must reduce the specific drawdown mismatch failure mode it was designed to address.|
|flip/rotation reduction|`flip=IMPROVED` and `rotation=IMPROVED`|Improving only one churn channel can leave the other channel as an unaddressed failure path.|
|A/B review confidence|`PROMISING`|Formal research resources require comparison evidence against existing alternatives.|
|confirmation target count|`>=3`|At least three forward confirmation targets reduce reliance on one-off qualitative evidence.|

## Review Conditions

The policy is marked `pilot_baseline`. Recalibration requires owner review and
new evidence, especially after:

- the first owner-reviewed paper-shadow weekly cycle;
- a material change to stress or A/B evidence generation;
- replacement of manual drawdown/flip labels with data-backed event extraction;
- enough post-ChatGPT forward evidence exists to estimate which thresholds are predictive;
- any production promotion workflow is proposed.

## Reader Brief Contract

The calibration report writes a Reader Brief section with:

- `promotion_threshold_calibration_id`;
- `promotion_threshold_policy_id`;
- `promotion_threshold_policy_version`;
- `promotion_threshold_status`;
- `promotion_threshold_current_interpretation`;
- `promotion_threshold_stress_required`;
- `promotion_threshold_confirmation_minimum`;
- `promotion_threshold_validation_status`;
- `promotion_threshold_next_action`.

Missing calibration artifacts must be displayed as `MISSING`; Reader Brief must
not run calibration, rewrite thresholds, or infer promotion approval.
