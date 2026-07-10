# TRADING-2438M1C Owner Mapping Resolution and PIT Replay Pre-Registration Plan
最后更新：2026-07-10

**Date:** 2026-07-10
**Base commit:** `e7456aa8`
**Current state:** M1 strict validation passes with `APPROVE/APPROVE/REDEFINE`, but `M2 eligible=0` because owner mappings and metric-policy details are incomplete.

## 1. Next step

Do not run real PIT replay yet.

Split the remaining work into:

1. **Read-only baseline contract inventory**
   - Resolve actual repository IDs for signals, confirmations, vetoes, regimes, exposure units, and transition rules.
   - Do not create candidate parameters or thresholds.
2. **Owner pre-registration**
   - Fill the owner-review, metric contract, and screening policy using only inventory-confirmed IDs.
   - Freeze policy version, hash, approval timestamp, and approval commit before M2.

This is evidence collection, not another control-plane wrapper.

## 2. Baseline mapping inventory

Generate:

```text
outputs/research_strategies/growth_tilt_owner_mapping_inventory/
  baseline_signal_inventory.json
  baseline_confirmation_inventory.json
  baseline_veto_inventory.json
  baseline_regime_inventory.json
  baseline_exposure_unit_inventory.json
  baseline_transition_trace_sample.json
docs/research/growth_tilt_owner_mapping_inventory.md
```

Required fields:

```yaml
baseline:
  config_id:
  config_version:
  decision_trace_schema_version:
  evaluation_cadence:
  exposure_unit:
  target_exposure_field:
  current_exposure_field:

signals:
  - signal_id:
    output_path:
    channel:
    source_family:
    pit_approved:
    allowed_usage:
    diagnostic_only:
    callable_runtime_source:

confirmations:
  - confirmation_id:
    transition:
    hard_or_soft:
    current_rule:
    required_steps:
    threshold:
    output_path:

vetoes:
  - veto_id:
    hard_or_soft:
    output_path:
    priority:
    pit_approved:

regimes:
  - regime_id:
    output_path:
    allowed_transitions:

exposure:
  unit:
  minimum_increment:
  maximum_value:
  baseline_transition_caps:
```

An unresolved conceptual mapping must keep M2 blocked.

## 3. Candidate A owner recommendation

```yaml
candidate_id: recovery_reentry_speedup_guard
decision: APPROVE
role: RECOVERY_REENTRY_TIMING_ACCELERATOR
```

Use the existing defensive-channel recovery permission output, not a raw indicator:

```yaml
recovery_signal:
  preferred_semantic_id: re_risk_allowed_probability
  required_channel: defensive
  preferred_source_family: drawdown_recovery
  raw_indicator_mapping_allowed: false
```

The exact runtime ID/path must come from the inventory.

Define speedup only as reduced persistence:

```yaml
speedup:
  mode: REDUCE_RECOVERY_PERSISTENCE_BY_ONE_STEP
  candidate_required_steps: baseline_required_steps_minus_one
  minimum_required_steps: 1
  maximum_lead_steps: 1
```

Applicable transition:

```yaml
from: [defensive]
to: [neutral]
excluded_current_states: [risk_off]
```

Recommended screening cap:

```yaml
provisional_exposure:
  fraction_of_remaining_gap: 0.25
  absolute_qqq_equivalent_delta_cap: 0.05
  cap_rule: MIN_OF_FRACTION_AND_ABSOLUTE_CAP
  target_exposure_override_allowed: false
  tqqq_increase_allowed: false
```

Expiry and rollback:

```yaml
maximum_active_steps: 2
expire_when_baseline_confirms: true
expire_when_recovery_signal_false: true
expire_when_regime_scope_ends: true
on_any_hard_veto: RETURN_TO_BASELINE_PATH
on_pit_or_data_failure: BLOCKED
```

## 4. Candidate B owner recommendation

```yaml
candidate_id: false_risk_off_confirmation_relaxation
decision: APPROVE
role: DEFENSIVE_ENTRY_SOFT_CONFIRMATION_GRACE
```

Select exactly one baseline confirmation that:

```text
- participates in neutral/constructive -> defensive;
- is classified as soft;
- has a PIT-approved callable runtime output;
- is not data quality, missing data, extreme volatility, hard drawdown,
  rate shock, event shock, or emergency risk-off;
- can be the sole condition causing the defensive transition.
```

Recommended first rule:

```yaml
relaxation:
  mode: ONE_STEP_GRACE
  selected_soft_confirmation_id: RESOLVE_FROM_BASELINE_INVENTORY
  baseline_rule: RESOLVE_FROM_BASELINE_INVENTORY
  candidate_rule: DELAY_SOFT_CONFIRMATION_EFFECT_BY_ONE_STEP
  grace_steps: 1
  remove_confirmation_entirely: false
```

Applicable transition:

```yaml
from: [neutral, constructive]
to: [defensive]
excluded_current_states: [risk_off]
```

Exposure protection:

```yaml
retained_exposure:
  maximum_qqq_equivalent_delta_vs_baseline_defensive_target: 0.05
  maximum_value: PRE_TRIGGER_BASELINE_EXPOSURE
  target_exposure_increase_allowed: false
  tqqq_increase_allowed: false
```

Expiry:

```yaml
maximum_active_steps: 1
expire_when_selected_confirmation_persists: true
expire_when_regime_scope_ends: true
auto_extension_allowed: false
```

## 5. Hard-veto mapping

Use the complete set of callable, PIT-valid baseline hard vetoes. Do not invent a candidate-specific set.

Expected semantic categories:

```text
risk_off_veto
volatility/high-volatility veto
rates-liquidity/rate-shock veto
trend-break/emergency-drawdown veto
```

Conditional:

```text
event_risk_veto:
  include only with validated PIT contract

tqqq_veto:
  preserve if present; A/B still cannot increase TQQQ
```

Owner rule:

```yaml
hard_veto_policy:
  mapping_source: BASELINE_RUNTIME_INVENTORY
  bypass_allowed: false
  candidate_specific_removal_allowed: false
  unresolved_hard_veto: BLOCK_M2
```

## 6. Relative delta policy

```yaml
relative_delta_policy:
  numerical_epsilon: 1.0e-12
  minimum_semantic_denominator: 1.0e-8
  denominator_below_minimum: BLOCKED_BASELINE_MEASURE_TOO_SMALL
  use_epsilon_as_substitute_value: false
```

Computation:

```text
if abs(baseline_measure) < minimum_semantic_denominator:
    BLOCKED_BASELINE_MEASURE_TOO_SMALL
else:
    delta = (candidate_measure - baseline_measure) / abs(baseline_measure)
```

Epsilon is only a floating-point guard. It must not turn a zero baseline into an artificial relative result.

## 7. Empty-event policy

```yaml
empty_event_policy:
  minimum_primary_event_count: 5
  no_eligible_events: BLOCKED_NO_ELIGIBLE_EVENTS
  eligible_but_fewer_than_minimum: BLOCKED_INSUFFICIENT_PRIMARY_EVENTS
  both_candidate_and_baseline_zero_events: BLOCKED_NO_ELIGIBLE_EVENTS
  empty_events_equal_zero_improvement: false
```

## 8. Event metric definitions

### False risk-off

```yaml
definition_id: FALSE_RISK_OFF_OPPORTUNITY_COST_V1
event_anchor: defensive exposure reduction while no hard risk-off veto is active
horizon_steps: 5
reference: FROZEN_GROWTH_REFERENCE_BASKET
measure: OPPORTUNITY_COST
overlap_policy: NON_OVERLAPPING_EVENT_STARTS
```

```text
underexposure =
  max(0, frozen neutral/constructive reference exposure
         - strategy QQQ-equivalent exposure)

event_cost =
  underexposure * max(0, growth reference forward net return over 5 steps)
```

### Missed upside

```yaml
definition_id: RECOVERY_MISSED_UPSIDE_OPPORTUNITY_COST_V1
event_anchor: approved recovery signal first becomes eligible after a defensive state
horizon_steps: 10
reference: FROZEN_BASELINE_RECOVERY_TARGET_PATH
measure: OPPORTUNITY_COST
overlap_policy: ONE_EVENT_PER_RECOVERY_EPISODE
```

```text
recovery_gap =
  max(0, frozen recovery reference exposure
         - strategy QQQ-equivalent exposure)

event_cost =
  recovery_gap * max(0, growth reference forward net return over 10 steps)
```

### Whipsaw

```yaml
definition_id: EXPOSURE_DIRECTION_REVERSAL_V1
reversal_window_steps: 5
minimum_initial_change_qqq_equivalent: 0.05
minimum_reversal_fraction: 0.50
measure: REVERSED_EXPOSURE_PLUS_COST
```

A whipsaw occurs when exposure changes at least 5 percentage points and reverses at least 50% within five evaluation steps.

## 9. Screening policy pre-registration

```yaml
policy:
  policy_id: growth_tilt_candidate_pit_screening_policy_v1
  policy_class: PIT_REPLAY_SCREENING_ONLY
  owner_id: USE_EXISTING_STABLE_REPOSITORY_OWNER_ID
  version: 1.0.0
  approved_at: OWNER_APPROVAL_TIMESTAMP
  approved_commit: OWNER_APPROVAL_COMMIT
  source_hash: SHA256_OF_CANONICAL_POLICY
  result_visibility_at_approval: NONE
  expires_after_completed_replay_rounds: 1
```

Keep the existing first-round boundaries unless changed before results are visible:

```yaml
return_delta_vs_baseline: {comparator: GTE, value: 0.0, unit: percentage_point}
max_drawdown_delta_vs_baseline: {comparator: LTE, value: 0.50, unit: percentage_point}
turnover_delta_vs_baseline: {comparator: LTE, value: 0.10, unit: relative_fraction}
whipsaw_delta: {comparator: LTE, value: 0.10, unit: relative_fraction}

recovery_reentry_speedup_guard:
  primary: {metric: missed_upside_delta, comparator: LTE, value: -0.05}
  secondary: {metric: false_risk_off_delta, comparator: LTE, value: 0.05}

false_risk_off_confirmation_relaxation:
  primary: {metric: false_risk_off_delta, comparator: LTE, value: -0.05}
  secondary: {metric: missed_upside_delta, comparator: LTE, value: 0.00}
```

`PASS` remains screening-only and owner-review-required.

## 10. M1C validation

Before M2 eligibility becomes non-zero:

```text
A recovery signal resolves to a callable defensive-channel output
A persistence rule resolves to a baseline transition rule
B confirmation is exactly one callable soft confirmation
A/B hard veto sets match the baseline hard-veto inventory
all mappings are PIT-valid
regime IDs and exposure units resolve
event definitions are complete
epsilon and empty-event policies are complete
policy owner/version/hash/approval commit are present
policy approval predates result visibility
```

Expected result:

```text
decision_count=3/3
approve/redefine=2/1
owner_mapping_ready=2/2
metric_contract_ready=1/1
screening_policy_ready=1/1
m2_eligible=2
strict_errors=0
```

## 11. Existing full-suite failures

The current `5166 passed / 46 failed` does not need to block owner pre-registration. Real replay evidence should only be accepted when:

```text
- all new M1/M2 focused tests pass;
- all tests for the new CLI and its parameter contract pass;
- the 46 failures are frozen in a known-failure manifest;
- node IDs and messages match the pre-M1 baseline;
- none of them invoke or validate the new growth-tilt commands;
- no new failure is introduced.
```

## 12. Recommended task

```text
TRADING-2438M1C
Growth Tilt Baseline Runtime Mapping Inventory and Owner Pre-Registration
```

Definition of done:

```text
read-only baseline mapping inventory generated
A/B mappings resolved to actual runtime IDs
hard veto set frozen
regime and exposure units frozen
epsilon and empty-event policy approved
three event definitions approved
screening policy owner/version/hash/approval commit frozen
strict M1 reports M2 eligible=2
no real replay executed in this task
```

Then proceed to:

```text
TRADING-2438M2
Approved Candidate Compute-Plane Binding and Real PIT Replay
```

## 13. Implementation progress

2026-07-10: Read-only inventory implementation is complete. The strict command generated the six required inventory artifacts plus the primary JSON and Chinese report with source hashes and zero source/strict errors. The inventory itself is ready for owner review, but `owner_mapping_ready=0/2` and M2 remains blocked because the repository has no PIT-approved recovery output plus persistence contract, no exactly-one callable PIT soft confirmation, no complete callable/PIT hard-veto set, no governed transition contract, and no complete QQQ-equivalent scalar binding.

The numerical relative-delta policy, detailed empty-event branches, three event definitions, stable policy owner, and unchanged first-round screening boundaries are now recorded as `PENDING_OWNER_PREREGISTRATION`. Approval timestamp, approval commit, and canonical policy hash remain unset; no owner approval is inferred. No PIT replay, backtest, scoring, paper-shadow, production, portfolio-weight mutation, or broker action was executed.

Validation passed: 63 focused inventory/M1 tests, 125 focused integration/documentation tests, 198 fast-tier tests, and 197 contract-validation tests, plus scoped Ruff, compileall, strict real CLI runs, and `git diff --check`.

2026-07-10 / M1D interpretation correction: `do_not_de_risk_pass=false` is an offline channel-selection result, not a current runtime observation. It does not by itself invalidate the callable `re_risk_allowed_probability` producer. Candidate A remains mapping-blocked because per-output PIT lineage, baseline compiler consumption, recovery persistence/reset semantics, and effective transition timing are absent. The M2 eligibility conclusion remains zero.
