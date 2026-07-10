# TRADING-2438M1D Growth Tilt Baseline Contract Decision Pack and Implementation Plan
最后更新：2026-07-10

**Document date:** 2026-07-10  
**Base commit:** `407720dc38832ecaa685388170caede10756646f`  
**Current state:** M1C inventory completed; runtime mapping remains blocked; no real PIT replay executed.  
**Scope:** baseline contract decisions only. This task must not run M2 replay and must not generate runtime metrics.

---

# 1. Executive decision

M1C has shown that the blocker is no longer candidate configuration completeness. The baseline growth-tilt system does not yet expose a sufficiently governed interface for a candidate overlay to consume.

The recommended decisions are:

| Area | Decision | Reason |
|---|---|---|
| Recovery persistence | **ADD governed baseline contract** | Existing callable recovery producer is not enough; transition persistence and reset semantics are undefined |
| Candidate A | **Keep APPROVE, M2-ineligible pending contracts** | Its research mechanism remains coherent if it only changes an existing baseline persistence rule |
| Candidate B soft confirmation | **Do not invent a new soft confirmation** | No exactly-one callable PIT soft confirmation exists |
| Candidate B | **Change to REDEFINE unless an existing latent soft component is proven** | Current approved semantics depend on a baseline component that does not exist |
| Hard-veto interface | **ADD governed aggregate contract** | Safety constraints must be callable, PIT-traceable, complete, and reusable |
| `event_risk_veto` | **Do not fabricate** | Classify as resolved, not applicable, or blocking depending on actual baseline role and PIT availability |
| Regime transition | **ADD governed baseline transition contract** | Candidate timing changes cannot be interpreted without requested/applied state and transition priority |
| Exposure scalar | **ADD or formalize the baseline-native scalar** | Do not force a QQQ-equivalent mapping if the repository has no authoritative conversion |
| QQQ-equivalent binding | **Defer unless already derivable from governed baseline logic** | An ad hoc beta or leverage conversion would create a new investment assumption |
| Candidate C | **Remain REDEFINE** | Not part of the first M2 wave |

M1D should therefore be split into:

```text
TRADING-2438M1D1
Baseline Contract Decision Pack

TRADING-2438M1D2
Approved Baseline Contract Implementation and Readiness Validation
```

Neither task runs a real PIT replay.

---

# 2. Important interpretation of the M1C audit

## 2.1 `do_not_de_risk_pass=false` is not by itself a mapping failure

A current runtime value of `false` is an observed state, not proof that the producer or mapping is invalid.

The actual blocker is that the repository does not yet define:

```text
how re_risk_allowed_probability relates to do_not_de_risk_pass
which output is a prerequisite, guard, diagnostic, or transition request
what persistence the baseline requires
what resets accumulated persistence
when a transition becomes effective
```

M1D must classify these semantics.

Candidate A must not force either output to `true`. It may only modify a governed persistence rule after all baseline prerequisites and hard vetoes are satisfied.

## 2.2 Mapping readiness and candidate activation must remain separate

A contract can be mapping-ready even when its current value is false.

```yaml
contract_readiness:
  producer_callable: true
  output_path_resolved: true
  semantics_registered: true
  pit_lineage_valid: true

runtime_observation:
  value: false
  candidate_active: false
```

Do not require a signal to be currently true in order to approve its compute-plane mapping.

---

# 3. Candidate disposition

## 3.1 Candidate A — `recovery_reentry_speedup_guard`

Recommended state:

```yaml
decision: APPROVE
m2_eligibility: BLOCKED_PENDING_BASELINE_CONTRACTS
candidate_role: RECOVERY_REENTRY_TIMING_ACCELERATOR
```

It remains valid only under this invariant:

> Candidate A reduces an existing baseline recovery persistence requirement by one evaluation step. It does not create a new recovery signal, bypass a baseline gate, change the recovery target, or accelerate the post-confirmation ramp.

Eligibility conditions:

```text
baseline recovery signal/permission output is callable
baseline required persistence is explicitly governed
baseline required persistence >= 2
reset and missing-data semantics are governed
all hard vetoes are resolved
requested/applied regime transitions are governed
native exposure scalar and cap units are governed
```

If baseline persistence is already `1`, Candidate A has no valid “minus one step” variant and must be changed to `REDEFINE` or `WITHDRAW`.

## 3.2 Candidate B — `false_risk_off_confirmation_relaxation`

Recommended immediate state:

```yaml
decision: REDEFINE
reason: NO_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION
```

Do not add a new soft confirmation only to preserve the previously approved candidate. That would modify baseline semantics and candidate semantics at the same time, making the replay uninterpretable.

Two valid routes exist.

### Route B1 — prove an existing latent soft component

Candidate B may return to `APPROVE` only if M1D proves that an existing baseline condition:

```text
has a callable runtime output
has PIT-valid lineage
participates in the baseline defensive transition
is independently observable in the decision trace
is explicitly classified as soft
can be the sole non-hard cause of the transition
```

A documentation alias is insufficient; the component must exist on the compute path.

### Route B2 — preferred redefinition

If no such component exists, redefine B as:

```text
non_hard_defensive_entry_persistence_guard
```

Role:

```yaml
candidate_role: NON_HARD_DEFENSIVE_ENTRY_PERSISTENCE_GUARD
changes_soft_component: false
changes_aggregate_non_hard_request_persistence: true
changes_hard_veto_behavior: false
```

Mechanism:

```text
baseline aggregate non-hard defensive request appears
→ candidate requires one additional consecutive evaluation step
→ any hard veto applies immediately with no grace
→ candidate never increases exposure above the pre-request baseline path
```

Parameter relationship:

```yaml
candidate_required_steps: baseline_required_steps_plus_one
maximum_added_steps: 1
hard_veto_bypass_allowed: false
auto_extension_allowed: false
```

If the repository also lacks a callable aggregate non-hard defensive request, B should be `WITHDRAW`, not implemented through inferred raw indicators.

## 3.3 Candidate C

```yaml
decision: REDEFINE
m2_eligible: false
```

No change in M1D. It remains outside the first replay wave.

---

# 4. Required baseline contracts

## 4.1 Recovery persistence contract

Add a reusable baseline contract, not a Candidate A-specific helper.

Suggested ID:

```text
growth_tilt_recovery_persistence_contract_v1
```

Required schema:

```yaml
contract_id:
version:
owner:
effective_from:

recovery_permission:
  signal_id:
  producer_entrypoint:
  output_path:
  unit:
  comparator:
  threshold:
  pit_lineage_ref:

prerequisites:
  do_not_de_risk_field:
  do_not_de_risk_semantics:
    # HARD_PREREQUISITE | SOFT_PREREQUISITE | DIAGNOSTIC_ONLY
  required_fields:

persistence:
  baseline_required_consecutive_steps:
  maximum_gap_steps:
  reset_on_false:
  reset_on_missing:
  reset_on_hard_veto:
  evaluation_cadence:

transition:
  requested_transition_id:
  effective_timing:
  target_state_source:

missing_policy:
  missing_signal:
  missing_prerequisite:
  invalid_provenance:
```

Owner decisions required:

```text
Is do_not_de_risk_pass a hard prerequisite, soft prerequisite, or diagnostic?
What is the baseline persistence count?
What resets persistence?
Does a missing observation reset or block?
Does the transition apply on the same step or next step?
```

Candidate A contract:

```yaml
candidate_required_consecutive_steps:
  expression: max(1, baseline_required_consecutive_steps - 1)
maximum_lead_steps: 1
```

The candidate may not override any other field.

## 4.2 Defensive-entry request contract

Add this contract only if the baseline has a callable aggregate defensive request.

Suggested ID:

```text
growth_tilt_defensive_entry_request_contract_v1
```

Required schema:

```yaml
request:
  request_id:
  producer_entrypoint:
  output_path:
  pit_lineage_ref:
  request_class: AGGREGATE_NON_HARD_DEFENSIVE_REQUEST

persistence:
  baseline_required_consecutive_steps:
  reset_on_false:
  reset_on_missing:
  evaluation_cadence:

hard_veto_override:
  aggregate_hard_veto_ref:
  immediate_application: true

transition:
  requested_transition_id:
  effective_timing:
  target_state_source:
```

Do not decompose an opaque aggregate request into invented soft subconditions.

The redefined B candidate may only change:

```text
baseline_required_consecutive_steps + 1
```

with a maximum added delay of one evaluation step.

## 4.3 Hard-veto aggregate contract

This is required baseline safety infrastructure and should be added.

Suggested ID:

```text
growth_tilt_hard_veto_aggregate_contract_v1
```

Required component schema:

```yaml
hard_veto_components:
  - veto_id:
    semantic_role:
    producer_entrypoint:
    output_path:
    pit_lineage_ref:
    priority:
    active_when:
    missing_policy:
    resolution_status:
      # RESOLVED_CALLABLE
      # EXPLICITLY_NOT_APPLICABLE
      # BLOCKED_NO_PIT_CONTRACT
    not_applicable_rationale:
```

Aggregate output:

```yaml
aggregate:
  output_id: growth_tilt_hard_veto_active
  active: boolean
  active_component_ids: []
  unresolved_component_ids: []
  evaluation_status:
  reason_codes: []
```

Required rules:

```text
candidate cannot remove a baseline hard veto
candidate cannot change veto priority
candidate cannot read raw VIX/rates/trend/event data directly
missing hard-veto evidence is BLOCKED, not false
aggregate must preserve component-level trace
```

### Unresolved components

- `risk_off_veto`: must be resolved if it is part of baseline behavior.
- `trend_break_veto`: must be resolved if it is part of baseline behavior.
- `event_risk_veto`: classify as `RESOLVED_CALLABLE`, `EXPLICITLY_NOT_APPLICABLE_TO_THIS_BASELINE`, or `BLOCKED_NO_PIT_CONTRACT`.

`EXPLICITLY_NOT_APPLICABLE` is allowed only if the baseline contract proves that event risk is diagnostic/advisory and does not control the relevant transition.

If it is a true hard veto without PIT lineage, M2 remains blocked. Do not silently omit it or exclude event dates after observing results.

## 4.4 Regime transition contract

Suggested ID:

```text
growth_tilt_regime_transition_contract_v1
```

Required schema:

```yaml
states:
  canonical_state_ids: []
  source_field:
  schema_version:

transition_request:
  request_id:
  from_state:
  to_state:
  requested_at:
  request_reason_codes:
  request_source:

transition_application:
  applied: boolean
  applied_at:
  effective_timing:
  applied_state:
  blocked_by:
  superseded_by:

priority:
  hard_veto_priority:
  ordinary_request_priority:
  conflict_resolution:

persistence:
  contract_ref:
  completed_steps:
  required_steps:
```

The contract must distinguish current, requested, applied, blocked, and superseded states. Candidate overlays operate on a transition request, not on an inferred regime label.

## 4.5 Exposure scalar contract

Do not make QQQ-equivalent exposure a requirement unless the repository already contains a governed conversion.

Preferred approach:

```text
use the baseline-native growth-tilt exposure scalar
```

Suggested ID:

```text
growth_tilt_exposure_scalar_contract_v1
```

Required schema:

```yaml
scalar:
  scalar_id:
  source_field:
  unit:
  minimum_value:
  maximum_value:
  minimum_increment:
  interpretation:
  pit_lineage_ref:

targets:
  current_scalar_field:
  requested_target_scalar_field:
  applied_target_scalar_field:

caps:
  baseline_transition_cap:
  hard_risk_cap_ref:

conversion:
  qqq_equivalent_supported: false
  qqq_equivalent_formula_ref:
```

Rules:

```text
no beta-estimated conversion invented for M1D
no leverage multiplier inferred from instrument name
no TQQQ increase through Candidate A or B
candidate cap uses native scalar units
```

If a governed QQQ-equivalent formula already exists, it may be referenced. Otherwise, replace previous `5pp QQQ-equivalent` proposals with an owner-approved native-scalar cap.

---

# 5. M1D1 — Baseline Contract Decision Pack

M1D1 is an owner decision task. It should not implement candidate behavior.

Suggested CLI:

```bash
aits research strategies \
  growth-tilt-baseline-contract-decision-pack \
  --as-of YYYY-MM-DD \
  --strict
```

Required decisions:

```yaml
recovery_persistence:
  create_contract: true
  baseline_required_steps:
  do_not_de_risk_semantics:
  reset_policy:
  effective_timing:

defensive_entry:
  existing_callable_soft_confirmation_found: true|false
  existing_callable_aggregate_non_hard_request_found: true|false
  candidate_b_route: KEEP_APPROVE|REDEFINE_AGGREGATE_PERSISTENCE|WITHDRAW

hard_veto:
  risk_off_veto_resolution:
  trend_break_veto_resolution:
  event_risk_veto_resolution:
  complete_baseline_set:

transition:
  canonical_state_schema:
  requested_applied_split:
  priority_policy:

exposure:
  native_scalar_id:
  unit:
  range:
  minimum_increment:
  qqq_equivalent_supported:
```

Artifacts:

```text
growth_tilt_baseline_contract_decision_pack.json
growth_tilt_candidate_disposition_after_baseline_audit.json
growth_tilt_hard_veto_resolution_matrix.json
growth_tilt_transition_exposure_decision.json
docs/research/growth_tilt_baseline_contract_decision_pack.md
```

M1D1 completion does not make any candidate M2-eligible.

---

# 6. M1D2 — Contract implementation

Implement only contracts approved in M1D1:

```text
recovery persistence
aggregate non-hard defensive request, if it exists
hard-veto aggregate
regime transition
native exposure scalar
```

Recommended module boundaries:

```text
research_quality/
  growth_tilt_recovery_persistence_contract.py
  growth_tilt_defensive_entry_request_contract.py
  growth_tilt_hard_veto_contract.py
  growth_tilt_regime_transition_contract.py
  growth_tilt_exposure_scalar_contract.py
  growth_tilt_baseline_contract_readiness.py
```

Prefer adapters over duplicated logic:

```text
existing baseline producer
→ governed adapter
→ versioned contract output
→ candidate overlay consumer
```

Do not reimplement the underlying signal.

---

# 7. Readiness rules

A contract is ready only when:

```text
producer is callable
output path exists
schema is versioned
semantics are owner-approved
PIT lineage is present
missing policy is explicit
runtime trace can materialize the output
artifact reload preserves the result
```

Candidate A becomes M2-eligible only when:

```text
recovery persistence ready
hard-veto aggregate complete
transition contract ready
native exposure scalar ready
baseline persistence >= 2
owner-approved candidate cap present
screening policy pre-registration complete
```

Redefined Candidate B becomes M2-eligible only when:

```text
aggregate non-hard defensive request ready
baseline persistence defined
hard-veto immediate path ready
transition contract ready
native exposure scalar ready
new B definition receives second owner APPROVE
screening policy is updated and re-hashed before results
```

---

# 8. Focused test plan

## Recovery persistence

1. Callable producer may return false while contract remains ready.
2. Missing producer output is blocked.
3. `do_not_de_risk_pass` classification is mandatory.
4. Persistence increments only on eligible consecutive steps.
5. False observation resets according to policy.
6. Missing observation follows the registered missing policy.
7. Hard veto resets or blocks persistence.
8. Same-step/next-step transition timing is deterministic.
9. Baseline persistence below 2 invalidates Candidate A.
10. Candidate A changes only required persistence by minus one.

## Defensive-entry request

11. No soft confirmation may be synthesized from a candidate name.
12. An independently callable existing soft component can be registered.
13. Documentation-only aliases do not qualify.
14. Aggregate request adapter preserves baseline output.
15. Redefined B adds exactly one persistence step.
16. Hard veto bypasses B grace immediately.
17. B cannot auto-extend grace.
18. Missing aggregate request blocks evaluation.

## Hard veto

19. Every baseline hard veto has a resolution status.
20. Missing callable evidence is not interpreted as inactive.
21. Candidate cannot remove a veto.
22. Candidate cannot change priority.
23. Aggregate output preserves active component IDs.
24. Unresolved hard veto prevents M2 eligibility.
25. `event_risk_veto` cannot be marked not applicable without rationale and baseline evidence.
26. Raw indicators cannot be consumed by the overlay.

## Transition and exposure

27. Current/requested/applied states remain distinct.
28. Invalid transition is blocked.
29. Hard-veto request supersedes ordinary candidate request.
30. Effective timestamp is deterministic.
31. Transition reason codes survive artifact reload.
32. Native scalar unit is mandatory.
33. Candidate delta uses the native scalar.
34. Ad hoc QQQ-equivalent conversion is rejected.
35. TQQQ increase is rejected.
36. Candidate cap cannot exceed baseline hard risk cap.
37. Minimum increment and range are enforced.

## Governance

38. B decision changes to REDEFINE when no callable soft component exists.
39. C remains excluded.
40. No real PIT replay is invoked.
41. No six-metric runtime artifact is generated.
42. Approval timestamp/hash remains empty until owner pre-registration.
43. M2 eligible count remains zero until every required contract is ready.
44. Report registry/catalog/system-flow/task archive remain consistent.

---

# 9. Definition of done

## M1D1

```text
baseline contract decisions are explicit
A remains APPROVE but blocked pending contracts
B is KEEP_APPROVE, REDEFINE, or WITHDRAW based on actual callable evidence
C remains REDEFINE
recovery persistence semantics are owner-decided
hard-veto resolution matrix is complete
transition and native exposure scalar decisions are complete
no replay is run
```

## M1D2

```text
approved baseline contracts are implemented as adapters
all runtime outputs are callable and versioned
hard-veto set is complete or exact blocker remains
transition request/application trace is materialized
native exposure scalar is governed
focused and contract tests pass
no real candidate replay is run
```

Expected honest outcomes include:

```text
A mapping ready, B redefinition pending, M2 eligible=1
A/B mapping ready after second B approval, M2 eligible=2
hard veto unresolved, M2 eligible=0
baseline persistence=1, A requires redefinition, M2 eligible=0
```

The task must not force `M2 eligible=2`.

---

# 10. Recommended route

```text
TRADING-2438M1D1
Baseline Contract Decision Pack

TRADING-2438M1D2
Baseline Contract Adapters and Readiness

TRADING-2438M1E
Candidate Owner Re-Approval and Screening Policy Freeze
  - A final cap approval
  - B redefinition approval if applicable
  - policy owner/timestamp/commit/hash

TRADING-2438M2
Approved Candidate Compute-Plane Binding and Real PIT Replay
```

---

# 11. Final recommendation

Add contracts where the baseline already has real behavior that lacks a governed interface:

```text
YES:
  recovery persistence
  hard-veto aggregate
  transition request/application
  native exposure scalar
  aggregate non-hard defensive request if it actually exists
```

Do not add a new baseline concept solely to rescue a candidate:

```text
NO:
  invented exactly-one soft confirmation
  invented event-risk veto
  inferred QQQ-equivalent conversion
  candidate-specific replacement for unresolved baseline safety logic
```

The main research consequence is that Candidate B should no longer remain automatically approved. The absence of an exactly-one callable PIT soft confirmation is evidence that its original mechanism does not match the current baseline architecture. Redefining it around aggregate non-hard defensive-entry persistence is the cleanest alternative, provided that aggregate request is itself callable and PIT-governed.

---

# 12. Implementation progress

2026-07-10: M1D1 decision-pack implementation is complete. The strict command separates offline selection from runtime mapping, records A=`APPROVE`, B/C=`REDEFINE`, and keeps M1D2/M2 blocked. Actual compute-path evidence shows that the recovery producer/path/usage semantics exist but the baseline compiler does not consume recovery permission and no PIT-lineage/persistence contract exists. `defensive_hold` is accepted by the compiler but has no callable producer, so Candidate B's redefinition cannot be implemented and meets its documented withdrawal condition pending owner confirmation. Volatility/TQQQ hard-veto components are callable; risk-off/event-risk/trend-break remain blocked. Requested/applied transition fields and a baseline-native scalar remain absent. Existing QQQ-equivalent logic is retained only as a governed cap formula, not a candidate-delta unit.

M1D2 remains `BLOCKED_OWNER_INPUT`. No adapter or candidate behavior was implemented, and no replay, backtest, scoring, runtime metric, paper-shadow, production, portfolio-weight mutation, or broker action was run.

Validation passed: 150 focused integration/documentation tests, 198 fast-tier tests (`fast-unit_20260710T131353Z`), and 197 contract-validation tests (`contract-validation_20260710T131501Z`), plus scoped Ruff, compileall, three strict real CLI runs, and `git diff --check`.
