# TRADING-2438M1D1A Owner Decision Resolution and Candidate-A Reframing Plan
最后更新：2026-07-10

**Date:** 2026-07-10
**Base commit:** `a576ab87f03dc4fba6f5570c83bdca7451f04078`
**Current state (2026-07-10 implementation update):** M1D1A owner outcomes、M1D2 adapters 和 M1E evidence gate 均已完成；`APPROVE/REDEFINE/WITHDRAW=0/2/1`；replacement A=`KEEP_REDEFINED_BLOCKED`；`M2 eligible=0`；no real PIT replay executed.

---

# 1. New conclusion from M1D1

M1D1 establishes a stronger result than “baseline contracts are incomplete”:

```text
re_risk_allowed_probability has a callable producer
but the baseline does not consume it as a governed recovery transition input
and no baseline recovery persistence/reset/timing behavior exists
```

Therefore, the current Candidate A definition cannot be implemented honestly as:

```text
candidate persistence = baseline persistence - 1
```

Doing so would first invent a new baseline recovery mechanism and then claim the candidate is a speedup of that invented mechanism. The resulting replay would mix:

```text
new baseline strategy behavior
+
candidate timing variation
```

and could not isolate the candidate hypothesis.

M1D2 must not implement a baseline recovery-persistence contract solely to rescue Candidate A.

---

# 2. Recommended candidate decisions

| Candidate | Recommended decision | Reason |
|---|---|---|
| `recovery_reentry_speedup_guard` | `REDEFINE_PENDING_PIT_LINEAGE` | No governed baseline consumption exists, so “speedup” has no reference rule |
| `false_risk_off_confirmation_relaxation` | `WITHDRAW` | No callable soft confirmation and no callable aggregate non-hard defensive request |
| `missed_upside_reentry_accelerator` | Keep `REDEFINE` and remove from current route | Still overlaps the recovery family and has no independent contract |

After these decisions, an honest M1 state is:

```text
approved=0
redefine=2
withdraw=1
M2 eligible=0
```

This is not research failure. It prevents the system from converting names into ungoverned investment logic.

---

# 3. Candidate A replacement hypothesis

If the owner wants to preserve the recovery research direction, replace Candidate A with a new hypothesis:

```text
capped_recovery_permission_overlay
```

Suggested role:

```yaml
candidate_role: CAPPED_RECOVERY_PERMISSION_OVERLAY
changes_baseline_recovery_persistence: false
introduces_candidate_overlay: true
changes_post_confirmation_ramp: false
changes_hard_veto_behavior: false
```

Research question:

> When the existing callable recovery-permission evidence becomes valid and all governed hard vetoes are inactive, does a small, short-lived provisional recovery overlay reduce missed upside without damaging drawdown, turnover, or whipsaw?

This is a new candidate overlay compared directly with the unchanged baseline. It is not described as a “speedup” of a nonexistent baseline persistence rule.

The new candidate may return to `APPROVE` only after PIT lineage, signal semantics, transition timing, hard-veto completeness, and native exposure binding are approved.

---

# 4. The 18 owner decisions

The following decision pack should be completed before any M1D2 compute work.

## Recovery family

### D01 — Candidate A disposition

**Recommendation:**

```yaml
candidate_id: recovery_reentry_speedup_guard
decision: REDEFINE
replacement_candidate_id: capped_recovery_permission_overlay
reason: BASELINE_DOES_NOT_CONSUME_RECOVERY_PERMISSION
```

Alternative allowed only with proof:

```text
KEEP_APPROVE only if an existing governed baseline consumption path is located.
A newly implemented path does not count as existing proof.
```

### D02 — Recovery producer PIT lineage

**Recommendation:**

```yaml
required: true
producer: re_risk_allowed_probability
approval_condition:
  - source inputs have as-of timestamps
  - producer version is recorded
  - output is reproducible from PIT-valid inputs
  - no future outcome is consumed
failure_action: KEEP_REDEFINED_OR_WITHDRAW
```

Callable code alone is insufficient.

### D03 — Recovery output semantics

Owner must classify the output as exactly one of:

```text
CALIBRATED_PROBABILITY
UNSCALED_SCORE
DIAGNOSTIC_SCORE
BOOLEAN_PERMISSION
```

**Recommendation:** do not approve a threshold until this classification and calibration evidence are known.

### D04 — Recovery threshold

**Recommendation:**

```yaml
threshold_source: EXISTING_VERSIONED_PRODUCER_CONTRACT_OR_PRE_REGISTERED_CALIBRATION
default_0_5_allowed: false
post_replay_tuning_allowed: false
```

If no governed threshold exists, the replacement candidate remains blocked.

### D05 — Recovery observation persistence

For the replacement overlay, do not create a baseline persistence rule.

**Recommended first screening contract:**

```yaml
candidate_required_consecutive_steps: 1
baseline_required_consecutive_steps: null
interpretation: CANDIDATE_TRIGGER_RULE_NOT_BASELINE_SPEEDUP
```

Any later comparison of 1-step versus 2-step confirmation should be a separate candidate-family experiment.

### D06 — Reset and invalidation

**Recommendation:**

```yaml
reset_on_signal_false: true
reset_on_missing: BLOCKED
reset_on_any_hard_veto: true
reset_on_invalid_pit_lineage: BLOCKED
```

### D07 — Candidate effective timing

**Recommendation:**

```yaml
decision_time: evaluation_step_t
effective_time: next_executable_evaluation_step
same_step_application_allowed: false
```

This should be aligned with the repository’s normal execution convention. If that convention is unresolved, next-step application is the fail-closed choice.

### D08 — Overlay expiry

**Recommendation:**

```yaml
maximum_active_steps: 1
expire_when_signal_false: true
expire_when_hard_veto_active: true
expire_when_baseline_reaches_or_exceeds_overlay_target: true
auto_extension_allowed: false
```

A one-step overlay provides the cleanest first test of timing benefit.

## Candidate B and C

### D09 — Candidate B final disposition

**Recommendation:**

```yaml
candidate_id: false_risk_off_confirmation_relaxation
decision: WITHDRAW
reason:
  - NO_CALLABLE_PIT_SOFT_CONFIRMATION
  - NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST
```

Do not create either interface solely to preserve B.

Reopen condition:

```text
A future baseline version independently introduces a governed, callable,
PIT-valid non-hard defensive request or soft confirmation.
```

### D10 — Candidate C disposition

**Recommendation:**

```yaml
candidate_id: missed_upside_reentry_accelerator
decision: REDEFINE
current_route_enabled: false
```

Move it to a later `post-confirmation ramp mechanics` family only after a governed recovery transition exists.

## Hard vetoes

### D11 — `risk_off_veto`

**Recommendation:**

```yaml
required_for_recovery_overlay: true
resolution_required: RESOLVED_CALLABLE
missing_policy: BLOCKED
```

No recovery overlay can run without the baseline’s authoritative risk-off safety path.

### D12 — `trend_break_veto`

Owner must determine whether this is actual baseline control logic or only a conceptual label.

**Recommendation:**

```yaml
allowed_resolution:
  - RESOLVED_CALLABLE
  - EXPLICITLY_NOT_APPLICABLE_TO_BASELINE
```

`EXPLICITLY_NOT_APPLICABLE` requires baseline trace evidence. It cannot be chosen merely because no producer was found.

### D13 — `event_risk_veto`

**Recommendation:**

```yaml
do_not_create_for_this_candidate: true
```

Choose one:

```text
RESOLVED_CALLABLE
EXPLICITLY_NOT_APPLICABLE_TO_BASELINE
BLOCKED_NO_PIT_CONTRACT
```

If the baseline truly uses it as a hard veto and it lacks PIT lineage, the recovery candidate remains blocked. Do not remove event windows after seeing replay results.

### D14 — Hard-veto aggregate missing policy

**Recommendation:**

```yaml
unresolved_component: BLOCKED
missing_runtime_value: BLOCKED
candidate_specific_removal_allowed: false
candidate_specific_priority_change_allowed: false
```

A missing veto is never interpreted as inactive.

## Regime transition

### D15 — Canonical transition source

**Recommendation:**

Use an existing baseline state/target field only.

Required split:

```yaml
current_state_field:
requested_target_state_field:
applied_target_state_field:
```

If the repository has no governed requested/applied distinction, implement that baseline trace contract before the candidate overlay.

### D16 — Transition timing

**Recommendation:**

```yaml
candidate_request_created_at: evaluation_step_t
candidate_request_applied_at: next_executable_evaluation_step
```

No same-bar retrospective application.

### D17 — Transition priority

**Recommendation:**

```text
1. invalid PIT/data contract -> BLOCKED
2. hard veto / emergency risk request
3. baseline mandatory defensive transition
4. baseline ordinary transition request
5. approved recovery overlay request
6. exposure/risk-cap clamp
```

The candidate cannot supersede an explicit baseline defensive request.

## Exposure binding

### D18 — Native scalar and candidate cap

**Recommendation:**

Use the baseline-native scalar, not QQQ-equivalent exposure.

Required fields:

```yaml
native_scalar_id:
current_scalar_field:
requested_target_scalar_field:
applied_target_scalar_field:
minimum_value:
maximum_value:
minimum_increment:
```

Recommended first overlay cap:

```yaml
candidate_delta_cap:
  formula: MIN(
    0.25 * positive_remaining_gap_to_baseline_neutral_target,
    one_native_transition_increment
  )
  qqq_equivalent_unit_allowed: false
  tqqq_increase_allowed: false
```

If no single governed native scalar exists, the replacement candidate remains blocked.

---

# 5. What M1D2 should implement

M1D2 should implement only baseline contracts that are independently required to describe existing behavior.

## Implement

```text
hard-veto aggregate adapter
current/requested/applied regime transition trace
baseline-native exposure scalar trace
PIT lineage report for re_risk_allowed_probability
```

## Implement conditionally

```text
recovery-permission semantic adapter:
  only if it exposes the existing producer without changing baseline decisions
```

This adapter may materialize:

```yaml
signal_id:
raw_runtime_value:
semantic_type:
threshold_contract_ref:
eligible:
pit_lineage:
```

but it must not cause a baseline transition.

## Do not implement

```text
new baseline recovery persistence
new baseline recovery transition
new soft confirmation
new aggregate non-hard defensive request
new event-risk veto
new QQQ-equivalent candidate delta conversion
```

Those would introduce strategy behavior rather than expose existing behavior.

---

# 6. Revised task sequence

## TRADING-2438M1D1A — Owner Decision Resolution

Output:

```text
all 18 decisions completed
A redefined or withdrawn
B withdrawn
C remains out of route
hard-veto classifications signed
transition timing/priority signed
native scalar choice signed
```

No runtime code and no replay.

## TRADING-2438M1D2 — Existing Baseline Contract Adapters

Implement:

```text
hard-veto aggregate
transition trace
native exposure scalar
recovery producer PIT-lineage and semantic adapter
```

No new strategy behavior.

## TRADING-2438M1E — Replacement Candidate Contract

Only if M1D2 proves all prerequisites:

```text
approve capped_recovery_permission_overlay
approve signal threshold
approve one-step next-period overlay
approve native-scalar cap
freeze metric policy owner/version/timestamp/commit/hash
```

## TRADING-2438M2 — Real PIT Replay

Run only the replacement A candidate if it becomes eligible.

A valid first wave may contain exactly one candidate.

---

# 7. Readiness conditions for the replacement A

```text
recovery producer PIT lineage valid
output semantic type known
threshold versioned and pre-registered
risk_off_veto resolved
all other actual baseline hard vetoes resolved or explicitly not applicable
transition requested/applied trace ready
native exposure scalar ready
candidate cap expressed in native units
next-step timing registered
screening policy frozen before result visibility
```

Expected readiness:

```text
approved candidate count = 1
M2 eligible candidate count = 1
```

There is no requirement to restore a three-candidate set.

---

# 8. Focused tests

1. Callable recovery producer without PIT lineage remains blocked.
2. `do_not_de_risk_pass=false` is not a mapping error.
3. Offline selection output cannot become a runtime prerequisite.
4. No baseline recovery persistence is created.
5. Replacement A does not claim to speed up baseline persistence.
6. Threshold cannot default to `0.5`.
7. Missing threshold calibration blocks approval.
8. Candidate request applies on the next executable step.
9. Candidate overlay lasts at most one step.
10. Signal false resets the overlay.
11. Missing signal blocks rather than returning false.
12. Any hard veto cancels the overlay.
13. Unresolved hard veto blocks M2.
14. Candidate cannot supersede a baseline defensive request.
15. Current/requested/applied states remain distinct.
16. Candidate cap uses the native scalar.
17. QQQ-equivalent cap formula cannot be used as candidate delta unit.
18. TQQQ exposure cannot increase.
19. B is excluded from executor binding.
20. C is excluded from the first wave.
21. No new soft confirmation is created.
22. No aggregate non-hard request is synthesized.
23. No event-risk veto is invented.
24. No real PIT replay runs in M1D2.
25. Policy approval metadata remains empty until M1E.
26. Report registry, catalog, system flow, and task archive remain consistent.

---

# 9. Definition of done

## M1D1A

```text
18/18 owner decisions complete
A current contract no longer APPROVE as a baseline speedup
B WITHDRAW
C REDEFINE and out of route
hard-veto resolution policy complete
transition timing/priority complete
native scalar decision complete
M2 eligible remains 0
```

## M1D2

```text
existing baseline behavior exposed through governed adapters
no new baseline decision behavior introduced
recovery producer PIT lineage result materialized
hard-veto aggregate complete or exact blockers emitted
transition trace materialized
native exposure scalar materialized
no candidate replay invoked
```

## M1E

```text
replacement A approved only if all prerequisites are proven
screening policy frozen
M2 eligible becomes 1, or remains 0 with exact blocker
```

---

# 10. Final recommendation

The research direction should be narrowed from:

```text
three top candidates
```

to:

```text
one possible recovery overlay hypothesis
```

The system has now provided useful negative evidence:

```text
B does not match the current baseline architecture
C is not yet orthogonal or executable
A is not a speedup because the baseline has no governed recovery-consumption rule
```

The correct next move is not to add enough baseline behavior to make these names executable. It is to expose the baseline’s real safety, transition, exposure, and PIT-lineage contracts, then approve only the replacement hypothesis that can be expressed without inventing a reference strategy.

---

# 11. Implementation progress — 2026-07-10

M1D1A 已实现 owner-resolution schema、D01～D18 evidence validation、四项 machine-readable artifacts、CLI、中文报告和权威 M1 disposition 更新。真实 strict run 为 `GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS`，`approved/redefine/withdraw=0/2/1`、owner gaps=0、M2 eligible=0；7 项 blocker outcome 为 D02/D04/D11/D12/D13/D15/D18。focused parallel pytest 为 63 passed。

M1D2 已实现以下四个 adapter slice：

1. hard-veto aggregate：保留五项 baseline component identity；任何 unresolved/missing/PIT-invalid component 都输出 `BLOCKED`，不得解释为 false。
2. regime transition trace：只物化已有 current state及缺失的 requested/applied fields，不用相邻行或候选意图推断 baseline request。
3. native exposure scalar：只接受显式受治理 binding；当前 absence 输出 exact blocker，禁止 QQQ-equivalent 或 TQQQ substitution。
4. recovery permission：记录 producer、`UNSCALED_SCORE` semantic、PIT lineage fields及 threshold status；缺失 lineage/threshold 时不得产生 trigger。

M1D2 不实现 baseline recovery persistence、candidate overlay、event-risk producer、soft confirmation、aggregate non-hard request、replay或 runtime screening metrics。

M1E 随后只实现 evidence gate：当任一 prerequisite 未证明或 second-owner/policy approval provenance 缺失时，replacement A 必须保持 `KEEP_REDEFINED_BLOCKED`；不得写出 APPROVE runtime spec或 M2 binding。

真实 M1D2 strict run 为 `GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_READY_WITH_BLOCKERS`，adapter implemented/ready/blocked=`4/0/4`；真实 M1E strict run 为 `GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT_READY_BLOCKED`，prerequisites=`2 PASS/8 BLOCKED`、disposition=`KEEP_REDEFINED_BLOCKED`、approved candidate=0、M2 eligible=0。D1A～M1E combined focused parallel pytest为144 passed；未运行 replay/runtime metrics或改变 baseline/candidate/production状态。
