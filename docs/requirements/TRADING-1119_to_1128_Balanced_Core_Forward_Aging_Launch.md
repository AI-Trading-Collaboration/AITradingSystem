# TRADING-1119 to 1128 Balanced Core Forward-Aging Launch

最后更新：2026-06-28

## Background

TRADING-1110 to 1118 completed the focused diagnosis of
`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`. The real-run master
review concluded that the candidate is not a growth-component-ready or
production candidate. Its allowed next role is a research-only balanced core
forward-aging candidate between the defensive primary `equal_risk_qqq_sgov`
and the hard benchmark `100_qqq`.

This batch safely launches the candidate into a research-only forward-aging
watchlist, writes the first observation, and creates a dual comparator panel.

## Safety Boundary

- No paper-shadow activation.
- No production activation.
- No broker connection or broker action.
- No real trading advice.
- Do not modify the original `equal_risk_qqq_sgov` definition.
- Do not revive Layer-1 selector, tail-risk fallback, TQQQ-heavy, LEAPS, Wheel,
  or Options paths.
- Observation target weights are research-only decision records, not live
  portfolio recommendations.

Every artifact must keep:

```text
paper_shadow_allowed = false
production_allowed = false
broker_action = none
manual_review_required = true
production_effect = none
```

## Stage Breakdown

### Stage 1: Activation Contract And Definition Lock

- TRADING-1119 adds
  `aits research strategies balanced-core-watchlist-activation-contract`.
- TRADING-1120 adds
  `aits research strategies balanced-core-definition-lock`.
- Acceptance:
  - Candidate remains `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`.
  - Upstream master status is `BALANCED_CORE_FORWARD_AGING_REVIEWABLE`.
  - Owner recommendation is `ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE`.
  - Definition hash is locked without overwriting `equal_risk_qqq_sgov`.

### Stage 2: Dry-Run, First Observation, Duplicate Guard

- TRADING-1121 adds
  `aits research strategies balanced-core-forward-aging-dry-run`.
- TRADING-1122 adds
  `aits research strategies balanced-core-first-observation-write`.
- TRADING-1123 adds
  `aits research strategies balanced-core-idempotency-duplicate-guard`.
- Acceptance:
  - Dry-run discloses candidate and comparator target weights, data quality
    status, and definition hash without writing an observation.
  - First observation is append-only under
    `outputs/research_strategies/growth_components/forward_aging_observations/`.
  - Re-running for the same strategy/date returns already-exists and preserves
    target weights, signal inputs, and definition hash.

### Stage 3: Maturity Gate And Dual Comparator Panel

- TRADING-1124 adds
  `aits research strategies balanced-core-maturity-scoreboard-safety-gate`.
- TRADING-1125 adds
  `aits research strategies dual-forward-aging-comparator-panel`.
- Acceptance:
  - Maturity windows are 5d, 10d, 20d, 60d, and 120d.
  - Sample floors come from the reviewed growth-tilt registry policy.
  - Scoreboard remains pending/insufficient while samples are immature.
  - Panel compares defensive primary, balanced core candidate, hard benchmark,
    and static references without producing trading recommendations.

### Stage 4: Safe Preview And Master Review

- TRADING-1126 adds
  `aits research strategies dual-forward-aging-reader-brief-safe-preview`.
- TRADING-1127 adds
  `aits research strategies balanced-core-owner-launch-pack`.
- TRADING-1128 adds
  `aits research strategies dual-forward-aging-master-review`.
- Acceptance:
  - Reader Brief preview is not wired into formal daily Reader Brief.
  - Owner launch pack confirms definition lock, first observation, duplicate
    guard, scoreboard safety, comparator panel, and safety flags.
  - Master review final status is limited to research-only states.

## Artifacts

- `outputs/research_strategies/growth_components/balanced_core_watchlist_activation_contract.json/md`
- `outputs/research_strategies/growth_components/balanced_core_definition_lock.json/md`
- `outputs/research_strategies/growth_components/balanced_core_forward_aging_dry_run.json/md`
- `outputs/research_strategies/growth_components/forward_aging_observations/balanced_core_forward_aging_observation_<date>.json/md`
- `outputs/research_strategies/growth_components/balanced_core_idempotency_duplicate_guard.json/md`
- `outputs/research_strategies/growth_components/balanced_core_maturity_scoreboard_safety_gate.json/md`
- `outputs/research_strategies/roadmap/dual_forward_aging_comparator_panel.json/md`
- `outputs/research_strategies/roadmap/dual_forward_aging_reader_brief_safe_preview.json/md`
- `outputs/research_strategies/roadmap/balanced_core_owner_launch_pack.json`
- `docs/research/balanced_core_owner_launch_pack.md`
- `outputs/research_strategies/roadmap/dual_forward_aging_master_review.json`
- `docs/research/dual_forward_aging_master_review.md`

## Progress Notes

- 2026-06-26: Created as IN_PROGRESS. Implementation must update report
  registry, artifact catalog, system flow, tests, and task register in the same
  change. Required validation is the task package validation listed by the
  project owner.
- 2026-06-26: Implementation completed and moved to VALIDATING. Added the 10
  requested `aits research strategies ...` commands, policy-backed balanced
  core maturity floors, append-only observation writing, duplicate guard,
  dual comparator panel, Reader Brief safe preview, owner launch pack and
  master review. Validation passed Ruff, compileall, focused growth tilt,
  growth restart, Layer-2 readiness, task/register/report/docs contract tests,
  and `git diff --check`.
