# TRADING-1110 to 1118 Growth Tilt Focused Diagnosis And Watchlist Reconsideration

最后更新：2026-06-26

## Status

- Current status: VALIDATING
- Owner: project owner review
- Last updated: 2026-06-26

## Background

TRADING-1100～1109 real-run convergence found a focused research candidate:

```text
equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1
```

The real-run master conclusion remains research-only:

```text
final_status = GROWTH_TILT_RESEARCH_ONLY
highest_raw_tier = COMPONENT_READY_GROWTH
watchlist_allowed = false
beta_adjusted_edge = exists_but_not_material
paper_shadow_allowed = false
production_allowed = false
broker_action = none
manual_review_required = true
```

This batch does not expand the strategy family and does not approve a watchlist
entry. It diagnoses why the current vol-target candidate worked, whether the
return lift is mostly beta, whether it is more appropriate as balanced core,
and whether a nearby parameter variant is more stable.

## Scope

Implement the following research-only CLIs and artifacts:

- `best-growth-tilt-candidate-deep-dive`
- `vol-target-growth-tilt-local-sensitivity`
- `beta-adjusted-edge-methodology-audit`
- `growth-tilt-balanced-core-role-review`
- `growth-tilt-vs-equal-risk-missed-upside-review`
- `growth-tilt-parameter-neighbor-finalist-review`
- `growth-tilt-watchlist-reconsideration-gate`
- `growth-tilt-owner-diagnosis-pack`
- `growth-tilt-focused-diagnosis-master-review`

## Non-Goals

- Do not add a new growth tilt family.
- Do not modify `equal_risk_qqq_sgov`.
- Do not automatically add any candidate to forward-aging observation.
- Do not allow paper-shadow, production, broker/order integration, options,
  LEAPS, Wheel, tail-risk fallback, TQQQ-heavy mainline or Layer-1 selector
  restart.
- Do not output real trading advice.

## Acceptance Criteria

- Add all 9 `aits research strategies ...` CLI commands.
- Generate JSON/Markdown artifacts under
  `outputs/research_strategies/growth_components/`.
- Generate docs copies:
  - `docs/research/growth_tilt_owner_diagnosis_pack.md`
  - `docs/research/growth_tilt_focused_diagnosis_master_review.md`
- Add report registry entries with `artifact_selection_policy=latest_available`,
  `required_for_daily_reading=false`, `production_effect=none`, and
  `broker_action=none`.
- Update `docs/artifact_catalog.md` and `docs/system_flow.md`.
- Add focused tests for builders, CLI registration, registry entries and
  research-only safety fields.
- Required validation:
  - `python -m ruff check src tests`
  - `python -m compileall -q src tests`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_tilt.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_research_restart.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_layer2_strategy_component_readiness.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
  - `git diff --check`

## Stage Breakdown

1. Candidate deep dive and vol-target local sensitivity.
2. Beta-adjusted methodology audit, balanced-core role review and missed-upside
   review.
3. Neighbor finalist review and watchlist reconsideration gate.
4. Owner diagnosis pack and focused diagnosis master review.

## Progress Notes

- 2026-06-26: Created task and requirements document before implementation.
  Scope is limited to focused diagnosis of the existing vol-target Growth Tilt
  candidate and preserves all research-only safety boundaries.
- 2026-06-26: Implemented the 9 requested focused diagnosis CLIs, report
  registry entries, artifact catalog/system flow documentation, owner/master
  docs copies and focused tests. Ran
  `growth-tilt-focused-diagnosis-master-review --as-of 2026-06-25` with local
  cache; status is `BALANCED_CORE_FORWARD_AGING_REVIEWABLE`, candidate is
  `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`, owner recommendation
  is `ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE`, and all safety fields
  remain `paper_shadow_allowed=false`, `production_allowed=false`,
  `broker_action=none`, `manual_review_required=true`.
- 2026-06-26: Validation passed: Ruff, compileall, focused Growth Tilt pytest,
  Growth Restart pytest, Layer-2 readiness pytest, task/report/docs contract
  pytest, and `git diff --check`.
