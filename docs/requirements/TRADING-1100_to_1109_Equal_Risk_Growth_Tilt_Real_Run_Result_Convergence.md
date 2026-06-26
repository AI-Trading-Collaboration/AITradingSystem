# TRADING-1100 to 1109 Equal-Risk Growth Tilt Real-Run Result Convergence

最后更新：2026-06-26

## Status

- Current status: VALIDATING
- Owner: project owner review
- Last updated: 2026-06-26

## Numbering Note

The owner request labels this work as `TRADING-1085～1094`. In the current
repository, `TRADING-1085`, `TRADING-1086`, and `TRADING-1087` are already used
for completed or active governance/data tasks. This implementation preserves the
owner-requested scope and CLI names, but tracks the work under the next
non-conflicting range: `TRADING-1100～1109`.

## Background

`TRADING-1065～1084 Equal-Risk Growth Tilt Exploration` added the research-only
framework, registry, search CLIs, ranking/tiering, attribution, period replay,
cost sensitivity, frontier, definition lock, owner pack, master review and
Reader Brief preview.

That framework does not yet prove that an effective offensive variant exists.
This batch must run the implemented candidates and converge the real results
without adding more growth tilt families.

## Scope

Implement real-run convergence CLIs and artifacts for the existing
Equal-Risk Growth Tilt candidates:

- `growth-tilt-real-cli-suite`
- `growth-tilt-candidate-result-summary`
- `growth-tilt-tier-validation`
- `growth-tilt-beta-adjusted-edge-review`
- `growth-tilt-risk-return-frontier-review`
- `growth-tilt-period-drawdown-cost-triage`
- `growth-tilt-vs-equal-risk-and-qqq-final-gate`
- `growth-tilt-forward-aging-watchlist-review`
- `growth-tilt-owner-decision-pack-real-run`
- `growth-tilt-real-result-master-review`

## Required Questions

The final master review must answer:

- whether a useful Equal-Risk Growth Tilt candidate exists;
- the highest Tier 1 / Tier 2 / Tier 3 classification reached;
- whether the candidate clearly improves on `equal_risk_qqq_sgov`;
- whether it narrows the annual-return gap versus `100_qqq`;
- whether it approaches or exceeds `100_qqq`;
- whether the return lift is explained only by higher effective beta;
- whether max drawdown, Calmar, Sharpe, turnover and switch count are acceptable;
- whether results are concentrated in the 2024 AI rally;
- whether one candidate is research-only forward-aging watchlist reviewable;
- whether original `equal_risk_qqq_sgov` remains defensive primary;
- whether no paper-shadow, no production and no broker boundaries remain active.

## Safety Boundary

All outputs must remain research-only:

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`
- `broker_action = none`
- no real trading advice
- no paper-shadow activation
- no production activation
- no broker/order integration
- no LEAPS, Wheel, Options, tail-risk fallback, Layer-1 selector restart or
  TQQQ-heavy mainline
- no modification to the original `equal_risk_qqq_sgov`
- no new growth tilt candidate family

## Acceptance Criteria

- Add the 10 required `aits research strategies ...` CLI commands.
- Generate JSON/Markdown artifacts under
  `outputs/research_strategies/growth_components/`.
- Generate docs copies:
  - `docs/research/growth_tilt_owner_decision_pack_real_run.md`
  - `docs/research/growth_tilt_real_result_master_review.md`
- Add report registry entries for all 10 new report IDs with
  `artifact_selection_policy=latest_available`,
  `required_for_daily_reading=false`, `production_effect=none`, and
  `broker_action=none`.
- Update `docs/artifact_catalog.md` and `docs/system_flow.md`.
- Add focused tests covering builders, CLI registration, registry entries and
  research-only safety fields.
- Required validation must pass:
  - `python -m ruff check src tests`
  - `python -m compileall -q src tests`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_tilt.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_research_restart.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_layer2_strategy_component_readiness.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
  - `git diff --check`

## Stage Breakdown

1. Real-run suite and candidate result summary.
2. Tier validation, beta-adjusted edge review and risk-return frontier review.
3. Period/drawdown/cost triage, final gate and watchlist review.
4. Real-run owner decision pack and master review.

## Progress Notes

- 2026-06-26: Created task and requirements document. Implementation must reuse
  existing TRADING-1065～1084 candidates and preserve the research-only boundary.
- 2026-06-26: Implemented the 10 requested convergence CLIs, report registry
  entries, artifact catalog/system flow documentation, focused tests and docs
  copies. Ran the real master CLI with local cache `--as-of 2026-06-25`;
  `growth_tilt_real_result_master_review` status is
  `GROWTH_TILT_RESEARCH_ONLY`, best candidate is
  `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`, highest tier is
  `COMPONENT_READY_GROWTH`, beta-adjusted edge status is
  `BETA_ADJUSTED_EDGE_PRESENT`, triage status is `GROWTH_TILT_TRIAGE_PASS`,
  watchlist review is `GROWTH_TILT_KEEP_RESEARCH_ONLY`, and owner
  recommendation is `KEEP_GROWTH_TILT_RESEARCH_ONLY`.
- 2026-06-26: Validation passed: Ruff, compileall, focused growth tilt pytest,
  growth research restart pytest, Layer-2 readiness pytest, task/report/docs
  contract pytest, and `git diff --check`.
