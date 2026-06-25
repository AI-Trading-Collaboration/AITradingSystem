# TRADING-1065 to 1084 Equal-Risk Growth Tilt Exploration

最后更新：2026-06-26

## Status

- Current status: DONE
- Owner: project owner review
- Last updated: 2026-06-26

## Background

The previous `NO_GROWTH_EDGE_FOUND` conclusion only applies to the current
Controlled Growth V2 candidate batch. It does not prove that offensive strategy
research is permanently exhausted.

This batch pauses the failed growth search path and continues structured growth
exploration by tilting the existing `equal_risk_qqq_sgov` defensive core toward
balanced or growth-tilted roles. The original `equal_risk_qqq_sgov` strategy
definition must not be modified.

## Scope

Implement TRADING-1065 through TRADING-1084:

- growth research framing correction;
- equal-risk growth-tilt objective contract;
- candidate registry and registry review;
- cap/floor, risk-budget, trend boost, missed-upside, small TQQQ overlay, and
  volatility-target search artifacts;
- ranking, tiering, beta/risk-budget attribution, period/drawdown replay,
  cost/turnover sensitivity, and tradeoff frontier;
- definition lock/versioning, forward-aging readiness gate, owner decision
  pack, master review, roadmap update, and Reader Brief safety preview.

## Safety Boundary

All outputs must remain research-only:

- `paper_shadow_allowed=false`;
- `production_allowed=false`;
- `broker_action=none`;
- `manual_review_required=true`;
- no real trading advice;
- no LEAPS, Wheel, Options, tail-risk fallback, Layer-1 selector restart, or
  TQQQ-heavy mainline.

Candidate strategy IDs must be distinct from `equal_risk_qqq_sgov`.

## Acceptance Criteria

- Add `config/research/equal_risk_growth_tilt_candidate_registry.yaml`.
- Add all required `aits research strategies ...` CLI commands for TRADING-1065
  through TRADING-1084.
- Generate JSON/Markdown artifacts under:
  - `outputs/research_strategies/growth_components/`;
  - `outputs/research_strategies/roadmap/`;
  - `docs/research/growth_tilt_owner_decision_pack.md`;
  - `docs/research/growth_exploration_master_review.md`.
- Add all required report registry entries with
  `artifact_selection_policy=latest_available`, `required_for_daily_reading=false`,
  `production_effect=none`, and `broker_action=none`.
- Update `docs/artifact_catalog.md` and `docs/system_flow.md`.
- Add focused tests for the new module and CLI.
- Required validation must pass:
  - `python -m ruff check src tests`
  - `python -m compileall -q src tests`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_tilt.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_research_restart.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_layer2_strategy_component_readiness.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
  - `git diff --check`

## Stage Breakdown

1. Direction and candidate-pool setup: TRADING-1065 to TRADING-1067.
2. Search execution: TRADING-1068 to TRADING-1073.
3. Ranking and validation: TRADING-1074 to TRADING-1078.
4. Versioning and owner gate: TRADING-1079 to TRADING-1084.

## Progress Notes

- 2026-06-26: Created task and requirements document before implementation.
- 2026-06-26: Implemented the equal-risk growth tilt registry, 20 research
  strategy CLIs/artifacts, report registry entries, artifact catalog/system
  flow updates, owner/master docs copies, and focused tests. Owner-facing
  recommendations remain manual-review-only; no artifact enables paper-shadow,
  production, broker action, options, tail-risk fallback, Layer-1 selector
  restart, or TQQQ-heavy mainline.
- 2026-06-26: Validation passed: `python -m ruff check src tests`,
  `python -m compileall -q src tests`, focused growth tilt pytest, existing
  growth restart pytest, Layer-2 readiness pytest, task/report/documentation
  pytest, and `git diff --check`.
