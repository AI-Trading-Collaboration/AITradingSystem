# TRADING-2401: Dynamic Strategy Recombination Line Plateau And Data Signal Quality Decision

最后更新：2026-07-07

## Context

TRADING-2400 recorded the targeted gate evidence owner review decision. The
system did not approve observation, retained the research value of
`growth_tilt_guarded_transfer_valid_until_strict_v1`, and required a plateau /
data-signal quality decision before generating more variants.

TRADING-2401 decides whether the current recombination line has reached a local
plateau and whether the next phase should move to data quality, PIT coverage,
signal quality, regime labeling and threshold meta-dataset review.

## Scope

This task will:

1. Read prior TRADING-2400 owner decision artifacts.
2. Read prior TRADING-2399 targeted gate evidence retest artifacts.
3. Read prior TRADING-2398 gate evidence plan artifacts.
4. Read prior TRADING-2397 owner decision artifacts.
5. Read prior TRADING-2396 recombination retest artifacts.
6. Summarize recombination line history from TRADING-2396 through TRADING-2400.
7. Evaluate plateau criteria.
8. Record owner decision:
   `PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`.
9. Generate next research direction decision and TRADING-2402 route.

## Non-Goals

- Do not run a new backtest.
- Do not run `aits validate-data --as-of 2026-07-05` unless the implementation
  starts reading fresh cached market data.
- Do not generate a new signal or scoring output.
- Do not approve research-only observation.
- Do not enable paper-shadow, scheduler, event append, outcome binding,
  production, broker/order, scheduled task or daily report paths.

## Expected Decision

```text
PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY
```

Required conclusion:

- recombination line plateau detected: true
- continue local targeted improvement recommended: false
- data / PIT / signal quality review recommended: true
- regime labeling review recommended: true
- threshold meta-dataset recommended: true
- research-only observation approved: false
- recommended next route:
  `TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review`

## Safety Boundary

- `production_effect=none`
- `broker_action=none`
- `candidate_auto_accept_approved=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_recombination_line_plateau_decision/plateau_decision_result.json`
- `outputs/research_strategies/dynamic_strategy_recombination_line_plateau_decision/recombination_plateau_review.json`
- `outputs/research_strategies/dynamic_strategy_recombination_line_plateau_decision/next_research_direction_decision.json`
- `outputs/research_strategies/dynamic_strategy_recombination_line_plateau_decision/data_signal_quality_review_route.json`
- `docs/research/dynamic_strategy_recombination_line_plateau_decision.md`
- `docs/research/dynamic_strategy_recombination_plateau_review.md`
- `docs/research/dynamic_strategy_data_signal_quality_next_direction.md`
- `docs/research/dynamic_strategy_2402_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-recombination-line-plateau-decision`
  returns
  `DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_DECISION_READY`.
- Output records owner decision
  `PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`.
- Output identifies base candidate as
  `growth_tilt_lower_turnover_guarded_transfer_v1`.
- Output identifies best targeted variant as
  `growth_tilt_guarded_transfer_valid_until_strict_v1`.
- Output records decision from 2399 as `CONTINUE_TARGETED_IMPROVEMENT`.
- Output records observation preview candidate count as 0.
- Output sets recombination line plateau detected to true.
- Output sets continue local targeted improvement recommended to false.
- Output recommends data-signal quality, PIT coverage, regime labeling and
  threshold meta-dataset review.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, docs freshness, task-register consistency,
  report contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to prior validated artifacts from TRADING-2396 through
  TRADING-2400 and must not run a new backtest or approve observation.
- 2026-07-07: Implemented
  `aits research strategies dynamic-strategy-recombination-line-plateau-decision`,
  report builder, focused tests, report registry, artifact catalog, system flow,
  research docs and completed archive. Real CLI run returned
  `DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_DECISION_READY`
  with owner decision
  `PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`,
  plateau detected=true, continue local targeted improvement=false, data/PIT/signal
  quality review=true, regime labeling review=true, threshold meta-dataset
  review=true and next route
  `TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review`.
- 2026-07-07: Focused validation passed: Ruff, py_compile, focused parallel
  pytest 3 passed and real CLI run.
- 2026-07-07: Full closeout validation passed: full Ruff, compileall, docs
  freshness 588 docs PASS, documentation contract 1298 reports PASS,
  task-register consistency run active=319 / completed=462 / failed=0,
  task-register consistency validate checks=5 / failed=0 / warnings=0,
  terminal-status guard no matches, and contract-validation 197 passed with
  runtime artifact
  `outputs/validation_runtime/contract-validation_20260707T060528Z/test_runtime_summary.json`.
