# TRADING-2403: Dynamic Strategy PIT Coverage Matrix And Signal Construction Review

最后更新：2026-07-07

## Context

TRADING-2402 已完成 dynamic strategy data / PIT / signal quality gap review，
真实状态为 `DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`。
2402 结论是当前不恢复 candidate search；recombination line 保持 paused；下一步应把
dynamic strategy 使用的数据、特征、信号、valid-until、regime labels 和 gate inputs
结构化为 PIT coverage matrix，并审查 signal construction 是否足以支撑后续研究。

## Scope

This task will:

1. Read TRADING-2402 gap review artifacts.
2. Read TRADING-2401 plateau decision artifacts.
3. Read TRADING-2399 targeted gate evidence retest artifacts.
4. Read TRADING-2386 expanded candidate pool retest artifacts.
5. Read TRADING-2364 execution cadence bias audit artifacts.
6. Run or consume `aits validate-data --as-of 2026-07-05`.
7. Build a PIT coverage matrix for market data, technical features, strategy
   signals, execution semantics, regime labels and gate inputs.
8. Build signal construction review for growth tilt, valid-until / stale signal,
   turnover guardrails and signal-to-execution lag.
9. Build regime labeling review and regime expectation mapping proposal.
10. Build threshold meta-dataset gap definition.
11. Produce prioritized remediation matrix and TRADING-2404 route.

## Non-Goals

- Do not run a new strategy backtest.
- Do not generate a new trading signal or scoring output.
- Do not resume strategy candidate search.
- Do not approve research-only observation for any candidate.
- Do not enable scheduler, event append, outcome binding, paper-shadow,
  paper trade, shadow position, production, broker/order, scheduled task or
  daily report paths.

## Data Quality Gate

Because 2403 directly reviews data / PIT / signal quality, the default closeout
will run:

```text
aits validate-data --as-of 2026-07-05
```

Outputs must expose validate-data status, error count, warning count and warning
classification. If implementation consumes prior validated summaries instead of
rerunning the gate, completed notes must explain why.

## Expected Decision

Default next-direction decision:

- `OPTION_A_BUILD_PIT_COVERAGE_MATRIX_IMPLEMENTATION`
- `OPTION_B_REVIEW_AND_REFACTOR_SIGNAL_CONSTRUCTION`
- `OPTION_C_BUILD_REGIME_EXPECTATION_SCORING`
- `OPTION_D_BUILD_THRESHOLD_META_DATASET`

Default route:

```text
TRADING-2404_Dynamic_Strategy_PIT_Coverage_Matrix_Implementation_Plan
```

Expected safety result:

- `candidate_search_resumed=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/pit_signal_review_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/pit_coverage_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/signal_construction_review.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/regime_labeling_review.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/remediation_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/threshold_meta_dataset_gap.json`
- `docs/research/dynamic_strategy_pit_coverage_signal_construction_review.md`
- `docs/research/dynamic_strategy_pit_coverage_matrix.md`
- `docs/research/dynamic_strategy_signal_construction_review.md`
- `docs/research/dynamic_strategy_regime_labeling_review.md`
- `docs/research/dynamic_strategy_2404_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-pit-coverage-signal-construction-review`
  returns `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`.
- Output reads and validates TRADING-2402 route to TRADING-2403.
- Output records validate-data status, error count and warning count.
- Output sets PIT coverage matrix, signal construction review, valid-until /
  stale signal review, regime labeling review, threshold meta-dataset gap and
  prioritized remediation matrix readiness fields to true.
- Output sets candidate_search_resumed=false.
- Output recommends TRADING-2404 PIT coverage matrix implementation plan.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, validate-data, docs freshness, task-register
  consistency, report contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to PIT coverage matrix and signal construction review
  only. It must not run a new strategy backtest, generate new trading signals,
  resume candidate search, approve observation or touch execution paths.
- 2026-07-07: Ran `aits validate-data --as-of 2026-07-05`; result
  `PASS_WITH_WARNINGS`, errors=0, warnings=2, report
  `outputs/reports/data_quality_2026-07-05.md`, audit artifact
  `artifacts/data_refresh_audit/validation/validate_data_2026-07-05_becbcfd2e105dee4.json`.
  The two warnings remain `prices_download_manifest_checksum_missing` and
  `prices_adjustment_ratio_jump`.
- 2026-07-07: Implemented
  `aits research strategies dynamic-strategy-pit-coverage-signal-construction-review`,
  builder module, PIT coverage matrix, signal construction review, valid-until /
  stale-signal review, regime labeling review, threshold meta-dataset gap,
  remediation matrix, TRADING-2404 route, research docs, registry, catalog,
  system flow and focused tests. Real CLI run returned
  `DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`,
  with 14 PIT matrix rows, 8 remediation rows, `growth_tilt_engine` and
  `valid_until_window` marked as blocking gaps, candidate_search_resumed=false,
  next route `TRADING-2404_Dynamic_Strategy_PIT_Coverage_Matrix_Implementation_Plan`,
  and all safety fields false/none.
- 2026-07-07: Focused validation passed:
  `python -m ruff check src\ai_trading_system\dynamic_strategy_pit_coverage_signal_construction_review.py tests\research_strategies\test_dynamic_strategy_pit_coverage_signal_construction_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`,
  `python -m py_compile ...`, and
  `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_pit_coverage_signal_construction_review.py`
  with 3 passed. Final closeout validation is recorded in the completed archive.
- 2026-07-07: Full closeout validation passed: Ruff, compileall, docs freshness
  590 docs / 0 issues, documentation contract 1300 reports / 0 errors / 0
  warnings, task-register consistency active=319 / completed=464 / failed=0,
  task-register consistency validate checks=5 / failed=0 / warnings=0,
  no terminal DONE row left in current task register, `git diff --check`, and
  contract-validation 197 passed with runtime artifact
  `outputs/validation_runtime/contract-validation_20260707T070000Z/test_runtime_summary.json`.
