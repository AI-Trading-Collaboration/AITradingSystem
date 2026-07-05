# TRADING-2365 Dynamic Strategy Event-Driven Retest And Candidate Ranking

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING`
- priority: `P0`
- status: `DONE`
- owner: project owner follow-up review
- source route: `TRADING-2365_Dynamic_Strategy_Event_Driven_Retest_And_Candidate_Ranking`
- source task: `TRADING-2364_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_AND_RETEST`
- target status: `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`
- target next route: `TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis`

## Scope

TRADING-2364 confirmed that monthly or fixed execution cadence can distort dynamic strategy conclusions. This task reruns dynamic strategy candidates under `valid_until_window` as the primary execution cadence and compares monthly, signal-event-driven, and cooldown-limited event-driven actual paths. The output is a cost-aware candidate ranking and research-only decision summary.

## Implementation Plan

- Add `src/ai_trading_system/dynamic_strategy_event_driven_retest.py`.
- Add CLI `aits research strategies dynamic-strategy-event-driven-retest`.
- Read and verify the TRADING-2364 cadence-bias audit artifact.
- Discover dynamic strategy candidates from `config/research/strategy_execution_policy_registry.yaml`, with existing execution-semantics strategies as coverage floor.
- Run the same cached-data validation path used by execution-semantics research before loading market data.
- Write outputs under `outputs/research_strategies/dynamic_strategy_event_driven_retest/`.
- Write research docs:
  - `docs/research/dynamic_strategy_event_driven_retest.md`
  - `docs/research/dynamic_strategy_candidate_ranking.md`
  - `docs/research/dynamic_strategy_retest_decision_summary.md`
  - `docs/research/dynamic_strategy_2366_route.md`
- Update report registry, artifact catalog, system flow, task register, and completed task archive.
- Add focused tests in `tests/research_strategies/test_dynamic_strategy_event_driven_retest.py`.

## Acceptance Criteria

- CLI runs and writes JSON/Markdown artifacts.
- `status=DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`.
- Source TRADING-2364 artifact is loaded and confirms cadence bias, old dynamic results needing retest, and `valid_until_window` as recommended cadence.
- Primary execution cadence is `valid_until_window`.
- Cadences include static, monthly, signal-event-driven, valid-until, and cooldown-limited event-driven.
- Candidate ranking exists and includes cost-adjusted return, turnover, turnover-adjusted score, dynamic-vs-static gap, valid-until-vs-monthly gap, stale/missed/cooldown/constraint counts, decision, and reason.
- Monthly results are kept as deprecated reference rows and must not be the default ranking cadence.
- Conclusions answer the top candidate, whether it survives cost adjustment, whether it relies on high turnover, which old results are deprecated, whether cost/cooldown sensitivity is required, and whether `valid_until_window` should become the default dynamic backtest cadence.
- Scheduler, event append, outcome binding, paper-shadow, production, daily report, and broker fields remain disabled / false / none.

## Implementation Result

- Added `src/ai_trading_system/dynamic_strategy_event_driven_retest.py`.
- Added CLI `aits research strategies dynamic-strategy-event-driven-retest`.
- Added report registry, artifact catalog, system-flow, task-register, completed-task, and focused test coverage.
- Generated outputs:
  - `outputs/research_strategies/dynamic_strategy_event_driven_retest/event_driven_retest_result.json`
  - `outputs/research_strategies/dynamic_strategy_event_driven_retest/candidate_ranking.json`
  - `outputs/research_strategies/dynamic_strategy_event_driven_retest/cadence_comparison_matrix.json`
  - `docs/research/dynamic_strategy_event_driven_retest.md`
  - `docs/research/dynamic_strategy_candidate_ranking.md`
  - `docs/research/dynamic_strategy_retest_decision_summary.md`
  - `docs/research/dynamic_strategy_2366_route.md`

## Real Run Result

- status: `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`
- source TRADING-2364 confirmed: `true`
- source recommended cadence: `valid_until_window`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_error_count: `0`
- candidate_count: `6`
- scenario_count: `25`
- primary_execution_cadence: `valid_until_window`
- top_candidate: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- top_decision: `OWNER_REVIEW_REQUIRED`
- top_turnover_adjusted_score: `0.214462`
- any_candidate_survives_cost_adjustment: `true`
- any_candidate_relies_on_high_turnover: `true`
- any_candidate_survives_cooldown_constraints: `true`
- monthly_results_deprecated: `true`
- next_route: `TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis`

## Validation Result

- `python -m ruff check src/ai_trading_system/dynamic_strategy_event_driven_retest.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_event_driven_retest.py`: PASS
- `python -m compileall src/ai_trading_system/dynamic_strategy_event_driven_retest.py src/ai_trading_system/cli_commands/research_execution_semantics.py`: PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_event_driven_retest.py`: 3 passed
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`: 3 passed
- `aits validate-data --as-of 2026-07-05`: PASS_WITH_WARNINGS, error_count=0
- `aits research strategies dynamic-strategy-event-driven-retest --as-of 2026-07-05`: READY
- `aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md`: PASS, checked_docs=553
- `aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json`: PASS, reports=1262
- `aits reports task-register-consistency run --as-of 2026-07-05`: PASS
- `aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json`: PASS
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`: PASS, 197 passed, runtime artifact `outputs/validation_runtime/contract-validation_20260705T094652Z/test_runtime_summary.json`
- `git diff --check`: no whitespace errors; `docs/task_register.md` reported a CRLF-to-LF normalization warning only.

## Safety Boundary

This task is strategy research only. It may read existing market data, strategy configs, signal artifacts, and run research backtests. It must not enable scheduler, append historical event logs, bind outcomes, mutate outcome stores, enable paper-shadow, enter production, call broker APIs, send orders, or generate daily reports.

## Validation Plan

```bash
aits validate-data --as-of 2026-07-05
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_event_driven_retest.py
python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py
aits research strategies dynamic-strategy-event-driven-retest --as-of 2026-07-05
aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md
aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json
aits reports task-register-consistency run --as-of 2026-07-05
aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## Progress Notes

- 2026-07-05: Added as `IN_PROGRESS`. TRADING-2364 completed the cadence bias audit and routed to event-driven retest / candidate ranking. This task must use a data-quality-gated retest and must not return to scheduler guardrail work.
- 2026-07-05: Implemented and moved to `DONE`. Real CLI run produced `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`; candidate ranking is ready, but the top candidate remains `OWNER_REVIEW_REQUIRED` because high turnover still needs TRADING-2366 cost / turnover / cooldown sensitivity.
