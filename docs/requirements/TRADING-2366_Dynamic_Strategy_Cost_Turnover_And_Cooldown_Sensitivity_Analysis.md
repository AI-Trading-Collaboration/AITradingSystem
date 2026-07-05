# TRADING-2366 Dynamic Strategy Cost Turnover And Cooldown Sensitivity Analysis

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2366_DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_ANALYSIS`
- priority: `P0`
- status: `DONE`
- owner: project owner follow-up review
- source route: `TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis`
- source task: `TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING`
- target status: `DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY`
- target next route: `TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate`

## Scope

TRADING-2365 ranked dynamic strategy candidates under `valid_until_window` and left the top candidate in `OWNER_REVIEW_REQUIRED` because cost, turnover, cooldown, min-holding, slippage, and max-weight-delta sensitivity were not yet complete. This task reads the TRADING-2365 artifacts, reruns sensitivity under the same data-quality-gated execution semantics path, and updates candidate decisions.

## Implementation Plan

- Add `src/ai_trading_system/dynamic_strategy_cost_turnover_cooldown_sensitivity.py`.
- Add CLI `aits research strategies dynamic-strategy-cost-turnover-cooldown-sensitivity`.
- Read and verify TRADING-2365 event-driven retest result, candidate ranking, and cadence comparison matrix.
- Build a layered sensitivity grid covering cost / slippage, cooldown / min-holding, turnover cap / max-weight-delta, and combined stress scenarios.
- Rerun `valid_until_window` as the primary cadence and retain `cooldown_limited_event_driven` as robustness comparison.
- Write JSON outputs under `outputs/research_strategies/dynamic_strategy_cost_turnover_cooldown_sensitivity/`.
- Write Chinese research docs:
  - `docs/research/dynamic_strategy_cost_turnover_cooldown_sensitivity.md`
  - `docs/research/dynamic_strategy_sensitivity_matrix.md`
  - `docs/research/dynamic_strategy_decision_update_summary.md`
  - `docs/research/dynamic_strategy_2367_route.md`
- Update report registry, artifact catalog, system flow, task register, and completed task archive.
- Add focused tests in `tests/research_strategies/test_dynamic_strategy_cost_turnover_cooldown_sensitivity.py`.

## Acceptance Criteria

- CLI runs and writes JSON/Markdown artifacts.
- `status=DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY`.
- Source TRADING-2365 artifact is loaded and confirms `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`.
- Primary execution cadence is `valid_until_window`.
- Top candidate from 2365 exists and sensitivity candidates include the top three 2365 candidates plus current dynamic default if available.
- Sensitivity grid, cost-adjusted metrics, turnover metrics, cooldown metrics, combined stress results, ranking after sensitivity, decision update, top candidate after sensitivity, top candidate decision after sensitivity, and next route are present.
- Reports answer whether the 2365 top candidate survives realistic / conservative / harsh assumptions, whether it depends on high turnover, whether cooldown materially weakens the edge, whether ranking changes, and whether the candidate can be upgraded from `OWNER_REVIEW_REQUIRED`.
- Scheduler, event append, outcome binding, paper-shadow, production, daily report, and broker fields remain disabled / false / none.

## Safety Boundary

This task is strategy research only. It may read existing market data, strategy configs, TRADING-2365 artifacts, and run research backtests. It must not enable scheduler, append historical event logs, bind outcomes, mutate outcome stores, enable paper-shadow, enter production, call broker APIs, send orders, or generate daily reports.

## Implementation Result

The implementation adds `aits research strategies dynamic-strategy-cost-turnover-cooldown-sensitivity` and writes the required sensitivity JSON / Markdown package. The real run for 2026-07-05 returned `DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY` with `primary_execution_cadence=valid_until_window`, `candidate_count=3`, `sensitivity_scenario_count=69`, and `matrix_row_count=483`.

The TRADING-2365 top candidate remains `OWNER_REVIEW_REQUIRED` after sensitivity. Ranking changed after sensitivity: `dynamic_regime_overlay_v0_4_lower_turnover` became the top robustness candidate, while `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` remains viable but owner-review-gated because turnover and drawdown sensitivity remain material. No candidate is promoted to paper-shadow, production, scheduler, or broker action.

## Decision Result

- top candidate from TRADING-2365: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- source top decision after sensitivity: `OWNER_REVIEW_REQUIRED`
- top candidate after sensitivity: `dynamic_regime_overlay_v0_4_lower_turnover`
- top candidate decision after sensitivity: `OWNER_REVIEW_REQUIRED`
- ranking changed after sensitivity: `YES`
- source top survives realistic / conservative cost: `YES` / `YES`
- source top turnover acceptable: `NO`
- upgrade from owner review recommended: `NO`
- next route: `TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate`

## Validation Plan

```bash
aits validate-data --as-of 2026-07-05
python -m ruff check src/ai_trading_system/dynamic_strategy_cost_turnover_cooldown_sensitivity.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_cost_turnover_cooldown_sensitivity.py
python -m compileall src/ai_trading_system/dynamic_strategy_cost_turnover_cooldown_sensitivity.py src/ai_trading_system/cli_commands/research_execution_semantics.py
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_cost_turnover_cooldown_sensitivity.py
python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py
aits research strategies dynamic-strategy-cost-turnover-cooldown-sensitivity --as-of 2026-07-05
aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md
aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json
aits reports task-register-consistency run --as-of 2026-07-05
aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## Progress Notes

- 2026-07-05: Added as `IN_PROGRESS`. TRADING-2365 completed event-driven retest / candidate ranking and routed to this cost, turnover, and cooldown sensitivity task. This task must use a data-quality-gated retest and must not enable scheduler, paper-shadow, production, or broker.
- 2026-07-05: Implemented and moved to `DONE`. Validation passed Ruff, compileall, focused parallel pytest, adjacent execution-semantics parallel pytest, `aits validate-data --as-of 2026-07-05` (`PASS_WITH_WARNINGS`, error_count=0), real CLI run, docs freshness, documentation contract, task-register consistency run/validate, contract-validation, and diff checks. Scheduler, event append, outcome binding, paper-shadow, production, daily report, and broker/order fields remain disabled / false / none.
