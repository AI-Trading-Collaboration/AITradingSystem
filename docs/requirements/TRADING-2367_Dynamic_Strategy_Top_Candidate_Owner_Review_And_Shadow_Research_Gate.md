# TRADING-2367 Dynamic Strategy Top Candidate Owner Review And Shadow Research Gate

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2367_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE`
- priority: `P0`
- status: `DONE`
- owner: project owner follow-up review
- source route: `TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate`
- source tasks: `TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING`, `TRADING-2366_DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_ANALYSIS`
- target status: `DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY`
- target next route: `TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol`

## Scope

TRADING-2365 selected a return-ranking top candidate, while TRADING-2366 selected a different robustness top candidate after cost, turnover, cooldown, min-holding, slippage, and max-weight-delta sensitivity. This task reads the prior validated 2365 / 2366 artifacts, compares the ranking top and robustness top candidates, builds an owner review package, and produces a research-only shadow observation gate decision.

`shadow research gate` means research report observation only. It does not create paper trades, shadow positions, event rows, bound outcomes, production actions, broker calls, or daily reports.

## Implementation Plan

- Add `src/ai_trading_system/dynamic_strategy_top_candidate_owner_review_gate.py`.
- Add CLI `aits research strategies dynamic-strategy-top-candidate-owner-review-gate`.
- Read and verify TRADING-2365 event-driven retest artifacts.
- Read and verify TRADING-2366 sensitivity artifacts.
- Compare ranking top from 2365, robustness top from 2366, static baseline, and all candidates present in the source artifacts.
- Generate candidate owner review metrics covering returns, cost drag, drawdown, turnover, cooldown, constraints, ranking rank, robustness rank, and prior decisions.
- Generate a research-only shadow observation gate decision and owner review checklist.
- Write JSON outputs under `outputs/research_strategies/dynamic_strategy_top_candidate_owner_review_gate/`.
- Write Chinese research docs:
  - `docs/research/dynamic_strategy_top_candidate_owner_review_gate.md`
  - `docs/research/dynamic_strategy_candidate_owner_review_comparison.md`
  - `docs/research/dynamic_strategy_shadow_research_gate_decision.md`
  - `docs/research/dynamic_strategy_2368_route.md`
- Update report registry, artifact catalog, system flow, task register, and completed task archive.
- Add focused tests in `tests/research_strategies/test_dynamic_strategy_top_candidate_owner_review_gate.py`.

## Acceptance Criteria

- CLI runs and writes JSON / Markdown artifacts.
- `status=DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY`.
- Source TRADING-2365 artifact confirms `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`.
- Source TRADING-2366 artifact confirms `DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY`.
- Primary execution cadence remains `valid_until_window`.
- Output includes `ranking_top_from_2365`, `robustness_top_from_2366`, `ranking_robustness_divergence_detected`, `owner_review_required`, `candidate_review_comparison`, `recommended_gate_candidate`, `recommended_gate_decision`, `research_only_shadow_observation_allowed`, and `recommended_next_research_task`.
- Reports answer whether the 2365 ranking top remains recommended, whether the 2366 robustness top should replace it for research-only observation, whether ranking / robustness divergence exists, whether true paper-shadow is allowed, whether broker / production is allowed, and whether the next route is shadow observation protocol or more candidate optimization.
- Scheduler, event append, outcome binding, paper-shadow execution, paper trade creation, shadow position creation, production, daily report, and broker fields remain disabled / false / none.

## Safety Boundary

This task is strategy research only. It may read existing TRADING-2365 and TRADING-2366 artifacts and generate review reports. It must not enable scheduler, append historical event logs, bind outcomes, mutate outcome stores, create paper trades, create shadow positions, enable paper-shadow execution, enter production, call broker APIs, send orders, or generate daily reports.

`research_only_shadow_observation_allowed=true` is allowed only as a research report gate. It is not paper-shadow execution.

## Implementation Result

The implementation adds `aits research strategies dynamic-strategy-top-candidate-owner-review-gate` and writes the required owner review gate JSON / Markdown package. The real run for 2026-07-05 returned `DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY` with `primary_execution_cadence=valid_until_window` and `candidate_review_comparison` count 7.

TRADING-2365 ranking top remains `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` with decision `OWNER_REVIEW_REQUIRED`. TRADING-2366 robustness top is `dynamic_regime_overlay_v0_4_lower_turnover`. Ranking / robustness divergence is detected, so the recommended gate candidate is the robustness top, but the recommended gate decision remains `OWNER_REVIEW_REQUIRED` because turnover acceptability remains `NO` for both top candidates.

## Decision Result

- ranking top from TRADING-2365: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from TRADING-2366: `dynamic_regime_overlay_v0_4_lower_turnover`
- ranking / robustness divergence detected: `true`
- recommended gate candidate: `dynamic_regime_overlay_v0_4_lower_turnover`
- recommended gate decision: `OWNER_REVIEW_REQUIRED`
- research-only shadow observation allowed: `true`
- shadow observation should start: `OWNER_REVIEW_REQUIRED_BEFORE_START`
- true paper-shadow allowed: `false`
- paper trade / shadow position created: `false` / `false`
- production / broker allowed: `false` / `false`
- next route: `TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol`

## Validation Plan

```bash
python -m ruff check src/ai_trading_system/dynamic_strategy_top_candidate_owner_review_gate.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_top_candidate_owner_review_gate.py
python -m compileall src/ai_trading_system/dynamic_strategy_top_candidate_owner_review_gate.py src/ai_trading_system/cli_commands/research_execution_semantics.py
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_top_candidate_owner_review_gate.py
python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py
aits research strategies dynamic-strategy-top-candidate-owner-review-gate --as-of 2026-07-05
aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md
aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json
aits reports task-register-consistency run --as-of 2026-07-05
aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required for the implementation CLI because this task does not read cached market data or run a new backtest; it only reads prior artifacts that already carry `PASS_WITH_WARNINGS` data quality status from TRADING-2365 / TRADING-2366. The final validation may still reference the prior data quality status.

## Progress Notes

- 2026-07-05: Added as `IN_PROGRESS`. TRADING-2366 completed sensitivity and routed to this owner review gate. This task must compare the 2365 return-ranking top with the 2366 robustness top and must not enable scheduler, event append, outcome binding, paper-shadow execution, production, or broker.
- 2026-07-05: Implemented and moved to `DONE`. Validation passed Ruff, compileall, focused parallel pytest, adjacent execution-semantics parallel pytest, real CLI run, docs freshness, documentation contract, task-register consistency run/validate, contract-validation, and diff checks. `aits validate-data` was not rerun because the 2367 CLI only reads prior validated 2365 / 2366 artifacts and does not read cached market data or run a new backtest. Scheduler, event append, outcome binding, paper-shadow execution, paper trade, shadow position, production, daily report, and broker/order fields remain disabled / false / none.
