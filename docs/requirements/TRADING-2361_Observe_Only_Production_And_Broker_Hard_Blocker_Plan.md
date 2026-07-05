# TRADING-2361 Observe-Only Production And Broker Hard-Blocker Plan

最后更新：2026-07-05

## Metadata

- task_id: `TRADING-2361_OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN`
- priority: `P0`
- status: `DONE`
- owner: system implementation; project owner follow-up review
- source route: `TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan`
- target status: `OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`
- target readiness: `READY_FOR_2362_WITH_CAVEATS`

## Scope

Define the current production and broker-action hard blockers after TRADING-2360 without entering paper-shadow, production, broker, account, order, or capital-at-risk paths.

## Implementation Plan

- Add `src/ai_trading_system/high_intensity_risk_cap_production_broker_hard_blocker_plan.py`.
- Add CLI `aits research trends high-intensity-risk-cap-observe-only-production-broker-hard-blocker-plan`.
- Write outputs under `outputs/research_trends/high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan/`.
- Write research docs:
  - `docs/research/high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.md`
  - `docs/research/high_intensity_2362_promotion_blocker_matrix_route.md`
- Add focused tests in `tests/research_trends/test_high_intensity_production_broker_hard_blocker_plan.py`.

## Acceptance Criteria

- CLI runs and writes JSON/Markdown artifacts.
- Loader fail-closes unless TRADING-2360, TRADING-2359, and TRADING-2358 artifacts are readable and match expected blocked routes.
- `production_hard_blocker_plan_ready=true`.
- `broker_hard_blocker_plan_ready=true`.
- `capital_at_risk_blocker_ready=true`.
- `human_confirmation_requirement_ready=true`.
- `production_enabled=false`, `production_attempted=false`.
- `broker_action_enabled=false`, `broker_action_attempted=false`.
- `capital_at_risk_allowed=false`.
- `promotion_allowed=false`.
- `next_route=TRADING-2362_Observe_Only_Consolidated_Promotion_Blocker_And_Safety_Evidence_Matrix`.

## Safety Boundary

This task must not enable scheduler, create cadence, execute manual run, append event, mutate event log, bind outcome, mutate outcome store, enable paper-shadow, create paper trade, create shadow position, enable production, import/call broker API, query account for execution, create/preview/send order, read fresh market data, generate signal, run backtest, or generate daily report.

## Validation Plan

```bash
python -m pytest -n 16 --dist loadfile tests/research_trends/test_high_intensity_production_broker_hard_blocker_plan.py
```

Then run the unified closeout validation from the owner pack.

## Progress Notes

- 2026-07-05: Added as `IN_PROGRESS`. TRADING-2360 is already complete, so implementation starts from TRADING-2361.
- 2026-07-05: Implemented and moved to `DONE`. The real CLI generated the production hard-blocker plan, broker hard-blocker plan, capital-at-risk blocker, human confirmation requirement, blocked-promotion rationale, safety boundary, research docs, and 2362 route with status `OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`. Focused parallel pytest for TRADING-2361 through TRADING-2363 passed 13 tests.
