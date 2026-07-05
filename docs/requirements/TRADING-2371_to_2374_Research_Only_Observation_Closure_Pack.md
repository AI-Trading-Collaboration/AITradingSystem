# TRADING-2371 to TRADING-2374 Research-Only Observation Closure Pack

最后更新：2026-07-05

## Metadata

- task_family: `TRADING-2371_to_2374_RESEARCH_ONLY_OBSERVATION_CLOSURE_PACK`
- priority: `P0`
- status: `IN_PROGRESS`
- owner: system implementation; project owner reassessment after TRADING-2374
- source task: `TRADING-2370_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_NO_SIDE_EFFECT_VALIDATION`
- source status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_NO_SIDE_EFFECT_VALIDATION_READY`
- primary observation candidate: `dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top from 2365: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from 2366: `dynamic_regime_overlay_v0_4_lower_turnover`

## Scope

This closure pack turns the research-only shadow observation line into an auditable manual research loop and then stops at owner reassessment. It is not paper-shadow, not scheduler, not simulated trading, and not production.

## Step Breakdown

1. `TRADING-2371_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION`
   - Add owner review decision artifact and CLI.
   - Decision: `APPROVE_RESEARCH_ONLY_OBSERVATION_CONTINUE_WITH_NO_EXECUTION`.
   - Next route: `TRADING-2372_Dynamic_Strategy_Research_Only_Observation_Log_Schema_And_Report_Plan`.
2. `TRADING-2372_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_AND_REPORT_PLAN`
   - Define research-only observation log schema and report plan.
   - Do not generate daily reports or write event logs.
   - Next route: `TRADING-2373_Dynamic_Strategy_Research_Only_Observation_Report_Dry_Run`.
3. `TRADING-2373_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN`
   - Generate one manual research-only observation report dry-run from 2371 / 2372 / 2369 / 2370 artifacts.
   - Do not run scheduler, append events, bind outcomes, or create paper-shadow state.
   - Next route: `TRADING-2374_Dynamic_Strategy_Research_Only_Observation_Owner_Reassessment_Checkpoint`.
4. `TRADING-2374_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_CHECKPOINT`
   - Record owner reassessment checkpoint.
   - Stop linear observation tasks by default.
   - Final route: `OWNER_REASSESSMENT_REQUIRED_BEFORE_TRADING_2375`.

## Safety Boundary

The entire pack may only read prior validated research artifacts and generate research-only artifacts and documentation. It must not enable scheduler, create scheduled tasks, append historical event logs, bind outcomes, mutate outcome stores, enable paper-shadow, create paper trades, create shadow positions, enable production, call broker APIs, send orders, or generate daily reports.

## Validation Plan

Each step must run focused parallel pytest for its new module and then the standard governance gates:

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile <focused-test-file>
aits docs validate-freshness --output-path outputs/reports/docs_freshness_2026-07-05.md
aits docs report-contract --as-of 2026-07-05 --output-path outputs/reports/documentation_contract_2026-07-05.md --json-output-path outputs/reports/documentation_contract_2026-07-05.json
aits reports task-register-consistency run --as-of 2026-07-05
aits reports task-register-consistency validate --source-json-path outputs/reports/task_register_consistency_2026-07-05.json
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

`aits validate-data` is not required while the implementation only reads prior validated TRADING-2369 / 2370 / 2371 / 2372 artifacts and does not read fresh cached market data, recompute strategy state, generate technical features, run scoring, run a new backtest, or generate a daily report.

## Acceptance Criteria

- TRADING-2371 status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_READY`.
- TRADING-2372 status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_AND_REPORT_PLAN_READY`.
- TRADING-2373 status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_READY`.
- TRADING-2374 status: `DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_CHECKPOINT_READY`.
- TRADING-2374 final route: `OWNER_REASSESSMENT_REQUIRED_BEFORE_TRADING_2375`.
- All steps keep paper-shadow, paper trade, shadow position, event append, outcome binding, scheduler, production, daily report, broker, and order fields disabled / false / none.

## Progress Notes

- 2026-07-05: Added closure pack after TRADING-2370 completed replay no-side-effect validation and routed to TRADING-2371. Implementation must proceed in isolated task commits from 2371 through 2374.
- 2026-07-05: TRADING-2371 implemented and moved to `DONE`. Real run status is `DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_READY`; owner_decision=`APPROVE_RESEARCH_ONLY_OBSERVATION_CONTINUE_WITH_NO_EXECUTION`, research_only_observation_continue_allowed=true, and next route is `TRADING-2372_Dynamic_Strategy_Research_Only_Observation_Log_Schema_And_Report_Plan`.
- 2026-07-05: TRADING-2372 implemented and moved to `DONE`. Real run status is `DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_AND_REPORT_PLAN_READY`; observation_log_schema_ready=true, observation_report_plan_ready=true, schema_only=true, report_plan_only=true, periodic_daily_report_generated=false, event_log_written=false, and next route is `TRADING-2373_Dynamic_Strategy_Research_Only_Observation_Report_Dry_Run`. Validation passed full Ruff, `compileall -q src tests`, focused parallel pytest 3 passed, adjacent execution semantics parallel pytest 3 passed, real CLI run, docs freshness PASS, documentation contract PASS, task-register consistency run/validate PASS, contract-validation 197 passed, and `git diff --check`.
