# TRADING-1049 to 1056 Equal-Risk Growth V2 Real-Run Result Convergence

## Background

TRADING-1031 to 1048 implemented the research-only equal-risk forward-aging
stabilization layer and controlled growth component v2 restart. This batch does
not widen the research surface. It runs and summarizes those CLIs, then decides
whether equal-risk remains healthy and whether controlled growth v2 has a
candidate strong enough for component-ready owner review.

All outputs keep the fixed safety boundary:

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`

Layer-1 selector remains dry-run only. QQQ-plus growth remains inactive
reference. Tail-risk fallback, TQQQ-heavy mainline, LEAPS, Wheel and Options
remain blocked or quarantined.

## Step Breakdown

|Task|Stage|Status|Acceptance Criteria|
|---|---|---|---|
|TRADING-1049|Real CLI suite summary|VALIDATING|Add `aits research strategies equal-risk-growth-v2-real-cli-suite`, run the 18 TRADING-1031 to 1048 CLIs through the same builders, and write unified JSON/Markdown status rows.|
|TRADING-1050|Equal-risk live health summary|VALIDATING|Add health summary CLI with latest observation, continuity, duplicate, invalid artifact, maturity, scoreboard and Reader Brief safety fields.|
|TRADING-1051|Controlled growth v2 candidate summary|VALIDATING|Add grouped candidate summary across low-turnover, volatility-targeted, drawdown-guarded, attribution, period, cost and readiness artifacts.|
|TRADING-1052|Beta-adjusted edge review|VALIDATING|Add review comparing top candidate to `100_qqq`, `equal_risk_qqq_sgov`, `qqq_60_sgov_40` and `qqq_50_sgov_50`; classify beta-only, material, weak or regime-concentrated edge.|
|TRADING-1053|Period/drawdown/cost triage|VALIDATING|Add unified triage for period split, drawdown episode and cost sensitivity stability.|
|TRADING-1054|Growth final gate|VALIDATING|Add final gate deciding whether component-ready review is allowed while keeping paper-shadow, production and broker disabled.|
|TRADING-1055|Dual-track owner decision pack|VALIDATING|Add owner decision pack JSON and `docs/research/dual_track_owner_decision_pack.md`.|
|TRADING-1056|Roadmap v2 real-result master review|VALIDATING|Add master review JSON and `docs/research/roadmap_v2_real_result_master_review.md` summarizing 1049 to 1055.|

## Dependencies and Sequencing

1. Register the task and preserve the detailed requirement in this document.
2. Implement the convergence module and CLI wrappers.
3. Update report registry, artifact catalog and system flow.
4. Extend focused tests for builder output, safety fields and report registry.
5. Run the required validation commands from the attachment.

## Validation Commands

```bash
python -m ruff check src tests
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_research_restart.py
python -m pytest -n 16 --dist loadfile tests/test_layer1_meta_policy_readiness.py
python -m pytest -n 16 --dist loadfile tests/test_layer2_strategy_component_readiness.py
python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py
git diff --check
```

## Progress Log

- 2026-06-26: Added and moved to `IN_PROGRESS` because the project owner requested TRADING-1049 to 1056 real-run result convergence after TRADING-1031 to 1048. Scope is limited to research-only aggregation, health review, growth candidate review, final gate, owner decision pack and master review.
- 2026-06-26: Implementation completed and moved to `VALIDATING`. Added eight research strategy CLIs/artifacts, report registry entries, artifact catalog/system flow updates and focused tests. Validation passed Ruff, compileall, focused equal-risk/growth pytest, Layer-1/Layer-2 regression pytest, report/task/documentation pytest and `git diff --check`.
