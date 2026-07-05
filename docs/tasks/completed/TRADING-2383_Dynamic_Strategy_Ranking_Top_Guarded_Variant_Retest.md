# TRADING-2383 Dynamic Strategy Ranking Top Guarded Variant Retest

## 结论

TRADING-2383 已完成 actual guarded ranking-top variant retest，真实 CLI run 返回
`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY`。

本次 run 已执行 cached-data quality gate：

- `aits validate-data --as-of 2026-07-05`
- status：`PASS_WITH_WARNINGS`
- errors：0
- warnings：2

最佳 guarded variant：

- `equal_risk_growth_tilt_guarded_turnover_v1`
- decision：`CONTINUE_OPTIMIZATION`
- candidate_ready_for_research_only_observation：false

下一步 route：

- `TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_Observation_Decision`

## 产物

- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/guarded_variant_retest_result.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/guarded_variant_ranking.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/time_regime_slice_matrix.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/decision_update.json`
- `docs/research/dynamic_strategy_ranking_top_guarded_variant_retest.md`
- `docs/research/dynamic_strategy_guarded_variant_ranking.md`
- `docs/research/dynamic_strategy_guarded_variant_slice_matrix.md`
- `docs/research/dynamic_strategy_2384_route.md`

## 边界

- scheduler：disabled
- scheduled task：not created
- event append：disabled
- outcome binding：disabled
- paper-shadow：disabled
- paper trade：not created
- shadow position：not created
- production：disabled
- broker action：none
- order generated：false
- daily report：not generated

## 验证

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_ranking_top_guarded_variant_retest.py`：3 passed
- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`：PASS_WITH_WARNINGS / errors=0 / warnings=2
- `python -m ai_trading_system.cli research strategies dynamic-strategy-ranking-top-guarded-variant-retest --as-of 2026-07-05`：READY
- `python -m ai_trading_system.cli docs validate-freshness`：568 docs PASS
- `python -m ai_trading_system.cli docs report-contract --latest`：1280 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run`：active=319 / completed=442 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260705T160431Z/test_runtime_summary.json`
- `git diff --check`：PASS；仅 CRLF normalization warning
