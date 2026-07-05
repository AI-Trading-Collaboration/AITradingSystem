# TRADING-2375 Dynamic Strategy Candidate Optimization And Ranking-Robustness Divergence Review Completion

最后更新：2026-07-05

## Summary

- status: `DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`
- ranking top from 2365: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from 2366: `dynamic_regime_overlay_v0_4_lower_turnover`
- divergence detected: `true`
- primary execution cadence: `valid_until_window`
- monthly rebalance allowed for primary ranking: `false`
- best candidate after optimization: `dynamic_regime_overlay_v0_4_lower_turnover`
- recommended decision after optimization: `OWNER_REVIEW_REQUIRED`
- next route: `TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest`
- daily report generated: `false`
- scheduler enabled: `false`
- event append enabled: `false`
- outcome binding enabled: `false`
- paper-shadow enabled: `false`
- production enabled: `false`
- broker action enabled: `false`

## Artifacts

- `outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/divergence_review_result.json`
- `outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/optimization_matrix.json`
- `outputs/research_strategies/dynamic_strategy_candidate_optimization_divergence_review/candidate_decision_update.json`
- `docs/research/dynamic_strategy_candidate_optimization_divergence_review.md`
- `docs/research/dynamic_strategy_ranking_vs_robustness_divergence_review.md`
- `docs/research/dynamic_strategy_candidate_optimization_matrix.md`
- `docs/research/dynamic_strategy_2376_route.md`

## Decision Notes

TRADING-2375 confirmed the 2365 / 2366 divergence. The ranking top preserved the largest cost-adjusted return gap, but its drawdown and rebalance profile remained materially less robust. The fusion candidates were generated and stress-tested, but the best optimization score still selected `dynamic_regime_overlay_v0_4_lower_turnover`; fusion candidates did not yet outperform both source candidates. The next step is targeted retest, not paper-shadow.

## Safety Boundary

This task only generated strategy research artifacts. It did not enable scheduler, append events, bind outcomes, mutate outcome stores, generate a daily report, enable paper-shadow, create a paper trade, create a shadow position, enable production, call broker API, generate an order, or touch broker state.

## Validation

- `aits validate-data --as-of 2026-07-05`: `PASS_WITH_WARNINGS`, errors=0
- full Ruff: PASS
- compileall: PASS
- focused parallel pytest: 3 passed
- real CLI run: `DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`
- docs freshness: PASS
- documentation contract: PASS
- task-register consistency run/validate: PASS
- contract-validation: 197 passed (`outputs/validation_runtime/contract-validation_20260705T125418Z/test_runtime_summary.json`)
- git diff --check: PASS with CRLF normalization warnings only
