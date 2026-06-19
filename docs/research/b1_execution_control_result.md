# B1 Execution Control Result

最后更新：2026-06-19

状态：`B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY`

## 结论

B1 execution / no-trade / turnover-control runner 已在 `normal_market_regime` mini-backfill
窗口完成。该 runner 没有调用 P0 dynamic allocator、signals、regime 或 feature store。

结果属于 mixed research-only evidence：B1 相对 B0 的 total return 略高，但 max drawdown
略差，turnover 从 0 增加到 0.1243。不得自动进入 B2。

## 指标

| Metric | B1 | B0 | Delta |
|---|---:|---:|---:|
| Total return | 31.60% | 31.31% | 0.29% |
| CAGR | 62.79% | 62.16% | 0.63% |
| Max drawdown | -6.25% | -6.13% | -0.12% |
| Turnover | 0.1243 | 0.0000 | 0.1243 |

## Validation

- 511A-C contract validation：`PASS`
- Data quality gate：`PASS_WITH_WARNINGS`
- Signal robustness：`NOT_APPLICABLE_B1_EXECUTION_CONTROL_NO_SIGNAL_INPUT`
- Holdout accessed：`false`
- Forbidden logic check：`PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE`

## Runtime Artifacts

- Contract validation：`reports/etf_portfolio/weight_research/unblock_511/contract_validation_20260619T082538.json`
- B1 result：`reports/etf_portfolio/weight_research/unblock_511/b1_execution_control_result_20260619T082549.json`
- B1 daily：`reports/etf_portfolio/weight_research/unblock_511/b1_execution_control_daily_20260619T082549.csv`

## Safety Boundary

`research_only=true`、`manual_review_only=true`、`paper_shadow_activation=false`、
`official_target_weights=false`、`broker_action_allowed=false`、`production_effect=none`。

## Next Action

人工复核 B1 mixed evidence。未经复核，不得实现或运行 B2。
