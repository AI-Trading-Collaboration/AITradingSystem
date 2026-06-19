# B0 Static Strategic Baseline Result

最后更新：2026-06-19

状态：`B0_MINI_BACKFILL_COMPLETE_CONTROL_ONLY`

## 结论

B0 静态战略基线已经在 `normal_market_regime` mini-backfill 诊断窗口完成一次可复现运行。
该结果只作为研究控制组，不是 candidate promotion、v3 approval、paper-shadow 或 official
target weights。

## 运行窗口

- Market regime：`ai_after_chatgpt`
- Requested range：2023-01-03 至 2023-07-31
- Effective signal range：2023-01-03 至 2023-07-27
- Window catalog：`docs/research/research_window_catalog.json`
- Data quality gate：`aits validate-data`，结果 `PASS_WITH_WARNINGS`
- ETF backtest summary data quality：`PASS`

## B0 来源

B0 使用 backtest benchmark `B000/static_default_portfolio`。权重来自
`config/etf_portfolio/assets.yaml` 的 `default_weight`：

| Symbol | Weight |
|---|---:|
| SPY | 30.00% |
| QQQ | 40.00% |
| SMH | 15.00% |
| SOXX | 0.00% |
| CASH | 15.00% |

## Mini-Backfill 指标

| Metric | Value |
|---|---:|
| Total return proxy | 31.31% |
| CAGR | 62.16% |
| Max drawdown proxy | -6.13% |
| Sharpe | 3.25 |
| Turnover | 0.00 |
| Rotation count | 0 |
| Cost-adjusted proxy | 31.31% |
| Excess total return vs B001 | 11.12% |
| Drawdown reduction vs B001 | 1.41% |

`cost-adjusted proxy` 沿用现有 static benchmark 口径：初始化后无 rebalance turnover，
不生成 broker/order execution model，也不模拟真实建仓订单成本。

## Runtime Artifacts

- Run id：`etf-backtest-20260619T074656Z`
- Summary JSON：`reports/etf_portfolio/backtests/weight_research_v1_b0/etf-backtest-20260619T074656Z/summary.json`
- Metrics JSON：`reports/etf_portfolio/backtests/weight_research_v1_b0/etf-backtest-20260619T074656Z/metrics.json`
- Data quality report：`outputs/reports/data_quality_2026-06-19.md`

## 限制

- 这只是 B0 control result，不能证明 B1-B6 的任何独立增益。
- `untouched_temporal_holdout` 仍未冻结，本次没有使用 holdout。
- Stress / window stability 不能由单一 normal-market mini window 得出结论。
- B1-B6 需要独立 runner 和 signal robustness evidence，不能用 P0 动态策略总结果替代。

## Safety Boundary

`research_only=true`、`manual_review_only=true`、`paper_shadow_activation=false`、
`official_target_weights=false`、`broker_action_allowed=false`、`production_effect=none`。
