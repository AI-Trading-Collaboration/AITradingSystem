# B1 Metric Semantics And Comparator Audit

- Status：B1_ATTRIBUTION_PARTIAL
- Source：D:\Work\AITradingSystem\docs\research\b1_execution_control_result.json
- Production Effect：none

## Metric Contract

| Metric | Unit | Formula | Positive Direction |
|---|---|---|---|
| return_delta | absolute return fraction | candidate.total_return - comparator.total_return | candidate return is higher |
| drawdown_reduction | absolute max-drawdown fraction | abs(comparator.max_drawdown) - abs(candidate.max_drawdown) | candidate drawdown is smaller |
| turnover_delta | absolute cumulative turnover | candidate.turnover - comparator.turnover | lower is better for execution control when comparator is B0R |

## Checks

| Check | Status | Message | Observed |
|---|---|---|---|
| return_delta_semantics | PASS | absolute total-return fraction difference: B1 total_return - historical B0 total_return | 0.0028624381570199198 |
| drawdown_reduction_semantics | PASS | abs(B0 max_drawdown) - abs(B1 max_drawdown); positive means drawdown improved | -0.001205305105831056 |
| turnover_delta_semantics | PASS | absolute cumulative-turnover difference: B1 turnover - historical B0 turnover | 0.12431805306786908 |
| historical_b0_turnover_zero | WARN | historical B0 reports zero turnover, so it is not a valid rebalance comparator | 0.0 |
| historical_b1_positive_turnover | WARN | historical B1 positive turnover can come from static target rebalancing drift, not only execution control | 0.12431805306786908 |
| pure_execution_attribution | WARN | B1 - historical B0 mixes execution/no-trade controls with the introduction of static target rebalancing | requires B0R comparator |

## Reader Brief

- Summary：历史 B1 指标单位和方向可以解释，但 comparator attribution 只部分有效。
- Key Result：B1_ATTRIBUTION_PARTIAL
- Blocking Issues：B1E 必须使用 B0R 作为 primary comparator 后才能进入有效归因。
- Warnings：历史 B1 - B0 不应解释为纯 execution/no-trade 模块贡献。
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：运行 B0H/B0R baseline family，然后重新运行 B1E vs B0R。
