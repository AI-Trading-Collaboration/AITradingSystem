# B1 Isolated Attribution Result

- Status：B1_ATTRIBUTION_VALID_MIXED
- Range：2023-01-03 至 2023-07-31
- Data Quality：PASS_WITH_WARNINGS
- Target Path Validation：PASS
- Production Effect：none

## Metrics

- B1E Total Return：31.60%；CAGR：62.79%；Max Drawdown：-6.25%；Turnover：0.1243
- B0R Total Return：31.29%；CAGR：62.12%；Max Drawdown：-6.13%；Turnover：0.5988
- B0H Total Return：32.31%；CAGR：64.36%；Max Drawdown：-6.25%；Turnover：0.0000

## B1E vs B0R

- return_delta：0.003019
- cagr_delta：0.006622
- drawdown_reduction：-0.001184
- sharpe_delta：-0.011380
- turnover_delta：-0.474450

## Execution Metrics

- gross_target_turnover：0.5987679672402311
- executed_turnover：0.12431805306786908
- skipped_trades：140
- cost_saved：9.488998283447237e-05
- missed_benefit_proxy：0.0
- average_execution_delay：NOT_MODELED_B1_USES_SKIP_OR_TRADE_DECISIONS
- constraint_hit_count：0
- urgent_risk_action_delay：NOT_APPLICABLE_NO_RISK_SIGNAL_IN_B1

## Attribution Gate

- B2/B3 May Continue：True
- E0/E1 Variants Required：True

## Reader Brief

- Summary：B1E 已使用 B0R 作为 primary comparator 形成可审计归因。
- Key Result：B1_ATTRIBUTION_VALID_MIXED
- Blocking Issues：none
- Warnings：若结果为 mixed/negative，E 不得默认进入最终候选，后续 R/T 必须运行 E0/E1。
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：冻结五层接口并建立 signal diagnostics framework
