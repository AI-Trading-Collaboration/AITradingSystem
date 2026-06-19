# Static Baseline Family Result

- Status：STATIC_BASELINE_FAMILY_READY_RESEARCH_ONLY
- Range：2023-01-03 至 2023-07-31
- Data Quality：PASS_WITH_WARNINGS
- Production Effect：none

## Metrics

- B0H Total Return：32.31%；CAGR：64.36%；Max Drawdown：-6.25%；Turnover：0.0000
- B0R Total Return：31.29%；CAGR：62.12%；Max Drawdown：-6.13%；Turnover：0.5988

## B0R vs B0H

- return_delta：-0.010179
- cagr_delta：-0.022372
- drawdown_reduction：0.001184
- sharpe_delta：0.012066
- turnover_delta：0.598768

## Boundary

- Target Path Checksum：`668f538ecc30ef2a7f54a17b76586e25d89569cfc2229e12301ee87efb7a446c`
- Forbidden Logic：PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE
- Holdout Accessed：false

## Reader Brief

- Summary：B0H/B0R 双基准已生成；B0R 可作为 B1E 的 primary comparator。
- Key Result：STATIC_BASELINE_FAMILY_READY_RESEARCH_ONLY
- Blocking Issues：B1E attribution rerun 尚未完成前不得继续 B2/B3。
- Warnings：B0R 的 turnover 来自预先冻结的静态目标再平衡，不代表市场信号。
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：运行 B1E vs B0R attribution gate。
