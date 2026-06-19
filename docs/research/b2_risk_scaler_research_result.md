# B2 Risk Scaler Research Result

- Status：B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY
- Signal Gate：B2_SIGNAL_READY
- Data Quality：PASS_WITH_WARNINGS
- Production Effect：none

## Metrics

- B2-E0 Total Return：-7.21%；CAGR：-59.26%；Max Drawdown：-10.96%；Turnover：0.1383
- B2-E1 Total Return：-7.20%；CAGR：-59.19%；Max Drawdown：-10.84%；Turnover：0.0000

## B2-E1 vs B2-E0

- return_delta：0.000127
- cagr_delta：0.000670
- drawdown_reduction：0.001187
- sharpe_delta：-0.081906
- turnover_delta：-0.138327

## Reader Brief

- Summary：B2 risk signal, diagnostics, target mapping and E0/E1 mini-backfill completed.
- Key Result：B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY
- Blocking Issues：none
- Warnings：B2 is research-only and changes total exposure only, not relative asset selection.
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：Proceed to B3 only if B2 signal diagnostics are non-BLOCKED.
