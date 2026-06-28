# Channel-Specific v3 Selection Rule Review

selection_status：`RISK_ON_VETO_PASS`
final_status：`CHANNEL_V3_RISK_ON_VETO_ONLY`

| channel | status | allowed_families | key evidence |
| --- | --- | --- | --- |
| do_not_de_risk | DO_NOT_DERISK_FAIL | drawdown_recovery | false_risk_off_cost_reduction=False, missed_upside_reduction=False, defensive_probe_regression_count=8 |
| risk_on_veto | RISK_ON_VETO_PASS | volatility_compression, rates_liquidity | false_add_risk_cost_reduction=True, veto_blocks_growth_overlay=True, tqqq_allocation_enabled=False |

即使 channel 通过，本批最多允许 observe-only diagnostic；promotion、paper-shadow、production、broker 均保持 blocked。
