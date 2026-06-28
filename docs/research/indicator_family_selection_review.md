# Indicator Family Selection Review

状态：`INDICATOR_FAMILY_SELECTION_READY`

该报告为 research-only evidence，不产生 target weights，不触发 promotion、paper-shadow、production 或 broker。

| family_name | selected_for_next_model | selected_channels | blocked_channels | rejected_reason |
|---|---|---|---|---|
| trend_persistence | True | return_seeking_diagnostic | add_risk, allocation, production, broker | 2023+ trend dependence blocks add-risk allocation evidence. |
| relative_strength | True | return_seeking_diagnostic | defensive_channel, add_risk, allocation, production, broker | Relative-strength evidence includes QQQ/TQQQ consistency and is beta/TQQQ dependent. |
| volatility_compression | True | risk_on_veto | add_risk, allocation, production, broker | Useful as veto evidence; not an add-risk accelerator. |
| drawdown_recovery | True | do_not_de_risk | add_risk, allocation, production, broker | Selected only for do-not-de-risk; false rebound risk blocks add-risk. |
| breadth_participation | False |  | all_channels_until_pit_source_approved | No PIT-approved breadth or constituent-history source is available locally. |
| rates_liquidity | True | risk_on_veto | add_risk, allocation, production, broker | Macro context is useful as risk-on veto, not return boost. |
| event_risk | False |  | all_channels_until_pit_source_approved | Event timestamps and availability source are not PIT-approved. |
