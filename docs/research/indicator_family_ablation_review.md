# Indicator Family Ablation Review

状态：`INDICATOR_FAMILY_ABLATION_EVIDENCE_READY`

本报告由 `aits research trends indicator-family-ablation` 生成。它消费既有 PIT feature、label 和 action-value research artifacts，不直接刷新 cached market/macro data；`data_quality_contract=PREVIOUS_VALIDATED_RESEARCH_ARTIFACTS`。

| Family | Selected channels | Blocked channels | 2023+ | Beta/TQQQ | Selected |
|---|---|---|---|---|---|
| trend_persistence | return_seeking_diagnostic | add_risk, allocation, production, broker | True | False | True |
| relative_strength | return_seeking_diagnostic | defensive_channel, add_risk, allocation, production, broker | True | True | True |
| volatility_compression | risk_on_veto | add_risk, allocation, production, broker | False | False | True |
| drawdown_recovery | do_not_de_risk | add_risk, allocation, production, broker | False | False | True |
| breadth_participation |  | all_channels_until_pit_source_approved | False | False | False |
| rates_liquidity | risk_on_veto | add_risk, allocation, production, broker | False | False | True |
| event_risk |  | all_channels_until_pit_source_approved | False | False | False |

结论：本批只生成 family-level evidence 和下一轮 research-only channel feature set。所有 family 仍 `can_emit_weights=false`，dynamic promotion、paper-shadow、production 和 broker 均保持关闭。
