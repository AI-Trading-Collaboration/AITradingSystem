# Indicator Family Ablation Review

状态：`INDICATOR_FAMILY_ABLATION_READY`

本报告由 `aits research trends indicator-family-ablation` 生成。当前为 registry-only diagnostic matrix，不消费 cached market data，因此 `data_quality_contract=NOT_REQUIRED_REGISTRY_ONLY`。

| Family | PIT | Diagnostic only | Candidate |
|---|---|---|---|
| trend_persistence | True | True | False |
| relative_strength | True | True | False |
| volatility_compression | True | True | False |
| drawdown_recovery | True | True | False |
| breadth_participation | True | True | False |
| rates_liquidity | True | True | False |
| event_risk | True | True | False |

所有 family 输出均为 diagnostic-only。没有真实 ablation evidence 和预注册 selection rule 前，不允许进入 combined model candidate、promotion、paper-shadow、production 或 broker。
