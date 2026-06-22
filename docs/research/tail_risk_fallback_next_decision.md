# Tail-Risk Fallback Next Decision

状态：TAIL_RISK_NEXT_DECISION_BLOCKED

本文件是人工复核用的研究决策文档，不允许 promotion、paper-shadow、production weight 或 broker/order action。

## 结论

- 当前策略是否仍然 blocked：True
- independent outcome 是否足够：True
- 是否被简单 baseline 支配：True
- 是否值得构建 trigger v2：False
- owner_next_action：pause

## 污染指标

- precision
- recall
- f1
- return metrics
- tail-risk hit rate
- fallback triggered hit rate
- label-based validation

## Research-Only 可用指标

- independent_forward_outcome_sample_counts
- counterfactual_baseline_diagnostics
- error_cost_ledger
- feature_availability_catalog

## Paper-Shadow Review 前置任务

- TRADING-827 resolved or accepted as quarantine reason
- TRADING-828 independent forward outcomes present
- TRADING-829/830/838 no blocking leakage or boundary status
- TRADING-839 manual reviewable readiness gate
