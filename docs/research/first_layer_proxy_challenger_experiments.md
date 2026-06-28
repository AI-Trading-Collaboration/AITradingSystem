# First-layer proxy challenger experiments

- status: `FIRST_LAYER_PROXY_CHALLENGER_EXPERIMENTS_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `2022-12-01` to `latest`
- actual_signal_range: `2023-02-22` to `2026-03-27`
- data_quality_status: `PASS_WITH_WARNINGS`
- safety: row-level `validation_ready` is offline-only; `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`

## 结论

Challenger matrix 已生成，但 validation_ready 只表示 offline experiment readiness。RSP / QQQE 缺失会阻塞 equal/cap-weight divergence 与 combined proxy；所有 rows 仍不能进入 promotion、paper-shadow、production 或 broker。

## Experiments

|experiment|validation_ready|missing_proxy_ids|promotion_allowed|scope|
|---|---:|---|---:|---|
|`baseline`|True|``|False|offline_challenger_experiment_only_not_promotion|
|`baseline_plus_trend_structure`|True|``|False|offline_challenger_experiment_only_not_promotion|
|`volatility_regime`|True|``|False|offline_challenger_experiment_only_not_promotion|
|`risk_appetite`|True|``|False|offline_challenger_experiment_only_not_promotion|
|`equal_cap_weight_divergence`|False|`rsp_to_spy,qqqe_to_qqq`|False|offline_challenger_experiment_only_not_promotion|
|`combined_proxy`|False|`rsp_to_spy,qqqe_to_qqq`|False|offline_challenger_experiment_only_not_promotion|

## Audit notes

- experiment_count: `6`
- validation_ready_count: `4`
- promotion_allowed_count: `0`
- true_breadth_replaced: `False`
- stress_validation_allowed: `False`
