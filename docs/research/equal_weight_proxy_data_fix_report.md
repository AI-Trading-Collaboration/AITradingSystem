# Equal-weight proxy data fix report

- status: `EQUAL_WEIGHT_PROXY_DATA_FIX_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `2022-12-01` to `2026-06-26`
- repair_window: `2018-01-01` to `2026-06-26`
- data_quality_before: `PASS_WITH_WARNINGS`
- data_quality_after: `PASS_WITH_WARNINGS`
- safety: `replacement_for_true_breadth=false`, `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`

## 结论

RSP / QQQE 的原始阻塞不是 provider 不支持，也不是 symbol mapping 问题；TRADING-2271 的缺失来自本地价格缓存未覆盖这些 ticker。免费 Yahoo Finance 路径可返回两者历史价格，本批已补齐为 price-only diagnostic proxy。即使补齐，RSP/SPY 与 QQQE/QQQ 仍不是 historical constituent breadth，不能声明 true breadth。

## Root cause

|symbol|root_cause|provider_status|rows_valid|cache_after_start|cache_after_end|replacement_for_true_breadth|
|---|---|---|---:|---|---|---:|
|`RSP`|`LOCAL_PRICE_CACHE_COVERAGE_GAP_NOT_PROVIDER_OR_MAPPING`|`AVAILABLE`|2132|2018-01-02|2026-06-26|False|
|`QQQE`|`LOCAL_PRICE_CACHE_COVERAGE_GAP_NOT_PROVIDER_OR_MAPPING`|`AVAILABLE`|2132|2018-01-02|2026-06-26|False|

## Updated proxy coverage

- proxy_count: `12`
- data_available_count: `8`
- primary_window_covered_count: `7`
- replacement_for_true_breadth_count: `0`

|alternative|status|allowed_usage|replacement_for_true_breadth|
|---|---|---|---:|
|`qqqe_to_qqq`|`PRICE_PROXY_AVAILABLE_NOT_TRUE_BREADTH`|nasdaq_equal_cap_weight_price_diagnostic|False|
|`rsp_to_spy`|`PRICE_PROXY_AVAILABLE_NOT_TRUE_BREADTH`|sp500_equal_cap_weight_price_diagnostic|False|
|`sector_etf_relative_strength`|`BLOCKED`|sector_price_confirmation_diagnostic|False|
|`top_n_equal_weight_proxy`|`DIAGNOSTIC_DESIGN_ONLY_NOT_BUILT`|diagnostic_proxy_only_not_true_breadth|False|

## Artifacts

- updated_proxy_coverage_matrix: `D:\Work\AITradingSystem\outputs\research_trends\equal_weight_proxy_data_fix\updated_proxy_coverage_matrix.json`
- blocked_proxy_resolution_status: `D:\Work\AITradingSystem\outputs\research_trends\equal_weight_proxy_data_fix\blocked_proxy_resolution_status.json`

## Blocked status

True breadth 仍被 `price_proxy_not_constituent_membership` 阻塞。解除条件是提供 owner-approved PIT breadth source，包含 historical constituents、daily membership query、delisted securities 和 known-at semantics。
