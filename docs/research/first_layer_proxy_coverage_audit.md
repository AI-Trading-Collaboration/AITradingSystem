# First-layer proxy coverage audit

- status: `FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `2022-12-01` to `latest`
- primary_window_start: `2021-02-22`
- data_quality_status: `PASS_WITH_WARNINGS`
- safety: `replacement_for_true_breadth_count=0`, `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`

## 结论

免费 / 低成本 proxy 只能保留为 diagnostic input。可用 ETF ratio 是 price proxy，不是 historical constituent breadth；listing status 和 holdings gate 仍缺 known-at / historical membership 证明，不能替代 true breadth。

## Coverage rows

|proxy_id|group|data_available|primary_window_coverage|PIT_safe_or_not|replacement_for_true_breadth|
|---|---|---:|---|---|---:|
|`rates_liquidity_free_v1`|free_feature_family|True|covered|`PIT_APPROVED_MARKET_SERIES`|False|
|`volatility_compression_free_v1`|free_feature_family|True|covered|`PIT_APPROVED_PRICE_PROXY`|False|
|`macro_event_calendar_free_v1`|free_feature_family|False|missing|`PIT_WARNING_UNTIL_SOURCE_PUBLISHED_AT_CONFIRMED`|False|
|`event_risk_free_v1`|free_feature_family|True|covered|`PIT_WARNING_DIAGNOSTIC_ONLY`|False|
|`participation_proxy_free_v1`|free_feature_family|True|registry_only|`NOT_TRUE_PIT_BREADTH`|False|
|`smh_to_qqq`|etf_ratio_price_proxy|True|covered|`PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH`|False|
|`soxx_to_qqq`|etf_ratio_price_proxy|True|covered|`PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH`|False|
|`rsp_to_spy`|etf_ratio_price_proxy|True|covered|`PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH`|False|
|`qqqe_to_qqq`|etf_ratio_price_proxy|True|covered|`PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH`|False|
|`sector_etf_relative_strength`|etf_ratio_price_proxy|False|missing_components:XLK|`PIT_BLOCKED_BY_PRICE_COVERAGE`|False|
|`alpha_vantage_listing_status`|external_low_cost_proxy_gate|False|missing_or_unverified|`PIT_WARNING_CURRENT_SNAPSHOT_NOT_INDEX_MEMBERSHIP`|False|
|`fmp_etf_holdings_low_cost_gate`|external_low_cost_proxy_gate|False|missing_or_unverified|`PIT_WARNING_UNTIL_HOLDING_DATE_REPORTED_DATE_KNOWN_AT_CONFIRMED`|False|

## Audit notes

- proxy_count: `12`
- data_available_count: `8`
- primary_window_covered_count: `7`
- `replacement_for_true_breadth=false` 是本报告的核心结论；任何后续 challenger experiment 都必须继续把 proxy 与 true breadth 区分开。
