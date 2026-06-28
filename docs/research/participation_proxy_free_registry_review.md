# Participation Proxy Free Registry Review

- 状态：`DIAGNOSTIC_ONLY`

| Proxy | Ratio | Status | Caveats |
|---|---|---|---|
| `smh_to_qqq` | `SMH/QQQ` | `DIAGNOSTIC_ONLY` | `NOT_TRUE_PIT_BREADTH, ETF_PRICE_ONLY, SURVIVORSHIP_SAFE_IF_ETF_PRICE_ONLY` |
| `soxx_to_qqq` | `SOXX/QQQ` | `DIAGNOSTIC_ONLY` | `NOT_TRUE_PIT_BREADTH, ETF_PRICE_ONLY, SURVIVORSHIP_SAFE_IF_ETF_PRICE_ONLY` |
| `rsp_to_spy` | `RSP/SPY` | `REGISTRY_ONLY` | `NOT_TRUE_PIT_BREADTH, PRICE_CACHE_COVERAGE_REQUIRED, DIAGNOSTIC_ONLY` |
| `qqqe_to_qqq` | `QQQE/QQQ` | `REGISTRY_ONLY` | `NOT_TRUE_PIT_BREADTH, PRICE_CACHE_COVERAGE_REQUIRED, DIAGNOSTIC_ONLY` |
| `sector_etf_relative_strength` | `sector_etf_basket/QQQ` | `REGISTRY_ONLY` | `NOT_TRUE_PIT_BREADTH, PRICE_CACHE_COVERAGE_REQUIRED, DIAGNOSTIC_ONLY` |

这些 proxy 不是 true PIT breadth，不允许进入 promotion evidence。
