# Free Feature Family Reopen Readiness

- 状态：`FREE_FEATURE_FAMILY_REOPEN_READINESS_READY_WITH_BLOCKERS`

| Family | Readiness | Reason |
|---|---|---|
| `rates_liquidity_free_v1` | `READY_FOR_RATES_LIQUIDITY_RESEARCH` | DGS2/DGS10/DTWEXBGS local PIT market series coverage evaluated. |
| `volatility_compression_free_v1` | `READY_FOR_VOLATILITY_COMPRESSION_RESEARCH` | VIX index level and QQQ realized volatility baseline evaluated. |
| `macro_event_calendar_free_v1` | `BLOCKED` | Official calendar rows require captured known_at/source_published_at evidence. |
| `event_risk_free_v1` | `DIAGNOSTIC_ONLY` | Overlay score can be produced, but calendar event risk remains incomplete without official rows. |
| `participation_proxy_free_v1` | `DIAGNOSTIC_ONLY` | ETF ratios are not true PIT breadth and cannot enter promotion evidence. |

所有 readiness 只允许后续 research；promotion 仍 blocked。
