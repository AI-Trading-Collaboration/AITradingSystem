# Market Calendar Freshness Policy

最后更新：2026-06-16

本文记录 TRADING-366 的 market-calendar-aware freshness policy。该 policy 只用于
market data freshness 解释，不放宽 evidence staleness threshold，不刷新数据，不修改
paper-shadow、broker 或 production state。

## Resolver

实现入口：

- `src/ai_trading_system/trading_calendar.py`
- `src/ai_trading_system/market_calendar_freshness.py`

`resolve_us_equity_market_freshness` 输出：

- `latest_complete_market_date`
- `freshness_reference_date`
- `market_calendar_status`
- `market_calendar_reason`
- `market_session_kind`
- `calendar_adjustment_reason`
- `calendar_adjusted_staleness`
- `market_close_time`
- `data_ready_time`
- `data_vendor_delay_minutes`

## Rules

- Normal trading day: regular close 后再经过 vendor-ready buffer 才把当天视为 complete market
  date；buffer 内使用上一完整交易日。
- Weekend: requested date 使用上一交易日作为 market reference。
- U.S. market holiday: requested date 使用上一交易日作为 market reference。
- Partial trading day: scheduled early close 使用 early close time 和同一 vendor-ready buffer。
- Data vendor delay: close 后但 buffer 未结束时，不把当天缺失自动标记为 stale；仍要求 source 覆盖
  `freshness_reference_date`。

## Fail-Closed Boundary

Calendar adjustment 只能改变 `price_data` 和 `market_panel_data` 的 age reference。若 source
timestamp 早于 `freshness_reference_date`，evidence staleness monitor 仍按 policy threshold 输出
STALE 或 BLOCKING。Signal、stress、A/B、owner review、paper-shadow daily、drift 和 weekly review
继续按 requested as-of 的 research artifact timestamp 评估。

Unscheduled market closures 或供应商异常延迟不在 baseline calendar 内自动消化；需要作为 data-source
或 calendar investigation item 处理。
