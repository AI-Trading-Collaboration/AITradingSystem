# TRADING-366 Market Calendar Freshness Policy

最后更新：2026-06-16

## 背景

TRADING-354B 已把 `price_data` 和 `market_panel_data` 的 freshness age 从本地
requested calendar date 调整为 latest completed U.S. equity market date，避免
weekend、holiday 或美股未收盘时把真实已覆盖最新完整交易日的数据误判为 stale。

当前逻辑仍嵌在 evidence staleness monitor 内部，缺少独立 resolver 和明确的 partial
trading day / vendor delay 说明。TRADING-366 将该逻辑提升为共享 market calendar
freshness policy，保持 fail-closed：calendar policy 只能消除 false stale，不得隐藏真实
stale data。

## 目标

- 新增 market calendar resolver，统一输出 latest complete market date 和 freshness
  reference date。
- 覆盖 normal trading day、weekend、U.S. market holiday、partial trading day 和 data vendor
  delay window。
- Evidence staleness monitor 使用共享 resolver，而不是内置 ad hoc date math。
- Report/finding 继续输出 `latest_complete_market_date`、`freshness_reference_date`、
  `calendar_adjusted_staleness` 和 `stale_reason`，并补充 calendar rule fields 便于审计。
- 增加 weekend / holiday / normal trading day / partial day / vendor delay focused tests。
- 更新 README、operations runbook、system flow、artifact catalog、policy docs 和 task
  register。

## 非目标

- 不放宽 evidence staleness threshold。
- 不新增 stale waiver。
- 不刷新或补造 price cache、market panel 或 paper-shadow artifacts。
- 不引入 broker/order/production mutation。
- 不尝试覆盖 unscheduled market closures；此类事件必须作为 data-source/calendar
  investigation item 处理。

## Policy Contract

Market-data freshness 使用两个日期：

- `latest_complete_market_date`：按 U.S. equity calendar、market close time 和 vendor-ready
  buffer 计算出的最新完整交易日。
- `freshness_reference_date`：`requested_as_of` 与 `latest_complete_market_date` 的较早者。

Resolver 必须披露：

- `market_calendar_status`
- `market_calendar_reason`
- `market_session_kind`
- `market_close_time`
- `data_ready_time`
- `calendar_adjustment_reason`
- `calendar_adjusted_staleness`

当 `price_data` 或 `market_panel_data` 的 timestamp 覆盖 `freshness_reference_date` 时，不能仅
因为 requested as-of 落在 weekend、holiday、partial day 未 ready、或 vendor delay buffer 内而
标记 stale。若 source timestamp 早于 `freshness_reference_date`，仍必须按 policy threshold 产生
STALE/BLOCKING。

## 验收标准

- Shared resolver 可区分 normal trading day、weekend、U.S. holiday、partial trading day
  和 vendor delay。
- Evidence staleness monitor 使用 shared resolver，并在 report/finding 中保留 expected
  freshness output fields。
- Focused tests 覆盖 weekend、holiday、normal day、partial day、vendor delay 和真实 stale 不被
  calendar policy 隐藏。
- 文档和 task register 同步。
- Focused pytest、ruff、compileall、documentation contract、report index、Reader Brief
  validation 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 market calendar freshness policy 和
  evidence staleness monitor integration，不刷新数据、不放宽 threshold、不触发 production。
- 2026-06-16：实现完成并转 DONE；新增 `src/ai_trading_system/market_calendar_freshness.py`，
  `trading_calendar.py` 增加 scheduled partial trading day / early close metadata，
  evidence staleness monitor 改为读取 shared resolver。真实 monitor
  `evidence-staleness-monitor_ab047a679762dda0` 输出
  `freshness_reference_date=2026-06-12`、`latest_complete_market_date=2026-06-12`、
  `calendar_adjusted_staleness=true`、`calendar_adjustment_reason=requested_as_of_after_latest_complete_market_date`、
  `stale_artifacts=[]`、`blocking_artifacts=[]`、`missing_artifacts=[]`，price / market panel
  findings 均为 FRESH；weekly coverage 仍为 `RECOVERY_MODE_REVIEW`，因此
  `safe_to_continue_shadow=false` 且需要 manual review。Focused pytest 57 passed，Ruff PASS，
  compileall PASS，documentation contract PASS，report index `PASS_WITH_EXPLICIT_WAIVERS` /
  unwaived=0，Reader Brief OK，Reader Brief quality OK，git diff check PASS（仅 task register
  CRLF notice）。
