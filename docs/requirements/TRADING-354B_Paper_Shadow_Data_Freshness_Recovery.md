# TRADING-354B Paper Shadow Data Freshness Recovery

最后更新：2026-06-16

## 1. 背景

TRADING-354A 已生成 latest paper-shadow weekly review
`paper-shadow-weekly-review_f21aec8c5f94ea48`，并解除
`paper_shadow_weekly_review` missing blocker。随后真实 evidence staleness monitor
`evidence-staleness-monitor_9cb9357aade5728d` 仍输出
`safe_to_continue_shadow=false`，原因是 `price_data` 和 `market_panel_data` 被标记为
STALE。

核查后确认，本地价格缓存和 market panel 已覆盖 2026-06-12。该日期是 2026-06-16
日本时间上午运行时可用的 latest completed U.S. equity trading day；2026-06-16 作为
requested as-of 是本地日历口径，不代表美股 2026-06-16 已收盘。将 market data 按
requested calendar date 直接计算 age 会造成 false stale blocker。

## 2. 目标

1. 保留 TRADING-354 staleness monitor 的 fail-closed 行为。
2. 对 `price_data` 和 `market_panel_data` 使用 market-calendar-adjusted
   `freshness_reference_date`。
3. 在 manifest、report、Markdown、Reader Brief 和 CLI summary 中披露
   `requested_as_of`、`freshness_reference_date`、`latest_complete_market_date` 和
   `market_calendar_status`。
4. 在每个 finding 中披露 `stale_reason`，避免把 calendar adjustment 隐藏成隐性通过。
5. 用真实 data quality gate、market panel artifact 和 evidence staleness monitor run
   验证 stale blocker 被解除。

## 3. 非目标

- 不放宽 freshness threshold。
- 不增加 waiver。
- 不伪造 price cache、market panel、daily observation、drift monitor 或 weekly review。
- 不刷新外部 provider 数据，除非后续 owner 显式要求。
- 不生成 official target weights、order ticket、broker action 或 production mutation。

## 4. 设计决策

Market-data freshness 继续读取同一 policy YAML 中的 threshold，但 age 的 reference date
由 `freshness_reference_date` 提供：

- `requested_as_of`：operator 请求或 CLI 输入的业务日期。
- `latest_complete_market_date`：生成报告时按 U.S. equity market 日历可确认的最新完整交易日。
- `freshness_reference_date`：`requested_as_of` 与 `latest_complete_market_date` 的较早者。
- `market_calendar_status`：说明 requested date 是交易日、休市日，或被 latest completed
  market day 限制。

只有 `price_data` 和 `market_panel_data` 使用该 market-calendar-adjusted reference。Signal、
stress、A/B、owner review、paper-shadow daily、drift 和 weekly review 仍按 requested
as-of 的 research artifact freshness 口径评估。

## 5. 验收标准

- `validate-data --as-of 2026-06-12` 通过或仅有已披露 warning，errors=0。
- Latest valid market panel 覆盖 `latest_complete_market_date`。
- `evidence-staleness-monitor run --as-of 2026-06-16` 输出
  `freshness_reference_date=2026-06-12`、`latest_complete_market_date=2026-06-12`。
- `price_data` 和 `market_panel_data` 不再因为 local calendar date ahead of market close
  被误判为 STALE。
- `safe_to_continue_shadow=true` 仅在 missing/blocking/stale artifacts 均为空且 status 为
  FRESH/ACCEPTABLE 时出现。
- README、operations runbook、system flow、artifact catalog、policy、requirements 和 task
  register 同步更新。
- focused pytest、ruff、compileall、documentation contract、report index、Reader Brief
  quality 和 git diff check 通过。

## 6. 进展记录

- 2026-06-16：新增并进入 IN_PROGRESS；root cause 是 requested as-of 使用本地 2026-06-16，
  但运行时 latest completed U.S. equity trading day 仍为 2026-06-12。最佳解法是让
  market-data freshness 使用显式 market calendar reference，不接受 waiver 或 threshold
  放宽。
- 2026-06-16：实现完成并转 DONE；`validate-data --as-of 2026-06-12` 返回
  `PASS_WITH_WARNINGS` / errors=0 / warnings=1，`reports market-panel --as-of 2026-06-12`
  返回 `PASS_WITH_WARNINGS` / available=6/6。真实 monitor
  `evidence-staleness-monitor_605356e7ccc2baf0` 输出
  `evidence_freshness_status=ACCEPTABLE`、`requested_as_of=2026-06-16`、
  `freshness_reference_date=2026-06-12`、`latest_complete_market_date=2026-06-12`、
  `market_calendar_status=TRADING_DAY`、`stale_artifacts=[]`、`blocking_artifacts=[]`、
  `missing_artifacts=[]`、`safe_to_continue_shadow=true`；price / market panel findings 均为
  FRESH，`stale_reason=calendar_adjusted_to_latest_complete_market_date`。`report --latest`
  和 `validate-evidence-staleness-monitor --monitor-id
  evidence-staleness-monitor_605356e7ccc2baf0` 均 PASS。Focused pytest 21 passed，Ruff
  PASS，documentation contract PASS，report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0，
  Reader Brief quality OK，compileall PASS，git diff check PASS。保持 no waiver / no threshold
  relaxation / no data fabrication / no official target / no broker / no production mutation。
