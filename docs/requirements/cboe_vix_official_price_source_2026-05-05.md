# Cboe VIX 官方历史数据接入

## 背景

FMP Starter 已作为默认股票/ETF 日线价格主源，但 FMP 不覆盖内部价格标的
`^VIX`。`aits validate-data` 因主价格缓存缺少 `^VIX` 阻断，导致
`score-daily` 无法继续执行风险情绪评分、宏观风险资产预算和日报结论。

Cboe 官方 VIX historical data 页面提供 1990 至今、每日更新的 VIX Index
历史 CSV：

- 页面：`https://www.cboe.com/tradable_products/vix/vix_historical_data`
- CSV：`https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`
- 字段：`DATE,OPEN,HIGH,LOW,CLOSE`

## 决策

接入 Cboe VIX official historical data，作为内部 ticker `^VIX` 的专用主源。
FMP 继续负责股票/ETF 主价格；Cboe 只补 `^VIX`，两者合并写入
`data/raw/prices_daily.csv`。Marketstack 保持第二行情源，但继续不要求覆盖
`^VIX`。

## 实现要求

- `aits download-data` 默认下载 FMP 股票/ETF 主价格后，如果配置的价格标的包含
  `^VIX`，必须从 Cboe CSV 下载对应日期范围并追加到主价格缓存。
- Cboe 记录标准化为现有价格 schema：
  `date,ticker,open,high,low,close,adj_close,volume`。
- `adj_close` 等于 `close`；`volume` 为空，因为 VIX Index 不是可交易股票成交量。
- Cboe 下载失败、CSV schema 异常、目标日期范围内无记录时必须失败，不得用 FMP、
  Yahoo、Marketstack 或历史本地缓存补写。
- `download_manifest.csv` 必须为 Cboe 单独追加 source_id、provider、endpoint、
  request parameters、row count、checksum；不得记录任何密钥。
- `config/data_sources.yaml` 必须登记 Cboe 来源、授权边界、缓存路径和限制。
- `docs/system_flow.md` 必须显示 FMP + Cboe + Marketstack + FRED 的数据流。

## 验收

- 单元测试覆盖 Cboe CSV 标准化、下载失败、主缓存合并和 manifest 记录。
- `aits data-sources validate` 通过。
- `aits download-data` 写入的 `prices_daily.csv` 包含 `^VIX`。
- 若 FMP 自身仍有 SPY/SOXX/OHLC 异常，`validate-data` 可以继续失败，但失败原因中
  不应再出现 `prices_missing_expected_values: ^VIX`。

## 状态记录

- 2026-05-05：新增并进入实现。原因：DATA-007 已接入 FMP 主价格源，但 `^VIX`
  缺口仍阻断质量门禁；owner 要求后续接入 VIX。
- 2026-05-05：baseline 实现完成。新增 `CboeVixPriceProvider`，`download_daily_data`
  在主源缺少 `^VIX` 时从 Cboe CSV 补入主价格缓存，并为 `cboe_vix_daily_prices`
  写入 manifest。聚焦测试 102 passed，Ruff 通过，`aits data-sources validate` 通过；
  live smoke 验证 2026-04-30 至 2026-05-01 返回 2 行 `^VIX`。
