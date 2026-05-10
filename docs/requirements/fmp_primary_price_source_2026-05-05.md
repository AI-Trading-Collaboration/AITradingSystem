# FMP 主价格源接入方案

最后更新：2026-05-05

## 背景

2026-05-05 的数据质量门禁显示 Yahoo Finance 主价格缓存存在 SPY 非正
close/adj_close、OHLC 逻辑错误、SOXX 极端复权价格波动，以及与 Marketstack
第二行情源在 GOOG、SMH 等标的上的大规模 split / adjustment 口径差异。

项目 owner 已购买 FMP Starter plan，并确认后续接入 FMP 作为价格主源。

## 决策

- FMP / Financial Modeling Prep 作为默认股票和 ETF 日线价格主源。
- `data/raw/prices_daily.csv` 继续作为标准化主价格缓存，不改变下游路径。
- Marketstack 继续作为 `data/raw/prices_marketstack_daily.csv` 第二行情源，用于
  cross-provider reconciliation。
- Yahoo Finance 不再作为默认 `download-data` 主源；保留为显式可选 provider，
  用于迁移调查或临时手工比较，不进入默认生产路径。
- FMP API key 只从 `FMP_API_KEY` 环境变量读取，不写入报告、manifest 或错误输出。

## 数据源语义

- Provider：Financial Modeling Prep。
- Endpoints：`https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted`
  和 `https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted`。
- 参数：`symbol`、`from`、`to`、`apikey`。
- 标准化字段：`date`、`ticker`、`open`、`high`、`low`、`close`、`adj_close`、
  `volume`。
- raw OHLC/volume 使用 FMP `non-split-adjusted` endpoint；`adj_close` 使用 FMP
  `dividend-adjusted` endpoint 的 `adjClose`。若 `adjClose` 缺失则必须显式失败，
  不能静默用 `close` 伪装复权价格。
- 价格路径直接按内部 ticker 请求 FMP；`GOOG` 不再映射为 `GOOGL`，避免把
  Alphabet C share 与 A share 混在一起。估值、分析师预期和 forward-only PIT
  路径的 provider alias 独立维护。
- `^VIX` 不通过 FMP 主价格源抓取；由 Cboe VIX official historical data
  单独接入并合并进主价格缓存。
- `DTWEXBGS` 继续通过 FRED 宏观序列路径，不进入价格缓存。

## 实施步骤

1. 新增 `FmpPriceProvider`，逐 ticker 调用 FMP EOD non-split-adjusted 和 dividend-adjusted
   endpoints，校验响应 schema、空结果、HTTP error 和 provider error。
2. `aits download-data` 默认使用 FMP 主源，要求 `FMP_API_KEY`；保留
   `--price-provider yahoo` 作为显式迁移选项。
3. 由于 FMP Starter 当前是 US coverage，默认 FMP 请求应排除 `^VIX` 等非股票/ETF
   宏观代理；`^VIX` 由 `cboe_vix_daily_prices` 单独补入主价格缓存。
4. 下载 manifest 记录 FMP provider、endpoint、内部 ticker 列表、provider symbol
   alias、start/end、row count 和 checksum，但不记录 API key。
5. 数据源目录、README 和 `docs/system_flow.md` 同步更新。

## 验收标准

- `aits download-data` 默认读取 `FMP_API_KEY` 并写入 `prices_daily.csv`。
- 下载 manifest 的主价格 `source_id` 为 `fmp_eod_daily_prices`。
- `prices_daily.csv` 不包含 Yahoo `DX-Y.NYB`，并从 FMP 标准化 `adjClose`。
- Marketstack 第二源仍写入独立缓存并参与质量门禁。
- 若 `FMP_API_KEY` 缺失，命令 fail closed，不回退到 Yahoo。
- 测试覆盖 FMP response normalization、价格 alias 排除、CLI 默认主源参数和
  manifest source id。

## 开放问题

- FMP Starter 的 5 年历史足够覆盖当前 `ai_after_chatgpt` 默认 regime 和 200 日
  warm-up，但不够更长历史压力测试；若需要 2018 起完整长期重建，应升级或另接
  更长历史的供应商。
- `^VIX` 已由 Cboe official historical data 接入；FMP 主源切换仍不能解释为
  全数据质量门禁完成，因为 Marketstack 第二源自检和跨源复权口径仍需真实样本验收。
- FMP 与 Marketstack 的 ETF adjusted close 分红复权口径仍需通过真实样本验收；
  Marketstack 第二源自身硬错误需单独归因，不能写成主源错误。

## 进展记录

- 2026-05-05：owner 确认已购买 FMP Starter plan，并要求接入 FMP 作为价格主源。
- 2026-05-05：完成 baseline 接入。`aits download-data` 默认使用 FMP 主价格源，
  写入 `prices_daily.csv` 25140 行；manifest 记录 `fmp_eod_daily_prices`、
  `fred_daily_rates` 和 `marketstack_eod_daily_prices`，不记录 API key。实现中发现
  FMP `full` endpoint 返回 split-adjusted close，不能作为 raw close 与 Marketstack
  reconciliation 对齐，因此改为 `non-split-adjusted` raw OHLC + `dividend-adjusted`
  `adjClose`。
- 2026-05-05：真实 `aits validate-data` 仍为 FAIL，错误 6、警告 8。剩余错误包括：
  `^VIX` 缺失，SPY 2026-04-07/2026-04-08 非正 close/adj_close，INTC/MSFT/SPY
  OHLC 逻辑异常，SOXX 2024-03-07 极端 adjusted close 波动，以及 GOOG 少量
  FMP/Marketstack adjusted close mismatch。`aits score-daily` 已按数据质量门禁停止；
  不能用本地补写、回填或平滑绕过。
- 2026-05-05：后续 DATA-008 已接入 Cboe VIX official historical data。`^VIX`
  缺失不应再作为 FMP 主源自身 blocker；剩余生产阻断需要按 DATA-009 区分
  FMP 主源、Marketstack 第二源自检和跨源口径差异。
- 2026-05-05：DATA-009 复核后拆分价格 alias 与估值/PIT alias。FMP 价格路径默认
  直接请求 `GOOG`；此前 SPY/INTC/MSFT/SOXX 的硬错误主要落在 Marketstack 第二源
  缓存，后续质量报告需用“来源”列区分主源、第二源和跨源核验。
