# 广义美元指数替换方案

## 背景

2026-05-05 的每日数据质量门禁发现 `DX-Y.NYB` 在 Yahoo Finance
主价格缓存中存在 OHLC 逻辑异常。`DX-Y.NYB` 原本用于表示 DXY / ICE U.S.
Dollar Index 风格的美元强弱代理，但它依赖 Yahoo 风格 ticker，且当前没有可
直接审计的官方授权数据源。

项目 owner 已确认不等待 ICE Currency Indices 商务流程，先将生产输入替换为
FRED / Federal Reserve 的 `DTWEXBGS`。

## 决策

- 用 `DTWEXBGS` 替换生产路径中的 `DX-Y.NYB`。
- 报告和配置语义使用“广义美元指数”或 `broad_usd_index`，不得继续把该输入
  称为 DXY。
- `DTWEXBGS` 走现有 FRED 下载、manifest、数据质量和特征构建链路。
- `DX-Y.NYB` 不再作为默认生产价格标的，也不再参与 Marketstack 第二行情源
  reconciliation。

## 数据源语义

- Provider：Federal Reserve Economic Data / FRED。
- Series：`DTWEXBGS`。
- 含义：Nominal Broad U.S. Dollar Index，广义贸易加权美元指数。
- 频率：Daily。
- 覆盖：从 2006-01-02 起，足以覆盖 `ai_after_chatgpt` 默认 regime。
- 边界：该指标不是 ICE DXY。任何报告、配置或 evidence bundle 不得把
  `DTWEXBGS` 表述为 official DXY。

## 实施步骤

1. 配置层：从 `macro.currency` 中移除 `DX-Y.NYB`，把 `DTWEXBGS` 加入
   `macro.rates`，并将宏观预算和评分规则的美元指标 subject 改为
   `DTWEXBGS`。
2. 特征层：FRED 宏观序列的利率变化特征限定在 DGS2/DGS10；指数水平的
   `return_<window>d` 用于 `DTWEXBGS`。
3. 数据质量：`validate-data` 应校验 `DTWEXBGS` 新鲜度、缺失、重复、数值范围
   和单日变化，但不能用利率百分比上下界或利率百分点日变化阈值错误约束美元指数水平。
4. 报告与审计：中文报告用“广义美元指数”，并保留 provider、series、row count
   和 checksum。
5. 文档：同步更新 `docs/system_flow.md`，说明美元宏观代理来自 FRED 而非 Yahoo。

## 验收标准

- `configured_price_tickers(load_universe())` 不再包含 `DX-Y.NYB`。
- `configured_rate_series(load_universe())` 包含 `DGS2`、`DGS10` 和 `DTWEXBGS`。
- `aits download-data` 会把 `DTWEXBGS` 写入 `data/raw/rates_daily.csv` 并记录
  manifest。
- `aits validate-data` 不再因 Yahoo `DX-Y.NYB` OHLC 异常阻断。
- `score-daily` 的宏观流动性评分和 `macro_risk_asset_budget` 使用
  `DTWEXBGS.return_20d`。
- 报告和文档不把 `DTWEXBGS` 误称为 DXY。

## 开放问题

- `^VIX` 仍在 Yahoo 主价格缓存中，后续需按 owner 讨论接入 Cboe VIX official
  historical data。
- 若未来需要交易市场熟悉的 official DXY，应另行接入 ICE 或有授权的 DXY 数据
  服务，并把它作为独立指标，而不是覆盖 `DTWEXBGS` 语义。

## 进展记录

- 2026-05-05：owner 确认采用 `DTWEXBGS` 替换 `DX-Y.NYB`，任务进入实现。
- 2026-05-05：实现完成并验证。`aits download-data` 写入 `DTWEXBGS` 2079 行，
  主价格缓存中 `DX-Y.NYB` 为 0 行；`aits validate-data` 对 `DTWEXBGS` 只保留
  2 条 2020 年历史大波动 warning，不再产生 error。当前 `score-daily` 仍因
  SPY/Yahoo OHLC、SOXX 异常和 Yahoo/Marketstack GOOG/SMH 口径差异等价格质量
  error 停止，已确认不属于 `DTWEXBGS` 替换阻塞。
