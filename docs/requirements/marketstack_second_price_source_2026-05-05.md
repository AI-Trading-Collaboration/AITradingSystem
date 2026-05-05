# DATA-005 Marketstack 行情第二来源接入

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`DATA-002`、`DATA-005`

## 背景

项目 owner 已订阅 Marketstack Basic，并已在本机环境变量中配置
`MARKETSTACK_API_KEY`。该订阅用于补齐 `market_prices` 的第二个
qualified source，降低 Yahoo Finance 单一公开便利源错误对趋势判断、特征、
回测和日报解释的影响。

当前目标不是替换主价格缓存，而是新增独立第二来源缓存和质量核验：

- 主价格缓存继续使用 `data/raw/prices_daily.csv`。
- Marketstack 写入独立缓存 `data/raw/prices_marketstack_daily.csv`。
- 下载审计继续写入统一 `data/raw/download_manifest.csv`。
- `aits validate-data` 在可审计第二来源存在时比较主价格与 Marketstack 的未调整收盘价，并报告 adjusted close 口径差异。
- 下游 `build-features`、`score-daily`、`backtest` 等仍使用主价格缓存，但必须看到数据质量报告中的第二来源状态。

## 设计边界

- Marketstack 只作为股票和 ETF 日线第二来源，不伪装成 VIX、DXY 或宏观利率的一手来源。
- `^VIX` 等 Marketstack Basic 不覆盖的 Yahoo 风格符号必须显式排除或记录限制，不得生成伪数据。
- 真实烟测显示 Marketstack 对部分 ETF 的 `adj_close` 与 raw `close` 相同，而 Yahoo `adj_close` 包含分红复权；质量门禁必须把 raw close 价差作为阻塞依据，并把 adjusted close 分红口径差异显式报告为限制。
- 价格差异进入质量门禁调查项；系统不得自动平滑、覆盖或择优选择供应商价格。
- API key 只从 `MARKETSTACK_API_KEY` 环境变量读取，不写入报告、manifest 或错误输出。
- 下载记录必须包含 provider、endpoint、请求参数、下载时间、row count、输出路径和 checksum。

## 实施步骤

1. 数据源登记：更新 `docs/task_register.md`、`config/data_sources.yaml`、`.env.example` 和本需求文档。
2. 下载接入：新增 Marketstack provider，标准化为 `date,ticker,open,high,low,close,adj_close,volume`，写入独立缓存和 manifest。
3. 质量门禁：扩展 `validate_data_cache`，校验 Marketstack 缓存 schema、覆盖、新鲜度、重复键、异常值，并比较主缓存与第二来源 raw `close` 和 adjusted close 口径差异。
4. CLI 串联：`aits download-data` 默认要求 `MARKETSTACK_API_KEY` 并抓取第二来源；`aits validate-data` 输出第二来源状态。
5. 文档和流图：更新 `README.md`、`docs/system_flow.md` 和相关说明。
6. 验证：补单元测试、CLI 测试、数据源目录测试，并运行 ruff / pytest。

## 验收标准

- `aits download-data` 能在不泄露 key 的前提下写入 `prices_marketstack_daily.csv` 和 manifest 记录。
- Marketstack 缓存至少覆盖核心股票/ETF 观察池，无法覆盖的宏观代理有明确限制说明。
- `aits validate-data` 报告显示第二来源价格文件、覆盖状态、重叠样本数量、raw close 价差异常和 adjusted close 口径差异；严重缺失或 raw close 超阈值价差能停止下游。
- `data_sources health` 中 `market_prices` 在 Yahoo + Marketstack 低成本组合下显示 `COVERED_BASELINE`，并不再因市场价格第二来源缺失而显示 `NOT_COVERED`。
- 报告和文档声明第二来源只做 cross-provider reconciliation，不改变主价格口径或自动修正价格。

## 状态记录

- 2026-05-05：新增并进入实现，原因：owner 已订阅 Marketstack Basic 并完成 key 配置，请求正式接入项目并按 TODO 顺序开发未阻塞工作项。
- 2026-05-05：从 IN_PROGRESS 改为 BASELINE_DONE，原因：已完成 Marketstack provider、独立缓存、download manifest、质量门禁、数据源目录、系统流图、README 和测试；真实烟测显示下载成功且 `validate-data` 为 `PASS_WITH_WARNINGS`，警告来自 ETF adjusted close 分红复权口径差异，raw close 核验通过。
