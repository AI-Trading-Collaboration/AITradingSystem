# AI Trading System

面向美股 AI 产业链的趋势分析、风险评分、回测与仓位建议系统。

项目目标不是预测市场，也不是自动交易，而是把“是否加仓、减仓、观望”变成可复盘、可验证、可控制风险的决策流程。

产品定位详见 [docs/product_strategy.md](docs/product_strategy.md)：系统应服务于能力圈、产业链因果、仓位决策和复盘归因，而不是扩张成全市场万能分析器。工程落地拆解见 [docs/implementation_backlog.md](docs/implementation_backlog.md)。

## MVP 范围

第一版只做闭环：

1. 市场价格与宏观风险数据采集。
2. 趋势、相对强弱、波动率、利率等特征计算。
3. 规则评分模型。
4. 仓位区间建议。
5. 与 QQQ、SMH/SOXX、SPY 的回测对比。
6. 每日 Markdown 报告。

SEC 基本面已经接入基础硬数据评分；估值、新闻/NLP、LLM 事件抽取继续放到后续阶段。

## 工程结构

```text
AGENTS.md                项目工程协作守则
config/                  投资标的池、模块权重、运行参数
config/watchlist.yaml    观察池和能力圈配置
config/industry_chain.yaml 产业链节点和因果图配置
config/market_regimes.yaml 市场阶段和默认回测区间配置
config/risk_events.yaml  风险事件等级和动作规则配置
config/data_sources.yaml 数据源目录、审计字段和来源限制
config/sec_companies.yaml SEC companyfacts CIK 映射
config/fundamental_metrics.yaml SEC 基本面指标映射
config/fundamental_features.yaml SEC 基本面特征公式
data/raw/                原始数据缓存，不提交
data/processed/          清洗后的中间数据，不提交
data/external/           外部导入数据，不提交
docs/                    架构和开发计划
docs/system_flow.md      数据输入、中间评估和输出结论示意图
docs/product_strategy.md 产品策略和模块原则
docs/implementation_backlog.md 可落地模块和工程 backlog
docs/examples/           可复制的输入模板，不包含个人交易记录
notebooks/               研究和临时分析
outputs/backtests/       回测输出，不提交
outputs/reports/         日报/周报输出，不提交
src/ai_trading_system/   应用代码
tests/                   单元测试
```

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,data,dashboard]"
python -m pytest
```

下载阶段 1 所需的日线数据：

```powershell
aits download-data --start 2018-01-01
```

默认会缓存核心范围：`SPY`、`QQQ`、`SMH`、`SOXX`、防守 ETF、`^VIX`、美元指数、`MSFT`、`GOOG`、`TSM`、`INTC`、`AMD`、`NVDA`，以及 FRED 的 `DGS2`、`DGS10`。每次下载会追加写入 `data/raw/download_manifest.csv`，记录 provider、endpoint、请求参数、下载时间、行数、输出路径和 sha256。如需抓取配置里的完整 AI 产业链标的：

```powershell
aits download-data --start 2018-01-01 --full-universe
```

校验本地数据缓存并生成质量报告：

```powershell
aits validate-data
```

质量报告默认写入 `outputs/reports/data_quality_YYYY-MM-DD.md`。如果校验出现错误，命令会返回非零退出码，后续评分和回测流程不应继续使用这批数据。

查看和校验数据源目录：

```powershell
aits data-sources list
aits data-sources validate --as-of 2026-05-02
```

数据源目录在 `config/data_sources.yaml`。它记录当前 Yahoo Finance、FRED、本地手工输入和计划接入来源的 provider、endpoint、缓存路径、审计字段、校验项和限制说明。这个命令不下载数据，只校验“来源是否可审计、限制是否明确”，用于后续接入财报、估值和新闻事件源前的来源纪律。

构建每日市场特征：

```powershell
aits build-features --as-of 2026-05-01
```

命令会先执行数据质量门禁，失败时停止。特征默认写入 `data/processed/features_daily.csv`，报告默认写入 `outputs/reports/feature_summary_YYYY-MM-DD.md`。

生成每日市场评分报告：

```powershell
aits score-daily --as-of 2026-05-01
```

命令会先执行市场数据质量门禁，再构建市场特征，并校验 `data/processed/sec_fundamentals_YYYY-MM-DD.csv`、生成 SEC 基本面特征，最后输出 `data/processed/scores_daily.csv` 和 `outputs/reports/daily_score_YYYY-MM-DD.md`。日报会同时汇总交易 thesis、风险事件、估值快照和交易复盘的复核状态；缺少本地手工输入会显示为警告，配置或 YAML 错误会显示为复核失败。SEC 基本面特征通过校验后会进入基本面硬数据评分；估值自动评分和政策地缘仍会在报告中明确标记为占位输入。

运行历史回测：

```powershell
aits backtest --to 2026-05-02 --quality-as-of 2026-05-02
```

回测命令会先执行市场数据质量门禁和 SEC companyfacts 缓存校验。默认市场阶段来自 `config/market_regimes.yaml`，当前为 `ai_after_chatgpt`，起点是 `2022-12-01`，即 ChatGPT 于 `2022-11-30` 公开发布后的首个完整美股交易日。当前基础版使用每日评分得到的 AI 仓位区间中点作为目标仓位，以 `SMH` 作为默认 AI 代理标的，并与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。每个 signal_date 会按 `filed_date <= signal_date` 生成 point-in-time SEC 基本面特征，并在回测报告中声明 SEC 基本面质量摘要；信号按收盘后生成、下一交易日生效，避免未来函数。

如需把 2019 年以来的历史作为非默认压力测试，可以显式指定：

```powershell
aits backtest --regime cross_cycle_stress --to 2026-05-02 --quality-as-of 2026-05-02
```

查看示例评分：

```powershell
aits score-example
```

示例报告会同时输出两个仓位口径：

- AI 仓位（股票风险资产内）。
- AI 仓位（总资产内），根据 `config/portfolio.yaml` 的风险资产预算换算。

查看和校验观察池能力圈配置：

```powershell
aits watchlist list
aits watchlist validate --as-of 2026-05-02
```

观察池校验会检查核心个股是否都在活跃观察池中、是否映射到 AI 产业链节点，以及高风险标的是否要求交易 thesis。

查看和校验产业链因果图：

```powershell
aits industry-chain list
aits industry-chain validate --as-of 2026-05-02
```

产业链校验会检查节点是否重复、父节点是否存在、因果图是否有环、节点是否配置领先指标和相关标的，以及观察池引用的产业链节点是否存在。

校验和复核交易 thesis：

```powershell
aits thesis validate --as-of 2026-05-02
aits thesis review --as-of 2026-05-02
```

交易 thesis 默认读取 `data/external/trade_theses/*.yaml`，该目录不提交。可参考 `docs/examples/trade_theses/nvda_ai_infra_template.yaml` 复制模板。校验会检查 schema、ticker 是否在观察池、产业链节点是否存在、验证指标和证伪条件是否完整；复核报告会输出原始假设是否仍成立、是否需要人工复核、是否已有证伪条件触发。

查看和校验风险事件分级规则：

```powershell
aits risk-events list
aits risk-events validate --as-of 2026-05-02
```

风险事件配置在 `config/risk_events.yaml`。当前基础版定义 L1/L2/L3 风险等级、AI 仓位折扣乘数、人工复核要求、影响产业链节点、相关标的、建议动作、升级条件和解除条件。风险事件不直接触发交易，只进入风险评估、thesis 复核和人工审核。

校验和复核估值、预期与拥挤度快照：

```powershell
aits valuation validate --as-of 2026-05-02
aits valuation review --as-of 2026-05-02
```

估值快照默认读取 `data/external/valuation_snapshots/*.yaml`，该目录不提交。可参考 `docs/examples/valuation_snapshots/nvda_valuation_template.yaml` 复制模板。当前基础版要求估值和预期数据带有来源、日期、采集时间和字段说明；公开便利源只能作为辅助，不能直接进入自动评分。

下载 SEC companyfacts 原始基本面数据：

```powershell
aits fundamentals list-sec-companies
$env:SEC_USER_AGENT="AITradingSystem wakare_no_kaze@outlook.com"
aits fundamentals download-sec-companyfacts --tickers NVDA,MSFT
aits fundamentals validate-sec-companyfacts --as-of 2026-05-02
aits fundamentals extract-sec-metrics --as-of 2026-05-02
aits fundamentals validate-sec-metrics --as-of 2026-05-02
aits fundamentals build-sec-features --as-of 2026-05-02
```

该命令读取 `config/sec_companies.yaml` 的 ticker/CIK 映射，下载 SEC EDGAR companyfacts JSON 到 `data/raw/sec_companyfacts/`，并追加写入 `sec_companyfacts_manifest.csv`。校验命令会检查 JSON、CIK、taxonomy 和 checksum。`extract-sec-metrics` 会先执行同一条 SEC 缓存质量门禁，通过后按 `config/fundamental_metrics.yaml` 抽取收入、毛利、营业利润、净利润、研发和 CapEx 等指标，默认输出 `data/processed/sec_fundamentals_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamentals_YYYY-MM-DD.md`。显式派生指标只允许使用配置声明的组件，例如 `gross_profit = revenue - cost_of_revenue`，且必须满足周期、单位、截止日、财年、财期和 accession number 一致。`config/sec_companies.yaml` 可以声明单家公司在 SEC companyfacts 路径可用的指标周期；当前 TSM 在该路径只要求年度指标，季度指标需后续接入 TSM 官方 IR 等可审计来源。`validate-sec-metrics` 会校验抽取后 CSV 的 schema、重复键、未来披露日期、数值合法性和配置覆盖率。`build-sec-features` 会先复用同一条 SEC 指标 CSV 校验门禁，通过后按 `config/fundamental_features.yaml` 生成毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度，默认输出 `data/processed/sec_fundamental_features_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`。`score-daily` 会复用同一条 SEC 指标校验和特征构建路径，校验失败时停止日报评分；通过后按 `config/scoring_rules.yaml` 的 `fundamentals` 规则使用 AI 核心观察池 SEC 特征中位数进行基本面硬数据评分。

复盘交易记录并做基础归因：

```powershell
aits review-trades --as-of 2026-05-02
```

交易记录默认读取 `data/external/trades/*.yaml`，该目录不提交。可参考 `docs/examples/trades/nvda_trade_template.yaml` 复制模板。该命令依赖缓存行情数据，会先执行数据质量门禁，再将交易收益与 `SPY`、`QQQ`、`SMH`、`SOXX` 同区间收益对比，辅助区分市场 Beta、AI 主题 Beta 和个股表现。

## 投资边界

系统输出只作为个人研究和仓位管理辅助，不构成投资建议。所有策略都需要回测、复盘，并显式考虑税费、滑点、汇率、交易延迟和极端风险。
