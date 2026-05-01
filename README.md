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

基本面、估值、新闻/NLP、LLM 事件抽取放到后续阶段。

## 工程结构

```text
AGENTS.md                项目工程协作守则
config/                  投资标的池、模块权重、运行参数
config/watchlist.yaml    观察池和能力圈配置
config/industry_chain.yaml 产业链节点和因果图配置
data/raw/                原始数据缓存，不提交
data/processed/          清洗后的中间数据，不提交
data/external/           外部导入数据，不提交
docs/                    架构和开发计划
docs/product_strategy.md 产品策略和模块原则
docs/implementation_backlog.md 可落地模块和工程 backlog
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

默认会缓存核心范围：`SPY`、`QQQ`、`SMH`、`SOXX`、防守 ETF、`^VIX`、美元指数、`MSFT`、`GOOG`、`TSM`、`INTC`、`AMD`、`NVDA`，以及 FRED 的 `DGS2`、`DGS10`。如需抓取配置里的完整 AI 产业链标的：

```powershell
aits download-data --start 2018-01-01 --full-universe
```

校验本地数据缓存并生成质量报告：

```powershell
aits validate-data
```

质量报告默认写入 `outputs/reports/data_quality_YYYY-MM-DD.md`。如果校验出现错误，命令会返回非零退出码，后续评分和回测流程不应继续使用这批数据。

构建每日市场特征：

```powershell
aits build-features --as-of 2026-05-01
```

命令会先执行数据质量门禁，失败时停止。特征默认写入 `data/processed/features_daily.csv`，报告默认写入 `outputs/reports/feature_summary_YYYY-MM-DD.md`。

生成每日市场评分报告：

```powershell
aits score-daily --as-of 2026-05-01
```

命令会先执行数据质量门禁，再构建特征，最后输出 `data/processed/scores_daily.csv` 和 `outputs/reports/daily_score_YYYY-MM-DD.md`。MVP 阶段的基本面、估值、政策地缘会在报告中明确标记为占位输入。

运行历史回测：

```powershell
aits backtest --from 2019-01-01 --to 2026-05-02 --quality-as-of 2026-05-02
```

回测命令会先执行数据质量门禁。当前基础版使用每日评分得到的 AI 仓位区间中点作为目标仓位，以 `SMH` 作为默认 AI 代理标的，并与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。信号按收盘后生成、下一交易日生效，避免未来函数。

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

## 投资边界

系统输出只作为个人研究和仓位管理辅助，不构成投资建议。所有策略都需要回测、复盘，并显式考虑税费、滑点、汇率、交易延迟和极端风险。
