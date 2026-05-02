# 开发计划

## 产品定义

开发一个辅助个人进行美股 AI 产业链仓位管理的投资决策系统。系统要回答：

0. 这笔交易是否处在能力圈内。
1. 当前 AI 产业链适合重仓、中仓、轻仓还是防守。
2. 风险主要来自趋势、利率、波动、估值、基本面还是政策事件。
3. 如果减仓，应该减多少；如果恢复，什么条件下加回。
4. 策略长期是否优于 QQQ、SMH/SOXX、SPY，并且最大回撤是否可接受。
5. 本次收益或亏损来自市场 Beta、行业/主题 Beta、个股 Alpha、仓位错误还是运气。

产品策略和模块原则见 `docs/product_strategy.md`。可落地模块、数据对象和命令拆解见 `docs/implementation_backlog.md`。

默认市场阶段为 `ai_after_chatgpt`：锚定事件是 `2022-11-30` ChatGPT 公开发布，默认回测起点是 `2022-12-01`，即其后的首个完整美股交易日。2019 年以来的数据保留为跨周期压力测试和长窗口 warm-up，不作为 AI 主线行情的默认结论窗口。

## 基础工程规则

项目根目录的 `AGENTS.md` 是本项目的工程协作守则，后续开发必须遵守。核心原则：

- 遇到问题不能默认用临时方案绕过去。
- 必须先分析最优方案为什么不可行或暂时受阻。
- 如果确实需要临时方案，必须先沟通原因、风险、影响和退出条件。
- 数据来源、数据质量、回测和评分链路不能接受静默降级。
- 新增正式数据源前，必须先进入 `config/data_sources.yaml` 并通过 `aits data-sources validate`。

## 阶段 0：工程初始化

状态：已完成初始骨架。

交付物：

- Python 项目结构。
- Git 基础配置文件。
- 标的池配置。
- 评分模型骨架。
- 架构与开发计划文档。
- 基础测试和 CI 配置。

## 阶段 1：MVP 投资驾驶舱

目标周期：1-2 周。

范围：

- 拉取价格、VIX、利率数据。
- 计算均线、相对强弱、波动和利率变化特征。
- 实现趋势、宏观流动性、风险情绪评分。
- 基本面、估值、政策地缘先支持手工输入或中性默认值。
- 输出当前 AI 产业链仓位建议。
- 跟踪核心个股观察池：MSFT、GOOG、TSM、INTC、AMD、NVDA。
- 回测 QQQ、SMH/SOXX、SPY 作为基准。
- 生成 Markdown 日报。
- 建立数据源目录，记录当前来源、审计字段、校验项和限制说明。

边界：

- 阶段 1 主要解决市场环境和趋势风险，不假装已经完成基本面、估值、产业链因果和复盘归因。
- 日报中应显式标注哪些评分来自硬数据，哪些仍是中性占位或手工输入。

验收标准：

- 一条命令能生成最新评分报告。
- 一条命令能跑历史回测。
- 报告能解释本期相对上期为什么加仓、减仓或不动。
- 报告同时输出股票风险资产内 AI 仓位和总资产内 AI 仓位。
- 默认回测覆盖 ChatGPT 之后的 AI 主线行情；跨周期压力测试可以额外覆盖 2019、2020、2022 这类不同市场环境，但必须在报告中标注为非默认市场阶段。

## 阶段 2：基本面与估值增强

目标周期：1-2 个月。

范围：

- 建立持仓与观察池管理，记录能力圈、买入理由、目标周期和风险等级。
- 建立产业链节点模型，覆盖云厂商 CapEx、GPU/ASIC、HBM、先进封装、晶圆代工、设备材料。
- 建立财报数据结构。状态：已实现 SEC companyfacts 原始 JSON 下载、审计清单、缓存校验、基础指标抽取、特征构建和日报基本面评分接入。
- 跟踪收入增速、毛利率、EPS 预期、CapEx、数据中心收入。
- 跟踪 Forward P/E、PEG、EV/Sales 和历史分位。
- 跟踪拥挤度和预期过热信号。
- 支持公司和产业链层级的评分。
- 增加个股/ETF 权重建议。

验收标准：

- 财报季可以录入或导入关键指标。
- 信息可以映射到产业链节点，并说明影响的是短期情绪还是长期现金流预期。
- 系统能区分“价格回调但基本面仍强”和“趋势坏且基本面下修”。
- 仓位建议能说明基本面和估值贡献。

## 阶段 3：新闻、政策与 LLM 结构化

目标周期：长期迭代。

范围：

- 新闻事件分类。
- 风险事件 L1/L2/L3 分级。
- 地缘与政策风险分级。
- 财报电话会摘要。
- LLM 辅助抽取事件、指引和风险。
- 建立事件回测和人工复核流程。
- 建立交易 thesis 的假设验证和证伪追踪。
- 建立复盘归因模块，区分市场 Beta、行业/主题 Beta、个股 Alpha、仓位错误和纪律问题。

验收标准：

- 新闻事件不直接触发交易，只改变风险评分或人工审核状态。
- 每条高影响事件都能追溯来源、时间和判断依据。
- 历史事件库能用于复盘，不引入未来函数。
- 每笔重要交易可以复盘原始假设是否成立，以及收益/亏损来源。

## 核心模块优先级

|优先级|模块|开发含义|
|---|---|---|
|P0|持仓与观察池管理|记录标的、能力圈、仓位、成本、thesis、周期和风险等级|
|P1|产业链信息监控|把信息映射到云 CapEx、GPU/ASIC、HBM、先进封装、代工、设备材料等节点|
|P2|市场环境|跟踪利率、美元、指数趋势、VIX、半导体相对强弱|
|P3|估值与预期|跟踪估值分位、增长预期、拥挤度和预期变化|
|P4|仓位决策|把产业、估值、市场、事件风险、技术面转成仓位区间|
|P5|复盘归因|复盘 Beta、主题趋势、Alpha、仓位和纪律问题|

## 近期迭代建议

当前最小落地路径见 `docs/implementation_backlog.md`。第一个开发迭代建议先做这些闭环事项：

1. 实现行情数据下载和本地缓存。状态：已实现基础版，命令为 `aits download-data`，并写入 `download_manifest.csv` 审计清单。
2. 实现数据质量门禁。状态：已实现基础版，命令为 `aits validate-data`。
3. 实现技术特征计算。状态：已实现基础版，命令为 `aits build-features`。
4. 实现 100 分评分到仓位区间的规则。状态：已实现基础版，命令为 `aits score-daily`。
5. 实现观察池与能力圈配置。状态：已实现基础版，命令为 `aits watchlist list/validate`。
6. 实现简单回测引擎。状态：已实现基础版，命令为 `aits backtest`，包含 signal_date 级 point-in-time SEC 基本面特征。
7. 实现产业链节点与因果图配置。状态：已实现基础版，命令为 `aits industry-chain list/validate`。
8. 输出一份日报 Markdown。状态：已实现基础版，命令为 `aits score-daily`。
9. 实现交易 thesis 与假设验证。状态：已实现基础版，命令为 `aits thesis list/validate/review`。
10. 实现风险事件分级规则。状态：已实现基础版，命令为 `aits risk-events list/validate`。
11. 实现估值与拥挤度快照。状态：已实现基础版，命令为 `aits valuation list/validate/review`。
12. 实现交易复盘归因。状态：已实现基础版，命令为 `aits review-trades`。
13. 实现 SEC companyfacts 原始基本面数据下载和缓存校验。状态：已实现基础版，命令为 `aits fundamentals download-sec-companyfacts` 和 `aits fundamentals validate-sec-companyfacts`。
14. 实现 SEC companyfacts 基础指标抽取和派生指标校验。状态：已实现基础版，命令为 `aits fundamentals extract-sec-metrics` 和 `aits fundamentals validate-sec-metrics`，输出结构化 CSV 和中文 Markdown 报告。
15. 实现 SEC 基本面比率特征。状态：已实现基础版，命令为 `aits fundamentals build-sec-features`，先复用 SEC 指标 CSV 校验门禁，再输出毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度。
16. 接入 SEC 基本面硬数据评分。状态：已实现基础版，`aits score-daily` 会先校验 SEC 指标 CSV、构建 SEC 基本面特征，通过后按 `config/scoring_rules.yaml` 的 `fundamentals` 规则评分；失败时停止日报评分。
17. 接入回测 point-in-time SEC 基本面。状态：已实现基础版，`aits backtest` 会校验 SEC companyfacts 缓存，并按每个 signal_date 只使用 `filed_date <= signal_date` 的 SEC 事实生成基本面特征。
18. 接入估值快照评分。状态：已实现基础版，`aits score-daily` 会在估值快照校验通过后，用合规且未过期的估值快照按估值分位和拥挤比例评分；`public_convenience` 来源不会进入自动评分。

## 阶段 1 数据缓存约定

第一版数据缓存使用 CSV，便于检查和调试：

- `data/raw/prices_daily.csv`：日线价格，字段为 `date,ticker,open,high,low,close,adj_close,volume`。
- `data/raw/rates_daily.csv`：FRED 利率数据，字段为 `date,series,value`。

默认下载核心观察范围。`--full-universe` 会额外下载配置文件中的完整 AI 产业链标的。

## 阶段 1 数据质量约定

数据质量门禁命令为 `aits validate-data`，默认读取：

- `data/raw/prices_daily.csv`
- `data/raw/rates_daily.csv`

报告默认写入 `outputs/reports/data_quality_YYYY-MM-DD.md`。如果报告状态为 `FAIL`，后续评分报告和回测不应继续使用这批数据。

阈值集中配置在 `config/data_quality.yaml`，当前基础版包括：

- 数据新鲜度。
- 单日调整收盘价异常波动。
- 调整收盘价比例跳变。
- 利率合理范围。
- 利率单日异常变化。

## 阶段 1 特征缓存约定

市场环境特征命令为 `aits build-features`。该命令会先执行数据质量门禁，失败时停止。

默认输出：

- `data/processed/features_daily.csv`
- `outputs/reports/feature_summary_YYYY-MM-DD.md`

特征配置集中在 `config/features.yaml`，当前基础版包括：

- 20/50/100/200 日均线。
- 1/5/20 日收益率。
- SMH/SPY、SOXX/SPY、QQQ/SPY 相对强弱。
- VIX 20 日均值和 252 日分位。
- DGS2、DGS10 的 5/20 日变化。
- 核心观察池长期均线趋势宽度。

## 阶段 1 每日评分约定

每日评分命令为 `aits score-daily`。该命令会先执行市场数据质量门禁，再构建市场特征，并校验 SEC 指标 CSV、构建 SEC 基本面特征，最后把交易 thesis、风险事件、估值快照和交易复盘状态写入日报复核摘要。

默认输出：

- `data/processed/scores_daily.csv`
- `outputs/reports/daily_score_YYYY-MM-DD.md`

评分规则集中在 `config/scoring_rules.yaml`。当前基础版包括：

- 趋势：指数趋势、半导体趋势、核心观察池宽度、SMH/SPY 相对强弱。
- 基本面：已通过校验的 AI 核心观察池 SEC 特征中位数，包括季度毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度。
- 宏观流动性：DGS10、DGS2、美元指数。
- 风险情绪：VIX 当前值、VIX 分位、VIX 短期变化。
- 估值：已通过校验且来源合规的估值快照，使用估值分位和拥挤比例；缺少有效快照时标记为数据不足。
- 政策地缘：明确标记为 MVP 占位输入。
- 人工复核摘要：汇总 thesis、风险事件、估值快照和交易复盘状态；交易复盘复用同一份数据质量门禁结果。

如果某个硬数据模块的信号覆盖率低于配置阈值，模块使用中性分并标记为 `insufficient_data`，不能静默给出伪精确分数。

## 观察池与能力圈约定

观察池配置文件为 `config/watchlist.yaml`，用于记录核心个股是否处在能力圈内、映射到哪些 AI 产业链节点、默认风险等级和是否要求交易 thesis。

命令：

- `aits watchlist list`
- `aits watchlist validate`

校验报告默认写入 `outputs/reports/watchlist_validation_YYYY-MM-DD.md`。核心观察池中的每个个股都必须出现在活跃观察池中，并且必须映射到至少一个 AI 产业链节点。高风险或极高风险标的必须要求 thesis，避免后续报告把高风险单票当成无约束的高置信度输入。

## 阶段 1 回测约定

历史回测命令为 `aits backtest`。命令会先执行数据质量门禁，失败时停止。

默认规则：

- 默认市场阶段为 `config/market_regimes.yaml` 中的 `ai_after_chatgpt`，回测起点为 `2022-12-01`。
- 使用 `SMH` 作为 AI 仓位代理标的。
- 与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。
- 每日收盘后生成信号，下一交易日生效。
- 目标仓位使用评分模型输出的 AI 仓位区间中点。
- 仓位变化低于最小调仓阈值时不调仓。
- 默认扣除 5 bps 单边交易成本。

默认输出：

- `outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md`
- `outputs/backtests/backtest_daily_YYYY-MM-DD_YYYY-MM-DD.csv`

当前回测状态会标记为 `PASS_WITH_LIMITATIONS`，因为估值和政策/地缘模块仍是 MVP 占位输入。回测已经按 signal_date 接入 point-in-time SEC 基本面特征，但仍不能代表完整投资系统已经完成。

如果要运行跨周期压力测试，可以使用 `--regime cross_cycle_stress`，其默认起点为 `2019-01-01`，使用 2018 年历史作为 200 日均线和 252 日 VIX 分位的 warm-up。该区间覆盖更多宏观压力环境，但不应替代 ChatGPT 之后 AI 主线行情的默认解释窗口。

## 产业链因果图约定

产业链配置文件为 `config/industry_chain.yaml`，用于记录 AI 产业链节点、父子关系、领先指标、相关标的、影响周期、现金流相关性和情绪相关性。

命令：

- `aits industry-chain list`
- `aits industry-chain validate`

校验报告默认写入 `outputs/reports/industry_chain_validation_YYYY-MM-DD.md`。基础版校验会检查节点 ID 是否重复、父节点是否存在、因果图是否有环、每个节点是否配置领先指标和相关标的，以及观察池引用的产业链节点是否存在。

产业链节点当前只作为信息组织和后续评分输入的结构基础，不直接触发交易动作。

## 已确认的产品决策

- 决策频率：第一版按日更新，基于美股日线收盘后数据生成信号。
- 交易对象：第一版同时跟踪 ETF 和核心个股。ETF 用于基准和主回测，MSFT、GOOG、TSM、INTC、AMD、NVDA 用于核心观察池和趋势一致性评分。
- 数据源：建议第一版用免费源快速验证流程，第二期再替换或补充付费基本面数据。
- 输出形态：先 CLI + Markdown 日报，规则稳定后再做 Streamlit 仪表盘。
- 仓位口径：同时输出“股票风险资产中的 AI 仓位”和“总资产中的 AI 仓位”。

## 日频信号约束

每日信号容易引入过度交易，所以 MVP 需要在规则里加入约束：

- 使用收盘后数据，避免盘中噪音。
- 仓位变化设置最小调整阈值，例如低于 5% 不操作。
- 同一方向连续触发或关键均线确认后再调整核心仓。
- 将“信号变化”和“实际交易建议”分开输出，便于人工复核。
