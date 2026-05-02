# 工程落地 Backlog

本文把 `docs/product_strategy.md` 中的方向拆成可开发模块。它的用途不是替代开发计划，而是把“想建设什么”转换成数据结构、命令、验收标准和阶段顺序。

## 落地原则

- 先做能形成闭环的最小模块，再扩展数据源和模型复杂度。
- 所有依赖市场缓存数据的模块必须先通过 `aits validate-data`。
- 任何评分必须说明输入来源：硬数据、手工录入、占位默认值，还是 LLM 辅助抽取。
- 不把新闻摘要直接变成交易动作；新闻只能改变假设、风险等级、人工审核状态或评分输入。
- 所有仓位建议必须同时输出“风险资产内口径”和“总资产口径”。
- 默认策略解释和回测优先使用 `ai_after_chatgpt` 市场阶段：锚定 `2022-11-30` ChatGPT 公开发布，默认起点 `2022-12-01`。2019 年以来的长窗口只作为跨周期压力测试或 warm-up。

## 当前已完成基础

|能力|状态|说明|
|---|---|---|
|工程骨架|已完成|Python package、CLI、测试、CI、文档|
|数据下载|已完成基础版|`aits download-data`，价格、VIX、DXY、FRED 利率，并写入 `download_manifest.csv` 审计清单|
|数据源目录|已完成基础版|`config/data_sources.yaml` 和 `aits data-sources list/validate`，记录来源类型、审计字段、校验项和限制说明|
|SEC 基本面原始数据|已完成基础版|`config/sec_companies.yaml` 和 `aits fundamentals download-sec-companyfacts`，下载 companyfacts JSON 并写入 manifest|
|SEC 基本面缓存校验|已完成基础版|`aits fundamentals validate-sec-companyfacts`，校验 JSON、CIK、taxonomy 和 checksum|
|数据质量门禁|已完成基础版|`aits validate-data`，失败时非零退出|
|市场环境特征|已完成基础版|`aits build-features`，趋势、相对强弱、VIX、利率、核心池宽度|
|每日市场评分|已完成基础版|`aits score-daily`，趋势、宏观流动性、风险情绪和占位项|
|仓位评分骨架|已完成基础版|100 分映射到仓位区间，支持总资产换算|
|观察池与能力圈|已完成基础版|`aits watchlist list/validate`，核心个股能力圈和产业链节点映射|
|历史回测|已完成基础版|`aits backtest`，每日评分动态仓位与 SPY/QQQ/SMH/SOXX 基准对比|
|产业链因果图|已完成基础版|`aits industry-chain list/validate`，节点、父子关系、领先指标和观察池引用校验|
|交易 thesis 与假设验证|已完成基础版|`aits thesis list/validate/review`，结构化假设、验证指标、证伪条件和复核报告|
|风险事件分级|已完成基础版|`aits risk-events list/validate`，L1/L2/L3、影响节点、相关标的和动作规则|
|估值与拥挤度|已完成基础版|`aits valuation list/validate/review`，估值快照、预期指标、拥挤度信号和来源校验|
|复盘归因|已完成基础版|`aits review-trades`，交易记录校验、数据质量门禁和 SPY/QQQ/SMH/SOXX 基础归因|
|日报复核摘要|已完成基础版|`aits score-daily` 汇总 thesis、风险事件、估值快照和交易复盘状态，交易复盘复用同一份数据质量门禁结果|
|产品策略|已完成文档|能力圈、产业链因果、假设验证、复盘归因|

## 推荐建设顺序

### 阶段 1：历史回测命令

状态：已实现基础版。

目的：检验每日评分和仓位映射在历史行情中的收益、回撤和换手表现。

建议 CLI：

```powershell
aits backtest --to 2026-05-02 --quality-as-of 2026-05-02
```

输出：

- `outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md`
- `outputs/backtests/backtest_daily_YYYY-MM-DD_YYYY-MM-DD.csv`

当前规则：

- 先执行数据质量门禁，失败则停止。
- 未显式传入 `--from` 时，起点来自 `config/market_regimes.yaml` 的默认市场阶段。
- 默认市场阶段为 `ai_after_chatgpt`，即 `2022-12-01` 开始的 ChatGPT 后 AI 主线行情。
- 每日收盘后计算评分，目标仓位从下一交易日收益开始生效，避免未来函数。
- 使用 AI 仓位区间中点作为目标仓位。
- 变化小于 `config/scoring_rules.yaml` 的最小调仓阈值时维持原仓位。
- 默认用 `SMH` 作为 AI 代理标的，并与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。
- 默认扣除单边交易成本 5 bps。

限制：

- 基本面、估值、政策/地缘仍是 MVP 占位输入，因此回测只能验证当前硬数据规则和仓位映射，不能视为完整策略结论。
- 当前未计入税费、汇率、融资利率、盘口冲击和盘中执行偏差。
- `cross_cycle_stress` 从 `2019-01-01` 开始，适合作为非默认压力测试；这类结果需要和默认 AI regime 结果分开解释。

### M1：市场环境特征模块

状态：已实现基础版。

目的：先把 MVP 的硬数据转成可解释特征，为每日评分提供输入。

数据输入：

- `data/raw/prices_daily.csv`
- `data/raw/rates_daily.csv`
- `data/raw/download_manifest.csv`
- `outputs/reports/data_quality_YYYY-MM-DD.md`

主要数据对象：

- `MarketFeatureRow`
- `TrendFeatureSet`
- `RiskSentimentFeatureSet`
- `MacroLiquidityFeatureSet`

建议 CLI：

```powershell
aits build-features --as-of 2026-05-01
```

输出：

- `data/processed/features_daily.csv`
- `outputs/reports/feature_summary_YYYY-MM-DD.md`

第一版特征：

- SPY、QQQ、SMH、SOXX、核心个股相对 20/50/100/200 日均线。
- SMH/SPY、QQQ/SPY 相对强弱。
- VIX 当前值、20 日均值、历史分位。
- DGS10、DGS2 的 5/20 日变化。
- 核心观察池中处于长期均线以上的比例。

验收标准：

- 命令会先执行或调用数据质量门禁，失败时停止。
- 输出中标记每个特征的计算窗口和所需最小历史长度。
- 如果历史长度不足，不能静默填充；必须输出 warning 或缺失原因。

### M2：每日市场评分模块

状态：已实现基础版。

目的：把 M1 特征转成趋势、宏观流动性、风险情绪评分。

主要数据对象：

- `ScoreComponent`
- `DailyScoreReport`
- `PositionRecommendation`

建议 CLI：

```powershell
aits score-daily --as-of 2026-05-01
```

输出：

- `outputs/reports/daily_score_YYYY-MM-DD.md`
- `data/processed/scores_daily.csv`

第一版评分：

- 趋势评分：指数趋势、半导体趋势、核心个股趋势一致性、相对强弱。
- 宏观流动性评分：10Y、2Y、美元指数趋势。
- 风险情绪评分：VIX 水平、VIX 分位、波动上升速度。
- 基本面、估值、政策地缘：明确标记为中性占位或手工输入。

验收标准：

- 报告必须声明数据质量状态。
- 报告必须区分硬数据评分和占位评分。
- 仓位建议必须同时输出风险资产内 AI 仓位和总资产 AI 仓位。
- 低于最小仓位变化阈值时，输出“信号变化”但不输出交易动作。

### M3：观察池与能力圈模块

状态：已实现基础版。

目的：记录哪些标的在能力圈内，以及为什么值得观察或持有。

配置建议：

- `config/watchlist.yaml`

主要数据对象：

- `WatchlistItem`
- `CircleOfCompetenceProfile`
- `HoldingIntent`

建议字段：

- ticker
- company_name
- sector
- ai_chain_nodes
- competence_score
- competence_reason
- default_risk_level
- thesis_required
- active

建议 CLI：

```powershell
aits watchlist list
aits watchlist validate
```

当前基础版输出：

- `outputs/reports/watchlist_validation_YYYY-MM-DD.md`

验收标准：

- 每个核心标的必须映射到至少一个产业链节点。
- 能力圈外标的不能被默认纳入高置信度评分。
- 观察池变更必须可 diff，可审计。

### M4：产业链节点与因果图模块

状态：已实现基础版。

目的：把信息映射到产业节点，而不是只记录新闻标题。

配置建议：

- `config/industry_chain.yaml`

核心节点第一版：

- cloud_capex
- gpu_asic_demand
- hbm_supply_demand
- advanced_packaging
- foundry_demand
- semiconductor_equipment
- ai_inference_demand
- export_controls
- power_and_data_center_capacity

主要数据对象：

- `IndustryChainNode`
- `NodeIndicator`
- `IndustrySignal`

建议字段：

- node_id
- parent_node_ids
- leading_indicators
- related_tickers
- impact_horizon: short / medium / long
- cash_flow_relevance

当前基础版输出：

- `outputs/reports/industry_chain_validation_YYYY-MM-DD.md`

当前基础版校验：

- 节点 ID 不能重复。
- 父节点必须存在。
- 因果图不能形成环。
- 每个节点必须配置领先指标和相关标的。
- 观察池引用的产业链节点必须存在。

验收标准：

- 每条产业信息必须映射到节点。
- 必须区分短期情绪影响和长期现金流影响。
- 节点分数不能直接触发交易，只能进入评分和人工审核。

### M5：交易 thesis 与假设验证模块

状态：已实现基础版。

目的：把每笔重要交易变成可验证假设。

存储建议：

- `data/external/trade_theses/*.yaml`。该目录不提交，用于本地个人交易假设。
- `docs/examples/trade_theses/` 提供可复制模板，不代表真实交易建议。

主要数据对象：

- `TradeThesis`
- `ValidationMetric`
- `FalsificationCondition`
- `ThesisReview`

建议字段：

- thesis_id
- ticker
- direction
- created_at
- time_horizon
- position_scope
- entry_reason
- validation_metrics
- falsification_conditions
- risk_events
- review_frequency
- status

建议 CLI：

```powershell
aits thesis list
aits thesis validate
aits thesis review --as-of 2026-05-01
```

当前基础版输出：

- `outputs/reports/thesis_validation_YYYY-MM-DD.md`
- `outputs/reports/thesis_review_YYYY-MM-DD.md`

当前基础版校验：

- YAML schema 是否完整。
- `thesis_id`、验证指标、证伪条件和风险事件 ID 是否重复。
- 活跃 thesis 的 ticker 是否在活跃观察池中。
- thesis 引用的产业链节点是否存在。
- 验证指标是否超过复核频率。
- 高强度证伪条件触发后，thesis 是否仍错误保持 active。

验收标准：

- 没有 thesis 的主动交易不能被报告标记为高置信度。
- 每个 thesis 必须包含证伪条件。
- 复盘时必须判断原始假设是否仍成立。

### M6：风险事件分级模块

状态：已实现基础版。

目的：把重大事件从主观恐慌转换成结构化风险动作。

配置建议：

- `config/risk_events.yaml`

主要数据对象：

- `RiskEvent`
- `RiskEventLevel`
- `RiskActionRule`

等级：

- L1：普通噪音，观察。
- L2：中等风险，降低部分仓位，等待确认。
- L3：重大风险，快速降风险，等待重新定价。

验收标准：

- 每条风险事件必须有来源、时间、等级、影响节点和建议动作。
- L2/L3 事件必须进入日报显著位置。
- 风险事件动作不能绕过仓位上限和人工复核规则。

当前基础版命令：

```powershell
aits risk-events list
aits risk-events validate --as-of 2026-05-02
```

当前基础版输出：

- `outputs/reports/risk_events_validation_YYYY-MM-DD.md`

当前基础版校验：

- L1/L2/L3 等级必须完整且唯一。
- 风险等级越高，AI 仓位折扣乘数不能更高。
- L2/L3 必须要求人工复核。
- 事件影响的产业链节点必须存在。
- 事件相关标的必须处于配置的数据 universe 或观察池中。
- 活跃 L2/L3 事件建议配置升级条件和解除条件。

### M7：估值与拥挤度模块

状态：已实现基础版。

目的：避免“好公司但价格已经透支”的错误。

数据来源优先级：

1. 正式财务/估值 API 或付费数据源。
2. 手工导入的财报和估值表。
3. 公开网页或 LLM 抽取只能作为辅助，不能直接作为主数据源。

主要数据对象：

- `ValuationSnapshot`
- `ExpectationSnapshot`
- `CrowdingSignal`

第一版指标：

- Forward P/E。
- EV/Sales。
- PEG。
- 收入增速预期。
- EPS 预期变化。
- 估值历史分位。
- 财报后利好不涨。
- 龙头股相对行业走弱。

验收标准：

- 估值数据必须有来源和更新时间。
- 如果数据源不可审计，不能进入自动评分，只能进入人工备注。
- 拥挤度只改变仓位折扣，不直接决定买卖。

当前基础版命令：

```powershell
aits valuation list
aits valuation validate --as-of 2026-05-02
aits valuation review --as-of 2026-05-02
```

当前基础版输入：

- `data/external/valuation_snapshots/*.yaml`。该目录不提交，用于本地手工或供应商估值快照。
- `docs/examples/valuation_snapshots/` 提供可复制模板，不代表真实交易建议。

当前基础版输出：

- `outputs/reports/valuation_validation_YYYY-MM-DD.md`
- `outputs/reports/valuation_review_YYYY-MM-DD.md`

当前基础版校验：

- 估值快照必须包含来源类型、来源名称、日期和采集时间。
- ticker 必须处于数据 universe 或观察池中。
- 估值倍数不能为负数。
- 估值历史分位必须在 0-100 范围内。
- 快照超过新鲜度阈值会警告。
- `public_convenience` 来源只能作为辅助，不能直接进入自动评分。

### M8：复盘归因模块

状态：已实现基础版。

目的：区分市场 Beta、行业/主题 Beta、个股 Alpha、仓位错误和纪律问题。

主要数据对象：

- `TradeReview`
- `PerformanceAttribution`
- `RuleViolation`

第一版归因：

- 相对 SPY。
- 相对 QQQ。
- 相对 SMH/SOXX。
- 个股相对 AI 核心观察池均值。
- 仓位贡献。

建议 CLI：

```powershell
aits review-trades --from 2026-01-01 --to 2026-03-31
```

当前基础版命令：

```powershell
aits review-trades --as-of 2026-05-02
```

当前基础版输入：

- `data/external/trades/*.yaml`。该目录不提交，用于本地交易记录。
- `docs/examples/trades/` 提供可复制模板，不代表真实交易建议。
- `data/raw/prices_daily.csv` 和 `data/raw/rates_daily.csv`，用于数据质量门禁和基准归因。

当前基础版输出：

- `outputs/reports/trade_review_YYYY-MM-DD.md`

当前基础版校验和归因：

- 命令必须先通过 `aits validate-data` 同一路径的数据质量门禁。
- 交易记录必须包含 ticker、方向、开仓日期、入场价格；已关闭交易必须包含平仓日期和出场价格。
- 交易记录建议关联 `thesis_id`，否则报告会警告。
- 交易收益与同区间 `SPY`、`QQQ`、`SMH`、`SOXX` 对比。
- 归因提示只做市场 Beta、AI 主题 Beta 和个股表现的规则化摘要。

验收标准：

- 赚钱也必须归因，不能只复盘亏损。
- 报告必须指出是否违反原始交易规则。
- 报告必须区分判断正确、行情 Beta、仓位运气和纪律问题。

## 数据文件与配置规划

|路径|用途|阶段|
|---|---|---|
|`config/features.yaml`|市场环境特征窗口、相对强弱组合和 VIX/利率设置|M1|
|`config/watchlist.yaml`|观察池和能力圈|M3，已实现基础版|
|`config/industry_chain.yaml`|产业链节点和因果图|M4，已实现基础版|
|`config/market_regimes.yaml`|市场阶段、默认回测区间和压力测试区间|阶段 1，已实现基础版|
|`config/risk_events.yaml`|风险事件等级和动作规则|M6，已实现基础版|
|`config/sec_companies.yaml`|SEC companyfacts ticker/CIK 映射|阶段 2，已实现基础版|
|`config/scoring_rules.yaml`|评分规则和权重|M2|
|`data/raw/download_manifest.csv`|下载审计清单，记录 provider、endpoint、请求参数、下载时间、行数和 checksum|M1，已实现基础版|
|`data/raw/sec_companyfacts/`|SEC companyfacts 原始 JSON 和下载 manifest|阶段 2，已实现基础版|
|`data/processed/features_daily.csv`|每日特征|M1|
|`data/processed/scores_daily.csv`|每日评分|M2|
|`data/external/trade_theses/`|交易 thesis|M5，已实现基础版|
|`data/external/valuation_snapshots/`|估值、预期和拥挤度快照|M7，已实现基础版|
|`data/external/trades/`|交易记录|M8，已实现基础版|
|`docs/examples/trade_theses/`|交易 thesis YAML 模板|M5，已实现基础版|
|`docs/examples/valuation_snapshots/`|估值快照 YAML 模板|M7，已实现基础版|
|`docs/examples/trades/`|交易记录 YAML 模板|M8，已实现基础版|
|`outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md`|历史回测报告|阶段 1，已实现基础版|
|`outputs/reports/daily_score_YYYY-MM-DD.md`|每日评分报告|M2|
|`outputs/reports/thesis_review_YYYY-MM-DD.md`|假设复核报告|M5|
|`outputs/reports/risk_events_validation_YYYY-MM-DD.md`|风险事件规则校验报告|M6|
|`outputs/reports/valuation_validation_YYYY-MM-DD.md`|估值快照校验报告|M7|
|`outputs/reports/valuation_review_YYYY-MM-DD.md`|估值与拥挤度复核报告|M7|
|`outputs/reports/trade_review_YYYY-MM-DD.md`|复盘归因报告|M8|

## 近期最小落地路径

接下来建议按这个顺序开发：

1. 实现 SEC companyfacts 基础财报指标抽取，先输出结构化财报摘要，不进入自动评分。
2. 在 SEC 指标抽取通过质量校验后，把基本面从占位项逐步转为可评分模块。

原因：

- 阶段 1 的市场数据、评分、观察池、回测、产业链配置、交易 thesis、风险事件分级、估值与拥挤度快照、交易复盘基础闭环，以及日报复核摘要集成已经完成。
- 下一步的主要价值是把 SEC companyfacts 原始 JSON 转成可解释、可校验的基本面指标，并在进入自动评分前先保证字段映射、taxonomy 差异和 restatement 风险可控。

## 不应马上做的事

- 不应直接做全市场选股。
- 不应直接接新闻并让 LLM 输出买卖建议。
- 不应在没有估值数据来源审计的情况下做估值自动评分。
- 不应在没有交易记录结构的情况下做复杂绩效归因。
- 不应把基本面、估值、政策地缘继续长期保留为无说明的中性占位。
