# 工程落地 Backlog

本文把 `docs/product_strategy.md` 中的方向拆成可开发模块。它的用途不是替代开发计划，而是把“想建设什么”转换成数据结构、命令、验收标准和阶段顺序。

## 落地原则

- 先做能形成闭环的最小模块，再扩展数据源和模型复杂度。
- 所有依赖市场缓存数据的模块必须先通过 `aits validate-data`。
- 任何评分必须说明输入来源：硬数据、手工录入、占位默认值，还是 LLM 辅助抽取。
- 不把新闻摘要直接变成交易动作；新闻只能改变假设、风险等级、人工审核状态或评分输入。
- 所有仓位建议必须同时输出“风险资产内口径”和“总资产口径”。

## 当前已完成基础

|能力|状态|说明|
|---|---|---|
|工程骨架|已完成|Python package、CLI、测试、CI、文档|
|数据下载|已完成基础版|`aits download-data`，价格、VIX、DXY、FRED 利率|
|数据质量门禁|已完成基础版|`aits validate-data`，失败时非零退出|
|市场环境特征|已完成基础版|`aits build-features`，趋势、相对强弱、VIX、利率、核心池宽度|
|仓位评分骨架|已完成基础版|100 分映射到仓位区间，支持总资产换算|
|产品策略|已完成文档|能力圈、产业链因果、假设验证、复盘归因|

## 推荐建设顺序

### M1：市场环境特征模块

状态：已实现基础版。

目的：先把 MVP 的硬数据转成可解释特征，为每日评分提供输入。

数据输入：

- `data/raw/prices_daily.csv`
- `data/raw/rates_daily.csv`
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

验收标准：

- 每个核心标的必须映射到至少一个产业链节点。
- 能力圈外标的不能被默认纳入高置信度评分。
- 观察池变更必须可 diff，可审计。

### M4：产业链节点与因果图模块

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

验收标准：

- 每条产业信息必须映射到节点。
- 必须区分短期情绪影响和长期现金流影响。
- 节点分数不能直接触发交易，只能进入评分和人工审核。

### M5：交易 thesis 与假设验证模块

目的：把每笔重要交易变成可验证假设。

存储建议：

- `data/external/trade_theses/*.yaml` 或后续数据库表。

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
aits thesis validate
aits thesis review --as-of 2026-05-01
```

验收标准：

- 没有 thesis 的主动交易不能被报告标记为高置信度。
- 每个 thesis 必须包含证伪条件。
- 复盘时必须判断原始假设是否仍成立。

### M6：风险事件分级模块

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

### M7：估值与拥挤度模块

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

### M8：复盘归因模块

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

验收标准：

- 赚钱也必须归因，不能只复盘亏损。
- 报告必须指出是否违反原始交易规则。
- 报告必须区分判断正确、行情 Beta、仓位运气和纪律问题。

## 数据文件与配置规划

|路径|用途|阶段|
|---|---|---|
|`config/features.yaml`|市场环境特征窗口、相对强弱组合和 VIX/利率设置|M1|
|`config/watchlist.yaml`|观察池和能力圈|M3|
|`config/industry_chain.yaml`|产业链节点和因果图|M4|
|`config/risk_events.yaml`|风险事件等级和动作规则|M6|
|`config/scoring_rules.yaml`|评分规则和权重|M2|
|`data/processed/features_daily.csv`|每日特征|M1|
|`data/processed/scores_daily.csv`|每日评分|M2|
|`data/external/trade_theses/`|交易 thesis|M5|
|`outputs/reports/daily_score_YYYY-MM-DD.md`|每日评分报告|M2|
|`outputs/reports/thesis_review_YYYY-MM-DD.md`|假设复核报告|M5|
|`outputs/reports/trade_review_YYYY-MM-DD.md`|复盘归因报告|M8|

## 近期最小落地路径

接下来建议按这个顺序开发：

1. M2：每日市场评分报告。
2. M3：观察池与能力圈配置。
3. M4：产业链节点配置。

原因：

- M2 延续当前已经完成的数据层和特征层，能最快形成可运行日报。
- M3/M4 会把产品策略中的能力圈和产业链因果落进配置，为后续基本面和新闻模块打地基。
- M5-M8 需要更明确的持仓、交易记录和估值数据源，适合在日报闭环稳定后推进。

## 不应马上做的事

- 不应直接做全市场选股。
- 不应直接接新闻并让 LLM 输出买卖建议。
- 不应在没有估值数据来源审计的情况下做估值自动评分。
- 不应在没有交易记录结构的情况下做复杂绩效归因。
- 不应把基本面、估值、政策地缘继续长期保留为无说明的中性占位。
