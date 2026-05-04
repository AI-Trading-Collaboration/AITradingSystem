# 工程落地 Backlog

本文把 `docs/product_strategy.md` 中的方向拆成可开发模块。它的用途不是替代开发计划，而是把“想建设什么”转换成数据结构、命令、验收标准和阶段顺序。

具体未完成任务、优先级、阻塞原因、owner 配合事项和基础版遗留缺口统一登记在 `docs/task_register.md`。本文负责长期模块路线图；任务状态变更以 task register 为准。

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
|SEC 基本面指标抽取|已完成基础版|`config/fundamental_metrics.yaml` 和 `aits fundamentals extract-sec-metrics`，先过 SEC 缓存质量门禁，再输出结构化指标摘要和中文报告；支持显式派生指标和公司级 SEC 周期覆盖声明|
|SEC 基本面指标校验|已完成基础版|`aits fundamentals validate-sec-metrics`，校验指标 CSV 的 schema、重复键、未来披露日期、数值合法性和配置覆盖率，并输出缺失 `ticker / metric_id / period_type` 观测清单|
|SEC 基本面特征|已完成基础版|`config/fundamental_features.yaml` 和 `aits fundamentals build-sec-features`，先过 SEC 指标 CSV 门禁，再生成毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度|
|SEC 基本面评分|已完成基础版|`aits score-daily` 会校验 SEC 指标 CSV、构建 SEC 特征，并用 AI 核心观察池 SEC 特征中位数进行基本面硬数据评分|
|数据质量门禁|已完成基础版|`aits validate-data`，失败时非零退出|
|市场环境特征|已完成基础版|`aits build-features`，趋势、相对强弱、VIX、利率、核心池宽度|
|每日市场评分|已完成基础版|`aits score-daily`，趋势、SEC 基本面、宏观流动性、风险情绪、估值快照和政策/地缘发生记录评分|
|仓位评分骨架|已完成基础版|100 分映射到仓位区间，支持总资产换算|
|观察池与能力圈|已完成基础版|`aits watchlist list/validate`，核心个股能力圈和产业链节点映射|
|历史回测|已完成基础版|`aits backtest`，每日评分动态仓位与 SPY/QQQ/SMH/SOXX 基准对比|
|回测 SEC 基本面|已完成基础版|`aits backtest` 按 signal_date 从 SEC companyfacts 构建 point-in-time 基本面特征，避免使用未来披露，并输出 SEC 基本面质量摘要|
|回测估值/风险事件|已完成基础版|`aits backtest` 按 signal_date 过滤估值快照和风险事件证据/解决状态，避免估值与政策/地缘模块使用未来信息|
|回测模块覆盖率趋势|已完成基础版|回测报告输出数据质量门禁摘要、评分模块覆盖率摘要、按月覆盖率趋势、按月来源类型趋势、月度输入问题下钻、月度输入证据 URL 摘要、月度风险事件证据 URL 明细、月度 ticker 输入摘要、月度 ticker SEC 特征明细、月度估值快照来源和月度风险事件证据来源分布，并写出机器可读输入覆盖诊断 CSV，辅助识别历史切片中的基本面、估值或风险事件覆盖缺口|
|回测输入审计报告|已完成基础版|`aits backtest` 额外写出 `backtest_audit_*.md`，汇总数据质量、point-in-time 输入切片、模块覆盖率、来源类型、历史输入问题和执行假设，判断本次回测是否可解释；`--fail-on-audit-warning` 可把非 PASS 审计状态转为本地门禁失败|
|产业链因果图|已完成基础版|`aits industry-chain list/validate`，节点、父子关系、领先指标和观察池引用校验|
|交易 thesis 与假设验证|已完成基础版|`aits thesis list/validate/review`，结构化假设、验证指标、证伪条件和复核报告|
|风险事件分级|已完成基础版|`aits risk-events list/validate`，L1/L2/L3、影响节点、相关标的和动作规则|
|风险事件发生记录|已完成基础版|`aits risk-events list-occurrences/validate-occurrences`，校验实际触发事件、证据来源、日期和 active/watch/resolved 状态|
|风险事件发生记录 CSV 导入|已完成基础版|`aits risk-events import-occurrences-csv`，导入人工复核后的结构化 CSV，多证据行合并并写入 YAML；不会把原始新闻自动变成交易动作|
|估值与拥挤度|已完成基础版|`aits valuation list/validate/review`，估值快照、预期指标、拥挤度信号和来源校验|
|FMP 估值/预期 API|已完成基础版|`aits valuation fetch-fmp`，从 Financial Modeling Prep 拉取 quote、TTM key metrics、TTM ratios 和 annual analyst estimates，生成 paid vendor 快照、原始 analyst estimate 历史缓存、本地估值历史分位、checksum 拉取报告和估值校验报告|
|FMP analyst history 校验|已完成基础版|`aits valuation validate-fmp-history`，校验原始 analyst-estimates JSON 的 schema、checksum、row_count、ticker、请求参数、日期和重复 estimate date|
|估值/预期 CSV 导入|已完成基础版|`aits valuation import-csv`，把结构化宽表转换为估值快照 YAML，并生成 checksum 导入报告|
|估值评分|已完成基础版|`aits score-daily` 使用已通过校验、未过期且来源合规的估值快照，对估值分位和拥挤比例评分；`public_convenience` 不进入自动评分|
|TSMC IR 季度基本面导入|已完成基础版|`aits fundamentals fetch-tsm-ir-quarterly` 可从官方季度页面下载文本版 Management Report；`extract-tsm-ir-pdf-text` 可从官方 PDF 文本层生成可审计文本；`import-tsm-ir-quarterly` 可导入单个已抽取本地文本并用 `--filed-date` 记录公开/披露日期；`import-tsm-ir-quarterly-batch` 可按 manifest 批量回填历史季度，重复季度、缺文件、非官方 URL 或任一季度解析错误时整批停止写入；`merge-tsm-ir-sec-metrics` 和 `aits backtest` 会按评估日或 signal_date 选择最新已披露 TSM 季度，把收入、毛利、营业利润、净利、研发和 CapEx 合并进统一 SEC-style 指标路径|
|复盘归因|已完成基础版|`aits review-trades`，交易记录校验、数据质量门禁和 SPY/QQQ/SMH/SOXX 基础归因|
|日报复核摘要|已完成基础版|`aits score-daily` 汇总 thesis、风险事件规则与发生记录、估值快照和交易复盘状态，交易复盘复用同一份数据质量门禁结果|
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
- `outputs/backtests/backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv`
- `outputs/backtests/backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md`

当前规则：

- 先执行数据质量门禁，失败则停止。
- 未显式传入 `--from` 时，起点来自 `config/market_regimes.yaml` 的默认市场阶段。
- 默认市场阶段为 `ai_after_chatgpt`，即 `2022-12-01` 开始的 ChatGPT 后 AI 主线行情。
- 每日收盘后计算评分，目标仓位从下一交易日收益开始生效，避免未来函数。
- 使用 AI 仓位区间中点作为目标仓位。
- 变化小于 `config/scoring_rules.yaml` 的最小调仓阈值时维持原仓位。
- 默认用 `SMH` 作为 AI 代理标的，并与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。
- 默认扣除单边交易成本 5 bps；可用 `--slippage-bps` 显式加入线性滑点或盘口冲击估算。

限制：

- 回测已接入 point-in-time SEC 基本面特征、估值快照和政策/地缘风险事件发生记录；结果仍受税费、汇率、融资、非线性盘口冲击、容量约束和盘中执行偏差等简化假设限制。
- 当前默认未额外计入滑点；如需保守执行假设，必须显式传入 `--slippage-bps`。该参数只做线性扣减，不替代真实成交量、盘口深度或盘中执行模型。
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

目的：把 M1 特征和已通过校验的 SEC 基本面特征转成趋势、基本面、宏观流动性、风险情绪评分。

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
- 基本面评分：基于 AI 核心观察池 SEC 特征中位数，先覆盖季度毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度。
- 宏观流动性评分：10Y、2Y、美元指数趋势。
- 风险情绪评分：VIX 水平、VIX 分位、波动上升速度。
- 估值评分：使用已通过校验、未过期且来源合规的估值快照。
- 政策/地缘评分：使用已通过校验、来源合规且处于 active/watch 的风险事件发生记录；缺少有效记录时标记为数据不足。

验收标准：

- 报告必须声明市场数据质量状态和 SEC 基本面质量状态。
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
- `data/external/risk_event_occurrences/*.yaml`
- `docs/examples/risk_event_occurrences/`

主要数据对象：

- `RiskEvent`
- `RiskEventLevel`
- `RiskActionRule`
- `RiskEventOccurrence`
- `RiskEventEvidenceSource`

等级：

- L1：普通噪音，观察。
- L2：中等风险，降低部分仓位，等待确认。
- L3：重大风险，快速降风险，等待重新定价。

验收标准：

- 每条风险事件规则必须有等级、影响节点和建议动作。
- 每条实际发生记录必须有来源、时间、状态和判断摘要。
- L2/L3 事件必须进入日报显著位置。
- 风险事件动作不能绕过仓位上限和人工复核规则。
- 不能把 `config/risk_events.yaml` 的 `active` 监控标记当作事件已经发生。

当前基础版命令：

```powershell
aits risk-events list
aits risk-events validate --as-of 2026-05-02
aits risk-events import-occurrences-csv --input-path data/external/risk_event_imports/reviewed_events.csv --as-of 2026-05-02
aits risk-events list-occurrences
aits risk-events validate-occurrences --as-of 2026-05-02
```

当前基础版输出：

- `outputs/reports/risk_events_validation_YYYY-MM-DD.md`
- `outputs/reports/risk_event_occurrence_import_YYYY-MM-DD.md`
- `outputs/reports/risk_event_occurrences_YYYY-MM-DD.md`

当前基础版校验：

- L1/L2/L3 等级必须完整且唯一。
- 风险等级越高，AI 仓位折扣乘数不能更高。
- L2/L3 必须要求人工复核。
- 事件影响的产业链节点必须存在。
- 事件相关标的必须处于配置的数据 universe 或观察池中。
- 活跃 L2/L3 事件建议配置升级条件和解除条件。
- 发生记录的 `event_id` 必须引用已配置规则。
- CSV 导入只接受人工复核后的结构化发生记录；同一 `occurrence_id` 的多行只用于合并证据来源。
- 发生记录日期不能晚于评估日期，active/watch 记录超过新鲜度阈值会警告。
- 只有 `primary_source`、`paid_vendor` 或 `manual_input` 且证据等级为 `S/A/B` 的 active 发生记录可以进入普通评分；`B` 级不能单独触发仓位闸门，`C/D/X` 和单独的 `public_convenience` 证据只能作为辅助或人工复核。

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
aits valuation fetch-fmp --tickers NVDA,MSFT --as-of 2026-05-02
aits valuation validate-fmp-history --as-of 2026-05-02
aits valuation import-csv --input-path data/external/valuation_imports/vendor_export.csv --as-of 2026-05-02
aits valuation list
aits valuation validate --as-of 2026-05-02
aits valuation review --as-of 2026-05-02
```

当前基础版输入：

- `FMP_API_KEY` 环境变量。命令只读取 key，不输出、不落盘。
- `EODHD_API_KEY` 环境变量。`fetch-eodhd-trends` 只读取 key，不输出、不落盘。
- FMP provider symbol alias。内部核心观察池保留 `GOOG`，FMP 请求使用 `GOOG -> GOOGL`，并在拉取报告和 analyst history 请求参数中显式记录。
- `data/raw/fmp_analyst_estimates/`。保存 FMP analyst-estimates 原始响应、请求参数、下载时间、row count 和 checksum，用于 90 日 EPS revision。
- `data/raw/eodhd_earnings_trends/`。保存 EODHD Earnings Trends 原始响应、请求参数、下载时间、row count 和 checksum，用于当前采集日可见的 EPS 90 日修正 baseline。
- `data/external/valuation_snapshots/*.yaml`。该目录不提交，用于本地手工或供应商估值快照。
- `data/external/valuation_imports/*.csv`。结构化宽表导入源，每行必须声明真实来源、采集日期和来源类型。
- `docs/examples/valuation_snapshots/` 提供可复制模板，不代表真实交易建议。

当前基础版输出：

- `outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md`
- `outputs/reports/eodhd_earnings_trends_fetch_YYYY-MM-DD.md`
- `outputs/reports/fmp_analyst_history_validation_YYYY-MM-DD.md`
- `outputs/reports/valuation_validation_YYYY-MM-DD.md`
- `outputs/reports/valuation_import_YYYY-MM-DD.md`
- `outputs/reports/valuation_review_YYYY-MM-DD.md`

当前基础版校验：

- 估值快照必须包含来源类型、来源名称、日期和采集时间。
- FMP 拉取报告必须记录 endpoint、请求标的、provider symbol alias、下载时间、返回记录数和 checksum。
- FMP analyst history 校验必须检查原始 JSON schema、checksum、row_count、ticker、请求参数、日期和重复 estimate date。
- FMP 返回负数估值倍数时必须在拉取报告中记录警告，并跳过该指标，不写入会导致估值快照校验失败的负倍数字段。
- FMP `eps_revision_90d_pct` 必须读取接近 90 天前、同一 fiscal estimate date 的历史 `epsAvg`；历史不足时报告警告并跳过。
- EODHD baseline `eps_revision_90d_pct` 使用 `calendar/trends` 的 `epsTrendCurrent` 和 `epsTrend90daysAgo` 合并进当前可见基础估值快照；必须标记为 `captured_at_forward_only`，不能作为采集日前严格 PIT 回测输入。
- FMP `valuation_percentile` 使用本地 point-in-time 估值快照历史计算；每个 metric 至少需要 3 个历史点，历史不足时报告警告并跳过。
- 当日估值复核和日报评分只使用每个 ticker 截至评估日最新的可见快照；历史快照只用于分位计算和 point-in-time 回测，不能重复计入当日评分。
- CSV 导入必须通过必填列、日期、数值和来源类型校验，导入失败时不写入快照 YAML。
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
|`config/sec_companies.yaml`|SEC companyfacts ticker/CIK 映射；TSM quarterly 通过 TSM IR 合并补齐|阶段 2，已实现基础版|
|`config/fundamental_metrics.yaml`|SEC/TSMC IR taxonomy/concept/unit 到内部基本面指标的映射、支撑指标和显式派生规则|阶段 2，已实现基础版|
|`config/fundamental_features.yaml`|SEC 基本面特征公式和周期偏好|阶段 2，已实现基础版|
|`config/scoring_rules.yaml`|评分规则和权重|M2|
|`data/raw/download_manifest.csv`|下载审计清单，记录 provider、endpoint、请求参数、下载时间、行数和 checksum|M1，已实现基础版|
|`data/raw/sec_companyfacts/`|SEC companyfacts 原始 JSON 和下载 manifest|阶段 2，已实现基础版|
|`data/external/fundamentals/tsm_ir/`|TSMC IR 官方 PDF 和抽取后的 Management Report 文本审计证据|阶段 2，已实现基础版|
|`data/processed/sec_fundamentals_YYYY-MM-DD.csv`|SEC 基本面指标抽取结果，是日报 SEC 基本面评分的输入|阶段 2，已实现基础版|
|`data/processed/sec_fundamental_features_YYYY-MM-DD.csv`|SEC 基本面比率特征，是日报基本面硬数据评分的审计输出；回测会按 signal_date 在内存中生成 point-in-time 特征|阶段 2，已实现基础版|
|`data/processed/tsm_ir_quarterly_metrics.csv`|TSMC IR 官方 Management Report 季度指标导入缓存；可显式合并进统一 SEC-style 指标 CSV|阶段 2，已实现基础版|
|`data/processed/features_daily.csv`|每日特征|M1|
|`data/processed/scores_daily.csv`|每日评分|M2|
|`data/external/trade_theses/`|交易 thesis|M5，已实现基础版|
|`data/external/valuation_imports/`|估值、预期和拥挤度 CSV 导入源|M7，已实现基础版|
|`data/raw/fmp_analyst_estimates/`|FMP analyst-estimates 原始历史快照，用于 EPS revision 计算|M7，已实现基础版|
|`data/external/valuation_snapshots/`|估值、预期和拥挤度快照|M7，已实现基础版|
|`data/external/risk_event_imports/`|人工复核后的风险事件发生记录 CSV 导入源|M6，已实现基础版|
|`data/external/risk_event_occurrences/`|实际触发或观察中的政策/地缘风险事件发生记录|M6，已实现基础版|
|`data/external/trades/`|交易记录|M8，已实现基础版|
|`docs/examples/trade_theses/`|交易 thesis YAML 模板|M5，已实现基础版|
|`docs/examples/valuation_snapshots/`|估值快照 YAML 模板|M7，已实现基础版|
|`docs/examples/risk_event_occurrences/`|风险事件发生记录 YAML 模板|M6，已实现基础版|
|`docs/examples/trades/`|交易记录 YAML 模板|M8，已实现基础版|
|`outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md`|历史回测报告，包含绩效、执行成本摘要、数据质量门禁摘要、质量摘要、模块覆盖率摘要、月度覆盖率趋势、月度来源类型趋势、月度输入问题下钻、输入证据 URL 摘要、风险事件证据 URL 明细、ticker 输入摘要、ticker SEC 特征明细、估值快照来源和风险事件证据来源分布|阶段 1，已实现基础版|
|`outputs/backtests/backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv`|机器可读历史输入覆盖诊断，包含 component coverage、source_type、issue、source_url、ticker 输入、SEC 特征和风险事件证据 URL 记录|阶段 1，已实现基础版|
|`outputs/backtests/backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md`|中文回测输入审计报告，给出 PASS/PASS_WITH_WARNINGS/FAIL、审计发现和后续修复建议；可用 `--fail-on-audit-warning` 将非 PASS 状态作为命令失败|阶段 1，已实现基础版|
|`outputs/reports/daily_score_YYYY-MM-DD.md`|每日评分报告|M2|
|`outputs/reports/thesis_review_YYYY-MM-DD.md`|假设复核报告|M5|
|`outputs/reports/risk_events_validation_YYYY-MM-DD.md`|风险事件规则校验报告|M6|
|`outputs/reports/risk_event_occurrence_import_YYYY-MM-DD.md`|风险事件发生记录 CSV 导入报告|M6，已实现基础版|
|`outputs/reports/risk_event_occurrences_YYYY-MM-DD.md`|风险事件发生记录校验和复核报告|M6|
|`outputs/reports/valuation_import_YYYY-MM-DD.md`|估值 CSV 导入报告|M7，已实现基础版|
|`outputs/reports/valuation_validation_YYYY-MM-DD.md`|估值快照校验报告|M7|
|`outputs/reports/valuation_review_YYYY-MM-DD.md`|估值与拥挤度复核报告|M7|
|`outputs/reports/trade_review_YYYY-MM-DD.md`|复盘归因报告|M8|
|`outputs/reports/sec_fundamentals_YYYY-MM-DD.md`|SEC 基本面指标抽取报告，声明 SEC 缓存质量状态和缺失项|阶段 2，已实现基础版|
|`outputs/reports/sec_fundamentals_validation_YYYY-MM-DD.md`|SEC 基本面指标 CSV 校验报告，包含缺失观测清单|阶段 2，已实现基础版|
|`outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`|SEC 基本面特征摘要，声明指标 CSV 质量状态、特征公式和限制|阶段 2，已实现基础版|
|`outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md`|TSMC IR PDF 文本抽取报告，声明官方来源、输入 PDF、输出文本、页数、字符数和 checksum|阶段 2，已实现基础版|
|`outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md`|TSMC IR 季度基本面导入报告，声明官方来源、checksum、缺失项和限制|阶段 2，已实现基础版|
|`outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md`|FMP 估值/预期 API 拉取报告，记录 endpoint、请求标的、下载时间、row count、checksum 和字段限制|M7，已实现基础版|
|`outputs/reports/fmp_analyst_history_validation_YYYY-MM-DD.md`|FMP analyst-estimates 原始历史缓存校验报告|M7，已实现基础版|

## 近期最小落地路径

接下来建议按这个顺序开发：

1. 用 `aits valuation fetch-fmp` 为 AI 核心观察池生成第一批真实 paid vendor 估值快照，并检查 `valuation_validation_*.md` 的警告是否只来自预期内的估值历史样本不足或 EPS revision 历史窗口不足。
2. 在基础估值快照存在后运行 `aits valuation fetch-eodhd-trends`，用 EODHD Earnings Trends 先补当前日报的 `eps_revision_90d_pct` baseline；该结果只从采集日后可见。
3. 按 `DATA-003` 建设 forward-only PIT 快照归档，先把 analyst estimates、price target、ratings 和 earnings calendar 的 raw payload、manifest、checksum、parser version 与 `available_time` 留存下来；等自建 analyst-estimates 快照自然覆盖 90 天后，再评估严格采集日后可见的 `eps_revision_90d_pct` 是否开始稳定生成。
4. 按 `BACKTEST-002` 为回测报告增加 A/B/C 数据可信度标签和 0/1/3/5/10/20 交易日滞后敏感性，防止把供应商回填历史误写成严格 PIT 结论。
5. 为风险事件发生记录接入正式供应商或一手来源 API，并复用现有 CSV/YAML 的来源审计字段。
6. 用真实历史数据持续验证 `backtest_audit_*.md` 和输入覆盖诊断 CSV；如果审计报告长期需要按日下钻，再评估是否把覆盖诊断 CSV 拆分为更细的日频文件。

原因：

- 阶段 1 的市场数据、评分、观察池、回测、产业链配置、交易 thesis、风险事件分级、估值与拥挤度快照、交易复盘基础闭环，以及日报复核摘要集成已经完成。
- SEC 基本面特征、FMP/CSV 估值快照和风险事件发生记录已经接入当日日报评分与 point-in-time 回测；FMP analyst-estimates 已开始留存原始历史快照，后续可在 90 天窗口满足后生成 EPS revision；TSMC IR 季度指标也能从官方页面文本或 PDF 文本层导入并显式合并进统一基本面指标 CSV；SEC 指标校验已输出完整缺失观测清单，回测报告已能显示数据质量门禁摘要、模块覆盖率摘要、月度覆盖率趋势、月度来源类型趋势、月度输入问题下钻、月度输入证据 URL 摘要、月度风险事件证据 URL 明细、月度 ticker 输入摘要、ticker SEC 特征明细、月度估值快照来源和月度风险事件证据来源分布，并额外写出机器可读输入覆盖诊断 CSV 和中文输入审计报告。下一步的主要价值是减少风险事件人工数据搬运、观察 FMP 90 天历史窗口是否足够稳定，并用真实历史数据验证审计报告和覆盖诊断长表是否需要拆分。

## 不应马上做的事

- 不应直接做全市场选股。
- 不应直接接新闻并让 LLM 输出买卖建议。
- 不应在没有估值数据来源审计的情况下做估值自动评分。
- 不应在没有交易记录结构的情况下做复杂绩效归因。
- 不应把估值、政策地缘继续长期依赖无来源审计的手工输入；CSV 导入也必须保留 source_type、source_name、captured_at 和 checksum。
