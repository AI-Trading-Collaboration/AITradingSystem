# 系统架构

## 目标

本系统服务于 AI 产业链仓位管理，而不是自动化交易。核心输出是：

- 交易是否处在能力圈内。
- 信息影响了哪个产业链节点。
- 系统当前的只读认知状态，包括市场状态、产业链节点状态、thesis 状态、风险边界、置信度和改变判断的条件。
- 当前风险环境评分。
- AI 产业链目标仓位区间，包括股票风险资产内口径和总资产口径。
- 原始交易假设是否仍然成立。
- 相比上期的变化原因。
- 回测中的收益、回撤、换手与基准对比。
- 交易复盘和收益归因。

## 分层

```text
数据层 -> 数据质量门禁 -> 特征层 -> 产业链/假设层 -> 认知状态层 -> 评分层 -> 仓位层 -> 复盘层 -> 报告/仪表盘层
```

认知状态层对应规划中的 `belief_state`，第一版只作为解释和审计层，不直接改变生产评分、`position_gate`、回测仓位或交易建议。详细设计见 `docs/requirements/cognitive_model_2026-05-04.md`。

详细的数据输入、中间评估、回测和输出结论流程见 `docs/system_flow.md`。该图是工程事实的一部分，后续改变命令、配置、缓存、门禁、评分、回测或报告结构时必须同步维护。

系统产品原则见 `docs/product_strategy.md`。后续模块设计不能只围绕技术指标扩张，必须保留能力圈、产业链因果、仓位约束和复盘归因这四条主线。

默认策略解释区间是 `ai_after_chatgpt`：锚定 `2022-11-30` ChatGPT 公开发布，默认回测起点为 `2022-12-01`。2019 年以来的数据可用于跨周期压力测试，但报告必须把这种长窗口和默认 AI 主线行情区间区分开。

## 数据层

第一期只接入低复杂度数据：

- 价格：SPY、QQQ、SMH、SOXX、AI 产业链龙头。
- 核心个股观察池：MSFT、GOOG、TSM、INTC、AMD、NVDA。
- 风险情绪：VIX。
- 宏观利率：美国 2 年期、10 年期国债收益率。
- 可选：美元指数、TLT。

财报、估值、政策/地缘发生记录、新闻与事件抽取放到第二期和第三期。其中 SEC 基本面、估值快照和手工审计的风险事件发生记录已经接入基础评分；估值快照已支持 FMP paid vendor API 拉取和结构化 CSV 导入，风险事件发生记录已支持结构化 CSV 导入，以减少 YAML 手工维护；新闻/NLP 和 LLM 自动抽取仍未接入交易评分。

数据源目录配置为 `config/data_sources.yaml`，校验命令为 `aits data-sources validate`。该目录记录每个来源的 provider、endpoint、缓存路径、审计字段、校验项、来源类型和限制说明。新增正式财报、估值、新闻或事件来源前，必须先补充该目录并通过校验。

本地缓存先采用 CSV：

- `prices_daily.csv`：标准化后的日线 OHLCV 数据。
- `rates_daily.csv`：长表格式的 FRED 利率数据。
- `download_manifest.csv`：追加式下载审计清单，记录 provider、endpoint、请求参数、下载时间、行数、输出路径和 sha256。

数据下载命令为 `aits download-data`。默认只抓核心观察池，`--full-universe` 才抓完整 AI 产业链配置，避免 MVP 阶段数据面过宽。

基本面一手数据第一步接入 SEC EDGAR companyfacts。公司映射配置为 `config/sec_companies.yaml`，下载命令为 `aits fundamentals download-sec-companyfacts`。该命令要求显式提供 `--user-agent` 或 `SEC_USER_AGENT`，输出原始 JSON 到 `data/raw/sec_companyfacts/` 并写入 `sec_companyfacts_manifest.csv`。缓存校验命令为 `aits fundamentals validate-sec-companyfacts`，检查 JSON、CIK、taxonomy 和 checksum。指标映射配置为 `config/fundamental_metrics.yaml`，抽取命令为 `aits fundamentals extract-sec-metrics`；该命令会先执行 SEC 缓存质量门禁，通过后输出 `data/processed/sec_fundamentals_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamentals_YYYY-MM-DD.md`。显式派生指标必须写在配置中，并且要求组件指标的周期、单位、截止日、财年、财期和 accession number 一致。`aits fundamentals validate-sec-metrics` 会检查 CSV schema、重复键、未来披露日期、数值合法性和配置覆盖率，并把缺失观测结构化为 `ticker / metric_id / period_type` 清单，供回测月度输入问题下钻复用。TSM 季度指标可通过 `aits fundamentals fetch-tsm-ir-quarterly` 从 TSMC Investor Relations 官方季度页面发现并下载 Management Report 文本；如果官方资源是 PDF 或二进制，需要先用 `aits fundamentals extract-tsm-ir-pdf-text` 从本地官方 PDF 的文本层生成可审计文本，再用 `aits fundamentals import-tsm-ir-quarterly --filed-date YYYY-MM-DD` 导入。`filed_date` 是 Management Report 公开/披露日期，历史回测按 `signal_date` 选择当时最新已披露季度；采集时间 `captured_at` 只作为本地审计时间，不能替代公开日期。历史季度回填可用 `aits fundamentals import-tsm-ir-quarterly-batch` 读取 manifest CSV，字段为 `fiscal_year,fiscal_period,source_url,input_path,filed_date`，相对 `input_path` 按 manifest 所在目录解析；重复季度、缺文件、非官方 URL 或任一季度解析错误会让整批失败并停止写入。PDF 抽取报告记录官方 URL、PDF 路径、输出文本路径、抽取时间、页数、字符数和 checksum；扫描件或无文本层 PDF 会停止并要求 OCR 或人工抽取，不能生成伪文本。TSM IR 指标写入 `data/processed/tsm_ir_quarterly_metrics.csv`，并生成 `outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md`、`outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md` 和 `outputs/reports/tsm_ir_quarterly_batch_YYYY-MM-DD.md`；`aits fundamentals merge-tsm-ir-sec-metrics` 会按评估日期选择最新已披露 TSM IR 季度，把收入、毛利、营业利润、净利、研发和 CapEx 显式合并到统一 SEC-style 指标 CSV，只替换重复 TSM quarterly 键，并复用 SEC 指标 CSV 校验报告；`aits backtest` 也会读取同一 TSM IR 季度缓存，按每个 `signal_date` 补齐 point-in-time TSM quarterly metrics。金额单位保留 Management Report 披露尺度，例如 `TWD_billions` 或 `USD_billions`。不能用半年度 6-K 临时拆分替代季度数据。基本面特征配置为 `config/fundamental_features.yaml`，构建命令为 `aits fundamentals build-sec-features`；该命令会先复用 SEC 指标 CSV 校验门禁，再输出 `data/processed/sec_fundamental_features_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`。比率特征如果分子/分母周期、单位或披露来源不一致，会记录覆盖警告并跳过该特征；分母非正数仍作为错误停止，避免生成不稳定比率。`aits score-daily` 也会复用同一条 SEC 指标 CSV 校验和特征构建路径，校验失败时停止日报评分，通过后把 SEC 特征接入基本面硬数据评分。季度 CapEx 强度需要多期共同周期对齐，当前只生成年度 CapEx 强度。

## 数据质量门禁

所有评分、日报和回测在读取缓存数据前，都应先通过 `aits validate-data`。质量门禁检查：

- 文件存在性、字段结构和 CSV 可读性。
- `date+ticker`、`date+series` 唯一性。
- 配置中核心标的和利率序列是否完整。
- 日期格式、未来日期和数据新鲜度。
- OHLC 合法性、价格非正值、负成交量。
- 调整收盘价异常跳变和可疑单日波动。
- 利率值合理区间和可疑单日变化。

校验阈值放在 `config/data_quality.yaml`，避免把投资含义写死在代码里。校验失败时命令返回非零退出码，并生成 Markdown 报告供排查。

## 特征层

第一期特征保持可解释：

- 价格相对 20/50/100/200 日均线。
- SMH/SPY、QQQ/SPY 相对强弱。
- VIX 当前值与历史分位。
- 10 年期收益率短期变化。
- 龙头股趋势一致性。

特征构建命令为 `aits build-features`。命令必须先通过数据质量门禁，输出：

- `data/processed/features_daily.csv`
- `outputs/reports/feature_summary_YYYY-MM-DD.md`

特征使用 tidy 格式存储，核心字段为 `as_of,source_date,category,subject,feature,value,unit,lookback,source,notes`，方便后续评分模块按 feature 名称读取。

## 产业链和假设层

后续需要建立产业链因果图，而不是只按 ticker 或新闻来源组织信息：

```text
云厂商 CapEx -> GPU/ASIC 需求 -> HBM 需求 -> 先进封装需求 -> 晶圆代工需求 -> 设备与材料需求
```

每条基本面、新闻或财报信息都应尽量映射到产业链节点，并判断它影响的是短期情绪还是长期现金流预期。

重要交易需要记录 thesis，包括买入理由、验证指标、证伪条件、目标周期、仓位理由和风险事件处理规则。

观察池和能力圈配置已落在 `config/watchlist.yaml`。校验命令为 `aits watchlist validate`，用于确保核心个股都在活跃观察池中、每个核心标的都有 AI 产业链节点映射，并且高风险标的必须要求交易 thesis。

产业链因果图配置已落在 `config/industry_chain.yaml`。校验命令为 `aits industry-chain validate`，用于确保节点 ID 唯一、父节点存在、因果图无环、每个节点有领先指标和相关标的，并且观察池引用的产业链节点都存在。产业链节点在基础版中不直接触发交易动作，只作为信息映射、假设验证和后续基本面/事件评分的结构基础。

交易 thesis 基础版已支持 `data/external/trade_theses/*.yaml`。校验命令为 `aits thesis validate`，复核命令为 `aits thesis review`。当前版本不自动判断 thesis 对错，而是检查结构、观察池引用、产业链节点、验证指标、证伪条件、复核新鲜度和已触发风险，确保主动交易假设可审计、可复盘。

风险事件分级基础版已落在 `config/risk_events.yaml`。校验命令为 `aits risk-events validate`，用于确保 L1/L2/L3 等级、AI 仓位折扣、人工复核要求、影响产业链节点、相关标的、建议动作、升级条件和解除条件都可审计。这个配置只代表“需要监控的规则”，不代表风险已经发生。实际发生记录读取 `data/external/risk_event_occurrences/*.yaml`，命令为 `aits risk-events list-occurrences` 和 `aits risk-events validate-occurrences`；日报政策/地缘评分只读取已通过校验的 active 发生记录。保守 source policy 下 `S/A` 级证据可支持普通评分和仓位闸门，`B` 级只支持普通评分，`C/D/X`、`watch` 或 `public_convenience` 单源只能进入报告和人工复核。

估值与拥挤度基础版读取 `data/external/valuation_snapshots/*.yaml`。FMP 接入命令为 `aits valuation fetch-fmp`，会从 `quote-short`、`key-metrics-ttm`、`ratios-ttm` 和 annual `analyst-estimates` 生成 `paid_vendor` 快照，并输出 `outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md` 记录 endpoint、请求标的、provider symbol alias、下载时间、row count、checksum、历史 analyst 快照读取数、本地估值历史读取数和字段限制；API key 只从 `FMP_API_KEY` 环境变量读取，不写入报告。内部核心观察池保留 `GOOG`，FMP 请求参数使用显式 alias `GOOG -> GOOGL`，生成的估值快照仍归属内部 ticker `GOOG`。FMP 返回负数估值倍数时不会写入快照字段，而是在拉取报告中记录 provider 值不可用警告。每次成功拉取会把原始 `analyst-estimates` 响应写入 `data/raw/fmp_analyst_estimates/`，`aits valuation validate-fmp-history` 会校验原始 JSON 的 schema、checksum、row_count、ticker、请求参数、日期和重复 estimate date；通过后才能可靠用于 FMP 本地历史口径的 `eps_revision_90d_pct`。`valuation_percentile` 使用本地 point-in-time 估值快照历史计算，每个 metric 至少需要 3 个历史点；如果缺少真实历史快照，可先运行 `aits valuation fetch-fmp-valuation-history` 从 FMP historical `key-metrics` / `ratios` 拉取历史 `ev_sales` 和 `peg` 分布，原始响应写入 `data/raw/fmp_historical_valuation/`，生成的历史估值快照 `captured_at` 固定为采集日，因此历史回测在采集日前不可见。这是当前供应商历史接口回填，不等同于真实 point-in-time vendor archive，不能用于伪造 `eps_revision_90d_pct`。EODHD baseline 命令为 `aits valuation fetch-eodhd-trends`，读取 `calendar/trends` 的 `epsTrendCurrent` 和 `epsTrend90daysAgo`，原始响应写入 `data/raw/eodhd_earnings_trends/`，再把当前 EPS 90 日修正合并进当前可见基础估值快照；合并快照标记为 `captured_snapshot`、`vendor_current_trend` 和 `captured_at_forward_only`，估值倍数、估值分位和拥挤度继承基础快照，不由 trends 推断。该 baseline 只能改善当前日报覆盖，不能替代 Intrinio/Zacks、FactSet 或等价真实 PIT estimates archive。校验命令为 `aits valuation validate`，复核命令为 `aits valuation review`；当前评分和复核按 `as_of/captured_at` 只选择每个 ticker 最新可见快照，历史快照保留给分位计算和 point-in-time 回测，不会重复计入当日日报评分。当前版本不从网页或 LLM 自动抽取估值结论；手工录入、正式披露或付费供应商快照都必须带来源、日期、采集时间和字段说明。公开便利源只能作为人工备注或辅助证据，不能直接进入自动评分。FMP 的 `forward_pe` 由当前 quote 与最近未来 annual EPS estimate 计算，`revenue_growth_next_12m_pct` 是 annual estimate 代理口径。

## 评分层

沿用规划文档里的 100 分框架：

|模块|权重|
|---|---:|
|趋势|25|
|基本面|25|
|宏观流动性|15|
|风险情绪|15|
|估值|10|
|政策/地缘|10|

当前基础版已实现趋势、SEC 基本面、宏观流动性、风险情绪四类硬数据，并接入估值快照和政策/地缘发生记录的手工/审计输入评分。基本面评分第一版只使用已通过校验的 AI 核心观察池 SEC 特征中位数，包括季度毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度；估值评分第一版只使用已通过校验、未过期、且不是 `public_convenience` 来源的估值快照，按估值分位和过热快照比例打分；政策/地缘评分第一版只读取已通过校验、来源合格且证据等级为 `S/A/B` 的 active 发生记录，按 L2/L3 数量和最低 AI 仓位乘数打分；`B` 级记录只能影响普通评分，仓位闸门只读取 `S/A` 且 `action_class=position_gate_eligible` 的 active 记录。阈值写在 `config/scoring_rules.yaml`，用于先形成可审计规则，不把单家公司估值、财报解读或新闻判断硬编码进代码。

每日评分命令为 `aits score-daily`。命令会先执行市场数据质量门禁并构建市场特征，再校验 SEC 指标 CSV、构建 SEC 基本面特征，之后校验估值快照和风险事件发生记录，并汇总交易 thesis、风险事件、估值快照和交易复盘状态，最后输出：

- `data/processed/scores_daily.csv`
- `outputs/reports/daily_score_YYYY-MM-DD.md`

评分报告必须区分硬数据信号、部分硬数据、手工/审计输入、历史不足、占位输入和人工复核摘要。基本面只有在 SEC 指标 CSV 校验和 SEC 特征构建通过后才能显示为硬数据评分；估值只有在快照校验通过且来源合规时才能参与评分；政策/地缘只有在发生记录校验通过且证据来源合规时才能参与评分。

## 仓位映射

|总分|AI 仓位（股票风险资产内）|
|---:|---:|
|80-100|80%-100%|
|65-80|60%-80%|
|50-65|40%-60%|
|35-50|20%-40%|
|0-35|0%-20%|

系统同时输出总资产口径：

```text
AI 仓位（总资产内） = 股票/风险资产预算（总资产内） * AI 仓位（股票风险资产内）
```

股票/风险资产预算先由 `config/portfolio.yaml:portfolio` 给出静态区间，再由
`config/portfolio.yaml:macro_risk_asset_budget` 在 VIX、DGS10 利率变化或 DXY
走强触发宏观流动性压力时下调。该层只影响总风险资产预算；AI 在风险资产内部的相对权重仍由评分模型和 `position_gate` 单独解释。

## 回测层

回测必须至少输出：

- CAGR
- Max Drawdown
- Sharpe Ratio
- Sortino Ratio
- Calmar Ratio
- Turnover
- Time in Market
- 相对 SPY、QQQ、SMH/SOXX 的超额收益

回测默认计入单边交易成本，可通过 `--slippage-bps` 显式加入线性滑点或盘口冲击估算；后续再加入税费、汇率、融资利率、容量约束和基金申赎延迟。

基础版命令为 `aits backtest`。当前实现会先执行市场数据质量门禁和 SEC companyfacts 缓存校验，然后按每日评分输出的总资产内 AI exposure 区间中点进行动态仓位回测；每日明细同时保留风险资产内 AI 相对权重、宏观调整后总风险资产预算和实际总资产内 AI exposure。每个 signal_date 会重新从 SEC companyfacts 中抽取 `filed_date <= signal_date` 的指标并构建 point-in-time 基本面特征；估值快照会按 `as_of/captured_at <= signal_date` 过滤并保留每个 ticker 最新快照；风险事件发生记录会按当时可见证据和 resolved_at 重建 active/watch 状态，避免把当前估值或当前风险事件结论带入历史回测。信号在收盘后生成、下一交易日收益生效，避免未来函数。默认市场阶段来自 `config/market_regimes.yaml`，当前为 `ai_after_chatgpt`，起点 `2022-12-01`；`cross_cycle_stress` 保留为 2019 年以来的非默认压力测试。默认策略代理标的是 `SMH`，默认基准为 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有。

回测报告还会输出执行成本摘要、数据质量门禁摘要、评分模块覆盖率摘要、按月聚合的模块覆盖率趋势、按月聚合的来源类型趋势、月度输入问题下钻、月度输入证据 URL 摘要、月度风险事件证据 URL 明细、月度 ticker 输入摘要、月度 ticker SEC 特征明细、月度估值快照来源和月度风险事件证据来源分布，并同步写出 `backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv` 机器可读覆盖诊断和 `backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md` 中文输入审计报告。覆盖诊断用于区分策略表现变化、输入覆盖缺口变化、评分来源口径变化、单票输入覆盖、单票 SEC 特征覆盖、风险事件证据来源覆盖和具体校验问题；审计报告给出 PASS/PASS_WITH_WARNINGS/FAIL、审计发现和修复建议，判断本次回测是否可解释；`--fail-on-audit-warning` 可把非 PASS 审计状态作为命令失败。数据质量门禁摘要会直接列出错误/警告计数、价格/利率/下载 manifest 行数、日期范围和 checksum。覆盖率低于 100%、来源类型包含 `partial_hard_data`、`partial_manual_input`、`insufficient_data`、`placeholder`，月度输入问题下钻出现 SEC/估值/风险事件 issue，估值或风险事件长期依赖 `public_convenience`，或 URL 摘要显示来源过窄的月份，需要在解释回测结论时单独查看对应切片质量摘要。

## 复盘归因层

系统需要区分市场上涨带来的 Beta 和自身判断带来的 Alpha。第一版归因可以先使用 SPY、QQQ、SMH/SOXX 作为基准，后续扩展到：

- 市场 Beta。
- 行业 Beta。
- 主题 Beta。
- 个股 Alpha。
- 仓位和交易纪律影响。

交易复盘基础版命令为 `aits review-trades`，默认读取 `data/external/trades/*.yaml`。该命令依赖缓存行情数据，因此必须先通过数据质量门禁；通过后用交易记录的 entry/exit 价格计算交易收益，并与同区间 `SPY`、`QQQ`、`SMH`、`SOXX` 调整收盘价收益对比。当前归因是规则化摘要，不等同于完整因子归因。

## 报告层

第一期先生成每日 Markdown 报告，等规则稳定后再做 Streamlit 仪表盘。报告应包含：

- 本期建议仓位。
- 上期建议仓位。
- 股票风险资产内 AI 仓位。
- 总资产内 AI 仓位。
- 评分变化。
- 加仓/减仓触发项。
- 下一期观察条件。
