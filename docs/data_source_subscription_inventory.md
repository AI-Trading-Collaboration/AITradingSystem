# 数据源订阅与权限盘点

最后更新：2026-06-28

本文件整理当前项目已配置、已审计或实际使用的数据源订阅情况。范围只包括数据源和数据处理服务的权限、用途边界、主要限制和后续 blocker；不改变 `config/data_sources.yaml`、数据流、评分、回测、paper-shadow、production weight 或 broker/order 行为。

## 依据

- Source catalog：`config/data_sources.yaml`
- 凭据模板：`.env.example`
- 当前进程环境变量 presence：2026-06-28 本机检查，只记录 true/false，不记录 secret 值
- 最近一次 entitlement probe：`outputs/data_quality/current_subscription_data_coverage/current_subscription_data_coverage_matrix.json`，生成时间 2026-06-21
- Source qualification 结果：`outputs/data_quality/current_subscription_source_qualification_batch/current_subscription_source_qualification_batch_review.json`，以及 FMP、Marketstack、data foundation、vendor gate 相关 artifacts

本次盘点没有重新运行 live provider entitlement probe，避免消耗供应商 quota。若订阅或 key 刚发生变更，应重新运行 `aits data source-qualification subscription-audit` 并复核输出。

## 总体结论

- 当前本机可见关键凭据：`FMP_API_KEY`、`MARKETSTACK_API_KEY`、`EODHD_API_KEY`、`FRED_API_KEY`、`BEA_API_KEY`、`BLS_API_KEY`、`ALPHA_VANTAGE_API_KEY`、`CONGRESS_API_KEY`、`GOVINFO_API_KEY`、`OPENAI_API_KEY`、`SEC_USER_AGENT`。
- 当前本机不可见：`FINANCIAL_MODELING_PREP_API_KEY`。FMP 实际使用 `FMP_API_KEY`，`FINANCIAL_MODELING_PREP_API_KEY` 只是 audit runner 支持的别名。
- 2026-06-28 最小服务端 key 验证：BEA `GETDATASETLIST` 返回 13 个 dataset；BLS v2 `LNS14000000` 2024 样本返回 `REQUEST_SUCCEEDED`；Alpha Vantage `GLOBAL_QUOTE` / `MSFT` 返回 `Global Quote`。
- 最近一次 subscription audit 覆盖 9 个 provider、29 个 endpoint；17 个 endpoint accessible，6 个 provider 有 key presence。
- TRADING-738 requirement match 结果为 true/unknown/false = 6/2/1，`requires_new_paid_source_count=1`。但后续 TRADING-759 已把 forward evidence archive 改判为 internal capture requirement，因此当前 vendor gate 结论是 `DO_NOT_BUY_NEW_SOURCE_YET`。
- 当前可进入 controlled research 的主价格源是 FMP；Marketstack 固定为 `LIMITED_SECOND_SOURCE_ONLY`；两者都没有解除 promotion、paper-shadow 或 production blocker。
- 所有 subscription / qualification artifacts 均为 validation-only / observe-only，固定 `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`。

## 当前凭据 Presence

|环境变量|2026-06-28 presence|关联来源|说明|
|---|---:|---|---|
|`FMP_API_KEY`|true|Financial Modeling Prep|默认主价格、估值、PIT forward snapshots 使用。|
|`FINANCIAL_MODELING_PREP_API_KEY`|false|Financial Modeling Prep|仅为 audit runner 兼容别名；当前项目实际默认读取 `FMP_API_KEY`。|
|`MARKETSTACK_API_KEY`|true|Marketstack|默认第二行情源；不允许作为 primary source。|
|`EODHD_API_KEY`|true|EODHD|用于 Earnings Trends baseline；部分价格/公司行动 endpoint 在上次 audit 中受 plan 限制。|
|`ALPHA_VANTAGE_API_KEY`|true|Alpha Vantage|2026-06-28 `GLOBAL_QUOTE` 最小验证通过；当前仍无生产用途，接入前需 source qualification。|
|`FRED_API_KEY`|true|FRED API / ALFRED 候选|当前 `fredgraph.csv` 日线宏观源不要求 key；API key 可支持 release/vintage 类候选。|
|`BEA_API_KEY`|true|BEA API|2026-06-28 `GETDATASETLIST` 最小验证通过；当前只作为免费/官方 macro calendar / release metadata 候选。|
|`BLS_API_KEY`|true|BLS Public Data API|2026-06-28 v2 time-series 最小验证通过；当前只作为免费/官方 macro release/value 候选，revision-sensitive 值仍需 PIT/vintage policy。|
|`CONGRESS_API_KEY`|true|Congress.gov|官方政策/法案监控。|
|`GOVINFO_API_KEY`|true|GovInfo|官方 Federal Register package metadata 校验；上次 audit endpoint HTTP 500。|
|`OPENAI_API_KEY`|true|OpenAI Responses API|官方候选风险事件 metadata/formal 预审；不是市场数据源。|
|`SEC_USER_AGENT`|true|SEC EDGAR|SEC fair access 所需身份；不是 API key。|

## Entitlement Probe 摘要

最近一次 live entitlement probe 为 2026-06-21，探测窗口主要是 2024-01-02 至 2024-01-05 的代表性样本，不能当作完整历史深度证明。

|Provider|Key presence / 订阅状态|Probe 结果|当前使用边界|主要 blocker|
|---|---|---|---|---|
|Financial Modeling Prep|`FMP_API_KEY` present；paid vendor active|价格 full/light/non-split-adjusted、dividends、splits、delisted、income statement 可访问；earnings calendar 和 S&P 500 constituents 受 plan/permission 限制|默认股票/ETF 主价格源；估值/预期快照来源；controlled research 主价格源允许|provider timestamp、conservative available-time、as-of/lineage owner review、delisted validation 仍阻断 promotion|
|Marketstack|`MARKETSTACK_API_KEY` present；Basic paid vendor active|EOD price、splits、dividends 可访问；ticker metadata / ETF holdings 返回 HTTP 404；TRADING-759 row snapshot coverage=0.125 已解释为 SPY-only probe scope|第二行情源、raw close reconciliation、adjusted close basis cross-check|最终角色 `LIMITED_SECOND_SOURCE_ONLY`；price/split/dividend discrepancy 仍阻断 promotion；不允许 primary source|
|EODHD|`EODHD_API_KEY` present；paid vendor active|EOD price、dividends、splits 受 plan/permission 限制；exchange symbol list 和部分 fundamentals 可访问；options diagnostic HTTP 404|Earnings Trends 当前 EPS revision baseline；fundamentals/asset-master 只能 diagnostic|不能替代真实 PIT estimates archive；价格/公司行动权限不足；current-view risk|
|Alpha Vantage|`ALPHA_VANTAGE_API_KEY` present；optional provider not active|2026-06-28 `GLOBAL_QUOTE` / `MSFT` key validation 通过；旧 2026-06-21 subscription audit 仍显示 key missing|无当前生产或研究关键路径用途|需要重跑 `subscription-audit` 才能更新正式 entitlement matrix；即便 endpoint 可访问也需 source qualification|
|FRED|`FRED_API_KEY` present；官方/免费来源|series observations 可访问，available-time supported|宏观利率、广义美元指数、release/calendar 候选|revision-sensitive 宏观值需要 vintage/ALFRED 审计，不能用 current view 代替 PIT|
|BEA|`BEA_API_KEY` present；官方/免费来源|2026-06-28 `GETDATASETLIST` 返回 13 个 dataset|GDP / NIPA / PCE release metadata 候选；当前不进入生产评分|release schedule 可作为 calendar risk；修订宏观值需要单独 vintage/PIT 审计|
|BLS|`BLS_API_KEY` present；官方/免费来源|2026-06-28 v2 time-series `LNS14000000` 返回 `REQUEST_SUCCEEDED`|CPI、employment、JOLTS 等官方宏观候选；当前不进入生产评分|release values 有 revision/current-view 风险；模型使用前需 PIT/vintage/known-at contract|
|Cboe|不需要 key；官方来源|VIX daily history 可访问|补 `^VIX` 到主价格缓存，供 risk sentiment / macro risk budget|只覆盖 VIX index OHLC，不覆盖 futures、skew、VVIX 或 option surface|
|SEC EDGAR|不需要 API key；需要 `SEC_USER_AGENT`|submissions/companyfacts 可访问；submissions 有 accepted-time contract|基本面一手来源、reconstructed filing-time PIT features|companyfacts 是重建 PIT，不是 strict vendor archive；必须保留 raw checksum 和 accepted/filed time gate|
|GovInfo|`GOVINFO_API_KEY` present；官方来源|上次 packages endpoint 返回 HTTP 500|Federal Register package metadata 校验候选|当前可用性需重跑确认；失败时只能显式跳过/警告，不能静默替代|
|Congress.gov|`CONGRESS_API_KEY` present；官方来源|bill search 可访问；likely use 为 `research_label_only`|AI chip / export control / trade policy 法案监控|research label only，不得直接作为 strategy input 或 promotion evidence|

## Source Catalog 订阅状态

|source_id|Provider|类型 / 状态|订阅或凭据|当前用途与边界|
|---|---|---|---|---|
|`fmp_eod_daily_prices`|Financial Modeling Prep|paid_vendor / active|`FMP_API_KEY` present|默认主价格源；写 `data/raw/prices_daily.csv`；controlled research 允许，promotion 仍 blocked。|
|`cboe_vix_daily_prices`|Cboe Global Markets|primary_source / active|无 key|补充 `^VIX`；不覆盖 VIX 衍生品曲面。|
|`yahoo_finance_daily_prices`|Yahoo Finance via yfinance|public_convenience / inactive|无 key|仅保留显式 `--price-provider yahoo` 诊断；默认生产主源已移除。|
|`marketstack_eod_daily_prices`|Marketstack|paid_vendor / active|`MARKETSTACK_API_KEY` present|第二行情源；`LIMITED_SECOND_SOURCE_ONLY`，不覆盖主缓存、不自动修正价格。|
|`fred_daily_rates`|FRED|primary_source / active|日线 CSV 无 key；`FRED_API_KEY` present|Treasury rates 和 Federal Reserve broad USD index；`DTWEXBGS` 不是 ICE DXY。|
|`fmp_valuation_expectations`|Financial Modeling Prep|paid_vendor / active|`FMP_API_KEY` present|估值、analyst estimates、price target、ratings、earnings calendar 和 forward-only PIT snapshots；不是 strict PIT vendor archive。|
|`eodhd_earnings_trends`|EODHD|paid_vendor / active|`EODHD_API_KEY` present|当前 EPS trend baseline；不替代 Intrinio/Zacks/FactSet 等真实 PIT estimates archive。|
|`sec_company_facts`|SEC EDGAR|primary_source / active|`SEC_USER_AGENT` present|SEC companyfacts 原始 JSON、指标抽取和基本面评分。|
|`sec_accession_filing_archive`|SEC EDGAR|primary_source / active|`SEC_USER_AGENT` present|submissions/accession metadata 与 accepted time traceability。|
|`sec_edgar_reconstructed_pit_features`|SEC EDGAR|primary_source / active|`SEC_USER_AGENT` present|B 级 reconstructed filing-time PIT fundamentals；strict vendor archive=false。|
|`tsm_investor_relations_quarterly_results`|TSMC Investor Relations|primary_source / active|无 key|补 TSM quarterly fundamentals；只接受官方页面/文本层/PDF 抽取链路。|
|`official_federal_register_policy_documents`|Federal Register API|primary_source / active|无 key|政策/地缘官方候选；进入人工/LLM 复核，不直接改变仓位。|
|`official_bis_federal_register_notices`|Federal Register API / BIS notices|primary_source / active|无 key|Entity List、EAR、advanced computing / AI chip export control 监控。|
|`official_ofac_sdn_xml`|OFAC Sanctions List Service|primary_source / active|无 key|SDN sanctions official bulk source。|
|`official_ofac_consolidated_xml`|OFAC Sanctions List Service|primary_source / active|无 key|非 SDN / CMIC / sectoral sanctions 监控。|
|`official_ustr_press_releases`|USTR|primary_source / active|无 key|Section 301、trade policy review 线索。|
|`official_trade_csl_json`|International Trade Administration|primary_source / active|无 key|Consolidated Screening List cross-check。|
|`official_congress_bills`|Congress.gov API|primary_source / active|`CONGRESS_API_KEY` present|官方法案监控；research_label_only。|
|`official_govinfo_federal_register`|GovInfo API|primary_source / active|`GOVINFO_API_KEY` present|Federal Register package metadata 校验；上次 live probe HTTP 500，需重跑确认。|
|`openai_llm_claim_prereview`|OpenAI|paid_vendor / active|`OPENAI_API_KEY` present|风险事件 metadata/formal 预审；输出 pending/review evidence，不单独触发交易动作。|
|`local_trade_theses`|Local YAML input|manual_input / active|无 key|交易 thesis 和 falsification 条件；缺失时不能报告高置信 thesis。|
|`local_valuation_snapshots`|Local YAML input|manual_input / active|无 key|手工估值/预期/拥挤度快照；自动评分前仍需合格来源与校验。|
|`structured_valuation_csv_imports`|Structured valuation CSV export|manual_input / active|无 key|减少 YAML 维护；每行仍必须声明真实 source。|
|`self_archived_pit_snapshots`|Local forward-only PIT snapshot archive|manual_input / active|无 key|本地 forward-only PIT raw snapshot manifest；只允许 `available_time <= decision_time` 查询。|
|`local_risk_event_occurrences`|Local YAML input|manual_input / active|无 key|已确认风险事件发生记录；日报不把监控规则当作发生事实。|
|`reviewed_risk_event_daily_attestations`|Human-reviewed attestation|manual_input / active|无 key|人工复核声明；系统不得自动代填。|
|`reviewed_risk_event_occurrence_csv_imports`|Human-reviewed event occurrence CSV|manual_input / active|无 key|风险事件结构化导入；L2/L3 仍需人工复核。|
|`local_trade_records`|Local YAML input|manual_input / active|无 key|交易复盘/归因输入。|
|`local_portfolio_positions`|Local CSV input|manual_input / active|无 key|只读持仓暴露分解；缺文件日报显示 `NOT_CONNECTED`。|
|`news_event_source_tbd`|To be selected|paid_vendor / planned|未选择 provider|付费新闻/事件源尚未确定；不能进入评分或自动交易。|

## 免费 / 官方 PIT 候选补充

`config/data/free_data_source_registry.yaml` 当前是 pilot baseline，安全边界为 research-only，`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。其中 FRED market series、Cboe VIX historical、Federal Reserve calendar、BLS release calendar、BEA release metadata、FRED release calendar、NY Fed calendar 等可作为免费/官方候选；revision-sensitive 宏观值必须有 vintage / ALFRED as-of 支持后才能升级为 model-ready。

## 当前采购判断

`outputs/controlled_strategy_research/data_vendor_decision_gate.json` 当前状态为 `DO_NOT_BUY_NEW_SOURCE_YET`。含义不是“现有数据源已满足 production”，而是：在现有 blocker mapping 下，还没有足够证据证明应立即新增付费来源。当前更优先的动作是继续修复/验证 source qualification、PIT/lineage、Marketstack second-source discrepancy、FMP promotion blockers 和 forward evidence internal capture。

## 复核建议

- 订阅、plan 或 API key 改变后，先运行 `aits data source-qualification subscription-audit`，再更新本文件。
- 默认日报/回测仍必须先通过 `aits validate-data` 或同源 validation code path；subscription accessible 不等于数据质量通过。
- 任何 provider 从 diagnostic / controlled research 升级到 promotion evidence、paper-shadow 或 production 前，必须有 source qualification artifact、lineage/PIT 证明、owner review 和 task-register 状态迁移。
