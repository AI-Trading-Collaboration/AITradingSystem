# ETF Portfolio Allocation System

状态：BASELINE_DONE
任务 ID：TRADING-062
最后更新：2026-05-31

## 背景

项目 owner 提供 `G:/Download/AITradingSystem_ETF_Portfolio_Development_Document.md`，要求把系统从以个股研究和趋势分析为主的链路，扩展为 ETF 主仓配置、趋势状态识别、模拟舱验证、组合级回测和权重治理闭环。

本实现必须适配现有仓库事实：

- 项目包名为 `ai_trading_system`，统一 CLI 为 `aits`。
- 现有系统已具备 `download-data`、`validate-data`、market features、daily score、backtest、shadow parameter、Dashboard、Reader Brief 和 report registry 等模块。
- 新 ETF 组合闭环先作为 P0 baseline 接入，不修改 `config/parameters/production/current.yaml`，不自动升级任何 production 权重或交易动作。

## 范围

### P0 Baseline

实现文档要求的最小可运行 ETF 组合系统：

1. ETF asset config、strategy config、risk config、backtest config 和 schema validation。
2. 本地 CSV/Parquet price adapter、可选 yfinance provider、标准价格 schema 和数据质量校验。
3. Feature store：收益、均线、均线斜率、实现波动率、回撤、成交量 z-score、相对强弱 ratio momentum。
4. ETF signal engine：trend、momentum、relative strength、risk、composite、direction、confidence、reason codes。
5. Regime engine：Risk-On、Neutral、Risk-Off、Shock-Recovery、Overheated。
6. Portfolio allocation + constraints：目标权重、cash absorb、regime constraints、asset/risk-group caps、rebalance delta。
7. Portfolio backtester：信号滞后一交易日执行、交易成本、buy-and-hold/static baselines、metrics、no-lookahead test。
8. Simulation ledger：daily snapshot、idempotent upsert、forward return evaluator、model_version 区分。
9. Daily Markdown portfolio brief。
10. `aits` CLI 子命令和 P0 tests。

### P1 Observe-Only Baseline

P1 不直接进入 production。本轮已按开发文档落地 observe-only 基础版：

- relative strength table 覆盖 ETF pairs、confirmation pairs 和 satellite stock / ETF pairs；
- AI / Semiconductor / MegaCap confirmation scores；
- satellite stock candidate evaluator，尊重 trend、relative strength、risk 和 regime satellite cap；
- portfolio attribution 基础字段：asset contribution、sleeve contribution、allocation effect、risk contribution 和 turnover contribution；
- experiment registry、model_version/config_hash 追踪、weight governance status；
- event calendar basic risk flag；
- P1 market-data-dependent reports 复用 ETF price quality gate，并在 Markdown 中披露 `data_quality_status` 和质量报告路径。

### P2 Observe-Only Baseline

P2 在开发文档中定义为“后续再实现”，且进入前置条件为 P0 完全通过、P1 核心模块稳定、模拟舱能记录和评估多个模型版本、回测无未来函数测试稳定通过。当前仍不得伪造 EDGAR/news/options/holdings/live broker 数据。

本轮继续推进 P2 的可审计底座，而不是接入未批准外部数据源：

- `config/etf_portfolio/p2.yaml` 记录每类 P2 数据源、schema、PIT timestamp 字段、缺失时的明确状态、candidate-only / read-only / no-broker 边界。
- `aits etf p2 derive-edgar-events` 可从现有 SEC PIT filing timeline 派生 EDGAR metadata event canonical input；该输入只表示 filing 元数据，`sentiment_score=0.0`，不做无来源文本情绪判断。
- `aits etf p2 fetch-edgar-text` 只能从 SEC filing timeline 中按 as-of / availability gate 选择有限候选，下载或读取官方 filing 文本，写入本地 text cache、document index 和 source manifest；该命令不做 LLM/情绪/投资结论解释。
- `aits etf p2 edgar-topics` 只能读取已缓存的 official filing text，并按 `p2.yaml` 的 topic keywords 输出 observe-only 主题计数；该命令不生成 sentiment、财务结论、权重或交易建议。
- `aits etf p2 derive-options-risk` 可从本地 `^VIX` 市场缓存派生 options risk proxy；该输入只覆盖 IV-rank proxy，明确保留 VXN/skew vendor 字段缺失状态，不伪装成完整期权数据。
- `aits etf p2 normalize-options-risk` 可将 vendor/manual options IV/VXN/skew CSV 规范化为 canonical `options_iv_skew.csv`；该命令只接受本地审计输入，不下载未批准 provider，并记录 manifest/checksum。
- `aits etf p2 normalize-holdings` 可将 issuer/vendor/manual holdings CSV 规范化为 canonical ETF holdings；`downloaded_at` 继续作为 PIT availability 字段，历史 as-of 若使用未来接收数据必须失败。
- `aits etf p2 normalize-news` 可将 vendor/manual news theme CSV 规范化为 canonical news theme events；缺少显式 sentiment 时只用 `p2.yaml` 的中性默认，并在 limitation 中披露，不调用 LLM 推断。
- `aits etf p2 news-themes` 在 canonical news input 存在时输出 observe-only 主题事件追踪，包括 symbol/theme event count、weighted sentiment、avg relevance、latest summary 和 PIT/data limitation；缺输入时仍显式 `MISSING_INPUT`。
- EDGAR / 财报文本解释、新闻主题追踪、期权 IV/skew、ETF holdings look-through 先接受本地审计 CSV/Parquet 输入；缺输入时输出 `MISSING_INPUT`，不补造结论。
- `aits etf p2 import-source` 将本地审计输入校验后写入 canonical CSV，并追加 `source_manifest.csv`，记录 provider、source URL、download timestamp、row count 和 checksum。
- `aits etf p2 weight-optimizer` 只能输出 candidate-only weight set，复用 ETF price quality gate 和 P0 signals，不写入 `target_weights.csv`，不替代正式 allocation。
- 高级风险模型使用已通过 ETF price gate 的缓存和目标权重生成 observe-only 风险诊断。
- walk-forward / ML ranking / ensemble 只生成 candidate 或 readiness 报告，不写 production 参数。
- 多账户或实盘交易接口只实现 read-only preflight / policy report，默认 `enabled=false`、`paper_only=true`、`broker_routing_allowed=false`，不得下单。

后续最佳方案仍是为 P2 各数据源接入真实 provider/API、PIT availability、source manifest、质量门禁和 observe-only 报告，再由 owner 决定 provider/API 权限、成本、延迟和 production 边界。

## 设计决策

- 使用现有 `aits` CLI，不引入新顶层包 `aitrading`。
- ETF 默认入口保持 `aits etf ...`；为兼容开发文档的 P0 workflow 示例，提供根级 compatibility aliases，但不覆盖现有主系统 `aits backtest`。
- P0 ETF 组合模块放在 `src/ai_trading_system/etf_portfolio/`，减少对现有个股评分主线的扰动。
- 输出默认使用中文报告；标准字段、ticker、model_version、config_hash 等兼容性标识保留英文。
- 所有投资解释相关阈值进入 `config/etf_portfolio/*.yaml`，并带 policy metadata。
- ETF 数据依赖必须显式经过数据质量门禁；从缓存生成 features/signals/backtests/reports 的命令必须先调用同一校验代码路径并在产物中记录 quality status/report。
- 默认 market regime 为 `ai_after_chatgpt`，结论窗口从 2022-12-01 开始；更早数据仅用于 warm-up、压力测试或 regime 对照。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|Phase 0 仓库审计与实施说明|DONE|确认现有 CLI、配置、数据、回测、报告入口；创建或更新 `IMPLEMENTATION_NOTES.md`。|
|Phase 1 配置与 schema|DONE|ETF config 可加载并校验 default weights、asset min/default/max、CASH、risk regimes。|
|Phase 2 价格数据层|DONE|本地价格标准化、质量校验、fixture 数据可运行；缺关键字段 fail closed。|
|Phase 3 Feature Store|DONE|rolling 特征符合 no-lookahead，持久化输出稳定。|
|Phase 4 Signal Engine|DONE|score clamp、reason codes、direction/confidence、composite weights 正确。|
|Phase 5 Regime Engine|DONE|Risk-On/Risk-Off/Shock-Recovery/Overheated/Neutral fixtures 通过。|
|Phase 6 Allocation & Constraints|DONE|target weights sum=1，CASH 吸收未分配，Risk-Off cash_min 生效。|
|Phase 7 Backtester|DONE|一交易日 execution lag、transaction cost、baselines、toy return 和 no-lookahead 测试通过。|
|Phase 8 Simulation Ledger|DONE|record idempotent、future returns 不足为 null、model_version 可区分。|
|Phase 9 Daily Report|DONE|portfolio brief 生成并包含 regime、signals、weights、risk constraints、simulation。|
|Phase 10 P0 全流程验证|DONE|目标 pytest、`aits etf run daily --date latest --dry-run`、`aits etf backtest run --fast`、全量 pytest/ruff/compileall 通过。|
|Phase 11 P1 baseline|DONE|相对强弱报告、AI/半导体确认分、卫星股候选、组合归因、实验登记、权重治理状态和事件风险 flag 已实现；全部 observe-only。|
|Phase 12 P2 observe-only contracts|DONE|新增 P2 配置、schema、canonical import/source manifest、缺失输入显式报告、advanced risk、walk-forward readiness、ML candidate ranking、ensemble 和 live preflight；全部 production_effect=none。|
|Phase 13 P2 EDGAR local PIT adapter|DONE|从 `data/processed/sec_edgar/filing_timeline.csv` 派生 `data/etf_portfolio/p2/edgar_text_events.csv`，保留 SEC source URL、available time、row checksum 和 source manifest；报告为 `PASS_WITH_LIMITATIONS`，不做文本情绪推断。|
|Phase 14 P2 VIX options-risk proxy adapter|DONE|从 `data/raw/prices_daily.csv` 的 `^VIX` close series 派生 `data/etf_portfolio/p2/options_iv_skew.csv`，保留 ETF data quality gate、VIX source quality、row checksum 和 source manifest；报告为 `PASS_WITH_LIMITATIONS`，明确缺 VXN/skew vendor 字段。|
|Phase 15 P2 ETF holdings source normalizer|DONE|将 issuer/vendor/manual holdings CSV 规范化为 `data/etf_portfolio/p2/etf_holdings.csv`，保留 provider/source URL/download timestamp/row checksum，并继续由 `holdings-lookthrough` 输出 observe-only 报告；未批准实时 issuer API 前不自动下载。|
|Phase 16 P2 news theme source normalizer|DONE|将 vendor/manual news theme CSV 规范化为 `data/etf_portfolio/p2/news_theme_events.csv`，保留 provider/source URL/published_at/available_at/row checksum；缺 sentiment 时只用配置中性默认并披露 limitation，不调用 LLM。|
|Phase 17 P2 EDGAR full-text fetch/cache baseline|DONE|新增 `aits etf p2 fetch-edgar-text`，复用 `filing_timeline.csv` 的 PIT availability、限制 symbol/form/limit、要求 SEC User-Agent、记录 text checksum / document index / manifest；真实 smoke 获取 NVDA 2026-05-21 可见 10-Q 文本 1 份，仍不做财报解释、情绪分数或投资结论。|
|Phase 18 P2 EDGAR text topic audit baseline|DONE|新增 `aits etf p2 edgar-topics`，关键词主题由 `p2.yaml` 治理，读取 cached official filing text 输出 topic counts / matched keywords / limitation；真实 NVDA 10-Q smoke 输出 4 个 topic rows，仍不生成 sentiment、财务结论、权重或交易建议。|
|Phase 19 P2 ML weight optimizer baseline|DONE|新增 `aits etf p2 weight-optimizer`，参数由 `p2.yaml` 治理，输入经过 ETF data quality gate，输出只读候选权重、data quality、config hash 和 limitation；真实 2026-05-29 smoke 输出 5 行候选权重，未写入 P0 target weights 或交易动作。|
|Phase 20 P2 news theme tracking baseline|DONE|将 `aits etf p2 news-themes` 从 source contract 升级为有输入时的主题事件追踪报告；保留 PIT gate、source limitation、candidate-only / production_effect=none 边界，不调用 LLM、不生成交易建议；fixture smoke 输出 NVDA/SMH 两行主题追踪。|
|Phase 21 P2 options IV/VXN/skew source normalizer|DONE|新增 `aits etf p2 normalize-options-risk`，将 vendor/manual options risk CSV 规范化为 canonical `options_iv_skew.csv`：支持 IV rank、skew z-score、VXN level、PIT available time、source URL、row checksum 和 source manifest；不下载 provider、不把 VIX proxy 伪装成完整 options feed。|
|Phase 22 ETF brief report registry / Reader Brief visibility|DONE|把 ETF portfolio brief、ETF data quality、ETF backtest summary 和 ETF P2 walk-forward readiness 纳入 `config/report_registry.yaml`、Reader Brief navigation/freshness 和 artifact catalog；只读展示已有 artifact，不重跑 ETF 上游、不改变正式权重或交易动作。|
|Phase 23 ETF CLI compatibility aliases|DONE|补齐接近开发文档的顶层 alias：`aits data ingest/validate`、`aits features build`、`aits signals generate`、`aits regime generate`、`aits portfolio allocate`、`aits simulation ...`、`aits report daily`、`aits run daily` 指向 ETF workflow；根级 `backtest` 保留给现有主系统，ETF 回测继续使用 `aits etf backtest ...`。|
|Phase 24 README operator documentation|DONE|补齐 README ETF 运行说明：默认 `aits etf ...` namespace、compatibility aliases、P0/P1/P2 命令、artifact 路径、数据质量门禁、`ai_after_chatgpt` 默认窗口、`production_effect=none` / no-broker 边界和外部数据源限制。|
|Phase 25 P1 experiment run/compare registry|DONE|补齐 `aits etf experiments run --config <candidate.yaml>` 和 `aits etf experiments compare --baseline production`；只读记录候选配置 hash/diff、可选 backtest summary metrics 和对 production baseline 的 delta，不写正式配置、不自动 promotion。|
|Phase 26 simulation benchmark outcome fields|DONE|补齐 simulation ledger 后验字段 `relative_return_vs_spy_20d`、`relative_return_vs_qqq_20d`、`weight_contribution_20d` 和组合级 portfolio-vs-benchmark 摘要；未来窗口不足保持 null。|
|Phase 27 backtest artifact and benchmark completeness|DONE|补齐原文 10.1/10.5/10.7 要求的 ETF backtest 权重历史、交易历史、资产级收益贡献、`metrics.json` 别名和 QQQ 50/200 MA 基准；继续披露 no-lookahead / data quality / AI regime 口径。|
|Phase 28 daily brief simulation performance integration|DONE|补齐原文 12.2 Daily Report 的 Simulation Performance：日报应读取 simulation ledger，展示 model_version 级 hit rate 和 portfolio-vs-benchmark 摘要；缺 ledger 或窗口不足时明确说明。|
|Phase 29 simulation record date CLI compatibility|DONE|补齐原文 11.4 / 16.6 的 `simulation record --date latest` 语义：record 应能按 allocation date 选择 latest 或指定日期，缺日期 fail closed。|
|Phase 30 features/backtest CLI option compatibility|DONE|补齐原文 16.2 / 16.5 的 `features build --end latest` 和 ETF backtest `--config` 选项；根级 `aits backtest` 仍保留主系统语义。|

## 开放问题

- ETF 主流程是否加入全局 `ops daily-run` 自动链。当前保持 `aits etf ...` / compatibility alias 手动入口隔离；Reader Brief/report registry 只读可见性已接入，不自动运行 ETF 上游。
- P2 真实 provider/live 集成仍需 owner/API/PIT policy 决策：live news vendor feed / LLM sentiment、完整 EDGAR 财报解释策略、真实 VXN/skew vendor feed、实时 issuer holdings API、多账户或实盘 broker route。当前实现提供 source contract、canonical normalizer、bounded official EDGAR text cache、topic audit、observe-only reports、candidate-only optimizer/ensemble 和 read-only live preflight，不伪造这些外部能力。

## 进展记录

- 2026-05-31：新增任务和需求文档，状态 IN_PROGRESS。已审计现有项目结构、CLI 入口、配置文件、数据质量门禁、market feature 与 backtest 模块；决定先实现隔离的 ETF P0 baseline，避免扰动现有 production 参数。
- 2026-05-31：P0 baseline 完成并验证。新增 `config/etf_portfolio/*.yaml`、`src/ai_trading_system/etf_portfolio/`、`aits etf ...` CLI、ETF daily report、simulation ledger、backtest output 和 12 项 P0 测试；验证通过目标 pytest、受影响回归、full pytest 1600 passed、ruff 和 compileall。继续进入 P1 observe-only baseline。
- 2026-05-31：P1 observe-only baseline 完成。新增 `config/etf_portfolio/p1.yaml`、relative strength/confirmation/satellite/attribution/event/governance/experiment CLI 与 4 项 P1 单测；P1 产物固定 `production_effect=none`，不修改 P0 target weights、production 参数或 broker/trading 状态。
- 2026-05-31：开始 P2 observe-only contracts。目标是把 EDGAR/news/options/holdings/live 等高级输入先变成有 schema、有缺失状态、有报告、有安全边界的工程底座；不接入未批准 provider，不用临时数据替代真实 PIT 数据。
- 2026-05-31：P2 observe-only contracts 完成。新增 `config/etf_portfolio/p2.yaml`、`aits etf p2 ...` 命令组、canonical import/source manifest、EDGAR/news/options/holdings source contract reports、advanced risk、walk-forward readiness、ML ranking candidate、ensemble candidate 和 live preflight；缺外部输入输出 `MISSING_INPUT`，live/broker 输出 `BLOCKED_BY_POLICY`，全部 `production_effect=none`。
- 2026-05-31：P2 EDGAR local PIT adapter 完成。新增 `aits etf p2 derive-edgar-events`，从既有 SEC PIT filing timeline 生成 `edgar_text_events.csv` 和 source manifest；2026-05-29 派生 6952 行，`p2 edgar-text` 状态为 `PASS_WITH_LIMITATIONS`，`sentiment_score` 固定中性，后续真实文本解释仍需 provider / policy 决策。
- 2026-05-31：P2 VIX options-risk proxy adapter 完成。新增 `aits etf p2 derive-options-risk`，从本地 `^VIX` 价格缓存生成 `options_iv_skew.csv` 和 source manifest；2026-05-29 派生 8336 行，`p2 options-risk` 状态为 `PASS_WITH_LIMITATIONS`，`iv_rank` 使用 `p2.yaml` 阈值，VXN/skew 字段显式为空并在报告中声明限制。
- 2026-05-31：P2 ETF holdings source normalizer 完成。新增 `aits etf p2 normalize-holdings`，支持常见 `Ticker` / `Weight (%)` 等 issuer CSV 列名和显式列名覆盖，输出 canonical holdings/source manifest；smoke 使用 fixture 写入 `artifacts/etf_portfolio/p2_holdings_normalize_smoke/`，证明 PIT gate 会拒绝未来 `downloaded_at`，带正确 `downloaded_at` 后 `holdings-lookthrough` 输出 `PASS_WITH_LIMITATIONS`。
- 2026-05-31：P2 news theme source normalizer 完成。新增 `aits etf p2 normalize-news`，支持常见 `Ticker` / `Topic` / `Headline` / `Published` / `provider_available_at` 列名，输出 canonical news/source manifest；smoke 写入 `artifacts/etf_portfolio/p2_news_normalize_smoke/`，`news-themes` 输出 `PASS_WITH_LIMITATIONS` 并披露 neutral sentiment/default relevance limitation。
- 2026-05-31：P2 EDGAR full-text fetch/cache baseline 完成。新增 `aits etf p2 fetch-edgar-text` 和 `fetch_edgar_text_documents_from_timeline`，按 timeline 的 `available_for_signal_date` / `available_time_utc` 做 PIT gate，支持 `--symbol`、`--filing-type` 和 `--limit`，HTTP 获取要求 `SEC_USER_AGENT` 或 `--user-agent`；真实 smoke 生成 `data/etf_portfolio/p2/edgar_text_documents.csv` 和 `data/etf_portfolio/p2/edgar_text_documents/NVDA_10-Q_0001045810-26-000052_7eee9866ed6e.txt`，报告 `FETCHED`、`production_effect=none`。
- 2026-05-31：P2 EDGAR text topic audit baseline 完成。新增 `edgar_text_analysis` policy config 和 `aits etf p2 edgar-topics`，对 cached official filing text 输出 `ai_demand`、`export_controls`、`supply_constraints`、`capex_investment` topic counts；报告固定 `candidate_only=true`、`auto_promotion=false`、`production_effect=none`，并声明不做 sentiment、财务解释或投资结论。
- 2026-05-31：P2 ML weight optimizer baseline 完成。新增 `weight_optimizer` policy config、`build_weight_optimizer_candidates` 和 `aits etf p2 weight-optimizer`；报告复用 ETF price quality gate，输出 SPY/QQQ/SMH/SOXX/CASH 候选权重、component score、history days、data quality、config hash 和 limitation，固定 `candidate_only=true`、`auto_promotion=false`、`production_effect=none`。
- 2026-05-31：P2 news theme tracking baseline 完成。新增 `news_themes.tracking_lookback_days` / `max_report_rows` / candidate-only policy，`aits etf p2 news-themes` 在 canonical input 存在时输出 symbol/theme event count、weighted sentiment、avg relevance、latest summary、source limitation 和 observe-only 边界；fixture smoke 写入 `artifacts/etf_portfolio/p2_news_tracking_smoke/`。
- 2026-05-31：P2 options IV/VXN/skew source normalizer 开始。目标是让 owner 或 vendor 提供的本地审计 options CSV 可进入 canonical `options_iv_skew.csv`，同时继续保留当前 VIX proxy 的限制披露。
- 2026-05-31：P2 options IV/VXN/skew source normalizer 完成。新增 `normalize_options_risk_source` 和 `aits etf p2 normalize-options-risk`，支持常见 `Ticker` / `As Of` / `provider_available_at` / `IV Rank` / `Skew Z` / `VXN Level` 列名、显式列名覆盖、provider/source URL/downloaded_at/row checksum/source manifest；fixture smoke 写入 `artifacts/etf_portfolio/p2_options_normalize_smoke/`，`options-risk` 输出 `PASS_WITH_LIMITATIONS` 且 numeric summary 包含 IV、skew、VXN。
- 2026-05-31：ETF brief report registry / Reader Brief visibility 开始。目标是让 `reports/etf_portfolio/*_portfolio_brief.md`、ETF data quality、ETF backtest summary 和 P2 readiness 报告进入现有 `aits reports index` / Reader Brief report navigation，而不把 ETF daily run 自动塞入主 daily-run 调度链。
- 2026-05-31：ETF brief report registry / Reader Brief visibility 完成。`config/report_registry.yaml` 新增 `etf_portfolio_brief`、`etf_data_quality`、`etf_backtest_summary`、`etf_p2_walk_forward`；`reports index` 支持从 ETF backtest run 目录名解析 compact run date；`docs/artifact_catalog.md` 和 `docs/system_flow.md` 已同步。Smoke：`aits reports index --as-of 2026-05-29` 显示 ETF brief/data quality 为 `FRESH`，`aits reports reader-brief --date 2026-05-29` 的 report navigation 中出现 ETF 条目，Reader Brief quality 为 `OK`。
- 2026-05-31：ETF CLI compatibility aliases 开始。目标是让原文档中的 `data ingest`、`features build`、`signals generate`、`regime generate`、`portfolio allocate`、`simulation ...`、`report daily`、`run daily` 有顶层兼容入口，同时不覆盖现有根级 `backtest` 主系统命令。
- 2026-05-31：ETF CLI compatibility aliases 完成。新增 `aits data ingest/validate`、`aits features build`、`aits signals generate`、`aits regime generate`、`aits portfolio allocate`、`aits simulation record/evaluate/report`、`aits report daily` 和 `aits run daily`，均复用 `aits etf ...` 实现；`aits backtest --help` 仍为主系统每日评分回测。Smoke：`aits run daily --date latest --dry-run` 输出 2026-05-29 ETF brief 且 `data_quality_status=PASS`。
- 2026-05-31：ETF README operator documentation 开始。目标是补齐原文 Definition of Done 中 “README 说明如何运行” 的交付，覆盖 ETF workflow、artifact、数据质量门禁和安全边界。
- 2026-05-31：ETF README operator documentation 完成。README 新增 ETF 主仓组合配置系统运行说明、P0/P1/P2 命令、compatibility alias、artifact 路径、`ai_after_chatgpt` 窗口、数据质量门禁、Reader Brief 可见性和 `production_effect=none` / no-broker 边界；新增测试锁定关键 README 语义。
- 2026-05-31：ETF P1 experiment run/compare registry 开始。目标是补齐原文 P1 Experiment Registry 的 `experiments run` / `experiments compare` 入口，但保持 candidate-only / observe-only，不写正式策略或 target weights。
- 2026-05-31：ETF P1 experiment run/compare registry 完成。新增 `aits etf experiments run/compare` 和根级 `aits experiments run/compare/register` compatibility alias；experiment run 记录候选 YAML hash、参数 diff、可选 backtest summary metrics、manual-review 状态和 `production_effect=none`，compare 输出 observe-only metric delta 或 `MISSING_METRICS`，不写正式配置、target weights 或 promotion。
- 2026-05-31：ETF simulation benchmark outcome fields 开始。目标是补齐原文 Simulation Ledger 后验字段和 simulation report 的 portfolio vs SPY/QQQ 摘要，同时保持 forward window 不足时 null。
- 2026-05-31：ETF simulation benchmark outcome fields 完成。`aits etf simulation evaluate` 现在写入 20d SPY/QQQ-relative return、weight contribution 和组合级 portfolio-vs-benchmark 字段；`aits etf simulation report` 新增 Portfolio vs Benchmarks 摘要。Smoke 使用 2026-04-01 historical ledger 样本生成有效 20d outcome，默认 latest ledger 因未来窗口不足保持 null；目标测试、ruff、compileall、diff check 和全量 pytest 1627 passed。
- 2026-05-31：ETF backtest artifact and benchmark completeness 开始。审计原文 10.1、10.5、10.7 后发现当前 ETF backtest 只有 `daily.csv` 和 summary，缺权重历史、交易历史、资产级贡献、`metrics.json` 别名和 `ma_50_200_qqq` 基准；本阶段补齐这些审计产物，不改变 production 参数或交易动作。
- 2026-05-31：ETF backtest artifact and benchmark completeness 完成。`aits etf backtest run` 现在输出 `daily.csv`、`weights.csv`、`trades.csv`、`summary.json`、`metrics.json` 和 `summary.md`；daily rows 包含 `asset_returns_json` / `asset_contributions_json`，summary/metrics 包含扩展指标、benchmark relative return 和 `ma_50_200_qqq`。Smoke：`aits etf backtest run --fast --output-dir artifacts/etf_portfolio/backtest_artifact_smoke` 生成完整 run 目录；目标测试、ruff、compileall、diff check 和全量 pytest 1627 passed。
- 2026-05-31：ETF daily brief simulation performance integration 开始。审计原文 12.2 后发现 Daily Portfolio Brief 虽有 Simulation Performance 小节，但 CLI 日报未读取 simulation ledger，只输出占位文案；本阶段接入真实 ledger 摘要，保持缺数据显式说明。
- 2026-05-31：ETF daily brief simulation performance integration 完成。新增 `summarize_simulation_for_brief`，`aits etf report daily` / `aits etf run daily` 的 Simulation Performance 小节读取 ledger 并展示 model-version records、20d hit rate、avg portfolio return 和 relative vs SPY/QQQ；缺 ledger、空 ledger、窗口不足或 benchmark 缺失保持明确 `n/a` / no-record。Smoke：`aits etf report daily --date latest --output-path artifacts/etf_portfolio/daily_brief_simulation_summary_smoke.md` 显示默认 ledger 摘要；目标测试、ruff、compileall、diff check 和全量 pytest 1628 passed。
- 2026-05-31：ETF simulation record date CLI compatibility 开始。审计原文 11.4 / 16.6 和 README 后发现文档命令为 `simulation record --date latest`，但当前 CLI 未暴露 `--date`，只能记录 allocation 文件中的全部行；本阶段补齐 latest/指定日期选择和缺日期 fail-closed。
- 2026-05-31：ETF simulation record date CLI compatibility 完成。`aits etf simulation record --date latest` 和根级 `aits simulation record --date latest` 现在按 allocation 最新日期记录；指定日期只记录对应 allocation rows，缺 date 字段或缺指定日期 fail closed，重复运行保持 upsert。Smoke 写入 `artifacts/etf_portfolio/simulation_record_date_smoke/` 与 root alias smoke；目标测试、ruff、compileall、diff check 和全量 pytest 1629 passed。
- 2026-05-31：ETF features/backtest CLI option compatibility 开始。审计原文 16.2 / 16.5 后发现 `features build --end latest` 当前会按日期解析失败，且 ETF backtest run 缺 `--config` 选项；本阶段补齐 ETF namespace 兼容参数，根级 `aits backtest` 继续保留现有主系统命令。
- 2026-05-31：ETF features/backtest CLI option compatibility 完成。`aits features build --end latest` 现在使用 ETF price cache 最新日期作为 feature end；`aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` 读取指定 ETF backtest policy，仍不覆盖根级主系统 `aits backtest`。Smoke 生成 `artifacts/etf_portfolio/features_end_latest_smoke.csv` 和 `artifacts/etf_portfolio/backtest_config_smoke/`；目标测试、ruff、compileall、diff check 和全量 pytest 1630 passed。
- 2026-05-31：ETF 组合配置系统 baseline 覆盖审计完成，需求文档状态改为 `BASELINE_DONE`。P0/P1/报告/CLI/回测/模拟舱/Reader Brief 可见性均有实现和验证；P2 外部实时数据和 broker route 仅保持 observe-only/read-only 安全边界，剩余升级依赖 owner/provider/API/PIT policy 决策。
