# 系统数据流示意图

本文档是系统从数据输入、中间评估到输出结论的流程图。它不是一次性说明文档，而是工程事实的一部分：后续新增命令、数据源、配置、评分模块、回测路径或报告输出时，必须同步维护本文件。

第一次理解系统时，优先阅读 `docs/learning_path.md`；如果需要从零理解输入数据如何计算成输出数据，先读 `docs/calculation_logic.md`；看到具体 CSV、JSON、Markdown 或 HTML 产物时，优先查 `docs/artifact_catalog.md`。本文继续保留全链路事实和维护边界。

结构化重构的模块边界见 `docs/architecture/module_boundaries.md`，workflow / artifact / `production_effect` 运行契约见 `docs/architecture/workflow_contract.md`。这些契约先服务于 daily-run manifest 和后续 workflow 分层，不改变本图描述的业务流向。

![AI Trading System 数据流总览](assets/system_flow_overview.svg)

## 维护边界

必须更新本文件的情况：

- 新增、删除或改名 CLI 命令。
- 新增、删除或改名关键配置文件。
- 改变 `data/raw`、`data/processed`、`outputs/reports`、`outputs/runs`、`outputs/backtests` 的核心文件结构。
- 改变数据质量门禁位置、通过条件或失败后的停止行为。
- 改变评分模块、仓位映射、回测默认市场阶段或报告结论结构。
- 接入或改变交易 thesis、风险事件、估值、新闻、认知状态、复盘归因等模块。

不需要更新本文件的情况：

- 不改变外部行为的内部重构。
- 不改变字段含义、命令输入输出或报告解释的性能优化。
- 单元测试、类型标注、格式化等纯工程维护。

## 总览

```mermaid
flowchart TD
    subgraph Source["数据输入"]
        U["config/universe.yaml<br/>标的池、基准、FRED 宏观序列"]
        P["config/portfolio.yaml<br/>风险资产预算、仓位上限和 risk_budget gate 参数"]
        Q["config/data_quality.yaml<br/>质量阈值 + 价格一致性窗口 + FRED series 级 freshness<br/>指数 volume / 已知拆股 / corporate-action window / 二源自检规则"]
        F["config/features.yaml<br/>特征窗口和相对强弱组合"]
        FAVC["config/feature_availability.yaml<br/>PIT feature availability 目录<br/>event_time / available_time / decision_time"]
        S["config/scoring_rules.yaml<br/>评分模块、signal 规则、score->position band、日报结论 policy、confidence/source-type policy、仓位动作阈值和 position_gates 上限<br/>legacy weights 只作未接入 resolver 时的 fallback"]
        WPC["config/weights/weight_profile_current.yaml<br/>生产评分/回测基础权重 profile、bounds、score-point confidence delta 语义"]
        SWPC["config/weights/shadow_weight_profiles.yaml<br/>隔离测试权重 profile：production_effect=none，用于长期观察和主线评分对比"]
        SGPC["config/weights/shadow_position_gate_profiles.yaml<br/>隔离测试 hard gate / confidence / risk budget cap：production_effect=none"]
        SPSC["config/weights/shadow_parameter_search_space.yaml<br/>validation-only shadow weight/gate 参数搜索空间"]
        SPOC["config/weights/shadow_parameter_objective.yaml<br/>shadow 参数搜索目标函数、样本门槛、生产邻近性 regularization 和 top-N 策略"]
        SPPC["config/weights/shadow_parameter_promotion_contract.yaml<br/>shadow 参数 search 与 production 晋级分层 contract；production_effect=none"]
        WPM["config/weights/calibration_protocol.yaml<br/>调权实验 protocol manifest：数据、成本、执行、切分、trial 和 benchmark"]
        FSP["config/feedback_sample_policy.yaml<br/>市场反馈优化样本政策：reporting / pilot / diagnostic / promotion floor"]
        BVP["config/backtest_validation_policy.yaml<br/>回测数据可信度覆盖率、稳健性默认实验参数、解释阈值、candidate 多目标 veto 阈值和 promotion gate policy"]
        PGM["config/parameter_governance.yaml<br/>可调参数面治理 manifest：source level、owner 输入状态、证据要求、行动边界和 production_effect=none"]
        W["config/watchlist.yaml<br/>观察池与能力圈<br/>decision_stage: watch_only / active_trade"]
        WL["config/watchlist_lifecycle.yaml<br/>观察池 point-in-time 生命周期"]
        I["config/industry_chain.yaml<br/>产业链节点与因果图"]
        R["config/market_regimes.yaml<br/>AI regime 与压力测试区间"]
        BPC["config/benchmark_policy.yaml<br/>AI proxy 与 benchmark 解释口径"]
        SCC["config/scenario_library.yaml<br/>AI 产业链情景压力测试库"]
        CTC["config/catalyst_calendar.yaml<br/>未来催化剂日历和事件前/后复核要求"]
        EPC["config/execution_policy.yaml<br/>advisory execution action taxonomy 和执行纪律"]
        GOVC["config/rule_cards.yaml<br/>production / candidate / retired rule cards"]
        RE["config/risk_events.yaml<br/>L1/L2/L3 风险事件动作规则"]
        REX["data/external/risk_event_occurrences/*.yaml<br/>已触发/观察的风险事件发生记录<br/>S/A/B/C/D/X、严重性、概率、动作等级、lifecycle_state、dedup_group、expiry_time"]
        REXATT["data/external/risk_event_occurrences/review_attestation_*.yaml<br/>人工复核声明：覆盖窗口、复核人、来源范围和下次复核"]
        REXCSV["data/external/risk_event_imports/*.csv<br/>人工复核后的风险事件发生记录导入表"]
        RPRCSV["data/external/risk_event_prereview_imports/*.csv<br/>OpenAI 结构化预审结果导入表"]
        OPSRC["Federal Register / BIS / OFAC / USTR / Congress.gov / GovInfo / Trade.gov CSL<br/>低成本官方政策/地缘来源"]
        LLMI["docs/examples/llm_claim_prereview/*.yaml<br/>LLM claim 预审输入：source_id、source URL、采集时间和待发送内容级别"]
        ME["data/external/market_evidence/*.yaml<br/>新市场信息证据账本"]
        MECSV["data/external/market_evidence_imports/*.csv<br/>人工复核或 LLM 分类后的 evidence 导入表"]
        DS["config/data_sources.yaml<br/>数据源目录、审计字段、来源限制和 provider LLM 权限"]
        SEC["config/sec_companies.yaml<br/>SEC CIK、taxonomy 预期和指标周期"]
        FM["config/fundamental_metrics.yaml<br/>SEC 指标映射、支撑指标和派生规则"]
        FF["config/fundamental_features.yaml<br/>SEC 基本面特征公式和周期偏好"]
        TSMPDF["TSMC IR Management Report PDF<br/>官方季度资料 PDF"]
        TSMTXT["TSMC IR Management Report 文本<br/>官方季度资料的已抽取文本"]
        TSMMAN["TSMC IR 批量导入 manifest CSV<br/>季度、官方 URL 和本地文本路径"]
        TH["data/external/trade_theses/*.yaml<br/>主动交易假设、验证指标、证伪条件<br/>created_at / status_updated_at / nested updated_at<br/>watch_only 标的不要求 thesis"]
        VS["data/external/valuation_snapshots/*.yaml<br/>估值、预期、拥挤度快照<br/>PIT 可信度和回测用途"]
        VSCSV["data/external/valuation_imports/*.csv<br/>结构化估值/预期导入表"]
        TD["data/external/trades/*.yaml<br/>交易记录、价格、thesis_id<br/>recorded_at / updated_at / opened_at / closed_at"]
        POS["data/external/portfolio_positions/current_positions.csv<br/>真实账户持仓快照<br/>ticker、市值、AI 暴露、节点/地区/因子/相关性标签"]
        MD["外部数据源<br/>FMP / Cboe VIX / Marketstack / FRED<br/>暂无新增 macro/price source 计划"]
        YFD["Yahoo Finance<br/>public_convenience 诊断性第三来源<br/>production_effect=none"]
        FMP["Financial Modeling Prep API<br/>historical-price-eod/non-split-adjusted + dividend-adjusted / quote / TTM metrics / ratios / estimates<br/>price target / ratings / earnings calendar<br/>provider symbol alias 可审计记录"]
        CBOEVIX["Cboe VIX official historical data<br/>VIX_History.csv<br/>^VIX OHLC / no volume"]
        EODHDT["EODHD Earnings Trends API<br/>calendar/trends<br/>epsTrendCurrent / epsTrend90daysAgo"]
        PITMAN["data/raw/pit_snapshots/manifest.csv<br/>forward-only PIT raw snapshot manifest<br/>available_time / checksum / row count"]
    end

    subgraph Cache["本地缓存"]
        ERC["data/raw/external_request_cache/<provider>/<api_family>/<cache_key>/<br/>response.body + metadata.json<br/>外部供应商请求级缓存：HIT 不再请求供应商；MISS 才发送并归档<br/>API key / token / Cookie / User-Agent 脱敏"]
        DL["aits download-data"]
        PR["data/raw/prices_daily.csv<br/>FMP 股票/ETF + Cboe ^VIX 主价格缓存"]
        MSPR["data/raw/prices_marketstack_daily.csv<br/>Marketstack 第二行情源<br/>cross-provider reconciliation"]
        RR["data/raw/rates_daily.csv<br/>FRED DGS2 / DGS10 / DTWEXBGS"]
        DM["data/raw/download_manifest.csv<br/>provider / endpoint / 参数 / checksum"]
        DLF["outputs/reports/download_data_diagnostics_YYYY-MM-DD.md<br/>download-data 失败诊断<br/>provider / cache status / cache key / 脱敏请求参数 / 下游影响"]
        SFD["aits fundamentals download-sec-companyfacts"]
        SFV["aits fundamentals validate-sec-companyfacts"]
        SFJ["data/raw/sec_companyfacts/*.json"]
        SFM["data/raw/sec_companyfacts/sec_companyfacts_manifest.csv"]
        SFSD["aits fundamentals download-sec-submissions"]
        SFSJ["data/raw/sec_submissions/*.json<br/>filing history / accepted time metadata"]
        SFAD["aits fundamentals download-sec-filing-archive"]
        SFAJ["data/raw/sec_filings/<ticker>/<accession>/index.json<br/>accession directory raw index"]
        SFAC["aits fundamentals sec-accession-coverage"]
        SFACR["outputs/reports/sec_accession_coverage_YYYY-MM-DD.md"]
        SFVR["outputs/reports/sec_companyfacts_validation_YYYY-MM-DD.md"]
        SFE["aits fundamentals extract-sec-metrics"]
        SFVC["aits fundamentals validate-sec-metrics"]
        SFC["data/processed/sec_fundamentals_YYYY-MM-DD.csv"]
        SFR["outputs/reports/sec_fundamentals_YYYY-MM-DD.md"]
        SFCR["outputs/reports/sec_fundamentals_validation_YYYY-MM-DD.md"]
        SFF["aits fundamentals build-sec-features"]
        SFFC["data/processed/sec_fundamental_features_YYYY-MM-DD.csv"]
        SFFR["outputs/reports/sec_fundamental_features_YYYY-MM-DD.md"]
        TSMP["aits fundamentals extract-tsm-ir-pdf-text"]
        TSMF["aits fundamentals fetch-tsm-ir-quarterly"]
        TSMI["aits fundamentals import-tsm-ir-quarterly"]
        TSMIB["aits fundamentals import-tsm-ir-quarterly-batch"]
        TSMM["aits fundamentals merge-tsm-ir-sec-metrics"]
        TSMC["data/processed/tsm_ir_quarterly_metrics.csv"]
        TSMPR["outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md"]
        TSMR["outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md"]
        TSMBR["outputs/reports/tsm_ir_quarterly_batch_YYYY-MM-DD.md"]
        FMPH["data/raw/fmp_analyst_estimates/<ticker>/*.json<br/>FMP analyst estimates 原始历史快照<br/>文件名含 captured_at / downloaded_at / checksum，避免覆盖 PIT manifest payload"]
        FMPVH["data/raw/fmp_historical_valuation/*.json<br/>FMP historical key-metrics/ratios 原始响应"]
        FMPFP["data/raw/fmp_forward_pit/*.json<br/>FMP forward-only PIT raw archive"]
        FMPFPN["data/processed/pit_snapshots/fmp_forward_pit_YYYY-MM-DD.csv<br/>FMP as-of 标准化索引<br/>normalized_id 使用有界 ASCII slug + checksum"]
        FMPFPR["outputs/reports/fmp_forward_pit_fetch_YYYY-MM-DD.md<br/>FMP PIT 抓取报告"]
        EODHDTR["data/raw/eodhd_earnings_trends/*.json<br/>EODHD Earnings Trends 原始响应"]
        PITF["aits pit-snapshots fetch-fmp-forward<br/>抓取 FMP estimates / price target / ratings / earnings calendar"]
        PITB["aits pit-snapshots build-manifest<br/>从现有 FMP/EODHD raw cache 建立通用 PIT manifest"]
        PITV["aits pit-snapshots validate<br/>校验 PIT manifest / payload checksum / available_time"]
        PITR["outputs/reports/pit_snapshots_validation_YYYY-MM-DD.md<br/>PIT 快照归档质量报告"]
        OPRAW["data/raw/official_policy_sources/YYYY-MM-DD/*<br/>官方来源 raw payload、row count 和 sha256"]
        OPCAND["data/processed/official_policy_source_candidates_YYYY-MM-DD.csv<br/>pending_review 人工复核候选；production_effect=none"]
        OPTRI["data/processed/official_policy_candidate_triage_YYYY-MM-DD.csv<br/>AI 模块相关性 triage；降低无明显联系候选优先级；production_effect=none"]
    end

    subgraph Gate["数据质量门禁"]
        V["aits validate-data<br/>schema / completeness / freshness / duplicate keys / suspicious values<br/>按 consistency_start_date 执行价格波动/复权、宏观变化和 Marketstack reconciliation<br/>可解释项转 INFO；已知拆股窗口日期口径差异可归因；主源和 raw close 未解决冲突仍 fail closed"]
        QR["outputs/reports/data_quality_YYYY-MM-DD.md<br/>+ data_quality_YYYY-MM-DD_marketstack_reconciliation.csv<br/>问题表标注价格主源 / 第二行情源 / 跨源核验 / FRED / manifest 来源"]
        YDG["aits data-sources yahoo-price-diagnostic<br/>只对 Marketstack self-check 异常 ticker/date 拉取 Yahoo raw OHLC<br/>不写主缓存、二源缓存、评分或回测真值"]
        YDR["outputs/reports/yahoo_price_diagnostic_YYYY-MM-DD.md<br/>provider、endpoint、request params、row count、checksum、FMP/Marketstack/Yahoo 对比"]
        Stop["错误时停止后续评分、特征、回测或报告"]
    end

    subgraph Feature["中间评估：市场特征"]
        BF["aits build-features"]
        FT["data/processed/features_daily.csv"]
        FR["outputs/reports/feature_summary_YYYY-MM-DD.md"]
        FAVR["outputs/reports 或 outputs/backtests/feature_availability_YYYY-MM-DD.md<br/>PIT 特征可见时间规则和 source 覆盖"]
    end

    subgraph Score["中间评估：评分和仓位"]
        SD["aits score-daily<br/>默认运行官方来源抓取 + OpenAI 预审 + LLM formal 写入<br/>可用 --skip-risk-event-openai-precheck 跳过<br/>可选 --run-id 贯穿 trace / Decision Card"]
        EWR["effective weight resolver<br/>weight_profile + approved overlay + signal context + as_of<br/>输出 effective_weights / overlay ids / confidence_delta / position_multiplier"]
        MBG["macro_risk_asset_budget<br/>VIX、DGS10、DTWEXBGS 广义美元指数触发总风险资产预算下调"]
        PG["position_gate<br/>评分仓位、判断置信度、组合限制、风险预算、风险事件、估值拥挤、thesis 和数据置信度取最严格上限<br/>输出 gate_class / target_effect / execution_effect 审计"]
        CONF["判断置信度<br/>按模块来源、覆盖率、质量门禁和人工复核汇总<br/>生成 confidence position gate"]
        FST["关注股票趋势分析<br/>core_watchlist ticker 的 1/5/20 日收益 + MA20/50/100/200 位置<br/>production_effect=none"]
        NH["产业链节点热度与健康度<br/>industry_chain/watchlist + 市场特征 + 基本面/估值/风险/thesis 复核<br/>watch_only 缺 thesis 不触发 gate；production_effect=none"]
        PEX["组合暴露分解<br/>真实持仓 CSV + industry_chain/watchlist 映射<br/>production_effect=none"]
        PER["outputs/reports/portfolio_exposure_YYYY-MM-DD.md<br/>ticker / node / region / customer chain / factor / correlation cluster"]
        SC["data/processed/scores_daily.csv<br/>模块分、整体分、confidence、仓位区间、effective weights 和 gate 摘要"]
        EADV["执行建议<br/>execution_policy + 最终仓位变化 + confidence/gate<br/>production_effect=none"]
        EPR["outputs/reports/execution_policy_YYYY-MM-DD.md<br/>动作词表校验和问题清单"]
        DR["outputs/reports/daily_score_YYYY-MM-DD.md<br/>Decision Card v2：Data Gate、Run ID / Trace、Main Invalidator、Next Checks<br/>Data Lineage Card、Score-to-Position Funnel、Binding Gate Ladder、复核五问<br/>Base Signal / Risk Caps、结论使用等级、变化原因树、关注股票趋势、认知状态、执行建议和仓位闸门"]
        DSNAP["data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json<br/>当日判断快照、score_architecture、risk lifecycle 和 belief_state_ref"]
        PLED["data/processed/prediction_ledger.csv<br/>append-only production/challenger prediction ledger"]
        BS["data/processed/belief_state/belief_state_YYYY-MM-DD.json<br/>只读认知状态"]
        BSH["data/processed/belief_state_history.csv<br/>只读认知状态历史索引"]
        DRT["outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json<br/>claim / evidence / dataset / quality / run_id / run manifest / belief_state / rule_versions"]
    end

    subgraph Backtest["历史回测"]
        BT["aits backtest"]
        BTG["aits backtest-input-gaps"]
        BTPC["aits backtest-pit-coverage<br/>forward-only PIT 覆盖持续验证"]
        BPCR["outputs/backtests/backtest_pit_coverage_YYYY-MM-DD.md<br/>B/A readiness、历史 C 级原因和升级日期"]
        BWATCH["point-in-time 观察池<br/>按 signal_date 过滤 lifecycle 可见 ticker"]
        BSEC["point-in-time SEC 基本面特征<br/>按 signal_date 只读已披露 companyfacts 与 TSM IR"]
        BVAL["point-in-time 估值快照<br/>按 signal_date 过滤 as_of/captured_at"]
        BRISK["point-in-time 风险事件发生记录和复核声明<br/>按 signal_date 过滤证据、resolved_at 和 reviewed_at"]
        BIG["outputs/backtests/backtest_input_gaps_YYYY-MM-DD_YYYY-MM-DD.md<br/>历史估值/风险事件输入缺口诊断"]
        BD["outputs/backtests/backtest_daily_YYYY-MM-DD_YYYY-MM-DD.csv<br/>含 confidence_score / confidence_level / feature_as_of / universe_as_of / industry_node 状态 / effective_weights_json"]
        BIC["outputs/backtests/backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv<br/>signal_date 输入覆盖、source_type 和 PIT 字段"]
        BR["outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md<br/>含结论使用等级、Backtest Data Quality、判断置信度分桶、产业链节点历史状态和基准政策解释"]
        BROB["outputs/backtests/backtest_robustness_YYYY-MM-DD_YYYY-MM-DD.md/json<br/>按 backtest validation policy 运行成本压力、起点后移、固定/vol-targeted/no-gate 仓位、再平衡频率、趋势/alpha/risk/gate 架构基线、权重扰动、same-turnover/same-exposure random、样本外切分和买入持有基准对比<br/>摘要记录 as-run policy、data credibility、coverage/source veto、bootstrap CI、effective weights 和 scenario weight metadata"]
        BLAG["outputs/backtests/backtest_lag_sensitivity_YYYY-MM-DD_YYYY-MM-DD.md/json<br/>feature/universe lag 0/1/3/5/10/20 敏感性"]
        BMP["outputs/backtests/model_promotion_YYYY-MM-DD_YYYY-MM-DD.md/json<br/>按 backtest validation policy + feedback sample policy 检查数据可信度、robustness、lag、shadow outcome 和 rule governance 晋级门槛"]
        BGA["aits backtest-gate-attribution<br/>读取 backtest daily + input coverage；production_effect=none"]
        BGAR["outputs/backtests/gate_event_attribution_YYYY-MM-DD_YYYY-MM-DD.md<br/>gate avoided_drawdown / missed_upside 与 event label readiness"]
        BA["outputs/backtests/backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md<br/>输入审计状态、发现和修复建议"]
        BRT["outputs/backtests/evidence/backtest_YYYY-MM-DD_YYYY-MM-DD_trace.json<br/>claim / evidence / dataset / quality / run manifest / rule_versions"]
    end

    subgraph Trace["报告反查"]
        TLK["aits trace lookup<br/>按 claim/evidence/dataset/quality/run id 反查 evidence bundle"]
    end

    subgraph Feedback["反馈校准"]
        FBC["aits feedback calibrate<br/>先执行数据质量门禁，再观察历史 decision_snapshot"]
        DOCSV["data/processed/decision_outcomes.csv<br/>1D/5D/20D/60D/120D outcome"]
        DCR["outputs/reports/decision_calibration_YYYY-MM-DD.md<br/>分桶校准、样本限制和基准政策解释"]
        FPC["aits feedback calibrate-predictions<br/>先执行数据质量门禁，再观察 prediction ledger"]
        POCSV["data/processed/prediction_outcomes.csv<br/>production/challenger prediction outcome"]
        POR["outputs/reports/prediction_outcomes_YYYY-MM-DD.md<br/>按 candidate/model version 分桶"]
        FCO["aits feedback apply-calibration-overlay<br/>读取 context + weight profile + approved overlays<br/>只计算并导出 effective_weights；score-daily/backtest 通过 resolver 读取同一逻辑"]
        ACO["data/processed/approved_calibration_overlay.json<br/>approved_soft 历史校准 overlay；approved_hard hard effect 未接入执行层前 fail closed<br/>candidate 或过期项不得影响 production"]
        CTX["outputs/current_context.json<br/>最近一次默认 score-daily 的校准匹配上下文"]
        EW["outputs/current_effective_weights.json<br/>matched overlays、base/effective weights、confidence_delta、position_multiplier 和审计原因"]
        FSWP["aits feedback run-shadow-weight-profiles<br/>读取 production decision_snapshot，组合隔离测试权重和 shadow gate profile<br/>计算 shadow score / model band / gated band"]
        SWPO["data/processed/shadow_weight_profile_observations.csv<br/>按 as_of/profile 记录主线评分、shadow 评分、分数差、模型仓位、gate profile 和 gate 后观察仓位"]
        SWPR["outputs/reports/shadow_weight_profiles_YYYY-MM-DD.md<br/>shadow weight + shadow gate profile 与主线评分对比报告"]
        SWPL["data/processed/shadow_weight_profile_prediction_ledger.csv<br/>可选隔离 challenger prediction ledger；production_effect=none"]
        FSWPE["aits feedback evaluate-shadow-weight-performance<br/>比较 production vs shadow weight/gate 后仓位的 position-weighted return / MDD / turnover / cost"]
        SWPERF["outputs/reports/shadow_weight_performance_YYYY-MM-DD.md/.csv<br/>return-leading profile 与主线表现差异；validation-only"]
        FSPS["aits feedback search-shadow-parameters<br/>按指定区间枚举 validation-only shadow weight/gate 参数候选<br/>计算 objective、factorial attribution、cap-level attribution、最终仓位变化解释、Pareto front 和 best/diagnostic profile"]
        SPSR["outputs/parameter_search/<run_id>/{manifest.json,trials.csv,pareto_front.csv,best_profiles.yaml,search_report.md}<br/>可复现搜索登记、lineage checksum、factorial/cap attribution、最优/诊断候选和治理边界"]
        FSPP["aits feedback evaluate-shadow-parameter-promotion<br/>读取 search bundle + promotion contract；只评估晋级 readiness"]
        SPPR["outputs/parameter_search/<run_id>/shadow_parameter_promotion_<run_id>.md/json<br/>NOT_PROMOTABLE / READY_FOR_FORWARD_SHADOW / READY_FOR_OWNER_REVIEW；production_effect=none"]
        FCP["aits feedback validate-calibration-protocol<br/>校验 nested walk-forward、purging/embargo、trial、benchmark 和 production boundary"]
        FCPR["outputs/reports/calibration_protocol_YYYY-MM-DD.md<br/>调权防过拟合 protocol 校验报告"]
        FSH["aits feedback run-shadow<br/>从 production snapshot/trace 派生 challenger prediction；production_effect=none"]
        FSM["aits feedback shadow-maturity<br/>按 candidate/horizon 评估 forward shadow 样本成熟度；支持 promotion / validation mode"]
        FSMR["outputs/reports/shadow_maturity_YYYY-MM-DD.md<br/>READY_FOR_SHADOW / READY_FOR_VALIDATION_REVIEW / READY_FOR_GOV_REVIEW 样本门槛"]
        FCC["aits feedback build-causal-chain<br/>串联 snapshot、trace evidence、模块变化、gate 和 outcome"]
        DCC["data/processed/decision_causal_chains.json<br/>signal_time_context + post_signal_observations"]
        DCCR["outputs/reports/decision_causal_chains_YYYY-MM-DD.md<br/>因果链摘要和质量状态"]
        FCL["aits feedback lookup-chain<br/>按 chain_id 查询因果链"]
        FLQ["aits feedback build-learning-queue<br/>结果归因和学习复核队列"]
        DLQ["data/processed/decision_learning_queue.json<br/>归因分类、owner、next step、规则候选标记"]
        DLQR["outputs/reports/decision_learning_queue_YYYY-MM-DD.md<br/>学习队列摘要和样本限制"]
        FLL["aits feedback lookup-learning<br/>按 review_id 查询复核项"]
        FRE["aits feedback build-rule-experiments<br/>候选规则实验台账"]
        REXP["data/processed/rule_experiments.json<br/>replay / forward shadow 计划，production_effect=none"]
        REXPR["outputs/reports/rule_experiments_YYYY-MM-DD.md<br/>规则候选、验证计划和治理边界"]
        FRL["aits feedback lookup-rule-experiment<br/>按 candidate_id 查询规则实验"]
        FPR["aits feedback build-parameter-replay<br/>读取 backtest robustness summary；production_effect=none"]
        FPRR["outputs/reports/parameter_replay_YYYY-MM-DD.md/json<br/>baseline vs 参数候选收益、回撤和换手变化<br/>汇总 OOS、same-turnover random、signal-family baseline 和 data credibility 证据"]
        FPCAND["aits feedback build-parameter-candidates<br/>从 parameter replay 生成 candidate-only trial ledger<br/>支持 strict / flow_validation 门禁模式"]
        PCAND["data/processed/parameter_candidates.json<br/>参数 trial、候选状态、material 标记、多目标 veto、evaluation_mode 和治理下一步"]
        PCANDR["outputs/reports/parameter_candidates_YYYY-MM-DD.md<br/>参数候选台账、trial registry、OOS/random/data veto 证据；flow_validation 只作接线验证"]
        FPSH["aits feedback run-parameter-shadow<br/>默认写入 data/processed/prediction_ledger_flow_validation.csv；production_effect=none"]
        FPSHR["outputs/reports/parameter_shadow_predictions_YYYY-MM-DD.md<br/>parameter shadow 写入行数、来源 ledger 和 no-production 边界"]
        FPG["aits feedback evaluate-parameter-governance<br/>读取参数治理 manifest 和 candidate ledger；owner 暂缺量化输入时只输出治理动作"]
        FPGR["outputs/reports/parameter_governance_YYYY-MM-DD.md/json<br/>参数面 action 分布、blocked reason、owner-required 边界和 config checksum"]
        FGV["aits feedback validate-rule-cards<br/>规则生命周期校验"]
        GVR["outputs/reports/rule_governance_YYYY-MM-DD.md<br/>rule card 校验和复核到期状态"]
        FGL["aits feedback lookup-rule-card<br/>按 rule_id 查询 rule card"]
        FGP["aits feedback promote-rule-card / retire-rule-card<br/>owner approval 后受控切换 production/retired rule"]
        GLR["outputs/reports/rule_lifecycle_promote/retire_YYYY-MM-DD.md<br/>规则生命周期操作审计"]
        FBPV["aits feedback validate-benchmark-policy<br/>AI proxy / benchmark policy 校验"]
        BPR["outputs/reports/benchmark_policy_YYYY-MM-DD.md<br/>基准角色、选择口径和问题清单"]
        FBPL["aits feedback lookup-benchmark-policy<br/>按 ticker 或 basket 查询基准口径"]
        FLR["aits feedback loop-review<br/>周期性闭环复核"]
        FLRR["outputs/reports/feedback_loop_review_YYYY-MM-DD.md<br/>证据、快照、decision/prediction outcome、因果链、学习队列和任务状态"]
        MFO["aits feedback optimize-market-feedback<br/>独立市场反馈优化编排；production_effect=none"]
        MFOR["outputs/reports/market_feedback_optimization_YYYY-MM-DD.md<br/>readiness、样本门槛、as-if 窗口、错误复盘、参数 replay/候选/治理和执行频次"]
        PIR["aits reports investment-review<br/>周报/月报投资复盘"]
        PIRR["outputs/reports/investment_weekly/monthly_review_YYYY-MM-DD.md<br/>判断变化、仓位变化、证据、production vs challenger outcome 和规则学习"]
        EDASH["aits reports dashboard<br/>证据下钻型静态 dashboard v2"]
        EDASHR["outputs/reports/evidence_dashboard_YYYY-MM-DD.html/json<br/>Decision Card、alerts、history、feedback review、trace 下钻"]
        DTASKD["aits reports daily-tasks<br/>每日关键结论展示页"]
        DTASKDR["outputs/reports/daily_task_dashboard_YYYY-MM-DD.html/json<br/>key_conclusions、shadow parameter 结果/参数对比表、return 口径、重要风险、任务状态和可点击子报告链接"]
        ALERT["score-daily alert evaluation<br/>data/system + investment/risk 只读告警"]
        ALERTR["outputs/reports/alerts_YYYY-MM-DD.md<br/>等级、触发/解除条件、引用和去重键"]
    end

    subgraph Governance["结构校验"]
        WV["aits watchlist validate"]
        WR["outputs/reports/watchlist_validation_YYYY-MM-DD.md"]
        WVL["aits watchlist validate-lifecycle"]
        WLR["outputs/reports/watchlist_lifecycle_YYYY-MM-DD.md"]
        IV["aits industry-chain validate"]
        IR["outputs/reports/industry_chain_validation_YYYY-MM-DD.md"]
        RV["aits risk-events validate"]
        RVR["outputs/reports/risk_events_validation_YYYY-MM-DD.md"]
        ROI["aits risk-events import-occurrences-csv"]
        ROIR["outputs/reports/risk_event_occurrence_import_YYYY-MM-DD.md"]
        RPI["aits risk-events import-prereview-csv<br/>OpenAI 输出只进入待人工复核队列"]
        LLMCFG["config/llm_request_profiles.yaml<br/>按请求类型配置 model / reasoning / timeout / cache TTL / retry / candidate limit"]
        RPO["aits risk-events precheck-openai<br/>Responses API live 风险事件整理<br/>读取 risk_event_single_prereview profile<br/>短 TTL agent 精确请求缓存<br/>provider 权限 fail closed"]
        RPQ["data/processed/risk_event_prereview_queue.json<br/>schema v2：llm_extracted / pending_review 预审队列<br/>记录 model 与 reasoning effort"]
        RPIR["outputs/reports/risk_event_prereview_import_YYYY-MM-DD.md"]
        RPOR["outputs/reports/risk_event_prereview_openai_YYYY-MM-DD.md<br/>request id、response id、checksum 和权限边界"]
        OPF["aits risk-events fetch-official-sources<br/>抓取低成本官方来源；缺 API key 显式跳过"]
        OPFR["outputs/reports/official_policy_sources_YYYY-MM-DD.md<br/>官方来源抓取报告：row count/checksum/候选/跳过来源"]
        OPT["aits risk-events triage-official-candidates<br/>按 AI 模块相关性分类官方候选"]
        OPTR["outputs/reports/risk_event_candidate_triage_YYYY-MM-DD.md<br/>must_review / review_next / sample_review / auto_low_relevance"]
        OPLLM["aits risk-events precheck-triaged-official-candidates<br/>只对高优先级官方候选做 OpenAI 风险等级预审<br/>读取 risk_event_triaged_official_candidates profile"]
        OPLLMR["outputs/reports/risk_event_prereview_triaged_openai_YYYY-MM-DD.md<br/>LLM level_suggestion / status_suggestion / 人工复核问题 / cache HIT-MISS"]
        OPLLMF["aits risk-events apply-llm-formal-assessment / score-daily 自动 LLM formal<br/>将 LLM 预审队列写为正式评估 occurrence / attestation"]
        OPLLMFR["outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md<br/>LLM formal assessment 导入报告"]
        LLMP["aits llm precheck-claims<br/>Responses API + Structured Outputs<br/>读取 llm_claim_prereview profile<br/>短 TTL agent 精确请求缓存<br/>provider 权限 fail closed"]
        LLMQ["data/processed/llm_claim_prereview_queue.json<br/>schema v2：claim-centric llm_extracted / pending_review 队列<br/>记录 model 与 reasoning effort"]
        LLMR["outputs/reports/llm_claim_prereview_YYYY-MM-DD.md<br/>request id、model、reasoning effort、prompt version、checksum、cache 状态和权限边界"]
        OAC["data/processed/agent_request_cache/*.json<br/>archive/{provider}/{api_family}/YYYY-MM-DD/*.json<br/>Authorization 脱敏；cache_allowed gate"]
        LLMCFG --> RPO
        LLMCFG --> OPLLM
        LLMCFG --> LLMP
        RAT["aits risk-events record-review-attestation<br/>人工确认无未记录重大风险事件"]
        ROV["aits risk-events validate-occurrences"]
        ROR["outputs/reports/risk_event_occurrences_YYYY-MM-DD.md"]
        DSV["aits data-sources validate"]
        DSR["outputs/reports/data_sources_validation_YYYY-MM-DD.md"]
        DSH["aits data-sources health<br/>provider health score + reconciliation 覆盖"]
        DSHR["outputs/reports/data_sources_health_YYYY-MM-DD.md<br/>manifest/cache/checksum/freshness/coverage"]
        PSMF["aits pit-snapshots fetch-fmp-forward<br/>阶段 2：FMP forward-only PIT 抓取<br/>--continue-on-failure 可用于日常调度非阻断失败报告"]
        PSMB["aits pit-snapshots build-manifest<br/>现有 raw cache 归档"]
        PSV["aits pit-snapshots validate<br/>PIT raw snapshot 质量门禁"]
        PSR["outputs/reports/pit_snapshots_validation_YYYY-MM-DD.md<br/>缺跑不能事后补 strict PIT"]
        EVI["aits evidence import-csv"]
        EV["aits evidence validate"]
        EVIR["outputs/reports/market_evidence_import_YYYY-MM-DD.md"]
        EVR["outputs/reports/market_evidence_YYYY-MM-DD.md"]
        SCV["aits scenarios validate<br/>情景库节点/ticker/risk event/gate 映射校验"]
        SCR["outputs/reports/scenario_library_YYYY-MM-DD.md<br/>情景映射和人工复核要求"]
        SCL["aits scenarios lookup<br/>按 scenario_id 查询情景"]
        CTV["aits catalysts validate / upcoming<br/>未来 5/20/60 天催化剂分桶"]
        CTR["outputs/reports/catalyst_calendar_YYYY-MM-DD.md<br/>upcoming catalyst 和复核要求"]
        CTL["aits catalysts lookup<br/>按 catalyst_id 查询事件"]
        EPV["aits execution validate<br/>校验 advisory action taxonomy"]
        EPRG["outputs/reports/execution_policy_YYYY-MM-DD.md<br/>执行政策校验报告"]
        EPL["aits execution lookup<br/>按 action_id 查询执行动作"]
        PEV["aits portfolio exposure<br/>真实持仓只读暴露分解；缺少文件时 NOT_CONNECTED"]
        ODP["aits ops daily-plan / daily-run<br/>每日运行计划、America/New_York 最新已完成交易日默认 as-of、交易日历判断、凭据检查、输入可见性预检查、真实执行和调度顺序<br/>daily-run 支持 --run-output-root / --run-id / --legacy-output-mode"]
        ODPR["outputs/runs/daily/<executed_at_utc>/as_of_YYYY-MM-DD__<safe_run_id>/{manifest.json,reports/,traces/,metadata/}<br/>legacy mirror: outputs/reports/daily_ops_* 与 daily_score_*<br/>market session、上一交易日、步骤、环境变量 presence、input visibility status、pre-run input checksum、ArtifactRef artifact checksum、visibility cutoff、git/config/rule hash、执行状态和跳过声明"]
        DOCF["aits docs validate-freshness<br/>检查任务/需求文档最后更新日期"]
        DOCFR["可选 outputs/reports/docs_freshness_YYYY-MM-DD.md<br/>缺失最后更新或状态日期更新时 fail closed"]
        ODR["aits ops replay-day / replay-window<br/>历史交易日 cache-only 归档回放<br/>冻结 as-of 可见输入窗口、市场/宏观 raw cache 过滤、手工输入隔离视图、OpenAI disabled/cache-only 可见性过滤、dashboard、可选 production diff、窗口批量回放"]
        ODRR["outputs/replays/YYYY-MM-DD/<run-id>/ 与 outputs/replays/windows/<run-id>/<br/>input freeze manifest、prices/rates replay raw cache、OpenAI cache-only 过滤报告、trade_theses/trades 过滤视图、replay_run、diff_vs_production、replay_window、score/dashboard/health/secret 输出"]
        OPH["aits ops health<br/>关键 pipeline artifact + PIT 抓取/快照健康检查<br/>休市日可用 --non-trading-day"]
        OPR["outputs/reports/pipeline_health_YYYY-MM-DD.md<br/>market session、存在性、mtime、row count、freshness、checksum、fetch status"]
        SCS["aits security scan-secrets<br/>本地 secret hygiene 扫描"]
        SCSR["outputs/reports/secret_hygiene_YYYY-MM-DD.md<br/>疑似 secret 脱敏问题清单"]
    end

    subgraph Thesis["交易假设复核"]
        TL["aits thesis list"]
        TV["aits thesis validate"]
        TR["aits thesis review"]
        TVR["outputs/reports/thesis_validation_YYYY-MM-DD.md"]
        TRR["outputs/reports/thesis_review_YYYY-MM-DD.md"]
    end

    subgraph Valuation["估值与拥挤度复核"]
        VF["aits valuation fetch-fmp"]
        VHF["aits valuation fetch-fmp-valuation-history"]
        VET["aits valuation fetch-eodhd-trends"]
        VFR["outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md"]
        VHFR["outputs/reports/fmp_historical_valuation_fetch_YYYY-MM-DD.md"]
        VETR["outputs/reports/eodhd_earnings_trends_fetch_YYYY-MM-DD.md"]
        VI["aits valuation import-csv"]
        VIR["outputs/reports/valuation_import_YYYY-MM-DD.md"]
        VFH["aits valuation validate-fmp-history"]
        VFHR["outputs/reports/fmp_analyst_history_validation_YYYY-MM-DD.md"]
        VL["aits valuation list"]
        VV["aits valuation validate"]
        VR["aits valuation review"]
        VVR["outputs/reports/valuation_validation_YYYY-MM-DD.md"]
        VRR["outputs/reports/valuation_review_YYYY-MM-DD.md"]
    end

    subgraph TradeReview["交易复盘归因"]
        RT["aits review-trades"]
        RTR["outputs/reports/trade_review_YYYY-MM-DD.md"]
    end

    MD --> ERC
    FMP --> ERC
    CBOEVIX --> ERC
    EODHDT --> ERC
    OPSRC --> ERC
    SEC --> ERC
    TSMTXT -. "HTTP fetch only；本地导入不经外部请求" .-> TSMI
    ERC -. "HIT 复用；MISS 后归档" .-> DL
    ERC -. "HIT 复用；MISS 后归档" .-> SFD
    ERC -. "HIT 复用；MISS 后归档" .-> SFSD
    ERC -. "HIT 复用；MISS 后归档" .-> SFAD
    ERC -. "HIT 复用；MISS 后归档" .-> TSMF
    ERC -. "HIT 复用；MISS 后归档" .-> OPF
    ERC -. "HIT 复用；MISS 后归档" .-> PSMF
    ERC -. "HIT 复用；MISS 后归档" .-> VF
    ERC -. "HIT 复用；MISS 后归档" .-> VHF
    ERC -. "HIT 复用；MISS 后归档" .-> VET
    FMP --> DL
    CBOEVIX --> DL
    U --> DL
    DS --> DL
    DL --> PR
    DL --> MSPR
    DL --> RR
    DL --> DM
    DL -. "失败时 fail closed 并写入脱敏诊断" .-> DLF
    SEC --> SFD
    DS --> SFD
    SFD --> SFJ
    SFD --> SFM
    SEC --> SFSD
    DS --> SFSD
    SFSD --> SFSJ
    SFC --> SFAD
    DS --> SFAD
    SFAD --> SFAJ
    SFC --> SFAC
    SFSJ --> SFAC
    SFAJ --> SFAC
    SFAC --> SFACR
    SFJ --> SFV
    SFM --> SFV
    SFV --> SFVR
    SEC --> SFE
    FM --> SFE
    SFJ --> SFE
    SFM --> SFE
    SFE --> SFVR
    SFE --> SFC
    SFE --> SFR
    SFC --> SFVC
    SEC --> SFVC
    FM --> SFVC
    SFVC --> SFCR
    SFC --> SFF
    SEC --> SFF
    FM --> SFF
    FF --> SFF
    SFF --> SFCR
    SFF --> SFFC
    SFF --> SFFR
    DS --> TSMP
    TSMPDF --> TSMP
    TSMP --> TSMTXT
    TSMP --> TSMPR
    DS --> TSMF
    TSMF --> TSMTXT
    TSMF --> TSMC
    TSMF --> TSMR
    TSMTXT --> TSMI
    DS --> TSMI
    TSMI --> TSMC
    TSMI --> TSMR
    TSMTXT --> TSMIB
    TSMMAN --> TSMIB
    DS --> TSMIB
    TSMIB --> TSMC
    TSMIB --> TSMBR
    TSMC --> TSMM
    SEC --> TSMM
    FM --> TSMM
    TSMM --> SFC
    TSMM --> SFCR

    U --> V
    Q --> V
    DS --> V
    WPC --> FCO
    WPM --> FCP
    FCP --> FCPR
    ACO --> FCO
    CTX --> FCO
    FCO --> EW
    PR --> V
    MSPR --> V
    RR --> V
    V -->|通过或 PASS_WITH_WARNINGS| QR
    V -->|FAIL| Stop
    QR --> YDG
    PR --> YDG
    MSPR --> YDG
    RR --> YDG
    YFD --> YDG
    YDG --> YDR

    PR --> BF
    RR --> BF
    F --> BF
    FAVC --> BF
    W --> BF
    QR --> BF
    BF --> FT
    BF --> FR
    BF --> FAVR

    WPC --> EWR
    ACO --> EWR
    FT --> SD
    QR --> SD
    FAVC --> SD
    FAVR --> SD
    S --> SD
    EWR --> SD
    P --> SD
    EPC --> SD
    GOVC --> SD
    SEC --> SD
    FM --> SD
    FF --> SD
    SFC --> SD
    TH --> SD
    RE --> SD
    REX --> SD
    REXATT --> SD
    VS --> SD
    TD --> SD
    POS --> SD
    SD -. "--risk-event-openai-precheck" .-> OPF
    I --> NH
    W --> NH
    FT --> NH
    POS --> PEX
    I --> PEX
    W --> PEX
    SD --> SFCR
    SD --> SFFC
    SD --> SFFR
    SD --> EPR
    SD --> MBG
    SD --> PG
    MBG --> SC
    MBG --> DR
    MBG --> DRT
    MBG --> CONF
    PG --> SC
    PG --> DR
    PG --> DRT
    PG --> CONF
    PG --> EADV
    CONF --> SC
    CONF --> DR
    CONF --> DSNAP
    CONF --> EADV
    EPC --> EADV
    SD --> CTX
    EWR --> EW
    NH --> DR
    NH --> DRT
    PEX --> PER
    PEX --> DR
    EADV --> DR
    EADV --> DRT
    DRT --> DSNAP
    DSNAP --> PLED

    PR --> BT
    RR --> BT
    F --> BT
    FAVC --> BT
    S --> BT
    EWR --> BT
    BVP --> BT
    BVP --> BROB
    BVP --> BMP
    P --> BT
    W --> BT
    WL --> BT
    R --> BT
    BPC --> BT
    GOVC --> BT
    QR --> BT
    SEC --> BT
    FM --> BT
    FF --> BT
    SFJ --> BT
    SFM --> BT
    TSMC --> BT
    VS --> BT
    RE --> BT
    REX --> BT
    REXATT --> BT
    PR --> BTG
    RR --> BTG
    VS --> BTG
    RE --> BTG
    REX --> BTG
    REXATT --> BTG
    BTG --> BIG
    PITMAN --> BTPC
    BTPC --> BPCR
    BT --> BSEC
    BT --> BVAL
    BT --> BRISK
    BT --> BWATCH
    BWATCH --> BD
    BSEC --> BD
    BVAL --> BD
    BRISK --> BD
    BT --> BD
    BT --> BIC
    BT --> BR
    BT --> BROB
    BT --> BLAG
    BT --> FAVR
    BT --> BMP
    BT --> BA
    BT --> BRT
    BD --> BGA
    BIC --> BGA
    BGA --> BGAR
    FCPR -. "实验准入证据" .-> REXP
    BROB --> FPR
    FPR --> FPRR
    FPRR --> FPCAND
    FPCAND --> PCAND
    FPCAND --> PCANDR
    PGM --> FPG
    PCAND --> FPG
    FPG --> FPGR
    SWPC --> FSWP
    SGPC --> FSWP
    DSNAP --> FSWP
    FSWP --> SWPO
    FSWP --> SWPR
    SWPO --> FSWPE
    PR --> FSWPE
    FSWPE --> SWPERF
    SPSC --> FSPS
    SPOC --> FSPS
    WPC --> FSPS
    DSNAP --> FSPS
    PR --> FSPS
    FSPS --> SPSR
    SPPC --> FSPP
    SPSR --> FSPP
    FSPP --> SPPR
    FSP --> FBC
    FSP --> FPC
    FSP --> FSM
    FSP --> FLR
    FSP --> PIR
    FSP --> BMP
    DRT --> TLK
    BRT --> TLK
    DSNAP --> FBC
    PR --> FBC
    RR --> FBC
    BPC --> FBC
    FBC --> DOCSV
    FBC --> DCR
    PLED --> FPC
    PR --> FPC
    RR --> FPC
    FPC --> POCSV
    FPC --> POR
    DSNAP --> FCC
    DOCSV --> FCC
    POCSV --> BMP
    DRT --> FCC
    FCC --> DCC
    FCC --> DCCR
    DCC --> FCL
    DCC --> FLQ
    FLQ --> DLQ
    FLQ --> DLQR
    DLQ --> FLL
    DLQ --> FRE
    FRE --> REXP
    FRE --> REXPR
    REXP --> FRL
    REXP --> FSH
    DSNAP --> FSH
    DRT --> FSH
    FSH --> PLED
    POCSV --> FSM
    FSM --> FSMR
    GOVC --> FGV
    REXP --> FGV
    FGV --> GVR
    GOVC --> FGL
    BPC --> FBPV
    FBPV --> BPR
    BPC --> FBPL
    EVI --> FLR
    DSNAP --> FLR
    DOCSV --> FLR
    DCC --> FLR
    DLQ --> FLR
    REXP --> FLR
    FLR --> FLRR
    QR --> MFO
    DOCSV --> MFO
    POCSV --> MFO
    DCC --> MFO
    DLQ --> MFO
    REXP --> MFO
    FSMR --> MFO
    FPRR --> MFO
    PCAND --> MFO
    FPGR --> MFO
    ACO --> MFO
    EW --> MFO
    FSP --> MFO
    FLRR --> MFO
    MFO --> MFOR
    MFOR --> PIR
    SC --> PIR
    DSNAP --> PIR
    DOCSV --> PIR
    DLQ --> PIR
    REXP --> PIR
    BS --> PIR
    PIR --> PIRR
    DR --> EDASH
    DRT --> EDASH
    DSNAP --> EDASH
    BS --> EDASH
    EDASH --> EDASHR
    EDASHR --> TLK
    QR --> ALERT
    FT --> ALERT
    SC --> ALERT
    DSNAP --> ALERT
    ROR --> ALERT
    CTV --> ALERT
    ALERT --> ALERTR
    ALERT --> DR

    U --> WV
    W --> WV
    I --> WV
    WV --> WR
    WL --> WVL
    W --> WVL
    U --> WVL
    WVL --> WLR
    I --> IV
    W --> IV
    IV --> IR
    RE --> RV
    I --> RV
    W --> RV
    U --> RV
    RV --> RVR
    RPRCSV --> RPI
    RE --> RPI
    RPI --> RPQ
    RPI --> RPIR
    LLMI --> RPO
    OPCAND -->|日报前自动 metadata_only 预审| RPO
    DS --> RPO
    RE --> RPO
    OAC -->|TTL HIT| RPO
    RPO -->|MISS/EXPIRED 实际请求归档| OAC
    RPO --> RPQ
    RPO --> RPOR
    RPQ -->|pending_review；不直接评分| SD
    RPQ -->|人工确认后才可整理为 occurrence CSV| REXCSV
    RPQ --> FLR
    OPSRC --> OPF
    DS --> OPF
    OPF --> OPRAW
    OPF --> OPCAND
    OPF --> OPFR
    OPCAND --> OPT
    OPT --> OPTRI
    OPT --> OPTR
    OPCAND --> OPLLM
    OPTRI --> OPLLM
    OAC -->|TTL HIT| OPLLM
    OPLLM -->|MISS/EXPIRED 实际请求归档| OAC
    OPLLM --> RPQ
    OPLLM --> OPLLMR
    RPQ --> OPLLMF
    OPLLMF --> OPLLMFR
    OPLLMF --> ROV
    OPF --> DM
    OPCAND -->|人工复核后才可整理为 occurrence CSV| REXCSV
    OPCAND -->|可作为每日复核 checked_sources 依据| RAT
    LLMI --> LLMP
    DS --> LLMP
    OAC -->|TTL HIT| LLMP
    LLMP -->|MISS/EXPIRED 实际请求归档| OAC
    LLMP --> LLMQ
    LLMP --> LLMR
    LLMQ -->|人工确认后才可整理为 evidence CSV| MECSV
    LLMQ -->|风险事件人工确认后才可整理为 occurrence CSV| REXCSV
    LLMQ --> FLR
    RAT --> REXATT
    RAT --> ROR
    REXCSV --> ROI
    ROI --> REX
    ROI --> ROIR
    ROI --> ROR
    RE --> ROV
    REX --> ROV
    REXATT --> ROV
    ROV --> ROR
    DS --> DSV
    DSV --> DSR
    DS --> DSH
    DSH --> DSHR
    DS --> PSMF
    FMP --> PSMF
    PSMF --> FMPFP
    PSMF --> FMPFPN
    PSMF --> FMPFPR
    DS --> PSMB
    FMPH --> PSMB
    FMPVH --> PSMB
    FMPFP --> PSMB
    EODHDTR --> PSMB
    PSMB --> PITMAN
    PITMAN --> PSV
    PSV --> PSR
    MECSV --> EVI
    EVI --> ME
    EVI --> EVIR
    ME --> EV
    EV --> EVR
    SCC --> SCV
    I --> SCV
    W --> SCV
    RE --> SCV
    SCV --> SCR
    SCC --> SCL
    CTC --> CTV
    I --> CTV
    W --> CTV
    RE --> CTV
    CTV --> CTR
    CTC --> CTL
    EPC --> EPV
    EPV --> EPRG
    EPC --> EPL
    POS --> PEV
    I --> PEV
    W --> PEV
    PEV --> PER
    ODP --> ODPR
    ODP -.-> DL
    ODP -.-> PSMF
    ODP -.-> SD
    ODP -.-> OPH
    ODP -.-> SCS
    DOCF --> DOCFR
    PITMAN -.-> ODR
    FMPFPN -.-> ODR
    VS -.-> ODR
    ODR -.-> SD
    ODR -.-> OPH
    ODR -.-> SCS
    ODR --> ODRR
    PR --> OPH
    RR --> OPH
    FT --> OPH
    SC --> OPH
    QR --> OPH
    DR --> OPH
    OPH --> OPR
    SCS --> SCSR

    TH --> TL
    TH --> TV
    TH --> TR
    W --> TV
    I --> TV
    W --> TR
    I --> TR
    TV --> TVR
    TR --> TRR

    FMP --> VF
    FMP --> VHF
    FMP --> PITF
    EODHDT --> VET
    DS --> VF
    DS --> VHF
    DS --> PITF
    DS --> VET
    U --> VF
    U --> VHF
    U --> PITF
    U --> VET
    VHF --> FMPVH
    VHF --> VS
    VHF --> VHFR
    VHF --> VVR
    FMPH --> VF
    FMPVH --> VF
    FMPFPN --> VF
    VF --> VS
    VF --> FMPH
    VF --> VFR
    VF --> VVR
    VS --> VET
    VET --> EODHDTR
    VET --> VS
    VET --> VETR
    VET --> VVR
    PITF --> FMPFP
    PITF --> FMPFPN
    PITF --> FMPFPR
    FMPH --> PITB
    FMPVH --> PITB
    FMPFP --> PITB
    EODHDTR --> PITB
    DS --> PITB
    PITB --> PITMAN
    PITMAN --> PITV
    PITV --> PITR
    FMPH --> VFH
    VFH --> VFHR
    VSCSV --> VI
    VI --> VS
    VI --> VIR
    VI --> VVR
    VS --> VL
    VS --> VV
    VS --> VR
    U --> VV
    W --> VV
    VV --> VVR
    VR --> VRR

    TD --> RT
    PR --> RT
    RR --> RT
    QR --> RT
    RT --> RTR
```

## 每日评分链路

```mermaid
flowchart TD
    A["用户执行<br/>aits score-daily --as-of YYYY-MM-DD"] --> B["读取配置<br/>universe / data_quality / features / feature_availability / scoring_rules / portfolio / risk_events / execution_policy"]
    B --> C["读取缓存<br/>prices_daily.csv / prices_marketstack_daily.csv / rates_daily.csv"]
    C --> D["调用数据质量门禁<br/>validate_data_cache"]
    D -->|FAIL| E["停止<br/>输出 data_quality 报告和错误数量"]
    D -->|PASS 或 PASS_WITH_WARNINGS| EP0["校验 execution_policy<br/>固定 advisory action taxonomy，输出 execution_policy 报告"]
    EP0 -->|FAIL| EPF["停止<br/>输出 execution policy 错误和报告路径"]
    EP0 -->|PASS 或 PASS_WITH_WARNINGS| FAV0["校验 PIT 特征可见时间目录<br/>feature_availability catalog<br/>缺少 source rule 则停止"]
    FAV0 -->|FAIL| FAVF["停止<br/>输出 feature_availability 报告"]
    FAV0 -->|PASS| F["构建当日市场特征<br/>build_market_features"]
    F --> G["写入特征缓存<br/>features_daily.csv"]
    F --> H["写入特征摘要<br/>feature_summary_YYYY-MM-DD.md<br/>引用 PIT 特征可见时间报告"]
    F --> FST["构建关注股票趋势分析<br/>core_watchlist ticker 的收益、均线位置和数据缺口<br/>只读解释层"]
    F --> NH["构建产业链节点热度与健康度<br/>industry_chain + watchlist + 市场趋势特征 + SEC/TSM 基本面 + 估值 + 风险事件 + thesis<br/>watch_only 标的缺 thesis 不触发 gate；只读解释层"]
    F --> PE["读取真实持仓 CSV 并构建组合暴露<br/>缺少文件为 NOT_CONNECTED；存在但格式错误则停止"]
    F --> R["复用已通过的数据质量结果<br/>汇总 thesis / 风险事件 / 估值 / 交易复盘状态"]
    PE --> PE1["写入 portfolio_exposure_YYYY-MM-DD.md<br/>ticker、产业链节点、地区、客户链、因子、相关性簇"]
    R --> V1["估值快照校验和复核<br/>validate_valuation_snapshot_store<br/>输出 PIT 可信度、历史来源和回测用途"]
    R --> OPX{"是否启用<br/>--risk-event-openai-precheck"}
    OPX -->|是| OPF2["抓取官方政策/地缘来源<br/>fetch_official_policy_sources<br/>写 raw payload、manifest、候选 CSV 和官方来源报告"]
    OPF2 -->|FAIL| OPFSTOP["停止<br/>输出 official_policy_sources 报告和错误数量"]
    OPF2 -->|PASS 或 PASS_WITH_WARNINGS| RPO2["OpenAI 风险事件 metadata_only 自动预审<br/>读取 risk_event_daily_official_precheck profile；store=false；短 TTL agent 精确请求缓存；生产最新已完成交易日允许 request_timestamp <= visibility_cutoff；历史 as-of 仍按 as-of 当日 UTC 末尾 fail closed；provider 权限和 cache_allowed fail closed；irrelevant 不进人工队列"]
    RPO2 -->|FAIL| RPOSTOP["停止<br/>输出 risk_event_prereview_openai 报告"]
    RPO2 -->|PASS 或 PASS_WITH_WARNINGS| RPQ2["写入 risk_event_prereview_queue.json<br/>候选上限、model、reasoning、timeout、HTTP client、cache TTL 和重试次数来自 profile；请求最终失败则整批停止；llm_extracted / pending_review"]
    RPQ2 --> RPF2["score-daily 默认写入 LLM formal occurrence / attestation<br/>不是人工复核；B 级 evidence 不能单独触发 position gate"]
    OPX -->|否| G1["风险事件发生记录和复核声明校验<br/>validate_risk_event_occurrence_store<br/>watch 不评分；B 只普通评分；C/D/X 只复核；有效复核声明只补足空记录证明"]
    RPF2 --> G1
    F --> S1["校验 SEC 指标 CSV<br/>validate_sec_fundamental_metrics_csv"]
    S1 -->|FAIL| S2["停止<br/>输出 SEC 指标 CSV 校验报告"]
    S1 -->|PASS 或 PASS_WITH_WARNINGS| S3["构建 SEC 基本面特征<br/>build_sec_fundamental_features_report"]
    S3 -->|FAIL| S4["停止<br/>输出 SEC 特征报告"]
    S3 -->|PASS 或 PASS_WITH_WARNINGS| S5["写入 SEC 特征 CSV 和报告<br/>sec_fundamental_features_YYYY-MM-DD.*"]
    G --> I["构建每日评分<br/>build_daily_score_report"]
    H --> I
    R --> I
    V1 --> I
    G1 --> I
    S5 --> I
    I --> J["趋势评分<br/>指数趋势、半导体趋势、核心池宽度、相对强弱"]
    I --> F1["基本面评分<br/>SEC 特征中位数：毛利率、营业利润率、净利率、R&D、CapEx"]
    I --> V2["估值评分<br/>估值分位和拥挤比例；排除过期和 public_convenience"]
    I --> K["宏观流动性评分<br/>DGS10、DGS2、DTWEXBGS 广义美元指数"]
    I --> L["风险情绪评分<br/>VIX 水平、分位、变化速度"]
    I --> M["政策/地缘评分<br/>只读可评分 active 发生记录；无 active 时必须有当前有效人工或 LLM formal 声明才脱离 insufficient_data"]
    J --> N["AI 产业链评分和评分模型仓位区间<br/>风险资产内 AI 仓位"]
    F1 --> N
    V2 --> N
    K --> N
    L --> N
    M --> N
    K --> MB["宏观总风险资产预算<br/>静态 portfolio 预算基础上只允许下调"]
    L --> MB
    N --> C1["判断置信度汇总<br/>按模块来源、覆盖率、质量门禁和人工复核状态扣减<br/>基于评分模型仓位生成 confidence gate"]
    N --> G2["position_gate<br/>判断置信度、组合限制、risk_budget、风险事件、估值拥挤、thesis 状态和数据置信度取最严格上限"]
    C1 --> G2
    G2 --> O["总资产口径换算<br/>宏观调整后的 portfolio 风险资产预算"]
    MB --> O
    C1 --> P["写入 scores_daily.csv<br/>记录模块分、整体分、confidence、仓位区间和触发 gate 摘要"]
    O --> EP1["生成执行建议<br/>当前最终区间 + 上一期最终区间 + confidence/gate + execution_policy<br/>production_effect=none"]
    C1 --> EP1
    EP0 --> EP1
    O --> T["写入 evidence bundle<br/>claim/evidence/dataset/quality/run manifest，含 belief_state 与关注股票趋势 claim 引用"]
    C1 --> T
    T --> D0["写入 decision_snapshot JSON<br/>保存评分、置信度、仓位、gate、质量、trace 引用和 belief_state_ref"]
    D0 --> PL0["追加 prediction_ledger.csv<br/>production prediction；outcome_status=PENDING"]
    C1 --> D0
    O --> BS["写入 belief_state JSON<br/>只读认知状态，不直接改变评分或仓位"]
    T --> BS
    BS --> BH["更新 belief_state_history.csv<br/>按 signal_date upsert 历史索引"]
    BS --> D0
    T --> Q["写入 daily_score_YYYY-MM-DD.md<br/>含今日结论卡、Data Lineage Card、Score-to-Position Funnel、Binding Gate Ladder、复核五问、结论使用等级、变化原因树、关注股票趋势、产业链节点热度与健康度、组合暴露、认知状态、执行建议、人工复核摘要、仓位闸门和可追溯引用"]
    C1 --> Q
    FST --> Q
    NH --> Q
    PE1 --> Q
    BS --> Q
    EP1 --> Q
    Q --> DASH["生成 evidence_dashboard_YYYY-MM-DD.html/json<br/>daily-run 在 score_daily 后先生成 parameter governance / market feedback / loop review / weekly investment review，再触发 dashboard；读取日报、trace、decision_snapshot、belief_state、alerts、scores_daily 和同日复盘报告"]
    T --> DASH
    D0 --> DASH
    BS --> DASH
```

## 回测链路

```mermaid
flowchart TD
    A["用户执行<br/>aits backtest"] --> B["解析市场阶段<br/>默认 ai_after_chatgpt"]
    B --> C["确定回测起点<br/>默认 2022-12-01"]
    C --> BVP0["读取 backtest_validation_policy<br/>robustness 默认参数和 promotion gate 要求"]
    BVP0 --> BP0["读取 benchmark_policy<br/>校验 strategy_ticker / benchmark 解释口径"]
    BP0 -->|FAIL| BPF["停止回测<br/>输出 benchmark policy 错误"]
    BP0 -->|PASS 或 PASS_WITH_WARNINGS| D["读取 prices_daily.csv / prices_marketstack_daily.csv / rates_daily.csv"]
    D --> E["调用数据质量门禁<br/>validate_data_cache"]
    E -->|FAIL| F["停止回测<br/>输出 data_quality 报告"]
    E -->|PASS 或 PASS_WITH_WARNINGS| FAV0["校验 PIT 特征可见时间目录<br/>feature_availability catalog<br/>缺少 source rule 则停止"]
    FAV0 -->|FAIL| FAVF["停止回测<br/>输出 feature_availability 报告"]
    FAV0 -->|PASS| W0["校验 watchlist_lifecycle<br/>缺少当前核心 ticker 或重复记录时停止"]
    W0 -->|FAIL| WF["停止回测<br/>输出 watchlist_lifecycle 报告"]
    W0 -->|PASS 或 PASS_WITH_WARNINGS| S1["校验 SEC companyfacts 缓存<br/>validate_sec_companyfacts_cache"]
    S1 -->|FAIL| S2["停止回测<br/>输出 SEC companyfacts 校验报告"]
    S1 -->|PASS 或 PASS_WITH_WARNINGS| TSM["读取 TSM IR 季度缓存<br/>按 filed_date 参与 TSM quarterly 补齐"]
    TSM --> G["生成交易日序列<br/>signal_date -> return_date<br/>lag sensitivity 额外计算 feature_as_of / universe_as_of"]
    G --> WL0["按 universe_as_of 过滤观察池 lifecycle<br/>只使用当日已进入且节点映射可见的 ticker"]
    WL0 --> H["逐日构建市场特征<br/>只使用 feature_as_of 当日及之前数据"]
    G --> H2["逐日构建 point-in-time SEC 特征<br/>只使用 filed_date <= feature_as_of 的 SEC facts 与 TSM IR 季度"]
    H --> I["逐日评分<br/>使用同一套 scoring_rules"]
    H2 --> I
    I --> C0["判断置信度<br/>保存 confidence_score / confidence_level"]
    H --> NH0["重建产业链节点历史状态<br/>industry_chain + watchlist + PIT 基本面/估值/风险输入<br/>production_effect=none"]
    H2 --> NH0
    I --> J["评分映射到评分模型 AI 仓位区间"]
    J --> PG["应用 position_gate<br/>取判断置信度、组合限制、risk_budget、风险事件、估值拥挤、thesis 和数据置信度的最严格上限"]
    C0 --> PG
    PG --> MB["应用 macro_risk_asset_budget<br/>VIX、DGS10、DTWEXBGS 广义美元指数可下调总风险资产预算"]
    MB --> K["使用总资产内 AI exposure 中点并应用最小调仓阈值<br/>低于阈值维持原仓位"]
    K --> L["下一交易日收益生效<br/>避免未来函数"]
    L --> M["扣除显式交易摩擦假设<br/>commission / spread / slippage / market impact / tax / FX / financing / ETF delay"]
    M --> N["汇总策略指标<br/>CAGR / Max Drawdown / Sharpe / Sortino / Calmar / Turnover"]
    N --> O["对比基准<br/>SPY / QQQ / SMH / SOXX 买入持有"]
    BP0 --> Q
    C0 --> P["写入每日明细 CSV<br/>含 confidence_score / confidence_level / feature_as_of / universe_as_of"]
    NH0 --> P
    O --> P
    O --> Q["写入回测报告 Markdown<br/>包含市场阶段、结论使用等级、Backtest Data Quality、PIT 特征可见时间、核心输入 PIT 覆盖、置信度分桶和产业链节点历史状态"]
    C0 --> Q
    NH0 --> Q
    O --> Q2["可选写入稳健性报告 Markdown / JSON<br/>按 backtest_validation_policy 运行成本压力 / 起点后移 / 固定 exposure / 再平衡间隔 / 权重扰动 / 买入持有基准<br/>调参场景复用缓存 PIT 上下文并调用同一评分/回测执行路径；production_effect=none"]
    O --> Q3["可选写入 lag sensitivity Markdown / JSON<br/>feature lag、universe lag 与 rebalance delay<br/>production_effect=none"]
    BVP0 --> Q2
    Q2 --> Q4["可选写入模型晋级门槛 Markdown / JSON<br/>按 policy 检查 data credibility / robustness / lag / shadow outcome / rule governance"]
    BVP0 --> Q4
    Q3 --> Q4
    Q4 --> Q
    O --> R["写入输入覆盖诊断 CSV<br/>component / ticker / issue / source_url / point_in_time_class / backtest_use"]
    O --> S["写入输入审计报告 Markdown<br/>数据质量 / Backtest Data Quality / PIT 输入 / 来源 / 执行假设"]
    O --> T["写入 evidence bundle<br/>claim/evidence/dataset/quality/run manifest<br/>含成本假设 parameters"]
```

## 结论输出与解释责任

```mermaid
flowchart LR
    A["数据质量状态"] --> E["报告结论"]
    B["AI 产业链评分<br/>趋势 / 基本面 / 宏观流动性 / 风险情绪"] --> E
    C2["判断置信度<br/>模块来源 / 覆盖率 / 质量门禁 / 人工复核"] --> E
    C["手工/审计输入<br/>估值 / 政策地缘发生记录"] --> E
    D["市场阶段<br/>ai_after_chatgpt / cross_cycle_stress"] --> E
    F["能力圈和产业链配置<br/>watchlist / industry_chain"] --> E
    L["交易 thesis<br/>active_trade 主动交易假设<br/>验证指标 / 证伪条件 / 风险事件"] --> E
    N["估值与拥挤度<br/>估值分位 / 预期 / 过热信号 / PIT 可信度"] --> E
    Q["风险事件发生记录<br/>active/watch / 证据等级 / 动作等级 / 仓位乘数"] --> E
    Y["真实组合暴露<br/>持仓 CSV / 单票 / 节点 / 地区 / 因子 / 相关性簇"] --> E
    U2["决策快照<br/>score / confidence / gate / quality / trace refs"] --> E
    P["交易复盘<br/>市场 Beta / 主题 Beta / 个股表现"] --> E
    S["认知状态<br/>belief_state / 多维置信度 / 仓位边界 / 改变判断条件"] --> E
    X["执行建议<br/>execution_policy / 上期最终区间 / advisory action"] --> E

    E --> G["必须说明<br/>本次数据质量是否通过"]
    E --> V2["必须说明<br/>结论使用等级和投资姿态标签，并区分两者含义"]
    E --> H["必须说明<br/>哪些分数来自硬数据"]
    E --> I["必须说明<br/>哪些模块仍是占位或限制"]
    E --> J["必须说明<br/>建议仓位的口径和限制"]
    E --> K["必须说明<br/>回测区间和市场阶段"]
    E --> M["必须说明<br/>交易假设是否仍成立或需要复核"]
    E --> O["必须说明<br/>估值数据来源和是否只能作为辅助"]
    E --> Q2["必须说明<br/>政策/地缘是否来自已校验发生记录"]
    E --> P2["必须说明<br/>收益来自基准 Beta 还是个股表现"]
    E --> R2["必须说明<br/>仓位闸门来源、上限和是否触发"]
    E --> T2["必须说明<br/>市场吸引力评分和判断置信度不是同一件事"]
    E --> Y2["必须说明<br/>组合暴露是否接入真实持仓；NOT_CONNECTED 时不能把模型仓位当作账户交易数量"]
    E --> U3["必须保存<br/>可复原当日判断的 decision_snapshot"]
    E --> S2["必须说明<br/>认知状态是只读解释层还是已批准的 production 规则输入"]
    E --> X2["必须说明<br/>执行建议不是自动交易指令，production_effect=none"]
    E --> PIT2["必须说明<br/>特征可见时间目录是否通过，不能只用 event_time 当作可交易可见性"]
    E --> PROMO2["必须说明<br/>历史回测不是 promotion 通过；晋级还需要 shadow outcome 和 rule governance"]
```

## 认知状态层

该层对应 `COGNITION-001` 和 `docs/requirements/cognitive_model_2026-05-04.md`。第一版是只读解释层，用于把系统当日“相信什么、依据什么、置信度如何、哪些风险限制仓位、哪些条件会改变判断”结构化保存下来。它不得直接改变生产评分、`position_gate`、回测仓位或交易建议。

```mermaid
flowchart TD
    E0["market_evidence<br/>新市场信息证据账本<br/>S/A 可评分和支持闸门；B 只普通评分；LLM/public_convenience 不直接评分"] --> B0["belief_state<br/>只读认知状态"]
    C0["industry_node_state<br/>node_heat / node_health / coverage / concentration"] --> B0
    T0["thesis state<br/>draft / active / warning / challenged / invalidated / closed"] --> B0
    R0["risk event state<br/>watch / active / severity / evidence grade"] --> B0
    V0["valuation state<br/>crowding / PIT confidence / action bias"] --> B0
    S0["score + position_gate<br/>模型区间 / 最终区间 / 限制因素"] --> B0
    Q0["data quality + trace bundle<br/>quality refs / evidence refs / run manifest"] --> B0

    B0 --> B1["data/processed/belief_state/belief_state_YYYY-MM-DD.json"]
    B0 --> B2["data/processed/belief_state_history.csv"]
    B0 --> B3["daily_score evidence bundle<br/>belief_state dataset / claim 引用"]
    B0 --> D0["decision_snapshot<br/>保存 score / confidence / gate / quality / trace / belief_state_ref"]
    D0 --> PL0["prediction_ledger<br/>production prediction append-only"]
    B0 --> RPT["daily_score 报告<br/>中文认知状态摘要"]

    BPX["benchmark_policy<br/>AI proxy / benchmark 解释口径"] --> O0
    D0 --> O0["aits feedback calibrate<br/>decision_outcomes<br/>1D / 5D / 20D / 60D / 120D"]
    PL0 --> PO0["aits feedback calibrate-predictions<br/>prediction_outcomes"]
    O0 --> CA0["aits feedback build-causal-chain<br/>decision_causal_chains<br/>signal_time_context / post_signal_observations"]
    CA0 --> CAQ["aits feedback lookup-chain<br/>按 chain_id 查询因果链"]
    CA0 --> L0["aits feedback build-learning-queue<br/>learning_queue<br/>错误归因和成功样本归因"]
    L0 --> LQ["aits feedback lookup-learning<br/>按 review_id 查询复核项"]
    L0 --> LR0["aits feedback loop-review<br/>周期复核报告<br/>证据 / 快照 / outcome / 因果链 / 学习队列 / task register"]
    L0 --> MFO0["aits feedback optimize-market-feedback<br/>市场反馈优化 readiness / as-if 窗口 / 执行频次"]
    BR0["backtest robustness summary<br/>参数扰动、成本压力、再平衡和基线复测"] --> PR0["aits feedback build-parameter-replay<br/>参数复测收益变化报告"]
    PR0 --> PC0["aits feedback build-parameter-candidates<br/>参数候选 trial ledger"]
    PC0 --> PS0["aits feedback run-parameter-shadow<br/>validation-only 参数 challenger 默认写入隔离 ledger"]
    PS0 --> PO0
    PL0 --> SWP0["aits feedback run-shadow-weight-profiles<br/>隔离测试权重与主线评分对比"]
    SWP0 --> PO0
    PC0 --> MFO0
    PR0 --> MFO0
    L0 --> RC0["rule_candidate<br/>候选规则建议"]
    RC0 --> SH0["aits feedback run-shadow<br/>challenger prediction 写入 ledger<br/>不影响 production 输出"]
    SH0 --> PO0
    PO0 --> SM0["aits feedback shadow-maturity<br/>样本成熟度和 promotion readiness"]
    PO0 --> MFO0
    SM0 --> MFO0
    SM0 --> GOV0["rule card + manual approval<br/>批准后才可进入 production rules"]
    GOV0 --> GOV1["aits feedback promote-rule-card / retire-rule-card<br/>受控更新 config/rule_cards.yaml"]

    GOV1 -.-> PROD0["production scoring / position_gate rules"]
```

## 当前已实现与待接入模块

```mermaid
flowchart TD
    subgraph Done["已实现基础版"]
        A["数据下载<br/>aits download-data"]
        B["数据质量门禁<br/>aits validate-data"]
        C["市场特征<br/>aits build-features"]
        D["每日评分<br/>aits score-daily<br/>含结论卡、关注股票趋势分析、产业链节点热度与健康度、组合暴露、risk_budget gate、SEC 基本面、估值快照、政策/地缘发生记录和复核声明、置信度、执行建议和人工复核摘要"]
        E["历史回测<br/>aits backtest<br/>含 point-in-time 输入、覆盖率、来源类型、输入问题、URL、ticker 和证据来源下钻"]
        F["观察池校验<br/>aits watchlist validate"]
        F2["观察池生命周期<br/>aits watchlist validate-lifecycle"]
        G["产业链图校验<br/>aits industry-chain validate"]
        H["交易 thesis<br/>aits thesis list/validate/review"]
        I["风险事件分级<br/>aits risk-events list/validate"]
        I2["风险事件发生记录<br/>aits risk-events list-occurrences/validate-occurrences"]
        I3["风险事件 CSV 导入<br/>aits risk-events import-occurrences-csv"]
        I4["风险事件每日复核声明<br/>aits risk-events record-review-attestation"]
        I5["官方政策/地缘来源抓取<br/>aits risk-events fetch-official-sources<br/>候选只进入 pending_review"]
        J["估值与拥挤度<br/>aits valuation list/validate/review"]
        J3["FMP 估值/预期 API<br/>aits valuation fetch-fmp"]
        J4["FMP 历史估值 API<br/>aits valuation fetch-fmp-valuation-history"]
        J5["EODHD EPS trend baseline<br/>aits valuation fetch-eodhd-trends"]
        J2["估值 CSV 导入<br/>aits valuation import-csv"]
        PF1["组合暴露分解<br/>aits portfolio exposure<br/>真实持仓 CSV 只读报告；缺少文件时 NOT_CONNECTED"]
        EV0["新市场信息证据账本<br/>aits evidence import-csv / validate"]
        FB1["决策快照<br/>decision_snapshot_YYYY-MM-DD.json<br/>保存评分、置信度、仓位、gate、质量和 trace 引用"]
        FB2["结果观察与校准<br/>aits feedback calibrate<br/>生成 decision_outcomes 和 calibration report"]
        FB3["决策因果链 ledger<br/>aits feedback build-causal-chain / lookup-chain<br/>串联 evidence、模块变化、gate、snapshot 和 outcome"]
        FB4["学习复核队列<br/>aits feedback build-learning-queue / lookup-learning<br/>失败和成功样本归因"]
        FB5["反馈闭环周期复核<br/>aits feedback loop-review<br/>汇总证据、快照、outcome、因果链、学习队列和任务状态"]
        FB6["市场反馈优化编排<br/>aits feedback optimize-market-feedback<br/>汇总 readiness、样本限制、as-if 窗口、候选规则、参数候选、shadow 和 overlay"]
        FB7["参数复测收益变化<br/>aits feedback build-parameter-replay<br/>读取 backtest robustness summary，比较 baseline vs 参数候选"]
        BP1["基准政策治理<br/>aits feedback validate-benchmark-policy / lookup-benchmark-policy<br/>AI proxy 与 benchmark 解释口径"]
        SC1["情景压力测试库<br/>aits scenarios validate / lookup<br/>节点、ticker、risk event 和 gate 映射"]
        CT1["未来催化剂日历<br/>aits catalysts validate / upcoming / lookup<br/>5/20/60 天事件前后复核"]
        EX1["执行纪律政策<br/>aits execution validate / lookup<br/>advisory action taxonomy"]
        OPS0["每日运行计划与执行<br/>aits ops daily-plan / daily-run<br/>交易日完整评分；休市日跳过评分并保留风险/缓存健康步骤"]
        OPS1["Pipeline health<br/>aits ops health<br/>关键 artifact + PIT 抓取/快照健康；休市日不要求当日日报"]
        SEC1["Secret hygiene<br/>aits security scan-secrets<br/>配置、文档、报告和 trace 脱敏扫描"]
        K["交易复盘归因<br/>aits review-trades"]
        L["日报集成<br/>汇总 thesis、风险规则与发生记录、估值和复盘摘要"]
        M["数据源目录<br/>aits data-sources list/validate"]
        M2["数据源健康与 reconciliation 覆盖<br/>aits data-sources health"]
        N["基本面一手数据<br/>aits fundamentals list-sec-companies / download-sec-companyfacts / download-sec-submissions / download-sec-filing-archive"]
        O["SEC 基本面指标摘要<br/>aits fundamentals extract-sec-metrics / validate-sec-metrics"]
        P["SEC 基本面特征<br/>aits fundamentals build-sec-features"]
        QP["TSMC IR PDF 文本抽取<br/>aits fundamentals extract-tsm-ir-pdf-text"]
        Q0["TSMC IR 官方页面抓取<br/>aits fundamentals fetch-tsm-ir-quarterly"]
        Q["TSMC IR 季度基本面导入<br/>aits fundamentals import-tsm-ir-quarterly"]
        Q3["TSMC IR 批量季度导入<br/>aits fundamentals import-tsm-ir-quarterly-batch"]
        Q2["TSMC IR 合并到统一指标<br/>aits fundamentals merge-tsm-ir-sec-metrics"]
    end

    subgraph Planned["规划/待接入"]
        R0["认知状态层<br/>COGNITION-001 belief_state<br/>只读，不直接改评分或仓位"]
        R1["证据下游自动联动<br/>LLM-001<br/>不得绕过人工复核"]
        R3["候选规则实验<br/>EXPERIMENT-001"]
        R4["规则生命周期治理<br/>GOV-001 rule card / manual approval"]
    end

    C --> D
    C --> OPS0
    C --> OPS1
    C --> SEC1
    OPS0 --> A
    OPS0 --> N
    OPS0 --> O
    OPS0 --> J3
    OPS0 --> D
    OPS0 --> OPS1
    OPS0 --> SEC1
    EX1 --> D
    D --> FB1
    FB1 --> FB2
    BP1 --> FB2
    FB2 --> FB3
    FB3 --> FB4
    FB4 --> FB5
    FB4 --> R3
    D --> E
    BP1 --> E
    F --> H
    F2 --> E
    G --> H
    G --> I
    I --> I2
    I3 --> I2
    H --> I
    M --> J3
    M --> J4
    M --> M2
    J4 --> J3
    J4 --> J
    J3 --> J
    J2 --> J
    M --> PF1
    PF1 --> D
    EV0 --> R0
    I --> J
    I2 --> L
    J --> K
    M --> C
    M --> N
    M --> QP
    QP --> Q
    M --> Q0
    M --> Q
    M --> Q3
    N --> O
    Q0 --> Q2
    Q --> Q2
    Q3 --> Q2
    O --> P
    Q2 --> P
    P --> D
    H --> L
    I --> L
    J --> L
    K --> L
    L --> D
    D --> R0
    H --> R0
    I2 --> R0
    J --> R0
    R1 --> R0
    FB1 --> FB2
    R0 --> FB2
    FB2 --> R3
    R3 --> R4
```

## 文件和命令责任表

|层级|命令或文件|责任|当前状态|
|---|---|---|---|
|学习入口|`docs/learning_path.md`|按使用者阅读顺序解释系统做什么、不做什么，以及数据来源、质量门禁、特征、评分、权重、仓位 gate、日报、trace、ledger、shadow 参数搜索和 feedback 的关系；不替代本文的工程事实维护责任|已实现基础版|
|计算逻辑说明|`docs/calculation_logic.md`|面向无金融背景读者解释价格、收益率、移动平均、相对强弱、VIX、利率、基本面、估值、confidence、gate 和 position，并按输入、输出、计算逻辑、原因、设计思路和常见误解说明数据质量门禁、feature building、signal normalization、component score、overall score、仓位映射、macro budget、position gates、日报/snapshot/trace/ledger 与 shadow 参数搜索；不改变任何 production 计算|已实现基础版|
|产物目录|`docs/artifact_catalog.md`|按 artifact 说明生成者、上游输入、关键字段或内容、下游使用、production_effect 和常见误解；帮助使用者看到文件后判断其来源、用途和是否影响 production|已实现基础版|
|字段字典|`docs/schema/fields.yaml`|机器可读字段解释字典，覆盖 `scores_daily.csv`、decision snapshot、trace bundle、prediction ledger 和 shadow parameter search 核心字段；记录 meaning、produced_by、upstream_fields、downstream_usage、production_effect 和 common_misunderstanding；只做文档解释，不改变 schema 或运行逻辑|已实现基础版|
|数据源|FMP / Cboe VIX / Marketstack / FRED|FMP 提供股票/ETF 主价格；Cboe VIX official historical data 提供内部 `^VIX`；Marketstack 提供股票/ETF 第二行情源；FRED 提供 DGS2、DGS10 和 `DTWEXBGS` 广义美元指数原始输入|已实现基础版；owner 2026-05-10 决定暂无新增 macro/price qualified source 计划；主源异常或跨源 raw close 未解决冲突仍阻断质量门禁，第二源自身异常默认进入可审计 INFO/reconciliation，宏观单源限制继续披露|
|下载|`aits download-data`|拉取并标准化为本地 CSV 缓存，同时追加下载审计 manifest；默认要求 `FMP_API_KEY` 写入 FMP 股票/ETF 主价格，并从 Cboe 补 `^VIX` 到主价格缓存；默认要求 `MARKETSTACK_API_KEY` 写入 Marketstack 第二行情源缓存，临时无 Marketstack key 环境必须显式 `--without-marketstack`；Yahoo 仅可通过 `--price-provider yahoo` 显式迁移调查使用；FRED response 前网络异常使用 60 秒 timeout、最多 2 次尝试并记录 attempt 诊断；失败时写入 `download_data_diagnostics_YYYY-MM-DD.md`，记录 provider、失败阶段、cache status、cache key、脱敏请求参数和下游影响，不保存 secret、stdout/stderr 原文或供应商响应正文|已实现基础版|
|原始缓存|`data/raw/prices_daily.csv`|FMP 股票/ETF 日线 OHLCV 和调整收盘价主缓存，加 Cboe `^VIX` OHLC；价格主源、Marketstack 第二源和跨源核验问题需在质量报告中分源归因|已实现基础版；主源异常仍阻断质量门禁|
|原始缓存|`data/raw/prices_marketstack_daily.csv`|Marketstack 股票/ETF 日线第二来源缓存，用于 cross-provider reconciliation；self-check 异常默认作为第二源健康告警，不覆盖主价格缓存|已实现基础版|
|原始缓存|`data/raw/rates_daily.csv`|FRED 宏观序列长表，当前包含 DGS2、DGS10 和 `DTWEXBGS`；`DTWEXBGS` 不是 ICE DXY|已实现|
|下载审计|`data/raw/download_manifest.csv`|记录 provider、endpoint、请求参数、下载时间、行数、输出路径和 checksum|已实现|
|质量门禁|`aits validate-data`|校验 schema、完整性、新鲜度、重复键、异常值；价格波动、复权比例和主价格缓存与 Marketstack 第二来源 reconciliation 默认只统计 `config/data_quality.yaml:prices.consistency_start_date` 以来样本；宏观 freshness 先用 `config/data_quality.yaml:rates.max_stale_calendar_days`，再应用 `rates.series_overrides` 的 series 级阈值，当前 `DTWEXBGS` 因 Federal Reserve H.10 周度发布机制允许更长日历滞后，DGS2/DGS10 仍保持默认阈值；宏观单日变化默认只统计 `config/data_quality.yaml:rates.consistency_start_date` 以来样本；配置化指数 volume 缺失、已知拆股复权跳变、已知拆股窗口内主源/二源 raw close 日期口径差异、Marketstack 自身坏点和 raw close 已核验的 adjusted close 分红复权口径差异进入 INFO 与 reconciliation 记录；主源错误、第二源缺失/不可读、重叠覆盖不足和 raw close 跨源未解决冲突仍 fail closed|已实现|
|质量报告|`outputs/reports/data_quality_YYYY-MM-DD.md` / `data_quality_YYYY-MM-DD_marketstack_reconciliation.csv`|声明数据是否可用于下游结论，显示价格一致性和宏观变化检查窗口，并在问题表标注价格主源、第二行情源、跨源核验、FRED 宏观序列或下载审计清单来源；Marketstack reconciliation CSV 逐行记录 ticker/date、主/二源数值、分类规则、证据和 severity，不改写任何价格缓存|已实现|
|PIT 特征可见时间目录|`config/feature_availability.yaml` / `outputs/reports/feature_availability_YYYY-MM-DD.md`|统一记录价格、宏观、观察池、SEC/TSM 基本面、估值、风险事件和市场证据等输入族的 `event_time`、`source_published_at`、`available_time`、`decision_time`、默认保守滞后和缺少可见时间时的 A/B 级使用策略；`build-features`、`score-daily`、`backtest` 会写出 PIT 特征可见时间报告，报告包含字段级 source 检查、`available_time` 覆盖率、未来可见时间行数和保守 fallback 策略，失败时停止，trace bundle 记录该目录摘要|已实现基础版|
|特征|`aits build-features`|先执行数据质量门禁，再生成可解释市场特征，并输出 PIT 特征可见时间报告；缺少 availability rule 的 source 会 fail closed，特征摘要引用该报告|已实现|
|特征缓存|`data/processed/features_daily.csv`|保存 tidy 格式特征|已实现|
|组合与风险预算配置|`config/portfolio.yaml`|定义静态总风险资产预算、`macro_risk_asset_budget` 下调阈值、AI 总资产上限、真实组合集中度提示阈值和 `risk_budget` gate 参数；宏观预算层用 VIX、DGS10 和 `DTWEXBGS` 广义美元指数下调总风险资产预算，`risk_budget` gate 继续约束风险资产内 AI 仓位上限|已实现基础版|
|评分规则配置|`config/scoring_rules.yaml`|定义评分模块、signal 规则、score->position band、日报结论边界、confidence cutoff、confidence cap multiplier、source_type confidence、仓位动作阈值和 position gate 上限；带 `policy_metadata`，用于追踪 owner/status/rationale/validation；模块权重的生产入口由 `weight_profile_current.yaml` + approved overlay resolver 提供，`scoring_rules.weights` 只作为未接入 resolver 时的 legacy fallback 和信号族基线参考；`score-daily`、回测评分和 robustness 信号族基线不再从散落的 if/else 推断仓位带或结论边界|已实现基础版|
|Shadow weight profile manifest|`config/weights/shadow_weight_profiles.yaml`|维护若干套隔离测试权重 profile，初始值参考生产 `weight_profile_current.yaml`，每套都必须 `production_effect=none`、信号集合与生产 profile 一致、权重和为 1 且不越过生产 profile bounds；用于长期观察、日报级主线评分对比和隔离 prediction outcome，不替换生产权重|已实现基础版|
|Shadow 参数搜索配置|`config/weights/shadow_parameter_search_space.yaml` / `config/weights/shadow_parameter_objective.yaml`|定义 validation-only 参数搜索的权重网格、gate cap 网格、目标函数、验证级样本门槛、gate relaxation / weight distance / changed dimension regularization、生产邻近性限制和 top-N 输出策略；配置 checksum 会写入搜索 manifest 和报告；报告输出 weight-only / gate-only / combined factorial attribution、cap-level attribution 和最终仓位变化解释，未达 objective 门槛时只展示 diagnostic-leading trial，不批准生产替换|已实现基础版|
|Shadow 参数晋级 contract|`config/weights/shadow_parameter_promotion_contract.yaml`|把 search ranking 与生产晋级检查分离；要求 eligible best、样本 floor、正 excess、回撤/换手约束、gate 主导 cap review、forward shadow、owner approval、rollback condition，并保持 `approved_hard_allowed=false`；输出只用于 readiness，不写 production 配置|已实现基础版|
|LLM 请求 profile 配置|`config/llm_request_profiles.yaml`|按请求类型配置 OpenAI Responses endpoint、model、reasoning effort、请求读超时、HTTP client、本地 request cache TTL、最大重试次数、候选上限、官方来源抓取 limit 和 LLM formal 写入参数；`llm precheck-claims`、`risk-events precheck-openai`、`risk-events precheck-triaged-official-candidates`、`score-daily` 和 `daily-run` 默认读取 profile，CLI 显式参数只覆盖本次运行；不开放 prompt/schema 版本配置，避免破坏结构化输出契约|已实现基础版|
|评分|`aits score-daily`|先执行市场数据质量门禁和 PIT feature availability 校验，再校验 `execution_policy`、SEC 指标 CSV、构建 SEC 基本面特征、复核估值快照、风险事件发生记录和当前有效复核声明，读取真实持仓 CSV 生成只读组合暴露；默认会在风险事件发生记录校验前抓取官方政策/地缘来源并调用 OpenAI 风险事件 `metadata_only` 预审，可用 `--skip-risk-event-openai-precheck` 跳过；预审成功后默认按 LLM request profile 的 formal assessment 设置把队列写入 LLM formal occurrence/attestation，full coverage `llm_formal_assessment` 置信度为 65%，低于人工复核但不再触发低置信模块，且不得单独触发 position gate；可选 `--run-id` 会写入日报 trace bundle 和 Decision Card，未传入时保持 `run:daily_score:YYYY-MM-DD`；默认读取 `risk_event_daily_official_precheck` profile，当前成本 pilot 包含 10 条官方候选上限、每源官方抓取 limit 30、`gpt-5.5`、`reasoning.effort=medium`、120 秒读超时、`requests` HTTP client、24 小时本地 agent request cache TTL 和 2 次最大重试；可用 `--llm-request-profile` 切换 profile，或用 `--official-policy-limit`、`--openai-cache-ttl-hours`、`--openai-model`、`--openai-reasoning-effort`、`--openai-timeout-seconds`、`--openai-http-client urllib` 和 `--risk-event-openai-precheck-max-candidates` 覆盖本次运行；完全相同 request payload 在 TTL 内 cache HIT 不重新发送，MISS/EXPIRED 才实际请求并写入 `data/processed/agent_request_cache` 审计归档；provider `cache_allowed=false` 时 fail closed；单个 OpenAI 请求最终失败则整批 fail closed；随后基于已通过校验/复核的市场特征生成只读关注股票趋势分析，并基于市场特征、SEC/TSM/ADR 基本面、估值、风险事件和 thesis 生成只读产业链节点热度与健康度；`watch_only` 观察阶段 ticker 缺少主动交易 thesis 不触发 thesis warning 或 thesis gate，`active_trade` 阶段仍按 thesis 纪律约束；评分模块权重先由 effective weight resolver 读取 `weight_profile_current.yaml` 和 approved overlay 按 as-of/context 解析，approved overlay 中未知 signal fail closed；命中 overlay 可调整 effective weights、confidence delta 和 soft position multiplier，并在日报、CSV 和 trace 中记录 profile version、overlay ids、effective weights 与审计原因；再用 `macro_risk_asset_budget` 下调总风险资产预算，并通过 `position_gate` 把评分仓位、判断置信度、历史校准 overlay soft cap、组合限制、风险预算、风险事件、估值拥挤、thesis 状态和数据置信度取最严格上限，输出 AI 产业链评分、判断置信度、最终仓位区间、advisory 执行建议、日报、decision snapshot、prediction ledger 行和只读 `belief_state`|已实现|
|评分缓存|`data/processed/scores_daily.csv`|保存每日评分结构化结果，component 行记录模块 confidence、`weight_profile_version`、matched overlay ids 和单模块 effective weight；overall 行记录整体 confidence、模型/最终/置信度调整仓位区间、静态和宏观调整后总风险资产预算、总资产 AI 仓位区间、宏观预算触发等级、仓位闸门摘要和 `effective_weights_json`；置信度调整仓位基于评分模型原始仓位计算，并作为 `confidence` gate 参与最终上限约束，用于日报上期对比|已实现|
|日报|`outputs/reports/daily_score_YYYY-MM-DD.md`|开头输出 Decision Card v2，固定呈现状态标签、市场吸引力、判断置信度、`Data Gate`、`Run ID / Trace`、评分映射仓位、风险闸门后最终仓位、总风险资产预算、执行动作、主结论、三个核心原因、最大限制、下一步触发条件、`Main Invalidator` 和 `Next Checks`；随后输出 `Data Lineage Card`，列出生成命令、market regime、关键输入、关键输出、trace 和 `production_effect`；`Base Signal / Risk Caps` 内含 `Score-to-Position Funnel` 和 `Binding Gate Ladder`，按 component score、effective weights、overall score、score band、confidence、macro risk budget、position gates 和 final position 解释分数到仓位路径，并标明 binding gate；正文继续输出复核五问、结论使用等级、适用范围、变化原因树、什么情况会改变判断、关注股票趋势分析、产业链节点热度与健康度、组合暴露、认知状态摘要、执行建议、宏观风险资产预算、市场数据质量状态、SEC 基本面质量状态、风险事件发生记录状态、当前有效风险事件复核声明数量、估值 PIT 可信度、仓位闸门来源/上限/触发状态、置信度调整后模型仓位、限制说明、人工复核摘要和可追溯引用章节；关注股票趋势分析按 `core_watchlist` 显示逐 ticker 1/5/20 日收益、20/50/100/200 日均线位置、相对均线偏离和数据覆盖；当前项目范围为趋势判断/投研辅助，不触发交易；执行建议、关注股票趋势、节点热度/健康度和组合暴露均明确 `production_effect=none`，不是自动交易指令|已实现|
|结论使用等级|`outputs/reports/daily_score_YYYY-MM-DD.md#结论使用等级` / `outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md#结论使用等级`|报告输出 `trend_only`、`actionable`、`review_required`、`research_only`、`data_limited` 或 `backtest_limited` 等使用边界，并与投资姿态标签分开；当前 `score-daily` 和回测以 `trend_judgment` 范围运行，干净通过时也只能显示“趋势判断，不触发交易”，不能自动升级为仓位复核或交易执行；低置信度、人工复核失败、来源不足、数据质量失败和回测覆盖不足会自动降级，说明原因、解除条件和证据引用|已实现基础版|
|每日运行计划|`aits ops daily-plan`|生成本地或云 VM 可用的每日运行计划；未显式传入 `--as-of` 时，默认按 `America/New_York` 的 U.S. equity market 日历选择最新已完成交易日，常规交易日美东 16:30 之后使用当日，16:30 前、周末或 NYSE 常规整日休市日使用上一交易日；随后判断 U.S. equity market session，并在报告中声明 `TRADING_DAY` 或 `CLOSED_MARKET`、休市原因、上一交易日和日历来源；交易日列出 `download-data`、带 `--continue-on-failure` 的 `pit-snapshots fetch-fmp-forward`、SEC companyfacts 刷新、SEC metrics 抽取/校验、FMP 估值快照刷新、`score-daily`、`feedback optimize-market-feedback`、`feedback loop-review`、weekly `reports investment-review`、`reports dashboard`、`ops health` 和 `security scan-secrets` 的顺序、必需环境变量、预期 artifact、质量门禁和阻断关系；休市日若上一交易日价格缓存已覆盖则跳过 `download-data`，否则只用上一交易日作为 `download-data --end`，并默认跳过 `score-daily`、复盘前置报告和 dashboard，不生成新日报评分、decision snapshot、evidence bundle、执行动作或网页展示层；只做计划和环境变量非空检查，不执行下载、API 调用、评分或报告生成；缺少关键环境变量时显示 `BLOCKED_ENV`，可用 `--fail-on-missing-env` 作为调度前门禁|已实现基础版|
|每日运行执行器|`aits ops daily-run`|复用 `daily-plan` 的步骤顺序真实调用本地 CLI，未显式传入 `--as-of` 时使用同一最新已完成美股交易日 resolver；先写计划报告并执行输入可见性预检查，再执行交易日完整链路，并在 `score-daily` 成功后先自动生成同日 `parameter_governance`、`market_feedback_optimization`、`feedback_loop_review` 和 weekly `investment_review`，再调用 `aits reports dashboard` 生成只读 HTML/JSON 展示层；执行报告和本轮子报告收录进 canonical run bundle 后，额外生成 `daily_task_dashboard_YYYY-MM-DD.html/json` 每日关键结论页；`daily-run` 是生产调度入口，不用于历史时点复现，输入可见性预检查同样按最新已完成美股交易日判断，显式未来 `as_of` 或历史 `as_of` 返回 `BLOCKED_VISIBILITY`，不执行任何 download/PIT/SEC/valuation/OpenAI/dashboard 子命令，历史日期提示改用 `aits ops replay-day --mode cache-only`；休市日执行官方政策/地缘来源抓取、PIT、SEC companyfacts、SEC metrics 抽取/校验、FMP valuation snapshots、`ops health --non-trading-day` 和 secret scan，同时显式跳过 `score-daily`、复盘前置报告和 dashboard；默认写入 canonical run bundle，可用 `--run-output-root` 指定根目录、`--run-id` 固定运行标识、`--legacy-output-mode mirror/off` 控制旧 `outputs/reports` 兼容镜像；canonical run bundle 只从 legacy 报告路径收录本轮运行开始后实际产出的同日 artifact，避免 skipped dashboard 等旧文件误入本轮输出；执行器内部优先用项目 `.venv` Python 调用 daily-run direct dispatcher，找不到本地虚拟环境时才回退当前 Python，避免 Windows 上从 `aits.exe` 父进程递归启动 `aits.exe`、PATH 上的全局 `aits` 污染每日子命令环境，以及 Typer 对整棵 CLI 做全局解析时触发的 Windows 原生崩溃；direct dispatcher 是 daily-run 子命令薄适配层，会把 `score-daily --skip-risk-event-openai-precheck` 映射到当前 `score_daily(risk_event_openai_precheck=False)`，并传递 `--llm-request-profile`，候选上限未显式覆盖时由 `score-daily` profile 解析，且支持 dashboard 前置的 feedback/report 子命令，避免主 CLI 签名演进阻断生产调度或把默认请求策略重新硬编码；子命令环境显式设置 `PYTHONMALLOC=malloc`、`PYTHONFAULTHANDLER=1`、`PYTHONDONTWRITEBYTECODE=1`，为每次 `daily-run` 使用独立 `PYTHONPYCACHEPREFIX=outputs/tmp/pycache/daily_run/run_*`，并在启动子命令前清理 `src/**/__pycache__`，降低 Windows 本机长流程子进程原生崩溃和字节码缓存异常风险；缺少阻断性环境变量时返回 `BLOCKED_ENV`；任一执行步骤退出码非 0 或关键 artifact 报告状态非 `PASS*` 时停止，不继续下游步骤；dashboard、每日关键结论页和复盘前置报告均为只读展示/复核层，失败只进入运行报告或命令退出状态，不改变已生成的评分、仓位、回测或执行建议；显式 `SKIPPED` 步骤在计划、执行报告和每日关键结论页中保留限制声明，不得被解释为生成了投资结论|已实现基础版|
|每日运行报告|`outputs/runs/daily/<executed_at_utc>/as_of_YYYY-MM-DD__<safe_run_id>/reports/daily_ops_plan_YYYY-MM-DD.md` / `reports/daily_ops_run_YYYY-MM-DD.md` / `metadata/daily_ops_run_metadata_YYYY-MM-DD.json`，默认镜像到 `outputs/reports/daily_ops_*`|计划报告中文输出计划状态、评估日期、market session、上一交易日、休市原因、必需环境变量是否可见、逐步骤命令、输入可见性类别、输出路径、质量门禁和显式跳过声明；执行报告中文输出真实执行状态、开始/结束时间、退出码、耗时、stdout/stderr 行数、输入可见性阻断和预期 artifact 路径，不保存 stdout/stderr 原文、API key、token 或付费内容原文；`download_data` 失败时预期产物包含 `download_data_diagnostics_YYYY-MM-DD.md`，用于定位 provider/cache 阶段但不改变 fail-closed 行为；metadata sidecar 结构化记录 run id、git commit/dirty diff hash、config/rule card hash、命令清单、必需 env presence、input visibility status/issues、production visibility cutoff、pre-run input checksum、step result 摘要和 produced artifact checksum，不保存 secret 值、stdout/stderr 原文或付费内容原文；当前仍未接入主动 GitHub Actions 生产 cron、通知或云备份|已实现|
|每日关键结论 Dashboard|`aits reports daily-tasks` / `outputs/runs/daily/<executed_at_utc>/as_of_YYYY-MM-DD__<safe_run_id>/reports/daily_task_dashboard_YYYY-MM-DD.html` / `.json`，默认镜像到 `outputs/reports/daily_task_dashboard_*`|读取 `daily_ops_run_metadata_YYYY-MM-DD.json`、执行报告路径、本轮同日子报告、evidence dashboard JSON、最近一个 search window 不晚于 as-of 的 shadow parameter search bundle，以及该 search window 内 production decision snapshots；首屏输出 `key_conclusions`，按投资结论、数据可信度、参数治理、反馈复盘、运行健康汇总关键业务结论、支撑要点、重点风险和来源步骤；反馈复盘会展示 shadow parameter 诊断领先 trial、shadow vs production return、excess、主因、关键 cap、promotion 状态和阻断条件，并先输出 production/current vs shadow candidate 的结果对比（Total return、Max drawdown、Turnover、Beat rate、样本覆盖和 return 计算口径），再把参数对比拆成 Gate cap override 与权重参数两个分区；Gate cap override 的 production/current 列展示回测窗口内实际 production gate cap 数值或区间，声明诊断领先不等于可上线；子任务详情入口以卡片形式展示阶段性结论、风险和同目录 Markdown/HTML/JSON 子报告相对链接，作为后续 PIT、SEC、valuation、score、feedback、health、secret hygiene 专属网页的统一入口；运行状态摘要、失败/跳过/风险限制计数、visibility cutoff、逐步骤状态、耗时、return code、stdout/stderr 行数、输入可见性类别和详情报告路径后置为审计/排错信息；缺失子报告、`PASS_WITH_*`、`ACTIVE_WARNINGS` 或 `SKIPPED` 均显式进入风险区；该页不替代 evidence dashboard 的投资论证，也不替代各子任务 Markdown/HTML 审计源|已实现基础版|
|每日运行 Bundle|`outputs/runs/daily/<executed_at_utc>/as_of_YYYY-MM-DD__<safe_run_id>/manifest.json` / `reports/` / `traces/` / `metadata/`|`daily-run` 的 canonical artifact bundle；第一层 `<executed_at_utc>` 表示本轮实际执行 UTC 时间，第二层同时记录评估日 `as_of` 和 safe run id，便于同一 as-of 多次运行按执行时间归档；manifest 记录 schema version、run id、safe run id、execution timestamp、as_of、run root、状态、visibility cutoff、legacy output mode、输入 artifact、canonical 输出 artifact、legacy mirror artifact、checksum、size 和 file count；artifact 记录由 `ArtifactRef` 统一生成；`data/raw` 与 `data/processed` 仍是状态缓存和校验引用来源，不是每轮完整复制的归档副本，bundle 只归档本次运行报告、trace 和 metadata；legacy mirror 迁移期保留旧路径可读性|已实现基础版|
|文档新鲜度检查|`aits docs validate-freshness` / 可选 `outputs/reports/docs_freshness_YYYY-MM-DD.md`|CI 中检查 task register、implementation backlog、runbook 和 `docs/requirements/*.md` 是否包含 `最后更新：YYYY-MM-DD`，且该日期不得早于文档内部最新状态记录日期；失败时命令返回非 0，防止任务状态和需求文档继续漂移|已实现|
|历史交易日归档回放|`aits ops replay-day`|单日 cache-only replay 入口，默认不调用 live provider 或 OpenAI；默认可见窗口优先读取 production `daily_ops_run_metadata_YYYY-MM-DD.json` 的 `visibility_cutoff`，缺失时退回 as-of 当日 UTC 末尾；先生成 input freeze manifest，`prices_daily.csv`、`prices_marketstack_daily.csv` 和 `rates_daily.csv` 会按 `date <= as_of` 写入 replay raw cache，并生成 replay 专用 download manifest，避免生产缓存中的未来行情/宏观行触发历史复现误读；PIT manifest/normalized 使用不晚于 as-of 日末的 effective input cutoff 过滤，production normalized CSV 已被补跑覆盖为未来 `available_time` 时会从 filtered raw manifest 按 ticker 选择 as-of 当天最新 raw payload 重建 replay-scoped normalized；valuation snapshots、`risk_event_occurrences`、`trade_theses` 和 `trades` 均写入 as-of 可见隔离视图，`score-daily` 通过 path override 读取 replay bundle；thesis 按 `created_at/status_updated_at` 和嵌套 `updated_at/triggered_at` 整条过滤，交易记录按 `recorded_at/opened_at/closed_at/updated_at` 过滤且未来平仓信息在 replay 视图中还原为未平仓；随后把 `score-daily`、`reports dashboard`、`ops health` 和 secret scan 的输出写入 `outputs/replays/YYYY-MM-DD/<run-id>/`；`features_daily.csv`、`scores_daily.csv`、daily score、alerts、decision snapshot、evidence bundle、dashboard HTML/JSON、prediction ledger、PIT manifest、market/macro raw cache、valuation snapshots、risk event occurrences、trade thesis 和 trade records 均不改写生产路径；缺少关键输入或可见窗口内无数据时 fail closed，手工输入缺失或被全量排除会进入限制/警告而不是读取 production 目录；`--inventory-only` 只生成输入清单和诊断；`--compare-to-production` 生成本地 production artifact 与 replay artifact 的结构化 checksum/row diff；OpenAI replay 默认为 `disabled`，`--openai-replay-policy cache-only` 读取历史预审队列和报告，但只复用 `request_timestamp/cache_created_at` 或匹配 cache 文件中的生成/请求时间不晚于 effective OpenAI replay cutoff 的记录，晚于 cutoff 或缺少可证明时间戳的记录进入排除审计，不调用 live OpenAI|已实现|
|历史交易日批量回放|`aits ops replay-window`|按 U.S. equity trading day 枚举日期窗口，逐日复用 `replay-day` 的 cache-only 输入冻结和隔离输出；周末和 NYSE 常规整日休市日默认跳过并记录原因；默认某个交易日失败即停止，`--continue-on-failure` 可继续后续交易日；窗口报告只做索引和状态汇总，不改写任何 production artifact|已实现基础版|
|历史回放报告|`outputs/replays/YYYY-MM-DD/<run-id>/replay_run.md` / `input_freeze_manifest.csv` / `diff_vs_production.md`；`outputs/replays/windows/<run-id>/replay_window.md`|中文输出 replay 状态、as-of、run id、visible cutoff、方法边界、输入冻结清单、被排除的未来 PIT/valuation/manual input 数量、子命令状态、stdout/stderr 行数和关键 replay 输出路径；production diff 报告输出日报、alerts、dashboard、decision snapshot、trace、features/scores 当日行等比较状态、row count 和 checksum；窗口报告输出交易日状态、diff 状态、bundle/report 路径和跳过日期；结构化 JSON 同步保存，不包含 API key、token 或付费内容原文|已实现基础版|
|Pipeline health|`aits ops health`|只读检查关键 pipeline artifact；交易日检查价格缓存、利率缓存、数据质量报告、特征缓存、评分缓存、日报、FMP PIT 抓取报告、PIT manifest、PIT 质量报告和 FMP PIT normalized as-of CSV 是否存在、是否为空、mtime、row count、`available_time` 新鲜度、raw payload checksum 和 FMP PIT 抓取报告状态；当 `as_of` 等于当前最新已完成美股交易日时，PIT 新鲜度按本次 health 运行时间作为 production visibility cutoff，允许收盘后/JST 次日生成的 forward-only PIT；历史 as-of 未进入 production cutoff 时仍按 as_of 严格阻断未来 `available_time`；休市日可用 `--non-trading-day`，不要求当日 data_quality、features、scores 或 daily_score 报告存在，但仍检查市场缓存和 PIT 健康；不把运行健康解释为投资结论有效|已实现基础版|
|Pipeline health 报告|`outputs/reports/pipeline_health_YYYY-MM-DD.md`|中文输出 market session、artifact 检查表、PIT 抓取失败、PIT 缺跑/断更/row count/checksum 问题、错误/警告数量、问题清单和方法边界；第一阶段未接入结构化 run log、后台调度器、异常栈或 API 错误采集|已实现基础版|
|Pipeline health 告警|`outputs/reports/pipeline_health_alerts_YYYY-MM-DD.md`|`aits ops health` 把失败或警告的 health check 转成只读 data/system alert，记录触发/解除条件、claim/evidence 引用和去重键；`production_effect=none`，不改变评分、仓位、回测或执行建议|已实现基础版|
|Secret hygiene 扫描|`aits security scan-secrets`|扫描配置、文档、报告、manifest、trace bundle 等文本文件中的疑似 API key、token、secret、password 或 bearer credential；报告只输出脱敏片段，不输出完整疑似密钥|已实现基础版|
|Secret hygiene 报告|`outputs/reports/secret_hygiene_YYYY-MM-DD.md`|中文输出扫描入口、扫描文件数、疑似 secret 脱敏问题清单和方法边界；第一阶段不替代企业密钥管理、pre-commit hook、CI secret scan 或供应商权限审批|已实现基础版|
|产业链节点热度与健康度|`score-daily` 日报章节 / `backtest_daily_*.csv` / 回测报告摘要|基于 `config/industry_chain.yaml`、`config/watchlist.yaml`、已通过门禁的市场趋势特征、SEC/TSM 基本面特征、估值快照、风险事件发生记录和 thesis 复核，按节点输出热度等级、市场覆盖率、集中度、健康度、健康覆盖率、支持项、风险/限制和数据缺口；`watch_only` ticker 用于行业趋势观察，缺少主动交易 thesis 不作为仓位约束；回测中按 `signal_date` 重建并在每日明细保存 top 节点、热度、健康等级和数据缺口，同时输出历史状态摘要；只做解释和诊断，不把价格热度写成基本面健康度，也不把估值拥挤或风险事件写成基本面证伪；不改变 production scoring、`position_gate`、回测仓位或执行建议|已实现基础版|
|关注股票趋势分析|`score-daily` 日报章节 / `daily_score` trace bundle|基于 `config/universe.yaml` 的 `ai_chain.core_watchlist` 和已通过门禁的 `features_daily` 价格/趋势特征，逐 ticker 输出 1/5/20 日收益、20/50/100/200 日均线位置、相对 50/200 日均线偏离、趋势状态和缺失窗口；只做趋势判断解释，`production_effect=none`，不改变评分、仓位闸门、执行建议或 prediction ledger|已实现基础版|
|组合暴露分解|`aits portfolio exposure` / `score-daily` 日报章节|基于 `data/external/portfolio_positions/current_positions.csv` 或显式传入的真实持仓 CSV，按 ticker、产业链节点、地区、客户链、因子和相关性簇分解 AI 名义暴露；缺少持仓文件时显示 `NOT_CONNECTED`，存在但格式错误时停止；不得用观察池、模型建议仓位或 AI 产业链评分替代真实账户持仓|已实现基础版|
|组合暴露报告|`outputs/reports/portfolio_exposure_YYYY-MM-DD.md`|中文输出持仓快照日期、总市值、AI 名义暴露、AI 占比、最大单票占 AI 暴露、ETF beta 覆盖率、暴露分组表和问题清单；第一阶段 `production_effect=none`，不改变评分、仓位闸门、执行建议或回测仓位|已实现基础版|
|风险预算 gate|`score-daily` / `backtest` 仓位闸门|在共享 `position_gate` 层读取 `config/portfolio.yaml:risk_budget`；高 VIX 或高 VIX 分位会压低最终 AI 仓位上限，真实持仓接入后单票、节点、相关性簇集中或 ETF beta 覆盖不足也会压低上限；缺少真实持仓时不使用观察池替代组合集中度|已实现基础版|
|日报 Evidence Bundle|`outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`|记录日报 `claim`、`evidence`、`dataset`、`quality` 和 `run_manifest`；`run_manifest.run_id` 可由 `score-daily --run-id` 或 `daily-run --run-id` 贯穿到 Decision Card；bundle 包括 `belief_state`、关注股票趋势分析 dataset/claim 引用、本次运行适用的 production rule version manifest 和 weight calibration 参数，用于从核心结论反查输入上下文、数据快照、effective weights、只读认知状态和规则版本|已实现|
|决策快照|`data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`|每次 `score-daily` 通过质量门禁后保存 signal_date、market regime、整体分、模块分、判断置信度、模型/最终/置信度调整仓位、静态和宏观调整后总风险资产预算、position gates、质量状态、人工复核、估值状态、风险事件状态、trace bundle 引用、`belief_state_ref`、rule version manifest 和配置路径|已实现基础版|
|当前校准上下文与有效权重|`outputs/current_context.json` / `outputs/current_effective_weights.json`|默认 `score-daily` 在生产输出路径下写出最近一次校准匹配上下文和 resolver 结果，供 `aits feedback apply-calibration-overlay`、市场反馈 dashboard 和人工审计读取；自定义测试/临时输出不覆盖全局 current 文件；当没有 approved overlay 时，独立 overlay 命令允许缺少 current context 并只输出 base/effective weights 相等的审计结果；一旦存在 approved overlay 但缺 context，仍然 fail closed；该产物不批准 overlay、不改变 production scoring 或 position gate|已实现基础版|
|Shadow weight profile 观察|`aits feedback run-shadow-weight-profiles` / `data/processed/shadow_weight_profile_observations.csv` / `outputs/reports/shadow_weight_profiles_YYYY-MM-DD.md`|读取 production `decision_snapshot` 的组件分数和当前 gate，按 `shadow_weight_profiles.yaml` 中每套测试权重计算 shadow overall score、score delta、模型仓位和 gate 后观察仓位；默认只 upsert 独立 observation ledger，可显式提供隔离 prediction ledger 路径写入 `production_effect=none` challenger prediction，再由 `calibrate-predictions` 和 `shadow-maturity` 长期观察；observation ledger 同步记录 production/shadow 的模型目标仓位和 gate 后目标仓位；不修改 production `weight_profile_current.yaml`、approved overlay、正式 prediction ledger、日报结论或仓位 gate|已实现基础版|
|Shadow weight 表现评估|`aits feedback evaluate-shadow-weight-performance` / `outputs/reports/shadow_weight_performance_YYYY-MM-DD.md` / `.csv`|读取隔离 shadow weight observation ledger 和价格缓存，按同一 signal date 比较 production gate 后仓位与 shadow gate 后仓位的 position-weighted return、最大回撤、换手和显式成本，输出 return-leading profile；该报告只用于 validation 调参方向，不批准 overlay、不写生产权重、不改变正式 prediction ledger、日报结论或仓位 gate|已实现基础版|
|Shadow 参数搜索器|`aits feedback search-shadow-parameters` / `outputs/parameter_search/<run_id>/manifest.json` / `trials.csv` / `pareto_front.csv` / `best_profiles.yaml` / `search_report.md`|读取指定区间的 production decision snapshots、价格缓存、生产权重 profile、搜索空间和目标函数，枚举 shadow weight/gate 组合并计算 position-weighted return、最大回撤、换手、beat rate、objective、生产邻近性指标和 Pareto front；输出 source weight / price / decision snapshot checksum、resolver version、git commit sha、dirty worktree 标记、factorial attribution、cap-level attribution 和最终仓位变化解释；不写 production 权重、approved overlay、正式 prediction ledger、日报结论或仓位 gate|已实现基础版|
|Shadow 参数 promotion contract|`aits feedback evaluate-shadow-parameter-promotion` / `outputs/parameter_search/<run_id>/shadow_parameter_promotion_<run_id>.md/json`|读取 search bundle 和 `shadow_parameter_promotion_contract.yaml`，输出 `NOT_PROMOTABLE`、`READY_FOR_FORWARD_SHADOW` 或 `READY_FOR_OWNER_REVIEW`；无 eligible best、样本不足、非正 excess、回撤/换手恶化、gate 主导未完成 cap review、缺 forward shadow、缺 owner/rollback 或 approved_hard 未接入时不得给 production-ready 结论；`production_effect=none`|已实现基础版|
|Prediction / shadow ledger|`data/processed/prediction_ledger.csv` / `data/processed/prediction_ledger_flow_validation.csv`|每次 `score-daily` 通过质量门禁后追加 production prediction 行到正式 `prediction_ledger.csv`，记录 run id、model/rule version、candidate_id、`production_effect`、features/data/trace 引用、decision_time、signal、score、confidence、模型目标仓位和 gate 后仓位；`aits feedback run-shadow` 可从 production `decision_snapshot` 和 trace 派生 challenger prediction 行并强制 `production_effect=none`；`aits feedback run-parameter-shadow` 默认把 validation-only 参数 challenger 写入隔离 `prediction_ledger_flow_validation.csv`，避免 flow validation 默认污染正式 production ledger，显式传入 ledger 路径时仍由调用方承担隔离责任；后验 outcome 字段初始为 `PENDING`，不得改写 signal-time 输入|已实现基础版|
|证据下钻 dashboard|`aits reports dashboard` / `outputs/reports/evidence_dashboard_YYYY-MM-DD.html` / `outputs/reports/evidence_dashboard_YYYY-MM-DD.json`|读取日报 Markdown、日报 evidence bundle、decision snapshot、可选 belief_state、可选 `alerts_YYYY-MM-DD.md`、可选 `scores_daily.csv`、同日 `market_feedback_optimization_YYYY-MM-DD.md`、`feedback_loop_review_YYYY-MM-DD.md` 和 `investment_weekly_review_YYYY-MM-DD.md`，生成本地静态 HTML 与 JSON payload；交易日 `daily-run` 会在 dashboard 前生成这三份复盘报告，手工单独运行 dashboard 时若报告缺失则降级显示未接入；顶部 Decision Card 展示执行动作、最终仓位、总风险预算、置信度、Data Gate、最大限制、上期变化、Top evidence、invalidators 和 Next Checks；后续按快速读者、投资复核者和系统审计者分层展示论证链、仓位 gate、thesis/risk/valuation 状态、claim/evidence/dataset/quality refs、输入路径、row count、checksum、告警聚合、近 20 个交易日趋势、反馈复盘/学习闭环摘要和 trace lookup 命令；缺少 alerts、历史 CSV 或反馈复盘报告时降级显示限制；`production_effect=none`，不改变评分、仓位、回测或执行建议，也不替代 Markdown 日报和 trace bundle 的审计责任|已实现基础版|
|决策结果校准|`aits feedback calibrate`|先校验 `benchmark_policy`，再复用 `aits validate-data` 同一质量门禁，从历史 `decision_snapshot` 和 `prices_daily.csv` 生成 1D/5D/20D/60D/120D outcome，按总分、置信度、gate、thesis、风险等级和估值状态分桶输出校准报告；结果只能进入规则复核，不能自动修改生产规则|已实现基础版|
|Prediction outcome 校准|`aits feedback calibrate-predictions`|先复用 `aits validate-data` 同一质量门禁，从 append-only prediction ledger 和 `prices_daily.csv` 生成指定 horizon 的 `prediction_outcomes.csv`，按 candidate、model version、production/shadow、置信度和 benchmark excess return 分桶输出报告；结果只能进入 promotion gate、复盘和规则治理，不能改写 prediction ledger 的 signal-time 字段|已实现基础版|
|调权协议校验|`aits feedback validate-calibration-protocol` / `outputs/reports/calibration_protocol_YYYY-MM-DD.md`|读取调权实验 protocol manifest，校验必填数据/配置版本、`ai_after_chatgpt` 日期范围、nested walk-forward、purging/embargo、trial 次数、benchmark set、参数分层、多重测试折扣和 `production_effect=none` 边界；通过只表示实验协议可进入后续研究，不批准 overlay、不改变 production scoring、position_gate 或回测仓位|已实现基础版|
|参数 replay 收益变化报告|`aits feedback build-parameter-replay` / `outputs/reports/parameter_replay_YYYY-MM-DD.md` / `.json`|读取已通过回测链路生成的 `backtest_robustness_*.json`，把模块权重扰动、再平衡频率、成本压力、起点后移、fixed/vol-targeted/no-gate exposure、信号族基线、alpha/risk/gate 架构基线、same-turnover/same-exposure random 和 OOS holdout 等场景接入 feedback 闭环；输出 baseline vs challenger 的总收益、回撤、Sharpe、换手、bootstrap CI 和 material delta 标记，同时汇总 `robustness_evidence`：data quality / data credibility grade、coverage/source veto、有效独立窗口、OOS 退化、random percentile、signal-family / score-architecture baseline 是否被基础策略跑赢、bootstrap 与明确标注为 proxy 的 Deflated Sharpe / PBO 统计证据状态；material 判定优先使用 robustness summary 内嵌 policy，旧 summary 缺少 policy 时读取当前 `config/backtest_validation_policy.yaml` 并披露 limitation；只做 candidate-only 解释，不能自动生成 approved overlay 或修改 production scoring|已实现|
|参数候选台账|`aits feedback build-parameter-candidates` / `data/processed/parameter_candidates.json` / `outputs/reports/parameter_candidates_YYYY-MM-DD.md`|读取 `parameter_replay_YYYY-MM-DD.json`，把每个参数复测场景登记为 trial，并只将可评估的参数类场景登记为 candidate-only 参数候选；记录 candidate id、linked trial、来源场景、收益差异、回撤变化、换手、material 标记、data quality、data credibility、coverage/placeholder/source veto、有效独立窗口、OOS、same-turnover random、signal-family baseline、score-architecture baseline、bootstrap CI、label horizon、veto reasons、recommendation status、`evaluation_mode`、shadow/governance 状态和下一步；默认 `strict` 下正向 material 变化必须同时通过 data/OOS/random/baseline/drawdown/statistical/sample 多目标门禁，才能进入 `READY_FOR_FORWARD_SHADOW`；显式 `flow_validation` 只用于流程接线验证，会保留严格 veto 并追加 `flow_validation_override`，允许 validation-only shadow，但不得作为 production 晋级证据；不批准参数上线、不生成 approved overlay|已实现|
|参数治理 manifest|`config/parameter_governance.yaml`|登记可调参数面、source level、owner/status/rationale、validation evidence、review/exit 条件、candidate category 映射、owner quantitative input 状态、允许动作和 `production_effect=none`；`validation_shadow` source level 只表示 validation-only shadow 参数面，不是 production prior；owner 暂缺量化输入时只允许保持当前、继续收集证据、准备 shadow、owner-required 或 blocked-by-data/policy，不允许系统代填生产参数|已实现基础版|
|参数治理报告|`aits feedback evaluate-parameter-governance` / `outputs/reports/parameter_governance_YYYY-MM-DD.md` / `.json`|读取参数治理 manifest 和 `data/processed/parameter_candidates.json`，按参数面汇总 candidate status、veto reasons、config checksum、owner 输入状态和建议动作；缺配置或治理字段 fail closed，缺 owner 量化输入输出 `PASS_WITH_LIMITATIONS`；当 candidate ledger 为 `flow_validation` 时只能输出 validation-only shadow 准备/观察动作并附带 no-production 约束，不得进入 owner approval 或 production；不修改 `scoring_rules`、`backtest_validation_policy`、`feedback_sample_policy`、weight profile、approved overlay 或 rule card；交易日 `daily-run` 在 market feedback/dashboard 前生成该报告|已实现基础版|
|参数 shadow runner|`aits feedback run-parameter-shadow` / `outputs/reports/parameter_shadow_predictions_YYYY-MM-DD.md`|读取 parameter candidate ledger 中 `READY_FOR_FORWARD_SHADOW` 且 `production_effect=none` 的候选，复用 production `decision_snapshot`、trace、feature snapshot 和 data quality 引用，默认追加 validation-only prediction 到 `data/processed/prediction_ledger_flow_validation.csv`；常用于 `flow_validation` 接线验证；不写正式日报动作、不改变 `scores_daily.csv`、position gate、belief_state、approved overlay、正式 prediction ledger 或 production 权重|已实现基础版|
|Gate/event 归因报告|`aits backtest-gate-attribution` / `outputs/backtests/gate_event_attribution_YYYY-MM-DD_YYYY-MM-DD.md`|读取已生成的 `backtest_daily_*.csv` 和 `backtest_input_coverage_*.csv`，按 gate 估算 trigger_count、average_position_reduction、avoided_drawdown、missed_upside、net_effect、false_alarm 和 late_trigger，并汇总风险事件 label readiness；结果是一阶历史解释，不是完整反事实回测，不得相加为生产收益结论|已实现基础版|
|Challenger shadow runner|`aits feedback run-shadow` / `outputs/reports/shadow_predictions_YYYY-MM-DD.md`|读取 `rule_experiments.json` 中 forward shadow 状态可运行的 candidate，复用 production `decision_snapshot`、trace、feature snapshot 和 data quality 引用，追加 challenger prediction 到 `prediction_ledger.csv`；不写正式日报动作、不改变 `scores_daily.csv`、position gate、belief_state 或 production rule|已实现基础版|
|Forward shadow 样本成熟度|`aits feedback shadow-maturity` / `outputs/reports/shadow_maturity_YYYY-MM-DD.md`|读取 `prediction_outcomes.csv`，按 candidate、horizon、market regime 和 `production_effect` 汇总 available/pending/missing、平均收益、胜率、最大回撤和 benchmark excess；默认 `promotion` mode 使用 feedback sample policy 的 promotion floor，只有达标才进入 `READY_FOR_GOV_REVIEW`；显式 `--review-mode validation` 使用 pilot floor 或显式 `--min-available-samples`，只允许进入 `READY_FOR_VALIDATION_REVIEW`，不能作为 production rule 晋级证据；样本不足时保持 `READY_FOR_SHADOW` 或 `MISSING`|已实现基础版|
|决策结果缓存|`data/processed/decision_outcomes.csv`|保存每个 `snapshot_id`、观察窗口、AI proxy return、最大回撤、实现波动、SPY/QQQ/SMH/SOXX return 与超额收益、hit/miss、分桶字段、gate/thesis/risk/valuation 状态和 `belief_state` 路径|已实现基础版|
|决策校准报告|`outputs/reports/decision_calibration_YYYY-MM-DD.md`|输出市场阶段、样本数量、观察窗口、数据质量状态、benchmark policy 状态、基准解释边界、样本不足限制、重叠窗口限制、全局摘要和各分桶平均收益/回撤/波动/胜率/超额收益|已实现基础版|
|决策因果链构建|`aits feedback build-causal-chain`|读取历史 `decision_snapshot`、`decision_outcomes.csv` 和 trace bundle 引用，生成 `decision_causal_chain`；`signal_time_context` 只记录 signal_date 当时可见的 evidence、模块分变化、置信度变化、gate 和仓位变化，后验 outcome 只能进入 `post_signal_observations`|已实现基础版|
|决策因果链缓存|`data/processed/decision_causal_chains.json`|保存 `chain_id`、market regime、linked evidence、linked decision snapshot、quality、affected modules、score/confidence/position delta、triggered gates、append-only outcome windows、review status 和预留 `linked_rule_candidate`|已实现基础版|
|决策因果链报告|`outputs/reports/decision_causal_chains_YYYY-MM-DD.md`|输出因果链摘要、数据质量状态、触发 gate、outcome 窗口数量和未来 outcome 不得改写 signal-time 因果解释的治理边界|已实现基础版|
|决策因果链查询|`aits feedback lookup-chain`|按 `chain_id` 从 `decision_causal_chains.json` 反查单条链路，显示市场阶段、质量状态、decision snapshot、evidence、受影响模块、触发 gate 和 outcome 窗口|已实现基础版|
|决策学习队列构建|`aits feedback build-learning-queue`|从 `decision_causal_chains.json` 生成学习复核队列，记录成功/失败方向、`data_issue`、`rule_issue`、`sample_limited` 等归因分类、evidence、owner、next step 和是否需要候选规则；样本不足不得生成规则候选|已实现基础版|
|决策学习队列缓存|`data/processed/decision_learning_queue.json`|保存 `review_id`、关联 `chain_id`、market regime、decision snapshot、evidence、触发 gate、受影响模块、outcome summary、归因分类、复核状态、owner、next step、规则候选需求和治理边界|已实现基础版|
|决策学习队列报告|`outputs/reports/decision_learning_queue_YYYY-MM-DD.md`|中文输出分类摘要、复核队列、样本限制和“不得自动修改 production scoring / position_gate / thesis / 日报结论”的治理边界|已实现基础版|
|决策学习队列查询|`aits feedback lookup-learning`|按 `review_id` 反查学习复核项，显示关联因果链、方向、归因分类、规则候选标记、owner、next step 和原因|已实现基础版|
|候选规则实验台账构建|`aits feedback build-rule-experiments`|从 `decision_learning_queue.json` 中 `rule_candidate_required=true` 且非 `sample_limited` 的复核项生成候选规则实验台账；记录触发原因、关联 causal chain、候选假设、历史 replay 计划、前向 shadow 计划、样本限制、风险、回滚条件和 `production_effect=none`|已实现基础版|
|候选规则实验缓存|`data/processed/rule_experiments.json`|保存 candidate-only 规则实验记录；历史 replay 尚未运行时标记 `NOT_RUN`，前向 shadow 标记 `PENDING`；未完成 replay/shadow 和 `GOV-001` 批准前不得影响 production scoring、position gate、thesis、日报或回测|已实现基础版|
|候选规则实验报告|`outputs/reports/rule_experiments_YYYY-MM-DD.md`|中文报告输出候选规则数量、未运行 replay、待前向 shadow、验证计划和治理边界；不声明候选规则已验证或已批准|已实现基础版|
|候选规则实验查询|`aits feedback lookup-rule-experiment`|按 `candidate_id` 反查候选规则实验，显示关联 learning review、causal chain、触发原因、候选假设、replay/shadow 计划、production effect 和治理状态|已实现基础版|
|规则治理配置|`config/rule_cards.yaml`|登记 production、candidate、retired rule card；每张卡记录 rule id、类型、版本、owner、适用范围、来源配置、上线原因、验证引用、样本限制、已知限制、回滚条件、最后复核和下次复核日期；`score-daily` 和 `backtest` 会校验该 registry，并把适用的 production rule versions 写入 run manifest|已实现基础版|
|规则治理校验|`aits feedback validate-rule-cards`|校验 rule card schema、重复 id、production 审批/基线登记、验证引用、candidate 是否链接 rule experiment、来源配置路径和复核到期状态；不批准规则上线，只做治理台账校验|已实现基础版|
|Rule card promotion / retirement|`aits feedback promote-rule-card` / `aits feedback retire-rule-card` / `outputs/reports/rule_lifecycle_*_YYYY-MM-DD.md`|promotion 只允许 candidate rule card，必须提供 owner、批准理由、model promotion report 引用和 prediction/shadow outcome 引用，写入 `approval=approved`、`validation=shadow_passed` 和 production 生效日；retirement 只允许 production rule card，必须写明退役原因和 `retired_at`；输出后立即复用 rule card validator 校验|已实现基础版|
|规则治理报告|`outputs/reports/rule_governance_YYYY-MM-DD.md`|中文报告输出 rule card 数量、production/candidate 数量、类型分布、审批状态、验证状态和问题清单；`baseline_recorded` 只表示已有 production 行为已纳入审计台账|已实现基础版|
|规则治理查询|`aits feedback lookup-rule-card`|按 `rule_id` 反查 rule card，显示版本、生命周期状态、适用范围、来源配置、审批、验证、复核时间和回滚方式|已实现基础版|
|基准政策配置|`config/benchmark_policy.yaml`|登记默认 AI proxy、默认 benchmark、最低建议角色、SPY/QQQ/SMH/SOXX 的解释角色、适用场景、限制和未来 custom AI basket 治理要求|已实现基础版|
|基准政策校验|`aits feedback validate-benchmark-policy`|校验 benchmark policy schema、重复 ticker/id、默认 AI proxy、默认 benchmark、source config 路径、复核到期、自定义 AI basket 的 point-in-time lifecycle 要求，以及本次 strategy_ticker/benchmarks 是否登记|已实现基础版|
|基准政策报告|`outputs/reports/benchmark_policy_YYYY-MM-DD.md`|中文报告输出 benchmark 数量、custom basket 数量、角色覆盖、默认选择、选中口径摘要和问题清单；planned custom AI basket 不生成正式 basket return|已实现基础版|
|基准政策查询|`aits feedback lookup-benchmark-policy`|按 benchmark id、ticker 或 custom basket id 反查解释角色、适用场景、限制、是否默认基准和是否可作为 AI proxy 候选|已实现基础版|
|情景压力测试配置|`config/scenario_library.yaml`|登记 AI 产业链压力场景、类型、方向、严重度、影响节点、ticker、关联 risk event、position gate 影响、观察条件、证据要求、人工复核要求和解释边界|已实现基础版|
|情景压力测试校验|`aits scenarios validate`|校验 scenario library schema、重复 id、产业链节点、ticker、risk event、position gate、复核到期和 `not_probability_forecast=true`；情景不得伪装为概率预测或直接改 production 规则|已实现基础版|
|情景压力测试报告|`outputs/reports/scenario_library_YYYY-MM-DD.md`|中文报告输出情景数量、类型/严重度摘要、节点/ticker/risk event/gate 映射、观察条件、人工复核要求和治理边界|已实现基础版|
|情景压力测试查询|`aits scenarios lookup`|按 `scenario_id` 反查单个情景，显示类型、方向、严重度、影响节点、ticker、风险事件、gate impact、观察条件和人工复核要求|已实现基础版|
|未来催化剂日历配置|`config/catalyst_calendar.yaml`|登记 catalyst calendar schema、来源策略、复核周期和手工/审计事件；每个事件记录日期、类型、重要性、ticker/节点/risk event 映射、事件前动作、事件后复核目标、来源、采集时间、复核人和置信度|已实现基础版|
|未来催化剂日历校验|`aits catalysts validate`|校验日历 schema、重复 id、review due、未来采集/复核时间、已过期 scheduled 事件、ticker/节点/risk event 引用、高重要性事件前后复核要求和高重要性 public convenience 来源|已实现基础版|
|未来催化剂日历报告|`outputs/reports/catalyst_calendar_YYYY-MM-DD.md`|中文报告输出日历状态、事件数量、未来 5/20/60 天 upcoming catalyst、事件前动作、事件后复核目标、来源和治理边界|已实现基础版|
|未来催化剂查询|`aits catalysts lookup`|按 `catalyst_id` 反查事件日期、类型、重要性、相关 ticker/节点、风险事件、事件前动作、事件后复核目标、来源和复核元数据|已实现基础版|
|执行纪律配置|`config/execution_policy.yaml`|登记 advisory execution policy、再平衡阈值、加仓/减仓阈值、低置信度人工复核、禁止主动加仓 gate、冷却期和固定 action taxonomy|已实现基础版|
|执行纪律校验|`aits execution validate`|校验 execution policy schema、必需 action id、重复 action、报告可用性和复核到期状态；该政策只影响报告动作语言，不改变 production scoring、`position_gate` 或回测仓位|已实现基础版|
|执行纪律报告|`outputs/reports/execution_policy_YYYY-MM-DD.md`|中文报告输出政策版本、阈值、冷却期、advisory action taxonomy 和问题清单；`score-daily` 会写入该报告并在日报执行建议章节引用校验状态|已实现基础版|
|执行动作查询|`aits execution lookup`|按 `action_id` 反查固定动作定义，例如 `maintain`、`small_increase`、`no_new_position`、`reduce_to_target_range`、`wait_manual_review`、`observe_only`|已实现基础版|
|反馈闭环复核|`aits feedback loop-review`|按复核窗口汇总 market evidence、decision snapshots、decision_outcomes、prediction_outcomes、decision_causal_chains、decision_learning_queue、rule_experiments 和 task register 状态；声明 `ai_after_chatgpt` 市场阶段和可执行/需复核/研究用途边界|已实现基础版|
|反馈闭环复核报告|`outputs/reports/feedback_loop_review_YYYY-MM-DD.md`|中文周期报告输出新证据、快照、decision/prediction outcome、因果链、学习队列、规则候选、blocked task 和状态统计；prediction/shadow 样本不足时只标记研究用途，不直接生成调仓建议，也不自动修改生产规则|已实现基础版|
|市场反馈优化编排|`aits feedback optimize-market-feedback`|只读汇总 data quality、decision/prediction outcomes、decision causal chains、learning queue、rule experiments、parameter replay、parameter candidates、approved calibration overlay 和 current effective weights；输出 readiness、样本门槛、as-if 回放窗口、错误复盘、候选规则、参数复测收益变化、参数候选状态、overlay 状态和周/月执行频次；`production_effect=none`，不改变 `score-daily`、`position_gate`、thesis、日报结论或回测仓位|已实现基础版|
|市场反馈样本政策|`config/feedback_sample_policy.yaml`|配置 feedback / shadow / promotion 路径的 reporting、pilot、diagnostic 和 promotion 样本 floor；`feedback calibrate`、`feedback calibrate-predictions`、`feedback shadow-maturity`、`feedback loop-review`、`investment-review`、model promotion 和 `optimize-market-feedback` 读取该政策；当前 `feedback_sample_policy_v2` 将 diagnostic floor 下调到当前已积累 outcome 样本刚好可启动后续验证，promotion floor 不下调；pilot/validation 阶段允许启动因果链、学习队列、候选规则整理、shadow 复核和诊断复盘，但不得输出正式调权结论或晋级 production|已实现基础版|
|市场反馈优化报告|`outputs/reports/market_feedback_optimization_YYYY-MM-DD.md`|中文输出市场阶段、复核窗口、默认 `ai_after_chatgpt` as-if 窗口、产物状态、样本不足限制、learning queue 分类、rule experiment replay/shadow 状态、parameter replay 场景数与 material delta、parameter candidate/trial/owner review/risk review 数量、parameter governance action 分布、approved overlay 命中状态、与 daily-run/loop-review/investment-review 的兼容边界和下一步|已实现基础版|
|投资周报/月报复盘|`aits reports investment-review` / `outputs/reports/investment_weekly_review_YYYY-MM-DD.md` / `investment_monthly_review_YYYY-MM-DD.md`|读取 `scores_daily.csv`、decision snapshots、belief_state、decision outcomes、prediction outcomes、learning queue 和 rule experiments，面向投资复核者回答本期结论/仓位是否变化、前三个证据、产业链节点状态、thesis/risk/valuation 状态、production vs challenger shadow 表现、市场验证和规则学习；`production_effect=none`，不改变评分、仓位、回测或执行建议|已实现基础版|
|投资与数据告警|`outputs/reports/alerts_YYYY-MM-DD.md` / `outputs/reports/daily_score_YYYY-MM-DD.md#告警摘要`|`score-daily` 基于数据质量、特征警告、低可信模块、估值健康、risk event gate、thesis 复核、仓位上限变化和未来 5 天 high/critical catalyst 生成只读 data/system 与 investment/risk 告警；每条告警记录等级、触发/解除条件、claim/evidence 引用和去重键；`production_effect=none`，不改变评分、仓位、回测或执行建议|已实现基础版|
|认知模型需求|`docs/requirements/cognitive_model_2026-05-04.md`|定义 AI 产业链可审计认知模型边界、`belief_state` 第一阶段、阶段路线、禁止自动改生产规则的治理边界和关联任务|已登记|
|认知状态缓存|`data/processed/belief_state/belief_state_YYYY-MM-DD.json`|只读认知状态快照，结构化记录市场状态、产业链节点状态、估值、风险、thesis、仓位边界、宏观总风险资产预算、限制因素、多维置信度、trace 引用和 `decision_snapshot` 引用；明确不直接改变评分、闸门、回测仓位或交易建议|已实现基础版|
|认知状态历史|`data/processed/belief_state_history.csv`|认知状态历史索引，按 `signal_date` upsert，记录 `belief_state_id`、路径、生成时间、production_effect、置信度、数据质量、最终仓位边界、限制数量、trace 路径和 decision snapshot 路径|已实现基础版|
|认知状态报告|`outputs/reports/daily_score_YYYY-MM-DD.md#认知状态`|日报中的中文认知状态摘要，明确 `belief_state` 是只读解释层，而不是已批准进入 production 规则的输入|已实现基础版|
|回测验证政策|`config/backtest_validation_policy.yaml`|定义 backtest data credibility 的模块覆盖率阈值、robustness 默认起点后移、成本压力、权重扰动、随机种子、样本外切分、固定 exposure、vol-targeted exposure、再平衡间隔、coverage/source veto、bootstrap CI、有效独立样本/purging 口径、解释阈值，以及 promotion gate 阻断数据可信度等级、必需 robustness category、最小 lag sensitivity 天数和 rule governance 状态；带 `policy_metadata`，回测稳健性和模型晋级报告会输出 policy version|已实现基础版|
|回测|`aits backtest`|先校验 `benchmark_policy`、数据质量门禁和 PIT feature availability，再基于每日评分、`macro_risk_asset_budget` 和同一套 `position_gate` 动态回测；策略实际敞口使用总资产内 AI exposure，风险资产内 AI 相对权重和总风险资产预算在每日明细中分别保留；按显式成本假设扣除 commission、bid-ask spread、linear slippage、market impact、tax、FX、annual financing carry 和 ETF delay，并按 signal_date 构建 point-in-time watchlist lifecycle、SEC 基本面特征、TSM IR 季度补充、估值快照切片、风险事件发生记录、复核声明切片和只读产业链节点热度/健康度历史状态；feature/universe lag 场景会改用更早的 `feature_as_of` 和 `universe_as_of`；可通过 `--robustness-report`、`--lag-sensitivity-report` 或 `--promotion-report` 生成可选稳健性、滞后敏感性和模型晋级门槛报告，robustness/promotion 默认参数读取 `config/backtest_validation_policy.yaml`|已实现|
|回测历史输入缺口诊断|`aits backtest-input-gaps`|先执行数据质量门禁，再按回测 signal_date 诊断历史估值快照、严格 PIT 估值、风险事件 occurrence 和人工复核声明覆盖；报告只列缺口和补数入口，不生成或伪造历史估值/风险事件/无风险声明|已实现基础版|
|回测历史输入缺口报告|`outputs/backtests/backtest_input_gaps_YYYY-MM-DD_YYYY-MM-DD.md`|中文报告列出每个 signal_date 的估值状态、估值快照数量、严格 PIT 估值数量、风险事件 occurrence 数量、当前有效人工复核声明数量和风险覆盖状态；明确 occurrence 为 0 不能自动代表历史无事件|已实现基础版|
|Forward-only PIT 覆盖验证|`aits backtest-pit-coverage`|读取并校验 PIT raw snapshot manifest 后，按输入族、source、ticker 和 `available_time` 汇总自建 forward-only 快照覆盖，输出历史 C 级原因、B/A readiness、first eligible date 和解除条件；manifest 校验失败时停止，不把未通过校验的快照计入回测可信度|已实现基础版|
|Forward-only PIT 覆盖报告|`outputs/backtests/backtest_pit_coverage_YYYY-MM-DD.md`|中文报告输出 manifest 状态、快照数、row count、B 级最小覆盖日期数、最新快照日龄、source 覆盖摘要、升级日期判断和历史 C 级原因；A 级仍要求 strict PIT vendor archive 或等价一手可见时间证明|已实现基础版|
|回测输入覆盖诊断|`outputs/backtests/backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv`|机器可读输出评分模块覆盖、来源类型、输入问题、证据 URL、ticker 输入、SEC 特征、风险事件证据、来源类型、估值 `point_in_time_class`、`history_source_class`、`backtest_use` 和 `confidence_level` 聚合，便于跨月审计和回归分析|已实现|
|回测报告|`outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md`|输出市场阶段、结论使用等级、Backtest Data Quality A/B/C、vendor historical estimates 使用声明、自建快照使用声明、Minimum Feature Lag、Universe PIT、Corporate Actions Handling、核心输入 PIT 覆盖、绩效指标、benchmark policy 状态、基准解释边界、执行成本摘要、宏观风险资产预算摘要、仓位闸门摘要、判断置信度分桶、产业链节点历史状态摘要、数据质量门禁摘要、SEC 基本面、估值快照、风险事件质量摘要、模块覆盖率摘要、月度覆盖率趋势、月度来源类型趋势、月度输入问题下钻、月度输入证据 URL 摘要、月度风险事件证据 URL 明细、月度 ticker 输入摘要、月度 ticker SEC 特征明细、月度估值快照来源和月度风险事件证据来源分布；C 级输入下 Sharpe/CAGR/收益表只作探索性诊断|已实现|
|回测成本假设|`aits backtest --cost-bps --spread-bps --slippage-bps --market-impact-bps --tax-bps --fx-bps --financing-annual-bps --etf-delay-bps`|成本模型第一阶段是显式假设拆分，不等同于真实券商成交回报；每日明细保存每类成本扣减，回测报告显示成本摘要，trace run manifest 记录 `cost_assumptions` 便于复现|已实现基础版|
|回测稳健性报告|`outputs/backtests/backtest_robustness_YYYY-MM-DD_YYYY-MM-DD.md` / `.json`|可选输出，复用同一 point-in-time 输入，按 `config/backtest_validation_policy.yaml` 运行基础动态策略、成本压力、起点后移、固定总资产 AI exposure、vol-targeted exposure、no-gate model target、配置化再平衡频率、趋势-only / 趋势+风险情绪信号族基线、alpha-only / risk-state-only / gate-modules score 架构基线、单模块权重上下扰动、same-turnover random、same-exposure random，以及时间顺序 in-sample / out-of-sample holdout，并把结果与买入持有 SPY/QQQ/SMH/SOXX 或用户配置基准对比；成本、窗口切片、样本内/样本外和权重扰动场景缓存昂贵的 PIT feature/report 上下文，但重新调用同一评分与回测执行路径；权重扰动通过 effective weight resolver 的 per-scenario multiplier 进入同一评分链路，不复制评分逻辑；benchmark、架构基线和随机策略作为执行/信号/分层诊断，不作为生产评分替代逻辑；Markdown 报告和机器可读 JSON 摘要均声明 `production_effect=none`、输出 policy metadata、data credibility grade、coverage/source veto、bootstrap CI、weight profile version、matched overlay ids 和 effective weights，`remaining_gaps` 为空时仍保留数据可信度、样本长度和 owner 审批限制|已实现|
|回测滞后敏感性报告|`outputs/backtests/backtest_lag_sensitivity_YYYY-MM-DD_YYYY-MM-DD.md` / `.json`|可选输出，默认测试 feature/universe lag `0,1,3,5,10,20` 个交易日并保留 1 个交易日 rebalance delay；复用同一数据质量门禁、同一成本假设和同一 universe PIT 规则；C 级输入下仅用于识别未来函数风险，不解除输入可信度降级|已实现基础版|
|模型晋级门槛报告|`outputs/backtests/model_promotion_YYYY-MM-DD_YYYY-MM-DD.md` / `.json`|可选输出，按 `config/backtest_validation_policy.yaml` 和 `config/feedback_sample_policy.yaml` 汇总 Backtest Data Quality、robustness、lag sensitivity、prediction/shadow outcome 和 rule card registry 状态；阻断数据可信度等级、必需 robustness category、最小 lag sensitivity 天数和 shadow outcome floor 均来自 policy；缺少 shadow outcome 或关键证据时只允许进入 `READY_FOR_SHADOW`，`READY_FOR_GOV_REVIEW` 仍需 owner approval 和 rule governance|已实现基础版|
|回测输入审计报告|`outputs/backtests/backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md`|输出 PASS/PASS_WITH_WARNINGS/FAIL、数据质量、Backtest Data Quality、point-in-time 输入、模块覆盖率、来源类型、执行假设、审计发现和修复建议，判断本次回测是否可解释；`--fail-on-audit-warning` 可把非 PASS 审计状态转为命令失败|已实现|
|回测 Evidence Bundle|`outputs/backtests/evidence/backtest_YYYY-MM-DD_YYYY-MM-DD_trace.json`|记录回测 `claim`、`evidence`、`dataset`、`quality`、`run_manifest`、`benchmark_policy`、feature availability 配置引用和本次运行适用的 production rule version manifest，用于从绩效、数据质量、输入覆盖、PIT 可见时间和规则版本结论反查上下文|已实现|
|报告反查|`aits trace lookup`|按 claim/evidence/dataset/quality/run id 读取 evidence bundle 并输出中文摘要和原始 JSON 上下文|已实现|
|数据源健康|`aits data-sources health`|读取 `config/data_sources.yaml` 和 `data/raw/download_manifest.csv`，输出 provider health score、cache path 存在性、latest manifest downloaded_at/row_count/checksum、checksum drift、manifest/cache 新鲜度和 source reconciliation 覆盖状态；`market_prices` 在 FMP + Marketstack 低成本组合下评估覆盖，跨供应商不足不自动平滑数据；inactive/diagnostic-only 来源的历史 manifest checksum 漂移只记为调查警告，active 来源 checksum mismatch 仍 fail closed|已实现基础版|
|数据源健康报告|`outputs/reports/data_sources_health_YYYY-MM-DD.md`|中文报告展示方法边界、领域级 reconciliation 覆盖、provider health、latest manifest 明细、缓存问题和调查项；当前低成本版达到 `BASELINE_DONE`，生产级跨源校验仍依赖 owner 提供长期可用第二来源和授权策略|已实现基础版|
|外部供应商请求级缓存|`data/raw/external_request_cache/<provider>/<api_family>/<cache_key>/metadata.json` + `response.body`|所有接入缓存 wrapper 的外部供应商请求先按 schema version、provider、api family、HTTP method、endpoint、脱敏 query/body/header identity 生成 cache key；FMP、Marketstack、Cboe VIX、FRED、SEC、TSMC IR、official policy、EODHD 和 yfinance 路径命中 cache 时不再请求供应商，MISS 才发送请求并写入响应体、status code、headers、body checksum 和脱敏请求身份；API key、token、Authorization、Cookie、User-Agent 不写入原文；该底层请求 cache 不替代业务 raw cache、download manifest、PIT manifest 或日报质量门禁|已实现基础版|
|PIT raw snapshot manifest|`data/raw/pit_snapshots/manifest.csv`|forward-only 自建 PIT 快照索引，记录 source、endpoint、request params、canonical/provider symbol、raw payload path、sha256、bytes、row count、`ingested_at`、`available_time`、PIT 可信度、回测用途和 provider 授权字段；缺跑日期不能事后补写成 strict PIT|已实现基础版|
|FMP forward-only PIT 抓取|`aits pit-snapshots fetch-fmp-forward`|抓取 FMP analyst estimates、price target、ratings 和 earnings calendar，写入 `data/raw/fmp_forward_pit/` 与 `data/processed/pit_snapshots/fmp_forward_pit_YYYY-MM-DD.csv`，`available_time` 固定为本系统下载写入时间；标准化 `normalized_id` 使用有界 ASCII slug + SHA256 checksum 短摘要，避免供应商非 ASCII/超长字段影响 PIT 标识稳定性；供应商、权限、写入或校验失败时输出脱敏中文失败报告；显式 `--continue-on-failure` 仅用于日常调度继续后续自带质量门禁的步骤，不把失败快照作为可用 PIT 输入；不改变当前评分语义|已实现基础版|
|FMP PIT as-of 修正查询|`aits valuation fetch-fmp --pit-normalized-path`|`eps_revision_90d_pct` 默认从 FMP PIT normalized 索引读取 analyst-estimates 历史，只使用 `available_time <= decision_time` 的同一 fiscal estimate date；自建历史不足 90 天时明确降级，不用未来快照或供应商当前历史视图补洞|已实现基础版|
|PIT manifest 生成|`aits pit-snapshots build-manifest`|从现有 FMP analyst estimates、FMP historical valuation、FMP forward PIT 和 EODHD Earnings Trends raw cache 生成通用 PIT manifest，并立即复用校验报告；不改变当前评分或估值复核语义|已实现基础版|
|PIT manifest 校验|`aits pit-snapshots validate`|校验 manifest schema、必填字段、raw payload 存在性、sha256、bytes、row count、重复 snapshot id、`available_time <= ingested_at`、strict PIT 误标、低可信 strict 声明和 provider LLM/再分发权限；失败时后续不得使用这些快照|已实现基础版|
|PIT 快照质量报告|`outputs/reports/pit_snapshots_validation_YYYY-MM-DD.md`|中文输出状态、manifest 路径、快照数量、来源摘要、样例、错误/警告和方法边界；通过不代表已进入评分，下游仍必须通过 `available_time <= decision_time` 查询|已实现基础版|
|PIT 日常健康检查/告警|`aits ops health` / `outputs/reports/pipeline_health_alerts_YYYY-MM-DD.md`|每日 health 检查要求 FMP PIT 抓取报告、PIT manifest、当日 FMP PIT normalized CSV 和 PIT 质量报告可见，并检查抓取报告状态、manifest/normalized row count、latest `available_time` 和 raw payload sha256；最新生产交易日允许 `available_time <= visibility_cutoff` 的 forward-only PIT，历史回放/旧 as-of 仍要求不读未来可见输入；抓取 FAIL/PASS_WITH_WARNINGS、缺跑、断更、row count 异常或 checksum 异常进入 data/system alert|已实现基础版|
|能力圈|`config/watchlist.yaml`|记录核心标的、能力圈、`decision_stage` 和 thesis 要求；当前核心观察池已扩展到云 CapEx、GPU/ASIC、HBM/存储、先进制程/设备、应用商业化等代表性 ticker；`watch_only` 用于观察分析，`active_trade` 才要求主动交易 thesis 纪律|已实现基础版|
|观察池生命周期|`config/watchlist_lifecycle.yaml`|记录 ticker 的 `added_at`、`removed_at`、`active_from`、`active_until`、能力圈状态、节点映射可见日期、thesis 要求可见日期、来源和复核人，用于回测防幸存者偏差|已实现基础版|
|观察池生命周期校验|`aits watchlist validate-lifecycle`|校验当前核心/活跃观察池是否都有 point-in-time lifecycle 记录、是否存在重复记录，以及当前活跃 ticker 在评估日是否可用于评分/回测|已实现基础版|
|观察池生命周期报告|`outputs/reports/watchlist_lifecycle_YYYY-MM-DD.md`|输出生命周期记录、当前活跃记录数、错误和警告；回测先校验该报告，失败则停止|已实现基础版|
|产业链|`config/industry_chain.yaml`|记录产业链节点和因果关系|已实现基础版|
|市场阶段|`config/market_regimes.yaml`|记录默认 AI regime 和压力测试区间|已实现|
|风险事件|`config/risk_events.yaml`|记录 L1/L2/L3 风险和动作规则|已实现基础版|
|风险事件校验|`aits risk-events validate`|校验风险等级、产业链引用、相关标的和动作规则|已实现基础版|
|风险事件发生记录|`data/external/risk_event_occurrences/`|记录真实触发或观察中的政策/地缘事件、状态、证据来源、S/A/B/C/D/X 证据等级、严重性、概率、影响范围、时效性、可逆性、动作等级、人工复核人、复核日期、复核决策、理由、下次复核日期和时间线；也可记录人工复核声明，说明指定窗口内已检查来源范围且未发现未记录重大风险事件；保守 source policy 下 `S/A` 可支持评分和仓位闸门，`B` 只支持普通评分，`C/D/X` 只复核|已实现基础版|
|风险事件每日复核声明|`aits risk-events record-review-attestation`|在用户显式提供复核人、来源范围和理由后写入 `review_attestation` YAML；声明只表示人工复核覆盖窗口和列出的来源范围，不会自动触发仓位闸门，也不会覆盖已记录 active/watch 发生记录|已实现基础版|
|风险事件官方来源抓取|`aits risk-events fetch-official-sources`|按 owner 确认的低成本官方来源组合抓取 Federal Register/BIS/OFAC/USTR/Congress.gov/GovInfo/Trade.gov CSL；写入 raw payload、download manifest、待复核候选 CSV 和中文报告；Congress.gov/GovInfo 缺 API key 时显式跳过并报告；候选强制 `pending_review` 和 `production_effect=none`，不得直接评分、触发仓位闸门或写入正式 occurrence|已实现基础版|
|官方政策来源原始缓存|`data/raw/official_policy_sources/YYYY-MM-DD/*`|保存官方来源原始 JSON/XML/HTML payload，文件名包含 source_id 和 checksum 前缀；manifest 记录 provider、endpoint、请求参数、下载时间、row count、输出路径和 checksum；API key 不写入报告或 manifest|已实现基础版|
|官方政策来源待复核候选|`data/processed/official_policy_source_candidates_YYYY-MM-DD.csv`|从官方来源 metadata/title/summary 中抽取政策/地缘候选，记录 source_id、provider、source URL、published_at、matched risk_id/ticker/node、raw payload checksum 和人工复核问题；只能作为 owner 每日复核、AI 模块相关性 triage 和 reviewed occurrence CSV 的输入|已实现基础版|
|官方政策来源抓取报告|`outputs/reports/official_policy_sources_YYYY-MM-DD.md`|中文输出抓取状态、来源数、payload 数、候选数、错误/警告、跳过来源、每个 payload 的 row count/checksum 和候选摘要；报告声明不代表已确认事件或无事件结论|已实现基础版|
|官方政策来源 AI 模块 triage|`aits risk-events triage-official-candidates`|读取官方来源待复核候选 CSV，按 AI 模块直接相关性输出 `must_review`、`review_next`、`sample_review`、`auto_low_relevance`、`duplicate_or_noise`；分类优先看标题、URL、来源名称和明确 metadata，不盲目继承宽泛 sanctions/geopolitics 自动映射出的 ticker 或 risk_id；输出强制 `production_effect=none`，不得写入正式 occurrence、评分或仓位闸门|已实现基础版|
|官方政策来源 AI 模块 triage CSV|`data/processed/official_policy_candidate_triage_YYYY-MM-DD.csv`|保存原始候选引用、matched topic/risk/ticker/node、triage bucket、AI relevance score、命中信号、分类理由和复核策略；`auto_low_relevance` 只代表低优先级，不代表已确认无风险|已实现基础版|
|官方政策来源 AI 模块 triage 报告|`outputs/reports/risk_event_candidate_triage_YYYY-MM-DD.md`|中文输出 bucket 统计、高优先级候选、降低优先级候选摘要、方法边界和问题清单；用于减少人工复核面，不替代复核声明|已实现基础版|
|高优先级官方候选 OpenAI 风险等级预审|`aits risk-events precheck-triaged-official-candidates`|读取官方候选 CSV 和 triage CSV，默认按 `risk_event_triaged_official_candidates` profile 只把 `must_review/review_next` 送入 OpenAI metadata-only 预审，输出 `status_suggestion`、`level_suggestion`、matched risk/ticker/node 和人工复核问题；provider LLM 授权未知或 `cache_allowed=false` 时 fail closed；完全相同 request payload 在 TTL 内复用本地缓存；输出强制 `llm_extracted / pending_review`，不得写入正式 occurrence、评分或仓位闸门|已实现|
|高优先级官方候选 OpenAI 预审报告|`outputs/reports/risk_event_prereview_triaged_openai_YYYY-MM-DD.md`|中文输出模型、reasoning effort、输入 checksum、待复核数量、L2/L3 候选、active 候选、cache HIT/MISS、错误/警告和请求诊断；风险等级只作为人工复核建议|已实现|
|LLM 正式风险评估导入|`aits risk-events apply-llm-formal-assessment` / 默认 `aits score-daily`|读取 `risk_event_prereview_queue.json`，按 owner 决策把 LLM 预审结果写入正式 risk occurrence YAML 和 LLM formal attestation；`reviewer` 必须为 `llm_formal_assessment:<model>`，不得伪装成人工复核；LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 position gate；2026-05-12 起 `score-daily` 在 OpenAI 预审成功后默认自动执行该写入，以缓解人工复核不可用导致的 `policy_geopolitics` 低置信|已实现基础版|
|LLM 正式风险评估报告|`outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md`|中文输出输入队列 checksum、写入 occurrence 数、active/watch 数、attestation 状态、model/request/checksum 追踪和“未人工复核”边界；日报政策/地缘来源类型显示为 `llm_formal_assessment`，full coverage 置信度为 65%，低于人工复核但不再触发低置信模块|已实现基础版|
|风险事件发生记录 CSV 导入|`aits risk-events import-occurrences-csv`|导入人工复核后的事件发生记录 CSV，多证据行按 `occurrence_id` 合并并写入 YAML；关键字段、证据等级、动作等级和人工复核元数据冲突时停止；缺失 `action_class` 默认 `manual_review`|已实现基础版|
|风险事件发生记录导入报告|`outputs/reports/risk_event_occurrence_import_YYYY-MM-DD.md`|记录 CSV 行数、checksum、导入记录数、错误和警告|已实现基础版|
|风险事件发生记录校验|`aits risk-events validate-occurrences`|校验实际发生记录 schema、event_id、日期、新鲜度、证据来源、证据等级和动作等级，并校验复核声明的覆盖窗口、复核人、结论、来源范围和过期状态；按评估日过滤可见 occurrence/evidence/attestation，晚于评估日的后续记录以 warning 记录并排除，不进入历史日评分、仓位闸门或当前有效声明；`watch` 默认只进入报告和人工复核，`B` 级 active 证据只能普通评分，`C/D/X` 或 public convenience 单源不得自动评分或触发仓位闸门；只有当前有效复核声明才能让空发生记录脱离 `insufficient_data`|已实现基础版|
|风险事件 OpenAI 预审导入|`aits risk-events import-prereview-csv`|导入固定结构化输出，保存 model、reasoning effort、prompt version、request id、request timestamp、source URL、输入/输出 checksum、候选 risk_id、ticker/产业链节点映射和人工复核问题；无生产 visibility cutoff 时，晚于 `as_of` 当日 UTC 末尾的 request timestamp 会 fail closed；输出强制为 `llm_extracted` / `pending_review`，不写入正式发生记录|已实现基础版|
|Agent 请求缓存与审计归档|`data/processed/agent_request_cache/*.json` / `archive/{provider}/{api_family}/YYYY-MM-DD/*.json`|所有启用缓存的 live agent 请求先按 cache schema version、provider、api family、endpoint、完整 request payload checksum 和 input checksum 生成 cache key；OpenAI Responses 是首个接入 adapter，prompt version、model、reasoning effort、结构化输出 schema、source permission 和 content sent level 都包含在 request payload/input checksum 中；默认 TTL 来自 `config/llm_request_profiles.yaml`，可用 `--openai-cache-ttl-hours` 覆盖本次 OpenAI adapter；TTL 内 HIT 复用成功响应，MISS/EXPIRED 才重新发送；每次实际发送均归档 sanitized request headers、request payload、response status/header/body、attempt diagnostics、client request id、provider request id、input/output checksum 和 cache key；Authorization/API key 不写入；`cache_allowed=false` 时 fail closed，不发起 live 请求|已实现|
|风险事件 OpenAI live 预审|`aits risk-events precheck-openai` / `aits score-daily --risk-event-openai-precheck`|独立命令读取 JSON/YAML source-permission 输入，默认使用 `risk_event_single_prereview` profile；日报前自动预审读取官方来源候选 CSV 并默认只发送 `metadata_only`，默认使用 `risk_event_daily_official_precheck` profile；两者都会按 provider `llm_permission` 和 `cache_allowed` fail closed 后调用或复用 OpenAI Responses API；profile 配置 model、reasoning effort、timeout、HTTP client、cache TTL、重试次数、候选上限和 LLM formal 写入参数，CLI 显式参数只覆盖本次运行；`score-daily` 仅在评估日等于最新已完成美股交易日时，把本轮运行时间作为 OpenAI 预审 visibility cutoff，允许收盘后 UTC/JST 次日生成前一美股交易日的 request timestamp，历史 as-of 或无 cutoff 路径仍按 as-of 当日 UTC 末尾 fail closed；单个请求最终失败则整批停止且不写部分队列；失败报告输出 sanitized transport diagnostics，包括 attempt、HTTP client、client request id、endpoint host、payload byte size、input checksum、HTTP status、OpenAI x-request-id 或异常类型，不输出 API key、Authorization header 或未授权全文；`score-daily` 预审成功后默认继续写入 LLM formal occurrence/attestation，作为政策/地缘正式评估输入；LLM formal 不伪装成人工复核、不单独触发 position gate；保存 request/response id、model、reasoning effort、prompt version、source permission、输入/输出 checksum、cache 状态、候选 risk_id、ticker/节点映射和人工复核问题|已实现|
|风险事件 OpenAI 预审队列|`data/processed/risk_event_prereview_queue.json`|保存 schema v2 的待人工复核预审记录、model、reasoning effort 和 cache 状态；L2/L3 或 active 候选只作为 review queue，不得直接进入评分、仓位闸门或回测；人工确认后必须通过 reviewed occurrence CSV 和 `validate-occurrences` 进入正式发生记录|已实现|
|风险事件 OpenAI 预审报告|`outputs/reports/risk_event_prereview_import_YYYY-MM-DD.md` / `outputs/reports/risk_event_prereview_openai_YYYY-MM-DD.md`|中文报告输出输入行数或 LLM claim 数、model、reasoning effort、checksum、cache HIT/MISS、待复核数量、L2/L3 候选、active 候选、错误和警告；live 报告显示 Responses API 调用边界，并在失败或重试成功时输出 sanitized attempt diagnostics 和 HTTP client；CSV 报告声明不发起 API 请求|已实现|
|风险事件 OpenAI 预审模板|`docs/examples/risk_event_prereview/openai_prereview_template.csv` / `docs/examples/risk_event_prereview/openai_live_precheck_template.yaml`|提供固定结构化 CSV 导入示例和 live API source-permission 输入示例；owner 2026-05-10 允许个人研究、非商用目的下的已授权 paid vendor 文本进入 OpenAI 预审，但付费供应商内容仍只有 `external_llm_permitted=true` 或 provider `llm_permission.external_llm_allowed=true` 且内容级别不超过授权范围时才允许发送|已实现基础版|
|LLM claim 预审|`aits llm precheck-claims`|从 JSON/YAML 输入读取 source_id、来源引用、采集时间和待发送内容，先按 `config/data_sources.yaml` 的 provider LLM 权限和 `cache_allowed` fail closed，再按 `llm_claim_prereview` profile 调用或复用 OpenAI Responses API 固定结构化输出；profile 配置 endpoint、model、reasoning effort、timeout、HTTP client、cache TTL 和重试次数，请求默认 `store=false`，可用 CLI 参数覆盖本次运行或用 `--llm-request-profile` 切换 profile，失败报告输出 sanitized transport diagnostics；报告和队列保存 request id、model、reasoning effort、prompt version、输入/输出 checksum、source permission、cache 状态和结构化 claim，不保存 API key、Authorization header 或未授权全文|已实现|
|LLM claim 预审队列|`data/processed/llm_claim_prereview_queue.json`|保存 schema v2 的 claim-centric `llm_extracted` / `pending_review` 记录、model、reasoning effort、risk_event_candidate 和 thesis_signal_match 候选；不得直接进入评分、thesis 状态迁移、仓位闸门或回测；人工确认后才可整理为 market_evidence 或 reviewed risk occurrence 导入|已实现基础版|
|LLM claim 预审报告|`outputs/reports/llm_claim_prereview_YYYY-MM-DD.md`|中文报告输出 provider、source、model、reasoning effort、request id、内容发送级别、claim 数量、错误/警告和“不得评分/不得触发仓位闸门”边界；不输出 API key 或未授权全文|已实现基础版|
|LLM claim 输入模板|`docs/examples/llm_claim_prereview/openai_claim_precheck_template.yaml`|提供 source-permission envelope/catalog 驱动输入示例；真实运行前必须确认 provider 的 `llm_permission.external_llm_allowed=true` 且内容级别不超过授权范围|已实现基础版|
|数据源目录|`config/data_sources.yaml`|记录 provider、endpoint、缓存路径、审计字段、校验项、来源限制和 provider 级 LLM 处理权限；paid vendor 文本进入 OpenAI 预审必须记录 personal-use approval、`external_llm_allowed`、`content_sent_level`、`cache_allowed`、`redistribution_allowed` 和 `approval_ref`；外部 LLM 授权未知时默认 fail closed|已实现基础版|
|数据源校验|`aits data-sources validate`|校验数据源目录是否可审计、活跃来源是否声明校验和限制，并校验外部 LLM 处理授权是否有 approval_ref、reviewed_at 和付费供应商 license_scope|已实现基础版|
|SEC 公司映射|`config/sec_companies.yaml`|记录核心标的 ticker、CIK、taxonomy 预期和统一指标周期覆盖范围；US 公司使用 SEC companyfacts，TSM 季度通过 TSM IR 合并补齐，ASML 等 ADR/foreign issuer 按 SEC companyfacts 实际 taxonomy 和可披露年度覆盖进入审计|已实现基础版|
|SEC 指标映射|`config/fundamental_metrics.yaml`|记录 SEC/TSMC IR taxonomy/concept/unit 到内部基本面指标的映射、年度/季度偏好、支撑指标和显式派生规则；ASML 等非美元披露公司保留 SEC companyfacts 原始 `EUR` 单位用于公司内比率，TSMC IR 保留 Management Report 的 `TWD_billions`/`USD_billions` 等披露尺度|已实现基础版|
|SEC 特征公式|`config/fundamental_features.yaml`|记录 SEC 基本面比率特征公式和周期偏好|已实现基础版|
|SEC 基本面下载|`aits fundamentals download-sec-companyfacts`|下载 SEC companyfacts 原始 JSON 并写入审计 manifest|已实现基础版|
|SEC 基本面校验|`aits fundamentals validate-sec-companyfacts`|校验 SEC companyfacts JSON、CIK、taxonomy 和 manifest checksum|已实现基础版|
|SEC 指标抽取|`aits fundamentals extract-sec-metrics`|先执行 SEC companyfacts 质量门禁，通过后抽取收入、毛利、营业利润、净利润、研发和 CapEx 等结构化摘要；只在显式配置且组件事实完全对齐时生成派生指标|已实现基础版|
|SEC 指标校验|`aits fundamentals validate-sec-metrics`|校验 SEC 基本面指标 CSV 的 schema、重复键、未来披露日期、数值合法性和按公司周期覆盖声明计算的配置覆盖率，并输出缺失 `ticker / metric_id / period_type` 观测清单|已实现基础版|
|SEC submissions 下载|`aits fundamentals download-sec-submissions`|下载 active CIK 的 submissions filing history JSON，写入 `data/raw/sec_submissions/` 和 manifest，记录 filing count、additional files、checksum 和请求参数|已实现基础版|
|SEC accession archive 下载|`aits fundamentals download-sec-filing-archive`|按 SEC 指标 CSV 当日实际使用的 accession 下载 accession directory `index.json`，写入 `data/raw/sec_filings/<ticker>/<accession>/index.json` 和 manifest；默认节流低于 SEC fair access 限制，不下载全量历史 filing|已实现基础版|
|SEC accession 覆盖报告|`aits fundamentals sec-accession-coverage` / `outputs/reports/sec_accession_coverage_YYYY-MM-DD.md`|检查 SEC 指标 CSV 已使用 accession 是否有 submissions metadata、accepted time 和 archive index checksum 覆盖；报告 `production_effect=none`，只提高审计追溯，不直接改变评分|已实现基础版|
|SEC 特征构建|`aits fundamentals build-sec-features` / `aits score-daily`|先复用 SEC 指标 CSV 校验门禁，通过后生成毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度；分子/分母周期、单位或披露来源不一致时记录覆盖警告并跳过该特征，分母非正数仍作为错误停止；日报会在本地 TSMC IR 缓存存在时先按 as-of 合并 TSM 季度指标，再运行同一条特征构建路径|已实现基础版|
|SEC 指标缓存|`data/processed/sec_fundamentals_YYYY-MM-DD.csv`|保存 SEC 基本面指标结构化抽取结果，是日报 SEC 基本面评分的输入|已实现基础版|
|SEC 特征缓存|`data/processed/sec_fundamental_features_YYYY-MM-DD.csv`|保存 SEC 基本面比率特征，是日报基本面硬数据评分的审计输出|已实现基础版|
|SEC 指标报告|`outputs/reports/sec_fundamentals_YYYY-MM-DD.md`|输出 SEC 缓存校验状态、抽取行数、缺失指标和方法限制|已实现基础版|
|SEC 指标校验报告|`outputs/reports/sec_fundamentals_validation_YYYY-MM-DD.md`|声明抽取后 CSV 是否可进入基本面特征构建和日报评分，并列出缺失观测清单供回测下钻使用|已实现基础版|
|SEC 特征报告|`outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`|输出 SEC 指标 CSV 校验状态、特征公式、特征行数和限制说明|已实现基础版|
|TSMC IR PDF 文本抽取|`aits fundamentals extract-tsm-ir-pdf-text`|从本地官方 TSMC IR PDF 的可抽取文本层生成 Management Report 文本，并记录官方 URL、输入/输出路径、页数、字符数和 checksum；无文本层时停止|已实现基础版|
|TSMC IR PDF 文本抽取报告|`outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md`|声明 PDF 文本抽取状态、官方来源、输入 PDF、输出文本、页数、字符数、checksum 和错误/警告|已实现基础版|
|TSMC IR 官方页面抓取|`aits fundamentals fetch-tsm-ir-quarterly`|从 TSMC Investor Relations 官方季度页面发现并下载 Management Report 文本，保存原始文本审计证据后生成季度指标；PDF/二进制资源会停止并要求使用 PDF 文本抽取命令|已实现基础版|
|TSMC IR 季度基本面导入|`aits fundamentals import-tsm-ir-quarterly`|解析 TSMC Investor Relations 官方 Management Report 已抽取文本，生成收入、毛利、营业利润、净利、研发、利润率和 CapEx 等季度指标；`--filed-date` 记录公开/披露日期，用于 point-in-time 回测可见性；金额保留来源披露尺度|已实现基础版|
|TSMC IR 批量季度导入|`aits fundamentals import-tsm-ir-quarterly-batch`|读取 manifest CSV 中的 `fiscal_year/fiscal_period/source_url/input_path/filed_date`，批量解析多个本地官方 Management Report 文本；重复季度、缺文件、非官方 URL 或任一季度解析错误时整批停止写入|已实现基础版|
|TSMC IR 批量 manifest 模板|`docs/examples/fundamentals/tsm_ir_quarterly_manifest_template.csv`|提供历史季度回填的 CSV 字段示例；相对 `input_path` 按 manifest 所在目录解析，`filed_date` 作为历史回测可见日期|已实现基础版|
|TSMC IR 季度指标缓存|`data/processed/tsm_ir_quarterly_metrics.csv`|保存 TSM 官方季度基本面指标、source URL、公开/披露日期、采集时间和 checksum；可通过显式合并命令、`score-daily` 自动合并或 `aits backtest` 按 signal_date 选择当时最新已披露季度进入 SEC-style 指标口径|已实现基础版|
|TSMC IR 季度报告|`outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md`|输出 TSMC IR 来源、指标行数、checksum、缺失指标和限制说明|已实现基础版|
|TSMC IR 批量季度报告|`outputs/reports/tsm_ir_quarterly_batch_YYYY-MM-DD.md`|输出批量 manifest、每个季度的状态、source URL、source path、checksum、行数和错误/警告；只有整批通过才写 CSV|已实现基础版|
|TSMC IR 指标合并|`aits fundamentals merge-tsm-ir-sec-metrics` / `aits score-daily`|按评估日期选择最新已披露 TSM IR 季度，把收入、毛利、营业利润、净利、研发和 CapEx 转为 SEC-style 指标行，只替换重复 TSM quarterly 键，并复用 SEC 指标 CSV 校验报告；日报计划在 `validate-sec-metrics` 前显式运行该合并，`score-daily` 也会在校验前合并本地 TSM IR 缓存|已实现基础版|
|交易假设|`data/external/trade_theses/`|记录主动交易 thesis、验证指标、证伪条件、状态机当前状态、前状态、状态变化原因、证据引用和人工复核要求；`created_at`、`status_updated_at`、验证指标 `updated_at`、证伪条件 `triggered_at` 和风险事件 `updated_at` 是 replay 可见性过滤输入；当前没有任何活跃 ticker 要求 thesis 时，目录缺失不触发 thesis gate|已实现基础版|
|交易假设模板|`docs/examples/trade_theses/`|提供可复制 YAML 模板，不提交个人记录|已实现基础版|
|假设校验|`aits thesis validate`|校验 schema、观察池引用、产业链节点、证伪约束、状态迁移、状态变化元数据和人工复核要求|已实现基础版|
|假设复核|`aits thesis review`|输出 thesis 是否仍成立、处于 warning/challenged、是否需要人工复核或是否证伪触发；日报将 invalidated 视为人工复核失败输入|已实现基础版|
|估值拥挤度|`data/external/valuation_snapshots/`|记录估值分位、预期变化、拥挤度、point-in-time 等级、历史来源等级、可信度、可信度原因和回测用途|已实现基础版|
|FMP historical valuation 原始缓存|`data/raw/fmp_historical_valuation/`|保存 FMP historical `key-metrics` / `ratios` 原始响应、请求参数、下载时间、row count 和 checksum，用于回填当前估值分位的本地历史分布|已实现基础版|
|FMP 历史估值拉取|`aits valuation fetch-fmp-valuation-history`|从 Financial Modeling Prep historical `key-metrics` 和 `ratios` 拉取年度或季度历史倍数，生成 paid vendor 历史估值快照；快照标记为 `backfilled_history_distribution`、`low` confidence 和 `captured_at_forward_only`，不能伪装为严格 point-in-time 历史输入|已实现基础版|
|FMP 历史估值拉取报告|`outputs/reports/fmp_historical_valuation_fetch_YYYY-MM-DD.md`|记录 provider、endpoint、period、limit、请求标的、provider symbol alias、下载时间、原始记录数、checksum、生成历史估值快照数、错误和警告；不输出 API key|已实现基础版|
|FMP analyst estimates 历史缓存|`data/raw/fmp_analyst_estimates/`|保存原始 annual analyst-estimates 响应、请求参数、下载时间、row count 和 checksum，用于同一 fiscal estimate date 的 90 日 EPS revision；文件名包含 captured date、downloaded_at 和 checksum 前缀，同日多次运行不得覆盖已被 PIT manifest 引用的 raw payload|已实现基础版|
|FMP analyst history 校验|`aits valuation validate-fmp-history`|校验原始 analyst-estimates JSON 的 schema、checksum、row_count、ticker、请求参数、日期和重复 estimate date|已实现基础版|
|FMP 估值/预期拉取|`aits valuation fetch-fmp`|从 Financial Modeling Prep 拉取 quote、TTM key metrics、TTM ratios 和 annual analyst estimates，按显式 provider symbol alias 处理 `GOOG -> GOOGL`，对负数估值倍数记录警告并跳过该指标，读取历史 analyst 快照计算 `eps_revision_90d_pct`，读取本地估值快照历史计算 `valuation_percentile`，生成 paid_vendor 当前采集快照 YAML，并复用估值快照校验；本地历史可来自真实 point-in-time 快照或 `fetch-fmp-valuation-history` 的 captured_at 审计回填|已实现基础版|
|FMP 拉取报告|`outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md`|记录 provider、endpoint、请求标的、provider symbol alias、下载时间、返回记录数、checksum、历史 analyst 快照读取数、本地估值历史读取数、生成快照数、字段口径限制、错误和警告；不输出 API key|已实现基础版|
|FMP analyst history 校验报告|`outputs/reports/fmp_analyst_history_validation_YYYY-MM-DD.md`|记录原始历史快照数量、ticker 覆盖、记录数、checksum 校验结果、错误和警告|已实现基础版|
|EODHD Earnings Trends 原始缓存|`data/raw/eodhd_earnings_trends/`|保存 EODHD `calendar/trends` 原始响应、请求参数、下载时间、row count 和 checksum，用于当前采集日可见的 EPS 90 日修正 baseline|已实现基础版|
|EODHD EPS trend baseline 拉取|`aits valuation fetch-eodhd-trends`|从 EODHD Earnings Trends 拉取 `epsTrendCurrent` 和 `epsTrend90daysAgo`，合并进当前可见基础估值快照，生成带 `vendor_current_trend` 和 `captured_at_forward_only` 的 paid vendor 快照；估值倍数、估值分位和拥挤度继承基础快照，不由 trends 推断|已实现基础版|
|EODHD EPS trend baseline 报告|`outputs/reports/eodhd_earnings_trends_fetch_YYYY-MM-DD.md`|记录 provider、endpoint、请求标的、provider symbol、下载时间、trend 记录数、checksum、基础估值快照读取数、生成合并快照数、错误和警告；不输出 API key；报告声明采集日前严格回测不可见|已实现基础版|
|估值 CSV 导入|`aits valuation import-csv`|导入结构化估值/预期 CSV，转换为估值快照 YAML，并复用现有快照校验|已实现基础版|
|估值导入报告|`outputs/reports/valuation_import_YYYY-MM-DD.md`|记录 CSV 行数、checksum、导入快照数、错误和警告|已实现基础版|
|估值模板|`docs/examples/valuation_snapshots/`|提供可复制 YAML 模板，不提交个人记录|已实现基础版|
|估值校验|`aits valuation validate`|校验来源、日期、ticker、指标值、新鲜度、PIT 可信度和回测用途；按 `as_of/captured_at <= 评估日` 过滤可见快照，晚于评估日的后续快照以 warning 记录并排除，不进入历史日评分/复核；回填历史分布输出警告，低可信快照不得声明 strict point-in-time 回测用途|已实现基础版|
|估值复核|`aits valuation review`|按 `as_of/captured_at` 选择每个 ticker 最新可见快照，输出估值是否偏贵、拥挤或数据过期，并显示 `valuation_percentile`、`eps_revision_90d_pct` 当前覆盖、PIT 等级、可信度和回测用途|已实现基础版|
|历史估值切片|`src/ai_trading_system/historical_inputs.py`|回测中按 signal_date 过滤估值快照，只保留 as_of/captured_at 不晚于信号日且每个 ticker 最新的快照|已实现基础版|
|历史风险事件切片|`src/ai_trading_system/historical_inputs.py`|回测中按 signal_date 过滤风险事件证据和复核声明，排除当时已解决事件，并把未来 resolved/dismissed 状态重解释为 active/watch；复核声明只保留当时已 review 且 checked_sources 已可见的记录|已实现基础版|
|市场证据账本|`data/external/market_evidence/`|记录新市场信息 evidence_id、来源类型、采集时间、去重键、影响 ticker/产业链节点、S/A/B/C/D/X 证据等级、方向、置信度、人工复核状态和可链接对象；LLM 抽取证据强制 pending_review|已实现基础版|
|市场证据导入|`aits evidence import-csv`|从人工复核或 LLM 分类后的 CSV 导入 market_evidence YAML，记录 CSV 行数、checksum、导入数量和错误|已实现基础版|
|市场证据校验|`aits evidence validate`|校验证据账本 schema、重复 evidence_id/source key、未来日期和来源策略；报告显示按保守 source policy 可作为普通评分输入的 evidence 数；`B` 级须 confirmed 后才能普通评分，`C/D/X`、`llm_extracted` 与 `public_convenience` 只能进入待复核或辅助解释|已实现基础版|
|市场证据报告|`outputs/reports/market_evidence_YYYY-MM-DD.md`|输出 evidence 记录、来源类型、证据等级、复核状态、关联对象和问题清单|已实现基础版|
|交易记录|`data/external/trades/`|记录真实交易、价格、仓位和 thesis_id；`recorded_at` 用于证明记录可见性，`updated_at/opened_at/closed_at` 用于 replay 过滤和未来平仓信息隔离|已实现基础版|
|交易复盘|`aits review-trades`|先过数据质量门禁，再对比 SPY/QQQ/SMH/SOXX 做基础归因|已实现基础版|
|日报复核摘要|`aits score-daily`|汇总 thesis、风险事件规则与发生记录、估值快照和交易复盘状态；交易复盘复用同一份数据质量门禁结果|已实现基础版|
