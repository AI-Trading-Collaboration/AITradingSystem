# AI Trading System

面向美股 AI 产业链的投资认知、趋势分析、风险评分、回测与仓位建议系统。

项目目标不是预测市场，也不是自动交易。当前阶段只做 AI 产业链趋势判断和投研复核辅助，不实际触发交易；仓位区间、gate 和执行动作都是解释语言，不能直接转成账户买卖。长期方向是可审计认知模型：持续记录 `belief_state`、证据、置信度、风险边界和规则改进建议，但生产规则必须经过回测、shadow mode 和人工批准。

产品定位详见 [docs/product_strategy.md](docs/product_strategy.md)：系统应服务于能力圈、产业链因果、仓位决策和复盘归因，而不是扩张成全市场万能分析器。工程落地拆解见 [docs/implementation_backlog.md](docs/implementation_backlog.md)，具体未完成任务和优先级见 [docs/task_register.md](docs/task_register.md)。

## MVP 范围

第一版只做闭环：

1. 市场价格与宏观风险数据采集。
2. 趋势、相对强弱、波动率、利率等特征计算。
3. 规则评分模型。
4. 仓位区间建议。
5. 与 QQQ、SMH/SOXX、SPY 的回测对比。
6. 每日 Markdown 报告。

SEC 基本面已经接入基础硬数据评分；估值快照和政策/地缘风险发生记录已经接入可审计的手工输入评分，并支持从结构化 CSV 导入来减少手工 YAML 维护。TSMC IR 季度基本面已支持从官方 Management Report 文本或 PDF 可抽取文本层导入，并可显式合并到统一 SEC-style 指标 CSV；LLM claim 预审已支持 OpenAI Responses API 结构化输出和待复核队列，默认使用 `gpt-5.5`、`reasoning.effort=high` 与 `requests` HTTP 客户端，单请求失败最多重试 2 次，并记录审计字段；live OpenAI 请求默认写入本地短 TTL request/response 缓存，完全相同 payload 在 TTL 内复用，但只能生成 `llm_extracted` / `pending_review` 线索，不能直接触发交易动作。

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
docs/task_register.md   未完成任务、优先级、状态和阻塞项登记表
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

默认会缓存核心范围：`SPY`、`QQQ`、`SMH`、`SOXX`、防守 ETF、`^VIX`、`MSFT`、`GOOG`、`TSM`、`INTC`、`AMD`、`NVDA`，以及 FRED 的 `DGS2`、`DGS10`、`DTWEXBGS`。`DTWEXBGS` 是 Federal Reserve 广义美元指数代理，不是 ICE DXY。`download-data` 默认要求 `FMP_API_KEY`，使用 FMP 写入股票/ETF 主价格，并从 Cboe VIX official historical data 补 `^VIX` 到 `data/raw/prices_daily.csv` 主价格缓存；默认也要求 `MARKETSTACK_API_KEY` 并写入 `data/raw/prices_marketstack_daily.csv` 作为股票/ETF 第二行情源。Yahoo Finance 仅保留为显式 `--price-provider yahoo` 迁移调查选项，不再是默认生产主源。如需在无 Marketstack key 的临时环境只更新主缓存，必须显式传入 `--without-marketstack`，且默认生产路径的 `validate-data` 仍会要求第二来源文件。Marketstack Basic 当前不覆盖 `^VIX`；第二来源只用于可覆盖标的的 cross-provider reconciliation，不会覆盖主价格缓存。每次下载会追加写入 `data/raw/download_manifest.csv`，记录 provider、endpoint、请求参数、下载时间、行数、输出路径和 sha256，但不记录 API key。如需抓取配置里的完整 AI 产业链标的：

所有外部供应商 HTTP/下载请求还会先经过请求级缓存 `data/raw/external_request_cache/`。该缓存按 provider、api family、HTTP method、endpoint 和脱敏后的请求参数生成 cache key；完全相同请求命中时直接复用本地 `response.body`，不再发送给供应商，避免消耗 FMP、Marketstack、SEC、FRED、Cboe、EODHD、TSMC IR、官方政策源或 yfinance 的调用额度。cache metadata 只保存脱敏请求身份、status code、response headers、body checksum 和路径；API key、token、Authorization、Cookie、User-Agent 不写入原文。这个请求级 cache 是底层供应商调用保护，不替代 `download_manifest.csv`、PIT manifest、SEC/valuation raw payload 或数据质量门禁。若 `download-data` 在供应商连接、HTTP、JSON/schema 或标准化阶段失败，会写出 `outputs/reports/download_data_diagnostics_YYYY-MM-DD.md`，报告只保留 provider、cache status、cache key、脱敏请求参数和下游影响，不保存 stdout/stderr 原文或供应商响应正文。

```powershell
aits download-data --start 2018-01-01 --full-universe
```

校验本地数据缓存并生成质量报告：

```powershell
aits validate-data
```

质量报告默认写入 `outputs/reports/data_quality_YYYY-MM-DD.md`。如果校验出现错误，命令会返回非零退出码，后续评分和回测流程不应继续使用这批数据。
当 `data/raw/prices_marketstack_daily.csv` 存在或默认生产路径要求第二来源时，质量门禁会校验 Marketstack 缓存 schema、覆盖、新鲜度、重复键、异常值，并比较主价格缓存和 Marketstack 的 raw `close` 价差。`aits validate-data` 会同步写出 `data_quality_YYYY-MM-DD_marketstack_reconciliation.csv`：能证明为指数 volume 不适用、已知拆股复权跳变、Marketstack 第二源自身坏点，或 raw `close` 已对齐但 adjusted close 分红复权口径不同的，会以 `INFO` 记录规则、证据和主/二源数值；不能解释的 raw close/OHLC 冲突仍作为 warning/error 保留，不会自动平滑或修正任何价格。

当质量报告显示 Marketstack 第二源自身坏行时，可运行只读诊断复查：

```powershell
aits data-sources yahoo-price-diagnostic --as-of 2026-05-02
```

该命令只针对 Marketstack self-check 异常的 `ticker/date` 附近样本拉取 Yahoo raw OHLC/close，输出 `outputs/reports/yahoo_price_diagnostic_YYYY-MM-DD.md`。Yahoo 保持 `public_convenience` / `production_effect=none`，不会写入 `prices_daily.csv`、`prices_marketstack_daily.csv`、评分、仓位闸门或回测真值；失败、空结果或与 FMP 不一致只进入调查报告。

查看和校验数据源目录：

```powershell
aits data-sources list
aits data-sources validate --as-of 2026-05-02
```

数据源目录在 `config/data_sources.yaml`。它记录当前 FMP、Marketstack、FRED、本地手工输入和计划接入来源的 provider、endpoint、缓存路径、审计字段、校验项、限制说明和 provider 级 LLM 处理权限。这个命令不下载数据，只校验“来源是否可审计、限制是否明确”，用于后续接入财报、估值和新闻事件源前的来源纪律；外部 LLM 授权未知时默认 fail closed。

建立并校验 forward-only PIT raw snapshot manifest：

```powershell
aits pit-snapshots fetch-fmp-forward --as-of 2026-05-02
aits pit-snapshots build-manifest --as-of 2026-05-02
aits pit-snapshots validate --as-of 2026-05-02
```

`fetch-fmp-forward` 会抓取 FMP analyst estimates、price target、ratings 和 earnings calendar，写入 `data/raw/fmp_forward_pit/` 与 `data/processed/pit_snapshots/fmp_forward_pit_YYYY-MM-DD.csv`，并刷新 PIT manifest。`build-manifest` 会把现有 FMP analyst estimates、FMP historical valuation、FMP forward PIT 和 EODHD Earnings Trends 原始缓存登记到 `data/raw/pit_snapshots/manifest.csv`，记录 raw payload 路径、sha256、row count、请求参数、`ingested_at`、`available_time`、PIT 可信度和 provider 授权字段。`validate` 会生成 `outputs/reports/pit_snapshots_validation_YYYY-MM-DD.md`；严重错误时后续评分、回测或报告不得使用这些快照。PIT 快照是 forward-only 日常前置步骤，缺跑日期不能事后补写成 strict PIT。

日常调度如果希望 PIT 抓取失败不阻断后续日报，可显式使用：

```powershell
aits pit-snapshots fetch-fmp-forward --as-of 2026-05-02 --continue-on-failure
```

该模式只改变命令退出码：供应商、权限、写入或 PIT 校验失败仍会写入脱敏失败报告或由 `ops health` 告警暴露；失败快照不得作为可用 PIT 输入，`score-daily` 仍必须执行自己的市场数据质量、SEC、估值、风险事件和 rule card 门禁。

后续 `aits valuation fetch-fmp` 默认读取 `data/processed/pit_snapshots/` 的 FMP PIT 标准化索引计算 `eps_revision_90d_pct`，只使用 `available_time <= decision_time` 的自建快照；自建历史不足 90 天时会明确降级，不用未来快照或供应商当前历史视图补洞。

日常运行健康检查会检查 PIT 快照是否缺跑、断更、row count 是否低于阈值，以及 manifest 中 raw payload checksum 是否仍能复核：

```powershell
aits ops health --as-of 2026-05-02
```

该命令默认检查 `data/raw/pit_snapshots/manifest.csv`、当日 `data/processed/pit_snapshots/fmp_forward_pit_YYYY-MM-DD.csv` 和 `outputs/reports/pit_snapshots_validation_YYYY-MM-DD.md`，并输出 `outputs/reports/pipeline_health_YYYY-MM-DD.md` 与 `outputs/reports/pipeline_health_alerts_YYYY-MM-DD.md`。告警只做 data/system 复核提示，`production_effect=none`，不改变评分、仓位、回测或执行建议。

在接入本地任务计划程序或云 VM 前，可以先生成每日运行计划，检查命令顺序、必需环境变量和预期输出：

```powershell
aits ops daily-plan --as-of 2026-05-02
```

计划默认包含 `download-data`、带 `--continue-on-failure` 的 `pit-snapshots fetch-fmp-forward`、`fundamentals download-sec-companyfacts`、`fundamentals extract-sec-metrics`、`fundamentals merge-tsm-ir-sec-metrics`、`fundamentals validate-sec-metrics`、`valuation fetch-fmp`、`score-daily`、`reports dashboard`、`ops health` 和 `security scan-secrets`。报告写入 `outputs/reports/daily_ops_plan_YYYY-MM-DD.md`，只检查阻断性环境变量是否非空，不输出 secret 值，也不实际调用供应商 API。缺少 `FMP_API_KEY`、`MARKETSTACK_API_KEY`、`SEC_USER_AGENT` 或默认 OpenAI 预审需要的 `OPENAI_API_KEY` 时，计划状态会显示 `BLOCKED_ENV`；其中 `FMP_API_KEY` 会因 `download-data`、PIT 估值快照刷新等默认步骤而阻断，PIT 抓取自身失败则进入失败报告和 pipeline health 告警。如需把它作为调度前门禁，可加 `--fail-on-missing-env`。若离线排查需要跳过 OpenAI 预审、SEC fundamentals、估值快照刷新或 PIT 抓取，必须显式传入对应 `--skip-*` 选项，后续日报和运行记录仍需声明该限制。

`daily-plan` 和 `daily-run` 会先判断 U.S. equity market session。周末或 NYSE 常规整日休市日会进入 `CLOSED_MARKET` 模式：报告声明休市原因和上一交易日；如果主价格和 Marketstack 第二行情源已覆盖上一交易日，则跳过 `download-data`，否则只用上一交易日作为 `download-data --end`；默认跳过 `score-daily` 和 dashboard，不生成新的日报评分、decision snapshot、evidence bundle、prediction ledger 行、执行动作或网页展示层。休市日仍会运行官方政策/地缘来源抓取、PIT、SEC、valuation、`ops health --non-trading-day` 和 secret scan，用于风险线索和缓存健康复核。

未显式传入 `--as-of` 时，`daily-plan` 和 `daily-run` 默认按 `America/New_York` 的 U.S. equity market 日历选择最新已完成交易日：常规交易日美东 16:30 之后使用当日，16:30 前、周末或 NYSE 常规整日休市日使用上一交易日。这个默认值避免本机时区或 UTC 零点把亚洲早晨运行误判为下一个尚未完成的美股交易日；显式传入 `--as-of` 时仍按用户指定日期执行可见性门禁。

每日真实执行入口使用同一份计划顺序，并额外生成脱敏执行报告：

```powershell
aits ops daily-run --as-of 2026-05-02
```

`daily-run` 会先写出 `outputs/reports/daily_ops_plan_YYYY-MM-DD.md`，再做输入可见性预检查，随后按顺序调用本地 CLI。`daily-run` 是生产调度入口，不用于历史时点复现；输入可见性预检查对交易日使用最新已完成美股交易日，对显式休市日使用当前 America/New_York 日期，显式未来 `--as-of` 会在任何 download/PIT/SEC/valuation/OpenAI/dashboard 子命令前返回 `BLOCKED_VISIBILITY`，显式历史 `--as-of` 会提示改用 `aits ops replay-day --mode cache-only --as-of YYYY-MM-DD`。交易日 `score-daily` 成功后会自动生成 `evidence_dashboard_YYYY-MM-DD.html/json`，该步骤只读展示已生成 artifact，`production_effect=none`，不改变评分、仓位、回测或执行建议。执行器内部优先用项目 `.venv` Python 调用 daily-run direct dispatcher，找不到本地虚拟环境时才回退当前 Python，避免 Windows 上从 `aits.exe` 父进程递归启动 `aits.exe`、PATH 上的全局 `aits` 污染每日子命令环境，以及 Typer 对整棵 CLI 做全局解析时触发的 Windows 原生崩溃；子命令环境显式设置 `PYTHONMALLOC=malloc`、`PYTHONFAULTHANDLER=1`、`PYTHONDONTWRITEBYTECODE=1`，为每次 `daily-run` 使用独立 `PYTHONPYCACHEPREFIX=outputs/tmp/pycache/daily_run/run_*`，并在启动子命令前清理 `src/**/__pycache__`，降低 Windows 本机长流程子进程原生崩溃和字节码缓存异常风险。缺少阻断性环境变量时直接返回 `BLOCKED_ENV`，任一执行步骤退出码非 0 或关键 artifact 报告状态非 `PASS*` 时停止，不继续下游步骤；休市日的显式 `SKIPPED` 步骤不视为失败。执行报告写入 `outputs/reports/daily_ops_run_YYYY-MM-DD.md`，只记录步骤状态、退出码、耗时、stdout/stderr 行数和预期 artifact 路径，不保存 stdout/stderr 原文、API key、token 或付费内容原文；同目录会同步写入 `daily_ops_run_metadata_YYYY-MM-DD.json`，记录 run id、git/config/rule hash、命令清单、必需环境变量 presence、输入可见性状态、pre-run input checksum、produced artifact checksum 和 production visibility cutoff，同样不保存 secret 值或命令输出原文。调度器应调用 `daily-run`；`daily-plan` 保留为只读计划和凭据检查。

历史交易日分析产出回放使用隔离 replay bundle，默认只读本地归档输入，不调用 live provider 或 OpenAI：

```powershell
aits ops replay-day --as-of 2026-05-08 --mode cache-only
aits ops replay-day --as-of 2026-05-08 --mode cache-only --compare-to-production
aits ops replay-day --as-of 2026-05-08 --mode cache-only --openai-replay-policy cache-only
aits ops replay-window --start 2026-05-01 --end 2026-05-08 --mode cache-only
```

`replay-day` 会在 `outputs/replays/YYYY-MM-DD/<run-id>/` 下生成 input freeze manifest、过滤后的 PIT manifest、过滤后的 valuation snapshot 视图、过滤后的 `trade_theses` / `trades` 手工输入视图、replay-scoped `score-daily` 输出、dashboard、pipeline health、secret hygiene 和 `replay_run.md/json`。默认可见窗口优先读取 production `daily_ops_run_metadata_YYYY-MM-DD.json` 的 `visibility_cutoff`，没有 metadata 时才退回 as-of 当日 UTC 末尾。它会按 as-of 可见窗口排除未来 PIT manifest 行、未来 valuation YAML、未来 thesis 状态和不可证明可见的交易记录，并把 `score-daily` 的 features、scores、daily score、alerts、decision snapshot、evidence bundle、prediction ledger、`evidence_dashboard_YYYY-MM-DD.html/json` 等输出全部写入 replay 命名空间；生产 canonical artifacts 不会被改写。打开 `--compare-to-production` 时会额外生成 `diff_vs_production.md/json`，比较本地 production 与 replay 的日报、alerts、dashboard、decision snapshot、trace、features/scores 当日行等 artifact checksum 和行数。OpenAI replay 策略默认为 `disabled`；`--openai-replay-policy cache-only` 会读取历史 `risk_event_prereview_queue.json` 和当日 OpenAI 预审报告，但只把 `request_timestamp/cache_created_at` 或匹配 cache 文件时间可证明不晚于 replay cutoff 的记录写入 replay queue，晚于 cutoff 或缺少可证明时间戳的记录进入排除审计，不调用 live OpenAI。`replay-window` 按 U.S. equity trading day 批量运行单日 cache-only replay，周末和常规整日休市日默认跳过，并在 `outputs/replays/windows/<run-id>/replay_window.md/json` 汇总每个交易日的状态和 diff 状态。

构建每日市场特征：

```powershell
aits build-features --as-of 2026-05-01
```

命令会先执行数据质量门禁，失败时停止。特征默认写入 `data/processed/features_daily.csv`，报告默认写入 `outputs/reports/feature_summary_YYYY-MM-DD.md`。

生成每日市场评分报告：

```powershell
aits score-daily --as-of 2026-05-01
```

命令会先执行市场数据质量门禁，再构建市场特征，并校验 `data/processed/sec_fundamentals_YYYY-MM-DD.csv`、生成 SEC 基本面特征，最后输出 `data/processed/scores_daily.csv` 和 `outputs/reports/daily_score_YYYY-MM-DD.md`。日报会同时汇总交易 thesis、风险事件、估值快照和交易复盘的复核状态；缺少本地手工输入会显示为警告，配置或 YAML 错误会显示为复核失败。SEC 基本面特征通过校验后会进入基本面硬数据评分；估值快照通过校验后会以估值分位和拥挤比例进入手工/审计输入评分，过期快照和 `public_convenience` 来源不会进入自动评分；政策/地缘评分只读取已校验的 `data/external/risk_event_occurrences/*.yaml` 发生记录，没有合格发生记录时显示为数据不足，不把 `config/risk_events.yaml` 的监控规则当作已发生风险或无风险证明。

日报评分默认启用官方来源 + OpenAI 风险事件预审；如需排查或离线运行，可显式传入 `--skip-risk-event-openai-precheck`：

```powershell
aits score-daily --as-of 2026-05-05 --skip-risk-event-openai-precheck
```

该流程会先抓取 Federal Register/BIS/OFAC/USTR/Congress.gov/GovInfo/Trade.gov CSL 等官方政策/地缘来源，再用 `OPENAI_API_KEY` 调用 OpenAI Responses API 做 `metadata_only` 预审；为控制成本和延迟，默认最多处理 20 条官方候选，可用 `--risk-event-openai-precheck-max-candidates` 调整；默认模型为 `gpt-5.5`、`reasoning.effort=high`、请求读超时为 120 秒，默认 HTTP 客户端为 `requests`，默认底层 agent 请求缓存目录为 `data/processed/agent_request_cache`、TTL 为 24 小时，可用 `--openai-cache-ttl-hours 12` 改成半天窗口，也可用 `--openai-model`、`--openai-reasoning-effort`、`--openai-timeout-seconds` 和 `--openai-http-client urllib` 调整或对照；完全相同 request payload 在 TTL 内 cache HIT 不重新调用 OpenAI，MISS/EXPIRED 才重新发送。生产日报只在评估日等于最新已完成美股交易日时，允许 OpenAI `request_timestamp` 晚于 `as_of` 但不晚于本轮 `visibility_cutoff`，用于支持收盘后 UTC/JST 次日生成前一美股交易日日报；历史 as-of 或无生产 cutoff 的路径仍按 `as_of` 当日 UTC 末尾 fail closed。单个 OpenAI 请求遇到超时、429 或 5xx 等瞬时失败时最多重试 2 次，第 3 次仍失败则整批 fail closed。失败报告只输出 sanitized transport diagnostics，包括 attempt、HTTP client、client request id、endpoint host、payload byte size、input checksum、HTTP status、OpenAI x-request-id 或异常类型，不输出 API key、Authorization header 或未授权全文；本地请求缓存会记录 provider/api family、脱敏 request、response body、attempt diagnostics、cache key 和 checksum，OpenAI 归档位于 `archive/openai/responses/YYYY-MM-DD/`。预审输出写入 `data/processed/risk_event_prereview_queue.json` 和 `outputs/reports/risk_event_prereview_openai_YYYY-MM-DD.md`；按 2026-05-12 owner 决策，日报默认还会把成功预审结果写入 LLM formal occurrence/attestation 和 `outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md`，作为政策/地缘正式评估输入。LLM formal 不是人工复核，full coverage 置信度低于人工复核但不再按低置信模块处理；LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 position gate。缺少 OpenAI key、官方来源抓取失败、provider LLM 权限失败、provider `cache_allowed=false`、OpenAI 请求最终失败或 LLM formal 写入失败时，默认日报评分会停止。

运行历史回测：

```powershell
aits backtest --to 2026-05-02 --quality-as-of 2026-05-02
```

回测命令会先执行市场数据质量门禁和 SEC companyfacts 缓存校验。默认市场阶段来自 `config/market_regimes.yaml`，当前为 `ai_after_chatgpt`，起点是 `2022-12-01`，即 ChatGPT 于 `2022-11-30` 公开发布后的首个完整美股交易日。当前基础版使用每日评分得到的 AI 仓位区间中点作为目标仓位，以 `SMH` 作为默认 AI 代理标的，并与 `SPY`、`QQQ`、`SMH`、`SOXX` 买入持有基准对比。每个 signal_date 会按 `filed_date <= signal_date` 生成 point-in-time SEC 基本面特征，也会按 `as_of/captured_at <= signal_date` 过滤估值快照，并按当时可见证据重建风险事件发生记录；回测报告会声明数据质量门禁错误/警告计数、缓存文件摘要、SEC、估值和风险事件质量摘要，并输出执行成本摘要、评分模块覆盖率摘要、月度覆盖率趋势、月度来源类型趋势、月度输入问题下钻、月度输入证据 URL 摘要、月度风险事件证据 URL 明细、月度 ticker 输入摘要、月度 ticker SEC 特征明细、月度估值快照来源和月度风险事件证据来源分布，同时写出机器可读的 `backtest_input_coverage_YYYY-MM-DD_YYYY-MM-DD.csv` 覆盖诊断和中文 `backtest_audit_YYYY-MM-DD_YYYY-MM-DD.md` 输入审计报告。审计报告会汇总数据质量门禁、point-in-time 输入切片、模块覆盖率、来源类型、历史输入问题和执行假设，帮助判断这次回测是否可解释；如需把审计 WARNING 作为本地门禁失败，可加 `--fail-on-audit-warning`。信号按收盘后生成、下一交易日生效，避免未来函数。默认扣除 5 bps 单边交易成本；如需保守执行假设，可用 `--slippage-bps` 显式加入线性滑点或盘口冲击估算。

回测完成后，可以单独生成 gate 与事件效果归因审计：

```powershell
aits backtest-gate-attribution --backtest-daily-path outputs/backtests/backtest_daily_2026-04-01_2026-05-05.csv --input-coverage-path outputs/backtests/backtest_input_coverage_2026-04-01_2026-05-05.csv --as-of 2026-05-06
```

该报告只读使用已生成的回测 daily 和 input coverage CSV，按 gate 输出 trigger count、平均仓位降幅、avoided drawdown、missed upside、net effect、false alarm 和 late trigger，并显示风险事件/LLM 标签可用性。它是 `production_effect=none` 的一阶解释报告，不改变回测、评分、仓位闸门或执行策略，也不能把多个 gate 的净效应相加为生产收益结论。

任何回测调权或权重优化实验进入 shadow / promotion 前，先校验调权协议 manifest；模板见 `docs/examples/weights/calibration_protocol_template.yaml`：

```powershell
aits feedback validate-calibration-protocol --manifest-path config/weights/calibration_protocol.yaml --as-of 2026-05-06
```

协议校验会要求固定数据快照、配置版本、成本和执行假设、`ai_after_chatgpt` 日期范围、nested walk-forward、purging/embargo、trial 次数、benchmark set、参数分层和多重测试折扣。通过校验只表示实验协议可用于研究，不批准 overlay、不修改 production scoring、`position_gate` 或回测仓位。

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

交易 thesis 默认读取 `data/external/trade_theses/*.yaml`，该目录不提交。可参考 `docs/examples/trade_theses/nvda_ai_infra_template.yaml` 复制模板。校验会检查 schema、ticker 是否在观察池、产业链节点是否存在、验证指标和证伪条件是否完整；复核报告会输出原始假设是否仍成立、是否需要人工复核、是否已有证伪条件触发。历史 replay 会把该目录复制成隔离过滤视图，只保留 `created_at`、`status_updated_at`、验证指标 `updated_at`、证伪触发时间和风险事件 `updated_at` 均不晚于 replay as-of 的 thesis。

查看和校验风险事件分级规则：

```powershell
aits risk-events list
aits risk-events validate --as-of 2026-05-02
aits risk-events triage-official-candidates --as-of 2026-05-02
aits risk-events precheck-triaged-official-candidates --as-of 2026-05-02
aits risk-events apply-llm-formal-assessment --as-of 2026-05-02
aits risk-events precheck-openai --input-path docs/examples/risk_event_prereview/openai_live_precheck_template.yaml --as-of 2026-05-02
aits risk-events import-prereview-csv --input-path docs/examples/risk_event_prereview/openai_prereview_template.csv --as-of 2026-05-02
aits risk-events import-occurrences-csv --input-path data/external/risk_event_imports/reviewed_events.csv --as-of 2026-05-02
aits risk-events list-occurrences
aits risk-events validate-occurrences --as-of 2026-05-02
```

风险事件配置在 `config/risk_events.yaml`，只定义需要监控的 L1/L2/L3 规则、AI 仓位折扣乘数、人工复核要求、影响产业链节点、相关标的、建议动作、升级条件和解除条件。`triage-official-candidates` 读取官方来源候选 CSV，按 AI 模块直接相关性输出 `must_review`、`review_next`、`sample_review`、`auto_low_relevance` 和 `duplicate_or_noise`，用于降低无明显 AI/半导体/先进计算/出口管制/核心 ticker 关系候选的人工复核优先级；triage 输出保持 `production_effect=none`，不代表已确认无风险。`precheck-triaged-official-candidates` 默认只把 `must_review/review_next` 高优先级官方候选送入 OpenAI metadata-only 预审，输出 `status_suggestion` 和 `level_suggestion`。按当前 owner 决策，`apply-llm-formal-assessment` 和默认 `score-daily` 可把 LLM 预审队列写成正式 occurrence 和 LLM formal attestation；该模式不是人工复核，日报来源类型显示为 `llm_formal_assessment`，LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 position gate。OpenAI 只能通过 `precheck-triaged-official-candidates`、`apply-llm-formal-assessment`、`precheck-openai`、`import-prereview-csv` 或 `score-daily --risk-event-openai-precheck` 参与风险事件链路；provider 授权未知或不允许本地缓存归档时 fail closed。实际发生记录默认读取 `data/external/risk_event_occurrences/*.yaml`，该目录不提交；可参考 `docs/examples/risk_event_occurrences/export_control_active_template.yaml` 复制模板。`import-occurrences-csv` 只接受人工复核后的结构化 CSV，同一 `occurrence_id` 的多行用于合并证据来源，关键字段冲突会停止导入。政策/地缘评分只读取已通过校验且评估日可见的发生记录；晚于评估日的 occurrence、evidence 或复核声明会在校验报告中以 warning 记录并排除，不进入历史日评分、仓位闸门或当前有效声明。`public_convenience` 证据只能作为辅助，不能单独进入自动评分。

校验和复核估值、预期与拥挤度快照：

```powershell
aits valuation fetch-fmp --tickers NVDA,MSFT --as-of 2026-05-02
aits valuation fetch-fmp-valuation-history --tickers NVDA,MSFT --as-of 2026-05-02
aits valuation fetch-eodhd-trends --tickers NVDA,MSFT --as-of 2026-05-02
aits valuation validate-fmp-history --as-of 2026-05-02
aits valuation import-csv --input-path data/external/valuation_imports/vendor_export.csv --as-of 2026-05-02
aits valuation validate --as-of 2026-05-02
aits valuation review --as-of 2026-05-02
```

估值快照默认读取 `data/external/valuation_snapshots/*.yaml`，该目录不提交。可参考 `docs/examples/valuation_snapshots/nvda_valuation_template.yaml` 复制模板。`fetch-fmp` 从 Financial Modeling Prep 读取 `quote-short`、`key-metrics-ttm`、`ratios-ttm` 和 annual `analyst-estimates`，API key 只从 `FMP_API_KEY` 读取，不会写入报告；命令会生成 `outputs/reports/fmp_valuation_fetch_YYYY-MM-DD.md`，再写入估值快照 YAML 并复用 `valuation validate`。核心观察池内部 ticker 保持 `GOOG`，但 FMP 请求会使用显式 provider symbol alias `GOOG -> GOOGL`，并在拉取报告和 analyst history 请求参数中记录。FMP 返回负数估值倍数时不会写入快照，会在拉取报告中记录警告。每次成功拉取都会把原始 `analyst-estimates` 响应写入 `data/raw/fmp_analyst_estimates/`，用于后续按同一 fiscal estimate date 计算 `eps_revision_90d_pct`；`validate-fmp-history` 会校验这些原始 JSON 的 schema、checksum、row_count、ticker、请求参数、日期和重复 estimate date。`fetch-fmp-valuation-history` 会从 FMP historical `key-metrics` / `ratios` 拉取历史 `ev_sales` 和 `peg` 分布，原始响应写入 `data/raw/fmp_historical_valuation/`，并生成带 `captured_at` 审计日期的 paid vendor 历史估值快照，用于后续 `fetch-fmp` 计算 `valuation_percentile`；这不等同于真实 point-in-time vendor archive，历史回测在采集日前不可见，也不能用于伪造 `eps_revision_90d_pct`。`fetch-eodhd-trends` 从 EODHD `calendar/trends` 读取 `epsTrendCurrent` 和 `epsTrend90daysAgo`，API key 只从 `EODHD_API_KEY` 读取；命令会保存 `data/raw/eodhd_earnings_trends/` 原始响应，并把当前 EPS 90 日修正合并进已存在的基础估值快照。EODHD 合并快照只补当前采集日可见的 `eps_revision_90d_pct`，估值倍数、估值分位和拥挤度继承基础快照，不能替代真实 PIT estimates archive。`valuation_percentile` 使用本地估值快照历史计算；每个估值 metric 至少需要 3 个历史点，样本不足时不会伪造分位。日报和 `valuation review` 会按 `as_of/captured_at` 只选择每个 ticker 的最新可见快照进入当日评分，并在报告中显示 `valuation_percentile` 与 `eps_revision_90d_pct` 的当前覆盖；晚于评估日的后续快照会在校验报告中以 warning 记录并排除，不进入历史日评分或复核。`import-csv` 可把结构化宽表导入为估值快照 YAML，并生成 `outputs/reports/valuation_import_YYYY-MM-DD.md`；CSV 每行仍必须声明真实 `source_name`、`source_type` 和采集日期。当前基础版要求估值和预期数据带有来源、日期、采集时间和字段说明；公开便利源只能作为辅助，不能直接进入自动评分。

下载 SEC companyfacts 原始基本面数据：

```powershell
aits fundamentals list-sec-companies
$env:SEC_USER_AGENT="AITradingSystem wakare_no_kaze@outlook.com"
aits fundamentals download-sec-companyfacts --tickers NVDA,MSFT
aits fundamentals validate-sec-companyfacts --as-of 2026-05-02
aits fundamentals extract-sec-metrics --as-of 2026-05-02
aits fundamentals validate-sec-metrics --as-of 2026-05-02
aits fundamentals download-sec-submissions --tickers NVDA,MSFT
aits fundamentals download-sec-filing-archive --as-of 2026-05-02
aits fundamentals sec-accession-coverage --as-of 2026-05-02
aits fundamentals build-sec-features --as-of 2026-05-02
aits fundamentals fetch-tsm-ir-quarterly --source-url https://investor.tsmc.com/english/quarterly-results/2026/q1 --fiscal-year 2026 --fiscal-period Q1 --as-of 2026-05-02
aits fundamentals extract-tsm-ir-pdf-text --input-path data/external/fundamentals/tsm_ir/2026_q1_management_report.pdf --source-url https://investor.tsmc.com/english/quarterly-results/2026/q1/management-report.pdf --output-path data/external/fundamentals/tsm_ir/2026_q1_management_report.txt --as-of 2026-05-02
aits fundamentals import-tsm-ir-quarterly --input-path data/external/fundamentals/tsm_ir/2026_q1_management_report.txt --source-url https://investor.tsmc.com/english/quarterly-results/2026/q1 --fiscal-year 2026 --fiscal-period Q1 --filed-date 2026-04-16 --as-of 2026-05-02
# 历史季度回填：先按模板准备真实本地文本路径，再运行批量导入。
aits fundamentals import-tsm-ir-quarterly-batch --manifest-path data/external/fundamentals/tsm_ir/tsm_ir_quarterly_manifest.csv --as-of 2026-05-02
aits fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-02
```

该命令读取 `config/sec_companies.yaml` 的 ticker/CIK 映射，下载 SEC EDGAR companyfacts JSON 到 `data/raw/sec_companyfacts/`，并追加写入 `sec_companyfacts_manifest.csv`。校验命令会检查 JSON、CIK、taxonomy 和 checksum。`extract-sec-metrics` 会先执行同一条 SEC 缓存质量门禁，通过后按 `config/fundamental_metrics.yaml` 抽取收入、毛利、营业利润、净利润、研发和 CapEx 等指标，默认输出 `data/processed/sec_fundamentals_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamentals_YYYY-MM-DD.md`。显式派生指标只允许使用配置声明的组件，例如 `gross_profit = revenue - cost_of_revenue`，且必须满足周期、单位、截止日、财年、财期和 accession number 一致。`validate-sec-metrics` 会输出完整缺失观测清单，格式为 `ticker / metric_id / period_type`，便于回测报告按月下钻缺口。`download-sec-submissions` 会下载 `data.sec.gov/submissions` filing history 到 `data/raw/sec_submissions/`；`download-sec-filing-archive` 会按当日 SEC 指标 CSV 实际使用的 accession 下载 accession directory `index.json` 到 `data/raw/sec_filings/`；`sec-accession-coverage` 生成 `outputs/reports/sec_accession_coverage_YYYY-MM-DD.md`，检查 submissions metadata、accepted time 和 archive index checksum 覆盖。第一版不默认下载全部 exhibit 或全历史 filing，下载范围限于当前指标实际用到的 accession。TSM 季度指标可以用 `fetch-tsm-ir-quarterly` 从 TSMC Investor Relations 官方季度页面发现并下载 Management Report 文本；若官方资源是 PDF 或二进制，先用 `extract-tsm-ir-pdf-text` 从本地官方 PDF 的文本层生成可审计文本，再用 `import-tsm-ir-quarterly` 导入本地文本。`filed_date` 代表 Management Report 公开/披露日期，用于历史回测 point-in-time 可见性；历史季度回填可用 `import-tsm-ir-quarterly-batch` 读取 manifest CSV，字段为 `fiscal_year,fiscal_period,source_url,input_path,filed_date`，相对路径按 manifest 所在目录解析；模板在 `docs/examples/fundamentals/tsm_ir_quarterly_manifest_template.csv`；同一批次重复季度、缺文件或非官方 URL 会失败且不写入 CSV。PDF 抽取依赖可选依赖 `pypdf`（安装 `.[data]` 会包含它）；扫描件或无文本层 PDF 会停止并要求 OCR 或人工抽取，不能生成伪文本。TSM IR 默认写入 `data/processed/tsm_ir_quarterly_metrics.csv`、`outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md`、`outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md` 和 `outputs/reports/tsm_ir_quarterly_batch_YYYY-MM-DD.md`；`merge-tsm-ir-sec-metrics`、`score-daily` 和 `aits backtest` 会按评估日或 `signal_date` 选择当时最新已披露 TSM 季度，再把收入、毛利、营业利润、净利、研发和 CapEx 转为 SEC-style 指标行；日报前置计划会在 `validate-sec-metrics` 前显式执行 TSM IR 合并，`score-daily` 也会在本地 TSM IR 缓存存在时自动合并后再校验。金额单位保留 Management Report 披露尺度，例如 `TWD_billions` 或 `USD_billions`。不能用半年度 6-K 拆分替代季度数据。`build-sec-features` 会先复用同一条 SEC 指标 CSV 校验门禁，通过后按 `config/fundamental_features.yaml` 生成毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度，默认输出 `data/processed/sec_fundamental_features_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`。`score-daily` 会复用同一条 SEC 指标校验和特征构建路径，校验失败时停止日报评分；通过后按 `config/scoring_rules.yaml` 的 `fundamentals` 规则使用 AI 核心观察池 SEC 特征中位数进行基本面硬数据评分。

复盘交易记录并做基础归因：

```powershell
aits review-trades --as-of 2026-05-02
```

交易记录默认读取 `data/external/trades/*.yaml`，该目录不提交。可参考 `docs/examples/trades/nvda_trade_template.yaml` 复制模板。交易记录应包含 `recorded_at` 和可选 `updated_at`，用于证明记录在 PIT replay 时点是否可见；缺少 `recorded_at` 会在交易复盘校验中提示警告，严格 replay 不会把缺少可证明记录时间的交易当作可见输入。该命令依赖缓存行情数据，会先执行数据质量门禁，再将交易收益与 `SPY`、`QQQ`、`SMH`、`SOXX` 同区间收益对比，辅助区分市场 Beta、AI 主题 Beta 和个股表现。

## 投资边界

系统输出只作为个人研究和仓位管理辅助，不构成投资建议。所有策略都需要回测、复盘，并显式考虑税费、滑点、汇率、交易延迟和极端风险。
