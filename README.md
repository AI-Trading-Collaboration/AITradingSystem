# AI Trading System

面向美股 AI 产业链的投资认知、趋势分析、风险评分、回测与仓位建议系统。

项目目标不是预测市场，也不是自动交易。当前生产路径只做 AI 产业链趋势判断和投研复核辅助，不实际触发交易；仓位区间、gate 和执行动作都是解释语言，不能直接转成账户买卖。第二套交易执行子系统已启动 paper trading MVP，但默认只模拟成交、记录审计和生成复盘报告，不接真实券商、不读取真实 API key、不提供实盘下单路径。长期方向是可审计认知模型：持续记录 `belief_state`、证据、置信度、风险边界和规则改进建议，但生产规则必须经过回测、shadow mode 和人工批准。

产品定位详见 [docs/product_strategy.md](docs/product_strategy.md)：系统应服务于能力圈、产业链因果、仓位决策和复盘归因，而不是扩张成全市场万能分析器。工程落地拆解见 [docs/implementation_backlog.md](docs/implementation_backlog.md)，具体未完成任务和优先级见 [docs/task_register.md](docs/task_register.md)。

## MVP 范围

第一版只做闭环：

1. 市场价格与宏观风险数据采集。
2. 趋势、相对强弱、波动率、利率等特征计算。
3. 规则评分模型。
4. 仓位区间建议。
5. 与 QQQ、SMH/SOXX、SPY 的回测对比。
6. 每日 Markdown 报告。

SEC 基本面已经接入基础硬数据评分；估值快照和政策/地缘风险发生记录已经接入可审计的手工输入评分，并支持从结构化 CSV 导入来减少手工 YAML 维护。TSMC IR 季度基本面已支持从官方 Management Report 文本或 PDF 可抽取文本层导入，并可显式合并到统一 SEC-style 指标 CSV；LLM claim 预审已支持 OpenAI Responses API 结构化输出和待复核队列，LLM 请求策略默认读取 `config/llm_request_profiles.yaml`，当前 profile 保持 `gpt-5.5`、`requests` HTTP 客户端和单请求失败最多重试 2 次；日报自动风险预审 profile 正在做成本 pilot，`reasoning.effort=medium`，其他手动/通用 profile 仍可保持 `high`；live OpenAI 请求默认写入本地短 TTL request/response 缓存，完全相同 payload 在 TTL 内复用，但只能生成 `llm_extracted` / `pending_review` 线索，不能直接触发交易动作。

Paper trading engine MVP 位于 `src/ai_trading_system/trading_engine/`。它只接受标准 `OrderIntent`，强制执行 `PreTradeRiskChecker -> ExecutionService -> PaperBroker` 链路，默认读取 `config/trading_engine.yaml` 的 paper-only 配置，并把订单意图、风控、订单、成交和组合快照写入 `data/trading_engine/audit/` JSONL。可用下面命令跑通模拟闭环：

```powershell
python scripts/run_paper_trading_demo.py --date 2026-05-17
```

demo 会生成 `reports/trading_daily/2026-05-17.md`。该报告固定为 `production_effect=none`，不是实盘交易指令。

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
config/trading_engine.yaml paper trading engine 风控和执行配置
data/raw/                原始数据缓存，不提交
data/processed/          清洗后的中间数据，不提交
data/external/           外部导入数据，不提交
data/trading_engine/     paper trading 审计日志，不提交
docs/                    架构和开发计划
docs/system_flow.md      数据输入、中间评估和输出结论示意图
docs/learning_path.md    按阅读顺序理解数据、评分、仓位 gate、trace、ledger 和 shadow
docs/calculation_logic.md 面向无金融背景读者解释输入数据如何计算成输出数据
docs/artifact_catalog.md 关键产物的生成者、上游、下游、production_effect 和常见误解
docs/schema/fields.yaml  关键 CSV/JSON 字段的机器可读解释字典
docs/product_strategy.md 产品策略和模块原则
docs/implementation_backlog.md 可落地模块和工程 backlog
docs/task_register.md   未完成任务、优先级、状态和阻塞项登记表
docs/examples/           可复制的输入模板，不包含个人交易记录
notebooks/               研究和临时分析
outputs/backtests/       回测输出，不提交
outputs/reports/         日报/周报输出，不提交
reports/trading_daily/   paper trading demo 日报，不提交
src/ai_trading_system/   应用代码
src/ai_trading_system/trading_engine/ 独立 paper trading 执行子系统
scripts/run_paper_trading_demo.py paper trading MVP demo
tests/                   单元测试
```

## 本地开发

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,data,dashboard,brokers]"
python -m pytest
```

日常开发不要盲等 full pytest。先按改动范围运行分层验证，默认使用 16 worker
并行 pytest（`-n 16 --dist loadfile`），输出中会显示实际 pytest 命令、workers、
distribution 和慢测试耗时：

```powershell
python scripts/run_validation_tier.py --list
python scripts/run_validation_tier.py fast-unit --write-runtime-artifact
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
python scripts/run_validation_tier.py report-validation --write-runtime-artifact
python scripts/run_validation_tier.py integration --write-runtime-artifact
python scripts/run_validation_tier.py slow-research-regression --write-runtime-artifact
python scripts/run_validation_tier.py full --write-runtime-artifact
```

需要复现串行行为时显式加 `--workers 1`；不要把并行失败静默改写成串行 PASS。
`--write-runtime-artifact` 会写出
`outputs/validation_runtime/<run_id>/test_runtime_summary.json` 和
`test_runtime_reader_brief.md`，用于记录 suite、命令、runtime、promotion-blocking
状态和 no-production safety boundary。

`fast-unit` 用于 CLI wiring、轻量 helper、report registry 和 documentation contract
快速反馈；`contract-validation` 是 docs/report/artifact/safety contract 的 promotion-facing
门禁；`report-validation` 覆盖 Reader Brief 和报告导航；`integration` 覆盖 scheduler、
trading_engine 和跨模块集成；`slow-research-regression` 单独承载 Dynamic v3、backtest
simulation 和研究回归。旧命令 alias 仍可用：`fast`、`reader-brief`、`dynamic-v3`
和 `trading-engine` 会分别解析到新的正式 suite。涉及投资解释、data quality、scoring、
backtest、report registry、Reader Brief、broker safety 或跨模块契约的改动，最终仍应跑
对应领域 gate，并在交付前尽量跑 `full`；如果 full 或 slow research suite 因环境上限
超时，不能记为 PASS，需要记录已通过的 scoped suites、超时时间和 top slow tests。

项目主线运行环境固定对齐 Python 3.11：CI 使用 3.11，`pyproject.toml` 的
Ruff/Black/Mypy 目标也是 `py311`。Windows 本机如果裸 `python` 指向 3.12+
或 3.14，应优先使用 `.\.venv\Scripts\python.exe` 或先激活 `.venv` 再运行
pytest、ruff、black、daily-run 和 IBKR Paper read-only snapshot。

本地验证通过并 push 后，还需要把 GitHub Actions CI 当作同一轮验证项：打开 [Actions](https://github.com/AI-Trading-Collaboration/AITradingSystem/actions)，确认本次 commit 触发的最新 run 结束且 `success`。如果 CI 失败，先看失败 job 日志，本地复现并修复后再次提交推送，直到 CI 通过；交付说明应记录通过的 run 号或链接。

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

## ETF 主仓组合配置系统

ETF 主仓组合配置系统位于 `src/ai_trading_system/etf_portfolio/`，配置在
`config/etf_portfolio/`。它围绕 `SPY`、`QQQ`、`SMH`、`SOXX` 和 `CASH`
生成 ETF 特征、信号、市场状态、目标权重、组合级回测、模拟舱记录和
Markdown portfolio brief。默认研究窗口是 `ai_after_chatgpt`，回测默认从
2022-12-01 开始；更早数据只用于 warm-up、压力测试或 regime 对照。

默认入口使用隔离命名空间 `aits etf ...`：

```powershell
aits etf validate-config
aits etf data validate --date latest
aits etf features build
aits etf signals generate --date latest
aits etf regime generate --date latest
aits etf portfolio allocate --date latest
aits etf simulation record --date latest
aits etf simulation evaluate --as-of latest
aits etf simulation report
aits etf report daily --date latest
aits etf run daily --date latest --dry-run
aits etf backtest run --fast
```

为兼容 ETF 开发文档里的短路径示例，根级 CLI 也提供等价 alias：
`aits data ingest/validate`、`aits features build`、`aits signals generate`、
`aits regime generate`、`aits portfolio allocate`、`aits simulation
record/evaluate/report`、`aits report daily`、`aits run daily` 和 `aits experiments
run/compare/register`。根级
`aits backtest` 已属于现有主系统每日评分回测，ETF 回测继续使用
`aits etf backtest run/report`，避免混淆两套投资解释链路。
`aits features build --end latest` 可按价格缓存最新日期构建特征；`aits etf backtest
run --config config/etf_portfolio/backtest.yaml` 可显式指定 ETF backtest policy。
TRADING-064 batch experiments 使用
`aits etf experiments run --pack etf_calibration_v1 --start YYYY-MM-DD --end YYYY-MM-DD`
或 `aits etf experiments run --experiment <experiment_id> --start YYYY-MM-DD --end YYYY-MM-DD`。
输出写入 ignored runtime 目录 `reports/etf_portfolio/experiments/<run_id>/`，包含
`run_manifest.json`、`experiment_results.json`、`benchmark_results.json`、
`metrics_summary.json` 和 `diagnostics_summary.json`。运行后可用
`aits etf experiments compare --run-id <run_id>` / `--latest` 生成 comparison report，
再用 `aits etf experiments select-candidates --run-id <run_id>` / `--latest` 生成
shadow-only candidate selection gate。只有 `eligible_for_shadow` candidate 可以继续用
`aits etf experiments enroll-shadow --run-id <run_id> --candidate <candidate_id>` 或
`--latest --top N` 登记到 `data/simulation/etf_shadow_candidates.json`。登记后可用
`aits etf experiments weekly-review --as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/experiments/weekly_reviews/weekly_review_YYYY-MM-DD.json/md`。
这些 experiment manifest、comparison report、candidate selection report、shadow registry
和 weekly review 已登记到 `config/report_registry.yaml`；`aits reports index` 可发现最新
artifact，Reader Brief 的 `ETF Calibration Experiments` 区块会只读展示 latest pack、top
candidate、rejected/blocked count、active shadow candidates、weekly review action、
safety status 和 detail report。缺失 artifact 时显示 `MISSING`，不会自动运行 experiment、
backtest、shadow enrollment 或 weekly review。
最终门禁使用 `aits etf experiments validate --pack etf_calibration_v1`，输出
`reports/etf_portfolio/experiments/validation/*_experiment_validation.json/md`，验证
registry、pack、runner、comparison/ranking、candidate gate、shadow/weekly review、
report integration、P2/live safety 和 no-production/no-broker 边界。

TRADING-065 forward simulation 使用已登记的 shadow candidates 做真实 forward
observation，不改变 ETF production allocation。`aits etf forward update --date
YYYY-MM-DD` 或 `--latest` 读取 `data/simulation/etf_shadow_candidates.json`、ETF price
cache、baseline allocation 和 `QQQ` / `SPY` / `SMH` benchmark，写入 evaluation-only
forward update 和 `data/simulation/etf_forward_decisions.csv` decision-time ledger；
forward return 字段不会写入 decision rows。`aits etf forward dashboard --latest`
生成 candidate vs baseline vs benchmark dashboard；`aits etf forward weekly-review
--latest` 汇总 rolling metrics、risk/turnover/constraint hit 和 allowed next actions；
`aits etf forward watchlist --latest` 只生成本地 watchlist summary；`aits etf forward
validate` 是 TRADING-065 final gate。所有 forward 输出固定
`observe_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true` 和 `production_promotion_allowed=false`，allowed actions
不包含 production promotion 或 broker action。Daily ops 会在 `aits ops daily-run`
中、report index 和 Reader Brief 之前尝试刷新 forward update / dashboard /
watchlist；Reader Brief 的 `ETF Forward Simulation` 区块只读摘录 report index 指向的
latest artifacts，缺失时显示 no active / missing 状态，不补造 forward 结论。

TRADING-066 AI confirmation overlay 从
`config/etf_portfolio/ai_confirmation_universe.yaml` 读取 universe membership。该
source config 定义 mega-cap AI、semiconductor hardware、cloud AI platform、AI ETF
proxy 和 event-risk reference groups，并区分 required 与 optional symbols。
`config/etf_portfolio/ai_confirmation_policy.yaml` 治理 score bands、MegaCapAIScore
component weights、relative-strength normalization、drawdown penalty、event-risk
adjustment、composite component weights 和 coverage warning floor。TRADING-066
的 breadth feature baseline 可用 `aits etf ai-confirmation features --date YYYY-MM-DD`
生成，输出 `reports/etf_portfolio/ai_confirmation/features/ai_confirmation_features_YYYY-MM-DD.json/csv`。
该命令先执行 ETF price quality gate，只使用 `date <= score_date` 的价格，strict required
AI universe data 缺失时 fail closed。MegaCapAIScore 从 mega-cap breadth、relative
strength vs `QQQ` / `SPY`、drawdown penalty 和 coverage penalty 计算 0-100 score，
AISemiconductorRelativeStrengthScore 从 `QQQ/SPY`、`SMH/QQQ`、`SOXX/QQQ`、
`SMH/SPY`、`SOXX/SPY` 和 optional AI ETF proxy pairs 计算 ETF-level confirmation，
AI event risk overlay 只按 FOMC/CPI/PCE、major AI earnings、semiconductor earnings、
export-control window 等日历事件输出 active/upcoming/recent risk flags，不预测事件方向。
`AIConfirmationScore` composite 将 semiconductor breadth、MegaCapAIScore、
AISemiconductorRelativeStrengthScore 和 event risk adjustment 合成为 0-100 candidate-only
score，并输出 `action_hint`、`reason_codes`、data coverage 和 safety fields。
`aits etf ai-confirmation report --date YYYY-MM-DD` 在 ETF price quality gate 通过后生成
standalone JSON/Markdown report，路径为
`reports/etf_portfolio/ai_confirmation/reports/ai_confirmation_report_YYYY-MM-DD.json/md`，
并已登记到 `config/report_registry.yaml` 供 `aits reports index` 只读发现。
`aits etf ai-confirmation overlay --date YYYY-MM-DD --candidate <candidate_id>` 读取显式
base candidate weights 和 AI confirmation report，输出 bounded
`after_candidate_weights` / `candidate_weights` / `shadow_weights` /
`hypothetical_weights` 到
`reports/etf_portfolio/ai_confirmation/overlays/`，但不写 official ETF target weights。
Reader Brief 的 `AI Confirmation` 区块只读摘录 report index 指向的最新 AI confirmation
report，展示 score、band、component scores、event risk、safety 和 detail report link；缺失或
insufficient data 时显示 no overlay recommendation，不运行上游 scoring。后续 validation gate
`aits etf ai-confirmation validate` 会 fail-closed 检查 universe config、score policy、report、
overlay、Reader Brief、report registry 和 safety fields，并输出
`reports/etf_portfolio/ai_confirmation/validation/ai_confirmation_validation_YYYY-MM-DD.json/md`。
所有 AI confirmation 输出必须固定
`observe_only=true`、`candidate_only=true`、`production_effect=none`、
`broker_action=none`、`manual_review_required=true`；overlay-adjusted weights 只能作为
candidate/shadow/hypothetical weights，不写 official ETF target weights。

TRADING-072 AI confirmation forward attribution review 只验证
`AIConfirmationScore` 与 component scores 是否对未来 ETF / semiconductor / satellite
表现具备 attribution / explanatory value，不改变任何生产权重。`aits etf
ai-attribution build --as-of YYYY-MM-DD` 只读既有
`reports/etf_portfolio/ai_confirmation/reports/ai_confirmation_report_*.json` 和 ETF price
cache，先通过 ETF price quality gate，再生成
`reports/etf_portfolio/ai_attribution/datasets/ai_attribution_dataset_YYYY-MM-DD.json/csv/md`。
每条 row 固定 `score_date`、`forward_window`、`evaluation_as_of_date`、
`evaluation_only=true`、component scores、regime、QQQ/SPY/SMH/SOXX forward returns、
relative returns、forward drawdown/volatility、sample availability、source report path 和
safety fields；forward returns 只能用于 attribution/evaluation。`aits etf
ai-attribution report --as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/ai_attribution/reports/ai_attribution_report_YYYY-MM-DD.json/md`，
包含 score bucket analysis、component-level attribution、regime-conditional attribution、
event-risk attribution、redundancy diagnostics、evidence scorecard、manual review
recommendation 和 source links。Reader Brief 的 `AI Attribution Review` 区块只读摘录
latest attribution report，展示 overall status、best/weak evidence、redundancy、manual
review 和 detail report；缺失时显示 `MISSING`，不运行上游。`aits etf ai-attribution
validate` 输出
`reports/etf_portfolio/ai_attribution/validation/ai_attribution_validation_YYYY-MM-DD.json/md`，
校验 A-J workflow、Reader Brief/report registry visibility、evaluation-only separation 和
safety fields。所有 AI attribution 输出固定 `observe_only=true`、`candidate_only=true`、
`production_effect=none`、`broker_action=none`、`manual_review_required=true`，不得写
production weights、不得自动 promotion、不得触发 broker action。

TRADING-073 satellite replacement forward attribution review 只验证
satellite replacement / fallback decisions 是否相对 ETF-first exposure 具备 forward
attribution value，不改变任何生产权重。`aits etf satellite-attribution build --as-of
YYYY-MM-DD` 只读既有
`reports/etf_portfolio/satellite/reports/satellite_replacement_report_*.json`、可选
AI confirmation reports 和 ETF / satellite price cache，先通过 ETF price quality gate，再生成
`reports/etf_portfolio/satellite_attribution/datasets/satellite_attribution_dataset_YYYY-MM-DD.json/csv/md`。
每条 row 固定 `decision_date`、`eligibility_date`、`replacement_plan_date`、
`forward_window`、`evaluation_as_of_date`、`evaluation_only=true`、ticker、benchmark ETF、
role/group、eligibility status、SatelliteCandidateScore、component scores、fallback flag、
replacement weight、stock-vs-benchmark forward returns、replacement-vs-ETF impact、
drawdown/volatility、event flag、sample availability、source report path 和 safety fields；
forward returns 只能用于 attribution/evaluation。`aits etf satellite-attribution report
--as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/satellite_attribution/reports/satellite_attribution_report_YYYY-MM-DD.json/md`，
包含 eligibility bucket analysis、stock-vs-benchmark attribution、fallback-to-ETF
attribution、score attribution、risk attribution、role/group attribution、AI confirmation
interaction attribution、evidence scorecard、manual review recommendation 和 source links。
Reader Brief 的 `Satellite Attribution Review` 区块只读摘录 latest attribution report，
展示 overall status、eligible/fallback/role/risk evidence、weak evidence、manual review 和
detail report；缺失时显示 `MISSING`，不运行上游。`aits etf satellite-attribution
validate` 输出
`reports/etf_portfolio/satellite_attribution/validation/satellite_attribution_validation_YYYY-MM-DD.json/md`，
校验 A-L workflow、Reader Brief/report registry visibility、evaluation-only separation、
forbidden production/trading output keys 和 safety fields。所有 satellite attribution 输出固定
`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true`，不得写 production weights、不得自动 promotion、不得触发
broker action。

TRADING-064 controlled calibration experiments 从
`config/etf_portfolio/experiments.yaml` 和
`config/etf_portfolio/experiment_packs.yaml` 读取。experiment registry 定义允许观察的
base allocation、regime multiplier、semiconductor cap、rebalance threshold 和
relative strength weight 实验；`etf_calibration_v1` pack 只引用这 16 个受控实验，不做
uncontrolled combinatorial search。`risk_adjusted_v1` ranking policy 的 component weights、
component scales、turnover/drawdown thresholds 和 hard rejection rules 也在 pack config 中治理；
comparison report 只有在该 policy 可用时才输出 candidate scores，且 high-return candidate
仍会被 turnover、drawdown、missing benchmark 或 unsafe safety flags 拒绝。
`shadow_only_manual_review` promotion policy 的 `min_candidate_score`、blocked/rejected hard
rejection 分类和 `production_promotion_allowed=false` 也在同一配置中治理；candidate
selection gate 只输出 `eligible_for_shadow`、`needs_more_data`、`rejected` 或 `blocked`，
不会把 ranking 结果升级为 production change。Shadow enrollment registry 只保存
`shadow_id`、`candidate_id`、`experiment_id`、source run、model/config hash、start date、
status 和 evaluation schedule，路径在 ignored runtime 目录；重复登记同一 candidate
不会追加重复记录。`weekly_shadow_review_v1` review policy 的最小观察天数、longer
observation excess return、drawdown 和 turnover 阈值同样在 pack config 中治理；weekly
review 只能建议 `continue_shadow`、`needs_more_data`、`reject_candidate` 或
`promote_to_longer_observation`，不得输出 production promotion 动作。每个 experiment 和 pack 必须包含 `observe_only=true`、
`production_effect=none`、`broker_action=none` 和 `manual_review_required=true`。新增实验或
pack 时只能使用 registry 支持的 override key，并先通过 loader/测试校验；这些 config 本身
不运行回测、不写正式 target weights、不触发 broker action。

ETF 数据依赖命令会先运行 ETF price quality gate，失败时停止；报告必须披露
`data_quality_status`、质量报告路径、`model_version`、`config_hash`、实际请求日期和
regime 日期范围。主要产物路径：

- `data/etf_portfolio/features.csv`
- `data/etf_portfolio/signals.csv`
- `data/etf_portfolio/regimes.csv`
- `data/etf_portfolio/target_weights.csv`
- `data/simulation/etf_ledger.csv`
- `reports/etf_portfolio/YYYY-MM-DD_portfolio_brief.md`
- `reports/etf_portfolio/backtests/<run_id>/daily.csv`
- `reports/etf_portfolio/backtests/<run_id>/weights.csv`
- `reports/etf_portfolio/backtests/<run_id>/trades.csv`
- `reports/etf_portfolio/backtests/<run_id>/summary.md`
- `reports/etf_portfolio/backtests/<run_id>/summary.json`
- `reports/etf_portfolio/backtests/<run_id>/metrics.json`
- `reports/etf_portfolio/backtests/<run_id>/stability_diagnostics.json`
- `reports/etf_portfolio/backtests/<run_id>/stability_diagnostics.md`
- `reports/etf_portfolio/governance/YYYY-MM-DD_parameter_governance.json`
- `reports/etf_portfolio/governance/YYYY-MM-DD_parameter_governance.md`
- `reports/etf_portfolio/credibility/YYYY-MM-DD_credibility_gate.json`
- `reports/etf_portfolio/credibility/YYYY-MM-DD_credibility_gate.md`
- `reports/etf_portfolio/experiments/<run_id>/run_manifest.json`
- `reports/etf_portfolio/experiments/<run_id>/comparison_report.json`
- `reports/etf_portfolio/experiments/<run_id>/candidate_selection_report.json`
- `data/simulation/etf_shadow_candidates.json`
- `reports/etf_portfolio/experiments/weekly_reviews/weekly_review_YYYY-MM-DD.json`
- `reports/etf_portfolio/experiments/validation/YYYY-MM-DD_etf_calibration_v1_experiment_validation.json`
- `data/simulation/etf_forward_decisions.csv`
- `reports/etf_portfolio/forward/updates/forward_update_YYYY-MM-DD.json`
- `reports/etf_portfolio/forward/dashboard/forward_dashboard_YYYY-MM-DD.json`
- `reports/etf_portfolio/forward/weekly_reviews/weekly_review_YYYY-MM-DD.json`
- `reports/etf_portfolio/forward/watchlist/forward_watchlist_YYYY-MM-DD.json`
- `reports/etf_portfolio/forward/validation/forward_validation_YYYY-MM-DD.json`
- `reports/etf_portfolio/ai_confirmation/features/ai_confirmation_features_YYYY-MM-DD.json`
- `reports/etf_portfolio/ai_confirmation/features/ai_confirmation_features_YYYY-MM-DD.csv`
- `reports/etf_portfolio/satellite/features/satellite_features_YYYY-MM-DD.json`
- `reports/etf_portfolio/satellite/reports/satellite_replacement_report_YYYY-MM-DD.json`
- `reports/etf_portfolio/satellite/experiments/satellite_shadow_experiment_YYYY-MM-DD.json`
- `reports/etf_portfolio/satellite/validation/satellite_validation_YYYY-MM-DD.json`
- `reports/etf_portfolio/weekly_review/aggregation/weekly_review_aggregation_YYYY-MM-DD.json`
- `reports/etf_portfolio/weekly_review/weekly_review_YYYY-MM-DD.json`
- `reports/etf_portfolio/weekly_review/validation/weekly_review_validation_YYYY-MM-DD.json`

`target_weights.csv` 输出 `constraints_applied` 和结构化
`constraint_diagnostics` JSON。正式 allocation 会执行 asset cap/floor、risk group /
semiconductor sleeve cap、regime equity cap、cash floor、`min_rebalance_delta`、
`max_rebalance_trade_weight` 和 `max_daily_turnover`，每条诊断记录包含约束 ID、资产或
sleeve、before/after 权重、原因和 severity。drawdown / volatility penalty 属于 signal
risk score 层，会在 composite score 进入 allocation 前体现，不作为后验仓位修补。
`aits etf report daily` 会把这些 decision-time 输入渲染为可审计解释：顶部 safety
banner 明示 `observe_only=true` / `production_effect=none` / manual-review-only /
no broker action，正文展示 current regime、asset-level scores、target / previous
weights、weight deltas、top positive/negative drivers、constraints applied、benchmark
context、simulation status、P2/live candidate-only note 和 actionability note；附加的
evaluation-only 字段不得进入 decision sections。

ETF no-lookahead timing contract 固定为：raw market data date = `t`、feature snapshot
date = `t`、signal date = `t`、allocation decision date = `t`、最早执行日为 `t` 之后的
下一交易日。`src/ai_trading_system/etf_portfolio/no_lookahead.py` 会校验
`execution_date > signal_date`、`feature_source_date <= signal_date`、decision payload
不得包含 future/evaluation 字段，且 ETF daily brief 的 decision sections 不得泄漏
evaluation-only 字段。

ETF backtest 显式记录 `signal_date < execution_date < return_date`：信号使用 `t`
收盘可见信息，目标权重在下一交易日 close 执行，收益窗口从 execution date 到后续
return date。输出 `asset_returns_json`、`asset_contributions_json`、权重历史、交易 delta、
成本和 benchmark 比较。默认 benchmark registry 为 B001-B008：buy-and-hold `SPY` / `QQQ` / `SMH` /
`SOXX`、`static_growth_balanced`、`static_ai_growth`、`ma_50_200_qqq` 和
`risk_off_cash_switch`。summary / metrics 会输出 `benchmark_comparisons` common schema，
用于审计，不是收益承诺。`config/etf_portfolio/backtest.yaml` 通过
`primary_benchmark_id` 指定标准比较基准；summary / metrics 同时输出
`standardized_metrics`、`monthly_returns` 和 `metric_null_reasons`，覆盖 NAV、total return、
CAGR、volatility、drawdown、Sharpe、Sortino、Calmar、monthly excess return、benchmark
excess return、benchmark drawdown reduction、average equity exposure 和 cash weight。无法计算的
Sharpe / Sortino / Calmar / monthly / benchmark 字段必须保留 null 并说明原因，不得补 0。
Backtest summary 同时输出 `allocation_stability_diagnostics`，
并写出 `stability_diagnostics.json/md`；`aits etf backtest diagnostics --latest` 可从
既有 run 的 `daily.csv` / `weights.csv` 重新生成同一 stability 诊断，覆盖 turnover、
weight delta、regime transition、constraint hit rate、cash/equity/semiconductor exposure
和 asset exposure time。

`aits etf simulation evaluate` 会在 forward window 足够时补充 `forward_return_20d`、
`relative_return_vs_spy_20d`、`relative_return_vs_qqq_20d`、
`weight_contribution_20d` 和组合级 portfolio-vs-benchmark 字段；窗口不足或 benchmark
缺失时保持 null。Simulation ledger 使用 `record_type=decision` / `record_type=evaluation`
分层：decision rows 固定 `evaluation_only=false`、`observe_only=true`、`production_effect=none`
并保留 decision/config/snapshot hash、asset score、target weight、previous weight 和 delta JSON；
evaluation rows 才承载 future/forward return、`evaluation_as_of_date` 和 portfolio-vs-benchmark
outcome，且固定 `evaluation_only=true`。
`aits etf simulation report` 会按 `model_version` 汇总 20d hit rate 和 portfolio vs
SPY/QQQ 表现；`aits etf report daily` 的 Simulation Performance 小节会读取同一 ledger
摘要，并区分 decision records 与 evaluation records。该信息用于人工复核，不构成自动
promotion 或交易指令。

P1 observe-only 入口包括 `aits etf relative-strength report`、`aits etf confirmation
report`、`aits etf satellite evaluate`、`aits etf satellite features`、`aits etf satellite
report`、`aits etf satellite run`、`aits etf satellite experiment`、`aits etf satellite
validate`、`aits etf attribution report`、`aits etf events risk-flag`、`aits etf governance
status`、`aits etf experiments register`、`aits etf experiments run --config <candidate.yaml>`
和 `aits etf experiments compare --baseline production`。P2
observe-only 入口包括 `aits etf p2 edgar-text`、`derive-edgar-events`、
`fetch-edgar-text`、`edgar-topics`、`normalize-news`、`news-themes`、
`derive-options-risk`、`normalize-options-risk`、`options-risk`、`normalize-holdings`、
`holdings-lookthrough`、`advanced-risk`、`walk-forward`、`ml-ranking`、
`weight-optimizer`、`ensemble` 和 `live-preflight`。

TRADING-067 satellite stock replacement policy 从
`config/etf_portfolio/satellite_universe.yaml` 和
`config/etf_portfolio/satellite_policy.yaml` 读取。它把 AI / semiconductor 个股映射到
`QQQ`、`SMH` 或 `SOXX` benchmark ETF，计算 stock-vs-ETF relative strength、趋势、
momentum、drawdown、volatility、coverage 和 AI confirmation support，输出
replacement eligibility gate、candidate-only replacement plan、shadow experiment 和
standalone JSON/Markdown report。默认保持 ETF first；数据不足、gate 未通过或风险约束触发时
输出 `fallback_to_etf=true`。所有 satellite 输出固定 `observe_only=true`、
`candidate_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true`，只允许 `candidate_weights`、`shadow_weights`、
`hypothetical_weights` 或 `replacement_plan`，不写 official ETF target weights。

TRADING-068 weekly portfolio review 把 ETF baseline、experiment/forward shadow
候选、AI confirmation、satellite replacement、risk/watchlist 和 validation gate 汇总为一个
人工周度复核包。`aits etf weekly-review aggregate --as-of YYYY-MM-DD` 只读扫描
report registry / report index 和既有 artifacts，显式记录 loaded/missing sections、
source report paths 和 target weights snapshot；`aits etf weekly-review generate --as-of
YYYY-MM-DD` 生成 `reports/etf_portfolio/weekly_review/weekly_review_YYYY-MM-DD.json/md`；
`aits etf weekly-review validate` 生成 final validation gate。Weekly review 不运行上游
experiment、forward、AI 或 satellite 命令，不补造缺失结论，不写 official target weights，
不自动 promotion/rejection，不触发 broker。所有输出固定 `observe_only=true`、
`candidate_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true`。Reader Brief 的 `Weekly Portfolio Review` 区块只读展示最新
weekly review 的 status、active shadow candidates、AI/satellite/risk/manual action 摘要和
detail report link；缺失时显示 no latest weekly review report found。

TRADING-069 portfolio decision journal 把 TRADING-068 weekly review 的人工结论持久化到
ignored runtime state `data/simulation/etf_portfolio_decision_journal.json`。`aits etf
decision-journal add --weekly-review-path <weekly_review.json> --action-item-id
<weekly-action-id> ...` 会校验 source weekly review、action item、source evidence 和安全字段；
`update/list/remove` 只维护 journal active/removed entries 和 audit trail，remove 会移入
`removed_entries` 而不是静默删除。`report --as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/decision_journal/decision_journal_YYYY-MM-DD.json/md/html`；
`analytics` 输出 review outcome 计数和 confidence/follow-up 摘要；`propose-state-updates`
只生成 candidate state update proposal，不修改 shadow registry；`validate` 检查 weekly review
link、action item、disallowed actions 和 safety fields。所有 journal 输出固定
`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true`，并阻断 `place_order`、`enable_broker_action` 和
`promote_to_production_without_governance`。Reader Brief 的 `Portfolio Decision Journal` 区块只读
摘录 latest journal report 的 entry count、decision status counts、follow-up count、confidence 和
detail report link，不运行 journal CLI。

TRADING-070 parameter review 从 latest forward dashboard、weekly review、decision journal、
experiment comparison、candidate selection 和 validation gate 只读聚合 evidence。`aits etf
parameter-review aggregate --as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/parameter_review/aggregation/parameter_review_evidence_YYYY-MM-DD.json/md`，
显式保留 source report paths、candidate evidence records、missing source warnings 和 safety
banner。缺少 forward dashboard 或 forward candidate rows 时输出 `status=needs_more_data` /
`reason=INSUFFICIENT_FORWARD_EVIDENCE`；缺失可选 source 只作为 warning 或 candidate-level
partial evidence，不补造结论。`aits etf parameter-review report --as-of YYYY-MM-DD` 和 `run`
生成
`reports/etf_portfolio/parameter_review/reports/parameter_review_YYYY-MM-DD.json/md`，
汇总 candidate comparison、decision journal evidence、proposal package、governance scorecard、
manual review requirements、next steps 和 source report links。所有输出固定
`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、
`manual_review_required=true`，不写 official target weights、不改 baseline config、不触发
broker，也不自动 promotion。Reader Brief 的 `ETF Parameter Review` 区块只读摘录 latest
parameter review report 的 status、candidate/proposal counts、main reason、safety posture 和
detail report link；缺失时显示 `MISSING`，不运行 parameter-review CLI。`aits etf
parameter-review validate` 生成
`reports/etf_portfolio/parameter_review/validation/parameter_review_validation_YYYY-MM-DD.json/md`，
确认 schema、aggregation、comparison、journal linker、proposal generator、governance
scorecard、report generator、Reader Brief visibility、source links 和 unsafe action blocking
完整，失败时 fail closed。

TRADING-071 ETF weight calibration 建立 dual-track 初始权重候选流程。`aits etf
weight-calibration search --search etf_initial_weight_search_v1` 先通过 ETF price quality gate；
TRADING-078A 新增 `config/etf_portfolio/weight_calibration_presets.yaml`，可用
`--preset last_2y/last_3y/last_5y/post_2022_bear/ai_cycle_recent/full_available`
解析 historical date range，并把 resolved preset metadata 写入 search payload。Search 再按
`config/etf_portfolio/weight_search.yaml` 的 SPY/QQQ/SMH/SOXX/CASH bounded grid、
candidate cap、objective policy、benchmark set、walk-forward windows 和 regime splits 生成
historical candidate initial weights，输出
`reports/etf_portfolio/weight_calibration/<run_id>/summary.json/md`、`metrics.csv`、
`ranking.json`、`robustness.json` 和
`data/etf_portfolio/weight_calibration/<run_id>/candidate_weight_sets.json/csv`。
TRADING-078B 新增 `aits etf weight-calibration export-top --latest --top N` 或
`--run-id <run_id>`，从 search run 导出 Top-N JSON / CSV / Markdown 到
`reports/etf_portfolio/weight_calibration/top_candidates/`，字段包括 rank、weight set、
weights、historical score、CAGR、drawdown、Sharpe/Sortino/Calmar、turnover、cash /
semiconductor exposure、QQQ benchmark comparison、overfit risk、forward readiness、
blockers/warnings 和固定 safety fields。
TRADING-078C 新增 `aits etf weight-calibration comparison --latest --top N` 或
`--run-id <run_id>`，把 current baseline、Buy & Hold QQQ/SPY/SMH、static portfolio
references 和 Top-N candidates 写成 comparison JSON / CSV / Markdown 到
`reports/etf_portfolio/weight_calibration/comparison/`，缺失 volatility 等 search
payload 未提供指标时保留 null 并写入 `metric_null_reasons`。
TRADING-078D 新增 `aits etf weight-calibration regime-robustness --latest --top N`，
从 search robustness slices 生成 heatmap-ready JSON / CSV / Markdown 到
`reports/etf_portfolio/weight_calibration/regime_robustness/`；matrix 覆盖
`risk_on`、`neutral`、`risk_off`、`growth_leadership`、`semiconductor_leadership`、
`high_volatility` 和 `growth_underperformance`，缺失 regime slice 保留 `MISSING` 和
`confidence_warning`。
TRADING-078E 新增 `aits etf weight-calibration overfit-explain --latest --top N`，
把 Top-N candidates 的 TRADING-071G diagnostics 转成 human-readable JSON / Markdown 到
`reports/etf_portfolio/weight_calibration/overfit_explanations/`，包含 top overfit
reasons、supporting metrics、blocking metrics、manual review note、forward readiness
和固定 safety fields。
TRADING-078F 新增 `aits etf weight-calibration enroll-top --latest --top N` 和
`aits etf weight-calibration enroll --latest --weight-set <weight_set_id>`，只允许
`forward_readiness_status=shadow_ready` 且无 blockers 的 Top-N candidates 写入
`data/etf_portfolio/weight_calibration/forward_enrollments.json`；ledger 保留
`enrollment_id`、`shadow_candidate_id`、source links、warnings 和 safety fields，不写
production weights、baseline config 或 broker state。
TRADING-078G 新增 `aits etf weight-calibration recommendation --latest --top N`，
生成 candidate-only initial weight recommendation JSON / Markdown 到
`reports/etf_portfolio/weight_calibration/recommendations/`，汇总 run metadata、data
range/preset、search constraints、Top-N candidates、benchmark comparison、regime
robustness、overfit explanations、forward readiness、shadow enrollment recommendation、
source artifacts 和 next steps；报告只建议 forward shadow review，不应用权重。
TRADING-078H 在 Reader Brief 新增 `ETF Initial Weight Candidates` 区块，只读 report
index 指向的 latest `etf_initial_weight_recommendation_report`，展示 latest preset、top
candidate、suggested shadow action、overfit risk、best robustness、blocked candidate count、
safety status 和 detail report link；缺失 recommendation report 时显示 `MISSING`，不运行
weight-calibration recommendation、search、enrollment 或任何上游命令。
TRADING-078I 新增 `aits etf weight-calibration usability-validate`，生成
`reports/etf_portfolio/weight_calibration/validation/historical_calibration_usability_validation_*.json/md`。
该 gate 使用 deterministic sample pipeline 校验 presets、bounded search、Top-N export、
comparison table、regime heatmap、overfit explanations、shadow enrollment workflow、
recommendation report、Reader Brief registry visibility 和安全边界；unsafe
`production_effect`、`broker_action`、缺失 `manual_review_required`、unbounded search 或
enrollment production mutation 均 fail closed。
TRADING-079 新增 `aits etf weight-calibration diagnostics --include-robust-packs`，在
Top-N 全部被 `blocked_by_overfit_risk` 等 gate 阻断时运行多 preset 横向诊断。该命令默认比较
`last_2y`、`last_3y`、`last_5y`、`post_2022_bear`、`ai_cycle_recent` 和
`full_available`，可同时运行 `etf_initial_weight_balanced_lower_semiconductor_v1`、
`etf_initial_weight_defensive_growth_v1` 和 `etf_initial_weight_ai_moderate_v1` 三个
bounded robust search pack，输出
`reports/etf_portfolio/weight_calibration/search_diagnostics/historical_weight_search_diagnostics_*.json/md`、
`*_stable_shapes.csv` 和 `*_near_shadow.csv`。报告包含 per-preset Top-N、overfit risk
distribution、shadow-ready count、`cross_preset_stability_score`、`rank_consistency`、
`weight_shape_similarity`、`regime_failure_count`、`near_shadow_candidates`、rescue
suggestions 和 shadow minimum criteria；rescue suggestion 只提示人工复核方向，例如降低
semiconductor cap、提高 cash floor 或扩大历史窗口，不 enroll、不注册、不应用权重。
TRADING-080 新增 `config/etf_portfolio/cache_policy.yaml` 和
`aits etf weight-calibration diagnostics --cache read-write/read-only --no-cache
--force-refresh --workers auto --resume --run-id <run_id> --include-performance-report`。
Diagnostics cache key 使用 deterministic canonical JSON + SHA256，强制纳入
`source_config_hash`、`data_hash`、`engine_version` 和 layer-specific inputs；cache manifest
校验 schema、safety、config/data/engine hash。Read-write 模式会为 price/returns matrix、
candidate backtest、regime robustness 和 diagnostics aggregation 写入 cache；cache miss 的
per-candidate backtest / robustness 计算可通过 `--workers` 并行执行，run manifest 与
performance report 记录 cache hit/miss/write、worker count、resume status 和 slowest step。
CLI 摘要直接输出 price matrix / aggregation cache status 以及 hit/miss/write count。Runtime cache 写入 ignored
`data/cache/weight_calibration/`，performance report 写入
`reports/etf_portfolio/weight_calibration/performance/`。`aits etf weight-calibration
performance-validate` 生成
`reports/etf_portfolio/weight_calibration/validation/weight_calibration_cache_parallel_validation_*.json/md`，
fail closed 校验 cache policy、cache key、manifest、price/returns cache payload、
candidate/regime/aggregation cache behavior、parallel runner、resume manifest、
performance report 和 safety boundary；该 gate 只证明 cache/parallel research workflow
可审计，不代表任何 candidate 可上线。
TRADING-081 新增 `config/etf_portfolio/profiling_policy.yaml`，并扩展
`aits etf weight-calibration diagnostics` 支持 `--profile off/summary/detailed/cprofile`、
`--profile-output <path>` 和 `--profile-top-n N`。默认 `summary` 只记录轻量
step/cache/worker timing；`detailed` 额外写出 deterministic candidate hotspot
JSON/CSV/Markdown；`cprofile` 才生成 `cprofile.stats` 和 top functions JSON/Markdown。
Profiling artifacts 默认写入 ignored
`reports/etf_portfolio/weight_calibration/profiling/<run_id>/`，包含
`profiling_report.json/md`、`candidate_hotspots.*`、cache timing breakdown、parallel
worker timing、vectorization audit、regime mask precomputation assessment 和 optimization
recommendations。`aits etf weight-calibration profiling-validate` fail closed 校验 policy、
profiler、report generator、Reader Brief integration 和固定 safety boundary。该 workflow
只回答 cold run 时间分布，不注册 candidate、不 enroll shadow、不修改 production weights、
baseline config 或 broker state，也不引入 C/C++/Rust/Numba/Polars rewrite。
TRADING-082 新增 `config/etf_portfolio/shadow_ready_review.yaml` 和
`aits etf shadow-review package --latest --top N` / `approve` / `enroll-approved` /
`validate`。Package 只读 TRADING-079 diagnostics JSON、stable shapes CSV 和 near-shadow
CSV，把 `shadow_ready` observations 按 weight shape 聚合、排序并生成 owner review package；
`approve` 只捕获 owner decision；`enroll-approved` 只有在 owner decision 为
`approved_for_shadow` 且 review package / shape / selected weight set 可追溯时才生成
forward tracking link。Review package、approval、enrollment 和 validation artifacts 写入
ignored `reports/etf_portfolio/shadow_ready_review/`；Reader Brief 的
`Shadow Candidate Review` 区块只读 latest artifacts，展示 top candidate、pending review、
approved enrollment、latest decision 和 safety。该 workflow 不写 production weights、不改
baseline config、不自动 promotion、不触发 broker，也不允许未审批 auto-enrollment。
TRADING-083 新增 `config/etf_portfolio/trend_calibration.yaml` 和
`aits etf trend-calibration run --start YYYY-MM-DD --end YYYY-MM-DD` / `report --latest` /
`validate`。Run 命令先执行 `aits validate-data` 等价 cached market / macro data quality
gate，随后生成 ignored
`reports/etf_portfolio/trend_calibration/datasets/`、`reports/`、`registry/` 和
`validation/` artifacts，用于 two-layer dynamic ETF allocation roadmap 的 Layer 1：
trend-analysis information weight calibration。Report 展示 top candidate signal configs、
score bucket forward attribution、redundancy risk、regime stability、data quality status、
`ai_after_chatgpt` regime 和 source links；Reader Brief 的 `Trend Signal Calibration`
区块只读 latest report。Forward return / drawdown 字段固定 `evaluation_only=true`，只能
用于 attribution/evaluation；该 workflow 不输出 ETF target weights、不写 baseline config、
不自动 promotion、不触发 broker。
TRADING-084 新增 `config/etf_portfolio/dynamic_allocation_policy.yaml` 和
`aits etf dynamic-allocation decide --date YYYY-MM-DD` / `report --latest` / `validate`。
Decision engine 按配置化 base weights、regime targets、trend overlays、event-risk cash
overlay、exposure constraints 和 rebalance gates，把 Layer 1 trend/regime/risk scores
映射为 candidate-only dynamic allocation decision records。Artifacts 写入 ignored
`reports/etf_portfolio/dynamic_allocation/decisions/`、`reports/`、`registry/` 和
`validation/`；Reader Brief 的 `Dynamic Allocation Candidate` 区块只读 latest report。
本阶段可输出 `candidate_target_weights` 供人工复核和后续 TRADING-085/086 使用，但不写
official `data/etf_portfolio/target_weights.csv`、不改 baseline config、不自动 promotion、
不触发 broker。
TRADING-085 新增 `config/etf_portfolio/dynamic_calibration.yaml` 和
`aits etf dynamic-calibration run --pack dynamic_etf_v1 --cache read-write --workers auto` /
`report --latest` / `validate`。Batch runner 组合 TRADING-083 trend signal configs 与
TRADING-084 dynamic allocation policy profiles，生成 two-layer candidate packs、trend score
cache、allocation path cache、dynamic calibration proxy cache、ranking components 和 batch
report。Artifacts 写入 ignored `reports/etf_portfolio/dynamic_calibration/` 和
`data/cache/dynamic_calibration/`；Reader Brief 的 `Dynamic Calibration Batch` 区块只读
latest report。TRADING-085 的 `dynamic_backtest` cache 是 calibration proxy，用于筛选
TRADING-086 输入；完整 dynamic robustness、walk-forward、false-signal diagnostics 仍属于
TRADING-086。该 workflow 不写 official target weights、不改 baseline config、不自动
promotion、不触发 broker、不做未经 owner approval 的 enrollment。
TRADING-086 新增 `config/etf_portfolio/dynamic_robustness.yaml` 和
`aits etf dynamic-robustness report --candidate <candidate_id>` / `report --latest` /
`validate`。Report 命令先执行 `aits validate-data` 等价 cached market / macro data
quality gate，随后用真实 ETF price return 构造 daily dynamic allocation path，并比较
dynamic candidate、static base candidate、current ETF baseline、QQQ/SPY/SMH buy-and-hold
和 best static historical candidate。报告输出 CAGR、total return、max drawdown、
volatility、Sharpe、Sortino、Calmar、turnover、upside/downside capture、risk-off
drawdown reduction、false risk-off opportunity cost、false risk-on drawdown cost、
walk-forward robustness、regime attribution、turnover sensitivity、AI/semiconductor
contribution、event risk overlay attribution 和 overfit diagnostics。Artifacts 写入 ignored
`reports/etf_portfolio/dynamic_robustness/`；Reader Brief 的 `Dynamic Robustness Review`
区块只读 latest report。该 workflow 是 TRADING-087 owner-approved dynamic shadow review
前置证据，不写 official target weights、不改 baseline config、不自动 promotion、不触发
broker、不做 shadow enrollment。
TRADING-087 新增 `config/etf_portfolio/dynamic_shadow.yaml` 和
`aits etf dynamic-shadow package --latest --top 3` / `approve` / `enroll-approved` /
`update` / `weekly-review` / `validate`。Package 只读 TRADING-086 robustness、
TRADING-085 calibration、validation 和 operations artifacts，生成 owner review package；
approval 必须记录 owner rationale 和 decision journal link；enrollment 只接受
`approved_for_dynamic_shadow` 且无 hard blocker 的 approval。`update` 先执行
`aits validate-data` 等价 cached market / macro data quality gate，再生成 dynamic/static/
current/QQQ/SPY/SMH forward tracking metrics；`weekly-review` 汇总 active_shadow、
needs_more_data、watch、reject_pending_review、rejected 和 archived。Artifacts 写入 ignored
`reports/etf_portfolio/dynamic_shadow/` 和
`data/simulation/etf_dynamic_shadow_candidates.json`；Reader Brief 的
`Dynamic Shadow Review` 区块只读 latest artifacts，Strategy Evidence Dashboard 只读 latest
weekly review。该 workflow 不写 official target weights、不改 baseline config、不自动
promotion、不触发 broker、不允许未经 owner approval 的 enrollment。
TRADING-088 新增 `config/etf_portfolio/dynamic_failure_diagnostics.yaml` 和
`aits etf dynamic-rescue run --base-candidate <candidate_id>` / `--latest-failed-package` /
`report --latest` / `validate`。`run` 先执行 `aits validate-data` 等价 cached market /
macro data quality gate，再读取 failed dynamic robustness report 和 optional dynamic shadow
package，生成 failure diagnostics dataset、Layer 1 trend signal failure attribution、false
risk-off/on attribution、Layer 2 allocation underperformance attribution、turnover /
constraint hit breakdown、v0.2-v0.5 bounded rescue templates、rescue candidate comparison 和
evaluation report。Artifacts 写入 ignored `reports/etf_portfolio/dynamic_rescue/`；Reader
Brief 的 `Dynamic Strategy Rescue` 区块只读 latest report。该 workflow 只生成
candidate-only rescue evidence，不修改 `dynamic_allocation_policy.yaml` v0.1 baseline、
不写 official target weights、不改 production baseline、不自动 approval、不自动 enrollment、
不触发 broker；`rescue_success_candidate_found` 只表示可进入后续 TRADING-089 深度复核。
TRADING-089 新增 `config/etf_portfolio/dynamic_v2_review.yaml` 和
`aits etf dynamic-v2-review package --latest-rescue-report` / `report --latest` /
`validate`。Package 只读 TRADING-088 rescue evaluation、v0.4 candidate robustness report
和 optional dynamic shadow package，生成 Dynamic v0.2 review-only 复核包：识别
`dynamic_regime_overlay_v0_4_lower_turnover` 的 false risk-off、turnover 和 static gap
改善，同时把 constraint hit worsening 和 negative drawdown preservation 保留为 hard
blockers。Artifacts 写入 ignored `reports/etf_portfolio/dynamic_v2_review/`；Reader
Brief 的 `Dynamic v0.2 Review` 区块只读 latest package。该 workflow 只给 owner
人工复核使用，固定 `review_candidate / not_shadow_ready`，不生成 approval、不 enroll
shadow、不写 official target weights、不修改 baseline 或 production state、不触发 broker。
TRADING-090 新增 `config/etf_portfolio/dynamic_v3_constraint_aware_rescue.yaml` 和
`aits etf dynamic-v3-rescue run --latest-v2-review` / `report --latest` / `validate`。
`run` 只读 TRADING-089 v0.4 review package，加载 constraint / drawdown root cause，生成
pre-constraint normalization、soft constraint penalty/smoothing、drawdown guardrail、
emergency risk-off 和 v0.3a-v0.3d candidate template comparison。Artifacts 写入 ignored
`reports/etf_portfolio/dynamic_v3_rescue/`；Reader Brief 的 `Dynamic v0.3 Rescue`
区块只读 latest report。该 workflow 只能给 owner 复核 v0.3 constraint-aware rescue
candidate，固定 `review_candidate / not_shadow_ready`，不生成 approval、不 enroll shadow、
不写 official target weights、不修改 baseline 或 production state、不触发 broker。
TRADING-091 新增 `config/etf_portfolio/dynamic_v3_real_evaluation.yaml` 和
`aits etf dynamic-v3-rescue real-evaluate` / `real-report --latest` /
`validate-real`。`real-evaluate` 在读取 cached price / macro data 前先执行 `aits
validate-data` 等价质量门禁，通过后把 TRADING-090 v0.3a-v0.3d templates
materialize 为 in-memory DynamicAllocationPolicyConfig，并复用 TRADING-086
price-driven robustness path 评估 v0.3 vs baseline / v0.2 / v0.4 / static base /
current ETF baseline / QQQ / SPY / SMH。报告输出 constraint hit、false risk-off、
drawdown preservation、turnover、static gap 和 overfit / market-window concentration
综合分析，并给出 `promote_candidate` / `observe_only` / `reject` promotion gate
判定。该判定只是人工复核候选资格，不自动 approval、不 enroll shadow、不写 official
target weights、不修改 baseline 或 production state、不触发 broker；Reader Brief 的
`Dynamic v0.3 Real Evaluation` 区块只读 latest real evaluation report。
TRADING-092 新增 `config/etf_portfolio/dynamic_v3_failure_attribution.yaml` 和
`aits etf dynamic-v3-rescue failure-attribution` / `failure-attribution-report --latest` /
`validate-attribution`。`failure-attribution` 只读 TRADING-091 latest real evaluation
reject 结果，先执行 `aits validate-data` 等价质量门禁，再用相同 source configs 和真实
ETF price cache 重建 v0.3 / v0.4 robustness daily paths，用于输出 v0.3 rejection
attribution、v0.3 vs v0.4 metric delta、constraint hit reason/regime/ticker/rebalance
window bucket、drawdown degradation attribution、overfit `REVIEW_REQUIRED` 解释、v0.4
promotion review 和 v0.5 design recommendation。当前真实 latest 结论为
`v0_4_promotion_review=observe_v0_4_with_constraint_guard`、
`v0_5_design_recommendation=recommend_v0_5_constraint_guard`：v0.4 不能直接 promote，
但应优先保留 v0.4 exposure path 并设计单独 constraint guard。该判定仍只是人工复核
证据，不自动 approval、不 enroll shadow、不写 official target weights、不修改 baseline
或 production state、不触发 broker；Reader Brief 的 `Dynamic v0.3 Failure Attribution`
区块只读 latest attribution report。
TRADING-093 到 TRADING-110 新增
`config/etf_portfolio/dynamic_v3_rescue/parameter_sweep_v1.yaml`、
`parameter_sweep_real_smoke.yaml`、`parameter_sweep_profiles.yaml`、
`parameter_governance_v1.yaml` 和完整真实参数研究闭环：`data-audit run/report`、
`sweep-config validate/preview`、`sweep profile-list/profile-validate/run-profile`、
`sweep run/status/validate/leaderboard/report`、`injection-audit run/report`、
`candidate report/attribution`、`walk-forward run/report/select-run/selection-report`、
`robustness run/report`、`overfit run/report`、`governance validate/report/diff`、
`research index-build/query/compare/history`、`shadow register/list/report/monitor-run/monitor-report`、
`artifacts latest/validate/repair-latest/stale`、`promotion review/pack` 和对应 validate 命令。
Runtime artifacts 写入 ignored `reports/etf_portfolio/dynamic_v3_rescue/`，observe-only
registry 写入 `registry/etf_portfolio/dynamic_v3_rescue_shadow_candidates.yaml`。该平台先
data audit 与 parameter governance gate，再 hard gate、soft ranking、candidate attribution、
true walk-forward selection、overfit review、shadow monitor 和 promotion review pack。
默认 CI / focused tests 继续使用 `tiny_fixture` profile；manual research run 使用
`small_real` / `medium_real` / `overnight_real` profile 接入 TRADING-091 price-driven real
evaluation。Real mode 会先运行 cached data quality gate，每个 candidate 写入独立 real
evaluation artifact，并在 `candidate_results.jsonl` 披露 evaluator mode/version、real
artifact path、data quality、metrics source 和 `search_space_version`；tiny fixture mode
明确 `not_for_investment_decision=true`，只验证 artifact contract，不等于真实 promotion
evidence。所有输出固定 `production_effect=none`、`broker_action=none`、
`production_candidate_generated=false`；tiny promotion pack 不允许进入 `promote_candidate`，
real leaderboard 最多输出 `promote_candidate + manual_review_required`，overfit `HIGH_RISK`
必须 fail closed，PBO / DSR placeholder 只能是 `REVIEW_REQUIRED`，不得生成
`production_candidate`、approval、shadow enrollment、baseline mutation、official target
weights mutation 或 broker action。Reader Brief 的 `Dynamic Rescue Parameter Sweep` 区块
只读 latest sweep leaderboard / promotion pack / shadow monitor，并显示 evaluator mode、
fixture-only 限制和 observe-only monitor 状态。
TRADING-111 到 TRADING-113 为该真实研究闭环补齐证据完整性门禁：`data-provenance
inspect-price-cache/repair-price-manifest/validate` 显式报告 price cache checksum、
download manifest checksum coverage 和 `RECONSTRUCTED_MANIFEST` provenance 状态；
`window-audit run/report/inspect-artifact` 与 `validate-window-audit` 汇总 configured /
requested / actual backtest window，阻断截断窗口被解释为完整 AI regime 结论；real
evaluator sweep artifact 额外导出 `weight_path_metadata.json`、`daily_weights.csv`、
rebalance / constraint / rescue / turnover path CSV，`weight-path validate/report` 和
candidate attribution 使用这些路径把 explainability 标为 `COMPLETE`、`PARTIAL` 或
`INCOMPLETE`。Promotion pack 现在写出 `evidence_summary.json`，并在 window、weight
path、candidate attribution 或 data provenance 不完整时阻断 `promote_candidate`；Reader
Brief 只读展示这些 evidence status，不补跑上游审计。
TRADING-114 到 TRADING-120 把 `medium_real` 候选发现整理成 owner-readable 研究包：
`evidence-summary run/report` 汇总候选级 data/window/weight/attribution/overfit/provenance
证据，`medium-real report` 记录 300 candidate 运行、reject 分布、runtime 和 artifact size，
`regime-coverage run/report` 检查 AI bull、drawdown、semiconductor stress、sideways /
high-vol / recovery 覆盖缺口，`candidate interpretation-pack` 为 top candidate 输出参数、
weight path、turnover、drawdown protection 和 regime behavior，`observe-pool build/report`
只写 observe-only pool artifact，默认不同步 shadow registry，`overnight-readiness run/report`
只评估 runtime / disk / failure rate / evidence completeness，不启动 `overnight_real`，
`research-decision run/report` 汇总 evidence、regime、interpretation、observe pool 和 readiness
并给出下一步 Codex task。所有新增输出固定 `production_effect=none`、`broker_action=none`、
`manual_review_required=true`，Reader Brief 只读 latest evidence summary、observe pool、
overnight readiness 和 research decision，不运行上游、不生成 approval、shadow enrollment、
production candidate、baseline mutation、official target weights mutation 或 broker action。
TRADING-121 到 TRADING-125 继续处理 medium_real “0 usable candidates” 的证据门禁问题：
`evidence-diagnosis run/report` 把 blocker 分为 true hard failure、soft/manual-review blocker
和 warning，`gate-impact run/report` 在不修改原始 sweep 的前提下模拟修复/降级场景，
`gate-policy validate/report/apply` 使用
`config/etf_portfolio/dynamic_v3_rescue/evidence_gate_policy_v1.yaml` 记录 owner-reviewed
hard/soft gate policy，`candidate-recovery run/report` 只生成 observe-only/manual-review
recovered candidates，`observe-pool rebuild --recovery-id` 重建人工观察池，
`research-decision update/update-report` 输出 go/no-go matrix 和下一步 owner action。该链路只允许把 policy 明确
允许的 soft blockers 恢复为 observe-only/manual-review；true hard failures 仍 fail closed。
所有输出固定 `production_effect=none`、`broker_action=none`、
`manual_review_required=true`、`production_candidate_generated=false`，Reader Brief 只读 latest
diagnosis、impact、policy、recovery、observe pool 和 decision update，不运行上游、不自动启动
`overnight_real`、不 approval、不 enroll shadow、不写 official target weights、不修改 baseline
或 production state、不触发 broker。
TRADING-126 到 TRADING-130 把 rebuilt observe pool 压缩为可人工复核的 shadow shortlist，并增加
position advisory / owner review 前置层：`shortlist build/report` 从 observe pool 选择 5-20 个
候选并保留 hard-fail exclusions、score breakdown 和 selection reasons；`candidate-cluster run/report`
输出 parameter / daily weight path / metric similarity matrices、clusters 和 representatives，daily
weight path 缺失时只标 `INCOMPLETE`；`shadow-shortlist build/report` 只把 representatives 写成
monitoring pack，不写 shadow registry；`position-advisory run/report` 读取
`config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`，没有 current portfolio snapshot
时输出 `TARGET_ONLY`，有 snapshot 时输出 candidate target weights、consensus target weights 和
position deltas；`position-review pack/report` 汇总 shortlist、cluster、shadow shortlist 和 advisory，
输出 owner checklist、go/no-go decision 和 Reader Brief section。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/shortlist|candidate_cluster|shadow_shortlist|position_advisory|position_review/`，
Reader Brief 只读 latest artifacts，report registry 已登记对应 report id。所有输出继续固定
`production_effect=none`、`broker_action=none`、`broker_action_allowed=false`、
`owner_approval_required=true`、`production_candidate_generated=false`、`production_readiness=NOT_READY`；
position advisory 不是交易指令，不写 official target weights、不修改 baseline/production state、不触发 broker。
TRADING-131 到 TRADING-135 把上述一次性 shadow shortlist / position review pack 接入持续
manual-review observation：`shadow-monitor activate/run/report` 从 shadow shortlist 生成 daily target
weights、weekly summary、promotion clock 和 recommendation；`portfolio-snapshot validate/report/normalize`
只接受 manual YAML snapshot，并 fail closed 校验权重和值一致性、重复 symbol、负值、currency、
`as_of` 和 `metadata.broker_imported=false`；`position-advisory daily-run/daily-report` 基于 monitor run
输出 `TARGET_ONLY` 或 `SNAPSHOT_DELTA` daily advisory；`consensus-drift run/report` 计算 symbol
dispersion、pairwise disagreement、risk/cash/defensive exposure disagreement，`HIGH_DISAGREEMENT`
强制 daily advisory `manual_review`；`owner-review create/list/report/record-decision` 记录 owner
decision 和 paper-only action。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/shadow_monitor_runs|portfolio_snapshot|position_advisory_daily|consensus_drift|owner_review_journal/`，
Reader Brief 只读 latest monitor/advisory/drift/owner review artifacts。所有输出继续固定
`production_effect=none`、`broker_action=none`、`broker_action_allowed=false`、
`broker_action_taken=false`、`owner_approval_required=true` 和 `manual_review_required=true`；
manual snapshot 不是 broker import，daily advisory 不是 order ticket，owner review journal 也不写
official target weights、baseline/production state 或 broker state。
TRADING-136 到 TRADING-140 新增
`config/etf_portfolio/dynamic_v3_rescue/paper_portfolio_v1.yaml` 和
`src/ai_trading_system/etf_portfolio/dynamic_v3_paper_tracking.py`，把 daily advisory /
owner review journal 延伸为 paper portfolio tracking、advisory outcome evaluation、
owner attribution、shadow aging v2 和 weekly advisory review。新增 CLI 包括
`paper-portfolio init/apply-review/state/report`、`advisory-outcome track/update/report`、
`owner-attribution run/report`、`shadow-aging run/report`、`weekly-advisory-review run/report`
及 `validate-paper-portfolio`、`validate-advisory-outcome`、`validate-owner-attribution`、
`validate-shadow-aging`、`validate-weekly-advisory-review`。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/paper_portfolio|advisory_outcome|owner_attribution|shadow_aging|weekly_advisory_review/`，
Reader Brief 只读 latest paper/outcome/attribution/aging/weekly review artifacts。`advisory-outcome update`
在真实 as-of 日期内先运行 cached market / macro data quality gate；未到期窗口保持
`PENDING`，数据不足保持 `INSUFFICIENT_DATA`。Paper portfolio 只用 action ledger
重建纸面状态，不是真实 portfolio 或 broker import；所有输出继续固定
`production_effect=none`、`broker_action=none`、`broker_action_allowed=false`、
`broker_action_taken=false`、`production_candidate_generated=false`，eligible shadow
candidate 也只能进入人工 promotion review。
TRADING-141 到 TRADING-145 新增
`src/ai_trading_system/etf_portfolio/dynamic_v3_historical_replay.py`，把已存在的
historical daily advisory artifacts 转成 PIT-safe replay inventory、historical replay
variants、backfilled outcome evaluation、historical paper simulation 和 replay performance
review。新增 CLI 包括 `replay-inventory build/report`、`historical-replay run/report`、
`backfill-outcome run/report`、`historical-paper-sim run/report`、
`replay-performance-review run/report`，以及 `validate-replay-inventory`、
`validate-historical-replay`、`validate-backfill-outcome`、
`validate-historical-paper-sim`、`validate-replay-performance-review`。新增 artifacts
位于
`reports/etf_portfolio/dynamic_v3_rescue/replay_inventory|historical_replay|backfilled_outcome|historical_paper_sim|replay_performance_review/`，
Reader Brief 只读 latest replay/backfill/sim/performance artifacts。`backfill-outcome run`
默认先运行 cached market / macro data quality gate；historical replay 默认排除
`PIT_UNSAFE`，`PIT_WARNING` 只有显式 `--include-pit-warning` 才进入 replay。
`ADVISORY_GENERATED_AFTER_AS_OF_DATE`、`MISSING_PRICE_DATA` 和缺少 target weights
属于 hard PIT limitations，会归入 `PIT_UNSAFE` / `INELIGIBLE`，即使显式
`--include-pit-warning` 也不得进入 replay。该链路只做
历史 outcome 复核和 calibration recommendation artifact，不写 config、不生成 production
candidate、不自动 promotion、不改真实组合或 official target weights、不触发 broker。
TRADING-146 到 TRADING-150 在上述 artifacts 上新增 replay result diagnosis 和
manual-only advisory rule calibration。新增 CLI 包括 `replay-diagnosis run/report`、
`backfill-repair run/report`、`variant-comparison run/report`、`rule-calibration run/report`、
`replay-forward-bridge run/report`，以及 `validate-replay-diagnosis`、
`validate-backfill-repair`、`validate-variant-comparison`、`validate-rule-calibration` 和
`validate-replay-forward-bridge`。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/replay_diagnosis|backfill_repair|variant_comparison|rule_calibration|replay_forward_bridge/`。
`replay-diagnosis` 解释 `PARTIAL` / `PENDING` 原因；`backfill-repair` 只在当前 price cache
已有足够历史价格时把 due window 重算为 `AVAILABLE`，不覆写原 backfill；`variant-comparison`
比较 `no_trade`、`consensus_target`、`limited_adjustment`、`owner_decision`、`paper_action`；
`rule-calibration` 只输出 owner approval required proposals，`auto_apply=false`；`replay-forward-bridge`
把 replay 结论接回 forward tracking、weekly review 和 Reader Brief。该阶段继续固定
`production_effect=none`、`broker_action_allowed=false`，不自动修改
`position_advisory_v1.yaml`、official target weights、paper/real portfolio、baseline/production state
或 broker。
TRADING-151 到 TRADING-155 继续补齐 forward outcome accumulation 和 replay sample
expansion。新增 CLI 包括 `outcome-due scan/report/update-ready`、
`replay-sample-expansion run/report`、`outcome-dashboard build/report`、
`limited-vs-notrade run/report`、`consensus-risk run/report`，以及
`validate-outcome-due`、`validate-replay-sample-expansion`、
`validate-outcome-dashboard`、`validate-limited-vs-notrade` 和
`validate-consensus-risk`。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/outcome_due|replay_sample_expansion|outcome_dashboard|limited_vs_notrade|consensus_risk/`。
`outcome-due scan` 必须先通过 cached data quality gate；`replay-sample-expansion`
保留 PIT classification 并排除 `PIT_UNSAFE`；`outcome-dashboard` 只聚合
forward / historical / simulation outcome availability 和 pending reasons，Reader Brief
只读该 dashboard；`limited-vs-notrade` 只在样本足够时比较 `limited_adjustment`
与 `no_trade`；`consensus-risk` 只输出 consensus exposure、drawdown 和 turnover
risk review，不生成 default consensus execution。该阶段继续固定
`production_effect=none`、`broker_action_allowed=false`、`broker_action_taken=false`、
`production_candidate_generated=false`、`automatic_candidate_promotion=false`，不修改
policy、official target weights、paper/real portfolio、baseline/production state 或 broker。
TRADING-156 到 TRADING-160 把 due outcome 从“可更新清单”推进为可审计的滚动证据闭环。
新增 CLI 包括 `outcome-update-review run/report`、`outcome-update run/report`、
`rolling-evidence-refresh run/report`、`evidence-trend run/report`、
`forward-outcome-decision run/report`，以及 `validate-outcome-update-review`、
`validate-outcome-update`、`validate-rolling-evidence-refresh`、`validate-evidence-trend`
和 `validate-forward-outcome-decision`。新增 artifacts 位于
`reports/etf_portfolio/dynamic_v3_rescue/outcome_update_review|outcome_update|rolling_evidence_refresh|evidence_trend|forward_outcome_decision/`。
`outcome-update-review` 在执行更新前生成人工复核包，检查 price availability、
future-data decision safety 和 downstream impact；`outcome-update` 只保留
`READY_TO_UPDATE` window 的实际 mutation，并把 NOT_DUE / PRICE_MISSING / review-blocked
window 写入 skipped audit；`rolling-evidence-refresh` 刷新 outcome dashboard、
limited-vs-notrade、consensus-risk、owner-attribution、shadow-aging、weekly review 和
Reader Brief section；`evidence-trend` 在历史不足时固定 `INSUFFICIENT_HISTORY`；
`forward-outcome-decision` 生成 weekly go/no-go matrix、next actions 和 Reader Brief
`Dynamic Rescue Forward Outcome Decision` 区块。该阶段继续固定 no broker、no production、
no owner auto approval、no official target weight mutation、no real portfolio mutation、
no baseline mutation 和 no policy auto apply。
TRADING-161 到 TRADING-168 新增 `backtest-sim` historical simulation advisory evaluation。
配置入口为
`config/etf_portfolio/dynamic_v3_rescue/backtest_simulation_advisory_v1.yaml`，
CLI 包括 `backtest-sim config-validate`、`event-generate/report`、
`variants-generate/report`、`outcome-run/report`、`paper-run/report`、
`regime-review/report`、`sensitivity-run/report`、`calibration-pack/report` 和
`forward-bridge/report`，验证入口为 `validate-backtest-sim-events`、
`validate-backtest-sim-variants`、`validate-backtest-sim-outcome`、
`validate-backtest-sim-paper`、`validate-backtest-sim-regime`、
`validate-backtest-sim-sensitivity`、`validate-backtest-sim-calibration` 和
`validate-backtest-sim-forward-bridge`。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_events|backtest_sim_variants|backtest_sim_outcome|backtest_sim_paper|backtest_sim_regime|backtest_sim_sensitivity|backtest_sim_calibration|backtest_sim_forward_bridge/`。
所有输出固定 `outcome_mode=BACKTEST_SIMULATION`、
`pit_safety_status=SIMULATION_NOT_PIT`、`not_for_production=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`auto_policy_apply=false` 和
`production_effect=none`。该链路评估 current shadow shortlist/manual advisory 规则，
不是 PIT replay，不是 production evidence；calibration pack 只生成 owner review
proposal，forward bridge 只生成 confirmation targets 和 Reader Brief section。
TRADING-169 到 TRADING-173 在上述 simulation 结果之上新增 interpretation / advisory
review 层。CLI 包括 `sim-interpretation run/report`、`sim-risk-return run/report`、
`sim-defensive-validation run/report`、`advisory-proposal-review run/report` 和
`forward-confirmation-plan run/report`，验证入口为 `validate-sim-interpretation`、
`validate-sim-risk-return`、`validate-sim-defensive-validation`、
`validate-advisory-proposal-review` 和 `validate-forward-confirmation-plan`。Artifacts
写入
`reports/etf_portfolio/dynamic_v3_rescue/sim_interpretation|sim_risk_return|sim_defensive_validation|advisory_proposal_review|forward_confirmation_plan/`。
Interpretation pack 解释每个 variant 的 role、return/risk profile 和 recommended
usage；risk-return review 把 limited / defensive / consensus 相对 `no_trade` 的收益、
drawdown 和 turnover 代价拆开；defensive validation 明确
`defensive_limited_adjustment` 不能因 overall best 自动视为防守有效；proposal review
固定 `auto_apply=false`、`owner_approval_required=true`；forward confirmation plan
只生成后续 tracking 的 success / failure 条件。该阶段继续固定
`BACKTEST_SIMULATION_NOT_PIT`、`production_effect=none`、no broker、no production、no
official target weight mutation、no `position_advisory_v1.yaml` mutation 和 no policy auto
apply。
TRADING-174_to_178_FORWARD_CONFIRMATION_CYCLE 把上述 static
`forward_confirmation_plan` 升级为持续执行闭环。CLI 包括
`confirmation-targets register/list/report`、`confirmation-progress update/report`、
`confirmation-evaluate run/report`、`rule-review-cycle run/report` 和
`rule-owner-decision create/list/record/report`；验证入口为
`validate-confirmation-targets`、`validate-confirmation-progress`、
`validate-confirmation-evaluate`、`validate-rule-review-cycle` 和
`validate-rule-owner-decision`。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/forward_confirmation_registry|confirmation_progress|confirmation_evaluation|rule_review_cycle|rule_owner_decision/`，
并同步生成 reviewable
`registry/etf_portfolio/dynamic_v3_rescue_forward_confirmation_targets.yaml`。Progress
tracker 会读取 latest `limited-vs-notrade` 和 `consensus-risk` evidence；缺少样本、窗口或
pressure-regime 标签时必须保持 `INSUFFICIENT_EVENTS` / `NOT_READY`，不得伪造
READY。Rule review cycle 默认 `policy_change_allowed=false`；owner decision journal
只记录人工决策，`approve_manual_policy_review` 也不自动修改配置。该闭环继续固定
`auto_apply=false`、`broker_action_allowed=false`、`production_effect=none`，不触发 broker、
不进入 production、不修改 `position_advisory_v1.yaml`、policy、official target weights、
portfolio 或 baseline state。
TRADING-179_to_183_CONFIRMATION_CYCLE_WEEKLY_OPS 把上述闭环扩展为 weekly/manual
evidence operations。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/confirmation_cycle_schedule_v1.yaml` 和
`config/etf_portfolio/dynamic_v3_rescue/pressure_regime_tagging_v1.yaml`；CLI 包括
`confirmation-cycle plan/runbook/validate-config/weekly-run/weekly-report`、
`pressure-regime-tag validate-config/run/report`、`confirmation-dashboard build/report` 和
`rule-review-queue build/report`，对应验证入口为
`validate-confirmation-cycle-weekly`、`validate-pressure-regime-tag`、
`validate-confirmation-dashboard` 和 `validate-rule-review-queue`。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/confirmation_cycle_plan|confirmation_cycle_weekly|pressure_regime_tag|confirmation_dashboard|rule_review_queue/`，
并由 report registry / Reader Brief 只读展示。Weekly runner 默认 dry-run，不执行 outcome
update；只有显式 `--execute-ready-updates` 才会调用 safe outcome update，且仍需先通过
cached data quality gate。Pressure regime thresholds 全部来自配置；dashboard 和 queue 只供
人工复核，不授权 defensive label、policy mutation、official target weights mutation、
portfolio mutation、broker action 或 production action。
TRADING-184_to_188_PRESSURE_REGIME_DEFENSIVE_VALIDATION 在 weekly evidence operations
之后新增 pressure-regime sample expansion 和 defensive rule validation 链路。CLI 包括
`pressure-tag-diagnosis run/report`、`pressure-outcome-backfill run/report`、
`defensive-pressure-compare run/report`、`defensive-rule-review run/report` 和
`weekly-ops-decision-update run/report`，对应验证入口为
`validate-pressure-tag-diagnosis`、`validate-pressure-outcome-backfill`、
`validate-defensive-pressure-compare`、`validate-defensive-rule-review` 和
`validate-weekly-ops-decision-update`。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/pressure_tag_diagnosis|pressure_outcome_backfill|defensive_pressure_compare|defensive_rule_review|weekly_ops_decision_update/`，
并由 report registry / Reader Brief 只读展示。Backfill 明确区分 `FORWARD_OUTCOME`、
`HISTORICAL_REPLAY` 和 `BACKTEST_SIMULATION`；simulation 样本统一标记
`SIMULATION_NOT_PIT`、`can_support_production=false`。该链路只解释 pressure tag
mapping gap、扩充 research-only pressure outcome inventory，并生成 owner checklist /
weekly next actions；不批准 defensive label、不修改 `position_advisory_v1.yaml`、
policy、official target weights、portfolio 或 baseline state、不触发 broker。
TRADING-189_to_198_DEFENSIVE_HYPOTHESIS_FORWARD_EVIDENCE 在上述 pressure validation
后新增 defensive hypothesis review 和 forward evidence automation。CLI 包括
`defensive-hypothesis-deep-dive run/report`、`defensive-label-review run/report`、
`defensive-failure-study run/report`、`defensive-research-note run/report`、
`defensive-owner-pack run/report`、`forward-pressure-capture plan/report`、
`pressure-trigger scan/report`、`pressure-capture run/report`、
`pressure-sample-ledger update/report` 和 `weekly-defensive-evidence run/report`，对应
验证入口为 `validate-defensive-hypothesis-deep-dive`、
`validate-defensive-label-review`、`validate-defensive-failure-study`、
`validate-defensive-research-note`、`validate-defensive-owner-pack`、
`validate-forward-pressure-capture`、`validate-pressure-trigger`、
`validate-pressure-capture`、`validate-pressure-sample-ledger` 和
`validate-weekly-defensive-evidence`。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/forward_pressure_capture_v1.yaml`。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/defensive_hypothesis_deep_dive|defensive_label_review|defensive_failure_study|defensive_research_note|defensive_owner_pack|forward_pressure_capture|pressure_trigger|pressure_capture|pressure_sample_ledger|weekly_defensive_evidence/`，
并由 report registry / Reader Brief 只读展示。该链路把 simulation 中的 supporting /
contradicting cases、label 风险、failure pattern、owner decision options、daily/weekly/event
capture plan、trigger scan、capture run/skip、pressure sample ledger 和 weekly evidence
汇总成 forward tracking baseline；所有输出固定 `RESEARCH_ONLY`、`auto_apply=false`、
`policy_change_allowed=false`、`broker_action_allowed=false`、`production_effect=none`，不修改
`position_advisory_v1.yaml`、policy、official target weights、portfolio 或 baseline/production
state，也不触发 broker。
TRADING-199_to_203_MANUAL_PORTFOLIO_GUARDRAILS 在 dynamic v3 rescue shadow shortlist
和 position advisory 之上新增严格 manual snapshot / exposure / drift / execution
guardrail / review pack 链路。CLI 包括 `manual-portfolio validate/normalize/report`、
`portfolio-exposure validate/report`、`position-drift run/report`、
`execution-guardrails check/report` 和 `manual-execution-review pack/report`，对应
验证入口为 `validate-manual-portfolio`、`validate-portfolio-exposure`、
`validate-position-drift`、`validate-execution-guardrails` 和
`validate-manual-execution-review`。配置入口为
`manual_portfolio_snapshot_schema_v1.yaml`、`portfolio_exposure_policy_v1.yaml`、
`execution_guardrails_v1.yaml` 和 `position_advisory_v1.yaml` 的 `drift_analysis`
section。Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/manual_portfolio_snapshot|portfolio_exposure|position_drift|execution_guardrails|manual_execution_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue Manual Execution Review`
区块。该链路只消费 owner-maintained manual snapshot 和 shadow shortlist target weights；
它不导入 broker、不生成 order ticket、不写 official target weights、不修改 portfolio /
baseline / production state。所有输出固定 `broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`owner_approval_required=true`
和 `production_effect=none`。
TRADING-204_to_208_REAL_MANUAL_SNAPSHOT_DRY_RUN_AND_OWNER_DECISION_LOOP 在上述
manual execution review 之上新增真实人工 snapshot intake、dry-run、owner decision、
paper-only action 和 weekly rollup 链路。CLI 包括
`real-snapshot template/lint/intake/report`、`real-snapshot-dry-run run/report`、
`real-execution-owner-review create/record/report`、`real-snapshot-paper-action apply/report`
和 `weekly-real-snapshot-review run/report`，对应验证入口为
`validate-real-snapshot`、`validate-real-snapshot-dry-run`、
`validate-real-execution-owner-review`、`validate-real-snapshot-paper-action` 和
`validate-weekly-real-snapshot-review`。模板入口为
`config/etf_portfolio/dynamic_v3_rescue/current_portfolio_snapshot.real.template.yaml`。
Artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/real_snapshot_intake|real_snapshot_dry_run|real_execution_owner_review|real_snapshot_paper_action|weekly_real_snapshot_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue Real Snapshot Advisory Review`
区块。该链路只接受 owner 提供、脱敏后的 manual snapshot；redaction lint 会阻断
account/order/tax-lot/personally identifying/statement path 类字段；dry-run 只生成
advisory evidence，owner decision 只记录 `monitor`、`paper_adjustment`、`no_trade`、
`needs_more_data`、`reject` 或 `manual_follow_up`，paper action 只更新 paper-only
projection。所有输出固定 `broker_action_allowed=false`、`broker_action_taken=false`、
`order_ticket_generated=false`、`real_portfolio_mutated=false`、
`production_effect=none`，不得导入 broker、生成订单、修改真实仓位、official target
weights、baseline 或 production state。
TRADING-209_to_213_SYSTEM_TARGET_PORTFOLIO_AND_PAPER_SHADOW 在 dynamic v3 rescue
shadow shortlist / candidate consensus / position advisory policy 之上新增完全独立的
research model target portfolio 和 paper shadow account 链路，不读取 owner real
portfolio。CLI 包括 `model-target config-validate/generate/report`、`paper-shadow
init/state/report`、`model-rebalance simulate/report`、`paper-shadow-performance
run/report --as-of YYYY-MM-DD` 和 `system-target-review pack/report`，对应验证入口为
`validate-model-target`、`validate-paper-shadow`、`validate-model-rebalance`、
`validate-paper-shadow-performance` 和 `validate-system-target-review`。配置入口为
`model_target_portfolio_v1.yaml` 和 `paper_shadow_account_v1.yaml`；runtime artifacts
写入
`reports/etf_portfolio/dynamic_v3_rescue/model_target|paper_shadow|model_rebalance|paper_shadow_performance|system_target_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
区块。`paper-shadow-performance run` 在读取缓存价格前执行 `aits validate-data`
等价门禁，artifact 显式写出 `performance_start_date`、`evaluation_as_of` 和
`data_quality_status`。所有输出固定 `research_target_only=true`、
`paper_shadow_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、
`order_ticket_generated=false`、`production_effect=none`，不得生成 broker action、
order ticket、official target weights、production candidate 或真实 portfolio mutation。
TRADING-214_to_218_PAPER_SHADOW_HISTORICAL_BACKFILL_AND_ROLLING_TARGET_EVALUATION 在
上述 research model target methods 之上新增 historical paper shadow backfill 和 rolling
target evaluation。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/paper_shadow_backfill_v1.yaml`，默认请求
`market_regime=ai_after_chatgpt`、`2022-12-01` 到 latest available。CLI 链路为
`paper-shadow-backfill config-validate/run/report` ->
`paper-shadow-rolling-eval run/report`、`paper-shadow-regime-review run/report`、
`paper-shadow-stability run/report` -> `system-target-selection-review run/report`；
对应验证入口为 `validate-paper-shadow-backfill`、
`validate-paper-shadow-rolling-eval`、`validate-paper-shadow-regime-review`、
`validate-paper-shadow-stability` 和 `validate-system-target-selection-review`。Runtime
artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_backfill|paper_shadow_rolling_eval|paper_shadow_regime_review|paper_shadow_stability|system_target_selection_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
区块。`paper-shadow-backfill run` 在读取 cached prices 前执行 `aits validate-data`
等价门禁，artifact 显式写出 `data_quality_status`、actual date range、`mode=BACKTEST_SIMULATION`
和 `not_pit_safe=true`。selection review 的 `recommended_research_method` 只代表继续观察或
人工复核优先级；所有输出固定 `not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`production_effect=none`，不得修改 target config、official target weights、paper/real
portfolio、baseline/production state、policy 或触发 broker。
TRADING-219_to_223_PAPER_SHADOW_SELECTION_DRILLDOWN_AND_RESEARCH_METHOD_HARDENING
在上述 selection review 之后新增原因归因和 research method hardening pack。CLI 链路为
`selection-attribution run/report`、`limited-long-risk run/report`、
`limited-consistency run/report`、`data-warning-impact run/report` 和
`research-method-hardening run/report`；对应验证入口为
`validate-selection-attribution`、`validate-limited-long-risk`、
`validate-limited-consistency`、`validate-data-warning-impact` 和
`validate-research-method-hardening`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/selection_attribution|limited_long_risk|limited_consistency|data_warning_impact|research_method_hardening/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
hardening fields。该链路只解释 `limited_adjustment` 为什么被推荐、为什么仍为
`REVIEW_REQUIRED`、长窗口风险/一致性/数据 warning 是否改变结论，以及 owner 下一步
checklist；它不写 `model_target_portfolio_v1.yaml`、不写 `position_advisory_v1.yaml`、
不写 official target weights、不修改 paper/real portfolio、baseline/production state 或
policy、不生成 order ticket、不触发 broker。即使 hardening decision 未来通过，也只表示
`hardened_primary_research_method` 观察口径，不是 production approval。
TRADING-224_to_228_LIMITED_ADJUSTMENT_WEAKNESS_DIAGNOSIS_AND_RESEARCH_METHOD_REFINEMENT
在 hardening pack 之后继续拆解 `limited_adjustment` 的未通过原因。CLI 链路为
`limited-instability run/report`、`limited-risk-attribution run/report`、
`data-warning-repair-plan run/report`、`alternative-method-review run/report` 和
`refined-method-proposal run/report`；对应验证入口为
`validate-limited-instability`、`validate-limited-risk-attribution`、
`validate-data-warning-repair-plan`、`validate-alternative-method-review` 和
`validate-refined-method-proposal`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/limited_instability|limited_risk_attribution|data_warning_repair_plan|alternative_method_review|refined_method_proposal/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
refined proposal fields。该链路只诊断 rolling instability、return-improves-risk-worsens
归因、data warning blocking / repair plan、替代 research method 候选和 refined next
method proposal；`risk_capped_limited_adjustment` 与 `regime_gated_limited_adjustment`
在本阶段只是 conceptual candidate，不实现、不写 target config、不写 official target
weights、不修改 `position_advisory_v1.yaml`、paper/real portfolio、baseline/production
state 或 policy、不生成 order ticket、不触发 broker。

TRADING-229_to_233_RISK_CAPPED_LIMITED_ADJUSTMENT_RESEARCH_METHOD 实现
`risk_capped_limited_adjustment` research-only method。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/risk_capped_limited_adjustment_v1.yaml`；
CLI 入口为 `risk-capped-limited config-validate/report-config/generate/report`、
`validate-risk-capped-limited-config`、`validate-risk-capped-limited`、
`risk-capped-backfill run/report`、`validate-risk-capped-backfill`、
`risk-capped-comparison run/report`、`validate-risk-capped-comparison`、
`risk-capped-review pack/report` 和 `validate-risk-capped-review`。Runtime artifacts
写入
`reports/etf_portfolio/dynamic_v3_rescue/risk_capped_limited_config|risk_capped_limited|risk_capped_backfill|risk_capped_comparison|risk_capped_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
risk-capped fields。该链路从 `limited_adjustment` 生成 cap events / reallocation
events，backfill 前执行 cached data quality gate，比较 return、drawdown、
semiconductor exposure 和 rolling stability，并输出 owner review decision；所有输出
固定 `research_target_only=true`、`paper_shadow_only=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`，不得写 official target weights、`position_advisory_v1.yaml`、
paper/real portfolio、baseline/production state、policy、order ticket 或 broker。

TRADING-239_to_245_WEIGHT_OPTIMIZATION_EXPERIMENT_FACTORY 建立
`limited_adjustment` 后续权重优化的轻量实验筛选闭环。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/weight_optimization_hypothesis_v1.yaml`、
`weight_variant_transform_v1.yaml` 和 `weight_experiment_matrix_v1.yaml`；CLI 入口为
`hypothesis-backlog build/report`、`variant-transform validate-spec/report-spec`、
`experiment-matrix build/report`、`batch-experiment run/report`、
`experiment-triage run/report`、`top-variant-interpretation run/report`、
`method-promotion-plan run/report`，以及 `validate-hypothesis-backlog`、
`validate-variant-transform-spec`、`validate-experiment-matrix`、
`validate-batch-experiment`、`validate-experiment-triage`、
`validate-top-variant-interpretation` 和 `validate-method-promotion-plan`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/hypothesis_backlog|variant_transform_spec|experiment_matrix|batch_experiment|experiment_triage|top_variant_interpretation|method_promotion_plan/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
experiment fields。该链路先记录 failure taxonomy 和 hypothesis backlog，再用
lightweight transform spec 与 15 个初始 variants 批量生成 weight path、跑 cached data
quality gate 后的 paper-shadow backfill、输出 performance/regime/stability metrics，
最后经 triage gate、top variant interpretation 和 promotion plan 只挑选 1-2 个后续可正式
实现的 research method 候选；本阶段不实现 formal method、不修改
`position_advisory_v1.yaml` 或 `model_target_portfolio_v1.yaml`、不写 official target
weights、不修改 paper/real portfolio、baseline/production state、policy、order ticket 或
broker。所有输出固定 `experiment_only=true`、`research_screening_only=true`、
`not_formal_research_method=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`。

TRADING-246_to_250_SMOOTHED_LIMITED_ADJUSTMENT_RESEARCH_METHOD 实现
`smooth_weights_3d_limited_adjustment` 和 `smooth_weights_5d_limited_adjustment`
research-only method。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/smoothed_limited_adjustment_v1.yaml`；CLI 入口为
`smoothed-limited config-validate/report-config/generate/report`、
`validate-smoothed-limited-config`、`validate-smoothed-limited`、
`smoothed-backfill run/report`、`validate-smoothed-backfill`、
`smoothed-comparison run/report`、`validate-smoothed-comparison`、
`smoothed-review pack/report` 和 `validate-smoothed-review`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_limited_config|smoothed_limited|smoothed_backfill|smoothed_comparison|smoothed_review/`，
并登记 report registry / Reader Brief `Dynamic Rescue System Target Portfolio`
smoothed fields。该链路从 `limited_adjustment` 生成 3d / 5d 平滑 target weights、
smoothing events、lag events 和 weight jump reduction summary，backfill 前执行 cached
data quality gate，比较 smoothed vs `limited_adjustment` / risk-capped / baseline 的
return、drawdown、turnover、rolling/regime/stability 和 lag cost，并输出 owner review
decision；所有输出固定 `research_target_only=true`、`paper_shadow_only=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`，不得写 official target weights、`position_advisory_v1.yaml`、
paper/real portfolio、baseline/production state、policy、order ticket 或 broker。

TRADING-251_to_255_SMOOTHED_METHOD_EVIDENCE_DRILLDOWN_AND_FORWARD_CONFIRMATION
在上述 smoothed review 之后新增 evidence drilldown 和 weekly watch 闭环。CLI 入口为
`smoothed-review-attribution run/report`、`validate-smoothed-review-attribution`、
`smoothing-benefit-lag run/report`、`validate-smoothing-benefit-lag`、
`smoothed-regime-validation run/report`、`validate-smoothed-regime-validation`、
`smoothed-confirmation register/report`、`validate-smoothed-confirmation`、
`smoothed-watch-pack run/report` 和 `validate-smoothed-watch-pack`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_review_attribution|smoothing_benefit_lag|smoothed_regime_validation|smoothed_forward_confirmation|smoothed_watch_pack/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Method Watch`。该链路解释
`CONTINUE_OBSERVATION` / `LOW` confidence 原因，拆解 smoothing benefit vs lag cost，
验证 `sideways_choppy` 和 `strong_recovery` 表现，登记 `smooth_3d_vs_limited`、
`smooth_3d_vs_static_baseline`、`smooth_3d_sideways_choppy_improvement` 和
`smooth_3d_recovery_lag_watch` forward confirmation targets，并生成 owner weekly watch
pack。所有输出继续固定 `research_target_only=true`、`paper_shadow_only=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`；达标后也不自动 promotion，不写 official target weights、不触发
broker/order/production。

TRADING-256_to_260_SMOOTHED_EVIDENCE_COMPLETENESS_AND_PROMOTION_READINESS
在 smoothed watch pack 之后新增 evidence completeness 和 promotion readiness review。
CLI 入口为 `smoothed-evidence-gap run/report`、`validate-smoothed-evidence-gap`、
`smoothed-churn-backfill run/report`、`validate-smoothed-churn-backfill`、
`sideways-mixed-attribution run/report`、`validate-sideways-mixed-attribution`、
`smoothed-readiness-scorecard run/report`、`validate-smoothed-readiness-scorecard`、
`smoothed-owner-review-update run/report` 和
`validate-smoothed-owner-review-update`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_evidence_gap|smoothed_churn_backfill|sideways_mixed_attribution|smoothed_readiness_scorecard|smoothed_owner_review_update/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Owner Review`。该链路说明
benefit / lag 缺失证据、direct signal churn / weight jump / direction flip metrics、
sideways mixed 原因、3d vs 5d readiness score 和 owner decision options。Readiness weights、
jump thresholds 和 review thresholds 是 research pilot baseline；`PROMOTE_FOR_REVIEW` 只代表
可进入 owner review 讨论，不代表 automatic promotion。所有输出继续固定
`research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`；不得新增/启用 target method、修改
`position_advisory_v1.yaml`、official target weights、paper/real portfolio、baseline/production
state、policy、order ticket 或 broker。

TRADING-261_to_265_SMOOTHED_METHOD_PROMOTION_REVIEW_AND_PRIMARY_CANDIDATE_GATE
在 smoothed owner review update 之后新增 promotion review / paper-shadow primary
research candidate gate。CLI 入口为 `smoothed-promotion-review pack/report`、
`validate-smoothed-promotion-review`、`primary-research-candidate-gate run/report`、
`validate-primary-research-candidate-gate`、`smoothed-forward-binding run/report`、
`validate-smoothed-forward-binding`、`paper-shadow-primary-switch plan/report`、
`validate-paper-shadow-primary-switch`、`smoothed-owner-promotion create/record/report`
和 `validate-smoothed-owner-promotion`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_promotion_review|primary_research_candidate_gate|smoothed_forward_binding|paper_shadow_primary_switch|smoothed_owner_promotion/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Promotion Decision`。
该链路把 `PROMOTE_FOR_REVIEW` 与正式 promotion 分开：3d method 可进入 owner review
并可被 gate 标记为 `ELIGIBLE_FOR_OWNER_APPROVAL`，但 paper-shadow primary candidate
变更仍为 `OWNER_DECISION_REQUIRED`，switch plan 只生成计划，owner promotion journal
只记录决策。Forward binding 把 3d vs limited、sideways improvement 和 recovery lag watch
targets 接入 weekly evidence / dashboard / rule-review queue 的观察语义。所有输出继续固定
`research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`；不得自动切换 primary candidate、写 official
target weights、修改 `position_advisory_v1.yaml`、paper/real portfolio、baseline/production
state、policy、order ticket 或 broker。

TRADING-266_to_270_SMOOTHED_FORWARD_EVIDENCE_OPERATIONS_AND_PRIMARY_CANDIDATE_READINESS
在 smoothed owner promotion decision 之后新增 forward evidence operations 和 owner
renewal 闭环。CLI 入口为 `smoothed-forward-progress update/report`、
`validate-smoothed-forward-progress`、`smoothed-weekly-dashboard build/report`、
`validate-smoothed-weekly-dashboard`、`smoothed-event-monitor update/report`、
`validate-smoothed-event-monitor`、`smoothed-switch-readiness recheck/report`、
`validate-smoothed-switch-readiness`、`smoothed-owner-renewal pack/report` 和
`validate-smoothed-owner-renewal`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_forward_progress|smoothed_weekly_dashboard|smoothed_event_monitor|smoothed_switch_readiness|smoothed_owner_renewal/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Owner Renewal`。该链路跟踪
`smooth_3d_vs_limited` forward events、sideways_choppy samples 和 recovery lag watch，
把 weekly dashboard、event monitor、switch readiness criteria 和 owner renewal options
连接起来。当前 forward/sideways/recovery 样本不足时，recheck 保持
`WAIT_FOR_MORE_FORWARD_DATA`，owner action 仍建议 `continue_observation`。所有输出继续固定
`research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`；不得自动 switch、写 official target weights、
修改 `position_advisory_v1.yaml`、paper/real portfolio、baseline/production state、policy、
order ticket 或 broker。

TRADING-271_to_275_SMOOTHED_FORWARD_SAMPLE_BOOTSTRAP_AND_DAILY_EVIDENCE_COLLECTION
在 owner renewal 继续观察之后新增 smoothed forward sample 自动采集闭环。CLI 入口为
`smoothed-daily-emission run/report`、`validate-smoothed-daily-emission`、
`smoothed-outcome-due scan/report`、`validate-smoothed-outcome-due`、
`smoothed-outcome-update run/report`、`validate-smoothed-outcome-update`、
`smoothed-forward-classify run/report`、`validate-smoothed-forward-classify`、
`smoothed-forward-weekly-run run/report` 和 `validate-smoothed-forward-weekly-run`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_daily_emission|smoothed_outcome_due|smoothed_outcome_update|smoothed_forward_classification|smoothed_forward_weekly_run/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Forward Sample Bootstrap`。
Daily emission 只在 as-of price / model target / weights / data-quality 条件满足时记录
research-only forward observation event；due scan 只标记 1/5/10/20-day windows 是否可更新；
outcome update 只写 due 且价格可用的 realized windows；classification 只把 updated outcomes
归入 sideways/recovery/fast-regime-change/lag-warning evidence buckets；weekly runner 串接
sample collection、progress、dashboard、monitor、readiness recheck 和 owner renewal，但固定
`can_execute_switch=false`。读取 cached ETF prices 的 run/scan/update/weekly-run CLI 会先执行
`aits validate-data` 等价质量门禁，并在 CLI output 中披露 `validate_data_status` 和
`validate_data_report`。所有输出继续固定 `research_target_only=true`、
`paper_shadow_only=true`、`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`；不得使用未来数据、补造 outcome、自动 switch、写 official target
weights、修改 `position_advisory_v1.yaml`、paper/real portfolio、baseline/production state、
policy、order ticket 或 broker。

TRADING-276_to_280_SMOOTHED_FORWARD_DATA_FRESHNESS_AND_LATEST_AVAILABLE_BOOTSTRAP
在 smoothed forward sample bootstrap 后新增 freshness preflight、latest-available
daily emission、blocked explain、refresh plan 和 retry orchestration。CLI 入口为
`smoothed-data-preflight run/report`、`validate-smoothed-data-preflight`、
`smoothed-latest-emission run/report`、`validate-smoothed-latest-emission`、
`smoothed-blocked-explain run/report`、`validate-smoothed-blocked-explain`、
`smoothed-refresh-plan run/report`、`validate-smoothed-refresh-plan`、
`smoothed-bootstrap-retry run/report` 和 `validate-smoothed-bootstrap-retry`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/smoothed_data_preflight|smoothed_latest_emission|smoothed_blocked_explain|smoothed_refresh_plan|smoothed_bootstrap_retry/`，
并登记 report registry / Reader Brief `Dynamic Rescue Smoothed Freshness Bootstrap`。
Preflight 调用 `aits validate-data` 等价门禁并披露 `freshness_status`、
`validate_data_status` 和 `latest_valid_as_of`；requested date 数据 stale、missing 或
future-as-of 时，due scan、outcome update、classification 和 weekly run 保持 blocked。
Latest-available emission 只允许在 `latest_valid_as_of` 生成 daily observation event，
固定 `due_scan_allowed=false`、`outcome_update_allowed=false`、`future_data_used=false`。
Refresh plan 只列出 source requirements 和 rerun commands，不刷新外部数据源。Retry 先跑
preflight；只有 fresh 状态才执行完整 weekly runner，blocked 状态直接记录
`retry_status=BLOCKED`。所有输出继续固定 `research_target_only=true`、
`paper_shadow_only=true`、`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`can_execute_switch=false`、`production_effect=none`；不得绕过 validate-data、使用未来数据、
补造 outcome、自动 switch、写 official target weights、修改 `position_advisory_v1.yaml`、
paper/real portfolio、baseline/production state、policy、order ticket 或 broker。

TRADING-281_to_285_SMOOTHED_DATA_REFRESH_EXECUTION_AND_SCHEDULED_RETRY_OPERATIONS
在上述 freshness bootstrap 后新增受控 source refresh execution、post-refresh validation、
retry resume、sample growth dashboard 和 owner data readiness status pack。CLI 入口为
`smoothed-source-refresh plan/execute/report`、`validate-smoothed-source-refresh`、
`smoothed-post-refresh-validate run/report`、`validate-smoothed-post-refresh`、
`smoothed-retry-resume run/report`、`validate-smoothed-retry-resume`、
`smoothed-sample-growth build/report`、`validate-smoothed-sample-growth`、
`smoothed-data-readiness pack/report` 和 `validate-smoothed-data-readiness`。
Refresh execution 使用
`config/etf_portfolio/dynamic_v3_rescue/smoothed_source_refresh_v1.yaml`，默认 dry-run；
写 cache 必须显式传入 `--execute-refresh`，并记录每个 source 的 before/after row count、
latest date、checksum、provider error 和 `refresh_status`。Post-refresh validation 重新运行
`aits validate-data` 等价门禁和 smoothed preflight；只有 `retry_decision=RETRY_READY`
时 retry resume 才能继续完整 weekly runner，否则 fail closed 写 blocked artifact。Sample
growth 输出 forward/sideways/recovery before/after/delta 和 target progress；data readiness
把 refresh、validation、resume、growth 汇总为 owner-readable `current_status` 与
`recommended_owner_action`，并接入 report registry / Reader Brief
`Dynamic Rescue Smoothed Data Readiness`。所有输出仍固定 `can_execute_switch=false`、
`broker_action_allowed=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`；缺 provider credentials、network failure、stale-after-refresh 或
validation failure 必须显式记录 blocker，不得伪造 fresh cache、绕过 validate-data、补造 outcome
或自动修改 official target weights / portfolio / broker state。

TRADING-286_to_315_WEIGHT_OPTIMIZATION_BATCH_SEARCH_NO_PROMOTION_AND_TARGETED_SEARCH_V3
在 smoothed refresh/readiness 链路之后新增 research-only batch search 和 adaptive
promotion planning，并在 expanded search 无 promoted candidate 时追加 no-promotion
diagnostics 与 targeted expanded search v3。配置入口为
`config/etf_portfolio/dynamic_v3_rescue/weight_search_space_v2.yaml`；CLI 入口为
`weight-search-space validate/report`、`weight-experiment-batch2 build/report`、
`weight-batch-backfill run/resume/report`、`weight-scorecard run/report`、
`weight-robustness-review run/report`、`weight-adaptive-branch run/report`、
`weight-expanded-search build/run`、`weight-candidate-cluster run/report`、
`weight-top-candidate-interpretation run/report`、`weight-method-promotion-gate run/report`、
`formal-method-auto-plan run/report`、`weight-search-dashboard build/report`、
`owner-research-decision-pack build/report`、`no-promotion-review run/report`、
`near-miss-candidates extract/report`、`cash-buffer-attribution run/report`、
`search-coverage-gap run/report`、`targeted-search-v3 build/report`、
`targeted-v3-backfill run/resume/report`、`near-miss-ab-comparison run/report`、
`promotion-threshold-sensitivity run/report`、`candidate-promotion-v2 run/report`、
`next-formal-or-search-plan run/report` 和对应 `validate-*` 命令。Runtime artifacts
写入
`reports/etf_portfolio/dynamic_v3_rescue/weight_search_space|weight_experiment_batch2|weight_batch_backfill|weight_scorecard|weight_robustness_review|weight_adaptive_branch|weight_expanded_search|weight_candidate_cluster|weight_top_candidate_interpretation|weight_method_promotion_gate|formal_method_auto_plan|weight_search_dashboard|owner_research_decision_pack|no_promotion_review|near_miss_candidates|cash_buffer_attribution|search_coverage_gap|targeted_search_v3|targeted_v3_backfill|near_miss_ab_comparison|promotion_threshold_sensitivity|candidate_promotion_v2|next_formal_or_search_plan/`，
并登记 report registry / latest pointers / Reader Brief batch search 摘要。Backfill run 必须先执行
`aits validate-data` 等价质量门禁并披露 `data_quality_status`、actual date range 和
`latest_valid_as_of`；scorecard policy 是 `weight_batch_search_scorecard_pilot_v1`
pilot baseline，hard reject flags 不能被 composite score 覆盖。Promotion gate 只表示可进入
formal implementation planning；`formal-method-auto-plan` 只生成 plan、`implemented=false`，
不修改正式 method config。No-promotion review 只解释 gate / component failure；
near-miss、cash buffer attribution 和 coverage gap 只决定 targeted search focus；
targeted v3 matrix 必须保持 60～120 variants；`targeted-v3-backfill` 继续披露数据质量报告；
threshold sensitivity 是 diagnostic-only，不改变 base gate；candidate promotion v2 和 next plan
仍只支持 owner manual review。所有输出固定 `experiment_only=true`、
`research_screening_only=true`、`not_formal_research_method=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`；不得写 official target weights、修改 `position_advisory_v1.yaml`
或 `model_target_portfolio_v1.yaml`、paper/real portfolio、baseline/production state、policy、
order ticket 或 broker。

TRADING-316_to_325_SIGNAL_LEVEL_DIAGNOSIS_AND_GATE_CALIBRATION_WITH_TARGETED_MICRO_SEARCH_V4
在 targeted v3 仍未给出正式 promotion 后，追加 signal-level diagnosis、gate calibration
review 和 bounded micro-search v4。CLI 入口为 `gate-calibration-review run/report`、
`scorecard-attribution run/report`、`signal-instability-diagnosis run/report`、
`consensus-quality-review run/report`、`micro-search-v4-design run/report`、
`micro-search-v4-backfill run/report`、`gate-calibrated-review run/report`、
`signal-vs-parameter-attribution run/report`、`next-research-direction run/report`、
`owner-research-roadmap update/report` 和对应 `validate-*` 命令。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/gate_calibration_review|scorecard_attribution|signal_instability_diagnosis|consensus_quality_review|micro_search_v4_design|micro_search_v4_backfill|gate_calibrated_review|signal_vs_parameter_attribution|next_research_direction|owner_research_roadmap/`，
并登记 report registry / latest pointers / Reader Brief 摘要。`micro-search-v4-backfill`
必须先执行 `aits validate-data` 等价质量门禁并披露 `data_quality_status`、
actual date range、`latest_valid_as_of` 和数据质量报告路径。Gate calibration 只允许
diagnostic relaxed scenario，不改变 official gate；v4 design 限制 20～40 variants；
gate-calibrated review 只能比较 official gate 与 diagnostic gate；next direction 和
owner roadmap 只生成 owner research checklist。所有输出继续固定 no broker / no order /
`production_effect=none`，不得修改 `position_advisory_v1.yaml`、official target weights、
paper/real portfolio、baseline/production state、policy 或 broker。

TRADING-326_to_335_SIGNAL_FEATURE_DIAGNOSIS_AND_CANDIDATE_QUALITY_FILTER_PIPELINE
在 TRADING-316～325 判断需要 signal-level fix 后，追加 research-only signal feature
diagnosis 与 candidate quality filter pipeline。CLI 入口为 `signal-failure-taxonomy
validate/report`、`candidate-signal-ledger build/report`、`signal-churn-root-cause
run/report`、`regime-mismatch-attribution run/report`、`candidate-quality-filter-design
run/report`、`filtered-candidate-backfill run/report`、`filtered-vs-original-comparison
run/report`、`signal-gate-experiment run/report`、`filtered-candidate-promotion-review
run/report`、`owner-signal-roadmap build/report` 和对应 `validate-*` 命令。Runtime artifacts
写入
`reports/etf_portfolio/dynamic_v3_rescue/signal_failure_taxonomy|candidate_signal_ledger|signal_churn_root_cause|regime_mismatch_attribution|candidate_quality_filter_design|filtered_candidate_backfill|filtered_vs_original_comparison|signal_gate_experiment|filtered_candidate_promotion_review|owner_signal_roadmap/`，
并登记 report registry / latest pointers / Reader Brief 摘要。Filtered backfill 只从已披露
`data_quality_status` 的 source v4 backfill / signal ledger 派生，不重新绕过 cached data quality
gate；promotion review 只能输出 `CONTINUE_TESTING` 或 owner-reviewed formal research planning
候选，不自动创建 formal method、official target weights、portfolio mutation、order ticket 或 broker
action。所有输出固定 `experiment_only=true`、`research_screening_only=true`、
`not_formal_research_method=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`。

TRADING-336_to_345_FILTERED_SIGNAL_CANDIDATE_EVIDENCE_EXPANSION_AND_FORMALIZATION_READINESS
在 TRADING-334/335 将 `median_plus_regime_mismatch_filter` 留在继续验证状态后，追加
research-only evidence expansion、spec review、stress backfill、drawdown mismatch / flip
rotation diagnostics、A/B review、confirmation target registration、formalization readiness、
owner review pack 和 next decision。CLI 入口为 `filtered-candidate-evidence run/report`、
`median-regime-filter-spec review/report`、`filtered-candidate-stress-backfill run/report`、
`drawdown-mismatch-reduction run/report`、`flip-rotation-reduction run/report`、
`filtered-candidate-ab-review run/report`、`signal-gate-confirmation register/report`、
`filtered-formalization-readiness run/report`、`owner-filtered-candidate-review pack/report`、
`filtered-next-decision run/report` 和对应 `validate-*` 命令。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/filtered_candidate_evidence|median_regime_filter_spec|filtered_candidate_stress_backfill|drawdown_mismatch_reduction|flip_rotation_reduction|filtered_candidate_ab_review|signal_gate_confirmation|filtered_formalization_readiness|owner_filtered_candidate_review|filtered_next_decision/`，
并登记 report registry / latest pointers / Reader Brief 摘要。该链路只读既有 filtered
comparison / promotion review evidence 和本链路上游 artifacts，不新增 official signal gate、
formal method config、official target weights、portfolio mutation、order ticket 或 broker action；
所有输出继续固定 `experiment_only=true`、`research_screening_only=true`、
`not_formal_research_method=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
`auto_apply=false`、`production_effect=none`。

TRADING-346_FORMAL_RESEARCH_METHOD_CONTRACT 将上述 filtered candidate readiness
chain 汇总为 research-only formal method contract。CLI 入口为
`aits etf dynamic-v3-rescue research-method-contract build`、
`aits etf dynamic-v3-rescue research-method-contract report --latest` 和
`aits etf dynamic-v3-rescue validate-research-method-contract --contract-id <contract_id>`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/formal_research_method_contract/`，包含
contract manifest、contract JSON、decision JSON、Reader Brief section、Markdown report 和
validation JSON/Markdown，并登记 report registry / latest pointer / Reader Brief 摘要。
该 contract 公开 promotion states、objective gates、failure conditions、paper-shadow
eligibility 和 safety boundary；`FORMAL_RESEARCH_READY` 仅表示可以另开 research-only
method implementation，不是 production approval。所有输出继续固定
`formal_research_contract_only=true`、`not_formal_research_method=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`order_ticket_generated=false`、`auto_apply=false`、`production_effect=none`。

TRADING-348_PROMOTION_GATE_THRESHOLD_CALIBRATION 将 TRADING-346 使用的
promotion-facing gate bands 迁移到可审计治理配置。配置入口为
`config/research/promotion_gate_thresholds.yaml`，owner-readable 说明在
`docs/promotion_gates/threshold_calibration.md`。CLI 入口为
`aits etf dynamic-v3-rescue promotion-gate-threshold-calibration report` 和
`aits etf dynamic-v3-rescue promotion-gate-threshold-calibration validate`。Runtime
artifacts 写入
`run/review/register/promotion-gate-threshold-calibration/`，包含 calibration
manifest、JSON/Markdown report、Reader Brief section 和 validation JSON/Markdown，并登记
report registry / Reader Brief 摘要。该 policy 覆盖 stress strength、drawdown mismatch
reduction、flip/rotation reduction、A/B review confidence 和 confirmation target count；
它是 `pilot_baseline` / governance-only，不改变 formal contract decision logic，不为了当前
candidate pass 而调参，不生成 official target weights、broker action、order ticket 或
production mutation。

TRADING-350_PAPER_SHADOW_PROTOCOL 在 formal contract 之后定义 observation-only
paper-shadow protocol。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-protocol build`、
`aits etf dynamic-v3-rescue paper-shadow-protocol report --latest` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-protocol --protocol-id <protocol_id>`。
Runtime artifacts 写入 `reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_protocol/`，
包含 protocol manifest、protocol JSON、Markdown report、Reader Brief section 和
validation JSON/Markdown，并登记 report registry / latest pointer / Reader Brief 摘要。
Protocol 定义 eligibility、20 trading day pilot observation baseline、daily review fields
和 exit conditions；`hypothetical_weight_recommendation` 必须标记为 paper-shadow-only，
不能被 broker/order 系统消费。Protocol 本身不初始化 paper account、不写
official target weights、不触发 broker 或 order ticket，所有输出固定
`paper_shadow_protocol_only=true`、`observation_only=true`、`manual_review_only=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、`production_effect=none`。

TRADING-371_SIGNAL_INPUT_COMPLETENESS_MONITOR 在 paper-shadow daily / weekly 使用 signal
inputs 前新增只读 completeness guard。CLI 入口为
`aits etf dynamic-v3-rescue signal-input-completeness run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue signal-input-completeness report --latest` 和
`aits etf dynamic-v3-rescue validate-signal-input-completeness --latest`。Policy 位于
`config/etf_portfolio/dynamic_v3_rescue/signal_input_completeness_v1.yaml`，显式定义
required signal files、schema / feature version、required columns、coverage universe 和 stale
thresholds。Monitor 检查 missing signal files、stale signal files、incompatible schema version、
empty signal series、partial market coverage 和 missing required feature columns，输出
`signal_input_status=OK|WARNING|BLOCKING`。`BLOCKING` 或缺少 monitor 会让
`paper-shadow-daily run` fail closed，并传播到 weekly review、evidence staleness monitor、
shadow continuation readiness 和 Reader Brief。该 guard 不刷新数据、不补造 signal / feature
artifact、不运行上游、不写 official target weights、不触发 broker/order、不修改 paper account
或 production state；所有输出固定 `signal_input_completeness_monitor_only=true`、
`read_only_signal_input_check=true`、`data_downloaded_by_monitor=false`、
`pipelines_executed_by_monitor=false`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-385_SIGNAL_INPUT_ROOT_CAUSE_AND_RESTORE 在 signal input completeness 阻断后新增
root-cause / recovery audit。Operator 必须先通过 canonical path 恢复输入：
`aits validate-data --as-of YYYY-MM-DD`、`aits etf data validate`、
`aits etf features build --end latest`、`aits signals build-snapshot --latest`、
`aits etf signals generate --date latest` 和 `aits reports signal-snapshot --latest`，随后运行
`aits etf dynamic-v3-rescue signal-input-completeness run --as-of YYYY-MM-DD`。Recovery CLI
为 `aits etf dynamic-v3-rescue signal-input-recovery run --as-of YYYY-MM-DD
--restored-monitor-id <monitor_id> --previous-monitor-id <blocking_monitor_id>`、
`report --latest` 和 `validate-signal-input-recovery --latest`。输出记录
`SIGNAL_INPUTS_RESTORED|SIGNAL_INPUTS_RESTORED_WITH_WARNINGS|SIGNAL_INPUTS_STILL_BLOCKED`、
canonical generation path、root cause、restored feature/signal artifact ids 和 hard-stop
next action。该 audit 不运行上游、不刷新数据、不补造 signal / feature / snapshot artifact、
不放宽 completeness policy、不写 official target weights、不触发 broker/order、不修改 paper account
或 production state；`SIGNAL_INPUTS_STILL_BLOCKED` 必须停止后续 promotion / extended-shadow
recovery chain。

TRADING-386_SIGNAL_INPUT_COMPLETENESS_RECOVERY_RERUN 在 385 之后新增明确恢复状态
artifact。CLI 入口为 `aits etf dynamic-v3-rescue signal-input-completeness-recovery run
--as-of YYYY-MM-DD`、`report --latest` 和 `validate-signal-input-completeness-recovery
--latest`。该 workflow 重新运行或读取 TRADING-371 completeness monitor，检查 required
files、schema version、non-empty signal series、required feature columns、market coverage
和 as-of consistency，并输出
`SIGNAL_INPUTS_RESTORED|SIGNAL_INPUTS_RESTORED_WITH_WARNINGS|SIGNAL_INPUTS_STILL_BLOCKED`、
blocker list、warning list、hard-stop flag 和 next action。它只运行 completeness monitor，
不运行 feature / signal / snapshot builders、不刷新数据、不补造 signal artifact、不放宽
policy、不写 official target weights、不触发 broker/order、不修改 paper account 或
production state。

TRADING-351_PAPER_SHADOW_DAILY_RUNNER 在 formal contract 和 paper-shadow protocol
之后新增 observation-only daily runner。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-daily run`、
`aits etf dynamic-v3-rescue paper-shadow-daily report --latest` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-daily --latest`。Run 输入必须显式给出
candidate id、observation date、market panel artifact、latest signal artifact、formal
research method contract、paper-shadow protocol 和 signal input completeness monitor；输出写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_daily/`，包含 daily manifest、
observation JSON、Markdown report、Reader Brief section 和 validation JSON/Markdown，并登记
report registry / latest pointer / Reader Brief 摘要。`hypothetical_weight_recommendation`
只表示 paper-shadow-only note，不是 official target weights；所有输出固定
`manual_review_only=true`、`paper_shadow_daily_only=true`、`observation_only=true`、
`hypothetical_weight_paper_shadow_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`order_ticket_generated=false`、`auto_apply=false`、
`production_effect=none`，不得初始化或修改 paper account、broker、order 或 production state。

TRADING-352_PAPER_SHADOW_DRIFT_MONITOR 在 daily paper-shadow observation 后新增
只读 drift monitor。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-drift-monitor report` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-drift-monitor --monitor-id <monitor_id>`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_drift_monitor/`，包含 drift manifest、
JSON report、JSONL findings、Markdown report、Reader Brief section 和 validation
JSON/Markdown，并登记 report registry / latest pointer / Reader Brief 摘要。Monitor 比较
当前 observation 与 formal research method contract 中的 validated behavior，输出
`drift_severity=NONE|WATCH|WARNING|BLOCKING`、turnover / risk-off / drawdown mismatch /
flip-rotation / benchmark / missing-input findings 和 next action。所有输出固定
`paper_shadow_drift_monitor_only=true`、`read_only_monitor=true`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`order_ticket_generated=false`、`paper_account_state_mutated=false`、`auto_apply=false`、
`production_effect=none`；不得刷新数据、重跑上游、修改 paper account、生成 order 或自动
promote/reject candidate。

TRADING-352A_REPORT_INDEX_WARNING_CLEANUP 为 report index 增加 freshness basis 和显式
visibility waiver audit。`config/report_registry.yaml` 的 defaults 现在按 cadence 定义
`freshness_basis`：daily / weekly / biweekly 使用 U.S. equity trading days，避免周末把
Friday artifact 误判为 Monday stale；ad-hoc / monthly 继续用 calendar days。机器可读
waiver policy 位于 `config/report_index_visibility_waivers.yaml`，每条 waiver 必须记录
report id、issue status、reason、owner、accepted impact、validation coverage 和 exit
condition。`aits reports index` 输出 `visibility_audit`、`explicit_waivers`、per-report
`visibility_issue` / `visibility_waiver`；如果所有 missing/stale issue 都有显式 waiver，
状态为 `PASS_WITH_EXPLICIT_WAIVERS`。该状态不代表 waived artifact 已刷新，只代表 warning
已被审计解释；required daily missing 和 production-effect risk 仍不能被 waiver 静默压掉。

TRADING-360_REPORT_QUALITY_GATE 新增通用只读报告质量门禁。`aits reports quality-gate
--date YYYY-MM-DD` 读取同日 report index、latest report artifacts 和 Reader Brief JSON，
输出 `outputs/reports/report_quality_gate_YYYY-MM-DD.json/md`，显式给出
`report_quality_status`、`missing_sections`、`blocking_quality_issues` 和
`warning_quality_issues`。普通 report 会检查 purpose、input artifacts、output decision、
safety boundary、limitations 和 next action；Reader Brief 会检查 human-readable summary、
key result、blocking issues、warnings 和 recommended next step。缺 section 先作为可见
warning，production-effect 风险、不可读 artifact 或 Reader Brief 缺核心摘要/结果/下一步会
fail closed。该 gate 不修复报告、不重跑上游、不刷新数据、不修改 source artifact 或 production
state；report registry / report index 已登记，Reader Brief 只读展示 latest quality gate 摘要。

TRADING-361_ARTIFACT_LINEAGE_GRAPH 新增 candidate research / paper-shadow artifact 依赖链。
`aits reports artifact-lineage --date YYYY-MM-DD` 只读读取 report registry、report index
和既有 artifact paths，输出 `outputs/reports/artifact_lineage_graph_YYYY-MM-DD.json/md`；
`aits reports validate-artifact-lineage --date YYYY-MM-DD` 校验 required families、required
edges 和 production safety，输出 `outputs/reports/artifact_lineage_validation_YYYY-MM-DD.json/md`。
Graph 覆盖 data artifacts、cache catalog、refresh audit、PIT manifest、signal artifacts、
daily paper-shadow、drift monitor、weekly review、staleness monitor、readiness reports 和
owner reviews。缺 required family / edge 或 unsafe `production_effect` 会 fail closed；stale
artifact 会作为 warning 披露。该链路不运行上游、不刷新数据、不补造 artifact、不修改 paper-shadow
decision / owner review / production state；daily scheduler 在 report index 前生成并校验，Reader Brief
只读展示 latest lineage 摘要。

TRADING-362_TASK_REGISTER_CONSISTENCY_CHECK 新增 task register consistency gate。
`aits reports task-register-consistency run --as-of YYYY-MM-DD` 只读读取
`docs/task_register.md`、`docs/task_register_completed.md`、`config/report_registry.yaml`
和 `docs/artifact_catalog.md`，校验 task id、status、active/completed 去重、terminal
status 归档、显式 docs link、artifact family / catalog entry、report registry entry 和
Reader Brief entry；`report --latest` 从既有 JSON 重渲染 Markdown；`validate --latest`
对 blocking issue fail closed。输出 `outputs/reports/task_register_consistency_YYYY-MM-DD.json/md`
和 `task_register_consistency_validation_YYYY-MM-DD.json/md`，包含 Reader Brief 摘要和
`production_effect=none` safety boundary；该 gate 不修改 task register、不补造需求文档、
不运行上游、不刷新数据、不写 official target weights、不触发 broker/order 或 production
mutation。

TRADING-375_REGISTRY_WARNING_WAIVER_EXPIRY 强化 report index visibility waiver 治理。
`config/report_index_visibility_waivers.yaml` 中每条 waiver 现在必须记录 `waiver_id`、
affected report registry entry / artifact family、reason、owner、`created_at`、`expires_at`、
`review_status` 和 `linked_task_id`。`aits reports index` 只会应用 `approved_active`
且未过期的 waiver，并在 payload 中披露 expired / inactive waiver counts；过期或未获
active review 的 waiver 不再能把 missing/stale issue 清成 explicit waiver。新增
`aits reports waiver-inventory --as-of YYYY-MM-DD` 和
`aits reports validate-waiver-inventory --latest`，输出
`outputs/reports/report_index_waiver_inventory_YYYY-MM-DD.json/md` 和
`report_index_waiver_inventory_validation_YYYY-MM-DD.json/md`；expired waiver 或缺失
registry entry 必须 fail closed，接近过期或 inactive review status 作为人工复核 warning。
Reader Brief 只读展示 latest waiver inventory，不刷新 artifact、不补造 report、不降低
missing/stale/production-effect risk。

TRADING-376_READER_BRIEF_CONSISTENCY_PACK 新增 Reader Brief section consistency gate。
标准 section contract 为 Summary、Key Result、Blocking Issues、Warnings、Safety Boundary
和 Next Action。`aits reports reader-brief-consistency --as-of YYYY-MM-DD` 只读读取
同日 report index 和 include-in-Reader-Brief report artifacts，输出
`outputs/reports/reader_brief_consistency_pack_YYYY-MM-DD.json/md`；`aits reports
validate-reader-brief-consistency --latest` 输出
`reader_brief_consistency_validation_YYYY-MM-DD.json/md`。Daily Reader Brief 缺核心
summary/key result/safety boundary/next action 必须 fail closed；legacy report section
gap 先作为可见 warning，不重写历史 artifact。Reader Brief quality gate 也会显式检查
safety boundary、next action 和 decision state；该链路不运行上游、不刷新数据、不补造
report、不写 official target weights、不触发 broker/order 或 production mutation。

TRADING-377_PRODUCTION_BOUNDARY_STATIC_SCANNER 新增 production boundary static scan。
`aits reports production-boundary-static-scan --as-of YYYY-MM-DD` 只读扫描 source、config、
docs 和 scripts 中的 broker、order ticket、live allocation、official target weight、
production mutation、auto execute、live order、account id、API key 等 production-facing
terms，并使用 `config/production_boundary_static_scan_allowlist.yaml` 把 documentation-only
和既有 paper/stub boundary surfaces 降为可见 warning。输出
`outputs/reports/production_boundary_static_scan_YYYY-MM-DD.json/md`；`aits reports
validate-production-boundary-static-scan --latest` 输出
`production_boundary_static_scan_validation_YYYY-MM-DD.json/md`。高置信 production action
或非 paper secret-like literal 必须 `BLOCKING` fail closed；warning 不会被静默隐藏。

TRADING-364_OWNER_REVIEW_TEMPLATE_V2 新增 governance-wide owner review template。
`aits reports owner-review-template-v2 --as-of YYYY-MM-DD` 输出
`outputs/reports/owner_review_template_v2_YYYY-MM-DD.json/md`，固化 required fields：
candidate id、evidence interpretation、main reason to continue/reject、uncertainty、
required follow-up、final owner action、linked input artifacts 和 safety status；owner action enum
固定为 `continue_shadow|enter_extended_shadow|needs_more_evidence|return_to_research|reject_candidate|hold`。
`aits reports validate-owner-review-template-v2 --latest` 输出 validation artifact，并可用
`--review-json-path <filled_review.json>` 只读校验已填写 review。缺 required field、非法 owner action、
缺 linked input artifact、缺 safety status 或 `SAFETY_BLOCKED` 搭配 continuation action 必须 fail closed。
该 template 不记录 owner decision、不 append audit log、不运行 evidence collection、不刷新数据、不改变
candidate / paper-shadow / production state；Reader Brief 只读展示 latest template readiness。

TRADING-378_OWNER_DECISION_AUDIT_LOG 新增 append-only owner decision audit log。
`aits reports owner-decision-audit-log append --decision-json-path <owner_decision.json>`
只向默认 `data/governance/owner_decision_audit_log.jsonl` 追加一条 JSONL 记录，不重写既有记录。
每条记录包含 decision id、timestamp、candidate id、input artifacts、owner action、reason summary、
safety status 和 next action；`aits reports owner-decision-audit-log report --as-of YYYY-MM-DD`
输出 `outputs/reports/owner_decision_audit_log_YYYY-MM-DD.json/md`，披露 latest decision、
owner action counts、monthly review pack input 和 promotion board input；`aits reports
owner-decision-audit-log validate --latest` 输出 validation artifact。Malformed JSONL、
duplicate decision id、缺 required field、非法 owner action / safety status、`SAFETY_BLOCKED`
搭配 continuation action 或 production mutation 字段必须 fail closed。该 audit log 是
governance-only，不改变 strategy outputs、candidate / paper-shadow / production state，不生成
official target weights、order ticket 或 broker action；Reader Brief 只读展示 latest audit log status。

TRADING-365_RESEARCH_MONTHLY_REVIEW_PACK 新增 monthly research governance review pack。
`aits reports research-monthly-review-pack --as-of YYYY-MM-DD` 只读读取同日 report index
和既有 candidate ledger、paper-shadow weekly review、staleness monitor、readiness、safety audit、
owner decisions、cost sensitivity、benchmark baseline control、artifact lineage graph 与 data governance
artifacts，输出 `outputs/reports/research_monthly_review_pack_YYYY-MM-DD.json/md`。
`aits reports validate-research-monthly-review-pack --latest` 输出
`research_monthly_review_pack_validation_YYYY-MM-DD.json/md`。报告披露 active / rejected /
paper-shadow / needs-evidence candidates、major blockers、major warnings、safety audit status、
data governance status、owner decision status 和 monthly Reader Brief。该 pack 不运行上游、
不刷新数据、不补造缺失 artifact、不修改 strategy / candidate / paper-shadow / production state、
不生成 official target weights、order ticket 或 broker action；candidate blockers 只是 manual owner
review 结论输入，不自动 reject 或 promote。

TRADING-379_PAPER_SHADOW_PROMOTION_BOARD 新增 paper-shadow-only promotion board。
`aits reports paper-shadow-promotion-board --as-of YYYY-MM-DD` 只读读取同日 report index
和既有 monthly review、canonical health、weekly review、readiness、drift、cost sensitivity、
benchmark baseline、safety audit、owner review / owner decision audit log 与 lineage graph，输出
`outputs/reports/paper_shadow_promotion_board_YYYY-MM-DD.json/md`。
`aits reports validate-paper-shadow-promotion-board --latest` 输出
`paper_shadow_promotion_board_validation_YYYY-MM-DD.json/md`。Board decision 限定为
`EXTEND_SHADOW|CONTINUE_NORMAL_SHADOW|RETURN_TO_RESEARCH|REJECT|HOLD_FOR_MORE_DATA`；
缺 evidence、stale/readiness/health/cost/benchmark/safety/owner decision blocker 必须进入
required checklist / blocking reasons；`EXTEND_SHADOW` 还要求所有 extension evidence PASS 且
owner 明确请求 extended shadow。该 board 不运行上游、不刷新数据、不补造 evidence、不修改
strategy / candidate / paper-shadow / production state，不生成 official target weights、order ticket
或 broker action；即使 `EXTEND_SHADOW` 也只表示 owner 可继续复核 extended shadow protocol，不是
live trading 或 official allocation approval。

TRADING-391_EVIDENCE_PACK_REBUILD_AFTER_RECOVERY 新增 post-recovery evidence pack。
`aits reports recovery-evidence-pack --as-of YYYY-MM-DD` 只读读取同日 report index 和 latest
signal input completeness / recovery、evidence staleness、shadow continuation readiness、
paper-shadow health、cost metrics materialization、cost-sensitivity review、benchmark baseline
metrics materialization、benchmark baseline control 和 research safety boundary audit，输出
`outputs/reports/recovery_evidence_pack_YYYY-MM-DD.json/md`。
`aits reports validate-recovery-evidence-pack --latest` 输出
`recovery_evidence_pack_validation_YYYY-MM-DD.json/md`。Pack status 限定为
`RECOVERY_EVIDENCE_COMPLETE|RECOVERY_EVIDENCE_PARTIAL|RECOVERY_EVIDENCE_BLOCKED`，只表示
required recovery evidence 是否可用于人工复核；source conclusions 中的 cost/benchmark/health
blocker 仍必须作为 remaining recovery blockers 披露。该 pack advisory-only，不运行上游、不刷新
数据、不补造缺失 artifact、不写 owner decision、不修改 strategy / candidate / paper-shadow /
production state，不生成 official target weights、order ticket 或 broker action，也不批准 promotion、
extended shadow 或 live trading。

TRADING-400_RESEARCH_GOVERNANCE_RECOVERY_PACK 新增最终 recovery governance pack。
`aits reports research-governance-recovery-pack --as-of YYYY-MM-DD` 只读汇总同日
report index 和 latest signal input restoration、signal completeness recovery、readiness /
health recovery、normal paper-shadow resumption gate、cost / benchmark evidence、
recovery evidence pack、owner hold decision、monthly review、promotion board、observation
clock、extended shadow protocol、roadmap dashboard 和 decision snapshot lifecycle，输出
`outputs/reports/research_governance_recovery_pack_YYYY-MM-DD.json/md`。
`aits reports validate-research-governance-recovery-pack --latest` 输出
`research_governance_recovery_pack_validation_YYYY-MM-DD.json/md`。Status 限定为
`RECOVERY_GOVERNANCE_HEALTHY|RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS|RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED|RECOVERY_GOVERNANCE_BLOCKED`，
并披露 remaining blockers、remaining warnings、next owner action、normal paper-shadow
是否可恢复、extended shadow 是否仍 forbidden、live trading 是否仍 forbidden。该 pack 只做
owner recovery review 输入，不运行上游、不刷新数据、不补造 missing artifacts、不写 owner
decision、不修改 strategy / candidate / paper-shadow / production state；即使 healthy，也最多
支持 owner 人工复核恢复 normal paper-shadow observation，永不批准 live trading、official target
weights、broker action 或 order ticket。

TRADING-401～407_RECOVERY_TRIAGE_AFTER_TRADING_400 新增 recovery triage batch。
`aits reports recovery-blocker-triage --as-of YYYY-MM-DD` 把 TRADING-400 的 remaining
blockers 展开为 machine-readable blocker triage；`aits reports report-index-warning-triage`
分类 unwaived report-index warnings；`aits reports recovery-pack-source-depth-audit`
检查 `sources=16/16` 是否真正 healthy / complete；`aits reports recovery-owner-action-map`
把 blockers、warnings 和 unhealthy sources 转成 owner / code / data checklist。各命令都有
对应 `validate-*` CLI。最后 `aits reports recovery-governance-rerun-after-triage --as-of
YYYY-MM-DD` 只读重跑 canonical recovery governance pack，并把 triage context 附到 pack；
不得 force healthy、不得解决 blocker、不得新增 silent waiver、不得刷新数据或补造 artifact。
当前 2026-06-17 rerun 仍为 `RECOVERY_GOVERNANCE_BLOCKED`，normal paper-shadow
`false`，extended shadow / live trading 继续 forbidden，`production_effect=none`。

TRADING-421_EXACT_BLOCKER_AND_WARNING_INVENTORY 新增逐条 blocker / warning 清单。
`aits reports exact-blocker-warning-inventory --as-of YYYY-MM-DD` 只读读取 latest recovery
blocker triage、post-recovery governance pack、source depth audit、report-index warning
triage、normal paper-shadow observation clock 和 owner action map，输出
`outputs/reports/exact_blocker_warning_inventory_YYYY-MM-DD.json/md`；对应
`aits reports validate-exact-blocker-warning-inventory --latest` 生成 validation artifact。
该 report 不只给 counts，而是逐条列出 9 个 blocker、10 个 recovery warning 和 report-index
warning 的 source artifact、upstream dependency、repair type、waiver eligibility、next action
以及 normal/extended/live boundary。它不运行上游、不刷新数据、不补造 signal/snapshot/metrics、
不创建 waiver、不写 owner decision、不触发 broker/order，`production_effect=none`。

TRADING-425_METRIC_SOURCE_MAP 新增 cost/benchmark metric 来源证明层。
`aits etf dynamic-v3-rescue metric-source-map run --as-of YYYY-MM-DD` 只读读取 latest 或
指定 `backtest_sim_outcome`、cost policy 和 cached price symbols，输出
`reports/etf_portfolio/dynamic_v3_rescue/metric_source_map/<source_map_id>/` 下的
manifest、JSON/Markdown report、Reader Brief section 和 validation。报告逐项列出 candidate
cost metrics 与 static/no-trade/QQQ/SPY/equal-weight ETF baseline metrics 的 source artifact、
required fields、missing fields 和 `derivable_now`；只有 `METRIC_SOURCE_MAP_READY` 且相关
row 可派生时，后续恢复任务才允许 materialize 对应 research metric。该层不 materialize
cost/benchmark metrics、不运行 optimization/backtest、不刷新数据、不补造 metric、不写 official
target weights、不触发 broker/order，`production_effect=none`。

TRADING-408～419_RECOVERY_BLOCKER_RESOLUTION_BATCH 继续处理 recovery blockers。
`aits reports remaining-blocker-resolution-ledger --as-of YYYY-MM-DD` 把 remaining
blockers / warnings 映射到 source、artifact、dependency、root cause、owner/code/data
action 和 normal/extended/live boundary；`aits reports report-index-warning-cleanup
--as-of YYYY-MM-DD` 对照当前 report index 区分 true blocker、stale artifact、metadata
repair 和 owner review needs，且不 silent waive；`aits reports normal-paper-shadow-observation-clock
--as-of YYYY-MM-DD` 只有在 normal resumption gate non-blocked 后才开始 normal observation
计数；`aits reports post-recovery-governance-pack --as-of YYYY-MM-DD` 汇总 latest recovery
pack、ledger、warning cleanup、clock 和 owner decision audit log。各命令都有对应
`validate-*` CLI。该 batch 不补造 signal/cost/benchmark/observation artifact、不把
`owner_action=hold` 解读为 normal observation authorization、不批准 extended shadow 或
live trading、不写 official target weights、不触发 broker/order，`production_effect=none`。

TRADING-429～438_DECISION_STAGE_REVIEW 新增 recovery blocked 之后的 owner-decision
前置治理审查层。`aits reports decision-stage-review --as-of YYYY-MM-DD` 会一次生成
exact eight-blocker decision review、normal shadow gate gap analysis、promotion blocker
after metrics review、candidate research return assessment、owner decision options packet、
owner decision dry-run、observation clock readiness plan、post-decision rerun plan、
report quality warning drilldown 和 governance status snapshot。也可用
`aits reports eight-blocker-decision-review --as-of YYYY-MM-DD` 只生成 exact blocker
review，`aits reports validate-eight-blocker-decision-review --latest` 校验必须正好列出
8 个 blocker；`aits reports owner-decision-dry-run --as-of YYYY-MM-DD --decision-option
keep_hold` 只验证 proposed owner decision record 形状和 append-only 约束。输出写入
`outputs/reports/*_YYYY-MM-DD.json/md` 并登记 report registry / Reader Brief
`Decision-Stage Governance Snapshot`。该 layer 只提供 owner review evidence 和 dry-run
validation：`owner_decision_dry_run` 固定 `would_append=false` / `real_entry_written=false`；
owner options 只列 `keep_hold`、`approve_resume_normal_shadow`、`return_to_research`
和 `reject_candidate`，并说明当前 gate 是否允许。它不 append owner decision、不生成
normal-shadow signoff、不启动 observation clock、不恢复 normal shadow、不批准 extended
shadow/live trading、不写 official target weights、不修改 candidate / paper-shadow /
production state、不触发 broker/order，`production_effect=none`。

TRADING-439～448_RETURN_TO_RESEARCH_RESET_BATCH 在 decision-stage review 后记录 owner
`return_to_research` 决策并生成 research reset artifacts。`aits reports
return-to-research-reset --as-of YYYY-MM-DD` 默认写入
`docs/decisions/TRADING-439_return_to_research_decision_YYYY-MM-DD.json`，并通过
append-only `data/governance/owner_decision_audit_log.jsonl` 追加
`owner_action=return_to_research`；如果同一 `decision_id` 已存在则复用既有记录，不重复
append。命令同时生成 transition pack、failure mode attribution、reusable evidence
extraction、hypothesis backlog、next candidate spec draft、research backfill plan、
archived candidate status update、research-cycle reset pack、final governance snapshot 和
validation artifact。`aits reports validate-return-to-research-governance-snapshot --latest`
校验 owner decision、candidate `RETURNED_TO_RESEARCH`、normal paper-shadow disabled、
extended/live forbidden、next backlog/spec 存在和 no broker/order/official target boundary。
该批次不会 reject candidate、恢复 normal shadow、批准 extended shadow/live trading、创建
official target weights、修改 candidate/paper-shadow/production state 或触发 broker/order；
`next_candidate_spec_draft` 只是 research-only spec。

TRADING-449～459_NEXT_RESEARCH_CYCLE_BATCH 在 return-to-research reset 后生成下一轮
research-only 证据链。依次运行 `aits reports next-research-cycle-intake --as-of
YYYY-MM-DD`、`next-candidate-spec-freeze`、`next-candidate-backfill`、
`next-candidate-stress-review`、`next-candidate-cost-benchmark-review`、
`next-candidate-vs-returned-candidate-comparison`、
`next-candidate-signal-robustness-review`、
`next-candidate-overfit-window-sensitivity`、`next-candidate-research-gate`、
`next-candidate-owner-research-review-packet` 和
`next-candidate-research-cycle-snapshot`，并用对应 `validate-next-*` 命令生成
validation artifacts。TRADING-451 backfill 在解释指标前会调用同一 validate-data code
path，并把 quality gate status 写入 artifact；如果缺 executable next-candidate
signal/weight binding，backfill 和下游 gate 必须 fail closed 为
`CANDIDATE_BACKFILL_NEEDS_EXECUTABLE_BINDING` / `NEEDS_MORE_EVIDENCE` /
`NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE`，不得补造收益、drawdown、turnover、
benchmark 或 cost-survival 结论。该批次不创建 paper-shadow candidate、不恢复 normal
shadow、不批准 extended/live、不写 official target weights、不 append owner decision、不修改
strategy/candidate/paper-shadow/production state、不触发 broker/order；
Reader Brief 只读展示 `next_candidate_research_cycle_snapshot`。

TRADING-380_CANDIDATE_REJECTION_POSTMORTEM_TEMPLATE 新增 candidate rejection postmortem
template。`aits reports candidate-rejection-postmortem-template --as-of YYYY-MM-DD`
只读读取同日 report index 中的 latest promotion board、owner decision audit log、monthly
review pack 和 safety audit，并可选 `--postmortem-json-path <filled.json>` 校验 owner 已填写
postmortem；默认无 filled record 时只输出空模板，不补造 rejection。输出
`outputs/reports/candidate_rejection_postmortem_template_YYYY-MM-DD.json/md` 和 validation
artifact。必需 sections 包括 candidate summary、reason for rejection、failed evidence gates、
failed stress scenarios、data quality issues、safety boundary issues、whether idea can be
revisited 和 lessons learned。该 template 不拒绝候选、不修改 candidate / paper-shadow /
production state，不生成 official target weights、order ticket 或 broker action；filled record
缺 required section、revisit decision、lessons learned 或带 state/broker/order mutation flag 时
fail closed。

TRADING-399_DECISION_SNAPSHOT_LIFECYCLE_POLICY 新增 decision snapshot lifecycle policy。
`aits reports decision-snapshot-lifecycle-policy --as-of YYYY-MM-DD` 只读检查 canonical
`data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`、同日 report index
和 latest available decision snapshot，输出
`outputs/reports/decision_snapshot_lifecycle_policy_YYYY-MM-DD.json/md`；
`aits reports validate-decision-snapshot-lifecycle-policy --latest` 输出 validation artifact。
Status 限定为
`SNAPSHOT_AVAILABLE|SNAPSHOT_NOT_DUE|SNAPSHOT_MISSING_BLOCKING|SNAPSHOT_MISSING_NON_BLOCKING`：
canonical snapshot 存在且日期一致时 available；未来日期或 NYSE 休市日缺失为 not due；
交易日到期且缺当日 snapshot 为 blocking；只有显式 `--allow-latest-context` 才可把 latest
available snapshot 披露为受限 reader context，且不能冒充当日 snapshot。该 policy 不运行
`score-daily`、不刷新数据、不创建/复制/回填缺失 snapshot、不修改 production state，
不批准 paper-shadow、extended shadow、official target weights、order ticket、broker action
或 live trading。

TRADING-396_EXTENDED_SHADOW_OBSERVATION_CLOCK 新增 extended-shadow minimum observation
period clock。`aits reports extended-shadow-observation-clock --as-of YYYY-MM-DD`
只读读取同日 report index 中的 paper-shadow weekly review 和 promotion board evidence，
输出 `outputs/reports/extended_shadow_observation_clock_YYYY-MM-DD.json/md`；`aits reports
validate-extended-shadow-observation-clock --latest` 输出 validation artifact。Clock status
限定为 `OBSERVATION_PERIOD_UNMET|OBSERVATION_PERIOD_PARTIAL|OBSERVATION_PERIOD_MET`，
披露 observation start date、complete / missing / invalid days、invalid reasons 和
current / required count。缺可审计 observation days 必须保持 unmet，不补造 observation dates，
也不批准 extended shadow、live trading、official target、order ticket、broker action 或
production mutation。

TRADING-381_EXTENDED_SHADOW_PROTOCOL 新增 extended shadow protocol eligibility report。
`aits reports extended-shadow-protocol --as-of YYYY-MM-DD` 只读读取同日 report index 和
既有 promotion board、evidence staleness monitor、weekly review、readiness、safety audit、
cost sensitivity、benchmark baseline、owner decision audit log、lineage graph 与
extended-shadow observation clock，输出
`outputs/reports/extended_shadow_protocol_YYYY-MM-DD.json/md`。`aits reports
validate-extended-shadow-protocol --latest` 输出 validation artifact。Protocol 严格要求 no
blocking stale data、no unresolved safety warnings、stable weekly review coverage、acceptable
cost sensitivity、benchmark comparison available、owner review complete、lineage graph
available，并通过 observation clock 满足 TRADING-350 pilot baseline minimum observation
period `20` trading days。TRADING-397 recovery rerun 后，该 report 可输出
`EXTENDED_SHADOW_BLOCKED|EXTENDED_SHADOW_NOT_READY|EXTENDED_SHADOW_REVIEW_REQUIRED|EXTENDED_SHADOW_ELIGIBLE`：
hard source blocker 保持 blocked，只有 observation-only gap 才是 not ready，warning-only
evidence 是 review required。即使 eligible 也只表示 owner 可继续复核 extended paper-shadow
observation plan，不是 live trading 或 official allocation approval；不运行上游、不刷新数据、
不补造 observation / owner / metrics，不修改 candidate / paper-shadow / production state，
不生成 official target weights、order ticket 或 broker action。

TRADING-382_RESEARCH_ROADMAP_DASHBOARD 新增只读 research roadmap dashboard。
`aits reports research-roadmap-dashboard --as-of YYYY-MM-DD` 聚合
`docs/task_register.md`、`docs/task_register_completed.md`、同日 report index 和 latest
monthly review pack、promotion board、extended shadow protocol、candidate rejection
postmortem template、safety audit、lineage graph 与 task-register consistency artifact，输出
`outputs/reports/research_roadmap_dashboard_YYYY-MM-DD.json/md`。
`aits reports validate-research-roadmap-dashboard --latest` 输出 validation artifact。Dashboard
状态限定为 `ROADMAP_HEALTHY|ROADMAP_WARNINGS|ROADMAP_BLOCKED`，披露 active /
completed tasks、open blockers、stale / missing artifacts、active candidates、latest
paper-shadow / data governance / safety / lineage status 和 next recommended tasks。该 report
只做 manual roadmap review 输入，不自动调度任务、不修改 task register、candidate、
paper-shadow 或 production state，不运行上游、不刷新数据、不补造 artifacts，不生成 official
target weights、order ticket 或 broker action。

TRADING-384_RESEARCH_GOVERNANCE_END_TO_END_PACK 新增只读 research governance
end-to-end pack。`aits reports research-governance-end-to-end-pack --as-of YYYY-MM-DD`
聚合同日 report index 和 latest task-register consistency、waiver inventory、Reader Brief
consistency、safety audit、production boundary scan、owner review template、owner decision
audit log、monthly review pack、promotion board、extended shadow protocol、roadmap dashboard
和 artifact lineage graph，输出
`outputs/reports/research_governance_end_to_end_pack_YYYY-MM-DD.json/md`。
`aits reports validate-research-governance-end-to-end-pack --latest` 输出 validation
artifact。Overall status 限定为
`GOVERNANCE_HEALTHY|GOVERNANCE_HEALTHY_WITH_WARNINGS|GOVERNANCE_MANUAL_REVIEW_REQUIRED|GOVERNANCE_BLOCKED`，
并披露 source coverage、validation pass/warning/fail counts、top blockers 和 next actions。
该 pack 只做 manual governance review 输入，不运行上游、不刷新数据、不补造 owner
decision / evidence / metrics，不修改 task / strategy / candidate / paper-shadow /
production state，不生成 official target weights、order ticket 或 broker action。

TRADING-363_RESEARCH_SAFETY_BOUNDARY_AUDIT 新增 recurring research safety boundary audit。
`aits reports research-safety-boundary-audit --as-of YYYY-MM-DD` 只读读取同日 report index、
task registers 和既有 report artifacts，检查 task scope 与 artifact metadata 是否仍保持
no official target weights、no broker/order、no production mutation、manual-review-only。
输出 `outputs/reports/research_safety_boundary_audit_YYYY-MM-DD.json/md`；`aits reports
validate-research-safety-boundary --latest` 输出
`research_safety_boundary_validation_YYYY-MM-DD.json/md`。Unsafe positive signal 必须
`SAFETY_BLOCKED` fail closed；legacy artifact missing safety metadata 只作为可见 warning，
不重写历史 artifact、不运行上游、不刷新数据、不补造 report。Reader Brief 只读展示 latest
safety status、shadow continuation readiness input 和 future promotion board input。

TRADING-353_PAPER_SHADOW_WEEKLY_REVIEW 在 daily observation 与 drift monitor 之后新增
manual weekly review layer。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-weekly-review build`、
`aits etf dynamic-v3-rescue paper-shadow-weekly-review report --latest` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-weekly-review --weekly-review-id <weekly_review_id>`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_weekly_review/`，包含 weekly
manifest、review JSON、Markdown report、Reader Brief section 和 validation JSON/Markdown，
并登记 report registry / latest pointer / Reader Brief 摘要。Weekly review 只读取既有
paper-shadow daily observations、drift monitors、formal research method contract 和 candidate
decision ledger，并读取 latest signal input completeness summary，输出 signal / recommendation / turnover / drawdown / flip-rotation /
benchmark stability、missing inputs、`weekly_decision=CONTINUE|WATCH|RETURN_TO_RESEARCH|REJECT`
和 machine-readable decision policy。TRADING-353A 进一步输出 coverage sufficiency：
`selected_window_start/end`、`expected_market_days`、`covered_market_days`、
`missing_market_days`、`coverage_ratio`、`coverage_classification`、
`coverage_safe_for_continuation` 和 `coverage_status`；一日 recovery artifact 会标记为
`RECOVERY_MODE_REVIEW`，不会被静默当成 full weekly review。默认只有
`FULL_WEEK_REVIEW` 支持 continuation；partial/recovery continuation 必须显式记录
manual coverage override reason。所有输出固定
`paper_shadow_weekly_review_only=true`、`read_only_review=true`、
`data_downloaded_by_review=false`、`pipelines_executed_by_review=false`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`order_ticket_generated=false`、`paper_account_state_mutated=false`、`auto_apply=false`、
`production_effect=none`；不得刷新数据、重跑 daily/drift 上游、修改 candidate decision
ledger、生成 official target weights、order ticket、broker action 或 production mutation。

TRADING-349_CANDIDATE_DECISION_LEDGER 在 formal contract 和 paper-shadow protocol
之后记录 append-only candidate decision ledger。CLI 入口为
`aits etf dynamic-v3-rescue candidate-decision-ledger record`、
`aits etf dynamic-v3-rescue candidate-decision-ledger report --latest` 和
`aits etf dynamic-v3-rescue validate-candidate-decision-ledger --ledger-run-id <ledger_run_id>`。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/candidate_decision_ledger/`，canonical JSONL
ledger 为 `candidate_decision_ledger.jsonl`；每次 record 还会写出 run-specific manifest、
record JSON、ledger snapshot、Markdown report、Reader Brief section 和 validation JSON/Markdown，
并登记 report registry / latest pointer / Reader Brief 摘要。Ledger 记录 candidate id、
evidence status、stress / mismatch / rotation / A/B result、confirmation count、owner action、
final decision 和 next required action；它只记录人工复核轨迹，不是 owner approval、
paper account initializer、official target weights、broker/order workflow 或 production
promotion。所有输出固定 `candidate_decision_ledger_only=true`、`append_only_ledger=true`、
`manual_review_only=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-354_EVIDENCE_STALENESS_MONITOR 在 candidate decision ledger 后新增只读 freshness
guard。CLI 入口为 `aits etf dynamic-v3-rescue evidence-staleness-monitor run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue evidence-staleness-monitor report --latest` 和
`aits etf dynamic-v3-rescue validate-evidence-staleness-monitor --monitor-id <monitor_id>`。
Freshness thresholds 由
`config/etf_portfolio/dynamic_v3_rescue/evidence_staleness_policy_v1.yaml` 管理，覆盖
price data、market panel、signal artifact、stress backfill result、A/B review、owner review、
paper-shadow daily observation、paper-shadow drift monitor 和 paper-shadow weekly review。
TRADING-354B 明确 price data 与 market panel 的 freshness age 使用
`freshness_reference_date`。TRADING-366 将该日期计算提升为共享
`resolve_us_equity_market_freshness` policy，按 U.S. equity calendar 区分 normal trading day、
weekend、U.S. market holiday、partial trading day 和 data vendor delay window：closed-market
requested dates 使用上一交易日，未完成或 vendor-ready buffer 内的交易日使用 latest completed
market date。Signal、stress、A/B、owner review 和 paper-shadow artifacts 仍按 requested as-of 的
research freshness 口径评估。CLI summary、manifest、report、Markdown 和 Reader Brief 会披露
`requested_as_of`、`freshness_reference_date`、`latest_complete_market_date`、
`market_calendar_status`、`market_session_kind`、`calendar_adjustment_reason`、
`calendar_adjusted_staleness` 和 per-source `stale_reason`，避免 pre-close / weekend / holiday
local calendar date 把已覆盖 latest complete market session 的 price / market panel 误判为
stale，同时 source timestamp 早于 `freshness_reference_date` 时仍 fail-closed。TRADING-353A
后，monitor 也读取 latest `paper_shadow_weekly_review` 的 `coverage_status` 和
`coverage_safe_for_continuation`；coverage 不足时 freshness status 可以保持 ACCEPTABLE，但
`safe_to_continue_shadow=false`，`next_refresh_action` 会要求 full weekly review 或显式 manual
coverage override。
Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/evidence_staleness_monitor/`，包含 manifest、
report JSON、findings JSONL、Markdown report、Reader Brief section 和 validation JSON/Markdown。
输出 `evidence_freshness_status`、`stale_artifacts`、`blocking_artifacts`、
`missing_artifacts`、`next_refresh_action`、`safe_to_continue_shadow` 和
`safety_boundary_status`；该 monitor 不刷新数据、不运行 market panel 或 research 上游、不修改
candidate decision ledger、不写 target weights、不触发 broker/order，所有输出固定
`evidence_staleness_monitor_only=true`、`data_downloaded_by_monitor=false`、
`pipelines_executed_by_monitor=false`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-354C_SHADOW_CONTINUATION_READINESS 在 staleness monitor 后新增单一 paper-shadow
continuation readiness answer。CLI 入口为
`aits etf dynamic-v3-rescue shadow-continuation-readiness run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue shadow-continuation-readiness report --latest` 和
`aits etf dynamic-v3-rescue validate-shadow-continuation-readiness --latest`。该报告只读
latest paper-shadow daily observation、drift monitor、weekly review、evidence staleness
monitor、latest data quality report 和可用 source safety boundary payloads，输出
`READY_TO_CONTINUE`、`READY_WITH_WARNINGS`、`MANUAL_REVIEW_REQUIRED`、
`BLOCKED_MISSING_ARTIFACTS`、`BLOCKED_STALE_DATA` 或 `BLOCKED_SAFETY_BOUNDARY`，
并显式披露 `safe_to_continue_shadow`、`missing_artifacts`、`blocking_artifacts`、
`stale_artifacts`、`coverage_status`、`manual_review_required`、`next_required_action`、
`data_validation_status` 和 `safety_boundary_status`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/shadow_continuation_readiness/`，包含 manifest、
report JSON、Markdown report、Reader Brief section 和 validation JSON/Markdown。该 readiness
report 不运行 `aits validate-data`、不刷新数据、不重跑上游、不修改 candidate ledger、不写
official target weights、不触发 broker/order、不修改 paper account 或 production state，所有输出固定
`shadow_continuation_readiness_only=true`、`advisory_only=true`、
`data_downloaded_by_readiness=false`、`pipelines_executed_by_readiness=false`、
`not_official_target_weights=true`、`broker_action_allowed=false`、`production_effect=none`。

TRADING-383_CANONICAL_PAPER_SHADOW_HEALTH_RERUN 在 fallback/cache/signal/staleness/readiness
链路后新增 canonical paper-shadow health 聚合。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-health run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue paper-shadow-health report --latest` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-health --latest`。该 report 只读 latest
data source fallback policy、cache catalog、signal input completeness、paper-shadow daily
observation、drift monitor、weekly review、evidence staleness monitor、shadow continuation
readiness、data refresh audit、price cache 和 market panel artifacts，输出
`HEALTHY`、`HEALTHY_WITH_WARNINGS`、`MANUAL_REVIEW_REQUIRED`、`BLOCKED_DATA`、
`BLOCKED_SIGNAL_INPUTS`、`BLOCKED_DRIFT` 或 `BLOCKED_SAFETY`，并披露
`safe_to_continue_shadow`、data/signal/fallback/cache/weekly/drift/readiness/data-refresh
statuses、blocking reasons、warnings、next required action、Reader Brief section 和 validation
artifact。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_health/`。该 health rerun 不刷新数据、
不补造 source artifact、不重跑上游、不写 official target weights、不触发 broker/order、不修改
paper account 或 production state，所有输出固定 `paper_shadow_health_check_only=true`、
`read_only_health_aggregation=true`、`data_downloaded_by_health_check=false`、
`pipelines_executed_by_health_check=false`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-387_READINESS_AND_HEALTH_RECOVERY_CHAIN 在 signal input recovery 后新增最小
readiness/health 恢复链。CLI 入口为
`aits etf dynamic-v3-rescue readiness-health-recovery run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue readiness-health-recovery report --latest` 和
`aits etf dynamic-v3-rescue validate-readiness-health-recovery --latest`。该 chain 只重跑
evidence staleness monitor、shadow continuation readiness 和 canonical paper-shadow health，
输出 `PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION`、
`PAPER_SHADOW_STILL_BLOCKED` 或 `MANUAL_REVIEW_REQUIRED`，并披露 source artifact ids、
source statuses、validation statuses、blocking reasons、warning reasons、Reader Brief section
和 validation artifact。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/readiness_health_recovery/`。该恢复链只判断
normal paper-shadow observation 是否可恢复，不运行 promotion board、不执行 extended-shadow
protocol、不写 official target weights、不触发 broker/order、不修改 paper account 或 production
state；所有输出固定 `readiness_health_recovery_chain_only=true`、
`normal_paper_shadow_observation_gate_only=true`、`promotion_board_allowed=false`、
`extended_shadow_allowed=false`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-388_NORMAL_PAPER_SHADOW_RESUMPTION_GATE 在 readiness/health recovery 后新增显式
normal paper-shadow resumption gate。CLI 入口为
`aits etf dynamic-v3-rescue normal-paper-shadow-resumption-gate run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue normal-paper-shadow-resumption-gate report --latest` 和
`aits etf dynamic-v3-rescue validate-normal-paper-shadow-resumption-gate --latest`。该 gate
只读取 latest 或指定的 readiness/health recovery artifact 与 owner decision evidence，输出
`RESUME_NORMAL_SHADOW_ALLOWED`、`RESUME_NORMAL_SHADOW_WITH_WARNINGS` 或
`RESUME_NORMAL_SHADOW_BLOCKED`，并披露 resumption requirements、owner action、manual owner
review completion、blocking/warning reasons、Reader Brief section 和 validation artifact。
`hold` / `keep_hold` 是允许的 safe non-promotion owner action，但不授权恢复；只有
`continue_normal_shadow` 或 `approve_resume_normal_shadow` 且人工 owner review completed
才可能让 normal paper-shadow observation 恢复。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/normal_paper_shadow_resumption_gate/`。该 gate 只允许
normal observation，不运行 promotion board、不执行 extended-shadow protocol、不批准 live
trading、不写 official target weights、不触发 broker/order、不修改 paper account 或 production
state；所有输出固定 `normal_paper_shadow_resumption_gate_only=true`、
`normal_paper_shadow_observation_only=true`、`promotion_board_allowed=false`、
`extended_shadow_allowed=false`、`live_trading_allowed=false`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-373_PAPER_SHADOW_OUTCOME_ATTRIBUTION 在 paper-shadow weekly review 和 canonical
health 后新增 research-only outcome attribution layer。Policy config 为
`config/etf_portfolio/dynamic_v3_rescue/paper_shadow_outcome_attribution_v1.yaml`，
披露 driver taxonomy、HIGH/MEDIUM/LOW/UNKNOWN confidence policy、owner/version、rationale
和 review condition。CLI 入口为
`aits etf dynamic-v3-rescue paper-shadow-outcome-attribution run`、
`aits etf dynamic-v3-rescue paper-shadow-outcome-attribution report --latest` 和
`aits etf dynamic-v3-rescue validate-paper-shadow-outcome-attribution --latest`。该 report
只读 paper-shadow weekly review 和 optional paper-shadow health context，把 weekly outcome
归因到 market move、signal change、regime transition、data stale warning、fallback source
used、signal input incompleteness、drift warning、weekly coverage warning 和 manual owner
decision，输出 dominant driver、confidence、active/unknown driver counts、Reader Brief
section 和 validation artifact；缺 weekly review 时 fail closed 为
`BLOCKED_MISSING_WEEKLY_REVIEW`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_outcome_attribution/`。该 attribution
layer 不重算 weekly decision、signal、regime、performance 或 health，不刷新数据、不补造
upstream artifact、不写 official target weights、不触发 broker/order、不修改 paper account 或
production state，所有输出固定 `research_only=true`、`outcome_attribution_only=true`、
`read_only_attribution=true`、`weekly_decision_mutated=false`、
`data_downloaded_by_attribution=false`、`pipelines_executed_by_attribution=false`、
`not_official_target_weights=true`、`broker_action_allowed=false`、
`production_effect=none`。

TRADING-374_SHADOW_DECISION_COMPARISON 在 paper-shadow continuation readiness 后新增
current-vs-previous decision comparison。CLI 入口为
`aits etf dynamic-v3-rescue shadow-decision-comparison run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue shadow-decision-comparison report --latest` 和
`aits etf dynamic-v3-rescue validate-shadow-decision-comparison --latest`。该 report 只读
current 和 previous shadow continuation readiness artifacts，并在可解析时引用 weekly/health
context，比较 `safe_to_continue_shadow`、readiness status、weekly decision、drift severity、
stale/missing artifacts、signal input completeness、fallback status 和 safety boundary status。
输出 `decision_changed`、`change_reason`、`previous_state`、`current_state`、
`recommended_owner_action`，并把变化分类为 `NO_CHANGE`、`IMPROVED`、`DETERIORATED`、
`BLOCKED` 或 `RECOVERED`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/shadow_decision_comparison/`。该 comparison 不重算
weekly decision、readiness、health、signal、regime 或 performance，不刷新数据、不补造 upstream
artifact、不写 official target weights、不触发 broker/order、不修改 paper account 或 production
state，所有输出固定 `research_only=true`、`shadow_decision_comparison_only=true`、
`read_only_comparison=true`、`decision_mutated=false`、
`data_downloaded_by_comparison=false`、`pipelines_executed_by_comparison=false`、
`not_official_target_weights=true`、`broker_action_allowed=false`、`production_effect=none`。

TRADING-359_COST_SENSITIVITY_REVIEW 在 paper-shadow weekly/health 后新增 research-only
transaction-cost sensitivity review。Policy config 为
`config/etf_portfolio/dynamic_v3_rescue/cost_sensitivity_review_v1.yaml`，披露
zero/low/medium/high cost assumptions、meaningful improvement threshold、owner/version、
rationale 和 review condition。CLI 入口为
`aits etf dynamic-v3-rescue cost-sensitivity-review run --candidate-metrics-path <metrics.json>`、
`aits etf dynamic-v3-rescue cost-sensitivity-review report --latest` 和
`aits etf dynamic-v3-rescue validate-cost-sensitivity-review --latest`。该 report 只读
candidate metrics、paper-shadow weekly review 和 paper-shadow health context，输出 turnover、
cost drag、gross performance proxy、net performance proxy、每个 scenario 是否仍 meaningful、
aggregate `cost_sensitivity_status`、promotion-board input summary、Reader Brief section 和
validation artifact。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/cost_sensitivity_review/`。该 review 不接 broker、
不生成真实 execution model、不刷新数据、不补造 upstream artifact、不写 official target weights、
不修改 paper account 或 production state，所有输出固定 `research_only=true`、
`cost_sensitivity_review_only=true`、`execution_model_ready=false`、
`broker_action_allowed=false`、`order_ticket_generated=false`、`not_official_target_weights=true`、
`production_effect=none`。

TRADING-389_COST_SENSITIVITY_METRICS_MATERIALIZATION 在 cost sensitivity review 前新增
numeric input materialization。CLI 入口为
`aits etf dynamic-v3-rescue cost-sensitivity-metrics-materialization run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue cost-sensitivity-metrics-materialization report --latest` 和
`aits etf dynamic-v3-rescue validate-cost-sensitivity-metrics-materialization --latest`。该
artifact 默认读取 latest `backtest_sim_outcome` 的 `simulated_variant_summary.json`，把
`turnover`、`gross_performance_proxy`、`baseline_performance_proxy`、
`gross_improvement_proxy`、`drawdown_proxy` 和 `trade_rotation_count` 写入
`candidate_cost_metrics.json`，然后把该 JSON 交给既有 cost-sensitivity review 重新运行。
当前 governance candidate `median_plus_regime_mismatch_filter` 与 source numeric row
`limited_adjustment` 的映射必须显式披露；status 限定为
`COST_INPUTS_AVAILABLE|COST_INPUTS_PARTIAL|INSUFFICIENT_COST_INPUTS`。该 materialization
只使用既有 research artifacts，不发明 metrics、不运行新 optimization/backtest、不生成
execution model、不写 official target weights、不触发 broker/order、不修改 paper account 或
production state；`BACKTEST_SIMULATION` metrics 只能作为 research cost review input，不是
PIT/live performance 或 promotion approval。

TRADING-390_BENCHMARK_BASELINE_METRICS_MATERIALIZATION 在 benchmark baseline control 前新增
candidate / baseline numeric input materialization。CLI 入口为
`aits etf dynamic-v3-rescue benchmark-baseline-metrics-materialization run --as-of YYYY-MM-DD`、
`aits etf dynamic-v3-rescue benchmark-baseline-metrics-materialization report --latest` 和
`aits etf dynamic-v3-rescue validate-benchmark-baseline-metrics-materialization --latest`。该
artifact 默认读取 latest 或指定 `backtest_sim_outcome`、TRADING-389
`candidate_cost_metrics.json`、cost-sensitivity review，并在同一 `aits validate-data` 质量门禁
通过后使用 cached prices 生成 `static_allocation`、`no_trade`、`qqq_only`、`spy_only` 和
`equal_weight_etf` baseline metrics。`equal_weight_etf` 必须由 SPY/QQQ/SMH/SOXX equal weight
和同一 simulation event windows 重新计算，不能用 shadow candidate group 代替；status 限定为
`BASELINE_METRICS_AVAILABLE|BASELINE_METRICS_PARTIAL|INSUFFICIENT_BASELINE_METRICS`。该流程只为
benchmark baseline control 提供 research-only inputs 并重跑 existing control pack，不把
benchmark comparison 当作 live allocation signal，不刷新数据、不优化策略、不写 official target
weights、不触发 broker/order、不修改 paper account 或 production state。

TRADING-370_BENCHMARK_BASELINE_CONTROL_PACK 在 paper-shadow weekly 和 cost-sensitivity
后新增 research-only benchmark baseline controls。Policy config 为
`config/etf_portfolio/dynamic_v3_rescue/benchmark_baseline_control_v1.yaml`，披露
static allocation、no-trade、QQQ-only、SPY-only、equal-weight ETF baselines 的 baseline id、
asset universe、rebalancing assumption、cost assumption、applicability、limitations、
owner/version、rationale 和 review condition。CLI 入口为
`aits etf dynamic-v3-rescue benchmark-baseline-control run --candidate-metrics-path <metrics.json> --baseline-metrics-path <baselines.json>`、
`aits etf dynamic-v3-rescue benchmark-baseline-control report --latest` 和
`aits etf dynamic-v3-rescue validate-benchmark-baseline-control --latest`。该 pack 只读
candidate metrics、baseline metrics、paper-shadow weekly review 和 cost-sensitivity review，
输出 candidate-vs-baseline comparison summary、monthly review pack inputs、Reader Brief
section 和 validation artifact；缺 numeric metrics 时 fail closed 为
`INSUFFICIENT_BASELINE_METRICS`。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/benchmark_baseline_control/`。该 control pack
不运行 backtest、不刷新数据、不补造 metrics、不生成 execution model、不写 official target
weights、不修改 paper account 或 production state，所有输出固定 `research_only=true`、
`benchmark_control_pack_only=true`、`execution_model_ready=false`、
`broker_action_allowed=false`、`order_ticket_generated=false`、
`not_official_target_weights=true`、`production_effect=none`。

TRADING-372_CANDIDATE_REGRESSION_REPLAY 在 benchmark baseline control 后新增
research-only candidate behavior regression guard。Policy config 为
`config/etf_portfolio/dynamic_v3_rescue/candidate_regression_replay_v1.yaml`，披露
AI-after-ChatGPT fixed replay window、stored expected behavior、expected output/decision/schema/
safety/Reader Brief fields、acceptable-change policy、owner/version、rationale 和 review
condition。CLI 入口为
`aits etf dynamic-v3-rescue candidate-regression-replay run --current-behavior-path <behavior.json>`、
`aits etf dynamic-v3-rescue candidate-regression-replay report --latest` 和
`aits etf dynamic-v3-rescue validate-candidate-regression-replay --latest`；省略
`--current-behavior-path` 时只读 latest benchmark baseline control artifact。该 report 输出
changed outputs、changed decisions、changed safety metadata、changed artifact schema、
changed Reader Brief fields、acceptable/breaking classification、Reader Brief section 和
validation artifact；breaking change fail closed 为 `BREAKING_CHANGE_DETECTED`。Runtime artifacts
写入 `reports/etf_portfolio/dynamic_v3_rescue/candidate_regression_replay/`。该 replay guard
不优化 strategy behavior、不运行 backtest、不刷新数据、不补造 upstream artifact、不写 official
target weights、不修改 paper account 或 production state，所有输出固定 `research_only=true`、
`regression_guard_only=true`、`strategy_behavior_changed=false`、
`data_downloaded_by_replay=false`、`pipelines_executed_by_replay=false`、
`broker_action_allowed=false`、`order_ticket_generated=false`、
`not_official_target_weights=true`、`production_effect=none`。

TRADING-356_STRESS_SCENARIO_LIBRARY 在 evidence freshness guard 后新增 Dynamic v3 rescue
专用 stress scenario library。CLI 入口为
`aits etf dynamic-v3-rescue stress-scenario-library report`、`... report --latest` 和
`aits etf dynamic-v3-rescue validate-stress-scenario-library --library-run-id <library_run_id>`。
Scenario selection policy 由
`config/etf_portfolio/dynamic_v3_rescue/stress_scenario_library_v1.yaml` 管理，覆盖
rapid drawdown、slow drawdown、V-shaped recovery、high volatility sideways market、
false risk-off cluster、rate shock、AI sector correction、semiconductor-led selloff 和
liquidity squeeze。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/stress_scenario_library/`，包含 manifest、
normalized `stress_scenario_library.json`、Reader Brief section、Markdown report 和
validation JSON/Markdown。该 library 只定义 candidate validation scenarios、selection
rationale、expected failure modes 和 required evidence，不运行 stress backfill、不刷新数据、
不修改 candidate ledger、不写 target weights、不触发 broker/order，所有输出固定
`stress_scenario_library_only=true`、`candidate_validation_only=true`、
`data_downloaded_by_library=false`、`pipelines_executed_by_library=false`、
`not_probability_forecast=true`、`not_official_target_weights=true`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-357_DRAWDOWN_EVENT_CASEBOOK 在 stress scenario library 后新增 Dynamic v3 rescue
历史 drawdown event casebook。CLI 入口为
`aits etf dynamic-v3-rescue drawdown-event-casebook report`、`... report --latest` 和
`aits etf dynamic-v3-rescue validate-drawdown-event-casebook --casebook-run-id <casebook_run_id>`。
Casebook schema 由
`config/etf_portfolio/dynamic_v3_rescue/drawdown_event_casebook_v1.yaml` 管理，每个事件必须记录
event name、start/end date、max drawdown、recovery behavior、regime label、candidate
response、benchmark response 和 review notes。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/drawdown_event_casebook/`，包含 manifest、
normalized `drawdown_event_casebook.json`、Reader Brief section、Markdown report 和
validation JSON/Markdown。该 casebook 是 qualitative research diagnostic artifact，不是
trading signal、data-backed performance evidence、stress/backtest executor 或 production
gate；它不下载数据、不运行上游 pipeline、不修改 candidate ledger、不写 target weights、
不触发 broker/order，所有输出固定 `drawdown_event_casebook_only=true`、
`research_diagnostic_only=true`、`not_trading_signal=true`、
`data_downloaded_by_casebook=false`、`pipelines_executed_by_casebook=false`、
`broker_action_allowed=false`、`production_effect=none`。

TRADING-358_FLIP_ROTATION_EVENT_CASEBOOK 在 drawdown event casebook 后新增 Dynamic v3
rescue signal flip / rotation event casebook。CLI 入口为
`aits etf dynamic-v3-rescue flip-rotation-event-casebook report`、`... report --latest` 和
`aits etf dynamic-v3-rescue validate-flip-rotation-event-casebook --casebook-run-id <casebook_run_id>`。
Casebook schema 由
`config/etf_portfolio/dynamic_v3_rescue/flip_rotation_event_casebook_v1.yaml` 管理，每个事件必须记录
date、previous state、new state、trigger signal、useful/false-positive classification、
turnover impact 和 candidate behavior，并输出 useful count、false-positive count、
dominant trigger 和 next action。Runtime artifacts 写入
`reports/etf_portfolio/dynamic_v3_rescue/flip_rotation_event_casebook/`，包含 manifest、
normalized `flip_rotation_event_casebook.json`、Reader Brief section、Markdown report 和
validation JSON/Markdown。该 casebook 是 qualitative research diagnostic artifact，不是
trading signal、data-backed signal evidence、backtest executor 或 production gate；它不下载
数据、不运行上游 pipeline、不修改 candidate ledger、不写 target weights、不触发 broker/order，
所有输出固定 `flip_rotation_event_casebook_only=true`、`research_diagnostic_only=true`、
`not_trading_signal=true`、`data_downloaded_by_casebook=false`、
`pipelines_executed_by_casebook=false`、`broker_action_allowed=false`、
`production_effect=none`。

`aits etf weight-calibration register-candidates --run-id/--latest --top N` 把 selected
historical candidates 写入 ignored
`data/etf_portfolio/weight_calibration/candidate_weight_registry.json`。`aits etf
weight-calibration enroll-forward --weight-set <id>` 或 `--latest --top N` 只把 safe
candidate 登记到
`data/etf_portfolio/weight_calibration/forward_enrollments.json`，包含 `shadow_id`、
`weight_set_id`、source search linkage、tracking state 和 `forward_tracking_link`；blocked、
rejected 或 `needs_more_data` candidate 会 fail closed。`aits etf weight-calibration
aggregate-evidence --as-of YYYY-MM-DD` 读取 candidate registry、forward enrollment
ledger、optional historical search summary 和 optional forward/weekly/journal/parameter-review
JSON，生成
`reports/etf_portfolio/weight_calibration/evidence/backtest_forward_evidence_YYYY-MM-DD.json/md`，
比较 historical expected return/drawdown/turnover/stability 与 forward realized metrics，
输出 `expectation_gap`、`drawdown_gap`、`turnover_gap`、`stability_gap` 和 evidence status。
缺少 forward row 或 forward days 不足时保持 `needs_more_forward_data`，不补造结论。`aits etf
weight-calibration overfit-diagnostics` 读取 candidate registry、optional search summary 和
optional TRADING-071F evidence，输出
`reports/etf_portfolio/weight_calibration/overfit_diagnostics/overfit_diagnostics_*.json/md`，
按 performance concentration、single-period dependency、regime fragility、turnover
instability、constraint hit instability、weight extremeness、benchmark dependency 和
forward/backtest divergence 给出 low/medium/high/critical risk bands。`aits etf
weight-calibration generate-proposals` 读取 candidate registry、optional TRADING-071F
evidence 和 optional TRADING-071G diagnostics，输出
`reports/etf_portfolio/weight_calibration/proposals/candidate_weight_proposals_*.json/md`；
proposal type 只允许 `continue_forward_observation`、`reject_weight_set`、
`defer_until_more_forward_data`、`propose_extended_shadow` 和
`propose_manual_baseline_review`，并 fail closed 阻断 `apply_weight_set`、
`promote_to_production`、`enable_broker_action`。`aits etf weight-calibration report
--latest` 或显式 artifact path 生成
`reports/etf_portfolio/weight_calibration/reports/dual_track_calibration_YYYY-MM-DD.json/md`，
汇总 search configuration、top historical candidates、walk-forward/regime robustness、
overfit diagnostics、forward evidence comparison、candidate registry status、proposal
scorecard、manual review package、source report links 和 next steps。Report registry 新增
`etf_weight_dual_track_calibration_report`；Reader Brief 的 `ETF Weight Calibration`
区块只读 latest report，展示 search pack、top historical candidate、forward evidence
status、overfit risk、candidate status、manual review proposal count、safety posture 和
detail link，缺失 report 时显示 `MISSING` 且不运行上游。当前 TRADING-071J 仍是
candidate-only / observe-only visibility 层。`aits etf weight-calibration validate`
生成
`reports/etf_portfolio/weight_calibration/validation/weight_calibration_validation_*.json/md`，
校验 weight search config、bounded search、sample historical search、walk-forward/regime
robustness、candidate registry、forward enrollment、backtest-forward aggregator、overfit
diagnostics、proposal generator、report generator、Reader Brief/report-registry visibility、
unsafe proposal type blockers、evidence-linked proposals 和 proposal-only safety fields；
失败时 fail closed。所有输出固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、
`broker_action=none`、`manual_review_required=true`，不写 official target weights、不改
baseline config、不写 shared experiment shadow registry、不触发 broker。

TRADING-074 operations schedule spec 位于
`config/etf_portfolio/operations_schedule.yaml`。它把 ETF portfolio research 的
daily / weekly / biweekly / monthly / manual-review workflow 记录为 source config，
包含 step id、command、dependencies、expected outputs、max allowed age、failure policy
和 owner review requirement，并固定 `observe_only=true`、`candidate_only=true`、
`production_effect=none`、`broker_action=none`、`manual_review_required=true`。
当前 TRADING-074A/B/C/D/E/F/G/H/I/J/K 提供 loader/validator、配置校验、daily / weekly /
biweekly / monthly operations command graph、只读 artifact freshness checker、failure
policy evaluator、owner checklist builder、scheduler dry-run、operations health report 和
Reader Brief operations health section 以及 final operations validation gate。
`build_daily_operations_command_graph` 会生成 topological `execution_order`、required /
optional 节点、输入/输出、failure policy、estimated runtime class 和 safety fields；optional
attribution 节点可跳过，required 节点不可跳过，cycle 或 missing required node 会 fail
closed。`build_weekly_operations_command_graph` 会从 weekly schedule 生成同样
dry-run-only 的 dependency-aware graph，保留 `operations_health_check` /
`forward_update` 等 daily 上游为 `external_dependencies`，并标记 weekly review、decision
journal、parameter review、watchlist、operations report 和 Reader Brief weekly navigation
的 manual-review checkpoints；pilot config 可把 parameter review 节点标成 optional 后跳过。
这些 graph 不执行命令、不自动调度 weekly/monthly 任务、不替代 `aits ops daily-run` 的统一
外部入口。`build_biweekly_operations_command_graph` 会规划 attribution scorecard review 和
weight calibration evidence update；`build_monthly_operations_command_graph` 会规划 data quality
audit、bounded historical weight search、dual-track calibration report、parameter governance、
strategy dashboard 和 monthly operations report，并把 weight search 标记为 `slow` runtime
class。`check_operations_artifact_freshness` 会从 command graph 和 schedule spec 生成
artifact metadata，解析 JSON / text / filename 中的 `generated_at` / `as_of_date`，对
`{run_id}` 这类动态占位符执行 glob resolution，并把 required stale/missing artifacts 标记为
blocking、optional missing artifacts 标记为 warning，同时把 upstream blocking 状态传播到
dependent steps。`evaluate_operations_failure_policy` 会从 freshness report 生成
`etf_operations_failure_policy_v1` 只读报告，把 missing / stale / unknown /
dependency-blocked artifacts 映射为 `info` / `warning` / `error` / `critical` severity、
policy action、pipeline/dependent-step blocking、manual review requirement 和 recommended
action；`fail_pipeline` 会阻断 pipeline，`block_dependent_steps` 会阻断下游步骤，
`skip_optional_step` 只产生 warning，`manual_review_required` 进入 owner review。
`build_operations_owner_review_checklist` 会从 `manual_review_steps` 生成
`etf_operations_owner_review_checklist_v1` 只读 checklist，把 safety boundary、cadence gate、
failure summary、blocking / warning / manual-review events 和 signoff 固化为 daily / weekly /
monthly / incident owner review template。`aits etf ops dry-run --cadence ... --as-of ...`
会生成 `etf_operations_scheduler_dry_run_v1` 只读 JSON，汇总 planned steps、execution order、
skipped optional steps、blocking failures、warnings、expected outputs、owner checklist status
和 safety；它固定 `dry_run_only=true`、`commands_executed=false`、
`production_state_mutated=false`，不执行 planned commands、不写 production state。
`aits etf ops report --cadence ... --as-of ...` 会生成
`etf_operations_health_report_v1` JSON / Markdown，默认写入
`reports/etf_portfolio/operations/<cadence>/operations_health_<date>.json/md`，展示
safety banner、run metadata、pipeline schedule、command graph summary、artifact freshness
summary、dependency status、failures / warnings、owner review checklist、expected next run
和 source artifacts；report 固定 `commands_executed=false`、
`production_state_mutated=false`，只把 dry-run/freshness/failure/checklist 状态变成可读报告。
Reader Brief 的 `Operations Health` 区块只读 report index 指向的 latest
`etf_operations_health_report_v1`，展示 status、blocking failures、warnings、stale/missing
artifacts、next owner review、safety posture 和 detailed report link；缺失 health report
时只显示 section-level `MISSING`，不运行上游、不补造状态。`aits etf ops validate --as-of ...`
会生成 `etf_operations_validation_v1` JSON / Markdown，默认写入
`reports/etf_portfolio/operations/validation/operations_validation_<date>.json/md`，复用该
配置、daily/weekly/biweekly/monthly graph、deterministic freshness probes、failure policy
report、owner checklist、dry-run report、operations health report 和 Reader Brief registry
integration，fail-closed 校验 required step missing、dependency cycle、unsafe
`production_effect` / `broker_action` / missing `manual_review_required` 和固定安全边界。

TRADING-075 data quality and staleness governance policy 位于
`config/etf_portfolio/data_quality.yaml`。`aits etf data-quality report --as-of
YYYY-MM-DD` 只读读取 ETF price cache、当前 ETF config hash/model version、
`config/report_registry.yaml` 和既有 artifacts，生成
`reports/etf_portfolio/data_quality/governance/data_quality_report_YYYY-MM-DD.json/md`，
覆盖 price freshness、missing bars / calendar coverage、return outliers、
corporate-action sanity、config/model drift、evidence completeness、validation gate
freshness、report staleness、Reader Brief links、blocking failures、warnings 和 manual
review items。Critical required findings 用于阻断 dependent research interpretation；
optional missing artifacts 只 warning。Reader Brief 的 `ETF Data Quality` 区块只读
latest `etf_data_quality_governance_report`，展示 overall status、blockers、warnings、
各 section status、detail link 和 safety posture；缺失 report 时显示 `MISSING`，不运行
上游、不补造质量结论。`aits etf data-quality validate --as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/data_quality/validation/data_quality_validation_YYYY-MM-DD.json/md`，
使用 deterministic probes 校验 policy、checker availability、report generator、Reader
Brief/report-registry integration 和 safety boundary；该 gate 不因为本地 cache 暂时 stale
而误判工程实现失败。所有 TRADING-075 输出固定 `observe_only=true`、`candidate_only=true`、
`production_effect=none`、`broker_action=none`、`manual_review_required=true`，不写
official target weights、不改 baseline config、不触发 broker。

TRADING-076 strategy evidence dashboard registry 位于
`config/etf_portfolio/evidence_dashboard.yaml`。`aits etf evidence-dashboard aggregate
--as-of YYYY-MM-DD` 只读扫描 report index 和既有 ETF research artifacts，输出
loaded / missing / stale / blocked source aggregation；`aits etf evidence-dashboard report
--as-of YYYY-MM-DD` 生成
`reports/etf_portfolio/evidence_dashboard/strategy_evidence_dashboard_YYYY-MM-DD.json/md`，
把 ETF baseline、weight calibration、forward simulation、AI confirmation / attribution、
satellite replacement / attribution、parameter review、weekly review、decision journal、
data quality、operations health 和 validation gates 汇总为 evidence cards、candidate
ranking、conflicts、data-quality overlay 和 manual review priority queue。每个 card 都保留
source module、source report path、source metric、as-of date、freshness、data quality、
validation status 和 sample count；缺失、stale、blocked 或 optional missing evidence
必须显式显示。Reader Brief 的 `Strategy Evidence Dashboard` 区块只读 latest
`etf_strategy_evidence_dashboard`，展示 overall status、strongest / weakest evidence、
blocking issues、manual review priority count、data quality status 和 detailed link；缺失时
显示 `MISSING`，不运行上游、不补造结论。`aits etf evidence-dashboard validate --as-of
YYYY-MM-DD` 生成
`reports/etf_portfolio/evidence_dashboard/validation/strategy_evidence_validation_YYYY-MM-DD.json/md`，
fail-closed 校验 schema、registry、aggregator、cards、ranking、conflict overlay、manual
queue、report generator、Reader Brief integration、traceability 和 safety boundary。所有
TRADING-076 输出固定 `observe_only=true`、`candidate_only=true`、
`production_effect=none`、`broker_action=none`、`manual_review_required=true`，不自动
promotion、不改 baseline / production weights、不触发 broker。

TRADING-077 baseline candidate review policy 位于
`config/etf_portfolio/baseline_review.yaml`。`aits etf baseline-review eligibility
--candidate <candidate_id>` 和 `matrix` 只读 strategy evidence dashboard、weight
calibration、forward evidence、parameter review、decision journal、data quality、
operations health、validation gates、AI / satellite attribution 和 source links，判断
candidate 是否可进入 owner baseline review；critical data quality、blocked evidence
dashboard、failed / stale gates、forward sample too small、missing required journal link、
parameter review blocked、unsafe production effect 或 broker action 都会 fail closed。
`aits etf baseline-review package --candidate <candidate_id>` 生成 manual review package；
`capture-decision` 捕获 owner decision；`proposal-draft` 只在
`approve_for_proposal_draft` 且 journal linkage 存在后生成 proposal-only draft；
`outcome` 追踪 review cycle；`validate` 是 final safety gate。Reader Brief 的
`Baseline Candidate Review` 区块只读 latest package / decision / proposal / outcome，
展示 eligible / needs-more / blocked counts、latest decision、proposal draft count、
safety posture 和 detail link；缺失时显示 `MISSING`，不运行上游。TRADING-077 全部输出
固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、
`broker_action=none`、`manual_review_required=true`，不写 `target_weights.csv`、不改
baseline config、不自动 promotion、不触发 broker。

`aits etf governance summary --candidate <candidate.json>` 使用
`config/etf_portfolio/governance.yaml` 的参数治理 policy 输出候选晋级摘要，固定
`production_effect=none` 且 `manual_review_required=true`。候选必须先通过测试、shadow
mode、最小样本、benchmark comparison、turnover、drawdown/no-lookahead 和 P2/live
self-promotion gate；通过时仅进入 `ELIGIBLE_FOR_MANUAL_REVIEW`，不会自动替换
`production_baseline`。
`aits etf credibility validate` 聚合 TRADING-063A~J credibility checks，输出单一
PASS/FAIL JSON/Markdown gate，覆盖 runtime artifact hygiene、benchmark suite、
no-lookahead、toy accounting、risk constraints、allocation stability、simulation schema、
backtest metrics、daily brief explainability、parameter governance 和 P2/live safety；PASS
只表示可继续 shadow evaluation，仍固定 `production_effect=none`、manual review required、
no broker action。

ETF brief 与 ETF backtest summary 已登记到 report registry / Reader Brief navigation，
Reader Brief 还会只读摘录最新 ETF backtest standardized metrics、weekly portfolio
review、AI confirmation 和 satellite replacement 摘要，便于人工下钻，但该
可见性层只读扫描已有 artifact，不运行 ETF 上游、不写 production weights、不触发
broker 或 trading action。P1/P2 当前均固定 `production_effect=none`；EDGAR 文本层只做
官方 filing cache 和受治理 topic count，不做自动财报解释；news/options/holdings
只接受本地审计输入或显式 proxy，live news vendor feed、LLM sentiment、真实 VXN/skew
vendor feed、实时 issuer holdings API 和多账户/实盘接口仍需要 owner/provider/API/PIT
policy 决策后才能升级。

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

生成并校验 source-level point-in-time source manifest：

```powershell
aits data-sources pit-manifest report --as-of 2026-05-02
aits data-sources pit-manifest validate --latest
```

该 manifest 写入 `reports/data_governance/pit_source_manifest/<manifest_id>/`，每个 source 记录 source name、retrieval time、effective date、revision risk、`STRONG_PIT|APPROX_PIT|NON_PIT|UNKNOWN`、cache path、checksum、refresh policy 和 validation policy。它只建立未来研究数据治理 contract，不修复全部 PIT 数据问题；`APPROX_PIT`、`NON_PIT` 或 `UNKNOWN` 不能自动支持 backtest、scoring、paper shadow 或 production 结论。Reader Brief 只读 report index latest artifact 展示 grade counts 和 non-strong source ids；所有输出固定 `production_effect=none`，不刷新数据、不运行下游管线、不触发 broker。

定义并校验 paper-shadow research data source fallback policy：

```powershell
aits data fallback-policy run --as-of 2026-05-02
aits data fallback-policy report --latest
aits data fallback-policy validate --latest
```

TRADING-368 新增 `config/data_source_fallback_policy.yaml` 和
`reports/data_governance/data_source_fallback_policy/<report_id>/`。Fallback 状态只允许
`PRIMARY_OK`、`FALLBACK_USED`、`FALLBACK_UNAVAILABLE` 和 `BLOCKED_NO_VALID_SOURCE`。
`FALLBACK_USED` 必须在 JSON report、Markdown、Reader Brief 和 downstream summary 中明确显示
primary source、fallback source、reason、provider、endpoint、request parameters、download
timestamp、row count、checksum、metadata status 和 safety boundary；不得把 fallback 数据静默写成
primary。`FALLBACK_UNAVAILABLE` / `BLOCKED_NO_VALID_SOURCE` 会进入 evidence
staleness / shadow continuation readiness 的 blocking data path。该报告不下载数据、不写 cache、
不运行评分/回测、不写 official target weights、不触发 broker/order，固定
`production_effect=none`。

生成并校验 paper-shadow / research cached inputs checksum catalog：

```powershell
aits data cache-catalog run --as-of 2026-05-02
aits data cache-catalog report --latest
aits data cache-catalog validate --latest
```

TRADING-369 新增 `config/cache_catalog.yaml` 和
`reports/data_governance/cache_catalog/<catalog_id>/`。Catalog 只读扫描已存在的 price、
secondary price、macro 和 market panel cache，记录 cache path、artifact id、data type、
as_of、checksum、row/column count、created/modified timestamp、source ids/names、
validation timestamp 和 refresh audit id。Missing required cache、unreadable cache 或
explicit/previous catalog checksum mismatch 会 fail closed，并在 data refresh audit、PIT
source manifest、evidence staleness、shadow continuation readiness 和 Reader Brief 中披露。
该报告不下载数据、不刷新或修复 cache、不补造缺失 cache、不绕过 `aits validate-data`、
不写 official target weights、不触发 broker/order，固定 `production_effect=none`。

为 paper-shadow 依赖的数据刷新和数据质量门禁建立统一 audit trail：

```powershell
aits validate-data --as-of 2026-05-02
aits data refresh-audit report --as-of 2026-05-02
aits data refresh-audit validate --latest
```

`validate-data` 会同步写入 `artifacts/data_refresh_audit/validation/validate_data_*.json`
sidecar，记录本次 validation gate 的 data type、source、start/end time、as-of date、
status、checksum、record count、warning count 和 error count。`refresh-audit report`
只读聚合 latest validation sidecar、既有 `artifacts/data_refresh/YYYY-MM-DD/market_data_refresh_summary.json`
和 U.S. market calendar skip reason，输出 `reports/data_governance/data_refresh_audit/<audit_id>/`。
缺少 validation sidecar 会 fail closed；该报告不刷新数据、不补造 cache、不降低
`aits validate-data` 门禁、不写 official target weights，也不触发 broker/order。Reader Brief
只读 report index latest artifact 展示 audit status、failed/skipped counts、warning/error counts
和 next action。

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

计划顺序由 `config/scheduled_tasks.yaml` 的 `daily_trading_day` 约束。交易日默认包含 `download-data`、`validate-data`、带 `--continue-on-failure` 的 `pit-snapshots fetch-fmp-forward`、`pit-snapshots build-manifest`、`pit-snapshots validate`、SEC companyfacts/metrics、FMP valuation、`score-daily`、`reports dashboard`、SEC PIT shadow observe/monitor、score change attribution、market panel、report index、documentation contract、research governance summary、Reader Brief、report quality gate、Reader Brief quality、Dynamic v3 rescue `schedule observe`、`ops health` 和 `security scan-secrets`。报告写入 `outputs/reports/daily_ops_plan_YYYY-MM-DD.md`，只检查阻断性环境变量是否非空，不输出 secret 值，也不实际调用供应商 API。缺少 `FMP_API_KEY`、`MARKETSTACK_API_KEY`、`SEC_USER_AGENT` 或默认 OpenAI 预审需要的 `OPENAI_API_KEY` 时，计划状态会显示 `BLOCKED_ENV`；其中 `FMP_API_KEY` 会因 `download-data`、PIT 估值快照刷新等默认步骤而阻断，PIT 抓取自身失败则进入失败报告和 pipeline health 告警。如需把它作为调度前门禁，可加 `--fail-on-missing-env`。若离线排查需要跳过 OpenAI 预审、SEC fundamentals、估值快照刷新或 PIT 抓取，必须显式传入对应 `--skip-*` 选项，后续日报和运行记录仍需声明该限制。

`daily-plan` 和 `daily-run` 会先判断 U.S. equity market session。周末或 NYSE 常规整日休市日会进入 `CLOSED_MARKET` 模式：报告声明休市原因和上一交易日；如果主价格和 Marketstack 第二行情源已覆盖上一交易日，则跳过 `download-data`，否则只用上一交易日作为 `download-data --end`；默认跳过 `score-daily` 和 dashboard，不生成新的日报评分、decision snapshot、evidence bundle、prediction ledger 行、执行动作或网页展示层。休市日仍会运行官方政策/地缘来源抓取、PIT、SEC、valuation、Dynamic v3 rescue `schedule observe` closed-market audit、`ops health --non-trading-day` 和 secret scan，用于风险线索、research gate 状态和缓存健康复核。

未显式传入 `--as-of` 时，`daily-plan` 和 `daily-run` 默认按 `America/New_York` 的 U.S. equity market 日历选择最新已完成交易日：常规交易日美东 16:30 之后使用当日，16:30 前、周末或 NYSE 常规整日休市日使用上一交易日。这个默认值避免本机时区或 UTC 零点把亚洲早晨运行误判为下一个尚未完成的美股交易日；显式传入 `--as-of` 时仍按用户指定日期执行可见性门禁。

每日真实执行入口使用同一份计划顺序，并额外生成脱敏执行报告：

```powershell
aits ops daily-run --as-of 2026-05-02
```

`daily-run` 会先写出 `outputs/reports/daily_ops_plan_YYYY-MM-DD.md`，再做输入可见性预检查，随后按 `config/scheduled_tasks.yaml` 校验后的顺序调用本地 CLI。`daily-run` 是生产调度入口，不用于历史时点复现；输入可见性预检查对交易日使用最新已完成美股交易日，对显式休市日使用当前 America/New_York 日期，显式未来 `--as-of` 会在任何 download/PIT/SEC/valuation/OpenAI/dashboard 子命令前返回 `BLOCKED_VISIBILITY`，显式历史 `--as-of` 会提示改用 `aits ops replay-day --mode cache-only --as-of YYYY-MM-DD`。交易日 `score-daily` 成功后会依次生成 `evidence_dashboard_YYYY-MM-DD.html/json`、SEC PIT shadow observe/monitor、score change attribution、market panel、report index、documentation contract、research governance summary、Reader Brief、report quality gate 和 Reader Brief quality；随后运行 `aits etf dynamic-v3-rescue schedule observe --as-of YYYY-MM-DD`，只做 Dynamic v3 rescue weekly due/skip/block 审计、latest pointer validation、stale 检查和可选 observe-only shadow monitor，输出 `reports/etf_portfolio/dynamic_v3_rescue/schedule_observe/dynamic_v3_rescue_schedule_observe_YYYY-MM-DD.json/md`，不自动 `run-profile`、不运行 real sweep、不生成 promotion pack 或 `production_candidate`。这些报告和治理步骤固定 `production_effect=none`，不改变评分、仓位、回测、production weights、active shadow weights 或交易建议。weekly / biweekly / monthly / ad hoc research 任务只在 `config/scheduled_tasks.yaml` 登记，不由 `daily-run` 自动触发。执行器内部优先用项目 `.venv` Python 调用 daily-run direct dispatcher，找不到本地虚拟环境时才回退当前 Python，避免 Windows 上从 `aits.exe` 父进程递归启动 `aits.exe`、PATH 上的全局 `aits` 污染每日子命令环境，以及 Typer 对整棵 CLI 做全局解析时触发的 Windows 原生崩溃；子命令环境显式设置 `PYTHONMALLOC=malloc`、`PYTHONFAULTHANDLER=1`、`PYTHONDONTWRITEBYTECODE=1`，为每次 `daily-run` 使用独立 `PYTHONPYCACHEPREFIX=outputs/tmp/pycache/daily_run/run_*`，并在启动子命令前清理 `src/**/__pycache__`，降低 Windows 本机长流程子进程原生崩溃和字节码缓存异常风险。缺少阻断性环境变量时直接返回 `BLOCKED_ENV`，任一执行步骤退出码非 0 或关键 artifact 报告状态非 `PASS*` 时停止，不继续下游步骤；休市日的显式 `SKIPPED` 步骤不视为失败。执行报告写入 `outputs/reports/daily_ops_run_YYYY-MM-DD.md`，只记录步骤状态、退出码、耗时、stdout/stderr 行数和预期 artifact 路径，不保存 stdout/stderr 原文、API key、token 或付费内容原文；同目录会同步写入 `daily_ops_run_metadata_YYYY-MM-DD.json`，记录 run id、git/config/rule hash、命令清单、必需环境变量 presence、输入可见性状态、pre-run input checksum、produced artifact checksum 和 production visibility cutoff，同样不保存 secret 值或命令输出原文。调度器应调用 `daily-run`；`daily-plan` 保留为只读计划和凭据检查。

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
如果是第一次阅读系统，先看 `docs/learning_path.md`；如果看到某个 CSV、JSON 或 Markdown 产物不确定用途，先查 `docs/artifact_catalog.md`，也可以用 `aits explain scores_daily.score`、`aits explain --kind gate "binding gate"` 或 `aits explain --kind artifact risk_event_prereview_queue` 做只读反查。日报顶部的 `Data Lineage Card`、`Score-to-Position Funnel` 和 `Binding Gate Ladder` 会把本次运行的输入、输出、分数到仓位路径和最严格 gate 直接列出。

日报评分默认启用官方来源 + OpenAI 风险事件预审；如需排查或离线运行，可显式传入 `--skip-risk-event-openai-precheck`：

```powershell
aits score-daily --as-of 2026-05-05 --skip-risk-event-openai-precheck
```

该流程会先抓取 Federal Register/BIS/OFAC/USTR/Congress.gov/GovInfo/Trade.gov CSL 等官方政策/地缘来源，再用 `OPENAI_API_KEY` 调用 OpenAI Responses API 做 `metadata_only` 预审；请求默认使用 `config/llm_request_profiles.yaml` 中的 `risk_event_daily_official_precheck` profile：当前最多处理 10 条官方候选、官方来源每源抓取 limit 为 30、模型为 `gpt-5.5`、`reasoning.effort=medium`、请求读超时为 120 秒、HTTP 客户端为 `requests`、agent 请求缓存 TTL 为 24 小时、单请求失败最多重试 2 次。可用 `--llm-request-profile` 切换请求 profile，也可用 `--risk-event-openai-precheck-max-candidates`、`--official-policy-limit`、`--openai-cache-ttl-hours`、`--openai-model`、`--openai-reasoning-effort`、`--openai-timeout-seconds` 和 `--openai-http-client urllib` 做本次运行覆盖；完全相同 request payload 在 TTL 内 cache HIT 不重新调用 OpenAI，MISS/EXPIRED 才重新发送。生产日报只在评估日等于最新已完成美股交易日且显式传入 `--risk-event-openai-precheck-visibility-cutoff` 时，允许 OpenAI `request_timestamp` 晚于 `as_of` 但不晚于本轮 `visibility_cutoff`，用于支持收盘后 UTC/JST 次日生成前一美股交易日日报；`daily-run` 会为最新生产日自动向 `score-daily` 注入该 cutoff，直接运行 `score-daily` 若不传 cutoff 或用于历史 as-of，仍按 `as_of` 当日 UTC 末尾 fail closed。失败报告只输出 sanitized transport diagnostics，包括 attempt、HTTP client、client request id、endpoint host、payload byte size、input checksum、HTTP status、OpenAI x-request-id 或异常类型，不输出 API key、Authorization header 或未授权全文；本地请求缓存会记录 provider/api family、脱敏 request、response body、attempt diagnostics、cache key 和 checksum，OpenAI 归档位于 `archive/openai/responses/YYYY-MM-DD/`。预审输出写入 `data/processed/risk_event_prereview_queue.json` 和 `outputs/reports/risk_event_prereview_openai_YYYY-MM-DD.md`；预审报告会披露 `request_timestamp`、`as_of` 和 `visibility_cutoff` 的关系。按 2026-05-12 owner 决策，日报默认还会按 profile 中的 `formal_assessment` 设置把成功预审结果写入 LLM formal occurrence/attestation 和 `outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md`，作为政策/地缘正式评估输入。LLM formal 不是人工复核，full coverage 置信度低于人工复核但不再按低置信模块处理；LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 position gate。缺少 OpenAI key、官方来源抓取失败、provider LLM 权限失败、provider `cache_allowed=false`、OpenAI 请求最终失败或 LLM formal 写入失败时，默认日报评分会停止。

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

把已生成的回测稳健性摘要接入反馈闭环，生成参数复测收益变化报告：

```powershell
aits backtest --robustness-report --to 2026-05-12 --quality-as-of 2026-05-13
aits feedback build-parameter-replay --as-of 2026-05-13
aits feedback build-parameter-candidates --as-of 2026-05-13
aits feedback evaluate-parameter-governance --as-of 2026-05-13
aits feedback optimize-market-feedback --as-of 2026-05-13
```

`build-parameter-replay` 读取 `outputs/backtests/backtest_robustness_*.json`，比较 baseline 与模块权重扰动、再平衡频率、成本压力等参数场景的收益、回撤和换手变化，并写出 `outputs/reports/parameter_replay_YYYY-MM-DD.md/json`。robustness 调参场景会缓存昂贵的 point-in-time feature/report 上下文，但权重扰动、成本压力和窗口切分仍重走同一评分/回测执行路径，不维护第二套评分逻辑。material 判定优先使用 robustness summary 内嵌的 as-run policy；旧 summary 缺少 policy 时，会读取当前 `config/backtest_validation_policy.yaml` 并在报告中披露 limitation。`build-parameter-candidates` 再把这些场景登记到 `data/processed/parameter_candidates.json` 和 `outputs/reports/parameter_candidates_YYYY-MM-DD.md`，作为后续 shadow / owner review 的 candidate-only trial ledger；正向 material 变化只有通过 data/OOS/random/baseline/statistical/sample 多目标门禁后才可能进入 forward shadow，负向 material 变化进入 risk review。`evaluate-parameter-governance` 读取 `config/parameter_governance.yaml` 和 candidate ledger，按参数面输出 keep/current、collect evidence、prepare shadow、owner-required 或 blocked-by-data/policy 动作建议；owner 暂缺量化输入时不得由系统代填生产数值。该流程 `production_effect=none`，不会生成 approved overlay 或改变 `score-daily`、`position_gate`、日报结论或回测仓位。

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

风险事件配置在 `config/risk_events.yaml`，只定义需要监控的 L1/L2/L3 规则、AI 仓位折扣乘数、人工复核要求、影响产业链节点、相关标的、建议动作、升级条件和解除条件。`triage-official-candidates` 读取官方来源候选 CSV，按 AI 模块直接相关性输出 `must_review`、`review_next`、`sample_review`、`auto_low_relevance` 和 `duplicate_or_noise`，用于降低无明显 AI/半导体/先进计算/出口管制/核心 ticker 关系候选的人工复核优先级；triage 输出保持 `production_effect=none`，不代表已确认无风险。`precheck-triaged-official-candidates` 默认按 `risk_event_triaged_official_candidates` profile 只把 `must_review/review_next` 高优先级官方候选送入 OpenAI metadata-only 预审，输出 `status_suggestion` 和 `level_suggestion`。按当前 owner 决策，`apply-llm-formal-assessment` 和默认 `score-daily` 可把 LLM 预审队列写成正式 occurrence 和 LLM formal attestation；该模式不是人工复核，日报来源类型显示为 `llm_formal_assessment`，LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 position gate。OpenAI 只能通过 `precheck-triaged-official-candidates`、`apply-llm-formal-assessment`、`precheck-openai`、`import-prereview-csv` 或 `score-daily --risk-event-openai-precheck` 参与风险事件链路；live 请求参数由 `config/llm_request_profiles.yaml` 的 profile 管理，CLI 显式参数只覆盖本次运行；provider 授权未知或不允许本地缓存归档时 fail closed。实际发生记录默认读取 `data/external/risk_event_occurrences/*.yaml`，该目录不提交；可参考 `docs/examples/risk_event_occurrences/export_control_active_template.yaml` 复制模板。`import-occurrences-csv` 只接受人工复核后的结构化 CSV，同一 `occurrence_id` 的多行用于合并证据来源，关键字段冲突会停止导入。政策/地缘评分只读取已通过校验且评估日可见的发生记录；晚于评估日的 occurrence、evidence 或复核声明会在校验报告中以 warning 记录并排除，不进入历史日评分、仓位闸门或当前有效声明。`public_convenience` 证据只能作为辅助，不能单独进入自动评分。

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

该命令读取 `config/sec_companies.yaml` 的 ticker/CIK 映射，下载 SEC EDGAR companyfacts JSON 到 `data/raw/sec_companyfacts/`，优先保存 SEC 返回的原始 response bytes；若 provider 只能返回 parsed dict，则使用 compact streaming JSON 写入，不做 pretty/sorted 巨型中间字符串。命令会追加写入 `sec_companyfacts_manifest.csv`，checksum 基于实际落盘文件计算。校验命令会检查 JSON、CIK、taxonomy 和 checksum。`extract-sec-metrics` 会先执行同一条 SEC 缓存质量门禁，通过后按 `config/fundamental_metrics.yaml` 抽取收入、毛利、营业利润、净利润、研发和 CapEx 等指标，默认输出 `data/processed/sec_fundamentals_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamentals_YYYY-MM-DD.md`。显式派生指标只允许使用配置声明的组件，例如 `gross_profit = revenue - cost_of_revenue`，且必须满足周期、单位、截止日、财年、财期和 accession number 一致。`validate-sec-metrics` 会输出完整缺失观测清单，格式为 `ticker / metric_id / period_type`，便于回测报告按月下钻缺口。`download-sec-submissions` 会下载 `data.sec.gov/submissions` filing history 到 `data/raw/sec_submissions/`；`download-sec-filing-archive` 会按当日 SEC 指标 CSV 实际使用的 accession 下载 accession directory `index.json` 到 `data/raw/sec_filings/`；`sec-accession-coverage` 生成 `outputs/reports/sec_accession_coverage_YYYY-MM-DD.md`，检查 submissions metadata、accepted time 和 archive index checksum 覆盖。第一版不默认下载全部 exhibit 或全历史 filing，下载范围限于当前指标实际用到的 accession。TSM 季度指标可以用 `fetch-tsm-ir-quarterly` 从 TSMC Investor Relations 官方季度页面发现并下载 Management Report 文本；若官方资源是 PDF 或二进制，先用 `extract-tsm-ir-pdf-text` 从本地官方 PDF 的文本层生成可审计文本，再用 `import-tsm-ir-quarterly` 导入本地文本。`filed_date` 代表 Management Report 公开/披露日期，用于历史回测 point-in-time 可见性；历史季度回填可用 `import-tsm-ir-quarterly-batch` 读取 manifest CSV，字段为 `fiscal_year,fiscal_period,source_url,input_path,filed_date`，相对路径按 manifest 所在目录解析；模板在 `docs/examples/fundamentals/tsm_ir_quarterly_manifest_template.csv`；同一批次重复季度、缺文件或非官方 URL 会失败且不写入 CSV。PDF 抽取依赖可选依赖 `pypdf`（安装 `.[data]` 会包含它）；扫描件或无文本层 PDF 会停止并要求 OCR 或人工抽取，不能生成伪文本。TSM IR 默认写入 `data/processed/tsm_ir_quarterly_metrics.csv`、`outputs/reports/tsm_ir_pdf_text_YYYY-MM-DD.md`、`outputs/reports/tsm_ir_quarterly_YYYY_Qn_YYYY-MM-DD.md` 和 `outputs/reports/tsm_ir_quarterly_batch_YYYY-MM-DD.md`；`merge-tsm-ir-sec-metrics`、`score-daily` 和 `aits backtest` 会按评估日或 `signal_date` 选择当时最新已披露 TSM 季度，再把收入、毛利、营业利润、净利、研发和 CapEx 转为 SEC-style 指标行；日报前置计划会在 `validate-sec-metrics` 前显式执行 TSM IR 合并，`score-daily` 也会在本地 TSM IR 缓存存在时自动合并后再校验。金额单位保留 Management Report 披露尺度，例如 `TWD_billions` 或 `USD_billions`。不能用半年度 6-K 拆分替代季度数据。`build-sec-features` 会先复用同一条 SEC 指标 CSV 校验门禁，通过后按 `config/fundamental_features.yaml` 生成毛利率、营业利润率、净利率、R&D 强度和年度 CapEx 强度，默认输出 `data/processed/sec_fundamental_features_YYYY-MM-DD.csv` 和 `outputs/reports/sec_fundamental_features_YYYY-MM-DD.md`。`score-daily` 会复用同一条 SEC 指标校验和特征构建路径，校验失败时停止日报评分；通过后按 `config/scoring_rules.yaml` 的 `fundamentals` 规则使用 AI 核心观察池 SEC 特征中位数进行基本面硬数据评分。

复盘交易记录并做基础归因：

```powershell
aits review-trades --as-of 2026-05-02
```

交易记录默认读取 `data/external/trades/*.yaml`，该目录不提交。可参考 `docs/examples/trades/nvda_trade_template.yaml` 复制模板。交易记录应包含 `recorded_at` 和可选 `updated_at`，用于证明记录在 PIT replay 时点是否可见；缺少 `recorded_at` 会在交易复盘校验中提示警告，严格 replay 不会把缺少可证明记录时间的交易当作可见输入。该命令依赖缓存行情数据，会先执行数据质量门禁，再将交易收益与 `SPY`、`QQQ`、`SMH`、`SOXX` 同区间收益对比，辅助区分市场 Beta、AI 主题 Beta 和个股表现。

## 投资边界

系统输出只作为个人研究和仓位管理辅助，不构成投资建议。所有策略都需要回测、复盘，并显式考虑税费、滑点、汇率、交易延迟和极端风险。
