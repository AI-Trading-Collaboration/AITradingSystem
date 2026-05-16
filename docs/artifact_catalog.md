# 产物目录

本文从“看到一个文件后怎么理解它”的角度列出关键 artifact。`docs/system_flow.md` 仍是工程事实源；本文是阅读索引，帮助使用者判断 artifact 的生成者、上游输入、下游用途、production 影响和常见误解。

如果需要理解输入数据如何计算成输出数据，先读 `docs/calculation_logic.md`；字段级含义见 `docs/schema/fields.yaml`。该 YAML 先覆盖 `scores_daily.csv`、decision snapshot、trace bundle、prediction ledger 和 shadow parameter search 的核心字段。

## Production / advisory 主链路

| Artifact | 由谁生成 | 上游输入 | 关键字段或内容 | 下游使用 | 是否影响 production | 常见误解 |
|---|---|---|---|---|---|---|
| `data/raw/prices_daily.csv` | `aits download-data` | FMP 股票/ETF、Cboe VIX | `ticker`、`date`、OHLCV、`adj_close`、provider metadata | `validate-data`、features、backtest、outcome | 是，作为主价格缓存 | 不是供应商原始响应；已标准化为本地长表 |
| `data/raw/prices_marketstack_daily.csv` | `aits download-data` | Marketstack | 第二行情源 OHLCV | `validate-data` reconciliation | 是，作为质量核验输入 | 不覆盖主价格缓存 |
| `data/raw/rates_daily.csv` | `aits download-data` | FRED | `series`、`date`、`value` | macro features、quality gate、risk budget | 是 | `DTWEXBGS` 是广义美元指数代理，不是 ICE DXY |
| `data/raw/download_manifest.csv` | `aits download-data` | 本次下载请求 | provider、endpoint、request params、timestamp、row count、checksum | 数据源审计、质量报告 | 是，作为来源审计 | 不包含 API key 或 token 原文 |
| `outputs/reports/data_quality_YYYY-MM-DD.md` | `aits validate-data` | raw market/macro cache、download manifest、data quality config | schema、freshness、duplicates、suspicious values、reconciliation | features、score-daily、backtest 前置门禁 | 是，失败时下游应停止 | PASS 不等于投资结论可靠，只代表数据缓存可继续用 |
| `outputs/reports/data_quality_YYYY-MM-DD_marketstack_reconciliation.csv` | `aits validate-data` | FMP 主源、Marketstack 第二源 | ticker/date、主/二源数值、分类、severity、证据 | 数据质量复核 | 是，作为质量证据 | INFO 不代表改价，只是可解释差异 |
| `data/processed/features_daily.csv` | `aits build-features` / `aits score-daily` | 通过门禁的 raw cache、feature config | tidy features、as_of、ticker/series、value | scoring、reports、backtest | 是 | 不是所有字段都会进入评分 |
| `outputs/reports/feature_summary_YYYY-MM-DD.md` | `aits build-features` | features、quality gate | feature coverage、warnings | score-daily、人工复核 | 是，作为特征健康报告 | 不能替代 `validate-data` |
| `outputs/reports/feature_availability_YYYY-MM-DD.md` | `build-features` / `score-daily` / `backtest` | `config/feature_availability.yaml`、feature rows | PIT availability、future rows、fallback 策略 | score-daily、backtest fail-closed | 是 | 缺少 availability rule 不是小问题，会触发 fail closed |
| `data/processed/sec_fundamentals_YYYY-MM-DD.csv` | SEC/TSM fundamentals pipeline | SEC companyfacts、TSMC IR | metric、period、filed/disclosed dates、value | fundamental scoring | 是 | TSM 可由官方 IR 合并，不等同 SEC companyfacts 缺失 |
| `data/processed/sec_features_YYYY-MM-DD.csv` | `aits fundamentals build-sec-features` / `score-daily` | SEC-style metrics | margin、R&D、CapEx 等特征 | fundamental component | 是 | 单位或周期不一致时会跳过或告警，不应手工平滑 |
| `data/external/valuation_snapshots/*.yaml` | `aits valuation fetch-fmp` 或人工导入 | FMP / 可审计估值来源 | valuation percentile、crowding、EPS revision、PIT metadata | valuation component、valuation gate | 是 | 过期或 public convenience 来源不能伪装成高可信生产输入 |
| `data/external/risk_event_occurrences/*.yaml` | 风险事件导入、LLM formal、人工复核 | 官方来源、LLM precheck、review attestation | risk id、level、evidence grade、lifecycle、action class | policy/geopolitics score、risk gate、alerts | 是 | LLM formal 不是人工复核，B 级 evidence 不能单独触发 gate |
| `data/external/trade_theses/*.yaml` | 人工维护 | thesis、验证指标、证伪条件 | status、review state、invalidators | thesis gate、日报解释 | 是，对 active_trade 纪律有效 | watch_only ticker 缺 thesis 不等于 thesis 失败 |
| `data/external/portfolio_positions/current_positions.csv` | 人工持仓快照 | 真实账户持仓 | ticker、市值、AI exposure、节点/地区/因子 | portfolio exposure、risk budget 扩展 | Advisory 输入 | 缺失时不能用观察池或模型建议仓位替代真实持仓 |
| `data/processed/scores_daily.csv` | `aits score-daily` | features、fundamentals、valuation、risk、weights、gates | component score、overall、confidence、effective weights、model/final position、gate summary | 日报、snapshot、dashboard、previous comparison | 是，作为评分输出 | 不等于交易指令，也不等于账户实际仓位 |
| `outputs/reports/daily_score_YYYY-MM-DD.md` | `aits score-daily` | scores、quality、features、manual inputs、trace context | Decision Card、Data Lineage、funnel、gate ladder、module scores、review sections | 人读主报告、dashboard 输入 | Advisory / trend judgment | 报告仓位是解释区间，不是自动买卖 |
| `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json` | `aits score-daily` | 当日评分上下文、gate、quality、trace | market regime、scores、confidence、positions、position_gates、trace refs | feedback、shadow、replay、dashboard | 是，记录 production 判断 | 不是回测结果，且不应被后验 outcome 改写 |
| `outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json` | `aits score-daily` | report claims、evidence、datasets、quality、run manifest | claim/evidence/dataset/quality refs、run id、rule versions、weight calibration | dashboard、trace lookup、审计 | 不直接改变结果 | 不是日报正文，而是证据索引 |
| `data/processed/prediction_ledger.csv` | `score-daily` / challenger shadow | decision snapshot、trace | candidate_id、production_effect、signal date、score、final position、outcome status | outcome calibration、shadow maturity | production 行影响正式观察；challenger 行必须隔离解释 | 后验 outcome 不应改写 signal-time 输入 |
| `outputs/reports/evidence_dashboard_YYYY-MM-DD.html` | `aits reports dashboard` | daily report、trace、snapshot、belief state、alerts、scores | 分层展示结论、evidence、dataset、quality、gate、trend | 本地只读下钻 | 否，`production_effect=none` | dashboard 不替代 Markdown 日报和 trace 的审计责任 |

## Daily-run / replay 运行产物

| Artifact | 由谁生成 | 上游输入 | 关键字段或内容 | 下游使用 | 是否影响 production | 常见误解 |
|---|---|---|---|---|---|---|
| `outputs/reports/daily_ops_plan_YYYY-MM-DD.md` | `aits ops daily-plan` / `daily-run` | market calendar、env presence、config | planned commands、required env、expected artifacts、skip reason | 调度前检查、daily-run | 否，只读计划 | 计划报告不代表命令已经执行 |
| `outputs/reports/daily_ops_run_YYYY-MM-DD.md` | `aits ops daily-run` | plan、实际子命令结果 | step status、exit code、duration、artifact paths | 运行审计、问题定位 | 是，作为生产运行审计 | 不保存 stdout/stderr 原文或 secret |
| `outputs/reports/daily_task_dashboard_YYYY-MM-DD.html/json` | `aits ops daily-run` / `aits reports daily-tasks` | daily_ops_run metadata、本轮同日子报告、evidence dashboard JSON、最近可用 shadow parameter search bundle、search window 内 production decision snapshots | `key_conclusions`、投资动作/仓位/置信度/Data Gate、数据可信度、参数治理、反馈复盘、shadow parameter 诊断领先 trial 与收益差距、production/current vs shadow candidate 结果对比表（Total return、Max drawdown、Turnover、Beat rate、样本覆盖和 return 计算口径）、分区参数对比表（Gate cap override 与权重参数，含 production 实际 gate cap 数值/区间）、运行健康、任务状态、子报告 `href` 和可点击子任务下钻入口 | 每日关键结论入口 | 否，`production_effect=none` | 任务状态是审计信息，不是页面首要结论；shadow parameter 诊断领先不等于可上线；return 只统计 AVAILABLE outcome 且扣除成本/滑点后复利累计；各子任务 Markdown/HTML/JSON 仍是审计源，后续可升级为专属网页 |
| `outputs/runs/daily/<executed_at>/as_of_YYYY-MM-DD__<run_id>/manifest.json` | `aits ops daily-run` | 本轮 daily-run artifacts | run id、checksum、legacy mirror、visibility cutoff；artifact 记录由 `ArtifactRef` 统一生成 | 复现、归档、审计 | 是，作为运行 bundle | `data/raw` / `data/processed` 仍是状态缓存，不会每轮完整复制；manifest 只记录产物引用和 checksum，不保存 stdout/stderr 原文 |
| `outputs/replays/YYYY-MM-DD/<run_id>/replay_run.md` | `aits ops replay-day` | cache-only input freeze | visible cutoff、excluded future inputs、replay artifacts | 历史复现、diff | 否，不改 production artifacts | replay 不是 live daily-run，也不应调用 live provider |
| `outputs/replays/YYYY-MM-DD/<run_id>/diff_vs_production.md` | `replay-day --compare-to-production` | replay bundle、production artifacts | checksum、row count、status diff | PIT 复核 | 否 | diff 说明产物不同，不自动说明哪个结论正确 |

## Backtest / feedback 产物

| Artifact | 由谁生成 | 上游输入 | 关键字段或内容 | 下游使用 | 是否影响 production | 常见误解 |
|---|---|---|---|---|---|---|
| `outputs/backtests/backtest_YYYY-MM-DD_YYYY-MM-DD.md` | `aits backtest` | validated data、PIT inputs、scoring rules | regime、date range、returns、drawdown、benchmark comparison、limitations | 投研复核、parameter replay | 否，回测不自动改规则 | 默认结论窗口应声明 `ai_after_chatgpt` |
| `outputs/backtests/backtest_daily_YYYY-MM-DD_YYYY-MM-DD.csv` | `aits backtest` | daily scoring context | signal date、score、target exposure、next return、costs、gates | robustness、gate attribution、parameter replay | 否 | 不是 production ledger |
| `outputs/backtests/backtest_input_coverage_*.csv` | `aits backtest` | PIT slices、quality reports | source coverage、missing inputs、warning status | audit、candidate veto | 否 | 高收益但覆盖不足不应直接晋级 |
| `outputs/reports/parameter_replay_YYYY-MM-DD.md/json` | `aits feedback build-parameter-replay` | robustness summary | scenario deltas、materiality、limitations | parameter candidate ledger | 否 | as-if replay 不是 approved overlay |
| `data/processed/parameter_candidates.json` | `aits feedback build-parameter-candidates` | parameter replay | candidate id、trial metrics、veto reasons、governance status | parameter governance、shadow runner | 否 | candidate ready 不等于 owner approval |
| `outputs/reports/market_feedback_optimization_YYYY-MM-DD.md` | `aits feedback optimize-market-feedback` | outcomes、causal chains、candidates、overlays | readiness、sample floors、open gaps、next actions | 复盘、治理 | 否 | readiness 不是生产配置变更 |
| `data/processed/prediction_outcomes.csv` | `aits feedback calibrate-predictions` | prediction ledger、prices | horizon、available/pending/missing、benchmark excess | shadow maturity、promotion review | 否 | outcome 是后验观察，不能改写原 prediction |

## Shadow / validation-only 参数搜索产物

| Artifact | 由谁生成 | 上游输入 | 关键字段或内容 | 下游使用 | 是否影响 production | 常见误解 |
|---|---|---|---|---|---|---|
| `config/weights/shadow_weight_profiles.yaml` | 人工维护 / 系统建议 | production weight profile | validation-only profile、weights、metadata | shadow observation | 否，`production_effect=none` | 不替换 `weight_profile_current.yaml` |
| `config/weights/shadow_position_gate_profiles.yaml` | 人工维护 / 系统建议 | scoring/portfolio gate policy | validation-only gate cap overrides | shadow observation | 否 | relaxed gate 不是 production gate 放宽 |
| `config/weights/shadow_parameter_search_space.yaml` | 人工维护 | production weights/gates | weight grid、gate cap grid、bounds | parameter search | 否 | 搜索空间内最优不是无限连续空间最优 |
| `config/weights/shadow_parameter_objective.yaml` | 人工维护 | validation policy | objective、sample floor、regularization、eligibility | parameter search ranking | 否 | objective 排名不是 promotion contract |
| `outputs/parameter_search/<run_id>/manifest.json` | `aits feedback search-shadow-parameters` | snapshots、prices、search configs | input checksum、resolver version、git status、run params | reproducibility、promotion contract | 否 | manifest 不批准上线 |
| `outputs/parameter_search/<run_id>/trials.csv` | `search-shadow-parameters` | weight/gate candidates | trial id、metrics、objective、eligibility、regularization | analysis、Pareto front | 否 | trial 多不代表统计独立样本多 |
| `outputs/parameter_search/<run_id>/pareto_front.csv` | `search-shadow-parameters` | trials | non-dominated trials | review | 否 | Pareto front 不等于可上线名单 |
| `outputs/parameter_search/<run_id>/best_profiles.yaml` | `search-shadow-parameters` | eligible/diagnostic trials | candidate profile snapshot | review / forward shadow prep | 否 | 不应复制覆盖生产配置 |
| `outputs/parameter_search/<run_id>/search_report.md` | `search-shadow-parameters` | manifest、trials、attribution | eligible status、diagnostic-leading trial、factorial attribution、cap-level attribution、position changes | 人读参数搜索报告 | 否，`production_effect=none` | diagnostic-leading 只是当前样本解释，不是 production 建议 |
| `outputs/parameter_search/<run_id>/shadow_parameter_promotion_<run_id>.md/json` | `aits feedback evaluate-shadow-parameter-promotion` | search bundle、promotion contract | NOT_PROMOTABLE / READY_FOR_FORWARD_SHADOW / READY_FOR_OWNER_REVIEW、blockers | promotion review | 否，直到 owner approval 仍不改 production | `READY_FOR_FORWARD_SHADOW` 仍不是上线批准 |
| `data/processed/prediction_ledger_flow_validation.csv` | `aits feedback run-parameter-shadow` | parameter candidates、production snapshot | validation-only challenger predictions | flow validation outcomes | 否 | 不应和正式 `prediction_ledger.csv` 混同 |

## production_effect 标签

| 标签 | 含义 | 典型位置 |
|---|---|---|
| `production` | 正式生产判断链路的一部分，会进入 canonical scoring / snapshot / ledger 语义 | production decision snapshot、production prediction ledger 行 |
| `advisory` | 投研辅助或趋势判断输出，不自动交易 | daily score report、execution advisory |
| `none` | 只读、诊断、shadow 或 validation-only，不改变生产评分、仓位 gate、正式 ledger 或 approved overlay | dashboard、shadow search、flow validation ledger |
| `validation-only` | 用于验证参数、规则或流程，不可当成生产候选批准 | parameter search、shadow gate / weight profiles |
| `blocked` | 被质量、治理、样本、owner approval 或 promotion contract 阻断 | promotion report、candidate governance |

运行时核心标签由 `ai_trading_system.core.production_effect.ProductionEffect` 表达。历史报告中较长的解释性 production effect 文本需要逐处确认语义后再迁移，不能自动归并。

当一个产物声明 `production_effect=none` 时，它仍然可以非常有价值，但价值是诊断、学习、复核或准备 forward shadow，而不是直接改变 production 结论。
