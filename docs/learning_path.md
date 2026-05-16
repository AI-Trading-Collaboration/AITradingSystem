# 系统学习路径

本文是第一次理解系统时的阅读入口。`docs/system_flow.md` 仍是工程事实和审计源文档；本文只按学习顺序组织，让使用者知道应该先看什么、每一步解决什么问题、输入输出在哪里、用哪份报告复核。

如果你没有金融背景，建议先读 `docs/calculation_logic.md`。它用普通语言解释价格、收益率、移动平均、相对强弱、VIX、利率、基本面、估值、confidence、gate 和 position，并说明输入数据如何一步步算成日报、snapshot、trace 和 ledger。

默认市场阶段是 `ai_after_chatgpt`：锚点为 ChatGPT 于 2022-11-30 公开发布，默认回测结论窗口从 2022-12-01 开始。早于该日期的数据可以用于 warm-up、压力测试或 regime 对比，但不应被当成默认 AI cycle 结论窗口。

## 第 0 步：系统做什么、不做什么

本步骤解决什么问题：

- 明确系统是 AI 产业链趋势判断和投研复核辅助，不是自动交易器。
- 明确日报里的仓位区间、gate 和执行动作是解释语言，不是账户买卖指令。

输入文件：

- `README.md`
- `docs/product_strategy.md`
- `config/market_regimes.yaml`

输出文件：

- `outputs/reports/daily_score_YYYY-MM-DD.md`
- `outputs/reports/evidence_dashboard_YYYY-MM-DD.html`

应该看哪份报告确认：

- 日报的 `结论使用等级`、`执行建议`、`Data Lineage Card` 和 `仓位闸门`。

## 第 1 步：数据从哪里来

本步骤解决什么问题：

- 确认价格、宏观、SEC/TSM 基本面、估值、风险事件、thesis 和真实持仓输入分别来自哪里。
- 区分生产来源、第二来源、public convenience 诊断来源和人工输入。

输入文件：

- `config/data_sources.yaml`
- `data/raw/prices_daily.csv`
- `data/raw/prices_marketstack_daily.csv`
- `data/raw/rates_daily.csv`
- `data/raw/download_manifest.csv`
- `data/external/valuation_snapshots/*.yaml`
- `data/external/risk_event_occurrences/*.yaml`
- `data/external/trade_theses/*.yaml`

输出文件：

- `outputs/reports/download_data_diagnostics_YYYY-MM-DD.md`
- `outputs/reports/data_sources_YYYY-MM-DD.md`

应该看哪份报告确认：

- `aits data-sources validate --as-of YYYY-MM-DD` 生成的数据源校验报告。
- `outputs/reports/data_quality_YYYY-MM-DD.md` 中的数据缓存摘要。

## 第 2 步：数据质量门禁能不能放行

本步骤解决什么问题：

- 判断 cached market / macro data 是否能进入特征、评分、回测和报告。
- 确认 schema、完整性、新鲜度、重复键、异常值和跨源 reconciliation 是否通过。

输入文件：

- `data/raw/prices_daily.csv`
- `data/raw/prices_marketstack_daily.csv`
- `data/raw/rates_daily.csv`
- `data/raw/download_manifest.csv`
- `config/data_quality.yaml`

输出文件：

- `outputs/reports/data_quality_YYYY-MM-DD.md`
- `outputs/reports/data_quality_YYYY-MM-DD_marketstack_reconciliation.csv`

应该看哪份报告确认：

- `aits validate-data --as-of YYYY-MM-DD` 的质量报告。
- 后续日报中的 `Data Gate` 和 `Data Lineage Card`。

## 第 3 步：市场特征如何生成

本步骤解决什么问题：

- 把通过门禁的价格、ETF、宏观数据转换成趋势、相对强弱、波动、利率和美元相关特征。
- 确认每个 feature 是否满足 PIT availability。

输入文件：

- `config/features.yaml`
- `config/feature_availability.yaml`
- `data/raw/prices_daily.csv`
- `data/raw/rates_daily.csv`
- `docs/calculation_logic.md`

输出文件：

- `data/processed/features_daily.csv`
- `outputs/reports/feature_summary_YYYY-MM-DD.md`
- `outputs/reports/feature_availability_YYYY-MM-DD.md`

应该看哪份报告确认：

- `feature_summary_YYYY-MM-DD.md`
- `feature_availability_YYYY-MM-DD.md`
- `docs/calculation_logic.md` 的“第 2 步：把原始数据变成 feature”。

## 第 4 步：component score 如何得到

本步骤解决什么问题：

- 把 trend、fundamentals、macro_liquidity、risk_sentiment、valuation、policy_geopolitics 等模块各自打分。
- 明确来源类型、覆盖率和置信度如何影响模块解释。

输入文件：

- `data/processed/features_daily.csv`
- `data/processed/sec_fundamentals_YYYY-MM-DD.csv`
- `data/processed/sec_features_YYYY-MM-DD.csv`
- `data/external/valuation_snapshots/*.yaml`
- `data/external/risk_event_occurrences/*.yaml`
- `data/external/trade_theses/*.yaml`
- `config/scoring_rules.yaml`
- `docs/calculation_logic.md`

输出文件：

- `data/processed/scores_daily.csv`
- `outputs/reports/daily_score_YYYY-MM-DD.md`

应该看哪份报告确认：

- 日报的 `模块评分`、`硬数据信号`、`人工复核摘要` 和 `Score-to-Position Funnel`。
- `docs/calculation_logic.md` 的“第 3 步”和“第 4 步”。

## 第 5 步：权重如何合成 overall score

本步骤解决什么问题：

- 解释 component score 如何通过 effective weights 合成 overall score。
- 区分生产权重、approved overlay 和 shadow validation 权重。

输入文件：

- `config/weights/weight_profile_current.yaml`
- `outputs/current_context.json`
- `outputs/current_effective_weights.json`
- `config/scoring_rules.yaml`

输出文件：

- `data/processed/scores_daily.csv`
- `outputs/current_effective_weights.json`
- 日报中的 `Historical Calibration` / `Overlay Audit`

应该看哪份报告确认：

- 日报的 `Score-to-Position Funnel` 和 `Historical Calibration`。
- `outputs/current_effective_weights.json`。
- `docs/calculation_logic.md` 的“第 5 步：用 effective weights 合成 overall score”。

## 第 6 步：score 如何映射成 model position

本步骤解决什么问题：

- 把 weighted overall score 映射成风险资产内 AI 模型仓位区间。
- 确认该区间还没有经过所有 hard gate。

输入文件：

- `config/scoring_rules.yaml`
- `data/processed/scores_daily.csv`

输出文件：

- `scores_daily.csv` 的 `model_risk_asset_ai_min/max`
- 日报的 `评分映射仓位`

应该看哪份报告确认：

- 日报的 `Score-to-Position Funnel`。

## 第 7 步：gate 如何压缩 final position

本步骤解决什么问题：

- 解释 confidence、calibration overlay、portfolio limit、macro risk budget、risk events、valuation、thesis 和 data confidence 如何共同限制最终仓位。
- 找出 binding gate，也就是当前最严格的限制。

输入文件：

- `config/scoring_rules.yaml`
- `config/portfolio.yaml`
- `data/external/valuation_snapshots/*.yaml`
- `data/external/risk_event_occurrences/*.yaml`
- `data/external/trade_theses/*.yaml`
- `data/external/portfolio_positions/current_positions.csv`

输出文件：

- `data/processed/scores_daily.csv`
- `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`
- 日报的 `Binding Gate Ladder` 和 `仓位闸门`

应该看哪份报告确认：

- 日报的 `Binding Gate Ladder`。
- Evidence dashboard 的 gate 表。
- `docs/calculation_logic.md` 的“第 7 步”到“第 9 步”。

## 第 8 步：日报、snapshot、trace、ledger 分别是什么

本步骤解决什么问题：

- 区分人读报告、机器快照、证据 bundle 和 append-only prediction ledger。
- 避免把日报、回测结果、trace 和 ledger 行混为一谈。

输入文件：

- `data/processed/scores_daily.csv`
- `outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`

输出文件：

- `outputs/reports/daily_score_YYYY-MM-DD.md`
- `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`
- `outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`
- `data/processed/prediction_ledger.csv`

应该看哪份报告确认：

- `docs/artifact_catalog.md`
- 日报的 `Data Lineage Card`
- `outputs/reports/evidence_dashboard_YYYY-MM-DD.html`

## 第 9 步：shadow / parameter search 为什么不影响 production

本步骤解决什么问题：

- 理解 validation-only 参数搜索、shadow 权重、shadow gate 和 flow validation ledger 的隔离边界。
- 防止把 in-sample diagnostic-leading trial 误读为生产规则批准。

输入文件：

- `config/weights/shadow_weight_profiles.yaml`
- `config/weights/shadow_position_gate_profiles.yaml`
- `config/weights/shadow_parameter_search_space.yaml`
- `config/weights/shadow_parameter_objective.yaml`
- `config/weights/shadow_parameter_promotion_contract.yaml`

输出文件：

- `outputs/parameter_search/<run_id>/manifest.json`
- `outputs/parameter_search/<run_id>/trials.csv`
- `outputs/parameter_search/<run_id>/pareto_front.csv`
- `outputs/parameter_search/<run_id>/best_profiles.yaml`
- `outputs/parameter_search/<run_id>/search_report.md`
- `outputs/parameter_search/<run_id>/shadow_parameter_promotion_<run_id>.md`
- `data/processed/prediction_ledger_flow_validation.csv`

应该看哪份报告确认：

- `search_report.md` 的 `治理边界`、Factorial Attribution、Cap-Level Attribution 和 Position Change Attribution。
- promotion contract 报告中的 `NOT_PROMOTABLE` / `READY_FOR_FORWARD_SHADOW` / `READY_FOR_OWNER_REVIEW`。

## 第 10 步：回测和 feedback 如何回到规则复核

本步骤解决什么问题：

- 解释回测、prediction outcome、calibration、learning queue 和 rule experiment 如何支持规则复核。
- 明确这些反馈不能自动改写生产规则。

输入文件：

- `data/processed/decision_snapshots/*.json`
- `data/processed/prediction_ledger.csv`
- `data/raw/prices_daily.csv`
- `config/backtest_validation_policy.yaml`
- `config/feedback_sample_policy.yaml`
- `config/rule_cards.yaml`

输出文件：

- `outputs/backtests/backtest_*.md`
- `outputs/backtests/backtest_*_trace.json`
- `data/processed/prediction_outcomes.csv`
- `outputs/reports/feedback_loop_review_YYYY-MM-DD.md`
- `outputs/reports/market_feedback_optimization_YYYY-MM-DD.md`

应该看哪份报告确认：

- backtest audit / trace 报告。
- feedback loop review。
- market feedback optimization readiness。

## 常用阅读顺序

每天只想快速理解结论：

1. 打开 `outputs/reports/daily_score_YYYY-MM-DD.md`。
2. 看 `今日结论卡`、`Data Lineage Card`、`Score-to-Position Funnel`、`Binding Gate Ladder`。
3. 如有疑问，再打开同日 evidence dashboard。

想复核数据和来源：

1. 看 `Data Gate`。
2. 看 `outputs/reports/data_quality_YYYY-MM-DD.md`。
3. 看 trace bundle 的 dataset / quality refs。

想理解参数搜索：

1. 看 `outputs/parameter_search/<run_id>/search_report.md`。
2. 先确认 `production_effect=none`。
3. 再看 eligible 状态、diagnostic-leading trial、factorial attribution、cap-level attribution 和 promotion contract。

想从零理解计算逻辑：

1. 先读 `docs/calculation_logic.md` 的基本词解释。
2. 按“数据质量门禁 -> feature -> signal -> component score -> overall score -> position gate”的顺序看。
3. 再回到某一天的日报，对照 `Score-to-Position Funnel` 和 `Binding Gate Ladder`。

想确认字段含义：

1. 先查 `docs/schema/fields.yaml`。
2. 看 `meaning`、`produced_by`、`upstream_fields`、`downstream_usage`、`production_effect` 和 `common_misunderstanding`。
3. 再回到对应 artifact 或 trace bundle 复核本次实际值。
