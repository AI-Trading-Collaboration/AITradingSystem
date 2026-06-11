# TRADING-214～218 Paper Shadow Historical Backfill and Rolling Target Evaluation

最后更新：2026-06-12

## 状态

`VALIDATING`

本阶段把 TRADING-209～213 的 2 天 paper shadow 链路验证扩展为从
`2022-12-01` 开始的 AI after ChatGPT regime 历史回填、滚动窗口评估、regime
表现复核、稳定性与换手诊断，以及最终 system target method selection review。

## 背景

TRADING-209～213 已生成 research-only model target、paper shadow account、
model rebalance、短窗口 performance 和 system target review pack。当前 performance
window 只有 `2026-06-09` 到 `2026-06-10`，只能证明链路可运行，不能判断
`limited_adjustment`、`defensive_limited_adjustment`、`consensus_target`、
`selected_top_candidate` 等 method 的长期研究价值。

历史回填使用项目默认 AI regime：

- anchor event: ChatGPT public launch on `2022-11-30`
- default backfill start: `2022-12-01`
- pre-2022 data only for warm-up/stress/regime comparison, not primary conclusion

## 范围

### TRADING-214 Paper Shadow Historical Backfill Runner

新增 `paper_shadow_backfill_v1.yaml`，运行 research-only historical simulation，
输出 backfill manifest、rebalance calendar、method state history、trade ledger、
data quality summary 和 Markdown report。

### TRADING-215 Rolling Target Evaluation Windows

基于 backfill state history 输出 full、yearly、rolling 3-month、rolling 6-month、
rolling 12-month metrics，并计算 rank stability。

### TRADING-216 Paper Shadow Regime Performance Review

按 `ai_trend`、`tech_drawdown`、`semiconductor_pullback`、`risk_off`、
`sideways_choppy`、`strong_recovery` 聚合 method 表现；样本不足时显式输出
`INSUFFICIENT_DATA`。

### TRADING-217 Target Method Stability and Turnover Diagnostics

分析 method 权重路径跳变、rebalance turnover、cash/risk asset volatility 和
large jump events，避免只按收益排序。

### TRADING-218 System Target Method Selection Review

汇总 historical backfill、rolling eval、regime review 和 stability diagnostics，
生成 method scorecard、selection decision、owner research checklist、Reader Brief
section 和 selection review report。

## 明确不做

- broker API、broker import、自动下单、order ticket
- owner approval 自动化
- official target weights 写入
- production candidate 自动晋级
- 真实持仓映射
- 交易成本 / slippage sensitivity 和图表化增强

所有新增输出必须继续固定：

- `research_target_only=true`
- `not_official_target_weights=true`
- `paper_shadow_only=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_effect=none`

## 实施顺序

1. 新增 source config 和 task/register/docs baseline。
2. 实现 historical backfill artifact generator 和 validator。
3. 实现 rolling eval、regime review、stability diagnostics 和 validators。
4. 实现 selection review、Reader Brief section 和 validators。
5. 接入 CLI、report registry、artifact catalog、system flow、operations runbook 和 README。
6. 新增 focused tests，运行必需验证。
7. 跑通真实链路，记录 artifact id、结论、限制和下一步。

## 设计决策

- Historical backfill 是研究模拟，不是 PIT-safe 生产回测。配置必须暴露
  `not_pit_safe=true` 和 `mode=BACKTEST_SIMULATION`，报告不得把它解释为正式交易规则证据。
- Backfill 与 live paper shadow 分离：live paper shadow 表示当前研究账户状态；
  backfill 是从历史价格和固定 method 规则重建 method-level path，用于横向比较。
- Rolling evaluation 用于防止全区间平均掩盖 regime 或局部窗口失效。
- Regime review 用 rule-based return buckets 生成初版 regime 标签；样本不足时不得强行排名。
- Stability/turnover diagnostics 是 target method selection 的同级证据，不允许只因收益最高而推荐 method。
- Selection review 默认保持 `limited_adjustment` 作为主 research method，除非证据显示它在收益、回撤、regime 和稳定性上被明显替代；这仍不构成 official target 或 broker action。

## 验收标准

- `paper-shadow-backfill config-validate/run/report` 可运行。
- `validate-paper-shadow-backfill` PASS。
- `paper-shadow-rolling-eval run/report` 和 validator PASS。
- `paper-shadow-regime-review run/report` 和 validator PASS。
- `paper-shadow-stability run/report` 和 validator PASS。
- `system-target-selection-review run/report` 和 validator PASS。
- 新 artifacts 写入 dynamic_v3_rescue runtime roots，并接入 latest pointer。
- Report registry、artifact catalog、system flow、operations runbook、README、Reader Brief 更新。
- Focused tests PASS。
- `python -m ruff check src tests`、`python -m compileall -q src tests`、`git diff --check` PASS。
- 能说明 full pytest 是否完成；若未完成，记录原因和已完成验证。

## 开放限制

- Regime bucket 初版是 rule-based research diagnostic，不是经 owner 审批的正式 regime classifier。
- 当前 method path 以固定 method rule 和 weekly rebalance 重建，不能证明真实交易可执行性。
- 缺少交易成本/slippage sensitivity；本阶段成本默认 0 bps，只用于 method shape research。
- `not_pit_safe=true` 表示历史回填可用于研究比较，不可作为生产晋级证据。
- 指定 `aits reports reader-brief --date 2026-06-12` 因缺少当日
  `decision_snapshot_2026-06-12.json` 阻断；本次用 `--latest` 生成
  `reader_brief_2026-06-08` 验证 Reader Brief 只读集成，且显式使用
  `report_index_2026-06-12.json`。

## 验收结果

- Backfill artifact: `paper-shadow-backfill_2138461d25e686e0`
  - actual date range: `2022-12-01` to `2026-06-10`
  - rebalance count: `176`
  - data quality: `PASS_WITH_WARNINGS`
- Rolling eval artifact: `paper-shadow-rolling-eval_ada0ef48cdc2276f`
  - window count: `135`
  - method count: `7`
- Regime review artifact: `paper-shadow-regime-review_a4af4ee7ab6e1471`
  - `defensive_limited_adjustment_status=MIXED`
- Stability artifact: `paper-shadow-stability_49010cc0297a0d84`
  - jump event count: `4`
- Selection review artifact: `system-target-selection-review_83a214b3223d937b`
  - `recommended_research_method=limited_adjustment`
  - `decision_status=REVIEW_REQUIRED`
  - `not_official_target_weights=true`
  - `broker_action_allowed=false`

验证已通过：

- `paper-shadow-backfill config-validate`
- `validate-paper-shadow-backfill`
- `validate-paper-shadow-rolling-eval`
- `validate-paper-shadow-regime-review`
- `validate-paper-shadow-stability`
- `validate-system-target-selection-review`
- `python -m ai_trading_system.cli validate-data --as-of 2026-06-10`
- `python -m ai_trading_system.cli etf dynamic-v3-rescue validate`
- `python -m ai_trading_system.cli etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`
- `python -m ai_trading_system.cli reports index --date 2026-06-12`
- `python -m ai_trading_system.cli reports reader-brief --latest --report-index-path outputs/reports/report_index_2026-06-12.json`
- `python -m pytest tests/test_model_target_portfolio.py tests/test_paper_shadow_account.py tests/test_model_rebalance.py tests/test_paper_shadow_performance.py tests/test_system_target_review.py tests/test_paper_shadow_backfill.py tests/test_paper_shadow_rolling_eval.py tests/test_paper_shadow_regime_review.py tests/test_paper_shadow_stability.py tests/test_system_target_selection_review.py -q`
- `python scripts/run_validation_tier.py full --json-output outputs/validation_full_TRADING_214_to_218.json`
- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `git diff --check`

## 进展记录

- 2026-06-12: 新增任务文档并进入 `IN_PROGRESS`，原因：owner 要求推进 TRADING-214～218，从短窗口 paper shadow 验证升级为 AI after ChatGPT 历史回填、rolling/regime/stability/selection review 闭环。
- 2026-06-12: baseline 实现完成并转入 `VALIDATING`。真实链路已生成
  backfill / rolling eval / regime review / stability / selection review artifacts；
  full validation tier `2372 passed, 643 warnings`；selection review 建议继续以
  `limited_adjustment` 作为主 research method，但状态为 `REVIEW_REQUIRED`，
  不构成 official target weights、production approval 或 broker action。
