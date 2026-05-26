# TRADING-008A post-merge sanity review

最后更新：2026-05-17

关联任务：`TRADING-008A`

## 结论

未发现非预期逻辑变更。`40c15b596dc75230bff84d2d7257eae80f16afe6` 的 135 个文件改动中，
131 个是 Python 文件，4 个是 Markdown 文档；其中 128 个 Python 文件 AST 与父提交一致，
属于 Black 机械格式化。只有 3 个 Python 文件存在 AST 变化，均在 Shadow Impact
hardening 范围内：Shadow Impact report module、Shadow Impact 单元测试和 dashboard
Shadow Impact 只读边界测试。

GitHub Actions CI 已通过：`CI` run `25987350045`，run #73，commit
`40c15b596dc75230bff84d2d7257eae80f16afe6`，状态 `completed/success`，job `test`
为 `success`。

## 变更分类

### Shadow impact functional files

这些文件存在 Shadow Impact hardening 相关语义变化：

- `src/ai_trading_system/trading_engine/reports/shadow_parameter_impact.py`
  - 新增 `LOW_DATA_QUALITY` conservative status。
  - 新增 `continuous_replay_missing` warning。
  - 为 `impact_gate` 写入 `blocking_reason_explanations`、`warning_explanations` 和合并后的
    `reason_explanations`。
  - Markdown 增加 continuous replay artifact path / date range 展示。
  - continuous replay summary 增加 `source_artifact.path`、`mode`、`date_range` 和
    `used_for_comparison`。
  - daily-independent replay 的 `profile_results` 不再用于 continuous portfolio comparison。
  - `impact_status` 运行时校验必须属于保守枚举。
- `tests/trading_engine/test_shadow_parameter_impact.py`
  - 增加 JSON/Markdown 危险语义扫描。
  - 增加 policy snapshot、`production_effect=none`、gate explanation、warning explanation、
    `continuous_replay_missing` 和 daily-independent replay 防误用测试。

### Dashboard integration files

- `src/ai_trading_system/daily_task_dashboard.py`
  - AST 与父提交一致，仅 Black 格式化。
  - 未发现 dashboard production logic 变化。
- `tests/test_daily_task_dashboard.py`
  - 增加 dashboard Shadow Impact 输出危险语义扫描。
  - 增加 fixture 中 policy snapshot、impact gate explanation 和 continuous replay source
    fields。
  - 增加只读边界守卫，覆盖 replay runner、candidate runner、broker、shadow iteration 和
    shadow parameter promotion import path。

### Docs/test files

文档语义更新：

- `docs/artifact_catalog.md`
- `docs/requirements/shadow_parameter_impact_evaluation_2026-05-17.md`
- `docs/system_flow.md`
- `docs/task_register.md`

测试语义更新：

- `tests/trading_engine/test_shadow_parameter_impact.py`
- `tests/test_daily_task_dashboard.py`

测试文件中仅 Black 格式化、AST 未变的文件列在 pure formatting files。

### Pure formatting files

以下 128 个 Python 文件 AST 与父提交一致，判定为纯 Black 格式化：

- `scripts/run_paper_trading_demo.py`
- `scripts/run_paper_trading_replay.py`
- `src/ai_trading_system/agent_request_cache.py`
- `src/ai_trading_system/alerts.py`
- `src/ai_trading_system/backtest/audit.py`
- `src/ai_trading_system/backtest/daily.py`
- `src/ai_trading_system/backtest/gate_attribution.py`
- `src/ai_trading_system/backtest/input_gaps.py`
- `src/ai_trading_system/backtest/lag_sensitivity.py`
- `src/ai_trading_system/backtest/pit_coverage.py`
- `src/ai_trading_system/backtest/robustness.py`
- `src/ai_trading_system/belief_state.py`
- `src/ai_trading_system/benchmark_policy.py`
- `src/ai_trading_system/calibration_protocol.py`
- `src/ai_trading_system/catalyst_calendar.py`
- `src/ai_trading_system/cli.py`
- `src/ai_trading_system/config.py`
- `src/ai_trading_system/daily_task_dashboard.py`
- `src/ai_trading_system/data/market_data.py`
- `src/ai_trading_system/data/quality.py`
- `src/ai_trading_system/data_sources.py`
- `src/ai_trading_system/decision_causal_chains.py`
- `src/ai_trading_system/decision_learning_queue.py`
- `src/ai_trading_system/decision_outcomes.py`
- `src/ai_trading_system/decision_snapshots.py`
- `src/ai_trading_system/docs_freshness.py`
- `src/ai_trading_system/evidence_dashboard.py`
- `src/ai_trading_system/execution_policy.py`
- `src/ai_trading_system/external_request_cache.py`
- `src/ai_trading_system/feature_availability.py`
- `src/ai_trading_system/features/market.py`
- `src/ai_trading_system/feedback_loop_review.py`
- `src/ai_trading_system/fmp_forward_pit.py`
- `src/ai_trading_system/focus_stock_trends.py`
- `src/ai_trading_system/fundamentals/sec_features.py`
- `src/ai_trading_system/fundamentals/sec_filings.py`
- `src/ai_trading_system/fundamentals/sec_metrics.py`
- `src/ai_trading_system/fundamentals/tsm_ir.py`
- `src/ai_trading_system/historical_replay.py`
- `src/ai_trading_system/industry_node_state.py`
- `src/ai_trading_system/llm_precheck.py`
- `src/ai_trading_system/llm_request_profiles.py`
- `src/ai_trading_system/market_evidence.py`
- `src/ai_trading_system/market_feedback_optimization.py`
- `src/ai_trading_system/official_policy_sources.py`
- `src/ai_trading_system/ops_daily.py`
- `src/ai_trading_system/order_intent_candidates.py`
- `src/ai_trading_system/parameter_candidates.py`
- `src/ai_trading_system/parameter_governance.py`
- `src/ai_trading_system/parameter_replay.py`
- `src/ai_trading_system/periodic_investment_review.py`
- `src/ai_trading_system/pit_snapshots.py`
- `src/ai_trading_system/portfolio_exposure.py`
- `src/ai_trading_system/prediction_ledger.py`
- `src/ai_trading_system/price_source_diagnostics.py`
- `src/ai_trading_system/report_traceability.py`
- `src/ai_trading_system/risk_event_candidate_triage.py`
- `src/ai_trading_system/risk_event_llm_formal.py`
- `src/ai_trading_system/risk_event_prereview.py`
- `src/ai_trading_system/risk_events.py`
- `src/ai_trading_system/rule_experiments.py`
- `src/ai_trading_system/rule_governance.py`
- `src/ai_trading_system/scenario_library.py`
- `src/ai_trading_system/scoring/daily.py`
- `src/ai_trading_system/scoring/macro_budget.py`
- `src/ai_trading_system/scoring/position_gates.py`
- `src/ai_trading_system/scoring/position_model.py`
- `src/ai_trading_system/secret_hygiene.py`
- `src/ai_trading_system/shadow_iteration.py`
- `src/ai_trading_system/shadow_weight_profiles.py`
- `src/ai_trading_system/thesis.py`
- `src/ai_trading_system/trading_engine/audit/jsonl.py`
- `src/ai_trading_system/trading_engine/config/trading_config.py`
- `src/ai_trading_system/trading_engine/execution/execution_service.py`
- `src/ai_trading_system/trading_engine/execution/paper_broker.py`
- `src/ai_trading_system/trading_engine/intent_builder.py`
- `src/ai_trading_system/trading_engine/market_snapshot_provider.py`
- `src/ai_trading_system/trading_engine/portfolio/reconciliation.py`
- `src/ai_trading_system/trading_engine/reports/trading_daily_report.py`
- `src/ai_trading_system/trading_engine/risk/pre_trade_checker.py`
- `src/ai_trading_system/valuation.py`
- `src/ai_trading_system/valuation_sources.py`
- `src/ai_trading_system/watchlist.py`
- `src/ai_trading_system/watchlist_lifecycle.py`
- `src/ai_trading_system/weight_calibration.py`
- `tests/test_alerts.py`
- `tests/test_backtest.py`
- `tests/test_benchmark_policy.py`
- `tests/test_catalyst_calendar.py`
- `tests/test_cli_direct.py`
- `tests/test_config.py`
- `tests/test_daily_scoring.py`
- `tests/test_data_download.py`
- `tests/test_data_sources.py`
- `tests/test_decision_causal_chains.py`
- `tests/test_evidence_dashboard.py`
- `tests/test_fmp_forward_pit.py`
- `tests/test_historical_replay.py`
- `tests/test_llm_precheck.py`
- `tests/test_llm_request_profiles.py`
- `tests/test_market_evidence.py`
- `tests/test_official_policy_sources.py`
- `tests/test_ops_daily.py`
- `tests/test_order_intent_candidates.py`
- `tests/test_parameter_replay.py`
- `tests/test_pit_snapshots.py`
- `tests/test_portfolio_exposure.py`
- `tests/test_price_source_diagnostics.py`
- `tests/test_risk_event_prereview.py`
- `tests/test_risk_event_sources.py`
- `tests/test_risk_events.py`
- `tests/test_rule_governance.py`
- `tests/test_run_artifacts.py`
- `tests/test_scenario_library.py`
- `tests/test_sec_filings.py`
- `tests/test_sec_metrics.py`
- `tests/test_sec_validation.py`
- `tests/test_shadow_iteration.py`
- `tests/test_shadow_weight_profiles.py`
- `tests/test_thesis.py`
- `tests/test_tsm_ir.py`
- `tests/test_valuation_sources.py`
- `tests/test_watchlist.py`
- `tests/test_weight_calibration.py`
- `tests/trading_engine/test_audit_integrity.py`
- `tests/trading_engine/test_continuous_portfolio_replay.py`
- `tests/trading_engine/test_paper_trading_replay.py`
- `tests/trading_engine/test_safety_boundaries.py`

### Unexpected touched files

未发现带有 AST 变化的 unexpected touched files。

存在 128 个范围较广的 Python 文件被 Black 机械格式化。它们不是 Shadow Impact 功能逻辑
变更，但属于为满足 `python -m black --check scripts src tests` 而纳入的格式基线修正。
AST 对比确认这些文件无 Python 语义变化。

## 135 个文件摘要

- 总计：135 files changed，`1747 insertions`，`2855 deletions`。
- Python 文件：131。
- Markdown 文档：4。
- Python AST unchanged：128。
- Python AST changed：3。
- 非 shadow-impact 相关逻辑变化：未发现。
- Dashboard production code：`src/ai_trading_system/daily_task_dashboard.py` AST unchanged，仅格式化。
- Shadow Impact production logic：仅 `src/ai_trading_system/trading_engine/reports/shadow_parameter_impact.py`
  有预期 hardening 变化。

## 语义安全检查

检查范围：

- `scripts/run_shadow_parameter_impact.py`
- `src/ai_trading_system/trading_engine/reports/shadow_parameter_impact.py`
- `src/ai_trading_system/daily_task_dashboard.py`
- `tests/trading_engine/test_shadow_parameter_impact.py`
- `tests/test_daily_task_dashboard.py`

危险语义扫描：

- `PROMOTE_TO_PRODUCTION`：生产输出相关代码无命中。
- `READY_FOR_LIVE`：生产输出相关代码无命中。
- `SHOULD_TRADE`：生产输出相关代码无命中。
- `APPROVED_FOR_TRADING`：生产输出相关代码无命中。
- `live trade approval` / `production promotion` 等短语：生产输出相关代码无命中。

状态枚举：

- `impact_status` 允许集合严格为：
  - `INSUFFICIENT_DATA`
  - `OBSERVE_ONLY`
  - `SHADOW_PROMISING_BUT_LIMITED`
  - `NO_CLEAR_IMPROVEMENT`
  - `SHADOW_UNRELIABLE`
  - `LOW_DATA_QUALITY`
- `_build_window_evaluation` 对生成状态做运行时 membership check，超出集合会抛出
  `ValueError`。

Production effect：

- Shadow Impact JSON 顶层固定输出 `production_effect=none`。
- `impact_gate` 内部也固定输出 `production_effect=none`。
- Dashboard fixture 和 payload assertions 覆盖 `production_effect=none`。

## Policy / explanation / replay source 检查

Shadow Impact JSON 字段：

- `policy_id`：存在，默认 `shadow_parameter_impact_policy`。
- `policy_version`：存在，当前 policy version 为 `1`。
- `thresholds_snapshot`：存在，来自 `config/shadow_parameter_impact_policy.yaml`。
- `production_effect`：存在且为 `none`。

Impact gate：

- `impact_gate.explanation` 存在。
- `impact_gate.blocking_reason_explanations` 存在。
- `impact_gate.warning_explanations` 存在。
- `impact_gate.reason_explanations` 合并 blocker 和 warning explanation。

Continuous replay source：

- 无 replay artifact 时：
  - `continuous_replay.source_artifact.exists=false`
  - `path=""`
  - `mode="missing"`
  - `date_range.start=""`
  - `date_range.end=""`
  - `used_for_comparison=false`
  - `warning_codes` 包含 `continuous_replay_missing`
- continuous portfolio replay 时：
  - 记录 artifact path、mode、date range。
  - `used_for_comparison=true`。
  - 不输出 `continuous_replay_missing`。
- daily-independent replay 时：
  - 记录 artifact path、mode、date range。
  - `used_for_comparison=false`。
  - `continuous_replay.available=false`。
  - profile `final_equity` / `max_drawdown_pct` 不作为 continuous comparison 使用。
  - `warning_codes` 包含 `continuous_replay_missing` 和 `daily_independent_only`。

## 本地验证结果

| Command | Result |
|---|---|
| `python -m pytest tests/trading_engine/test_shadow_parameter_impact.py` | PASS, 6 passed |
| `python -m pytest tests/trading_engine` | PASS, 67 passed |
| `python -m pytest tests/test_daily_task_dashboard.py` | PASS, 8 passed |
| `python -m pytest` | PASS, 649 passed |
| `python -m ruff check scripts src tests` | PASS, all checks passed |
| `python -m black --check scripts src tests` | PASS, 240 files unchanged |

额外检查：

- `git diff-tree --name-only -r 40c15b5`：135 files。
- Python AST compare against parent commit：128 Python files unchanged, 3 Python files changed.
- Semantic safety inline script：PASS。

## Review finding

No findings. 当前证据支持以下判断：

- CI green。
- Shadow Impact Hardening 语义安全。
- 135 个文件的大范围改动除预期 Shadow Impact hardening 和相关测试/文档外，为 Black
  机械格式化。
- 未发现非 shadow-impact 相关逻辑变更。
- 不需要 revert 或 fix commit。
