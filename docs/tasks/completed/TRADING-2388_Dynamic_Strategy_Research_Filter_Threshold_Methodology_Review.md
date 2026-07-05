# TRADING-2388 Dynamic Strategy Research Filter Threshold Methodology Review

最后更新：2026-07-06

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2388_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW`
- CLI：`aits research strategies dynamic-strategy-research-filter-threshold-methodology-review`
- 真实 run status：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- 下一路由：`TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`

## 完成内容

- 新增 research filter threshold methodology review builder。
- 读取 TRADING-2364 / 2365 / 2366 / 2375 / 2376 / 2379 / 2383 / 2386 / 2387 prior validated artifacts 和既有 threshold constants。
- 输出 threshold methodology review、threshold inventory、gate taxonomy、candidate threshold outcome matrix 和 recommended gate policy proposal。
- 将 research-only observation gate 与 paper-shadow gate 明确分层，并保留 owner-review-only 中间层。
- 更新 report registry、artifact catalog、system flow、task register、requirements doc 和 focused tests。

## 关键结论

- 当前验收标准不是纯历史统计阈值；它混合了项目内实验、工程安全边界、保守 heuristic 和 owner risk preference。
- Research-only observation 是 artifact-only / no-side-effect gate，门槛应低于 paper-shadow gate。
- Reference candidate 不应 auto-accept，但不应 hard-block owner review；推荐策略为 `BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`。
- `time_slice_pass_rate`、`regime_expectation_score`、`drawdown_materiality`、`return_per_drawdown_penalty` 和 owner-review boundary 需要后续统计校准。
- 2388 不批准 observation、不修改真实 gate、不启用 paper-shadow / production / broker。

## 验证

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_research_filter_threshold_methodology_review.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：573 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1285 reports PASS
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py`：6 passed
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=447 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260705T174509Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning

## 数据质量门禁说明

本任务未运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated research artifacts 和既有 threshold constants，不读取 fresh cached market data、不重新 backtest、不生成 signal/scoring、daily report 或交易建议。
