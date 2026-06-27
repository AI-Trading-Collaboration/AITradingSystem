# TRADING-1256～1285：Actual-Path Owner Review 与 Policy Sensitivity

最后更新：2026-06-27

## 背景

TRADING-1201～1235 已完成 execution semantics actual-path rebacktest、`dynamic_promotion_readiness.v1`、actual-path-only promotion gate、legacy dynamic result downgrade、8 策略聚合输出和 tracked evidence snapshot。Dynamic promotion 继续保持 `BLOCKED`，这是本批任务的安全前提。

当前 actual-path 下仍值得继续 owner review 的 dynamic candidates 仅限：

- `limited_adjustment`
- `dynamic_v0_5_ai_trend_confirmed_only`

暂不推进：

- `defensive_limited_adjustment`：`signal_staleness_review_fail`
- `dynamic_regime_overlay_v0_4_lower_turnover`：`lag_cost_review_warn`

## 目标

1. 基于 tracked evidence 与 runtime artifacts 生成 dynamic actual-path owner review decision。
2. 对两个 surviving candidates 做 policy sensitivity test，判断其 edge 是否依赖单一 execution policy。
3. 继续禁止 dynamic promotion、paper-shadow 自动加入、production config mutation 和 broker/order effect。
4. 确保 promotion/readiness/owner decision 的正向依据只来自 actual-path metrics；target-path metrics 仅用于 diagnostic。
5. 登记 TQQQ 2025-11-20 `prices_adjustment_ratio_jump` 为非阻断 data-quality watchlist。

## 阶段拆解

### Stage A：Owner Review Decision

读取：

- `docs/research/execution_semantics_actual_path_rebacktest_review.md`
- `docs/research/artifact_snapshots/execution_semantics_actual_path_rebacktest_snapshot.yaml`
- `docs/research/execution_semantics_strategy_survival_review.md`（如存在）
- `inputs/research_reviews/execution_semantics_strategy_survival_matrix.yaml`（如存在）
- `outputs/research_strategies/execution_semantics/` 下 actual-path runtime artifacts

输出：

- `docs/research/dynamic_actual_path_owner_review_decision.md`
- `inputs/research_reviews/dynamic_actual_path_owner_review_decision.yaml`

每个 candidate 必须记录 actual-path metrics、相对 `no_trade` / `100_qqq` / `qqq_60_sgov_40` / `qqq_50_sgov_50` 差异、target-vs-actual diagnostic gap、lag/staleness materiality、turnover、readiness blocker 和人工 owner decision 字段。

### Stage B：Policy Sensitivity

只测试：

- `limited_adjustment`
- `dynamic_v0_5_ai_trend_confirmed_only`

对照：

- `no_trade`
- `100_qqq`
- `qqq_60_sgov_40`
- `qqq_50_sgov_50`

矩阵维度：

- `execution_lag_days`: 0, 1, 2
- `rebalance_frequency`: `next_trading_day`, `weekly`, `monthly`
- `signal_validity_window_days`: 1, 3, 5, 10, 20
- `turnover_constraint`: `existing_default`, `relaxed`, `strict`

输出 runtime artifacts：

- `outputs/research_strategies/policy_sensitivity/index.json`
- `outputs/research_strategies/policy_sensitivity/leaderboard_actual_path.csv`
- `outputs/research_strategies/policy_sensitivity/target_vs_actual_gap_summary.csv`
- `outputs/research_strategies/policy_sensitivity/promotion_readiness_summary.json`
- `outputs/research_strategies/policy_sensitivity/policy_sensitivity_matrix.csv`
- `outputs/research_strategies/policy_sensitivity/policy_sensitivity_summary.json`

输出 tracked reports：

- `docs/research/dynamic_actual_path_policy_sensitivity_review.md`
- `inputs/research_reviews/dynamic_actual_path_policy_sensitivity_matrix.yaml`

分类值：

- `POLICY_STABLE`
- `POLICY_SENSITIVE_BUT_WATCHABLE`
- `POLICY_FRAGILE`
- `INSUFFICIENT_EVIDENCE`

### Stage C：Data Warning Watchlist

登记：

- `docs/research/data_quality_watchlist.md`
- `inputs/data_quality_watchlist/tqqq_adjustment_ratio_jump_2025_11_20.yaml`

当前状态为 `NON_BLOCKING_WARNING`。如果 TQQQ 未来进入正式 strategy universe，则升级为 `BLOCKING_DATA_QUALITY_REVIEW`。

## 验收标准

1. Owner review decision report/YAML 已生成并登记。
2. 两个 surviving dynamic candidates 均有 explicit owner decision 字段，且 `owner_manual_review_required=true`。
3. Policy sensitivity matrix 已生成，且 ranking/classification 只使用 actual-path metrics。
4. Target-path metrics 仅作为 target-vs-actual / lag / staleness diagnostic，不参与 promotion/readiness 正向结论。
5. Dynamic promotion 继续 `BLOCKED`；不得输出 `PROMOTED`。
6. TQQQ adjustment warning 已作为 non-blocking watchlist 登记。
7. `docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`、`config/report_registry.yaml` 与相关测试同步更新。
8. 指定 Ruff、compileall、focused parallel pytest 和 `git diff --check` 通过。

## 进展记录

- 2026-06-27：新增需求文档并登记任务；进入实现阶段。
- 2026-06-27：实现完成并转入验证。新增 owner review decision CLI/report、staged policy sensitivity CLI/runtime artifacts、TQQQ non-blocking data-quality watchlist、report registry / artifact catalog / system flow / focused tests。真实运行结论：两个 surviving candidates 的 owner recommendation 均为 `WATCH_ONLY`；policy sensitivity 均为 `POLICY_SENSITIVE_BUT_WATCHABLE`，best surviving candidate 为 `limited_adjustment`，recommended next action 仍为 `WATCH_ONLY`；dynamic promotion 继续 `BLOCKED`。
- 2026-06-27：验证通过 `python -m ruff check src tests`、`python -m compileall -q src tests`、`python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py tests/test_execution_semantics_rebacktest_gate.py`、`python -m pytest -n 16 --dist loadfile tests/test_external_validation.py`、`python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py` 和 `git diff --check`。
