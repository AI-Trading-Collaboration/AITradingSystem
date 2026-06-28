# TRADING-1187 to 1200 Codex Execution Semantics Repair and Dynamic Rebacktest

最后更新：2026-06-28

## 背景

TRADING-1165～1186 已经建立 execution semantics 审计层，并明确当前 master status 为
`EXECUTION_SEMANTICS_REQUIRES_REBACKTEST`。本批次承接该结论，把旧动态策略结果从
candidate evidence 降级为 `PRE_EXECUTION_SEMANTICS`，并建立基于 actual position path
的重回测入口和 promotion gate。

本轮实现保持 research-only，不进入 paper-shadow、production 或 broker。

## 阶段拆解

1. Metric convention hardening：为 Portfolio Visualizer signoff 增加 metric namespace，
   允许 annual return 作为 static baseline reconciliation，但禁止 monthly risk metric
   直接进入 internal daily-risk gate。
2. Promotion gate freeze：旧动态 target-path / pre-semantics backtest 只能作为
   `PRE_EXECUTION_SEMANTICS_CANDIDATE_EVIDENCE`，不得用于 ranking 或 promotion。
3. Execution policy enforcement：`strategy_execution_policy_registry.yaml` 同时登记 policy
   定义和 strategy binding；缺失 binding 或字段时 fail closed。
4. Actual-path rebacktest：新增 `execution-semantics-rebacktest` CLI，输出 target path、
   actual path、lag cost、signal staleness 和 promotion readiness artifacts。
5. Documentation and catalog：更新 report registry、artifact catalog、system flow 和 tests。

## 验收标准

- 动态策略缺失 explicit execution policy 时不能 promotion。
- 只有 legacy / pre-semantics backtest 的动态策略必须输出 `REBACKTEST_REQUIRED` 和
  `NOT_PROMOTION_ELIGIBLE`。
- 静态 baseline 不被 execution-semantics rebacktest gate 错误阻断。
- Actual-path rebacktest 输出：
  - `summary.json`
  - `metrics_actual_path.json`
  - `metrics_target_path.json`
  - `target_vs_actual_position_path.csv`
  - `lag_cost_report.md`
  - `signal_staleness_report.md`
  - `execution_policy_snapshot.yaml`
  - `promotion_readiness.json`
- Promotion readiness 默认只读取 `actual_weight_path`；`target_weight_path` 只作为 diagnostic。
- 报告字段包含 `backtest_generation`、`position_path_used_for_metrics`、
  `execution_policy_id`、`execution_lag_bdays`、`rebalance_frequency`、
  `signal_validity_window_bdays`、`metric_convention_namespace`、
  `promotion_eligible` 和 `rebacktest_required`。
- 新增 focused tests 覆盖 metric convention、rebacktest gate、policy registry、
  target/actual path 和 no-lookahead semantics。

## 安全边界

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`
- 不删除旧报告，只降级其 promotion eligibility。
- 不把 Portfolio Visualizer risk metrics 当作内部 promotion gate 的风险门槛。
- 不把 target path 指标写入最终 promotion metrics。

## 进展记录

- 2026-06-27：新增需求文档并进入 `IN_PROGRESS`。本批承接
  `EXECUTION_SEMANTICS_REQUIRES_REBACKTEST`，目标是补齐 strategy binding、metric namespace
  hardening、actual-path rebacktest CLI、legacy dynamic result freeze 和 focused tests。
- 2026-06-27：实现完成并转入 `VALIDATING`。新增 metric convention namespace guard、
  strategy execution policy binding hard gate、legacy dynamic target-path result freeze、
  `execution-semantics-rebacktest` actual-path CLI 和 per-strategy artifacts；focused parallel
  pytest、execution semantics / equal-risk / external validation 回归、report/task/documentation
  contract、Ruff、compileall 和 `git diff --check` 均已通过。Dynamic promotion 仍默认 blocked，
  直到 actual-path rebacktest artifact、policy/metric namespace 和 owner manual review 全部通过。
