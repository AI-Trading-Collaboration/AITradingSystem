# TRADING-1201 to 1220 Execution Semantics Actual-Path Rebacktest Closeout

## 背景

TRADING-1187～1200 已经建立 execution-semantics-aware actual-path rebacktest
基础框架，但 dynamic promotion 仍保持 blocked。当前批次的目标不是解除 promotion，
而是把阻断项拆成机器可读 checklist、只允许 actual-path metrics 进入决策输入，并生成
多策略 actual-path 聚合复核包。

本批继续保持 research-only，不进入 paper-shadow、production 或 broker。

## 阶段拆解

1. Promotion readiness schema v1：把 `promotion_readiness.json` 升级为
   `dynamic_promotion_readiness.v1` checklist，`final_status` 只由 check 状态派生。
2. Actual-path-only gate：promotion/readiness 决策只读取 `metrics_actual_path.json`
   口径，`metrics_target_path.json` 仅保留 diagnostic role。
3. Metric namespace hardening：新 runtime artifacts 使用 `actual_path_*`、
   `target_path_*`、`external_pv_*` 和 `target_vs_actual_*` 明确命名。
4. Policy binding enforcement：缺 strategy binding 或 policy definition 时 fail closed；
   每个 per-strategy artifact 记录 policy hash。
5. Multi-strategy aggregation：默认覆盖 policy registry 中的核心 static/dynamic
   策略，生成 `index.json`、`leaderboard_actual_path.csv`、
   `target_vs_actual_gap_summary.csv`、`promotion_readiness_summary.json` 和
   `owner_review_pack.md`。
6. Materiality review：把 execution lag 和 signal staleness 的 materiality summary 接入
   readiness checklist；本批不启用自动 waiver，只设计 schema。
7. Legacy evidence deprecation：旧 dynamic pre-execution-semantics result 标记为
   `PRE_EXECUTION_SEMANTICS_LEGACY_EVIDENCE`，不得进入 promotion gate 输入。
8. Documentation contract：同步 report registry、artifact catalog、system flow、task
   register 和 focused tests。

## 验收标准

- 每个 readiness blocking reason 都有 `status`、`severity`、`required_action` 和
  `evidence_artifact`。
- 任意 `critical` check 为 `fail` 或 `pending` 时，`final_status=blocked`。
- `owner_manual_review` 默认 `pending`，自动测试不能把它改成 `pass`。
- `metrics_target_path.json` 明确为 diagnostic-only，不能解除 promotion 阻断。
- 新 actual-path artifacts 不再使用模糊 promotion-facing 指标名作为主输出字段。
- 默认多策略 rebacktest 覆盖 no-trade、static QQQ/SGOV baseline、limited adjustment、
  defensive limited adjustment、v0.4 lower-turnover 和 v0.5 AI trend confirmed 策略。
- 聚合 leaderboard 只使用 actual-path metrics 排序。
- 缺 policy binding 的策略写入 `index.json` blocked row，而不是被跳过。
- owner review pack 明确展示 target-path diagnostic-only、actual-path leaderboard、
  target-vs-actual gap、lag/staleness summary 和 manual signoff checklist。
- Dynamic promotion 仍保持 blocked，除非未来 owner manual review 明确签署。

## 安全边界

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`
- 不调整 execution policy 以提高通过率。
- 不把 target-path metrics 用于 promotion、primary ranking 或 paper-shadow eligibility。
- 不把 Portfolio Visualizer monthly risk metrics 与内部 daily risk metrics 混用。

## 进展记录

- 2026-06-27：新增需求文档并进入 `IN_PROGRESS`。本批承接附件
  TRADING-1201～1220，范围为 readiness checklist、actual-path-only promotion gate、
  metric namespace hardening、多策略 actual-path aggregation、owner review pack 和
  legacy evidence deprecation；dynamic promotion 必须继续 blocked。
- 2026-06-27：实现完成并转入 `VALIDATING`。`promotion_readiness.json` 升级为
  `dynamic_promotion_readiness.v1` checklist，`final_status` 由 checks 派生；
  `metrics_actual_path.json` 使用 `actual_path_*` 决策字段，`metrics_target_path.json`
  标记为 diagnostic-only；每个 strategy artifact 写入 policy hash 和 normalized
  execution policy contract；默认多策略 run 输出 `index.json`、
  `leaderboard_actual_path.csv`、`target_vs_actual_gap_summary.csv`、
  `promotion_readiness_summary.json` 和 `owner_review_pack.md`。真实 CLI dry-run 已生成
  8 个策略 runtime artifacts，dynamic promotion 仍为 `BLOCKED`，owner manual review
  继续 pending。
