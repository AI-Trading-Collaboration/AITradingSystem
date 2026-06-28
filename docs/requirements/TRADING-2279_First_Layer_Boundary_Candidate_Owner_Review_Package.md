# TRADING-2279 First-Layer Boundary Candidate Owner Review Package

最后更新：2026-06-29

## 背景

TRADING-2278 已用 gate policy v2 和 active selection policy v2 重跑 first-layer
challenger matrix，确认 `wf_504d_baseline` 不再被旧 binary active selection 误杀。
TRADING-2279 整理 owner review package，回答 boundary candidates 是否值得继续研究、
只保留 diagnostic baseline，或应重新降级。

## 范围

- 对 `wf_504d_baseline` 和 `wf_378d_initial` 输出指标对比、风险 flag、tradeoff
  summary 和 owner-review decision 建议。
- 汇总 4 个 `OFFLINE_VALIDATION_READY` 候选的共同特征。
- 汇总 4 个 `BLOCKED` 候选的失败原因，并检查是否存在明显误分类。
- 给出下一步实验计划，特别是是否需要扩大 `wf_504d` / `wf_378d` 附近参数邻域搜索。
- 对不可用的 candidate-level false risk、beta / TQQQ、benchmark consistency、lead-time
  和 recovery-delay 指标必须显式标明 unavailable / required action。

## 输出

- `docs/research/first_layer_boundary_candidate_owner_review.md`
- `outputs/research_trends/first_layer_boundary_owner_review/boundary_candidate_comparison_matrix.json`
- `docs/research/owner_review_candidate_tradeoff_summary.md`
- `outputs/research_trends/first_layer_boundary_owner_review/offline_validation_ready_candidate_summary.json`
- `outputs/research_trends/first_layer_boundary_owner_review/blocked_candidate_failure_reason_summary.json`
- `docs/research/recommended_next_experiment_plan.md`

## 边界

`OWNER_REVIEW_REQUIRED` 不等于 promotion；`RESEARCH_ACCEPTED` 不等于 paper-shadow。
本批只生成 owner-review package，不训练模型，不扩大实验，不打开 promotion、
paper-shadow、production 或 broker。所有产物固定 `promotion_allowed=false`、
`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 进展

- 2026-06-29：新增并进入 `IN_PROGRESS`；本批承接 TRADING-2278 matrix v2，
  目标是整理 owner review evidence 和下一步实验计划，不产生 promotion candidate。
- 2026-06-29：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-boundary-owner-review`，并生成 owner-review
  Markdown、comparison matrix、tradeoff summary、offline-ready summary、blocked
  failure reason summary 和 next experiment plan。真实 run 显示
  `wf_504d_baseline` 为 `OWNER_REVIEW_REQUIRED` / `expand_neighborhood`，
  `wf_378d_initial` 为 `RESEARCH_ACCEPTED` / `continue_research`；
  4 个 `OFFLINE_VALIDATION_READY` 候选仍需 future candidate-level actual-path
  backtest，4 个 `BLOCKED` 候选未发现明显误分类。promotion、paper-shadow、
  production 和 broker 继续 false/none。
- 2026-06-29：验证通过 Ruff、compileall、focused parallel pytest（51 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T153924Z/test_runtime_summary.json`。
