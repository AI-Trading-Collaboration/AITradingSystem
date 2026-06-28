# TRADING-2278 First-Layer Challenger Matrix v2 Rerun

最后更新：2026-06-29

## 背景

TRADING-2275 将 first-layer gate policy 调整为分层语义，TRADING-2277 将
active selection 拆成 research selection、owner-review queue、offline validation queue
和 promotion boundary。TRADING-2278 负责用这套 v2 policy 重新生成 first-layer
challenger matrix，确认候选不再被旧 active selection binary block 误杀。

## 范围

- 通过可复现 code path 读取 / 再生 TRADING-2275 gate policy v2 和 TRADING-2277
  active selection policy v2 evidence。
- 输出 v2 challenger matrix，并保留旧 current active selection state 到 v2 state 的
  `candidate_state_transition_from_v1`。
- 输出 research candidate queue、owner review queue、blocked queue 和 promotion
  boundary check。
- 显式验证：
  - `wf_504d_baseline` 仍为 `OWNER_REVIEW_REQUIRED`，不得被改写为 `BLOCKED`。
  - `wf_378d_initial` 仍为 `RESEARCH_ACCEPTED` 或 `OFFLINE_VALIDATION_READY`。
  - `OWNER_REVIEW_REQUIRED` 不得自动变成 `PROMOTION_READY`。
  - `RESEARCH_ACCEPTED` 不得自动触发 promotion。
  - promotion、paper-shadow、production 和 broker action 继续固定 false/none。

## 输出

- `outputs/research_trends/first_layer_challenger_matrix_v2/first_layer_challenger_matrix_v2.json`
- `docs/research/first_layer_challenger_report_v2.md`
- `outputs/research_trends/first_layer_challenger_matrix_v2/research_candidate_queue_v2.json`
- `outputs/research_trends/first_layer_challenger_matrix_v2/owner_review_queue_v2.json`
- `outputs/research_trends/first_layer_challenger_matrix_v2/blocked_candidate_queue_v2.json`
- `docs/research/promotion_boundary_check_v2.md`

## 边界

本批只重跑 research matrix / queues，不训练模型，不打开 promotion，不进入
paper-shadow，不修改 production，不触发 broker。所有产物必须固定：
`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、
`broker_action=none`。

## 进展

- 2026-06-29：新增并进入 `IN_PROGRESS`；本批承接 TRADING-2275 / TRADING-2277，
  目标是验证 v2 selection queues，不是产生 promotion candidate。
- 2026-06-29：实现完成并转入 `VALIDATING`；真实 run 输出 candidate_count=`10`、
  research_accepted=`1`、offline_validation_ready=`4`、owner_review_required=`1`、
  blocked=`4`、promotion_ready=`0`。`wf_504d_baseline` 从 v1 `BLOCKED` 迁移为
  v2 `OWNER_REVIEW_REQUIRED`，`wf_378d_initial` 从 v1 `BLOCKED` 迁移为 v2
  `RESEARCH_ACCEPTED`；promotion boundary check 全部通过。Focused parallel pytest
  52 passed。
- 2026-06-29：最终验证通过 Ruff、compileall、focused parallel pytest（52 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T152152Z/test_runtime_summary.json`。
