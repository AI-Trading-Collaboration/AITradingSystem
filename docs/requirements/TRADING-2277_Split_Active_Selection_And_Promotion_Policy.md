# TRADING-2277 Split Active Selection And Promotion Policy

最后更新：2026-06-28

## 背景

TRADING-2276 证明 current active selection rule 的边际效用为 `negative`：
它把 current accept 压到 `0`，并把 gate policy v2 下应为
`OWNER_REVIEW_REQUIRED` 的 `wf_504d_baseline` 当作 `BLOCKED`。本批把
first-layer active selection 从 binary accept/block 模式拆成 research selection
与 promotion selection，避免 owner-review 候选被误杀。

## 范围

- 明确定义 selection states：`RESEARCH_ACCEPTED`、`OWNER_REVIEW_REQUIRED`、
  `OFFLINE_VALIDATION_READY`、`BLOCKED`、`INCONCLUSIVE`、`PROMOTION_READY`。
- active selection 只决定 ranked review、owner review queue、offline validation queue
  和 blocked list，不再直接决定 promotion。
- 若 `gate_policy_v2_state=OWNER_REVIEW_REQUIRED`，不得改写为 `BLOCKED`，必须进入
  owner-review queue，并输出 risk flags / tradeoff summary。
- 若 `gate_policy_v2_state=ACCEPTED`，可进入 research accepted / offline validation，
  但不得自动进入 promotion。
- severe defensive regression、PIT/no-lookahead violation、data quality failure 仍为
  hard block。
- 对非 blocked 候选输出 ranked review queue，排序字段包括 utility、false risk deltas、
  drawdown、turnover、benchmark consistency、2022 stress exposure、2023+ dependency
  flag、beta / TQQQ attribution。
- 显式覆盖 boundary candidates：
  - `wf_504d_baseline`: utility=`0.070283`，v2 state=`OWNER_REVIEW_REQUIRED`，
    v2 policy state 应保持 `OWNER_REVIEW_REQUIRED`。
  - `wf_378d_initial`: utility=`0.041538`，v2 state=`ACCEPTED`，v2 policy state
    应进入 `RESEARCH_ACCEPTED` 或 `OFFLINE_VALIDATION_READY`。

## 输出

- `docs/research/active_selection_policy_v2.md`
- `outputs/research_trends/first_layer_active_selection_policy_v2/active_selection_policy_v2.yaml`
- `outputs/research_trends/first_layer_active_selection_policy_v2/research_candidate_queue.json`
- `outputs/research_trends/first_layer_active_selection_policy_v2/owner_review_queue.json`
- `docs/research/promotion_boundary_report.md`
- `outputs/research_trends/first_layer_active_selection_policy_v2/updated_challenger_selection_matrix.json`

## 边界

`RESEARCH_ACCEPTED` 不等于 promotion；`OWNER_REVIEW_REQUIRED` 不等于 `BLOCKED`；
`OFFLINE_VALIDATION_READY` 不等于 paper-shadow。Promotion、paper-shadow、production
和 broker action 继续固定 false/none。

## 进展

- 2026-06-28：根据 owner 最新要求调整 TRADING-2277 范围并进入 `IN_PROGRESS`；
  旧的 gate policy v2 challenger rerun 暂缓到 split selection / promotion policy
  完成之后。实现必须通过可复现 TRADING-2276 code path 生成 source evidence。
- 2026-06-28：实现完成并转入 `VALIDATING`；真实 run 输出 state counts
  `RESEARCH_ACCEPTED:1 / OWNER_REVIEW_REQUIRED:1 / OFFLINE_VALIDATION_READY:4 /
  BLOCKED:4 / INCONCLUSIVE:0 / PROMOTION_READY:0`。`wf_504d_baseline` 保持
  `OWNER_REVIEW_REQUIRED`，`wf_378d_initial` 进入 `RESEARCH_ACCEPTED`；promotion、
  paper-shadow、production 和 broker action 继续 false/none。Focused parallel pytest
  `tests/test_first_layer_active_selection_policy_v2.py` 通过。
- 2026-06-28：最终验证通过 Ruff、compileall、focused parallel pytest（49 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T150616Z/test_runtime_summary.json`。
