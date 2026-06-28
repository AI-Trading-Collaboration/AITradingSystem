# TRADING-2280 First-Layer Candidate-Level Actual-Path Validation

最后更新：2026-06-29

## 背景

TRADING-2278 / 2279 已把 first-layer 候选从 binary block 拆成 research、owner
review、offline validation 和 blocked queues。TRADING-2280 的目标是对所有非
`BLOCKED` 候选补齐 candidate-level actual-path validation，确认真实后验质量，
并为后续 `wf_504d` / `wf_378d` neighborhood search 提供可比基准。

## 范围

- 覆盖 `wf_504d_baseline`、`wf_378d_initial` 和 4 个
  `OFFLINE_VALIDATION_READY` challenger rows。
- 对已有 actual-path evidence 的 `wf_*` 候选输出完整 metrics 和状态稳定性判断。
- 对缺少 candidate-level signal / prediction artifact 的 offline challenger rows，
  不补造收益、风险或 objective metrics；必须显式标记 actual-path validation
  blocker，并按证据状态 reclassify。
- 输出 updated research / owner-review / offline-validation queues，但不打开
  promotion、paper-shadow、production 或 broker。

## 输出

- `docs/research/candidate_actual_path_validation_report.md`
- `outputs/research_trends/first_layer_candidate_actual_path_validation/candidate_actual_path_matrix.json`
- `outputs/research_trends/first_layer_candidate_actual_path_validation/candidate_risk_attribution_matrix.json`
- `docs/research/candidate_state_reclassification_report.md`
- `outputs/research_trends/first_layer_candidate_actual_path_validation/updated_research_candidate_queue.json`
- `outputs/research_trends/first_layer_candidate_actual_path_validation/updated_owner_review_queue.json`
- `outputs/research_trends/first_layer_candidate_actual_path_validation/updated_offline_validation_queue.json`

## 设计决策

最佳完整解法是对每个 candidate 运行 candidate signal artifact -> actual-path
evaluator -> objective/risk attribution matrix。当前 `wf_504d_baseline` 和
`wf_378d_initial` 已有 frozen actual-path evidence；4 个 offline challenger rows
只有 experiment definition、required proxies 和 target objective terms，没有可执行
candidate signal / prediction artifact。因此本批必须把这 4 行标成
`INCONCLUSIVE` / missing candidate signal artifact，而不是把 baseline evidence
复制成 candidate-level validation。

## 边界

`RESEARCH_ACCEPTED` 不等于 promotion；`OWNER_REVIEW_REQUIRED` 不等于 promotion；
`OFFLINE_VALIDATION_READY` 不等于 paper-shadow。所有产物固定
`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、
`broker_action=none`。

## 验收标准

- 每个覆盖候选都有 `candidate_id`、`previous_state`、`updated_state`、
  `utility_rank`、`primary_risk_flag`、`continue_research`、
  `expand_neighborhood` 和 `promotion_ready=false`。
- actual-path 可用候选输出 actual_path_utility、return/drawdown、Calmar/Sharpe、
  turnover/net-of-cost、defensive probe、2022 stress 和 2023+ dependency。
- candidate-level unavailable 指标必须有 status，不得默认为 0。
- 4 个 offline challenger rows 必须显式披露缺少 candidate signal artifact，并不得
  留在可误解为已完成 actual-path 的 state。
- CLI、report registry、artifact catalog、system flow 和 focused tests 同步更新。

## 进展

- 2026-06-29：新增并进入 `IN_PROGRESS`；已确认 offline-ready rows 缺少可执行
  candidate signal / prediction artifact，本批按 no-silent-workaround 规则输出
  validation blocker 和 reclassification，而不是伪造 actual-path metrics。
- 2026-06-29：实现完成并转入 `VALIDATING`；新增 CLI
  `aits research trends first-layer-candidate-actual-path-validation`，并生成
  candidate actual-path validation report/matrix、risk attribution matrix、state
  reclassification report 和 updated research / owner-review / offline-validation
  queues。真实 run 覆盖 6 个非 `BLOCKED` rows，其中 2 个已有 candidate-level
  actual-path evidence，4 个 offline challenger rows 因缺少 candidate signal /
  prediction artifact 转为 `INCONCLUSIVE`。
- 2026-06-29：验证通过 Ruff、compileall、focused parallel pytest（54 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T155922Z/test_runtime_summary.json`。

## 当前结论

- `wf_504d_baseline` 保持 `OWNER_REVIEW_REQUIRED`，actual_path_utility=`0.070283`，
  primary_risk_flag=`2023_plus_dependency`，建议作为 `wf_504d` 邻域扩展基准继续
  owner review；promotion_ready=`false`。
- `wf_378d_initial` 保持 `RESEARCH_ACCEPTED`，actual_path_utility=`0.041538`，
  primary_risk_flag=`coverage_rule_not_satisfied`，可作为较稳的 `wf_378d` 安全基线；
  promotion_ready=`false`。
- `baseline`、`baseline_plus_trend_structure`、`risk_appetite` 和
  `volatility_regime` 从 `OFFLINE_VALIDATION_READY` 转为 `INCONCLUSIVE`；原因是缺少
  candidate-level signal / prediction artifact，不是 actual-path utility 失败。
