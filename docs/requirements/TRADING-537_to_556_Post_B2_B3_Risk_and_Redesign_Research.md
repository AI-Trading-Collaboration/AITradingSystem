# TRADING-537 to 556 Post B2/B3 Risk and Redesign Research

最后更新：2026-06-19

## 背景

TRADING-530~536 已完成 B2 risk-overlay path 与 B3 branching checkpoint：

- TRADING-530：`B2_RISK_OVERLAY_MIXED`
- TRADING-531：`B3_REDESIGN_REQUIRED`
- TRADING-532：`B1_OPTIONAL_WRAPPER`
- TRADING-533：`CONTINUE_B2_ONLY_PATH`
- TRADING-534：`B2_ONLY_NEEDS_MORE_EVIDENCE`
- TRADING-535：`B3_REDESIGN_HYPOTHESES_READY`
- TRADING-536：`CONTINUE_B2_ONLY_RESEARCH`

本批次继续保持 research-only / manual-review-only，不运行 B5/B6/v3，不访问
untouched holdout，不生成 official target weights，不激活 paper-shadow，不触发
broker/order/live trading，不修改 production state，不自动 append owner decision。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-537|冻结 B2-only research scope|VALIDATING: `B2_ONLY_RESEARCH_SCOPE_PASS`|
|TRADING-538|扩展 B2 risk-heavy window catalog|VALIDATING: `B2_RISK_HEAVY_WINDOW_CATALOG_READY`|
|TRADING-539|B2-only risk-heavy diagnostic backfill backfill summary|VALIDATING: `B2_RISK_HEAVY_BACKFILL_COMPLETE`|
|TRADING-540|B2 false risk-off / re-entry attribution|VALIDATING: `B2_PROTECTS_BUT_REENTRY_LAG_HIGH`|
|TRADING-541|B2 cost and benchmark survival review|VALIDATING: `B2_COST_BENCHMARK_MIXED`|
|TRADING-542|B2-only research gate|VALIDATING: `B2_ONLY_NEEDS_MORE_EVIDENCE`|
|TRADING-543|冻结 B3 redesign constraints|VALIDATING: `B3_REDESIGN_CONSTRAINTS_FROZEN`|
|TRADING-544|排序 B3 redesign hypotheses|VALIDATING: `B3_REDESIGN_HYPOTHESES_RANKED`|
|TRADING-545|B3 signal-direction precheck|VALIDATING: `B3_SIGNAL_DIRECTION_PRECHECK_MIXED`|
|TRADING-546|B3 redesigned mini-backfill gate/blocker|VALIDATING: `B3_REDESIGNED_MINI_BACKFILL_BLOCKED`|
|TRADING-547|B3 redesign gate|VALIDATING: `B3_REDESIGN_RETURN_TO_HYPOTHESIS`|
|TRADING-548|B1 wrapper compatibility with B2|VALIDATING: `B1_WRAPPER_MIXED_WITH_B2`|
|TRADING-549|B1 wrapper compatibility with redesigned B3|VALIDATING: `B1_B3_WRAPPER_TEST_BLOCKED_NO_VALID_B3`|
|TRADING-550|Post-B2/B3 branch synthesis|VALIDATING: `CONTINUE_B2_ONLY_RESEARCH`|
|TRADING-551|Retest B4 with redesigned B3 gate|VALIDATING: `B4_REDESIGNED_RETEST_BLOCKED_NO_VALID_B3`|
|TRADING-552|B5 readmission after redesigned B4 gate|VALIDATING: `B5_READMISSION_BLOCKED`|
|TRADING-553|Research cadence controller|VALIDATING: `RESEARCH_CADENCE_CONTROLLER_READY`|
|TRADING-554|Candidate exploration backlog manager|VALIDATING: `BACKLOG_READY`|
|TRADING-555|Monthly research program review|VALIDATING: `MONTHLY_RESEARCH_PROGRAM_REVIEW_READY`|
|TRADING-556|Final branch decision snapshot|VALIDATING: `CONTINUE_B2_ONLY_PATH`|

## 输入与边界

- 输入为 canonical TRADING-525~536 `docs/research` artifacts、本地 research window
  catalog 和 cached data quality gate。
- 输出前必须重新运行同一路径 `aits validate-data` gate，并在所有 artifact 中披露状态。
- B2-only 路线只允许 fast asymmetric risk overlay 相关机制；不得混入 B3 slow tilt、
  B5 confidence、B6 regime、P0 mixed allocator 或 official target weights。
- B3 redesign 只允许约束冻结、hypothesis ranking 和 signal-direction precheck；若 precheck
  未明确 PASS，TRADING-546/547 必须 fail closed，不得生成 redesigned B3 weight 或 B4 retest。
- B1 wrapper 只能作为 optional research wrapper 评估，不得默认变成通用执行层。

## 验收标准

- 生成 20 个 canonical JSON/Markdown artifacts：
  `b2_only_research_scope`、`b2_risk_heavy_window_catalog`、
  `b2_only_risk_heavy_diagnostic_backfill`、`b2_false_risk_off_reentry_attribution`、
  `b2_cost_benchmark_survival_review`、`b2_only_research_gate`、
  `b3_redesign_constraints`、`b3_redesign_hypothesis_ranking`、
  `b3_signal_direction_precheck`、`b3_redesigned_mini_backfill`、
  `b3_redesign_gate`、`b1_wrapper_compatibility_with_b2`、
  `b1_wrapper_compatibility_with_redesigned_b3`、`post_b2_b3_branch_synthesis`,
  `retest_b4_with_redesigned_b3`、`b5_readmission_after_redesigned_b4`,
  `research_cadence_controller`、`candidate_exploration_backlog_manager`,
  `monthly_research_program_review` 和 `final_branch_decision_snapshot`。
- 所有 artifact 披露 `market_regime=ai_after_chatgpt`、requested date range、
  data quality status、source artifacts、Reader Brief 和 safety boundary。
- `b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`，直到 redesigned B4 证明
  positive/additive；当前批次不得批准 paper-shadow、extended shadow 或 live trading。
- 更新 `docs/system_flow.md` 和 `docs/artifact_catalog.md`。
- Focused tests、JSON parse、ruff、compileall 和 `git diff --check` 通过。

## 状态记录

- 2026-06-19：新增本批次，原因：owner 附件要求在 TRADING-530~536 后继续 B2-only
  risk-heavy research、B3 redesign audit、B1 wrapper compatibility 和后续 branch gate；
  当前 B5/B6/v3 均保持 blocked。
- 2026-06-19：实现完成并转入 VALIDATING。`aits etf weight-research
  post-b2-b3-research --write-source-alias` 先运行同一路径 `aits validate-data` gate，
  生成 20 个 canonical artifacts；最终 TRADING-556=`CONTINUE_B2_ONLY_PATH`，且
  `b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`。验证通过 focused pytest
  19 passed、scoped ruff、compileall、20 个 JSON parse 和 `git diff --check`。
