# Reader Brief Current Decision Summary

最后更新：2026-06-07

任务 ID：OPS-016

状态：DONE

## 背景

2026-06-04 修复 FRED tail refresh 后，`aits ops daily-run --as-of 2026-06-03`
全流程 31/31 PASS，但 `outputs/reports/reader_brief_2026-06-03.html` 的
Core Decision 仍显示 `missing`。排查发现 Reader Brief 于
2026-06-04T10:26:37+09:00 生成，读取的是修复前失败 run
`daily_ops_run:2026-06-03:20260603T223626Z` 的
`daily_decision_summary_2026-06-03.json`；当前成功 run 的
`daily_decision_summary_2026-06-03.json` 在 2026-06-04T10:27:17+09:00 才被
daily-run finalization 刷新。

## 需求

1. `aits ops daily-run` 最终发布的 Reader Brief 必须读取同一 run 的
   `daily_decision_summary` 和 `daily_task_dashboard`，不得保留同一 `as_of` 的旧失败
   run Core Decision。
2. 该修复只允许重建只读 Reader Brief / Reader Brief quality artifacts；不得重跑
   scoring、PIT、SEC、valuation、backtest、shadow、weight 或 docs 上游。
3. 输出必须继续固定 `production_effect=none`，不得写 production weights、active
   shadow weights、broker state 或交易指令。
4. canonical run bundle 和 legacy `outputs/reports` mirror 必须保持一致。

## 验收

- 单测覆盖 daily-run finalization 后 Reader Brief 的 `run_context.run_id` 来自当前
  run，而不是 pre-existing stale summary。
- `aits reports reader-brief --as-of 2026-06-03` 或完整 daily-run 复测后，Core
  Decision 不再因旧失败 run 显示 `missing` / `Data Gate=MISSING`。
- `aits reports validate-reader-brief --as-of 2026-06-03` 仍为 OK 或显式披露受限上下文。

## 进展

- 2026-06-04：新增并进入实现。根因为 daily-run finalization 在 Reader Brief
  step 后才写出 current-run `daily_decision_summary`，导致 Reader Brief 读取旧同日
  summary。
- 2026-06-04：实现完成并进入验证。`aits ops daily-run` finalization 在
  `reader_brief` step 已 PASS 时，用 current-run canonical `daily_decision_summary`
  和 `daily_task_dashboard` 重建 Reader Brief / Reader Brief quality，并 mirror 到
  legacy `outputs/reports`。验证通过目标 pytest、ruff、diff check、
  `aits reports reader-brief --as-of 2026-06-03`、`aits reports validate-reader-brief
  --as-of 2026-06-03` 和 `aits docs report-contract --latest`；HTML Core Decision 已
  显示 current run `daily_ops_run:2026-06-03:20260604T011839Z`、Data Gate PASS、
  `wait_manual_review` 和 `40%-40%`。
- 2026-06-04：完整重跑 `aits ops daily-run --as-of 2026-06-03`，run id 为
  `daily_ops_run:2026-06-03:20260604T020951Z`，31/31 steps PASS，并输出
  `Reader Brief final`。最终 legacy Reader Brief Core Decision 读取同一 run：
  `wait_manual_review`、`40%-40%`、Data Gate PASS；`validate-reader-brief`
  failed=0，状态为 `LIMITED_READER_CONTEXT`，限制来自缺失/陈旧阅读上下文，不影响
  今日评分链路。
- 2026-06-07：收口为 DONE。原因：最新真实 `aits ops daily-run --as-of
  2026-06-05 --run-id codex_20260605_20260607103901` 31/31 steps PASS；最终
  Reader Brief 的 `run_context.run_id`、`daily_decision_summary` 和
  `daily_task_dashboard` 均绑定同一 run，Core Decision 显示 `maintain`、
  `40%-40%`、Data Gate PASS、`production_effect=none`，不再保留旧失败 run 的
  `missing` 结论。canonical run bundle 与 legacy `outputs/reports` mirror 的
  Reader Brief、decision summary 和 task dashboard checksum 一致，Reader Brief
  quality failed=0；`LIMITED_READER_CONTEXT` 仅来自缺少 trace bundle 的可追溯性限制，
  不影响本任务 current-run summary/dashboard 验收。
