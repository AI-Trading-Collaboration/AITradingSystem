# TRADING-398 Roadmap Dashboard Recovery Rerun

最后更新：2026-06-17

## 背景

TRADING-391 到 TRADING-397 已更新 recovery evidence、owner hold、monthly review、
promotion board、observation clock 和 extended shadow protocol。附件要求在这些治理报告更新后
重跑 research roadmap dashboard，汇总 active blockers、stale/missing artifacts、
paper-shadow status、data governance、monthly review、promotion board 和 extended shadow status。

## 范围

- 更新 `aits reports research-roadmap-dashboard --as-of YYYY-MM-DD` recovery status vocabulary。
- Dashboard status 支持：
  - `ROADMAP_HEALTHY`
  - `ROADMAP_WARNINGS`
  - `ROADMAP_BLOCKED`
- 继续接受 legacy `ROADMAP_WITH_WARNINGS` 以便旧 artifact validation 可读。
- Extended shadow `EXTENDED_SHADOW_BLOCKED` 和 `EXTENDED_SHADOW_NOT_READY` 必须保持 roadmap
  blocker；`EXTENDED_SHADOW_REVIEW_REQUIRED` 作为 visible warning / manual review state。
- 重跑 `2026-06-17` dashboard 和 validation。

## 安全边界

- Dashboard 是只读 summary，不修复 blocker。
- 不运行 upstream governance reports、data refresh、paper-shadow observation 或 scheduler。
- 不修改 task register、candidate state、paper-shadow state 或 production state。
- 不生成 official target weights、order ticket 或 broker action。

## 验收标准

- Real rerun 输出 `ROADMAP_BLOCKED`，且 extended shadow blocker、promotion/monthly/data/safety/
  stale/missing artifact 状态可见。
- Validation `PASS` 或 `PASS_WITH_WARNINGS` 且 failed checks=0。
- Reader Brief、README、artifact catalog、operations runbook、system flow、task register 和
  focused tests 更新。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief quality、
  data quality gate 和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备更新 dashboard recovery status vocabulary，并重跑 2026-06-17 roadmap dashboard。|
|2026-06-17|DONE|Dashboard recovery status 更新为 `ROADMAP_HEALTHY|ROADMAP_WARNINGS|ROADMAP_BLOCKED`，validator 保留 legacy `ROADMAP_WITH_WARNINGS` 兼容；extended shadow `EXTENDED_SHADOW_BLOCKED` / `EXTENDED_SHADOW_NOT_READY` 明确保持 roadmap blocker。最终 rerun `outputs/reports/research_roadmap_dashboard_2026-06-17.json/md` 输出 `ROADMAP_BLOCKED`、active_tasks=77、completed_tasks=348、open_blockers=5、stale_artifacts=19、missing_artifacts=20、active_candidates=1、paper_shadow=`HOLD_FOR_MORE_DATA`、data_governance=`PASS_WITH_WARNINGS`、safety=`SAFETY_PASS_WITH_WARNINGS`、lineage=`PASS`。Blockers: active task blockers、unwaived report-index warnings、extended shadow blocked、promotion board hold、monthly candidate major blockers。Validation `outputs/reports/research_roadmap_dashboard_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS`、failed=0、source_blockers=5、source_warnings=1。Focused tests 7 passed，ruff passed；该 rerun 只读汇总，不修复 blocker、不修改 task/candidate/paper-shadow/production state、不批准 extended/live/official target/broker/order。|
