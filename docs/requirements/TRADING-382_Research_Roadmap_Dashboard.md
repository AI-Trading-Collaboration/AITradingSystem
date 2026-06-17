# TRADING-382 Research Roadmap Dashboard

最后更新：2026-06-17

状态：DONE

## 背景

TRADING-362 到 TRADING-381 已建立多项 governance reports，但 owner 需要一个聚合入口查看当前 research roadmap、阻塞项、latest paper-shadow / safety / lineage 状态，以及下一批应优先处理的任务。该 dashboard 是只读治理视图，不自动创建、关闭或重排任务。

## 范围

- 新增 `aits reports research-roadmap-dashboard --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-research-roadmap-dashboard --latest`。
- 只读读取 `docs/task_register.md`、`docs/task_register_completed.md`、同日 report index 和 latest governance report artifacts。
- 聚合 active tasks、completed tasks、open blockers、stale artifacts、active candidates、latest paper-shadow status、data governance、safety、lineage 和 next recommended tasks。
- 输出 JSON / Markdown roadmap dashboard、validation artifact 和 Reader Brief section。

## 安全边界

- `production_effect=none`。
- 不修改 task register，不自动创建、关闭、归档或重排任务。
- 不修改 candidate / paper-shadow / production state。
- 不生成 official target weights、order ticket 或 broker action。
- stale / missing artifacts 必须作为 roadmap blocker 或 warning 披露，不得静默隐藏。

## 验收标准

- CLI 可生成 `outputs/reports/research_roadmap_dashboard_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/research_roadmap_dashboard_validation_YYYY-MM-DD.json/md`。
- Dashboard 包含 current active tasks、completed task count、open blockers、stale artifact summary、active candidate summary、latest paper-shadow status、latest data governance、latest safety、latest lineage 和 next recommended tasks。
- Validation 对缺 required sections、unsafe production effect、缺 Reader Brief section 或 state/broker/order mutation flag fail closed。
- Reader Brief 展示 dashboard status、validation status、active/completed counts、blocker count、stale artifact count、paper-shadow/data/safety/lineage states 和 top next task。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md`、`README.md` 和 task register 同步。
- Focused tests 覆盖 aggregation、validation fail closed、CLI output 和 Reader Brief summary。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。本阶段只聚合既有 task/report artifacts，不自动修复 missing/stale artifacts 或变更任务状态。
- 2026-06-16：实现完成并归档为 `DONE`。新增 report module、CLI、validation、Reader Brief section、registry/catalog/docs/runbook/system flow 和 focused tests。真实 artifact `outputs/reports/research_roadmap_dashboard_2026-06-16.json/md` 输出 `ROADMAP_BLOCKED`、active_tasks=77、completed_tasks=333、open_blockers=5、stale_artifacts=10、missing_artifacts=20、active_candidates=1、paper_shadow=`HOLD_FOR_MORE_DATA`、data_governance=`BLOCKED`、safety=`SAFETY_PASS_WITH_WARNINGS`、lineage=`PASS`；validation 输出 `PASS_WITH_WARNINGS`、failed=0。该结果只披露既有 blockers/stale artifacts，不自动创建、关闭或重排任务，不补造 report，不修改 candidate / paper-shadow / production state。
- 2026-06-16：验证通过。Task-register consistency PASS，documentation contract PASS，report index `PASS_WITH_EXPLICIT_WAIVERS` / reports=404 / unwaived=0，Reader Brief latest 使用 2026-06-15 decision snapshot + 2026-06-16 report index 展示 roadmap section且 quality OK，focused pytest 36 passed，Ruff PASS，compileall PASS。精确 2026-06-16 Reader Brief 仍受本机缺 `data/processed/decision_snapshots/decision_snapshot_2026-06-16.json` 限制；未补造 snapshot。
- 2026-06-17：TRADING-398 recovery rerun 将 dashboard warning status 更新为 `ROADMAP_WARNINGS`，validator 保留 legacy `ROADMAP_WITH_WARNINGS` 兼容；extended shadow `EXTENDED_SHADOW_BLOCKED` / `EXTENDED_SHADOW_NOT_READY` 明确保持 roadmap blocker，`EXTENDED_SHADOW_REVIEW_REQUIRED` 作为 visible warning / manual review state。
