# TRADING-401 to TRADING-407 Recovery Triage After TRADING-400

最后更新：2026-06-17

## 背景

TRADING-400 已生成 research governance recovery pack，并确认当前真实状态为
`RECOVERY_GOVERNANCE_BLOCKED`。Pack 源报告 16/16 可读，但仍有 9 个 remaining
blockers、10 个 remaining warnings，normal paper-shadow 不得恢复，extended shadow 和
live trading 仍 forbidden。

本批任务只做 triage、source depth audit、owner action mapping、validation 扩展和只读 rerun。
不得解决 blocker、不得新增 silent waiver、不得补造数据、不得生成 official target weights、
不得接 broker/order，也不得修改 production state。

## 范围

|任务|目标|输出|
|---|---|---|
|TRADING-401|把 TRADING-400 的 9 个 blockers 展开成 machine-readable blocker triage|`recovery_blocker_triage_YYYY-MM-DD.json/md` 和 validation|
|TRADING-402|把 report index 的 9 个 unwaived warnings 分类并给出处理建议|`report_index_warning_triage_YYYY-MM-DD.json/md` 和 validation|
|TRADING-403|清理 task register docs 的 line-ending normalization warning|`.gitattributes`/docs line endings 和 `git diff --check` clean|
|TRADING-404|确认 TRADING-400 `sources=16/16` 是否代表 source 可用且健康|`recovery_pack_source_depth_audit_YYYY-MM-DD.json/md` 和 validation|
|TRADING-405|把 blockers/warnings/source audit 汇总为 owner-action checklist|`recovery_owner_action_map_YYYY-MM-DD.json/md` 和 validation|
|TRADING-406|扩展 recovery governance pack focused validation coverage|扩展 `tests/test_research_governance_recovery_pack.py` fixtures/tests|
|TRADING-407|在 triage/audit/action map/validation 扩展后只读 rerun recovery governance pack|新的 recovery pack JSON/MD、validation JSON/MD 和 rerun context|

## 安全边界

- 只读读取既有 report index、TRADING-400 recovery pack 和相关 triage artifacts。
- 不运行上游 data/scoring/backtest/paper-shadow/extended-shadow 生产链路。
- 不刷新 market/macro cache，不补造 missing source artifacts。
- 不写 owner decision，不修改 candidate/paper-shadow/production state。
- 不生成 official target weights、order ticket 或 broker action。
- normal paper-shadow 只有在 blocker 真正消除且 owner review 明确允许时才可能恢复；本批不消除 blocker。
- extended shadow 和 live trading 继续 forbidden。

## 验收标准

- TRADING-401 输出 `recovery_blocker_count=9`，且 normal paper-shadow / extended shadow / live trading boundary 不被放宽。
- TRADING-402 输出 9 个 unwaived report-index warnings，所有 warning 均有分类和建议，不新增 waiver。
- TRADING-403 后 `git ls-files --eol docs/task_register*.md` 不再显示 mixed worktree line endings，`git diff --check` clean。
- TRADING-404 输出 `source_availability=16/16`、source health summary、unhealthy source list 和 next action。
- TRADING-405 输出 `next_owner_action`、`next_code_action`、`next_data_action`、paper-shadow resumption preconditions、extended shadow forbidden reasons 和 `live_trading_forbidden=true`。
- TRADING-406 focused pytest 数量从 TRADING-400 的 5 个实质增加，覆盖 blocked、healthy-with-warnings、missing source、unwaived warning、normal paper-shadow false、extended shadow forbidden、live trading forbidden。
- TRADING-407 只读 rerun recovery pack；不得 force healthy，当前 blockers 未解决时保持 blocked。
- Ruff、compileall、documentation contract、report quality gate、data quality gate、active task register consistency、report index 和 `git diff --check` 通过或按既有 accepted warning 可见披露。

## 阶段拆解

1. 登记任务并新增本 requirement 文档。
2. 新增 recovery triage reports 模块、CLI 和 focused tests。
3. 生成 2026-06-17 triage/audit/action map artifacts。
4. 扩展 TRADING-400 focused validation tests。
5. 清理 CRLF normalization warning。
6. 更新 report registry、artifact catalog、README、operations runbook 和 system flow。
7. 跑验证，归档任务到 completed，并按本地提交纪律 commit/push。

## 进度记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 TRADING-401～407 批量 requirement 文档和 task-register active rows；开始实现只读 triage/audit/action-map/rerun 工具。|
|2026-06-17|DONE|新增 recovery triage module、CLI、report registry/catalog/runbook/README/system-flow 文档、focused tests 和真实 2026-06-17 artifacts；blocker triage=9，report-index unwaived warning triage=9，source availability=16/16 且 unhealthy=13，owner action map open_actions=34；rerun recovery pack 仍 `RECOVERY_GOVERNANCE_BLOCKED`，normal paper-shadow=false，extended/live forbidden。|
