# TRADING-391 Evidence Pack Rebuild After Recovery

最后更新：2026-06-17

## 背景

TRADING-385 至 TRADING-390 已恢复 signal inputs、readiness/health、normal paper-shadow
resumption gate、cost metrics 和 benchmark baseline metrics。TRADING-391 负责把这些恢复后
artifacts 聚合成一个 post-recovery evidence pack，供 owner / monthly review / promotion-board
准备使用。

## 范围

新增 read-only recovery evidence pack：

- `aits reports recovery-evidence-pack --as-of YYYY-MM-DD`
- `aits reports validate-recovery-evidence-pack --latest`

默认读取 latest 或指定同日可见 artifact：

- signal input completeness recovery / signal input completeness
- evidence staleness monitor
- shadow continuation readiness
- canonical paper-shadow health
- cost-sensitivity metrics materialization / cost-sensitivity review
- benchmark baseline metrics materialization / benchmark baseline control
- research safety boundary audit

## Statuses

- `RECOVERY_EVIDENCE_COMPLETE`
- `RECOVERY_EVIDENCE_PARTIAL`
- `RECOVERY_EVIDENCE_BLOCKED`

这些 status 只说明 recovery evidence pack 是否可用于人工复核，不批准 promotion、extended
shadow、official target weights、broker/order 或 live trading。

## Safety

Evidence pack 是 advisory-only aggregation。它不运行上游恢复步骤、不刷新数据、不补造缺失
artifact、不写 owner decision、不修改 candidate / paper-shadow / production state、不生成 official
target weights、不触发 broker/order。

## Progress

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档；实现范围收敛为只读聚合最新恢复 artifacts，缺 source 时 fail closed 到 `RECOVERY_EVIDENCE_PARTIAL` 或 `RECOVERY_EVIDENCE_BLOCKED`，不得把 evidence pack 自身解释为 promotion approval。|
|2026-06-17|DONE|新增 `aits reports recovery-evidence-pack`、`validate-recovery-evidence-pack`、JSON/Markdown pack、validation artifact、Reader Brief section、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `outputs/reports/recovery_evidence_pack_2026-06-17.json/md` 输出 `RECOVERY_EVIDENCE_COMPLETE`，source 10/10 可读、validation 10/10 可读、remaining recovery blockers=2、warnings=5；validation `outputs/reports/recovery_evidence_pack_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS` / failed=0。Remaining blockers 来自 cost sensitivity `NOT_MEANINGFUL_UNDER_COSTS` 和 benchmark baseline `CANDIDATE_UNDERPERFORMS_BASELINES`；该 pack 不是 promotion、extended shadow、official target、broker/order 或 live approval。|
