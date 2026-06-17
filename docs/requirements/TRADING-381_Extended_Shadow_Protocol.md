# TRADING-381 Extended Shadow Protocol

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-379 建立了 paper-shadow promotion board，TRADING-380 建立了 rejection postmortem template。附件要求继续定义 extended shadow protocol，用比普通 paper-shadow 更严格的 evidence checklist 判断候选是否可以进入 extended paper-shadow observation。该 protocol 仍不是 live trading 或 official allocation approval。

## 范围

- 新增 `aits reports extended-shadow-protocol --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-extended-shadow-protocol --latest`。
- 只读读取同日 report index 和既有 source artifacts。
- 输入包括 promotion board、weekly review、evidence staleness/readiness、cost sensitivity、benchmark baseline、research safety boundary audit、owner decision audit log 和 artifact lineage graph。
- 输出 JSON / Markdown extended shadow protocol report、validation artifact 和 Reader Brief section。

## 严格 Eligibility 要求

- 无 blocking stale data。
- 无 unresolved safety warnings。
- weekly review coverage stable。
- cost sensitivity acceptable。
- benchmark comparison available。
- owner review / owner decision complete。
- lineage graph available。
- 满足 minimum observation period。

## Minimum Observation Period

最低观察期沿用 TRADING-350 paper-shadow protocol pilot baseline：`20` trading days。该阈值只用于 extended shadow eligibility，不是 live trading 或 official allocation threshold；后续若要调整，必须通过 policy / task register 记录校准依据。

## 安全边界

- `production_effect=none`。
- Protocol 只输出 extended paper-shadow eligibility，不修改 candidate / paper-shadow / production state。
- 不生成 official target weights，不生成 order ticket，不触发 broker。
- 不补造 observation days、owner decision、cost metrics、benchmark metrics 或 safety clearance。

## 验收标准

- CLI 可生成 `outputs/reports/extended_shadow_protocol_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/extended_shadow_protocol_validation_YYYY-MM-DD.json/md`。
- Report 明确输出 eligibility status、minimum observation period、observed days、required checklist、blocking reasons、safety boundary 和 next action。
- Validation 对非法 status、缺 required source、unsafe production effect、缺 Reader Brief section 或 state/broker/order mutation flag fail closed。
- Reader Brief 展示 eligibility status、validation status、candidate id、observed/minimum days、blocking counts、safety/readiness/owner/lineage status 和 detail link。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md`、`README.md` 和 task register 同步。
- Focused tests 覆盖 eligibility pass/block、validation fail closed、CLI output 和 Reader Brief summary。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。当前真实 evidence 已显示 stale/readiness/cost/benchmark/owner decision 缺口；预期 extended protocol 应 fail-closed，而不是提升候选状态。
- 2026-06-16：实现完成并归档 `DONE`。新增 extended shadow protocol report/validation、CLI、Reader Brief section、registry/catalog/runbook/system flow/README/tests。真实 protocol `outputs/reports/extended_shadow_protocol_2026-06-16.json/md` 输出 `EXTENDED_SHADOW_BLOCKED`、observed/minimum days=`0/20`、blocked=7、warnings=1；validation `PASS_WITH_WARNINGS`、failed=0。当前缺 promotion board clearance、fresh readiness、safety warning clearance、cost/benchmark evidence、owner decision 和可证明 observation days，未补造任何证据，保持 read-only / paper-shadow-only / no official target / no broker / no production mutation。
- 2026-06-17：TRADING-397 recovery rerun 扩展 protocol status taxonomy 为 `EXTENDED_SHADOW_BLOCKED|EXTENDED_SHADOW_NOT_READY|EXTENDED_SHADOW_REVIEW_REQUIRED|EXTENDED_SHADOW_ELIGIBLE`。Hard source blocker 仍为 blocked；observation-only gap 为 not ready；warning-only evidence 为 review required；eligible 仍不是 live trading、official target、broker/order 或 production approval。
