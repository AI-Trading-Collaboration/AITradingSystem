# TRADING-375 Registry Warning Waiver Expiry

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-352A 已把 report index missing/stale visibility warning 区分为
unwaived warning 与 explicit waiver。当前 waiver metadata 可解释 reason、owner、
impact、validation coverage 和 exit condition，但缺少强制过期、review status、
linked task 和可审计 inventory report。后续 governance pack 需要确认
`PASS_WITH_EXPLICIT_WAIVERS` 只代表当前、有人负责且未过期的 waiver。

## 范围

本任务强化 `config/report_index_visibility_waivers.yaml` 和 `aits reports index`：

- 为每个 waiver 增加 `created_at`、`expires_at`、`review_status` 和 `linked_task_id`；
- 校验 waiver 不得默认永久有效；
- 过期 waiver 必须 fail closed；
- report index 只允许未过期且 metadata 完整的 waiver 解除 warning；
- 输出当前 waiver inventory report 和 validation artifact；
- Reader Brief、artifact catalog、report registry、operations runbook、system flow 和 tests 同步。

## 安全边界

- 只读读取 report registry、report index waiver policy 和既有 artifacts；
- 不补造 missing/stale reports；
- 不刷新数据、不运行上游；
- 不把 waived artifact 当作 fresh evidence；
- 不写 official target weights、broker/order 或 production state；
- 所有新增输出固定 `production_effect=none`。

## CLI

- `aits reports waiver-inventory --as-of YYYY-MM-DD`
- `aits reports validate-waiver-inventory --latest`

## 验收标准

- `aits reports index --as-of YYYY-MM-DD` 在 current waiver 下仍可返回
  `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0；
- expired waiver 导致 report index warning 或 waiver inventory validation fail closed；
- waiver-free state 报告 PASS；
- waiver inventory JSON/Markdown、Reader Brief summary 和 registry entries 可发现；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、
  compileall 和 git diff check 通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-375，目标是让
  report index waivers 显式归属、可过期并可被 inventory report 审计。
- 2026-06-16：实现并归档为 DONE。新增 expiry-aware waiver schema validation、
  `aits reports waiver-inventory`、`aits reports validate-waiver-inventory`、Reader Brief
  摘要、registry/catalog/system flow/runbook/README 集成和 focused tests。真实
  inventory artifact 输出 `inventory_status=PASS`、expanded waivers=31、active=31、
  expired=0、expiring_soon=0；validation `PASS`、checks=6、failed=0；report index
  `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0 / expired_waivers=0。保持只读、
  不自动续期 waiver、不补造 report、不刷新数据、不运行上游、不写 official target weights、
  不触发 broker/order 或 production mutation。
