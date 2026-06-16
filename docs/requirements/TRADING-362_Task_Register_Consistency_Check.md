# TRADING-362 Task Register Consistency Check

最后更新：2026-06-16

## 背景

TRADING-361 已建立 artifact lineage graph。后续治理任务需要先确认
`docs/task_register.md`、`docs/task_register_completed.md`、report registry、
Reader Brief、需求文档链接和已归档任务之间没有互相矛盾的状态。

## 范围

本任务新增只读 task register consistency check：

- 解析当前任务登记和 completed register；
- 校验任务 ID、状态、active/completed 去重和 terminal status 归档纪律；
- 校验显式 `docs/...md` 链接存在；
- 校验 report registry 中存在本检查及 validation artifact 的 report entry；
- 校验 Reader Brief report entry 存在；
- 输出机器可读 JSON、Markdown 和 validation artifact。

## 安全边界

- 只读读取 Markdown、report registry 和既有 report artifacts；
- 不刷新 market / macro cache；
- 不运行 backtest、scoring、paper-shadow 或 broker 相关命令；
- 不生成 official target weights、order ticket 或 production mutation；
- 所有输出固定 `production_effect=none`。

## CLI

- `aits reports task-register-consistency run --as-of YYYY-MM-DD`
- `aits reports task-register-consistency report --latest`
- `aits reports task-register-consistency validate --latest`

## 验收标准

- consistency report 输出 `PASS`、`PASS_WITH_WARNINGS` 或 `FAIL`；
- validation CLI 对 blocking issue fail closed；
- report registry 可发现 consistency report 和 validation report；
- report payload 包含 Reader Brief 摘要；
- README、system flow、operations runbook、artifact catalog 和 task register 同步；
- focused tests、ruff、compileall 和 diff check 通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；实现只读一致性检查、CLI、registry、Reader Brief 摘要、文档和 focused tests。
- 2026-06-16：DONE；归档后真实 `task_register_consistency_2026-06-16` 为 PASS
  （active=77、completed=322、checks=13、failed=0），
  `task_register_consistency_validation_2026-06-16` 为 PASS（checks=5、failed=0）。
  验证通过 report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0、
  documentation contract PASS、Reader Brief latest OK、Reader Brief quality latest OK、
  focused pytest、Ruff、compileall 和 git diff check。精确日期 Reader Brief
  `2026-06-16` 因本地缺少 `decision_snapshot_2026-06-16.json` 未生成；
  这不是 consistency gate blocker，latest available Reader Brief 已验证。
