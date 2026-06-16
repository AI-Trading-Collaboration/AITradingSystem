# TRADING-363 Research Safety Boundary Audit

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-376 已经让 Reader Brief-facing reports 的 section contract 可见，但 research、
paper-shadow、readiness 和 future promotion board 输入仍需要一个 recurring safety boundary audit。
该 audit 应在任何 future promotion milestone 前运行，确认当前任务和 artifacts 仍是 research /
advisory / manual-review-only，未引入 official target weights、broker integration、order tickets、
production mutation 或 automatic live allocation。

## 范围

- 新增只读 safety boundary audit report 和 validation artifact；
- 扫描 active/completed task register 和 requirement/doc snippets 中的 forbidden capability language；
- 从 report index 读取 existing report artifacts，检查 safety metadata：
  `production_effect`、`broker_effect`、`order_effect`、`manual_review_only`、
  `official_target_weights`；
- 将 unsafe positive signals 设为 `SAFETY_BLOCKED`；
- 将 legacy missing metadata 作为可见 warning，不重写历史 artifacts；
- 在 Reader Brief、report registry、artifact catalog、README、system flow、operations runbook 和
  focused tests 中同步。

## 安全边界

- 只读读取 task registers、report index 和既有 artifacts；
- 不重写历史 artifacts；
- 不运行 upstream scoring/backtest/paper-shadow/data pipeline；
- 不刷新数据、不补造 missing artifact；
- 不改变投资结论、score、weights、gates、paper account 或 production state；
- 不写 official target weights、broker/order 或 production mutation；
- 所有新增输出固定 `production_effect=none`。

## CLI

- `aits reports research-safety-boundary-audit --as-of YYYY-MM-DD`
- `aits reports validate-research-safety-boundary --latest`

## 状态

- `SAFETY_PASS`
- `SAFETY_PASS_WITH_WARNINGS`
- `SAFETY_BLOCKED`

## 验收标准

- audit 输出 JSON/Markdown，包含 task boundary checks、artifact metadata checks、blocking/warning
  issues、Reader Brief summary 和 safety boundary；
- validation CLI 对 schema、production_effect、status enum、blocking issue、required safety dimensions
  fail closed；
- unsafe positive broker/order/official target/live allocation/production mutation signal 必须阻断；
- legacy missing safety metadata 只能作为可见 warning，不得静默补造或重写 historical artifact；
- Reader Brief 展示 latest safety audit status，并供 shadow continuation readiness / future promotion board
  inputs 引用；
- report registry、artifact catalog、README、system flow、operations runbook 和 task register 同步；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、compileall 和 git diff
  check 通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-363，目标是建立只读 research
  safety boundary audit，先用 fail-closed unsafe positive checks 和 visible legacy metadata warnings
  覆盖 current governance chain。
- 2026-06-16：DONE；新增 `research-safety-boundary-audit` 和
  `validate-research-safety-boundary` CLI、report registry、Reader Brief section、artifact catalog、
  README、system flow、operations runbook 和 focused tests。真实 2026-06-16 audit 输出
  `SAFETY_PASS_WITH_WARNINGS`，task checks=418、artifact checks=368、unsafe signals=0、
  missing metadata=1497、shadow continuation readiness input=`AVAILABLE_WITH_WARNINGS`、
  future promotion board input=`REQUIRES_WARNING_REVIEW`；validation 输出
  `SAFETY_PASS_WITH_WARNINGS`，checks=7、blocking failed=0、warnings=1。为验证 Reader Brief
  同日 section，本地 latest 可生成日期 2026-06-15 也生成 audit/validation 并重新生成 Reader Brief，
  section 显示 `SAFETY_PASS_WITH_WARNINGS`、unsafe signals=0、missing metadata=1476；精确
  2026-06-16 Reader Brief 仍受缺失
  `data/processed/decision_snapshots/decision_snapshot_2026-06-16.json` 限制。
