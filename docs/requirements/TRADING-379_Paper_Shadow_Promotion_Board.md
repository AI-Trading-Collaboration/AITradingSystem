# TRADING-379 Paper Shadow Promotion Board

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-365 已生成 monthly research review pack，把 paper-shadow、data governance、safety、owner decisions、cost 和 benchmark blockers 放到同一手工复核入口。附件要求下一步建立 paper-shadow promotion board，用统一 evidence checklist 决定候选是否可以进入 extended paper-shadow、继续普通 shadow、返回研究、拒绝或等待更多数据。

## 范围

- 新增 `aits reports paper-shadow-promotion-board --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-paper-shadow-promotion-board --latest`。
- 只读读取同日 report index 和既有 source artifacts。
- 输入包括 weekly review、shadow continuation readiness、paper-shadow drift monitor、cost sensitivity、benchmark baseline control、research safety boundary audit、owner review / owner decision audit log 和 artifact lineage graph。
- 输出 JSON / Markdown promotion board report、validation artifact 和 Reader Brief section。

## 决策枚举

- `EXTEND_SHADOW`
- `CONTINUE_NORMAL_SHADOW`
- `RETURN_TO_RESEARCH`
- `REJECT`
- `HOLD_FOR_MORE_DATA`

## 安全边界

- `production_effect=none`。
- Board decision 只用于 paper-shadow / research governance。
- 不推进 live trading，不写 official target weights，不修改 candidate state、paper-shadow state、portfolio、production state、config 或 cache。
- 不生成 order ticket，不触发 broker，不自动 owner approval，不自动 promotion。
- 缺 evidence、stale data、insufficient cost / benchmark metrics、缺 owner decision 或 safety warning 必须进入 required checklist / blocking reasons，不得静默通过。

## 验收标准

- CLI 可生成 `outputs/reports/paper_shadow_promotion_board_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/paper_shadow_promotion_board_validation_YYYY-MM-DD.json/md`。
- Validation 对非法 decision、缺 evidence checklist、缺 required source、unsafe production effect、缺 safety boundary 或 live/official/broker mutation flag fail closed。
- Reader Brief 展示 board decision、validation status、checklist pass/block counts、blocking reasons、owner action status 和 detail link。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md`、`README.md` 和 task register 同步。
- Focused tests 覆盖 decision mapping、validation fail closed、CLI output 和 Reader Brief summary。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。当前真实 evidence 已显示 signal/readiness/cost/benchmark/owner decision 缺口；预期 board 应 fail-closed 到 `HOLD_FOR_MORE_DATA`，而不是提升候选状态。
- 2026-06-16：实现完成并归档 `DONE`。新增 `paper_shadow_promotion_board` report/validation、CLI、Reader Brief section、registry/catalog/runbook/system flow/README/tests。真实 board `outputs/reports/paper_shadow_promotion_board_2026-06-16.json/md` 输出 `HOLD_FOR_MORE_DATA`、checks=9、passed=2、blocked=4、warnings=3，主要 blocker 为 readiness stale data、cost inputs insufficient、benchmark baseline metrics insufficient 和 owner decision audit log empty；validation `PASS_WITH_WARNINGS`、failed=0。未补造 owner decision、metrics 或 evidence，保持 read-only / paper-shadow-only / no official target / no broker / no production mutation。
