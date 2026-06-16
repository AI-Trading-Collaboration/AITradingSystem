# TRADING-376 Reader Brief Consistency Pack

最后更新：2026-06-16

状态：DONE

## 背景

当前主 Reader Brief、report quality gate、task register consistency 和 waiver inventory
已经分别披露读者摘要、下一步和 safety boundary，但不同 research、paper-shadow、
data governance 和 safety report 的 Reader Brief section 命名仍不完全一致。后续 governance
pack 需要一个只读 consistency pack，确认 newly generated reports 至少能用同一组读者字段解释
结论、阻断、warning、安全边界和下一步。

## 标准 Section

新增 Reader Brief section contract：

- Summary
- Key Result
- Blocking Issues
- Warnings
- Safety Boundary
- Next Action

主 Reader Brief 可以从现有 narrative/status/action/safety 字段映射到该 contract；report artifact
若已有 `reader_brief` mapping，则应直接包含这些语义。历史 artifact 不重写，缺失项由 consistency
pack 可见披露；新增模板应优先直接输出完整 section。

## 范围

- 新增只读 Reader Brief consistency pack report 和 validation artifact；
- 从 latest report index 读取 include-in-Reader-Brief report artifacts；
- 检查 missing section、missing next action、missing safety boundary 和 unclear decision state；
- 将主 Reader Brief 的 safety/next-action/decision checks 纳入 quality gate；
- Reader Brief、report registry、artifact catalog、README、system flow、operations runbook 和 tests 同步。

## 安全边界

- 只读读取 report index、Reader Brief JSON 和既有 report artifacts；
- 不重写历史 artifacts；
- 不运行上游 report/scoring/backtest/shadow/data pipeline；
- 不刷新数据、不补造 missing report；
- 不改变投资结论、score、weights、gates 或 production state；
- 不写 official target weights、broker/order 或 production mutation；
- 所有新增输出固定 `production_effect=none`。

## CLI

- `aits reports reader-brief-consistency --as-of YYYY-MM-DD`
- `aits reports validate-reader-brief-consistency --latest`

## 验收标准

- consistency pack 输出 JSON/Markdown，包含标准 section 覆盖、missing section、unclear decision
  和 safety/next-action issue；
- validation CLI 对自身 schema、production_effect、安全边界和主 Reader Brief core section fail closed；
- legacy report section gap 只能作为可见 warning，不得改写历史 artifact 或静默补造；
- Reader Brief quality 检查包含 safety boundary、next action 和 decision clarity；
- report registry、artifact catalog、README、system flow、operations runbook 和 task register 同步；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、compileall 和
  git diff check 通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-376，目标是统一新生成
  Reader Brief section contract，并用只读 consistency pack 暴露 legacy/report-template 缺口。
- 2026-06-16：实现完成并归档为 DONE；新增 `reader-brief-consistency` report/validation
  CLI、standard section extraction、Reader Brief consistency summary、Reader Brief quality safety
  / next-action / decision checks、report registry、artifact catalog、README、system flow、
  operations runbook 和 focused tests。真实 artifact `reader_brief_consistency_pack_2026-06-16`
  输出 `PASS_WITH_WARNINGS`（checked reports=366、full coverage=4、missing sections=1381、
  unclear decisions=192、blocking=0），validation 输出 `PASS_WITH_WARNINGS`（checks=7、
  warning checks=2、blocking=0）。Warnings 来自 legacy Reader Brief-facing artifacts 尚未统一
  section contract；本任务按安全边界不重写历史 artifact、不刷新数据、不运行上游、不写
  score/weights/gates/production state。
