# TRADING-352A Report Index Warning Cleanup

最后更新：2026-06-15

## 1. 背景

TRADING-352 完成后，`aits reports index` 仍返回 `PASS_WITH_WARNINGS`。当前 warning
主要来自两类可见性问题：

- Friday daily artifacts 在 Monday morning / before U.S. close 被按 calendar days 误判 stale；
- 若干历史 ETF / dynamic shadow / baseline review artifact family 尚未生成真实 runtime
  artifact，但 registry 已登记用于 Reader Brief 和人工导航。

这些 warning 需要被明确分类，不能继续以笼统 `PASS_WITH_WARNINGS` 混在一起。

## 2. 目标

1. 保留 report index 只读行为，不运行或补造任何上游 artifact。
2. 用 registry policy 显式定义 freshness age basis。
3. 对 U.S. equity daily/weekly cadence 使用 trading-day freshness，而不是 calendar-day
   weekend aging。
4. 增加机器可读 waiver 文件，用于记录 legacy / optional visibility exception。
5. report index payload 输出 visibility audit，区分 unwaived warnings 和 explicit waivers。
6. 当所有 warning 都有显式 waiver 时，状态返回 `PASS_WITH_EXPLICIT_WAIVERS`。

## 3. 非目标

- 不改变 scoring、paper-shadow、candidate decision、target weights 或 strategy logic。
- 不刷新 market data。
- 不生成 missing artifact 的替身文件。
- 不把 required daily reading artifact 的 missing 状态静默 waive。

## 4. Artifact / Policy Contract

Source policy:

- `config/report_index_visibility_waivers.yaml`

Report index JSON 新增：

- `visibility_audit`
- `waiver_policy`
- `explicit_waivers`
- per-report `freshness_basis`
- per-report `visibility_issue`
- per-report `visibility_waiver`

## 5. Safety Boundary

所有改动固定：

- `production_effect=none`
- read-only report indexing only
- no upstream command execution
- no generated replacement artifact
- no broker/order/target-weight effect

## 6. 验收标准

- `aits reports index --as-of 2026-06-15` 返回 `PASS` 或
  `PASS_WITH_EXPLICIT_WAIVERS`，不再是笼统 `PASS_WITH_WARNINGS`。
- waiver 文件列出 reason、owner、impact、validation coverage 和 review/exit condition。
- focused report index tests 通过。
- documentation contract、Reader Brief quality、ruff、compileall、git diff check 通过。

## 7. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只清理 report index
  freshness/visibility audit，不刷新或补造任何报告 artifact。
- 2026-06-15：实现完成并转入 DONE；新增
  `config/report_index_visibility_waivers.yaml`、cadence-level freshness basis、
  visibility audit、per-report waiver metadata 和 CLI `--waiver-path`。`aits reports index
  --as-of 2026-06-15` 返回 `PASS_WITH_EXPLICIT_WAIVERS`，`missing_count=20`、
  `stale_count=10`、`explicit_waiver_count=30`、`unwaived_warning_count=0`。Focused
  report index / documentation contract tests 16 passed，documentation contract PASS，
  Reader Brief quality OK，ruff、compileall 和 git diff check 通过；安全边界保持
  read-only / no upstream rerun / no replacement artifact / no strategy or production change。
