# TRADING-2298 Task Register and Daily Scheduler Validation Restore

最后更新：2026-06-29

## 背景

TRADING-2283 收尾验证留下两个非 generator 路径红项：

- task register consistency 因 TQQQ data quality watchlist 使用非标准 task id 而 fail closed。
- full pytest 中 daily scheduler ordering tests 仍期望 `score_daily` 后直接进入 Reader Brief/report chain，但当前配置和 system flow 已登记 `forward_evidence_dry_run_daily` 在 `score_daily` 后执行。

本任务只恢复治理和测试一致性，不改变 daily scheduler 实际执行顺序、数据质量门禁、评分、回测、报告 schema、production weights、active shadow weights 或 broker/order path。

## 范围

1. 将 TQQQ adjustment ratio jump watchlist 从非标准 task id 改为合法 `TRADING-2297_TQQQ_ADJUSTMENT_RATIO_JUMP_WATCHLIST`。
2. 确认 `forward_evidence_dry_run_daily` 是预期 daily dry-run archive step，且固定 `production_effect=none`、不触发 broker/order、paper-shadow 或 production mutation。
3. 更新 daily scheduler ordering tests，使期望顺序为 `score_daily` -> `forward_evidence_dry_run_daily` -> Reader Brief/report chain。
4. 同步 README 和 scheduler runbook 中的 daily order 描述。
5. 重跑 task/documentation consistency 和相关 scheduler focused pytest。

## 验收标准

- `aits reports task-register-consistency run/validate` 对当前项目返回 PASS。
- `tests/test_ops_daily.py` 和 `tests/test_scheduled_tasks.py` 中相关 ordering tests 通过。
- 文档说明与 `config/scheduled_tasks.yaml` / `build_daily_ops_plan` 顺序一致。
- 本任务不生成 cached-data dependent scoring、backtest 或 daily report 结论；不需要额外运行 `aits validate-data`。

## 进展记录

- 2026-06-29: 新增任务并进入 `IN_PROGRESS`；确认 TQQQ watchlist 是 data quality watchlist 任务，需重新编号而不是放宽 task id 校验。
- 2026-06-29: 实现完成并转入 `DONE`；TQQQ watchlist 改为合法 `TRADING-2297`，daily scheduler tests 改为接受 `forward_evidence_dry_run_daily` 位于 `score_daily` 与 dashboard/Reader Brief chain 之间，README 和 scheduler runbook 同步实际顺序。
- 2026-06-29: 验证通过 focused parallel pytest（52 passed）、task-register consistency run/validate（PASS/PASS）、docs freshness（468 docs, 0 issues）、documentation contract（PASS）、Ruff、`git diff --check` 和全量 parallel pytest（3504 passed, 643 warnings）。本任务未生成 cached-data dependent scoring/backtest/daily report 结论，因此未额外运行 `aits validate-data`。
