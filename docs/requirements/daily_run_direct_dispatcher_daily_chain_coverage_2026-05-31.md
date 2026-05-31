# OPS-061 daily-run direct dispatcher daily chain coverage

任务 ID：`OPS-061`

最后更新：2026-05-31

## 背景

2026-05-31 周期性运营运行按 `docs/operations/operations_runbook.md` 只触发
`aits ops daily-run`。本轮实际 `as_of=2026-05-29`，前 16 个步骤通过：
`download_data`、`validate_data`、PIT、SEC、valuation、`score_daily`、evidence
dashboard、SEC PIT shadow observe/monitor、score change attribution 和 market panel。

第 17 步 `market_data_freshness` 失败：

```text
daily-run direct dispatcher 不支持命令：data freshness --latest
```

失败诊断：

```text
outputs/runs/daily/20260530T223234Z/as_of_2026-05-29__daily_ops_run_2026-05-29_20260530T223234Z/reports/diagnostics/daily_ops_step_failure_2026-05-29_market_data_freshness_20260530T223418Z.md
```

## Intended Best Solution

`ai_trading_system.cli_direct` 应覆盖 `config/scheduled_tasks.yaml` 中
`daily_trading_day` 链路实际由 `aits ops daily-run` 调用的全部命令。新增或重命名
daily step 时，测试必须能发现 scheduled command 与 direct dispatcher coverage 不一致。

至少需要补齐：

- `aits data freshness --latest`
- `aits data recover-freshness --latest`
- `aits portfolio track-candidate --latest`
- `aits portfolio review-tracking --latest --show-window-progress`
- `aits reports portfolio-tracking-review --latest`

## Blocker

当前 `daily-run` 为规避 Windows/Typer 长流程稳定性问题，经 direct dispatcher 调用子命令。
新接入 daily chain 的 market freshness 和 portfolio tracking 命令已在
`config/scheduled_tasks.yaml` 登记，但 direct dispatcher 未同步支持，导致统一入口
fail-closed。

## Behavioral Impact

- 本轮 run 已通过 data quality gate、PIT、valuation 和 `score_daily`。
- `market_data_freshness` 及其后续 daily step 未在本轮执行。
- 不应把早前同一 `as_of` 的 freshness、tracking、Reader Brief 或 health artifact 当作
  本轮通过结论。
- 非 daily weekly/biweekly/monthly/governance 任务不应在本轮 daily blocker 后补跑。

## Risk

如果用手动子命令补跑，会绕开 runbook 要求的统一 scheduler trigger，并掩盖 daily chain
coverage 漂移。后续报告可能误以为 Reader Brief、tracking review 或 health/secret scan
来自同一 daily-run。

## Validation Requirements

- 单元测试覆盖每个 `daily_trading_day` command 都能被 `cli_direct` dispatch，或在配置中
  显式标记无需 direct dispatch。
- `aits ops daily-run` 对最新完成交易日可运行到 Reader Brief 和
  `validate-reader-brief`；若真实数据不足，输出 `LIMITED` / `INSUFFICIENT_DATA`，而不是
  unsupported command。
- 失败步骤仍写入脱敏 diagnostic artifact。
- 不修改 `config/parameters/production/current.yaml`，不写 production weights 或 active
  shadow weights，不触发 broker/trading action。

## Progress

- 2026-05-31: 新增任务并标记 `READY`。本轮仅记录 blocker，未修改业务代码，未手动补跑
  `data freshness` 或后续 daily step。
- 2026-05-31: 确认该阻断不符合预期。`config/scheduled_tasks.yaml` 已把
  `data freshness --latest` 和后续 tracking/report steps 登记到 daily chain；当前失败是
  direct dispatcher coverage 漂移，而不是 freshness 数据状态、供应商延迟或样本不足。
  任务进入 `IN_PROGRESS`，修复范围限定为 dispatcher coverage、测试和真实 daily-run 验证。
- 2026-05-31: 归档前发现 `OPS-060` 已被周期任务总入口 runbook 使用，按任务登记规则将本任务
  ID 修正为 `OPS-061`。
- 2026-05-31: 实现完成。`cli_direct` 已补齐 `data freshness`、`data recover-freshness`、
  `portfolio track-candidate`、`portfolio review-tracking` 和
  `reports portfolio-tracking-review`；新增测试从 `config/scheduled_tasks.yaml` 读取
  `daily_trading_day` 并逐条验证 direct dispatcher coverage。真实
  `aits ops daily-run` 对 `as_of=2026-05-29` 完成 28/28 步 PASS，Reader Brief 和
  `validate-reader-brief` 均 PASS。任务状态转为 `DONE`。
