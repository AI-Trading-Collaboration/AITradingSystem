# Daily Weight Adjustment Job Runbook

最后更新：2026-05-18

## 目的

本 runbook 说明如何手动生成每日只读权重调节 summary。该流程只串联既有
`TRADING-015`、`TRADING-016`、`TRADING-017` 产物，不自动修改 production 权重、
不写 approved profile、不触发 IBKR / PaperBroker / replay runner，也不改变 daily
dashboard 主投资结论。

## 手动运行

先确保同日上游产物已经存在：

- `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.json`
- `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.md`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.md`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.md`

运行：

```powershell
python scripts/run_daily_weight_adjustment.py --date YYYY-MM-DD
```

输出：

- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.md`

缺任一上游 JSON 或 Markdown 时，summary 只标记 `LIMITED` / `INSUFFICIENT_DATA`，
并在 `missing_artifacts` 中列出缺失文件。脚本不会补造 improvement，也不会把缺失输入
下的 promotion gate 展示为 `READY_FOR_MANUAL_REVIEW`。

## 未来调度接入

当前不自动启用定时任务。未来可以在确认 owner 认可后接入以下任一种调度方式：

- GitHub Actions：在 daily ops 产物生成后增加一个只读 job step，运行上述 Python 命令。
- Windows Task Scheduler：在本机 daily-run 之后增加独立任务，工作目录指向 repo 根目录。
- cron：在 Linux/VM daily-run 之后增加同一命令。

无论采用哪种方式，调度步骤必须保持：

- `production_effect=none`
- `mode=observe_only`
- 不修改 production profile
- 不写 approved profile
- 不调用 IBKR / PaperBroker / replay runner
- 不调用 controlled fill / lifecycle / comparison 脚本
- 不改变 dashboard 主投资结论
