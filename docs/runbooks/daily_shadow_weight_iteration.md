# Daily Shadow Weight Iteration Runbook

最后更新：2026-05-19

## 目的

本 runbook 说明如何手动运行 TRADING-018B shadow-only 权重迭代。该流程只维护
`data/derived/weight_iterations/shadow/current_shadow_weights.json`，不修改 production
权重、不写 approved profile、不触发 IBKR / PaperBroker / replay runner，也不改变 daily
dashboard 主投资结论。

## 手动运行

先确认同日上游产物存在：

- `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.json`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.json`

运行：

```powershell
python scripts/run_daily_shadow_weight_iteration.py --date YYYY-MM-DD
```

如果 TRADING-018A dry-run 文件使用非默认命名：

```powershell
python scripts/run_daily_shadow_weight_iteration.py `
  --date YYYY-MM-DD `
  --scheduler-dry-run-json outputs/reports/<scheduler_dry_run>.json
```

## 输出检查

成功运行后检查：

- `data/derived/weight_iterations/shadow/candidates/shadow_weight_candidate_YYYY-MM-DD.json`
- `data/derived/weight_iterations/shadow/candidates/shadow_weight_candidate_YYYY-MM-DD.md`
- `data/derived/weight_iterations/shadow/logs/shadow_weight_iteration_run_YYYY-MM-DD.json`
- `data/derived/weight_iterations/shadow/logs/shadow_weight_iteration_run_YYYY-MM-DD.md`

如果 decision 为 `UPDATE`，还应有：

- `data/derived/weight_iterations/shadow/current_shadow_weights.json`
- `data/derived/weight_iterations/shadow/history/shadow_weights_YYYY-MM-DD.json`
- `data/derived/weight_iterations/shadow/history/shadow_weights_YYYY-MM-DD.md`

所有 JSON/Markdown 必须保持：

- `production_effect=none`
- `manual_review_only=true`
- `mode=shadow_only`

## Decision 处理

| Decision | 含义 | 处理 |
|---|---|---|
| `UPDATE` | 输入完整、safety PASS、confidence 达标且 delta 非零 | 检查 current/history/candidate/log；进入后续多日观察。 |
| `NO_UPDATE` | 输入完整且 safety PASS，但信号不足或 dry-run/force-no-update | 不更新 current；阅读 candidate Markdown 的原因。 |
| `INSUFFICIENT_DATA` | 缺少必要 artifact 或 artifact 类型无效 | 补齐对应 TRADING-015/016/017/018/018A artifact 后重跑。 |
| `SAFETY_BLOCKED` | scheduler dry-run safety 未通过 | 先修复 TRADING-018A safety blocker；不得强行更新 shadow state。 |
| `ERROR` | 脚本异常 | 检查 run log 和 traceback；不要手工改 production 权重。 |

## 确认 production 没有被修改

运行后确认：

```powershell
git diff -- config/weights/weight_profile_current.yaml
```

预期无变化。TRADING-018B 只允许首次初始化时读取 production profile 的 `base_weights`
并复制成独立 shadow state，不得引用或回写 production 配置。

## 回滚 shadow current state

如果需要回滚 shadow-only current state：

1. 找到最近一个可信 history snapshot：
   `data/derived/weight_iterations/shadow/history/shadow_weights_YYYY-MM-DD.json`
2. 备份当前 `current_shadow_weights.json`。
3. 将选定 history JSON 复制为 `current_shadow_weights.json`。
4. 记录原因到本 runbook 对应任务或新的 task-register item。

该回滚只影响 shadow state，不影响 production。

## Windows Task Scheduler 接入

当前不自动启用定时任务。接入时应放在 TRADING-018A dry-run 之后，并使用固定工作目录：

```powershell
cd D:\Work\AITradingSystem
python scripts/run_daily_shadow_weight_iteration.py --date YYYY-MM-DD
```

调度任务必须保留：

- 不传入任何 production write 参数；
- 不连接 broker；
- 不运行 replay runner；
- 不自动调用 TRADING-018C/018D；
- scheduler dry-run safety 非 PASS 时保持 `SAFETY_BLOCKED`。
