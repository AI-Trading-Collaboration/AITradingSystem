# Daily Operator Brief Scheduler Dry Run

## 1. 目的

TRADING-026 用于在启用任何真实每日调度前，只读检查 Daily Trading System Operator Brief 的定时生成条件是否安全、输入是否齐全、缺失依赖是否能按预期降级。

本任务不创建 scheduler，不运行 operator brief，不运行上游 pipeline。

## 2. 如何运行

```bash
python scripts/run_daily_operator_brief_scheduler_dry_run.py --date YYYY-MM-DD
```

常用参数：

```bash
python scripts/run_daily_operator_brief_scheduler_dry_run.py \
  --date 2026-05-23 \
  --data-root data \
  --expected-run-hour 9 \
  --expected-run-minute 0 \
  --timezone Asia/Tokyo \
  --lookback-days 3
```

`--strict` 会把 optional 023/024 缺失或 stale 也视为 not ready。`--fail-on-missing-required` 会在写出 artifact 后，对 required input missing 返回非零退出。

## 3. dry_run_decision

|Decision|含义|
|---|---|
|`READY`|Required TRADING-021 digest 存在且 fresh，023/024 optional inputs 可用，没有 safety blocker。|
|`READY_WITH_WARNINGS`|Required TRADING-021 digest 可用，但 023/024 缺失、invalid 或 stale；operator brief 若执行预期降级 `WATCH`。|
|`NOT_READY`|TRADING-021 digest 缺失、invalid、stale，或 strict mode dependency check 失败。|
|`SAFETY_BLOCKED`|任一输入 artifact 安全字段异常，例如 broker/replay/trading execution 为 true。|
|`ERROR`|dry-run 自身运行异常。|

## 4. dry_run_status

|Decision|Status|
|---|---|
|`READY`|`OK`|
|`READY_WITH_WARNINGS`|`WATCH`|
|`NOT_READY`|`ACTION_REQUIRED`|
|`SAFETY_BLOCKED`|`SAFETY_BLOCKED`|
|`ERROR`|`ERROR`|

## 5. Dependency Check

Dependency check 回答“如果未来真实 scheduler 触发 operator brief，输入条件是否具备”。

- `missing_required_inputs` 非空时，不应启用真实 scheduler。
- `missing_optional_inputs` 只覆盖 TRADING-023 pipeline health summary 和 TRADING-024 data freshness summary。
- `stale_inputs` 说明 artifact 日期超过 freshness policy；TRADING-021 stale 会 `NOT_READY`，023/024 stale 会 `READY_WITH_WARNINGS`。

## 6. Safety Check

Safety check 回答“已发现的输入 artifact 是否违反只读和无执行边界”。

重点字段：

- `scheduler_created=false`
- `operator_brief_executed_by_scheduler_dry_run=false`
- `pipelines_executed_by_scheduler_dry_run=false`
- `data_downloaded_by_scheduler_dry_run=false`
- `apply_executed_by_scheduler_dry_run=false`
- `rollback_executed_by_scheduler_dry_run=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

任何输入 artifact 把 apply、rollback、broker、replay 或 trading 执行字段标为 true，都必须先调查，不得通过编辑 TRADING-026 artifact 消除。

## 7. READY_WITH_WARNINGS

`READY_WITH_WARNINGS` 表示 dry-run 本身可执行，且 required digest 可用，但 operator brief 若真正运行，预期会因为 023/024 缺失或 stale 降级 `WATCH`。

处理方式：

- 先查看 `missing_optional_inputs` 和 `stale_inputs`。
- 需要完整系统健康与数据新鲜度视图时，按各自 runbook 手动生成 023/024。
- 不要从 TRADING-026 自动运行 023/024。

## 8. NOT_READY

`NOT_READY` 通常说明 TRADING-021 digest 缺失、invalid 或 stale。

处理方式：

- 按 TRADING-021 runbook 重新生成或定位 digest。
- 检查 digest 日期是否符合当前 dry-run date。
- strict mode 下同时检查 optional inputs。

## 9. SAFETY_BLOCKED

`SAFETY_BLOCKED` 表示输入 artifact 的安全字段与只读边界冲突。

处理方式：

- 查看 `safety_check.blocking_reasons`。
- 回到对应 artifact 的生成任务调查来源。
- 在原因未清楚前，不启用 scheduler，也不依赖 operator brief scheduled generation。

## 10. 为什么不创建 scheduler

TRADING-026 的验收目标是验证未来调度前置条件，不是安装任务。真实 scheduler 会引入运行权限、时间边界、日志保留、失败重试、工作目录和解释器路径等额外风险，应由单独任务和人工 checklist 处理。

## 11. 为什么不执行 operator brief

本任务需要验证“如果现在执行是否具备条件”，而不是生成新 brief。执行 TRADING-022 会混淆 dry-run 输入检查和实际报告生成，也会让 dashboard 难以证明自己只读。

## 12. 未来手动安装注意事项

Windows Task Scheduler、cron 或 GitHub Actions 后续只能手动接入，且应先满足：

- TRADING-026 连续稳定输出 `READY` 或明确可接受的 `READY_WITH_WARNINGS`。
- 真实调度命令只运行 TRADING-026 或被人工批准的目标脚本。
- 工作目录、Python 解释器、环境变量和日志路径固定。
- 首次启用前人工确认不会触发 apply、rollback、broker、replay、trading 或 data download。
- Dashboard 继续只读读取 artifact，不触发任何 scheduler 或 script。
