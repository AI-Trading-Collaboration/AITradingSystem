# Daily Weight Adjustment Scheduler Dry Run Runbook

最后更新：2026-05-18

## 目的

本 runbook 说明如何用本地 Windows Task Scheduler 每日运行
`scripts/run_daily_weight_adjustment_scheduler_dry_run.py`。该流程只验证每日调度
可以安全调用 daily weight adjustment summary pipeline：

- `mode=dry_run`
- `production_effect=none`
- `manual_review_only=true`
- 不修改 production profile
- 不写 approved profile
- 不触发 IBKR、PaperBroker、replay、controlled fill、lifecycle 或 comparison
- 不自动 commit 或 push GitHub repo

## 本地手动运行

先确认同日上游产物已经由 `TRADING-015`、`TRADING-016`、`TRADING-017` 和
`TRADING-018` 流程生成：

- `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.json`
- `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.md`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.md`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.md`

在项目根目录运行：

```powershell
Set-Location D:\Work\AITradingSystem
.\.venv\Scripts\python.exe scripts\run_daily_weight_adjustment_scheduler_dry_run.py --date YYYY-MM-DD
```

输出：

- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.md`
- `outputs/reports/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.md`

若缺少任一上游 artifact，dry-run 应输出 `LIMITED`，并在 `missing_artifacts` 中列出
缺失文件；这表示调度封装可运行，但上游证据不完整。

## 推荐运行时间

推荐在本地时间周二到周六 07:30-08:30 JST 运行，前提是当日上游 daily-run、
weight adjustment candidate、candidate evaluation、promotion gate 和 daily summary
已完成。这个时间通常落在美股常规交易日收盘后，适合用前一自然日作为 `as_of`。

美国假日、周末或上游任务失败时，dry-run 可能输出 `LIMITED`。这不是自动修复信号，
需要查看 `missing_artifacts` 和上游任务日志。

## Windows Task Scheduler 配置

1. 打开 Windows Task Scheduler。
2. 选择 `Create Task`，不要使用 Basic Task。
3. `General`：
   - Name：`AITradingSystem Daily Weight Adjustment Scheduler Dry Run`
   - 勾选 `Run whether user is logged on or not`
   - 勾选 `Run with highest privileges`
4. `Triggers`：
   - Weekly：Tuesday, Wednesday, Thursday, Friday, Saturday
   - Time：07:45 JST
5. `Actions`：
   - Program/script：`powershell.exe`
   - Start in：`D:\Work\AITradingSystem`
   - Add arguments：

```powershell
-NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; Set-Location 'D:\Work\AITradingSystem'; $AsOf=(Get-Date).AddDays(-1).ToString('yyyy-MM-dd'); New-Item -ItemType Directory -Force -Path 'outputs\logs' | Out-Null; .\.venv\Scripts\python.exe scripts\run_daily_weight_adjustment_scheduler_dry_run.py --date $AsOf *> ('outputs\logs\daily_weight_adjustment_scheduler_dry_run_' + $AsOf + '.log'); exit $LASTEXITCODE"
```

6. `Conditions`：
   - 可取消 `Start the task only if the computer is on AC power`，视本机运行纪律决定。
7. `Settings`：
   - 勾选 `Run task as soon as possible after a scheduled start is missed`
   - 勾选 `Stop the task if it runs longer than 30 minutes`
   - 若任务仍在运行，选择 `Do not start a new instance`

## 项目目录和 .venv

Task Scheduler 的 `Start in` 必须是项目根目录：

```text
D:\Work\AITradingSystem
```

不要依赖全局 `python` 或 PATH 上的 `aits.exe`。Task Scheduler action 应显式调用：

```text
D:\Work\AITradingSystem\.venv\Scripts\python.exe
```

若 `.venv` 不存在，先在交互式 PowerShell 中创建并安装依赖：

```powershell
Set-Location D:\Work\AITradingSystem
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev,data,dashboard,brokers]"
```

## 日志文件

建议把 stdout/stderr 写入：

```text
outputs/logs/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.log
```

日志只用于本地排错，不应提交到 GitHub。若日志包含本地绝对路径或环境细节，仍按本地
运行审计处理。

## 检查 output artifacts

运行后检查：

```powershell
$AsOf = "YYYY-MM-DD"
Get-Item "outputs\reports\daily_weight_adjustment_scheduler_dry_run_$AsOf.json"
Get-Item "outputs\reports\daily_weight_adjustment_scheduler_dry_run_$AsOf.md"
Get-Content "outputs\reports\daily_weight_adjustment_scheduler_dry_run_$AsOf.json" |
  ConvertFrom-Json |
  Select-Object as_of,dry_run_status,pipeline_status,production_effect,manual_review_only
```

必须满足：

- `production_effect=none`
- `manual_review_only=true`
- `safety_checks.production_profile_write_attempted=false`
- `safety_checks.approved_profile_write_attempted=false`
- `safety_checks.ibkr_order_path_called=false`
- `safety_checks.paperbroker_order_path_called=false`
- `safety_checks.replay_runner_called=false`
- `safety_checks.dashboard_write_only_summary=true`
- `safety_checks.forbidden_terms_absent=true`

## 确认 GitHub repo 不被自动 commit

该脚本不调用任何 Git 命令。调度任务运行前后可以手动检查：

```powershell
git status --short
git log -1 --oneline
```

预期：

- `git log -1` 不因 scheduler dry-run 自动变化。
- `git status --short` 至多显示本地生成的 output/log 文件。
- 不应出现由 Task Scheduler 自动创建的 commit、push、tag 或 branch。

## 失败或 LIMITED 处理

若 `dry_run_status=LIMITED`：

1. 查看 `missing_artifacts`。
2. 确认同日 015/016/017/018 上游产物是否存在。
3. 查看 Task Scheduler History 和 `outputs/logs/...log`。
4. 不要手工补写通过状态；先修复真实上游失败原因。

若任务退出码非 0：

1. 先查看日志文件中的 Python exception。
2. 确认 `Start in` 是否指向项目根目录。
3. 确认 `.venv\Scripts\python.exe` 是否存在。
4. 确认当前 Windows 用户有读取 repo 和写入 `outputs/reports`、`outputs/logs` 的权限。
5. 修复后手动运行同一命令，再恢复 Task Scheduler。

## 连续 3～7 天观察

dry-run 初始启用后，建议连续观察 3～7 个美股交易日：

- 每天记录 Task Scheduler exit code。
- 每天检查 JSON 的 `dry_run_status`、`pipeline_status`、`missing_artifacts` 和 `warnings`。
- 确认 safety checks 始终保持预期值。
- 确认没有 production profile 或 approved profile diff。
- 确认没有自动 commit/push。
- 若连续出现 `LIMITED`，按上游 artifact 缺失原因拆分新任务，不在 scheduler 层绕过。

## GitHub Actions Schedule

当前阶段暂不建议直接启用 GitHub Actions schedule。

原因：

- 本地 `outputs/`、私有配置和缓存不会完整存在于 GitHub runner。
- IBKR / PaperBroker 相关本地环境在 GitHub runner 不可用，虽然本 dry-run 不应触发它们。
- 当前目标是验证本机 Windows Task Scheduler 调度边界，而不是启用云端生产 cron。
- 云端 schedule 容易把缺失本地 artifact 误读为流程失败或误导后续自动化。

后续若要评估 GitHub Actions schedule，应先单独建立云端 artifact 同步、secret 策略、
runner 权限和只读输出归档方案，并作为新的任务登记。
