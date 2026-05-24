# Daily Operator Brief Scheduler Manual Installation Guide

## 1. Purpose

本 runbook 说明如何人工安装 Daily Trading System Operator Brief 的每日 scheduler，并给出 Windows Task Scheduler、cron 和 GitHub Actions 的手动配置 checklist。

TRADING-027 是 documentation-only。它不自动创建、修改或启用任何 scheduler，也不运行 operator brief、dry-run、数据刷新、评分、回测、broker、replay 或 trading execution。

本 runbook 的安全边界固定为：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "runbook_only": true,
  "scheduler_created": false,
  "operator_brief_executed_by_runbook": false,
  "pipelines_executed_by_runbook": false,
  "data_downloaded_by_runbook": false,
  "apply_executed_by_runbook": false,
  "rollback_executed_by_runbook": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

## 2. Recommended Daily Execution Order

推荐的每日人工或未来 scheduler 顺序是：

1. Generate parameter governance daily digest: TRADING-021.
2. Generate pipeline health summary: TRADING-023.
3. Generate data freshness summary: TRADING-024.
4. Generate daily trading system operator brief: TRADING-022.
5. Run scheduler dry run: TRADING-026, optional before enabling scheduler.

TRADING-027 不执行这些命令。它只说明真实 scheduler 启用前应如何人工检查和安装。

## 3. Pre-installation Checklist

启用任何真实 scheduler 前，人工必须确认：

- [ ] Python environment is available.
- [ ] Repository path is stable.
- [ ] Working directory is correct.
- [ ] Required environment variables are configured.
- [ ] TRADING-026 dry-run has returned `READY` for at least 3 consecutive days, or every `READY_WITH_WARNINGS` result has been reviewed and accepted.
- [ ] No `SAFETY_BLOCKED` status exists.
- [ ] No unexpected `broker_execution`, `replay_execution`, or `trading_execution` flags exist.
- [ ] No pending apply / rollback action is triggered by the scheduler.
- [ ] Scheduler command does not include apply / rollback / broker / replay / trading scripts.

默认稳定观察窗口为 `N = 3` 个连续日。若 operator brief 用于重要人工决策，可延长到 5 到 7 个交易日。

## 4. Safety Checklist Before Enabling Scheduler

启用前逐项检查：

- [ ] Scheduler command only runs read-only reporting scripts.
- [ ] Scheduler command does not run TRADING-018E2 apply.
- [ ] Scheduler command does not run TRADING-018E3 rollback.
- [ ] Scheduler command does not run broker integration.
- [ ] Scheduler command does not run replay runner.
- [ ] Scheduler command does not run trading execution.
- [ ] Scheduler output directory is under `data/derived/`.
- [ ] Scheduler logs are stored separately.
- [ ] Scheduler dry-run result is reviewed before enabling.
- [ ] Scheduler command does not run market, backtest, scoring, or data download pipelines unless a future reviewed task explicitly changes that boundary.
- [ ] Scheduler command does not modify production profile or shadow weights.

Do not schedule apply or rollback scripts.

## 5. Windows Task Scheduler Manual Setup

This is an example only. TRADING-027 does not create a Windows Task Scheduler task.

Manual setup steps:

1. Open Task Scheduler.
2. Select `Create Basic Task` or `Create Task`.
3. Name: `Daily Trading System Operator Brief`.
4. Trigger: `Daily`.
5. Time: `09:00` local time, or another reviewed operator-selected time.
6. Action: `Start a program`.
7. Program/script: full path to `python.exe`.
8. Add arguments: `scripts/run_daily_trading_system_operator_brief.py`.
9. Start in: repository root, for example `C:\path\to\AITradingSystem`.
10. Configure log redirection through a manually reviewed `.bat` or PowerShell wrapper if persistent logs are required.

Windows `%DATE%` formatting is locale-dependent and unstable. Prefer letting the project script default to today, or use PowerShell to produce ISO dates.

Manual `.bat` wrapper example. Do not write or install this automatically:

```bat
@echo off
cd /d C:\path\to\AITradingSystem
python scripts\run_daily_trading_system_operator_brief.py
```

Manual PowerShell wrapper example with explicit ISO date. Do not write or install this automatically:

```powershell
$date = Get-Date -Format "yyyy-MM-dd"
Set-Location "C:\path\to\AITradingSystem"
python scripts/run_daily_trading_system_operator_brief.py --date $date
```

If a wrapper is used, store logs outside generated report directories or in a reviewed `logs/` path:

```powershell
$date = Get-Date -Format "yyyy-MM-dd"
Set-Location "C:\path\to\AITradingSystem"
python scripts/run_daily_trading_system_operator_brief.py --date $date *> "logs/operator_brief_$date.log"
```

Before saving the task, inspect the command line and confirm it does not include `apply`, `rollback`, broker, replay runner, trading execution, market download, backtest, or scoring commands.

## 6. cron Manual Setup

This is an example only. Do not add it automatically.

Linux/macOS example:

```cron
0 9 * * * cd /path/to/AITradingSystem && /usr/bin/python3 scripts/run_daily_trading_system_operator_brief.py >> logs/operator_brief_cron.log 2>&1
```

Operational checks:

- Use absolute paths for repository, Python, and logs.
- Confirm the Python environment includes the project dependencies.
- Confirm the repository path is stable and not a temporary checkout.
- Confirm the cron user has read/write access to `data/derived/` and the selected log path.
- Confirm the cron line does not run apply / rollback / broker / replay / trading scripts.
- Keep cron logs separate from generated operator brief artifacts.

## 7. GitHub Actions Optional Setup

GitHub Actions is optional and requires a separate review for artifact storage, secrets, retention, and whether the cloud runner has the right cached inputs. Do not commit a workflow in TRADING-027.

Documentation-only example:

```yaml
name: Daily Operator Brief

on:
  schedule:
    - cron: "0 0 * * *"
  workflow_dispatch:

jobs:
  operator-brief:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: python scripts/run_daily_trading_system_operator_brief.py
```

Safety notes:

- This example is documentation only.
- Do not commit a workflow in TRADING-027.
- Do not include secrets unless required and reviewed.
- Do not run broker / replay / trading commands.
- Do not run apply or rollback commands.
- Do not assume local `data/derived/` artifacts exist on a cloud runner.
- Define artifact upload, retention, and access control before enabling any cloud schedule.

## 8. Recommended Scheduling Modes

Mode A: Manual only.

- Recommended initially.
- User runs dry-run and operator brief manually.
- Best default until dry-run stability and artifact dependencies are observed.

Mode B: Local scheduler.

- Windows Task Scheduler or cron.
- Suitable after TRADING-026 is stable for the reviewed observation window.
- Keeps local caches, local environment, and operator logs under local control.

Mode C: GitHub Actions.

- Useful for cloud-based summary generation.
- Requires careful artifact storage design.
- Requires explicit review of secrets, retained artifacts, and missing local cache behavior.

Recommended route:

```text
Manual only -> dry-run stable -> local scheduler -> optional GitHub Actions
```

## 9. Handling Dry-run Decisions

TRADING-026 dry-run decisions should be handled as follows:

|Decision|Meaning|Action|
|---|---|---|
|`READY`|Scheduler inputs are available and safety checks passed.|Manual operator brief generation may be scheduled after checklist review.|
|`READY_WITH_WARNINGS`|Optional inputs may be missing or stale.|Scheduler can run, but output may degrade to `WATCH`; review warnings before enabling.|
|`NOT_READY`|Required input is missing, invalid, or stale.|Do not enable scheduler; fix required input availability first.|
|`SAFETY_BLOCKED`|Safety invariant failed.|Do not enable scheduler; inspect input artifacts immediately.|
|`ERROR`|Dry-run failed.|Do not enable scheduler; check logs and rerun dry-run manually after fixing the cause.|

For `READY_WITH_WARNINGS`, review whether missing TRADING-023 or TRADING-024 summaries are acceptable for the scheduled brief. If full pipeline health and data freshness are required, generate those summaries through their own reviewed manual runbooks before relying on the scheduled brief.

For `SAFETY_BLOCKED`, never clear the status by editing the dry-run artifact. Investigate the source artifact and why an execution-related flag or production effect violated the read-only contract.

## 10. Manual Validation After Scheduler Runs

After every scheduler run, manually inspect the generated operator brief and related logs:

- [ ] Operator brief JSON generated.
- [ ] Operator brief Markdown generated.
- [ ] `brief_status` is not `SAFETY_BLOCKED`.
- [ ] `broker_execution = false`.
- [ ] `replay_execution = false`.
- [ ] `trading_execution = false`.
- [ ] `apply_executed_by_operator_brief = false`.
- [ ] `rollback_executed_by_operator_brief = false`.
- [ ] Dashboard reads artifact without triggering pipelines.
- [ ] Input artifact paths and checksums are present where expected.
- [ ] Data quality status or linked quality report is visible in downstream operator-facing output when cached data is part of the conclusion.
- [ ] Markdown warnings are consistent with JSON status fields.
- [ ] Logs do not contain secrets, API keys, broker account identifiers, or unexpected command output.

## 11. Failure Handling

Use this table for first-response troubleshooting:

|Failure|Check|Do not do|
|---|---|---|
|Python not found|Confirm absolute `python.exe` or `/usr/bin/python3` path and environment activation.|Do not switch to an unreviewed global Python that lacks project dependencies.|
|Wrong working directory|Confirm `Start in`, `Set-Location`, or `cd` points to repository root.|Do not run scripts from a temporary directory.|
|Missing repository path|Confirm scheduler user can access the repository path.|Do not recreate a scheduler against a different checkout without review.|
|Missing TRADING-021 digest|Run TRADING-026 dry-run manually and inspect dependency check.|Do not run apply / rollback or edit digest artifacts to pass readiness.|
|Missing TRADING-023 / 024 summaries|Review `READY_WITH_WARNINGS` and decide whether degraded `WATCH` output is acceptable.|Do not make TRADING-027 run upstream summaries automatically.|
|Operator brief output missing|Check scheduler logs, script exit code, working directory, and file permissions.|Do not assume dashboard absence means no issue.|
|`SAFETY_BLOCKED`|Inspect input artifact safety fields and source task run logs.|Do not enable or keep running scheduler until the source cause is understood.|
|Permission denied|Check scheduler user permissions for repository, `data/derived/`, and log path.|Do not grant broad broker or production profile permissions to fix a report write failure.|
|Stale artifacts|Run TRADING-026 manually and inspect stale input list.|Do not silently widen freshness windows without policy review.|
|Git working tree pollution|Check tracked diff and untracked generated outputs separately.|Do not commit generated runtime artifacts or unrelated formatting churn.|

For every failure:

- Check logs first.
- Run TRADING-026 dry-run manually only when the operator explicitly chooses to validate readiness.
- Do not run apply / rollback.
- Do not run broker / replay / trading.
- Do not run market, backtest, scoring, or data download pipelines from this runbook.

## 12. Disable / Pause Scheduler

Windows:

```text
Task Scheduler -> Find task -> Disable
```

cron:

```text
crontab -e
# comment out the operator brief line
```

GitHub Actions:

```text
Disable workflow or remove schedule trigger manually.
```

Do not delete logs before reviewing failures. Retain the latest scheduler log, operator brief JSON/Markdown, and any TRADING-026 dry-run artifact used to diagnose readiness.

## 13. Security and Safety Notes

The scheduler must never run:

- TRADING-018E2 apply.
- TRADING-018E3 rollback.
- broker execution.
- replay runner.
- trading execution.
- data download unless explicitly reviewed in future tasks.
- market, backtest, or scoring pipelines unless a future task explicitly changes the scope and updates the system flow.

The scheduler should only call reviewed read-only reporting scripts. It must not modify `config/weights/weight_profile_current.yaml`, shadow weight state, manual approval files, broker configuration, production reports, or runtime secrets.

## 14. Future Automation Boundary

TRADING-027 does not create scheduler.

A future task may create a scheduler configuration template generator, but it must remain opt-in, reviewable, and documentation-visible. A suitable follow-up is:

```text
TRADING-028: Scheduler Configuration Template Generator
```

TRADING-028 should generate reviewable templates such as Windows Task Scheduler XML, cron line, or GitHub Actions workflow examples. It still should not automatically install scheduler entries.

## 15. Generated Templates From TRADING-028

TRADING-028 已提供 template-only 生成器：

```bash
python scripts/generate_daily_operator_brief_scheduler_templates.py --date YYYY-MM-DD
```

生成目录：

```text
data/derived/operator_briefs/scheduler_templates/
```

使用要求：

- 只把生成的 `.template` 文件作为人工审查输入。
- 不要直接把 GitHub Actions template 写入 `.github/workflows/`。
- 不要把 cron template 写入 crontab。
- 不要导入 Windows Task Scheduler XML，除非 TRADING-026 dry run 已经 `READY` 且本 runbook checklist 已通过。
- 若 metadata `template_generation_status=SAFETY_BLOCKED`，不得复制或安装任何模板。

TRADING-028 仍然不创建、不安装、不启用 scheduler，不运行 TRADING-021/022/023/024/026，也不触发 apply、rollback、broker、replay、trading execution、data download、market/backtest/scoring pipeline。
