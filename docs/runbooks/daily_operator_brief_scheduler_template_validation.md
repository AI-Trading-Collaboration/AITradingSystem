# Daily Operator Brief Scheduler Template Validation

## 1. Purpose

TRADING-029 对 TRADING-028 生成的 Daily Operator Brief scheduler `.template` 文件做独立静态验证。它用于回答模板是否存在、路径是否安全、基础语法是否可解析、命令是否只调用允许脚本，以及是否包含 apply / rollback / broker / replay / trading 等危险命令。

本任务只验证模板，不安装 scheduler，不运行模板，不运行 operator brief。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "scheduler_template_validation_only": true,
  "read_only": true,
  "scheduler_created": false,
  "scheduler_installed": false,
  "scheduler_enabled": false,
  "templates_executed_by_validator": false,
  "operator_brief_executed_by_validator": false,
  "pipelines_executed_by_validator": false,
  "data_downloaded_by_validator": false,
  "apply_executed_by_validator": false,
  "rollback_executed_by_validator": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 validator 本身可以被运行，不表示 scheduler template 已安装、启用或获准。

## 2. Run Validation

默认读取 latest TRADING-028 metadata：

```bash
python scripts/validate_daily_operator_brief_scheduler_templates.py --date 2026-05-24
```

指定 metadata：

```bash
python scripts/validate_daily_operator_brief_scheduler_templates.py \
  --date 2026-05-24 \
  --template-metadata-file data/derived/operator_briefs/scheduler_templates/daily_operator_brief_scheduler_templates_2026-05-24.json
```

常用严格退出：

```bash
python scripts/validate_daily_operator_brief_scheduler_templates.py \
  --date 2026-05-24 \
  --fail-on-warning \
  --fail-on-critical
```

输出目录：

```text
data/derived/operator_briefs/scheduler_template_validation/
data/derived/operator_briefs/scheduler_template_validation/logs/
```

## 3. validation_status

|状态|处理|
|---|---|
|`PASS`|可进入人工审查阶段；仍不得自动安装。|
|`PASS_WITH_WARNINGS`|先处理 warning 或记录人工接受理由，再进入人工审查。|
|`FAIL`|修复缺失模板、后缀、required safety text 或语法问题后重新验证。|
|`SAFETY_BLOCKED`|停止安装流程；检查危险命令、危险路径或 metadata safety 异常。|
|`INPUT_MISSING`|先生成或定位 TRADING-028 metadata。|
|`INPUT_INVALID`|修复 metadata JSON 或确认 `task_id=TRADING-028`。|
|`ERROR`|检查 run log 和异常信息后重试。|

## 4. template status

|状态|处理|
|---|---|
|`PASS`|单个模板静态验证通过。|
|`WARNING`|存在 placeholder path 或风险词说明，需人工复核。|
|`FAIL`|缺少必要结构、语法失败、后缀不合法或缺少 required safety text。|
|`SAFETY_BLOCKED`|发现危险命令、危险路径或 scheduler install 行为。|
|`MISSING`|metadata 声明模板但文件不存在。|
|`ERROR`|单项检查异常，查看 blocking reasons。|

## 5. PASS_WITH_WARNINGS Handling

`PASS_WITH_WARNINGS` 不代表可以跳过人工审查。常见 warning 是 placeholder repo path、非阻断风险词说明或需要确认的环境路径。

处理顺序：

1. 打开 validation Markdown。
2. 阅读 `Warnings` 和对应 template result。
3. 确认 warning 只是人工安装前的占位或说明。
4. 记录人工接受理由，或回到 TRADING-028 重新生成模板。

## 6. FAIL Handling

`FAIL` 通常来自缺失文件、`.template` 后缀不合法、XML/YAML/cron 基础语法失败，或 PowerShell / batch 缺少 `TEMPLATE ONLY` / `Manual review required`。

不要手工编辑 validation JSON。修复 TRADING-028 template artifact 后重新运行 validator。

## 7. SAFETY_BLOCKED Handling

`SAFETY_BLOCKED` 表示模板不应进入人工安装流程。

立即检查：

- 是否出现 apply / rollback script。
- 是否出现 `schtasks /Create` 或 `crontab -`。
- 是否试图写 `.github/workflows/`、系统 Task Scheduler 或 cron 目录。
- TRADING-028 metadata safety fields 是否异常。
- 模板 active command 是否包含 broker / replay / trading execution。

## 8. Prohibited Commands

模板 active command 禁止出现：

- `scripts/run_shadow_promotion_apply.py`
- `scripts/run_shadow_promotion_rollback.py`
- `run_shadow_promotion_apply`
- `run_shadow_promotion_rollback`
- `schtasks /Create`
- `schtasks.exe /Create`
- `crontab -`
- `crontab -e`
- broker execution
- replay runner
- trading execution

## 9. Allowlist

允许脚本：

- `scripts/run_daily_operator_brief_scheduler_dry_run.py`
- `scripts/run_daily_trading_system_operator_brief.py`

允许外部命令：

- `python`
- `python3`
- `powershell`
- `pwsh`
- `cd`
- `Set-Location`
- `Get-Date`
- `Write-Output`
- `Write-Error`
- `echo`
- `exit`

## 10. Why No Installation

Scheduler 安装会改变未来自动执行行为。TRADING-029 的职责是验证 TRADING-028 产物是否适合进入人工审查，不负责创建、复制、安装或启用任何 scheduler。

推荐顺序：

```text
TRADING-026 dry run
  -> TRADING-027 manual installation runbook
  -> TRADING-028 scheduler template generator
  -> TRADING-029 scheduler template validation
  -> manual review
  -> manual installation only if approved
```

## 11. Dashboard

Daily task dashboard 的 `Scheduler Template Validation Report` 卡片只读取 latest TRADING-029 validation artifact，展示 status、coverage、critical/warning counts 和 Markdown path。Dashboard 不运行 validator、template generator、operator brief、scheduler creation 或任何交易/数据流水线。
