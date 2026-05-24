# Daily Operator Brief Scheduler Configuration Templates

## 1. Purpose

TRADING-028 生成 Daily Operator Brief scheduler 的可审阅配置模板，降低人工拼接 Windows Task Scheduler、cron 和 GitHub Actions 配置时的路径和命令错误。

本任务只生成模板，不安装 scheduler，不写系统级调度配置，不启用 GitHub Actions workflow。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "scheduler_template_only": true,
  "read_only": true,
  "scheduler_created": false,
  "scheduler_installed": false,
  "scheduler_enabled": false,
  "operator_brief_executed_by_template_generator": false,
  "pipelines_executed_by_template_generator": false,
  "data_downloaded_by_template_generator": false,
  "apply_executed_by_template_generator": false,
  "rollback_executed_by_template_generator": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 template generator 本身可运行，不表示生成模板已安装、启用或获准。

## 2. Generate Templates

默认生成所有模板：

```bash
python scripts/generate_daily_operator_brief_scheduler_templates.py --date 2026-05-24
```

常用参数：

```bash
python scripts/generate_daily_operator_brief_scheduler_templates.py \
  --date 2026-05-24 \
  --repo-root D:\Work\AITradingSystem \
  --python-path D:\Work\AITradingSystem\.venv\Scripts\python.exe \
  --expected-run-hour 9 \
  --expected-run-minute 0 \
  --timezone Asia/Tokyo
```

默认输出目录：

```text
data/derived/operator_briefs/scheduler_templates/
```

## 3. Template Files

生成产物：

- `daily_operator_brief_scheduler_templates_YYYY-MM-DD.json`
- `daily_operator_brief_scheduler_templates_YYYY-MM-DD.md`
- `windows/daily_operator_brief_task_YYYY-MM-DD.xml.template`
- `windows/run_daily_operator_brief_YYYY-MM-DD.ps1.template`
- `windows/run_daily_operator_brief_YYYY-MM-DD.bat.template`
- `cron/daily_operator_brief_cron_YYYY-MM-DD.txt.template`
- `github_actions/daily_operator_brief_workflow_YYYY-MM-DD.yml.template`

所有配置模板必须保留 `.template` 后缀，直到人工审查完成并决定复制到真实 scheduler 配置位置。

## 4. Manual Review

复制任何模板前必须检查：

- [ ] metadata `template_generation_status` 是 `GENERATED` 或已审查的 `GENERATED_WITH_WARNINGS`。
- [ ] `scheduler_created=false`。
- [ ] `scheduler_installed=false`。
- [ ] `scheduler_enabled=false`。
- [ ] `manual_review_required.required=true`。
- [ ] 所有模板仍以 `.template` 结尾。
- [ ] 模板命令只运行 TRADING-026 dry run 和 TRADING-022 operator brief。
- [ ] 未出现 apply / rollback / broker / replay / trading execution 命令。
- [ ] 日志路径位于 `data/derived/operator_briefs/scheduler_logs/` 或经人工批准的等价路径。

## 5. Use With TRADING-027

TRADING-028 只减少人工拼接成本。真实安装步骤仍以 `docs/runbooks/daily_operator_brief_scheduler_manual_installation.md` 为准。

推荐顺序：

```text
TRADING-026 dry run READY
  -> TRADING-027 manual installation checklist
  -> TRADING-028 generated templates review
  -> manually copy reviewed template
  -> manually enable scheduler
```

如果 TRADING-026 为 `NOT_READY`、`SAFETY_BLOCKED` 或 `ERROR`，不得安装或启用 scheduler。

## 6. Why No Automatic Installation

Scheduler 安装会改变运行环境和未来自动执行行为。当前系统仍处于人工审查优先阶段，自动安装可能绕过 dry-run 决策、路径检查、环境权限检查和人工确认。

因此 TRADING-028 禁止：

- 创建 Windows Task Scheduler task。
- 写入 crontab。
- 写入 `.github/workflows/*.yml`。
- 调用 `schtasks`。
- 调用 `crontab`。
- 启用 GitHub Actions schedule。

## 7. Handling SAFETY_BLOCKED

`SAFETY_BLOCKED` 表示模板路径或模板内容违反安全边界。

处理步骤：

1. 阅读 metadata `safety_validation.blocking_reasons`。
2. 确认输出路径是否位于 `data/derived/operator_briefs/scheduler_templates/`。
3. 检查模板命令是否包含禁止脚本或安装命令。
4. 修正输入参数后重新生成。
5. 不要通过手工编辑 metadata 清除 `SAFETY_BLOCKED`。

被阻断时，生成器不会写可安装 scheduler 模板文件。

## 8. Prohibited Commands

模板和真实 scheduler 都不得运行：

- `scripts/run_shadow_promotion_apply.py`
- `scripts/run_shadow_promotion_rollback.py`
- `schtasks /Create`
- `crontab -`
- broker runner
- replay runner
- trading execution
- market pipeline
- backtest pipeline
- scoring pipeline
- data download

`.github/workflows/` 可以在注释中作为人工复制说明出现，但 TRADING-028 不得把 workflow 文件直接写入该目录。

## 9. Future Extensions

未来 TRADING-029 可以增加模板质量验证，例如：

- Windows XML schema basic validation。
- cron syntax basic validation。
- GitHub Actions YAML parse validation。
- command allowlist validation。
- path safety validation。

TRADING-029 仍应保持 validation-only，不安装 scheduler。
