# TRADING-028: Daily Operator Brief Scheduler Configuration Templates

关联任务：`TRADING-028`

状态：`DONE`

## 目标

新增只读、template-only 的 Daily Operator Brief scheduler 配置模板生成器，生成可人工审查和复制的 Windows Task Scheduler、PowerShell、batch、cron、GitHub Actions 模板，以及 metadata JSON 和 summary Markdown。

TRADING-028 不安装 scheduler，不写系统级调度配置，不启用 GitHub Actions workflow，不运行 TRADING-021 / TRADING-022 / TRADING-023 / TRADING-024 / TRADING-026，不运行 market / backtest / scoring / data download / broker / replay / trading pipeline。

## 输出

- `data/derived/operator_briefs/scheduler_templates/daily_operator_brief_scheduler_templates_YYYY-MM-DD.json`
- `data/derived/operator_briefs/scheduler_templates/daily_operator_brief_scheduler_templates_YYYY-MM-DD.md`
- `data/derived/operator_briefs/scheduler_templates/windows/daily_operator_brief_task_YYYY-MM-DD.xml.template`
- `data/derived/operator_briefs/scheduler_templates/windows/run_daily_operator_brief_YYYY-MM-DD.ps1.template`
- `data/derived/operator_briefs/scheduler_templates/windows/run_daily_operator_brief_YYYY-MM-DD.bat.template`
- `data/derived/operator_briefs/scheduler_templates/cron/daily_operator_brief_cron_YYYY-MM-DD.txt.template`
- `data/derived/operator_briefs/scheduler_templates/github_actions/daily_operator_brief_workflow_YYYY-MM-DD.yml.template`

所有模板文件必须以 `.template` 结尾。禁止输出 `.github/workflows/*.yml`、真实 crontab 文件或系统 Task Scheduler 安装路径。

## 安全边界

所有 TRADING-028 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `scheduler_template_only=true`
- `read_only=true`
- `scheduler_created=false`
- `scheduler_installed=false`
- `scheduler_enabled=false`
- `operator_brief_executed_by_template_generator=false`
- `pipelines_executed_by_template_generator=false`
- `data_downloaded_by_template_generator=false`
- `apply_executed_by_template_generator=false`
- `rollback_executed_by_template_generator=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`
- `safe_for_scheduler=true`

`safe_for_scheduler=true` 只表示 template generator 本身可被定期运行，不表示生成的模板已安装、启用或获准。

## Status 规则

|状态|含义|
|---|---|
|`GENERATED`|所有请求模板已生成，safety validation 通过。|
|`GENERATED_WITH_WARNINGS`|部分可选模板未生成，或 repo / Python 路径使用 placeholder，safety validation 通过。|
|`SAFETY_BLOCKED`|输出路径不安全，或模板内容包含危险执行命令 / 自动安装路径。|
|`ERROR`|运行异常。|

## Safety Scan

模板生成后必须扫描模板文本和输出路径。以下内容必须阻断：

- `scripts/run_shadow_promotion_apply.py`
- `scripts/run_shadow_promotion_rollback.py`
- `schtasks /Create`
- `crontab -`
- 自动写入 `.github/workflows/`
- 系统 Task Scheduler 安装路径
- broker / replay / trading execution 命令

人工审查注释可以说明禁止项，但不得形成可执行命令。

## Dashboard

Daily task dashboard 新增 `Scheduler Configuration Templates` 只读卡片，只读取 latest TRADING-028 metadata artifact。卡片展示：

- `template_generation_status`
- `scheduler_created`
- `scheduler_installed`
- `scheduler_enabled`
- `manual_review_required`
- generated template count
- Windows / cron / GitHub Actions template path
- summary Markdown path

Dashboard 禁止触发 018B-027、028 script、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-028，本文记录目标、边界、状态规则和验收。|
|2. 核心模板生成器|DONE|实现配置输入、路径安全校验、模板渲染、metadata、Markdown 和 safety checklist。|
|3. CLI|DONE|新增 `scripts/generate_daily_operator_brief_scheduler_templates.py`。|
|4. Dashboard|DONE|新增只读卡片，只读取 TRADING-028 metadata artifact。|
|5. 文档|DONE|更新 runbook、system flow、artifact catalog。|
|6. 测试与验证|DONE|覆盖模板生成、安全边界、危险命令阻断、Markdown、dashboard、smoke 和 output invariants；全仓 Black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 进展记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。本阶段只允许生成可审阅模板 artifact，不创建真实 scheduler、不执行 operator brief 或上游任务、不触发任何交易或数据流水线。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 builder、CLI、Windows Task Scheduler XML / PowerShell / batch / cron / GitHub Actions `.template` 输出、metadata JSON、summary Markdown、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；验证通过 GENERATED / SAFETY_BLOCKED / template content smoke、`tests/trading_engine/test_daily_operator_brief_scheduler_templates.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-05-24：最终收尾验证完成并改为 `DONE`。repo 外 smoke 确认默认生成 `GENERATED`、5 个 `.template` 输出存在、危险 `.github/workflows` 输出为 `SAFETY_BLOCKED` 且不创建 unsafe 目录；模板内容全部包含 `TEMPLATE ONLY` 和 `Manual review required`，不包含 `run_shadow_promotion_apply.py`、`run_shadow_promotion_rollback.py`、`schtasks /Create`、`crontab -` 安装命令，active command 不包含 broker / replay / trading execution；metadata 15 项安全边界全部为预期值；确认无真实 Windows Task Scheduler XML 安装文件、crontab 文件、`.github/workflows/*.yml` 或 scheduler 自动安装脚本；dashboard import guard 确认 Scheduler Configuration Templates 卡片只读读取 TRADING-028 metadata artifact，不触发 018B-027、028 script、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断。
