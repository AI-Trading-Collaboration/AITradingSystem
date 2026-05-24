# TRADING-029: Daily Operator Brief Scheduler Template Validation

关联任务：`TRADING-029`

状态：`DONE`

## 目标

新增只读、validation-only 的 Daily Operator Brief scheduler template validation report。它读取 TRADING-028 生成的 metadata JSON 和其中声明的 `.template` 文件，执行静态安全检查、基础语法检查、路径检查和命令 allowlist 检查，并生成 JSON / Markdown / run log。

TRADING-029 不安装 scheduler、不启用 scheduler、不运行模板、不运行 operator brief、不运行 TRADING-021/022/023/024/026/028，不运行 market / backtest / scoring / data download / broker / replay / trading pipeline。

## 输入

- TRADING-028 metadata：`data/derived/operator_briefs/scheduler_templates/daily_operator_brief_scheduler_templates_YYYY-MM-DD.json`
- Windows Task Scheduler XML template：`*.xml.template`
- PowerShell wrapper template：`*.ps1.template`
- batch wrapper template：`*.bat.template`
- cron line template：`*.txt.template`
- GitHub Actions workflow template：`*.yml.template`

## 输出

- `data/derived/operator_briefs/scheduler_template_validation/daily_operator_brief_scheduler_template_validation_YYYY-MM-DD.json`
- `data/derived/operator_briefs/scheduler_template_validation/daily_operator_brief_scheduler_template_validation_YYYY-MM-DD.md`
- `data/derived/operator_briefs/scheduler_template_validation/logs/daily_operator_brief_scheduler_template_validation_run_YYYY-MM-DD.json`
- `data/derived/operator_briefs/scheduler_template_validation/logs/daily_operator_brief_scheduler_template_validation_run_YYYY-MM-DD.md`

## 安全边界

所有 TRADING-029 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `scheduler_template_validation_only=true`
- `read_only=true`
- `safe_for_scheduler=true`
- `scheduler_created=false`
- `scheduler_installed=false`
- `scheduler_enabled=false`
- `templates_executed_by_validator=false`
- `operator_brief_executed_by_validator=false`
- `pipelines_executed_by_validator=false`
- `data_downloaded_by_validator=false`
- `apply_executed_by_validator=false`
- `rollback_executed_by_validator=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

`safe_for_scheduler=true` 只表示 validator 本身可运行，不表示 scheduler template 已安装、启用或获准。

## Validation Status

|状态|含义|
|---|---|
|`PASS`|metadata 安全边界通过，所有声明模板存在，路径、后缀、基础语法、allowlist 和危险扫描均通过。|
|`PASS_WITH_WARNINGS`|没有阻断问题，但存在 placeholder path、风险词说明或其他需人工复核 warning。|
|`FAIL`|模板缺失、后缀不合法、基础语法不合法或 required safety text 缺失。|
|`SAFETY_BLOCKED`|metadata safety 异常、危险命令、危险路径、scheduler 安装命令或 template 试图写真实 scheduler 位置。|
|`INPUT_MISSING`|TRADING-028 metadata 缺失。|
|`INPUT_INVALID`|metadata JSON 无法解析、不是对象或 `task_id != TRADING-028`。|
|`ERROR`|运行异常。|

## Template Status

|状态|含义|
|---|---|
|`PASS`|单个模板存在，`.template` 后缀、路径、语法、allowlist 和危险扫描均通过。|
|`WARNING`|仅有非阻断 warning，例如 placeholder path。|
|`FAIL`|模板缺失必要结构、语法失败、后缀不合法或 required safety text 缺失。|
|`SAFETY_BLOCKED`|发现危险命令、危险路径、scheduler install 命令或 unsafe output path。|
|`MISSING`|metadata 声明模板但文件不存在。|
|`ERROR`|单项检查异常。|

## 检查范围

- metadata 必须为 `TRADING-028`，且 TRADING-028 safety fields 全部符合 template-only 边界。
- 所有模板路径必须位于 `data/derived/operator_briefs/scheduler_templates/`，且文件名以 `.template` 结尾。
- 禁止模板路径落入 `.github/workflows/`、`C:\Windows\System32\Tasks`、`/etc/cron.d`、`/var/spool/cron` 或真实 `crontab` 文件。
- XML template 必须可由 XML parser 解析，并包含 `Task` / `Actions` / `Exec`。
- GitHub Actions YAML template 必须可由 YAML parser 解析，并包含 `name` / `on` / `jobs`。
- cron template 必须存在非注释 cron 行，且至少包含 5 个时间字段和允许命令。
- PowerShell / batch template 必须包含 `TEMPLATE ONLY` 和 `Manual review required`，且 active command 只允许 dry-run / operator brief 调用。

## 禁止命令和路径

发现以下 active command 或危险路径时必须 `SAFETY_BLOCKED`：

- `scripts/run_shadow_promotion_apply.py`
- `scripts/run_shadow_promotion_rollback.py`
- `run_shadow_promotion_apply`
- `run_shadow_promotion_rollback`
- `schtasks /Create`
- `schtasks.exe /Create`
- `crontab -`
- `crontab -e`
- `broker execution`
- `replay runner`
- `trading execution`
- `.github/workflows/`
- `C:\Windows\System32\Tasks`
- `/etc/cron.d`
- `/var/spool/cron`

`apply` / `rollback` 作为安全说明中的禁止项可以出现在注释中；作为 active command 或 script path 必须阻断。

## Dashboard

Daily task dashboard 新增 `Scheduler Template Validation Report` 只读卡片，只读取 latest TRADING-029 validation artifact。卡片展示：

- `validation_status`
- `summary_level`
- `templates_declared`
- `templates_found`
- `templates_passed`
- `templates_with_warnings`
- `templates_failed`
- critical findings count
- warnings count
- Markdown path

Dashboard 禁止触发 018B-028、TRADING-029 script、template generator、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-029，本文记录目标、边界、状态规则、输出和验收。|
|2. 核心 validator|DONE|实现 metadata 加载、模板路径解析、静态语法校验、allowlist、危险扫描、status aggregation、JSON/Markdown/run log 输出。|
|3. CLI|DONE|新增 `scripts/validate_daily_operator_brief_scheduler_templates.py`。|
|4. Dashboard|DONE|新增只读卡片，只读取 TRADING-029 validation artifact。|
|5. 文档|DONE|新增 runbook，更新 system flow 和 artifact catalog。|
|6. 测试与验证|DONE|覆盖 metadata、coverage、syntax、dangerous commands、required safety text、status aggregation、dashboard 和 output safety invariants；全仓 Black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 进展记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。本阶段只允许静态验证 TRADING-028 `.template` artifact，不创建或启用 scheduler，不执行模板、operator brief、上游 pipeline 或交易执行。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 validator、CLI、validation JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；repo 外 smoke 确认正常模板 `PASS`、invalid XML `FAIL`、apply script `SAFETY_BLOCKED`、placeholder path `PASS_WITH_WARNINGS`；验证通过 `tests/trading_engine/test_daily_operator_brief_scheduler_template_validation.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-05-24：最终收尾验证完成并改为 `DONE`。repo 外 smoke 再次确认正常模板 `PASS`、invalid XML 输出 `FAIL`、模板包含 `run_shadow_promotion_apply.py` 输出 `SAFETY_BLOCKED`、placeholder path 输出 `PASS_WITH_WARNINGS`；四路径均确认 validation output 顶层安全边界为 `production_effect=none`、`manual_review_only=true`、`scheduler_template_validation_only=true`、`read_only=true`、`scheduler_created=false`、`scheduler_installed=false`、`scheduler_enabled=false`、`templates_executed_by_validator=false`、`operator_brief_executed_by_validator=false`、`pipelines_executed_by_validator=false`、`data_downloaded_by_validator=false`、`apply_executed_by_validator=false`、`rollback_executed_by_validator=false`、`broker_execution=false`、`replay_execution=false`、`trading_execution=false`；dashboard import guard 确认 Scheduler Template Validation Report 卡片只读读取 TRADING-029 artifact，不触发 018B-028、TRADING-029 script、template generator、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading；收尾验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
