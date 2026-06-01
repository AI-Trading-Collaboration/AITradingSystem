# TRADING-036 Notification Delivery Failure Classification / Retry Readiness

最后更新：2026-05-26

## 背景

TRADING-035 已生成只读 `Notification Delivery Audit Summary`，可以回答通知链路审计状态是否为 `PASS`、`INCOMPLETE`、`MISMATCH`、`SAFETY_BLOCKED` 或异常状态。当前缺口是审计状态还没有转化为可行动的失败分类、重试准备度和人工复核要求。

TRADING-036 在 TRADING-035 artifact 之上新增只读 classification 层。它只读取既有 audit summary，输出失败类别、是否需要人工复核、是否安全重试、是否阻断后续通知链路和建议动作。

## 目标

1. 新增 `src/ai_trading_system/trading_engine/notification_delivery_failure_classification.py`。
2. 新增 `scripts/run_notification_delivery_failure_classification.py`。
3. 默认读取 latest TRADING-035 audit summary；支持 `--audit-summary` 指定源 JSON。
4. 输出 `outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`、`.md` 和 `.log`。
5. Daily task dashboard 新增只读 `Notification Delivery Failure Classification` 卡片，只读取 TRADING-036 JSON。
6. 更新 runbook、requirements index、system flow、artifact catalog、task register 和测试。

## 安全边界

- `mode=read_only`
- `production_effect=none`
- `manual_review_only=true`
- 不发送 email / Slack / Discord / webhook / mobile push
- 不创建或修改 Gmail draft
- 不运行 TRADING-030/031/034/035 或任何发送、market、backtest、scoring、data download、broker、replay、trading pipeline
- 不自动 retry
- 不修改历史投递状态
- 不修改 production 参数
- Dashboard 只能读取 TRADING-036 JSON，不运行 classifier 或任何 notification pipeline

## 输入与输出

默认 source finder 同时支持当前项目实际 TRADING-035 路径和外部兼容路径：

- `data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json`
- `outputs/notification_delivery_audit_summary/notification_delivery_audit_summary_YYYY-MM-DD.json`

默认输出目录：

- `outputs/notification_delivery_failure_classification/`

CLI 参数：

- `--audit-summary <path>`
- `--output-dir <path>`
- `--as-of-date YYYY-MM-DD`
- `--fail-on-critical`

## 分类规则

|TRADING-035 audit_status|TRADING-036 category|overall_status|highest_severity|manual review|safe_to_retry|blocks chain|
|---|---|---|---|---|---|---|
|`PASS`|none|`PASS`|`NONE`|false|false|false|
|`INCOMPLETE`|`MISSING_ARTIFACT` 或 `CONFIGURATION_FAILURE`|`ERROR`|`ERROR`|true|false|true|
|`MISMATCH`|`CONTENT_MISMATCH`|`ERROR`|`ERROR`|true|false|true|
|`SAFETY_BLOCKED`|`SAFETY_BLOCKED`|`CRITICAL`|`CRITICAL`|true|false|true|
|unknown / malformed / missing source|`UNKNOWN` 或 `MISSING_ARTIFACT`|`UNKNOWN` 或 `ERROR`|`WARN` / `ERROR`|true|false|true|

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 新增 TRADING-036，本文记录范围、安全边界、分类规则和验证计划。|
|2. 核心模块与 CLI|DONE|可只读加载 latest 或指定 TRADING-035 artifact，写 JSON/Markdown/log，`--fail-on-critical` 可在 critical 时非零退出。|
|3. Dashboard 只读卡片|DONE|payload 和 HTML 展示 overall_status、highest_severity、total_failures、manual review、safe_to_retry、blocks chain、source audit status 和 generated_at；import guard 证明不触发 classifier 或发送路径。|
|4. 文档更新|DONE|runbook、requirements index、system flow、artifact catalog 更新，并说明 TRADING-036 不发送通知、不自动 retry。|
|5. 测试与验证|VALIDATING|专项测试覆盖分类、missing/malformed/unknown、Markdown banner、retry readiness、safety invariants、CLI 和 dashboard 只读；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验收标准

- 能读取 TRADING-035 audit summary。
- 能输出 failure classification JSON、Markdown 和 run log。
- 正确区分 `PASS`、`INCOMPLETE`、`MISMATCH`、`SAFETY_BLOCKED`、unknown、missing source 和 malformed source。
- 输出 retry readiness、manual review requirement、notification chain blocking 和 recommended actions。
- Dashboard 新增只读卡片，缺 artifact 时 graceful display。
- 不发送任何外部通知，不修改任何 production 或 delivery state。
- 不混入 `data/derived`、`outputs/parameter_search` 或虚拟环境目录的生成物。

## 进展记录

- 2026-05-26：新增并进入 IN_PROGRESS。原因：owner 要求在 TRADING-035 audit summary 之上建立只读 failure classification / retry readiness 层；当前阶段严格 classify/report-only，不执行通知、重试、状态变更或 production 参数变更。
- 2026-05-26：进入 VALIDATING。已完成核心模块、CLI、classification JSON/Markdown/log、dashboard 只读卡片、runbook、requirements index、system flow、artifact catalog 和专项测试；验证通过 `tests/trading_engine/test_notification_delivery_failure_classification.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断，未格式化该无关文件。
