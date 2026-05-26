# TRADING-037 Retry Candidate Queue / Manual Approval Gate

## 背景

TRADING-036 已在 TRADING-035 notification delivery audit summary 之上生成 failure classification / retry readiness，可以输出失败类别、`safe_to_retry`、`requires_manual_review`、`retry_blockers` 和 `recommended_actions`。当前缺口是系统还没有把 retryable failure 转换为稳定、可审查、可追踪的 retry candidate queue。

TRADING-037 新增只读 retry candidate queue 层。它只读取既有 TRADING-036 classification JSON，生成 retry candidate queue 和 manual approval gate report，不执行真实 retry，也不修改任何通知或 production 状态。

## 目标

1. 新增 `src/ai_trading_system/trading_engine/retry_candidate_queue.py`。
2. 新增 `scripts/run_retry_candidate_queue.py`。
3. 默认读取 latest TRADING-036 classification JSON；支持 `--classification-report` 指定源 JSON。
4. 输出 `outputs/retry_candidate_queue/retry_candidate_queue_YYYY-MM-DD.json`、`.md` 和 `.log`。
5. Daily task dashboard 新增只读 `Retry Candidate Queue` 卡片，只读取 TRADING-037 JSON。
6. 更新 runbook、requirements index、system flow、artifact catalog、task register 和测试。

## 安全边界

- `mode=read_only`
- `production_effect=none`
- `manual_review_only=true`
- 不发送 email / Slack / Discord / webhook / mobile push
- 不执行 retry 或重新投递 notification
- 不修改 delivery state
- 不修改 production 参数
- 不修改 TRADING-035 / TRADING-036 历史 artifact
- 不运行 TRADING-036 generator、TRADING-035 audit 或任何上游 notification pipeline
- Dashboard 只能读取 TRADING-037 JSON，不运行 queue generator、不执行 retry、不发送 notification、不修改 approval 状态

## 输入与输出

默认输入目录：

- `outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`

默认输出目录：

- `outputs/retry_candidate_queue/`

CLI 参数：

- `--classification-report <path>`
- `--output-dir <path>`
- `--as-of-date YYYY-MM-DD`
- `--fail-on-safety-blocked`

## 队列状态规则

|来源状态|TRADING-037 queue_status|candidate / blocked 行为|manual review|safe_to_execute_retry|
|---|---|---|---|---|
|TRADING-036 `PASS` 且无 failure|`EMPTY`|无 candidate，无 blocked item|false|false|
|存在 retryable failure|`PENDING_APPROVAL`|生成 `PENDING_APPROVAL` retry candidate|true|false|
|仅存在 non-retryable failure|`BLOCKED`|生成 blocked item|true|false|
|存在 `SAFETY_BLOCKED`|`SAFETY_BLOCKED`|禁止 retry，Markdown 高亮 critical banner|true|false|
|source missing / malformed|`SOURCE_UNAVAILABLE`|不崩溃，记录 source parse status 和 recommended action|true|false|

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 新增 TRADING-037，本文记录范围、安全边界、状态规则和验证计划。|
|2. 核心模块与 CLI|DONE|可只读加载 latest 或指定 TRADING-036 artifact，写 JSON/Markdown/log，`--fail-on-safety-blocked` 可在 safety blocked 时非零退出。|
|3. Dashboard 只读卡片|DONE|payload 和 HTML 展示 queue status、candidate/blocker counts、manual review、retry execution safety、approval status、generated_at 和 source classification status；import guard 证明不触发 queue generator 或发送/retry 路径。|
|4. 文档更新|DONE|runbook、requirements index、system flow、artifact catalog 更新，并说明 TRADING-037 不执行 retry、不发送 notification。|
|5. 测试与验证|VALIDATING|专项测试覆盖 queue 规则、missing/malformed source、Markdown banner、approval gate、safety invariants、CLI 和 dashboard 只读；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验收标准

- 能读取 TRADING-036 classification JSON。
- 能输出 retry candidate queue JSON、Markdown 和 run log。
- 正确区分 `EMPTY`、`PENDING_APPROVAL`、`BLOCKED`、`SAFETY_BLOCKED` 和 `SOURCE_UNAVAILABLE`。
- retryable failure 转为 `PENDING_APPROVAL` candidate。
- non-retryable failure 转为 blocked item。
- 输出 approval gate 和 safety invariants。
- Dashboard 新增只读卡片，缺 artifact 时 graceful display。
- 不发送任何外部通知，不执行 retry，不修改任何 production 或 delivery state。
- 不混入 `data/derived`、`outputs/parameter_search` 或虚拟环境目录的生成物。

## 进展记录

- 2026-05-26：新增并进入 IN_PROGRESS。原因：owner 要求在 TRADING-036 failure classification / retry readiness 之上建立只读 retry candidate queue 和 manual approval gate；当前阶段严格 queue/report-only，不执行通知、重试、状态变更或 production 参数变更。
- 2026-05-26：进入 VALIDATING。已完成核心模块、CLI、retry candidate queue JSON/Markdown/log、dashboard 只读卡片、runbook、requirements index、system flow、artifact catalog 和专项测试；验证通过 `tests/trading_engine/test_retry_candidate_queue.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断，未格式化该无关文件。
