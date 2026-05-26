# TRADING-038 Manual Approval Record / Retry Execution Dry Run

## 背景

TRADING-037 已能把 TRADING-036 的 retryable notification delivery failure 转换成
`PENDING_APPROVAL` retry candidates，并保持不执行 retry、不发送 notification、不修改
delivery state、approval state 或 production 参数。当前缺口是系统还没有可审计地表达人工
批准意图，也没有在真实 retry executor 之前生成 dry-run-only 模拟报告。

TRADING-038 在 TRADING-037 queue 之上新增人工批准记录读取和 retry execution dry-run
层。它只读取既有 queue JSON 和可选人工 approval JSON，生成 dry-run JSON、Markdown 和
run log，不执行真实 retry，也不修改 approval 输入或任何生产状态。

## 目标

1. 新增 `src/ai_trading_system/trading_engine/retry_execution_dry_run.py`。
2. 新增 `scripts/run_retry_execution_dry_run.py`。
3. 默认读取 latest TRADING-037 retry candidate queue；支持 `--queue-report` 指定源 JSON。
4. 支持可选 `--approval-record`，默认只读查找
   `inputs/manual_retry_approvals/manual_retry_approval_YYYY-MM-DD.json`，其次兼容
   `configs/manual_retry_approval_YYYY-MM-DD.json`。
5. 输出 `outputs/retry_execution_dry_run/retry_execution_dry_run_YYYY-MM-DD.json`、`.md`
   和 `.log`。
6. Daily task dashboard 新增只读 `Retry Execution Dry Run` 卡片，只读取 TRADING-038 JSON。
7. 更新 runbook、requirements index、system flow、artifact catalog、task register 和测试。

## 安全边界

- `mode=dry_run_only`
- `production_effect=none`
- `manual_review_only=true`
- approval record 是输入，只读，不由程序创建或修改
- 不发送 email / Slack / Discord / webhook / mobile push
- 不执行真实 retry 或重新投递 notification
- 不修改 delivery state
- 不修改 approval state
- 不修改 production 参数
- 不修改 TRADING-035 / TRADING-036 / TRADING-037 历史 artifact
- 不运行 TRADING-037 generator、TRADING-036 classifier、TRADING-035 audit 或任何上游
  notification pipeline
- Dashboard 只能读取 TRADING-038 JSON，不运行 dry-run generator、不执行 retry、不发送
  notification、不修改 approval record 或 delivery state

## 输入与输出

默认输入目录：

- `outputs/retry_candidate_queue/retry_candidate_queue_YYYY-MM-DD.json`
- `inputs/manual_retry_approvals/manual_retry_approval_YYYY-MM-DD.json`
- `configs/manual_retry_approval_YYYY-MM-DD.json`

默认输出目录：

- `outputs/retry_execution_dry_run/`

CLI 参数：

- `--queue-report <path>`
- `--approval-record <path>`
- `--output-dir <path>`
- `--as-of-date YYYY-MM-DD`
- `--fail-on-safety-blocked`
- `--fail-on-approval-mismatch`

## 状态规则

|条件|dry_run_status|行为|
|---|---|---|
|TRADING-037 queue 缺失、malformed 或 report type 非法|`SOURCE_QUEUE_UNAVAILABLE`|不崩溃，不生成 simulated action，真实 retry 不允许|
|TRADING-037 `queue_status=EMPTY`|`NOTHING_TO_DRY_RUN`|不生成 simulated action，真实 retry 不允许|
|TRADING-037 `queue_status=SAFETY_BLOCKED`|`SAFETY_BLOCKED`|不生成 simulated action，Markdown 高亮 critical banner|
|存在 `PENDING_APPROVAL` candidate 但无 approval record|`WAITING_FOR_MANUAL_APPROVAL`|不生成 simulated action|
|approval record 中 candidate_id 不存在于 queue|`APPROVAL_MISMATCH`|mismatch 进入 blocked item，真实 retry 不允许|
|candidate 为 `PENDING_APPROVAL` 且 approval 为 `APPROVED_FOR_DRY_RUN`，同时 safety constraints 保持 dry-run only|`READY_FOR_DRY_RUN`|只生成 simulated retry action，所有 actual/external/state mutation 字段为 false|

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 新增 TRADING-038，本文记录范围、安全边界、状态规则和验证计划。|
|2. 核心模块与 CLI|DONE|可只读加载 latest 或指定 TRADING-037 artifact 和可选 approval record，写 JSON/Markdown/log，fail flags 可在 safety blocked / approval mismatch 时非零退出。|
|3. Dashboard 只读卡片|DONE|payload 和 HTML 展示 dry-run status、candidate/action counts、retry safety、generated_at 和 source queue status；import guard 证明不触发 dry-run generator 或发送/retry 路径。|
|4. 文档更新|DONE|runbook、requirements index、system flow、artifact catalog 更新，并说明 TRADING-038 只做 dry-run，不执行 retry。|
|5. 测试与验证|VALIDATING|专项测试覆盖 source missing/malformed、empty、waiting approval、ready、mismatch、rejected、safety blocked、Markdown、CLI 和 dashboard 只读；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；全仓 black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，TRADING-038 触达文件 Black 通过。|

## 验收标准

- 能读取 TRADING-037 retry candidate queue。
- 能读取可选 manual approval record。
- 能输出 retry execution dry-run JSON、Markdown 和 run log。
- 正确区分 `SOURCE_QUEUE_UNAVAILABLE`、`NOTHING_TO_DRY_RUN`、
  `WAITING_FOR_MANUAL_APPROVAL`、`READY_FOR_DRY_RUN`、`APPROVAL_MISMATCH` 和
  `SAFETY_BLOCKED`。
- approved candidate 可生成 deterministic simulated retry action。
- rejected / mismatch / blocked candidate 不生成 retry action。
- `actual_retry_executed`、`external_delivery_executed` 和 `state_mutation_executed` 永远为
  false。
- Dashboard 新增只读卡片，缺 artifact 时 graceful display。
- 不发送任何外部通知，不执行 retry，不修改任何 production、delivery 或 approval state。
- 不混入 `data/derived`、`outputs/parameter_search` 或虚拟环境目录的生成物。

## 进展记录

- 2026-05-26：新增并进入 IN_PROGRESS。原因：owner 要求在 TRADING-037 retry candidate
  queue / manual approval gate 之上建立只读 manual approval record 和 retry execution
  dry-run 层；当前阶段严格 dry-run/report-only，不执行通知、重试、状态变更或 production
  参数变更。
- 2026-05-26：进入 VALIDATING。已完成核心模块、CLI、retry execution dry-run
  JSON/Markdown/log、dashboard 只读卡片、runbook、requirements index、system flow、
  artifact catalog 和专项测试；验证通过
  `tests/trading_engine/test_retry_execution_dry_run.py`、`tests/test_daily_task_dashboard.py`、
  `tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仅被既有无关
  `tests/test_market_data.py` baseline 阻断，TRADING-038 触达文件 Black 通过。
