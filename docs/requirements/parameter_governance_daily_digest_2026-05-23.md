# TRADING-021: Parameter Governance Daily Digest

关联任务：`TRADING-021`

当前状态：DONE

最后更新：2026-05-23

## 背景

TRADING-018B 到 TRADING-020 已形成 parameter governance 的 shadow learning、comparison、
multi-day review、manual proposal、approved apply / rollback、lifecycle audit、summary 和
static web view 链路。TRADING-020 适合完整下钻，但每日复核还需要一份短摘要，快速回答：

- 今天参数治理是否安全；
- 是否需要人工操作；
- 是否存在 pending apply / rollback；
- 是否存在 safety anomaly；
- shadow 是否继续优于 production；
- 最新 promotion lifecycle 处于什么状态；
- 今天建议做什么。

## 目标

- 新增 `scripts/run_parameter_governance_daily_digest.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/parameter_governance_daily_digest.py`。
- 只读读取 latest TRADING-019 governance summary。
- 可选只读读取 latest TRADING-020 web view metadata、latest 018C2 multi-day review 和 latest
  018F lifecycle audit。
- 输出：
  - `data/derived/weight_iterations/governance/digests/parameter_governance_daily_digest_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/governance/digests/parameter_governance_daily_digest_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/governance/digests/logs/parameter_governance_daily_digest_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/governance/digests/logs/parameter_governance_daily_digest_run_YYYY-MM-DD.md`
- Daily task dashboard 新增只读 `Parameter Governance Daily Digest` 卡片，只读取 TRADING-021
  digest artifact。
- 新增 runbook、system flow、artifact catalog 和测试。

## 安全边界

所有 TRADING-021 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "digest_only": true,
  "governance_only": true,
  "apply_executed_by_digest": false,
  "rollback_executed_by_digest": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 digest 生成器可以定时运行。TRADING-021 不得触发
018B、018C、018C2、018D、018E1、018E2、018E3、018F、019、020、scoring、broker、
replay 或 trading execution，不得写 production / shadow weights。

生成 digest 前必须校验 TRADING-019 summary：

- `task_id=TRADING-019`
- `production_effect=none`
- `governance_only=true`
- `apply_executed_by_governance=false`
- `rollback_executed_by_governance=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

安全字段异常时输出 `digest_status=SAFETY_BLOCKED`，仍生成 blocked JSON / Markdown，但不得静默生成
正常摘要。

## 状态枚举

`digest_status` 只允许：

- `OK`
- `WATCH`
- `ACTION_REQUIRED`
- `URGENT`
- `INPUT_MISSING`
- `INPUT_INVALID`
- `SAFETY_BLOCKED`
- `ERROR`

`summary_level` 只允许：

- `NORMAL`
- `WATCH`
- `ACTION`
- `URGENT`
- `UNKNOWN`

状态映射以 TRADING-019 的 `governance_state`、`action_required`、`action_level` 和
`safety_boundary_audit.status` 为准，不重新生成治理判断。

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-021，本文记录目标、边界、阶段和验收。|
|2. 核心 digest builder|DONE|只读加载 summary 和可选 metadata，输出 digest JSON/Markdown/run log；不调用任何上游 pipeline 或执行器。|
|3. 状态映射与安全校验|DONE|覆盖 OK/WATCH/ACTION_REQUIRED/URGENT/INPUT_MISSING/INPUT_INVALID/SAFETY_BLOCKED/ERROR；安全字段异常必须 safety blocked。|
|4. Weight snapshot 与 pending readout|DONE|提取 production/shadow weights、delta summary、largest delta、pending items、review/lifecycle 状态和 alerts。|
|5. CLI|DONE|支持 `--date`、`--data-root`、`--governance-summary-file`、`--web-view-metadata-file`、`--lookback-days`、`--fail-on-safety-anomaly`。|
|6. Dashboard|DONE|只读读取 latest digest artifact 并展示 digest status、summary level、headline、state、pending apply/rollback、alert counts 和 Markdown path。|
|7. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|8. 测试与 smoke|DONE|覆盖状态映射、input/safety、Markdown banners、weight snapshot、dashboard 只读和 output invariants。|
|9. 验证收尾|DONE|最终 repo 外 smoke、dashboard 只读 guard、目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 已通过；全仓 black check 被既有 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。|

## Dashboard 读取边界

Dashboard 只能读取
`data/derived/weight_iterations/governance/digests/parameter_governance_daily_digest_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-021 script，也不运行 TRADING-020、TRADING-019、
018B、018C、018C2、018D、018E1、018E2、018E3、018F，不触发 scoring、broker、replay 或
trading execution。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-019 summary 与 TRADING-020
  web view 后增加每日快速阅读摘要；本任务只压缩、解释和重排 TRADING-019 summary，不生成新治理判断，
  不执行 promotion/apply/rollback，不触发 broker、replay 或 trading execution。
- 2026-05-23：实现完成并推进到 `VALIDATING`。新增 digest builder、CLI、dashboard 只读卡片、
  runbook、system flow、artifact catalog 和专项测试。验证结果：`tests/trading_engine/test_parameter_governance_daily_digest.py`
  21 passed；`tests/test_daily_task_dashboard.py` 8 passed；`tests/trading_engine` 389 passed / 1 warning；
  全量 `pytest` 973 passed / 1 warning；`ruff check scripts src tests` passed；触达文件 black check passed。
  全仓 `black --check scripts src tests` 被既有 `tests/test_market_data.py` 格式 baseline 阻断，未格式化该无关文件。
  Repo 外临时 smoke 覆盖 OK、ACTION_REQUIRED、URGENT、SAFETY_BLOCKED，均生成预期 status 与 Markdown banner。
- 2026-05-23：从 `VALIDATING` 改为 `DONE`。最终收尾再次使用 repo 外临时 fixture 运行 OK、
  ACTION_REQUIRED、URGENT、SAFETY_BLOCKED 四路径 smoke，并确认 digest 顶层安全边界固定为
  `production_effect=none`、`manual_review_only=true`、`digest_only=true`、`governance_only=true`、
  `apply_executed_by_digest=false`、`rollback_executed_by_digest=false`、`broker_execution=false`、
  `replay_execution=false`、`trading_execution=false`。Markdown 验证 OK 包含 `Continue observation`，
  ACTION_REQUIRED 顶部包含 `Action Required` 且 `pending_apply=true` 渲染为 pending apply，URGENT 顶部包含
  `URGENT: Manual Attention Required` 且 critical findings 显示在 Alerts/Critical，SAFETY_BLOCKED 顶部包含
  `Digest Safety Blocked`。Dashboard import guard 验证 Parameter Governance Daily Digest 卡片只读读取 digest
  artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/018F/019/020/021、scoring、broker、replay 或
  trading execution。收尾验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff；
  全仓 black check 仍只被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
