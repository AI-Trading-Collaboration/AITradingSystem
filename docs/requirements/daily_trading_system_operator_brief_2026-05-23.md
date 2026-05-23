# TRADING-022: Daily Trading System Operator Brief

关联任务：`TRADING-022`

当前状态：DONE

最后更新：2026-05-23

## 背景

TRADING-021 已提供参数治理 daily digest，用来回答参数治理是否安全、是否存在 pending
apply / rollback，以及是否有 safety anomaly。系统层面仍需要每日 operator brief，集中回答：

- 今天整个交易系统是否健康；
- 哪些 pipeline 成功、失败或未知；
- 数据是否新鲜；
- 哪些 artifacts 缺失；
- 哪些事项需要人工处理；
- 今天能否信任系统输出。

## 目标

- 新增 `scripts/run_daily_trading_system_operator_brief.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/daily_trading_system_operator_brief.py`。
- 必须只读读取 latest TRADING-021 parameter governance daily digest。
- 可选只读读取 pipeline health、data freshness、market report、backtest / weight iteration
  artifacts。
- 输出：
  - `data/derived/operator_briefs/daily_trading_system_operator_brief_YYYY-MM-DD.json`
  - `data/derived/operator_briefs/daily_trading_system_operator_brief_YYYY-MM-DD.md`
  - `data/derived/operator_briefs/logs/daily_trading_system_operator_brief_run_YYYY-MM-DD.json`
  - `data/derived/operator_briefs/logs/daily_trading_system_operator_brief_run_YYYY-MM-DD.md`
- Daily task dashboard 新增只读 `Daily Trading System Operator Brief` 卡片，只读取
  TRADING-022 operator brief artifact。
- 新增 runbook、system flow、artifact catalog 和专项测试。

## 安全边界

所有 TRADING-022 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "operator_brief_only": true,
  "read_only": true,
  "apply_executed_by_operator_brief": false,
  "rollback_executed_by_operator_brief": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 operator brief 生成器可以定时运行。TRADING-022 不得触发
018B、018C、018C2、018D、018E1、018E2、018E3、018F、019、020、021、market pipeline、
backtest pipeline、scoring pipeline、broker、replay 或 trading execution，不得写
production / shadow weights。

生成 operator brief 前必须校验 TRADING-021 digest：

- `task_id=TRADING-021`
- `production_effect=none`
- `digest_only=true`
- `governance_only=true`
- `apply_executed_by_digest=false`
- `rollback_executed_by_digest=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

安全字段异常时输出 `brief_status=SAFETY_BLOCKED`，仍生成 blocked JSON / Markdown，但不得静默生成
正常简报。

## 状态枚举

`brief_status` 只允许：

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

`INPUT_MISSING` 只用于必需的 TRADING-021 digest 缺失。可选 artifact 缺失只能使对应模块为
`UNKNOWN`，不得导致整个 brief 变成 `INPUT_MISSING`。

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-022，本文记录目标、边界、阶段和验收。|
|2. 核心 operator brief builder|DONE|只读加载 TRADING-021 digest 和可选 artifacts，输出 JSON/Markdown/run log；不调用任何上游 pipeline 或执行器。|
|3. 状态映射与安全校验|DONE|覆盖 OK/WATCH/ACTION_REQUIRED/URGENT/INPUT_MISSING/INPUT_INVALID/SAFETY_BLOCKED/ERROR；digest 安全字段异常必须 safety blocked。|
|4. Optional artifact 汇总|DONE|pipeline health、data freshness、market report、backtest/weight iteration 缺失时输出 UNKNOWN；存在时汇总 status、path、sha256 和关键字段。|
|5. Pending manual actions|DONE|从 digest action_required、pending_apply、pending_rollback 和 critical findings 生成人工待处理事项。|
|6. CLI|DONE|支持 `--date`、`--data-root`、`--parameter-governance-digest-file`、`--lookback-days`、`--fail-on-critical`、`--include-optional-artifacts`。|
|7. Dashboard|DONE|只读读取 latest operator brief artifact 并展示 brief status、summary level、headline、trust flag、manual action flag、parameter/pipeline/data 状态和 alert count。|
|8. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|9. 测试与 smoke|DONE|覆盖状态映射、optional artifacts、pending actions、Markdown banners、dashboard 只读和 output invariants。|
|10. 验证收尾|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 repo 外 smoke 通过；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断。|

## Dashboard 读取边界

Dashboard 只能读取
`data/derived/operator_briefs/daily_trading_system_operator_brief_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-022 script，也不运行 TRADING-021、020、019、
018B、018C、018C2、018D、018E1、018E2、018E3、018F，不触发 market、backtest、scoring、
broker、replay 或 trading execution。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-021 参数治理 daily digest
  后增加系统级每日 operator brief；本任务只聚合已有 artifacts，不重新运行上游 pipeline，不修改
  production/shadow，不触发 broker、replay 或 trading execution。
- 2026-05-23：实现完成并进入 `VALIDATING`。新增 operator brief builder、CLI、dashboard
  只读卡片、runbook、system flow、artifact catalog 和专项测试。验证通过
  `tests/trading_engine/test_daily_trading_system_operator_brief.py`、`tests/test_daily_task_dashboard.py`、
  `tests/trading_engine`、全量 `pytest`、`ruff check scripts src tests` 和 repo 外 OK /
  ACTION_REQUIRED / URGENT / SAFETY_BLOCKED smoke。`black --check scripts src tests`
  仅被既有无关 `tests/test_market_data.py` formatting baseline 阻断，未混入无关格式化 diff。
- 2026-05-23：最终收尾验证通过并进入 `DONE`。再次执行 repo 外临时 fixture smoke：
  OK 输出 `OK/NORMAL` 且 Markdown 包含 `Continue observation`；ACTION_REQUIRED 顶部包含
  `Action Required` 且 pending manual actions 为 true；URGENT 顶部包含
  `URGENT: Manual Attention Required` 且 critical alert 非空；SAFETY_BLOCKED 顶部包含
  `Operator Brief Safety Blocked`。四路径均确认 operator brief 输出固定
  `production_effect=none`、`manual_review_only=true`、`operator_brief_only=true`、
  `read_only=true`、`apply_executed_by_operator_brief=false`、
  `rollback_executed_by_operator_brief=false`、`broker_execution=false`、
  `replay_execution=false`、`trading_execution=false`，且 optional artifacts 缺失时显示
  `UNKNOWN` 而不是失败。Dashboard import guard 和专项测试确认 Daily Trading System Operator
  Brief 卡片只读读取 operator brief artifact，不触发 018B-022、market、backtest、scoring、
  broker、replay 或 trading execution。
