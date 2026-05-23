# TRADING-023: Pipeline Health Summary

关联任务：`TRADING-023`

当前状态：DONE

最后更新：2026-05-23

## 背景

TRADING-022 已能生成 Daily Trading System Operator Brief，但系统还缺少专门的 pipeline
health artifact，因此 operator brief 的 `pipeline_health` 可能仍为 `UNKNOWN`。

TRADING-023 补齐这一层：只读扫描既有 artifacts、run logs、summary 文件、日期戳、状态字段和安全字段，
生成 Pipeline Health Summary，供 dashboard 展示，并为后续 TRADING-022 接入提供稳定输入。

## 目标

- 新增 `scripts/run_pipeline_health_summary.py`。
- 新增核心模块 `src/ai_trading_system/trading_engine/pipeline_health_summary.py`。
- 内置首版 pipeline registry，覆盖 TRADING-018B 到 TRADING-022。
- 输出：
  - `data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.json`
  - `data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.md`
  - `data/derived/pipeline_health/logs/pipeline_health_summary_run_YYYY-MM-DD.json`
  - `data/derived/pipeline_health/logs/pipeline_health_summary_run_YYYY-MM-DD.md`
- Daily task dashboard 新增只读 `Pipeline Health Summary` 卡片，只读取 TRADING-023 artifact。
- 新增 runbook、system flow、artifact catalog 和专项测试。

## 安全边界

TRADING-023 必须严格只读：

- 不运行 018B/018C/018C2/018D/018E1/018E2/018E3/018F/019/020/021/022。
- 不运行 market、backtest、scoring、broker、paper runner、replay runner 或 trading execution。
- 不修改 production profile、shadow weights、approved profile 或任何上游 artifact。
- Dashboard 只能读取 TRADING-023 artifact，不运行 TRADING-023 script。

所有 TRADING-023 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "pipeline_health_only": true,
  "read_only": true,
  "pipelines_executed_by_health_check": false,
  "apply_executed_by_health_check": false,
  "rollback_executed_by_health_check": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 health summary 可以定时生成，不能触发任何上游任务。

## 状态枚举

`health_status` 只允许：

- `OK`
- `WATCH`
- `ACTION_REQUIRED`
- `CRITICAL`
- `INCOMPLETE`
- `ERROR`

pipeline 单项 `status` 只允许：

- `HEALTHY`
- `WATCH`
- `ACTION_REQUIRED`
- `CRITICAL`
- `MISSING`
- `STALE`
- `OPTIONAL_MISSING`
- `UNKNOWN`
- `ERROR`

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-023，本文记录目标、边界、阶段和验收。|
|2. 核心 builder|DONE|扫描 registry、定位 latest artifact、读取 status field、判断 missing/stale/status/safety。|
|3. JSON/Markdown/run log|DONE|写出 summary JSON、Markdown 和 run log，包含固定安全字段。|
|4. CLI|DONE|支持 `--date`、`--data-root`、`--lookback-days`、`--freshness-days`、`--fail-on-critical`、`--include-optional-pipelines`。|
|5. Dashboard|DONE|新增只读卡片，只读取 latest TRADING-023 artifact，不 import 或运行任何 pipeline。|
|6. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|7. 测试与 smoke|DONE|覆盖 required/optional、状态映射、新鲜度、安全字段、overall health、Markdown、dashboard 和 output invariants。|
|8. 验证收尾|DONE|最终 repo 外 smoke、dashboard 只读确认、目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 Black check 结果已记录；全仓 Black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## Dashboard 读取边界

Dashboard 只能读取
`data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-023 script，也不运行 018B-022、market、backtest、
scoring、broker、replay 或 trading execution。

## 后续接入

本任务先生成 TRADING-023 artifact。TRADING-022 可在后续小改中只读读取该 artifact，把 operator
brief 的 `pipeline_health` 从 `UNKNOWN` 升级为结构化状态。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求实现只读 Pipeline Health Summary，
  只扫描既有 artifacts，不运行任何 pipeline，不修改 production/shadow，不触发 broker/replay/trading。
- 2026-05-23：实现完成并进入 `VALIDATING`。新增 pipeline health summary builder、CLI、
  JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；
  当前已通过新增专项 pytest、dashboard pytest、触达文件 ruff 和 Black check，下一步执行扩大验证和
  repo 外 smoke。
- 2026-05-23：扩大验证完成，继续保持 `VALIDATING`。验证通过
  `python -m pytest tests/trading_engine/test_pipeline_health_summary.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、`python -m pytest tests/trading_engine -q`
  （417 passed / 1 warning）、全量 `python -m pytest -q`（1003 passed / 1 warning）和
  `python -m ruff check scripts src tests`。仓库外临时 fixture CLI smoke 验证 OK / INCOMPLETE /
  CRITICAL / STALE 四路径分别输出 `OK`、`INCOMPLETE`、`CRITICAL`、`ACTION_REQUIRED`，且均确认
  `pipelines_executed_by_health_check=false`、`broker_execution=false`、`replay_execution=false`、
  `trading_execution=false`。`python -m black --check scripts src tests` 仍仅被既有无关
  `tests/test_market_data.py` baseline 阻断；触达文件 Black check 已通过。
- 2026-05-23：最终收尾验证完成，从 `VALIDATING` 改为 `DONE`。再次使用 repo 外临时 fixture
  验证 OK / INCOMPLETE / CRITICAL / STALE 四路径；OK Markdown 包含 `Health Summary`、
  `Required Pipelines`、`Optional Pipelines`，CRITICAL Markdown 顶部包含
  `CRITICAL: Pipeline Health Issue Detected`，STALE required artifact 输出 `ACTION_REQUIRED`
  且 Markdown 顶部包含 `Action Required`；`include_optional_pipelines=true` 时 optional
  missing 输出 `WATCH`，不导致 `INCOMPLETE`。所有 smoke 输出和 run log 均确认固定安全边界：
  `production_effect=none`、`manual_review_only=true`、`pipeline_health_only=true`、
  `read_only=true`、`pipelines_executed_by_health_check=false`、
  `apply_executed_by_health_check=false`、`rollback_executed_by_health_check=false`、
  `broker_execution=false`、`replay_execution=false`、`trading_execution=false`。Dashboard import
  guard 确认 `Pipeline Health Summary` 卡片只读读取 TRADING-023 artifact，不触发 018B-022、
  TRADING-023 script、market/backtest/scoring/broker/replay/trading。随后重新执行
  `python -m pytest tests/trading_engine/test_pipeline_health_summary.py -q`（10 passed）、
  `python -m pytest tests/test_daily_task_dashboard.py -q`（10 passed）、
  `python -m pytest tests/trading_engine -q`（417 passed / 1 warning）、
  `python -m pytest -q`（1003 passed / 1 warning）和
  `python -m ruff check scripts src tests`（passed）。`python -m black --check scripts src tests`
  仍仅因既有无关 `tests/test_market_data.py` baseline 返回 would reformat，未混入无关格式化 diff。
