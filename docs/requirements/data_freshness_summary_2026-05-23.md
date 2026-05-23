# TRADING-024: Data Freshness Summary

关联任务：`TRADING-024`

当前状态：DONE

最后更新：2026-05-24

## 背景

TRADING-022 已能生成 Daily Trading System Operator Brief，TRADING-023 已补齐
Pipeline Health Summary。当前剩余缺口是 operator brief 中的 `data_freshness.status`
仍可能为 `UNKNOWN`。

TRADING-024 补齐一个独立、只读的数据新鲜度 artifact：扫描已有数据文件、报告、artifact、
日期戳和 metadata，判断关键数据源是否存在、是否足够新鲜、哪些缺失或过期，以及是否需要人工检查。

## 目标

- 新增 `scripts/run_data_freshness_summary.py`。
- 新增核心模块 `src/ai_trading_system/trading_engine/data_freshness_summary.py`。
- 内置首版 data source registry，覆盖 TRADING-021/022/023、TRADING-019、shadow weights、
  018C2 review、018F lifecycle audit、market/backtest/price/news 或 signal cache 等来源。
- 输出：
  - `data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.json`
  - `data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.md`
  - `data/derived/data_freshness/logs/data_freshness_summary_run_YYYY-MM-DD.json`
  - `data/derived/data_freshness/logs/data_freshness_summary_run_YYYY-MM-DD.md`
- Daily task dashboard 新增只读 `Data Freshness Summary` 卡片，只读取 TRADING-024 artifact。
- 新增 runbook、system flow、artifact catalog 和专项测试。

## 安全边界

TRADING-024 必须严格只读：

- 不下载数据，不刷新数据，不调用外部 API。
- 不运行 market、backtest、scoring、TRADING-018B 到 TRADING-023 或任何上游 pipeline。
- 不修改 production profile、shadow weights、approved profile 或任何上游 artifact。
- 不执行 apply、rollback、promotion、broker、replay 或 trading execution。
- Dashboard 只能读取 TRADING-024 artifact，不运行 TRADING-024 script。

所有 TRADING-024 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "data_freshness_only": true,
  "read_only": true,
  "data_downloaded_by_freshness_check": false,
  "pipelines_executed_by_freshness_check": false,
  "apply_executed_by_freshness_check": false,
  "rollback_executed_by_freshness_check": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 freshness summary 可以定时生成，不能触发任何上游任务。

## 状态枚举

整体 `freshness_status` 只允许：

- `OK`
- `WATCH`
- `STALE`
- `MISSING`
- `CRITICAL`
- `ERROR`

source 单项 `status` 只允许：

- `FRESH`
- `WATCH`
- `STALE`
- `MISSING`
- `OPTIONAL_MISSING`
- `CRITICAL`
- `UNKNOWN`
- `ERROR`

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-024，本文记录目标、边界、阶段和验收。|
|2. 核心 builder|DONE|扫描 registry、定位 latest artifact、读取日期字段/status field、判断 missing/stale/status/safety。|
|3. JSON/Markdown/run log|DONE|写出 summary JSON、Markdown 和 run log，包含固定安全字段和 safety validation。|
|4. CLI|DONE|支持 `--date`、`--data-root`、`--lookback-days`、`--freshness-days`、`--market-date`、`--fail-on-critical`、`--include-optional-sources`。|
|5. Dashboard|DONE|新增只读卡片，只读取 latest TRADING-024 artifact，不 import 或运行任何 pipeline。|
|6. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|7. 测试与 smoke|DONE|覆盖 required/optional、状态映射、日期抽取、新鲜度、安全字段、overall status、Markdown、dashboard 和 output invariants。|
|8. 验证收尾|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 repo 外 OK/MISSING/CRITICAL/STALE smoke 已通过；全仓 Black check 仅被既有无关 baseline 阻断。|

## Dashboard 读取边界

Dashboard 只能读取
`data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-024 script，也不运行 TRADING-018B 到
TRADING-023、market、backtest、scoring、data download、broker、replay 或 trading execution。

## 后续接入

本任务先生成 TRADING-024 artifact。TRADING-022 可在后续 TRADING-025 中只读读取
TRADING-023 和 TRADING-024 artifacts，把 operator brief 的 `pipeline_health` 和
`data_freshness` 从 `UNKNOWN` 升级为结构化状态。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求实现只读 Data Freshness Summary，
  只扫描既有数据和 artifacts，不下载数据、不运行 pipeline、不修改 production/shadow、
  不触发 broker/replay/trading。
- 2026-05-23：实现完成并进入 `VALIDATING`。新增 data freshness summary builder、CLI、
  JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；
  验证通过 `python -m pytest tests/trading_engine/test_data_freshness_summary.py -q`（12 passed）、
  `python -m pytest tests/test_daily_task_dashboard.py -q`（11 passed）、
  `python -m pytest tests/trading_engine -q`（429 passed / 1 warning）、
  `python -m pytest -q`（1016 passed / 1 warning）和
  `python -m ruff check scripts src tests`。repo 外临时 fixture smoke 验证 OK / MISSING /
  CRITICAL / STALE 四路径分别输出 `OK`、`MISSING`、`CRITICAL`、`STALE`，且均确认
  `data_downloaded_by_freshness_check=false`、`pipelines_executed_by_freshness_check=false`。
  `python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py`
  baseline 阻断；本次触达文件 Black check 已通过。
- 2026-05-24：最终收尾验证完成，从 `VALIDATING` 改为 `DONE`。再次使用 repo 外临时
  fixture 验证 OK / MISSING / CRITICAL / STALE / optional-missing 五路径；OK Markdown 包含
  `Freshness Summary`、`Required Sources`、`Optional Sources`；MISSING Markdown 顶部包含
  `Required Data Missing`；CRITICAL Markdown 顶部包含
  `CRITICAL: Data Freshness Issue Detected`；STALE Markdown 顶部包含
  `Stale Required Data Detected`；optional missing 输出 `WATCH` 而不是 `MISSING`。所有 smoke
  输出均确认固定安全边界：`production_effect=none`、`manual_review_only=true`、
  `data_freshness_only=true`、`read_only=true`、
  `data_downloaded_by_freshness_check=false`、
  `pipelines_executed_by_freshness_check=false`、
  `apply_executed_by_freshness_check=false`、
  `rollback_executed_by_freshness_check=false`、`broker_execution=false`、
  `replay_execution=false`、`trading_execution=false`。Dashboard import guard 确认
  Data Freshness Summary 卡片只读读取 TRADING-024 artifact，不触发 018B-023、TRADING-024
  script、market/backtest/scoring/data download/broker/replay/trading。随后重新执行目标 pytest、
  dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 Black check；全仓 Black check
  仍只被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
