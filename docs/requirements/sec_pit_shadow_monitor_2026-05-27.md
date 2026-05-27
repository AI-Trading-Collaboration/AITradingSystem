# TRADING-046: SEC PIT Shadow Observe Rolling Monitor

## 背景

TRADING-044/045 已经把 `capex_intensity` 放入隔离的 SEC PIT observe-only lane，并修正了
baseline coverage 误判：当前真实链路可以区分 factor failure、baseline 不足、label 不足、样本
观察期不足和数据质量限制。

下一步不新增因子、不改权重，而是把 observe-only lane 固化为可每天或每周重复运行的滚动监控报告。

## 目标

新增 `aits sec-pit shadow-monitor`，只读消费既有 SEC PIT shadow observe 和 baseline coverage
产物，为 `capex_intensity` 生成滚动监控摘要，回答：

1. 最近 20D / 60D rolling RankIC 是否仍支持当前负权重方向。
2. `semiconductor` bucket 是否继续优于 `platform` bucket。
3. observe-only score 相对 baseline 是否改善排序。
4. 是否触发 warning 或 rollback condition。
5. 观察期还剩多少天。
6. 证据是否足够从 `INSUFFICIENT_MONITORING_SAMPLE` 进入 `OK_MONITORING`。

本任务不得修改 production weights、active shadow weights、production scoring config、trading action、
prediction ledger、order intent 或 baseline backfill CSV。

## 输入

- `outputs/sec_pit_shadow_observe/sec_pit_shadow_scores_*.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_bucket_comparison_*.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_monitoring_plan_*.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_observe_summary_*.json`
- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_summary_*.json`
- `data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv`

## CLI

```bash
aits sec-pit shadow-monitor \
  --shadow-observe-dir outputs/sec_pit_shadow_observe \
  --baseline-coverage-dir outputs/sec_pit_baseline_coverage \
  --window-days 20 60 \
  --output-dir outputs/sec_pit_shadow_monitor
```

也支持默认 latest artifact discovery：

```bash
aits sec-pit shadow-monitor --latest
```

## 输出

- `outputs/sec_pit_shadow_monitor/sec_pit_shadow_monitor_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_shadow_monitor/sec_pit_shadow_monitor_summary_YYYY-MM-DD.md`
- `outputs/sec_pit_shadow_monitor/sec_pit_shadow_rolling_metrics_YYYY-MM-DD.csv`
- `outputs/sec_pit_shadow_monitor/sec_pit_shadow_warning_events_YYYY-MM-DD.csv`

所有输出固定 `production_effect=none`、`manual_review_required=true`。

## 状态枚举

- `INSUFFICIENT_MONITORING_SAMPLE`（TRADING-046A 后仅保留为历史兼容状态）
- `MONITORING_ACTIVE`
- `OK_MONITORING`
- `WARNING`
- `ROLLBACK_RECOMMENDED`
- `FAILED_VALIDATION`

## 规则

- baseline coverage gate 未通过时不得输出 `ROLLBACK_RECOMMENDED`。
- label/sample/观察期不足时不得输出 `ROLLBACK_RECOMMENDED`。
- TRADING-046A 后，coverage gate 已通过但 minimum evidence 未达标时输出 `MONITORING_ACTIVE`；
  coverage、minimum sample、observation days 达标且无 warning/rollback 时输出 `OK_MONITORING`，
  rolling metrics 暂不完整只阻断 rollback，不再被解释为 sample insufficiency。
- factor underperformance 必须被确认后才能输出 `ROLLBACK_RECOMMENDED`。
- 本任务的 pilot rollback confirmation baseline 要求至少一个 RankIC rollback breach 且至少一个 outcome
  metric rollback breach 同时出现；这是人工复核前的保守保护，不是 production promotion policy。
- dashboard 只读读取 monitor summary，不运行 monitor、不运行 shadow-observe、不运行 baseline coverage audit。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记和需求文档|DONE|任务表链接本文；本文记录输入、CLI、状态枚举、输出和安全边界。|
|2. 核心 rolling monitor|DONE|只读读取 existing artifacts，生成 summary JSON/Markdown、rolling metrics CSV 和 warning events CSV。|
|3. CLI|DONE|新增 `aits sec-pit shadow-monitor`，支持显式目录和 `--latest`。|
|4. Dashboard 和文档|DONE|daily task dashboard 新增只读 `SEC PIT Shadow Monitor` 卡片；更新 system flow、artifact catalog、learning path 和 runbook。|
|5. 测试与验证|DONE|新增专项测试覆盖 schema、Markdown、status、rollback gate、dashboard read-only、determinism 和 no production config writes。|

## 测试计划

- `tests/trading_engine/test_sec_pit_shadow_monitor.py`
- `tests/test_daily_task_dashboard.py`

覆盖：

- summary JSON schema；
- Markdown generation；
- rolling metrics CSV schema；
- warning events CSV schema；
- insufficient monitoring sample status；
- OK monitoring status；
- rollback only after coverage gates pass；
- dashboard read-only behavior；
- deterministic output；
- no production config writes。

## 进展记录

- 2026-05-27：新增并进入 `IN_PROGRESS`。原因：TRADING-045 后真实链路已从 baseline
  覆盖误判推进到 observe-only shadow lane 正常运行，下一步需要持续滚动监控，而不是继续扩展因子或改权重。
- 2026-05-27：从 `IN_PROGRESS` 改为 `DONE`。原因：已完成只读 rolling monitor、CLI、
  summary JSON/Markdown、rolling metrics CSV、warning events CSV、dashboard 只读卡片、runbook、
  artifact catalog、learning path、system flow 和专项测试；验证通过目标 SEC PIT monitor/dashboard/
  TRADING-044/045 回归、全量 pytest、ruff、触达 Python Black check 和 CLI help smoke。

## 验证记录

- `python -m pytest tests/trading_engine/test_sec_pit_shadow_monitor.py -q`：7 passed。
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed。
- `python -m pytest tests/trading_engine/test_sec_pit_shadow_observe.py -q`：12 passed。
- `python -m pytest tests/trading_engine/test_sec_pit_baseline_coverage.py -q`：3 passed。
- `python -m pytest tests/trading_engine/test_baseline_score_backfill.py -q`：4 passed。
- `python -m pytest -q`：1324 passed, 1 warning。
- `python -m ruff check src tests docs config scripts`：passed。
- `python -m black --check` for touched Python files：passed。
- `python -m ai_trading_system.cli sec-pit shadow-monitor --help`：passed。
