# TRADING-045: Historical Baseline Score Backfill and Shadow Monitoring Coverage

最后更新：2026-05-27

## 背景

TRADING-044 的 SEC PIT observe-only shadow lane 已能为 `capex_intensity` 生成隔离观察产物，且真实运行没有修改 production scoring、production weights、active shadow weights 或交易行为。

真实运行结果显示：

- `candidate_feature=capex_intensity`
- `observe_weight=-0.025`
- `production_effect=none`
- `score_rows=5964`
- `rank_shift_rows=126`
- `bucket_comparison_rows=3`
- safety checks 为 14 passed / 0 warning / 0 failed
- `shadow_status=LIMITED_BASELINE_MISSING`
- `monitoring_status=ROLLBACK_TRIGGERED`

该 rollback 状态由 `data/processed/scores_daily.csv` historical baseline score 覆盖不足触发，不是已确认的 factor failure。当前状态会把数据覆盖问题和真实因子表现问题混在一起，影响 observe-only 监控解释。

## 目标

新增 historical baseline score research backfill、baseline coverage audit 和 shadow observe monitoring quality gate，使 `aits sec-pit shadow-observe` 在历史窗口内能区分：

1. 真实因子表现恶化；
2. baseline score 覆盖不足；
3. forward label 覆盖不足；
4. monitoring 样本数不足。

本任务不得修改 production decisions、active trading behavior、production weights、active shadow weights 或 production scoring config。

## 设计边界

- Baseline backfill 只写 research-only 行，固定 `research_backfill=true`、`production_effect=none`。
- 默认不得覆盖已有 output path；只有显式 `--overwrite` 才允许写入已存在的目标文件。
- Backfill 使用现有 score-daily 模块评分逻辑生成 market-wide baseline score，再按 requested ticker 写覆盖行；ticker price availability 和 score signal coverage 进入 completeness metadata。
- Backfill row 是 shadow monitoring 的 historical baseline artifact，不是 live production score，不进入 prediction ledger，不写 decision snapshot，不触发 order intent。
- Coverage audit 只读读取 baseline score CSV 和 SEC PIT feature panel，不运行 backfill、不运行 shadow-observe。
- Shadow observe 只有 baseline coverage、label coverage 和 monitoring sample count 都通过配置门槛时，才允许把 factor performance breach 标记为 factor rollback。
- Dashboard 只读读取 existing shadow observe summary 和 baseline coverage summary artifact，不触发任何 pipeline。

## 新 CLI

```bash
aits score-daily backfill-baseline \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --tickers NVDA MSFT AMD AVGO GOOG META AMZN \
  --output-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --mode research_backfill
```

```bash
aits sec-pit audit-baseline-coverage \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --baseline-score-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --feature-panel data/processed/sec_pit/sec_pit_feature_panel.csv \
  --output-dir outputs/sec_pit_baseline_coverage
```

## 输出产物

Baseline score CSV minimum schema:

- `decision_date`
- `ticker`
- `baseline_score`
- `baseline_rank`
- `baseline_action`
- `score_source`
- `score_version`
- `input_feature_count`
- `available_feature_count`
- `missing_feature_count`
- `score_completeness_ratio`
- `research_backfill`
- `production_effect`
- `generated_at`

Coverage audit artifacts:

- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_summary_YYYY-MM-DD.md`
- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_by_ticker_YYYY-MM-DD.csv`
- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_by_date_YYYY-MM-DD.csv`
- `outputs/sec_pit_baseline_coverage/sec_pit_baseline_gap_YYYY-MM-DD.csv`

Allowed coverage statuses:

- `OK`
- `LIMITED_COVERAGE`
- `INSUFFICIENT_COVERAGE`
- `MISSING_BASELINE`
- `FAILED_VALIDATION`

Allowed shadow monitoring statuses after this task:

- `OK`
- `LIMITED_BASELINE_MISSING`
- `LIMITED_LABELS_MISSING`
- `INSUFFICIENT_MONITORING_SAMPLE`
- `ROLLBACK_TRIGGERED_BY_FACTOR`
- `ROLLBACK_TRIGGERED_BY_DATA`
- `FAILED_SAFETY_CHECK`

## 阶段拆解

|阶段|状态|内容|验收标准|
|---|---|---|---|
|1. 任务登记和需求文档|DONE|登记 TRADING-045，固定不改 production 的边界和产物 schema。|任务表链接本文；本文记录 CLI、产物、状态枚举、测试和验证命令。|
|2. Baseline backfill|DONE|新增 research-only historical baseline writer 和 `aits score-daily backfill-baseline`。|输出 schema 完整；默认不覆盖已有文件；每行带 completeness metadata；`production_effect=none`。|
|3. Baseline coverage audit|DONE|新增只读 audit builder 和 `aits sec-pit audit-baseline-coverage`。|生成 summary JSON/Markdown、by ticker、by date、gap CSV；schema 与状态枚举稳定。|
|4. Shadow observe monitoring quality gate|DONE|扩展 config 和 monitoring logic。|数据覆盖不足时输出 limited/data rollback 状态，不再误报 factor rollback；coverage gates 通过后才允许 factor rollback。|
|5. Dashboard 和文档|DONE|更新只读 dashboard、artifact catalog、system flow、learning path、runbook。|Dashboard 展示 baseline coverage ratio/status、monitoring reason、factor/data trigger，且只读。|
|6. 验证|DONE|新增/更新专项测试并运行目标验证。|目标 pytest、dashboard pytest、全量 pytest、ruff 和 touched Python black check 通过。|

## 测试计划

- `tests/trading_engine/test_baseline_score_backfill.py`
- `tests/trading_engine/test_sec_pit_baseline_coverage.py`
- `tests/trading_engine/test_sec_pit_shadow_observe.py`
- `tests/test_daily_task_dashboard.py`

覆盖：

- baseline backfill schema；
- research backfill 默认不覆盖 existing production output；
- baseline coverage summary / by ticker / by date / gap schema；
- missing baseline emits `LIMITED_BASELINE_MISSING`；
- incomplete baseline 不触发 factor rollback；
- coverage gates 通过后才允许 factor rollback；
- shadow observe 继续固定 `production_effect=none`；
- dashboard 只读读取 baseline coverage artifacts；
- deterministic output。

## 验证命令

```bash
python -m pytest tests/trading_engine/test_baseline_score_backfill.py -q
python -m pytest tests/trading_engine/test_sec_pit_baseline_coverage.py -q
python -m pytest tests/trading_engine/test_sec_pit_shadow_observe.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest -q
python -m ruff check config src tests scripts docs
python -m black --check <touched-python-files>
```

全仓 Black check 可能仍被既有无关 `tests/test_market_data.py` formatting baseline 阻断；本任务不修复无关 baseline。

## 进展记录

- 2026-05-27：新增任务并进入实现。原因：TRADING-044 真实 observe-only run 暴露 baseline coverage 不足导致 `monitoring_status=ROLLBACK_TRIGGERED`，需要把数据限制与真实 factor rollback 分离。
- 2026-05-27：实现阶段完成，进入验证。已新增 baseline backfill、baseline coverage audit、shadow observe monitoring quality gate、dashboard 只读卡片、runbook、artifact catalog、learning path 和 system flow 更新；仍需完成全量 pytest、ruff 和 touched Python Black check。
- 2026-05-27：验证完成并归档 DONE。验证通过 `tests/trading_engine/test_baseline_score_backfill.py`、`tests/trading_engine/test_sec_pit_baseline_coverage.py`、`tests/trading_engine/test_sec_pit_shadow_observe.py`、`tests/test_daily_task_dashboard.py`、全量 `python -m pytest -q`（1315 passed, 1 warning）、全量 `python -m ruff check config src tests scripts docs`、触达 Python 文件 `python -m black --check ...`，并完成两个新增 CLI 的 help smoke。
- 2026-05-27：TRADING-045A path safety 后续修正更新本文示例。`backfill-baseline` 默认和 runbook 示例改为 `data/processed/research/` research-only 路径；若显式写 `data/processed/scores_daily.csv` 需要额外 `--overwrite-production-path`。
- 2026-05-27：TRADING-045B shadow status 口径修正。真实链路显示 baseline coverage audit 为 `OK`，但顶层 `shadow_status` 仍因少量 tail baseline gap 输出 `LIMITED_BASELINE_MISSING`；已改为只有 coverage gate 未通过时才降级，coverage gate 通过的局部缺口只写入 limitations。
