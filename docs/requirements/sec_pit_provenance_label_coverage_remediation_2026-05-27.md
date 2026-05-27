# TRADING-042A: SEC PIT Provenance and Label Coverage Remediation

## 背景

TRADING-042 已新增 `aits sec-pit diagnose-run`，真实 `--latest` 诊断显示：

- `first_loss_stage=xbrl_facts_long`;
- feature panel missing provenance rows 为 51,972；
- baseline artifact 使用 `data/processed/scores_daily.csv` fallback；
- `max_drawdown_forward_20d` coverage 为 0；
- 修正前 coverage ratio 大于 1 的 feature 为 4 个；
- near-promotion feature 为 3 个，但只允许人工复核，不能自动 promotion。

这些结果说明当前 SEC PIT 链路仍需继续修复 provenance、label coverage 和重复观测源头。不得把本任务解释为 TRADING-043 shadow promotion review。

## 目标

修复 TRADING-042 诊断暴露的数据覆盖和可审计性根因，并重新运行真实 SEC PIT backfill / evaluation / baseline comparison / diagnostics，确认剩余问题被消除或被明确登记为数据源限制。

## 阶段拆解

|阶段|优先级|内容|验收标准|
|---|---|---|---|
|1|P0|复核 `xbrl_facts_long` accession timeline join 和 fallback 逻辑|能解释 B-grade / C-grade 来源，尽可能保留 accession、accepted datetime、filed date、raw sha、source concept、pit grade；无法还原的历史事实必须明确标记为数据源限制而不是静默降级。|
|2|P0|复跑 regenerated `mapped_metrics_long -> intervals -> feature_panel`|新产物保留 TRADING-042 新增 provenance 字段和 `source_lineage`；derived feature 不丢失组成事实 lineage。|
|3|P1|复跑 evaluation / baseline comparison|`signal_attribution` 和 `decision_impact` 保留 provenance；`max_drawdown_forward_20d`、`relative_return_vs_QQQ_20d`、`forward_return_20d` label 覆盖可审计。|
|4|P1|修复遇到的真实运行问题|包括 schema mismatch、alias join、baseline resolver、duplicate observation、coverage ratio、label builder 或 Markdown/JSON summary 中发现的问题；不得绕过 blocker。|
|5|P2|更新诊断、文档和任务状态|`diagnose-run --latest` 产物反映修复后状态；若仍不足以进入 TRADING-043，报告明确首要剩余 blocker。|

## 安全边界

- 本任务不得修改 production weights、approved profile、shadow weights 或 actions。
- 不得自动把 SEC PIT feature 推到 `PROMOTE_TO_SHADOW` 或实盘使用。
- forward / relative / drawdown labels 只能作为 evaluation labels，不得进入 feature panel 或 scoring features。
- 任何数据源无法补齐的 provenance 缺口必须在诊断和任务记录中披露。

## 验证命令

至少运行：

```bash
aits sec-pit build-panel --start 2023-01-01 --end 2026-05-26
aits sec-pit evaluate --start 2023-01-01 --end 2026-05-26 --tickers NVDA,MSFT,AMD,AVGO,GOOGL,META,AMZN
aits sec-pit compare-baseline --start 2023-01-01 --end 2026-05-26 --tickers NVDA,MSFT,AMD,AVGO,GOOGL,META,AMZN --baseline-score-path data/processed/scores_daily.csv
aits sec-pit diagnose-run --latest
python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q
python -m pytest tests/trading_engine/test_sec_pit_evaluation.py -q
python -m pytest tests/trading_engine/test_sec_pit_baseline_comparison.py -q
python -m pytest -q
python -m ruff check config src tests scripts docs
python -m black --check <touched-python-files>
```

## 进展记录

- 2026-05-27：新增并进入 `IN_PROGRESS`，原因：owner 要求继续修复 TRADING-042 诊断暴露的问题，且过程中遇到的问题都要一并修复。本任务聚焦数据质量和覆盖，不进入 shadow promotion review。
- 2026-05-27：改为 `DONE`。已修复 feature panel 源头重复观测、baseline comparison `source_lineage` JSON 汇总、forward max drawdown label 正值问题、diagnostics 对尾部 20D label 的解释和 stale recommendation 文案；真实复跑后 diagnostics 为 `OK`，downstream missing provenance 0，coverage ratio > 1 feature 0，duplicate observations 0，drawdown label coverage 0.9444，forward/relative label 缺失只发生在缺少未来 20D 价格的尾部日期。`capex_intensity` 当前只进入 manual-review shadow candidate 输出，未修改 production/shadow weights 或 actions。

## 收尾验证

- `aits sec-pit build-metrics --to 2026-05-26`：生成新 `mapped_metrics_long.csv`
- `aits sec-pit build-panel --from 2023-01-01 --to 2026-05-26`：生成无重复观测的 `sec_pit_feature_panel.csv`
- `aits sec-pit evaluate --start 2023-01-01 --end 2026-05-26 --tickers NVDA --tickers MSFT --tickers AMD --tickers AVGO --tickers GOOGL --tickers META --tickers AMZN`：PASS
- `aits sec-pit compare-baseline --start 2023-01-01 --end 2026-05-26 --tickers NVDA --tickers MSFT --tickers AMD --tickers AVGO --tickers GOOGL --tickers META --tickers AMZN --baseline-score-path data/processed/scores_daily.csv`：OK
- `aits sec-pit diagnose-run --latest`：OK
- `python -m pytest tests/test_sec_pit_backfill.py tests/trading_engine/test_sec_pit_evaluation.py tests/trading_engine/test_sec_pit_baseline_comparison.py tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q`：37 passed
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed
- `python -m pytest -q`：1288 passed, 1 warning
- `python -m ruff check config src tests scripts docs`：passed
- `python -m black --check <touched-python-files>`：passed
