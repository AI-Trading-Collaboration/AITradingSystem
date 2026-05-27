# TRADING-042: SEC PIT Real Run Diagnostics & Data Coverage Improvement

## 背景

TRADING-039 已建立 SEC EDGAR reconstructed PIT backfill，TRADING-040 已建立 SEC PIT cognitive evaluation，TRADING-041 已建立 SEC PIT baseline comparison。2026-05-26 小规模 AI / semiconductor universe 真实端到端运行完成，但结果不具备 shadow promotion 条件：没有 `PROMOTE_TO_SHADOW` feature，shadow candidate weights 全为 0，`incremental_alpha_20d=0.0`，promotion / downgrade bucket 和 `action_changed` 均为空，约 51,972 行缺少 SEC provenance，`data_quality_score` 仅约 0.46-0.48，`coverage_ratio` 出现大于 1，drawdown label 在 comparison 输出中为 NaN，并且 `GOOGL` 因 SEC company config 仅有 `GOOG` 被拒绝。

## 目标

新增 `aits sec-pit diagnose-run`，对真实 SEC PIT 运行生成只读诊断层，定位 provenance 丢失阶段、alias 解析、baseline artifact 选择、forward label 覆盖、coverage ratio 大于 1 的原因，以及若 provenance quality 修复后哪些 feature 接近 promotion。

本任务不得自动把任何 SEC PIT feature 晋级到 production 或 shadow weights。所有候选敏感性输出必须固定 `manual_review_required=true`、`production_effect=none`。

## 阶段拆解

|阶段|优先级|内容|验收标准|
|---|---|---|---|
|1|P0|Provenance gap audit 与传播修复|`mapped_metrics_long -> intervals -> feature_panel -> signal_attribution -> baseline_comparison` 保留 accession、accepted datetime、filed date、form、period、source concept/taxonomy、raw sha/path、PIT grade、available time 和 `source_lineage`；诊断报告能指出首个明显丢失阶段。|
|2|P0|GOOG / GOOGL alias|新增 `config/ticker_aliases.yaml`；SEC PIT 命令标准化用户 ticker；报告同时显示 input / canonical ticker。|
|3|P1|Baseline artifact resolver|`compare-baseline` 和 diagnostics 支持显式 path、显式 dir、默认 `outputs/daily_score`、`data/processed/scores_daily.csv` fallback 和 degraded status，并在 summary 中披露实际 artifact。|
|4|P1|Coverage ratio 和 label coverage|coverage ratio 以 `decision_date x ticker` 去重口径修正到不超过 1；重复观测可审计；evaluation attribution 输出 drawdown label；diagnostics 输出 label coverage audit。|
|5|P2|Candidate sensitivity 与 dashboard|输出 near-promotion sensitivity CSV/summary；daily task dashboard 只读读取 latest diagnostics JSON，不运行 diagnostics。|

## 输出产物

- `outputs/sec_pit_diagnostics/sec_pit_real_run_diagnostics_YYYY-MM-DD.json`
- `outputs/sec_pit_diagnostics/sec_pit_real_run_diagnostics_YYYY-MM-DD.md`
- `outputs/sec_pit_diagnostics/sec_pit_provenance_gap_YYYY-MM-DD.csv`
- `outputs/sec_pit_diagnostics/sec_pit_coverage_audit_YYYY-MM-DD.csv`
- `outputs/sec_pit_diagnostics/sec_pit_alias_resolution_audit_YYYY-MM-DD.csv`
- `outputs/sec_pit_diagnostics/sec_pit_label_coverage_audit_YYYY-MM-DD.csv`
- `outputs/sec_pit_diagnostics/sec_pit_candidate_sensitivity_YYYY-MM-DD.csv`

## 安全边界

- 诊断命令只读读取已有 artifact 和本地 CSV，不下载数据、不运行 evaluation/comparison、不修改 score-daily、production weights、approved overlay、shadow weights 或 actions。
- Candidate sensitivity 只表达 hypothetically blocked-by-data-quality，不输出可自动执行的 promotion。
- Dashboard 卡片只读取 latest diagnostics JSON；缺 artifact 时显示 `MISSING`。

## 验证命令

目标验证：

```bash
python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine/test_sec_pit_evaluation.py -q
python -m pytest tests/trading_engine/test_sec_pit_baseline_comparison.py -q
python -m ruff check config src tests scripts docs
python -m black --check <touched-python-files>
```

若时间允许，再运行全量：

```bash
python -m pytest -q
```

全仓 Black 仍可能被既有 `tests/test_market_data.py` 格式基线阻断；本任务不修 unrelated formatting baseline。

## 进展记录

- 2026-05-27：新增并进入 `IN_PROGRESS`，原因：真实 SEC PIT 端到端运行暴露 provenance、alias、baseline artifact、label coverage 和 coverage ratio 诊断缺口；开始按 P0/P1/P2 顺序实现。
- 2026-05-27：改为 `DONE`。已完成诊断 CLI、artifact 输出、provenance propagation、alias、baseline resolver、coverage/label audit、candidate sensitivity、dashboard 只读卡片和文档更新。真实 `--latest` 产物显示 `first_loss_stage=xbrl_facts_long`、feature panel missing provenance 51,972 行、baseline fallback 使用 `data/processed/scores_daily.csv`、`max_drawdown_forward_20d` coverage 为 0、修正前 coverage ratio > 1 的 feature 4 个、near-promotion feature 3 个；仍不得进入 shadow promotion，应先修复 SEC provenance 和 label coverage 后再评估 TRADING-043。

## 收尾验证

- `python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q`：10 passed
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed
- `python -m pytest tests/trading_engine/test_sec_pit_evaluation.py -q`：11 passed
- `python -m pytest tests/trading_engine/test_sec_pit_baseline_comparison.py -q`：7 passed
- `python -m pytest -q`：1286 passed, 1 warning
- `python -m ruff check config src tests scripts docs`：passed
- `python -m black --check <touched-python-files>`：passed
