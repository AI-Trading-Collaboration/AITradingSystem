# SEC PIT Baseline Coverage Audit Runbook

本 runbook 对应 `TRADING-045`。它用于只读审计 historical baseline score 是否覆盖 SEC PIT
observe-only shadow monitoring 的 ticker/date 窗口。该命令不运行 backfill，不运行
shadow-observe，不修改 baseline score CSV。

## 运行命令

```bash
aits sec-pit audit-baseline-coverage \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --baseline-score-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --feature-panel data/processed/sec_pit/sec_pit_feature_panel.csv \
  --output-dir outputs/sec_pit_baseline_coverage
```

若当前项目使用 reconstructed SEC EDGAR 路径，也可以传入：

```bash
--feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv
```

## 输出

- `sec_pit_baseline_coverage_summary_YYYY-MM-DD.json`
- `sec_pit_baseline_coverage_summary_YYYY-MM-DD.md`
- `sec_pit_baseline_coverage_by_ticker_YYYY-MM-DD.csv`
- `sec_pit_baseline_coverage_by_date_YYYY-MM-DD.csv`
- `sec_pit_baseline_gap_YYYY-MM-DD.csv`

summary 状态只允许：

- `OK`
- `LIMITED_COVERAGE`
- `INSUFFICIENT_COVERAGE`
- `MISSING_BASELINE`
- `FAILED_VALIDATION`

gap type 只允许：

- `MISSING_SCORE_ROW`
- `MISSING_BASELINE_SCORE`
- `MISSING_BASELINE_RANK`
- `MISSING_ACTION`
- `LOW_COMPLETENESS`

## 解释顺序

1. 先看 summary JSON 的 `coverage_status`、`coverage_ratio`、`missing_rows` 和
   `score_completeness_avg`。
2. 用 by-ticker CSV 找长期缺口 ticker；重点看 `first_missing_date`、`last_missing_date` 和
   `recommended_action`。
3. 用 by-date CSV 找单日横截面缺口；`missing_tickers` 可直接用于补跑 ticker 参数。
4. 用 gap CSV 区分整行缺失、baseline score/rank/action 缺失和 low completeness。
5. 覆盖不足只能解释为 data limitation，不应写成 factor underperformance。

## Dashboard

Daily task dashboard 只读读取 latest
`outputs/sec_pit_baseline_coverage/sec_pit_baseline_coverage_summary_YYYY-MM-DD.json`，展示
baseline coverage ratio/status、expected/actual/missing rows 和 score completeness。Dashboard 不运行
audit 或 backfill。

## 验证命令

```bash
python -m pytest tests/trading_engine/test_sec_pit_baseline_coverage.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
```
