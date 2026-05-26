# SEC PIT Backfill Runbook

本 runbook 对应 `TRADING-039`，说明 SEC EDGAR reconstructed filing-time PIT backfill 的日常入口。

## Backfill

```bash
aits sec-pit backfill \
  --from 2022-12-01 \
  --to 2026-05-26 \
  --sec-companies-path config/sec_companies.yaml \
  --processed-dir data/processed/sec_edgar \
  --report-dir outputs/reports/sec_pit_backfill
```

## 关键产物

- `data/raw/sec_edgar/manifest/sec_edgar_raw_manifest.csv`
- `data/processed/sec_edgar/filing_timeline.csv`
- `data/processed/sec_edgar/xbrl_facts_long.csv`
- `data/processed/sec_edgar/mapped_metrics_long.csv`
- `data/processed/sec_edgar/fundamental_pit_intervals.csv`
- `data/processed/sec_edgar/fundamental_pit_daily_panel.csv`
- `data/processed/sec_edgar/sec_pit_feature_panel.csv`
- `outputs/reports/sec_pit_backfill/sec_pit_validation_YYYY-MM-DD.json`

## 下游 TRADING-040

Backfill 完成并通过 validation 后，运行：

```bash
aits sec-pit evaluate \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --universe config/sec_companies.yaml \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_evaluation
```

TRADING-040 不修改 backfill 产物；它只读取 feature panel、价格/利率缓存和 policy config，
生成 observe-only evaluation artifacts。
