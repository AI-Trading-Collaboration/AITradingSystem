# Baseline Score Research Backfill Runbook

本 runbook 对应 `TRADING-045`。它用于补齐 SEC PIT observe-only shadow monitoring 需要的
historical baseline score 覆盖，不用于生成 production decision、prediction ledger、order
intent 或 active trading behavior。

## 运行命令

```bash
aits score-daily backfill-baseline \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --tickers NVDA MSFT AMD AVGO GOOG META AMZN \
  --output-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --mode research_backfill
```

未传 `--output-path` 时，默认会写入 `data/processed/research/` 下的 research-only CSV。命令会先运行
`validate_data_cache` 同一质量门禁，并写出本次 backfill 对应的数据质量报告。质量门禁失败时停止，不写
baseline rows。

## 输出字段

最低字段：

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

TRADING-045 也写入 `data_quality_status`、`data_quality_report_path`、`baseline_limitations` 和可用的
component score 字段；historical component 不可得时保留空值，并通过 completeness metadata 披露限制。

## 安全边界

- `research_backfill` 固定为 `true`。
- `production_effect` 固定为 `none`。
- 默认输出隔离在 `data/processed/research/`，不写 production `data/processed/scores_daily.csv`。
- 如果显式把 `--output-path` 指向 `data/processed/scores_daily.csv`，必须额外传
  `--overwrite-production-path`；若文件已存在，还必须同时传 `--overwrite`。
- 不修改 `config/scoring_rules.yaml`、production weight profile、approved overlay、decision snapshot、
  prediction ledger、order intent 或 active shadow state。
- 缺 historical input feature 时仍生成 row，但必须增加 missing count、降低 completeness ratio，并在
  limitation 字段中披露。

## 复核顺序

1. 先确认 backfill 命令输出的数据质量状态为 PASS。
2. 检查 research-only baseline CSV 是否包含 requested ticker/date 的 baseline rows。
3. 检查 `research_backfill=true` 和 `production_effect=none` 是否全量成立。
4. 检查 `score_completeness_ratio`、`missing_feature_count` 和 `baseline_limitations`，不要把低覆盖行解释成高可信 baseline。
5. 再运行 `aits sec-pit audit-baseline-coverage`，确认 shadow observe 窗口覆盖率。

## 验证命令

```bash
python -m pytest tests/trading_engine/test_baseline_score_backfill.py -q
python -m pytest tests/trading_engine/test_sec_pit_baseline_coverage.py -q
```
