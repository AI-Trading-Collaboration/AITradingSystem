# SEC PIT Cognitive Evaluation Runbook

本 runbook 对应 `TRADING-040`，用于把 TRADING-039 SEC reconstructed PIT feature panel
评估为 observe-only shadow candidate evidence。

## 运行命令

```bash
aits sec-pit evaluate \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --universe config/sec_companies.yaml \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_evaluation
```

可选覆盖 universe：

```bash
aits sec-pit evaluate --start 2023-01-01 --end 2026-05-26 --tickers NVDA,MSFT,AMD,AVGO,GOOGL,META,AMZN
```

`GOOGL` 会通过 `config/ticker_aliases.yaml` 解析为 SEC canonical ticker `GOOG`，前提是
`config/sec_companies.yaml` 中存在 `GOOG`。

## 必须检查

- `outputs/sec_pit_evaluation/sec_pit_evaluation_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_evaluation/sec_pit_feature_effectiveness_YYYY-MM-DD.csv`
- `outputs/sec_pit_evaluation/sec_pit_signal_attribution_YYYY-MM-DD.csv`
- `outputs/sec_pit_evaluation/sec_pit_shadow_candidate_weights_YYYY-MM-DD.csv`
- 同目录 data quality report

## 安全边界

- 主评估只允许 `available_time <= decision_time` 的行。
- 缺少 `available_time` 的行排除，不得进入 IC 或 attribution。
- `period_end <= decision_time` 不可作为可见性条件。
- `pit_grade_policy` 固定为 `B_RECONSTRUCTED_SEC_FILING_PIT`。
- 缺少 `accession_number`、`accepted_datetime`、`filed_date` 或 `raw_sha256` 会降低
  `pit_quality_score`，不得 promoted。
- `signal_attribution` 必须保留 SEC provenance 和 `source_lineage`，并输出
  `max_drawdown_forward_20d` 作为 evaluation label，不得作为 feature 使用；该 drawdown
  label 必须为 `<= 0`，单调上涨窗口记为 `0.0`。
- 所有 shadow weights 固定 `manual_review_required=true`、`production_effect=none`。

## 解释顺序

1. 先看 summary JSON/Markdown 的 data quality status 和 PIT exclusion counts。
2. 再看 `feature_effectiveness` 的 coverage、RankIC、stability 和 recommendation。
3. 用 `signal_attribution` 复核 ticker/date 层的 normalized contribution。
4. 只把 `shadow_candidate_weights` 当作人工 review 输入，不得写回 production weights。
5. 若要判断这些 feature 是否真的改善 decision-level 排名、回撤规避或 action review
   queue，继续运行 `docs/runbooks/sec_pit_baseline_comparison.md` 中的
   `aits sec-pit compare-baseline`。
6. 若真实 run 出现 provenance 缺失、coverage ratio 大于 1、drawdown label NaN 或 baseline
   artifact fallback，运行 `docs/runbooks/sec_pit_real_run_diagnostics.md`。

## Dashboard

Daily task dashboard 只读读取 latest `sec_pit_evaluation_summary_*.json`，显示 latest evaluation
date、universe size、feature count、recommendation counts、top 5 features 和 PIT safety status。
Dashboard 不运行 evaluation、不重新读取 market data、不修改任何权重。
