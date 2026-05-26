# SEC PIT Baseline Comparison Runbook

本 runbook 对应 `TRADING-041`，用于把 TRADING-040 SEC PIT evaluation artifact
与现有 baseline score artifact 做 decision-level 只读对比。

## 运行命令

```bash
aits sec-pit compare-baseline \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --sec-pit-evaluation-dir outputs/sec_pit_evaluation \
  --baseline-score-dir outputs/daily_score \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_baseline_comparison
```

如果 baseline score CSV 不在默认目录，可直接把 `--baseline-score-dir` 指向 CSV：

```bash
aits sec-pit compare-baseline \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --baseline-score-dir data/processed/scores_daily.csv
```

## 必须检查

- `sec_pit_baseline_comparison_summary_YYYY-MM-DD.json`
- `sec_pit_baseline_comparison_summary_YYYY-MM-DD.md`
- `sec_pit_decision_impact_YYYY-MM-DD.csv`
- `sec_pit_rank_shift_YYYY-MM-DD.csv`
- `sec_pit_incremental_alpha_YYYY-MM-DD.csv`

## 状态解释

- `OK`：baseline 与 SEC PIT evaluation 存在 ticker/date overlap，可生成完整比较。
- `LIMITED_BASELINE_MISSING`：缺 baseline artifact；默认降级生成空 CSV 和限制说明。
- `LIMITED_SEC_PIT_EVALUATION_MISSING`：缺 TRADING-040 artifact；默认降级生成限制说明。
- `INSUFFICIENT_OVERLAP`：两侧 artifact 存在但无共同 ticker/date。
- `FAILED_VALIDATION`：输入 schema 或解析失败。

需要 fail closed 的调度场景使用 `--strict`。

## 安全边界

- 本命令只读 artifact，不创建市场数据管线。
- 不修改 production weights、approved overlay、score-daily 输出或 production action。
- `decision_impact` 每行固定 `manual_review_required=true`、`production_effect=none`。
- `available_time > decision_date` 的 attribution row 必须排除。
- 报告继续披露 `B_RECONSTRUCTED_SEC_FILING_PIT` 限制；不能把 reconstructed PIT 当作
  strict vendor archive PIT。

## Dashboard

Daily task dashboard 只读读取 latest `sec_pit_baseline_comparison_summary_*.json`，展示
latest comparison date、comparison status、decision count、action changed count、material
rank shift count、incremental alpha 20d、drawdown improvement 20d、top promoted tickers 和
top downgraded tickers。Dashboard 不运行 comparison、不重新读取 market data、不修改权重或
action。
