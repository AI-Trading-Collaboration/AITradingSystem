# SEC PIT Shadow Observe Rolling Monitor Runbook

本 runbook 对应 `TRADING-046` / `TRADING-046A`，用于对 `capex_intensity` 的 SEC PIT
observe-only lane 生成每日或每周滚动监控报告。

本命令只读既有 TRADING-044/045 artifacts，不新增因子、不调整权重、不写 production scoring
config、不写 active shadow state，也不触发交易。

## 运行命令

```bash
aits sec-pit shadow-monitor \
  --shadow-observe-dir outputs/sec_pit_shadow_observe \
  --baseline-coverage-dir outputs/sec_pit_baseline_coverage \
  --baseline-score-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --window-days 20 60 \
  --output-dir outputs/sec_pit_shadow_monitor
```

也可自动发现 latest artifacts：

```bash
aits sec-pit shadow-monitor --latest
```

## 输入

- `sec_pit_shadow_observe_summary_YYYY-MM-DD.json`
- `sec_pit_shadow_scores_YYYY-MM-DD.csv`
- `sec_pit_shadow_bucket_comparison_YYYY-MM-DD.csv`
- `sec_pit_shadow_monitoring_plan_YYYY-MM-DD.csv`
- `sec_pit_baseline_coverage_summary_YYYY-MM-DD.json`
- `data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv`

## 输出

- `sec_pit_shadow_monitor_summary_YYYY-MM-DD.json`
- `sec_pit_shadow_monitor_summary_YYYY-MM-DD.md`
- `sec_pit_shadow_rolling_metrics_YYYY-MM-DD.csv`
- `sec_pit_shadow_warning_events_YYYY-MM-DD.csv`

## 状态解释

- `MONITORING_ACTIVE`：coverage gate 通过，但 minimum sample / observation-day evidence
  仍在积累。
- `OK_MONITORING`：coverage、样本和观察天数达到门槛，且未触发 warning 或 rollback；
  rolling metrics 暂不完整时只阻断 rollback，不再解释为 sample insufficiency。
- `WARNING`：rolling RankIC、relative return、drawdown improvement 或 bucket 对比触发 warning，
  但尚未满足 rollback recommendation 条件。
- `ROLLBACK_RECOMMENDED`：coverage/sample gates 通过、rollback 所需 rolling metrics 可用，
  且 RankIC rollback breach 与 outcome rollback breach 同时出现；仍然需要人工复核。
- `FAILED_VALIDATION`：输入 artifact 缺失、schema 不满足要求，或 baseline coverage gate 未通过。
- `INSUFFICIENT_MONITORING_SAMPLE`：保留为历史兼容状态；coverage gate 已通过后的正常滚动
  观察不再输出该状态。

## 必看字段

- `monitor_status`
- `monitor_maturity`
- `rolling_metrics_available`
- `state_transition_reason`
- `candidate_feature`
- `observe_weight`
- `rolling_rank_ic_20d`
- `rolling_rank_ic_60d`
- `monitoring_sample_count`
- `monitoring_days_elapsed`
- `monitoring_days_remaining`
- `warning_count`
- `rollback_recommended`
- `production_effect`

## 安全边界

- 所有输出固定 `production_effect=none`、`manual_review_required=true`。
- baseline coverage gate 未通过时不得输出 `ROLLBACK_RECOMMENDED`。
- minimum sample / observation-day evidence 未达标时不得输出 `ROLLBACK_RECOMMENDED`。
- rollback 所需 rolling metrics 不可用时不得输出 `ROLLBACK_RECOMMENDED`。
- factor underperformance 未经 RankIC 与 outcome 双重确认时不得输出 `ROLLBACK_RECOMMENDED`。
- dashboard 只读读取 latest monitor summary，不运行 `shadow-monitor`。
- 本命令不写 `config/weights/weight_profile_current.yaml`、`config/weights/shadow_weight_profiles.yaml`、
  `scores_daily.csv`、prediction ledger、order intent 或 approved overlay。
