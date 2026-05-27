# SEC PIT Observe-Only Shadow Lane Runbook

本 runbook 对应 `TRADING-044`，用于把已人工复核的 SEC PIT candidate 放入隔离
observe-only shadow lane。当前唯一支持 candidate 是 `capex_intensity`，初始
`observe_weight=-0.025`，最大允许初始绝对权重为 `0.050`。

本命令只记录如果该 SEC PIT component 进入评分会怎样影响 score 和 rank；它不修改
production weights、active shadow weights、approved overlay、production action、order intent
或非 SEC shadow lane。

## 运行命令

```bash
aits sec-pit shadow-observe \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --candidate-review-dir outputs/sec_pit_candidate_review \
  --evaluation-dir outputs/sec_pit_evaluation \
  --comparison-dir outputs/sec_pit_baseline_comparison \
  --diagnostics-dir outputs/sec_pit_diagnostics \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --baseline-score-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --candidate-feature capex_intensity \
  --observe-weight -0.025 \
  --max-allowed-weight 0.050 \
  --output-dir outputs/sec_pit_shadow_observe
```

也可自动发现 latest upstream artifacts：

```bash
aits sec-pit shadow-observe --latest
```

`--latest` 会查找最新 candidate review、evaluation、baseline comparison、diagnostics 和
feature panel artifact。Baseline score 可用 `--baseline-score-path` 显式指定；未指定时使用
baseline resolver，并在 summary 中披露 fallback 或 missing 状态。

若 historical baseline 覆盖是本轮复核重点，先运行：

```bash
aits sec-pit audit-baseline-coverage \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --baseline-score-path data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --output-dir outputs/sec_pit_baseline_coverage
```

## 配置

默认配置：

- `config/sec_pit_shadow_observe.yaml`
- lane id: `sec_pit_capex_intensity_observe_only`
- lane status: `observe_only`
- manual decision: `APPROVE_OBSERVE_ONLY_SHADOW`
- production effect: `none`
- PIT grade policy: `B_RECONSTRUCTED_SEC_FILING_PIT`
- monitoring quality gate:
  - `min_baseline_coverage_ratio: 0.90`
  - `min_label_coverage_ratio: 0.90`
  - `min_monitoring_sample_count: 20`
  - `data_limited_status_prevents_factor_rollback: true`

配置中的安全阈值和 monitoring quality gate 是 observe-only policy，不授权 production
promotion。baseline coverage、label coverage 或 monitoring sample count 未达标时，monitoring
只能输出 limited/data 状态，不能把信号表现解释成 factor rollback。

## 必须检查

- `sec_pit_shadow_observe_summary_YYYY-MM-DD.json`
- `sec_pit_shadow_observe_summary_YYYY-MM-DD.md`
- `sec_pit_shadow_scores_YYYY-MM-DD.csv`
- `sec_pit_shadow_rank_shift_YYYY-MM-DD.csv`
- `sec_pit_shadow_bucket_comparison_YYYY-MM-DD.csv`
- `sec_pit_shadow_monitoring_plan_YYYY-MM-DD.csv`
- `sec_pit_shadow_safety_audit_YYYY-MM-DD.csv`

## 解释顺序

1. 先看 summary 的 `shadow_status`、`production_effect`、`candidate_review_status`、
   `diagnostics_status`、`provenance_complete`、`data_quality_score` 和
   `baseline_overlap_risk`。
2. 再看 `baseline_coverage_ratio`、`label_coverage_ratio`、`monitoring_sample_count`、
   `monitoring_status`、`monitoring_status_reason`、`factor_rollback_triggered` 和
   `data_limitation_triggered`。只有 coverage/sample gate 达标后，factor rollback 才有解释资格。
3. 再看 safety audit。任何 critical safety failure 都表示本轮不得解释 shadow score。
4. 用 shadow scores CSV 查看每个 ticker/date 的 baseline score、SEC PIT component、
   observe-only score 和 rank delta。
5. 用 rank shift CSV 复核哪些 ticker 被 observe-only component 上调或下调。
6. 用 bucket comparison CSV 分别看 `all`、`semiconductor`、`platform` 和可选 `other`。
7. 用 monitoring plan CSV 观察 rolling RankIC、relative return、drawdown、hit rate、
   data quality、provenance 和 baseline overlap 的 warning / rollback 状态。

## 状态解释

- `OK`：安全检查通过，baseline 和 labels 足以生成 observe-only score/rank artifact。
- `LIMITED_BASELINE_MISSING`：baseline score 缺失或为空；仍输出 partial shadow score，但 rank
  shift、observe score 和 monitoring 解释受限。
- `LIMITED_LABELS_MISSING`：forward label 缺失，不能解释后验效果。
- `INSUFFICIENT_MONITORING_SAMPLE`：baseline 和 label 覆盖可用，但有效 monitoring 样本数低于
  `monitoring_quality_gate.min_monitoring_sample_count`；不得触发 factor rollback。
- `ROLLBACK_TRIGGERED_BY_FACTOR`：coverage 和 sample gate 均达标后，factor performance 指标触发
  rollback 条件。
- `ROLLBACK_TRIGGERED_BY_DATA`：数据质量、provenance 或 baseline overlap 等数据类条件触发
  rollback；这不是 factor underperformance。
- `LIMITED_CANDIDATE_REVIEW_MISSING`：缺少 TRADING-043 candidate review；不会生成 score rows。
- `FAILED_SAFETY_CHECK`：critical safety check failed；只写 safety audit 和 degraded summary。
- `FAILED_VALIDATION`：输入 schema 或解析失败。

## 安全边界

- 所有相关输出固定 `manual_review_required=true`、`production_effect=none`。
- 输出路径必须隔离在 `outputs/sec_pit_shadow_observe`。
- 不写 `config/weights/weight_profile_current.yaml`。
- 不写 `config/weights/shadow_weight_profiles.yaml`。
- 不写 score-daily、prediction ledger、order intent、approved overlay 或 active shadow state。
- `capex_intensity` 的 observe lane 不是 production promotion，也不是 active shadow lane 更新。
- 仍按 `B_RECONSTRUCTED_SEC_FILING_PIT` 解释，不得当作 strict vendor archive PIT。

## Dashboard

Daily task dashboard 只读读取 latest
`outputs/sec_pit_shadow_observe/sec_pit_shadow_observe_summary_YYYY-MM-DD.json`，展示 latest shadow
observe date、shadow status、lane id、candidate feature、observe weight、production effect、score
rows、top positive / negative rank shifts、safety check status、baseline coverage ratio/status、
monitoring status reason、factor rollback triggered 和 data limitation triggered。Dashboard 不运行
`aits sec-pit shadow-observe`，不写任何配置，也不运行 baseline coverage audit 或 backfill。
