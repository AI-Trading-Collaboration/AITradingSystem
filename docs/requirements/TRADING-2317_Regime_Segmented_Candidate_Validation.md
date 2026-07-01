# TRADING-2317 Regime-Segmented Candidate Validation

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2316 已生成 PIT-approx regime label series。TRADING-2317 使用这些 labels
重新解释已有候选的 actual-path evidence，但不得把 regime labels 变成 direct signal、
scope override、risk-cap override、portfolio input、paper-shadow、production 或 broker path。

Owner roadmap 指定要重新解释：

- volatility risk-cap
- breadth proxy
- AI leadership
- liquidity pressure

## 目标

新增 CLI：

```bash
aits research trends regime-segmented-candidate-validation
```

把已有 validation / diagnostics artifacts 按 `primary_trend_regime` 和
`volatility_overlay` 分段，输出 diagnostic-only coverage、performance、blocker 和
interpretation matrix。

## 输入

默认读取：

```text
config/research/regime_segmented_candidate_validation_policy.yaml
outputs/research_trends/regime_label_generator_diagnostic_poc/regime_label_series.csv
outputs/research_trends/regime_label_generator_diagnostic_poc/regime_label_generation_summary.json
outputs/research_trends/scope_narrowed_candidate_actual_path_validation/
outputs/research_trends/breadth_proxy_signal_concept_selection/
outputs/research_trends/ai_leadership_actual_path_validation/
outputs/research_trends/liquidity_rates_actual_path_validation/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
```

命令必须先调用 `validate_data_cache` 同源质量门禁。若 gate failure 或 TRADING-2316
label source 不可用，必须 fail closed。

## 产物

- `regime_segmented_candidate_validation_summary.json`
- `regime_segmented_candidate_performance_matrix.json`
- `regime_segmented_candidate_performance_matrix.csv`
- `regime_segmented_candidate_coverage_matrix.json`
- `regime_segmented_candidate_coverage_matrix.csv`
- `regime_segmented_family_blocker_matrix.json`
- `regime_segmented_family_blocker_matrix.csv`
- `regime_segmented_interpretation_matrix.json`
- `regime_segmented_interpretation_matrix.csv`
- `regime_segmented_candidate_validation_safety_boundary.json`
- `docs/research/regime_segmented_candidate_validation.md`
- `data_quality_YYYY-MM-DD.md`

## 实施边界

1. Segmentation role。
   - 只解释已有 actual-path validation rows 的表现是否集中在某些 regime segment。
   - 不训练模型，不生成 candidate signal，不改变 candidate scope/recommendation。

2. Breadth proxy。
   - 当前 TRADING-2304 selected_concept_count=0，source status 为 source-blocked。
   - 本任务必须输出 blocker row，不得伪造 breadth segment performance。

3. Evidence interpretation。
   - `segment_min_eligible_records`、small sample 状态和 interpretation labels 必须来自
     policy manifest。
   - 分段结果只能输出 diagnostic interpretation，不得输出 promotion-ready、
     paper-shadow-ready、production-ready 或 broker-ready 状态。

4. 安全边界。
   - `diagnostic_only=true`
   - `segmentation_only=true`
   - `actual_path_validation_consumed=true`
   - `new_actual_path_validation_executed=false`
   - `candidate_signal_generated=false`
   - `candidate_artifact_generated=false`
   - `direct_strategy_signal_allowed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends regime-segmented-candidate-validation`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、actual requested date range、
  data quality status、label source status、candidate family coverage 和 safety flags。
- Volatility risk-cap、AI leadership、liquidity pressure 至少生成可审计 segmented
  performance rows；breadth proxy 生成 source-blocked family blocker row。
- 输出不得改写 TRADING-2292 / 2309 / 2313 / 2314 的原 validation / scope conclusion。
- 输出不得生成 candidate signal、promotion、paper-shadow、production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2316 next task 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 run status=
  `REGIME_SEGMENTED_CANDIDATE_VALIDATION_READY_DIAGNOSTIC_ONLY`，
  data_quality_status=`PASS_WITH_WARNINGS`，actual_requested_date_range=
  `2022-12-01..2026-06-29`，performance_row_count=294，coverage_row_count=31，
  family_blocker_row_count=4，interpretation_row_count=4。Focused parallel pytest
  `tests/research_trends/test_regime_segmented_candidate_validation.py` 6 passed。
- 2026-07-01: 验证通过 Ruff、compileall、regime focused parallel pytest 19 passed、
  `aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS、真实 CLI run、
  `contract-validation` 193 passed。Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T064402Z/test_runtime_summary.json`。
