# TRADING-686 / TRADING-687 Masking Effectiveness And Lineage Repair

最后更新：2026-06-20

## 背景

TRADING-665～685 已把 historical trace expansion 推到 `VALIDATING`：

- expanded historical trace：22 dates / 352 rows；
- asset universe：8 assets；
- valuation/crowding masking diagnostic cases：176；
- `full_advisory_trace_eligible_count=22`；
- `component_validation_trace_eligible_count=28`；
- gate root-cause 中仍有 `lineage_manifest_missing=96`。

本批次继续在 read-only / validation-only 边界内推进 TRADING-686 与 TRADING-687。

## 安全边界

- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production `data_quality_gate` 或 feature availability gate。
- 所有 recommendation 只能作为 validation recommendation，不得进入 promotion gate。
- partial / component / backtest bridge 样本必须保留：
  - `promotion_gate_allowed=false`；
  - `allowed_uses=[diagnostic, ablation, sensitivity_analysis]`；
  - `trace_source`；
  - `confidence`。

## TRADING-686：valuation/crowding masking effectiveness review

### 输出要求

对以下只读场景做正式对比：

1. `baseline`
2. `no_valuation_crowding_masking`
3. `capped_masking`

每个场景至少输出：

- `avg_return_1d` / `avg_return_5d` / `avg_return_10d` / `avg_return_20d`
- `hit_rate_1d` / `hit_rate_5d` / `hit_rate_10d` / `hit_rate_20d`
- `max_drawdown`
- `drawdown_preservation`
- `missed_upside_count`
- `false_risk_off_count`
- `drawdown_reduced_count`
- `turnover`
- `constraint_hit_count`

### 分层要求

结果必须同时分层输出：

- `full_advisory_only`
- `component_only`
- `backtest_bridge`
- `by_date`
- `by_asset`
- `by_regime`
- `by_event_window`

报告不得只使用 `row_count` 下结论；每层必须披露：

- `date_count`
- `asset_count`
- `case_count`
- `unique_regime_count`
- `correlated_asset_cluster_count`

### Recommendation 口径

允许输出以下 recommendation 之一：

- `keep_baseline_masking`
- `prefer_capped_masking`
- `disable_masking_candidate`
- `insufficient_evidence`

Recommendation 必须附带 `recommendation_scope=validation_only` 与
`promotion_gate_allowed=false`。

## TRADING-687：lineage manifest repair

### 输出要求

针对 root-cause 中 `lineage_manifest_missing=96` 的 affected artifacts，输出：

- `source_artifact_path`
- `generated_at`
- `as_of_date`
- `decision_time`
- `config_hash`
- `input_snapshot_hash`
- `trace_contract_version`
- `production_equivalent`
- `manifest_validation_status`

### Gate audit 目标

修复后重新运行 gate availability audit，目标：

- `audited_date_count >= 40`
- `full_advisory_trace_eligible_count > 22`
- `component_validation_trace_eligible_count >= 28`
- `lineage_manifest_missing` 明显下降

若 `full_advisory_trace_eligible_count > 22` 因 PIT feature availability 或 production
data quality gate fail-closed 无法达成，必须明确记录阻塞 root cause，不得通过放宽 gate
达成目标。

## 实施步骤

1. 追加 effectiveness review builder、CLI 和 focused tests。
2. 追加 lineage manifest repair builder、CLI 和 focused tests。
3. 在真实 expanded historical trace 上生成 artifacts。
4. 对 lineage-missing 日期执行不放宽 gate 的 replay/gate 证明或 repair audit；若仍 fail-closed，输出 root-cause 分类。
5. 重新生成 gate availability audit 并记录目标达成情况。
6. 使用并行 pytest / validation tier 做验证。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮只新增 validation artifacts 与报告建议，不修改任何 production 权重链路或 forbidden weights。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增 `valuation_crowding_masking_effectiveness_review` 与 `lineage_manifest_repair_report` builder/CLI/validation-pack artifacts 和 focused tests。Effectiveness review 基于 expanded historical trace 输出 22 dates、8 assets、176 cases，full_advisory_only / backtest_bridge 均有 176 cases，component_only 因无 trace-backed pre/post mask signal 保持 case_count=0；baseline / no valuation-crowding masking / capped masking 均输出 avg return、hit rate、max drawdown、drawdown preservation、turnover 和 constraint hit；因 20d outcome 未成熟导致 outcome_missing_count=168，decision recommendation=`insufficient_evidence`，且 `recommendation_scope=validation_only`、`promotion_gate_allowed=false`。
- 2026-06-20：针对修复前 `lineage_manifest_missing=96` 输出 12 个 affected artifacts，均为 `SOURCE_ARTIFACT_MISSING`、`production_equivalent=false`。随后对 2026-04-27～2026-05-14 的 12 个缺失日期运行 PIT-sliced `score-daily` replay 证明，全部被 production feature availability gate fail-closed，均写出 `feature_availability.md`，未生成 daily indicator trace。修复后 gate availability audit：audited_date_count=40、component_validation_trace_eligible_count=40、full_advisory_trace_eligible_count=22、root_cause_reason_class_counts=`expected_pit_limitation:144`，`lineage_manifest_missing` 从 96 降为 0。`full_advisory_trace_eligible_count > 22` 未达成，原因是 PIT feature available_time 晚于 decision_time；未放宽 production data_quality_gate 或 feature gate。Validation pack artifact_count=28，validation-pack-stability `PASS`、stable=true。
