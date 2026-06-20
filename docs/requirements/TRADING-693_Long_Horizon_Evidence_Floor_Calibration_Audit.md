# TRADING-693 Threshold Registry And Calibration Audit

最后更新：2026-06-20

## 背景

TRADING-665～692 的 indicator research validation pack 已经把
valuation/crowding masking、outcome maturity、robustness review 和 validation
rollup 串成只读审计链路。TRADING-693 本轮不做阈值校准，而是把系统中的工程
缺省阈值和 magic numbers 先登记成可审计 inventory，标记风险等级和后续校准
优先级。

原 long-horizon evidence floor 问题继续纳入本任务：20d full advisory mature
cases 与 floor 50 的关系不得被表述为 validated statistical threshold。floor 50
在 registry 中标记为 `HEURISTIC_GUARDRAIL`，并进入 evidence/sample-floor
calibration backlog。

## 安全边界

- 本轮只生成 validation-only artifacts。
- 不修改 production / paper-shadow / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production data_quality_gate。
- 不把任何 A 类未校准阈值作为 promotion dependency。
- validation pack summary 可以暴露 `thresholds_blocking_promotion`，但不能批准
  promotion、paper-shadow、broker/order 或 official target weights。

## 范围

覆盖：

- config 中的 policy/default thresholds；
- indicator research threshold / masking / dominance / outcome maturity 常量；
- validation pack stability 与 audit 默认值；
- masking diagnostics 与 valuation/crowding counterfactual 默认值；
- outcome maturity evidence floor / sample floor；
- promotion gates；
- robustness gates；
- backtest / replay / allocation constraints；
- tests 中固化的阈值和 fixture magic numbers。

低风险常量不纳入本轮 registry：格式精度、数组索引、schema/protocol version、
重试次数、timeout、UI 尺寸和不影响投资解释的纯工程常量。

## 输出

1. `config/research/threshold_registry.yaml`
   - 每个 threshold 必须记录：
     - `threshold_id`
     - `current_value`
     - `unit`
     - `where_used`
     - `purpose`
     - `impact_scope`
     - `decision_affecting`
     - `promotion_gate_affecting`
     - `production_weight_affecting`
     - `default_reason`
     - `calibration_status`
     - `evidence_level`
     - `recommended_calibration_method`
   - 额外记录 `threshold_class`、`calibration_required` 和
     `no_promotion_dependency_without_review`，用于 validation pack audit。

2. Threshold 分级：
   - `A`：high-impact decision / promotion / signal-affecting；
   - `B`：validation quality / research workflow affecting；
   - `C`：engineering runtime / performance / operational。

3. A 类未校准阈值标记：
   - `calibration_status` 只能使用 `UNCALIBRATED_DEFAULT` 或
     `HEURISTIC_GUARDRAIL`；
   - `calibration_required=true`；
   - `no_promotion_dependency_without_review=true`。

4. Calibration backlog：
   - valuation/crowding 相关阈值；
   - evidence floor / sample floor；
   - masking ratio threshold；
   - robustness gate threshold；
   - promotion gate threshold；
   - risk-off / trend confirmation threshold。

5. Validation pack 输出 `threshold_audit_summary`：
   - `total_threshold_count`
   - `high_impact_threshold_count`
   - `uncalibrated_high_impact_count`
   - `heuristic_guardrail_count`
   - `calibrated_count`
   - `thresholds_blocking_promotion`
   - `thresholds_blocking_promotion_count`

## 验收

- `threshold_registry.yaml` 存在，并覆盖本任务列出的模块族。
- `build_threshold_registry_audit()` 输出 `threshold_registry_audit` artifact。
- `aits research indicators threshold-audit` 可写出 JSON/Markdown artifact。
- `aits research indicators validation-pack` 的 summary 包含
  `threshold_audit_summary`。
- 所有 A 类未校准阈值均带 promotion review block 标记。
- Calibration backlog 按 owner 关心的六类优先级输出。
- focused 并行 pytest / validation tier 通过，或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。初始范围是 long-horizon evidence
  floor calibration audit。
- 2026-06-20：按 owner 新指令扩展为全系统 threshold inventory、A/B/C 分级、
  promotion-blocking 标记和 calibration backlog；保持 validation-only，不修改
  production / paper-shadow / official weights。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `config/research/threshold_registry.yaml`、`threshold_registry_audit`
  builder/CLI、validation-pack `threshold_audit_summary` 和 rerun stability
  projection。真实 CLI 输出 `total_threshold_count=44`、
  `high_impact_threshold_count=36`、`uncalibrated_high_impact_count=36`、
  `heuristic_guardrail_count=25`、`calibrated_count=0`、
  `thresholds_blocking_promotion_count=36`，`production_effect=none`。验证通过
  focused 并行 pytest 36 passed、task/docs 并行 pytest 10 passed、config 并行
  pytest 17 passed、Ruff、py_compile、Black check、真实 CLI threshold-audit /
  validation-pack 和 `git diff --check`。
- 2026-06-20：补齐 long-horizon evidence floor calibration audit 并复验。真实
  expanded historical trace rerun 输出
  `current_20d_full_advisory_mature_cases=32`、`current_floor=50`、
  `floor_name=heuristic_min_full_advisory_cases`、`calibration_status=uncalibrated`、
  `recommendation_changes_across_floors=false`、
  `first_floor_where_recommendation_stabilizes=20`、
  `twenty_day_conclusion_driver=sample_count_and_robustness_failures`。Effective
  sample size 为 `raw_case_count=32`、`unique_date_count=1`、
  `unique_asset_count=8`、`correlated_asset_cluster_count=3`、`regime_count=1`；
  robustness gate 因 `leave_one_date_out_stable=false` 和
  `leave_one_cluster_out_stable=false` 未通过，calibration conclusion 为
  `insufficient_data_to_calibrate_floor`，`floor_50_action=floor_50_retained_as_heuristic`。
  Threshold audit 输出 `total_threshold_count=44`、`high_impact_threshold_count=36`、
  `uncalibrated_high_impact_count=36`、`production_weight_affecting_threshold_count=0`。
  Validation pack artifacts=33，validation-pack-stability `PASS stable=true`；
  验证通过 Ruff、py_compile、focused 并行 pytest 42 passed、`git diff --check`
  和 full 并行 validation tier 2977 passed / 643 warnings / 209.82s。所有新增
  输出继续 `promotion_gate_allowed=false`、`production_weight_change_allowed=false`、
  `paper_shadow_change_allowed=false`。
