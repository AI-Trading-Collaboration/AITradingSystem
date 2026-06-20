# TRADING-699 Dynamic / Trend Evidence Coverage Expansion

最后更新：2026-06-20

## 背景

TRADING-698 已完成 dynamic/trend threshold sensitivity review，但实际 evidence
coverage 仍偏窄：

- `full_advisory_case_count=22`
- `cluster_count=1`
- `regime_count=1`
- mature cases `1d/5d/10d/20d = 20 / 16 / 11 / 0`
- `evidence_strength=low`
- recommendation 仅为 `sensitivity_tested_only`

本任务只扩展 validation evidence 覆盖，不改变任何阈值、权重或生产路径。

## 安全边界

- 不修改任何 threshold `current_value`。
- 不修改 production / paper-shadow / official weights。
- 不改变 production scoring、position gate、paper shadow state、broker/order 或
  official target weights。
- 不允许升级为 `VALIDATED_BOUNDARY`。
- recommendation 只能是 validation recommendation。
- 所有输出继续固定：
  - `production_effect=none`
  - `promotion_gate_allowed=false`
  - `paper_shadow_change_allowed=false`
  - `production_weight_change_allowed=false`

## 范围

重新运行以下 3 个阈值 sensitivity review：

- `dynamic_allocation.risk_off_score_thresholds`
- `dynamic_allocation.risk_on_confirmation_thresholds`
- `trend_calibration.score_bands`

## Coverage 目标

- `cluster_count >= 3`
- `regime_count >= 3`
- `full_advisory_case_count` 尽量提高，并明确来源与限制
- 至少补齐 1d/5d/10d mature cases
- 20d 若仍不足，输出 pending maturity tracker，而不是把缺口隐藏在 row count 中

## 输出要求

扩展后的 sensitivity review 必须继续输出：

- by horizon
- by date
- by asset
- by correlated asset cluster
- by regime
- by event window
- full_advisory_only
- component/backtest bridge if applicable

不得只用 row count 下结论，必须披露：

- `mature_date_count`
- `mature_case_count`
- `full_advisory_case_count`
- `cluster_count`
- `regime_count`
- `sample_quality_breakdown`
- 20d pending maturity tracker when applicable

## Recommendation 约束

允许值仅限：

- `sensitivity_tested_only`
- `keep_current_value`
- `adjust_candidate`
- `insufficient_data`
- `collect_evidence_only`

本轮不得输出 production/paper-shadow/promotion recommendation。

## 数据策略

优先复用已有 validation artifacts、expanded historical trace、component/backtest bridge
和 research campaign control-window evidence。若现有 artifacts 不含足够多资产、
多 cluster 或多 regime 的 production-equivalent full advisory trace，报告必须把这些
来源标记为 component/backtest bridge 或 coverage-extension evidence，并保留
validation-only 限制。

## 验收

- Report 覆盖 3 个目标阈值。
- 每个目标阈值重新输出 sensitivity variants 和全部指定分层。
- 覆盖 summary 明确 TRADING-698 baseline 与 TRADING-699 扩展后样本差异。
- `cluster_count` 与 `regime_count` 达到目标，或明确阻塞原因与 data gap。
- 20d 未成熟时输出 pending maturity tracker。
- 所有输出保持 `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- focused 并行 pytest、Ruff、py_compile、真实 CLI、`git diff --check` 和 validation
  tier 通过或记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮只做 evidence coverage expansion
  和 validation-only sensitivity rerun，不修改 production / paper-shadow /
  official weights，不修改任何 threshold current value。
- 2026-06-20：实现显式 `--coverage-extension-root`，读取 research campaign
  control-window risk-signal evidence 并生成 `trace_source=backtest_trace_bridge`
  的 coverage-extension cases；standalone review 与 validation pack 均可携带同一
  coverage root。真实 rerun 输出 `tested_threshold_count=3`、
  `sensitivity_tested_count=3`、`validated_boundary_count=0`、
  `thresholds_changed_count=0`、`cluster_count=4`、`regime_count=4`、
  mature cases `1d=1748`、`5d=1744`、`10d=1739`、`20d=1728`、
  `full_advisory_case_count=22`、`coverage_extension_case_count=1728`。
  coverage targets 已达成；新增 bridge cases 不增加 full advisory equivalence，
  因此 recommendation 仍为 `sensitivity_tested_only`、`evidence_strength=low`。
  输出继续固定 `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- 2026-06-20：验证通过 `py_compile`、Ruff、Black check、focused indicator
  pytest `42 passed`、task/docs/config 并行 pytest `27 passed`、
  `git diff --check` 和 full 并行 validation tier
  `2983 passed / 643 warnings / 141.74s`。Runtime artifact:
  `outputs/validation_runtime/full_20260620T134945Z/test_runtime_summary.json`。
