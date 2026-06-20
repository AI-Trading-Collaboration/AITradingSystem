# TRADING-691 Masking Robustness Review

最后更新：2026-06-20

## 背景

TRADING-690 已为 valuation/crowding masking effectiveness review 输出
scenario × horizon conclusion matrix。当前基线为：

- recommendation=`preliminary_short_horizon_only`
- 1d/5d neutral or incomplete
- 10d supports `keep_baseline_masking_candidate`
- 20d `insufficient_long_horizon_evidence`
- `promotion_gate_allowed=false`

TRADING-691 在该 conclusion matrix 上新增 robustness review，用于判断 10d
baseline support 是否稳健、1d/5d neutral/incomplete 的原因，以及是否有足够
保守证据从 preliminary 升级到更强 validation recommendation。

## 安全边界

- 只生成 read-only / validation-only artifacts。
- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production data_quality_gate。
- 不使用 20d 当前不足样本推导正式结论。
- 所有 recommendation 都固定 `promotion_gate_allowed=false`、
  `production_weight_change_allowed=false`、`paper_shadow_change_allowed=false`。

## 输出要求

新增 scenario delta matrix，比较：

- baseline vs no valuation/crowding masking
- baseline vs capped masking
- capped masking vs no masking

每个 horizon 输出：

- `delta_avg_return`
- `delta_median_return`
- `delta_hit_rate`
- `delta_downside_capture`
- `delta_max_drawdown`
- `delta_missed_upside_count`
- `delta_false_risk_off_count`
- `delta_drawdown_reduced_count`
- `delta_turnover`
- `delta_constraint_hit_count`

新增 aggregation，不允许只用 row_count 判断：

- `equal_weight_by_date`
- `equal_weight_by_asset`
- `equal_weight_by_correlated_asset_cluster`
- `full_advisory_only`
- `all_validation_sources`

新增 case diagnostics：

- top winning cases
- top losing cases
- false risk-off cases
- missed upside cases
- drawdown reduction cases
- by asset
- by regime
- by event window

新增 10d baseline support attribution：

- 10d baseline wins 是否集中在少数日期
- 是否集中在 semiconductor/AI cluster
- 是否由 component-only 或 backtest-bridge 样本驱动
- full_advisory_only 中是否仍然成立
- 是否由单一 extreme event 驱动

新增 1d/5d neutral/incomplete explanation：

- mature sample 是否足够
- scenario 差异是否过小
- outcome 噪声是否过高
- 是否被同一日期/同一 cluster 稀释
- 是否存在 short-horizon whipsaw

新增 pending 20d maturity tracker：

- current 20d mature cases
- pending 20d cases
- expected maturity dates
- by asset / by date

## Conservative Evidence Gate

只有满足以下条件才允许从 `preliminary_short_horizon_only` 升级为更强
validation recommendation：

- 至少 1d/5d/10d 中两个 horizon 方向一致；
- full_advisory_only 不与 all-sources 结论冲突；
- date-level aggregation 不与 row-level aggregation 冲突；
- correlated asset cluster aggregation 不显示单一 cluster 主导；
- missed_upside / false_risk_off 没有明显恶化；
- `promotion_gate_allowed=false`。

## 验收

- scenario delta matrix schema test 通过；
- date-level aggregation test 通过；
- cluster-level aggregation test 通过；
- full_advisory_only consistency check test 通过；
- 10d support source attribution test 通过；
- 20d pending maturity tracker test 通过；
- conservative evidence gate test 通过；
- `promotion_gate_allowed=false`；
- 并行 pytest / validation tier 通过或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `valuation_crowding_masking_robustness_review` builder/CLI、validation-pack
  artifact 和稳定性 projection；真实 expanded historical trace rerun 输出
  `status=PASS_WITH_WARNINGS`、`source_effectiveness_recommendation=preliminary_short_horizon_only`、
  `final_validation_recommendation=keep_preliminary_short_horizon_only`、
  `scenario_delta_row_count=12`、`case_diagnostic_count=2448`、
  `current_20d_mature_cases=56`、`pending_20d_cases=1176`。
  Conservative evidence gate 的 stronger candidate 为
  `keep_baseline_masking_candidate`，但因
  `two_primary_horizons_consistent=false` 且
  `missed_upside_false_risk_off_not_worse=false`，最终保持 preliminary。
  10d baseline support attribution 显示 baseline wins 138 / 192，
  `top_date_share=0.115942`、`semiconductor_ai_cluster_share=0.405797`、
  `component_or_bridge_driven=false`、`full_advisory_only_still_holds=true`、
  `single_extreme_event_driven=false`。1d/5d mature sample 足够，但 scenario
  差异小、outcome noise 高且存在 short-horizon whipsaw。所有输出继续固定
  `promotion_gate_allowed=false`、`production_weight_change_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_logic_changed=false`。
- 2026-06-20：验证通过。`ruff check` 通过；`py_compile` 通过；
  focused 并行 pytest `tests/test_indicator_research.py`
  `tests/test_task_register_consistency.py` 为 39 passed / 21.35s；
  `git diff --check` 通过；full validation tier 使用 16 workers
  `--dist loadfile`，结果为 2974 passed / 642 warnings / 155.55s，
  runtime artifact 写入
  `outputs/validation_runtime/trading-691-full/test_runtime_summary.json`。
