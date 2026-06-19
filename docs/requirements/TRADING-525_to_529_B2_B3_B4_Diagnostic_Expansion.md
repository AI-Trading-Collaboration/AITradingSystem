# TRADING-525 to 529 B2/B3/B4 Diagnostic Expansion

最后更新：2026-06-19

## 背景

TRADING-511A~511D 与 TRADING-521~524 已完成 research-only unblock 和 B1-B4
diagnosis。当前有效结论：

- B0 static strategic baseline complete。
- B1 execution/no-trade/turnover control complete，但证据为 conditional mixed。
- B2 fast risk scaler complete，但当前 mini window 未证明独立贡献。
- B3 slow relative tilt complete，但当前 mini window 为负贡献。
- B4 B2+B3 complete，但 interaction classification 仍为 `INCONCLUSIVE`。
- B5、B6、v3 必须继续 blocked。

本批次只做 diagnosis 和 admission checkpoint，不修改 strategy logic，不调整阈值，不生成
official target weights，不创建 paper-shadow、broker/order、live trading 或 production effect。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-525|B2 risk-scaler trigger coverage audit|VALIDATING|
|TRADING-526|B3 slow-tilt negative contribution attribution|VALIDATING|
|TRADING-527|B1/B2/B3/B4 multi-window diagnostic expansion|VALIDATING|
|TRADING-528|B4 interaction evidence synthesis|VALIDATING|
|TRADING-529|B5 admission checkpoint|VALIDATING|

## 输入与边界

- 输入为 canonical `docs/research` artifacts、`research_window_catalog.json`、frozen
  B2/B3 module policy 和本地 cached ETF price/rate data。
- 所有数据相关诊断必须先运行同一路径 `aits validate-data` gate，并在输出披露状态。
- 只允许使用 development / diagnostic windows；不得使用
  `untouched_temporal_holdout`。
- B0/B1/B2/B3/B4 多窗口诊断必须复用已冻结的 B1 execution control、B2 risk
  signal/target mapping、B3 relative tilt signal/target mapping 和 B4 interaction formula。
- 不得混入 P0 dynamic allocator、B5 confidence shrinkage、B6 regime information、
  official target weight semantics、paper-shadow 或 broker/order side effect。

## 必需输出

### TRADING-525

输出 `b2_risk_scaler_trigger_coverage_audit.json/md`，至少包含：

- `risk_trigger_count`
- `risk_trigger_dates`
- `risk_signal_values`
- `risk_scaler_exposure_changes`
- `risk_off_events`
- `risk_reentry_events`
- `unused_risk_signal_reason`
- `window_risk_intensity`

状态必须为：

- `B2_TRIGGERED_WITH_EFFECT`
- `B2_NOT_TRIGGERED_WINDOW_INSUFFICIENT`
- `B2_SIGNAL_PRESENT_BUT_SCALER_INACTIVE`
- `B2_BINDING_ISSUE`
- `B2_THRESHOLD_TOO_INSENSITIVE`
- `B2_REQUIRES_RISK_HEAVY_WINDOWS`

### TRADING-526

输出 `b3_slow_tilt_negative_contribution_attribution.json/md`，至少包含：

- `tilt_signal_series_summary`
- `tilt_direction_by_asset`
- `tilt_magnitude_summary`
- `tilt_turnover_contribution`
- `wrong_tilt_dates`
- `relative_strength_lag_summary`
- `asset_contribution_breakdown`
- `cost_drag_from_tilt`

状态必须为：

- `B3_NEGATIVE_DUE_TO_SIGNAL_DIRECTION`
- `B3_NEGATIVE_DUE_TO_SIGNAL_LAG`
- `B3_NEGATIVE_DUE_TO_TURNOVER`
- `B3_NEGATIVE_DUE_TO_TILT_CAP`
- `B3_NEGATIVE_DUE_TO_WINDOW`
- `B3_BINDING_ISSUE`
- `B3_REQUIRES_MORE_WINDOWS`
- `B3_SHOULD_BE_REDESIGNED`

### TRADING-527

输出 `b1_b4_multi_window_diagnostic_expansion.json/md`。窗口至少覆盖：

- normal uptrend window
- rapid drawdown window
- slow drawdown window
- high-volatility sideways window
- V-shaped recovery window
- semiconductor correction window
- false risk-off cluster window

每个窗口必须标记：

- `window_id`
- `start_date`
- `end_date`
- `market_regime`
- `purpose`
- `allowed_stage=diagnostic`
- `holdout_allowed=false`

每个窗口必须运行 B0、B1、B2、B3、B4，并输出：

- B1 vs B0
- B2 vs B0
- B3 vs B0
- B4 vs B0
- B4 vs B2
- B4 vs B3
- `return_delta`
- `drawdown_delta`
- `turnover_delta`
- `cost_delta`
- `benchmark_relative_delta`
- `risk_trigger_count`
- `tilt_contribution`
- `constraint_hits`
- `window_result`

状态必须为：

- `MULTI_WINDOW_DIAGNOSTIC_COMPLETE`
- `MULTI_WINDOW_DIAGNOSTIC_PARTIAL`
- `MULTI_WINDOW_DIAGNOSTIC_BLOCKED`

### TRADING-528

输出 `b4_interaction_evidence_synthesis.json/md`，汇总 TRADING-525~527 并判断：

- B2 是否在 risk-heavy windows 有有效贡献；
- B3 是否在 trend / relative-strength windows 有有效贡献；
- B4 是否优于 B2；
- B4 是否优于 B3；
- B4 是否优于 B0；
- B4 是否在 cost / turnover / benchmark 维度恶化；
- B4 是否在 stress windows 中稳定；
- B4 是否仍主要等同于 B3。

Interaction classification 必须为：

- `B4_POSITIVE_SYNERGY`
- `B4_MOSTLY_ADDITIVE`
- `B4_REDUNDANT`
- `B4_NEGATIVE_INTERFERENCE`
- `B4_INCONCLUSIVE_MORE_EVIDENCE`
- `B4_RETURN_TO_DESIGN`

### TRADING-529

输出 `b5_admission_checkpoint.json/md`。状态必须为：

- `B5_ADMISSION_ALLOWED`
- `B5_ADMISSION_BLOCKED_MORE_EVIDENCE`
- `B5_ADMISSION_BLOCKED_RETURN_TO_DESIGN`
- `B5_ADMISSION_BLOCKED_NEGATIVE_INTERACTION`

必须输出：

- `b5_allowed`
- `b6_allowed`
- `v3_allowed`
- `next_recommended_task`

强制规则：

- 如果 `b5_allowed=false`，则 `b6_allowed=false`。
- 如果 `b5_allowed=false`，则 `v3_allowed=false`。
- 即使 `b5_allowed=true`，也不得自动运行 B5，必须进入下一批任务。

## 安全边界

所有输出必须保持：

- `research_only=true`
- `manual_review_only=true`
- `official_target_weights=false`
- `production_effect=none`
- `broker_action_allowed=false`
- `order_ticket_generated=false`
- `paper_shadow_activation=false`
- `extended_shadow_allowed=false`
- `live_trading_allowed=false`

## 状态记录

- 2026-06-19：owner 附件要求在 TRADING-521~524 后新增 TRADING-525~529；
  目标是解释 B2 未触发、B3 负贡献、B4 是否只是单窗口现象，并生成 B5
  admission checkpoint。当前 B4 仍 inconclusive，B5/B6/v3 继续 blocked。
- 2026-06-19：实现只读 CLI `aits etf weight-research diagnose-b2-b4-expansion`
  和 canonical aliases：`docs/research/b2_risk_scaler_trigger_coverage_audit.json/md`、
  `docs/research/b3_slow_tilt_negative_contribution_attribution.json/md`、
  `docs/research/b1_b4_multi_window_diagnostic_expansion.json/md`、
  `docs/research/b4_interaction_evidence_synthesis.json/md`、
  `docs/research/b5_admission_checkpoint.json/md`。
- 2026-06-19：真实诊断结论为 TRADING-525
  `B2_REQUIRES_RISK_HEAVY_WINDOWS`、TRADING-526
  `B3_NEGATIVE_DUE_TO_SIGNAL_DIRECTION`、TRADING-527
  `MULTI_WINDOW_DIAGNOSTIC_COMPLETE`、TRADING-528 `B4_REDUNDANT`、TRADING-529
  `B5_ADMISSION_BLOCKED_MORE_EVIDENCE`；`b5_allowed=false`、`b6_allowed=false`、
  `v3_allowed=false`，不继续 B5/B6/v3。
