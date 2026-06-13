# TRADING-251 to 255 Smoothed Method Evidence Drilldown and Forward Confirmation

最后更新：2026-06-13

## 状态

VALIDATING

## 背景

TRADING-246 to 250 已经实现 `smooth_weights_3d_limited_adjustment` 和
`smooth_weights_5d_limited_adjustment` research-only methods，并产出真实 artifacts：

- smoothed target: `smoothed-limited_eae4d8aa3efe7669`
- smoothed backfill: `smoothed-backfill_27939e31bfdf54c6`
- comparison: `smoothed-comparison_6e51482964e50fab`
- review: `smoothed-review_3275f9ae7fde2ebb`

当前 review decision 为 `CONTINUE_OBSERVATION`，confidence 为 `LOW`，
`requires_forward_confirmation=true`。本阶段不新增 target method，不自动 promotion，
不写 official target weights，不修改 `position_advisory_v1.yaml`，不触发 broker/order/production。

## 目标

本阶段把 smoothed method 的支持证据、反对证据、lag cost、regime 表现和 forward
confirmation 目标结构化，使 owner 和 weekly operations 能持续观察：

1. `smooth_weights_3d_limited_adjustment` 改善了哪些问题。
2. smoothing 是否降低 weight jumps、signal churn、turnover 并改善 rolling consistency。
3. `smooth_weights_3d_limited_adjustment` 是否保留 `limited_adjustment` 收益优势。
4. smoothing 在 `sideways_choppy` 是否更稳。
5. smoothing 在 `strong_recovery` / fast regime change 是否明显滞后。
6. `smooth_weights_5d_limited_adjustment` 是否过度平滑。
7. 进入下一轮 promotion review 前需要哪些 forward confirmation。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-251|Smoothed Review Reason Attribution|VALIDATING|`smoothed-review-attribution run/report` 和 `validate-smoothed-review-attribution` 可运行，输出 supporting/blocking reasons、why_not_promote、why_not_reject。|
|TRADING-252|Smoothing Benefit vs Lag Cost Drilldown|VALIDATING|`smoothing-benefit-lag run/report` 和 `validate-smoothing-benefit-lag` 可运行，输出 benefit summary、lag cost summary、tradeoff matrix。|
|TRADING-253|Sideways / Recovery Regime Validation|VALIDATING|`smoothed-regime-validation run/report` 和 `validate-smoothed-regime-validation` 可运行，输出 sideways 和 recovery lag validation。|
|TRADING-254|Smoothed Forward Confirmation Target Registration|VALIDATING|`smoothed-confirmation register/report` 和 `validate-smoothed-confirmation` 可运行，targets 可读且 `auto_apply=false`。|
|TRADING-255|Smoothed Method Operational Watch Pack|VALIDATING|`smoothed-watch-pack run/report` 和 `validate-smoothed-watch-pack` 可运行，生成 owner checklist、Reader Brief section 和 watch summary。|

## Pilot Baselines And Governance

以下阈值和样本要求来自本阶段 owner 任务说明，先作为可审计 pilot baseline 使用：

- `smooth_3d_vs_limited.required_forward_events=10`
- `smooth_3d_sideways_choppy_improvement.required_sideways_events=5`
- `smooth_3d_recovery_lag_watch.required_recovery_events=5`
- confirmation windows: `[1, 5, 10, 20]`
- `smooth_3d_vs_limited.return_delta_min=-0.001`
- `turnover_delta_max=0.0`
- `drawdown_delta_max=0.0`
- sideways churn / weight jump / turnover delta max 均为 `0.0`

这些数值不代表 production promotion gate。它们只用于 research watch 和 owner review
前的 forward confirmation 目标登记。退出条件：积累足够 forward events 后，由 owner
review 把 pilot baseline 替换为 evidence-backed calibration，或明确拒绝 smoothed method。

## Safety Boundary

所有新增 artifacts 和报告必须固定：

- `research_target_only=true`
- `paper_shadow_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

达标后也不自动 promotion。任何 promotion、official target weights、broker action 或
production policy mutation 都必须另开 owner-approved workflow。

## Implementation Plan

1. 复用现有 `smoothed_review`、`smoothed_comparison` 和 `smoothed_backfill` artifacts 构建 review attribution。
2. 复用 comparison metrics、stability metrics、rolling metrics 和 lag analysis 构建 benefit / lag drilldown。
3. 复用 smoothed backfill state path 和现有 regime labeling 逻辑输出 sideways / recovery validation。
4. 登记 smoothed forward confirmation targets，保留 watch-only recovery lag target。
5. 汇总 1-4 为 weekly/owner watch pack，并提供 Reader Brief section。
6. 更新 README、operations runbook、system flow、report registry、artifact catalog 和 task register。
7. 新增 focused tests，运行 validators、ruff、compileall 和 diff check。

## Progress Notes

- 2026-06-13: 新增需求文档并进入 IN_PROGRESS；本阶段只做 evidence drilldown、
  forward confirmation registration 和 operational watch，不实现新方法、不改变 production。
- 2026-06-13: baseline 实现完成并转入 VALIDATING；真实链路输出 attribution
  `smoothed-review-attribution_75ec54d7e572038d`、benefit / lag drilldown
  `smoothing-benefit-lag_ea3a057745a3f0cd`、regime validation
  `smoothed-regime-validation_3fd897c7c66b3c40`、confirmation targets
  `smoothed-confirmation_0753b4cfbe5a2777`、watch pack
  `smoothed-watch-pack_520686f9c6924a84`。Review attribution 仍为
  decision=`CONTINUE_OBSERVATION`、confidence=`LOW`；benefit / lag tradeoff 当前为
  `INSUFFICIENT_DATA`；sideways validation=`MIXED`；recovery lag status=`LOW`；
  watch recommended_action=`continue_observation`，forward_confirmation_status=`IN_PROGRESS`。
  这些结果支持继续观察，不支持 promotion、official target weights、broker/order 或
  production change。

## 验证记录

2026-06-13 latest smoothed evidence chain:

- attribution: `smoothed-review-attribution_75ec54d7e572038d`
- benefit / lag drilldown: `smoothing-benefit-lag_ea3a057745a3f0cd`
- regime validation: `smoothed-regime-validation_3fd897c7c66b3c40`
- confirmation targets: `smoothed-confirmation_0753b4cfbe5a2777`
- watch pack: `smoothed-watch-pack_520686f9c6924a84`
- Reader Brief latest snapshot date: `2026-06-12`
- data quality: `PASS_WITH_WARNINGS`，错误数 0，警告数 1

Validation passed:

- `aits validate-data`
- `aits etf dynamic-v3-rescue smoothed-review-attribution run/report`
- `aits etf dynamic-v3-rescue smoothing-benefit-lag run/report`
- `aits etf dynamic-v3-rescue smoothed-regime-validation run/report`
- `aits etf dynamic-v3-rescue smoothed-confirmation register/report`
- `aits etf dynamic-v3-rescue smoothed-watch-pack run/report`
- `aits etf dynamic-v3-rescue validate-smoothed-review-attribution`
- `aits etf dynamic-v3-rescue validate-smoothing-benefit-lag`
- `aits etf dynamic-v3-rescue validate-smoothed-regime-validation`
- `aits etf dynamic-v3-rescue validate-smoothed-confirmation`
- `aits etf dynamic-v3-rescue validate-smoothed-watch-pack`
- `aits etf dynamic-v3-rescue validate`
- `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`
- `aits reports index --date 2026-06-12`
- `aits reports index --date 2026-06-13`
- `aits reports reader-brief --latest`
- `aits reports validate-reader-brief --latest`
- `aits docs report-contract --as-of 2026-06-13`
- `python -m pytest tests/test_smoothed_review_attribution.py tests/test_smoothing_benefit_lag.py tests/test_smoothed_regime_validation.py tests/test_smoothed_confirmation.py tests/test_smoothed_watch_pack.py -q`
- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `git diff --check`
- `python -m pytest tests -q` -> `2415 passed, 640 warnings`

`aits reports index` 仍为 `PASS_WITH_WARNINGS`，原因是当前 registry 中既有
missing/stale artifacts；本任务新增 report ids 已通过 documentation contract 覆盖检查。
按 `2026-06-13` 直接生成 Reader Brief 时，现有 CLI 因缺少
`decision_snapshot_2026-06-13.json` 正常拒绝；`--latest` 使用最新 snapshot
`2026-06-12`，并在刷新同日 report index 后正确读取
`Dynamic Rescue Smoothed Method Watch` section。
