# TRADING-256 to 260 Smoothed Evidence Completeness and Promotion Readiness

最后更新：2026-06-13

## 状态

VALIDATING

## 背景

TRADING-246 to 250 已经实现 `smooth_weights_3d_limited_adjustment` 和
`smooth_weights_5d_limited_adjustment` research-only methods。TRADING-251 to 255
已经生成 smoothed review attribution、benefit / lag drilldown、regime validation、
forward confirmation targets 和 weekly watch pack。

当前真实 artifacts：

- review attribution: `smoothed-review-attribution_75ec54d7e572038d`
- benefit / lag: `smoothing-benefit-lag_ea3a057745a3f0cd`
- regime validation: `smoothed-regime-validation_3fd897c7c66b3c40`
- confirmation: `smoothed-confirmation_0753b4cfbe5a2777`
- watch pack: `smoothed-watch-pack_520686f9c6924a84`
- smoothed backfill: `smoothed-backfill_27939e31bfdf54c6`
- baseline backfill: `paper-shadow-backfill_2138461d25e686e0`
- risk-capped backfill: `risk-capped-backfill_3d41bb93e038bbe4`

当前 watch 结论仍是 `CONTINUE_OBSERVATION` / `LOW` confidence，主要 blocker 为
`benefit_lag_tradeoff=INSUFFICIENT_DATA`、`sideways_validation=MIXED` 和
`forward_confirmation_status=IN_PROGRESS`。

## 目标

本阶段补齐 smoothed method 当前缺失的关键证据，并给 owner 一个可审计的
promotion readiness review 输入：

1. 拆解 `benefit_lag_tradeoff=INSUFFICIENT_DATA` 的具体原因。
2. 直接量化 `signal_churn`、`weight_jump`、`direction_flip` 和 turnover 变化。
3. 解释 `sideways_validation=MIXED` 的 window-level 原因。
4. 生成 `smooth_weights_3d_limited_adjustment` vs
   `smooth_weights_5d_limited_adjustment` readiness scorecard。
5. 生成 owner review update 和 Reader Brief section。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-256|Benefit / Lag Missing Evidence Diagnosis|VALIDATING|`smoothed-evidence-gap run/report` 和 `validate-smoothed-evidence-gap` 可运行，输出 missing evidence matrix、gap reason summary 和 metric backfill plan。|
|TRADING-257|Signal Churn / Weight Jump Metric Backfill|VALIDATING|`smoothed-churn-backfill run/report` 和 `validate-smoothed-churn-backfill` 可运行，输出 method-level churn metrics、weight jump events、direction flip events 和 churn reduction summary。|
|TRADING-258|Sideways Mixed Result Attribution|VALIDATING|`sideways-mixed-attribution run/report` 和 `validate-sideways-mixed-attribution` 可运行，输出 sideways window outcomes、mixed reason summary 和 3d vs 5d breakdown。|
|TRADING-259|Smoothed 3d vs 5d Promotion Readiness Scorecard|VALIDATING|`smoothed-readiness-scorecard run/report` 和 `validate-smoothed-readiness-scorecard` 可运行，输出 3d/5d scorecard 和 promotion readiness decision。|
|TRADING-260|Smoothed Method Owner Review Update|VALIDATING|`smoothed-owner-review-update run/report` 和 `validate-smoothed-owner-review-update` 可运行，输出 owner decision options、checklist、report 和 Reader Brief section。|

## Pilot Baselines And Governance

Readiness scorecard 的初始权重来自 owner 任务说明，作为可审计 pilot baseline：

- return preservation: 20%
- drawdown impact: 15%
- turnover reduction: 15%
- weight jump reduction: 15%
- signal churn reduction: 15%
- sideways behavior: 10%
- recovery lag: 5%
- forward confirmation readiness: 5%

硬阻断条件：

- return preservation = `POOR`
- recovery lag = `HIGH`
- sideways status = `WORSE`
- forward confirmation status = `FAILED`
- data quality = `FAIL`

这些规则只服务 research review，不是 production promotion gate。退出条件：积累足够
forward confirmation evidence 后，由 owner review 将 pilot baseline 替换为
evidence-backed calibration，或明确拒绝 smoothed method。

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

Promotion readiness 不等于 production approval。即使 scorecard 输出
`PROMOTE_FOR_REVIEW`，也只表示 `PROMOTE_FOR_RESEARCH_REVIEW` 输入，不得写 official
target weights、修改 `position_advisory_v1.yaml`、触发 broker/order、改变真实仓位或自动
设置默认执行规则。

## Implementation Plan

1. 基于 benefit / lag、regime validation 和 watch pack 生成 evidence gap diagnosis。
2. 基于 smoothed / baseline / risk-capped backfill state history 计算 churn、weight jump
   和 direction flip metrics。
3. 基于 regime validation 与 churn metrics 输出 sideways mixed attribution。
4. 聚合 attribution、benefit / lag、churn、sideways attribution 和 confirmation targets，
   生成 readiness scorecard。
5. 基于 scorecard 和 watch pack 生成 owner review update、checklist 和 Reader Brief
   section。
6. 更新 README、operations runbook、system flow、report registry、artifact catalog、
   task register 和本需求文档。
7. 新增 focused tests，运行 validators、ruff、compileall、diff check 和必要真实链路。

## Progress Notes

- 2026-06-13: 新增需求文档并进入 IN_PROGRESS；本阶段只补 evidence completeness 和
  promotion readiness review，不新增 target method、不自动 promotion、不改变 production。
- 2026-06-13: baseline 实现完成并转入 VALIDATING。真实链路输出 evidence gap
  `smoothed-evidence-gap_158ecc5c059c38c1`、churn backfill
  `smoothed-churn-backfill_058180ef572eddc4`、sideways attribution
  `sideways-mixed-attribution_46cc74a75b39c60a`、readiness scorecard
  `smoothed-readiness-scorecard_f313eff7fe1d04fb`、owner review update
  `smoothed-owner-review-update_e7219838a9e64226`。Gap diagnosis 显示
  `tradeoff_can_be_resolved_by_backfill=true` 且 `requires_forward_data=true`；
  direct churn best method 为 `smooth_weights_5d_limited_adjustment`，但 3d churn
  reduction status 为 `STRONG`；sideways dominant reason 为
  `churn_reduction_helped`，recommendation 为 `prefer_3d_over_5d`；readiness
  decision 为 `PROMOTE_FOR_REVIEW`、confidence=`LOW`；owner update 推荐
  `continue_observation`，原因是 forward confirmation 仍在进行中。
- 2026-06-13: 五个新增 validate CLI、report CLI、`aits validate-data`
  (`PASS_WITH_WARNINGS`，0 error / 1 warning)、`aits etf dynamic-v3-rescue
  validate`、`artifacts validate --family dynamic_v3_rescue`、documentation
  contract、focused pytest、ruff、compileall、git diff check、report index、Reader
  Brief latest/quality 和 full pytest `2416 passed, 640 warnings` 已通过。Full
  pytest 首次在 documentation contract warning 处失败，补齐 artifact catalog
  schema/status terms 后恢复 PASS。
