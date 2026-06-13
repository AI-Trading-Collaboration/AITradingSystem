# TRADING-261 to 265 Smoothed Method Promotion Review and Primary Candidate Gate

最后更新：2026-06-13

## 状态

DONE

## 背景

TRADING-256 to 260 已经把 `smooth_weights_3d_limited_adjustment` 推进到
`PROMOTE_FOR_REVIEW`，但 scorecard confidence 仍为 `LOW`，forward confirmation
仍是 `IN_PROGRESS`，owner update 的推荐动作仍是 `continue_observation`。

本阶段把该状态推进为正式 promotion review gate，并只在 paper shadow / research
scope 内判断是否具备成为 primary research candidate 的 owner approval 资格。

## 目标

1. 生成 smoothed promotion review pack，解释 supporting evidence、blocking issues
   和 owner review 边界。
2. 运行 primary research candidate gate，判断 `smooth_weights_3d_limited_adjustment`
   是否 eligible for owner approval。
3. 将 smoothed forward confirmation targets 绑定到 weekly progress / dashboard /
   rule review queue 观察周期。
4. 生成 paper shadow primary research candidate switch plan，但不自动切换。
5. 记录 owner promotion decision journal，支持 continue / promote / defer / reject /
   request-more-forward-data。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-261|Smoothed Promotion Review Pack|DONE|`smoothed-promotion-review pack/report` 和 `validate-smoothed-promotion-review` 可运行，输出 evidence summary、blocking issues、report 和 Reader Brief section。|
|TRADING-262|Primary Research Candidate Gate|DONE|`primary-research-candidate-gate run/report` 和 `validate-primary-research-candidate-gate` 可运行，输出 gate decision 和 criteria results。|
|TRADING-263|Forward Confirmation Progress Binding|DONE|`smoothed-forward-binding run/report` 和 `validate-smoothed-forward-binding` 可运行，输出 bound confirmation targets、progress requirements 和 Reader Brief section。|
|TRADING-264|Paper Shadow Primary Candidate Switch Plan|DONE|`paper-shadow-primary-switch plan/report` 和 `validate-paper-shadow-primary-switch` 可运行，输出 switch plan、安全检查和 Reader Brief section。|
|TRADING-265|Owner Promotion Decision Journal|DONE|`smoothed-owner-promotion create/record/report` 和 `validate-smoothed-owner-promotion` 可运行，记录 owner decision 但不自动切换。|

## Promotion Review Boundary

`PROMOTE_FOR_REVIEW` 只表示证据足以进入 owner / research review，不表示：

- 自动成为 primary method；
- 自动写入 official target weights；
- 自动进入 production；
- 自动触发 broker；
- 自动修改真实仓位或 `position_advisory_v1.yaml`。

本阶段最多允许生成 paper shadow / research scope 的 primary candidate plan 和 owner
decision journal。即使 owner 后续选择 promotion，本阶段命令也只记录或规划，不执行切换。

## Primary Research Candidate Gate

Primary gate 的范围固定为 `paper_shadow_research_only`。Gate criteria 包括：

- promotion review decision 必须是 `PROMOTE_FOR_REVIEW`；
- churn reduction 必须是 `STRONG` 或 `MODERATE`；
- recovery lag 必须是 `LOW` 或 `MEDIUM`；
- forward confirmation 可以是 `IN_PROGRESS` 或 `PASS`，但 `IN_PROGRESS` 只能是 warning；
- production safety 必须是 `NO_PRODUCTION`。

`ELIGIBLE_FOR_OWNER_APPROVAL` 不是 automatic promotion。它只表示 owner 可以在后续人工决策中批准 paper shadow primary research candidate。

## Forward Confirmation Binding

Smoothed forward binding 把既有 `smoothed-confirmation` targets 显式连接到 weekly
evidence / dashboard / rule review queue 的观察语义：

- `smooth_3d_vs_limited` 需要 10 个 forward events；
- `smooth_3d_sideways_choppy_improvement` 需要 5 个 sideways events；
- `smooth_3d_recovery_lag_watch` 需要 5 个 recovery events，状态为 watch-only。

这些样本 floors 来自 owner-requested pilot baseline；退出条件是积累足够 forward evidence 后，由 owner review 替换为 evidence-backed calibration 或关闭该候选。

## Switch Plan Boundary

Paper shadow switch plan 只说明如果 owner 批准，research section 可以把
`paper_shadow_primary_research_candidate` 从 `limited_adjustment` 切换到
`smooth_weights_3d_limited_adjustment`。该 plan 不修改 official target weights、不修改真实组合、不生成 order ticket、不调用 broker。

Rollback method 固定为 `limited_adjustment`，直到 owner 复核出新的 baseline。

## Owner Promotion Decision Journal

Owner decision 支持：

- `pending`
- `continue_observation`
- `promote_to_primary_research_candidate`
- `defer`
- `reject`
- `request_more_forward_data`

`promote_to_primary_research_candidate` 只允许后续任务执行 paper-shadow research candidate switch；本阶段 record 命令仍不自动切换、不写 official target weights、不触发 broker/production。

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

## Implementation Plan

1. 基于 readiness scorecard、owner update 和 watch pack 生成 promotion review pack。
2. 基于 promotion review pack 运行 primary research candidate gate。
3. 基于 smoothed confirmation artifact 和 gate decision 生成 forward binding。
4. 基于 gate 和 binding 生成 paper shadow primary switch plan。
5. 基于 promotion review、gate 和 switch plan 创建 / 记录 owner promotion decision。
6. 更新 README、operations runbook、system flow、report registry、artifact catalog、
   task register 和 Reader Brief。
7. 新增 focused tests，运行 validators、ruff、compileall、diff check 和整体验收链路。

## Progress Notes

- 2026-06-13: 新增需求文档并进入 IN_PROGRESS；本阶段只建立 promotion review /
  paper-shadow primary candidate gate / forward binding / switch plan / owner decision
  journal，不自动 promotion、不写 official target weights、不改变 production。
- 2026-06-13: DONE。实际运行链路产出
  `smoothed-promotion-review_a057c47f05f5eec5`、
  `primary-research-candidate-gate_68e71ad6a55f41c3`、
  `smoothed-forward-binding_2334059295c625ec`、
  `paper-shadow-primary-switch_cb49f5b23bee2270` 和
  `smoothed-owner-promotion_83918219eb47a95a`；owner decision 已记录为
  `continue_observation`，`paper_shadow_primary_candidate_change_allowed=false`，
  `actual_switch_executed=false`。验证通过 focused tests、全量 pytest、Ruff、
  compileall、documentation contract、dynamic-v3 rescue validation、
  five artifact validators 和 family artifact validation；data quality gate 为
  `PASS_WITH_WARNINGS`，错误数 0、警告数 1。所有新增产物仍为 paper-shadow /
  research-only，`production_effect=none`。
