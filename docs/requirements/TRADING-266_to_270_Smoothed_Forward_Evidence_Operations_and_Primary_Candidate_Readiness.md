# TRADING-266 to 270 Smoothed Forward Evidence Operations and Primary Candidate Readiness

最后更新：2026-06-13

## 状态

DONE

## 背景

TRADING-261 to 265 已经把 `smooth_weights_3d_limited_adjustment` 推进到
`ELIGIBLE_FOR_OWNER_APPROVAL`，但 owner decision 仍为 `continue_observation`。
前置 artifacts 明确要求继续跟踪 forward confirmation、sideways_choppy 样本和
recovery lag watch，且不得自动切换 primary candidate。

本阶段把 smoothed forward evidence 纳入 weekly operations / readiness recheck /
owner decision renewal 闭环。所有产物仍是 paper-shadow / research-only，不写
official target weights，不修改 `position_advisory_v1.yaml`，不触发 broker 或
production。

## 目标

1. 基于 `smoothed-forward-binding_2334059295c625ec` 跟踪 forward progress。
2. 生成 weekly evidence dashboard，汇总 forward progress、owner decision 和 safety。
3. 维护 sideways / recovery event accumulation monitor。
4. 基于 dashboard 和 monitor 重新检查是否应再次提交 owner review。
5. 生成 owner decision renewal pack，支持 owner 周期性更新继续观察或后续动作。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-266|Smoothed Forward Progress Tracker|DONE|`smoothed-forward-progress update/report` 和 `validate-smoothed-forward-progress` 可运行，输出每个 target 的 progress_status、summary 和 Reader Brief section。|
|TRADING-267|Smoothed Weekly Evidence Dashboard|DONE|`smoothed-weekly-dashboard build/report` 和 `validate-smoothed-weekly-dashboard` 可运行，输出 dashboard summary、target status table 和 Reader Brief section。|
|TRADING-268|Sideways / Recovery Event Accumulation Monitor|DONE|`smoothed-event-monitor update/report` 和 `validate-smoothed-event-monitor` 可运行，输出 sideways/recovery inventories 和 accumulation summary。|
|TRADING-269|Primary Candidate Switch Readiness Recheck|DONE|`smoothed-switch-readiness recheck/report` 和 `validate-smoothed-switch-readiness` 可运行，criteria 可读且 `can_execute_switch=false`。|
|TRADING-270|Owner Decision Renewal Pack|DONE|`smoothed-owner-renewal pack/report` 和 `validate-smoothed-owner-renewal` 可运行，owner options、checklist、Reader Brief section 可读。|

## Forward Progress Calculation

Forward progress 从 smoothed forward binding 中的 bound targets 初始化：

- `smooth_3d_vs_limited` 需要 10 个 forward events；
- `smooth_3d_sideways_choppy_improvement` 需要 5 个 sideways events；
- `smooth_3d_recovery_lag_watch` 需要 5 个 recovery events，且保持 watch-only 语义。

当前阶段不从 cached market data 重算价格或收益，不读取 broker，不补造 forward
events。没有可审计 forward samples 时，available events 维持 0，progress_status 不能变成
`READY_FOR_REVIEW`。后续若有 forward event ledger，可在同一 artifact contract 下补充
available counts 和 metrics。

## Weekly Dashboard Reading

Weekly dashboard 只汇总已有 progress、gate/owner 状态和 safety boundary：

- `forward_confirmation_status=IN_PROGRESS` 表示 forward evidence 尚未满足；
- `ready_for_switch_recheck=false` 表示不能把 readiness 等同于 switch；
- `weekly_recommendation=continue_observation` 是当前 owner-facing operational action；
- `broker_action_allowed=false` 和 `production_effect=none` 必须在摘要中可见。

## Event Monitor

Event monitor 分别输出 `sideways_event_inventory.jsonl` 和
`recovery_event_inventory.jsonl`。当前无新增样本时 inventory 文件可以为空，但 summary
必须显示 required/available/pending/progress_pct，并把 recommended action 保持为
`continue_event_collection`。

Sideways 样本用于验证 smoothing 是否改善 churn；recovery 样本用于检查 smoothing 是否
造成 risk-on response lag。任何 lag warning 都必须进入 readiness criteria 和 owner
renewal pack，不能静默忽略。

## Switch Readiness Recheck

Readiness recheck 消费 weekly dashboard、event monitor 和既有 paper-shadow switch plan。
它只回答是否应再次向 owner 提出 review，不执行 switch。

Criteria 包括：

- forward events 是否达到 10；
- sideways events 是否达到 5；
- recovery lag watch 是否没有 high lag warning。

只要 forward 或 sideways 样本不足，recheck 维持 `WAIT_FOR_MORE_FORWARD_DATA` 或
`CONTINUE_OBSERVATION`。`can_execute_switch` 固定为 false，`owner_decision_required` 固定为
true，`auto_switch` 固定为 false。

## Owner Renewal Pack

Owner renewal pack 汇总前次 owner decision、当前 recheck decision 和可选 owner actions：

- `continue_observation`
- `request_more_forward_data`
- `promote_to_primary_research_candidate`
- `defer`
- `reject`

当前 evidence 不足时推荐 `continue_observation`。`promote_to_primary_research_candidate`
只在 readiness recheck 返回 `READY_FOR_OWNER_REVIEW` 后才适合作为 owner 选项；即便 owner
后续选择 promotion，本阶段也不执行 primary candidate switch。

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

Readiness 不等于 automatic switch。Owner renewal pack 是人工复核材料，不是 order ticket、
broker instruction、production target weight update 或 `position_advisory_v1.yaml` mutation。

## Implementation Plan

1. 新增 five-stage artifact builders、payload readers、Markdown renderers 和 validators。
2. 新增 CLI：`smoothed-forward-progress`、`smoothed-weekly-dashboard`、
   `smoothed-event-monitor`、`smoothed-switch-readiness`、`smoothed-owner-renewal`。
3. 接入 report registry、Reader Brief、artifact catalog、system flow、README 和 operations
   runbook。
4. 新增 focused tests 覆盖 progress、dashboard、event monitor、readiness、owner renewal、
   safety invariants 和 Reader Brief integration。
5. 跑通真实链路并执行 validators、ruff、compileall、diff check 和必要 pytest。

## Progress Notes

- 2026-06-13: 新增需求文档并进入 IN_PROGRESS；本阶段建立 smoothed forward evidence
  operations 和 primary candidate readiness recheck 闭环，仍不自动切换、不写 official
  target weights、不触发 broker/production。
- 2026-06-13: 实现完成并归档为 DONE；真实链路输出 progress
  `smoothed-forward-progress_043ae715c38b1bca`、dashboard
  `smoothed-weekly-dashboard_9c87f68487f79831`、monitor
  `smoothed-event-monitor_3d4f7f46618f8c2a`、recheck
  `smoothed-switch-readiness_3a6efe3f119a1177`、renewal
  `smoothed-owner-renewal_098a277c055fad3c`。当前 forward/sideways/recovery evidence
  仍为 0/10、0/5、0/5，`ready_for_switch_recheck=false`，
  `recheck_decision=WAIT_FOR_MORE_FORWARD_DATA`，`can_execute_switch=false`，
  recommended owner action 保持 `continue_observation`。
- 2026-06-13: 验证通过 dynamic-v3 rescue validation、five artifact validators、family
  artifact validation、focused pytest、documentation contract / Reader Brief pytest、ruff、
  compileall、git diff check 和 full pytest `2426 passed, 640 warnings`；所有新增 artifacts
  继续固定 no broker / no order / no official target weights / no production effect。
