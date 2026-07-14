# TRADING-266 to 270 Smoothed Forward Evidence Operations and Primary Candidate Readiness

最后更新：2026-07-14

## 状态

BASELINE_DONE（ARCH-004G2.4CR=`COMPLETE_G2_4_CONTINUES`；2026-06-13 DONE baseline 已被
G2.4CP/CQ 的 source-backed null-candidate/0-target 结论 supersede；独立 forward/PIT/DQ/
cost/holdout 证据仍待后续任务积累）

## 背景

2026-06-13 baseline 曾把 `smooth_weights_3d_limited_adjustment` 推进到
`ELIGIBLE_FOR_OWNER_APPROVAL` 并固定创建 3 个 forward targets。该前提已被 2026-07-14
G2.4CP/CQ 的 source-backed chain supersede：当前 candidate=null、Gate=
`CONTINUE_OBSERVATION`、Binding=`targets=[]/NOT_REGISTERED`、Switch proposed=null、Owner
推荐 `request_more_forward_data`。旧 operations consumer 仍固定 3d、强制 3 targets、回填
10/5/5 requirements，并使用 shallow validator；这些是本 slice 必须清除的审计发现。

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
|TRADING-266|Smoothed Forward Progress Tracker|BASELINE_DONE|使用 v2 snapshot；只跟踪 Binding 真实 targets，0 targets 保持 `NOT_REGISTERED`，不得固定 3d 或补造 10/5/5 requirements。|
|TRADING-267|Smoothed Weekly Evidence Dashboard|BASELINE_DONE|从 validated Progress 重建；candidate=null、0 targets 和 not-registered 状态逐级保持。|
|TRADING-268|Sideways / Recovery Event Accumulation Monitor|BASELINE_DONE|只消费同 Progress lineage 的真实 classified events；无 candidate/target 时 inventory 为空且状态 `NOT_REGISTERED`。|
|TRADING-269|Primary Candidate Switch Readiness Recheck|BASELINE_DONE|严格验证 Dashboard/Monitor/Switch exact lineage；无 candidate 时不得进入 owner review，始终不执行 switch。|
|TRADING-270|Owner Decision Renewal Pack|BASELINE_DONE|严格验证 Recheck/Owner journal；candidate=null 时 promote option 不可用，推荐动作来自当前 evidence。|

## Forward Progress Calculation

Forward Progress 只从 validated CQ Binding 的真实 `targets` 初始化。每个 target 的
`target_id`、candidate、evidence type、required count/window 与 status 均逐字段保持；本层不按
method name 注入 requirement，也不把旧 10/5/5 baseline 当作缺省值。只有调用方显式提供
`outcome_update_ids` / `classification_ids` 时才读取对应 artifacts，并要求 candidate、target、
Binding/Progress lineage exact match；不扫描目录聚合不相关 evidence。

当前 Binding=`targets=[]/NOT_REGISTERED`，所以 candidate=null、target rows、requirements、
updated outcomes 与 classified events 均为空，required/available 全为 0，Progress 保持
`NOT_REGISTERED` 并推荐 `request_more_forward_data`。这不是“0/10 尚未完成”，而是当前没有
可合法跟踪的候选 target。

## Weekly Dashboard Reading

Weekly Dashboard 只从 validated Progress 的 bounded business views 重建，不重新解释 CQ
Gate/Owner 或固定候选。当前输出 candidate/current owner/gate/confidence 均为 null、
`forward_confirmation_status=NOT_REGISTERED`、`ready_for_switch_recheck=false`、
`weekly_recommendation=request_more_forward_data`、target table 为空；broker/production safety
始终显式。

## Event Monitor

Event Monitor 分别输出 `sideways_event_inventory.jsonl` 与
`recovery_event_inventory.jsonl`，但只消费 Progress 已冻结的 exact-lineage classification
commitments，不再次扫描 classification 目录。candidate=null 时 inventory 和 requirements
为空、sideways/recovery status=`NOT_REGISTERED`、recommended action=
`request_more_forward_data`。后续真实 candidate 存在时，lag warning 才按同 target lineage 进入
readiness 与 owner renewal，不能跨候选拼接。

## Switch Readiness Recheck

Readiness Recheck 消费 validated Dashboard、Monitor 和 CQ Switch，要求 Dashboard/Monitor
指向同一 Progress，且 candidate 与 Switch proposed candidate exact match。criteria 只能从
真实 Progress targets/status/events 计算，不固定 forward=10 或 sideways=5。当前无候选时
`recheck_decision=NO_ELIGIBLE_CANDIDATE`、criteria 为空、hard blocker=
`no_eligible_candidate`、warnings 为空、`owner_decision_required=false`；
`can_execute_switch=false` 与 `auto_switch=false` 对所有分支固定。

## Owner Renewal Pack

Owner Renewal Pack 汇总 validated Recheck 与同 candidate 的 Owner journal，并输出可用性受证据
约束的 owner actions：

- `continue_observation`
- `request_more_forward_data`
- `promote_to_primary_research_candidate`
- `defer`
- `reject`

当前 candidate=null、Recheck=`NO_ELIGIBLE_CANDIDATE`，因此推荐
`request_more_forward_data`；该 option available/recommended=true，`continue_observation`
不推荐，`promote_to_primary_research_candidate` 明确 `available=false`。即使未来具备候选且
option 可用，本阶段仍不执行 primary candidate switch。

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

## G2.4CR Canonical Exit Contract

1. 15 callbacks 迁独立 canonical interface/domain；legacy root 对应 decorators/callbacks 与
   domain business logic 删除，只保留 lazy compatibility wrappers。
2. Progress/Dashboard/Monitor/Recheck/Renewal 分别冻结 versioned `*.v2` input snapshot；任何
   正式写件前验证 live upstream sources、generated cutoff、chronology 和 exact lineage。
3. Candidate 与 targets 只能来自 validated CQ Binding/Switch/Owner chain；candidate=null 时
   target rows、requirements、events 与 switch criteria 均不得用旧 3d/10/5/5 baseline 补造。
4. Progress 只接受与 Binding targets exact match 的 outcome/classification evidence；Dashboard
   与 Monitor 只能投影同一 Progress；Recheck 必须要求 Dashboard/Monitor 同 Progress 且与
   CQ Switch candidate exact match；Renewal 必须要求 Recheck/Owner journal candidate exact match。
5. 所有 status、counts、criteria、options、Markdown、checklist 与 Reader Brief 从 snapshot
   content 计算；missing/null/0-target 保持显式，workflow PASS 不得解释为 candidate readiness。
6. Validator 重验 live sources，并从 immutable snapshot 逐 byte 重建全部 JSON/JSONL/
   Markdown/Reader Brief；source/snapshot/output tamper、future/cross-lineage/duplicate/non-finite
   evidence 全部 fail closed。
7. 全链保持 current-definition/not-PIT、manual research/paper-shadow only、no official/no
   auto/no order/no broker，`production_effect=none`；G2.4CR 单 slice 完成不触发 phase-level
   handoff，也不进入 G2.5。
8. 消除同一 validation session 内对未变化 artifact DAG 的重复递归验证。缓存键必须至少包含
   validator identity、artifact absolute path 与完整文件内容指纹；任一 source/snapshot/output
   byte 变化必须 cache miss 并重新验证，不能以跳过 source gate、固定 fixture 或关闭 byte
   rebuild 换取速度。以 `test_smoothed_forward_progress.py` 的优化前 `557.27s` 为基线，记录
   优化后同测试耗时与降幅，并继续通过 tamper fail-closed 测试。

## Progress Notes

- 2026-07-14: G2.4CR=`COMPLETE_G2_4_CONTINUES`。15 callbacks 与旧业务实现已迁
  canonical operations interface/domain；五类 bounded v2 snapshots、pre-output live source/
  cutoff validation、Binding-only candidate/targets、explicit evidence ids、exact Progress/
  candidate lineage、null/0-target/`NOT_REGISTERED`、content-derived all views 与 byte rebuild
  闭合。当前 Progress/Dashboard/Monitor=`NOT_REGISTERED`，Recheck=
  `NO_ELIGIBLE_CANDIDATE`，Renewal promote unavailable且建议`request_more_forward_data`；
  workflow PASS 不构成投资结论。研发效率基准`557.27s→13.60s`（-97.56%/40.98x），
  最大snapshot`633.06MB→9.49MB`（约-98.5%）。Focused/CLI baseline/architecture/contract=
  `147/120/279/203 passed`；generated=`922 modules/1,122 tests/858 writers/0 violations`。
  单slice不触发ARCH-005 handoff，继续剩余G2.4，不进入G2.5，`production_effect=none`。
- 2026-07-14: 研发耗时优化完成第一阶段验证。相同
  `tests/test_smoothed_forward_progress.py` 从 `557.27s` 降至 session-cache-only 的
  `122.66s`，最终在 bounded source 修复后降至 `13.60s`，总降幅 `97.56%`、约
  `40.98x`。分层 profile 证明 Backfill 首次计算仅约 `1.56s`，原主要成本是高扇入节点
  重复验证与 input snapshot 递归嵌套；旧最大 snapshot 为 `633.06MB`，新链最大为
  `9.49MB`，约减少 `98.5%`。实现通用 content-fingerprint-aware validation session，
  History/Hardening/Smoothed Method source binding 复用未变化 PASS；Evidence/Readiness/
  Promotion/Operations bounded bundle 不再嵌入上游 input snapshot，完整上游 snapshot 仍由
  live validator 重放。Confirmation 改用 validated Review/Regime manifests 的显式 lineage；
  Sideways 显式绑定 Smoothed/Baseline sources，不再穿透 Churn snapshot。独立 cache 单测
  `2 passed`；method/evidence/readiness/promotion hardening `25 passed in 244.46s`，policy/
  source/snapshot/output/render tamper 仍 fail closed。
- 2026-07-14: owner 要求在继续切片的同时评估研发耗时。聚焦 progress 集成测试 PASS，但
  单测耗时 `557.27s`（9m17s）。初步审计定位为完整 Smoothed artifact DAG 在 method /
  evidence / readiness / promotion / operations 多层被重复递归验证；既有 validation session
  cache 只覆盖 promotion/operations，无法复用早期 source PASS。决定抽取通用、content-
  fingerprint-aware 的 session cache 并贯通 source binding/validator；缓存仅在同一会话内生效，
  artifact byte 改变即失效，生产校验强度与 tamper fail-closed 不变。实现后用同一测试做
  before/after benchmark，再继续 G2.4CR 原验证计划。
- 2026-07-14: G2.4CR contract freeze 并进入 `IN_PROGRESS`。范围为 TRADING-266～270 的
  Progress/Dashboard/Monitor/Recheck/Renewal 共 15 callbacks，迁独立 canonical operations
  interface/domain。审计确认旧链不做 pre-output upstream validation、没有 immutable input
  snapshot、可跨 lineage 扫描 outcome/classification，并固定 3d candidate、3 targets 与
  10/5/5 requirements；validators 只检查文件/数量/少量安全字段，不能重放来源或全部 views。
  Exit 固定五类 bounded v2 snapshots、live source/cutoff、exact lineage、null/0-target/
  NOT_REGISTERED preservation、content-derived views 与 byte rebuild。当前 CQ source truth 为
  candidate=null/0 target，不得以旧 DONE artifact 回填；不进入 G2.5，
  `production_effect=none`。
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
