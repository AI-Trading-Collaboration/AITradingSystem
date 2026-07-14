# TRADING-261 to 265 Smoothed Method Promotion Review and Primary Candidate Gate

最后更新：2026-07-14

## 状态

BASELINE_DONE（ARCH-004G2.4CQ canonical migration、正式 architecture/contract gate、
manifests/deprecation/source-hash closeout 已完成；投资结论仍等待独立 forward/PIT/DQ/cost/
holdout 证据与 owner review）

## 背景

2026-06-13 baseline 曾把 `smooth_weights_3d_limited_adjustment` 固定为 candidate 并输出
`PROMOTE_FOR_REVIEW`。该结论已被 2026-07-14 G2.4CP source-backed evidence supersede：
当前 Scorecard=`CONTINUE_OBSERVATION/INSUFFICIENT_EVIDENCE`、
`candidate_method=null`、Confirmation=`INSUFFICIENT_EVIDENCE/0 targets`、Owner action=
`request_additional_evidence`。旧 CQ consumer 仍会在 null 时回填 3d/5d、写静态 supporting
evidence、补造三个 forward targets，并让 validator 只接受 promote 路径；这些是本 slice
必须清除的审计发现，不再代表有效业务语义。

本阶段把该状态推进为正式 promotion review gate，并只在 paper shadow / research
scope 内判断是否具备成为 primary research candidate 的 owner approval 资格。

## 目标

1. 生成 smoothed promotion review pack，解释 supporting evidence、blocking issues
   和 owner review 边界。
2. 运行 primary research candidate gate，仅判断上游 Confirmation/Scorecard 已确认的
   evidence-backed candidate 是否 eligible for owner approval；不得固定具体方法。
3. 将 smoothed forward confirmation targets 绑定到 weekly progress / dashboard /
   rule review queue 观察周期。
4. 生成 paper shadow primary research candidate switch plan，但不自动切换。
5. 记录 owner promotion decision journal，支持 continue / promote / defer / reject /
   request-more-forward-data。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-261|Smoothed Promotion Review Pack|BASELINE_DONE|`smoothed-promotion-review pack/report` 和 validator 使用 v2 snapshot；candidate/null、supporting evidence、blockers 全部由 validated upstream content 重建。|
|TRADING-262|Primary Research Candidate Gate|BASELINE_DONE|Gate 不创造 candidate；candidate=null 或 readiness 非 promote 时显式 `CONTINUE_OBSERVATION`，artifact workflow 仍可验证为 PASS。|
|TRADING-263|Forward Confirmation Progress Binding|BASELINE_DONE|只复制 Confirmation 中真实登记的 targets；0 targets 保持空并输出 `NOT_REGISTERED`，不得补造固定 3d target ids。|
|TRADING-264|Paper Shadow Primary Candidate Switch Plan|BASELINE_DONE|candidate=null 时 proposed candidate=null、switch forbidden；current/rollback method 来自 reviewed policy。|
|TRADING-265|Owner Promotion Decision Journal|BASELINE_DONE|Create/record 都从 snapshot 重建；无 eligible candidate 时不得记录 promote decision，journal 始终不执行切换。|

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

Primary gate 的范围由 reviewed promotion policy 固定为 `paper_shadow_research_only`。Gate
criteria 必须逐项披露 candidate authority、readiness、实际 churn/recovery/forward evidence 和
safety，而不能写静态 PASS。Gate criteria 包括：

- promotion review decision 必须是 `PROMOTE_FOR_REVIEW`；
- churn reduction 必须是 `STRONG` 或 `MODERATE`；
- recovery lag 必须是 `LOW` 或 `MEDIUM`；
- candidate 必须来自 Confirmation 并与 Scorecard/Owner/Watch exact match；
- forward confirmation 的 warning/eligible 状态由 reviewed policy 管理；
- production safety 必须是 `NO_PRODUCTION`。

`ELIGIBLE_FOR_OWNER_APPROVAL` 不是 automatic promotion。它只表示 owner 可以在后续人工决策中批准 paper shadow primary research candidate。

## Forward Confirmation Binding

Smoothed forward binding 把既有 `smoothed-confirmation` targets 原样连接到 weekly
evidence / dashboard / rule review queue 的观察语义：

- target id、candidate method、window、sample floor 和 status 均来自 validated
  Confirmation artifact；
- Confirmation 为 0 targets 时 Binding 必须保持 `targets=[]`、`NOT_REGISTERED`；
- Binding 不允许凭 Gate 或方法名补造 forward/sideways/recovery targets。

这些样本 floors 来自 owner-requested pilot baseline；退出条件是积累足够 forward evidence 后，由 owner review 替换为 evidence-backed calibration 或关闭该候选。

## Switch Plan Boundary

Paper shadow switch plan 只说明如果 Gate 确有 eligible candidate 且 owner 后续批准，
research section 才可以提出从 reviewed current primary 切换到该 evidence-backed candidate。
candidate=null 时 proposed candidate 必须保持 null，不能形成 switch request。该 plan 不修改
official target weights、不修改真实组合、不生成 order ticket、不调用 broker。

Current primary、rollback method 和 owner-decision vocabulary 进入带 owner/version/rationale/
review condition 的 reviewed promotion policy，不再作为未治理 literal 隐藏在代码中。

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

## G2.4CQ Canonical Exit Contract

1. 16 callbacks 迁至独立 canonical interface/domain，legacy root 对应 decorators/callbacks 与
   domain business logic 删除，只保留 lazy compatibility wrappers。
2. Promotion/Gate/Binding/Switch/Owner 分别冻结 versioned `*.v2` input snapshot；producer
   正式写件前调用适用 upstream validator，校验 live source bytes、timezone chronology、
   exact Review/Comparison/Smoothed/Baseline/candidate lineage 和 reviewed policy。
3. Candidate authority 唯一来自 Confirmation，经 Scorecard/Owner/Watch/Promotion/Gate/
   Binding/Switch/Owner 逐级 exact match；null 永不回填，诊断排名永不创建 candidate。
4. Supporting evidence、criteria、blockers、bound targets、switch eligibility 和 owner record
   全部从 snapshot content 计算；missing/non-finite/insufficient 保持 null/insufficient，不能
   转 0、静态 PASS 或静态 evidence claim。
5. Validator 重验 live sources/policy，并从 snapshot 逐 byte 重建全部 JSON/Markdown/
   checklist/Reader Brief。合法的 `CONTINUE_OBSERVATION`/0-target/no-switch workflow 必须
   validation PASS；validator 不能把“必须 promote”当 artifact 完整性条件。
6. Owner `record` 是受控 journal mutation：先验证当前 snapshot/views，再以 record request
   原子重建同一 artifact；若 candidate/gate/switch 不允许 promotion，记录 promote decision
   必须 fail closed。
7. 全链保持 current-definition/not-PIT、manual research/paper-shadow only、no official/no
   auto/no order/no broker，`production_effect=none`；G2.4CQ 单 slice 完成不触发 phase-level
   handoff，也不进入 G2.5。

## Progress Notes

- 2026-07-14: G2.4CQ `COMPLETE_G2_4_CONTINUES`，TRADING-261～265 转
  `BASELINE_DONE`。16 callbacks 与旧业务实现已迁 canonical smoothed-promotion
  interface/domain；五类 bounded v2 snapshots、live source/policy replay、Confirmation-only
  candidate、exact lineage/chronology、content-derived views、null/0-target、Owner record 原子
  重建和 byte rebuild 均闭合。Focused hardening/five-stage=`7/5 passed`，正式
  architecture/contract=`278/203 passed`，generated=`919 modules/1,120 tests/858 writers/0
  violations`。当前 candidate=null、Gate=`CONTINUE_OBSERVATION`、Binding=
  `targets=[]/NOT_REGISTERED`、Switch proposed=null、Owner 推荐
  `request_more_forward_data`；这只证明链路可复算，不是 promotion 或投资结论。整个 G2.4
  继续，未触发 ARCH-005 handoff，也不进入 G2.5，`production_effect=none`。
- 2026-07-14: G2.4CQ implementation complete，进入 `VALIDATING`。16 callbacks 已迁 canonical
  smoothed-promotion interface/domain，legacy root 对应 callback/decorator 清零，旧领域只保留
  11 个 lazy compatibility wrappers。Promotion/Gate/Binding/Switch/Owner 五类 v2 snapshots、
  pre-output live upstream/policy validation、Confirmation-only candidate、exact lineage/
  chronology、content-derived criteria/targets/switch/journal、null/0-target preservation、Owner
  record 原子重建与全部 views byte rebuild 已闭合。Source binding 只冻结消费的 business views
  与 commitments，不递归内嵌上游 input snapshot；完整 upstream validator 与 tamper fail-closed
  仍保留。当前 fixture candidate=null、Promotion不可进入review、Gate=`CONTINUE_OBSERVATION`、
  Binding=`targets=[]/NOT_REGISTERED`、Switch proposed=null、Owner推荐
  `request_more_forward_data`；非法 promote record 被拒绝，合法 continue-observation 可 PASS。
  等待 focused/architecture/contract、manifests/deprecation/source hashes closeout；本 slice 不
  触发 phase-level handoff，G2.4 继续且不进入 G2.5，`production_effect=none`。
- 2026-07-14: G2.4CQ contract freeze 并进入 `IN_PROGRESS`。审计确认 16 callbacks、
  五类 producer 和 Owner record mutation；旧 fixed 3d/5d fallback、静态 supporting evidence/
  criteria、0-target 补造、candidate-null switch plan 和 promote-only validator 失效，按上述
  v2/exact-lineage/content-derived/candidate-authority 契约重建。
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
