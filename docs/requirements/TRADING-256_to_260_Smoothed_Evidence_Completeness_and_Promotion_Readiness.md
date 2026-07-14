# TRADING-256 to 260 Smoothed Evidence Completeness and Promotion Readiness

最后更新：2026-07-14

## 状态

BASELINE_DONE（G2.4CP canonical migration / readiness evidence hardening；G2.4 继续）

## 背景

TRADING-246 to 250 已经实现 `smooth_weights_3d_limited_adjustment` 和
`smooth_weights_5d_limited_adjustment` research-only methods。TRADING-251 to 255
已经生成 smoothed review attribution、benefit / lag drilldown、regime validation、
forward confirmation targets 和 weekly watch pack。

以下 2026-06-13 artifacts 仅保留为 superseded baseline 复现记录，不再代表当前可信结论：

- review attribution: `smoothed-review-attribution_75ec54d7e572038d`
- benefit / lag: `smoothing-benefit-lag_ea3a057745a3f0cd`
- regime validation: `smoothed-regime-validation_3fd897c7c66b3c40`
- confirmation: `smoothed-confirmation_0753b4cfbe5a2777`
- watch pack: `smoothed-watch-pack_520686f9c6924a84`
- smoothed backfill: `smoothed-backfill_27939e31bfdf54c6`
- baseline backfill: `paper-shadow-backfill_2138461d25e686e0`
- risk-capped backfill: `risk-capped-backfill_3d41bb93e038bbe4`

G2.4CO 重建后的有效上游状态为 Review=`CONTINUE_OBSERVATION/LOW`、
`candidate_method=null`、Confirmation=`INSUFFICIENT_EVIDENCE/0 targets`、
Watch forward=`NOT_REGISTERED`。因此 CP 只能逐方法报告诊断证据，不能用 readiness
分数补造 3d/5d candidate 或 promotion 结论。

## 目标

本阶段补齐 smoothed method 当前缺失的关键证据，并给 owner 一个可审计的
promotion readiness review 输入：

1. 拆解 `benefit_lag_tradeoff=INSUFFICIENT_DATA` 的具体原因。
2. 直接量化 `signal_churn`、`weight_jump`、`direction_flip` 和 turnover 变化。
3. 解释 `sideways_validation=MIXED` 的 window-level 原因。
4. 为两个 smoothed method 逐方法生成 readiness diagnostics，并且只允许 upstream
   Confirmation 的 evidence-backed candidate 进入 readiness decision。
5. 生成 owner review update 和 Reader Brief section。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-256|Benefit / Lag Missing Evidence Diagnosis|BASELINE_DONE|`smoothed-evidence-gap run/report` 和 `validate-smoothed-evidence-gap` 可运行，输出 per-method evidence matrix、candidate gap reason summary 和 metric backfill plan。|
|TRADING-257|Signal Churn / Weight Jump Metric Backfill|BASELINE_DONE|`smoothed-churn-backfill run/report` 和 `validate-smoothed-churn-backfill` 可运行；显式 deltas/turnover identity、method/date uniqueness、lineage 与 missing/null 可审计。|
|TRADING-258|Sideways Mixed Result Attribution|BASELINE_DONE|`sideways-mixed-attribution run/report` 和 `validate-sideways-mixed-attribution` 可运行；逐方法输出 window outcomes，不固定 preferred method。|
|TRADING-259|Candidate-aware Smoothed Readiness Scorecard|BASELINE_DONE|`smoothed-readiness-scorecard run/report` 和 validate 可运行；missing component 不得计分，candidate=null 时 recommended/secondary=null 且不得 promote。|
|TRADING-260|Smoothed Method Owner Review Update|BASELINE_DONE|`smoothed-owner-review-update run/report` 和 validate 可运行；candidate=null 时 promotion option 必须非推荐。|

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

`readiness_policy` 已进入
`config/etf_portfolio/dynamic_v3_rescue/smoothed_limited_adjustment_v1.yaml`，并带有
owner、version、status、rationale、intended effect、validation evidence 与 review
condition。`INSUFFICIENT_DATA`、`INSUFFICIENT_EVIDENCE` 和 `NOT_REGISTERED` 不在 score
mapping 中，`missing_component_score=null`；任一 component 缺失时 overall score 保持
null。

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

## G2.4CP Contract Freeze

2026-07-14 在 G2.4CO closeout 后复核旧 baseline，确认其 `PROMOTE_FOR_REVIEW` 结论不能继续作为可信当前结论：Evidence Gap、CLI 和 owner options 固定把 3d/5d 当 primary/secondary；Gap 把缺失数值经 `_float(None)` 变成 0；Churn 可拼接任意 Smoothed/Baseline/Risk backfills，未重验上游、日期唯一性或 exact baseline lineage，缺少 deltas 时又静默使用数值 fallback；Sideways、Scorecard、Owner Update 可组合不同 Review/Comparison/Backfill lineage。Readiness 权重、status scores、jump thresholds 与 `0.75/0.45` boundaries 散落在代码；`INSUFFICIENT_DATA` 和 `NOT_REGISTERED` 仍被赋正分，Scorecard 会在 G2.4CO 已明确 candidate=null、targets=[] 时从两方法中强行选出 recommended/secondary，造成无候选也可 `PROMOTE_FOR_REVIEW` 的语义错误。旧 validators 只检查文件、集合、枚举和 safety fields，不能重算来源、lineage、指标、score、decision、Markdown 或 Reader Brief。

G2.4CP 退出契约：

1. Gap、Churn、Sideways、Scorecard、Owner Update 各自产生 bounded `*.v2` input snapshot，记录 timezone-aware cutoff、validated upstream full-byte commitments、readiness policy binding 与 `production_effect=none`。
2. Gap 要求 Benefit/Lag、Regime、Watch 共享同一 Review→Comparison→Smoothed→Baseline lineage；Churn 要求 Smoothed、Baseline、Risk exact same baseline id/range/chronology；Sideways 要求 Regime 与 Churn 同 Backfill lineage；Scorecard 要求 Attribution、Benefit/Lag、Churn、Sideways、Confirmation 全部同 Review/Comparison/Backfill lineage；Owner Update 要求 Scorecard 与 Watch 同 lineage。
3. 3d/5d 所有诊断均逐方法输出，不预设 primary/secondary。Confirmation candidate 为 null 时，Scorecard 的 recommended/secondary 必须均为 null、decision不得为 `PROMOTE_FOR_REVIEW`，Owner options 不得补造 3d candidate。
4. Jump/severity thresholds、score weights/status mappings、promotion/continue boundaries、hard blocks、sample floors 进入带 owner/version/rationale/review condition 的 reviewed policy。Missing/non-finite/不足证据保持 null/`INSUFFICIENT_EVIDENCE`，不得赋默认正分或经0参与总体分。
5. Churn 的 method/date 必须唯一、ordered、finite；若 ledger 未提供显式 deltas，只允许在 artifact schema 明确保证 turnover=`0.5*sum(abs(delta))` 时使用命名的 turnover identity，并在输出披露计算来源；否则 fail closed。Direction flip 不得从缺失 deltas 补造。
6. 所有 producer 在正式目录前重验 live source/config/cutoff；所有 validator 再次重验 live source/policy、拒绝 invalid/missing/ambiguous/future/cross-lineage/duplicate/non-finite/tamper，并逐 byte 重建 JSON/JSONL/Markdown/checklist/Reader Brief。
7. 迁移15 callbacks并清除 legacy root对应callback/decorator/public domain/CP-specific helper实现；CLI tree/help/exit parity保持不变。同步 research chain、flow、runbook、registry/catalog、manifests、compatibility/deprecation evidence。
8. 本层保持 current-definition/not-PIT research/manual-only；不新增 target method，不改变 official weights/config/policy/portfolio/production，不 auto apply，不生成 order，不调用 broker，`production_effect=none`。

## Progress Notes

- 2026-07-14: G2.4CP `COMPLETE_G2_4_CONTINUES`，单个 slice 完成不触发
  phase-level ARCH-005 handoff。15 callbacks 已迁 canonical interface/domain，legacy CLI
  root 为 17,328 行/468 functions/429 decorators，legacy domain 为 19,856 行/760
  functions；generated manifests 为 917 modules/1,119 tests/858 direct writers/0
  violations，CLI tree 保持 41 roots/291 groups/993 leaves/0 duplicate，hash
  `d4744f3e...bc6f3`。Focused integration/hardening/CLI 为 1/7/116 passed；
  architecture-fitness 277 passed（`architecture-fitness_20260714T084835Z`），
  contract-validation 203 passed（`contract-validation_20260714T085007Z`）。当前结果仍为
  candidate=null、Gap 不能靠 direct backfill 解决且仍需 forward observation、Scorecard
  `CONTINUE_OBSERVATION/INSUFFICIENT_EVIDENCE`、Owner action
  `request_additional_evidence`、promotion option=false；不构成投资或 promotion 结论。
- 2026-07-14: 下游兼容审计发现 TRADING-261～265 legacy consumer 仍会把 candidate-null
  回填为固定 3d/5d；本 slice 的 readiness/gate 因未达门槛仍 fail closed 为
  `CONTINUE_OBSERVATION`，但该 fixed-role consumer 语义不可接受，已作为紧接的下一
  G2.4 slice contract-freeze 输入，不在 CP 中用 workaround 或测试放宽掩盖。
- 2026-07-14: canonical implementation 已落地并进入 focused validation。新增独立
  `dynamic_v3_system_target_smoothed_readiness.py` domain/interface，五类 v2 snapshots、
  live source/config replay、exact lineage、chronology、显式 deltas turnover identity、
  per-method Sideways、null-preserving score 与 candidate-aware owner options 已闭合；旧
  public domain/render/helper 和 15 legacy CLI callbacks 已减除，legacy Python surface
  只保留 lazy compatibility wrappers。当前 fixture 的 Gap direct churn/jump metrics 已
  available，`tradeoff_can_be_resolved_by_backfill=false`；但 candidate 仍为 null，故
  Scorecard=`CONTINUE_OBSERVATION/INSUFFICIENT_EVIDENCE`、recommended/secondary=null，
  Owner action=`request_additional_evidence`，promotion option=false。
- 2026-07-14: G2.4CP contract freeze并进入 `IN_PROGRESS`；旧固定3d、跨lineage、missing-to-zero、unreviewed scores 与 no-candidate promotion baseline失效，按上述 v2/policy/null-preserving/candidate-aware 契约重建。
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
