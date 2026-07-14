# TRADING-251 to 255 Smoothed Method Evidence Drilldown and Forward Confirmation

最后更新：2026-07-14

## 状态

BASELINE_DONE（G2.4CO canonical migration / evidence contract hardening 完成；等待独立 forward/PIT/DQ/cost/holdout 证据与 owner review）

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

1. 3d / 5d 各自改善了哪些问题，且不预设 primary / secondary 角色。
2. smoothing 是否降低 weight jumps、signal churn、turnover 并改善 rolling consistency。
3. 每个候选是否保留 `limited_adjustment` 收益优势。
4. smoothing 在 `sideways_choppy` 是否更稳。
5. smoothing 在 `strong_recovery` / fast regime change 是否明显滞后。
6. `smooth_weights_5d_limited_adjustment` 是否过度平滑。
7. 进入下一轮 promotion review 前需要哪些 forward confirmation。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-251|Smoothed Review Reason Attribution|BASELINE_DONE|`smoothed-review-attribution run/report` 和 validator 使用 v2 snapshot、exact lineage 与 content-derived views，输出 per-method supporting/blocking reasons、why_not_promote、why_not_reject。|
|TRADING-252|Smoothing Benefit vs Lag Cost Drilldown|BASELINE_DONE|`smoothing-benefit-lag run/report` 和 validator 输出 null-preserving per-method benefit、lag 与 tradeoff matrix。|
|TRADING-253|Sideways / Recovery Regime Validation|BASELINE_DONE|`smoothed-regime-validation run/report` 和 validator 对 paired finite samples输出 sideways/recovery，missing turnover fail closed。|
|TRADING-254|Smoothed Forward Confirmation Target Registration|BASELINE_DONE|只为唯一 eligible recommended method 登记 targets；当前无候选时0 targets/`INSUFFICIENT_EVIDENCE`，`auto_apply=false`。|
|TRADING-255|Smoothed Method Operational Watch Pack|BASELINE_DONE|Watch 要求四源同一 lineage，生成 candidate-specific checklist、Reader Brief 和 watch summary；当前 forward=`NOT_REGISTERED`。|

## Pilot Baselines And Governance

以下阈值和样本要求来自本阶段 owner 任务说明，先作为可审计 pilot baseline 使用：

- `required_forward_events=10`
- `required_sideways_events=5`
- `required_recovery_events=5`
- confirmation windows: `[1, 5, 10, 20]`
- `forward_return_delta_floor=-0.001`
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
4. 仅为 Review 唯一 eligible recommended method 登记 forward confirmation targets；无候选时登记零 targets。
5. 汇总 1-4 为 weekly/owner watch pack，并提供 Reader Brief section。
6. 更新 README、operations runbook、system flow、report registry、artifact catalog 和 task register。
7. 新增 focused tests，运行 validators、ruff、compileall 和 diff check。

## G2.4CO Contract Freeze

2026-07-14审计确认，旧baseline只能证明命令可运行，不能证明证据链可重放：producer未在正式写件前调用CN上游validator，也未冻结source/config/policy bytes或generated cutoff；Attribution、Benefit/Lag、Regime、Confirmation和Watch可各自拼接不同Review/Comparison/Backfill lineage；validator只检查文件、少量枚举和安全字段，不能重算来源选择、lineage、指标、target或Markdown。旧helper、CLI摘要与报告还把3d写死为primary，并用未治理的forward/sideways/recovery阈值登记targets；当CN当前Review已明确`recommended_method=null`时，这会把“无候选”错误解释成“3d待确认”。

G2.4CO退出契约：

1. 五阶段各自产生bounded `*.v2` input snapshot，记录timezone-aware `generated_at`、validated upstream full-byte commitments、policy binding和`production_effect=none`。
2. Attribution要求Review→Comparison→Smoothed Backfill→Baseline Backfill exact lineage；Benefit/Lag与Regime复用同一Comparison/Smoothed/Baseline链；Confirmation要求Review与Regime同链；Watch要求四类上游artifact共享同一Review/Comparison/Backfill lineage与chronology。
3. sample不足、缺失metric或无recommended method保持`null/INSUFFICIENT_EVIDENCE`，不得经默认0进入status/target/readiness；Confirmation仅为Review真实evidence-backed recommended method登记targets，当前无推荐时`target_count=0`且`candidate_method=null`。
4. return/turnover/jump/lag/sample floors与forward target模板进入带owner/version/rationale/review condition的reviewed policy；不得继续依赖散落hardcoded阈值。
5. 所有validator重验live source与policy、拒绝invalid/missing/ambiguous/future/cross-lineage/duplicate/non-finite，并逐byte重建全部JSON、Markdown、checklist和Reader Brief；普通checksum只作完整性证据，不是签名。
6. 迁移15 callback且legacy root对应callback/decorator/domain实现清零；CLI tree/help/exit parity不变。同步README、研究执行链、system flow、runbook、registry/catalog、manifests、deprecation和architecture evidence。
7. 本层保持not-PIT research/manual-only；不新增target method，不改变official weights/config/policy，不auto apply，不生成order，不调用broker，`production_effect=none`。

## Progress Notes

- 2026-07-14: G2.4CO `BASELINE_DONE / COMPLETE_G2_4_CONTINUES`。Focused=12、current slice + CLI contract=120、downstream compatibility=1、architecture-fitness=276、contract-validation=203，全部 parallel PASS；architecture runtime=`outputs/validation_runtime/architecture-fitness_20260714T071701Z/test_runtime_summary.json`，contract runtime=`outputs/validation_runtime/contract-validation_20260714T071839Z/test_runtime_summary.json`。Generated inventory=915 modules/1118 test files/858 direct writers/0 violations；CLI tree=41 roots/291 groups/993 leaves/0 duplicates/hash unchanged。整个 G2.4 未完成，继续下一 slice，不触发 ARCH-005 handoff。
- 2026-07-14: G2.4CO implementation / focused closeout 进入 `VALIDATING`：15 callback 与旧领域实现已迁出 legacy roots，五类 v2 snapshots、live replay、exact lineage、candidate-driven targets、null-preserving regime metrics 和 byte-derived validators 已实现；CLI tree 保持 41/291/993、hash=`d4744f3ec1bbbfc05d10246f7969b3f9174e4cfebc9bec9d8b39a472e83bc6f3`，current slice + CLI contract 120 tests PASS（119-test parallel run + 新增 5d eligible candidate regression 1 test），downstream readiness compatibility 1 test PASS。等待 architecture / contract tiers 后转 `BASELINE_DONE`；整个 G2.4 仍继续。
- 2026-07-14: G2.4CO contract freeze并进入`IN_PROGRESS`；旧固定3d candidate/跨lineage/补0/浅validator baseline不再作为可信当前结论，按上述v2 snapshot与evidence-backed target契约重建。
- 2026-07-14: source-backed hardened fixture 已确认 3d/5d benefit 都为 `MODERATE`、tradeoff 都为 `FAVORABLE`，sideways 为 `MIXED/IMPROVED`，recovery lag 都为 `LOW`；但 Review 仍为 `CONTINUE_OBSERVATION/LOW`、recommended/secondary 都为 null，因此 Confirmation 正确输出 `INSUFFICIENT_EVIDENCE`、`candidate_method=null`、`targets=[]`，Watch forward=`NOT_REGISTERED`。局部 favorable tradeoff 不覆盖 drawdown hard block。
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

2026-07-14 G2.4CO canonical validation：

- focused / hardening：12 passed；覆盖全链 replay、cross-lineage、policy/render drift、sample floor、missing turnover、5d eligible candidate 与 no-candidate zero-target。
- current slice + CLI contract：120 passed；CLI 41/291/993、tree hash `d4744f3ec1bbbfc05d10246f7969b3f9174e4cfebc9bec9d8b39a472e83bc6f3` 不变。
- downstream `tests/test_smoothed_readiness_review.py`：1 passed。
- architecture-fitness：276 passed，runtime `architecture-fitness_20260714T071701Z`。
- contract-validation：203 passed，runtime `contract-validation_20260714T071839Z`。
- Ruff、compileall、git diff check：PASS。

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
