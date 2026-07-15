# TRADING-306 to TRADING-315 No-Promotion Diagnostics and Targeted Search v3

最后更新：2026-07-15

## 状态

`IN_PROGRESS`（ARCH-004 G2.4 canonical migration/hardening）

Owner 要求完成附件中的 TRADING-306～315。本阶段已把 TRADING-286～305 的 no-promotion 结论扩展为可审计诊断链路、targeted v3 research-only search/backfill、promotion decision v2 和 next formal-or-search plan；后续 owner 复核点是是否接受继续小范围 v4/信号层诊断，或另开 scorecard policy 校准任务。

## 背景

TRADING-286～305 已完成 Weight Optimization Batch Search & Adaptive Promotion Pipeline。真实链路中 initial matrix 为 64 variants，expanded matrix 为 87 variants，expanded scorecard 为 `weight-scorecard_62b9d331d9b34229`，top interpretation 为 `cash_buffer_10`，但 promoted candidate count 为 0，formal method auto plan 为 `SKIPPED_NO_PROMOTED_CANDIDATE` / `implemented=false`。

本阶段目标是把“没有 promotion candidate”从单一结论升级为可审计诊断、near-miss 提取、cash buffer attribution、search coverage gap、targeted v3 matrix、v3 backfill、A/B comparison、threshold sensitivity、candidate promotion v2 和下一步 formal-or-search plan。

2026-06-14 的实现与真实 artifact 只保留为历史 research baseline。ARCH-004 G2.4 审计确认这些
producer 直接读取 mutable artifact payload，未在写件前调用 canonical source validator，也没有冻结
bounded input/policy snapshot；各 validator 主要检查文件存在、shape、枚举与 safety 字段，不能从 live
source 重放 lineage、计算结果或 Markdown bytes。因此旧 `VALIDATING` 不等于 canonical contract 已闭合。

## ARCH-004 G2.4 原子迁移计划

为避免把剩余约 10K 行多代实现整体复制为另一个 god module，TRADING-306～315 分为三个独立
可验证 slice：

1. `G2.4CW1 / TRADING-306～309`：No-Promotion Review、Near-Miss、Cash-Buffer Attribution、
   Search Coverage Gap，共 12 callbacks / 12 public domain entrypoints；
2. `G2.4CW2 / TRADING-310～312`：Targeted Search v3、DQ-gated Backfill、Near-Miss A/B，
   共 10 callbacks / 10 public domain entrypoints；
3. `G2.4CW3 / TRADING-313～315`：Threshold Sensitivity、Promotion v2、Next Plan，共
   9 callbacks / 9 public domain entrypoints。

`G2.4CW1` canonical owner 固定为
`interfaces/cli/etf_portfolio/dynamic_v3_weight_search_diagnostics.py` 与
`etf_portfolio/dynamic_v3_weight_search_diagnostics.py`。退出标准：

- 四类 bounded `*.v2` input snapshots；No-Promotion 冻结 validated CV2 Scorecard 与 reviewed
  diagnostics policy，Near-Miss 要求同一 Scorecard→Review，Cash Attribution 要求同一
  Scorecard→Near-Miss，Coverage Gap 要求 validated CV1 Search Space 与同一
  Scorecard→Near-Miss→Attribution lineage；
- producer 在创建 output directory 前完成 source/config validation、唯一 ID、timezone-aware
  generated time、chronology、schema、finite/null 与 cross-lineage 检查；任何 invalid/missing/
  duplicate/future/non-finite/source drift 均 fail closed 且不留下半成品；
- near-miss/gate assessment/variant recommendation/search coverage 等投资解释启发式由 reviewed
  policy manifest 管理并在输出中暴露 policy version，不保留无解释 numeric/string literals；
- validator 重验 live source/config，重算全部 JSON/JSONL/Markdown/Reader Brief 并逐 byte 比较；
  source、policy、snapshot、schema、lineage 或任一 output tamper 必须失败；
- legacy CLI 与 domain 只保留 lazy compatibility dispatch，command path/options/default/help/exit、
  41/291/993 CLI tree/hash 不变；不得自动放宽 gate、自动采用候选、修改 official weights/paper
  primary/portfolio/production/order/broker；`production_effect=none`。

单个 CW slice 完成只能标记 `COMPLETE_G2_4_CONTINUES`。CW1/CW2/CW3 全部完成也不代表整个
G2.4 phase exit；后续 migration matrix 与 phase-level handoff gate 仍须单独通过，不进入 G2.5。

## Safety Boundary

- `research_screening_only=true`
- `experiment_only=true`
- `not_formal_research_method=true`，除非 promotion v2 后续明确进入 research-only formal implementation plan
- `not_official_target_weights=true`
- `paper_shadow_only=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

本阶段不得调用 broker、生成 order ticket、写 official target weights、修改真实仓位、自动 owner approval、自动切换 paper shadow primary candidate，或修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。

## Stage Breakdown

|Task|状态|Scope|验收标准|
|---|---|---|---|
|TRADING-306|BASELINE_DONE_CW1|No-promotion root cause review|`no-promotion-review run/report` 与 `validate-no-promotion-review` 可运行；输出 root cause、gate failure distribution、component weakness 和 next action。|
|TRADING-307|BASELINE_DONE_CW1|Near-miss candidate extraction|`near-miss-candidates extract/report` 与 validator 可运行；输出 near-miss JSONL、family summary 和 focus families。|
|TRADING-308|BASELINE_DONE_CW1|Cash buffer 10 attribution|`cash-buffer-attribution run/report` 与 validator 可运行；解释收益、回撤、turnover、rolling、recovery lag tradeoff。|
|TRADING-309|BASELINE_DONE_CW1|Search space coverage gap|`search-coverage-gap run/report` 与 validator 可运行；输出 family/parameter gaps 和 targeted v3 recommendation。|
|TRADING-310|PENDING_CW2|Targeted Search v3 matrix|`targeted-search-v3 build/report` 与 validator 可运行；60～120 variants，每个 variant 有 near-miss parent 或 coverage gap reason。|
|TRADING-311|PENDING_CW2|Targeted v3 backfill|`targeted-v3-backfill run/resume/report` 与 validator 可运行；先执行 `aits validate-data` 等价 cached data quality gate，输出 performance/regime/stability/churn metrics。|
|TRADING-312|PENDING_CW2|Near-miss A/B comparison|`near-miss-ab-comparison run/report` 与 validator 可运行；比较 v3 variants、near-miss parent、smooth_weights_3d 和 limited_adjustment。|
|TRADING-313|PENDING_CW3|Promotion threshold sensitivity|`promotion-threshold-sensitivity run/report` 与 validator 可运行；relaxed threshold 只进入 `REVIEW_REQUIRED`，不得自动放宽 base gate。|
|TRADING-314|PENDING_CW3|Candidate promotion decision v2|`candidate-promotion-v2 run/report` 与 validator 可运行；输出 promoted/rejected/keep-testing 列表和 decision。|
|TRADING-315|PENDING_CW3|Next formal or continue search plan|`next-formal-or-search-plan run/report` 与 validator 可运行；根据 promotion v2 生成 formal method candidates、keep-testing plan 或 next search plan。|

## Design Decisions

- v3 search 复用现有 limited_adjustment backfill 和 experiment transform engine，不新增外部数据源或 broker 输入。
- `targeted-v3-backfill` 走与 batch2 backfill 同一类 cached ETF price/rates data quality gate，并在 manifest 中披露 `data_quality_status`、`latest_valid_as_of`、market regime 和实际 date range。
- promotion threshold sensitivity 是诊断 artifact，不改变 base promotion gate policy；任何 relaxed scenario 只能输出 review-required 候选。
- promotion v2 仍是 research decision support，不等于 owner approval、official target weights、paper shadow primary switch、production mutation 或 broker action。
- 如果 v3 无 promoted candidates，系统必须生成继续搜索或返回 signal-level diagnosis 的计划，而不是中断。

## Progress Notes

- 2026-07-15: `G2.4CW1=COMPLETE_G2_4_CONTINUES`。focused/architecture/contract/full=
  `166/287/203/6,032 passed`，full=`1,773.27s`、641 warnings；generated=`936/1,129/858/0`，
  CLI tree/hash不变。长尾前三=`984.85/674.58/574.26s`，CW1 hardening=`279.65s`第8；相对
  CV3 full `1,592.38s`为单次约11.3%回归，不宣称稳定改善，后续保持PASS-only content
  fingerprint复用与duration+peak-memory sharding方向，绝不减少gate。该结果只关闭CW1，next继续
  CW2；CW2/CW3及whole G2.4 phase exit仍pending，不触发ARCH-005 handoff、不进入G2.5，
  `production_effect=none`。
- 2026-07-15: `G2.4CW1` canonical implementation完成并转`VALIDATING`。12 callbacks已离开legacy
  root，12个旧domain入口只保留lazy wrappers；legacy CLI=`13,522行/340 functions/301 decorators`，
  legacy weight domain=`10,320行`。四类v2 snapshots、reviewed `weight_search_diagnostics_v1`
  policy、exact Scorecard→Review→Near-Miss→Attribution与Search Space→Coverage lineage、21个
  emitted views逐byte重建、4 schema/3 cross-lineage/policy binding/pre-output chronology tamper
  fail-close均已实现。业务四阶段+hardening并行=`5 passed / 216.58s`，generated=
  `936 modules / 1,129 tests / 858 writers / 0 violations`，CLI仍`41/291/993/0`且tree hash不变；
  architecture/contract/full验证与最终source hashes进行中。CW2/CW3及whole G2.4仍pending，
  `production_effect=none`。
- 2026-07-15: `G2.4CW1` contract freeze并进入 `IN_PROGRESS`。范围固定 TRADING-306～309 的
  12 callbacks / 12 public domain entrypoints，迁独立 diagnostics interface/domain；四类 v2
  snapshots、reviewed diagnostics policy、validated exact Scorecard→Review→Near-Miss→Attribution
  与 Search Space→Coverage lineage、pre-output fail-close、live replay/all-view byte rebuild及
  source/policy/schema/cross-lineage/output tamper为退出合同。CW2/CW3及whole G2.4仍pending，
  不触发ARCH-005 handoff、不进入G2.5，`production_effect=none`。
- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-306～315 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 实现完成并转入 `VALIDATING`。真实链路输出 no-promotion review `no-promotion-review_52f5d7e23bda2288`，结论为 promoted candidate count=0、gate assessment=`TOO_STRICT`，主要阻断为 `composite_score_below_promotion_threshold`、`regime_behavior_mixed`、`rolling_consistency_not_strong_enough`、`insufficient_drawdown_improvement` 和 `return_preservation_weak`；near-miss extraction `near-miss-candidates_44faa683f4ee71a0` 输出 20 个 near-miss，集中在 smoothing family，`cash_buffer_10_near_miss=false`；cash buffer attribution `cash-buffer-attribution_195a76e5d95c3b12` 认为 `cash_buffer_10` 被 `composite_score_gate` 阻断，primary failure reason=`insufficient_robustness`，但可作为 cash-buffer/smoothing/threshold/median-consensus hybrid 继续测试；coverage gap `search-coverage-gap_dee60c99ece5da8a` 建议聚焦 `cash_buffer_smoothing_hybrid`、`cash_buffer_threshold_hybrid`、`median_consensus_smoothing`、`top5_consensus_threshold`、`sideways_cooldown_cash_buffer` 和 `smoothing_recovery_fast_restore`；targeted v3 matrix `targeted-search-v3_599f424a15c6114a` 生成 87 variants；targeted v3 backfill `targeted-v3-backfill_e7682602c7655f64` 完成 87/87 variants，data quality=`PASS_WITH_WARNINGS`，actual range=`2022-12-01`～`2026-06-10`，latest_valid_as_of=`2026-06-12`；A/B comparison `near-miss-ab-comparison_153686221c32e9bc` 的 best v3 variant 为 `cash_buffer_10_plus_smooth_2d_alpha_40`，但 v3_win_count=0、inconclusive_count=87；threshold sensitivity `promotion-threshold-sensitivity_64c3f26d9962f3e7` 未发现 base 或 relaxed-only promoted candidates，policy effect 仍为 diagnostic-only；candidate promotion v2 `candidate-promotion-v2_feab730f0e665860` 决策为 `RUN_ANOTHER_TARGETED_SEARCH`，promoted_count=0、rejected_count=87；next formal-or-search plan `next-formal-or-search-plan_8e375e22540acb95` 决策为 `CONTINUE_SEARCH_PLAN`，recommended_next_action=`run_smaller_v4_or_signal_level_diagnosis`。10 个新增 validators、10 个 report 命令、dynamic-v3 root validation、artifact family validation、documentation contract、Reader Brief、Reader Brief quality、ruff、compileall、git diff check、focused pytest `10 passed` 和 full pytest `2467 passed, 640 warnings` 已通过。全链路保持 research-only / no official target weights / no broker / no production / no owner auto approval。
