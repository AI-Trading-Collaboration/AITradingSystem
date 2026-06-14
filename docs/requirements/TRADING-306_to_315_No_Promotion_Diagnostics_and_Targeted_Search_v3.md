# TRADING-306 to TRADING-315 No-Promotion Diagnostics and Targeted Search v3

最后更新：2026-06-14

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-306～315。本阶段已把 TRADING-286～305 的 no-promotion 结论扩展为可审计诊断链路、targeted v3 research-only search/backfill、promotion decision v2 和 next formal-or-search plan；后续 owner 复核点是是否接受继续小范围 v4/信号层诊断，或另开 scorecard policy 校准任务。

## 背景

TRADING-286～305 已完成 Weight Optimization Batch Search & Adaptive Promotion Pipeline。真实链路中 initial matrix 为 64 variants，expanded matrix 为 87 variants，expanded scorecard 为 `weight-scorecard_62b9d331d9b34229`，top interpretation 为 `cash_buffer_10`，但 promoted candidate count 为 0，formal method auto plan 为 `SKIPPED_NO_PROMOTED_CANDIDATE` / `implemented=false`。

本阶段目标是把“没有 promotion candidate”从单一结论升级为可审计诊断、near-miss 提取、cash buffer attribution、search coverage gap、targeted v3 matrix、v3 backfill、A/B comparison、threshold sensitivity、candidate promotion v2 和下一步 formal-or-search plan。

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
|TRADING-306|VALIDATING|No-promotion root cause review|`no-promotion-review run/report` 与 `validate-no-promotion-review` 可运行；输出 root cause、gate failure distribution、component weakness 和 next action。|
|TRADING-307|VALIDATING|Near-miss candidate extraction|`near-miss-candidates extract/report` 与 validator 可运行；输出 near-miss JSONL、family summary 和 focus families。|
|TRADING-308|VALIDATING|Cash buffer 10 attribution|`cash-buffer-attribution run/report` 与 validator 可运行；解释收益、回撤、turnover、rolling、recovery lag tradeoff。|
|TRADING-309|VALIDATING|Search space coverage gap|`search-coverage-gap run/report` 与 validator 可运行；输出 family/parameter gaps 和 targeted v3 recommendation。|
|TRADING-310|VALIDATING|Targeted Search v3 matrix|`targeted-search-v3 build/report` 与 validator 可运行；60～120 variants，每个 variant 有 near-miss parent 或 coverage gap reason。|
|TRADING-311|VALIDATING|Targeted v3 backfill|`targeted-v3-backfill run/resume/report` 与 validator 可运行；先执行 `aits validate-data` 等价 cached data quality gate，输出 performance/regime/stability/churn metrics。|
|TRADING-312|VALIDATING|Near-miss A/B comparison|`near-miss-ab-comparison run/report` 与 validator 可运行；比较 v3 variants、near-miss parent、smooth_weights_3d 和 limited_adjustment。|
|TRADING-313|VALIDATING|Promotion threshold sensitivity|`promotion-threshold-sensitivity run/report` 与 validator 可运行；relaxed threshold 只进入 `REVIEW_REQUIRED`，不得自动放宽 base gate。|
|TRADING-314|VALIDATING|Candidate promotion decision v2|`candidate-promotion-v2 run/report` 与 validator 可运行；输出 promoted/rejected/keep-testing 列表和 decision。|
|TRADING-315|VALIDATING|Next formal or continue search plan|`next-formal-or-search-plan run/report` 与 validator 可运行；根据 promotion v2 生成 formal method candidates、keep-testing plan 或 next search plan。|

## Design Decisions

- v3 search 复用现有 limited_adjustment backfill 和 experiment transform engine，不新增外部数据源或 broker 输入。
- `targeted-v3-backfill` 走与 batch2 backfill 同一类 cached ETF price/rates data quality gate，并在 manifest 中披露 `data_quality_status`、`latest_valid_as_of`、market regime 和实际 date range。
- promotion threshold sensitivity 是诊断 artifact，不改变 base promotion gate policy；任何 relaxed scenario 只能输出 review-required 候选。
- promotion v2 仍是 research decision support，不等于 owner approval、official target weights、paper shadow primary switch、production mutation 或 broker action。
- 如果 v3 无 promoted candidates，系统必须生成继续搜索或返回 signal-level diagnosis 的计划，而不是中断。

## Progress Notes

- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-306～315 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 实现完成并转入 `VALIDATING`。真实链路输出 no-promotion review `no-promotion-review_52f5d7e23bda2288`，结论为 promoted candidate count=0、gate assessment=`TOO_STRICT`，主要阻断为 `composite_score_below_promotion_threshold`、`regime_behavior_mixed`、`rolling_consistency_not_strong_enough`、`insufficient_drawdown_improvement` 和 `return_preservation_weak`；near-miss extraction `near-miss-candidates_44faa683f4ee71a0` 输出 20 个 near-miss，集中在 smoothing family，`cash_buffer_10_near_miss=false`；cash buffer attribution `cash-buffer-attribution_195a76e5d95c3b12` 认为 `cash_buffer_10` 被 `composite_score_gate` 阻断，primary failure reason=`insufficient_robustness`，但可作为 cash-buffer/smoothing/threshold/median-consensus hybrid 继续测试；coverage gap `search-coverage-gap_dee60c99ece5da8a` 建议聚焦 `cash_buffer_smoothing_hybrid`、`cash_buffer_threshold_hybrid`、`median_consensus_smoothing`、`top5_consensus_threshold`、`sideways_cooldown_cash_buffer` 和 `smoothing_recovery_fast_restore`；targeted v3 matrix `targeted-search-v3_599f424a15c6114a` 生成 87 variants；targeted v3 backfill `targeted-v3-backfill_e7682602c7655f64` 完成 87/87 variants，data quality=`PASS_WITH_WARNINGS`，actual range=`2022-12-01`～`2026-06-10`，latest_valid_as_of=`2026-06-12`；A/B comparison `near-miss-ab-comparison_153686221c32e9bc` 的 best v3 variant 为 `cash_buffer_10_plus_smooth_2d_alpha_40`，但 v3_win_count=0、inconclusive_count=87；threshold sensitivity `promotion-threshold-sensitivity_64c3f26d9962f3e7` 未发现 base 或 relaxed-only promoted candidates，policy effect 仍为 diagnostic-only；candidate promotion v2 `candidate-promotion-v2_feab730f0e665860` 决策为 `RUN_ANOTHER_TARGETED_SEARCH`，promoted_count=0、rejected_count=87；next formal-or-search plan `next-formal-or-search-plan_8e375e22540acb95` 决策为 `CONTINUE_SEARCH_PLAN`，recommended_next_action=`run_smaller_v4_or_signal_level_diagnosis`。10 个新增 validators、10 个 report 命令、dynamic-v3 root validation、artifact family validation、documentation contract、Reader Brief、Reader Brief quality、ruff、compileall、git diff check、focused pytest `10 passed` 和 full pytest `2467 passed, 640 warnings` 已通过。全链路保持 research-only / no official target weights / no broker / no production / no owner auto approval。
