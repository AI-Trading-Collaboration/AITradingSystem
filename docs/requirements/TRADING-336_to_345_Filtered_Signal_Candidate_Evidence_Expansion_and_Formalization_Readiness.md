# TRADING-336 to TRADING-345 Filtered Signal Candidate Evidence Expansion and Formalization Readiness

最后更新：2026-06-15

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-336～345。本阶段承接 TRADING-326～335 的结论：`median_plus_regime_mismatch_filter` 是 filtered-vs-original comparison 的当前最佳 filtered candidate，source recommendation 为 `PROMOTE_FOR_REVIEW`，但 filtered promotion review 仍为 `CONTINUE_TESTING`，`formalization_ready=false`。

## 背景

TRADING-326～335 已完成并进入 `VALIDATING`，关键输出包括：

- signal failure taxonomy failure modes=16。
- candidate signal ledger dominant failure=`direction_flip_high`，`data_quality_status=PASS_WITH_WARNINGS`。
- signal churn root cause dominant=`top_candidate_rotation`。
- regime mismatch dominant=`risk_increase_during_drawdown`。
- candidate quality filter design 生成 5 个 filters。
- filtered backfill 生成 5 个 variants，`data_quality_status=PASS_WITH_WARNINGS`。
- filtered-vs-original comparison best=`median_plus_regime_mismatch_filter`，recommendation=`PROMOTE_FOR_REVIEW`。
- signal gate experiment recommended next action=`continue_forward_confirmation`，`formalization_ready=false`。
- filtered candidate promotion review decision=`CONTINUE_TESTING`。

本阶段目标是围绕 `median_plus_regime_mismatch_filter` 扩展证据，回答它是否真正减少 drawdown mismatch、direction flips、top candidate rotation 和 signal churn，是否优于 smooth / limited / median baselines，以及是否可以进入 formal research method implementation。

## Safety Boundary

- `research_screening_only=true`
- `experiment_only=true`
- `not_official_target_weights=true`
- `paper_shadow_only=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

本阶段不得调用 broker、导入 broker data、生成 order ticket、写 official / production target weights、自动 owner approval、自动修改真实仓位、自动切换 paper shadow primary candidate，或修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。

## Stage Breakdown

|Task|状态|Scope|验收标准|
|---|---|---|---|
|TRADING-336|VALIDATING|Filtered Candidate Evidence Drilldown|`filtered-candidate-evidence run/report` 与 `validate-filtered-candidate-evidence` 可运行；输出 evidence summary、component breakdown、strength/weakness matrix 和 reader brief section。|
|TRADING-337|VALIDATING|Median + Regime Mismatch Filter Specification Review|`median-regime-filter-spec review/report` 与 validator 可运行；输出 readable spec YAML 和 contract。|
|TRADING-338|VALIDATING|Filtered Candidate Expanded Backfill / Stress Windows|`filtered-candidate-stress-backfill run/report` 与 validator 可运行；输出 stress window inventory、metrics 和 summary。|
|TRADING-339|VALIDATING|Drawdown Regime Mismatch Reduction Review|`drawdown-mismatch-reduction run/report` 与 validator 可运行；输出 before/after、blocked signal helpful/harmful rate 和 summary。|
|TRADING-340|VALIDATING|Direction Flip / Top Candidate Rotation Reduction Review|`flip-rotation-reduction run/report` 与 validator 可运行；输出 direction flip、top candidate rotation 和 signal churn reduction summary。|
|TRADING-341|VALIDATING|Filtered Candidate A/B Review|`filtered-candidate-ab-review run/report` 与 validator 可运行；对比 smooth 3d、limited、median、static 和 no-trade baselines。|
|TRADING-342|VALIDATING|Signal Gate Forward Confirmation Registration|`signal-gate-confirmation register/report` 与 validator 可运行；注册 forward confirmation targets，固定 `auto_apply=false`。|
|TRADING-343|VALIDATING|Formalization Readiness Gate v1|`filtered-formalization-readiness run/report` 与 validator 可运行；输出 research method readiness decision 和 blockers。|
|TRADING-344|VALIDATING|Owner Filtered Candidate Review Pack|`owner-filtered-candidate-review pack/report` 与 validator 可运行；输出 owner summary、checklist 和 reader brief section。|
|TRADING-345|VALIDATING|Next Decision|`filtered-next-decision run/report` 与 validator 可运行；输出 next decision 和 next task plan。|

## Design Decisions

- `median_plus_regime_mismatch_filter` 的 base method 固定为 `median_target_weights`；本阶段只把现有 filtered candidate 规格化，不把它注册为 production method。
- Filter spec 是 research-only contract：`tech_drawdown`、`semiconductor_pullback` 和 `risk_off` 阻止加风险，`strong_recovery` 允许有限 risk restore，`sideways_choppy` 降低 active tilt 并要求 signal persistence。
- Stress backfill 使用 TRADING-326～335 artifacts 中的 filtered comparison、promotion review、candidate signal ledger 和 filter spec 派生 stress windows。缺 forward evidence 时不得阻断 historical research evidence expansion，但会阻断 official target / production promotion。
- Drawdown mismatch reduction 单独解释 `risk_increase_during_drawdown` 的 before / after、helpful / harmful blocked signal rate。
- Flip / rotation reduction 单独解释 `direction_flip_high`、`top_candidate_rotation` 和 signal churn 是否下降，并标出 responsiveness 风险。
- A/B review 明确比较 `median_plus_regime_mismatch_filter` 与 `smooth_weights_3d_limited_adjustment`、`limited_adjustment`、`median_target_weights`、`static_baseline`、`no_trade_baseline`，把信号质量改善和 return preservation 分开。
- Forward confirmation registration 只登记 observation targets；`auto_apply=false`，不改变 paper shadow primary candidate。
- Formalization readiness gate 允许 `can_implement_research_only_method=true`，但只要 forward confirmation 不足或 evidence mixed，就不得写 official target weights。
- Owner review pack 只供人工复核，不执行 owner approval。

## Required Outputs

本阶段必须更新 README、operations runbook、system flow、report registry、artifact catalog、requirements、task register 和 Reader Brief 集成。报告必须说明：

1. 为什么 `median_plus_regime_mismatch_filter` 是当前 top filtered candidate。
2. stress window backfill 如何判断 filter 是否有效。
3. drawdown mismatch reduction 如何解释。
4. direction flip / rotation reduction 如何解释。
5. A/B review 如何比较 smooth 3d / limited / median。
6. forward confirmation targets 如何注册。
7. formalization readiness gate 如何判断是否可实现 research method。
8. 为什么仍然 no official target / no broker / no production。

## Progress Notes

- 2026-06-15: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-336～345 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-15: 实现完成并转入 `VALIDATING`。真实链路输出 evidence `filtered-candidate-evidence_bd523ee9a1c57d2b`=`PROMISING`、spec `median-regime-filter-spec_ae8ebf919e69f9c8` contract=`PASS`、stress `filtered-candidate-stress-backfill_3a8a444ec8f99352`=`STRONG`、drawdown reduction `drawdown-mismatch-reduction_222a00f7d9552eef`=`IMPROVED`、flip reduction `flip-rotation-reduction_ff6cbc9fd57e61c7`=`IMPROVED`、A/B `filtered-candidate-ab-review_42e52370a750657e`=`PROMISING`、confirmation `signal-gate-confirmation_b93f65491d9088b0` target_count=3、readiness `filtered-formalization-readiness_ae76d764e17100dd`=`READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION`、owner review `owner-filtered-candidate-review_40c887b806117d4f` action=`formalize_research_method`、next decision `filtered-next-decision_323b8f865c33e673`=`FORMALIZE_RESEARCH_METHOD`；10 个新增 report/validate commands、dynamic-v3 validation、artifact family validation、documentation contract、report index、Reader Brief/quality、ruff、compileall、git diff check 和 focused pytest `10 passed` 已通过；full pytest 在 604 秒超时，未作为通过依据。所有输出仍保持 no official target / no broker / no production。
