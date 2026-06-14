# TRADING-326 to TRADING-335 Signal Feature Diagnosis and Candidate Quality Filter Pipeline

最后更新：2026-06-14

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-326～335。本阶段承接 TRADING-316～325 的结论：targeted v3 和 micro-search v4 未产生 official promoted candidate，gate calibration 为 `REASONABLE`，主要失败来源转向 `SIGNAL_QUALITY` / `signal_churn`。因此本阶段不继续无差别扩大参数搜索，而是建立 signal failure taxonomy、candidate event ledger、churn / regime mismatch attribution、candidate quality filters、filtered backfill、gate experiment、promotion review 和 owner roadmap。

## 背景

TRADING-316～325 已完成并进入 `VALIDATING`，关键输出包括：

- gate calibration `calibrated_assessment=REASONABLE`，未修改 official gate。
- scorecard attribution 显示弱项集中在 `signal_churn_score`、`drawdown_score`、`regime_score`。
- signal diagnosis 显示 `requires_signal_level_fix=true`，dominant issue=`signal_churn`。
- consensus review 未发现可单独解释失败的 consensus family。
- micro-search v4 backfill 完成 24/24，`data_quality_status=PASS_WITH_WARNINGS`。
- signal-vs-parameter attribution 判断 `failure_source=SIGNAL_QUALITY`，confidence=`MEDIUM`。
- next direction 为 `SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS`。

## Safety Boundary

- `research_screening_only=true`
- `experiment_only=true`
- `not_formal_research_method=true`
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
|TRADING-326|VALIDATING|Signal Feature Failure Taxonomy|`signal-failure-taxonomy validate/report` 与 `validate-signal-failure-taxonomy` 可运行；输出 normalized taxonomy、failure mode catalog、safety fields。|
|TRADING-327|VALIDATING|Candidate Signal Event Ledger|`candidate-signal-ledger build/report` 与 validator 可运行；输出 `signal_events.jsonl`、candidate summary 和 reader brief section。|
|TRADING-328|VALIDATING|Signal Churn Root Cause Review|`signal-churn-root-cause run/report` 与 validator 可运行；输出 dominant root cause、event clusters 和 mitigation candidates。|
|TRADING-329|VALIDATING|Regime Mismatch Signal Attribution|`regime-mismatch-attribution run/report` 与 validator 可运行；输出 mismatch events、summary 和 recommended filter。|
|TRADING-330|VALIDATING|Candidate Quality Filter Design|`candidate-quality-filter-design run/report` 与 validator 可运行；输出 proposed filters、config draft 和 reader brief section。|
|TRADING-331|VALIDATING|Filtered Candidate Pool Backfill|`filtered-candidate-backfill run/report` 与 validator 可运行；输出 filtered specs、performance 和 signal metrics。|
|TRADING-332|VALIDATING|Filtered Candidate vs Original Comparison|`filtered-vs-original-comparison run/report` 与 validator 可运行；输出 comparison matrix 和 improvement summary。|
|TRADING-333|VALIDATING|Signal Persistence / Disagreement Gate Experiment|`signal-gate-experiment run/report` 与 validator 可运行；输出 persistence / disagreement gate variants 和 summary。|
|TRADING-334|VALIDATING|Filtered Candidate Promotion Review|`filtered-candidate-promotion-review run/report` 与 validator 可运行；输出 decision、candidate specs 和 reader brief section。|
|TRADING-335|VALIDATING|Owner Signal-Level Research Roadmap|`owner-signal-roadmap build/report` 与 validator 可运行；输出 owner roadmap summary、checklist 和 reader brief section。|

## Design Decisions

- Taxonomy 是 reviewed-config style pilot baseline，记录 owner、status、review condition 和 no-production safety boundary。
- Event ledger 使用既有 targeted v3 / v4 backfill、scorecard attribution、signal instability 和 consensus dispersion evidence 构造研究型 signal event ledger；若逐日信号细节不足，event quality 会保持 `unknown` 或 `neutral`，不得伪造真实 forward outcome。
- Churn root cause、regime mismatch 和 filter design 的阈值是 TRADING-326～335 pilot constants，只用于 research screening；正式投资解释前必须用后续 owner-reviewed policy 替换或校准。
- Filtered backfill 是 lightweight transform-composable prototype，不注册 formal method、不写 method config、不改变 paper shadow primary candidate。
- Promotion review 只允许输出 `PROMOTE_FOR_FORMAL_RESEARCH_IMPLEMENTATION`、`CONTINUE_TESTING`、`REJECT` 或 `INSUFFICIENT_DATA`；任何 positive decision 仍需要 forward confirmation 和 owner review。
- Owner signal roadmap 回答是否继续 signal-level research、是否 formalize filtered candidate、是否继续 smoothed forward confirmation、是否停止大规模参数搜索，以及 no official target / no broker / no production 是否仍成立。

## Progress Notes

- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-326～335 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 实现完成并转入 `VALIDATING`；真实链路输出 taxonomy `signal-failure-taxonomy_4dd4a4a1a340799b`=16 failure modes，ledger `candidate-signal-ledger_0ba7661d9a7dba34` dominant failure=`direction_flip_high` / `data_quality_status=PASS_WITH_WARNINGS`，churn root cause `signal-churn-root-cause_79bdbb1a94fe7547` dominant=`top_candidate_rotation`，regime mismatch `regime-mismatch-attribution_e0cf4b6989b248b4` dominant=`risk_increase_during_drawdown`，filter design `candidate-quality-filter-design_edafde5d99e021fe`=5 filters，filtered backfill `filtered-candidate-backfill_e0b3be39c9a9a33f`=5 variants / `data_quality_status=PASS_WITH_WARNINGS`，comparison `filtered-vs-original-comparison_e77634a8cf5851b3` best=`median_plus_regime_mismatch_filter` / recommendation=`PROMOTE_FOR_REVIEW`，signal gate experiment `signal-gate-experiment_cda00c7a3e8dc1ee` recommended_next_action=`continue_forward_confirmation` / formalization_ready=false，promotion review `filtered-candidate-promotion-review_ffdb14e1b2782e02` decision=`CONTINUE_TESTING`，owner roadmap `owner-signal-roadmap_64c7d31037ee42a4` recommended_owner_action=`continue_forward_confirmation_and_signal_gate_evidence`；10 个新增 validators、dynamic-v3 validation、artifact family validation、documentation contract、Reader Brief/quality、report index、ruff、compileall、git diff check、focused pytest 和 full pytest `2478 passed, 640 warnings` 已通过；保持 no official target / no broker / no production。
