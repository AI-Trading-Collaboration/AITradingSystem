# TRADING-326 to TRADING-335 Signal Feature Diagnosis and Candidate Quality Filter Pipeline

最后更新：2026-07-19

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-326～335。本阶段承接 TRADING-316～325 的当前 canonical
结论：targeted v3 和 micro-search v4 未产生 official promoted candidate，但现有 aggregate proxy 不能证明
主要失败来源是 `SIGNAL_QUALITY` / `signal_churn`。因此本阶段建立 signal failure taxonomy、candidate
event ledger、churn / regime mismatch attribution、candidate quality filters、filtered backfill、gate
experiment、promotion review 和 owner roadmap；在 validated dated signal evidence 到位前必须保持
`INSUFFICIENT_DATA`，不得把旧 pilot 合成事件当成真实研究结论。

## 背景

TRADING-316～325 已完成并进入 `VALIDATING`，关键输出包括：

- gate calibration `calibrated_assessment=REASONABLE`，未修改 official gate。
- scorecard attribution 显示弱项集中在 `signal_churn_score`、`drawdown_score`、`regime_score`。
- signal diagnosis 因没有 validated dated signal ledger，`requires_signal_level_fix=null`、
  `evidence_status=INSUFFICIENT_DATA`。
- consensus review 因没有 dated candidate weight path，dispersion / quality delta 保持 null。
- micro-search v4 backfill 完成 24/24，`data_quality_status=PASS_WITH_WARNINGS`。
- signal-vs-parameter attribution 当前判断 `failure_source=INCONCLUSIVE`、confidence=`LOW`，下一步是
  `DEFER_AND_BUILD_DATED_EVIDENCE`。
- TRADING-326～335 的既有实现和历史 artifact 只可作为历史 pilot context，不是当前 attribution proof。

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
- Event ledger 只接受 validated `signal_flip_events` 作为 dated event source。Aggregate v4 metrics 只能说明
  source coverage，不能回填日期、direction、forward return 或 event quality；缺少 dated rows 时
  `signal_events=[]`、method count/return 为 null、`evidence_status=INSUFFICIENT_DATA`。
- Churn root cause、regime mismatch 和 filter design 的阈值已迁入带 owner/version/rationale/
  validation evidence/review condition 的 reviewed pilot policy；代码不再以未治理常量决定投资解释。
- Filtered backfill 是 lightweight transform-composable prototype，不注册 formal method、不写 method config、不改变 paper shadow primary candidate。
- Promotion review 只允许输出 `PROMOTE_FOR_FORMAL_RESEARCH_IMPLEMENTATION`、`CONTINUE_TESTING`、`REJECT` 或 `INSUFFICIENT_DATA`；任何 positive decision 仍需要 forward confirmation 和 owner review。
- Owner signal roadmap 回答是否继续 signal-level research、是否 formalize filtered candidate、是否继续 smoothed forward confirmation、是否停止大规模参数搜索，以及 no official target / no broker / no production 是否仍成立。

## EB1 Canonical Execution Contract

ARCH-004G2.4-EB1 将 TRADING-326～330 的15个 producer/report/validator callbacks 和15个 domain
public entrypoints迁至 `dynamic_v3_signal_filter_foundation.py`；legacy CLI 已减去实现，legacy domain只保留
同名 lazy compatibility wrapper。五段计算链如下：

|Stage|输入|输出|核心计算与 fail-closed 规则|当前结果|
|---|---|---|---|---|
|Taxonomy|reviewed `signal_feature_failure_taxonomy_v1.yaml` policy binding|normalized YAML、mode catalog、manifest、Markdown、v2 snapshot|校验policy schema/metadata/safety、mode/family完整性；全部view逐byte可重建|16 modes；PASS|
|Candidate Ledger|validated Taxonomy、Micro Search Design/Backfill、optional validated Signal/Consensus full bundles|event JSONL、method summary、Reader Brief、manifest、v2 snapshot|严格 Design→Backfill→Signal/Consensus lineage与pre-output chronology；只映射真实dated rows，不从aggregate proxy合成事件|0 dated events；`INSUFFICIENT_DATA`|
|Churn Root Cause|validated Ledger full bundle|summary、clusters、mitigations、report、v2 snapshot|有events才按disagreement/sideways/flip evidence归因；无events时cause/count-share保持null且不提出mitigation|cause=null；0 clusters；0 mitigations|
|Regime Mismatch|validated同一Ledger full bundle|mismatch events/summary/report、v2 snapshot|仅从真实ledger event的regime/action字段归因；无events不生成mismatch结论|0 mismatches；dominant=null|
|Quality Filter Design|validated同ledger的Churn+Mismatch、同一policy binding|filter JSON、config YAML、Reader Brief、report、v2 snapshot|先校验两源同ledger/同policy；只有两类evidence均SUFFICIENT才生成review-only filters|0 filters；`INSUFFICIENT_DATA`|

所有producer在创建output目录前完成source validation、identity/lineage和timezone-aware chronology检查；
snapshot冻结exact file set/path/hash与policy binding，validator重验live sources并重算全部23个JSON/JSONL/YAML/
Markdown views。source、policy binding、cross-lineage、chronology或任一output byte发生漂移均FAIL。该链不运行
上游、不刷新cache、不修改official gate/policy/weights/portfolio/order/broker/production。

## 后续优化空间

- 先建设validated dated signal ledger与candidate weight path；在此之前不得校准churn/mismatch/filter方向。
- dated evidence到位后，以独立forward cohort校准dispersion、persistence、flip和harmful-share阈值，并记录
  owner review/expiry condition；不要直接沿用pilot值。
- 当前focused重型fixture单文件约52～58秒；`pytest -n16 --dist loadfile`并行两条重型DAG后墙钟约72秒。
  后续可把Micro Search validated immutable source bundle提升为module-scoped fixture，但必须保留content-
  fingerprint invalidation、tamper copy-on-write和真实validator，不能缓存FAIL。
- TRADING-331～335 在后续EB2迁移时必须同步修正“empty filter仍产生候选/结论”的legacy语义，并沿用同一
  snapshot、lineage、null和all-view rebuild契约。

## Progress Notes

- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-326～335 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 实现完成并转入 `VALIDATING`；真实链路输出 taxonomy `signal-failure-taxonomy_4dd4a4a1a340799b`=16 failure modes，ledger `candidate-signal-ledger_0ba7661d9a7dba34` dominant failure=`direction_flip_high` / `data_quality_status=PASS_WITH_WARNINGS`，churn root cause `signal-churn-root-cause_79bdbb1a94fe7547` dominant=`top_candidate_rotation`，regime mismatch `regime-mismatch-attribution_e0cf4b6989b248b4` dominant=`risk_increase_during_drawdown`，filter design `candidate-quality-filter-design_edafde5d99e021fe`=5 filters，filtered backfill `filtered-candidate-backfill_e0b3be39c9a9a33f`=5 variants / `data_quality_status=PASS_WITH_WARNINGS`，comparison `filtered-vs-original-comparison_e77634a8cf5851b3` best=`median_plus_regime_mismatch_filter` / recommendation=`PROMOTE_FOR_REVIEW`，signal gate experiment `signal-gate-experiment_cda00c7a3e8dc1ee` recommended_next_action=`continue_forward_confirmation` / formalization_ready=false，promotion review `filtered-candidate-promotion-review_ffdb14e1b2782e02` decision=`CONTINUE_TESTING`，owner roadmap `owner-signal-roadmap_64c7d31037ee42a4` recommended_owner_action=`continue_forward_confirmation_and_signal_gate_evidence`；10 个新增 validators、dynamic-v3 validation、artifact family validation、documentation contract、Reader Brief/quality、report index、ruff、compileall、git diff check、focused pytest 和 full pytest `2478 passed, 640 warnings` 已通过；保持 no official target / no broker / no production。
- 2026-07-19: ARCH-004G2.4-EB1审计确认上述2026-06-14 ledger/churn/mismatch/filter结果来自
  aggregate proxy默认值和合成forward return，不能继续作为当前研究证据。TRADING-326～330迁至canonical
  snapshot-backed foundation，改为0 dated events、null/`INSUFFICIENT_DATA`并禁止生成filters；23 views
  live replay/tamper、exact lineage/chronology、CLI parity与legacy compatibility focused=`15 passed / 71.80s`。
- 2026-07-19: EB1正式architecture/contract=`372/262 passed`；最终parent-bound Full=
  `6,295 passed / 2 skipped / 1,066.73s`，完整runtime profile/provenance与`6,297 nodes / 1,069 files`
  COMPLETE duration profile重验PASS。该验证只确认实现/证据链，未把缺失dated evidence改写为正向研究结论。
  Callback matrix由`715/252`更新为`730 migrated/237 pending`；本记录不宣告TRADING-331～335或整个
  G2.4完成，`production_effect=none`。
