# TRADING-316 to TRADING-325 Signal-Level Diagnosis and Gate Calibration with Targeted Micro Search v4

最后更新：2026-07-16

## 状态

`IN_PROGRESS`（ARCH-004 G2.4 canonical remigration：CX1/CX2 complete，CX3 pending）

Owner 要求完成附件中的 TRADING-316～325。本阶段承接 TRADING-306～315 的 no-promotion 结论，不继续无差别扩大参数空间，而是先判断 gate、scorecard、signal、consensus 与当前 AI regime 区间是否解释了没有 promotion candidate。

## 背景

TRADING-306～315 已完成 targeted v3 诊断链路。真实输出显示：

- no-promotion review `no-promotion-review_52f5d7e23bda2288`：promoted=0，gate_assessment=`TOO_STRICT`。
- targeted v3 matrix `targeted-search-v3_599f424a15c6114a`：87 variants。
- targeted v3 backfill `targeted-v3-backfill_e7682602c7655f64`：87/87 completed，data quality=`PASS_WITH_WARNINGS`，range=`2022-12-01`～`2026-06-10`。
- promotion threshold sensitivity `promotion-threshold-sensitivity_64c3f26d9962f3e7`：base / relaxed-only 均无 promoted candidates。
- candidate promotion v2 `candidate-promotion-v2_feab730f0e665860`：decision=`RUN_ANOTHER_TARGETED_SEARCH`，promoted=0。
- next formal-or-search plan `next-formal-or-search-plan_8e375e22540acb95`：decision=`CONTINUE_SEARCH_PLAN`，recommended_next_action=`run_smaller_v4_or_signal_level_diagnosis`。

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
|TRADING-316|BASELINE_DONE_CX1|Promotion gate calibration review|`gate-calibration-review run/report` 与 `validate-gate-calibration-review` 可运行；输出 gate strictness、component impact、diagnostic relaxed gate；`can_change_official_gate=false`。|
|TRADING-317|BASELINE_DONE_CX1|Scorecard component attribution|`scorecard-attribution run/report` 与 validator 可运行；输出 rejected variants 的 component distribution、variant matrix 和 family weakness。|
|TRADING-318|BASELINE_DONE_CX1|Signal-level instability diagnosis|`signal-instability-diagnosis run/report` 与 validator 可运行；无dated ledger时events为空、counts/returns为null且结论为`INSUFFICIENT_DATA`。|
|TRADING-319|BASELINE_DONE_CX1|Candidate consensus quality review|`consensus-quality-review run/report` 与 validator 可运行；只接受exact source variant，无dated method evidence时dispersion/delta为null且结论为`INSUFFICIENT_DATA`。|
|TRADING-320|BASELINE_DONE_CX2|Near-miss micro search v4 design|`micro-search-v4-design run/report` 与 validator 可运行；生成 20～40 个 v4 variants，每个有 rationale、安全边界和 failure-mode link。|
|TRADING-321|BASELINE_DONE_CX2|Micro search v4 backfill|`micro-search-v4-backfill run/report` 与 validator 可运行；完成或明确 partial，输出 performance/regime/stability/signal metrics。|
|TRADING-322|BASELINE_DONE_CX2|Gate-calibrated candidate review|`gate-calibrated-review run/report` 与 validator 可运行；official / diagnostic gate 双轨输出，diagnostic gate 不改正式 gate。|
|TRADING-323|BASELINE_DONE_CX2|Signal vs parameter failure attribution|`signal-vs-parameter-attribution run/report` 与 validator 可运行；明确 failure_source、confidence、recommended research shift。|
|TRADING-324|PENDING_CX3|Next research direction decision pack|`next-research-direction run/report` 与 validator 可运行；生成 next decision、next task plan 和 safety boundary。|
|TRADING-325|PENDING_CX3|Owner research roadmap update|`owner-research-roadmap update/report` 与 validator 可运行；输出 owner summary、checklist、Reader Brief section。|

## Design Decisions

- Gate calibration 只做 diagnostic review，不修改 official gate policy；relaxed gate 输出只可用于 manual review evidence。
- Scorecard attribution 使用现有 v3 backfill / scorecard rows，不新增外部数据源。
- Signal diagnosis 与 consensus review 可将已有 aggregate stability / churn / regime / performance
  metrics作为明确标注的aggregate proxy rows，但不得把它们转换为dated events、event counts、
  subsequent returns、dispersion或method deltas；缺少真实逐日ledger时这些字段必须为空/null，报告
  必须标注`INSUFFICIENT_DATA`。
- v4 micro search 限制为 20～40 variants，聚焦 cash buffer、smoothing、median/top-k consensus、dispersion gate、高分歧 hold/reduce tilt、sideways hold/fast restore。
- v4 backfill 复用 targeted v3 backfill 的 cached data quality gate 与 scorecard path，输出 market regime 和 actual date range。
- 最终 decision pack 必须明确下一步继续 micro search、转向 signal feature diagnosis、candidate quality filter、gate policy review 或 defer。

## Progress Notes

- 2026-07-16: ARCH-004G2.4CX2=`COMPLETE_G2_4_CONTINUES`。12 callbacks/12 public入口完成
  canonical micro-search foundation迁移，legacy root=`12,339/297/258`，legacy weight domain=
  `7,409 lines/12 lazy wrappers`，matrix=`709/258/0/0`。四类bounded v2 snapshots、reviewed policy、
  CX1四源同Scorecard/Targeted lineage、Design→Backfill→Gate exact lineage、historical calculation/
  current quality cache role、pre-output chronology/live replay、全部views byte rebuild及output/policy/
  cross-lineage/cache-role tamper闭合。真实结论保持Design=`PASS_WITH_WARNINGS/INSUFFICIENT_DATA/
  PILOT_HYPOTHESIS_ONLY`、Gate official=`0.72`/diagnostic=`0.67`且不改policy、Attribution=
  `INCONCLUSIVE/LOW/DEFER_AND_BUILD_DATED_EVIDENCE`，不默认`MARKET_REGIME`。Focused business/
  architecture=`13/156 passed`，architecture/contract/full=`298/203/6,061 passed`，full=
  `641 warnings / 2,838.24s`；generated=`945/1,126/858/0`。旧四文件累计`3,479.43s`降为
  `269.82s/13 tests`（observed -92.25%/12.90x），full较CX1单次快约13.95%但不声明稳定
  full-suite改善。TRADING-324～325/CX3及whole G2.4仍pending，不触发ARCH-005 handoff、不进入
  G2.5，`production_effect=none`。
- 2026-07-16: ARCH-004G2.4CX2 contract freeze并进入`IN_PROGRESS`。本slice只迁
  TRADING-320～323的12 callbacks/12 public入口到canonical micro-search foundation；TRADING-
  324～325留CX3。审计确认旧Design不调用CX1 validators、不冻结四源binding且预设variant集合与
  实际`INSUFFICIENT_DATA`脱节；旧Backfill validator不重放Design/Baseline/cache/DQ也不重建views；
  旧Gate用未冻结`BATCH2_PROMOTE_SCORE=0.72`和`GATE_DIAGNOSTIC_RELAXATION=0.05`重打分；旧
  Attribution在Signal/Consensus缺dated evidence时默认落到`MARKET_REGIME/LOW`，无法作为可信
  failure-source结论。退出要求四类bounded v2 snapshots、reviewed pilot policy、CX1四源同一
  Scorecard及exact Targeted Matrix→Backfill、Design→Backfill→Gate exact lineage、历史计算cache与
  current DQ evidence角色分离、pre-output chronology/live replay与全部views byte rebuild/tamper。
  缺dated signal/consensus evidence必须输出`INCONCLUSIVE/LOW`和补齐ledger/evidence行动；固定
  research-only/no official/no auto/no order/no broker，`production_effect=none`。
- 2026-07-16: CX1 final full复验=`6,050 passed / 642 warnings / 3,298.22s`，artifact=
  `outputs/validation_runtime/full_20260715T145342Z/test_runtime_summary.json`；六个旧CX2 fixture
  失败均已闭合。Architecture/contract=`296/203 passed`。Focused共享fixture局部改善仍为
  `555.74s -> 216.42s`（2.57x），但final full较首轮`2,962.64s`反增，因此不声明稳定整体提速；
  top tail为confirmation weekly `1,353.68s`，而CX2相关micro design/backfill/gate/attribution/
  roadmap/direction单测为`726.98~900.41s`。后续CX2必须复用同一immutable upstream与
  PASS-only validation session，并保留DQ/live replay/all-view tamper gates。
- 2026-07-15: 首轮full=`6,044 passed / 6 failed / 2,962.64s`，六项同根失败均为旧CX2
  micro-search v4 fixture在2026 chronology下继续使用截至2024-02-29 cache，required DQ gate正确
  fail closed。现已把历史计算cache与独立current-quality cache分离，不改原cache SHA、生产DQ配置
  或回测窗口；DQ=`PASS_WITH_WARNINGS`且同`-n16 --dist loadfile`六文件复验=`6 passed /
  739.19s`。最终full已于2026-07-16通过；CX2仍不得视为canonical完成。
- 2026-07-15: ARCH-004G2.4CX1=`COMPLETE_G2_4_CONTINUES`。12 callbacks/12 public入口已迁
  canonical diagnosis-foundation interface/domain，legacy root=`12,693/309/270`，legacy weight
  domain=`8,063 lines/12 lazy wrappers`。四类bounded v2 snapshots、reviewed policy、same-scorecard
  与Matrix→Backfill exact lineage、pre-output chronology、live source/policy replay及全部views byte
  rebuild/tamper闭合。当前没有dated signal/method ledger，因此Signal events为空且count/return为
  null，Consensus dispersion/delta为null，两者均为`INSUFFICIENT_DATA`；这是真实证据边界，不是
  producer失败。Focused=`166 passed / 233.27s`；共享fixture对比旧`555.74s/4 tests`降为
  `216.42s/10 tests`（-61.05%/2.57x），测试policy保留6个必需family，production 60～120 variants
  门禁未改。Matrix=`697 migrated / 270 pending`；TRADING-320～323留CX2、324～325留CX3，whole
  G2.4未完成，不触发handoff、不进入G2.5，`production_effect=none`。
- 2026-07-15: ARCH-004G2.4CX1 contract freeze并进入`IN_PROGRESS`。本slice只迁
  TRADING-316～319的12 callbacks/12 public domain入口到canonical
  `dynamic_v3_signal_diagnosis_foundation.py`，TRADING-320～323留CX2、324～325留CX3。
  审计确认旧producer在写件前不调用上游content-derived validator、不冻结source/config/
  policy bytes、可混用不同Scorecard lineage，validator只检查文件/枚举/少量safety字段且不能
  重建views。旧Signal逻辑还把aggregate score换算为虚构flip/jump/false-risk count，把所有事件
  写在同一end date并填`subsequent_return=0`；Consensus会为缺失method选择任意family/首行作
  fallback，均可能把“无逐日证据”误写为“已观测事件”。CX1退出要求四类bounded v2 snapshots、
  reviewed diagnosis policy、validated Review+Sensitivity回到同一source Scorecard、validated
  Scorecard=Targeted Matrix source Scorecard及exact Matrix→Backfill lineage、pre-output chronology、
  live source/policy replay和全部views逐byte重建。无dated ledger时events必须为空、count/return/
  dispersion保持null并显式`INSUFFICIENT_DATA`；Consensus只接受exact source variant，缺失method
  不得fallback。固定diagnostic/manual research、no official/no auto/no order/no broker、
  `production_effect=none`。
- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-316～325 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 旧实现曾转入`VALIDATING`并生成全链结果。CX1审计已确认其中Signal
  `requires_signal_level_fix=True`与Consensus `no_consensus_specific_failure`依赖把aggregate score
  转为虚构dated counts/同日events/zero subsequent returns及任意fallback method，因此这两项不再
  作为当前可信研究结论；其余TRADING-320～325产物也必须在CX2/CX3完成exact lineage、bounded
  snapshot与live replay后才能重新声明当前结论。旧v4 backfill的`2022-12-01`～`2026-06-10`
  range和`PASS_WITH_WARNINGS`仅保留为historical artifact evidence，不绕过当前remigration gate。
