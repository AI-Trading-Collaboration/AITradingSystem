# TRADING-471 to TRADING-485 Evidence Repair and Candidate Revision Batch

最后更新：2026-06-18

## Status

Status: IN_PROGRESS.

## 背景

本批次承接 TRADING-470 executable research-cycle snapshot。最新已确认结论是
`EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE`，research gate 为
`NEEDS_MORE_EVIDENCE`。当前候选已有 executable research binding，但证据不足：
backfill 仍为 `CANDIDATE_BACKFILL_PARTIAL`，stress 为 `WEAK`，cost / benchmark
为 `COST_BENCHMARK_REVIEW_WEAK`，vs-returned comparison 为
`MIXED_VS_RETURNED_CANDIDATE`，signal robustness 为
`SIGNAL_ROBUSTNESS_BLOCKED`，window sensitivity 为 `WINDOW_FRAGILE`。

本批次仍是 research-only / manual-review-only。禁止 paper-shadow activation、
extended shadow、live trading、official target weights、broker/order artifacts、
owner decision append、production mutation 或 automatic position control。

## 阶段拆解

|Step|任务|状态|依赖|验收标准|
|---|---|---|---|---|
|TRADING-471|executable-research-evidence-gap-ledger|DONE|TRADING-470 artifacts|生成 `executable_research_evidence_gap_ledger` JSON/Markdown 和 validation artifact；ledger 逐项记录 gap id、source report、current/expected value、root cause category、fix type、blocking、candidate redesign requirement；Reader Brief 披露非聚合原因。|
|TRADING-472|backfill-partial-root-cause-and-repair-plan|DONE|TRADING-471 + latest backfill|解释每个 incomplete window 的缺失日期/feature/signal/schema/coverage/binding issue，并输出 repairability 状态。|
|TRADING-473|signal-robustness-blocker-drilldown|DONE|TRADING-471 + signal robustness review|列出每个 blocker 的 input artifact、failed field、expected/actual value 和 repair path，不放松 signal completeness rules。|
|TRADING-474|window-fragility-attribution|READY|TRADING-471 + window sensitivity review|比较 early/middle/recent/stress-heavy/calm windows，判断 fragility 来源和是否可继续研究。|
|TRADING-475|stress-weakness-attribution|READY|TRADING-471 + stress review|解释 failed stress scenarios、candidate/benchmark behavior、drawdown mismatch、rotation/turnover impact，不调参掩盖 failure。|
|TRADING-476|cost-benchmark-weakness-attribution|READY|TRADING-471 + cost/benchmark review|解释 cost weakness、benchmark weakness、baseline 对比和 redesign repairability。|
|TRADING-477|candidate-redesign-hypothesis-v2|READY|TRADING-471~476|基于 gap 生成 P0/P1/P2 v2 research hypotheses，只生成假设，不激活 paper-shadow。|
|TRADING-478|candidate-v2-spec-freeze|READY|TRADING-477|冻结 research-only v2 spec，明确与 TRADING-470 candidate 的差异和 stop conditions。|
|TRADING-479|candidate-v2-executable-binding-update|READY|TRADING-478|实现 v2 research-only binding 并通过 safety audit；若 safety audit failed，禁止 backfill。|
|TRADING-480|candidate-v2-mini-backfill|READY|TRADING-479 safety pass|只跑 compact representative windows，输出 mini-backfill 状态。|
|TRADING-481|candidate-v2-mini-gate|READY|TRADING-480|决定是否进入 full backfill；不允许 paper-shadow。|
|TRADING-482|candidate-v2-full-backfill-if-approved|READY|TRADING-481 approval|只有 mini gate 为 `V2_PROCEED_TO_FULL_BACKFILL` 时才运行 full backfill。|
|TRADING-483|candidate-v2-research-gate|READY|TRADING-482 full backfill|输出 v2 research gate；即使 promising 也不激活 paper-shadow。|
|TRADING-484|v2-owner-research-review-packet|READY|TRADING-483|准备 owner research options；不自动 append owner decision。|
|TRADING-485|v2-research-cycle-snapshot|READY|TRADING-484|生成最终 v2 research-cycle snapshot。|

## 硬停止条件

- 如果 signal robustness 不可修复，必须先 redesign candidate。
- 如果 window fragility 来自 overfit risk，不得继续同一 candidate。
- 如果 stress weakness 是结构性问题，不得继续同一 candidate。
- 如果 v2 mini-backfill 为 weak，不得运行 full backfill。
- 如果 v2 research gate 不是 promising，不得准备 paper-shadow task。
- 本批次任何阶段都不得批准 paper-shadow、extended shadow、live trading、
  official target weights、broker/order 或 production mutation。

## 进展记录

- 2026-06-18: 新增本批次需求文档和任务登记。TRADING-471 开始实现，范围限定为
  从现有 TRADING-470 source artifacts 生成非聚合 evidence gap ledger、Reader Brief
  和 focused validation；不运行上游、不修补 metrics、不改变 candidate 行为。
- 2026-06-18: TRADING-471 完成 pending commit。真实 2026-06-17 ledger
  `executable_research_evidence_gap_ledger_2026-06-17` 输出
  `EXECUTABLE_RESEARCH_EVIDENCE_GAP_LEDGER_READY`，读取 12 个 TRADING-470 source
  artifacts，生成 33 个 gap rows：backfill coverage 6、signal robustness 3、
  window fragility 5、stress weakness 6、cost/benchmark weakness 6、comparison
  weakness 7；blocking gaps=25，candidate redesign gaps=8。Validation
  `executable_research_evidence_gap_ledger_validation_2026-06-17` 为 PASS，
  checks=14、failed=0。未运行上游、未补造 metrics、未调参、未 append owner
  decision、未创建 paper-shadow、未批准 extended/live、未写 official target weights、
  未触发 broker/order、未修改 production。下一步是 TRADING-472 backfill partial
  root-cause and repair plan。
- 2026-06-18: TRADING-472 开始实现。范围限定为只读读取最新
  `next_candidate_backfill` 和已验证的 TRADING-471 evidence gap ledger，逐窗口记录
  missing dates / feature inputs / signal outputs / schema / market coverage /
  binding execution issue；不重新运行 backfill、不补造逐日 signal 缺口、不削弱
  completeness rules、不改变 candidate spec、不触发 paper-shadow 或 production。
- 2026-06-18: TRADING-472 完成 pending commit。真实 2026-06-17 repair plan
  `backfill_partial_root_cause_repair_plan_2026-06-17` 输出 `BACKFILL_REPAIRABLE`；
  source backfill 为 `CANDIDATE_BACKFILL_PARTIAL`，required windows=6、
  incomplete windows=6、binding-repairable windows=6、data-repairable windows=0、
  candidate-spec issue windows=0。所有 incomplete windows 均披露
  `missing_dates_status=not_enumerated_in_source_artifact`，缺失 signal output 为
  window-level `historical_signal_series:<window>`，artifact-level root causes 为
  `historical_dynamic_binding_unavailable`、`single_point_signal_binding_used_as_static_proxy`
  和 `single_point_weight_binding_used_as_static_proxy`。Validation
  `backfill_partial_root_cause_repair_plan_validation_2026-06-17` 为 PASS，
  checks=14、failed=0。未重新运行 backfill、未补造逐日日期或 metrics、未改变
  candidate spec、未削弱 signal completeness rules、未 append owner decision、未创建
  paper-shadow、未批准 extended/live、未写 official target weights、未触发 broker/order、
  未修改 production。下一步是 TRADING-473 signal robustness blocker drilldown。
- 2026-06-18: TRADING-473 开始实现。范围限定为只读读取
  `next_candidate_signal_robustness_review`、validated signal binding、
  TRADING-472 repair plan 和 TRADING-471 ledger，逐 blocker 输出 exact input
  artifact、failed field、expected/actual value、repair path 和是否可在不放松
  signal completeness rules 的前提下修复；不刷新 signal inputs、不补造 historical
  signal series、不放宽 completeness rules、不改变 candidate spec、不触发 paper-shadow
  或 production。
- 2026-06-18: TRADING-473 完成 pending commit。真实 2026-06-17 drilldown
  `signal_robustness_blocker_drilldown_2026-06-17` 输出
  `SIGNAL_ROBUSTNESS_REPAIRABLE`；source signal robustness status 为
  `SIGNAL_ROBUSTNESS_BLOCKED`，source signal binding 为
  `CANDIDATE_SIGNAL_BINDING_COMPLETE_WITH_WARNINGS`，source backfill repair plan 为
  `BACKFILL_REPAIRABLE`。Blocker rows=3：`partial_signal_series`
  -> `binding_fail_closed_condition`，`stale_signal_series` -> `stale_signal_series`，
  `market_coverage_gap` -> `partial_market_coverage`；repairable blockers=3，
  repairable_without_rule_relaxation=true，candidate redesign blockers=0，
  not repairable blockers=0，signal_completeness_rules_relaxed=false。Validation
  `signal_robustness_blocker_drilldown_validation_2026-06-17` 为 PASS，
  checks=14、failed=0。未刷新 signal inputs、未补造 historical signal series、未放松
  completeness rules、未改变 candidate spec、未 append owner decision、未创建
  paper-shadow、未批准 extended/live、未写 official target weights、未触发 broker/order、
  未修改 production。下一步是 TRADING-474 window fragility attribution。
