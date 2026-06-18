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
|TRADING-474|window-fragility-attribution|DONE|TRADING-471 + window sensitivity review|比较 early/middle/recent/stress-heavy/calm windows，判断 fragility 来源和是否可继续研究。|
|TRADING-475|stress-weakness-attribution|DONE|TRADING-471 + stress review|解释 failed stress scenarios、candidate/benchmark behavior、drawdown mismatch、rotation/turnover impact，不调参掩盖 failure。|
|TRADING-476|cost-benchmark-weakness-attribution|DONE|TRADING-471 + cost/benchmark review|解释 cost weakness、benchmark weakness、baseline 对比和 redesign repairability。|
|TRADING-477|candidate-redesign-hypothesis-v2|DONE|TRADING-471~476|基于 gap 生成 P0/P1/P2 v2 research hypotheses，只生成假设，不激活 paper-shadow。|
|TRADING-478|candidate-v2-spec-freeze|DONE|TRADING-477|冻结 research-only v2 spec，明确与 TRADING-470 candidate 的差异和 stop conditions。|
|TRADING-479|candidate-v2-executable-binding-update|DONE|TRADING-478|实现 v2 research-only binding 并通过 safety audit；若 safety audit failed，禁止 backfill。|
|TRADING-480|candidate-v2-mini-backfill|DONE|TRADING-479 safety pass|只跑 compact representative windows，输出 mini-backfill 状态。|
|TRADING-481|candidate-v2-mini-gate|DONE|TRADING-480|决定是否进入 full backfill；不允许 paper-shadow。|
|TRADING-482|candidate-v2-full-backfill-if-approved|DONE|TRADING-481 approval|只有 mini gate 为 `V2_PROCEED_TO_FULL_BACKFILL` 时才运行 full backfill。|
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
- 2026-06-18: TRADING-474 开始实现。范围限定为只读读取
  `next_candidate_overfit_window_sensitivity`、TRADING-473 signal drilldown、
  TRADING-472 repair plan 和 TRADING-471 ledger，比较 early / middle / recent /
  stress-heavy / calm-market splits，归因 regime dependence、overfit threshold、
  signal instability、turnover concentration、drawdown behavior、benchmark-relative
  weakness 和 cost sensitivity；不重新运行 backfill、不调参隐藏 fragility、不改变
  candidate spec、不触发 paper-shadow 或 production。
- 2026-06-18: TRADING-474 完成 pending commit。真实 2026-06-17 attribution
  `window_fragility_attribution_2026-06-17` 输出
  `WINDOW_FRAGILITY_ATTRIBUTION_READY`，source window sensitivity 为
  `WINDOW_FRAGILE`，source signal drilldown 为 `SIGNAL_ROBUSTNESS_REPAIRABLE`，
  source backfill repair 为 `BACKFILL_REPAIRABLE`。结论为
  `MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED`，split_count=5，fragile windows=2
  （recent_window、stress_heavy_window），under-observed windows=3
  （early_window、middle_window、calm_market_window），stable windows=0，
  overfit_risk=HIGH，acceptable_for_further_research=false，acceptance condition 为
  repair dynamic signal binding and redesign drawdown/stress handling。Failure modes
  为 drawdown_behavior_failure、under_observed_static_proxy 和 high_overfit_risk。
  Validation `window_fragility_attribution_validation_2026-06-17` 为 PASS，
  checks=13、failed=0。未重新运行 backfill、未调参隐藏 fragility、未改变 candidate
  spec、未 append owner decision、未创建 paper-shadow、未批准 extended/live、未写
  official target weights、未触发 broker/order、未修改 production。下一步是 TRADING-475
  stress weakness attribution。
- 2026-06-18: TRADING-475 开始实现。范围限定为只读读取
  `next_candidate_stress_review`、TRADING-474 window attribution、TRADING-473 signal
  drilldown、TRADING-472 repair plan 和 TRADING-471 ledger，逐 required stress scenario
  记录 candidate behavior、expected behavior、benchmark behavior、drawdown mismatch、
  rotation/flip issue、turnover impact、root cause 和 design implication；不重新运行
  stress/backfill、不调阈值隐藏 failure、不改变 candidate spec、不触发 paper-shadow 或
  production。
- 2026-06-18: TRADING-475 完成 pending commit。真实 2026-06-17 attribution
  `stress_weakness_attribution_2026-06-17` 输出
  `STRESS_WEAKNESS_ATTRIBUTION_READY`，source stress result=`WEAK`，
  source window fragility judgment=`MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED`，
  source signal drilldown=`SIGNAL_ROBUSTNESS_REPAIRABLE`，source backfill repair
  `BACKFILL_REPAIRABLE`。Required scenarios=6，source scenarios=6，
  failed scenarios=2（`slow_drawdown` FAIL 和 `v_shaped_recovery` MISSING），
  warning scenarios=4（rapid drawdown、high-volatility sideways、false risk-off
  cluster、AI / semiconductor correction），root causes=4：
  `structural_drawdown_failure`、`required_stress_scenario_missing`、
  `weak_return_drawdown_warning`、`partial_static_proxy_evidence_limit`。Design
  judgment=`REDESIGN_REQUIRED`，reject_current_candidate=false。Validation
  `stress_weakness_attribution_validation_2026-06-17` 为 PASS，checks=11、
  failed=0。未重新运行 stress/backfill、未调阈值隐藏 failure、未改变 candidate
  spec、未 append owner decision、未创建 paper-shadow、未批准 extended/live、未写
  official target weights、未触发 broker/order、未修改 production。下一步是
  TRADING-476 cost/benchmark weakness attribution。
- 2026-06-18: TRADING-476 开始实现。范围限定为只读读取
  `next_candidate_cost_benchmark_review`、其链接的 cost sensitivity source report、
  benchmark baseline control pack、TRADING-475 stress attribution、TRADING-474 window
  attribution、TRADING-472 repair plan 和 TRADING-471 ledger，逐 cost scenario 与
  required baseline 解释 high turnover / cost drag / weak gross and net proxy /
  benchmark underperformance / insufficient defensive or recovery benefit；不重新运行
  cost/backfill、不调阈值隐藏 weakness、不改变 candidate spec、不触发 paper-shadow 或
  production。
- 2026-06-18: TRADING-476 完成 pending commit。真实 2026-06-17 attribution
  `cost_benchmark_weakness_attribution_2026-06-17` 输出
  `COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY`，source cost/benchmark status 为
  `COST_BENCHMARK_REVIEW_WEAK`，source cost sensitivity 为
  `NOT_MEANINGFUL_UNDER_COSTS`，source benchmark baseline status 为
  `CANDIDATE_UNDERPERFORMS_BASELINES`。Cost scenarios=4，均为
  `weak_net_return_proxy`；benchmark baselines=5，其中 static allocation、no-trade、
  QQQ-only、SPY-only 为 `insufficient_outperformance_margin`，equal-weight ETF 为
  `benchmark_underperformance`。Root causes=6：
  `weak_gross_return_proxy`、`weak_net_return_proxy`、`turnover_cost_exposure`、
  `insufficient_benchmark_outperformance`、`benchmark_underperformance` 和
  `partial_static_proxy_distortion`。Design judgment=`REDESIGN_REQUIRED`，
  fixable_by_candidate_redesign=true，reject_current_candidate=false。Validation
  `cost_benchmark_weakness_attribution_validation_2026-06-17` 为 PASS，checks=13、
  failed=0。未重新运行 cost/benchmark/backfill、未调阈值隐藏 weakness、未改变
  candidate spec、未 append owner decision、未创建 paper-shadow、未批准 extended/live、
  未写 official target weights、未触发 broker/order、未修改 production。下一步是
  TRADING-477 candidate redesign hypothesis v2。
- 2026-06-18: TRADING-477 开始实现。范围限定为只读读取 TRADING-471~476 的 ledger /
  repair / signal / window / stress / cost-benchmark attribution artifacts，生成 P0/P1/P2
  v2 research hypotheses，覆盖 signal robustness repair、lower turnover、window
  stability、stress handling、benchmark-relative behavior 和 cost survival；不冻结 v2
  spec、不实现 binding、不运行 backfill、不创建 paper-shadow、不批准 extended/live、不写
  official target weights、不触发 broker/order、不修改 production。
- 2026-06-18: TRADING-477 完成 pending commit。真实 2026-06-17 hypotheses
  `candidate_redesign_hypothesis_v2_2026-06-17` 输出
  `CANDIDATE_REDESIGN_HYPOTHESIS_READY`，source ledger/repair/signal/window 状态分别为
  `EXECUTABLE_RESEARCH_EVIDENCE_GAP_LEDGER_READY`、`BACKFILL_REPAIRABLE`、
  `SIGNAL_ROBUSTNESS_REPAIRABLE`、`WINDOW_FRAGILITY_ATTRIBUTION_READY`，source stress
  和 cost/benchmark design judgment 均为 `REDESIGN_REQUIRED`。Hypotheses=6：
  P0=3、P1=2、P2=1；required target coverage=6/6，覆盖 signal robustness repair、
  lower turnover、window stability、stress handling、benchmark-relative behavior 和
  cost survival。Validation `candidate_redesign_hypothesis_v2_validation_2026-06-17`
  为 PASS，checks=12、failed=0。未冻结 v2 spec、未实现 binding、未运行 backfill、未
  append owner decision、未创建 paper-shadow、未批准 extended/live、未写 official target
  weights、未触发 broker/order、未修改 production。下一步是 TRADING-478 candidate v2
  spec freeze。
- 2026-06-18: TRADING-478 开始实现。范围限定为只读读取 TRADING-477 hypotheses，
  选择最高优先 P0 hypothesis 冻结 research-only v2 spec，记录 candidate id、signal /
  feature inputs、regime assumptions、drawdown handling、rotation handling、turnover
  constraints、cost expectations、benchmark expectations、validation windows、stop
  conditions 和与 TRADING-470 candidate 的差异；不实现 binding、不运行 backfill、不创建
  paper-shadow、不批准 extended/live、不写 official target weights、不触发 broker/order、
  不修改 production。
- 2026-06-18: TRADING-478 完成 pending commit。真实 2026-06-17 spec freeze
  `candidate_v2_spec_freeze_2026-06-17` 输出 `CANDIDATE_V2_SPEC_FREEZE_READY`，
  selected hypothesis=`v2_turnover_cost_benchmark_guard`、priority=P0、target_count=3，
  candidate id=`median_plus_regime_mismatch_filter_v2_turnover_cost_benchmark_guard`，
  market_regime=`ai_after_chatgpt`，requested_date_range=`2023-01-03..2025-04-30`，
  difference_count=4、stop_condition_count=5，paper_shadow_eligible=false。
  Validation `candidate_v2_spec_freeze_validation_2026-06-17` 为 PASS，checks=9、
  failed=0。未实现 binding、未运行 backfill、未 append owner decision、未创建
  paper-shadow、未批准 extended/live、未写 official target weights、未触发 broker/order、
  未修改 production。下一步是 TRADING-479 candidate v2 executable binding update。
- 2026-06-18: TRADING-479 开始实现。范围限定为读取 TRADING-478 frozen v2 spec，
  生成 research-only v2 signal binding、hypothetical research weight binding 和 executable
  binding safety audit summary，保持 research_only/manual_review_only、official target
  weights=false、production/broker/order effect=none；缺失、stale 或 invalid input 必须
  fail closed。若 safety audit failed，本批次不得进入 TRADING-480 mini backfill。
- 2026-06-18: TRADING-479 完成 pending commit。真实 2026-06-17 binding update
  `candidate_v2_executable_binding_update_2026-06-17` 输出
  `CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS`，candidate id=
  `median_plus_regime_mismatch_filter_v2_turnover_cost_benchmark_guard`。
  Signal rows=396、weight rows=396，covered validation contexts=6/7；未覆盖
  `v_shaped_recovery`，原因是 source backfill window dates missing。Latest signal date=
  `2025-04-30`，average_turnover_proxy=0.031818，latest_turnover_proxy=0.0。
  Data quality=`PASS_WITH_WARNINGS`，safety audit=`EXECUTABLE_BINDING_SAFETY_WARNING`
  且 allows mini backfill=true。Validation
  `candidate_v2_executable_binding_update_validation_2026-06-17` 为 PASS，checks=12、
  failed=0。未运行 backfill、未生成 return/drawdown/cost/benchmark metrics、未 append
  owner decision、未创建 paper-shadow、未批准 extended/live、未写 official target
  weights、未触发 broker/order、未修改 production。下一步是 TRADING-480 candidate v2
  mini backfill。
- 2026-06-18: TRADING-480 开始实现。范围限定为读取 TRADING-479 v2 binding update，
  仅在 safety audit pass 或 acceptable warning 时运行 research-only compact mini backfill；
  覆盖 normal、drawdown、high-volatility sideways 和 false risk-off representative windows，
  输出 return proxy、drawdown proxy、turnover、rotation count、cost proxy inputs 和 signal
  completeness。不运行 full backfill、不创建 paper-shadow、不写 official target weights、
  不触发 broker/order、不修改 production。
- 2026-06-18: TRADING-480 完成 pending commit。真实 2026-06-17 mini backfill
  `candidate_v2_mini_backfill_2026-06-17` 输出 `V2_MINI_BACKFILL_WEAK`；
  source binding=`CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS`，
  source safety audit=`EXECUTABLE_BINDING_SAFETY_WARNING`，data quality=
  `PASS_WITH_WARNINGS` 且 passed=true。四个代表窗口均 COMPLETE：normal market
  regime return/drawdown proxy=0.367401/-0.065783，slow drawdown=-0.056003/-0.184629，
  high-volatility sideways=-0.029807/-0.109893，false risk-off=-0.066467/-0.076432；
  aggregate return/drawdown proxy=0.053781/-0.184629，turnover_proxy=1.775，
  rotation_count=13，false_risk_off_count=42，signal_completeness_ratio=1.0。
  Validation `candidate_v2_mini_backfill_validation_2026-06-17` 为 PASS，checks=10、
  failed=0。结论为 weak，因此按硬停止条件 TRADING-481 mini gate 必须阻止 full
  backfill，除非后续 owner 明确选择新的修复任务；未运行 full backfill、未生成
  benchmark conclusion、未创建 paper-shadow/official weights/broker/order/production。
- 2026-06-18: TRADING-481 开始实现。范围限定为只读读取 TRADING-478 v2 spec、
  TRADING-479 v2 binding safety audit、TRADING-480 mini backfill 与 validation、
  signal robustness quick check、turnover/cost quick check，输出
  `V2_PROCEED_TO_FULL_BACKFILL` / `V2_NEEDS_REDESIGN` / `V2_REJECT_RESEARCH_CANDIDATE`
  / `V2_BLOCKED` gate decision、strongest positive/negative evidence、Reader Brief
  和 validation artifact。该 gate 不运行 full backfill、不允许 paper-shadow、不写 official
  target weights、不触发 broker/order、不修改 production。
- 2026-06-18: TRADING-481 完成 pending commit。真实 2026-06-17 mini gate
  `candidate_v2_mini_gate_2026-06-17` 输出 `V2_NEEDS_REDESIGN`，
  full_backfill_allowed=false，blocked_reason=`mini_backfill_weak`；source spec=
  `CANDIDATE_V2_SPEC_FREEZE_READY`，source binding=
  `CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS`，source mini backfill=
  `V2_MINI_BACKFILL_WEAK`，source mini validation=PASS，safety audit=
  `EXECUTABLE_BINDING_SAFETY_WARNING`，data quality=`PASS_WITH_WARNINGS`。Strongest
  positive evidence=5（窗口完整、signal completeness=1.0、normal return proxy 正、
  cost proxy inputs available、dynamic signal rows available）；strongest negative
  evidence=6（mini weak、slow_drawdown/high_volatility_sideways/false_risk_off 三个
  return proxy 为负、v_shaped_recovery 未覆盖、source binding warning）。
  Validation `candidate_v2_mini_gate_validation_2026-06-17` 为 PASS，checks=10、
  failed=0。按 gate 决策不得运行 TRADING-482 full backfill；未运行 full backfill、
  未创建 paper-shadow/official weights/broker/order/production。
- 2026-06-18: TRADING-482 开始实现。范围限定为先读取 TRADING-481 mini gate；
  若 gate 不是 `V2_PROCEED_TO_FULL_BACKFILL`，输出 blocked artifact，披露 return
  proxy、drawdown proxy、turnover、rotation count、false risk-off count、cost inputs、
  benchmark inputs 和 signal completeness 均因 mini gate 未批准而未生成；不运行 full
  backfill、不补造 metrics、不创建 paper-shadow、不写 official target weights、不触发
  broker/order、不修改 production。
- 2026-06-18: TRADING-482 完成 pending commit。真实 2026-06-17 full-backfill-if-approved
  `candidate_v2_full_backfill_if_approved_2026-06-17` 输出
  `V2_FULL_BACKFILL_BLOCKED_BY_MINI_GATE`；source mini gate decision=
  `V2_NEEDS_REDESIGN`，source mini gate validation=PASS，full_backfill_executed=false，
  required outputs=8、generated=0、blocked=8，return proxy、drawdown proxy、turnover、
  rotation count、false risk-off count、cost inputs、benchmark inputs 和 signal completeness
  均以 `mini_gate_decision:V2_NEEDS_REDESIGN` 标记为 not generated。Validation
  `candidate_v2_full_backfill_if_approved_validation_2026-06-17` 为 PASS，checks=9、
  failed=0。未运行 full backfill、未补造 metrics、未创建 paper-shadow/official
  weights/broker/order/production。后续 TRADING-483 research gate 只能读取该 blocked
  state，不得声称 v2 full backfill 已完成。
