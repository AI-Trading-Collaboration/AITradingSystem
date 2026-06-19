# TRADING-521 to 524 B1-B4 Diagnosis Batch

最后更新：2026-06-19

## 背景

TRADING-511E~520 已完成 B1-B4 research-only mini-backfill 与 checkpoint。
当前结论有效：

- B2 completed research-only。
- B3 completed research-only。
- B4 completed research-only，但 `interaction_effects.classification=INCONCLUSIVE`。
- B5 正确 blocked，因为 core combo inconclusive。
- B6 正确 blocked，因为没有 valid pre-regime combo。
- TRADING-520 保持 `WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`。

本批次只做 diagnosis，不继续 B5/B6/v3，不改 strategy logic，不写 official
target weights，不创建 paper-shadow、broker/order 或 production effect。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-521|B1-B4 component result attribution|VALIDATING|
|TRADING-522|B4 interaction inconclusive drilldown|VALIDATING|
|TRADING-523|E0/E1 baseline consistency audit|VALIDATING|
|TRADING-524|B4 next decision checkpoint|VALIDATING|

## 验收标准

### TRADING-521

输出 `b1_b4_component_result_attribution.json/md`，至少包含：

- B1 vs B0；
- B2 vs B0；
- B3 vs B0；
- B4 vs B0；
- B4 vs B2；
- B4 vs B3；
- return delta、drawdown delta、turnover delta、cost delta、benchmark-relative delta；
- stress result、window result、signal robustness result；
- 每个模块是否 independently useful。

比较必须披露 B0 comparator 语义。B2/B3/B4 的 module-only 主比较使用 E0 与同窗口
B0R；E1 只作为 execution-control 交互参考，不得替代 module-only attribution。

### TRADING-522

输出 `b4_interaction_inconclusive_drilldown.json/md`，解释 B4 为何仍是
`INCONCLUSIVE`，并按以下 root-cause taxonomy 标记：

- B2 weak；
- B3 weak；
- B2 and B3 conflict；
- sample window insufficient；
- utility score ambiguous；
- turnover/cost offsets benefit；
- stress result mixed；
- benchmark result mixed；
- signal robustness issue；
- interaction formula / threshold issue。

输出状态仅允许：

- `B4_INCONCLUSIVE_DUE_TO_INSUFFICIENT_EVIDENCE`
- `B4_INCONCLUSIVE_DUE_TO_NEGATIVE_INTERFERENCE`
- `B4_INCONCLUSIVE_DUE_TO_WEAK_COMPONENT`
- `B4_REQUIRES_MORE_WINDOWS`
- `B4_SHOULD_RETURN_TO_DESIGN`

### TRADING-523

输出 `e0_e1_baseline_consistency_audit.json/md`，检查：

- B0 static default portfolio；
- B1 execution baseline；
- B2 risk scaler only；
- B3 slow tilt only；
- B4 B2+B3 only；
- no P0 mixed allocator；
- no regime signal；
- no confidence shrinkage；
- no feature-store leakage。

### TRADING-524

输出 `b4_next_decision_checkpoint.json/md`。允许 decision：

- `PROCEED_TO_CONFIDENCE_SHRINKAGE`
- `RUN_MORE_B4_WINDOWS`
- `REVISE_B2_RISK_SCALER`
- `REVISE_B3_SLOW_TILT`
- `RETURN_TO_ABLATION_DESIGN`
- `STOP_CURRENT_RESEARCH_LINE`

Hard rules：

- B4 仍 inconclusive 时不得允许 B5。
- B5 invalid/blocked 时不得允许 B6。
- 不得创建 paper-shadow、live trading、official weights、broker/order。

## 实施边界

- 输入只读读取 canonical `docs/research` artifacts 和 policy config。
- 运行同一路径 data-quality gate，并在结果中披露状态。
- 不重新运行 B2/B3/B4 backfill。
- 不修改 strategy logic、threshold、scaler、target mapping 或历史 artifact。
- 输出保持 `research_only=true`、`manual_review_only=true`、
  `official_target_weights=false`、`production_effect=none`。

## 状态记录

- 2026-06-19：owner 要求先 commit/push 511A-D/B2/B3/B4/checkpoint batch，然后新建
  TRADING-521~524 diagnosis batch；明确不要继续 B5/B6/v3。
- 2026-06-19：实现只读 CLI `aits etf weight-research diagnose-b1-b4` 和 canonical
  aliases：`docs/research/b1_b4_component_result_attribution.json/md`、
  `docs/research/b4_interaction_inconclusive_drilldown.json/md`、
  `docs/research/e0_e1_baseline_consistency_audit.json/md`、
  `docs/research/b4_next_decision_checkpoint.json/md`。
- 2026-06-19：真实诊断结论为 TRADING-521
  `B1_B4_COMPONENT_ATTRIBUTION_READY`、TRADING-522 `B4_REQUIRES_MORE_WINDOWS`、
  TRADING-523 `E0_E1_BASELINE_CONSISTENCY_PASS_WITH_LIMITATIONS`、TRADING-524
  `RUN_MORE_B4_WINDOWS`；`b5_allowed=false`、`b6_allowed=false`，不继续
  B5/B6/v3，不产生 official weights、paper-shadow、broker/order 或 production effect。
