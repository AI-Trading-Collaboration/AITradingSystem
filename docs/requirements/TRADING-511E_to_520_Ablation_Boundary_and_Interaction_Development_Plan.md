# TRADING-511E to 520 Ablation Boundary and Interaction Development Plan

最后更新：2026-06-19

## 背景

本计划承接 TRADING-511A-D。当前 B1 mini-backfill 已完成，但结果只能作为 runner
smoke test 和 mixed research-only evidence；它尚不能直接解释为纯粹
execution/no-trade/turnover-control 模块有效。

本批次的首要目标不是强行跑完 B2-B6，而是先修复消融归因边界：

- 明确 Feature、Signal、Target、Execution、Evaluation 五层边界。
- 将现有 B0 拆成 B0H static buy-and-hold / natural drift reference 与 B0R static target
  + naive deterministic rebalance reference。
- 将 B1 重新定义为 B1E：与 B0R 使用同一静态目标路径，但加入 execution/no-trade/
  turnover control。
- 后续 R/T/C/G 模块只能在通过前置归因与 signal diagnostics gate 后继续。
- untouched holdout 不得在 development、mini-backfill 或 full-backfill 阶段提前使用。

## 阶段拆解

|任务|目标|状态|
|---|---|---|
|TRADING-511E|审计当前 B1 metric semantics 与 comparator attribution|DONE|
|TRADING-511F|实现 B0H/B0R baseline family 与 B0R vs B0H comparison|DONE|
|TRADING-511G|用 B0R 重新运行 B1E，输出 B1 attribution gate|DONE_VALID_MIXED|
|TRADING-512A|冻结五层 research interface contract 与 dependency boundary validation|DONE|
|TRADING-512B|建立 canonical signal diagnostics framework|DONE|
|TRADING-512C-F|B2 fast asymmetric risk scaler signal、diagnostics、target mapping、E0/E1 mini-backfill|DONE_RESEARCH_ONLY|
|TRADING-513A-D|B3 slow relative tilt signal、diagnostics、target mapping、E0/E1 mini-backfill|DONE_RESEARCH_ONLY|
|TRADING-514A-B|B4 R x T combination contract and interaction mini-backfill|DONE_INCONCLUSIVE|
|TRADING-515A-B|B5 confidence shrinkage contract and interaction review|CONTRACT_DONE_REVIEW_BLOCKED|
|TRADING-516A-B|B6 regime information contract and incremental evaluation|CONTRACT_DONE_EVALUATION_BLOCKED|
|TRADING-517-520|main/interaction synthesis, candidate v3 spec/gate, program checkpoint|CHECKPOINT_NEEDS_MORE_EVIDENCE|

## Hard Stops

- B1 无法形成有效归因时停止，不继续 B2/B3。
- B0R 与 B1E 目标路径不一致时停止。
- Signal layer 直接输出权重时停止。
- Allocation/Target layer 重新计算隐藏 feature 或引入未声明 signal 时停止。
- Evaluation layer 修改 feature/signal/target 时停止。
- Risk/tilt signal diagnostics 为 BLOCKED 时停止。
- untouched holdout 被提前访问时停止。
- 任何输出产生 official target weights、paper-shadow activation、broker/order、live trading
  或 production mutation 语义时停止。

## 验收标准

### TRADING-511E

- 新增 audit artifact，不修改历史 B1 artifact。
- 冻结 `return_delta`、`drawdown_reduction`、`turnover_delta` 的单位、比较基准和方向。
- 明确当前 B0/B1 比较是否满足纯模块归因。
- 输出 `B1_ATTRIBUTION_VALID`、`B1_ATTRIBUTION_PARTIAL` 或 `B1_ATTRIBUTION_INVALID`。

### TRADING-511F

- B0H 输出 static hold / natural drift result。
- B0R 输出 static target + naive deterministic rebalance result。
- B0R 不使用任何市场信号、deadband、cost threshold 或 turnover optimization。
- B0R 与 B1E 使用同一目标路径、同一窗口和同一 execution lag。
- 输出 B0R vs B0H comparison。

### TRADING-511G

- B1E 以 B0R 为 primary comparator。
- 输出 `B1_ATTRIBUTION_VALID_POSITIVE`、`B1_ATTRIBUTION_VALID_MIXED`、
  `B1_ATTRIBUTION_VALID_NEGATIVE` 或 `B1_ATTRIBUTION_INVALID`。
- 若 B1E 为 mixed/negative，后续 B2/B3 仍可研究，但 E 不得默认进入最终候选，
  R/T 必须保留 E0 和 E1 变体。

### TRADING-512A/B

- 输出 `research_layer_interface_contract.json`、`dependency_boundary_validation.json` 和
  Reader Brief。
- Evaluator 不得导入 signal/allocator 实现；signal module 不得导入 execution/evaluation；
  allocator 只能消费 signal artifact；execution 只能消费 target path artifact。
- Signal diagnostics 只评价 signal coverage/freshness/schema/missingness/state transitions/
  cross-window stability/event diagnostics/robustness status，不评价组合收益。

### TRADING-512C-F / 513A-D / 514A-B

- B2 输出 `b2_risk_scaler_research_result.json`，状态
  `B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY`，signal diagnostics PASS，
  未访问 untouched holdout。
- B3 输出 `b3_relative_tilt_research_result.json`，状态
  `B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY`，signal diagnostics PASS，
  未访问 untouched holdout。
- B4 输出 `b4_risk_tilt_interaction_result.json`，状态
  `B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY`，但 interaction
  classification 为 `INCONCLUSIVE`，原因是只具备 return/drawdown/turnover partial
  utility，尚缺 full scorecard、stress、benchmark 和 signal robustness penalty gates。

### TRADING-515-520

- `confidence_shrinkage_contract.json` 与 `regime_information_contract.json` 已冻结
  research-only contract。
- `confidence_interaction_review.json` 状态为
  `CONFIDENCE_INTERACTION_BLOCKED_CORE_COMBO_INCONCLUSIVE`。
- `regime_incremental_evaluation.json` 状态为
  `REGIME_INCREMENTAL_EVALUATION_BLOCKED_NO_PRE_REGIME_COMBO`。
- `main_interaction_effect_synthesis.json` 输出 `INCONCLUSIVE`；
  `candidate_v3_spec_from_proven_effects.json` 输出 `V3_SPEC_BLOCKED_NO_PROVEN_EFFECTS`；
  `candidate_v3_mini_gate_result.json` 输出 `V3_BLOCKED`。
  `weight_research_program_v1_snapshot.json` 保持
  `WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`。

## 状态记录

- 2026-06-19：owner 附件要求继续推进 TRADING-511E-520。按项目规则先登记本计划，
  当前实现从 Phase 0 开始；B2-B6 不会绕过 B1 attribution gate、signal diagnostics gate
  或 untouched holdout policy。
- 2026-06-19：TRADING-511E 输出 `B1_ATTRIBUTION_PARTIAL`，确认历史 B1 不可作为纯
  execution attribution；TRADING-511F 输出 B0H/B0R 双基准；TRADING-511G 输出
  `B1_ATTRIBUTION_VALID_MIXED`，B2/B3 可以继续但 E 不得默认进入最终候选。TRADING-512A/B
  已输出五层接口、dependency validation PASS 和 signal diagnostics framework。下一步是
  TRADING-512C/513A 独立 signal artifact，不得跳过 diagnostics gate。
- 2026-06-19：TRADING-512C-F、513A-D、514A-B 已生成 B2/B3/B4 research-only
  mini-backfill artifacts；B4 interaction classification 为 `INCONCLUSIVE`。TRADING-515A
  与 516A contract 已冻结，但 515B/516B/517-519 按 hard stop 输出 blocked/inconclusive
  checkpoint；TRADING-520 snapshot 为 `WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE`。
