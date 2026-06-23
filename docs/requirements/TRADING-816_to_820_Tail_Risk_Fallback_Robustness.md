# TRADING-816 to TRADING-820: Tail-Risk Fallback Robustness
最后更新：2026-06-23

## Context

TRADING-815 的真实 controlled review 给出 `CONTINUE`，并且 TRADING-814 的最佳 variant 为
`tail_risk_benchmark_fallback`：tail_loss_reduction=1.0，turnover/cost 不恶化，holdout_pass_rate=1.0。
这说明 benchmark-first fallback 比原始 value surface 更像可持续策略模块，但它仍然只是
controlled research candidate。

下一阶段不证明 promotion readiness，而是验证：

- fallback 是否在更多切片下仍稳健；
- fallback 触发是否有 precision/recall，而不是粗暴少交易；
- tail loss reduction 是否明显大于 missed upside cost；
- forward dry-run archive 是否能每日记录 fallback signal、trigger reason 和未来 maturity outcome。

## Safety Boundary

本批全部输出仍为 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不进入 paper-shadow，不生成 official target weights，不触发 broker/order，不把 fallback 结果
  解释为 production risk policy。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-816 | Tail-Risk Benchmark Fallback Robustness Expansion | 扩大 `tail_risk_benchmark_fallback` 的 controlled review，输出 trigger/frequency、tail loss、mean/median delta、upside capture、missed upside、turnover/cost/drawdown delta 和 by asset/horizon/regime/cluster | VALIDATING |
| TRADING-817 | Fallback Trigger Precision / Recall Audit | 定义 true/false positive/negative，输出 precision、recall、false positive/negative rates、missed upside from false positive、tail loss from false negative | VALIDATING |
| TRADING-818 | Opportunity Cost / Upside Capture Review | 审计 benchmark upside days、strategy participation、upside capture ratio、missed upside cases/concentration，并判断 tail loss reduction 是否大于 missed upside cost | VALIDATING |
| TRADING-819 | Forward Evidence Integration for Tail-Risk Policy | 将 tail-risk benchmark fallback 加入 forward dry-run archive，记录 benchmark output、fallback signal、trigger/reason、expected avoided risk 和未来 maturity placeholder | VALIDATING |
| TRADING-820 | Tail-Risk Policy Controlled Review Board | 汇总 816～819，决策只能为 CONTROLLED_RESEARCH_CONTINUE/WATCHLIST_FORWARD_MATURITY/WATCHLIST/PIVOT_OVERCONSERVATIVE/KILL/DATA_REQUIRED | VALIDATING |

## Implementation Plan

1. 在 `config/research/controlled_strategy_next_stage_research.yaml` 中新增 TRADING-816～820
   policy；所有阈值都是 controlled diagnostic / owner-review sorting 规则，不是生产风控边界。
2. 新增五个 runner 和 CLI：
   - `aits research strategies tail-risk-benchmark-fallback-robustness-expansion`
   - `aits research strategies tail-risk-fallback-trigger-precision-recall-audit`
   - `aits research strategies tail-risk-opportunity-cost-upside-capture-review`
   - `aits research strategies tail-risk-forward-evidence-integration`
   - `aits research strategies tail-risk-policy-controlled-review-board`
3. 更新 report registry、artifact catalog、system flow 和 focused tests。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-816 结论只能为 `CONTINUE|WATCHLIST|KILL|DATA_REQUIRED`，不能 promotion。
- TRADING-817 必须定义 true_positive、false_positive、false_negative、true_negative；
  fallback_precision / recall 不能只按 row count 披露，还必须披露 missed upside 和 false-negative tail loss。
- TRADING-818 不能要求 missed upside 为 0；通过条件是 tail loss reduction 明显大于 missed upside cost。
- TRADING-819 必须保持 append-only / forward evidence semantics，future outcome 只能是 maturity placeholder
  或 append-only later outcome，不能回填未来结果。
- TRADING-820 即使结果继续很好，也最多输出 `CONTROLLED_RESEARCH_CONTINUE`，不得进入 paper-shadow。

## Progress Notes

- 2026-06-22：新增本需求文档并进入 `IN_PROGRESS`；owner 明确当前主线从初步
  tail-risk policy family review 推进到 fallback robustness、trigger quality、opportunity cost 和
  forward evidence integration，仍禁止 promotion、paper-shadow 和 production mutation。
- 2026-06-22：实现完成并转入 `VALIDATING`；真实 CLI run 输出：
  TRADING-816 `TAIL_RISK_FALLBACK_ROBUSTNESS_EXPANDED`，
  robustness_decision=`CONTINUE`，fallback_trigger_count=554，
  fallback_frequency=0.257435，tail_loss_reduction=1.0，
  mean_delta_vs_benchmark=0.003607，median_delta_vs_benchmark=0.0005，
  upside_capture=1.002927，missed_upside_count=0，false_fallback_count=0，
  turnover_delta=0，cost_delta=0，max_drawdown_delta=0；
  TRADING-817 `TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_AUDITED`，
  true_positive=311，false_positive=0，false_negative=0，true_negative=1598，
  fallback_precision=1.0，fallback_recall=1.0，false_positive_rate=0，
  false_negative_rate=0，missed_upside_from_false_positive=0，
  tail_loss_from_false_negative=0；
  TRADING-818 `TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_REVIEWED`，
  benchmark_upside_case_count=1448，strategy_participation=1.0，
  upside_capture_ratio=1.002927，missed_upside_cost=0，
  avoided_tail_loss=58.678235；
  TRADING-819 `TAIL_RISK_FORWARD_EVIDENCE_INTEGRATED`，
  forward_record_count=250，fallback_trigger_count=67，
  future_outcome_status=`pending_maturity`，append_only_integrity_pass=true；
  TRADING-820 `TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_COMPLETE`，
  decision=`CONTROLLED_RESEARCH_CONTINUE`，robustness/trigger_quality/opportunity_cost/
  forward_integration conditions all true。
  所有 artifacts 固定 promotion_gate_allowed=false、paper_shadow_change_allowed=false、
  production_weight_change_allowed=false、broker_action=none、production_effect=none。
- 2026-06-22：验证通过。完整 `tests/test_controlled_strategy_batch.py` 并行 pytest
  73 passed；`fast-unit` 178 passed，runtime artifact：
  `outputs/validation_runtime/fast-unit_20260621T154905Z/test_runtime_summary.json`；
  `contract-validation` 177 passed，runtime artifact：
  `outputs/validation_runtime/contract-validation_20260621T155820Z/test_runtime_summary.json`；
  `report-validation` 55 passed，runtime artifact：
  `outputs/validation_runtime/report-validation_20260621T160745Z/test_runtime_summary.json`。
  Ruff、Black check、compileall、`git diff --check` 和 task-register terminal-status scan
  均通过。
