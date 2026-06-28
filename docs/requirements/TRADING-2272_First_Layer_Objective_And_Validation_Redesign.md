# TRADING-2272 First-Layer Objective And Validation Redesign

最后更新：2026-06-28

## 背景

TRADING-2270 已把 current first-layer baseline 的 failure taxonomy、
benchmark consistency 和 regime slices 固化为 diagnostic evidence。TRADING-2271
进一步证明免费 / 低成本 proxy 不能替代 true PIT breadth。附件下一步要求把这些发现
转成可审计的 objective / validation contract，而不是继续沿用隐含的 first-layer
performance interpretation。

## 范围

- 定义并固化 first-layer objective terms：
  - `false_risk_on_cost`
  - `false_risk_off_cost`
  - `drawdown_warning_lead_time`
  - `recovery_delay_days`
  - `regime_flip_penalty`
  - `benchmark_consistency_score`
  - `stress_slice_minimum_requirements`
- 从 TRADING-2270 current-state artifacts 读取当前 baseline evidence。
- 从 TRADING-2271 proxy coverage audit 读取 true breadth replacement blocker。
- 生成 objective term contract、validation redesign summary 和 owner review report。
- 审计现有 first-layer performance gates 的边际 actual-path utility：对每个 gate 执行
  `no_gate`、`relaxed_gate`、`current_gate`、`strict_gate` 反事实 acceptance，并汇总
  accepted / rejected candidates 的 frozen second-layer actual-path utility proxy。

## 非目标

- 不重新训练 first-layer model。
- 不修改 active first-layer composer、selection rule 或 reopen gate。
- 不把 proxy audit 里的 ETF price ratio、listing status 或 holdings gate 当作 true breadth。
- 不把 redesigned objective 直接解释为 validation-ready、promotion、paper-shadow、production 或 broker evidence。

## 验收标准

- 生成 `first_layer_objective_validation_redesign.json`。
- 生成 `first_layer_objective_validation_redesign.yaml`。
- 生成 `first_layer_objective_validation_redesign.md`。
- 每个 objective term 必须有 `term_id`、`definition`、`measurement_source`、`direction`、
  `current_baseline_value`、`validation_role` 和 `promotion_interpretation`。
- Stress slice minimum requirements 必须显式披露 2022 bear / rate-shock slice 当前
  `NO_SIGNAL_COVERAGE`，因此不能宣称 stress validation。
- Reports 披露 `market_regime=ai_after_chatgpt`、requested range、actual signal range、
  data quality status、proxy replacement status 和固定安全边界。

## 进展

- 2026-06-28：调整并进入 `IN_PROGRESS`；本批只定义 objective / validation contract，
  不训练模型、不改变 active gates、不进入 TRADING-2273 challenger experiments。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-objective-validation-redesign`、objective validation policy、
  JSON/YAML/Markdown artifacts 和 focused tests。真实 run data_quality_status=`PASS_WITH_WARNINGS`，
  objective_term_count=7，false_risk_on_cost=198，false_risk_off_cost=499，
  drawdown_warning_lead_time=5，recovery_delay_days=5，regime_flip_penalty=`1.956242`，
  benchmark_consistency_score=`0.392621`，stress slice coverage=`3/4 slices covered`；
  2022 slice 仍为 `NO_SIGNAL_COVERAGE`，true_breadth_replaced=false，validation/promotion
  /paper-shadow/production/broker 全部 false/none/BLOCKED。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（2 passed）、
  governance focused parallel pytest（45 passed）、`python -m ai_trading_system.cli docs validate-freshness`、
  `git diff --check` 和 `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  （193 passed）；runtime artifact=`outputs/validation_runtime/contract-validation_20260628T124530Z/test_runtime_summary.json`。
