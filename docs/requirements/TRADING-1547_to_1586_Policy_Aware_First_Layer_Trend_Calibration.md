# TRADING-1547 to 1586 Policy-Aware First-Layer Trend Calibration

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1547_to_1586_POLICY_AWARE_FIRST_LAYER_TREND_CALIBRATION`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-27
- Market regime: `ai_after_chatgpt`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 编号说明

Owner 附件使用 TRADING-1546～1585，但 `TRADING-1527_to_1546_DEFENSIVE_OVERLAY_GATE_NO_SURVIVOR_DIAGNOSIS` 已占用 TRADING-1546 收尾编号。为避免 ID 复用，本批登记为 TRADING-1547～1586。附件中的原 TRADING-1546 映射为本批 TRADING-1547，后续顺延。

## 背景

Expanded QQQ / SGOV / TQQQ actual-path research 与 defensive overlay diagnosis 已确认：

- full allocation survivor count = 0；
- defensive overlay gate pass count = 0；
- `limited_adjustment` 只是 research-only watch pending split evidence；
- 部分 state candidates 有 drawdown / Calmar 诊断价值，但不能证明 full allocation edge。

本批不继续直接搜索权重，而是检验第一层 `PIT indicators -> trend_state` 是否有可验证 edge。用于反向校准第一层的 second-layer probes 必须是动态、冻结、trend-sensitive 的，否则无法反映第一层输出是否有价值。

## 任务映射

| New ID | Attachment item | Scope |
|---|---|---|
| TRADING-1547 | original 1546 | Scope and architecture |
| TRADING-1548 | original 1547 | Dynamic second-layer probe registry |
| TRADING-1549 | original 1548 | Probe validation |
| TRADING-1550 | original 1549 | Action-value matrix generator |
| TRADING-1551 | original 1550 | Action-value score policy |
| TRADING-1552 | original 1551 | Ex-post trend label generator |
| TRADING-1553 | original 1552 | Label quality review |
| TRADING-1554 | original 1553 | PIT feature matrix builder |
| TRADING-1555 | original 1554 | Feature availability / PIT audit |
| TRADING-1556 | original 1555 | Baseline rule scorecard |
| TRADING-1557 | original 1556 | Low-complexity logistic / ordinal prototype |
| TRADING-1558 | original 1557 | Walk-forward trainer |
| TRADING-1559 | original 1558 | Trend model evaluation |
| TRADING-1560 | original 1559 | Plug calibrated first layer into frozen probes |
| TRADING-1561 | original 1560 | Actual-path rebacktest with calibrated first layer |
| TRADING-1562 | original 1561 | Overlay-specific review |
| TRADING-1563 | original 1562 | Risk-on / TQQQ diagnostic review |
| TRADING-1564 | original 1563 | Consensus vs single-probe label comparison |
| TRADING-1565 | original 1564 | Guardrail tests |
| TRADING-1566 | original 1565 | Report registry / artifact catalog / system flow |
| TRADING-1567 | original 1566 | Owner review pack |
| TRADING-1568 | original 1567 | Forward watch plan |
| TRADING-1569 | original 1568 | Validation |
| TRADING-1570～1586 | original 1569～1585 | Iteration, final matrix, closeout and commit |

## Expected Artifacts

- `docs/research/first_layer_policy_aware_calibration_scope.md`
- `config/research/first_layer_calibration_scope.yaml`
- `config/research/dynamic_second_layer_probe_registry.yaml`
- `config/research/action_value_score_policy.yaml`
- `config/research/first_layer_trend_scorecard_v1.yaml`
- `outputs/research_trends/action_value_matrix/action_value_matrix.csv`
- `outputs/research_trends/action_value_matrix/action_value_summary.json`
- `outputs/research_trends/trend_labels/single_probe_trend_labels.csv`
- `outputs/research_trends/trend_labels/consensus_trend_labels.csv`
- `inputs/research_reviews/consensus_trend_label_summary.yaml`
- `docs/research/consensus_trend_label_quality_review.md`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix.csv`
- `outputs/research_trends/pit_feature_matrix/feature_availability_report.json`
- `docs/research/first_layer_feature_pit_audit.md`
- `inputs/research_reviews/first_layer_feature_pit_audit.yaml`
- `outputs/research_trends/models/first_layer_trend_scorecard_v1_predictions.csv`
- `outputs/research_trends/models/first_layer_logistic_v1/`
- `outputs/research_trends/walk_forward/first_layer_walk_forward_predictions.csv`
- `outputs/research_trends/walk_forward/first_layer_walk_forward_metrics.json`
- `docs/research/first_layer_trend_model_walk_forward_review.md`
- `inputs/research_reviews/first_layer_trend_model_walk_forward_matrix.yaml`
- `outputs/research_trends/probe_backtest/old_first_layer_vs_new_first_layer_actual_path.csv`
- `outputs/research_trends/probe_backtest/probe_level_metrics.csv`
- `outputs/research_trends/calibrated_first_layer_actual_path_rebacktest/`
- `docs/research/calibrated_first_layer_defensive_overlay_review.md`
- `docs/research/calibrated_first_layer_risk_on_tqqq_diagnostic_review.md`
- `docs/research/single_probe_vs_consensus_trend_label_review.md`
- `docs/research/first_layer_policy_aware_calibration_owner_review_pack.md`
- `docs/research/calibrated_first_layer_forward_watch_plan.md`
- `inputs/research_reviews/first_layer_policy_aware_calibration_final_matrix.yaml`
- `docs/research/first_layer_policy_aware_calibration_closeout.md`

## Acceptance Criteria

- At least three frozen dynamic second-layer probes are defined and validated as trend-sensitive.
- Static baselines cannot be used as trend calibration probes.
- Action-value matrix and consensus trend labels are generated from future outcomes only for labels.
- PIT feature matrix includes `known_at`, `available_at`, `decision_at`, and blocks non-PIT features from training.
- First-layer model outputs only `trend_state`, `confidence`, `validity_days`, and `decay_profile`, never portfolio weights.
- Walk-forward validation is used; no full-sample train/test reuse.
- Calibrated first-layer predictions are plugged back into the same frozen probes for actual-path diagnostics.
- Any improvement must be described as overlay/action-value/risk-off quality, not target-path promotion evidence.
- TQQQ risk-on probe remains research-only diagnostic.
- Dynamic promotion, paper-shadow, production and broker stay blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_policy_aware_calibration.py`
- `python -m pytest -n 16 --dist loadfile tests/test_defensive_overlay_gate.py`
- `python -m pytest -n 16 --dist loadfile tests/test_expanded_allocation_universe.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-27: Registered task and requirement document. Implementation must not restore dynamic promotion and must keep second-layer probes frozen before label generation.
- 2026-06-27: Implemented `aits research trends full-pack`, policy configs, frozen dynamic probe validation, action-value matrix, consensus labels, PIT feature audit, low-complexity scorecard/logistic walk-forward, calibrated first-layer actual-path probe diagnostics, owner pack, forward watch plan, final matrix, closeout, report registry, artifact catalog, system flow, and guardrail tests. Real full-pack result: status=`FIRST_LAYER_POLICY_AWARE_CALIBRATION_READY_PROMOTION_BLOCKED`, action_value_matrix_size=66640, walk_forward_balanced_accuracy=0.151597, final_status=`FIRST_LAYER_CALIBRATION_NO_MATERIAL_IMPROVEMENT_PROMOTION_BLOCKED`; promotion/paper-shadow/production/broker remain blocked/false/none.
