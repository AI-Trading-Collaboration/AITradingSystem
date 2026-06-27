# TRADING-1616 to 1645 First-Layer Up-State Learning Repair

## 状态

- Task id: `TRADING-1616_to_1645_FIRST_LAYER_UP_STATE_LEARNING_REPAIR`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-27
- Market regime: `ai_after_chatgpt`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1547_to_1586_POLICY_AWARE_FIRST_LAYER_TREND_CALIBRATION` 已跑通 full-pack，但 five-class first-layer walk-forward 输出塌缩到 `risk_off` / `defensive` / `neutral`：

- predicted `constructive` = 0；
- predicted `risk_on` = 0；
- validation labels 同期仍包含 `constructive=96`、`risk_on=18`；
- 接回 frozen probes 后 annual return / Sharpe / Calmar 均下降，表现为过度防守。

本批不恢复 dynamic promotion，不改 production，也不重新自由优化 second-layer probe 权重。目标是把 first-layer 修复为分层模型：risk-off detector、upper-state detector、risk-on severity scaler。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1616 | Failure diagnosis snapshot |
| TRADING-1617 | Probe role metadata repair |
| TRADING-1618 | Upper-state label audit |
| TRADING-1619 | Hierarchical label builder |
| TRADING-1620 | Up-state PIT feature expansion |
| TRADING-1621 | Feature PIT audit v2 |
| TRADING-1622 | Risk-off detector v2 |
| TRADING-1623 | Upper-state detector v1 |
| TRADING-1624 | Risk-on severity scaler |
| TRADING-1625 | Threshold calibration |
| TRADING-1626 | Hierarchical first-layer composer |
| TRADING-1627 | Walk-forward evaluation v2 |
| TRADING-1628 | Plug hierarchical first layer into frozen probes |
| TRADING-1629 | Actual-path rebacktest v2 |
| TRADING-1630 | Over-defensive collapse guardrails |
| TRADING-1631 | Return-seeking probe compatibility |
| TRADING-1632 | Class-imbalance and split diagnostics |
| TRADING-1633 | Owner review pack |
| TRADING-1634 | Forward watch plan update |
| TRADING-1635 | Registry / catalog / system flow |
| TRADING-1636 | Validation |
| TRADING-1637 to 1645 | Final matrix, closeout, commit |

## Expected Artifacts

- `docs/research/first_layer_up_state_failure_diagnosis.md`
- `inputs/research_reviews/first_layer_up_state_failure_diagnosis.yaml`
- `docs/research/upper_state_label_audit.md`
- `inputs/research_reviews/upper_state_label_audit.yaml`
- `outputs/research_trends/trend_labels/hierarchical_trend_labels.csv`
- `inputs/research_reviews/hierarchical_trend_label_summary.yaml`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v2.csv`
- `docs/research/up_state_feature_expansion_review.md`
- `docs/research/first_layer_feature_pit_audit_v2.md`
- `inputs/research_reviews/first_layer_feature_pit_audit_v2.yaml`
- `outputs/research_trends/models/risk_off_detector_v2/`
- `docs/research/risk_off_detector_v2_review.md`
- `outputs/research_trends/models/upper_state_detector_v1/`
- `docs/research/upper_state_detector_v1_review.md`
- `outputs/research_trends/models/risk_on_severity_scaler_v1/`
- `docs/research/risk_on_severity_scaler_review.md`
- `config/research/first_layer_threshold_policy_v1.yaml`
- `docs/research/first_layer_threshold_calibration_review.md`
- `config/research/hierarchical_first_layer_v1.yaml`
- `outputs/research_trends/models/hierarchical_first_layer_v1_predictions.csv`
- `docs/research/hierarchical_first_layer_walk_forward_review.md`
- `inputs/research_reviews/hierarchical_first_layer_walk_forward_matrix.yaml`
- `outputs/research_trends/hierarchical_first_layer_probe_backtest/`
- `docs/research/hierarchical_first_layer_actual_path_review.md`
- `inputs/research_reviews/hierarchical_first_layer_actual_path_matrix.yaml`
- `docs/research/return_seeking_probe_compatibility_with_up_state_model.md`
- `docs/research/first_layer_class_imbalance_split_diagnostics.md`
- `docs/research/first_layer_up_state_learning_owner_review_pack.md`
- `docs/research/hierarchical_first_layer_forward_watch_plan.md`
- `inputs/research_reviews/first_layer_up_state_learning_final_matrix.yaml`
- `docs/research/first_layer_up_state_learning_closeout.md`

## Acceptance Criteria

- First-layer five-class collapse is formally recorded with label/prediction/action-path evidence.
- Probe registry explicitly marks return-seeking behavior while keeping all probes research-only and promotion/broker disabled.
- Hierarchical labels include risk-off binary, upper-state binary, three-zone and severity fields.
- Upper-state split audit reports train/validation coverage and collapse risk.
- PIT feature matrix v2 records feature family, known_at, available_at, decision_at, missing_rate and split coverage.
- Risk-off and upper-state detectors are trained/evaluated separately with walk-forward splits.
- Validation cannot silently pass when validation has upper-state labels but predictions contain zero upper-state states.
- Hierarchical first-layer predictions are plugged back into the same frozen probes for actual-path diagnostics.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_up_state_learning.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_policy_aware_calibration.py`
- `python -m pytest -n 16 --dist loadfile tests/test_return_seeking_second_layer_probes.py`
- `python -m pytest -n 16 --dist loadfile tests/test_defensive_overlay_gate.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-27: Registered task and requirement document. Implementation must preserve prior full-pack outputs and add a separate up-state repair pack with blocked promotion safety fields.
- 2026-06-27: Implemented `aits research trends up-state-repair`, return-seeking probe metadata, failure diagnosis, upper-state audit, hierarchical labels, PIT feature matrix v2, risk-off / upper-state / severity models, threshold review, hierarchical walk-forward, frozen-probe actual-path v2, owner pack, forward watch plan, final matrix, closeout, and guardrail tests. Real result: predicted upper_state=185 (`constructive=159`, `risk_on=26`), `upper_state_precision=0.064865`, `upper_state_recall=0.105263`, split-level upper-state collapse guardrail triggered, actual-path improved_vs_flat_probe_count=0, final_status=`UP_STATE_FEATURES_INSUFFICIENT_PROMOTION_BLOCKED`; promotion/paper-shadow/production/broker remain blocked/false/none.
