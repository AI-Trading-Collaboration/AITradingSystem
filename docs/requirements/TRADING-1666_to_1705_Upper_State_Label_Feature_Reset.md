# TRADING-1666 to 1705 Upper-State Label / Feature Reset

## 状态

- Task id: `TRADING-1666_to_1705_UPPER_STATE_LABEL_FEATURE_RESET`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-27
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`, requested start `2021-02-22`
- Legacy comparison window: `legacy_research_window_2022_12`, requested start `2022-12-01`
- Sensitivity window: `exact_three_asset_primary_only_extension`, requested start `2020-05-28`, SGOV secondary-source caveat
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1616_to_1645_FIRST_LAYER_UP_STATE_LEARNING_REPAIR` 已经把 first-layer flat collapse 拆成 risk-off detector、upper-state detector 和 risk-on severity scaler。真实运行显示上行状态不再完全预测为 0，但 `upper_state_detector_v1` 的 precision / recall 过低，接回 frozen second-layer probes 后 `improved_vs_flat_probe_count=0`。因此下一步不应继续围绕旧 five-class / upper-state label 做 threshold 或 class-weight 微调。

本批改为重置研究问题本身：使用 `TRADING-1646_to_1665_RESEARCH_WINDOW_EXTENSION_VALIDATION` 建立的 primary validated window，冻结 second-layer probes，重新生成 action-value labels，并只训练可解释、低复杂度、research-only 的 first-layer 上行子模型。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1666 | 固化 up-state repair failure review |
| TRADING-1667 | Alternating two-layer research protocol |
| TRADING-1668 | Upper-state label taxonomy v2 |
| TRADING-1669 | Window-aware label regeneration plan |
| TRADING-1670 | Action-value score policy v2 |
| TRADING-1671 | Action-value matrix v2 |
| TRADING-1672 | Upper-state labels v2 |
| TRADING-1673 | Label quality review v2 |
| TRADING-1674 | Up-state feature inventory v2 |
| TRADING-1675 | PIT feature matrix v3 |
| TRADING-1676 | First-layer feature PIT audit v3 |
| TRADING-1677 | Do-not-de-risk model v1 |
| TRADING-1678 | Stay-constructive model v1 |
| TRADING-1679 | Add-risk model v1 |
| TRADING-1680 | High-confidence risk-on diagnostic model v1 |
| TRADING-1681 | First-layer threshold policy v2 |
| TRADING-1682 | First-layer composer v2 |
| TRADING-1683 | First-layer walk-forward review v3 |
| TRADING-1684 | Frozen-probe actual-path review |
| TRADING-1685 | Failure attribution |
| TRADING-1686 | Guardrail tests |
| TRADING-1687 | Owner review pack |
| TRADING-1688 | Forward watch disposition |
| TRADING-1689 | Registry, catalog, system flow, task register |
| TRADING-1690 | Validation |
| TRADING-1691 to 1705 | Final matrix, closeout, commit |

## Expected Artifacts

- `docs/research/up_state_repair_result_review.md`
- `inputs/research_reviews/up_state_repair_result_review.yaml`
- `docs/research/alternating_two_layer_research_protocol.md`
- `config/research/alternating_two_layer_research_protocol.yaml`
- `config/research/upper_state_label_taxonomy_v2.yaml`
- `docs/research/upper_state_label_taxonomy_reset.md`
- `docs/research/window_aware_upper_state_label_regeneration_plan.md`
- `config/research/action_value_score_policy_v2.yaml`
- `outputs/research_trends/action_value_matrix_v2/action_value_matrix_v2.csv`
- `outputs/research_trends/action_value_matrix_v2/action_value_summary_v2.json`
- `outputs/research_trends/trend_labels/upper_state_labels_v2.csv`
- `inputs/research_reviews/upper_state_label_v2_summary.yaml`
- `docs/research/upper_state_label_quality_review_v2.md`
- `inputs/research_reviews/up_state_feature_inventory_v2.yaml`
- `docs/research/up_state_feature_inventory_review.md`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3.csv`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3_report.json`
- `docs/research/first_layer_feature_pit_audit_v3.md`
- `inputs/research_reviews/first_layer_feature_pit_audit_v3.yaml`
- `outputs/research_trends/models/do_not_de_risk_model_v1/`
- `outputs/research_trends/models/stay_constructive_model_v1/`
- `outputs/research_trends/models/add_risk_model_v1/`
- `outputs/research_trends/models/high_confidence_risk_on_model_v1/`
- `docs/research/do_not_de_risk_model_review.md`
- `docs/research/stay_constructive_model_review.md`
- `docs/research/add_risk_model_review.md`
- `docs/research/high_confidence_risk_on_model_review.md`
- `config/research/first_layer_threshold_policy_v2.yaml`
- `docs/research/first_layer_threshold_calibration_review_v2.md`
- `config/research/first_layer_composer_v2.yaml`
- `outputs/research_trends/models/first_layer_composer_v2_predictions.csv`
- `docs/research/first_layer_walk_forward_review_v3.md`
- `inputs/research_reviews/first_layer_walk_forward_matrix_v3.yaml`
- `docs/research/first_layer_v2_frozen_probe_actual_path_review.md`
- `inputs/research_reviews/first_layer_v2_frozen_probe_actual_path_matrix.yaml`
- `docs/research/first_layer_v2_failure_attribution.md`
- `inputs/research_reviews/first_layer_v2_failure_attribution.yaml`
- `docs/research/upper_state_label_feature_reset_owner_review_pack.md`
- `docs/research/risk_off_only_forward_watch_plan.md` or `docs/research/first_layer_v2_forward_watch_plan.md`
- `docs/research/upper_state_label_feature_reset_closeout.md`
- `inputs/research_reviews/upper_state_label_feature_reset_final_matrix.yaml`

## Acceptance Criteria

- `upper_state_detector_v1` failure is explicitly preserved as prior evidence, not overwritten.
- First-layer calibration uses frozen second-layer probes and cannot modify state-to-portfolio weights.
- Second-layer mapping research, if run later, must use frozen first-layer predictions and cannot tune labels, features or first-layer models.
- Validation rounds freeze both layers and cannot reuse failed holdout evidence for immediate tuning.
- Upper-state labels v2 include `do_not_de_risk`, `stay_constructive`, `add_risk` and `high_confidence_risk_on`.
- `add_risk` is not equivalent to `stay_constructive`; `high_confidence_risk_on` remains research-only diagnostic unless sample quality and actual-path evidence support escalation in a future task.
- First-layer models output trend-state signals, confidence, validity and decay metadata only; they must not output portfolio weights.
- Every new research artifact includes `research_window_id`, `requested_start`, `actual_portfolio_start`, `window_role` and `data_quality_contract`.
- Target-path metrics cannot pass any first-layer gate.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_upper_state_label_taxonomy_v2.py`
- `python -m pytest -n 16 --dist loadfile tests/test_alternating_two_layer_research_protocol.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_up_state_learning.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_policy_aware_calibration.py`
- `python -m pytest -n 16 --dist loadfile tests/test_return_seeking_second_layer_probes.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-27: Registered task and requirement document. The implementation must treat pre-2022 data as validation/sensitivity context under the AI regime rules, not as production eligibility evidence.
- 2026-06-27: Implemented `aits research trends upper-state-reset`, alternating two-layer protocol, upper-state label taxonomy v2, action-value score policy v2, threshold/composer policies, action-value matrix v2, labels v2, PIT feature matrix v3, four first-layer submodel diagnostics, composer v2 predictions, walk-forward v3, frozen-probe actual-path review, failure attribution, owner pack, forward watch disposition, final matrix, report registry, artifact catalog, system flow and guardrail tests.
- 2026-06-27: Real run generated action-value rows=57,312, label rows=14,328, primary horizon-20 label rows=1,282, PIT feature rows=1,343, composer predictions=2,205, frozen-probe improved-vs-flat-reference count=4/4. Final status is `UPPER_STATE_LABEL_RESET_IMPROVES_ACTUAL_PATH`, but all four submodels remain `MODEL_EDGE_WEAK`; failure attribution primary reason is `SUBMODEL_PRECISION_WEAK`. Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Real Run Summary

- Command: `python -m ai_trading_system.cli research trends upper-state-reset`
- Primary validated window: `exact_three_asset_validated`, requested/actual/portfolio start `2021-02-22`, data quality `PASS_WITH_WARNINGS`.
- Label summary: do_not_de_risk positives 320, stay_constructive positives 392, add_risk positives 86, high_confidence_risk_on positives 120; high-confidence label share 0.186427; average disagreement 0.064060.
- Model summary: do_not_de_risk precision 0.282838 / recall 0.523979; stay_constructive precision 0.229827 / recall 0.470501; add_risk precision 0.083744 / recall 0.425000; high_confidence_risk_on precision 0.016897 / recall 0.065089. All are `MODEL_EDGE_WEAK`.
- Frozen-probe actual-path summary: 4/4 probes improved versus prior flat reference rows, but those prior rows are explicitly `PRIOR_ROWS_LEGACY_REFERENCE_ONLY`; this is owner-review evidence, not a promotion leaderboard.
- Closeout: `UPPER_STATE_LABEL_RESET_IMPROVES_ACTUAL_PATH`; first-layer v2 remains research-only and next action is `OWNER_REVIEW_FIRST_LAYER_V2_FORWARD_WATCH`.
