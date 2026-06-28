# TRADING-1736 to 1765 First-Layer V2 Label Feature Model Reset

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1736_to_1765_FIRST_LAYER_V2_LABEL_FEATURE_MODEL_RESET`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Original owner attachment id: `TRADING-1696~1725`
- Renumbering reason: `TRADING-1716_to_1735_SECOND_LAYER_PROBE_LIBRARY_FREEZE` already uses part of the requested 1696~1725 range.
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`, requested/actual portfolio start `2021-02-22`
- Frozen second-layer: `dynamic_second_layer_probe_registry_v2`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1716_to_1735_SECOND_LAYER_PROBE_LIBRARY_FREEZE` 已冻结 second-layer probe registry v2，并确认 7 个 probes 可作为 first-layer action-value label generators，1 个 risk-on probe 仅 diagnostic-only。本批进入 first-layer 研究轮，只允许调整 label / feature / model / threshold，不得修改 second-layer probe weights。

当前 caveat 是 prior `first_layer_composer_v2_predictions.csv` 的 effective prediction coverage 可能晚于 primary window start。本批必须显式审计 label、feature、prediction、portfolio start，不能让名义 primary window `2021-02-22` 掩盖实际 prediction 从 2023 之后才开始。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1736 | First-layer v2 frozen probe scope and contract |
| TRADING-1737 | Effective coverage audit |
| TRADING-1738 | Action-value matrix v2 under frozen probe registry v2 |
| TRADING-1739 | Label taxonomy v2 generation and quality review |
| TRADING-1740 | PIT feature matrix v3 |
| TRADING-1741 | Feature PIT audit v3 |
| TRADING-1742 | Low-complexity first-layer submodels v2 |
| TRADING-1743 | Threshold calibration v2 |
| TRADING-1744 | First-layer composer v2 predictions |
| TRADING-1745 | Window-aware walk-forward v3 |
| TRADING-1746 | Frozen-probe actual-path backtest v3 |
| TRADING-1747 | Failure attribution |
| TRADING-1748 | Guardrail tests |
| TRADING-1749 | Owner review pack |
| TRADING-1750 | Forward watch plan draft |
| TRADING-1751 | Registry, catalog, system flow and task register |
| TRADING-1752 | Validation |
| TRADING-1753~1765 | Closeout, commit and push |

## Expected Artifacts

- `docs/research/first_layer_v2_frozen_probe_scope.md`
- `inputs/research_reviews/first_layer_v2_frozen_probe_contract.yaml`
- `docs/research/first_layer_v2_effective_coverage_audit.md`
- `inputs/research_reviews/first_layer_v2_effective_coverage_audit.yaml`
- `outputs/research_trends/action_value_matrix_v2/action_value_matrix_v2.csv`
- `outputs/research_trends/action_value_matrix_v2/action_value_summary_v2.json`
- `outputs/research_trends/trend_labels/upper_state_labels_v2.csv`
- `inputs/research_reviews/upper_state_label_v2_summary.yaml`
- `docs/research/upper_state_label_quality_review_v2.md`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3.csv`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3_report.json`
- `docs/research/up_state_feature_inventory_review_v3.md`
- `docs/research/first_layer_feature_pit_audit_v3.md`
- `inputs/research_reviews/first_layer_feature_pit_audit_v3.yaml`
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
- `docs/research/first_layer_v2_owner_review_pack.md`
- `docs/research/first_layer_v2_forward_watch_plan.md` or `docs/research/risk_off_only_forward_watch_plan.md`
- `docs/research/first_layer_v2_label_feature_model_closeout.md`
- `inputs/research_reviews/first_layer_v2_label_feature_model_final_matrix.yaml`
- updates to `config/report_registry.yaml`, `docs/artifact_catalog.md`, `docs/system_flow.md`, and `docs/task_register.md`

## Acceptance Criteria

- The run uses `dynamic_second_layer_probe_registry_v2` and proves the registry is frozen for this first-layer round.
- Primary-window metadata and `research_audit_metadata.modified_layer=first_layer` are present on tracked review artifacts.
- Effective coverage audit reports requested start, label start, feature start, prediction start, portfolio start, 2021/2022 coverage, and `PRIMARY_WINDOW_COVERAGE_INCOMPLETE` if prediction start is later than 2022-01-01.
- Action-value matrix v2, labels v2, feature matrix v3, feature PIT audit, submodel diagnostics, threshold review, composer predictions, walk-forward review and frozen-probe actual-path review are regenerated under frozen probe registry v2.
- `do_not_de_risk`, `stay_constructive`, `add_risk` and `high_confidence_risk_on` labels are generated; high-confidence risk-on remains research-only diagnostic.
- First-layer model outputs do not contain portfolio weights and target-path metrics cannot pass the first-layer v2 gate.
- If actual-path does not materially improve, failure attribution must use explicit reasons such as `WINDOW_COVERAGE_INCOMPLETE`, `FEATURES_INSUFFICIENT`, `MODEL_UNDERFIT`, or `NO_ACTION_VALUE_EDGE`.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_v2_label_feature_reset.py`
- `python -m pytest -n 16 --dist loadfile tests/test_dynamic_second_layer_probe_registry_v2.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_up_state_learning.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_policy_aware_calibration.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document. Implementation will reuse existing first-layer reset builders where correct, but the default entry point for this batch must freeze `dynamic_second_layer_probe_registry_v2` and add coverage/contract closeout artifacts.
- 2026-06-28: Implemented `aits research trends first-layer-v2-reset`, defaulting to frozen `dynamic_second_layer_probe_registry_v2`, and generated frozen-probe contract, effective coverage audit, action-value matrix v2, labels v2, PIT feature matrix v3, feature PIT audit, submodel diagnostics, threshold/composer outputs, walk-forward v3, frozen-probe actual-path v3, failure attribution, owner pack and final closeout. Real run final status is `WINDOW_COVERAGE_INCOMPLETE`: label/feature start=`2021-02-22`, prediction/portfolio effective start=`2023-02-22`, label rows=1,282, composer predictions=2,205, PIT feature rows=1,343, approved PIT features=26, frozen-probe actual-path improved_vs_flat_reference_count=8/8, primary failure reason=`WINDOW_COVERAGE_INCOMPLETE`, next action=`REBUILD_WALK_FORWARD_COVERAGE_BEFORE_OWNER_ESCALATION`.
- 2026-06-28: Validation passed with Ruff, compileall, focused first-layer v2 guardrail tests, dynamic second-layer registry tests, research audit metadata tests, research window contract tests, prior first-layer regression tests, execution semantics tests, artifact governance tests, task/register/report/documentation tests, and `git diff --check`; final staged diff check is performed during closeout.
