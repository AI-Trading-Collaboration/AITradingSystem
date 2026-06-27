# TRADING-1786 to 1805 First-Layer V2 Defensive Probe Regression Diagnosis

## 状态

- Task id: `TRADING-1786_to_1805_FIRST_LAYER_V2_DEFENSIVE_PROBE_REGRESSION_DIAGNOSIS`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`
- Frozen second-layer: `dynamic_second_layer_probe_registry_v2`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

TRADING-1766 to 1785 修复了 first-layer v2 walk-forward coverage：`wf_252d_initial` 和 `wf_expanding_initial` 均从 `2022-02-18` 开始预测并覆盖 2022。覆盖问题修复后，coverage-aware selection 仍为 0，因为 coverage-pass variants 接回 frozen second-layer probes 后触发 `DEFENSIVE_PROBE_REGRESSION`。本批只诊断 regression 来源和可降级使用边界，不修改 second-layer probe registry v2，不新增 probes，不调 threshold/class weight，不恢复 dynamic promotion。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1786 | Coverage rebuild closeout reclassification |
| TRADING-1787 | Defensive probe regression inventory |
| TRADING-1788 | Probe-role group comparison |
| TRADING-1789 | 2022 defensive regression slice review |
| TRADING-1790 | Signal error attribution |
| TRADING-1791 | Policy variant stability review |
| TRADING-1792 | Return-seeking-only reclassification check |
| TRADING-1793 | Risk-off-only fallback assessment |
| TRADING-1794 | Decision matrix |
| TRADING-1795 | Owner brief |
| TRADING-1796 | Guardrail tests |
| TRADING-1797 | Registry, catalog, system flow and task register |
| TRADING-1798 | Validation |
| TRADING-1799 to 1805 | Closeout, commit and push |

## Expected Artifacts

- `docs/research/first_layer_v2_coverage_rebuild_reclassification.md`
- `inputs/research_reviews/first_layer_v2_coverage_rebuild_reclassification.yaml`
- `docs/research/first_layer_v2_defensive_probe_regression_inventory.md`
- `inputs/research_reviews/first_layer_v2_defensive_probe_regression_inventory.yaml`
- `docs/research/first_layer_v2_probe_role_group_comparison.md`
- `inputs/research_reviews/first_layer_v2_probe_role_group_matrix.yaml`
- `docs/research/first_layer_v2_2022_defensive_regression_slice_review.md`
- `inputs/research_reviews/first_layer_v2_2022_defensive_regression_slice.yaml`
- `docs/research/first_layer_v2_signal_error_attribution.md`
- `inputs/research_reviews/first_layer_v2_signal_error_attribution.yaml`
- `docs/research/first_layer_v2_policy_variant_stability_review.md`
- `inputs/research_reviews/first_layer_v2_policy_variant_stability_matrix.yaml`
- `docs/research/first_layer_v2_return_seeking_diagnostic_reclassification.md`
- `inputs/research_reviews/first_layer_v2_return_seeking_diagnostic_reclassification.yaml`
- `docs/research/first_layer_v2_risk_off_only_fallback_assessment.md`
- `inputs/research_reviews/first_layer_v2_risk_off_only_fallback_assessment.yaml`
- `docs/research/first_layer_v2_defensive_regression_diagnosis_review.md`
- `inputs/research_reviews/first_layer_v2_defensive_regression_diagnosis_matrix.yaml`
- `docs/research/first_layer_v2_defensive_regression_owner_brief.md`
- `docs/research/first_layer_v2_defensive_regression_diagnosis_closeout.md`
- `inputs/research_reviews/first_layer_v2_defensive_regression_diagnosis_final_matrix.yaml`
- updates to `config/report_registry.yaml`, `docs/artifact_catalog.md`, `docs/system_flow.md`, and `docs/task_register.md`
- `tests/test_first_layer_defensive_regression_diagnosis.py`

## Acceptance Criteria

- Coverage rebuild positive evidence is reclassified as late-window-only partial evidence where it does not satisfy coverage-aware selection.
- Probe-level inventory identifies which coverage-pass probes regress and preserves annual return, drawdown, Sharpe, Calmar and turnover diagnostics where references are available.
- Role-group comparison states whether regression is concentrated in defensive / drawdown-control probes versus return-seeking probes.
- 2022 drawdown, recovery and post-ChatGPT transition slices disclose state distribution, exposure, drawdown, missed defensive benefit, false re-risk/add-risk cost and avoided drawdown.
- Signal attribution identifies risk-off missed, false do-not-de-risk, false add-risk, high-confidence risk-on false positive, early re-risk, train-window instability and feature coverage gap evidence.
- Policy stability review compares 504d, 378d, 252d, expanding and warm-start policies, including sample counts, label counts and prediction distributions.
- Return-seeking diagnostic and risk-off-only fallback assessments keep owner review, promotion, paper-shadow, production and broker disabled.
- Final matrix records final diagnosis, owner review status, promotion status, analyzed policy variants and remaining blockers.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_defensive_regression_diagnosis.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_walk_forward_coverage.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_v2_label_feature_reset.py`
- `python -m pytest -n 16 --dist loadfile tests/test_dynamic_second_layer_probe_registry_v2.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document. Implementation must derive the diagnosis from the prior coverage rebuild artifacts and frozen `dynamic_second_layer_probe_registry_v2`; any return-seeking-only or risk-off-only conclusion remains diagnostic-only and cannot enable owner review, promotion, paper-shadow, production or broker use.
- 2026-06-28: Implemented `aits research trends first-layer-defensive-regression-diagnosis`, generated reclassification, regression inventory, role-group comparison, 2022 defensive regression slices, signal attribution, policy stability review, return-seeking diagnostic reclassification, risk-off-only fallback assessment, diagnosis matrix, owner brief and closeout/final matrix. Real run result: final status=`FIRST_LAYER_V2_RETURN_SEEKING_DIAGNOSTIC_ONLY`, final diagnosis=`RETURN_SEEKING_ONLY_DIAGNOSTIC`; coverage-pass policies are `wf_252d_initial` and `wf_expanding_initial`; regressed probes are `defensive_overlay_probe`, `drawdown_control_probe`, and `balanced_dynamic_probe`; pure return-seeking and risk-on diagnostic probes improve but remain diagnostic-only; risk-off-only fallback is not supported; owner review remains false and promotion/paper-shadow/production/broker remain blocked/false/false/none.
- 2026-06-28: Validation passed: `python -m ruff check src tests`; `python -m compileall -q src tests`; focused parallel pytest for first-layer defensive regression diagnosis, coverage rebuild, first-layer v2 label reset, dynamic second-layer registry v2, research audit metadata, research window contracts, execution semantics, research artifact governance, task register consistency, report index and documentation contract; `git diff --check`; `git diff --cached --check`.
