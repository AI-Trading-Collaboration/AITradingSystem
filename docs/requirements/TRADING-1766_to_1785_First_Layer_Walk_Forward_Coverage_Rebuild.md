# TRADING-1766 to 1785 First-Layer Walk-Forward Coverage Rebuild

## 状态

- Task id: `TRADING-1766_to_1785_FIRST_LAYER_WALK_FORWARD_COVERAGE_REBUILD`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`, requested/actual portfolio start `2021-02-22`
- Frozen second-layer: `dynamic_second_layer_probe_registry_v2`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1736_to_1765_FIRST_LAYER_V2_LABEL_FEATURE_MODEL_RESET` 真实运行得到局部正面但不可采纳的结果：8/8 frozen probes 在 effective prediction window 内优于 flat reference，但 first-layer prediction / portfolio effective start 是 `2023-02-22`，没有覆盖 2022 主回撤和修复阶段。本批只修 walk-forward coverage 和有效预测区间，不修改 second-layer probe weights，不恢复 promotion。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1766 | Coverage blocker diagnosis |
| TRADING-1767 | Walk-forward coverage policy v2 |
| TRADING-1768 | Coverage simulation matrix |
| TRADING-1769 | Early feature coverage audit |
| TRADING-1770 | Feature optionalization policy |
| TRADING-1771 | Model training variants |
| TRADING-1772 | Actual-path rebacktest by coverage policy |
| TRADING-1773 | 2022 stress / recovery slice review |
| TRADING-1774 | Coverage-aware selection rule |
| TRADING-1775 | Coverage rebuild failure attribution |
| TRADING-1776 | Guardrail tests |
| TRADING-1777 | Owner review pack |
| TRADING-1778 | Registry, catalog, system flow and task register |
| TRADING-1779 | Validation |
| TRADING-1780~1785 | Closeout, commit and push |

## Expected Artifacts

- `config/research/first_layer_walk_forward_coverage_policy_v2.yaml`
- `config/research/first_layer_feature_optionalization_policy.yaml`
- `config/research/first_layer_v2_coverage_aware_selection_rule.yaml`
- `docs/research/first_layer_v2_coverage_blocker_diagnosis.md`
- `inputs/research_reviews/first_layer_v2_coverage_blocker_diagnosis.yaml`
- `docs/research/first_layer_walk_forward_coverage_simulation.md`
- `inputs/research_reviews/first_layer_walk_forward_coverage_simulation_matrix.yaml`
- `docs/research/first_layer_v2_early_feature_coverage_audit.md`
- `inputs/research_reviews/first_layer_v2_early_feature_coverage_audit.yaml`
- `outputs/research_trends/models/first_layer_v2_coverage_rebuild/`
- `docs/research/first_layer_v2_coverage_rebuild_model_review.md`
- `inputs/research_reviews/first_layer_v2_coverage_rebuild_model_matrix.yaml`
- `docs/research/first_layer_v2_coverage_policy_actual_path_review.md`
- `inputs/research_reviews/first_layer_v2_coverage_policy_actual_path_matrix.yaml`
- `docs/research/first_layer_v2_2022_stress_recovery_slice_review.md`
- `inputs/research_reviews/first_layer_v2_2022_slice_matrix.yaml`
- `docs/research/first_layer_v2_coverage_rebuild_failure_attribution.md`
- `inputs/research_reviews/first_layer_v2_coverage_rebuild_failure_attribution.yaml`
- `docs/research/first_layer_v2_coverage_rebuild_owner_pack.md`
- `docs/research/first_layer_v2_walk_forward_coverage_rebuild_closeout.md`
- `inputs/research_reviews/first_layer_v2_walk_forward_coverage_rebuild_final_matrix.yaml`
- updates to `config/report_registry.yaml`, `docs/artifact_catalog.md`, `docs/system_flow.md`, and `docs/task_register.md`

## Acceptance Criteria

- The prior `2023-02-22` prediction start is decomposed into train-window, min-train, feature lookback, label horizon, action-value availability, and split-policy causes.
- At least five coverage variants are registered and at least three are simulated, including 504d baseline, 378d initial, 252d initial, expanding initial, and warm-start diagnostic.
- At least one non-diagnostic variant attempts 2022 coverage and reports whether the coverage pass rule is satisfied.
- Early PIT feature coverage audit reports which features are usable in 2021/2022 and whether optionalization is needed.
- Model variants and frozen-probe actual-path results are regenerated without changing `dynamic_second_layer_probe_registry_v2`.
- A 2022 stress/recovery slice review reports risk-off, do-not-de-risk, add-risk, false-risk-off, false-risk-on, missed-upside, avoided-drawdown, re-risk timing, and probe-level results.
- Coverage-aware selection rule blocks owner escalation unless coverage pass, actual-path improvement, 2022 slice, same-risk reporting and safety gates are satisfied.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
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

- 2026-06-28: Registered task and requirement document. This batch starts from the frozen second-layer v2 registry and the prior `WINDOW_COVERAGE_INCOMPLETE` final matrix; it may produce positive coverage-aware evidence, but cannot unblock dynamic promotion, paper-shadow, production or broker use.
- 2026-06-28: Implemented `aits research trends first-layer-coverage-rebuild`, coverage policy variants, feature optionalization policy, coverage-aware selection rule, generated review matrices/docs, report registry/catalog/system-flow updates and guardrail tests. Real run results: baseline first prediction remains `2023-02-22`; `wf_252d_initial` and `wf_expanding_initial` first predict on `2022-02-18` and satisfy coverage, but coverage-aware actual-path selection remains empty because both coverage-pass variants trigger `DEFENSIVE_PROBE_REGRESSION`. Final status is `COVERAGE_REBUILD_SUCCESS_ACTION_PATH_NO_LONGER_IMPROVES`; next action is `KEEP_FIRST_LAYER_V2_COVERAGE_REBUILD_BLOCKED`.
- 2026-06-28: Focused validation passed for `python -m ruff check src/ai_trading_system/first_layer_walk_forward_coverage.py tests/test_first_layer_walk_forward_coverage.py tests/test_research_audit_metadata.py`, focused compileall, and `python -m pytest -n 16 --dist loadfile tests/test_first_layer_walk_forward_coverage.py`. Full validation plan remains pending.
