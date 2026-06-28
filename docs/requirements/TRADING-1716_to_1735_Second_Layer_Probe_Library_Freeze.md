# TRADING-1716 to 1735 Second-Layer Probe Library Freeze

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1716_to_1735_SECOND_LAYER_PROBE_LIBRARY_FREEZE`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Original owner attachment id: `TRADING-1676~1695`
- Renumbering reason: `TRADING-1666_to_1705_UPPER_STATE_LABEL_FEATURE_RESET` already uses the 1676~1695 range.
- Market regime: `ai_after_chatgpt`
- Research window: `exact_three_asset_validated`, requested/actual portfolio start `2021-02-22`
- Safety boundary: research-only, actual-path required, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

`TRADING-1706_to_1715_RESEARCH_WINDOW_ADOPTION_CLOSEOUT` 已把 `2021-02-22` primary validated window 设为后续默认研究窗口。后续第一层 label / feature / model 重做之前，必须先冻结第二层 dynamic probe library，否则第一层 action-value labels 的目标函数会随 probe 定义变化而漂移。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1716 | Second-layer probe registry v2 |
| TRADING-1717 | Probe constraint validation |
| TRADING-1718 | QQQ-equivalent exposure review |
| TRADING-1719 | Primary-window actual-path rebacktest |
| TRADING-1720 | Same-risk static frontier comparison |
| TRADING-1721 | TQQQ contribution / stress review |
| TRADING-1722 | Probe action-value readiness matrix |
| TRADING-1723 | First-layer calibration dependency update |
| TRADING-1724 | Research audit metadata integration |
| TRADING-1725 | Owner review pack |
| TRADING-1726 | Report registry / artifact catalog / system flow |
| TRADING-1727 | Validation |
| TRADING-1728~1735 | Closeout, commit and push |

## Expected Artifacts

- `config/research/dynamic_second_layer_probe_registry_v2.yaml`
- `docs/research/dynamic_second_layer_probe_registry_v2_review.md`
- `outputs/research_probes/second_layer_v2/qqq_equivalent_exposure_by_state.csv`
- `outputs/research_probes/second_layer_v2/actual_path_rebacktest/probe_actual_path_metrics.csv`
- `docs/research/second_layer_probe_exposure_review_v2.md`
- `inputs/research_reviews/second_layer_probe_exposure_matrix_v2.yaml`
- `docs/research/second_layer_probe_actual_path_review_v2.md`
- `inputs/research_reviews/second_layer_probe_actual_path_matrix_v2.yaml`
- `docs/research/second_layer_probe_same_risk_frontier_review_v2.md`
- `inputs/research_reviews/second_layer_probe_same_risk_frontier_matrix_v2.yaml`
- `docs/research/second_layer_probe_tqqq_stress_review_v2.md`
- `inputs/research_reviews/second_layer_probe_tqqq_stress_matrix_v2.yaml`
- `docs/research/second_layer_action_value_probe_readiness_review_v2.md`
- `inputs/research_reviews/second_layer_action_value_probe_readiness_v2.yaml`
- `docs/research/first_layer_calibration_probe_dependency_update.md`
- `docs/research/second_layer_probe_library_freeze_owner_pack.md`
- `docs/research/second_layer_probe_library_freeze_closeout.md`
- `inputs/research_reviews/second_layer_probe_library_freeze_final_matrix.yaml`
- updates to `config/report_registry.yaml`, `docs/artifact_catalog.md`, `docs/system_flow.md`, and `docs/task_register.md`

## Acceptance Criteria

- Probe registry v2 defines at least eight probes: defensive overlay, balanced dynamic, drawdown control, no-TQQQ return seeking, low-TQQQ balanced growth, QQQ-heavy growth, capped risk-on diagnostic, and asymmetric risk-on slow-confirm.
- Every probe is trend-sensitive, long-only, weight sums to one by state, and disables promotion/broker.
- TQQQ constraints are explicit; no-TQQQ probes have zero TQQQ and diagnostic probes are research-only.
- Primary-window actual-path metrics, same-risk frontier comparison, TQQQ stress review, and action-value readiness are generated.
- Post-1665 artifacts include primary window metadata and `research_audit_metadata` with `modified_layer=second_layer`.
- Downstream first-layer calibration must freeze `dynamic_second_layer_probe_registry_v2`.
- Dynamic promotion, paper-shadow, production and broker remain blocked/false/none.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_dynamic_second_layer_probe_registry_v2.py`
- `python -m pytest -n 16 --dist loadfile tests/test_return_seeking_second_layer_probes.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_expanded_allocation_universe.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document. This batch freezes the second-layer probe library before further first-layer calibration; it does not enable promotion, paper-shadow, production, or broker use.
- 2026-06-28: Implemented `dynamic_second_layer_probe_registry_v2`, `aits research trends second-layer-probe-freeze`, generated exposure / actual-path / same-risk / TQQQ stress / readiness / owner-pack / closeout artifacts, and registered the new reports. Real run status is `SECOND_LAYER_RETURN_SEEKING_PROBES_DIAGNOSTIC_ONLY`: probe_count=8, approved_action_value_probe_count=7, diagnostic_only_probe_count=1, rejected_probe_count=0, same-risk summary has 7 probes beating the same-risk static frontier and 1 diagnostic-only probe, TQQQ stress blocked count=0, data_quality_status=`PASS_WITH_WARNINGS`; dynamic promotion, paper-shadow, production, and broker remain blocked/false/none.
- 2026-06-28: Validation passed with Ruff, compileall, the focused parallel pytest suites listed above, research artifact governance checks, task/register/report/documentation checks, and `git diff --check`; final staged diff check is performed during closeout.
