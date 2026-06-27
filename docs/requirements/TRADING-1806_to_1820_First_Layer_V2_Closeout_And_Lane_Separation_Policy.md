# TRADING-1806 to 1820 First-Layer V2 Closeout and Lane Separation Policy

## 状态

- Task id: `TRADING-1806_to_1820_FIRST_LAYER_V2_CLOSEOUT_AND_LANE_SEPARATION_POLICY`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`
- Frozen upstream evidence: `first_layer_v2_defensive_regression_diagnosis_final_matrix.v1`
- Safety boundary: research-only, actual-path required for any future candidate, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

TRADING-1786 to 1805 已确认 current first-layer v2 的 final status 为 `FIRST_LAYER_V2_RETURN_SEEKING_DIAGNOSTIC_ONLY`，final diagnosis 为 `RETURN_SEEKING_ONLY_DIAGNOSTIC`，主要归因为 `DEFENSIVE_REGRESSION_DUE_TO_FALSE_ADD_RISK`。Coverage-pass variants 能覆盖 2022，但会破坏 `defensive_overlay_probe`、`drawdown_control_probe`，并在部分 policy 中连带 `balanced_dynamic_probe`。因此本批不再尝试把 first-layer v2 调成 universal trend layer，而是正式关闭 universal route，并建立 two-lane signal usage policy。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1806 | First-layer v2 universal layer closeout |
| TRADING-1807 | Lane separation policy |
| TRADING-1808 | Signal usage matrix |
| TRADING-1809 | Guardrail tests |
| TRADING-1810 to 1820 | Lane separation closeout, registry/catalog/system-flow updates, validation, commit and push |

## Expected Artifacts

- `docs/research/first_layer_v2_universal_layer_closeout.md`
- `inputs/research_reviews/first_layer_v2_universal_layer_closeout.yaml`
- `config/research/two_lane_signal_policy.yaml`
- `docs/research/two_lane_signal_policy.md`
- `inputs/research_reviews/first_layer_signal_usage_matrix.yaml`
- `docs/research/first_layer_signal_usage_matrix.md`
- `docs/research/two_layer_lane_separation_closeout.md`
- `inputs/research_reviews/two_layer_lane_separation_final_matrix.yaml`
- updates to `config/report_registry.yaml`, `docs/artifact_catalog.md`, `docs/system_flow.md`, and `docs/task_register.md`
- `tests/test_two_lane_signal_policy.py`

## Acceptance Criteria

- Current first-layer v2 is explicitly rejected as a universal trend layer.
- Closeout records allowed states `UNIVERSAL_FIRST_LAYER_REJECTED`, `RETURN_SEEKING_DIAGNOSTIC_ONLY`, and `DEFENSIVE_USAGE_BLOCKED`.
- Defensive channel allows only defensive preservation outputs and blocks add-risk / high-confidence risk-on.
- Return-seeking channel may only be diagnostic unless future gated growth overlay prerequisites are met; it cannot drive defensive overlay or promotion.
- Gated integration channel is policy-defined but not enabled; risk-off veto has priority over any growth overlay.
- Signal usage matrix gives every signal `allowed_usage`, `blocked_usage`, and `required_gate`.
- Guardrail tests prove add-risk cannot drive defensive overlay, risk-off veto blocks growth overlay, return-seeking diagnostic cannot enable promotion, universal first-layer v2 is rejected, and usage matrix fields are complete.

## Validation Plan

- `python -m ruff check tests/test_two_lane_signal_policy.py`
- `python -m compileall -q tests`
- `python -m pytest -n 16 --dist loadfile tests/test_two_lane_signal_policy.py`
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_defensive_regression_diagnosis.py tests/test_research_audit_metadata.py tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document. Implementation must remain policy/closeout only: no new defensive model, no return-seeking allocation path, no gated integration actual-path run, no owner review re-enable, no promotion, no paper-shadow, no production and no broker.
- 2026-06-28: Implemented universal first-layer v2 closeout, two-lane signal policy, first-layer signal usage matrix, lane separation closeout/final matrix, report registry entries, artifact catalog entry, system flow update and guardrail tests. Real final status=`LANE_SEPARATION_POLICY_READY`; universal first-layer status=`UNIVERSAL_FIRST_LAYER_REJECTED`; return-seeking diagnostic retained; defensive usage blocked; gated integration remains policy-defined but not enabled.
- 2026-06-28: Validation passed: `python -m ruff check tests/test_two_lane_signal_policy.py`; `python -m compileall -q tests`; `python -m pytest -n 16 --dist loadfile tests/test_two_lane_signal_policy.py`; `python -m pytest -n 16 --dist loadfile tests/test_first_layer_defensive_regression_diagnosis.py tests/test_research_audit_metadata.py`; `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`; `git diff --check`.
