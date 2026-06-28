# Boundary-Aware Two-Layer Optimization Framework Closeout

状态：`BASE_OVERLAY_VETO_FRAMEWORK_READY`

本批完成 boundary-aware two-layer framework 的 research-only 基础闭环。它没有恢复 gated integration，也没有产生 allocation candidate。

## 已完成状态

- `BOUNDARY_CONTRACT_READY`
- `CHANNEL_POLICY_READY`
- `BASE_OVERLAY_VETO_FRAMEWORK_READY`
- `INDICATOR_FAMILY_ABLATION_READY`
- `DO_NOT_DERISK_TRACK_READY`
- `RISK_ON_VETO_TRACK_READY`
- `RETURN_SEEKING_DIAGNOSTIC_LOG_READY`

## 关键边界

- Defensive channel 不能 emit add-risk、risk-on 或 TQQQ signal。
- Return-seeking diagnostic channel 不能 emit weights，不能启用 promotion。
- Risk veto priority 高于 growth overlay。
- Second layer 不允许读取 raw indicators。
- 所有 optimization tracks 都有 pre-registered selection rule。

## 结论

当前结果是框架 ready，不是策略 ready。Dynamic promotion、paper-shadow、production 和 broker 全部继续 disabled；owner review candidate count 为 0。

## 验证

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_two_layer_boundary_contract.py tests/test_two_layer_policy_compiler.py tests/test_two_layer_error_attribution.py tests/test_channel_aware_actual_path_evaluator.py tests/test_boundary_aware_selection_rules.py tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py tests/test_research_artifact_governance.py tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
