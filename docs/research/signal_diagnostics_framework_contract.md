# Signal Diagnostics Framework Contract

- Status：SIGNAL_DIAGNOSTICS_FRAMEWORK_READY
- Accepted Statuses：SIGNAL_DIAGNOSTICS_PASS, SIGNAL_DIAGNOSTICS_PASS_WITH_WARNINGS, SIGNAL_DIAGNOSTICS_BLOCKED

## Required Checks

- coverage
- freshness
- schema_compatibility
- missingness
- state_transitions
- cross_window_stability
- event_based_diagnostics
- robustness_status
- fail_closed_reason

## Reader Brief

- Summary：通用 signal diagnostics framework 已冻结，只评价 signal 质量。
- Key Result：SIGNAL_DIAGNOSTICS_FRAMEWORK_READY
- Blocking Issues：B2/B3 进入 target mapping 前必须得到非 BLOCKED diagnostics。
- Warnings：Diagnostics PASS 不是组合收益或候选晋级结论。
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：实现 B2/B3 signal 时先输出 signal artifact 并运行 diagnostics。
