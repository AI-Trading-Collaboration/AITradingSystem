# High-Intensity Threshold Selection Caveat Report

- selection_caveat_id: `HIGH_INTENSITY_THRESHOLD_SELECTION_CAVEAT_V1`
- selected_threshold_id: `COMPOSITE_HIGH_INTENSITY_RULE`
- uses_future_return_optimization: `False`
- strict_pit_ready: `False`
- pit_approximation_ready: `True`
- threshold_stability_risk: `MODERATE`
- missed_stress_risk: `MEDIUM`
- false_warning_risk: `MODERATE`

Selected threshold 不是已经 forward validated 的 production rule；它只是进入 TRADING-2336 observe-only event logger 的研究规则。