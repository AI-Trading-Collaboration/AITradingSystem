# Owner Review Gate 语义

| gate | layer | owner review meaning | tradeoff explanation |
|---|---|---|---|
|`2022_slice_not_worse_than_flat_reference`|`slice_review_gate`|`OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY`|2022_slice_not_worse_than_flat_reference 需要先区分 severe slice regression 与轻微 return / drawdown tradeoff，再决定是否拒绝候选。|
|`not_2023_plus_only`|`owner_review_risk_flag`|`OWNER_REVIEW_REQUIRED`|TRADING-2274 显示该 gate 阻断了 highest utility actual-path candidate；它保留为 2023+ dependence risk note，但不再是自动拒绝规则。|
|`not_beta_dependency`|`inconclusive_diagnostic_gate`|`DIAGNOSTIC_ONLY`|当前 dependency evidence 仍是 lane-level diagnostic evidence，不是 candidate-level 一票否决 proof。|
|`not_tqqq_dependency`|`inconclusive_diagnostic_gate`|`DIAGNOSTIC_ONLY`|当前 dependency evidence 仍是 lane-level diagnostic evidence，不是 candidate-level 一票否决 proof。|
|`probability_threshold_0_55`|`threshold_sensitivity_gate`|`THRESHOLD_SENSITIVITY_ONLY`|0.55 / 0.60 probability thresholds 缺少 candidate-level probability distribution evidence，应进入 calibration 和 threshold sensitivity reporting。|
|`probability_threshold_0_60`|`threshold_sensitivity_gate`|`THRESHOLD_SENSITIVITY_ONLY`|0.55 / 0.60 probability thresholds 缺少 candidate-level probability distribution evidence，应进入 calibration 和 threshold sensitivity reporting。|
|`all_slices_not_worse`|`slice_review_gate`|`OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY`|all_slices_not_worse 需要先区分 severe slice regression 与轻微 return / drawdown tradeoff，再决定是否拒绝候选。|
|`no_slice_regression`|`slice_review_gate`|`OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY`|no_slice_regression 需要先区分 severe slice regression 与轻微 return / drawdown tradeoff，再决定是否拒绝候选。|

Owner review 只允许候选进入 offline review / owner review 状态；不允许自动 promotion、paper-shadow、production 或 broker action。
