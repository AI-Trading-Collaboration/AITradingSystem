# Breadth Participation Data Inventory

TRADING-2302 不读取 market cache；下表是静态 feasibility inventory。

|input_name|category|target_etf|pit_status|bias_risk|recommended_usage|
|---|---|---|---|---|---|
|`QQQ_etf_price_history`|etf_index_price_history|`QQQ`|`PIT_APPROXIMATION_READY`|`LOW_BIAS`|benchmark_anchor_after_aits_validate_data_pass|
|`QQQ_current_constituents_snapshot`|etf_current_constituents|`QQQ`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|diagnostics_only_after_snapshot_freeze|
|`QQQ_historical_constituents`|historical_etf_constituents|`QQQ`|`BLOCKED_NO_RELIABLE_DATA`|`UNACCEPTABLE_FOR_VALIDATION`|data_source_decision_required_before_validation|
|`QQQ_constituent_price_history`|constituent_price_history|`QQQ`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|coverage_audit_only_until_membership_source_exists|
|`SPY_etf_price_history`|etf_index_price_history|`SPY`|`PIT_APPROXIMATION_READY`|`LOW_BIAS`|benchmark_anchor_after_aits_validate_data_pass|
|`SPY_current_constituents_snapshot`|etf_current_constituents|`SPY`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|diagnostics_only_after_snapshot_freeze|
|`SPY_historical_constituents`|historical_etf_constituents|`SPY`|`BLOCKED_NO_RELIABLE_DATA`|`UNACCEPTABLE_FOR_VALIDATION`|data_source_decision_required_before_validation|
|`SPY_constituent_price_history`|constituent_price_history|`SPY`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|coverage_audit_only_until_membership_source_exists|
|`SMH_etf_price_history`|etf_index_price_history|`SMH`|`PIT_APPROXIMATION_READY`|`LOW_BIAS`|benchmark_anchor_after_aits_validate_data_pass|
|`SMH_current_constituents_snapshot`|etf_current_constituents|`SMH`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|diagnostics_only_after_snapshot_freeze|
|`SMH_historical_constituents`|historical_etf_constituents|`SMH`|`BLOCKED_NO_RELIABLE_DATA`|`UNACCEPTABLE_FOR_VALIDATION`|data_source_decision_required_before_validation|
|`SMH_constituent_price_history`|constituent_price_history|`SMH`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|coverage_audit_only_until_membership_source_exists|
|`current_constituents_proxy_breadth`|alternative_proxy_inputs|`ALL`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_SURVIVORSHIP_BIAS`|diagnostics_only_after_snapshot_freeze|
|`equal_weight_etf_proxy`|alternative_proxy_inputs|`QQQ/SPY`|`PIT_APPROXIMATION_READY`|`MODERATE_BIAS`|offline_diagnostics_only_not_true_breadth|
|`sector_etf_proxy`|alternative_proxy_inputs|`QQQ/SMH`|`PIT_APPROXIMATION_READY`|`MODERATE_BIAS`|diagnostic_leadership_proxy_only|
|`mega_cap_concentration_proxy`|alternative_proxy_inputs|`QQQ/SPY/SMH`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`HIGH_LOOKAHEAD_BIAS`|warning_only_after_weight_source_audit|
|`qqq_vs_equal_weight_nasdaq_proxy`|alternative_proxy_inputs|`QQQ`|`PIT_APPROXIMATION_READY`|`MODERATE_BIAS`|diagnostics_only_not_constituent_membership|
|`smh_internal_leadership_proxy`|alternative_proxy_inputs|`SMH`|`PIT_APPROXIMATION_READY`|`MODERATE_BIAS`|diagnostics_only_not_internal_membership|

ETF / index price history 只可在后续 cached-data quality gate 通过后使用；current constituent snapshots 只能 diagnostics；historical constituents 缺口仍是 strict PIT blocker。
