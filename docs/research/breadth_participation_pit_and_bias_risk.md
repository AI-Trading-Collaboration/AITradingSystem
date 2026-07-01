# Breadth Participation PIT and Bias Risk

|target_etf|historical_constituents_available|proxy_only|survivorship|lookahead|recommendation|
|---|---:|---:|---|---|---|
|`QQQ`|False|True|`HIGH_SURVIVORSHIP_BIAS`|`HIGH_LOOKAHEAD_BIAS`|`CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY`|
|`SPY`|False|True|`HIGH_SURVIVORSHIP_BIAS`|`HIGH_LOOKAHEAD_BIAS`|`CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY`|
|`SMH`|False|True|`HIGH_SURVIVORSHIP_BIAS`|`HIGH_LOOKAHEAD_BIAS`|`CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY`|

|target_etf|acceptable_for_poc|actual_path_validation|promotion|notes|
|---|---:|---:|---:|---|
|`QQQ`|True|False|False|Diagnostics-only proxy. It must not be relabeled as strict PIT, actual-path validation evidence, promotion evidence, or broker input.|
|`SPY`|True|False|False|Diagnostics-only proxy. It must not be relabeled as strict PIT, actual-path validation evidence, promotion evidence, or broker input.|
|`SMH`|True|False|False|Diagnostics-only proxy. It must not be relabeled as strict PIT, actual-path validation evidence, promotion evidence, or broker input.|
