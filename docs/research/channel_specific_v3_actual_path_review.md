# Channel-Specific v3 Actual-Path Review

状态：`CHANNEL_SPECIFIC_V3_ACTUAL_PATH_READY_PROMOTION_BLOCKED`

- data_quality_status: `PASS_WITH_WARNINGS`。
- defensive_probe_regression_count: `8`。
- same-risk static frontier 与 limited_adjustment 仅作为外部诊断参考，不是本批 candidate。

| variant | probe | annual_return | max_drawdown | calmar | net_of_cost_delta_vs_baseline | regression |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| baseline_first_layer_v2 | defensive_overlay_probe | 0.144202 | -0.075336 | 1.914106 | 0.0 | False |
| baseline_first_layer_v2 | balanced_dynamic_probe | 0.166786 | -0.075096 | 2.220957 | 0.0 | False |
| baseline_first_layer_v2 | drawdown_control_probe | 0.168064 | -0.092919 | 1.808719 | 0.0 | False |
| baseline_first_layer_v2 | no_tqqq_return_seeking_probe | 0.165372 | -0.081131 | 2.038323 | 0.0 | False |
| baseline_first_layer_v2 | low_tqqq_balanced_growth_probe | 0.18032 | -0.086445 | 2.085946 | 0.0 | False |
| baseline_first_layer_v2 | qqq_heavy_growth_probe | 0.182131 | -0.10493 | 1.735737 | 0.0 | False |
| baseline_first_layer_v2 | capped_risk_on_diagnostic_probe | 0.19512 | -0.099095 | 1.969016 | 0.0 | False |
| baseline_first_layer_v2 | asymmetric_risk_on_slow_confirm_probe | 0.18032 | -0.086445 | 2.085946 | 0.0 | False |
| do_not_de_risk_enabled | defensive_overlay_probe | 0.153771 | -0.076801 | 2.002199 | 0.009569 | True |
| do_not_de_risk_enabled | balanced_dynamic_probe | 0.177549 | -0.086511 | 2.052334 | 0.010763 | False |
| do_not_de_risk_enabled | drawdown_control_probe | 0.1899 | -0.115214 | 1.648237 | 0.021836 | True |
| do_not_de_risk_enabled | no_tqqq_return_seeking_probe | 0.176054 | -0.084859 | 2.074665 | 0.010682 | False |
| do_not_de_risk_enabled | low_tqqq_balanced_growth_probe | 0.191139 | -0.097428 | 1.961841 | 0.010819 | False |
| do_not_de_risk_enabled | qqq_heavy_growth_probe | 0.19283 | -0.106632 | 1.808369 | 0.010699 | False |
| do_not_de_risk_enabled | capped_risk_on_diagnostic_probe | 0.206074 | -0.109926 | 1.874663 | 0.010954 | False |
| do_not_de_risk_enabled | asymmetric_risk_on_slow_confirm_probe | 0.191139 | -0.097428 | 1.961841 | 0.010819 | False |
| risk_on_veto_enabled | defensive_overlay_probe | 0.142365 | -0.075336 | 1.889717 | -0.001837 | True |
| risk_on_veto_enabled | balanced_dynamic_probe | 0.151708 | -0.073761 | 2.056748 | -0.015078 | False |
| risk_on_veto_enabled | drawdown_control_probe | 0.145638 | -0.090138 | 1.615725 | -0.022426 | True |
| risk_on_veto_enabled | no_tqqq_return_seeking_probe | 0.155883 | -0.081131 | 1.92136 | -0.009489 | False |
| risk_on_veto_enabled | low_tqqq_balanced_growth_probe | 0.163225 | -0.086707 | 1.882478 | -0.017095 | False |
| risk_on_veto_enabled | qqq_heavy_growth_probe | 0.174545 | -0.10493 | 1.663441 | -0.007586 | False |
| risk_on_veto_enabled | capped_risk_on_diagnostic_probe | 0.170378 | -0.099471 | 1.712852 | -0.024742 | False |
| risk_on_veto_enabled | asymmetric_risk_on_slow_confirm_probe | 0.163225 | -0.086707 | 1.882478 | -0.017095 | False |
| do_not_de_risk_plus_risk_on_veto | defensive_overlay_probe | 0.151918 | -0.076801 | 1.978075 | 0.007716 | True |
| do_not_de_risk_plus_risk_on_veto | balanced_dynamic_probe | 0.162331 | -0.086744 | 1.871381 | -0.004455 | False |
| do_not_de_risk_plus_risk_on_veto | drawdown_control_probe | 0.167055 | -0.115553 | 1.445699 | -0.001009 | True |
| do_not_de_risk_plus_risk_on_veto | no_tqqq_return_seeking_probe | 0.166477 | -0.085003 | 1.958498 | 0.001105 | False |
| do_not_de_risk_plus_risk_on_veto | low_tqqq_balanced_growth_probe | 0.173887 | -0.097687 | 1.780036 | -0.006433 | False |
| do_not_de_risk_plus_risk_on_veto | qqq_heavy_growth_probe | 0.185175 | -0.106632 | 1.736583 | 0.003044 | False |
| do_not_de_risk_plus_risk_on_veto | capped_risk_on_diagnostic_probe | 0.181106 | -0.110297 | 1.641985 | -0.014014 | False |
| do_not_de_risk_plus_risk_on_veto | asymmetric_risk_on_slow_confirm_probe | 0.173887 | -0.097687 | 1.780036 | -0.006433 | False |
| flat_reference | defensive_overlay_probe | 0.165657 | -0.140068 | 1.182685 | 0.021455 | True |
| flat_reference | balanced_dynamic_probe | 0.165657 | -0.140068 | 1.182685 | -0.001129 | False |
| flat_reference | drawdown_control_probe | 0.184872 | -0.162659 | 1.136563 | 0.016808 | True |
| flat_reference | no_tqqq_return_seeking_probe | 0.175281 | -0.151422 | 1.157566 | 0.009909 | False |
| flat_reference | low_tqqq_balanced_growth_probe | 0.175281 | -0.151422 | 1.157566 | -0.005039 | False |
| flat_reference | qqq_heavy_growth_probe | 0.194429 | -0.17378 | 1.118818 | 0.012298 | False |
| flat_reference | capped_risk_on_diagnostic_probe | 0.175281 | -0.151422 | 1.157566 | -0.019839 | False |
| flat_reference | asymmetric_risk_on_slow_confirm_probe | 0.175281 | -0.151422 | 1.157566 | -0.005039 | False |
