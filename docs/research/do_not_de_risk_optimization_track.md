# Do-Not-De-Risk Optimization Track

状态：`DO_NOT_DERISK_TRACK_READY`

本 track 只研究如何减少 false risk-off，不允许把 `do_not_de_risk` 转成 add-risk 或 TQQQ increase。

预注册通过条件：

- `false_risk_off_cost` 下降。
- `missed_upside` 下降。
- `defensive_probe_regression_count=0`。
- `actual_path_improved_probe_count>=2`。
- 2022 slice 不恶化。

任何结果若没有通过 `config/research/do_not_de_risk_selection_rule.yaml` 的 selection rule，不能成为 candidate。当前 promotion、paper-shadow、production、broker 全部 blocked。
