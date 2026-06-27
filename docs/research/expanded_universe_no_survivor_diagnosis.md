# Expanded Universe No-Survivor Diagnosis

- Status: `EXPANDED_UNIVERSE_NO_SURVIVOR_DIAGNOSIS_READY`
- Market regime: `ai_after_chatgpt`
- Candidate count: `11`
- Full allocation survivor count: `0`
- Static frontier dominates: `7`
- No material improvement: `4`
- Walk-forward failed or pending: `11`

结论：full allocation gate 继续 `BLOCKED`。本报告只把失败原因转成 defensive overlay 诊断输入，不恢复 promotion、paper-shadow、production 或 broker。

## Candidate Rows

|candidate_id|verdict|same_risk_baseline|walk_forward_failed|stress_risk_too_high|net_of_cost_failed|
|---|---|---|---|---|---|
|expanded_state_highest_return_under_max_dd_cap|STATIC_FRONTIER_DOMINATES|simplex_qqq0900_sgov0050_tqqq0050|True|True|True|
|expanded_state_lowest_turnover|STATIC_FRONTIER_DOMINATES|simplex_qqq0950_sgov0050_tqqq0000|True|False|True|
|expanded_state_highest_calmar|STATIC_FRONTIER_DOMINATES|simplex_qqq0850_sgov0150_tqqq0000|True|False|True|
|expanded_state_highest_sharpe|STATIC_FRONTIER_DOMINATES|simplex_qqq0850_sgov0150_tqqq0000|True|False|True|
|expanded_state_lowest_drawdown|STATIC_FRONTIER_DOMINATES|simplex_qqq0850_sgov0150_tqqq0000|True|False|True|
|limited_adjustment|STATIC_FRONTIER_DOMINATES|simplex_qqq0600_sgov0400_tqqq0000|True|False|True|
|static_simplex_qqq0150_sgov0850_tqqq0000|NO_MATERIAL_IMPROVEMENT|simplex_qqq0000_sgov0950_tqqq0050|True|False|False|
|static_simplex_qqq0000_sgov0950_tqqq0050|NO_MATERIAL_IMPROVEMENT|simplex_qqq0000_sgov0950_tqqq0050|True|False|False|
|static_simplex_qqq0100_sgov0900_tqqq0000|NO_MATERIAL_IMPROVEMENT|simplex_qqq0100_sgov0900_tqqq0000|True|False|False|
|static_simplex_qqq0050_sgov0950_tqqq0000|NO_MATERIAL_IMPROVEMENT|simplex_qqq0050_sgov0950_tqqq0000|True|False|False|
|static_simplex_qqq0000_sgov1000_tqqq0000|STATIC_FRONTIER_DOMINATES|simplex_qqq0000_sgov1000_tqqq0000|True|False|True|
