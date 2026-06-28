# Norgate 2Y Partial Evidence Review Policy

- policy_id: `norgate_2y_partial_evidence_review_policy_v1`
- status: `active`
- production_effect: `none`
- broker_action: `none`

该 policy 固定 TRADING-2268 的 2Y partial evidence review 口径：feature
variation、bucket balance、outcome dominance、event count、baseline incremental
delta、QQQ/SPY/SMH benchmark consistency 和 2022 stress-window 缺口。所有阈值只用于
解释 Norgate trial 局部证据和 owner purchase memo，不是 production threshold、
promotion gate 或正式 first-layer acceptance rule。

购买语义固定为 split decision：trial-based purchase recommendation 只回答
2Y trial 是否已经证明直接购买；paid stress-window experiment recommendation 只回答
owner 是否可选择付费验证 2021-2024 压力窗口。当前 no-incremental-value 路径必须输出
`trial_based_purchase_recommendation=no`、
`stress_window_paid_experiment_recommendation=conditional_yes`，但
`purchase_allowed=false`。

安全边界固定：

- `primary_window_validated=false`
- `model_ready_for_2021_primary_window=false`
- `reopen_gate_allowed=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `purchase_allowed=false`
- `purchase_allowed_without_owner_approval=false`
