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

安全边界固定：

- `primary_window_validated=false`
- `model_ready_for_2021_primary_window=false`
- `reopen_gate_allowed=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `purchase_allowed_without_owner_approval=false`
