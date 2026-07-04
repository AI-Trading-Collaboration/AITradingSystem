# High-Intensity Scheduler Wiring Safety Gate

- safety_gate_status: `PASS`
- safety_error_count: `0`
- block_if_scheduler_enabled: `True`
- block_if_target_weight_generated: `True`
- block_if_broker_action_requested: `True`

Safety gate 对 scheduler enabled/default enabled、target weight、rebalance、
broker action、paper-shadow、production 和 manual-review trade instruction 全部 fail closed。