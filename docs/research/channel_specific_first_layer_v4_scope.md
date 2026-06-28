# Channel-Specific First-Layer v4 Scope

状态：`CHANNEL_SPECIFIC_FIRST_LAYER_V4_SCOPE_FAIL_CLOSED`

## 摘要

- `start_allowed`: `False`
- `gate_status`: `FIRST_LAYER_REOPEN_DENIED`
- `owner_approval_recorded`: `False`
- `blockers`: `['reopen_gate_not_allowed', 'owner_approval_missing']`
- `universal_first_layer_allowed`: `False`
- `TQQQ_allocation_allowed`: `False`

v4 只有在 reopen gate 和 owner approval 同时通过后才允许继续；当前产物不训练模型、不输出 allocation、不进入 paper-shadow/production/broker。
