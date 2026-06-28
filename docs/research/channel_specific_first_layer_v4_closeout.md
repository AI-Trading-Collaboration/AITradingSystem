# Channel-Specific First-Layer v4 Closeout

状态：`CHANNEL_V4_REOPEN_EVIDENCE_INSUFFICIENT`

## 摘要

- `final_status`: `CHANNEL_V4_REOPEN_EVIDENCE_INSUFFICIENT`
- `start_allowed`: `False`
- `blockers`: `['reopen_gate_not_allowed', 'owner_approval_missing']`
- `candidate_count`: `0`
- `promotion`: `blocked`
- `paper_shadow`: `False`
- `production`: `False`
- `broker`: `none`

v4 只有在 reopen gate 和 owner approval 同时通过后才允许继续；当前产物不训练模型、不输出 allocation、不进入 paper-shadow/production/broker。
