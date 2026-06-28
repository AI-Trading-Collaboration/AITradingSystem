# First-Layer Reopen Gate Decision

状态：`FIRST_LAYER_REOPEN_DENIED`

## 摘要

- `decision_status`: `FIRST_LAYER_REOPEN_DENIED`
- `reopen_allowed`: `False`
- `owner_approval_recorded`: `False`
- `blockers`: `['pit_warning_used_as_model_ready', 'owner_approval_missing']`
- `free_feature_final_status`: `DIAGNOSTIC_ONLY_EVIDENCE`
- `participation_final_status`: `NORGATE_DUE_DILIGENCE_RECOMMENDED`
- `candidate_count`: `0`
- `promotion_allowed`: `False`

本产物只执行 reopen decision gate；不做模型优化，不输出权重，不进入 paper-shadow、production 或 broker。
