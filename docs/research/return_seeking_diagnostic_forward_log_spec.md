# Return-Seeking Diagnostic Forward Log Spec

状态：`RETURN_SEEKING_DIAGNOSTIC_LOG_READY`

Forward log 只记录 diagnostic evidence：

- `signal`
- `confidence`
- `diagnostic_flag`
- `future_realized_outcomes`
- `realized_outcome_window`

禁止字段：

- target weights
- trade advice
- paper-shadow action
- broker action
- production action

该 log 用于后验 action-value research 和 indicator family review，不是 owner-review candidate evidence。
