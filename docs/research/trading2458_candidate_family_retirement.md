# TRADING-2458 Candidate Family 正式退役记录

- 状态：`RETIRED`
- 主动消费：`BLOCKED_RETIRED_CANDIDATE_FAMILY`
- Owner 决策：`owner_decision:TRADING-2458:2026-07-25:retire_current_saturated_candidate_family`
- Package：`dynamic-v3-clean-trading2452_11991ac7965cfcd7aa18`
- Candidate universe：`dynamic-v3-clean-universe-trading2452_aa64f915302704bb9224`（300 个）
- 研究窗口：requested/evaluated start `2021-02-22` / `2021-02-22`，evaluated end `2025-12-31`

## 结论

当前四个 template、七个 candidate axis 和 300-candidate universe 已正式退役。
旧 manifest 中的历史 eligibility 仅作为 immutable evidence 保留，不再赋予任何主动资格。

允许的动作仅限：

- `historical_evidence_read`
- `historical_evidence_identity_validation`
- `historical_diagnostic_content_validation`

以下动作均 fail closed：

- `package_write`
- `package_rebuild_to_disk`
- `historical_evaluator_rerun`
- `candidate_expansion`
- `parameter_search`
- `candidate_selection`
- `watchlist_enrollment`
- `paper_shadow_enrollment`
- `promotion`
- `production_reuse`
- `broker_execution`

## 边界

- 不退役通用 research framework 或 `equal_risk_qqq_sgov` forward aging。
- 不改变 QLD 的 role-limited implementation instrument 定位。
- 不授权新 hypothesis/generator、constraint gate、prospective、paper-shadow 或生产行为。
- 新 family 必须使用新 identity、独立预注册、无污染 selection protocol 和新 Owner 决策。

全过程：`production_effect=none`、`broker_action=none`。
