# Event Override Execution Semantics Review

- Market regime: `ai_after_chatgpt`
- Date range: `2022-12-01` to `2026-06-26`
- Dynamic promotion: `BLOCKED`
- Event reviews: `36`
- Override triggers: `36`
- Pending plan supersedes: `36`
- T+1 executions: `36`
- Blocked overrides: `0`

## Owner Questions

1. T 日事件触发 T+1 调整是否被正确建模？是，override decision 的 effective_at 必须晚于 decision_at，actual execution 仅在 T+1 path row 记录。
2. pending plan supersede 是否无未来函数？是，trace 写出 event_known_at、review_at、decision_at、effective_at 和 no-lookahead checks。
3. 哪些 pending plan 被覆盖？见 per-strategy `pending_plan_ledger.csv` 与 `supersede_log.csv`。
4. 覆盖后 actual-path 是否改善？见下方 base vs event override 表。
5. 改善来自降低回撤，还是减少错误风险暴露？矩阵记录 drawdown delta、QQQ exposure drag delta 和 risk-off event net contribution。
6. 是否牺牲过多上涨收益？用 annual_return_delta_vs_base 与 verdict 判断。
7. 是否增加过多 turnover？用 turnover_delta_vs_base 与 verdict 判断。
8. 哪个候选仍值得继续 watch？仅允许 owner 复核 watch-only event override candidates。
9. 是否识别出 paper-shadow preflight candidate？见 matrix 的 paper_shadow_preflight_candidate；它不是 paper-shadow 批准。
10. 为什么 dynamic promotion 仍保持 blocked？owner manual review、paper-shadow preflight 和 production approval 均未发生，target-path metrics 仍为 diagnostic-only。

## Base vs Event Override

|strategy_id|base_strategy|annual_return_delta|max_drawdown_delta|turnover_delta|trigger_count|verdict|
|---|---|---:|---:|---:|---:|---|
|limited_adjustment_event_override_v1|limited_adjustment|-0.002084|0.00098|4.4|9|EVENT_OVERRIDE_INCREASES_TURNOVER_TOO_MUCH|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|dynamic_v0_5_ai_trend_confirmed_only|0.002111|-0.000318|15.3|27|EVENT_OVERRIDE_TOO_NOISY|

## Safety Boundary

- research_only=true
- paper_shadow_allowed=false
- production_allowed=false
- broker_action=none
