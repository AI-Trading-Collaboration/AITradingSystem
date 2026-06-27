# Research Window Extension Owner Review Pack

- 状态：`RESEARCH_WINDOW_EXTENSION_OWNER_REVIEW_READY_PROMOTION_BLOCKED`
- 市场周期：`ai_after_chatgpt`

## 摘要
- primary_window: `exact_three_asset_validated`
- primary_start: `2021-02-22`
- extension_window: `exact_three_asset_primary_only_extension`
- extension_start: `2020-05-28`
- legacy_window: `legacy_research_window_2022_12`
- static_metric_rows: `198`
- probe_metric_rows: `15`
- actual_path_metric_rows: `30`
- promotion_status: `BLOCKED`

## 安全边界
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Owner 问答

- why_extend_from_2022_12: `2022-12 窗口过短，且高度集中在 AI/tech 强趋势阶段。`
- why_not_2020_05_26_portfolio_start: `SGOV primary cache 缺少 2020-05-26 和 2020-05-27 可交易行。`
- extension_caveat: `2020-05-28 extension 在 2021-02-22 前存在 SGOV secondary-source gap。`
- effect_on_conclusions: `所有结果继续是 research-only，并且必须按 window tag 解读。`
- why_promotion_blocked: `Window extension 只是验证证据，不是 owner approval 或 production readiness。`
