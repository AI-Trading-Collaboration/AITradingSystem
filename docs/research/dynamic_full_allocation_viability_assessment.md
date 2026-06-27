# Dynamic Full Allocation Viability Assessment

元数据：

- review_id：`dynamic_strategy_closeout_2026-06-27`
- source_commit：`28cabc10b042bd9da98780070aea9f85d54c5b5d`
- market_regime：`ai_after_chatgpt`
- requested date range：`2022-12-01`～`2026-06-26`
- metric_namespace：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- promotion_status：`BLOCKED`
- owner_review_status：`OWNER_REVIEW_REQUIRED`

## 结论

结论：`PAUSE_FULL_ALLOCATION_RESEARCH`。

Dynamic strategy 当前没有足够证据作为 full allocation strategy。`limited_adjustment` 在 actual-path 下相对 `qqq_60_sgov_40` 只有极小收益优势，且低于 `100_qqq`；其他 dynamic variants 受 false risk-off、risk-on recovery delay、turnover、event override noise、walk-forward/regime fragility 和 stress blockers 限制。Batch 4 的 artifact governance 说明证据可审计，但这不等于 promotion approval。

## 必答问题

1. Dynamic strategy 是否有足够证据作为 full allocation strategy？

没有。当前 evidence 支持“存在部分防御信号价值”，不支持“完整仓位控制策略”。actual-path ranking、cost/cash、stress、regime expansion 和 walk-forward 均未形成稳定 promotion 证据。

2. 如果继续研究，最主要未解决问题是什么？

核心问题不是单个 bug，而是 full allocation edge 不稳定：risk-off 太容易错杀上涨期，risk-on 恢复慢，简单 baseline 经常更透明且表现更强，event override 的 runtime taxonomy provenance 仍不足。

3. 是否存在合理路径在短期内解决？

短期内没有。可行路径需要新的 locked-sample actual-path candidate、PIT timestamp audit、walk-forward / out-of-sample、net-of-cost、stress 和 owner-approved paper-shadow preflight，而不是继续在同一历史窗口调参。

4. 继续 historical tuning 是否会增加过拟合风险？

会。TRADING-1361～1400 已显示 `dynamic_v0_5_ai_trend_confirmed_only` 为 `REGIME_OVERFITTED`，两个 event override variants 为 `PARAMETER_SENSITIVE`。在 closeout 后继续围绕同一历史样本寻找最优参数，会降低结论可信度。

5. 是否应停止 full allocation promotion 路线？

应停止当前 promotion 路线，并把 full allocation research 状态设为 `PAUSED`。重新打开必须满足 `inputs/research_reviews/dynamic_full_allocation_reopen_criteria.yaml`。

## Owner Recommendation

默认 owner decision 建议为 `APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`。这不是批准交易，也不是进入 paper-shadow；只是承认 full allocation research 暂停，同时保留 defensive overlay / advisory diagnostic 的 observe-only 研究价值。
