# TRADING-597 to 604 B2 Final Evidence and Role Decision
最后更新：2026-06-23

## 背景

TRADING-588~596 已确认当前 B2 不支持 fast asymmetric risk overlay。正面证据只集中在 `slow_drawdown`，且仍是 single-window-only；re-entry lag 为 signal-driven，窗口内未观察到恢复到 normal exposure。B4/B5/B6/v3 与 paper-shadow 均保持 blocked。

## 安全边界

- research-only、manual-review-only。
- 不允许 paper-shadow、extended shadow、live trading、official target weights、broker/order、production mutation。
- 不访问 untouched holdout。
- 本批次不运行 B3/B4/B5/B6/v3。
- 不调 B2 threshold、re-entry logic 或其他参数。
- 不自动 append owner decision。

## 任务拆解

|任务|目标|状态|
|---|---|---|
|TRADING-597|B2 slow-drawdown evidence completion|VALIDATING|
|TRADING-598|B2 slow-drawdown edge validation|VALIDATING|
|TRADING-599|B2 fast-risk role deprecation review|VALIDATING|
|TRADING-600|B2 re-entry lag design implication|VALIDATING|
|TRADING-601|B2 role reclassification|VALIDATING|
|TRADING-602|B2 final research gate|VALIDATING|
|TRADING-603|B2 research line owner packet|VALIDATING|
|TRADING-604|B2 branch snapshot final|VALIDATING|

## 预期产物

- `b2_slow_drawdown_evidence_completion.json/md`
- `b2_slow_drawdown_edge_validation.json/md`
- `b2_fast_risk_role_deprecation_review.json/md`
- `b2_reentry_lag_design_implication.json/md`
- `b2_role_reclassification.json/md`
- `b2_final_research_gate.json/md`
- `b2_research_line_owner_packet.json/md`
- `b2_branch_snapshot_final.json/md`

## 验收标准

- TRADING-597 必须先检查 `research_window_catalog` 与现有 B2 artifacts 是否存在第二个独立、非 holdout slow-drawdown diagnostic window；如果没有，不得补造窗口，必须输出 `B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW`。
- TRADING-598 不得把 single-window slow-drawdown edge 解释为 repeatable。
- TRADING-599 必须明确 current B2 不再声称 fast-risk overlay capability，除非证据状态发生实质变化。
- TRADING-600 必须记录 signal recovery、exposure recovery、missed rebound proxy、lag root cause 与是否系统性；不得调 re-entry logic。
- TRADING-601~602 必须把 role classification 与 final gate 绑定到 repeatability、utility、re-entry lag、fast-risk role 和 holdout 边界；不得允许 B4/B5/B6/v3 或 paper-shadow。
- TRADING-603 必须生成 owner-readable packet，但不得写 owner decision。
- TRADING-604 必须输出 final branch snapshot，保留 `B4_retest_allowed=false`、`b5_allowed=false`、`b6_allowed=false`、`v3_allowed=false`、`paper_shadow_allowed=false`。
- 所有产物必须披露 `ai_after_chatgpt` regime、requested date range、data quality gate、source artifacts、Reader Brief 和 safety boundary。
- focused tests、JSON parse、ruff、compileall、`git diff --check` 通过。

## 状态记录

- 2026-06-19：新增本批次并进入 IN_PROGRESS，原因：TRADING-596 仍为 `CONTINUE_B2_ONLY_RESEARCH`，但 current form 已不支持 fast-risk 角色，slow_drawdown repeatability 缺第二独立窗口，re-entry lag 仍是关键风险；本批次需要给出 current-form final evidence 和 role decision，继续保持 B3/B4/B5/B6/v3 与 paper-shadow blocked。
- 2026-06-19：实现完成并转入 VALIDATING；`b2-final-decision-research` 输出 TRADING-597=`B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW`、598=`B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY`、599=`B2_FAST_RISK_ROLE_DEPRECATED`、600=`B2_REENTRY_LAG_REQUIRES_DESIGN_REWORK`、601=`B2_RISK_OVERLAY_NEEDS_REDESIGN`、602=`B2_CURRENT_FORM_RETURN_TO_DESIGN`、603=`B2_RESEARCH_LINE_OWNER_PACKET_READY`、604=`RETURN_B2_TO_DESIGN`。结论：current catalog 没有第二个独立非 holdout slow_drawdown diagnostic window，single-window edge 阻断 promising/narrow module；current B2 fast-risk role deprecated；re-entry lag 需设计复核；final branch 返回 B2 design，B3/B4/B5/B6/v3 和 paper-shadow 继续 blocked。
