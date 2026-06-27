# Dynamic Strategy Blocker Inventory

元数据：

- review_id：`dynamic_strategy_closeout_2026-06-27`
- source_commit：`28cabc10b042bd9da98780070aea9f85d54c5b5d`
- market_regime：`ai_after_chatgpt`
- requested date range：`2022-12-01`～`2026-06-26`
- metric_namespace：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- promotion_status：`BLOCKED`
- owner_review_status：`OWNER_REVIEW_REQUIRED`
- policy_hash：`7fd0905654ff11f436d407bee1fd123760c74f76750401b12de57f50abbe8bf7`

## 结论

TRADING-1326～1485 的 blocker 已经足够说明：dynamic strategy 不应继续作为 full allocation promotion 主线推进。`limited_adjustment` 是唯一仍有 actual-path edge 的候选，但只略高于 `qqq_60_sgov_40`，明显低于 `100_qqq`，且 stress gate 仍为 blocked。其他 dynamic / event override / staleness-aware variants 主要暴露 false risk-off、turnover、policy sensitivity、regime fragility 和 runtime taxonomy gap。

## Blocker Inventory

|Blocker|类别|严重性|状态|主要证据|建议|
|---|---|---|---|---|---|
|`DYN-CLOSEOUT-001_TARGET_PATH_LEGACY_EVIDENCE`|governance|blocking|accepted|`research_artifact_governance_snapshot.yaml`|旧 dynamic evidence 标注为 `CLOSEOUT_REVIEWED_LEGACY_EVIDENCE`，target-path metrics 只保留 diagnostic role。|
|`DYN-CLOSEOUT-002_FULL_ALLOCATION_EDGE_NOT_STABLE`|execution|blocking|open|`actual_path_edge_attribution_matrix.yaml`|暂停 full allocation research，不继续历史调参。|
|`DYN-CLOSEOUT-003_STALENESS_REPAIR_NO_IMPROVEMENT`|staleness|material|closed|`staleness_repair_matrix.yaml`|两个 repaired variants 均归档为 research evidence。|
|`DYN-CLOSEOUT-004_EVENT_OVERRIDE_TAXONOMY_RUNTIME_GAP`|event_override|blocking|open|`event_override_ex_ante_taxonomy.yaml`|事件 override 进入任何 preflight 前必须补 timestamped external event taxonomy provenance。|
|`DYN-CLOSEOUT-005_EVENT_OVERRIDE_TURNOVER_AND_NOISE`|event_override|material|open|`event_override_survival_matrix.yaml`|event override 仅允许 observe-only diagnostic。|
|`DYN-CLOSEOUT-006_RISK_OFF_TOO_NOISY`|stress|blocking|open|`risk_timing_quality_matrix.yaml`|只保留 risk-reduction advisory 与 manual review，不自动控制仓位。|
|`DYN-CLOSEOUT-007_PIT_DATE_LEVEL_CAVEAT`|data|material|accepted|`pit_data_availability_inventory.yaml`|full allocation 重新打开前必须有更强 PIT timestamp audit。|
|`DYN-CLOSEOUT-008_WALK_FORWARD_REGIME_FRAGILITY`|overfitting|blocking|open|`dynamic_strategy_walk_forward_matrix.yaml`|继续 historical tuning 会增加过拟合风险。|
|`DYN-CLOSEOUT-009_COST_TURNOVER_DRAG`|cost|material|open|`transaction_cost_cash_yield_matrix.yaml`|高换手变体不得 promotion，overlay watch 必须披露 net-of-cost。|
|`DYN-CLOSEOUT-010_STRESS_GATE_BLOCKED`|stress|blocking|open|`stress_risk_metrics_matrix.yaml`|stress signals 只能作为 advisory flags。|
|`DYN-CLOSEOUT-011_SIMPLE_BASELINES_NOT_BEATEN`|regime|blocking|open|`regime_baseline_expansion_matrix.yaml`|allocation 结论优先使用简单透明 baseline，dynamic modules 降级为 diagnostics。|

## 后续处理

这些 blocker 不需要通过新的历史参数搜索继续“修复”。更合理的处理是关闭 full allocation promotion 路线，把 surviving risk modules 降级为 defensive overlay / advisory diagnostic，并在 owner 批准后进入 observe-only forward watch。
