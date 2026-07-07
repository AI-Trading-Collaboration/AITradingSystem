# TRADING-2413 Growth Tilt Engine Source Traceability Remediation

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source task：`TRADING-2412`
- blocker under review：`growth_tilt_engine`
- next route：`TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation`

## 背景

TRADING-2412 已完成 `growth_tilt_engine` as-of semantics remediation，真实 status 为 `GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`，`input_gap_count=7`，`as_of_gap_count=2`，`as_of_remediated_count=2`，`remaining_blocked_or_gap_count=7`，`contract_ready_count=0`，并把下一步限定为 TRADING-2413 source traceability remediation。

TRADING-2413 的目标是处理 TRADING-2411 remediation plan 中 `source_traceability_required`，以及 TRADING-2412 updated source feature mapping 中仍存在 missing / ambiguous source traceability 的 source features。2413 只补 source traceability metadata；不处理 signal validity dependency，不实现 `valid_until_window`，不解除或降级 blocker。

## 输入

- TRADING-2411：
  - `remediation_plan_result.json`
  - `ordered_remediation_items.json`
  - `unresolved_blocker_summary.json`
- TRADING-2412：
  - `as_of_remediation_result.json`
  - `before_after_remediation.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
  - `docs/research/growth_tilt_engine_as_of_semantics_remediation.md`
- Governed config / registry references：
  - `config/research/equal_risk_growth_tilt_candidate_registry.yaml`
  - `config/report_registry.yaml`
  - `docs/artifact_catalog.md`

## Source Traceability Remediation Status

每个被处理对象必须归入以下状态之一：

- `source_traceability_remediated`
- `source_traceability_partially_remediated`
- `source_traceability_blocked_by_missing_upstream_artifact`
- `source_traceability_blocked_by_missing_registry_entry`
- `source_traceability_blocked_by_ambiguous_source_boundary`
- `source_traceability_not_applicable_non_signal_feature`
- `source_traceability_unresolved`

禁止出现未分类状态。

## Contract Metadata

2413 需要为 source traceability gap feature 输出：

- `source_feature_id`
- `source_feature_name`
- `upstream_source_system`
- `upstream_artifact_path`
- `upstream_artifact_id`
- `upstream_report_registry_id`
- `upstream_config_path`
- `upstream_config_key`
- `source_snapshot_reference`
- `source_snapshot_hash`
- `derived_from_prior_artifact`
- `fresh_market_data_required=false`
- `traceability_status`
- `traceability_blocking_reason`

对于 governed config source，可以使用 config file + key + hash 形成 traceability evidence。对于 only-described derived research windows 或 missing standalone signal artifact，若无法确认 source snapshot / hash / artifact path，必须保持 blocked 或 unresolved，不能凭空生成 snapshot。

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2413 只处理 source traceability 维度。
2. 新增 reusable source traceability remediation helper，识别 2411 / 2412 中的 traceability gap items。
3. 生成 source traceability contract metadata、before / after remediation artifact 和 updated source feature mapping。
4. 新增任务级 wrapper，fail-closed 读取 2411 / 2412 artifacts/docs/registry/catalog references。
5. 新增 CLI `aits research strategies growth-tilt-engine-source-traceability-remediation`。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`。
- TRADING-2411 / 2412 中所有 source traceability 相关 items 都被处理或明确保留 blocked。
- 每个处理对象都有 before / after 状态。
- 每个处理对象都有确定的 traceability remediation status。
- 上游 artifact / registry / config 引用必须可追踪，不能凭空生成。
- 无法确认 source snapshot / hash / upstream reference 的 feature 必须保持 blocked 或 unresolved。
- as-of semantics 已修复的状态不能被回退。
- 未完成 validity dependency / PIT gate 的 feature 不能被标记为 contract-ready。
- `contract_ready_count=0`。
- `growth_tilt_engine_blocker_resolved=false`、`growth_tilt_engine_blocker_downgraded=false`。
- `valid_until_window_blocker_resolved=false`、`valid_until_window_blocker_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2413 禁止执行：

- 修复 signal validity dependency
- 实现 `valid_until_window`
- generate new feature
- generate dynamic strategy signal
- run scoring / backtest / daily report
- read fresh cached market data
- run candidate search
- approve observation
- enable paper-shadow, paper trade, shadow position
- append event or bind outcome
- create scheduler
- enable production or broker/order path
- resolve or downgrade `growth_tilt_engine`
- resolve or downgrade `valid_until_window`

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run status=`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`；input gap count=7，source traceability gap count=7，source traceability remediated count=2，remaining source traceability gap count=5，remaining blocked/gap count=7，contract-ready count=0；`equal_risk_baseline_weights` 和 `risk_on_trend_filter_context` 已输出 governed config source snapshot hash，`target_vol_policy`、`trend_features`、`volatility_inputs`、`drawdown_features` 和 `growth_tilt_engine_signal_artifact` 因缺少可确认 upstream key / source snapshot / standalone artifact 明确保留 blocked；as-of ready 状态未回退，validity dependency / PIT gate 未标 ready，`growth_tilt_engine` / `valid_until_window` blocker 均保持 unresolved / undowngraded。
