# TRADING-2412 Growth Tilt Engine As-Of Semantics Remediation

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source task：`TRADING-2411`
- blocker under review：`growth_tilt_engine`
- next route：`TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation`

## 背景

TRADING-2411 已完成 `growth_tilt_engine` contract gap remediation plan，真实 status 为 `GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`，纳入 7 个 blocked/gap source features，并把下一步限定为 TRADING-2412 as-of semantics remediation。

TRADING-2412 的目标是处理 remediation plan 中 `as_of_semantics_required` 或 `missing_as_of_semantics=true` 的 source features。2412 只补齐 as-of metadata 与 no-lookahead contract，不修复完整 source traceability，不处理 signal validity dependency，不解除或降级 blocker。

## 输入

- TRADING-2411：
  - `remediation_plan_result.json`
  - `contract_gap_remediation_plan.json`
  - `ordered_remediation_items.json`
  - `validation_design.json`
  - `unresolved_blocker_summary.json`
  - `docs/research/growth_tilt_engine_contract_gap_remediation_plan.md`
- TRADING-2410 source feature mapping reference

## As-Of Remediation Status

每个被处理对象必须归入以下状态之一：

- `as_of_semantics_remediated`
- `as_of_semantics_partially_remediated`
- `as_of_semantics_blocked_by_missing_upstream_artifact`
- `as_of_semantics_blocked_by_ambiguous_source_boundary`
- `as_of_semantics_not_applicable_non_signal_feature`
- `as_of_semantics_unresolved`

禁止出现未分类状态。

## Contract Metadata

2412 需要为 as-of gap feature 输出：

- `as_of_date`
- `as_of_timestamp`
- `effective_date`
- `known_at`
- `source_observed_at`
- `feature_computed_at`
- `lookback_window`
- `lookahead_allowed=false`
- `pit_safe=true|false|unknown`
- `as_of_semantics_status`

PIT safety 不能凭空假设；若未具备完整 source traceability 或 source snapshot evidence，必须保持 `pit_safe=unknown` 或 blocked。即使 as-of semantics 已补齐，也不能自动标记 contract-ready。

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2412 只处理 as-of 维度。
2. 新增 reusable as-of remediation helper，识别 2411 中的 as-of gap items。
3. 生成 before / after remediation artifact、as-of contract metadata 和 updated source feature mapping。
4. 新增任务级 wrapper，fail-closed 读取 2411 artifacts/docs/registry/catalog references。
5. 新增 CLI `aits research strategies growth-tilt-engine-as-of-semantics-remediation`。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`。
- TRADING-2411 中所有 as-of semantics 相关 remediation items 都被处理或明确保留 blocked。
- 每个处理对象都有 before / after 状态。
- 每个处理对象都有确定的 as-of status。
- `lookahead_allowed=false` 被显式编码。
- PIT safety 不凭空假设；无法确认时保持 `pit_safe=unknown`。
- 未完成 source traceability / validity dependency 的 feature 不能被标记为 contract-ready。
- `contract_ready_count=0`。
- `growth_tilt_engine_blocker_resolved=false`、`growth_tilt_engine_blocker_downgraded=false`。
- `valid_until_window_blocker_resolved=false`、`valid_until_window_blocker_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2412 禁止执行：

- 修复完整 source traceability
- 修复完整 signal validity dependency
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
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run status=`GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`；input gap count=7，as-of gap count=2，as-of remediated count=2，remaining blocked/gap count=7，contract-ready count=0；`drawdown_features` 和 `volatility_inputs` 已输出 explicit as-of metadata、before/after mapping 与 `lookahead_allowed=false` no-lookahead contract；source traceability、validity dependency、PIT gate 和 `valid_until_window` 仍未修复，`growth_tilt_engine` / `valid_until_window` blocker 均保持 unresolved / undowngraded。
