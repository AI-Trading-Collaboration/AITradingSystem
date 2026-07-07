# TRADING-2411 Growth Tilt Engine Contract Gap Remediation Plan

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source tasks：`TRADING-2410` / `TRADING-2409` / `TRADING-2406`
- blocker under review：`growth_tilt_engine`
- next route：`TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`

## 背景

TRADING-2410 已完成 `growth_tilt_engine` source feature contract mapping。真实 mapping status 为 `GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`，known source features=10，`unclassified_feature_count=0`，`contract_ready_count=0`，`blocked_or_gap_count=7`。当前 `growth_tilt_engine` 和相关 `valid_until_window` blocker 仍保持 unresolved / undowngraded。

TRADING-2411 的目标是把 2410 暴露的 blocked/gap features 转成可执行 remediation plan、实施顺序和 validation design。本任务不执行具体 remediation，不修改 growth tilt engine 评分逻辑，不生成新 feature / signal / scoring / backtest / daily report，不恢复 candidate search，不进入 observation / paper-shadow / production / broker。

## 输入

- TRADING-2410：
  - `mapping_result.json`
  - `source_feature_contract_mapping.json`
  - `contract_mapping_validation.json`
  - `unresolved_gap_summary.json`
  - `docs/research/growth_tilt_engine_source_feature_contract_mapping.md`
  - report registry / artifact catalog entries
- TRADING-2409 reusable contract schema reference
- TRADING-2406 source feature inventory reference

## Remediation Categories

每个 remediation item 必须归入以下类别之一：

- `as_of_semantics_required`
- `source_traceability_required`
- `validity_dependency_required`
- `pit_gate_requirement_required`
- `upstream_artifact_reference_required`
- `ambiguous_feature_boundary_requires_owner_review`
- `non_signal_feature_exclusion_required`
- `blocked_pending_prior_remediation`

禁止出现未分类 remediation item。

## 排序策略

排序必须稳定、可复现，默认类别优先级如下：

1. `source_traceability_required`
2. `as_of_semantics_required`
3. `validity_dependency_required`
4. `pit_gate_requirement_required`
5. `ambiguous_feature_boundary_requires_owner_review`
6. `upstream_artifact_reference_required`
7. `non_signal_feature_exclusion_required`
8. `blocked_pending_prior_remediation`

同一类别内按 feature id 字典序排序。每个 item 必须输出：

- `remediation_order`
- `blocks_contract_ready`
- `blocks_pit_gate`
- `requires_owner_review`
- `can_be_implemented_without_fresh_market_data`

## 产物

- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/remediation_plan_result.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/contract_gap_remediation_plan.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/ordered_remediation_items.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/validation_design.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/unresolved_blocker_summary.json`
- `docs/research/growth_tilt_engine_contract_gap_remediation_plan.md`
- `docs/research/growth_tilt_engine_contract_gap_validation_design.md`
- `docs/research/dynamic_strategy_2412_route.md`

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2411 只做 remediation plan/design。
2. 新增 reusable remediation plan helper，把 2410 blocked/gap mapping rows 转成 remediation items。
3. 新增任务级 wrapper，fail-closed 读取 2410 artifacts/docs/registry/catalog references。
4. 新增 CLI `aits research strategies growth-tilt-engine-contract-gap-remediation-plan`。
5. 生成 machine-readable remediation plan、ordered items、validation design、unresolved blocker summary、research docs 和 2412 route。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`。
- TRADING-2410 中所有 blocked/gap features 都被纳入 remediation plan。
- gap count 与 2410 `blocked_or_gap_count` 一致，除非文档明确解释差异。
- 每个 remediation item 都有明确类别、执行动作、依赖、验证方式。
- `unclassified_remediation_item_count=0`。
- 没有 gap 被静默标记为 resolved。
- `growth_tilt_engine_blocker_resolved=false`、`growth_tilt_engine_blocker_downgraded=false`。
- `valid_until_window_blocker_resolved=false`、`valid_until_window_blocker_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2411 禁止执行：

- implement remediation
- 修改 `growth_tilt_engine` 评分逻辑
- generate new feature
- generate dynamic strategy signal
- run candidate search
- approve observation
- enable paper-shadow, paper trade, shadow position
- append event or bind outcome
- create scheduler / daily report
- run new strategy backtest
- generate scoring output
- enable production or broker/order path
- resolve or downgrade `growth_tilt_engine`
- resolve or downgrade `valid_until_window`

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run status=`GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`，gap count=7，`unclassified_remediation_item_count=0`，`silent_gap_resolution_count=0`，`silent_blocker_downgrade_count=0`；下一步 route=`TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`。本任务未运行 `aits validate-data`，因为只读取 TRADING-2410 prior artifacts/docs/registry/catalog，不读取 fresh cached market data、不生成 feature/signal/scoring/backtest/daily report 或交易建议。
