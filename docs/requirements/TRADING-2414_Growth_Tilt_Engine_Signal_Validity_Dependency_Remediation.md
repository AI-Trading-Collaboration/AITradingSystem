# TRADING-2414 Growth Tilt Engine Signal Validity Dependency Remediation

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source tasks：`TRADING-2411` / `TRADING-2412` / `TRADING-2413`
- blocker under review：`growth_tilt_engine`
- coupled blocker：`valid_until_window`
- next route：`TRADING-2415_Growth_Tilt_Engine_PIT_Gate_Readiness_Snapshot`

## 背景

TRADING-2411 已把 TRADING-2410 暴露的 7 个 blocked/gap features 转成 remediation plan，其中 `execution_signal_validity_policy` 属于 `validity_dependency_required`。

TRADING-2412 已补齐 `volatility_inputs` 与 `drawdown_features` 的 as-of semantics，但未补 source traceability、signal validity dependency、PIT gate 或 `valid_until_window`。

TRADING-2413 已补 source traceability 维度。真实 status 为 `GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`，input gap count=7，source traceability gap count=7，source traceability remediated count=2，remaining source traceability gap count=5，remaining blocked/gap count=7，contract-ready count=0，下一步 route 为 TRADING-2414。

TRADING-2414 的目标是只处理 signal validity dependency metadata、blocked reason、before/after mapping 和 validation design。它不得实现 `valid_until_window`，不得解除或降级任何 blocker，也不得恢复 candidate search、observation、paper-shadow、production 或 broker。

## 输入

- TRADING-2411：
  - `remediation_plan_result.json`
  - `ordered_remediation_items.json`
  - `unresolved_blocker_summary.json`
- TRADING-2412：
  - `as_of_remediation_result.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
  - `docs/research/growth_tilt_engine_as_of_semantics_remediation.md`
- TRADING-2413：
  - `source_traceability_remediation_result.json`
  - `source_traceability_contract_metadata.json`
  - `before_after_source_traceability_remediation.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
  - `docs/research/growth_tilt_engine_source_traceability_remediation.md`
- Registry / catalog references：
  - `config/report_registry.yaml`
  - `docs/artifact_catalog.md`

## Validity Dependency Remediation Status

每个被处理对象必须归入以下状态之一：

- `validity_dependency_remediated`
- `validity_dependency_partially_remediated`
- `validity_dependency_blocked_by_valid_until_window`
- `validity_dependency_blocked_by_missing_source_traceability`
- `validity_dependency_blocked_by_missing_upstream_artifact`
- `validity_dependency_blocked_by_ambiguous_signal_boundary`
- `validity_dependency_not_applicable_non_signal_feature`
- `validity_dependency_unresolved`

禁止出现未分类状态。

## Contract Metadata

2414 需要输出以下 signal validity dependency metadata 字段：

- `source_feature_id`
- `derived_signal_id`
- `validity_dependency_id`
- `validity_basis`
- `validity_window_required`
- `valid_until_required`
- `valid_until_available`
- `validity_start_reference`
- `validity_end_reference`
- `staleness_policy`
- `expiration_policy`
- `recompute_required_on_expiry`
- `validity_dependency_status`
- `validity_blocking_reason`

如果 `valid_until_required=true` 且 `valid_until_available=false`，必须保持 `validity_dependency_status=blocked`、`validity_blocking_reason=valid_until_window_unresolved`、`contract_ready=false`。

如果 source traceability 仍未 ready，则 signal validity dependency 不得被标记为 ready，必须保持 blocked，并说明缺失 source snapshot / upstream artifact / standalone signal artifact。

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2414 只处理 signal validity dependency 维度。
2. 新增 reusable signal validity dependency remediation helper，读取 2411 / 2412 / 2413 的 mapping 与 remediation records。
3. 为 validity dependency gap rows 生成 contract metadata、before/after remediation artifact 和 updated source feature mapping。
4. 新增任务级 wrapper，fail-closed 校验 prior artifact status、route、gap count、remaining blocker summary、registry 和 catalog。
5. 新增 CLI `aits research strategies growth-tilt-engine-signal-validity-dependency-remediation`。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`。
- 2411 中 `validity_dependency_required` item、2413 中 validity 未评估且仍属于 growth tilt gap 的 source features 都被处理或明确保留 blocked。
- 每个处理对象都有 before / after 状态。
- 每个处理对象都有确定的 allowed validity dependency remediation status。
- 已有 as-of ready 状态不得回退。
- 已有 source traceability ready 状态不得回退。
- source traceability 未完成的 feature 不能被标记为 validity-ready。
- `valid_until_window` 未完成时，`valid_until_required=true` 的 feature 不能被标记为 validity-ready。
- 未完成 PIT gate 的 feature 不能被标记为 contract-ready。
- `contract_ready_count=0`。
- `growth_tilt_engine_blocker_resolved=false`、`growth_tilt_engine_blocker_downgraded=false`。
- `valid_until_window_blocker_resolved=false`、`valid_until_window_blocker_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2415_Growth_Tilt_Engine_PIT_Gate_Readiness_Snapshot`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2414 禁止执行：

- implement `valid_until_window`
- modify `growth_tilt_engine` scoring logic
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
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run status=`GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`；input gap count=7，validity dependency gap count=8，validity dependency remediated count=2，blocked by valid_until_window count=1，blocked by source traceability count=5，remaining blocked/gap count=7，contract-ready count=0；`equal_risk_baseline_weights` 和 `risk_on_trend_filter_context` 的 signal validity dependency metadata 已补齐为 ready，但因 as-of / PIT gate 等维度仍未 ready，contract-ready 仍为 false；`execution_signal_validity_policy` 因 `valid_until_window_unresolved` 保持 blocked；`target_vol_policy`、`trend_features`、`volatility_inputs`、`drawdown_features`、`growth_tilt_engine_signal_artifact` 因 source traceability / source snapshot 未完成保持 blocked；`growth_tilt_engine` / `valid_until_window` blocker 均保持 unresolved / undowngraded。
- 2026-07-08：提交前验证通过 focused parallel pytest 8 passed、Ruff、compileall、真实 2414 CLI、docs freshness 601 docs PASS、documentation contract 1311 reports PASS、task-register consistency run active=319 / completed=475 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、active register terminal-status scan clean、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260707T171506Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning，退出码 0）。本任务未运行 `aits validate-data`，因为不读取 fresh cached market data、不生成 feature/signal/scoring/backtest/daily report。
