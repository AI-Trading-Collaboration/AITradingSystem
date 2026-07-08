# TRADING-2415 Growth Tilt Engine PIT Gate Readiness Snapshot

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source tasks：`TRADING-2410` / `TRADING-2411` / `TRADING-2412` / `TRADING-2413` / `TRADING-2414`
- blocker under review：`growth_tilt_engine`
- coupled blocker：`valid_until_window`
- next route：`TRADING-2416_Growth_Tilt_Engine_Remaining_Contract_Blocker_Remediation_Plan`

## 背景

TRADING-2410 到 TRADING-2414 已把 `growth_tilt_engine` 的 source feature contract、gap remediation plan、as-of semantics、source traceability 和 signal validity dependency 逐步拆解成可审计 artifacts。2414 仍保留 `growth_tilt_engine` 与 `valid_until_window` blocker 未解除且未降级，candidate search、observation、paper-shadow、production 和 broker 均保持 disabled。

TRADING-2415 的目标是只基于上述 prior artifacts 生成 PIT gate readiness snapshot：汇总所有已知 source features 在 as-of semantics、source traceability、validity dependency、valid-until availability 和 PIT gate 维度的当前状态，并给出 remaining blocker matrix。它不得修复 source feature、不得实现 `valid_until_window`、不得补造 PIT evidence、不得解锁候选搜索或任何交易路径。

## 输入

- TRADING-2410：
  - `mapping_result.json`
  - `source_feature_contract_mapping.json`
- TRADING-2411：
  - `remediation_plan_result.json`
  - `ordered_remediation_items.json`
  - `unresolved_blocker_summary.json`
- TRADING-2412：
  - `as_of_remediation_result.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
- TRADING-2413：
  - `source_traceability_remediation_result.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
- TRADING-2414：
  - `signal_validity_dependency_remediation_result.json`
  - `updated_source_feature_mapping.json`
  - `remaining_blocker_summary.json`
- Registry / catalog references：
  - `config/report_registry.yaml`
  - `docs/artifact_catalog.md`

## PIT Gate Status Taxonomy

每个 known source feature 必须归入以下状态之一：

- `pit_gate_ready`
- `pit_gate_blocked_by_missing_as_of_semantics`
- `pit_gate_blocked_by_missing_source_traceability`
- `pit_gate_blocked_by_missing_validity_dependency`
- `pit_gate_blocked_by_valid_until_window`
- `pit_gate_blocked_by_missing_upstream_artifact`
- `pit_gate_blocked_by_ambiguous_source_boundary`
- `pit_gate_not_applicable_non_signal_feature`
- `pit_gate_unresolved`

禁止出现未分类 PIT gate status。

## Readiness Matrix

2415 必须为所有 known source features 输出 matrix row，字段包括：

- `source_feature_id`
- `source_feature_name`
- `as_of_semantics_status`
- `source_traceability_status`
- `validity_dependency_status`
- `valid_until_required`
- `valid_until_available`
- `pit_gate_status`
- `pit_gate_blocking_reason`
- `contract_ready`
- `eligible_for_candidate_search`
- `eligible_for_observation`
- `eligible_for_paper_shadow`
- `eligible_for_production`

`contract_ready=true` 仅允许在以下条件同时满足时出现：

- `as_of_semantics_status=ready`
- `source_traceability_status=ready`
- `validity_dependency_status=ready`
- `pit_gate_status=pit_gate_ready`
- 如果 `valid_until_required=true`，则 `valid_until_available=true`

如果 `valid_until_required=true` 且 `valid_until_available=false`，必须输出 `pit_gate_status=pit_gate_blocked_by_valid_until_window`，且 `contract_ready=false`。

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2415 只生成 PIT gate readiness snapshot。
2. 新增 reusable PIT gate readiness snapshot helper，聚合 2410-2414 prior artifacts。
3. 新增任务级 wrapper，fail-closed 校验 prior artifact status、route、counts、safety fields、registry 和 catalog。
4. 新增 CLI `aits research strategies growth-tilt-engine-pit-gate-readiness-snapshot`。
5. 生成 machine-readable snapshot artifact、readiness matrix、remaining blocker summary 和 human-readable research docs。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED`。
- matrix 覆盖 TRADING-2410 的全部 known source features。
- as-of semantics、source traceability、validity dependency 和 valid-until status 只从 prior artifacts 聚合，不静默修复或降级 blocker。
- missing source traceability 必须阻断 PIT gate。
- `valid_until_required=true` 且 `valid_until_available=false` 必须阻断 PIT gate。
- `contract_ready_count` 不得增加，真实 run 仍为 0。
- `growth_tilt_engine_blocker_resolved=false`、`growth_tilt_engine_blocker_downgraded=false`。
- `valid_until_window_blocker_resolved=false`、`valid_until_window_blocker_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2416_Growth_Tilt_Engine_Remaining_Contract_Blocker_Remediation_Plan`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2415 禁止执行：

- implement `valid_until_window`
- modify source feature contracts to mark blockers fixed
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
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run status=`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED`；source feature count=10，as-of ready count=2，source traceability ready count=2，validity dependency ready count=2，PIT gate ready count=0，contract-ready count=0，PIT gate blocked count=10，blocked by source traceability count=5，blocked by valid_until_window count=1；`growth_tilt_engine` / `valid_until_window` blocker 均保持 unresolved / undowngraded，candidate search、observation、paper-shadow、production 和 broker 均保持 disabled / false / none。
- 2026-07-08：提交前验证通过 Ruff、compileall、focused parallel pytest 5 passed、真实 2415 CLI、docs freshness 603 docs PASS、documentation contract 1312 reports PASS、task-register consistency run active=319 / completed=477 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、active register terminal-status scan clean、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260708T020706Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning，退出码 0）。本任务未运行 `aits validate-data`，因为不读取 fresh cached market data、不生成 feature/signal/scoring/backtest/daily report。
