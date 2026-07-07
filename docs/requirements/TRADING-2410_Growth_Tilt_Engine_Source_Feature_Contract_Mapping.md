# TRADING-2410 Growth Tilt Engine Source Feature Contract Mapping

最后更新：2026-07-08

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-08
- source tasks：`TRADING-2405` / `TRADING-2406` / `TRADING-2409`
- blocker under review：`growth_tilt_engine`
- next route：`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`

## 背景

TRADING-2409 已实现 reusable signal as-of / source feature traceability / signal validity contract schema 与 validator。TRADING-2406 已为 `growth_tilt_engine` 生成 source feature inventory、PIT risk audit、signal construction gap analysis 和 remediation plan。当前 `growth_tilt_engine` 仍是 `BLOCKING`，不能恢复 candidate search 或进入 observation / paper-shadow / production。

TRADING-2410 的目标是把 `growth_tilt_engine` 当前依赖的 source features 映射到 2409 contract requirement，识别 as-of、source traceability、validity dependency 和 blocker gap。2410 不修复 growth tilt engine，不生成新信号，不运行 replay validation，不清除或降级 blocker。

## 输入

- TRADING-2409：
  - `contract_schema_result.json`
  - `source_feature_traceability_contract_schema.json`
  - `signal_as_of_contract_schema.json`
  - `signal_validity_contract_schema.json`
  - `contract_schema_snapshot.json`
- TRADING-2406：
  - `source_feature_inventory.json`
  - `pit_risk_audit.json`
  - `signal_construction_gap_analysis.json`
  - `remediation_plan_result.json`
- TRADING-2405：
  - `pit_gate_result.json`
  - `pit_blocker_summary.json`
- Config：
  - `config/research/dynamic_strategy_pit_input_registry.yaml`
  - `config/research/equal_risk_growth_tilt_candidate_registry.yaml`

## Mapping Status

每个已知 source feature 必须落入以下状态之一：

- `mapped_contract_ready`
- `mapped_with_caveats`
- `missing_as_of_semantics`
- `missing_source_traceability`
- `missing_validity_dependency`
- `ambiguous_source_feature`
- `excluded_non_signal_feature`
- `blocked_unresolved`

禁止出现未分类 feature。缺少 as-of / source traceability / validity 语义的 feature 不能标记为 `mapped_contract_ready`。

## 产物

- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/mapping_result.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/source_feature_contract_mapping.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/contract_mapping_validation.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/unresolved_gap_summary.json`
- `docs/research/growth_tilt_engine_source_feature_contract_mapping.md`
- `docs/research/growth_tilt_engine_contract_mapping_validation.md`
- `docs/research/dynamic_strategy_2411_route.md`

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2410 只做 source feature contract mapping。
2. 新增 reusable mapping helper，分类 2406 source feature inventory 并调用 2409 `source_feature_traceability_contract` validator。
3. 新增任务级 wrapper，fail-closed 读取 2409 / 2406 / 2405 artifacts 与 governed configs。
4. 新增 CLI `aits research strategies growth-tilt-engine-source-feature-contract-mapping`。
5. 生成 machine-readable mapping、validation result、unresolved gap summary、research docs 和 2411 route。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`。
- 所有已知 `growth_tilt_engine` source features 都被枚举。
- 每个 feature 都有明确 mapping status，且 status 属于 allowed set。
- `unclassified_feature_count=0`。
- 缺少 as-of / source traceability / validity 语义的 feature 不得标记为 `mapped_contract_ready`。
- `blockers_resolved=false`、`blockers_downgraded=false`、`growth_tilt_engine_blocking_gap_resolved=false`、`growth_tilt_engine_severity_downgraded=false`。
- `candidate_search_enabled=false`、`observation_enabled=false`、`paper_shadow_enabled=false`、`production_enabled=false`、`broker_enabled=false`。
- 下一任务固定为 `TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2410 禁止执行：

- 修复或解除 `growth_tilt_engine`
- downgrade blocker severity
- mark TRUE_PIT
- generate dynamic strategy signal
- run candidate search
- approve research-only observation
- enable paper-shadow, paper trade, shadow position
- append event or bind outcome
- create scheduler / daily report
- run new strategy backtest
- generate scoring output
- enable production or broker/order path

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI run 返回 status=`GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`，known source features=10，`unclassified_feature_count=0`，`contract_ready_count=0`，`blocked_or_gap_count=7`，next route=`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`。本任务未运行 `aits validate-data`，因为只读取 prior validated TRADING-2409 / 2406 / 2405 artifacts 与 governed growth tilt / PIT configs，不读取 fresh cached market data、不运行新 backtest、不生成 technical features、new signal、scoring、daily report 或交易建议；candidate search、observation、paper-shadow、scheduler、event append、outcome binding、production、broker/order 全部保持 disabled / false / none。
- 2026-07-08：最终验证通过 focused parallel pytest 9 passed、真实 CLI run、Ruff、compileall、docs freshness、documentation contract、task-register consistency run/validate、contract-validation 197 passed 和 `git diff --check`。
