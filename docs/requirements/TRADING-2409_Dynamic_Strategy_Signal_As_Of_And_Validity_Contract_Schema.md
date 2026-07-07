# TRADING-2409 Dynamic Strategy Signal As-Of And Validity Contract Schema

最后更新：2026-07-07

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-07
- source tasks：`TRADING-2405` / `TRADING-2406` / `TRADING-2407` / `TRADING-2408`
- blocking gaps：`growth_tilt_engine` / `valid_until_window`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

## 背景

TRADING-2408 已生成 blocking gap remediation implementation plan，并把下一步固定为统一 signal as-of / validity contract schema。当前 `growth_tilt_engine` 和 `valid_until_window` 仍是 `BLOCKING`，candidate search、research-only observation、paper-shadow、production 和 broker path 均保持 blocked。

TRADING-2409 的目标是先实现可复用 contract schema 与基础 validator，供后续 TRADING-2410 / 2411 的 mapping、TRADING-2412 replay validation 和 TRADING-2413 owner downgrade review 使用。本任务不绑定真实策略信号，不执行 replay，不清除 blocker，不降级 severity。

## 输入

- TRADING-2408：
  - `implementation_plan_result.json`
  - `contract_schema_plan.json`
  - `candidate_search_gate_policy.json`
- TRADING-2405：
  - `pit_input_registry_snapshot.json`
  - `pit_gate_result.json`
  - `pit_blocker_summary.json`
- Config：
  - `config/research/dynamic_strategy_pit_input_registry.yaml`

## 产物

- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/contract_schema_result.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/signal_as_of_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/source_feature_traceability_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/signal_validity_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/contract_schema_snapshot.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/pit_gate_integration_plan.json`
- `outputs/research_quality/signal_contracts/signal_as_of_contract_schema.json`
- `outputs/research_quality/signal_contracts/source_feature_traceability_contract_schema.json`
- `outputs/research_quality/signal_contracts/signal_validity_contract_schema.json`
- `outputs/research_quality/signal_contracts/contract_schema_snapshot.json`
- `docs/research/dynamic_strategy_signal_as_of_validity_contract_schema.md`
- `docs/research/dynamic_strategy_signal_as_of_contract_schema.md`
- `docs/research/dynamic_strategy_source_feature_traceability_contract_schema.md`
- `docs/research/dynamic_strategy_signal_validity_contract_schema.md`
- `docs/research/dynamic_strategy_2410_route.md`

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2409 只做 reusable contract schema implementation。
2. 新增 `research_quality` reusable modules：
   - `signal_as_of_contract.py`
   - `source_feature_traceability_contract.py`
   - `signal_validity_contract.py`
   - `signal_contract_schema_snapshot.py`
3. 新增任务级 wrapper `dynamic_strategy_signal_as_of_validity_contract_schema.py`，fail-closed 读取 2408 / 2405 artifacts 与 PIT registry。
4. 新增 CLI `aits research strategies dynamic-strategy-signal-as-of-validity-contract-schema`。
5. 生成 strategy-level JSON / Markdown 与 reusable quality artifacts。
6. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
7. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`。
- `signal_as_of_contract_schema_ready=true`、`source_feature_traceability_contract_schema_ready=true`、`signal_validity_contract_schema_ready=true`。
- `schema_validation_helpers_ready=true`、`contract_schema_snapshot_ready=true`、`pit_gate_integration_plan_ready=true`。
- 三类 contract schema 包含 owner 附件要求的 required fields、enum values 和 invariants。
- 基础 validator 能返回 `valid`、`schema_name`、`error_count`、`warning_count`、`errors`、`warnings`，并覆盖 required field、enum、date order、horizon、PIT confidence、forward window 与 carry-forward / valid-until 错误。
- `growth_tilt_engine_blocking_gap_resolved=false`、`valid_until_window_blocking_gap_resolved=false`、`any_blocker_severity_downgraded=false`。
- `candidate_search_allowed=false`、`candidate_search_resumed=false`、`research_only_observation_allowed=false`、`research_only_observation_approved=false`、`paper_shadow_allowed=false`、`paper_shadow_enabled=false`、`event_append_enabled=false`、`outcome_binding_enabled=false`、`scheduler_enabled=false`、`production_enabled=false`、`broker_action_enabled=false`、`daily_report_generated=false`。
- 下一任务固定为 `TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2409 禁止执行：

- clear / downgrade `growth_tilt_engine` 或 `valid_until_window`
- mark TRUE_PIT
- resume candidate search
- approve research-only observation
- enable paper-shadow, paper trade, shadow position
- append event or bind outcome
- create scheduler / daily report
- run new strategy backtest
- generate new trading signal or scoring output
- enable production or broker/order path

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-07：实现完成并归档 `DONE`。新增 reusable contract schema modules、schema validators、contract snapshot、PIT gate integration plan、任务级 wrapper / CLI、strategy-level artifacts、research-quality artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 CLI run 返回 status=`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`，`route_to_next_task=TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`，两个 blocker 均保持 unresolved / undowngraded，所有 safety fields 保持 false / none。
- 2026-07-07：未运行 `aits validate-data --as-of 2026-07-05`，因为本任务仅读取 prior validated TRADING-2408 / 2405 artifacts 与 governed PIT input registry，不读取 fresh cached market data、不运行新 backtest、不生成 technical features、trading signal、scoring、daily report 或交易建议。
