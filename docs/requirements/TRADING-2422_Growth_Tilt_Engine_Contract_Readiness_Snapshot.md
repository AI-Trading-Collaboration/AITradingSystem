# TRADING-2422 Growth Tilt Engine Contract Readiness Snapshot

最后更新：2026-07-09

## 状态

- 任务登记：`TRADING-2422_GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统实现 + 项目 owner 后续复核
- 日期：2026-07-09

## 背景

TRADING-2421 已完成 Growth Tilt Engine PIT gate readiness recheck after source
traceability remediation，真实 CLI status 为
`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_REMEDIATION_READY`。
当前 `pit_gate_ready=true`、`pit_gate_ready_count=1`、remaining blockers 为空，但
`contract_ready=false`，并明确 route 到 TRADING-2422 独立快照。

TRADING-2422 的目标是判断 Growth Tilt Engine 是否满足进入 paper-shadow preflight 前的
contract readiness 要求。即使 contract readiness 为 READY，本任务也不启用 paper-shadow、
production 或 broker。

## 范围

允许：

- 读取 TRADING-2421 after-remediation PIT gate readiness artifact。
- 读取 TRADING-2420 source traceability remediation artifacts。
- 读取 report registry、artifact catalog、system flow 和相关 research docs。
- 生成 contract readiness snapshot、contract evidence map、contract gap summary 和
  TRADING-2423 route。

禁止：

- 不重新修复 source traceability。
- 不重新开启 PIT gate remediation。
- 不运行 backtest、scoring 或 daily report。
- 不生成新 signal、feature 或交易建议。
- 不读取 fresh cached market data。
- 不启用 candidate search、research-only observation、paper-shadow、scheduler、event append、
  outcome binding、production 或 broker/order path。
- 不跳过 paper-shadow preflight。

## Contract Readiness 检查项

- `pit_gate_ready` 必须为 true。
- `remaining_pit_blockers` 必须为空。
- TRADING-2420 source traceability remediation status 必须为 READY。
- report registry 必须登记 2420、2421、2422 相关报告。
- artifact catalog 必须登记 2422 command 和关键 artifacts。
- system flow 必须包含 2422 route。
- research docs 必须存在并可读取。
- CLI 输出必须 deterministic。
- paper-shadow / production / broker 必须保持 disabled。
- manual review boundary 必须保持 true。

## 输出

- `outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/contract_readiness_snapshot_result.json`
- `outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/contract_evidence_map.json`
- `outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/contract_gap_summary.json`
- `outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/contract_requirements.json`
- `docs/research/growth_tilt_engine_contract_readiness_snapshot.md`
- `docs/research/growth_tilt_engine_contract_evidence_map.md`
- `docs/research/growth_tilt_engine_contract_gap_summary.md`
- `docs/research/dynamic_strategy_2423_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-contract-readiness-snapshot --as-of 2026-07-08`
  可真实运行。
- 如果 PIT gate ready、remaining blockers 为空、source traceability remediation READY、
  registry/catalog/docs/system flow 对齐，且安全边界全部 disabled，则 status 为
  `GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY`。
- READY 时 `contract_ready=true`、`contract_ready_count=1`、`contract_gap_count=0`、
  `missing_contract_evidence_count=0`、`incomplete_contract_field_count=0`。
- 如果任一 contract requirement 失败，则 status 为
  `GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_BLOCKED_BY_CONTRACT_GAPS`，并明确列出 gap。
- paper-shadow、production、broker、scheduler、event append、outcome binding、daily report 全部
  false / none。
- READY next route 为 `TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight`。
- BLOCKED next route 为 `TRADING-2423_Growth_Tilt_Engine_Contract_Gap_Remediation`。
- report registry、artifact catalog、system flow、task register 和 focused tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_contract_readiness_snapshot.py`
- `aits research strategies growth-tilt-engine-contract-readiness-snapshot --as-of 2026-07-08`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data` 不在默认验证中运行，因为本任务只读取 prior artifacts、registry、catalog、
system flow 和 docs，不读取 fresh cached market/macro/features/signals，不运行 backtest/scoring/daily report。

## 进展记录

- 2026-07-09：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为
  contract readiness snapshot；不生成新 signal、不运行 backtest/scoring、不启用 paper-shadow /
  production / broker，paper-shadow preflight 留给 TRADING-2423。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI
  `aits research strategies growth-tilt-engine-contract-readiness-snapshot --as-of 2026-07-08`
  输出 `GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY`；`contract_ready=true`、
  `contract_ready_count=1`、`contract_gap_count=0`、`missing_contract_evidence_count=0`、
  `incomplete_contract_field_count=0`，next route 为
  `TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight`。paper-shadow preflight 未启动，
  paper-shadow / production / broker 仍全部 disabled。
