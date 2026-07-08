# TRADING-2420 Growth Tilt Engine Signal Artifact Source Traceability Remediation

最后更新：2026-07-08

## 状态

- 任务登记：`TRADING-2420_GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统验证完成；项目 owner 后续复核 TRADING-2421 PIT gate readiness recheck
- 日期：2026-07-08

## 背景

TRADING-2419 已完成 Growth Tilt Engine PIT gate readiness recheck，真实状态为
`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY`。
当前唯一 remaining blocker 为 `growth_tilt_engine_signal_artifact`，classification 为
`source_traceability`。

TRADING-2420 的目标是为 `growth_tilt_engine_signal_artifact` 建立 standalone、可审计、
可复核的 source traceability manifest / lineage map / missing evidence summary。即使 2420
remediation 成功，本任务也不直接标记 PIT gate ready 或 contract ready，后续必须由
TRADING-2421 独立 recheck。

## 范围

允许：

- 读取 TRADING-2419 readiness recheck artifact / blocker classification / research docs。
- 读取 TRADING-2418 valid-until dependency evidence、signal validity contract evidence、
  stale signal policy evidence 和 growth tilt valid-until alignment evidence。
- 读取 TRADING-2417 source traceability / upstream artifact closure evidence。
- 读取 report registry、artifact catalog 和相关 research docs。
- 生成 `growth_tilt_engine_signal_artifact` 的 source traceability manifest、source lineage map、
  missing evidence summary、remediation result 和 TRADING-2421 route。

禁止：

- 不生成新交易信号或交易建议。
- 不运行 backtest、scoring 或 daily report。
- 不读取 fresh cached market data。
- 不启用 candidate search、research-only observation、paper-shadow、scheduler、event append、
  outcome binding、production 或 broker/order path。
- 不直接把 PIT gate 标记为 ready。
- 不在证据链不完整时伪造 READY。

## 输出

- `outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/remediation_result.json`
- `outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/source_traceability_manifest.json`
- `outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/source_lineage_map.json`
- `outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/missing_source_evidence_summary.json`
- `docs/research/growth_tilt_engine_signal_artifact_source_traceability_remediation.md`
- `docs/research/growth_tilt_engine_signal_artifact_source_traceability_manifest.md`
- `docs/research/growth_tilt_engine_signal_artifact_source_lineage_map.md`
- `docs/research/dynamic_strategy_2421_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-signal-artifact-source-traceability-remediation`
  可真实运行。
- source traceability manifest 明确 artifact id、source artifact list、source documents、
  source registry entries、source generation commands、as-of boundary、valid-until boundary 和
  dependency closure reference。
- 如果 source evidence 完整，status 为
  `GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY`。
- 如果 required source evidence 缺失，status 为
  `GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_BLOCKED_BY_MISSING_EVIDENCE`。
- 不直接输出 PIT gate ready 或 contract ready；`pit_gate_ready=false`、`contract_ready=false`。
- paper-shadow、production、broker、scheduler、event append、outcome binding、daily report 全部
  false / none。
- successful remediation next route 为
  `TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_Source_Traceability_Remediation`。
- report registry、artifact catalog、system flow、task register 和 focused tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_signal_artifact_source_traceability_remediation.py`
- `aits research strategies growth-tilt-engine-signal-artifact-source-traceability-remediation --as-of 2026-07-08`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data` 不在默认验证中运行，因为本任务只读取 prior artifacts、registry、catalog
和 docs，不读取 fresh cached market/macro/features/signals，不运行 backtest/scoring/daily report。

## 进展记录

- 2026-07-08：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为 signal artifact
  source traceability remediation，不生成新 signal、不直接标记 PIT gate ready、不启用任何交易或观察路径。
- 2026-07-08：实现完成并归档 `DONE`。真实 CLI status=
  `GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY`；
  `growth_tilt_engine_signal_artifact` source traceability evidence chain complete，missing /
  incomplete / unresolved counts 均为 0，next route=
  `TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_Source_Traceability_Remediation`。
  本任务仍保持 `pit_gate_ready=false`、`contract_ready=false`，不生成新 signal、不运行
  backtest/scoring/daily report，不启用 paper-shadow / production / broker。未运行
  `aits validate-data`，因为只读取 prior artifacts、registry、catalog 和 docs，不读取 fresh
  cached market data。
