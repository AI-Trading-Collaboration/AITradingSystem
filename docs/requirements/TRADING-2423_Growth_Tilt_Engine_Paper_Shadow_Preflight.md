# TRADING-2423 Growth Tilt Engine Paper Shadow Preflight

最后更新：2026-07-09

## 状态

- 任务登记：`TRADING-2423_GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT`
- 当前状态：`DONE`
- 优先级：`P0`
- owner：系统实现 + 项目 owner 后续复核
- 日期：2026-07-09

## 背景

TRADING-2422 已完成 Growth Tilt Engine contract readiness snapshot，真实 CLI
status 为 `GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY`。
当前 `pit_gate_ready=true`、`contract_ready=true`、contract gap count=0，且 next route
指向 `TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight`。

TRADING-2423 的目标是执行 paper-shadow 启动前的 preflight 安全边界检查。本任务可以记录
`paper_shadow_preflight_started=true` 和 `paper_shadow_preflight_ready` 的真实检查结果，但不启用
paper-shadow、不进入 schedule、不生成新 signal、不运行 production 或 broker。

## 范围

允许：

- 读取 TRADING-2422 contract readiness snapshot artifacts。
- 读取 TRADING-2421 after-remediation PIT gate readiness artifacts。
- 读取 TRADING-2420 source traceability remediation artifacts。
- 读取 report registry、artifact catalog、system flow 和相关 research docs。
- 生成 paper-shadow preflight artifact、preflight checklist、preflight gap summary 和 TRADING-2424 route。

禁止：

- 不启用 paper-shadow。
- 不运行 paper-shadow schedule。
- 不生成新 signal、feature 或交易建议。
- 不运行 backtest、scoring 或 daily report。
- 不读取 fresh cached market data。
- 不修改实际组合权重或 target weights。
- 不启用 production 或 broker/order path。
- 不跳过人工 review 边界。

## Preflight 检查项

- `pit_gate_ready` 必须为 true。
- `contract_ready` 必须为 true。
- `remaining_pit_blockers` 必须为空。
- `contract_gap_count` 必须为 0。
- source traceability recheck status 必须为 `ACCEPTED`。
- paper-shadow 必须尚未启用。
- production 必须 disabled。
- broker 必须 disabled。
- manual review boundary 必须存在且为 true。
- preflight task 可以将 `paper_shadow_preflight_started` 记录为 true。
- generated signal / generated trading advice / backtest / scoring / daily report 必须为 false。

## 输出

- `outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/paper_shadow_preflight_result.json`
- `outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/preflight_checklist.json`
- `outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/preflight_gap_summary.json`
- `docs/research/growth_tilt_engine_paper_shadow_preflight.md`
- `docs/research/growth_tilt_engine_paper_shadow_preflight_checklist.md`
- `docs/research/growth_tilt_engine_paper_shadow_preflight_gap_summary.md`
- `docs/research/dynamic_strategy_2424_route.md`

## 验收标准

- CLI `aits research strategies growth-tilt-engine-paper-shadow-preflight --as-of 2026-07-08`
  可真实运行。
- 如果 PIT gate ready、contract ready、remaining blockers 为空、contract gap count=0、
  source traceability accepted、manual review boundary present，且 paper-shadow / production / broker 均
  disabled，则 status 为 `GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_READY`。
- READY 时 `paper_shadow_preflight_started=true`、`paper_shadow_preflight_ready=true`、
  `preflight_gap_count=0`。
- 如果任一 preflight requirement 失败，则 status 为
  `GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_BLOCKED_BY_PREFLIGHT_GAPS`，并明确列出 gap。
- READY next route 为 `TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan`。
- BLOCKED next route 为 `TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Preflight_Gap_Remediation`。
- paper-shadow、production、broker、scheduler、event append、outcome binding、daily report 全部
  false / none。
- report registry、artifact catalog、system flow、task register 和 focused tests 一致。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_paper_shadow_preflight.py`
- `aits research strategies growth-tilt-engine-paper-shadow-preflight --as-of 2026-07-08`
- `aits docs validate-freshness`
- `aits docs report-contract --latest`
- `aits reports task-register-consistency run`
- `aits reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

`aits validate-data` 不在默认验证中运行，因为本任务只读取 prior artifacts、registry、catalog、
system flow 和 docs，不读取 fresh cached market/macro/features/signals，不运行 backtest/scoring/daily report。

## 进展记录

- 2026-07-09：根据 owner 附件新增并进入 `IN_PROGRESS`。实现范围固定为 paper-shadow
  preflight；不生成新 signal、不运行 backtest/scoring、不启用 paper-shadow / production / broker，
  paper-shadow enablement plan 留给 TRADING-2424。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI
  `aits research strategies growth-tilt-engine-paper-shadow-preflight --as-of 2026-07-08`
  输出 `GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_READY`；`paper_shadow_preflight_started=true`、
  `paper_shadow_preflight_ready=true`、`preflight_gap_count=0`，next route 为
  `TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan`。paper-shadow runtime /
  schedule 未启用，production / broker 仍全部 disabled。
