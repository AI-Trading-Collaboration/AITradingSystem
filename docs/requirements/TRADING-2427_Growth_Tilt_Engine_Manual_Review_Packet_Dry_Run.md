# TRADING-2427 Growth Tilt Engine Manual Review Packet Dry-Run

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2427_GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2426 paper-shadow schedule dry-run READY 后，验证 Growth Tilt Engine
manual review packet 是否可以在无副作用状态下生成。

本任务只生成 review packet dry-run artifact，不生成真实交易建议，不生成 actionable
allocation change，不启用 paper-shadow，不进入 production / broker。

## 输入

- TRADING-2426 schedule dry-run result
- TRADING-2426 schedule boundary checklist
- TRADING-2426 schedule no-effect audit summary
- TRADING-2425 dry-run wiring result
- TRADING-2425 manual review handoff wiring plan
- TRADING-2424 paper-shadow enablement plan result
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/manual_review_packet_dry_run_result.json`
- `outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/manual_review_packet.json`
- `outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/manual_review_checklist.json`
- `outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/no_advice_boundary_summary.json`
- `outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/reviewer_handoff_manifest.json`
- `docs/research/growth_tilt_engine_manual_review_packet_dry_run.md`
- `docs/research/growth_tilt_engine_manual_review_packet.md`
- `docs/research/growth_tilt_engine_manual_review_packet_checklist.md`
- `docs/research/growth_tilt_engine_manual_review_packet_no_advice_boundary_summary.md`
- `docs/research/growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest.md`
- `docs/research/dynamic_strategy_2428_route.md`

## CLI

```bash
aits research strategies growth-tilt-engine-manual-review-packet-dry-run --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_READY
```

READY payload 必须至少包含：

```yaml
manual_review_packet_dry_run_ready: true
manual_review_packet_gap_count: 0
manual_review_required: true
trading_advice_generated: false
actionable_allocation_generated: false
paper_shadow_enabled: false
paper_shadow_schedule_enabled: false
production_enabled: false
broker_enabled: false
next_route: TRADING-2428_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary
```

## 安全边界

本任务不得：

- 生成 trading advice
- 生成 actionable allocation change
- 生成 broker order
- 修改实际组合权重
- 启用 paper-shadow
- 启用 schedule 或 scheduler
- 运行 paper-shadow daily job
- 运行 production
- 触发 broker/order
- 读取 fresh cached market data
- 运行 backtest、scoring 或 daily report

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2427 只读取 prior artifacts/docs、
report registry、artifact catalog 和 system flow，不读取 fresh cached market data，
不运行 backtest/scoring/daily report，不生成 feature/signal 或交易建议。

如果实现阶段引入 fresh cached market/features/signals/event data 读取，本任务必须重新
引入 `aits validate-data` 或同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- manual review packet dry-run result、manual review packet、manual review checklist、
  no-advice boundary summary、reviewer handoff manifest 和 2428 route 均生成。
- TRADING-2426 READY 状态被读取并继承。
- packet 明确标记 dry-run / no-advice / manual-review-only。
- 不生成 trading advice、actionable allocation、broker order 或 portfolio mutation。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_manual_review_packet_dry_run.py
aits research strategies growth-tilt-engine-manual-review-packet-dry-run --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI run 输出
  `GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_READY`，
  manual_review_packet_dry_run_ready=true，manual_review_packet_gap_count=0，
  trading_advice_generated=false，actionable_allocation_generated=false，
  paper_shadow_enabled=false，paper_shadow_schedule_enabled=false，
  production_enabled=false，broker_enabled=false，next route 指向
  `TRADING-2428_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts/docs、registry、
  catalog 和 system flow，不读取 fresh cached market data、不运行 backtest/scoring/
  daily report、不生成 feature/signal 或交易建议。
