# TRADING-2418 Valid Until Window Dependency Evidence Closure

最后更新：2026-07-08

## 结论

- task register id：`TRADING-2418_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE`
- status：`DONE`
- CLI status：`GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`
- next route：`TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck`
- production effect：`none`
- broker action：`none`

TRADING-2418 将 TRADING-2416 / 2417 暴露的唯一 `valid_until_window`
dependency blocker 转成 TRADING-2419 PIT readiness recheck 可读取的证据。真实 CLI run
保留 2417 / 2416 / 2415 结论：`source_feature_count=10`、
`pit_gate_ready_count=0`、`contract_ready_count=0`、`pit_gate_blocked_count=10`、
`blocked_by_source_traceability_count=5`、`blocked_by_valid_until_window_count=1`。

本任务没有标记任何 source feature 为 PIT gate ready 或 contract ready，没有解除或降级
`growth_tilt_engine` / `valid_until_window` blocker，没有恢复 candidate search，没有批准
observation / paper-shadow / execution，没有运行新策略回测或生成新 trading signal。

## 输出

- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/closure_result.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/signal_validity_contract_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/stale_signal_policy_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/growth_tilt_valid_until_alignment_evidence.json`
- `outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_valid_until_dependency_evidence_closure.md`
- `docs/research/growth_tilt_engine_signal_validity_contract_evidence.md`
- `docs/research/growth_tilt_engine_stale_signal_policy_evidence.md`
- `docs/research/growth_tilt_engine_valid_until_alignment_evidence.md`
- `docs/research/dynamic_strategy_2419_route.md`

## 关键结果

- valid-until dependency feature：`execution_signal_validity_policy`
- valid-until dependency evidence rows：`1`
- pre-recheck evidence ready count：`1`
- still blocked valid-until dependency count：`0`
- source traceability still blocked feature：`growth_tilt_engine_signal_artifact`
- valid-until dependency evidence ready：`true`
- signal validity contract evidence ready：`true`
- stale signal policy evidence ready：`true`
- growth tilt valid-until alignment evidence ready：`true`
- remaining blocker summary ready：`true`
- PIT gate recheck required：`true`
- auto mark PIT gate ready：`false`
- auto mark contract ready：`false`
- growth tilt engine blocking gap resolved：`false`
- growth tilt engine severity downgraded：`false`
- valid until window blocking gap resolved：`false`
- valid until window severity downgraded：`false`
- candidate search allowed/resumed：`false` / `false`
- research-only observation allowed/approved：`false` / `false`
- paper shadow enabled：`false`
- event append enabled：`false`
- outcome binding enabled：`false`
- scheduler enabled：`false`
- production enabled：`false`
- broker action enabled：`false`
- daily report generated：`false`

## Data Quality Boundary

未运行 `aits validate-data`。原因：本任务只读取 TRADING-2407 / 2411 / 2414 /
2415 / 2416 / 2417 prior artifacts、PIT input registry、strategy execution policy
registry、report registry 和 artifact catalog；不读取 fresh cached market/macro/features/signals/event
data，不运行 backtest/scoring/daily report，也不生成交易建议。

## 验证

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_engine_valid_until_dependency_evidence_closure.py`：PASS，4 passed
- `aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure --as-of 2026-07-08`：PASS，status=`GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`
- `aits docs validate-freshness`：PASS，606 docs，0 issues
- `aits docs report-contract --latest`：PASS，1315 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，13 checks，0 failed
- `aits reports task-register-consistency validate --latest`：PASS，5 checks，0 failed，0 warnings
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260708T051124Z/test_runtime_summary.json`
- `git diff --check`：PASS
- active-row scan：PASS，`docs/task_register.md` 无 DONE / BASELINE_DONE / DROPPED active row

## 后续

下一步路线为 `TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck`。2419
应重新读取 2417 source traceability evidence 和 2418 valid-until dependency evidence，
但不得把 2418 结果自动解释为 PIT gate ready、contract ready 或 blocker downgrade。
