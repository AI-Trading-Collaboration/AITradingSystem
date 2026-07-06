# TRADING-2389 Dynamic Strategy Calibrated Gate Owner Review And Next Decision

最后更新：2026-07-06

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2389_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION`
- CLI：`aits research strategies dynamic-strategy-calibrated-gate-owner-review-decision`
- 真实 run status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- Owner decision：`ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL`
- 下一路由：`TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution`

## 完成内容

- 新增 calibrated gate owner review decision builder。
- 读取 TRADING-2386 / 2387 / 2388 prior validated artifacts。
- 输出 owner review decision、calibrated gate adoption record、non-approval record 和 TRADING-2390 route。
- 更新 report registry、artifact catalog、system flow、task register、requirements doc 和 focused tests。
- 明确本任务是 owner decision / adoption / non-approval record，不是 observation approval、paper-shadow、scheduler、event/outcome、daily report、production 或 broker readiness。

## 关键结论

- Owner 采纳 TRADING-2388 calibrated research-only gate methodology。
- Research-only observation gate 与 paper-shadow gate 必须保持分层。
- Reference candidate policy 采纳为 `BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`。
- 当前不批准 candidate auto-accept，也不批准 current best candidate research-only observation。
- 允许后续进行 calibrated reclassification preview，但必须进入 TRADING-2390 component attribution review。
- 后续仍需要 statistical threshold calibration。

## 验证

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_owner_review_decision.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：575 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1286 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=449 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T020854Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning

## 数据质量门禁说明

本任务未运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2386 / 2387 / 2388 research artifacts，不读取 fresh cached market data、不重新 backtest、不生成 signal/scoring、daily report 或交易建议。
