# TRADING-2390 Dynamic Strategy Calibrated Gate Candidate Reclassification And Component Attribution

最后更新：2026-07-06

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2390_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION`
- CLI：`aits research strategies dynamic-strategy-calibrated-gate-candidate-reclassification`
- 真实 run status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- 下一路由：`TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision`

## 完成内容

- 新增 calibrated gate candidate reclassification builder。
- 读取 TRADING-2365 / 2366 / 2386 / 2388 / 2389 prior validated artifacts。
- 输出 candidate reclassification preview、component attribution review、owner review recommendation 和 TRADING-2391 route。
- 更新 report registry、artifact catalog、system flow、task register、requirements doc 和 focused tests。
- 明确本任务是 reclassification preview / component attribution，不是 observation approval、paper-shadow、scheduler、event/outcome、daily report、production 或 broker readiness。

## 关键结论

- 当前 best candidate `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` 从 `CONTINUE_OPTIMIZATION` 预览重分类为 `OWNER_REVIEW_REQUIRED`。
- `dynamic_turnover_budgeted_growth_tilt_v1` 与 `dynamic_valid_until_expiry_strict_v1` 标记为 `COMPONENT_VALUE_ONLY`。
- 没有 candidate auto-accept。
- 没有 research-only observation approval。
- 下一步进入 TRADING-2391 owner review / observation decision record。

## 验证

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_candidate_reclassification.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：576 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1287 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=450 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T145156Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning

## 数据质量门禁说明

本任务未运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2365 / 2366 / 2386 / 2388 / 2389 research artifacts，不读取 fresh cached market data、不重新 backtest、不生成 signal/scoring、daily report 或交易建议。
