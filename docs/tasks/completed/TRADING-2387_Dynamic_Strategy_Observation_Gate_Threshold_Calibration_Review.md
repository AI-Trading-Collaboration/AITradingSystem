# TRADING-2387 Dynamic Strategy Observation Gate Threshold Calibration Review

最后更新：2026-07-06

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2387_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW`
- CLI：`aits research strategies dynamic-strategy-observation-gate-threshold-calibration-review`
- 真实 run status：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- 下一路由：`TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`

## 完成内容

- 新增 observation gate threshold calibration review builder。
- 读取 TRADING-2365 / 2366 / 2384 / 2385 / 2386 prior validated artifacts 和 TRADING-2386 gate threshold constants。
- 输出 gate calibration review、gate policy review、candidate reclassification preview 和 recommended gate policy update。
- 将 current best `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` 在 calibrated-policy preview 下归类为 `OWNER_REVIEW_REQUIRED`，且 `auto_accept_allowed=false`、`owner_review_allowed=true`。
- 更新 report registry、artifact catalog、system flow、task register、requirements doc 和 focused tests。

## 关键结论

- 2386 的 `CONTINUE_OPTIMIZATION` 在当前规则下合理。
- 当前 research-only observation gate 可能过于接近 paper-shadow gate，因为缺少 owner-review-only 中间层。
- Reference candidate 不应 auto-accept，但可进入 owner review。
- Time/regime slice threshold 应分层；regime slice 更适合改为 regime expectation policy。
- Drawdown materiality 应结合 return compensation；current best 的 `return_per_drawdown_penalty=0.48887`，适合 owner-review 判断而非 auto-accept。
- 2387 不批准 observation、不修改真实 gate、不启用 paper-shadow / production / broker。

## 验证

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_observation_gate_threshold_calibration_review.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：572 docs PASS
- `python -m ai_trading_system.cli docs report-contract --latest`：1284 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run`：active=319 / completed=446 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260705T172918Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning

## 数据质量门禁说明

本任务未运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated research artifacts 和既有 threshold constants，不读取 fresh cached market data、不重新 backtest、不生成 signal/scoring、daily report 或交易建议。
