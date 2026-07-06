# TRADING-2391 Dynamic Strategy Calibrated Gate Candidate Owner Review And Observation Decision

完成日期：2026-07-07

## 摘要

- 任务登记：`TRADING-2391_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION`
- 状态：`DONE`
- 真实 run status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- Owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- 下一路由：`TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan`

## 已交付

- 新增 `src/ai_trading_system/dynamic_strategy_calibrated_gate_candidate_owner_review_decision.py`。
- 新增 CLI `aits research strategies dynamic-strategy-calibrated-gate-candidate-owner-review-decision`。
- 输出 owner review decision、candidate owner review record、observation non-approval record 和 TRADING-2392 route。
- 更新 research docs、report registry、artifact catalog、system flow、task register 和 completed archive。
- 新增 focused builder / CLI / registry-doc tests。

## 关键结论

- current best candidate 仍为 `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`。
- previous decision 为 `CONTINUE_OPTIMIZATION`。
- calibrated preview decision 为 `OWNER_REVIEW_REQUIRED`。
- `OWNER_REVIEW_REQUIRED` 不等于 observation approval。
- 默认不批准 candidate auto-accept 或 research-only observation。
- 保留 `dynamic_turnover_budgeted_growth_tilt_v1` 与 `dynamic_valid_until_expiry_strict_v1` 作为 component-value follow-up。
- 下一步进入 TRADING-2392 component attribution and gate evidence plan。

## 安全边界

- `research_only_observation_approved=false`
- `candidate_auto_accept_approved=false`
- `paper_shadow_enabled=false`
- `paper_trade_created=false`
- `shadow_position_created=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`
- `production_effect=none`
- `broker_action=none`

## 数据质量门禁

本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2386 / 2388 / 2389 / 2390 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

## 验证

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_candidate_owner_review_decision.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：577 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：1288 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：active=319 / completed=451 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T151622Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning
