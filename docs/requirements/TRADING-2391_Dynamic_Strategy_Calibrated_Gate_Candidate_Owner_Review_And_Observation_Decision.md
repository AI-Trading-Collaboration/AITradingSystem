# TRADING-2391 Dynamic Strategy Calibrated Gate Candidate Owner Review And Observation Decision

最后更新：2026-07-07

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-07
- 任务登记：`TRADING-2391_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION`
- 目标状态：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- 默认 owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- 下一路由：`TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan`

## 背景

TRADING-2390 已把 TRADING-2389 采纳的 calibrated research-only gate 应用于 expanded candidate pool，并把 current best candidate 从 `CONTINUE_OPTIMIZATION` preview 为 `OWNER_REVIEW_REQUIRED`。该 preview 只表示候选足以进入人工决策层，不等于批准 observation、paper-shadow 或任何执行路径。

本任务只记录 owner review decision。默认没有明确 owner 批准 observation，因此不批准 research-only observation，保留 `OWNER_REVIEW_REQUIRED`，并继续 component attribution / gate evidence follow-up。

## 范围

允许动作：

- 读取 TRADING-2386 / 2388 / 2389 / 2390 prior validated artifacts。
- 校验 current best candidate、previous decision、calibrated preview decision、owner-adopted gate policy 和 source safety fields。
- 记录 candidate owner review decision。
- 生成 observation non-approval record。
- 保留 `dynamic_turnover_budgeted_growth_tilt_v1` 与 `dynamic_valid_until_expiry_strict_v1` 作为 component-value follow-up。
- 生成 TRADING-2392 route。
- 更新 registry、artifact catalog、system flow、task register 和完成归档文档。

禁止动作：

- 运行新 backtest、生成新 signal、features、scoring 或 daily report。
- 批准 candidate auto-accept 或 research-only observation。
- 启用 scheduler、创建 scheduled task 或接入 daily-run。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API、发送 order 或生成 target weight / rebalance instruction。
- 修改真实 gate、阈值、score band、promotion gate、backtest acceptance rule、position constraint 或数据质量门。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2386 / 2388 / 2389 / 2390 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为读取 cached market/macro data、重新 backtest、生成 signal/scoring 或 daily report，则必须先运行 `aits validate-data` 或同源质量门，并在输出中披露质量状态。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_calibrated_gate_candidate_owner_review_decision.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-calibrated-gate-candidate-owner-review-decision`。
3. fail-closed 校验 TRADING-2386 / 2388 / 2389 / 2390 source status、preview decision、owner decision、reference policy、component value candidates 和 safety fields。
4. 输出 `owner_review_decision.json`、`candidate_owner_review_record.json`、`observation_non_approval_record.json` 和 `next_route.json`。
5. 生成 research docs：main report、candidate owner review record、observation non-approval record 和 TRADING-2392 route。
6. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、任务登记和完成归档。
7. 新增 focused builder / CLI / registry-doc tests。
8. 运行 focused validation、真实 CLI、文档/registry/task-register/contract checks。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。
- 输出明确包含：
  - `current_best_candidate=equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
  - `previous_decision=CONTINUE_OPTIMIZATION`
  - `calibrated_preview_decision=OWNER_REVIEW_REQUIRED`
  - `owner_review_decision_recorded=true`
  - `owner_decision=DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
  - `research_only_observation_approved=false`
  - `candidate_auto_accept_approved=false`
  - `owner_review_required_retained=true`
  - `component_attribution_continue_recommended=true`
  - `component_value_candidates` 包含 `dynamic_turnover_budgeted_growth_tilt_v1` 和 `dynamic_valid_until_expiry_strict_v1`
  - `recommended_next_research_task=TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan`
- 所有 safety fields 保持 false / none；不批准 observation、paper-shadow、production、broker、scheduler、event append、outcome binding 或 daily report。
- Registry、artifact catalog、system flow、task register 和 completed archive 一致。
- Focused tests、Ruff、compileall、真实 CLI、docs freshness、report contract、task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。当前存在已归档的 `TRADING-2391_DAILY_INCREMENTAL_REFACTOR_DYNAMIC_STRATEGY_REPORT_WRITER_BOUNDARY`，本任务使用完整唯一 ID 区分 strategy research owner decision。
- 2026-07-07：实现完成并归档 `DONE`。新增 calibrated gate candidate owner review decision builder、CLI、candidate owner review record、observation non-approval record、TRADING-2392 route artifacts、research docs、registry、catalog、system flow、task register completed archive 和 focused tests；真实 run status=`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_candidate_owner_review_decision.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-calibrated-gate-candidate-owner-review-decision --as-of 2026-07-07`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 验证结果

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
