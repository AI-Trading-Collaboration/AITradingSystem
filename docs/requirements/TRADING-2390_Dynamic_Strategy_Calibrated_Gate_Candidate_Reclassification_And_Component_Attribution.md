# TRADING-2390 Dynamic Strategy Calibrated Gate Candidate Reclassification And Component Attribution

最后更新：2026-07-06

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-06
- 任务登记：`TRADING-2390_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION`
- 目标状态：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- 上游 owner decision：`ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL`
- 下一路由：`TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision`

## 背景

TRADING-2389 已采纳 TRADING-2388 的 calibrated research-only gate methodology，但明确不批准任何 candidate observation、paper-shadow、production 或 broker workflow。TRADING-2390 只把该 calibrated gate 应用于 TRADING-2386 的候选结果，生成 candidate reclassification preview 和 component attribution review。

本任务不是新回测、不是 signal/scoring 生成，也不是 observation approval。

## 范围

允许动作：

- 读取 TRADING-2365 / 2366 / 2386 / 2388 / 2389 prior validated research artifacts。
- 应用 TRADING-2389 采纳的 calibrated research-only gate policy。
- 生成 candidate reclassification preview。
- 生成 component attribution review。
- 生成 owner review recommendation 和 TRADING-2391 route。
- 更新 registry、artifact catalog、system flow、task register 和完成归档文档。

禁止动作：

- 启用 scheduler、创建 scheduled task 或接入 daily-run。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 批准 research-only observation、paper-shadow、production 或 broker action。
- 调用 broker API、发送 order 或生成 target weight / rebalance instruction。
- 运行新 backtest、生成新 signal、features、scoring 或 daily report。
- 修改真实 gate、阈值、score band、promotion gate、backtest acceptance rule、position constraint 或数据质量门。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2365 / 2366 / 2386 / 2388 / 2389 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为读取 cached market/macro data、重新 backtest、生成 signal/scoring 或 daily report，则必须先运行 `aits validate-data` 或同源质量门，并在输出中披露质量状态。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_calibrated_gate_candidate_reclassification.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-calibrated-gate-candidate-reclassification`。
3. fail-closed 校验 TRADING-2365 / 2366 / 2386 / 2388 / 2389 source status、owner decision、reference policy、calibrated methodology 和 safety fields。
4. 输出 `reclassification_result.json`、`candidate_reclassification_preview.json`、`component_attribution_review.json` 和 `owner_review_recommendation.json`。
5. 生成 research docs：main report、candidate reclassification preview、component attribution review 和 TRADING-2391 route。
6. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、任务登记和完成归档。
7. 新增 focused builder / CLI / registry-doc tests。
8. 运行 focused validation、真实 CLI、文档/registry/task-register/contract checks。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`。
- 输出明确包含：
  - `calibrated_gate_policy_source=TRADING-2389`
  - `reference_candidate_policy=BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
  - `candidate_reclassification_ready=true`
  - `component_attribution_ready=true`
  - `owner_review_recommendation_ready=true`
  - `current_best_candidate=equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
  - `current_best_candidate_previous_decision=CONTINUE_OPTIMIZATION`
  - `current_best_candidate_preview_decision=OWNER_REVIEW_REQUIRED`
  - `candidate_auto_accept_approved=false`
  - `research_only_observation_approved=false`
  - `component_value_candidates` 非空
  - `recommended_next_research_task=TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision`
- 所有 safety fields 保持 false / none；不批准 observation、paper-shadow、production、broker、scheduler、event append、outcome binding 或 daily report。
- Registry、artifact catalog、system flow、task register 和 completed archive 一致。
- Focused tests、Ruff、compileall、真实 CLI、docs freshness、report contract、task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-06：实现完成并归档 `DONE`。新增 calibrated gate candidate reclassification builder、CLI、candidate reclassification preview、component attribution review、owner review recommendation、TRADING-2391 route artifacts、research docs、registry、catalog、system flow、task register completed archive 和 focused tests；真实 run status=`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_candidate_reclassification.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-calibrated-gate-candidate-reclassification --as-of 2026-07-06`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 验证结果

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_candidate_reclassification.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：576 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1287 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=450 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T145156Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning
