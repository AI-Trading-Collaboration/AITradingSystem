# TRADING-2389 Dynamic Strategy Calibrated Gate Owner Review And Next Decision

最后更新：2026-07-06

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-06
- 任务登记：`TRADING-2389_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION`
- 目标状态：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- 默认 owner decision：`ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL`
- 下一路由：`TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution`

## 背景

TRADING-2386 已完成 expanded candidate pool retest，TRADING-2387 已提出 research-only observation gate calibration proposal，TRADING-2388 已把 threshold methodology、gate taxonomy、candidate threshold outcome matrix 和 recommended gate policy proposal 整理成 owner review 输入包。

本任务记录 owner 对 calibrated research-only gate methodology 的采纳决策，并明确当前不批准任何 candidate 进入 research-only observation、paper-shadow、production 或 broker workflow。它不是新 backtest、不是 signal/scoring 生成，也不是 observation approval。

## 范围

允许动作：

- 读取 TRADING-2386 / 2387 / 2388 prior validated research artifacts。
- 记录 owner decision 和 calibrated gate adoption record。
- 记录 current best / reference candidate 的 non-approval decision。
- 生成下一步 candidate reclassification / component attribution route。
- 更新 registry、artifact catalog、system flow、task register 和完成归档文档。

禁止动作：

- 启用 scheduler、创建 scheduled task 或接入 daily-run。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 批准 research-only observation、paper-shadow、production 或 broker action。
- 调用 broker API、发送 order 或生成 target weight / rebalance instruction。
- 运行新 backtest、生成新 signal、features、scoring 或 daily report。
- 修改阈值、score band、promotion gate、backtest acceptance rule、position constraint 或数据质量门。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2386 / 2387 / 2388 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为读取 cached market/macro data、重新 backtest、生成 signal/scoring 或 daily report，则必须先运行 `aits validate-data` 或同源质量门，并在输出中披露质量状态。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_calibrated_gate_owner_review_decision.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-calibrated-gate-owner-review-decision`。
3. fail-closed 校验 TRADING-2386 / 2387 / 2388 source status、source hashes、reference policy、calibrated preview 和 safety fields。
4. 输出 `owner_review_decision.json`、`calibrated_gate_adoption_record.json`、`non_approval_record.json` 和 `next_reclassification_route.json`。
5. 生成 research docs：owner decision、gate adoption、non-approval record 和 TRADING-2390 route。
6. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、任务登记和完成归档。
7. 新增 focused builder / CLI / registry-doc tests。
8. 运行 focused validation、真实 CLI、文档/registry/task-register/contract checks。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`。
- 输出明确包含：
  - `owner_decision_recorded=true`
  - `threshold_methodology_adopted=true`
  - `research_only_vs_paper_shadow_gate_separated=true`
  - `reference_candidate_policy_adopted=BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
  - `candidate_auto_accept_approved=false`
  - `current_best_candidate_observation_approved=false`
  - `calibrated_reclassification_preview_approved=true`
  - `component_attribution_review_required=true`
  - `future_statistical_threshold_calibration_required=true`
- 所有 safety fields 保持 false / none；不批准 observation、paper-shadow、production、broker、scheduler、event append、outcome binding 或 daily report。
- Registry、artifact catalog、system flow、task register 和 completed archive 一致。
- Focused tests、Ruff、compileall、真实 CLI、docs freshness、report contract、task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-06：实现完成并归档 `DONE`。新增 calibrated gate owner review decision builder、CLI、owner review decision/adoption/non-approval/2390 route artifacts、research docs、registry、catalog、system flow、task register completed archive 和 focused tests；真实 run status=`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`。

## 验证计划

- `python -m ruff check ...`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_owner_review_decision.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-calibrated-gate-owner-review-decision --as-of 2026-07-06`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 验证结果

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_calibrated_gate_owner_review_decision.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：575 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1286 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=449 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T020854Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning
