# TRADING-2397 Dynamic Strategy Recombination Candidate Owner Review And Observation Decision

最后更新：2026-07-07

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2397_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION`
- 优先级：P0
- 下一路由：`TRADING-2398_Dynamic_Strategy_Recombination_Candidate_Gate_Evidence_And_Targeted_Improvement_Plan`
- 任务边界：owner decision / observation non-approval / gate evidence gap summary；不批准 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report。

## 背景

TRADING-2396 已完成 component recombination candidate retest。真实结果显示 best recombination candidate=`growth_tilt_lower_turnover_guarded_transfer_v1`，best decision=`OWNER_REVIEW_REQUIRED`，observation preview candidates=0。TRADING-2397 的职责是把该结果转成 owner review decision record：承认当前最佳候选进入 owner review 层级，但由于没有 observation preview candidate，默认不批准 research-only observation，并把下一步限定为 gate evidence / targeted improvement plan。

## 非目标与安全边界

本任务不得：

- run new backtest；
- generate new signal / scoring；
- approve research-only observation；
- enable scheduler / create scheduled task；
- append event / mutate historical event log；
- bind outcome / mutate outcome store；
- enable paper-shadow / create paper trade / shadow position；
- enable production / broker / order；
- generate daily report。

允许读取 prior validated TRADING-2396 / 2395 / 2394 / 2393 artifacts，记录 owner decision、observation non-approval、gate evidence gap summary 和 TRADING-2398 route。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_strategy_recombination_candidate_owner_review_decision.py`。
2. 新增 CLI：`aits research strategies dynamic-strategy-recombination-candidate-owner-review-decision`。
3. Fail-closed 校验 TRADING-2396 retest / ranking / evidence / decision update，以及 TRADING-2395 plan、TRADING-2394 owner decision、TRADING-2393 component ablation artifacts。
4. 记录 owner decision=`KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE`。
5. 输出 owner review decision、observation non-approval record、gate evidence gap summary 和 TRADING-2398 route。
6. 更新 report registry、artifact catalog、system flow、task register 和 completed archive。
7. 新增 focused tests。

## 验收标准

真实 CLI run 必须返回：

- `status=DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- `best_recombination_candidate=growth_tilt_lower_turnover_guarded_transfer_v1`
- `best_recombination_decision_from_2396=OWNER_REVIEW_REQUIRED`
- `owner_review_decision_recorded=true`
- `owner_review_required_retained=true`
- `observation_preview_candidates_count=0`
- `research_only_observation_approved=false`
- `gate_evidence_gap_summary_ready=true`
- `recommended_next_research_task=TRADING-2398_Dynamic_Strategy_Recombination_Candidate_Gate_Evidence_And_Targeted_Improvement_Plan`
- candidate auto-accept、paper-shadow、paper trade、shadow position、scheduler、event append、outcome binding、production、broker、daily report 全部为 false / none

## 数据质量门禁

本任务只读取 prior validated artifacts，不读取 fresh cached market data、不运行新 backtest、不生成 technical features、scoring、daily report 或交易建议，因此本任务自身不运行：

```bash
aits validate-data --as-of 2026-07-05
```

输出中必须披露 `data_quality_gate_executed=false` 和 `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA`。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_recombination_candidate_owner_review_decision.py`
- `python -m ai_trading_system.cli research strategies dynamic-strategy-recombination-candidate-owner-review-decision --as-of 2026-07-07`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 owner decision / non-approval record，只读取 prior artifacts；不批准 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report。
- 2026-07-07：实现完成并归档 `DONE`。真实 CLI run 返回 `DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`；best recombination candidate=`growth_tilt_lower_turnover_guarded_transfer_v1`，best decision from 2396=`OWNER_REVIEW_REQUIRED`，observation preview candidates=0，owner decision=`KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE`，next route=`TRADING-2398_Dynamic_Strategy_Recombination_Candidate_Gate_Evidence_And_Targeted_Improvement_Plan`。candidate auto-accept / research-only observation / paper-shadow / scheduler / event append / outcome binding / production / broker / daily report 全部保持 disabled / false / none。

## 验证结果

- `python -m ruff check .`：PASS。
- `python -m compileall -q src tests`：PASS。
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_recombination_candidate_owner_review_decision.py`：3 passed。
- `python -m ai_trading_system.cli research strategies dynamic-strategy-recombination-candidate-owner-review-decision --as-of 2026-07-07`：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。
- `python -m ai_trading_system.cli docs validate-freshness`：583 docs PASS。
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：1294 reports PASS。
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319，completed=457，failed=0。
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0。
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T174825Z/test_runtime_summary.json`。
- `git diff --check`：PASS（仅 Git CRLF normalization warning，退出码 0）。
