# TRADING-2388 Dynamic Strategy Research Filter Threshold Methodology Review

最后更新：2026-07-06

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-06
- 任务登记：`TRADING-2388_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW`
- 目标状态：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- 下一路由：`TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`

## 背景

TRADING-2387 已完成 observation gate threshold calibration review，并指出 current research-only observation gate 可能过于接近 paper-shadow gate。Owner 进一步要求先审查研究过滤标准本身：哪些 threshold 是工程安全硬约束，哪些是研究质量过滤，哪些只是保守 heuristic，哪些需要后续统计校准。

2388 因此定位为 `Research filter threshold meta-review`。它不是候选策略回测，也不是 observation owner decision。

## 范围

允许动作：

- 读取 TRADING-2364 / 2365 / 2366 / 2375 / 2376 / 2379 / 2383 / 2386 / 2387 prior validated research artifacts。
- 读取现有 threshold constants 和 decision rule 代码。
- 建立 threshold inventory、gate taxonomy、candidate threshold outcome matrix 和 recommended gate policy proposal。
- 生成 research docs 和 TRADING-2389 route。

禁止动作：

- 批准 observation、paper-shadow 或 broker action。
- 修改真实 gate 或策略执行路径。
- 启用 scheduler、append event、bind outcome、mutate outcome store。
- 创建 paper trade / shadow position。
- 运行 production、调用 broker API 或发送 order。
- 生成 daily report。
- 运行新 backtest、生成新 signal、features 或 scoring。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated research artifacts 和既有 threshold constants，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为重新读取行情、重新 backtest 或生成 signal / scoring，则必须先运行同源 cached-data quality gate 并在输出中披露。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_research_filter_threshold_methodology_review.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-research-filter-threshold-methodology-review`。
3. fail-closed 校验 2364 / 2365 / 2366 / 2375 / 2376 / 2379 / 2383 / 2386 / 2387 source status、key route、safety fields 和 source hashes。
4. 建立 execution-cadence、cost/turnover、slice stability、drawdown/risk、reference candidate、relative candidate threshold inventory。
5. 建立 research-only observation / paper-shadow / production-broker gate taxonomy。
6. 汇总关键候选 threshold outcome matrix。
7. 输出 recommended gate policy proposal 和 future statistical calibration needs。
8. 更新 registry、artifact catalog、system flow、task register 和完成归档文档。
9. 新增 focused tests 并执行验证。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`。
- 输出 JSON 至少包含 `threshold_methodology_review_result.json`、`threshold_inventory.json`、`gate_taxonomy.json`、`candidate_threshold_outcome_matrix.json`、`recommended_gate_policy_proposal.json`。
- Markdown 主报告明确回答当前验收标准并非完全基于历史经验、哪些 threshold 已有项目内实验证据、哪些是保守 heuristic、research-only observation 应低于 paper-shadow gate、reference candidate 不应 hard-block auto review、time/regime/drawdown threshold 需要统计校准。
- 所有 safety fields 保持 false / none；不批准 observation、paper-shadow、production 或 broker。
- Registry、artifact catalog、system flow、task register 一致。
- Focused tests、Ruff、compileall、docs gates、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-06：实现完成并归档 `DONE`。新增
  `aits research strategies dynamic-strategy-research-filter-threshold-methodology-review`
  CLI、threshold methodology review builder、5 个 JSON 输出、5 个 research docs、
  registry/catalog/system_flow 登记和 focused tests。真实 run status 为
  `DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`，下一路由为
  `TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`。

## 验证

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_research_filter_threshold_methodology_review.py`：3 passed
- 真实 CLI run：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- `python -m ai_trading_system.cli docs validate-freshness`：573 docs PASS
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-06`：1285 reports PASS
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py`：6 passed
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=447 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260705T174509Z/test_runtime_summary.json`
- `git diff --check`：仅 CRLF normalization warning

## 完成时数据质量门禁说明

本任务未运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated
research artifacts 和既有 threshold constants，不读取 fresh cached market data、不重新
backtest、不生成 technical features、scoring、daily report 或交易建议。
