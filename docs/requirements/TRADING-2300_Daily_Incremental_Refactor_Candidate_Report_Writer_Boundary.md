# TRADING-2300: Daily Incremental Refactor Candidate Report Writer Boundary

最后更新：2026-06-30

## 背景

每日增量重构巡检以 `656aa565aa63ff37f2e795e72e393c7796981c48` 之后的变更为评估范围。增量范围新增 TRADING-2285～2289 regenerated/refined candidate actual-path validation、inconclusive diagnostics、confidence scaling refinement 和 refined regeneration outputs。多个 research-only report modules 重复实现相同的 artifact 写入模式：

- 写入 `{**common, "rows": rows}` JSON matrix。
- 将 rows 通过 `clean_for_yaml` 规范化后写入 CSV。
- 为 actual-path / refined actual-path matrix 单独保留重复 `_write_csv`。

这些 helper 只负责 artifact serialization，不是投资解释、阈值、score band、promotion gate 或数据质量门。重复实现会增加后续 candidate/refined report 扩展时 JSON/CSV 清洗行为不一致的维护风险。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记重构范围|DONE|task register 和本需求文档记录维护目标、边界、验收标准和 safety impact。|
|集中 report writer helper|DONE|在 `post_2085_research_common` 中提供共享 CSV row writer 和 JSON+CSV matrix writer；受影响 candidate/refined modules 复用 helper。|
|行为兼容验证|DONE|focused candidate/refined pytest、Ruff、compileall、docs freshness、task-register consistency 和 `git diff --check` 通过；不要求 `aits validate-data`，因为本轮不生成新的 cached-data dependent scoring/backtest/daily report 输出。|

## Guardrails

- 不新增、删除或重命名任何外部 CLI command。
- 不改变 command 参数、默认路径、artifact path、JSON key、CSV column、report schema、status enum、safety fields 或 fail-closed 语义。
- 不改变 threshold、score band、confidence cutoff、promotion gate、position constraint、backtest acceptance rule、data quality gate、market-regime interpretation 或投资解释。
- 不写 production weights、active shadow weights、paper account state、broker order 或 trading action。
- 如果验证发现 generated artifact payload、CSV shape、report status 或 safety boundary 发生语义变化，停止并按 no-silent-workaround 流程记录 blocker，不提交未验证重构。

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`。本轮维护目标是收敛 TRADING-2285～2289 candidate/refined research report 中重复的 JSON+CSV matrix writer 和 CSV row writer，降低后续 report artifact 清洗行为分叉风险；预期无外部行为变化。
- 2026-06-30: 实现完成并转入 `DONE`。`post_2085_research_common` 新增 `write_csv_rows()` 和 `write_matrix_artifacts()`；`regenerated_candidate_actual_path_validation.py`、`refined_candidate_actual_path_validation.py`、`regenerated_candidate_inconclusive_diagnostics.py` 和 `candidate_confidence_scaling_refinement_plan.py` 复用共享 writer。外部 CLI、artifact path、JSON key、CSV column、report schema、status/safety fields 和投资解释保持兼容；验证通过 Ruff、compileall、focused parallel pytest（99 passed）、4 个 CLI help smoke、docs freshness、docs/task focused pytest、terminal 状态扫描和 `git diff --check`。本轮未生成 cached-data dependent scoring/backtest/daily report 输出，因此未运行 `aits validate-data`。
