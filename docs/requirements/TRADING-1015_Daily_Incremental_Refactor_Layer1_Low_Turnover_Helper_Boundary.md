# TRADING-1015: Daily Incremental Refactor Layer-1 Low-Turnover Helper Boundary

最后更新：2026-06-25

## 背景

每日增量重构巡检以 `a012e18df277d461663d7feff824419c80f32509`
之后的变更为评估范围。增量范围新增了 Layer-1 simple-rule selector
research、结果审查和 low-turnover refinement。`layer1_simple_rule_meta_policy.py`
继续承载报告生成、path construction、ranking、owner decision 和 helper logic，
模块边界开始影响后续维护可读性。

本任务只拆出 TRADING-1009～1014 low-turnover ranking / owner-decision 的纯
helper 与 research-only pilot 常量，保持 CLI、artifact schema、data quality gate、
report output、状态枚举和投资解释不变。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记重构范围|DONE|task register 和本需求文档记录本轮维护目标、边界、验收标准和 safety impact。|
|拆分 helper 边界|DONE|新增 `layer1_low_turnover_selector_helpers.py`，集中 low-turnover pilot 常量、ranking summary、dominance、candidate selection 和 owner decision helper；原 `layer1_simple_rule_meta_policy.py` 继续负责 context/data/path/report 写入。|
|行为兼容验证|DONE|focused Layer-1 pytest、CLI help/import smoke、Ruff/compileall、task/docs consistency 和 `git diff --check` 通过；本轮未执行 cached-data dependent real report，因此未生成新的 data quality sidecar。|

## Guardrails

- 不新增或修改任何 CLI command surface。
- 不改变 report schema、artifact path、status enum、threshold、score band、
  promotion gate、position constraint、backtest acceptance rule 或 data quality gate。
- 不写 production weights、active shadow weights、paper account state、broker order
  或 trading action。
- 不把 TRADING-1009～1014 的 research-only pilot 参数升级为 production policy。
- 若验证发现低换手输出发生语义变化，停止并按 no-silent-workaround 流程记录
  blocker，不提交未验证重构。

## 进展记录

- 2026-06-25: 新增任务并进入 `IN_PROGRESS`。本轮维护目标是拆出 Layer-1
  low-turnover ranking / owner-decision helper 边界，降低
  `layer1_simple_rule_meta_policy.py` 继续膨胀的维护风险；预期无外部行为变化。
- 2026-06-25: 实现完成并转入 `DONE`。新增
  `layer1_low_turnover_selector_helpers.py`，保留原 CLI、artifact schema、status
  enum、data quality gate 和 safety fields；验证通过 focused xdist pytest、CLI help
  smoke、Ruff、compileall、task/docs consistency 和 `git diff --check`。
