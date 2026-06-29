# TRADING-2296: Daily Incremental Refactor Candidate Generator Safety Boundary

最后更新：2026-06-29

## 背景

每日增量重构巡检以 `e2b9720456e4de56d4a6191dfe989d2baf80c836`
之后的变更为评估范围。增量范围新增大量 first-layer research / Norgate trial /
candidate artifact governance 模块；最新 TRADING-2283 first-layer executable candidate
generator framework 中，registry、runtime、signal spec 和 framework smoke generator 多处重复
写入相同的 research-only safety metadata：

- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `historical_executable_artifact=false`
- `actual_path_validation_ready=false`
- `permanently_inconclusive_override_allowed=false`

这些字段是研究候选生成框架的固定安全边界，不是可调 heuristic。重复手写会增加后续真实
trend/risk/volatility generator 接入时漏改或字段不一致的维护风险。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记重构范围|DONE|task register 和本需求文档记录维护目标、边界、验收标准和 safety impact。|
|集中 safety metadata helper|DONE|在 first-layer candidate generator shared module 中提供固定 research-only safety helper，并在 registry、runtime、signal spec 和 framework smoke generator 复用。|
|行为兼容验证|DONE|focused generator pytest、CLI smoke、Ruff、compileall、docs freshness 和 `git diff --check` 通过；不要求 `aits validate-data`，因为本轮不消费 cached market/macro data，也不生成 scoring/backtest/daily report 输出。|

## Guardrails

- 不新增、删除或重命名任何外部 CLI command。
- 不改变 command 参数、默认路径、artifact path、report schema、validation status、
  status enum、safety fields 或 fail-closed 语义。
- 不改变 threshold、score band、promotion gate、position constraint、backtest acceptance rule、
  market-regime interpretation 或投资解释。
- 不写 production weights、active shadow weights、paper account state、broker order 或 trading action。
- 如果验证发现 generated artifact payload、validation summary 或 safety boundary 发生语义变化，
  停止并按 no-silent-workaround 流程记录 blocker，不提交未验证重构。

## 进展记录

- 2026-06-29: 新增任务并进入 `IN_PROGRESS`。本轮维护目标是集中 TRADING-2283
  first-layer candidate generator framework 的固定 research-only safety metadata，降低后续
  executable generator 扩展时的字段一致性风险；预期无外部行为变化。
- 2026-06-29: 实现完成并转入 `DONE`。`first_layer_candidate_signal_generator.py`
  新增 `generator_operation_safety_fields()`、`candidate_artifact_safety_fields()`、
  `framework_smoke_artifact_safety_fields()` 和 `trading_2281_boundary_fields()`；
  registry、runtime、signal spec 和 `framework_smoke_candidate` 改为复用 helper。
  外部 CLI、artifact schema、validation status、safety fields 和投资解释保持兼容；
  验证通过 Ruff、compileall、focused parallel pytest（24 passed）、CLI smoke、docs freshness
  和 `git diff --check`。
