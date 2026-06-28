# TRADING-1887: Docs Freshness Metadata Restoration

最后更新：2026-06-28

## 背景

TRADING-1886 重构收尾运行 `python -m ai_trading_system.cli docs validate-freshness`
时发现 21 个近期 requirements 文档缺少 `最后更新：YYYY-MM-DD` 元数据。该缺口会导致
docs freshness gate fail closed，影响后续自动化提交和 CI。

本任务只补齐 requirements 文档元数据，不改变系统行为、投资解释、数据流、CLI、配置、
cache schema、report output、data quality gate、scoring、backtest、paper-shadow、
production weights、broker 或 order path。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记修复范围|DONE|task register 和本需求文档记录 docs freshness 缺口、边界和验收标准。|
|补齐元数据|DONE|21 个缺失元数据的 requirements 文档均包含 `最后更新：2026-06-28`。|
|复验文档门禁|DONE|`python -m ai_trading_system.cli docs validate-freshness`、docs/task focused pytest 和 `git diff --check` 通过。|

## Guardrails

- 只补齐 `最后更新：2026-06-28` 元数据。
- 不修改文档结论、验收标准、投资解释、任务状态或 safety boundary。
- 不创建 waiver，不降低 docs freshness gate。
- 不写 production weights、active shadow weights、paper account state、broker order
  或 trading action。

## 进展记录

- 2026-06-28: 新增任务并进入 `IN_PROGRESS`。本轮发现 21 个 requirements 文档缺少
  freshness metadata；预期仅补齐元数据并复验 docs freshness。
- 2026-06-28: 实现完成并转入 `DONE`。补齐 21 个 requirements 文档的
  `最后更新：2026-06-28` 元数据；不改变文档结论、任务状态、投资解释或 safety
  boundary。`docs validate-freshness` 恢复 PASS。
