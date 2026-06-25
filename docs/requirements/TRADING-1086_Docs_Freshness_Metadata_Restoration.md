# TRADING-1086: Docs Freshness Metadata Restoration

最后更新：2026-06-26

## 背景

TRADING-1085 重构收尾验证运行
`python -m ai_trading_system.cli docs validate-freshness` 时发现 3 个新增
requirements 文档缺少 `最后更新：YYYY-MM-DD` 元数据：

- `docs/requirements/TRADING-1031_to_1048_Equal_Risk_Forward_Aging_Stabilization_and_Layer2_Growth_Restart.md`
- `docs/requirements/TRADING-1049_to_1056_Equal_Risk_Growth_V2_Real_Run_Result_Convergence.md`
- `docs/requirements/TRADING-1065_to_1084_Equal_Risk_Growth_Tilt_Exploration.md`

本任务只补齐文档 freshness metadata，不改变系统行为、数据流、报告契约、
scoring、backtest、data quality gate、market-regime interpretation 或投资解释。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记修复范围|DONE|task register 和本需求文档记录 docs freshness failure、受影响文件和 safety impact。|
|补齐元数据|DONE|3 个 requirements 文档均包含 `最后更新：2026-06-26`。|
|复验 docs gate|DONE|docs freshness、docs/task focused pytest 和 `git diff --check` 通过。|

## Guardrails

- 仅补齐 metadata 行，不改写需求语义、任务状态、acceptance criteria 或投资解释。
- 不修改 CLI、配置、cache schema、report output、data quality gate、scoring、
  backtest、production weights、paper-shadow weights、broker/order 或 trading action。

## 进展记录

- 2026-06-26: 新增任务并进入 `IN_PROGRESS`。触发原因是 docs freshness gate
  对 3 个新增 requirements 文档输出 `missing_last_updated`。
- 2026-06-26: 实现完成并转入 `DONE`。已补齐 TRADING-1031_to_1048、
  TRADING-1049_to_1056 和 TRADING-1065_to_1084 requirements 文档的
  `最后更新：2026-06-26` 元数据；`docs validate-freshness` 恢复 PASS。
