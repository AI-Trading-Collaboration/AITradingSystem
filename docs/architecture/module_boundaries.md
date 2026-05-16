# 模块边界

最后更新：2026-05-17

本文记录重构后的目标边界。当前实现还在分阶段迁移，不能把本文理解为已经完成的目录清单；它用于约束后续改动不要继续把 CLI、workflow、报告渲染和业务计算混在一起。

## 核心原则

- `core` 只放跨流程共享的低层契约，不读取业务配置，不调用供应商，不计算投资结论。
- CLI 只负责参数解析、命令注册和调用 workflow/service；业务逻辑不继续堆进 `cli.py`。
- workflow 负责编排步骤、输入输出、阻断语义、run manifest 和审计状态；不重新计算评分结论。
- reports 负责收集既有结论、组装 view model 和渲染；dashboard 不自行推导投资结论。
- shadow 负责 validation-only 搜索、归因、promotion readiness 和 ledger 隔离；不得污染 production 权重、gate、approved overlay 或正式 prediction ledger。

## 当前已落地边界

| 模块 | 当前职责 | 禁止承担的职责 |
|---|---|---|
| `ai_trading_system.core.artifacts` | `ArtifactRef`，统一 artifact manifest 字段、checksum、size 和 directory file count | 不决定 artifact 是否合格；不读取业务报告内容 |
| `ai_trading_system.core.production_effect` | `ProductionEffect` 核心标签枚举和保守解析 | 不把未知 legacy 文本自动映射成生产影响 |
| `ai_trading_system.core.workflow` | 通用 `WorkflowStep` / `WorkflowStepResult` 契约 | 不执行子命令；不替代 daily-run 现有执行器 |
| `ai_trading_system.cli_commands.docs` | `aits docs` 低耦合命令组 | 不导入主 `cli.py`；不承担其他命令组注册 |
| `ai_trading_system.reports.daily_task_dashboard_view_model` | daily task dashboard 的 report/detail/key conclusion view model | 不读取 metadata、报告文件或渲染 HTML |
| `ai_trading_system.run_artifacts` | daily-run bundle 路径、legacy mirror、manifest 写入 | 不生成业务报告；不保存 stdout/stderr 原文 |
| `ai_trading_system.ops_daily` | daily-plan / daily-run 编排、可见性审计，以及到通用 workflow contract 的适配 | 不改变评分、仓位、shadow promotion 或 approved overlay |
| `ai_trading_system.shadow.lineage` | shadow validation-only 的 checksum、git lineage 和项目路径解析 helper | 不计算 trial objective、promotion readiness 或报告结论 |

## 后续迁移顺序

1. 继续让 daily-run / replay / shadow 逐步复用 `ArtifactRef` 和 workflow adapter，保持 manifest JSON 兼容。
2. 继续拆 daily task dashboard 的 collector / view model / renderer；当前已先抽 view model。
3. 按 `ARCH-001` 继续从低耦合命令组迁移 CLI；当前已先迁移 `aits docs`。
4. 继续拆 shadow 子系统的 search / attribution / promotion / report 边界；当前已先抽 lineage helper。
5. 最后再拆 scoring / position gate 等生产解释链路。

任何阶段都必须用目标测试或 golden/characterization diff 证明没有非预期输出变化。
