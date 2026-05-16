# ARCH-003：结构化降复杂度后续切片

最后更新：2026-05-17

关联任务：`ARCH-003`

## 背景

owner 要求在已完成 `ARCH-002` 第一阶段之后，继续依次推进五个低风险重构任务，并完成测试、修复和验收。该工作仍遵守“结构化降复杂度、不改变生产逻辑”的边界。

## 阶段顺序

| 阶段 | 范围 | 主要验收 |
|---|---|---|
| 1. daily-run workflow 适配 | 给 `ops_daily` 增加与 `WorkflowStep` / `WorkflowStepResult` 的适配层 | 不改变 daily-run 执行逻辑、metadata 或报告输出；`tests/test_ops_daily.py` 通过 |
| 2. daily task dashboard 第一刀 | 抽出 collector/view-model/rendering 的第一层低风险边界 | HTML/JSON payload 保持兼容；`tests/test_daily_task_dashboard.py` 通过 |
| 3. 低耦合 CLI 命令组拆分 | 先迁移 `docs` 或 `security` 等低耦合 Typer 子命令组 | 命令名、参数、退出码兼容；相关 CLI tests 通过 |
| 4. shadow 子系统前置整理 | 从 `shadow_weight_profiles.py` 抽出纯类型、manifest lineage 或报告 helper | 不改变 search/shadow/promotion 输出；`tests/test_shadow_weight_profiles.py` 通过 |
| 5. ProductionEffect 渐进替换 | 在低风险路径用 `ProductionEffect` 替代裸字符串 | 不批量改写报告长文本；边界测试通过 |

## 不变边界

- 不改变 production scoring、position gate、approved overlay 或正式 prediction ledger。
- 不改变 backtest 默认市场阶段或回测收益语义。
- 不改变 shadow parameter search ranking 或 promotion readiness 结论。
- 不重命名核心数据目录。
- 不把 dashboard 或 shadow diagnostic 结果写成 production 建议。

## 验收标准

- 每个阶段均有目标测试。
- 最终通过 `ruff`、目标 `mypy`、`git diff --check` 和全量 `pytest`。
- 文档和任务登记同步反映阶段进展。
- 未跟踪运行产物不纳入代码变更。

## 进展记录

- 2026-05-17：新增协调任务，开始阶段 1：daily-run workflow contract 适配。
- 2026-05-17：五个阶段基础实现完成并进入 VALIDATING。阶段 1 新增 daily-run 到 workflow contract 的适配函数；阶段 2 抽出 daily task dashboard view model；阶段 3 迁移 `aits docs` 命令组到 `cli_commands/docs.py`；阶段 4 抽出 shadow lineage helper；阶段 5 在低风险边界使用 `ProductionEffect` 并修复 `prediction_ledger.py` 目标 mypy 问题。验证通过目标测试、全量 ruff、全量 pytest 和 diff check。
