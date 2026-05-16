# ARCH-002：workflow 与 artifact 契约重构

最后更新：2026-05-17

关联任务：`ARCH-002`

## 背景

本轮重构目标是结构化降复杂度，不改变生产评分、仓位闸门、回测语义、shadow promotion 结论或正式 ledger。当前 `ops_daily.py` 已经有 daily-run plan、step、step result 和 run metadata 的本地结构，`run_artifacts.py` 已经有 run bundle manifest 记录逻辑，但这些契约仍分散在具体模块中。

先抽出共享契约可以支撑后续 CLI 分包、daily task dashboard 分层、shadow 参数子系统拆分和流程编排增强，同时用现有测试证明输出行为保持兼容。

## 范围

本任务分阶段推进：

1. 抽出 `src/ai_trading_system/core/` 基础契约：
   - `ProductionEffect`：统一核心 production effect 标签；
   - `ArtifactRef`：统一 artifact manifest 记录字段；
   - workflow step/result dataclass：记录 step id、命令、输入、输出、blocking 和 production effect。
2. 将 run manifest artifact 记录迁移到 `ArtifactRef`，保持 JSON 字段和值兼容。
3. 文档明确模块边界和 workflow contract。
4. 后续阶段再考虑 daily-run plan/result 逐步对齐共享 workflow contract；CLI 分包继续由 `ARCH-001` 负责。

## 不在本轮范围

- 不迁移 CLI command handler。
- 不重命名 `data/raw`、`data/processed`、`outputs/reports`、`outputs/runs`。
- 不改变 `score-daily`、`backtest`、daily task dashboard 或 shadow 参数搜索的业务结论。
- 不把 shadow 参数搜索结果接入 production。

## 验收标准

- `run_artifacts` 继续输出相同 schema 的 `manifest.json`，包括 `path`、`exists`、`artifact_type`、`sha256`、`size_bytes` 和 `file_count`。
- `ProductionEffect` 至少覆盖 `production`、`advisory`、`none`、`validation-only` 和 `blocked`，并提供保守解析方法。
- workflow contract 文档说明 step/result 的字段语义、production/shadow 边界和后续迁移顺序。
- 目标测试、ruff 和 diff check 通过。
- 未跟踪运行产物不被纳入代码变更。

## 进展记录

- 2026-05-17：新增任务和需求文档，进入第一阶段实现。当前选择 run manifest artifact 记录作为最小行为保持重构切片。
- 2026-05-17：第一阶段实现完成并进入 VALIDATING。新增 `ai_trading_system.core` 契约模块，`run_artifacts` 改为通过 `ArtifactRef` 生成 manifest artifact 记录；新增模块边界和 workflow contract 文档，并同步 `docs/system_flow.md` 与 `docs/artifact_catalog.md`。验证通过目标测试、ruff、mypy、docs freshness、diff check 和全量 pytest。
- 2026-05-17：第二阶段适配层完成。`ops_daily` 新增 `daily_ops_step_to_workflow_step()` 和 `daily_ops_step_result_to_workflow_step_result()`，把既有 daily-run step/result 显式映射到通用 workflow contract；不改变 daily-run 执行器、metadata 或报告输出。目标测试、ruff 和目标 mypy 通过。
