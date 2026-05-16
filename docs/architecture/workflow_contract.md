# Workflow Contract

最后更新：2026-05-17

本文定义 daily-run、replay、shadow validation 和后续 CLI 编排应逐步对齐的最小契约。当前第一阶段只落地共享类型和 run manifest artifact 记录，后续阶段再迁移具体工作流。

## Step

`WorkflowStep` 描述一个可审计步骤，而不是一个裸命令字符串：

| 字段 | 含义 |
|---|---|
| `step_id` | 稳定机器 ID，用于 metadata、dashboard 和测试断言 |
| `name` | 人读名称 |
| `command_name` | 逻辑命令名，例如 `aits ops daily-run` |
| `command` | 实际参数列表；展示时必须脱敏 |
| `production_effect` | 核心 production effect 标签 |
| `required_inputs` | 上游 artifact refs |
| `expected_outputs` | 预期产物 artifact refs |
| `blocking` | 失败后是否阻断下游 |
| `can_run_on_closed_market` | 休市日是否允许运行 |

## Result

`WorkflowStepResult` 记录步骤执行后的审计状态：

| 字段 | 含义 |
|---|---|
| `step_id` | 对应 step |
| `status` | `PASS`、`WARN`、`FAIL`、`SKIPPED` 或 `BLOCKED` |
| `started_at` / `finished_at` | 执行时间；跳过时可为空 |
| `artifacts` | 实际产物 refs |
| `key_conclusions` | 已由上游报告生成的关键结论摘要 |
| `risks` | 阻断、限制或审计风险摘要 |
| `production_effect` | 本结果对 production 的影响标签 |

## ArtifactRef

`ArtifactRef` 是 run manifest 和后续 artifact catalog runtime 化的基础结构：

- `path`：本地路径；
- `exists`：记录写 manifest 时是否存在；
- `artifact_type`：目录为 `directory`，文件默认取扩展名；
- `sha256`：只对文件计算；
- `size_bytes`：只对文件记录；
- `file_count`：只对目录记录。

这个结构只描述产物，不判定业务含义。质量门禁、promotion contract、report conclusion 仍由各自模块负责。

## Production Effect

核心标签为：

| 标签 | 语义 |
|---|---|
| `production` | 正式生产判断链路的一部分 |
| `advisory` | 投研辅助或趋势判断输出，不自动交易 |
| `none` | 只读、诊断、dashboard 或 shadow，不改变 production |
| `validation-only` | 用于验证参数、规则或流程，不可当成生产批准 |
| `blocked` | 被质量、治理、样本、owner approval 或 contract 阻断 |

未知 legacy 文本不得被自动视为某个核心标签。需要迁移时必须逐处确认业务含义。

## 行为保持要求

workflow 契约迁移阶段不得改变：

- `scores_daily.csv`、decision snapshot、prediction ledger 和 backtest daily 的业务字段；
- daily task dashboard 的关键结论和 production/shadow 边界；
- shadow parameter search / promotion 的 validation-only 语义；
- run manifest 的既有 JSON 字段和值，除非同一变更明确登记 schema 迁移并提供兼容策略。
