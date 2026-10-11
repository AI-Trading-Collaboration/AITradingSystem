# AI Trading System

面向美股 AI 产业链的投资研究与每日报告系统：采集行情和宏观数据，经数据质量门禁后做趋势判断、风险评分和仓位解释，
生成每日报告；研究和回测用同一套数据。系统不连接实盘交易，不下单（`production_effect=none`，`broker_action=none`）。

## 从这里开始

| 要做什么 | 先看 |
|---|---|
| 改代码、文档或任务记录 | [AGENTS.md](AGENTS.md)，以及 `.agents/skills/` 下的 ship-change、task-records、periodic-operations、research-run |
| 每日运行、调度、恢复 | [运维手册](docs/operations/operations_runbook.md) |
| 系统怎么运转 | [系统流程](docs/system_flow.md) 第 1～6 节（第 7 节是只读的历史附录） |
| 产物在哪里 | [产物目录](docs/artifact_catalog.md) |
| 有哪些任务 | `tasks/<ID>.yaml`；视图：[未完成](docs/task_register.md)、[已完成](docs/task_register_completed.md) |

## 几条硬规则

- 读取缓存行情或宏观数据的命令，先过 `aits validate-data`；不通过就不解释投资结论。
- 改动经 ship 门禁发布（`tools/gov008/ship.py`，先 `--dry-run`），步骤见 AGENTS.md 的 "How a change ships"。
  要进生产的改动，发布后按运维手册第 9.1 节切换运行副本。
- 每日调度只有 Windows 计划任务 "AITradingSystem Daily Run"。

## 旧版 README

本文件在 2026-10-11 改写。改写前的 README（约 300 KB，其中很多命令已在 GOV-008 删除）原样保存在
[docs/archive/README.md](docs/archive/README.md)，只作历史查阅。
