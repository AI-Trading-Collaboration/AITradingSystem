---
task: {{TASK_ID}}
zone: A
model: {{MODEL}}
thinking: {{THINKING}}
tools: read,bash,edit,write
owner_decision: {{OWNER_DECISION_OR_NONE}}
allow_done: {{ALLOW_DONE}}
read_paths:
  - .agents/skills/task-records/SKILL.md
  - {{RELATED_REQUIREMENT_OR_TASK}}
---
# {{TASK_ID}}：记录类变更

## 目标

{{说明要登记、更新或关闭的任务/需求/记录，以及为什么需要这次记录变更。}}

## 允许改的路径

- tasks/{{TASK_FILE}}.yaml
- docs/requirements/{{REQUIREMENT_FILE}}.md
- docs/task_register.md 与 docs/task_register_completed.md（只能由 `.venv/Scripts/python.exe tools/tasks.py render` 生成）
- {{其他仅记录类路径}}

## 步骤

1. 读取 task-records skill 和相关任务/需求。
2. 用 `.venv/Scripts/python.exe tools/tasks.py new|set|append-note` 更新任务；追加 notes 必须用 `append-note`。
3. 如需要，编辑需求文档并记录 owner decision 原文。
4. 运行 `.venv/Scripts/python.exe tools/tasks.py render` 和 `.venv/Scripts/python.exe tools/tasks.py check`。
5. 检查 git diff 只包含允许路径。

## 验收

- task 文件 schema 合法，rendered views 最新。
- notes 追加使用 `append-note`，没有手改 rendered views。
- {{其他验收条件}}。

## 等价检查（删除和重构类必填）

{{如删除旧记录或迁移文档，列出旧路径到新路径的映射、保留的信息字段和检查命令；否则写“不适用”。}}

## 停止条件

- 需要改非记录类路径。
- 需要把任务置为 DONE 但 `allow_done` 不是 true。
- 找不到 owner decision 原文。
- {{其他停止条件}}。

## 汇报

用中文汇报：任务状态、变更文件、render/check 结果、owner decision（如有）和后续责任方。production_effect=none；broker_action=none。
