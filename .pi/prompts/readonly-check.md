---
task: {{TASK_ID}}
template: readonly-check
zone: A
model: {{MODEL}}
thinking: {{THINKING}}
tools: readonly
owner_decision: {{OWNER_DECISION_OR_NONE}}
allow_done: false
ship: none
allowed_paths: []
read_paths:
  - {{PATH_TO_READ}}
---
# {{TASK_ID}}：只读检查

## 目标

{{说明要核查的问题、期望输出和不改变任何文件的边界。}}

## 允许改的路径

见 front matter 的 `allowed_paths`；本模板为只读，列表为空。

## 步骤

1. 读取本任务单列出的 `read_paths`。
2. 用只读命令检查：{{列出允许的 rg/find/ls/测试 dry-run 命令}}。
3. 汇总事实、证据路径和不确定项。

## 验收

- 没有文件变更。
- 结论逐条引用证据路径。
- {{其他验收条件}}。

## 等价检查（删除和重构类必填）

不适用；本任务只读。如涉及删除或重构，把要证明等价的输入、输出、调用方和回退方式写在这里：{{EQUIVALENCE_CHECKS}}。

## 停止条件

- 需要写文件或运行非只读命令。
- 发现工作区存在无关变更。
- {{其他停止条件}}。

## 汇报

用中文汇报：检查范围、命令摘要、证据路径、结论、风险和未解决问题。production_effect=none；broker_action=none。
