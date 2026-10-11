---
task: {{TASK_ID}}
template: mechanical-change
zone: B
model: {{MODEL}}
thinking: {{THINKING}}
tools: default
owner_decision: none
allow_done: false
ship: publish
allowed_paths:
  - {{PATH_1}}
  - {{PATH_2}}
  - tests/{{TEST_PATH}}
read_paths:
  - .agents/skills/ship-change/SKILL.md
  - {{PRIMARY_FILE}}
---
# {{TASK_ID}}：机械性 B 区变更

## 目标

{{说明机械性修改目标，例如重命名、格式调整、测试补齐或无语义重构。}}

## 允许改的路径

见 front matter 的 `allowed_paths`。

## 步骤

1. 从 main 建任务分支：`git checkout -b {{BRANCH}} main`。
2. 读取允许路径和相关测试。
3. 执行机械性修改；不要改变投资逻辑、数据流、评分、回测或交易边界。
4. 运行针对性测试：`.venv/Scripts/python.exe -m pytest {{TEST_SELECTION}} -n 16 --dist loadfile -q`。
5. 运行必要 lint：`.venv/Scripts/python.exe -m ruff check {{PYTHON_FILES}}`。
6. 检查 diff 只包含允许路径，提交后运行 `.venv/Scripts/python.exe tools/gov008/ship.py --repo . --dry-run`。

## 验收

- 测试和 lint 通过。
- dry-run 显示 zone B 或更低；如变成 C 区立即停止。
- {{其他验收条件}}。

## 等价检查（删除和重构类必填）

{{列出等价检查：旧入口/新入口、删除文件无引用的 rg 命令、样例输入输出一致性、迁移映射和回退方式。若非删除/重构，写“不适用”。}}

## 停止条件

- 需要改允许路径之外的文件。
- dry-run 显示 C 区或要求 owner decision。
- 测试失败原因超出本任务范围。
- 工作区出现无关变更。

## 汇报

用中文汇报：分支、提交 SHA、变更摘要、测试/lint/dry-run 摘要、等价检查结果和未完成事项。production_effect=none；broker_action=none。
