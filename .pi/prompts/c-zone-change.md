---
task: {{TASK_ID}}
template: c-zone-change
zone: C
model: {{MODEL}}
thinking: {{THINKING}}
tools: default
owner_decision: {{OWNER_DECISION_ID}}
allow_done: false
ship: publish
allowed_paths:
  - {{C_ZONE_PATH}}
  - tests/{{RELATED_TEST_PATH}}
  - docs/requirements/{{REQUIREMENT_FILE}}.md
  - tasks/{{TASK_FILE}}.yaml
  - docs/task_register.md
  - docs/task_register_completed.md
read_paths:
  - AGENTS.md
  - .agents/skills/ship-change/SKILL.md
  - docs/requirements/{{REQUIREMENT_FILE}}.md
---
# {{TASK_ID}}：C 区语义关键变更

## 目标

{{说明 C 区变更目标、投资解释影响、为什么必须改 C 区，以及 owner decision 原文所在位置。}}

## 允许改的路径

见 front matter 的 `allowed_paths`；docs/task_register.md 与 docs/task_register_completed.md 只能由 `.venv/Scripts/python.exe tools/tasks.py render` 生成。

## 步骤

1. 读取 AGENTS.md、ship-change skill、相关需求文档和 owner decision 原文。
2. 核对启动脚本已设置的 `AITS_PI_OWNER_DECISION` 与 front matter 的 `owner_decision` 一致；不一致就停止。
3. 从 main 建任务分支：`git checkout -b {{BRANCH}} main`。
4. 修改允许路径，保持 C 区控制不被削弱；C 区文件只用 edit 和 write 工具改，不要用 bash 或 powershell 改。
5. 更新任务记录；追加 notes 用 `.venv/Scripts/python.exe tools/tasks.py append-note`。
6. 运行相关的针对性测试：`.venv/Scripts/python.exe -m pytest {{TARGETED_TESTS}} -q`。完整 PR suite 由 ship 门禁跑一次，dry-run 之前不要重复跑。
7. 提交时加入 `Owner-Decision: {{OWNER_DECISION_ID}}` trailer。
8. 运行 `.venv/Scripts/python.exe tools/gov008/ship.py --repo . --dry-run` 后停下；门禁和发布在 Claude 审查后由 owner 账户运行，pi 不运行 `--gate`、`--execute`、`--publish` 或 `publish.ps1`。

## 验收

- owner decision trailer 存在且与需求文档原文一致。
- 针对性测试通过，dry-run 显示 C 区和预期门禁。
- 报告、文档或系统流在受影响时同步更新。
- {{其他验收条件}}。

## 等价检查（删除和重构类必填）

{{C 区删除/重构必须列出等价证明：行为不变的测试、数据/报告输出对比、旧路径引用清零、回退策略；非删除/重构也需说明为何不适用。}}

## 停止条件

- owner decision 缺失、与原文不符或守卫锁定。
- 需要改允许路径之外的 C 区文件。
- 测试失败且修复会削弱门禁、数据质量或投资解释控制。
- 远端/main 分叉或工作区有无关变更。

## 汇报

用中文汇报：分支、提交 SHA、owner decision、C 区文件、测试结果、dry-run 摘要、风险和未解决问题。accept_checks、测试或 ruff 报出的问题，不要为了让它通过而换一种写法绕开；照实写进汇报并说明原因，由 Claude 判断。production_effect=none；broker_action=none。
