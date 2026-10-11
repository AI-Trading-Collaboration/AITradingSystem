# DEVX-025 pi 工作台

## 1. 背景

GOV-008 迁移后，pi 将承担日常开发工作台职责。试跑暴露出两类偏差：一是 AGENTS.md 中的工程规则需要在 pi 会话内更早、更明确地阻断（例如 C 区确认、系统 Python、未排除 dirty exclusion 的全仓库 git 命令、未读 operations runbook 即运行 `aits ops`）；二是任务记录和交付流程需要减少手工编辑和重复门禁成本。

本需求覆盖“迁移后 pi 用法方案第三版”的第一阶段工作台建设，拆成三次运行，避免把 B 区守卫与 C 区 ship 语义改造混在同一次变更中。

## 2. owner 决定

owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"把 ship 拆成'受限账户跑门禁、你的账户只发布'吗"选择了"在 1a 拆"（选项后果："门禁只在受限账户里跑一次，每次发布省约 10 分钟，pi 写的代码不在你的账户里运行"）。在运行 03 执行。

## 3. 分三次运行的步骤与验收

### 运行 01：守卫加固、append-note、任务单模板

步骤：

1. 登记 DEVX-025 任务记录，并把本需求文档作为多步计划来源。
2. 加固 `.pi/extensions/aits-guard`：C 区拒绝后锁定、owner decision 环境变量校验、powershell 覆盖、系统解释器拦截、全仓库 git pathspec 约束、`aits ops` runbook 约束、任务 notes/DONE 约束、长 tool_result 落盘、settled 报告。
3. `tools/tasks.py` 增加 `append-note <ID> <text>`，用中文分号追加并自动 render。
4. `ship-change` 与 `task-records` skill 改用项目 venv 和 `append-note` 写法。
5. 新增 `.pi/prompts/` 下四个任务单模板。

验收：

- 守卫第 4 节 a–i 每项至少有一个“该拦的拦了”和一个“不该拦的没拦/正确路径放行”的测试。
- `append-note` 测试通过，`tools/tasks.py check` 通过。
- `ship.py --dry-run` 显示本次为 B 区并列出 PR suite 门禁；不执行 `--execute`。

### 运行 02：启动脚本、验收脚本、看板

步骤：

1. 增加 pi 工作台启动脚本，固化 run/task/zone/allowed paths 等环境变量输入。
2. 增加验收脚本，汇总守卫、任务记录和模板检查。
3. 增加轻量看板或索引，显示当前 pi 运行状态、报告路径和未完成项。

验收：

- 启动脚本不注册新的生产/operations 计划任务。
- 验收脚本能在只读检查模式下运行，并对缺失环境变量给出明确错误。
- 看板只读取本地 run artifacts，不改变数据、报告或策略结果。

### 运行 03：ship 拆成 gate 和 publish（C 区）

步骤：

1. 按 owner 决定把 `tools/gov008/ship.py` 拆成受限账户运行 gate 与 owner 账户 publish 的受控路径。
2. 更新 ship policy、文档和测试，保证门禁只跑一次且发布只接受已验证树。
3. 为 C 区变更提交引用 `Owner-Decision: owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split`。

验收：

- C 区门禁和 owner decision 要求不被削弱。
- gate artifact 能证明 tested tree，publish 只 fast-forward 到该树。
- 原 `ship --execute --push` 的安全语义有等价或更强替代。

## 4. 开放问题

- 受限账户的“只能手动运行的计划任务”是否作为 periodic-operations“不要注册第二个计划任务”规则的例外，待 owner 决定（阶段 1b 前）。

## 5. 进度

- 2026-10-11：运行 01 开始；任务记录、需求文档、守卫加固、`append-note` 和 prompt 模板纳入本次变更。
- 2026-10-11：运行 01 验收退回一次（拦截终止运行、报告漏掉已提交改动、测试缺反向用例等），执行 02 修正。
- 2026-10-11：运行 02 增加 brief 解析器、pi 启动/受限账户脚本、机械验收脚本和 runs 看板，并补充对应测试。
- 2026-10-11：运行 02 验收退回一次（三个 PowerShell 脚本在 5.1 下跑不通、验收脚本的区划和 Gate 解析与 ship 不一致等），执行 02 修正。
- 2026-10-11：运行 02 第二次验收退回（真实 pi 的启动解析、stdin 带 BOM、安装脚本两处），执行 03 修正。
- 2026-10-11：运行 02 第三次验收退回（看板重复计数工具调用、safe.directory 路径没转换、accept_checks 漏认几种 skip 写法），执行 04 修正。

## 6. 运行影响边界

production_effect=none；broker_action=none。本需求只改变开发工作台和任务记录工具，不改变投资逻辑、数据流、评分、回测或交易边界。
