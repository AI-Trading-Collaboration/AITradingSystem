# DEVX-025 pi 工作台

## 1. 背景

GOV-008 迁移后，pi 将承担日常开发工作台职责。试跑暴露出两类偏差：一是 AGENTS.md 中的工程规则需要在 pi 会话内更早、更明确地阻断（例如 C 区确认、系统 Python、未排除 dirty exclusion 的全仓库 git 命令、未读 operations runbook 即运行 `aits ops`）；二是任务记录和交付流程需要减少手工编辑和重复门禁成本。

本需求覆盖“迁移后 pi 用法方案第三版”的第一阶段工作台建设，拆成三次运行，避免把 B 区守卫与 C 区 ship 语义改造混在同一次变更中。

## 2. owner 决定

owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"把 ship 拆成'受限账户跑门禁、你的账户只发布'吗"选择了"在 1a 拆"（选项后果："门禁只在受限账户里跑一次，每次发布省约 10 分钟，pi 写的代码不在你的账户里运行"）。在运行 03 执行。

owner_decision:DEVX-025:2026-10-11:manual_agent_task_exception —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"同意把 pi 的手动计划任务登记为运维规则的例外吗？"选择了"同意例外"（选项后果："注册这个手动任务，写成 owner 决定，并在运维手册里登记；以后启动 pi 不用每次输密码。"）。在运行 05 登记到运维手册 9.2 节和 periodic-operations skill。

owner_decision:DEVX-025:2026-10-11:tighten_d_work_acl —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"收紧 D:\Work 的权限，去掉'所有登录用户'的修改和读取吗？"选择了"收紧"（选项后果："D:\Work 只留管理员、SYSTEM 和你的账户；aits-agent 只拿到自己的三个目录，以及仓库 .git 的只读权限。"）。对追问"你现在还在 D:\Work 里用 Codex 吗？"回答"还用"，所以 CodexSandboxUsers 组保留 D:\Work 的修改权限。D:\Work 本身由 owner 用管理员身份手工收紧；运行 05 让安装脚本给受限账户的三个目录关闭继承。

owner_decision:DEVX-025:2026-10-11:model_routing —— 2026-10-11 owner（Baker）在项目线程里提出 pi 的云端调用统一走 OpenAI 订阅登录（不用 API key），云端模型用 gpt-6.1-sol；在决定卡片上，对问题"选定 pi 云端模型 gpt-6.1-sol 的默认推理强度"选择了"high"（选项后果："所有云端运行都用高强度，更稳，但更慢，额度消耗也更快。"）；对第 7 节的本地、云端分工约定回复"好的，先这样约定"。在运行 08 执行。

owner_decision:DEVX-025:2026-10-11:agent_read_base_python —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"允许 aits-agent 读取你用户目录里的 Python 安装吗？"选择了"授权读取"（选项后果："给 aits-agent 这一个目录的只读权限，你运行一条命令就好，随时可以撤销；它仍然读不到你用户目录的其他部分。"）。owner 当天已手工授权；运行 08 让安装脚本对 agent venv 的底层 Python 目录做同样的授权。

owner_decision:DEVX-025:2026-10-11:agent_task_launch —— 2026-10-11 owner（Baker）在项目线程的决定卡片上，对问题"以后由我直接启动受限账户的计划任务吗？"选择了"让我启动"（选项后果："我把请求放进队列后，自己运行 schtasks 启动这个任务，你不用每次动手。"）。授权只覆盖用 `schtasks /Run` 启动"AITradingSystem pi agent"这一个任务，不包括修改、新建或删除任务。

owner_decision:DEVX-025:2026-10-12:signoff_policy —— 2026-10-12 owner（Baker）在项目线程里指出，每次发布前复制验收记录、点过卡片再照抄原话已经成了形式，要求统一考虑"到底什么情况需要我来签字"。Claude 整理 10-10 以来各线程里的 68 次请求，提出"签决定、不签执行"的方案，owner 打字同意："同意按签字方案调整：GOV-009-03 跑 gate 并发布到 main；GOV-009-04、05 按结果 tree 5150c117、5cd32350 事先批准，审查和 gate 通过后直接发布到 main；另开任务把方案落进规则和发布脚本。"方案要点：

- owner 签的是决定：方向和范围、影响投资解释的 C 区改动、规则和门禁本身（AGENTS.md、ship 规则、pi 守卫、`publish.ps1`）、R2/R3、电脑上的权限，以及结果偏离已批预期的情况。签的时候写清范围和预期结果；执行结果和预期一致就不再问。
- A/B 区发布不再需要 owner 的验收记录，由 Claude 审查、门禁和 tree 核对放行。
- C 区结果事先能算出时，决定里写明预期 tree，按 tree 事先放行；pi 自己写代码的 C 区改动仍要一次验收记录。

在运行 11、12 执行。

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

1. 按 owner 决定把 `tools/gov008/ship.py` 拆成 `--gate`（跑门禁、写 `Gate:`、不动 main）与 `--publish`（不跑测试，核对 `Gate:`、tree、区划和 Owner-Decision 后快进 main）的受控路径，保留 `--execute` 一步走。
2. 新增 owner 账户使用的 `scripts/agent/publish.ps1`，从 main 取 `ship.py` 到仓库外临时目录执行发布，只发布验收记录里 tree 对应的提交，避免运行分支代码。
3. 更新文档和测试（不改 ship policy），保证门禁只跑一次且发布只接受已验证树。
4. 为 C 区变更提交引用 `Owner-Decision: owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split`。

验收：

- C 区门禁和 owner decision 要求不被削弱。
- gate artifact 能证明 tested tree，publish 只 fast-forward 到该树。
- 原 `ship --execute --push` 的安全语义有等价或更强替代。

### 运行 04：工作台小修

步骤：

1. `accept_checks.py` 的 `Gate:` 读取、格式校验、区划核对和 C 区 Owner-Decision 收集对齐 `ship.py`，并补认 Python/TypeScript 跳过写法与 `.test.ts` 测试删除。
2. `agent-run.ps1` 改为运行中按字节流式写入 `events.jsonl`，stderr 同步落盘，避免 stdout 解码改写中文字节。
3. `.pi/extensions/aits-guard` 拦截 pi 写运行目录四个受管产物，修正 heredoc 正文误拦，并只把真实测试命令记入报告。
4. `publish.ps1` 对验收 tree 和哈希比较改用区分大小写的匹配与相等判断，拒绝大写 tree。
5. 补充上述行为的 Python、PowerShell 和 node:test 覆盖，保持发布流程语义不变。

验收：

- 小修不触碰 C 区 ship policy/ship.py，`ship.py --dry-run` 显示本次为 B 区。
- 新增验收脚本、启动脚本、守卫和发布脚本回归测试通过。

### 运行 05：受限账户安装脚本修正与例外登记

步骤：

1. 登记手动计划任务唯一例外和收紧 `D:\Work` 的 owner 决定，并把例外写入运维手册和 periodic-operations skill。
2. 修正 `setup-agent-account.ps1` 的权限位判断、重复问题报告、`-CheckOnly` 退出码、`-WhatIf` 只读预览，以及受限账户三个目录关闭继承后的 ACL。
3. 补充安装脚本函数级和 `-CheckOnly`/`-WhatIf` 覆盖，验证只读权限不误报可写、重复 ACE 只报一次、预览不改系统状态。

验收：

- 新旧测试通过。
- `ship.py --dry-run` 显示本次为 B 区。
- 安装脚本仍只由 owner 在 1b 以管理员身份运行完整模式。

### 运行 06：1b 前置（固定 pi 路径、显式信任、任务启动权限）

步骤：

1. 安装脚本只允许 Windows PowerShell 5.1，增加 `-PiCommand` 并在完整模式前检查固定 pi 路径存在且目录不被普通用户写入。
2. 计划任务命令显式传入 `-PiCommand`，注册后给 owner SID 只读和执行权限；`agent-run.ps1` 每次 pi 运行加 `--approve`，`run-brief.ps1 -DryRun` 同步展示。
3. 补充安装脚本、npm cmd-shim 解析、`--approve` 参数和预览输出覆盖，保持安装完整模式只由 owner 在 1b 管理员执行。

验收：

- 新旧测试通过。
- `ship.py --dry-run` 显示本次为 B 区。
- 安装脚本仍只由 owner 在 1b 以管理员身份运行；1b 时实测受限账户下守卫生效、owner 不提权能启动任务。

### 运行 07：权限检查直接读 SID

步骤：

1. 安装脚本的 `Test-PathAcl` 改为直接按 SID 读取 ACL 条目，只在报告问题时尝试把 SID 翻译为帐户名。
2. 补充 `-CheckOnly` 和 `-WhatIf` 覆盖，验证 Program Files 常见的应用程序包 SID 读权限不会触发 `IdentityNotMappedException` 或误报。

验收：

- 新旧测试通过。
- `ship.py --dry-run` 显示本次为 B 区。
- owner 在 1b 重新运行安装脚本时，pi 目录的权限检查不再抛错。

### 运行 08：模型配置、任务启动检查和小修

步骤：

1. 云端模型改为订阅登录的 `openai/gpt-6.1-sol`，在 `agent_config.json` 配置默认推理强度 high。
2. `agent-run.ps1` 启动 pi 时从子进程环境去掉所有 `*_API_KEY`，不改用户和系统环境变量，订阅凭据不可用时不回退到环境 API key。
3. `run-brief.ps1 -Fix` 在本地出错、aborted、没有回复或一次退回后自动升级云端；默认推理强度按有效模型读取，任务单显式值优先。
4. 队列模式保留 `schtasks /Run` 输出和退出码，并检查请求在 `PickupSeconds` 内被取走；启动失败或未取走时删除本次队列文件并退出 2。
5. `brief.py` 校验 thinking 的合法取值，拒绝无效推理强度。
6. `accept_checks.py` 按 YAML 解析后的 status/notes 比较，允许仅引号形式改变的追加，拒绝改写、删空和非映射任务文件。
7. 安装脚本要求账户密码，并按 owner 授权给 agent venv 的底层 Python 目录只读和执行权限；只通过 `-WhatIf`/`-CheckOnly` 验证，不执行安装完整模式。

验收：

- 新旧测试通过（含 pi 进程里没有 `*_API_KEY` 的用例）；没有 PowerShell 时按现有辅助函数跳过。
- `ship.py --dry-run` 显示本次为 B 区，不执行 gate 或发布。
- 用真实配置 DryRun 时，云端任务单的模型参数是 `openai/gpt-6.1-sol:high`，本地默认值不追加 thinking 冒号。

### 运行 09：守卫按引号切分命令，安装脚本补齐

步骤：

1. 守卫按引号和注释切分命令：`&&`、`||`、`;`、`|`、`|&`、后台 `&` 和换行都是分隔，引号和注释里的不算；heredoc 正文是否交给 shell 按所在的段判断，修正 `|` 或 `||` 后面紧跟 shell 时少看一个字符的问题。
2. 找命令词时跳过 `env`、`exec`、`time` 的选项（如 `env -i`、`env -u X`），`command -v` 只查名字、不算执行；系统 Python 按命令词判断，覆盖 `python3`、带路径的系统 Python 和前置变量赋值，项目 `.venv` 下的解释器照常放行。
3. 全仓库 git 规则认出 `git -C`、`-c`、`--no-pager` 等全局选项，并覆盖 `git add -u`/`--all`、`git commit -a` 和不带路径的 `git stash`；强推、rebase 等禁止命令带全局选项也拦。
4. 安装脚本在受限账户缺少"作为批处理作业登录"权利时补上，保留现有持有者；agent venv 按主 venv 的 `pip freeze --exclude-editable` 逐包同版本安装，再以 editable 方式装项目，最后核对两边一致。只通过 `-WhatIf`/`-CheckOnly` 验证。

验收：

- 新旧测试通过；新增的守卫用例在修改前的守卫上失败。
- `ship.py --dry-run` 显示本次为 B 区。
- owner 以管理员身份重跑安装脚本后，agent venv 与主 venv 一致，在 agent clone 里复测 PR 测试。

### 运行 10：受限账户的信任边界

owner 以管理员身份重跑安装脚本后，只读核对发现：脚本以管理员身份执行 agent venv 里的 python 和 pip，并在 agent clone 里做 editable 构建，而这些位置 aits-agent 都能改，它放进去的代码会在下一次重跑时以管理员身份运行；验收记录 acceptance.json 放在运行目录里，aits-agent 也能写。

步骤：

1. 安装脚本每次从底层 Python 重建 agent venv：先在验收根目录里建好只给 aits-agent 读和执行权限的目录，再改名放进 agent clone；按主 venv 的 `pip freeze --exclude-editable` 安装，项目源码通过 `.pth` 文件加入，不再在 agent clone 里做 editable 构建；requirements 文件写在验收根目录。旧 venv 用 `rmdir /s` 删除，不跟随里面的链接。管理员执行的 Python 都带 `-I`。
2. 三个 agent 目录的权限已经符合预期时不再重设，避免 icacls 把权限传播进 aits-agent 建的子目录；计划任务里 owner 的权限条目按 SID 判断，重跑不再重复添加。
3. 新建验收根目录 `D:\Work\AITradingSystem_agent_reviews`（`agent_config.json` 的 `review_root`），只有管理员、SYSTEM 和 owner 能访问。`publish.ps1` 只读 main 上配置的验收根目录里的 `<运行名>\acceptance.json`，不再读运行目录里的同名文件，`-AcceptanceFile` 指向运行目录时拒绝；`run-brief.ps1` 从同一位置统计退回次数；看板加 `--review-root`。
4. 约定：owner 账户不执行 agent clone 和 agent venv 里的程序，也不在 agent clone 里跑会刷新索引的 git 命令（status、diff）；受限账户产出的代码先经 Claude 审阅，再在 owner 账户里跑门禁。

验收：

- 新旧测试通过；新增的发布、启动和看板用例在修改前的脚本上失败；PowerShell 用例在 Windows PowerShell 5.1 下通过，包括重建 venv 时不跟随链接、目录权限判断。
- `ship.py --dry-run` 显示本次为 B 区。
- 发布后 owner 以管理员身份重跑安装脚本：验收根目录建好，agent venv 被重建并与主 venv 一致；之后在 agent clone 里复测 PR 测试。

### 运行 11：签字方案落进发布脚本和守卫

依据 `owner_decision:DEVX-025:2026-10-12:signoff_policy`。

步骤：

1. `publish.ps1` 必须带 `-ExpectedTree`（Claude 审查过的 tree），分支的 tree 必须与它相同。分支最后一个提交的 `Gate:` trailer 是 A 或 B 区时不再读验收记录，`ship.py --publish` 再按 main 的规则核对区划；C 区或没有 `Gate:` trailer 时，仍要验收根目录里最后一条通过的记录，且记录的 tree 与 `-ExpectedTree` 相同。去掉 `-AcceptanceFile` 参数。
2. 守卫：owner 决定已登记在 main 的需求文档里，且命令改到的 C 区路径都在本次允许路径内时，认可这些改动并记入报告的 C 区确认；否则照旧锁定。pi 不能运行 `ship.py --gate`、`--execute`、`--publish` 和 `publish.ps1`。识别测试命令时跳过 `env` 等包装和前置的变量赋值。`guard.test.ts` 开头清掉继承来的 `AITS_PI_*` 变量。
3. ship-change skill 和 C 区任务单模板改成新的发布流程。

验收：

- 新旧测试通过；新增的发布和守卫用例在修改前的脚本上失败；PowerShell 用例在 Windows PowerShell 5.1 下通过。
- C 区改动的 `Gate:` trailer 被改成 B 区时发布失败，main 不动。
- `ship.py --dry-run` 显示本次为 B 区。

### 运行 12：签字原则写进 AGENTS.md（C 区）

步骤：

1. AGENTS.md 写进签字原则和新的两步发布流程。
2. `config/gov008_ship.yaml` 把 pi 守卫（`.pi/extensions/aits-guard/**`）和 `scripts/agent/publish.ps1` 列入 C 区，以后改它们需要 owner 决定。
3. 提交带 `Owner-Decision: owner_decision:DEVX-025:2026-10-12:signoff_policy` trailer。

验收：

- C 区门禁和 owner 决定的要求不被削弱。
- `ship.py --dry-run` 显示本次为 C 区。
- 发布后 owner 在电脑上 Claude Code 的设置里加一条只匹配 `publish.ps1` 的 `permissions.allow` 规则。

### 运行 13：pi 进程去掉所有像密钥的环境变量

起因：2026-10-16 OPS-086-07 的 pi 为找车道变量运行了 `printenv | grep -E 'PI_|LANE|RUN_DIR'`，`PI_` 匹配到 `TIINGO_API_TOKEN` 里的 `API_`，这个变量的值进了工具结果，随对话发给了云端模型。运行 08 只去掉名字以 `_API_KEY` 结尾的变量。

步骤：

1. `agent-run.ps1` 启动 pi 时从子进程环境去掉名字含 `KEY`、`TOKEN`、`SECRET` 或 `PASSWORD`（不分大小写）的所有变量；`GIT_CONFIG_KEY_<n>` 除外（它是 git 设置名，去掉后有 `GIT_CONFIG_COUNT` 时 git 报错）。照旧不改用户和系统环境变量，`AITS_PI_*` 仍由启动脚本按本次运行重新设置。
2. 测试覆盖 `_API_TOKEN`、大小写混合的 `_Secret`、`_PASSWORD`、名字中间的 `_KEY_` 和保留的 `GIT_CONFIG_KEY_<n>`。

验收：

- 新旧测试通过；这条用例在修改前的脚本上失败；没有 PowerShell 时按现有辅助函数跳过。
- `ship.py --dry-run` 显示本次为 B 区。

## 4. 开放问题

- 已决定：受限账户的手动计划任务作为 periodic-operations“不要注册第二个计划任务”规则的唯一例外（`owner_decision:DEVX-025:2026-10-11:manual_agent_task_exception`），运行 05 已登记。

## 5. 进度

- 2026-10-11：运行 01 开始；任务记录、需求文档、守卫加固、`append-note` 和 prompt 模板纳入本次变更。
- 2026-10-11：运行 01 验收退回一次（拦截终止运行、报告漏掉已提交改动、测试缺反向用例等），执行 02 修正。
- 2026-10-11：运行 02 增加 brief 解析器、pi 启动/受限账户脚本、机械验收脚本和 runs 看板，并补充对应测试。
- 2026-10-11：运行 02 验收退回一次（三个 PowerShell 脚本在 5.1 下跑不通、验收脚本的区划和 Gate 解析与 ship 不一致等），执行 02 修正。
- 2026-10-11：运行 02 第二次验收退回（真实 pi 的启动解析、stdin 带 BOM、安装脚本两处），执行 03 修正。
- 2026-10-11：运行 02 第三次验收退回（看板重复计数工具调用、safe.directory 路径没转换、accept_checks 漏认几种 skip 写法），执行 04 修正。
- 2026-10-11：运行 03 把 ship 拆成 gate 和 publish，新增 publish.ps1（owner 账户只发布，运行 main 上的 ship.py，不跑测试）。
- 2026-10-11：运行 03 验收退回一次（publish 不跑门禁的测试不成立、拒绝用例断言太宽、验收记录没有锁定 tree、读子进程输出可能卡住），执行 03 修正。
- 2026-10-11：运行 04 小修：accept_checks 门禁核对对齐 ship.py、补认跳过写法；agent-run 运行中写 events.jsonl；守卫拦截写运行目录产物、heredoc 误拦和测试误记；publish.ps1 区分大小写。
- 2026-10-11：运行 04 验收退回一次（守卫拆分命令的正则少了 `|`、测试记录用例不成立、heredoc 接收者判断和 `<<<`、skip 选项写法），执行 02 修正。
- 2026-10-11：运行 04 验收第二次退回（heredoc 经 `env`、多级管道或同一行后面的 shell 执行时正文被剥掉；删测试用例缺断言；测试文件多余空行），执行 03 修正。
- 2026-10-11：运行 04 发布（main e1c489055）。运行 05：登记手动计划任务例外和收紧 D:\Work 两项 owner 决定；安装脚本修正权限位判断、重复报告、CheckOnly 退出码和 WhatIf，并给受限账户的三个目录关闭继承。
- 2026-10-11：运行 05 发布（main d3c1f8d40），owner 收紧 D:\Work。运行 06：安装脚本只认 Windows PowerShell 5.1，计划任务固定使用管理员安装的只读 pi，并允许 owner 不提权启动；agent-run 每次显式信任工作目录，保证受限账户下守卫照常加载。
- 2026-10-11：运行 06 发布（main 17fda1f49）。1b 第一次运行安装脚本时，权限检查遇到 Program Files 目录上的"所有应用程序包"条目抛错，脚本在建账户之前停下，没有改动。运行 07：权限检查改为直接按 SID 读取。

- 2026-10-11：运行 07 发布（main f328dd127）。1b：owner 以管理员身份完成安装脚本、封住 D 盘其他目录、以 aits-agent 用订阅登录 pi；只读核对确认 aits-agent 读不到 owner 的用户目录和 D 盘其他目录。核对时发现三处问题，都已处理：计划任务的启动请求返回 SUCCESS，但任务没有运行，原因是 aits-agent 没有"作为批处理作业登录"权利（任务日志错误 0x80070569），owner 已手工添加，任务已能以 aits-agent 启动；agent venv 的底层 Python 在 owner 的用户目录里，aits-agent 读不到，owner 已授权读取；pi 1.1.0 的订阅登录凭据在 `openai` 下面，模型要写 `openai/…`，不是 `openai-codex/…`。owner 授权 Claude 直接启动这个计划任务。GOV-008-01（第一次用本地模型）发布（main fa0ab59d6）。运行 08：模型配置和约定、pi 进程去掉 API key 环境变量、任务启动检查、notes 按解析值比较、安装脚本小修。
- 2026-10-11：运行 08 发布（main ce82757b8）。1b 隔离核对（DEVX-025-iso01）通过：受限账户下守卫照常拦截，aits-agent 读不到 owner 的用户目录和 D 盘其他目录，写不进 owner 的仓库目录和 `.git`，底层 Python 出站被防火墙拦住。核对时在 agent clone 跑 PR 测试，有 263 个失败、43 个错误，原因是 agent venv 只装了项目和几个开发工具（36 个包，主 venv 有 92 个），版本也和主 venv 不同，不是隔离问题。运行 09：守卫按引号切分命令并修正几处规则，安装脚本补批处理登录权利、agent venv 与主 venv 同版本。
- 2026-10-11：运行 09 发布（main ead6a6291）。owner 以管理员身份重跑安装脚本，agent venv 与主 venv 同版本（89 个包）。以 aits-agent 复测（DEVX-025-iso02）：agent clone 从主仓库拉取被 git 的所有权检查拦下，未刷新，在原提交上测；PR 测试 8022 个通过、24 个失败，其中 23 个依赖主仓库里未纳入 git 的 `outputs/` 证据文件，1 个是 CLI 输出按控制台宽度折行（受限账户的临时路径更长），都与依赖无关；门禁继续在 owner 账户里跑，受限账户只跑与改动相关的测试。运行 10：安装脚本不再执行受限账户能改的程序，验收记录移到受限账户写不到的验收根目录。
- 2026-10-12：运行 10 发布（main d27674d09），owner 重跑安装脚本，验收根目录建好；此后 OPS-083、GOV-009、DATA-022 的 pi 运行都按两步式发布。owner 同意签字方案（`owner_decision:DEVX-025:2026-10-12:signoff_policy`）。运行 11：`publish.ps1` 按 Claude 审查的 tree 发布，A/B 区不再要验收记录；守卫认可已登记 owner 决定下脚本对允许路径内 C 区文件的改动，不让 pi 自己跑门禁和发布。运行 12（C 区）把签字原则写进 AGENTS.md，守卫和 `publish.ps1` 进 C 区。
- 2026-10-12：运行 11 发布。运行 12（C 区）：AGENTS.md 写进签字原则和新的两步发布流程，`config/gov008_ship.yaml` 把 pi 守卫和 `publish.ps1` 列入 C 区。
- 2026-10-16：OPS-086-07 的 pi 用 `printenv | grep` 找车道变量时，把 `TIINGO_API_TOKEN` 的值打进了工具结果，随对话发给了云端模型（没进 git；owner 换 token）。运行 13：pi 子进程去掉名字含 KEY、TOKEN、SECRET、PASSWORD 的环境变量。

## 6. 运行影响边界

production_effect=none；broker_action=none。本需求只改变开发工作台和任务记录工具，不改变投资逻辑、数据流、评分、回测或交易边界。

## 7. 模型选择约定

依据 `owner_decision:DEVX-025:2026-10-11:model_routing`。Claude 写任务单时按下面的规则填 `model` 和 `thinking`。

- 云端模型是 `openai/gpt-6.1-sol`，走 owner 的 ChatGPT 订阅登录，不用 API key。默认推理强度 high（`agent_config.json` 的 `default_thinking.cloud`），任务单可以单独指定。
- 计费方式由 pi 用的凭据决定，和 provider 名字无关：每个账户的 pi 都只用 `/login` 里的 Sign in with ChatGPT 登录 `openai`，不在 pi 里保存 API key；登录后在交互界面发一句话，确认底部状态栏显示 `(sub)`。`agent-run.ps1` 启动 pi 时去掉名字含 `KEY`、`TOKEN`、`SECRET` 或 `PASSWORD` 的环境变量（运行 13，`GIT_CONFIG_KEY_<n>` 除外），所以订阅凭据不可用时运行直接报错，不会改走按量计费，pi 打印环境变量时也不会把密钥发给模型。
- 用本地模型（`model: local`），要同时满足三条：
  - A 区，或者 B 区里的机械改动：记录和文档、任务卡、搬移、重命名、删除、跑检查和测试、小范围只读调查；
  - 改动是确定的：由 Claude 事先测好的脚本或逐条改动清单完成，并且有预期的 tree 哈希或明确的检查结果；
  - 要读的文件和测试输出不多，本地模型的上下文装得下。
- 用云端模型（`model: cloud`）：pi 要自己写或改代码、写或改测试、排查原因、做设计判断、跨多个文件重构、读大段日志；所有 C 区改动（另外仍需 owner decision）。
- 升级：本地模型出错（stopReason 为 error 或 aborted，或者没有回复），或者结果被退回一次，修正运行直接换云端，不在本地反复重试。`run-brief.ps1 -Fix` 按这条自动切换。
- 订阅额度用完时等额度恢复；改用 API key 之前先问 owner。
- 两种模型的审核、门禁、发布标准完全相同。
- 跑满大约 10 次之后，按模型复盘一次：一次通过率、返工轮数、越界改动、工具错误和耗时，再决定要不要放宽本地模型的范围。和 DEVX-028（pi 运行回放审查）一起做。
- 本地模型的输入不出机器。靠近凭据或券商配置的只读调查，本地模型能胜任时优先用本地。
