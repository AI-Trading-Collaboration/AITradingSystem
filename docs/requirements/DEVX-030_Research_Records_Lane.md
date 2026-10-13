# DEVX-030 研究记录快车道

## 1. 背景

- 2026-10-13 的研究耗时评估：一道研究题约 2–4 天，owner 动手 4–5 次。墙钟大多花在排队和等批准上：研究命令和工程批次排同一个 pi 队列，每次研究运行发布前都要等 Claude 审补丁、再转述给电脑会话。
- 研究运行只写评估台自己生成的记录：`research/<题号>/runs/NNNN.json`、`research/<题号>/window_reads.jsonl`，S4b 之后还有 `research/_screens/ledger/NNNN.json`。这些都是 A 区文件，只新增或追加，用 `prev_sha256` 连成链，评估台的不变量（`tests/invariants/test_research_ledger_invariants.py` 第 17 条）检查链和 git 历史。
- 逐次审补丁拦不住记录里的问题：Claude 看到的也只是评估台写的记录。能拦住的是机械规则：提交里只有记录、只新增或追加、链连得上、记录在已发布的代码上生成。
- 方案全文见项目文件 `knowledge/claude/ops/research-fast-lane-2026-10-13.md`。

## 2. owner 决定

owner_decision:TRADING-2565:2026-10-13:research_throughput —— owner（Baker）在研究线程的两张决定卡上作答：

- 2026-10-13 11:12:21Z，卡片问题："第二题开题前,先做通用信号策略和研究记录快车道这两项优化吗?"
  - 选择："两项都做"
  - 选项后果："S4 后加两批;之后每题约 1–2 天,你动手 2–3 次。"
  - 卡片说明里快车道的内容："第二个 pi 会话在单独的工作区跑研究命令、按天提交记录,要改 pi 的运行规则(我写)。"
- 2026-10-13 13:22:47Z，卡片问题："研究记录发布前,用机械检查代替我逐次审查补丁吗?"
  - 选择："机械检查"
  - 选项后果："研究运行跑完、检查通过就由电脑会话发布,我事后再读汇总;每次省 15–30 分钟。"
  - 卡片说明："机械检查指 ship.py 在发布前自动核对四项:提交里只有研究记录;只新增文件或在文件末尾追加;运行记录和筛选账的哈希链完好;git 历史只追加。验证窗口读取不受影响:读之前照旧问你,并且单独一次运行。"

通用信号策略（S4d）的部分记在 TRADING-2565 需求文档。本任务只做快车道。

## 3. 设计

### 3.1 什么算研究记录

`config/gov008_ship.yaml` 的 `research_records`（1.2.0）：

- 编号记录（`numbered`）：`research/*/runs/*.json`、`research/_screens/ledger/*.json`；
- 追加文件（`appended`）：`research/*/window_reads.jsonl`。

每条规则写明记录里哪个字段是生成时的提交（`commit_field`），哪个字段说明有没有本地代码改动（`modified_field`），以及哪些哈希必须等于那个提交里某个文件的哈希（`files_at_commit`，路径里 `{dir}` 是记录所在目录，`{字段}` 取记录里的值，只能是一个文件名）。这些路径都在 A 区。

### 3.2 规划时的机械检查（`ship.py` 的 `plan`）

改动里有研究记录时：

1. 改动里只能有研究记录（"research records ship on their own"）。
2. 编号记录只能新增；文件名是 `NNNN.json`，接着 base 上同一目录的最后一号连续编号；第一条的 `prev_sha256` 是 base 上最后一条的 `chain_digest`（CRLF 按 LF 算，和评估台一致），后面每条接前一条。
3. 追加文件只能新增或在末尾追加：base 上的内容是新内容的前缀，追加部分以换行结尾，每行是 JSON 对象，`prev_sha256` 接上一行。
4. 记录都是普通文件（100644）。
5. 每条记录（追加文件是每个新行）：`commit_field` 是完整的 40 位提交号，并且是 base 的祖先；`modified_field` 必须是 `false`（`unavailable` 也不行）；`files_at_commit` 里的哈希等于那个提交里对应文件的 sha256。运行记录核对策略模块和 `prereg.yaml`；筛选账核对信号模块和规格；验证窗口读取核对 `approval_commit` 上的 `prereg.yaml`。

第 5 条比卡片说明里的四项多：研究工作区里策略模块不在 `git_provenance` 的代码路径（`src`、`config`、`scripts`、`tools`）里，没有它，一个本地改过、没提交的策略也能留下 `code_modified=false` 的记录。

检查通过时门禁只跑 `gates.records`（`pytest tests/invariants -q -k repository_ledgers`，在 HEAD 的树上核对链、编号和 git 历史只追加），`--dry-run` 的输出里有 `research_records`：记录数，只有一个提交时还有补丁指纹 `patch` 和提交信息哈希 `message`。发布照旧用 `publish.ps1 -ExpectedPatch -ExpectedMessage`（A 区，不需要验收记录）；`publish.ps1` 用 main 上的 `ship.py` 和 main 上的规则重做上面全部检查，分支上的代码和规则影响不到自己的发布。

### 3.3 main 前进后的重放（`ship.py --replay-records`）

研究运行期间 main 可能前进。分支只有一个提交、在 main 的某个旧提交上、只含研究记录、通过上面的检查，并且这些记录文件在新 main 上和在旧提交上完全一样时，`--replay-records` 在新 main 上重建这个提交：同样的新增文件和追加行、同样的提交信息（去掉 `Gate:` 行）、同样的作者和时间，补丁指纹不变。任何一条不满足就拒绝，分支不动。这不是 agent 的 rebase：`ship.py` 机械地做，做不到原样就停。

### 3.4 研究工作区

- 第二个工作区 `D:\Work\AITradingSystem_research`（`git worktree`，自己的 `.venv`），平时 detached 停在 main 的提交上。
- `run-brief.ps1` 新增 `-StateRoot <绝对目录>`，传给运行环境的 `AITS_STATE_ROOT`（DATA-023 的 `paths.state_dir`），评估台读主工作区的研究数据和快照；运行记录写在研究工作区自己的 `research/` 下。
- 研究会话（电脑上的 Claude Code，JACK 账户，因为 pi 的受限账户读不了研究数据）用 `run-brief.ps1 -Direct -Repo D:\Work\AITradingSystem_research -StateRoot D:\Work\AITradingSystem` 跑研究 brief，一次一个运行。pi 停在 `--dry-run` 后，研究会话 `--gate`，把研究工作区 detach，再在主工作区（在 main 上、干净、没有 pi 在用）跑 `publish.ps1`。步骤写在 `research/_template/brief.md`。
- 验证窗口每题一次，读之前照旧问 owner，并且单独一次运行。

## 4. 步骤与验收

### 运行 01：登记决定（A 区）

本需求文档。运行 02 的提交要引用 `owner_decision:TRADING-2565:2026-10-13:research_throughput`，pi 的 brief 和验收检查要求这个决定先在 main 的 `docs/requirements` 里。

### 运行 02（C 区，规则本身，按 tree 批）

改动：

- `tools/gov008/ship.py`：3.2 的检查、`--replay-records`、`patch_fingerprint` 可指定 head；
- `config/gov008_ship.yaml`：1.2.0，`research_records` 和 `gates.records`；
- `scripts/agent/run-brief.ps1`：`-StateRoot`；
- AGENTS.md、ship-change 和 research-run 两个 skill、`research/_template/brief.md`、`docs/system_flow.md` 第 5 节；
- 测试：`tests/test_research_lane_ship.py`（新），`tests/test_agent_scripts.py` 加 `-StateRoot` 用例；
- 任务记录，本需求文档的进度。

验收：

- `tests/test_research_lane_ship.py` 用真实规则里的 `research_records` 在临时仓库里跑：正向（记录单独发布、接着 base 编号、追加、筛选账、门禁和按补丁发布、重放）和反向（混入别的文件、跳号、链断、不是 JSON、短提交号、未发布的提交、`code_modified` 为真或 `unavailable`、策略或预注册哈希不对、模块文件不存在、文件名带路径、可执行位、改或删已发布记录、追加文件改旧行或缺换行、重放的各种拒绝）。
- 变异检查：对 `ship.py` 的新代码做 34 个变异（去掉每一项检查、重放保留 `Gate:` 行、丢作者、提交信息换成 CRLF、不更新工作区等），每个都让测试失败。
- 重放出来的提交信息逐字节等于原提交去掉 `Gate:` 行（提交信息以字节写给 git；Windows 上文本模式的 stdin 会把换行变成 CRLF）。
- `-StateRoot`：DryRun 输出 `AITS_STATE_ROOT=<目录>`；相对路径和不存在的目录退出 2。
- 全套 PR 测试通过；按 tree 的老流程、决定记录和补丁发布的现有用例不变。

### 电脑上的准备（发布后，owner 同意再做）

- 建研究工作区和它的 `.venv`；
- 开研究会话（新的远程控制线程），写清只跑研究 brief、只发布研究记录；
- 需要时把 `publish.ps1` 补丁模式和 `ship.py --replay-records` 加进电脑设置的放行规则（owner 自己改）。

## 5. 开放问题

- 多个研究运行同时进行：现在规定一次一个。以后要并行，需要按题分工作区，重放规则不变。
- `research_records` 只认评估台现有的三种记录。以后新增记录类型，要先改规则（按 tree 批）。

## 6. 进度

- 2026-10-13：owner 选"两项都做"和"机械检查"（`owner_decision:TRADING-2565:2026-10-13:research_throughput`）。运行 01 登记本需求；运行 02（C 区）实现第 3 节，云端全套测试通过后按 tree 请 owner 批准。
- 2026-10-13：运行 02（C 区）的改动在云端测好：`tests/test_research_lane_ship.py` 47 条通过；对 `ship.py` 新代码做的 34 个变异都让测试失败；`-StateRoot` 用例在 PowerShell 7 下通过，Windows PowerShell 5.1 由电脑上的门禁覆盖；全套 PR 测试 3148 passed，另有 22 条在云端失败，改动前也一样（IBKR 模拟和两条按日期的报告用例）。改的是规则本身，按 tree 请 owner 批准。

## 7. 运行影响边界

production_effect=none；broker_action=none。只改研究记录的发布方式和研究运行的工作区，不改评估台、研究数据、投资逻辑、评分、回测或交易边界。
