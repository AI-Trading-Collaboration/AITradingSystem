# DEVX-031 pi 并行车道

## 1. 背景

- 到 2026-10-14，pi 队列一次只跑一批：pi 在主工作区跑，跑完、审完、批完、发布完，才开下一批。
- 真正花时间的不是 pi 本身。以 2026-10-14 的 TRADING-2565-05 为例（时间为 JST）：
  - pi 跑到 dry-run 用了 24 分钟；
  - 审查用了 20 分钟；
  - 之后约 3 小时在等批准和避开 12 点日报，期间后面的批次按约定都不能开始。
- 不算等 owner 的时间，一批在电脑和审查上要 60–75 分钟。其余时间花在：
  - 传文件；
  - 电脑会话逐条手工核对哈希；
  - 重复的全量测试；
  - 两轮审查。
- 串行的硬约束（main 909137b3 上核对过）：
  - `publish.ps1` 只能在主工作区运行；
  - `ship.py` 从不把改动挪到新的 main 上，只有研究记录有 `--replay-records`；
  - AGENTS.md 不许 agent 做 rebase；
  - 补丁指纹带 3 行上下文，main 在附近改过几行，指纹就会变；
  - 日报窗口在 `run-brief.ps1` 里固定 70 分钟，但一次日报实测约 15 分钟。
- 方案全文见项目文件 `knowledge/claude/ops/pi-queue-parallel-2026-10-14.md`（第 2–6 节）。

## 2. owner 决定

owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes：owner（Baker）在项目线程「pi 队列并行化」中的三次表态：

- 2026-10-14 04:33:34Z 问："有办法改造为并行吗"。之后 04:42:02Z 问车道能否更多："本地模型同一时间只能处理一个队列，云端其实可以处理多个把，所以我们的车道数量是否可以更多一些？"
- 2026-10-14 04:52:53Z，决定卡片：
  - 问题："把 pi 的分支推到 GitHub 供云端审查吗?"
  - 选择："推"
  - 选项后果："pi/* 分支会出现在公开仓库里，云端直接拉取审查，不再传 bundle，审完发布后分支删除。"
- 2026-10-14 05:01:21Z，回复 Claude 的重构清单："好的，这样改下吧"。那份清单包括：
  - 门禁提前；
  - 三个固定脚本代替手写长指令；
  - 只审一次；
  - 日报窗口按实际结束放开；
  - 同一条链叠着跑；
  - 把"挪到新 main"做成 `ship.py` 的命令，和脚本、日报窗口合成一个规则批次，由 owner 按 tree 批一次。

## 3. 设计

### 3.1 车道

- 车道是主工作区的 git worktree，各有自己的 `.venv`。
  - 名字和路径写在 `scripts/agent/agent_config.json` 的 `lanes`：`lane1`、`lane2`、`lane3`，即 `D:\Work\AITradingSystem_lane1`…`lane3`。
  - 用 `lane.py setup` 建。
- pi 在车道里跑：`run-brief.ps1 -Direct -Repo <车道>`。默认不带 `-StateRoot`：带上 `AITS_STATE_ROOT=<主工作区>` 会让 pi 自己在车道里跑的测试去读主工作区的数据（2026-10-14 第一次真车道运行里，`test_qqq_long_history` 失败，`test_secret_hygiene` 扫主工作区 outputs 太久，pi 用了 72 分钟而不是约 35 分钟）。需要读主工作区研究数据的批次，在 `lane.py start` 加 `--with-state-root`。门禁和检查本来就不带它。
- 主工作区只用来发布。
- 一条车道从 `start` 起被一个运行占用，直到它发布或 `release`。占用状态写在 `<runs_root>\lanes\<车道>.json`。

### 3.2 `scripts/agent/lane.py`

电脑会话只调用它，每个动作打印一行 JSON。退出码：0 成功；1 某一步失败；2 什么都没做就拒绝。

| 动作 | 做什么 |
|---|---|
| `setup --lane L` | 建车道 worktree，detached 在 main。用主 `.venv` 的基础 Python 和同样的包建 `.venv`，代码用 `.pth` 指向车道的 `src`；最后核对 HEAD、包、导入路径和工作区是否干净 |
| `start --lane L --run R [--fix 文件]` | 运行目录要有 `brief.md` 和 `SHA256SUMS`，并逐个核对哈希。brief 要用 main 的 `brief.py` 校验，并且就是这个运行的。车道空闲、干净、detached 时，先挪到 main 再启动 pi，一直等到 pi 停下 |
| `report --lane L --run R` | 依次做：main 版 `accept_checks`；车道里分支自己的 `ship.py --gate`；`accept_checks --gate`；main 版 `ship.py --dry-run`，取 tree、区和指纹。然后把 `pi/*` 分支推到 origin 供审查，结果写进 `lane-report.json` |
| `refresh --lane L --run R` | main 前进后，先跑 main 版 `ship.py --replay`，再做一遍 `report` 的检查、门禁和推送 |
| `publish --lane L --run R` + 审过的指纹 | 指纹是三选一：`--expected-tree`；`--expected-patch` 加 `--expected-message`；`--expected-change` 加 `--expected-message`。main 前进了就先 `refresh`，但按 tree 批的批次不 refresh，而是拒绝。然后在主工作区跑 `publish.ps1`，删掉远端分支，把车道放回 main |
| `release --lane L --run R` | 不发布，放弃车道。分支保留 |
| `status` | 列出各车道、它们的运行、main 和日报窗口 |

`lane.py` 自己执行的规则：

- 两条车道的 `allowed_paths` 不能重叠。按每个 glob 通配符前的字面前缀判断，宁严勿松。渲染出来的任务总表除外，因为 `--replay` 会重新渲染它。
- 本地模型一次只跑一个运行。
- 日报窗口关着时，不启动、不跑门禁、不发布。
- 一次只发布一批（`<runs_root>\lanes\publish.lock`）。
- 车道只会挪到 main 或自己运行的分支上。
- 检查脚本、`ship.py` 和 `brief.py` 都用 main 上的版本，复制到临时目录里跑。只有门禁跑分支自己的 `ship.py`，和原来在主工作区一样，而发布时 `publish.ps1` 用 main 版重新核对一遍。

### 3.3 `ship.py --replay` 和改动指纹

- `--replay`：分支上只有一个提交，而且直接接在 main 的某个旧提交上。它用 `git merge-tree --write-tree --merge-base=<父提交>` 在新 main 上合并：
  - 不碰工作区；
  - 有冲突就拒绝。例外是 `docs/task_register*.md`：取新 main 的版本，再用 `tools/tasks.py render` 重新渲染；
  - 新提交保留作者、去掉 `Gate:` 行的提交信息，以及改动指纹；
  - 之后要重新跑门禁；
  - 只含研究记录的提交拒绝，那种提交照旧用 `--replay-records`。
- 改动指纹（`change`）：和补丁指纹算法相同，只是 diff 不带上下文（`--unified=0`），所以只看每个文件加了哪些行、删了哪些行。改动的位置对不对，由重跑的门禁检查。
- `--dry-run` 对单提交分支打印 `fingerprints`：`patch`、`change`、`message`。
- `--publish --expected-change C --expected-message M` 和 `publish.ps1 -ExpectedChange` 是补丁模式的另一种写法，其余检查不变：门禁 trailer、区、Owner-Decision、决定记录或按 tree 的验收。

### 3.4 日报窗口（`scripts/agent/daily_window.py`）

- `daily_windows_jst` 里的窗口加上 `label`：`PRIMARY`、`RESCUE`，对应 OPS-082 调度包装脚本的窗口名。
- 窗口内，只要当天这个窗口的日报已经在 `daily_scheduler_dir` 写出 `<日期>_<label>[_n].summary.json`，而且写出时间不早于窗口开始，就提前放开。
- 没有 summary、没有目录、没有 label，窗口照旧关到结束。
- `run-brief.ps1` 和 `lane.py` 都用它。
- OPS-086 把运行副本移走时，要同步改 `daily_scheduler_dir`。

### 3.5 推送分支

- 只有 `lane.py report` 和 `refresh` 会推送，用 `--force-with-lease`，只推 `pi/*` 运行分支。
- `publish` 成功后删除远端分支。
- pi 本身照旧不能推送，pi 守卫没有改。

## 4. 验收

- `tests/test_parallel_lanes_ship.py`：
  - 改动指纹不受附近改动影响，补丁指纹会受影响；
  - replay 保留改动、提交信息和作者，不带 Gate；
  - 冲突时拒绝，分支不动；
  - 任务总表重新渲染；
  - 各种拒绝情形；
  - `--dry-run` 的指纹；
  - 按 change 发布和它的拒绝情形；
  - 命令行参数规则。
- `tests/test_agent_publish.py`：main 在附近改过行之后，按补丁发布被拒绝，replay 后按改动发布成功；`-ExpectedChange` 的参数校验。
- `tests/test_agent_lane.py`：
  - glob 重叠判断；
  - 日报窗口按 summary 提前放开，以及各种反例；
  - 两条车道并行，路径重叠时拒绝；
  - 启动时的各种拒绝都不会启动 pi；
  - 本地模型只能一个；
  - `report` 依次做检查、门禁、推送；
  - `refresh` 后改动指纹不变；
  - 两条车道先后发布，第二条自动 replay，用真的 `publish.ps1`；
  - 按 tree 批的批次在 main 前进后拒绝；
  - 指纹不符时什么都不动；
  - 发布锁、`release`、`status`；
  - 真实配置里的窗口标签和 summary 目录。
- 全套 PR 测试通过。规则 1.3.0 把 `scripts/agent/lane.py` 放进 C 区和 `decision_record_excluded`。

## 5. 步骤

| 步 | 内容 | 状态 |
|---|---|---|
| 01 | 本需求文档（A 区） | 已发布 |
| 02 | `ship.py`、`publish.ps1`、`lane.py`、`daily_window.py`、`run-brief.ps1`、`agent_config.json`、规则 1.3.0、AGENTS.md、测试、任务卡（C 区，按 tree 批） | 本批 |
| 03 | 电脑准备：owner 的设置里加车道放行规则；`lane.py setup` 建 lane1–lane3 | 02 发布后 |
| 04 | 试跑：两批不相干的 B 区批次同时跑；记录测试用时和 CPU 争用；决定开 2 条还是 3 条车道 | 03 后 |

## 6. 不在本任务里

- 同一条链叠着跑（上一批审查通过后，下一批就基于它的分支开跑）：车道稳定后另立任务。
- 按 tree 批的批次改成认补丁：会动 owner 定的边界，不做。
- pi 搬到云端容器：不做。
- pi 的受限账户（DEVX-025 1b）：车道照旧由 JACK 账户运行。

## 7. 进展

- 2026-10-14：需求和实现由 Claude 写（规则和门禁本身由 Claude 端到端负责，见 token-usage 分工），云端测试见第 4 节。
- 2026-10-14，02 的云端验证（Linux、Python 3.13、PowerShell 7）：
  - 全套 PR 测试除 30 个失败外都通过；这 30 个在改动前的 main 上同样失败（Windows 专用的 schtasks、icacls 用例，IBKR 模拟、交易时段检查和两条报告用例）。
  - 对 `ship.py`、`lane.py`、`daily_window.py` 的关键检查做了 22 个变异，20 个让测试失败。剩下 2 个是兜底检查，正常路径上碰不到：replay 后再核对一次提交信息；冲突的任务总表先取 main 的版本，再被重新渲染覆盖。
