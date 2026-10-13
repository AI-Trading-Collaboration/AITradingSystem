# pi 研究运行的 brief 模板（TRADING-2565 S3c）

写一道题的 pi 运行 brief 时，复制下面两种之一，把 `<...>` 填好。一道题至少两次运行：先开发，后验证。**验证必须单独一次运行**，这样运行报告列出的改动只可能来自那一次读取。

两种运行的共同规矩：

- 运行记录和读取记录在 `research/<题号>/` 下，是 A 区，用本地模型。
- pi 只运行 brief 列出的命令，看屏幕输出，不写代码，不改 `prereg.yaml` 和策略模块。
- 数据规矩：
  - 不打开 `data/raw/research/`、`data/raw/external_request_cache/` 下的任何文件，也不打开 `research/<题号>/runs/` 里的记录；要看结果就用 `report`。
  - 屏幕输出只有汇总，可以原样写进运行报告。
- 不要删除、改名或手工编辑 `research/<题号>/` 下的任何文件。哪条命令报错就停下汇报，不要重试，也不要换参数再跑。
- 全仓库的 git status 和 git diff 带 `. ':(exclude,literal)docs/research/growth_tilt_owner_diagnosis_pack.md'`。

## 开发运行

```
---
task: <任务号>
run: <任务号>-<序号>
template: custom
zone: A
model: local
thinking: high
tools: default
allow_done: false
ship: publish
allowed_paths:
  - research/<题号>/runs/**
read_paths: []
---
# <运行号>：<题号> 开发窗口（A 区）

前提：`research/<题号>/prereg.yaml` 和策略模块已在 main 上；快照 `<快照 id>` 在本机且检查通过。

1. 确认在 main、工作区干净，记下 `git rev-parse HEAD`。`git switch -c pi/<运行号>`。
2. 依次运行，每条把屏幕输出原样记进运行报告：
   - `python -m ai_trading_system.research_bench ledger <题号>`
   - `python -m ai_trading_system.research_bench run <题号> --strategy <id> [--set 参数=值 ...] [--falsify]`
     （逐条列出。每个要进验证的策略，至少有一条用预注册参数加 `--falsify`，验证前先确认证伪能跑通）
   - `python -m ai_trading_system.research_bench report <题号>`
3. 某条 `run` 输出以 `refused`（试验数用完）或 `checks failed` 开头时，记下并继续下一条；其他报错就停。
4. `git status --short`（带排除写法）只应列出新增的 `research/<题号>/runs/*.json`，数目等于第 2 步运行的 `run` 条数减去被拒绝的条数（检查失败和出错的运行也有记录）。不符就停下汇报。
5. 提交，提交信息 `<任务号>: <题号> development runs <起>-<止>`。按 ship-change 发布。
```

## 验证运行

前提：

- 预注册已随 owner decision 进了 main；
- 开发运行已经发布；
- 每个要验证的策略都跑过 `--falsify`。

验证窗口每题只能读一次，所以这次运行只做读取和报告。

```
---
task: <任务号>
run: <任务号>-<序号>
template: custom
zone: A
model: local
thinking: high
tools: default
allow_done: false
ship: publish
allowed_paths:
  - research/<题号>/runs/**
  - research/<题号>/window_reads.jsonl
read_paths: []
---
# <运行号>：<题号> 验证窗口读取（A 区，每题一次）

前提：main 上有 `<owner decision>`（docs/requirements）和获批的 `research/<题号>/prereg.yaml`。

1. 确认在 main、工作区干净，记下 `git rev-parse HEAD`。`git switch -c pi/<运行号>`。
2. `python -m ai_trading_system.research_bench ledger <题号>`：读取一栏应为 `-`，链应为 `ok`。不是就停。
3. `python -m ai_trading_system.research_bench validate <题号>`，只运行一次。
   - 输出以 `validation:` 开头表示被拒绝，什么都没读，停下汇报原文。
   - 否则第一行是 `read <题号>-w0001 registered`，之后每个验证策略一行结果。即使有策略报错也不要重跑。
4. `python -m ai_trading_system.research_bench report <题号>`，原样记进运行报告。
5. `git status --short`（带排除写法）只应列出 `research/<题号>/window_reads.jsonl` 和新增的 `research/<题号>/runs/*.json`，运行记录的数目等于验证策略数。不符就停下汇报，不要提交。
6. 提交，提交信息 `<任务号>: <题号> validation read <题号>-w0001`。按 ship-change 发布。
```

一致性窗口（`consistency <题号>`）同样单独一次运行，步骤和验证运行相同，命令换成 `consistency`。

运行发布后，Claude 根据 `report` 的输出写 `research/<题号>/decision.md`，只引用登记过的运行号和读取号。结论用 `research/README.md` 里的四个词。
