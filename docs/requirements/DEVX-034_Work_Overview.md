# DEVX-034 整体工作总览（私有，经学习 App 打开）

## 1. 背景

- 研究线有公开的研究进展页（TRADING-2568，GitHub Pages）。工程和生产没有页面，信息分在三处：仓库（任务卡、需求文档、main 的提交）、电脑（生产日报的运行状态、当前发布包、部署记录、报告质量门禁）、云端项目文件（每天的开发日报）。
- 卡片是平铺的，`area` 是自由文字，看不出哪几张卡合起来是一件事；2026-10-24 进行中、验证中、提议的 24 张卡里，10-20 以后改过的只有 3 张，写着等 Baker 的 4 张都停在 10-13 到 10-16。直接搬上页面会误导，所以先清卡片、定主线（第 1 步），再写生成器。
- 方案全文在 claude.ai 的私有文档「整体工作进展展示方案」：三层，最上面一屏总览（顶部单列“等 Baker 的”，下面生产日报、研究、工程、协作与成本四条线各一张卡），中间每条线一页，最底下是现有的记录。研究那一列的中间层就是现在的研究进展页，不重做。

## 2. owner 决定

Baker 2026-10-25 00:35 JST 在项目里写：

> 是这样的 我们现在虽有研究页面展示，但是没有工程相关进展的展示，我希望借由这次契机和你讨论下如果将我们整体的工作更好的展示出来，做到重点清晰层次分明，再接入到learning system的统一入口中

owner_decision:DEVX-034:2026-10-25:work_overview_private —— owner（Baker）2026-10-25 00:47 JST（2026-10-24 15:47Z）在「设计整体工作进展展示」线程的决定卡上作答：

- 卡片问题：「整体工作总览放在哪、谁能看？」
- 选择：「私有，经学习 App 打开」
- 选项后果：「AITradingSystem 加一个只读生成器（pi 写），学习 App 定时调用并经 Tailscale 提供；我先做第 1 步：清卡片、拟主线草案。」
- 卡片说明：「研究页已经公开在 GitHub Pages。工程、生产日报状态和成本里有花费、你的等待时间、电脑路径，日报正文还用了不许再分发的数据，所以不适合公开。方案文档第 6 节有路线图。」
- 没选的两项：「私有，做成 claude.ai 页面」（每次由 Claude 重新发布，读不到电脑上的生产状态）、「先只清卡片和定主线」。

范围：AITradingSystem 里只加一个只读生成器（`src/`，B 区）和这份需求、主线文件、任务卡；公开研究页不变；`.github/**` 不改。学习 App 那边的接入写在 LearningSystem 仓库，按那边的规矩做；LearningSystem 的代码和数据不进 AITradingSystem，AITradingSystem 也不读 LearningSystem。

## 3. 第 1 步：卡片核对（DEVX-034-01）

2026-10-25 01:10 JST 按 main `ae908eb9` 和电脑上的生产记录（生产状态根的运行状态、取数清单和日报报告，只读）逐张核对。Baker 2026-10-25 00:54–00:58 JST 在方案文档的批注里答了五个问题；建议一栏他没有改。

| 卡 | 结果 | 依据 |
|---|---|---|
| DATA-024 | DONE | DATA-024-01 随发布包进生产；as_of 2026-10-22 起 SEC 文件 17 家里 15 家和旧运行副本不同，和 10-16 发现的新取对旧缓存 15/17 一致 |
| DEVX-030 | DONE | 运行 02 于 2026-10-14 发布，研究工作区已建，研究记录车道每天在用 |
| DEVX-031 | DONE | 04 于 2026-10-14 发布，后来又发布两次修补；车道每天在用 |
| DATA-022 | DONE | 2026-10-23、10-24 两次 12:00 生产日报 PASS；Baker 确认 Marketstack 已退订、key 已删（「是的」） |
| TRADING-2566 | DONE | 第 2 项（Baker 审一次 negative_results.md）不做，每道题结束时 Baker 逐条确认失败标签已经覆盖；Baker 同意（「好的，关了吧」） |
| OPS-083 | 改写下一步 | 第 1、2 步达标（secret_hygiene 平时约 8 秒，score_daily 43–69 秒）；下一步修换包后的全量重扫，Baker 同意（「按你说的办」） |
| DATA-001 | BASELINE_DONE | as_of 2026-10-23：valuation_percentile 17/17、eps_revision_90d_pct 15/17（FMP analyst-estimates，EODHD 已退订）；完整 DONE 要稳定 90 天 PIT 历史 |
| PROD-004 | 改写下一步 | FMP 已恢复，运营验收可以做了，由 Claude 做 |
| DEVX-025 | DEFERRED | 两件 owner 设置 2026-10-12 已做；受限账户复测（10b）没做，2026-10-12 起 pi 在 owner 账户的主工作区和车道里跑 |
| TRADING-2568 | 改一句 | 运行 04（快筛和判断面板）随 TRADING-2569 延后，状态仍是 BASELINE_DONE |
| TRADING-2569 | DEFERRED | 2026-10-16 以后没有判断记录；Baker：「你没在用的话就没用了」 |
| TRADING-2570 | DEFERRED | 第一次复查要等前向样本满 250 个交易日（约 2027-10-15），2027-09-01 前重开 |
| DEVX-026、DEVX-027 | DEFERRED | 2026-10-12 以后没动，排在延后的 DEVX-025 之后 |
| CALIBRATION-003、004、005 | DEFERRED | 2026-10-10 迁移以后没动，研究走研究自动循环 |
| KNOWLEDGE-001、PUBLISHING-001、DOCS-PLATFORM-001 | DEFERRED | 2026-07-12 登记以后没动；这次的总览另开本卡，不挂在 K0/K1 下 |
| STORAGE-001 | DEFERRED | 2026-07-12 重开以后没动 |

每张延后的卡在 next_step 里写了退出条件，原来的下一步留在 main `ae908eb9` 上的那一版。

不在这一批改的：OPS-086（Baker 2026-10-25 00:55 JST 同意现在把旧运行副本和研究数据搬进 archive，「现在应该就可以搬了把，看你安排」；由另一个线程搬，搬完关卡）；TRADING-2567（研究循环线程用 TRADING-2567-39 改写）；TRADING-2565、OPS-085 照旧。

同一批顺带改 `docs/data_source_subscription_inventory.md` 一行（Marketstack 已退订，EODHD 2026-10-13 已退订）和 OPS-083 需求文档的进展。`config/data_sources.yaml` 里两个源仍写 active，它在 C 区，下次改这个文件时一起改。

## 4. 工程主线文件

`docs/work_overview/mainlines.yaml`（`work_mainlines.v1`）：

- `updated`：最后一次改的日期。
- `mainlines`：每条有 `id`（小写加下划线，不重复）、`title`、`goal`（这条线要达到什么）、`milestone`（现在到哪，一句话）和 `cards`（任务卡 id，必须在 `tasks/` 下存在）。
- `research_cards`：研究本身的卡。研究不算工程主线，进度看研究进展页。

规则：

1. 状态是 IN_PROGRESS、VALIDATING、PROPOSED、READY、BLOCKED_OWNER_INPUT、BLOCKED_EXTERNAL 的卡，除 `research_cards` 外，恰好属于一条主线。BASELINE_DONE、DEFERRED、DONE 的卡可以留在主线里，不强制（2026-10-25 有 73 张 BASELINE_DONE 的卡不在任何主线里，大多是 2026-10-10 迁移以前的，总览只计数）。
2. 一张卡最多出现在一条主线里，不同时出现在主线和 `research_cards` 里。
3. 新开、关掉一张工程卡或换主线时，在同一个提交里改这个文件；里程碑变了也改 `milestone`。
4. 第 2 步的生成器检查这几条，违反的写在页面上，不让生成失败。

2026-10-25 的五条主线：日报运行与发布、数据源、开发流程、研究工具、展示与知识。

## 5. 后面几步

1. **第 2 步：生成器第 1 版（DEVX-034-02，B 区）**。Claude 写需求和验收测试，pi 写代码，走 pi 队列。内容是等 Baker 的（卡片 next_owner 里写着 Baker 的在办卡）、工程按主线分组、生产日报最近 14 天日历（每天过没过、用时、有没有 17:30 补跑）和研究汇总（调用 `research_site`，不经网络）。只出 `work.json`，不出 `work.html`：页面由学习 App 用自己的样式画（Baker 2026-10-25 01:26 JST：「主页已经重新设计了，对应的入口也暴露出来了到时候你可以直接接入到这个网页的分页中」）。日报那块读状态根 `outputs/run_control/scheduler/` 下每次调用的摘要，不读 `outputs/run_control/daily/states/`：摘要里有窗口、开始和结束时间、发布包号和自动重试的结果，状态文件里没有。详见第 8 节。
2. **第 3 步：接入学习 App**（LearningSystem 仓库）。由学习 App 那边的工程 coordinator 按那边的规矩做：App 后端调用生成器、提供 `work.json`，研究页加一张总览卡和一个子页面。Baker 2026-10-25 03:10 JST：「好的，我们这些工作以及反馈等做完后统一转给学习app的工程coordinator把」。约定见第 8.4 节。
3. **第 2 版**（用一两周再定）：每天的开发日报在电脑上留一份 `index.csv` 副本，接进来做协作与成本那块，加主线进度和趋势。

要守的规则：

1. 公开边界：公开页只放研究。成本、Baker 的等待时间、电脑路径、生产细节只进私有页。
2. 数据许可：FMP 等来源是 `external_llm_allowed: false`、`redistribution_allowed: false`。日报正文（owner_daily_brief、reader_brief）和任何价格都不进总览，只放过没过、用时、发布包、门禁结论；前瞻台账只用 `research/_forward/weekly.md` 的汇总，不读 `ledger.jsonl`。
3. 只读、单向：生成器只读仓库 main、生产状态根和日报副本，不联网，不写生产，不碰运行副本和发布包；读不到的块在页上写“读不到”，绝不影响日报。
4. 读已提交的内容：读 main，不读开发工作区里的草稿。
5. 不加计划任务：README 的硬规则是每日调度只有 “AITradingSystem Daily Run”；生成由学习 App 后端定时调用。
6. 分区和分工：生成器在 B 区，和 `research_site` 同一个包；不改 `.github/**`（C 区）。

## 6. 验收

- 第 1 步：`tools/tasks.py check` 通过；主线文件满足第 4 节规则（云端用 main 上的卡核对过）；没有测试钉住改动的卡。
- 第 2 步：主线文件里的卡都存在、规则 1–2 的违反写进 `work.json`；生成器只读、不联网、不写生产和运行副本，读不到的块写“读不到”；`work.json` 不含日报正文、价格和供应商原始数据；同样的输入生成逐字节相同的输出；`tests/test_work_overview.py` 和全套 PR 测试通过。
- 第 3 步按 LearningSystem 仓库自己的验收。

## 7. 进展

- 2026-10-25：第 1 步核对完；DEVX-034-01 记录批（本文件、主线文件、21 张卡的改动和本卡、订阅清单一行、OPS-083 进展）交 pi 队列。
- 2026-10-25：第 1 步上 main（`094e1466`）。第 2 步生成器第 1 版（DEVX-034-02）：第 8 节、验收测试和代码一起上 main；下一步在电脑上只读跑一次，再把第 8.4 节的约定交给学习 App 那边。

## 8. 第 2 步：生成器第 1 版（DEVX-034-02）

写于 2026-10-25。验收测试 `tests/test_work_overview.py` 是规格，下面写它的意思和测试没逐字写出的部分。

### 8.1 命令和位置

`python -m ai_trading_system.work_overview --repo <仓库> --state-root <生产状态根> --daily-root <发布包目录> --out <work.json>`，可选 `--ref`（默认 `main`）和 `--today`（日历的最后一天，默认 JST 今天）。代码在 `src/ai_trading_system/work_overview/`（B 区），Python 接口是 `build(repo, *, state_root, daily_root, today, ref)` 和 `main(argv)`。

- 学习 App 的后端用开发检出的 `.venv` 调它（`D:\Work\AITradingSystem\.venv\Scripts\python.exe -B -m ai_trading_system.work_overview ...`），不用发布包里的 Python：发布包只给日报用。
- `--out` 在仓库、生产状态根或发布包目录里面时退出码 2、什么都不写；否则先写同目录的 `<名字>.tmp` 再改名，读的一方不会读到写了一半的文件。
- 同样的输入（包括 `--today`）生成逐字节相同的文件：`json.dumps(..., ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False)` 加换行，LF。文件里没有生成时间，App 用文件的修改时间。
- 读不到的块写 `{"status": "unreadable", "error": "读不到：..."}`，原因同时进 `problems`；退出码仍是 0。在云端的真实 main 和电脑上生产记录的副本上约 3 秒。

### 8.2 读什么

| 来源 | 读的内容 | 不读 |
|---|---|---|
| 仓库 `--ref` 上已提交的内容 | `git archive` 把 `tasks`、`docs/work_overview`、`docs/requirements`、`research`、`config/research`、`src/ai_trading_system/research_screens/signals`（`TREE_PATHS`，`research_site` 读的就是这些）解到系统临时目录，用完删；任务卡、主线文件和研究汇总都从这份副本读。另外 `git log` 取最近 10 个提交的标题和每张卡最后改动的日子，`git rev-list --count` 算发布包落后几个提交 | 工作区里没提交的改动、别的分支；`research/_forward/ledger.jsonl`（`research_site` 不读它） |
| 生产状态根 | `outputs/run_control/scheduler/<日期>_<PRIMARY 或 RESCUE>[_<n>].summary.json`，只读日历 14 天里的；`outputs/reports/report_quality_gate_<as_of>.json` 里日期最新的一个，只取结论和两类问题的条数 | 日报正文（`owner_daily_brief`、`reader_brief`）、`.md` 和 `.log`、`run_control/daily/states/`、`data/` 下的任何文件；摘要里的 `findings`、`steps[].detail`、`capture_components[].error_excerpt` 和各种路径 |
| 发布包目录 | `current.json`、`deploy_log.jsonl` 的最后 5 条（只取 `at`、`action`、`outcome`、`release_id`） | `releases/` 下的任何文件 |

不联网（测试把 socket 换掉）。打开的生产文件只能是上表左边那几种（测试记下每次打开）。

### 8.3 work.json 的内容（`work_overview.v1`）

- `source`：`ref`、`commit`、`commit_time`。
- `waiting`（等 Baker 的）：在办的卡（状态是 IN_PROGRESS、VALIDATING、PROPOSED、READY、BLOCKED_OWNER_INPUT、BLOCKED_EXTERNAL）里，状态是 BLOCKED_OWNER_INPUT 的，或 `next_owner` 第一个括号之前写着 Baker 或 owner（不分大小写）的；按优先级、卡号排。每张卡是 `id`、`area`、`status`、`priority`、`next_owner`、`next_step`、`updated`（这张卡在 `--ref` 上最后改动那天，JST）。
- `engineering`：主线文件的 `updated`；每条主线的 `id`、`title`、`goal`、`milestone`、它的卡和各状态的张数；`research_cards`；第 4 节规则的违反：`unassigned`（在办却不在任何主线和研究卡里的卡）、`duplicates`（出现不止一次的卡号）、`missing`（没有这张卡）；全部卡按状态的张数 `totals`；最近 10 个提交 `recent`（12 位提交号、JST 日子、标题）。违反也写进 `problems`：「主线文件：没有卡 X」「主线文件：X 出现了不止一次」「主线文件：在办的卡 X 不在任何主线里」。主线文件读不到时只有这一块读不到，`waiting` 照常。
- `daily`：最近 14 天（`--today` 往前 13 天到当天，按调度那天的 JST 日子）每天一行：`result`、`as_of`、`minutes`、`release_id` 和当天每次调用（`name` 是文件名里的 PRIMARY、RESCUE、PRIMARY_2 这些；`window`、`as_of`、`status`（摘要的 `cli_status`）、`blocker`、开始和结束时间、`minutes`、`release_id`、`exit_code`、`retry`（数据源失败自动重试的结果）、失败的步骤 id、失败的数据源 id）。一天的 `result`：调用按开始时间排，第一个 `PASS` 是 PRIMARY 窗口的为 `PASS`，是 RESCUE 窗口的为 `RESCUED`（17:30 补跑过了）；全部是 `RUN_CONTROL_ALREADY_COMPLETE` 为 `NO_NEW_DAY`（周日、周一没有新的美股交易日）；有调用但没有过的为 `FAILED`；没有摘要为 `NO_RUN`。`minutes` 和 `release_id`、`as_of` 取过了的那次，没过时取最后一次（`minutes` 这时为空）。`counts` 是五种结论的天数。`release`：`current.json` 的发布包号、源提交、激活时间、上一个发布包，`commits_behind`（`--ref` 比源提交多几个提交，仓库里找不到源提交时为空），最近 5 条部署记录。`quality_gate`：最新那天的 `as_of`、结论、阻断和警告的条数。
- `research`：`research_site.collect` 的 `summary`，当期章程的期号、路径、期限和方向，前瞻账本的行数和起止日，前瞻核对的条数和待核数，家族表各状态的个数。别的研究内容看研究进展页。
- `collaboration`：`{"status": "later"}`，第 2 版接开发日报副本。
- `problems`：读不到的文件和违反主线规则的地方，每条一句。

### 8.4 学习 App 怎么接

由学习 App 那边的工程 coordinator 在 LearningSystem 仓库按那边的规矩做（第 5 节第 2 条），这里只写约定：后端加一个只读接口返回 work.json，文件超过约 10 分钟就在后台重新生成一次（正在生成时照旧返回旧文件），不改后台循环、不加 Windows 计划任务；研究页加一张“工作总览”卡和一个子页面，用 App 自己的样式画上面四块，`status` 不是 `ok` 的块写 `error` 那句话。`--today` 不传，由生成器取 JST 今天。
