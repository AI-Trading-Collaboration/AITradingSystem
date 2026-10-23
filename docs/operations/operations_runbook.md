# AITradingSystem Operations Runbook

最后更新：2026-10-23（OPS-086 步骤 8：日报从发布包运行，第 1、3、9 节改写）；2026-10-13（OPS-084：主触发时间改为 12:00；OPS-085：17:30 对数据源临时故障自动重试一次）；2026-10-11（OPS-082：Windows 计划任务、终止恢复改为运行副本校验、新增第 9 节）；2026-10-10 GOV-008 P5 重写

执行任何 daily、weekly、biweekly、monthly、调度或产物目录任务前先读本文：确认 cadence、触发路径、质量门禁、
预期产物和"不影响 production"的边界。数据怎么流动见 `docs/system_flow.md` 第 1～6 节；产物怎么理解见
`docs/artifact_catalog.md`。2026-10-10 之前的完整旧版（含已删除的发布、调度发布与 ETF 机制）在
`docs/operations/operations_runbook_legacy_2026-10-10.md`，只作历史查阅。

## 1. 唯一入口

```powershell
aits ops daily-run                      # 外部调度唯一入口；评估最近一个已完成的美股交易日
aits ops daily-run --as-of YYYY-MM-DD   # 指定评估日
aits ops daily-plan --fail-on-missing-env   # 只出计划、检查环境变量，不执行
aits ops replay-day --mode cache-only --as-of YYYY-MM-DD   # 历史严格复现，不用 daily-run 补造历史 PIT
```

- 只允许一个外部调度入口。weekly / biweekly / monthly / ad hoc 任务不得登记为独立的系统计划任务（Windows Task Scheduler、
  cron、GitHub Actions 或云调度器都一样）。
- 未给 `--as-of` 时，`daily-run` 与 `validate-data` 使用 America/New_York 最近已完成交易日加收盘后 3 小时
  provider-ready 缓冲，不用主机本地日期。
- **调度（OPS-082，2026-10-11 起；OPS-086 步骤 7，2026-10-22 起从发布包运行）**：唯一外部调度入口是 Windows 计划任务
  `\AITradingSystem Daily Run`，启动 `D:\Work\AITradingSystem_daily\run_daily.ps1`；它按 `current.json` 选出已激活的日报发布包，
  在状态根 `D:\Work\AITradingSystem_data\prod\state` 上经包装脚本调用一次 `aits ops daily-run`；每天 12:00 与 17:30
  （Asia/Tokyo）两个触发时间，17:30 是同一任务的补救时间，重复触发由 `daily-run` 的运行控制去重
  （`owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1`）。旧运行副本 `D:\Work\AITradingSystem_ops_runtime`
  不再运行日报、不更新，保留作回退（第 9.4 节）。Codex automation `aitradingsystem-pit` 已停用
  （owner 2026-10-11 暂停），任一时刻只有一个调度来源；不得再登记第二个调度。包装脚本、中文摘要、注册、发布包部署与回退
  步骤见第 9 节。为旧调度服务的 release promotion、scheduler checkout preflight、Codex scheduler observation 与
  deployment acceptance 命令已在 GOV-008 删除，`daily-run` 不再持有 checkout guard（`--manual-execution` 选项保留但已无作用）。

## 2. 每日链路

步骤与依赖以 `config/scheduled_tasks.yaml`（`scheduled_tasks_v8`，`daily_trading_day` 共 32 个任务）为准，顺序如下：

| 阶段 | 步骤（命令） | 条件与阻断 |
|---|---|---|
| 输入 | `ops capture-daily-inputs`（交易日）：market_macro、fmp_forward_pit、sec_companyfacts、fmp_valuation、official_policy_sources | 每个组件独立尝试，原始字节与 checksum manifest、XNYS 缺口台账全部保留；`CAPTURED` 只表示字节已保存，不等于 DQ 通过 |
| 输入（休市日） | `download-data`、`pit-snapshots fetch-fmp-forward`、`fundamentals download-sec-companyfacts`、`valuation fetch-fmp`、`risk-events fetch-official-sources` | 只在休市日运行 |
| DQ 前刷新 | `data recover-freshness --latest --no-price-writes` | OPS-080：影子跟踪资产 GOOGL、BRK.B（不在评分股票池）已由 `download-data` 一起下载、进入同一份下载发布并过 DQ；这一步不补抓、不改写价格，只刷新价格缓存登记、backtest manifest 和 freshness，缺历史只报告 `SOURCE_DELAYED`。需要补抓时人工运行不带 `--no-price-writes` 的 `recover-freshness`，之后重跑 DQ。失败只阻断 artifact lineage 和 Reader Brief，不阻断 DQ 和评分 |
| 数据质量 | `validate-data --execution-profile daily_default.v1` | 写 DQ receipt 与按 profile/as-of 的发现指针；**只有 strict PASS** 才能放行评分 |
| PIT | `pit-snapshots project-fmp-forward-capture` → `build-manifest` → `validate` | 失败快照不得作为 PIT 输入 |
| SEC | `fundamentals extract-sec-metrics` → `merge-tsm-ir-sec-metrics` → `validate-sec-metrics` | |
| 评分 | `score-daily --consumer-authorization-profile daily_score_daily@1.0.0` | 评分前严格复核 DQ receipt、capture 证据与来源；任一缺失或漂移则零运行 |
| 评分之后 | `forward-evidence capture-dry-run-daily`、`reports dashboard`、`sec-pit shadow-observe/shadow-monitor`、`reports score-change-attribution`、`reports market-panel`、`data freshness`（只读） | 只读或只写研究存档；不改写 DQ 检查过的输入、不写权重、不触发 broker |
| 报告 | `reports artifact-lineage` → `validate-artifact-lineage` → `reports index` → `docs report-contract` → `reports research-governance-summary` → `reports reader-brief` → `reports quality-gate` → `reports validate-reader-brief` | 质量失败必须在输出中可见 |
| 收尾 | `ops health`、`security scan-secrets`（always-run） | 它们的 PASS 不覆盖投资门禁 |

之后 `daily-run` 在同一运行控制下做 canonical finalization：生成本次运行的 decision summary 与 task dashboard，
重新生成最终 Reader Brief 并让质量检查绑定最终字节；`report_quality_status=FAIL` 或 Reader Brief 质量 `FAILED` 时运行失败，
`PASS_WITH_WARNINGS`、`LIMITED_READER_CONTEXT` 可以完成但必须披露并进入人工复核。

**休市日**：不生成新的评分、决策快照、Reader Brief 评分产物或 forward evidence；数据刷新与 PIT/SEC 检查照常。

**缺失与失败**：某个 capture 组件缺失只阻断依赖它的步骤，其他步骤继续并保留证据；整体状态保持
`BLOCKED_DEPENDENCY` 或 `FAIL`，finalization 不运行。

## 3. 运行控制与恢复

- 同一 workflow/规格/as-of 只能有一个活动运行；已完成的重复触发不重跑；中断的运行只恢复明确幂等且未 PASS 的步骤。
  状态与台账在 `outputs/run_control/daily/states/`。**不得手工删除状态或锁来强行重跑**，过期锁由控制层回收。
- 步骤列表变化会产生新的规格 id 与新的幂等键（旧状态与台账保留），所以修改 `config/scheduled_tasks.yaml` 后同一 as-of
  会作为新运行开始。
- 计划任务的第二次触发（17:30 或手动）遇到同一 as-of 已 PASS 时返回 `RUN_CONTROL_ALREADY_COMPLETE`（退出码 0），遇到
  BLOCKED/FAILED 终态时返回 `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`（退出码 1），都不执行任何步骤；摘要分别归为
  "无阻断"与"等待新 as_of"。
- 例外（OPS-085，`owner_decision:OPS-085:2026-10-13:rescue_retry_after_source_failure`）：17:30 触发带
  `--retry-after-source-failure`。上一次运行是该 as-of 的第 1 次尝试、采集已关闭（`PARTIAL_CAPTURE`，校验 PASS）、除
  `pipeline_health`/`secret_hygiene` 外没有步骤失败，且每个失败组件都是可重抓的历史源（`IMMUTABLE_RAW_BACKFILL`，目前只有
  `market_macro`）、blocker 可重试、没有 traceback 时，运行控制以 `SOURCE_RETRY` 从 `capture_daily_inputs` 起重跑一次
  （第 2 次尝试）；已 PASS 的采集组件复用，失败组件在 `rescue/` 补救会话里重抓。父运行状态与回执在
  `outputs/run_control/daily/source_retry/<key>/attempt_2/`。密钥、权限、额度、格式错误、traceback、PIT/官方源/SEC 组件失败
  或其他步骤失败都不重试。摘要写明结果（`RETRIED` 或 `NOT_ELIGIBLE:<原因>`）。条件见 OPS-085 需求文档第 3 节。
- 终止后恢复（OPS-071）只能用同一入口：`aits ops daily-run --recovery-parent-run-id <id> --recovery-from-step <step>
  --recovery-reason-code <code>`，三项必须同时给出；只允许从 `artifact_lineage` 及其后的报告/收尾步骤恢复，已 PASS 的
  capture、DQ、PIT、评分必须复用。恢复绑定的当前版本是**正在运行的代码根的版本**：发布包为 `RELEASE.json` 记的源提交（清单校验必须通过，即发布包文件没被改过），
  运行副本为 HEAD（工作区必须干净、HEAD 必须是本地 `origin/main` 可达的提交）；两者都要与父运行 manifest 的 `git_commit` 不同
  （OPS-082 决定 e）；CLI 构造请求与运行控制放行时各检查一次。发布包上的恢复命令在当前发布包目录的 `.venv` 里运行，先把环境变量
  `AITS_STATE_ROOT` 设成状态根、`PYTHONDONTWRITEBYTECODE` 设成 `1`（与 `run_daily.ps1` 一样）；这条路径在生产上还没做过，第一次做时先在 owner 指示下核对命令。
  不再需要 `AITS_OPS_DEPLOYMENT_RECEIPT`（已无代码读取）。恢复只在 owner 指示下人工执行，计划任务不会自动恢复。
- 同一 as-of 的终止运行没有合法恢复边界时，等待下一个 provider-ready 交易日的普通运行，不得伪装成恢复。
- 历史缺口：`aits ops recover-historical-gap` / `validate-historical-gap` 只由 owner 人工触发单个已审查的队列项，只生成
  隔离证据，不改旧运行、不补造 strict PIT。

## 4. 数据质量门禁

`aits validate-data` 是缓存行情与宏观数据的必经门禁。任何从缓存数据产生特征、评分、回测或日报的命令都先运行它
（或调用同一代码路径），失败即停止；下游报告写明 DQ 状态或链接质量报告。

- 一次执行只调用一次 canonical validator，并把策略、输入字节、报告与 `DataQualityEvidence` 冻结到
  `outputs/data_quality/executions/{receipt_id}/receipt.json`；报告正文按 SHA-256 存在 `outputs/data_quality/reports/`。
- 显式 `--as-of` 默认解析为 `manual.v1`，不会覆盖 daily 指针；只有满足全部默认输入约束的 `daily_default.v1` 才发布
  `outputs/data_quality/executions/discovery/daily_default/{as_of}/current.json`。指针只用于发现，不是质量证明。
- `download-data` 先复用已有缓存，只请求缺失的尾部窗口；Marketstack 第二源做额度预检，额度不足默认 fail closed，
  只有 `config/data_source_request_budget_policy.yaml` 登记的小额例外可以继续，并在下载清单中披露。

## 5. 非每日任务

`config/scheduled_tasks.yaml` 另登记 27 个任务：weekly（回测 `unified_primary_2021` 与稳健性、参数回放/候选/治理、
权重候选评估与晋升门禁、研究治理摘要复核）、biweekly（投资复核、反馈闭环、shadow lane、SEC PIT observe-only、
人工 thesis 与风险复核）、monthly（文档契约、产物目录、报告登记、数据源覆盖、PIT 覆盖、长窗口回测复核）、
ad hoc（SEC PIT 回填/评估/基线对比/诊断/候选复核、大参数搜索、缓存回放窗口）。

- `daily-run` 只为它们写不执行的 `periodic_operations_plan_YYYY-MM-DD.json`：是否到期、缺哪些证据。自动派发关闭
  （`config/operations/periodic_control.yaml`：`automatic_command_dispatch_enabled: false`）。
- 人工运行：

  ```powershell
  aits ops periodic-dispatch --task-id <task> --daily-status PASS --data-quality-status PASS `
    --data-quality-evidence-id <receipt-id> --source-artifact-id <artifact> --owner-decision-id <id> `
    --confirm-manual-dispatch            # ad hoc 任务另加 --explicit-trigger
  ```

  未解析的占位符或不在允许前缀里的命令在执行前 BLOCKED。
- 每个任务的输出写明 cadence、as_of、日期范围、来源产物、数据质量状态与 `production_effect`。缺少上游时输出
  `SKIPPED`/`LIMITED`/`INSUFFICIENT_DATA` 或 fail closed，不补造结论。

## 6. 操作前检查

1. 确认 cadence、`as_of`、日期范围、研究窗口（默认起点 2021-02-22）与是否交易日。
2. 确认 `aits validate-data` 或同一路径门禁已通过，并记下质量报告。
3. 确认 `production_effect`、权重写入、broker 与交易动作的边界（默认全部为 none）。
4. 会改变 CLI、配置、产物、报告 schema、数据流、评分、回测或解释的改动，同一次改动更新 `docs/system_flow.md`、
   `docs/artifact_catalog.md`、任务记录与相关需求文档或本文。
5. 遇到阻塞不静默绕过：说明最佳方案、阻塞原因、是否先修阻塞；临时绕过需 owner 同意并记录原因、影响、风险、验证与退出条件。

## 7. 前瞻采集（已删除）

GOV-009（2026-10-12）删除了手工前瞻采集入口 `scripts/run_named_data_quality.py`、capture hold
（`scripts/capture_hold.py`）和它们的 named DQ 子进程。新的前瞻记录直接接进日常调度（TRADING-2565）。
旧的步骤和边界在 tag `research-archive-2026-10-12` 下本文的第 7 节。

## 8. 已删除的机制

下列内容在 GOV-008 删除，旧版手册里的相关段落不再适用：发布与验证机制（fence、lease、Full 验证、validation tier）、
release candidate/canary/promote、runtime git exclusions、scheduler checkout preflight、Codex scheduler observation、
deployment acceptance、workflow health 遥测与周任务、Atlas、DEVX-015 源码保全检查点、ETF 候选链（`aits etf ...`、
dynamic-v3 rescue、候选跟踪每日步骤）。发布改由 `python tools/gov008/ship.py` 完成（见 AGENTS.md）。

## 9. 计划任务与日报发布包（OPS-082、OPS-086）

日报从 2026-10-22 晚上起（OPS-086 步骤 7）只在日报发布包里运行：发布包在 `D:\Work\AITradingSystem_daily\releases\<release_id>`
（从已经 ship 的提交构建，清单里的文件只读，没有 `.git`，`.venv` 在发布包目录里），`D:\Work\AITradingSystem_daily\current.json` 记当前和上一个
release，入口是同目录的 `run_daily.ps1`；`data/`、`outputs/`、`artifacts/` 在状态根 `D:\Work\AITradingSystem_data\prod\state`，
构建出的 zip 放在 `D:\Work\AITradingSystem_releases`。开发 checkout（`D:\Work\AITradingSystem`）不运行日报，它的 DQ 回执与报告不代表生产日报状态。
旧运行副本 `D:\Work\AITradingSystem_ops_runtime`（单独的 clone，detached 到一个已经 ship 的提交，`.venv` 是 editable 安装）不再运行日报、不更新，保留作回退
（第 9.4 节）；什么时候清理或搬进 archive 由 owner 决定。

### 9.1 发布包更新（部署新版本）

只在 owner 指示下做：把一个已经 ship 的提交变成新的发布包并激活，计划任务和入口不变。在开发仓 `D:\Work\AITradingSystem`：

1. 选提交：先 `git fetch origin main`，该提交必须已在 `origin/main` 上（`git merge-base --is-ancestor <SHA> origin/main` 退出码 0）；
   构建只读这个提交里的内容，不读未提交改动。
2. 选时间：放在当天 17:30 触发器结束之后、次日 11:50 之前，并确认没有活动运行：计划任务不在运行
   （`(Get-ScheduledTask -TaskName 'AITradingSystem Daily Run').State` 不是 `Running`），状态根的 `outputs\run_control\daily\locks\`
   下没有锁文件（部署脚本也会拒绝）。有活动运行就等它结束，不删锁。新提交改了 `config/scheduled_tasks.yaml` 的步骤列表时，规格编号变了，
   同一 as_of 会按新规格重新开始（第 3 节），这类部署一定要放在当天日报完成之后。
3. 构建并部署，命令见第 9.3 节：`build_daily_release.py --commit <SHA> --out-dir D:\Work\AITradingSystem_releases`，再用“后续部署”那条
   `deploy_daily_release.py --bundle D:\Work\AITradingSystem_releases\<release_id>.zip --daily-root D:\Work\AITradingSystem_daily --python C:\Python314\python.exe`。
   部署联网装依赖，依赖约束沿用当前 release 的 `.venv_freeze.txt`，不删旧 release。
4. 核对：部署输出有 `outcome=ACTIVATED` 和新的 `release_id=`；`D:\Work\AITradingSystem_daily\current.json` 的 `release_id` 是新的、
   `previous_release_id` 是上一个；`deploy_log.jsonl` 最后一行是这次的部署记录。下一次触发（12:00 或 17:30）就是新 release 的第一次运行，
   它的摘要 `<状态根>\outputs\run_control\scheduler\<日期>_<窗口>.summary.md` 写着发布包 id。
5. 在相关任务记录或需求文档里记下 release_id、源提交、时间和第一次运行的结果。

### 9.2 计划任务

- **注册或更新**（幂等，重复运行不会产生第二个任务）：在当前发布包目录运行
  `powershell -NoProfile -File scripts\ops\register_daily_scheduler_task.ps1 -DailyRoot D:\Work\AITradingSystem_daily`（任务 `\AITradingSystem Daily Run`
  启动 `D:\Work\AITradingSystem_daily\run_daily.ps1`）。不带 `-DailyRoot` 是旧行为：任务从运行副本 `D:\Work\AITradingSystem_ops_runtime` 启动
  （回退用，第 9.4 节）。脚本要求主机时区为 Tokyo Standard Time；发现另一个计划任务也在启动 daily-run 时拒绝注册。
  任务：当前用户、仅登录时运行（不存密码）、不提权、每天 12:00 与 17:30、StartWhenAvailable、不并行实例、单次上限 6 小时。
  12:00 自 2026-10-13 起（OPS-084，此前 09:30）：纽约夏令 23:00、冬令 22:00，晚于 Cboe 更新 VIX 历史文件的时间。
  改了触发时间要在当前发布包目录用同样的参数重跑一次本注册脚本，再用下面的查看命令确认。部署新 release 不需要重新注册（入口不变）。
- **手动触发**（与定时触发走同一路径）：`Start-ScheduledTask -TaskName 'AITradingSystem Daily Run'`。
- **查看配置**：`schtasks /query /tn "\AITradingSystem Daily Run" /xml`。
- **停用**：`Disable-ScheduledTask -TaskName 'AITradingSystem Daily Run'`，只在 owner 决定时执行；不得另建第二个调度来源。
- **唯一例外：pi 受限账户的手动任务**（`owner_decision:DEVX-025:2026-10-11:manual_agent_task_exception`）。`\AITradingSystem pi agent` 由 `scripts\agent\setup-agent-account.ps1` 注册，以本地标准用户 `aits-agent` 运行，没有触发器。`scripts\agent\run-brief.ps1` 写入 `D:\Work\AITradingSystem_agent_runs\queue\request.json` 后用 `schtasks /Run` 启动它，它运行 `D:\Work\AITradingSystem_agent\scripts\agent\agent-run.ps1` 跑一次 pi（DEVX-025）。任务通过 `-PiCommand` 使用管理员装在 `C:\Program Files\aits-pi` 的固定版本 pi，aits-agent 只能读不能改；任务权限允许 owner 账户不提权启动。它不调用 `aits ops`，不访问运行副本，不是第二个调度来源；不得给它加触发器，也不得用它运行别的命令。aits-agent 的密码由任务计划程序保存、设为永不过期；要换密码时先 `Set-LocalUser aits-agent -Password (Read-Host -AsSecureString)`，再以管理员身份重跑安装脚本并输入同一个新密码。
- 任务以 `conhost.exe --headless` 启动 Windows PowerShell，不弹控制台窗口。conhost 不转发退出码，所以
  `Get-ScheduledTaskInfo` 的 `LastTaskResult` 只表示包装脚本已启动；**运行结论看日志与摘要**。

包装脚本 `scripts\ops\daily_scheduler_run.ps1`（在发布包里，由 `run_daily.ps1` 带窗口参数调用；`run_daily.ps1` 读不到 `current.json`、字段无效或发布包缺文件时，向 `D:\Work\AITradingSystem_daily\run_daily_error.log` 追加一行并退出 2，不调用 daily-run）每次触发做且只做：

1. 前置检查：发布包里 `RELEASE.json` 校验通过（文件没被改过）、`AITS_STATE_ROOT` 是绝对路径、`.venv\Scripts\aits.exe` 存在
   （回退到运行副本时改为：工作区干净、HEAD 可从本地 `origin/main` 到达）。任一失败不调用 daily-run，退出码 2，摘要归为"控制面问题"，
   并写明阻断码（如 `RUNTIME_RELEASE_MODIFIED`、`STATE_ROOT_NOT_SET`）。
2. 只设置 daily-run 实际读取的环境变量：`FMP_API_KEY`、`SEC_USER_AGENT`、`OPENAI_API_KEY`、
   `CONGRESS_API_KEY`、`GOVINFO_API_KEY`（Marketstack 自 DATA-022 起不再读）；进程里没有时取用户环境变量。值不打印、不写日志，日志只记 PRESENT/MISSING。
   新增或停用数据源时，同一次改动更新这个列表。
3. 调用一次 `.venv\Scripts\aits.exe ops daily-run`（不带 `--as-of`，由 daily-run 解析最近 provider-ready 交易日）；
   RESCUE 窗口（15:00 以后）加 `--retry-after-source-failure`（OPS-085，见第 3 节）。
4. 输出与退出码写 `<状态根>\outputs\run_control\scheduler\<东京日期>_<PRIMARY|RESCUE>.log`（15:00 前为 PRIMARY；已存在则加 `_2`、`_3`）。
5. `scripts\ops\daily_scheduler_summary.py` 用代码（不调用 LLM）写同名 `.summary.md`（中文）与 `.summary.json`：as_of、窗口、
   终态、每步状态、每个 capture 组件、DQ 状态与阻断原因分类。通知只写这些文件。

摘要的阻断分类是给运维的初步判断，每条依据写明出处：

| 分类 | 典型证据 | 下一步 |
|---|---|---|
| 无阻断 | daily-run PASS，或同一 as-of 已 PASS 不重跑 | 无 |
| 数据源不可用 | capture 组件 blocker 为 provider 鉴权/额度/不可用/schema/请求失败（含付费源停订） | 等数据源恢复或改源（研究策略调整在迁移后与 owner 讨论）；不重试刷额度 |
| 数据质量未通过 | `validate_data` FAIL | 读 DQ 报告；按数据问题处理，不放宽门禁 |
| 代码缺陷 | 未处理异常、非数据原因的步骤失败、finalization 失败 | 任务记录 + `ship-change` 修复，再构建新发布包并部署（第 9.1 节） |
| 控制面问题 | 前置检查失败（含发布包被改过）、`run_daily_error.log` 有新行（`current.json` 或发布包文件无效）、运行控制拒绝（并发、恢复无效）、缺环境变量、本地 source-control 问题 | 按证据修复环境；发布包被改过时按第 9.3 节从新提交重新构建、部署，或 `--activate` 回退；不删状态或锁 |
| 等待新 as_of | 同一 as-of 已有 BLOCKED/FAILED 终态的重复触发；as-of 尚未 provider-ready | 等下一个 provider-ready 交易日的普通运行；只有 owner 指示才做终止恢复 |

### 9.3 发布包构建、部署与回退（OPS-086）

步骤 7 已在 2026-10-22 晚完成（第四次切换，发布包 `20261022-545e19985`，源提交 `545e19985`），日报从发布包运行；下面是构建、部署和回退的命令，
每条只在 owner 指示下运行，不登记第二个调度来源。生产的 `<daily_root>` 是 `D:\Work\AITradingSystem_daily`，`<state_root>` 是
`D:\Work\AITradingSystem_data\prod\state`，`<bundle_dir>` 是 `D:\Work\AITradingSystem_releases`。
在开发仓运行下列命令，`<已发布SHA>` 必须已在本地 `origin/main` 上，`<daily_root>`、`<state_root>` 为独立绝对路径，
`<bundle_dir>` 为构建产物目录；执行前替换占位符。不得把发布包目录当状态根。

```powershell
# 构建：只读提交里的清单规则、报告登记表和 git archive 内容，不读未提交改动。
.venv/Scripts/python.exe tools/release/build_daily_release.py --commit <已发布SHA> --out-dir <bundle_dir>
# 首次部署（`<daily_root>` 还没有 current.json 时；生产已经部署过，日常不用）：必须明确状态根，并从现有环境提取依赖约束。
.venv/Scripts/python.exe tools/release/deploy_daily_release.py --bundle <bundle_dir>/<release_id>.zip --daily-root <daily_root> --state-root <state_root> --constraints-from D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\python.exe --python C:\Python314\python.exe
# 若 owner 明确选择不固定依赖，首次部署用 --unpinned 替代 --constraints-from。
.venv/Scripts/python.exe tools/release/deploy_daily_release.py --bundle <bundle_dir>/<release_id>.zip --daily-root <daily_root> --state-root <state_root> --unpinned --python C:\Python314\python.exe
# 后续部署（日常更新用这条，第 9.1 节）：沿用 current.json 的状态根、当前 release 的 .venv_freeze.txt 约束。
.venv/Scripts/python.exe tools/release/deploy_daily_release.py --bundle <bundle_dir>/<新release_id>.zip --daily-root <daily_root> --python C:\Python314\python.exe
# 回退或切换：只核对已安装的 release，不再安装；仍须无活动运行。
.venv/Scripts/python.exe tools/release/deploy_daily_release.py --activate <旧release_id> --daily-root <daily_root>
# 在当前发布包目录运行（生产已注册，见第 9.2 节）：更新同一个任务的入口与工作目录。
powershell -NoProfile -File scripts/ops/register_daily_scheduler_task.ps1 -DailyRoot <daily_root>
schtasks /query /tn "\AITradingSystem Daily Run" /xml
```

`--python` 是建 venv 的解释器，默认是运行脚本的那个解释器（开发仓 `.venv`，Python 3.11），而依赖约束是运行副本 Python 3.14 的
`pip freeze`，所以三条部署命令都明写 `--python C:\Python314\python.exe`。解释器的主次版本和约束来源（`--constraints-from` 的解释器，
后续部署为当前 release 的 venv）不同，部署以 `PYTHON_VERSION_MISMATCH` 拒绝，解包之前就停，不建 release 目录；读不出解释器版本为
`PYTHON_UNUSABLE`。`--unpinned` 没有约束来源，不比较。2026-10-15 的试运行第一次部署就是用开发仓的 3.11 对 3.14 的约束，pip 安装失败。

部署先确认状态根（首次必填，后续不可改变）、任务不在 `Running`、状态根里无日报锁、依赖约束来源明确；
然后核对 zip 旁的 `.sha256`、`RELEASE.json`、文件集合、路径安全及逐文件 sha256，拒绝覆盖已有 release。
检查通过才解到 `<daily_root>\releases\<release_id>`，建 `.venv`、按约束 editable 安装、保存 freeze，
运行隔离的 `release_identity verify`，将清单文件和 `RELEASE.json` 设为只读，再以状态根环境在临时目录输出
`ops daily-plan --fail-on-missing-env`。计划报告不写进生产状态；失败不切换指针或入口。

全部成功才原子替换 `current.json`（版本 `aits_daily_current.v1`，记录当前/上一 release、状态根、源提交与激活时间），
再原子复制该 release 的入口为 `<daily_root>\run_daily.ps1`。该入口设置 `AITS_STATE_ROOT`、
`PYTHONDONTWRITEBYTECODE=1` 后调用发布包里的包装脚本；日报日志和摘要在
`<state_root>\outputs\run_control\scheduler\`，不是 release 目录。
入口读不到指针、字段或文件无效时，向 `<daily_root>\run_daily_error.log` 追加错误并退出 2。
每次部署/回退（含拒绝）向 `<daily_root>\deploy_log.jsonl` 追加时间、动作、结果、拒绝码、release_id 和原因。

安装、freeze、核对、只读设置或计划检查失败时，保留目录并改名为 `<release_id>.failed-<UTC时间戳>`，不删除。
部署脚本**从不删除 release**；超过 3 个只打印 `cleanup_candidate`（排除最新 3 个、当前和上一个），由 owner 决定清理。
不得手改 release 文件；修复应从新提交重新构建、部署，或用 `--activate` 回退。发布包不含 `.git` 和状态目录。
部署和回退只写发布目录、`current.json`、`run_daily.ps1` 和 `deploy_log.jsonl`，不写状态根；production_effect=none；broker_action=none。

### 9.4 运行副本（只在回退时用）

运行副本 `D:\Work\AITradingSystem_ops_runtime` 在步骤 7 之后保留，不更新、不运行日报，只作回退。回退有两层，先用轻的：

1. 回到上一个发布包：`deploy_daily_release.py --activate <旧release_id> --daily-root D:\Work\AITradingSystem_daily`（第 9.3 节，要求无活动运行）。
2. 回到运行副本：在运行副本根目录不带 `-DailyRoot` 运行
   `powershell -NoProfile -File scripts\ops\register_daily_scheduler_task.ps1`，同一个任务改为从运行副本启动。运行副本的 `data/`、`outputs/`、
   `artifacts/` 停在 2026-10-22 第四次切换那天，没有之后几天的日报状态；回退时如果已经过了几个交易日，先和 owner 确认状态怎么带回运行副本，不要直接启动。
   要再回到发布包，用第 9.2 节带 `-DailyRoot` 的注册。

运行副本更新（只在 owner 为回退需要而决定更新时做，日常不做）：只把运行副本移到已经 ship 的提交（`git rev-parse main origin/main` 相等）。在运行副本根目录：

1. 确认没有活动运行：计划任务不在运行（`(Get-ScheduledTask -TaskName 'AITradingSystem Daily Run').State` 不是
   `Running`），`outputs\run_control\daily\locks\` 下没有锁文件。有活动运行就等它结束，不删锁。
2. 确认工作区干净：`git status --porcelain=v1 --untracked-files=normal` 没有输出。不干净就停下报告。
3. `git fetch origin main`，然后 `git checkout --detach <已 ship 的 SHA>`；该 SHA 必须可从 `origin/main` 到达
   （`git merge-base --is-ancestor <SHA> origin/main` 退出码 0）。
4. 不得 `pull`、`reset`、`clean`、`stash`；不改 `data/` 与 `outputs/`；不删除旧 state、ledger、recovery 回执或 intent。
5. 确认 editable 安装仍指向本副本 `src`。`pyproject.toml` 的依赖变化时先在任务记录里说明，再在本副本 `.venv` 里按变化安装；
   没有变化就不动 `.venv`。
6. `.venv\Scripts\aits.exe ops daily-plan --fail-on-missing-env` 应为 `READY`（交易日 capture 模式下也可能是
   `READY_WITH_BLOCKED_BRANCHES`，需说明原因）。
7. 在相关任务记录或需求文档里记下新 HEAD、时间与 daily-plan 结果。
