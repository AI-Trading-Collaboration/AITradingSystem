# OPS-082：确定性 Windows 调度替代 Codex automation

最后更新：2026-10-11

稳定任务 ID：`OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER`

状态：`IN_PROGRESS`（M1 实施中；见第 7 节）

原始总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`

GOV-008 后依据：`docs/requirements/GOV-008_Research_First_Governance_Refactor.md` 第 19～21 节；
`docs/operations/operations_runbook.md` 第 1、3、9 节；任务记录
`tasks/OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER.yaml`。

## 1. 问题

生产 daily 的入口 `aits ops daily-run` 本身是确定性的；GOV-008 后，运维手册第 1 节仍记录：此前外部触发是
Codex automation `aitradingsystem-pit`，在独立运行副本里调用 `aits ops daily-run`。pi 没有调度器；若不重新指定外部触发者，
daily 的启动仍依赖 owner 对 Codex automation 的启停决定。

GOV-008 已删除原设计依赖的旧发布与调度机制，旧方案不能按 2026-09-23 文本直接实施：

- release promotion / release candidate / canary / promote 与旧 validation tier、fence、lease 机制已删除；发布改由
  `python tools/gov008/ship.py` 完成。
- `ops_release_promotion.py`、`ops_scheduler_checkout.py`、scheduler checkout preflight、Codex scheduler observation 已删除。
- 旧 runbook 中的 deployment acceptance 命令已删除；`daily-run` 不再持有 checkout guard，`--manual-execution` 选项保留但已无作用。
- workflow health 遥测与周任务已删除。

## 2. Owner 决定

`owner_decision:GOV-007:2026-09-23:pre_migration_convergence_v1` 第 2 条：改为确定性调度，过渡期保留
Codex automation。GOV-008 后 AGENTS.md 与 runbook 已不再维持"不得使用 Windows 任务计划程序"的旧禁令。

2026-10-11 owner（Baker）在聊天中确认以下两条决定（经发起本任务的线程会话转达）：

**`owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1`**

- a) Windows 任务计划程序是唯一外部调度入口；
- b) Codex automation `aitradingsystem-pit` 停用（owner 已于 2026-10-11 约 11:36 JST 在 Codex 应用里暂停），任一时刻只有一个调度来源；
- c) PowerShell 包装脚本设置环境、调用一次运行副本的 `aits.exe ops daily-run`、用代码生成中文状态摘要，LLM 不参与，通知只写文件；
- d) 保留 17:30 rescue，作为同一个计划任务的第二个触发时间（09:30、17:30 Asia/Tokyo），重复触发由 daily-run 现有运行控制去重；
- e) 终止恢复不再依赖 `AITS_OPS_DEPLOYMENT_RECEIPT`，改为校验运行副本工作区干净且 HEAD 是 `origin/main` 上可达的提交，
  用它代替 receipt 的 `candidate_commit` 与父运行 manifest 的 `git_commit` 比对；
- 计划任务：当前用户、仅登录时运行、StartWhenAvailable、不并行实例、不保存密码、不用管理员。

**`owner_decision:GOV-008:2026-10-11:research_after_migration`**

研究策略调整（含付费源停订后的替代）推迟到工程迁移结束后讨论。付费数据源（FMP、Marketstack）停订期间，ordinary daily
全链 PASS 不是 OPS-082 的完成条件；本次只做工程验证。

同日的任务处置（OPS-078 并入本任务后 DROPPED；OPS-070/072/073/074/079 改 VALIDATING 等）记录在 GOV-008 需求文档第 21 节之后。

## 3. 设计（按上述决定实施）

| 部分 | 实现 |
|---|---|
| 运行副本 | `D:\Work\AITradingSystem_ops_runtime`：独立 clone，detached 到已 ship 的 main 提交；`.venv` 为 editable 安装（`_editable_impl_ai_trading_system.pth` 指向本副本 `src`）。更新步骤见运维手册第 9.1 节 |
| 计划任务 | `\AITradingSystem Daily Run`，由 `scripts/ops/register_daily_scheduler_task.ps1` 幂等注册（`Register-ScheduledTask -Force`；发现另一个启动 daily-run 的任务时拒绝）。两个每日触发 09:30、17:30（主机时区必须是 Tokyo Standard Time）；当前用户、`LogonType Interactive`（仅登录时运行，不存密码）、`RunLevel Limited`、StartWhenAvailable、`MultipleInstances IgnoreNew`、单次运行上限 6 小时 |
| 无窗口启动 | 动作是 `conhost.exe --headless powershell.exe -NoProfile -NonInteractive -File <运行副本>\scripts\ops\daily_scheduler_run.ps1`（owner 偏好后台进程不弹控制台窗口）。conhost 不转发子进程退出码，所以计划任务的"上次运行结果"只表示包装脚本已启动，运行结论看日志与摘要 |
| 包装脚本 | `scripts/ops/daily_scheduler_run.ps1`：① 前置检查：工作区干净（`git status --porcelain`）、HEAD 可从本地 `origin/main` 到达、`aits.exe` 存在，任一失败不调用 daily-run、退出码 2；② 只设置 daily-run 实际读取的环境变量（grep `src` 确认：`FMP_API_KEY`、`MARKETSTACK_API_KEY`、`SEC_USER_AGENT`、`OPENAI_API_KEY`、`CONGRESS_API_KEY`、`GOVINFO_API_KEY`），进程里没有时取用户环境变量，日志只记 PRESENT/MISSING；不设置 Codex automation 旧合同里的 `AITS_*` 变量（已无代码读取）；③ 调用一次 `<运行副本>\.venv\Scripts\aits.exe ops daily-run`；④ 输出与退出码写 `<运行副本>\outputs\run_control\scheduler\<东京日期>_<窗口>.log`（已存在则加 `_2`、`_3`，不覆盖）；⑤ 调用摘要脚本 |
| 摘要 | `scripts/ops/daily_scheduler_summary.py`（只用标准库，读文件，不调用 LLM）：读 daily-run 输出、run state 与 ledger、run metadata、capture manifest、DQ 发现指针与回执，写 `<stem>.summary.md`（中文）与 `<stem>.summary.json`：as_of、窗口、终态、每步状态、每个 capture 组件、DQ 状态、阻断原因分类。分类：数据源不可用（provider 鉴权/额度/不可用等 capture blocker，含付费源停订）、数据质量未通过（validate_data FAIL）、代码缺陷（未处理异常、非数据原因的步骤失败）、控制面问题（前置检查、运行控制、缺环境变量、本地 source-control 问题）、等待新 as_of（同一 as_of 已有终态的重复触发、as_of 尚未 provider-ready）。错误摘录再做一次密钥脱敏 |
| 去重 | 不新增逻辑：同一 workflow/规格/as_of 已 PASS 返回 `RUN_CONTROL_ALREADY_COMPLETE`；已有 BLOCKED/FAILED 终态返回 `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`，不执行任何步骤 |
| 终止恢复 | `OperationsRecoveryRequest` 升为 v2：`current_release_commit` = 运行副本 HEAD，另记 `runtime_checkout_root` 与 `runtime_origin_main_commit`，去掉 deployment receipt 字段。CLI 构造请求时与运行控制获取租约时各检查一次（`platform/operations/runtime_checkout.py`）：工作区干净、HEAD 可从 `origin/main` 到达、HEAD 与请求一致；仍要求与父运行 manifest 的 `git_commit` 不同。拒绝码 `RECOVERY_RUNTIME_CHECKOUT_DIRTY`、`RECOVERY_RUNTIME_CHECKOUT_HEAD_NOT_ON_ORIGIN_MAIN`、`RECOVERY_CURRENT_RELEASE_MISMATCH`、`RECOVERY_RUNTIME_CHECKOUT_GIT_FAILED`。已写下的 v1 恢复回执是不可变历史，不回读 |
| 删除 | `src/ai_trading_system/ops_scheduler_business_contract.py` 及其单元测试、`config/operations/aitradingsystem_pit_automation_prompt.md`（无代码或研究记录引用；Codex automation 停用后不再需要） |

## 4. 依赖与并入

- **OPS-078 并入本任务**（Codex automation 的隔离 carrier 与同日 rescue 窗口）：rescue 保留为同一计划任务的 17:30 触发时间
  （决定 d），不形成第二个调度入口；隔离 carrier 的问题随 Codex automation 停用消失。OPS-078 改为 DROPPED。
- OPS-070、OPS-072、OPS-073、OPS-074、OPS-079 的真实 daily 验收改为"OPS-082 新调度下、付费数据源恢复后的第一次 ordinary
  daily 全链"（状态 VALIDATING）；OPS-073/079 的恢复路径已不依赖 deployment receipt。
- 付费数据源 FMP 与 Marketstack 目前停订：依赖它们的 capture 组件会失败并留下证据，整体 BLOCKED_DEPENDENCY 或 FAIL，
  不评分、不调用 OpenAI、不生成日报。

## 5. 验收标准

本次（工程验证，`BASELINE_DONE`）：

- 计划任务按上表注册，`schtasks /query /xml` 可见两个触发时间、运行身份、仅登录时运行、不提权；
- 通过 `Start-ScheduledTask` 触发一次：daily-run 执行一次，依赖付费源的 capture 组件失败并留下错误证据，不评分、不调用
  OpenAI、不生成日报；摘要归为"数据源不可用"；
- 再触发一次：同一 as_of 不重跑，摘要归为"等待新 as_of"；
- 任一时刻只有一个调度来源（Codex automation 为 PAUSED）；
- 终止恢复不依赖 deployment receipt（测试覆盖工作区不干净、HEAD 不在 origin/main 上、请求后 HEAD 变化三种拒绝）；
- 运维手册与 `docs/system_flow.md` 同步。

转 `DONE`：付费数据源恢复后的第一次 ordinary daily 全链 PASS（DQ 门禁与报告质量状态可见）。production_effect 只限 daily
既有运营边界，不新增交易或 broker 行为。

## 6. 已关闭的问题

第 6 节原列的问题（唯一入口与停用条件、触发时间与运行身份、环境合同与通知、rescue 窗口、恢复依赖、OPS-070..079 去向）
已由第 2 节两条决定与同日任务处置回答。剩余开放项：无（付费源恢复时间不由本任务决定）。

## 7. 进度

- 2026-09-23：登记（GOV-007 P0-B）。
- 2026-10-11：按 GOV-008 后现状重述记录：原方案依赖的 release promotion、scheduler checkout preflight、Codex scheduler observation、deployment acceptance 与 workflow health 机制已删除；当前方向是唯一外部调度入口调用一次 `aits ops daily-run`，Windows Task Scheduler 与包装脚本仍需 owner 确认；终止恢复仍受 `AITS_OPS_DEPLOYMENT_RECEIPT` / 现存 receipt 限制。
- 2026-10-11：owner 决定 `deterministic_scheduler_v1` 与 `research_after_migration`（第 2 节）。M1 实施：包装、注册、摘要脚本，
  恢复改为运行副本校验，测试，文档；删除旧业务合同模块与 automation 提示词。只读核对：运行副本 `D:\Work\AITradingSystem_ops_runtime`
  为 detached `6498d0300`，工作区干净，`outputs\run_control\daily\locks` 为空，最后一次 daily PASS 为 as_of 2026-09-04；
  `~/.codex/automations/aitradingsystem-pit/automation.toml` 的 `status = "PAUSED"`（只读，未编辑）；系统代码页 65001，
  无需额外的 Python 编码变量；本机原有计划任务中没有启动 daily-run 的任务。
