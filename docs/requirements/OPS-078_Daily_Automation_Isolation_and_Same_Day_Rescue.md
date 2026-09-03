# OPS-078：每日 Automation 隔离 Carrier 与同日补救窗口

稳定任务 ID：`OPS-078_DAILY_AUTOMATION_ISOLATION_AND_SAME_DAY_RESCUE`

Owner 指令：`owner_instruction:OPS-078:2026-09-03:add-dedicated-worktree-mechanism`

## 背景与问题

`aitradingsystem-pit` 是周期运营的唯一 external scheduler entry。业务 trigger 已被 OPS-077
约束为只从 `D:\Work\AITradingSystem_ops_runtime` 的独立 Git clone 和 runtime-local executable
执行，但 Codex automation 的 carrier 仍绑定 development project。共享 development checkout
处于任务分支、存在 active lease 或正在 Full/publication 时，automation 的控制面和 post-stage
可能在调用业务 trigger 前 fail closed；若当天没有第二个受治理检查窗口，本应采集的 provider-ready
session 可能直到下一日才被发现，形成 `EXPECTED_AS_OF_NOT_RECORDED` 风险。

这类阻断与 provider 数据本身不同：它通常发生在业务 trigger 之前，因而不会产生 capture manifest、
DQ receipt 或 gap-ledger row。仅查看既有 gap ledger 可能误把“没有记录”当成“没有缺口”。

## 受治理设计决定

### 1. 隔离 carrier

既有 automation 原位改为 `projectless` local target，不再把 development project checkout 作为
启动 cwd。每次 invocation 的第一项动作仍必须显式进入 permanent runtime clone，读取 memory 与
runtime-local runbook，随后才允许做任何业务判断。

本设计实现了 owner 所要求的“每日任务专用工作空间”，但不把业务运行迁移到长期 Git linked
worktree。原因是 runtime policy 已要求：

- 业务 checkout 必须与 development checkout 使用不同 Git common dir；
- exact release 只能来自 active deployment receipt；
- scheduler 不得自动 pull、switch、clean、stash、reset 或追随 mutable main；
- runtime-local Python、import provenance、clean audit 和 rollback evidence 必须绑定 permanent clone。

因此 `projectless carrier + receipt-gated independent runtime clone` 比 linked worktree 更强，也避免
新引入“worktree 分支落后于 main / 被用户占用 / shared common-dir lease 冲突”。Development checkout
只保留为 Atlas 与 workflow-health post-stage 的显式输入；它的 active lease 或 drift 只阻断相应
post-stage，不得阻断已满足 runtime gate 的 daily business trigger。

### 2. 单 scheduler、双窗口

仍只有一个 scheduler ID：`aitradingsystem-pit`，不新增第二条 automation 或 Windows Task Scheduler。
同一 RRULE 在 `Asia/Tokyo` 每日提供两个 invocation window：

1. `PRIMARY`：09:30；
2. `SAME_DAY_RESCUE`：17:30。

每次 invocation 的业务 trigger 上限仍为 1。第二窗口不是无条件 retry：

- 当天 expected provider-ready `as_of` 已有 canonical terminal PASS 时，记录
  `RESCUE_NOT_NEEDED`，不调用 trigger；
- 早间在 trigger 前发生 control-plane blocker，且 fresh key 无 state/active lock、active receipt
  与 exact release 均通过时，下午可按 `READY_FOR_NEW_AS_OF_ORDINARY` 调用一次 ordinary daily；
- 早间已产生 terminal FAILED/BLOCKED parent 时，继续使用四类 terminal disposition；只有满足
  OPS-071 完整 tail recovery contract 才能 recovery，否则保持
  `WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY` 或 `BLOCKED_EXTERNAL_OR_OWNER`；
- 禁止用第二窗口执行同 `as_of` ordinary、重复 provider/capture/DQ/PIT/score、删除旧 state/ledger、
  扩大 recovery allowlist 或绕过 attempt/lock/idempotency gate。

### 3. 缺口可见性

每次 invocation 在 daily 阶段执行或依规跳过后，必须把 resolver 的 expected provider-ready `as_of`
与 canonical state、run ledger、manifest、capture validation 和 gap ledger 对账，至少输出以下一种
`gap_visibility_status`：

- `NO_GAP_EVIDENCE`：expected `as_of` 已有可重放 capture/DQ/PIT/score terminal evidence；
- `GAP_EXPOSURE_PRESENT`：canonical evidence 明确存在 partial/missed/blocking component；
- `EXPECTED_AS_OF_NOT_RECORDED`：expected `as_of` 没有 canonical state/manifest/ledger，不能当作无缺口；
- `INDETERMINATE`：artifact 不可读、identity/date/hash 不一致或上游不足。

摘要同时披露 `invocation_window_role`、expected/canonical `as_of`、是否调用 trigger、data quality、
provider/capture 影响、next window/next owner 和 exit condition。此对账只读 canonical evidence，不得
手工补造 receipt、manifest、gap row 或数据结论。

## 变更范围

- `config/operations/ops_release_promotion.yaml`
- `config/operations/ops_scheduler_checkout.yaml`
- `config/operations/aitradingsystem_pit_automation_prompt.md`
- `src/ai_trading_system/ops_release_promotion.py`
- `src/ai_trading_system/ops_scheduler_checkout.py`
- `docs/operations/operations_runbook.md`
- `docs/system_flow.md`
- scheduler/release/architecture focused tests 与生成 authority
- existing Codex automation `aitradingsystem-pit` 的原位 target/RRULE/prompt 更新

## 验收标准

1. Canonical policy 明确 `projectless` carrier，actual automation config 不得携带 project target、
   project id 或 development cwd；business runtime 仍为 independent clone。
2. Actual scheduler 仍为 1 个 entry，RRULE 精确包含 09:30 与 17:30 两个 JST window，每 invocation
   最多一次 `aits ops daily-run`。
3. Scheduler observation 对 actual config bytes、prompt hash、updated-at、target、无 cwd、双窗口和
   single-entry 约束独立验证；任一 drift fail closed。
4. Rescue window 测试覆盖 primary PASS、pre-trigger blocker 后 fresh ordinary、nonrecoverable terminal
   WAIT、active lock/state blocker 与禁止 same-as-of ordinary。
5. 摘要合同显式区分 `EXPECTED_AS_OF_NOT_RECORDED` 与 gap-ledger 中的零 `MISSED`；没有 artifact 不得
   推断为数据完整。
6. Focused tests、architecture/contract/integration/reproducibility/Full validation 全部通过；普通
   main push 后 exact release promotion、actual scheduler observation、deployment acceptance 与
   runtime-local active scheduler preflight 通过。
7. 只有后续真实 provider-ready ordinary daily 全链 PASS 才能把业务状态称为
   `OPERATIONALLY_ACCEPTED`；本任务工程完成最多为 `SCHEDULER_BOUND`。
8. `production_effect=none`；不写 production/active-shadow weights，不触发 broker/order/trading。

## 临时 workspace 生命周期

- owner：`OPS-078` coordinator；
- purpose：隔离 task registration、implementation、validation、integration 与 release evidence；
- path：`D:\Work\AITradingSystem_ops078_lane`；
- branch：`codex/ops-078-daily-isolation`；
- frozen base：`eab7971d3a41f4802f110200d70620df443341be`；
- exit condition：candidate 完成普通 main push、runtime exact promotion、existing automation 原位更新、
  scheduler binding/preflight 验证且所有唯一 evidence 进入 canonical durable location 后，审计
  tracked/untracked/ignored bytes，释放 lease/publication fence，安全移除 worktree；若阻塞则在本文件
  记录保留原因、next owner 与具体退出条件。

## 当前状态

2026-09-03：任务进入 `VALIDATING`。已实现 projectless/无 cwd scheduler binding、单 entry 双窗口、
每 invocation 单 trigger、gap visibility classifier、canonical prompt、runbook 和 system-flow 更新；
核心实现与 architecture authority 的最终并行 focused validation 为 `54 passed`，Ruff、Black、
architecture manifest、report/catalog authority、compatibility authority 与 canonical task source 均为
`PASS`。Publication transaction `ops-078-daily-isolation-v1` 在 `CANDIDATE_COMMIT_PRE` 因漏声明
`tests/test_devx_006d_report_catalog_flow_authority.py` 而 fail closed；没有提交、发布或 production effect。
该 transaction 已以 `FAILED` 释放，改由补齐 exact shared-path claim 的
`ops-078-daily-isolation-v2` 接续。退出条件是 v2 从完整 dirty attribution 重新经过 generated rebuild、
candidate binding、正式 validation 与 publication closeout，不复用 v1 的任何成功声明。下一步是正式
validation、普通 main push、exact release promotion、
actual automation 原位更新和 runtime-local binding preflight。本阶段不调用 `aits ops daily-run`，不补写
历史 gap，也不宣称 2026-09-02/2026-09-03 数据已完整。
