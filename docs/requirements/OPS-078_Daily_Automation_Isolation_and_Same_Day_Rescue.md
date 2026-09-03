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

1. Canonical policy 明确 `projectless` carrier，actual automation config 不得携带 project target 或
   project id；`cwds` 只允许 Codex projectless local serializer 的 exact neutral home sentinel `["~"]`，
   development/runtime/project 或其他 cwd 均拒绝；business runtime 仍为 independent clone。
2. Actual scheduler 仍为 1 个 entry，RRULE 精确包含 09:30 与 17:30 两个 JST window，每 invocation
   最多一次 `aits ops daily-run`。
3. Scheduler observation 对 actual config bytes、prompt hash、updated-at、target、exact neutral
   carrier cwd sentinel、双窗口和
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
9. Scheduler/release policy 迁移时，旧 active receipt 只有同时命中 reviewed
   `deployment_id + release_commit + size + SHA-256` allowlist，并通过 schema/status/lifecycle、
   content-derived id、runtime identity 与 safety boundary 检查，才能成为 roll-forward predecessor；
   tamper、未登记 receipt 或宽松 field compatibility 必须在 active receipt 写入前阻断。

## 临时 workspace 生命周期

- owner：`OPS-078` coordinator；
- purpose：隔离 task registration、implementation、validation、integration 与 release evidence；
- path：`D:\Work\AITradingSystem_ops078_lane`；
- branches：`codex/ops-078-daily-isolation`、`codex/ops-078-carrier-sentinel-fix`、
  `codex/ops-078-acceptance-rollforward`；
- frozen base：`eab7971d3a41f4802f110200d70620df443341be`；
- exit condition：candidate 完成普通 main push、runtime exact promotion、existing automation 原位更新、
  scheduler binding/preflight 验证且所有唯一 evidence 进入 canonical durable location 后，审计
  tracked/untracked/ignored bytes，释放 lease/publication fence，安全移除 worktree；若阻塞则在本文件
  记录保留原因、next owner 与具体退出条件。

## 当前状态

2026-09-03：任务进入 `VALIDATING`。已实现 projectless/无 development cwd scheduler binding、单 entry 双窗口、
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

首个正式候选 `bffb11dc497ccee272c4009fed0ab28a72b750a5` 的 `fast-unit` 为
`350 passed`；`architecture-fitness` 为 `883 passed / 2 failed`。两个失败均为新增 canonical task
和 system-flow entry 后 repository-wide 精确计数测试未同步（`1057→1058`、`3135→3136`），不涉及
runtime 业务逻辑、provider、DQ/PIT/score 或 production effect。失败 artifact 保存在
`outputs/validation_runtime/architecture-fitness_20260903T122818Z/test_runtime_summary.json`，v2 transaction
已正式关闭；v3 对两处计数作耐久修复并要求从新 candidate 重跑全部六级正式 validation。

v3 新候选的五个前置 tier 全部通过：`fast-unit=350`、`Architecture=885`、`Contract=278`、
`Integration=995`、`Reproducibility=24`。正式 Full 为 `10170 passed / 22 failed / 6 skipped`，失败
artifact=`outputs/validation_runtime/full_20260903T130803Z/test_runtime_summary.json`。其中 4 项是 OPS-078
成为 latest compatibility successor、system-flow 更新后测试仍保留旧 successor/SHA/count；18 项是在隔离
worktree 中缺少既有 Git-ignored retained evidence locator。v4 只修正 current-authority assertions，并按既有
TRADING-2500/2549/2553/2554 hydration 合同，从 canonical development checkout 向相同相对路径复制并逐项
复核以下只读 evidence roots：

- `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json`；
- `outputs/research_trends/operational_forecast/trading_2542i_real_v3/`；
- `outputs/qqq_options/signal_packages/trading_2542i_operational_forecast_real_v3/`；
- `outputs/research/first_layer_composer_v2_foundational_falsification_v1/`；
- `outputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1/`；
- `outputs/research/first_layer_composer_v2_matched_placebo_v1/`。

这些副本只恢复 frozen tests 的 repository-relative locator，不是新 DQ、研究或投资 evidence，不执行
provider、research run、production 或 broker action。复制前要求目标不存在、source 位于 canonical root、
destination 位于 OPS-078 lane；复制后按每个 root 的 sorted relative path、size 与 SHA-256 生成确定性
inventory 并与 source 一致。退出条件是 parent-bound Full 与发布收口完成、canonical source 仍存在且 hash
一致；随后随 exact worktree 清理，不能将副本发布进 Git 或 runtime deployment。

v4 hydration 已完成 source/destination 独立 inventory 等值复核（tree digest 输入为按相对路径排序的
`path<TAB>size<TAB>sha256` UTF-8 行）：

- O1 DQ gate：`1 file / 4,057 bytes / tree_sha256=556ee31b…66849`；文件 SHA-256 仍为
  `ca02b431…a1ca`；
- TRADING-2542I real materialization：`69 files / 6,690,288 bytes /
  tree_sha256=992bb8db…7c911`；
- TRADING-2542I signal package：`1,205 files / 3,478,266 bytes /
  tree_sha256=8954df51…866b6`；
- foundational falsification v1：`6 files / 7,742 bytes / tree_sha256=f6bc55d5…27a6`；
- foundational failure-fix v1：`5 files / 41,469 bytes / tree_sha256=d6675e58…e953`；
- matched-placebo v1：`5 files / 11,153 bytes / tree_sha256=d8225ca3…f4f5`。

所有 destination 在复制前均不存在；hydration 后未修改 canonical source，也未运行任何被复制 artifact 的
producer。后续 cleanup 必须再次计算相同 inventory，并确认 canonical source 仍匹配后随 worktree 移除。

hydration 后对 prior Full failed set 执行 16-worker `--lf`：21 个仍存在的 node 中 `20 passed / 1
failed`；唯一失败是历史 successor payload 已随 authoritative report-flow 更新为 `entry_count=3136`，而
`tests/test_devx_006d_report_catalog_flow_authority.py` 仍断言 3135。另一个旧 system-flow 参数化 node id
因 expected SHA/count 更新而不再存在，不能用 cache absence 视为 PASS。v4 已以失败证据关闭；v5 将该
successor expectation 同步为 3136、重新生成全部 authority，并显式运行包含新参数化 node 的等价完整集合。

v5 的显式 69-test 等价集合为 `68 passed / 1 failed`。唯一失败是同步 system-flow SHA 时把 authority
给出的 `fa388d8c0ba7f36dbcdbd938a9682a16a8742135f2a9dd88c3f278df014203a1` 抄成了错误中段；
render bytes、index 和 live file 本身始终一致。v5 已失败关闭；v6 只纠正该 exact SHA transcription，
重新生成依赖该 test hash 的 architecture/compatibility authority，并从完整 69-test 集合复验。

v6 把 Full-failure 等价集合与 OPS-078 核心/架构回归合并为 137 项，结果 `137 passed`，Ruff
`PASS`；Black 仅要求格式化新增的 successor 常量。因为该 test file 参与 compatibility source hash，v6
未在 generated-post 后直接修改并沿用旧 authority，而是失败关闭；v7 在 generator 前执行格式化并重新
生成全部依赖 authority，随后须重跑同一 137-test suite 与 parent-bound Full。

2026-09-04：v7 candidate `96663843dc7119338524155315d6d3f8dfb77814` 已完成六层正式验证，
Full=`10192 passed / 6 skipped / 0 failed`，普通 push 后 runtime promotion 达到
`PROMOTED_NOT_ACTIVATED`，existing automation 已原位改为 `target=projectless` 与 09:30/17:30 双窗口。
Actual Codex serializer 同时写入 `cwds=["~"]`，而 v7 policy 的 `expected_cwds_empty=true` 将该 neutral
carrier sentinel 误判为 development cwd；runtime-local scheduler observation 因
`SCHEDULER_DEVELOPMENT_CWD_FORBIDDEN` fail closed。未激活 deployment、未运行 daily、无 provider 或
production effect。v8 将该实际产品合同耐久建模为 exact allowlist `["~"]`；空值、development/runtime/
project 或其他 cwd 均继续拒绝，并从新 candidate 重跑 authority、formal validation、promotion 与 binding。

v8 focused=`28 passed`，Ruff/Black 均 PASS；首次 generator replay 在 report-flow build 以
`RCF_SOURCE_SEAL_DRIFT` fail closed，因为上述 system-flow 文档更新后的 exact identity 已变为
`2343730 bytes / SHA-256=497e3a0832a37d793d08a26331f51744c4b98b26f5f50261cff68051feef50e2 /
git_blob=2a3bfc990675114e5e9f1f5ef39023ec610e0ab0`，而 DEVX-006D policy 仍固定 v7 bytes。v8 未提交并已
以 FAIL 释放；v9 同步这组 exact source seal 后必须从头重放全部 generator，不复用 v8 的中间输出。

v9 candidate `3048a2178a383c7f240cb1e9c8aafa53a796913c` 已完成六层正式验证，Full=
`10193 passed / 6 skipped / 0 failed`，并已普通 push、promotion 至 runtime 与完成 actual projectless
双窗口 scheduler observation。首次 deployment activation 在写 active receipt 前以
`DEPLOYMENT_SCHEDULER_BINDING_MISMATCH` fail closed：activation 用 v9 双窗口 policy 重新验证旧 active
receipt 的单窗口 RRULE。旧 active receipt 保持
`deployment_id=ops_deployment_13d42bc41d6fcb3228f8abf28d1717807544b66b`、
`release_commit=ea8937b2a07f5c4fc52ba1c437566017be137baa`、`10907 bytes`、
`SHA-256=b2dd9727ec7afdd7792244b3d6b571b907f92ca6a7f58017604add4bab95b94d` 原始 bytes；runtime HEAD 已
切换到 v9 但 release 仍未激活，未调用 daily/provider/DQ/PIT/score。v10 以这四项 exact commitment
建立 reviewed prior-active roll-forward gate，并测试未登记/tampered receipt 继续阻断；不使用宽松
schema、RRULE、target 或 cwd compatibility。

v10 candidate `20f969236a0abd956bf2f0800d74c39545226cea` 的 `fast-unit=350 passed`；首次
Architecture 为 `884 passed / 1 failed`，唯一失败
`tests/test_arch_004e_devex.py::test_architecture_fitness_passes_and_detects_stale_manifest`
显示 module manifest、test manifest 与 aggregate shadow index 未按候选重建。根因是 coordinator
把 publication generator id `architecture-manifests` 错映射为 `architecture_arch005_registry.py
generate`，而正式 writer 应为 `scripts/architecture_devex.py generate`。失败 artifact=
`outputs/validation_runtime/architecture-fitness_20260903T181337Z/test_runtime_summary.json`；v10 已以
FAILED 释放，v11 使用正确 writer 重建全部 architecture manifests、report-flow/compatibility authority，
提交新候选并从头重跑六层正式验证，不复用 v10 的 fast-unit PASS 声明。
