固定运营根为 `D:\Work\AITradingSystem_ops_runtime`。本 automation 必须使用 projectless 隔离 carrier，不得绑定 development project、project id 或 development cwd；不得从初始执行目录运行任何业务或 post-stage 命令。第一步必须进入固定运营根，读取 automation memory 与该目录的 `docs/operations/operations_runbook.md`。若 actual automation target/cwd 不满足该合同，在 provider/cache/report mutation 前按 `CONTROL_PLANE_DRIFT` 阻断。

本 automation 不保存或选择 release commit。调用唯一业务 trigger 前，只设置以下稳定、非 secret 环境合同：

- `AITS_EXTERNAL_SCHEDULER=1`
- `AITS_OPS_CHECKOUT_ROOT=D:\Work\AITradingSystem_ops_runtime`
- `AITS_DEVELOPMENT_CHECKOUT_ROOT=D:\Work\AITradingSystem`
- `AITS_OPS_DEPLOYMENT_RECEIPT=D:\Work\AITradingSystem_ops_runtime\outputs\operations\deployment\active.json`
- `AITS_OPS_PYTHON=D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\python.exe`

不得设置 `AITS_OPS_RELEASE_COMMIT`。Exact release 只能由 active deployment receipt 解析，并由 runtime-local preflight 对 receipt、HEAD、origin/main、runtime Python、import provenance、checkout cleanliness 与 scheduler binding 做一致性验证。任何 mismatch 均在 provider/cache/report mutation 前 fail closed；不得切换、pull、clean、stash、reset 或直接修复 runtime checkout。

唯一外部业务 trigger 是 `D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\aits.exe ops daily-run`，每次 automation invocation 最多调用一次。不得使用全局 `aits`、开发 checkout executable、第二 scheduler 或独立 non-daily trigger。Terminal parent 的 recovery、等待新 provider-ready `as_of`、new-as-of ordinary 与 external/owner blocker 路由只按 runtime policy 和 runbook 解析；不得硬编码某次事故日期、run id、key 或旧 release，不得执行同 `as_of` ordinary、扩大 recovery allowlist、删除或篡改旧 state/ledger/manifest。

现有唯一 scheduler entry 每日有两个 Asia/Tokyo invocation window：09:30 `PRIMARY` 与 17:30 `SAME_DAY_RESCUE`。先按当前本地时间记录 `invocation_window_role`。Rescue 不是无条件 retry：若 expected provider-ready `as_of` 已有完整 terminal evidence，记录 `RESCUE_NOT_NEEDED` 且不调用 trigger；若 primary 在 trigger 前被 control-plane 阻断，fresh key 无 state/active lock 且 receipt/exact release 均通过，rescue 可执行一次 ordinary daily；若 primary 已产生 terminal FAILED/BLOCKED，则仍只按四类 disposition 决定 recovery、WAIT 或 BLOCKED，禁止用同 `as_of` ordinary 重做 provider/capture/DQ/PIT/score。

若唯一 trigger 返回 FAIL/BLOCKED，本 invocation 不得第二次调用。读取 canonical evidence，按 `CODE_DEFECT`、`CONTROL_PLANE_DRIFT`、`PROVIDER_TRANSIENT`、`OWNER_ACTION_REQUIRED` 或 `WAITING_FOR_NEW_AS_OF` 分型，记录影响、风险、验证覆盖、退出条件和 next owner。需要业务代码修复时，只能在 `D:\Work\AITradingSystem` 按 AGENTS、canonical task register 与 `run-governed-development` 推进；不得直接修改 runtime。代码验证或 deployment 完成不得表述为 `OPERATIONALLY_ACCEPTED`，只有后续新 provider-ready ordinary daily 全链 PASS 才能使用该状态。

在 daily 阶段执行或依规跳过后，按 runbook 对 Atlas 策略研究页面执行 R0 健康核验，并对 development checkout 运行现有 `reports ensure-workflow-health` weekly post-stage。两者不是第二业务 trigger；不得把工程 freshness、workflow-health 或 canary PASS 提升为数据质量、策略有效性、human review、production 或 broker approval。任何 active lease、main/origin divergence、source/hash drift 或 writer/replay failure 均 fail closed，不得切换或清理用户 checkout。

Daily 阶段之后还必须把 resolver 的 expected provider-ready `as_of` 与 canonical state、run ledger、manifest、capture validation 和 gap ledger 对账，输出唯一 `gap_visibility_status`：`NO_GAP_EVIDENCE`、`GAP_EXPOSURE_PRESENT`、`EXPECTED_AS_OF_NOT_RECORDED` 或 `INDETERMINATE`。Expected `as_of` 没有 state/ledger/manifest 时必须使用 `EXPECTED_AS_OF_NOT_RECORDED`，不能因为旧 gap ledger 的 `MISSED=0` 就声称没有缺口。该对账只读 canonical bytes，不得补造 artifact。

输出中文摘要，列出 `invocation_window_role`、expected/canonical `as_of`、release lifecycle state、terminal disposition、执行/跳过任务、关键 artifact、`gap_visibility_status`、data quality status、failure class、next window/next owner、production effect 和人工复核事项。不得写 production weights 或 active shadow weights，不得触发 broker/order/trading action。
