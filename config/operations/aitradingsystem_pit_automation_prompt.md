固定运营根为 `D:\Work\AITradingSystem_ops_runtime`。初始 Codex project 仅承载此 automation；不得从初始 project checkout 执行业务命令。第一步必须进入固定运营根，读取 automation memory 与该目录的 `docs/operations/operations_runbook.md`。

本 automation 不保存或选择 release commit。调用唯一业务 trigger 前，只设置以下稳定、非 secret 环境合同：

- `AITS_EXTERNAL_SCHEDULER=1`
- `AITS_OPS_CHECKOUT_ROOT=D:\Work\AITradingSystem_ops_runtime`
- `AITS_DEVELOPMENT_CHECKOUT_ROOT=D:\Work\AITradingSystem`
- `AITS_OPS_DEPLOYMENT_RECEIPT=D:\Work\AITradingSystem_ops_runtime\outputs\operations\deployment\active.json`
- `AITS_OPS_PYTHON=D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\python.exe`

不得设置 `AITS_OPS_RELEASE_COMMIT`。Exact release 只能由 active deployment receipt 解析，并由 runtime-local preflight 对 receipt、HEAD、origin/main、runtime Python、import provenance、checkout cleanliness 与 scheduler binding 做一致性验证。任何 mismatch 均在 provider/cache/report mutation 前 fail closed；不得切换、pull、clean、stash、reset 或直接修复 runtime checkout。

唯一外部业务 trigger 是 `D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\aits.exe ops daily-run`，每次 automation invocation 最多调用一次。不得使用全局 `aits`、开发 checkout executable、第二 scheduler 或独立 non-daily trigger。Terminal parent 的 recovery、等待新 provider-ready `as_of`、new-as-of ordinary 与 external/owner blocker 路由只按 runtime policy 和 runbook 解析；不得硬编码某次事故日期、run id、key 或旧 release，不得执行同 `as_of` ordinary、扩大 recovery allowlist、删除或篡改旧 state/ledger/manifest。

若唯一 trigger 返回 FAIL/BLOCKED，本 invocation 不得第二次调用。读取 canonical evidence，按 `CODE_DEFECT`、`CONTROL_PLANE_DRIFT`、`PROVIDER_TRANSIENT`、`OWNER_ACTION_REQUIRED` 或 `WAITING_FOR_NEW_AS_OF` 分型，记录影响、风险、验证覆盖、退出条件和 next owner。需要业务代码修复时，只能在 `D:\Work\AITradingSystem` 按 AGENTS、canonical task register 与 `run-governed-development` 推进；不得直接修改 runtime。代码验证或 deployment 完成不得表述为 `OPERATIONALLY_ACCEPTED`，只有后续新 provider-ready ordinary daily 全链 PASS 才能使用该状态。

在 daily 阶段执行或依规跳过后，按 runbook 对 Atlas 策略研究页面执行 R0 健康核验，并对 development checkout 运行现有 `reports ensure-workflow-health` weekly post-stage。两者不是第二业务 trigger；不得把工程 freshness、workflow-health 或 canary PASS 提升为数据质量、策略有效性、human review、production 或 broker approval。任何 active lease、main/origin divergence、source/hash drift 或 writer/replay failure 均 fail closed，不得切换或清理用户 checkout。

输出中文摘要，列出 `as_of`、release lifecycle state、terminal disposition、执行/跳过任务、关键 artifact、data quality status、failure class、production effect 和人工复核事项。不得写 production weights 或 active shadow weights，不得触发 broker/order/trading action。
