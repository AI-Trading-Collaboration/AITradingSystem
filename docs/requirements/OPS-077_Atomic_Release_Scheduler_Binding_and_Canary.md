# OPS-077：Atomic Release Scheduler Binding 与 Pre-release Canary

最后更新：2026-09-03

稳定任务 ID：`OPS-077_ATOMIC_RELEASE_SCHEDULER_BINDING_AND_CANARY`

Owner 指令：`owner_instruction:OPS-077:2026-09-03:continue-root-prevention-fix`

状态：`VALIDATING`

## 1. 问题与证据

2026-09-02 的 DATA-001 修复已通过正式验证、普通 push、exact release promotion 与
deployment acceptance，permanent runtime HEAD 和 active deployment receipt 均绑定
`12f1e6458e5af4b7ced444e0f4fdc25365afe965`。但 2026-09-03 的 existing Codex automation
`aitradingsystem-pit` 仍在 prompt 中硬编码旧
`AITS_OPS_RELEASE_COMMIT=e479cce0aaee13ab43cd09f1fe067170aa459f6c`，因此 invocation 在入口
exact-release gate fail closed，未调用唯一 `aits ops daily-run`。

根因不是 fail-closed gate 缺失，而是 release identity 存在两个可变 writer：

1. runtime `outputs/operations/deployment/active.json`；
2. Git 外 Codex automation prompt 中的 `AITS_OPS_RELEASE_COMMIT`。

当前 `ops_release_promotion_policy.v1` 的 scheduler observation 只验证 provider、scheduler id、
entry count、enablement、working directory、runtime Python、trigger 与 required environment
**名称集合**；它不验证 release environment 的实际值，也不绑定 automation prompt/config
bytes。2026-09-02 的 active receipt 仍携带 `observed_at=2026-07-28T07:24:34.3159163Z` 的旧
scheduler observation，证明 stale observation 可以跨 release 被重用。

这使 deployment receipt 可以显示 `ACTIVE_OWNER_ACCEPTED`，而 external automation 仍不可运行。
当前状态只能称为 `DEPLOYED`，不能称为 `SCHEDULER_BOUND` 或 `OPERATIONALLY_ACCEPTED`。

## 2. Owner 决策与目标架构

本任务采用最小 serial contract wave，先消除 mutable release SHA 双写，再建立可审计 canary；
不在本波引入第二 scheduler、broker/production 行为或未经审核的数据源切换。

### 2.1 单一 release authority

- `ops_deployment_acceptance.v2` active receipt 是 scheduler release identity 的唯一可变 authority；
- external automation 不再设置或硬编码 `AITS_OPS_RELEASE_COMMIT`；
- 既有稳定 launcher `aits ops daily-run` 必须先读取固定 canonical active receipt，严格验证 receipt、runtime
  HEAD、origin/main、runtime Python、package provenance、checkout cleanliness 与 promotion state，
  然后在进程内投影 exact release environment 并调用唯一 ordinary/recovery router；
- 若调用者仍提供 legacy `AITS_OPS_RELEASE_COMMIT`，它只能作为 migration assertion，必须与
  active receipt exact match；mismatch fail closed，不能覆盖 receipt。

该波不直接创建新的 versioned runtime directory tree。Immutable release directory + atomic
pointer swap 作为后续兼容升级的目标拓扑；本波先把现有 permanent runtime 从“prompt SHA + receipt
SHA 双写”收敛为“receipt 单写 + runtime self-verification”，从结构上消除本次事故类别。

### 2.2 Stable automation contract

- 在 Git 中新增 canonical `aitradingsystem-pit` prompt contract；prompt 只保存稳定 runtime root、
  receipt path、runtime-local launcher、terminal disposition 与安全边界，不保存 mutable commit、
  某次 incident 的固定日期/key 或历史 release；
- scheduler observation v2 必须记录 actual prompt SHA-256、canonical prompt path/SHA、automation
  updated-at、fresh observation time，以及 schedule/model/status/target 等稳定 binding；
- deployment acceptance 不得接受早于当前 scheduler config update、与 canonical prompt hash 不同、
  或缺少 actual binding values 的 observation；旧 observation 不能跨 binding change 重用；
- Codex automation 更新仍通过 automation API 原位完成，不直接手工编辑 TOML，不新增 scheduler。

### 2.3 Operational acceptance state

发布状态明确分为：

`CODE_FIXED -> DEPLOYED -> SCHEDULER_BOUND -> OPERATIONALLY_ACCEPTED`

- `PROMOTED_NOT_ACTIVATED` promotion event 只证明 runtime 已达到 `DEPLOYED`；
- 包含 fresh scheduler binding 的 `ops_deployment_acceptance.v2` active receipt 才证明
  `SCHEDULER_BOUND`；legacy v1 仅对 policy allowlist 中的既有 release 提供过渡兼容；
- 只有新 provider-ready `as_of` 的 ordinary daily 完成 capture、strict DQ、PIT/SEC/valuation、
  score、dashboard 与 Reader Brief 全链 PASS，才写 `OPERATIONALLY_ACCEPTED`；
- provider transient、control-plane mismatch、code defect、owner action 与 waiting-new-as-of 必须分型，
  不能统一汇报为“已修好”。

### 2.4 Incident-regression pre-release canary

- release candidate 在 promotion 前必须运行 `provider_request_performed=false` 的 incident canary；
- `aits ops release-canary` 在 exact candidate 上自动使用
  `pytest -n 16 --dist loadfile` 执行 reviewed test node，不接受 CLI 调用者提交的任意 PASS JSON；
- evidence 记录 exact candidate、test node、pytest arguments/return code、stdout/stderr SHA、执行时间、
  被移除的 provider credential 名称与 production safety boundary；聚合器重新读取并校验 evidence
  bytes/path/size/SHA 和语义；
- 2026-09-01 unknown risk-event ID 与 signed `eps_revision_90d_pct` incident 是 required regression
  scenario，后续 release 不得删除、替换或跳过。完整 retained daily bundle replay 仍属于后续拓扑，
  本波不从缺失 raw evidence 推造历史 DQ、score 或运营结论。

## 3. 实施阶段

### S0 — 登记、合同与现状审计

- canonical task 注册、supporting requirement、exact frozen base 与 worktree lifecycle；
- 复核 active receipt、automation config、scheduler observation 与发布代码，保留 SHA/时间证据。

### S1 — Receipt-authoritative launcher 与 scheduler policy v4

- scheduler checkout policy 升级为 receipt-authoritative release；
- 加固既有 `aits ops daily-run` launcher contract：解析 active receipt、投影环境、复用现有 checkout preflight，任何
  mismatch 在 provider/cache/report mutation 前停止；
- legacy release env 只允许 exact assertion，不再是 authority；
- CLI 保持唯一 stable scheduler launch/preflight 入口，不改变 `aits ops daily-run` 的业务语义。

### S2 — Fresh scheduler binding

- promotion policy/receipt 升级并绑定 canonical prompt 与 actual automation observation；
- fresh observation 需要 exact prompt hash、updated-at、observed-at、status/schedule/model/target；
- acceptance/activation 拒绝旧 schema、旧 observation、prompt drift、release 双写与 duplicate entry。

### S3 — Pre-release canary 与 incident corpus

- 新增 zero-provider-request canary manifest/result contract；
- exact candidate 自动运行 signed expectation metric 与 unknown risk ID 的 reviewed pytest node；
- missing/tamper/as-of/lineage/provider-request attempt 均 fail closed。

### S4 — 文档、数据流与验证

- 更新 operations runbook、`docs/system_flow.md`、CLI/config/test contract；
- focused pytest 默认 `-n 16 --dist loadfile`；
- 运行 Fast Unit、Architecture、Contract、Integration、Reproducibility 与 Full；
- 任何 failure 保留 parent evidence，不用串行或 gate relaxation 掩盖。

### S5 — 发布与 external binding

- validated candidate 按 publication fence 进入 local main，fetch 后 ordinary non-force push；
- 创建 exact release candidate、promotion 与 deployment acceptance；
- 通过 Codex automation API 原位把现有 automation 切换到 canonical stable prompt，保持 id、
  schedule、model、reasoning、notification 与 target 不变；fresh view/binding 验证 PASS 后才记录
  `SCHEDULER_BOUND`；
- 本 invocation 不执行第二次 daily trigger。下一 provider-ready ordinary daily 才能完成
  `OPERATIONALLY_ACCEPTED`。

## 4. 验收标准

1. automation prompt/config 不含 mutable release commit；release identity 只来自 canonical active receipt；
2. launcher 在任何 provider/cache/report mutation 前验证 receipt/runtime/HEAD/executable/package；
3. legacy release env missing 可正常由 receipt 投影，present mismatch fail closed；
4. scheduler observation 绑定 actual prompt bytes/hash 与 fresh config identity，旧 observation 不可重用；
5. deployment receipt 不得在 scheduler binding 缺失或漂移时声称完整激活；
6. incident-regression canary 保持 zero-provider-request，历史 incident tests 在候选 release 上 PASS；
7. existing scheduler entry count 仍为 1，外部业务 trigger 仍唯一；
8. `CODE_FIXED/DEPLOYED/SCHEDULER_BOUND/OPERATIONALLY_ACCEPTED` 状态可审计；
9. runbook、system flow、policy、CLI、tests 与 release evidence 同步；
10. `production_effect=none`、无 production/active-shadow weight write、broker/order/trading action。

## 5. 路径、依赖与生命周期

- frozen base：`fcb2a420ed1489189ea1ec9a323724943dcaee52`；
- branch：`codex/ops-077-atomic-release-binding`；
- worktree：`D:\Work\AITradingSystem_ops077_atomic_release_binding`；
- owner：OPS-077 coordinator；
- dependency：OPS-070 release/runtime control plane、OPS-073 terminal disposition、OPS-074 retained
  capture consumption、ARCH-005 publication fence；
- temporary workspace exit condition：candidate 已验证并进入 local/remote main，required evidence
  已进入 canonical location，exact release promotion/deployment/automation binding 完成或形成明确
  owner/external blocker，tracked/untracked/ignored audit 无唯一未替代内容、无进程/lease 依赖后，执行
  `git worktree remove` 与 `git worktree prune`；未满足时在本 requirement 与 task event 记录原因、
  next owner 和恢复边界。

## 6. 安全与非目标

- 不新增第二 scheduler 或独立 business trigger；
- 不执行同 `as_of` ordinary，不扩大 OPS-071 recovery allowlist；
- 不删除或篡改历史 failed state/ledger/manifest/raw bytes；
- 不把 canary 当成真实 DQ、score、投资结论或 operational acceptance；
- 不改变研究窗口、score threshold、position cap、production weights 或 broker contract；
- 本任务的 external automation 更新属于已由本轮 owner 指令限定的 R2 local automation change；
  PR、force-push、remote divergence repair 和 production/broker action仍不授权。

## 7. 进度记录

- 2026-09-03：Owner 要求继续推进根治方案。READ_ONLY preflight PASS，local main=origin/main=
  `fcb2a420...`；当前主 checkout 属于 TRADING-2556，因此从 exact main 建立独立 OPS-077 lane。
- 2026-09-03：首次 SINGLE_LANE preflight 按预期仅以 `TASK_NOT_REGISTERED` fail closed；取得
  publication transaction `ops-077-atomic-release-binding-20260903-v1` / lease
  `lease-d08b75efadaae982e9e8`，只允许先登记 task 与 requirement。
- 2026-09-03：完成 policy v2/v4、canonical automation prompt、receipt-authoritative scheduler
  preflight、fresh actual-config observation、deployment acceptance v2、自动 incident canary runner 与
  portable canary evidence promotion；新增 prompt drift、stale observation、legacy SHA mismatch、
  provider-request claim 与 exact pytest-node 回归。聚焦并行验证 45 passed。
- 2026-09-03：在最新 `main` 协调候选 `89a55f59e...` 上，Fast Unit 350、Architecture Fitness
  884、Contract 278、Integration 995、Reproducibility 24 均 PASS；首次 final Full 为
  10149 passed、6 skipped、18 failed。失败分成两类：新 OPS-077 compatibility section 已成为
  current authority，但两个 consumer test 的 section allowlist/count 未同步；隔离 worktree 未携带
  tracked policy 以 SHA-256 精确绑定的三组 ignored validation inputs。v8 仅同步 consumer authority，
  并从 durable main 按 policy hash 核验后导入这些验证输入；不修改研究结果、DQ/PIT 语义或生产状态。
