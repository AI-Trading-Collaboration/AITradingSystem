# OPS-074 Daily Official-Policy Capture Consumption

## 状态

- 任务 ID：`OPS-074_DAILY_OFFICIAL_POLICY_CAPTURE_CONSUMPTION`
- 优先级：`P0`
- 状态：`VALIDATING`
- 当前 owner：Codex（耐久修复、验证、发布与 deployment acceptance）
- 后续 owner：operations owner（下一个合法 new as-of ordinary run 人工复核）
- 登记日期：2026-08-05

## 事件与根因

2026-08-05 existing automation 在 exact runtime release
`4a0d168fbeb773be3ced8065cfe1b3194902543f` 上启动了唯一一次 ordinary
daily run：

- `as_of=2026-08-04`
- run ID：`daily_ops_run:2026-08-04:20260805T003613Z`
- idempotency key：`operations_run_d84f59992f7f758ab7576d21`
- terminal status：`FAILED`
- failed step：`score_daily`

`daily_input_capture` 已成功向 Federal Register 发起请求、保存原始 bytes、生成
candidate CSV 和 source report，且 capture validation 为 `PASS`。但 `score_daily`
没有消费这些已验证留存证据，而是再次调用 live official-policy fetch。第二次
Federal Register 请求返回 HTTP 503，导致评分失败。根因是 capture 与 consumer
之间缺少受管理的留存 artifact 绑定，不是应通过扩大 retry 或放宽失败门禁处理的
provider 问题。

## 安全与恢复边界

- 当前 automation invocation 的唯一 daily trigger 已用完，修复和 promotion 后不得第二次调用。
- 保持失败 parent 的 state、ledger、manifest、diagnostic 和原始 bytes immutable。
- 不扩大 `terminal_recovery_allowed_from_step_ids`，不用 same-as-of ordinary 绕过 recovery contract。
- 不写 production weights 或 active shadow weights，不触发 broker/trading action。
- 本修复只改变 capture-active daily 的 official-policy 输入绑定；manual/non-daily live-fetch 入口保持现有合同。

## 最佳耐久方案

1. 在 capture-active daily orchestration 中把 exact date-scoped capture manifest 显式传给
   `score_daily`。
2. `score_daily` 通过同一 capture validator 确认 policy/status/component、artifact
   exact path/size/SHA 与 same-as-of 绑定。
3. 从已验证 candidate CSV 读取候选，对每个候选再校验 `as_of`、
   `production_effect=none`、`review_status=pending_review`、raw path 归属与
   raw SHA lineage。
4. 评分流程在此模式下不调用 provider；missing、tamper、drift、duplicate 或
   as-of mismatch 全部 fail closed，不回退至 live fetch。
5. 报告显式记录 capture manifest/source report/candidate artifact 及校验状态，
   保留 `production_effect=none`。

## 阶段与依赖

1. **S0 证据与登记**：固定 canonical 失败证据、根因、恢复边界和验收标准。
2. **S1 留存输入合同**：实现 strict loader/validator 与 score CLI/orchestration 显式传递。
3. **S2 报告与数据流**：更新 policy/version、operations runbook 与 `docs/system_flow.md`。
4. **S3 focused validation**：覆盖正向复用、零 network call、tamper/as-of/raw-lineage
   fail-closed 与 orchestration 参数绑定。
5. **S4 formal closeout**：在 exact candidate 上完成六类验证，普通推送 main，
   promotion exact release 并通过 deployment acceptance。

S1 依赖 S0；S2 与 S1 同一变更提交；S3 依赖 S1/S2；S4 依赖 S3。

## 实施与临时工作区计划

- governed mode：`SINGLE_LANE`
- frozen base：`5dc32d240a9fe440e3d7b8fe6a5651a0461849f9`
- task branch：`codex/ops-074-official-policy-capture-consumption`
- implementation checkout：`D:\Work\AITradingSystem`
- 不创建额外临时 worktree，避免复制未经审计的运行证据；原 development
  checkout 在任务分支上实施，合并/push 后返回 clean local `main`。
- 受 known-unrelated exclusion 保护的路径不打开、不哈希、不修改。

## 验收标准

- capture-active daily score 在测试中对 live official-policy fetch 零调用，且评分使用
  same-as-of validated retained candidates。
- manifest/candidate/raw artifact 任一 path、size、SHA、as-of 或 lineage 不匹配时稳定
  fail closed，不回退为 provider 重请。
- canonical official-policy/score 报告显式披露留存 capture 证据与零 provider side effect。
- `daily_input_capture` policy/workflow identity 变更可审计，旧 failed parent 不被重写。
- focused tests、Ruff、Black 与 Architecture/Contract/Report/Reproducibility/Integration/Full
  正式验证在最终 exact candidate 通过。
- local main 与 remote main 等于已验证 candidate；runtime active deployment receipt 切换到
  该 exact release 并通过 deployment acceptance。
- 无 production/shadow weight 变更，无 broker/trading action，`production_effect=none`。

## 开放问题与退出条件

开放问题只限于 formal validation 中可能出现的其他真实 contract 漂移；不需要 owner
改变投资 policy 或 recovery allowlist。任务在实现、六类验证、普通 main push、exact
release promotion/deployment acceptance 全部完成后转 `VALIDATING`；最终运营验收由下一个
合法 new as-of ordinary run 提供，不在本 invocation 伪造。

## 进度日志

- 2026-08-05：S0 完成。canonical evidence 证明 capture official-policy PASS，score
  的第二次 Federal Register 请求 HTTP 503；登记耐久修复方案与不可绕过的
  recovery/production 边界。
- 2026-08-05：S1～S3 完成并转 `VALIDATING`。capture-active score 现在显式
  绑定 exact manifest，复用 capture validator，严格校验 download rows/candidate/raw
  lineage，并写入 `provider_request_performed=false` 证据报告；不默认回退
  live fetch。focused parallel pytest=`182 passed`、Ruff/Black PASS；开发版 loader 对
  runtime 2026-08-04 真实留存证据只读校验得到 `PASS / 8 payloads /
  406 candidates / provider_request_performed=false`。scoped mypy 仅保留仓库既有
  `87 errors`，本次引入的额外错误已清零。等待 S4 exact-candidate 六类正式
  验证、普通 main push 与 exact release promotion/deployment acceptance。
