# DATA-GOV-001 D0E `daily_score_daily` 首个逐 Consumer 迁移

最后更新：2026-07-29

## 任务信息

- task id：`DATA-GOV-001_D0E_DAILY_SCORE_CONSUMER_MIGRATION`
- parent：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- priority：`P0`
- status：`DONE`
- exact consumer：`daily_score_daily@1.0.0`
- owner：data platform owner / operations consumer coordinator / architecture coordinator
- production effect：`none`
- broker action：`none`

## Owner 授权与目标

Owner 已要求按
`D0C crash durability -> ACL -> per-consumer migration`
推进。D0C 与 D0D 均已完成 scoped capability 验收；既有 Wave15 Owner 决策又只授权
`daily_score_daily@1.0.0`，因此 D0E 固定选择该 consumer，不扩大到
`daily_validate_data`、weekly/monthly consumer、QLD automatic selection、production
weights 或 broker。

D0E 要解决的是：一个 exact consumer 只有在同一个 exact store / publication identity
上同时取得 strict DQ、immutable publication、crash durability、ACL enforcement 和
consumer authorization PASS 后，才能获得 consumer-scoped migration capability。历史
D0A false safety 字段继续 immutable；D0E 使用新的独立 attestation，不翻转 generic
global cutover。

## 当前证据不可直接组合

现有 PASS 证明了三个不同边界，但不构成同一 store 的迁移证明：

1. canonical DQ receipt
   `dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4`
   与 `daily_score_daily@1.0.0` authorization 绑定 canonical `data/raw` publication；
2. D0C bundle
   `data_foundation_d0c_bundle_0d36f7073a10d7b1db0f94be750b0b7f`
   绑定独立 durability rehearsal publication/store；
3. D0D bundle
   `data_foundation_acl_bundle_3f68c2174cf4ffe1753ef8b9f32de5ea`
   绑定另一 isolated ACL store，且 live root 已按验收合同清理。

因此不得把三个 bundle id 放入同一报告后直接宣称 migration PASS。D0E 必须在一个新的
exact isolated candidate store 上重建共同 store identity 的完整证据链；若 DQ、
publication、durability、ACL 或 consumer authorization 指向不同 root、generation、
manifest 或 bytes，validator 必须 fail closed。

## 分阶段实现

### S0：same-store 合同与 readiness

- 冻结 `data_foundation_consumer_migration_attestation.v1`，至少绑定 exact consumer
  id/version、candidate store identity、resolved root、publication transaction/generation、
  file manifest、strict DQ receipt、durability attestation、ACL attestation、consumer
  authorization、policy/validator版本与全部 source checksum；
- reviewed policy 明确只接受 `PASS / 0 error / 0 warning`，任一 missing、warning、
  expiry、identity/path/profile/checksum/tamper/drift 均阻断；
- D0C/D0D historical bundles只作为 capability/protocol precedent，不作为 D0E 的
  same-store PASS evidence；
- 输出 `production_effect=none`、`generic_consumer_cutover_allowed=false`、
  `automatic_non_daily_dispatch=false`。

### S1：isolated candidate store

- 在任务专属、可清理的 isolated root 内，用 canonical durable publication path 建立
  candidate generation；禁止修改 repository `data/raw`、现行 scheduler runtime、外部
  provider cache 或 production store；
- candidate 必须保存 exact immutable manifest、publication identity 与来源 checksum，
  并通过 D0C durable commit/profile validator；
- 对同一个 resolved store root 应用 D0D reviewed ACL policy，并重新生成该 root 的 native
  ACL attestation；不得复用其他 root 的 historical PASS。

### S2：same-store DQ 与 authorization

- strict DQ runner必须显式消费 candidate publication，而不是通过默认路径回落到
  `data/raw`；
- receipt、publication companion、durability与ACL evidence须共享 exact store identity
  和 generation lineage；
- 为 `daily_score_daily@1.0.0` 生成 content-addressed、可撤销的 migration
  authorization；不能授权其他 consumer，也不能把历史全局 false 改写为 true；
- unsupported filesystem、principal、session/calendar/source或validator drift均返回 typed
  blocker，不得降级为 warning PASS。

### S3：consumer-scoped rehearsal

- negative matrix至少覆盖 missing/warning/FAIL/expired、consumer/profile mismatch、
  publication/DQ/durability/ACL/store identity drift与artifact tamper；
- 任一 negative case 必须在 runner 和 downstream artifact 写入前阻断：
  `runner_calls=0`、`downstream_artifacts=0`；
- positive case只在 isolated candidate store和任务 output root执行 controlled
  `daily_score_daily` rehearsal；不得写 production weights、active shadow weights或 broker；
- rehearsal结束后按生命周期规则保存 canonical evidence并清理非必要 live store；若清理受阻，
  记录 exact path、风险、next owner和exit condition。

### S4：formal validation 与关闭

- focused DQ/publication/durability/ACL/consumer migration tests，默认 xdist；
- Ruff、strict mypy、compileall；
- task registry、DevEx、system flow、compatibility/deprecation freshness；
- Architecture、Contract、Report、Reproducibility、Integration；自然集成边界运行
  parent-bound Full；
- governed commit、latest-main revalidation、local-main fast-forward、ordinary non-force
  push、local/remote SHA equality、lease release和temporary workspace cleanup。

## 验收标准

1. 只覆盖 `daily_score_daily@1.0.0`，其他 consumer与automatic non-daily保持未授权；
2. exact publication、strict DQ、durability、ACL与consumer authorization共享同一 store
   identity/generation/manifest/bytes lineage；
3. strict `PASS / 0 error / 0 warning` only，所有 drift/tamper/expiry/mismatch负例 fail
   closed；
4. positive isolated rehearsal可执行，所有负例保持
   `runner_calls=0 / downstream_artifacts=0`；
5. live `data/raw`、provider cache、scheduler、QLD、strategy/backtest/weights、production与
   broker均不改变；
6. historical D0A/D0B/D0C/D0D bytes与 false safety fields不重写，generic cutover继续
   false；
7. formal gates与 governed closeout全部 PASS 后任务才可转 `DONE`。

## 当前进度

- 2026-07-29：根据 Owner 顺序完成任务登记并冻结 S0～S4 边界。READ_ONLY 审计确认
  `daily_score_daily@1.0.0` 是唯一既有 Owner-authorized exact consumer；同时确认 canonical
  DQ、D0C 与 D0D 当前绑定不同 store identity，不能直接组合为 migration PASS。下一步先完成
  governed preflight，再实现 same-store isolated candidate；尚未修改 runtime、live store、
  scheduler、production或broker。
- 2026-07-29：首次真实 replay 输出
  `outputs/validation_runtime/data_foundation_d0e_20260729T045000Z/` 在 legacy projection
  copy 前因 transaction path 被重复拼接而 fail closed；没有生成 migration attestation 或
  bundle，source operations runtime保持只读。该目录只含不完整candidate副本，不能满足empty-root/
  ACL precondition，且无unique canonical evidence。按exact absolute allowlist执行PowerShell递归
  删除时被host command policy拒绝；未使用跨shell或自建删除绕过。目录暂保留，next owner为本地
  operator，exit condition为确认后续canonical D0E bundle PASS且本目录仍无unique evidence后删除
  exact path。实现已修正publication-store-relative normalization；后续尝试必须使用新的空、
  task-identifiable output root。
- 2026-07-29：第二次真实 replay 输出
  `outputs/validation_runtime/data_foundation_d0e_20260729T045500Z/` 已在 isolated candidate
  上执行 strict DQ，但 verifier 因 historical `download_manifest.csv` 的 absolute
  `output_path` 仍绑定 source operations runtime 而 fail closed；没有生成 migration
  attestation 或 bundle，source operations runtime仍保持只读。该目录不含可作为 D0E PASS
  的 unique canonical evidence，且此前同类 exact cleanup 已被host command policy拒绝，
  因此不再尝试未经授权的删除绕过；next owner与exit condition同首次失败目录。直接修复方案
  固定为通过既有 canonical `LEGACY_LOCAL_CACHE_IMPORT` 合同把historical publication bytes
  发布为新的candidate transaction和candidate-specific manifest；禁止改写historical
  manifest、放宽DQ verifier或拼接不同store的证据。
- 2026-07-29：第三次真实 isolated replay
  `outputs/validation_runtime/data_foundation_d0e_20260729T045400Z/` PASS，bundle id为
  `data_foundation_consumer_migration_bundle_e19d0f959975c54f400025919921d3f6`。Exact
  `daily_score_daily@1.0.0` 已在同一candidate store上绑定新的durable publication、native
  ACL attestation、strict DQ `PASS / 0 error / 0 warning`、新的consumer authorization与
  controlled read rehearsal；`production_effect=none`、`broker_action=none`，没有修改source
  operations runtime、live repository `data/raw`、scheduler、QLD、weights或broker。任务进入
  S3 negative/focused validation与S4 formal validation；candidate按policy保留用于revalidation。
- 2026-07-29：S3/S4与自然集成边界完成。Focused migration/DQ/publication/durability/ACL
  matrix=`155 passed`，compatibility/deprecation=`143 passed`；Architecture/Contract/Report/
  Reproducibility/Integration分别为`784/276/57/24/995 passed`，Integration=`642 warnings`。
  唯一natural-boundary Full=`7677 passed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/full_20260729T051742Z/test_runtime_summary.json`，trigger=
  `natural_integration_boundary`，boundary=
  `DATA-GOV-001-D0E-NATURAL-INTEGRATION-20260729`。验收标准全部满足，任务转`DONE`；
  后续consumer必须另立exact migration任务，不能把本bundle扩成generic cutover。
  Active→completed shadow归档和final compatibility固化后，post-Full
  Architecture/Contract=`784/276 passed`。
- 2026-07-29：closeout 生命周期复核确认两个失败 candidate
  `outputs/validation_runtime/data_foundation_d0e_20260729T045000Z/`
  （61 files / 49,620,404 bytes）与
  `outputs/validation_runtime/data_foundation_d0e_20260729T045500Z/`
  （68 files / 58,558,389 bytes）仍无 unique canonical evidence，且没有 active process
  依赖；canonical PASS evidence 继续保留在
  `outputs/validation_runtime/data_foundation_d0e_20260729T045400Z/`
  （36 files / 24,779,756 bytes）。按两个 exact absolute paths、PowerShell 单 shell 和
  validated repository-output containment 再次申请递归删除，仍被 host command policy
  拒绝；未改用其他 shell、脚本或绕过。两个失败目录继续保留且不可作为 PASS evidence，
  next owner 为本地 operator；exit condition 为 host policy 允许上述 exact allowlist
  删除，或 operator 在确认 canonical PASS 目录完整后手动删除。删除成功前可从本机恢复
  这些失败副本；本任务的受控迁移结论与 tracked closeout 不依赖它们。
