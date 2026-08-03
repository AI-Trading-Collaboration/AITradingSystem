# TRADING-2489：QuantConnect QQQ Options Platform Evidence Manual Bundle V1

最后更新：2026-08-03

稳定任务 ID：
`TRADING-2489_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_MANUAL_BUNDLE_V1`

优先级：`P1`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

外部平台 Owner token：`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`

## 1. 目标与边界

本任务为后继本地独立 reconciliation 建立 strictly offline、content-bound、可人工收集且可程序化重验的
QuantConnect Free Cloud result evidence bundle 合同。Bundle 必须完整表达 Results、Orders、Trades、Logs、
Report、Project Files、platform screenshot、engine/tier/license status、双人 attestation、每文件 checksum 和
共享 `PlatformEvidenceManifestRecord`，并把缺失、未知、矛盾、篡改与安全违规保持为 typed fail-closed 结果。

本任务只实现合同、policy、strict loader/validator、synthetic fixtures 和 manual collection runbook。它不：

- 登录 QuantConnect、查看账户/organization、创建或修改 project；
- 调用 QuantConnect API/CLI/HTTP/Object Store 或运行 cloud backtest；
- 下载、复制、重建或导出 raw option chain、minute quote、TradeBar、OpenInterest rows；
- 收集真实 Results/Orders/Trades/Logs/Report/screenshots；
- 运行 paper/live/broker/production、创建订单或形成投资结论；
- 把 synthetic fixture、public docs、caller declaration 或 tracked default 冒充真实平台 evidence；
- 改写 2480 capability、2481 shared schema、2482 DQ/PIT、2483 signal package、2484 adapter、2485 selector、
  2486 execution、2487 accounting 或 2488 lifecycle authority。

实际平台 collection 只能由独立的
`TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1` 在明确 Owner token 下执行。本任务不能通过
“contract ready”隐式授权外部动作。

## 2. Exact inherited authority

2489 必须继承且不得重定义：

|Authority|Exact identity|2489 用法|
|---|---|---|
|2480 capability policy|`config/research/qc_qqq_options_capability_admission_v1.yaml` SHA-256=`e2a429e7a6e2537c064261546f32771d4f824449f548a905befe5a93f1a6b2cc`|只通过 `verify_qc_qqq_options_capability_admission_receipt` 从 canonical policy/evidence bytes 重建并验证 receipt；caller 自报 decision 不具 authority。|
|2481 shared contract schema|SHA-256=`c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`|只用既有 `EvidenceArtifact`、`PlatformEvidenceManifestRecord`、`CapabilityStatus`、`ExportClassification`、`QQQOptionsSafetyBoundary` 及其 `seal/from_json_bytes/canonical_bytes/content_sha256`；不复制 shared record。|
|2481 shared policy|SHA-256=`d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`|继承 research-only、no promotion/export/execution safety。|
|2482 DQ/PIT policy|SHA-256=`1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`|platform option-event DQ/PIT 与 local cached-data DQ 分轴；2489 只做 artifact evidence validation，状态保持 `NOT_EVALUATED`，不得伪造 PASS。|
|2484 adapter policy|SHA-256=`b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`|继承 QQQ/MINUTE、11 项 engine identity requirement、六项 result mapping、no cloud/no raw export/no pretend engine boundary。|

2489 policy 必须绑定以上 exact hashes。任何 predecessor policy/schema/hash 漂移必须 fail closed；本任务不得用
“兼容”名义接受另一套 hash 或 field list。

## 3. Primary Research Window

- primary requested/evaluated start 默认且只能是 `2021-02-22`；
- `2022-12-01` 不是默认、minimum start 或投资结论边界；
- 其他起点只允许由 TRADING-2492 的 reviewed sensitivity/proxy/stress role 与 DQ caveat 明确授权；
- V1 tracked policy 的 `approved_non_primary_authority_count=0`；
- bundle metadata、shared manifest、adapter descriptor 与 capability evidence 的 requested/evaluated range 必须
  cross-bind；历史 retained evidence 不能替新 collection 决定日期。

## 4. Canonical bundle layout

V1 package root 必须是 non-symlink directory，inventory 必须 exact，不允许 extra/missing file、symlink、junction、
absolute locator、`..`、case alias 或 duplicate semantic identity：

```text
bundle_metadata.json
artifact_index.json
platform_evidence_manifest.json
artifacts/logs.txt
artifacts/orders.csv
artifacts/project_files.zip
artifacts/report.pdf
artifacts/results.json
artifacts/trades.csv
artifacts/platform_ui.png
attestations/collector.json
attestations/independent_reviewer.json
```

七个 platform artifact slots 中，六项必须逐字继承 2484 result mapping：

|mapping id|platform artifact|timestamp semantics|export classification|
|---|---|---|---|
|`logs`|`Logs`|`ALGORITHM_TIMEZONE`|`EXPORT_ALLOWED_DERIVED`|
|`orders_csv`|`Orders CSV`|`UTC`|`EXPORT_ALLOWED_DERIVED`|
|`project_files`|`Project Files`|`NOT_APPLICABLE`|`EXPORT_ALLOWED_DERIVED`|
|`report_pdf`|`Report PDF`|`MIXED_DECLARED_BY_ARTIFACT`|`EXPORT_ALLOWED_DERIVED`|
|`results_json`|`Results JSON`|`UTC`|`EXPORT_ALLOWED_DERIVED`|
|`trades_csv`|`Trades CSV`|`UTC`|`EXPORT_ALLOWED_DERIVED`|

第七项 `platform_ui_screenshot` 仅证明实际 platform/tier/project/backtest UI state，不得含 account、broker、
credential、secret 或 raw data rows。六项 inherited mapping 的
`collection_authority_task_id=TRADING-2489_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_MANUAL_BUNDLE_V1` 必须 exact。

## 5. Policy 与 authorization

新增 tracked policy：
`config/research/qc_qqq_options_platform_evidence_manual_bundle_v1.yaml`。

tracked default 必须冻结：

```text
status=REVIEWED_OFFLINE_CONTRACT_BASELINE
collection_authorized=false
required_collection_authority_task_id=TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1
owner_authorization_status=NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS
required_capability_decision=CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT
data_quality_gate_required=false
option_event_dq_status=NOT_EVALUATED
option_event_pit_status=NOT_EVALUATED
decision=QC_MANUAL_EVIDENCE_BUNDLE_V1_READY
```

`aits validate-data` 不在 2489 运行，因为本任务不读取 cached market/macro data、不产生 feature、score、backtest
或 daily report。这个“不需要”不能被解释为 platform option-event DQ/PIT PASS；两轴必须继续
`NOT_EVALUATED`，后继 2490 才能从真实 evidence 派生 reconciliation/DQ 结果。

测试可在临时目录中构造显式 `OWNER_REVIEWED_ACTIVE` policy 与 fully confirmed 2480 evidence，用于证明 validator
能够接受结构正确的 synthetic package；fixture 永远不写入 tracked default、不代表真实授权、不产生外部动作。

## 6. Typed task-owned contract

task-owned public contract 至少包括：

- policy rule、safety、policy/load-result models；
- sealed canonical bundle descriptor；
- sealed `bundle_metadata`，显式包含 2484 的 11 项 engine identity required fields：
  `adapter_descriptor_sha256`、`algorithm_language`、`backtest_id`、`evaluated_end`、`evaluated_start`、
  `lean_engine_identity`、`project_id`、`repository_code_sha`、`requested_end`、`requested_start`、
  `resource_runtime_telemetry`；
- sealed artifact index，artifact payload 复用 2481 `EvidenceArtifact`；
- sealed collector 与 independent-reviewer attestations；
- sealed validation result/receipt；
- strict policy loader、descriptor builder、bundle loader/validator 与 canonical replay API；
- typed `QCManualEvidenceBundleContractError`，包含稳定 error code。

所有 task-owned sealed models 必须提供 `seal`、`canonical_bytes`、`canonical_sha256`/`content_sha256` 与
`from_json_bytes`，拒绝 caller-supplied hash、noncanonical JSON、extra field、NaN/Inf、naive/future time、unsorted
或 duplicate tuple。

## 7. Admission and cross-binding

strict validator 必须按顺序：

1. 读取并验证 tracked/explicit policy bytes 与 SHA；
2. 在读取 bundle artifact 前检查 policy collection authorization；tracked default 保持 no collection；
3. 对明确 active 的 synthetic/authorized policy，先通过 2480 canonical verifier 重建 receipt；
4. receipt 必须是 `CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT`，无 blocking reasons，全部 required item/field
   confirmed，且 policy/evidence/file/content hashes exact；
5. 枚举 exact non-symlink inventory；mandatory file 缺失输出/抛出 typed
   `MANUAL_COLLECTION_INCOMPLETE`，不得降级为 warning；
6. 重算每个 artifact SHA-256 与 byte count，并与 artifact index exact 对齐；
7. strict parse canonical metadata/index/attestations/shared manifest；
8. cross-bind bundle/run/backtest/project/code/adapter/policy/contract/window/engine/tier/license/capability receipt、
   artifact index、collector/reviewer 与 shared manifest；
9. 从实际 package facts 构造 expected `PlatformEvidenceManifestRecord` 并与 manifest bytes exact 比较；不得信任
   caller 自报 PASS/status/manifest；
10. 仅在全部规则通过时返回 sealed validation result，decision=`MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION`。

`CapabilityStatus` 语义：

- tier、engine、license 任一 `UNKNOWN` -> `MANUAL_COLLECTION_INCOMPLETE`；
- 任一 `CONTRADICTED` -> `MANUAL_COLLECTION_INVALID`；
- 只有三者 `CONFIRMED` 且相关 evidence/cross-binding 完整才可 ready；
- shared manifest 的 `tier_status`/`engine_identity_status` 必须从 metadata facts 派生，license status 保留在
  task-owned metadata/attestation/result，不能塞进未定义的 shared field。

## 8. Security and license boundary

Bundle 必须固定：

- `raw_option_rows_included=false`；
- `account_or_broker_identifiers_included=false`；
- `secrets_included=false`；
- `raw_option_chain_reconstruction_allowed=false`；
- `raw_minute_quote_export_allowed=false`；
- `raw_open_interest_export_allowed=false`；
- `object_store/api/cli/http access performed=false`；
- `production_effect=none`、`broker_action=none`。

Logs、CSV、JSON 等文本 artifact 必须经过 deterministic prohibited-content marker scan；命中 raw chain/quote/OI、
credential/secret/token、account/broker identifier marker 即 fail closed。PDF/PNG/ZIP 等 binary artifact 不做不可靠的
内容推断，必须由 collector 与 independent reviewer 对相同 file/index hashes 分别 attestation。Attestation 不能替代
checksum、receipt 或 metadata cross-binding。

Collector 与 reviewer 必须：

- portable identity 非空且不同；
- collector time <= reviewer time <= validation time；
- 绑定同一 bundle id、metadata SHA、artifact-index SHA、capability receipt SHA；
- reviewer 绑定 collector attestation content/file SHA；
- 全部 completeness/checksum/license/no-raw/no-secret/no-account/no-broker declarations 为 true；
- 任一 false、missing、tamper、same-person 或 reversed chronology -> `MANUAL_COLLECTION_INVALID`。

## 9. Decision taxonomy

工程 descriptor 的退出 decision 固定：
`QC_MANUAL_EVIDENCE_BUNDLE_V1_READY`，只表示 offline contract/runbook 可用。

package validation decision：

- `MANUAL_COLLECTION_INCOMPLETE`：mandatory file、confirmed status、authorized capability 或 required fact 缺失；
- `MANUAL_COLLECTION_INVALID`：tamper、checksum/canonical/cross-binding/license/security/two-person rule 矛盾；
- `MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION`：仅表示 bundle 可交给 2490，本身不是 DQ/PIT PASS、策略有效、
  range expansion、promotion、paper/live/production 或 broker authorization。

UNKNOWN 永不产生 ready。External PASS 永不覆盖 internal DQ/PIT FAIL/NOT_EVALUATED。

## 10. Manual collection runbook

新增 `docs/runbooks/qc_qqq_options_manual_evidence_collection_v1.md`，必须包含：

1. 前置 2492 Owner token、2480 confirmed capability receipt 与 disposable bounded project identity；
2. 在平台 UI 中逐项收集六个 inherited result artifacts 与一张脱敏 screenshot；
3. 收集前检查 export/license classification，禁止 raw rows/secret/account/broker identifiers；
4. 每次写盘立即记录 UTC timestamp、source locator、byte count、SHA-256；
5. collector 完成 exact inventory 与 attestation；
6. independent reviewer 从文件 bytes 重算全部 hashes、检查 license/security/identity/range，再签 reviewer attestation；
7. 运行 2489 strict validator；
8. 只有 ready result 才交给 2490；任何 incomplete/invalid 立即停止，不补造、不平滑、不删减 mandatory evidence；
9. bundle root 的 retention、cleanup、recoverability 与 raw/export prohibition；
10. 明确本 runbook 当前不可执行，直到 2492 token 独立授予。

## 11. Verification matrix

focused unit/property/golden 至少覆盖：

1. tracked default exact、collection unauthorized、no external action；
2. exact inherited hashes、六项 mapping 与 11 项 engine fields；
3. 每一个 mandatory file 缺失均为 `MANUAL_COLLECTION_INCOMPLETE`；
4. fully confirmed synthetic active package ready；
5. blocked/forged/tampered/noncanonical/wrong-path 2480 receipt fail closed；
6. tier/engine/license UNKNOWN 与 CONTRADICTED；
7. artifact byte/hash/count mismatch、duplicate id、wrong mapping/path/timestamp/export classification；
8. extra file、root/file symlink、path traversal、absolute/case alias locator；
9. wrong run/backtest/project/code/adapter/policy/contract/range/engine/receipt/lineage；
10. collector=reviewer、reversed time、false declaration、reviewer wrong collector hash；
11. raw chain/quote/OI、secret/token、account/broker markers；
12. caller-supplied shared manifest/status 与 actual package facts mismatch；
13. option-event DQ/PIT 保持 `NOT_EVALUATED`，external ready 不升级；
14. primary start `2021-02-22`、unreviewed non-primary FAIL、`2022-12-01` not default；
15. input enumeration/permutation determinism、canonical replay、extra field/tamper rejection、stable golden hashes；
16. no QuantConnect/cloud/API/CLI/HTTP/Object Store/raw export/paper/live/broker/production action；
17. adjacent 2480–2489、compatibility/deprecation、DevEx/task shadow 与 five-tier final-tree validation。

## 12. Governed stages 与 path claims

frozen base：`ddde87301b970d8ef82160034fe9b836f9579435`

branch：`codex/trading-2489-qc-manual-evidence-bundle`

`contract_change=true`：新增 task-owned public bundle contract，但不修改 shared schema/policy。

|阶段|工作|退出条件|
|---|---|---|
|S0|task row、requirement、START/LANE|registered、exact base、lease/path/contract preflight PASS。|
|S1|policy、descriptor、default blocked|inherited hashes/layout/safety/authorization negatives PASS。|
|S2|metadata/index/attestation/manifest strict model|canonical seal/replay、inventory/hash/two-person rules PASS。|
|S3|capability receipt 与 package admission|2480 verifier、engine/tier/license/window/lineage cross-binding PASS。|
|S4|security/license/DQ boundary|raw/secret/account markers、binary attestation、NOT_EVALUATED axes PASS。|
|S5|manual collection runbook、property/golden|incomplete/invalid/ready taxonomy 与 deterministic identity PASS。|
|S6|shared wiring 与 closeout|system flow、generated/current authority、adjacent/compat/formal、ordinary push/cleanup PASS。|

task-owned paths：

```text
config/research/qc_qqq_options_platform_evidence_manual_bundle_v1.yaml
src/ai_trading_system/qqq_options_research/platform_evidence_bundle.py
tests/test_qc_qqq_options_platform_evidence_bundle.py
docs/requirements/TRADING-2489_QC_QQQ_Options_Platform_Evidence_Manual_Bundle_V1.md
docs/runbooks/qc_qqq_options_manual_evidence_collection_v1.md
config/architecture/fragments/flows/qc_qqq_options_platform_evidence_manual_bundle.yaml
config/architecture/fragments/modules/qc_qqq_options_platform_evidence_manual_bundle.yaml
```

coordinator-owned paths：

```text
docs/task_register.md
docs/system_flow.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/test_trading2452_architecture_contract.py
```

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、copy、stage 或修改。

## 13. 状态记录

- 2026-08-03：TRADING-2496 ordinary push/cleanup RELEASE 后，从 exact latest main
  `ddde87301b970d8ef82160034fe9b836f9579435` 启动 2489；main/origin/HEAD exact，checkout guard clean，
  runner=0。`SINGLE_LANE START --contract-change` preflight PASS，无 blocker/lease/warning。本次首写仅更新
  task row 与 requirement；LANE PASS 前不写 policy/module/tests/runbook/fragments。
- 2026-08-03：LANE preflight PASS 后完成 task-owned policy/module/tests/runbook、architecture fragments 与
  system-flow wiring。tracked policy SHA-256=`16a638da88595c029acce0e7bcfcac7a847a40fe9d3d1d6289e259367cf7310d`，
  module LF SHA-256=`8d3d12db0542e30454ce4d7673fa357f4eaef7c3c43fab38e4cd8dc3be8a1565`。
  public API 固定为 `QCManualArtifactRule`、`QCApprovedNonPrimaryWindow`、
  `QCPlatformEvidenceBundleSafety`、`QCPlatformEvidenceBundlePolicy`、
  `QCPlatformEvidenceBundlePolicyLoadResult`、`QCEngineIdentityField`、
  `QCManualEvidenceArtifactIndex`、`QCManualEvidenceBundleMetadata`、
  `QCManualEvidenceAttestation`、`QCPlatformEvidenceBundleDescriptor`、
  `QCPlatformEvidenceBundleValidationRecord`、`LoadedQCPlatformEvidenceBundle`、三个 builder/loader API。
  focused 首轮 `21 passed / 1 failed in 4.81s`，唯一 failure 为测试把 policy-layer hash drift 误期望为
  package-layer code；第二轮 `21 passed / 1 failed in 4.78s` 暴露 YAML 全零 hash 被解析为整数；第三轮
  `21 passed / 1 failed in 4.68s` 已到达 intended drift path但保留诊断断言；第四轮因非上下文 patch 命中
  错误同文行而 `20 passed / 2 failed in 4.82s`。使用函数上下文精确修复测试后，相同
  `python -m pytest -n 16 --dist loadfile tests/test_qc_qqq_options_platform_evidence_bundle.py`
  最终 `22 passed in 4.69s`。Ruff、strict mypy、compileall PASS；产品实现未因这些 fixture/assertion
  failure 调整。task row 现为 `BASELINE_DONE`，真实 collection 仍由 2492 Owner authority 阻断。
- 2026-08-03：2480–2489 adjacent parallel coverage `259 passed in 9.36s`；task shadow generate/validate
  `961 total / 456 active / 505 completed` 且 legacy/v2 byte-identical；DevEx generate/validate
  `1078 modules / 1245 tests / 856 direct writers / 0 violations`。
- 2026-08-03：compatibility/deprecation 同一命令
  `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
  的 failure-fix 轨迹为：首轮 `101 passed / 86 failed in 186.78s`（85 项 successor/current-authority
  级联与 1 项 ARCH-004G frozen inventory stale）；误把 2489 section 插入历史 prefix 的第二轮为
  `94 passed / 94 failed in 168.46s`，随后完整撤回并验证历史前缀恢复到 `2,871,382` bytes、
  SHA-256=`c7ae8d494a825237dab5492d6af12040b9e016b3bb777f49b4022071a0d3ab87`；正确 EOF append 后为
  `107 passed / 81 failed in 171.97s`，暴露 inherited live-authority 集合不完整；补齐 inherited authority
  与 Python casefold 排序后，代表性 `5 passed in 16.83s`；全覆盖依次为
  `166 passed / 22 failed in 127.11s`、`179 passed / 9 failed in 122.87s`，分别定位为 2494–2496 EOF/
  successor 及 2478–2486 仍固定 2495 authority。仅提升 successor/current-authority 引用、刷新 current
  source hashes，不改历史 payload、不降低 prefix/exact-byte/hash 验证，首次全绿为
  `188 passed in 115.72s`；写回 failure-fix 记录并刷新 current hashes 后，同覆盖 final-byte 重跑为
  `188 passed in 115.61s`。上述 FAIL 均保留为 focused failure-fix 证据，不是 formal gate evidence。
- 2026-08-03：TRADING-2480 read-only evidence wave 将 capability policy 升级为 v1.1.0；本 policy 仅刷新
  exact inherited `capability_policy_sha256`，自身 LF SHA-256 变为
  `16a638da88595c029acce0e7bcfcac7a847a40fe9d3d1d6289e259367cf7310d`。manual bundle schema、
  file inventory、collection authorization、license/export 与 safety 语义均未重定义；真实 collection 仍 blocked。
