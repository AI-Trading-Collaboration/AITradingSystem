# QuantConnect QQQ Options Manual Evidence Collection V1 Runbook

## 1. 目的与当前状态

本 runbook 只定义 TRADING-2489 的离线、derived/export-safe evidence package 收集与本地验证程序。它不是
QuantConnect 登录、project 创建、cloud backtest、API/CLI、HTTP、Object Store、raw data export、paper、live、
broker 或 production 授权。

tracked policy 当前固定：

- `status=REVIEWED_OFFLINE_CONTRACT_BASELINE`；
- `collection_authorized=false`；
- `owner_authorization_token=NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`；
- required collection authority 是
  `TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1`。

因此当前只能构建/验证 synthetic fixture 或检查 package 合同。没有 2492 的明确 Owner-reviewed token 时，操作人
必须停止，不能打开 QuantConnect 账户上下文或收集真实 artifact。

## 2. 真实收集前的 fail-closed preflight

未来 2492 获批后，单一 coordinator 必须逐项确认：

1. active policy 的 `status=OWNER_REVIEWED_ACTIVE`、`collection_authorized=true`，且 token 以
   `owner_decision:TRADING-2492:` 开头；
2. 2480 capability policy、evidence 与 receipt 使用
   `verify_qc_qqq_options_capability_admission_receipt()` 重建后，decision 是
   `CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT`；
3. account tier、assigned engine/node、license/export boundary 均有可复核事实，不能由操作人自报 PASS；
4. run 的 requested/evaluated start 对 primary package 都是 `2021-02-22`；其他起点必须存在 tracked
   reviewed `SENSITIVITY` / `PROXY` / `STRESS` authority 和 DQ caveat；
5. package 只保留 derived results，不含 option chain rows、quote rows、OI rows、secret、token、account id、
   brokerage id 或 raw option export；
6. collector 与 independent reviewer 是不同人员；
7. 当前没有并发 collector 修改同一 bundle root。

任一项为 UNKNOWN 时使用 `MANUAL_COLLECTION_INCOMPLETE` 并停止；任何事实矛盾、伪造、自相矛盾或越权内容
使用 `MANUAL_COLLECTION_INVALID` 并隔离 package。

## 3. 固定 package layout

bundle root 只能包含以下 12 个文件和 `artifacts/`、`attestations/` 两个目录：

```text
<bundle_root>/
  artifact_index.json
  bundle_metadata.json
  platform_evidence_manifest.json
  artifacts/
    logs.txt
    orders.csv
    platform_ui.png
    project_files.zip
    report.pdf
    results.json
    trades.csv
  attestations/
    collector.json
    independent_reviewer.json
```

不允许 symlink、junction/reparse point、额外文件/目录、case alias、absolute locator、`..` traversal 或 ZIP 内
path traversal/symlink。每个 artifact 最大 10 MiB；空文件不是完整 evidence。

六个 result mapping 必须逐字继承 2484：

| artifact id | platform artifact | timestamp semantics | local file |
|---|---|---|---|
| `logs` | Logs | `ALGORITHM_TIMEZONE` | `artifacts/logs.txt` |
| `orders_csv` | Orders CSV | `UTC` | `artifacts/orders.csv` |
| `project_files` | Project Files | `NOT_APPLICABLE` | `artifacts/project_files.zip` |
| `report_pdf` | Report PDF | `MIXED_DECLARED_BY_ARTIFACT` | `artifacts/report.pdf` |
| `results_json` | Results JSON | `UTC` | `artifacts/results.json` |
| `trades_csv` | Trades CSV | `UTC` | `artifacts/trades.csv` |

`platform_ui` 是额外的 derived screenshot slot，必须记录 tier、project/backtest、evaluated range 与 engine
identity 的可复核 UI 上下文，但截图前必须去除账户、broker、secret 与 raw option rows。

## 4. Collector procedure（仅在 2492 授权后）

1. 记录 exact repository commit、2484 adapter descriptor SHA、project id、backtest id、Lean engine identity、
   assigned runtime telemetry、requested/evaluated range、algorithm language 与 collection UTC time。
2. 仅通过 2492 明确批准的手工 UI 路径运行/查看 bounded backtest；不得把 2489 当作 API/CLI/cloud-run
   authority。
3. 导出/保存六个 reviewed derived result artifacts，并保存一张去敏 platform UI screenshot。
4. 对 text/CSV/JSON 执行 UTF-8、shape 与 prohibited-marker 检查。人工复核 binary ZIP/PDF/PNG 不含 secret、
   account/broker id 或 raw rows。
5. 以实际 bytes 计算 SHA-256 与 byte count，按 artifact id 排序生成 sealed
   `QCManualEvidenceArtifactIndex`。
6. 生成 sealed `QCManualEvidenceBundleMetadata`；tier、engine、license 只能写 observed
   `CONFIRMED|UNKNOWN|CONTRADICTED`。UNKNOWN 不能写 PASS，CONTRADICTED 不能继续。
7. 调用 `build_qc_qqq_options_platform_evidence_manifest()` 从 metadata、artifact index、verified capability
   receipt 和 policy bytes 重建 2481 `PlatformEvidenceManifestRecord`；不得手写 PASS manifest。
8. bundle close 后由 collector 生成 sealed `QCManualEvidenceAttestation`，role=`COLLECTOR`。
9. 在 reviewer 完成前，package disposition 保持 `MANUAL_COLLECTION_INCOMPLETE`。

## 5. Independent review

reviewer 必须重新计算全部 file hash/byte count，复核 exact inventory、platform tier、engine identity、license
authority、date range、artifact shape、安全扫描与 binary 内容边界。reviewer 不能与 collector 同名，并须把 exact
collector attestation file SHA 写入 reviewer attestation。

reviewer 发现缺失或 UNKNOWN 时不得补造事实；保持 incomplete 并写清 exit condition。发现 hash drift、额外文件、
contradicted status、raw rows、secret/account/broker marker 或错误 research window 时，把 bundle 隔离为 invalid，
不得进入 2490 reconciliation。

## 6. Canonical local validation

使用 `load_qc_qqq_options_manual_evidence_bundle()`，同时提供：

- bundle root；
- capability receipt path；
- capability evidence path；
- active 2489 policy path。

loader 先重建 capability receipt，再检查 exact filesystem inventory、canonical control bytes、artifact hash/size/
shape、安全标记、metadata lineage、primary window、shared manifest equality 与双人 attestation。只有全部 confirmed
才返回 sealed `QCPlatformEvidenceBundleValidationRecord`，disposition 为
`MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION`。

重复加载同一 immutable package 必须得到 byte-identical validation record。任何 artifact byte 变化都使旧 index、
metadata、manifest 和 attestation 失效，必须从真实 bytes 全链重建，不允许只更新最末 hash。

## 7. Data-quality 与后续边界

2489 不读取 cached market/macro data，也不计算 feature、score、backtest 或 daily report，因此不运行
`aits validate-data`。这不代表 option-event DQ/PIT 已通过：metadata、shared manifest 与 validation record 都固定
`data_quality_gate_required=false`、`option_event_dq_status=NOT_EVALUATED`、
`option_event_pit_status=NOT_EVALUATED`。

ready package 只允许交给 TRADING-2490 本地 reconciliation；它不是策略有效性、Free tier 充分性、production、
promotion、paper/live 或 broker approval。package 的保留、移动与删除须遵守 task-owned temporary workspace
lifecycle：先验证 canonical destination 与 hashes、确认无 runner 依赖，再记录 exact allowlist 和 recoverability。
