# TRADING-2490：QuantConnect QQQ Options Local Ingest Validator and Reconciliation V1

最后更新：2026-08-03

稳定任务 ID：
`TRADING-2490_QC_QQQ_OPTIONS_LOCAL_INGEST_VALIDATOR_RECONCILIATION_V1`

优先级：`P1`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

外部平台 Owner token：`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`

## 1. 目标与非目标

本任务建立 strictly offline、deterministic、content-bound 的 QuantConnect evidence 本地 ingest、独立复算与
reconciliation 合同。它只接受 TRADING-2489 strict loader 从实际文件事实重建并判定为
`MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION` 的 export-safe bundle，并与 2483 signal/run、2486
execution、2487 accounting、2488 lifecycle canonical outputs 逐层 cross-bind。

本任务实现：

- 从 2489 package root、capability receipt 与 policy paths 重新运行既有 strict loader，不信任 caller 自报 ready；
- 对 Results JSON、Orders CSV、Trades CSV 使用 policy-bound ingest profile，形成 task-owned normalized facts；
- 从 predecessor canonical bytes 重建 local run/order/fill/cash/lifecycle facts；
- exact 比较 identity、symbol/SID、side、quantity、state、range、lineage 与 source checksum；
- 仅在 Owner-reviewed tolerance/rounding/mapping policy 下比较 price、fee、cash、PnL、metrics 与 timing；
- 输出 2481 `ReconciliationReportRecord` 与 task-owned typed exact/difference/result records；
- 将所有差异闭合为固定 taxonomy、owner、evidence、impact 与 disposition，并保持 canonical replay。

本任务不：

- 登录 QuantConnect、创建/修改 project、调用 API/CLI/HTTP/Object Store 或运行 cloud backtest；
- 读取、下载、复制、重建或导出 raw option chain、minute quote、OpenInterest、Greeks 或 provider raw rows；
- 从日志、报告、截图或 project archive 猜测缺失订单、fill、quote、engine、license 或 DQ/PIT 事实；
- 把 synthetic fixture、外部 PASS、caller declaration 或 tracked default 冒充真实 reconciliation；
- 激活 2485 selection、2486 execution、2487 accounting 或 2488 lifecycle 的 blocked policy；
- 修改或重定义 2481 shared records、2482 DQ/PIT、2483 package、2484 adapter、2485 selector、2486 execution、
  2487 accounting、2488 lifecycle 或 2489 evidence bundle authority；
- 形成策略有效、扩窗、promotion、paper/live/production 或 broker authorization 结论。

## 2. Exact inherited authority

2490 policy 必须 exact-bind 以下 authority；漂移一律 fail closed：

|Authority|Exact SHA-256|2490 用法|
|---|---|---|
|2481 shared contract schema|`c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`|只复用 `RunManifestRecord`、`OrderIntentRecord`、`OrderEventRecord`、`FillEventRecord`、`PositionLifecycleEventRecord`、`PortfolioSnapshotRecord`、`DQReportRecord`、`ReconciliationReportRecord`、shared envelope/enums/safety 的 seal/from_json/canonical authority。|
|2481 shared policy|`d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`|继承 research-only、no promotion/export/execution safety。|
|2482 DQ/PIT policy|`1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`|local cached DQ、platform option-event DQ、execution/accounting/lifecycle stage status 分轴；UNKNOWN/NOT_EVALUATED 不得产生 PASS。|
|2483 signal export policy|`cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9`|从 canonical `run_manifest.json` 重建 run/window/lineage，不信任 external 文件名或标签。|
|2484 adapter policy|`b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`|继承 QQQ/MINUTE、engine identity、result mapping 与 no raw export/no pretend engine boundary。|
|2485 selection policy|`bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30`|只核验 lineage；`selection_authorized=false` 不得被 reconciliation 激活。|
|2486 execution policy|`8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a`|只从 `QQQOptionExecutionResult.from_json_bytes` 重建 order/fill facts；same-bar/daily-close fill 与未评估 DQ 不得被外部结果洗白。|
|2487 accounting policy|`faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e`|只从 `QQQOptionCashAccountingResult.from_json_bytes` 重建 Decimal ledger、cash、fee、lot、position 与 snapshot。|
|2488 lifecycle policy|`1798b6696e0f31571f9242a4276a06530fb951d15f250a2ef6756ac547037582`|只从 `QQQOptionPositionLifecycleResult.from_json_bytes` 重建 lifecycle event/state/scope/cash preservation。|
|2489 evidence bundle policy|`16a638da88595c029acce0e7bcfcac7a847a40fe9d3d1d6289e259367cf7310d`|必须调用 2489 strict loader；只消费其已重验的 export-safe files、metadata、manifest、attestations 与 checksums。|

2490 不复制上述 sealed records。若后继需要改变 shared schema、DQ/PIT/cache/license/public contract，则停止本 lane，
先完成最小 serial contract wave。

## 3. Primary Research Window

- primary requested/evaluated start 默认且只能是 `2021-02-22`；
- `2022-12-01` 不是默认、minimum start、primary conclusion boundary 或 required comparator；
- non-primary 起点只接受 2489 metadata 中已由 policy exact 授权的 sensitivity/proxy/stress role 与 DQ caveat；
- internal manifest、2489 metadata、platform normalized facts 与 result record 的 requested/evaluated ranges 必须 exact；
- requested 与 evaluated range 必须同时写入输出，历史 retained evidence 不得替新 run 决定范围。

## 4. Required Data Quality and safety boundary

2490 不读取 cached market/macro data，所以不新跑 `aits validate-data`。这不等于 option-event 或任何 stage DQ PASS。
结果必须从 canonical predecessor facts 派生：

- 2489 bundle-validation 自身的 option-event DQ/PIT 始终保留 `NOT_EVALUATED`，不得就地改写；2490 只有在
  Results export 内存在 canonical 2481 `DQReportRecord`、并从其 schema/status/scope/range/lineage/checksums
  事实重验后，才可在独立 reconciliation result 轴派生 option-event DQ/PIT；缺失或伪造仍为
  `NOT_EVALUATED`/FAIL；
- 2486 execution、2487 accounting、2488 lifecycle 的 FAIL/NOT_EVALUATED、blocked/unauthorized/invalid 状态必须进入
  result blockers；
- external bundle 或 platform status 的 PASS/READY 不能覆盖 internal FAIL、NOT_EVALUATED、cash preservation、scope
  violation 或 invalid run；
- default unauthorized 与 incomplete manual bundle 都输出 typed no-promotion/no-execution result，不创建 order/fill；
- safety 固定 research-only、external action/cloud/API/CLI/HTTP/Object Store/raw export/paper/live/production/broker false/none。

## 5. Policy、mapping 与 threshold governance

新增 tracked policy：
`config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml`。

tracked default 固定：

```text
status=OWNER_REVIEW_REQUIRED_BASELINE
reconciliation_authorized=false
owner_authorization_status=NOT_GRANTED_FOR_RECONCILIATION_POLICY
ingest_profile_status=UNKNOWN_REQUIRES_PLATFORM_EVIDENCE
tolerance_policy_status=UNKNOWN_REQUIRES_POLICY_REVIEW
identity/symbol/SID/side/quantity/state/range/lineage/source_checksum comparison=EXACT
decision=LOCAL_QC_RECONCILIATION_V1_READY_POLICY_BLOCKED
```

Results JSON pointer、Orders/Trades CSV source columns、timestamp parsing、platform status mapping、currency/price/fee/cash
rounding、absolute/relative tolerances 与 metric definitions 都会影响解释，必须位于 reviewed active policy，包含 owner、
version/status、rationale、validation evidence、review/expiry condition。tracked default 不填猜测值。

测试可在临时目录构造显式 `OWNER_REVIEWED_ACTIVE` synthetic policy 和 synthetic 2489 bundle，证明 generic strict ingest
与 reconciliation mechanics；fixture 不代表真实 QuantConnect export schema、真实 Owner 授权或投资证据。

## 6. Typed contract and normalized facts

task-owned public contract 至少包括：

- `QCReconciliationDifferenceClass`，精确七值：`LOGIC`、`PLATFORM`、`PROVIDER`、`TIMING`、
  `REALITY_MODEL`、`LICENSE`、`MANUAL_COLLECTION`；
- `QCReconciliationDisposition`：`ACCEPTED_EXPLAINED`、`REQUIRES_FIX`、`BLOCKED_EVIDENCE`、`INVALID_RUN`；
- unresolved/active criteria、policy/safety/load-result models；
- sealed normalized platform run/order/fill/cash/lifecycle/metric facts；
- sealed internal artifact request，包含 run manifest、execution result tuple、accounting result、lifecycle result canonical bytes；
- sealed exact identity check 与 task-owned difference detail；numeric reconciliation 必须复用 2481
  `ReconciliationReportRecord`；
- sealed aggregate result/receipt，包含 input identity、bundle hashes、predecessor hashes、ranges、DQ/PIT/stage states、
  exact/numeric checks、difference taxonomy、overall disposition、blockers 与 safety；
- typed `QCLocalReconciliationContractError`，包含稳定 error code；
- policy loader、input identity builder、strict bundle ingest/reconciliation 与 canonical replay API。

所有 task-owned sealed records 提供 `seal`、`canonical_bytes`、`canonical_sha256`/`content_sha256`、
`from_json_bytes`，拒绝 caller-supplied hash、noncanonical JSON、extra fields、NaN/Inf、naive/future time、duplicate/
unsorted tuple 和 input permutation drift。

## 7. Strict ingest and reconciliation sequence

主 API 必须按顺序：

1. 加载 tracked/explicit 2490 policy，重验 exact predecessor hashes；
2. 未授权或 mapping/tolerance unresolved 时返回 typed policy-blocked result，不读取/猜测平台业务字段；
3. active 情况必须调用 2489 `load_qc_qqq_options_manual_evidence_bundle`，不得接受 caller 构造的 Loaded object；
4. 验证 2489 ready disposition、policy/file/content hashes、bundle/run/range/engine/license/safety；
5. 从 canonical bytes strict replay 2483 manifest、2486 execution、2487 accounting、2488 lifecycle；
6. cross-bind repository/policy/contract/adapter/selector/execution/accounting/lifecycle SHA、run/range/lineage/source；
7. 重算 2489 Results/Orders/Trades 文件 hashes，再按 reviewed mapping profile 解析，不读取 raw option rows；
8. 将 external rows normalize、sort、deduplicate；重复 identity、missing field、extra semantic identity、unknown status、
   nonfinite/noncanonical Decimal、timestamp ambiguity 一律 fail closed；
9. exact compare categorical/identity facts；按 reviewed criteria 比较 numeric/timing/metrics，并生成 shared numeric reports；
10. 从实际 differences 派生七类 taxonomy 与 disposition，不信任 caller 自报 PASS/explanation；
11. external PASS 遇 internal FAIL/NOT_EVALUATED 时整体不得 PASS；
12. 输出 sealed deterministic result，同输入不同排列必须 byte-identical。

## 8. Difference taxonomy and outcome

每项 difference 必须包含 `check_id`、layer、class、local/platform evidence hash、owner、impact、explanation 与
disposition。class 只能是：

- `LOGIC`：本地 deterministic logic/contract 复算与 export facts 不一致；
- `PLATFORM`：engine/platform behavior 或 artifact semantics 不一致；
- `PROVIDER`：provider identity/content 可验证差异；不能用此类猜测受限 raw rows；
- `TIMING`：chronology/timezone/latency/session mapping 差异；
- `REALITY_MODEL`：fill/slippage/fee/partial/cancel/reject 等 reviewed model 差异；
- `LICENSE`：许可/导出/usage authority 缺失或矛盾；
- `MANUAL_COLLECTION`：mandatory manual evidence、attestation、checksum 或 mapping 缺失/错误。

overall outcome：

- `LOCAL_RECONCILIATION_POLICY_BLOCKED`：tracked default 或任一 mapping/tolerance 未 reviewed；
- `LOCAL_RECONCILIATION_INPUT_INVALID`：canonical、lineage、range、checksum、schema、security 或 internal safety 失败；
- `LOCAL_RECONCILIATION_INCOMPLETE`：2489 bundle/evidence 必需事实缺失；
- `LOCAL_RECONCILIATION_REQUIRES_FIX`：存在 `REQUIRES_FIX` 或 `INVALID_RUN` 差异；
- `LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW`：全部 exact/numeric checks PASS 或仅有 reviewed
  `ACCEPTED_EXPLAINED`，且 internal DQ/PIT/stage 状态不含 FAIL/NOT_EVALUATED；它仍不是策略有效或扩窗授权。

UNKNOWN 永不产生 ready。完整工程出口在真实 Owner-reviewed policy + 2492 evidence 后才允许
`LOCAL_QC_RECONCILIATION_V1_READY`；本任务默认工程出口为
`LOCAL_QC_RECONCILIATION_V1_READY_POLICY_BLOCKED`。

## 9. Verification matrix

focused unit/property/golden 至少覆盖：

1. tracked default exact、policy-blocked、no external action；
2. exact predecessor hashes 与 shared `ReconciliationReportRecord` reuse；
3. caller forged ready/Loaded object 不可绕过 2489 strict loader；
4. 2489 incomplete/invalid/not-ready、file tamper、manifest/attestation/hash mismatch；
5. canonical internal manifest/execution/accounting/lifecycle replay 与 tamper/extra-field rejection；
6. run/range/repository/lineage/policy/contract/adapter/selector/execution/accounting/lifecycle mismatch；
7. exact identity/symbol/SID/side/quantity/state/source checksum match/mismatch；
8. Results/Orders/Trades mapping missing/duplicate/unknown status/bad Decimal/bad timestamp；
9. all seven difference classes and four dispositions；
10. external PASS cannot override internal DQ/PIT FAIL/NOT_EVALUATED, blocked execution/accounting/lifecycle or invalid run；
11. unresolved tolerance/rounding/mapping cannot yield PASS；zero tolerance/rounding is not silently assumed；
12. reviewed synthetic tolerance boundaries、Decimal exact arithmetic、delta identity；
13. order/fill chronology、partial/reject/cancel、cash/fee/position/lifecycle/metric reconstruction；
14. primary start `2021-02-22`、unreviewed non-primary FAIL、`2022-12-01` not default；
15. permutation determinism、canonical replay、stable golden hashes；
16. no raw chain/quote/OI reconstruction and no QuantConnect/cloud/API/CLI/HTTP/Object Store/paper/live/broker/production；
17. adjacent 2480–2490、compatibility/deprecation、DevEx/task shadow 与 final-tree five-tier validation。

## 10. Governed stages and path claims

frozen base：`7866052bca1dcc63154500bc5803c3086c729e30`

branch：`codex/trading-2490-qc-local-reconciliation`

`contract_change=true`：新增 task-owned public reconciliation policy/models/API，不修改 shared schema/policy。

|阶段|工作|退出条件|
|---|---|---|
|S0|task row、requirement、START/LANE|exact base、contract-change、lease/path claims PASS。|
|S1|policy、descriptor、blocked default|predecessor hashes、window、mapping/tolerance governance PASS。|
|S2|strict ingest and normalized facts|2489 loader、Results/Orders/Trades mapping、canonical replay PASS。|
|S3|internal reconstruction and checks|run/order/fill/cash/lifecycle exact/numeric/property/golden PASS。|
|S4|taxonomy/result/safety|seven classes、dispositions、external-PASS/internal-FAIL negatives PASS。|
|S5|shared wiring and closeout|system flow、generated/current authority、adjacent/compat/formal、ordinary push/cleanup PASS。|

task-owned paths：

```text
config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml
src/ai_trading_system/qqq_options_research/local_reconciliation.py
tests/test_qc_qqq_options_local_reconciliation.py
docs/requirements/TRADING-2490_QC_QQQ_Options_Local_Ingest_Validator_Reconciliation_V1.md
config/architecture/fragments/flows/qc_qqq_options_local_ingest_reconciliation.yaml
config/architecture/fragments/modules/qc_qqq_options_local_ingest_reconciliation.yaml
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

## 11. Status record

- 2026-08-03：TRADING-2489 ordinary push/cleanup 后，从 exact latest main
  `7866052bca1dcc63154500bc5803c3086c729e30` 开始 2490。首写仅更新 task row 与本 requirement；
  START/LANE PASS 前不写 policy/module/tests/fragments/shared generated paths。外部 QuantConnect 动作=none。
- 2026-08-03：START/LANE preflight 均 PASS，mode=`SINGLE_LANE`、`contract_change=true`，无 blocker、lease、
  undeclared path 或外部动作。完成 tracked policy、task-owned sealed public models/API、2489 strict reload、
  2483/2486/2487/2488 canonical replay、Results/Orders/Trades normalized ingest、exact/numeric checks、七类 difference、
  四类 disposition、五类 outcome、system flow 与 architecture fragments。
- focused failure-fix：首轮 `13 passed / 4 failed in 3.96s`，根因分别为 task-owned replay 使用 Python strict
  validation 而非 JSON strict mode，以及一个 fixture 经 `model_dump` 丢失 nested record type；修复为
  `model_validate_json` canonical replay、保留 typed fixture，并在 internal FAIL 时提前产生 typed invalid result。
  扩展第二轮 `20 passed / 1 failed in 4.04s`，唯一失败为测试选中 earlier PASS order state；收窄至 terminal
  changed FAIL 后最终相同 `-n 16 --dist loadfile` focused 覆盖 `22 passed in 4.06s`。Ruff、compileall、task-owned
  mypy 均 PASS；2480–2490 adjacent `281 passed in 9.54s`。
- compatibility/deprecation failure-fix：一次外层 1 秒 timeout 留下自然退出、无 terminal artifact 的非证据尝试；
  首个完整覆盖 `166 passed / 23 failed in 126.17s`，全部为 2490 append 后 historical EOF/successor authority
  尚停在 2489；只提升 current-authority，不改历史 payload/prefix/hash。第二轮
  `186 passed / 3 failed in 114.89s`，剩余为 2495/2496/2489 mismatch 双向差集尚未扣除 2490 successor shadow；
  精确接管后第三轮相同 `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py
  tests/test_arch_004g_deprecation.py` 为 `189 passed in 112.54s`。首两轮仅作 focused failure-fix evidence，
  不作正式 gate evidence。
- pre-formal authority：ARCH-004G frozen inventory=`1079 modules / 1246 tests / 856 writers`、
  `inventory_id=arch_004g_deprecation_inventory_a199439fca82eeef0505`；DevEx=`1079/1246/856/0`；task shadow
  `961 total / 456 active / 505 completed` 且 v1/v2 byte-identical。2489 historical compatibility blob exact
  prefix=`2881870 bytes / SHA-256 44fcdba26ae22ca5682f0ca6a2b3b7a63f35d660b84932744be18fb0cc191524`。
  task status 现写回 `BASELINE_DONE`；正式 final-tree five-tier 尚未启动，必须在最终 generated/current hash replay
  再 PASS 后串行运行，Full 保持独占。
- 2026-08-03：TRADING-2480 read-only evidence serial contract wave 仅刷新 2489 evidence policy 的 inherited
  exact hash；本 policy LF SHA-256 变为
  `7b813e5288b3de9d792c9d958f2e0b0cac6252dc92aa50ed9539248ce03bbe5d`。mapping、tolerance、
  reconciliation authorization 与 blocked disposition 语义均未改变。
