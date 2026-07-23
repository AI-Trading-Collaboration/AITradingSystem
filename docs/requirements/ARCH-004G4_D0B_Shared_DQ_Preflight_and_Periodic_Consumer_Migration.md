# ARCH-004G4 + DATA-GOV D0B 共享 DQ Preflight 与周期 Consumer 迁移

最后更新：2026-07-23

## 任务信息

- task ids：`ARCH-004G4_OPERATIONS_PERIODIC_CONSUMER_MIGRATION`、
  `DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- parent：`ARCH-004G_DOMAIN_MIGRATION_AND_SUBTRACTION`、`DATA-GOV-001`
- priority：`P0`
- status：`COMPLETE_WAVE12_S2`（G4 overall=`VALIDATING`；DATA-GOV overall=`IN_PROGRESS`）
- owners：architecture coordinator / operations platform worker / data platform worker
- source phase：`G2_5_COMPLETE_G4_D0B_NEXT`
- source base：`12b1fb86369f146c9ef1c7ac54872eb8150ed791`
- production effect：`none`
- broker action：`none`

## 为什么先做这一批

G2.5 已证明项目具备双 domain worker 的并行控制能力，但其历史 rehearsal 基于
`6ee5903a...`，候选批次是 G4 + G3，且 `dispatch_allowed=false`。当前权威路线已经变为 G4 +
D0B，因此旧 artifact 只能作为能力证据，不能作为本批次的执行 manifest。

G4 与 D0B 存在一个必须先由 coordinator 冻结的共享缝隙：现有 periodic due gate 只验证
`CanonicalStatus.PASS + 非空 data_quality_evidence_id`，没有重验 DQ policy、canonical validator、
输入文件、download manifest 与报告 bytes。若先迁 periodic consumer，任意字符串都可能冒充已验证
DQ evidence；若先让 data worker 修改 operations contract，又会破坏并行所有权。因此本批采用：

1. coordinator 串行冻结 typed DQ execution receipt 与 preflight；
2. data worker 和 operations worker 只读共享 contract，在不相交路径并行实现；
3. coordinator 最后串行接入 CLI、scheduler、runbook、system flow 与中央 manifests；
4. 所有真实 consumer cutover、automatic non-daily dispatch、production 与 broker 行为继续关闭。

## 历史证据与兼容边界

- Wave11 compatibility baseline 与 bootstrap handoff 中已捕获的 `6ee5903a...`、历史 hashes 和
  handoff 事实保持不可变；当前 tracked `arch_004g2_5_parallel_readiness.json` 允许由 canonical
  generator 在 descendant HEAD 上重新绑定 `source_base_commit` 并确定性刷新源码 hashes，但它仍只
  是 capability rehearsal，不得被改写成本批执行授权。
- 不改写 DATA-GOV D0A 的 `data_snapshot_quality_report.v1`、immutable publication manifest，
  也不把其中 `dq_execution_provenance_verified=false`、`consumer_cutover_allowed=false` 翻转为 true。
- D0B 生成新的 companion receipt/attestation；它引用并验证历史或当前 bytes，但不重写旧证据。
- `DataQualityEvidence.v1` 保持兼容。D0B 专属的 policy、validator、invocation、input 与 report
  provenance 进入新的 `data_quality_execution_receipt.v1`。
- 默认研究与数据一致性窗口继续从 `2021-02-22` 开始；`2022-12-01` 不作为默认边界。

## 分阶段计划

|stage|scope|输出|退出条件|
|---|---|---|---|
|W12-S0|coordinator shared contract/readiness freeze|本需求、任务事实、reviewed DQ policy metadata、shared receipt/preflight contract、当前 HEAD 的 G4+D0B manifests/readiness|schema、reason codes、ownership、base、hash、no-production safety 可机械验证|
|W12-S1-D0B1|canonical DQ execution receipt|data-side runner、immutable receipt、严格 verifier、tamper/drift tests|policy/validator/invocation/input/manifest/report/evidence 可重算；FAIL 也有 receipt；consumer cutover 仍 false|
|W12-S1-G4A|non-executing native parity harness|periodic consumer adapter、2/2/1 representative matrix、artifact/flow fragments|runner spy + fake clock 下 due/lock/retry/resume/DQ blocker parity；0真实 dispatch|
|W12-S2|shared integration|`aits validate-data` receipt wiring、operations typed preflight、runbook/system flow/catalog、generated manifests/compatibility|combined focused、architecture、contract、integration 与一次 Full PASS|
|D0B2|DQ rule completeness|market-calendar freshness、逐 ticker window coverage/internal gap、finite checks、真实 cache manifest 修复|严格 source binding PASS；不得把 mismatch 降级为 warning|
|D0B3/G4B|first consumer authorization|publication binding、per-consumer reviewed policy、首个 periodic evidence consumer|仅被授权 consumer 可消费 verified preflight；non-daily automatic dispatch 仍 false|
|G4C|cadence observation/freeze gate|真实 daily/weekly/monthly observation ledger、legacy caller ratchet|G4 转 `VALIDATING` 后异步积累；不占用开发 worker；满足后才允许 freeze/removal review|

D0B1 或 G4A 单独通过不等于 D0B 或 G4 完成。S2 通过后，如果真实 cadence 观察尚未满足，G4
保持 `VALIDATING`，开发槽位可按路线转给 G3。

## G4A 代表矩阵口径

“2 daily、2 weekly、1 monthly”在 G4A 中明确指五个不同 consumer identity，而不是等待五个真实
日历周期：

- daily：`daily_validate_data`、`daily_score_daily`；
- weekly：`weekly_backtest`、`weekly_research_governance_summary_review`；
- monthly：`monthly_data_source_coverage_review`。

G4A 用确定性 calendar/fake clock 验证 trading day、closed market、due、not-due、condition gate，
并用 fake runner 验证 command、`WorkflowSpec`、`RunLedger`、artifact refs、lock、duplicate trigger、retry
exhausted、safe/unsafe resume。DQ missing、FAIL、tamper、as-of drift 或 policy/validator/input/report drift 时，
必须在 runner 调用和下游 artifact 写出之前阻断：`runner_calls=0`、`downstream_artifacts=0`。

真实 cadence 观察是 G4C 的 phase-level evidence，不得把 G4A fixture parity 冒充为真实运行。

## Shared DQ execution contract

### 输入

`data_quality_execution_receipt.v1` 至少绑定：

1. identity：`receipt_id`、`run_id`、`contract_id`；
2. chronology：`started_at`、`ended_at`、`checked_at`、`as_of`、requested/evaluated window；
3. policy：`policy_id`、`policy_version`、`role=data_quality`、`status=REVIEWED`、owner、
   normalized repo-relative POSIX path、raw SHA-256；
4. validator：stable id/version、Python entrypoint、至少一个有序 implementation source path/SHA-256；
5. exact invocation：全部会影响验证结果的参数，以稳定排序的 canonical JSON 表示；
6. ordered inputs：role、path、SHA-256、size、row count、schema/source role、manifest path/SHA、
   matched source ids/record refs；
7. report：path、SHA-256、size、raw status、error/warning/info counts、issue codes 与独立
   blocking issue codes；
8. 现有 `DataQualityEvidence.v1` 的机械投影；
9. safety：`dq_execution_provenance_verified=true`、`consumer_cutover_allowed=false`、
   `production_effect=none`。

receipt 使用 strict canonical JSON 与完整 64-hex SHA-256 生成 content-derived `receipt_id`，写入
immutable/content-addressed 路径。parser 拒绝 non-finite number、duplicate/unknown key、错误字段类型、
非 UTC 时间、非规范路径与非 canonical bytes；不得用 `str()` 隐式修正非法 payload。
`latest` 只用于发现，不能作为证明。verifier 必须从磁盘重新读取并校验 receipt、policy、validator、
input、manifest 与 report bytes，不能接受 caller-supplied PASS。

### 输出

严格 verifier 只在所有绑定重算通过后通过 module-private capability factory 返回
`VerifiedDataQualityPreflight`。该 value 不提供公开构造或反序列化入口，并绑定实际 receipt path、bytes
SHA、size 与 verified-at；G4 adapter 接 receipt path/expected context 后调用 verifier，不能消费 caller
构造的 typed object、裸 status 或裸 evidence id。

D0B1 对 `PASS_WITH_WARNINGS` 固定 fail-closed。是否允许 warning 只能由 D0B3 新增的 reviewed typed
per-consumer profile 决定；不得用裸 boolean、静默 PASS 映射或 G4 本地配置绕过，也不授权任何真实
consumer。

## W12-S2 chronology 与 discovery 冻结

G4A、D0B1 focused 与交叉审计通过后，S2 冻结以下集成语义：

1. 无显式 `--as-of` 的 `aits validate-data` 与 daily-run 共用 America/New_York 最近已完成交易日和
   3 小时 provider-ready buffer；不得继续使用主机本地时区的 `date.today()`。
2. `operations_as_of` 是 scheduler/due/calendar 的触发日期；`data_quality_as_of` 是 validate-data
   实际命令中的 `download_end`。休市日两者不同，receipt verifier 只对后者做 exact match。
3. `checked_at`、`started_at`、`ended_at` 均保留真实 UTC 时间；runner 必须验证
   `started_at <= checked_at <= ended_at`，不得用 `max()` 或时区改写抬平未来时间。
4. canonical runner 必须先捕获prices/rates、required secondary、manifest、policy与validator source的
   exact bytes，再从该snapshot解析、校验、计数和计算SHA；不得让validator检查A而receipt绑定并发替换后的B。
   `evaluated_window`固定为requested range与required date-bearing inputs共同覆盖的真实交集；无交集时
   contract v1以`DQ_WINDOW_MISMATCH`停止，不伪造范围。
5. `daily-run`必须显式传`--execution-profile daily_default.v1`；direct CLI显式`--as-of`时`auto`解析为
   `manual.v1`。profile不是caller自报字符串：runner、strict verifier与publisher必须独立证明默认
   prices/rates/Marketstack/manifest路径、required secondary、无custom backtest manifest，并把受contained
   read保护的`config/universe.yaml` exact bytes/SHA绑定进invocation，复算core（非full AI chain）tickers与
   rate series。只有上述typed profile证明全部通过时，才能发布discovery pointer。pointer固定为
   `outputs/data_quality/executions/discovery/daily_default/{data_quality_as_of}/current.json`，schema 为
   `data_quality_execution_discovery_pointer.v1`。pointer 只含 profile/date/published-at 与 receipt
   id/path/SHA/size，不含可信 PASS 或 evidence id；custom/manual profile 不覆盖该 pointer。
6. pointer 使用 canonical JSON 原子发布，report/receipt/pointer必须复用D0A root-bound directory chain与
   descriptor-attested atomic writer/read，拒绝parent symlink/junction、file symlink与check/write TOCTOU逃逸；
   pointer发布并回读成功后才能更新validation audit latest。consumer必须按固定profile/date
   找到候选，校验 pointer
   canonical bytes、exact content-addressed receipt path/SHA/size，再调用公开 verifier，并复核 preflight
   与当前 daily validate step 的真实 start/end 边界；禁止 glob、mtime 或裸 latest-as-proof。
7. daily-run 额外写 `native_periodic_consumer_parity_plan.v1` sidecar；现有
   `periodic_operations_plan.v1` 继续作为兼容 preview，只有从 typed preflight 机械投影的 evidence id 与
   receipt refs 才可写入。两者均不 dispatch、不 cutover、不产生 production/broker effect。

### 稳定 reason codes

- receipt：`DQ_RECEIPT_MISSING`、`DQ_RECEIPT_SCHEMA_UNSUPPORTED`、
  `DQ_RECEIPT_FIELDS_INVALID`、`DQ_RECEIPT_ID_MISMATCH`；
- policy：`DQ_POLICY_MISSING`、`DQ_POLICY_NOT_REVIEWED`、`DQ_POLICY_ID_MISMATCH`、
  `DQ_POLICY_VERSION_MISMATCH`、`DQ_POLICY_PATH_MISMATCH`、`DQ_POLICY_SHA_MISMATCH`；
- validator：`DQ_VALIDATOR_ID_MISMATCH`、`DQ_VALIDATOR_ENTRYPOINT_MISMATCH`、
  `DQ_VALIDATOR_IMPLEMENTATION_MISSING`、`DQ_VALIDATOR_SHA_MISMATCH`；
- chronology/window：`DQ_AS_OF_MISMATCH`、`DQ_WINDOW_INVALID`、`DQ_WINDOW_MISMATCH`、
  `DQ_EXECUTION_CHRONOLOGY_INVALID`；
- input/source：`DQ_INPUT_SET_MISMATCH`、`DQ_INPUT_MISSING`、`DQ_INPUT_SHA_MISMATCH`、
  `DQ_INPUT_SIZE_MISMATCH`、`DQ_INPUT_ROW_COUNT_MISMATCH`、`DQ_MANIFEST_MISSING`、
  `DQ_MANIFEST_SHA_MISMATCH`、`DQ_MANIFEST_CURRENT_CHECKSUM_MISSING`、
  `DQ_SOURCE_ID_UNREVIEWED`；
- report/evidence：`DQ_REPORT_MISSING`、`DQ_REPORT_SHA_MISMATCH`、
  `DQ_REPORT_STATUS_CONFLICT`、`DQ_REPORT_COUNT_MISMATCH`、`DQ_EVIDENCE_ID_MISMATCH`、
  `DQ_EXECUTION_FAILED`、`DQ_WARNING_NOT_ALLOWED`；
- cutover/safety：`DQ_PROVENANCE_NOT_VERIFIED`、`DQ_CONSUMER_NOT_AUTHORIZED`、
  `PRODUCTION_EFFECT_INVALID`。

## 当前真实数据 blocker

只读审计发现当前 `data/raw/prices_daily.csv` 的 SHA-256 未被 `download_manifest.csv` 当前记录覆盖，
而 rates 与 Marketstack checksum 有匹配。D0B 严格 source binding 启用后必须输出
`DQ_MANIFEST_CURRENT_CHECKSUM_MISSING` 并阻断。D0B2 应直接修复 download/publish/manifest 事务并重跑
`aits validate-data`；不得通过忽略该 input、伪造 source id 或降低为 warning 绕过。

## 并行所有权与共享租约

### operations worker 独占

- `src/ai_trading_system/platform/operations/periodic_consumer_migration.py`
- `tests/test_arch_004g4_operations_consumer_migration.py`
- `config/architecture/fragments/artifacts/arch_004g4_periodic_consumers.yaml`
- `config/architecture/fragments/flows/arch_004g4_periodic_consumers.yaml`

### data worker 独占

- `src/ai_trading_system/data/quality_execution.py`
- `tests/test_data_quality_execution.py`
- data-local fixtures；D0B2/D0B3 的 publication/provider 模块只有在后续 slice 明确登记后才开放

### coordinator 单写

- 本需求、ARCH-004/DATA-GOV requirements、task register 与 task shadows；
- `src/ai_trading_system/contracts/data_quality_execution.py` 及 shared exports；
- `config/data_quality.yaml` governance metadata；
- shared market-date policy、DQ discovery pointer module 与 public exports；
- root/data/operations CLI wiring、`ops_daily.py`、`scheduled_tasks.py`、两个 legacy adapters；
- `config/scheduled_tasks.yaml`、operations policies、operations runbook；
- `docs/system_flow.md`、artifact catalog、report registry；
- architecture policy、Wave12 readiness/change manifests、generated aggregates、compatibility baseline、
  deprecation inventory 与 formal validation artifacts。

worker 不得编辑共享路径；coordinator 不在 worker 实现期间改变已冻结 contract。所有 worker 使用同一
base，最多两个 domain worker。Full/architecture/contract formal tier 和真实 19 GiB cache scan 各自只取
一个独占资源 lease；focused tests 禁止真实网络、真实 sleep 与真实 weekly backtest。

legacy import whitelist 至少覆盖 `legacy/periodic_operations_adapter.py` 和
`legacy/scheduled_tasks_adapter.py`。G4A 后这两个 adapter 不得新增 caller；freeze/removal 只能由
coordinator 在 parity 与 reachability gate 通过后执行。

## 验证与性能门禁

1. S0：shared contract focused + contract validation，不跑 Full；
2. worker：各自 `pytest -n 16 --dist loadfile` focused、Ruff、Black、scoped mypy；
3. combined focused：新 G4/D0B tests、immutable publish、F1 operations、ops daily、scheduled tasks；
4. coordinator 刷新 task shadows、module/test manifests、deprecation、compatibility/source hashes；
5. integration、reproducibility、architecture-fitness、contract-validation；
6. 仅在最终 runtime/test tree 上跑一次 Full；commit/push 后核对远端 CI。

若新测试进入 `--dist loadfile` 尾部、Full wall/P95/P99、tail idle 或内存出现异常增长，立即暂停扩容并
登记独立 performance task。不得在 correctness wave 中用降低测试覆盖、真实 DQ 重复扫描或串行 pytest
掩盖性能问题。canonical DQ execution 应一次执行、多个 consumer 验证同一 immutable receipt；cache
key 必须绑定 policy SHA、validator version/SHA、as-of 与全部 input checksums。

## Wave12 phase exit

- G4A 与 D0B1 各自 focused/typed tamper tests PASS；
- 当前 HEAD 的 change manifests/readiness、shared contract path/version/SHA、active shared lease count、
  known unrelated worktree files 可复算；
- shared integration 后 architecture/contract/integration/reproducibility 与一次 Full PASS；
- module/test manifests、compatibility baseline、deprecation inventory 和 source hashes fresh；
- 只提交/推送可归属变更，保留既有无关 research 文档；
- `automatic_command_dispatch_enabled=false`、`consumer_cutover_allowed=false`、
  `production_effect=none`、`broker_action=none`；
- 本 gate 不自动授权 G3/G5、ARCH-004H、策略 B/C、paper-shadow、promotion 或 production。

## 工作量与后续方向

- S0：0.5～1 人日；
- D0B1～D0B3：约 4～7 人日；
- G4A～G4B：约 2～3 人日；
- shared integration/formal gate：0.5～1 人日；
- 两 worker 并行后预计墙钟约 3～5 个工作日。

S2 已通过且 G4 转为 `VALIDATING`；在 Wave13 GOV-006 N1 收口后，operations worker 可释放给
G3 Reporting Native Migration；真实
cadence observation 由计划任务证据链异步积累。G3 后再推进 G5，D0C/D1 的准确并行点在届时根据
typed DQ、lineage 与 shared-path 冲突重新冻结。GOV-006 N1～N3 保持独立 coordinator 批次，不与
domain final integration 混合。

## 状态记录

- 2026-07-23：首次 formal Full=`6824 passed / 1 failed / 3 skipped / 643 warnings / 1103.75s`，
  相对上一基线 `1106.60s` 无整体性能回退。唯一失败不是运行实现或用户 research 文档，而是
  `config/data_quality.yaml` 的 reviewed governance 使已关闭 TRADING-2452 package 对 live policy
  正确产生 `SOURCE_OR_POLICY_CHECKSUM_DRIFT`，旧测试却仍要求该 package 可再次执行。禁止重生成或
  改写正式 package：其 id=`dynamic-v3-clean-trading2452_11991ac7965cfcd7aa18`、manifest SHA-256=
  `8319cd55...ff6c47f`、有效 run 与 TRADING-2453 lineage 保持冻结。当前回归已改为验证冻结 bytes/id
  不变且 active DQ policy 漂移时 fail closed，完整 TRADING-2452 focused=`32 passed`，compatibility
  authority focused=`3 passed`。带首次失败 artifact parent provenance 的 failure-fix Full 最终
  `6825 passed / 3 skipped / 643 warnings / 1147.04s`；正确性 PASS，较 `1106.60s` 基线约慢 `3.65%`，
  只登记为 `smoothed` 局部长尾复核风险，不据单次运行宣称稳定性能回退。Wave12 S2 phase exit 完成，
  G4 转 `VALIDATING`，下一主批次为 Wave13 GOV-006 N1；automatic dispatch、consumer cutover、G3/G5、
  strategy、production 与 broker 仍未自动开放。
- 2026-07-23：S2 的四项 pre-formal P1 已全部关闭并经独立只读复核确认无剩余 P0/P1：validator、
  manifest 与 report 计算复用同一组 captured bytes；`daily_default.v1` 精确绑定默认输入、
  `config/universe.yaml` 的 contained bytes/SHA 与 core ticker/rate universe，manual/as-of run 不得发布
  daily pointer；`evaluated_window` 从实际共同 finite 日期交集推导；canonical report/receipt/pointer 与
  universe capture/复验均使用 root-bound directory/leaf read-write primitive，metadata 后替换回归必须
  fail closed 且不得产生输出。combined focused=`246 passed / 1 skipped`，独立安全复核 focused=
  `162 passed`，Ruff/Black/mypy strict/diff-check 均 PASS；当前进入 generated state freshness 与 formal
  architecture/contract/reproducibility/integration/Full gate，仍不授权 dispatch、consumer cutover、G3/G5、
  strategy、production 或 broker。
- 2026-07-23：S2 独立 pre-formal review 发现三个不能带入正式门禁的 P1：validator 对 CSV
  `read/hash` 与 D0B 后置 manifest/stat/header 读取尚未证明为同一 captured bytes；显式历史
  `validate-data --as-of` 仍可能被误判为 `daily_default` 并覆盖 discovery pointer；receipt 的
  `evaluated_window` 仍可能机械等于 requested range 而大于真实共同数据覆盖。当前先补同字节
  capture、显式 execution profile 与 actual evaluated-window 推导/负例，再进入 generated state 和
  formal tiers；不得以 focused PASS 或 pointer 不含 PASS 字段降级这三个问题。
- 2026-07-23：G4A=`24 passed`、D0B1=`20 passed`，F1+G4A=`72 passed`，D0B contract/readiness
  combined=`37 passed`；Ruff/Black/mypy strict 均 PASS。交叉审计关闭 receipt producer 自依赖、
  operations/DQ date 混用和 future timestamp 抬升风险，进入 `W12_S2_SHARED_INTEGRATION`。S2 只开放
  shared date policy、profile/as-of 隔离 discovery pointer、CLI/daily typed preflight、sidecar 与文档集成；
  automatic dispatch、consumer cutover、G3/G5、production/broker 继续关闭。
- 2026-07-23：W12-S0 PASS，进入 `W12_S1_PARALLEL_G4A_D0B1`。当前 HEAD/base、G4A+D0B1
  `change_manifest.v1`、shared contract/policy hashes、两 worker ownership、factory/seal import
  whitelist、content-addressed receipt path 与零 active shared lease 已冻结；plan id=
  `lane-plan-3cd8056e3b0aaf45e3cf`。focused/readiness=`35 passed`，补充 readiness/deprecation focused
  均 PASS；首次 contract-validation=`265 passed / 1 failed / 133.7s`，唯一失败是两份新增治理引用使
  operations/scheduled deprecation reference inventory 合法增长。保留 Wave11 历史 oracle 后新增
  Wave12 current oracle、刷新 live inventory，再次 contract-validation=`266 passed / 129.57s`。
  coordinator manual worker assignment 已允许；artifact/automatic dispatch、merge、lease acquisition、
  consumer cutover、production 与 broker 仍为 false/none。
