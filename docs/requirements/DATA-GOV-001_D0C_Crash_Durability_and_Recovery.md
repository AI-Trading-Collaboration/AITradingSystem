# DATA-GOV-001 D0C Crash Durability and Recovery

最后更新：2026-07-29

## 任务信息

- task id：`DATA-GOV-001_D0C_CRASH_DURABILITY_AND_RECOVERY`
- parent task：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`
- priority：`P0`
- status：`DONE`
- owner：data platform engineering coordinator
- owner decision：project owner 于 2026-07-29 授权按
  `D0C crash durability -> ACL -> per-consumer migration` 的顺序推进
- production effect：`none`
- broker action：`none`

## 目标

D0C 关闭“正常返回时逻辑原子”与“进程崩溃或断电后仍可恢复”之间的证据缺口，并为后续
ACL 和逐 consumer migration 提供可独立复核的运行基础。任务同时关闭以下已登记缺口：

1. immutable publication 的 file data 与 namespace metadata durable commit；
2. 跨进程 writer / reader / dataset lock 与进程死亡后的锁恢复；
3. crash-point rehearsal 与重启后 old-or-new、never-torn 验证；
4. retention/reference-safe GC、删除证明与被引用对象保护；
5. forward-only、manual input、关键 config 的 checksum backup / restore rehearsal；
6. publication replay input `row_count` 基于 immutable CSV bytes 重算；
7. bound-directory descriptor close 的结构化 cleanup 可观测性。

## 治理边界

- D0A 的 immutable manifest、`SnapshotPublishResult` 和历史 evidence 继续保持
  `crash_durability_verified=false`；D0C 不回写或重解释这些 bytes。
- 新能力使用独立的 `data_publication_durability_attestation.v1`。只有 exact publication、
  platform/filesystem profile、protocol version、crash matrix、GC 和 restore rehearsal
  全部绑定且 PASS，attestation 才能声明 scoped durability PASS。
- Windows profile 只接受 local NTFS + `FILE_FLAG_WRITE_THROUGH` handle-bound rename +
  `FlushFileBuffers`/post-rename attestation；POSIX profile要求 file `fsync`、atomic rename/link
  后 parent-directory `fsync`。未知 filesystem、SMB/network share、unsupported flush、
  probe failure 或证据缺失必须 fail closed。
- 硬件 controller cache、云盘或虚拟化层是否真正兑现 write-through 属于部署 profile；
  未绑定 reviewed storage guarantee 时不得把 OS protocol PASS 扩大为任意硬件的绝对承诺。
- D0C 不翻转 `store_acl_verified`，不授权新 consumer，不改变 QLD、score、strategy、
  backtest、weights、production 或 broker 行为。

参考的官方平台语义：

- Microsoft `FILE_FLAG_WRITE_THROUGH`：NTFS 会刷新由该请求产生的 metadata 变更，包括
  rename；
- Microsoft `FlushFileBuffers`：要求可写 file handle；全 volume flush 需要管理员权限，
  因此不能用普通 directory handle 冒充 volume durability；
- POSIX profile 以 file + containing directory 的显式 `fsync` 为最低协议。

## 实施阶段

### S0：合同、平台 profile 与 no-claim 边界

- 冻结 schema、checkpoint、error code、platform probe 和 evidence lineage；
- D0C attestation 与 D0A manifest 分离；
- exact filesystem/profile 不支持时 fail closed，不降级成 warning。

验收：task/register/requirement 完整；contract tests证明历史 D0A false flags 不变。

### S1：durable publication 与 replay bytes 验证

- temp file 完整写入并 file-sync；
- replace/link 通过绑定 descriptor 完成，随后执行平台规定的 namespace durability step；
- durable step 失败时不得返回成功；若 replace 已发生而 durability 无法判定，返回 typed
  `INDETERMINATE`，不得伪装 rollback；
- directory creation、immutable install、pointer history 与 current commit 都覆盖必要的
  namespace durability；
- descriptor close 失败输出结构化 phase/path/error/commit-state cleanup observation；
- replay input validator 从 immutable CSV bytes重算 logical data rows，并拒绝
  content/metadata mismatch、malformed CSV 和非 CSV replay input。

验收：focused positive/negative/race tests PASS；D0A bytes/schema保持兼容。

### S2：跨进程与 crash-point matrix

最少 checkpoint：

1. `FILE_DURABLE_BEFORE_REPLACE`
2. `REPLACED_BEFORE_NAMESPACE_DURABLE`
3. `NAMESPACE_DURABLE_BEFORE_ATTEST`
4. `ATTESTED_BEFORE_ACK`

matrix 至少覆盖：

- writer/writer serialization 与 CAS conflict；
- reader 在 pre-commit 只能看到旧的完整 generation；
- writer进程死亡后 OS lock 可由新 writer获取；
- 每个 checkpoint 的子进程强制退出后，重启 validator 只允许 old 或 new 完整 generation；
- namespace durable checkpoint 之后必须恢复 new generation；
- current、history、manifest、source、DQ report 与 payload 任一 torn/missing/tamper 都 FAIL。

验收：真实 subprocess，而非仅线程或 mock；结果输出 deterministic machine-readable
rehearsal receipt。

### S3：retention/reference-safe GC

- plan 与 apply 分离；apply 绑定 exact plan id/hash 和 store identity；
- current chain、pointer history、manifest/source/DQ/payload、外部 run/lineage reference、
  retention-until 与 legal/audit hold 都是保护根；
- apply 前重新获取 maintenance authority 并重算引用；新增或漂移引用使计划失效；
- 只删除 allowlisted managed roots中的 exact unreferenced objects；
- deletion proof 记录 path、pre-delete SHA/size/identity、reason、policy version、执行时间和
  post-delete absence；
- symlink/reparse/hardlink、unknown entry、active writer、reference ambiguity 或 retention
  未到期必须 fail closed。

验收：被引用对象永不进入 deletion set；stale plan、TOCTOU、hardlink/reparse 和 partial
delete负例 PASS。

### S4：checksum backup / restore rehearsal 与 D0C attestation

- backup source只接受 reviewed allowlist，并区分 `forward_only`、`manual_input`、
  `critical_config`；
- backup bundle为 immutable content-addressed objects + canonical manifest，绑定 relative
  path、category、SHA-256、size、source identity、captured-at和policy；
- restore只能进入空的隔离目标，禁止覆盖 live source；恢复后逐文件 checksum、manifest
  completeness和所需语义 validator均 PASS；
- 至少完成一组 canonical fixture rehearsal；真实 production/store restore仍需独立
  operations change window；
- final attestation exact绑定 S1 protocol、S2 matrix、S3 GC proof和S4 restore receipt。

验收：missing/extra/tamper/path escape/collision/non-empty destination 均 FAIL；final
attestation machine validation PASS。

## 实施顺序与依赖

`S0 -> S1 -> S2 -> S3 -> S4 -> formal validation -> closeout`

S1 对 immutable publication 的 durability semantics 属于 consumer-visible contract 边界，
必须先走最小 serial contract wave；S2-S4 只能从该 exact base 继续。ACL 任务只能在 D0C
完成或明确记录剩余平台 blocker 后启动；逐 consumer migration 又必须等待 ACL 独立验收。

## Formal validation

- focused data publication / download publication / D0C tests，默认 xdist；
- Black、Ruff、strict mypy（task-owned modules）；
- Architecture、Contract、Report、Reproducibility、Integration tiers；
- natural integration boundary 的 parent-bound Full；
- task registry freshness、system flow freshness、governed worktree audit；
- task branch commit、latest-main candidate revalidation、local main fast-forward、ordinary
  non-force push和 local/remote SHA equality。

## 当前验收证据

2026-07-29 的隔离验收 bundle：

- path：
  `outputs/validation_runtime/data_foundation_d0c_20260729T030000Z/rehearsal_bundle.json`；
- bundle id：`data_foundation_d0c_bundle_0d36f7073a10d7b1db0f94be750b0b7f`；
- bundle SHA-256：
  `e70bac12b8f962309fcf6c468931cae1392cc4520e3c1cab029495ea8ce2ed20`；
- filesystem profile：Windows local fixed NTFS，protocol
  `data_publication_durable_commit.v1`，profile PASS；硬件 controller cache 与 ACL 继续是
  明示 limitation；
- crash receipt：
  `crash_rehearsal_6c48c8c9cb74ea1c1f9213790295c55e`，四个真实子进程强制退出 checkpoint
  恢复 generation 依次为 `1 / 2 / 2 / 2`，current validator 与 lock reacquisition 均 PASS；
- GC receipt：`store_gc_receipt_98a6d59fb184a309332b9966b79d31e0`，只删除超期、
  未引用 rehearsal orphan，current/history链保持可验证；
- restore receipt：`checksum_restore_2bd33e51810e407d01b6081f4be58058`，三类 reviewed
  category fixture 在空隔离目录逐 checksum 恢复并通过 semantic validator；
- final attestation：
  `durability_attestation_e7ce6d7bfb14cc2d7e981680e908538b`，scoped durability=true；
  D0A manifest的`crash_durability_verified=false`保持不变，ACL/cutover仍为 false；
- 临时 crash/restore workspace 已在证据生成后清理；验收 publication store、DQ evidence、
  content-addressed backup store与七份hash-bound证据保留在bundle目录。
- 首次preliminary bundle
  `outputs/validation_runtime/data_foundation_d0c_20260729T024500Z/`已由上述新schema bundle
  完全取代且无canonical引用；本次尝试按精确absolute allowlist删除时被host command policy拒绝，
  因此保留为ignored、只读历史validation output。它不参与验收；next owner为本地operator，
  exit condition为在允许的cleanup surface复核新bundle仍PASS后删除该exact目录。

该 bundle 是隔离 rehearsal evidence，不是 live production store restore、ACL验收、consumer
迁移或任意 hardware/controller 的绝对断电保证。D0C implementation、formal validation 与
latest-main integration evidence 已完成并转`DONE`；ACL与逐consumer migration继续要求独立任务。

## 状态记录

- 2026-07-29：Owner授权按 D0C、ACL、逐consumer migration顺序继续。S0合同冻结并进入
  `IN_PROGRESS`；尚未修改 writer、执行GC/restore、生成durability PASS或开放consumer。
- 2026-07-29：S1-S4实现和隔离验收完成。新增file/namespace durable commit、结构化cleanup
  observation、immutable CSV replay row-count重算、reference-safe GC、checksum backup/restore
  和独立durability attestation；publication/download/D0C focused为`122 passed, 1 skipped`，
  其中download+D0C为`72 passed`。真实四checkpoint子进程crash bundle PASS；下一步只执行formal
  validation和governed closeout，ACL仍未启动。
- 2026-07-29：首轮architecture-fitness为`734 passed / 45 failed`，artifact=
  `outputs/validation_runtime/architecture-fitness_20260729T015702Z/test_runtime_summary.json`。
  根因是D0C改变的live source尚未进入append-only compatibility hash authority，且最后一次任务行
  更新发生在shadow generation之后；durability focused与rehearsal仍为PASS。按fail-closed流程先补
  D0C serial compatibility section、刷新architecture/task manifests，再以该失败artifact为修复依据
  重跑architecture门禁；不把失败降级或跳过。
- 2026-07-29：第二轮architecture-fitness为`771 passed / 9 failed`，artifact=
  `outputs/validation_runtime/architecture-fitness_20260729T021448Z/test_runtime_summary.json`。
  其中7项为D0C后继authority与新增module/test导致的deprecation inventory确定性漂移，已补齐并由
  8项focused regression全部PASS；剩余2项均为typed `CARRIER_PUSH_DRIFT`，因为冻结lane
  `HEAD=b646fc9a`而local/remote main已推进到`82f9720c`。按ARCH-005规则不得在旧base伪造PASS；
  下一步提交clean lane、生成并验证base-drift plan，在latest-main integration candidate刷新
  coordinator-generated state后重跑完整formal tiers。
- 2026-07-29：frozen lane commit=`da00215628dd20a173692d3841088345d6ea7257`；
  `outputs/architecture/data_gov_001_d0c_integration_20260729/`登记为本任务的ignored
  integration workspace，owner=`integration-coordinator`，purpose为保存
  `change_manifest.v1`与可复算的base-drift plan，exit condition为final candidate进入
  local/remote main、formal evidence进入canonical runtime目录且该workspace不再被验证依赖。
  Plan=`integration-revalidation-75feb5f0d35d44fdd1ff`独立validate PASS，结论为
  `RECONCILIATION_REQUIRED`，0 blocker、0 contract conflict、0 undeclared path；
  reviewed reconciliation严格限于`docs/task_register.md`与
  `tests/test_arch_004_refactor_policy.py`，另5处overlap为coordinator refresh。唯一latest-main
  candidate=`codex/data-gov-001-d0c-integration@82f9720c`已保留DATA-GOV-002 Phase C历史
  authority并追加D0C current authority；task registry/DevEx重建和14项兼容性/deprecation
  focused regression均PASS。该阶段随后运行D0C focused与全部formal tiers；ACL仍未启动。
- 2026-07-29：唯一natural-boundary Full在
  `outputs/validation_runtime/full_20260729T024512Z/test_runtime_summary.json`以
  `7651 passed / 3 skipped / 644 warnings` PASS；final-tree Architecture/Contract/Report/
  Reproducibility/Integration分别为`782/276/57/24/995 passed`，D0C focused为
  `122 passed / 1 skipped`。D0C scoped durability、reference-safe GC与checksum restore
  验收完整闭合，任务转`DONE`；`store_acl_verified=false`、`consumer_cutover_allowed=false`、
  QLD automatic selection、production与broker边界均不改变。下一独立任务为ACL。
