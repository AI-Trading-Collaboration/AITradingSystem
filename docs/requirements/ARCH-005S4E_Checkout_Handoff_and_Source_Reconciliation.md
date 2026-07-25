# ARCH-005S4E：Checkout Handoff 与 Source Reconciliation

## 状态

- task id：`ARCH-005S4E_CHECKOUT_HANDOFF_AND_SOURCE_RECONCILIATION`
- parent：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`
- priority：`P0`
- status：`BASELINE_DONE`
- owner：architecture control-plane owner / integration coordinator
- owner decision：
  `owner_decision:ARCH-005S4E:2026-07-25:approve_checkout_handoff_reconciliation_v1`
- dependency：`ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD=BASELINE_DONE`
- production effect：`none`
- broker action：`none`

## 问题与根因

OPS-070 最初在共享 `main` checkout 中形成 23 个归属文件，随后为避免混入并发
DEVX-001/owner 修改，将这些文件按 SHA-256 复制到 clean isolated worktree 并在独立分支继续。
目标分支最终形成 7 个 reviewed commits，但原 checkout 没有执行强制 source reconciliation：

- 23 个 OPS-070 文件停留在 5 个不同历史提交的拼合状态，另有 1 个 mixed requirement；
- `task_register.md` 同时包含 OPS-070 与 DEVX-001，后者尚无 commit；
- 两行 task register 增量触发 319 个 task shadow files 与 5 个 architecture manifests；
- dirty `main` 因而无法执行 S4C fast-forward integration 或提供 clean-attribution evidence。

现有 S4D 能在业务副作用前阻断 dirty/unattributed checkout，但没有覆盖“从共享 checkout
迁出后如何证明原副本已被目标提交取代、哪些 bytes 必须保留、哪些 generated views 必须重建”
的退出事务。这是控制面 closeout 缺口，不是允许整体 reset 的理由。

## 目标

1. 受保护的 `main` branch 对 domain mutation fail closed；只有 integration coordinator
   可以在声明 shared scope 后执行受控 mutation。
2. 从 source checkout 向 isolated target checkout 迁移时生成 immutable
   `checkout_handoff.v1`，绑定 checkout identity、base/head、目标 ref、owned/generated/
   retained/known-unrelated path roles，以及 source/target exact bytes。
3. 目标提交形成后生成 `checkout_reconciliation_report.v1`，对 source residue 分类：
   - `EXACT_TARGET`；
   - `SUPERSEDED_IN_TARGET_HISTORY`；
   - `SOURCE_RESTORED_OR_STAT_ONLY`；
   - `GENERATED_INVALIDATED`；
   - `RETAIN_UNIQUE`；
   - `KNOWN_UNRELATED_NOT_READ`；
   - `MIXED_SPLIT_REQUIRED`；
   - `TARGET_LINEAGE_MISSING`；
   - `UNATTRIBUTED_DIRTY`。
4. 只有 exact target/history 分类可进入 coordinator cleanup allowlist；mixed、unique、
   missing-lineage 和 unattributed 状态必须 fail closed。
5. 本切片只生成审计事实与显式 allowlist，不执行自动 `git restore`、reset、delete、commit、
   merge、push 或 task status mutation。
6. generated views 只由 integration coordinator 在最终 canonical source 上重建；domain lane
   不得把中间生成物作为 closeout 事实。

## 数据合同

### `checkout_handoff.v1`

最少字段：

- `task_id`、`created_at`、`base_commit`、`target_ref`；
- source/target checkout identity、HEAD、branch、Git common dir；
- path role 与 canonical repository-relative path；
- tracked/untracked/deleted status；
- raw bytes SHA-256、Git normalized blob id、size；
- source/target copy equality；
- known-unrelated path name、rationale、owner ref，不读取/hash/copy其 bytes；
- `automatic_cleanup_allowed=false`、`task_source_cutover=false`、
  `production_effect=none`、`broker_action=none`；
- canonical checksum。

### `checkout_reconciliation_report.v1`

最少字段：

- handoff checksum、target exact commit 与 base ancestry；
- 每个 path 的 prepared/current/target/history evidence；
- classification、reason codes、cleanup eligibility；
- complete cleanup allowlist、retained/mixed/generated/unattributed lists；
- `decision=READY_FOR_COORDINATOR_RECONCILIATION|PASS_CLEAN|BLOCKED`；
- `automatic_cleanup_allowed=false` 与 canonical checksum。

## 实施阶段

### S0：Policy 与 characterization

- 冻结 protected-branch、path role、classification、checksum 和 fail-closed policy；
- 记录 OPS-070/DEVX-001 真实 residue 的只读 characterization；
- 不修改当前 dirty `main`。

退出：policy/requirement/task register 已登记，现状分类可重复。

### S1：Handoff producer 与 reconciliation auditor

- 实现 exact-copy handoff producer、schema/checksum validator；
- 实现 target ancestry/blob-history auditor；
- 实现 protected-main mutation guard；
- 增加 read-only CLI。

退出：copy tamper、manifest tamper、base drift、target divergence、post-handoff source mutation、
mixed snapshot、generated fanout、known-unrelated zero-read 和 protected-main 负例全部 PASS。

### S2：当前 main 的受控收口

- 在 S1 formal validation PASS 后对当前 `main` 运行 recovery audit；
- 将 DEVX-001 unique bytes 迁到独立 reviewed change；
- 仅对 exact/history-proven OPS residue形成 coordinator cleanup allowlist；
- 由最终 canonical task register 重建 generated views；
- clean main 后再执行 S4C integration gate。

退出：当前 main 无未归属或 mixed residue；所有 unique evidence 已进入 reviewed branch/commit；
OPS-070 与本任务 required validation 绑定最终 tree。任何自动清理仍为 false。

## 验收标准

- main domain mutation与非 coordinator shared mutation fail closed；
- source/target identity、base/head/ref、path、raw SHA-256和normalized blob均可复算；
- target later commits supersede prepared bytes时仍能通过 first-parent history证明 lineage；
- source在handoff后变化时输出`MIXED_SPLIT_REQUIRED`，不进入cleanup allowlist；
- generated path只输出`GENERATED_INVALIDATED`，不逐文件推断业务归属；
- retained unique path保留且不自动清理；
- known-unrelated exact path不读取、不hash、不复制；
- manifest/report tamper、missing path、wrong repo、non-ancestor target全部fail closed；
- focused、architecture、contract、integration、reproducibility与required Full parallel PASS；
- `task_source_cutover=false`、`production_effect=none`、`broker_action=none`。

## 临时工作区生命周期

- owning task：`ARCH-005S4E_CHECKOUT_HANDOFF_AND_SOURCE_RECONCILIATION`
- absolute path：`D:\Work\AITradingSystem_ops070_livefix_20260725`
- final branch：`codex/arch-005-s4e-current-main-reconciliation`
- implementation branch：`codex/arch-005-s4e-checkout-reconciliation`
- base：`ops070-closeout@925315059b88ee781e9dae7960d232714a610566`
- purpose：在已推送的 OPS-070 clean baseline 上实现 S4E，不继续写 dirty main。
- exit condition：required validation PASS、归属文件提交并普通 push；确认运行证据已进入 canonical
  location、无 unique untracked/ignored content或活动进程后，才允许按显式绝对路径和
  `git worktree remove` 清理。当前 OPS runtime checkout 在运营验收完成前继续保留。

## 状态记录

- 2026-07-25：对当前 `main@fc6313416` 生成首次真实
  `RECOVERY_AUDIT` handoff（checksum
  `f90d1e194805225eb4252c26548efce427c25af8a38ae3c9a94c9ef35859c06f`），351项全部
  归属完成：23 owned、324 generated、3 retained、1 known-unrelated，unattributed=0。
  首份report（checksum
  `0d08c7fccaa2021327f5a672c24463d01412c14d46570de5c0bb7ab336bc7394`）
  严格输出`BLOCKED`：7项`EXACT_TARGET`、15项
  `SUPERSEDED_IN_TARGET_HISTORY`、324项`GENERATED_INVALIDATED`、3项
  `RETAIN_UNIQUE`、1项`KNOWN_UNRELATED_NOT_READ`；唯一
  `TARGET_LINEAGE_MISSING`为OPS-070 requirement迁移前快照。该快照是目标当前
  requirement的严格前缀，但从未作为exact blob进入target first-parent历史，因此不把
  “文本已包含”降格为cleanup证明。22项allowlist和324项generated均暂不清理。
- 2026-07-25：DEVX-001独有`AGENTS.md`规则、supporting requirement与task-register事实已
  迁入独立reconciliation branch的reviewed change；OPS-070 task row继续采用目标分支更晚的
  canonical状态，没有用旧main行回退。首次handoff/report保存在
  `outputs/architecture/arch_005_s4e/`。
- 2026-07-25：commit `e45d77158`用Git index exact blob方式保存迁移前OPS requirement
  `e571ea3b...`与mixed task register `0cb3e25b...`，commit `913232c75`立即恢复目标
  canonical版本；最终tree未回退，但first-parent历史保留了source exact bytes。第二份
  handoff/report checksums=`c14067df6affa0d5cb973435c743aaf300e54ad3d6040c56dec5b744513ee349`/
  `57ad581a0cc070dfe19908c54289414eef7fc34c362766f95d3dfb6483d18439`，
  target=`913232c7519ca96a0041ae525b53e9b8e43dc331`，结果9项`EXACT_TARGET`、
  17项`SUPERSEDED_IN_TARGET_HISTORY`、324项`GENERATED_INVALIDATED`、1项
  `KNOWN_UNRELATED_NOT_READ`，blocking/unattributed/retained均为0，decision=
  `READY_FOR_COORDINATOR_RECONCILIATION`。
- 2026-07-25：按第二份报告的完整allowlist人工恢复343个tracked路径，并删除7个untracked
  residue；删除目标均已存在于远端
  `codex/arch-005-s4e-current-main-reconciliation`的reviewed history，可从Git恢复。
  当前`main`只剩policy精确声明的
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，本任务未读取、hash、复制或修改该文件；
  对S4D guard而言不再有unattributed/shared residue。automatic cleanup仍为false，本次是
  integration coordinator在PASS report后执行的显式人工收口，不自动merge/cutover。
- 2026-07-25：首次正式Full=`7254 passed / 4 skipped / 1 failed / 643 warnings`；
  唯一失败为既有`test_reverse_concurrency_keeps_latest_candidate_current`在Full CPU竞争下用
  `sleep(20ms)`假设newest worker必先取得锁，真实结果为older先发布、newest随后合法覆盖，最终
  pointer仍为`run-004`。生产monotonic gate未失效；直接修复为event-gated确定性ordering，
  newest提交后再同时释放三个stale contenders竞争同一lock。保留原并行失败证据，后续只允许
  先做并行focused复核，再以原runtime summary作为parent执行`failure_fix_rerun` Full；不得用
  serial PASS替代。
- 2026-07-25：并行focused复核=`86 passed / 1 skipped`；以首次Full失败摘要为parent执行
  `failure_fix_rerun`，替换Full=`7255 passed / 4 skipped / 643 warnings`，耗时
  `992.77s`。运行摘要：
  `outputs/validation_runtime/full_20260725T133339Z/test_runtime_summary.json`；首次失败摘要：
  `outputs/validation_runtime/full_20260725T131315Z/test_runtime_summary.json`。S0/S1 required
  validation全部通过，进入S2 current-main recovery audit；该状态不授权自动restore/delete。
- 2026-07-25：S4C在最终候选`a318b9e1120215ce400870aa3dfab5d36e47a2a3`
  上完成focused/architecture/contract/integration/reproducibility=`56/634/275/995/23 passed`，
  natural integration boundary Full=`7255 passed / 4 skipped / 642 warnings`，运行摘要为
  `outputs/validation_runtime/full_20260725T142638Z/test_runtime_summary.json`。预检确认
  active lease=0、候选相对旧`origin/main`为behind=0/ahead=12、known-unrelated exact path未被
  候选触达；随后仅以`git merge --ff-only`集成并普通push，最终
  `main=origin/main=candidate=a318b9e1120215ce400870aa3dfab5d36e47a2a3`。该集成不授权
  automatic cleanup、S5、task source cutover、production或broker。
- 2026-07-25：owner 同意先修复 dirty-main/迁移后残留问题。审计确认 351 个状态项中，
  319 个为 task registry 生成物、5 个为 architecture 生成物、23 个为 OPS-070 源文件、
  2 个为 DEVX-001 unique 文件、1 个为 mixed task register、1 个为 CRLF stat-only。
  当前无 Python/pytest/scheduler writer；问题由 OPS-070 隔离迁移后缺少 source reconciliation，
  再叠加 DEVX-001 生成物刷新造成。任务进入 `IN_PROGRESS`，先完成 S0/S1，不自动修改 dirty main。
