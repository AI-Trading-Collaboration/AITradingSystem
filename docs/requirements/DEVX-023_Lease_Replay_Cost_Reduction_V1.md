# DEVX-023：租约重放成本与发布耗时增长（S3c + W 先行）

最后更新：2026-10-08

稳定任务 ID：`DEVX-023_LEASE_REPLAY_COST_REDUCTION`

状态：`IN_PROGRESS`（S3c / S3d / W 已发布；2026-10-08 owner 选择先做链级并行重放 P（第 12 节），再做 DEVX-016 C3；封印 S（第 10 节）留作后备，评审前不实现）

总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`；上游证据：`docs/requirements/DEVX-022_Full_Under_Two_Hours_Program_V1.md` 第 15、17 节。

## 1. 问题与证据（摘要）

- 每条围栏命令的墙钟几乎全是租约库重放；真实发布的 `local-publish` 约等于「租约事件数 × 每事件重放次数 × 单次重放耗时」。
- 2026-10-06 第三次真实发布：`local-publish` 86 分 13 秒（M4 为 71 分钟），仍低于 100 分钟告警线，但两次发布结构相同（发布部分各 28 个事件、20 个 >100 KB 的 v3 事件、合计 11.8 MiB），
  变长的是单次重放：13.6–14.7 s（5,642 个事件）→ 15.9–17.1 s（5,730 个事件）。**每多一次发布，单次重放约 +3 s，发布约 +15 分钟，下一次预计约 105 分钟。**
- 命名 DQ 父证明内嵌整库重放，规范字节 32.4 → 42.3 MiB，同样随发布线性增长。

## 2. Owner 决定

- 2026-10-06（AskUserQuestion）：选择「S3c + W 先做」，之后观察一次发布再决定封印 S。
- 沿用的前置决定：不引入跨调用缓存、不落盘「已校验」标记（DEVX-018 O3 被否决的设计）；S3a（调用内记忆化）与 S3b（事件行表外置）已获批准并已发布。

## 3. 读码与实测结论（决定本方案的事实，2026-10-06）

1. **晚期事件的「两张大表」其实是同一张表。** 最大的晚期事件（约 0.97 MB）里，`profile_inspection.captures`（1,376 行 `{path, sha256, size_bytes}`，约 313 KB）出现在 **三个位置**：
   `execution.hook_capsule.ready.inputs.profile_inspection`（执行记录的顶层胶囊，仅在某些状态下存在）、`execution.publication_attempts[i].hook_capsule.ready.inputs.profile_inspection`、
   `execution.publication_stable_observation.profile_inspection`。同一次发布的全部晚期事件里这张表只有 **1 个不同的版本**；`read_file_custodies` 早已由 S3b 外置。其余字段都小于 7 KB。
2. **S3c 只让新事件变小。** 历史事件文件不可改写，真实库里现有约 40 个 0.5–0.97 MB 的 v3 事件、29 个 22 MB 的 v2 事件原样保留，所以 S3c 能让增长停止，**不能**降低现有的重放耗时。
3. **现有重放耗时的主要来源**（cProfile，一次真实库重放 22.4 s，含探针开销）：`parse_lease_event` 14.2 s（`json.loads` 5.7 s、规范序列化与哈希 4.4 s）；`_validate_hook_ready` 69 次共 7.3 s（每次约 106 ms）。
   S3a 的记忆化是**单槽**的（只记住最后一个值，`hook_ready_rows`、`hook_ready_digest`、`profile_captures`），而重放按租约链依次处理，多条链各带不同的表，槽在链与链之间来回失效；链内连续事件才命中，且每次调用仍有约 50 ms 未被记忆化的固定开销（对 1 万个路径做 `str(Path)`、排序、`casefold` 去重等）。
4. **一次 `record_publication_hook` 约 9 次重放**（读 `ExecutionLifecycle`）：持锁之前 4 次（`fence.replay` 只读围栏事务、不重放租约库；`_head` 1 次、`_require_original_publication` 内的围栏校验与 `super()._head` 共约 2–3 次），
   持锁之后约 5 次（`_head` 1 次、`_require_original_publication` 再来 2–3 次、`PublicationLifecycle._append` 的 `super()._head` 1 次、`ExecutionLifecycle._append` 的 `store.replay()` 1 次）。持锁段内 store 状态不会变化（所有写入都经过 OS 仲裁锁，且读后立即写入才算变化），所以这些重放中的大部分是同一个结果。
5. 一次重放的写入前后关系：只有 `_append_event` 才改变库；在同一个持锁段里，取得 head 之后到写入之前没有其他写入者。

## 4. 设计

### 4.1 S3d（S3a 类，先做）：多槽、按内容摘要的调用内记忆化

- 范围：`_validated_hook_ready_paths`、`_hook_ready_distribution_digest`、`_validate_publication_profile_binding` 的单槽记忆化改为**多槽、按内容键**：携带摘要的 `ExternalizedRows` 用其 SHA-256（加 `count`/`cwd` 等原有条件）作键；
  没有摘要的普通 list（历史内嵌事件）保持原有的「相等比较」语义，仍只保留最后一个值。键的数量以一次 replay 调用内出现的不同表为界，调用结束即丢弃（沿用 `_REPLAY_MEMO` 的生命周期）。
- 新增：`_validate_hook_ready` 的**整体**记忆化（只记成功）。键 = 对以下输入的存储形式做规范哈希：`execution["hook_capsule"]`（大表以 marker 摘要代入，所以键很小）、`execution["request"]`、`execution["request_sha256"]`、
  `execution["checkout_plan"]["plan"]["topology"]["candidate_checkout"]["root"]["identity"]`。`_validate_hook_ready` 是这些输入的纯函数（只用 `Path` 的语法运算，没有文件系统观察、没有时钟），所以相同键的第二次调用结论必然相同。
  只有当胶囊里的大表都是携带摘要的 `ExternalizedRows` 时才启用整体记忆化；内嵌大表的历史事件走原路径，语义不变。
- 不变量：不跨调用、不落盘、没有「已校验」标记；键中任何一项不同就完整校验；失败从不入记忆。
- 预期：现有重放里 `_validate_hook_ready` 的 7.3 s 大部分消失（v3 事件的 40 次校验 → 约 2 次），并且之后每次发布给重放增加的校验成本 ≈ 0。

### 4.2 S3c（本任务 owner 选择的核心）：外置 `profile_inspection.captures`

- 机制与 S3b 完全相同：行数不少于 1,024 的表在**存储形式**里替换为 `{"externalized_rows.v1": {"sha256", "row_count"}}`，规范 JSON 字节存为内容寻址 blob，先写 blob 再写事件，事件 ID 是存储形式的哈希；读取时展开为不可变、带摘要的 `ExternalizedRows`，下游校验器看到的数据不变。
- 位置从两处扩展为声明式的位置表：
  `read_file_custodies`（沿用：`hook_capsule.ready.inputs`、`publication_attempts[i].hook_capsule.ready.inputs`）；
  `captures`（新增三处：`hook_capsule.ready.inputs.profile_inspection`、`publication_attempts[i].hook_capsule.ready.inputs.profile_inspection`、`publication_stable_observation.profile_inspection`）。
  `_rewrite_rows`、`_assert_no_stray_marker`、`_qualifying_rows` 统一读这张位置表；marker 只在声明的位置合法，别处出现一律 `LEASE_EXTERNALIZED_ROWS_INVALID`。
- 兼容性（**实现时修订**）：S3c 事件使用新的 schema `execution_lease_event.v4` / `execution_lease.v4`，不再沿用 v3。原因：真实库里已有 S3b 时代写出的 v3 事件，它们的 `captures` 表是**内嵌**的，事件 ID 是对那份存储形式的哈希；若 v3 的位置集合被扩大，这些事件重算 ID 时 `captures` 会被压成 marker，哈希不符，整个重放 FAIL。所以位置集合属于存储形式的「代」：v3 只认 S3b 的两处（`read_file_custodies`），v4 认全部五处；解析时由事件声明的 schema 选择位置集合，v4 与「至少有一个 marker」严格互斥（与 S3b 对 v3 的规则一致）。历史事件（内嵌 v2、S3b 的 v3）原样可读，新事件一律是 v4；S3b 时代的旧代码读到 v4 会因 schema 不识别而 fail closed。**单一检出里新旧代码不会同时写库**，不需要双向兼容。
- 预期：晚期事件 ~0.95 MB → 约 20 KB；每次发布留给库的事件字节 11.8 MiB → 不到 1 MiB；blob 每次发布新增约 10 MB（`read_file_custodies`，内容随候选变化）与约 0.3 MB（`captures`），每个**不同**的 blob 在一次重放里只读一次（约 0.15 s），所以逐发布增长约 +0.2 s 而不是 +3 s。
- 诚实边界：S3c 不改变历史事件，现有的约 40 个大事件产生的 ~5 s 解码/哈希成本保留。

### 4.3 W：持锁段内复用重放（**实现时修订**：由显式传递改为段内隐式复用）

- 事实：`PublicationLifecycle.__init__` 复用围栏自己的 store **同一个实例**（`super().__init__(fence.guard.store)`），所以一个持锁段里 lifecycle 的重放与围栏校验的重放（`guard.replay()`）走的是同一个 `FileExecutionLeaseStore.replay()`；
  `atomic()` 的绑定（`self._atomic_context.binding`）是该实例的线程局部状态。显式传递 replay 需要改 `_head/_append/_require_original_publication` 的四个签名，且只能省掉其中约 3 次（围栏的重放传不进去）；在 store 层复用能覆盖持锁段内的全部重放，也不改任何签名。
- 设计：`store.replay()` 在**本实例、本线程持有仲裁锁**（`atomic()` 的最外层段内，嵌套的 `atomic()` 是同一段）时，用该段自己的上一次重放回答重复调用，条件是三者都未变：
  (1) 进程内**写入代数**（按 store 根路径计数，任何实例的 `_append_event` 成功写入都会加一）；(2) 事件目录**指纹**（租约目录数、事件文件数、最新 mtime、总字节；约几十毫秒）——防御纵深，覆盖绕过了计数器的写入者；(3) 仍在同一段内。
  复用只存在于该段的线程局部状态，段结束即丢弃，**不跨调用、不落盘**；段外每次调用都是完整重放，与改动前一致。其他线程（不持锁）永不复用。
- 印章在重放**之前**取：重放期间任何变化只会让下一次调用判为未命中（保守方向）。
- **校验模式**：环境变量 `AITS_LEASE_SECTION_REPLAY_VERIFY=1` 时，每一次复用都再做一次完整重放并比较 `to_dict()`，不一致抛 `LEASE_SECTION_REPLAY_STALE`。用于测试与端到端夹具：它同时能发现「某个调用者就地改了共享的 head」这类污染（lifecycle 代码都先 `_copy` 再改，校验模式是对这一习惯的验证，而不是假设）。
- 预期：一个 `record_publication_hook` 约 8 次重放（持锁前 3 次：`_head`、围栏校验、`super()._head`；持锁后 5 次：`_head`、围栏校验、`super()._head`、`PublicationLifecycle._append` 的 `super()._head`、`ExecutionLifecycle._append` 的 `store.replay()`）→ 持锁段内 5 → 1，合计 8 → 4。
- 不变量：段内第一次调用与今天完全相同；写入后下一次调用是完整重放且能看到该写入；`_arbiter()` 的非 `atomic()` 路径（`acquire`、`heartbeat`、`terminal` 等自己拿锁的方法）不启用复用（保守，范围外）。

### 4.4 不做（本任务范围外）

- 封印 S（持久化的按租约链检查点）：留待观察一次发布后再决定，需要 owner 评审信任模型。
- 隐式的进程级或跨调用缓存；改写或压缩历史事件文件；命名 DQ 父证明不再内嵌整库重放（证明合同变更，须评审，登记为后续项，见第 8 节）。

## 5. 步骤、依赖、顺序与验收

| 步骤 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| D0 | 登记任务行与本文档（本次） | — | 任务行 `IN_PROGRESS`，文档链接在行内 |
| D1 | 基线测量（只读，不改代码）：真实库重放分项（`parse_lease_event`、`_validate_hook_ready` 调用数与耗时）、`record_publication_hook` 的重放数（计数器）、一次重放的 `heads_digest` | D0 | 数字写入第 9 节 |
| D2 | S3d：多槽记忆化 + `_validate_hook_ready` 整体记忆化 | D1 | 先红后绿的单元测试：多链交替的存储里 `_validate_hook_ready` 实际执行次数 = 不同键的个数；任一键字段被改动（请求、definition、表内容、plan 根身份）都走完整校验并得到同样的失败；记忆化只在 `replay_validation_scope` 内有效；真实库重放结论与改动前**逐项相同**（status、lease_heads、active_leases、head_event_ids、event_count、issues、`heads_digest`），耗时下降 |
| D3 | S3c：位置表 + 外置 `captures` | D2（不依赖，可并行，但同一文件，串行更安全） | 先红后绿：新事件的三个位置都是 marker 且 blob 存在；展开后与原表逐值相同；marker 出现在未声明位置、缺 blob、摘要/行数不符、无读取器 → fail closed；小于 1,024 行保持内嵌；历史 v2/S3b 事件原样可读；混合库重放结论与全量内嵌版逐项相同；真实库上用 S3c 代码重放结论不变 |
| D4 | W：持锁段内复用重放 | D2 | 单元测试：段内重复调用只重放一次（计数）、段外每次都重放、写入（本实例或别的实例）后下一次是完整重放且可见该写入、目录里多出文件使复用失效、其他线程不复用、校验模式能抓出被污染的共享 head；打开校验模式跑全部相关聚焦回归与重型端到端（`*-full-profile-publish`）全部通过 |
| D5 | 集成：聚焦回归、**真实库消费者冒烟清单（第 7 节）**、`system_flow` 同步、授权重封（`checkout_*` 等历史固定源按需接管） | D2–D4 | 清单全绿；`docs/system_flow.md` 的 S3b 段落改写为 S3b/S3c/S3d/W 的现状，并重算封印与测试里钉死的 SHA |
| D6 | 候选重封链 → 正式验证 → Full → 发布（须按当时的发布授权规则确认） | D5 | 第 6 节整体验收 |

## 6. 整体验收标准（以实测为准，不以模型为准）

1. 一次真实库重放：结论与改动前逐项相同；耗时 ≤ 改动前的 70%（目标；静机三次取中位数）。
2. 一次 `record_publication_hook` 的租约库重放数 ≤ 7（计数）。
3. 下一次真实发布：给租约库留下的事件字节 < 1 MiB（现 11.8 MiB）；发布 `local-publish` 耗时 ≤ 60 分钟（目标，现 86 分钟；仅在 owner 授权发布时实测，不据此推迟其他工作）；每多一次发布单次重放的增量 < +0.5 s（以相邻两次发布后的静机重放对比）。
4. 不引入跨调用缓存、不落盘「已校验」标记；历史事件文件字节不变（校验：真实库事件文件清单与哈希在改动前后逐一相同）。
5. 命名 DQ 活体证明与遥测快照在真实库上通过（S3b 回归的教训，见第 7 节）。
6. 文档与 `system_flow` 同步。

## 7. 风险与缓解（含 S3b 回归的教训）

- **存储形式变更必须对真实库的每一个消费者做冒烟，而不只是写入与重放。** S3b 的回归就是真实库第一次出现新形式后，严格 JSON 消费方与不带读取器的解析点失败。本任务的 D5 强制清单（全部在真实库上、只读）：
  (1) `store.replay()` 的结论与摘要与改动前一致；(2) `canonical_json_bytes(replay.to_dict())` 通过且无子类泄漏；(3) 遥测快照构建 PASS；(4) 命名 DQ 活体证明（正式验证 stage 1）通过；
  (5) `parse_lease_event` 调用点静态守卫测试通过（新增位置不得被未带读取器的调用点读到）；(6) 发布前清单与 `worktree-audit` 通过；(7) 夹具里的重型端到端发布变体（`*-full-profile-publish`）通过。
- 记忆化的健全性：S3d 只记成功；键覆盖函数读取的**全部**输入；有「改动任一输入字段」的对照测试；记忆体不跨 `replay_validation_scope`。
- W 的健全性：只在本实例、本线程持锁的段内；写入代数 + 目录指纹两道作废；段结束丢弃；校验模式逐次对账；默认路径不变。
- 旧代码与新事件：S3b 代码读 S3c marker 会 fail closed；本检出内不会混用两套代码。

## 8. 开放问题与后续项

1. S3c + W + S3d 之后若发布耗时仍不够，是否做封印 S（owner 决定，按当时的实测）。
2. 命名 DQ 父证明不再内嵌整库重放（改为只绑定重放摘要与来源租约）：证明合同变更，随下一次合同波次评审。
3. W 的范围：`acquire`/`heartbeat`/`terminal` 等自己拿仲裁锁（不经 `atomic()`）的方法本次不启用复用，是否扩展按实测决定。
4. `local-publish` 的 worker 墙钟（10,800 s）与 git 子进程等待（7,200 s）随选定方案复核。

## 9. 进度

- 2026-10-06：登记并转 `IN_PROGRESS`（任务行事务 `gov-007-devx023-start-task-20261006-v1`）；本文档为第一版。D1 基线数字在 DEVX-022 第 17 节（重放 15.9–17.1 s、`_validate_hook_ready` 69 次 7.3 s、`parse_lease_event` 14.2 s）。
- 2026-10-06 晚：D1–D5 的实现与验证（候选尚未发布）。提交内容：`parallel_control_kernel.py`（位置表与 schema v4、泛化的写时复制遍历、按位置精确的游离 marker 扫描、写入代数与指纹、持锁段复用）、`workflow_coordination.py`（S3d）、
  `tests/test_devx022_lease_event_externalization.py`（该文件现含 S3b、S3b 回归、S3d、S3c、W 的测试，共 30 个）、`docs/system_flow.md` 的一段（一个块，条目数 1,491 不变）与 `devx_006d` 封印及其测试里钉死的 SHA。
  - **D2 S3d**：真实库差分重放（基线 `25d026bf6` 对新代码，同一库状态 5,750 个事件 / 860 个 head）：status、event_count、lease_heads、`heads_digest`、`head_event_ids_digest`、issues **逐项相同**；耗时 15.0–15.3 s → 13.9–14.0 s（−7.5%）。
    69 次 hook-ready 提问里 29 次未键控（历史 v2 内嵌表），其余 40 次只有 2 个键，完整校验共 31 次、2.0 s。结论：S3d 让之后每次发布的校验增量 ≈ 0；现有重放的下限仍由 29 个历史大事件的读取/解码/哈希决定（只有封印 S 能去掉）。
  - **D3 S3c**：新事件为 schema v4（见 4.2 的修订）。用**真实数据**演练：把真实库里 4 条发布租约链（201 个事件，嵌入/S3b 形式共 648.6 MiB）重编码为 v4 写入临时库，用真实校验器重放：PASS、无 issue、8.0 s，4 个 head 与源逐值相同，
    事件字节 648.6 MiB → 6.1 MiB（8 个 blob 共 38 MiB），`canonical_json_bytes(replay.to_dict())` 通过。夹具里的重型发布节点产生 18 个 v4 事件，三个位置（两处 `ready.inputs.profile_inspection.captures`、`publication_stable_observation`）与 `read_file_custodies` 都是 marker，最大事件 66.8 KB（真实 S3b 时代的晚期事件约 0.95 MB）。
  - **D4 W**：单元测试 4 组（段内只重放一次、写入后失效、本实例与其他实例的写入、目录里多出文件、其他线程不复用、校验模式抓出被污染的共享 head）；事件目录指纹在真实库上约 60 ms（一次重放约 14 s）。
  - **验证**（全部开着 `AITS_LEASE_SECTION_REPLAY_VERIFY=1`，任何一次复用与完整重放不一致都会抛错）：聚焦回归 A（租约/内核/仲裁/检查点/命名 DQ/发布围栏/集成等 17 个模块，排除 `real_full_chain`）1,453 通过、4 失败（2 个是 W 的计数测试在校验模式下的预期差异，已改为与该环境变量无关；
    2 个是 `source_preservation` 的「已提交实现」测试，工作树未提交时按设计必败）；聚焦回归 B（`test_devx015_workflow_coordination.py`）334 通过；重型端到端 1 个（`test_original_publication_cli_ff_only_and_independent_recovery[full-profile-publish]`，真实 `local-publish` 钩子链路）通过，846.9 s。**没有一次 `LEASE_SECTION_REPLAY_STALE`。**
  - 待办：`architecture_report_catalog_flow_authority` 等生成器重封（`devx_006d` 的两个依赖生成物的测试在重封前按预期失败）、两个未跑的重型变体（`native-linked`、`orig-head-equals-main`）随 Full 验证、真实库消费者冒烟清单（第 7 节）在重封后与发布后各跑一次。
  - 临时资源（生命周期）：`D:/Work/devx023-old`（基线源码导出，差分重放用，0.05 GB）、`D:/Work/devx023-t1/-t2/-t3`（上述验证的 basetemp）、`D:/Work/devx023-batchA.log`/`-batchB.log`/`-heavy1.log`；收口时按精确路径白名单清理。
- **2026-10-07：D6 候选 `d3d34872b` 通过正式验证并真实发布**（owner 预授权，普通推送 `25d026bf6..d3d34872b`；事务 `gov-007-d23-formal-20261006-v1` RELEASED/COMPLETED；无恢复、无人工干预；`main = origin/main = d3d34872ba83146639c432d58b6a2c18c07ea24a`）。这是真实库里**第一批 v4 事件**。
  - 正式验证：`named-parent-positive` 579 s（上次 505 s；基线 440–502，告警线 900）、`contract-validation` 264 s、`integration` 78 s、`reproducibility` 48 s、`architecture-fitness` 1,412 s（基线 1,308–1,408）；**Full 14,791 通过 / 4 跳过 / 0 失败，pytest 8,737.8 s（2:25:37，仍在 8,023–8,698 s 基线带的边缘，未触 9,500 s 告警线）**，runner 8,830 s，总 CPU 约 57.2 CPU 小时（上次 53.8；整机平均 71.6%）。
  - **发布耗时（本任务的目标）**：`local-publish` **54 分 8 秒**（上次 86 分 13 秒，M4 71 分钟；−37%）；`LOCAL_MAIN_FF_PRE` 3:49、`REMOTE_PUSH_PRE` 3:18、从 Full 结束到事务释放共约 65 分钟（上次约 100 分钟）。阶段：`hooks_created` +16 分、`ready_held` +22 分、`heads_switched` +30.5 分、`merge_resumed` +34 分、`merge_exit 0` +49 分。
  - **租约库增长（逐项对照验收标准，真实库实测）**：发布部分的租约事件（`scratchpad/pub_timeline.py`）：M4 / d21b / d23：v3·v4 事件均为 20 个，**>100 KB 的事件 20 / 20 / 0**，v3·v4 事件字节 **11.85 / 11.84 / 1.13 MiB**（−90%），发布事件跨度 74.0 / 89.4 / 58.1 分钟，每个钩子事件间隔中位数 129 / 162 / 107 s（均值 170 / 208 / 135 s）。
    该发布链共 53 个事件、1.72 MiB，最大事件 68.5 KB（此前 0.97 MB）；`captures` 的三个位置与 `read_file_custodies` 在 20 个事件里都是 marker（`scratchpad/post_publication_smoke.py`）。
  - **验收对照**：(1) 真实库重放结论与基线逐项相同 ✓；耗时 ≤ 改动前 70% ✗（15.1 → 13.9 s，−8%：现有重放的下限由历史 22 MB 事件决定，目标当时偏乐观）；(2) 每个钩子事件的重放次数 ≤ 7：**未直接计数**（真实发布的钩子在子进程里），以钩子事件间隔 162 → 107 s（−34%）作间接证据；
    (3) 下次发布给租约库留下的事件字节 < 1 MiB：**1.13 MiB，基本达成**（余下是 20 个约 57 KB 的事件：`git_merge`、`checkout_plan`、`hook_capsule.definition` 等小结构）；`local-publish` ≤ 60 分钟 ✓（54 分）；
    **每多一次发布单次重放的增量 < +0.5 s ✗：实测 +1.5–1.8 s**（同一代码，发布前 13.9 s / 5,750 个事件 → 发布后 15.4–15.7 s / 5,813 个事件）；(4) 无跨调用缓存、历史事件文件不变 ✓；(5) 命名 DQ 活体证明（stage 1）与遥测快照（PASS，6,688 个来源，250 s）在真实库上通过 ✓；`canonical_json_bytes(replay.to_dict())` 通过（52.2 MiB）✓；(6) `system_flow` 同步 ✓。
  - **残余增长的原因（cProfile，发布后）**：新增的 20 个 v4 事件只触发 1 次完整 hook-ready 校验（89 次提问、32 次完整校验，上次 69 / 31），但这一次完整校验针对一张约 17.9k 行的自定义清单，约 1.1–1.7 s（`_validated_hook_ready_paths` 4.5 s / 32 次累计，`parse_lease_event` 14.6 s）；
    此外每个发布的 9.65 MB blob 每次重放要读取、哈希、解码一次。S3c + S3d + W 使增长从约 +3 s 降到约 +1.6 s（−50%）并把发布从 86 降到 54 分钟，**但不能消除**：只要每次重放仍要重新校验每一个历史发布链的清单，每次发布就会多一份。只有封印 S（终态链的校验结果按事件字节摘要持久化）才能去掉这部分。
  - **旧代码对含 v4 事件的库 fail closed**：用基线代码重放现在的真实库得到 `status FAIL`（20 个 v4 事件的 schema 不识别），符合设计；含义是之后不能再用 S3b 时代的代码读写这个库（回滚会连带要求清空或迁移，不要回滚）。
  - **命名 DQ 父证明继续增长**：整库重放的规范字节 32.4 → 42.3 → **52.2 MiB**（每次发布 +约 10 MiB，来自已释放 head 的托管表），`named-parent-positive` 505 → 579 s。已登记的后续项（第 8 节第 2 条）按本数据提高优先级：证明不再内嵌整库重放。
  - 状态：S3c / S3d / W 交付并发布；**待 owner 决定封印 S**（见上；数据已齐：残余增长 +1.6 s/次发布，发布 54 分钟，下一次预计约 60 分钟）。
- 2026-10-07（第五次发布之后）：**命名 DQ 父证明的体积决定 stage 1 的耗时**（量化，详见 DEVX-022 第 17.6 节）。两个 stage 1 测试写出的真实文件：`test_parent_pre_guard.json` 35.70 → 46.60 → 57.52 MB（d21b → d23 → d24，每次发布 +约 10.9 MB），
  `activation_cli_*_parent.json` 170.7 → 224.4 → 278.2 MB；每个组件的耗时同比例增长（单次活体证明约 0.8 s / MB，整个阶段约 12 s / MB），`named-parent-positive` 505 → 579 → 751 s，近似 86 s + 11.3 s / MB。
  外推：下一次发布约 860–890 s（告警线 900 s），再下一次约 980–1,030 s（越线）；正式 Full 里同一个文件的两个测试随之增长。
  因此第 8 节第 2 条（证明只绑定重放摘要与来源租约、不再内嵌整库重放）提高优先级：它同时去掉每次发布「+约 10.9 MB × 全部活体证明」；它是证明合同变更（生产代码 `named_quality_dispatch.py`、`named_quality_execution.py`、
  `prospective_capture_execution.py` 同样定义并消费 `named_dq_existing_parent_proof.v1`，不是只改测试），须 owner 评审。
  无合同变更的缓解（仅测试层，可选）：把 `tests/test_named_data_quality_actual_candidate.py` 的两个测试拆成两个文件，使 `--dist loadfile` 把它们放到两个 worker（阶段约 751 → 约 550 s，测试 2 约 530 s 是瓶颈）。待 owner 决定。
- 2026-10-07：**owner 决定：「命名 DQ 父证明不再内嵌整库重放」（第 8 节第 2 条）排在 DEVX-016 的 C3 之前**（会话中答复；依据：stage 1 耗时随证明体积线性增长，DEVX-022 第 17.6 节外推 C3 发布时约 980–1,030 s 越过 900 s 告警线）。路线：C2（S3）发布 → 本任务的证明契约波次（先出设计，owner 评审合同后再实现）→ C3 → C4。封印 S 的决定仍待 owner，不与本波次绑定。下一步是设计文档：先读 `named_quality_dispatch.py`、`named_quality_execution.py`、`prospective_capture_execution.py` 与 `tests/named_data_quality_support.py` 里 `named_dq_existing_parent_proof.v1` 的生产者与消费者，列出每个消费者实际需要的字段，再决定证明改为绑定「重放摘要 + 来源租约头事件」时哪些校验必须保留（尤其是 PIT/来源租约关联的完整性），设计经 owner 评审后再实现。
- 2026-10-07（晚，更正）：**stage 1 的归因更正与路线调整**。C2 窗口的逐调用计时（DEVX-022 第 17.7 节）否定了上面两条里「命名 DQ 证明体积决定 stage 1 耗时」的结论：证明体积确实长到 68.40 MB（+10.9 MB），stage 1 却只有 761 s；
  耗时 ≈ 约 32 次租约库重放 × 约 21.4 s（重放约占 90%，证明构造与序列化约 3%）。因此 owner 把路线改为 **DEVX-016 C2 → 本任务的封印 S（先出设计）→ C3**；W-DQ（第 8 节第 2 条，证明只留源租约视图）降为可选的证据体积卫生（第 11 节）。
  封印 S 与既有的「不落盘已校验标记」边界（DEVX-018 O3、`FileExecutionLeaseStore._replay_uncached` 的不变量）直接冲突，所以第一步是 owner 决定是否改这条边界（第 10.7 节 0 号问题）；不改的话走第 10.8 节的替代方案（减少重放次数、按租约链并行重放，均未实测）。

## 10. 封印 S：按租约链封印终态链的校验（设计草案 2026-10-07；owner 2026-10-09 批准信任模型，实现中，见 10.9）

owner 2026-10-07 决定路线为 DEVX-016 C2 → 本节（封印 S）→ C3。依据是 C2 窗口里 stage 1 的逐调用计时（DEVX-022 第 17.7 节）：耗时 ≈ 重放次数 × 单次重放，而不是证明体积。

### 10.1 为什么做（数据）
- **stage 1 约 31 次重放**：测试进程里 14 次（每次活体证明 2 次：`fence.validate` 内 1 次 + `guard.replay()` 1 次），两个生产 CLI 子进程里约 17 次（按时间线估算）；每次约 21 s，合计约 660 s，占 761 s 的 87%。证明序列化与哈希只有 24 s（3%）。
- **围栏命令与发布几乎全是重放**：`acquire` 1 次、`checkpoint` 2 次、`release` 4 次；`local-publish` ≈ 事件数 × 每事件重放次数 × 单次重放（DEVX-022 第 15.2 节）。
- **单次重放在变长**：空闲机上 d23 15.5 s → 现在 20.4–21.0 s（5,926 个事件、876 条链、721 MB）。cProfile（含约 ×1.5 的探针开销，30.9 s）：`parse_lease_event` 19.9 s（JSON 解码 7.3 s、规范序列化与哈希 7.0 s）、`validate_execution` 14.7 s（其中 hook-ready 完整校验 33 次 9.8 s）、跨事件转移校验 3.5 s、文件读取与哈希合计约 1.9 s。
- **其中 875/876 条链是终态**（RELEASED/BLOCKED/REASSIGNED）：内容不可变，每次重放都在重复验证同一批字节；只有当前活着的事务链会变。
- 原型实测（只读，2026-10-06，`scratchpad/seal_proto.py`）：全量重放 17.35 s；逐字节哈希全部事件文件 1.13 s；封印重放 4.41 s，`LeaseReplay` 的 status / lease_heads / active_leases / head_event_ids / event_count / issues 与全量重放逐项相同；把某条终态链的一个文件摘要置为不符后，该链回退为全量校验，结论仍相同。

### 10.2 设计
1. **封印文件** `<租约库根>/replay_seal.v1.json`（不在 `events/` 里，不影响链的枚举），由显式命令写出：
   - 头部：schema、`kernel_fingerprint`（见下）、创建时间与工具版本、来源重放的 `event_count` / 链数 / `head_event_ids` 摘要；
   - 每条**终态且来源重放无 issue** 的链一项：`head_event_id`、`head_state`、有序的事件文件清单（文件名 + 内容 sha256）、`chain_sha256`；活着的链不入封印；
   - `seal_sha256`（对规范化正文）。写入只用 `write_bytes_atomic`，读者永远看到旧封印或新封印，不会看到半个。
2. **封印感知的重放**（`FileExecutionLeaseStore._replay_uncached` 里一个窄钩子，逻辑放在新模块 `lease_replay_seal.py`）：
   - 封印缺失、schema 不识别、`seal_sha256` 不符、`kernel_fingerprint` 不符 → **整体按今天的全量重放**；
   - 对每条链：先**逐字节哈希目录里的全部事件文件**（全库约 1.1 s，每次重放都做）；该链在封印里、终态、文件清单与哈希逐项相同 → 只完整解析校验它的**头事件**，其余事件复用封印的结论；否则（未封印、被改动、新增、缺文件、顺序不同）→ **该链全量校验**，与今天完全相同；
   - 跨链的 ACTIVE 资源冲突检查、`LeaseReplay` 的字段与顺序不变。
3. **内核指纹** `kernel_fingerprint`：对决定校验结论的源码与策略做哈希——`parallel_control_kernel.py`、`workflow_coordination.py`（hook-ready 与转移校验）、`config/architecture/arch_005_parallel_control_policy.yaml`、`config/architecture/arch_005_s4d_checkout_guard.yaml`，外加解释器小版本；清单是一个有注释的具名常量，并有静态守卫测试：重放路径新引入的校验模块不在清单里就失败。指纹变了，整份封印失效，回到全量重放，直到显式重建。
4. **封印怎么来**：显式命令（`architecture_arch005_lease_arbiter.py seal`，在仲裁锁内）先做一次**全量重放（不使用任何旧封印）**，再写封印；`seal verify` 做「封印重放 vs 全量重放」的 `to_dict()` 逐项比较，不一致即失败。S3 发布命令在 completed 释放之后加一步 `seal`（约 25 s），并在发布前检查里加一项 `seal verify`。

### 10.3 信任模型（需要 owner 评审的核心）
- **封印不替代完整性检查**：每次重放仍逐字节哈希每个事件文件，任何字节的增删改、重排都会让该链回退到全量校验并得到与今天相同的结论（包括 issues）。
- **被信任的只有一件事**：「这批字节在同一份内核代码与策略下曾通过完整校验」这个**结论**，复用于字节完全相同的终态链。
- **局限（如实写明）**：封印是本机的本地状态，与租约库本身同一信任级别，不是签名。能同时伪造事件文件和重算封印的写入者，可以让一条本会被全量校验拒绝的历史被接受；全量校验也防不了能写事件文件的恶意者伪造一条合法链。本系统的威胁模型是事故、漂移与 agent 误操作，不是恶意本机写入者，两种情况都在范围外。
- **缓解**：(1) 内核指纹把封印绑定到代码与策略；(2) `seal verify` 在正式验证里对真实库做封印与全量的逐项对账（Full 里加一个真实库测试，发布前检查里加一项）；(3) 任何封印问题的默认动作是全量校验（fail-safe，不是 fail-open）；(4) 全量重放始终可用，环境变量 `AITS_LEASE_SEAL=off` 强制使用；(5) 封印只收录来源重放无 issue 的终态链。
- **仍然不引入**：隐式的跨调用缓存、进程级缓存、对历史事件文件的任何改写。
- **与既有决定直接冲突（必须先说清）**：封印本身就是一种落盘的「已校验」结论。`FileExecutionLeaseStore._replay_uncached` 的注释明确写着不接纳任何 caller-owned event 或落盘的 already-checked 标记；DEVX-018 的 O3（跨调用缓存与落盘已校验标记）被 owner 否决过，DEVX-023 第 2 节沿用了这条边界。所以封印 S 不是在既有边界之内的优化，而是请 owner **把这条边界改为**：允许一种显式、哈希绑定、绑定内核指纹、可被 `seal verify` 对账、首次发布默认关闭的封印。**如果 owner 不愿改这条边界，就不应做封印 S**，改走第 10.8 节不改边界的替代方案。（本页最初把封印写成「沿用 O3 的边界」，不准确，已更正。）

### 10.4 验收标准（以实测为准）
1. 在真实库和全部变异夹具上，封印重放的 `to_dict()` 与全量重放**逐项相同**（含 issues）。变异类：篡改一个字节、截断、删除文件、新增文件、重排文件名、用另一个合法事件替换、改变头状态；各自发生在已封印链与未封印链上；
2. 封印缺失、损坏、旧 schema、指纹不符、`seal_sha256` 不符 → 行为与今天完全一致（全量重放，结论相同）；
3. 随机单字节翻转的性质测试：任一事件文件的任意单字节变化都使该链被全量校验；
4. 真实库一次重放 ≤ 全量的 30%（目标：约 21 s → ≤ 6 s，静机三次取中位数）；
5. 实测收益：stage 1 ≤ 350 s（现 761 s）；`local-publish` ≤ 35 分钟（现 57 分钟）；围栏命令墙钟降到现在的 1/3 以内；每次发布给重放增加的耗时 < +0.5 s（现 +1.7 s）；
6. 不改变租约语义；`system_flow` 与任务行同步；`docs/requirements/DEVX-022` 第 15.1 节的基线按新实测更新。

### 10.5 步骤与顺序
| 步骤 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| S0 | owner 评审本节的信任模型与三个决定（10.7） | DEVX-016 C2 已发布 | owner 批准或提出修改 |
| S1 | 实现：`lease_replay_seal.py`（构建、读取、校验、指纹）+ 内核里的窄钩子 + `seal` / `seal verify` 命令 | S0 | 10.4 第 2 条 |
| S2 | 测试（全是新文件）：变异-差分、性质、回退、原子写入、指纹静态守卫 | S1 | 10.4 第 1、3 条 |
| S3 | 候选 A：封印功能随候选发布，**默认关闭**（`AITS_LEASE_SEAL=1` 才启用）；候选的正式验证里真实库 `seal verify` 通过 | S2 | 全部 tier + Full 通过；对账一致 |
| S4 | 发布后用封印重放测一次真实收益（stage 1、围栏命令、`local-publish`），写回 DEVX-022 第 17 节 | S3 | 10.4 第 4 条 |
| S5 | 候选 B：默认开启，S3 命令加 `seal` 与 `seal verify` 步骤 | S4 对账一致 | 10.4 第 5 条 |

### 10.6 风险
- **重放是整套治理的核心**：实现错误会让守卫接受无效历史。缓解是变异-差分测试、`seal verify` 常驻正式验证、默认关闭的第一次发布（S3），以及 `AITS_LEASE_SEAL=off`。
- **历史固定源码**：`parallel_control_kernel.py` 在兼容权威里被哈希固定，改动需按 DEVX-015/S3b 回归的做法接管（加入最新 section 的源路径列表与 `DEVX_015_WORKFLOW_ADDED_SOURCE_PATHS`）；新测试一律放新文件。
- **封印过期**：新终态链未入封印时只是该链走全量校验，不影响正确性；超过阈值（建议 50 条）时 `seal verify` 报告「建议重建」，不判失败。
- **规模**：约 600–800 行代码与约 40 项测试，一个候选加一个后续候选。

### 10.7 需要 owner 决定
0. **边界**：是否同意把「不落盘已校验标记」（DEVX-018 O3、`_replay_uncached` 的不变量）改为「允许经 `seal verify` 对账的显式封印」？这一条是前提；答「否」则下面三条不必回答，改走第 10.8 节。
1. **信任模型**：接受「封印复用的是对字节完全相同终态链的完整校验结论，绑定内核指纹，本地、不签名」吗？
2. **封印由谁重建**：显式命令，S3 发布命令在 completed 释放之后自动执行一步（建议），还是只手工执行？
3. **上线节奏**：先默认关闭发一次再默认开启（建议，多一个候选的成本），还是一次到位？

**owner 决定（2026-10-09，AskUserQuestion，选项「批准信任模型，开始实现 (Recommended)」）**：0 号问题（边界）与 1 号问题（信任模型）= **批准**——允许一种显式、哈希绑定、绑定内核指纹、可被 `seal verify` 对账、默认关闭的封印，接受「封印复用的是对字节完全相同的终态链的完整校验结论，本地、不签名」。2 号（重建）与 3 号（上线节奏）按本节建议执行：显式命令，候选 B 里由 S3 发布命令自动重建；先默认关闭发一次（候选 A），再默认开启（候选 B）。该选项的文字写明「先出实现与差分 / 变异测试，仍由你评审后才在发布链里启用」，所以**候选 A 只含实现，默认关闭**；在发布链里启用（候选 B）和是否把封印模块放进命名 DQ 受限子进程的评审导入清单（见 10.9），都要等 owner 看过候选 A 的实测后再定。

### 10.8 不改信任边界的替代方案（未实测，列出以便比较）
租约库的校验是**按租约链相互独立**的（`_replay_lease_events` 逐链检查唯一 id、因果头、转移规则、执行转移；跨链只有最后的 ACTIVE 资源冲突检查），所以：
- **A. 减少重放次数**：stage 1 约 32 次重放里，测试进程里每次活体证明连做 2 次（`fence.validate` 内一次、证明构造里 `guard.replay()` 再一次，同一库状态），生产 CLI 子进程里有 5 次与 12 次。去掉证明里重复的那一次，测试进程可省 7 次（约 150 s，约 20%）；子进程里的 `restore`/`recheck` 是各阶段的权威复核，减少它们是语义评审，不是机械优化。
- **B. 按租约链并行重放（多进程）**：每条链的 `parse_lease_event` 与转移校验互不依赖，可分给 N 个工作进程，父进程只汇总每条链的结论并解析头事件。无持久化状态、不改信任边界；理论上 21 s 可到约 4–6 s（受 Windows 进程启动、导入内核模块与结果回传的开销限制），**尚未实测**，需要先做只读原型测量。风险是并行实现的复杂度与围栏命令（持锁、git 钩子里）下启动进程池的行为。
- **C. 微优化**：`_validated_hook_ready_paths` 去掉 `Path` 往返（cProfile 显示 33 次调用合计 6.3 s，约 −14%）、JSON 解码与规范序列化的重复（解码 7.3 s、规范序列化 7.0 s）——语义不变，收益有限。
比较：封印 S 的收益最大且最稳定（约 4×），但需要改边界；B 不改边界、收益潜力接近，但未测、实现复杂；A 只对 stage 1 的测试进程有用。建议 owner 在 0 号问题上先表态；若「不改边界」，我先做 B 的只读原型再决定。

#### 10.8.1 B 的只读原型实测（2026-10-08 00:15；owner 选择「先测并行重放原型」）
- **方法**：真实库（5,978 个事件、877 条链，`scratchpad/par_replay_proto.py`，不写库、不改仓库）。工作进程各自解析并校验整条链（沿用内核的 `parse_lease_event` 与 `_replay_lease_events`，每个进程有自己的重放记忆域），父进程按 `_replay_lease_events` 的方式合并并做最后的 ACTIVE 资源冲突检查；重的链先派（LPT）。不持久化任何东西，不改信任边界。
- **结果与串行逐项相同**：`status`、`lease_heads`、`active_leases`、`head_event_ids`、`event_count`、`issues` 全部相等（每个配置、每次试验都对账）。
- **耗时**（串行 17.7–20.7 s；含建进程池与导入，冷启动）：2 个工作进程 13.1–13.5 s；4 个 7.8–8.0 s；8 个 7.6–8.0 s；12 个 8.2–8.4 s；16 个 8.8 s；24 个 9.3–9.5 s；32 个 10.5–10.9 s。即约 **2.3×，4–8 个进程就到顶**，更多进程只增加启动开销；同一个池里的第二次重放 8–9 s，没有更快。父进程汇总（反序列化头租约）1.1–1.9 s。
- **到顶的原因是链的耗时极度偏斜**：877 条链的单链耗时合计约 26.5 s，其中 7 条发布链占约 24 s；最重的两条（`lease-6f32…`、`lease-e888…`）各含 292–362 MB 的历史 v2 事件（单个事件约 22 MB），独立测量分别 5.8 s 与 4.7 s（JSON 解码 2.4 s + `parse_lease_event` 2.0–3.0 s + 链校验 0.3–0.4 s；并发时 7.4 s 与 6.2 s）。它们是不会再增长的历史链，所以链级并行的下限就是它们的单链耗时加启动与汇总；其余 870 余条链合计只有约 2.5 s。
- **还能更快的路径**：事件级并行（把这两条链的事件也分给进程，解析互不依赖，只有转移校验按顺序）理论上可到约 4–5 s，但实现更复杂；封印 S 直接跳过这两条链（哈希核对 + 解析头事件），原型 4.4 s。
- **实现要点与风险**：(1) `ExternalizedRows`（刻意不可变的 `list` 子类）需要一个可 pickle 的 reducer，否则头租约无法从工作进程回传（原型里用 `copyreg` 补；生产里是内核的小改动）；(2) 每个工作进程在 Windows 上 `spawn` 并导入内核约 1–2 s，所以每次重放都新建进程池约 +2 s，仍远小于收益；(3) Full 里 16 个 xdist 工作进程若同时重放会造成进程数超订，需要一个全局上限（环境变量，默认取很小的值）；(4) 在围栏命令持锁段、git 钩子与 job object 里启动子进程的行为需要实测；(5) 事件目录里若有事件的 `lease_id` 与目录名不符（串行实现会按 `lease_id` 重新分组），并行实现必须检测并回退到串行；(6) 必须保留串行实现作为权威并默认可关闭。
- **估算收益**（按 stage 1 约 32 次重放、其余约 76 s 不变）：并行 stage 1 约 785 → 330–400 s，`local-publish` 约 61 → 35 分钟左右；封印 S：stage 1 → 约 280 s，`local-publish` → 约 25 分钟。**两者的差别**：并行约 2.3×、不改信任边界、可用环境变量关闭、结果可逐项对账，但复杂度在多进程；封印约 4×、随库增长更稳，但要改边界并评审信任模型。两者可以先后做（并行先行，封印留作后备）。

### 10.9 实现计划（2026-10-09；对 10.2–10.5 的细化与四处更正）

**布局（全是新文件，加内核里的一个窄钩子）**：
- `src/ai_trading_system/platform/architecture/lease_replay_seal.py`：开关解析、内核指纹、封印的构建 / 原子写 / 读取校验、封印感知的重放、`verify`（封印重放与全量重放的 `to_dict()` 逐项比较）；
- `scripts/architecture_arch005_lease_seal.py`：`build`（持仲裁锁，先做不使用任何旧封印的逐链全量校验再写封印）、`verify`（只读）、`status`（只读，说明封印为什么有效 / 失效）。不放进现有的 `architecture_arch005_lease_arbiter.py`——那是 DEVX-014 的迁移脚本，被哈希固定；
- `parallel_control_kernel.py`：`FileExecutionLeaseStore._replay_uncached` 开头一个窄钩子——`AITS_LEASE_SEAL` 未设 / 空 / `0` / `off` 时不导入、不读封印，行为与改动前逐字相同；开启时导入被拒或任何异常都回到原路径（并行 / 串行）；
- 测试（全是新文件）：`tests/test_arch_005_lease_replay_seal.py`（夹具库上的差分与变异、性质、回退、原子写、指纹、开关、与 W / P 的交互）。

**对 10.2–10.5 的更正与细化**：
1. **终态 = `_LEASE_TRANSITIONS` 里没有出边的状态，即 `RELEASED` 与 `REASSIGNED`**。10.2 写的是 RELEASED / BLOCKED / REASSIGNED，不对：`BLOCKED` 有 `BLOCKED -> RELEASED` 的取消路径，不封印；终态集合直接从内核的转移表派生，不另写一份清单。
2. **内核指纹改为「目录规则」，不是 4 个文件的具名清单**（更正 10.2 第 3 点）。实测一次完整重放执行代码的仓库文件只有 `workflow_coordination.py`、`parallel_control_kernel.py`、`workflow_contract.py`、`workflow_integration.py` 与 `artifacts/writer.py`，但 `workflow_coordination.py` 与内核的静态导入闭包有 42 个模块（含延迟导入），逐个判断「谁影响结论」既容易漏、又要持续维护清单。指纹因此取一个**超集**：`src/ai_trading_system/platform/architecture/**/*.py` 与 `platform/artifacts/*.py` 的逐文件哈希、`yaml_loader.py`、`config/architecture/arch_005_parallel_control_policy.yaml`、`config/architecture/arch_005_s4d_checkout_guard.yaml`、解释器小版本与 `SEAL_CODE_VERSION`。超集永远安全；代价只是任何一个这些源文件变了封印就失效（回到全量重放，行为与今天相同）。因为 `src` 在一条发布链内不变，而候选 B 让 S3 命令在链开头重建封印（约 25 s），所以代价可以忽略。静态守卫改为测试：改动其中任何一个文件、策略或解释器版本都会改变指纹；`__pycache__` 与测试文件不入指纹。
3. **封印文件**位于租约库根的 `replay_seal.v1.json`（不在 `events/` 下，不影响链的枚举），只用 `write_bytes_atomic` 写；每条链一项：文件清单（文件名 + 内容 sha256）、`head_event_id`、`head_state`、`event_count`；头部含 `kernel_fingerprint`、创建时间、来源重放的事件数与链数；`seal_sha256` 对规范化正文。**只收录来源校验无 issue 的终态链**；`build` 不要求整库 PASS，但在摘要里写出非终态链与有 issue 的链。
4. **封印感知的重放**：读封印并校验 schema / `seal_sha256` / 指纹，任一不符 → 返回 `None`（调用方走原路径）。通过后对每个链目录：先逐字节哈希目录里的全部事件文件；该链在封印里、文件清单与哈希逐项相同、头事件文件存在且解析后 `lease_id` 等于目录名、`event_id` 等于封印的 `head_event_id`、状态仍是终态 → 只完整解析校验头事件，其余事件复用封印的结论；其他任何情形（未封印、被改动、新增、缺文件、顺序不同、头事件不符）→ 该链走与串行完全相同的逐链全量校验。跨链的 ACTIVE 冲突检查与 `LeaseReplay` 的字段、排序都经内核已有的 `_assemble_lease_replay`，只有一份。
5. **命名 DQ 受限子进程**：候选 A 不改它们（它们是按设计保持串行的，12.4）。它们是 stage 1 里约七成重放和 Full 关键路径（命名 DQ 候选文件 9,207 s）的所在，所以封印要真正降低 Full，必须把 `lease_replay_seal` 放进它们的评审导入清单（`scripts/run_named_data_quality.py` 的捕获式加载器只服务清单内的模块）——这会略微放宽受限子进程的导入边界（封印模块是评审过的提交代码，封印文件是它已经信任的租约库里的数据，但它仍是边界变化）。**这是候选 B 的独立 owner 决定，候选 A 里不做。**

**步骤与验收（沿用 10.5 的编号）**：
| 步骤 | 内容 | 验收 |
|---|---|---|
| S1 | `lease_replay_seal.py` + 内核窄钩子 + `architecture_arch005_lease_seal.py`（`build` / `verify` / `status`） | 默认关闭时行为与改动前逐字相同（既有租约 / 内核 / 围栏聚焦回归不变） |
| S2 | 测试：封印重放与全量重放 `to_dict()` 逐项相同（含 issues）——在全封印、部分封印、新增链、非终态链的夹具库上；变异类（篡改一个字节、截断、删除文件、新增文件、重排文件名、用另一个合法事件替换、改变头状态）各自发生在已封印链与未封印链上；封印缺失 / 损坏 / 旧 schema / 指纹不符 / `seal_sha256` 不符 → 与今天完全一致；随机单字节翻转的性质测试；原子写；开关与回退；与 W / P 的交互；真实库 `verify` 的测试（无真实库则跳过） | 10.4 第 1–3 条 |
| S3 | 候选 A：随候选发布，**默认关闭**；候选的正式验证里 `verify` 在拷贝的真实库上通过 | 全部 tier + Full 通过；对账一致 |
| S3.5 | 发布后在真实库上手工 `build` + `verify` + 计时（stage 1 的 14 次测试进程重放、围栏命令、钩子里的重放），写回 DEVX-022 | 10.4 第 4 条（一次重放 ≤ 全量的 30%） |
| S4 | owner 看过 S3.5 的实测，决定候选 B 的两件事：在发布链里启用（S3 命令在链开头 `build`，发布前检查加 `verify`）、以及是否放进命名 DQ 的评审导入清单 | owner 批准 |
| S5 | 候选 B：默认开启；S3 命令加 `seal` 与 `seal verify` 步骤；视 owner 决定改命名 DQ 的导入清单 | 10.4 第 5 条 |

**风险与回滚**：重放是整套治理的核心，实现错误会让守卫接受无效历史——缓解是变异 – 差分测试、`verify` 常驻正式验证、默认关闭的第一次发布，以及随时可用的 `AITS_LEASE_SEAL=off`（或删除 `replay_seal.v1.json`）。`parallel_control_kernel.py` 是历史固定源码，改动按 DEVX-023 P 的做法（只改内核窄钩子，权威由生成器链刷新）。规模：约 500–700 行代码与约 40 项测试。

#### 10.9.1 实现记录与实测（2026-10-09；候选 A，默认关闭）

**交付**：`lease_replay_seal.py`（开关、内核指纹、构建 / 原子写 / 读取校验、封印感知重放、`verify`、`status`）、`parallel_control_kernel.py` 里的一个窄钩子（`_replay_uncached` 开头；`AITS_LEASE_SEAL` 未设则不导入、不读封印）和 `parse_lease_event` 的一个默认不变的关键字参数、`scripts/architecture_arch005_lease_seal.py`（`build` 持仲裁锁并用 `write_bytes_atomic` 一次替换；`verify`、`status` 只读）、`tests/test_arch_005_lease_replay_seal.py`（82 项）。相邻的内核 / 租约库 / 并行重放 / 外置行表回归 238 项通过。

**实现中发现并处理的三处设计稿遗漏**（对 10.2–10.9 的补充）：
1. **封印必须覆盖链引用的外置行表（blob）**。设计稿只逐字节哈希事件文件，但 v3 / v4 事件里的大表存放在 `blobs/<摘要前两位>/<摘要>.json`，被改动或删除的 blob 会让全量解析报 `LEASE_EXTERNALIZED_ROWS_*`，而封印路径不解析非头事件，就会漏掉。现在构建时用一个记录型读取器记下每条链的事件读过哪些 blob（摘要 + 行数），封印里存下；重放时核对每个 blob 是常规文件、大小有界、字节哈希等于它自己的名字（每次调用每个 blob 只哈希一次）。真实库 16 个 blob、89 MB，哈希全部 0.06 s。
2. **封印头事件的语义校验可以跳过，但结构、schema、事件 id 与 blob 不能**。第一版按设计稿完整解析头事件，真实库副本上的封印重放要 11–12 s——11 条发布链的头事件各自重跑一次 hook-ready 完整校验（`validate_execution`，每次约 0.5–0.7 s）。头事件的字节与封印时通过完整校验的字节逐字节相同，所以和其余事件一样复用那个结论：`parse_lease_event(..., validate_execution_payload=False)`（默认 True，封印路径是唯一调用方），仍检查结构、schema、事件 id 的哈希与 blob。封印重放降到 3.4–4.2 s。
3. **串行重放按事件里的 lease id 分组，而不是按目录**。变异测试发现：把另一条链的事件拷进某个目录，串行重放会把它记到那条（可能已封印、目录没变的）链上，报重复事件 id；按目录的封印路径看不到。所以只要一个需要完整校验的目录里出现 lease id 不等于目录名的事件，整个封印路径就**放弃**（`SealBailout`），交给正常重放回答；`verify` 也报告这种放弃。构建时同样统计这类目录。

**真实库副本上的实测**（`D:/Work/p-seal-store` = 真实库的 `events/` 与 `blobs/` 拷贝，898 条链 / 6,282 个事件，不触碰真实库；证据 `outputs/architecture/integration_revalidation/devx015-v389/claude_seal_real_store_differential.json`）：构建 31–35 s（896 条终态链入封印，2 条非终态链不入），封印文件 911 KB；`verify`：封印重放与串行全量重放的 `to_dict()` **逐项相同**；经 `store.replay()`：开关未设 32.7 s，`AITS_LEASE_SEAL=1` **3.4 / 4.2 s**（约 8–9 倍），`off` 30.9 s。纯逐字节读取并哈希全部事件文件 0.99 s。验收 10.4 第 4 条（一次重放 ≤ 全量的 30%）：达成（≈ 12%）。

**步骤状态**：S1 完成、S2 完成、S3 完成（候选 A 于 2026-10-09 随第十次发布上线，默认关闭；第一次发布尝试因与封印无关的写死 5 s git 探测超时失败，加固后重发一次通过，见 DEVX-022 第 17.12、17.13 节）、S3.5 完成（下面 10.9.2）；S4（owner 看过实测后决定候选 B：链内启用，以及是否把模块加入命名 DQ 受限子进程的评审导入清单）待决定。

#### 10.9.2 S3.5：真实库上的实测（2026-10-09 21:31–21:36，候选 A 发布之后；证据 `claude_seal_s35_real_store.json`）

**对象**：真实租约库，6,375 个事件 / 903 条链（第十次发布之后）。每个计时都在新进程里跑，没有进程内备忘可借；脚本在会话 scratchpad（`seal_s35.py`、`seal_s35_timer.py`），宿主在清理刚结束后，Defender 仍可能在后台。唯一的写入是 `build` 在库根目录放下 `replay_seal.v1.json`（922,765 字节）。

| 模式 | 重放秒数（3 次 / 2 次） | 均值 | 对串行 |
|---|---|---|---|
| 串行（开关全关） | 33.12 / 32.39 / 32.06 | 32.52 | 1× |
| 4 进程并行（P6，`AITS_LEASE_PARALLEL_REPLAY=4`） | 13.76 / 13.25 | 13.51 | 2.4× |
| 封印（`AITS_LEASE_SEAL=1`） | 3.24 / 2.77 / 3.07 | 3.03 | **10.7×** |

- **构建**：`build` 35.2 s（库内计时 34.6 s），覆盖 901 条终态链 / 6,371 个事件；2 条非终态链（共 4 个事件）不封印、每次走完整重放；`foreign_event_directories` 0、`skipped_with_issues` 0、`skipped_other` 0。
- **验证**：`verify` 37.2 s，`ok: true`，`differing_fields` 为空——封印路径（3.05 s）与串行完整重放（33.08 s）逐字段相同。
- **结果相同**：串行、并行、封印三种模式的 `replay.to_dict()` 规范 JSON 的 sha256 完全相同（`e9bb36a7…0b3b`），状态 PASS，事件数 6,375，head 数 903。
- **指纹**：记录的内核指纹与当前内核指纹相同；`status` 报告「若启用则生效」，并预计只有 2 条链、4 个事件需要完整重放。
- **留在库里的东西**：封印文件现在留在真实库根目录，开关默认关闭，对任何进程无效；库继续增长后，新增链按未封印链完整校验、已封印链重新计算文件与 blob 哈希，所以过期封印不会产生错误结果，只会少拿一点收益；重建是显式命令（候选 B 才会让发布命令在链开头自动重建）。

**与设计预期的对照**：真实库副本上（898 条链）的预期是 8–9 倍（28.6–32.7 s → 3.4–4.2 s）；真实库实测 10.7 倍（32.5 → 3.0 s），因为串行基线随库增长了，而封印路径虽然重新哈希全部事件文件、但只解析头事件，成本随库增长慢得多。

**候选 B 的预期效果（估计，未实测）**：(1) 开关在发布命令及其子进程、`local-publish` 的 worker 与钩子里打开：这些进程里每次重放约 13 s（P6）降到约 3 s，E52 / E56 / E57 / E61 / E62 与 `local-publish` 中的重放部分再缩小 3–4 倍；(2) stage 1 与 Full 里测试进程内的重放同样降低，但命名 DQ 受限子进程（stage 1 约七成重放、Full 里关键路径上的命名 DQ 候选文件）只有在 `lease_replay_seal` 被加入其评审导入清单之后才能受益——这是放宽一个隔离边界的决定，须 owner 评审；(3) 不启用任何一项，Full 仍会继续按约 +5% / 次增长。


#### 10.10 候选 B：只在发布链里启用封印（owner 2026-10-09 夜选择；实施计划）

**owner 决定**（AskUserQuestion，选项「只在发布链里启用 (Recommended)」）：封印只在发布链的进程里启用——发布命令及其子进程、stage 1 的测试进程、`local-publish` worker 与钩子；发布命令在链开头重建并校验封印。**不**动命名 DQ 受限子进程的评审导入清单（隔离边界不变），**不**在正式 Full 里启用（Full 合同不变）。要把收益带进 Full，须另行评审把 `lease_replay_seal` 加入命名 DQ 导入清单的设计（放宽一个隔离边界），这次没有选。

**范围与不变量**
- **策略**：与 P6 共用同一份经评审的角色策略 `config/architecture/devx_023_parallel_replay_scope.v1.yaml`（版本 1.0.0 → 1.1.0），新增 `lease_seal` 一节，角色同 P6：`s3_command`、`validation_stage_1`、`local_publish_worker` 为 enabled；`validation_pre_full_tiers` disabled（先测量）；`formal_full`、`named_dq_restricted_children` 为 `never_enabled`（载入器把启用它们的策略视为无效并什么都不启用）。状态 `PILOT_BASELINE`；退出条件：下一条链的实测之后换成有证据的 `OWNER_APPROVED`，且不晚于 2027-01-08。回滚 = 把角色改成 disabled 或把状态改成 `DISABLED`（不改代码）。
- **开关**：`AITS_LEASE_SEAL=1` 只由策略按角色给；继承来的值先被清掉（与 P6 同一套 `without_switch` / `apply_switch` / `environment_for`）。封印命中、未命中、被拒绝导入（命名 DQ 受限子进程，内核钩子已经把失败的导入当作「没有封印」）、指纹失配都不改变结果，只改变耗时；串行重放仍是权威。
- **新步骤 `A06.seal_rebuild`**（阶段 A，依赖门之后、prep 事务之前）：`architecture_arch005_lease_seal.py build` 然后 `verify`；策略里 `s3_command` 未启用或策略无效时记 `NOT_APPLICABLE`。`build` 持仲裁锁并完整校验每条链，所以此刻不能有活动租约（A02 干净树与 A03 安静宿主已经保证）；`verify` 比对封印重放与串行重放逐字段相同，不同则步骤失败、什么都不继续。真实库上 build 35 s + verify 37 s，预期给链增加约 75 s，换回链里所有串行重放。封印绑定内核指纹（`platform/architecture/**` 等），候选改了这些文件后封印失效，所以必须在链内用候选自己的代码重建——这是放在链开头而不是链尾的原因。
- **S3 小项 (g)（同一候选）**：E50 与 A03 共用的进程检查，对桌面应用自己的瞬时 `git.exe` 轮询加有界重试（3 次、间隔 5 s，具名常量，属 AGENTS 允许的低风险常量）；最后一次仍看到进程就照旧失败；不放宽对任何进程类型的要求，也不按命令行签名放行。

**验收（以实测为准；发布本候选的链仍跑旧代码，收益只能在下一条链里量到）**
1. `A06` 在真实库上 build + verify 合计 ≤ 90 s 且 PASS；journal 记录封印链数、事件数与指纹；
2. E52 / E56 各降 ≥ 40%（v2：147.3 / 163.9 s），E57 / E61 / E62 各降 ≥ 25%，`local-publish`（E54）≤ 28 分钟（v2：39:19），A–C 不变或更快，stage 1 ≤ 650 s（v2：741.9 s；受限子进程里约七成重放仍串行，所以不指望更多）；
3. Full 的 pytest 不变（Full 角色是 `never_enabled`；±3% 之外的变化要解释）；journal 无 FAILED，`slow_steps` 为空；
4. 全程结果与串行逐字节相同（A06 的 `verify` 每条链开头都做一次，加上既有的 82 项封印测试）。

**测试**：角色策略（`lease_seal` 一节的合法 / 非法形态、`never_enabled`、状态不在 `ENABLED_STATUSES` 则什么都不启用）、`environment_for` 同时给出两个开关、`without_switch` 清掉两个、与 `lease_replay_seal.SEAL_ENV` 的一致性；A06 的步骤计划与顺序、假执行器上的 build / verify 成功 / 失败 / `NOT_APPLICABLE` / 重入；D30 的参数与 E53 的环境带上封印开关；E50 重试（第一次看到进程、第二次干净 → 通过；三次都有 → 失败）。`docs/system_flow.md` 加一段并重算封印。


**实现记录（2026-10-09 夜，候选 B 已实现、尚未冻结）**：
- **策略**：`config/architecture/devx_023_parallel_replay_scope.v1.yaml` 升到 1.1.0，新增 `lease_seal` 一节（`s3_command`、`validation_stage_1`、`local_publish_worker` 为 enabled，`validation_pre_full_tiers` disabled，`formal_full` 与 `named_dq_restricted_children` 为 `never_enabled`）。
- **角色模块** `parallel_replay_scope.py`：`ParallelReplayScope` 多一个默认空的 `seal_roles`；`seal_for(role)`；`environment_for(role)` 按角色给出一个或两个开关（`AITS_LEASE_PARALLEL_REPLAY`、`AITS_LEASE_SEAL`）；`without_switch` / `apply_switch` 同时处理两个开关；缺 `lease_seal` 一节 = 不给封印（P6 之前形态的策略仍然有效），`lease_seal` 不合法 = 整份策略无效、什么都不开（新增问题码 `SCOPE_CONFIG_SEAL_SECTION` / `_SEAL_ROLES` / `_SEAL_ROLE_VALUE` / `_SEAL_NEVER_ENABLED_ROLE`）；`SEAL_ENV` 与 `lease_replay_seal.SEAL_ENV` 由测试钉成相等。
- **发布命令**：`publication_steps.py` 新增 `A06.seal_rebuild`（`build_precondition_steps(..., seal_rebuild=...)`，由 `publication_cli.py` 按 `s3_command` 角色传入；`build` 然后 `verify`，结果事实记 `seal`、`seal_sha256`、`kernel_fingerprint`、`sealed_chains`、`sealed_events`、构建与核对耗时；`NOT_APPLICABLE` / `PUBLICATION_RUN_SEAL_BUILD` / `PUBLICATION_RUN_SEAL_VERIFY`；基线 90 s 仅用于报告）；`publication_publish_steps.py` 的 D30 在 stage 1 角色有封印时给驱动加 `--stage-1-lease-seal`，E53 的 worker 环境经 `environment_for` 自动带上；`scripts/architecture_arch005_validate_candidate.py` 把该参数变成 stage 1 的环境（驱动进程本身不带开关）。
- **S3 小项 (g)**：`HostFactsCollector.settled_live_processes()`（5 次观察、间隔 6 s，具名常量）被 A03、D30 的「没有其他进程」检查与 E50 共用；原来 E50 的「重试 3 次」之间没有间隔，一次桌面应用的 `git.exe` 轮询只要持续超过几秒就会连续看到。
- **测试**：新文件 `tests/test_arch_005_lease_seal_scope.py`（29 项：策略各形态、两个开关的环境、A06 的各种结果、有界重复观察）；既有的 P6 范围测试 3 项按 1.1.0 更新；运行与 CLI 测试各加 4 项与 2 项的接线测试；假的 World 多了封印 CLI 的脚本化应答。聚焦回归 400 项通过（含封印 82 项与并行重放测试）；`architecture_devex.py validate`：依赖门 PASS，只剩 3 条预期的清单新鲜度项；006d 的 3 个新鲜度测试在生成器链之前红（system_flow 片段待重建），属预期。

## 11. W-DQ（可选）：命名 DQ 证明只保留源租约的重放视图（设计草案，2026-10-07；不是耗时对策）

**更正**：本节最初是按「stage 1 耗时随证明体积增长」的归因写的，owner 也据此一度把它排在 C3 之前。C2 窗口的逐调用计时（DEVX-022 第 17.7 节）否定了该归因：stage 1 的 87% 是约 31 次租约库重放，证明构造与序列化只占约 3%（再加上子进程里的解析与哈希，估计合计 3–15%）。owner 随后改为 C2 → 封印 S（第 10 节）→ C3。本节降为可选的小改动，价值是证据体积卫生（每次 stage 1 运行写出约 0.7 GB 的证明 JSON，每次发布 +约 54 MB）与少量耗时，不再作为耗时对策，也不再有时间验收目标。

### 11.1 问题与量化
- 命名 DQ 活体证明内嵌整库重放，体积随发布线性增长：`test_parent_pre_guard.json` 35.70 → 46.60 → 57.52 → 68.40 MB（每次发布 +约 10.9 MB），`activation_cli_*_parent.json` 170.7 → 224.4 → 278.2 → 331.9 MB；每个证明文件还会被写出多份（测试 2 每次运行写 2 份约 332 MB 的 `activation_cli_*_parent.json`，子进程 stdout 约 68 MB），保留的证据目录随之增长。
- 这部分对 stage 1 耗时的贡献很小（序列化约 24 s / 761 s；见 DEVX-022 第 17.7 节），所以本节**不以耗时为目标**。

### 11.2 读码结论（谁产生、谁使用）
- **生产者**：`src/ai_trading_system/data/named_quality_dispatch.py` 的 `_read_lease` 在返回的 `named_capture_lease_recheck.v1` 里写入 `"lease_replay": replay.to_dict()`，即**全部**租约 head（约 870 个，每个带自己的 execution 载荷）；`restore_named_capture_lease`、`recheck_named_capture_lease`（父进程每次检查）与 `prospective_capture_execution` 的 `prospective_capture_parent_proof.v1` 都经过它。所有生产路径都经过这一处：`composer_prospective_capture.py`（第 294 行）、`prospective_capture_execution.py`（第 302 行）、`research_outcome_access.py`（第 272 行）与 `named_quality_dispatch.py` 的派发父证明（第 710 行）都调用 `recheck_named_capture_lease` → `_read_lease`，所以改一处即覆盖全部生产者。测试夹具 `tests/named_data_quality_support.py::_live_parent_proof` 另有一份同样内嵌整库重放的证明（只作为测试证据写出，生产代码不消费）。
- **唯一的验证器** `verify_retained_named_capture_proof`（同文件）对 `lease_replay` 只读：`status == PASS`、`issues == []`、`event_count` 为 ≥ 1 的整数，以及 `active_leases` / `lease_heads` / `head_event_ids` 三个列表里 `lease_id == source_lease_id` 的那一项（必须恰好等于 `[head.to_dict()]` / `[{lease_id, event_id}]`）。`named_quality_execution` 对父进程 postguard 的检查只读 `active_lease` 的 `lease_id` 与 `state`；其余消费者都经过同一个验证器。**没有任何消费者使用其他租约的 head。**
- 因此整库重放里 99.8% 以上的字节（其他租约的 head 正文）没有消费者，却在每次证明里被构造（`to_dict`）、规范序列化、哈希、写出、再解析。

### 11.3 方案：只改生产者，验证器不改
证明里的 `lease_replay` 改为「源租约视图」：
- 保留：`schema_version`、`status`、`event_count`、`issues`、`head_event_ids` **完整列表**（只含 lease_id 与 event_id，约 70 KB，对每个 head 的身份仍有承诺）；
- `lease_heads` 与 `active_leases` 只留 `lease_id == source_lease_id` 的那一项；
- 新增 `lease_head_count`（整库 head 数）与 `form = "SOURCE_LEASE_VIEW.v1"`，让证明自描述为过滤形态。
旧的完整形态证明（已留存的证据）仍能被同一验证器通过（它是超集），新形态也通过同一验证器，所以**没有新 schema、没有 fail-closed 的新标记，回滚安全**（与 S3c 不同）。测试夹具里的 `_live_parent_proof` 同步改为同一视图（用同一个视图函数，避免两边漂移）。

### 11.4 信任边界
- 证明的定位不变：验证器文档已写明它只验证「原始租约快照在记录时刻」，是 local-parent attestation，不是签名，也不替代原始的活体重放、来源封印与进程绑定。
- 丢弃的是其他租约的 head 正文（hook manifest、托管表等），它们没有消费者，且随时可由真实库重放得到；保留的完整 `head_event_ids` 仍把证明绑定到当时整库的 head 集合。
- 不引入任何新的缓存、封印或持久化检查点（与 DEVX-023 的封印 S 无关，S 仍待 owner 决定）。

### 11.5 验收标准（以实测为准）
1. 新证明与旧完整形态的夹具证明都通过 `verify_retained_named_capture_proof`；
2. 篡改仍被拒绝：现有 6 类变异（`active_lease`、`replay_head`、`replay_event`、`replay_count`、`audit_head`、`audit_dirty`）全部保留，并新增针对新形态的：源租约项缺失或被替换、`head_event_ids` 里源项被改、`event_count` 非整数、`issues` 非空；
3. 在真实库（≥ 870 个 head）上，一次证明的字节 < 1 MiB（目标约 0.1–0.3 MiB），且不再随每次发布增长：回归测试构造含 N 个大 head 的合成重放，证明大小与 N 的 head 正文无关、只随 `head_event_ids` 线性（约 +80 B / head）；
4. 证据体积：一次证明 < 1 MiB（见第 3 条），因此每次 stage 1 运行写出的证明 JSON 从约 0.7 GB 降到几十 MB；耗时只作观察项（预计省 3–15%），不作验收；
5. 不改变 DQ/PIT 语义、验证器判定结果与 receipt 合同的其他字段；`docs/system_flow.md` 命名 DQ 一段补一句。

### 11.6 步骤、依赖与顺序
| 步骤 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| W1 | 设计评审（owner；可选，优先级低于封印 S） | DEVX-016 C2 已发布 | owner 批准或提出修改 |
| W2 | 实现：生产者视图函数 + 测试夹具同步 | W1 | 11.5 第 1、2 条 |
| W3 | 新增测试文件（视图、兼容、篡改、大小回归、消费者枚举守卫） | W2 | 11.5 第 2、3 条 |
| W4 | 候选链：用 S3 命令发布（dogfood；owner 在自己的终端推送） | W3 | 全部 tier + Full 通过 |
| W5 | 发布后测量证明体积与耗时，写回 DEVX-022 第 17 节 | W4 | 11.5 第 3、4 条 |

### 11.7 风险与缓解
- **未发现的消费者读取其他租约的 head**：W3 增加静态守卫测试，枚举 `lease_replay` 在 `src/` 里的全部读取点并要求都在白名单内（当前：验证器与 `named_quality_execution` 的 postguard 检查）；
- **证明形态的判别**：验证器不需要判别；未来若要区分，`form` 字段存在即新形态；
- **受哈希固定的历史源码**：`named_quality_dispatch.py` 出现在兼容权威的源路径里（`compatibility_authority.py` 与 `tests/test_devx_006c_compatibility_authority.py` 各 3 处）。实现时先看它是否已在最新 section（V3）的源路径列表里；若不在，按 DEVX-015/S3b 回归的做法加入 `_devx_015_workflow_contract_section` 与 `DEVX_015_WORKFLOW_ADDED_SOURCE_PATHS`；新测试一律放新文件（被改过的已固定测试文件也需要权威）；
- **`head_event_ids` 随库增长**：每次发布 +1–3 个 head，约 +80–240 B，可忽略；
- **与封印 S 的关系**：本方案去掉的是「与证明体积成正比」的部分，单次重放本身（约 15–20 s，每次发布 +1.5–1.8 s）仍在；S 另议。

### 11.8 与耗时目标的关系
C2 的正式窗口里已用 `stage1_timing_plugin.py` 单独跑了一次 stage 1：重放占 87%，证明序列化约 3%（DEVX-022 第 17.7 节）；因此本节的耗时目标已撤销，只保留体积卫生目标。

## 12. P：链级并行重放（实现计划；owner 2026-10-08 选择「实现链级并行重放，再做 C3」）

依据：第 10.8.1 节的只读原型实测（真实库 5,978 个事件 / 877 条链，结果与串行逐项相同，重放 17.7–20.7 s → 7.6–8.0 s，约 2.3×，4–8 个进程到顶）。封印 S（第 10 节）留作后备，评审前不实现。

### 12.1 范围与不变量
- 只做**链级**并行：每条租约链的解析与校验互不依赖（`_replay_lease_events` 逐链检查唯一 id、因果头、转移规则与执行转移），跨链只有最后的 ACTIVE 资源冲突检查；事件级并行（再分拆最重的两条历史链）不在本任务范围。
- **串行实现保持权威**：默认关闭，由环境变量开启；任何异常、超时、池创建失败、反序列化失败、事件的 `lease_id` 与目录名不符，一律回退到串行重放并得到串行的结论。
- 不持久化任何东西、不跨调用缓存、不改信任边界；`LeaseReplay` 的 status / lease_heads / active_leases / head_event_ids / event_count / issues 与串行**逐项相同**。
- 不改历史事件文件，不改围栏/租约的语义，不改任何公开签名。

### 12.2 设计
1. **新模块** `src/ai_trading_system/platform/architecture/lease_parallel_replay.py`：`configured_workers(environ)`（解析开关）、`replay_in_parallel(events_root, blobs_root, workers)`（返回 `LeaseReplay` 或 `None` 表示调用方走串行）。工作进程用 `multiprocessing` 的 spawn 池：初始化时进入重放记忆域与行表共享域，任务是一条链的目录（按目录字节数从大到小派，重链先），工作进程返回该链的 `LeaseReplay` 片段（pickle）；父进程用与串行**同一个汇总函数**合并。
   池创建前检查 `__main__`：若它是一个脚本（有 `__file__` 且没有 `__spec__`）且源码里没有 `if __name__ == "__main__"` 守卫，则 spawn 会在子进程里重跑脚本顶层，所以直接回退串行。
2. **内核的小改动**（`parallel_control_kernel.py`，受哈希固定的历史源码，按 DEVX-015/S3b 回归的做法处理权威）：(a) `ExternalizedRows` 增加 `__reduce__`（可 pickle，仍不可变，摘要随行）；(b) 把 `_replay_lease_events` 末尾的汇总（ACTIVE 冲突检查、排序、`LeaseReplay` 构造）抽成 `_assemble_lease_replay`，串行与并行共用，排序与状态语义只有一份；
   (c) `_replay_uncached` 先问 `configured_workers()`，为 0 则走原来的串行主体（改名为 `_replay_serial`，逐字不变）；(d) 校验模式 `AITS_LEASE_PARALLEL_REPLAY_VERIFY=1`：并行结果再与一次串行重放比较，不一致抛 `LEASE_PARALLEL_REPLAY_MISMATCH`（与 W 的 `AITS_LEASE_SECTION_REPLAY_VERIFY` 同一思路）。
3. **开关与常量**：`AITS_LEASE_PARALLEL_REPLAY`：未设、空、`0`、`1`、`off`、`false` 或无法解析 → 串行；整数 N ≥ 2 → 最多 N 个工作进程（不超过上限）；`auto` / `on` → 默认进程数。具名常量（AGENTS.md 启发式治理：这是性能参数，不影响投资解释；记为试点基线，退出条件是用多次实测校准）：
   默认进程数 4、上限 8（实测 4–8 个进程同样快，16 个 8.8 s、32 个 10.7 s 只增加启动开销）；最小事件数 600（更小的库串行只要几秒，不值得 2 s 的建池）；等待结果的上限 600 s（超时回退串行）。
4. **第一次发布默认关闭**：候选里功能完整但默认关闭；候选链里用一次诊断运行（环境变量开启）对照 stage 1（现 785 s），差分测试在 Full 里常驻；发布后另起一个小候选，在 S3 命令与验证驱动的子进程环境里开启（`config/architecture` 里的已评审配置值），再测一次完整链的收益。

### 12.3 步骤、依赖与验收
| 步骤 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| P0 | 登记任务行与本节（本次） | owner 选择（2026-10-08） | 任务行 `IN_PROGRESS`，链接本节 |
| P1 | 实现：新模块 + 内核的小改动（12.2 第 1、2 点） | P0 | 默认关闭时行为与改动前逐字相同（既有租约/内核/围栏/命名 DQ 聚焦回归不变） |
| P2 | 测试（全是新文件）：差分与变异、回退、开关、池的健壮性 | P1 | 见下 |
| P3 | `system_flow` 与 DEVX-022 第 15.2 节补一句；`parallel_control_kernel.py` 的权威接管 | P1、P2 | 生成器链零差异 |
| P4 | 候选链：用 S3 命令发布（dogfood；owner 推送）；链内用环境变量开启做一次 stage 1 诊断运行 | P2、P3 | **已完成（2026-10-08，第七次发布 `5150efbac`）**：全部 tier + Full 通过（15,027 通过 / 4 跳过 / 0 失败）；诊断运行里测试进程内单次重放 21.43 → 8.20 s（38%，达成 ≤ 45%）；stage 1 718.6 s（关）→ 568.7 s（开，−21%）；原先写的 ≤ 450 s 作废（受限子进程保持串行，见 12.4） |
| P5 | 发布后在真实库上测量（重放、stage 1、围栏命令）并写回 DEVX-022 第 17 节 | P4 | **重放与 stage 1 已完成（2026-10-08）**：真实库 6,076 个事件 / 885 条链，串行 20.1–21.0 s；4 个工作进程 8.1 / 9.7 / 9.4 s（均值 9.1 s = 串行的 45%），8 个工作进程 7.9 / 9.0 / 8.1 s（均值 8.3 s = 40%）；6 次对账全部逐项相同，校验模式（`store.replay()` 并行结果再与串行比较）31.8 / 31.1 s 一致；stage 1 见 DEVX-022 第 17.8 节；围栏命令的实测并入 P6(a) |
| P6 | 后续小候选：分两步评估开启范围——(a) S3 命令与验证驱动的子进程环境（围栏命令、测试进程；命名 DQ 的受限子进程不开），(b) local-publish worker 与 git 钩子（计数器显示 local-publish 的 63 分钟里 python 平均只有约 1 个核）；每一步测一次完整链 | P5 | 完整链耗时对照 DEVX-022 第 15.1 节；开启范围由已评审的配置值给出，不靠环境里的临时变量 |

P2 的测试要求：(1) 在多链夹具库上并行与串行的 `LeaseReplay` 逐项相等（含带外置行表与执行的链，使 pickle 与重组被覆盖）；(2) 变异差分：篡改一个字节、截断文件、删除链中间的事件、增加重复事件、用另一个合法事件替换、事件的 `lease_id` 与目录名不符（并行必须回退串行且结论相同）——每种变异下并行与串行的结论（含 issues）相同；
(3) 回退：池创建失败、工作进程崩溃、任务超时、反序列化失败 → 返回串行的结论；(4) 开关解析：未设/`off`/`1`/无法解析 → 不创建池（用探针断言）；`auto` 与整数的进程数与上限；小于最小事件数 → 串行；(5) `ExternalizedRows` 的 pickle 往返：摘要保留、仍不可变、深拷贝行为不变；
(6) 校验模式：注入一个有缺陷的并行汇总，断言抛 `LEASE_PARALLEL_REPLAY_MISMATCH`；(7) `__main__` 守卫检查：无守卫的脚本回退串行；(8) 真实库只读对账脚本（不进 CI，进候选证据）：并行与串行逐项相同。

### 12.4 风险与开放问题
- **spawn 与调用方的 `__main__`**：见 12.2 第 1 点的守卫检查；pytest 与 xdist 工作进程的 `__main__` 没有可重跑的脚本。
- **Full 里的进程数超订**：功能默认关闭，Full 里只有差分测试自己用 2 个工作进程；开启后的全局上限留给 P6 评估。
- **git 钩子与 job object 里启动子进程**：P4 的诊断运行与 P6 的完整链会实测；任何异常都回退串行，不影响正确性。
- **与封印 S 的关系**：并行约 2.3×、不改信任边界；封印约 4× 但要改「不落盘已校验标记」的边界，评审前不实现，作为并行不够时的后备。
- **首次诊断的发现（2026-10-08，P4）**：开启开关后 stage 1 的两个测试 164 s 就失败了：`NAMED_BOOTSTRAP_UNREVIEWED_IMPORT: ai_trading_system.platform.architecture.lease_parallel_replay`（命名 DQ 的受限子进程只准导入评审过的模块，开关又从父进程继承到了子进程）。这暴露两件事：(1) 内核钩子只保护了辅助模块内部的异常，没有保护「导入它」这一步，违背了「任何异常回退串行」的不变量，已修复（导入失败一律串行，并加测试）；(2) **受限子进程按设计不应开进程池**——工作进程是不经 captured loader 的新解释器，结果却会被受限子进程当作权威使用，这会削弱子进程「只运行评审过的提交代码」的隔离；所以这些子进程保持串行。代价：stage 1 的重放里约一半（子进程内约 17 次）得不到加速；测试进程内的 14 次重放实测 21.4 → 8.9 s（4 个工作进程，2.4×），stage 1 总耗时预计约 785 → 600–620 s（−22%），不是原先估算的 330–400 s。能同时加速子进程的是封印 S（同一份被评审的代码内，不开进程）。围栏命令与 git 钩子等未受限的上下文的收益留给 P6 实测。
- 开放问题（owner）：无阻塞项；默认进程数与上限是试点基线，P5/P6 实测后校准。

### 12.5 进度
- 2026-10-08：owner 选择先做并行重放再做 C3；本节与任务行登记（P0）。
- 2026-10-08：**P1、P2 已完成（候选尚未冻结）**。新模块 `lease_parallel_replay.py`（开关解析、链级进程池、回退、校验模式、`__main__` 守卫检查）；内核三处小改动：`_assemble_lease_replay`（串行与并行共用的汇总）、`ExternalizedRows.__reduce__`、`_replay_uncached` 的默认关闭钩子（原主体逐字不变，改名 `_replay_serial`）。测试全是新文件：`test_arch_005_lease_parallel_replay.py`（35 项，进程内）与 `test_arch_005_lease_parallel_replay_store.py`（14 项，真实进程池，对 11 种磁盘变异——篡改一个字节、截断、删中间事件、重复事件文件、垃圾 JSON、非 JSON 文件、空链目录、事件内容互换、两个 ACTIVE 租约重叠、链分叉、事件放进别的链的目录——逐项对账串行与并行）；默认关闭时既有的内核/外置/调度/仲裁测试（80 项）全部不变。真实库只读对账（`claude_p1_real_store_diff.log`，5,982 个事件 / 878 条链）：串行 18.3 s，4 个工作进程 8.3–9.2 s（三次），结果逐项相同，校验模式下 `store.replay()` 一致。ruff、mypy strict 通过（内核文件本身不是 black 风格，所以没有对它跑 black，以免产生大面积无关改动）。下一步 P3：生成器链与权威接管，然后 P4 候选链（用 S3 命令）。
- 2026-10-08：**P4 首次诊断（S3 命令的 run `p-20261008-v2` 暂停在 `C26.readiness` 后，开关开启 4 个工作进程跑 stage 1）**：两个测试在 164 s 失败（受限子进程拒绝导入新模块，见 12.4）；测试进程内 10 次重放平均 8.92 s（串行 21.43 s），证明构造与序列化不变。已修复钩子（导入失败串行回退）并补测试，随后用新 run 重做候选链；估算与验收按 12.3 / 12.4 修订。
- 2026-10-08：**P3、P4 完成，已随第七次发布发布**（候选 `5150efbac`，S3 命令 run `p-20261008-v3`，owner 在终端推送，普通推送 `0834ed957..5150efbac`）。Full 15,027 通过 / 4 跳过 / 0 失败；开关默认关闭，常驻 Full 的是 36 + 14 项差分与变异测试。
  诊断运行（开关开、4 个工作进程、活的 `FORMAL_VALIDATION_PRE` 事务、冻结驱动之前）：stage 1 **568.7 s**；测试进程内 14 次重放均值 8.20 s（串行 21.43 s），共 114.8 s（串行 300.0 s）；其余约 419 s 在受限子进程里（串行）。同一候选冻结驱动里（关）的 stage 1 是 718.6 s，所以开启的收益是 −21%（对 C2 窗口的关闭诊断运行 761.1 s 是 −25%）。
- 2026-10-08：**发布链上的新证据**（DEVX-022 第 17.8 节）：(1) 逐进程计数器：stage 1 只有约 1.2 个 python 核（机器约 88% 空闲），`local-publish` 的 63 分钟里 python 平均约 1 个核——这两段都是串行重放；
  (2) Full 的最慢节点仍是 composer 激活测试（1,985.9 s，重放主导）；(3) Full 的墙钟由命名 DQ 候选文件（单个 loadfile 单元，6,933 s）因调度种子过期而晚起跑决定，与本任务无关（DEVX-022 登记了种子刷新）。
- 2026-10-08：**P5/P6 计划修订**：P6 分 (a)(b) 两步（见 12.3）。(b) 的预期收益最大：`local-publish` 的 63 分钟占整条链的 22.5%，若其中重放占约 85% 且按测试进程内的 2.6 倍加速，上限约省 30 分钟——这是推测，待实测。
  开启范围必须由已评审的配置值给出；命名 DQ 的受限子进程保持串行不变（隔离边界）。
- 2026-10-08：**P5（重放部分）完成**——发布后的代码在真实库上重测：串行 20.14 s（6,076 个事件 / 885 条链）；4 个工作进程 8.09 / 9.72 / 9.43 s、8 个工作进程 7.86 / 8.98 / 8.12 s（串行 20.95 s）；6 次对账全部 `identical=True`、无回退，校验模式 31.82 / 31.05 s 一致。
  4 个工作进程的均值恰好在验收线 45% 上，8 个工作进程更稳（40%）；下限仍是两条历史大链。单次串行重放相对发布前（5,926 个事件、20.4–21.0 s）没有增长（20.1–21.0 s），与 S3c/W 之后的预期一致。
- 2026-10-08（夜）：**第八次发布（C3a）的实测，P6 升为下一个候选**（数据与归因见 DEVX-022 第 17.10 节）。
  - 真实库 6,210 个事件 / 893 条链 / 728 MB。逐链串行重放合计 26.0 s：两条历史大链 5.77 + 4.41 s；每条发布链（47–63 个事件）1.2–2.8 s，永久计入每次重放；自第七次发布后的测量（20.14 s / 6,076 个事件）以来新增 9 条链 / 138 个事件，合计 +2.7 s。晚间重测 `store._replay_serial()` 25.2–32.0 s（安静时约 25–26 s，owner 的交易软件与 Defender 在后台时到 31 s），需要夜里再测一次校准。
  - stage 1 873.6 s（v3 718.6 s，+21.6%）、`local-publish` 82 分钟（v3 63 分钟，+30%）、命名 DQ 候选文件 ×1.2：全部是重放主导的工作（Full 里最慢的节点 composer 激活测试 +28% 同向）；stage 1 之外的各阶段与 v3 持平或更快。所以**每一次发布尝试（包括被放弃的）都让之后的每次重放永久变慢约 1.2–2.8 s**，下一次发布的 stage 1 预计越过 900 s 告警线。
  - P6 的目标阶段（时间线见 DEVX-022 第 17.10 节）：(a) S3 命令与验证驱动的子进程环境（stage 1 约 32 次重放；Full 里的重放主导节点，例如 composer 激活测试 v6 为 2,542.8 s，v3 为 1,985.9 s）；(b) `local-publish` worker 与 git 钩子（钩子目录创建前 21 分钟、`merge_resumed` 前 25 分钟、worker 结束后 7 分钟收养，全是重放；引用事务持锁窗口 2 分 46 秒也在其中）。命名 DQ 的受限子进程保持串行（12.4）。开启范围由已评审的配置值给出。
  - 封印 S（第 10 节）仍是唯一能停止增长的方案，等待 owner 对「不落盘已校验标记」边界的决定；并行只把常数除以约 2.3，增长率不变。

### 12.6 P6 实施计划与进度（2026-10-08 夜；依据 DEVX-022 第 17.10 节的实测）

**为什么现在做**：重放主导的工作随租约库增长变慢（stage 1 +21.6%、`local-publish` +30%、composer 激活测试 +28%），每一次发布尝试（含被放弃的）让之后的每次串行重放永久变慢约 1.2–2.8 s，下一次发布的 stage 1 预计越过 900 s 告警线。并行不改信任边界，能立刻把常数除以约 2.5–3（真实库、安静时：串行 26.0 s → 4 个工作进程 10.2–11.3 s，8 个 9.9–10.2 s，结果逐项相同）；封印 S（第 10 节）仍是唯一能停止增长的方案，待 owner 对信任模型的决定。

**开启范围**（P6 的核心决定；每一行是已评审配置 `config/architecture/devx_023_parallel_replay_scope.v1.yaml` 里的一个值）：

| 角色 | 进程 | 状态 | 理由 |
|---|---|---|---|
| `s3_command` | 发布命令本身及其围栏 / 检查点 / 生成器 / 收尾子进程 | 开 | E52 / E56 各 235–273 s，A–C 的生成器与审计都在重放 |
| `validation_stage_1` | 验证驱动的 named-parent-positive 阶段（约 32 次重放，873.6 s） | 开 | 唯一占 stage 时间大头的重放消费者；经驱动参数 `--stage-1-parallel-replay-workers` 只加到该阶段的环境，驱动进程本身不带开关 |
| `local_publish_worker` | `local-publish` worker 与它的 git 钩子 | 开 | 82 分钟里重放占大头；持锁窗口（2 分 46 秒）里的钩子也在其中；经启动器对这一次启动的环境 |
| `validation_pre_full_tiers` | contract-validation / integration / reproducibility / architecture-fitness | **关** | 这些 tier 的 pytest 跑 16 个 xdist worker，每个再开进程池会超订；测量之后再评估 |
| `formal_full` | 正式 Full（pytest 与 xdist 工作进程） | **关（never_enabled）** | owner 2026-10-05 选择 C：不改正式 Full 的合同与环境 |
| `named_dq_restricted_children` | 命名 DQ 的受限子进程 | **关（never_enabled）** | 12.4：隔离边界，子进程只运行评审过的提交代码 |

**机制与不变量**：
1. 模块 `parallel_replay_scope.py` 读取并校验配置（严格 YAML：重复键 / 非有限数视为无效；缺失、不可读、格式错误、状态不在 `PILOT_BASELINE` / `OWNER_APPROVED`、`workers` 不在 2–8、角色集合不等于六个已知角色、任一 `never_enabled` 角色被设为 enabled → 整份策略无效，什么也不开）；`environment_for(role)` 只在策略生效且该角色开启时给出 `AITS_LEASE_PARALLEL_REPLAY=<workers>`。变量的值只来自配置：从调用 shell 继承来的值被丢弃（`without_switch`），发布脚本对自己的进程也只按角色设置（`apply_switch`）。
2. 接线：`publication_cli.default_environment` 载入范围并把 `s3_command` 的变量放进子进程环境；`PublishConfig.parallel_replay` 给 D30（驱动命令行追加 `--stage-1-parallel-replay-workers N`）与 E53（`WmiDetachedLauncher.launch(environment=...)` 只对这一次启动生效）；验证驱动的 `ValidationConfig.stage_environment` 只给 `named-parent-positive`，并把附加变量写进该阶段的结果（`environment_additions`）；`run.json` 记录启动时的范围快照（状态、版本、worker 数、开启角色、策略文件哈希）。
3. 默认行为不变：配置缺失或未生效 = 与现状逐字相同；任何异常仍回退串行（P1）；不持久化、不跨调用缓存、不改围栏 / 租约语义；并行结果与串行逐项相同（差分与变异测试已常驻 Full）。
4. 启发式治理（AGENTS.md）：worker 数 4 与上限 8 是性能参数，状态 `PILOT_BASELINE`，退出条件 = 用 P6.4 那条链的实测校准后换成带证据的 `OWNER_APPROVED` 版本，最迟 2027-01-08；各角色开关改一个值即可关掉，不需要回滚代码。

**测试**（`tests/test_arch_005_parallel_replay_scope.py` 26 项 + 接线测试）：真实策略有效且生效、六个角色与 never_enabled 不变量；十五类无效策略各给出预期的问题码且什么也不开；缺失 / 非 YAML / 非 UTF-8 / 重复键；四种状态；`without_switch` / `apply_switch`（继承值不会胜出、未开启的角色没有开关）；与 `lease_parallel_replay` 的常量一致；S3 运行（`test_arch_005_publication_run.py`）：开启时 D30 带参数而驱动环境为空、E53 带环境并记入 facts，未开启 / 只开一部分角色时对应进程什么也没有；驱动（`test_arch_005_publication_validation.py`）：附加环境只到 stage 1、Full 与四个 tier 没有、共享环境不被污染；启动器（`test_arch_005_publication_services.py`）：环境只对这一次启动生效、不安全的值被拒；CLI：`run.json` 快照、真实接线只从策略取值。

**隔离演练**（2026-10-08 夜，真实租约库 6,214 个事件，临时仓库与钩子，演练脚本在会话 scratchpad，目录已删除）：隐藏的 WMI 启动 → cmd → worker → `git merge --ff-only` → sh 钩子 → python 钩子里重放真实库。串行对照：每次钩子里的重放 26.0–26.3 s，合并全程 187 s；开关 = 4：每次 10.2–10.8 s，**无回退**（`LAST_FALLBACK_REASON` = None），状态 PASS，合并全程 77 s（2.4×）。这覆盖了钩子 / 无控制台 / Job 对象（只有 `KILL_ON_JOB_CLOSE`，没有活动进程数限制）这些担心点中的前两个；Job 的进程数限制已读码确认不存在。

**测量与验收**（候选链用候选的代码，所以在同一条链里直接量）：stage 1 ≤ 650 s（现 873.6 s）；E52 / E56 各降 ≥ 25%；E54 ≤ 60 分钟（现 82 分钟；演练给出的乐观估计约 40–45 分钟）；journal 无 FAILED；Full 的 pytest 时长不变（±3%，因为 Full 里没开）。**回滚**：任一阶段比串行慢或出现回退以外的异常 → 把对应角色关掉（配置改一个值）。

| 步骤 | 内容 | 状态 |
|---|---|---|
| P6.0 | 登记（本节 + 任务行） | 完成（任务行 `6ce7a54b1`） |
| P6.1 | 配置 + `parallel_replay_scope.py` + 测试 | 完成（`54e039b26`） |
| P6.2 | 接线（`publication_cli`、`publish.py`、启动器、驱动 `stage_environment`）+ 测试 | 完成（`54e039b26`） |
| P6.3 | 文档：`docs/system_flow.md` 一段并重算封印；DEVX-022 第 15.2 节一行；本节 | 完成（`54e039b26`） |
| P6.4 | 候选链（S3 命令）并测量 | 完成（run `p-20261008-v7`，候选 `a63827b28` 已发布；结果见 12.6.1） |
| P6.5 | 结果写回 DEVX-022 / DEVX-023 / 任务行；校准 worker 数，评估 `validation_pre_full_tiers` | 部分完成：结果已写回；`workers: 4` 保持（4 与 8 在真实库上差别在噪声内）；`validation_pre_full_tiers` 暂不开，等测量 |

**风险**：(1) 钩子 / worker 里的进程池 spawn：`__main__` 守卫检查（仓库里调用重放的脚本都有守卫，没有守卫的脚本回退串行——这也是我用无守卫的临时脚本测不出加速的原因）；(2) 进程超订：`local-publish` 期间没有别的重负载，Full 与 xdist 的 tier 不开；(3) 每次重放的建池启动成本约 2 s：stage 1 的 P4 诊断已测得净收益 −21%（568.7 对 718.6 s），钩子演练净收益 2.5×；(4) 配置的治理：`PILOT_BASELINE` 的退出条件见上。

#### 12.6.1 P6.4 的实测结果（2026-10-09，S3 run `p-20261008-v7`，候选 `a63827b28`；完整对照表在 DEVX-022 第 17.11 节）

| 验收项（12.6） | 目标 | 实测 | 结论 |
|---|---|---|---|
| E52 / E56 | 各降 ≥ 25% | 148.0 s（−37%）/ 168.1 s（−39%） | 达成 |
| E54 `local-publish` | ≤ 60 分钟 | 2,328.6 s = 38.8 分钟（v6 82.1 分钟，−52.7%） | 达成 |
| journal 无 FAILED | 无 | 无 FAILED，`slow_steps` 为空 | 达成 |
| stage 1 | ≤ 650 s | 744.8 s（v6 873.6 s，−14.7%） | **未达成**：约七成重放在按设计保持串行的受限子进程里，目标估高了 |
| Full 的 pytest | 不变（±3%） | 9,676 s，+4.6% | **未达成，与 P6 无关**：Full 的角色是 `never_enabled`；是租约库增长的重放成本（composer 激活测试 +5.2%，命名 DQ 候选文件 +4.7%） |

- 额外收益：A–C 782 → 607.8 s（−22%，生成器 / governed preflight 等子进程在重放）；E50 / E57 / E61 / E62 各降 45–57%；整条链 5 小时 20 分 → 4 小时 30 分（−16%）。
- 回退与故障：没有。`local-publish` worker 里的进程池在隐藏的 WMI → worker → git 钩子 → python 钩子这条真实进程链里正常工作（演练的结论在真实运行里重现），`LAST_FALLBACK_REASON` 没有触发过失败路径，journal 无 FAILED。
- 局限：P6 压的是链里「发布命令 + `local-publish`」两段（约两成）；占约八成的 Full 与受限子进程不在范围内（12.4、owner 2026-10-05 选择 C）。`PILOT_BASELINE` 的校准结论：4 个工作进程在这条链上足够，仍保持试点状态，退出条件不变（换成带证据的 `OWNER_APPROVED` 版本，最迟 2027-01-08）。
- 下一步候选（按价值排序，未启动）：封印 S（唯一能停止增长，待 owner 决定）；DEVX-022 M6 拆开命名 DQ 候选文件（最多 −27 分钟 Full）；测量四个 xdist tier 的重放占比后决定是否开 `validation_pre_full_tiers`。
