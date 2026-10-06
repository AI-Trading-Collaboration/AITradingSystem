# DEVX-023：租约重放成本与发布耗时增长（S3c + W 先行）

最后更新：2026-10-06

稳定任务 ID：`DEVX-023_LEASE_REPLAY_COST_REDUCTION`

状态：`IN_PROGRESS`（2026-10-06 owner 选择「S3c + W 先做」；封印 S 留待观察一次发布后再决定）

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
- 兼容性：schema 仍是 `execution_lease_event.v3` / `execution_lease.v3`，「位置集合」属于 v3 的定义。历史事件（内嵌 / S3b 形式）原样可读；S3b 时代的旧代码读到 S3c 的 marker 会 fail closed（不会误读）。**单一检出里新旧代码不会同时写库**，不需要双向兼容。
- 预期：晚期事件 ~0.95 MB → 约 20 KB；每次发布留给库的事件字节 11.8 MiB → 不到 1 MiB；blob 每次发布新增约 10 MB（`read_file_custodies`，内容随候选变化）与约 0.3 MB（`captures`），每个**不同**的 blob 在一次重放里只读一次（约 0.15 s），所以逐发布增长约 +0.2 s 而不是 +3 s。
- 诚实边界：S3c 不改变历史事件，现有的约 40 个大事件产生的 ~5 s 解码/哈希成本保留。

### 4.3 W：同一原子段内显式传递 replay（不引入隐式缓存）

- 持锁段（`store.atomic(...)`）里，在 `_head` 取得的那一份 replay 之后、`_append_event` 之前，store 不会变化。`ExecutionLifecycle._head`、`PublicationLifecycle._head`/`_append`、`_require_original_publication`
  新增可选参数 `replay`（默认 `None` = 与今天完全相同）；持锁段内的调用者把第一次 `_head` 得到的 replay 往下传，`_append` 用它取前一个事件 ID 与物理 head，而不是再重放两次。
- 写入之后（`_append_event` 返回）该 replay 即作废：`_append` 返回后调用者不得复用。
- 围栏自己的重放（`fence.validate_publication_merge_window` 内的 `guard.replay()`）属于另一个 store 实例，**本任务不共享**（共享需要进程级隐式缓存，风险与评审成本都更高）；若实测收益不足再单独评审「W2：按根路径与代数号的隐式持锁段缓存」。
- 预期：一次 `record_publication_hook` 的重放数 9 → 6–7（持锁段内 5 → 2–3）。
- 不变量：默认路径不变；传入的 replay 必须来自同一个 store 实例且 `status == "PASS"`；`_append` 在使用传入 replay 之前仍校验 head 的 `lease_id`、`actor`、事件 ID 与物理 head 一致（等价于今天的重放结论）。

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
| D4 | W：显式传递 replay | D2 | 单元测试：持锁段内重放次数的上界（计数）；传入 replay 与重新重放结论相同；写入后复用被拒绝；默认路径行为不变；重型端到端（`*-full-profile-publish`）全部通过 |
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
- W 的健全性：只在持锁段内、显式传递、写入后作废；围栏自己的重放不共享；默认路径不变。
- 旧代码与新事件：S3b 代码读 S3c marker 会 fail closed；本检出内不会混用两套代码。

## 8. 开放问题与后续项

1. S3c + W + S3d 之后若发布耗时仍不够，是否做封印 S（owner 决定，按当时的实测）。
2. 命名 DQ 父证明不再内嵌整库重放（改为只绑定重放摘要与来源租约）：证明合同变更，随下一次合同波次评审。
3. W2（隐式持锁段缓存）：仅在 W 实测收益不足时评审。
4. `local-publish` 的 worker 墙钟（10,800 s）与 git 子进程等待（7,200 s）随选定方案复核。

## 9. 进度

- 2026-10-06：登记并转 `IN_PROGRESS`（任务行事务 `gov-007-devx023-start-task-20261006-v1`）；本文档为第一版。D1 基线数字在 DEVX-022 第 17 节（重放 15.9–17.1 s、`_validate_hook_ready` 69 次 7.3 s、`parse_lease_event` 14.2 s）。
