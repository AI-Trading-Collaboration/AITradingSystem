# DEVX-006C：Compatibility Authority Fragmentation

最后更新：2026-08-09

稳定任务 ID：`DEVX-006C_COMPATIBILITY_AUTHORITY_FRAGMENTATION`

优先级：`P1`

状态：`BASELINE_DONE`

计划模式：`SINGLE_LANE`

合同变更：`true`

Owner 决定：
`owner_decision:DEVX-006C:2026-08-09:authorize_c_then_d_then_s5_serial_v1`

## 1. 背景与目标

DEVX-006 B 波已经把 task shadow 改为 stable task-id fragments，但
`inputs/architecture/arch_004_compatibility_baseline.yaml` 仍是多 MB append-only monolith。
每次新增 current-hash authority 都会要求所有任务争用同一中央文件和中央测试，形成真实的
shared-writer 冲突与大范围 dirty-path amplification。

Project owner 已要求按以下顺序真正解决共享路径问题：

1. DEVX-006 C：compatibility authority fragmentation；
2. DEVX-006 D：report/catalog/flow lossless fragmentation；
3. ARCH-005 S5：task source canonical cutover。

本任务只实施第 1 步。D 波必须从 C 波 ordinary push/cleanup/resource release 后的新 exact
`main` 独立登记；S5 必须再等待 D 波完成。不得把顺序授权解释为三个 cutover 同时进行。

## 2. Exact base 与前置条件

- TRADING-2504 已由原单一 coordinator 完成 ordinary push、cleanup 和 resource release；
- exact start base：`cb437a4d4be178180f60cb3ee2d2994c1be45f94`；
- start 时 `local main = origin/main = exact base`；
- start 时 governed audit clean、active lease=0、formal runner=0；
- TRADING-2504 的 formal evidence 不作为本任务 evidence，本任务所有验证从 exact 新 base 重跑；
- known-unrelated `docs/research/growth_tilt_owner_diagnosis_pack.md` 继续由 exact exclusion
  保护，本任务不得读取、修改、stage、提交或清理其内容。

## 3. 权威冻结与目标合同

### 3.1 Legacy prefix freeze

- 冻结 start base 上现有 compatibility monolith 的 exact Git blob、LF bytes、SHA-256、section
  顺序和语义；
- C 波不得重写、重排、压缩、重新格式化或删除 legacy sections；
- C 波完成后，legacy monolith 只作为 immutable prefix，不再接受新 section append；
- 历史 tests、requirements 与 artifacts 仍可通过 merged loader 重放同一 section mapping/order/hash。

Exact legacy seal（start base）：

- raw/LF byte count：`3151107`；
- file/LF SHA-256：`253b976b2740f0097e1d8949ec8eaf3846c82809f88fdf3a47fb76fae6023842`；
- Git blob：`12496b5578207855e5e4dd81159f80a5d0e8a3bb`；
- top-level ordered entry count：`306`；ordered-entry-id SHA-256：
  `81240fcd54624e808478267b62612a33b2c4f05a336a216ddf4208f9837e101b`；
- mapping replay SHA-256：
  `6b575efd51d511ac8a42d271a7e4ceba0b98b4ff73eafec7234ae95ba059c9cf`；
- legacy 内存在 1 个冻结的 nested duplicate key（
  `phase_g2_4br_etf_cli_dynamic_v3_backtest_sim_calibration.superseded_source_paths`）。
  该既有字节只在 exact hash 验证后按历史 PyYAML last-value-wins 语义重放；所有新 policy、
  index 与 fragment 仍严格拒绝 duplicate key，不把 legacy 例外扩展为新写入许可。

### 3.2 Fragment contract

- 每个新 section 使用独立 canonical fragment；
- fragment identity 由 canonical section content 计算，不得包含 wall-clock、checkout path、line
  number 或 mutable partition；
- fragment 路径必须 content-addressed，missing/duplicate/hash mismatch/path escape/symlink/unknown
  field 一律 fail closed；
- fragment 不得复制完整 legacy monolith，也不得建立第二套可人工编辑的事实源。

### 3.3 Hash-chain index

- 小型 index 记录 schema、legacy prefix path/hash、ordered fragment entries、section id、fragment
  path/hash、previous entry hash、entry hash、order 与 final chain hash；
- index canonical serialization 必须确定性；输入文件枚举顺序不得改变 bytes/hash；
- duplicate section id、duplicate fragment path、broken previous hash、reorder、missing fragment、content
  tamper、legacy prefix drift 均 fail closed；
- index 只允许引用 repository-root-contained tracked regular files。

### 3.4 Merged loader 与 source cutover

- canonical loader 同时读取 frozen legacy prefix + ordered fragments；
- loader 输出必须保留 legacy mapping/order，并在尾部追加 fragments；
- 在 parity、consumer inventory、direct-reader migration 和 rollback 演练全部 PASS 前，
  `source_cutover_allowed=false`；
- cutover 原子提交完成后，新 section 只写 fragment + 小型 index，禁止继续向 legacy monolith
  追加，禁止 dual write；
- rollback 只能关闭 fragment adoption 并回到冻结 legacy prefix；不得把新 fragment 静默写回
  monolith 或丢弃已产生的 section。

## 4. Consumer migration

C1 必须从 exact task base 动态生成全部 direct consumer inventory，不沿用历史硬编码计数。每个
consumer 至少记录：

- exact path、role、读取方式与引用次数；
- 是否需要 ordered mapping、raw legacy bytes、current-hash lookup 或 append-only prefix assertion；
- migration adapter、validation 与 rollback；
- migration status 与 remaining direct-read reason。

所有 runtime/script/test consumer 必须改为调用同一 canonical loader 或明确的 immutable-prefix
reader。source cutover 前，直接把 monolith 当作可增长 current authority 的 consumer 必须为零。

## 5. 实施阶段

### C0：登记与合同冻结

- 新增本任务行与 supporting requirement；
- 记录 Owner 顺序授权、exact released base、路径 claims、生产边界与退出条件；
- governed `SINGLE_LANE` preflight PASS 后才允许 implementation write。

### C1：动态 inventory 与 legacy seal

- 生成 exact consumer inventory；
- 冻结 legacy Git blob/LF/file SHA、section count、ordered section IDs 与 replay checksum；
- 记录所有当前 append/current-hash authority 写入点。

### C2：fragment/index/loader 合同

- 实现 strict models、canonical serialization、content-addressed fragments、hash-chain index 和
  merged loader；
- 建立 deterministic build/validate CLI；
- 完成 positive、tamper、duplicate、reorder、missing、path/symlink 与 repeat-build tests。

### C3：consumer migration 与 cutover rehearsal

- 迁移全部 direct consumers；
- 对 frozen legacy-only、legacy+one fragment、legacy+multi-fragment 三种状态证明 mapping/order/hash
  可重放；
- 证明新 section 只改变一个 fragment 与 index，不改变 legacy bytes 或既有 fragment bytes；
- 进行 rollback rehearsal，证明无 section 丢失。

### C4：final cutover 与治理写回

- 以本任务首个真实 fragment 表达 C 波后的新 compatibility section；
- 冻结 `legacy_append_allowed=false`、`fragment_source_active=true`、`dual_write=false`；
- 更新 task register、parent requirement、generated manifests/shadows 与必要 architecture policy；
- 本任务不改变 `docs/system_flow.md`，因为它只改变研发 compatibility authority 的存储与读取，
  不改变投资系统从数据输入到结论的数据流。

### C5：验证与收口

- focused pytest 使用 16 workers / `loadfile`；
- Ruff、strict mypy、compileall PASS；
- Architecture、Contract、Integration、Reproducibility 与唯一自然边界 Full 在 final tree PASS；
- task commit、local-main fast-forward、ordinary push、SHA equality、governed audit 与 branch cleanup
  PASS 后才 release C 波；
- D 波从 C 波 released exact main 重新登记和 preflight，不复用本任务 formal evidence。

## 6. 计划路径与 ownership

Task-owned implementation 计划包括：

- `config/architecture/devx_006c_compatibility_authority.yaml`；
- `src/ai_trading_system/platform/architecture/compatibility_authority.py`；
- `scripts/architecture_compatibility_authority.py`；
- `tests/test_devx_006c_compatibility_authority.py`；
- C1 inventory 确认的 direct consumer files。

Coordinator-only/generated 计划包括：

- `docs/task_register.md`；
- 本 requirement 与 DEVX-006 parent requirement；
- `registry/architecture_compatibility_authority/**`；
- `inputs/architecture/devx_006c_compatibility_authority_index.json`；
- `inputs/architecture/devx_006c_compatibility_consumer_inventory.json`；
- module/test manifests、architecture fitness、task shadow 与 formal runtime artifacts。

对 C1 尚未发现的 path 不做推测性 broad claim；新增 consumer path 必须先更新本文 inventory/claims
并重新运行 preflight。

## 7. 验收标准

- legacy monolith exact bytes/SHA 与 start base 一致；
- legacy section mapping/order/hash byte-for-byte 可重放；
- fragment/index canonical bytes repeat-build 一致；
- 新 section 不修改 legacy 或既有 fragment bytes；
- loader 对 legacy + fragments 完整重放，全部 tamper/path/chain failure typed fail closed；
- legacy append writer 与 growth-assuming direct consumer 为零；
- rollback rehearsal 无 section 丢失；
- no dual write、no second editable authority、no silent fallback；
- focused/static/formal/Full 与 governed closeout PASS；
- `production_effect=none`、`broker_action=none`；不改变数据、DQ/PIT、策略阈值、scoring、backtest、
  weights、paper/live 或投资结论。

## 8. 工作区生命周期

- task branch：`codex/devx-006c-compatibility-authority-fragmentation`；
- 本任务不创建额外 repository worktree、clone 或 external cache；
- 既有其他 worktree 不属于本任务，不读取、不修改、不清理；
- closeout 后仅在 exact ancestry、canonical evidence、tracked/untracked/ignored audit、runner=0 与
  recoverability 都确认后删除已合并分支；Git 历史和 remote main 提供恢复边界。

## 9. 当前进度

- 2026-08-09：Owner 明确要求按 C → D → S5 顺序推进；TRADING-2504 coordinator 已回传
  `main=origin/main=cb437a4d4be178180f60cb3ee2d2994c1be45f94`、clean、runner=0、resource
  RELEASE；DEVX-006C 建立并进入 `IN_PROGRESS`。
- 2026-08-09：C1 dynamic inventory 从 exact base 与当前 checkout 双向扫描完成：8 个 consumer；
  base growth-assuming direct reads=`135`（中央 compatibility test 133、TRADING-2452 test 2），
  cutover 后=`0`；保留的 raw reads 仅用于 immutable historical-prefix byte assertions；
  runtime legacy append writer=`0`，唯一 writer 是 temp test fixture。Bootstrap handoff 已显式改用
  immutable-prefix bytes reader；current authority tests 已统一改用 merged loader。
- 2026-08-09：C2/C3 已建立 canonical JSON content-addressed fragment、SHA-256 hash-chain index、
  strict path/symlink/duplicate/unknown-field/tamper/missing/reorder gates、deterministic builder/validator
  与 rollback-only legacy view。首轮 focused=`33 passed`；完整 compatibility 合同首轮暴露旧
  TRADING-2504 latest-authority 硬编码并直接迁移，第二轮仅剩 task-shadow carry-forward 合同 1 项，
  修正后定向 PASS；最终全量/静态/formal 结果待 final-tree 验证写回。
- 2026-08-09：最终完整 compatibility contract=`209 passed`；legacy-only 306 项与 merged 307 项
  顺序、TRADING-2504 历史 authority、DEVX-006 task-shadow authority carry-forward 全部 PASS。
  实现状态转为 `BASELINE_DONE`，进入 final-tree static/formal/Full 与 governed closeout。
