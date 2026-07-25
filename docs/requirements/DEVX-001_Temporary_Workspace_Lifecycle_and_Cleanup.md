# DEVX-001 临时工作区生命周期与清理

稳定任务 ID：`DEVX-001_TEMPORARY_WORKSPACE_LIFECYCLE_AND_CLEANUP`

## 1. 背景与目标

2026-07-23 至 2026-07-24 的 Wave14、OPS-067 和验证工作在 `D:\`、
`D:\Work` 下创建了多批 Git worktree、本地 clone、外置 mypy cache 和验证目录。
隔离工作区有助于 exact-tree、clean-clone 和并行验证，但部分已完成或已放弃的目录未在
closeout 时清理，造成空间占用、状态误读和未审计修改被误删的风险。

本任务的目标是：

1. 只清理能够证明已完成、已放弃或纯缓存性质的目录；
2. 保留仍在处理、含未审计修改、含尚未归档证据或用途不明确的目录；
3. 把临时目录的创建、归属、退出条件和清理责任写入项目工程规则；
4. 清理后验证 Git worktree 元数据与主工作区状态一致。

本任务不改变 CLI、数据流、缓存 schema、评分、回测、报告或投资结论，因此不更新
`docs/system_flow.md`。

## 2. 清理边界

清理对象按以下类别审计：

- Git worktree：先检查 `git status`、HEAD、包含该提交的正式 ref 及关联任务状态；
  使用 `git worktree remove`，不得只从文件管理器删除目录；
- 本地 clone：检查 tracked/untracked/ignored 内容、HEAD 是否已进入正式历史及关联任务状态；
- cache：确认是可重建的 mypy/pytest/ruff 等缓存，并确认没有活动进程引用；
- supervised run：遵循其 reviewed clean-only cleanup 合同，不删除未审计变更或仍需保留的运行证据。

以下情况必须保留并记录原因：

- tracked 或 untracked 修改尚未审计；
- 任务仍处于 `IN_PROGRESS`、`VALIDATING` 或等待 owner acceptance，且目录仍是当前证据来源；
- commit、artifact 或运行状态尚未进入 canonical 保存位置；
- 无法证明目录归属或可恢复性。

### 2.1 已合入 workspace 的统一删除依据

“分支或 HEAD 已合入”只是必要条件，不单独授权删除。后续审计必须复用本次 OPS-067
清理形成的判定：

1. 通过 commit ancestry 或 reviewed patch/PR equivalence 证明实现已进入正式主线；
2. required commit、失败证据、PASS 证据和 import/lineage proof 已进入 canonical governed
   location；immutable artifact 必须复核记录中的 SHA-256；
3. 审计 tracked、untracked 和 ignored 内容。与最新 `main` byte-different 的中间文件，
   如果已被后续 reviewed candidate 取代且不是 canonical evidence，可明确分类为
   `SUPERSEDED`，不因 dirty 状态永久保留；
4. 确认没有活动进程、scheduler entry、当前 validation 或 operational acceptance 依赖路径；
5. 使用显式绝对路径 allowlist 删除，并记录目录数量、文件数量、释放空间、保留证据和
   不可恢复内容。

只有 1～5 全部满足时才归类为 `REMOVE_SUPERSEDED`。任一项无法证明时继续使用
`KEEP_ACTIVE`、`KEEP_DIRTY` 或 `KEEP_UNCERTAIN`，并记录 owner 与退出条件。任务仍为
`IN_PROGRESS`/`VALIDATING` 并不自动要求保留 clone；关键是该路径是否仍是当前证据或执行来源。

## 3. 实施步骤与依赖

1. 建立包含绝对路径、目录类型、HEAD、Git 状态、任务状态和大小的只读清单。
2. 将清单分为 `REMOVE`、`KEEP_ACTIVE`、`KEEP_DIRTY`、`KEEP_UNCERTAIN`。
3. 删除纯缓存和已完成的 clean clone/worktree；先处理 Git 元数据，再删除普通目录。
4. 运行 `git worktree list --porcelain` 和 `git worktree prune`，确认没有 stale registration。
5. 在 `AGENTS.md` 增加临时工作区生命周期规则。
6. 更新本文件和 `docs/task_register.md`，记录实际删除、保留原因、验证与状态转换。

依赖：

- 不能覆盖当前主工作区中与 OPS-070 有关的在途修改；
- OPS-067/ENG-VAL-010 仍在验证或等待 operational acceptance 的目录，只有在证明不是当前
  evidence source 时才可清理；
- destructive cleanup 必须使用显式绝对路径 allowlist，并在执行前再次检查路径与状态。

## 4. 验收标准

- 已完成且无独立成果的临时目录被清理；
- 所有含未审计修改、活动任务证据或用途不明确的目录均保留；
- 删除前后目录数量与逻辑大小有记录；
- Git worktree registration 无 stale entry；
- 主仓库现有 OPS-070 修改 byte-for-byte 不被本任务覆盖；
- `AGENTS.md` 明确规定：创建临时目录时记录用途/owner/退出条件；提交成功、成果迁移完成或
  放弃开发后必须清理；若暂不能清理，必须在 task register 或 supporting document 记录路径、
  原因、风险、owner 和退出条件；
- 文档格式检查与 `git diff --check` 通过。

## 5. 开放问题

- OPS-067 的 11 个 dirty/candidate/formal/scan clone 已按 2.1 的统一依据完成审计和删除，
  不再是开放项；首次 FAIL 与最终 PASS Full evidence 已在主工作区保留同字节 canonical 副本。
- `D:\w14c_scanner_20260723_185121` 仍需单独审计：
  index 含 5,959 个 staged deletions，worktree 含 5,893 个对应 untracked 文件；其中
  `inputs/architecture/arch_004g_deprecation_inventory.yaml` 的内容既不等于该 snapshot HEAD，
  也不等于当前 `main`。关联的 `D:\w14c_commit_index_20260723_194551` 一并保留，避免在
  recovery 判断前丢失可能相关的 index 状态。

## 6. 状态记录

- 2026-07-25：项目 owner 要求清理已完成目录，并将临时目录在提交完成或放弃开发后的清理
  责任写入工程规则。任务登记为 `IN_PROGRESS`；已完成只读初查，尚未执行删除。
- 2026-07-25：完成显式 allowlist 清理并转 `BASELINE_DONE`。删除 29 个直接审计目标
  （15 个 Git worktree、2 个 local clone、12 个 mypy cache）及 3 个清空后的 supervised
  container，共扫描 115,531 个文件，释放约 3.22 GiB 逻辑空间。Git worktree 使用
  `git worktree remove`，随后 `git worktree prune`；清理后只保留主工作区和上述 dirty
  scanner worktree registration。

  已删除 worktree：

  - `D:\w14c_546282`
  - `D:\w14c_798490`
  - `D:\w14c_final_20260723`
  - `D:\w14c_formal_20260723_191754`
  - `D:\w14c_formal_20260723_193430`
  - `D:\w14c2_f0701164`
  - `D:\w14c2_post_3db0eb5d`
  - `D:\w14s01_7230754`
  - `D:\Work\AITradingSystem_wave14_validation`
  - `D:\Work\AITradingSystem-eb0-candidate`
  - `D:\Work\AITradingSystem\main`
  - `D:\Work\AITradingSystem-supervised-runs` 下两次已完成 run 的四个 clean lane worktree

  已删除 clone：

  - `D:\Work\AITradingSystem_wave14_c5gen_20260724`
  - `D:\Work\AITradingSystem_wave14_formal`

  已删除 cache：

  - `D:\mypy_cross_maincwd_20260723_192152`
  - `D:\mypy_verbose_main_20260723_192033`
  - `D:\mypy_verbose_snap_20260723_192055`
  - `D:\w14c_all_mypy_cache_20260723_193353`
  - `D:\w14c_mypy_cache_20260723_191938`
  - `D:\w14c_portable_mypy_cache_20260723_192755`
  - `D:\w14c_post_evidence_mypy_cache_20260723_194412`
  - `D:\w14c_snapshot_final_mypy_cache_20260723_193450`
  - `D:\w14c_snapshot_mypy_cache_20260723_192007`
  - `D:\w14c_snapshot_mypy_path_cache_20260723_192140`
  - `D:\w14c_supervised_mypy_cache_20260723_192504`
  - `D:\w14c_supervised_mypy_cache_20260723_192550`

- 2026-07-25：保留 11 个 `D:\Work\AITradingSystem_ops067_*` 目录。9 个含本地修改或
  validation output；另两个 clean 目录仍绑定 OPS-067/ENG-VAL-010 当前 validation 或
  operational acceptance，不把 clean 误当作已完成。它们的 next owner 为 OPS-067/ENG-VAL-010
  coordinator，退出条件为 canonical acceptance、唯一成果迁移和逐目录 diff/ignored-content
  审计完成。
- 2026-07-25：project owner 要求清理所有同类“HEAD 已合入且 canonical evidence 已迁移”
  的临时 workspace。复核确认上述 11 个 OPS-067 clone 的 HEAD 全部是 `origin/main`
  祖先；首次失败 Full 的 summary/profile/import proof 与最终 PASS Full 的
  summary/profile 已在主工作区保留同字节副本，SHA-256 分别为
  `f49fe012...e2e7`、`80ac1117...2114`、`c7ad6fdc...0af4`、
  `33cb2f5f...435`、`6e8c076c...b87`；没有活动进程或 Windows Scheduled Task 引用
  目标目录。中间 dirty 内容与 validation output 均被后续候选取代，不再是 canonical
  evidence source。

  使用显式绝对路径 allowlist 删除以下 11 个 independent clone：

  - `D:\Work\AITradingSystem_ops067_candidate_8fb33e5`
  - `D:\Work\AITradingSystem_ops067_candidate_ada1f7b`
  - `D:\Work\AITradingSystem_ops067_candidate_b0f4c82`
  - `D:\Work\AITradingSystem_ops067_candidate_d92eae35_sparse`
  - `D:\Work\AITradingSystem_ops067_candidate_f0aaa851_sparse`
  - `D:\Work\AITradingSystem_ops067_candidate_fcffae49_sparse`
  - `D:\Work\AITradingSystem_ops067_fix_workspace`
  - `D:\Work\AITradingSystem_ops067_formal`
  - `D:\Work\AITradingSystem_ops067_full_00b41d5`
  - `D:\Work\AITradingSystem_ops067_scan_20260724`
  - `D:\Work\AITradingSystem_ops067_scan2_20260724`

  本轮共删除 73,575 个文件，释放 1,563,750,567 bytes（约 1.456 GiB）。删除后执行
  `git worktree prune`，不存在 stale registration。保留项不满足同类删除条件：
  `D:\w14c_scanner_20260723_185121` 含唯一 dirty/index 状态；
  `D:\Work\AITradingSystem-TRADING-2459-style-discovery` 含未推送在途修改；
  `D:\Work\AITradingSystem_ops070_livefix_20260725` 与
  `D:\Work\AITradingSystem_ops_runtime_20260725` 仍绑定未合入的
  `origin/ops070-closeout` 及真实验收 evidence。
- 删除通过 `Remove-Item`/`git worktree remove` 直接执行，未进入回收站。已提交内容可从
  Git 历史恢复，可重建 cache 可重新生成；只存在于已删除目录的 ignored cache/旧 validation
  output 不保证恢复。删除前已依据 Wave14 COMPLETE、S4A BASELINE_DONE、clean Git 状态和
  commit ancestry 确认这些目录不再是当前 canonical evidence source。

## 7. 2026-07-26 scanner forensic reconciliation

project owner授权按风险顺序处理三个既存worktree，首先处理
`D:\w14c_scanner_20260723_185121`。删除前只读复核得到：

- scanner HEAD=`242aac767d9b4d694e2b91b22853a7875e583815`，其父提交
  `e2da21894ea8e8921a86c6c1b48d7b191f0f142c`已在main历史中；snapshot commit本身不是
  main祖先，但Wave14 readiness实现已由后续reviewed main提交持续演进并通过当前formal Full；
- worktree Git metadata中的index文件已不存在，先前记录的
  `D:\w14c_commit_index_20260723_194551` companion目录也已不存在。使用系统临时目录中的
  alternate index执行`read-tree HEAD`与exact-excluded refresh后，5,959-path表象收敛为
  1个真实tracked差异、0个untracked差异；原worktree和Git metadata均未修改；
- 唯一真实差异为
  `inputs/architecture/arch_004g_deprecation_inventory.yaml`，raw SHA-256=
  `482dfaef9eb3709587c9419e2eea123e069099e311cfcdb62b3e5fae4f7546cc`，
  normalized Git blob=`7a5be6e699dd115c74715d76035ea486731ed38c`。该exact blob已存在于main祖先
  `6a498f60993569e48afc4fd6597fdb6b515e17b8`与
  `cb29989229035d6d790cca4f2594755c69a8f45e`，因此不是唯一未迁移证据；
- alternate-index审计只发现40个ignored `src` cache文件、954,591 bytes，没有
  `outputs/`、`reports/`、`data/`或`artifacts/` evidence；没有活动进程引用该路径；
- 删除allowlist仅包含上述scanner绝对路径。删除前规模为6,000 files /
  99,417,684 logical bytes。执行`git worktree remove --force`与`git worktree prune`后，
  必须确认目录、registration与stale metadata均消失，再把本节和task register更新为完成。

恢复边界：snapshot commit及唯一dirty blob均可从Git历史恢复；ignored cache可重建且不保证
恢复；已消失的旧companion index不再被宣称保留。known-unrelated research文档继续通过exact
pathspec排除，未读取、hash或复制其bytes。

执行结果：`git worktree remove --force`只作用于上述exact allowlist，随后
`git worktree prune`完成。目标目录与worktree registration均已不存在，释放6,000 files /
99,417,684 logical bytes。scanner forensic与清理阶段完成；DEVX-001继续保持
`IN_PROGRESS`以推进TRADING-2459迁移及OPS-070 runtime的独立退出条件。收口验证为
focused=`64 passed`、architecture-fitness=`646 passed`，runtime artifact=
`outputs/validation_runtime/architecture-fitness_20260725T181946Z/test_runtime_summary.json`。

## 8. 2026-07-26 TRADING-2458/2459/2460 clean-main integration

scanner清理后，下一风险项为
`D:\Work\AITradingSystem-TRADING-2459-style-discovery`中的37项在途权威内容。旧worktree
HEAD=`fc6313416d78f56a29519f41ca564eaa1f90e8ce`已是main祖先，但23个tracked修改与14个
untracked文件尚未形成可审阅提交；其中同时包含TRADING-2458、TRADING-2459、
TRADING-2460及共享architecture/task/report权威文件，不能直接删除或将整棵旧worktree
覆盖到当前main。

本轮先建立隔离集成worktree：

- owning tasks：`TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC`、
  `TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE`、
  `TRADING-2460_DECISION_TARGET_CAPABILITY_AUDIT_LABEL_FOUNDATION`和
  `DEVX-001_TEMPORARY_WORKSPACE_LIFECYCLE_AND_CLEANUP`；
- path：`D:\Work\AITradingSystem_trading2459_integration_20260726`；
- branch：`codex/trading-2458-2460-integration`；
- purpose：从当前reviewed main重放37项任务内容，保留main上的后续修复，重新生成共享
  manifests、task shadow、deprecation inventory和compatibility authority，并执行formal
  validation；
- evidence boundary：旧worktree中的任务专属tracked/untracked文档与实现必须先进入Git；
  ignored运行证据只有在canonical main副本存在且hash一致，或已迁移到受治理位置后才能分类为
  superseded/rebuildable；
- exit condition：集成变更通过focused、architecture、contract、report、reproducibility及
  required Full，commit/push并进入reviewed main；随后复核旧worktree和集成worktree的
  tracked/untracked/ignored内容、活动进程、canonical evidence与恢复边界，按exact allowlist
  执行`git worktree remove`和`git worktree prune`。

创建新worktree前已确认旧目录中的两个任务专属ignored输出根为空；对应
`leveraged_exposure_instrument_evaluation`与
`decision_target_capability_audit_label_foundation`运行证据已位于主工作区canonical
`outputs/research_strategies/`根。旧worktree中的contract、architecture和首次失败Full
runtime artifact也在主工作区存在同路径、同规模副本；删除前仍需逐文件SHA-256复核，
不能仅以路径或文件数判定等价。
