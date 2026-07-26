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

复核结果：旧worktree中的
`contract-validation_20260725T101417Z`（3 files / 36,373 bytes）、
`architecture-fitness_20260725T102719Z`（3 files / 40,214 bytes）和
`full_20260725T103158Z`（4 files / 13,602,396 bytes）已与主工作区同相对路径副本逐文件
比对SHA-256，三组均byte-identical。旧worktree两个任务专属
`outputs/research_strategies/`根为空；主工作区对应QLD与label-foundation canonical根分别
保留12和7个文件。因此旧worktree不再是这些运行证据的唯一副本，但仍须等待代码集成进入
reviewed main、formal validation通过且无活动进程后才能删除。

clean-main集成formal验证已通过：focused=`100 passed`、Ruff/Black/strict mypy PASS、
report-validation=`57 passed`、reproducibility=`23 passed`、
contract-validation=`275 passed`、architecture-fitness=`648 passed`、
integration=`995 passed`、required Full=`7281 passed / 4 skipped / 643 warnings`，
Full runtime artifact=
`outputs/validation_runtime/full_20260725T185736Z/test_runtime_summary.json`。下一步只剩集成
commit/push进入reviewed main、验证commit ancestry与canonical evidence，然后检查活动进程并
按exact allowlist移除旧TRADING与临时集成worktree；DEVX-001仍保持`IN_PROGRESS`直到
OPS-070 runtime满足其独立退出条件。

执行结果：旧worktree的37项内容先以checkpoint commit=`95a26bcac`完整保留，再从
reviewed main=`3e58b2c6d`重放并修复生成权威冲突；最终集成commit=
`0f585879650f3433008bbbfbbaf52f47dba1ae15`已纯快进进入`main`并推送到
`origin/main`。删除前把8组正式验证目录、25个文件、13,924,990 bytes复制到主工作区
同相对路径，并逐文件复核SHA-256；两个任务专属ignored输出根仍为0 files / 0 bytes，
没有活动进程引用目标目录。

本轮删除allowlist严格限定为：

- `D:\Work\AITradingSystem-TRADING-2459-style-discovery`
- `D:\Work\AITradingSystem_trading2459_integration_20260726`

两个worktree均为0项tracked/untracked差异；旧worktree含4,775个ignored文件 /
200,003,010 bytes，集成worktree含4,399个ignored文件 / 171,083,471 bytes，均由已迁移
validation evidence之外的可重建cache、编译产物和被正式验证取代的运行中间物组成。执行
`git worktree remove --force`与`git worktree prune`后，目录和registration均不存在，
共释放21,289 files / 573,489,392 logical bytes（约546.92 MiB）。

恢复边界：集成实现可从`main`、`origin/main`及
`origin/codex/trading-2458-2460-integration`恢复；旧在途字节仍可从本地
`codex/trading-2459-style-discovery`的checkpoint commit恢复；canonical研究输出和正式
validation evidence保留在主工作区。已删除的ignored cache与非canonical中间物未进入回收站，
不保证逐字节恢复但可重建。known-unrelated research文档继续按exact pathspec排除，未读取、
hash、复制或纳入提交。TRADING工作区退出条件已全部满足；DEVX-001继续保持
`IN_PROGRESS`，仅剩`D:\Work\AITradingSystem_ops_runtime_20260725`的独立合法scheduler验收
与证据迁移/清理。

## 9. 2026-07-27 research worktree lifecycle reconciliation

DEVX-004 收口后的只读 preflight 发现 Git 仍登记 10 个未写入本任务保留清单的
TRADING-2458/2461/2462 worktree。本轮按 2.1 的统一删除依据逐目录复核，而不是把
`checkout_worktree_audit=PASS` 或 clean tracked state 单独当作删除授权。

### 9.1 删除候选与 exact allowlist

以下 8 个目录属于已完成任务的历史 carrier，目标实现已由后续 reviewed main 候选取代；
其中 4 个 HEAD 是当前 main 祖先，另外 4 个 non-ancestor branch 的 commit/patch 已由任务
requirement 明确记录为 base-drift 后废弃或重建的中间版本：

- `D:\Work\AITradingSystem_t2461_7b88`：10,456 files / 320,620,735 bytes；
- `D:\Work\AITradingSystem_t2461_8a31`：10,242 files / 317,855,952 bytes；
- `D:\Work\AITradingSystem_t2461_a309`：9,056 files / 285,564,707 bytes；
- `D:\Work\AITradingSystem_t2461_fb18`：10,633 files / 326,009,624 bytes；
- `D:\Work\AITradingSystem_trading2458_retirement_20260726`：
  7,716 files / 201,837,924 bytes；
- `D:\Work\AITradingSystem_trading2458_retirement_rebase_20260726`：
  10,340 files / 272,670,538 bytes；
- `D:\Work\AITradingSystem_trading2458_retirement_rebase2_20260726`：
  10,337 files / 265,455,757 bytes；
- `D:\Work\AITradingSystem_trading2458_retirement_rebase3_20260726`：
  9,576 files / 287,260,237 bytes。

TRADING-2461 最终 7b88 的 43 个 required output files 已在主工作区同相对路径逐文件
SHA-256 相等；旧 a309/8a31/fb18 validation outputs 是被后续 exact-base Full 取代的
中间证据。untracked task-owned code/config/research files除历史 requirement 与 task shadow
状态外均与当前 main 相同；差异项由后续 reviewed completed archive 明确取代。

TRADING-2458 v4 的最终验证证据仍仅位于 rebase3 worktree。删除前必须把以下 6 组
19 files / 14,017,755 bytes 迁移至主工作区相同的 canonical
`outputs/validation_runtime/trading2458_retirement_rebase3_20260726/` 路径，并复核
bundle SHA-256=`0bfe5f2929ccfbd7404e6c15a509acf3a751db916acdbdffca373eb8a13cdc21`：
`closeout_architecture`、`closeout_contract`、`closeout_full`、
`final_evidence_architecture`、`final_evidence_contract` 与
`post_full_architecture`。

8 个删除候选均没有活动进程或 Windows Scheduled Task 引用。删除只允许使用上述 exact
绝对路径，先逐个执行 `git worktree remove --force`，再执行 `git worktree prune`。
已提交实现可由 Git main/历史分支恢复；迁移后的 final evidence 由主工作区恢复；被后续
候选取代的 ignored cache、旧 validation outputs、lease/intents 与 mutable shadow views
不保证恢复。

### 9.2 强制保留

- `D:\Work\AITradingSystem_t2462_tailrisk` 与
  `D:\Work\AITradingSystem_t2462_tailrisk_v2` 含尚未进入 main 的 TRADING-2462
  tail-risk robustness implementation/requirement。审计期间 v2 的 PID `45804` 执行
  `validate_content_rebuild.py`，随后正常结束并把 branch 从 `573a8c27b`推进到
  checkpoint `c1ce71825`，进一步证明该路径仍是活动研究来源。分类为 `KEEP_ACTIVE`，
  next owner 为 strategy research owner；退出条件是任务授权/归档结论、独有代码与证据
  迁移、进程结束和重新审计。
- `D:\Work\AITradingSystem_ops_runtime_20260725` 继续分类为 `KEEP_ACTIVE`，仍由
  OPS-070 下一合法 scheduler run、canonical evidence 迁移与 operational acceptance
  退出条件控制，本轮不得修改或删除。

known-unrelated
`docs/research/growth_tilt_owner_diagnosis_pack.md` 在所有 Git inspection 中使用 exact
literal exclusion，未读取、hash、复制、暂存或修改。

### 9.3 执行结果与 worktree-audit 事故

TRADING-2458 v4 的 19 files / 14,017,755 bytes 已迁移至主工作区；源与目标逐文件
size/SHA-256相等，目标 bundle SHA-256=
`0bfe5f2929ccfbd7404e6c15a509acf3a751db916acdbdffca373eb8a13cdc21`。
首次 PowerShell copy 尝试因 `Copy-Item -LiteralPath` 不展开 `*`，只创建6个空目录，
未复制、覆盖或删除文件；随后改为逐文件 exact `-LiteralPath` 复制并通过完整哈希复核。

上述8个exact worktree已执行`git worktree remove --force`与`git worktree prune`，目录和
registration均消失，共删除78,356 files / 2,277,275,474 logical bytes。清理后只剩主
checkout、OPS-070 runtime与两个TRADING-2462 worktree。8条本地branch ref暂时保留，作为
旧carrier commit的直接恢复入口；是否删除branch须另行做branch-retirement审计。

删除完成后复核发现，本轮“逐worktree checkout audit PASS”证据无效：命令虽然在目标目录
执行，但调用的是主工作区
`D:\Work\AITradingSystem\scripts\architecture_arch005_checkout_guard.py`；该脚本的
`PROJECT_ROOT`固定取脚本所在仓库，因此实际重复审计了主checkout。删除前确实完成并保留了
每个目标的branch/HEAD/main ancestry、exact-excluded untracked清单、ignored清单、任务文件
与main hash对照、output逐文件hash对照、进程和Scheduled Task检查；但没有正确执行目标
worktree的tracked unstaged/staged diff audit。

由于目录已直接删除且未进入回收站，无法事后证明不存在unique uncommitted tracked bytes，
也不保证恢复这类潜在字节。风险缓解证据包括：任务最终实现已进入reviewed main；任务
requirements明确记录多轮base-drift supersession；任务专属untracked实现已与main对照；
TRADING-2461 final与迁移后的TRADING-2458 final evidence已逐hash保全；旧branch refs仍在。
这些证据支持实现与required evidence可恢复，但不把缺失的tracked-diff检查改写为PASS。

在新增target-bound worktree audit入口并验证其确实绑定被审计repository之前，不得继续删除
OPS-070或TRADING-2462 worktree。该工具缺口必须作为独立P0 developer-workflow follow-up
登记和修复。

本轮收口验证通过：architecture-fitness=`686 passed`，runtime artifact=
`outputs/validation_runtime/architecture-fitness_20260726T170436Z/test_runtime_summary.json`；
contract-validation=`275 passed`，runtime artifact=
`outputs/validation_runtime/contract-validation_20260726T170655Z/test_runtime_summary.json`。
这些PASS证明当前治理记录、兼容性基线和contract在主checkout中一致，不补足删除前缺失的
target tracked-diff审计，也不解除后续worktree删除禁令。DEVX-001继续保持`IN_PROGRESS`，
等待DEVX-005修复以及OPS-070/TRADING-2462各自满足独立退出条件。

2026-07-27后续修复：DEVX-005已实现并验证`checkout_worktree_audit.v2`，要求跨worktree审计
显式传入`--target-repository`，输出policy/target/registration双身份并在审计前后检查漂移。
真实TRADING-2462 v3 target只读审计PASS，focused=`29 passed`、Architecture=`692 passed`、
Contract=`275 passed`。因此“缺少target-bound工具”的前置阻塞在该修复进入reviewed main后
解除；但本任务不会据此自动删除OPS-070或任何TRADING-2462 worktree。每个候选仍必须重新执行
target-bound audit、unique evidence/active process/scheduler/recoverability检查并取得其独立退出
条件；本轮没有执行新的worktree删除。
