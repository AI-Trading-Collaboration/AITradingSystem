# ARCH-004 Wave14 D0B2 + G3 Parallel Readiness

最后更新：2026-07-24

## 基本信息

- task id：`ARCH-004W14_D0B2_G3_PARALLEL_READINESS`
- parent：`ARCH-004`
- priority：`P0`
- status：`IN_PROGRESS`（current stage=`S2_SHARED_INTEGRATION_AND_FORMAL_EXIT`）
- owner：architecture coordinator
- source wave：Wave13 `GOV-006 N1` formal closeout
- source wave base commit：`e2da21894ea8e8921a86c6c1b48d7b191f0f142c`
- source wave tree：`73ba7a3830cbc47ccb6dbfb3488eeed5431653c2`
- production effect：`none`

## 为什么现在先做 S0

Wave13 已把高置信任务状态归一化并通过 formal gate；当前下一条可执行关键路径是
`DATA-GOV D0B2 + bounded ARCH-004G3`。这两个 domain 的实现文件可以互斥，但都需要读取同一批
任务状态、DQ/report contracts、generated manifests 和共享文档。如果不先冻结 exact base、owned/shared
paths、task/requirement bindings 和 coordinator 单写边界，并行开发会重新退化为“代码可以同时写，
但共享语义与最终集成仍靠口头约定”。

S0 只建立可复现的 coordinator manual assignment 准入门禁。它不执行 worker 命令、不获取 lease、
不改变 task status、不切换 consumer，也不产生策略、paper-shadow、production 或 broker 副作用；
machine-level dispatch 始终保持关闭。

## 长期优先级与本波位置

1. Wave14 S0：冻结并验证可执行入口；
2. Wave14：并行实现 D0B2 与首个 bounded G3 native slice；
3. Wave14 closeout后、Wave15 assignment前：经owner确认后推进
   `ARCH-005S4D` narrow S0/S1 checkout writer/operations lease guard；
4. Wave15：从S4D最终HEAD生成exact readiness，推进D0B3 + G4B first consumer，并完成
   G3 close/readiness；
5. G5 + D0C/D1，随后 G6 decision-sensitive characterization；
6. G7 -> H cutover/removal；
7. contracts 稳定后才推进 Knowledge、Publishing 与只读 UX。

策略 A 已关闭。策略 B/C、新候选、prospective、paper-shadow、promotion、production 和 broker 不因本
readiness 自动开放。

## B / C / D 非自引用提交模型

readiness 必须区分三个身份：

- `B`：Wave13 最终已推送 source wave base，即
  `e2da21894ea8e8921a86c6c1b48d7b191f0f142c`；
- `C`：S0 reusable infrastructure、任务登记、支持文档、fresh generated state 与预置 exact C/D
  双态测试的 pre-evidence commit；它在生成 evidence 前必须已普通推送；
- `D`：承载 reviewed Wave14 policy 与 readiness evidence 的 exact two-artifact carrier commit；
  相对 C 的 first-parent diff 只能是这两份 output。

所有 `change_manifest.v1.base_commit` 指向 `C`。Evidence 记录 `B -> C` ancestry、生成时
`HEAD == origin/main == C` 以及 C 的 exact tree/blob 状态，但不记录自身 carrier commit `D`，从而避免
Git commit 自引用。D 推送后，coordinator 才能根据 S0 PASS 人工分配两个 domain worker。

## 分阶段实施

### S0.1 Reusable readiness infrastructure

交付：

- 通用 `wave_readiness` policy/evidence loader、builder、validator 与薄 CLI；
- strict YAML/JSON、Git commit-view、canonical checksum 和 fail-closed negative tests；
- 预先纳入 `tests/test_arch_004_wave14_d0b2_g3_readiness.py` 作为 C/D 双态合同：
  C 中要求两份 carrier output 原子地同时缺失并确认仍处 S0.1，D 中要求两份 output 原子地同时存在并
  对真实 tracked policy/evidence 执行完整 validator；
- 本需求与 task-register 状态；
- 刷新 module/test/aggregate manifests、task baseline/index/fragments、compatibility/deprecation hashes。

退出：

- focused、static、architecture/contract 中与基础设施相关的门禁 PASS；
- commit `C` 只包含可归属变更并普通推送；
- `main == origin/main == C`；
- 用户已有 `docs/research/growth_tilt_owner_diagnosis_pack.md` 修改未读取、未改写、未提交。

### S0.2 Exact Wave14 policy and evidence

交付：

- `config/architecture/arch_004_wave14_d0b2_g3_readiness.yaml`；
- `inputs/architecture/arch_004_wave14_d0b2_g3_parallel_readiness.json`；
- C 中已跟踪的 exact 双态测试在 D 自动切换为真实 artifact validation；D 本身不新增测试文件；
- exact D0B2、G3 和 integration-coordinator `change_manifest.v1`；
- source/task/requirement/blob hashes、generated-state replay、worktree attribution、manual no-lease
  authority、deterministic lane plan。

退出：

- 两次 build byte-identical；
- source Wave13 base B、lane base C、task shadows、requirements、module/test/aggregate manifests 和
  compatibility views全部机械匹配；
- lane plan 为 `DOMAIN(2) -> COORDINATOR(1)`，两个 domain owned paths 无交叉，domain
  `shared_paths=[]`；
- `status=PASS` 只允许 coordinator manual assignment；
- `automatic_command_dispatch=false`、`lease_acquisition_allowed=false`、
  `automatic_merge=false`、`consumer_cutover_allowed=false`、
  `task_registry_mutation_allowed=false`、`production_effect=none`、`broker_action=none`；
- D 必须是 C 的 first-parent 直接子提交，且 `C..D` diff 精确只有上述 policy/evidence 两个路径；
- policy/evidence carrier commit `D` 普通推送后，才具备判定整个 S0 phase-level exit 的条件。

### S0 phase-level exit 与停止规则

单独完成 S0.1、69 个 focused tests、C snapshot formal gate 或任一旧 rehearsal 都不等于 S0 PASS。
整个 S0 必须同时满足：

- C 已普通推送，且 final safe-C snapshot 的 focused/static/architecture/contract、generated state、
  compatibility、deprecation 与 source hashes 全部 PASS/fresh；
- D 是 C 的 first-parent 直接子提交，`C..D` 精确只有 policy/evidence 两份 artifact，且两份 bytes
  与 D 中 tracked blobs 完全一致；
- D 已普通推送，并由 C 中预置的 dual-state test 与 D-state validator 对真实 artifact 完整验证；
- carrier lineage、remote state、worktree attribution 与 known-unrelated exclusion 全部 PASS。

上述条件全部满足前不得分配 domain worker；满足后也只允许 coordinator 根据已冻结 lane plan 进行
manual assignment。`dispatch_allowed=false`、`automatic_command_dispatch=false` 与
`lease_acquisition_allowed=false` 继续不变。`next_slice_unblocked=false` 表示不得由控制面自动释放、
选择或执行下一 slice，不否定 owner 已授权路线下、通过 S0 gate 后的 coordinator manual assignment。
S0 PASS 也不代表 D0B2/G3 已完成，不开放 consumer cutover、策略研究、promotion、production 或 broker。

### S1 Parallel domain implementation

#### D0B2 data lane

目标：

- 把 prices/rates/download manifest/composite binding 收敛为 staged immutable publication +
  validated manifest + atomic discovery boundary；
- canonical DQ gate补齐 market-calendar freshness、逐 ticker requested-window coverage、internal gap
  和 finite-value checks；
- 任一 data/manifest/source/row-count/window mismatch 均为 typed blocker，不降级为 warning；
- 不提前授权 consumer cutover。

Domain exact owned paths：

- `src/ai_trading_system/data/download.py`
- `src/ai_trading_system/data/download_publication.py`
- `src/ai_trading_system/data/quality.py`
- `tests/test_data_download.py`
- `tests/test_data_download_publication.py`
- `tests/test_data_quality.py`

Domain module ids：

- `ai_trading_system.data.download`
- `ai_trading_system.data.download_publication`
- `ai_trading_system.data.quality`

Domain contract claims：

- `data_quality_evidence.v1` / `v1` / `READ`
- `data_quality_execution_receipt.v1` / `v1` / `READ`
- `download_publication_transaction.v1` / `v1` / `WRITE`

本 slice 的语义 claims固定为
`ATOMIC_COMPOSITE_PUBLICATION`、`EXACT_ARTIFACT_SOURCE_BINDING`、
`MARKET_CALENDAR_FRESHNESS`、`REQUESTED_WINDOW_COVERAGE_INTERNAL_GAP`、
`FINITE_VALUE_GATE`、`FAIL_CLOSED_ZERO_DOWNSTREAM` 与 `NO_CONSUMER_CUTOVER`。

`config/data_quality.yaml`、`src/ai_trading_system/config.py`、DQ contracts、
`src/ai_trading_system/data/__init__.py`、CLI wiring、`trading_calendar.py`、
`tests/test_data_quality_execution.py`、system flow 和 catalogs 仍由 coordinator 单写。

Removal targets：

- 删除 `download.py` 中“先逐个覆盖 CSV，最后写 manifest”的发布顺序；
- 删除多个 provider以局部row count重复绑定同一个merged prices文件的歧义；
- 删除 required-current 文件 checksum reconstruction/容忍路径；
- 固定路径 legacy reader保留到D0B3，本阶段不删除。

#### Bounded G3 reporting lane

首个 slice 固定为 Owner Daily 的 `data_quality_and_pit` native provider，原因是它只搬运既有 DQ/PIT
事实，不涉及 score、threshold、position、backtest 或 promotion 重算。

Domain exact owned paths：

- `src/ai_trading_system/platform/reporting/reader_brief_native.py`
- `tests/test_arch_004g3_reporting_native_migration.py`
- `config/architecture/fragments/artifacts/arch_004g3_reader_brief_native.yaml`
- `config/architecture/fragments/flows/arch_004g3_reader_brief_native.yaml`
- `config/architecture/fragments/reports/arch_004g3_reader_brief_native.yaml`

Domain contract claims：

- `artifact_envelope.v1` / `v1` / `READ`
- `report_spec.v1` / `v1` / `READ`

Domain 只实现：

- 与 legacy `_data_quality_pit_safety` 同签名、同19字段和同插入顺序的 pure compatibility
  projector；
- 专用 typed `ReportSectionSpec -> ReportSectionViewModel` provider；
- PASS 与 MISSING/BLOCKED fixtures、determinism、no-recompute 和 dependency tests。

Coordinator 随后单写：

- `src/ai_trading_system/reports/reader_brief.py`：导入 projector并删除本地49行 builder；
- `src/ai_trading_system/platform/reporting/owner_daily.py`：把该 section 从 generic provider切换为
  native provider；
- public exports、registry/catalog/flow、requirements、task register 与 generated state。

首 slice ratchet：

- native core coverage `0 -> 1/10`；
- generic provider responsibility `10 -> 9`；
- Reader Brief 本地 `_data_quality_pit_safety` AST定义 `1 -> 0`；
- top-level functions `367 -> <=366`，legacy LOC净下降；
- legacy JSON/HTML bytes、path、schema、status保持；
- legacy HTML renderer本 slice不搬迁，避免不必要的 parity 风险。

### S2 Shared integration and formal exit

Coordinator按冻结顺序集成 shared contracts/config/exports、Reader Brief/Owner Daily cut-in、flow、
catalog、task requirements、generated manifests、compatibility baseline 与 deprecation inventory。
Domain worker不得并发编辑这些路径。

#### S1 后发现的 coordinator scope amendment

真实 `daily-run` 证明 `ops_daily._execution_command` 会把
`aits validate-data ... --execution-profile daily_default.v1` 转换为
`python -m ai_trading_system.cli_direct ...`。S0 policy冻结了泛化的CLI wiring与
`cli_commands/data_cache.py`，但漏列真正执行的direct dispatcher，导致profile在边界被丢弃并被
`auto + explicit --as-of`解析为`manual.v1`。

该缺口按owner“继续修复”指令作为最小coordinator incident fix纳入：

- `src/ai_trading_system/cli_direct.py`
- `tests/test_cli_direct.py`

历史S0 policy/evidence carrier `39a3ea730`保持不可变，不回写或伪造当时scope；新增
`config/architecture/arch_004_wave14_d0b2_g3_scope_amendment.yaml`绑定source carrier、owner指令、
exact新增路径、其自身governance artifact/test路径、原因和安全边界。改动只恢复既有daily profile的
原样传播；无profile时仍传`auto`，显式
`--as-of`继续解析为`manual.v1`。当前代码变更本身`production_effect=none`；未来正常daily执行会按既有
设计写本地content-addressed DQ receipt/discovery，不授权consumer cutover、weights、production或broker。

验证按风险分层：

- 每个 domain 先跑自己的 focused + static；
- shared integration 跑 combined focused、report-validation、architecture、contract、
  reproducibility/integration；
- 整个 Wave14 只在最终 tracked state 运行一次 required Full；
- 若出现异常长尾，先确认重复扫描、fixture/cache miss、worker tail idle 与内存压力；不得删除测试或降低
  DQ/PIT/parity门禁。

## Coordinator-only shared paths

S0.2 policy 至少冻结：

- `config/architecture/arch_004_refactor_policy.yaml`
- `config/architecture/devex_ownership_policy.yaml`
- `config/data_quality.yaml`
- `config/reporting/reporting_architecture.yaml`
- `config/report_registry.yaml`
- `docs/artifact_catalog.md`
- `docs/architecture/dual_lane_development_operating_model.md`
- 本需求、ARCH-004/G/DATA-GOV requirements
- `docs/system_flow.md`
- `docs/task_register.md`
- `docs/task_register_completed.md`
- `inputs/architecture/arch_004_compatibility_baseline.yaml`
- `inputs/architecture/arch_004e_aggregate_shadow_index.yaml`
- `inputs/architecture/arch_004e_architecture_fitness.yaml`
- `inputs/architecture/arch_004e_module_manifest.yaml`
- `inputs/architecture/arch_004e_test_manifest.yaml`
- `inputs/architecture/arch_004g_deprecation_inventory.yaml`
- `inputs/architecture/arch_005_task_registry_baseline.yaml`
- `inputs/architecture/arch_005_task_shadow_index.yaml`
- `src/ai_trading_system/reports/reader_brief.py`
- `src/ai_trading_system/platform/reporting/owner_daily.py`
- `src/ai_trading_system/platform/reporting/__init__.py`
- `src/ai_trading_system/config.py`
- `src/ai_trading_system/contracts/data_quality.py`
- `src/ai_trading_system/contracts/data_quality_execution.py`
- `src/ai_trading_system/data/__init__.py`
- `src/ai_trading_system/cli_commands/data_cache.py`
- `src/ai_trading_system/trading_calendar.py`
- `tests/test_data_quality_execution.py`
- shared contracts/public exports/CLI wiring及相应 integration tests。

Exact superset 由 S0.2 policy 绑定；domain manifest命中任一 coordinator-only path必须 fail closed。

## Commit snapshot 与工作区归属

- 所有 B/C source、task、requirement 和 generated-state 验证从 Git commit blob或 C 的最小
  allowlist snapshot读取，不从主工作区同名文件读取，也不物化整个 C tree；
- 主工作区 pre-write guard 使用
  `git status --porcelain=v2 -z --untracked-files=all -- . :(top,literal,exclude)<known-unrelated>`，对每个
  known-unrelated path机械加入显式 exclude pathspec；
- known unrelated allowlist为
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，只允许存在或为空，不读取其 bytes；
- C snapshot只通过
  `git archive --format=tar <C> <exact-allowlist...>`包含 `src/ai_trading_system`、`tests`、
  active/completed task registers、generated-state paths、architecture fragments及 ownership
  policy声明的aggregate current/fragment roots；validated extractor在写出前拒绝known-unrelated
  member、非普通文件/目录、path escape、case collision和link metadata；
- evidence显式记录`GIT_ARCHIVE_C_ALLOWLIST_ONLY`、canonical archive paths与excluded paths，并验证
  archive/excluded无相等、祖先或后代碰撞；
- 任一未声明 dirty/staged/untracked path均阻断 S0；
- 历史 worktree列表不等价于 active owner/lease；
- 历史G2.5 evidence在descendant验证时，若live deterministic rebuild已漂移，必须先证明
  source-base first-parent直接child carrier中的evidence exact blob，再从该carrier的安全Git snapshot
  按原base语义重放；不得用descendant当前文件重新解释历史证据；
- compatibility旧raw hash只有在后续normalization record同时绑定
  `previous_worktree_sha256`、`hash_normalization=git_eol_lf`与canonical SHA，且当前normalized bytes
  仍匹配时才视为同内容；历史source record不重写。

## Lease 语义

当前项目没有全局 lease registry。本 S0 使用：

- `lease_authority.kind=NONE_AT_S0_MANUAL_ASSIGNMENT`
- `lease_namespace_created=false`
- `lease_acquisition_allowed=false`
- `active_shared_path_lease_count=0`

这里的0来自“本 gate没有创建或允许 lease namespace”的权限不变量，不宣称扫描了所有历史
`outputs/**`。未来如启用自动 lease，必须先声明唯一 exclusive root并用
`FileExecutionLeaseStore(...).replay()`证明完整scope。

## 主要 fail-closed 负例

- duplicate/unknown/missing YAML或JSON字段，NaN/Infinity/overflow finite；
- B/C unknown、B不是C祖先、C不是HEAD/remote祖先、build时HEAD/remote drift；
- source/task/requirement/shadow/generated manifest hash或status漂移；
- validate时`HEAD==C`、carrier未跟踪policy/evidence、carrier blob与本地bytes不一致，或remote ref退化为
  `HEAD`/`FETCH_HEAD`等可写本地引用；
- domain owned path交叉、domain声明shared path、命中 coordinator-only path、缺失或多个coordinator；
- selected domain的`change_id/task_id`交叉错配，或task fragment root存在未进入index的额外YAML；
- worktree出现未声明path、status调用缺少known-unrelated exclude pathspec，或把无关用户文件纳入hash；
- C archive allowlist为空、与known-unrelated路径碰撞，或archive尝试包含任一forbidden member；
- 把producer/replay依赖加入dirty allowlist，或让policy/evidence输出覆盖task、source、generated、
  manifest、owned/shared/coordinator路径；
- manual S0开启dispatch/lease/merge/cutover/task mutation/production/broker任一权限；
- evidence checksum、policy/generator hash、lane plan或source binding篡改；
- evidence尝试记录自身carrier commit并形成自引用；
- normalization migration缺少previous/canonical双向绑定，或当前normalized content发生真实漂移；
- 历史G2.5 carrier缺失、不是source-base first-parent直接child、evidence blob漂移、snapshot不能安全
  提取，或carrier snapshot replay不再重现tracked evidence。

## 进展记录

- 2026-07-24：S2 shared/generated/compatibility fixed point 已完成首轮闭合：
  task registry=`407 active / 487 completed / 894 total`且 consumer view byte-identical，
  DevEx=`1007 modules / 1172 tests / 18 aggregate fragments / 856 direct writers /
  0 violations`，deprecation inventory 在两轮生成间 byte-stable；compatibility S2 suffix
  已冻结 112 个 normalized source records、85 个 superseded authority paths、95 个
  Full-sensitive paths与19个 post-Full evidence-only allowlist paths。candidate C
  `4235590dff21f02492edd3408474b02915c501bf`上的 pre-Full combined focused 为
  `342 passed / 1 skipped / 1 failed`；唯一失败不是domain实现回退，而是历史S0 readiness
  replay仍把“远端已持有carrier、local HEAD为其合法后继”错误判为`HEAD != origin/main`。
  本阶段不通过提前推送未验证candidate绕过该门禁；正在把carrier判定修正为显式ancestry语义，
  同时保留未推送carrier、remote/lane无祖先关系及local/remote分叉的fail-closed负例。原candidate
  将由修复后的新candidate取代，修复、generated/source-hash fixed point及全部pre-Full tiers通过前
  不启动唯一final Full；`next_slice_unblocked=false`、`production_effect=none`保持不变。
- 2026-07-24：D0B2第二轮独立审查发现的publication临界区P1已关闭。外围捕获的legacy
  prices/secondary/manifest存在性、exact bytes、SHA-256与size现在组成typed pre-commit condition，
  由`publish_immutable_snapshot`在同一dataset lock内、validated snapshot之后且atomic pointer
  replace之前通过root-contained/no-follow读取重验；manifest historical prefix只使用捕获时的exact
  bytes。regular manifest replacement、prices link replacement与callback异常均fail closed为零pointer/
  零publication，authority、staging与dataset lock在异常后可释放并允许同dataset再次成功发布。
  combined focused=`180 passed / 1 skipped`；独立复核=`136 passed / 1 skipped`、关键race/callback
  `4 passed`且未发现新增P0/P1。该结果只关闭domain code blocker；generated/compatibility/formal tiers与
  唯一final Full仍须闭合后才能通过phase exit。
- 2026-07-24：phase-exit独立审计未发现P0，但发现三个不能带入formal gate的P1：
  manifest只有bytes/header/row-count外壳校验而缺current-generation transaction语义绑定；首次legacy
  cache bootstrap可跟随leaf symlink/reparse读取root外bytes；G3只用`<`/`<=`宽松规模ratchet且artifact
  fragment被本地exclude忽略。D0B2因此增加publisher precommit/resolver共用semantic validator、
  normalized source-binding copy和root-contained/no-follow bootstrap；G3增加历史F3 raw SHA与当前
  Reader Brief source SHA/29,005 LOC/366 functions、native/generic=`1/9`、fragments=`5/0 active`
  exact ratchet及字段级漂移负例。Ignored artifact fragment必须在clean candidate显式force-add并由
  `git ls-files`/clean-clone验证。上述修复完成focused/static后仍只代表code gate，generated/formal/
  final Full尚未闭合。
- 2026-07-23：两个domain实现与首轮独立审计完成。D0B2在修复secondary
  present→absent retirement、relative `output_dir` predecessor path及canonical INVALID隐式二次解析后，
  combined data focused=`156 passed`，Black/Ruff/strict mypy PASS；G3独立审计确认native
  `data_quality_and_pit` provider、legacy 19字段parity、native/generic=`1/9`及LOC/function ratchet均无
  P0/P1。当前进入S2 shared integration；final generated state、formal tiers与唯一Wave14 Full尚未完成。

后续非Wave14 release blocker已归入现有长期任务：DATA-GOV D0C重算immutable replay-input CSV
`row_count`；G3后续slice增加代表性完整Reader Brief JSON/HTML before/after golden parity；Wave15
G4B first-consumer requirement补一条真实`ops_daily -> cli_direct -> daily discovery`跨层回归。
- 2026-07-23：S0 exact readiness carrier 已以 `39a3ea730` 普通推送并使
  `main == origin/main`；project owner 随后明确要求继续修复本次真实
  `DQ_INPUT_ROW_COUNT_MISMATCH`。Coordinator 据此完成 manual assignment：
  D0B2 data lane 只写本需求冻结的 3 个 data modules / 3 个 focused tests，
  bounded G3 reporting lane 只写其 native reporting owned paths；两域保持
  `shared_paths=[]`，shared docs/config/exports、最终 daily-run 和 formal gate
  仍由 coordinator 单写。验收必须由真实 composite publication 让最终文件的
  checksum、size、full row count 与 source-event provenance 同时可验证，并保持
  mismatch fail closed；不得手工修 manifest、降级 warning、启用 consumer cutover、
  production 或 broker。
- 2026-07-23：Wave13 closeout commit
  `e2da21894ea8e8921a86c6c1b48d7b191f0f142c` 已普通推送；实时 `git fetch --prune origin`
  成功，随后本地 `HEAD`、`FETCH_HEAD` 与 `origin/main`均为该commit。主工作区只保留既有无关
  research文档修改。S0.1开始，尚未分配 D0B2/G3 worker。
- 2026-07-23：只读scope审计选定 G3首slice为`data_quality_and_pit` native provider；它有唯一legacy
  builder/caller，可用最小范围实现真实ownership ratchet与byte parity。D0B2 exact owned paths也已
  收敛为3个data modules与3个focused test files；两域没有domain写路径交叉，仍须由S0.2 policy/
  evidence机械冻结后才可派发。
- 2026-07-23：S0.1 generic producer/CLI初版、33个readiness focused tests与1个direct-writer ratchet
  focused gate已实现。该处`33 / 1168`是后续C/D双态与snapshot hardening前的rehearsal快照；当时DevEx机械重建为
  `1005 modules / 1168 tests / 15 aggregate fragments / 856 direct writers / 0 violations`，
  task shadow重建为`406 active / 487 completed / 893 total`且byte-identical。首次DevEx正确发现
  snapshot extractor新增`Path.write_bytes`会把writer增至857；已改为严格校验member后使用安全tar
  extraction，并保持ratchet 856。commit-snapshot replay同时发现
  `config/architecture/fragments/artifacts/growth_tilt_candidate_family_closure.yaml`长期被tracked
  aggregate index引用却受本地`artifacts/`规则忽略；C将显式纳管该491-byte架构fragment，而不是删除、
  绕过或吸收新的策略研究结果。S0.1仍在review/compatibility/formal gate阶段，D0B2/G3未分配。
- 2026-07-23：clean-commit architecture rehearsal暴露并修复两项portable replay缺陷。其一，
  compatibility旧raw checkout hash只有在后续`git_eol_lf`记录同时绑定
  `previous_worktree_sha256`与canonical SHA、且当前normalized bytes仍匹配时才视为等价；LF/CRLF
  物理差异不再产生假漂移，material content drift仍fail closed。其二，历史G2.5 evidence在live
  deterministic fast path发生descendant drift时，必须验证source-base first-parent直接child carrier中的
  tracked evidence exact blob，并从该carrier的安全Git snapshot按原base语义重建；当前workspace只供
  lineage/blob查询，禁止用后续catalog/flow变化刷新历史证据。新增carrier tamper、snapshot drift与
  non-direct-child负例。snapshot/C-D hardening前的clean-snapshot rehearsal architecture=
  `533 passed / 79.36s`、contract=`266 passed / 137.17s`均PASS；该时点仍未形成final C formal
  evidence，整个S0尚未PASS，D0B2/G3仍未分配。
- 2026-07-23：S0.2只读执行审计进一步发现原实现的repo-wide `git status`和whole-tree
  `git archive`会让known-unrelated tracked文档落入查询/物化范围，同时D的exact two-file carrier合同与
  尚未跟踪的真实artifact测试冲突。S0.1因此改为显式status exclude pathspec +
  `GIT_ARCHIVE_C_ALLOWLIST_ONLY`，并在C预置C/D双态测试；D继续精确只承载policy/evidence。新增调用参数、
  allowlist archive及forbidden-member负例，任何遗漏或路径碰撞均fail closed；该修复不读取无关文档
  bytes，不改变D0B2/G3 scope、策略语义或production边界。
- 2026-07-23：snapshot/C-D hardening后的focused精确口径为
  `38 generic readiness + 1 exact C/D dual-state + 30 historical G2.5 carrier replay = 69 passed`；
  tracked test inventory随新增双态测试由`1168 -> 1169`。前述clean architecture=`533`与
  contract=`266`只作为hardening前rehearsal保留，不能冒充carrier C最终formal evidence；最终
  manifests、deprecation、compatibility source hashes及formal artifacts仍须从安全C snapshot刷新并
  PASS后才可提交C。
- 2026-07-23：首个排除known-unrelated文档的final-C candidate snapshot中，Black、Ruff与focused=
  `100 passed`，但无本地editable-package遮蔽的fresh strict mypy真实暴露4个既有architecture依赖模块
  共11个类型错误；主工作区增量/本地包解析得到的PASS不能作为portable证据。S0.1不采用
  `follow-imports=skip`或缓存绕过；现已对`parallel_control_kernel`、`parallel_control_scheduler`、
  `cli_contract`与`supervised_automation`完成窄幅类型正确性修复，并将4个路径纳入Wave14 current
  source authority。Black、Ruff、fresh strict mypy均PASS，S2/S3/S4/CLI contract回归=`169 passed`；
  runtime output与dispatch、lease、scheduler、process语义不变。下一步重建safe-C snapshot与final
  formal artifacts。
- 2026-07-23：重建后的safe-C validation snapshot=`7b6f5b6e9aec2ab69801cd1d26bea25bb3688810`、
  tree=`baf8a636486793389349544e73698dbe308cfe30`，精确纳入41个可归属路径并仅为验证从tree移除
  known-unrelated文档，snapshot diff=42 paths、受保护路径未物化、worktree clean。该snapshot的
  Black/Ruff/fresh strict mypy与expanded focused=`269 passed`；正式architecture=
  `534 passed / 64.75s`，artifact=`architecture-fitness_20260723T103603Z`、
  SHA256=`4022c3b1d7910c0762fbbf706a64e8c2a40079261498c7428d0a86e749c43f34`、
  size=`28626`；正式contract=`266 passed / 137.59s`，
  artifact=`contract-validation_20260723T103714Z`、
  SHA256=`ed93506bb131b8e09ab9c587a9ec84c9e676abe40407d541280fe86b892e7da1`、
  size=`28282`。两份artifact已复制回canonical ignored runtime目录并保持byte-identical；仍需刷新
  post-evidence manifests/task registry/source hashes并复验后，才可形成并推送真实carrier C。
- 2026-07-23：carrier C1=`6a498f60993569e48afc4fd6597fdb6b515e17b8`已普通推送；
  首次真实S0.2 build继续fail closed暴露commit snapshot root与task baseline消费者扫描合同不一致：
  baseline按`src/tests/scripts`扫描129个consumer，但snapshot只归档`src/tests`，机械重放为127，
  缺少`generate_qqq_plus_growth_closeout_artifacts.py`与`run_clean_clone_release_acceptance.py`。
  最终lane base C因此增加`scripts` scan root与回归断言后重新验证/推送，D仍精确只承载两份artifact。
  同次build还发现历史compatibility baseline的G2.4BR冻结前缀含duplicate key；该前缀受byte
  immutability保护，不得回写。S0.2将B/C compatibility都按exact raw Git-blob SHA绑定，并另以C的
  `arch_004e_architecture_fitness.v1/PASS`承担当前结构化语义权威；未来若治理须新增v2 normalization
  overlay，不得改写v1历史bytes。当前无domain assignment、dispatch、lease、cutover、production或broker
  副作用。
- 2026-07-23：修复`scripts` snapshot root后的safe-C2 candidate=
  `7b31a3dba480383fbc9a6fc1e118b3b52fb5e78d`，validation-only safe snapshot=
  `c5873ae1a6f4b85cf9c71cc5a5bdcec0a4e1093b`，受保护文档未物化。Black、Ruff、fresh strict
  mypy、focused/freshness/deprecation与registry/DevEx验证均PASS；正式architecture=
  `534 passed / 68.64s`，artifact=`architecture-fitness_20260723T112455Z`、
  SHA256=`3d9879036752fec7d04357ac66c8b47c82b05fb2cee486010e3d558b7abe344c`、
  size=`27602`；正式contract=`266 passed / 138.53s`，
  artifact=`contract-validation_20260723T112604Z`、
  SHA256=`b6eca48edb84c5926a680c1b970745d6a67915494a817cd7d5f733ecd12bb856`、
  size=`27259`。两份runtime目录已逐文件复制回canonical ignored路径并验证byte-identical；
  当前只剩post-evidence generated state/source hash复验、最终C2提交推送与S0.2 exact carrier D，
  整个S0仍未PASS。
