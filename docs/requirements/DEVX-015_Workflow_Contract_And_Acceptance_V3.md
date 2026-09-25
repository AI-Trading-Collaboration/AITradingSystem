### v389 隔离 profile 检查器与 V02 加载实现语义的修复（2026-09-25）

- 根因：`3c28347e3` 将开发模式 profile 检查子进程改为 `-I` 并运行调用方自身实现（非候选脚本）。后果一：`_full_readiness_semantics`/`check_full_readiness` 要求检查代码根等于候选根，测试中 pytest/CLI 以 lane 代码检查夹具仓库，恒判 inadmissible。后果二：检查子进程不再继承调用方启动代码，原生夹具的测试注册表映射与 V02 注入故障均无法到达。后果三：实际 Full 启动器冻结环境写入候选 src 的 `.pyc`，夹具 `.gitignore` 仅忽略 `outputs/`，readiness 报 `READINESS_INSPECTION_CODE_UNTRACKED`、release 报未归属脏文件。
- 生产修复（V02 environment_or_loaded_module_changed）：`_prepare_current_full_profile` 在信任隔离检查结果前，调用方自身执行既有 `acceptance_runtime_identity()`（原 Full worker 已通过的已加载源码保管检查），失败即 `PUBLICATION_FULL_CLOSURE_INVALID`。实测正常 ~10s 通过；注入 yaml `Reader.peek` co_filename 故障被 `ACCEPTANCE_LOADED_CODE_ORIGIN` 拒绝。未新增校验器或放宽检查器隔离。
- 测试夹具修复：`canonical_merge_repository` 的 profile 模式把 fence 模块实现根绑定到夹具中逐字节相同的副本并前置夹具 `src` 到 PYTHONPATH；在夹具 `.git/info/exclude`（非跟踪变更）加入 `__pycache__/` 以镜像项目忽略语义；`_v03_cli` 在目标仓库自带实现时运行其自身脚本/src；原生夹具启动代码对 `-I` 检查子进程显式执行同一映射后再运行原脚本；formal 探针时限对齐生产 `FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS`（实测 59s）；见证断言改为仅 CLI 1 个故障进程且拒绝原因必须为 `ACCEPTANCE_LOADED_CODE_ORIGIN`。
- 串行代表验证：formal-profile PASS；lifecycle unchanged/index-replaced 与 V02 身份漂移共 3 PASS / 2951.68s（日志 `outputs/validation_runtime/devx015-claude-code/trio_v1.log`）。
- 另两处测试陈旧修复：`test_remote_admission_rechecks_original_full_profile` 原把 origin 整体改为本地 bare，违反 P01 `repository_identity`；改用既有约定仅 `set-url --push` 并显式 fetch 本地 bare。`distribution_drift` 合成发行版缺 RECORD，被 `10ac47c94` 新增的 `ACCEPTANCE_DEPENDENCY_INVENTORY` 提前拒绝，未覆盖原意；补合法 RECORD 后真实触发运行时身份变更拒绝。REMOTE_PUSH_PRE 公共 CLI 观察时限 90s→210s（180s profile probe + 收尾，与本模块其它 CLI 探针一致）。原生夹具对 pytest 进程自身发起的检查同样显式施加映射。
- 原生 native 与 remote admission control/changed-profile 串行 3 PASS / 1976.03s（`native_pair_v2.log`），HKCU 测试键无新增。
- 首次回归 54 个失败节点确认：-n 4 load 下 31 PASS，其余 22 超时均为 600s 挂死保护在 4 个内层 16-worker Full 并发下触发（串行同步骤 170–520s），加 `distribution_drift` 1 个真实陈旧；修复后 -n 2 load 21/22 PASS / 6072s（`regression_v1_failed_confirm2.log`），唯一失败为上述 90s 时限，修复后串行 PASS / 726.89s。结论：重型整链节点并行度不超过 2。
- ruff 全部通过；mypy：fence 模块 0 错误，`workflow_coordination.py` 15 个错误均不在本次改动行内（既有）。
- 遗留 HKCU 测试键 3 个（`926d04cd…`、`927fd4f4…` 为此前崩溃遗留；`b12411aa…` 为 v387 回归中被中止的 linked-contender 原生测试遗留，含测试注册值与 MachineGuid 副本），未自行删除，交 owner 处理。

### v388 回归中既有失败的诊断与修复（2026-09-25）

- 回归（coordination/execution/acceptance/validation_tier/protected entry，-n 4 loadfile，默认 %TEMP% basetemp）结果 717 PASS / 40 FAIL / 23 ERROR / 2:30:52，日志 `outputs/validation_runtime/devx015-claude-code/devx015a_regression_v1.log`。在不含 v386/v387 的 HEAD `7ae909f4c` 独立 worktree 上重跑全部失败节点：只有 3 个 validation_tier 用例在 HEAD 上通过（v386 引入，见下），其余 51 个在 HEAD 上同样失败，属本 lane 既有问题。
- v386 引入：`tests/test_validation_tier_script.py` 的两个 `_Fence` 替身缺少 `guard`，新门禁访问 `fence.guard.store.coordination_binding` 抛 AttributeError。修复替身为未登记主机（`coordination_binding=None`），不放宽生产访问。
- 既有问题 A（28 个 `test_mandatory_acceptance_actual_runner_chain`）：夹具仅按模块加载时的导入闭包复制源文件，漏掉 runner 在函数内延迟导入的 `workflow_coordination`、`source_preservation` 等，子进程 ModuleNotFoundError。修复：`_RUNNER_LAZY_MODULES` 显式列出 runner 路径的延迟导入模块参与闭包发现；复制的仍只是已提交源码。
- 既有问题 B（14 个 setup ERROR）：默认 pytest basetemp 过深，复制仓库时长需求文档名超过 Windows 路径上限（`Filename too long`）。环境要求：DEVX-015 回归必须使用短 `--basetemp D:/Work/devx015-<run-id>`（与既有 Codex 运行一致）；代码不改。
- 既有问题 C（原生 Full 链路，约 12 个）：测试内子进程防挂死时限 120s 低于实测（mandatory acceptance 源码身份哈希 ~170s），超时后连带 teardown `LEASE_EXECUTION_NOT_TERMINAL`。修复：`test_fixed_candidate_actual_runner_result_survives_main_advance` 与 `test_actual_mandatory_xdist_runs_inside_full_job_and_records_custody` 的时限改为 600s 并注明仅为挂死保护。
- 既有问题 D（R01 `test_r01_original_source_job_replays_and_rejects_changed_payload`）：真实并发缺陷。lease 仲裁锁为非阻塞；活体 worker 在每次效果前的准入读取与同请求的 REPLAY_ONLY 观察各自短暂持锁，任一方遇到瞬时重叠即 `LEASE_ARBITER_BUSY` 失败——worker 失败会杀掉有效执行，重放方失败则误报。修复：新增 `ExecutionLifecycle._live_execution_arbiter_window`，仅用于 `_require_worker` 两次准入读取与 `recover` 的首段只读检查，遇 BUSY 在 `LIVE_EXECUTION_ARBITER_WAIT_SECONDS=10`（间隔 0.05s）内重试，超时仍按 BUSY fail closed；不抢锁、不改内核 `store.atomic` 与其它调用方"BUSY 即返回"的语义。代表用例 1 PASS / 518.91s（与 v359 记录的 625s 同量级）。
- 五类代表用例均串行、短 basetemp 验证通过；全部原失败节点的确认重跑见后续记录。未执行任何原生 RegRenameKey。

### v387 DEVX-015A 主机登记锚点改为父键单值（2026-09-25）

- 背景：2026-09-25 00:10（0xBE）与 01:06（0x3B）本机两次蓝屏，调用栈均为 `NtRenameKey → CmRenameKey → CmpReferenceSecurityNode → CmpKeySecurityIncrementReferenceCount+0x1c`，两次都发生在 DEVX-015 回归运行期间，触发者为原生测试 `test_enrollment_native_registry_key_publication_preserves_payload_and_destination`（经 `publish_registry_key` 调用 `advapi32!RegRenameKey`）。第二次发生在 TaskStop 之后 18 秒，xdist worker 未被及时终止。任务 `DEVX-015A_HOST_REGISTRY_SINGLE_VALUE_ANCHOR_V1` 与 owner 决定 `owner_decision:DEVX-015A:2026-09-25:single_value_anchor_v1` 已在 main 登记；需求文档 `docs/requirements/DEVX-015A_Host_Registry_Single_Value_Anchor.md`。S1：本 lane 未提交改动均为 v386/v387，归属 DEVX-015，本任务在同一 lane 实施。
- 实现：`HOST_REGISTRY_KEY` 改为受保护父键 `SOFTWARE\AITradingSystem`，锚点为其下单个 REG_SZ 值 `WorkflowControl.RegistrationV1`；删除 `publish_registry_key`、prepared 键与全部 `RegRenameKey` 绑定。读取端：父键或值缺失返回未登记；值存在严格校验；遗留 `SOFTWARE\AITradingSystem\WorkflowControl` 子键经只读探测即 `HOST_REGISTRATION_LEGACY_KEY` fail closed。登记端：同一受保护父键创建方式与 `assert_protected` 门禁不变；父键只允许此一个值且无子键（否则 `HOST_REGISTRATION_ANCHOR_CHANGED`）；值不存在才写入并 flush，读回逐字节比较；已存在且相同幂等，不同则 `HOST_REGISTRATION_CHANGED` 且不覆盖。已知差异：检查后写入不是跨进程原子（RegRenameKey 的目标存在拒绝是内核级），由管理员独占写权限、arbiter 串行登记与读回比较 fail closed 约束，验收时评审。`_publish_cutover_registration` 的状态切换沿用同一键与值名，语义不变。
- 测试：`test_enrollment_single_value_anchor_protocol_with_isolated_io`（无值/写后崩溃仍完整可见并幂等/已存在相同/已存在不同拒绝不覆盖/遗留子键拒绝不写入）、`test_enrollment_native_single_value_anchor_write_readback_and_delete`（仅 HKCU 新鲜测试根的单值写入、读回、陌生值与子键拒绝、删除；不做任何键重命名，前后 HKCU 测试根清单一致）、`test_no_registry_key_rename_api_is_bound_anywhere`（静态守护 src/scripts/tests/tools 不得绑定 RegRenameKey/NtRenameKey）、读取端故障测试新增 legacy-key 并把 missing-value 改为未登记。定向 13 PASS（串行、无 xdist），HKLM\SOFTWARE\AITradingSystem 仍不存在。
- 历史章节 v271–v274 关于准备键与 RegRenameKey 发布的描述保留原文，自本节起被取代；system_flow 同步更新。未写真实 HKLM/ACL，未执行主机登记；两棵崩溃残留测试键（HKCU，已导出 .reg 至 `D:/Work/AITradingSystem_backups/2026-09-25/`）由 owner 处理。本机禁止再执行任何原生 RegRenameKey。93/106 不变。

### v386 执行账户范围正式确认与原入口受保护Full门禁（2026-09-24）

- Owner决定 `owner_decision:DEVX-015:2026-09-24:worker_account_scope_full_only_v1`：独立受限执行账户 `AITSWorker` 只用于测试和 Full；源码候选、安装、本地发布三条协调流程继续以 owner 账户在原 Job containment 下运行。这正式确认 v297–v357 的实际方向：v298–v300 只把 worker capability 接入原 Full 适配器，v301 明确不授予 worker 对控制 store 的修改权限，v355–v357 组合已安装启动器、注册 worker 登录与一次性交换目录；三条协调流程的 worker（`_require_worker` 的 `store.atomic`、`fence.checkpoint`、`record_created_object` 等）从未迁往受限账户。v296 记录的接线矛盾因此不作为待办。剩余风险：源码候选中的生成器以 owner 账户运行仓库代码，受 fence/lease/候选绑定约束但不受账户隔离。执行 harness 为 Claude Code（agent_harness=claude_code）。
- 缺口：原入口 `run_validation_tier.py full` 在主机注册后仍可不经受保护启动器派发 Full，账户隔离可被绕过。修复：主机控制状态新增可选字段 `full_execution_policy`（`COORDINATOR_JOB` | `PROTECTED_WORKER_REQUIRED`，缺省为 `COORDINATOR_JOB`，非法值 fail closed）；正式注册写入 `PROTECTED_WORKER_REQUIRED`；`_validate_publication_transaction_for_full` 在任务承诺与验收绑定之后、readiness 与 FULL_DISPATCHED 认领之前，若声明为 `PROTECTED_WORKER_REQUIRED` 且非受保护启动器调用，则以 `FULL_PROTECTED_LAUNCHER_REQUIRED` 拒绝，不消耗 Full 认领。现有合成主机夹具未声明该字段，行为不变，已映射的 L01 等原生 Full/发布用例不受影响。
- 新增 `tests/test_devx015_protected_full_entry.py`：策略缺省/合法/非法、控制状态非法值在 fence 构造时拒绝、四种组合（声明+普通入口拒绝；声明+受保护入口放行；COORDINATOR_JOB 与缺省放行）且事务与 dispatch claim 不变。focused 首轮 6 PASS。本轮在冻结 lane `03d10b4`（HEAD `7ae909f`）上实施，LANE preflight PASS（BASE_DRIFT_DEFERRED_TO_INTEGRATION_PLAN）；canonical 任务行、生成物与正式验证在整合事务中完成。
- 旧根盘点变化（影响 v382 的旧锁迁移前置）：GOV-007 于 2026-09-24 经 owner 授权，用新增的 `scripts/architecture_arch005_sibling_lease_store_migration.py` 把主 checkout `D:/Work/AITradingSystem` 的目录式 arbiter 迁移为文件式（migration id `gov-007-main-checkout-os-arbiter-20260924-v1`，receipt SHA `fa9914ac…94b1`，453 个 lease head 不变，旧目录保留在 `arbiter-migrations/<id>/legacy/`）。因此 v382 的 10 个目录式旧锁现为 9 个；主 checkout 现为唯一 coordinator。`D:/Work/AITradingSystem_devx014_source_preservation` 的 store 在 2026-09-23/24 被 GOV-007 发布事务使用，事件数已超出 v382 快照。L03 迁移执行前必须按 v382 的要求在锁内重新盘点，不沿用旧快照。
- 不改变：93/106 映射、L03/I05/X05 待办、协调 worker 的 `_require_worker` 语义、v369–v385 的 L03 切换实现。主机上 `AITSWorker` 仍禁用，未安装新运行时、未写 HKLM、未改 ACL。

### v294-v295 原启动器显式worker token前置（2026-09-22）

- v29614PASS23.58s（XML67388c938412e4f9bde0f172f00bf5eb0936c5a6c04d8a814636b90792bd14f7）：1314故障不回退、Job/stdout清理，原生AsUser路径和既有Job/custody回归通过；已canonical登记。v294 XML610fc28e90c6d89057971a7508affb70553a8fb49bdbae04debeca7ffeaad43d；v295 XML257786a395a83239dcec777c2823ea14d0dfc05478624baa7f0ed12ebc869508。单模块strict mypy补齐factory成员/ctypes句柄类型后PASS；没有放宽运行时检查。

- 原WindowsJobProcess新增create_as_worker，使用CreateProcessAsUserW并保留原JOB_LIST、HANDLE_LIST、CREATE_SUSPENDED、显式env和清理；没有凭据输入/持久化或普通子进程回退。WindowsWorkerToken只复制真实primary token、校验受信调用者给定SID与非elevated，持有非继承句柄并绑定PID/thread/session；调用侧必须从受保护配置取得SID和令牌。字典/关闭能力/同principal拒绝，跨session不继承句柄。
- v2948PASS17.52s：实际当前primary token复制、原句柄关闭后存活、非继承标记、异线程拒绝、wrong SID/impersonation拒绝且无泄漏、缺失/JSON/同SID/关闭能力无Job/stdout，以及原token/Job正向回归。不是实际AITSWorker启动。
- v295初轮1PASS7.66s：真实CreateRestrictedToken和CreateProcessAsUserW、创建时Job/stdio、挂起/恢复/退出清理通过；broker SID的区分明确用观察值注入，原生执行实际仍当前SID的受限token，不替代两账户原生接受。后续加入权限不足1314不回退故障用例待运行。Ruff及workflow_execution单模块strict mypy（follow-imports=silent）PASS。
- 下一必须解决原worker直接OpenJobObject/query及FileExecutionLeaseStore写入与新权限隔离的接线矛盾；不能为了兼容给worker控制store写权限，也不能忽略命名Job查询句柄会延迟KILL_ON_JOB_CLOSE的生命周期。当前账户保持禁用，无安装服务、权限变更或ACTIVE。仍93/106及全部最终门禁未完成。

### v292 独立执行账户方向与启动身份前置（2026-09-22）

- v2929PASS21.40秒（XML019a44305e31bdd898ac5eb5a8fe774f4a5b9ed457bcc6281e13a14a42ea4bcc），原生token/独立.NET对照/无句柄泄漏及拒绝/清理覆盖通过；v29311PASS23.21秒（XML8b5bcdb355a8a19bed9575e7323cd42e13434081a19ef4aa4f7d62a32270ef18），现有Job继承、目录custody、非成员拒绝、父退出清理通过。Ruff PASS，已登记canonical结果。下述预登记中的待执行测试已完成，但真实管理员/异账户启动未验收，20项focused不新增完整映射。

- Owner选择独立受限执行账户，且只授权第一阶段账户/合成目录权限验证。实际canary创建SID尾1010并已禁用；原失败记录保留，两项rename返回完整HRESULT 0x80070005，只因IOException分类缺失误报。其余8项符合预期。该合成令牌证据不代替跨进程、旧句柄、正式可信入口、崩溃恢复或L03。
- 实际OPS080 92路径已由原V2保全为c42ec8fda204989580306e1b7d930d0f3ada8dda，独立验证通过，原HEAD/索引/引用/源码不变，保全租约RELEASED。回执绑定实现10ac47c，后续实现变化不能重解释该历史绑定；不等于S4/S5或最终C。
- 原WindowsJobProcess使用CreateProcessW继承调用者进程token，故直接以管理员运行原启动器会使候选继承权限。本轮在原启动器增加primary token读取、创建任何Job/stdout前拒绝elevated和三个系统服务SID、实际挂起子进程token一致性检查；仍通过原清理关闭失败子进程。非管理员不等于独立账户隔离，后续仍需显式worker身份接线、受保护运行时和原控制协议。
- 新v6原publication事务为devx-015-execution-identity-20260922-v6，lease-7b7f3729c1edd9a82c0c，TASK_SOURCE_PRE_WRITE；canonical预登记和LANE preflight PASS。原生令牌读取/独立.NET对照/句柄泄漏、注入高权限观察值的无副作用拒绝、实际挂起子进程的身份漂移注入清理，以及既有Job回归待执行。注入场景不声称真实管理员正向。
- 保持93/106，真实可信执行、旧入口退役、原集成review更新、最终required tiers/Full/publication与OPS080 S4/S5仍未完成。不改主机账户、HKLM、实际目录ACL或PIT调度。

### v283-v284 V2 固定策略入口与提交身份前置（2026-09-21）

- 默认 arch_005_source_preservation.yaml/V1/64文件保持；新增固定 arch_005_source_preservation_v2.yaml/V2/92文件，仍16MiB且不自动拆分。通过原CLI --policy选择，严格路径/schema绑定，任意路径及两方向版本替换均拒绝。原 implementation binding 绑定选定策略实际已提交字节；共用原lease/store/ref namespace，无新执行入口。
- V2 recovery task为DEVX015，引用既有V3 owner closure decision；不能据此修改主机权限或触发production。范围review位于docs/architecture/devx015_mixed_source_preservation_v2_review.md。旧v4通过原release保留FAILED/RELEASED，加入新策略精确范围的v5已ACQUIRED→TASK_SOURCE_PRE_WRITE，lease-22c42a2869e5c8d5697a；preflight PASS。首次acquire因重复传入自动附加resource路径在acquire lease前被拒绝，修正参数后通过，保留两次输出。
- v28317PASS76.41s，XMLc98dd9137d299c6136ef637cc4444e83d1e694660d8c010fe3421833f8ab38d8；覆盖固定locator、两版policy/module/CLI/loaded-root/helper-root绑定正反例、非法schema及V2保全/重封伪造。v28416PASS98.02s，XML25b522e4d3f86b67421cc6ac6c910b26deb523695818d78a4758db6622d28c83；覆盖当前compatibility闭包新增策略、false binding拒绝、8项V2请求拒绝及默认V1 raw行为。Ruff/scoped diff PASS。
- 新增 test_committed_v2_cli_preserves_92_paths_and_independent_process_validates：实际ROOT/当前提交/原CLI preserve与独立validate子进程，绑定V2策略，无identity monkeypatch或copied implementation。当前未提交，尚未运行此正例；与既有V1 committed E2E一样，必须source commit后及最终Full实际PASS，不能将skip当接受。
- system_flow与当前V3兼容性来源声明已同步；正式generated authority尚需原顺序重建。准备source commit仅保存工程实现，保持IN_PROGRESS，不把commit等同final C或新映射。仍93/106，I05/L03/X05、真实OPS080保全及S4/S5、最终required tiers/Full/publication未完成。

### v281-v282 保留检查阶段的树查询批量优化（2026-09-21）

- 原 _tree_entry 委托 _tree_entries，每批最多16个声明路径，只读取 Git metadata；检查精确路径、唯一 regular blob/mode/OID与明确缺失。每次 _state 和独立 _verify_snapshot 都重新读取，不跨阶段缓存；原 check-attr 时序、raw bytes和历史/完整tree校验不变。
- v281 3PASS193.16s，XML3502450828682e44482350a2c5f105eda2416426fc38075179c5e8ba6006130d：33文件跨批次与不同commit/缺失/目录拒绝、92路径保全+独立重验、混合新history与重封伪造均通过。v282 V1 raw、排除文件poison、捕获后drift及filter拒绝4PASS120.42s，XML264042b586a41a49c6d4b9398900e73c7a15b84cc5ea0d1220a68f97f4a40f35。Ruff/scoped diff PASS。
- 同一92路径仪表比较：Git调用3321→2203，ls-tree1196→78、63.034s→4.126s；capture+independent validate178.063s→119.141s，缩短33.09%。这是合成92路径阶段测量，不是实际OPS080或整套Full加速结论。没有修改属性检查以进一步缩短时间。
- 原会话65991/69251均exit0。未改变实际V1策略、OPS080源码或主机权限；保持93/106和所有未完成验收。下一推进真实V2策略范围/合同与提交身份接受，不继续无边界微优化。

### v277-v280 V2 混合源码协议与 92 路径规模验证（2026-09-21）

- 原 SourcePreservation 入口增加 opt-in V2 policy/request/receipt/event/validation 版本绑定；实际 policy 仍 V1，未修改 OPS080 源码或启用真实保全。V2 显式绑定 ADD/MODIFY/DELETE、基线 mode/object、raw bytes 或删除不存在证明；原 lease、身份、完整 dirty scope、配置前后检查、历史追加及独立树验证保持。新增普通非 executable 文件；拒绝 canonical history 删除和大小写绕过。
- v277 4FAIL/4PASS48.54s 是合成 trusted root 缺 Git marker，原环境门禁提前拒绝；用实际 init/config/commit 修复夹具，没有放松生产检查。v278 4PASS83.50s；v279 12PASS187.51s，涵盖新 canonical history、两种全部重封校验和的伪造、V1 正向/历史重写/状态拒绝，XML27f92f54f205bc8c84a2e05e65eba47fb4dcbbfb970d1113266897ea77965019。
- v280 10PASS245.17s，XMLb7ec67f3ba84022db4256d97f6b2f52f4b238dc633a7096924652a98f3493e1e；包含同 OPS080 形状的44修改/31新增/17删除、8项请求拒绝、非法 policy schema。真实 Git capture/独立 validate 后 source HEAD、branch/remote refs、real index、文件保持，租约已释放。合成内容不等于实际14.7MB源码；implementation binding 明确模型化，不是已提交 CLI 身份接受。
- 92路径仪表记录3321次 Git 调用、保全/验证178.063s，其中1196次 ls-tree 63.034s、835次 check-attr 42.375s。下一评估每次检查内的批量读取，不跨阶段缓存身份/文件事实，不删除门禁。原 v280 session94766 exit0；v280 preflight PASS。system_flow 已同步 opt-in 边界。
- 保持93/106 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED；未增加 I05/L03/X05 映射。实际 V2 policy 不在当前事务声明范围，后续须经原范围/合同流程；真实保全、最终候选 required tiers/Full/publication 与 OPS080 S4/S5 未完成。

### v275-v276 混合源码保全的只读树校验前置（2026-09-21）

- OPS080原READ_ONLY preflight仍PASS、HEAD9489d807f2fb6fd8795ccbadb62e7983b96a3709，无ACTIVE；92个精确非排除dirty路径实际为44修改、31新增、17删除。现存raw bytes14747407，未超过16MiB。92路径全部在原v5-source事务声明内；原publication replay PASS、FAILED、3events，closeout FAIL/RELEASED。没有打开或hash排除研究文档，没有重开旧事务或改OPS080源码。
- V1 max_files64不是唯一限制：原保全入口仅支持tracked unstaged modifications，原tree_entry要求既有blob。因此不能靠调高max_files处理该输入；当前策略/请求/回执仍是V1，没有启用混合保全。
- 原SourcePreservation._expected_tree保留默认replacement-only，增加显式内部allow_path_changes只读树oracle，支持新增普通blob、绑定既有blob的删除、空目录剪枝和Git目录排序；拒绝缺失删除、带对象的删除、mode变化、文件祖先及path/descendant重叠。只读取tree metadata，不读取未声明blob，不写Git对象/index/ref。该私有能力不授予保全准入；未改变外部CLI/schema/dataflow，system_flow无需变更。
- v275 preflight/canonical preregister PASS；真实Git private-index作为独立期望值producer，对比修改+新增+删除、完整目录/全树删除、空文件新增，以及7个拒绝对照，11PASS14.55s，XML e28b17e81a4225e305dfb6cd059af9da87990e8e8463c150a9670774e7e952ce。生产校验只用cat-file tree，原索引/private oracle index/refs/object count均保持。
- v276含原V1保全/linked/幂等/未来源码变化/请求拒绝/未暂存状态拒绝/留存证据篡改/自洽伪造回归35PASS620.24s，XML0336300f01b91fcec259b7fdd9e559dda55de589a85d2d961254d8bb9b664509。Ruff及限定diff PASS。耗时集中原保全+独立重验case（约43-51秒/例），新11项短；无证据证明新oracle引入耗时回退，不删除原安全检查以提速。
- 仍需版本化mixed request、基线/不存在证明、原lease内capture、snapshot、独立receipt重验及真实当前候选接受；本轮不把tree oracle当整套保全，不新增93/106映射。L03与管理员隔离/ACTIVE、I05/X05、最终C required tiers/Full/publication及OPS080 S4/S5仍未完成。原v276 session63155已exit0，无活动测试。
### v271-v274 初始化准备/发布恢复与真实 RegRenameKey 实证（2026-09-21）

- 改为先准备完整对象再发布：计划显式包含 .aits-enrollment-<目标路径摘要> sibling preparation_path，原生保护目录创建后先写完整不可变 journal，再用 MoveFileExW 无覆盖发布正式 root，保持 device/file_id。原 arbiter 只在正式 root 初始化，准备容器不成为第二 store 或锁。目录创建后日志前中断可用原始仍匹配现场的 plan 重入；已有完整日志可由原 recover 入口从 preparation_path 继续。未知目录/错计划/重封漂移仍拒绝。
- 注册算法先构造 SOFTWARE/AITradingSystem/WorkflowControl.prepared-<registration摘要> 保护键，检查仅有预期值、写入并 FlushKey，最后用原 RegRenameKey 无覆盖发布为 WorkflowControl。新路径不产生空正式 key；重复恢复仅接纳完整相同注册。历史来源不明的空正式 key 继续 fail closed，不以新计划覆盖。原 native admin token/ACL/父项保护门禁保持；未修改真实 HKLM/ACL。
- v271 22PASS46.18s（XML5a9df42076dfcc05362df5e4250c0555c27a62758e72540e8eec1e11c3132520）；v27211PASS9.51s（f6156f043280c53e9afa9df3dab2a08f442b2c7d0151dd1153f5a79c8e98f85a），原准备/日志/正式root发布前后恢复，保持旧 ACTIVE 事件；注册协议 I/O 模型覆盖准备键空/完整、发布前后中断、不同竞争目标/未知正式空键/篡改准备键。模型不冒充管理员实证。
- v2731PASS8.09s（7adb227304d26c122c8f58601d2842e5a938f6f6332abc745b8067a1d8b20bba），原 publish_registry_key 的实际 RegRenameKey 在新 HKCU UUID 下验证完整值随键发布、已存在目标不能覆盖、竞争者值保留。无 HKLM override/写入，没有 ACL 修改。原生目录发布也验证实际 identity 保持、目标与竞争目录均不被覆盖。
- v274受影响完整回归54PASS67.83s，XML fa9a875cabf4676a1ff73944fa284d3dbe3c67faf05bf047fb8fa19045e34a21；Ruff/限定 diff PASS。原生键测试使用与生产同样的 KEY_READ + KEY_WRITE、64-bit view。最新 fresh root Software/AITS-DEVX015-Test-f2f854de254147018c3421dee4dc638d 已按原 bounded helper 清理，前后原两 roots/18 rows 完全相等。原始观察已归档 outputs/validation_runtime/devx015-v274-native-registry-observation.json，SHA6eba04266f18f69e0bbb61bb3213a43580e7cccd100d6b231edd633bdc59e776；来源 pytest-19620/popen-gw0/test_enrollment_native_registr0/native-enrollment-rename-observation.json。
- 当前新计划 outputs/DEVX015-host-enrollment-plan-20260921-v274.json（当前 chat），摘要96f981698d26d54ab7d692b6c461b995fb6c8b93643cb9cdbf55c258fbe59ecc。正式 C:/ProgramData/AITradingSystem.WorkflowControl 及其计划准备目录均未创建；开发 store 仍只有原 v4 活动租约，状态 DRAIN_REQUIRED。原 host-inspection NOT_ENROLLED 已保存repo outputs/validation_runtime/devx015-v274-host-inspection.json。
- v274 canonical结果登记首次因notes中的字面管道符被 CELL_INVALID:cells[7] 拒绝，移除该格式字符后同一change-id登记PASS，未绕过校验。无运行session；原 v4 lease约04:50 JST到期，后续预检必须重核。
- 下一重点是原生管理员正向/跨进程崩溃接受、完整 writer/entrypoint inventory 与旧 binary OS fence、ACTIVE 切换。尤其须审实际普通 writer 的旧根及祖先 ACL/可重新创建路径与预先打开写句柄；不能仅把 root DACL 改只读就宣称永久 OS fence，也不在身份/祖先范围未明确时对当前用户施加广泛 deny ACL。
- 当前93/106 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变；L03七项、I05两项、X05四项及 final C required tiers/Full/publication、OPS080 S4/S5全部保留。参考原生API：[RegRenameKey无覆盖语义](https://learn.microsoft.com/en-us/windows/win32/api/winreg/nf-winreg-regrenamekey)。
### v265-v270 管理员 DRAINING 安装/恢复工程与真实目录句柄修复（2026-09-21）

- 原 CLI 新增 control-enroll/control-enroll-recover；实际 TokenElevation + Administrators 成员检查在请求路径访问前执行，无自动提权/调用方 admin 布尔值。新 root/管理文件创建时使用管理员与 SYSTEM 写权限、AU 只读、protected DACL；保护目录句柄覆盖祖先并绑定 handle volume/file_id。root/日志/状态写入采用受保护 staging、flush、无覆盖 rename；临时 staging 从不充当 authority。
- 持久期望状态日志 host-enrollment.v1.json 绑定原计划、实际 root、policy/markers、locator/retirement/state/registration；原旧/新 arbiter 负责串行化。重放只完成同一 DRAINING，保留旧 ACTIVE/原 event bytes，新的 store 不复制事件；HKLM anchor 最后写入并由原 binding 读回。SOFTWARE/AITradingSystem 父项不存在时由同一管理员入口保护创建。没有 ACTIVE 代码分支，没有安装旧 binary OS fence，没有运行实际主机安装。
- v265 8PASS19.27s；XML 3a936f72c2f3718b4e4f456c268de89b7ff593a9f032e07f03478cd1e658f106。真实普通令牌拒绝；模拟管理员 transport 的 journal/state/locator/retirement/anchor 五边界恢复与幂等验证。后者仅协议测试，不是原生管理员正向验收。
- v266 39PASS/1FAIL57.31s；XML e0b91bfd8fd20862e7354409d1d7c1bc305d23b72c9cb5d641553c1e7af2fc7f。真实 native 目录 rename 实证揭示 FILE_READ_ATTRIBUTES-only 不参与所需删除共享约束。修复为 FILE_LIST_DIRECTORY|FILE_READ_ATTRIBUTES 后，v267 1PASS8.20s（XML e6aa627de05953d9c1f1e972746fe28ebf61bb223a93fb2c063ed176c3157e2b），原生 rename 被拒绝且句柄释放后可改名；随后增加 GetFileInformationByHandle 身份核对。
- v268 原 enrollment/cutover/drain/retirement/managed-host 40PASS56.62s；XML0872256846dd574fb9fa3f56844c42f99162eb50dd3349751da56006b278e258。v26912PASS24.95s（f88dafd6d994b758ee35266eee4955c18608d2fc092ea78bd24c8d7abe467f7a），补 root-before-journal 有限拒绝；v2703PASS9.51s（301dc1dbd60204ac2629f1cf5a8a261dd3f404c78f143a3445810b2bd22967af），真实 token 拒绝、原生 SDDL/DACL/目录句柄、当前 ProgramData 与 HKLM SOFTWARE 父 ACL 只读检查通过。目录/注册表均没有 ACL 修改。
- 当前 ProgramData owner SYSTEM，protected；HKLM SOFTWARE owner Administrators，protected。其 inheritable CREATOR_OWNER 是父级占位 SID，新子 key 使用 protected DACL 不继承；原生检查仅对明确允许继承的注册父项容许该占位项，对管理根/文件仍拒绝普通 writer。主机只读 control-inspect 仍 NOT_ENROLLED。Ruff、限定 diff 与 v270 canonical result PASS。
- 原生日志前中断或空注册 key 不能无证据推断归属：ENROLLMENT_PREPARE_INCOMPLETE / HOST_REGISTRATION_INCOMPLETE 保留原物，不创建第二 authority、不覆盖未知对象。这里仍是未完成的 bootstrap recovery 边界，下一应完善可审阅的原生恢复协议；真实管理员正向、完整参与集合/普通 writer 身份、旧 binary OS fence、ACTIVE 切换与完整七项 L03 仍未验收。
- 官方 API 核对依据：[CreateFile 分享权限](https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-createfilea)、[RegCreateKeyEx](https://learn.microsoft.com/en-us/windows/win32/api/winreg/nf-winreg-regcreatekeyexw)、[CREATOR_OWNER SID](https://learn.microsoft.com/en-us/windows/win32/secauthz/well-known-sids)。原生模型不可冒充实际管理员安装证据。
- 93/106 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED 不变，未新增 L03/X05 映射；final C required tiers/Full/publication、OPS080 S4/S5 保持完整目标。无运行 session。原 v4 lease-6a13b2890b3e8cda9b36 仍使用，约04:50 JST到期，下一阶段先核验。没有变更旧两 HKCU 残留，没有触发业务 daily。
### v263-v264 首次登记只读计划公开入口（2026-09-21）

- 新增原 CLI control-enrollment-plan：原 repository_identity 验证 origin/sentinels，记录 checkout/common、明确 legacy roots、拟定 control root/parent 物理身份、原 policy 摘要、全量原 lease replay/head events 和实际 OS executor 排空结果。拒绝重复/重叠/alias roots、相对 root、非空目标、损坏账本、已登记主机；不创建目录、arbiter 或注册表。
- 结果明确 CALLER_DECLARED_NOT_HOST_EXHAUSTIVE、snapshot_atomic=false、activation_allowed=false；plan hash 仅用于审阅绑定，不是执行权限。ACTIVE/未终态/真实 Job 存活 => DRAIN_REQUIRED，否则仍 ADMIN_INSTALLATION_REQUIRED。管理员保护、完整参与集合、旧 binary fence、锁内重核及 durable cutover/recovery 尚未实现。
- v263 preflight、preregister PASS；13PASS25.48s，XML 91b545373630752443bc8137fa773da6abd2987123bcb36b53e5ca01dfc076d9。v264 添加真实存活 Job 前后证据，10PASS21.71s，XML 77bb71c4efab476b358f1658682f1345e5bc0a296f3eba566c7387974a3d2410。测试通过原 subprocess CLI/Git 身份/原账本，不替换身份或注册表门禁，完整 fixture 文件字节前后相同；空/ACTIVE/live Job/损坏账本/路径与仓库身份拒绝均覆盖。Ruff/限定 diff check/canonical result PASS。
- 原公开入口已在当前开发 store 只读运行：353 events、唯一 ACTIVE lease-6a13b2890b3e8cda9b36（当前 v4）、无非终态 execution，历史记录 Job ABSENT/process EXITED。拟定 C:/ProgramData/AITradingSystem.WorkflowControl 未创建，尚非已授权安装目标。计划输出当前 chat outputs/DEVX015-host-enrollment-plan-20260921.json，摘要90e4afce6a4a8f9eed8b667c3d171db429bcd7eb613481e81f46ddca2dc209f3；这只是声明的开发 store，不是整个主机或 OPS runtime 迁移清单。
- docs/system_flow 已同步入口与边界。93/106 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED 保持，不映射 L03/X05。无活测试进程。下一实际管理员安装、旧入口 OS fence 和持久切换恢复仍须工程实现；不能把当前 v4 ACTIVE 当异常清理。正式 final C tiers/Full/publication 和 OPS080 S4/S5 未完成。
### v261-v262 迁移排空检查覆盖 publication 子进程（2026-09-21）

- v261 真实 Windows Job 复现：父进程已退出、独立 native handle 确认子进程存活；publication/nested 两项未拒绝，direct/installation 两项通过，共 2FAIL/2PASS 13.29s。XML SHA256 cfdf9b29969b3aafed885c2ae44ca3b29d867c7719ce0da96a0275ed9851fba6。
- 原 _cutover_execution_observations 仅遍历顶层和 installation；现递归遍历 installation_attempts/publication_attempts，每个节点仍使用原 observe_job/observe_process 与原 CUTOVER_EXECUTOR_NOT_DRAINED 门禁，无缓存、无迁移激活授权。
- v262 四结构真实进程测试及原 cutover/drain/retirement/managed-host 回归共 18PASS27.21s；XML SHA256 a94758fa8c812a1e90dd59874980e3483fdaab1e5ac19ec000308ea3e0f83f27。preflight、canonical task-source、Ruff、限定路径 diff check 均 PASS。
- 此处为 OS 排空观察器前置缺口修复，不是完整合法 publication ledger、管理员迁移或 L03 完整验收，不新增映射；93/106 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED 保持。
- 下一步：在原公开入口实现并验证管理员迁移、旧 binary OS fence、持久 cutover/recovery；实际主机此前只读结果为未提升、NOT_ENROLLED。尚未修改 HKLM/ACL 或触发 OPS080。最终候选 required tiers/Full/publication 与 OPS080 S4/S5 仍待完成。v4 原事务有效性每阶段重查；当前无运行测试 session。
### v257-v260 真实发布闭环通过、重放性能优化落地（2026-09-21）

- v257 session21552最终exit0，1PASS2670.79s（44m30）；XML SHA256 7b0ea76233220cff144d1094f1b44570c1ed573b703a0288adbaa9eaf028386e。真实Full PASS256.48s/74PASS235.67s，public LOCAL_PUBLISHED exit0，独立REPLAY_ONLY/LOCAL_PUBLISHED exit0，无第二dispatch。原Git/worker均return0/JobEMPTY。47事件仅lease-75891d2325136a01975b曾ACTIVE，唯一publication PID59240/creation_time134343944677786796，原租约RELEASED；peer资源冲突且原worker仍活。目录D:/Work/devx015-v257/popen-gw0/test_original_publication_cli_0，保留唯一证据至归档且无依赖后治理清理；没有remote发布。
- L01.linked_worktree_publication映射已增加，三项L01都有真实节点。当前93/106，PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED保持；新增node collect-only 1/1.12s。剩余13：I05两项、L03七项、X05四项，另有最终候选/全部正式验证/Full/发布及OPS080 S4/S5。
- 复核发现移出锁后须保留Git检查前writer身份/迁移phase门禁。v258原序列1FAIL8PASS10.83s，明确未准入writer先到Git；已在原decorator acquire分支前补_assert_writer('compound')，准入与最终atomic仍重复原门禁。
- 重放提效：FileExecutionLeaseStore.replay当次parse已完整验证每个event，私有_replay_lease_events仅复用这一次完整验证并检查全部转换边；缺失execution仍走完整失败门禁。公开replay_lease_events接收外部对象时仍完整校验。没有跨事件/跨调用缓存，也不缓存现场身份/哈希/授权。
- v259原kernel、门禁顺序、锁范围/HEAD与main漂移、正确hash但fields/identity/removed篡改正负例共24PASS27.14s，XML ac4ceb72565b3c776ab38f2dcf7abe565b1accc8702a4afab5a92a01473ad635。v260真实contained exit/incomplete result/launcher crash recovery、installation history、native registered competition、publication concurrency17PASS51.16s，XML66073daa54d13331de6e9cbb2a9a9224f4a000dd972f109d7e9867d6a49bdad0。Ruff及限定diff PASS。
- 实际代码交替基准：原v257 kernel source SHA771fb56f2e7477702c34e64898342e09380c1e9d13e19065efd679abf371fa96；同一47原事件旧19.894/20.479s、新12.286/12.530s，完整LeaseReplay对象相等且事件字节未变，局部约38%改善，不等于端到端发布加速。诊断脚本/结果在当前chat work/devx015-v259-replay-benchmark.py/.json。之前纯Path factory缓存约5%收益，未采用。
- v258 preflight PASS，v258/v260 canonical登记均PASS。当前无运行session，repo输入可在原合法事务下继续。下一L03基于原生注册夹具补七变体，不能把_registered_control的monkeypatch覆盖直接映射为原生验收；相关helper在tests/test_devx015_workflow_coordination.py native_full_host_registration约352、_exercise_native_registry约1971。当前v4事务lease-6a13b2890b3e8cda9b36仍用于原task，新的正式阶段必须重查。原旧两个注册表残留仍保留，不能再要求用户手工删除。

### v253-v257 发布仲裁锁范围诊断与修复验证

- v253 preflight PASS、v252失败已登记。短OS锁复现1FAIL/9.81s，证明publication acquire外层锁覆盖工作区Git检查，阻塞实际store execution_worker_admit。
- acquire现在由原CheckoutLeaseGuard串行准入，耗时检查不再持有外层锁；最终事务写仍使用原store atomic并重核HEAD/main，并发重放验证原不可变参数，不释放已落盘事务的同一租约。没有新增锁、队列或放宽worker断言。
- v254 2PASS/1FAIL14.81s：探针误延伸release检查，修正探针仅用于acquire。v255原并发/等待/取消/有限竞争及锁范围9PASS106.93s，XML e980b8bcfa8e203e8dc55983beebcc2fff30102aa583cfcc7c939eb791112c70。v256 HEAD/main漂移2PASS12.24s，XML 6f57f42b16d3792e2f04f0b4890bc2416d342a2558d05c1be229dc53870108fd。Ruff/限定diff PASS。
- v257 已canonical预登记；唯一根D:/Work/devx015-v257，原native-linked publication单节点，16/loadfile，原Full→真实发布竞争→ff-only→独立recovery全部保持。目录owner DEVX015，证据归档且无运行依赖后治理清理。未运行前不计PASS，当前92/106保持；OPS未执行。

### v252 终态失败：原生初始化通过，真实仲裁冲突暴露

- session58323 exit1：1FAIL747.21秒，XML SHA170ef1338158eeddf437b72d3332fe872c54c8f14cb0f4b25d57c0ef4403c325。FullPASS301.39秒，74PASS274.29秒，比v247250.12秒约慢20%。
- 真正publication worker stdout已不再HOST_REGISTRATION_REQUIRED，现为LEASE_ARBITER_BUSY；父original-publication-cli也exit2/LEASE_ARBITER_BUSY。peerPUBLICATION_LEASE_CONFLICT/detail LEASE_RESOURCE_CONFLICT，worker83748/creation_time134343930779797500请求后EXITED。不计成功竞争。
- 原recovery exit0/STABLE_FAILED_ATTEMPT/ORIGINAL_UNCHANGED。所有日志保留D:/Work/devx015-v252/popen-gw0/test_original_publication_cli_0；下一canonical登记结果，诊断真实仲裁锁持有者/临界区，不能仅放宽observer或延长等待重跑Full。相关parallel_control_kernel.atomic约1155，lease_arbiter OS锁非阻塞。
- 无运行session，repo输入可在合法预检后修复。当前92/106保持，整体验收未完成。v252结果尚待canonical登记。
### v252 修复候选src bootstrap后的真实publication重跑

- v251短测结果及v252计划已正式登记devx015-v252-preregister.json，preflight PASS。真实候选src/sitecustomize在原生fixture拥有路径中声明并随候选冻结；生产worker固定环境不改。
- 唯一D:/Work/devx015-v252，XML outputs/validation_runtime/devx015-v252.xml；保留至归档且无依赖。运行中冻结输入，原Full后实际local-publish/竞争/恢复全部必须通过，原v249/v250失败保留。92/106不预先提升。
### v250/v251 候选src原生bootstrap验证

- native_full_host_registration在原bootstrap生成后，把相同bytes写入仅一次disposable candidate src/sitecustomize.py；canonical native fixture owned paths明确加入该路径，随后会随实际Full candidate提交冻结。生产PYTHONPATH=root/src和身份门禁未修改。
- v250廉价probe失败1FAIL11.92秒，XMLfc2e6c197e03f568bdcdd79f5342b5c82b73759eef5315eb34f89f71639bb9d8：轻量repo没有候选package，固定src后误加载已安装旧实现并返回legacy store。失败保留。
- 修复轻量fixture先复制真实ai_trading_system包到src，子进程断言实际__file__位于候选src。v2511PASS14.93秒，XMLb662dd302469946849cfb1514f8ecc8ff4d6f1831df3df3ac01ae2da5d6943f9，真实固定src环境解析shared store。Ruff/限定diff PASS，registry仅原两历史根。
- D:/Work/devx015-v250和v251保留至归档且无依赖；v250预登记完成，v251为同一启动输入修复短重试，结果待canonical同步。无运行session，尚未重跑昂贵publication。下一先登记结果/预检，然后运行新native publication参数，检查候选新增sitecustomize纳入scope后的真实Full/profile门禁。92/106保持。
### v249 终态失败：真实publication worker丢测试native bootstrap

- session8031 exit1：1FAIL671.13秒，XML SHA12c574ea1f6dd2e17f5dbf59585dacd6d9aba75463f899300685506e8c332396。FullPASS286.68秒，publication CLI exit2/RECOVERY_REQUIRED worker returncode1；recovery exit0/STABLE_FAILED_ATTEMPT/ORIGINAL_UNCHANGED，旧两registry根无新增。
- 实际publication-d61ccff3171c16019a4d0dcfe6ea4412185eed10f23c6f768aecd867dc154950.stdout明确WORKFLOW_CONTROL_HOST_REGISTRATION_REQUIRED。生产workflow_coordination.py约3785将worker PYTHONPATH固定root/src，外部native-host-bootstrap不继承。父CLI修复不覆盖真实worker。不能放宽生产身份或环境冻结门禁。
- observer worker78332/creation_time134343920057991150从RUNNING变EXITED；peerPUBLICATION_LEASE_CONFLICT实际detail为LEASE_ARBITER_BUSY，不计通过。所有CLI JSON/worker log保留D:/Work/devx015-v249/popen-gw0/test_original_publication_cli_0。
- canonical结果devx015-v249-task-source-result.json。下一先设计候选提交前test-only native bootstrap在实际固定src启动路径的覆盖并廉价实跑该路径；保留真实模块/登记检查，不先重跑Full。候选新增sitecustomize是否满足fixture范围需核对原scope/commit身份，不修改原生产模块去接受环境覆盖。
- 无运行session，92/106保持，整体未完成。
### v249 实际publication竞争测试已接线，待执行

- v248映射结果正式登记devx015-v248-task-source-result.json；92/106保持。v249已canonical预登记，新增原test_original_publication_cli_ff_only_and_independent_recovery的native-linked参数，保留原Full/public local-publish/真实FF/独立recovery完整断言。
- RUNNING publication_attempts选择真实publication进程（不误取Full），前后observe_process存活，peer公开acquire资源冲突，仅允许精确intent；原发布尝试数/FF/恢复终态不放宽。观察异常先等publication完成和原recovery，再传播，保留terminal释放机会。
- 发现_v03_cli覆盖PYTHONPATH会丢原生bootstrap，已对存在的测试native-host-bootstrap/sitecustomize.py显式前置该路径；普通fixture保持。Ruff/diff初次PASS，最后异常传播小改尚需再检查。
- 尚未运行v249。下一先廉价核对native CLI bootstrap保留/节点收集，然后启动唯一D:/Work/devx015-v249；真实发布可能比Full长，跟踪原stage不重复启动。无需重新跑v243/v247。
### v248 L01两个Full变体映射闭合，92/106

- 留存v243/v247各20个事件经原parse_lease_event/replay_lease_events重新验证PASS：各只有一个ever-ACTIVE lease、一个process identity、terminal active0。v243 lease-da174600153e4c59f487/PID50720；v247 lease-54bdb30180bfcc0c013c/PID40872。配合实际Full完成、live拒绝、受保护产物/index/refs检查，映射linked_worktree_full及two_managed_repositories_host_full到对应实际测试参数。
- manifest现92/106，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED；实际publication变体仍缺，L03/I05/X05及最终C/Full/publication/OPS080未完成。两节点collect-only明确-n0非执行，2 collected/1.45秒；限定diff PASS。未为映射重复昂贵Full。
- canonical预登记devx015-v248-preregister.json；结果同步见当前requirement，最终task-source结果尚待登记。下一补真实publication执行竞争，不重复已完成Full证据。
### v247 独立仓库实际host Full竞争 PASS

- session83103终态0：1PASS491.96秒，XML SHA5e85962d84353135e47b0ecc34d7046b34d1f43861ece66bec3c88223029b61e。内部74PASS229.51秒；summaryPASS250.12秒，SHAb69be889746a052cba56d099c7800d7cfec1b05082dccd4af087d34fe65b3525。
- D:/Work/devx015-v247保存native-contender-scopes.json和native-live-full-contention.json，双方publication scopes不同、资源交集仅host Full；executor40872/creation_time134343906554359826在公开竞争前后RUNNING，冲突lease54bdb30180bfcc0c013c。仅精确申请intent，无其他output，refs/index不变；原Full成功、原failed未发布release后无active，registry仅原两历史根。
- canonical结果devx015-v247-task-source-result.json。无运行session。下一审L01 mapped variants是否满足全部oracle，再补实际publication/L03；当前90/106尚未改，不混同项目最终Full/发布或OPS080完成。
- 总491.96秒、Full250.12秒，未见新异常慢阶段。v245失败保留；v246修复防止非main clone输入缺口导致昂贵重跑。
### v247 修复main后的独立原生Full竞争重跑

- v246结果与v247计划已正式登记devx015-v247-preregister.json，当前v4 preflight PASS。仅重跑独立参数，使用已通过廉价非main源分支回归的修复。
- D:/Work/devx015-v247及outputs/validation_runtime/devx015-v247.xml保留至归档且无依赖；运行期间冻结输入，核对资源交集仅host Full、实际executor存活、公开冲突拒绝、受保护输出无变化和原Full正常收尾。原v245失败保留，不提升90/106或项目最终接受。
### v246 clone main 修复通过；v245已终态

- v245 session61150 exit1：1FAIL493.76秒，XML SHAfb03865056c5a582d3a971a22d0506fa2e9bdede3c85e292333ab05ce381466f。原Full driver exit0，无teardown错误；竞争因独立clone缺本地main未触达资源冲突，不计PASS。
- helper在独立clone之后读取原refs/heads/main；若peer本地main缺失，仅从该真实SHA创建，并再次核验精确相等。廉价测试强制原checkout处于非main分支后clone，复现原缺口。
- v246 1PASS10.96秒，XML SHA094ab81b3bee629be112a89fb2ace15ace0b8545099db3a4758f4b65ceb1837a，Ruff PASS。D:/Work/devx015-v246保留至归档且无依赖；无生产修改。下一正式登记结果后运行修复后的独立实际Full竞争，不复用v245为成功证据。整体90/106保持。
- 无运行session。v246正式结果尚待登记，预登记devx015-v246-preregister.json已完成。
### v245 独立仓库实际Full竞争预登记

- 复用v244原生独立登记及v243实际Full observer，仅执行新增independent参数；不同common identity的publication scopes分开，所申请资源交集必须精确等于host/<host_id>/full，并保存native-contender-scopes.json。
- 原Full运行期间peer公开acquire应冲突，无额外受保护效果；Full正常收尾后failed未发布释放。D:/Work/devx015-v245及XML outputs/validation_runtime/devx015-v245.xml保留至归档且无依赖，owner DEVX015。不是项目最终Full或发布验收，不预先提高90/106。
### v244 独立仓库原生Full资源前置 PASS

- 原生注册helper新增independent_contender opt-in：独立clone --no-hardlinks，保留原origin身份，不写生产registry。两真实子进程均解析共享root；真实binding断言common identity不同、publication scopes不相交、host Full scope相同。
- 1PASS10.20秒；XML SHA71919fd676dc1381528826254eb23deeb47d8436c11b1cdd30c276ec420eec48，Ruff/限定diff PASS，registry仅原两根无新增。D:/Work/devx015-v244保留到归档且无依赖。canonical结果devx015-v244-task-source-result.json。
- 已确认publication acquire总是申请publication和Full资源，linked topology两者都共享，因此v243不能独立证明host Full冲突。本轮仅登记前置，不增加90/106。下一canonical_merge_repository增加native-independent-full-profile-publish并传independent_contender，复用v243实际Full observer；运行中保存双方精确scopes及冲突lease。此后还须实际publication及L03/最终C/OPS080。
- 无运行session，未启动新Full。
### v243 实际原生Full期间linked竞争 PASS

- session29190终态exit0：1 PASS494.33秒，XML SHA325ec209683d8abfa4160aee8d5c31b8799e13a264dcb32da1e5b6cc5feb2862。实际内部74 PASS223.80秒；Full summary PASS244.34秒，SHA0506ba12545aa64c7c8a173ae439d09a2405baba117b29ef63588c673c80a75e。全部证据保留D:/Work/devx015-v243。
- executor50720/creation_time134343891638698445竞争前后RUNNING；peer原publication CLI拒绝PUBLICATION_LEASE_CONFLICT，仅产生精确申请intent，index/refs不变。原Full完成、原failed未发布release后无active lease，registry仅原两根无新增残留。
- 当前证明实际Full执行期间的linked publication准入互斥，不是peer实际Full公开请求、两独立repo Full竞争或实际publication执行；不提前补齐整个L01，不提升90/106。项目最终C/Full/publication与OPS080未完成。
- 耗时494.33秒总计，Full244.34秒，余约250秒为准备/门禁/收尾；与既有夹具相符。后续可复用同一次Full增加不同竞争请求观察，不能跳过强制门禁。
- v243结果已调用canonical登记，输出devx015-v243-task-source-result.json。无运行测试session；下一先审查Full contender公开入口所需真实候选绑定，避免再次只证明publication acquire。
### 2026-09-20：v243 纠正Full竞争观察及失败处理

- v242终态1FAIL+1ERROR/259.75秒，XML SHA7d9cf0c0f859019c4ad6e54d1c1b78a4694e0b1ac91e65617cb20e454cf735d6。真实executor79664/creation_time134343887730569857在竞争前后RUNNING，peer公开入口PUBLICATION_LEASE_CONFLICT；失败仅outputs存在及提前kill driver引发非terminal释放失败。原目录/事件保留，无新增registry根，不伪造terminal。
- 复核原详细L01合同规定无transaction/claim/launch/ref或push效果，原guard在判定前持久化intent。修正过宽的目录不存在断言为精确仅允许publication-live-full-loser.json一份请求记录；任何其他输出、index或refs改变仍失败，不调整产品门禁。
- observer异常先保存异常，等待原驱动完成并保存stdout/stderr，再传播；超时仍有界终止，不能把观察失败当作驱动成功。v243唯一D:/Work/devx015-v243，XML outputs/validation_runtime/devx015-v243.xml，owner DEVX015，保留至归档且无依赖。失败后输入变化因此允许一次重跑原实际Full竞争；90/106及最终验收不提升。
### 2026-09-20：v242 实际Full期间linked竞争执行

- preflight PASS、canonical预登记完成。增加native-linked-full-profile-publish夹具模式，沿用v241真实linked注册。实际Full helper新增可选live observer，默认路径保持。
- observer等待原lease execution RUNNING并由Windows PID/creation_time确认存活，再从peer执行原publication CLI acquire；要求PUBLICATION_LEASE_CONFLICT、原executor仍活、peer无outputs且Git refs保持。Full原检查继续完成，事务按failed未发布释放；本轮不执行publication，不声称发布验收。此限定取代初始预登记中Full后继续publication的实施安排，发布仍为必要后续工作。
- Ruff/限定diff PASS；唯一根D:/Work/devx015-v242，XML outputs/validation_runtime/devx015-v242.xml，owner DEVX015，保留到归档且无依赖。隔离fixture Full不替代项目最终Full，90/106尚不提升。
### 2026-09-20：v241 实际Full夹具的linked登记前置

- native_full_host_registration新增opt-in linked_contender，在第一个租约前创建native-full-contender linked worktree并登记其真实目录identity；默认路径保持。两个真实Python子进程通过既有native bootstrap分别从原checkout与peer解析同一control root，无registry或binding mock。
- single/linked两对照2 PASS/12.63秒；XML SHA256 5aeae41c17d3aeec8104d24031341e6af1481e317cc9049a1d27215932711c01；Ruff/限定diff PASS，测试后仅原两历史registry根，无新增根，control无lease events。
- D:/Work/devx015-v241保留runtime JSON、fixture及XML关联，owner DEVX015，归档且无依赖后清理。此为真实Full竞争的必要登记前置，不完成L01、不增加90/106、未运行Full。无生产变更。
- 下一实现：canonical_merge_repository目前只接受native-full-profile-publish，需增加显式linked模式传递linked_contender；_run_actual_profile_full目前subprocess.run阻塞，要在真实进程活跃时观察原execution事件并派发独立peer公开准入，证明失败原因是竞争且无输出/发布副作用。不通过mock PASS或marker运行代替实际Full。现有_run_actual_publication_fixture最终本地merge/push路径也须准确区分公开执行入口范围。

# DEVX-015：可完成、可恢复的工程工作流合同 V3

### 2026-09-20：v240 M04 原生共享 store 断言对照

- canonical cycle828预登记后执行原/M04两对照，2 PASS/15.30秒；XML SHA256 4ae7a6fb89167215e542ff939904ae9c3f42c788f649f99c5bbb83dd64d6012a。仅隔离子进程将coordinated_lease_store的root选择改为legacy_root，真实registry绑定和其他门禁保留。真实REGISTERED输出指向checkout/forbidden-legacy-store，原NATIVE_REGISTERED_STORE_MUST_BE_SHARED断言拒绝；正常共享竞争通过。
- before方法SHA5fbc5fad834f889130425bce0c223817f0687bdd6ef575b740a109dd5f88edec，after95d2e04a99d9ea1e376d50e3b746de7e86be2ba035ce74c78838d24c97ea6ace，module74370c2096be019749e02522b46743b3ca3c855b3d9578332ffc564a0a8f37c5。Ruff/限定diff检查通过，测试后仅原两历史registry根，无新增根。
- 证据范围是原共享store选择约束，不是两个执行者实际运行、实际Full/发布完成，不补齐L01或增加90/106。生产模块未改。D:/Work/devx015-v240保留driver/results/原变方法证据直到归档且无依赖后清理；下一步实际L01/L03执行验收。DEVX015及OPS080仍未完成。

### 2026-09-20：v239 原生共享准入竞争结果

- 四个既有原生竞争节点4 PASS/35.41秒，XML SHA256 d6c05248a56aeba0f192703b9af369d994581b738467715c4a9cb34c3671610e；cycle827正式登记。测试后AITS测试根枚举仅原两根，无新增根，未删除历史键。
- 此证据仅证明当前进程视图的共享租约/发布准入竞争，不是实际项目Full或完成发布，不补齐L01；90/106及最终DEVX015/OPS080验收仍未完成。D:/Work/devx015-v239保留四份真实子进程结果直到归档且无依赖后清理。
- 新shell未绑定checkout src时预检重放失败；显式PYTHONPATH绑定当前src后同一预检PASS，两份输出保留。后续命令保持显式源码绑定，不据初次环境错误判定authority损坏。

### 2026-09-20：v238 注册表执行视图纠正及原生夹具清理验证

- v238 1 PASS/9.96秒，XML SHA8fbaca2fb61b9d7c8246ba24c29c5af2f647063da4e195490ffc50a1761b4776。新UUID原生夹具完成registered/denied真实子进程路径且清理无残留；前后原两根共18键、mtime/值摘要完全相同。旧键未清理或改写，原新建范围退出helper可用；可继续明确视图的隔离原生测试，不提升真实host/deployment/最终验收。Ruff PASS。

- 用户桌面regedit与独立cmd均找不到原两测试键。进一步同一个Windows PowerShell可执行文件、同SID、64位、同完整HKU路径只读对照：Codex子进程两个键存在，独立进程两个键均不存在，Software正对照均存在。进程环境差异已实证；MSIX安装清单的Windows11新版RegistryWriteVirtualization仅排除Chrome NativeMessagingHosts，符合私有视图解释。物理hive仍因共享锁未读取，不声称已经清除两键。
- 撤回历史“桌面主机残留必须人工删除”判断。用户已要求处理该边界；原两私有根保留作为证据，不删除、不改ACL、不换接口绕过原删除拒绝。原no-new-fixture暂缓条件的前提被此次独立对照纠正：只先运行一个已存在的fresh UUID隔离原生夹具，明确限定当前进程视图；前后完整保留根清单/子键mtime/值摘要必须相同，无新增残留才可继续。任何新增残留立即停止，不把它提升为真实运营host或全体验收。
- v220既有bounded child-first cleanup只作用于本次先证不存在后创建的唯一新根；本轮新增外围观察验证，不改注册表生产逻辑。v238 owner DEVX015，唯一根D:/Work/devx015-v238、XML outputs/validation_runtime/devx015-v238.xml，原输出保留至归档且无依赖；无Full、真实HKLM、runtime、Codex配置/清单变更。
- 原v3过期后已通过原release以FAILED/RELEASED收尾，新v4 devx-015-registry-view-correction-20260920-v4与原scope相同、preflight PASS；不复活过期租约。诊断原脚本/三份观测在当前任务work/registry-view-diagnostic；用户交付记录在outputs/DEVX015-native-cleanup-plan-20260920.md。

### 2026-09-20：v236 M08 实际历史Git blob与自报哈希反证预登记

- v236四对照与原矩阵5 PASS/39.22秒，XML SHAb827241d1cbe34ebb2a5ff1dd12bd23b1664a6c9ed7e5208703947ae11c30095。原实现拒绝两个自洽伪造hash字段，M08实际接受而触发原DID NOT RAISE，恢复原方法后二者再次被历史blob校验拒绝；正确绑定始终通过，真实source/index/refs未改。
- 原方法SHA0df417da7abbf200d9bfa70d503b573b43ecb52ba6a51ccc61d74ef207ff76a4，错误方法SHAb44218fa986071f33428618c32662415105f509c288f6f12af688d27b74b0d6c。实际own blob/module SHA6d428cf9896db2c9721064ddd9a6a55a3a6ad1393cd6b57565e92c9e4af79e5f，helper config blob SHA95c2141759a40ccc7af34645ca2a128fb9058cd4d6bad5ad597ee1c4f83abac9，均不等于伪造零hash。完整binding/方法/反事实证据保留v236；生产未改，90/106不变，尚非最终验收。

- 复用原历史实现绑定九负例测试，新增精确选择own/helper blob hash。两个自报hash字段均设相同伪造值；独立真实cat-file必须证明与实际提交blob不等。M08仅删除_verify_implementation历史blob哈希对比，其余schema/真实Git对象/路径和当前执行校验保持；原typed拒绝未抛才计目标，正确绑定仍通过，恢复原方法后伪造绑定重新被拒绝。
- 四组对照加原九负例矩阵一次，限定历史code provenance目标，不替代所有receipt/status/最终验收。保存方法/module哈希、完整原/伪造binding、实际blob哈希，source/index/refs保持。v236 preflight PASS、原v3 scope；D:/Work/devx015-v236、XML outputs/validation_runtime/devx015-v236.xml，保留至归档且无依赖。无生产修改/Full/新registry，90/106及OPS080 S4/S5仍未完成。

### 2026-09-20：v234 M07 真实capture worker删除/字节丢失反证预登记

- v235两错误实现2 PASS/53.37秒，XML SHAee4838567fa03496680a607625156cb3bf4593a72a597a69c74623179c2348a2；原正例复用v234 PASS。实际tree750624ec964d74cad5354f8db3092351b7fa02e6保留应删除文件，tree89b3bfb3165b46b56f745397f21055b9fdfbef1b含CRLF归一化字节，原TASK_CHECKPOINT_SNAPSHOT分别delete not represented exactly/blob size mismatch拒绝。无receipt/ref，原source/index/config保持，两store replay PASS且无active lease。
- 原方法SHA492233b9d149d4946b8d3bc202cdf3b4b6512a16a160501cdc3c36c117d046b2；删除mutant方法SHA53366c39b0b5c338dc91dd33f015b5f03f5d3f4ec50695248b5e99e1d3e89c55，归一化mutant方法SHA4f93ccaf82dd33a85da089e3c576ec32f5370aa8f7979384b515a3e8ad968403。原/变更方法、实际module哈希、Git对象及worker日志已保留；生产未改，仍90/106，不代表最终验收。

- v234原1 PASS/2 FAIL/52.97秒，XML SHAe71fc85cc8b5ecd04a509356959d3d9138be13cdcf7d2223ba693421c9d9e85b。两mutant在plan即TASK_CHECKPOINT_DRIFT拒绝，尚未执行worker，不计kill。诊断复制模块write_text在Windows写为CRLF，实际committed blob也为CRLF，违反原COMMITTED_SOURCE_GIT_EOL_LF绑定；未放宽门禁。原source备份及失败fixture保留v234。
- v235仅改夹具变更源码以write_bytes明确LF保存，seed后廉价核对无CRLF且cat-file blob等于原字节，其他门禁保持。重跑两mutant，原正例结果复用；唯一根D:/Work/devx015-v235、XML outputs/validation_runtime/devx015-v235.xml，原失败证据不覆盖。

- 三组原/删除遗漏/CRLF归一化对照。只在隔离夹具seed commit前修改真实capture_worker的删除index entry或hash-object输入；真实实现身份、Job、原snapshot validator保持。原成功测试须通过；错误实现必须实际生成Git tree，被原TASK_CHECKPOINT_SNAPSHOT删除/大小校验拒绝，且独立cat-file证明残留删除文件或归一化字节。父进程仅替换loaded origin是既有夹具边界，不作为真实仓库身份验收。
- owner DEVX015，v234 preflight PASS、原v3 scope。D:/Work/devx015-v234及outputs/validation_runtime/devx015-v234.xml，保留原/变更方法/module哈希、真实Git对象、worker日志/失败证据到归档且无依赖。无生产变更/新registry/Full；90/106及最终候选/OPS080未完成。

### 2026-09-20：v233 M06 实际重复执行反证预登记

- v233两对照2 PASS/10.57秒，XML SHA256 8320e6e7eaca1c227f9d02ebd5c9c640a26e1779915a5907634c6e74767d795a。原执行一次、M06实际执行两次，命中原命名断言；两个独立store均replay PASS、无ACTIVE lease、RELEASED/RESULT_RECORDED，实际Job消失。原方法SHA4f4fcc93ed3e38c6883f527cd691f3b9b0a4a42a537940261dbff8d27a8767fd、错误方法SHAaed8ec995d7e466e1f809ec4bf2f46a3bdb855a020a28bafc6e194d723f3708b；实际module SHA74370c2096be019749e02522b46743b3ca3c855b3d9578332ffc564a0a8f37c5。Ruff PASS；不增加90/106映射或宣称最终验收。

- 原真实launcher崩溃测试EXIT_CONFIRMED边界、非source请求，原/错误实现两对照。M06仅在ExecutionLifecycle.recover原无完整结果收尾分支插入冻结argv重新派发；使用真实Windows Job保证隔离，真实进程resume/wait/EMPTY/close，保留原身份检查、terminal记录和release。第二次执行必须由原RECOVERY_MUST_NOT_DISPATCH_AGAIN断言捕获，并独立证实两个执行记录、不同进程身份、原argv一致及原lease正常终态。任意异常不计kill。
- 方法精确目标/compile前置，保存原/变更方法、module哈希、真实重复进程身份和反事实结果。该夹具含合成host/任务绑定，只证明实际进程恢复不重复执行，不提升为X05真实仓库/平台或最终Full验收。
- owner DEVX015，cycle816，v233 preflight PASS；唯一根D:/Work/devx015-v233，XML outputs/validation_runtime/devx015-v233.xml。保留至归档且无依赖后清理。生产模块不改，不建registry fixture、不运行Full；仍90/106，最终候选和OPS080 S4/S5未完成。

### 2026-09-20：v232 M10 实际Full后的真实远端观察反事实预登记
- v232完成1 PASS/745.98秒，XML SHA256 a9078ff589614f5dcdd024d0b2c7fd3c73e3f1c95f200dee01e3a3cafafecd7b；实际隔离Full74 PASS/242.94秒，summary SHA256 564f39dc974feefbb5b8b2e1f54a95f3531c0172c2269e23ce71410108f02ce9。真实candidate/local tracking为37eac22c0a372d3cd0e9ac8309bec583dbdf1b49，独立peer将remote推进c88c66943c5266ea9aaf8d5d788ee7e1e4e84a42；原观察读新remote，M10实际exit0/OBSERVED读旧tracking，被原REMOTE_TIP_MUST_MATCH_ACTUAL_ENDPOINT捕获。原summary/refs/index/lease/events未变，两次真实非force push断言通过。
- 原事务独立replay PASS/FAILED、原release完成，lease-7383cb17f6965160e6d8。m10-counterfactual/三次observation/loaded-method/terminal-replay保存于v232 fixture父目录。实际module SHA3aa83ed327e5aec595d446a64c48caa4b086e679e73ec477bcb2f355ae5ad62d，方法before639f440207ae0cb24c426f6babd3774ae6b80074226e684b1d11445cefdf7ffe、after520f10f460b4e5bf2976ada6b1dcd4120bf7dc1a23e1adecf70e8e68d1d5e136。外层v232-closeout preflight PASS；生产模块未改，90/106不变，不替代项目最终Full或OPS080 S4/S5。复用一次Full节省对照重建，未跳过发布检查。

- 复用full-profile-publish原完整advanced-remote链，仅一次真实Full、同一候选push与独立peer推进remote。M10将原_observe_push_remote唯一git ls-remote命令换成真实git for-each-ref读取本地origin/main；不伪造stdout/对象，保留其余endpoint/parse/原公开入口检查。方法形状与compile在Full派发前关闭，原/变更源码及实际加载hash保留。
- 正常观察须读真实新远端；同一状态错误观察exit0/OBSERVED却读旧C，并被原REMOTE_TIP_MUST_MATCH_ACTUAL_ENDPOINT断言捕获。原Full summary/refs/index/lease和publication events不变，无额外push；原cleanup因远端前进拒绝，原FAILED释放。真实Git trace必须仍仅候选push+peer push两次、无force。原政策identity不替换，无新registry。
- owner DEVX015，唯一根D:/Work/devx015-v232，XML outputs/validation_runtime/devx015-v232.xml，原Full/driver/observations/trace保留至归档且无依赖后清理。正式cycle814；全程冻结SUT/tests/manifest/docs且监控原阶段，不重启。此隔离Full不是项目最终Full，90/106与其它验收缺口保持。

### 2026-09-20：v230 M03 两个真实候选的绑定反事实预登记

- v231四对照及新增原task矩阵5 PASS/13.29秒，XML SHA256 `dc2a3f23747ad93757e9bf3de2cdb2adc08886ee80af18efefb686d87a2a4f9b`。错误实现实际返回旧commit绑定或把committed wrong-task输入绑定为当前任务，原typed拒绝未抛的断言捕获；当前有效候选正例正常，refs/index保持，原函数恢复。原方法SHAb73d70dae31a22b8db0cc2135fe2580a12569de1ce3e8e98902b92d72c9f7664，M03方法SHA85ed3013e7b1591271d14fc9ccd6bcc98fece64e6826c6eb784c5c2e2142a054；原v2309PASS/2invalid失败不提升。

- v230原9 PASS/2 FAIL/15.11秒，XML SHA256 `cc29c31be40034698d984a66d360a5c7401ebecf28623d94707f9824c3eb9c6d`。两个反证在实际注入前INVALID_MUTATION_TARGET拒绝：源方法在读取前后各有一次HEAD比较，单处假设错误；未触发mutant，不能算kill。原源码备份与结果保留v230。
- 完整审阅后v231固定精确两处候选比较和一处manifest task_id绑定，将三处作为同一个M03错误实现删除。各自原/错误对照覆盖真实同tree不同commit与实际committed wrong-task manifest；其余真实Git/blob/完整inventory检查保留，正确候选/任务正例仍须通过。原typed拒绝未抛才算目标，且需真实返回错误候选或将wrong-task输入绑定为当前task；不存在对象或任意异常不计。
- v231只跑四反证对照及新增原task矩阵节点，之前9个原矩阵结果不重跑。owner DEVX015，唯一根D:/Work/devx015-v231，XML/源码/真实返回绑定保留至归档且无依赖后清理。不改生产/registry/Full，scope仍原v3。
- 原candidate负例传全零SHA；仅删除HEAD绑定后可能因对象不存在被其它Git检查拒绝，不能将此误记目标kill。原真实Git inventory测试新增other-candidate：两个确实存在、tree相同但commit不同的提交，HEAD为新commit，请求旧commit。原实现须ACCEPTANCE_CANDIDATE_CHANGED拒绝。
- M03仅删除bind_mandatory_acceptance中HEAD/candidate比较，其他实际committed manifest/blob/106项inventory检查全保留。错误方法真实返回旧candidate绑定时，原pytest.raises的DID NOT RAISE才计目标反证，并独立核对返回旧commit与当前HEAD不同、refs/index未改；原/错误实现对当前HEAD的正例均须通过。仅隔离pytest worker内替换且finally恢复原函数，保存原/变更方法与实际module哈希、两个Git身份、真实返回binding。
- 原9项inventory矩阵加两对照，16/loadfile；owner DEVX015，唯一根D:/Work/devx015-v230，XML outputs/validation_runtime/devx015-v230.xml，保留到归档且无依赖后清理。正式cycle813、v230 preflightPASS；不改生产或registry，不运行Full/runtime。合成inventory仅用于候选绑定，不代表106项断言质量或最终M03/全体验收接受。

### 2026-09-20：v229 M02 真实冲突一律放行反事实预登记

- v229三种冲突原/错误对照3 PASS/65.34秒，XML SHA256 `b63f615ccc7b7a302a1f044441147c7a6aff92b7b0007c7c13510303ecd8220e`；三个原事务终态独立回放PASS/FAILED。每种原实现REVIEW_NOT_FROZEN拒绝，错误方法返回exit0/READY且无stderr，被原拒绝断言捕获；原refs/index/files/events在观察前后相同，不是import/API错误。
- 当前实际workflow_integration module SHA256 `e42790319e5bc0488c342284225c35438e2e919b42531bd4698c02fd10c4b66a`；原方法SHA `b365b42b646f9d5aee93df4ecd0dca6b5813e95750493c62ce468b08037ecdb9`，M02方法SHA `caea65160cd8645927cc7497a9e22aa58a417adc4a51243288394c728a6db23a`。没有生产修改或新registry，映射90/106不变。
- M10入口审阅：原observe_remote必须具有实际candidate及LOCAL_MAIN_FF_PRE或原REMOTE_PUSH_PRE历史，不能用缺失Full的虚构事务替代。其反证应接入已有实际Full/remote-advance链共用一次候选，而不是为每次观察重复Full，也不修改已保留的历史remote现场来凑通过；尚未实施或运行M10。
- 原text/clean-semantic/crosspath三夹具各先跑原公开入口拒绝，再在同一源码状态跑显式M02：validate_controlled_merge_plan仅将缺失review_ref分支改成READY_FOR_CONTROLLED_MERGE。原当前task/repo/plan身份、真实Git merge/独立语义消费不修改。原UNRESOLVED_CONFLICT_MUST_NOT_GAIN_PERMISSION断言必须捕获，实际mutant exit0/READY且无stderr才成立；任意异常不算kill。
- 原/变更方法、实际加载module hash、driver和两次CLI结果保留。三夹具共用各自原Git状态执行对照，避免六次重建；refs/index/files/config/lease events前后不变，原CLI文件不改，子进程runpy执行原路径/argv。生产代码不改，不运行Full或registry。
- owner DEVX015，唯一根D:/Work/devx015-v229，XML outputs/validation_runtime/devx015-v229.xml，同原归档/无依赖退出条件。正式cycle811、v229同scope preflight PASS；仍90/106，M02结果不替代其它mutants/16缺口/最终C及OPS080 S4/S5。

### 2026-09-20：v227 M01 已吸收源码一律拒绝反事实预登记

- v228原两对照2 PASS/32.00秒，XML SHA256 `13f0e0338df58987d0b6dc7c9844ffaf3778009d66c02ca9b865307b2839cfba`；原CLI源码未改，refs/HEAD/index/config/fixture字节保持。两个原事务独立回放PASS/FAILED，租约lease-56ef12fdb937b2c3efd9与lease-87c2618d8e0829b02b27正常原入口释放。M01在原目标断言被捕获，无bootstrap对照v227原节点PASS保留。
- 实际workflow_integration模块SHA256 `e42790319e5bc0488c342284225c35438e2e919b42531bd4698c02fd10c4b66a`，原方法SHA `0149ec836a8deec506b63ed190ce6c76ebaa651b3edeeae47adc675e3df63305`、M01方法SHA `33e1fac7aff41059e99ddfd69165339198c082cc32c532580d9b4c91e6286f07`；只在测试子进程替换已定位方法。v227两dirty现场及teardown错误不提升，原补FAILED回执保留。M01/M05本轮目标反证不增加90/106映射，也不代表最终候选或全部mutants已接受。
- v227原3 PASS/2 teardown ERROR/46.09秒，XML SHA256 `d1cbe7750f24877f2f5be09ba8ebae8a30ba4b897b212a957d1c21343bd1f152`。两对照断言通过，但改写fixture CLI不在原lease声明内，原释放正确报CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED。未提升此结果，原测试源码/注入CLI/现场保留；两原lease已RELEASED，经原publication release failed补齐两事务FAILED回执，不删除/改写dirty现场。
- v228改为子进程启动器只在内存安装显式mutant后用runpy执行原CLI文件，原argv/文件路径和所有identity门禁保持。fixture源码完全不改，原快照和CLI字节须前后一致。只复验两个受影响对照；v227原无bootstrap节点已过且不受此分支改动影响。owner DEVX015，唯一根D:/Work/devx015-v228，XML与原driver/结果保留至归档且无依赖后清理；无registry/Full或production动作。
- 原all-absorbed真实Git/公开CLI夹具，新正反对照复用原目标测试。独立Git逐项核对L/M实际mode/type/blob/删除相同；M01只在原_plan_from_authority的已吸收相等分支注入MUTANT_OVERLAP_ALWAYS_REJECT。原CLI脚本副本仅装载此显式错误方法，原identity/authority/registry检查不替换，生产文件不改。
- 原公开入口必须正常通过all-absorbed；错误版本仅能在原ABSORBED_SOURCE_MUST_NOT_BE_REJECTED断言处被捕获，并独立确认实际stderr含确切mutation reason。两者原refs/HEAD/index/config/fixture文件均不变。原/变更方法、fixture原CLI、实际加载模块hash与CLI结果保留，不能把任意运行错误计为kill。
- owner DEVX015，唯一根D:/Work/devx015-v227，XML outputs/validation_runtime/devx015-v227.xml，同原证据归档/无依赖后清理。正式cycle809；复验原未注入all-absorbed节点加两对照，不运行Full或registry。此为M01当前目标证据，不替代I05最终源码终态或全部mutants验收。

### 2026-09-20：v226 M05 忽略实际pytest失败反事实预登记

- v226四对照和M09回归5 PASS/16.39秒，XML SHA256 `31e2fd6fa78156dd9e8abcbff7c7cc3848bd6035dbff81919adc6cca593defc7`。实际pytest_exitstatus=1、call报告failed保持；M05错误结果exit0/PASS/空reasons，被原非零退出断言捕获。原完整选择及真实失败正例正常，M09既有目标仍捕获。原方法SHA2695c1bb9993e74e217269885000005f45dcc444fccee86c390a7d7d72b0a38e，M05方法SHA9cfb0f537b81b027a4b1431236ba2f4bd80cf9c36b93ae2f2188239886342ed1。生产源码未改、映射90/106不变。

- 在原M09小型实际xdist driver增加唯一显式M05方法体变更：原sessionfinish生成结果前清空failure reasons并将session.exitstatus置0，模拟忽略实际pytest失败。原pytest报告与输入不改，保留真实call=failed及原pytest_exitstatus=1；原外部非零退出断言须以PYTEST_FAILURE_MUST_REMAIN_FAILURE捕获false PASS，不能把任意异常当kill。
- 原实现/mutant分别对实际assert True/assert False做四对照；完整正例必须正常，错误实现实际失败却exit0/PASS才触发目标反证。另复验原M09 deselection-mutant分支，确认共用driver的原反证不受影响。生产源码不改，原/变更方法和实际模块hash、真实报告与解释器保存。
- owner DEVX015，唯一根D:/Work/devx015-v226，XML outputs/validation_runtime/devx015-v226.xml，保留至归档且无依赖后治理清理。正式cycle808，原v3 heartbeat PASS/expiry2026-09-20T10:02:31.387308+00:00、同scope preflight PASS。不增mapping、不运行registry/Full/runtime；原10mutants与16变体/最终C/OPS080全部验收仍必需。

### 2026-09-20：源码事务v2到v3的原入口过期收尾

- v225正式任务写回先被PUBLICATION_LEASE_EXPIRED拒绝，原lease-c17f4a8bb0f01ea4199c仍ACTIVE但expires_at=2026-09-20T12:52:21.167608+09:00、execution=null；原事务replay PASS/TASK_SOURCE_PRE_WRITE。没有续活过期租约或改写时钟/记录。
- 原publication release --outcome failed成功保存终态FAILED、lease RELEASED及v225 XML证据。后续事务devx-015-recovery-closeout-20260920-v3由同一原入口取得，lease `lease-5487a92f37a6f8eb8d51`，transaction SHA `ed551bf613d661da19849bd4dd7803dfa0048949ba67aaa7c339dc2940e4fdf5`；精确核对owned/shared paths、generators、required tiers、B/L/M、task/actor/thread全部与v2相同，原checkpoint进入TASK_SOURCE_PRE_WRITE，lane preflight PASS。
- 首次acquire错误地重传了原入口自动追加的两项资源，PUBLICATION_PATH_DUPLICATE在租约创建前拒绝；第二次只从CLI参数去掉这两项隐式资源，最终事务scope完全相同，不扩大或缩小权限。正式任务cycle807已补记M09和恢复结果；不创建源码替代工作树。
- 后续长步骤之间须在原租约仍有效时通过既有heartbeat入口维护并核对期限，避免再次把超期恢复当成正常进度；不放宽TTL或引入额外scheduler。历史注册表清理限制不变。

### 2026-09-20：v223 M09 实际deselection反事实预登记

- v224 4 PASS/14.01秒，XML SHA256 `5b9d6a57028f5059e807bf51b9197781a4965395bfa536115487b07eabfc488f`；v225精确剩余3节点3 PASS/15.80秒，XML SHA256 `c0bb0dbc345a1613982ebcbb1f51e8acbd54674970e371d577814ba81951c3b9`。结合v223原3个未受regex修正影响的正常对照，M09实际被原冻结集合断言捕获；v223原1FAIL及SHA `6e256af33acb7b7c34118435846ae9e13c04329c196ebbd4d12095c4d630efa0`保持。
- 原实现完整选择与mutant完整选择均exit0/PASS/保留required；原实现deselect exit1/FAIL/保留required；mutant实际两worker collection后exit0/PASS/required=[]/reports={}，原断言拒绝。加载模块SHA256 `fb44f3b3b2eccf89b1dba1e9e13834c6611ea4a10239ac6c0a79ac3756597d4b`，原method SHA `2695c1bb9993e74e217269885000005f45dcc444fccee86c390a7d7d72b0a38e`、mutant SHA `f1324c71e6723ca7d23ef894a54c935fafc9f54db89e079f2849564c1c132ce1`。不是API/import错误；生产源码未改，反证仅在原结果guard作用域，不替代最终候选/全体mutant或X05。映射保持90/106。
- v223原3 PASS/1 FAIL/13.52秒：完整选择两对照及原实现真实deselect通过；mutant确实返回exit0/PASS、required_nodes=[]，原MANDATORY_FROZEN_SELECTION_CHANGED断言触达。外层regex错误要求全文等于标识，拒绝了pytest合法附加的assert差异，属于反证harness失败，原XML和原测试源码保留在v223，不改记PASS。
- v224仅将错误匹配改为精确首行标识加换行/结束边界；仍要求AssertionError、实际mutant result为空必测且PASS/exit0、两worker collection，任意异常不算kill。复验原失败节点并验证受新增证据记录影响的原missing/skip/xfail/xpass/failure/collect_only六分支。原三个已过对照不受匹配修改影响，不重跑。owner DEVX015，唯一根D:/Work/devx015-v224，原XML/日志保留至归档且无依赖后清理；不运行registry或Full。
- v224实际4 PASS/14.01秒：M09目标断言及missing/failure/collect_only通过。focused命令的not pass关键词也排除了参数reason包含PASS的skip/xfail/xpass，故未覆盖计划全部六分支；不把4PASS当7项完成。v225从原实际collection取这三个精确node，不用关键词筛选补跑；唯一根D:/Work/devx015-v225，原证据按同一生命周期保留。
- 剩余16变体之外，原10个mutants也必须由目标断言杀死；不能把普通负例PASS当成错误实现已被检验。v223复用原test_mandatory_acceptance_real_xdist_result_guard，子进程实际pytest/2worker，保留原冻结required_nodes身份断言。
- 唯一M09变更在原MandatoryAcceptancePlugin.pytest_sessionfinish前将self.required缩成真实collection交集，模拟偷偷删除未执行必测项。只在隔离测试子进程替换此方法体，原生产文件不改；保存实际加载模块路径/字节hash、原/变更方法源码/hash、driver/stdout/原result。原实现和mutant分别运行完整选择与真实-k optional排除，共四对照；只接受原断言MANDATORY_FROZEN_SELECTION_CHANGED被捕获，import/API/fixture错误、未知异常或仅非零退出不能算kill。
- owner DEVX-015，唯一根D:/Work/devx015-v223，XML outputs/validation_runtime/devx015-v223.xml；保留到正式归档且无依赖后治理清理。v223 preflight PASS，原任务cycle806；不创建新registry，不运行Full/runtime，不新建通用mutation框架。该反事实是当前原runner作用域证据，最终候选及其他mutants/106变体仍必需。

### 2026-09-20：v221 收集审计发现旧映射，v222 精确节点修复

- v222三个原节点3 PASS/13.08秒，XML SHA256 `180b78d47b70c0e04063845febad5fe31afa2d6e5f3ab58260d557061d26d176`。修复后90项映射共153个唯一节点全部存在于原1,220节点收集结果，missing为空；被收集的测试源码未改，不重复collect-only。新审计devx015-v222-collection-audit.json显式绑定旧FAIL审计，不覆盖v221。正式验收仍未执行。
- 七个映射模块实际collect-only收集1,220节点/5.92秒/exit0，未执行fixture。映射152个唯一字符串中发现一个失效：X03.handle_inheritance仍使用未参数化的test_launcher_os_exit_kills_job_tree_without_inherited_job_handle，实际已为[popen]/[inherited-child]两个节点。原审计FAIL保留，collection SHA256 `388095fb0ad624c2b66cf1d14e26ca5bc781965de7392908ffbefb878c649104`，不把pytest收集exit0当映射有效。
- 修正为两个当前精确node，保留原nonallowlisted句柄测试；v222只复验这三个原节点，生产/测试实现不改，16/loadfile。owner DEVX-015，唯一根D:/Work/devx015-v222，原XML与观察保留至归档且无依赖后治理清理。映射仍90/106、16缺口；该局部收集审计不满足X05全体mandatory_collection_complete。

### 2026-09-20：v220 原生夹具退出清理合同修复预登记

- v220内存模型7 PASS/8.03秒，XML SHA256 `dc87407b4256507050b07da822c36b273fb3c63492f111abb169bd361d069fae`，Ruff和限定路径diff检查通过。两个原生fixture创建点均增加fresh-root检查并复用同一退出helper；原生接线尚未执行，仍不得宣称真实残留已清理。已有历史根未触及，映射保持90/106。
- 重新核对v56原native-runtime-check.json，returncode0/runtimePASS；故障在固定Software清单漏掉OS创建的System后代，删除非空根WinError5。v220仅修正未来fresh fixture退出：注册前核验唯一根不存在，恢复原HKLM映射后先完成精确根的全部只读inventory，再按子先父后删除；上限256键/16层，不合法根、枚举错误或超预算在任何删除前拒绝。
- 仅使用内存注册表模型验证OS额外子树、仅根、无效范围、枚举失败、宽度/深度超限及非法子键名；模型PASS不能提升为原生清理、L01/L03或Full证据。原两处残留不调用新helper，不重试被拒绝的动作；原生fixture继续暂停。无生产/运行态行为更改。
- owner DEVX-015，测试临时根D:/Work/devx015-v220、XML outputs/validation_runtime/devx015-v220.xml，保留原证据至归档且无依赖后治理清理。正式cycle803；当前90/106，最终Full/发布/OPS080 S4/S5未完成。

### 2026-09-20：v219 实际PID复用验证终态

- v219新增原生节点及两个既有身份回归3 PASS/33.82秒，无skip/error/failure；XML SHA256 `d87db43989f22401412f29e82ba14a660b6cc87f157f587bb4a26f3519e0795c`。新节点22.727秒，756次创建即取得实际复用，无需用满480秒预算。
- 原第0次PID66672/FILETIME134343491582663596已退出且Job/全部句柄关闭，第755次实际内核再次分配PID66672/FILETIME134343491808530141。独立Win32 Oracle核对两代创建时间与各自Job成员；旧tuple被原observe_process判REUSED，原observe_job拒绝为WORKFLOW_EXECUTION_JOB_IDENTITY_MISMATCH，当前进程仍活。当前身份RUNNING且解释器worker78356确实运行于新Job，原fresh observe_job终止返回EMPTY/active0，两个原生进程句柄均确认退出。756条记录均确认原Job关闭后不存在。
- 登记X02.pid_reuse/X03.process_identity_reuse，累计90/106映射、16未映射；原14 counters/10 mutants与PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变。该节点证明实际身份复用隔离，不代表全体lease迁移或项目Full/发布通过。原证据位于D:/Work/devx015-v219/popen-gw0/test_actual_pid_reuse_rejects_0，v218无复用原结果不提升；无生产代码/registry/runtime修改。

### 2026-09-20：v219 实际PID复用自包含验证预登记

- v218终态NO_REUSE_OBSERVED：512次/15.032秒，512条原Job关闭后不存在的独立观察，未增加映射。当前仍88/106、PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED。不把创建时间加一的负例提升为真实复用证据。
- v219只新增原生测试，复用原WindowsJobProcess与独立NativeOracle；最多16,384次或480秒创建窗，同时最多一个自有Job，原10秒关闭机制不变。上限是本次测试的资源预算，不是PID分配算法假设。实际旧进程退出且全部句柄释放后，只有再次观察到相同PID、不同FILETIME，才核验旧身份REUSED/拒绝终止，新身份RUNNING、真实解释器可执行及正常终止。无复用则以INSUFFICIENT失败，不能skip、自造身份或再扩大上限。
- 正式任务cycle801、原scope preflight PASS。owner DEVX-015，唯一根D:/Work/devx015-v219，原逐次观察/结果/XML保留至最终归档且无依赖后治理清理；不改生产代码，不创建注册表fixture，不运行Full/runtime。新节点通过后还须按原X02/X03 oracle审查映射，不自动声明完整验收。

### 2026-09-20：v218 原生PID实际复用有限探测预登记

- L01真实host注册仍受已知两处测试注册表残留清理的工具策略阻断；不启动新的注册表fixture。v218先探测Windows实际PID复用，不修改生产代码或身份判断。只用既有WindowsJobProcess逐一创建悬停进程，独立Win32 Oracle核验PID/FILETIME/Job成员；原Job收尾及所有句柄关闭后再创建下一个。同时最多一个Job，最多512次、60秒尝试窗（工程探测上限），原10秒有界关闭机制不变。
- 若实际观察到同一PID对应新FILETIME，保留两次原生观测，核验旧身份被observe_process判REUSED且不能终止当前Job，新身份仍能正常收尾；否则记录NO_REUSE_OBSERVED，不能用伪造tuple提升为实际复用。此为可行性探测，不直接增加X02/X03映射或替代完整验收。
- owner DEVX-015，唯一临时根D:/Work/devx015-v218-pid-probe，脚本在本任务work目录；逐次身份/终态及原件保留至最终证据归档且无依赖后治理清理。不运行Full、registry或runtime。outer同scope preflight PASS；当前88/106。

### 2026-09-20：v215诊断sidecar断言失败；v216便宜校准与v217重验预登记

- v217原完整节点1 PASS/608.68s，XML SHA256 `be44e05c90321df17c28b8fd2f5970447043a545d25dcad143d25e0d4b16ccf7`。实际Full74 PASS/235.63s，summary SHA256 `071b00bcb1d74ae3534253b1b2f2cdd6ded2f38f817438c3079768a75bd2d2e6`；候选C=`f4d0271be89a280f13e9ec6e30d6b265410a3a1e`，M=`ac89483f608c180894a83a506ed39fd912525203`。独立终态核对peer完整index mode/OID/stage确实等于M树，而peer HEAD解析为C；原scene保留，不冒充checkout一致。
- 四公开入口均exit2：inspect/publish为PUBLICATION_EXPECTED_MAIN_STALE，recover/adopt为LEASE_EXECUTION_PUBLICATION_MISSING。全部业务事件/Full/intent/ref/index/原锁anchor及peer私有字节不变；原failed release后replay PASS/FAILED/active leases=0。只有短arbiter诊断sidecar允许合法更新。v215原FAIL及v216便宜校准保持各自原身份。
- 登记P01.ref_updated_before_checkout，累计88/106映射、18未映射；本变体证明外部ref-only中间态不能被消费，原publisher真实崩溃后的稳定恢复由已有P01.checkout_recovery/v180单独证明。不是声称原publisher曾产生本次外部故障，不把保留的坏peer标成稳定发布，最终项目Full/发布/OPS080仍必需。PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED、14 counters/10 mutants不变。
- 耗时：本次内层235.63s低于v215240.43s，未见退化；12:23:07 LOCAL_MAIN_FF_PRE至12:23:16 FAILED约9秒完成公开拒绝与收尾。等待期间OPS080只读preflight PASS，HEAD仍9489d807f2fb6fd8795ccbadb62e7983b96a3709、92 dirty paths、active leases=0；原要求S4正式验证/集成/部署及S5新合法daily仍未完成，未改其源码或运行态。

- v216两项原公开入口校准2 PASS/12.92s，XML SHA256 `eac44c24ea8edf88e8cb6c184d0423c442f376a3407757b94f231ae4615a4ac0`。ACTIVE原租约无publication记录时，两命令均PUBLICATION_MISSING，所有业务状态及原arbiter anchor不变；原failed release完成。现在仅启动v217原完整节点，不重跑其它已通过生成/安装测试。

- v215原整体1 FAIL/610.91s，XML SHA256 `27e28903636493e3567530c48fa3e512e53080e375a7dbb445887f4e6056d6d3`；内层实际Full74 PASS/240.43s。原inspect/publish均EXPECTED_MAIN_STALE拒绝，recover为PUBLICATION_MISSING，但测试错误要求arbiter.owner.json也逐字不变。原fixture已FAILED/active leases=0，失败及原测试源码保留D:/Work/devx015-v215，不提升原FAIL。
- 同一已终态fixture的原recover重新校准：只有精确leases/arbiter.owner.json改变，refs/index/业务JSON不变，仍PUBLICATION_MISSING/FAILED/active0；原JSON位于fixture父目录v215-owner-sidecar-calibration.json。原ExecutionLifecycle.recover先进入短arbiter再检查publication，lease_arbiter明确该sidecar仅诊断；因此只修测试snapshot排除此单文件的字节比较，仍要求schema/actor/RELEASED，并新增真实arbiter.lock原字节和inode保留检查，所有lease/events/transaction/Full/ref/index仍完全匹配。
- v216为两项原公开recover/adopt在ACTIVE但无publication记录时的便宜校准，唯一根D:/Work/devx015-v216，XML outputs/validation_runtime/devx015-v216.xml；通过后v217原完整P01节点重验，唯一根D:/Work/devx015-v217，XML outputs/validation_runtime/devx015-v217.xml。均16/loadfile，owner DEVX-015，原fixture/CLI/XML保留至归档且无依赖后治理清理。outer同scope preflight PASS，无生产改动/registry/runtime动作。仍87/106，不将诊断文件更改解释成业务授权。

### 2026-09-20：P01 ref 已更新而旧 checkout 未同步的公开拒绝预登记

- v215创建原full-profile-publish受控fixture及唯一main peer，实际Full和LOCAL_MAIN_FF_PRE后先经原inspection正例，再以真实expected-old Git CAS把main从M更新至C，保留peer原HEAD文件/index/私有canary字节及身份：symbolic HEAD解析为C但index仍M。故障明确来自外部Git，不伪称原publisher产生过这一中间态，也不伪造publication attempt/receipt。
- 原公开inspection及local-publish须PUBLICATION_EXPECTED_MAIN_STALE拒绝；原recover及adopt-published须PUBLICATION_MISSING拒绝，不能只凭main=C消费中间态。全部保留原Full/intent/lease/execution/index和peer私有字节；原failed release到终态，无发布/恢复授权。保留故意不同步peer证据，不重置main、checkout私有内容或改写Git历史。原publisher中断后的稳定恢复继续由v180原证据单独证明，不能将本次外部注入替代它。
- outer v2同scope preflight PASS；唯一根D:/Work/devx015-v215（包含ref-only-main-peer），16/loadfile，1个真实Full原入口节点，XML outputs/validation_runtime/devx015-v215.xml。owner DEVX-015，原fixture/Full/CLI/Git与peer原件保留至最终证据归档且无依赖后治理清理。生产未改，不运行registry/runtime；当前87/106，结果和P01证据充分性核对前不增映射。此为隔离fixture实际Full，不是项目最终候选Full。

### 2026-09-20：X01 自包含结果原安装入口预登记

- v214原完整流程1 PASS/671.57s，XML SHA256 `bcadada61cc601b1a7fb21084de695d837b682296aa9df867fa36584b96f4140`。真实source候选 `00592debbf00b3d605351fade93fd72932d5db49` 的worker结果被伪报为main提交、自报PASS并带内部可重算artifact hash，原公开source-install仍在read_bound_json按原lease外部artifact哈希以REFERENCE_DRIFT拒绝；零新增安装Job/lease/event/ref/index副作用。恢复私有原件后同一source正常安装/交接到终态，fixture原failed release完成。登记X01.self_reference，87/106映射、19未映射；X01五个变体均有映射，但PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED及最终项目Full/发布/OPS080边界不变。
- 效率实测：worker四生成器183.68s、architecture89.54s，较v212的190.24s/94.51s略短；整体671.57s比v212689.54s短，但输入与对抗数量不同，不称受控加速。worker结果11:46:33至外层source CLI11:50:17约224s，已从原start_source_candidate代码确认父进程独立重算四生成器并核对Git对象，不能用子进程自报结果跳过。v212 architecture的1,297个输入/7,561次读取仅提示待测热点，不据此引入会弱化持续校验的缓存。

- v214复用原source Job四生成器/private S/原安装计划正例，只替换原worker_result.json：自报PASS，snapshot.commit伪报为另一个确实存在的main对象，再添加artifact指向结果自身，sha256按去除artifact的完整payload重算。明确校准内部自包含hash一致、实际文件hash不等于原lease绑定；原公开source-install必须REFERENCE_DRIFT拒绝且零新增Job/run/lease/event/ref/index/源码副作用。保留原件与伪造件，仅恢复测试私有字节，再走同一个原source正常安装/交接终态，不替换原lease或结果权威。
- outer v2同scope preflight PASS；唯一根D:/Work/devx015-v214，16/loadfile，1个原完整流程节点，XML outputs/validation_runtime/devx015-v214.xml。owner DEVX-015，原fixture/CLI/XML保留至归档且无依赖后治理清理。此次不重跑v212两个未变对抗或项目Full，生产未改，无registry/runtime；终态前86/106。

### 2026-09-20：X01 历史改写原公开 source 准入预登记

- v213 5 PASS/110.35s，XML SHA256 `de72fa6fe7d3f14fc8786f5e090d5b488784d3a904ab40debcda9c9913477d1a`。原prepare正例后重算旧TASK_REGISTERED事件链、fragment和index，当前authority/scope不变且原canonical validator通过；原公开source-candidate仍在inspect_canonical_merge_outputs以WORKFLOW_MERGE_CANONICAL_EVENT_HISTORY_CHANGED拒绝。零source Job/run/execution/新lease事件/ref/index副作用，原failed release终态FAILED、active leases=0。登记X01.old_canonical_events，86/106映射、20未映射，PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变；自包含hash尚未覆盖。
- 此次只跑受提取helper影响的四项inventory回归及新增公开入口，未重复完整source生成/安装或Full。原CLI、改写前后两文件与XML保留，失败历史不重写。

- v213复用source-job真实canonical仓库与原review冻结；先经原prepare正例，再只改原TASK_REGISTERED历史payload，并用原canonical renderer重算全部事件/fragment/index/views，保持当前task projection/review不变。内部validator必须通过，而原source-candidate公开进程必须CANONICAL_EVENT_HISTORY_CHANGED拒绝且零Job/run/lease/event/ref/index/源码副作用。保留改写前后canonical原件及公开CLI，最后原failed release；不从内部helper异常直接宣称入口验收。
- outer v2同scope preflight PASS；唯一根D:/Work/devx015-v213，16/loadfile，新增原入口节点及提取helper影响的4个原inventory节点，XML outputs/validation_runtime/devx015-v213.xml。owner DEVX-015，原fixture/CLI/XML保留至归档且无依赖后治理清理。先做便宜准入，不重复已通过的完整生成/安装或Full；生产不改，不运行registry/runtime。终态前85/106，不登记self_reference。

### 2026-09-20：X01 原 source Job 生成证据与安装闭合预登记

- v212原完整流程1 PASS/689.54s，XML SHA256 `447c670fc2fac81c99faf8df5bf868f96b4809083f491db10df077bdc24495d6`。同一真实source候选 `d0de4209a53ff38850a3b04dc8ebafb7f0675c20` 的旧canonical index capture被原公开安装入口以SOURCE_CAPTURE_CHANGED拒绝；仅替换manifest被REFERENCE_DRIFT拒绝，均exit1且无新安装授权/Job、lease/event/ref/index副作用。恢复测试私有字节后原正常安装及交接通过。登记X01.stale_outputs、manifest_only_replaced，85/106映射、21未映射；仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，不登记old_canonical_events/self_reference，不是项目最终Full。
- 提效：两项对抗与正常安装共用一次真实四生成器source Job，避免重复生成；生成阶段190.24s、architecture子阶段94.51s，相比旧v197整体约14–20%变慢，样本源不同，不能据此断定回归或跳过检查。保留原阶段日志供后续定位。

- v212复用原source-job完整四生成器/私有双parent提交/公开安装/最终交接测试链。在原成功source Job及真实candidate S之后，先经原安装计划正例，再把一个确实已更新的canonical输出capture换为原main旧blob，另一次仅替换generation.json为顺序反转的合法JSON。原公开source-install应分别SOURCE_CAPTURE_CHANGED/REFERENCE_DRIFT拒绝，零新lease/event/installation目录/Job或源码/index/ref副作用；每次仅恢复夹具制造的私有字节，保存原件与替换件及CLI输出。随后同一原source候选正常安装/交接到原终态，不创建替代source或重跑生成。
- outer v2同scope preflight PASS。唯一根 `D:/Work/devx015-v212`，16/loadfile，1个原完整流程节点，XML `outputs/validation_runtime/devx015-v212.xml`。owner DEVX-015，保留fixture/原私有generation/capture/S及对抗副本、CLI/XML至最终证据归档且无依赖后治理清理。生产代码不改；不运行Full、registry fixture或runtime操作。终态前仍83/106，不借此登记old_canonical_events/self_reference。

### 2026-09-20：ref ABA 原 Full/公开发布入口验收预登记

- v211原端到端节点1 PASS/647.75s，XML SHA256 `c59fa46ed1c31a579a7f59602116fff79ef962a26d616897cae08deaa837b64a`。真实候选 `63af45bed5d7f300e6fe081e6f5d47534427f16c` 的Full内层74 PASS/256.15s，summary SHA256 `cc574c6f031e3a5c886db116a658fd72e6ff4e1761a28366222af36ae35eaecc`；不是项目最终Full。
- 原inspection先OBSERVED；真实Git M=`f8223e7344e1a57454614087c23e262938a442e5`→N=`8b7686b572aaa3135a8804151d6f78b7314480e4`→M后，原inspection及local-publish均exit2/PUBLICATION_LOCAL_INTENT_CHANGED，原lease/execution/Full/intent/candidate/index不变，无publication attempt。原failed release终态FAILED、replay PASS、active leases=0，保留两条reflog，不改写任何历史；此FAILED是预期未发布收尾，不是测试或Full失败。
- 登记X02.ref_aba，累计83/106映射、23未映射，PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED、14 counters/10 mutants不变。现有生产保护有效，本次只加原入口测试和证据，不新增恢复/执行框架。
- 阶段观测（JST）：FULL_DISPATCHED 11:01:17.798、FORMAL_VALIDATION_RESULT 11:07:05.020、LOCAL_MAIN_FF_PRE 11:08:21.690、FAILED 11:08:40.117。内层256.15s接近此前同类244–274s，结果后至LOCAL_MAIN_FF_PRE约76.67s主要为原profile准入复核；本次无异常长阶段，不用进度100%替代Full终态。原fixture/回执/XML保留至归档且无依赖后治理清理。

- v211复用原full-profile-publish fixture与实际Full/Job/全部readiness链，在原LOCAL_MAIN_FF_PRE后先经公开inspection取得OBSERVED正例；独立原Git以expected-old CAS执行M→N→M，确认两条真实reflog、main当前值回到M、candidate/index/Full/lease/原intent不变。随后原公开inspection及local-publish都必须因原intent历史/对象绑定变化typed拒绝，零新增publication attempt或执行授权。最后原failed release完成，保留两条历史，不回写ref/log/receipt，不以当前SHA相同冒充原拓扑未变。
- 原outer v2同scope preflight PASS；唯一根 `D:/Work/devx015-v211`，16/loadfile，1个原端到端节点，XML `outputs/validation_runtime/devx015-v211.xml`。owner DEVX-015，原fixture/Full summary/CLI/ABA字节保留到最终归档且无依赖后治理清理。不运行native registry fixture，不把合成仓库实际Full称作项目最终Full。生产代码不改，先验证现有原公开入口的历史保护；仍82/106映射。

### 2026-09-20：main 准入后竞态原入口预登记

- v210原5个main-advance节点全部PASS/33.61s，XML SHA256 `108ab151101ae73027f3e3f8e227f8fc7a40ce2f25c032d69e70b00f9fe93344`。完全相同argv/environment的Git竞争在checkpoint期间被拒绝，独立native probe WinError32；原checkpoint进程退出后同命令成功，HEAD/index保持原值。结合v209五个未改正例与v208十二个未改其它回归，登记X02.main_toctou，共82/106映射、24未映射，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，不替代ref ABA历史或最终项目Full。
- 效率核验：v208最慢单节点13.371s，四个固定候选结果回归各12.7–13.0s，无异常长停留。后续仅重跑受oracle/环境校准影响的节点，复用相同生产代码的已通过回归，没有跳过最终必需tiers/Full。

- v209原10节点全部PASS/56.67s，XML SHA256 `7af1527f7b9e9f8c8e69cf30d5c6809d3d937fbca1b806e5736c3279d337363e`。复核发现后置Git helper继承host环境，虽argv相同但未显式复用首次独立Git的环境；校准改为精确复用advance.args和原environment。v210仅重验受影响的5个main-advance节点，生产代码及5个正例不变，复用v209正例和v208的12个其他回归（v208整体仍20PASS/2FAIL，XML SHA256 `7b39176e89fbbf2f7247d240ec3fbb133797379fe0cb70cd62c94181acd86bbe`）。唯一根 `D:/Work/devx015-v210`、16/loadfile、XML保留，同owner/归档清理条件。

- v208原22节点20 PASS/2 FAIL、152.25s；两FAIL均是无reflog竞争的Git错误文本断言，原Git确实exit128，但只报告couldn't set refs/heads/main而非Permission denied。测试改用原独立CreateFileW探针证明main loose文件或packed-refs的WinError32，继续要求原进程退出后完全相同Git命令能前进。生产代码不变，原已通过terminal/固定候选结果回归不重跑。v209只重验5布局各正负例10节点，唯一根 `D:/Work/devx015-v209`、16/loadfile、原XML保留，同owner/归档清理条件。v208整体失败保留不提升。

- v207原6节点全部PASS/34.52s，XML SHA256 `b2cb7d4411614825f9ed72368d017730a32466f0d74f67f0af1c94535b147579`。loose/packed竞争均因原reflog持有被拒绝，因此追加无reflog两布局，独立证明ref/目录自身保护，而不把日志保护当packed fallback证明；同时覆盖symbolic target。每次竞争拒绝后，原checkpoint进程退出，再执行完全相同的Git命令必须成功，证明无无关权限错误或句柄泄漏。
- v208运行上述5种布局各正负例10节点、原transaction两节点，以及原terminal/并发/等待者和固定候选Full结果在main前进后仍可记录的回归；唯一根 `D:/Work/devx015-v208`、16/loadfile、原XML保留，同owner/归档清理条件。测试仅合成候选结果，不是项目最终Full；生产代码与v207相同。

- v206原2节点1 PASS/1 FAIL、15.67s；XML SHA256 `775acd78910f343e9812902b8221171a4b91559644930d58fecee3e70ff20061`。真实Git M→N成功后原checkpoint仍exit0/PASS，触达生产缺陷，无身份门禁替换。修复在原arbiter内部以既有Windows read/directory custody持有main ref、已有reflog及packed fallback；跟随symbolic ref目标，不创建Git lock/第二arbiter。转换副作用前复核HEAD/main观测值。POSIX仍为原cooperative arbiter和写前复核，不扩大native证明。
- v207覆盖loose/packed两种布局的无竞争与main竞争4节点，并复验原transaction两节点，共6节点。唯一根 `D:/Work/devx015-v207`、16/loadfile、XML保留，同owner/归档清理条件。先检查原正常路径及packed兼容，再扩展终态/固定候选结果回归。

- 已复核原outer v2 replay PASS/TASK_SOURCE_PRE_WRITE及同scope preflight PASS。v206复用原公开checkpoint真实payload完成后的有限观察屏障，独立native PID/FILETIME核验进程，原Git update-ref以M为expected执行一次M→N。没有替换检查结果或身份门禁；若竞争成功，原checkpoint必须在heartbeat/event前typed拒绝并保留N；若被已有原保护拒绝，原checkpoint应通过。无竞争正例必须通过。只创建原synthetic fixture，无真实main/runtime变更，不以此替代ref ABA历史检验。
- 唯一根 `D:/Work/devx015-v206`，16/loadfile，2节点，XML `outputs/validation_runtime/devx015-v206.xml`；owner DEVX-015，原源码/测试/CLI/Git输出保留至最终证据归档且无依赖后治理清理。先运行当前实现，取得实际旧入口证据再定修复；仍81/106映射。

### 2026-09-20：v202 原 transaction 竞态触达真实缺陷

- v205原25节点全部PASS/45.50s，XML SHA256 `a160e18adca686fe0eb2e82a7df7ed8f2231fa91473cbd63bc3f5d9f958a9ba7`。原公开checkpoint竞态写入被拒绝，独立native probe返回WinError32；无改动正例、lease竞争/等待者、terminal crash/replay/并发release通过。16项原read custody默认/显式共享模式验证叶及祖先不可write/replace/rename、兄弟receipt可原子替换和异常后全部句柄释放。登记X02.transaction_toctou，81/106映射、25未映射，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED。本机Windows实证不扩展为POSIX mandatory custody证明。
- v203的8个唯一fixture均经原公开release入口补齐FAILED receipt；此前已全部RELEASED，恢复未增加lease/event、未改变原transaction字节，日志保存在各fixture的post-failure-terminal-recovery.json。v203/v204原FAIL不提升。v205后只调整测试函数签名换行；四文件Ruff及两生产文件strict mypy通过，无语义变化。没有运行项目最终Full或推进OPS080运行部署。

- v204 2 PASS/1 FAIL、15.82s，XML SHA256 `2f78ec10604bf6753b6260ff6a350ff6c2949f8b91da231640195f8abd1eb13e`。无改动及默认native probe通过；实际transaction写入被拒绝，但Python CRT仅给errno13、winerror=None，新增测试错误要求Python提供native码。改用原独立CreateFileW probe证明WinError32，不放宽生产拒绝条件。v205复验原8节点、默认write probe及16项默认/显式父目录共享的原read-custody身份/别名/异常退出回归；后者须证明兄弟receipt可原子替换但原叶/祖先仍拒绝write/replace/rename。唯一根 `D:/Work/devx015-v205`，16/loadfile，原XML保留，同owner/清理条件；不运行Full或注册表夹具。

- v203 原8节点均FAIL/41.41s，XML SHA256 `0b5a1aa87e968159116cfaa711800bcd67e9b21b1bc1a2da6daf529fd2f42355`。新read custody默认不共享目录写入，导致同目录原closeout receipt原子rename被WinError32拒绝；保留失败证据，不取消安全检查。修正为仅显式publication调用允许parent write sharing，leaf仍只share-read、全部目录仍deny-delete；普通输入custody默认不变。v204先检验原竞态两节点及默认native write probe，共3节点，唯一根 `D:/Work/devx015-v204`、16/loadfile、原XML保留，同owner/归档清理条件；若通过再补原terminal回归。

- v202 原公开 checkpoint 终态 1 PASS/1 FAIL、15.02s；XML SHA256 `3cd7a575243a88d6345e56f46f0ad1a512894695176f1e04347e053eb10b32ee`。真实外部改写 transaction 并重算 hash 成功后，原 CLI 仍 exit0/PASS；无改动正例通过。原源码、测试、CLI 日志及变更字节保留，不提升失败为通过。
- 修复在现有 store arbiter 内对 Windows checkpoint/release 持有原 transaction 叶文件和祖先身份至转换返回；写入前复核完整 replay，禁止无效 replay 经 binding 返回 PASS。POSIX 保留现有 cooperative arbiter 和写前复核，不宣称具有 Windows mandatory custody，不改变 Ubuntu CI 平台支持。
- v203 复验原竞态两节点及原 lease/terminal 回归；唯一根 `D:/Work/devx015-v203`，16/loadfile，XML `outputs/validation_runtime/devx015-v203.xml`。owner DEVX-015，原 fixture/日志/XML 保留到最终归档且无依赖后治理清理。原生注册表夹具不运行；终态前不新增映射，仍80/106。

### 2026-09-20：X02 路径竞态原节点复核预登记

- v201原6节点全部PASS/57.69s，XML SHA256 `0a6a84e6073bd35feefe11d09cf7f3275afed48e6097cbc55b7feaa509f58068`。原公开路径交换与native观察器正负校准通过，登记X02.path_toctou，累计80/106映射、26未映射，原14 counters/10 mutants及PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变。
- 下一v202检查原transaction验证与追加事件之间的窗口：原公开checkpoint在真实_checkpoint_payload完成后仅暂停，不替换任何身份/验证结果；独立原生身份核验子进程后，外部尝试重写transaction.thread_id并按原canonical JSON规则重算hash。若OS持有阻止写入，原流程应正常通过；若写入成功，必须在heartbeat/event前typed拒绝，不允许非法replay包装成PASS。同时原无改动正例必须通过。唯一根 `D:/Work/devx015-v202`，16/loadfile，XML及原SUT/测试/CLI/变更前后字节保留；owner DEVX-015，归档且无依赖后治理清理。先取得原实现行为，再决定修复，不把新API缺失作为red。

- 逐行审阅原S02 public plan/capture/recover测试：实际worker检查后，独立线程交换原叶文件或祖先junction；原生观察器确认替代canary零内容访问，原件保留、无成功receipt/ref、typed拒绝后恢复至有限失败。该原语同时直接覆盖X02路径对象check/open竞态，不借此覆盖transaction/main/ref ABA/PID各独立变体。
- v201只复验两个原swap节点及四个原观察器正负校准，生产/测试代码不改；唯一根 `D:/Work/devx015-v201`，16/loadfile，XML `outputs/validation_runtime/devx015-v201.xml`。owner DEVX-015，保留原fixture/日志/XML至最终归档且无依赖后治理清理；不创建原生注册表夹具。取得原终态后才登记X02.path_toctou。

### 2026-09-20：原等待请求取消及后来者前进通过

- v200原6个公开CLI节点全部PASS/89.51s，XML SHA256 `a138bfdcf703c574ca6913a8639e14737a479b750b93552b0e4a9376087066b1`；v199未改变kernel的12 PASS继续有效，原XML SHA256 `04a550e40d2248973982a0fe2d468bcd0e70b093a0dd96f7d24df6f4d372d385`（该次整体16 PASS/2 FAIL，不提升整体结果）。新cancel-first/race两个节点分别15.300/13.287s；原等待者、失效请求及有限竞争回归均通过。
- 原公开cancel-request在原store/arbiter内取消精确BLOCKED请求；错误actor、活动请求、已有后继generation的旧请求均拒绝且不新增lease事件。取消重放无新事件，同一原请求重试被typed拒绝；独立两个原CLI竞争只有一个结果能取得执行授权，已观察BUSY在两个原进程退出后以同一请求核对终态。后来原等待者可前进，最终无active，原历史事件字节、refs/index不变。登记X04.cancelled_queue_head，当前79/106映射、27未映射；仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，尚非最终验收。
- 三个原生产模块/一个原CLI及测试Ruff、四生产文件strict mypy通过；系统流已同步入口和边界。未建立第二queue/store/scheduler，未改runtime/触发daily。v199/v200原目录/回执/XML保留，最终归档且无依赖后治理清理。outer v2继续承担源码阶段，最终候选及正式tiers/Full仍待全部V3闭合后执行。

### 2026-09-20：原 source generation/commit 四窗口恢复闭环

- v199原18节点终态16 PASS/2 FAIL，84.14s。cancel-first实际取消成功且重放无新事件，但原publication acquire漏接CheckoutGuardError，取消后原请求虽被LEASE_ALREADY_TERMINAL拒绝，却逃逸为traceback/exit1；补原acquire的typed异常转换。竞争场景实际cancel成功、retry获LEASE_ARBITER_BUSY/exit2；原arbiter不排队，测试修正为两个原进程结束后，仅对已观察BUSY用同一原请求追加一次终态核对，仍要求取消与新授权不能同时成功。此非第二scheduler、无新request id，不把BUSY当终态。
- v200仅重验受原publication acquire修复影响的6个公开CLI节点，复用v199未改变kernel的12 PASS；独立根 `D:/Work/devx015-v200`、16/loadfile、XML `outputs/validation_runtime/devx015-v200.xml`，原fixture/CLI日志保留至最终归档/无依赖清理。v199原失败不覆盖或提升；cancelled_queue_head尚未登记。

- v197 commit-after 原1 PASS/312.32s，XML SHA256 `46fc11b344613bc8d62e21a03e7a46efb8506afbfd7ed55394a7283b50714e32`；v198另三窗口原3 PASS/343.42s，XML SHA256 `78836ca1829c132c1a97211f0d7474651c587ee41740deb666152ec8d0aa0c42`。v198三个case分别307.039/333.047/308.090s，在独立仓库/store/Job按参数并行完成，无Full。此为实际调度时间证据，不是生产端到端速度对比。
- 四窗口均独立核对producer、绑定启动器、实际worker的PID/FILETIME与Job归属，终止原producer并证明执行树退出；原新进程source-recover有限失败，原源码/HEAD/index/refs、私有generation/capture/object intent与已创建commit保持，重复恢复无派发/新事件。登记R02.canonical_generation_commit，累计78/106映射、28未映射；仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，最终候选Full、迁移与OPS080均未完成。原目录/XML/日志保留到最终证据归档和无依赖治理清理。
- 下一X04取消语义仅针对尚未执行的BLOCKED请求，使用原store/arbiter和追加事件，不建立队列或scheduler。取消后同一原请求不可重新取得执行权；错误actor、活动或已派生下一generation的请求不得取消，重复取消无新事件。后来原等待者须经公开入口在holder释放后前进。实现/实证未完成前不登记cancelled_queue_head。
- 已通过当前SINGLE_LANE/coordinator/LANE/contract-change preflight（原v2事务及同scope，无blocker）。原store新增受限BLOCKED→RELEASED/REQUEST_CANCELLED事件，replay验证原不可变lease字段/无execution/原actor或coordinator，保留原state schema；cancel在原arbiter内拒绝活动和有后继的请求、重复取消幂等。原checkout guard新增cancel-request公开入口，只绑定指定repository的原intent/lease，不创建第二authority。v199限定两个实际公开CLI序列（先取消/同时重试竞争）、原X04等待者/失效请求/有限竞争和原S2 kernel回归；唯一根 `D:/Work/devx015-v199`，默认16/loadfile，原XML/CLI回执保留。owner DEVX-015，全部终态且证据归档/无依赖后治理清理；不把新增接口在旧版本缺失当成触达旧生产缺陷的red。

### 2026-09-20：v194 ACK 丢失后原 CLI 收尾恢复通过

- v197 原commit-after节点1 PASS/312.32s/exit0。实际producer、Job绑定启动器与实际worker原生身份分别核对，终止producer后三者退出且Job不存在；新进程RECOVERED_FAILED、FAILED/RELEASED，保留原commit及所有原始私有字节、源码/HEAD/index/refs，重放不派发或追加事件。下一v198仅补generation-before、generation-after、commit-before三个独立参数，唯一根 `D:/Work/devx015-v198`，原XML `outputs/validation_runtime/devx015-v198.xml`。本次focused明确使用16 workers/`--dist load`替代默认loadfile，各参数独立Git仓库、原lease store和Job，无Full；使同文件三个隔离场景并行，原断言/输入冻结及清理条件不变。全部窗口终态前仍不登记R02.canonical_generation_commit。

- v196 原1 FAIL+1 teardown ERROR/310.04s，XML SHA256 `b1d8fdd54ab2463ce8803d0834e73861a268c8cd6b22a0a69e2f89805b19ac70`。实际到达 commit-after，私有 commit `c91bd6ff2044b74f2e17a615643c19ca9b8b88d1`；错误是测试将原 Job 绑定的 venv 启动器55464与同Job实际worker60040判为同PID，未执行预期中断。外层Popen终止不足以停止真正producer13024，原teardown正确拒绝非终态lease。已沿用原 `_terminate_fixture_producer` 按记录PID/FILETIME终止真实producer，独立确认Job不存在；原source-recover最终exit0/RECOVERED_FAILED、FAILED/RELEASED且不派发，保留全部原证据，不把事后恢复作为v196 PASS。
- 事后首次恢复误传真实项目task_id，被合成事务身份门禁拒绝；随后读取原transaction.task_id纠正。该手动恢复漏设PYTHONDONTWRITEBYTECODE，产生合成fixture的pycache，原release报告DIRTY_UNATTRIBUTED但lease已RELEASED；核实后设置正确环境，通过原终态恢复补齐FAILED receipt。新增pycache作为诊断痕迹保留，未修改原源码/手工编辑租约。此差错仅限v196隔离fixture。
- 新测试复用已有原生终止helper，分别核验producer、原绑定启动器、实际worker的FILETIME及后两者的同Job归属，终止实际producer后证明三者退出；finally也按精确producer身份收尾。下一v197仍仅commit-after单节点，唯一根 `D:/Work/devx015-v197`、默认16/loadfile、原XML保留；不修改生产进程模型或门禁，不以父子PID相同作为前提。

- v195 原执行4 FAIL/275.73s，均在 `_freeze_candidate_fixture` 的原 Git diff 空白门禁被 `mode.txt` 的 CRLF 拒绝，未产生 source worker 日志/未触达恢复窗口；不是生产恢复反例。仅把该新增夹具源文件换行改为 LF，不改门禁或生产实现。下一 v196 先运行路径最深的 `commit-after-source-job` 单节点，-n16/loadfile，独立根 `D:/Work/devx015-v196`，XML `outputs/validation_runtime/devx015-v196.xml`；若通过再补另三个窗口，避免重复相同准备失败。v195 原证据保留，v196 owner/退出清理条件同前。

- 下一 v195 使用现有 source-job 夹具，在原 fixture 历史/运行时绑定之前冻结四处有限观察屏障：generation 编码前、generation.json 写入后、commit-tree 前、commit-tree 后。原公开 CLI 派发真实 Job，独立 native PID/FILETIME/Job oracle 确认屏障后终止原 launcher，核对 worker 随 Job 退出；新进程原 source-recover 应有限失败、不安装/派发，保留源码、refs/index、私有生成字节及已创建 commit。四个独立参数共用同一原 helper，不复用不同候选的 Full。临时根 `D:/Work/devx015-v195`，owner DEVX-015，原 XML/日志/夹具保留至证据归档且无进程依赖后治理清理；默认 -n16/loadfile。终态前不登记 R02.canonical_generation_commit。

- v194 原执行 exit0，2 PASS/808.42s；XML `outputs/validation_runtime/devx015-v194.xml` SHA256 `b788913c9a820322a9dab810085840cf0c92f083bc482ce827d70ecf6397b9ff`。候选 `22d2d4167131df79cc2a257fa0ca833b73b0f0a7` 的真实 push 成功后 launcher 退出47，原 origin/main 保持旧值；新进程原候选 CLI 的 CLEANUP_PRE、completed release、release replay 均 exit0/PASS。单次 push/无 force、refs/index/Full summary/Full execution 投影保持及无 active fixture lease 断言全部通过；独立双 push 校准同时通过。
- 登记 R02.remote_ack，当前77/106映射、29未映射，仍 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED。原 fixture Full 不是项目最终候选 Full。原目录 `D:/Work/devx015-v194`、原 XML 与三份 CLI 回执保留，待最终证据归档及无依赖治理清理。继续 canonical generation/commit 等剩余 V3，再完成 OPS-080 S4/S5。
- 现成事件：FULL_DISPATCHED 07:32:22、FORMAL_VALIDATION_RESULT 07:38:29、LOCAL_MAIN_FF_PRE 07:40:03、CLEANUP_PRE 07:41:44、RELEASED 07:41:46（JST）。实际 Full 与结果核验区间约367s，下一准入约94s；保留此阶段证据，不据此声称生产性能优化完成。
- 用户已明确授权仅清理已备份的两个旧 HKCU 测试根及子键；原 PowerShell 精确删除命令再次被自动审批以 `blocked by policy` 拒绝，CreateProcess 未执行，未删除任何键。保留备份及授权，不换接口绕过；原生注册表夹具恢复仍待清理条件解除。此限制不暂停其他已授权开发。

### 2026-09-20：R02 原始半截文件与 I06 精确候选准入

- 下一v194限原ack-lost节点及已有独立双push计数校准。原真实push成功后launcher退出47，保留stale origin/main；新增原candidate脚本/导入来源的新进程CLEANUP_PRE、completed release及release replay，独立比较refs/index/Full summary字节、原Full execution投影、单次push trace和无force参数。复用原CLI/原store，不增加恢复入口。新CLI测试等待上限300s，依据已观察73s后续准入成本留诊断余量；不改生产超时、Full runner预算或断言。根D:/Work/devx015-v194、-n16/loadfile、原XML与场景保留；终态前不登记R02.remote_ack。

- v192原7节点PASS/304.49s，XML `outputs/validation_runtime/devx015-v192.xml` SHA256 `3de29bf1abf58e53f80d1427a27da447bb8476a75f456d40a682ccbe7f774974`。原fixture在冻结前注入raw写入前、实际flush/fsync半截文件、写入后故障；独立native oracle确认原Job worker与producer退出。完整源码/HEAD/index/config不变，半截/完整原捕获字节保留，无capture manifest时不冒充完整snapshot；原actor恢复到INSUFFICIENT且不派发。原CAPTURED/OBJECTS_WRITTEN/REF_CREATED及终态receipt并发新进程恢复同组通过。登记R02.capture_objects_ref_receipt。
- v193原I06节点PASS/664.52s，XML `outputs/validation_runtime/devx015-v193.xml` SHA256 `701c854b27fe2f1fa21730b5d20d0858f4f52d2d767cd0cbb170d9531db0cea4`。一次实际fixture Full/合法canonical DONE准备耗时522.39s；公开新进程preflight对C=e42500ab40ba53a39b859b9f18459e19f27e3089返回exit0/PASS、COMPLETED_VALIDATED_CANDIDATE_INTEGRATION。随后真实Git创建同tree的新HEAD=f84fcdb460b1bd21cfb18690622c12fe2770bb58，公开新进程exit2/BLOCKED、TASK_NOT_REGISTERED。各次CLI前后refs/index/源码/canonical/事务/lease记录一致，原Full execution未被候选漂移覆盖。登记I06.valid_completed_candidate与wrong_candidate；一个Full支持同一C的正例及后续身份漂移反例，不为两个独立C复用验证。
- 当前76/106映射、30未映射。两个测试模块Ruff与限定路径diff检查通过；v192后仅修正参数列表换行，不改执行语义。仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED；此处fixture Full不是项目最终C Full。v192/v193原目录、CLI回执、时间记录/XML保留。
- v193现成事件时序：FORMAL_VALIDATION_PRE 07:14:11、FULL_DISPATCHED 07:15:36、execution request 07:16:20、内层pytest输出100%约07:18:39、FORMAL_VALIDATION_RESULT 07:21:23、LOCAL_MAIN_FF_PRE 07:22:36（本机JST）。85s/44s准备间隔与约164s结束核验、73s后续准入值得继续分解；不要把用例100%等同正式Full完成。已采用同一真实Full正反配对，避免第二次相同准备；未跳过任何原身份/回执检查。

### 2026-09-20：R02 生命周期之前窗口与性能定位

- v191已取得7 PASS/149.25s/exit0，XML `outputs/validation_runtime/devx015-v191.xml` SHA256 `c74b1dee6b761bebfb3994799e3531124a5b5ed9737f68338b1fa074cb7c9b9b`。checkpoint三阶段分别40.339/40.300/43.896s，Full四节点合计134.679s，两模块按loadfile并行且全部终结；未重复执行v190或正式Full。结合v190登记R02.claim_launch（真实claim event/projection、reserve/create/bind/resume intent）、child_exit_before_result（退出记录前后、结果记录前后和Full未记录/已记录退出）、release_cleanup（成功release前后有限失败终态、原证据保留）。累计73/106映射、33未映射，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED；capture_objects_ref_receipt的写入中途、canonical_generation_commit和remote_ack仍未因此完成。release_cleanup为原checkpoint租约终态/证据清理边界，不授权真实工作树删除或替代最终发布closeout。

- 在原 checkpoint 真实进程测试中补 BEFORE_EXIT_CONFIRMED、BEFORE_RESULT_RECORDED、BEFORE_SUCCESS_RELEASED，保留原7窗口和native PID/FILETIME/Job、原证据保留、新进程有限终态、不重新派发检查。v190 原10节点全部PASS/340.86s，XML `outputs/validation_runtime/devx015-v190.xml` SHA256 `2075a0221faabf00311f8962d3bde42c5eb50cbbadb366693f1e53ec09bae1d6`。实际子进程已退出而durable execution仍RUNNING的窗口，恢复不能推断原退出码或把未记录执行收作PASS；成功release前后保留原已记录结果与历史。
- 同一原worker中断测试继续覆盖CAPTURED、OBJECTS_WRITTEN、REF_CREATED；新增producer仍活时公开recover只OBSERVE_ONLY、无写/派发的检查，及实际到达事件集合断言。与原Full claim两节点、exit-unrecorded/exit-0两节点合并为v191共7节点，-n16/loadfile，独立模块并行；终态未取得前不声明PASS或新增映射。
- 两个只读cProfile探针正常exit0：实际运行时inventory 16554文件/441758533字节，读取身份核验14.703s；同inventory的live custody建立61.125s、释放后总69.516s，16556 file custody对象。主线程累计retain57.939s、bound I/O42.843s、nt.stat19.538s、关闭8.274s；运行中曾观察76216原生句柄。探针与focused测试重叠运行，这不是受控速度对比，也不能解释全部发布耗时或宣称端到端优化已完成。未建立authority缓存或削弱native核验。后续优化先围绕重复保全/核验调用计数取证，不因pytest输出间隔擅自重启原执行。
- v190根D:/Work/devx015-v190及XML保留；v191根D:/Work/devx015-v191单次启动，保留至终态和证据归档。两个性能探针已释放全部自身句柄并正常退出。项目main/remote/runtime未变，最终C/Full与OPS080验收仍未完成。

### 2026-09-20：R02 请求持久化与 acquire 返回前恢复证据登记

- 当前源码原节点复验：v188 的 source request 写入前/后真实 launcher 退出、新进程公开恢复均通过，2 PASS/198.28s；XML `outputs/validation_runtime/devx015-v188.xml`，SHA256 `1166527e747078b379563a00955cc038056d7018610dc7c3467c5eb281bad912`。独立 native oracle 核对 PID/FILETIME、进程退出及 Job 不存在；错误 actor/request 被拒绝，原 request/refs/index/source bytes 保留，终态 FAILED/RELEASED、INSUFFICIENT，重复 source-recover/source-candidate 不派发、不追加事件。登记 R02.request_persist 的 before/after 两节点。
- v189 的 BEFORE_ACQUIRE、ACTIVE_PERSISTED、TRUNCATED_ACQUIRED 原三节点复验，3 PASS/89.55s；XML `outputs/validation_runtime/devx015-v189.xml`，SHA256 `d4739bfe057ca9172989fae68200ce946f5c8f7f22f6fce0faa7923b1b1800f3`。实际 producer 在租约获取前、已持久化但未返回、ACQUIRED 事件写入17字节后退出；新进程有限失败恢复，原源码/refs/证据保持，无新派发或残留 active lease；无租约旧尝试重放不影响后来独立租约。登记 R02.acquire_before_return。
- 当前70/106映射、36项未映射，保留 PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED、14 counters、10 mutants。上述是 source request 与 checkpoint acquire 的具体边界，不推定 canonical generation、claim/launch、child-exit/result、remote ACK、cleanup 等其它 R02 已验收；最终 exact C 必跑验证仍须执行。
- 两组均使用隔离短根 D:/Work/devx015-v188、D:/Work/devx015-v189；原测试与生产实现未改。v188 使用2个 xdist workers/loadfile（2节点），v189 使用项目默认16/loadfile；没有串行重跑或替换失败结果。保留两组 fixture/XML，待证据归档和治理清理。此轮不重复重建原发布 Full，不修改实际 main/remote 或 OPS-080 runtime。

### 2026-09-20：v187 原 main CAS 恢复闭环通过

- 生命周期：v185/v186失败或未知现场、v187成功原fixture及outer-harness三文件/receipt继续保留，待最终证据归档和去重审计。两个纯路径探针 D:/Work/devx015-path-probe-20260919、D:/Work/devx015-path-short-20260919 的tracked/untracked/ignored核查未发现新增内容；清理命令被工具自动审批以blocked by policy拒绝（未提供更具体理由），未绕过。Owner为本任务coordinator，退出条件为允许的治理清理入口可用、无进程依赖及证据已保全。

- 复核原v180 XML SHA0f53c1590cfdd13ba318d1b4e9daa72403fbadbe9b4ee69dee4f53839d736514，唯一原中断节点PASS；当前函数与v180独立保留源码AST相同。该节点实际覆盖原提交后中断、新进程恢复、单attempt/Full不变、原main/C稳定和再次重放，补登记R02.local_publish。累计68/106映射、38未映射；不重跑该34分钟旧链仅为补映射，不把历史生产版本PASS提升为当前最终C验收。

- 原 v187 外层终态1 PASS/1843.40s/exit0，XML SHA256 edb917ff6b6a946a10585e4a44d5ef89ae78e5d8f64d54368e47b758b61c92cd。实际fixture Full PASS/274.13s；不是项目最终C的正式Full。
- 原候选 eeb065f92b9af0ee8b99a5631a36aeb48c6b31a8，M=60fdd71c17359befa6b1bd63d18a519cf52ac5c1，独立原Git一次CAS到N=17181ad931e7eb25c55d71293b8131323d6c6902，exit0。原公开publisher exit2/RECOVERY_REQUIRED；新进程recover exit0/RECOVERED_FAILED，stable_state=CANDIDATE_RETAINED_MAIN_ADVANCED；再次新进程exit0/REPLAY_ONLY。
- 原head_recovery为RESTORED、稳定观察v4；候选恢复原branch/C，peer detached M及原index/private bytes保持，N保留，Full投影不变、单publication attempt。原fixture事务最后FAILED、失败lease正常释放；外层teardown也通过。终态后未发现相关pytest或v187 Python进程。
- P01.main_cas_race登记原精确node，现67/106映射、39未映射；PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED保持。14 counters、10 mutants、其它原义务及最终C required tiers/Full/授权迁移发布、OPS080工程部署和新ordinary daily均未被替代。
- 提效边界：固定本worktree导入；重试先用短独立探针确认准备错误；保留未改变的v177/v180结果；不重复OPS080原507项。原事件显示v177/v180从LOCAL_MAIN_FF_PRE到终结分别约36.6/22.9分钟，下一性能诊断应拆解发布/恢复核验而非先加速pytest。该区间含必要检查，不能全部称为浪费；本轮没有修改生产性能实现或宣称全链加速。

### 2026-09-19：续作准入与 v186 单项证据补齐

- 短探针确认同一合成提交：118字符peer根触发最长文件265字符，Git明确Filename too long/128；短根原Git成功/0。只缩短临时路径，不修改生产代码、测试或Git配置。
- v187纠正重验唯一根 D:/Work/devx015-v187，先确认不存在；同一原main-advance精确节点、-n16/loadfile，XML outputs/validation_runtime/devx015-v187.xml。运行期冻结全部输入，证据保留/无进程及唯一内容审计后清理；不覆盖原v185/v186。新长跑前根据实际Git树计算最长路径。该修正只处理测试准备错误，不改变原P01 oracle。

- v186 终态 1 FAIL/105.30s，失败在原测试创建 peer worktree，Git exit128；未到实际 Full/发布。新根名称过长使最长 peer 文件路径达到265字符，是待短探针验证的 Windows 路径假设，不记为 P01 red。原目录/XML保留。
- 路径探针仅使用新独立 D:/Work/devx015-path-probe-20260919 bare clone，来源为 v186 合成 fixture，不改原 fixture 或项目。目标长目录使用与 v186 peer 相同字符数，短目录使用 D:/Work/devx015-path-short-20260919；两个 detached worktree 只用于捕获原 Git stderr/退出码。完成后记录结果，审计无唯一内容/进程再清理；不改变 core.longpaths 或主项目配置。

- Owner 明确继续 DEVX-015、OPS-080，先检查提效；不改变原 V3/106 变体、14 counters、10 mutants 或最终正式验收。
- 本工作区必须以项目 Python 3.11 配合本工作区 src 导入。未绑定 PYTHONPATH 的旧 checkout 实现误报 LEASE_EVENT_HASH；绑定后原 replay PASS，不能称历史事件损坏。
- v185 原目录保留；当前无相关 Python 测试进程，fixture 未产生 publication transaction、Full 或 XML，退出原因仍未知。不得删除现场或把缺失 XML 当作通过。
- 旧 v5 source transaction 经官方 release 成为 FAILED/RELEASED。新 devx-015-closeout-resume-20260919-v1、lease-7fc482c9041d92c23bed 在同工作区 LANE PASS。初次 acquire 重复声明自动资源、初次 preflight 重复 lane/coordinator claims 均被拒；更正调用参数后通过，未改门禁。
- 原 v185 fixture 上 report builder 的 write=False 独立 cProfile 为 0.75s/PASS，仅排除该局部作为当前优化目标，不代表完整 fixture 或验收。
- v186 唯一根 D:/Work/devx015-publication-main-cas-v186-resume-20260919，启动前确认不存在；原测试文件 SHA256 467651851d4a91f7f6420ed9bce18ea319ccdf5fe3d3ac29e4673e5314a4871d 与 v185 相同。只执行原 test_original_publication_cli_recovers_independent_main_advance[full-profile-publish]，-n16 --dist loadfile --tb=short；XML outputs/validation_runtime/devx015-publication-main-cas-v186-resume-20260919.xml。
- 执行时冻结源码/测试/manifest；不加入定时 native stack dump、不重复已通过正常发布/中断链。失败先按位置诊断，无新证据不自动重复长链。目录用于 P01 缺失证据，结果与原文件保全后，确认唯一内容和进程依赖再治理清理。66/106 仍仅映射，OPS-080、最终 C/Full/发布未完成。

### 2026-09-15：v184终态与v185原Full/公开发布main CAS闭环预注册

- v184原40225终态37PASS265.86s/exit0，XML SHA
  f3a76c58a1dd9c6db82b5a376d37e492eb1f29bfa8bebc79ef4e14a65e91bb5e。
  两个N入口在无原ready/Full时拒绝且无写入；生产持有器原生竞争、v2恢复及stable.v4基线与
  15种反例、旧M恢复回归通过。五份原生产/测试文件保留v184根；仍不是真实正向发布恢复闭环。
- v185唯一根D:/Work/devx015-publication-main-cas-v185，启动前确认不存在；XML为
  outputs/validation_runtime/devx015-publication-main-cas-v185.xml，-n16 --dist loadfile --tb=short，
  唯一节点tests/test_arch_005_integration_publication_fence.py::
  test_original_publication_cli_recovers_independent_main_advance[full-profile-publish]。
- 一份新实际Full与原public publisher：测试自己的main peer及私有canary在Full前创建；
  public handoff显式授权只限该fixture peer。原持久HEADS_SWITCHED、原worker/Git活体、未有FAST
  准备后，独立Git只做一次M→N CAS；保留CAS命令/退出/原请求/原进程及hooks证据，不改hook/回执。
  等原publisher真实退出后，新进程recover须RECOVERED_FAILED/stable.v4，候选恢复C、N保留、
  peer detached M且index/private bytes不变、原Full不变、单attempt；再次新进程REPLAY_ONLY，
  原失败lease经正常release收尾。错过窗口必须失败而非换成正常发布PASS。
- 原函数整体源码固定，不对原Full fixture后补代码；错误保留现场及独立恢复结果，不手改lease。
  这一个闭环也不能代替全部V3/14计数/10mutant/最终C正式Full/授权发布/OPS080整体验收。

### 2026-09-15：v184原事务N恢复入口与独立失败adopter实现预注册

- 原fence新增N恢复窗口，复用原lease/phase/plan/Full/request及原Job/Git终态校验；普通发布
  expected-main检查不放宽。原公开recover在N路径调用原记录v2的恢复，持有原目录、N ref及
  两份现存reflog，再仅逆写candidate与原归属ORIG_HEAD/锁。peer保持detached M。
- 新独立adopter在原恢复完成后重查当前V(C)，短原生持有及原arbiter内重检，追加stable.v4
  CANDIDATE_RETAINED_MAIN_ADVANCED；返回RECOVERED_FAILED而不是LOCAL_PUBLISHED。
  profile/原attempt/原恢复digest/现场/原生终态全部绑定，重放不改记录、不允许重新dispatch。
  缺失日志不能获得现存文件持有保证，当前typed拒绝PUBLICATION_RECOVERY_JOURNAL_CUSTODY_UNAVAILABLE；
  该边界仍需纳入后续恢复覆盖，不能以缺失日志的现场冒充完整原生持有。
- v184唯一根D:/Work/devx015-main-advanced-runtime-contract-v184，启动前确认不存在；XML为
  outputs/validation_runtime/devx015-main-advanced-runtime-contract-v184.xml，-n16 --dist loadfile
  --tb=short。integration测试节点test_publication_main_advanced_durable_recovery_contract（23）
  与test_publication_failed_head_recovery_scene_and_durable_contract（12）；fence测试节点
  test_publication_lifecycle_requires_original_ready_event[main-advanced-window]及
  [main-advanced-original]（2），预期37例。restored内另验证stable.v4基线和15种绑定/终态/权限反例。
- 两个完整持有场景改用生产hold_publication_main_advanced_recovery_scene，不再复制测试持有实现。
  本批仍非真实原Full/public publisher竞争闭环；原生fence正向收尾/新进程恢复/重放/释放尚待实跑。

### 2026-09-15：v182原生reflog反例与v183完整持有验证预注册

- v183原55500终态3PASS38.60s/exit0，XML SHA
  a1ff275ebca3f0c2ac5817c7d2de6e5562987f13e5ebdf710fd7d5b5f51f5abc。
  只持有ref的反例保持拒绝；ref及两份日志同时持有的peer/root竞争均失败且所有原日志及N不变，
  随后candidate-only恢复、RESTORED及重入观察通过。三份原文件已保留v183根。
  生产integration52229d539c338d61170bb4bec07a43356169b8066d9e48cc57febce5aab3493d，
  coordination671d8c209acfa8d09492f05e0844e436f37276d7aa2f5b3e439c70a4c3b06e49；
  测试f349dfaade3e780bfdcb2e9ec5a0db114e95303b85b3091f80fdfabdd0acdc20。
  当前是v2恢复契约与原生持有证据，不是公开恢复入口或整体P01/DEVX015/OPS080验收。
- v182原19441终态1FAIL45PASS314.45s/exit1，XML SHA
  393c16f00d969eded709291826114d3039ece237b3e42cabbb2d337eb9396400。
  唯一held-N失败在真实观察器PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG：main仍N，但Git在
  ref替换因持有而失败前已追加N→N2日志。不是回执字段失败；只持有N不足以保持恢复现场。
  三份原生产/测试文件已保留v182根，不改失败回执、不清理原反例日志。
- 修正验证要求：保留held-N为明确原生反例，观察器拒绝后不执行candidate逆写；新增同时持有
  main ref、main reflog和candidate HEAD reflog的两种竞争（从peer/root发起），必须竞争失败、
  日志及N均不变，然后candidate-only逆写、持久RESTORED和重入验证通过。不是降低原正例标准。
- v183唯一外部根D:/Work/devx015-main-advanced-ref-and-log-custody-v183，启动前确认不存在；
  XML outputs/validation_runtime/devx015-main-advanced-ref-and-log-custody-v183.xml。
  -n16 --dist loadfile --tb=short，仅运行tests/test_devx015_workflow_integration.py的
  test_publication_main_advanced_durable_recovery_contract[held-N]、[held-N-and-reflogs]、
  [held-N-and-reflogs-candidate]，预期3例；不重跑其余45个未改动分支。
- 本次只修正原生持有实验；生产v2契约不变，公开mainN恢复和独立稳定adopter仍待接通，
  必须将ref及reflog持有纳入真实原fence恢复临界区，不能以测试中的直接逆写冒充发布验收。

### 2026-09-15：v182较新main持久恢复契约预注册

- 预注册D:/Work/devx015-main-advanced-recovery-contract-v182，启动前确认不存在；
  XML outputs/validation_runtime/devx015-main-advanced-recovery-contract-v182.xml。
  -n16 --dist loadfile --tb=short，tests/test_devx015_workflow_integration.py三节点：
  test_publication_main_advanced_durable_recovery_contract（21）、
  test_publication_main_advanced_read_only_scene（13）、
  test_publication_failed_head_recovery_scene_and_durable_contract（12），预期46例。
- 新head_recovery.v2严格区分N场景和旧M v1；原plan/request不变，恢复完成只要求candidate
  原HEAD/C，peer保持detached M；前后N ref原生身份/内容及日志必须相同。恢复重入绑定首次
  scene_before，未知同字节ref替换不得重新采集后授权。原INTENT只能追加为RESTORED，不准反向或改写。
- 21例覆盖原生candidate-only逆写、原归属ORIG恢复/锁清理、持有N时原生Git竞争写失败、
  未完成记录不能terminal消费、v1/v2混淆、活体误收尾、FAST超界、原绑定损坏、peer挂回main、
  main/ref/logs改写、未恢复candidate、时间及INTENT篡改、同字节ref替换后重入拒绝。
  记录测试使用明确shape-only执行上下文，不是Full/fence/adopter权限；公开mainN恢复仍未接通。

### 2026-09-15：v181较新main只读观察器原生边界预注册

- 实跑结果：原32243终态25PASS157.08s/exit0，XML SHA256
  11c71ab4cd4a06569c9544adfdd58556e7d973449f717bc2534220b78bfd7bed。
  integration生产SHA110d7b5655f23b2bc5814b369888ce2cfc08a8b21ba4409aeb900dab9aceccf9，
  测试SHA2469714ea5a95e87ddc2c8a64b7ed41eff96e5e0337b7bdf3291cc587c2fa037；
  原文件已保留在v181根，Ruff/生产strict mypy/限定路径diff检查通过。
  这只证明原生只读观察与旧M回归；尚未接通新持久恢复记录/独立稳定adopter/真实发布竞争。
- 预注册唯一外部根D:/Work/devx015-main-advanced-observer-v181，启动前必须确认不存在；
  XML为outputs/validation_runtime/devx015-main-advanced-observer-v181.xml。
  使用-n16 --dist loadfile --tb=short，运行tests/test_devx015_workflow_integration.py中的
  test_publication_main_advanced_read_only_scene及
  test_publication_failed_head_recovery_scene_and_durable_contract；共预期25例。
- 新观察器只识别原HEAD交接之后、原FAST准备之前的单次M→N：实际后继对象、main ref原生身份、
  完整原reflog前缀加唯一M→N、candidate等价索引、peer detached M、原计划与捕获重检。
  原M-only公开恢复入口不放宽。新函数不写文件，不提供执行/恢复/发布权限。
- 13个新增原生Git场景先验证合法N基线，再验证candidate恢复和拒绝peer重新挂main、FAST准备、
  M/C/非后继、错索引、多次日志、未知锁、捕获期间ref替换、HEAD替换；保留原ref/文件及拒绝残留。
  另12例原M恢复观察/记录回归。此批不替代原事务恢复契约或真实Full/public publisher竞争验收。

### 2026-09-15：P01较新main竞争恢复缺口与实现边界

- 当前诊断：recover_local_publication在main既非C也非M时直接
  PUBLICATION_RECOVERY_MAIN_ADVANCED；原恢复observer只接受M，原stable observation v1/v2
  重建的topology也必须等于M时intent。不是只改一个分支即可安全恢复。
  尤其原HEAD逆序恢复会让peer重获symbolic main；若main=N而peer索引仍M，会破坏一致性。
- 下一实现先覆盖原HEAD交接后、原FAST_FORWARD准备前，独立合法Git CAS使M前进到N，
  原publisher失败且原Job/Git真实终态的竞争。不得改写原expected_main=M、原plan/Full或N。
  目标失败稳定状态为CANDIDATE_RETAINED_MAIN_ADVANCED：candidate恢复原branch/C及等价索引，
  已交接peer保持原授权的detached M和原索引/私有文件，未交接peer不得冒充已交接状态。
  不把peer重新挂到N、不清理陌生锁、不重置N、不宣称C发布成功，也不授予重复发布权限。
- 新恢复须由原fence/原lease/原attempt验证，N必须是M的合法后继且不同于C；实际N ref对象、
  目录/配置/源branch、原Full和候选索引需捕获并重检。短恢复期间保持N原生身份和内容稳定，
  只恢复原记录可证明归属的candidate HEAD及ORIG_HEAD效果；记录INTENT/完成及新稳定观察。
  原交易历史不重写；fresh process能够重入同一恢复意图，再独立确认稳定后原失败lease可释放。
  FAST已准备/已提交、ref ABA/未知替换或新的竞争变化不能混入该窄状态，须保留并分项处理。
- 验证顺序：原生Git N及candidate/peer身份的窄场景和拒绝边界，原记录契约，再真实原Full/
  public publisher竞争/新进程恢复/重放/原lease收尾。最终仍必须补齐其他P01/X02竞态，
  本条只是实现边界冻结，不是已实现、测试通过或106项整体验收；不重跑v177/v180。

### 2026-09-15：v180原提交后中断、独立恢复及重放完整通过

- 原7538终态1PASS2057.26s，XML0f53c1590cfdd13ba318d1b4e9daa72403fbadbe9b4ee69dee4f53839d736514。
  原C56b5c21d5eac98a89bffe0ebed7c9748c43ebd09，实际Full74PASS244.06s，summary
  2878f609dbfc92bf57818f8e3eefe7165bed66dfda12433d11f1c22a3c6359ff，full/exit0/非预演。
- 原持久ORIG prepared/committed、FAST prepared/committed后，测试核验原main=C和原Git活体，
  按worker76560/134338923966949363与原Job成员身份终止原组，Git45628/134338926185488822；
  回执EMPTY/active0。公开publisher退出2/RECOVERY_REQUIRED/native1067；独立公开恢复退出0
  LOCAL_PUBLISHED/recoveredtrue，再次新进程退出0 REPLAY_ONLY。无注入错误，无手改回执或ref。
- 独立94185确认原事务f24844b026f14b6b45d6a7079f2d4f7a402d7e545e278e3c546b550a315526e6
  FAILED/lease-c629c096415f92aa45c2 RELEASED，唯一attempt，Git退出回执仍null，缺失worker结果
  仍INSUFFICIENT/artifactnull，四个hook不变，稳定依据RECOVERED_STABLE_C。
  外测试已验证恢复前非terminal、前后Full不变、HEAD/main=C、HEAD分支main、重放不改变执行记录。
- 七份源/测试原件保留v180根，测试SHA5e580395839ad66d9d6f06fdda7ccbf604e13cacf32410bb1fa4e9af0edcfe9a。
  此为实际提交后原Job中断与checkout稳定恢复证据；不是全部P01或final C/DEVX015/OPS080验收。
  下一项main_cas_race：必须保留独立较新main，不得回写M以便收尾；先确定原恢复可达稳定边界。
  不重复未改变的v177正常路径或v180中断路径，不启动Pi/SoL-Pi。
- 将实际已执行节点映射到P01.checkout_recovery；映射为66/106、40项尚未映射。
  mapping_state仍PARTIAL_NOT_ACCEPTANCE_READY、整体execution_state仍NOT_EXECUTED；
  节点映射不是最终提交身份、统一counters/mutants或全体验收完成。

### 2026-09-15：v178测试投影错误终态；v179/v180修正验证预登记

- v179已终态10PASS6.69s，覆盖正确唯一attempt、外层Full诱饵、缺失/空/重复attempt、
  错误kind/非RUNNING/缺失merge/已退出/过早/过晚；只是测试驱动投影检查，不是原生中断验收。
  修正先投影publication_attempts唯一项再检查真实身份；注入检查异常会保留错误原件，
  等原publisher终态并捕获公开恢复后才断言失败。v180仍待运行，生产SUT未改。

- v178原69037终态1FAIL2716.57s，KeyError git_merge；XML
  266b6681515c6223c675d6e811bb07472784f87844c4e683e106635a7e64ed53。
  原测试SHA d2b28bae624da6faa55f629711fb2518fea9a0eab935a8972d1803794de99f5c已归档原根。
  外层执行记录保存Full，发布attempt位于publication_attempts[-1]；错误读取外层导致未中断。
  原Full74PASS249.88s，summary6c18322c8b1b70e5b7f8ee7ecac867823f8002f18ca682fbbfb4ad8802fa01d4。
  原C edbd2af46a28f9f5f8b4a039e3fa843c642d45a0正常LOCAL_PUBLISHED，原Git退出0；
  独立11911确认原事务913125726ca666eb19c909bef292d5bff01fef6a2c62d9abf3e0c52bee71ce1f
  FAILED/lease-7d15dc94100454d1e031 RELEASED，唯一attempt/ORIGINAL_GIT_EXIT_ZERO。
  不是中断证据；没有改写失败、手动中断或借旧Full给新代码授权。
- 仅修正测试唯一发布attempt投影，并让注入检查异常后仍捕获原公开恢复结果再报告失败。
  v179：DEVX-015所有 D:/Work/devx015-publication-interruption-projection-v179，唯一
  tests/test_arch_005_integration_publication_fence.py::test_publication_interruption_attempt_projection，
  轻量投影/拒绝回归，不能宣称真实Job/Full验收。v180：DEVX-015所有
  D:/Work/devx015-publication-interruption-v180，唯一同文件
  test_original_publication_cli_interrupted_after_main_commit[full-profile-publish]。
  两者均-n16/loadfile、启动前目录必须不存在；XML分别位于outputs/validation_runtime/
  devx015-publication-interruption-projection-v179.xml与devx015-publication-interruption-v180.xml。
- 原件与回执保留，只有原pytest/Job终态、原租约释放和证据归档后才审计清理；生产SUT不变，
  v179通过才运行v180。未执行，不增加P01/106验收计数。最终C/发布/OPS080完整目标不变。

### 2026-09-15：v178 原发布提交后中断与独立恢复预登记

- DEVX-015所有 D:/Work/devx015-publication-interruption-v178，启动前必须不存在；
  唯一 tests/test_arch_005_integration_publication_fence.py::
  test_original_publication_cli_interrupted_after_main_commit[full-profile-publish]，
  -n16/loadfile，XML outputs/validation_runtime/devx015-publication-interruption-v178.xml。
- 新原fixture C/实际Full/公开publisher不换代码、不修改hook/权威/回执。stdout仅唤醒；
  原持久记录必须恰为ORIG prepared/committed、FAST prepared/committed，main=C，Git仍活，
  再以原worker PID+creation+Job成员身份核验后终止该测试Job，保留公开publisher记录原生退出。
  错过窗口必须失败。独立新公开进程必须恢复LOCAL_PUBLISHED/RECOVERED_STABLE_C，再次重放；
  原Full和唯一失败attempt不变，missing result保持INSUFFICIENT，恢复前不得成为terminal可消费。
- 仅测试代码新增，生产SUT不变；当前未执行，不增加P01或106变体验收计数。正常v177不重复。
  原件、回执、XML须保留并哈希归档，原Job/pytest终态与原租约释放确认后才能审计清理此目录；
  不触及真实main/peer/admin/OPS。最终C全门禁、发布、OPS080整体验收目标不变。

### 2026-09-15：v177 原正常发布及独立回放完整通过

- 原40652终态1PASS2939.93s，XML5e6b729d0edb25247569d0d7acd82a6088739205c8c475afd17f379b0f0991bf。
  原fixture C4d73ae2303c710954cd114eb14e6d316a29e74f3，实际Full74PASS254.83s，summary
  bb55337609e297b5341bed7fac47fe56528ee9d4fe9666f675a96ce0755863d3，full/exit0/非预演。
  正常公开发布exit0 LOCAL_PUBLISHED/recoveredfalse，独立恢复exit0 REPLAY_ONLY；原Full不变，
  单一attempt，原Git28412/134338858003095028退出0，worker67352/134338855813243926。
- 原事务5ac6944f78d4bc073bff98dd893910872c661d6b6fa66113aab6a375447dd8a5，
  lease-a9fa7967240ab6861c58，独立原fixture replay PASS/FAILED/RELEASED，稳定状态依据
  ORIGINAL_GIT_EXIT_ZERO。fixture因不执行远端阶段按既定测试契约FAILED收尾，不改写正常本地成功。
  AUTO_MERGE aborted/prepared记录同一空锁身份[515852503,281474979085917]，committed无锁。
  七份源/测试原件保留v177根。此为正常原Full/Job/public-hook/ff-only/replay证据，不是全部P01、
  DEVX015真实最终C或OPS080验收。下一步确定性中断/竞争场景；不得重复未改变的正常成功Full。

### 2026-09-15：v176 原记录契约通过；v177 新完整发布链路预登记

- v176终态13PASS6.92s，XML7554739249e2e6f316556fbb9bae08f01ca27ad7187f14088cf808acd3477d79。
  有效构造、旧记录兼容、非空/替换/错误路径/错误阶段/缺失身份/hash/历史改写等检查通过；
  v175五种原生来源已PASS，不重复执行。不是Full或整体验收，生产修改仍待原组合验证。
- v177预登记DEVX-015所有D:/Work/devx015-publication-original-cli-full-v177，启动前不存在。
  唯一tests/test_arch_005_integration_publication_fence.py::
  test_original_publication_cli_ff_only_and_independent_recovery[full-profile-publish]，
  -n16/loadfile，XML outputs/validation_runtime/devx015-publication-original-cli-full-v177.xml。
  绑定新fixture C和原Full/public入口/原Job-Git/hooks/ff-only/独立恢复，验证AUTO_MERGE窄窗口修正。
  v172原失败和RECOVERED_STABLE_C均保留，不复用其Full给新代码授权；全部真实最终C与OPS080仍待验。

### 2026-09-15：v175 构造失败保留；v176 修正记录 fixture

- v175原67355终态13FAIL/5PASS18.01s，XML9f04bf0a03e8f2bdd2be37a063e7a32ef94a82c0bbf9cce757edf2cfec365006。
  五种真实原生来源回归通过；十三记录用例均在有效样本构造时被PLAN_BINDING拒绝，不是安全反例PASS。
  原测试保留v175根。原因是effect-only helper改HEAD identity却未重算原plan；新组合恢复原parser
  fixture的absent identity，独立plan construction已通过，不修改生产校验或历史回执。
- v176预登记DEVX-015所有D:/Work/devx015-auto-merge-receipt-contract-v176，启动前不存在。
  只重跑修正后的tests/test_devx015_workflow_integration.py::
  test_publication_auto_merge_cleanup_receipt_contract十三变体，-n16/loadfile，XML
  outputs/validation_runtime/devx015-auto-merge-receipt-contract-v176.xml；不重复五项已通过的原生来源测试。

### 2026-09-15：v174 边界通过；v175 原记录及原生来源回归预登记

- v174原85289终态13PASS12.47s，XML8397e908100922b6a2656aa1c3fc3c8696efe46cbd1191a85f39d63ccb44443d。
  实际Git调用新observer与十二文件/阶段边界通过，三生产模块严格mypy、Ruff和生成hook编译通过。
  四份原件保留v174根。仍需原公开hook/Full组合，不增加验收映射。
- v175预登记DEVX-015所有D:/Work/devx015-auto-merge-receipt-regression-v175，启动前不存在，
  -n16/loadfile，XML outputs/validation_runtime/devx015-auto-merge-receipt-regression-v175.xml。
  tests/test_devx015_workflow_integration.py::test_publication_auto_merge_cleanup_receipt_contract
  十三变体（明确synthetic仅记录契约）与tests/test_devx015_workflow_execution.py::
  test_git_reference_hook_requires_original_native_ancestor五种真实来源/拒绝/句柄回归。
  先验证有效构造再注入错误；不以导入/构造失败伪装安全拒绝。

### 2026-09-15：v173 原生锁生命周期通过；v174 窄阶段修正预登记

- v173原93590终态2PASS12.97s，XML9c508f78eaf69afa42dc81c7af408e4add00fad3ae4311384ef3aeafe605920d。
  本机Git2.45.1.windows.1在post-merge后AUTO_MERGE aborted/prepared持有同一空packed-refs.lock，
  committed已消失；拒绝锁时Git128而main已到C。原始探针/输出保留v173根，不等于Full发布验收。
- 修正仅原Git派生AUTO_MERGE清理hook：原fence再次验证contained ancestor/活worker，原
  FAST_FORWARD committed/post-merge前缀、原mainC/全部原元数据仍必需；锁只能空、同一原生身份，
  原hook新增可选cleanup_lock记录并在原store原子段前后复查。普通窗口、恢复/终态仍拒绝残留锁。
- v174预登记DEVX-015所有D:/Work/devx015-auto-merge-cleanup-boundaries-v174，启动前不存在；
  tests/test_devx015_workflow_integration.py的
  test_git_ff_only_reference_transaction_with_candidate_index[auto-lock-observer]及
  test_auto_merge_cleanup_lock_observer_boundaries十二变体，-n16/loadfile，XML
  outputs/validation_runtime/devx015-auto-merge-cleanup-boundaries-v174.xml。
  原生Git调用实际observer加文件/阶段负例；不冒充原Full/Job准入。完整hook记录/门禁和最终纵向仍待验。

### 2026-09-15：v172 正常发布失败但稳定 C 恢复通过；v173 锁生命周期复现

- v172 原2943终态1FAIL/2546.82s，XML
  131543e6e2da60ea37fd4f399faaedf9bc500bee2d5e4f64d224340f8203323a。
  实际Full74PASS250.87s，C a7f24f4a448d09948849bce2eb91d1b7fef3035b；原ORIG_HEAD和
  FAST_FORWARD prepared/committed、post-merge0已记录，AUTO_MERGE因packed-refs.lock拒绝，
  Git128/公开发布exit2；独立公开恢复exit0 LOCAL_PUBLISHED/RECOVERED_STABLE_C，原租约正常
  FAILED/RELEASED，原失败和Full不变。六份原源码/测试保留v172根；不计正常发布或整体验收PASS。
- v173预登记DEVX-015所有D:/Work/devx015-auto-merge-lock-native-repro-v173，启动前不存在。
  tests/test_devx015_workflow_integration.py::test_git_ff_only_reference_transaction_with_candidate_index
  的guarded、auto-lock-deny两变体，-n16/loadfile，XML
  outputs/validation_runtime/devx015-auto-merge-lock-native-repro-v173.xml。
  仅原生Git最小复现：记录真实hook顺序和packed-refs.lock身份/字节/终态消失，比较原生成功
  与拒绝锁时失败但main已到C；不是原Full/Job发布权限证据。不先扩大生产允许锁范围、不删现场锁。

### 2026-09-15：v171 原生修正回归通过，v172 新纵向 Full 预登记

- v171原68945终态30PASS/36.78s；XML
  54085254c3877b875ad63d8b6d36f61ad6448db8108dc51a4d5a2136a2b35c51，四份源/测试原件留存v171。
  八种ORIG_HEAD提交成功并拒绝持有目录rename，继承目录三种目标的abort/commit和父退出保护
  六种通过，四种身份/异常清理和八种原file只读回归通过，四种恢复分派便宜检查通过。
  已排除撤掉目录保护才能提交的方案；发布显式目录share-write仍不share-delete，默认保持不变。
- v172预登记DEVX-015所有D:/Work/devx015-publication-original-cli-full-v172，启动前不存在；
  外pytest16/loadfile，XML outputs/validation_runtime/devx015-publication-original-cli-full-v172.xml。
  唯一node test_original_publication_cli_ff_only_and_independent_recovery[full-profile-publish]，
  用新隔离C/原Full验证修正后的实际public入口、hooks、ff-only和独立recovery；不复用v169 Full
  给新代码授权，不修改v169原失败/独立采纳/FAILED收尾证据。不将fixture Full当成真实最终C Full。
  当前未启动/未通过；全部DEVX015/OPS080验收边界与未批准peer/admin范围保持不变。

### 2026-09-15：v170 定位目录共享冲突，v171 最小修正回归预登记

- v170原86284终态4PASS/4FAIL，16.38s，XML
  98c311c581626c081f9b7eac62761e1fd69fffdfbe63e325930967b4d7945d87；源/测试原件保留v170。
  无custody/root-only/refs-only/bounded-hook无common持有成功；common持有与all持有失败，
  无hook、bounded读取、普通读取均复现同一ORIG_HEAD commit exit128。原因限定到common目录
  只共享读取的持有模式与Git提交冲突，不归咎于Full、hook读取或Git安装文件硬链接。
- 原hold_bound_directory默认不变；仅显式allow_child_updates=True的目录custody允许共享写入，
  仍不共享删除/重命名，原file/input custody不变。仅原publish HEAD交接目录保持阶段启用。
  实现与严格类型检查通过，但兼容提交和目录替换拒绝必须由下列原生测试证明，不以静态证明。
  共享模式语义来源：Microsoft CreateFileW dwShareMode（FILE_SHARE_WRITE / FILE_SHARE_DELETE）。
- v171预登记DEVX-015所有D:/Work/devx015-publication-directory-share-fix-v171，启动前不存在；
  外16/loadfile，XML outputs/validation_runtime/devx015-publication-directory-share-fix-v171.xml。
  8原ORIG_HEAD提交与持有期间rename拒绝、6原继承目录abort/commit及父退出后保护、
  4原目录身份/异常释放回归、8原文件只读custody回归、4恢复分派便宜合同检查，共30个。
  分派检查的authority是显式synthetic，不冒充原Full。此轮通过后才考虑新完整公开发布链路。
  这仍不是全部P01/41剩余变体或final C/OPS080整体验收。

### 2026-09-15：v169 原 Full 通过，真实合并失败，原失败已独立采纳

- v169原93065终态1FAIL/1ERROR，1728.19s；XML
  90b1615bd3bb64419075ede0386e7e05b3095881670554e233bf83eacd7fe563。
  隔离C6513cfdbd97e21cc34090d45ddaeea3cfa21af17的原Full实际PASS/full/exit0/non-print，
  74PASS238.13s，summary b942932d4256bba4cd7fc70e6eb8b75c6b9c224b1286b24f5bc2f552352f8082。
  原public worker已实走hooks_created/ready_held/heads_switched/merge_resumed；Git exit128，
  prepared ORIG_HEAD hook成功记录五级原生祖先链后，Git报couldn't set ORIG_HEAD。未成功发布。
- 同一原public recovery实写head_recovery RESTORED，但index未被Git替换，原入口错误选择
  index-replaced adopter，拒绝PUBLICATION_INDEX_REPLACEMENT_REQUIRED，且拒绝未采纳release。
  使用隔离C内未改动的原版本API adopt_unchanged_failed_attempt（原60965终态PASS）完成
  ORIGINAL_UNCHANGED独立采纳，再由原23007正常FAILED/RELEASED收尾；不改旧C/Full/失败记录。
  原readiness持有17885文件，但无.git文件；尚未证明Windows冲突的具体来源，不盲目移除custody。
- 实际源码recover入口已在租约到期前按真实index状态选择原两个adopter，静态检查PASS，
  待针对性测试；不将此静态结果当作原发布成功。六份v169源/测试原件留存根目录。
  source v4到期后已由原API FAILED/RELEASED，v5同范围续接TASK_SOURCE_PRE_WRITE、LANE PASS；
  未扩TTL、未新建store/branch/worktree，未启动Pi或真实main/peer/admin/OPS操作。
- v170预登记DEVX-015所有D:/Work/devx015-orig-head-native-commit-repro-v170，启动前不存在；
  外pytest16/loadfile，XML outputs/validation_runtime/devx015-orig-head-native-commit-repro-v170.xml。
  最小真实ORIG_HEAD提交对比无custody/root/common/refs持有、普通读取与bounded读取hook，
  八个场景定位v169提交失败；只作故障复现，不重复Full或计作P01整体验收。
  原41变体/14counters/10mutants/finalC全门禁/迁移发布closeout及OPS080完成要求不变。

### 2026-09-15：原发布入口与 M 侧恢复纵向路径，待运行验收

- 原 public local-publish/worker/hook 接入固定 Git ff-only、原 Job/native origin、原 Full
  与同一事务历史。git_merge 先记录 resume 意图，再记录 prepared 文件身份、hook 与真实退出。
  Git argv 增加 maintenance.auto=false；不禁用原固定 hooks，不启动替代执行器。
  独立 LOCAL_PUBLISHED v3 观察重验 C/HEAD/index/ref prepared identity 与 Full profile。
- local-publication-recover 只使用原事务：原 Job/Git 确认退出后，main=C 走独立采纳；main=M
  仅恢复原计划中的 HEAD 与原 prepared ORIG_HEAD/锁。head_recovery 先持久化 INTENT，
  按逆序恢复 candidate HEAD、peer HEAD，保存 ORIG_HEAD 原字节（不伪造已替换的旧文件身份），
  仅删除身份及字节完全匹配的原 prepared 锁，再记录 RESTORED。未知锁、未知 HEAD、main 前进
  都拒绝且保留现场；peer index/private files 不变。未采纳前不准 release，不将失败改成 PASS。
- v168 预登记 DEVX-015 所有的 D:/Work/devx015-publication-recovery-vertical-precheck-v168，
  启动前不存在；外 pytest 16/loadfile，XML
  outputs/validation_runtime/devx015-publication-recovery-vertical-precheck-v168.xml。
  12真实 Git/native 文件恢复观察与 receipt 合同 + 原15 switched-window回归；这不是原 Full 权限。
- v169 预登记 DEVX-015 所有的 D:/Work/devx015-publication-original-cli-full-v169，启动前不存在；
  外 pytest 16/loadfile，XML outputs/validation_runtime/devx015-publication-original-cli-full-v169.xml。
  单个新原 Full fixture →真实 public local-publish/worker/hooks/ff-only→新进程 recovery，
  不重跑旧 prepare/abort。若正常路径失败，保留原失败并尝试同事务公开恢复，不能算成功路径 PASS。
  v168 原77787已终态27PASS/183.56s，XML
  d025cd3b7ab263bdf2b52fb98f998c1153ba196f7738a6b0254f50061a669911；六份源/测试原件留在v168。
  该前置检查不证明原 Full/store 权限；v169尚未启动。65/106映射不是通过数，
  41变体/14 counters/10 mutants/final C 全门禁、
  migration/publication/closeout 与 OPS080 工程/部署/新 lawful daily 的完成要求均保持不变。

### 2026-09-15：HEAD handoff 后原窄门禁的真实 Git 验证

- v167原25662终态31PASS/114.37s；XML
  fc9dd00f4ce335d3976a669a1ad228c69bf13c6406e8c21a997baeb16d5b251e。
  15新真实Git/native HEAD观察与16原拓扑/plan回归通过，三个SUT及测试原件保留v167。
  原fence/Full/worker合成入口目前只通过静态检查，尚未以原Full纵向运行；不得提前计入P01。
  下一唯一整体验收目标为P01.checkout_recovery：先贯通原入口/固定hook/ff-only/稳定成功失败恢复，
  再取原CLI新进程恢复实证；不再为未变化的prepare/abort重复Full。65/106映射、余41仍以
  config/architecture/devx_015_workflow_acceptance.v1.json为权威，不将新31例视为整体验收数。

- v167 预登记 D:/Work/devx015-switched-head-native-window-v167，DEVX-015所有、启动前不存在；
  外16/loadfile，XML outputs/validation_runtime/devx015-switched-head-native-window-v167.xml。
  新15例真实Git/原生HEAD写入：普通/linked/packed正例，未切换、main/source分支变化、index、
  HEAD身份替换、peer index、配置、reflog、ORIG_HEAD、未知锁、额外worktree、捕获中漂移拒绝。
  回归原拓扑capture及checkout plan；只读observer不授予Full/lease权限，不声称原事务发布已通过。
- fence.validate_publication_head_handoff只从本fence原active lease历史取得HEADS_SWITCHED记录，
  重验LOCAL、plan、原request/LOCAL intent、执行合同及仅HEAD变化的实际现场；普通validate
  仍严格HEAD=C。原PublicationLifecycle可在窄门禁后重验原Full custody与实际worker身份；
  switch_publication_heads末尾接入该门禁。所有原生进程/真实Full/完整merge恢复仍须另行实证。

### 2026-09-15：原 Git launch 与 HEAD handoff 合同预登记

- v166原32121终态102PASS/17.04s；XML
  f05e28ba09f8405dd11e8ea966a11ee0e88309ccc8ae40b20de8a7ab272c5d2a。
  原SUT d70dc0aabc4ea181228431955187ffa0fe24186de91c8a7e5d5aa7c0ac155d99 与测试
  b0d52ae6863e8678046ba1387900606ac58428fd61d0e4205ae21e31c3178db2 保留v166。
  仅31launch+19effect绑定+12顺序及40旧合同回归，不替代新原事务native positive/公开hook与
  ff-only、部分HEAD恢复、独立stable adoption。真实HEAD/main仍为原S/M，无实际接管或发布。

- v165原30480终态62FAIL/40PASS/22.93s；62新增例均在synthetic fixture缺少job_name处失败，
  未触达生产校验，不计为生产red。原SUT/测试保留v165。补齐原shape request的job_name后从源头
  重导出绑定，不替换生产native证明；v166预登记D:/Work/devx015-publication-git-launch-contract-v166，
  启动前不存在，DEVX-015所有，同102例与外16/loadfile；XML
  outputs/validation_runtime/devx015-publication-git-launch-contract-v166.xml。

- v165 预登记 DEVX-015 所有的 D:/Work/devx015-publication-git-launch-contract-v165，启动前不存在；
  外16/loadfile；XML outputs/validation_runtime/devx015-publication-git-launch-contract-v165.xml。
  新launch绑定31例、HEAD effect绑定19例与顺序12例，并回归原ready绑定/checkout plan顺序。
  仅synthetic合同，不创建Full、租约或发布权限；所有真实发布验收仍须原事务活句柄链路证明。
- prepare_git_launch 接原ready活文件、原Job及三份实际Git文件custody后创建悬停ff-only child，
  绑定原git_launch；switch_publication_heads 在同一原store先追加HEAD_HANDOFF_INTENT，
  再以原native身份和before bytes执行HEAD条件写入，最后HEADS_SWITCHED；目录custody保持至
  调用体退出，不授予resume/PASS。peer选项必须显式请求，真实peer授权未收到，未执行该接管。
  实际public hook、resume effect-window、stable成功采用与部分HEAD恢复仍待完整接通与实证。

### 2026-09-14：原 Git launch 失败恢复验证预登记

- v164 原55775终态25PASS/15.83s；XML
  85a1589b04e57f46b29b6c46ef397d414c53865e5fe4da8aaa3360961ce504c9。
  原生进程/文件七例及15稳定观察+3Full projection回归通过，原SUT/测试保留预登记根目录。
  该七例使用真实 Python 进程身份，不声称实际 Git 发布或原 Full 事务已完成。

- v164 预登记 DEVX-015 所有的 D:/Work/devx015-git-launch-recovery-v164，启动前确认不存在；
  外层 pytest 16/loadfile；XML outputs/validation_runtime/devx015-git-launch-recovery-v164.xml。
  原生退出/仍存活进程与真实 ORIG_HEAD、两份 reflog、未知锁/merge state 七例，另回归原稳定
  observation 和 Full projection。只验证失败恢复只读拒绝，不冒充真实 Full、ff-only 发布或 P01 验收。
  原 Git launch 记录及 effect-window/public hook/最终发布仍需完整连通；保留全部整体剩余验收项。

### 2026-09-14：X04原等待请求释放后重试

- v104原42954终态2PASS/24.88s，XMLae038f3fc5b0776eefa9df377555ab5abd38d99b6f09ec403d67e4e95e67fd29。
  两种原CLI失效身份均拒绝，后续原等待请求获准并释放，无active，refs/index及所有非owner治理
  原bytes不变；保留owner实证为execution_lease_os_arbiter_owner.v2/RELEASED/integration-coordinator。
  测试d3d26bfdc20602094d2e88d0cc1efb00e9b25132e4b0cdf83667cff820703dbc及kernel保留v104。
  结合v101有效反证、v102原store/evaluator四失效+恢复不消费状态及正常回归，只新增invalid_request
  六节点映射；现65/106，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，不是65已验收。
  41未映射、取消真实语义/接口、P01受控执行与恢复、统一14counters/10目标mutants、finalC全门禁、
  迁移发布及OPS080工程部署新合法daily仍需完成。没有第二store/scheduler、native修改或Pi。
- v103原50150终态2FAIL/18.95s，XMLf3a622d1ef360f5c977ee459aa8a0e2a48bd0a8846190163d38608f28f70e833；
  原CLI正确typed拒绝，失败在过宽governance全字节断言；不能据此认定失效请求被接纳。
  CLI初始化会更新原OS arbiter诊断owner，沿用既有V03 oracle：文件集合及全部非owner原bytes必须
  相同，仅精确leases/arbiter.owner.json可变且schema/RELEASED必须有效，额外保留两份原owner。
  v104预登记D:/Work/devx015-x04-invalid-public-cli-v104，启动前不存在，DEVX-015拥有，外16/loadfile；
  XML outputs/validation_runtime/devx015-x04-invalid-public-cli-20260914-v104.xml；生产不变，仅纠正oracle，
  若仍有非owner差异即失败并按实际差异诊断；保留证据后治理清理。
- v102原98529终态8PASS/44.60s，XML5e149ba0391a456c7fede0081e32e3962fc4d9a85efe13710880f34cdbc5e478。
  四失效readiness拒绝且无事件，后续有效等待者前进；失效reassign也保持原EXPIRED，正确恢复
  仍成功。原公开单等待/有限竞争及原reassign回归通过。kernel
  cf723fbc0c6b4c5380748802079525d871b8fae53b0320ce39c2bbfe36c0e107；原件保留v102。
  Ruff/strict mypy/scoped diff PASS，flow/RCF同步a4a99293d44c113e84f61435eda494d56599d8f5f0dc314a094e69ad46016908。
  v103预登记D:/Work/devx015-x04-invalid-public-cli-v103，启动前不存在，DEVX-015拥有；
  XML outputs/validation_runtime/devx015-x04-invalid-public-cli-20260914-v103.xml，外16/loadfile。
  原公开CLI队首BLOCKED后提交失效lane/main身份，验证原typed拒绝、lease事件不变，原后续waiter前进；
  补齐公开入口失效序列，不替代readiness错配实证，也不声称已完成取消接口。保留证据后治理清理。
- v101原进程直接终态1PASS/3FAIL/7.91s，XML00e4a4ee6886c8978b69bfb0c92b1333459383de5e7b503f828925352608b8ed。
  status失效正确拒绝；manifest/change/policy三个单字段错配实际未抛异常，触达错误接纳，
  setup及原holder释放全部通过；原kernel/测试保留v101，非fixture/import/collection失败。
  修复原acquire/reassign共用精确readiness主体绑定，且reassign在旧事件改写前校验。
  v102预登记D:/Work/devx015-x04-invalid-readiness-v102，启动前不存在，DEVX-015拥有，
  XML outputs/validation_runtime/devx015-x04-invalid-readiness-20260914-v102.xml，外16/loadfile；
  原四个失效序列加错误恢复不消费旧状态/正确恢复仍可执行，以及公开竞争/原reassign回归。
  不新增store或接口；保留证据后治理清理，不据此声明取消/最终验收完成。
- v101预登记D:/Work/devx015-x04-invalid-readiness-v101，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-x04-invalid-readiness-20260914-v101.xml，外16/loadfile。
  原store/evaluator建立holder与两个BLOCKED waiter；实际释放后，首waiter使用单字段失效的
  readiness(status/manifest/change/policy)，应在任何lease事件前拒绝，后续有效原waiter仍可前进。
  先不改生产代码，检查身份串用是否实际触达；这不是Full/发布证据，也不覆盖取消接口。
  保留原始XML/代码；退出后按证据归档及无进程条件治理清理。
- v100原91529终态1PASS/26.70s，XML ff48c5bef0227cfc7062ca5dab1b9cc8dd2a15a26aa4ce6e6f9b648d7a3f4371。
  一个holder及三个原waiter均经原CLI逐个持有、释放，所有中间冲突保持原lease heads，
  每个原请求最终获准且关联自身旧BLOCKED；旧事件字节、refs/index不变，最终无active。
  原kernel及测试保留v100；测试SHA cbc7f6ee09f63dd3b06c2f09fc2cbd9debb55422422d340a755c04f335d7f465。
  只新增finite_competition映射，现64/106，仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED；
  42未映射、统一14counters、10目标mutants、finalC全部tiers/实际Full和迁移发布仍未完成。
- v100预登记：D:/Work/devx015-x04-finite-contention-v100，DEVX-015拥有，启动前核实不存在；
  XML outputs/validation_runtime/devx015-x04-finite-contention-20260914-v100.xml。
  原公开CLI一个holder及三个同资源waiter，逐次有限释放；每轮未获准者重试保持原BLOCKED，
  每个获准者独占且关联自己的旧请求，最后全部通过原release终态、无active、refs/index不变。
  外16/loadfile，仅映射实际序列，不宣称任意调度公平性；不增加生产store/queue/scheduler。
  原始CLI输出和XML保留，完成证据归档与无进程审计后治理清理。
- v99原4555终态3PASS/20.20s，XML41594e1b49b93ed9841b2215e40e4540bc943175d8e5c75089dba590b4c172ad。
  原公开CLI冲突重放不新增lease，原持有者release后同完整请求成功建立关联新lease，旧BLOCKED
  event bytes不变、重复成功请求重放同active，refs/index不变；原请求最终走原release。
  原store验证错误actor无副作用；阻塞后generation2仍可一次执行reassign至3，第二次恢复仍按
  原配额拒绝；既有reassign回归通过。只映射X04.eligible_waiter_after_release，现63/106映射，
  仍PARTIAL/NOT_EXECUTED，43未映射+所有统一计数/10mutants/finalC门禁/OPS080继续。
  kernel SHAb86b203e6f25348ccf0dec4c433e43bfcb0ed86e678ff12dfd73c4caf72fcc59，测试原件保留v99。
  Ruff/strict mypy/scoped diff通过；flow与RCF SHA同步438eccd547af52af00f61ab5fd44f145b2a8d83a26aef831646985b044367db4。
- v98原6398终态1FAIL/12.96s，XML21c8591238f9fd7be6fd6eaa8f8536ed1de5d83493cc4dfd0d369a44dbfaba73。
  实际A公开acquire成功、B记录BLOCKED、A公开release成功且active为空；B同请求新CLI实际exit1，
  stderr为CheckoutGuardError LEASE_ALREADY_TERMINAL，触达原缺陷。修复前kernel/测试原件保留v98。
  修复方向：原BLOCKED事件不变，完整身份/当前readiness复核后，仅资源已释放才在原store创建
  previous_lease_id关联的新generation；仍有冲突时原BLOCKED重放，不制造新尝试。执行reassign
  配额按原链中实际REASSIGNED计数，不能因从未执行的BLOCKED消耗。不是新增scheduler或公平队列。
- 修复验证预登记D:/Work/devx015-x04-waiter-retry-v99，启动前不存在，DEVX-015拥有，外16/loadfile，
  XML outputs/validation_runtime/devx015-x04-waiter-retry-20260914-v99.xml；原公开等待序列与
  原lease reassign回归、阻塞后执行配额验证，保留证据后治理清理。取消/失效/有限竞争仍未完成。
- 原kernel acquire持久化REQUESTED→BLOCKED，BLOCKED在原转换图中为终态；原checkout/publication
  同请求重试未使用新generation或显式恢复路径。先通过原公开CLI A持有→B冲突→A实际release→
  B同完整请求新进程重试复现，不能只据静态代码宣告失败，也不把shadow scheduler选择当执行公平性。
- 预登记D:/Work/devx015-x04-original-waiter-v98，DEVX-015拥有，启动前不存在，外16/loadfile，
  XML outputs/validation_runtime/devx015-x04-original-waiter-20260914-v98.xml。现有生产代码不改，
  test_x04_original_waiter_retries_after_holder_release要求原请求在实际释放后可前进；保留所有
  原CLI argv/退出/输出及原BLOCKED证据，无新store/scheduler/手改generation；退出后证据保留并治理清理。
  历史BLOCKED终态不得直接改写；若证实缺口，设计原store内可审计的新尝试关联并复核完整身份。
  这里只覆盖eligible_waiter_after_release的必要序列，取消队首/失效请求/有限竞争仍需实际验证。

### 2026-09-14：P01实际本地发布拓扑与恢复接缝

- v163原62993终态17PASS12.02s，XML0ebfb24016ef6f0a067975cf1d573ec5283862d09342803e72d2ecc6a915b09f；
  原namespace/root/parent/leaf/bytes/multilink/missing/异常及默认/显式预算边界全部保持，源码与
  测试原件保留v163。结合v162共28局部检查通过，安装Git硬链接读取兼容性已实测，下一步直接
  接入原事务Git pre-resume、HEAD/ref效果窗口和真实publisher；这些仍不构成P01/整体验收。
- v162原17456终态11PASS17.43s，XML4a2e6046c4bfbb44804dc65598b96b08a09d8d4bdfefe3c29521e2b3c5bfd2d2。
  实际安装Git的cmd/真实mingw64 Git/sh已按原2/4/1链接数完成只读持有；未写安装路径。
  自有fixture中，默认/错数/错身份拒绝，有效及异常路径两别名实际Win32写拒绝32且退出恢复可写；
  原暂停child继承v2，父custody关闭后仍保护两别名，原Job退出恢复可写，v1/forged/closed回归通过。
  contract SUT90962c84f15c7614b19684094bfe7cbb2ae5b5876b4e9812e1db374c1621e736，测试
  71c1ce9afc833245a351326c223dcb3b74d2dd9b265b58a9f7e7fd19fb6b3be1，原件保留v162。
  v163预登记D:/Work/devx015-read-custody-regression-v163，启动前不存在、DEVX015拥有，外16/loadfile，
  原只读custody namespace/内容/单链接/异常8项及预算9项，共17项；XML
  outputs/validation_runtime/devx015-read-custody-regression-20260914-v163.xml；原件保留后治理清理。
  仍必须完成原事务Git启动绑定/真实ff-only效果窗口/独立稳定采用，不把本兼容保护当完整发布。
- 原Git启动输入接入的现场约束：安装cmd/git.exe为2 links，实际mingw64/bin/git.exe为4 links，
  usr/bin/sh.exe为1 link；不能用仅保护另一份单链接启动器替代实际执行程序。原bounded_regular_bytes
  已可绑定精确链接数，但活hold只接受1，需保留普通源码默认拒绝并补齐安装文件的只读保护。
  hold_bound_read_file新增显式expected_link_count，非1时使用v2绑定；同原RootDirectory原生
  打开校验实际数量/身份/bytes，binding从活leaf查询FILE_STANDARD_INFO再次核对数量/大小。
  私有file I/O多链接参数仅允许retain_read_file_handles路径，写/创建/删除默认仍为1；
  v1结构保持，原ready v1验证不自动接受v2。原Git启动绑定尚未接入，不假称已可发布。
  v162预登记D:/Work/devx015-git-image-read-custody-v162，启动前不存在，DEVX015拥有，外16/loadfile；
  5个hardlink持有正反/异常、1个实际安装Git三文件只读保护、4个原inherited read custody变体
  （新增hardlink-success）及1个原bounded hardlink默认拒绝，共11项；XML
  outputs/validation_runtime/devx015-git-image-read-custody-20260914-v162.xml。
  不写任何安装文件；只在自有fixture验证别名Win32写拒绝及原child继承/退出恢复可写。
  保留原件后治理清理，随后继续原事务Git pre-resume/效果窗口/真实ff-only及稳定采用。
- v161原49372终态13PASS24.15s，XML37e8f4daffb0ac99015fc807ced747c8da8cd26e1a7c9957b3deb71edad6dd21；
  原inherited child、非成员零副作用、暂停创建与单resume、父先退出不早释放、launcher死亡整树
  清理均通过，源码与测试原件保留v161。v160实际有效/调用体异常为4层原生链，handle count
  168→172→168，三个拒绝路径168→168。没有新增Job或保留query Job句柄。
  接下来须把来源原语接入原事务Git pre-resume绑定、真实ff-only效果窗口及独立稳定采用；
  不再把来源原语或准备组件当作完整publisher，也不据这18项局部检查宣布65/106已验收。
- v160原46508终态5PASS17.52s，XML SHA7d68d72f8e95eb56a575ca47b20b41d33b3072ffc23bddfdd94c72972e4eaa15。
  实际Git prepared hook正例原生链匹配原Git并写入唯一测试ref；错创建时间/同Job sibling/错误Job
  在Git效果前拒绝，main及目标ref不变；调用体异常拒绝且关闭句柄。独立原生handle count在
  yield内恰为基线+链长（没有多余query Job/snapshot），结束恢复基线，原Job退出消失。
  execution SUT fb44f3b3b2eccf89b1dba1e9e13834c6611ea4a10239ac6c0a79ac3756597d4b，测试
  4b2a46c0bcdf5118707b1dddb874ca00a0478aa58fabe8078783728ae3e60036，原件保留v160；
  Ruff/该源strict mypy PASS；该来源组件仍未接入真实发布事务，不作为P01或Full接受。
  v161预登记D:/Work/devx015-native-origin-containment-v161，启动前不存在，DEVX015拥有，
  外16/loadfile，原6 inherited-child/2非成员拒绝/1暂停创建/2父先退出/2launcher死亡共13回归，
  XML outputs/validation_runtime/devx015-native-origin-containment-20260914-v161.xml；保留证据后治理清理。
- 原fence固定hook的真实来源尚不能仅用同Job成员推断。新增hold_contained_ancestor原生上下文：
  当前hook至原Git最多8层，快照仅定位parent PID，每层实际OpenProcess/创建时间/活状态/原Job
  复核，原Git完整身份必须匹配；持有全部process句柄并在上下文前后重查。query Job句柄不跨
  yield保留，避免破坏原launcher kill-on-last-handle；所有错误路径关闭本次观察句柄。
  参考Microsoft PROCESSENTRY32W与父PID复用说明：
  https://learn.microsoft.com/en-us/windows/win32/api/tlhelp32/ns-tlhelp32-processentry32w；
  https://devblogs.microsoft.com/oldnewthing/20150403-00/?p=44313。
  原语只证明来源，不是新lease/恢复/执行许可，不加入常拒绝的占位CLI冒充完整hook。
  v160预登记D:/Work/devx015-git-hook-native-origin-v160，DEVX015拥有且启动前核实不存在，
  外16/loadfile，XML outputs/validation_runtime/devx015-git-hook-native-origin-20260914-v160.xml。
  实际Git reference-transaction prepared hook的有效来源、错创建时间、同Job非祖先、错误Job及
  调用体异常5项，独立Win32核验活链与句柄数、拒绝前后ref不变、退出Job消失；不是baseline red、
  Full或真实发布事务验收。原件保留后治理清理，随后必须接入真实ff-only发布与效果窗口。
- v159原32142终态1PASS1472.96s；独立XML1/0/0/0/time1472.932，SHA
  375151cf5878cd2d3779bb9bf38262840df93c9c1ca3d48084f568096c9a593c。
  fixture C898880e0001a4f5fb7578190b0342e4de2997629实际Full PASS/full/exit0/非print，summary
  ee295516c516ea45c5f9bf65bd77d70d40e27d27017e4591f06bcdbf48a6ace5。
  原worker71356/134338676772700321，17885实际输入含16556 runtime与1325 profile captures；
  原Git49272/134338681654091108。阶段ready_held291.453s、main_prepare_begin353.156s、
  main_prepared516.547s、main_prepare_aborted523.453s；实际准备阶段163.391s超过旧预算剩余时间，
  两段有界等待已越过该测试缺陷，不改变生产门禁或把进度日志当PASS。
  原三处Win32写拒绝、readset/目录句柄寿命、ready/preparation篡改拒绝、未知lock保留、
  原Job/Git退出、伪worker PASS拒绝、Full投影保持、原CLI索引替换恢复与REPLAY_ONLY、失败释放均通过。
  独立公开replay PASS/FAILED/issues[]；测试与SUT原件保留D:/Work/devx015-publication-phase-full-v159。
  此为准备/回滚/失败恢复整链，不是实际ff-only成功发布；未添加P01完整变体映射。
  下一步仍为原fence public hook入口及真实publisher/effect-window/独立稳定采用，不以新小探针
  代替端到端交付；65/106映射、41缺失、统一14计数/10目标mutants/finalC全部门禁/迁移发布及
  OPS080工程部署新合法daily均仍必需。无Pi、第二store/调度器、新agent或未授权main-peer/admin动作。
- v157原2007终态1FAIL1ERROR1046.18s，XML
  ee1191b55d9051874db989817610cfb61916c2ec016f6d12e1dad073ac3e8c0b。
  原fixture C58f1037425fcbd6f2f6b7fda232b7197c5b47a10实际Full PASS且非print；同步阶段为
  hooks_created66.672s/ready_held232.500s/inputs_released240.766s/source_drift_rejected290.766s，
  同时进入main_prepare，随后原总420s观察上限到达，无main_preparation。
  原JobABSENT，launcher79772/134338654716470526、primary82124/134338660918678816、
  worker75468/134338660919781288均EXITED；原recover/adopt_unchanged成功且Full投影不变。
  释放前仍有效原事务只读观测：original_binding8.875s、current_profile81.468s/PASS/1325 captures、
  original_topology12.704s/OBSERVED。prepare_main_reference仍包含多次上述复核，不能将前半段
  消耗后的129s误作完整后半段预算。仅将测试外层分为输入阶段420s及Git准备阶段420s；
  同步stdout只触发下一段有界等待，不是完成见证/恢复或发布权限；最终全部原断言保留。
  生产180s profile/原Job与真实身份/Full/发布门禁均未改，SUT仍81ec021762e50103c39506819f882e9fdaca7301a4faabb4d731095289459073。
  v158预登记D:/Work/devx015-publication-phase-wait-v158，启动前不存在，DEVX015拥有，外16/loadfile，
  同步日志与阶段边界2项，XML outputs/validation_runtime/devx015-publication-phase-wait-20260914-v158.xml。
  v159预登记D:/Work/devx015-publication-phase-full-v159，同样启动前不存在、DEVX015拥有、外16/loadfile，
  原index-replaced-full-profile-publish整链1项，XML outputs/validation_runtime/devx015-publication-phase-full-20260914-v159.xml。
  保留原件后按治理清理；这些不是P01或DEVX015整体验收，后续真实publisher/41variants/finalC/OPS080范围不变。
- v154原55421终态95PASS65.45s，XML1a396af19b5fe620babf6e2a73535e3d044884d17d80be4002c3561bfb8c571d。
  v155原90193终态1FAIL1ERROR1027.22s，XML1d2bb1ef0b063f5bb21951b40759461251a54f9794579630ba08cf9fa83b3e53。
  本次不是420s观察超时：400s定时栈诊断输出至pathlib._cparts后原worker已退出。Windows
  Application1000在22:02:20记录83216/134338641400595592，精确匹配原worker，异常0xc0000005，
  python311.dll偏移0x259cf3。当前与原Full同SHA的engine中_Py_DumpTraceback导出RVA0x259a6c，
  差0x287，吻合已保留v79 cdb栈Py_DumpTraceback+0x287；不得再次引入该定时诊断。
  本次cdb启动AccessDenied，未提权/复制执行器绕过、未上传dump；以WER身份/偏移、原栈输出、
  原Full engine SHA与本地导出RVA交叉核对，不伪称本次cdb已成功。
  fixture Ca0d1687cd75a78810c33ba828d4c92540ddf2b48实际Full74PASS239.38s且非print，17885输入
  与原子进程保护到达；无main_preparation。原JobABSENT与launcher/primary/workerEXITED，原
  recover/adopt_unchanged闭合且Full投影不变；22:12JST原公开FAILED/RELEASED，lease-012e5c240861be4198a5。
  释放前对仍有效原事务执行只读_prepare_current_full_profile，83.906s/PASS/1325 captures，
  execution SHA6d3eaf06e5cf07032206d744d9dbc74244f26502e3a0ce9bf3683564bcd39a1b；未派发Full或Git，
  不据此接受整链。生产SUT仍为v154的81ec021762e50103c39506819f882e9fdaca7301a4faabb4d731095289459073。
  仅移除测试worker定时faulthandler，换成显式阶段调用的同步stdout日志；无定时器/线程/恢复授权。
  预登记v156 D:/Work/devx015-publication-stage-trace-v156，DEVX015拥有且启动前不存在，外16/loadfile，
  新同步日志1项，XML outputs/validation_runtime/devx015-publication-stage-trace-20260914-v156.xml。
  预登记v157 D:/Work/devx015-publication-stage-full-v157，同样DEVX015拥有且启动前不存在，外16/loadfile，
  原index-replaced-full-profile-publish整链1项，XML outputs/validation_runtime/devx015-publication-stage-full-20260914-v157.xml。
  原95项生产回归复用未改SUT的结论；180/420s边界未改，须用新实际整链终态确认，DEVX015/OPS080仍未完成。
- v152原36321终态88PASS21.91s，XML0d7429626517fd83e5816dbf4cf0c37efc557d53eb890e1364b9a57166de3b0a。
  v153原69531终态1FAIL1ERROR1049.49s，再次是420s worker witness超时；XML
  b8d4392658f17ab10f48b85234109fc03403225f330cc7f8fb275112f08c020b。
  fixture Cad6776373a7b37a5bee94c12a5d7ea0b82ba5e36实际Full74PASS240.86s，ready/三处32拒绝
  到达但无main_preparation；独立JobABSENT与原launcher/primary/workerEXITED，原recover/
  adopt_unchanged完成且Full投影不变；21:30:33JST原公开FAILED/RELEASED，lease-c79159d463119cee4b7c。
  两次等价超时后不再盲目重跑，改为原完整_require_original_publication剖析：带cProfile开销
  34.128s，其中两次store.replay累计32.362s、25次ready结构校验及132次Full投影造成重复工作。
  修复Full投影先筛选再深复制；公开validate_execution_transition完整校验current树一次后，
  私有递归只重核每层边规则，不再次校验同一已验证子树；不跨事件/调用缓存或跳过现场检查。
  用原v153归档代码与新代码对同一原32事件完整store.replay交替测量，9.781s ->4.986s，
  两者PASS且同唯一event head、issues空。诊断进程仅临时切换两份完整校验实现，未省略检查、
  未修改磁盘/租约，不能当实际发布授权或整链验收。
  预登记v154 D:/Work/devx015-ready-replay-contract-v154，DEVX015拥有且启动前不存在，外16/loadfile；
  原v152的88项加Full投影3项、原Full adapter[0]及两类安装转换回归3项，共95项。
  XML outputs/validation_runtime/devx015-ready-replay-contract-20260914-v154.xml。
  预登记v155 D:/Work/devx015-ready-replay-full-v155，同样DEVX015拥有且启动前不存在，外16/loadfile；
  原index-replaced-full-profile-publish整链1项，XML outputs/validation_runtime/devx015-ready-replay-full-20260914-v155.xml。
  180/420s边界不变；测试worker仅增加400s一次faulthandler栈输出，若再超时可定位具体阶段，
  不延时、不自动重启、不改变结果；结束时取消该诊断。仍需实际整链通过及完整DEVX015/OPS080验收。
- v150原54098终态86PASS22.53s，XML288965a13e7b00f6166051bbba3e519a6f852616f2958992c11218815b54d7ad。
  v151原44340终态1FAIL1ERROR1053.91s，XML95442d5c62cb3354fe4f83d08443594774c960e416149020ee73c74794420e4c。
  fixture C89e9a945e81f90e4d5ddfa371205dd64523ff720实际Full74PASS240.21s/full非print；
  ready已原store一次追加，17885活输入/child精确继承、三处原生Win32 32写拒绝已到达。
  worker未在原420s内到达main_preparation，未将局部事实升级为整链PASS。独立确认JobABSENT、
  launcher/workerEXITED、无main_preparation；原recover(observe)及adopt_unchanged完成，Full投影
  不变；21:02JST原公开release FAILED/RELEASED，lease-baf0116c98e0ebed8465，原失败XML保留。
  原首次release证据在fixture外被PUBLICATION_EVIDENCE_FILE_MISSING拒绝；仅将同XML复制到
  fixture outputs/v151-failure.xml后原接口完成，不改receipt/lease，不重启失败worker。
  剖析原ready验证单次0.676/0.681/0.682s，相同17885清单的单次调用内词法Path复用后
  0.300/0.321/0.324s；仅消除重复root/parent解析，所有输入校验仍执行，不缓存验证结论。
  保留180/420s原边界；该测量不是整链性能验收。新增relative-root及parent-traversal反证。
  预登记v152 D:/Work/devx015-hook-ready-lexical-contract-v152，DEVX015拥有且启动前不存在；
  外16/loadfile，原v150节点加两反证共88项，XML outputs/validation_runtime/devx015-hook-ready-lexical-contract-20260914-v152.xml。
  预登记v153 D:/Work/devx015-hook-ready-lexical-full-v153，同样DEVX015拥有且启动前不存在；
  外16/loadfile，原index-replaced-full-profile-publish实际Full整链1项，XML outputs/validation_runtime/devx015-hook-ready-lexical-full-20260914-v153.xml。
  整体41缺失变体/最终候选门禁发布及OPS080仍须继续，未启动Pi/SoL-Pi。
- 20:10JST确认原final-v3为TASK_SOURCE_PRE_WRITE/execution=None，无运行测试或canonical writer，
  原公开release按FAILED/RELEASED保留v149 XML，未把fixture Full当真实finalC或Full parent。
  final-v4继承相同S/main/全部scope/四生成器/mandatory tiers，新lease-4afec427f8e480a402ac；
  首次因重复传入两项自动资源而在创建前拒绝，确认无新事务/active后仅移除CLI自动追加项，
  成功获取；最终存储scope逐字段与v3相同，LANE PASS并进入TASK_SOURCE_PRE_WRITE，无TTL改写。
  新事务SHA1e94da8510374c52ef2ab1c5dd59848f0e107c7b2f676fe114bee1ffe7dc0d4b。
- ready实现：原worker内部进入hold_hook_capsule_inputs，同原store复核实际活binding、原Full
  capture/profile/clean状态后一次追加workflow_publication_hook_ready.v1。outer首次转换要求
  profile.execution_sha256等于真实prior.execution完整hash，新outer只将最后ready还原None后
  必须完全等于prior；不做后续状态剥离/自引用hash/复制第二份Full。后续ready不可改写或删除。
  纯结构校验固定hook原创建身份、跨文件namespace、完整runtime原承诺重算、原profile每项
  路径/SHA/大小与事务/policy、hook Python及入口覆盖；False不可用0冒充。原runtime明确与
  已持有原Full execution_validation_identity.json对应hash比较（仅排除environment）。
  记录所有执行许可仍False；原context退出不能靠序列化ready重新取得能力，不开放实际发布。
  预登记v150 D:/Work/devx015-hook-ready-contract-v150，DEVX-015拥有且启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-hook-ready-contract-20260914-v150.xml。
  新ready shape/原prior转换、原capsule初始与对象追加转换、既有index profile结构回归及新ready
  入口wrong-phase反证；shape fixtures不创建文件且不充当Full/Job授权证明。
  预登记v151 D:/Work/devx015-hook-ready-original-full-v151，同样DEVX-015拥有且启动前不存在；
  XML outputs/validation_runtime/devx015-hook-ready-original-full-20260914-v151.xml，外16/loadfile。
  原index-replaced-full-profile-publish实际Full链1项：ready唯一追加绑定真实old outer、原Full投影
  不变，完整child继承/3处原生32拒绝、释放后ready仍不可重建/改写、原源码漂移/prepare-abort/
  index恢复/终态重放仍须通过。原SUT/测试/XML保存后治理清理，不能以结构绿替代实际整链。
- v149原70603终态1PASS/1043.71s，XMLfc355689b09e371207eecd2abb57a3b099458cfd225e42131da01ce69e9daa04。
  原fixture C=37ef782c80952e88623c8a1949b9ae770f07fb14实际Full PASS且非print-only；17885项
  原生组合保护与同原Job child精确继承完成，src/a.py、固定reference hook、原Full结果三处
  独立Win32写打开均32拒绝。context退出每项custody失效、原lease事件不变；随后原源码漂移
  typed拒绝、Git prepare/abort、index替换恢复、REPLAY_ONLY及失败释放整链通过。
  新独立公开CLI重放PASS/FAILED/issues=[]；原SUT/测试与完整快照保留v149。
  v148独立probe前可写/保护内32/后可写且内容不变，1PASS/6.82s，XML
  08f52e9b93b00103fd338cc28684637e1afb70567ad6b9e9663d6f0142c96652。
  这证明发布输入活保护接入原Full及恢复链，不是ready追加、Git实际ff-only或整体验收；后续继续。
- v146终态1PASS/9.37s，XML442091b984614ae5177d40d5cd9d5ff45b5e387edb577a29bbe1cca432e2feef。
  v147原55238终态1FAIL/1ERROR/852.78s，XML9814956f39c4de2c07e0666da26374e007eaacdb49bbb642257a64da3cb7ebfe。
  原fixture C=b32249544e825c95ef65c41183db95987aa0b61b实际Full PASS，worker84192/134338559115736766；
  组合17885项（runtime16556+profile1325+4）实际活保护和原child精确继承均达到，原输入快照保留。
  child已被拒绝写打开src/a.py，但Python CRT只报告errno13/PermissionError、winerror=None，
  新测试错要求winerror5/32而失败；不是写入保护放行。生产不改，改用独立Win32 CreateFileW
  GENERIC_WRITE/OPEN_EXISTING/SHARE_ALL，必须原生ERROR_SHARING_VIOLATION=32，不写入或截断。
  原Job ABSENT、launcher/worker EXITED确认后，经原PublicationLifecycle.recover(observe)记
  INSUFFICIENT，再独立采用ORIGINAL_UNCHANGED，原Full承诺未变；原公开release终态FAILED/RELEASED。
  v148预登记D:/Work/devx015-native-write-probe-v148，DEVX-015拥有、启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-native-write-probe-20260914-v148.xml，精确native探测1项，
  原文件保护前可写打开、保护内原生32拒绝、退出后可写打开，文件bytes不变。
  v149预登记D:/Work/devx015-publication-inputs-original-full-v149，同样DEVX-015拥有且启动前不存在，
  XML outputs/validation_runtime/devx015-publication-inputs-original-full-20260914-v149.xml，外16/loadfile，
  原index-replaced-full-profile-publish实际Full整条1项重验。只纠正probe错误通道，原生产不变；
  真实writer/Full/恢复仍须原链完成，失败证据不能升级PASS。保留原件后治理清理。
- 发布读取窗口复用hold_hook_capsule_inputs：只允许原Full/LOCAL、RUNNING原worker、已创建完整
  3对象且无main_preparation/ready的attempt；先持有全部runtime，再运行原Full inspector，
  其已包含候选runner源码与checkout清单，不另建扫描。固定hook须匹配原created leaf/父链，
  inspector captures须逐项精确bytes/hash/size及原checkout root，另保护原事务与选定policy。
  同原store atomic重核原worker/attempt、clean candidate及原outer/profile全部捕获，然后
  才返回活custody tuple与观察快照；不追加ready、不授予Git resume，ExitStack负责全部退出。
  v146预登记D:/Work/devx015-publication-live-inputs-v146，DEVX-015拥有、启动前不存在，
  XML outputs/validation_runtime/devx015-publication-live-inputs-20260914-v146.xml，外16/loadfile，
  精确wrong-phase新入口1项，验证原拒绝且无事件/输出；不把shape反证当实际Full准入。
  v147预登记D:/Work/devx015-publication-inputs-original-full-v147，同样启动前不存在且DEVX-015拥有，
  XML outputs/validation_runtime/devx015-publication-inputs-original-full-20260914-v147.xml，外16/loadfile，
  原test_publication_lifecycle_binds_original_actual_full[index-replaced-full-profile-publish]1项；
  真实Full后原worker组合全部保护，实际原Job child继承同完整集合并独立验证源码/hook/Full证据
  写打开拒绝，context退出每项custody失效、原事件不变，随后原source漂移拒绝/Git prepare-abort/
  index-replaced恢复与release仍须通过。测试观察期限420s覆盖两次原180s profile与native工作，
  不改变生产timeout/租约。只读输入组合不是ready或实际发布；原证据保留后治理清理。
- v145原40168终态6PASS/128.45s，XML68b83efd4fd0c53646521920cbab40e495ce568b9f794ecdf4d0f0363f27431e。
  小对照closed child保留/删除均167；完整16556输入的正常child继承/运行/退出与body异常路径
  都精确167→167；原terminate/close/context实际进程树清理通过。原SUT/测试/XML保留v145。
  Ruff与单模块strict mypy(follow-imports=silent)、显式路径diff检查通过；非该定向模式的
  导入展开另报告7个其他文件35项类型错误，不能称全仓类型检查通过。未扩改无关源码。
  下一ready复用原runner inspection已经捕获的全部runner_identity候选源码/checkout输入，
  叠加固定hook实际身份及全部runtime活句柄；不另建重复源码扫描。仍须原Full/current profile
  与同原store的outer-before-ready绑定，再实现实际hook/ff-only/恢复，不以此六项代替整体验收。
- v144原进程终态1PASS/7.29s，XML7109fdea85ba538aed1382b717a241d5f4e06bff4a305809cf64f46b130cb04e。
  原unbuffered open/close/del165→166→165→165；buffered165→167→166→165；
  原closed child保留169、删除167，实测两项closed buffered stream对象残留解释完整路径+2。
  修复WindowsJobProcess.close：已关闭stream从owner列表移除，仍未关闭者保留供失败重试，
  process/thread/Job终止与原异常报告不变；不要求调用者del/GC才完成close。
  v145预登记D:/Work/devx015-native-cleanup-v145，DEVX-015拥有且启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-native-cleanup-20260914-v145.xml。
  原对照新增closed child保留/删除计数必须相等；完整runtime两模式及原terminate/close/context
  三种实际进程树清理回归，共6项；原句柄计数精确相等，不加容差。证据保留后治理清理。
- v143原70782终态2PASS/1FAIL/129.60s，XML1e521abcfbbb278b2818e94c2d3e654f821d9c9e18d2c0674b6d477aa1f77038。
  三轮小对照与完整runtime body异常释放通过；success仍169→171，原代码及测试保留v143，
  不继续放宽oracle。原close保留self._files内已关闭的buffered stream对象；小对照每轮child
  重新赋值时旧对象析构，而完整测试同时保留control与child，可能解释差2，尚须实测。
  v144预登记D:/Work/devx015-native-stream-custody-v144，DEVX-015拥有且启动前不存在，
  XML outputs/validation_runtime/devx015-native-stream-custody-20260914-v144.xml，外16/loadfile。
  原native对照增加原buffered/unbuffered文件open/close/del及child close/del计数，
  无WinAPI替换、无生产修改；只定位对象保留，不作为完整清理验收。证据保留后治理清理。
- v142原native小对照终态1PASS/7.65s；无runtime集合也有首次165→169，后两轮完整
  create/bind/resume/wait/close/gc均169→169。trace显示首次CreateProcessW额外增加2，
  另有文件打开/对象释放相关差异；这只证明三轮观察中首次差异不累积，不归因未验证的OS内部机制。
  新完整集合测试在同一原worker先执行三轮原native child，对后两轮严格相等检查后才取基线；
  保留cold与每轮计数，完整集合释放仍要求最终精确回到该基线，不允许+4容差。
  预登记D:/Work/devx015-complete-runtime-custody-v143，DEVX-015拥有且启动前不存在；
  XML outputs/validation_runtime/devx015-complete-runtime-custody-20260914-v143.xml，外16/loadfile。
  仅运行完整集合success/body-exception与新增稳定性断言的小对照，共3项；生产不变。
  原证据保留，终态后治理清理；不作为ready/Full/发布或整体验收通过。
- v141原87614终态1PASS/1FAIL/115.57s，body异常清理通过；success已继承16556项并实际运行child，
  child原生句柄84934，但父冷基线165与最终169差4而失败，不能放宽断言或直接宣称泄漏已排除。
  改用不持有runtime的三轮原InheritedJobChild对照，Python trace只观察实际原函数行间句柄变化，
  不替换/模拟WinAPI。预登记D:/Work/devx015-native-child-handle-control-v142，DEVX-015拥有且
  启动前不存在，外16/loadfile，XML outputs/validation_runtime/devx015-native-child-handle-control-20260914-v142.xml。
  此diagnostic只定位冷启动/重复创建的实际句柄变化，不作为完整runtime清理验收通过证据。
- v140原39805终态5PASS/2FAIL/124.13s，失败证据与原SUT/测试保留。两个新规模case均在
  已关闭custody正确抛出WORKFLOW_READ_FILE_CUSTODY_OWNER后，因新测试漏写WORKFLOW_前缀而失败；
  生产检查不改，仅修正该断言。成功路径在失败前已形成16556项真实child绑定（原绑定9038282
  bytes保留），但child resume/完整退出检查当时未到达，不能据此宣告通过。
  新预登记D:/Work/devx015-complete-runtime-custody-v141，DEVX-015拥有且启动前不存在，
  XML outputs/validation_runtime/devx015-complete-runtime-custody-20260914-v141.xml，外16/loadfile；
  只重验这两个新case，旧5项已通过且未变，不重跑已完成证据。
- 下一完整runtime live custody复用原observe_dependency，每项原exe/engine及distribution输入
  都通过原hold_bound_read_file持有精确实际bytes/leaf/root/父链，root仅来自本解释器prefix、
  base_prefix及已安装distribution origins，不接受调用者另选root。原native打开每次重核
  expected目录身份；已持有链的expected身份可复用但不能替代原生检查。只读budget沿用原runtime
  64MiB单项、原总量/数量上限；外ExitStack在捕获或调用体异常时释放全部句柄。返回原identity
  与实际custody tuple，不能由序列化证明重建能力，仍须外层原Full/Job/attempt准入。
  预登记D:/Work/devx015-complete-runtime-custody-v140，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-complete-runtime-custody-20260914-v140.xml。
  在真实原Job worker持有全部当前runtime文件，原child显式继承完整集合，在父custody关闭后
  检查原生Job/PID/创建时间及实际child句柄数量，原body异常与child退出后检查原worker句柄释放；
  保留完整pre_resume绑定与摘要，不对真实安装文件做写入探测，不将规模测试当作ready或发布。
- v139原98961终态28PASS/48.11s；原安装runtime两次实际观察identity相同，回调逐项覆盖
  原exe/engine与16554项distribution输入，重新计算原排序承诺一致；fixture全类别输入与
  回调失败释放、原线程池/文件custody回归、默认/显式大文件及预算反证通过。
  此处只完成原读取观察接缝，尚未批量持有完整runtime并继承到实际Git child，也未记录ready。
- v139读取保护接入契约：复用原acceptance_runtime_identity及distribution读取循环增加可选
  observe_dependency(path, actual_bytes)，在原调用线程逐次观察实际读到的原exe/engine和全部
  已纳入原distribution承诺的代码、二进制、.pth和元数据；既有identity/hash/排序、20000文件/
  512MiB总量/64MiB单文件上限及原.py/.pyc缓存语义不变。回调不是custody或执行授权证明；
  ready调用者仍须用实际活原生句柄保护每项并与原Full比较，异常须退出原owned reader池。
  当前原runtime实查16554文件/441758533 bytes，四个原输入大于16MiB（最大ruff.exe32376320）；
  hold_bound_read_file默认仍16MiB，显式budget仅接受整数0..64MiB并要求实际expected bytes不超限，
  不改变既有普通默认、拒绝multilink/身份漂移或任何写入语义。只读资源参数不是读取授权。
  预登记D:/Work/devx015-runtime-read-custody-v139，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-runtime-read-custody-20260914-v139.xml。真实fixture RECORD
  覆盖全类别、回调失败/线程池退出和只读保护释放；原安装runtime双观察核对承诺不变，
  大文件默认拒绝/显式通过及预算类型/上限反证，不将回调返回值视为已获得ready。
- v137原80709终态85PASS/32.58s，XML40d346ed90ed2b780d7d2a0a5576e8ae295dd337fac55e9914c4d347d26a86d2。
  v138原6629终态1PASS/880.01s，XML9a3296e3ed96c0994c23a44ac354d48ad47dc164edaf5773b3b74e026094cb21。
  夹具C=f5860625a87e5040807ee256b452cba8f78fccaf，实际Full74PASS/239.20s，原summary full/PASS、
  mandatory PASS且print_only=false。原Job worker82808/134338510649507673实际创建三对象，
  未知同名目录/canary保持原样（仅测试将自有碰撞目录移到保留名），实际bytes/身份与原记录相符，
  重复创建拒绝；Full后源码漂移拒绝、原Git prepare-abort及公开CLI失败恢复/重放通过。
  独立新CLI replay PASS/FAILED/issues=[]，原lease-77db95a58807b33ba157已失败释放。
  源与测试副本保留v138。ready仍未实现，原始创建记录不能作为Git执行权；真实main/peer未改。
- 下一hook materialization只允许原definition的目录→reference-transaction→post-merge三对象顺序。
  同原Full/LOCAL/worker/plan/candidate准入，复用原recoverable create的实际fd回调；回调从
  GetFinalPathNameByHandleW/fstat核验目标路径、类型、空文件、single-link与物理身份，原root
  身份必须等于checkout plan，实际父链身份亦绑定。原store atomic再次核验并追加对象后才清除
  delete-on-close并写入固定bytes；旧对象/定义/时间/worker不可改写，一次只能追加一个对象，
  首目录父链固定后后续文件须沿用并包含该原目录身份。ready仍必须None，创建不授予执行权。
  部分或完整已创建记录不可重复materialize；既有未知同名目录/文件拒绝且不删除/覆盖，保留
  原失败证据；对象记录表示实际创建身份而不谎称文件最终bytes已写完。完成后再由原read-only
  custody/currentFull-profile/实现与runtime读取窗口证明提供ready；本步不resume Git。
  预登记D:/Work/devx015-hook-created-object-v137及D:/Work/devx015-hook-materialization-original-full-v138，
  DEVX-015拥有且启动前确认不存在；外16/loadfile，XML分别
  outputs/validation_runtime/devx015-hook-created-object-20260914-v137.xml和
  outputs/validation_runtime/devx015-hook-materialization-original-full-20260914-v138.xml。
- v135原99452终态28PASS/21.17s，XMLc2e8754c6b2691cd429e6a84ba131c7817a751eb029d3a022b3a619d3660c13d。
  v136原53433终态1PASS/846.48s，XMLf96fceca848fcfa3d0535b3184d49adb59ead5cb250744b4db56265dfc49234c。
  夹具C=4d9efe460ccaa480d6a93f9e62d239d87e2e8094，原实际Full74PASS/240.78s，summary PASS且
  mandatory PASS/print_only=false。原worker86568/134338493751534995记录并只重读同capsule，
  objects=[]/ready=None且目录未创建；Full后源码漂移实际拒绝PUBLICATION_CANDIDATE_DIRTY，
  原Git prepare-abort、替换index的公开CLI失败恢复/REPLAY_ONLY与终态释放回归通过。
  原源/测试副本保留v136；此为原发布链夹具回归，不是实际main发布或最终候选整体验收。
- v134原36059终态74PASS/18.87s，XML0b2b9716c7f9af8338e03bf15ca01969246fb4e60c83e859d5228e86aac67313；
  定义替换/越界/控制字符反证和实际Git-for-Windows sh固定argv/stdin/退出码通过，源副本保留v134。
  下一步同原lease内record_hook_capsule_definition：先原Full/LOCAL/worker/clean/原plan核验，
  再同atomic重核并只追加一次原定义、原worker、时间、空objects/ready=None；同请求重读只REPLAY_ONLY。
  原checkout plan先存在且不可变，不允许同时追加其他变更或在main_preparation后追加。
  在原生创建/ready证明接入之前明确拒绝任何序列化objects/ready声明，失败终态保持定义不变。
  预登记D:/Work/devx015-hook-capsule-record-v135和D:/Work/devx015-hook-capsule-original-full-v136，
  均DEVX-015拥有且启动前确认不存在，外16/loadfile；XML分别
  outputs/validation_runtime/devx015-hook-capsule-record-20260914-v135.xml及
  outputs/validation_runtime/devx015-hook-capsule-original-full-20260914-v136.xml。
  v135结构/不可变转换/未ready原公开路径拒绝；v136原实际Full/LOCAL/Job记录与重读、定义不创建文件、
  原Git prepare-abort/失败恢复与终态release回归，仍不是实际Git发布或最终combined Full。
- v134先实现原hook capsule的纯定义/重建验证：两种固定入口、原stdout父目录+完整request
  SHA、原checkout plan/actor/policy/Python/CLI参数和逐文件bytes。只有Git的单个stage参数
  可展开，原stdin直达CLI；错参数数目拒绝。重封SHA不能授权更换任一路径/内容/身份。
  定义本身不创建文件，不代表原Full/Job许可；后续仍须原store append-before-create、
  原生对象身份回调、readonly custody及实际hook CLI/effect-window准入，不能提前resume Git。
  预登记D:/Work/devx015-hook-capsule-definition-v134，DEVX-015拥有，启动前已确认不存在，
  XML outputs/validation_runtime/devx015-hook-capsule-definition-20260914-v134.xml，外16/loadfile。
  定义/替换反证及真实Git-for-Windows sh的argv/stdin/退出码转发测试，不冒充发布验收。
- v133原95943终态51PASS/50.23s，XML8df12e6d5ec6457e3fde9c960ad1fa4475cf29d26f3d2ad96dbe24803cf6bace。
  新read-file custody的8项实际读/写/替换/rename及身份/内容/multilink/missing/body失败清理、
  3项原Job child双文件继承/forged/第二custody关闭后部分dup清理通过；原创建/恢复/应用/目录/
  父进程退出回归通过。源和测试副本保留v133。pre_resume v2绑定实际继承file proofs，旧v1保留。
- 下一原attempt hook_capsule：目录仅由原stdout父目录和完整request SHA导出；固定
  reference-transaction/post-merge入口bytes绑定原request/actor/policy/Python/CLI，不接受任意脚本。
  原Full/LOCAL/worker/clean/checkout_plan核验后，先在同store追加不可变定义与空objects；
  目录/文件通过原create_bound_recoverable_*的delete-on-close回调逐个追加原生身份后才保留写入。
  objects只能顺序追加，定义/旧对象不可重写，部分创建只能保留失败证据；不得覆盖未知同名对象。
  完整后原runner当前Full-profile检查在原arbiter外执行，在同atomic复核原execution/captured bytes；
  hook文件及原profile captures用原read-file custody保护，才记录一次ready。ready里的profile绑定
  追加ready之前的exact outer execution，不可把后来状态随意重封为旧Full。packet仅活原owner持有，
  序列化结果不提供执行权；尚不resume Git，也不改main/peer HEAD。后续Git launch和effect-window
  准入须在原attempt绑定，不能用require_candidate=false或忽略旧main代替。
  支持Git2.45.1.windows.1源码finish明确还调用post-merge及auto-maintenance；正式argv/固定hook
  集合需覆盖这些副作用，不悄悄跳过既有仓库策略hooks。来源：
  https://raw.githubusercontent.com/git-for-windows/git/v2.45.1.windows.1/builtin/merge.c （410-456），
  https://raw.githubusercontent.com/git-for-windows/git/v2.45.1.windows.1/run-command.c （1698-1718）。
  此处尚未采用任何既有hook忽略或自动维护策略覆盖，只固定必须检查的执行边界。
- 固定hook读取窗口：复用原RootDirectory-relative NtCreateFile增加已有普通文件只读custody，
  精确root/parents/leaf物理身份与原bytes，拒绝reparse/multilink/不存在/漂移；叶句柄仅READ、
  SHARE_READ，不能写/截断/删除，整个父链deny-delete。仅复制已验证原生句柄，可显式传给
  原InheritedJobChild；父custody退出后child仍保护原文件，直至原child/Job退出。无新store/锁文件，
  pre_resume快照升v2追加实际继承的read_file_custodies证明，不能由调用方传serialized绑定冒充；
  保留v131已记录v1历史，v1不提供新的hook文件继承证明。无文件继承的新快照明确空列表。
  无serialized句柄授权；现有创建/应用/删除语义不变。hook文件仍须绑定原Full/request/attempt。
  v133预登记D:/Work/devx015-bound-hook-file-custody-v133，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-bound-hook-file-custody-20260914-v133.xml，外16/loadfile。
  独立真实进程验证持有期间读可行而写/替换/目录改名拒绝、退出后可写可改名，含错误身份/
  内容/链接/异常清理；保留原创建/目录custody回归，不将原生文件保护作为Full或发布授权。
- v132原65220终态2PASS/3FAIL/13.46s，XMLa6c8e0735fabd8c0e67484dac328f026c0bb4700fa16b8dd2f2b932770dd0dc5。
  失败均是原RCF生成态：policy system_flow seal仍为S的2400540 bytes/e5d9d7dce930a4046162e63e2fd1d125d1dedf7739018422fbb7bdf6df3d1045，
  当前源2418655 bytes/a79209702f2e624fa46d4213c8674e56ae564524d86ab9a83abb8a4aa5ff5308；shadow未重建。
  git cat-file确认HEAD:S flow仍2400540，任务已有v118/119明确待最终官方RCF重建，本次并非新增解析器错误。
  保留失败，不修改旧seal绕过校验、不把test期望SHA同步声称RCF生成态PASS，不重复跑同一失败。
  最终源冻结后在原四generator的report-flow-authority阶段重建seal/fragments/index/consumer inventory，
  再跑freshness/全部mandatory；当前TASK_SOURCE_PRE_WRITE不提前伪装为GENERATED_REBUILD完成。
- v131原62418终态10PASS/17.09s，XML038385c1d92bb0083ab528334d48fbc2a37b4db07449ced11154791bb119cec9。
  原native oracle验证worker/child PID创建时间与Job、重复pre-resume快照及原argv/cwd/env/stdout；
  跨thread、已resume、未resume即退出、closed句柄拒绝。目录继承/父sibling保留与新旧路径
  owner-os-exit整树关闭通过，查询句柄未延长Job生命；实现/测试副本保留v131。
  尚未将新快照追加至原PublicationLifecycle，也未绑定不可变hook/原Full profile，不能执行真实发布。
  v132预登记D:/Work/devx015-publication-flow-freshness-v132，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-publication-flow-freshness-20260914-v132.xml，外16/loadfile；
  验证本轮system_flow/RCF受冻结SHA变化的freshness和lossless影子一致性，证据保留后治理清理。
- v130原56705终态3PASS/24.32s，XMLe090c778d9c1abdde8e4e238d6b2cd277627377d19e2525fbebd6a406da65f4d。
  实际生产prepared解析器+测试hook：成功真实M→C；提前main前进由ORIG_HEAD gate拒绝128且
  index物理对象不变；ORIG_HEAD prepared中另一真实Git进程推进main，原merge CAS拒绝128并
  保留新main，此时index物理对象改变但bytes不变。不能把两种失败副作用归为同一UNCHANGED。
  下一原生接缝InheritedJobChild.pre_resume_binding仅从原owner未resume的活process句柄，
  核验原worker/current Job与child实际成员/PID/创建时间，再返回不可作为授权的launch快照。
  读后立即关闭query Job句柄，不能复发last-handle kill延迟；已resume/退出/关闭/跨thread拒绝。
  不声明外部线程暂停计数或原Full绑定，后者须由原PublicationLifecycle记录和再次核验。
  v131预登记D:/Work/devx015-inherited-child-pre-resume-v131，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-inherited-child-pre-resume-20260914-v131.xml，外16/loadfile；
  在原真实Job/独立native oracle测试重复快照、身份/argv、拒绝与父退出整树关闭。证据保留后治理清理。
- v129原45987终态42PASS/36.45s，XML64f7978962e7c80e0bd572ac0c49104535ae62c184fb758d4382b3533bd55159。
  原计划7项真实Git采集、6项binding shape、10项不可变转换、19项prepared精确集合检查通过。
  原转换规则抽成同生产调用的private helper，未放宽规则。shape测试无Full/lease执行授权。
  v130预登记D:/Work/devx015-prepared-reference-actual-git-v130，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-prepared-reference-actual-git-20260914-v130.xml，外16/loadfile。
  在自有真实Git fixture中将生产解析器接入reference-transaction hook，验证真实ff-only成功、
  main已提前前进以及ORIG_HEAD prepared期间并发main前进均不覆盖新main；记录全部hook/引用/
  index/ORIG_HEAD副作用。测试hook本身不视为生产发布授权，仍需原Full/原Job/暂停Git和hook custody。
- 下一原发布执行门禁：固定reference-transaction prepared校验器仅准许实际观察的三种集合：
  ORIG_HEAD强制旧0写原M；HEAD与refs/heads/main同时且仅原M→C；AUTO_MERGE旧0→0删除。
  拒绝未知ref、重复行、缺HEAD/main配对、错M/C、混合阶段集合及畸形输入。解析器不授予Full/Job
  或发布权限；最终hook还必须绑定原attempt/暂停Git身份/不可变hook输入并复核原执行状态。
  v129预登记D:/Work/devx015-checkout-plan-transition-v129，DEVX-015拥有，启动前不存在，
  XML outputs/validation_runtime/devx015-checkout-plan-transition-20260914-v129.xml，外16/loadfile。
  计划结构/不可变转换与prepared输入专项验证；shape测试不冒充真实Full原链，真实原链v128保留。
  本轮不改真实main/peer，不恢复暂停Git执行；实际hook链接和发布副作用仍需后续原链验收。
- v128原47565终态4PASS/846.49s，XML4635434eb2db61f59cf6064067ef6f162f558c02daadfecde370ba09b1ffe7cd。
  fixture C21dc3ef90c4ef14ccc8438017289f771383c37be，实际Full74PASS/242.45s；原worker
  75568/134338445777313417在原Job内完成plan一次追加和无追加重放；原Full后源漂移拒绝、
  同原Git prepare/abort、实际进程退出、未知锁保留拒绝、index替换后公开恢复与重放全部通过。
  原Full投影、失败attempt与checkout_plan保留，失败恢复不变成发布PASS。源码/测试副本保留v128。
  尚缺plan字段篡改/删除/错误转换专项回归，原unchanged分支本轮未重跑；实际merge执行与
  PUBLISHED/partial/TTL恢复仍未实现，不能增加P01验收映射或宣称最终C Full/整体验收通过。
- v127原14288终态7PASS/30.22s，XML551de87363db899585af1dc1f8e84f73547e17f1b478cc0da7423f50d8aad6f4。
  真实Git计划采集保留HEAD/index/peer元数据；未知锁与在途合并拒绝且不读取内容，篡改和采集漂移拒绝。
  这仍不是Full或发布授权。v128预登记D:/Work/devx015-checkout-plan-original-full-v128，DEVX-015拥有，
  启动前不存在，外16/loadfile，XML outputs/validation_runtime/devx015-checkout-plan-original-full-20260914-v128.xml。
  接入原PublicationLifecycle同store/原Full/原Job，验证一次追加、原worker重放、旧Full投影保留及
  原prepare/abort/失败恢复链保留plan；运行现有actual_full的index-replaced分支及未就绪门禁。
  新增immutable字段不允许覆盖、删除、跨状态或在main_preparation之后补写；本批不执行真实主仓发布。
- 下一原attempt绑定：只读checkout plan绑定原request SHA/LOCAL intent拓扑、candidate HEAD
  指向main的预期字节及可选peer HEAD固定原M的预期字节；peer出现仅记录需授权，不视为授权。
  另捕获ORIG_HEAD旧对象与HEAD/main reflog前缀指纹；拒绝AUTO_MERGE/MERGE_*等在途状态及
  未知HEAD/index/ref/ORIG_HEAD锁，不读写未知锁内容。计划记录当前可允许的辅助元数据变化，
  但dispatch/publication/resume仍为false。原Job worker在原Full/LOCAL/C/clean与原生身份复核后，
  在同store原子重捕获并一次追加checkout_plan；既有plan不可覆盖/删除，变动只得到typed拒绝。
  不更改旧request.v5、Full投影、原main_preparation或旧失败记录；实际merge身份/执行门禁接入前
  该计划不构成发布能力。v127预登记D:/Work/devx015-publication-checkout-plan-v127，DEVX-015拥有，
  启动前确认不存在；XML outputs/validation_runtime/devx015-publication-checkout-plan-20260914-v127.xml，
  外16/loadfile。先用真实plain/linked Git、ORIG_HEAD、在途状态/未知锁/篡改/捕获后漂移验证计划，
  不把只读计划用例冒充原Full/原Job事件链；原链验证在接入后单独运行，证据保留后治理清理。
- v126原77286终态26PASS/32.81s，XML30301cd18772848620eaab84be7d18860fbdf3e7632cc1bfa590d703c7b0c52c。
  原v125新child owner-os-exit与旧Popen路径均通过；查询Job句柄创建验证后关闭，child仅保留
  自身process/thread/stdout与原目录继承，不推迟last-handle kill。原5模式child/父sibling/
  暂停期间目录保护及退出后rename控制、非成员无效果拒绝、原owner/整树/错误timeout回归通过。
  原SUT1bbcd57b6cac5c9388d34ebf3db2dde9625ae1e1e32eb5b7310d4752da1dea88与测试保留v126。
  v125失败XMLab26fc23af034818468dba4c06e85adaef194712edc7cde4e70a8f9584a8c70c，独立Job ABSENT已确认。
  Ruff/strict mypy/scoped diff通过，flow/RCF同步b6d619b96847392f69b770907eafa2010d98996bba31a23d578de515b80e1e34。
  本次尚未把新native child接到实际publisher；下一步在原attempt内绑定checkout/副作用计划和
  暂停Git的真实身份，再接--ff-only条件gate与HEAD/ORIG_HEAD/AUTO_MERGE/index恢复，不重复旧
  prepare/abort作为发布完成，不新增P01映射。仍须全部41/14counters/10mutants/finalC门禁与
  授权迁移发布，然后OPS080工程/部署/new lawful daily，Pi/SoL-Pi不启动。
- v125原99929终态1FAIL/18.01s，触达原launcher已os._exit后实际parent仍存活，非setup/收集失败。
  原测试按精确原identity终止所拥有的残留树，后续独立observe_job核验；原SUT/测试保留v125。
  修复原生child持有期：原Job查询句柄仅用于创建时验证，必须在child返回/resume前关闭，
  不能保留到child结束；child自身close改为不依赖原Job句柄，只回收原process/thread/stdio。
  仍不关闭/终止原Job，保留父退出kill-on-last-handle。v126预登记
  D:/Work/devx015-inherited-child-owner-exit-v126，DEVX-015拥有，创建前确认不存在；
  XML outputs/validation_runtime/devx015-inherited-child-owner-exit-20260914-v126.xml，外16/loadfile。
  原v124全组加v125新退出路径，真实Windows/native/目录保护与父sibling存活均重验；证据保留后治理清理。
- v124原35264终态25PASS/31.66s，XMLcbf9811a778aea5c82bf370e2ed228c4a226cf1fcf3f14098aa88a88d45e213。
  新child暂停/单resume/线程归属/退出或close、父+sibling存活、目录仅由暂停child继承保护、
  非成员/伪造或关闭custody拒绝及原Job回归通过。原源码0ffdd29bc69dc6fcf6ff3550e9ec47adaa47548576fab5a9ab009c06cd335745
  与测试保留v124。但审查发现新child holder长期保留原Job查询句柄，可能推迟原launcher死亡时
  last-handle kill；原v124旧launcher死亡回归走旧Popen，不能覆盖新child路径。
  v125预登记D:/Work/devx015-inherited-child-owner-exit-v125，DEVX-015拥有，创建前确认不存在；
  XML outputs/validation_runtime/devx015-inherited-child-owner-exit-20260914-v125.xml，外16/loadfile。
  原进程/native oracle/原launcher os._exit框架追加新child路径，先不改SUT。仅在真实身份和
  原Job仍ACTIVE得到证明后，以原测试清理入口终止所拥有的树；不会留下无监督进程。
- v123原58645终态4PASS/13.50s，XMLa604bdbcd34bd4acee18b73c61e79058d780e2ec1cd9d2c849f5cabba59c9ddc。
  Git实际普通/guarded --ff-only成功M→C；已前进main与显式拒绝均exit128且main/refs不变。
  四例index字节/tree/worktree C均保持，但物理index均更换，ORIG_HEAD均写为命令开始HEAD。
  成功trace还含HEAD+main共同ref事务和AUTO_MERGE删除；拒绝实际出现aborted通知，保留原trace，
  不把文档概述覆盖当前Git实际行为。全部命令/完整before-after/trace及测试副本保留v123。
  因此真实publisher必须保存允许的ORIG_HEAD/AUTO_MERGE副作用、HEAD过渡和新index身份，
  不能将hook拒绝当成现场不变，也不将prototype钩子直接用于生产。
- 下一原生执行接缝：复用现有Windows CreateProcess/显式stdio与handle_list，原Job成员才能
  创建同Job继承的suspended child，不新建Job、不暴露原Job terminate能力、不把子进程退出
  当Job树清空。原句柄identity/launch binding须在resume前可供原租约记录；仅原创建者线程
  可resume一次。失败/close只处理自身child，父worker和原Job不受其销毁；树级退出仍由原
  launcher/原Job执行验收。若有原生目录custody，只继承已校验目录句柄，拒绝伪造/已关闭custody。
  新child capability不得作为WindowsJobProcess交给通用Full bind；没有此原生前置绑定，
  不改用先运行后观察的普通Popen来启动有实际副作用的merge。
  v124预登记D:/Work/devx015-inherited-suspended-child-v124，DEVX-015拥有，创建前确认不存在；
  XML outputs/validation_runtime/devx015-inherited-suspended-child-20260914-v124.xml，外16/loadfile。
  验证实际原Job内暂停/一次resume/退出或close、父与sibling不被终止、非成员无副作用拒绝及
  原owner创建/退出/整树清理回归；保留证据后治理清理。不宣称本接缝已完成P01发布。
- v123有限Git行为验证预登记：D:/Work/devx015-ff-only-reference-contract-v123，DEVX-015拥有，
  启动前确认不存在；XML outputs/validation_runtime/devx015-ff-only-reference-contract-20260914-v123.xml，
  外16/loadfile。只在原small_repository夹具检验实际Git2.45.1：candidate index/worktree已C、
  HEAD临时指向main=M时的真实merge --ff-only；再验证固定reference-transaction prepared钩子
  是否能强制main原M→C，拒绝已前进的main以及钩子拒绝，观察ORIG_HEAD/HEAD/index/文件变化。
  官方https://git-scm.com/docs/githooks/2.45.0#_reference_transaction仅描述prepared锁后old/new/ref
  输入与非零拒绝，不覆盖symbolic HEAD；不能直接据文档宣告发布或跨对象原子性。
  这是发布路径选择的行为实验，不是生产hook已获采纳，不新增store/queue/scheduler，不触碰真实
  peer/源码工作树，不记P01通过或Full证据；全部原命令与状态留存后治理清理。实验失败先定位
  实际语义，不放宽旧M匹配、不用先CAS后空merge冒充原规则；采用实现前仍需原意图/Job/句柄/
  schema/lease/捕获及部分写入恢复合同，真实peer接管须另行精确授权。
- v122原52559终态1PASS/817.98s，XML09cee9c377849329857f629ae3cba100b12ad45d37ae18d9cb0a32f6d3b470e4。
  全新C205ced059fb27f525a0b79514a3ff0227f739da1实际Full74PASS/241.65s；源码漂移拒绝、
  worker72128/134338406735197730与Git92172/134338407640053724原Job/native身份、真实prepare/
  abort/失败记录、未知锁保留拒绝、原Git终止，以及own index等字节更换后的公开稳定恢复和
  新CLI REPLAY_ONLY通过。原Full投影/attempt未改，index bytes/新物理身份保持，准备resolution
  绑定且execution终态，fixture经原failed release收口。新独立CLI replay PASS/FAILED/issues=[]，
  lease-e08b842e2fb7d6a2ae39；实际main仍92fcc66b6335c16aa7b641fd90067e477b58af31，不是发布成功。
  当前production/CLI/test副本及原始输出保留v122。仅新增index分支本轮运行，unchanged分支保留
  在最终整合回归；不替代最终C所有tiers/Full，也不新增P01三变体整体验收映射。
  下一步实际commit/checkouthandoff必须同时满足原AGENTS的--ff-only规则与P01精确main条件更新、
  原C/V保持、部分写入和TTL恢复；不能先更新ref再把空merge包装成实际fast-forward证明。
- v121原2533终态26PASS/35.85s，XMLaacf3c1d31d042704d1ddf369e9b53352227f42098fd2a61fd3b7009ddeadeb5。
  原v115第一次公开恢复因69个05:07UTC产生的未跟踪pyc被clean门禁拒绝；确认非tracked、无活动
  Python后，逐路径/哈希留证并移动到v121/preserved-v115-bytecode，可恢复、不删除，index未变。
  原5142实际公开恢复成功，第二独立CLI REPLAY_ONLY；原Full SHA04ca34d439058ac5e2b8b1acf0b957e587f942fc4352f3359c40f58d2908e4b9
  保持，原失败INSUFFICIENT不变，原Job ABSENT，1325 captures、原/当前拓扑独立匹配。
  独立证明v121/v115-independent-recovery-verification.json SHA14572bf57a283c4d96759c798941503f83ba348b515d9e886ee5c67d236ce1be。
  原公开release完成FAILED/RELEASED，后续新CLI replay PASS/FAILED/issues=[]，原事件未改写。
  此为窄失败恢复已执行证据，不是P01整体/成功发布；当前observer源码保留v121，不改旧fixture C。
- v122预登记D:/Work/devx015-failed-index-actual-full-v122，DEVX-015拥有，创建前确认不存在；
  XML outputs/validation_runtime/devx015-failed-index-actual-full-20260914-v122.xml，外16/loadfile。
  将现有实际Full/原Job/Gitprepare/abort/native退出链增加index-replaced分支，以全新原C跑公开恢复
  两次及失败释放，不依赖外部保留v115才能回归。先只运行index-replaced分支；原unchanged分支
  保留用于最后整合回归，不将同名节点或旧Full等同于当前新增分支。保留原件后治理清理。
- v121预登记：D:/Work/devx015-failed-index-public-recovery-v121，DEVX-015拥有，创建前确认不存在。
  XML outputs/validation_runtime/devx015-failed-index-public-recovery-20260914-v121.xml，外16/loadfile；
  v2观察绑定十五种synthetic合同检查（不构造Full证据/不写事件）、两个实际CLI未ready拒绝、
  v120九种原Git index比较回归。公开local-publication-recover-index复用原fence/store，检查
  原LOCAL阶段/actor/C后调用窄失败恢复；保留typed拒绝。profile严格字段/捕获scope/digest/size
  不代替原runner当前实检。通过后才对保留v115执行独立原链恢复；证据保留后治理清理。
- v120原86701终态18PASS/48.25s，XML
  8493e476a2bc91f0196662b0a75dc8ab8eacff767e2866f76692c76fdbbceda8。
  九种原拓扑回归加九种恢复观察合同通过；新比较器验证原/当前拓扑digest、index字段及
  物理身份结构，拒绝不变identity、缺字段、非法size/identity、错digest、main/tree变化。
  实际小Git index等字节更换可通过比较，但不等于Full/独立稳定采用或发布许可。
  Ruff/strict mypy/scoped diff通过，源码及测试保留v120。新v2观察与恢复方法完整采用链尚未
  验证，公开恢复命令尚未接入，原v115现场未改；继续这些原路径和所有剩余整体验收。
- v120预登记：D:/Work/devx015-failed-index-recovery-contract-v120，DEVX-015拥有，创建前确认
  不存在；XML outputs/validation_runtime/devx015-failed-index-recovery-contract-20260914-v120.xml。
  实际小Git原C/index对象更换及九种观察合同正负例，原拓扑九种回归，外16/loadfile。
  只验证拓扑合同及其只读边界，不作为Full、稳定采用、P01整体验收或发布证据。
  原v115现场仍不改动；保留本轮代码/XML，证据归档且无进程依赖后按治理流程清理。
- 下一恢复出口仅处理失败attempt的candidate index对象更换：原main/HEAD/config/peer等其余拓扑
  必须完全不变，当前index须由原Git独立对照原C的tree并拒绝hidden flags，原clean与当前Full
  profile/captured bytes仍须重验，原Job及prepared Git已退出且无遗留lock。原事务锁内重复全部
  检查，再追加独立CANDIDATE_STABLE_INDEX_REPLACED观察，保留原/当前两份index身份与原Full。
  这是失败状态的稳定恢复，不是ORIGINAL_UNCHANGED、发布PASS或重新派发许可；不改写index、
  源码、原intent或旧事件。源码/staged/HEAD/main/peer/配置变动及未知锁不走此窄出口。
  原v115现场用于实际恢复验证；先验证新观察合同与拒绝边界，之后才调用新公开恢复命令。
- v119原82112终态47PASS/746.96s（readiness原模块46项+原发布全链1项），XML
  b8f4a4afe941f0b0f9afe08ab99194bca524a95ba316f82647e113ab91386afe。
  actualFull74PASS242.60s、C=fabfad6255c1edc430f02b61f63c49c94c3a969f；真实源码漂移得到
  PUBLICATION_CANDIDATE_DIRTY，恢复后当前profile/原拓扑保持并完成Git prepare。原worker
  81500/134338375680604808及Git80784/134338376572882687均经独立native Job oracle验证。
  独立进程rename原refs/heads被OS winerror32拒绝，原Git退出后同目录rename/恢复成功；
  实际abort/FAIL收存、未知lock拒绝且保留、原Git退出/lock缺失resolution、独立UNCHANGED和
  原failed release通过。新进程原CLI replay PASS/FAILED/issues=[]，不把故意失败worker当发布PASS。
  当前源码与原始证据保留v119；不增加P01整体验收映射。下一步仍为受控commit/checkout、
  PUBLISHED及部分写入/过期恢复（包括v115原index变化现场），全部41/统一14counters/10mutants、
  最终C全部tiers/实际Full、迁移发布与OPS080工程部署新合法daily，均按原目标继续。
- v118原3640终态12PASS/16.20s，XMLf001d2b43477a5102171780cc7b907bc001756205aee239636e465fffb5eac37；
  clean/dirty及optional locks六组合与原audit六回归通过，原index bytes/物理身份保持，dirty仍exit1。
  原源码及测试保留v118。Ruff/strict mypy/scoped diff通过；不把小探针作为长链或P01验收。
  v119预登记D:/Work/devx015-publication-current-profile-v119，DEVX-015拥有，启动前不存在；
  外16/loadfile，原actualFull/LOCAL/source drift/恢复/当前profile/prepare/abort/独立UNCHANGED全链，
  增加原发布worker持有时独立进程rename refs/heads拒绝、原Git退出后同目录rename恢复成功；
  同时运行原tests/test_validation_readiness.py全部回归。XML
  outputs/validation_runtime/devx015-publication-current-profile-20260914-v119.xml；不重复v115已通过且
  未变的拓扑/基础目录用例，原RCF生成态失败仍保留待最终官方重建。原进程终态前冻结代码与canonical，
  原件保留及归档/无活进程后治理清理；当前仍无commit/publicpublisher或整体完成声明。
- v117原20249终态9PASS/3FAIL16.52s，XMLc453b138fc9f2773f0860fdc99daea310681893911084428d901e569e3c5c9c8。
  dirty三项和既有audit六项通过；clean三项返回1（并非原刷新断言），原Git --quiet在关闭刷新后
  把stat-only判脏。原现场无写入诊断：--quiet/--exit-code返回1，--name-only/--raw列出缓存miss，
  而--numstat/--shortstat/--stat/patch均无实际变化输出。保留v117原代码/XML，未改旧index。
  修复原只读_git的quiet diff分支为--numstat -z内容比较，将实际变化清单映射回0/1，错误仍拒绝；
  不输出patch或改写index，不把仅stat失效当源码变化，真实dirty仍拒绝。
  v118预登记D:/Work/devx015-readiness-index-preservation-v118，启动前不存在，DEVX-015拥有；外16/loadfile，
  原12节点重跑，XML outputs/validation_runtime/devx015-readiness-index-preservation-20260914-v118.xml。
  原始证据保留及无活进程/归档后治理清理条件不变；不以小探针替代原长链或最终C验收。
- final-v2已通过原release终态FAILED/lease RELEASED，原transaction bytes SHA保持df2a96b6…；
  原final-v3 acquire首次因把工具自动添加的两个resource再次作为参数传入而PATH_DUPLICATE，
  在intent/lease前拒绝，无事务文件。移除这两个重复CLI参数后同ID正常acquire/源阶段/LANE PASS；
  机器比较shared_paths只增加validation_readiness.py，原owned/generators/tiers/S/M/actor/thread不变。
  新lease-08e24bb1782e5400288f、transaction SHA d5a76ac34c147cb42f01131363643aac212450dc1b035550be8519f92223ee87。
  直接修复原readiness Git命令固定diff.autoRefreshIndex=false，未修改原v115夹具源码或index。
  v117预登记D:/Work/devx015-readiness-index-preservation-v117，启动前确认不存在；外16/loadfile，
  六个clean/dirty与optional-lock设置组合加六个既有checkout audit原index回归，XML
  outputs/validation_runtime/devx015-readiness-index-preservation-20260914-v117.xml。实际dirty仍须exit1，
  所有index原bytes/identity保持；保留原件后治理清理。新生产代码尚未记通过，长链不重复启动。
- v116原命令终态3FAIL/8.81s，XML3e137b2252ef91ad4a325dfdafb5345e68a1cab0fe40a6f333598149f3729f9c。
  三种外部optional-lock设置均实际Git返回0，但原index bytes被stat刷新，触达目标断言。
  直接修复src/ai_trading_system/platform/architecture/validation_readiness.py的只读Git入口，
  固定diff.autoRefreshIndex=false（与既有checkout audit相同原因），不削弱脏源码/拓扑拒绝。
  当前final-v2未声明该路径；结束其未执行项目Full的源阶段事务，保留FAILED/release原收据，
  通过同一公开fence acquire建立final-v3，继承全部原声明，仅增加这一个生产路径；同S/M/分支/
  store，不改旧事务bytes、不新增替代worktree、无第二writer。继任LANE PASS后才修改该源码。
  原v115隔离失败attempt已由原recover观察实际退出而RECOVERED_TERMINAL；因index对象已变，
  不伪造UNCHANGED或强行release，原现场留待不同稳定恢复路径处理，不篡改原index/intent。
- v115原90338终态18PASS/2FAIL/1ERROR、709.76s，XML
  a9d0290d0bd92ca7d351eea114c875c9a3d416569e5304cb582dd2012f0b9180。
  actualFull74PASS239.59s，C42da24f4ce445c7d51c8e2ae7d8784dc4f55ff34；源码漂移拒绝并恢复后，
  原profile检查成功返回但后续拓扑拒绝PUBLICATION_LOCAL_INTENT_CHANGED；独立逐字段比较只发现
  candidate index物理身份及SHA变化（原e956f440…→6d18ca81…），refs/HEAD保持，未进入Git prepare。
  不弱化拓扑；保留原源码/XML/失败租约。teardown正确拒绝非终态释放。另一FAIL为system_flow
  旧shadow bytes与当前source不同，最终官方生成重建前保持未通过，不改历史生成证据冒充当前。
  排查原readiness Git diff的stat刷新；先做小型真实Git反证，不重跑长链来猜原因。
- v116预登记D:/Work/devx015-readiness-index-preservation-v116，DEVX-015拥有，启动前核实不存在；
  外16/loadfile、3个原readiness._git diff调用（optional locks未设/0/1），实际安装原私有index bytes，
  检查返回0时index bytes/对象身份不变；不替换身份门禁或执行Full。XML
  outputs/validation_runtime/devx015-readiness-index-preservation-20260914-v116.xml；当前readiness实现不改，
  原件保留后按无活进程/证据归档条件治理清理。若触达刷新才是有效red，不预宣告根因。
- v114原76805终态10PASS/21.32s，XML SHA256
  575cf2d20a03a3700cfb98f3fa51ae605e08af4d4f842bb3a21bfcc502186338。
  真实Git继承目录句柄后，独立进程rename common/refs/heads均被OS拒绝；父侧全部句柄关闭后
  仍拒绝，原Git abort/退出后同目标rename及恢复成功。错root/parent/leaf及body异常后均可恢复。
  这证明句柄寿命，不冒充真实父进程死亡场景；原XML/源码保留，未新增P01验收映射。
- 接入原PublicationLifecycle：原拓扑增加refs/heads目录身份，原句柄custody跨固定Git存续；
  抽取原fence当前Full profile只读检查，原checkpoint顺序不变，worker在Git前及记录前于原atomic
  重验原execution/captured bytes和clean candidate。仍仅prepare/abort，不开放commit或新publisher。
  v115预登记D:/Work/devx015-publication-current-profile-v115，DEVX-015拥有，启动前确认不存在；
  外16/loadfile，原actualFull/LOCAL/Job加入post-Full源码实际漂移拒绝及恢复后正常prepare/abort；
  另含真实拓扑与目录custody回归。XML outputs/validation_runtime/devx015-publication-current-profile-20260914-v115.xml。
  等待witness观察界限210s只容纳原180s检查器及30s观察，不改变生产Full时限。原进程终态前冻结
  SUT/tests/manifest/canonical；保留原件，证据归档及无活进程审计后治理清理。未运行前不记PASS。
- 新目录custody原语复用现有RootDirectory-relative NtCreateFile与原身份验证，在原句柄关闭前
  DuplicateHandle保留完整root/ancestor/leaf链；原副本不可继承，仅Popen显式handle_list的临时
  副本可传给受控子进程。父侧关闭后子进程副本应继续保护目录，不能以父活体代替子进程边界。
  这不是新store/queue，也不授予任务或发布权限；尚待真实Git实测与原PublicationLifecycle接入。
  v114预登记D:/Work/devx015-publication-directory-custody-v114，启动前不存在，DEVX-015拥有；
  外16/loadfile，3个真实Git prepare/abort继承common/refs/heads目录保护、4个错身份/异常释放，
  以及原3个Git prepared-ref回归；XML
  outputs/validation_runtime/devx015-publication-directory-custody-20260914-v114.xml。
  原Git未退出时独立进程rename必须拒绝，父句柄全部关闭后仍拒绝；Git退出后同目标rename/恢复
  必须成功以排除旧ACL假阳性。保留实际输出，原进程全终态及证据归档后治理清理。
  新API机制测试不是旧baseline red/P01整体验收；真实目录替换/准备流程接入仍待完成。
- v113原34167终态13PASS/673.61s，XML SHA256
  c142db0c1e6d62bdc732461a26bc3e314cb952c253f7e715005d5b8ff2af9882。
  actualFull74PASS242.35s；未知lock被拒绝、bytes/identity/原execution未变；测试自建canary
  保留至rejected-independent-main.lock后，真实absence/native terminal resolution、独立采用、
  原Full/attempt不变、重放与failed release全部通过。新进程原CLI replayPASS/FAILED/issues=[]，
  C=1b06d1faecb66ab448f1e8aae461fae601a45c3a，原.lock不存在，canary原内容仍在。
  原代码与fixture保留v113。仅关闭准备与abort失败边界，不新增P01完成映射，不是final C验收。
  下一步必须固定common/refs/heads目录的原生身份/活句柄跨Git效果，并复用现有公开Full profile
  inspector复核当前采用资格及原atomic captures，不能仅以旧LOCAL记录或Full custody代替当前
  loaded/input闭合；随后实现contained commit/checkout及独立PUBLISHED/partial/expired恢复。
  原v2 lease已通过heartbeat续期至2026-09-14T19:29:08.874118+09:00，fresh LANE preflight PASS。
- v112短节点1PASS6.58s，XML749827d8bdc7db0d1a55014c95eb7d06327433201282fcca8c10d700559b0b09；
  最初pytest未建父目录在sessionstart报WinError3，无root/XML/collection；确认不存在后只创建已
  预登记父目录再运行，不计此启动失败为产品red。实际Git exe显式identity+2links读取45032bytes，
  SHA4ab2b391de7fb5eb3afb34b5bd19ee7f71d0450bf7e4c207b201a0392e3825ae。
  v112长原39195终态1FAIL643.63s，XMLbc26489dcb1f2d965e6a55f3f0eccb41460fd68f0a0934c2df2eca13c68b550d。
  actualFull74PASS243.50s；真实准备/原event/独立Git native oracle/不可改写拒绝/abort及exit7/FAIL
  收存均通过；未知独立main.lock存在时原adopter未拒绝，目标DID NOT RAISE为有效SUT red。
  原fixture随后按旧不安全观察允许failed release；原canary保留原.lock，不伪改记录或删除证据。
  修复在独立采用前及原atomic内核验原Git已EXITED/REUSED、锁路径lstat为不存在；任何残留拒绝，
  采用记录绑定原preparation SHA与实际OS进程/absence resolution，不允许准备存在而resolution缺失。
  v113预登记D:/Work/devx015-publication-preparation-resolution-v113，启动前不存在，DEVX-015拥有，
  同原actualFull节点及2未ready/9请求隔离/1adapter回归，外16/loadfile；XML
  outputs/validation_runtime/devx015-publication-preparation-resolution-20260914-v113.xml。
  保留原fixture/结果/新canary（仅测试自建对象移至证据目录，非产品自动删除），归档后治理清理。
- v111原59273终态12PASS/1FAIL/1ERROR/646.76s，XML
  bac89341d3d2c8c7071771de3290b7a4bf9ea27706f37a112f403095cbf285e8。
  actualFull74PASS244.00s；新准备组件把安装Git exe的正常2条hardlinks误当source alias拒绝，
  Git尚未启动；actual.exe=45032bytes，physical identity已原地核验，不放宽普通source默认门禁。
  原Job失败导致未terminal释放拒绝；原61823恢复调用终态RECOVERED_TERMINAL后，
  独立UNCHANGED与原failed release完成，replayPASS/FAILED/issues=[]，不编辑原receipt/事件。
  原源码/测试/fixture/XML保留v111。修复限定reader显式冻结identity+exact link count；
  原source/artifact默认仍只允许1链接。Git子进程环境清除GIT_*重定向并记录实际派生env hash。
  v112预登记D:/Work/devx015-publication-preparation-lock-v112，启动前不存在，DEVX-015拥有。
  先同根外16/loadfile短节点核验显式exe硬链接/旧默认拒绝，使用独立short子根；再原actualFull
  准备/abort节点增加未知main.lock残留必须拒绝释放的目标断言，原adopter此门禁尚未实现。
  XML分别outputs/validation_runtime/devx015-publication-preparation-links-20260914-v112.xml和
  outputs/validation_runtime/devx015-publication-preparation-lock-20260914-v112.xml；
  短basetemp为D:/Work/devx015-publication-preparation-lock-v112/short，长为
  D:/Work/devx015-publication-preparation-lock-v112/long；均启动前不存在，不覆盖任何已创建root。
  保留canary/原执行后治理清理。
- 后续准备组件接入原PublicationLifecycle：仅原活Job worker/当前Full与LOCAL intent/原拓扑可用，
  固定Git子进程继承原Job，原句柄核验PID/creation/member，发送精确LF并读取实际两ACK。
  真实锁对象、原拓扑、实际worker/Git身份和exe SHA写入同attempt.main_preparation，
  原Full与先前attempt不变，记录不得删除/替换；该组件退出时原Git abort并等待终态，
  不开放commit。仍须接稳定PUBLISHED adopter与checkout/崩溃恢复后才可开放公开publisher。
  v111预登记D:/Work/devx015-publication-preparation-custody-v111，启动前确认不存在；DEVX-015拥有。
  原actualFull绑定/Job节点加入原准备事件、独立native Git oracle、不可改写拒绝、abort后原失败
  收存/UNCHANGED释放；同时原2未ready+9请求隔离+1actualFull adapter投影回归。外16/loadfile，
  XML outputs/validation_runtime/devx015-publication-preparation-custody-20260914-v111.xml。
  保留原fixture/测试/实际事件及输出；全部原句柄终态且证据归档后按治理退出，不计P01完整接受。
- v110原1580终态3PASS/10.41s，XML SHA256
  a395a3335f3d5d42b46727feabe37c200950742d2e693e38a50d19d4fd5bf999。
  原Git进程实际start/prepare ACK后竞争更新被main.lock拒绝；commit将准备对象原物理身份
  保留至main，abort保持旧main；stale-old拒绝且保留竞争main。HEAD/index/source bytes不变。
  每例git-prepared-ref-evidence.json保存原argv/ACK/PID/竞争stderr/终态与准备对象身份。
  结论限定当前Windows/Git的原语行为，不等于原Job持久化、崩溃恢复或完整P01接受；
  下一步接原publication attempt事件与真实Job身份，独立稳定观察必须拒绝未知/替换对象，
  补原准备到commit/checkout窗口恢复后才允许公开publisher。实际项目main与peer未修改。
- v109原71666终态3FAIL/10.09s，XML SHA256
  5ef4d1ddf5fa08688efa8e3f57820328f156c213b8a10d274727a581c7c74df4。
  Windows text=True将协议LF转换CRLF，实际Git报unknown command: start，未抵达prepare；
  属测试协议输入缺陷，不是SUT竞争有效反证。原测试副本保留v109；改用明确ASCII bytes。
  v110预登记D:/Work/devx015-git-prepared-main-cas-v110，启动前确认不存在；DEVX-015拥有，
  同原3参数节点、外16/loadfile，XML outputs/validation_runtime/devx015-git-prepared-main-cas-20260914-v110.xml。
  仅隔离Git原语实验，不操作项目main/peer；全部子进程终态、证据归档后按治理清理。
- v108原59878终态1PASS/635.64s，XML6cdd9712ef1eb71bd676dc6375ede4faf2ceab86ac64c80ed738d4ce8cf8f03d。
  原内部Full74PASS/239.76s，C=e6e9224aaf7b928fd54ea3e7193d736f1a655508；修复后的原绑定正例与
  九错绑定通过，实际reserve/Job/native oracle/exit7/FAIL custody/selfPASS拒绝/释放前拒绝/
  独立UNCHANGED/重放/原fixture failed release全部通过。新进程原CLI replay=PASS、phase=FAILED、
  issues=[]；实际结果controlled_local_publication_worker_result.v1/FAIL/BEFORE_GIT_EFFECT。
  原代码/测试/执行证据保留v108。这里只证明未改checkout失败收口，不增加P01三变体完成映射。
- 下步先验证当前Git2.45.1的原生prepared ref事务。安装版官方git-update-ref.html说明：prepare
  锁定queued refs，old-oid不匹配时不改动；commit单ref原子更新，abort释放锁；多ref并非对reader
  同时可见。不能外推HEAD/index/worktree天然原子。先实测prepared main.lock的物理身份在commit
  后是否成为main ref，并验证竞争拒绝/abort/stale-old，供原Job后续持久记录实际准备对象使用。
  v109预登记D:/Work/devx015-git-prepared-main-cas-v109，启动前不存在，DEVX-015拥有，外16/loadfile，
  XML outputs/validation_runtime/devx015-git-prepared-main-cas-20260914-v109.xml；仅隔离publication_checkout
  内的真实Git原语测试，不是原公开publisher/Full/CAS恢复整体验收，不操作真实main或其他checkout。
  保留argv/ACK/退出/refs/index/锁对象身份，全部Git子进程原句柄退出后，证据归档并治理清理。
- v107原12314终态12PASS/41.69s；仅既有未ready/隔离/原Full投影回归，不替代实际发布attempt。
  v108预登记D:/Work/devx015-publication-failed-native-job-v108，启动前不存在，DEVX-015拥有，外16/loadfile；
  XML outputs/validation_runtime/devx015-publication-failed-native-job-20260914-v108.xml。
  原实际Full fixture正例（租约authority与用户任务明确不同）、九错绑定、原reserve/同请求重放，
  原native Job bind/resume、原worker admission与独立NativeOracle成员/创建时间/退出；child在
  Git效果前实际exit7并产出FAIL。故障PASS bytes拒绝并保留，恢复原实际FAIL custody，采用前release
  应拒绝；独立UNCHANGED观察/幂等重放后，原fixture正常failed release。无真实main写入、无自造Full。
  本轮仍不是PUBLISHED/CAS/部分写入/过期恢复验收；如失败保留原执行证据，禁止更换或手改lease。
- 原PublicationLifecycle新增adopt_unchanged_failed_attempt：仅最后attempt实际RESULT_RECORDED非PASS，
  原事务/Full/intent和当前checkout完整相同、原Job无活动进程时，独立观察者在同store记录
  publication_stable_observation。观察绑定原attempt digest/原intent topology/真实observer，保持
  原Full及所有attempt原bytes不变；原记录不可删除/替换，之后才可正常failed release。
  这只支持失败且原状态未改变，不提供PUBLISHED、未启动取消、TTL后/部分写入恢复或新发布权限。
  通用worker PASS继续拒绝；不能用原Full PASS替代稳定态观察。
- v107预登记D:/Work/devx015-publication-stable-contract-v107，启动前不存在，DEVX-015拥有，外16/loadfile，
  XML outputs/validation_runtime/devx015-publication-stable-contract-20260914-v107.xml；先跑原12项隔离/
  未ready/实际Full adapter投影回归；v108随后登记并执行原实际Full+修复身份正例/九错绑定及
  真实原Job失败（原worker admission+独立native oracle），原失败结果不准自报PASS，采用前release拒绝，
  实际UNCHANGED后原fixture failed release。无main写入，后续真实成功发布及部分状态恢复仍必需。
  若验证失败，保留原Job/lease/fixture，按实际退出证明恢复，禁止手工改租约。证据归档后治理清理。
- v106原31353终态1FAIL/601.12s，XML2f5b2e7c93e22ec69237ec719970e813d42f16d61aea0398469eec3fb2ac1239。
  内部原Full 74PASS/241.64s，C=e7a49b09c1982176fcad0c975b6e0a049ab0d61b，LOCAL前置复核成功。
  新生命周期把execution.subject_task_id错误等同transaction.task_id，导致真实合法绑定拒绝；
  实际分别为ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD与DEVX-015-MERGE-FIXTURE。
  cwd/lease/actor相符；原代码和证据保留v106。修复保留execution与原Full/lease authority精确绑定，
  用户任务仍由原fence/checkout intent独立核验，不合并两个namespace，不通过改fixture身份绕过。
  下一在此真实正例补九种错绑定及真实失败发布Job的独立UNCHANGED采用，后者仅失败安全收口，
  不宣称PUBLISHED/CAS/部分checkout恢复已可用。
- v105原49434终态12PASS/42.85s；未ready原事务reserve/worker两路径无事件拒绝，既有v5通用入口
  九项隔离和实际Job Full adapter原投影回归通过。仅证明这些路径，不是实际发布attempt成功。
  v106预登记D:/Work/devx015-publication-original-full-binding-v106，启动前不存在，DEVX-015拥有，
  外16/loadfile，XML outputs/validation_runtime/devx015-publication-original-full-binding-20260914-v106.xml。
  使用原full-profile-publish fixture、原实际Full/完整readiness及LOCAL事件，先验证原binding正例，
  再验证八种错绑定reserve拒绝和未绑定worker拒绝；不append无法稳定收口的发布attempt，不伪造Full。
  原Full fixture最终走原release；这不是当前最终候选C整体Full，也不据此完成P01映射。
  稳定adopter及实际publisher/Job/CAS/中断恢复仍必需；保留原始证据后治理清理。
- 本轮实现原fence实例上的PublicationLifecycle：复用同一guard.store/atomic上下文，虚拟inner
  attempt承载原bind/resume/exit/失败custody/recovery，outer原Full投影不变。reserve重新核验
  原LOCAL_MAIN_FF_PRE事件、原intent、真实Full custody/claim和当前checkout；事件append前再次
  核验，不把请求自报hash当权限。同request不重派发；后续恢复仍须原terminal失败attempt链。
  新worker admission叠加原事务绑定与真实Job成员/活launcher，结果v5独立格式，通用custody
  拒绝自报PASS；未实现稳定采用前execution_is_terminal(v5)仍false。尚无公开publisher/worker
  命令，不以这层实现宣告CAS/恢复可用，不能在实际任务上reserve一个无法收口的发布尝试。
  先验证无Full/错事件/错Full主体均不append；后续必须补真实Job正常链及稳定adopter后才开放派发。
- v105预登记D:/Work/devx015-publication-lifecycle-admission-v105，启动前不存在，DEVX-015拥有，
  外16/loadfile，XML outputs/validation_runtime/devx015-publication-lifecycle-admission-20260914-v105.xml；
  原真实未ready事务拒绝及既有v5通用隔离/原Full事件投影回归。没有虚构Full/publication事件。
  保留原件/输出后治理清理，P01三项映射不变，整体仍未完成。
- v97原66941终态10PASS/39.94s，XML SHA
  7173d4aac017cd5a6a11da4a9b784636a2a482d9e84443bd2ff647528fe9bb85；SUT SHA
  14dbf742226675d5d2cfb66675ee87893381de4ff986b4c527da10eeb61de38d，测试SHA
  34740383107cc2690358ffe584cad4067f259d6ea0424849e066672a51b05b55。原件保留v97。
  原adapter真实Job/退出/summary之后检查候选记录而不append虚构发布：完整Full投影相同，
  内部重封Full reason及其新hash仍被原历史转换拒绝，退回旧Full/直接release/独立attempt/自报PASS拒绝；
  同时原9例请求拒绝回归通过。Ruff/strict mypy/scoped diff通过，flow与RCF SHA同步
  dbd4267d408d4c5834b9da6a40f451bbdd60d05b781ab805482f402265d276ad。
  专用reserve/Job入口/稳定采用/CAS/recovery仍未开放，62/106 PARTIAL/NOT_EXECUTED保持。
- v97预登记D:/Work/devx015-publication-envelope-v97，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-publication-envelope-20260914-v97.xml。租约execution.v5在
  原Full.v4字段外只增加publication_attempts，Full projection保持原记录；验证attempt绑定原
  Full/候选/lease/cwd/host等身份、严格前次恢复关联与历史不可改，所有新发布记录当前不可release，
  自报PASS禁止。通用reserve仍拒绝，尚未开放发布执行或稳定采用；不以此替代后续executor/CAS。
  使用既有原Full adapter实际Job/退出/summary凭据检查纯状态转换，绝不写入虚构发布event。
  同跑既有9个请求拒绝例；adapter测试不是formal Full/P01验收。保留原件后治理清理。
- v96原命令直接终态9PASS/7.99s，XML SHA
  aaca67ffb9102757391c72af932cc21439413ffe1a8895eb73850dd8dc9dfdc1；SUT SHA
  fd0542f53c1b8ee64a6d99e5dc7c942b77297deb0972ae43f6753c4587920bb8，测试SHA
  3c632224335a120613335b99223e93200beb7bbec04263e74bbb04af399eb191，原件保留v96。
  实际原store九例拒绝后replay不变、execution仍空，最后原release；有效请求仅格式接受，
  通用执行入口一律PUBLICATION_PARENT_REQUIRED，不建立新执行能力。Ruff/strict mypy/scoped diff通过。
  仅内部尚不可派发合同，无公开CLI/数据流变化；不增加P01映射，不替代后续真实执行/恢复验收。
- v96预登记D:/Work/devx015-publication-request-contract-v96，DEVX-015拥有，启动前不存在，
  外16/loadfile，XML outputs/validation_runtime/devx015-publication-request-contract-20260914-v96.xml。
  原workflow_coordination增加发布request.v5形状：独立kind、C/M、原transaction/event/intent/Full
  身份、首次PUBLISH或带前次执行SHA的RECOVER；原通用reserve与standalone execution验证明确拒绝。
  此阶段不创建发布执行记录、不接原source权限，不授予调度/写入；下一步必须在同租约内保留原
  Full凭据并绑定发布attempt/native Job/稳定态采用，之后才能开放执行入口。9个合同/原store
  无副作用拒绝节点仅验证格式隔离，不冒充实际Full/CAS/P01验收；证据保留后治理清理。
- v95原86646终态2PASS/1381.65s，XML SHA
  49256e6a20ba9d530a5874d487db573b1c6e9e1ace75721c596c9d2f2413d7f6。
  第一条原Full74PASS/248.68s后原公开检查正例通过，原事件绑定一致；实际单独config漂移拒绝
  PUBLICATION_LOCAL_INTENT_CHANGED，随后隐藏index拒绝，检查均不改写原事件/refs/index。
  第二条原Full74PASS/251.84s后既有普通发布链通过，实际fixture终态RELEASED；独立Git复核
  HEAD=main=bare remote main=f18b7b1ef2818ef75921ac061cbb82d0d8a89492。无真实项目发布。
  SUT SHAe632030abae1a1f1ea1b773781b449100f62a8179ba0ccf25070c84cda79292e，测试SHA
  2572aad84a63410944d6b9f8dc8b8edc2900be226a0fcf6bf3bdf0094d92e8e8，原件保留v95。
  既有正常fixture仍用Git switch/merge，并非新增受托管发布执行器；实际CAS/崩溃恢复仍必须实现，
  不据此增加P01完成映射。62/106 PARTIAL/NOT_EXECUTED、最终C全部门禁与OPS080整体验收均保持。
- v95预登记D:/Work/devx015-local-intent-event-v95，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-local-intent-event-20260914-v95.xml。复用v94原Full公开链，
  验证原LOCAL_MAIN_FF_PRE事件持久化候选/main/lease/transaction/真实拓扑；实际Git config变化后
  新捕获虽内部一致也须PUBLICATION_LOCAL_INTENT_CHANGED，原事件与Git状态不被检查改写。
  同跑现有normal实际bare-remote发布fixture，保留GitHub origin身份、仅pushurl和显式fetch使用
  原local bare remote，不放宽repo identity门禁。证据保留后治理清理，当前尚未执行。
  原事件是后续执行请求必须独立绑定的输入，不是自封hash执行能力；尚未接实际executor/CAS/recovery。
- v94原85339终态1PASS/621.04s，XML SHA
  786398c7160f91e6b206d1a0e31f53e2396741de146270770097dc5fa5d52d57。
  原隔离Full真实74PASS/253.87s，candidate=10d7203e85e7f0615daee90a3bfe3ed11d287019；
  原公开CLI实际exit0/OBSERVED，绑定原transaction/event/C/main；随后真实index隐藏标志故障
  exit2/LOCAL_PUBLICATION_INDEX_HIDDEN，两次检查均无refs/index/治理JSON改写，Full汇总原件不变。
  源码/CLI/测试原件保留v94；测试SHA80748d48d39ab01863a0da7ebe09bdc7e43f07abdb93fab8a73df22aa7b5e071。
  这补齐只读入口原Full后的公开正例，不是实际发布执行/恢复，更不是最终项目C的Full验收。
  P01三项与R02.local_publish仍未映射；62/106 PARTIAL/NOT_EXECUTED不变。无真实main/remote变动。
- v94预登记：D:/Work/devx015-local-inspection-actual-full-v94，DEVX-015拥有，启动前须不存在，
  外16/loadfile；XML outputs/validation_runtime/devx015-local-inspection-actual-full-20260914-v94.xml。
  新节点test_local_publication_inspection_cli_after_actual_full使用原canonical full-profile-publish
  fixture、原实际Job/16workers/完整readiness/profile，原LOCAL_MAIN_FF_PRE后调用原公开CLI正例，
  再实际assume-unchanged故障要求拒绝且不改变refs/index/治理原件。不得模拟Full或身份门禁；
  不运行native HKCU注册fixture。保留原件，证据归档后治理清理；尚未执行不计验收。
  发布意图可考虑绑定在既有LOCAL_MAIN_FF_PRE原事件payload（原store atomic下append），无需新增
  同phase事件/修改当前policy hash/第二日志；仍需实际执行托管、CAS、checkout恢复与独立采用。
- v93原70217终态10PASS/25.16s，XML SHA
  29064a9a0065f2d27b32397c10324776fa97013c37396bd6bdb7e3b11a3a029b；源码/CLI/测试原件保留v93。
  v91原9PASS XML1011e8e8e7cf96f74b07de3be03e2444dd2a3be763928a551de8f07e11da480f。
  新只读CLI要求原LOCAL_MAIN_FF_PRE+exact C，捕获前后原fence replay相同，候选clean门禁保持；
  builder原repo origin/sentinel核验、真实Git单/linked/packed拓扑、错误main/C/index/hidden flags、
  两种真实HEAD漂移均覆盖；原CLI未ready拒绝无refs/index/治理JSON写入。ready公开正例还需随实际
  原Full候选/执行器链验证，不能把纯builder正例冒充整条发布验收。
  workflow_integration SHA9b78a60e9915959733304fd98a483d6bd9af7400b785172ec24a32082d3a45a9；
  fence SHA32156f60699deef2a133f0801ecb8d88dac0a6b917078694a006428a3c187ccb；
  CLI SHAc0ee4297a10c19994c467339d5554d65d90be36a35033bb9a229870c37d2f5df。
  Ruff、3个changed生产文件strict mypy --follow-imports silent、scoped diff通过。
  system_flow及RCF期望SHA同步a8c02ecdbdc91a38e18cf7254632eeb75f05f1f73527f82cfb20eef293417a0e；
  仍62/106映射/44未映射、PARTIAL/NOT_EXECUTED，实际发布意图/条件写入/恢复和所有最终门禁继续。
- v92原进程终态1FAIL/8.24s（无后台句柄），XML SHA
  a813bd6534fbaf1e40ddef763515635dd8f342bd388893f8178970ed9a660a06。
  真实branch从C=2e5fe7cb4c790a60480d3536cec872030fc5f7f2变为同tree的
  D=2357e4edfd465cf25087cdc4b1a9c542da9385de，原构造器未抛错且输出candidate=C、captured_head=D；
  unexpected-admission.json及修复前SUT/测试保留v92。它仍publication=false，没有原项目发布副作用。
  修复在实际checkout快照后直接核对observed_head==candidate，再进入其他捕获步骤；旧最终全快照
  重核仍保留。v93必须覆盖初始及两类中途漂移，不以相同tree代替精确commit身份。
- v91原20145终态9PASS/22.79s，只读元数据正例/拒绝和原CLI未ready拒绝通过，非P01发布验收。
  复核发现另一个候选窗口须实测：首次HEAD=C检查之后、第一次worktree inventory之前，若真实
  branch已变为另一相同tree的新commit D，捕获可能稳定绑定D却仍自报candidate=C；原CLI末次
  candidate检查仍会拒绝，但纯拓扑构造器也必须拒绝这种错身份，不能供后续执行器误用。
  预登记D:/Work/devx015-local-topology-candidate-race-v92，先保持v91生产代码，新增真实Git update-ref
  fault node [candidate-before-capture]验证目标缺陷；不把import/新API缺失当red。
  XML outputs/validation_runtime/devx015-local-topology-candidate-race-20260914-v92.xml。
  修复后预登记D:/Work/devx015-local-publication-topology-v93重跑完整10例，XML
  outputs/validation_runtime/devx015-local-publication-topology-20260914-v93.xml；两根启动前均须不存在，
  DEVX-015拥有，外16/loadfile，证据归档后治理清理。仍不新增P01映射或发布权限。
- 当前核验：main=M实际占用D:/Work/AITradingSystem_devx014_source_preservation；DEVX-015候选
  checkout为D:/Work/AITradingSystem_devx015_integration，主目录D:/Work/AITradingSystem另有TRADING
  branch。不可根据目录名猜main位置，不得覆盖、切换或读取其他checkout的私人工作文件。
  原fence的LOCAL_MAIN_FF_PRE/REMOTE_PUSH_PRE仅准入，不是main ref/index/worktree的实际恢复执行器。
  现有Git2.45.1.windows.1，update-ref支持expected-old与prepare/commit，但该版本未提供symref-update；
  Git官方2.45文档明确多ref事务仍可能被并发reader观察到部分结果，不能声称跨崩溃原子。
  参考https://git-scm.com/docs/git-update-ref/2.45.0。
- 原P01/R02内的有限实施顺序：
  1. 原公开fence增加只读local-publication-inspect，要求原事务LOCAL_MAIN_FF_PRE与exact C；
     从真实worktree porcelain/共用Git/HEAD/index元数据捕获目标拓扑，重验同一快照。
     index的mode/OID/stage须与C树完全一致，main的旧checkout只读Git管理元数据，绝不遍历其worktree。
     输出明确dispatch/publication=false，不把自报plan SHA或只读诊断当执行授权。
  2. 在原租约/事务事件内绑定完整发布意图和实际checkout身份；受控main条件更新、main checkout接管
     与可消费边界须有明确持久顺序，不增加第二store/queue/scheduler，也不新建替代源码worktree。
  3. 崩溃恢复从原意图/原句柄身份与实际refs/HEAD/index判断，禁止覆盖较新main或未知对象；有限恢复
     到一致终态后才允许REMOTE_PUSH_PRE。保留原C/V及唯一源文件，原checkout有非本任务变化时拒绝。
  4. 实际Git竞争、ref更新前后崩溃、原CLI新进程恢复及最终required tiers/Full/发布事实分别验证。
  当前实施第1项；后3项仍必需，P01/R02.local_publish不因诊断节点通过而映射完成。
- 预登记D:/Work/devx015-local-publication-topology-v91，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-local-publication-topology-20260914-v91.xml。
  用真实单checkout/linked-main拓扑、错误C/main/index和捕获中拓扑漂移检查只读边界；新API不作为baseline red。
  原fixture与输出保留到最终证据归档后治理清理，无真实main/HEAD写入、无Full和注册表操作。

### 2026-09-14：X02同sequence竞争原CLI

- v90原75202终态1PASS/11.26s，XML SHA
  35dd7146872c2a6dfe22f4df3dd120925b5a46ca31f9bbd80e35ad0282797c74；测试/原fixture保留v90。
  原public observer精确exit2/PUBLICATION_BUSY，producer仍活且所有原refs/index/治理JSON不变；
  释放barrier后producer实际终态exit0，第三原CLI重放同receipt，只有ACQUIRED/FAILED两个事件，
  唯一sequence且无ACTIVE租约。登记X02.same_sequence_append，62/106部分映射、44未映射；
  统一counter/10mutants/finalC全验证/授权迁移发布及OPS080全链不缩减，未声明整体验收完成。
- 预登记D:/Work/devx015-terminal-sequence-cli-v90，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-terminal-sequence-cli-20260914-v90.xml。
  加强既有test_x02_two_process_release_has_one_terminal_transition：原producer在真实lease已释放、
  原publication终态event未写时，用stdin barrier持有原OS arbiter；另进程原public release CLI须
  精确PUBLICATION_BUSY，不是允许任意失败或成功。原refs/index及所有治理JSON在拒绝前后逐字不变。
  只有观测到该结果才释放原producer，等待实际exit0；再第三个原CLI重放须同一receipt，原事务对象
  不变，ACQUIRED/FAILED各一个sequence、无ACTIVE租约。此时序不以线程互斥或仅同binding替代。
  保存原CLI输出/事件/原fixture，证据验收归档后治理清理；通过前不新增映射，最终全目标不变。

### 2026-09-14：R01原source副作用执行者重放

- v89原92205终态1PASS/633.75s，XML SHA
  c9bd1a2294409c10316d020f97768f078a929d7503415f1e9b9869de9735c557；测试SHA
  5050361f8db918d67bbfd9a3c6073cda5fc89cb7d6567cc1e0c55dc71ee218b9，副本保留v89/execution-tests.py。
  原真实Job在live重放两端存活且同一process/request，重复命令REPLAY_ONLY/OBSERVE_ONLY；
  串行重放run字节/事件/refs不变；四字段变更均精确exit1/WORKFLOW_MERGE_SOURCE_REQUEST_BINDING，
  原字节finally恢复且所有原run字节及事件头不变。随后原安装/私有候选采用与原handoff全部通过，
  fixture事务终态FAILED/lease-38a18355b0b93f953135已释放；该FAIL是source-only交接边界，不是假Full。
  独立读取原租约事件得到source RESERVED/CONTAINED_SUSPENDED/RESUME_INTENT/RUNNING各一次，
  不是按重复返回同binding推定只有一个executor。登记R01三变体，61/106映射仍PARTIAL/NOT_EXECUTED；
  45剩余映射/全统一counter/10目标mutants/finalexactC所有tiers与Full/授权迁移发布/OPS080全链仍必需。
- v88原20523终态1FAIL/559.68s。原source Job完成真实生成/双parent候选/原字节与mode检查；
  live重复确为REPLAY_ONLY/OBSERVE_ONLY且两端进程活，串行重放也无新增事件/run字节。
  首个argv变更到达WORKFLOW_MERGE_SOURCE_REQUEST_BINDING，原workflow CLI未包装异常，
  合法拒绝exit1；测试误用了publication CLI的exit2，故此批不能映射R01或宣称后续安装已执行。
  原请求finally恢复，原fixture teardown治理释放，原测试副本保留v88/execution-tests.py。
  修订只断言原workflow精确exit1和精确异常码，不改生产入口或接受任意异常。
  预登记D:/Work/devx015-source-request-replays-v89，DEVX-015拥有，启动前不存在；同原节点/外16loadfile，
  XML outputs/validation_runtime/devx015-source-request-replays-20260914-v89.xml。
  保留到证据归档后治理清理，全部后续安装/handoff须实际通过，不沿用v88未执行结论。
- 预登记D:/Work/devx015-source-request-replays-v88，DEVX-015拥有，启动前不存在，外16/loadfile；
  XML outputs/validation_runtime/devx015-source-request-replays-20260914-v88.xml。
  复用既有source-job完整生成/私有双parent commit/安装/handoff实证，不另造执行平台或Full。
  新节点test_r01_original_source_job_replays_and_rejects_changed_payload[source-job]：
  原worker真实Job存活前后夹住同请求公开CLI调用，须REPLAY_ONLY/OBSERVE_ONLY且同一真实进程/请求；
  原生成后串行重放不可改变run字节/事件/refs；再逐项改变同ID原execution_request的argv、env、
  review、source-head，原CLI须SOURCE_REQUEST_BINDING拒绝，无新增事件，每例finally恢复原字节。
  不修改真实DEVX/OPS控制面，不创建注册表key；保存原CLI输出/故障payload/独立原件状态。
  继续原真实安装与handoff到FAILED租约释放终态，不能只在拒绝处结束或把binding当执行证明。
  当前58/106映射不提前新增；统一counter、mutants、最终C全门禁及两任务完整验收边界不变。
  测试完成后保留证据到治理验收归档清理；若live overlap未达到，判证据不足，不放宽成串行PASS。

### 2026-09-14：R02终态回执原CLI恢复

- v87原57447终态1PASS/11.84s，XML SHA
  94aaf178d25f271bb0fc0dd2238c928ded5b6ec26e406019eda157f223361a27，原fixture与测试副本保留v87。
  原CLI恢复得到与崩溃前待写内容逐字语义相同的FAILED receipt，重复CLI不新增事件/回执/租约，
  refs/index/原治理字节不变，只有原锁诊断变化且RELEASED；登记R02.terminal_before_receipt。
  58/106映射仍部分状态、最终验收NOT_EXECUTED；剩余48映射/统一counter/10mutants/finalC全门禁/
  迁移发布/OPS080全链保留。R01要求一个真实副作用执行者，不会只把acquire重复binding测试映射为完成。
- 预登记D:/Work/devx015-terminal-cli-recovery-v87，DEVX-015拥有，启动前不存在；外16/loadfile，
  XML outputs/validation_runtime/devx015-terminal-cli-recovery-20260914-v87.xml。
  加强既有test_r02_terminal_receipt_crash_is_recovered_in_fresh_process，不新增运行平台：
  原producer仅在真实FAILED事件已落盘、原receipt写入前os._exit(17)，留下PID/目标/待写内容。
  换原public release CLI恢复，并再原CLI重放一次。独立核对首次只新增原期望receipt，
  重放无新增治理对象，两次refs/index/原事件/租约ledger不变，只有原锁诊断侧车可变化且RELEASED。
  这是失败事务的可恢复终态，不伪造Full、PASS或发布；先保留崩溃与CLI原始证据再验收归档清理。
  当前57/106映射仍部分状态，通过前不新增R02映射；其余完整目标保持。

### 2026-09-14：V03独立冻结权威

- v86原97273终态2PASS/20.92s，XML SHA
  353556c9b5929b8484ace0eb4473640830287904a49e638ec2fa4f5e6360dedc。
  两端SHA清单证明仅精确arbiter诊断侧车变化；其余治理对象、refs、index及故障对象原字节不变，
  最终无ACTIVE租约。当前测试SHAfec3bf585f61203d635181abbd642e513134d59eee7958b3f83e7d04779f9ef4，
  副本保留v86/fence-tests.py。v85四个有效PASS及v86两PASS对应V03其余三个变体，映射57/106，
  PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED保持，不是57项最终验收。剩余49映射、统一counter、
  10mutants、最终exact C required tiers/Full、迁移发布及OPS080全链目标不变。
- v85原4767终态4PASS/2FAIL，64.64s，XML SHA
  50e2f4d611ac104f34dd7a50a1535b2d4e62671ec579c7c0266b1371e60573fe。
  六例的身份拒绝均达到，后两例因release串行化正常更新arbiter.owner.json诊断侧车而
  被过宽的全JSON恒等断言判失败；不是允许篡改receipt/event或lease ledger。
  原测试副本保留v85/fence-tests.py。修订断言精确要求差集只有原leases/arbiter.owner.json，
  两端均原schema/actor/RELEASED/production_effect=none；refs/index及其余全部JSON逐字不变，
  保存两端原侧车和独立状态SHA。生产代码不变，不删除锁检查。
  预登记D:/Work/devx015-v03-terminal-object-v86，启动前不存在；仅重跑上述receipt/event两例，
  外16/loadfile，XML outputs/validation_runtime/devx015-v03-terminal-object-20260914-v86.xml。
  DEVX-015拥有，保存原始证据至验收归档后治理清理；通过前不映射第三项，不缩减最终验收。
- 预登记`D:/Work/devx015-v03-frozen-authority-v85`，启动前须不存在，DEVX-015拥有；
  XML `outputs/validation_runtime/devx015-v03-frozen-authority-20260914-v85.xml`。
  六个独立fixture用例，不运行Full：
  1. 原CLI生成并验证真实Git lane/main/manifest计划，真实fence冻结原计划后，改掉task_delta
     并独立重算plan SHA/id，原rebuild须拒绝PLAN_REBUILD_MISMATCH且fence拒绝计划替换；
     另一例以原builder形成内部一致的新contract声明计划，独立validator可通过，但原冻结fence仍拒绝。
  2. 原真实checkpoint链到FORMAL_VALIDATION_PRE，以原publicvalidate验证合法任务/候选正例，
     再分别提交错误task参数或在fixture真实提交另一HEAD，核对精确绑定拒绝；这不是Full完成证明。
  3. 两个真实已失败收尾事务分别提供有效receipt/event；替换目标fixture对象前单独保存原件，
     原公开入口须根据目标事务/租约绑定拒绝外来对象，而非仅因对象自身checksum不一致。
     故障只在本批新建隔离fixture内；禁止改真实DEVX-015/OPS-080控制面或旧验收证据。
- 各例保存原CLI argv/exit/stdout/stderr及独立refs/index/原治理JSON快照，检查拒绝无额外写入；
  保留原件和故障对象，完成验收归档后治理清理。无新runtime/native注册表/生产操作。
  通过前不新增映射，不以低层身份准入当整体验收；最终C required tiers/Full/10mutants及OPS080不缩减。

### 2026-09-14：V02共享原Full的其余身份漂移

- v83原92589终态1FAIL/680.59s，XML SHA4a94ad5e1040c02ae5f833a02fe4939fab4aaedd60a396e3251dac713ef0f257。
  实际74Full PASS/240.49s，原inspection正例PASS，五项故障全部按预期拒绝且每次原件/refs/index/events
  不变；恢复原状态后的原CLI正例被_prepare_profile_checkpoint内部60s超时拒绝，故整批不映射为PASS。
  原CLI原始输出及source/test/runner已保留v83；teardown原入口收尾失败事务，不重写原V。
  先在该原fixture对实际runtime identity、loaded implementation及七readiness作只读分段计时，
  不恢复/复用其失败事务授予publication，不重跑Full、不删检查。确定耗时组成后才处理边界。
- 原fixture只读分段计时（原37389终态成功）：runtime identity11.654s/16554文件、
  implementation5.579s/1244文件、七readiness33.540s全部PASS，其中architecture_generated25.861s。
  三段合计50.773s，未含完整证据capture/末尾复核/启动及Git成本；原wrapper60s不足以容纳
  已扩展的组合检查。设置单独命名FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS=180工程上限，
  仅该只读组合probe使用，不改remote/network时限、Full执行、TTL或接纳标准；超时仍拒绝，
  锁外probe完成后原锁内candidate/main/event/input复核保持。该上限复用现有inspection观测180s
  量级，为120s readiness观测及其余身份/custody预留60s；不移除检查或缓存承诺。
  测试外层public CLI观察期限210s（含子probe180s和进程/错误收尾），并记录每probe实际elapsed。
- v84预登记`D:/Work/devx015-post-full-identities-v84`且启动前确认不存在，原v83同一节点完整重跑；
  XML `outputs/validation_runtime/devx015-post-full-identities-20260914-v84.xml`。
  原v83失败及原Full结果不重标，v84 source/test/probe结果保留后治理清理，整体验收范围不变。
- v84原34876终态1PASS/696.47s，XML SHA298013ace7a07f1f74b86148d7b04722023fc6723ba4eb9eedd70a090e9cd35c。
  实际Full74PASS/245.21s；正常inspection67.032s、加载origin拒绝17.778s、其余四fault均拒绝，
  恢复原状态后原CLI准入74.778s成功LOCAL_MAIN_FF_PRE，原refs/index/V证据SHA不变。
  两个正常实测均超过旧60s限，验证本次组合时限修复的必要性；并非移除检查获得PASS。
  候选7fb4c9574dfb6e7172cf85d68fcab3aec2107238；源码/测试/原输出保留v84。
  publication源码SHA6c0cd8055e6c0527d37c8ce2c1eae409624da216a2d22718c8e3bacca322b4bd，
  coordination测试SHA9af3fa61f7f6bfba19ce1e35cde6305640ba0af48ab1fed4ca82898dddc83042。
  新增V02三项节点映射后合计54/106；这是映射进展，不代表54项最终接受。
  52项剩余映射、统一计数/10mutants、最终C全部required tiers/Full/迁移发布及OPS080仍必需。
- 连带验证预算须在最终Full前对齐：_run_actual_profile_full原默认driver120s以及旧整链测试
  publicinspection60s，已小于本轮真实Full245s/inspection67s；新节点显式600/210已验证，
  旧16worker整链调用应对齐同一工程观察预算。不得改execution.py旧小case的2worker/120s，
  不新增Full重复证明已通过节点；保留旧节点并在最终候选required checks重新执行。
  随后继续V03真实plan重建与冻结publication绑定；已有只检查未重算checksum的用例不能直接记完整接受。
- 已将原16worker _run_actual_profile_full默认driver对齐600s，将旧whole-readiness测试的
  inspection/public CLI观察对齐210s；新已验证节点原本显式600/210，运行行为不变。
  本项Ruff/scoped diff通过，不声明旧节点已重新执行；在最终C required checks验证。
  execution.py旧2worker/120s用例、生产Full/TTL/remote时限及所有断言不变。
- 预登记`D:/Work/devx015-post-full-identities-v83`（启动前须不存在），DEVX-015拥有；
  XML `outputs/validation_runtime/devx015-post-full-identities-20260914-v83.xml`。
  唯一真实Full/7readiness/16worker形成C/V，先原publicinspection正例，再依次独立探针：
  1. 仅隔离public CLI进程通过外部测试bootstrap改变已加载yaml.Reader.peek的co_filename，
     不改安装文件、不替换门禁；子inspection继承同一故障，bootstrap各PID输出执行见证。
  2. 实际duration输入observed_seconds变化；3. 发布策略version变化；
     4. 实际generator入口由main变为exit99；5. test manifest移除一个必跑test选择。
  后四项只改fixture原有mutable文件，并用原bytes恢复，绝不改原V/receipt/事件。
  每探针核验拒绝、原refs/index/事务及Full证据SHA不变；最终原状态恢复后通过原CLI
  LOCAL_MAIN_FF_PRE正例。完整一次Full共享，禁止把仅上游runner拒绝当publication闭环。
- runtime探针是加载代码origin身份变化，不以无害的publication自身env差异充当负例。
  预期前者PUBLICATION_FULL_CLOSURE_INVALID；策略变化须实际policy commitment拒绝；
  其他mutable候选输入/生成器/选择变化须原clean-candidate门禁拒绝。保留全部探针见证。
  原件在验收归档后治理清理；不新增native registry键/系统安装/生产writer。
  本批通过后仍需统一计数/10mutants、最终C所有required tiers/Full/迁移发布及OPS080。

### 2026-09-14：V02真实Full后候选变更拒绝

- 原v79转储`C:/Users/32739/AppData/Local/CrashDumps/python.exe.96032.dmp`经本机cdb读取：
  execnet启动命令、故障线程Py_DumpTraceback/_Py_DumpTracebackThreads，其他四线程正在文件读取。
  fixture的conftest显式为每个worker安装30s重复dump_traceback_later；该额外诊断不是验收门禁。
  改为仅启用正常faulthandler崩溃诊断，删除fixture/driver重复timer，不改Python安装、并行度、
  身份检查、Full范围或期限。这是移除测试引入的干扰；尚不声称已证明所有native崩溃根因。
  CPython上游存在同类watchdog堆栈竞争报告https://github.com/python/cpython/issues/140815，
  仅作交叉参考，不替代本机证据或把不同版本缺陷视为本机已证明根因。
- 同批修复原controller在worker异常退出时读取缺失workeroutput导致二次异常：missing/非dict/
  error均保留None无效身份，正常报告保留原值，原sessionfinish继续拒绝worker身份不全。
  预登记`D:/Work/devx015-worker-down-hook-v80`及`D:/Work/devx015-worker-down-hook-v81`，
  分别原SUT与修复后的4项hook边界；只是定点回归，不计V02/native/106整体验收。
  XML同名20260914-v80/v81写入outputs/validation_runtime；原件保留后治理清理。
- v80终态3FAIL/1PASS、6.98s，原缺失/非dict直接AttributeError、error仍采信报告，正常对照PASS；
  XML SHA5e81f1c29a155f3594b43a981a163d8b8c0fa4dbcf72570c917200410b67e599。
  v81终态4PASS、6.49s，XML SHA2831080ef0457d92d62e4e69ff9f0684fe27acdf75feab0d81a5a3d38f4640e2。
  修复workflow_execution SHA c0cd69f2043fe9417fe0209e7c99cdad14ee96bd633b82898c533bd74be5d22e；
  原/修复源码与测试分别保留v80/v81，Ruff/单模块strict mypy/scoped diff PASS。
  仅此hook回归不能证明V02或native crash彻底消失；原v79失败证据仍保留。
- v82预登记`D:/Work/devx015-post-full-candidate-v82`并确认不存在，重跑原v79同一V02节点；
  XML `outputs/validation_runtime/devx015-post-full-candidate-20260914-v82.xml`。
  唯一差异为已记录controller修复及移除fixture-only重复stack timer；实际Full/七项readiness/
  16worker/身份校验/原发布入口均不缩减。启动后冻结SUT/tests/manifest直到原终态，原件保留后治理清理。
- v82原82182终态1PASS、593.90s；XML SHA49fa6f47e3d6e2b860ea1273a760ee08807850d24764bd47ea3e66efef6f7cf0。
  原真实Full 74项PASS/243.27s，七readiness与16worker见证齐全；原publicinspection PASS后，
  Git提交将候选b6d428fb5ff67fd4455a6f99d774b822ebdb7b73变为472c344c2dcec334f371a4f17bbea7fa701c0530。
  原公开LOCAL_MAIN_FF_PRE返回2/PUBLICATION_CANDIDATE_DRIFT；main c7afe5d1941e8eff588c6dcdfec83ea91e52d091、
  事务event及原Full证据SHA集合均不变。新增candidate_changed节点映射，合计51/106映射，
  不代表51项最终接受；55项映射/统一计数/10mutants/最终CtiersFull/迁移发布及OPS080仍须全部完成。
- 下一步通过v79保留仓库自己的原run_validation_tier.py full --recover-full入口，以原tx/task
  进行observe恢复；仅允许原Job/launcher终态已核验后的INSUFFICIENT失败收口，不重跑Full、
  不手改receipt、不丢弃原日志/请求/身份/profile。操作前后独立对比这些原件SHA及Git refs。
- v79原公开恢复已完成（07:49:56JST）：RECOVERED_FAILED_ATTEMPT/INSUFFICIENT，原exit3、
  Job EMPTY，dispatch_performed=false、publication_allowed=false；原lease-1b0884b9d9e709a7b74b
  已RELEASED，失败receipt和full_incomplete_recovery.json由原入口写出，proof SHA
  ff4e6d0d3d05493587dcbf7bf1a5cd3784971108ac8dfdef118374fa0663bfd3。
  操作前后四份原stdout/request/validation_identity/profile SHA及Git refs完全相同；不删除原件。
  v82 source/test/runner副本已保留，manifest核验51唯一映射、PARTIAL_NOT_ACCEPTANCE_READY。
  下一项仍为V02另三种身份漂移：尽量共用一次原真实Full，再做独立原发布准入探针；
  发布进程自身环境差异不等于原验证身份变化，不制造应拒绝但实际无害的“负例”。
- v79原84108已终止：1FAIL/1ERROR、396.15s；XML SHA
  400793c15b6a16b16c5093aa013f579331f48f8b5bbfaccd1b57d174eb23bca0。
  七项真实readiness PASS，执行request已持久化，但worker gw11异常退出，随后
  _BoundAcceptancePlugin.pytest_testnodedown直接读取缺失workeroutput导致连带AttributeError；
  内层no tests ran，未达到V02候选漂移断言，属于INVALID启动，绝不计有效red或验收PASS。
  fixture执行EXIT_CONFIRMED但未RESULT_RECORDED，因此release拒绝LEASE_EXECUTION_NOT_TERMINAL；
  不手改lease/receipt，不删除该隔离store。原源码/测试/runner已保留v79根。
  同时段Windows Application1000在07:28:19JST记录python311.dll异常0xc0000005，
  PID0x17720/start0x1DD43CF08CC82FD/report f66ca1c6-81ba-4d80-8a32-2947354114da；
  尚未证明其worker关联/根因，先核对native crash与原执行，不盲目重跑Full或弱化身份检查。
- 本批先覆盖candidate_changed：隔离仓库运行原真实Full/16 workers/全部七项readiness，
  原公开inspection须先PASS；随后仅在该测试仓库提交新candidate，再调用原公开
  LOCAL_MAIN_FF_PRE入口。独立核验拒绝、main不变、事务event不变、原Full证据bytes不变。
  不改写原receipt，不以伪造Full或内部mock代替；本批不声明另外三种V02漂移已覆盖。
- 预登记DEVX-015证据根`D:/Work/devx015-post-full-candidate-v79`，启动前确认不存在；
  XML `outputs/validation_runtime/devx015-post-full-candidate-20260914-v79.xml`。
  保留唯一原件及失败诊断，治理验收和证据归档完成后再审计清理；不新增生产writer或注册表键。
  该新测试的Full driver观察期限600s，依据现有真实16worker链曾需202s且包含额外身份捕获；
  不变更生产时限、不重标原120s超时为PASS，不更改既有测试默认时限。外层固定-n16/loadfile。
  23组106variants/10mutants、最终C正式tiers/Full/迁移发布及OPS080整体验收范围不变。

### 2026-09-14：受管host Full准入资源一致性

- 管理员保护根/普通writer及精确系统变更范围已向Owner提出具体方案，未得到答复前不执行
  HKLM/ACL变更或自行提权；继续原授权内的工程与验收，不把该外部条件当作全部源码停工理由。
- 对L01剩余准入检查发现：guard已把Full路径映射为host资源，publication validate(full)
  仍仅查旧raw路径，预计拒绝合法host槽。先以真实guard/fence acquire与正式验证前各checkpoint
  形成候选/租约，通过原公开validate入口验证；不伪造Full结果或将准入当成已执行Full。
- v77预登记`D:/Work/devx015-host-full-admission-v77`且确认不存在；测试host精确marker、
  覆盖父marker、错误marker及未注册legacy正例，原SUT先运行。
  XML `outputs/validation_runtime/devx015-host-full-admission-20260914-v77.xml`。
  只使用隔离registry transport，不创建实际注册表键；原源码/输出保留后治理清理。
  完整L01/native Full、L03、23组106variants/10mutants、最终C全部tiers/Full/发布与OPS080全部
  运营接受仍独立必需，不因本准入修复缩减。
- v77原71227终态2FAIL/2PASS、47.11s；XML SHA
  4fc238a794d585550a754fae716346595fd0ca23fb43561d2d2a65937258d78c。
  两个合法host WRITE槽都已由真实guard获得，原CLI main/argparse/validate报
  PUBLICATION_FULL_RESOURCE_MISSING；错误marker拒绝和未注册legacy正例通过。
  registry仅transport隔离，原候选/租约/阶段/CLI门禁执行；不是native Full或管理员安装接受。
  修复按可信binding.scoped_paths计算必需集合，必须包含host Full槽，且仅接受path/WRITE资源。
  v78预登记`D:/Work/devx015-host-full-admission-v78`且确认不存在，重跑4项并复核上一轮7项
  已登记旧终态追溯用例（共用注册夹具获得向后兼容参数）。
  XML `outputs/validation_runtime/devx015-host-full-admission-20260914-v78.xml`；
  源码/测试原件已保留v77，后续保留/清理与整体验收边界不变。
- v78原48347终态11PASS/67.39s；XML SHA
  0e44552a2e7c6fc35a4a8389fbf6714e08aaada2e3289e02c40593aa5a685cbc。
  原CLI代码通过真实argparse读取参数：两种合法host槽通过，错误marker拒绝，legacy正例通过；
  validate前后原store/事务bytes、index及refs相同，仍FORMAL_VALIDATION_PRE，无Full派发。
  原7项历史终态追溯回归通过；原生注册表/跨进程Full执行不在本批证明范围内。
  publication源码SHA 8a66b144217537895b54748fcc956a5f595143c1f41eb62075acd4a836903c62；
  测试SHA 4204f8bc57a8566f3cf8f9a00f822ea97c7b10b1a904421e5d36f95f99bee41e；均已保留v78。
  Ruff/strict mypy(scoped follow-imports silent)/显式路径diff通过。
- 等待管理员保护根/权限方案答复期间，继续不依赖系统权限的W2验收。V02不能仅映射runner
  内部拒绝：oracle要求不匹配V不能授权publication，需复用一次真实Full形成的C/V，随后在
  原公开发布准入上验证候选/环境/输入policy生成器/runner selection漂移，保留原V事实。
  先读原实际Full夹具并冻结本批时序/副作用边界；不使用伪造_full_result替代，不按每个helper
  重复Full。L01/L03管理员/原生接受、56项剩余映射、统一计数、10mutants及全部后续目标不变。

### 2026-09-14：管理员切换准入的历史终态追溯接缝

- 在连接管理员切换/恢复入口时核对到：publication release/terminal receipt仅从当前guard.store
  查lease；host切换后历史终态lease仍在已登记旧root，新host store不应复制它。
  先用原公开acquire/release产生真实FAILED终态事务，再配置隔离的host注册transport，
  以原release重放相同收据验证该缺口；这是迁移准入/恢复依赖，不替代管理员迁移实现。
- 允许修复范围：仅终态事务可只读追溯该checkout精确原runtime/leases路径，必须可信注册、
  retirement/物理身份/policy/完整ledger及原lease-intent均通过；拒绝ACTIVE恢复为写权限、
  未登记root、损坏和身份漂移，不扫描其它store，不复制历史lease，不创建第二写权威。
- v74预登记`D:/Work/devx015-host-terminal-origin-v74`且确认不存在；原SUT先验证缺陷，
  XML `outputs/validation_runtime/devx015-host-terminal-origin-20260914-v74.xml`。
  拒绝项和修复结果必须另存，不把API/夹具错误记为目标red；隔离目录保留证据后治理清理。
- 当前只读实测IsAdministrator=false，现有checkout runtime父目录属当前用户且继承普通用户
  Modify权限。未更改ACL/HKLM；实际管理员安装与精确保护范围仍待执行条件和后续实证。
  Win32实现设计参考Microsoft的SetNamedSecurityInfoW/GetSecurityInfo/RegFlushKey官方说明；
  本条不声称已经实现或执行管理员持久化切换。
- v74原44048终态1FAIL/4PASS、18.17s；XML SHA
  e009808c17d778cd02bdc9a2781a10595d47c85783d02b57dda32f5160bf7f2f。
  原公开release对真实已RELEASED的失败事务报PUBLICATION_LEASE_UNKNOWN，目标缺陷已触达；
  非Full/成功发布或原生管理员安装red。原SUT/测试保留v74；4项拒绝保持。
  修复仅终态读取已登记精确旧root，用native bounded reads和原canonical event parser/replay，
  核验retirement/epoch/policy/物理身份，拒绝未终态或损坏；原lease-intent约束保留。
  v75预登记`D:/Work/devx015-host-terminal-origin-v75`且确认不存在，重跑5项及原终态回归；
  XML `outputs/validation_runtime/devx015-host-terminal-origin-20260914-v75.xml`。
  无额外注册表键、原store修改或新authority；证据保留后治理清理。
- v75原4370终态1FAIL/7PASS、27.83s；XML SHA
  e116fb1490252b4d43b3b1ce2710f214d6e3f1283d4fad4682d33c4ced47b787。
  已从可信旧origin读到真实终态lease，但原intent校验按新host作用域重算旧manifest，触发
  CHECKOUT_LEASE_INTENT_BINDING。同一目标接缝的第二层缺陷，非新API/夹具失败；原源码/测试保留。
  只在RELEASED且原manifest不匹配当前作用域时，从精确已登记旧origin再次核验同一完整lease，
  才恢复其原intent路径命名空间；ACTIVE不走此分支，普通新host lease仍按当前作用域验证。
  v76预登记`D:/Work/devx015-host-terminal-origin-v76`，新增root物理替换、intent修改拒绝以及
  对旧ACTIVE的直接guard拒绝断言，重跑7项和3项原终态回归；路径已核验不存在。
  XML `outputs/validation_runtime/devx015-host-terminal-origin-20260914-v76.xml`；原目标、证据保留/
  治理清理边界不变；56映射/10mutants/全部后续验收不因此视为完成。
- v76原50197终态10PASS/33.88s；XML SHA
  e57c999196e98143dac507f53ad7bf62eadc4cf779e445d1c5c0dd988a2250cb。
  同一真实失败终态事务切换后原样重放；新host账本仍为空，原store/事件/事务/intent/注册状态
  bytes未改。未登记、retirement缺失、损坏账本、旧ACTIVE、root替换、intent修改均拒绝；
  原终态崩溃恢复/篡改收据/错actor三回归通过。此证据限真实失败事务，不冒充成功Full/发布。
  三个生产模块Ruff/strict mypy(scoped follow-imports silent)及显式路径diff检查通过。
  原源码和测试保留v76根；checkout_guard SHA d519ba8fc8f2f8f45464aecce901ef0fe79d78e52a44bbf49d5c1fcea0ab1375；
  publication SHA 61af371709b0ec4ddb12bb6a3b27a687d4f8c3ce8e69d3d882f1225f84dcd105；
  coordination SHA b862c389217ec1158157287c89ada342fba1b1027e0753abd4fbb0df954cfc77。
  下一步回到管理员可信注册、旧binary OS封锁、持久化切换与公开恢复入口；这项历史追溯修复
  不代替完整迁移、L03、最终C全部tiers/Full/发布或OPS080新合法daily接受。

### 2026-09-14：L03 host 切换临界区实施范围

- Owner再次确认推进DEVX-015和OPS-080到全部独立验收完成，不能以未发布状态汇报收口。
- 旧DEVX-014目录锁迁移不是L03的host迁移。先补齐实际host切换将复用的临界区：
  从可信注册确定旧/新root，按固定顺序持有原有arbiter；锁内复核物理身份、注册、policy、
  完整lease replay、所有ACTIVE及未终态execution，并观察冻结Job/进程的实际OS状态。
  TTL不构成排空；退出/异常必须释放实际持有的全部锁，不新增业务事件或第二权威。
- 该临界区只供后续管理员安装/切换/恢复实现使用，不自行写HKLM、ACL、phase或激活迁移。
  管理员注册、旧binary OS封锁、持久化切换与恢复、公开CLI和完整L03实证仍是必需交付。
  原host store仍有活动租约，禁止用它执行迁移；两项HKCU残留清理权限未解决前不新增原生注册表夹具。
- v71隔离验证预登记：`D:/Work/devx015-host-cutover-custody-v71`；DEVX-015拥有，
  用途为实际旧/新arbiter竞争、排空拒绝与异常释放；保留原始XML及源码后按既有生命周期规则清理。
  XML：`outputs/validation_runtime/devx015-host-cutover-custody-20260914-v71.xml`。
  不把新增API缺失、夹具错误或锁临界区测试记作有效baseline red或完整L03验收。
- v71原11800终态：7PASS/1FAIL、21.14s；XML SHA
  cba63e3caeaa34ef3841d3d543310ef013fcc761c2cf05af53418b5d427ec088。
  五项真实旧/新arbiter竞争/异常释放及两项原排空只读回归通过；原事件和状态bytes保持不变。
  父死子活夹具将Job成员数错误固定为1，实际3，尚未进入目标拒绝断言，分类INVALID非产品red。
  原源码/测试已保留v71根。修正为父/子各自OS身份、独立进程handle与实际Job membership，
  明确释放父后证明父已退出而子仍活，随后原探测拒绝；释放子后重新探测可排空。
  v72只重跑此受改用例，路径`D:/Work/devx015-host-cutover-custody-v72`已核验不存在；
  XML `outputs/validation_runtime/devx015-host-cutover-custody-20260914-v72.xml`。
  同一任务/证据保留及治理清理边界；不重复七项不变PASS，不增加完整L03映射。
- v72原68453终态1FAIL/37.40s；XML SHA
  45ffcb079c687e90d062c572e760a5cd4f13eda44e3ae69140424881d435f1e7。
  实际job.log为ModuleNotFoundError：夹具切换cwd后相对PYTHONPATH=src失效，未触达目标，
  分类INVALID启动，非产品red。原测试已保留；在原隔离cwd以绝对源码路径核验导入与OS身份
  成功后，仅冻结夹具PYTHONPATH为本轮实际src绝对路径；生产源码不变。
  v73预登记`D:/Work/devx015-host-cutover-custody-v73`且确认不存在，仅重跑同一父子用例；
  XML `outputs/validation_runtime/devx015-host-cutover-custody-20260914-v73.xml`。
  保留证据并按同一治理退出条件清理，不新增注册表键或修改真实主机配置。
- v73原命令终态1PASS/8.13s；XML SHA
  8d785988959400c36bc886d8049a83916e5a2889516e63fdef48d97201a96309。
  实际父/子各自记录pid+creation_time，独立native handles验证同一Job membership；父释放退出后
  子仍活，原OS排空探测拒绝；子释放并实际退出后，原探测返回EMPTY/0与EXITED。
  源码SHA ee76232a3ad3ab349e55a1eb7875ec266e9745f2c5579a8634211e9ba8132d84；
  测试SHA 568074702e4415d2454ab721d160b7629ea5b3306ea99901ce9e26262ce4a9da；均保留v73根。
  生产源码Ruff/strict mypy(scoped follow-imports silent)及显式路径diff检查通过。
  这6项新增用例和2项原回归是切换临界区/OS探测的针对性证据，不是完整L03或管理员安装验收；
  56项映射缺口、统一counters/10mutants、最终C必需tiers/Full、迁移发布及OPS080独立验收仍待完成。

## 1. 授权与交付边界

- task：`DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2`；P0；`IN_PROGRESS`。
- Owner 在完整外部审阅和具体授权问题后明确回复：批准在 DEVX-015 内实施这次完整工程合同修订。
- decision：`owner_decision:DEVX-015:2026-09-11:complete_workflow_contract_revision_v3`。
- 本次允许：归属明确的受审受控 merge commit、共享协调与排空迁移、固定候选验证与短发布分离、
  有限恢复动作，以及以下实证验收。该授权不是自动接受敏感合同语义的许可。
- 保持：不 force-push、不 rebase/cherry-pick、不改写旧历史、不合入无关提交、不复制历史
  canonical 事件冒充当前权威、不重标旧 PASS；production、active shadow、broker/order/fill
  和研究/DQ/PIT 边界不变。远端分歧修复和唯一源码销毁仍须各自适用授权。
- 原 S1-S5、consumer 与 OPS-080 全部要求保留；机制接受、发布切换、OPS-080 工程部署与
  新合法 provider-ready ordinary daily / immutable consumer closure 为独立接受层。

## 2. 精确来源、已有成果与当前工作区

- 初始 current main / 候选 HEAD：`03d10b4a2071ce6b9bbc87e982714471052b1db1`（M）。
- 旧 source lane：`6638bfcfb18397849218b6d67f9c348de6426ef6`（L）。
- 共同 frozen base：`0507e4dd129d2a33cd61479d9226dab7ea3dd5cd`（B）。
- B..L 为 5 个 DEVX-015 源码/修复提交；标题与路径清单只是归属审计输入，不独立证明全部历史可合入。
- 已发布最小准入合同不等于旧 S1 合入。旧四层 PASS、V4 failed Full、V5 未派发 Full、
  后续最小准入正式结果保持各自身份；新 C 必须获得自己的必要验证。
- 复用 `D:/Work/AITradingSystem_devx015_integration` 与现有
  `codex/devx-015-main6498-reconciliation`；不创建替代源码 worktree。
- 首个 source transaction：`devx-015-workflow-contract-source-20260911-v1`，以旧已部署
  fence 的正常 acquire / TASK_SOURCE_PRE_WRITE 记录本次授权和任务状态，未绑定或重标旧拒绝计划。
- 已有 `integration-revalidation-0771066af2b8b3643994` 的 serial decision 保留为真实证据。
  本次开始的是 Owner 已批准的新合同 wave，不将该旧计划伪改为 READY。

## 3. 本次合同修订的首次实施与切换

本 Owner 决定先允许协调者在当前合法 task lane、现有普通 fence 下修改声明过的工程合同和测试。
初次实现仍接受当前 guard 的 source/生成/提交检查。不得先启用未验证的新 guard，再让它证明
自己的授权有效；不得把未修改的旧 gate 输出标成新合同的 PASS。

首次受控整合前必须形成不可变 B/L/M 与逐项 disposition：
`ALREADY_ABSORBED`、`MERGE_REVIEWED`、`KEEP_CURRENT_AUTHORITY`、`REGENERATE` 或
`CONTRACT_SEMANTICS_UNRESOLVED`。校验文件 type/mode、add/modify/delete/rename 和完整历史归属。
对这份 Owner 授权的首轮材料，协调者在相同任务 scope 内审阅并冻结实际合并结果；不得因
路径重叠的旧保守分类再次索取同类授权，也不得借此忽略真实语义冲突。

采用一种整合路线：**受审受控 merge commit**，保留 M 和 L 的父关系，最终 main 仍只
fast-forward 到经验证的候选。必须先证明 L 的全部纳入历史归属本任务；不满足就停止，不能
默默切换为另一套绕过路径的吸收机制。当前 canonical task/events 使用官方 writer 更新；
派生 views/manifests 按当前官方工具重建，旧数组和生成 bytes 不直接复制。实现允许用精确
Git tree/parent 构造可审阅的 merge commit，不要求在活工作区先写入未经审阅的合并内容。

新日常合同只允许已登记任务、精确 source/claims、审阅后的敏感残差、完整验证与明确远端
条件下的同类工程整合，不要求 Owner 重复粘贴机器 hash。实际合同语义取舍、范围扩张、
不归属历史、不可恢复清理和 production/broker 决定仍升级。所有允许动作必须有公开 CLI、
明确输入和原始结果；不可用裸布尔值或自包含 hash 代替当前权威。

## 4. 正交对象与作用域

| 对象 | 必须保存的事实与边界 |
| --- | --- |
| Task authority | 当前 canonical task、范围、授权版本；历史不能复活当前取消、终态或损坏任务。 |
| Source checkpoint | 请求路径原 bytes、操作和完整性；不提供 task writer、生成、Full 或 publication 权限。 |
| Candidate C | 精确 commit/tree、来源、生成器与输入；验证期间执行 checkout 的源码不可变。 |
| Validation V | C、实际 interpreter/dependencies/module paths、policy/inputs/generators/runner/test selection；测试事实与当前采用资格分开。 |
| Publication attempt | 当前 local/remote 条件、短时资源、实际 local-applied/remote-confirmed/closeout；不改写 V。 |
| Recovery | 已知/未知副作用、允许探测和有限恢复出口；资源释放不等于任务成功。 |

路径写入资源为 `(checkout_identity, normalized_path)`；HEAD/index/commit 为 checkout
专属短操作；local main 为 `(repo_common_identity, refs/heads/main)`；Full 槽为真实
host 作用域，同一受管参与集合使用同一实际 store；远端为 exact endpoint/ref，不由本地锁
假冒跨主机互斥。复用既有 `FileExecutionLeaseStore` 与 queue，不另建 scheduler/第二权威。

迁移先注册可信 control root 与身份，排空受影响旧 writer 的租约、真实执行者及子进程，
阻止旧入口重新写旧 store；不复制 ACTIVE，不以 TTL 抢活体执行锁。明确短时 arbiter lock
与覆盖副作用生命周期的执行保护的区别。只在 actual Windows/Python 3.11 的实证范围声明能力。

main 前进不抹去 V(C)。先检查现 main 与 C 的真实 ancestry/语义，再决定是否需要 C2；
C2 或验证身份变化不得使用不匹配 PASS。发布只在稳定对外边界要求 ref/index/worktree 一致；
崩溃中间态不可消费，恢复后再开放。不能承诺三者跨崩溃天然原子。

## 5. 有限实施顺序和退出条件

| 波次 | 必须交付 | 退出/停止 |
| --- | --- | --- |
| W0 | 当前授权事件、精确 scope、冻结测试合同、首次切换合同 | 授权已取得；来源不可读或真实归属不足则停止，不新建替代源码。 |
| W1 | 先回收旧 S1 有效源码；同一候选补齐剩余分类、资源/身份、实际结果与发布恢复接缝 | 剩余源码逐项 disposition 完整；真实敏感冲突 `CONTRACT_SEMANTICS_UNRESOLVED`。 |
| W2 | 有效 red、mutation 反证、真实跨进程与恢复、最终 C 的 required tiers 和实际 Full、迁移/普通发布事实 | `MECHANISM_ACCEPTED` 需证据完整；缺平台/必测证据 `INSUFFICIENT`，不以 focused 代替。 |
| W3 | 重新核验 OPS-080 原始 92 项集合、bytes 保全、该任务自己的最终候选和全部验证 | `OPS080_ENGINEERING_READY`，不是运营完成。 |
| W4 | 既有授权下的部署身份与新的合法 ordinary daily / immutable consumer closure | 所有独立运营 gate 满足才完成 OPS-080。 |

本 wave 为 SINGLE_LANE；共享合同、canonical writer、生成与最终候选由一个协调者持有。
允许独立只读审查和已冻结 scope 的测试协作。不得冻结无关任务或把所有未来平台优化变成前置。
相关实现合在一个自然最终候选验证，不按每个 helper 重复 Full/发布。

## 6. 冻结的实证验收

机器清单：`config/architecture/devx_015_workflow_acceptance.v1.json`。
每个 case 记录 exact baseline/SUT/test 身份、初态、公开入口、时序/barrier、fault point、
独立最终状态、允许读写、实际副作用计数、原始证据和 seed。所有下表证据目前均 `NOT_EXECUTED`。

| Case | 必测内容与独立最终断言 |
| --- | --- |
| I01 | 同 blob/type/mode、双方删除、全部已吸收；剩余源码为空，不重复应用、不新增无意义合同波次。 |
| I02 | main 增量与旧有效规则保留；消费行为满足冻结预期，不能用文本包含替代语义证明。 |
| I03 | 不同 hunk、跨文件依赖、rename/modify、modify/delete、binary；精确 tree 与操作无遗漏重复。 |
| I04 | 文本冲突及 clean merge 的同/跨文件合同矛盾、删除合同声明后重封 hash；拒绝发布。 |
| I05 | 前置修复合入后回到同一任务剩余源码并抵达终态；不产生替代源码 worktree。 |
| I06 | 全部准入入口的精确 canonical identity；前缀误命中、旧 Markdown、损坏/终态/错候选/source-only 拒绝。 |
| S01 | 固定增改删集合、staged/unstaged、CRLF/binary/empty、预算和捕获中漂移；原 bytes/操作正确，HEAD/index/config 不改。 |
| S02 | 未归属、排除、secret、路径 alias/case、junction/reparse 及检查后替换；合成 canary 的越权读写为零。 |
| L01 | linked worktrees、独立 PID、同一实际 store 分别竞争发布和 Full；持有者/执行者峰值至多一，输家无副作用。 |
| L02 | A acquire 后实际写脏，再 B acquire/write，交错 release 与完整 preflight；不相交合法编辑均完成，index 短操作互斥。 |
| L03 | old/new writer、TTL/活锁、父死子活、锁文件替换、store alias、迁移崩溃和旧入口重启；无双 authority/ACTIVE 复制。 |
| V01 | main 在验证前、中、结果落盘前变化；通过实际 runner 保存原 V(C)，发布资格另判。 |
| V02 | C2、env/dependency/loaded module、inputs/policy/generator/runner/selection 变化；pytest PASS 但正式 closure 无效不得发布。 |
| V03 | plan/task/candidate/receipt/event 篡改并重封 hash、删除 claims、validate 后替换；外部冻结身份拒绝。 |
| P01 | main 条件检查后竞态、ref/index/worktree 崩溃；不覆盖新 main，稳定边界一致，恢复前不可消费。 |
| P02 | actual bare remote 分歧、push 成功 ACK 丢失、确认前远端继续前进；不 force、不盲重试，未知记 REMOTE_UNKNOWN 并提供探测出口。 |
| R01 | 同请求串行/并发重试、同 ID 不同完整 payload；一个副作用执行者，其余重放/观察，不只返回同 binding。 |
| R02 | 每个持久化边界前后崩溃；含 terminal event→receipt、claim→launch、子进程结束→父记录；新进程实际恢复到终态，唯一源码不删。 |
| X01 | S→generators→outputs→C→V；漏/乱序/旧输出/只换manifest、旧canonical事件、自包含hash均拒绝。 |
| X02 | 路径、transaction、main、并发同 sequence、ref ABA、PID reuse；按对象权威定义当前值或历史变化要求。 |
| X03 | Windows/Python3.11 实际版本、父子进程/xdist、句柄继承、启动失败、进程复用；只在确有证据时释放执行权。 |
| X04 | 有限队列的等待者、后来者、取消队首、失效请求；释放后 eligible waiter 可前进，不把互斥当公平性。 |
| X05 | fixture 真实通过 repo origin/sentinel/公开 CLI 身份检查；不得 monkeypatch 整个身份门禁，不可因新API缺失冒充 red。 |

修复前 red 必须触达旧版本目标缺陷；import/fixture/API 不存在是 `INVALID`，不是有效 red。
故障测试用小型真实进程，不嵌套整仓 Full。真实项目 Full 仅在最终候选边界执行。
独立 oracle 可以复用 Git/SHA/严格解析，不使用 SUT classifier/PASS 自证。

必杀错误实现：overlap 一律拒绝、一律放行、删除 candidate/task 绑定、退回每 checkout store、
忽略正式 closure/pytest FAIL、恢复时盲重派发、漏删除/改写原 bytes、只信自报 hash/receipt、
偷偷 deselect 必测项、把本地 origin/main 当真实远端。必须由目标断言捕获，不能用任意异常算杀死。

新增 mandatory cases 的 missing/skip/xfail/deselect 均不能接受；存量合法 skip 单列。
`PASS` 表示有效 case 满足其冻结预期（负例正确拒绝也可 PASS）；`FAIL` 表示有效执行违反断言；
`INSUFFICIENT` 表示证据缺失；`INVALID` 表示身份/fixture/目标触达不成立。
故障可恢复时必须实际走到终态；`UNKNOWN` 必须列允许探测和恢复出口，不能永久停车。

每个 case 记录 source_capture/candidate/full_claim/full_launch/publication_update/push/retry/
recovery_action、max_concurrent_holders/executors、extra_owner_approval、replacement_source_worktree、
forbidden_read/write。正常同一验证身份只派发一次 Full；必要 C2 的新 Full 不算重复缺陷。
禁止读写=0、额外人工批准=0、替代源码 worktree=0；liveness 仅在有限竞争、main 稳定且资源释放后断言。

不承诺无限 main 更新下立即发布、任意崩溃 exactly-once、未测平台/网络文件系统/所有 clone-host
全局互斥或任意合同的自动语义等价；不扩建通用语义合并器、分布式平台或全仓 hash 例外体系。

## 7. 保留、审计与当前执行事实

工作区退出条件仍为任务验证/发布、唯一内容与证据保全、无真实执行或运营依赖、受治理清理。
旧源码、失败事务与验证文件不覆盖。未取得唯一证据保全与恢复证明前不删除目录。

- 2026-09-11：当前 SINGLE_LANE preflight PASS，HEAD/main=03d10b4，dirty=[]、active lease=[]。
- source transaction 已按当前 fence 正常建立，官方 canonical writer 追加本次授权进度，任务仍 IN_PROGRESS。
- 首次 acquire 参数重复声明自动加入的 validation resource，在 lease acquire 前拒绝；transaction 未建立，
  移除重复声明后正常取得事务。不把参数错误描述为机制修复或业务副作用。
- 审计事件：此前两次 bare git status 未带已登记排除 pathspec，输出为空，未读取/hash/复制/修改
  排除文件内容；随后官方 audit 带完整排除集合 PASS。后续一律使用官方 audit 或完整精确排除。
- 本文是已批准合同与待执行验收，不预填实现、测试、迁移、发布或运营 PASS。
- 第一批真实 baseline red：生产 fence 与 exact03d10b4 无差异时，12 项 R01/R02 测试
  全部在目标断言失败，errors=0、skip=0、pytest=22.07s、外层最终退出码1。
  9项完整请求字段变化未拒绝；真实子进程在 terminal event 后、receipt 前以17退出，
  新 PID 恢复被旧代码拒绝；篡改 receipt 和错误 actor 的终态重放亦未拒绝。
  XML `outputs/validation_runtime/devx015-workflow-v3-baseline-r01-r02-20260911.xml`，
  SHA256 `cff9d9ef16b4834bb6cf2f947121cdaa576eee9f9f17e88906010d7f1fdbf9a4`。
  外层进程退出晚于 pytest summary，已确认真实句柄结束后才修改 SUT；不把输出完成等同于进程结束。
- 本批修复后完整 fence 测试文件 `19 passed / 40.52s`，外层退出码0，Ruff PASS。
  XML `outputs/validation_runtime/devx015-workflow-v3-fence-recovery-focused-20260911-v1.xml`。
  完整请求字段与绑定内容检查、终态 actor/receipt 验真及从已封存 terminal event 恢复回执已实现；
  同批回收旧 S1 guard/fence 的 source-only 防升级和严格 intent 绑定，以及 synthetic intent
  篡改测试的可恢复清理。这里只覆盖这批接缝，不是 23 组验收、S1-S5 或 OPS-080 完成。
- 旧 L 的 checkpoint module/CLI/policy 与两份测试已按审阅结果恢复，未复制旧 canonical 或派生文件。
  staged 的四项 baseline 在原 checkpoint 实现的明确拒绝点失败（不是缺 API）；XML
  `outputs/validation_runtime/devx015-workflow-v3-baseline-s01-staged-20260911.xml`，
  SHA256 `226a274d332abb269573102ccab0c93eb8e6879595a4e24cf32e300720573caa`。
  只取消普通 staged 状态的无关拒绝；冲突/sparse-directory index 仍拒绝，计划后 index 漂移仍拒绝。
  使用私有 index 从 HEAD 加请求 raw worktree bytes 构建快照，原 staged index 逐字保留。
  checkpoint/capability 两测试文件 `69 passed / 586.93s`，外层退出码0、Ruff PASS；XML
  `outputs/validation_runtime/devx015-workflow-v3-checkpoint-focused-20260911-v1.xml`。
  本证据是 focused 回归，旧中断拒绝行为尚未被有限恢复替代，不能计为 R02 全部通过。
- 逐提交与净变化审阅：B..L 五提交在受审范围均归属本任务。净变化 89 路径，原19个源码/测试/skill
  之外70个为 A32/M16/D22，普通100644；中间 AGENTS 插入后撤回，B/L净相同，条款保留在需求历史。
  当前canonical/task事件与DEVX002/DEVX015历史、三个当前skill文件保持当前权威；旧派生索引/内容寻址
  fragments及target seals由当前官方工具再生成。旧三份说明文档仅审阅插入，不覆盖现有主线增量。
- 下一 source transaction 扩展同一任务的模块声明，用于共享 coordination resolver、Windows execution
  containment、受控merge及其测试；旧 source transaction 按原合同终结保留，不改写它的scope/policy hash。
  该source手交不是已完成的publication，旧fence的FAILED/released不代表本实现或Full失败。
- source v2 已正常取得并持续校验 PASS：`devx-015-workflow-contract-source-20260911-v2`，
  lease `lease-5042b203d9dc55a5b82c`，仍为 `TASK_SOURCE_PRE_WRITE`；没有改写旧事务或切换生产。
- X02 真实双进程 release→terminal event 交错先在旧实现产生两个 terminal events，
  replay 明确失败，baseline 外层退出码1。改为同一现有 OS arbiter 下的短原子转换后，
  fence/S2/kernel/arbiter focused `62 passed / 50.80s`，外层退出码0；XML
  `outputs/validation_runtime/devx015-workflow-v3-atomic-fence-focused-20260911-v1.xml`。
- 执行对象新增兼容 v2 lease/event，原无 execution 的 v1 序列化不变；request reserve、
  suspended binding、resume intent、实际进程退出、结果 custody 均写入同一事件权威。
  未确认执行终态时禁止 TTL 自动过期、release/reassign 和同请求重派发。
  lifecycle+旧 kernel 首轮 `14 passed / 10.03s`，外层退出码0；XML
  `outputs/validation_runtime/devx015-workflow-v3-lifecycle-focused-20260911-v1.xml`。
- Windows/Python3.11 实际 Job backend 首轮 `4 failed / 17 passed`，不算旧版本 baseline red：
  两处测试假设（venv redirector PID 与 invalid executable 193/216）已纠正；两处清理
  断言暴露 Job active count=0 先于已观察进程句柄 signaled，已增加真实 Job 成员句柄退出等待。
  修后 backend+lifecycle `23 passed / 14.70s`，外层退出码0，Ruff PASS；XML
  `outputs/validation_runtime/devx015-workflow-v3-execution-lifecycle-20260911-v2.xml`。
  实测包含 suspended 无副作用、launcher os._exit、父先退子仍活、非白名单句柄、精确 PID/FILETIME
  终止与错误身份拒绝、非法 timeout、实际两worker xdist；不是全部 X03 或机制接受。
- 下一步仍为同一候选的共享 host root/旧 writer 排空迁移、当前 canonical 机器授权、受控 merge、
  V(C)/短发布和有限恢复集成及全部冻结验收。helper focused 不替代正式 tiers/Full，
  未执行 migration、main 更新、push、OPS-080 部署或 ordinary daily。
- 恢复新测试使用独立 launcher、真实 os._exit、PID/FILETIME/Job oracle 与 fresh store：
  RESERVED、CONTAINED_SUSPENDED、RESUME_INTENT、RUNNING、EXIT_CONFIRMED、结果落盘后等
  7个崩溃阶段及原正负例共 `10 passed / 13.52s`；XML
  `outputs/validation_runtime/devx015-workflow-v3-lifecycle-recovery-20260911-v3.xml`。
  已确认退出且结果有效时保留原 PASS；未知退出码即使有 PASS 文件也只记 INSUFFICIENT，
  同一请求不重新派发。此处尚非 checkpoint/source capture 和 publication 的全部 R02。
- 结构化 `workflow_task_authority.v1` 已通过官方 writer 冻结在当前 canonical 事件；
  scope `config/architecture/devx_015_merge_scope.v1.json` SHA256
  `7832b294ab25fb0c50d117689b7ea95eb9d94f3a7a782b15adf27b9768c229d2`。
  完整5提交历史涉及182个唯一路径，净变化89；逐提交操作数84/58/56/53/54。
  原阶段记录的89是净变化，不等于完整历史路径集合。当前 registry cycle=530，review_ref=null。
- 新公开 `merge-plan --record` 产生独立 `controlled_merge_plan.v1`，旧 revalidation plan 未改写。
  plan_sha256=`5bf41ad950e005cf0bc9283a13de2a25bfc4253af249584469839360c8bdee1c`；
  `ALREADY_ABSORBED=23 / REGENERATE=41 / KEEP_CURRENT_AUTHORITY=6 / 待具体审阅=19`。
  没有创建 merge commit；最终源结果未冻结，不能执行受控整合。
- 新 regular-file reader 首轮真实 leaf swap 测试在越权读取计数断言失败；修后读取前比较
  原文件身份与实际句柄。一次 scope-drift 测试 fixture 未声明该文件导致 teardown 拒绝，
  已修正 fixture scope。加入真实 canonical/CLI 与 Git type/mode/delete/history 攻击矩阵后，
  组合 `70 passed / 81.50s`，外层退出码0；XML
  `outputs/validation_runtime/devx015-workflow-v3-current-authority-integration-20260911-v3.xml`。
  中间6项旧 canonical 测试因新增consumer后派生inventory过期而失败；经官方refresh更新后再验，
  未跳过 freshness gate，未把该失败计作目标缺陷 baseline red。
- 共享 resolver/per-write epoch gate 与 checkout scoped resource 接线的 legacy 回归
  `42 passed / 51.82s`，外层退出码0；XML
  `outputs/validation_runtime/devx015-workflow-v3-writer-gate-legacy-focused-20260911-v1.xml`。
  目前无 host enrolment/迁移管理实现、无 ACTIVE 切换、无 old binary 写权限封锁实证；
  dispatch/supervised 真实启动入口尚待接线，不能把 legacy 回归描述为 L01/L03 接受。
- 共享控制当前增加 strict JSON/typed inventory、物理 checkout/common identity、同 epoch
  固定 resource markers 与完整 lease policy digest；retirement 必须属于当前 epoch 的登记集合。
  新增13项身份/反例加原lifecycle `23 passed / 14.82s`；与kernel/fence组合
  `55 passed / 51.53s`，外层退出码0，Ruff PASS；XML
  `outputs/validation_runtime/devx015-workflow-v3-control-identity-legacy-20260911-v2.xml`。
  这是临时真实Git/Windows目录 fixture 的控制读取/写门禁实证，不是 host enrollment 或 L03 完成。
- intent 持久化前新增同一短 arbiter 下的迁移门禁，并隔离 workflow repository identity 的
  Git 环境重定向。首轮 `2 failed / 40 passed`：一项是fixture未建legacy目录；另一项确实
  暴露新intent锁忙分支未返回既有BLOCKED语义，已修复。次轮 `1 failed / 41 passed`：
  剩余fixture嵌套Git目录被普通dirty路径检查提前拒绝，未触达目标；将合成control fixture
  放到已排除的该测试runtime域后单独复验，保留两轮失败原始XML，不记作baseline red。
  该唯一修正用例 `1 passed / 10.67s`，外层退出码0；XML
  `outputs/validation_runtime/devx015-workflow-v3-intent-migration-fixture-20260911-v3.xml`。
  其余41项已在v2通过、产品代码未再修改；未把分次结果冒称为同一全套运行。
- 尚未解决的迁移核心：可信host注册/永久tombstone、locator删除或Git失败不能退回legacy、
  old binary OS写入封锁与真实排空、原子管理阶段转换、dispatch/supervised接线；未启用迁移。
  当前进程只读核验 `IsAdministrator=false`；尚未尝试提权或修改系统ACL/注册表。
- 2026-09-11 后续恢复时，source v2 被实时 gate 拒绝为 `PUBLICATION_LEASE_EXPIRED`。
  未在过期租约下继续源码写入；确认 lease replay PASS、execution=null 且无相关Python进程后，
  通过原 fence release/outcome=failed 正常终结，保留旧closeout receipt和测试引用。
  该 FAILED 是未完成source事务的结束，不是Full或业务运行失败。新同范围 source v3
  `devx-015-workflow-contract-source-20260911-v3` 正常取得 `TASK_SOURCE_PRE_WRITE/PASS`，
  lease=`lease-86763fe7873699615e20`，transaction SHA256
  `decbf7aebbef49911e5607fd97c1df9b7100fdd5aac600c97e728accbce5413a`；旧策略/事务均未改写。
- 可信注册读取采用固定64-bit `HKLM\SOFTWARE\AITradingSystem\WorkflowControl` 的
  `RegistrationV1`，不接受环境或per-checkout信任root覆盖。注册绑定physical目录、locator SHA和
  control-state SHA；missing key才是pre-installation，已有key缺value/损坏拒绝。已登记host的
  Git/locator丢失、未登记checkout、重封locator/state均不回退独立legacy store。
  已登记旧root的retirement丢失也拒绝；直接构造未登记裸store无法进入writer body或产生lease events。
  安装必须保护HKLM项和control-state文件，并在同一现有arbiter下更新；该管理员安装尚未实现/执行。
- 只读复审识别并修复裸store旁路及marker大小写/父子声明旁路；普通path claim与所有相交特殊
  resource claim同时保留，不能用一个host字符串吞掉其他path冲突。真实kernel获取正例/冲突负例
  与guard/lifecycle组合 `62 passed / 46.40s`，外层退出码0、Ruff PASS；XML
  `outputs/validation_runtime/devx015-workflow-v3-host-anchor-20260911-v3.xml`。
  前两轮为 `57 passed / 46.21s`、`58 passed / 46.30s`，保留v1/v2 XML。
  新测试仅替换固定HKLM读接口的transport，实际strict parser、Git、Windows目录及lease arbiter仍执行；
  不将其记作真实注册表ACL、管理员安装、old binary封锁、跨进程迁移或完整L01/L03接受。
- 新公开只读 `control-inspect` 已在真实任务仓库运行，结果 `NOT_ENROLLED`、
  `migration_accepted=false`、`mutation_performed=false`。未更改注册表、ACL、main、远端、
  scheduler、OPS080部署或daily。下一步仍需完整迁移管理/执行入口和其余V3验收，不以本轮helper收口。
- 后续加强裸store的零副作用断言，当前实现出现有效red：虽然租约事件未写入，但被拒绝的
  请求已创建替代arbiter目录。单测在该目标断言失败，errors/skip=0，外层退出码1；XML
  `outputs/validation_runtime/devx015-host-root-prewrite-red-20260911.xml`。
  修复为 `atomic` 和普通 `_arbiter` 在创建锁根前先做只读writer准入，取得原OS锁后仍重复检查，
  不以锁外检查替代锁内epoch/phase复核。公开`acquire`及`atomic`拒绝后root均不存在。
  coordination/kernel/checkout/fence组合 `95 passed / 67.87s`、外层退出码0；XML
  `outputs/validation_runtime/devx015-host-root-prewrite-regression-20260911-v1.xml`。
  这是当前实现的副作用修复，不代表未实施的管理员迁移或剩余合同验收通过。
- 已增加公开 `control-drain-inspect --lease-policy <path>`：验证实际repo/host/epoch/policy和
  精确登记旧root/retirement，读取lease replay、head event ids、ACTIVE与未完成execution集合。
  不调用expire/release、不取得writer锁、不创建目录；时间过期的ACTIVE仍为`DRAIN_REQUIRED`。
  即使全部释放，也只给`CLEAR_SNAPSHOT_ONLY / OS_FENCE_EVIDENCE_REQUIRED`，
  `activation_allowed=false`；真实OS入口封锁、实际执行者清单及锁内切换检查仍独立必需。
  两种真实旧账本初态及原coordination组合 `37 passed / 17.91s`，外层退出码0、Ruff PASS；
  XML `outputs/validation_runtime/devx015-control-drain-inspection-20260911-v1.xml`。
  测试逐字比较旧store所有文件并重放其状态，确认不偷过期锁、不修改terminal账本。
  当前真实CLI结果`NOT_ENROLLED / lease_drain_state=UNAVAILABLE / mutation_performed=false`；
  尚未执行管理员安装、OS迁移或任何运营动作。此诊断快照不能作为未来ACTIVATE权限。
- 系统级执行条件只读实测：当前 `IsAdministrator=false`，请求打开HKLM SOFTWARE的创建子项
  权限得到`ACCESS_DENIED`，未创建任何项。已请求管理员级Windows执行环境；该条件阻止系统实证，
  但不阻止现有source事务内继续checkpoint等不依赖提权的源码工作，未把模拟测试降格为OS接受。
- checkpoint有限恢复前提增加create-only/fsync `capture_manifest.json` 与有界 `capture/*.bin`：
  `CAPTURED`事件前逐项保全和复核冻结请求的原bytes、删除操作与路径映射。`object_intent.json`
  在commit-tree前冻结parent/tree/ref/message SHA、时间及author/committer身份；相同意图可生成
  同一个commit OID。成功receipt的证据清单包括新材料，独立验证原请求bytes与实际commit身份。
  原历史成功receipt仍可按旧证据验证；缺少新材料不能授权新恢复，也未开放自动重试/Full/发布。
  专项首轮 `1 failed / 4 passed`：新篡改测试的重封摘要漏末尾换行，先被原摘要门禁拒绝，
  不算目标baseline red；另一个新增断言最初插错测试位置被Ruff识别，已移至中断测试。
  修后专项 `5 passed / 76.30s`，外层退出码0、Ruff PASS；XML
  `outputs/validation_runtime/devx015-checkpoint-durable-capture-20260911-v2.xml`。
  测试包括中断后持久bytes、创建前已有object intent、同参数真实Git同OID和重封receipt后raw
  bundle篡改拒绝。此处仍是恢复前提，尚未实现新进程recover命令或完整R02终态恢复。
- 同一持久捕获候选的 checkpoint/capability 两完整文件回归实际结束：`71 passed / 680.16s`，
  外层退出码0；XML `outputs/validation_runtime/devx015-checkpoint-durable-regression-20260911-v1.xml`。
  回归运行期间未修改受测源码，没有重启或把观察超时当作失败。专项5项包含于本轮，不累加覆盖数。
  当前attempt仍未记录生产进程身份；后续有限恢复必须补齐原执行者和副作用终态的真实验证，
  不能从超时或磁盘文件自行推断退出。这不是完整R02、管理员迁移或最终Full接受。
- 本次状态核对另发生一次未带排除集合的`git status --short`路径清单读取；未打开、散列、复制、
  暂存或修改排除文件内容。独立记录见任务工作区`outputs/devx015-audit-inspection-20260911.md`。
  后续仓库范围状态检查限定官方worktree-audit，直接Git检查仅限完整排除或任务文件允许列表。
- 下一有限恢复切片限定为六阶段均落盘且实际成功释放租约的`recover-terminal`：独立复验原请求、
  事件、原bytes/commit及实际store后，仅create-only写独立恢复回执，保留原receipt/failure/events。
  此路径不需要从PID/超时推断旧写入已结束：所有源码/ref副作用已由原终态证据验证，恢复不执行
  这些动作，也不替换原生产者的回执。早期阶段仍拒绝，须后续真实进程/子进程生命周期接线。
  重复请求必须重放同一独立记录；不同payload/actor、缺失或篡改终态、错误live lease均拒绝。
  本条为已授权R02内的实施分解，不声明该入口或新进程测试已经通过。
- `recover-terminal`已实现：原请求/actor、六阶段链、raw bundle/object intent、commit及真实store
  成功释放lease全部复验后，返回独立恢复记录；不替换原receipt/failure，也不授予额外执行权。
  专项V1实际3PASS/73.72s；V2实际1FAIL/5PASS/119.48s，双PID中的锁忙原被包装成INVALID。
  已改为只对实际`LEASE_ARBITER_BUSY`返回`WAITING_FOR_ARBITER/action=NONE`及同请求再探测出口，
  不称另一个本请求执行者存活、不自动重试；释放后实际重放至同一PASS。
- 独立只读复审识别新恢复writer的截断终态窗口；真实进程写17bytes/fsync后以23退出，
  在“最终文件不得暴露半成品”目标断言实际1FAIL/27.17s，errors/skip=0，非fixture/API假red。
  XML `outputs/validation_runtime/devx015-checkpoint-recovery-partial-write-red-20260911.xml`，
  SHA256 `434ce94c05ac9d22678127013da6d9f6c658864c3e72db107f8078aee9ca71cb`。
  修复为Windows同目录唯一staging完整写入/fsync/读回复核，再经`os.rename`安装不存在的最终名；
  不使用覆盖型replace，其他平台的新安装typed拒绝。参见
  [Python 3.11 os.rename](https://docs.python.org/3.11/library/os.html#os.rename)。
  中断staging只作为非权威诊断原bytes保留，后续显式调用不读取/覆盖/删除它，不形成新执行权威。
- 修后专项V3实际7PASS/157.91s/外层exit0；XML
  `outputs/validation_runtime/devx015-checkpoint-terminal-recovery-20260911-v3.xml`，SHA256
  `d66fe8c1f26e9a72bc5810244af50b73f827ddce2a8d78a414473db24d7b2c22`。
  覆盖真实capture退出后新PID恢复、双PID重放、恢复自身截断后新PID到终态、原失败保留、
  actor/request/live lease/raw bundle及重封终态事件反例。另两项确定性真实OS锁忙及Windows
  既存文件不可覆盖测试实际2PASS/36.81s，XML
  `outputs/validation_runtime/devx015-checkpoint-recovery-atomic-guards-20260911-v1.xml`。
  Ruff及独立修复复审通过；公共历史验证器的两个完整文件回归仍需新结果，不沿用此前71PASS。
  早期capture恢复、producer/子进程生命周期、完整R02/迁移/发布/OPS080接受仍未完成。
- 新恢复实现的checkpoint/capability两完整文件回归已实际结束：`80 passed / 849.57s`，
  外层退出码0，新增9项包含其中，不重复计数。XML
  `outputs/validation_runtime/devx015-checkpoint-terminal-regression-20260911-v1.xml`。
  回归期间未修改受测源码或重启，原进程终态后才记录本结果；此前失败及原71PASS各自保留。
- 早期执行接线的只读审阅与实现核对确认：当前source-only必须声明完整RUNTIME及源码路径，
  额外marker租约与child路径租约冲突；kernel的TTL保护只覆盖自身execution，不可用outer
  marker替代真实路径执行权。下一步使用同一路径租约承载ExecutionLifecycle，由parent
  reserve/Job bind/resume，child经真实进程/Job及精确请求准入后执行捕获主体，parent在实际
  全部退出/result后release及封存RELEASED。当前validation专用request/result须版本化区分
  checkpoint身份，不能伪装candidate validation；旧capture/历史receipt和拒绝边界保持可验证。
  这仅冻结下一实现方向，尚无早期恢复、child准入或对应新进程验收结果。
- checkpoint执行身份已增加独立`workflow_execution_request.v2/TASK_SOURCE_CAPTURE`，绑定真实
  source-only路径lease的manifest/base/intent、checkpoint task/request与scope摘要；不包含
  candidate/validation字段，旧v1分支保持原约束。新增read-only worker准入复验同lease/state/
  完整请求、launcher存活及当前进程实际Job成员关系，兼容venv redirector的受控后代PID。
  返回的witness不是可序列化执行许可；capture主体尚未接入，不能称早期恢复完成。
  首轮测试为1FAIL/1PASS，原因是测试`_until`调用漏参数、尚未触达目标，不算产品baseline red。
  修正后身份专项实际2PASS/10.37s，包含真实guard租约、独立Win32 PID/FILETIME/Job oracle、
  外部进程拒绝、校验型字段混入和非source-only lease拒绝。
- 后续coordination/backend/kernel三个完整文件组合实际71PASS/1FAIL/50.87s，外层exit1；XML
  `outputs/validation_runtime/devx015-checkpoint-execution-identity-regression-20260911-v1.xml`。
  失败是既有`test_actual_two_worker_xdist_is_contained_until_every_worker_exits`的进程树等待超时。
  原嵌套stdout和small.xml证明内部2PASS/0.53s，但不能证明整个Job退出；finally清理后进程清单
  未发现该测试相关残留。原因尚未确定，原失败不重标PASS，须继续核对真实退出链。
  本轮未修改捕获编排、发布、系统注册或OPS080；完整同lease capture/早期恢复仍待交付。
- 原失败测试以相同`-n 16 --dist loadfile`单项复现实际1PASS/7.31s；原三个完整文件在源码
  未改的条件下组合复跑实际72PASS/32.88s、外层exit0，XML分别为
  `outputs/validation_runtime/devx015-xdist-exit-reproduction-20260911-v1.xml`及
  `outputs/validation_runtime/devx015-checkpoint-execution-identity-regression-20260911-v2.xml`。
  没有修改30秒等待阈值或替换为串行验证。原71PASS/1FAIL仍保留为未解释的间歇性超时，
  本次未复现不证明根因消除；后续执行层改动须补足超时前的主进程/Job成员退出诊断。
  system_flow已同步独立checkpoint request/result与read-only worker准入边界，仍未宣称
  capture接线、早期恢复、最终Full或OPS080接受完成。
- 已补充`WindowsJobProcess.wait/terminate`超时前的只读诊断：主进程exit code、实际Job
  active count、保留handle的PID/FILETIME及原始wait status，明确`observation_only=true`。
  顺序读取不是原子退出证明，不修改等待阈值、成功判定、lease或清理行为，也不采集命令行。
  真实“父进程已退出、child仍存活”两个分支独立核验诊断中的child身份与未退出状态；
  backend完整文件实际21PASS/11.67s、外层exit0，Ruff PASS，XML
  `outputs/validation_runtime/devx015-execution-timeout-diagnostics-20260911-v1.xml`。
  此改动只使下一次超时可定位，不构成此前间歇性超时根因修复；capture接线仍是下一实现项。
- 同lease capture接线前先补足实现身份：新计划使用`task_checkpoint_implementation.v2`，
  own files精确绑定checkpoint module/CLI/policy以及workflow_coordination、workflow_execution、
  workflow_contract三个实际依赖；后者承载result的有界文件读取，不能遗漏其代码身份。
  当前执行同时校验三个模块的真实loaded origin及冻结Git blob/live bytes；非当前历史验证
  仍接受原v1精确三文件集合，v1不能用于当前执行或规避新增依赖校验。safe helper集合不改，
  历史Git证据与实时执行准入仍分开。此项本身不是capture子进程接线或早期恢复交付。
- 身份专项在最终三个执行依赖集合下实际18PASS/94.96s、外层exit0，Ruff PASS；XML
  `outputs/validation_runtime/devx015-checkpoint-implementation-v2-20260911-v2.xml`。
  包含真实Git blob/工作字节或新提交drift、错误/缺失loaded origin、精确集合/依赖hash篡改、
  历史v1只读通过且current拒绝，以及真实新进程CLI。独立只读复审确认指定lifecycle路径
  的项目依赖闭包包含于safe helpers加这三个模块；精确文件集合测试使用独立字面量，避免
  与生产常量同错同过。v1兼容目前是历史binding层证据，不冒称完整旧producer回执E2E。
  此前两依赖集合14PASS/78.44s仅保留中间证据；完整checkpoint/capability回归需新运行。
- implementation v2的checkpoint/capability两个完整文件实际93PASS/927.63s、外层exit0；XML
  `outputs/validation_runtime/devx015-checkpoint-implementation-v2-regression-20260911-v1.xml`。
  原执行句柄到终态前保持源码不变、没有重启；该93PASS仅属于接线前版本，不移用为新capture接受。
- `capture`现实际通过同路径lease的ExecutionLifecycle派发固定`capture-worker`，不再由父进程
  执行对象/ref副作用。worker重验真实Job成员、同lease、source/intent/scope及implementation，
  在关键副作用边界再验；父进程确认整个Job退出，核对worker结果/五阶段事件/源快照后记录
  result并释放。新receipt v2绑定执行请求、结果和真实worker观察；独立归档验证拒绝有执行链
  却降级为旧receipt v1。worker观察只用于诊断与绑定，不能作为可重用执行许可。
- 新`record_incomplete_result`仅在LIVE_CONTAINED_HANDLE退出已证实后记录INSUFFICIENT且
  artifact=None，不写缺失/损坏结果、不覆盖既有PASS、不允许重派发；未完成执行仍阻止释放。
  首轮真实CLI加missing/corrupt出口实际3PASS/39.35s；XML
  `outputs/validation_runtime/devx015-supervised-checkpoint-first-20260911-v1.xml`。
  后续真实Job/同源lease专项、真实producer退出后终态恢复与coordination完整文件组合实际
  43PASS/87.86s、外层exit0，XML
  `outputs/validation_runtime/devx015-supervised-checkpoint-identity-20260911-v1.xml`。
- 只读复审发现清理异常可能覆盖原始capture错误；真实worker退出0后将synthetic结果置为FAIL，
  再由真实terminate后注入清理报告异常，目标断言实际1FAIL/27.29s、外层exit1。保留XML
  `outputs/validation_runtime/devx015-checkpoint-primary-failure-red-20260911.xml`。
  修复分别保存primary与各清理操作错误到create-only execution_failure；诊断写入失败不覆盖
  原因，未确认完整执行结果仍不释放。修后与故障测试迁移的12项并行专项正在运行，尚无结果。
- 原CAPTURED/OBJECTS_WRITTEN/REF_CREATED的进程内hook已迁移为synthetic copied worker在
  seed commit前固定的有限插桩；不增加生产开关、任意argv或verifier替身。独立线程核验真实
  PID/FILETIME/Job及已落盘event，再进行main/source变化或释放有界barrier；线程必须收口并
  传播错误。ACQUIRED仍由原parent hook覆盖。早期恢复、系统迁移、最终Full/OPS080尚未交付。
- 修后与实际worker故障迁移专项已结束：12PASS/252.55s、外层exit0，独立XML读回
  tests=12、failures/errors/skipped=0；XML
  `outputs/validation_runtime/devx015-supervised-checkpoint-fault-migration-20260911-v1.xml`。
  包括原错/清理错分离、四阶段main前进、三阶段中断、三类实际source变化及真实commit-tree
  同OID复算。新增parent在成功result入账前严格核对全部五阶段payload、worker观察与请求绑定。
  Ruff和文档允许列表diff check通过；接线后完整checkpoint/capability回归仍需独立新结果。
- 完整回归首次启动发生执行环境偏差：误用系统Python3.14.4，而非合同限定的项目Python3.11.9。
  实际5PASS/27FAIL/63ERROR/94.02s、exit1，95项零skip；原XML保留为
  `outputs/validation_runtime/devx015-supervised-checkpoint-regression-20260911-v1.xml`。
  90项异常均为`LEASE_ARBITER_STATE_INVALID: protocol evidence changed during bounded capture`。
  同一现存源码文件的只读stat/fstat对照在3.14出现st_ctime_ns不一致，在3.11所有被比较字段一致；
  此次不是合同支持平台的有效验收，也不将其计为checkpoint目标缺陷red。未扩展平台支持范围或
  放松文件身份门禁。源码不变，以明确的项目解释器和既定Git隔离环境重跑相同95项，结果写入v2 XML；
  运行尚未终结，不预填PASS，也不覆盖v1失败记录。
- 下一R02切片在既有授权内限定为中断尝试的有限失败终态出口，不自动继续捕获。计划原子安装
  完整attempt v2/request后才acquire；记录实际producer PID/FILETIME，并与当前parent及存在时的
  execution.launcher交叉核验。parent acquire与无lease恢复均在同一现有store arbiter内复核
  精确请求、producer身份及恢复终态，不能以自报摘要或无lease的锁外快照替代排他检查。
  已亡producer且没有lease时只记录无获取事实；有lease时验证原intent/manifest/scope及真实
  execution终态后才正式失败释放。UNKNOWN/活producer/非空Job不释放，不以TTL推定退出。
  保留所有原checkpoint事件、failure、worker result、原bytes及object/ref；独立恢复回执可重放，
  不追加虚构checkpoint成功阶段。execution结果托管PASS与checkpoint完整性仍分开；原完整成功
  或六阶段成功释放应重放或走recover-terminal，不降格。旧无进程身份材料不补造PID。
  本条是实施边界，尚未实现或验证，不能据此声称完整R02或后续partial续作已完成。
- 正确项目环境下完整95项回归实际94PASS/1FAIL/1243.92s、exit1，v2 XML独立保留；唯一失败是
  `test_terminal_recovery_boundaries_preserve_original_failure[CAPTURED]`的旧parent hook未再触达
  已迁入worker的阶段，`DID NOT RAISE RuntimeError`。这是测试接线遗漏，不作为产品目标red。
  修正仅迁移该分支到现有真实worker故障夹具，保留早期恢复拒绝/原失败保留断言，并复验实际
  worker非零退出、INSUFFICIENT托管及同lease释放；RELEASED仍用parent hook。两分支定向验证
  正在运行，不预报结果。未修改生产源码、没有重启原回归，也不将分次结果称作一次全套PASS。
- 上述两个分支修后实际2PASS/54.39s、exit0，Ruff PASS；XML
  `outputs/validation_runtime/devx015-supervised-checkpoint-terminal-fixture-20260911-v1.xml`。
  当前证据为原完整运行94PASS/1FAIL加该唯一测试接线修复后的2PASS，不重标原运行。
  下一步按已冻结边界实现attempt v2与早期有限失败恢复；相关实现稳定后再做完整影响回归。
- attempt v2与`recover-interrupted`现已实现，尚待本切片测试：完整attempt/request由同目录
  staging fsync/读回后Windows create-only rename安装；v2 intent为完整attempt除intent_id的
  SHA256，含producer PID/FILETIME与request SHA，并进入真实lease immutable change_id。
  acquire/reserve均在同store arbiter内复验实际当前producer与原请求，已存在恢复终态不可launch。
  旧attempt v1历史验证保留，不补造其缺失producer；execution request兼容旧32位与新64位intent。
- 新恢复入口先独立观察实际producer；已亡后核对原intent/manifest/scope及存在时的实际
  execution.launcher。只经现有lifecycle确认执行终态后正式failed release，原已释放状态不改。
  独立`interrupted_recovery.json`为INSUFFICIENT且dispatch_allowed=false，原events/failure/
  worker result/raw bytes/ref不修改；结果托管PASS仍不等于checkpoint成功。完整成功优先原验证
  或recover-terminal。回执安装中断可再次安装，原失败记录不覆盖。
- 只读审阅在测试前发现并修正两处接缝：旧execution request正则仅收32位会拒绝新64位intent；
  无lease恢复的历史重放不应受后来无关ACTIVE捕获影响。新写入的潜在原租约检查限定当前workspace
  runtime scope，既有恢复仍按原request/attempt及真实原lease历史独立复验。
  截断可选checkpoint事件仅作为INVALID诊断保全，不能提供成功阶段；有限失败释放以真实
  attempt/intent/store为权威，不因不完整事件永远停在ACTIVE。对应真实崩溃/重放测试正在准备。
  以上不是已测PASS、partial续作、全部R02或系统迁移接受；正式Full与OPS080边界仍不变。
- 本切片与完整coordination组合实际52PASS/178.01s、exit0；XML
  `outputs/validation_runtime/devx015-checkpoint-interrupted-recovery-first-20260911-v1.xml`，SHA256
  `520a6c16e0f258b43e23c84686a05a36a6ea5d50ad4d914fe92c129bec5950ab`，独立读回零failure/error/skip。
  覆盖acquire前/ACTIVE持久化后/ACQUIRED写17bytes后真实producer退出、新PID到INSUFFICIENT；
  CAPTURED后真实launcher终止、独立Win32证明worker退出/Job消失、raw bytes保全与PID篡改拒绝；
  无获取恢复A在后来真实B租约ACTIVE时仍可验证/重放且B不受影响。原请求拒绝重派发。
  测试独立区分venv redirector与实际Python PID，终止前复验FILETIME，最后回收redirector。
  启动前Ruff报告测试脚本字符串一行过长，测试终态后仅格式拆行并Ruff PASS，原诊断不隐藏。
  本结果不是新完整checkpoint/capability回归或全部R02；恢复自身写入中断、其余持久边界与
  完整影响验证仍待推进，不重复按helper派发项目Full。
- 恢复自身写入中断的新增实证单项实际1PASS/26.86s、exit0，Ruff PASS；XML
  `outputs/validation_runtime/devx015-checkpoint-interrupted-recovery-own-write-20260911-v1.xml`。
  真实新恢复PID在原lease已RELEASED后写17bytes/fsync staging并以23退出，最终回执不存在；
  又一新PID实际到达INSUFFICIENT且重放不变，原staging和原证据保留。错误actor和不同完整request
  分别RECOVERY_ACTOR/RECOVERY_REQUEST拒绝，原文件与lease replay零修改。此项扩展已有ACTIVE
  边界测试，不与前52项相加冒充一次完整运行。后续仍需其余execution持久边界和完整影响回归。
- checkpoint执行持久边界新增七项实证实际7PASS/165.50s、exit0；XML
  `outputs/validation_runtime/devx015-checkpoint-interrupted-execution-boundaries-20260911-v2.xml`。
  RESERVED、JOB_CREATED、BOUND、RESUME_INTENT、EXIT_CONFIRMED、RESULT_RECORDED、
  SUCCESS_RELEASED均在真实API边界暂停并令实际producer以29退出，新PID恢复到INSUFFICIENT，
  原字节/ref保全且不重派发。后退出阶段保留execution结果托管PASS，checkpoint仍INSUFFICIENT；
  原成功释放的完整lease事件集合不被改写或追加虚构FAILED。独立Win32观测真实PID/FILETIME、
  Job成员与退出，不使用venv redirector PID代替执行者。Ruff PASS。
  首次筛选关键字误写导致零用例/6.07s，v1 XML作为无效选择记录保留；随后使用精确测试节点，
  不把空运行或本七项冒称完整回归。下一步对稳定实现运行完整checkpoint/capability影响回归。
- 稳定实现的两个完整文件 `test_arch_005_task_checkpoint.py` 与
  `test_arch_005_checkpoint_capability.py` 已在实际Windows/Python3.11、xdist16/loadfile下
  单次运行106PASS/1489.02s，exit0；运行期间相关源码和测试保持不变。独立解析XML确认
  tests=106、failures=0、errors=0、skipped=0；证据
  `outputs/validation_runtime/devx015-checkpoint-interrupted-recovery-regression-20260911-v1.xml`，
  SHA256=`a729c2e9e765d37e082cbdf49c9be4743b9b5b962019205dfc15fb12a83d97e1`。
  此结果是上述两文件完整影响回归，不代表23组机器接受、最终候选Full、迁移、发布或OPS080完成。
  后续整合审查已确认：现有controlled merge仅有计划/审阅冻结，尚缺公开候选构造；
  生成阶段目前仅校验IDs和顺序，候选构造前需精确覆盖reviewed sources、当前canonical writer
  结果和正式generated outputs的完整M→C增改删集合并核验freshness，不把排除目录当纳入授权。
  保留现有事务lane head=M和候选父节点[M,L]的首次切换路线，不引入不归属的中间提交。
- 候选构造前准入发现并复现：冻结review后改动实际源码字节、删除或index executable mode，
  原`merge-validate`仍返回READY。真实canonical writer/普通事务/冻结review测试的baseline为
  1PASS/3FAIL（49.84s），三项均命中DID NOT RAISE，不是API或fixture失败。
  现对所有非generated resolution和额外candidate_sources重新核对当前raw对象/mode；
  保留immutable review，不将它误作当前字节或原子构造许可。补充额外源码变更拒绝后，
  `test_devx015_workflow_integration.py`完整17PASS/112.60s、exit0，Ruff PASS；XML分别为
  `outputs/validation_runtime/devx015-merge-review-source-drift-red-20260911-v1.xml`和
  `outputs/validation_runtime/devx015-merge-review-source-drift-regression-20260911-v1.xml`。
  后续新增路径全集、generated freshness、候选构造及安装恢复仍须完成，本项不替代完整V3接受。
- 审阅后新增源码集合的独立baseline实际1FAIL/17.58s，命中未抛出异常而非fixture失败。
  冻结和重验现共用源码路径筛选，当前nongenerated dirty集合必须精确等于review捕获集合；
  新路径返回`MERGE_REVIEW_SOURCE_SET_CHANGED`。完整integration文件18PASS/129.54s、exit0，
  Ruff PASS；证据`outputs/validation_runtime/devx015-merge-review-source-set-red-20260911-v1.xml`
  与`outputs/validation_runtime/devx015-merge-review-source-set-regression-20260911-v1.xml`。
  此处只闭合源码捕获集合；被单列的canonical/generated目录仍需第二份精确输出闭包和freshness，
  不得借整目录筛除接受未审候选内容，也不据此声称构造/安装恢复或完整V3完成。
- 本轮source事务v3错误声明提交前Atlas，重现已有source/final顺序冲突。复核V2既有决定、
  旧source-v5原始GENERATED_PRE/POST→CANDIDATE_COMMIT_PRE→source-handoff证据后，
  复用已批准分段而不新增Atlas source-anchor模式。v3官方release为FAILED/lease RELEASED，
  原因是声明纠正/行政替代，不是代码或Full失败；未执行生成或Full。证据
  `outputs/validation_runtime/devx015-v3-source-generator-correction-20260911.md`。
  新`devx-015-workflow-contract-source-20260911-v4`已ACQUIRED→TASK_SOURCE_PRE_WRITE及LANE
  PASS；shared/owned paths、required tiers、M全部与v3一致，source generator改为四项
  canonical-task-source→architecture-manifests→report-flow-authority→compatibility-authority。
  首次acquire重复传入工具自动补充的两个资源，命中PATH_DUPLICATE且未取得lease；修正重复参数后
  正常acquire，不修改guard或减少资源。source merge S保留[M,L]；后续final事务在HEAD=S运行Atlas，
  形成generated子提交C=[S]并对C执行全部正式验证。V3要求保留M/L祖先关系，并未禁止这个子提交；
  本文早先“候选父节点[M,L]”限指source merge S，不应被误用来禁止已批准的final分段。
  生成/构造/Full/发布/OPS080仍未完成，不复用任何历史PASS。
- 新公开只读`canonical-input-inspect --task-id ...`识别当前官方canonical输出集合，并以外部M
  核验旧任务集合/稳定路径、无关fragment不变、本任务旧events完整前缀、governance周期前缀、
  templates/manifest/policy seal不变；未知fragment文件按元数据拒绝，不读其内容。
  实际仓库调用为VALIDATED_CANONICAL_INPUTS，1073 output paths/4 dependency paths；
  materialization_allowed=false，未冻结对象、未生成或授予提交权限。
  三项真实canonical夹具测试3PASS/22.10s；完整integration运行21PASS/1FAIL/151.44s，
  唯一失败是新增历史篡改测试误假设genesis为LEGACY_IMPORT，属于未触达目标的fixture错误。
  修正为实际TASK_REGISTERED并重算完整event链、fragment/index/view后，严格当前validator仍PASS，
  新M历史边界正确拒绝；该单项1PASS/11.66s。三次XML为
  `devx015-canonical-merge-output-inventory-20260911-v1.xml`、
  `devx015-canonical-merge-output-regression-20260911-v1.xml`、
  `devx015-canonical-merge-history-resealed-20260911-v1.xml`（均在outputs/validation_runtime）。
  不将分次结果冒称单次22PASS；源码其余生成器闭包、冻结对象与受控构造仍待接线。
- 新公开`architecture-input-inspect`只读重算五项官方architecture输出：原fitness validator覆盖
  module/test/aggregate freshness和dependency；额外核对已保存fitness、deprecation与实际重算值，
  并比较执行前后输出/policy字节，不能凭旧fitness PASS代替当前输出。
  首次合成夹具缺少实际deprecation target且未声明新增fixture路径，4FAIL/4teardown ERROR
  （26.76s）在目标前置失败，v1 XML保留，不视为有效red；未修改生产门禁。夹具改为完整
  lifecycle policy下的一个实际synthetic target并补齐claims，四项4PASS/28.07s、exit0、Ruff PASS，
  v2 XML为`outputs/validation_runtime/devx015-architecture-merge-output-inventory-20260911-v2.xml`。
  实际仓库公开CLI得到`WORKFLOW_MERGE_ARCHITECTURE_INPUTS_STALE_OR_INVALID`、exit1：
  本轮源码对应产物尚未重建，未将它们冒认为fresh或修改正式生成结果。原失败fixture证据保留，
  不以新fixture成功覆盖旧teardown错误。完整候选闭包和最终生成/验证仍未完成。
- 报告流新增公开只读`report-input-inspect`，官方builder(write=False)/validator重算精确输出与
  render parity，未知fragment按元数据拒绝且不打开；M已登记的旧fragment仅在真实Git blob字节
  未变时作为retained_main_paths保留，不冒充本次生成输出。输入/输出/保留文件及名称集合前后复核。
  初次3FAIL/21.87s是新增实现混用Path与str排序；修正后3PASS/21.94s。新增两个main历史分支
  首跑3PASS/2FAIL/34.38s，暴露Windows长路径下git show的revision/path歧义；改用ls-tree精确
  type/mode/OID再cat-file blob读取，不改变Git配置。最终五项单次5PASS/34.27s、exit0、Ruff PASS，
  XML为`outputs/validation_runtime/devx015-report-output-inventory-20260911-v4.xml`；v1/v2/v3
  原始结果保留，不把这些开发失败冒充旧实现baseline red。实际仓库只读CLI因system_flow源seal
  尚未刷新返回RCF_SOURCE_SEAL_DRIFT/exit1，没有写入或删除生成物。compatibility精确闭包、
  对象冻结、受控构造及最终验证/迁移/OPS080仍待完成。
- 兼容性新增公开只读`compatibility-input-inspect`：核对官方builder全部index entries的fragment
  SHA，而非仅latest；M的section顺序前缀与sealed legacy/policy不可更换。未知文件按元数据拒绝，
  M旧路径若仍存在必须保持真实blob字节；完整validator与前后字节/集合重验通过也不授予删除或
  materialization权限，source语义审阅仍单列。首次fixture遗漏publication policy触发的DEVX009
  及D/TRADING2542C依赖，实际4setup ERROR/9.06s、exit1，v1 XML保留且不计baseline red。
  修正为真实五段C/D/S5/TRADING2542C/DEVX009、当前canonical合法登记两个依赖task、真实RCF与
  compatibility builders；只为固定source声明列表提供inert fixture bytes，不替换身份门禁。
  四项实际4PASS/36.55s、exit0，v2 XML为
  `outputs/validation_runtime/devx015-compatibility-output-inventory-20260911-v2.xml`。
  包括正常五段全集、非末尾fragment陈旧、未知文件不打开不删除、旧M路径被篡改拒绝。测试前
  Ruff曾拒绝两条长字面量，修正后才启动pytest；当前Ruff PASS。真实公开CLI返回
  COMPATIBILITY_OUTPUT_SET_CHANGED/exit1：只读复核expected/main/actual各27，unknown=0，
  missing新输出=24、尚存待替换M旧输出=24；不能误称发现未知文件或完成生成。补强os.open
  canary后整份`test_devx015_workflow_integration.py`单次35PASS/233.44s、exit0，独立XML读回
  failures/errors/skipped均0；XML为
  `outputs/validation_runtime/devx015-generated-output-integration-regression-20260911-v1.xml`。
  本次闭合四类读取器与既有merge review的文件级影响回归，不等于完整候选生成输入闭包、
  冻结/构造/恢复、全套V3接受或Full；尚无真实生成、删除、候选commit或发布。
- 下一实施接缝沿用已批准S[M,L]分段：完整source request在既有publication lease上绑定当前
  canonical authority、immutable review、M/L、transaction、实现/环境与精确源码；采用同一
  ExecutionLifecycle/Windows Job执行官方source生成序列与私有index/object构造，不新增锁或调度器。
  新执行请求`workflow_execution_request.v3`专指`CONTROLLED_SOURCE_CANDIDATE`，绑定
  source_head_sha、source_request_sha256、source_transaction_sha256、task_authority_sha256、
  review_sha256及既有manifest/environment/lease/host/argv字段；不伪造尚不存在的candidate SHA或
  validation identity。worker结果专用schema，不可当正式验证或publication receipt。
  canonical writer/consumer refresh仍位于TASK_SOURCE_PRE_WRITE，随后GENERATED_PRE覆盖其原始
  执行证据，按source事务声明顺序继续官方生成。源/生成输入、输出全集与操作闭包均须前后复核；
  仅current freshness和自报success不能构造候选。worker只写精确private tree/commit S，不安装
  真实HEAD/index/main；同请求不盲重派发，实际退出与结果custody仍经现有lifecycle。
  安装是后续独立短操作与恢复状态，未安装不可进入FORMAL_VALIDATION_PRE。此条冻结实施边界，
  并非已经实现、执行或验收。
- source worker的独立v3身份与结果托管已实现；完整coordination文件实际43PASS/27.24s、exit0，
  XML `outputs/validation_runtime/devx015-source-worker-v3-lifecycle-20260911-v2.xml`，独立读回
  failures/errors/skipped均0。真实Job后代、同lease/request与PID/FILETIME均须匹配，source head
  必须等于lease base；任何source-only capability资源均拒绝。结果不含candidate/validation身份，
  worker准入仍不替代publication fence。此处尚未接入官方生成、私有S构造或真实HEAD安装。
- 当前尚未冻结review，已纠正原scope将V2需求和本任务canonical fragment固定为M的问题：V2
  仅新增V3指针，仍须逐项语义审阅；当前任务fragment由官方writer生成。原scope逐字归档保留，
  新scope SHA256=`70a40477ae61e23e8d0d20e726aaaf544c72e1b8b74b39dbbe71295482dfda13`经当前
  canonical cycle558绑定。公开新plan=`051e89dea7d6b51d6346b60667e4c88ba906b7113aecb87f9d103c3e58fde959`，
  23 ALREADY_ABSORBED / 20待审 / 4 KEEP_CURRENT_AUTHORITY / 42 REGENERATE；旧scope/plan不改写，
  B/L/M及事务保持原身份。这是声明归类纠正，不是自动批准20项源码残差。
- 报告policy纳入精确输出集合，同时保留输入身份；仅target来源seal字段可由官方流程刷新，
  其他合同字段须匹配M。新增真实builder通过但owner decision改变的反例仍由该历史边界拒绝。
  generated ALREADY_ABSORBED仅保留B/L/M历史事实，review必须result=null，不固定旧M blob。
  修复前真实canonical夹具1FAIL/14.91s命中CURRENT_AUTHORITY_REPLACED目标缺陷，XML
  `outputs/validation_runtime/devx015-absorbed-generated-review-red-20260911-v1.xml`；修后报告及
  generated review组合7PASS/55.91s、exit0，XML
  `outputs/validation_runtime/devx015-source-output-classification-20260911-v1.xml`，Ruff PASS。
  更新后的完整integration文件回归正在执行，不预填PASS；这些检查仍不是完整生成输入闭包、
  构造/安装恢复、最终Full或系统迁移/OPS080接受。
- 上述分类修正后的完整`test_devx015_workflow_integration.py`已单次实际37PASS/257.96s，
  exit0；Windows/Python3.11.9、xdist16/loadfile，期间受测源码和测试未变。独立XML读回
  tests=37、failures/errors/skipped=0，证据
  `outputs/validation_runtime/devx015-source-output-full-regression-20260911-v1.xml`。
  Ruff及任务允许列表diff check通过；此前35PASS和专项7PASS不与本次重复累计。下一步仍为
  完整生成输入/操作闭包、实际官方生成证据、私有S[M,L]及安装恢复，不把整文件回归作为Full。
- 下一实现将四个官方输出读取器组合为精确候选delta捕获：同一source transaction和当前
  canonical review下，当前dirty集合每一项必须属于审阅源码、官方精确输出或经M验证的官方
  obsolete删除集合；不得由generated根目录声明兜底。记录每项M对象、当前raw对象/mode与操作，
  复查完整路径集合、权限、review、HEAD/main及输入输出。该观察入口不授予提交权限；工作进程
  必须另行绑定实际生成输入及执行证据，再消费捕获的原bytes构造S，不能仅依据输出freshness。
- 候选delta公开观察与内存raw捕获已实现：source/current canonical输出、精确事务/order绑定，
  未覆盖路径按名称拒绝，生成mode保持M或新文件100644，report retained保持原对象，compatibility
  obsolete必须已删除；前后复核完整集合/对象/authority/HEAD/main。首5项真实canonical子集
  5PASS/77.61s；新增scan canary复现当前代码缺口，实际1FAIL/18.23s在“未知.py被打开”断言失败，
  XML `outputs/validation_runtime/devx015-candidate-input-read-red-20260911-v1.xml`，非fixture假red。
- 修复先分离canonical结构核验与完整inventory扫描：既有require_inventory_freshness=false
  不再无条件执行扫描，严格默认仍重算；新structure结果显式consumer_inventory_checked=false，
  不能授予执行资格。先复核reviewed source集合及M/已审来源允许的scan名称，再执行全部原freshness。
  修后首轮7FAIL/1PASS/1teardown ERROR/105.32s：ls-tree不支持exclude magic，以及新增fixture
  修改task-source脚本但漏声明该路径；保留v2 XML，不记为原产品baseline red。改用positive roots的
  tree元数据并在内容准入前排除名称，补齐fixture路径声明，未降低项目门禁。
  最终专项8PASS/129.49s、exit0，XML
  `outputs/validation_runtime/devx015-candidate-delta-closure-20260911-v3.xml`，Ruff PASS。
  此8项包括raw/binary/CRLF、source/index mode、未知scan脚本零打开、捕获中实际字节变化、
  结构通过但完整freshness拒绝；仅canonical生成器子集，不冒称四生成器全部组合、原子输入保护、
  实际生成/删除、S构造/恢复、Full或运营接受。完整integration/canonical影响回归仍待新结果。
- 稳定实现的完整integration与canonical cutover两个测试文件已单次80PASS/391.79s、exit0，
  实际Windows/Python3.11.9、xdist16/loadfile，运行期间受测源码/测试未变；独立XML读回
  tests=80、failures/errors/skipped=0。XML
  `outputs/validation_runtime/devx015-candidate-delta-canonical-regression-20260911-v1.xml`。
  Ruff与明确任务路径diff check通过；8项专项包含于80项，不累加成覆盖数。上述四类观察器
  不等于完整实际生成输入/执行保护；下一步仍需source worker生成接线及私有S/安装恢复。
- 生成执行采用限定于已知官方Python生成器的`GeneratedArtifactStage`：消费外部冻结的精确
  Git raw对象/mode清单，受验证同一句柄读取的bytes才交给生成器；官方writer产物留在内存，
  后继读取消费此前生成bytes。精确output/deletion集合和实际读集合须复核，原工作区不安装。
  此为私有S构造中的生成步骤，不是通用虚拟文件系统或任意代码安全沙箱；更高worker仍须绑定
  实现/环境、authority、lease、HEAD/index、输入全集与实际阶段顺序。本阶段不提供提交、
  materialization或publication许可。先以真实canonical生成器和实际I/O故障验证，再接四生成器。
- 内存生成步骤首7项实际7PASS/21.49s，包含真实canonical四输出及原文件/HEAD/index逐字不变。
  随后直接OS写入有效baseline2FAIL/7.26s：真实fixture文件被os.open截断、被os.replace替换。
  加入直接OS拒绝、扫描集合复核与prior新增/删除投影后，v2为9PASS/2FAIL（23.79s），仅错误
  标识名称不匹配；统一DIRECT_IO_DENIED后v3实际11PASS/24.03s。原结果保留，不重标。
- 只读复审与补测再确认metadata不应绕过冻结对象：未知实际文件的exists有效baseline1FAIL/
  7.12s；prior未声明/明确absent四分支及官方strict路径读取内存新文件两分支合计6FAIL/9.36s。
  现prior在进入前核对外部raw对象/absence，物理metadata与完整候选文件namespace/目录前缀核对；
  同路径实际读取按object+origin保存versions，真实canonical旧index与新index不再被覆盖折叠。
  仅两个官方_regular_path为精确内存对象适配，未全局放宽resolve；新内存父目录可扫描但不落盘。
  v4实际18PASS/1FAIL/1teardown ERROR（33.72s）：unknown正确拒绝但错误标识不匹配，以及
  versions fixture新consumer未在租约claim中。分别细分unknown错误、改用已声明且外部expected
  明确纳入的fixture路径，未放松guard。新增缺失父目录两个真实官方reader分支后，v5单次
  21PASS/35.23s、exit0，Windows/Python3.11.9、xdist16/loadfile，Ruff PASS。证据均在
  `outputs/validation_runtime/`：`devx015-generated-artifact-stage-20260911-v1/v2/v3/v4/v5.xml`
  （分别为独立文件）；三个baseline分别为`devx015-generated-stage-direct-os-baseline-20260911-v1.xml`、
  `devx015-generated-stage-metadata-baseline-20260911-v1.xml`及
  `devx015-generated-stage-prior-and-reader-baseline-20260911-v1.xml`。
  此21项不是四生成器全链、私有S、安装恢复、实际Full或运营接受；完整文件影响回归随后执行。
- 稳定实现的三个完整文件integration、canonical cutover、execution已单次122PASS/403.09s，
  exit0；实际Windows/Python3.11.9、xdist16/loadfile，运行期间受测源码/测试未变。独立XML
  tests=122、failures/errors/skipped=0，证据
  `outputs/validation_runtime/devx015-private-generator-stage-full-regression-20260911-v1.xml`。
  Ruff与明确任务路径diff check PASS；21专项包含其中，不与此前80项重复累计。三项缺口的
  定向独立复审通过；下游closure必须消费reads.versions，不把顶层末次读取作为唯一输入。
  首次canonical进度更新被CONSUMER_INVENTORY_STALE拒绝，未绕过；同一合法source事务的
  官方refresh-consumers PASS后，update成功追加cycle565。HEAD/main仍为03d10b4，未构造或
  安装S、未移动ref/发布、未作管理员迁移或OPS080动作。下一步仍为四生成器全量输入与实际
  source Job接线、私有S及独立安装恢复；本122项不替代全部V3/S1-S5、最终Full或运营接受。
- 下一执行接线固定四个官方生成器，不接受任意回调或重排。canonical刷新四输出；architecture
  依官方generate顺序构造五输出并要求fitness PASS；report仅刷新既定五个seal/count字段，使用
  官方splitter、builder和validator，保留M登记且字节未变的旧fragment；compatibility用官方
  write=False确定精确集合，再执行write=True/validate，仅删除M核准的obsolete fragments。
  各步消费同一外部输入namespace及前序实际内存bytes，保存各phase读取versions和输出集合；
  全链结束重新核对所有实际物理读取、metadata/扫描集合及原HEAD/index/main。普通Git历史读取
  必须保存其确切commit/path/对象，不能用未绑定的HEAD或环境重定向替代。此函数执行的是
  官方生成代码，不自行建立task/Job权限，也不安装工作区；其上层仍须source事务及实际Job托管。
- 四生成器实际串联及历史Git读取适配已实现。首次真实正例1FAIL/23.57s：stage内历史M读取
  触发descriptor拒绝；改为进入stage前读取精确M对象，没有放宽通用descriptor权限。正例复验
  1PASS/31.79s。进一步在M读取、物理/虚拟metadata与扫描入口统一应用已冻结排除集合后，
  生成器与stage专项单次29PASS/166.54s、外层exit0；独立XML tests=29且failures/errors/skipped=0，
  `outputs/validation_runtime/devx015-four-source-generators-and-stage-20260911-v1.xml`，SHA256
  `8f3eea9e923de53513081969170341c75ffd697b31c977a1e4dfce878b769a51`。
  运行期间受测源码/测试未变。该结果包含四个真实官方生成器、错序/缺项、未知源码零打开、
  report合同与M obsolete篡改、链尾输入变化、精确历史Git与排除集合反例；不是实际source Job、
  私有S构造/安装、Full、迁移或OPS080接受。下一步仍为上层完整输入namespace与执行托管接线。
- 四生成器稳定代码的三个完整文件execution/integration/canonical cutover回归实际130PASS/
  424.22s、外层exit0，独立XML tests=130且failures/errors/skipped=0。证据
  `outputs/validation_runtime/devx015-four-generators-full-regression-20260911-v2.xml`。
  v1因操作者写错cutover测试文件名导致0tests/exit1，属于INVALID启动，原XML保留；未重标为red
  或PASS。运行期间受测源码/测试未变，29项专项包含其中，不重复累计。此处仍不是V3整体接受。
- 2026-09-12恢复：source v4实时被PUBLICATION_LEASE_EXPIRED拒绝；官方事务/租约replay PASS，
  execution=null且无相关Python执行进程。通过原fence正常release/outcome=failed，lease RELEASED，
  保留130项XML绑定。此FAILED表示过期的未完成source事务终结，不是测试或Full失败。
  同一范围source v5 `devx-015-workflow-contract-source-20260912-v5`正常ACQUIRED→
  TASK_SOURCE_PRE_WRITE与LANE PASS，lease=`lease-3ba3b40ae8d3a0c00304`，transaction SHA256
  `acb41ea1f99b1081a086397a5819822672d2142c6ee59a0924ab014c62d8015b`；未改写旧事务/锁/历史。
  `prepare_source_generation`已实现M文件namespace、审阅源码和当前合法canonical输出的外部raw
  身份捕获，并单列M原始对象；未知dirty先拒绝，index隐藏标记拒绝，前后复核authority/index/集合。
  独立协作者在中断前留下5项测试，协调者接手复核并补齐两个index隐藏标记反例。
  v1为4PASS/1FAIL/81.75s：漂移测试未先建立dirty已审源，实际正确命中新增source集合拒绝；
  v2为6PASS/1FAIL/112.36s：修订夹具的CRLF文本先触发既有whitespace gate。两次不是有效
  baseline red，也未降低门禁。该目标用LF文本后，v3单次7PASS/118.97s、外层exit0，独立XML
  tests=7且failures/errors/skipped=0；三个原XML均保留于outputs/validation_runtime/
  `devx015-source-namespace-20260912-v1/v2/v3.xml`（独立文件）。Ruff与明确允许列表diff check PASS。
  本7项包含完整M与raw/binary/CRLF/新增/删除对象、未知generated路径实际open零触达、已审source
  漂移、actor/review身份、assume-unchanged/skip-worktree及工作区/HEAD/index保全。它是输入
  接线的专项验证，不与先前130项重复累计，不证明实际source Job、私有S/安装恢复或整体接受。
- 2026-09-13：`render_source_candidate_delta`接通实际四生成器与最终内存delta，逐项核对输出
  SHA/byte_count/删除全集；reviewed source沿用受审raw mode，generated仅沿用M mode或新100644。
  生成前后重新读取当前namespace/authority/HEAD/index；允许在实际source phase复验，不伪造
  回到TASK_SOURCE_PRE_WRITE。真实四生成器fixture在M前构造architecture基线，后经官方review、
  prepare与GENERATED_REBUILD_PRE进入本函数。三项单次3PASS/124.69s、外层exit0，独立XML
  tests=3且failures/errors/skipped=0，`outputs/validation_runtime/devx015-source-candidate-delta-20260913-v1.xml`。
  父进程`source-candidate`与同Job `source-worker`、私有index/完整tree metadata核验/[M,L]提交
  构造已接线，但真实CLI Job端到端测试尚待执行；本3项不是其PASS。未在实际源仓库派发、安装
  候选或发布。worker输出保护、结果独立采信与全部持久化边界恢复仍须验证，不能从父进程返回
  success或存在Git对象推断整个源事务已接受。
- 仅在自动回收的临时目录做Windows输出保护设计探针：父目录GENERIC_READ/share-read句柄
  允许新child写入，但现有os.replace(child, child)实际被WinError32拒绝；未继续尝试宽松share
  模式或在真实输出使用该方案。探针脚本保留于
  `outputs/architecture/workflow_integration/directory-handle-probe-20260911-v1.py`，临时fixture已
  确认不存在。此失败改变后续实现选择，不是机制接受、产品baseline red或生成副作用证据；
  读取需复用同一个已验证对象，输出保护需另行完成实际正反例，不能由检查后再普通open/replace
  推定原子安全。参考[CreateFileW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)。
- 2026-09-13：真实CLI source Job首轮1PASS/1FAIL、109.58s；失败在共享publication lease的
  guard task与业务task身份混用，reserve门禁拒绝且无worker派发。修正request subject为实际lease
  task，业务task继续由prepared authority与fence独立绑定。第二轮1FAIL/117.59s已实际派发Job，
  worker在生成前发现dirty_paths经JSON落盘由tuple变list，按精确复验拒绝；统一prepared返回list。
  两轮XML `devx015-source-job-20260913-v1/v2.xml` 保留，不作为旧实现baseline red。
  父侧采用另增实际wait收集的Job成员PID/FILETIME对照、固定author/committer/UTC时间的完整Git
  commit bytes核验，以及record_result同次读取SHA绑定。v3中断恢复若尚未独立采用，不得因
  exit0和worker PASS文件自动升级成功，终态为INSUFFICIENT/SOURCE_ADOPTION_INCOMPLETE；
  已托管的成功仅重放。对应实际崩溃用例与第三轮CLI测试正在验证，未预填PASS；安装恢复仍未完成。
- 协调模块整文件回归实际45PASS/34.64s、exit0；独立XML读回45/0失败/0错误/0跳过，
  `outputs/validation_runtime/devx015-source-adoption-recovery-coordination-20260913-v1.xml`，
  SHA256 `ca4f95ba46ad7cbeaf5158843610e7000bf108b7092bcb59faf2f62ff52665e5`。
  覆盖v3已知exit0且结果完整但采信前父崩溃、已托管后父崩溃，以及原v1/v2生命周期；
  同次结果SHA错误拒绝且不改变事件状态。此回归不替代source CLI端到端或安装接受。
- 第三轮真实完整源码fixture CLI通过：1PASS/649.14s、exit0，独立XML为1/0/0/0，
  `outputs/validation_runtime/devx015-source-job-20260913-v3.xml`，SHA256
  `38ec0f3e64e3614b79d3f3b606bd6c34b6820a4eb2b80d5642f73c28220dd0f0`。
  实际Windows Job、source worker、四官方生成器、私有S[M,L]及父侧独立delta/tree/完整commit
  bytes/原生PID-FILETIME采用通过；独立核验raw/CRLF/binary、mode、新增/删除及完整M保留，
  原source/HEAD/index/refs不变，公开同请求重放不再次派发。本PASS只属于该fixture版本。
  随后早期恢复接缝将完整execution/launcher reservation移到首个请求文件之前，并新增
  `source-recover`：仅原进程实际结束并恢复INSUFFICIENT后通过既有fence失败释放，保留私有
  对象；成功只重放，活体/未知不释放。其真实request写入前后崩溃测试待执行，不能复用上项
  PASS证明此新增接缝；真实源仓库安装/发布和全部原验收仍未完成。
- 早期恢复v1两项在freeze前被普通文本CRLF的git diff --check拒绝，2FAIL/133.81s，未触达
  恢复目标；保留XML，不作为产品baseline red。该普通文本fixture改LF，不改原门禁。v2实际
  2PASS/209.18s、exit0，独立XML2/0/0/0，
  `outputs/validation_runtime/devx015-source-early-recovery-20260913-v2.xml`，SHA256
  `6542cb0b4ad06096056825f64d9426c729e225406dab5f0daf6c619d32b0b86f`。
  原生PID/FILETIME证明请求首文件写前/写后原launcher死亡且无Job，再经公开source-recover
  抵达FAILED/lease RELEASED/INSUFFICIENT；错误actor/id拒绝不改事件，同recover和candidate
  重放不派发、不追加事件，原残缺/完整文件及source/HEAD/index/refs均保留。
  输出路径TOCTOU、独立真实候选安装恢复、106变体与10 mutants精确node映射/完整收集和
  证据编排、最终候选正式tiers/Full、真实共享迁移及OPS080仍属本任务未完成接受范围。
- 2026-09-13：一次输出写入接通`write_bound_once`，使用实际Windows root目录身份与逐级
  NtCreateFile RootDirectory句柄；不共享目录write/delete，OPEN_REPARSE_POINT后拒绝reparse，
  leaf FILE_CREATE拒绝覆盖，flush/fsync后关闭全部句柄。fdopen构造失败也显式关闭已转移fd。
  请求、execution request、capture、generation、object intent、worker result均已切换该入口；
  早期崩溃测试改在此实际写入边界注入，不替代身份准入。
  原生文件契约整文件24PASS/37.15s，独立XML24/0/0/0，
  `outputs/validation_runtime/devx015-bound-output-contract-regression-20260913-v1.xml`，SHA256
  `abf04e7bea867f3dd1085c1bb23b8e4583edf64117d9a5867277ac177eed4d27`。
  含实际junction/root替换、独立进程rename和directory write-open被WinError32拒绝、句柄关闭后
  可重命名、fdopen失败无句柄泄漏及别名拒绝；不将此窄输出原语当作任意安装的原子性证明。
  首轮NT调用误用Win32 generic access而遗漏所需SYNCHRONIZE，7项中2FAIL；改用明确NT
  FILE_GENERIC_READ/WRITE后7PASS/5.91s，原v1/v2 XML保留，不是产品baseline red。
- 私有index需要child rename，与上述目录保护冲突；因此改为只在内存以M完整tree metadata
  加已写入核验的delta构造精确tree，不新增临时index。检查Git 2.45.1源码发现mktree即使
  --missing仍查询关联对象type，默认hash-object tree的fsck_finish也读取attributes/modules/
  symlink关联blob，故未运行该mktree草稿。内部构造器严格验证名称、mode/type/OID和重复项，
  按目录追加slash的字节序列排序，目录mode编码40000；要求单一SHA-1、无compat格式。
  当前仅检查input-format输出，独立审查发现Git 2.45.1该输出不显示compat，故还须显式配置
  拒绝，待正在执行的冻结源码回归结束后补入，不把当前4PASS当作该门禁证据。
  以hash-object --literally写内部规范tree（不接受外部raw tree），独立算
  SHA1并保留父侧完整actual ls-tree/commit复核。这不是任意literal对象写入权限或新的源码准入。
  参考[Git hash-object实现](https://raw.githubusercontent.com/git/git/v2.45.1/builtin/hash-object.c)、
  [对象写入实现](https://raw.githubusercontent.com/git/git/v2.45.1/object-file.c)及
  [tree排序和fsck实现](https://raw.githubusercontent.com/git/git/v2.45.1/fsck.c)。
  真实Git专项4PASS/10.68s、exit0，独立XML4/0/0/0，
  `outputs/validation_runtime/devx015-no-index-tree-20260913-v1.xml`，SHA256
  `61b3f2ec3668b1fd876e3468ab8f0684680c1dcabbbce412fe57301aa13dbaba`。
  覆盖file-directory双向转换、raw/mode/symlink/gitlink/UTF-8排序、M空tree保留、关联blob移出
  object store后仍可构造（测试后恢复原对象）、无index/ref变更及非法entry在tree写入前拒绝。
  这证明构造不依赖保留blob内容，不单独证明OS层全部尝试读取计数为零。实际source Job、
  早期恢复与文件契约组合回归正在执行，当前不预填PASS；Git object-store输出路径、真实
  HEAD/index/worktree安装恢复及原全部接受仍需继续完成，Pi/SoL-Pi不启动。
- 安装设计的两个真实Windows临时探针（非接受测试）已执行：
  `outputs/architecture/workflow_integration/directory-relative-rename-probe-20260913-v1.py`
  显示GENERIC_READ或traverse+attributes/share-read同时阻止directory writer、parent rename
  与NtSetInformationFile相对child replace（均WinError32）；仅attributes允许三者，不能采用。
  `outputs/architecture/workflow_integration/directory-inplace-probe-20260913-v1.py`显示在相同
  受保护父目录下，独占原文件句柄write/truncate/flush成功、inode不变，handle disposition
  删除成功。两个fixture均自动回收且确认不存在，无真实checkout内容改动。
  因此下一安装实现应复用当前store/fence/已采信S，在持久intent与恢复隔离下对精确delta
  做同句柄原地写/删除，再核验稳定HEAD/index/worktree边界；不能把探针当作已实现安装，
  也不能承诺跨文件崩溃原子性。参考
  [Windows rename结构](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information)。
- 上述冻结源码组合回归已自然结束，27PASS/871.20s、外层exit0，独立XML27/0/0/0：
  `outputs/validation_runtime/devx015-bound-output-source-job-20260913-v1.xml`，SHA256
  `f2c46969fff2171ee698161534754a104927745313210823ed40c9bd8bfcddb0`。
  覆盖实际Windows完整源码Job/四生成器/无index私有S[M,L]/父侧独立采用、首文件before/after
  两项真实崩溃恢复与文件契约整文件24项；运行期间源码/测试未修改。本PASS不包含随后新增
  的compat显式配置检查，不宣称工作区安装/真实发布或V3整体验收。
- 随后在tree构造前从受控`git config --no-includes --null --list`检测任意大小写的
  extensions.compatobjectformat键并拒绝，另外保留SHA-1格式检查。专项v2为5PASS/1FAIL，
  compat夹具误设repository format-v0，Git先报v1-only extension而未触达目标typed拒绝；
  保留该XML，不作baseline red。夹具设置真实format-v1并先确认实际配置可读后，v3实际
  6PASS/11.39s、exit0，独立XML6/0/0/0，
  `outputs/validation_runtime/devx015-no-index-tree-20260913-v3.xml`，SHA256
  `4f118dcad71724c59ccd052a1d7e3e2eaaf3d77d2e2d1750eb01f231c8909765`。
  包含原4项完整tree行为及新增SHA-256/compat两项在ls-tree/对象写入前拒绝；不是把27与6
  合称同版本33项。当前无运行中的pytest/Job，Ruff及精确任务路径diff check PASS。
  下一实际实现仍为受保护的checkout安装/崩溃恢复及source-final handoff，继而完成原全部
  DEVX-015、OPS080接受与发布；不以本阶段检查点结束目标。
- 同句柄现有文件变更原语`apply_bound_file`已接通实际Windows native打开：在打开后、首次读取前
  核对device/file id和非reparse/单hardlink，精确before bytes，原地write/truncate/fsync/readback
  或handle disposition删除。原生整文件34PASS/38.43s、exit0，独立XML34/0/0/0，
  `outputs/validation_runtime/devx015-bound-inplace-contract-20260913-v2.xml`，SHA256
  `b97e0000954a486836b1c07dd40060a19a99faf394f3fa9df49e499aed46f01a`。
  v1为33PASS/1FAIL，参数化夹具重命名清理目标冲突，功能断言已通过；修正清理后复验，原XML保留。
- 安装托管复用原source租约事件链：`lease_execution.v2`只追加受控v4安装/恢复attempt，原source
  已采用结果不可改写。失败或未终态安装继续占有资源，TTL/release/expire/新竞争者不能抢占；
  显式RECOVER必须绑定原plan/candidate/environment及前次终态失败摘要，不重派发旧请求。
  当前两个真实Windows Job正例/失败后恢复专项2PASS/11.29s、exit0，独立XML2/0/0/0，
  `outputs/validation_runtime/devx015-installation-lifecycle-targeted-20260913-v4.xml`，SHA256
  `874a4a3cffae695807d6c4cde0b1b41ce5449b3037c0d26b889a81d54518e257`。
  前三次夹具问题分别为Job名称不合法、子进程相对PYTHONPATH加载旧包、竞争actor未在allowlist；
  已修正测试身份/环境，不降低生产门禁，原失败XML保留，不作为有效baseline red。
  该验证只证明执行托管；真实checkout安装与独立stable-state采用、公开恢复和source-final交接
  尚未完成。当前正在进行coordination/native两个完整文件回归，不预填PASS；两任务完整目标不变。
- 上述两个完整文件首轮81PASS/46.60s、exit0，XML
  `outputs/validation_runtime/devx015-installation-lifecycle-native-regression-20260913-v5.xml`，SHA256
  `ff97ad2cb3d738cb7eab715411fdbc5abb909cb482105a572c805e63fd7465ad`，独立81/0/0/0。
  随后只读复审发现该版本仍把worker自报PASS/stable_state用于释放判定，没有独立验证实际
  ref/index/worktree；此前PASS不证明此稳定接受缺口闭合，也不是实际安装验收。
  新增真实Job反证在目标断言产生2FAIL/11.59s（DID NOT RAISE），无fixture/import错误，XML
  `outputs/validation_runtime/devx015-installation-self-report-baseline-20260913-v1.xml`，SHA256
  `44fd28e3384a97430b0720ff972c59ad1fb54bd2f6968aa3c224dfca6fde773e`。
  通用record_result现在明确拒绝v4自报PASS，退出已知可记录INSUFFICIENT并继续保护原租约；
  不以新boolean/token绕过实际独立稳定验证。后续必须实现真实安装和该独立采用出口，才能
  声明安装可完成；当前整文件复验进行中，不把fail-closed阶段误写为终态可达接受。
- 该缺口修复后两个完整文件实际81PASS/46.71s、exit0，独立XML81/0/0/0，
  `outputs/validation_runtime/devx015-installation-self-report-regression-20260913-v6.xml`，SHA256
  `78316a9940f04c299bf9990f34e5f248d669d6a4e6382663b45859cad1a16111`。
  运行期间源码/测试未修改，结束后仅测试with语句换行通过Ruff，精确路径diff check PASS。
  证明自报稳定不释放、已知退出INSUFFICIENT继续保护和原失败链显式恢复托管；不证明实际
  checkout稳定接受或完整终态可达。下一步仍接入真实安装/独立稳定采用，不更改两任务完成目标。
- 安装前计划开始接入实际已采信source：`_prepare_source_installation`仅从原lease的source-v3
  RESULT_RECORDED/PASS定位原request/result/generation/capture，重新核对当前准备状态与原
  index摘要；记录实际root/gitdir/common身份、各待改文件原bytes/file identity、目标bytes、
  branch/HEAD/index/ref原状态和S。生成输出及源码capture均对照既有delta OID，待改路径先经过
  declared/exclusions检查；只构造内存计划，不创建安装文件或改变事件/ref/index/worktree。
  linked-worktree的index按实际gitdir解析，不能当作工作根下普通`.git/index`。
  计划须在下一安装reservation中外部绑定后才可持久化/执行；恢复必须使用原计划，不重新
  capture中间态作为before。packed-only分支恢复还必须核对实际解析回M，不能仅删除loose ref。
- 最终index采用精确S的tree metadata编码标准SHA-1 index-v2，stat cache归零、stage0，
  不读取关联blob或worktree，不生成临时index；消费者用真实Git验证mode/OID/排序与tree。
  依据[Git index format](https://git-scm.com/docs/index-format)，此内存编码步骤不是安装授权。
  当前真实Git index专项及完整source Job后计划验证正在运行，已出现待诊断失败，未填PASS；
  原生目录保护与常规child rename不兼容，因此实际安装仍须原计划身份约束的同句柄I/O、
  原store执行托管、独立实际稳定采用和有限恢复，不增加第二协调权威。
- 原验收静态映射复查（不是新执行PASS）：S01的add/modify/delete、staged/unstaged、CRLF/
  binary/empty、capture中漂移及main前进分别有checkpoint直接断言；budget恰好上限只覆盖
  reader，整体max_files/max_total_bytes边界仍需补齐。S01共用`_state`不记录Git config，
  原state不变断言不能单独证明config保全。S02的unowned/excluded/secret目前缺checkpoint
  子进程全过程零越权读写观察；output设备名反例不能替代source大小写别名/冲突；真实junction
  与swap已在reader/generated stage层证明，但未覆盖完整checkpoint/子Git链。
  对应候选测试位于`test_arch_005_task_checkpoint.py`、`test_arch_005_checkpoint_capability.py`、
  `test_devx015_workflow_acceptance.py`和`test_devx015_workflow_execution.py`。后续106项映射需
  保存精确node及这些层级缺口，不能把名称相近的局部测试计为全链接受。
- 首轮index/完整source Job后计划组合为2PASS/11FAIL、676.49s、exit1，XML
  `outputs/validation_runtime/devx015-installation-index-plan-20260913-v1.xml`，SHA256
  `e6f57a3ebbb236401834c187686f039ba4e22e10440089d331446e680f696c3d`。
  9项拒绝反例预期漏写既有MERGE错误码前缀；缺失blob夹具unlink只读Git object被WinError5
  拒绝，改为保留对象的rename/restore；不改变生产门禁，不作为有效baseline red。
  完整source Job及计划内容断言走通后，目录不变断言失败；旧快照取于重放前，现改为紧邻
  计划调用前并输出差异名单，完整用例复跑中，未将该失败重标成PASS。尝试只读重查原fixture时
  它已被测试正常teardown为FAILED，门禁拒绝；没有复活旧事务或再派发旧请求。
- 原生一次创建/原地应用新增可选`expected_root_identity`，检查计划中的原device/file id，
  并在实际打开后核对volume/file id；不再只依赖调用时新采集的root身份。真实root目录在
  计划后被rename并由普通新目录替换，两种创建/应用均在目标写入前拒绝，canary保持不变。
  index12项与原生整文件36项单次48PASS/46.56s、exit0，独立XML48/0/0/0，
  `outputs/validation_runtime/devx015-installation-index-native-20260913-v2.xml`，SHA256
  `3f45cf6328cb270055e6dc18d9fcfa869c500eb760e69020cdce7df3b92a4ece`。
  index正例含实际linked worktree、真实Git ls-files/write-tree和缺失blob metadata保留；
  此编码/原语接受不等于实际index/ref/worktree安装。完整source Job后计划v2仍在运行，
  最终公开安装/独立稳定采用/恢复/source-final交接及全部两任务接受继续推进。
- 完整source Job后安装计划v2已实际结束：1PASS/683.84s、exit0，独立XML1/0/0/0，
  `outputs/validation_runtime/devx015-installation-plan-source-job-20260913-v2.xml`，SHA256
  `a1dcdb0a8fd5cde3c1902785117703e314aea3061d2b57b5d0a8deb95aecbacd`。
  本次读回确认，不再沿用旧running状态；该结果尚未覆盖随后新增安装实现。
- 公开`source-install`、`source-install-worker`、`source-install-recover`已接线原source租约：
  首个完整plan摘要先reserve再持久化，实际Windows Job执行精确文件/index/ref变更，main不移动；
  父侧确认真实成员PID/FILETIME与退出，再独立核验原plan的实际稳定状态，才允许安装结果PASS。
  复用Git既有锁协议阻止普通Git并发写入，不新增workflow store/queue；原租约在失败时继续保护。
  同请求仅重放/观察；新恢复请求绑定原plan与前次终态失败摘要，不重派发source生成。
  当前真实全源码CLI安装验证及coordination整文件回归正在运行，未预填PASS。
- 只读复审发现原恢复草稿仅靠bytes识别torn-write可能覆盖后来替换的inode，已增加原计划文件
  identity核对，未知新建/替换对象保留并报`INSTALLATION_RECOVERY_IDENTITY_UNKNOWN`。
  这不是完整恢复闭合：新建文件的持久创建身份、首plan写入中断、目录/文件转换、全部持久边界
  实证与source-final交接仍待完成；不会把保护性拒绝当作终态可达接受。真实源仓库尚未安装/发布。
- 新接线的coordination整文件回归实际47PASS/40.25s、exit0，独立XML47/0/0/0，
  `outputs/validation_runtime/devx015-public-installation-coordination-20260913-v1.xml`，SHA256
  `e31f50618b309f94877085233a9083dde61a7ba3efc6fee95711c64fe7fe8b66`。
  这仍是执行托管回归，真实安装端到端尚在运行；不会用47PASS替代安装或完整验收。
- 公开安装端到端v1已实际结束：1PASS/700.63s、exit0，独立XML1/0/0/0，
  `outputs/validation_runtime/devx015-public-installation-source-job-20260913-v1.xml`，SHA256
  `1790b3e45adb1249e74da501c03a646c0abade7a3f025cdd58a93e95aa7ae682`。
  真实完整source Job生成S后，公开安装Job改变fixture的原文件/index/任务分支ref；独立Git
  ls-files mode/OID及raw文件与S一致、main保持M，原source托管保留，nested独立采用PASS，
  五个Git锁已移除，同安装请求重放不追加事件。该结果不等于真实项目安装/发布或恢复全验收。
- 随后补入Windows branch.casefold的main准入拒绝，以及全部管理目标/锁路径大小写规范化
  去重；在首lock创建前拒绝`refs/heads/Main`等别名，不依靠后续锁冲突才失败。
- 首plan持久化中断恢复新增纯原计划计算：只有原安装及先前恢复attempt均未绑定process、
  已由真实dead launcher/empty Job收口INSUFFICIENT时，才重算原source/manifest/capture计划，
  完整规范bytes必须匹配原v4预留planSHA。原截断plan保留，恢复副本只接受同一外部摘要；
  副本自身截断时保全原prefix后同句柄CAS补全，未知bytes拒绝。不向fence传旧时间、不复活TTL，
  不把当前partial源码重定义为before。实际新进程截断→公开RECOVER→稳定原状态测试与六项
  main别名零写入检查正在运行，未预填PASS。新建文件identity与其余恢复边界仍须闭合。
- 首plan真实截断及六项namespace检查已单次7PASS/697.35s、exit0，独立XML7/0/0/0，
  `outputs/validation_runtime/devx015-installation-early-plan-recovery-20260913-v1.xml`，SHA256
  `9b0c01e809030ae96b0ccf7ea86d717b8329bed4dc8e947ddf4f97f71b7680fb`。
  真实父进程在计划17bytes落盘后以29退出、Job从未绑定；新公开RECOVER生成同SHA恢复副本并
  到达SOURCE_RESTORED，原partial保留、原raw/index/refs保持原状态，历史为原INSUFFICIENT与
  新独立采用PASS，同请求重放不改变事件。此证据不是全部R02或迁移/正式验证完成。
- 只读复审发现恢复副本的保全文件自身截断会永久拒绝；已改为原完整partial仍在时，按实际
  identity和精确prefix原生CAS补齐保全文件，不递归新增保全链，不覆盖未知bytes。测试扩展为
  原plan截断→恢复副本截断→保全副本截断→新进程公开恢复，当前运行中，未用上项7PASS替代。
- source-final交接的下一实现复核沿用V2:118及本V3已授权行政终结：仅同一task/actor/source/
  installation request且最新独立安装PASS、原S[M,L]/HEAD/index/raw/main稳定时，记录不可变
  handoff证据并经既有fence `release(outcome=failed)`终結source事务；source_handoff PASS与
  publication FAILED/正式验证NOT_EXECUTED分开。后续普通final事务lane_head=S、expected_main=M，
  Atlas生成C[S]并取得自己的全部验证。当前仅冻结接线设计，未执行交接或释放真实source租约。
- 连续三次真实父进程截断后的公开恢复已单次1PASS/690.83s、exit0，独立XML1/0/0/0：
  `outputs/validation_runtime/devx015-installation-recovery-own-writes-20260913-v1.xml`，SHA256
  `cf8695c1e0e124853807f757d1690debda695ce19d85e59f692d1010c1125bae`。
  该结果仅关闭此前新增的恢复自身写入故障，不是全部安装恢复接受。
- `source-final-handoff`已接入公开CLI，绑定原source与最新独立INSTALL结果，重查S父链、
  raw/index/main、clean和Git锁，保存专用证据后走既有FAILED行政释放。正常真实完整链已单次
  1PASS/703.14s，独立XML1/0/0/0，`outputs/validation_runtime/devx015-source-final-handoff-20260913-v1.xml`，
  SHA256 `57eed5bef54863e6ad7b15ff7c8f811bbb4b3deba2043d4193a0a8b09928e3d4`。
  handoff证据自身截断恢复未闭合，当前实现不计入整体完成。原生relative hardlink设计探针显示
  父目录防替换句柄下链接失败32，放宽句柄虽成功却允许父目录替换；已排除该方案，未放松保护。
- 已针对复审修正交接观察竞态：原lease事件、完整状态和publication事件在同一既有arbiter下CAS；
  RELEASED恢复仅验证官方release事件及其ACTIVE前驱、精确reason/intent/handoff引用，补历史终结，
  不再核验或宣称当前checkout稳定。FAILED重放仅允许CANDIDATE_COMMIT_PRE直接前驱；确定性
  handoff绑定两条观察事件。真实heartbeat/释放交错→新CLI历史补齐，与实际正式阶段FAILED拒绝
  两个完整source实例并行验证中；尚无该新版执行PASS。静态复审未发现新增安全问题。
  固定handoff文件截断及marker落盘后heartbeat导致的重试活性仍须关闭，不以安全拒绝代替可恢复终态。
- 两实例交错测试实际2FAIL/819.31s，停在第二次交接的index精确字节校验，尚未到目标交错断言。
  `outputs/validation_runtime/devx015-source-final-handoff-races-20260913-v1.xml`，SHA256
  `0a59bf29b19a8e7313addabc919b62de681f1b19c915cb092ca4e61a7672f103`。独立六例复现为3FAIL/3PASS：
  未暂存`git diff --check`刷新zero-stat index，GIT_OPTIONAL_LOCKS三种设置均不能阻止；cached不刷新。
  仅审计命令增加`-c diff.autoRefreshIndex=false`，不改仓库/用户配置；保留精确excluded检查。
  旧正常交接1PASS未断言交接后的index bytes，不能据其宣称已排除此副作用。
- marker已按原ACTIVE lease event ID命名；heartbeat后的重试写新marker并保留旧证据。
  ACTIVE在双head CAS内复用固定两层prefix/retained native CAS恢复；RELEASED只接受精确完整证据。
  原plan恢复复用同一helper，未放松任意checkout新文件的身份要求。native/guard组合77PASS/63.42s、
  独立XML77/0/0/0，`outputs/validation_runtime/devx015-audit-evidence-native-regression-20260913-v1.xml`，
  SHA256 `3297d1df293517c581f0ec3a5c951b85e1658c30ce06bd4ffb53a5f45b0ae8c4`。
  三个真实完整source链正在运行：原lease/release交错、正式阶段FAILED拒绝、完整marker中断后heartbeat
  再发生marker/retained截断并新CLI恢复。静态复审无新增阻断；未将快速检查或旧PASS替代新版端到端接受。
- 上述组合已终态2PASS/1ERROR、858.52s，独立XML3/0/1/0，
  `outputs/validation_runtime/devx015-handoff-recovery-combined-20260913-v1.xml`，SHA256
  `19557e9bd811feadf3c022cc29e395ac54381d31e2b789fd116ef3fc9eccda37`。
  两个真实交错/后续正式阶段拒绝用例通过；marker崩溃用例缺少small_repository fixture导入，
  属INVALID setup而非产品red。现已补齐依赖，仅该用例单独重跑；尚未宣称整组通过。
- 新建文件create后、身份持久化前的恢复窗口新增原生机制探针，脚本保留于
  `outputs/architecture/workflow_integration/delete-on-close-probe-20260913-v1.py`。
  同任务目的为验证FILE_DELETE_ON_CLOSE创建后能否以EX21/Flags8清除；12个独立实际子进程，
  Windows build26200/Python3.11.9/D卷，涵盖绝对CreateFileW与RootDirectory相对NtCreateFile、
  正常close与os._exit41、无清除/普通FALSE/EX清除。父目录GENERIC_READ/shareREAD不降级。
  无清除和普通FALSE均在退出后删除；EX清除后原bytes/同inode在两种退出下保留。只证明此主机
  API机制，不证明原lease持久化或完整安装恢复。探针自己的唯一TemporaryDirectory已清理，
  无用户文件删除、无真实checkout安装。下一步在原installation attempt追加受真实句柄证明的
  creation identity，持久化后才清除on-close，再写payload；必须补齐前/中/后崩溃及替换inode实证。
  不使用可复制nonce/EA或匹配bytes替代原inode，不新增store；当前仍未接线，R02保持未完成。
- marker连续中断的修正fixture用例已终态1PASS/755.76s、exit0，独立XML1/0/0/0：
  `outputs/validation_runtime/devx015-handoff-marker-crash-recovery-20260913-v1.xml`，SHA256
  `ee5f57d16a5b55b7df965d7b78e69b89069f7088c743783f45a4cd2ad44e7447`。
  完整旧marker中断→真实heartbeat→新marker17bytes中断→retained3bytes中断→新公开CLI恢复，
  保全旧marker及partial，原index bytes不变，精确release引用/摘要/大小及无新事件重放均通过。
- 已加入`create_bound_recoverable_file`原语：相对FILE_CREATE+FILE_DELETE_ON_CLOSE、独占leaf，
  原目录句柄保护不变；只打开已存在父目录，不偷偷制造未记录的目录。回调收到同一真实fd，
  回调正常返回后重验identity/empty、归零指针，再EX21/Flags8清除并写入/fsync/精确读回。
  记录前异常/进程退出撤回leaf；清除后崩溃保留已记录identity的对象。缺失根identity在创建前拒绝。
  原`write_bound_once`行为不改；此原语不授予权限，原lease recorder与安装worker尚未接入。
  七个实际子进程覆盖回调异常/回调前记录崩溃/记录后崩溃/清除后空文件崩溃/partial payload/
  正常成功/回调移动fd指针；四个拒绝场景验证existing leaf、missing parent及错/缺root零新增写入。
  该测试中的观测journal不是lease authority，不将helper测试计作全链R02。
  首轮49PASS/3FAIL均为末尾fixture rename目的地重用，目标原生断言已触达；修正测试后52PASS，
  再补复审指针与root场景，最终54PASS/41.62s、exit0，独立XML54/0/0/0：
  `outputs/validation_runtime/devx015-native-recoverable-create-20260913-v3.xml`，SHA256
  `f35645be9a29c6dcf49b36c15d8f88186ab5e5e37b7319a1affb5ddaa29635f7`。
  下一步仍是原installation attempt的真实identity追加、恢复查询及完整安装故障链，不改变106项验收范围。
- 原installation attempt新增`lease_execution.v3 / created_objects`，旧inner v1仍能严格读取，
  外层v2与原source adoption保持原样。记录仅ACTIVE/RUNNING逐项追加；旧prefix、request/process/
  exit/result不能借追加改写；记录绑定原planSHA、root及file真实identity、相对path、目标摘要及worker。
  `record_created_object`是内部原生回调接缝，不提供外部fd授权CLI；首先要求实际原Job worker，
  独立重验原完整plan，从实际fd取得路径及空regular单链接identity，在既有短arbiter中再核worker、
  fd/root后append。回调返回前原事件写入必须完成。schema/history和实际非Job调用拒绝回归已
  48PASS/37.55s、独立XML48/0/0/0，`outputs/validation_runtime/devx015-creation-record-schema-20260913-v1.xml`，
  SHA256 `ee9306b21055caeb96ed3bb4cc7822f5ffc4e79f51ca92bd87d67d10ba0be0ce`。
- installation worker的新文件创建已接到原生原语及原lease记录；RECOVER按原attempt链查找
  对应root/path的创建identity并验证plan方向和目标摘要，不再只因原before身份为空永久拒绝。
  未知或替换inode仍拒绝。真实完整source→install→handoff用例新增独立初态缺失集合、
  实际文件identity/摘要断言，正在执行，尚未证明新接线成功。当前原语要求已有父目录，
  新父目录和file/directory转换仍须显式闭合，不切回未记录的创建路径掩盖失败。
- 同一隔离原生探针新增`--directories`六个真实子进程：RootDirectory相对创建目录、父目录
  GENERIC_READ/shareREAD不降级，目录FILE_DELETE_ON_CLOSE与EX21/Flags8也呈现相同结果：
  无清除/普通FALSE在close或os._exit41后消失，EX清除后同identity空目录保留。临时目录已清理，
  脚本保留；只证明本机机制，不声明目录lease记录或安装恢复完成。
- 本轮接线复审发现仍须修正的custody缺口：RECOVER在身份核验前对current==desired直接跳过，
  独立稳定校验也只看bytes；因此同内容替换inode可能被接受，虽未越权覆盖也违反身份边界。
  当前真实正例测试不覆盖该反例。执行中源码保持冻结；终态后须把身份核验放到no-op之前，
  并让独立stable adoption核对同一原attempt身份规则，补真实替换inode拒绝与可恢复终态实证。
- 2026-09-13：source-v5于05:32:00 JST到期；05:32:44有上述custody复审文档追加，
  属于过期租约下的文档写入门禁事件，保留事实，不重写时间或TTL。官方refresh拒绝后未继续
  实现写入；旧事务execution=null，05:34:50经官方release成为FAILED/lease RELEASED。
  同task/base/paths/generators/tiers的新source-v6于05:35:12取得，LANE PASS；不是发布或验收。
- 新接线完整source/install测试终态为1FAIL1ERROR/675.23s：真实worker在缺失父目录的
  FILE_OPEN处FileNotFoundError，随后fixture release因LEASE_EXECUTION_NOT_TERMINAL拒绝。
  XML `outputs/validation_runtime/devx015-creation-record-installation-20260913-v1.xml`，
  SHA256 `8fc49e82617c3d8cb1506e66d3368f71eadb9d6efcb101c7d1d85fffeba34c4b`。
  保留pytest-19104原fixture及未闭合attempt，不强行释放。下一步闭合目录记录/恢复与
  身份先于no-op核验，再实际恢复失败fixture并重新执行完整链；两任务整体目标不变。
- 原文件identity核验已移到no-op之前，stable adoption采用同一原attempt解析规则，实际读句柄
  在读取bytes之前验证预期identity，消除先lstat后读造成的替换窗口。focused58PASS/45.76s，
  XML `outputs/validation_runtime/devx015-installation-custody-identity-20260913-v1.xml`，
  SHA256 `776a0e668fd500d3e503aee9a59b7f5dedb30425ebbacbed20c36606fb4fccbc`。
  包含实际同bytes替换零读取拒绝和两个独立verifier接缝；不冒充完整Job恢复反例。
- 原生目录创建/空目录删除已实现；同held-fd记录后EX清除，每层重新打开的父目录句柄可按
  完整身份映射核验；只对精确empty对象删除，非空目录保留未知子项。新增五真实子进程边界
  和非空/替换父目录拒绝，总native61PASS/43.98s，XML
  `outputs/validation_runtime/devx015-native-directory-custody-20260913-v1.xml`，
  SHA256 `729fa202aca17cc1b174949692172257cd28a08bd24e329a9c04afde132956f9`。
  原plan尚未绑定目录，原installation尚未调用目录原语，仍不允许普通mkdir fallback。
- 只读复审固定下一接线：版本化原plan完整冻结获准父目录existing/absent及existing identity；
  旧plan可读但不获得新增目录权限，目录记录有显式kind且不伪造file-content摘要。
  创建浅到深、逐级held-handle核身份；恢复文件后仅对原absent且本链创建目录深到浅空删除。
  file/directory转换仍需单独闭合，不把未知子项当本任务所有物。
- pytest-19104真实旧CLI恢复尝试 `recover-missing-parent-20260913-v1` 被
  `LEASE_EXECUTION_INSTALLATION_RECOVERY_BINDING`拒绝于reserve，未启动恢复worker。
  旧合同要求全部process environment hash与首attempt完全一致；pytest结束后当前shell环境
  不同，尚未完成精确环境核验。保留原fixture/失败attempt，不重写hash/receipt或强行释放。
- 2026-09-13：目录接线已实现：新`controlled_source_installation_plan.v2`冻结获准文件与Git锁
  所需父目录的完整清单、原existing/absent和identity；原v1继续解析且不获得目录创建权限。
  旧未启动plan重建按原摘要选择v2或v1原bytes，不重封旧计划。原inner v3的created_objects
  接受严格`kind=directory`记录，不携带伪file SHA；记录仍来自原Job实际fd、原plan原absent
  路径和同root，回调完成原lease append后才清除delete-on-close。
  worker先验证原态，再浅到深创建和逐级核验实际父句柄；文件/锁创建和写入均传原父身份。
  RECOVER仅补必要Git锁父目录；adoption先核验恢复files/已知目录，移除锁后深到浅删除本链
  原absent空目录，再严格复核目录缺失和实际Git/raw状态。未知子项/替换身份保留拒绝。
- focused113PASS/58.02s、独立XML113/0/0/0/time57.999，
  `outputs/validation_runtime/devx015-directory-plan-record-wiring-20260913-v1.xml`，
  SHA256 `a3fda4d331f237e897271e30e73e94769616dda8f7459511afe63f5027d7950f`。
  只读复审发现v1锁创建的None映射会回退FILE_OPEN_IF；已补显式existing-parent-only，
  缺失父目录零创建拒绝/已有目录正常创建1PASS/6.09s，独立XML1/0/0/0/time6.065，
  `outputs/validation_runtime/devx015-legacy-lock-parent-refusal-20260913-v1.xml`，
  SHA256 `5e46b9393048fa6730460a5ab48d2f7987d1fcfdf19521fc87c5fdde4c7cfe67`。
  113与1不是同一次执行，不合并成新版全验收。
- 新完整source→install→handoff用例已启动，独立断言原缺失目录集合与真实目录记录/identity，
  以及原文件集合/index/refs/手交闭合。运行中冻结SUT/tests，不因等待超时重启。尚无本次
  完整链PASS，file/directory转换、真实lease中断恢复和原106/10mutants等整体验收仍须完成。
- 等待完整链期间执行三个实现模块的strict mypy定向检查（`--follow-imports=silent`）：
  当前201项错误，包含新增目录helper缺少类型注解，以及现有控制流终止/Optional/object收窄等。
  不将其归为无关baseline或跳过；运行中SUT保持冻结，终态后修复并重验，正式tiers仍须通过。
- 只读I01-I06映射核对：24变体只有部分现有断言可复用，尚无原106变体→精确node映射/runner；
  同blob/双方删除、inventory、authority拒绝、实际source树断言不能替代main-superset消费者语义、
  跨文件合同冲突、旧源码批准终态和所有准入入口。历史XML不作为当前SUT或完整矩阵PASS。
- 旧fixture恢复的环境差异已定位到合同接缝：本次请求绑定实际净化后完整environment摘要，
  但跨attempt同时强制等于首安装，pytest临时环境消失后阻断fresh-process recovery。
  后续最小修订须让RECOVER绑定自己的真实environment，保留原source/runtime/plan/candidate与
  previous-attempt摘要；worker须在任何写入前重验实际环境和原runtime，不仅依赖父检查。
  这是工程恢复身份，不允许继承旧环境的validation PASS；不保存含secret的旧环境、也不伪填旧hash。
  目前仅完成诊断，尚未改动该合同或重跑旧fixture。
- W1原20项残差本轮已完成分组只读审阅（未据此写入冻结review）。现存plan
  `outputs/architecture/workflow_integration/plans/051e89dea7d6b51d6346b60667e4c88ba906b7113aecb87f9d103c3e58fde959.json`
  仍为89净项：23 ALREADY_ABSORBED、20 CONTRACT_SEMANTICS_UNRESOLVED、4 KEEP_CURRENT_AUTHORITY、
  42 REGENERATE；不得将旧计划状态伪标为已接受。需要完成的明确接缝如下：
  1. 当前compatibility authority保留OPS081→DEVX015 admission，但缺checkpoint/V3 successor；
     在当前链追加精确closure，不能取L整文件删除现行权威或恢复旧S1永久拒绝partial语义。
  2. refactor-policy、compatibility、report-flow、TRADING2452测试增加同一successor及负例；
     deprecation inventory和catalog计数/hash由最终实际官方生成结果确定，旧L常量不复用。
  3. `test_governed_development_skill.py`补原L真实source-only lease→完整preflight拒绝用例；
     当前mock replay不能替代，且必须保留M的frozen-canonical准入矩阵。
  4. artifact_catalog/runbook当前仍为M，补checkpoint request/attempt/raw bundle/index/create-only
     ref/receipt和当前公开恢复入口；不照抄旧staged拒绝/旧attempt版本/人工释放，保留OPS081。
  5. policy与capability tests当前raw blob等于L；checkpoint CLI保留原plan/capture/validate并
     增加worker/两类恢复；checkpoint tests原测试函数保留、V3 staged/raw行为为批准替代。
     guard/fence/checkpoint核心和named-quality tamper测试已吸收且有后续V3修复，保留当前版。
     V2 requirement及system_flow保留M历史和当前V3；canonical cutover测试当前更强，不能倒退L。
  上述仍须实现缺失项、绑定精确review与最终验证，不需要重复索取同类源码整合授权。
- 新目录完整链终态1PASS/712.83s，`outputs/validation_runtime/devx015-directory-installation-full-chain-20260913-v1.xml`，
  SHA256 `47b94d05ab10027c2d7add0afd49805faf4a67bd7c886eca8301699cb041f568`。
  实际原Job目录/file创建记录与独立缺失集合一致，最终raw/index/refs、handoff及replay通过；
  fixture19109正常清理。该结果发生在下面环境修订之前，不挪作新版完整恢复或Full证据。
- RECOVER跨attempt环境相等约束已移除，原plan/candidate/source/previous-attempt绑定保持；
  每次request环境摘要仍不可变并绑定actual Job。安装worker在写入前新增实际environment和
  原manifest runtime复核。实际Job协调回归新增不同恢复环境正例、仅environment不符的
  真启动句柄拒绝（独立检查其余launch字段全部相同）；当前测试运行中，尚未声明新版PASS。
- 类型修订先将真正抛异常的_fail/_control_fail标为NoReturn，并为目录helper补注解；三个
  模块strict mypy错误从201降到113，仍未通过。余项包括严格JSON object收窄、回调返回类型、
  Optional execution与动态文件接缝类型，不能用ignore/关闭strict代替正式修复。
- 环境协调回归首轮47PASS1FAIL/36.85s：目标环境拒绝断言已通过，后续正例fixture因前一
  被拒绝Job留下create-only stdout而FileExistsError，不是产品目标反例；保存XML
  `devx015-recovery-own-environment-20260913-v1.xml`，SHA256
  `bde50dc8a783543b4f0aade8d4c99a179fc4a5fd61c1cd44fc2ca223b4826998`。
  测试确认被拒绝的suspended child未执行、stdout为空后，将该测试日志改名保留再做正例；
  v2终态48PASS/37.82s，独立48/0/0/0/time37.799，
  `outputs/validation_runtime/devx015-recovery-own-environment-20260913-v2.xml`，SHA256
  `3a2f04b34da5a6777d676dc9720dbb3406cdb05caf44b70096586efd16337866`。
- 真实公开partial-plan恢复用例现切换本次恢复环境并独立核对新environment SHA与历史不同，
  原计划17bytes、恢复plan7bytes、retention3bytes三中断及原字节/index/refs/重放断言保持；
  `devx015-fresh-environment-public-recovery-20260913-v1.xml`正在运行，尚无完整新版恢复结论。
- W1 文档残差已按当前 V3 实现补入 artifact_catalog 与 operations_runbook：记录
  source-only capture bundle、原/private index、attempt/receipt v2、公开 validate 与两种
  原执行恢复入口；保留 OPS-081 既有运行约束，明确 staged 非选择权限、无第二 scheduler、
  不以 source-only PASS 代替集成/Full/发布。仅文档闭合，不扩大源码执行或 OPS 权限；
  fresh-environment 完整恢复测试仍在运行，SUT/tests 保持冻结。
- 后续终态：fresh-environment public recovery **1 PASS / 690.08s**；独立 XML
  tests=1/failures=0/errors=0/skipped=0/time=690.060，SHA256
  `9f23f100c9172d8e3d325c83d350e4c1a795d4ba64924c99dcdcf62fee531e50`。
  该证据覆盖原三次 partial-plan/retention 中断后的不同环境公开恢复；不覆盖另行待做的
  created-file/directory 故障恢复，也不将历史 pytest-19104 失败现场标为已恢复。
- 终态后修复三个 workflow 模块剩余 strict 类型问题：严格 JSON object 形状拒绝、缺失
  execution 的显式拒绝、实际 directory parent identity map、None-return recorder、
  原始函数 Callable 与 SourcePreservation 类型、固定 tree tuple 和变量类型分离。
  未增加 type-ignore 或关闭 strict。三模块 `mypy --follow-imports=silent` 已无错误，
  Ruff check/format PASS；这些修改晚于上述完整恢复 PASS，不混用为同一版本验证。
  `devx015-strict-types-source-contract-20260913-v1.xml` 协调与集成回归正在运行，
  其后仍需原全部验收矩阵、最终 C/正式 Full、迁移/发布及 OPS-080 完整验收。
- 在上述回归运行期间，仅向未被该批次读取或导入的 acceptance 测试模块添加
  `install_creation_crash_fixture` / `observe_creation_crash` / `recover_creation_crash`；
  当前 SUT、已收集测试和共享 fixture 保持不变。故障钩子须在 disposable CLI 的 B/L/M
  冻结前安装，原 worker 的 Job/lease/runtime 门禁不替换；外部控制器核验实际 PID/FILETIME
  与 Job 后才允许 exit47。覆盖设计包括 append 前、append 后 clear 前和创建完成后，
  独立检查 held-fd identity、创建记录、实际缺失/内容与新环境 public RECOVER 原态/重放。
  目前只有 helper 实现及 Ruff/format/diff-check，尚未接线或产生这些故障场景 PASS。
  原协调/集成批次已输出失败标记但仍运行；必须先取得终态 traceback，不能宣称全批通过。
- 该批终态为 **134 PASS / 1 FAIL / 1397.11s**，独立 XML 135/1/0/0/time1397.081，
  `devx015-strict-types-source-contract-20260913-v1.xml` SHA256
  `d5ad716efd5dbbef547773b7d044e7971271fd49fcabbd6843bc095700387e7a`。
  唯一失败是 `test_candidate_delta_closes_review_and_real_canonical_outputs[none-candidate]`
  最后的 index bytes 不变断言；测试自己的 dirty audit 仍调用未禁用 autoRefreshIndex 的
  `git diff`。先新增 CLI 返回后、测试 audit 前的原 index 断言进行单用例归因，尚不提前
  判定是产品还是测试观测副作用。整批不报 PASS；实际 later-phase source/install/handoff
  长链在这批通过，但不替代其余矩阵或最终 Full。
- 单用例归因终态 **1 FAIL / 31.25s**，独立 XML1/1/0/0/time31.226，
  `devx015-candidate-inspection-index-isolation-20260913-v1.xml` SHA256
  `3098fa1c83379515eb0a6610c539d7838006a390faa4ef40afcc14b845afe546`。
  新增的 CLI 返回后 index 不变断言通过，原最终断言仍失败；故修改测试自己的 dirty-audit
  命令为 `git -c diff.autoRefreshIndex=false diff ...`，保留 CLI 后与最终两处断言及完整
  literal exclusions，不改产品实现或放宽期望。修复后的六变体正在重跑。
- 两条真实 created-object 恢复用例现已接线：fixture 明确 `source-job-created-file-after-create`
  与 `source-job-created-directory-after-create`，在 B/L/M 冻结前加入测试 CLI 钩子；原完整
  source Job / S[M,L] / raw/index/refs 校验保留，随后真实 worker 创建完成、原链记录已落盘时
  等待独立 Job/FILETIME 观测，再 exit47；公开 RECOVER 使用新环境并验证完整原态及不重派发。
  8 个精确节点已 collect-only 确认，实际 `-n16 --dist loadfile` 批次
  `devx015-original-job-creation-recovery-20260913-v1.xml` 运行中。此轮不声称另外四个
  before-record/after-record 场景、同字节替换反例或全部106变体已经执行；helper 支持不等于实证。
- 8 节点首轮终态 **6 PASS / 2 FAIL / 140.59s**，独立 XML8/2/0/0/time140.560，
  `devx015-original-job-creation-recovery-20260913-v1.xml` SHA256
  `2e79d9e80ece9504893b4de4015851fdbbe6cd9d57303bc1fed85a3d334b0730`。
  六个 candidate 检查变体通过，保留修正后的前后 index 断言；两条新长链均在注入 CLI 的
  import 阶段失败，未到 source execution request，更未到目标 creation 断言，故是无效
  故障测试启动，不是产品 baseline-red / mutant-kill / 恢复失败实证。
- `current_process_identity` 实际定义于 workflow_execution，不是 workflow_coordination
  的公开模块成员；修正测试钩子的 import。新增六种 kind/phase 启动 smoke，仅使用 stub
  main 检验真实 hook imports / wrapper 装载，不声称 Job 或创建验收；终态6PASS/9.04s，
  独立 XML6/0/0/0/time8.970，`devx015-creation-crash-hook-startup-20260913-v1.xml`
  SHA256 `fcd00f3a26904fc383c97d676c3a7bc2a3ce557e0354922349fa25bfb4bac9a9`。
  随后仅重跑两条实际长链，`devx015-original-job-creation-recovery-20260913-v2.xml`
  正在运行，保持其 SUT/测试/fixture 输入冻结。已通过的六变体不重复派发。
- v2 两条长链终态 **2 FAIL / 93.53s**，独立 XML2/2/0/0/time93.506，SHA256
  `87952ca60b9a961261ba23a1c9511bb0a6e7e7d51d16d240ae4f3da63a13068f`；仍在测试钩子启动
  阶段，实际 CLI 未导入 sys，而 stub main 的全局 sys 掩盖该缺陷。两次均未到目标断言，
  不属于有效产品反证；停止相同启动方式，改为钩子显式导入全部依赖并从 __file__ 派生根。
  启动 smoke 改为复制真实 workflow CLI，以非 __main__ 的 runpy 名称执行其真实 globals
  和 INSTALL hook 装载，但不调用实际 main/worker，不存在合成授权或身份门禁替换。
  新 smoke 终态6PASS/9.03s，独立 XML6/0/0/0/time9.002，
  `devx015-creation-crash-hook-startup-20260913-v2.xml` SHA256
  `7c2e4058365b4c3da35ca812b2c0ee5ce7e1d6b691f3d73c301799dedb2b4f24`。
  经该不同检查方式通过后才启动两条长链 v3，
  `devx015-original-job-creation-recovery-20260913-v3.xml` 当前运行中，仍无 creation 恢复 PASS。
- v3 已终态 **2 PASS / 821.01s**，独立 XML2/0/0/0/time820.986，SHA256
  `3ae1daffb793df8a0d068db652ace07d721f14ad74308b81f336c404aa78a217`。
  实际 file/directory after-create 均由独立控制器核对原 Job/PID/FILETIME 后释放到 exit47，
  原 lease durable creation record / native identity / payload 或空目录一致，新进程 public
  RECOVER 恢复原 source/index/refs/目录状态，原 attempt 不变且 replay 不重派发。不是其他
  四个 before/after-record 场景或全部 R02 / 106 / Full 的通过声明。
- 下一批已实现并 collect-only 核对6个精确节点：file/directory 各 before-record 与
  after-record 两边界，另加 same-bytes replacement 与 unknown-child 两个真实 public
  recovery 拒绝→合法恢复场景。后两者保留注入的外来对象 inode/bytes，检查拒绝发生于目标
  identity/empty-directory 操作而非启动失败、原 refs/index 不动、失败 attempt 留存；测试方
  只移动自己注入的对象到原测试目录保留，必要时放回原 inode，不编辑任何 receipt / lease。
  新 request 绑定失败前驱，再核对完整原态及保留外来证据。此项不声称拒绝期间所有已授权
  恢复写入都为零，也不以 byte equality 代替身份。Ruff/format/diff-check 通过，尚未运行通过。
- 上述六场景现已终态 **6 PASS / 1766.75s**，独立 XML6/0/0/0/time1766.674，
  `outputs/validation_runtime/devx015-creation-boundaries-and-foreign-custody-20260913-v1.xml`，
  SHA256 `5f998c8b279b10a0aa1385280ef2488e0a60eb790e919695a805f0fb8f50d6d8`。
  file/directory before-record、after-record 与两类外来对象 public recovery 均实际到达断言，
  原 request/Job/creation history、恢复原态和保留证据通过；仍不替代全部 R02、106/10 mutants。
- W1 补回原 L 的真实 source-only 租约拒绝用例，并升级为当前 canonical task 的精确终态。
  实际 CheckoutLeaseGuard acquire 后，用伪装 publication 文件分别调用完整 build_result 与
  新进程公开 preflight；拒绝 source-only mutation authority，同时验证 HEAD/all refs/index/
  source/canonical bytes/伪造输入/完整 lease replay 不变，无 scope/reader/validator mock。
  新例与既有 completed-admission 正反矩阵合计 **13 PASS / 180.97s**，XML13/0/0/0/time180.943，
  `outputs/validation_runtime/devx015-w1-real-source-only-admission-20260913-v1.xml`，SHA256
  `52b7cec8cf9ffccd7ee7e9fc98958f84e54dcbe9f7de73eafa883d32ff207683`。原 M frozen-lane 矩阵保留。
- W1 新增 `phase_devx_015_workflow_contract_v3` builder，继承当前 admission（其前驱仍 OPS081），
  不直接导入 L 的旧 checkpoint authority、永久 partial-retry 拒绝或改写既有历史。
  当前 source-only/checkpoint 与 V3 工作流源码形成显式精确集合，包含原 admission 的整个集合；
  保留 source-only 不授予生成/Full/publication、原请求/实际 Job 恢复、独立稳定态、敏感冲突阻断。
  段内明确 `IN_PROGRESS`、不声称 mechanism/deployment/operational acceptance。
  合成 closure 正例与缺失/双删/重复/额外/错 hash/源码漂移负例首轮14PASS/14.39s；随后真实仓库
  predecessor/source 验证发现误含“事务声明预留但文件不存在”的
  `config/architecture/devx_015_workflow_coordination.yaml`。保留真实失败 XML1/1/0/0/time9.971，
  `devx015-w1-workflow-actual-source-inheritance-20260913-v1.xml` SHA256
  `d0989c37f1b4bf033d0a216af39e336037bf00bfb8ac14494c23d4c8699c88dd`。
  已从 source records 与独立预期中移除不存在路径，未创建占位文件；其事务预留声明不被篡改。
  当前真实继承与全部合成回归 **15 PASS / 18.65s**，
  `outputs/validation_runtime/devx015-w1-workflow-source-inheritance-20260913-v2.xml`，
  独立 XML15/0/0/0/time18.630，SHA256
  `a5f883ce748fb93aefe158db635c5c187ad7c15a015dc087d202111b736cecef`。
  尚未生成/安装该段，refactor/RCF/restricted-history 等消费测试接缝和精确 frozen source review
  仍待完成；不能把 builder/局部测试通过当作 W1、最终 C/Full、发布或 OPS080 验收完成。
- W1 消费侧已追加 V3 后继，不移除现有 admission：refactor 的两类历史 mismatch 计算、
  精确唯一 source set/前驱/当前权威识别及 cached current-source 读取均支持 V3，读取时仍
  复核当前文件，保留旧 SHA。TRADING2452 的四条受限路径交集不扩张，针对旧 admission 和
  V3 分别保留有效、错 hash、缺 paths/sources、重复、错误权威、未知后继和额外路径反例。
  精确收集79节点，终态 **79 PASS / 23.65s**，独立 XML79/0/0/0/time23.629，
  `outputs/validation_runtime/devx015-w1-successor-consumer-guards-20260913-v1.xml` SHA256
  `802cf99ff6f4e89e727f1bb283f23d72d5ebe0e81385c17fda30b1a2dc358604`。
  devx006c 的当前末段断言与真实已安装 closure 检查、RCF 的 Composer→OPS081→admission→V3
  次序已接入，尚待实际生成后运行；不能把79个合成/读者边界节点当作已安装新段通过。
- 官方 architecture 只读重算仅报告 module/test/aggregate 三个 stale generated manifest，
  无新增 dependency violation。当前源码实算1227模块、1393测试文件、856 direct writers；
  当前 deprecation surface 与这些新 counts 投影得到
  `arch_004g_deprecation_inventory_3fd57b730010cbcdd376`，当前预期常量据此更新，旧 Wave
  历史常量/生命周期/违规与 removal 门禁均保留。该投影不是生成完成或 fitness PASS。
  官方 RCF splitter 对原当前 bytes 的无损拆分为1373 report /586 catalog /1254 flow，共3213，
  current 计数预期已更新，旧3000历史段保留。仍须最终实际生成/逐 byte SHA/已安装 closure
  复核；当前 RCF catalog/flow 的 exact SHA 预期须在最后文档冻结后更新，不删除该断言。
- 准备执行本轮真实 W1 受控源码整合：在同一 M/L/B 和 source-v6 事务下冻结89项逐条
  disposition、两个原 contract claims 和当前 candidate-source raw objects，20项旧保守冲突
  使用已审阅当前源码而非直接覆盖为 L。最终文档 bytes 与 RCF exact-SHA 断言一同冻结；
  运行期间不修改这些输入，执行事实先记外部 task checkpoint 与不可变原始产物。
  公开 merge-review/source-candidate 只推进受控私有 S[M,L]，后续安装必须独立验证。
  原106/10 mutants、所有机制/正式 tiers/最终 C Full、真实迁移/发布和 OPS080 工程/运营验收
  仍分别待完成；本轮源码整合不声明最终完成，也不启动 Pi/SoL-Pi。
- 真实 W1 review `5352d4dd98be4d3b74b804147a877ea2808e46c90a9fcda5c04d51bd06c112b9`
  已由公开 CLI 冻结89项 disposition/37项当前 source/两个 contract claims。
  首个请求 `devx015-w1-source-20260913-v1` 在准备阶段因已登记私人路径被误识别为
  `CANDIDATE_DELTA_UNCOVERED` 拒绝；无 request 目录、无 reserve/Job，事务 execution=null。
  未读取或纳入私人内容；原不可变审阅保留，修复后必须新审阅，不复用旧 source hash。
  合成 file/directory canary 的有效 red 为2失败，分别命中错误拒绝和目录下降。
  修复在 inventory 的 stat/open/descent 之前应用当前 policy 的精确路径/子树排除，
  默认生成 inventory 与其它未知源码仍严格拒绝。回归 **8 PASS**，独立 XML8/0/0/0/time85.229，
  `outputs/validation_runtime/devx015-excluded-source-scan-20260913-v1.xml`，SHA256
  `1c0ad2cebcdf8d118c8f6ceddea4269f7efb520b4708813775e5344a858f6056`；strict mypy PASS。
  继续推进新实际 S、最终 C 和原整体验收，DEVX-015 与 OPS-080 均不以中途状态结案。
- W1 请求 `devx015-w1-source-20260913-v2` 实际启动 Windows Job，随后在报告生成阶段
  `REPORT_OUTPUT_SET_CHANGED` 失败，未构造/安装私有候选。公开 source-recover 已返回
  `RECOVERED_FAILED`，v6 事务 FAILED/租约 RELEASED，原请求与日志保留，不重派发原请求。
  独立 metadata 审计：真实 fragments717，main tree717，当前 main index192；额外525项
  全部为 main 已保留历史，零未知项。修复 renderer/inspection 的历史名字集合为完整 main tree，
  仍核验普通文件 type/mode 与原始 bytes，未知或篡改项 fail closed。
  新测试首轮3项因 Windows Git show 路径解析失败，属于 INVALID；改用 cat-file 后，保留
  正例确实命中同一缺陷，篡改例亦未到达应有 bytes gate；未知例已正确在更早的未声明门禁
  拒绝，仅测试预期错误，不算该缺陷 red。有效/无效原始 XML 均保留。
  修复后完整四生成器正例、历史保留/篡改/未声明未知/已声明但非 main 未知，以及现有历史
  inspection 矩阵共 **8 PASS / 162.61s**；独立 XML8/0/0/0/time162.589，
  `outputs/validation_runtime/devx015-report-main-history-20260913-v1.xml`，SHA256
  `cdb4e442ec4ce180a5b55d0ca19ac0fb4952fbe84c0121143ab4bc240652c1fd`，strict mypy/Ruff PASS。
  新同范围 source-v7 事务继续，不修改旧事务；文档 splitter 当前1373/586/1256，总3215。
  下一步新冻结审阅与实际候选，不以本8项替代原106/10 mutants、最终 Full 或 OPS080 验收。
- 请求 `devx015-w1-source-20260913-v3` 实际运行至600秒工程等待界限，原 Job 尚有执行者，
  随后托管终止并确认父/子退出；无 generation/result/私有候选或安装。公开 source-recover
  返回 `RECOVERED_FAILED`，v7 FAILED/RELEASED，保留原请求与超时观察，不重派发该请求。
  v8 在同范围正常 acquire/LANE PASS 后补充 worker-only 阶段起止与 UTC 时间观察，
  覆盖 admission、准备、四生成器各阶段、delta 与最终输入复核；不记录原始 bytes，
  不改变生成产物/权威或600秒上限。callback 在 in-memory stage 之外调用，公开父进程不输出
  这些诊断。完整生成器正例、顺序拒绝和真实 delta closure **4 PASS / 122.87s**，
  `outputs/validation_runtime/devx015-worker-phase-observation-20260913-v1.xml`，
  独立 XML4/0/0/0/time122.852，SHA256
  `0050fd3e75ee66548e88b5142ff11f282378e8aea96732d29c3776a01113f605`；Ruff/strict mypy PASS。
  下一实际请求须先新冻结审阅，再用阶段日志定位真实仓库成本；未证明候选、安装或整体验收。
- v4 阶段日志证明输入准备143.057秒、canonical54.858秒、architecture117.183秒、
  report discovery17.855秒/历史预取间隔46.173秒/生成33.596秒、compatibility discovery54.425秒/
  生成105.321秒。四生成动作在 worker 运行约576秒时结束，仍需 stage/final input 完整复核；
  原600秒工程上限再次触发，随后原Job终止并由公开恢复收束 v8 FAILED/RELEASED，无安装。
  根据此次真实10049路径实测，将源码 worker 防挂起预算显式调整为1200秒；不改变安装600秒、
  lease TTL、任何候选/权限/来源/漂移验证或验收标准，超时终止与有限恢复仍保留。
  这是让完整已要求链具备足够有界运行时间，不是删除检查或靠日志认定 PASS；旧600秒失败
  证据与先前诊断决定保留。通用性能优化不是本次验收额外前置。
  有界等待/非法 timeout 拒绝/真实子进程清理回归 **14 PASS / 13.08s**，
  `outputs/validation_runtime/devx015-source-budget-custody-20260913-v1.xml`，
  独立 XML14/0/0/0/time13.057，SHA256
  `936d67d1e9b307f9c049b03b9e2f4de3367a41c4eae39b0650a3335c5cdd3f1d`；
  原阶段日志 SHA256 `3936328c44464269455f30a533702098ba10812beb791f142e7d8a294ad5bb8e`。
- v5 实际完成四生成器与最终输入复核，但紧凑 `generation.json` 为20,844,072 bytes，
  在 worker 结果写入前被通用16 MiB读取预算拒绝。公开恢复返回 RECOVERED_FAILED，
  v9 FAILED/RELEASED；保留 generation、capture 与 object intent，可能已有私有 Git 对象，
  不宣称候选收养、安装或发布。v10 在同范围正常 acquire/LANE PASS 后修复聚合预算：
  仅固定 generation.json 使用64 MiB上限，编码后、写入与私有对象构造前拒绝超限；
  worker复读与原编码bytes精确比较，父进程独立重算，安装/恢复读入绑定预期SHA。
  原生 regular-file/身份/共享模式保护与普通文件16 MiB预算不变，不删减生成器证据。
  真实四生成器与delta闭包、17 MiB往返/普通reader拒绝/错误SHA拒绝、64 MiB边界前后
  及reader超限矩阵共 **7 PASS**；独立XML7/0/0/0/time94.979，
  `outputs/validation_runtime/devx015-generation-manifest-budget-20260913-v1.xml`，SHA256
  `00d206f0a9ff79d1992aac7d6869003a22920e7e86017ec44726b8b516d8f6b9`。
  编码边界用例证明helper无文件副作用，实际worker调用顺序另由实现复核；不冒充整个worker
  注入测试或原106项整体验收。下一步新冻结审阅、真实候选与安装，最终Full/发布和OPS080
  整体验收仍须完成，Pi/SoL-Pi仍禁止。
- 真实 source v6 已由父进程独立重算、Git tree/commit/Job身份核验并记录 PASS；S 为
  `cc63488f2682a6247669e38e92d692ea58f6d2a9`，精确父提交为本节 M/L。
  installation v1 实际返回 SOURCE_INSTALLED，HEAD=S、main=M；source-final-handoff PASS
  并通过既有行政 FAILED 路径释放 source v10，不是安装或 Full 失败，也不是发布完成。
  安装后真实报告/目录/流程渲染 bytes、精确 SHA、entry count、freshness、兼容 successor
  **5 PASS / 15.87s**，XML `outputs/validation_runtime/devx015-installed-source-authority-20260913-v1.xml`，
  独立5/0/0/0/time15.853，SHA `6c19d4301082aa8e6b7c14a7a01e2c3bc5f7ac971abb5a129db9aca398385e54`。
  官方 worktree-audit PASS/dirty_paths=[]；普通 final-v1 事务在已安装S上获取，LANE PASS，
  当前 TASK_SOURCE_PRE_WRITE，canonical cycle611。此后修改须重新生成/验证最终C。
  四个DEVX015测试文件实际 collect-only 收集281个唯一node，exit0/0.87s，没有执行测试；
  不能将281或历史focused通过数替代原106变体。下一步逐项核对断言并绑定精确node、测试blob
  和runner身份，完整收集/执行门禁必须拒绝missing/skip/xfail/deselect，再补齐剩余机制与mutants。
- 新增 `MandatoryAcceptancePlugin`：接收外部必需node集合，记录真实xdist各worker收集清单、
  必需node全部setup/call/teardown报告；集合缺失/重复/worker不一致、deselect、skip、xfail、
  XPASS、阶段不完整/重复以及原pytest失败均禁止PASS，不将原非零exit恢复为成功。
  合法非mandatory存量skip不被误拒绝。该组件不是选例权限；完整106变体映射、精确代码/
  环境身份绑定和正式runner接入仍待完成，不宣称M09已正式执行或整体验收通过。
  8个真实独立小型xdist进程场景通过（正例、missing、deselect、skip、xfail、XPASS、断言
  失败、collect-only），初轮8PASS/19.16s；修正测试脚本字符串格式后再次8PASS/34.56s，
  XML `outputs/validation_runtime/devx015-mandatory-execution-guard-20260913-v2.xml`。
  Ruff PASS；strict mypy当前55个执行器属性声明错误，与HEAD原S源码独立运行结果的错误
  multiset完全一致，无新增错误；这不是mypy PASS，最终必需检查前仍须处理适用类型问题。
- I01 补齐公开 CLI 正例：真实且不同的B→L、B→M提交具有相同源码tree，在当前canonical
  权威和实际租约下运行 merge-plan/merge-validate（不使用planning-only），要求精确
  NO_RESIDUAL_SOURCE、全部source_paths被ALREADY_ABSORBED覆盖；独立Git核对每项type/mode/blob，
  100755/120000/gitlink、双方删除与rename旧端点缺失均明确断言。前后refs、HEAD、index、
  config及fixture文件字节完全相同，未生成review/candidate或重复应用源码；身份门禁不替换。
  新用例加现有分类/重封hash删claims等拒绝场景共7PASS/85.63s，XML
  `outputs/validation_runtime/devx015-i01-public-zero-residual-20260913-v1.xml`，
  独立7/0/0/0/time85.605，SHA `a19ca713d0649c990fb231433df860af87e47121de02d98221d8e6a60c483a86`。
  机器清单新增I01三个变体到该精确node的映射，明确PARTIAL_NOT_ACCEPTANCE_READY，仍保持
  execution_state=NOT_EXECUTED；其余103变体不能被默认为覆盖，最终C的test blob/runner/
  环境冻结与全矩阵执行尚待完成。这是新覆盖用例，不冒充已触达baseline缺陷的red或mutant kill。
- I04 公开CLI补充：实际merge-plan输出经外部独立JSON/SHA算法删除claims或required_acceptance
  后重封，merge-validate仍在PLAN_IDENTITY_OR_CLAIMS拒绝；未篡改但没有冻结review的计划
  在REVIEW_NOT_FROZEN拒绝。三例均独立比较全部refs/index/config、真实租约head事件和fixture
  文件原始bytes，零变化；当前canonical/source权限不替换。3PASS/41.83s，XML
  `outputs/validation_runtime/devx015-i04-public-claims-refusal-20260913-v1.xml`，
  独立3/0/0/0/time41.805，SHA `fe122ca852f256ba014bfcc24e540c2bb4ef825de93927d918a3cf2897583819`。
  实际collect-only核实三个node后，仅将claims/acceptance两个node映射到I04.deleted_claim_rehashed；
  共4/106变体有显式映射、102未映射。未审阅拒绝不冒充text/clean semantic/cross-path冲突
  的独立消费证明，也不代表publication端到端或mutant验收已完成；PARTIAL/NOT_EXECUTED不变。
- I02 新增真实消费向量：在B/L/M冻结前构造可执行合成规则，L保留有效shared变更但仍有
  legacy旧规则，M增加main新规则并移除legacy。分别用独立Python子进程执行L/M Git blob，
  验证二者都不满足最终冻结向量；明确reviewed resolution经当前canonical审阅、真实四生成器
  和delta闭包后，从实际captured候选字节执行，结果同时为VALID_LANE/NEW_MAIN/DENY/DENY。
  独立断言candidate blob/type/100755 mode及原checkout bytes/HEAD/index不变；不是自动语义
  合并器，也不冒充实际安装/发布终态。I02单例初轮PASS；格式修正后加原分类/I01回归
  3PASS/102.26s，XML `outputs/validation_runtime/devx015-i02-real-consumer-20260913-v2.xml`，
  独立3/0/0/0/time102.231，SHA `f01e286ce05e6335ff09f7421692fbabfaafbd6747f2e7ab9f9679b44d6bcf10`。
  Ruff PASS，XML精确node核验后映射I02三个变体；现7/106有显式映射、99未映射，
  PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变，最终C全矩阵及mutants仍待执行。
- I03.binary复用并在当前版本重跑三条已有实质断言链：真实四生成器delta对全部操作的
  before/after/type/mode/独立blob SHA和captured集合核对，二进制NUL/CRLF原样保留；
  无index构树与独立普通Git index构造的tree及递归type/mode/object集合一致，原index/refs不变；
  原始二进制capture和中途真实文件漂移拒绝。3PASS/85.10s，XML
  `outputs/validation_runtime/devx015-i03-binary-capture-oracles-20260913-v1.xml`，
  独立3/0/0/0/time85.069，SHA `8c89eec5949f7e2b8c6ab8d0c44983cd4807f3b8992cf97131f7e3fbd4d32af7`。
  XML精确node对应后只新增I03.binary映射，现8/106映射、98未映射；不把二进制证明扩展成
  rename/modify、modify/delete、disjoint hunks或跨文件消费证明，不复用历史PASS冒充当前执行。
- I03 rename/modify与modify/delete新增实际B/L/M冲突fixture：L重命名并删除另一文件，M修改
  两个原路径；明确冻结rename pair及reviewed deletion后运行真实四生成器/delta闭包。
  独立Git证明源/目标历史关系，候选新端点保留M的精确blob和100755 mode，旧端点及另一个
  已修改文件都是D且不进入captured集合，操作无重复，checkout原bytes/HEAD/index不变。
  首轮因fixture暂存CRLF触发既有trailing-whitespace门禁，1FAIL/1PASS，属于INVALID测试输入，
  不算产品缺陷red；改为合法LF文本，保留同一语义与全部门禁。第二轮2PASS/69.28s，XML
  `outputs/validation_runtime/devx015-i03-reviewed-operations-20260913-v2.xml`，
  独立2/0/0/0/time69.253，SHA `940414ae8a26c912f6095b118049b2aa460e6667e203a2df1a1bc684263f9af3`。
  Ruff PASS，精确node映射I03.rename_modify和modify_delete；现10/106映射、96未映射，
  仍不是自动语义合并器、实际安装/发布终态或最终C验收，PARTIAL/NOT_EXECUTED保持。
- I03.disjoint_hunks/cross_file_dependency补充真实文本与消费证明：B/L/M在不同代码区段及
  另一数据文件有明确差异，实际git merge-file -p返回0且原始输出精确等于冻结预期；独立
  Python分别执行L和M，确认两者均不满足最终向量。明确审阅后的代码经四生成器/delta闭包，
  再实际执行captured代码与保留的M数据，得到LANE_LEFT/MAIN_RIGHT/MAIN_DATA；M数据文件
  不出现覆写操作，候选code blob/100755 mode正确，checkout原bytes/HEAD/index不变。
  新用例与原分类回归2PASS/74.43s，XML
  `outputs/validation_runtime/devx015-i03-disjoint-crossfile-20260913-v1.xml`，
  独立2/0/0/0/time74.405，SHA `50d093736b46ec5ecc5146f6bddd8e56334faeea22f4a48e5c34700968751c3e`。
  Ruff PASS，精确node映射上述两个变体；现12/106映射、94未映射，I03五变体均有映射但
  未完成最终C/test blob/runner/environment绑定与全矩阵执行，PARTIAL/NOT_EXECUTED不变。
- I04三类真实冲突补齐：text fixture的实际git merge-file返回1并输出冲突标记；同文件
  clean-semantic及cross-path fixture的merge-file返回0，但独立子进程证明L/M各自消费成功，
  合并代码与M状态组合后在目标字段new触发KeyError，不能因文本干净视为语义有效。
  当前canonical两路径共享同一冻结contract claim；实际merge-plan给出未解决合同状态，
  merge-validate无planning-only仍在REVIEW_NOT_FROZEN拒绝，refs/HEAD/index/config、租约
  head事件和原文件bytes均不变。该门禁要求明确审阅，不声称自动理解任意程序语义。
  新三例加原分类回归4PASS/48.11s，XML
  `outputs/validation_runtime/devx015-i04-real-conflicts-20260913-v1.xml`，
  独立4/0/0/0/time48.091，SHA `5ea1f9bbea867d556720f9d874b0c71e9935f0b5cf434a3714262f615d3f4238`。
  Ruff PASS，精确node分别映射I04 text_conflict/clean_semantic_conflict/cross_path_same_contract；
  现15/106映射、91未映射，后续publication链/最终C测试身份与全矩阵验收仍须完成。

- 2026-09-13 committed acceptance binding：正式run_validation_tier在DEVX-015 Full
  claim前调用bind_mandatory_acceptance，从C读取manifest和test blobs，绑定mode/blob/SHA256。
  独立固定V3原106变体承诺，要求COMPLETE_REVIEWED且每变体恰好一个非空node映射；
  缺项/重复/替换变体/路径逃逸/非普通或缺失blob/候选漂移拒绝，保留原readiness与provenance。
  当前manifest仍15/106 PARTIAL，真实最终Full必须待补齐，不把synthetic完整映射当断言覆盖。
  实际Git正例和7个拒绝案例，加原8项真实xdist结果guard，共16PASS/38.66s；XML
  `outputs/validation_runtime/devx015-committed-acceptance-binding-20260913-v1.xml`，
  SHA `ad09398dace91876ef6e936ad1d61f6f21060956b4653d44caea68959e4202ec`，Ruff PASS。
  正例明确允许多个变体指向同一测试，仅证明绑定机制；断言充分性必须独立审阅。
  本步不是实际Full：loaded SUT/runner/environment身份、执行guard接入和独立结果消费尚待闭环。
  I06当前13PASS复核仍使用synthetic Full receipt；I05 source-job终态仅安装/交接，均不扩充验收。

- 2026-09-13 mandatory实际runner链：现有run_validation_tier通过原_run_command启动pytest，
  显式加载workflow_execution插件；private临时request绑定C，controller及workers独立重读C，
  controller汇总实际collection与setup/call/teardown，退出前重新核对绑定并写fresh结果。
  父进程再次核对C，独立检查required nodes、预期worker数、collection摘要/重复数、实际每阶段
  PASS且非xfail，拒绝缺失结果或只重标PASS的残缺报告；结果进入正式summary且不覆盖已有失败。
  禁用插件也因缺失结果而FAIL，不绕过原Full provenance/readiness/runtime-profile检查。
  真实Git+既有runner helper+真实2worker pytest共7种场景（pass/deselect/skip/xfail/failure/
  collect-only/disabled），加原8guard场景，15PASS/50.23s；XML
  `outputs/validation_runtime/devx015-mandatory-real-runner-20260913-v1.xml`15/0/0/0/time50.206，
  SHA `1cc2bdc2b824e2a95ecb2503014ffdb61048146e70833025df56d2cd532452ba`，Ruff PASS。
  这些使用完整形状的synthetic映射，不证明106变体断言充分性，也不是真实repository Full；
  loaded SUT/runner/environment与working bytes、结果文件身份/篡改边界仍需完整V02/V03验证。
  当前15/106 PARTIAL、10mutants、migration、最终C必需tiers/Full/publication及OPS-080仍未闭环。

- 2026-09-13 acceptance working identity：bind_acceptance_checkout复用workflow_contract原生
  bounded_regular_bytes和expected文件identity，只读取已绑定manifest/test路径；exact提交字节
  或既有Git LF文本比较核对C，另冻结raw SHA/size/file identity，不把raw身份归一化。
  runner执行前拒绝漂移且不运行测试体；controller/workers配置时核对，worker收集时检查必测
  module.__file__来源并在结束回传实际身份，controller/父进程复核，缺失或不同worker身份拒绝。
  实际执行中改写和同字节replace均以独立executed marker证明目标body已运行，再要求精确拒绝码；
  串行也执行同样检查。仍不声称证明全部loaded SUT/dependencies/environment或短暂改写恢复历史。
  v1首次测试1FAIL/18PASS：合法CRLF已提交文件被仅LF比较误拒绝，其他负例当时也未抵达目标，
  不作为负例有效证据。修正允许exact字节并加强拒绝原因/实际执行marker；v2共19PASS/77.08s，
  XML `outputs/validation_runtime/devx015-acceptance-working-identity-20260913-v2.xml` SHA
  `bf9f749b9099f731dcf5213ae2b36d7d36fb200bbf3d2cd407f00e3bcfcf4609`。
  Ruff字面量长度修正并新增serial正例，v3 actual runner12PASS/54.88s，XML
  `outputs/validation_runtime/devx015-acceptance-working-identity-20260913-v3.xml` SHA
  `a9bb414458482a606e9694181bab8b370160db726a1f67070209e4d546a6522e`，Ruff PASS。
  当前原106变体映射、mutants、最终C Full/发布和OPS验收仍按原范围推进，不改为局部验收。

- 2026-09-13 runtime identity：实际runner绑定当前解释器python.exe及Windows内核
  GetModuleFileNameW(sys.dllhandle)所指Python DLL的精确SHA、sys.version/implementation/prefix、
  安装distribution的Name/version/location清单摘要、有效环境摘要；native bounded读复用现有机制。
  不记录环境原值，仅固定排除request传输变量、PYTEST_CURRENT_TEST、PYTEST_VERSION和三个
  PYTEST_XDIST观察变量；其余环境和覆盖值绑定。未绑定的command解释器在启动前拒绝。
  controller/workers配置时及结束时、父进程结束时独立复核；实际环境变量和synthetic dist-info
  漂移用执行marker证明到达body后再要求身份拒绝码，不以任意错误充当负例。
  v1 actual runner15PASS/68.83s，XML outputs/validation_runtime/devx015-acceptance-runtime-identity-20260913-v1.xml
  SHA `db05e2474734c2f7db6e6086631c35485516fc9cafb855d0e32c6f3991c82ff5`。
  补真实已加载DLL与独立SHA断言后，v2五项正负例5PASS/34.58s，XML
  outputs/validation_runtime/devx015-acceptance-runtime-identity-20260913-v2.xml SHA
  `02030c5cb6de7818693bd12faa2bee39cc8430f0d08fd72207c161afdc2baf28`；修正Ruff常量getattr后
  v3正例1PASS/11.33s，XML outputs/validation_runtime/devx015-acceptance-runtime-identity-20260913-v3.xml
  SHA `0ddb653e1a77617d870f9f22cc7dfe6e02c22f7035e35e2088d8f14598712e86`，Ruff PASS。
  distribution metadata不是全部loaded dependency/SUT代码字节证明，其他V02/V03原要求仍须继续；
  原106变体/10mutants、迁移和最终C整体验收/发布、OPS-080运行验收仍未完成，不启动Pi/SoL-Pi。

- 2026-09-13 loaded gate code：bind_acceptance_implementation对workflow_execution、
  workflow_contract及父进程实际runner核对同checkout __file__、C源码和已加载函数/方法code。
  仅compile源文件，不exec；结构化比较字节码、常量、嵌套code、参数/闭包和位置元数据。
  原marshal字节比较在合法正例误报，改为结构化身份后正例通过，不把内部引用编码视作代码变化。
  actual runner fixture改从干净子进程获取真实import closure并复制/提交到fixture，本地加载
  自身代码，不借原checkout模块、不mock身份门禁；清单缓存避免Full收集模块全集扩大复制范围。
  loaded_code_drift实际替换内存函数__code__，保留磁盘源码、用执行marker和身份拒绝码确认目标。
  v1 actual runner16PASS/98.97s，XML outputs/validation_runtime/devx015-loaded-code-runner-20260913-v1.xml
  SHA `d3c0fb8f4fd6d3f18c6b5c72931768981fc2296eb85b2f4be0e427289eb0aa94`。
  修复新增3项mypy诊断后，正例/串行/内存替换v2三项3PASS/31.43s，XML
  outputs/validation_runtime/devx015-loaded-code-runner-20260913-v2.xml SHA
  `cb9acc55c8dfb5e74d81edf588ffa93e32ac6b0914c69aff62403e845ba16526`，Ruff PASS；
  mypy仍有原WindowsJobProcess的55项诊断，不能记类型检查PASS。
  当前有限验证门禁scope不替代全部SUT/第三方依赖代码和result custody，完整矩阵/发布/OPS验收继续。

- 2026-09-13 Job typing closure：WindowsJobProcess类级声明factory所初始化的API/handle/
  file/owner/identity字段；移除非self属性上的局部类型声明，launch_binding显式dict包装但保留
  原JSON深拷贝语义。原55项诊断清零，`mypy workflow_execution.py --follow-imports=skip`
  单模块检查PASS；不声称全项目类型检查或最终tier通过。Ruff PASS。
  原生suspend/resume、parent先退child存活、launcher崩溃、非allowlist句柄隔离、非法启动、
  process identity/timeout/cleanup及实际2worker xdist共21PASS/11.70s，XML
  outputs/validation_runtime/devx015-job-typing-native-lifecycle-20260913-v1.xml SHA
  `3cede2d147d1c01a61dfa50889a24543774b06bc20c46c022cbb0f02e88a172b`。
  增加导出launch_binding.argv修改不影响内部绑定的实际句柄断言，加loaded gate正例/serial/
  内存code漂移共4PASS/28.86s，XML outputs/validation_runtime/devx015-job-typing-bound-runner-20260913-v1.xml
  SHA `49a243c0cb3db47f9a41bcb88aeca3048eabe462e564b42eca440fc8c66ada25`。
  类型修正不扩展执行授权，不替代其余原106变体/10mutants、最终C验收发布和OPS-080运行闭环。

- 2026-09-13 mandatory result custody：父进程复用create_bound_recoverable_file预留空结果，
  在持有真实descriptor时，将result identity/root identity连同原绑定write_bound_once至durable
  request，再提交创建。请求通过环境descriptor绑定path/identity/SHA而非仅路径，child原生读取。
  controller仅用apply_bound_file更新原空结果，父进程bounded_regular_bytes按原identity读取；
  child输出内容/身份及既有runner语义检查不省略。正式summary保存request/result/root identity、
  原始result SHA/size；临时传输文件随后正常清理，不作为永久publication receipt。
  实际conftest在配置前替换同字节request，以及结束产生真实PASS后替换同字节result，均独立
  证明inode变化、bytes不变及mutation marker，再要求HANDLE_EXPECTED_IDENTITY_CHANGED拒绝。
  正例2PASS/33.18s、替换负例2PASS/19.80s，随后完整actual runner18PASS/111.74s，XML
  outputs/validation_runtime/devx015-result-custody-runner-20260913-v1.xml SHA
  `9cd4fa7da3a48d2c968eccde26fae3f631f138a7304d31d6f0aff2ff7f842377`。
  增加summary custody及原始SHA/size断言，正例/serial2PASS/25.57s，XML
  outputs/validation_runtime/devx015-result-custody-summary-20260913-v1.xml SHA
  `8b70977b3554af58707d1f28016aaa529dbdec2eeab625e01779953cd7c00210`，Ruff和单模块mypy PASS。
  本步不等于所有V03/R02/ABA历史或Full出版链完成；原矩阵、mutants、迁移和DEVX/OPS总验收继续。

- 2026-09-13 X03精确映射：审阅并复核实际Windows/Python3.11 native oracle测试，登记
  windows_parent_child_xdist（suspended launch、parent先退的wait/close、实际双worker）、
  handle_inheritance（launcher崩溃后Job子树死亡、非allowlist事件句柄）、launch_failure三变体。
  7个精确nodes实际7PASS/18.92s，XML outputs/validation_runtime/devx015-x03-native-mapping-20260913-v1.xml
  SHA `52de4204c5e6a7865fa4bddab741256d47f650f9ba8940b755c636153d2bfb1e`。
  加强双worker逐一原生IsProcessInJob断言，以及child SetEvent的ERROR_INVALID_HANDLE精确结果；
  两项重跑2PASS/7.99s，XML outputs/validation_runtime/devx015-x03-native-mapping-20260913-v2.xml
  SHA `b56fa6a16741281d10bff6302580ed10a48608320f14303144b668233bb5e74f`，Ruff PASS。
  manifest映射18/106，88未映射；独立核对23组/106变体/10mutants未改变，7nodes均出现在实际XML。
  exit_without_result/process_identity_reuse未因名称相近而登记；最终C身份/全矩阵执行仍须完成，
  execution_state=NOT_EXECUTED、mapping_state=PARTIAL_NOT_ACCEPTANCE_READY不变。

- 2026-09-13 S01 aggregate budget和config保全：新增actual CLI测试，在seed/canonical commit
  及original scope取得前冻结synthetic max_files/max_total_bytes；exact同时恰好到达两限，
  over_count/over_bytes分别超出一单位。独立CLI plan/capture/validate，不使用_engine或替换
  loaded-origin metadata。exact用Git tree/blob/diff独立验证恰好指定ADD/MODIFY/DELETE和raw bytes；
  超限要求TASK_CHECKPOINT_BUDGET、无request/捕获目录及refs不变。此处未宣称已证明所有子Git零读取。
  _state补记本地config/config.worktree原始bytes，HEAD/index/source/config均需保持；旧in-process
  plan和mixed staging仅作兼容回归，不纳入新增验收映射。
  三边界3PASS/48.98s，XML outputs/validation_runtime/devx015-s01-aggregate-budget-20260913-v1.xml
  SHA `67257afac82eafac34e527a743c9a3723c8a15f196438eade52cdab373001ce0`。
  加强精确delta后actual exact加两个兼容例3PASS/102.03s，XML
  outputs/validation_runtime/devx015-s01-config-and-exact-delta-20260913-v1.xml SHA
  `76436072ed21f504220a172e95a38703f9a039771ec987d74febffdf1d05a8b3`，Ruff PASS。
  仅登记S01 frozen_add_modify_delete_set和budget_boundary，现20/106映射、86未映射，
  原23组/106变体/10mutants及PARTIAL/NOT_EXECUTED不变；最终C整体验收、发布和OPS仍须完成。

- 2026-09-13 S01 real CLI staging/main：同一实际已提交实现通过独立CLI完成plan/capture/validate，
  覆盖unstaged/requested/unrequested/mixed/empty五状态；另一linked worktree真实推进main。
  独立校验请求raw SHA/size、完整Git delta/blob/deletion、未请求staged内容不入快照，
  原始HEAD/index/config/source保全。5PASS/166.95s，XML
  outputs/validation_runtime/devx015-s01-real-staging-main-20260913-v1.xml SHA
  `cae3d30d1f003ae1a612e039efd9a23c2681ed67d0092ee8588043a08f03e9bb`。
  对照实际XML登记staged_unstaged/crlf_binary_empty/main_advances，现23/106映射、83未映射；
  捕获中漂移尚待真实CLI补证，原23组/106变体/10mutants与PARTIAL/NOT_EXECUTED不变。

- 2026-09-13 S01 mutation_during_capture补证：移除旧测试_engine路径替换，实际已提交CLI
  plan/capture，原生worker CAPTURED屏障确认PID/FILETIME/Job归属后并发改变bytes/HEAD/index。
  父CLI要求TASK_CHECKPOINT_EXECUTION；worker原始stdout分别要求DRIFT/IDENTITY/DRIFT，
  execution_failure无cleanup_errors，原始并发改动保留，无receipt或新增checkpoint ref。
  加强版3PASS/70.33s，Ruff PASS；XML
  outputs/validation_runtime/devx015-s01-real-capture-drift-20260913-v2.xml SHA
  `fe1e98fd1f5cfc7bf1f12b3e9b66825e4ea58dae156db4c088ad910e7d4c73b3`。
  对照实际XML登记该变体，现24/106映射、82未映射；S01六变体已映射不等于最终C执行验收。
  原23组/106变体/10mutants与PARTIAL/NOT_EXECUTED不变，DEVX和OPS整体验收发布仍须完成。

- 2026-09-13 S02跨进程保护文件观察：新增仅用于synthetic canary的原生level-1 oplock观察器。
  不兼容open触发事件后先登记再释放，避免错误子进程挂住；终止时检查晚到事件并取消未决I/O。
  原生机制依据[Microsoft FSCTL_REQUEST_OPLOCK_LEVEL_1](https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_request_oplock_level_1)。
  none/Python读/Python写/真实git hash-object四项校验4PASS/5.89s，XML
  outputs/validation_runtime/devx015-s02-native-observer-20260913-v1.xml SHA
  `77cd9dc8868f8442dc48415845008ab34cfb8f21b5140718cf26c11cf23f4036`。
  实际已提交CLI plan/capture/validate期间五个保护canary零访问，独立验证原始state和canary bytes、
  快照不含未授权内容；显式请求unowned/excluded/secret均以TASK_CHECKPOINT_SCOPE在读取前拒绝，
  无request、capture目录或checkpoint ref。4PASS/52.90s，Ruff PASS，XML
  outputs/validation_runtime/devx015-s02-native-cli-canaries-20260913-v1.xml SHA
  `432c21787d89b68cd405d62bdad03e693299f96859953331bbab4d2c7887d933`。
  三变体映射包含原生观察器正负校验，不以父Python monkeypatch或bytes未变替代子Git零读取。
  现27/106映射、79未映射；path_alias_case/reparse/swap_after_check仍未映射。
  原23组/106变体/10mutants及PARTIAL/NOT_EXECUTED不变，无最终C/Full/发布/OPS验收声明。

- 2026-09-13 S02别名和reparse实际CLI：四种alias（大小写、parent、反斜线、dot）先用
  os.path.samefile独立确认实际同一synthetic excluded文件，再要求PATH/SCOPE拒绝及原生零访问。
  4PASS/21.24s，XML outputs/validation_runtime/devx015-s02-native-aliases-20260913-v1.xml SHA
  `1a5dca948b7aa8902091e16b3df2d37a0bb7188a527f0a7797b630d02c1f5687`。
  叶符号链接/祖先junction分别在plan及已有plan后的capture入口测试；原件先保留，确认实际
  reparse属性和目标，再要求TASK_CHECKPOINT_PATH、canary零访问、原件/index/config/HEAD不变，
  无receipt或checkpoint ref。4PASS/39.25s，XML
  outputs/validation_runtime/devx015-s02-native-reparse-20260913-v1.xml SHA
  `984859c0f3b7fd5cd25c09d733e87592fc6f69e28a0c073e01efc4bcdfa6bfea`，Ruff PASS。
  映射含原生观察器校验节点，现29/106映射、77未映射。swap_after_check仍未映射：
  checkpoint._bytes目前调用旧safe._regular，尚需真实检查后替换竞态验证，不能由静态reparse推定。
  原23组/106变体/10mutants与PARTIAL/NOT_EXECUTED不变，整体DEVX/OPS验收发布继续。

- 2026-09-13 S02检查后替换闭合：真实worker屏障复现旧读取器先返回28字节替代内容再拒绝的缺陷。
  checkpoint._bytes现通过metadata-only handle验证文件身份，再以ReOpenFile读取同一对象；
  叶文件及祖先junction替换均要求TASK_CHECKPOINT_DRIFT、原生保护canary零访问、原件保全，
  无receipt/ref增量。实际recover-interrupted到FAILED_ATTEMPT_TERMINAL_ONLY、禁止再dispatch，
  保留失败证据且原lease已释放。两变体2PASS/52.40s，XML
  outputs/validation_runtime/devx015-s02-native-swap-terminal-20260913-v1.xml SHA
  `929fa2e1f5a702b9bae3ccfde244eb8ec43d68e5ab483a0e6c88e9e31ad72b0a`。
  观察器修正为仅成功的overlapped完成计访问，保持发起线程直到结束，避免取消I/O假阳性。
  原生读取器/观察器22PASS；真实生成、安装恢复、handoff及runner等整批109PASS/2120.44s，XML
  outputs/validation_runtime/devx015-s02-native-reader-integration-20260913-v1.xml SHA
  `1ca511e35ef973b5b155bdd97dbc5bab7621e565867162149c03b1f3e98ba3de`；
  producer恢复补测1PASS/38.47s，XML
  outputs/validation_runtime/devx015-native-reader-producer-recovery-20260913-v1.xml SHA
  `b48c4ca4a3c0232c67227388666cb59da61709454af1cfdec1733b3d1082e820`。
  X03缺失/损坏结果两项使用实际Job退出和lease状态，要求INSUFFICIENT而非PASS，保留原artifact，
  释放后只允许REPLAY_ONLY；2PASS/7.84s，XML
  outputs/validation_runtime/devx015-x03-exit-without-result-20260913-v1.xml SHA
  `38728d7c9d9a66342446a90ccde7fc872040a937979e64ee581db7eab618d6fb`。
  登记S02 swap_after_check及X03 exit_without_result，现31/106映射、75未映射；
  23组/106变体/10mutants及PARTIAL/NOT_EXECUTED保持，以上不是最终C/Full/发布验收。

- 2026-09-13 X01 generator_missing_or_order：遗漏或颠倒四生成器顺序要求GENERATOR_ORDER且
  原fixture files/HEAD/index不变；正例实际运行四生成器并独立核对raw Git blob、mode、ADD/DELETE、
  frozen source和obsolete generated fragments，原工作树不变、无materialization/publication许可。
  三节点3PASS/82.15s，XML outputs/validation_runtime/devx015-x01-order-and-positive-chain-20260913-v1.xml
  SHA `48deb15b66ec5955c4b86a1a0bcf47fd7b2cd50744adde1d020d3022862d86cc`。
  仅登记顺序变体，不推定其余X01或最终C验证；现32/106映射、74未映射，原23/106/10不变。

- 2026-09-13 V01首次转移层修复：真实linked worktree推进main，已绑定C保持不变；原fence在
  before-dispatch或before-result均因PUBLICATION_EXPECTED_MAIN_STALE拒绝V(C)，PASS/FAIL四例
  均触达旧实现目标门禁（4FAIL/38.80s，非API缺失），XML
  outputs/validation_runtime/devx015-v01-fixed-candidate-red-20260913-v1.xml SHA
  `d9909c116e7694aec0f85401d14d520922b06a82c167935565c2bac8a5b6ef9d`。
  显式validation_tier仅在已绑定C的正式验证阶段允许main变化，保留C/lease/tier/plan/parent/resource
  检查；返回validation_only=true/publication_allowed=false。Full派发/结果记录保留原C与main观察，
  技术结果PASS/FAIL不重标；普通validate及LOCAL_MAIN_FF_PRE仍拒绝旧main条件。
  独立检查原HEAD/index/source、结果原bytes、当前main不覆盖、失败行政释放后结果仍保留。
  整份fence24PASS/92.78s，XML outputs/validation_runtime/devx015-v01-fixed-candidate-green-20260913-v1.xml
  SHA `590ee43dc1fe074e940933b8c45e716f7f3a47e0e9d0892707aed8d7c61fc24e`；Ruff和模块mypy PASS。
  新测试是fence转移层且结果为synthetic，不代替实际runner三时序；V01仍未映射，32/106不变。

- 2026-09-13 V01实际runner三时序：新增独立已提交实现/测试/实际policy副本，真实Git linked main
  在runner前、xdist实际必测函数执行中、结果记录前分别推进。真实repository_identity检查origin和
  两sentinel；独立公开publication-fence CLI验证固定候选，实际runner执行与记录helper运行，
  未替换身份门禁或伪造技术结果。PASS核对原binding及2份collection；FAIL必须包含实际目标
  AssertionError，不将任意非零当有效反例。原C/HEAD/index/test bytes保留，claim/result事件各一，
  当前main不覆盖，发布拒绝，行政释放后结果原bytes仍保留；同树C2也要求CANDIDATE_DRIFT。
  首轮6FAIL为漏带lazy workflow_coordination的INVALID fixture，不作为有效red；补齐依赖后
  helper链6PASS/104.37s，加入公开CLI及真实repo检查后6PASS/107.97s，XML
  outputs/validation_runtime/devx015-v01-actual-runner-cli-main-20260913-v1.xml SHA
  `49728138eaf19ea88e4289ccd0238cf13e0b2005eff7d968239db63b8eb6eb90`，Ruff PASS。
  登记V01三变体，现35/106映射、71未映射，原23组/106变体/10mutants保持。
  fixture的106到单节点绑定仅服务执行门禁；不是全矩阵覆盖，不嵌套整仓Full，最终C的完整
  readiness/required tiers/实际Full及发布仍独立必需。PARTIAL/NOT_EXECUTED不变。

- 2026-09-13 P02真实远端确认首次修复：测试实际push C后由独立clone推进bare remote，本地
  origin/main仍为C；另将仅本测试的bare目录保留到同一tmp parent并使原endpoint不可达，finally
  原位恢复。旧CLEANUP_PRE两例均错误放行（2FAIL/1PASS43.11s，目标DID NOT RAISE），XML
  outputs/validation_runtime/devx015-p02-actual-remote-red-20260913-v1.xml SHA
  `b2fb81f072d81b70f393e234f3e9429e371bfdd2f054419ba67364c21885a033`。
  现REMOTE_PUSH_PRE读取单一实际push URL的git ls-remote结果和祖先关系，保存endpoint摘要；
  CLEANUP_PRE重验同endpoint实际tip=C。拒绝多URL、endpoint替换；传输失败不输出可能含凭据的
  URL/stderr，明确REMOTE_UNKNOWN。公开remote-observe只读，不写事件、不授予push，允许重探测。
  原始三例3PASS/44.66s，XML outputs/validation_runtime/devx015-p02-actual-remote-green-20260913-v1.xml
  SHA `3719437e24943327141e63c29e74ad6ab767f1dd96e45a90c590b506454539f1`。
  补公开CLI、endpoint替换/恢复、未知状态经恢复后实际探测C并走完收尾；整份fence27PASS/143.99s，XML
  outputs/validation_runtime/devx015-p02-remote-cli-fence-regression-20260913-v1.xml SHA
  `39fde866bcf3b21379d2a15ff7c35d40fc109ffce4561e492be726fa325a98ae`；Ruff及两个模块mypy PASS。
  尚需ACK丢失、实际推送绑定/结果保全、短互斥区外网络探测与P02完整验收；不将此转移层测试
  的synthetic Full结果当正式Full。P02未映射，35/106、原23/106/10和PARTIAL/NOT_EXECUTED不变。

- 2026-09-13 P02 ACK丢失与短锁：采用合法非默认fetch refspec保留陈旧origin/main，真实Git
  push成功后launcher以47退出且不回传确认。独立bare tip=C，公开探测和收尾不再次push。
  1PASS/19.05s，XML outputs/validation_runtime/devx015-p02-ack-lost-20260913-v1.xml SHA
  `979b0e784eb235365d51a243b12b67aa30b6a28ab087acc6a0fbad17b66f50f7`。
  临时CLI的有限探测屏障在Git commit前冻结，独立PID/FILETIME证明确有存活进程；旧实现
  阻止另一进程使用同store短锁，1FAIL/19.20s到LEASE_ARBITER_BUSY，XML
  outputs/validation_runtime/devx015-p02-short-lock-red-20260913-v1.xml SHA
  `95c7ee90186f2b335a8d2a5195f5317ea8e02098d01c5e963ae3bf41e807cac3`。
  现网络探测在锁外进行，内部immutable准备值绑定事务SHA、head event、目标phase与观察bytes；
  锁内重新检查事件/endpoint、actor/lease/候选后提交。调用方提供准备参数在任何变更前拒绝。
  另一进程可在探测暂停时取得短锁；若真实释放事务，旧观察拒绝为REMOTE_OBSERVATION_STALE。
  初次green因fixture写pycache阻断clean gate，已仅令测试子进程不写字节码，未放宽clean gate。
  两例2PASS/37.42s，XML outputs/validation_runtime/devx015-p02-short-lock-green-20260913-v2.xml SHA
  `6dae0ce5ca385727463f578ee88f1b96ed930205fc2c03e6c1dcb6f0aeae930d`。
  整份fence及V01实际runner六例联合37PASS/199.59s，XML
  outputs/validation_runtime/devx015-p02-short-lock-and-v01-20260913-v1.xml SHA
  `19c66c0d01037a44de9568d7ee1e86d1671f7eaa87225bffd00032e825a80d50`；Ruff/模块mypy PASS。
  尚需发布前实际分歧、实际推送绑定与事实保全等闭合，不宣称跨远端原子性或全部P02通过；
  原35/106、23/106/10与PARTIAL/NOT_EXECUTED保持。现有lease使用公开heartbeat正常续期，
  不手改TTL、不新增事务或scheduler。

- 2026-09-13 P02补充：实际远端在push前分歧，且对象已存在本地而origin/main仍旧，
  REMOTE_PUSH_PRE拒绝PUBLICATION_ANCESTRY_INVALID并保持原事件/HEAD/index，1PASS/17.17s；
  outputs/validation_runtime/devx015-p02-before-push-divergence-20260913-v1.xml SHA
  `a264f881fb4c8cc523c5ef97f00e58307346ab60ae66644e019395fd42a98b71`。
  新COMPLETED事件保存CLEANUP_PRE实际观察，receipt只投影这份point-in-time确认；旧终态不补写。
  整份fence32PASS/197.82s，outputs/validation_runtime/devx015-p02-confirmation-receipt-20260913-v1.xml
  SHA `fbc68574e51151a6315770878a476af6eaa9b2a799fbc53c8d925e62cb71c4ea`。
  最后补结构拒绝与ACK回归2PASS/35.20s，outputs/validation_runtime/devx015-p02-confirmation-structure-20260913-v1.xml
  SHA `449a6d537c6a9a0b73ce4cc39e30568bd7c0b47ac8de034c5c73256d193d0548`。
  缺失观察测试仅验证parser拒绝，不作为公开恢复验收；P02仍未映射，35/106不变。
  正式run_validation_tier.py仍直接Popen，独立Job测试不证明正式Full托管，下一步必须接入
  同一执行租约reserve/bind/resume/exit/result链并证明C与source lease base的区别，不能改写base。
  整体交付仍含原23组/106变体/10mutants、最终C必要tiers和实际Full、迁移/发布，以及
  OPS-080工程部署和新合法ordinary daily/immutable consumer closure；不启动Pi/SoL-Pi。

- 2026-09-13 Full托管接线准备：runner新增_run_leased_command，复用同一ExecutionLifecycle，
  reserve后实际创建suspended Job，bind/resume、锁外等待整棵树和输出，再confirm_exit。
  不在该函数把进程退出当成最终验证接受；结果仍等待调用层独立验证/adoption。重复请求拒绝再派发。
  真实父进程退出0/7而子进程延后退出的两个用例通过，同时证明candidate不必等于source lease base，
  原base未改写。首次2PASS/7.54s，XML devx015-full-runner-job-custody-20260913-v1.xml SHA
  `42e5388da1819ab91fa03426a486c849d75ac32273b5bd5ad30b2e4111367902`。
  异常处理保留原错误及未完成custody说明后，runner整份回归加上述两例及重复派发拒绝共
  82PASS/33.84s，outputs/validation_runtime/devx015-full-runner-job-and-runner-regression-20260913-v1.xml
  SHA `352c9f1b9610b3004b029a6cd7b1e22d0c56b802574aaf60ef3e7ac090698038`。
  Ruff/限定路径diff-check通过；显式MYPYPATH=src的runner strict mypy仍57错误，未声称通过。
  正式main和mandatory helper尚未调用该函数，不能声称已完成正式接线，亦不新增106映射。
  下一步绑定实际Full admission/完整guarded argv/env、canonical task/候选/host epoch、持久request，
  正式入口调用，再把最终summary及mandatory/profile验证后的状态收存到同一lease后记录fence结果。

- 2026-09-13 Full正式入口接线：main构造_FullCommandRunner，mandatory helper在增加实际
  插件argv/env之后调用它；非Full和合法benchmark仍走原路径，已存在Full benchmark拒绝不变。
  适配器重新验证FULL_DISPATCHED、exact C/transaction/lease/parent，绑定真实canonical fragment、
  host/epoch、interpreter/runtime与环境摘要；不把lease source base改写为C，不用旧任务状态替代当前。
  同一lease reserve之后保留execution_request.json、execution_validation_identity.json和实际stdout，
  挂起创建/Job绑定/resume/整树退出；profile/mandatory最终状态完成后才把summary精确SHA写入
  execution_result.json并record_result，再记录fence结果。结果同bytes可重放，改写拒绝；这不是发布许可。
  两例真实canonical/fence/Job适配器PASS与FAIL证明候选/环境/输出/最终custody和错候选summary拒绝；
  readiness仅是明确synthetic边界输入，不声称执行整个正式Full准入。2PASS/31.23s，XML
  outputs/validation_runtime/devx015-full-adapter-actual-fence-custody-20260913-v1.xml SHA
  `80f8f585a15901ab378d83537c81892ed8a07888096abeb268289074ef377d46`。
  接着适配器重放/既有runner/真实V01 runner联合89PASS/121.40s，XML
  outputs/validation_runtime/devx015-full-entry-custody-runner-v01-20260913-v1.xml SHA
  `98cb603cda046e0001d48fc02f58ac7c359ae8a2bbbb242c1e5642c9c242a127`。
  最后补执行身份原始bytes留存、实际interpreter一致和新4个保留路径；对应适配器两例与
  公开CLI print-only覆盖拒绝五例7PASS/33.04s，XML
  outputs/validation_runtime/devx015-full-identity-retention-artifact-guards-20260913-v1.xml SHA
  `3d9db530c9953deca6af2100dd94e81d26d5ef173b862315892c1df9c2307452`。
  所有检查仅工程进展，35/106不变。
  下一步证明正式Full claim/reserve/launch/exit/result崩溃窗口、受控恢复和实际mandatory Job链，
  审核task取消/撤销与最终DONE的当前准入、完整loaded SUT身份；57项runner mypy仍未闭环。
  原整体验收/迁移发布/OPS-080工程部署及新合法ordinary daily接受要求保持。

- 2026-09-13 Full已收存结果的公开恢复：run_validation_tier.py full --recover-full
  --publication-transaction <原事务> --task-id <原任务>核验实际fence/S4D replay和ACTIVE owner、
  RESULT_RECORDED中冻结的artifact SHA、exact candidate/request/validation identity及summary SHA；
  新进程仅补记FORMAL_VALIDATION_RESULT，不派发、不发布。已补记时验证原event/evidence后只读重放；
  错task、结果/summary改写和print-only混用均拒绝。未收存窗口仍RECOVERY_REQUIRED，不能声明全部恢复完成。
  fence自身的FORMAL_VALIDATION_RESULT现核对同一lease冻结custody和此次实际evidence SHA，
  防止绕过恢复预检或在两入口之间换summary；无execution的旧边界证据不被重写为Job接受。
  测试在复制并提交的真实runtime下用真实Job执行0/7，launcher收存后os._exit(47)，新公开CLI恢复；
  native PID/FILETIME持有句柄验证实际launcher已死、Job不存在、无第二派发，summary/stdout原bytes保留。
  首轮2FAIL/54.53s仅因测试把venv redirector当actual Python PID，XML
  outputs/validation_runtime/devx015-full-recorded-custody-public-recovery-20260913-v1.xml SHA
  `e00ccab6dd67e2fac272f5b4991da434fc5239d843b89ee1621f8fec9bdd9a69`；不是baseline red。
  修正独立actual PID观察后2PASS/62.53s，v2 XML SHA
  `7a95025467edfdf5bcc2621d1d9bb7007bd1f8a5e79df302a5da5c6848630d2d`。
  加入结果原bytes改写、错task及直接fence summary拒绝，整份fence/runner/适配器/恢复联合
  120PASS/229.91s，outputs/validation_runtime/devx015-full-public-recovery-fence-runner-20260913-v1.xml
  SHA `e76350819cb106f973904b5d3aaa82910e5caa90f87ac1bcb59366ef773e3748`。
  随后runner在同一前台循环按TTL/3续租，初次先renew；不新增scheduler/lease。恢复仅在原lease
  仍ACTIVE且原custody核验通过后renew，拒绝重占已终止/重新分配的lease。1秒TTL实际2秒子进程
  与公开恢复两例3PASS/74.10s；退出时间超过首个TTL，仍同一lease且expires_at晚于实际exit。
  XML outputs/validation_runtime/devx015-full-lease-heartbeat-and-recovery-20260913-v1.xml SHA
  `78451566d66066f9dd9ab766464a19ee08c37e03d3334e13018c0aebfa8e6a4d`。
  readiness fixture明确只证明恢复/托管接缝，未运行整仓Full；35/106和原23/106/10不变。
  尚需claim/reserve/launch/exit/summary收存前窗口及有限恢复、actual mandatory Job完整链、
  当前task撤销/取消与DONE界限、loaded SUT全身份和runner57项mypy；不改变最终完整交付。

- 2026-09-13 Full当前任务冻结：实际取消终态为DROPPED。_full_task_commitment在Full claim前
  验证当前canonical registry、拒绝DROPPED和明确REVOKED授权；合法DONE不一概拒绝。
  核对scope原SHA/task/decision，任务原bytes必须与exact C的真实Git blob一致，parsed fragment
  也须匹配；保存task/event/status/fragment SHA，最终guarded argv启动前再次核对同一commitment。
  real canonical CLI更新的active/DONE/DROPPED/REVOKED、未提交记录和scope篡改六例通过；
  DROP/REVOKED经实际Full准入函数在claim前拒绝，无事件、claim、executor和Git/index副作用。
  首轮3PASS/2FAIL仅因夹具过早commit导致PUBLICATION_LANE_HEAD_DRIFT，不是baseline red；
  outputs/validation_runtime/devx015-full-task-current-candidate-gate-20260913-v1.xml SHA
  `1f269e1d5653a8fd296bb302ceea475284a83779b85af571f146504b6ad71114`。
  修正事务顺序后门禁/适配器/公开恢复/runner联合92PASS/176.95s，XML
  outputs/validation_runtime/devx015-full-task-gate-adapter-recovery-runner-20260913-v1.xml SHA
  `0d6bd833cde8858cd363707c64329efad680a3f9e62bd0427096e8d5b6344521`。
  最后scope补强6PASS/72.42s，outputs/validation_runtime/devx015-full-task-scope-candidate-gate-20260913-v1.xml
  SHA `140bddf37f86e139b71a18bb5f17f18834dc3a36279c7e4cae33429bbbd35b17`。
  旧runner单元fixture仍明确替代其authority边界；真实上述门禁不使用该替代，不以单元PASS冒充准入。

- 2026-09-13 actual mandatory Job接线：复制并提交真实runtime、canonical和固定小测试，
  _run_mandatory_acceptance_command注入真实插件后经_FullCommandRunner实际Job执行两worker xdist；
  测试用独立Win32 OpenJobObject/IsProcessInJob断言自己属于原Job，记录worker身份。
  正常和故意AssertionError均经原mandatory验证、同一lease custody和fence结果，退出后Job不存在。
  validation identity现在保留mandatory原绑定；原106映射到单一节点仅是此transport fixture，
  不是106覆盖或正式profile/readiness接受。首轮1PASS/1FAIL仅测试误取evidence.status，实际字段是
  evidence.execution.status；原XML devx015-actual-mandatory-xdist-full-job-20260913-v1.xml SHA
  `05d55c7240e7d0e161a2f55ab2e2cc7119af4696871f14cd94d16d2d6284a340`。
  修正并检查两份collection和完整setup/call/teardown后2PASS/81.90s，XML
  outputs/validation_runtime/devx015-actual-mandatory-xdist-full-job-20260913-v2.xml SHA
  `79dc6742194e3636d33e2f85768bfae63af10fcdce675fb6564f87020b769ea3`。
  两个真实节点加入既有X03/windows_parent_child_xdist的必跑清单，原四节点保留，collect-only精确6节点；
  35/106和PARTIAL/NOT_EXECUTED不变，未新增变体或把收存前崩溃/完整SUT/正式Full标成完成。

- 2026-09-13 Full验证承诺与收存前恢复：新增lease_execution.v4，只允许原launcher在实际
  LIVE_CONTAINED_HANDLE退出后，把最终验证的full_execution_result.v1及summary原SHA承诺
  追加到同一租约事件链；原v1/v2/v3历史不改写。当前request/C/validation identity、状态和
  summary路径严格绑定；commitment形成后不可更换，即使重封hash仍拒绝身份或事件转换漂移。
  Full runner在结果文件写入前冻结该承诺，后续record_result复核结果原bytes和summary原SHA。
  public full --recover-full在原launcher实际退出后，可从冻结承诺重建缺失结果文件、完成
  custody和fence；原launcher仍活时仅OBSERVE_ONLY。通用recover对缺失已承诺文件返回
  FULL_COMMITTED_RESULT_REQUIRES_VERIFIED_RECOVERY，不把原验证事实抹为INSUFFICIENT；
  无承诺松散v1 PASS不再自动采纳，保留原bytes、终态INSUFFICIENT且无再次派发。
  实际复制并提交runtime、canonical、fence、NativeOracle PID/FILETIME/Job、os._exit47后
  新CLI验证committed/artifact/recorded三窗口×PASS/FAIL。首轮8PASS/232.68s（含原adapter），
  XML outputs/validation_runtime/devx015-full-commitment-crash-recovery-20260913-v1.xml
  SHA `c9209eb6a7cd86693f52dc833c7658cc23c47c2accb149860242bbba9348128a`。
  补充活launcher不得接管、原承诺重封hash反证、无承诺松散PASS拒绝后，联合runner/fence/
  mandatory Job/真实generic崩溃回归135PASS/368.51s，XML
  outputs/validation_runtime/devx015-full-commitment-custody-regression-20260913-v1.xml
  SHA `63a6fde19d5f70b156d13e058d26730a6f7b645d6396eb9695345f1cc91610fe`。
  Ruff和workflow_coordination单模块mypy通过；runner当前仍57 typing错误，未宣称正式层通过。
  此证据不替代claim事件前后、reserve/create/resume/exit/summary未承诺窗口的公开终态恢复；
  claim当前尚无launcher身份，须补可验证接线。35/106不变，剩余71变体/10mutants、完整身份、
  managed migration、最终C required tiers/actual Full/发布收口，以及OPS080全部后续仍在推进。

- 2026-09-13 Full claim→reserve前恢复：integration_publication_full_dispatch.v2将完整claim及
  原launcher PID/FILETIME先保存于FULL_DISPATCHED事件，随后写claim文件投影；不再产生先有
  自包含claim文件、后有权威事件的新窗口。_FullCommandRunner在保留执行前要求原claim进程，
  另一进程不能拿相同binding启动。公开full --recover-full在实际原进程退出且原lease没有
  execution时，复核同一lease intent/状态、事件和原claim，补缺失投影并经原release关闭FAILED
  尝试，technical_status=NOT_EXECUTED；不伪造pytest FAIL、不重派发或发布。活原进程只观察，
  旧无launcher事件不自动获得恢复权。原release事件/receipt恢复路径复用，不新增store或scheduler。
  真实committed runtime/canonical/fence、新launcher os._exit47、独立NativeOracle验证事件落盘
  后投影前/投影后两窗口，2PASS/66.33s，XML
  outputs/validation_runtime/devx015-full-claim-process-recovery-20260913-v1.xml SHA
  `3de0c936c11cc030f7c11c1a7eab24f2fb95195153f64978280cf359308f5c9f`。
  补原launcher实际Full adapter冒用拒绝并将既有恢复/mandatory Job夹具的claim移至真实子launcher
  后，联合runner/fence/Job/custody/generic恢复137PASS/419.34s，XML
  outputs/validation_runtime/devx015-full-claim-custody-runner-regression-20260913-v1.xml SHA
  `9f68e41f5f8ff5ace73a282561466237968f54fdce9cc7ea658aa3d115cd5434`。
  最后将lease intent核验前移到投影写入前，窄复跑2PASS/67.25s，XML
  outputs/validation_runtime/devx015-full-claim-owner-before-projection-20260913-v1.xml SHA
  `e11f500de93faf5ad5858fdc6ad8e7829ffd4b298930d3e791a3a65eb7629b8f`。
  Ruff/fence单模块mypy通过，未跑正式层。claim事件前、reserve/create/resume/exit/未承诺summary
  等窗口仍须各自公开有限恢复实证，不能以这两个窗口替代R02。

### 剩余矩阵按共同机制推进（2026-09-13，非新验收合同）

仅复用本节原23组/106变体/10mutants和现有机器矩阵；39个已映射不等于39个最终C已验收。
当前67个未映射变体按以下互斥组定位工作，不能按67个新功能估工期：

| 分组 | 原case与未映射数 | 已知状态与下一步 |
| --- | --- | --- |
| 源码/准入/生成 | I05(2)、I06(7)、V03(4)、X01(4)，共17 | 已有受控源码、canonical准入和生成证据；逐入口核对原断言，区分缺接线和缺实证，再登记合格原始结果。 |
| 恢复/发布/竞态 | P01(3)、R01(3)、R02(10)、X02(6)、X03(1)，共23 | 多段已有实现/实证，Full尚有未承诺窗口；先复用故障设施补公开终态出口。P02补独立Git调用计数和参数反证后四项已映射，仍非正式Full或整体验收。 |
| 协调/迁移/活性 | L01(3)、L02(4)、L03(7)、X04(4)，共18 | 有原store/arbiter/Job基础，实际linked-host authority、迁移排空/重启及有限等待者序列仍需闭环；不以互斥测试冒充公平性，不另建scheduler。 |
| 验证身份/测试完整性 | V02(5)、X05(4)，共9 | 部分mandatory transport与当前候选门禁已实证，完整loaded SUT/env及正式closure、精确目标mutant反证仍未闭环。 |

每项审核只记录三类可核实结论：实现/接线缺失（先补实现），已有实现但缺对应实证（补精确
故障/独立oracle），已有对应证据但待身份/断言审核登记（先审核，避免重复执行）。目前分组
尚不是67项逐项分类完毕；不能把存在一个同名测试判为第三类。复用barrier/os._exit/native
oracle/runtime fixture，但保留各窗口独立断言和原始失败材料。10mutants仍要求目标断言杀死。

runner原57条mypy集中于动态object数值/列表、Mapping/None收窄、profile聚合循环变量复用
和Any返回；现已按根因清零并经runner/profile/实际mandatory Job联合回归，不新增ignore或关闭strict。
每组做最小充分回归，最终exact C仍完成全部mandatory tiers/actual Full。已观测窄组约1分钟、
恢复/runner/fence联合约6–7分钟、真实四生成器大组约35分钟，仅能说明测试窗口，不能推出
项目完成天数。全程工期需上述分类和最终门禁测量后再估；未依据吞吐测算的3–7天不是排期。
不新增任务/执行者/管理平台，不缩减OPS080六层验证、既有部署门禁与新provider-ready daily，
不启动Pi/SoL-Pi。

- 2026-09-13 runner类型清障：原57项mypy经TypeGuard/TypedDict明确已验证数值及数据结构，
  聚合worker/file不同tuple变量分离，完整profile Mapping及时间offset局部收窄，移除两处旧
  type ignore，严格单模块mypy清零；未关闭strict。命令结果必须原整型exit_code，不从bool/
  字符串/float伪造退出码；原非布尔、非负、有限数值校验保持。原runner83PASS/25.35s，XML
  outputs/validation_runtime/devx015-runner-typing-root-causes-20260913-v1.xml SHA
  `3ad1187ba921e8e6bfc8fd12e424cff3fd636ebfecbc0c81188f8e0b17813bfd`。
  补18个类型反证后runner/profile/实际mandatory-xdist-Job联合127PASS/80.16s，XML
  outputs/validation_runtime/devx015-runner-typing-profile-actual-job-20260913-v1.xml SHA
  `8324c9d11a99106883e9dcbddb3fafd88f54d1a4f4fc4658d1a582901bc1a7f9`。
  这是正式验证前清障和focused证据，不宣称整体typing tier或最终C Full通过。

- 2026-09-13 P02已有证据补强后登记：保留原真实bare remote分歧、ACK丢失、确认前继续前进、
  unreachable与公开探测/终态路径，新增独立Git Trace2 cmd_name/start SID及argv观察。
  原fixture每条协议精确push计数（advanced含peer共2，其余共1），禁止force参数或带+refspec；
  独立校准真实两次同SHA push，远端不变但计数必须2，捕获原末态oracle看不见的重复调用。
  observer通过临时环境启用，不替换Git/SUT方法；仅记录合成fixture Git调用，不改发布机制。
  原八变体加校准9PASS/203.78s，XML
  outputs/validation_runtime/devx015-p02-independent-push-count-20260913-v1.xml SHA
  `9a422a358d013a0c422314aeaa451e39620c3e969533376c14af95f8707de205`。
  增加argv无force核对后四P02变体加校准5PASS/102.70s，XML
  outputs/validation_runtime/devx015-p02-push-count-and-argv-20260913-v1.xml SHA
  `41505f43c680d03f1adf22177caae1b3af28fc925050eb2fe8030f3ffcb26105`。
  XML逐节点核对及映射5节点collect-only通过，原P02四变体登记后39/106映射、67项待补齐/审核；
  原23/106/10与PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED不变。fixture的synthetic Full
  summary只用于发布转移层，不宣称真实Full准入、canonical整体验收或新的实际remote发布；
  原baseline red、当前源码/节点及副作用证据保留，各case最后仍须exact C必跑验证。

- 2026-09-13 Full无最终承诺窗口补公开恢复：原v2 claim/request/launcher及租约身份核验后，
  复用原ExecutionLifecycle与同一事件链；真实原进程退出、Job已空/不存在后以INSUFFICIENT
  关闭失败尝试，不补造FORMAL_VALIDATION_RESULT或采纳松散PASS，不重派发、不发布。
  独立失败证明full_incomplete_recovery.v1绑定原execution，原stdout/summary/松散结果不改；
  已记录退出码0/7保留，未记录退出码保持未知。释放原租约后重复公开恢复仍幂等。
  实际子launcher在reserved/request/created/bound/resume-intent/running/exit-unrecorded/
  exit-0/exit-7/summary-0/summary-7共11窗口os._exit，公开CLI恢复11PASS/323.88s，XML
  outputs/validation_runtime/devx015-full-uncommitted-public-recovery-20260913-v1.xml SHA
  `4772b410319bdd4f5efddbca83f6543b0ac13dc06087a5e5fb4105292fc46c0c`。
  增加真实观察者Job句柄保留父死子活：默认RECOVERY_REQUIRED无变更，明确
  --recover-full-action terminate_frozen_job仅按冻结原PID/FILETIME和实际Job成员有限终止；
  独立native oracle证明活子进程真实退出、Job消失、原文件完整保留。该新窗口与原claim/
  committed/artifact/recorded恢复及runner联合111PASS/287.07s（0skip/errors/failures），XML
  outputs/validation_runtime/devx015-full-uncommitted-held-job-and-recovery-regression-20260913-v1.xml
  SHA `c2b2e835dbcf9ee380c339297d3ddbceb3f6343708f72f75ffd188c6039c23b7`。
  Ruff及runner单模块strict mypy通过。fixture readiness仍为组件合成输入，不宣称最终Full。
  当前39/106映射保持，不把上述窗口自动扩大为R02全部完成；claim事件前仍待核对。
  同次复核发现新v2 claim直接FORMAL_VALIDATION_RESULT在execution为空时可跳过custody，
  且低层结果收存与最终承诺的强制绑定仍待逐入口闭合；属于原I06/V02验收，不是新平台范围。
  DEVX-015最终C/正式验证/发布/迁移及OPS-080工程部署与新合法daily仍继续推进，不能以本组
  专项通过交付整个任务；不启动Pi/SoL-Pi。
- 2026-09-13 下一准入缺口已有有效red：真实canonical/提交候选/新v2 Full claim通过原公开
  fence API，在execution=None时提交PASS或FAIL均未拒绝。两项在目标raises断言失败，
  不是import/API/fixture缺失；2FAIL/62.38s，XML
  outputs/validation_runtime/devx015-full-direct-result-without-execution-red-20260913-v1.xml
  SHA `f782f4275476ad36eb776d41b66df8ce803acbf24f8f064ef31c49e40599f20a`。
  尚未实施该生产修复；应一并要求新claim的真实execution与最终commitment，并升级旧发布/
  V01/skill转移fixture的synthetic-summary捷径，不增加legacy布尔绕过或弱化原断言。

- 2026-09-13 上述直接Full结果准入缺口已修复：FORMAL_VALIDATION_RESULT强制实际execution/
  最终full_result_commitment，独立核对原v2 claim的C、deterministic request/Job和launcher，
  原冻结record/摘要与实际custody必须一致。checkpoint在原atomic内验证全部payload后才
  heartbeat；拒绝缺execution/commitment的PASS/FAIL提交，不续租、不改原事件链。
  旧不可变历史读取保持，无legacy开关。低层generic custody仍不是正式采用权限；公开恢复
  保留已收存的原PASS/FAIL和文件，将无最终承诺的尝试独立判为INSUFFICIENT并关闭原租约。
  新反证在实际Job退出与原低层record_result成功后省略最终承诺，旧入口仍未拒绝；2项目标
  断言FAIL/61.56s，XML outputs/validation_runtime/devx015-full-uncommitted-custody-admission-red-20260913-v1.xml
  SHA `6fe72a56e1f35395ccc74b9d4c38813dc74e291a8549920b342a80c512a5c0ba`。
  原测试临时Git fixture会被pytest保留策略清理；留存JUnit不能替代最终C上仍需执行并保留
  exact SUT/test身份的全部mutation/验收证据，不据这两份临时baseline宣布整体完成。
  P02、fence层V01及completed-task准入fixture已改为实际小Job/native进程观察/承诺/custody，
  其合成验证内容仍只证明转移机制；expiry反例使用原显式测试时钟，不授予生产TTL抢占。
  V01六项进一步改为真实canonical候选、原子claim、_FullCommandRunner、mandatory/xdist、
  最终承诺及fence收存；真实main在三个时点前进，原PASS/FAIL不改，C2和发布另行拒绝。
  manifest中六个V01节点已更新为full-recovery参数身份；总映射仍39/106，未扩称I06全部入口。
  新custody-0/7窗口证明保留实际原PASS/FAIL且恢复仍INSUFFICIENT；child启动journal追加记录，
  精确一行并保留原bytes，重复恢复不得增添第二次执行记录。
  首轮8PASS/2FAIL/1teardown error：失败是V01 fixture CRLF diff-check，以及退出7却写PASS
  被既有RESULT_PASS_WITHOUT_EXIT正确拒绝；修正fixture LF/FAIL，不放宽生产检查。
  两项修正回归2PASS/72.93s，XML outputs/validation_runtime/devx015-full-custody-fixture-corrections-20260913-v1.xml
  SHA `fd38d6a57bdd16cf5f2fd04d575090e749a6c5578da8bea14688eeb749f75dec`。
  失败fixture已走原公开恢复到FAILED/RELEASED/INSUFFICIENT并保留退出7；手动恢复漏设
  PYTHONDONTWRITEBYTECODE产生的71个自有未跟踪pyc，经精确路径/创建秒/非reparse核对后
  仅删除这些可再生缓存，再以原环境完成恢复；源码、原结果和原始失败JUnit保留。
  最终相关回归83PASS/1078.68s（0failures/errors/skipped），涵盖完整fence、公开Full恢复、
  实际adapter/mandatory Job、全部V01与completed-task准入；XML
  outputs/validation_runtime/devx015-full-commitment-admission-fence-recovery-regression-20260913-v1.xml
  SHA `b5bf08685997d2d52d71fc6ecedcd979b45375e24ddf65f39929713d0ea121fe`。
  该执行源码SHA：runner `d6cfc302a8619605fb8553fc1d6e2aa0afeca60b530e43fca9c30923873af135`；
  fence `16786cb785ce41ba35546cdcb6ca3520a4d3e760ad7f79cb6e5db89089229f71`。
  Ruff及上述两模块strict mypy通过，最终C/全部mandatory tiers/actual Full/迁移发布与OPS080仍开。
  下一既定V02接缝：_runtime_payload已将pytest技术PASS与profile不合格区分，后者
  can_support_promotion_evidence=false；当前LOCAL_MAIN_FF_PRE仍只看validation_status。
  需要有效反证及独立正式closure采用门禁，不能改写原技术结果或只信自报布尔/hash。
- 2026-09-13 下一profile对照fixture预登记：
  `outputs/validation_runtime/devx015-formal-profile-publish-red-20260913-v1-fixtures/`
  为本任务自有唯一pytest basetemp；用于实际canonical/Full Job/mandatory/性能记录对照，
  保留Git候选、原profile/summary和fence/runner/test/driver原bytes及hash。路径不复用、不覆盖；
  在最终exact C验收证据归档、无执行依赖且唯一内容核对后方可受治理清理。
  此对照只验证正式profile采用接缝，不替代其余Full readiness与全部V3实证。
- 该v1运行实际2 setup ERROR/11.610s，Git初始化遇到Windows Filename too long，
  未到达目标门禁，不能记为baseline red。保留原XML和目录；仅将下一独立测试的
  basetemp预登记为 `D:/t/devx015-fp2`（创建前确认不存在），避免过长绝对路径。
  所属DEVX-015，目的、原始证据保留及最终验收后治理清理条件与上述fixture相同；
  不修改Git全局配置、生产路径规则或验收门槛，不复用v1目录。
- v2启动在xdist前因父目录D:/t不存在而退出，未创建fixture，也不算有效red。
  下一独立basetemp为 `D:/Work/devx015-fp3`，父目录已存在；目的、所属任务、
  保留及治理清理条件不变，创建前必须验证目标不存在。
- v3实际2FAIL/1teardown ERROR/242.11s，仍非有效门禁red：formal子launcher超时120秒；
  filtered实际pytest退出0/mandatory PASS并收存，但profile telemetry FAIL，原始telemetry
  指明inactive_worker_ids=[gw0]（2 workers仅1个loadfile文件）；另缺少
  --no-loadscope-reorder。不得降低profile完整性检查或把该轮失败记为V02完成。
  formal原进程均实际退出后，原公开--recover-full在2026-09-13T11:13:11Z返回
  RECOVERED_FAILED_ATTEMPT/INSUFFICIENT、FAILED/RELEASED、dispatch_performed=false，
  原未知退出码保持null；原日志/profile/XML保留，不改写。超时原因仍需诊断，不能由
  日志中的1 passed推定进程正常退出。下一夹具须提供两个实际测试文件、冻结对应输入集合，
  按完整scheduler选项执行；同时保留实际进程退出诊断，不能只增加超时来掩盖问题。
- 下一独立profile fixture `D:/Work/devx015-fp4` 归属DEVX-015，创建前验证不存在，
  保留原Git、两个实际Job witness、profile/summary/进程超时stack及原始XML；
  最终C证据归档和无执行依赖审计后才清理。full-profile夹具在原acquire之前冻结
  两个真实native Job测试文件及诊断conftest，使用既定--no-loadscope-reorder；
  不放宽telemetry、生产路径/租约或原120秒超时，诊断只输出实际线程stack。
- fp4实际2FAIL/58.67s且无ERROR/超时：filtered已在完整telemetry/pytest0/mandatoryPASS后
  到达目标DID NOT RAISE；formal对照因合成duration helper固定source workers16而失败。
  下轮只在冻结前将本对照合成duration source workers设为实际2，不改生产/历史原件。
  `D:/Work/devx015-fp5`预登记为同任务独立保留basetemp；创建前验证不存在，
  原始Git/进程/结果证据保留及最终C归档后清理条件与fp4一致，fp4原件不复用。
- fp5实际2FAIL/56.36s：完整duration合同明定source workers=16，2worker对照无效；
  filtered目标仍未拒绝，但不得据此省略有效formal正例。停止缩小worker数，按既有合同
  在冻结前创建16个实际native Job测试文件，真实-n16/loadfile与16worker witness。
  `D:/Work/devx015-fp6`为下一唯一保留basetemp，创建前确认不存在，生命周期同fp4；
  不改变生产16worker/profile检查，不把16小测试或106映射transport称作全部V3验收。
- fp6正式对照实际1PASS/1目标FAIL/76.84s，无ERROR/skips；formal正例完整16worker
  telemetry/profile/performance PASS，filtered完整pytest0/mandatory/telemetry PASS且正式
  performance FAIL，原LOCAL_MAIN_FF_PRE目标DID NOT RAISE。原XML为
  outputs/validation_runtime/devx015-formal-profile-publish-red-20260913-v6.xml，SHA
  b7023cea27394e3ec9f25de3f735053cde4c3b618ce85ac7526932e1922b9a85。
  原Git候选/原summary/profile/driver/test/SUT bytes及SHA保留fp6，不复写此有效baseline。
  新实现复用runner的严格captured-profile和mandatory检查，通过只读
  --inspect-full-publication-profile从原execution/commitment定位原summary/sidecar/identity，
  校验C输入、实际argv、canonical task和mandatory source；不只信自报PASS/hash。
  fence在原arbiter之外执行只读检查，在原atomic内重新核验event/execution和全部捕获字节后
  才允许LOCAL_MAIN_FF_PRE并续租。无第二validator/store/lease/queue，原pytest结果保持。
  此门禁是profile/mandatory接缝；全部loaded-SUT/env、其余Full readiness、旧转移fixture
  升级及原V3/最终C/OPS080验收仍开。当前实现尚待运行验证，不记为完成。
  `D:/Work/devx015-fp7`预登记为修复对照唯一保留basetemp，创建前确认不存在；
  归属任务、原始证据保留与最终C归档后治理清理条件同fp6。
- fp7实际2PASS/85.92s：真实16worker正常对照发布准入通过；filtered真实技术PASS仍保留，
  新发布门禁拒绝且原event/lease/raw summary不变。修复源码Ruff及2模块strict mypy PASS。
  继续加入公开只读CLI零写检查与原profile/validation identity的真实raw篡改拒绝，
  并验证正式builder原有的-n 16双参数形式；不增加例外或降低原要求。
  `D:/Work/devx015-fp8`为下一独立保留basetemp，创建前验证不存在，生命周期同fp7。
- fp8原128项（真实profile/公开CLI/原bytes篡改拒绝及完整runner/profile单测）
  实际128PASS/119.65s，无failures/errors/skips，严格检查保持。
  后续只读复审发现新preparation需保留旧main漂移错误的优先级：先验证当前main/C，
  再检查技术PASS。已最小调整；并补充实际检查成功后到原atomic前的真实sidecar篡改拒绝。
  `D:/Work/devx015-fp9`预登记为下一独立保留basetemp，归属、保留与清理条件同fp8；
  此次仍需原V01 main漂移回归，不能把前128PASS当作调整后已验证。
- fp9实际12PASS/222.86s（2个profile对照、4个fence V01和6个真实runner V01），
  检查后真实sidecar篡改在原atomic拒绝且原lease/event不变，main漂移优先级保持。
  XML outputs/validation_runtime/devx015-formal-profile-admission-v01-race-20260913-v9.xml
  SHA 9905562286011fa08cbf6b6601e7ebfbd792afeeb3b07948dc48497ba97c82af。
  现runner SHA 8a6cb30a8e1e6c89e3dff2437480842e425a1b513dcf3455dbc41f3808aa62a7；
  fence SHA f82ba40e3486a28d548096d6aa9001df21952a8f33c3066942cd8cafd5c09a36。
  原128PASS XML SHA dd6fd8b329ab2749dbd5df7a53e8c2dc9735367e1c674496f2057b599c026a59；
  本次Ruff/strict mypy及定向diff-check PASS。保持39/106部分映射，不将profile接缝扩大
  为全部V02/正式closure或迁移发布验收。下一步升级旧synthetic发布链fixture，并补核
  LOCAL_MAIN_FF_PRE之后到实际remote发布前的证据变化与完整readiness/身份要求。
  `D:/Work/devx015-pub10`预登记为旧发布链normal单例诊断的独立保留basetemp；
  归属DEVX-015，创建前验证不存在，原Git/结果/拒绝证据保持，最终C归档和无依赖审计后清理。
- pub10旧normal发布链实际1FAIL/27.52s：旧publication_checkout未包含正式runner，
  新入口PUBLICATION_FULL_CLOSURE_INVALID在缺scripts/run_validation_tier.py时拒绝。
  这是真实待升级fixture，不是新机制baseline red或应绕过的生产检查。原tiny Job/commitment
  已终态，11:39:29Z通过公开publication CLI正常failed release到FAILED/RELEASED；
  原repo/summary/结果/事件/拒绝XML保留。下一步须在candidate冻结前提供真实canonical任务、
  runner与profile/mandatory链，再复测全部P02和其他发布消费者；禁止用profile检查mock、
  伪造inspector PASS或legacy布尔恢复旧synthetic发布资格。
- 旧P02发布链已改为full-profile-publish真实canonical fixture，复用抽取的
  _run_actual_profile_full（原profile用例同用此真实链）；16个native Job/xdist文件、
  完整profile/mandatory、原claim/commitment与summary库存由实际runner产生。
  远端probe有限barrier在原fixture acquire和candidate冻结前写入；原网络探针不替换。
  去掉手写Markdown任务状态/合成Full summary，保留bare Git、actual Trace2 push计数、
  原全部8个远端状态及closeout原断言。先运行正常发布与原profile2对照；尚未宣称通过。
  `D:/Work/devx015-pub11`为DEVX-015下一独立保留basetemp，创建前确认不存在，
  保留原Git/Full证据/真实push trace；最终C证据归档、无进程依赖和唯一内容审计后治理清理。
- pub11实际3PASS/1teardown ERROR/135.12s：真实normal发布、profile正反与原篡改断言
  均通过；唯一错误是通用fixture在COMPLETED/RELEASED后仍请求failed replay，既有
  PUBLICATION_TERMINAL_REPLAY_INVALID正确拒绝。仅将fixture finally按原终态选择
  completed/failed重放，仍调用原release校验，不改生产结果/回执或隐藏错误。
  P02四个已有映射节点同步为新版-full-profile-publish参数身份，仍39/106部分映射；
  下一运行完整fence文件，原所有远端/P02/恢复/竞争断言保持。
  `D:/Work/devx015-pub12`预登记为本任务下一独立保留basetemp，创建前验证不存在；
  原始Git/profile/summary/push trace及回执保留，最终C归档与无依赖/唯一内容审计后清理。
- pub12完整fence实际31PASS/3FAIL/712.63s，原session35082已终态exit1。
  两个probe用例在前置remote-observe触发fixture无条件stdin barrier，而非目标并发断言；
  另一个terminal projection用例仍把旧Path传入已升级tuple fixture。均为fixture适配错误，
  不计机制red。仅将冻结barrier限定到真实CLEANUP_PRE checkpoint CLI，抽取共享真实发布
  helper并升级terminal用例到同一canonical/Full/remote链；原生产SUT与目标断言保持。
  下一独立basetemp为D:/Work/devx015-pub13，创建前确认不存在；归属DEVX-015，保留原始
  Git/Job/profile/summary/remote/回执证据，最终C归档与无依赖/唯一内容审计后治理清理。
- pub13三处实际复测3PASS/244.90s，原session57317终态exit0；原生产SUT未改。
  pub12四个已有P02映射节点均从原XML逐个核对PASS；完整pub12仍如实保留31PASS/3FAIL，
  不将分批回归改写成单次完整PASS。下一升级completed-task正例，以真实canonical writer
  DONE事件、提交候选、实际Full/profile/mandatory结果进入LOCAL_MAIN_FF_PRE并运行原
  preflight consumer；其余负例尚待同样升级，不把手写Markdown视为正式完成证据。
  D:/Work/devx015-pub14预登记为该正例保留basetemp，创建前确认不存在；归属DEVX-015，
  原始候选/事件/Full/准入证据保留，最终C归档与无依赖/唯一内容审计后治理清理。
- pub14真实正例1FAIL/60.29s，session73478终态exit1；真实canonical DONE、候选提交、
  Full/profile/mandatory与LOCAL_MAIN_FF_PRE全部成功，完整preflight返回PASS但来源误为
  ACTIVE，而不是COMPLETED_VALIDATED_CANDIDATE_INTEGRATION。确认活动生成表仅含
  DEVX-015-MERGE-FIXTURE-OTHER；canonical preflight evaluate_task_registration使用
  task_id in active_task_register子串匹配。这是触达现有I06缺陷的有效red，不能改预期为ACTIVE。
  D:/Work/devx015-pub14/source-evidence保留原preflight与测试bytes；原候选/Full/事件/XML
  同目录保留。下一应通过当前严格canonical权威确定精确task身份和状态，保留原completed
  candidate/phase/role/clean gates，不新增手写Markdown parser或只改单个前缀补丁；必须补
  正反例、原frozen-lane入口回归及最终bundle安装/parity验收。尚未修改生产preflight。
- I06修复实现：将read_canonical_task_at_commit的index/policy/精确fragment校验抽取为同一个
  sealed-task validator；新增read_current_canonical_task，只读当前三类必要原bytes、native
  bounded regular-file及前后byte复核，不读取Markdown/无关task blob，不要求全仓生成freshness。
  preflight实际入口改消费该当前证明；损坏authority报CANONICAL_TASK_AUTHORITY_INVALID，
  只有严格CANONICAL_TASK_NOT_FOUND可进入既有frozen-lane判断。completed原phase/task/C/
  coordinator/clean门禁保持。unit helper改接精确proof；尚待实证，不更新安装副本或验收计数。
  下一basetemp D:/Work/devx015-pub15预登记为真实DONE正例及原frozen-lane/registration回归，
  创建前确认不存在；原Git/claim/profile/准入证据保留，最终C归档与无依赖/唯一内容审计后清理。
- pub15实际44PASS/1FAIL/161.45s，原8390终态exit1；真实completed正例及多数原入口通过，
  唯一原frozen正例因clean checkout将Git LF转换为CRLF，被新增当前raw读取拒绝。
  不放宽canonical raw校验：clean audited checkout复用exact当前Git reader，dirty lane
  才读取当前未提交权威；这保留原合法checkout转换和当前task优先规则。补六项真实入口：
  未提交ACTIVE/DONE、前缀、仅旧Markdown、损坏index/fragment。两个受改模块strict mypy
  follow-imports=silent现PASS（七处本文件类型问题已修，无ignore）；不宣称其他依赖通过，
  原默认follow-imports完整检查43errors/10files保留为待最终项目检查诊断，不自动改无关模块。
  D:/Work/devx015-pub16预登记为下一真实入口回归保留basetemp；创建前验证不存在，
  归属、原证据保留与最终C后无依赖/唯一内容治理清理条件同pub15。
- pub16实际48PASS/3FAIL/233.77s，原65672终态exit1；原frozen与真实completed均通过。
  新ACTIVE用例默认fixture缺公开publication validator；两个损坏用例追加空行先被真实
  diff audit拒绝，尚未到目标canonical断言。仅改为完整runtime fixture及非canonical注释
  故障字节，保留audit与原断言。新增实际native reader读取fragment后真实改写index、
  再检查CANONICAL_CURRENT_INPUT_DRIFT并恢复原bytes的竞态；不mock证明/PASS。
  原真实工作区新canonical preflight已只读PASS，source_view=CURRENT_WORKTREE。
  D:/Work/devx015-pub17预登记为七项新current身份/竞态与完整task-source回归保留basetemp；
  创建前确认不存在，归属/证据保留/最终C后无依赖与唯一内容治理清理条件不变。
- pub17实际42PASS/122.09s，56011终态exit0，含七项current身份/真实读中变化与完整
  task-source测试文件。XML SHA4cf8a4f22dd2a232eeffbe5cd95c666bb0905f2c688cbf2c6b7e0bce70267cbc。
  当前canonical reader SHA52db269e058f81e16fca1a113dc72a9c8ee86000cd3b20ab7d9126a4e0753c90；
  preflight SHA71c40c215360c48b51612982e68ced97800b5000cf058d21267f139298ff4830。
  skill-creator quick_validate结构PASS，Ruff/scoped diff-check PASS；没有更新installed副本。
  原lease通过公开heartbeat正常续至2026-09-13T18:32:06.159403Z，未改生产TTL/新增lease。
- 剩余11个completed负例现在复用抽取的真实DONE/full-profile helper，不再使用旧synthetic
  Full summary/回拨两天时间。除有意停在FORMAL_RESULT的phase场景外，每例先以真实完整
  preflight证明COMPLETED_VALIDATED_CANDIDATE_INTEGRATION PASS，再注入原故障。
  expired仅自己的临时fixture在acquire/C之前冻结TTL180秒、heartbeat30秒，然后读取
  原expires_at并真实等待到期；原项目配置与时钟不变。tampered保留原损坏事务，原release
  明确PUBLICATION_REPLAY_INVALID后只经同一guard释放已终态的已知执行lease，不造回执。
  D:/Work/devx015-pub18预登记为完整governed skill回归保留basetemp，创建前确认不存在；
  保留每个原canonical/C/Full/profile/拒绝证据，最终C归档与无依赖/唯一内容审计后治理清理。
- pub18完整skill文件实际105PASS/4FAIL/2ERROR/1106.16s，原5444终态exit1。
  真实expired及其他新Full/profile准入场景通过；dirty故障write_text默认CRLF被Git视为
  trailing whitespace，先触发audit；dirty/audit的src/a.py不在新fixture lease声明中，
  guard已真实RELEASED但抛CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED，fence未写FAILED终态。
  22:02JST经原各自公开CLI failed release完成两事务FAILED/RELEASED；前置复核storePASS、
  leaseRELEASED、executionRESULT_RECORDED，无重派发、未恢复或删除故障source原bytes。
  三个旧snapshot资源单元仍使用虚构SHA及无canonical仓库，导致子进程落到editable旧main
  缺read_canonical_task_at_commit；这不是机制baseline red。仅修测试：pubfixture声明src/a，
  dirty/audit显式LF；资源单元用原真实admission_checkout+真实canonical提交/HEAD/audit，
  移除repository-scope mock，仅保留本单元目的所需的deterministic lease资源输入。
  生产SUT不变，Ruff/scoped diff-check PASS；完整pub18保留原失败，后续不能改称单次全绿。
  D:/Work/devx015-pub19预登记为dirty/audit与四个资源策略单元的六项定向复测保留basetemp；
  创建前确认不存在，归属/原证据保留及最终C后无依赖/唯一内容治理清理条件保持。
- pub19六项实际6PASS/146.85s，原45449终态exit0；dirty/audit各先经真实DONE/C/Full/profile
  完整预检PASS后分别正确拒绝，终态清理通过；四个资源单元使用实际Git/identity/canonical读取，
  只有待测lease资源输入为单元fixture。原pub18全部105PASS及错误仍保留，不能伪称单次全绿。
  下一对I06各入口既有测试加强typed拒绝码/只读副作用断言，再审实际覆盖映射；profile fixture
  明确不是whole Full readiness证明，不能提前宣称valid completed正式接受或全部V02完成。
  原所有106变体/10mutants、S1-S5、final C正式验证/迁移发布及OPS-080要求不变。
- I06入口回归v20实际12PASS/78.40s，原11049终态exit0，XML
  `outputs/validation_runtime/devx015-i06-entry-regression-20260913-v20.xml`，SHA
  `563510fd0c2465cbd6ab2279cf6cd38edac08fc995b22d1b7d858b00cd0d8ed4`。
  这是未改SUT的既有5种拒绝、authority持续性和6种Full任务准入回归，不新增验收映射。
  下一测试修订对authority/structure两个实际入口分别先证明有效authority，再检查6种错误码，
  包含canonical DONE但恢复旧ACTIVE Markdown；拒绝前后独立比较HEAD/refs/index、两task
  fragment/index/policy/scope/views原bytes、outputs、publication/store replay及无execution。
  不替代公开命令整链或whole Full readiness。D:/Work/devx015-i06-entry-v20保留原证据，
  新D:/Work/devx015-i06-entry-v21创建前确认不存在，用于加强断言后的定向复测；两者归属
  DEVX-015，最终C不可变归档校验和无进程/唯一内容审计后才治理清理，不复用basetemp。
- v21实际13PASS/6FAIL/96.06s，原74220终态exit1；六项失败仅测试常量未包含异常构造器
  固定WORKFLOW_前缀，不是生产有效red。原XML SHA
  `fd635824bdfdffb2bf3e9428bb47f702bfa1e7117f4503c26c39a917fa874d00`保留。
  修正三种精确code后v22两个workflow入口全部12PASS/73.40s，原17286终态exit0，XML SHA
  `fddbd6ccb32877f98718c60f5571cad8dae0f440a14dfcf090da52b07cc1c95d`。
  D:/Work/devx015-i06-entry-v22保留原fixture/证据，适用前述归档与无依赖治理清理条件。
  39/106映射不变；上述reader不独立证明全部公开准入或whole Full readiness。
- 下一验证原V02发布窗口：真实16worker/Job/profile经过LOCAL_MAIN_FF_PRE并实际ff到fixture
  main后，仅改原profile raw bytes，公开REMOTE_PUSH_PRE应拒绝而无新事件、lease、refs/index
  或远端push副作用；正常对照应通过。D:/Work/devx015-remote-profile-v23创建前确认不存在，
  预登记为DEVX-015该窗口原始Git/C/Full与SUT/test/raw结果保留目录；仅本地bare remote，
  不执行项目远端/OPS操作。结果先留存再决定修复，不把profile证明扩大为整个readiness。
- v23真实1PASS/1FAIL/113.25s，原20837终态exit1；正常对照成功，profile改raw bytes后公开
  REMOTE_PUSH_PRE仍exit0，为触达发布缺口的有效red。XML SHA
  `0b1c58f5ece91b93429d56f7e6b8c4db467deb33d4fe36e5a4f7d6d0f86444b9`，原fixture
  source-evidence保存修复前fence/runner/test和CLI stdout/stderr；技术PASS不改写、实际push=0。
  修复复用原只读profile inspector及原atomic的event/execution/raw captures复核，扩至远端
  准入；定位唯一原FORMAL_VALIDATION_RESULT而非误读最新LOCAL事件。显式validation_tier
  在LOCAL_MAIN_FF_PRE可重验V(C)，不授予新dispatch，普通validate/main门禁和远端main=HEAD=C
  及ancestry不变。D:/Work/devx015-remote-profile-v24创建前不存在，预登记为修复后原两case
  及原profile边界回归保留目录；归档/清理条件不变，完整readiness和原整体验收仍未完成。
- v24同一5145终态exit0，4PASS/257.66s：新remote正常对照与raw profile漂移拒绝、原local
  formal/filtered及capture-check竞态回归通过。两生产module strict mypy（follow-imports=silent）、
  Ruff和定向diff-check PASS；system_flow保持同一paragraph结构更新，RCF目标seal同步，
  最终generated refresh仍待自然边界。D:/Work/devx015-remote-profile-regression-v25创建前
  确认不存在，预登记完整validation runner与实际normal/remote-divergence发布路径回归；
  原证据保留与归档清理条件不变，不增加39/106映射或宣称whole readiness/最终验收。
- v25原76619实际终态exit0，104PASS/173.25s，独立XML复核104 tests、0 failure/error/skip。
  完整tests/test_validation_tier_script.py及实际normal/before-push-divergence发布链通过；
  XML `outputs/validation_runtime/devx015-remote-profile-regression-20260913-v25.xml` SHA
  `bb58db1724f52ad0d8ee7070686983d2a0aa39b2670278e21c739fc76f49fd98`。
  v24 XML SHA `03c980213ab73fa87ec274756aa6160881d52558b8e3ffd5abc6064d2663c2a8`。
  本修复fence SHA `77f7bc561d38f3f5419e10781585fae3e532205da979fbc6de435a12e8aa42d9`、
  runner SHA `8648e0ef081f887ffb5eb4a129c02a1b5fc4787d7ff4ae90d7516c45b391a35d`；
  同一运行期间冻结SUT/tests/manifest。原v23有效red不改写。下个重点仍是完整Full readiness、
  实际loaded SUT/dependency/env和原剩余矩阵、mutants、final C验证/迁移发布及OPS080全链，
  不把profile-only合法对照当作whole正式验收；原项目尚未发布/部署或启动Pi/SoL-Pi。
- Whole readiness夹具构造：复用source-job真实runtime/四生成器输入夹具，新增full-readiness
  模式；在初始Git/租约/C冻结前添加原inspector/runner和明确synthetic retained-policy/raw
  文件依赖，实际commit后运行公开validation_readiness.py的全部七检查器，无checker/identity
  monkeypatch。该模式尚需构造真实Atlas/current bundle，当前构造检查明确预期BLOCKED，
  不作为whole readiness或生产有效red；先检查其余实际输入是否齐备，再完成Atlas正例。
  D:/Work/devx015-readiness-fixture-v26预登记为该工程fixture/原检查结果保留目录，创建前
  确认不存在；无真实研究/DQ/Full/push/OPS动作，不替代源码worktree。最终C证据归档验证与
  无依赖/唯一内容审计后按原规则治理清理。生产SUT与39/106部分映射保持不变。
- v26原23401终态exit0，构造检查1PASS/94.50s；原公开readiness输出仍为BLOCKED，不能
  把pytest构造检查PASS解释成readiness PASS。真实fixture C为
  `2887589368aa49ff3791ac340219949f552c1f73`，inspection_code_root=target_root；candidate、
  retained evidence（5个原bytes绑定）、canonical tasks（4任务）、architecture、report-flow、
  compatibility六项PASS。唯一Atlas blocker为PAGE_EFFECTIVENESS_POLICY_MAPPING_REQUIRED:policy。
  原输出保留于v26/popen-gw0/test_whole_readiness_fixture_u0/actual-readiness.json，XML SHA
  `c0f318181841c4164a3c43e3748ea144a619ca3460746c72cddf2282920eafb0`；Ruff/定向diff检查通过。
  下一补齐实际Atlas policy/task coverage/current+comparison snapshot/diff/reader_state/HTML和
  既有研究静态证据依赖，再取得完整七项正例并接入正式闭合门禁。不得修改研究政策来适配小
  fixture、复用旧C的页面hash或替换聚合checker。当前进度不替代原Full身份、矩阵、mutants、
  final C验收/迁移发布及OPS080；未新增发布/部署/业务或交易动作。
- Atlas fixture输入构造采用固定当前source HEAD的Git原bytes：现有Atlas政策/明确source路径、
  requirement、受源index绑定的完整task fragments与原静态证据不改写；仅在隔离fixture初态
  与工程任务合成新的index/views，并经原canonical validator重验。不是给原项目复制历史事件、
  复活终态任务或生成研究授权。保留source commit/index SHA/每文件原SHA的输入capsule，
  最多512文件/64MiB；禁止路径在读取前拒绝，无outputs目录扫描。Git未保存的文件仅接受
  原冻结政策明确SHA绑定的单文件读取，并重核SHA；无下载、研究/DQ或broker动作。
  fixture C提交后调用原render_atlas_strategy_research_page.py --exact-commit C，再由原公开
  readiness执行七检查器；不重标旧页面。D:/Work/devx015-readiness-atlas-v27创建前确认不存在，
  预登记为这次正例构造和原始Git/C/capsule/render/readiness结果保留目录，原归档清理条件不变。
  尚未宣称正例通过；失败若为样本缺失则按fixture构造错误处理，不冒充生产有效red。
- v27原9254已终态，XML为1 setup error/73.359s：冻结TRADING-2564 requirement启用
  真实S2a compatibility section，但夹具缺少其引用的test_named_immutable_publication.py。
  补入S2a缺少的两份原Git测试输入，并明确full-readiness应生成6个兼容片段（其他模式仍5个）。
  不修改生产checker或研究政策。D:/Work/devx015-readiness-atlas-v28确认不存在后预登记为
  修正后的同一正例构造保留目录；保留v27原错误，原证据归档、无依赖及唯一内容清理条件不变。
- v28原29490终态exit1/1 setup error/74.532s：TRADING-2560 section进一步调用2564
  的完整历史源依赖链，缺named_data_quality_execution_sources_v1.json。停止逐文件试错，
  改为读取冻结Git compatibility index所绑定的2560片段的完整superseded_live_source_paths，
  一次补齐缺失的明确输入；原生成index/片段仅作capsule元数据，不安装为新权威。真实builder
  和validator仍重建/验证所有启用片段，其他fixture模式仍要求5片段；不关闭新启用section。
  D:/Work/devx015-readiness-atlas-v29用于该完整清单修复的隔离验证，创建前核对不存在，
  保留原错误与全部capsule/输出，退出仍受原最终证据归档与无依赖/唯一内容审计约束。
- v29原21630终态exit1/1 setup error/76.01s：独立启用的S2b有14个源输入不属于
  2560最终继承列表。已静态对照全部10个冻结2564/2560 section的源集合，改为其明确并集，
  而非只用最后section；不扩张到无关兼容section。D:/Work/devx015-readiness-atlas-v30
  预登记为此次并集修正测试目录，创建前确认不存在，保留/退出规则同前；原失败不改写。
- v30原16442终态exit1/1 failure/99.40s，完整fixture/canonical/compatibility已通过并进入
  原Atlas renderer；旧事件occurred_at为空，需要base_commit元数据，但隔离Git无该对象。
  仅对明确事件绑定的40位commit执行原repo cat-file commit，并以hash-object -t commit
  在fixture保存原对象，重核同SHA；不遍历/复制trees/blobs，不建立历史refs，不修改事件时间。
  capsule记录原对象SHA256，最多512个且单对象1MiB。D:/Work/devx015-readiness-atlas-v31
  预登记为该修复的隔离验证目录，创建前确认不存在，保留与退出条件同前。
- v31原93731终态exit1/1 failure/99.61s；原commit对象确实存在，直接Git诊断证明
  show -s还解析其直接parent 873ad74c，但该元数据不在fixture。补齐明确commit的直接parents
  原对象，仍不递归历史/trees/blobs，不修改refs或浅边界。D:/Work/devx015-readiness-atlas-v32
  预登记为该Git元数据依赖修正目录，创建前确认不存在，保留退出规则同前。
- v32原3098终态exit1/1 failure/94.96s。直接Git诊断表明parent已存在，当前错误为缺tree
  ccd2b754；改为对明确commit及直接parent补齐目录tree元数据（Git ls-tree/cat-file tree），
  最多4096个单对象1MiB，逐对象重核Git SHA/capsule SHA256。绝不读取/复制blob内容；
  排除文件的目录项元数据不是其文件内容。D:/Work/devx015-readiness-atlas-v33预登记为
  完整Git元数据依赖验证目录，创建前确认不存在，原冻结与保留退出条件不变。
- v33原76500终态exit1/1 failure/197.75s；真实Atlas render成功，新C
  613dfc063162adeb69d14a5cbc89a29884cf13ac的Atlas CURRENT/PASS、canonical97任务及
  candidate/architecture/report-flow/compatibility均PASS。唯一retained blocker为原O1政策
  引用outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json
  缺失；其他33依赖已验证。按原政策精确SHA补该文件，不执行DQ或改政策。
  公开readiness stdout/stderr改为断言前保存，以完整保留失败结果。D:/Work/devx015-readiness-atlas-v34
  预登记为该最后已知输入修正的隔离验证，创建前确认不存在，原冻结/证据保留/退出条件不变。
- v34原68351终态exit0，XML独立复核1PASS/0errors/0failures/0skip/194.263s。
  原公开readiness对fixture C `ef157d929629f94178726e03f8a96295e9fbcaea`七项全部PASS，
  full_dispatch_ready=true、blockers=[]：retained34、canonical97、Atlas CURRENT，真实
  architecture/report-flow/compatibility校验通过；refs/index未改变，dispatch/DQ/artifact writes为false。
  XML SHA256 `8c0cf1721d3738f0875e7d867e0c9fe9e69bf02e69a70167301c9b38d8f9a519`，
  原stdout保留v34/popen-gw0/test_whole_readiness_fixture_u0/actual-readiness.json。
  Ruff与定向diff检查通过。该正例仅是完整readiness夹具，不是原项目final C/Full/106变体或
  整体验收。下一步接入实际Full执行身份/发布闭合的正反例，继续原矩阵、10mutants、最终
  required tiers/实际Full/迁移发布，再完成OPS080工程部署与新的合法daily；无Pi/SoL-Pi。
- 下一whole Full链正例复用full-readiness-profile隔离模式：冻结真实政策/静态研究证据，
  样本tests仅作为fixture工程源，在新C前明确转换为每文件实际Windows Job探针，记录原输入SHA
  与执行SHA的executed-engineering-probes.json。不是原研究tests/106变体oracle，不执行研究。
  官方architecture builder按全部实际探针文件产生完整test manifest，不缩写为16文件或deselect；
  duration profile覆盖完整清单，真实兼容生成、提交C、新C Atlas及公开七检查器PASS后，将原
  readiness结果绑定到原生Full Job/profile/mandatory机制链，核对16workers与每文件witness。
  该正例用于后续闭合拒绝反证，尚不代表生产验收；生产SUT/原39/106映射不变。
  D:/Work/devx015-whole-full-chain-v35创建前确认不存在，预登记为本次机制链隔离验证、
  原结果/probe转换清单/driver保留目录；原证据归档与无进程/唯一内容审计后治理清理，不复用
  旧验证目录或新建源码worktree。同一次运行冻结SUT/tests/manifest直到原进程终态。
- v35原71451终态exit1/1 setup error/105.25s，真实报告流验证报RCF_CONSUMER_INVENTORY_STALE：
  探针源码转换发生在原consumer inventory构造之后。修正为所有探针/conftest就绪后再调用
  官方report-flow builder，仍在fixture源Git冻结之前，不改validator。D:/Work/devx015-whole-full-chain-v36
  创建前确认不存在，预登记为重建顺序修正的隔离运行，保留原失败和同一生命周期边界。
- v36原37670终态exit0/1PASS251.47s：新C800acb3c263ed5cf0ac2f202411a01c2507c02dc的
  七项readiness与原生Full Job/完整profile/mandatory机制记录均PASS；每文件探针、16workers、
  execution_validation_identity原readiness绑定与公开profile inspector、LOCAL_MAIN_FF_PRE
  均经真实路径完成，refs未改变。只是隔离机制链正例，不是原项目Full/106 oracle验收。
  下一反证仅在原七项真实PASS之后，从待绑定记录删除atlas_final_binding，保留其余原值；
  执行实际Job/pytest/profile后，公开LOCAL_MAIN_FF_PRE必须拒绝不完整原记录并保持事件不变。
  D:/Work/devx015-whole-full-readiness-red-v37创建前确认不存在，预登记为当前未改SUT的
  目标反证目录；原driver/结果/实际CLI stdout-stderr保留，原冻结与清理退出条件不变。
- v37原57689终态exit1/1failure226.71s，readiness/原生Full/profile均完成，publication CLI
  因测试错用--publication-transaction而未进入目标，分类INVALID（不能算V02有效red）。
  唯一修改为真实checkpoint参数--transaction。下一v38只执行missing-checker目标；先复用
  v36/v37原证据一次核验完整依赖链、官方生成/consumer校验、候选与SUT身份及公开CLI help，
  不重跑正常对照、不新增框架或逐项补文件。该case对应V02.pytest_pass_formal_closure_invalid，
  X05真实CLI/有效目标触达是证据准入条件；通过工程探针不等于原106项完成。
  D:/Work/devx015-whole-full-readiness-red-v38预登记为修正后的原SUT反证目录，创建前确认
  不存在，保留前次原始失败和同样的最终证据归档/无依赖/唯一内容审计退出条件。
- v38原77448终态exit1/1failure252.83s，为有效V02目标red：原七项真实PASS后删掉已绑定
  readiness的Atlas项，真实Job/pytest/profile仍PASS，公开checkpoint却exit0/LOCAL_MAIN_FF_PRE
  放行。C588bdec54fb14f07ee9b094efc3fcabc4ba00318；XML SHA
  1198781912d04799375bf647d1ed4b609c943a5c2a941ae4dd5dc0f2d3570401，原SUT/driver/执行身份/
  public CLI输出和source-harness.py保留于v38。不是参数错误或任意异常冒充目标red。
- 修复在原runner公开publication inspector核验完整原readiness七项/候选/路径/安全标志，
  与同C实际七检查器语义比较，仅忽略观测耗时；原runtime必须等于mandatory worker证据，
  当前interpreter/distribution身份单独重验，不用当前publication env替换原执行env。
  结构/raw/profile廉价拒绝先行，再运行真实七检查器，仍无派发或第二authority。已更新flow。
  现有profile/pub fixtures升级到真实readiness输入与完整工程探针清单，不保留marker绕过。
  尚未声明完整loaded-dependency bytes或readiness输入观察至atomic间的完整捕获已闭合；
  V02其他变体/V03竞态及最终矩阵仍须原要求验证。
  v39先验证修复后的新正反例与完整runner单测文件（对应V02目标和公开入口回归），不重复
  前序fixture构造失败；旧profile/publish模式后续按受影响路径补最小回归。D:/Work/devx015-whole-full-readiness-fix-v39
  创建前确认不存在，预登记为修复结果/原执行证据目录，原冻结/归档/无依赖清理条件不变。
- v39原84726终态exit0，独立XML104PASS/0failure/0error/0skip/565.389s；包括完整runner
  单测102项及真实whole-readiness正反例2项。XML SHA
  5d3378af50c5cd475730f10ba264b3a22f75287b39cd244b17d12c179cdafc22。
  V02.pytest_pass_formal_closure_invalid已将v38有效red和v39修复正反例对应到原矩阵node映射，
  映射40/106，仍PARTIAL_NOT_ACCEPTANCE_READY、正式execution_state仍NOT_EXECUTED。
  这是具体缺失readiness检查导致正式闭合无效的行为证据，不宣称所有V02变体、统一counter/
  完整runtime/输入竞态或原项目整体验收完成。旧profile/pub模式的最小受影响回归已collect-only
  精确确认2节点：原formal profile tamper/race链和normal真实bare-remote发布收口链。
  D:/Work/devx015-whole-full-regression-v40创建前确认不存在，预登记为这两项回归和原始结果
  保留目录；不重复完整runner单测，原冻结、最终证据归档及无依赖/唯一内容清理条件不变。
- v40原60096终态exit0，2PASS/392.88s：升级后的原formal profile测试实际拒绝raw profile、
  execution identity及真实检查后替换竞态；normal bare-remote链完成原Full→LOCAL→实际普通
  push→确认→收口。两项均使用真实七项readiness，未保留profile-only marker绕过；原104PASS
  不重复执行。V02目标修复与受影响路径回归成立，仍不是原项目final C验证/发布或OPS080验收。
  下一从原V02/V03尚缺的完整loaded-SUT/dependency、输入观察到atomic捕获与验证后替换证据
  继续，补原66项node映射及统一计数/10mutants；不扩建fixture平台，最终required tiers/Full/
  迁移发布及OPS080工程部署和新合法daily全部保留。
- 下一目标为V03.replace_after_validate及V02.inputs_policy_generators_changed中的真实runtime
  输入竞态：复用已通过v40的full-profile完整输入/Job/生成链，原公开inspection实际PASS后，
  仅在进入原atomic重验前替换明确被Git忽略的O1 gate或Atlas index.html原bytes；其他输入、
  原Full记录/C/refs/index不动。要求typed拒绝且无heartbeat/event/Full记录变化，随后还原该
  fixture输入以走原失败释放出口；原inspection/输入前后SHA/driver与事件数量保留。
  barrier只插入真实probe与原atomic之间，不替换任何checker/身份结果。新夹具依赖未增加，
  原policy路径和公开入口已一次核对；v41只先运行retained目标red，不重复旧正常对照。
  D:/Work/devx015-readiness-atomic-red-v41创建前确认不存在，预登记为该原SUT反证目录，
  原冻结/归档/无进程与唯一内容清理条件不变。生产SUT暂不改，尚不宣称有效red。
- v41原55039终态1FAIL/297.91s，为有效反证：真实inspection PASS后替换被忽略O1 gate，
  原atomic错误允许LOCAL_MAIN_FF_PRE，event从8变9；C d6beb9064df8591e028e33bf838d5351117b5463，
  XML SHA88ecb8919b6d43082a7bec446fe3a04ca0c9afc95fb19f0ab10c45e7a9af800d。
  修复复用原readiness有限依赖adapter与原captures/atomic重验：在实际七checker replay前捕获
  retained原policy/绑定文件/receipt链及Atlas manifest/outputs原bytes，重复捕获拒绝漂移；
  REMOTE_PUSH_PRE锁内也重验clean C。未扩建store或替代readiness，不宣称完整加载依赖已闭合。
  v42仅运行retained与Atlas两项竞态及原正常bare-remote收口回归；
  D:/Work/devx015-readiness-atomic-fix-v42预登记为修复证据目录，运行前核对不存在；
  原冻结、证据归档、无依赖和唯一内容审计后清理条件保持。
- v42原70933终态exit0，XML10PASS/0fail/0error/0skip/3078.586s，SHA
  682f398f8d6a1b9d77bf3f74e619f5c002a16bfb1dad74bd7b745ee75dd5e82b。
  选择范围纠正：命令未限定normal完整参数ID，实际为2个输入竞态加8个原remote变体，
  并非此前预记的3节点；发现后保留有效原执行、不取消重启，后续只用精确节点ID。
  retained/Atlas均实际probe1、事件8→8、仍FORMAL_VALIDATION_RESULT，原lease/refs/index/
  Full记录无改变；正常发布与原远端异常/恢复链也全部通过。runner SHA
  55fa0c160a49d49a7692091112ff9b2d6ddafff6b14adebb72f4550892d12807，fence SHA
  dfe6dd72147cf3033f235dd8162773362aedb3c55204c2bafbc9ed44d4b7631f。
  V03.replace_after_validate登记原有效节点，映射41/106仍PARTIAL_NOT_ACCEPTANCE_READY/
  NOT_EXECUTED；统一counter、完整V02输入/loaded dependency与最终C验收不由这次局部PASS代替。
  下一按原L02四变体补活租约路径归属：现acquire前后和release仅认可调用者自身claims，
  不认可同checkout其他活owner。复用原lease-intent/resource绑定，覆盖实际写脏后第二acquire、
  交错release、完整preflight及原短arbiter下index操作；终态/过期/source-only/错checkout的
  旧claims不得授权当前bytes。不扩建第二归属store或queue；原V02剩余工作继续保留。
- L02首个目标反证使用现有small真实Git guard fixture与两个独立Python进程，无准入mock：
  A通过原guard acquire并写a后等候，B原guard acquire/write不相交b；随后A只提交a、release，
  B只提交b、release，最终Git保留两者且无活lease。A提交时B仍dirty，检验实际交错释放。
  原driver/各PID准入结果保存；该初步反证不代替完整preflight或index并发变体。
  D:/Work/devx015-l02-live-owner-red-v43预登记为原SUT目标反证目录，运行前确认不存在；
  终态前冻结SUT/tests/manifest，最终原始证据归档、无进程/唯一内容审计后再清理。
- v43原70053终态1FAIL/13.67s，为有效L02反证：A PID73016取得原lease并写a，B PID87448
  因CHECKOUT_DIRTY_UNATTRIBUTED:src/a.py被拒绝，目标PASS断言失败。原XML SHA
  d72b6708f08c40d43ec910a9e48a2a7a7a4acd64a666f0b7a1102453db291dc1；原driver/harness/
  checkout_guard源码另存原v43目录（原SUT SHA12bbdf60d2f17603058f437ba8a1c984f3b32825d46e9526f6460a3d8b32ee39）。
  修复在原arbiter下读取当前lease replay，只将未过期、同checkout、原intent/resource绑定有效的
  ordinary mutation活owner用于dirty归属；不扩张调用者自己的claims，原resource conflict不变。
  acquire前后及release均复用同一有限归属检查，source-only/终态/错checkout不授予归属。
  v44先执行完整guard测试文件作为受影响回归，D:/Work/devx015-l02-live-owner-fix-v44为
  该修复结果保留目录，运行前确认不存在；原冻结/归档/无依赖审计清理条件不变。
- v44原53245终态33PASS/1FAIL/66.52s，XML SHA
  44bc57d1c70e156920fa0c949bfabd4a88f8fe00e0decce38bd2743891779675。
  真实双PID链A57180/B88392均准入并完成精确提交/交错释放，原valid red目标修复成立；
  released/expired/corrupt-intent/overlap拒绝及原guard回归通过。新增source-only负例因fixture
  错用DOMAIN/owned形式触发CHECKOUT_SOURCE_ONLY_SCOPE，未到目标，属INVALID而非有效red。
  按原合同改用SHARED、空owned及排序shared含固定checkpoint runtime；同时仅修正一处行长。
  v45只重跑该完整参数节点，D:/Work/devx015-l02-source-profile-fix-v45预登记为纠正后证据目录，
  原生命周期保持，不重跑其余33项；完整L02 preflight/index并发及同host其他checkout证据仍开放。
- v45原83994终态1PASS/12.50s，XML SHA
  06791bc5559cbc674e5b19afb3689cbddb9995a23cf0a7ae89e24a7c30a88f06。
  合法source-only原lease不能为另一ordinary写入者背书，目标拒绝已触达；v44其余33项不重跑。
  随后只调整长行布局，Ruff/strict guard mypy/scoped diff-check全部PASS。guard当前SHA
  e65d833ba419c0286db294f1d23a5f48f37b770cd1ec1e6abcc1347002f095b3。
  L02.write_dirty_then_second_acquire/interleaved_release仅登记已验证双PID节点，现43/106映射，
  仍非43项正式验收。下一完成原L02.full_preflight/checkout_index_serialization及同host
  wrong-checkout拒绝证据，原V02 loaded身份/63未映射、统一计数10mutants、最终C全部tiers/
  Full/迁移发布及OPS080独立工程部署新合法daily保持；无Pi/SoL-Pi。
- L02.full_preflight继续复用已有admission_checkout、原canonical片段/index生成helper及真实
  origin/sentinels/guard CLI，不进入Full/Atlas夹具。先实际公开preflight验证clean canonical
  输入，再A/B acquire/write不相交路径并分别完整preflight；遗漏活lease IDs必须拒绝。
  额外创建明确未认领src/unowned.py时，即使允许原A/B两个lease IDs也必须拒绝；预检不得
  改变lease events、refs/index，fault移除后A/B只提交自己路径并经原guard释放。
  D:/Work/devx015-l02-full-preflight-red-v46预登记为该目标反证目录，创建前确认不存在；
  原SUT脚本/argv/完整原始结果保留，终态前冻结SUT/tests/manifest，原归档及无依赖审计清理条件保持。
- v46原40083终态1FAIL/27.62s，公开clean和A/B双writer预检均先PASS、遗漏IDs正确BLOCKED，
  但加入src/unowned.py后仍错误PASS，目标拒绝断言失败，为有效red。XML SHA
  341b9ec7a6e28cd96dccdca87382dd66c50f2befd0709e0c451df5d5e5fc6366。
  修复canonical preflight：调用者声明之外的dirty只能由当前guard在原arbiter下核验同一活lease
  集合和intent/resource归属；allowed IDs不再等价于无范围dirty放行。无其他active owner时直接
  拒绝，不创建空store；READ_ONLY和自身scope已覆盖路径不增加probe。原source-only拒绝保持。
  v47仅重跑原节点，D:/Work/devx015-l02-full-preflight-fix-v47预登记为修复结果目录，
  原冻结/归档/无依赖审计清理条件保持；installed bundle在验证后按原byte parity流程同步，
  当前不把旧installed preflight结果标记为新合同通过。
- v47原39351终态1PASS/34.62s，XML SHA
  623eccd767be66d30d75827cb358a1a2dcb76f3ffa9a5b9e758060001cf861ef。
  原公开完整预检正反例通过；canonical skill quick_validate PASS，parity目前只差预期修改的
  scripts/preflight.py，尚未同步。补同节点的无active-owner未知dirty拒绝且不创建arbiter的
  独立断言，并与原allowed-snapshot/READ_ONLY能力控制单元回归一起跑v48；
  D:/Work/devx015-l02-preflight-scope-regression-v48预登记，原冻结/归档/无依赖审计清理条件保持。
- v48原3177终态5PASS/55.74s，XML SHA
  ba8e44aeeabd7fb93a6ebc52b58a75e7539ff7c744bca8a3f5b618e3b08508c2。
  无owner的未知dirty拒绝且不创建arbiter；双owner普通路径正常通过、未知dirty仍拒绝；原
  READ_ONLY/source-only能力控制回归保持。L02.full_preflight登记后44/106仅node映射，仍未正式验收。
  canonical/installed quick_validate及完整5文件byte parity通过，preflight SHA
  5bb393d661b94768cade183cbe14fae6a9ce69ea4c933512358d18d010d8b98c。
  同步前installed仍为旧Markdown子串准入版8123a1247366fa927640ae48ae8d23e5cda267f9c9a7f204c520c56fc7455c91，
  已将包括原验证I06和本次L02的canonical脚本同步，旧副本保存v48/installed-preflight-before-sync.py；
  这不是项目最终发布或整体验收，技能结构/UI/发现规则未变。
- 新installed实际LANE门禁发现原final-20260913-v1遗漏tests/test_validation_tier_script.py。
  显式diff确认56新增/3删除均为本任务原runner类型/Full fixture/输出覆盖回归，不含无关用户改动；
  原bytes SHA721fd4b419612be9deda63da24481afaa386c0d82ee1ade3a0775fc32876f0ab
  保存v48/scope-omission-validation-tier-tests.py。这是原事务scope遗漏事件，未用额外preflight
  参数或手改不可变transaction掩盖。确认原phase TASK_SOURCE_PRE_WRITE、candidate/execution均null后，
  官方failed release先释放lease并报CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED；重新确认RELEASED后
  同一官方release补齐FAILED回执，原terminal event57a73949745f49517555d6a0d5024b8fc904f6007d5b5e4125db94baa244b733。
  不把该事务失败说成Full失败，原无Full claim/执行。
- 当前唯一事务改为outputs/architecture/arch_005_integration_publication_fence/transactions/
  devx-015-workflow-contract-final-20260914-v2/transaction.json，semantic SHA
  e91e2d20d87e9129242b5d5136850ff65a47d1da95f76b0f967ed6522b2ad389，raw SHA
  df2a96b6cde11aed1e15a9ffe1bbea58f39b6a13fe8e34779d8eb82ee9d2b655，
  lease-53952461b7ed8acfcefd。Same S/M/branch/worktree、4 generator order/5 tiers；effective scope
  只新增tests/test_validation_tier_script.py，不增source worktree，不改旧事务/旧证据。
  首次acquire请求重复携带由API自动追加的两个保留resource，PUBLICATION_PATH_DUPLICATE在
  创建事务/lease前拒绝；确认新路径不存在且无active lease后去除重复参数，同一新ID实际acquire。
  原API自动保留两个资源，effective scope逐项diff仅上述一项。新TASK_SOURCE_PRE_WRITE及
  installed SINGLE_LANE LANE preflight PASS后恢复写入。下一仍L02 index并发/同host错checkout实证，
  其余62未映射、统一counter/10mutants/final C全部tiers/Full/迁移发布及OPS080完整验收均保留。
- L02 index实证复用small Git和原guard/store：两个真实PID已取得不相交lease并写入后，
  A持原短arbiter，B实际竞争须LEASE_ARBITER_BUSY；独立Git Trace2的add/commit为零且index/HEAD
  不变。A实际提交释放后B仅重试一次完成，最终两文件bytes和Git命令计数独立核验。
  D:/Work/devx015-l02-index-serialization-v49预登记为该项driver/trace/结果保留目录，创建前
  确认不存在；终态前冻结SUT/tests/manifest，证据归档、无进程/唯一内容审计后治理清理。
  此项不是完整统一counter、10mutants或整体验收；不使用Full/Atlas夹具或新queue/store。
- v49原98246终态exit0，独立XML 1PASS/0fail/error/skip，18.236s，SHA
  b2cdebac275c004f3ac7886e5658fa491b9aadb86de67a78a979e3da7a3e95c0。
  A PID95792实际持arbiter，B PID84352实际LEASE_ARBITER_BUSY，竞争阶段index/HEAD不变且
  Git add/commit计数均0；释放后双方各一次真实add/commit，最终两文件保留且active lease为空。
  driver/observations/逐进程Git Trace2保留v49/popen-gw0。本项不需生产修复、不声称valid red；
  L02四变体均有节点，现45/106映射仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED。
  下一同host受管store跨checkout竞争/错误身份实证，剩余61映射、统一counter/10mutants、
  final C所有required tiers/Full/迁移发布及OPS080工程部署新合法daily仍全部保留。
- L01/L03受管跨进程夹具先验证真实注册表transport：仅测试子进程通过Windows
  RegOverridePredefKey将HKLM映射到唯一HKCU测试键，真实OpenKey/QueryValueEx和原SUT
  Git/物理目录/host解析执行，实际HKLM不写。原fixture construction的transport patch在
  子进程启动前全部撤销。正例解析原共享store，未登记checkout须typed拒绝、无替代store
  和原control文件变化；记录两个独立PID/driver/结果。测试键只按新建精确清单删除并读回不存在。
  D:/Work/devx015-managed-registry-identity-v50创建前确认不存在，预登记为该输入准入/隔离
  机制证据目录；保留原始结果至最终归档、无进程/唯一内容审计后治理清理，运行期间冻结。
  此先决检查不是并发Full/发布、管理员ACL安装或迁移接受，不增加正式variant映射。
- v50原命令终态exit0，XML 1PASS/0fail/error/skip，7.202s，SHA
  18fa36a3dfcfb0b674aa0fd01681abb2cdad9cd62895df1c356f3b377427e8db。
  真实PID54796读取隔离原生注册表并解析已登记checkout/control；PID89668被
  WORKFLOW_CONTROL_HOST_REPOSITORY_NOT_REGISTERED拒绝，原control文件无变化且没有
  fallback store。注册表新建精确键全部删除并readback不存在；外部HKCU测试前缀复查为空。
  原driver/结果保留v50/popen-gw0/test_real_registry_host_reject0，原harness/SUT另存v50。
  两个行长问题在Ruff阶段停止并修正，未启动失败pytest、不当作baseline red。
  下一用同一进程局部原生注册表隔离和实际登记拓扑，批量验证linked publication、linked Full、
  独立受管repo同host Full竞争；scope-holder与实际执行/发布副作用分别核对。45/106映射不变，
  61剩余/统一counter/10mutants/final C/OPS080完整目标不变。
- 原生注册表夹具复用后批量补三种受管guard拓扑：linked-publication、linked-full、
  repositories-full；登记各actual common/checkout物理identity，同一store/state固定实际guard
  派生lease policy。两个独立PID分别原guard acquire，A活lease期间B须resource-conflict且
  没有执行目录；A原Windows Job一次有界子进程完成并释放后，B仅重试一次完成。原子store的
  scope互斥、真实子进程效果及无fallback根分别验证。该有限Job探针不是原项目Full或实际
  ref publication，不能以此单独完成L01全部oracle；后续仍需真实入口的Full/发布副作用计数。
  D:/Work/devx015-native-managed-contention-v51预登记为三拓扑及v50身份回归的证据目录，
  创建前确认不存在，运行期间冻结，原driver/结果/源码保留至最终归档及无依赖/唯一内容审计清理。
- v51原56283终态3FAIL/1PASS17.33s，XML SHA
  48aff71872f5bb565b15d5669cb91ab12d3232c3ba08aad04d1d946f3fea900b。
  三拓扑均已实际REGISTERED、A PASS、B LEASE_RESOURCE_CONFLICT/active1，identity回归通过；
  后续探针Job名错用Local/AITS-native而非原执行器要求Local/AITS-DEVX015-，无stdout/effect，
  属夹具INVALID，不是SUT有效red或完整并发接受。修正名称，补child终态stderr原始留存，
  v52仅重跑三个明确拓扑参数节点，v50身份回归不重复。
  D:/Work/devx015-native-managed-contention-v52预登记为修正后证据目录，创建前确认不存在；
  原冻结、归档、无依赖/唯一内容审计与精确测试注册表清理条件保持。
- v52原1006终态exit0，独立XML3PASS/0fail/error/skip21.924s，SHA
  af608f309daca89bc04d74f3da359aed44e8d221174491269fd174903ef87370。
  linked-publication PID75332/82356、linked-full29592/70808、repositories-full95164/42472，
  均实际同store登记，A PASS active1、B resource-conflict active1且零执行目录；A Job效果/释放，
  B一次重试与Job效果/释放，最终active0。原driver/结果/源码留v52；HKCU测试键清理/readback空。
  这批证明真实guard资源scope与有界Job探针，不声称原Full claim/profile/readiness/发布ref已验收，
  正式映射仍45/106。下一复用该原生隔离拓扑，将原公开fence/Full生命周期入口接入，按实际
  claim/launch/ref或push副作用与独立counter核对L01，不能仅将marker互斥登记为完整case。
- 下一将linked拓扑接原scripts/architecture_arch005_publication_fence.py的真实argv/main：
  原生子进程registry隔离内runpy执行原CLI，不替换parser/guard/identity；复制原三policy到
  合成Git候选，host markers改为实际fence publication.resource与outputs/validation_runtime。
  A真实acquire后B必须PUBLICATION_LEASE_CONFLICT且不产生transaction；A经原CLI failed
  release终结准入探针，B按新attempt ID一次重试并同样终结，原完整CLI stdout/argv保留。
  此为公开准入/有限释放证据，不假称ref发布或Full运行；候选原始完整链仍需后续接入。
  D:/Work/devx015-native-fence-cli-v53预登记为该原入口验证目录，创建前确认不存在；原冻结、
  证据归档/无依赖审计清理及精确HKCU测试键清理条件保持。
- v53原28943终态1PASS/11.64s，XML SHA
  03db65daf4adac88f111bb2d146390db0077ea41ad59380be7eec2cb34d3801c。
  原CLI acquire/typed conflict/failed release/新attempt一次重试/failed release完成。
  补独立原refs/index不变、逐进程GitTrace2零add/commit/update-ref/push及精确CLI次数断言；
  fixture仅fence使用main，guard拓扑显式fixture分支，避免引入protected-main的无关fixture拒绝。
  v54精确执行增强CLI节点和受影响三个guard拓扑，不跑Full/Atlas链；
  D:/Work/devx015-native-cli-counters-v54预登记为这批证据目录，原冻结/归档/精确清理条件保持。
- v54原53138终态exit0，XML4PASS/0fail/error/skip36.366s，SHA
  5880f917de6f972775fd18f93fa4e1bb2b8cd4b8d612ead86046e1f868201ca4。
  真实CLI序列严格acquire/acquire/release/acquire/release，exit codes0/2/0/0/0；输家无事务。
  全部四节点两checkout原refs/index逐字不变，逐PIDGit Trace2无add/commit/update-ref/push；
  guard三拓扑回归、最终无活租约/测试键清理均PASS。依然准入而非发布，45/106映射不变。
  下一直接复用原full-profile-publish完整链，在原fixture首次acquire之前注册隔离host（不复制
  ACTIVE）；原生process-local registry映射须显式传递每个Python参与者，不能假定自动继承。
  以一次完整真实链覆盖受管host的claim/执行/profile/发布闭合，再结合已经独立检验的资源
  scope和竞争oracle；不为每个锁变体重新搭完整Atlas/Full输入，也不把模拟record当真实Full。
- native-full-profile-publish复用原full-profile-publish所有输入/生成链；在canonical验证后、
  首次fence acquire前，以实际guard policy/common/checkout建立隔离host registration，确认
  原store无events/ACTIVE，不复制租约。测试专用固定sitecustomize对每个Python子进程执行
  原生RegOverridePredefKey；原pytest进程也局部映射，缺失测试键或失败立即退出91，不允许
  回退真实HKLM。启动witness仅证明native mapping，不替代原Job/Full执行身份。注册表keys
  仍只在唯一HKCU测试命名空间创建，原fixture释放结束后恢复映射并精确清理；实际HKLM不写。
  新节点复用原normal实际Full→LOCAL→bare push→确认→closeout完整链，并核验唯一带execution
  lease/原host store/无active与子进程native witnesses；不声称管理员ACL/迁移安装验收。
  D:/Work/devx015-native-whole-full-v55预登记为完整链证据目录，创建前确认不存在；运行期间
  冻结SUT/tests/manifest和bootstrap，保留原始失败或成功至最终证据归档/无进程与唯一内容审计清理。
- v55原27853终态exit1/2errors110.56s，XML SHA
  ac65c16f97c7ddf76aafabd8472b763d25ca9ca207961b14fde2f4c3e7cfb9a6。
  host注册/acquire后、canonical子进程import pydantic→asyncio/_overlapped时WinError10044：
  空HKLM重定向发生于Windows socket初始化之前；未触达Full，属夹具INVALID。原fixture已正常
  failed release，lease-8e68796eba202676afb7 RELEASED，candidate null；terminal事件
  5fc216fe1064d75e8cd0ddb456d5480dadc0a082b7121b0afbcf6539a742a3c3。
  socket初始化额外写入仅隔离测试键下System/CurrentControlSet/Services/WinSock2/Parameters
  的NameSpace_Catalog5/Protocol_Catalog9及空catalog分支，已创建的Software分支按原清单删完，
  删除测试根因尚有System分支WinError5。唯一遗留HKCU/Software/
  AITS-DEVX015-Test-1d95886582214e259626f5e079cf3eb3；精确根校验后的递归Remove-Item被工具
  策略拒绝，未绕过，暂保留该可重建测试残留，最终清理需允许的操作或owner手工处理。真实HKLM不写。
  下一先用已有small publication fixture，启动bootstrap在重定向前加载asyncio/socket/ssl和
  localhost resolver，再在映射后实际import pydantic、建socket、解析localhost和原guard/store，
  检查子进程结果与精确键清理；不改SUT身份checker，不直接重跑完整链。
  D:/Work/devx015-native-runtime-bootstrap-v56预登记为该最小启动验证，运行前确认不存在；
  原冻结/归档及无依赖审计清理条件保持，旧v55原始失败不覆盖。
- v56原命令终态1FAIL/8.55s，XML SHA
  d8e8d09212476663a4828c149a495db62ed585169488ab6beaebfe0fecff4555。
  原child实际returncode0/runtimePASS，pydantic/socket/localhost及原guard绑定均通过；
  唯一失败为context退出的测试根删除WinError5，非Full/产品缺陷。说明重定向前初始化修复了
  子进程运行，但OS仍可能在隔离根生成额外目录，原固定Software清单不足以删除测试根。
  新增保留键HKCU/Software/AITS-DEVX015-Test-e3955df6a5ae4ed1b04dbc6ae731460d，连同v55键
  已向owner异步请求仅针对两键的明确清理许可；工具先前拒绝递归registry删除，未换方式绕过。
  暂停新增这类注册表测试和完整native Full重跑，避免新增残留；不是整个DEVX目标阻塞，
  可继续V02已加载SUT/dependency身份等不依赖该环境的剩余工作。最终不得将这两次INVALID
  当Full failure或PASS，清理与真实native整链仍待完成，45/106映射不变。
- V02.loaded SUT反证复用现有真实mandatory runner小型Git夹具，新增loaded_sut_clean及
  loaded_sut_drift两节点使用真实16worker。drift在实际test_required内仅替换已加载本候选的
  integration_publication_fence._identifier.__code__，独立调用前后证明确实改变行为，源码文件
  和原test结果不伪造；正式结果必须FAIL。原driver/stdout/stderr及fault见证保留。
  原门禁仅绑定workflow_execution/workflow_contract/可选runner，尚未覆盖该真实已加载SUT；
  在修改生产实现前运行此正反例，不使用新registry键/Full-Atlas大夹具或任意异常冒充有效red。
  D:/Work/devx015-loaded-sut-red-v57预登记，创建前确认不存在；运行期间冻结SUT/tests/manifest，
  原证据最终归档、无进程/唯一内容审计后治理清理。旧native清理许可仍待owner回复。
- v57原95308终态1PASS/1FAIL29.39s，XML SHA
  1c9a6aa6ddf8a234c2f3cca518bb4df3064877a6d21c0882d777c39d437ce2bc。
  clean真实16worker对照PASS；drift目标是有效red：候选
  e430c19b76382991071cfc17873a7a74d5f0bcad内已加载fence._identifier独立调用由identity-probe
  变incorrect-loaded-body，原runner却exit0/mandatory PASS，非import/API/fixture异常。
  driver/result/fault、原harness及workflow_execution源码已保留v57，不改写原red证据。
  修复范围不能仅添加fence名单：原runner实际import closure只读审计为70个项目Python模块、
  264个有decorator函数、88个property，不能套用原两模块的裸.__code__比较误拒正常descriptor。
  下一构建候选绑定的完整源码inventory与已加载实现校验，支持正常descriptor/wrapper并拒绝
  任意已加载SUT drift；避免每模块启动一个Git子进程。父/worker导入集合可不同，必须区分
  共同候选source inventory和每进程loaded检查，不以集合偶合误拒。完整依赖bytes仍保留要求，
  不把这一有效red或后续单点修复说成V02全部完成。当前生产实现未改，45/106映射不变。
- v58修复验证预登记：D:/Work/devx015-loaded-sut-fix-v58，创建前已确认不存在。
  共同源码承诺改为候选src/scripts普通Python blob完整清单，一次Git tree读取；每进程独立
  核验实际加载的模块与authored function/class/descriptor/wrapper，父worker不要求相同导入集。
  只compile不exec，第三方依赖完整bytes仍未闭环。先运行原loaded_sut_clean/drift两精确node，
  外层及两内层均16workers；Ruff与模块strict mypy follow-imports=silent PASS。
  未限制imports的strict检查另报既有7个依赖模块错误，不伪称整个依赖类型检查PASS。
  运行中冻结SUT/tests/manifest；保留原driver、终态与源码证据，归档/进程及唯一内容审计后清理。
- v58原81853终态2PASS/40.38s；XML SHA
  f1185ce93febe048fb50d577137619599f9f88d6be2936283f8ab61e3bb99a26。
  原真实SUT替换已被拒绝且16worker正常对照仍PASS。源码及harness保留v58，不替代完整V02。
  v59预登记D:/Work/devx015-loaded-descriptors-v59（确认不存在）：同一真实runner夹具增加
  候选内普通probe模块，正常property getter/setter、classmethod、staticmethod、cached_property、
  cache、contextmanager、自定义wraps包装器均真实调用；分别修改property/类方法/包装器代码及
  wrapped绑定，实际断言篡改已生效后必须拒绝。五精确node内外16worker，原SUT不改，运行期冻结。
  原证据归档、无活进程与唯一内容审计后治理清理；不创建registry键，不扩大验收映射声明。
- v59原93965终态5PASS/94.98s，XML SHA
  f8cddd0a6e59b1c0dca806cb20b6ed30495e05d5c38c6bfb60096a1485ec29ba；五项含真实正常调用
  和已生效篡改的目标拒绝，源码/harness保留。v60预登记D:/Work/devx015-loaded-runner-regression-v60
  （确认不存在），仅原18个runner正反例回归，显式列举node，外层16worker；原小fixture内层2及
  既有serial调试保留，不重跑已通过七项新case。运行期冻结；证据归档/无活进程及唯一内容审计后清理。
- v60原46261终态18PASS/219.67s，XML SHA
  ddd3a8eed889acc5d857a507c0742cfc8c4dced8c3f1bb67c02d655fab3dee40；源码/harness保留。
  v61预登记D:/Work/devx015-dependency-red-v61（确认不存在），三个loaded_sut_dependency_精确node：
  clean/disk_drift/code_drift。在每个独立fixture相邻目录创建真实METADATA/RECORD与可导入模块，
  通过原PYTHONPATH传入实际driver/16workers。版本固定1.0，独立断言源码SHA改变或磁盘不变而
  实际函数返回7→99；原生产实现不变，目标要求FAIL。正常对照必须PASS；API/import错误不算red。
  仅修改隔离测试安装，不写共享venv；冻结SUT/tests/manifest，原结果/故障证据保留，归档及进程/
  唯一内容审计后清理。尚未把依赖代码闭合或V02全部验收标为完成。
- v61原18073终态1PASS/2FAIL60.08s，XML SHA
  8f11e71db2d091c3eb56695500a33cb096a0d6d40f7e891221c222d7cf5bb2e7。
  两个有效red：版本固定1.0，磁盘SHA实际改变/内存函数实际7→99且磁盘不变，原runner均exit0
  mandatoryPASS。clean16worker对照PASS，原结果/fault/源码/harness保留v61。
  安装包代码字节修复先加入原runtime identity，RECORD仅供文件名，实际native custody读取并
  哈希Python/原生二进制/导入路径配置/安装元数据；排除运行时生成pyc，不声称全部package data、
  stdlib或内存实现已闭合。拒绝缺inventory、越出安装根/解释器根、非普通文件或超预算；工程上限
  20000文件/单文件64MiB/总512MiB。正常当前环境9485文件309470951bytes，摘要
  de6e8ba07beb29ab5ba59fdb66eeed71cf1a34785bc211b27eee44550b9f7afa。
  首次扫描102.36s；定位每小文件按64MiB分配，100次86byte读取0.797s对实际size预算0.031s。
  调用方按min(lstat size,64MiB)设置预算，原native句柄/大小复核保持、观察后增长拒绝；无mtime缓存。
  重测相同完整摘要9.219s。v62预登记D:/Work/devx015-dependency-disk-fix-v62（此前确认不存在），
  仅原dependency clean/disk_drift两精确node，内外16worker；新增依赖fixture driver防挂起预算300s，
  旧case120s不变。Ruff/模块strict mypy PASS；运行期冻结，证据归档/进程及唯一内容审计后清理。
  dependency_code_drift仍保留有效red待修，不将磁盘字节修复当作V02完成。
- v62原95298终态2PASS/277.67s，XML SHA
  4c6d356efba81cfd64ae32044adfe34cd5c8d8478e86403506be7c328928ba3f。
  正常真实16workers通过，版本不变的依赖磁盘字节漂移被拒绝；原源码/harness保留v62。
  workflow_execution当前SHA06778f49f2cc203d62ddd284efe394c69ec8643331051d397effb46411504657。
  内存实现仍未修。只读正常runner+pytest样本发现8819个外部authored function与其源码compile一致、
  75项不同、852项generated/frozen；不能跳过不匹配或把裸源码compile误拒当反篡改通过。
  具体PyYAML Parser.parse_document_start：实际code与fresh compile仅linetable不同；既有pyc
  header timestamp/size匹配当前源码，实际code与pyc仅co_filename斜杠表示不同。这里只记录
  实测来源差异，未推断cache生产版本；后续须绑定真实cache/source与加载实现，不能信任mtime
  或忽略任意差异。其余stdlib/generated/全部依赖行为仍须分别证明，不扩大已验收声明。
- v63修订前已复核现有事务LANE预检PASS/S不变；当前扩展原runtime identity的捕获字节输入，
  将RECORD内既有pyc也纳入实际SHA，16554文件/441758533bytes，摘要
  bef2f7e92f7a3ed650f1e68581a5d7f3173baf4479cf8d6e419ded7f2e8cbd25。
  共用已加载源码校验器；每进程使用其刚捕获、已与共同runtime承诺比较的source/cache bytes，
  不把大源码数组重复写入17份回执。缓存magic/flags/type验证后仅以独立CodeType副本规范化loader
  文件名表示，不exec缓存；全部指令/常量/flags/名字/异常表与源码compile一致才允许line-table编码
  不同，实际loaded code仍须完整匹配source或已承诺cache。非普通、越界、损坏或不匹配仍拒绝。
  初始正常189模块48项兼容性拒绝，经typing-only overload识别、可确定版本/平台条件、后述生效
  声明覆盖、源码显式删除、真实descriptor/fixture包装及元类引用producer+闭包绑定修订后，
  批量只读诊断189模块零拒绝。没有module skiplist；未知构造不冒充通过。
  原pytest --version/-n16诊断终态0，runtime身份校验10.062s；这不是collection、worker或Full验收。
  仍须补runtime-generated赋值、native/stdlib、原实际实现身份变化及原全部V3接受；不把当前
  authored检查扩称完整堆或全部执行身份证明。
  D:/Work/devx015-dependency-code-fix-v63预登记（创建前已确认不存在）：仅原dependency clean/
  code_drift两精确node，内外16worker，原新fixture300s预算；真实driver/结果/源码保留，运行期间
  冻结SUT/tests/manifest。Ruff/模块strict mypy PASS；归档及活进程/唯一内容审计后治理清理。
- v63原87562终态2FAIL/115.20s，XML SHA
  54c775b32109518109fc0d5e178ea26acd7633ca2af5d36366f0f70253453c9f；这是正常启动误拒，
  非有效red：Config._getini_unknown_type被原legacypath早期hook合法替换，校验却仍要求旧类体；
  两个fixture的executed均未到达，真实driver/traceback/原SUT与harness保留v63。
  修复从已验证pytest_load_initial_conftests源码的直接MonkeyPatch.setattr声明解析精确owner、
  literal字段与实现对象；实现完整匹配相同捕获源码/cache才采用该已声明绑定，非任意文件内替换。
  实际--help配置阶段只读观察另定位os.environ.get条件及AnyIO的AssertionRewritingHook；修订后
  环境条件仅用原进程环境快照，不执行任意source表达式；rewritten模块用原已验证rewrite_asserts
  及其真实loader配置重建AST并精确compile比较，禁止用普通pyc或禁用rewrite代替。
  原--help观察57745终态诊断PASS/17.297s，不是test/worker/Full接受；Ruff通过后类型提示修正。
  catalog在任何外部读取前限制非捕获co_filename只能为解释器Lib内普通.py，不能由伪造code对象
  引导读取任意仓库文件或私人排除路径。没有读取真实私人排除文件。
  v64预登记D:/Work/devx015-dependency-code-regression-v64（确认不存在），重跑原同两精确node，
  内外16worker/原300s预算，运行期冻结，原证据归档及活进程/唯一内容审计后治理清理。
- v64 原78189终态2FAIL/112.88s，XML SHA
  3ae33ecbab5e2792495db80f64df83fde9762e82487553343ba45fecaa46840c。
  controller项目源码校验缺少同次捕获的pytest重写producer源码，启动阶段拒绝
  ACCEPTANCE_REWRITE_PRODUCER_ORIGIN；executed未到达，不是有效故障red。
  修订runtime捕获向原parent/controller/worker及publication inspector传递同次依赖字节，
  项目binder合并捕获上下文并拒绝候选路径字节冲突；不添加producer读取豁免。
  v65预登记D:/Work/devx015-dependency-context-v65：仅原dependency clean/code_drift
  两精确node，内外16worker/原300s预算；运行前确认不存在，运行期间冻结相关源码。
  真实driver/输出及源码保留，归档及活进程/唯一内容审计后治理清理；不代表Full或整体验收。
- v65 原89059终态2PASS/505.22s，XML SHA
  9364eb004977d3e6bc9aac1cc00630883815fee17697e854bca619b9857b11c9。
  clean原runner exit0/PASS/212.99s；code_drift真实executed与fault记录存在，原runner
  exit1/212.91s，以ACCEPTANCE_WORKER_INPUT_CHANGED拒绝。源码、runner和测试harness
  副本保留v65根目录。Ruff、两模块strict mypy和限定路径diff检查通过；本轮关闭该有效red，
  不扩称全部运行时身份、45/106映射验收、10mutants、最终C/Full、发布或OPS-080已完成。
- v66预登记D:/Work/devx015-runner-context-regression-v66：原test_validation_tier_script.py
  全文件回归，外层16worker/loadfile；检查捕获上下文接线对原runner证据与命令边界的影响。
  创建前确认路径不存在；运行时冻结SUT/tests/manifest，原XML和harness保留，归档及唯一内容/
  活进程审计后治理清理。此项不是最终C Full，不运行暂停中的native registry fixture。
- v66原44418终态102PASS/30.24s，XML SHA
  3c75b76afff04393b470842825070c1a7187e5c95fc909a263cbe9860c3d521d。
  原runner全文件回归通过；未把普通单元测试当作完整真实mandatory执行链。
  I06当前canonical六种原fixture加入独立CLI进程，保留原stdout/stderr及精确输入摘要；
  在真实故障写入后冻结index/refs/canonical/transaction/lease和源码，两个入口返回后独立
  对比无变更，不mock身份门禁。v67预登记D:/Work/devx015-i06-canonical-cli-v67：
  原六种current-canonical变体及真实source-only拒绝，外16/loadfile，创建前确认不存在；
  运行期间冻结相关源码与manifest。证据保留，归档及活进程/唯一内容审计后治理清理。
- v67原45620终态7PASS/718.29s，XML SHA
  a25bc17ba9f7aa9a54be1e3aa73f1f926db8748a3176cc980d23ef94b6b6ec5f。
  原始CLI证据与测试harness保留v67；按XML精确node登记I06的id_prefix/stale_markdown/
  current_terminal/corrupt_canonical/source_only五变体，现50/106映射、56未映射。
  PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED保持；wrong_candidate及合法completed candidate
  仍须真实Full准入证据，统一副作用计数、mutants及最终C所有门禁未被本轮替代。
- v68预登记D:/Work/devx015-original-loaded-regression-v68：原actual_runner_chain四个精确
  参数pass、loaded_sut_descriptors_clean、loaded_sut_drift、loaded_sut_dependency_disk_drift。
  外16/loadfile；旧pass保留内2/120s，新loaded_sut组内16且统一既有dependency组300s测试等待
  上限，依据v65真实runner约213s；不降低worker数、不更改生产超时/正式预算或断言，不将超时
  当作有效red。创建前确认不存在，运行期间冻结SUT/tests/manifest，原输出/harness保留，
  归档及活进程/唯一内容审计后治理清理。
- v68原70674终态1FAIL/3PASS/884.99s，XML SHA
  7fb3c07d26b2eeb01fa82a94a394e7775805059eb11b7655da5fba6e4b6cbdaf。
  旧pass真实两worker因外层subprocess.TimeoutExpired(120s)失败，不是有效故障拒绝；
  原candidate f837409aa1d9a87a1f9b3a9cd25b110639a885b0及SUT/runner/harness保留v68。
  descriptors_clean真实runner PASS/211.15s；source drift真实执行后以
  ACCEPTANCE_WORKER_INPUT_CHANGED拒绝/210.82s；dependency disk drift目标用例通过。
  全批终态后确认该时间窗没有残存Python进程，再在保留的原pass候选做只读cProfile采样，
  原30594终态0：runtime18.935s、implementation1.812s、runner2.006s；总22.753s。
  dependency capture18.059s，bounded_regular_bytes16706次/15.615s，lstat185482次/10.691s。
  此为父进程诊断，不是全链性能验收；不据此把cached-code比较当主要原因。
  下一步测量有界并发的相同原native读取，保留排序摘要、路径/字节/累计预算、每次身份与
  reparse校验和异常关闭；禁止mtime缓存、祖先检查跳过或仅提高旧120s等待预算。
- 原98306只读对照终态0：同一原reader/完整16554文件441758533bytes，串行观察12.167s、
  有界四在途10.175s、无profile串行16.348s，摘要均为
  bef2f7e92f7a3ed650f1e68581a5d7f3173baf4479cf8d6e419ded7f2e8cbd25。
  实施原capture内部最多四个Future的有界预读，逐个按原排序消费；不使用eager map或
  无界结果缓存。保持单文件64MiB、累计接受512MiB，最多额外四个单文件预算预读；异常
  时context等待全部自有read线程关闭，每次仍调用原native gate，不改文件锁/路径权限。
  新实现单次完整capture6.828s且同摘要/数量/字节，Ruff与模块strict mypy通过；该采样不能
  替代真实runner。v69预登记D:/Work/devx015-bounded-read-runner-v69：原pass内2/120s及
  dependency clean/code_drift内16/300s三个精确node，外16/loadfile，创建前确认不存在；
  运行期间冻结SUT/tests/manifest，证据保留，归档及活进程/唯一内容审计后治理清理。
- v69原27754终态3PASS/573.97s，XML SHA
  4603d1f08a137b9548f7011c8a19910617d516240f692ecfed6056752c434935。
  旧pass原2worker/120s内通过，实际runner79.33s；16worker正常依赖对照202.29s通过，
  实际内存代码替换仍被拒绝；SUT/harness和原始结果保留v69。
  v70预登记D:/Work/devx015-bounded-read-lifecycle-v70：新增none/missing/oversize三个
  精确capture边界参数，真实临时distribution与原reader，thread profile只观察并设置barrier，
  不替换身份门禁或返回值。正常强制首文件晚于另三次读取完成，独立排序SHA证明不乱序；
  异常观察实际join仍被阻塞的自有reader，证明不提前退出、不继续提交第五个读取。
  原64MiB越界由实际稀疏文件触发，非伪造预算结果；运行前确认路径不存在，外16/loadfile。
  运行期间冻结源码/测试/manifest，证据保留，归档及活进程/唯一内容审计后治理清理。
- v70原命令终态3PASS/6.81s，XML SHA
  f2b1dab20463940935131e32578b39ac42bd0bee453fedd277343a005be560f4。
  none/missing/oversize均到达四个原reader的真实barrier；首文件强制迟完成仍独立摘要一致。
  异常时实际Thread.join进入仍受阻reader，释放barrier后原调用才结束；无在途reader遗留，
  不提交第五次读取，也不返回部分source_inputs。此为新增读取窗口的回归证据，不增加V3
  映射数量或冒充mutant反证；源码/harness保留v70。原正常120s及故障拒绝回归v69已通过。

### 2026-09-22 v297 Full 输入拒绝前置

原 `_run_leased_command` 现于 execution reserve 和证据写入前校验实际环境摘要及传入的 validation identity 摘要；无 request_path 时也不能跳过身份校验。4 个参数化拒绝场景比较原 FileExecutionLeaseStore replay 全状态相等，且不生成 request/stdout/result；原真实进程树退出与同租约续租回归共 7 PASS，13.77s。Ruff 和定向 diff check PASS。证据 outputs/validation_runtime/devx015-v297-prewrite.xml。该结果不增加 V3 验收映射，不代表独立账户或 Full 验收通过；显式 worker token 与显式环境接入仍待完成。

### 2026-09-22 v298-v299 Full worker capability plumbing

WindowsWorkerToken exposes freshly checked SID/elevation/session metadata and validates a distinct launcher principal in the same session. The original Full adapter accepts an in-process live capability paired with an explicit environment; no broker environment is inherited on this path. Worker metadata is included in the original validation identity digest. The original leased command rejects missing/serialized capabilities and unbound identity before reserve, then uses create_as_worker with unchanged reserve/heartbeat/bind/resume/exit/result custody. No CLI token handle, alternate ledger, service installation or host permission changes were introduced.

v298 14 PASS/16.51s; v299 20 PASS/19.36s, covering original native AsUser ABI with modeled broker identity, existing process-tree lifecycle, live token binding copy integrity, incomplete configuration and serialized-token rejection. Ruff, workflow_execution module mypy and scoped diff check PASS. These are prerequisites, not actual distinct-account acceptance. Full adapter success under a genuinely separate account, protected deployed entrypoint, final candidate Full/publication and OPS080 acceptance remain pending. Evidence: outputs/validation_runtime/devx015-v298-worker.xml and devx015-v299-worker.xml.

### 2026-09-22 v300 original Full adapter chain

Expanded the existing real-fence/canonical-lease/summary test across inherited launch and a real restricted token with explicitly modeled broker SID, both exit 0 and exit 7. All four cases PASS in 145.80s. The worker child independently asserts the parent-only environment sentinel is absent; original validation identity hash binds the observed token metadata, and original lease reaches RESULT_RECORDED with matching summary commitment. Suspended child token observation remains native and unmodified. This is real adapter/AsUser/Job/lease/result integration with synthetic readiness and modeled broker identity, not formal Full or actual two-account acceptance. No administrator action or account change. Runner strict mypy PASS with MYPYPATH set to current src; initial default import resolution incorrectly selected installed untyped package and failed, preserved as diagnostic only. Ruff and scoped diff check PASS. Evidence outputs/validation_runtime/devx015-v300-full-adapter.xml. Protected deployed entrypoint and actual separate-account acceptance remain next.

### 2026-09-22 v301 protected entrypoint gap audit

Live source audit after v300 identified remaining integration requirements before privileged deployment. scripts/run_validation_tier.py main still uses its own module directory as candidate root and never passes a worker token/environment to the adapter. _run_mandatory_acceptance_command constructs runtime identity from os.environ even when its command_runner is an explicit-worker Full adapter. It allocates request/result in the launcher's default TemporaryDirectory; main separately allocates runtime-profile output there. Actual AITSWorker access has not been established for either path. Thus v300 is adapter evidence only and cannot authorize protected deployment.

Next implementation must share one explicit effective environment between mandatory identity capture and actual dispatch, separate protected implementation root from candidate data root without importing candidate code in the launcher, and bind request/result/profile exchange to a protected parent with exact worker permissions. Parent-owned request and accepted result authority must remain distinct from worker-writable output. Preserve existing file identity/reparse/hash checks, original lease and required profile/mandatory acceptance checks. Do not grant worker Modify over the control store or parent's general temporary directory. No host changes, no Full dispatch and no administrator action performed in this audit.

### 2026-09-22 v302-v303 mandatory environment coherence

Original Full adapter effective_environment now supplies both mandatory runtime identity capture and actual dispatch. Terminal mandatory runtime recheck also resolves this same source afresh, preserving drift rejection. v302 original real adapter/fence/lease/summary tests plus bounded mandatory pre-capture observation: 4 PASS/144.61s. v303 focused wrapper wiring tests: 2 PASS/6.52s; parent-only environment mutation does not affect explicit worker environment, while worker environment mutation is rejected as ACCEPTANCE_RUNTIME_CHANGED before result validation. v303 uses explicit unit seams for runtime/checkouts and does not claim native mandatory acceptance. Runner current-src mypy, Ruff and scoped diff check PASS. Cross-account request/result/profile exchange permissions and protected root/entrypoint integration remain pending; no system changes. Evidence outputs/validation_runtime/devx015-v302-environment.xml and devx015-v303-terminal-environment.xml.

### 2026-09-22 v304-v307 minimal result write rights

Bound native existing-file content writes now request 0x12019F without DELETE; deletion and recoverable creation retain their original access masks. A real disposable NTFS ACL test denies parent DELETE_CHILD and leaf DELETE, independently confirms ordinary read/write access, writes the reserved file via apply_bound_file, then proves both governed deletion and direct unlink denied. Original DACL buffers are retained and restored in finally. v306 1 PASS/6.08s; original installation directory recovery and same-byte replacement checks v307 3 PASS/8.36s. Ruff, module mypy PASS. No real workspace or system-root ACL changes.

v304 and v305 were failed fixture attempts retained unchanged: icacls basic D also denied SYNCHRONIZE and blocked ordinary reads/writes; independent r+b probe localized this. Corrected to advanced DE per Microsoft documentation https://devblogs.microsoft.com/oldnewthing/20191118-00/?p=103110 and https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/icacls. These results prove exact native file rights under the current account, not actual distinct-account acceptance. Runtime profile producer uses temporary-file rename, so its output-only child directory must be separate from read-only request and reserved-result authority. Protected exchange and trusted entrypoint remain pending.

### 2026-09-22 v308 protected exchange transport

Added administrator transport create_worker_exchange with an exact live WindowsWorkerToken, distinct launcher/session validation, protected parent check and pinned directories. It creates new root/result.json/profile objects only, installs protected DACLs at creation and compares native owner/group/DACL readback with the intended descriptor. Failures retain partial objects; existing roots are never adopted. Fixed roles: root and inherited request read-only worker; reserved result read/write without DELETE/DAC/owner; profile root allows child creation but no root DELETE/DELETE_CHILD/WRITE_ATTRIBUTES, inherited profile children can be replaced by their producer. No caller-provided ACL policy or alternate authority ledger.

v308 3 PASS/9.70s: real Windows descriptor parsing confirms protected non-inheritable security attributes and exact forbidden/effective mask bits, real directory pins and original public nonadmin enrollment rejection. These tests do not execute administrative exchange creation or prove actual worker access. Ruff and scoped diff check PASS. Coordination module mypy reports 9 preexisting errors; exact HEAD baseline copied to task work area reproduces the same 9 diagnostics, no new errors. Preserve baseline debt for final required validation; do not claim module mypy PASS. Protected exchange is not yet wired to mandatory/profile entrypoints and has not been deployed. Actual administrative create/readback and dual-account tests remain required.

### 2026-09-22 v309-v310 mandatory protected exchange admission

Added WindowsWorkerExchange in-process factory capability. It requires original actual administrator transport, binds original worker object and observed SID/session to owner process/thread, pins root/profile during its single-use context, verifies exact native security and directory/result identities before/after, and retains evidence rather than deleting on context exit. Original mandatory wrapper accepts only that capability on the explicit-worker path and never falls back to parent TemporaryDirectory. It pins the exact pre-reserved empty result through original hold_bound_read_file before publishing the immutable request, retaining ordinary request/result hashes and identity validation. Existing inherited path retains original recoverable creation.

v309 seven environment/configuration/nonadmin regressions PASS12.77s; v310 factory/nonadmin and missing/serialized exchange refusal three PASS8.95s. Ruff, runner mypy and scoped diff PASS; coordination mypy remains the same nine verified baseline diagnostics. No positive administrator creation or actual dual-account execution is claimed. Profile path capability exists but main profile allocation and protected implementation/candidate separation remain unwired. No system change or deployment; Full and OPS080 acceptance remain pending.

v313 trusted-inspector positive regression stopped during fixture setup before SUT: Git add rejected a long requirement filename under default pytest temporary path, 1 ERROR/122.66s. Original XML outputs/validation_runtime/devx015-v313-trusted-inspector-control.xml retained. Preregister D:/Work/devx015-v314 as one temporary pytest fixture root for DEVX015 trusted-inspector positive regression; verified absent before use. Purpose is the unchanged test in a shorter path, not a new development lane. Retain unique evidence on failure; remove only after terminal process/unique-content audit and canonical evidence preservation at task cleanup. No global Git/Windows setting changes.

### 2026-09-22 v311-v314 trusted inspector separation and positive counterexample

Publication inspection now selects the script from the loaded implementation root, starts Python with -I, and passes candidate root only to readonly --inspect-full-publication-profile. Other modes reject --inspection-candidate-root before effects. v3114PASS9.53s; v3124PASS9.43s include actual subprocess evidence that neither candidate same-name script nor injected PYTHONPATH/sitecustomize executes. Ruff, two-module mypy and scoped diff PASS. This is not complete publication acceptance.

v313 setup failed on default Windows fixture path length before SUT,122.66s. Retained XML. v314 used preregistered short D:/Work/devx015-v314, passed that setup obstacle and completed original synthetic Full/mandatory/profile/result custody. Positive publication regression then FAILED574.07s: PUBLICATION_FULL_CLOSURE_INVALID -> ACCEPTANCE_IMPLEMENTATION_ORIGIN: ai_trading_system. The existing bind_acceptance_implementation combines candidate source inventory with a requirement that verifier loaded modules originate from that candidate; this is incompatible with an independent trusted inspector. Do not restore candidate execution, suppress origin checks, mark positive PASS or rerun Full merely to diagnose it.

Next: explicitly separate candidate committed-source capture from trusted inspector loaded-code verification, preserve both checks and their own identities, then use readonly inspection on the retained terminal fixture to diagnose the boundary without replaying Full or advancing publication. A future changed final candidate still requires its own formal Full. v314 fixture retained at D:/Work/devx015-v314/popen-gw0/test_remote_admission_rechecks0/git-fixture, transaction outputs/architecture/arch_005_integration_publication_fence/transactions/merge-authority/transaction.json, original execution_request/result/validation_identity/profile/summary under outputs/validation_runtime. No project remote push occurred. Preparation roughly4.5min and the complete test574.07s were observed separately; path preflight should precede expensive fixture preparation. Git status/diff inherited configuration remains a separate privileged-deployment audit item. Profile exchange/main integration and actual dual-account validation remain pending.

### 2026-09-22 v315-v316 independent inspector identity

Refactored the original committed-source reader into one shared capture primitive. Candidate execution bind_acceptance_implementation still verifies loaded candidate code as before. Independent profile inspection separately checks candidate source rows against the original mandatory runner commitment and verifies its own loaded authored modules against fixed implementation src/scripts, with runtime dependency inputs and exact module-origin/code checks. Initial/final inspector captures are compared; original fence locked recheck accepts only candidate evidence or Python sources under its own fixed implementation src/scripts roots, never a caller-selected trusted root. Resolve that fixed root once per recheck, not once for every capture.

Live correction to earlier checkpoint: v314 transaction is FAILED, not still FORMAL_VALIDATION_RESULT; original readonly inspection correctly rejected it before any source checks. It was not revived or edited. v315 separate SOURCE_IDENTITY_DIAGNOSTIC_ONLY PASS18.26s against retained candidate bfb839c7c9804c4e89a007fc61ebb11bcb1ea817:1245 candidate source rows equal original evidence,72 inspector sources verified, changed loaded function and candidate-origin substitution rejected. Six watched files unchanged, entire original fence replay byte-identical before/after. This diagnostic neither dispatches Full nor admits publication and is not a replacement positive end-to-end test.

v3165PASS9.37s: actual Git committed source capture never executes top-level witness code and rejects source drift; fixed-root/candidate capture rechecks accept expected bytes and reject outside scope or changed digest. Capture recheck tests isolate the component rather than claim real transaction admission. Ruff, three-module mypy and scoped diff PASS. End-to-end positive publication under a new lawful transaction, protected runtime deployment, profile/main exchange integration, actual separate-account acceptance and original final Full/OPS080 remain pending. Retained artifacts: devx015-v315-source-identity-diagnostic.json, v315-fixture-replay-before/after (before filename devx015-v315-fixture-replay.json), v316-source-capture.xml under outputs/validation_runtime.

### 2026-09-22 v317 性能记录目录接入

原 v317 SINGLE_LANE preflight PASS 后，Full runner 提前构造，性能记录目录由实际 runner 上下文选择。受限 worker 必须持有真实 WindowsWorkerExchange；缺失或字典伪造在任何父进程临时目录分配前拒绝。读取 profile_directory 时复核交换目录 ACL 和文件身份；交换目录证据保留。普通执行保留原 TemporaryDirectory 生命周期。formal selection 的 PYTEST_ADDOPTS 使用 runner 实际环境。

验证：devx015-v317-focused.xml，16 workers/loadfile，4 passed in 7.21s；Ruff PASS，runner mypy PASS，指定路径 diff check PASS。本轮未执行 Full。仅完成目录与环境接线；可信父进程与候选源码身份分离、main 的真实账号上下文注入、实际管理员部署、最终 Full/publication 和 OPS-080 S4/S5 尚未完成。

### 2026-09-22 v317 启动端与候选身份分离

受限交换目录分支在父进程通过固定实现根校验已加载启动代码，候选源码仅作 data-only capture；子进程原 bind_acceptance_implementation 来源检查不变。运行后复核原启动清单与候选清单，launcher_identity 随 mandatory_acceptance 进入原结果承诺。发布 inspector 对 worker_identity 强制要求启动清单，按固定根限定路径、复核原字节，并纳入原 capture 再校验。普通同根路径只调用一次更严格 include_runner=True 校验，避免重复源码枚举。

devx015-v317-identity-focused.xml：8 passed in 7.71s，16 workers/loadfile；覆盖清单越界、缺失、重复、大小类型、字节篡改，以及既有候选只读捕获和环境检查。runner mypy、Ruff PASS。这些是组件证据，尚未证明真实 worker 分支端到端；下一步补该分支完整调用验证与 main 真实上下文接入，不复活 FAILED v314 事务。

### 2026-09-22 v317 交换分支验证与入口边界

新增原 mandatory wrapper 的 exchange 分支单元验证：真实 request/result 文件身份与请求写入保留，token/exchange 和身份捕获明确使用 unit seams。正常运行绑定 launcher_identity 和候选 runner_identity；分别模拟执行后 launcher/candidate 漂移，两者均拒绝并将 exit_code 转为1。devx015-v317-exchange-wiring.xml：3 passed in 7.05s，16 workers/loadfile；Ruff 修复测试格式后 PASS，显式路径 diff check PASS。不是实际双账号进程或管理员部署证据。

源码审查确认正式 main 仍无真实 worker 上下文；checkout_guard.collect_checkout_dirty_paths 的 git status 仅设置 core.quotepath=false 和 GIT_OPTIONAL_LOCKS=0，其余环境和仓库配置继承，_run_git_diff_check 同样仍运行 Git。父进程提权接入前必须完成受保护 Git 元数据和命令执行边界设计及验证，不能单凭传入 worker token 开放候选根。下一步审查该边界并登记最小涉及路径后实施；本轮未改 checkout_guard、未运行候选配置攻击、未增加部署授权。

### 2026-09-22 v318 Git审计回调修复

v318 canonical preregistration完成，原v6事务下SINGLE_LANE LANE预检PASS。一次性普通权限fixture复现原collect_checkout_dirty_paths执行仓库core.fsmonitor脚本；显式关闭后保持脏文件识别，0.313s，临时fixture自动清理。已在status和staged/unstaged diff审计命令显式设置core.fsmonitor=false，不改变排除路径或diff校验语义。

devx015-v318-git-audit.xml：3 passed in 15.10s（16 workers/loadfile）。新测试包含真实Git回调正对照，确认审计不执行回调且index字节及文件身份不变；原optional-lock与排除路径测试通过。Ruff、checkout_guard mypy、显式路径diff check PASS。该修复只封闭已证实fsmonitor路径，未声称filter、继承环境、可执行文件定位和Git元数据均已适合提权执行。正式受限账号main、保护部署、完整Full/publication及OPS080 S4/S5仍待完成。

### 2026-09-22 v318 clean-filter实证与正式入口约束

一次性普通权限fixture使用无业务内容source.txt、.gitattributes以及仅写marker并cat输入的clean filter。调用当前原collect_checkout_dirty_paths和_run_git_diff_check：status本例未执行filter，unstaged diff明确执行filter；index字节保持一致。耗时0.422s，fixture随TemporaryDirectory清理。该结果证明关闭fsmonitor不足以允许可信父进程对候选Git环境直接执行原审计；不能用index未变证明无代码执行。

正式入口必须同时约束Git可执行程序及依赖定位、子进程环境、common/worktree Git元数据与配置包含链。保持已有filter语义；不把全局禁用filter当作安全修复，因为过滤器影响worktree/index一致性判断。可信运行范围若不能证明命令路径与配置受保护，应在候选Git命令前拒绝；不能等到WindowsJobProcess创建pytest时才检查父令牌。启动清单与结果承诺不能代替启动前操作系统权限隔离。

下一步实现受保护Git执行上下文的原入口接入方案：枚举现有Git调用，确定同一原仓库common-dir与worktree配置的固定权限/环境要求，以及运行期间防替换约束，再最小化改动到共享执行接口。未批准或实施正式主机ACL切换，未改变Git filter语义，未运行原项目filter，未声称完成真实双账号或Full。

### 2026-09-22 v318 原配置准入复用审查

静态AST检索从run_validation_tier起始的可解析本地import关系：60个模块、42处subprocess调用。仅是静态候选清单，不是运行时可达性证明，未覆盖动态导入/别名调用/原生CreateProcess。清单中包括不在Full启动必经路径的Atlas、source-preservation和supervised操作，不能据此扩大修改到全部模块。

发现source_preservation.SourcePreservation已有_environment、_configuration_snapshot、_recheck_configuration_files/_recheck_environment：在旧guard之前拒绝filter/include/partialclone/外部Git路由，读取worktree实际配置及原始重复键，物理捕获与重检防止配置被遮蔽。原_git已使用禁pager、hooks、lazyfetch、协议和固定Git环境。这些约束应作为共享准入基础，不另写语义不同的配置解析器；它仍使用PATH中的git且没有管理员对象锁定，因此不能直接等同受保护运行能力。

复用前验证原配置测试：devx015-v318-reuse-config-admission.xml，14 passed in 17.36s，16 workers/loadfile，涵盖两侧worktree危险配置、common值不可被覆盖隐藏、include目标不打开。未执行Full或source preserve写入。下一步将已有配置准入作为共享实现保留原source-preservation行为，补固定可执行程序/OS对象保护后接入可信Full；先登记涉及source_preservation/runner/coordination/测试范围再改动。

### 2026-09-22 v319 共享Git配置准入实现

原v6事务下v319 canonical预登记和SINGLE_LANE预检PASS。将source_preservation原_environment、配置快照、物理重检、_git方法原样移动至同模块GitConfigurationAdmission；公开capture/recheck。SourcePreservation继承该实现，保留原构造policy、错误码、schema和原Git配置语义，不新增账本或判定协议。共享类明确不是OS保护执行能力，尚不能据此运行提权Full。

验证devx015-v319-shared-admission.xml：20 passed in 30.43s，覆盖共享/原快照一致、物理漂移先拒绝、两侧worktree危险配置、common遮蔽、include和原Git布尔语义。devx015-v319-original-preservation.xml：1 passed in 12.37s，原preserve入口在任何status/transform前拒绝filter且原状态不变。均16 workers/loadfile。Ruff、source_preservation mypy和指定路径diff check PASS。

后续补固定Git可执行路径及OS对象保护生命周期、可信Full main上下文接入和真实双账号验收。当前共享_git仍保留原PATH选择，未宣称完整安全；此轮未安装或切换主机，未运行Full，OPS080正式S4/S5未完成。

### 2026-09-22 v320 固定Git程序选择

v320预登记及原v6事务SINGLE_LANE预检PASS。独立GitConfigurationAdmission现在强制传入已存在绝对Git程序路径，经原_configuration_path拒绝重解析路径；共享_git使用该显式路径。SourcePreservation构造显式保留原git命令行为，不改变已存在source-preservation协议。

验证devx015-v320-fixed-git.xml：5 passed in 17.95s（16 workers/loadfile），包括真实Git在仅含伪git程序的PATH下仍产生同一快照并完成重检、相对/不存在路径在spawn前拒绝、原filter拒绝入口回归。Ruff、mypy和指定diff check PASS。路径固定不等同程序哈希、依赖或OS权限保护。

只读ACL观察：C:/Program Files/Git及cmd/git.exe所有者为Administrators；所检Git目录写授权为管理员/SYSTEM/TrustedInstaller。未遍历依赖或完整父链，不能据此宣布整个运行时保护通过。D:/Work/AITradingSystem/.git与config以及integration的.git指针由JACK拥有，并存在Authenticated Users Modify等授权，不符合目标受保护元数据边界。未修改权限。后续必须在具体部署/排空范围下处理，第一阶段账号canary授权不能自动覆盖这些正式对象。

### 2026-09-22 Git运行时保护对象实测

只读程序清单：cmd/git.exe 45032 bytes SHA256 4AB2B391DE7FB5EB3AFB34B5BD19EE7F71D0450BF7E4C207B201A0392E3825AE；mingw64/bin/git.exe 4003816 bytes SHA256 DC1EDE5D773E55D28F93BA10B7AD2AEB2D562AD514039BD93F4D396831F6DB7B，libexec/git-core/git.exe与后者相同。不能把cmd入口的路径固定当作全部执行文件已固定。

在一次性无业务Git仓库中，显式mingw64/bin/git.exe执行cat-file --batch，先收到missing响应确认启动，再用原进程PID读取已加载模块。观察到原Git主程序、Windows系统DLL和5个Git目录DLL：libintl-8.dll、libpcre2-8-0.dll、libiconv-2.dll、libwinpthread-1.dll、zlib1.dll。stdin正常关闭后exit0，fixture自动清理。仅说明该命令该次实际加载集合，不证明所有命令/延迟加载/子程序的完整闭包。

已有hold_bound_read_file可保持已验证叶文件的deny-write/delete句柄，pin_directories保持祖先不可删除；这些原语可复用，不新增平行锁。权限准入仍必须覆盖缺失配置文件所在父目录，否则句柄无法阻止新增config.worktree等路径。后续受保护运行时清单应包含直接Git主程序及审计命令所需依赖，并在启动前建立OS对象保护；本轮未改任何ACL、未部署运行时、未运行正式Full。

### 2026-09-22 v321 受保护文件持有组件

v321预登记和SINGLE_LANE预检PASS。原_WindowsEnrollmentAdministrator新增只读hold_protected_files：严格绝对路径/摘要输入，pin_directories固定祖先，逐个验证父目录和叶ACL，按摘要及原inode读取，进入原hold_bound_read_file后再检查保护，全部句柄维持到调用区间退出。附加protected_directories用于缺失配置名称的父目录保护；不创建文件、不修ACL、不授予执行。清单完整性和部署权限仍由上层证明。

验证devx015-v321-protected-files.xml：4 passed in 7.15s（16 workers/loadfile）。新测试只替换普通临时文件的ACL准入，Windows文件/祖先句柄全部真实，持有期间写/删/改名拒绝、释放后可写，错误hash不yield且释放资源；原安全描述符/目录pin与真实非管理员factory拒绝回归通过。Ruff、指定diff check PASS。coordination mypy仍9个已知基线诊断，未报告本轮新增行，不能称模块mypy PASS。

剩余：上层固定运行时依赖清单、安装器硬链接处理、真实ACL准入及管理员运行、Git配置物理捕获与持有对接、Full main真实worker上下文、最终Full/publication和OPS080 S4/S5。本轮无管理员变更，无正式部署或Full。

### 2026-09-22 v322 安装文件硬链接身份

只读stat确认mingw64/bin/git.exe为4 links，5个已观察Git DLL各2 links；libexec/git-core/git.exe为不同file-id的单链接文件，虽内容SHA一致也不是同一对象。v322 canonical预登记及原v6 SINGLE_LANE预检PASS。

hold_protected_files新增可选expected_file_identities，绑定(device,file_id,link_count)三元组；拒绝未声明文件的附加身份、非法类型及实际身份/链接数不符。只有精确声明才能将多链接数传入原bounded_regular_bytes/hold_bound_read_file；未声明保持单链接要求，未自动接受别名。

devx015-v322-hardlink-custody.xml：4 passed in 7.99s，16 workers/loadfile。原生文件持有覆盖单/多链接、错误digest、错误link count、正常释放，并确认硬链接别名无法写入持有inode。ACL准入仍为单元替身，不构成真实管理员安装证据。Ruff和指定diff check PASS。后续把配置与运行时明确清单接入同一持有区间；实际配置父目录权限、运行时依赖完整性、主机迁移、Full/publication和OPS080 S4/S5仍未完成。

### 2026-09-22 v323 持有期间Git配置准入

v323预登记和SINGLE_LANE预检PASS。GitConfigurationAdmission.held(source_root, runtime_files, runtime_identities)先构造实际管理员transport，要求固定Git程序出现在清单中，然后不调用Git地捕获.git/commondir定位文件和common/worktree配置。现存文件与运行时清单共同进入原hold_protected_files，原本缺失配置的父目录也列入保护。持有后再比对物理捕获，才执行原配置capture；退出前在句柄未释放时重检。yield副本防止调用者意外修改内部退出校验依据。没有ACL修复、安装或独立权威账本。

验证devx015-v323-held-admission.xml：6 passed in 15.52s，真实Windows句柄+Git解析，管理员/ACL为明确单元替身；验证capture及退出recheck均处于持有范围、配置写被拒绝、持有前新增config.worktree在任何Git调用前拒绝。devx015-v323-native-admin-rejection.xml：1 passed in 6.69s，真实非管理员token在Git元数据读取前拒绝，未skip。均16 workers/loadfile。Ruff、source_preservation mypy、指定diff check PASS。

仍须上层证明runtime_files依赖完整性和固定环境、Git其他调用的共享上下文、仓库元数据/真实主机权限、正式main worker接线与结果绑定。当前组件不是Full执行或部署授权；真实双账号Full/publication与OPS080 S4/S5尚未完成。

### 2026-09-22 v324 Git环境同源绑定与慢阶段定位

v324原v6 canonical预登记及SINGLE_LANE预检PASS。独立GitConfigurationAdmission构造要求git_environment，校验字符串/空字符/重复大小写键后复制保存；_environment_source同时供准入检查与_git实际subprocess环境使用。调用者或父进程后续环境改动不会替换该副本。SourcePreservation显式保留_git_environment=None，由原实时ambient环境检查继续拒绝不安全覆盖，原协议行为不变。

devx015-v324-bound-environment.xml：10 passed in 63.13s，16 workers/loadfile。新增测试实际观察Git subprocess环境，证明parent/caller变动后仍使用bound witness、无GIT_DIR路由覆盖，且原SourcePreservation仍在spawn前拒绝ambient GIT_DIR；共享准入/held/native-admin/filter回归通过。Ruff、source_preservation mypy、指定diff check PASS。

异常耗时已定位到选择范围：-k shared_git意外匹配test_linked_worktree_preserves_shared_git_identity_and_its_real_private_index，单例39.257s；全部case合计56.643s，suite63.106s。保留该正向保全回归证据，不重跑；后续focused验证使用精确nodeids，不能将此误判为新环境绑定性能退化。测试等待期间只观察原会话，没有重启。

后续仍需运行时完整性/固定子环境与所有必经Git调用连接、正式main受限worker接入、真实ACL部署和最终Full/publication、OPS080 S4/S5；本轮未部署或改ACL。

### 2026-09-22 正式Full接线的readiness同根门禁

沿main实际路径复核：_validate_publication_transaction_for_full先fence.validate、canonical task commitment、mandatory binding，然后check_full_readiness，最后才FULL_DISPATCHED。check_full_readiness在任何checker前要求target root等于_inspection_code_root，否则READINESS_INSPECTION_ROOT_MISMATCH。现有protected runtime与candidate分离设计若直接接入，会在此处拒绝，与新增Git组件本身无关。

精确复核原test_foreign_inspection_code_root_stops_before_any_checker：1 passed in 6.22s（16 workers/loadfile，devx015-v324-readiness-root-boundary.xml）；确认full_dispatch_ready=False且checks为空。本轮没有放松门禁。readiness原checker循环为串行，未见该模块ThreadPool/Executor，不应在接线过程中擅自改并发或跳检查。

后续需通过活体、同进程、在有效held区间内且绑定实际受保护检查器输入的上下文，支持明确的独立检查器根。仅Git配置snapshot或caller boolean不满足准入条件。普通standalone入口保留同根检查，仍需候选源/检查器源独立校验及原全部CHECKER_IDS；在该能力与所有实际Git调用绑定之前不启用提权main。validation_readiness.py已在v6声明范围；checkout_reconciliation.py不在，应先证实必经路径再决定是否扩展，不能因静态import清单就改全模块。

### 2026-09-22 v325 活体文件持有上下文

v325 canonical预登记和原v6 SINGLE_LANE预检PASS。新增HeldGitConfiguration，仅在GitConfigurationAdmission.held完成实际管理员准入、文件句柄持有及配置capture后创建；active_context取得同进程/同线程/当前区间对象，assert_current核对仓库范围与所需文件的已持有SHA。普通构造拒绝，区间退出即失效，即使退出重检异常也先失效。保留原held的snapshot副本yield，不改原schema/保全流程。

验证devx015-v325-live-context.xml：3 passed in 12.40s，16 workers/loadfile。覆盖正常持有、错误SHA、越界仓库、实际线程切换、嵌套拒绝、退出后对象及取用失效，原新增配置提前拒绝和实际非管理员拒绝仍通过。文件句柄/Git真实，正向ACL/elevation为明确测试替身。Ruff、source_preservation mypy及指定diff check PASS。

对象不是对不可信Python代码的同进程沙箱，也不授予执行/发布权。正式readiness消费时还必须验证实际检查器实现与运行时输入的完整保护，普通入口同根门禁保持。后续源于固定安装包的动态导入与native依赖、Git执行上下文共享、main worker接线、实际ACL部署和原最终验收均待完成。

### 2026-09-22 独立检查器入口的源码一致性约束

复核validation_readiness.py明确注释与实现：TRADING-2564不仅要求同根，还要求检查器及两个入口属于candidate，整个src及两个入口与candidate一致。_INSPECTION_REQUIRED_FILES包含validation_readiness.py、run_validation_tier.py、scripts/validation_readiness.py；_INSPECTION_CODE_PATHS检查整个src。保护了另一个版本的检查器不能替代这一约束。

_checkers固定7项，其中architecture_generated/report_flow/compatibility等函数内部存在延迟import。仅验证当前已加载模块后放开异根，会漏掉后续实际使用的检查代码。下一步先实现候选源码与固定检查器副本的独立数据校验，并要求对应固定文件处于活体held集合；保留原候选脏/未跟踪源码检查及所有CHECKER_IDS。固定代码可物理异根，不能任意版本异源。动态导入/pycache/native依赖需属于完整受保护安装集合，不凭已加载模块子集宣布闭包完整。

现有capture_acceptance_implementation只捕获数据，但内部仍直接subprocess.run Git；它在受保护入口中也必须使用绑定的绝对Git与显式环境，不能在身份比较时退回旧环境。应先提供受保护的源码树读取接线，再消费活体上下文改readiness。此轮仅源代码审查，无门禁改动、无Full或部署。

### 2026-09-22 v326 受保护源码树读取

v326预登记及原v6 SINGLE_LANE预检PASS。HeldGitConfiguration.candidate_source_tree只允许精确40位candidate SHA和固定ls-tree src/scripts命令；先校验活体/仓库范围，并使用assert_current返回的规范根调用已固定Git程序/显式环境。capture_acceptance_implementation新增可选精确类型git_context，经此通道读取树；伪造/过期/错误提交拒绝，不回退旧subprocess。原逐文件native读取、Git blob与字节摘要校验、资源限额以及子进程loaded-code绑定不变。

验证devx015-v326-held-source-tree.xml：3 passed in 13.29s，涵盖实际Git/native custody中的data-only capture、HEAD/字典/过期拒绝且未追加Git调用、原普通源码捕获与篡改拒绝。路径补查后devx015-v326-canonical-root.xml：2 passed in 10.36s，确认含..输入经过准入后实际_git cwd为规范已批准根。均16 workers/loadfile。Ruff、两模块mypy PASS。正向管理员ACL仍为测试替身，不能当部署证据。

下一步比较候选版本与固定检查器副本、要求对应实现文件处于活体held集合，再接独立readiness根。普通readiness门禁、其它尚未接线的Git调用和main worker接入仍待处理；未部署、未执行正式Full或OPS080 S4/S5验收。

### 2026-09-22 v327 固定检查器副本逐文件校验

v327 canonical预登记及SINGLE_LANE预检PASS；补记前devx015-v327-close-notes-preflight.json再次PASS。capture_matching_inspector_sources通过活体Git上下文读取candidate树，以Git blob核对固定检查器src/scripts Python副本，再要求每个副本的SHA处于当前held集合。只读取数据，不执行候选代码；错误版本与未持有副本分别拒绝。

devx015-v327-inspector-sources.xml实读3 tests、0 failures/errors/skipped、13.616秒；覆盖匹配、过期版本、未持有副本和原候选漂移拒绝。实际Git/文件句柄，正向管理员ACL仍为测试替身。此前Ruff、workflow_execution mypy和限定diff检查通过；本次未重复运行同一组测试。

仍未证明额外源码、pycache/native与动态导入闭包，未启用异根readiness。当前源码再次确认：readiness._git仍使用相对git和ambient环境；_inspection_code_identity除HEAD外检查必需文件、工作树/索引和未跟踪文件，_committed_yaml另有authority提交检查。因此不能只替换candidate_source_tree便开放异根。后续接线必须覆盖这些实际调用以及下游检查器，保留全部7个CHECKER_IDS和候选一致性门禁。

下一阶段按完整入口收敛：固定运行时清单及额外文件拒绝、readiness与必经Git统一上下文、main worker/token/exchange接线，然后新合法事务下的端到端验证。正式管理员部署仍需具体范围授权。当前v6继续IN_PROGRESS，未提交/Full/发布，DEVX015其余场景和OPS080 S4/S5未完成。

### 2026-09-22 v328 readiness范围合法继承

已确认Atlas缺occurred_at事件时间分支仍调用Git，并有进程级lru_cache；兼容性检查器通过build_repository_authority读取历史对象。不得只修改readiness表层或复用普通上下文历史缓存。

原v6 replay PASS/TASK_SOURCE_PRE_WRITE，candidate=null，无Full。经原release failed完成终止留证，lease-7b7f3729c1edd9a82c0c RELEASED；这是工程范围调整，不代表Full失败或代码丢弃。v7 acquire和TASK_SOURCE_PRE_WRITE checkpoint PASS，lease-e92474efd42406311046，transaction SHA 2a762827a75a72b48a39d1dd909728329e942429112cec4941b52652be1bb0cf。checkpoint绑定scope-transition和v6终态receipt。原源码及验证证据完整保留。

新旧范围比较仅增加src/ai_trading_system/atlas/page_effectiveness.py、tests/atlas/test_page_effectiveness.py、tests/test_validation_readiness.py；其余声明/生成顺序/必需tiers继承。首次acquire因重复传入自动注入的两个resource路径被拒绝，未创建事务；移除CLI重复参数后正式acquire通过，失败输出保留，不改门禁。

work/devx015-v328-preflight.json SINGLE_LANE PASS，覆盖source_preservation/readiness/compatibility/Atlas和相关测试。下一步实际实现受保护Git读取贯穿这些下游并隔离旧缓存，保留7 checker及版本/工作树校验。当前尚未修改该接线实现，不宣称已接通；正式运行时、worker入口、Full/publication/OPS080仍待完成。没有主机权限或部署改动。

### 2026-09-22 v328 受保护Git通道贯穿readiness下游

在v7预检范围内实现HeldGitConfiguration.inspection同步上下文及inspection_git_result，活体/仓库核对后只接收有限只读Git命令语法；未知命令和过期对象拒绝，无普通Git回退。底层复用原固定exe/env，通过_git_result保留返回码，同时显式禁用diff.autoRefreshIndex。普通SourcePreservation的_git继续返回原stdout契约。

check_full_readiness新增可选git_context并在其整个同步检查区间选择通道；原同根、源码提交/脏/未跟踪和7个checker保持。compatibility_authority三个Git读取函数消费该通道。Atlas repository_head及事件commit_time也接通，受保护时间读取既不消费也不写入普通lru_cache；普通调用保留缓存。该同步上下文不是线程池继承或同进程恶意代码沙箱，不授予执行/部署权限。

验证：devx015-v328-readiness-transport.xml 48 passed in 17.19s；增加差异/索引不变及非法命令拒绝后devx015-v328-git-transport-regression.xml 7 passed in 30.34s。实际Git和Windows句柄，正向管理员ACL仅明确测试替身；聚合正例的checker体是测试替身，仅证明路由贯穿全部7项，不能称真实Full依赖PASS。覆盖普通行为、固定程序环境、错误/过期上下文、旧缓存隔离、历史文件读取、无匹配/缺文件返回契约、工作树变化检测和索引字节不变。所有进程exit0。后组最慢单case4.435秒，未重复运行Full。

Ruff及限定diff检查PASS。四模块mypy仅Atlas两项object属性诊断，HEAD导出基线复现同两项；其余三模块无诊断，不声称整体mypy通过。canonical completion-event PASS，仍IN_PROGRESS。

后续：完整固定安装输入集合及额外源码/pycache/native约束，再启用受保护物理异根readiness；main仍需实际token/exchange/上下文注入及全部必经Git路由。真实权限部署、新合法端到端Full、publication、DEVX015余项及OPS080 S4/S5未完成。未修改主机ACL或启用worker。

### 2026-09-22 v329 固定项目副本源码集合

v329 canonical预登记和v7 SINGLE_LANE预检PASS。capture_matching_inspector_sources复用原入口，在字节比较前对固定src/scripts做有界、不跟随reparse的目录清单检查，核对candidate Python路径集合；拒绝多余/缺失源码、字节码缓存、本地扩展、路径配置及压缩导入载体。额外文件不读取内容，不执行代码。随后仍执行Git blob和活体held摘要检查。该source-only约束仅适用固定项目副本；第三方、stdlib和native运行时完整保护未因此成立。

devx015-v329-inspector-namespace.xml：3 passed in 15.86s，实际Windows文件句柄/Git及真实目录junction；覆盖额外源码、pyc、pyd、pth、zip、junction、缺失源码，以及原匹配/错误版本/未持有拒绝。管理员ACL正例仍为测试替身。Ruff、workflow_execution mypy和限定diff检查PASS。最后仅换行格式修正，无需重复pytest。

当前没有活跃测试，未运行Full/部署/发布。下一步继续固定安装运行时保护、异根检查器版本约束消费和main worker入口接线；原DEVX015最终验收和OPS080 S4/S5均未完成，v7仍TASK_SOURCE_PRE_WRITE。

### 2026-09-22 v330 独立运行时工程样本验证

canonical预登记及结果事件PASS。样本C:/Users/32739/Documents/Codex/2026-09-19/wo/work/devx015-runtime-probe-v330仅普通权限工程用途，不是部署或已提交候选。复制实际基础解释器/stdlib/DLLs/tcl、项目venv依赖、当前integration的src/scripts Python源；不读排除文档。python311._pth固定根/Lib/DLLs/site-packages/project路径，不含import site；以-I -S -B运行。

首次复制及源/目标SHA核对15860文件458676422字节，102.76秒。RECORD审查发现7110缺项（主要第三方pyc与命令入口），按原venv内精确路径补齐并逐文件比对，20.54秒；完整清单22970文件654769010字节。首版与完整文件manifest分开保留。RECORD审查输出首次后被同名复核覆盖，原首轮计数保留在命令输出及此记录；最终报告另存complete文件，不能把最终零缺项说成首轮即通过。

92个distribution RECORD最终零缺失/零越界。污染PYTHONPATH指向开发树且PYTHONHOME无效时，最终导入探针2.32秒PASS：所有已加载Python模块来自样本，native模块仅样本或Windows，site未加载。测试包括ssl/sqlite3/yaml/numpy/pandas/pytest/xdist及readiness/workflow_execution/runner导入，不执行Full或业务。原acceptance_runtime_identity真实调用16.40秒PASS，16554个依赖代码输入441758533字节；解释器与engine均来自样本。

这证明该样本在所测导入和身份校验上的独立性，不证明未来所有DLL延迟加载或完整特权保护。安装包.pth按RECORD保留为被禁site启动的惰性数据，并未执行；正式bootstrap仍须固定导入行为并绑定完整清单。复制的Scripts入口未经重新安装，其旧环境绑定不能作为正式命令入口。项目源码是当前dirty工程快照，不能宣称final candidate。

性能：初次复制是本轮主要成本；已有样本可复用做增量准备，无须重复复制整套环境。保留至清单/证据规范保全且无进程依赖后定点审计清理。下一步完整保护清单与固定bootstrap、正式main worker接线及可复核管理员部署脚本；未写ProgramData/修改ACL/启用worker，Full/publication/OPS080仍未完成。

### 2026-09-22 v331 acceptance必经Git接线

v331预登记及v7 SINGLE_LANE预检PASS。workflow_execution.bind_mandatory_acceptance的Git读取，以及_capture_acceptance_sources未显式传git_context的分支，现在先消费当前inspection_git_result；仅普通无上下文时保留原调用。受保护命令语法新增精确SHA的ls-tree固定源码树/单路径读取和cat-file blob，仍拒绝HEAD替代SHA及写命令。原candidate、106变体映射和blob类型/字节校验不变。

验证devx015-v331-acceptance-git.xml：3 passed in 15.62s。实际Git/native-held fixture证明默认capture及mandatory的rev-parse/ls-tree/cat-file都通过固定通道；将synthetic authority绑定为无效JSON后原映射门禁仍拒绝。正向ACL仍为测试替身，不是管理员部署。devx015-v331-original-binding.xml：10 passed in 13.21s，覆盖原真实Git正向和不完整/缺失/重复/错误候选/路径/blob/任务拒绝。Ruff、两个模块mypy、限定diff检查PASS。无存活测试，无Full。

原host-enrollment schema只登记控制根及仓库物理身份，不承载运行时安装清单；不能把样本manifest塞入或修改该schema来伪造部署权威。固定运行时安装保护仍需实际设计与验证。v330工程样本保留其原源码快照；本次新源码没有悄悄覆盖样本，因此不能把v330 identity PASS宣称为v331 runtime证据。

后续仍需完整安装输入保护、独立检查器入口对当前candidate同版本校验、main的真实worker/token/exchange和其它必经Git接入，再做合法端到端验收。v7继续，未提交/部署/发布，DEVX015余项和OPS080 S4/S5未完成。

### 2026-09-22 v332 实际大文件持有与成本定位

v332预登记与v7 SINGLE_LANE预检PASS。实际32376320字节Scripts/ruff.exe在原hold_protected_files入口以WORKFLOW_ARTIFACT_BUDGET失败，0.014秒；原因是两个读取调用沿用通用16MiB默认。现按文件大小显式传budget并保留底层64MiB硬上限，两个读取/持有调用一致，不改全局默认或任何ACL/hash/link约束。实际大文件修复后0.058秒PASS。

devx015-v332-native-file-budget.xml：9 passed in 8.62s，覆盖原小文件、超过16MiB文件、hardlink别名写拒绝、错摘要/错link拒绝、释放可写和超过64MiB拒绝。Ruff/限定diff检查PASS。coordination mypy仍原9项基线诊断，无新增，不称整体通过。

整套v330样本22970文件实际原生持有PASS：进入104.732秒，总105.443秒；句柄188→48798→188，期间写打开被拒绝。仅ACL/elevation准入为测试替身，其余使用原真实文件/目录句柄；这不是管理员保护或正式执行证据。结果DEVX015-runtime-full-custody-cost-20260922.json。

512文件剖析纠正后PASS：进入1.646秒，总1.664秒，句柄188→1237→188。nt.stat累计0.953秒，directory_identity累计0.595秒/562次，是明显重复开销。首个subset脚本误用不在subset里的ruff作拒绝见证，得到FAIL；未写入字节，原失败结果保留，改为subset内首文件后另存corrected结果，不改项目门禁。

下一步优化候选：只在pin_directories实际持有、禁止删除的区间内复用同一父目录的expected identity；保留每个叶文件的原生root身份验证、内容读取/hash/link/ACL/终态检查，不共享跨区间缓存。需先登记并测试多个同父目录文件、错误字节拒绝和释放，再对相同512文件比较；本轮尚未实现该优化。v7继续，全部进程terminal；未部署/Full/发布，DEVX015/OPS080仍未完成。

### 2026-09-22 v333 pinned parent复用优化

v333预登记与v7 SINGLE_LANE预检PASS。hold_protected_files仅在pin_directories持续持有禁止删除句柄的当前区间内保存每个父目录expected identity。每个叶文件仍经原hold_bound_read_file独立核对实际根/叶身份、hash/link和句柄；原ACL入口/逐文件/退出检查不变，无跨调用缓存。

devx015-v333-pinned-parent.xml：9 passed in 9.41s，增加同父目录两个文件均拒绝写、父目录改名拒绝、释放后可写和下一持有区间旧摘要拒绝；原大小/硬链接/超64MiB拒绝保留。Ruff及限定diff检查PASS。coordination原9项mypy基线仍未解决，本次不宣称模块类型检查通过。

对相同512文件做同一cProfile样本：进入1.646376→1.156634秒，减少约29.75%；directory_identity 562→67次，累计0.595→0.065秒。两次句柄均188→1237→188，写打开拒绝，终态PASS。结果DEVX015-runtime-subset-custody-v333-20260922.json及profile-v333文本。仅ACL/elevation为测试替身，不是实际管理员验收。未重复完整22970文件持有，不能把子集提速直接声称为完整运行时提升。

已完成这项有证据的局部优化，停止继续扩大性能改动。后续回到完整安装保护、bootstrap/正式main worker接线和真实验收；v7继续，未部署/Full/提交/发布，DEVX015/OPS080仍未完成。所有测试/剖析进程terminal。

### 2026-09-22 v334 完整runtime目录纳入原持有接口

v334 canonical预登记和v7 SINGLE_LANE预检PASS。GitConfigurationAdmission.held新增可选runtime_roots：与候选仓库分离的固定安装根先做有界metadata清单，每个regular文件必须已声明于runtime_files，拒绝reparse和非regular对象；所有目录包括空目录进入原pin及ACL检查。持有完成后、退出前重检目录/file-id/link-count/size与集合，在Git解析前拒绝插入。保留文件hash及native持有核验，不创建新登记权威。

HeldGitConfiguration.assert_runtime_root只对当前同进程/线程活体、已经完整纳入的根核验namespace，未知根和过期对象拒绝。它不授予代码执行、发布或管理员部署权限，也不证明根外native/OS依赖。普通未声明runtime_roots的原接口保持原语义。

devx015-v334-runtime-namespace.xml：4 passed in 16.74s。真实Git/native句柄、正向管理员ACL明确替身；覆盖完整根、空目录原生改名拒绝、未登记根/过期拒绝、新runtime文件在Git前拒绝，以及实际非管理员在metadata前拒绝。Ruff、source_preservation mypy通过。

对v330完整样本只运行metadata namespace核查：22970文件、2662目录，2.981秒PASS；这是文件集合/身份观察，不是重新校验全部内容hash或真实管理员保护。证据DEVX015-runtime-namespace-inventory-20260922.json。没有重复整套持有或Full。

下一步固定入口使用该活体完整根证明，结合runtime identity与同版本检查器源码，再接异根readiness和真实worker/token/exchange；样本v330源码快照未覆盖后续改动，不能冒充当前部署。v7继续，未部署/提交/发布，原DEVX015/OPS080最终验收未完成。所有进程terminal。

### v335 独立检查器 readiness 接入验证（2026-09-22）

已接入受保护运行时检查，包括隔离启动、完整目录持有、Python 和原生模块来源、运行时文件及候选源码一致性。新增真实持有上下文下拒绝普通开发解释器的回归，发现并修复 ExecutionContainmentError 未转换为 readiness BLOCKED 的异常传递问题。Ruff 通过，两项源模块 mypy 通过，readiness 与持有上下文聚焦测试 49 passed in 19.92s。测试使用原有管理员及 ACL 接缝，不构成管理员部署或独立运行时正向验收。独立运行时正向测试、实际 runner 注入、正式 Full、publication 与 OPS-080 仍未完成。当前无需用户 PowerShell 操作。

### v336 Full 前受保护 Git 传输（2026-09-22）

publication fence 的本地 _git 及祖先查询、runner 的 task commitment 对象大小与字节读取已接 inspection 上下文。仅允许明确列出的 ref、common-dir、当前分支、origin URL 读取及两个精确 SHA 的祖先判断；对象内容限精确 SHA 或 SHA:规范相对路径。未选择上下文保留普通调用，已选择但失效的上下文不回退。原候选大小、字节一致性及 canonical 语义校验保留。

真实 Git 与 native custody 聚焦测试 3 passed in 15.11s，普通 publication 并发身份重查及 writer gate 测试 3 passed in 13.98s；Ruff 及三个改动源模块 mypy 通过。测试管理员 ACL 部分仍使用原有显式接缝，不证明正式部署。main 的 token/environment/exchange 注入、其他直接 Git 调用及上下文全生命周期尚未收口，不启动 Full，不改变验收映射计数。当前无需用户 PowerShell 操作。

### v337 runner Git 闭合与回归修复（2026-09-22）

runner HEAD 探测和 profile 两份候选 manifest 原始字节读取接入现有 inspection Git。已选上下文失效时在普通错误兜底之前拒绝；未选上下文仍普通读取，profile no-optional-locks 语义保留。真实持有测试增加 HEAD、show 传输与退出后失效上下文拒绝。

105项组合回归首次102通过3失败，41.79秒；失败均为历史 UnitFullCommandRunner 替身缺 worker_token 导致原 sidecar 断言未执行。经任务登记和preflight后补齐替身的空 worker token/exchange 与 effective_environment，三个失败原用例重跑全部通过12.04秒。未通过修改生产检查或断言消除失败。Ruff、runner mypy通过。证据为首次102通过加修复后3通过，不冒充同一整套重跑。

后续仍需 checkout guard Git传输、main实际worker注入及完整上下文生命周期，再完成正式独立运行时和同一候选验收；Full/publication/OPS080未完成。

### v338 checkout guard 受保护 Git（2026-09-22）

身份、worktree列表、status脏路径及diff check统一经 _checkout_git_result 接现有inspection上下文。普通调用保留原配置参数，受保护调用经有限只读文法验证，失效上下文不回退。原始排除路径序列保留，未读取被排除内容。

新增真实持有上下文的checkout身份/列表/status/diff与索引字节不变测试。首次新测试属性名拼写错误已纠正。组合回归7通过1旧参数下标断言失败30.36秒；断言更新为同时要求core.fsmonitor=false、diff.autoRefreshIndex=false及原精确排除路径后，该用例通过12.10秒。Ruff与两个源模块mypy通过。证据不冒充一次完整重跑。测试ACL/elevation仍用原显式接缝，非管理员部署验收。

后续重点为main实际worker token/environment/exchange及上下文生命周期注入、剩余跨模块Git调用审计。Full、publication与OPS080未完成，无需用户PowerShell操作。

### v339 受信 Full 启动入口（2026-09-22）

新增 run_protected_full，仅接受实时 HeldGitConfiguration、WindowsWorkerToken、WindowsWorkerExchange 及独立环境快照。拒绝只读/恢复/benchmark等不兼容参数，在原Full claim前核验worker、交换目录和固定运行时。候选目录传入原_main，身份与环境注入原_FullCommandRunner，不引入替代执行器。整个调用选择同一inspection上下文，正常或异常退出均复位；readiness仅复用同一有效已选上下文，仍禁止另建嵌套选择。

Ruff及三个源模块mypy通过。source custody、runner、readiness组合155 passed in47.11s；追加原_main向原runner转交能力的单项测试通过6.54s。能力正向组合采用显式token/exchange/运行时准入测试接缝，不能作为管理员部署或双账户原生执行证明。普通CLI原行为经回归覆盖。

待完成：canonical历史Git读取等剩余依赖审计，受信安装/启动脚本和预审材料，真实运行时及worker验收，最终同一候选required tiers/Full/publication，OPS080工程与运营验收。未启用账户，未安装运行时，未运行正式Full，无需用户PowerShell操作。

### v340 canonical 历史 Git 读取（2026-09-22）

_canonical_git_read在已选inspection上下文下使用受保护Git；未选择时保留原始普通行为。有限文法新增精确40位SHA的commit解引用和固定ls-tree-z对象读取，不允许HEAD解引用、tree解引用或路径逃逸。普通blob类型和路径校验不变，失败保持CANONICAL_GIT_READ/缺失对象错误，不回退。

真实持有上下文3项通过16.60秒；普通历史读取精确版本与任务隔离、符号链接拒绝、替换对象隔离4项通过14.72秒。Ruff和两个源模块mypy通过。仍未部署受信运行时、未执行真实双账户Full，最终候选和OPS080验收未完成。

下一步：完成受信启动器部署前材料，并检查发布profile子进程的独立运行时与上下文传递；已接入口不等于正式操作授权或安装完成。无需用户PowerShell操作。

### v341 profile 独立检查器目录绑定（2026-09-22）

发现原_full_readiness_semantics固定要求inspection_code_root等于candidate，导致合法独立检查器记录在最终profile拒绝。现仅在当前held上下文及bind_protected_inspector_runtime再次准入通过后，使用实际固定inspector目录绑定原记录与当前readiness重验；candidate目录与SHA仍分别精确核对。普通无上下文入口保持原同根要求。

新增inspect_protected_full_publication_profile供受信launcher在整个只读probe期间选择实时上下文，不接受序列化替代。8项聚焦测试通过18.09秒，覆盖目录/版本/检查器清单反证、伪上下文和原publication阶段限制；Ruff与runner mypy通过。没有真实管理员部署正例，子进程重新创建held上下文尚待bootstrap接通；未改变180秒原超时预算，未运行Full。

下一步受信bootstrap与子进程启动绑定，随后才具备可复核的一次管理员操作材料。DEVX015与OPS080尚未完成，用户当前无需PowerShell操作。

### v342 子进程安装目录原生准入（2026-09-22）

新增hold_installed_inspector：从自身解释器确定固定runtime，要求隔离无site无字节码及固定cwd/prefix；真实管理员检查在盘点前执行。盘点复用原50000项界限，逐目录/文件ACL检查、有界原生读取和SHA，拒绝reparse及多硬链接，再通过原GitConfigurationAdmission.held建立完整根证明。Git固定为runtime/git/cmd/git.exe，PATH仅安装内cmd/mingw64/bin/usr/bin，清除继承GIT覆盖。只读构建能力，不修ACL或安装账户。

工程I/O上限2GiB，保留单文件64MiB原限制；真实包含Git的完整安装准入耗时仍待测，不能把旧Python原型测量当作新路径结果。原180秒profile超时未改。

5项启动/真实非管理员拒绝与原native custody测试通过21.82秒；补充完整文件集和固定Git环境组合测试通过6.75秒（管理员及Git准入为显式接缝）。Ruff与模块mypy通过。真实管理员正向安装、子进程启动调用接入、Full/publication/OPS080均未完成。当前无需用户PowerShell操作。

### v343 profile 实际子进程入口（2026-09-22）

固定安装中的profile命令使用-I -S -B与protected-inspector开关，cwd固定为解释器runtime目录；子进程通过hold_installed_inspector重新取得真实保护，再调用受保护只读profile入口。开关仅限只读profile，不能要求执行Full；普通开发入口保持原命令。

4项测试通过12.08秒：固定实现路径/隔离开关、普通命令保持、实际开发子进程在事务读取前拒绝且无文件写入、开关不得请求执行。Ruff及两个模块mypy通过。实际安装管理员正例仍未验证。

Git安装只读metadata盘点0.955秒：6356文件、731目录、419124878字节、无reparse，最大文件19553512字节。与既有Python原型654769010字节相加约1.074GB、29326文件，落在2GiB与50000项工程边界内，Git最大文件低于64MiB。仅容量估计，不是新完整安装的哈希/原生持有耗时；180秒profile超时尚未据此放宽。证据位于任务outputs/DEVX015-bundled-git-footprint-20260922.json。

下一步完整原型运行时测量与部署材料，真实双账户及最终Full/publication/OPS080验收仍待完成。目前无需用户PowerShell操作。

### v344 完整Python加Git原型测量（2026-09-22）

保留v330与旧证据，新增任务work/devx015-runtime-probe-v344。四线程复制逐文件源/目标SHA一致，29326文件1073918618字节53.13秒；源码为当前未提交工程快照，不是候选发布。补充原型内测量脚本后的prepared manifest为29327文件1073921588字节。92份RECORD无缺失/越界。隔离启动在故意无效PYTHONHOME及开发PYTHONPATH下通过2.505秒，无外部Python/native，site未加载；bundled Git2.45.1正常启动。

真实原生文件持有与真实Git配置读取、仅管理员/ACL为明确替身：hold_installed_inspector进入191.202秒，总计201.757秒，写入拒绝errno13，峰值65656句柄，184到187。独立复核首次Git capture即184到187，二次capture加GC仍187，增长并非随调用累积；该观察不是所有真实部署资源审计。

已证实仅安装准入超过profile180秒预算，且尚未计真实ACL与完整readiness；不能启动正式验收后才发现超时。下一步通过受保护部署清单提供预先审定哈希，保留完整namespace和原native持有重验，消除启动时重复全量读取；再测量，不直接放宽超时。此轮未改repo实现、未安装、未改ACL或账户，DEVX015/OPS080未完成。证据为任务outputs内runtime-v344系列及installed-custody-v344-20260922.json。无需用户PowerShell操作。

### v345 受保护安装清单消除重复读取（2026-09-22）

启动读取固定runtime-manifest.json，先检查ACL、有界读取并通过原native持有锁定清单，然后严格解析devx015_installed_runtime.v1。文件列表必须完整等于目录盘点加清单自身，唯一规范路径、字节数、链接数、64MiB单文件和2GiB总量边界不变。各文件仍在原GitConfigurationAdmission.held/hold_protected_files中校验ACL、实际哈希和原生身份并保持持有。缺失/坏清单拒绝，不回退实时自生成哈希。

12项测试通过24.37秒，覆盖错误hash、大小、额外文件、重复、自引用、越界、普通启动及真实非管理员拒绝。Ruff及模块mypy通过。正向组合保留明确ACL/elevation和Git准入接缝，真实文件读和持有验证写拒绝。

保留v344新建v345原型，复制逐文件SHA校验51.38秒，恢复3个空目录；加入4737896字节安装清单后29328文件1078660624字节。相同测量脚本和同一合成candidate，真实native/真实Git而ACL为替身：进入持有121.464秒（v344为191.202秒，减少36.5%），总132.216秒（原201.757秒），峰值62268句柄（原65656），写拒绝errno13保留，进程184到187与既有首次Git开销一致。完整真实ACL与profile/readiness尚未测量，不宣称180秒整体预算足够，也未放宽预算。

下一步完整profile剩余时延与部署前材料。运行时仍为普通工程原型，未安装ProgramData、未改账户/ACL、未运行正式Full，DEVX015/OPS080仍未完成。证据为任务outputs内runtime-v345系列及installed-custody-v345-20260922.json。当前无需用户PowerShell操作。

### v346 完整profile时延边界（2026-09-22）

v345隔离运行时独立测量：runtime identity 15.698秒，loaded inspector code1.303秒，72个检查器文件，含导入总17.841秒PASS；无管理员保护准入声明。profile另有多次身份核对，故不能以132秒安装准入低于180秒推断全链足够。

受保护安装子进程采用360秒硬上限，分配为安装准入180秒加原profile180秒；普通入口保持180秒。超时仍PUBLICATION_FULL_CLOSURE_INVALID，不自动重试、不跳过校验。4项命令/超时拒绝测试通过8.23秒，Ruff和模块mypy通过。360秒是明确工程预算，真实管理员ACL与完整成功profile仍待测，非验收PASS。

只读刷新主机状态：AITSWorker仍Enabled=false，SID尾号1010；C:/ProgramData/AITradingSystem.TrustedRuntime不存在，旧DEVX015Canary存在。旧失败canary不可当作干净新安装根，历史reconciled记录仅解释HRESULT，不宣称原FAILED通过。

下一步集中准备可审阅部署包与真实权限验证材料：剔除工程probe/测试替身、冻结文件哈希清单、只在新受保护目标原子创建且拒绝覆盖、不自动修旧ACL/删除旧证据；账户与正式仓库迁移需各自明确范围。此轮未部署、未启用账户，Full/publication及OPS080未完成。当前无需用户PowerShell操作。

### v347 安装传输组件（2026-09-22）

任务outputs/DEVX015-install-transport.cs作为待组合安装器的传输组件：原生源句柄不共享写/删除，校验实际final path、单硬链接、非reparse/目录、精确size和SHA，64MiB上限，确认后返回内存字节。目标写入要求真实64位管理员，使用Windows PowerShell5.1/.NET Framework FileStream CreateNew及创建时安全描述符；不提供修ACL/覆盖路径。

Windows PowerShell5.1编译成功，9项实际源读取/错误hash/size/预算/非规范路径/ADS/占用/硬链接/真实非管理员拒绝通过。PowerShell7不支持该FileStream构造，最终驱动必须在编译前固定版本。未验证管理员正向创建及其ACL，不宣称安装器完成。

父目录保护验证与持续pin、精确目标allowlist、完整manifest驱动和最终候选绑定仍须在驱动实现，禁止单独以该组件部署。工程输入29325文件约1.074GB，哈希核对9.78秒，无需另复制一份运行时。测试夹具保留于任务work/devx015-install-transport-test-146139d7894543dd8a6f090bbde08750，下一轮安装器验证完成并审计证据后可清理。无ProgramData、账户、注册表、调度变更。Full/publication/OPS080仍未完成。

### v348 安装驱动及真实目录保护检查（2026-09-22）

任务outputs/DEVX015-install-runtime.ps1默认Plan，固定Windows PowerShell5.1/64位，组件从已验证hash的内存字节编译。固定目标C:/ProgramData/AITradingSystem.TrustedRuntime/v348-engineering，根已存在拒绝。安装分支创建时设置受保护SDDL，不覆盖、不修ACL、不启用账户、不执行runtime。CreateDirectoryW拒绝已存在目录，CreateNew拒绝文件覆盖；连续pin系统盘根、ProgramData及新目录，源与目标字节通过原生读取重验。该分支未执行，无真实管理员正例声明。

Plan完整读取29325文件1073917523字节，3389目录，33.0126秒PASS，目标仍不存在。固定工程输入SHA 16bb5d711962a4fe55eff99bc6e8e87229e2bf06806ff8fc68cbabb6d9ef2702，安装manifest预期SHA 36879e544619c86838f63dea3063a6a45e98de35b542e6b91e4a1767e6279817。candidate_sha为空，不作最终发布证明。证据DEVX015-install-plan-20260922.json。

目录组件7项真实检查PASS：ProgramData父ACL及祖先pin、持有期间rename拒绝、Dispose后rename成功、junction拒绝、非管理员创建拒绝、普通用户目录拒绝、已释放对象拒绝。测试夹具保留work/devx015-directory-pin-test-5723ab7428114675ab9fa2ced9262b5a，后续证据审计后清理。旧9项传输证据不变。

仍需管理员安全创建及最终文件ACL复核、在执行前校验入口字节的bootstrap、实际双账号启动、最终候选Full/publication和OPS080。本轮未安装或修改账户/注册表/调度，当前无需用户PowerShell操作。

### v348 安装入口v2审阅就绪（2026-09-22）

保留旧组件及Plan证据，新建DEVX015-install-transport-v2.cs和DEVX015-install-runtime-v2.ps1。每个目标文件（含manifest）写入后读回hash并逐一比较owner/group/DACL与创建时预期SDDL。普通用户源码文件被实际拒绝。入口固定artifact目录，已通过SHA校验内存字节再ScriptBlock执行的完整Plan，11.8619秒PASS。前次33.01秒，未控制缓存，不能宣称代码优化。

任务outputs/DEVX015-install-review-20260922.md列明精确ProgramData新目标、约1.08GB、拒绝覆盖/失败保留、零账户/项目ACL/注册表/调度/runtime执行变更及代码hash。已请求本次安装授权，尚未收到回复或执行。当前进程未提升，可能需用户UAC确认。此新增安装不在原第一阶段合成权限授权内。candidate_sha仍空，双账号、Full/publication/OPS080继续未完成。

### v349 治理接续及已安装状态（2026-09-23）

用户已授权原安装与保留现场续装。工程runtime固定位置C:/ProgramData/AITradingSystem.TrustedRuntime/v348-engineering，续装206.249秒成功，独立全部字节/文件集/ACL复核18.621秒成功：29326文件、3389目录；AITSWorker仍禁用，未执行已安装runtime。原首文件ACL generic mask表示差异失败证据保留，v3规范预期mask后不放宽实际权限。

新代码登记时旧v7租约过期，原入口拒绝PUBLICATION_LEASE_EXPIRED，未修改实现。经正式release保留FAILED/RELEASED。首次v8 acquire错误重复传入两个自动声明的排他资源，得到PUBLICATION_PATH_DUPLICATE，尚未取得lease；移除重复CLI声明后按相同范围成功，不改变资源排他范围。

新事务devx-015-execution-identity-20260923-v8，SHA 5c400368f9d91480fb40f805ae6485f411abc7c0fc01007a778e3de833745270，lease-d364ae1b72dbcf21ade7，阶段TASK_SOURCE_PRE_WRITE。原HEAD10ac47c及main03d10b4未变。原canonical writer登记成功，新SINGLE_LANE preflight PASS。

下一实现：受信任调用方本地worker令牌取得上下文，内存凭据清零，复用原primary工厂的SID/elevation/session校验与句柄释放；先显式API替身测试，禁止实际登录/启用账户。已安装快照不随开发源码变化而改写。实际跨账号启动与权限条件、最终candidate/Full/publication及OPS080 S4/S5仍未完成。

### v349 本地worker令牌获取与生命周期（2026-09-23）

新增WindowsWorkerToken.logon_local受信任调用上下文：仅本地账户domain点、LogonUserW interactive/default，无重试或API回退。凭据必须为调用方独占的NUL终止ctypes wchar数组，不接受不可清零字符串/请求JSON；所有拒绝路径清零，并在yield之前清零。不启用账户、不授予权限、不加载profile、不授予启动权。调用方仍须从受保护配置提供account/expected SID及获得授权。

复用原from_primary_handle校验primary/SID/非提升及复制能力，再validate_launcher验证身份分离和session，上下文结束或异常时释放副本与原登录句柄。显式拒绝API声称成功但缺失句柄。

首轮10项新API替身及2项既有真实token回归共12PASS/14.52秒；mypy发现可空handle后增加显式空句柄拒绝，相关6项生命周期回归PASS/7.28秒。最终Ruff和该源模块mypy通过。新登录路径使用明确API替身，没有真实账户登录/启动证据，不汇总两轮为18个独立验收场景。参考微软LogonUserW：https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-logonuserw。

已安装v348工程快照保持原字节，未自动更新。新增源实现仍需最终候选绑定后按审阅部署流程安装。尚缺受保护身份配置/凭据供应、实际启动权限及环境策略、真实双账号正例、最终Full/publication/OPS080。v8事务继续TASK_SOURCE_PRE_WRITE，无真实账户/主机权限修改。

### v350 worker身份输入合同（实现前冻结）

仅作为可信launcher的账户选择绑定，不替代现有仓库/主机注册及发布权威。固定位置为已准入runtime版本目录的父目录/worker-identity.json，不接受CLI/env配置路径；采用devx015_worker_identity.v1，精确字段schema_version、host_id、account、sid，无密码字段。host_id必须等于machine_host_id；account仅本地名称，sid必须本地账户SID格式。配置通过管理员保护检查、有界16KiB读取、native持续持有后解析，持有覆盖整个token生命周期。先复用bind_protected_inspector_runtime证明运行实现与候选一致，再读取身份。密码只从可信调用方独占wchar缓冲区传入，所有失败均清零；不在此合同决定凭据持久化，不创建配置或启用账户。现有machine_registration.v1保持原schema。

### v350 受保护身份与登录组合结果（2026-09-23）

新增WindowsWorkerToken.logon_registered。先实际管理员构造与原bind_protected_inspector_runtime准入，再从固定runtime父目录/worker-identity.json读取身份；不接收可变配置路径。检查父目录/文件ACL，16KiB有界单链接读取，经原hold_protected_files持续native持有后严格解析四字段与当前host_id；在持有期间调用logon_local并保持到token退出。密码在包括准入/解析失败的所有路径清零。无创建配置、账户、密码持久化或真实登录动作。

18项组合与既有登录生命周期测试PASS/11.60秒；runtime/admin/token为明确接缝，实际bounded file读取保留。覆盖wrong host、extra password field、重复key、runtime/custody拒绝、调用方异常及清理顺序。Ruff和源模块mypy通过，格式修正后Ruff再次通过。不是管理员/真实跨账号验收。

仍需可信凭据供应和真实身份部署的具体方案、实际启动权限/环境验证、最终候选与安装源码同步、Full/publication及OPS080。已安装v348快照仍不修改。v8继续TASK_SOURCE_PRE_WRITE。

### v351/v352 凭据读取与原生解密检查（2026-09-23）

v351 canonical结果已通过原writer登记（governance cycle 943），状态IN_PROGRESS。4项confidential ACL/custody测试12.31秒通过，Ruff通过；未限定导入的mypy报告8文件44错误，包括coordination的9项，未宣称类型通过。

v352新增decrypted_worker_password：仅接收有界密文与32字节binding，原生CryptUnprotectData以UI_FORBIDDEN解密，不产生明文Python字符串；返回原生分配上的可写wchar数组视图。所有退出路径先memset清零再LocalFree。调用者必须在上下文内消费，不能保留视图。5项真实DPAPI合成测试6.85秒通过，覆盖正常、调用方异常、wrong binding、未终止和内部NUL；LocalFree观察接缝检查释放前字节清零，解密与释放仍使用真实API。Ruff通过；mypy --follow-imports=silent单源模块通过，不代表全仓类型检查。

尚需把该原语与hold_confidential_file及logon_registered组合。没有写入真实凭据、启用账户、改写已安装v348快照或运行Full。测试未见慢阶段，本轮不重复昂贵安装/全清单校验。最终candidate/Full/publication及OPS080 S4/S5继续未完成。

### v353 固定密文读取接入登录入口（2026-09-23）

logon_registered保留显式可清零缓冲区模式；不提供缓冲区时，在原身份文件持续持有、严格字段/current host检查之后，读取固定runtime父目录/worker-credential.dpapi。通过hold_confidential_file检查SY/BA精确ACL并保持原生文件持有，以原始identity文件SHA256的32字节摘要绑定DPAPI。释放顺序为token退出、明文清零并LocalFree、秘密文件释放、identity释放。失败不回退到其他凭据源。无新增账户/权限/主机写入。

首轮19项含真实DPAPI5项通过8.11秒；增加秘密ACL与decrypt拒绝后最终16项组合测试通过6.76秒。组合测试明确使用runtime/admin/decrypt/token接缝，不等同真实管理员登录；原生DPAPI由独立测试覆盖。每次组合runtime准入次数固定为1，避免包装旧入口带来重复全清单校验。Ruff及mypy --follow-imports=silent源模块通过。

下一步准备身份与密文配置部署、账户重置/启用与失败禁用的具体可审阅入口，再取得对应操作授权；现安装授权不覆盖这些账户变更。已安装v348工程快照仍未同步新增代码。真实双账号启动、最终candidate/Full/publication及OPS080 S4/S5未完成。

### v354 主机正例后的正式闭合审计（2026-09-23）

原canonical writer登记主机v2登录和挂起Job创建PASS，cycle948，仍IN_PROGRESS。用户授权的worker配置、JACK单项赋权、新token和单次v2主机探测均有独立输出receipt；worker最终禁用、child已终止，未执行Full/PIT。

当前manifest仍PARTIAL_NOT_ACCEPTANCE_READY/NOT_EXECUTED，缺13映射：I05 prerequisite_then_old_source/real_frozen_lane_replay；L03 old_new_writer/ttl_while_executor_alive/parent_dead_child_alive/lock_file_replaced/store_alias/migration_crash/old_entry_restart；X05 real_cli_repository_identity/baseline_defect_reached/required_platform_available/mandatory_collection_complete。不得用主机探测填充这些正式映射。

对三个具体文件比较开发与v348安装字节：workflow_execution.py、workflow_coordination.py不同；run_validation_tier.py相同。没有全清单重扫，不外推其他文件一致。定向搜索确认登录、安装持有与run_protected_full入口尚无正式组合调用者。下一实现应连接原入口、受保护身份/密文、exchange与明确worker环境及异常释放；完成后再冻结最终候选和部署，避免重复安装。相关审计位于chat outputs/DEVX015-post-host-closure-audit-20260923.json。OPS080 S4/S5仍待DEVX正式闭合。

### v355 安装运行时到Full的组合调用（2026-09-23）

新增run_installed_protected_full：先复用统一参数拒绝条件，再持有hold_installed_inspector，在Git inspection内读取候选commit，通过logon_registered消费固定身份/密文，创建一次性full-exchange UUID目录并持续持有，调用原run_protected_full。保留原Full gate及再次绑定检查，不启用账户、不自动安装或删除证据。

worker环境由OS GetSystemDirectoryW、已准入runtime路径和exchange profile构造；不继承launcher os.environ，固定PATH/Git配置、禁止usersite/bytecode，TEMP/HOME/profile位置明确。该目录是工作文件profile，不代表已LoadUserProfile或HKCU加载。真实完整环境合同与候选测试兼容性仍需后续验证。

5项接缝测试PASS/7.88秒，覆盖成功返回值及admission/logon/exchange/Full异常，确认exchange→token→runtime释放顺序、污染PATH和额外env不继承。只是真实OS系统目录读取，其余能力明确接缝，不宣称真实Full。Ruff修正一个导入排序后通过；初次mypy未绑定MYPYPATH读到已安装包而失败，绑定当前src后--follow-imports=silent单模块PASS。

本轮未运行账户、主机exchange、Full或更新v348快照。新组合仍须受保护可信入口实际调用、完整环境/profile验证与最终候选绑定；13项映射及DEVX015/OPS080正式验收仍未完成。

### v356 原CLI接入受保护Full组合（2026-09-23）

原runner新增--protected-full-candidate-root显式入口，main分派到run_installed_protected_full；普通_main若收到该参数却没有活体protected能力则拒绝，不能走普通子进程路径。组合入口要求参数candidate与显式root一致，绝对路径及Full/transaction/write-runtime参数先验证，之后才运行安装准入。未开启任何自动安装、账户启用或授予权限。

4项CLI测试及5项既有组合测试共9PASS/8.64秒，覆盖正确路由、print-only拒绝、相对路径拒绝、缺事务拒绝、绕过组合拒绝与异常释放。Ruff和绑定当前MYPYPATH的mypy --follow-imports=silent单模块PASS。测试使用明确能力替身，未运行真实Full或创建主机exchange。尚待实际worker环境/profile合同验证、安装候选同步及13项映射/最终Full/publication/OPS080。

### v357 修正一次性exchange重复消费（2026-09-23）

发现v355外层exchange.directory与原_run_mandatory_acceptance_command中的exchange.directory重复；真实WindowsWorkerExchange._used会拒绝第二次进入。前一组合替身没有调用原mandatory消费者，因此漏检。新回归实际调用原mandatory入口到checkout准入拒绝边界，确认目录仅由该消费者持有，禁止派发pytest。

先红2FAIL/3PASS复现外层提前消费（v357-red.xml），移除组合外层directory后9PASS/8.63秒（v357-green.xml，包含CLI回归）。现在运行时/token外层持有不变，由原mandatory流程唯一消费exchange。Ruff、正确MYPYPATH单模块mypy通过。目录保护和内部生命周期未放宽，未真实执行Full或主机变更。

后续继续环境/profile与精确候选部署、缺失13项正式映射、Full/publication及OPS080 S4/S5；不能以本轮回归替代最终验收。

### v358 实际pytest/xdist环境烟测（2026-09-23）

读取当前pytest实现确认配置pythonpath在显式-p插件加载前处理；项目pyproject配置src和点。对现有未保护诊断runtime v345实测：设置PYTHONDONTWRITEBYTECODE后dont_write_bytecode=True，未证实“_pth忽略变量导致写缓存”的猜测，因此不修改命令语法。

在chat work/devx015-environment-smoke-v358独立合成项目，调用当前_installed_worker_environment构造环境，运行诊断python -m pytest -p合成本地插件 -n2 --dist loadfile；4PASS，进程总4.125秒、pytest报1.14秒。实际验证插件来自candidate-local src、子进程isolated/no_site/禁止bytecode、TEMP/USERPROFILE与固定profile一致、GIT_CONFIG_NOSYSTEM和秘密变量不继承。证据outputs/DEVX015-environment-smoke-v358.json。

这是普通JACK进程和未保护诊断runtime，不是AITSWorker或已安装runtime，也不证明HKCU/profile加载、真实ACL、正式Full。未登录或启用账户，未更新安装。合成夹具保留用于证据，实际Full/13映射/候选publication和OPS080仍未完成。没有因猜测增加新的启动协议。

### v359 source-job 终态与耗时证据（2026-09-23）

原session63002退出0，1PASS/625.05秒；JUnit1测试无失败、错误或跳过，testcase618.332秒。原canonical writer结果PASS/cycle957，SINGLE_LANE preflight PASS，无新测试派发。worker日志分段：PREPARE16.551秒，GENERATORS180.816秒（内含architecture-manifests88.897秒），FINAL_INPUT_RECHECK17.347秒。worker_result写入到installation_plan写入约261秒是观察区间，包含原测试重放、纯plan准备、后续安装入口再验证等，不能直接归因为安装或锁等待。绝大部分总耗时在测试体，外围约6.7秒，减少xdist数量不足以解决主要成本。

I05闭合审查：原source-job在source-final-handoff后释放失败publication事务，formal NOT_EXECUTED；现有_run_actual_profile_full另写duration/profile并提交fixture candidate，不能直接拼接当作原始source已经完成正式终态。下一步必须让同一原始source身份贯穿批准的最终候选与原Full/publication门禁，保留旧source handoff和失败receipt，不能用新替代source工作区或synthetic Full结果填映射。13项映射、最终Full/publication及OPS080 S4/S5仍未完成。

### v360 同源final准入前置缺口（2026-09-23）

原合同V3已明确source交接后使用普通final事务lane_head=S/expected_main=M，经Atlas形成C[S]并取得自己的验证；不要求S字节永远不变，也不需要新增handoff协议。此前关于Full helper的结论应限定为不能直接拼接既有helper作为I05完成证明，不能误解为禁止合法C[S]生成。

对保留的v359 S=50030fbd4c24a11dfcbae5d83b1f7a9111ef260f只读检查：最初开发checkout脚本检查fixture因READINESS_INSPECTION_ROOT_MISMATCH拒绝；随后在原fixture cwd加载其自身src中的检查模块，0.119922秒以READINESS_INSPECTION_CODE_NOT_COMMITTED scripts/validation_readiness.py拒绝。checks为空，后续依赖未评估；无Full、PIT、账户或fixture写入。两份结果保存在chat outputs/DEVX015-v360-source-readiness.json及DEVX015-v360-source-local-readiness.json。

根因是source-job fixture未像full-readiness/profile分支那样在source冻结前纳入readiness入口。既有whole_profile又会把测试替换为Job机制探针，不能作为原I05验收。下一步在原fixture构造流程补齐冻结前的真实readiness/runner及必要输入，保留原测试与源码身份，再接既有普通final事务；不把旧S重写为新S、不建立替代source目录、不通过删检查或伪造Full关闭I05。先做便宜的提交对象/input检查，再运行昂贵链路。

### v361 冻结前真实readiness入口修复（2026-09-23）

原_install_source_job_runtime增加真实scripts/run_validation_tier.py与scripts/validation_readiness.py，均在M/source冻结前复制，未改变产品门禁或旧v359候选。新增test_source_job_freezes_real_readiness_entrypoints[source-job]使用实际canonical source-job fixture，验证HEAD=M、原_inspection_code_identity通过、两个入口的Git blob/工作文件/真实项目字节一致；随后改动readiness文件确认原DIRTY拒绝，finally恢复并重新通过。

原session40582退出0，1PASS/55.56秒，XML outputs/validation_runtime/devx015-v361.xml；Ruff与限定diff检查PASS。canonical prereg/result和SINGLE_LANE preflight均PASS。独占fixture D:/Work/devx015-v361保留至证据归档且无依赖后清理。该回归只覆盖冻结输入身份，未派发source生成、安装、Full或PIT，不增加I05映射。下一步对新fixture原committed模块执行只读readiness，定位剩余真实输入，不能把identity PASS当完整readiness或终态接受。

### v362 真实source优先，停止扩充合成夹具（2026-09-23）

v361保留fixture原已提交入口readiness实际运行23.95秒，identity PASS；retained_evidence缺六政策、Atlas缺policy mapping、compatibility缺fragment，共8 blocker。architecture_generated PASS耗时22.65秒，canonical/report PASS。输出chat outputs/DEVX015-v362-readiness.json，无Full/DQ/PIT或账户动作。

重要方向修正：既有_seed_readiness_retained_evidence明确生成synthetic research payload，只能检验工程哈希准入；不能靠继续补此类fixture证据完成真实I05。v361入口修复保留作为廉价防回归，但不再把完善source-job夹具当真实项目验收的前置无限扩张。回到真实开发source：显式事务src/scripts/tests allowlist当前18文件4528新增213删除；原committed V1/V2 preservation验收要求真实source commit后才可执行。下一步检查当前v8原source候选准备/生成/保存流程及所需完整输入，保存现有实现身份，再跑真实committed验收与C[S]最终门禁，不直接跳过事务提交、不以synthetic PASS填映射。OPS080 S4/S5仍保留。

### v363 实际candidate旧review不适用（2026-09-23）

实际checkout原CLI candidate-input-inspect失败WORKFLOW_MERGE_CANDIDATE_DELTA_UNCOVERED，尚未生成或安装候选。用原collect_checkout_dirty_paths及_exclusions、_candidate_source_paths定位：冻结review 37路径，当前20路径，新增6、缺席23。新增为runner、page_effectiveness、source_preservation、validation_readiness及两对应测试；详情chat outputs/DEVX015-v363-review-delta.json。缺席是当前dirty集合不含，不代表文件丢失。

旧review路径outputs/architecture/workflow_integration/reviews/7e8ece48df29c6c522809f6352312a914a3f4efceb34480082ad5aedd38d0b29.json，scope latest_main03d10b4a2071ce6b9bbc87e982714471052b1db1；真实HEAD10ac47c94b6958147498656041c31c43ac8fe181。原capture进一步要求HEAD=scope latest_main，因此只改review路径集合也不足。下一按当前任务分支source checkpoint保存边界检查请求/现有租约兼容性，再进行真实原集成审阅；不直接覆盖旧review或把HEAD伪称main，不重复派发已知不满足门禁的candidate。canonical v363 result PASS，全部最终验收仍待完成。

### v364 source自身提交准备（2026-09-23）

TaskCheckpoint只读_implementation_binding明确拒绝SOURCE_PRESERVATION_IDENTITY: trusted implementation is not exact committed source，未capture。不能用未提交的自身implementation绕过这个守卫保存自身。证据chat outputs/DEVX015-v364-checkpoint-binding.json。

找回实际既有工程source保存前例：v285在原事务GENERATED_REBUILD_PRE/POST和CANDIDATE_COMMIT_PRE之后，按scope精确路径源码提交产生当前10ac47c；v286真实committed E2E已通过。复用同一工程流程而不是再造checkpoint机制。下一执行当前v8生成器顺序canonical/architecture/report seal-build/compatibility及bundle校验，准备新精确pathspec提交脚本（不运行旧v285脚本，换新事务和证据路径，所有Git检查带范围或完整排除集）。main不动，source commit保持IN_PROGRESS而非final C。原旧OPS080 receipt仍绑定10ac，不能重解释。

v364 canonical已预登记，尚未推进GENERATED_PRE，未生成或提交；写完本节后再冻结文档，避免生成后反复追加造成seal漂移。下轮直接继续当前v8生成器准入和保存流程，不再重审fixture/readiness或重试checkpoint绑定。

### v369 剩余L03实现边界与新事务

当前93/106未改变。代码审查确认公开control-enroll/control-enroll-recover仅到DRAINING，返回activation_allowed=false/old_binary_os_fence_installed=false；host_cutover_custody真实持旧新arbiter并校验排空，但明确不改phase、不授权ACTIVE。已有registered_control/monkeypatch/helper测试不能直接映射全部L03。下一关键实现必须把旧binary OS fence、LEGACY_WRITERS_DISABLED到ACTIVE及失败恢复连成原公开入口，不新增第二store/scheduler；实际主机变更仍需要具体审阅授权，现未操作。

新事务devx-015-final-closure-20260923-v9，lane_head3c28347e，原v8完整scope/generator/required tiers复用。首次acquire因为传入了API自动添加的两个resource而PUBLICATION_PATH_DUPLICATE，在路径检查阶段拒绝；去除参数中自动项后acquire PASS（并未缩减结果scope）。已TASK_SOURCE_PRE_WRITE，canonical devx015-v369-remaining-closure登记源码提交/2PASS/Atlas结果及后续缺口。源码HEAD不变；本轮canonical写入后最终生成和Atlas新候选绑定仍须在下一自然提交边界统一更新，不能无条件延用v368 CURRENT结论。

### v370 L03状态切换恢复的具体实现约束（2026-09-23）

实查HostControlBinding.assert_current以HKLM registration.state_sha256读取固定CONTROL_STATE_NAME；_WindowsEnrollmentAdministrator.write_admin_json对已有不同bytes报ENROLLMENT_ARTIFACT_CHANGED，仅支持首次写入/相同重放。现有register为首次登记，不可直接当phase切换恢复。不能把state.phase直接改ACTIVE再补registry来宣称完成。

实施顺序：1. 从原trusted registration、物理根、原policy和完整旧新root inventory构造精确前后状态及只读计划，禁止任意caller自报fence已完成；2. 在原host_cutover_custody的全部arbiter内重新验证完整lease/execution/真实进程和Job排空，未知即拒绝；3. 管理员持久化受保护精确前后bytes/身份的切换日志，证明旧binary OS写入被封锁及祖先不可替换，不能仅用ACL文本或phase标志；4. 使用同一精确日志按顺序发布状态文件和HKLM注册哈希，每个断点允许fail-closed，公开恢复只接受已记录的有限前/后组合，未知bytes/epoch/SID/root不覆盖；5. 独立读回后才可ACTIVE，恢复不能通过重新启用旧writer退出。

测试必须覆盖日志前后、状态发布前后、注册发布前后与完成回执窗口，并保留旧新同时写、live TTL、父死子活、锁替换、root alias、旧入口重启的真实oracle。合成transport可做协议单元回归但不填完整L03；真实主机动作需具体范围授权。旧写句柄不会被DACL更改撤销，无法证明排空/封锁时不得继续激活。本轮仅边界审查及canonical预登记，未新增状态切换API、未改ACL/HKLM/服务/账户。

### v371 cutover恢复现场纯判定实现

新增workflow_coordination._cutover_publication_position：bounded strict JSON验证，前后状态只允许DRAINING到LEGACY_WRITERS_DISABLED或后者到ACTIVE，其他字段完全相等；注册只能改变state_sha256并绑定原始状态bytes。只返回BEFORE_PUBLICATION、STATE_PUBLISHED、REGISTRATION_PUBLISHED。注册先写、未知bytes、epoch漂移拒绝。纯函数不提供完整schema/来源认证、custody或activation资格，必须由后续受保护日志与原临界区调用；当前尚未接公开入口，不计L03映射。

10项focused PASS/7.76秒，XML repo outputs/validation_runtime/devx015-v371.xml，Ruff与精确scope diff PASS。测试为合成协议输入，无主机ACL/HKLM/账户动作。v371 prereg、preflight、results均通过。下一接受保护journal与原生有条件发布/恢复，不能把这个helper当迁移完成。v9 lease-50253ba863ba783f813f仍TASK_SOURCE_PRE_WRITE，HEAD3c28347e不变，当前无运行session。

### v372 受保护cutover journal读取接线

管理员新增hold_cutover_journal(root,digest)只读上下文：固定cutover-<digest>.json，复用原hold_protected_files的ACL/目录/文件持续持有；有界9MiB日志、四个各1MiB以内小写hex文档，strict JSON及精确schema键，完整原state/registration校验、物理root关联，再用v371单步phase/注册哈希判定。_control_state校验拆出_validate_control_state复用，原现场读取不变。yield期间持续持有，异常出口释放。不提供完整操作授权/custody/旧writer封锁，不写ACL/HKLM或state。

新日志transport seam五项、v371十项、原state严格类型五项，共20PASS10.84秒，XML outputs/validation_runtime/devx015-v372.xml。测试真实解析/物理identity/恢复判定，权限transport显式替身，因此非真实管理员日志接受或L03完成。Ruff首次两格式问题修正后PASS，限定diff PASS。canonical pre/results及preflight PASS，session均terminal。下一原生有条件发布/注册读回与公开恢复仍待实现，不能把只读journal当激活资格。v9仍TASK_SOURCE_PRE_WRITE，HEAD3c28347e不变。

### v373 状态先写窗口的恢复准入阻塞已定位

在实现native writer前审查确认：resolve_host_control_binding末尾binding.assert_current，后者按当前HKLM registration.state_sha256验证固定state；host_cutover_custody在拿任何arbiter前调用此链。因此合法STATE_PUBLISHED窗口也无法经普通custody取得恢复锁。不能先接writer而把无法进入的恢复留空，更不能放宽普通写入入口的哈希校验。

下一实现应是管理员日志绑定的恢复准入：读取真实注册原始bytes（现_trusted_host_registration返回parsed dict，须保留原byte身份而不自行规范化替换证据），持续持有v372 journal，验证当前state/registry精确位于v371有限组合，从完整已验证journal的旧state取物理root清单，确认全部原root仍存在、无别名重叠，再按原顺序持同一hold_lease_arbiter并重放全部租约/观察OS执行树；无复制ACTIVE、TTL释放或新store。特别原hold_lease_arbiter内部会mkdir/初始化，所以缺失root必须在此前明确拒绝，不得恢复为新authority。普通binding保持不一致即拒绝。只有取得这项原生恢复custody后再接条件写入和公开恢复CLI。

本轮canonical已预登记devx015-v373-recovery-admission-boundary，无新增代码或主机操作；前轮v37220PASS证据不扩展为恢复准入已通过。v9保持TASK_SOURCE_PRE_WRITE，HEAD3c28347e，所有会话terminal。下轮直接实现该恢复准入，不再重复审查同一调用链。

### v373 原始注册文本读取已实现

_trusted_host_registration_bytes从原固定HKLM64入口读取REG_SZ，保留严格完整schema与实际物理identity校验，再返回原文本的UTF8（不是底层UTF16数据，也不重序列化）。普通_trusted_host_registration复用该读取并保持dict返回/严格校验。六新场景验证空白格式原样保留、仅缺key可None、缺value/denied/错误kind/未知schema拒绝；加原journal/state回归16PASS11.35秒，XMLdevx015-v373.xml。Ruff修正单个行宽后PASS，限定diff PASS。Winreg读取明确替身，无真实注册写入。

本轮只完成恢复读取先决条件，尚未实现journal绑定恢复custody、native写入或公开CLI。下一直接组合原raw registration与已持journal，在原物理根完整存在且已排空条件下取得原arbiter；普通binding不放宽。canonical result PASS，HEAD3c28347e/v9不变，无活动会话。

### v374 日志绑定的中断恢复锁已实现

host_cutover_recovery_custody 在受保护日志持续持有期间，仅锁定原新旧物理根；保留完整状态及注册 schema、策略摘要、退休标记、租约重放和执行终止检查。允许精确 before/before、after/before、after/after 字节组合；倒序或未知状态拒绝，缺失旧根不重建。退出后 custody 失效，异常路径释放锁。无状态、注册或 ACL 发布，也不授予激活权限。

六项测试 PASS（16.22 秒，outputs/validation_runtime/devx015-v374.xml）；独立子进程验证双根锁竞争与退出释放。管理员文件传输和目录 pin 为明确测试替身，不视为真实管理员 ACL 或主机迁移验收。Ruff 和限定路径 diff 检查 PASS。后续仍需条件状态发布、原生注册更新、旧二进制 OS 隔离证明及真实验收。v9 保持 TASK_SOURCE_PRE_WRITE，HEAD 3c28347e，未执行 Full/PIT。

### v375 条件状态发布原语与差异收敛

新增内部 _WindowsEnrollmentAdministrator._publish_cutover_state，绑定持续有效的 recovery custody 和日志中的精确 after_state。仅 BEFORE_PUBLICATION 创建受保护唯一暂存文件，原生 WriteFile + FlushFileBuffers，重验现场后 MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)，再独立读回有限状态。STATE_PUBLISHED / REGISTRATION_PUBLISHED 重放不重写。未知现场拒绝；失败保留暂存文件，不自动重试。此原语依赖原 arbiter 串行化及管理员保护，不声称对其他管理员提供 OS compare-and-swap；调用方仍必须先建立旧 writer OS fence，目前没有公开激活接线。

测试 devx015-v375.xml：9 PASS / 22.34 秒；真实 Windows 文件创建、写入、刷盘、替换，真实独立进程锁竞争。ACL、目录 pin、注册读取为明确替身，发布失败为故障注入；不能计作真实主机迁移或 Full。Ruff/限定 diff PASS。未见异常慢阶段。测试目录 D:/Work/devx015-v375 由本任务持有，正式验收后确认无依赖且证据归档再清理。

另修正 v374 整文件格式化引入的无关差异：按 AST 与注释一致性恢复原有未改函数/方法排版，恢复前后完整 AST 等价；仅保留实际功能变化，不重跑无关昂贵测试。后续仍为精确注册更新、OS fence、公开有限恢复及完整 L03/I05/X05 验收。HEAD 3c28347e，v9 TASK_SOURCE_PRE_WRITE，未执行 Full/PIT/主机变更。

### v376 精确注册发布与刷盘失败恢复

新增内部 _publish_cutover_registration，仅使用固定 HKLM64 已有注册键的 OpenKey，不创建缺失 key。仅 STATE_PUBLISHED 可以写日志中的精确 UTF8/REG_SZ 文本；核对原值、持续 custody、SetValueEx、FlushKey，再检查句柄值和独立路径读回。错误顺序、未知现场、缺 key 均拒绝。REGISTRATION_PUBLISHED 重放不重复写值，但必须 FlushKey，以覆盖此前 SetValue 成功而 Flush 失败的有限恢复位置；单次失败不自动重跑。

v376 13 PASS / 31.04 秒（outputs/validation_runtime/devx015-v376.xml），Ruff 与限定 diff PASS。新增正常注册、刷盘中断、未知值、缺失键及写入前顺序拒绝；原状态/双根锁回归保留。Winreg 传输和 ACL 为明确替身，真实临时文件与双根竞争沿用原生；没有真实 HKLM、账户、Full 或 PIT 操作，不补记最终 L03 验收。用时与增加四个双根竞争场景一致，无异常慢阶段。

剩余关键实现是旧 writer OS fence 的实际证明、日志生成及公开有限恢复入口；两个内部 transport 均不自行提供旧句柄排空证明或激活授权。v9 仍 TASK_SOURCE_PRE_WRITE，HEAD 3c28347e。D:/Work/devx015-v376 保留至正式验收与证据审计后清理。

### v377 目录写共享屏障的真实能力边界

pin_directories 新增 deny_target_writers，默认保持原行为；显式目标使用 READ-only sharing，祖先继续 READ|WRITE sharing。真实 Win32 验证现存/新开 FILE_ADD_FILE 与 FILE_ADD_SUBDIRECTORY 写句柄均拒绝，DELETE 仍拒绝，失败和成功退出均无句柄泄漏。它不是完整 OS fence：不能单靠目录分享规则禁止按路径新增子文件，仍需递归 protected ACL、全部文件持有及原 custody；属性/DAC/owner 等既有权限不能由该测试推定已排除。

v377-verified.xml 四项 PASS / 8.82 秒；Ruff/限定 diff PASS。首次 v377.xml 的 FILE_DELETE_CHILD 在打开测试句柄时被现有 Modify ACL 拒绝（WinError5）；只读复核确认后改测 DELETE。v377-fixed.xml 又发现默认 pin 本就拒 DELETE，修正该测试前提；两失败 XML 和现场均保留，不视作真实 fence 通过。无 ACL、账户、HKLM 或 Full/PIT 变更，未见异常慢阶段。

D:/Work/devx015-v377、D:/Work/devx015-v377-fixed、D:/Work/devx015-v377-verified 由本任务保留至正式验收/证据审计后清理。下一继续组合完整旧根 inventory 和原文件 custody，再接日志/公开有限恢复。HEAD3c28347e、v9 TASK_SOURCE_PRE_WRITE，整体未验收。

### v378 原生反例：分享屏障不能清空既有安全权限句柄

只读实测原保留测试目录，打开各权限句柄后进入 deny_target_writers 屏障：FILE_ADD_FILE / FILE_ADD_SUBDIRECTORY / DELETE 均拒绝（32）；FILE_WRITE_ATTRIBUTES / WRITE_DAC 可同时持有；WRITE_OWNER 在打开时被原 ACL 拒绝（5）。探针 0.0195 秒，未写 ACL/内容，句柄全部关闭。证据 outputs/validation_runtime/devx015-v378-existing-handle-audit.json。v377 的四项 PASS 仍仅证明其具体权限集合，不可推广为完整旧 writer OS fence。

因此不能直接把递归 ACL 检查和文件/目录分享持有拼成完整隔离证明。下一评估维护边界：先冻结完整旧根及祖先、保存可信原字节/身份清单、保护新可信日志和旧根；在单独授权维护窗口清空全部旧进程句柄；随后按实际 boot identity、完整 ACL/父路径/物理身份/字节清单重新验收，才允许状态与注册发布。重启方案尚未最终审阅，不能使用壁钟或普通注销代替可证明的边界；不新增启动任务/服务，不自动重启或注销。

待明确完整主机 root/entrypoint inventory 和现有活动租约排空后，才能生成可批准的精确动作方案；此记录不授权任何主机变更。内部状态/注册 transport 保留，公开激活关闭，L03/I05/X05、Full/发布和 OPS080 验收未完成。v9 TASK_SOURCE_PRE_WRITE，HEAD3c28347e不变。

### v379 已知旧根盘点与维护范围前置

D:/Work/AITradingSystem* 一级真实 Git checkout 共20个，16个存在旧 lease root、4个缺失；当前 decoder 对15个旧根重放 PASS，历史 AITradingSystem_ops_runtime_20260725 因 checkout policy.v1 被当前入口拒绝，读取其原构造/只读 replay 后使用该 checkout 自有代码验证 PASS（87事件），不改旧策略或伪装兼容。主盘点3.628秒，历史补验0.049秒，无异常慢。

现存16根中两处仍有ACTIVE：本任务v9 lease-50253ba863ba783f813f；OPS077-v2 lease-877daeaa98540dfe8e17。后者最后可见原事件0006 phase=FORMAL_VALIDATION_PRE，candidate edd372797b1c270da0125453218931e9c400cec3，terminal=false。不按expires_at释放，不以租约表代替OS进程存活调查。本任务v9需源码保全并按原事务终结后才可切换；OPS077需原事务具证收尾。HKLM host注册当前仍缺失。

盘点仅覆盖明确D:/Work命名范围，不声称全主机入口穷举；两个worktrees容器无一级目录。观察非原子，未改业务root/ACL/HKLM，不创建缺失root。证据 outputs/validation_runtime/devx015-v379-legacy-root-inventory.json 及 devx015-v379-historical-root-replay.json。进一步维护方案须绑定完整entrypoint、root及祖先owner/DACL范围，处理原事务排空和旧安全句柄，重启本身不构成激活接受。OPS080旧根重放PASS且无ACTIVE，不等同S4/S5验收。

### v380 旧根祖先权限边界调查

对已知16旧根及祖先去重只读 Get-Acl，共82目录（补齐历史runtime祖先）：81目录由JACK所有且DACL继承，D盘根由SYSTEM所有。原SDDL完整保留 devx015-v380-root-ancestor-acl.json。未改任何ACL。逐路径审阅草案见任务输出 DEVX015-v380-cutover-scope-review.md，明确不可执行、无新授权。

原方案若只收紧leases叶目录，不能证明祖先路径不可替换；不能把静态ACL加目录分享持有误作永久旧binary封锁。具体实施前须限定祖先owner/删除/改权限边界，保留普通开发需求及可审计回滚；4个缺失旧根还需旧入口重启的拒绝策略，不能把ABSENT当fence或擅自创建authority。维护重启也必须结合可信日志/boot身份和字节/ACL重验，不能单凭重启激活。

这不授权对D:/Work或整个D盘递归修改权限。v9及OPS077-v2原租约具证收尾、完整entrypoint清单、公开恢复和L03/Full/发布及OPS080 S4/S5仍未完成。HEAD3c28347e、v9 TASK_SOURCE_PRE_WRITE。快照检查低于1秒，无异常慢阶段。

### v381 离线最小权限方案与内存权限判定

基于v380快照生成81项before/proposed SDDL：65祖先目录为对象级限制，16旧根拟冻结只读；不改D盘根。祖先owner拟BA，移除普通principal对目录自身DELETE/DELETE_CHILD/WRITE_DAC/WRITE_OWNER，原对子项的继承ACE分离保留；旧根拟SY/BA full、AU read/execute。无任何Set-Acl调用，execute_allowed=false。

当前真实非提升进程令牌（不是AITSWorker）DuplicateToken后，仅在内存中对拟议描述符执行原生AccessCheck，81项PASS：祖先read/add-file/add-directory允许，自身删除/子项删除/改DACL/改owner拒绝；旧根只读。65祖先的inheritable ACE顺序、SID、qualifier、mask、OI/CI/NP对比PASS（IO规范化），不是实际子目录继承实测。所有token/descriptor释放；没有登录/模拟身份执行/主机ACL变化。两校验均小于1秒。

任务outputs保留DEVX015-v381-offline-acl-plan.json、DEVX015-v381-accesscheck.json、DEVX015-v381-child-inheritance-check.json；work保留纯生成脚本。仍需完整叶对象清单及回滚、缺失旧根拒绝契约、旧安全句柄维护边界、原v9/OPS077收尾、具体动作授权和真实验收，不能据此直接激活或请求未准备完整的批量ACL执行。

### v382 完整已知旧根对象清单与旧锁迁移前置

16个已知旧根递归只读枚举6113对象：833目录、5280文件，共30200748字节。保存逐对象原SDDL、owner、protected、设备/文件ID、链接数、mtime/size；未读取业务内容用于此清单。枚举5.77秒，独立lstat身份检查0.387秒；无重解析点、硬链接、物理alias或size漂移，但明确非原子快照，执行前仍需锁内重验。3个对象owner为历史CodexSandboxOffline（主checkout目录式arbiter及owner文件、一个事件文件），不能忽略其回滚owner。

新发现16根仅6个文件式arbiter，10个仍目录式arbiter。10份owner.json为execution_lease_arbiter.v1/RELEASED，原bytes SHA已保留于DEVX015-v382-legacy-arbiter-admission.json；这是互斥诊断状态，不证明业务租约释放或旧进程/句柄quiescence。host_cutover_custody要求原文件式arbiter，因此受控legacy迁移是额外前置，不得通过直接删除/替换目录锁绕过。没有修改旧锁、ACL、租约、账户、HKLM或触发Full/PIT。

任务outputs保留v382-legacy-object-acl、lock-shape-summary、legacy-arbiter-admission三份JSON，代码与主机执行不变。下一统一维护依赖：源码保全/原事务收尾、完整入口冻结、精确权限及恢复、旧句柄维护边界、原legacy迁移、host DRAINING/切换/有限恢复、正式L03和全部后续验收。目标保持IN_PROGRESS，v9 TASK_SOURCE_PRE_WRITE。

### v383 原临界区内的持久精确日志生成

新增内部 _prepare_cutover_journal(HostCutoverCustody)：原锁与binding持续校验，从现场获取原state和注册REG_SZ精确UTF8 bytes，完整schema/identity校验，仅派生下一phase及注册state_sha256；原始before bytes不规范化。用与原admin writer一致的编码计算cutover-<sha>.json，原保护写入后通过hold_cutover_journal重新解析核验，返回activation_allowed=false。沿用管理写入1MiB envelope上限，超限在发布前拒绝，不扩张原界限。

v383 9 PASS / 9.59秒，包含两个phase、真实原生文件写入、重复准备保留同一文件ID/bytes、已有相同语义但不同bytes日志拒绝且不覆盖、原state/registry不变、custody关闭后拒绝，以及前journal parser回归。ACL、日志保护持有和注册读取为明确transport替身，不视作管理员安装/完整fence/最终L03。Ruff仅行宽修正后PASS，限定diff PASS，无异常慢阶段。

未写实际HKLM、ACL或运行账户，无重启/Full/PIT。D:/Work/devx015-v383由本任务保留至证据审计后清理。真实旧writer隔离、10旧目录锁受控迁移、受控切换/公开恢复及最终候选required tiers/Full/发布和OPS080 S4/S5仍未完成。HEAD3c28347e，v9 TASK_SOURCE_PRE_WRITE。

### v384 日志到状态/注册的双根中断恢复串联

扩展原journal preparation测试：真实旧/新根及退休marker，原host_cutover_custody准备日志，退出后原host_cutover_recovery_custody读取同一日志并重新锁定双根，再串联内部state/registration发布。两个phase覆盖日志后、state后、registration后三处RuntimeError中断；不一致state/registry时原普通binding以HOST_REGISTRATION_CHANGED拒绝。异常退出后custody失效，独立子进程可取得两锁；新恢复读取准确有限位置，注册仅SetValue一次，journal bytes及文件ID不变，最终原binding读回目标phase。

v384.xml 十项PASS17.40秒（原none/corrupt四项加六断点），Ruff及限定diff PASS，无异常慢。原生文件/目录pin/arbiter真实，ACL保护和winreg为明确替身；是同进程异常注入，不是强杀进程、真实HKLM或完整L03 migration_crash，不增加93/106映射。无主机ACL/账户/重启/Full/PIT操作。

D:/Work/devx015-v384保留至证据审计后清理。真实旧writer隔离、10旧目录锁受控迁移、公开切换/恢复入口、真实崩溃和最终候选全部验证/发布及OPS080 S4/S5仍待完成。HEAD3c28347e、v9 TASK_SOURCE_PRE_WRITE不变。

### v385 本轮源码保存边界

v371-v384已完成有限位置判定、原始注册文本读取、受保护journal解析/生成、原双根恢复custody、原生条件状态文件发布、精确注册更新及内部串联异常恢复；实际验收边界以各原XML和明确transport seam为准。现按既有source-save流程，在v9原事务内重建四类generated authority并保存task branch源码，未授权/未声明最终C、main推进、Full、发布或主机权限变更。

v378-v382调查仍约束后续真实主机阶段：旧WRITE_DAC句柄、祖先路径保护、10目录式旧锁和两ACTIVE业务租约均不能被局部PASS忽略。81项离线ACL AccessCheck、65继承对比和6113对象权限清单只是准备证据。公开激活/恢复、真实隔离/崩溃、I05/L03/X05及DEVX015最终验收和OPS080 S4/S5保持未完成。提交结果与耗时写入task outputs/DEVX015-v385-source-commit.json（如完成），不为记录提交SHA再制造已提交候选外的跟随改动。
