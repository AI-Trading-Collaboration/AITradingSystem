# DEVX-014：主线漂移后的仅源码保全与受控恢复 V1

## 身份、授权与问题

- task：`DEVX-014_DIRTY_SOURCE_PRESERVATION_RECOVERY_V1`；P0；`IN_PROGRESS`。
- next owner：Codex engineering coordinator；owner：Project Owner。
- Owner 在“主线漂移后的受控恢复流程，仅限工程治理，然后继续 S2b”的明确询问后答复“继续推进吧”；本轮授权仅为该工程治理能力及原 S2b 工程续作，不扩大任何研究或外部执行额度。
- mode：`SINGLE_LANE`，最小串行 workflow-contract wave；冻结 main：`4150a595ad2b9eb2df11ee552ead959c077aa417`。
- 原 S2b checkout 在 task-source 阶段 main 前进，旧 publication transaction 已 FAILED/RELEASED；18 项已归属 unstaged 修改不能进入要求 clean Git identity 的正常 drift planner。现有门禁行为正确，不应放宽 stale-main、dirty、terminal 或 candidate 验证规则。

## 2026-09-06 Owner 批准的 S1a 内核互斥修正

Owner 对“先修复现有租约内核的互斥获取与释放，通过验证后继续 S2b；仅工程治理，研究、数据及交易动作全部为 0”明确答复“批准”。`authorization_state=EXACT_PREAUTHORIZED`；此事件解除 S1a 的 owner scope blocker，不表示技术验证通过。旧 BLOCKED 事件、失败事务和诊断保持不变。

继续同一 `SINGLE_LANE`、branch、worktree 与 frozen base `4150a595ad2b9eb2df11ee552ead959c077aa417`，不新建替代 worktree。新的 source transaction 为 `devx-014-source-preservation-arbiter-source-20260906-v3`，SHA `72014ff67f1771191ee72717b756f5034ea8b1d6467d537a0571a1045f7cc57e`、lease `lease-02f96616fdb27eddeef1`，四项 source generator 顺序保持不变。首次 acquire 在写入前因显式参数重复包含两项自动资源被拒绝；确认事务目录不存在后仅删除重复参数，原门禁通过，无重复事务或执行。

S1a 最小串行合同：在现有 `FileExecutionLeaseStore` 内，将同一个 `arbiter.lock` 路径从可被搬走的目录协议改为 OS-backed 稳定普通文件。该文件是同一唯一 arbiter，不增加第二个 lease authority、scheduler、queue 或可绕行实现。持有者全程保留 OS handle；正常运行不得 rename/unlink 锁文件；墙钟过期不能抢占仍存活的 OS 锁；错误持有者不能释放他人 handle。Lease/event schema、资源冲突、TTL 与审计历史不改变，arbiter TTL 仅保留诊断含义。

旧目录不得静默转换。正常 acquire 遇 legacy directory 输出 migration-required；只通过显式、精确 root 的 quiescent migration 转换，要求旧执行者/heartbeat 已停并由 coordinator 核对，保留旧 owner 原始字节与迁移收据。新稳定文件使旧 reader 的 `arbiter.lock/owner.json` 明确拒绝，不能把 mixed-version 执行视为支持合同。迁移不删除 lease event、旧 transaction 或源工作区。

新增准确路径及分工：Hume 仅 `src/ai_trading_system/platform/architecture/parallel_control_kernel.py` 与 `src/ai_trading_system/platform/architecture/lease_arbiter.py`；Curie 仅 `tests/test_arch_005_s2_kernel.py` 与 `tests/test_arch_005_lease_arbiter.py`；coordinator 负责 `scripts/architecture_arch005_lease_arbiter.py`、源码保全实际代码身份绑定、所有既有 shared/docs/registry/generated/validation/publication 路径。worker 在新 preflight PASS 前不写代码，工程回归只使用 synthetic 临时 store，不触及业务数据。

验收依次为：确定性 ABA/活体过期不可抢锁、跨进程唯一进入、错误 owner 释放拒绝、crash 后 OS 释放、持久/临时元数据异常 fail closed、明确 legacy 迁移与旧 reader 拒绝；再执行旧 kernel/checkout/fence 与源码保全完整回归、真实 committed implementation E2E、最终五项生成链及正式 Full。只在这些门禁通过后发布、真实保全旧 S2b 并继续其原受控 integration。没有结果前不预告 PASS。

当前 managed sandbox 不允许直接写 sibling worktree，且既有 Python launcher 的沙箱启动失败；通过原审批机制对精确项目命令申请执行权限，已完成只读 audit/replay 和新事务建立。不得改用别的解释器或工作区以绕过权限，后续写入/测试继续按该权限边界执行。

S1a 实施门禁已通过：canonical 追加 Owner 批准事件、cycle 459，`SINGLE_LANE --contract-change --stage LANE` 绑定 v3 精确声明与自身 active lease，status PASS、blockers/serial requirements/warnings 均空。新增私有 helper 为第 28 项 source-preservation 关键实现依赖；原 27 项记录为历史状态，不改写旧收据。

Bootstrap 因原 checkpoint 会调用 heartbeat，不能先用新 kernel 对 legacy directory 通过 CANDIDATE_COMMIT_PRE。明确顺序为合成 focused 与独立代码复核 PASS → 冻结迁移 CLI/helper/kernel/source-preservation 四项 working SHA → `fence.validate` 只读验证当前活 lease/身份/main → 人工协调旧调用排空 → 本工作区唯一显式迁移 → 原 generator/heartbeat/commit/Full 流程。工程迁移收据必须标 `REVIEWED_WORKING_SOURCE_ENGINEERING_ONLY`，不得冒称 committed 或 published。

迁移 CLI 不接受任意 store-root：本 root 从现有 guard 派生；可选 `--source-request` 只在 `CLEANUP_PRE` 且 candidate=HEAD=main=origin/main、四项迁移实现与 source-preservation 有限代码闭包均为已发布 exact code 时接受。它调用同一 `inspect_migration_source` 只读预检，验证旧 terminal、source Git/精确 dirty bytes、同 common Git 与 source 活动 lease 为空；不创建 snapshot/ref/新 lease。quiescence receipt 必须绑定当前 coordinator transaction ID/SHA、目标 root、owner SHA 和 S1a Owner reference；它是明确人工排空的证据关联，不是可自签的执行许可。normal hold 不自动迁移，半迁移保留并 fail closed。

平台边界为 cooperative 同机进程及稳定本地文件系统，不声称未经验证的 NFS/SMB、恶意 unlink、旧版本并行或持锁时 fork 受支持。FD 不继承，正常锁文件及 root 不由清理器删除；Windows byte-range 与 POSIX flock 的实际验证状态分别披露。原 logical lease 的 integration-coordinator release 特例保留。技术依据：[Python msvcrt](https://docs.python.org/3/library/msvcrt.html)、[Python fcntl](https://docs.python.org/3/library/fcntl.html)。当前没有任何真实迁移或新 Full 结果。

## 固定范围与不可授予的权利

### S1b：实际 Git worktreeConfig 环境的显式安全 admission

首次真实迁移前只读预检未到达迁移：继承环境含空 `GIT_PAGER`；仅对本命令进程使用 PowerShell Env provider 清除继承的 GIT_* 并设置既定 fresh-child 允许集合后，环境项检查通过。此前 .NET SetEnvironmentVariable(null) 留下空项，拒绝证据不抹去。随后实际 common config 的 `extensions.worktreeConfig=true` 被原 V1 明确拒绝；本任务 config.worktree 缺失，不据此推断其它工作区配置相同。没有修改任何用户 Git 配置、owner、lease 事件或锁格式。此时 source migration/preservation 均为 0。

独立审查确认原拒绝正确，并批准在现有 DEVX-014 工程范围内补齐显式 common/worktree 配置验证，不新增 Owner 权限。此为新功能发布前的正式支持合同修正，不是关闭扩展或特例绕过。`_environment` 对 trusted/source 两个实际 checkout 分别读取 `git config --no-includes --null --list` 全部原始条目，禁 global/system 且保持固定 command flags；不能压成 last-value dict，否则安全覆盖值会掩盖文件中的危险值。worktreeConfig 使用 Git Boolean 语义，缺省/false/true 区分，非法值拒绝；includes、filter、fsmonitor、indirect executable、partial/promisor、replace refs 与环境重定向门禁不减弱。

每次执行捕获 common/config 与每个实际 git-dir/config.worktree 的精确 locator、存在/缺失、普通无 symlink/reparse 文件的 bytes SHA，以及全部受控配置条目摘要；不保留或打印 credential/URL 等配置原值。文件读取前检查路径，捕获前后与 legacy guard/首次 mutation 前复核；缺失→出现、扩展开关、字节或 locator 漂移均拒绝。该配置快照只绑定本次执行，独立保全 receipt 验证不得要求未来用户配置永远不变。原三份 source policy 必须先与 source HEAD 核对，普通 guard/fence 的公共合同、路径/phase/lease/exclusions 不改变。

失败清理也不能绕开环境门禁：若 lease acquire 后配置漂移，先重新检查再决定是否可调用旧 guard.release；不安全时保留原 typed cause 与 ACTIVE lease journal，交由明确的后续恢复处置，不自动恢复配置或强制释放。有自身 run 所有权时只记录实际 release disposition；未取得 run 所有权时不得新建 failure 目录或污染赢家证据。该 fail-closed 情形必须有专门合成负例。本任务首次真实 bootstrap 前观测 D: 为 Fixed/NTFS，尚无真实配置漂移或遗留新 source lease。

S1b 冻结后独立复核发现一处必须修正的解析缺口：无等号的 `core.fsmonitor` 是 Git 隐式 true，不能与显式空值 `core.fsmonitor =` 混为一谈。逐条解析必须保留 value separator；fsmonitor/sshCommand/gitProxy 的无值项均拒绝，原显式 false/0/空值支持不扩大。追加 trusted/source × common/worktree 的无值负例与显式空值正例。当前 110-case 回归在原冻结代码上执行，实施此修正前等其终态并保留原结果；静态审查发现不能被无相应负例的 PASS 掩盖。当前真实配置只读 admission/recheck 与 v3 fence validate 已通过，尚无迁移。

迁移 CLI 的 helper 后配置复核属于部分执行边界：若此时外层返回 BLOCKED，不能把已经发生的迁移记为 0。必须依据 helper 的 durable receipt、archive、failure journal 记录实际动作并停止，不自动重复 dispatch；外层 status 与实际执行计数分别披露。

include poison 的根因已通过安装版本 `git 2.45.1.windows.1` 与官方同版代码定位：config 列表在真正受 `--no-includes` 控制的读取前调用 `setup_auto_pager`，额外的 pager configuration lookup 会读取 effective includes。Hume 只读复用两份失败合成夹具，先做四次 explicit-file/cwd 对照，再做两次原 full 命令仅增加全局 `--no-pager` 的关键对照；后两次均 exit 0、15 entries、保留 include.path 原条目、stderr 为空，输出 SHA 分别为 `9d7a966b038778b8a65e378516c37fc5c69c660da84ba03e626f168a46877e80`、`fdba01278bebca3d821571967c78ac11e0cd4516caf34133df8099c18280fcb3`。未修改夹具或真实配置，未执行 guard、pytest 或迁移。最小正式修正为 `_git` 固定全局 `--no-pager`，同时保留 separator 拒绝无值执行性配置；完整 effective/native Boolean、物理配置快照与 FILTER 拒绝合同不变，不新增 file parser、预扫描、clone 或环境逃生接口。依据：[Git v2.45.1 config](https://raw.githubusercontent.com/git/git/v2.45.1/builtin/config.c)、[pager 选择与 no-pager](https://raw.githubusercontent.com/git/git/v2.45.1/git.c)。先追加 task event 与 preflight，再实施及 focused 验证。

追加合成测试：absent/false/true+缺失 worktree config、真实 distinct linked-worktree 正例；危险键仅在 trusted 或 source、危险 common 值被安全覆盖仍拒绝；include poison、非法 Boolean、配置漂移/缺失变存在/reparse 在 guard 或 mutation 前拒绝；源码/index/refs/config 不变。实现仍限已声明 source-preservation/迁移 CLI/原测试路径，更新同一 DEVX-014 29-source successor 的说明，不另建 phase。重新冻结四源码 SHA、执行 focused 和独立审查后才真实 bootstrap。原 reviewed-code v1 manifest 作为未进入迁移的预检身份保留，不能改写为新代码。依据：[Git config](https://git-scm.com/docs/git-config#FILES)、[Git worktree configuration](https://git-scm.com/docs/git-worktree#_configuration_file)。

### S1a 发布前兼容性与工作区拓扑复核

独立审查确认，原 S5 fragment 固定了旧 kernel/test 字节；只运行通用 builder 不能表达本次合法变化。追加唯一 successor `phase_devx_014_dirty_source_preservation_and_os_lease_arbiter_v1`，schema `devx_014_dirty_source_preservation_and_os_lease_arbiter.v1`，继承 `phase_ops_079_historical_daily_gap_recovery_executor_v1`。不修改 OPS-079 路径集合，不改 C/D/S5 冻结链。新增 section 由本 requirement 与现有 policy 定义，仅记录本任务保全/OS-lock/显式迁移工程合同，不绑定 runtime 收据、当前时间或未来 commit。

固定 29 项当前源码闭包包括：本任务八项 owned paths、两项 CLI、compatibility builder、ARCH-004 refactor/DEVX-006C/DEVX-006D/S5 四项 ratchet tests、system flow 及 RCF policy、DEVX-014 canonical fragment、task index/两 compatibility views/S5 consumer inventory、五项 architecture 输出、RCF index/consumer inventory。source_paths 与 superseded_live_source_paths 为同一排序集合；compatibility 自身 index/fragment 不自引用，未修改 Atlas 配置/tests、研究输入与 runtime receipts 不入集合。源阶段四项、最终阶段五项生成顺序不变，使用同一个新 section；相关实现/测试/生成路径均已在 v3 声明。

按真实 `git worktree list --porcelain` 核对：物理主目录 `D:\Work\AITradingSystem` 当前仍在旧 `codex/trading-2559-temporal-influence-falsification@0a0e6abef3eaebc760b200997160f5b64281a55f`，并非检出 main；没有工作区占用 main。本任务将按既有协议在当前 coordinator root 检出 main、仅 fast-forward 已验证 candidate，再执行普通 push。因此当前 root 的显式 bootstrap 已覆盖本次 main 发布所用 store，不因物理主目录仍存在 legacy directory 而迁移该旧 checkout。

发布后 source migration 的 branch identity 必须按阶段验证：TASK_SOURCE_PRE_WRITE 仍要求 acquire 时的原 branch/HEAD；CLEANUP_PRE 要求真实 current branch 为 main，且完整既有 REMOTE_PUSH_PRE→CLEANUP_PRE 链、candidate=HEAD=main=origin/main，root/common 始终等于原事务。不得简单删除 branch 检查。其它旧 checkout/run-specific store 保持原代码与原证据；升级它们需要独立精确排空/迁移范围，不自动升级、迁移、停止 scheduler 或放宽 source-request。本轮不新增 clean-main 或 arbitrary-store 迁移入口。

新增独立 `source_preservation.v1`，只构造 `RAW_BYTES_SOURCE_ONLY_UNVALIDATED` Git snapshot。复用 S4D `CheckoutLeaseGuard` / `FileExecutionLeaseStore`；不新增锁、scheduler、publication queue，不修改普通 fence/planner/Full runner 的授权逻辑。

真实 manifest replay、canonical DQ、研究/回测、download、cache mutation、provider、QuantConnect、Options、paper/live、broker/order/fill/position 和任何交易动作全部为 0。合成工程测试独立记录，不能充作研究证据。不得复用已消费的 TRADING-2557/2563 授权。

Snapshot 不具备 task-source write、generator、formal validation、Full、main FF、push、研究或交易权限；后续普通候选必须另经原 planner/fence/validation/publication 流程。旧事务、失败 Full、任务历史和源工作区不得改写；不自动 stash/reset/rebase/merge/cherry-pick、覆盖 ref 或清理任何旧工作区。

## V1 接口与严格支持合同

独立 CLI：`scripts/architecture_arch005_source_preservation.py preserve|validate`。实现模块暴露 `SourcePreservation(project_root, policy_path)`，`preserve(request: Mapping) -> dict`、`validate(receipt_path: Path) -> dict`，错误采用 `SourcePreservationError.code`。`project_root` 是可信已验证实现 root，不是 dirty source root。

Request 通过显式 JSON 文件传入，绑定 schema、preservation id、recovery task、owner instruction reference、source root/common Git dir/branch、frozen base、source HEAD、observed latest main、terminal source transaction exact path/SHA、以及逐项 source relative path/SHA/size/mode。具体字段在实现与 policy 中固定并由测试共同验证；CLI 不猜测身份或自动生成权限。

精确 request 字段：`schema_version=source_preservation_request.v1`、`preservation_id`、`recovery_task_id`、`source_task_id`、`owner_instruction_ref`、`actor`、`thread_id`、`source_root`、`source_common_git_dir`、`source_branch`、`frozen_base_sha`、`source_head_sha`、`observed_main_sha`、`observed_origin_main_sha`（可为 null）、`terminal_transaction={path,sha256,closeout_sha256}`、`files=[{path,sha256,size_bytes,git_mode}]`。files 排序、不重复且恰为 dirty 清单。Owner reference 固定 `owner_instruction:DEVX-014:2026-09-06:source-only-recovery`。V1 policy 资源上限 64 files / 16 MiB 用于限制一次工程保全，覆盖当前 18 项；并非投资阈值，超限明确拒绝而非自动拆分，扩容须独立复核。

实际 preserve 必须验证执行 module/CLI/policy 与 trusted implementation root 的 HEAD 字节一致。开发 focused tests 可在明确合成夹具中隔离 implementation-binding 边界，但 source Git/transaction/lease/snapshot 检验本身保持真实；source commit 后必须以真实 implementation binding 重跑端到端正例，不能把合成绑定当作生产身份。首次旧 S2b preservation 只使用最终发布实现。

既有 guard 没有 subprocess environment 注入接口，本 V1 不扩大该公共合同、也不改进程全局环境。fresh child 的 Git 环境必须在首次 helper 调用前严格检查：允许 `GIT_CONFIG_COUNT` 仅包含 key=`safe.directory` 且值为已核验 trusted/source exact root 的条目，以及固定 `GIT_OPTIONAL_LOCKS=0`（禁止 Git status 刷新真实 index）；其余 redirect/config/replace 风险环境拒绝。新的 Git 子进程使用剥离 ambient 的环境、精确 safe.directory，并禁用 hook/signing/lazy-fetch。该显式运行前提属于 V1 支持合同，不是临时绕过；将来若要可注入 guard 环境须独立合同评审。

父 helper 与新子进程还必须保持同样的配置源：固定 `GIT_CONFIG_NOSYSTEM=1`、`GIT_CONFIG_GLOBAL=os.devnull`，防止新子进程已隔离 global config 而旧 guard 仍加载其它 fsmonitor/filter。CLI 明确把自身 src 放在导入首位，实施身份检查仍须验证实际 loaded dependency roots；`--request` 文件只接受 trusted checkout 的 `outputs/validation_runtime` 下普通、无 reparse 的 JSON，校验路径后才读 bytes，避免用任意 request/policy/terminal locator 触碰 excluded 文件。Public Mapping API 不读取额外 request 文件。

实施身份的实际 profile 明确为 `COMMITTED_SOURCE_GIT_EOL_LF`：当前 28 个有限关键项目模块（初版 27 项，S1a 增加 lease_arbiter；包括 guard/fence/lease/canonical/JSON/YAML 和必要 helper/package initializer）核对 physical origin，记录工作文件 raw SHA 与 Git blob 内容 SHA；仅 Git 的源码 CRLF/LF 差异按该 profile 归一比对。它不枚举 Full 收集时的无关 imports，也不授予其它代码身份。此规则绝不应用于保全的 source bytes：snapshot 始终为 `RAW_BYTES_SOURCE_ONLY_UNVALIDATED`。固定三份旧治理 policy 必须在构造 guard 前与 source HEAD 核对；V1 拒绝其漂移、配置 include 和 replace refs，不静默加载新 authority。worktree 配置按 S1b 的独立作用域检查和执行期身份合同处理，不关闭用户设置。

1. 对原 transaction 完整 hash/event/closeout chain 做原 fence replay，要求 FAILED 且原 lease RELEASED；核对 task、actor、checkout/branch/head/base、common directory 和已声明路径归属。新 request 的 source task 必须等于原事务 task；recovery task 独立记录。
2. 冻结与重新检查 source HEAD、branch、real index SHA、main、origin（存在则绑定）、完整非 excluded dirty 清单及文件 bytes。frozen base 必须是 source HEAD 和 latest main 共同祖先。
3. V1 仅允许已跟踪、未暂存、普通文件的内容修改，Git mode 100644/100755。拒绝 staged、untracked/add/delete/rename/type-change、symlink/reparse/gitlink、未声明/逃逸路径及 unsupported filter transform。明确采用 raw bytes profile，CRLF 原样保全，不伪称普通 Git EOL 归一化。
4. 宽范围 Git inventory/diff 必须携带 policy 全部 exact literal exclusions。禁止打开、hash、copy 或 stage excluded 文件。不得扫描所有 tree blob；只继承旧 tree metadata 并更新 allowlist。
5. 获取现有 S4D shared-mutation lease，以 source HEAD 作为 exact base；在 lease 内再次检查输入。使用 raw `hash-object --stdin --no-filters`、私有 alternate index、`write-tree` 和以 source HEAD 为唯一 parent 的 `commit-tree`。真实 index、branch/HEAD、工作文件、main/origin 不变。
6. Create-only CAS ref：`refs/aits/source-preservation/<id>`；原 ref 不存在才创建。不覆盖成功、失败或部分运行的 receipt。幂等重放仅接受完全匹配的既有成功身份；中断/冲突证据保留并 fail closed。
7. 对 canonical task fragment 复用 `validate_canonical_fragment`，并强制旧 HEAD `events` 是捕获片段 `events` 的完整不变前缀。仅自洽 hash 不足以保护历史；不重绑定历史 base_commit。
8. 状态：`ACQUIRED → CAPTURED → OBJECTS_WRITTEN → REF_CREATED → VERIFIED → RELEASED`；失败终态保存 partial evidence。收据绑定执行代码/policy、源证据、lease、raw bytes/blob SHA、parent/tree/ref 与前后不变证明。成功只表示 source-only 可重验。
9. 所有 Git 子进程隔离环境重定向/replace/filter/下载、hook 与 signing；不从 dirty checkout 导入新治理代码。独立 receipt 验证检查 immutable 捕获事实与 snapshot，不要求未来 source 工作文件永远不变。

## 分阶段与验收

|阶段|责任与依赖|验收|
|---|---|---|
|S0|coordinator 登记与租约；当前 exact main|任务、requirement、路径声明、LANE preflight PASS|
|S1|coordinator 冻结 policy/CLI 合同；worker 分离 module 与 tests|raw snapshot、严格负例、append-only 历史、并发与故障证据；无普通 fence 修改|
|S2|coordinator 生成权威、最终候选验证与普通发布|focused、适用 architecture/contract/integration/reproducibility/Full；main=origin=candidate；失败不预报 PASS|
|S3|已发布 exact 实现对旧 S2b 首次保全|独立 receipt 通过；旧 18 项 bytes/branch/index 保留；以 snapshot lane head 在唯一 clean latest-main coordinator 重新执行原 drift plan|
|S4|原 TRADING-2564 任务继续 S2b|明确 domain overlap 协调、append-only task 更新、原 failed Full parent 绑定、最终候选验证与发布；S2b 完成不等于策略有效性已证实|

工程测试至少覆盖：真实 synthetic Git 分叉；wrong identity/ancestry/terminal/lease；staged/untracked/delete/mode/reparse/filter；exclusion poison-read；CRLF raw bytes/index/refs 不变；canonical 历史篡改；pre/post drift；并发 lease；ref 冲突与 partial failure；同 ID 不同内容；source-only 无发布权限。保持现有 stale-main/dirty/terminal fence/planner 负例。

## 所有权与工作区生命周期

- worker Hume 仅实现新 `src/ai_trading_system/platform/architecture/source_preservation.py`；worker Curie 仅实现 `tests/test_arch_005_source_preservation.py`。同一 exact base，不创建各自 branch/worktree；shared policy/CLI/requirement/registry/generated authorities/system_flow、正式验证、commit/publication 由 coordinator 唯一写入。
- 本任务 worktree：`D:\Work\AITradingSystem_devx014_source_preservation`；branch：`codex/devx-014-source-preservation`；common dir：`D:\Work\AITradingSystem\.git`。
- 创建前 lifecycle plan：原 S2b `outputs/validation_runtime/devx-014-source-preservation-workspace-plan-20260906.md`。purpose 是独立治理合同的 clean exact-main 实现与验证，并非为旧 S2b 改换 v2/v3 frozen lane。
- 源 S2b checkout：`D:\Work\AITradingSystem_trading2559_integration`；HEAD `06140c52ca4e5be718075b7f436b820b927637c4`，frozen base `293813e5e2e7b88886b79fc22cf77e2d57f1f346`。18 项 dirty 原样保留，terminal transaction `trading-2564-s2b-derived-fix-source-20260906-v1` immutable。
- cleanup exit：新合同发布且 canonical evidence hash 核对、tracked/untracked/ignored unique content 与进程依赖审计通过后移除本任务 clean worktree 并 prune。若接续作为唯一 S2b coordinator，先在两任务中明确交接、purpose 与最终退出条件；不先删除运行依赖路径。
- 不清理 root 或其它任务工作区；不读取 `docs/research/growth_tilt_owner_diagnosis_pack.md`。对失败 partial objects/ref/receipt 保留可审计证据，不自动删除。

## 状态记录

### 2026-09-06：正式 Full 三项失败后的最小合同测试修正

候选 `1389319ddd67109f4defb11a60521a7bbde7b43e` 的四项前置正式验证均已通过：architecture 1055、contract 281、integration 995、reproducibility 24。唯一 Full `full_20260906T061949Z` 于 `2026-09-06T07:26:22.889471Z` 结束，实际 `10658 PASS / 3 FAIL / 5 SKIP / 640 warnings`、16 workers/loadfile、pytest 3926.23s；summary SHA `3e04952c55c9b062bd6379d9b2d8cfc8f7630bb478af4c1da3d01b1a5ccf8ac2`，runtime profile SHA `6d797e69fe958c788ecc08af2bf1272da6ca5d697ce822669820334720ceb8c0`。原 summary/log/profile/reader brief 及失败合成 Git 夹具的六个工作/元数据文件、八个精确 reachable loose objects 共1126 bytes已保留；未复制其它 fixture 或 excluded 文档内容。Full 不是 PASS，旧 S2b 迁移和保全仍为0。

两项失败来自 `tests/test_trading2452_architecture_contract.py`：其特殊历史路径分支的显式后继集合遗漏本任务 DEVX-014，导致当前合法最新声明错误落入 `321 <= 302` 的旧阶段上界。仅补精确 phase 常量与既有集合，保留历史 hashes 不重写、supersession authority 和最新 live SHA 比较；不允许任意未来/近似名称阶段，不回退较早合法 SHA。新增真实命中特殊相对路径的正负例：合法 DEVX014、stale SHA、重写标记、错误 authority、缺失 superseded path、未知/近似后继与最新错误绑定。

第三项来自 `tests/test_architecture_wave_readiness.py`：合成 `_init_repo` 未规定本地 EOL，Windows 默认 write_text 产生 CRLF，隔离 system/global 配置后 Git 正确保留 `B\r\n`，但测试固定要求 `B\n`。生产 binary Git helper 行为正确，不做 normalize 或放宽断言；只在 fixture 初次 add/commit 前明确 local `core.autocrlf=true`。保留 carrier 专用 false；补隔离环境下 LF 正例以及显式 false 的 raw CRLF/binary bytes/SHA 正例。

这是原工程修复的最小测试/审计闭包修正，不改变锁实现、source-preservation 实现、生产 Git helper、运行窗口、DQ、研究或交易合同。同一 successor 的精确闭包从32增至34，仅新增上述两份实际修改测试；system_flow 只更新现有段落中的数量，不增新 flow block。预计 module1203/test-file1363、deprecation ID、flow1226、RCF3162/192、merged sections322不变，必须经真实生成核对，不能无依据调整 ratchet。新 source 事务 `devx-014-full-failure-fix-source-20260906-v1` 在相同 root/branch/base 下声明该范围；先 append-only canonical event 和 LANE preflight，再实施。依次完成 focused、source四生成/commit/真实E2E、final五生成/正式适用tiers；新 DEVX014 Full 使用 `failure_fix_rerun` 绑定上述真实失败 summary。S2b 后续 Full 仍绑定自己的旧失败 parent，不继承 DEVX014 结果。

原 final v2 已通过 publication command 于 `2026-09-06T07:30:40.441554Z` FAILED/RELEASED，main/origin未前移。新 source事务 SHA `6f7c03278977e193d0c68ba1796a27cf98726e0fbc3e9898e7ecb9539a49c88e`、lease `lease-d0b9d7bc22c2c32e0620`；失败历史和现有 Owner 精确授权不改写。旧 root 单次迁移仍待全部正式验证及正常发布通过，实际0/1；本 task-root 已迁移1不重复。全部真实 manifest replay、canonical DQ、研究、下载、cache/provider/QuantConnect/Options、paper/live/broker/order/fill/position/交易动作仍为0。

### 2026-09-06：正式 architecture 回归失败后的精确工程修正

源码 commit `550724649e79a22e118f7992461293d9305196b2` 已完成四生成链、54-case authority focused 和真实 committed implementation E2E（1 PASS、无 SKIP）。final v1 五生成链在同一 commit 上保持 tracked 零差异，retained-input missing-only copy 为 1231 files / 3,810,350 bytes、overwrite 0，1242 项 bindings 复核及 readiness 全部 PASS；这些前置结果不等于最终验收通过。

正式 `architecture-fitness_20260906T041925Z` 于 `2026-09-06T04:49:22.392587Z` 结束：16 workers/loadfile、1045 PASS / 4 FAIL / 0 SKIP，pytest 1793.58s。summary SHA `229c00523e6bd9aaeb7bd49efc93de049838f13203cd1bdce66cdef2709e019b`；完整 log、reader brief 和两项合成 Git 失败夹具的 12 个精确文件原始字节均保留，未读取 excluded 文档内容。新增锁协议与完整源码保全文件通过，但不以局部 PASS 覆盖整轮 FAIL。尚未 dispatch Full，Full parent 为 null。

原 final v1 在保留失败和 Owner 新授权收据后，通过原 publication command 行政 FAILED/RELEASED，main/origin 保持 `4150a595ad2b9eb2df11ee552ead959c077aa417`。同一 branch/root 上的新 source-fix transaction 为 `devx-014-architecture-failure-fix-source-20260906-v1`，SHA `1e408817dc47b314f386921e676dcc3ffb6d5860ae9399038c88af8cc708c042`，lease `lease-8dab7da0b7fad39c4d0f`；不新建替代工作区，不重复任何实际迁移，不伪造 failed Full parent。

已核对的最小修正与责任如下：

- coordinator 精确更新 current deprecation ratchet：module 1201→1203、test file 1361→1363，由新增保全/arbiter 两模块及两测试导致；已生成 inventory 仅这两数改变，canonical ID 从 `188e7fa0187b6ad93dc7` 变为 `f14312a2d972f96dbcdb`。历史常量、surface/removal/writer 合同不变。
- coordinator 修正 DEVX-011 的 RCF successor 元数据断言 3158→3162，原因是本任务 system_flow 四个新增分块；1373 report-registry + 563 artifact-catalog + 1226 flow = 3162，192 fragments 不变。原 workflow_health_contract/safety 不变，不用 live 值替代固定 expected，不重写历史 Git fragment。
- 两项 synthetic Git 夹具默认 `write_text` 在 Windows 产生 CRLF，而未声明可移植 EOL 合同；formal fresh-child 禁止 system/global Git 配置时，真实 diff-check 因行尾 CR 拒绝。只在 `tests/test_arch_005_integration_publication_fence.py` 与 `tests/test_arch_005_s4d_checkout_guard.py` 明确测试夹具 EOL，验证正常修改 PASS、真实尾随空白仍 FAIL；生产 checkout/fence 门禁和用户 Git 配置不改。coordinator 负责登记与审查后才分派这两个 test 文件。

上述三份本轮实际改动测试（另含 `tests/test_arch_004g_deprecation.py`）进入同一 DEVX-014 successor，精确源码闭包 29→32；builder、exact-set tests、说明和现有 flow 分块同步，不新增 phase 或 blank-line block。先 canonical 追加本事实及 preflight PASS，才实施；随后并行 focused、source 四生成链/commit/真实 E2E，再在同一新 commit 上 final 五生成链/正式适用 tiers/Full。若有新失败，保留并诊断，不缩小正式范围换取 PASS。

### 2026-09-06：Owner 明确批准旧 S2b 单次协议迁移（待发布前提）

Owner 对精确问题“当前修复验证并发布通过后，对 `D:\Work\AITradingSystem_trading2559_integration` 执行一次锁协议迁移，以继续原先批准的源码保全；旧锁记录和 18 项源码修改原样保留；研究、DQ、下载、缓存修改及交易动作全部为 0”答复“批准该工作区单次迁移”。`authorization_state=EXACT_PREAUTHORIZED`；receipt `outputs/validation_runtime/devx014-s2b-source-migration-owner-approval-20260906-v1.json` SHA `70018582aad1a1ef658d549838e11fc3db49cbe9323fa01d33742077ad294b10`。该批准不是技术 PASS。

执行前提仍为本修复通过全部正式门禁并正常发布、CLEANUP_PRE、published exact code、旧 source request/terminal/18 项 bytes 的 fresh admission 及新的进程排空证据。旧 root migration 最大 1、实际 0；本 task-root 已迁移累计 1，不再 dispatch。所有真实研究、manifest replay、canonical DQ、下载、cache/provider/QuantConnect/Options 及交易动作仍为 0。旧 S2b 工作区及 18 项修改不清理、不覆盖。

### 2026-09-06：本工作区唯一 arbiter 迁移完成，进入工程生成与验证

在新 preflight、四源码 SHA、配置、11 条原业务事件与进程排空复核后，唯一实际迁移于 `2026-09-06T03:57:43.182207+00:00` 完成，CLI exit 0 / PASS。migration id 为 `devx-014-task-root-os-arbiter-20260906-v1`，helper receipt SHA 为 `a6e5fa7cff71f07470ed2ad8069ac048b69a6fe3c4f45503f72e9b2006f70c48`；新 quiescence v2 原始 SHA `a0146d4671115f01a0e8c9a056e06f142e17692f57397f6dfcedda612dc602ed`。实际迁移累计 1，旧 auto-review 拒绝不计为实际执行。authorization 为 EXACT_PREAUTHORIZED，implementation profile 仍为 REVIEWED_WORKING_SOURCE_ENGINEERING_ONLY，不是已提交或已发布实现。

首次 heartbeat/checkpoint 前的只读 postcheck `2026-09-06T03:58:21.544659+00:00` 通过 strict completed-migration validation、v3 fence 与业务事件 replay。稳定 `arbiter.lock` 已为普通文件；新诊断 owner 为 RELEASED；archive 精确包含 request/quiescence/receipt 与原 `legacy/owner.json`，无 failure。旧 250-byte owner SHA `1481ce996900f09ae072e4c1d7c2075502a3d7c40baa7453277b7c1d9578c5f8` 原样保留；11 条事件 path/size/SHA、三个 head event ID 与唯一 active lease 均不变，HEAD/main/origin 和 Git 配置快照不变。完整证据见 `outputs/validation_runtime/devx014-task-root-migration-postcheck-20260906-v1.json`；未迁移其它 root、未运行 source preservation 或研究/DQ/数据/交易。

源码阶段将按 canonical-task-source → architecture-manifests → report-flow-authority → compatibility-authority 生成，并通过 focused compatibility/RCF 后提交。committed implementation E2E 必须在该真实源码 commit 上 PASS 且不能 SKIP；随后 source v3 仅通过 publication release 行政 FAILED/RELEASED，绑定 source handoff 和实际 source commit transition，candidate=null、Full=0，不伪装 failed Full。新 final transaction 在同 root 声明 retained-input exact destinations 后才 missing-only copy，使用 canonical → architecture → Atlas exact source HEAD → report-flow → compatibility 五生成链；在最终 candidate 上运行正式适用 tiers 与一次 Full，再按正常门禁发布。此段是执行计划，不是尚未产生的验证或发布结果。为维持 Atlas exact-commit covered-source identity，源码 commit 后不再改变本阶段 tracked requirement/canonical 状态；后续终态事实先写 runtime receipts，再由后继受控阶段同步任务。

### 2026-09-06：Owner 对精确本工作区迁移的继续授权

在明确询问“是否批准在 `D:\Work\AITradingSystem_devx014_source_preservation` 对现有 arbiter 执行一次锁协议迁移，保留旧记录且不改变业务租约事件，研究、数据和交易动作全部为 0”后，Owner 答复“继续吧”。该答复绑定本次单一目标与效果，`authorization_state=EXACT_PREAUTHORIZED`；任务从 BLOCKED_OWNER_INPUT 恢复 IN_PROGRESS，next owner 为 Codex engineering coordinator。此前进程创建前的拒绝及零执行收据保持不变；不是将权限设置本身当作业务授权。

续接只读 fence validate 与 SINGLE_LANE/contract-change LANE preflight 已 PASS：原 v3、唯一 active lease、原 branch/HEAD/main/origin 与 21 项任务归属均一致。实际迁移仍须新的代码/配置/旧 owner/11 条业务事件与进程排空复核，并使用新的 quiescence v2；不得复用过期静态观测。仅迁移本任务 root，不迁移物理旧主目录或其它 store；真实迁移一旦开始或留下部分收据，不自动再次 dispatch。成功后才能继续原工程生成、源码提交与正式验证流程；未执行阶段不得标记完成。

### 2026-09-06：实际迁移在进程创建前被权限审查拒绝

最小工程修正、独立静态审查与 64-case focused 已通过，但实际迁移命令的 `exec_command` 在 CreateProcess 前被 auto-review 拒绝：未识别到对 DEVX-014 精确迁移目标和效果的明确用户授权。没有改用间接入口或原 helper 绕过；迁移入口调用 0、实际迁移 0、业务 lease mutation 0。只读复核 `2026-09-06T02:54:31.219049+00:00` 确认旧 owner SHA 与全部 11 条 lease event 的 path/size/SHA 均不变，helper archive 和 CLI execution directory 均不存在；原 v3 仍在 TASK_SOURCE_PRE_WRITE，main/HEAD/origin 未前移。拒绝证据 `outputs/validation_runtime/devx014-migration-dispatch-permission-denial-20260906-v1.json` 保留，不把拒绝当作已执行的 migration failure 或未经授权运行事件。

已批准工程修正的 EXACT_PREAUTHORIZED 记录不改写；下一责任方为 Project Owner，需明确批准仅对 `D:/Work/AITradingSystem_devx014_source_preservation/outputs/architecture/arch_005_s4d_checkout_guard/leases` 切换旧 arbiter 运行时协议并保留旧记录，不改变业务事件。等待期间所有代码、旧 S2b 18 项修改、runtime 证据及本 worktree 均保留，不清理。原 lease `lease-02f96616fdb27eddeef1` 当时仍 ACTIVE、expires_at=`2026-09-06T06:44:10.097377+00:00`；未通过旧内核替代入口 heartbeat/release。后续必须重验 active lease、phase、main/code 和新的真实进程排空证据；过期时 fail closed，不复用已失效 authority 或旧 quiescence。生成、source commit、formal tiers/Full、保全及发布均未执行，研究/DQ/数据/交易仍为 0。

### 正式工程验证的 retained-input 准备（尚未复制）

- S1b 最小 causal fix 已冻结：module SHA `f9845936b4febb57686ec537c0139d359c4acb663809a1890d99dc22ec4c168a`，tests SHA `be410b7ead7f4306e0578a07444bc844a70f50a625b895d7a1eba075e6283436`，CLI 仍为 `7692c1891f8149ae6f29bdf8438c1286f332b8115a69b9cb5d6c444c6661e236`。Black/Ruff/strict mypy 与独立最终静态审查 PASS；并行 focused selector `config or fsmonitor or migration or concurrent_same_id or raw_snapshot or linked_worktree` 实际选中 `64 PASS / 298.94s`，16 workers/loadfile，无 FAIL/SKIP，XML `outputs/validation_runtime/devx014-s1b-causal-fix-focused-20260906-v2.xml` SHA `143b325c89dd004b8b09de86d2261aade841e0f2e1d6aee67019589132134169`。覆盖双 root/双 scope include poison、15 个无值/显式值用例、配置漂移/unsafe failure cleanup、迁移 phase/identity 与真实 raw/linked/concurrent；正式 Full 仍需覆盖完整 127-case 文件和 source-commit 后真实 implementation E2E。原失败结果不覆盖。此时 HEAD/main/origin 仍为 `4150a595ad2b9eb2df11ee552ead959c077aa417`，真实迁移尚待新四 SHA 冻结及 coordinator quiescence，尚无保全、Full、研究、DQ、数据或交易动作。

- S1b 首轮完整回归实际为 `107 PASS / 2 FAIL / 1 SKIP / 1218.02s`，16 workers/loadfile；XML `outputs/validation_runtime/devx014-s1b-source-preservation-focused-20260906-v1.xml` SHA `68cf9d4e804c59d7242b4a7950b4d239b422947b3fc674439e87c33b1b7ce462`。执行 module SHA `2b750e688fa7ef9ae886c7754160fb3612cd0072249b2cb006b2df7eb8cb3316`、tests SHA `148d276c9ff3f176acad8db7f5b7c7cbc22e9f427f2d9900f5ceb52e70c18ea4`、migration CLI SHA `7692c1891f8149ae6f29bdf8438c1286f332b8115a69b9cb5d6c444c6661e236`。两项失败均为 trusted/source 的 malformed include poison：预期在读取 include 前 FILTER 拒绝，实际第一条配置命令返回 ENVIRONMENT，必须诊断 Git 启动期配置解析并修正，不能放宽断言。唯一 SKIP 仍为 source-commit 前真实 implementation E2E。该轮运行期间只追加 coordinator requirement/canonical 记录，未改三项执行源码/测试。独立发现的无值 fsmonitor 缺口亦待修正；此轮不是安全验收 PASS，真实迁移/保全/研究/DQ/数据/交易仍为 0。

- S1a 第一轮完整合成回归为 `157 PASS / 1 SKIP / 663.71s`，16 workers/loadfile；XML `outputs/validation_runtime/devx014-os-arbiter-focused-20260906-v1.xml` SHA `c52c009d022f52a18234483f55d96b09dc2c485dda827d0174b3e34a5a3eee24`。其中新 kernel/arbiter 42 项、原 guard/fence/planner 48 项全部通过；唯一 SKIP 是 source commit 前的真实 implementation E2E。Windows OS 锁的跨进程测试实际执行，不声称已在 POSIX 执行。源码保全并发案例本轮走正常 BLOCKED decision，未走 typed exception；因此保留其旧诊断，同时将过时 CAS-only 断言精确更新为新协议 `LEASE_ARBITER_BUSY`，没有扩大异常允许集合。后续 phase-aware CLI/concurrency focused 为 `12 PASS / 27.23s`，16 workers/loadfile；CLI 首次 strict mypy 因 phase 类型为 object 在 pytest 前拒绝，显式字符串边界修正后 Black/Ruff/mypy PASS 再执行。
- 审计纠正：`devx-014-compatibility-topology-review-20260906-v1` canonical event 的人工输入 occurred_at=`2026-09-06T01:30:00Z` 比随后实际时钟观测 `01:27:31Z` 提前；这是 coordinator 时间输入错误，不是事件当时已在 01:30 执行。保留原事件并追加纠正说明；后续 writer 使用命令执行时 UTC 自动取值，不重写原时间或历史哈希。此问题不改变任务内容、授权、验证结果或运行计数。

新 worktree 不继承旧 S2b 的 ignored evidence。仅按已提交的四份 result-admission、O1 gate、signal policy 和已绑定 package receipt 的有限字段展开，独立复核 1242 个唯一文件：11 个目的已存在且 SHA 一致，1231 个缺失，总计 3,810,350 bytes；旧 S2b 对应源全部 regular、SHA/显式 size 一致。包含 1202 条 receipt 明确列出的 daily artifacts，不按日期生成清单，不递归扫描 outputs。

精确 copy plan：`outputs/validation_runtime/devx014-retained-evidence-plan-20260906-v1.json`，file SHA `020aa26672a385ea4b109de61360d91056f0b785596dd375b1255c92dd6d02e2`，plan SHA `898b34e88f37d4c6ee893759d0470e1a95a6e0910bbd894da65edabec6ac3699`。源固定 `D:\Work\AITradingSystem_trading2559_integration`，目的本 worktree 同 relative path。生成器为本任务 runtime `devx014_retained_evidence_plan.py`，只读 committed bindings 并生成计划；初始两个路径域遗漏（O1 validation_runtime、signal research_trends/qqq_options）均在写计划/复制前显式拒绝，补为实际已审核的域后成功，没有扩大到其它来源或执行重放。

复制必须等 final transaction 提前声明这些目的路径后才做，missing-only、create-new、复制前后 SHA 验证，已存在文件不得覆盖。复制属于工程测试依赖准备，不是新研究、DQ、数据下载、cache mutation 或 Options 回测。原件保留；temporary worktree 清理前确认 required evidence 在 canonical retained location 仍完整。本有限 readiness 清单不是 Full PASS 保证。

- 2026-09-06：Owner 批准；独立设计审查一致选择 source-only snapshot；当前 main `4150a595` clean audit PASS，active source publication transaction `devx-014-source-preservation-source-20260906-v1` 已到 TASK_SOURCE_PRE_WRITE。尚未实际 preservation、研究运行或 Full。
- 2026-09-06：S0 task writer 与 LANE preflight PASS。首次 LANE 仅因未显式带自身已核验 active lease 被拒绝，补上精确 lease id 后通过；没有跳过审计。独立 closeout review 发现 source v1 提前声明 final 五项（含 Atlas），故在任何 generator/Full 前行政终止并改用同 checkout/base 的 source v2 四项生成链。原事务及原因保留于 runtime scope-correction receipt；不改写历史，不执行重复生成或验证。final transaction 仍须五项。
- 2026-09-06：既有 `test_arch_005_integration_publication_fence.py`、`test_arch_005_integration_revalidation.py`、`test_arch_005_s4d_checkout_guard.py` 三文件并行回归 `48 passed / 51.10s`，16 workers/loadfile，exit 0。XML `outputs/validation_runtime/devx014-existing-guards-focused-20260906-v1.xml` SHA `25b357edd5fb41e5488d1d32797fc9302ae7141b5d0703d6a9c7c9921bc41630`。普通门禁源码未修改；此结果不能替代新增模块测试或最终 Full。真实 preservation/研究/DQ/数据/交易动作仍为 0。
- 2026-09-06：新增模块首轮 focused v1 为 `1 failed / 8.57s`（maxfail=1），原因是旧 publication 声明顺序与 guard 规范化排序不同，exact lease scope 重建未先使用 guard 排序。修正重建顺序而非放宽路径集合；该轮期间发生最后补丁，只作为开发诊断，XML `devx014_source_preservation_focused_v1.xml` 原样保留，不绑定事后 SHA 冒充验收。最后静态 Black/Ruff/strict mypy PASS，独立审查的具体安全问题已清零；模块冻结 SHA `85ffb8f720f4e115cd60bec4402d095e231bc7dd3a34f0dcb311a306e2df71cb` 后启动完整 focused v2，尚待结果。无真实 preserve 或 Full。

### S1 并发验收未完成：既有 arbiter 安全依赖

- frozen module 的完整 focused v2 实际为 `62 PASS / 1 FAIL / 1 SKIP / 413.70s`，16 workers/loadfile。XML `outputs/validation_runtime/devx014_source_preservation_focused_v2.xml` SHA `8ccb96b5008a0b2a948d8688bacbf8a1851b371312500679efb37c6ca49e69cf`。SKIP 是明确的 source-commit 前真实实现身份 E2E 前提，不能带入最终 Full。FAIL 是并发夹具遗漏了既有 guard 的异常拒绝路径；不是并发安全已完成。
- 后续 v3/v4 仍 FAIL，v5 单次 PASS 不作为原因已修复。因果诊断最多三次且首 FAIL 停止，实际仅执行 v6a：捕获 `LEASE_ARBITER_STATE_INVALID`，测试的 CAS-only 断言使它转换为 `SOURCE_PRESERVATION_PARTIAL`。XML `devx014_source_preservation_concurrency_causal_v6a.xml` SHA `98666f21bf91f6072c5178e651a397f350c016344ef263bc990b9edf67a5776c`；该轮完整底层原因因 assertion repr 截断而未被保留，不猜测为 missing-owner。
- 另一次、仅一次完整 cause 诊断 v7 为 `1 PASS / 27.67s`，JUnit user properties 记录 `PermissionError [WinError 5] → LEASE_ARBITER_CAS_FAILED → CheckoutGuardError → SOURCE_PRESERVATION_LEASE`，失败者未污染赢家目录。XML SHA `b8bd30d25e2b0223182244b8165d4c1c54612134ee7beb36c4753a91e86b41c0`。v7 没有复现或解释 v6a 的 `STATE_INVALID`，不以再次通过覆盖旧失败。
- 新模块保持 SHA `85ffb8f720f4e115cd60bec4402d095e231bc7dd3a34f0dcb311a306e2df71cb`，最终诊断测试 SHA `ab419e8bf3fcb2c00927fe046a93b5394f89df0e692ad432e61bff500129d840`。既有 kernel SHA `2f22ed9e443db0fc5c810aff5dfeb0f71ee7cc517528fa4e7fa3367b72fa0051` 未修改。

新增 P0 未完成依赖 `DEVX-014/S1a-arbiter-safety`，next responsible party 为 Project Owner / architecture-control-plane coordinator：两名独立 reviewer 均指出既有 `_arbiter` 先读取旧 `RELEASED`，随后未验证 owner generation 就 `os.replace` 当前目录；A 已发布新 `ACTIVE` 后，B 仍可能根据旧读取替换该目录。`finally` 同样不验证自己仍为 owner。该静态交错涉及互斥安全，不能仅称 availability 或正常竞争；发现时尚无实际租约双授予证据。

最优处理是单独审核并修正现有唯一 lease authority 的原子获取/释放，而不是新增第二把锁、在新模块自动重试，或放宽测试使其通过。该 dependency 不在当前 source transaction 的 kernel/test 声明路径内，因此本轮不修改内核、不启动生成/commit/Full/真实 preservation。继续条件：Owner 明确同意扩展到最小内核互斥修正；先登记该串行合同范围及准确路径、重建 publication 声明并通过 preflight，再实施。验收至少包含 deterministic stale-read/ABA 交错、临界区唯一进入、错误 owner 不能 release、crash/expiry/replay、Windows 并发与现有 gate 回归；所有研究/数据/交易动作仍为 0。

原 DEVX-014 worktree 和旧 S2b 18 项修改继续保留，禁止清理；在该安全依赖通过前 S1 并发验收、S2 发布和 S3 真实保全均未完成。没有接受临时绕过措施。

- 2026-09-06：唯一有界 synthetic ABA 诊断实际复现：A/B 都真实读取同一旧 `RELEASED`，A 先真实进入 `_arbiter` 临界区，再让 B 继续；`max_simultaneous_inside=2`、`errors=[]`，exit 0 / 0.641s。仅在线程调度处暂停原 `_read_arbiter_owner` 的真实返回，没有 mock 获取成功，没有修改内核。这证明 arbiter 层互斥缺陷已复现；`actual_execution_lease_records=0`，不声称真实 lease 双授予、仓库破坏或研究结果受污染。单命令临时目录 `C:\Users\32739\AppData\Local\Temp\devx014-aba-diagnostic-qmyhps1c` 已清理，无其它目录删除；诊断源码/输出将作为本任务 runtime immutable evidence 保留。状态转为 `BLOCKED_OWNER_INPUT`，等待上述最小内核修正范围批准，当前 source v2 通过原 publication 命令行政 FAILED/RELEASED 后停止，不提交未通过并发验收的变更。
- 因果证据已按 worker 唯一一次 `python -c` 调用的源码与 stdout 逐字归档，没有再次执行：`outputs/validation_runtime/devx014-arbiter-aba-diagnostic-20260906-v1.py` SHA `a7437be7aa6e325fecb77ba1f608d1c41a655e2f4cc05334d21ffd53c228e402`；同 stem `.stdout.jsonl` SHA `c5ff169a58c479bffb3d0f29f9e8561dd4c734965491385e3cdb5079954c6d00`。归档文件是事后保留的原命令内容，不伪称执行时已存在的脚本。终止原因收据 `outputs/validation_runtime/devx014-arbiter-safety-blocker-20260906-v1.md` 绑定这些文件及当前 canonical event `task-event-e9cea2142542375c3be50fa633b49862`。main/origin/HEAD 仍为 `4150a595ad2b9eb2df11ee552ead959c077aa417`，未运行 generator、正式 tier、Full、真实保全或保留数据复制。
