# DEVX-015：任务源码 Checkpoint 与验证发布分离 V2

## 身份、授权与当前状态

- task：`DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2`；P0；`IN_PROGRESS`。
- next owner：Codex engineering coordinator；Owner：Project Owner。
- Owner 于 2026-09-09 在完整机制设计草案后要求“你推进下这个流程？”，授权推进工程治理分阶段实现；不扩大研究、业务数据、production 或 broker 权限。
- mode：`SINGLE_LANE` 最小串行 workflow-contract wave；frozen base：`4ac10a668f203486f4f22921f7e2548adfc9ef42`。
- 全流程硬约束：主线正常前进不使任务源码失去安全保存途径；候选验证只证明精确候选；发布仍执行全部既有安全与最终验证门禁。

## 问题证据

OPS-080 独立 worktree 已有 92 项修改、新增和生成文件删除；source-v5 在 main 前进后遇到 `PUBLICATION_EXPECTED_MAIN_STALE`。现有 planner 要求 clean committed lane，而 DEVX-014 V1 只保全 tracked unstaged regular modifications，max_files=64。不能通过删文件、拆批规避限制、手工伪造 task event、关闭 main freshness 或改写旧事务解决。

默认 checkout lease root 是每个 project_root 下的 outputs；已观察两个 linked worktree 是独立目录。仓库级互斥必须通过真实跨 worktree/独立进程测试，不把此静态风险直接当成本次事故原因。

## 分阶段合同和顺序

### S0：准入、身份及复现合同（当前阶段）

1. 正式登记本任务和具体范围，保留 OPS-080 原工作区、历史事务及证据。
2. 固定任务源码身份、候选身份、验证身份、发布资格四类结论，不能互相冒充。
3. 首要验收：两个真实 linked worktrees，B 在 A 保存期间推进 main；A 含新增/删除的 92 项工作仍完整保存，且不获发布资格。
4. 固定项目 Python 3.11，明确执行代码来自本任务 src。S0 新发现的裸 Python 3.14 偏离项目环境及 partial lease 事故保留，不以新 PASS 覆盖。

### S1：正常 source-only checkpoint（最小首批实现）

- 复用既有 Git 环境、reparse/exclusion、安全读取、FileExecutionLeaseStore/OS arbiter；不新增第二个锁内核、scheduler 或队列。
- Checkpoint 只保存已声明 task scope 内的显式请求文件；任务身份、source HEAD、branch、Git common dir、配置及文件完整性仍精确核验。不枚举/读取/hash 未请求的工作区内容，未请求 tree 条目继承 source HEAD；不证明全任务完整性或干净工作区。
- main/origin 的观测值作为事实记录，不作为 task-only 保存的冻结要求；task HEAD/index/源文件变化仍拒绝。
- 支持普通文件修改、新增、删除；rename 使用明确 delete+add；对请求路径拒绝 symlink/reparse、submodule、unmerged、未归属和 known-unrelated 内容。所有请求的归属、排除、reparse、元数据预算先做完整两遍检查，再读取来源字节；不调用会隐式读取未请求工作区内容的 Git status/check-attr。
- 使用版本化 `SOURCE_ONLY_EXPLICIT_PATHS` 检查能力；`source_mutation_allowed=false`、`unscoped_worktree_status=NOT_INSPECTED`、`clean_integration_status=NOT_EVALUATED` 同时出现在不可变 intent、receipt 和 CLI 中。能力字段须进入 ChangeManifest hash，不能用 ACTIVE/shared 租约冒充普通 mutation、publication、task-source 或 Full 权限；历史 scope grant 只接受普通 mutation intent，不能递归授予。
- 独立验证 implementation schema、完整必需文件集合与真实历史 Git blobs；冻结首次验证的实现身份，允许 trusted main 只前进无关内容，代码/配置变化仍拒绝。生产级证明不得用 unit mock 替代。
- 策略显式规定工程资源预算并先做元数据预算检查；支持本次 92 项变更，不自动拆批绕限。
- 使用不可变、可独立验证的 source-only Git 内容身份与原字节证据。保存成功不代表生成物一致、测试通过、clean integration 或发布权限。
- 保存阶段不修改 main/remote、canonical published task state、源码文件和用户 index；不运行 generator/Full，也不要求持有 publication lock。
- 源码 checkpoint 与最终 task branch commit 的衔接在 S2 明确实现；在 S2 通过前不能把 snapshot receipt 送入现有 planner 冒充 clean lane。
- 持久记录 acquired/captured/objects/ref/verified/released 的实际阶段；异常保留源文件与部分证据，不自动重复业务动作。

### Task source checkpoint capability (DEVX-015 S1)

`scripts/architecture_arch005_task_checkpoint.py plan|capture|validate` saves only
explicit ordinary source paths bound to a released historical task mutation
intent. Its `SOURCE_ONLY_EXPLICIT_PATHS` v2 lease is not source-write permission:
the source branch/HEAD, real index and files stay unchanged. Unrequested live
worktree files are not inspected; their snapshot tree entries inherit source
HEAD. Always report `unscoped_worktree_status=NOT_INSPECTED` and
`clean_integration_status=NOT_EVALUATED`; never treat this as a complete dirty-set
audit, clean task commit, final validation or publication approval.

Use the exact committed implementation and reviewed Python/Git environment.
The plan binds its historical implementation across separate capture processes;
unrelated main/origin advancement is only an observation, not source drift.
Source/code/config/index drift and incomplete evidence still fail closed.
Ordinary mutation/publication/Full/preflight consumers must reject the source-only
capability even when its lease is ACTIVE or explicitly listed as allowed. Keep
old writer migration and checkpoint-to-integration consumption in the separately
reviewed S2 stage; S1 does not authorize automatic rebase/merge/cherry-pick,
canonical task changes, generators, Full, main fast-forward or push.

### S2：仓库级共享协调与源码进入集成

- repo identity 从真实 git common dir 派生；注册唯一共享控制面位置，复用既有 store/queue。
- checkout 写入资源、host Full 容量、main 发布资源分别作用于真实对象；跨 worktree 的两个进程不能同时取得同一发布权或单 Full 槽。
- old/new writer 的版本化迁移必须先排空活体执行者，保留旧历史；不复制 ACTIVE lease，不靠 TTL 抢占仍持有 OS 锁的进程。
- 已验证 checkpoint 进入集成时保留原任务分支与唯一证据，在一个受控候选内吸收审阅的任务净差异；不得自动 rebase/merge/cherry-pick 或改写用户历史。
- 若 Git 操作需要现有规则未许可的具体方式，先冻结其精确合同再执行，不能以新命名绕过。

### S3：候选验证与短时间发布分离

- 候选 C 固定 code/tree、环境、policy、生成器和输入。普通 main 推进不抹去 V 对 C 的结果。
- 新 C2 必须有自己的全部必要验证；旧 PASS 不可重标。Full 仍在最终候选自然边界串行执行。
- 发布在短锁内重新核对 expected main，并以条件更新完成；main 工作目录/index 保持一致。
- local main、remote push、runtime activation 分阶段留存实际结果，崩溃后先核实再续作。
- 使用既有队列的显式优先级和恢复路由，不新加周期 scheduler；等待不是任务失败。

### S4：OPS-080 实际恢复、集成及运营验收

- 原始 92 项工作经过完整归属审计后保存，独立重验 bytes/操作类型/来源；原工作区仍保留。
- 在最终候选官方重建 generated authorities，再执行 focused、Architecture、Contract、Integration、Reproducibility、Full、canary、deployment acceptance 及原任务要求的新 session 验收。
- 仅符合既有风险授权的运营动作可执行；不新增回测、业务下载、订单或 broker 操作。
- 不在 S1 宣称 OPS-080 已解阻或发布完成。

### S5：生成链与使用体验收敛

- source inputs S 与 final candidate C 分开绑定；生成物不嵌入包含自身的最终提交 SHA。
- 本地任务进度与 canonical published 状态区分；所有 canonical event append-only。
- 聚焦有证据的循环依赖、环境入口检查、等待与清理；不要求 OPS-080 先实现所有远期优化。

## 首批实施路径与验收

S1 新增 `task_checkpoint.py`、`architecture_arch005_task_checkpoint.py`、版本化 policy 与 focused tests；coordinator 负责本需求、AGENTS/system flow/runbook 和 canonical/generated authority。实现需要扩展现有安全 helper 的精确声明时，先更新事务与任务记录。

必须覆盖：新增/删除/92项、main在捕获前中后推进、source HEAD/index/config/bytes 漂移拒绝、文件预算、请求中未归属/secret/exclusion/reparse拒绝、未请求私有文件存在但绝不打开/hash且保存成功、租约竞争、崩溃恢复、不完整记录拒绝、重复id不覆盖、原始字节与Git身份独立验证；v2→v1 能力降级伪装与各写入消费者拒绝。原 guard/fence/DEVX014 回归保持。全部研究/业务数据/交易动作=0。

## 2026-09-09 受控衔接授权与当前进度

- 首轮独立 focused 测试 36 passed / 610.41s；随后新增7例未运行，共43例。该记录只证明旧实现的定向测试，不能替代新候选验证。独立审查要求上面的显式路径隔离、历史 implementation 验真和 frozen identity 三项修复，旧实现尚不用于 OPS080。
- main 前进至 `0507e4dd129d2a33cd61479d9226dab7ea3dd5cd`，v2 transaction 官方返回 `PUBLICATION_EXPECTED_MAIN_STALE`，已通过官方 release 记录 FAILED/RELEASED；旧工作区及证据保留，不修改旧 canonical/transaction。
- Owner 在获知该阻塞后明确同意一次受控衔接：保留原工作区，在最新主线上建立唯一候选，仅迁入明确归属工程文件，重新登记任务并完成全部验证，不沿用旧生成物或旧 PASS。此授权不是默认自动重建或重写历史的许可。
- 新候选 `D:/Work/AITradingSystem_devx015_integration`，branch `codex/devx-015-controlled-integration`，frozen base `0507e4dd129d2a33cd61479d9226dab7ea3dd5cd`；source/CLI/test/policy/本需求为精确迁入白名单，旧 registry、inputs/generated docs、outputs 不复制。
- 新 v3 transaction `devx-015-controlled-source-20260909-v3`，lease `lease-f52968918dad69b759d9`，PASS 到 TASK_SOURCE_PRE_WRITE；已声明最小合同涉及的 guard/fence/helper/policy/tests 和 coordinator authorities。canonical task 通过当前基线官方 writer 重新登记，不复制旧事件；原 source 作为未合入历史保留。
- 生命周期：新候选完成对应 final-tree 正式验证、main 集成/普通 push、唯一证据归档验证且无进程依赖，再经官方工作区审计清理。旧源码和 OPS080 原工作区在内容归属与可恢复性验收前不删除。Python 仍为3.11；无 production/broker/业务数据动作。

### 新候选 S1 源码/最终候选分段

- v3 为扩展 preflight consumer 声明、v4 为修正 source/final generator 顺序，分别经官方 release 留存 FAILED/RELEASED；未运行生成链或 Full。v4 原因证据为 `outputs/validation_runtime/devx015-v4-source-phase-transition.md`。
- v5 `devx-015-controlled-source-20260909-v5` / `lease-a443dfb4b99b7e93c14f` 到 TASK_SOURCE_PRE_WRITE，canonical cycle 520，LANE preflight PASS。源码阶段顺序为 canonical-task-source → architecture-manifests → report-flow-authority → compatibility-authority；源码提交后另行绑定 exact source 的 Atlas/final transaction 并执行全部正式 tiers，不复用旧 PASS。
- 独立 P1：acquire 可以在 ACTIVE 持久化后、handle 返回前失败。必须预先 create-only 保存 attempt/request/intent_id，失败按该身份只读重放实际 lease 状态，未知即 UNKNOWN；同 checkpoint id 的残留不自动重试。新增故障注入与独立复审是 S1 必须验收项。
- 新候选修复前 checkpoint focused 49 项全部覆盖通过：9 passed / 160.91s 与互补40 passed / 970.49s；包含真实 committed implementation、92项保存、未请求私有文件隔离及独立CLI跨进程/main推进。修复后定向3项通过：正常增改删、真实 store.acquire 写 ACTIVE 后抛错、attempt四种重封hash后的语义篡改拒绝。新测试首轮因测试canonical JSON漏末尾LF而1 PASS/1 FAIL /70.63s，修正该测试序列化后剩余2 PASS /63.75s；不隐去此测试夹具失败，也不将修复前49项当成最终Full。
- acquisition P1 独立代码复审已确认关闭，无新高价值 P1/P2；guard/fence联合47项及最后相关22项通过，preflight功能50项通过。compatibility新增28项合成/绑定测试通过，当前authority检查在官方生成前仍预期陈旧，必须重建后验证。
- core checkpoint Ruff/format、mypy（follow-imports=silent）通过；项目级普通mypy仍见既有导入模块错误，未修改无关模块。最终适用正式tiers仍全部重新执行，尚不宣称Full/发布/OPS080恢复。
- 时间记录更正：`devx015-s1-source-focused-accepted` 的 occurred_at 被协调者误填为 `2026-09-09T13:30:00+00:00`，但随后工具观测当前UTC为13:24:45，故该值不是实际执行时间；此前 `devx015-controlled-source-v5` 的13:10也是手填值，不用作精确执行时序证据。原事件不修改，追加 `devx015-source-timestamp-correction` 使用命令执行时系统UTC说明更正；依赖顺序使用原 append-only event链与真实publication事件，focused结果不变。
- 实仓 committed CLI 验收不再新建候选/工作区：source commit 后先按官方 publication release 保留 source handoff 并释放 v5（否则自身广域租约会与 checkpoint 竞争）；在同一候选对已提交 checkpoint module 建立并释放精确普通 scope intent，不改源码，然后实际 CLI plan → capture → independent validate。新 ref 仅为工程保存证据，tree可与source parent相同；增改删与跨main推进由真实合成Git夹具另行覆盖。HEAD/index/main不变，旧DEVX015与OPS080不参与。该正例通过后才取得 final transaction；此段不冒称尚未执行结果。

## 2026-09-10：正式验证后终态候选的受限集成准入

Owner 明确同意 `owner_decision:DEVX-015:2026-09-10:completed_validated_candidate_integration_v1`：正式验证通过、事务及候选匹配、工作区干净的已完成任务，可进入合入前检查。本项是 S3 阶段契约的最小串行前置修复，不代表 S1 最终验证、S2 仓库级协调或 OPS-080 恢复已完成。

- 保留现有工作区和源码提交 `0ebf327c7a00e32e584044fd02c881f210d1f3de`；使用独立事务 `devx-015-admission-contract-20260910-v1` 记录新改动，不修改旧事务或重标旧 PASS。
- `INTEGRATION` 识别已完成任务必须同时满足 coordinator、repository fence 验证成功、阶段精确为 `LOCAL_MAIN_FF_PRE`、事务 task ID 与请求一致、candidate SHA 与当前 HEAD 一致、audit `PASS` 且 dirty paths 为空。正式 PASS 必须由真实事务阶段链证明，helper 输入字典本身不是验证证据。
- `START`/`LANE` 仍要求活动任务；普通历史已完成任务不获集成权；无效、过期、终止、错身份、错候选、错阶段及 source-only 租约均不能进入受限分支。`CLOSEOUT` 及其余现有门禁保持不变。
- 先更新 canonical skill 及 DEVX-002 契约，验证后再同步 installed bundle。最小验证包括完整 `build_result`/CLI 与真实 Git、租约和事务链正例、参数化拒绝场景，以及现有 preflight/guard/fence 回归。不得用仅 helper 测试证明完整准入。
- 源码阶段重建 canonical task、architecture manifests、report-flow 和 compatibility authority；新候选另行执行适用正式 tiers、Full 与最终发布门禁。有效 bundle 一致性和精确 canonical task 查询的后续接入不得提前引入 final-stage generated freshness 要求或取消已有 writer 校验。
- 本轮不改变投资阈值、DQ/PIT、研究窗口、业务数据、production 或 broker 权限；完成受限准入不等于整个工程重构完成。

验证记录：canonical/installed 五文件 byte parity、skill quick validation、Ruff 与实际 LANE 预检 PASS；真实租约能力/发布 fence 回归 22 passed / 90.60s，XML 为 `outputs/validation_runtime/devx015-admission-guard-fence-v1.xml`。新增完整入口首次 13 例为 10 PASS / 3 FAIL（324s），失败分别是测试 source 标记旧值、audit 真实 CLI 抛错预期和夹具相对事务路径；均已修正测试，未改变门禁。当前完整预检测试 XML 记录于 `outputs/devx015_admission_preflight_tests.xml`，最终结果须以该实际运行及源码阶段 seal 为准。独立行为审阅未发现阻断性实现问题；合成 Full 收据只用于事务消费链测试，不能证明项目 Full 已执行。最终候选正式验证、main/remote 发布和 OPS-080 恢复仍待后续阶段。

## 2026-09-10：首轮 Architecture 失败修复

- 候选 `34ceff8a6a77be6a3f1a924c7001296b950d260a` 的实际 Architecture 运行为
  `811 passed / 2 failed / 2850.56s`；原始 summary 与日志保留在
  `outputs/validation_runtime/devx015-final-architecture-20260910-v1/`。
  原事务经官方 release 为 `FAILED`、租约 `RELEASED`；尚未运行 Full，不能将此报告伪作 Full parent。
- 两项失败分别是 live task 总数仍固定为 1067，而本任务合法新增后实际为 1068；以及
  DEVX-002 requirement 的已批准修改未进入最新 compatibility supersession 声明。
- 修复范围：任务测试以冻结 `0507e4dd129d2a33cd61479d9226dab7ea3dd5cd` 的完整 canonical
  task ID 集合作为保留基线，并核对当前数量、唯一性和本任务身份；允许后续合法新增，仍拒绝
  缺失、等数量替换、重复和计数错配。compatibility builder 与独立测试清单补齐 DEVX-002、
  canonical skill 的 SKILL/workflow 文档和本次修改的 task-source 测试路径；不改写历史 hash。
- 使用新事务 `devx-015-validation-fix-source-20260910-v1`，在原工作区完成有界修复、
  focused 正反例和官方生成，再绑定新 final candidate 执行全部必要正式 tiers。
  不把原 811 PASS 重标为新候选证据，不在本修复中改动 S1 权限、安装版或 S2-S5 运行语义。

## 2026-09-10：历史 supersession 消费链修复

- 同一候选 `1d413e4beab8c4e5f25cfa67756d30f3cbe2d489` 的 V3 Architecture
  实际结果为 `950 passed / 1 failed / 1063.61s`，退出码 2；完整回执保留在
  `outputs/validation_runtime/devx015-final-architecture-20260910-v3/test_runtime_summary.json`，
  SHA-256 `2647a4144974e35622476e6daa464c0cbf59d2370eff1135b44003095c2bcef1`。
  V2 中断无终态证据，不能替代本轮结果；V3 事务已官方 FAILED/RELEASED，无 Full parent。
- DEVX-015 最新生成声明已包含已批准修改的 DEVX-002 requirement；失败原因是
  `test_devx_003_is_preserved_historical_authority` 共用的历史 supersession 聚合未接入
  这份后续声明。新事务 `devx-015-compatibility-chain-fix-source-20260910-v1` 只补齐该
  消费连接，检查声明与 sources 的路径集合一致，保留历史 prefix/raw hash 和当前哈希验证。
- 必须验证原失败、合法后续声明和未声明/不一致路径拒绝，并运行完整 refactor-policy
  文件以覆盖同一消费链的其它历史断言；生成链更新后最终候选仍执行全部必要正式 tiers。
  不重标旧 PASS，不改变 S1 权限、S2-S5、OPS-080 或业务/交易边界。
- 第一轮完整 focused 实际 `237 passed / 74 failed / 699.70s`；全部失败的集合差仅为同一
  DEVX-002 文档路径（38项左侧多、36项右侧多），表明只向外层 union 补声明不足以覆盖
  历史等式断言。XML SHA-256 `b32db5dbc5cb1e84f61488f8ddd6a43b86f7b15e0fd1d051b4c1f3c43a842d9e`，
  原源码/本需求/兼容性index及fragment四份原字节已复制、逐项hash复核并保留在
  `outputs/validation_runtime/devx015-historical-chain-v1-source-evidence/`；旧source事务 FAILED/RELEASED。
- 新source V2 移除该外层补丁，将 DEVX-015 接入既有 `_latest_active_source_mismatches`
  的 temporal authority 列表。继续沿用原owner在边界之前、明确后继声明、历史已记录差异
  不消除、当前权威不处理自身的规则；新增隔离测试证明未声明漂移、边界owner和最新权威自身
  漂移均保留，后继不存在时无豁免。不通过改写旧captured hash或扩大路径常量解决。

## 2026-09-10：Full V4 失败与最小修复

- exact `11dee152719cf1d9a6fdd1d598a05e7fbf99230e` 的 Full 已实际结束：
  `367 failed / 12486 passed / 6 skipped / 1 teardown error`，pytest 3921.09s，
  退出码1；官方 summary SHA-256 为
  `88ab6d24fde11f0519c8c9b522866b43b36c1d3b5f5369bb8fbb0a302a792dff`，
  保留于 `outputs/validation_runtime/devx015-final-full-20260910-v4/`。
  事务已正式记录失败结果并 FAILED/RELEASED；详细归因与原字节hash见
  `outputs/validation_runtime/devx015-full-v4-failure-triage-20260910.md`。
- 完整368个失败块分为324个AGENTS身份连锁、43个显式父关联缺失、1个synthetic
  extra_intent清理error，无未分类项。profile把teardown失败的整个node归为failed，
  pytest另计该node的call PASS和teardown error；不得相加或隐藏该差别。
- 新source事务 `devx-015-full-fix-source-20260910-v1` 已声明本需求、AGENTS、
  `tests/test_named_quality_dispatch.py` 和既有coordinator/generated范围，并绑定上述实际
  失败Full parent。canonical任务先更新至cycle527，维持IN_PROGRESS；S2-S5/OPS080仍未完成。
- 文档组织修复：把AGENTS新增的21行S1专属操作条款完整移到本需求上方，保留为必须遵守的
  任务合同；根AGENTS恢复0507原字节SHA
  `3acc539ebc79d4c76501ff3c127b56713fe64b8eaac463905786f1709fd472f7`。
  不改变任何source-only能力、runtime或普通consumer拒绝检查，不修改研究config、
  冻结历史hash或投资语义。直接重绑研究链会牵动HISTORY_ONLY静态契约，故不采用该扩张方案。
- 测试夹具修复仅在本次新建synthetic intent被篡改时，同时断言recheck和release拒绝、
  handle未released且无新事件，再在finally恢复该新夹具原始字节供正常清理。
  不修补本轮失败scratch证据，不关闭严格schema或伪造released状态。
- 新final候选必须显式提供自己的真实 `AITS_NAMED_DQ_PUBLICATION_TRANSACTION` 与
  `AITS_NAMED_DQ_SOURCE_LEASE_ID`；先执行真实父关联正例，再运行必需Full。不得寻找latest、
  增加默认值、skip测试或把环境字符串当授权。新Full使用failure_fix_rerun和上述正式parent。
- 验收：实际AGENTS raw SHA恢复、21行原文完整保留、研究配置和runtime无变更、相关拒绝回归
  与既有静态策略loader通过；官方生成后，新候选重新执行全部required tiers。
  此节只记录修复合同，不预先宣称实现、测试、main/remote发布或OPS080恢复完成。

实际源码修复验收：27个完整文件以16 workers/loadfile运行，`642 passed /313.52s`，
外层进程退出码0；XML SHA-256 `7ff0e33e6fe2b3ccda8420ece25683a1348ebc03f77b64f9b0c99fe3e7c7306d`。
对旧Full失败node逐项核对，324个AGENTS身份失败全部在新XML中PASS、遗漏0，extra_intent
拒绝及清理测试亦PASS；未执行的43个真实父关联测试仍须在新FORMAL_PRE下验证。
canonical cycle528记录该结果，任务保持IN_PROGRESS；正式生成、新候选全部required tiers和发布尚待执行。

## 原始工作区生命周期（已由上方受控衔接记录替代）

唯一工作区 `D:\Work\AITradingSystem_devx015_checkpoint`；branch `codex/devx-015-checkpoint-publication-v2`，从上述 frozen main 新建。其它工作区均有任务或唯一证据，不复用、不清理。用途是本任务分阶段实现与最终验证；退出条件为对应成果集成及普通 push、唯一证据规范保留逐项 hash 验证、无活动进程和运行依赖，再经官方 worktree audit 清理。项目 Python 为 `D:/Work/AITradingSystem/.venv/Scripts/python.exe`，使用期间不得清理其来源环境。

## 实际进度与事故记录

- 2026-09-09：Owner 授权推进；独立工作区 governed audit PASS，未改 OPS-080、main、remote 或 runtime。
- 首次 v1 acquire 使用裸 Python 3.14（偏离 README 指定 3.11），返回 `LEASE_ARBITER_STATE_INVALID`。路径 stat 的 ctime=1788954027929709600，handle fstat=1788954027930714500，其余身份字段一致；同一原版检查在项目 3.11.9 PASS。不改检查，不声称兼容性修复。
- 此失败已实际写 ACTIVE lease `lease-70d923a33cb90ecefbf3`，但未创建 publication transaction。只读重放确认后，经官方 checkout guard release 精确释放，结果 PASS/RELEASED，历史保留。没有 transaction 可用，故不伪造 publication closeout。
- v2 source transaction `devx-015-checkpoint-source-20260909-v2`、lease `lease-ca3744af11ce8f86d91c`，使用项目 3.11、本任务 src，已 PASS 到 `TASK_SOURCE_PRE_WRITE`。task 注册与 LANE preflight 后才开始实现。尚无新增测试/Full 或源码 checkpoint 执行结果。

## 2026-09-10：OPS-081 主线前进后的受控协调

- V5 候选6638bfcfb的Architecture/Contract/Integration/Reproducibility均实际PASS；Contract281passed/414.68s。Full派发前main已前进6498d030，官方拒绝PUBLICATION_EXPECTED_MAIN_STALE，Full未启动。旧事务FAILED/RELEASED，原PASS与V4失败Full不改写。
- 精确plan `integration-revalidation-706b2f052615460bf94a` 独立validate PASS、RECONCILIATION_REQUIRED。协调方案见 `outputs/validation_runtime/devx015-main6498-coordinator-review-20260910.md`；保留OPS081在前、DEVX015在后的双authority安全合同，补充双phase拒绝覆盖，官方重建generated状态。
- 复用 `D:/Work/AITradingSystem_devx015_integration`，新分支 `codex/devx-015-main6498-reconciliation` 从exact6498d030079370f6fc6dd2edf3b71dd0505ed57e开始。原 `codex/devx-015-controlled-integration` 仍保留6638bfcfb及全部证据。切换前clean audit PASS、active leases=0且无关联执行进程；未新建工作区。
- 新事务 `devx-015-main6498-source-20260910-v1` 绑定此main和原lane差异plan。首次INTEGRATION仅TASK_NOT_REGISTERED；先按任务登记例外补齐canonical记录与本需求，再重跑准入，未通过前不改实现。
- 不自动合并/rebase/cherry-pick，不跳过Full，不重标旧结果。完整S2-S5、consumer与OPS080目标保持；候选与原证据在完成治理发布/独立保全/无进程依赖前保留。

## 2026-09-10：Owner 批准精确 frozen-lane canonical 任务准入

Owner 对登记顺序循环的明确建议回复“同意”，记录为
`owner_decision:DEVX-015:2026-09-10:frozen_lane_canonical_task_integration_admission_v1`。
本项不同于 completed-task 准入，不从此前授权推导。

- 仅在 coordinator 的 `INTEGRATION`、干净 exact latest-main candidate、真实有效普通 publication transaction 和独立验证的 integration plan 均匹配时，可使用计划精确 `lane_head` 的 canonical registry 核验尚未出现在当前主线的任务。
- 只从精确 Git commit 的官方 canonical index、该 task 的唯一 fragment 及其完整事件链核验；不得使用旧 Markdown 子串、latest 搜索、用户传入布尔值或不可核验 receipt。验证 source-of-truth、task identity、index/fragment checksum、event hash链、base/commit绑定及非终态任务状态；完整性不足即拒绝。
- frozen lane 的 source-only snapshot 能力不构成 clean committed task lane；检查计划/事务身份与现有拒绝门禁，不把 source-only lease 当普通 authority。
- 任意错 task/lane/plan/repository/candidate、脏目录、错phase/无效/过期/终态事务、缺失/重复/篡改 index/fragment/event、未登记或终态 frozen task 均拒绝。当前主线已有该任务时继续以当前 canonical状态为准，不从历史覆盖当前决定。
- 此准入只解决任务身份先验，不授予 task writer、generator、Full 或发布权限；通过后才按官方 TASK_SOURCE_PRE_WRITE 登记候选任务。START/LANE 和 CLOSEOUT 原规则不变，不放宽干净计划或最终验证要求。
- 最小实现进入 canonical skill 和确定性 reader；沿用既有 canonical schema/hash/事件验证，不复制第二份弱化验证器。不引入全仓 generated freshness 作为历史单任务查询前置要求。
- 必須通过真实Git/事务/plan/完整canonical事件正例、上述拒绝反例、双任务隔离及现有 completed-task/source-only/guard/fence 回归；canonical/installed一致、全部最终required tiers与发布仍独立完成。
- 已产生的四项登记变更和暂停事务hash证据保留。本轮在 exact6498 的 SINGLE_LANE 先开发这项已批准的最小合同，不将已dirty的旧plan重标为clean，也不借LANE门禁执行尚未准入的旧lane源码协调。

### 最小准入合同实现与证据边界

- 新增精确 Git canonical 单任务 reader；不运行历史 Python，不依赖 Markdown 或无关任务 blob。保持 index/policy/fragment/event 严格完整性与当前状态优先。
- preflight 仅在其它门禁全部通过后识别 `FROZEN_LANE_CANONICAL_PRE_WRITE`，仍要求 clean latest-main、coordinator、INTEGRATION、ACQUIRED 普通事务及同一 plan 哈希；其它阶段不变。
- focused：reader 21 passed/13.87s；完整 skill 文件 79 passed/65.91s，其中新增10项真实 Git/canonical/plan/fence/CLI 场景和23项辅助拒绝矩阵。Ruff、reader mypy、skill quick_validate 均 PASS。正式 required tiers 尚未执行，不能将此结果当作最终验收。
- 更正证据保全表述：暂停回执原先仅 hash 绑定四个 live 路径；本轮合法更新后它们已不再是原字节。已依据保留的 append-only 注册事件及确定性序列化事后重建四个独立副本，全部 SHA-256/size 与旧回执逐项相同。回执为 `outputs/validation_runtime/devx015-main6498-reconstructed-registration-20260910/reconstruction_receipt.json`；旧回执和当前事件不改写，不宣称当时已复制。
- 本最小合同先在 exact6498 单独形成验证候选，不吸收尚未通过准入的旧lane实现。旧6638分支及其正式证据保留；后续完整S1-S5、consumer及OPS080验收并未完成或被缩减。
- 最终代码另绑定 verified manifest task ID，且拒绝 plan/manifest 验证期间字节变化。保留此前已批准的 completed-task 受限 INTEGRATION 和 source-only allowed-lease 拒绝规则，避免 installed 更新回退行为；只迁入这些独立规则，不迁入旧lane checkpoint API。
- 最终 skill 测试文件96项全部通过（同一源码的5项PASS＋91项补集PASS，44.96s/120.88s）；旧lane独有API的1项真实source-only capability测试未吸收、未skip、未计为PASS，仍须后续旧lane集成验收。格式化前后AST等价。额外 preflight mypy 的4项错误在 exact6498 原文件复现相同诊断，作为既有问题记录，不扩大本次修复；reader mypy PASS 不等于 preflight mypy PASS。
- 安装包更新前五文件已独立复制并核对原始哈希，保留在 `outputs/validation_runtime/devx015-frozen-admission-installed-before-20260910/`。V5 的17份Atlas文件也已独立保存并逐项匹配旧生成seal，回执为 `outputs/validation_runtime/devx015-frozen-admission-v5-atlas-preservation-receipt-20260910.json`。

### 44e 源候选的任务身份断言修正

- `44e64f3acd2c9df6b6fe2adfad8667a74b0a3163` 源候选及其Atlas/七项readiness实际通过，但尚未派发正式tiers时，独立预审发现存量canonical测试固定1068任务；本轮合法登记后为1069。真实单node xdist复现1 failed/11.38s，保留XML与v1停止回执，不把该失败称为Full失败或修改旧PASS。
- 修正的验收合同是保留精确6498基线的完整任务ID集合、明确存在OPS081与DEVX015，并核对唯一性与计数一致。合法新增不应要求再次改固定总数；删除、等数量替换、重复、计数不符必须失败。只修改相关测试，不改reader、writer或准入行为。
- v1最终事务已官方FAILED/RELEASED；新source事务 `devx-015-frozen-task-admission-count-fix-source-20260910-v1` 继续当前工作区，完成该断言、必要生成及新候选验证。旧44e和V4/V5全部证据保持各自身份；S1-S5/consumer/OPS080并未完成。
- 修正后完整canonical测试文件35 passed/64.38s，包含原21项reader测试及新增身份保留正反例，Ruff/定向diff-check PASS；XML为 `outputs/validation_runtime/devx015-frozen-admission-canonical-full-focused-20260910.xml`。生产reader和skill源码相对44e未变，保留此前96项skill行为结果；全部正式tiers仍须新候选执行。
- 44e的17份Atlas字节已逐项核对其final seal并独立保留，回执为 `outputs/validation_runtime/devx015-frozen-admission-44e-atlas-preservation-receipt-20260910.json`；后续渲染不覆盖唯一旧证据。
