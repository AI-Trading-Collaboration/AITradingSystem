# ARCH-005 Parallel Development Control Plane

最后更新：2026-07-24

## 任务信息

- task id：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`
- priority：`P0`
- status：`BASELINE_DONE_S4C_VALIDATED_MAIN_INTEGRATION_S5_PENDING`
- owner：architecture coordinator / developer platform owner / integration coordinator
- owner review：project owner 负责 source-of-truth cutover 与调度策略复核
- hard dependency：`ARCH-004C_PLATFORM_CONTRACTS`、`ARCH-004E_DEVEX_OWNERSHIP_GENERATED_INDEXES` `DONE`；现有 task-register consistency baseline
- bootstrap start condition：`SATISFIED`；G2.4 phase exit source=`152f2d33`，`arch_005_bootstrap_handoff.v1`已提交推送并以`f1045634`修正为Git-blob可复算hash basis；`next_slice_unblocked=false`
- approved pre-bootstrap boundary：ARCH-004G2.4-EB2 integration gate 已 PASS，owner 批准的下一实现范围为最终可复用、非 cutover 的 manifest/conflict/lane-plan/evidence primitives；它不是 S0，不得迁移 task registry、切换事实源、生成替代 task views、派发任务或获取真实 lease
- pre-bootstrap status：`COMPLETE_NON_CUTOVER_G2_4_CONTINUES`，slice id=`ARCH-005-PB1`，base=`fe0e19b9`；只新增pure contracts/validators/planner及测试，不生成runtime registry或scheduler state
- integration milestone：S0～S4B 已在 G2.4 handoff 后完成；S4C validated-main integration 已获
  owner 窄授权并由 Wave 7 首次真实执行 PASS；S5 canonical cutover 尚未授权
- current safety follow-up：Wave14暴露同一checkout计划外task/automation第二writer风险；
  `ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD`已登记为`P0/PROPOSED`。Wave14 S2 formal exit
  已完成，当前停在S4D owner authorization gate；Wave15 assignment前仍须经owner确认推进窄版S0/S1
- downstream consumers：ARCH-004 G3/G4/G5 lanes、`PLATFORM-UX-001_SYSTEM_UNDERSTANDING_WORKBENCH`
- production effect：`none`

### S4A 受监督自动化增量

Owner 已批准在 S5 前先实现较窄的 S4A。该增量复用 S2 readiness/scheduler/lease，不修改 canonical
registry：controller 从 exact HEAD 为 engineering 与 research-evidence 创建两条隔离 worktree/branch，
只运行 policy exact-allowlist argv command，并物化 manifest/resource/Git/stdout/stderr/exit/timeout evidence。
两 lane PASS 后只写入等待人工 coordinator 的 integration queue；自动 commit、merge、push、PR、task
status mutation 和策略候选扩展继续关闭。详细边界、分片与验收见
`docs/requirements/ARCH-005_S4A_Supervised_Automation.md`。

S4A 真实 pilot 已完成 fail-closed discovery 与 successful rerun：首次 run 因受限环境缺 `APPDATA`
导致两 worker 无法定位 pytest，同时暴露 CLI wrapper 未传播内部 FAIL；修复后成功 run
`supervised-5de95c5f37821ac3` 的两 worker、13项validator与orphan audit全部PASS，active lease=0，
integration candidate 仍等待人工。当前状态为 formal gates/freshness closeout，S5 仍未授权。

Formal architecture/contract/reproducibility/full 已以 `446/265/23/6430 passed` 闭合，full wall=
`970.42s`、2 skipped、642 warnings，performance telemetry PASS、scheduler 无 fallback。S4A 因而转
`BASELINE_DONE`；它证明受监督执行闭环可用，不代表 S5 self-hosting/canonical cutover 已完成。

### S4B 双线 Operating Model

Owner 已确认后续默认采用 `engineering + strategy-evidence + integration-coordinator`。S4B 不新增
自动调度权限，而是把 S4/S4A 已验证能力转为日常研发协议：worker 从同一 exact base 获取不重叠的
owned paths、module/contract/resource claims，只运行 lane-local focused validation；任务表、system flow、
catalog/registry、root CLI/shared schema、generated manifests/views 与 formal gates 由 coordinator 单写和
统一集成。path/module/API/semantic/generated-view/runtime-resource/base/evidence-lineage 冲突必须在启动前
分类；能通过 leaf module、adapter 或 contract wave 拆解的先拆解，不能安全拆解的显式串行，禁止复制
helper、降低门禁或用 stale artifact 提升表面吞吐。

完整 operating model、冲突决策表、验证/性能复盘规则及近期双线队列见
`docs/architecture/dual_lane_development_operating_model.md`。S4B 仍保持 Markdown 为唯一可写任务事实源，
不授权 S5、worker 自动 commit/merge/push、PR、task status mutation、ARCH-004 G2.5 或策略/生产副作用。

### S4C 验证通过后的 main 自动集成

2026-07-22，Owner 授权 integration coordinator 在候选批次完成全部适用验证后自动完成
`commit -> fast-forward main -> ordinary push`，无需每批再次等待人工合并指令。该授权只缩短已通过
验证的 coordinator closeout，不让 worker 自行合并，也不把 integration PASS 解释为策略 PASS。

自动集成前必须同时满足：

1. 候选 worktree 只有本批可归属变更，shared/generated 文件由 coordinator 单写且 active lease=0；
2. lane focused 与本批 required architecture/contract/full 均 PASS；失败修复 Full 必须绑定原失败
   provenance，文档/generated-only closeout只能在最后代码 Full 之后且不得改变运行语义；
3. module/test manifests、task shadows、compatibility/deprecation/source hashes 对候选最终 tree 新鲜；
4. 本地 `main` 与 `origin/main` 已 fetch 校验，`origin/main` 是候选提交祖先，可使用 `--ff-only`；
5. commit 后 tree bytes 与验证时冻结 tree 相同，push 后必须验证 `main=origin/main=candidate`。

标准动作固定为 fetch、候选归属/验证复核、coordinator commit、切换 main、`git merge --ff-only`、
普通 `git push origin main` 和远端 SHA 复核。脏工作区、validation/base/hash stale、活动共享 lease、
main 分叉、非 fast-forward、无远端权限或 push rejection 任一出现都 fail closed 并报告；不得自动
rebase、建立 merge commit、force-push、删除用户改动或绕过门禁。S4C 不授权 PR 自动创建、S5
source-of-truth cutover、ARCH-004 G2.5、策略 search/promotion、production 或 broker action。

### S4D Shared Checkout Write Lease Guard（PROPOSED）

Wave14真实证明现有“worker owned paths + coordinator单写”只能约束同一计划内的worker，不能阻止
另一个Codex task或daily automation在同一checkout读取半写状态、发起provider/cache mutation或成为
第二writer。完整需求见
`docs/requirements/ARCH-005S4D_Shared_Checkout_Write_Lease_Guard.md`。

S4D建议顺序固定为：

1. Wave14 S2 formal/final Full、归属、commit/push先完整闭合；
2. owner明确把S4D从`PROPOSED`转为可执行状态；
3. S0冻结workspace identity、operation class、owned/shared intent与路径冲突矩阵；
4. S1复用现有ARCH-005 conflict/lease和operations run-control primitives，实现atomic
   acquire/release、heartbeat/expiry/replay及mutation/daily的pre-import/pre-provider gate；
5. 从S4D最终HEAD重新生成Wave15 exact requirement/readiness。

S4D必须path/operation-aware：重叠shared writer恰好一个成功，机械互斥domain仍可并行；不得建立
第三套lock authority，也不得退化为永久全仓串行锁。S2中纯观测telemetry可以随Wave15异步积累，但
任何仍改变shared execution entry的工作必须先通过适用formal gate。S4D不是S5，不切换task-register
source-of-truth，不授权production或broker。

## 决策

后续并行研发不能继续依赖一份由所有 worker 共同编辑的 Markdown 任务表。系统需要一个 Git-native、可审计、可重放、fail-closed 的并行研发控制平面，负责从需求、依赖和资源边界推导可运行任务，分配 execution lane，收集验证证据，并把状态、原因、结果和后续动作投影为人类可读视图。

`docs/task_register.md` 与 `docs/task_register_completed.md` 在显式 cutover 前继续作为权威事实源；cutover 后保留原路径和兼容表格结构，但改为 deterministic generated views，禁止人工双写。任何 source-of-truth 切换必须经过本需求定义的 parity、replay、rollback 和 owner review，不得由 ARCH-004G2 或某个局部实现静默完成。

2026-07-19 的门禁调整只把可直接服务 G2.4 后半程的最终 primitives 提前到 EB2 集成点；正式
S0 inventory/schema freeze、S1 shadow import/projection 与所有 source-of-truth 行为仍等待 phase-level
handoff。pre-bootstrap 产物必须使用与后续 S0～S4 相同的 versioned contracts，不得建立临时第二套
manifest、scheduler 或 lease 语义；若首个受控批次没有可验证收益或出现 shared-path/P0/P1 问题，
停止使用并保留审计证据，不以降低 G2.4 门禁换取速度。

### ARCH-005-PB1 实施冻结

本slice的输入是显式`change_manifest.v1` records、当前base commit、DevEx coordinator-only paths与
`validation_evidence.v1` records；输出只允许确定性的conflict report、lane plan和evidence binding result。
实现必须满足：

- manifest canonical serialization/hash与输入顺序无关，path/base/schema/identity异常fail closed；
- owned/shared path、module、contract read/write/version conflict均给出稳定reason code；
- stale base、domain lane触达coordinator-only path、非`production_effect=none`立即阻断；
- 显式capacity下生成deterministic domain waves，冲突任务不进入同一wave，coordinator只在最终integration wave；
- evidence必须绑定manifest hash、base、required tier、PASS status与root-contained真实artifact SHA；
- 输出固定`dispatch_allowed=false`、`lease_acquisition_allowed=false`，不得写task status、Markdown view、
  registry、lease、production或broker状态。

验收为focused contract tests、DevEx manifests/deprecation/source hashes fresh、architecture/contract/full
门禁PASS以及clean attribution；PB1完成仍保持正式S0、EB3和G2.5锁定。

2026-07-19，PB1 已按上述边界闭合。实现位于
`platform/architecture/parallel_control.py`，提供精确schema解析、canonical hash、path/module/contract
conflict、base/coordinator guard、显式capacity下的确定性domain waves、最终coordinator wave及真实artifact
SHA evidence binding；所有plan/binding输出继续固定dispatch/lease/registry mutation为false。Focused=
`49 passed / 35.50s`，runtime-profile=`23 passed / 15.64s`，正式architecture/contract=
`395/262 passed`；natural Full=`6,350 passed / 2 skipped / 912.17s`，profile/telemetry/performance/
provenance均PASS、scheduler applied且无fallback。真实sidecar机械生成`COMPLETE v8`，精确绑定
`6,352 nodes / 1,071 files`；module/test manifests=`954/1,129`、direct writer=`858`、violation=`0`，
deprecation inventory fresh。该结果不授权S0、registry/view cutover、真实dispatch/lease、EB3或G2.5；
`next_phase_or_slice_unblocked=false`、`production_effect=none`。

## 为什么这是基础设施

当前登记机制同时承担：

- 当前任务状态数据库；
- 全局 latest increment event log；
- owner 与 blocker 队列；
- 需求文档索引；
- 完成任务归档；
- 多个 runtime/report/test consumer 的隐式接口。

2026-07-12 讨论时的初始只读快照显示（用于证明瓶颈，不作为后续 cutover baseline）：

- active register 约 1.27 MiB，337 个活跃任务；
- completed register 约 1.16 MiB，522 个归档任务；
- 两者合计 859 个唯一任务、927 个显式文档链接；
- 最近 200 个提交约 96% 触达 active register；
- 298/337 个活跃任务处于 `VALIDATING`，单一状态轴已经难以表达真实排程就绪度。

这说明瓶颈不是 Markdown 语法，而是把 command queue、current state、history、audit index 和 compatibility API 放进同一全局可写对象。继续人工拆成多个总表只会把一个冲突热点变成多个冲突热点。

## 系统目标

控制平面必须回答并保存证据：

1. 当前有哪些任务，任务为什么存在；
2. 哪些任务已经满足启动条件，哪些仍被什么依赖阻塞；
3. 哪些任务可以并行，哪些因为 path、module、contract、policy 或 production effect 必须串行；
4. 本次为何选择某个任务和某条 lane；
5. 执行者获得了哪些输入、权限、资源租约和验证要求；
6. 执行结果、测试证据、失败原因和集成结果是什么；
7. 中断后如何恢复、重新分配或回滚；
8. 使用者如何理解系统正在做什么、为什么这么做以及后续如何改进。

目标链路：

```text
requirement
  -> canonical task record + append-only events
  -> dependency DAG + readiness evaluation
  -> deterministic ready queue
  -> resource/ownership lease
  -> human/agent/CI execution lane
  -> validation evidence
  -> integration queue
  -> generated status, audit and understanding views
```

## 核心不变量

1. 不同任务不得因为维护全局控制文件而发生文本冲突。
2. task、event、dependency、lease 和 evidence 都必须有稳定 ID、schema version 和 provenance。
3. 调度结果必须 deterministic 或明确记录经过 review 的 tie-break policy；禁止隐藏评分和未治理阈值。
4. 同一资源不得同时授予互斥 lease；unknown path、unknown owner、base drift、contract conflict 和 unsafe effect 必须 fail closed。
5. readiness 只说明是否满足启动条件，不得自动伪造 task status 或验收结论。
6. 同一任务的互斥 state transition 必须形成单一因果链；sibling transition 冲突必须阻断。
7. 没有验收证据不得标记完成；focused/impact-selected validation 不替代要求的 full gate。
8. lane 失败不得污染其他独立 lane；事件必须支持 replay、resume 和 reassignment。
9. shared aggregate 只能由 integration coordinator 确定性生成。
10. 本控制平面不得执行交易、修改投资策略、权重、阈值、promotion、paper-shadow、production 或 broker 状态。

## 逻辑架构

### 1. Canonical Task Registry

推荐采用一任务一稳定目录的 Git-native records；最终路径在 S0 schema review 冻结，当前首选布局为：

```text
registry/development_tasks/<domain>/<TASK_ID>/
  task.yaml
  events/<EVENT_ID>.yaml
```

不得直接放入 `config/architecture/fragments/tasks`：现有 aggregate shadow scanner 会把该目录下全部 YAML 按 `fragment_id/fragment_kind/owner/target_id` contract 解析，`task_record.v1` 与其不兼容。启用 `registry/development_tasks` 前必须先增加明确的 DevEx ownership/change rule、task-specific schema/manifest 和 generated-view target，不能依赖 unknown-path fallback。

`task_record.v1` 至少包括：

- `task_id`、`title`、`domain`、`parent_task_id`；
- `created_at`、`created_by`、`priority`；
- `accountable_owner`、`next_owner`；
- `requirement_refs`、`module_ids`、`contract_versions`；
- typed `dependencies`；
- structured `acceptance_criteria`；
- `production_effect`、`broker_action`；
- legacy source path、row checksum 与 history completeness。

记录的身份和创建事实保持稳定。状态、owner、priority、dependency、acceptance 或 progress 的后续变化通过 versioned event 表达，不通过修改全局表格表达。

### 2. Append-only Task Events

`task_event.v1` 至少包括：

- `event_id`、`task_id`、`event_type`；
- `occurred_at`、`actor`、`change_id`、`lane_id`；
- `base_commit`、`previous_state_event_id`；
- `from_status`、`to_status`；
- `payload`、`rationale`、`evidence_refs`。

普通 progress event 可以稳定排序并合并；status、owner、priority、acceptance 等互斥变更必须进入 task-specific causal chain。旧 Markdown 无法可靠恢复的历史只能标记 `LEGACY_HISTORY_PARTIAL`，不得伪造事件时间或原因。

### 3. Dependency DAG 与 Readiness Engine

依赖必须从自由文本中分离为 typed edge：

- `blocks_start`；
- `blocks_completion`；
- `parent_child`；
- `informational`。

每条 edge 记录 target task、required statuses、rationale、owner 和 add/resolve event。validator 必须拒绝 unknown target、自依赖和 hard-edge cycle。

初始迁移不得从旧 blocker prose 猜测 dependency；未结构化内容保留为 `UNSTRUCTURED_LEGACY`，由后续 review 转换。

治理状态与执行就绪度保持两个维度：

- governance status 首期完全兼容现有 task register；
- readiness 由依赖、资源、contract、validation 和 policy 派生；
- 后续是否拆分 delivery/validation status，必须另经 schema review，不在迁移时静默重解释 859 个历史任务。

### 4. Resource、Ownership 与 Lease

`execution_lease.v1` 绑定：

- task/change/lane/actor；
- base commit；
- owned paths、shared paths requested；
- module IDs、contract versions；
- generated outputs、removal targets；
- required validation tiers；
- production effect 与 broker action；
- reviewed policy version；
- lease lifecycle 与 release evidence。

lease duration、capacity、retry、aging 和 fairness 如需阈值，必须来自带 owner、version、rationale、evidence/review condition 的 policy manifest，禁止硬编码。

### 5. Deterministic Scheduler

scheduler 首期采用可解释的规则引擎，不引入不透明优化模型。它只对满足以下条件的任务生成 ready candidate：

- start dependencies satisfied；
- requirement 与 acceptance contract 完整；
- owner/capability 可用；
- owned-path 与 contract scope 不冲突；
- base revision 未漂移；
- required inputs 与 validation policy 可解析；
- safety boundary 明确。

每次选择或不选择必须输出 reason codes、输入 snapshot、policy version 和 alternatives。人工 override 必须成为审计事件，不能覆盖原始推导。

### 6. Execution Lanes

执行 adapter 至少支持：

- human-owned lane；
- Codex/agent lane；
- CI/validation lane；
- integration coordinator lane。

控制平面负责契约、派发、租约、状态和证据，不把某个 agent runtime、worktree 工具或外部 SaaS 变成 canonical source。adapter 崩溃后必须能从 event/evidence replay 恢复，而不是依赖聊天历史。

### 7. Validation 与 Integration

每个 task/change manifest 声明：

- focused feedback tests；
- minimum validation tiers；
- integration/full gates；
- generated aggregate targets；
- evidence artifact contract。

worker 只能修改其 task/event、owned module、tests 和 fragment。coordinator 按固定 merge order 集成 shared contract、adapter、domain migration、generated aggregates 与 compatibility removal。validator 必须在 merge 前检查 base drift、scope overlap、dependency freshness 和 evidence completeness。

### 8. Generated Views 与理解工作台

由 canonical replay 确定性生成：

- `docs/task_register.md` active compatibility view；
- `docs/task_register_completed.md` terminal compatibility view；
- latest activity；
- owner queue；
- blocker/dependency view；
- lane/lease/integration queue；
- scheduler decision trace；
- System Understanding Workbench read model。

任何 generated view stale 或与 canonical replay 不一致时必须 fail closed。view renderer 不得重新计算业务结论或改变 task state。

## 与现有架构任务的关系

### ARCH-004E

复用已经完成的 module/test ownership、impact selector、architecture fitness、scaffold 和 fragment/shadow aggregate 模式。ARCH-005 不复制这些能力。

### ARCH-004G2 Parallel Readiness Gate

G2 是 ARCH-004 的近期并行验收场景：

- `change_manifest.v1`；
- owned/shared-path overlap；
- base drift；
- coordinator-only paths；
- deterministic fragment preview；
- 三 lane readiness rehearsal。

`change_manifest.v1`、lease/conflict primitives 和 scheduler kernel 由 ARCH-005 冻结为长期 contract；G2 使用它们完成 G3/G4/G5 的三 lane rehearsal，不再建立第二套临时 manifest/scheduler。ARCH-005 增加 canonical task registry、dependency/readiness、execution adapters、event replay、integration queue 和 generated task views。为避免当前共享工作区和 manifests 被两个控制面同时修改，ARCH-005 S0 在整个 G2.4 phase-level handoff 后启动；真实 S4 dispatch 再与 G2 readiness milestone 汇合。

### ARCH-004H

手工 task register 的 source-of-truth retirement 属于受治理 cutover。ARCH-005 可以先运行 shadow 和 controlled dispatch；正式切换需与 ARCH-004H 的 aggregate/source retirement gate 对齐。

### PLATFORM-UX-001

Understanding Workbench 是本控制平面的只读客户端之一，不持有 scheduler state，不成为第二套 status/task source。

## G2.4 -> ARCH-005 Bootstrap Handoff Gate

Owner 已于 2026-07-12 确认此 handoff。它是 ARCH-005 实现的唯一启动入口，不要求 owner 在 G2.4 结束时人工暂停工作区。

### 触发条件

触发点是整个 `ARCH-004G2.4` phase exit，而不是某个 G2.4A～G2.4ZZ slice 完成。ARCH-004 coordinator 必须先证明：

- remaining callback/migration matrix 全部闭合；
- phase-required focused、architecture、contract 和 full validation PASS；
- module/test manifests、compatibility baseline、deprecation inventory、source hashes 全部 fresh；
- 可归属变更已经提交并正常推送；
- 无未提交的 ARCH-004 shared-path 改动或未完成 integration operation；
- 既有无关用户文件被明确列出且没有混入提交；
- `production_effect=none`、`broker_action=none`。

任一条件未满足时不得伪造 handoff，也不得通过跳过验证、吸收无关改动或口头说明继续。

### Handoff Contract

ARCH-004 coordinator 生成并验证 `arch_005_bootstrap_handoff.v1`。S0 冻结最终路径和 schema；最少字段为：

- `source_task_id=ARCH-004`、`completed_phase=ARCH-004G2.4`；
- `head_commit`、`base_commit`、`branch`、`push_status`；
- migration matrix completeness 与 remaining count；
- required validation artifact refs、status 和 checksums；
- module/test manifest、compatibility baseline、deprecation inventory 的 path/hash/freshness；
- active shared-path owner/lease/integration count；
- known unrelated worktree files 与 attribution；
- `next_slice_unblocked=false`；
- `production_effect=none`、`broker_action=none`；
- generated time、producer version 和 handoff checksum。

### 停止与恢复语义

1. 当前及后续 G2.4 slice 正常完成，不中断在途原子工作。
2. handoff 写入、验证、提交和推送后，ARCH-004 不得选择 G2.5、G3/G4/G5 或其他下一 slice。
3. ARCH-004 任务停止在 handoff boundary；工作区无需删除或关闭。
4. ARCH-005 只从 handoff 冻结的 commit 启动 S0/S1。
5. ARCH-005 S0/S1 通过后，必须有新的显式恢复指令，ARCH-004 才能进入 G2.5。
6. handoff artifact stale、hash mismatch 或 worktree attribution 不清时 fail closed，回到 ARCH-004 coordinator 修复。

## 实施阶段

### S0：契约、库存与 Characterization

- entry gate：`arch_005_bootstrap_handoff.v1` validation PASS 且 `next_slice_unblocked=false`；
- 冻结 `task_record.v1`、`task_event.v1`、dependency、lease、scheduler decision 和 generated-view contract；
- 记录现有两份 register 的 bytes、checksum、parser version、ID/status/owner/docs-link 集合和 row checksums；
- characterization 现有 runtime/report/test consumers；
- 冻结状态兼容、terminal projection、排序和 Markdown renderer 规则；
- 明确 source-of-truth cutover 与 rollback owner。

退出：schema 和状态机 review 完成；当前库存无丢失、无 ID overlap；migration baseline 可重复生成。

### S1：Shadow Registry 与 Compatibility Projection

- importer 由旧 Markdown 生成 per-task shadow fragments；
- canonical replay/compiler 生成 shadow index 和两份 compatibility views；
- 旧 Markdown 仍是唯一可写事实源；
- 禁止人工 dual write；
- semantic parity 覆盖全部任务字段、terminal 分类和 docs links；
- 相同输入重复生成 byte-identical。

退出：shadow parity PASS，S0 冻结 baseline 中的全部任务无丢失；所有无法恢复的历史被诚实标记。

### S2：Dependency、Readiness、Conflict 与 Lease Kernel

- typed dependency graph 与 cycle validation；
- readiness reason codes；
- change manifest binding；
- owned/shared path、module、contract、base drift 与 production-effect conflict validation；
- lease acquire/release/expire/reassign event contract；
- crash/replay/idempotency tests。

退出：unknown、conflict、cycle、stale base、unsafe effect 全部 fail closed；non-conflicting task 可并行获得 lease。

### S3：Read-only Shadow Scheduler

- 从 canonical snapshot 生成 ready queue 和 scheduling decisions；
- 只建议，不派发、不修改 task status；
- 对照人工队列运行至少两个 governance cycles；
- 披露 selected/not-selected reasons、policy version、capacity 和 alternatives；
- 发现差异只记录 evidence，不静默修正历史状态。

退出：同输入同 policy 结果一致；所有差异有分类和 owner disposition。

### S4：Controlled Three-lane Dispatch

- 选择三个不改变投资逻辑的独立 ARCH-004 slices；
- 分配 human/agent lanes 和 coordinator lane；
- 执行 lease、validation evidence、integration queue 和 failure recovery；
- conflicting fixture 必须在启动前拒绝；
- lane 失败不得阻断无依赖 lane；
- focused feedback 后仍执行规定的 phase/full gates。

退出：三 lane 持续并行，无 task-register shared-write conflict；merge/recovery/replay 证据完整。

### S4B：双线研发 Operating Model

- 默认选择一项可执行 engineering task 与一项可执行 strategy-evidence task；没有合格任务时允许 lane 空闲；
- worker 只持有独占 leaf paths，coordinator-only/shared/generated paths 在 integration wave 单写；
- contract/API/policy 变更先形成最小串行 contract wave，再从同一新 base 并行 domain implementation；
- focused validation 在 lane 内执行，architecture/contract/full 在自然集成边界统一执行；
- 记录 conflict、replan、lease expiry、base drift、返工、coordinator wait 与 validation runtime；
- 至少两个真实批次后，才基于 telemetry 评估是否从两个 domain worker 扩容。

退出：operating model、冲突协议、近期队列和 shared-path ownership 固化到项目文档；S5 继续锁定。

### S5：Canonical Cutover 与 Self-hosting

- 短暂冻结旧 register 写入并完成最终 import；
- 同一原子变更切换 canonical source、loader、validator、generator、治理规则和 consumer；
- Markdown 标记 generated/do-not-edit；
- 新任务通过 ARCH-005 自身登记、调度、验证和集成；
- 稳定两个 governance cycles 后移除 manual row-move workflow。

退出：直接读取手工 Markdown 的 runtime consumer 为零；generated compatibility paths 保留；rollback 演练通过。

### S6：Understanding 与持续优化

- 向 PLATFORM-UX-001 提供 versioned read model；
- 显示任务、依赖、ready reason、lease、结果、证据、阻塞和建议；
- 基于真实调度数据评估 throughput、queue age、failure/rework 和 conflict 分类；
- 任何新 heuristic、priority aging、capacity 或 fairness 政策单独治理和验证。

## 总体验收标准

- 一任务一 canonical identity，task/event/dependency/lease/evidence 全链路可重放；
- 不同任务的 worker 不再编辑同一任务事实文件；
- task ID、status、priority、owner、blocker、acceptance、docs links 和 terminal projection 无损迁移；
- dependency unknown/self/cycle 和 unsatisfied completion 均可检测；
- overlap、base drift、contract conflict、unsafe production effect 在执行前 fail closed；
- scheduler decision deterministic、versioned、可解释、可人工 override 且保留审计；
- crash 后可以 resume/reassign，不重复产生不可幂等 side effect；
- 三条独立 lane 可并行执行并由 coordinator 确定性集成；
- active/completed/latest/owner/dependency/understanding views 均由 canonical replay 生成；
- task-register consistency、architecture fitness、contract validation、clean-clone、reproducibility 和 full parallel pytest PASS；
- `production_effect=none`、`broker_action=none`。

## 回滚与兼容策略

1. S0～S3 不切换事实源，删除 shadow outputs 即可回滚。
2. S5 cutover 前冻结 legacy checksum 和 importer/compiler version。
3. cutover 必须是单一方向；禁止 YAML 与 Markdown 同时可写。
4. cutover 后如生成视图故障，优先 forward-fix；不得用旧 Markdown 覆盖已经产生的新 events。
5. 如必须回到 pre-cutover source，先把新 events 无损投影为 legacy-compatible snapshot，并由 owner 审核无事件丢失。
6. 原 register 路径在 consumer migration 完成前保持兼容，避免一次性破坏现有报告和测试。

## 已知集成风险

- `task_register_consistency.py` 不是唯一 consumer；Research Roadmap、Safety Boundary、Reader Brief/report schema 和大量 literal task-ID tests 也直接读取 Markdown。S0 必须形成完整 consumer inventory，S5 前先统一 canonical reader/compatibility layer。
- `docs/task_register_completed.md` 当前未纳入 shared integration ownership；S0 必须补充 coordinator/generated ownership。
- Git records 只承担持久审计事实，不能替代运行期原子 lease/CAS。S2 必须由单一逻辑仲裁器发放 lease，并使用 isolated worktree/branch；事后 merge-time 冲突检测不满足安全要求。
- generated active/completed views 不能由每个 worker 随事件提交，否则中央冲突会原样恢复。worker 只提交 task/event/owned files，coordinator 按 integration batch 统一生成 views。
- generated view 的“最后更新”必须取最大语义事件时间，而不是 renderer wall clock，保证 replay 与 byte-identical。
- 现有 `arch_004_worktree_attribution.yaml` 是手工中央清单；change manifest/lease 稳定后必须有显式 supersession，禁止形成两套 ownership 事实源。
- 当前 architecture manifests 可能因并行 G2.4 source/test slice 暂时 stale；本任务不得通过 regenerate 吸收其他 lane 的未提交改动。

## 非目标

- 不在 S0/S1 自动调度或启动 agent；
- 不把 task registry 迁到远程 SaaS、Notion、Jira 或数据库作为唯一事实源；
- 不自动 approve、merge 或关闭任务；
- 不从自由文本猜 owner、dependency、验收状态或 priority；
- 不用调度吞吐替代正确性、审计和 full validation；
- 不执行周期 operations、策略计算、数据刷新、paper/real portfolio 或交易行为；
- 不为追求并行复制 shared helper 或建立第二套 architecture/status/control plane。

## 当前开放问题

- S5 source-of-truth cutover 是否与 ARCH-004H 同一 wave 完成，以及最终 rollback owner；
- 在至少两个真实 S4B 批次后，是否有证据把两个 domain worker 扩为三个；
- Wave14 `D0B2 + bounded G3` 与Wave15 `D0B3 + G4B first consumer`的真实冲突、返工及
  coordinator-wait telemetry是否支持继续保持双domain worker；
- S4D narrow guard的等待时间、false block、lease expiry、unattributed dirty与同checkout
  operations阻断telemetry，是否证明当前瓶颈来自shared mutation safety而不是task source；
- S6 throughput、queue age、conflict/rework 与 coordinator wait telemetry 的长期 read model；
- S5 后 canonical event 写入采用一事件一文件还是 task-local append-only stream。

这些问题不影响已闭合的 S0～S4A 与已采用的 S4B operating model。S4C 已获得上述窄范围
validated-main integration 授权；任何扩大 lane capacity、切换 source-of-truth、扩大自动集成权限或
启动新的domain wave都必须通过该批从最终HEAD生成的exact manifests/ownership/readiness，不能从
历史G2.5 rehearsal或“双线默认”推断。

## 状态记录

- 2026-07-24：Wave14 S2已以replacement Full=`7007 passed / 4 skipped`及post-Full
  evidence-only gates完成，S4D dependency转为`satisfied`，但task仍保持`P0/PROPOSED`。
  `next_slice_unblocked=false`；只有owner明确授权窄版S0/S1后才可实现，且Wave15 exact readiness
  必须从S4D最终HEAD重建。该状态不授权S5、自动task mutation、production或broker。
- 2026-07-24：Wave14集成期间另一个Codex daily automation在同一checkout读取D0B2/G3中间状态并
  修改CLI/DQ/shared文档，主coordinator通过任务消息才停止第二writer。未发生未授权commit/push、
  weights或broker action，但事件证明S4C candidate集成门禁不能替代checkout运行前门禁。新增
  `ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD`为`P0/PROPOSED`；不扩大Wave14范围，建议其
  closeout后、Wave15前经owner确认执行narrow S0/S1，`production_effect=none`。

- 2026-07-22：S4C 首个真实批次已完成。候选分支最终提交=`80ffc28c273ff4e3a2d8a50ebe165ea2e7441a45`，
  fetch 后相对 `origin/main@0fc316e5` 为 `0 behind/6 ahead`；归属工作区 clean、active lease=0，
  focused/architecture/contract=`22/446/265 passed`，代码最终 Full 证据=`6553 passed/2 skipped`、
  runner=`1089.21s`，task shadow、module/test manifests、compatibility/deprecation/source hashes 均 fresh。
  coordinator 执行 `git merge --ff-only` 与普通 push 后复核
  `main=origin/main=candidate=80ffc28c273ff4e3a2d8a50ebe165ea2e7441a45`。S4C 因此转
  `BASELINE_DONE_S4C_VALIDATED_MAIN_INTEGRATION_S5_PENDING`；本次仅为研发集成，
  `production_effect=none`，不解锁 S5、ARCH-004 G2.5 或策略 promotion。

- 2026-07-22：project owner 授权验证通过后的 main 自动集成。范围仅限 integration coordinator 对
  已完成归属、freshness、required focused/architecture/contract/full、generated hashes、lease 与
  fast-forward 检查的候选执行 commit、`git merge --ff-only` 和普通 push；失败一律停止，不允许
  rebase/merge commit/force-push，也不授权 S5、G2.5、策略或 production 行为。当前 Wave 7 作为首个
  S4C 集成批次，候选=`codex/dual-lane-wave7-window-migration@1b46c116`，初始
  `origin/main=0fc316e5`、`0 behind/5 ahead`。

- 2026-07-20：project owner 确认后续默认按 engineering + strategy-evidence 双线推进，并要求重点
  固化冲突处理以提升安全并行度。S4B operating model 已记录 owned/shared/coordinator-only 分区、
  conflict classification、contract wave、resource claim、base freshness、失败隔离、固定 integration
  order、formal gate 去重和双批次 telemetry 扩容条件；首批队列为 OPS-065 + TRADING-2449 canonical
  artifact recovery audit。S5、自动集成和 G2.5 仍未授权。

- 2026-07-20：project owner 批准先推进 pre-S5 的较窄受监督自动化版本，boundary=
  `ARCH-005-S4A-SUPERVISED-AUTOMATION`。本批连接 S2～S4 内核与 isolated Git worktree、受审核
  command worker、evidence binding 和 human-gated integration queue；不自动 commit/merge/push，
  不切换 task source，不启动策略 candidate expansion 或 production。详细切片与验收见
  `docs/requirements/ARCH-005_S4A_Supervised_Automation.md`。

- 2026-07-20：S2～S4 受控 pilot 已闭合并转
  `BASELINE_DONE_S2_S4_COMPLETE_S5_PENDING`。S2 typed dependency/readiness/conflict/lease kernel
  fail closed；S3 两个 read-only governance cycles 生成相同 decision id 与 bytes；S4 成功 dispatch
  `controlled-dispatch-aca2d27f60304e5a5c60`，工程 lane 预期失败后仅重试自身，研究 lane 未受阻，
  coordinator 仅在两项 evidence PASS 后启动，成功链 13 个事件 replay PASS、active lease=0，pilot
  validator 29 项全 PASS。两条失败演练链均保留并以追加 expiry event 收口；未删除或改写历史。
  正式 architecture focused/fast-unit/architecture-fitness/contract-validation/reproducibility/full=
  `85/317/436/265/23/6420 passed`，full 另有 2 skipped、643 warnings、wall 956.37s；相对上一轮
  978.80s 缩短约 2.3%，slowest 50 未出现新增 S2～S4 测试，没有性能回退。
  完整设计、输入输出、失败恢复与优化边界见
  `docs/architecture/arch_005_s2_s4_closeout_2026-07-20.md`。S5 尚未授权，legacy Markdown 仍是唯一
  可写事实源，ARCH-004 仍停在 G2.5 前；`production_effect=none`。

- 2026-07-20：project owner 明确批准按 S2 → S3 → S4 继续推进，并采用“一条工程 lane +
  一条证据型策略研究 lane + integration coordinator lane”的受控试运行边界。本批先冻结
  reviewed pilot policy，再实现 typed dependency/readiness、single-arbiter lease/CAS/replay、只读
  deterministic scheduler 与 controlled dispatch/failure recovery；S4 只允许
  `production_effect=none`、不修改 task governance status、投资逻辑、策略阈值、paper-shadow、
  production 或 broker 状态。S5 canonical cutover 与 self-hosting 不在本批，Markdown 在 S4 完成后
  仍是唯一可写事实源。

- 2026-07-19：正式S0/S1已闭合并转`BASELINE_DONE`。S0冻结`task_record.v1`、`task_event.v1`、
  `task_dependency.v1`、`execution_lease.v1`、`scheduler_decision.v1`与
  `task_register_generated_view.v1`，并生成两份legacy register的bytes/SHA、row checksum、
  ID/status/owner/docs-link集合与125个runtime/test/script consumer characterization。真实库存为
  active/completed/unique=`427/442/869`、ID overlap=`0`。S1在
  `registry/development_tasks_shadow/<source>/<sha-prefix>/<task-id-sha>.yaml`生成869个per-task
  shadow fragments及`arch_005_task_shadow_index.v1`；重复生成byte-identical，两份compatibility
  projection与原Markdown逐byte一致。发现55行含超过8个cells的legacy歧义：保留raw row、全部cells和
  既有first-eight-cell投影，标记`LEGACY_HISTORY_PARTIAL`，不猜分隔边界。Fast/architecture/contract/
  reproducibility/full=`300/419/265/23/6394 passed`；Full另有`2 skipped/642 warnings/961.89s`，相对
  phase-exit Full 946.63s约增加1.6%，最慢测试族一致，无异常性能回归。Markdown继续是唯一可写事实源，
  dual-write/dispatch/lease/status mutation均false，`production_effect=none`。S2与ARCH-004 G2.5均未
  自动解锁；后者仍需owner新的显式指令。

- 2026-07-19：S0 entry的持久复验发现首版handoff对compatibility baseline记录的是Windows mixed-EOL
  worktree bytes SHA，而Git commit只保留LF blob，导致跨checkout无法只凭source commit复算。门禁按预期
  fail closed；未采用EOL猜测或跳过校验，直接把v1 tracked-file hash basis修正为
  `source_commit_git_blob_sha256`，更新checksum、focused 9 passed并以`f1045634`推送后恢复S0。

- 2026-07-19（历史状态，已被上方S0/S1完成记录取代）：G2.4 phase-level exit已PASS，matrix=`967/0/0/0`，四级validation、manifests、
  deprecation、source hashes与clean attribution均闭合。`arch_005_bootstrap_handoff.v1`严格validator
  已冻结，但handoff尚未写入/验证/提交/推送，因此正式S0仍保持等待，
  `next_slice_unblocked=false`。下一步先推送phase-exit source commit，再从该commit生成handoff，
  避免artifact对自身commit/hash形成循环引用。

- 2026-07-12：project owner 确认并行任务调度系统是后续研发基石并要求高优先级推进。完整 scope、阶段、验收和回滚已冻结，任务以 `P0/READY` 独立立项：它会决定资源冲突、验证证据和集成是否允许通过，属于系统正确性与审计基础设施；需求已 READY，S0 实现等待完整 G2.4 handoff，真实 dispatch 仍受 S4/G2 门禁约束。当前仅建立需求和迁移边界，不改变 runtime、task-register source-of-truth、scheduler、production 或 broker。
- 2026-07-12：owner 进一步确认不在 G2.4 进行中并发启动 ARCH-005。已向当前 ARCH-004 主任务预置 phase-level handoff 指令：完整 G2.4 exit、validation、commit/push 和 manifest freshness 闭合后生成 `arch_005_bootstrap_handoff.v1`，设置 `next_slice_unblocked=false` 并停止在 G2.5 之前；owner 无需届时手动暂停工作区。
- 2026-07-19：G2.4-EB2 integration gate 已 PASS，matrix仍为`745/222/0/0`，因此整个phase exit与
  正式S0仍未通过。Owner已批准的pre-bootstrap primitives现在可开始；其退出必须证明versioned
  manifest、path/module/contract conflict、base drift、coordinator-only guard、deterministic lane plan
  与validation evidence binding均fail closed。它不创建registry/view、不dispatch、不发真实lease，
  `next_slice_unblocked=false`、`production_effect=none`。
