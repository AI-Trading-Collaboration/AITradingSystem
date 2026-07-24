# ARCH-005S4D Shared Checkout Write Lease Guard

## 基本信息

- task id：`ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD`
- parent：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`
- priority：`P0`
- status：`IN_PROGRESS`
- owner：architecture control-plane owner / operations automation owner
- dependency：`SATISFIED_WAVE14_S2_COMPLETE`；建议在 Wave15 domain assignment 前完成窄版 S0/S1
- owner decision：`owner_decision:ARCH-005S4D:2026-07-24:approve_narrow_s0_s1_v1`
- production effect：`none`

## 问题与风险

2026-07-23 的 Wave14 集成期间，研发任务与 daily automation task 同时使用
`D:\Work\AITradingSystem` checkout。Automation 在 D0B2/G3 文件仍处于中间写入状态时启动过
`daily-run`，随后又修改了 `quality.py`、`quality_execution.py`、CLI wiring 与共享文档；主协调者通过
任务消息才停止第二写入者并取得路径归属。没有发生未授权 commit/push、weights 或 broker action，但这次
事件证明现有“worker owned paths + coordinator 单写”只约束同一计划内的 worker，不能阻止另一个 Codex
task 或 scheduler automation 在同一 checkout 读取半写状态或成为第二个 writer。

这是 correctness、data quality、auditability 和生产边界风险，不能仅依赖聊天约定或人工观察文件时间。

## 目标边界

建立 checkout-scoped、可重放、fail-closed 的写入与运营执行门禁：

1. 为 checkout 建立稳定 workspace identity，不以线程标题或当前进程列表代替身份。
2. Mutation task 在首次写入前声明 task/thread、base commit、owned/shared paths、operation class、
   heartbeat、expiry 和 release condition。
3. Shared-path writer 与 `daily-run`/periodic operation 使用独占 lease；domain path 可以在机械证明互斥后
   并行。
4. Automation 在 dirty/unattributed worktree、活动 mutation lease 或不完整 ownership declaration 下，
   必须在 provider request、cache mutation 和业务模块 import 前 `BLOCKED`。
5. Lease store 必须支持 atomic acquire/release、crash expiry、replay、stale-owner diagnosis 和审计
   artifact；不得靠删除 lock 或修改状态绕过。
6. Known-unrelated path只能通过 exact exclude pathspec处理，不读取、hash或复制其内容。
7. 与 ARCH-005 S4C fast-forward integration、现有 run-control lease和 task registry保持单一责任边界；
   本任务不授权 S5 canonical task source cutover。

## 分阶段工作

### S0 Policy and characterization

- 冻结 workspace identity、lease namespace、operation class和冲突矩阵；
- 为“开发 writer / shared coordinator / daily operation / read-only audit”建立行为 characterization；
- 记录本次 incident 与禁止的半写读取、双写和隐式接管路径。

Owner-reviewed S0 matrix由
`config/architecture/arch_005_s4d_checkout_guard.yaml@1.0.0`冻结：

|Operation class|Workspace gate|Path claim|与daily关系|允许的并行|
|---|---|---|---|---|
|`domain_mutation`|`READ`|declared `owned_paths=WRITE`|被daily `WRITE`阻断|exact path机械互斥时可并行|
|`shared_mutation`|`READ`|declared `shared_paths=WRITE`|被daily `WRITE`阻断|只允许与不相交domain并行，shared重叠仍排他|
|`daily_operation`|`WRITE`|无mutation path|与全部活动mutation排他|同checkout不并行|
|`read_only_audit`|`READ`|无mutation path|不产生业务写入|只读审计可并行|

路径冲突按casefold后的ancestor/descendant关系判断。workspace identity由resolved checkout与Git common
dir生成稳定SHA identity，并记录exact HEAD/upstream；lease authority继续使用
`execution_lease.v1` / `execution_lease_event.v1`，不新增第三套锁。

### S1 Narrow local guard

- 实现本地 checkout lease store、CLI/API preflight、heartbeat/expiry/replay；
- 将 mutation entry 与 `aits ops daily-run` 的 pre-import/pre-provider边界接入；
- 对互斥 domain workers保留并行能力，不把整个仓库退化为永久全局串行锁。

本轮实现入口为
`src/ai_trading_system/platform/architecture/checkout_guard.py`与
`scripts/architecture_arch005_checkout_guard.py`。Mutation使用显式
`acquire -> heartbeat -> release` API/CLI；重复相同intent会重放同一active lease，不依赖PID，
stale heartbeat在下一次atomic acquire前转为`EXPIRED`。`aits ops daily-run`在函数体与任何plan、
run bundle、provider、cache或report写入前持有daily WRITE gate，并在持有期间自动heartbeat。
BLOCKED decision固定声明零provider/cache/report/production/broker副作用。

Known-unrelated exclusion仅由policy登记exact
`docs/research/growth_tilt_owner_diagnosis_pack.md` path；guard只把该literal pathspec传给Git status，
不得打开、读取、hash或复制该文件的bytes。

### S2 Integration and telemetry

- 接入 supervised automation/S4C closeout与 Codex task handoff metadata；
- 输出等待时间、冲突原因、lease持有时间、误阻断与无归属写入 telemetry；
- 只有证据显示 task-register source-of-truth成为主要瓶颈时，才另行评估ARCH-005 S5。

## 验收条件

- 两个 writer 请求重叠shared path时恰好一个成功，另一个在写入前typed BLOCKED。
- Daily operation在活动研发lease或未归属dirty state下零provider request、零cache/report mutation。
- 两个机械互斥domain scope仍可并行，shared coordinator保持单写。
- Crash、stale heartbeat、重复触发、PID复用、路径大小写/祖先后代冲突和symlink/junction均有负例。
- Lease replay、worktree attribution、base/head/remote lineage和known-unrelated exclusion可独立验证。
- focused、architecture、contract、integration、reproducibility及required Full PASS。
- `task_source_cutover=false`、`production_effect=none`、`broker_action=none`。

## 当前决策

Wave14 S2 formal exit 已完成。2026-07-24，owner 通过
`owner_decision:ARCH-005S4D:2026-07-24:approve_narrow_s0_s1_v1`
明确授权窄版 S0/S1，从 `PROPOSED` 转为 `IN_PROGRESS`。授权顺序固定为先冻结 S0 policy /
characterization，再实现 S1 local guard；两者必须在 Wave15 两个 domain worker 启动之前完成并通过
适用正式门禁。S2 telemetry、Wave15 assignment、ARCH-005 S5、task source cutover、production 与
broker 仍未授权。

2026-07-24：dependency evidence 已闭合：Wave14 C7 replacement Full=`7007 passed / 4 skipped`
且post-Full evidence-only gates PASS。该结果与上述 owner decision 共同解除 S0/S1 implementation
gate；`next_slice_unblocked=false`仍适用于 Wave15，`production_effect=none`保持不变。

2026-07-24：S0 policy/characterization与S1实现已进入正式验证。当前focused evidence覆盖稳定
workspace identity/lineage、不相交domain并行、daily全局排他、并发重叠writer恰好一个PASS、重复
intent replay、casefold/祖先后代冲突、dirty/unattributed零业务输出、known-unrelated exact
exclusion、heartbeat/stale expiry、symlink/reparse拒绝及真实`aits ops daily-run`在run bundle前
阻断。只有required formal tiers和Full全部PASS后，task才可转为窄版`BASELINE_DONE`；S2与Wave15
仍保持未授权。
