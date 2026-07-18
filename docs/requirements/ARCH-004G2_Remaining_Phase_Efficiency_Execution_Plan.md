# ARCH-004G2 剩余阶段效率优先执行计划

最后更新：2026-07-18

## 任务信息

- task id：`ARCH-004G2_REMAINING_PHASE_EFFICIENCY_EXECUTION`
- parent：`ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION`
- companion：`ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE`
- priority：`P0`（G2.4 critical path）
- status：`IN_PROGRESS`（owner 已批准并启动 EB0；EB1 尚未启动）
- owner：architecture coordinator / test infrastructure owner
- production effect：`none`

## 决策背景

截至 2026-07-17，G2.4 callback matrix 以 `7c4cce3e` 为冻结基线：

- baseline callbacks：967；
- migrated：715；
- pending：252；
- unresolved / duplicate：0 / 0；
- current total leaves：993；
- `phase_exit_ready=false`。

CX3 已完成并推送，但随后 S2A 正式 full 虽然 `6,185 passed, 2 skipped`，wall time 达
`18,238.77s`，最大长尾 node 达 `17,801.26s`。继续采用小型 callback slice 加逐 slice full
会重复支付相同长尾、fixture 构建、shared-doc 刷新和 manifest/hash closeout 成本。Owner 因此批准
从效率出发调整顺序：先处理具有重复收益的最高长尾，再以 bounded integration batch 完成剩余
matrix，最后对稳定 callback 集合完成 phase closeout。

本计划只改变开发和验证编排，不改变任何 DQ、PIT、threshold、score、backtest、weight、promotion、
paper-shadow、production 或 broker contract，也不改变 ARCH-004 -> ARCH-005 的停止条件。

## 目标与边界

目标是在不减少 required gate 的前提下，把剩余 252 callbacks 从微切片改为 8 个可审计的
integration batches，并把高成本性能工作限制在 critical path：

1. 同一 producer/report/validator/legacy subtraction 不跨 batch；
2. 开发期持续运行 focused tests，正式 architecture/contract/full 在 batch integration boundary
   各执行一次；
3. 任一正式 gate 失败必须先诊断根因，不用 serial、skip、xfail、关闭 source validation 或临时
   workaround 替代 PASS；
4. shared docs、manifests、compatibility、deprecation 与 source hashes 在每个 batch 安全集成点统一刷新；
5. 真实 dated research evidence 的积累不阻塞结构迁移；缺证据继续诚实输出
   `INSUFFICIENT_EVIDENCE` 或等价状态；
6. ARCH-005、G2.5、G3～G7、ARCH-004H 及 DATA-GOV/Knowledge/Publishing/UX 不提前启动。

## 调整后的关键路径

```text
EB0 top-tail timebox
  -> EB1 / EB2 / EB3 signal-to-formalization chain
  -> EB4 evidence materialization and input readiness
  -> EB5 paper-shadow and shadow health
  -> EB6 weight calibration/research
  -> EB7 p2/experiment/satellite/simulation residuals
  -> EB8 AI/decision/forward/core residuals
  -> stable-set runtime acceptance
  -> G2.4 phase exit
  -> arch_005_bootstrap_handoff.v1
  -> STOP before G2.5
```

2026-07-17 S3A Weight Search第一批已完成实现并转入正式验证：五个既定hardening文件的同命令
wall time由`889.01s`降至`254.19s`（约`-71.4%`），未减少nodeid、all-view rebuild或tamper
矩阵。当前只保留Follow-up完整测试会话复用与Decision上游PASS-only fingerprint迁移；无收益的
Targeted/Diagnostics/Evaluation实验修改已撤销。最终正式focused=`291 passed / 1 skipped /
269.09s`、architecture=`312 passed / 51.75s`、contract=`204 passed / 43.65s`、full=
`6,195 passed / 2 skipped / 642 warnings / 1,789.86s`，full较pre-change缩短约`16.3%`。
按owner“本轮跑完后暂停复盘”的要求，本批不继续扩张到Research Foundation；EB1继续暂停，等待本轮
复盘后由owner选择Research Foundation第二批或恢复callback主线。

2026-07-17 owner随后批准继续尾部优化，启动有界S3B：先审计Signal Feature Quality=`744.32s`，
再审计Near-Miss/Promotion/Next Plan共享Weight Search DAG；Research Foundation与Smoothed不并发
扩张。S3B仍属于EB0 runtime closeout，不解锁EB1；只有同命令before/after与全部正式门禁闭合后才
再次暂停复盘。

S3B实现已完成并进入正式门禁：五个Signal/Weight目标的同命令从`498.20s`降至两次候选
`399.15s/363.87s`（约`-19.9%/-27.0%`），focused=`85 passed / 1 skipped / 430.45s`。
只保留Signal的消融回到`498.69s`，证明五个并行node必须保持一致session边界。Weight fixture内部
已是独立线性DAG，跨worker store风险超出本批，故未扩张第二项实现；EB1继续未启动。

S3B正式门禁闭合：full=`6,195 passed / 2 skipped / 642 warnings / 1,720.76s`，较S3A
`1,789.86s`再缩短约`3.9%`；architecture=`312 passed`、contract=`204 passed`。五个目标在full
中缩短约`19.6%～40.3%`，但剩余尾部已转为Research Foundation setup、Targeted hardening和
Smoothed Refresh。按既定边界本轮暂停复盘，不自动启动Research Foundation或EB1。

2026-07-18 owner批准继续三个Research Foundation候选，启动有界S3C。范围仅含Research
Direction=`487.92s`、Micro Search=`478.61s`、Signal Diagnosis=`455.51s`三个setup；先量化
不同loadfile worker的嵌套前缀重复构建，再评估fixture规模、单worker复用与安全跨worker store。
Smoothed、Targeted、EB1及下一callback slice不并发启动；S3C正式门禁闭合前不改变主线状态。

S3C同三文件pre-change=`51 passed / 333.25s`，三个setup分别为`328.00s / 279.32s /
310.34s`。审计确认跨worker store会引入absolute-path/self-commitment、共享可写root、锁恢复和
tamper copy-on-write风险，因此不实施；改为使用production generator仍完整覆盖全部8个required
families的最小test-only前缀。50 variants仅覆盖7个family并被正式validator fail-closed；52
variants通过config validator且保留默认80路径。候选同命令=`51 passed / 204.73s`，缩短约
`38.6%`；expanded focused=`54 passed / 249.89s`。正式architecture=`312 passed / 56.85s`、
contract=`204 passed / 42.28s`、full=`6,196 passed / 2 skipped / 642 warnings / 1,830.80s`
均PASS。三个目标在full中降至`318.96s / 314.23s / 278.90s`（约`-34.6% / -34.3% /
-38.8%`），但full总时长较`1,720.76s`增加约`6.4%`，说明其他loadfile尾部与运行波动已遮蔽
局部收益；S3C闭合但runtime任务继续，不自动启动Smoothed、Targeted或EB1。

2026-07-18 owner批准继续优化full整体性能并允许独立耗时单项多agent并行，启动S3D。最新full=
`6,196 passed / 2 skipped / 642 warnings / 1,830.80s`，top-50累计`13,724.22 worker-seconds`；
Smoothed、Weight/Targeted/Search、Research governance/promotion分别占top-50约`28.7% / 27.9% /
22.8%`。先以三个只读lane审计共享DAG和duration-aware调度，再由协调者选择互斥文件范围实施；
开发期使用focused/分片，正式architecture/contract/full仅在integration boundary各执行一次。EB1及
下一callback slice继续暂停，不因性能lane并行而改变G2.4/ARCH-005 handoff边界。

S3D审计后选择三项互斥实现并通过局部门槛：Smoothed五文件=`435.79s -> 348.57s`
（约`-20.0%`），Weight/Targeted五文件=`435.56s -> 313.45s`（约`-28.0%`），共享Signal
Foundation=`10 passed / 216.03s`；production默认80及targeted 60～120规模不变，compact
test-only路径仍完整覆盖全部required families。full lane用top-50聚合的44文件`PARTIAL_SEED`做
duration-descending stable loadfile排序，并新增全量node/file/worker/tail-idle sidecar；invalid profile
显式stock fallback且不能形成performance PASS。pre-full审计同步修复worker侧真实execution contract、
sidecar异常不得覆盖pytest exit和`pytest-xdist>=3.8`能力floor；串行/非loadfile/worker mismatch/
filtered selection均不得误报调度PASS。独立复核进一步要求最终canonical collection实际满足duration
stable order、sidecar exit与subprocess exit一致，并在summary前atomic写入final sidecar以记录真实hash；
任一不一致只令performance evidence FAIL。最终telemetry focused加family边界=`38 passed / 11.17s`，
14文件扩大focused=`37 passed / 395.37s`。architecture、contract、full、manifests与source hashes仍
pending，EB1和
下一callback继续暂停，`production_effect=none`。

Post-full closeout复核进一步把runner sidecar acceptance改为独立派生：严格JSON、真实runner invocation、
duration manifest bytes/metadata/fixed-point、完整test-manifest文件集合、collection/node/phase/file/worker/
outcome/session-window聚合、phase UTC与formal scheduler policy/tie/fallback语义必须一致，
unmatched file/worker identity或contract evaluator异常必须
fail closed，不能用不完整或flag-only payload伪造performance PASS。Runtime artifact改为
auxiliary-first、summary-last finalization，reader/log/sidecar记录最终真实hash/size，summary self record
显式省略自引用hash/size且不再预写`exists=false`；空output覆盖旧log，外部JSON与summary使用同一final
payload；外部JSON与任一managed artifact path冲突时fail-fast。相关focused先行通过，真实formal sidecar
经新contract重验仍PASS；无需重复运行full。

S3D formal integration最终闭合：新增test file使deprecation inventory预期`1125`被architecture首轮正确
拒绝，刷新到`1126`后architecture=`322 passed / 59.65s`、contract=`214 passed / 45.32s`、full=
`6,224 passed / 2 skipped / 642 warnings / 1,231.76s`。full相对`1,830.80s`直接基线缩短约
`32.7%`，slowest-50累计缩短约`16.0%`；完整sidecar证明16-worker final order、6,226-node
collection、exit binding、44/44 tracked file命中与phase completeness全部PASS，worker busy CV=`1.10%`、
tail capacity=`2.12%`、最大tail=`37.64s`。该结果只形成第1个complete profile，不声明连续3次稳定
改善；S3D切片完成而长期runtime任务继续。下一性能lane只读审计Layer1 meta-policy、promotion/owner
governance、Refined Method/Weight Dashboard，须在本切片提交后另行登记互斥实现范围；EB1和下一callback
未启动。Post-full contract hardening新增22个runner/evidence测试后，正式architecture/contract已重跑为
`344 passed / 50.28s`与`236 passed / 35.75s`；既有full证据经严格reader重验仍PASS，但未为同一切片
重复执行full。
继续暂停，`production_effect=none`。

2026-07-18 owner批准继续降低full耗时，S3E在首个complete profile上完成Layer1/Layer2、Refined
Method、Weight Dashboard与Promotion/Owner governance四个互斥lane的审计并实施有界优化。Layer1/
Layer2 forward outcome统一改为PIT等价、逐字段exact且按临时cube元素预算分块的NumPy kernel；Refined、
Weight与Owner仅延长单test/worker PASS-only content-fingerprint session；Candidate/Next/Sensitivity仅在
fixture显式使用覆盖全部required families的compact matrix，production默认规模不变。首份profile的
file busy / worker-seconds与隔离focused墙钟分别记录，不混作同口径结论；未删除nodeid或弱化tamper、
DQ、PIT、source replay、lineage、all-view验证。多个候选合并后只执行一次自然integration-boundary
architecture/contract/full；下一次full仅在order、exit、telemetry、performance及manifest freshness
全部PASS后才成为第2份complete profile。EB1与下一callback继续暂停，`production_effect=none`。

S3E首次候选full完整运行到100%但因nested-xdist外部临时suite collection竞态得到`6,245 passed /
1 failed / 2 skipped / 1,083.96s`，故只保留方向性性能trace、complete profile仍为1。根因修复保持
生产runner cleanup不变，把三个nested subprocess隔离到各自`cwd/rootdir/confcutdir=tmp_path`并使用
显式仓库`PYTHONPATH`和相对suite；原两文件、外层16-worker真实plugin组合连续3次均`59 passed`，
650次同前缀TEMP兄弟目录churn压力回归为`3 passed / 5.85s`。下一步刷新manifests/source hashes，
重跑architecture、contract与一次标准full；不得把失败run计为第2份profile。当前manifests刷新为
`947 modules / 1,126 test files / 858 writers / 0 violations`，architecture=`344 passed / 50.37s`、
contract=`236 passed / 37.29s`已PASS；标准full重跑=`6,246 passed / 2 skipped / 642 warnings /
1,109.04s`并由strict reader确认profile/telemetry/performance全PASS；但提交前code review发现多个非法
值时整表转换改变legacy首异常优先级，因此该run只作完整性能方向证据、不计最终第2份qualifying profile。
有效输入继续走批量fast path，conversion failure才按decision→horizon→strategy旧顺序重放；双非法值
exact type/message golden与focused/Ruff已PASS；刷新manifests/hashes后architecture=`344 passed /
50.53s`、contract=`236 passed / 34.47s`均PASS。replacement standard full=`6,246 passed / 2 skipped /
642 warnings / 1,040.50s`（`full_20260717T231920Z`），strict reader确认profile/telemetry/performance、
order、exit、16-worker loadfile、no fallback与manifest freshness全部PASS，成为第2份qualifying profile。
相对首份complete profile墙钟约`-15.53%`、worker busy约`-15.60%`、nearest-rank file P99约`-29.54%`、
八个S3E目标约`-55.03%`，tail max/total=`32.46/355.47s`；`complete_profile_count=2/3`，仍固定
`stable_full_improvement_claimed=false`。下一批按最新profile治理Smoothed operations/evidence session
断点、retry/weekly authority prefix与两份tracked research Markdown的test direct-writer隔离，不为每个
候选或为了凑计数单独跑full；第3份只在下一自然
integration boundary取得，日常研究继续使用DQ、workflow validator与focused tier。

S3F已按`231920Z`重新冻结Smoothed关键路径并完成三个互斥lane：Operations五leaf只延长test-local
PASS-only session，Evidence/Readiness只装饰11个磁盘链node，Retry/Weekly五热点使用真实Promotion→
Binding/Switch→recorded `continue_observation` authority prefix后继续各自完整链。无负载同命令
`-n16 --dist loadfile`结果分别为`165.38 -> 119.34s`（`-27.84%`）、`195.41 -> 61.99s`
（`-68.28%`）、`287.40 -> 249.57s`（`-13.16%`）；目标call合计`2,267.95 -> 1,647.07s`
（`-27.38%`）。两处tracked research Markdown direct writer已完整重定向到test tmp，exact=`2 passed /
94.12s`且运行后两文档与HEAD一致；expanded focused=`134 passed / 1 skipped / 356.52s`。生产代码、默认、
DQ/PIT/source/lineage/all-view/tamper、nodeid与安全边界均未改变。Manifests/compatibility/source hashes与
deprecation inventory已刷新，governance/architecture/contract分别为`28/344/236 passed`；唯一一次自然
integration-boundary full=`6,246 passed / 2 skipped / 642 warnings / 1,027.74s`，6248 nodes/1068 files、
order/telemetry/performance/no-fallback均PASS，成为第3份profile。相对`231920Z`，worker busy=`-1.94%`、
Smoothed38=`-16.04%`、S3F17文件=`-19.00%`，但wall仅`-1.23%`且tail/CV变差，说明关键路径已转移。
下一批先并行只读审计Layer1、Smoothed Weekly/Refresh、Refined Method与Weight Diagnostics，再冻结互斥
实现lane；不重复运行本批full，`stable_full_improvement_claimed=false`、`production_effect=none`。

S3G只读审计完成并冻结四lane。Layer1同文件6 nodes共有70次相同`_build_context`，计划仅在各node的
direct producer block内使用PASS/DQ/PIT及live-fingerprint约束的immutable context session，CLI和tamper
路径保持真实；Smoothed只给Refresh两个未装饰节点延长既有validation session，不缩短authority prefix；
Refined前两个read-only nodes共享带tree-fingerprint finalizer的module fixture，tamper独立，Diagnostics只
启用已存在且覆盖8个required families的52-variant test matrix；调度lane把44-file旧`PARTIAL_SEED`升级为
兼容旧状态、严格绑定`004439Z` 1,068-file/6,248-node source identity的`COMPLETE` profile，任何全集、hash、
worker或dist不匹配都fail closed/fallback。各lane须先取得无其他pytest负载的同命令isolated baseline，
并行实现后同命令after；四lane合并后才执行一次architecture/contract/full自然边界。不得减少nodeid、
DQ/PIT/source/lineage/all-view/tamper，不以离线LPT约`28.8s`容量机会宣称实际wall收益，EB1与下一callback
继续暂停，`production_effect=none`。

S3G isolated before已在无其他pytest进程、显式candidate `PYTHONPATH`下顺序完成：Layer1=
`6/357.91s`；Weekly、Refresh、合并=`1/230.57s`、`4/213.56s`、`5/267.77s`；Refined、Diagnostics、
合并=`3/330.34s`、`1/275.80s`、`4/381.70s`。B/C合并时aggregate call因资源竞争分别增至约
`518.21/721.35 worker-s`，所以after同时比较单文件wall、合并wall与call，不把合并墙钟遮蔽当作无收益。
Layer1与Refresh最小实现已按互斥文件落地且尚未由协调者运行after；Refined/Diagnostics与complete profile
进入并行实现，正式集成门禁仍pending。

S3G A因收益不稳定已撤回：同`357.91s` before的两次after为`330.74/362.52s`，约314行test helper
不能证明稳定收益，Layer1文件已byte-exact恢复HEAD。B Refresh单文件=`213.56 -> 185.66s`（`-13.06%`），
两个目标call=`-32.70%`；合并wall受Weekly/retry波动`+2.82%`，但目标call仍`-34.77%`。C Refined=
`330.34 -> 276.60s`（`-16.27%`）、Diagnostics=`275.80 -> 240.19s`（`-12.91%`）、合并=
`381.70 -> 247.81s`（`-35.08%`），aggregate phases=`-36.33%`；immutable tree finalizer与独立tamper
均PASS。B/C保留；D已在不改变现有59个runtime-contract nodeids前提下实施COMPLETE profile，最终
`59 passed / 12.56s`。1,068-file/6,248-node manifest的source/collection/file/row/order hashes与
`15,830.6369954 worker-s`总量全部机械断言并由loader重验；strict reader双向拒绝伪applied/伪fallback，
独立6248-node静态重建为profile/telemetry/performance、coverage、order、manifest rebind全PASS，P0/P1/P2
未留项。扩大focused=`168 passed / 1 skipped / 285.14s`，唯一skip为既有Windows/`os.fork`条件；
manifests/compatibility/deprecation初始刷新和治理focused=`23 passed / 32.19s`均PASS。正式
architecture=`344 passed / 58.90s`、contract=`236 passed / 45.03s`均PASS。

S3G唯一一次自然边界full=`6,246 passed / 2 skipped / 642 warnings / 1,243.76s`，exact
`6,248-node/1,068-file/16-worker`集合、COMPLETE order、no-fallback与profile/telemetry/performance均
PASS。相对`004439Z`，raw wall=`+21.02%`、worker busy=`+24.49%`、file P99=`+29.07%`；`981/1,068`
相同文件变慢且倍率median=`1.2625x`，证明本轮存在广泛执行膨胀，B/C三文件合计仅`+0.27%`，不是
回退来源。另一方面worker CV=`1.529% -> 0.00619%`、tail total/max=
`460.495/42.281s -> 0.177/0.017s`，说明COMPLETE排序几乎消除了调度尾部损失。当前两次并非同commit
受控A/B，不能判定是外部机器状态还是重型文件前置竞争；保留strict COMPLETE基础设施但把duration-only
策略视为pilot，`stable_full_improvement_claimed=false`。不以异常慢run刷新权重、不空跑full；下一批先
只读建模resource-aware stagger/bucket并按新profile选择互斥高耗时lane，下一次full仍只在自然边界执行。

2026-07-18 owner继续授权S3H多agent优化。下一批只实施两个低风险互斥lane：Search Coverage Gap单文件
启用既有52-variant complete-family test matrix；No Promotion/Near Miss Candidates/Cash Buffer三个legacy
结论文件启用相同compact前缀。两lane均须顺序取得无负载同命令before、并行改动、至少两次无负载after，
稳定改善门槛为10%；production默认80、8 required families、nodeid、DQ/PIT/source-lineage/tamper及
threshold/conclusion不变。第三lane只读比较duration-only/stock/resource-aware stagger/bucket，不在缺少
可复现resource class和wall/busy/P99不回退设计时修改默认调度。共享文档/manifests/compatibility与正式
集成由协调者独占；下一full仍只在S3H自然边界运行一次。

S3H两实现lane已达到退出门槛。Search Coverage Gap=`120.28s -> 95.77/91.64s`
（`-20.38%/-23.81%`）；三个legacy结论文件=`86.17s -> 69.38/62.57s`
（`-19.49%/-27.39%`），三个call合计=`173.51s -> 137.13/119.09s`。四文件均只在既有helper
调用点增加compact flag，52 variants仍覆盖8/8 families，production 80、nodeid、断言及安全链不变；
expanded focused=`6 passed / 116.57s`。调度只读模型发现新首批16同时启动3个setup-majority Foundation、
旧首批为0，静态rank`0/16/40` stagger不增加离线makespan；但profile缺RSS/I/O/read-amplification且存在
全局`1.2625x`膨胀，因果证据不足，本slice不修改duration-only pilot。下一步刷新共享manifests/hashes，
运行architecture/contract与唯一一次自然边界full；不增加第三个业务lane或额外full。

S3H自然边界full=`6,246 passed / 2 skipped / 643 warnings / 1,294.07s`，exact collection、COMPLETE
order、no-fallback与profile/telemetry/performance均PASS。四目标full合计=`440.604s -> 351.160s`
（`-20.30%`），但raw wall/worker busy相对S3G=`+4.05%/+4.08%`，其余文件累计约增加
`893.022 worker-s`，故保留局部改动但不声明全局稳定改善。worker CV=`0.00519%`、tail total/max=
`0.119/0.014s`再次证明调度均衡已接近收益上限；Foundation setup相对全局没有额外恶化，继续不做
经验stagger。S3H闭合为`COMPLETE_RUNTIME_TASK_CONTINUES`，下一批先并行只读审计最新profile的
Smoothed、Signal与Weight/Search高成本DAG，完成任务登记与isolated baseline后再实施；EB1及下一
callback slice仍未解锁。

2026-07-18 owner批准继续优化，S3I在三个并行只读审计后只冻结两个互斥实验。Lane A严格限定
`dynamic_v3_system_target_helpers.py`与`test_smoothed_forward_weekly_run.py`，尝试显式test-only
160-weekday compact authority；production `end=latest_available`契约不变，synthetic helper默认326
weekday不变，并由测试冻结production start/end/warmup/evaluation20/regime5及test warmup20/evaluation10/
regime2；compact PASS不冒充production样本充分性。worst-after改善不足10%或任何
Smoothed method/regime/sample、weekly九步、DQ/PIT/source/lineage/clean rebuild/tamper契约失败即撤回。
Lane B限定Targeted production module与hardening test，只有调用计数先证明五个unchanged上游validator
重复时才接入既有PASS-only content-fingerprint cache；worst-after不足10%或30s、FAIL被缓存、tamper未
失效即撤回。Signal/Diagnosis需跨worker immutable bundle及更强atomic/live-binding/COW架构，当前slice
拒绝；Diagnostics仅作为Lane B后续调用计数观察，不自动实施。两lane先顺序取得无负载baseline，再按
互斥文件多agent实现；合并后只运行一次architecture/contract/full自然边界，EB1/下一callback继续暂停。

S3I无负载isolated before已顺序完成：Smoothed weekly=`1 passed / 228.52s`、call=`225.40s`；Targeted
hardening=`1 passed / 188.80s`、call=`185.79s`。同两项在最新full为`413.31/373.28s`，因此局部
裁决只使用isolated same-command口径。两个互斥agent进入实现，但不得自行并发跑after；协调者待所有
编辑完成且无其他Python/pytest后，对每lane顺序运行至少两次after。

Lane A首轮after=`2 passed / 209.62s`、重型call=`206.33s`，相对before仅改善`8.27%/8.46%`。
预登记worst-after 10%门槛已不可达，故不以结果为由降低标准，也不浪费第二轮；两个Lane A文件已
byte-exact恢复base，裁决`REVERTED_THRESHOLD_MISS`。Lane B调用计数已证明coverage/near-miss/
scorecard/weight-backfill/paper-backfill分别重复`4/13/38/41/44`次，但首版legacy scope被安全审查
阻断：真实paper validator还读取两级semantic-selection inventory与nested cache paths。Lane B只有在
bounded schema/edge resolver显式覆盖全部paper cache path、weight独立price-root DQ siblings、Model
Target与Daily Advisory inventories，且任一解析失败直接绕过cache后，才允许进入after。

Lane B最终通过两轮独立P0/P1=`0`安全审查。实现只在active synchronous validation session内为
Coverage Gap、Near Miss、Weight Scorecard、Weight Backfill与Paper Backfill五个adapter复用exact PASS；
resolver以白名单schema→typed edge遍历完整DAG，校验Foundation/operations binding envelope，并显式
fingerprint全部paper cache paths、Weight独立price-root两个DQ siblings和Model Target / Daily Advisory
semantic-selection inventories。单/总bytes、JSON nodes、path、binding、inventory、queue与depth均bounded；
malformed、unknown、relative、commitment drift或预算异常全部直接执行真实validator，FAIL与exception不缓存。

同命令正式after=`136.04/136.92s`、call=`132.77/133.66s`；按worst相对before `188.80s`、
call=`185.79s`分别改善`27.48%/28.06%`并节省`51.88/52.13s`，超过预登记10%且30s双门槛，
裁决`RETAINED_THRESHOLD_PASS`。五adapter seed/reuse、独立Weight MISSING→PRESENT、Paper required/optional、
Model/Daily inventory、FAIL/exception、exact restore与真实relative-path bypass均有测试。expanded focused=
`82 passed / 1 skipped / 247.51s`；其中Targeted Search v3=`243.86s`仍是下一轮候选，但本轮不追加第三个
业务lane或第二次full。共享manifests/hashes已刷新；pre-full architecture=`344 passed / 59.34s`、
contract=`236 passed / 44.91s`，post-full tracked-state architecture/contract复验均PASS，最终runtime
artifact由compatibility baseline记录。唯一自然full=`6,246 passed / 2 skipped / 642 warnings /
1,172.32s`，相对同
collection的S3H `1,294.07s`减少`121.75s / 9.41%`；Targeted hardening文件由`373.292s`降至
`210.925s`（`-43.50%`），P99由`342.907s`降至`273.417s`（`-20.27%`）。scheduler为COMPLETE/
applied/no-fallback、telemetry PASS、node/file/worker=`6,248/1,068/16`且ordered collection hash一致。
单次full不构成稳定全局改善，故`stable_full_improvement_claimed=false`；S3I闭合为
`COMPLETE_RUNTIME_TASK_CONTINUES`，EB1与下一callback仍暂停，后续候选选择不触发空跑full。

2026-07-18 owner批准继续优化，S3J在三个互斥只读审计后冻结Targeted、Diagnostics与Smoothed三个lane。
Lane A只改`tests/test_targeted_search_v3.py`：慢测外层延长既有同步validation session，使helper返回后的最终
Targeted validator复用S3I hardened PASS upstream；production/default 60～120 variants、六family和最终内容重建
不变。Lane B抽出`dynamic_v3_weight_search_targeted.py`现有exact hardened DAG resolver至新共享validation-scope
模块，Targeted机械导入保持行为，再让Diagnostics四validator及scorecard/search-space adapter使用同一PASS-only
cache；禁止复制resolver或循环反向import。Lane C最初只允许审计Smoothed operations local-binding，必须先证明
现有间接cache未覆盖才可实现，不改weekly authority/window或四个目标测试的业务断言。

最新full目标为Targeted call=`328.72s`、Diagnostics=`343.63s`、Smoothed四call=
`302.08/300.35/295.50/272.39s`。每lane先顺序取一次无负载same-command before，至少两次after；A/B的
worst-after须同时改善≥10%且≥30s，B另须两轮独立P0/P1审查为0；C的worst-after wall须改善≥10%且≥30s、
四call总和改善≥15%且≥120s、任一call不得恶化>5%。门槛失败即byte-exact撤回，不事后降低标准。三个agent
只持有互斥实现文件，共享docs/manifests/hashes与唯一architecture/contract/full由coordinator集成；EB1、下一
callback及ARCH-005 bootstrap继续不解锁，`production_effect=none`。

三lane无负载isolated before已顺序完成。A Targeted=`1 passed / 195.03s`、call=`191.94s`，冻结
worst-after wall上限`165.03s`；B Diagnostics=`1 passed / 207.93s`、call=`204.73s`，上限
`177.93s`；C Smoothed四节点=`4 passed / 212.18s`，call=`209.01/206.48/200.02/186.83s`、
总和=`802.34s`，冻结worst-after wall≤`182.18s`、四call总和≤`681.99s`，对应单call不得超过
`219.46/216.80/210.02/196.17s`。full争用值不参与局部收益计算；实现现在才允许按上述互斥范围并行。

Lane C随后以零diff裁决`REJECTED_REDUNDANT_CACHE_ALREADY_PRESENT`。operations local source/binding路径经
Smoothed readiness与evidence/method helper后已进入active-session PASS-only content cache，四目标本身也有outer
session；唯一direct uncached latest selector被显式artifact ids短路。耗时来自四个独立`tmp_path`各自首次构建
326-day authority与weekly九步，机械叠加wrapper只会重复fingerprint。该lane不运行after；跨test immutable
authority bundle/COW只登记为后续架构候选，不在S3J自动扩张。

Lane A两次无负载after=`132.96/133.63s`，相对`195.03s` before按worst-after节省
`61.40s / 31.48%`；Lane B=`32.22/32.49s`，相对`207.93s`节省`175.44s / 84.37%`，
两轮独立P0/P1审查均为`0/0`。A/B均为`RETAINED_THRESHOLD_PASS`；Targeted production bounds、
六family、Diagnostics 21 views、schema/cross-lineage/policy/chronology、DQ/PIT/source与all-view tamper保持。
九文件expanded focused=`87 passed / 1 skipped / 126.14s`，唯一skip为Windows无`os.fork`条件用例。
S3J进入`VALIDATING`：先刷新system flow、module/test manifests、compatibility/deprecation/source hashes，
再顺序执行architecture、contract与唯一自然integration-boundary full；EB1、下一callback及ARCH-005 bootstrap
继续不解锁，`stable_full_improvement_claimed=false`、`production_effect=none`。

S3J正式闭合为`COMPLETE_RUNTIME_TASK_CONTINUES`。generated=`948 modules / 1,126 test/support files / 0
violations`，compatibility/deprecation focused=`8 passed/12.67s`，architecture=`344 passed/57.84s`，
contract=`236 passed/44.40s`；唯一full=`6,246 passed / 2 skipped / 643 warnings / 1,079.37s`，
exact node/file/worker=`6,248/1,068/16`，COMPLETE/applied/no-fallback及telemetry/performance均PASS。
相对`070333Z`，wall=`-92.95s / -7.93%`、file worker-s=`-1,491.409s / -8.03%`、P95=
`-12.14%`、P99=`-5.07%`；Targeted/Diagnostics file分别`328.770 -> 47.975s`与
`343.639 -> 63.284s`。单次full不支持稳定全局声明，故`stable_full_improvement_claimed=false`。
下一轮基于新profile并行只读审计Decision leaf（Formal/Gate）、Evaluation hardening与Execution comparison
三个互斥lane，完成任务登记和isolated before后才允许实现；EB1、下一callback及ARCH-005继续不解锁，
`production_effect=none`。

S3K三项只读审计已在commit=`6c66f78a`与`090446Z` profile上闭合。Decision lane仅给
Formal/Gate两个测试函数延长现有PASS-only session；Evaluation lane仅让Backfill经S3J neutral API、
Scorecard经Evaluation本地adapter复用，Matrix仍真实验证；Execution lane第一阶段仅hoist每build内与
strategy/policy无关的QQQ benchmark metrics，不引入comparison/rows/DQ cache。三者都需先取无负载
exact-node before，两次after的worst wall同时改善≥10%且≥30s；Decision的两个node分别裁决。
不达标或出现P0/P1即byte-exact撤回，不临时扩张为高风险缓存。

S3K继续采用分层验证：单lane只跑isolated/focused，retained lane整批合并后才跑一次
architecture/contract/full。日常研究不频繁跑pytest full，只强制DQ gate与本次研究实际依赖的
validator/focused链路；full仅用于integration/phase/broad-change/performance/failure-rerun等显式边界并记录触发原因。
本批不解锁EB1、下一callback或ARCH-005，`production_effect=none`。

四个exact-node isolated before已在无其他candidate Python/pytest负载下顺序闭合：Formal/Gate/
Evaluation/Execution wall=`151.84/119.22/150.26/168.43s`，call=`148.59/116.07/147.10/
163.09s`。按`min(0.90 * before, before - 30s)`冻结worst-of-2 wall上限=`121.84/89.22/
120.26/138.43s`，不得事后放宽。S3K现进入`IMPLEMENTING`，三个agent只编辑互斥文件，
coordinator继续独占共享docs、顺序after/focused与最终集成门禁。

S3K局部裁决已闭合：Formal=`151.84 -> 22.84/23.13s`、Gate=`119.22 -> 22.70/
22.74s`、Evaluation=`150.26 -> 29.99/30.04s`，worst wall分别改善`84.77%/80.92%/
80.01%`，均保留。Execution纯QQQ invariant hoist的第一次after=`173.24s`，未达`138.43s`
上限且高于`168.43s` before；因worst-of-2已无法达标，两个实现文件已byte-exact撤回，
不自动扩张到rows/DQ cache。S3K进入`VALIDATING`，先执行expanded focused与独立安全审查，
再刷新manifests/compatibility/deprecation/hashes并于整批自然集成边界只跑一次architecture/contract/full。
`stable_full_improvement_claimed=false`、`production_effect=none`。

S3K expanded focused=`93 passed/1 skipped/162.50s`，三轮独立只读审查最终P0/P1/P2=`0/0/0`。
generated manifests=`948/1,126/0 violations`，compatibility/deprecation=`13 passed/13.08s`，pre-full
architecture=`344 passed/59.64s`、contract=`236 passed/44.76s`。唯一自然full=`6,246 passed/
2 skipped/642 warnings/1,014.05s`，exact `6,248 nodes/1,068 files/16 workers`、COMPLETE/applied/
no-fallback、telemetry/performance PASS。相对S3J，wall/file worker-s/P95/P99分别改善
`6.05%/6.13%/21.10%/3.75%`；Formal/Gate/Evaluation full file改善`86.58%/85.09%/
81.61%`。仅一次after full，不声称稳定全局改善。

S3K post-full tracked-state已闭合为`COMPLETE_RUNTIME_TASK_CONTINUES`。首次architecture真实失败为
`343 passed/1 failed/97.51s`，不是module/test manifest漂移，而是full证据补入system flow后aggregate
shadow index尚未刷新；正式generate后的第二次architecture-fitness又由compatibility baseline assertion
捕获旧aggregate hash。统一刷新后
architecture=`344 passed/61.23s`，post-full contract初次复验=`236 passed/45.87s`，最终tracked-state
contract与focused hash证据写入compatibility baseline。没有第二次full；next owner=S3L candidate coordinator，
继续只读审计新profile候选，不解锁EB1、下一callback或ARCH-005，`production_effect=none`。

S3L只读候选审计已闭合并进入`BASELINING`。权威base=`27ce25ae`、profile=`103249Z`；首批三个
互斥lane为Confirmation Evaluate=`247.72s`、Confirmation Progress=`231.84s`、Weight Expanded Search=
`206.94s`。前两条只允许在各自module fixture generator的build→yield→teardown生命周期保持既有同步
PASS-only session，第三条只允许给现有单test function增加outer session；不得改helper、production resolver、
DQ cache、fixture规模、variant count、nodeid或断言。Confirmation output tamper与live-source drift validator
必须逐次真实执行且不进入cache；只允许eligible stable upstream plan复用PASS，plan bytes变化必须使content
fingerprint miss，FAIL/exception不缓存。Expanded Matrix和final Matrix validator继续真实执行。三条先顺序取得
无负载exact-file before，再按两次after的worst值同时满足`>=10%`且`>=30s`节省；失败lane byte-exact撤回。
单lane不跑full，整批保留项只在自然集成边界运行一次full，`production_effect=none`。

S3L无负载isolated before已闭合并进入`IMPLEMENTING`。Evaluate/Progress/Expanded wall分别为
`155.34/167.21/116.88s`，pytest=`153.77/166.68/116.37s`；Confirmation setup=
`42.11/47.75s`，主要call合计=`108.54/115.83s`，Expanded call=`113.25s`。冻结worst-of-2
wall上限=`125.34/137.21/86.88s`，不得事后放宽。三个agent只编辑互斥单test file且不自行跑
Python/pytest；coordinator继续独占共享docs、顺序after/focused和最终集成门禁。

S3L三lane双after均通过冻结门槛并进入`VALIDATING`。Evaluate=`155.34 -> 54.52/53.35s`
（worst `-100.82s/-64.90%`），Progress=`167.21 -> 52.23/52.39s`（`-114.82s/
-68.67%`），Expanded=`116.88 -> 28.92/29.40s`（`-87.48s/-74.85%`）。三文件
node数仍为`5/4/1`，Confirmation setup没有被隐藏，主要重复PASS calls降至约`4.8s`；Confirmation
output/live validators仍逐次真实执行，tamper/drift的FAIL断言与Expanded final Matrix validator均保留。下一步先做expanded focused和交叉安全审查，再刷新
共享manifests/hashes并运行整批唯一自然architecture/contract/full；`production_effect=none`。

S3L expanded focused=`136 passed/2 skipped/239.92s`，两项skip均为Windows缺少`os.fork`的既有条件
用例；三轮交叉只读审查P0/P1/P2=`0/0/0`。覆盖范围同时包含session基础设施、完整Confirmation链与
Weight Matrix→Expanded链；没有缩减nodeid、tamper/drift、DQ或final validator。S3L继续`VALIDATING`，
由coordinator刷新system flow/manifests/compatibility/deprecation/source hashes后进入唯一自然集成门禁。

S3L pre-full compatibility/deprecation focused=`8 passed/13.35s`；architecture-fitness=`344 passed/
59.03s`，artifact=`outputs/validation_runtime/architecture-fitness_20260718T120042Z/test_runtime_summary.json`；
contract-validation=`236 passed/44.09s`，artifact=`outputs/validation_runtime/contract-validation_20260718T120418Z/
test_runtime_summary.json`。集成只读审查发现两个P2文档一致性问题：task-register主行仍停在IMPLEMENTING，且
Confirmation安全机制误写为output/live validator发生cache miss。现已按真实实现修正为output/live validator逐次
真实执行、只有eligible upstream plan PASS按transitive fingerprint复用；下一步刷新受影响hash并在最终tracked
state重跑门禁后执行整批唯一自然full，`production_effect=none`。

S3L最终tracked-state pre-full复验为compatibility/deprecation focused=`8 passed/13.04s`、architecture=
`344 passed/58.10s`、contract=`236 passed/44.68s`。本批唯一自然full=
`6,246 passed/2 skipped/642 warnings/979.11s`，exact `6,248 nodes/1,068 files/16 workers`，collection
ordered/set hashes与S3K相同，scheduler=`COMPLETE/applied/no-fallback`、telemetry/performance=`PASS`。
相对S3K，wall=`-34.94s/-3.45%`、file worker-s=`16,032.72 -> 15,481.94s`（`-3.44%`）、
P95/P99=`-5.51%/-2.62%`；Evaluate/Progress/Expanded file time分别从`247.72/231.84/206.94s`
降至`83.26/78.17/53.15s`（`-66.39%/-66.28%/-74.31%`），node数仍为`5/4/1`。max file
基本持平（`+0.02%`），因此下一批优先治理新profile剩余长尾，不重开已闭合lane。只有一次S3L full，
`stable_full_improvement_claimed=false`；post-full tracked-state exact artifacts与最终状态以compatibility baseline
为准，不运行第二次full，不解锁EB1、下一callback或ARCH-005，`production_effect=none`。

S3M以commit=`ee68b98f`和qualifying profile=`full_20260718T121649Z`进入`BASELINING`。只读审计排除
已闭合或否决的Layer1、Smoothed、Refined transactional、Forward Plan tamper、Execution、Targeted/
Diagnostics、Formal/Gate/Evaluation及S3L三lane后，冻结两条互斥低风险候选：Top Candidate
Interpretation=`120.734s/1 node`，唯一允许编辑`tests/test_weight_top_candidate_interpretation.py`；Candidate
Cluster=`113.515s/1 node`，唯一允许编辑`tests/test_weight_candidate_cluster.py`。两条都只允许给现有单test
function增加同步outer validation session，使helper build与final validator共享既有eligible upstream PASS；public
Interpretation/Cluster final validator、all-view rebuild、lineage、DQ、nodeid与断言必须真实保留，不得改helper、
production resolver、fixture规模或新增supported root。coordinator先在无其他candidate Python/pytest负载下顺序
取得exact-file before，再冻结`worst-of-2 after <= min(0.90 * before, before - 30s)`；不达标或安全审查出现
P0/P1即byte-exact撤回。Refined=`DEFERRED_HIGH_RISK_TRANSACTIONAL_NOT_S3M`；Equal Risk两文件只做并行
只读后续审计，不混入本批实现。单lane不跑full，整批保留项只在自然集成边界运行一次full，
`production_effect=none`。

S3M无负载isolated before已闭合并进入`IMPLEMENTING`。Top Candidate Interpretation=`1 passed / pytest
79.21s / wall 79.90s`，call=`76.02s`；Candidate Cluster=`1 passed / pytest 69.00s / wall 70.68s`，
call=`65.69s`。按`min(0.90 * B, B - 30s)`冻结双after worst wall上限分别为`49.90s`与`40.68s`，
不得以after或full并发抖动事后放宽。两个agent只编辑互斥单test file且不自行运行Python/pytest；coordinator
继续独占共享docs、顺序after、focused和最终集成门禁。

S3M双after已在同一base、显式candidate `PYTHONPATH`且无其他Python/pytest负载下顺序闭合，两条lane均
保留并进入`VALIDATING`。Top Candidate Interpretation=`1 passed / pytest 24.19/23.39s / wall
24.78/23.97s`，call=`20.82/20.14s`，worst相对before下降`55.12s/68.99%`；Candidate Cluster=
`1 passed / pytest 23.41/23.22s / wall 23.97/23.76s`，call=`20.16/19.93s`，worst下降
`46.71s/66.09%`。两文件node数仍各`1`，worst分别低于冻结上限`49.90/40.68s`；下一步只做整批
expanded focused、独立交叉审查和共享门禁，单lane不补跑full。

S3M expanded focused在`-n 16 --dist loadfile`下=`89 passed / 1 skipped / pytest 99.62s / wall
100.17s`；唯一skip为Windows缺少`os.fork`的既有条件用例。coverage包含validation-session基础设施、两条
目标lane以及Weight Matrix/Backfill/Scorecard/Robustness/Adaptive/Expanded/Evaluation/Follow-up链。
两轮互斥lane交叉只读审查均为`P0/P1/P2=0/0/0`。首轮共享门禁已PASS：generated manifests=
`948 modules / 1,126 test-support files / 0 violations`，compatibility/deprecation focused=`8 passed /
14.16s`，architecture-fitness=`344 passed / 60.72s`，contract-validation=`236 passed / 45.66s`。
集成审查P0/P1/P2=`0/0/1`，唯一P2是本段和task register仍把已完成门禁写成下一步；已修正状态，必须
刷新受影响source hashes并复验current tracked state后，才运行整批唯一natural full。

S3M唯一natural full已PASS：`6,246 passed / 2 skipped / 642 warnings / pytest 990.51s / wall
991.64s`，artifact=`outputs/validation_runtime/full_20260718T133435Z/`；exact `6,248 nodes / 1,068
files / 16 workers`与S3L collection ordered/set hashes一致，scheduler=`COMPLETE/applied/no-fallback`、
telemetry/performance=`PASS`。Top Interpretation=`120.734 -> 30.463s`（`-74.77%`），Cluster=
`113.515 -> 32.895s`（`-71.02%`），两文件仍各`1 node`且合计减少`170.89 worker-s/-72.95%`。
但S3L→S3M总体wall=`979.11 -> 991.64s`（`+12.53s/+1.28%`）、file worker-s=
`15,481.94 -> 15,672.45s`（`+1.23%`）、P95/P99/max=`+1.70%/+3.22%/+1.89%`；排除两目标后
其余文件合计回升`361.40 worker-s/+2.37%`，主要Smoothed长尾普遍慢约`6%～7%`，说明局部收益被本次
非目标运行抖动抵消。两lane保留，但`stable_full_improvement_claimed=false`；不跑第二次full，进入post-full
tracked-state manifests/hash/architecture/contract闭合。

S3M post-full第一轮tracked-state已PASS：compatibility/deprecation focused=`8 passed / pytest 13.44s`，
architecture-fitness=`344 passed / 68.71s`，contract-validation=`236 passed / 45.44s`；full artifact内部
完整性、73个active sources、module/test manifests、aggregate bindings与deprecation freshness均无mismatch。
S3M据此进入`COMPLETE_RUNTIME_TASK_CONTINUES`。最终审查修正统一舍入、EB1锁语义与worktree attribution
11/11路径覆盖；归属focused首轮捕获未登记status枚举后，以既有schema status加显式S3M staging authority
完成契约修正，并为该authority补齐逐字段focused断言；修正后focused/architecture/contract=`9/344/236 passed`。exact最终artifacts、74个active
source hashes与final status以compatibility baseline为准，不运行第二次full。下一候选由S3N coordinator基于
`133435Z` profile选择，EB1、下一callback与ARCH-005仍锁定。

S3N在2026-07-19复盘后只登记一次有界收尾批次，权威base=`13d85f1e`、profile=`133435Z`。三个互斥
候选为Weight Adaptive=`116.378s/1 node`、Equal Risk Restart=`251.115s/5 nodes`（首个atomic scope仅
roadmap-v2 node=`208.875s`）和Equal Risk Tilt=`271.338s/5 nodes`。A只允许Adaptive test-function outer
session；B只允许roadmap-v2 test body消除同次suite/CLI已写artifact后的重复工作；C只允许既有test bodies
内复用同node immutable payload，不改文件尾helper/cross-import。所有public validator、至少一次真实CLI、
DQ/AI regime/lineage/research-only及paper-shadow/production/broker安全字段、nodeid与断言必须保留。

coordinator须先顺序取得A exact node、B roadmap-v2 exact node、C whole-file无负载isolated before，再冻结
各lane双after worst `<=min(0.90*B,B-30s)`；冻结前agent不得编辑，之后三个agent只编辑互斥单test file且
不自行跑Python/pytest。未达标或出现P0/P1即byte-exact撤回。整批只在保留lane合并后的自然边界运行一次
architecture/contract/full。三项合计理论50%节省仅约`19.96s` full容量，因此不把单次full自然波动作为
局部lane否决依据。S3N后停止小型wrapper微优化，再决定是否另立Smoothed immutable/COW架构研究；EB1、
下一callback和ARCH-005继续锁定，`production_effect=none`。

S3N无负载isolated before已顺序闭合：A Adaptive=`1 passed / pytest 59.29s / wall 59.889s / call
56.16s`，B Restart roadmap-v2=`1 passed / pytest 127.66s / wall 129.064s / call 122.51s`，C Tilt
whole-file=`5 passed / pytest 147.57s / wall 148.079s`，五call=`57.43/47.19/32.59/5.11/0.16s`。
按`min(0.90*B,B-30s)`预冻结双after worst wall上限为`29.889/99.064/118.079s`，不得事后放宽。
当前进入`IMPLEMENTING`前安全点，三个agent只可编辑各自互斥test file且不自行运行Python/pytest；
after/focused/共享门禁由coordinator顺序执行。

S3N双after已闭合并进入`VALIDATING`，三条lane均保留：A=`17.995/17.903s`，worst相对
`59.889s`为`-41.894s/-69.95%`；B=`78.485/78.410s`，相对`129.064s`为
`-50.579s/-39.19%`；C=`92.541/92.236s`，相对`148.079s`为`-55.538s/-37.51%`。
三条isolated worst合计减少`148.011s`，不将其直接声明为full wall收益。A/B交叉审查均
`P0/P1/P2=0/0/0`；next owner=integration coordinator闭合C审查、expanded focused、tracked
artifacts、architecture/contract与唯一natural full，`production_effect=none`。

S3N expanded focused=`97 passed / 1 skipped / pytest 108.37s / wall 108.890s`，唯一skip为Windows
缺少`os.fork`的既有条件用例。coverage包含三目标、session基础设施与Weight Foundation→
Scorecard→Robustness→Adaptive→Expanded→Evaluation/Follow-up链；三轮独立交叉审查
均`P0/P1/P2=0/0/0`，三目标Ruff/`py_compile` PASS。S3N继续`VALIDATING`，下一步只刷新
shared tracked artifacts、闭合architecture/contract并运行唯一natural full。

S3N pre-full manifests/compatibility/architecture/contract均PASS，本批唯一natural full=
`6,246 passed / 2 skipped / 643 warnings / wall 904.14s`，exact `6,248 nodes / 1,068 files / 16 workers`、
scheduler/telemetry/performance均PASS。三目标full file time=`116.378/251.115/271.338 →
28.473/167.742/117.175s`，合计`-325.442 worker-s/-50.94%`；overall wall/file worker-s相对S3M均
`-8.82%`，P95/P99/max=`-12.12%/-15.68%/-18.29%`。非目标也因运行波动改善`7.03%`，
所以不做稳定全局归因，`stable_full_improvement_claimed=false`。当前=
`POST_FULL_TRACKED_STATE_VALIDATING`，只刷新final docs/authority/manifests/hashes并复验focused/architecture/
contract，不跑第二次full；完成后暂停并向owner提交后续优化方向评估。

S3N第一轮post-full focused/architecture/contract=`9/344/236 passed`，authority转为
`COMPLETE_RUNTIME_TASK_CONTINUES`。协调者将刷新最终docs/manifests/hashes并对最终tracked state
复验同三门；随后暂停，不自动选择新微优化、Smoothed COW架构或其他ARCH-004 slice。
EB1、下一callback与ARCH-005继续锁定，`production_effect=none`。

S3N final tracked-state focused/architecture/contract=`13/344/236 passed`，architecture/contract artifacts=
`architecture-fitness_20260718T163403Z`/`contract-validation_20260718T163513Z`；77 active sources、12/12 changed/
declared tracked paths、3 excluded user docs未改，deprecation inventory fresh。S3N完成为
`COMPLETE_RUNTIME_TASK_CONTINUES`，next owner=project owner/performance-review coordinator。按owner要求现在暂停，
以本轮数据决定是否实施高风险Smoothed immutable/COW架构研究、仅做S4 provenance，
或返回ARCH-004主路径；不自动启动任一选项。

2026-07-19 owner 已选择方案 A，具名授权 `S4_FULL_TRIGGER_PROVENANCE`，base=`2962e02f`。该子任务
在任何 Full 启动前 fail-fast 校验 reviewed trigger/task/boundary（failure rerun另需parent），并把同一
`validation_trigger_provenance.v1` 写入summary/profile/Reader Brief。CLI与environment按whole envelope互斥，
failure rerun必须重验并绑定此前失败formal Full summary/profile的path/hash，profile binding PASS才可形成performance evidence；
保留独立developer CI daily scheduled Full（不是研究cadence），push/PR仍走fast-unit，manual默认避免无意
Full且不暴露clean checkout无法自证的failure rerun。S4只跑focused/architecture/contract，不为自身制造Full；
closeout提交推送后返回G2.4主线协调点。此授权不是phase解锁：EB1、下一callback、ARCH-005/G2.5仍
受原gate约束，`next_phase_or_slice_unblocked=false`、`production_effect=none`。

S4已按该合同闭合：expanded-focused/pre-final architecture/contract=`136/362/254 passed`，首轮architecture对新增module的
deprecation/test-manifest freshness漂移fail closed并在刷新后复验PASS；post-doc final tracked-state artifact/hash
不在活跃文档中递归固化，统一由self-excluded `arch_004_compatibility_baseline.yaml` 的S4 validation节点绑定。
24/24 attribution paths、83 active sources与generated manifests均fresh，Full run count=`0`。状态=`COMPLETE_RUNTIME_TASK_CONTINUES`，当前
返回G2.4协调点；这不是EB1或下一callback授权。

## EB0：最高长尾限时治理

时间预算：1～3 个连续推进日。它不是完成整个 runtime-budget 任务的授权。

范围只包含：

- 当前约 `17,801.26s` 的 smoothed/immutable-fixture 主长尾及其直接共享上游；
- 同一 node 内重复 validator replay 的 content-fingerprint reuse；
- immutable fixture 与 tamper copy-on-write 的最小闭环；
- active node/worker heartbeat、peak-memory 与 read-amplification 观测；
- 必要的 duration/peak-memory heavy-shard 上限，但不盲目增加 worker。

EB0 不批量迁移 28 个无关 legacy/no-scope cache 调用。超过 timebox 后，即使长期 S2/S3 验收尚未
完成，也应保留明确债务并进入 EB1；只有 required gate 无法可靠完成时，runtime 问题才继续阻塞
G2.4。EB0 退出需要同 node、同命令、同机器的 before/after evidence，以及 focused、architecture、
contract、full PASS；不得用不同 nodeid 集合计算改善比例。

## 剩余 callback batches

下表以当前 matrix 的 `(app_name, command_name)` 为 source of truth。每个 batch 启动时必须生成
callback-level freeze 清单并复核计数；若 matrix 因前一 batch 改变，允许迁移状态变化，不允许把
callback 静默换 batch、漏记或重复记账。

|Batch|范围|Pending callbacks|主要 app families|
|---|---|---:|---|
|EB1|Signal failure / ledger / churn / regime / filter design|15|`dynamic_v3_signal_failure_taxonomy_app`、`dynamic_v3_candidate_signal_ledger_app`、`dynamic_v3_signal_churn_root_cause_app`、`dynamic_v3_regime_mismatch_attribution_app`、`dynamic_v3_candidate_quality_filter_design_app`及5个匹配validator|
|EB2|Filtered backfill / comparison / gate / promotion / owner roadmap|15|`dynamic_v3_filtered_candidate_backfill_app`、`dynamic_v3_filtered_vs_original_comparison_app`、`dynamic_v3_signal_gate_experiment_app`、`dynamic_v3_filtered_candidate_promotion_review_app`、`dynamic_v3_owner_signal_roadmap_app`及5个匹配validator|
|EB3|Filtered evidence expansion and formalization readiness|30|`dynamic_v3_filtered_candidate_evidence_app`、`dynamic_v3_median_regime_filter_spec_app`、`dynamic_v3_filtered_candidate_stress_backfill_app`、`dynamic_v3_drawdown_mismatch_reduction_app`、`dynamic_v3_flip_rotation_reduction_app`、`dynamic_v3_filtered_candidate_ab_review_app`、`dynamic_v3_signal_gate_confirmation_app`、`dynamic_v3_filtered_formalization_readiness_app`、`dynamic_v3_owner_filtered_candidate_review_app`、`dynamic_v3_filtered_next_decision_app`及10个匹配validator|
|EB4|Evidence materialization, calibration and signal-input readiness|39|benchmark baseline control/metrics、candidate decision ledger/regression replay、cost sensitivity metrics/review、drawdown/flip casebooks、formal research method contract、metric source map、promotion threshold calibration、signal input completeness/recovery及匹配validator|
|EB5|Paper-shadow protocol, health, recovery and shadow decisions|37|evidence staleness、normal resumption、paper-shadow daily/drift/health/outcome/protocol/weekly、position review、readiness recovery、shadow continuation/decision、stress scenario及匹配validator|
|EB6|Weight calibration and weight research|40|`weight_calibration_app` 20、`weight_research_app` 20|
|EB7|P2, experiments, satellite, dynamic shadow and simulation residuals|40|`p2_app` 18、`experiments_app` 7、`satellite_app` 6、`dynamic_shadow_app` 6、`simulation_app` 3|
|EB8|AI, decision, forward and general ETF residuals|36|AI attribution/confirmation、decision journal、forward、backtest、governance及剩余 attribution/confirmation/credibility/ETF/events/portfolio/regime/relative-strength/report/run/signals apps|
|合计|||`252`|

EB1～EB3 保持当前 CX3 上下游语义连续性；EB4～EB5 优先按共享 evidence/fixture DAG 聚合；EB6
单独隔离投资敏感的 weight surfaces；EB7～EB8 收口其余 bounded contexts。任何 batch 若审计发现
必须拆分，拆分后的两个子批次仍共享一个 integration boundary 和一次正式 full，除非失败隔离证据
证明必须独立 gate。

## 单个 batch 的固定生命周期

1. **Freeze**：从 matrix 导出 callback-level 清单、base commit、owned/shared paths、上游/下游依赖、
   required fixtures 与测试 nodeids。
2. **Migrate**：producer、report、validator、domain public entrypoint 与 legacy subtraction 同批完成；
   不复制实现，不新增 god module。
3. **Focused validation**：使用并行 pytest；覆盖正向、missing/null、lineage、chronology、source/policy/
   output tamper 与 CLI parity。
4. **Integration refresh**：一次性刷新 matrix、CLI contract、module/test manifests、compatibility baseline、
   deprecation inventory、reporting inventory、source hashes和必要文档。
5. **Formal gates**：architecture、contract、full parallel；full 只在 integration boundary 运行，失败先诊断。
6. **Closeout**：确认 attributable worktree，提交并推送；记录 callback delta、验证 artifacts、runtime、
   long-tail、production effect 和下一 batch 是否解锁。

## Runtime-budget 与迁移主线的关系

S2/S3 不再作为一个必须全部完成后才能恢复 G2.4 的前置大任务：

- EB0 只治理会在未来 8 个 batch 中重复付费的最高长尾；
- EB1～EB8 只在触及相同 DAG 时顺手迁移 scope/reuse，不横向扩张到无关调用；
- 28 个 legacy/no-scope 调用按所属 batch 渐进迁移，未触及部分继续明确留债；
- 连续 3 次 full、P95、peak-memory 和 read-amplification 的完整性能验收只对稳定 callback 集合执行；
- 若 required gates 全部可靠 PASS，但长期性能目标尚未满足，runtime-budget 可以保持
  `IN_PROGRESS`，不得据此伪造 G2.4 callback completion，也不得无限阻塞 callback 主线。

## Phase exit 与停止条件

EB8 完成不等于 G2.4 自动完成。只有以下条件全部满足才可进入 phase exit：

- matrix=`967 migrated / 0 pending / 0 unresolved / 0 duplicate`；
- CLI保持993 leaves、0 duplicate，公开path/options/default/help/exit parity通过；
- required focused/architecture/contract/full PASS；
- module/test manifests、compatibility baseline、deprecation inventory、reporting inventory及source hashes
  fresh且验证通过；
- worktree attribution、shared-path owner/lease count和unrelated files可审计；
- `arch_005_bootstrap_handoff.v1`生成、验证、提交并推送，且
  `next_slice_unblocked=false`、`production_effect=none`。

完成 handoff 后 ARCH-004 必须停止在 G2.5 之前。ARCH-005 S0/S1 只能从 handoff commit 启动；其完成
后仍需新的显式 owner 指令才能恢复 ARCH-004。

## 预计工作量与复盘点

- EB0：1～3天；
- EB1～EB8：预计每批0.5～1.5天，复杂或失败诊断批次允许延长；
- phase closeout/handoff：1～2天；
- 当前 Goal 的基准预期：8～14个连续推进日。

在 EB0、EB3、EB5、EB8 后执行四次计划复盘。复盘只允许根据实际 callback delta、同口径 runtime、
失败/返工和 shared-path 冲突调整后续 batch 尺寸，不允许降低正确性或审计门禁来追赶日期。

## 延后且不阻塞本计划的工作

- 真实 dated signal ledger、forward/PIT/holdout evidence 的时间积累与 owner研究复核；
- 与当前 batch 无关的全部 legacy cache scope 迁移；
- DATA-GOV-001、STORAGE-001、KNOWLEDGE-001、PUBLISHING-001、PLATFORM-UX-001；
- ARCH-005 实现、G2.5/G3～G7、ARCH-004H cutover；
- 新研究功能、策略阈值调优、promotion、official target或production接入。

## 状态记录

- 2026-07-17：owner复核EB0 formal full结果后，批准在EB1前插入有界S3A尾部优化。pre-change
  full=`6,195 passed / 2 skipped / 642 warnings / 2,138.84s`；94%约在9分钟完成，最后6%持续
  到35分38秒，期间约13～14.5核持续活跃、working/private峰值约`9.41/21.65GiB`且可用内存
  约`57.9GiB`。S3A第一优先只治理Weight Search直接链的单node重复DAG，Research Foundation
  setup排第二；不改变nodeids/tamper/fail-closed、`-n16 --dist loadfile`正式门禁、callback matrix、
  runtime/投资语义或production边界。S3A完成前EB1保持未启动。
- 2026-07-17：EB0 clean candidate正式focused=`274 passed / 1 skipped / 248.21s`，在新增
  DevEx checkout-hash测试后仍略快于首轮`252.60s`；最大热点为smoothed freshness tamper
  `204.52s`，属于单文件loadfile尾部偏斜而非全局性能回退。正式architecture首轮=
  `310 passed / 1 failed / 62.70s`，唯一失败是deprecation inventory仍按原始工作区字节
  计算Python source的`byte_count/source_sha256`，使CRLF main与LF clean checkout漂移；修复为
  UTF-8 universal-newline LF规范化，并增加LF/CRLF等价测试，业务source内容、deprecation
  生命周期和removal gate均未改变。刷新inventory/manifests/hashes后architecture=
  `312 passed / 58.22s`、contract=`204 passed / 44.22s`，均在既有预算内；formal full仍
  `PENDING`，因此EB0保持`IN_PROGRESS`、EB1未启动，`production_effect=none`。
- 2026-07-17：EB0-S2C clean-checkout follow-up 首轮focused=`258 passed / 1 skipped /
  1 failed / 252.60s`。唯一失败为历史 `execution_cli_date_adapters_after_arch_004a1`
  frozen SHA记录了CRLF工作区字节，而`.gitattributes`明确`eol=lf`、clean candidate为LF；两边
  `git hash-object`完全相同。第二次baseline单节点`2.35s`继续暴露mixed-line-ending hash；严格以
  “旧SHA=main原始字节、main/candidate LF规范化内容完全相同”为条件，共识别6个current entries。
  根因扩展到DevEx `_sha256`按工作区原始字节生成manifest。耐久修复固定为：仅对仓库已声明LF的
  architecture text suffix做canonical LF SHA，6项baseline保留previous worktree SHA与
  `hash_normalization=git_eol_lf`；补LF/CRLF/mixed等价及binary不归一化测试。不得修改对应业务
  源文件、CLI/runtime/研究语义；两轮FAIL均不算PASS，须刷新manifests/source hashes后重跑。
- 2026-07-17：EB0-S2D～S2F focused 与分片诊断收口，但 formal full 仍为PENDING。
  Confirmation Targets=`10 passed / 85.37s`，相对同命令`665s`仍未结束的基线下界至少
  缩短`87.2%`；Advisory Proposal Review=`13 passed / 119.35s`，相对隔离worker约7.5分钟
  缩短约`73.5%`；Forward Plan+Rule Review=`22 passed / 209.97s`，相对约10/5分钟分别约
  缩短`65%/70%`；四文件合并focused=`45 passed / 235.97s`。排除历史top-45 heavy及由正式
  architecture gate覆盖的`test_arch_004*`后，四个correctness分片分别为
  `1,341/1,271+1 skip/1,718/1,452 passed`，JUnit wall=`246.411/371.109/313.790/249.942s`。
  历史top-45 heavy专项在`1,253s`预算停止，不算PASS；停止时剩6个Weight Search链文件，
  运行中峰值观测为16 active workers、约`6.33GiB` working set、`17.84GiB` private、系统仍有
  约`62GiB`可用内存。性能风险是duration/loadfile偏斜而非内存不足，转S3 debt；不得横向扩张
  EB0范围。候选树测试产生的默认输出副作用必须通过重建clean candidate隔离，不进入提交。
- 2026-07-17：EB0-S2F 根据第2隔离分片的worker临时目录，将最后两个直接Confirmation
  heavy shards限定为 `tests/test_forward_confirmation_plan.py`（约10分钟）和
  `tests/test_rule_review_cycle.py`（约5分钟、单worker working set约1.76GiB）。Forward Plan按
  S2E共享module immutable fixture并为plan output/bridge/proposal source tamper逐项byte restore；
  Rule Review已有module fixture，只把validation session生命周期从builder调用延长到module teardown。
  两文件nodeids、真实validator、source/policy/output tamper和`loadfile`串行边界保持不变，
  `production_effect=none`。
- 2026-07-17：EB0-S2E 将同根直接上游长尾限定到 `tests/test_advisory_proposal_review.py`：
  隔离分片的pytest worker临时目录显示该文件12个节点约耗时7.5分钟，并为每个positive/negative/
  tamper节点重复构建同一Advisory Proposal Review DAG。复用边界与S2D相同：module/worker内一次
  immutable upstream+review和一个validation session；output、risk-return source、calibration source
  tamper必须由function fixture在`finally`中byte-exact restore，policy test仍使用独立输出。
  不扩张到同分片其余多文件研究计算，`production_effect=none`。
- 2026-07-17：EB0-S2D 在隔离候选树的 full 失败诊断中发现新的直接 Confirmation 长尾：
  `tests/test_confirmation_targets.py` 在 `-n16 --dist loadfile` 下运行约 `11m05s` 后仍有一个
  CPU-bound worker，16-worker 进程树按诊断预算停止；pytest 临时目录显示该文件对同一
  `Forward Confirmation Plan` 上游按 test/parameter 重建并递归验证。该文件属于 EB0 已批准的
  smoothed Confirmation 直接共享上游。S2D 仅允许在单个 test module/worker 内构建一次 immutable
  上游和plan，并让 PASS-only content-fingerprint session 覆盖模块；output/source/materialized tamper
  必须继续逐项 fail closed，collected nodeids 与生产实现不变。首个“只复制plan边界”的实现因plan
  内部绝对self-path commitment被validator正确拒绝（`8 failed / 2 passed / 157.95s`），不得采用。
  两个必须修改plan的节点改用专用fixture原地tamper并在`finally`中byte-exact restore；同文件由
  `loadfile`串行执行，tamper期间content fingerprint必须失效，恢复后才允许复用历史PASS。
  完成后以同文件同命令 before/after、focused 与正式 gates 验证，`production_effect=none`。
- 2026-07-17：EB0-S2C 正式 architecture gate 首轮在 `55.60s` 内完成，未出现新的性能长尾，
  但暴露两个隔离验证可复现性缺口：generated module/test/aggregate manifests 需要随本批 source/test/doc
  变更刷新；CLI frozen tree 把 checkout 绝对根目录写入 `Path` 默认值，导致同一提交从临时 worktree
  验证时有 `987` 个 command node 仅因根目录不同而改变 hash。该问题不改变 CLI runtime 参数或
  投资语义，但会阻断无污染候选树验证并迫使共享工作区串行化。EB0 因此增加一个有界 S2C：仅在
  architecture contract 生成时把位于 `project_root` 下的绝对 `Path` 默认值规范化为稳定的
  project-relative token，补双 checkout 等价测试并重建 frozen CLI contract/manifests；不得改变实际
  callback defaults、CLI surface、数据门禁或 production 行为。完成后重新执行正式 architecture、
  contract 与 full gate；首轮 FAIL 不视为通过，`production_effect=none`。
- 2026-07-17：EB0 第一项根因修复与 focused gate 完成，formal architecture/contract/full 仍待执行，
  因而 EB0 保持 `IN_PROGRESS`、EB1 仍未启动。根因不是 hang 或机器内存不足，而是 legacy
  compatibility fingerprint 的 JSON node 安全上限为 `100,000`；真实
  `sideways_mixed_attribution_input_snapshot.json` 为 `9.09 MiB / 226,197 nodes / 45 bound paths`，
  每次在读入和解析后都转为 uncacheable，再回退整条 validator DAG，造成约 `64 MiB` artifact
  在单节点运行24分钟时累计读取 `171.05 GiB`。修复把 node cap 提高到 `500,000`，同时保留
  `64 MiB` document、`4,096` bound paths、aggregate path shape、link/topology、before/after
  fingerprint与PASS-only等独立安全门禁；新增高节点可缓存和超界仍bypass两类测试，并让最高
  hardening节点的producer、retry与tamper循环共享同一validation session。
  同节点同命令基线在`947.08s`仍未完成并按诊断timebox停止；修复后=`1 passed / 172.23s`、
  call=`168.66s`，相对基线下界至少缩短`81.8%`，且所有output/source tamper仍fail closed。
  6个历史最重节点并发=`6 passed / 446.15s`；15个smoothed高扇入文件扩大focused=
  `27 passed / 498.56s`，最慢=`493.29s`。修复后单节点在约2分20秒抽样累计读取约`1.41 GiB`
  并于2分52秒完成；该抽样不冒充完整read总量或稳定full结论，`production_effect=none`。
- 2026-07-17：owner 指示继续计划任务，并要求对异常长耗时及时验证性能风险。EB0 正式启动，
  冻结首个诊断对象为
  `tests/test_smoothed_freshness_hardening.py::test_retry_rebuilds_views_and_rejects_tampered_validated_weekly_child`；
  历史 formal full 同节点为 `17,801.26s`，同一 smoothed DAG 另有多节点达到约
  `1,743.04～13,604.76s`，已判定为工程性能风险而不是普通测试波动。先执行有界同节点基线、
  调用链和资源观测，再实现 PASS-only fingerprint reuse / immutable fixture / copy-on-write 的最小闭环；
  不扩张到无关 cache scopes，`production_effect=none`。
- 2026-07-17：owner 从推进效率角度批准调整后续开发计划。建立 EB0 + EB1～EB8 的关键路径，
  把252个pending callbacks按共享语义/fixture边界完整分配为15/15/30/39/37/40/40/36，固定
  batch-level formal gate、最终稳定集合性能验收和原ARCH-005 handoff停止条件。本次只调整计划，
  不启动EB0/EB1，不改变runtime、研究结论或production状态。
