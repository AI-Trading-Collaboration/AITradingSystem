# ARCH-004G2 Validation Runtime Budget 与 Immutable Fixture Reuse

最后更新：2026-07-17

## 任务信息

- task id：`ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE`
- priority：`P1`
- status：`IN_PROGRESS`
- owner：architecture coordinator / test infrastructure owner
- dependency：G2.4 原子切片不得被打断；优先随正在迁移的 chain 分批治理
- production effect：`none`

## 问题与证据

G2.4CR/CS 已证明主要耗时不在投资计算，而在高扇入 artifact DAG 对同一 immutable source
反复做 live validation、byte rebuild 与大 snapshot 读取：

- EB0-S2D～S2F最终focused：Targets=`10/85.37s`、Advisory=`13/119.35s`、Forward+Rule=
  `22/209.97s`、四文件合并=`45 passed/235.97s`；同口径基线下界/隔离worker观测对应局部
  改善至少`87.2%/73.5%/65%/70%`。四个非heavy correctness分片共覆盖`5,782 passed +
  1 skipped`且0 failure；top-45 heavy在`1,253s`预算停止，不是PASS。停止时6个active文件均为
  Weight Search共享DAG，16-worker峰值工作集/private约`6.33/17.84GiB`且可用内存约`62GiB`；
  因此S3应治理duration-aware heavy shard，而不是增加worker或扩张本轮fixture范围。
- clean candidate正式focused首轮=`258 passed / 1 skipped / 1 failed / 252.60s`；唯一失败是
  frozen baseline记录CRLF原始字节、仓库规范与clean checkout均为LF造成的SHA差异。第二次
  baseline单节点`2.35s`确认还有mixed line ending。严格main/candidate逻辑内容等价审计共6项；
  DevEx hash helper须对受`.gitattributes eol=lf`约束的architecture text canonicalize，并以
  LF/CRLF/mixed等价、binary raw-byte差异测试约束。baseline保留previous worktree SHA；不得改
  业务源文件。两轮均不算PASS，修复后必须同命令重跑。
- 修复后的正式focused=`274 passed / 1 skipped / 248.21s`，新增DevEx EOL约束后仍比首轮
  快约`1.7%`；最慢节点为smoothed freshness tamper=`204.52s`，证明剩余风险集中在单文件
  loadfile尾部而非整体回退。正式architecture首轮=`310 passed / 1 failed / 62.70s`，进一步
  暴露deprecation inventory的Python source byte/hash仍依赖CRLF/LF工作区；切换为UTF-8
  universal-newline LF后，architecture=`312 passed / 58.22s`、contract=`204 passed /
  44.22s`。生成manifests连续耗时`14.40～16.17s`，无异常增长；formal full仍待执行。
- EB0-S2D 隔离候选树诊断把 full 划为文件分片后，`tests/test_confirmation_targets.py` 在
  `11m05s` 预算停止时仍有一个满核 worker；pytest worker 临时目录证明同一文件内的
  positive、duplicate、4类 output tamper、live-plan drift 与materialized drift节点分别重建
  `run_forward_confirmation_plan_fixture` 的完整上游。治理边界固定为module/worker内一次
  immutable upstream+plan与一个PASS-only validation session；registry输出继续per-test隔离。
  首个per-test plan copy因绝对self-path commitment正确FAIL，已否决；两个plan tamper节点只允许
  在`loadfile`串行边界内原地修改并由fixture `finally` byte-exact restore，tamper指纹必须触发真实
  revalidation。不得减少tamper参数或把validator替换成stub。
- EB0-S2E 同一分片的 `tests/test_advisory_proposal_review.py` 约占7.5分钟，12个节点重复构建
  Confirmation Plan直接上游。仅按S2D模式共享module upstream+review；三类可变边界
  （review output、risk-return source、calibration source）分别由function fixture snapshot/restore，
  不共享tampered bytes，不改变真实validator或生产实现。
- EB0-S2F 第2分片最后两个heavy shards为Forward Plan约10分钟及Rule Review约5分钟；后者
  已构建一次module fixture但session过早关闭。治理仅复用同worker immutable DAG，并让现有
  byte-restore tamper测试在同一个module session内按内容指纹失效/恢复；峰值约1.76GiB记入S3
  duration+memory shard证据，不以增加worker掩盖偏斜。
- G2.4CR 单个 progress 测试从 `557.27s` 降至 `13.60s`，最大 snapshot 从
  `633.06MB` 降至 `9.49MB`；
- G2.4CS smoothed regression 从 `270.04s` 降至 `100.98s`，readiness chain 从
  `245.98s` 降至 `86.34s`；
- full baseline `full_20260714T024455Z` 为 `5,977 passed / 1,534.77s`，当前
  `full_20260714T173208Z` 为 `6,012 passed / 2,554.80s`。测试只增加 35 个，但固定
  `-n 16 --dist loadfile` 下长尾明显放大；
- 当前 full 收尾时只剩一个 CPU-bound worker，其他 15 个 idle worker 仍保留数百 MB；诊断观察
  到 active worker 约 97% CPU、约 `159 MB/s` read I/O、约 `548k page faults/s`，累计读取超过
  `231 GB`。这是重复读/重验和内存换页抖动，不是 coverage 本身不可避免的成本。
- G2.4CT final full 为 `6,018 passed / 2,514.64s`，相对 `2,554.80s` 仅改善约 `1.57%`，
  不足以声明稳定提速；最慢模块仍为 confirmation weekly `1,220.52s`、rule review queue
  `704.54s`、confirmation dashboard `615.90s`。这进一步确认 S2 content-fingerprint DAG去重
  应先于 S3 duration-aware shard，而不是单纯增加worker。
- G2.4CU已把5族下游validator接入同一validation session：单次Readiness复验会复用
  Refresh/Post/Resume/Growth嵌套链中的PASS-only content fingerprints；123个focused CLI/业务/
  hardening tests通过。该局部优化避免新链继续放大重复校验，但没有声称解决旧confirmation长尾。
- G2.4CU final full=`6,023 passed / 1,939.34s`，相对G2.4CT的`2,514.64s`单次缩短约
  `22.9%`，但尚未满足连续3次稳定性要求，不能归因为已完成的系统性优化。最新长尾为
  confirmation weekly=`1,229.71s`、confirmation dashboard=`824.66s`、rule review queue=
  `726.05s`；99%阶段仅少数worker继续计算，健康检查还观察到单worker working set最高约
  `3.7GB`。因此调度必须同时考虑duration和peak memory，固定`-n16`不是默认最优假设。
- G2.4CV1 final full=`6,025 passed / 2,501.39s`，相对G2.4CU反而增加`562.05s`
  （约`+29.0%`），明确否定“上轮22.9%已形成稳定系统性提速”。稳定前三长尾仍是
  confirmation weekly=`1,230.03s`、rule review queue=`712.22s`、confirmation dashboard=
  `603.42s`；新增weight-search foundation hardening不在slowest 50。99%阶段持续约十余分钟，
  但worker CPU持续增长；抽样最高working set约`2.0GB`，另有多个worker约`1.6～1.9GB`。
  因此根因仍是既有confirmation DAG重复重建、loadfile shard尾部放大与内存竞争，而不是CV1
  coverage本身。性能验收必须使用同机连续多次median/P95、最大shard、peak memory和read
  amplification，不能使用单次最好值。
- G2.4CV2 final full=`6,027 passed / 1,542.60s`，相对CV1单次缩短`958.79s`
  （约`-38.33%`），但CV1/CU/CV2三次分别为`2,501.39/1,939.34/1,542.60s`，变异仍大，
  不能把CV2快样本解释为系统性优化完成。本轮前三长尾为confirmation weekly=`975.52s`、
  confirmation dashboard=`628.06s`、rule review queue=`495.38s`；CV2 evaluation hardening=
  `164.97s`、expanded search=`130.21s`，新链已进入slowest 50但不是前三主瓶颈。72%阶段抽样
  单worker working set最高约`2.69GiB`，99%阶段18个Python进程合计约`12.23GiB`，且runner
  无active nodeid/elapsed/ETA心跳。S2因此增加Weight Search Evaluation immutable fixture DAG复用，
  S3必须限制memory-heavy shards并发，不能只按文件数平均或盲目增加worker。
- G2.4CV3现有六个业务测试在`-n16 --dist loadfile`下为`6 passed / 230.02s`；每个文件
  各自从Search上游重建一次完整fixture DAG。新增hardening改为单个module fixture只建链一次，覆盖
  27个materialized-view drift、6个snapshot schema drift与3个cross-lineage drift，但其六个baseline
  validator仍递归重复重放同一source链，实测`1 passed / 337.68s`。这说明fixture共享已减少重复producer，
  下一瓶颈是同一次validation run内的PASS-only validator重放去重，而不是继续增加worker。
- G2.4CV3 final full=`6,029 passed / 1,592.38s`，相对CV2的`1,542.60s`单次增加
  `49.78s`（约`+3.23%`），继续证明单次运行不能支持稳定提速结论。前三长尾仍为confirmation
  weekly=`977.42s`、dashboard=`635.07s`、rule queue=`505.32s`；CV3 decision hardening=
  `414.50s`已升至第4，Owner Pack/Dashboard/Formal Plan分别=`292.58/219.74/178.31s`。
  99%阶段抽样18个Python进程working set峰值约`15.09GiB`、private约`27.61GiB`，最大单worker
  working set约`3.29GiB`；尾段working set随后回落约`9.9GiB`。因此Decision链同run fingerprint
  复用和peak-memory-aware并发上限都是S2/S3必需项。
- G2.4CW1 final full=`6,032 passed / 1,773.27s`，相对CV3单次增加约`11.30%`；前三长尾
  confirmation weekly/dashboard/rule queue=`984.85/674.58/574.26s`，Diagnostics hardening=
  `279.65s`、第8。该结果继续否定以单次最好值宣称稳定改善。
- G2.4CW2 focused=`132 passed / 413.38s`；四个业务文件各自重建同一上游链，Matrix/Backfill/
  A/B/Hardening=`168.63/235.69/408.97/340.11s`。Hardening 已做到单文件内只建一次 immutable
  fixture，再用原地 tamper+restore 覆盖16 views/3 schemas/3 cross-lineage/policy/cache/resume，
  但 deep validator 仍重复 replay source DAG。Final full=`6,035 passed / 1,722.89s`，相对CW1
  单次缩短约`2.84%`，不构成稳定改善；前三长尾=`1,081.82/709.36/617.40s`，A/B=
  `519.88s`第4、Targeted hardening=`415.81s`第12。99%长尾期间16 workers均持续消耗CPU，
  抽样working set合计约`14.39GiB`、单worker最高约`1.99GiB`。因此下一工程优化应是：
  （1）为Targeted/后续CW3链引入同run PASS-only content-addressed validation session；
  （2）把完整 immutable Search→Diagnostics→Targeted fixture持久化为内容寻址基线，tamper走
  copy-on-write；（3）S3按duration+peak-memory限制heavy shard并发。不得用关闭live source/DQ/
  byte rebuild或减少collected nodeids换取速度。
- G2.4CX1将四个各自构造完整evidence DAG的旧测试文件合并为一个module-scoped immutable
  fixture，并在同一`artifact_validation_session`中复用PASS-only content fingerprints。旧四测试
  `4 passed / 555.74s`；新测试以同一业务语义覆盖4条producer/validator路径、4类输出篡改、
  live source tamper和policy tamper，共`10 passed / 216.42s`，墙钟降低`61.05%`、约`2.57x`。
  测试专用policy把targeted variants缩到6～12以降低fixture成本，但仍强制全部6个必需family；
  production policy仍为60～120，schema/family gates在早期压缩过度时如预期fail closed，未被放宽。
- CX1首轮full=`6,044 passed / 6 failed / 2,962.64s`，六项失败全部来自尚未迁移的CX2
  `micro_search_v4_backfill`链：CX1为修复chronology把下游fixture时间推进到2026，但旧CX2仍将
  只到2024-02-29的历史cache作为2026-03-25“当前质量cache”，required DQ gate因
  `prices_stale/rates_stale`正确FAIL。修复没有放宽DQ或改生产代码，而是为micro-backfill生成独立
  current-quality cache，只追加4个标的+1个利率的2026-03-25 freshness rows；原历史cache及其
  live-binding SHA保持不变，计算仍裁剪到请求的2022-12-01～2024-02-29窗口。DQ诊断为
  `PASS_WITH_WARNINGS`（仅既有secondary source/download manifest warning），原`-n16 --dist
  loadfile`六失败文件复验=`6 passed / 739.19s`。六文件仍各自重建同一DAG，说明CX2后续也应采用
  CX1的module/session fixture模式。
- CX1 final full=`6,050 passed / 642 warnings / 3,298.22s`，artifact=
  `outputs/validation_runtime/full_20260715T145342Z/test_runtime_summary.json`。它比首轮失败运行
  `2,962.64s`慢`335.58s`（约`+11.33%`），所以只承认CX1 focused的2.57x局部收益，不宣称
  full稳定改善。top tail为confirmation weekly=`1,353.68s`；其次follow-up hardening=
  `1,007.24s`、smoothed freshness hardening=`916.83s`。CX2六条业务链分别约
  `726.98~900.41s`且共享同一micro-search DAG；99%阶段最终只剩单worker运行，确认最大shard
  而非worker总数决定墙钟。工程优先级保持S2 immutable fixture/PASS-only content fingerprint/
  copy-on-write tamper，再做S3 duration+peak-memory sharding与active-node heartbeat；不得减少门禁。

## 设计原则

1. 不减少 required focused/architecture/contract/full gate，不用 `-k` 排除慢测试冒充 PASS。
2. 只在同一 validation session 内复用 `PASS`；key 至少包含 validator identity、artifact 绝对
   路径、完整内容指纹与会改变语义的参数。任一 byte/source/config/policy 变化必须失效。
3. Snapshot 只冻结 consumer 实际读取的 bounded business views/commitments；live validator
   仍重放完整权威 source，不递归复制无关 input snapshot 正文。
4. Immutable module fixture 可复用；tamper、drift、future、duplicate、cross-lineage 场景必须拥有
   隔离副本，测试不得相互覆盖同一 cache/source path。
5. Worker 数和分布策略由历史 duration、可用内存与峰值 working-set 预算推导；不能假设 worker
   越多越快。调整只改变调度，不改变 collected nodeids、assertions 或 exit gate。

## 与 G2.4 剩余批次的执行顺序

2026-07-17 owner 批准把本任务从“整包完成后才恢复callback迁移”调整为 critical-path companion。
详细计划见 `docs/requirements/ARCH-004G2_Remaining_Phase_Efficiency_Execution_Plan.md`：

1. 先执行EB0 1～3日timebox，只治理当前约`17,801.26s`最高长尾、其直接共享DAG、最小
   immutable fixture/copy-on-write和运行观测；
2. EB0通过正式focused/architecture/contract/full后进入EB1，不等待28个无关legacy/no-scope
   调用整包迁移；
3. EB1～EB8触及相同DAG时渐进迁移scope/reuse，禁止以性能任务横向扩张batch范围；
4. callback集合稳定后再执行连续3次full、P95、peak-memory、read-amplification和S3完整验收；
5. required gate无法可靠完成时性能问题才阻塞G2.4；长期目标未满足但required gates可靠PASS时，
   本任务保持`IN_PROGRESS`并诚实留债，不得无限阻塞callback matrix。

该顺序不降低本任务验收标准，也不把不同命令、nodeids或资源条件的样本拼成改善比例。

## 分阶段实施

### S1：Runtime telemetry 与预算

- 在 runtime artifact 增加 collected/passed nodeids、worker 分布、P50/P95/max duration、峰值
  working set、累计 read bytes/page faults（平台可用时）及历史基线差异；
- 对超过预算的active node周期输出nodeid、elapsed、worker与resource heartbeat，区分正常长测、
  资源争用和疑似挂起，避免只显示长期不变的suite百分比；
- 定义 full gate 的 wall-time、peak-memory、read-amplification warning budget；超预算产生显式
  warning/治理任务，不静默降级 gate。

### S2：Confirmation chain 去重

- 先治理当前长尾 `confirmation_cycle_weekly`、`rule_review_queue`、
  `confirmation_dashboard`、`rule_owner_decision`；
- 把 `weight_search_evaluation_hardening` / `weight_expanded_search` 接入同一不可变
  Search→Matrix→Backfill fixture DAG，正常链共享，tamper 分支copy-on-write；
- 把 `weight_search_decision_hardening` 的Cluster→Interpretation→Gate→Plan与
  Scorecard+Adaptive→Dashboard→Owner接入显式validation context；key必须包含validator、artifact
  absolute path、全view/source fingerprint与语义参数，同一run仅缓存PASS，tamper后必须立即失效；
- 接入 content-fingerprint validation session，并把 source commitments 改为 bounded contract；
- module fixture 共享 immutable upstream，tamper 场景 copy-on-write。

### S3：历史感知调度

- 用最近 N 次 runtime artifacts 生成 deterministic duration + peak-memory manifest；
- 比较 `loadfile`、`loadscope` 与显式 shard 的 wall-time/peak-memory；
- 根据机器可用内存选择 bounded worker count，并保证完整 nodeid 集合与失败传播一致。

### S3A：Weight Search tail hardening（owner批准的有界优先批次）

2026-07-17 owner在EB0 formal full后批准继续针对性优化尾部。该批次以
`full_20260717T075427Z`为唯一同机器pre-change full证据：`6,195 passed / 2 skipped /
2,138.84s`，94%约在9分钟完成，但最后6%持续到35分38秒；进程树约13～14.5核持续活跃，
working/private峰值约`9.41/21.65GiB`且系统仍有约`57.9GiB`可用内存，因此根因优先级为
重复DAG计算与file-level duration偏斜，不是hang或内存不足。

S3A严格限定为：

- 第一批只审计并治理Weight Search直接链，优先顺序为Follow-up=`1,059.34s`、Decision=
  `635.33s`、Targeted=`410.65s`、Diagnostics=`369.99s`、Dashboard=`300.18s`、Evaluation=
  `267.60s`；Near-Miss A/B=`652.38s`仅在证明共享同一上游后才可纳入；
- 先识别单node内部重复builder/validator replay，再选择module/session immutable fixture、
  content-fingerprint PASS-only validation context或byte-exact copy-on-write；不得仅为降时替换真实
  builder/validator、跳过all-view rebuild或缩减tamper矩阵；
- Research Foundation的`468.92～500.45s`setup作为第二批候选，必须等待Weight Search第一批
  同node同命令证据闭合，不与第一批并发扩大修改范围；
- 调度策略只做只读benchmark；在证明nodeid、fixture隔离与失败传播等价前，不把`worksteal`、
  `loadscope`或不同worker数的结果替代正式`-n16 --dist loadfile` gate。

S3A第一批退出要求：目标文件collected nodeids/skip/xfail不减少；source/policy/output/tamper任一
byte变化仍真实失效且FAIL不缓存；同node同命令before/after可比；scoped Ruff、focused并行pytest、
architecture/contract/full均PASS；所有改动`strategy_logic_changed=false`、`production_effect=none`。

### S3B：剩余 Weight/Search Signal tail hardening（owner批准继续）

2026-07-17 owner在S3A复盘后批准继续优化。S3B以
`outputs/validation_runtime/full_20260717T132134Z/test_runtime_summary.json`为pre-change full
基线：`6,195 passed / 2 skipped / 642 warnings / 1,789.86s`。范围按收益和语义邻接限定为：

- 第一优先审计`tests/test_signal_feature_quality_filter_pipeline.py`=`744.32s`，确认其setup、
  builder与validator是否重复构建同一Signal Foundation上游；
- 第二优先审计同一Weight Search链的Near-Miss A/B=`605.90s`、Promotion Threshold=
  `593.53s`、Next Plan=`577.02s`与Candidate Promotion v2=`561.20s`；只在调用图证明共享
  immutable上游且一个最小复用边界可同时受益时实施；
- Research Foundation setup=`453.54～482.45s`仅作为S3B后续候选，不与前两项同时改动；
  Smoothed、策略阈值、评分、回测、promotion结论、生产与broker语义均不在本批范围；
- 正式命令保持`-n 16 --dist loadfile`。不得减少nodeid、DQ/PIT/source validation、all-view
  rebuild、tamper/cross-lineage矩阵或用stub替换真实builder/validator。

S3B退出要求：先记录目标文件同命令before，再以相同nodeids/命令记录after；任何无明确收益的实验
必须撤销；PASS-only cache在source/policy/output byte变化时真实失效且FAIL不缓存；scoped Ruff、
focused、architecture、contract、full均PASS；`strategy_logic_changed=false`、
`cached_data_mutated=false`、`production_effect=none`。S3B完成前不启动EB1。

### S3C：三个 Research Foundation setup（owner批准继续）

2026-07-18 owner在S3B闭合复盘后批准继续三个Research Foundation候选。S3C以
`outputs/validation_runtime/full_20260717T145741Z/test_runtime_summary.json`为pre-change full
基线：`6,195 passed / 2 skipped / 642 warnings / 1,720.76s`。范围严格限定为：

- `tests/test_research_direction_foundation.py` setup=`487.92s`；
- `tests/test_micro_search_foundation.py` setup=`478.61s`；
- `tests/test_signal_diagnosis_foundation.py` setup=`455.51s`；
- Smoothed、Targeted hardening、策略/阈值/评分/回测/promotion、EB1与任何下一callback slice均不在
  本批范围。

三个模块当前均已有module-scoped immutable fixture及覆盖fixture生命周期的
`artifact_validation_session`，调用图为Direction→Micro Search→Signal Diagnosis→Weight Scorecard
的嵌套前缀，主要嫌疑是`--dist loadfile`下不同worker重复构建相同上游，而不是缺少test-level session。
S3C必须先运行三文件同命令before并量化builder/validator调用与fixture规模，再按以下顺序评估：

1. 是否存在不改变business contract的测试fixture规模冗余；
2. 是否存在单worker内部仍重复的builder/validator或可安全延长的session边界；
3. 只有绝对路径/self-path commitment、source/policy/config/cache/DQ bytes、并发锁、失败恢复、
   tamper copy-on-write与worker/process隔离全部可证明时，才允许实验跨worker content store。

禁止用预生成stub替代真实builder/validator，禁止把共享可写root暴露给tamper测试，禁止减少nodeid、
skip/xfail、DQ/PIT/source replay、lineage、all-view byte rebuild或tamper矩阵。任何未获得明确同命令收益、
需要放松绝对路径/commitment或造成共享状态耦合的实验必须撤销。退出要求为scoped Ruff、三文件
same-command、expanded focused、architecture、contract与full全部PASS；记录full尾部和内存/CPU风险；
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。S3C完成前不启动EB1。

### S3D：full critical-path、全量观测与剩余长尾（owner批准继续）

2026-07-18 owner确认继续优化full整体耗时，并允许对独立高耗时单项使用多个agent并行推进。
S3D以`outputs/validation_runtime/full_20260717T161557Z/test_runtime_summary.json`为基线：
`6,196 passed / 2 skipped / 642 warnings / 1,830.80s`。当前artifact只保存最慢50项；其累计
`13,724.22 worker-seconds`，相当于16-worker可用wall-capacity约`46.9%`，其中Smoothed=
`28.7%`、Weight/Targeted/Search=`27.9%`、Research governance/promotion=`22.8%`。运行期99%
阶段总CPU降至约25%、仍有约55GiB可用内存，主要嫌疑为重型文件与`--dist loadfile`尾部偏斜，
而不是增加worker或内存不足。

S3D先并行执行三个只读审计lane：Smoothed共享DAG、Weight/Targeted/Search共享DAG，以及full
全量duration/worker/file/idle观测与duration-aware调度。协调者独占本需求、task register、manifests、
compatibility baseline和最终集成；各实现worker必须获得互斥文件范围后才可编辑。候选必须以同文件、
同nodeid、同机器、同`-n 16 --dist loadfile`before/after证明收益；无明确收益或增加共享可写状态、
absolute-path/self-commitment、锁恢复、tamper污染风险的实验必须撤销。

S3D不得减少nodeid、skip/xfail、DQ/PIT/source replay、required family、lineage、all-view byte rebuild、
tamper或FAIL-not-cached覆盖，不改变production默认规模、策略/阈值/评分/回测/promotion结论。开发期只跑
focused/分片；正式architecture、contract、full仅在本integration boundary各运行一次，失败修复后才
重跑。局部改善必须与full critical path、worker偏斜和全量分布共同解释；单次最好full不得声明稳定
系统性提速，长期完成仍要求同机连续3次median/P95、peak-memory和read-amplification证据。
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`；EB1与下一callback
slice继续不自动启动。

### S4：持续回归约束

- architecture/contract tests 校验 runtime manifest freshness、调度确定性和 gate coverage；
- 对单 slice、单 module 和 full suite 记录 before/after；性能改善不能以关闭 source validation、
  byte rebuild、DQ/PIT 或 tamper tests 换取。

## 验收标准

- 当前 4 个 confirmation 长尾 module 的累计 wall time至少降低70%，最大单shard不超过当前
  `1,230.03s`的40%，且tamper/drift focused
  tests 全部 PASS；
- full collected nodeid 集合与优化前一致（仅允许任务本身新增测试），无 skip/xfail 增量；
- 同一机器连续 3 次 full 的 P95 wall time 不高于当前 `2,554.80s` 的 60%；
- peak memory 与 read amplification 有 runtime artifact 证据，累计 read bytes 至少降低 80%；
- source/config/policy 任一 byte 修改会使 cache 失效，FAIL 不缓存；
- module/test manifests、architecture/contract/full validation PASS；
- `strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

## 当前状态

2026-07-18 / S3D三lane实现与局部退出门槛通过，正式集成门禁待闭合。Smoothed五文件同命令=
`17 passed / 435.79s -> 17 passed / 348.57s`（约`-20.0%`）；四个目标节点改善约
`20.2%～26.9%`，未修改的freshness control=`265.08s -> 263.55s`。实现仅给四个完整正向链
test node接入既有`artifact_validation_session`，nodeid、DQ/source、all-view、tamper与FAIL-not-cached
覆盖不变。Weight/Targeted五文件同命令=`5 passed / 435.56s -> 6 passed / 313.45s`
（约`-28.0%`）；四个compact目标改善约`27.4%～28.6%`，默认Targeted Search仍执行production
initial=`80`、targeted=`60～120`，test-only compact路径使用完整覆盖8个base families的52 variants
及完整覆盖6个required targeted families的`6～12` variants。共享Signal Foundation回归=
`10 passed / 216.03s`，schema、cross-lineage、price/rates source、DQ/PIT chronology、all-view tamper、
corrupted resume与FAIL-not-cached均保留。

full lane新增tracked `PARTIAL_SEED` duration profile：从基线top-50 phase rows聚合44个文件，保持
完整collection与文件内node顺序，仅在`full/-n 16/--dist loadfile`按历史文件耗时稳定降序，并在命令
末尾固定`--no-loadscope-reorder`避免xdist按node数量二次重排；missing/invalid profile显式回退stock
loadfile排序且`performance_evidence_status=FAIL`。独立`test_runtime_profile.json`记录collection
ordered/set hashes、逐node phase、file/worker aggregates和tail idle，summary只嵌入小摘要。pre-full
scheduler审计先从xdist`workerinput/mainargv`恢复真实worker/dist contract，要求串行、非loadfile、worker
mismatch、loadscope reorder或filtered selection不得重排/形成performance PASS，并把required phase、
inactive/unexpected worker纳入telemetry completeness；随后独立复核补上两项fail-closed证据：最终16个
worker的canonical collection必须是duration stable-order fixed point且至少命中一个tracked file，sidecar
`pytest_exitstatus`必须为non-bool int并与runner subprocess exit完全一致。final sidecar在summary构建前
以同目录临时文件atomic replace，summary记录真实sidecar hash/size；写入失败或exit mismatch只令
performance evidence FAIL，不覆盖pytest exit。`pytest-xdist`dev floor从`>=3.6`提升为`>=3.8`以保证
`--no-loadscope-reorder`可用。最终telemetry focused加family边界=`38 passed / 11.17s`，包含真实16-worker
applied-order证明、真实2-worker fallback、exit mismatch及sidecar write failure回归；14文件扩大focused=
`37 passed / 395.37s`，最慢node=`390.04s`且并行wall tail约`5.3s`。当前architecture、contract、
module/test manifests、compatibility baseline、deprecation/source hashes与正式full仍待协调者闭合；单次
full不得声明稳定提速。`strategy_logic_changed=false`、
`cached_data_mutated=false`、`production_effect=none`，EB1及下一callback slice未启动。

Post-full独立closeout复核又识别并闭合两项证据完整性缺口。Runner不再信任sidecar自报的三个PASS字段：
它会严格拒绝duplicate JSON key与NaN/Infinity，独立重算collection ordered/set identity、node/phase、
file/worker/outcome与node/file/worker/top-level时间聚合，并把真实runner worker/dist/formal-selection、
tracked duration manifest bytes/hash/
metadata/stable-order以及`arch_004e_test_manifest.yaml`完整`file_role=test`文件集合绑定到acceptance
contract；session UTC/elapsed必须自洽且包住derived node window，phase UTC必须由epoch重算，
formal scheduler policy/tie/untracked-weight/fallback语义必须精确匹配；unmatched file/worker aggregate或
contract evaluator异常都fail closed；任何缺字段、错型、bool冒充int、aggregate/hash/order/safety漂移
只把performance evidence降为FAIL，不覆盖pytest exit。Artifact finalizer改为先写最终reader/log/sidecar，
再采集真实hash/size，
最后写summary；复用artifact目录时空pytest output会覆盖旧log，`--json-output`取得同一final payload。
`--json-output`若解析到summary/reader/log/sidecar/benchmark等managed path则在pytest启动前拒绝，防止
final inventory采样后被覆盖。
Summary self record只承诺最终path/existence，显式标记`SELF_REFERENCE_NOT_EMBEDDED`，不伪造不可能
自洽的self SHA/size。新增/更新focused=`59 passed`，并用新reader重验正式11.7MB sidecar仍为
collection/telemetry/performance PASS（6,226 nodes、1,068 files、16 workers、warnings=[]）。

S3D formal gate已闭合：architecture首轮因新增runtime-profile test file后deprecation frozen inventory仍为
`1125`而正确FAIL，刷新inventory id/count、test manifest与source attribution到`1126`后正式architecture=
`322 passed / 59.65s`；contract=`214 passed / 45.32s`；full=`6,224 passed / 2 skipped /
642 warnings / 1,231.76s`，artifact=`outputs/validation_runtime/full_20260717T191414Z/`。collection=
`6,226`，相对旧`6,198`的28个新增node全部由新runtime-profile文件17个、runner hardening 10个和
compact/default family契约1个解释。相对直接基线`1,830.80s`墙钟缩短`599.04s / 32.7%`
（约`1.49x`），也比历史单次最好`1,721.81s`短约`28.5%`；slowest-50累计从`13,724.22s`
降至`11,522.02s`（约`-16.0%`），说明收益同时来自真实重复工作减少与重文件前移，不能只归因为调度。
Post-full evidence hardening新增22个runner/contract nodes后，正式architecture/contract重跑为
`344 passed / 50.28s`与`236 passed / 35.75s`，artifacts分别为
`outputs/validation_runtime/architecture-fitness_20260717T202818Z/`与
`outputs/validation_runtime/contract-validation_20260717T202913Z/`。既有full sidecar经最终reader严格重验
仍为三状态PASS；按integration-boundary-only原则不重复运行同一切片的full。

首个complete sidecar=`test_runtime_profile.json`（SHA-256=`dadf1943ebeb2ec50a45b31a9e01c8334a59706e34c8325f22ca3e72f790fc51`，
`11,703,225 bytes`）记录`1,068` files、`6,226` nodes、`18,676` phases与16 workers；collection/
telemetry/performance均PASS，无missing/extra/duplicate/inactive/unexpected，sidecar exit=`0`与runner一致。
44/44 tracked files、145 nodes命中，final ordered SHA与expected均为
`21a6b674824b572ce57b5e2ae0743ea89299872e28a235cfa21d63ad96cb790c`，fallback=false。
worker busy=`1,183.80～1,221.36s`、CV=`1.10%`、P90-P10约`32.47s`；internal idle总计
`3.89s / 0.02% capacity`，tail idle总计`414.51s / 2.12% capacity`、最大`37.64s`。95%时仍有
16/16 workers active，采样working set峰值约`16.62GiB`后回落，旧94%后单worker长尾未重现。
但这只是连续3次要求中的第1个complete profile，`stable_full_improvement_claimed=false`；长期任务仍因
另外2次同机完整profile、peak-memory/read-amplification与active-node heartbeat/ETA未闭合而保持
`IN_PROGRESS`。S3D切片完成但runtime目标继续，EB1及下一callback仍未自动启动，
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

2026-07-18 / S3C实现及focused验证完成，正式门禁待闭合：同机器、同
`python -m pytest -n 16 --dist loadfile`三文件pre-change=`51 passed / 333.25s`，Direction=
`328.00s`、Micro Search=`279.32s`、Signal Diagnosis=`310.34s`。跨worker store因
absolute-path/self-commitment、共享root、锁恢复及tamper COW隔离风险未实施。S3C改为仅在
Research Foundation test fixture中使用production generator的最小完整前缀；50 variants只覆盖
7个required families并被正式validator拒绝（`5 passed / 46 errors`，不计为PASS），52 variants
完整覆盖8个families且config validator PASS，非Foundation默认路径仍为80。候选同命令=
`51 passed / 204.73s`（约`-38.6%`），Direction=`197.70s`（`-39.7%`）、Micro Search=
`195.15s`（`-30.1%`）、Signal Diagnosis=`182.45s`（`-41.2%`）；expanded focused覆盖默认
80、完整Weight Foundation rebuild/DQ及tamper=`54 passed / 249.89s`。nodeid、DQ/PIT/source、
lineage、all-view与tamper矩阵均保留。正式architecture=`312 passed / 56.85s`、contract=
`204 passed / 42.28s`、full=`6,196 passed / 2 skipped / 642 warnings / 1,830.80s`均PASS；
三个目标full setup=`318.96s / 314.23s / 278.90s`，较旧full分别约`-34.6% / -34.3% /
-38.8%`。但full总时长较`1,720.76s`增加`110.04s`（约`+6.4%`），其他Smoothed、Next
Plan、Promotion和Weight/Targeted loadfile尾部继续主导，单次full不支持全局加速结论。S3C闭合，
任务因长期连续3-run P95/read-amplification与调度偏斜债务继续IN_PROGRESS；Smoothed、Targeted、
EB1与下一callback slice均未启动。module manifest与deprecation inventory不变、test manifest已刷新，
本次仅测试fixture及其contract/doc evidence，`docs/system_flow.md`数据/CLI/报告流不受影响；
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

2026-07-17 / S3B实现与focused验证完成，正式门禁待闭合：同机器、同
`python -m pytest -n 16 --dist loadfile`五文件pre-change=`5 passed / 498.20s`，Signal=
`494.41s`、四个Weight节点=`371.03～410.71s`。五个完整test node统一共享既有
`artifact_validation_session`后，两次候选分别=`399.15s`（约`-19.9%`）与`363.87s`
（约`-27.0%`），第二次五节点收敛到`353.30～359.93s`。只保留Signal外层session的消融为
`498.69s`：Signal=`355.62s`，但四个未统一边界的Weight节点=`458.23～494.86s`；因此收益来自
完整并行组减少重复artifact validation与共享I/O竞争，不能拆成单节点偶然热缓存结论。

调用图审计确认四条Weight fixture是每worker独立`tmp_path`中的线性前缀链，进程内没有第二份
builder DAG；继续做跨worker immutable store必须新增持久化content identity、copy-on-write与live
binding治理，超出S3B最小边界且风险收益不足，故未实施第二项代码改动。专项focused覆盖五个目标、
Targeted/Follow-up hardening和完整validation-session契约，结果=`85 passed / 1 skipped / 430.45s`；
唯一skip为Windows不支持`os.fork`的既有条件用例。Ruff与diff check PASS，nodeid/skip/xfail、首次
完整build、DQ/PIT/source validation、all-view rebuild、tamper与FAIL-not-cached语义均未减少。
正式full=`6,195 passed / 2 skipped / 642 warnings / 1,720.76s`，artifact=
`outputs/validation_runtime/full_20260717T145741Z/test_runtime_summary.json`；相对S3A full
`1,789.86s`再缩短`69.10s`（约`-3.9%`）。五个目标在full中分别为Signal=`444.13s`
（约`-40.3%`）、Near-Miss=`444.57s`（约`-26.6%`）、Promotion=`437.77s`
（约`-26.2%`）、Next Plan=`403.21s`（约`-30.1%`）、Candidate v2=`451.04s`
（约`-19.6%`）。formal architecture=`312 passed`、contract=`204 passed / 42.36s`；architecture
前两轮各`311 passed / 1 failed`，均由compatibility baseline按顺序拦截本批测试及累计任务文档旧
source hash，刷新可归属hash后才PASS，失败未被降级或绕过。

full在97%尾部的5秒只读采样显示16个worker全部CPU活跃、working/private约
`8.84/21.04GiB`；后续降为6个活跃worker、`8.49/20.65GiB`，证明尾部仍是file-level CPU/I/O
偏斜而非hang或内存不足。下一候选已转为Research Direction/Micro Search/Signal Diagnosis
Foundation setup=`455.51～487.92s`，其次Targeted hardening=`467.36s`与Smoothed Refresh=
`449.83～450.09s`；按S3B停止边界不在本批继续扩张。Research Foundation、Smoothed与EB1仍未
启动，S3B闭合后暂停复盘；`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。

2026-07-17 / S3A Weight Search第一批实现完成、转入正式验证：同机器、同
`python -m pytest -n 16 --dist loadfile`五文件命令的pre-change基线为`5 passed / 889.01s`，
其中Follow-up=`884.24s`、Decision=`533.73s`；最终同命令为`5 passed / 254.19s`，wall time
缩短约`71.4%`，其中Follow-up=`229.78s`、Decision=`88.59s`。隔离Decision同节点从
`500.55s`降至`102.93s`（约`-79.4%`），Follow-up隔离同节点从`884.24s`降至
`352.36s`（约`-60.1%`）。实现只让完整hardening node共享既有
`artifact_validation_session`，并把Decision内部上游验证迁移到PASS-only content fingerprint
调用；第一次完整builder/rebuild仍执行，任一view、snapshot或cross-lineage byte篡改均产生新指纹并
真实FAIL，FAIL不缓存。原五文件nodeid、skip/xfail与all-view tamper矩阵未减少；scoped Ruff已PASS。
最终正式门禁为focused=`291 passed / 1 skipped / 269.09s`、architecture=`312 passed /
51.75s`、contract=`204 passed / 43.65s`、full=`6,195 passed / 2 skipped / 642 warnings /
1,789.86s`。full相对同机器pre-change `2,138.84s`缩短`348.98s`（约`16.3%`）；其中
Follow-up full节点=`1,059.34s -> 408.82s`（约`-61.4%`），Decision hardening=
`635.33s -> 157.40s`（约`-75.2%`）。剩余前三直接长尾转为Signal Feature Quality=
`744.32s`、Near-Miss A/B=`605.90s`、Promotion Sensitivity=`593.53s`，Research Foundation
setup仍为`453.54～482.45s`。Targeted/Diagnostics/Evaluation没有保留无收益的代码修改；Research
Foundation第二批未启动，等待owner在本轮收口复盘后决定。`strategy_logic_changed=false`、
`cached_data_mutated=false`、`production_effect=none`。

2026-07-17 / S3A启动：owner确认在EB0 formal full后继续针对尾部耗时做有界优化。pre-change full=
`6,195 passed / 2 skipped / 642 warnings / 2,138.84s`，runtime artifact=
`outputs/validation_runtime/full_20260717T075427Z/test_runtime_summary.json`。相对`2,554.80s`与
`2,514.64s`近期基线分别约缩短`16.3%/14.9%`，但仍比历史最快`1,534.77s`慢约`39.4%`，且未满足
连续3次P95目标。当前优化先处理Weight Search单node内部重复DAG，Research Foundation setup排第二；
不启动EB1、不改变正式gate、worker/distribution或production语义。

2026-07-17 / EB0 clean candidate：正式focused已`274 passed / 1 skipped / 248.21s`；
architecture在修复deprecation inventory原始EOL hash漂移后由`310 passed / 1 failed /
62.70s`转为`312 passed / 58.22s`，contract=`204 passed / 44.22s`。修复仅规范架构inventory
的文本字节计数与SHA，不改变被扫描业务source、生命周期或removal readiness。当前最慢focused
节点为`204.52s`的smoothed freshness tamper，architecture最慢节点为`30.15s`的manifest stale
检查；二者均未越过既有预算，但前者应作为S3 duration-aware shard治理输入。formal full仍为
`PENDING`，不得把当前状态记为EB0完成，`production_effect=none`。

2026-07-17 / EB0-S2C：正式architecture首轮在`55.60s`结束，说明node-cap修复没有把架构层拖成长尾；
失败为2项可复现性/新鲜度门禁而非业务回归。其一是本批修改source/test/doc后generated manifests stale；
其二是CLI frozen tree把checkout绝对根目录编码进`Path`默认值，临时候选worktree与主工作区因根目录
不同产生987个node hash漂移。为保持并发任务隔离且不把共享工作区未归属改动混入formal gate，本批
增加有界architecture-only修复：contract序列化仅把`project_root`内绝对`Path`规范化为稳定token，
实际Typer默认值、参数解析、callback、CLI surface和production行为不变；增加跨checkout等价测试，
刷新CLI frozen contract与module/test/aggregate manifests后重跑全部正式门禁。该首轮architecture为
明确FAIL证据，不得静默记作PASS，`production_effect=none`。

2026-07-17 / EB0-S2B：定位并修复smoothed最高长尾的compatibility fingerprint node-cap
cache bypass。真实`9.09 MiB`snapshot包含`226,197` JSON nodes但仅45个bound paths，超过旧
`100,000`节点上限后会先累计读/解析、再把整个validation转为uncacheable并递归重放；修复前
单节点24分钟读取`171.05 GiB`且未完成。Node cap调整为`500,000`，但`64 MiB`文档、4,096
bound paths、路径形状、link/topology、tamper、PASS-only与before/after fingerprint门禁保持不变；
超过新上限仍明确bypass。高节点cache hardening=`78 passed, 1 skipped / 3.47s`；同节点同命令
从`>947.08s`收敛至`1 passed / 172.23s`，6个最重节点=`6 passed / 446.15s`，15文件扩大
focused=`27 passed / 498.56s`。这些结果证明局部read amplification根因已解除，但尚未执行本批
architecture/contract/full，也没有连续3次full、完整peak-memory/read telemetry或S3验收；因此本任务
与EB0都保持`IN_PROGRESS`，不得宣称全局稳定提速，`production_effect=none`。

2026-07-17：owner批准效率优先重排。本任务当前下一动作收敛为EB0 top-tail timebox，而非继续横向
迁移全部28个legacy scopes；EB0后回到G2.4 EB1～EB8主线，scope migration随所属batch渐进完成，
完整S2/S3性能验收等待稳定callback集合。本次只改变计划顺序，不改变已实现S2A代码、验证结论、
runtime或production状态。

G2.4CR/CS 已完成第一批通用 validation session、bounded snapshot 和 fixture path isolation；本任务
承接剩余 confirmation/full-suite 长尾。CV1/CU/CV2的`2,501.39/1,939.34/1,542.60s`证明单次
结果波动大，尚无稳定系统性提速；CV2 evaluation hardening已成为次级新热点，但前三仍是既有
confirmation/rule链。CV3/CW2通过单次fixture建链降低producer重复，但full中Decision hardening=
`414.50s`、Targeted hardening=`415.81s`，working set抽样峰值约`15.09/14.39GiB`，说明recursive
validator和并发内存仍是显著次级热点。建议实施顺序保持S2→S3：先完成confirmation weekly/
dashboard、rule queue/owner decision及weight evaluation/decision/targeted链的content-fingerprint /
bounded immutable fixture治理，再用
duration+peak-memory manifest比较loadfile/loadscope/explicit shard并限制memory-heavy并发，补
active-node heartbeat、当前node/worker资源与ETA；最终用连续3次median/P95与最大shard验收。
它是研发效率治理，不是ARCH-004 G2.5解锁条件，也不得绕过owner已批准的phase-level
`arch_005_bootstrap_handoff.v1`停止条件。

2026-07-15 CW3 已实现第一批跨Targeted/Follow-up链的PASS-only validation session。Cache key不是
artifact id或mtime，而是artifact全部业务/snapshot bytes以及从snapshot递归发现的live
config/cache/source bindings bytes；只缓存validator PASS后的payload，任一byte drift、missing source
或不同root都会产生新key并重跑真实validator，FAIL不缓存，恢复为完全相同bytes后才可复用原PASS。
缓存仅存在于最外层context-local validation session，嵌套调用共享，session退出即整体释放；不跨
进程、不持久化、不替代DQ/source replay或18 views byte rebuild。相同三个CW3业务nodeid在优化前
运行`1,376.92s`仅完成2个、第三个被人工中断；优化后
三个全部PASS为`295.14s`，观测wall time至少缩短78.57%。CW3 hardening含18-view/schema/lineage/
policy/live price drift与chronology检查，使用同一正式transitive fingerprint实现PASS=`605.38s`。
曾测得更快的非标准临时缓存结果，但因未覆盖完整live binding fingerprint而明确不采用。该单次对比证明本链根因修复有效，但不替代
本任务对full连续3次median/P95、confirmation长尾与peak-memory-aware sharding的最终验收。

CW3 final full=`6,039 passed / 3,045.40s`，相对CW2 `1,722.89s`单次回退76.76%，因此明确
`stable_full_suite_improvement_claimed=false`。Top long-tail为Confirmation Weekly=`1,325.47s`、
CW3 Follow-up Hardening=`948.28s`、Smoothed Freshness Hardening=`925.75s`、Confirmation
Dashboard=`737.73s`、Rule Review Queue=`721.30s`。约98%后多数worker空闲，证明下一工程优化
不能只增加`-n`：应基于历史duration与peak memory把这些文件分到独立shard，普通快测另组；同时为
confirmation/weight-search建立跨测试可校验的immutable fixture content store，key必须包含完整source/
policy/config/cache/DQ bytes、validator version和Python/schema version，命中后仍重验live binding，
任一drift或FAIL不得复用。验收仍是连续3次median/P95、最大shard、峰值内存和read amplification，
不得减少任何required nodeid、DQ/PIT/tamper/byte-rebuild gate。

CX1证明“先合并重复fixture、再做validator session复用”能在增强tamper coverage的同时取得局部
`61.05%`墙钟收益；但这仍只是单链单次证据，`stable_full_suite_improvement_claimed=false`。
本任务进入`IN_PROGRESS`，下一工程动作不变：先治理confirmation weekly/dashboard/rule queue等既有
长尾的immutable content store与copy-on-write，再用duration+peak-memory manifest限制heavy
shard并发；不得把CX1局部快跑误写成full-suite优化完成。

2026-07-16：S2A Confirmation DAG治理进入`IN_PROGRESS`。最新完整门禁为
`6,097 passed / 643 warnings / 2,337.22s`；前三相关长尾为Confirmation Weekly=`1,047.59s`、
Rule Review Queue=`602.18s`、Confirmation Dashboard=`538.56s`。本原子切片先对
Confirmation Weekly及其共享上游做只读调用图/指纹审计，再接入现有PASS-only validation session
与immutable module fixture/copy-on-write tamper边界；只有在cache key覆盖validator identity、
artifact绝对路径、全部materialized/source/policy/config bytes与语义参数，且byte drift立即失效、
FAIL不缓存、live source/DQ/PIT/byte rebuild保持时才实施。Focused必须保留原nodeids并增加cache
命中/失效/隔离证明；完成后记录before/after，但不以单次局部结果宣称full稳定改善，
`production_effect=none`。

2026-07-16：S2A第一层dependency-aware validation cache已实现，本轮安全与focused验证已完成，
随后闭合的architecture/contract/full正式门禁证据见本节末。调用面审计确认生产代码共29个cache调用：
唯一Forward Plan显式scope及generic调用进入hardened lane；其余28个未迁移legacy/no-scope调用
进入独立compatibility lane，只保留历史artifact根直系文件与
exact `path`+`sha256`传递依赖语义。Generic调用或任何显式scope始终进入hardened lane；cache key
显式包含fingerprint mode/version，两个lane不共享结果。Legacy lane是迁移债务，不代表28个调用已获
hardened全DAG覆盖；后续必须逐链声明scope并验证读取预算。

Hardened `ArtifactFingerprintScope`支持显式content/metadata paths、missing状态、bounded glob
inventories与`discover_bound_paths`开关；cache语义包含validator callable identity/name/version、
validator kwargs、semantic key、artifact root，以及scope path的lexical/resolved identity。
启用自动发现时识别受支持的`path/checksum/sha256/file_contents/exists/download_timestamp`
commitment组合；commitment path必须是绝对路径，download timestamp同时绑定文件内容与metadata。
Path同时受单项/累计UTF-8 bytes与component-count预算约束，并在任何parents展开/resolve/stat前拒绝
超深或超长commitment。超过file/JSON node/path/inventory/total-bytes/component预算，遇到任一路径组件的link/junction、hash期间文件变化、
FIFO/socket/device等非常规content对象、无法安全canonicalize的参数或无法deep-copy的报告时，均绕过
cache并执行真实validator；metadata-only scope可继续只绑定类型、size与mtime。

只有validator执行前后指纹完全一致且结果为exact `PASS`时才写cache；`FAIL`、
`PASS_WITH_WARNINGS`、exception及不可复制的`PASS`均不缓存。Cache entry写入时复制，命中时先
deep-copy、再二次确认fingerprint，只有仍一致才返回独立副本；首次执行或安全绕过直接返回validator
结果。Digest memo以真实bytes为内容身份，并结合size/mtime/ctime/mode/inode/device及Windows原生
ChangeTime判断是否可复用；hash期间签名变化会放弃cache，而不是接受混合内容。二次确认缩小但不能
提供文件系统事务语义，运行边界仍要求immutable artifact与统一scheduler期间无并发写，避免ABA瞬态。
文件digest另有process-local、线程安全、有界LRU，仅保存摘要、签名与绑定路径，不保存artifact bytes；
绑定路径受单项32KiB/256 components、单commitment累计8MiB/16,384 components预算约束，且只有绝对路径live topology通过
regular-file/content或regular-file/directory metadata检查后才可晋升process cache。Process key只保留
canonical resolved-path字符串，不持有`Path`隐藏缓存；LRU以key字符串、签名、memo容器、摘要及
绑定字符串的保守retained-bytes计量，总容量64MiB；其key隔离
raw/compatibility/hardened mode，完整签名变化立即重读。Validation session owner包含PID/thread/task，
fork child会清空继承的session与digest LRU并重建lock；validation report仍只在active session内复用，
不因digest LRU跨session、worker或进程传播。Session不再保留第二套`file_digests` mirror，process LRU
逐出后不会由active session继续无界持有memo、bound strings或`Path`隐藏状态。

Confirmation首批只接入固定id、无latest/cutoff选择的Forward Plan validation。Plan resolver要求
含顶层在内共13类已知input snapshot的`report_type/schema_version`严格匹配、依赖拓扑恰为12个
bounded source directories，并显式绑定snapshot声明的`path/*_path`、`position_advisory_config`、
ETF配置、universe/data-quality policy、current shortlist，以及price manifest/secondary-source候选。
只有`position_advisory_config`、`price_cache_path`、`rates_cache_path`三个与真实validator一致的已知
project-relative字段可按`_resolve_project_path`归一化，其余relative path仍绕过cache；prices/rates同时
进入content与metadata scope，因此bytes不变但`mtime`漂移也会使旧PASS失效。
Plan使用`discover_bound_paths=false`与streaming content identity；process-local scope LRU只保存按
immutable snapshot bytes解析出的有界lexical字符串，不长期持有`Path`隐藏缓存，命中时才重建scope。
每条lexical/resolved path受32KiB UTF-8与256 components限制，每scope累计受8MiB与16,384
components限制；这些预算在snapshot path resolve/stat前执行。LRU同时受16 entries与64MiB
retained-byte cap约束；发生逐出且table slack超过保守容器预算时才压缩table，PID/fork child会清空
entries/byte counter并重建lock。
命中返回前仍重查12个inventory roots的存在性、link/junction、32 direct-entry与nested-directory限制，
并确认显式content/metadata path未变成directory；失败会丢弃scope并执行真实validator。
Validation report仍仅存在于当前同步session，不跨worker/
进程或持久化。未知schema、缺失完整schema集合、relative/linked/directory-valued path、未知`*_dir`
语义或超预算拓扑均安全绕过cache并逐次调用真实Plan validator。

Registry、Progress、Evaluation、Rule Cycle、Owner Decision、Queue、Dashboard与Weekly自身仍执行
真实validator、semantic-latest selection、journal检查、source replay及materialized-view byte rebuild；
它们只在各自最外层同步调用内部复用未变化Plan的`PASS`。当前未实现跨测试/跨worker immutable fixture
content store；Weekly tamper仍在隔离tmp fixture中原地修改并恢复，不能记为copy-on-write完成。

当前同机`python -m pytest -n 16 --dist loadfile` focused证据：digest/session cache contract=
`76 passed, 1 skipped / 4.37s`；Plan scope/path/fork/live-precondition safety=
`14 passed, 1 skipped / 4.42s`。两处唯一skip均为Windows不提供`os.fork`的POSIX条件用例；
byte-cap/eviction/table compaction、单项与累计bound-path/component预算、topology-before-promotion、
metadata round-trip、relative-field allowlist、PID guard、live-precondition及lane隔离均在本机PASS。
按文件隔离运行的四个目标模块全部PASS，Weekly=`2 passed / 113.75s`、Rule Review Queue=
`1 passed / 80.64s`、Confirmation Dashboard=`1 passed / 92.00s`、Owner Decision=
`8 passed / 115.92s`，合计12个nodeids。隔离运行用于避免多个数百MB fixture并发造成内存竞争，四项
wall time不可相加为一个combined-run结果。旧`26 passed / 67.15s`混合了cache与目标nodeids；历史
full per-file duration与当前focused命令、nodeid集合及资源条件不同，因此不计算before/after比例，
不声明已满足S2降幅或最大shard门槛。`stable_full_suite_improvement_claimed=false`。

一次诊断性full在99%/103.5分钟时由操作者为控制诊断成本人工中止，不是PASS证据：停止时6个
workers仍有CPU/I/O进展，每5秒逻辑读取合计约3.3GiB，且无paging/OOM/deadlock，不能描述为hang
或正式runtime-budget触发。该被中止full临时结果中的Weekly、Queue、Dashboard三个Confirmation
节点约79--90秒，不包含Owner Decision，也不能替代上面的isolated focused范围。两次5-node
smoothed DAG诊断分别在约25.07/25.22分钟仍有进展时人工中止；两次single-bootstrap在超过10分钟
采样/决定窗口后人工中止，pytest tmp总wall约12.70/13.00分钟，读取约714MiB/5秒。上述运行没有
预先定义的自动stop threshold；扩大digest LRU容量未改变该读量并已撤销，证据仅表明主成本仍在
真实validator DAG重放与I/O密集文件并发，而非Plan resolver或digest容量。所有人工中止运行均不得
计为PASS、hang治理或正式runtime-budget结论，也不能与历史不同命令/资源条件样本计算改善比例；
smoothed immutable fixture复用留给后续S2，duration+peak-memory heavy-shard调度留给S3。

最终代码状态已通过正式门禁：architecture=`304 passed / 51.06s`，artifact=
`outputs/validation_runtime/architecture-fitness_20260716T152957Z/test_runtime_summary.json`；
contract=`203 passed / 36.45s`，artifact=
`outputs/validation_runtime/contract-validation_20260716T153054Z/test_runtime_summary.json`；full=
`6,185 passed, 2 skipped, 642 warnings / 18,238.77s (5:03:58)`，artifact=
`outputs/validation_runtime/full_20260716T153146Z/test_runtime_summary.json`。Formal full自然结束并PASS，
因此此前103.5分钟人工停止只属于诊断运行，不应解释为hang；但本次full的最大长尾node为
`17,801.26s`，且smoothed相关节点占据主要长尾，反而确认S2 immutable fixture与S3 heavy-shard
调度仍是必要后续。一次formal full不能替代连续3次full median/P95，也不能形成S2/S3比例改善声明。

任务保持`IN_PROGRESS`。正式architecture/contract/full门禁已闭合；仍需迁移28个legacy scope、实现
跨worker immutable fixture与tamper copy-on-write、补足其余2次连续full及peak-memory/read-
amplification telemetry，并完成S3 duration+peak-memory sharding，才可评估S2/S3比例验收。
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。
