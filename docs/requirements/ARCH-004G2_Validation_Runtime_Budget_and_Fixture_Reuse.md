# ARCH-004G2 Validation Runtime Budget 与 Immutable Fixture Reuse

最后更新：2026-07-19

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
2. EB0只有在phase-exit/owner停止点满足、正式focused/architecture/contract/full通过且当时owner显式
   授权时才可进入EB1，不等待28个无关legacy/no-scope调用整包迁移；后续owner批准的S3延展和最新
   slice lock优先，当前EB1仍锁定；
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

### S3E：post-profile critical-tail 去重（owner批准继续）

2026-07-18 / S3D提交后，按首个complete full profile重新排序下一轮候选，而不是沿用旧top-50摘要。
首个complete profile按file busy / worker-seconds（不是focused墙钟）重排出的候选为：Layer1
meta-policy=`962.13 worker-s/6 nodes`、Refined Method=`553.26 worker-s/3 nodes`、Promotion/Owner
governance四文件合计=`1,744.08 worker-s`、Weight Dashboard=`362.54 worker-s/1 node`。四个互斥
只读审计lane已完成，协调者继续独占本需求、task register、manifests、compatibility baseline和最终集成。

S3E优先消除同一test node内重复artifact builder/validator DAG与可证明安全的production-default测试放大；
不得跨worker共享可写artifact，不得弱化content fingerprint失效、absolute-path/self-commitment、
tamper FAIL、DQ/PIT/source replay、lineage或all-view byte rebuild。每个实现必须保留nodeids与生产默认，
使用同机器、同`python -m pytest -n 16 --dist loadfile`命令形成before/after，并通过对应expanded focused。

S3E已选择并实施三类互斥优化：Layer1/Layer2 forward outcome计算改为有界分块NumPy window kernel，
严格保持`decision_date + 1` PIT、NaN/downside/ddof/rounding/异常优先级与scalar结果逐字段exact；Refined
Method、Weight Dashboard和Owner Decision Pack只在单test/worker内延长PASS-only content-fingerprint
validation session；Candidate Promotion、Next Formal/Search Plan和Promotion Sensitivity仅在测试fixture显式
启用满足全部required families的compact matrix，production默认`80`及targeted `60～120`保持不变。
Layer1由首份profile的`962.13 worker-s`降至隔离after调用累计`318.71 worker-s`（约`-66.9%`），
Layer2由`165.28`降至`47.97 worker-s`（约`-71.0%`）；Refined同命令墙钟由`373.89s`降至
`279.36s`（约`-25.3%`）。Weight首份profile的`362.54 worker-s`与隔离after墙钟`77.26s`、
Governance首份profile的`1,744.08 worker-s`与隔离after调用累计`604.24 worker-s`因运行选择和并发上下文
不同，只作为候选方向证据，不冒充同命令全局降幅。forward kernel的无界3D原型在`6,352×5`输入峰值
`141.18 MiB`，加入`262,144`元素临时cube预算后为`15.39 MiB`；该预算是执行内存不变量，不是投资阈值。
未删除任何nodeid、skip/xfail或验证族，full级整体收益仍待integration gate证明。

多个独立候选合并后只在一个自然integration boundary运行architecture、contract和full，禁止为每个候选
单独重复full。下一次full只是第2份profile候选；只有final order、exit binding、telemetry、performance和
manifest freshness全部PASS后才计入连续3次要求，否则complete profile计数仍为1。EB1与下一callback仍
未启动，`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

#### S3E候选full首次运行：性能方向成立、complete gate拒绝

2026-07-18 / 标准`python scripts/run_validation_tier.py full --write-runtime-artifact`完整运行到100%，
结果=`6,245 passed / 1 failed / 2 skipped / 641 warnings / 1,083.96s`，artifact=
`outputs/validation_runtime/full_20260717T215025Z/`。唯一失败为
`tests/test_validation_runtime_profile.py::test_real_xdist_plugin_writes_complete_noncomparable_profile`：
内嵌2-worker pytest在collection期间遇到`aits_pytest_runtime_profile_*`临时目录消失，继而两个worker
收集集不一致。该失败不是业务、策略、PIT或Layer2计算回归，但属于runtime evidence自身的并发可靠性
缺口，必须直接修复并补竞态回归；不得用重试、serial或忽略失败绕过。

本次sidecar仍证明scheduler applied/no fallback/order verified、`6,248` nodes、`1,068` files、16 workers、
collection与telemetry PASS且无missing/extra/duplicate/inactive；但pytest exit=`1`使performance evidence
FAIL，因此complete profile计数保持1。作为不升级资格的方向性trace，总墙钟相对首份complete profile
`1,231.76s -> 1,083.96s`（约`-12.0%`），worker busy约`-11.9%`，八个S3E目标合计
`3,787.29 -> 1,807.84 worker-s`（约`-52.3%`），file P99约`346.50 -> 255.45s`、`>=300s`
文件由16降至7；这些结果不能声明稳定PASS样本。修复后须刷新test manifest/compatibility/source hashes，
并重跑相关focused、architecture、contract及一次标准full；只有新run联合PASS才能成为第2份profile。

竞态根因已直接闭合在测试隔离边界，而未修改生产runner生命周期：三个在系统TEMP生成mini suite的
subprocess测试现在使用仓库与`src`的显式`PYTHONPATH`、绝对duration profile、临时目录内相对suite、
`cwd/rootdir/confcutdir=tmp_path`。Windows pytest不再为外部initial path遍历系统TEMP兄弟目录，避免其他
xdist worker清理`aits_pytest_runtime_profile_*`时在`scandir -> lstat`之间触发`FileNotFoundError`。
原故障组合在外层`-n16 --dist loadfile`并加载真实runtime plugin后连续3次均为`59 passed`
（`8.77/8.82/8.82s`）；另在650次同前缀兄弟目录并发创建/删除压力下，三条real/subprocess测试
`3 passed / 5.85s`。未加入retry、serial、skip/xfail或延迟cleanup。刷新`947 modules / 1,126 test
files / 858 direct writers / 0 violations`后，正式architecture=`344 passed / 50.37s`（artifact=
`outputs/validation_runtime/architecture-fitness_20260717T223306Z/`）、contract=`236 passed / 37.29s`
（artifact=`outputs/validation_runtime/contract-validation_20260717T223402Z/`）；标准full rerun仍待执行。

#### S3E正式full重跑：runtime contract PASS，final code audit后资格待重跑

2026-07-18 / 修复nested-xdist隔离、刷新manifests/source hashes并通过formal architecture/contract后，
标准`python scripts/run_validation_tier.py full --write-runtime-artifact`结果=`6,246 passed / 2 skipped /
642 warnings / 1,109.04s`，artifact=`outputs/validation_runtime/full_20260717T223652Z/`。严格reader
重验为`profile/telemetry/performance=PASS`、pytest exit=`0`、`6,248 nodes / 1,068 files / 16 workers`；
scheduler applied、no fallback、duration order verified，collection无missing/extra/duplicate/inactive，且与首次
S3E候选run的ordered/set SHA-256完全一致。该run的runtime contract完整，但提交前独立code review随后
发现向量化整表转换在“多个非法值并存”时改变legacy decision→horizon→strategy首异常优先级；现有full
未覆盖这一exact契约。因此`223652Z`只保留完整性能方向证据，不计最终S3E第2份qualifying profile，
`complete_profile_count`暂时保持1，`stable_full_improvement_claimed=false`。

相对首份complete profile，正式墙钟`1,231.76 -> 1,109.04s`（约`-10.0%`）、worker busy
`19,126.79 -> 17,071.59 worker-s`（约`-10.7%`）、八个S3E目标`3,787.29 -> 1,832.55
worker-s`（约`-51.6%`）、file P99 `353.76 -> 269.52s`，`>=300s`文件由16降至7。
但tail max/total为`45.81/512.68s`，高于首份profile的`37.64/414.51s`；当前前三文件为Layer1
`448.42s`、Smoothed Weekly `418.06s`、Smoothed Refresh Hardening `394.23s`，说明critical path已
转向Smoothed与剩余foundation/weight链。人工只读采样还观察到working/private约`13.8/25.8 GiB`，
这是采样值而非完整peak telemetry。

性能可比性保守限制：并行只读Smoothed审计在full启动附近有一个约91～92秒的单Python fixture分段
计时，无法排除最多覆盖full开始后的前92秒；它不影响pytest正确性、collection或sidecar完整性，且只
可能使本run变慢，但本run不得单独用于稳定P95结论。full再次覆写两份tracked research Markdown，已
恢复HEAD内容；测试direct-writer隔离作为后续研发效率风险保留。异常优先级已用valid-input批量转换
fast path加conversion-failure-only legacy-order replay直接修复，并新增11日、两个策略分别在row10/row6
含非法值的exact type/message golden；focused=`1 passed / 10.17s`、Ruff PASS。刷新manifests/hashes后，
architecture=`344 passed / 50.53s`（`architecture-fitness_20260717T231619Z`）、contract=`236 passed /
34.47s`（`contract-validation_20260717T231715Z`）均PASS；replacement standard full随后已运行并形成
第2份qualifying profile，见下节。之后下一批
优先补齐Smoothed operations /
evidence test-local session，并把retry/weekly前置链收窄为recorded-owner authority prefix；生产默认、
DQ/PIT/source replay/all-view/tamper语义不得改变。

#### S3E replacement standard full：第2份complete profile PASS

2026-07-18 / 最终语义修复、manifest/hash刷新及formal architecture/contract PASS后，标准
`python scripts/run_validation_tier.py full --write-runtime-artifact`得到`6,246 passed / 2 skipped /
642 warnings / 1,040.50s`，artifact=`outputs/validation_runtime/full_20260717T231920Z/`。当前manifest与
duration profile下的strict reader确认`profile/telemetry/performance=PASS`、pytest exit=`0`、
`6,248 nodes / 1,068 files / 16 workers`；scheduler applied、no fallback、duration order verified，
collection与`215025Z/223652Z`的ordered/set SHA-256完全一致，telemetry不存在missing、extra、duplicate、
inactive、inconsistent或invalid phase。测试再次覆写的两份tracked research Markdown已恢复为HEAD内容，
其test direct-writer隔离保留为S3F明确工作项。

相对首份qualifying profile `191414Z`，墙钟`1,231.76 -> 1,040.50s`（`-15.53%`）、worker busy
`19,126.79 -> 16,143.75 worker-s`（`-15.60%`）、nearest-rank file P99
`353.76 -> 249.25s`（`-29.54%`）、`>=300s`文件`16 -> 6`、八个S3E目标
`3,787.29 -> 1,703.14 worker-s`（`-55.03%`）。worker busy CV=`1.10%`，tail max/total=
`32.46/355.47s`；相对非资格方向run `223652Z`，墙钟再降`6.18%`、tail total降`30.66%`。
因此`complete_profile_count=2/3`，但collection相对首份profile包含已知22个新增runtime-contract nodes，
且尚无第3份稳定集合证据，继续固定`stable_full_improvement_claimed=false`。

不为凑齐计数立即空跑第3次full。日常研究迭代只执行DQ、对应workflow validator与focused tier；本地或
研究链Full只在下一自然integration boundary、phase exit/handoff、broad shared-contract变更、正式性能
profile或失败修复后重跑时执行。weekly/monthly研究复盘不得自动映射为周期Full。独立developer CI的既有
daily scheduled Full是明确例外，使用`scheduled_ci`归属；它不是业务scheduler入口，也不改变研究cadence。

### S3F：Smoothed critical-path session与authority prefix（owner批准继续）

2026-07-18 / S3E提交`d407bc9b`并推送后，S3F按第2份qualifying profile
`outputs/validation_runtime/full_20260717T231920Z/test_runtime_profile.json`重新冻结候选。当前名称包含
`smoothed`或`smoothing_`的38个test files累计`3,559.65 worker-s`；其中Weekly/Refresh/Retry五个
authority链、Evidence Hardening及五个Operations leaf files共11文件累计`2,773.41 worker-s`，约占
Smoothed总量`77.9%`。这不是删除验证的理由，而是把重复建链收敛到同一worker内可证明安全的
PASS-only session或更短但仍真实的authority prefix。

S3F分为三个互斥实现lane，协调者独占本需求、执行计划、task register、system flow、manifests、
compatibility与最终集成：

| lane | 独占范围 | 当前profile基线 | 计划边界 |
|---|---|---:|---|
|A Operations leaf session|`test_smoothed_event_monitor.py`、`test_smoothed_forward_progress.py`、`test_smoothed_weekly_dashboard.py`、`test_smoothed_switch_readiness.py`、`test_smoothed_owner_renewal.py`|`898.89 worker-s`|仅在单test/worker内复用immutable上游与PASS-only content-fingerprint validation；每个leaf仍运行真实producer/validator，source/output/tamper与null-candidate语义不变|
|B Evidence/Readiness session|`test_smoothed_readiness_review.py`、`test_dynamic_v3_system_target_smoothed_evidence_hardening.py`、`test_smoothed_review_attribution.py`、`test_smoothing_benefit_lag.py`、`test_smoothed_regime_validation.py`、`test_smoothed_confirmation.py`、`test_smoothed_watch_pack.py`|`493.79 worker-s`|收敛同worker重复Evidence/Review前缀；不得跳过cross-lineage、policy/source drift、all-view byte rebuild或tamper fail-close|
|C Retry/Weekly authority prefix|`test_smoothed_forward_weekly_run.py`、`test_smoothed_bootstrap_retry.py`、`test_smoothed_data_refresh.py`、`test_smoothed_refresh_hardening.py`、`test_smoothed_freshness_hardening.py`|`1,628.75 worker-s`|新增test-only recorded-owner authority prefix，只允许真实Promotion→Gate/Switch→recorded `continue_observation`路径；必须保留Binding、Switch Plan、Recorded Owner、chronology、独立cache及至少一条Weekly/Retry完整链|

两份tracked research Markdown的测试direct-writer隔离由协调者在三lane集成后单独治理；不得靠事后
`git checkout`、忽略diff或永久写入随机TEMP路径掩盖副作用。Runtime artifact的`trigger_reason`与
task/boundary/parent-run provenance属于S4审计增强，不与三个性能lane混成同一实现风险。

执行顺序固定为：先用每个lane的完整文件集合与同一`python -m pytest -n 16 --dist loadfile`命令取得
pre-change baseline；再由三个agent在互斥文件范围实施；随后分别同命令after、expanded focused、Ruff和
副作用检查。只有三个lane全部集成后才刷新manifests/compatibility/source hashes并执行一次
architecture、contract和标准full。该full是下一自然integration boundary及第3份profile候选；仅当
node set、order、exit、telemetry、performance、manifest freshness和无外部负载均PASS时才计数。即使形成
`3/3` complete profiles，也必须在collection差异、median/P95、memory/read amplification证据审计后才能
决定是否升级`stable_full_improvement_claimed`，不得只因计数达到3自动宣称长期目标完成。

S3F安全不变量：不减少nodeid、skip/xfail、required family或真实validator；不改变production默认、DQ、
PIT、source replay、lineage、all-view rebuild、tamper、policy、阈值、投资解释或paper-shadow/production/
broker边界；跨worker不共享可写artifact/cache；任何baseline/after失败先诊断根因，不用serial、retry或
临时cleanup delay绕过。`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`；EB1与下一callback继续暂停。

S3F pre-change同命令baseline已在无其他pytest负载下顺序完成：Lane A=`5 passed / 165.38s`，五个
热点call累计`771.58s`；Lane B=`13 passed / 195.41s`，最慢Readiness/Watch cross-lineage/Replay为
`79.74/74.79/65.97s`；Lane C=`17 passed / 287.40s`，五个真实Weekly/Retry热点call累计
`1,111.66s`。三个命令均为`-n 16 --dist loadfile`且无skip/failure；after必须使用相同文件集合、参数与
运行隔离，不把full profile worker-s与focused墙钟混算降幅。

三个lane已按互斥范围实施并完成同命令after。Lane A只给五个test function增加public
`with_artifact_validation_session`，不改共享helper；结果=`5 passed / 119.34s`，墙钟`-27.84%`，五call
`771.58 -> 574.00s`（`-25.61%`）。Lane B同样只把既有session延长到11个磁盘链node外层，纯内存与
无重复链node不装饰；结果=`13 passed / 61.99s`，墙钟`-68.28%`，主要call
`384.71 -> 134.53s`（`-65.03%`）。Lane C新增test-only
`run_smoothed_recorded_owner_authority_fixture`：真实运行Promotion链，按Binding/Switch/Pending Owner
manifest最大时间`+1s`记录`continue_observation`，消费者再`+1s`；旧Operations helper原样保留，五个
热点仍各自运行真实Weekly/Retry/Refresh与tamper validator。结果=`17 passed / 249.57s`，墙钟
`-13.16%`，五热点call`1,111.66 -> 938.54s`（`-15.57%`）。三命令顺序墙钟合计
`648.19 -> 430.90s`（`-33.52%`），目标call合计`2,267.95 -> 1,647.07s`（`-27.38%`）；这仍只是
focused同命令单次运行证据（`single_run_focused_evidence=true`），其中call口径是各pytest node
`call` phase duration的累计秒数（`aggregate_pytest_call_duration_seconds`），不是并行墙钟，也不冒充full
整体收益。

两处tracked-doc direct writer也已test-only隔离。Indicator Family Ablation exact CLI test把12个secondary
matrix、1个feature-set和15个Markdown默认输出全部monkeypatch到`tmp_path`并断言28件真实写出；Layer1
Final Gate exact CLI test仅用`functools.partial`为真实producer绑定临时owner doc。两个node=`2 passed /
94.12s`，运行后`indicator_family_only_model_review.md`与
`layer1_selector_pause_or_continue_owner_pack.md`均与HEAD byte内容一致且不进入worktree status。
扩大focused覆盖三个lane、validation-session核心、Smoothed operations/promotion/evidence hardening、
Layer1与boundary contract，共collection `135` nodes，结果=`134 passed / 1 skipped / 356.52s`；唯一skip
为既有Windows平台条件
`test_validation_session_active_report_and_digest_lock_do_not_cross_fork`（requires POSIX `os.fork`）。
Ruff与`git diff --check` PASS，三个性能lane node counts分别保持`5/13/17`，未增加retry/serial/skip/xfail。

S3F自然integration-boundary正式门禁已闭合：governance=`28 passed / 28.70s`、architecture=
`344 passed / 49.72s`、contract=`236 passed / 35.49s`；唯一一次标准full位于
`outputs/validation_runtime/full_20260718T004439Z/`，结果=`6,246 passed / 2 skipped / 642 warnings /
1,027.74s`。Strict reader确认profile/telemetry/performance、pytest exit、16-worker loadfile、duration
order、no fallback及test manifest全部PASS；sidecar覆盖`6,248 nodes / 1,068 files / 18,742 phases`，
ordered/set SHA分别为`7b7dc4a7838cbf17d68a64b2e3aaf4dfc59267c75109a58139ea1b03986d7322`与
`6e8fee8709c8e781952a29224a8857d01bd6df5c50f9070508ad1dac9c5a18ee`，与第2份profile完全一致，
因此成为第3份qualifying complete profile。

相对`231920Z`，full墙钟`1,040.50 -> 1,027.74s`（`-1.23%`）、worker busy
`16,143.75 -> 15,830.64 worker-s`（`-1.94%`）；Smoothed 38文件
`3,559.65 -> 2,988.52 worker-s`（`-16.04%`），S3F全部17文件
`3,021.42 -> 2,447.23 worker-s`（`-19.00%`）。三lane在full中的A/B/C分别改善
`17.07%/58.96%/7.96%`，证明实现收益真实存在；但wall收益较小，因为critical path已转移且调度尾部
变差：file P99=`249.25 -> 258.01s`、worker CV=`1.10% -> 1.53%`、tail max/total=
`32.46/355.47 -> 42.28/460.50s`。当前前五文件为Layer1=`436.82s`、Smoothed Weekly=
`366.89s`、Refined Method=`365.32s`、Smoothed Refresh=`342.49s`、Weight Diagnostics=`305.55s`；
它们构成下一批并行候选，不能再把Smoothed focused降幅线性外推为full墙钟。

三份qualifying墙钟为`1,231.76/1,040.50/1,027.74s`，median=`1,040.50s`、nearest-rank
P95=`1,231.76s`，满足既定墙钟阈值；但第1份collection少22个runtime-contract nodes，当前同集合样本
只有2份，且sidecar仍没有complete peak-memory与read-amplification证据。因此
`complete_profile_count=3`但`stable_full_improvement_claimed=false`，不得把计数闭合误报为长期验收完成。
两份tracked research Markdown在full前后SHA与worktree均不变，direct-writer隔离闭合；本批
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

2026-07-19 / ARCH-004G2.4-EB3 collection bootstrap：EB3新增一个test file后，PB1的
`COMPLETE v8 (6,352 nodes / 1,071 files)` identity自然过期。为避免先运行一次必然fallback的
formal Full，tracked manifest转为`PARTIAL_SEED v9`，保留1,071个历史file durations但不再声明exact
collection或stable improvement；EB3新文件保持stable collection order。下一次自然Full负责采集当前
collection与telemetry，若profile/validation provenance不完整仍fail closed，不能把fallback记为性能PASS。

2026-07-19 / ARCH-004G2.4-EB3 natural Full结果：唯一正式run=
`outputs/validation_runtime/full_20260719T081601Z/test_runtime_summary.json`，`6,357 passed / 2 skipped /
643 warnings / 981.25s`，精确覆盖`6,359 nodes / 1,072 files / 16 workers`。`PARTIAL_SEED v9`匹配
1,071个历史files与6,354个nodes，新文件按stable order调度；scheduler=`applied/no-fallback`，telemetry、
performance、validation-provenance binding全部PASS。最大tail-idle=`57.82s`，当前最慢node为Smoothed
Weekly no-due-windows=`476.01s`，其后Freshness/Refresh/Bootstrap长尾约`301.15～340.71s`；整体墙钟仍在
近期优化后`912～1,030s`区间。由于只有一次同集合样本且profile仍是partial seed，继续固定
`stable_full_improvement_claimed=false`；这些长尾作为后续runtime任务候选，不扩张EB3正确性范围。

### S3G：转移后关键路径与complete duration profile（owner批准继续）

2026-07-18 / S3F提交`f557d04d`并推送后，以第3份qualifying profile
`outputs/validation_runtime/full_20260718T004439Z/test_runtime_profile.json`冻结下一批。该profile
覆盖`6,248 nodes / 1,068 files / 16 workers`，文件duration合计`15,830.64 worker-s`；当前前五为
Layer1=`436.82s`、Smoothed Weekly=`366.89s`、Refined Method=`365.32s`、Smoothed Refresh=
`342.49s`、Weight Diagnostics=`305.55s`。实际worker busy最大值约`1,018.20s`，而完整文件duration
的离线LPT容量下界约`989.41s`，两者约`28.8s`差距只作为调度候选排序证据，不作为未来wall收益承诺。

S3G冻结为四个互斥lane；协调者继续独占本需求、执行计划、task register、system flow、module/test/
aggregate manifests、compatibility、deprecation inventory与最终集成：

| lane | 独占范围 | profile基线 | 实施与安全边界 |
|---|---|---:|---|
|A Layer1 per-node context session|`tests/test_layer1_meta_policy_readiness.py`|`6 nodes / 436.82 worker-s`|只在每个test的direct Python producer block内复用一次真实PASS/DQ/PIT context；key绑定resolved输入/输出路径、配置、日期及live content fingerprint，hit重验输入和materialized facts并deep-copy返回值；进入真实CLI前恢复原函数，不跨test/worker共享，不应用于DQ/source/lineage/tamper路径|
|B Smoothed Refresh outer session|`tests/test_smoothed_refresh_hardening.py`|`4 nodes / 342.49 worker-s`|只为`test_blocked_chain_freezes_v2_inputs_and_rebuilds_every_view`与`test_data_readiness_rejects_cross_chain_composition`增加既有public validation session外层；仍真实执行34项all-view/tamper、left/right chain、readiness与source drift；本轮不缩短326-day authority prefix、不删Evidence Gap、不改production validator|
|C Refined/Diagnostics fixtures|`tests/test_refined_method_proposal.py`、`tests/test_weight_search_diagnostics_hardening.py`|`3 nodes / 365.32s`与`1 node / 305.55s`|Refined仅由前两个read-only nodes共享module-scoped immutable真实fixture，并用tree fingerprint finalizer证明无写入；tamper node继续独立完整建链。Diagnostics仅显式使用既有test-only 52-variant complete-family matrix，仍覆盖8个required families、21 views、schema/lineage/policy/tamper；production 80-variant默认不变|
|D complete full-file scheduler profile|`scripts/pytest_runtime_profile.py`、`scripts/run_validation_tier.py`、对应runtime tests、`inputs/architecture/arch_004g2_full_duration_profile.yaml`|现有44-file旧`PARTIAL_SEED`；source profile为1,068-file exact duration|v1 loader兼容`PARTIAL_SEED`并新增fail-closed `COMPLETE`状态；complete manifest必须绑定source profile SHA-256、1,068-file/6,248-node collection identity与文件全集，runtime sidecar披露coverage；完整集合才使用`complete_full_duration_descending_stable`，缺失/重复/stale/worker/dist不匹配均stock fallback且performance evidence FAIL；保持同文件node顺序、完整nodeid和16-worker loadfile语义|

执行顺序固定为：共享文档先冻结；在无其他pytest负载下顺序运行A、B（Weekly/Refresh分别及合并）、
C（两个文件分别及合并）的同命令`-n 16 --dist loadfile -q --durations=0` pre-change baseline；随后按
互斥文件并行实现并使用相同命令after。D先通过loader/reorder/strict-reader focused contract，再由协调者
从上述source profile机械生成完整manifest；不得用估算duration、局部top-N或after focused比例伪装完整
profile。四lane合并后运行expanded focused、Ruff、diff/side-effect、manifest/hash freshness、architecture与
contract；只在这一自然integration boundary运行一次标准full。

S3G pre-change baseline已在无其他pytest进程下顺序完成，全部使用显式candidate `PYTHONPATH`与相同
`-n 16 --dist loadfile -q --durations=0`：A=`6 passed / 357.91s`，五个业务call为
`52.77/59.20/30.59/95.27/113.07s`；B的Weekly=`1 passed / 230.57s`、Refresh=
`4 passed / 213.56s`、两文件合并=`5 passed / 267.77s`，Refresh四call为
`39.06/1.68/21.70/147.80s`；C的Refined=`3 passed / 330.34s`、Diagnostics=
`1 passed / 275.80s`、两文件合并=`4 passed / 381.70s`，单文件call分别为
`99.87/83.61/143.50s`与`272.57s`。合并运行因CPU/I/O竞争使B/C aggregate call分别约
`518.21/721.35 worker-s`，因此after必须同时报告单文件wall、合并wall与aggregate call，不能把并行
资源竞争误写成代码回退或把墙钟遮蔽误写成无收益。

S3G A首次after=`6 passed / 330.74s`，相对`357.91s`仅`-7.59%`；相同命令第二次after=
`6 passed / 362.52s`，反而`+1.29%`。该方案需增加约314行test-local fingerprint/deep-copy/session逻辑，
两次结果不能证明收益稳定且维护成本过高，故完整撤回并确认Layer1测试文件与`f557d04d` byte-exact；
Layer1后续候选改为下一profile后的文件拆分或更直接的上游去重，不在本批为保留“优化”而接受复杂度。

S3G B/C同命令after已闭合。Refresh=`4 passed / 185.66s`，相对`213.56s`为`-13.06%`；两个
目标call=`60.76 -> 40.89s`（`-32.70%`），未改retry control仍真实执行。Weekly+Refresh合并wall=
`267.77 -> 275.33s`（`+2.82%`），但目标call=`72.83 -> 47.51s`（`-34.77%`）、全部call=
`518.21 -> 512.66s`（`-1.07%`）；回退来自Weekly/retry资源波动，证明不能只看被遮蔽wall。
Refined=`3 passed / 276.60s`（由`330.34s`下降`16.27%`），Diagnostics=`1 passed / 240.19s`
（由`275.80s`下降`12.91%`）；两文件合并=`4 passed / 247.81s`，由`381.70s`下降`35.08%`，
aggregate phases约`721.35 -> 459.29 worker-s`（`-36.33%`）。共享Refined tree finalizer PASS，tamper
仍独立完整建链；Diagnostics仍覆盖全部8个required families与21 views。B/C保留，A拒绝；D的
COMPLETE profile contract已实现；在该pre-full安全集成点，正式architecture/contract/full门禁尚待执行，
后续结果见本节formal closeout。

S3G D已把旧44-file `PARTIAL_SEED`兼容扩展为绑定`004439Z`的1,068-file/6,248-node
`COMPLETE` profile。机械生成时逐项断言source artifact SHA、collection ordered/set SHA、file-set SHA、
canonical file-row SHA、expected scheduled-order SHA和`15,830.6369954 worker-s`总量；loader重验
`valid=true`。完整集合、16-worker、`loadfile`和`--no-loadscope-reorder`同时匹配时才应用
`complete_full_duration_descending_stable`；任何stale/duplicate/worker/dist/loadscope不匹配均显式使用
`stock_loadfile_test_count_order`、`fallback=true`且performance evidence FAIL。Strict reader新增双向
eligibility约束，既拒绝不满足全集的伪applied，也拒绝exact eligible条件下的伪fallback；applied scheduler
的policy/tie/untracked/loadscope/file-order语义即使performance为FAIL也独立校验。独立审查用归档6248-node
phase telemetry按新顺序静态重建，profile/telemetry/performance、coverage、order和manifest rebind均PASS，
P0/P1/P2未留项。

D冻结59个既有nodeids的focused由before=`59 passed / 9.97s`变为最终`59 passed / 12.56s`；新增检查
只强化完整profile/strict-reader契约，不以该毫秒级fixture开销宣称性能收益。四lane扩大focused覆盖
validation-session、Weekly/Refresh control、Refined、Diagnostics默认/compact边界、runtime reader和共享
文档契约，共`168 passed / 1 skipped / 285.14s`；唯一skip仍是Windows缺少POSIX `os.fork`。Ruff、
py_compile、diff check、module/test/aggregate manifest生成及compatibility/deprecation source freshness均PASS；
pre-full architecture=`344 passed / 58.90s`、contract=`236 passed / 45.03s`均PASS；唯一一次自然边界
full随后执行。

S3G formal full=`6,246 passed / 2 skipped / 642 warnings / 1,243.76s`，pytest wall=
`1,242.72s`，artifact=`outputs/validation_runtime/full_20260718T031839Z/test_runtime_summary.json`。
严格reader确认相同`6,248 nodes / 1,068 files / 16 workers`、完整phase telemetry、expected order、
`scheduler_applied=true`、`fallback=false`、profile/telemetry/performance=`PASS`，tracked research Markdown
前后SHA不变。该PASS只证明正确性与证据契约，不等于原始性能改善。

相对source `004439Z`，summary wall=`1,027.74 -> 1,243.76s`（`+21.02%`）、profile wall=
`1,026.638 -> 1,242.285s`（`+21.01%`）、worker busy=`15,830.637 -> 19,707.901
worker-s`（`+24.49%`）、nearest-rank file P99=`258.007 -> 333.018s`（`+29.07%`）。
`1,068`个相同文件中`981`个变慢，文件倍率median=`1.2625x`，old/new file duration correlation=
`0.9914`；call/setup分别增加约`23.09%/40.84%`，属于广泛近似乘法膨胀，不能归因于单个B/C改动。
三个实际改动业务文件合计仅`1,013.359 -> 1,016.127s`（`+0.27%`），两个Refresh目标node仍
`-23.23%`、Refined两个共享fixture node仍`-30.24%`；Diagnostics虽原始`+8.82%`，相对同期重文件约
`+25%～29%`的控制变化仍与focused改善方向一致。因此B/C保留，但不声明稳定full收益。

COMPLETE调度的独立容量分解证明其负载均衡有效：worker busy CV=`1.529% -> 0.00619%`，tail
total/max=`460.495/42.281s -> 0.177/0.017s`；test-window与容量下界的差距由约`29.024s`
降至`0.492s`，消除约`28.53s`调度损失。但执行工作量下界同时增加约`242.33s`，净wall仍回退。
两次不是同commit受控A/B，尚不能区分外部机器降频与重型文件同时前置造成的CPU/I/O/memory竞争。
因此保留COMPLETE manifest、exact coverage、fallback和strict-reader基础设施，将
`complete_full_duration_descending_stable`视为试验性调度策略；不把异常慢run回写为新权重，也不为寻求
更好数字空跑full。下一自然边界前先做同commit的resource-aware stagger/bucket只读建模和focused
验证；验收必须同时要求raw wall、worker busy、P99不回退并保持CV/tail收益。

S3G验收不以单文件降幅代替full结论。每lane必须保持原nodeids、skip/xfail与required assertions；A/C的
immutable复用发生在同worker且必须证明输入未漂移，B不扩大到语义缩短，D只改变调度顺序。最终full已
验证collection set、同文件node顺序、scheduler applied/no fallback、profile/telemetry/performance、manifest
freshness与tracked-doc/worktree副作用；其原始wall/busy/P99回退而调度CV/tail改善，故全局收益判定为
`INCONCLUSIVE_RAW_REGRESSION`。仍不补写缺失的complete peak-memory/read-amplification，且固定
`stable_full_improvement_claimed=false`、`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。EB1、下一callback与ARCH-004下一slice继续暂停。

### S3H：低风险compact前缀与resource-aware调度建模（owner批准继续）

2026-07-18 / owner继续授权尽可能降低full耗时，并允许高耗时单项按互斥文件多agent并行。S3H以
`outputs/validation_runtime/full_20260718T031839Z/test_runtime_profile.json`为候选排序证据，但因该run
存在广泛`1.2625x`文件膨胀，所有old/new比较必须同时给出raw与全局worker-busy倍率归一口径，不能把
系统性变慢误判为代码回退或收益。

S3H只冻结两个低复杂度实现lane和一个只读调度lane；共享需求、task register、system flow、manifests、
compatibility与最终集成仍由协调者独占：

| lane | 独占范围 | 最新profile成本 | 候选与退出门槛 |
|---|---|---:|---|
|A Search Coverage Gap compact prefix|`tests/test_search_coverage_gap.py`|`196.08 worker-s`|只给既有fixture显式传入`compact_test_matrix=true`，继续覆盖8个required families；production默认80、nodeid、断言、DQ/PIT/source-lineage/tamper不变。同命令before一次、after至少两次；若两次after不能稳定改善至少10%，完整撤回。|
|B Legacy conclusion compact prefix|`tests/test_no_promotion_review.py`、`tests/test_near_miss_candidates.py`、`tests/test_cash_buffer_attribution.py`|合计`244.53 worker-s`|仅在三个既有helper调用点启用已有compact完整family矩阵；保留zero-promotion、`cash_buffer_10` near-miss、tradeoff/recommendation语义及所有threshold/conclusion。同命令before一次、after至少两次，并检查逐文件duration；稳定改善不足10%或任一安全断言需放宽即撤回。|
|C Resource-aware scheduler model|只读消费`004439Z`与`031839Z` profiles；若后续实施只允许runtime scheduler/test专属文件|duration-only已消除约`28.53s`调度损失但worker busy增加`24.49%`|独立量化首批重文件重合、resource contention proxy及duration-only/stock/stagger/bucket离线容量；在没有可复现分类、确定性tie、exact collection/fallback和不回退wall/busy/P99验收设计前不修改默认调度。|

基线必须在无其他pytest负载下由协调者顺序取得；A/B随后按互斥文件并行实现，禁止agent编辑共享文件。
合并后运行相同命令复测、expanded focused、Ruff、diff/side-effect、manifest/hash freshness及正式
architecture/contract；只在两个实现lane均达到退出门槛且调度决策冻结后的下一自然integration boundary
运行一次full。第三个高复杂度业务lane不启动：Layer1已因不稳定和314行复杂度撤回，Foundation跨worker
复用与tamper/race风险、Execution Semantics大上下文重构均尚未达到收益/风险门槛。

S3H isolated baseline与after均在commit`1d1701d6`、无其他Python/pytest进程、同一
`-n 16 --dist loadfile -q --durations=0`口径下顺序取得。Lane A before=`1 passed / 120.28s`、call=
`117.06s`；两次after=`1 passed / 95.77s`与`1 passed / 91.64s`，wall分别`-20.38%/-23.81%`，
call=`92.70/88.70s`（`-20.81%/-24.23%`）。Lane B before=`3 passed / 86.17s`，三个call=
`33.70/56.87/82.94s`、合计`173.51s`；两次after wall=`69.38/62.57s`（`-19.49%/-27.39%`），
call分别=`27.71/43.34/66.08s`与`21.70/38.29/59.10s`，合计`137.13/119.09s`
（`-20.97%/-31.36%`）。两lane的worst-after均超过10%门槛，四文件各自call均改善，故保留。
实现只有四个fixture调用点各增加`compact_test_matrix=True`；nodeid和断言byte-exact不变，52 variants
静态及`test_weight_search_space`真实检查均覆盖8/8 required families，production initial batch仍为80。
四目标加family contract的expanded focused=`6 passed / 116.57s`；并发wall只作正确性证据，不覆盖isolated
性能口径。

只读调度模型确认`031839Z`首批16文件同时启动三个setup-majority Foundation文件，而`004439Z`首批为0；
首批setup累计约`1,266.48s vs 0.16s`。三文件旧start约`223.12/223.35/366.89s`，新run均为`0s`，
duration约为旧值`1.37～1.38x`。以source duration对1,068 files做16-bin离线list scheduling，将三者放在
rank`0/16/40`时old/new无争用theoretical makespan仍为`989.415/1,231.791s`，说明stagger在静态容量上
没有可见代价；但两profile没有peak RSS、I/O/read amplification，且全局文件median同样膨胀`1.2625x`，
仍无法证明setup并发是因果来源。因此本slice的调度决策为`NO_CHANGE_INSUFFICIENT_RESOURCE_TELEMETRY`：
保留duration-only COMPLETE pilot和strict fallback，不新增经验resource threshold或重排默认；下一自然
full只作为新的同集合观测，若setup三文件在正常全局倍率下仍重复异常，再登记带RSS/I/O采样与受控
duration-only/stagger A/B的独立调度slice。

S3H唯一自然边界full=`6,246 passed / 2 skipped / 643 warnings / 1,294.07s`
（`full_20260718T042919Z`），exact `6,248-node/1,068-file/16-worker`集合、COMPLETE order、
no-fallback及profile/telemetry/performance均PASS。四个实现目标在full内由`440.604s`降至
`351.160s`，合计`-20.30%`；相对同轮其余matched files约`+5.76%`的中位膨胀，归一改善约
`24.64%`，证明compact前缀收益在全量并发中仍成立。与此同时raw wall/worker busy相对S3G分别
`+4.05%/+4.08%`，扣除四目标节省的`89.444 worker-s`后，其余文件累计增加约`893.022 worker-s`，
因此全局回归不能归因于S3H，也不能声明稳定full改善。COMPLETE scheduler再次达到worker busy
CV=`0.00519%`、tail total/max=`0.119/0.014s`；三项Foundation setup保持约100%重叠，但其累计仅
比S3G增加`1.98%`，低于全局中位膨胀，故继续裁决`NO_CHANGE_INSUFFICIENT_RESOURCE_TELEMETRY`，
不实施文件名/rank特判。S3H状态为`COMPLETE_RUNTIME_TASK_CONTINUES`；下一批必须按本profile重新
审计高成本Smoothed、Signal与Weight/Search DAG，先登记、取isolated baseline，再按互斥文件多agent
实现，仍只在多个候选合并后的自然integration boundary运行一次full。

### S3I：Smoothed compact authority与Targeted上游PASS cache（owner批准继续）

2026-07-18 / owner要求继续尽可能降低full耗时，并允许高耗时单项按互斥范围多agent并行。S3I以
`outputs/validation_runtime/full_20260718T042919Z/test_runtime_profile.json`为排序证据，先完成三个只读
审计lane再冻结实现；base commit=`77f394f0`，共享需求、task register、system flow、manifests、
compatibility、最终集成与full仍由协调者独占。

| lane | 独占实现范围 | 最新profile成本 | 实验与撤回门槛 |
|---|---|---:|---|
|A Smoothed weekly compact authority|`tests/dynamic_v3_system_target_helpers.py`、`tests/test_smoothed_forward_weekly_run.py`|`413.32 worker-s`|`REVERTED_THRESHOLD_MISS`：显式test-only 160-weekday profile首轮after=`2 passed / 209.62s`、重型call=`206.33s`，相对before `228.52/225.40s`仅改善`8.27%/8.46%`，低于预先冻结的worst-after 10%门槛。因为首轮已使worst gate数学上不可通过，按early-stop不再浪费第二轮；helper/test已byte-exact恢复base。production `end=latest_available`、synthetic默认326 weekdays及全部研究语义从未改变。|
|B Targeted upstream PASS cache|`src/ai_trading_system/etf_portfolio/dynamic_v3_weight_search_targeted.py`、`tests/test_weight_search_targeted_hardening.py`|`373.29 worker-s`|`RETAINED_THRESHOLD_PASS`：五个adapter接入同一session、content-fingerprint、PASS-only cache；严格schema→edge白名单覆盖Coverage/Cash/Near/Review/Scorecard/Weight/Matrix/Search/Paper/Model DAG，paper全部cache paths、weight独立price-root DQ siblings及Model Target/Daily Advisory两级semantic inventory进入fingerprint。resolver任一schema、commitment、拓扑或预算异常均直接执行真实validator。before=`188.80s`，两次正式after=`136.04/136.92s`；worst-after节省`51.88s`、改善`27.48%`，超过10%且30s双门槛。|
|C Signal Feature / Diagnosis immutable bundle|本slice无实现文件|Feature=`373.42s`、Diagnosis setup=`353.55s`|只读审计确认约95%为跨文件重复Diagnosis/Micro-Search producer前缀，但现有session已覆盖、52 variants已是8/8 family最小完整矩阵、后缀无放大。安全收益需要content-addressed immutable bundle、atomic publish、absolute-path/live binding重验与tamper私有副本，超出低风险slice；裁决`REJECTED_CURRENT_SLICE_ARCHITECTURE_BOUNDARY`。|

Lane A必须保留原nodeid/断言与全部真实producer/validator，完整`SMOOTHED_METHOD_TO_VARIANT`、非空
sideways/recovery并满足受审sample floor；scorecard继续`INSUFFICIENT_EVIDENCE`、candidate null，
confirmation targets empty、gate=`CONTINUE_OBSERVATION`、binding=`NOT_REGISTERED`、switch=
`NO_ELIGIBLE_CANDIDATE`、recorded owner=`continue_observation`；weekly九步、due/update/classification lineage、
DQ与price/rates bytes、requested/actual range disclosure、clean rebuild PASS及binding byte tamper FAIL均不变。
160-day值只是test fixture执行规模，不是投资阈值；若真实validator证明覆盖不足即撤回，不通过放宽policy保留。
测试还必须直接读取canonical production backfill config并冻结`start=2022-12-01`、`end=latest_available`、
`min_history_days_before_first_rebalance=60`、`evaluation.min_observations_per_window=20`与
`regime_policy.min_sample_count=5`，防止把synthetic helper范围误写成生产或研究结论窗口。compact只需
满足既有test policy `test_paper_shadow_backfill_v1`的warmup=20、evaluation minimum=10、regime minimum=2；
测试必须同时冻结这组test值。160-day PASS只证明该synthetic test fixture覆盖完整契约，不构成production
样本充分性、策略有效性或投资结论证据。

Lane B必须保留Targeted 16 views、3 schema、3 cross-lineage、policy、price/rates source、resume与chronology
检查；Weight test-only 52 variants继续覆盖8/8 families，Targeted 6～12继续覆盖6/6 families，production
默认80与60～120不变。content/source/config/policy任一byte变化必须使key失效，FAIL/exception不得缓存，
不得跨worker共享可写store。Diagnostics虽有相似五个直接适配器，但归一后已比`004439Z`快约15.5%，且
历史外层session实验无收益；仅在Lane B运行时调用计数证明剩余独立重放后另行登记，不在S3I自动扩张。

开发期只跑上述exact same-command、expanded focused、Ruff、py_compile与静态不变量；两个lane分别完成
retain/revert裁决并合并后，才刷新共享manifests/hashes，运行architecture、contract及唯一一次自然边界
full。不得为单lane单独跑full，不把raw `042919Z`的全局膨胀冒充代码回退或收益；
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

S3I isolated before已在commit`77f394f0`、显式candidate `PYTHONPATH`、无其他Python/pytest进程、同一
`-n 16 --dist loadfile -q --durations=0`口径下顺序取得。Lane A Smoothed weekly=`1 passed /
228.52s`、call=`225.40s`；Lane B Targeted hardening=`1 passed / 188.80s`、call=`185.79s`。两者在
`042919Z` full中的raw call分别为`413.31/373.28s`，说明全量并发膨胀显著；retain/revert必须只用
上述isolated same-command before与至少两次无负载after，不把raw full差值计入局部收益。

Lane A首轮after实际为`2 passed / 209.62s`、重型call=`206.33s`。新增轻量profile契约node约3秒，
但重型call相对before仍只减少`19.07s / 8.46%`，整体wall减少`18.90s / 8.27%`；预登记门槛要求
worst-after至少10%，首轮结果已令最终worst不可能达标。因此执行确定性early-stop，不事后降低门槛、
不空跑第二次after，完整撤回Lane A两个文件并验证`git diff --exit-code`为byte-exact HEAD。该结果否定
当前“继续缩短synthetic authority窗口”的性价比，不否定后续对同一Smoothed共享producer DAG做更强
immutable fixture/cache架构；本slice不再实施该方向。

Lane B在首版legacy cache取得性能信号后没有把结果作为可接纳证据：独立安全审查发现paper validator
还会读取nested cache paths、Model Target / Daily Advisory semantic-selection inventories，以及Weight
validator会读取price-root两个optional DQ siblings。实现随后改为bounded、exact schema→edge resolver；
单/总snapshot bytes、JSON nodes、binding/cache/explicit/inventory/path/queue/depth均有固定parser-safety
预算，streaming read阻止`stat`后增长越界，canonical scheduled set拒绝重复入队与conflicting commitment。
Foundation与operations两类binding envelope均校验kind、artifact id、source directory、file commitment与
expected child snapshot；任一解析或预算失败只绕过cache并执行真实validator，不进入legacy/partial cache。

最终same-command两次正式after分别为`1 passed / 136.04s`、call=`132.77s`及`1 passed /
136.92s`、call=`133.66s`。按较慢值相对before wall=`188.80s`、call=`185.79s`计算，分别节省
`51.88s / 27.48%`与`52.13s / 28.06%`，因此裁决`RETAINED_THRESHOLD_PASS`。测试以五个真实adapter
入口证明unchanged seed/reuse只调用validator一次；独立Weight price root真实覆盖optional
MISSING→PRESENT与exact restore，Paper真实validator覆盖required/optional cache bytes、Model/Daily sibling
inventory、FAIL/exception不缓存与restore复用；实际relative cache path证明resolver连续两次绕过cache。
两轮独立静态审查均为P0=`0`、P1=`0`。expanded focused覆盖validation-session contract、Targeted v3、
Near-Miss A/B与hardening，共`82 passed / 1 skipped / 247.51s`；并发下Targeted Search v3仍为
`243.86s`长尾，登记为下一轮profile候选，不影响本lane isolated退出门槛。

S3I唯一一次自然integration-boundary full=`6,246 passed / 2 skipped / 642 warnings / 1,172.32s`，
artifact=`outputs/validation_runtime/full_20260718T070333Z/test_runtime_summary.json`。与相同`6,248`
nodes、`1,068`files、`16`workers、相同ordered collection hash且同为COMPLETE duration scheduler/no fallback的
`042919Z` full=`1,294.07s`相比，本次raw wall减少`121.75s / 9.41%`；Targeted hardening文件由
`373.292s`降至`210.925s`，减少`162.367s / 43.50%`。file P95由`123.075s`降至`114.284s`
（`-7.14%`），P99由`342.907s`降至`273.417s`（`-20.27%`），max由`589.003s`降至
`550.314s`（`-6.57%`）；worker busy mean由`1,281.967s`降至`1,160.736s`，CV仍约
`0.005%`，tail idle total/max=`0.079/0.009s`。目标文件的full内下降与isolated双run方向一致，足以闭合
S3I局部retention；但全局wall仅有一次after且其下降小于目标文件下降，继续记录
`stable_full_improvement_claimed=false`，不得把并发负载变化归因于单一代码修改。S3I状态闭合为
`COMPLETE_RUNTIME_TASK_CONTINUES`；下一轮优先只读审计full内`328.72s`的Targeted Search v3及其他新profile
尾部，不自动重开已因门槛不足撤回的Lane A，也不为选候选空跑第二次full。

### S3J：Targeted session边界、Diagnostics共享DAG与Smoothed local binding cache

2026-07-18 / owner批准继续尽可能降低full耗时，并明确高耗时单项可由多个agent按互斥范围并行。S3J以
commit=`f8377721`及`outputs/validation_runtime/full_20260718T070333Z/test_runtime_profile.json`为
authoritative source；三个只读审计lane完成后先冻结范围与退出门槛，再取得无其他Python/pytest负载的
isolated before。共享需求、task register、system flow、manifests、compatibility、最终集成与full继续由
coordinator独占，任何agent不得自行运行full或编辑共享文件。

| lane | 独占候选范围 | 最新full证据 | 冻结方案与退出门槛 |
|---|---|---:|---|
|A Targeted Search outer session|`tests/test_targeted_search_v3.py`|目标call=`328.72s`；最近三次file=`221.21/314.16/328.77s`|慢测外层增加既有同步artifact validation session，使helper返回后最终Targeted validator仍处于同一session并只复用S3I hardened PASS upstream；不缩小production/default 60～120 matrix，不删最终validator或六family断言。same-command一次before、至少两次after；worst-after wall相对before须同时改善≥10%且≥30s，否则byte-exact撤回。|
|B Diagnostics shared hardened DAG|新增`src/ai_trading_system/etf_portfolio/dynamic_v3_weight_search_validation_scope.py`；机械迁移`dynamic_v3_weight_search_targeted.py` resolver；修改`dynamic_v3_weight_search_diagnostics.py`及`tests/test_weight_search_diagnostics_hardening.py`|目标call=`343.63s`；最近三次file=`305.55/340.29/343.64s`，三次CV约`5%`且最近两次仅`0.99%`波动|不得在Diagnostics复制约700行resolver或反向import Targeted形成循环；先将S3I exact schema→typed-edge、resource-bounded、inventory/explicit-path/content-commitment resolver抽成共享模块并证明Targeted行为不变，再为Diagnostics四个本地validator及其scorecard/search-space adapters接入同session PASS-only cache。一次before、至少两次after；worst-after wall须改善≥10%且≥30s，独立安全审查P0/P1均为0，否则整lane撤回。|
|C Smoothed operations local-binding cache|零实现文件|四目标isolated before wall=`212.18s`、call总和=`802.34s`；latest full四call=`302.08/300.35/295.50/272.39s`|`REJECTED_REDUNDANT_CACHE_ALREADY_PRESENT`：静态反证确认operations两条local-binding路径末端已经进入同一同步session的PASS-only content cache；四目标也都有outer session。唯一direct uncached `_select_latest_valid`被显式ids短路，四目标不触发。机械叠加promotion wrapper只会形成双层fingerprint，预期收益为0，故不实施、不跑after。|

Lane A静态调用图确认`run_targeted_search_v3_fixture`自身虽有session decorator，但helper返回即关闭；随后测试直接
调用最终validator，导致coverage/near/scorecard及其weight/paper/model/DQ上游独立完整重放。外层session不缓存
Targeted最终view rebuild，只让其上游adapter继续使用S3I既有hardened fingerprint，因此生产代码与投资解释不变。

Lane B单node在52-variant完整8-family fixture后直接调用四类validator约37次并tamper 21 views；Diagnostics
模块目前没有validation cache。安全收益必须来自共享exact DAG resolver，覆盖Coverage/Cash/Near/Review/
Scorecard/Weight/Matrix/Search/Paper/Model、price/rates、DQ siblings、Model Target/Daily Advisory inventories、
binding commitments、path/bytes/nodes/queue/depth预算；任一schema、path、topology、commitment或预算异常必须
bypass cache并执行真实validator，FAIL/exception不得缓存。

Lane C反证链为`operations -> smoothed_readiness -> smoothed_evidence/hardening`及`operations ->
smoothed_readiness -> smoothed_method`，两条末端均已调用`cached_artifact_validation`；四目标测试与authority
helper的nested session也在同pid/thread/task内复用。其成本来自每个独立`tmp_path`首次构建326-day authority与
weekly九步，而非local binding cache miss；跨test共享需要immutable authority bundle/COW的新架构，超出本lane。
因此Lane C在零diff状态拒绝，不添加无收益测试或双层cache。三个lane均不得减少nodeid、skip、
xfail、all-view、schema、lineage、policy、chronology或data-quality检查；不得改变production matrix、Smoothed日期、
研究阈值、结论、paper/production/broker状态。实现完成后先顺序执行same-command after与expanded focused；多个
retained lane合并后只运行一次architecture、contract及自然integration-boundary full，
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

S3J isolated before已在commit=`f8377721`、显式candidate `PYTHONPATH`、无其他candidate Python/pytest
进程且未开始实现的状态下顺序取得。Lane A=`1 passed / 195.03s`、call=`191.94s`，因此worst-after
必须不高于`165.03s`才能同时满足10%与30s门槛。Lane B=`1 passed / 207.93s`、call=`204.73s`，
worst-after上限=`177.93s`。Lane C=`4 passed / 212.18s`，四call分别=`209.01/206.48/200.02/
186.83s`、总和=`802.34s`；worst-after wall上限=`182.18s`，四call总和上限=`681.99s`，且对应
单call不得超过`219.46/216.80/210.02/196.17s`。这些阈值在实现前冻结，不以after结果下调；full profile
值只用于选候选，绝不作为isolated局部收益分母。

Lane A两次无负载same-command after=`1 passed / 132.96s`（call=`129.25s`）与`1 passed /
133.63s`（call=`130.57s`）；按worst-after计算，wall节省`61.40s / 31.48%`，call节省
`61.37s / 31.97%`，超过冻结门槛，裁决=`RETAINED_THRESHOLD_PASS`。Lane B两次after=
`1 passed / 32.22s`（call=`29.07s`）与`1 passed / 32.49s`（call=`29.39s`）；worst-after
wall节省`175.44s / 84.37%`，call节省`175.34s / 85.64%`，并通过两轮独立审查
P0/P1=`0/0`，裁决=`RETAINED_THRESHOLD_PASS`。共享resolver保留Targeted私有兼容alias；未知schema、
预算、path/topology/commitment或resolver异常仍直接执行真实validator，FAIL与validator exception不缓存。
覆盖session、Diagnostics四类public validator、Targeted及原hardening/tamper链的expanded focused=
`87 passed / 1 skipped / 126.14s`；唯一skip为Windows无`os.fork`条件用例。S3J现为`VALIDATING`，
architecture/contract/manifests/compatibility/source hashes与唯一自然integration-boundary full仍待闭合，
不据局部或单次full声明稳定全局提速；`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。

S3J正式集成门禁已闭合。generated manifests=`948 modules / 1,126 test/support files`，module/test orphan、
specific overlap、dependency/direct-writer violation均为0；compatibility/deprecation focused=`8 passed /
12.67s`，architecture=`344 passed / 57.84s`、artifact=`outputs/validation_runtime/
architecture-fitness_20260718T090136Z/test_runtime_summary.json`，contract=`236 passed / 44.40s`、
artifact=`outputs/validation_runtime/contract-validation_20260718T090239Z/test_runtime_summary.json`。唯一自然
full=`6,246 passed / 2 skipped / 643 warnings / 1,079.37s`，artifact=`outputs/validation_runtime/
full_20260718T090446Z/test_runtime_summary.json`，profile=`outputs/validation_runtime/full_20260718T090446Z/
test_runtime_profile.json`。exact node/file/worker=`6,248/1,068/16`，ordered/set hashes与S3I一致，scheduler=
COMPLETE/applied/no-fallback，telemetry/performance均PASS。

相对S3I `070333Z`，runner wall=`1,172.32 -> 1,079.37s`（`-92.95s / -7.93%`），file worker-s=
`18,571.776 -> 17,080.367s`（`-1,491.409s / -8.03%`），P95=`114.284 -> 100.407s`
（`-12.14%`），P99=`273.417 -> 259.553s`（`-5.07%`）；max因未修改的Layer1文件由`550.314`
增至`567.530s`（`+3.13%`）。S3J Targeted file=`328.770 -> 47.975s`（`-85.41%`），Diagnostics=
`343.639 -> 63.284s`（`-81.58%`），Targeted hardening=`210.925 -> 79.060s`（`-62.52%`）；三个
相关文件合计约减少`693.0 worker-s`，且原nodeids与tamper门禁完整PASS。因仍只有一次S3J after full，
`stable_full_improvement_claimed=false`。S3J状态闭合为`COMPLETE_RUNTIME_TASK_CONTINUES`；下一轮仅先并行
只读审计Decision leaf、Evaluation hardened-upstream与Execution comparison三个互斥候选，不自动启动实现、
EB1、下一callback或ARCH-005，`production_effect=none`。

### S3K：Decision leaf session、Evaluation upstream reuse与Execution invariant hoist

2026-07-18 / 以commit=`6c66f78a`及`outputs/validation_runtime/full_20260718T090446Z/
test_runtime_profile.json`为authoritative source完成三个互斥只读审计。开发期不把`full`当作单项内循环：
每lane只顺序执行exact-node isolated before/after与focused contract，仅在所有retained lane合并的自然
integration boundary执行一次architecture、contract和`full`。日常研究仅执行必要的data-quality gate与
实际涉及链路；不把daily/weekly/monthly研究调度自动映射成pytest `full`。

| lane | 独占候选范围 | 最新full证据 | 预登记最小方案与退出门槛 |
|---|---|---:|---|
|A Decision leaves|`tests/test_formal_method_auto_plan.py`；`tests/test_weight_method_promotion_gate.py`|Formal file/call=`259.553/259.551s`；Gate=`229.607/229.605s`|仅给两个现有test function增加`with_artifact_validation_session`，不装饰helper、不删最终validator、不缩减fixture。两个node分别取before与两次after，各自worst-after须`<= min(0.90 * before, before - 30s)`，不得用合并wall隐藏某个node未达标。|
|B Evaluation hardened upstream|`src/ai_trading_system/etf_portfolio/dynamic_v3_weight_search_evaluation.py`；`tests/test_weight_search_evaluation_hardening.py`|file/call=`262.962/262.960s`，近三次稳定为`262.96/258.19/256.24s`|测试延长既有validation session；`_validated_backfill`只调用S3J neutral API，Evaluation本地adapter复用Scorecard PASS，Matrix继续真实验证。不改S3J resolver/Targeted/Diagnostics，不声称Matrix是supported root。worst-of-2须同时改善≥10%且≥30s。|
|C Execution comparison invariants|`src/ai_trading_system/execution_semantics.py`；`tests/test_execution_semantics.py`|target call=`236.839s`、file=`243.386s`；近四次call CV约`0.442%`|第一阶段仅将与strategy/policy无关的QQQ target/benchmark metrics移出内层loop，使每build的QQQ benchmark从65次降为1次；不缓存comparison、rows或DQ payload。worst-of-2须同时改善≥10%且≥30s；若未达标则byte-exact撤回，rows-only session cache需重新登记完整bytes fingerprint和DQ等价门禁后才能实施，不在本lane自动扩张。|

Lane A只让同test内已稳定PASS的transitive upstream复用；Gate预计tracked validator=`122 -> 62`、
DQ=`39 -> 21`，Formal=`145 -> 63`、DQ=`45 -> 21`。两个final Gate/Formal validator仍真实执行，
`auto_apply=false`、`implemented=false`、`production_effect=none`及全部stage/decision断言不变。

Lane B的neutral resolver supported roots不含Matrix/Robustness/Adaptive；收益必须只来自Backfill/Scorecard的
PASS-only reuse及其间接消除的Matrix重放。同内容两次底层验证必须为1次，FAIL/exception各两次必须
为2次；source/config/price/rates变化必须miss/fail closed，14 views、3 schema、cross-lineage、
branch-decision tamper断言不得减少。

Lane C仅是纯等价loop-invariant hoist：不改行顺序、浮点计算输入、策略/政策数量、DQ调用、
JSON/Markdown schema或研究结论。需固定时间字段后证明65-row canonical payload及五个公开builder输出
byte-equivalent，并证明QQQ benchmark metrics=`1/build`。禁止使用path/mtime、当前不含secondary checksum的
DQ payload或可变rows引用建立缓存。

三个lane实现前必须在无其他candidate Python/pytest负载下顺序取exact-node isolated before，
然后才允许按互斥文件范围并行实现。任一lane未达预登记门槛或安全审查出现P0/P1，必须
byte-exact撤回；不减少nodeid/skip/xfail/DQ/PIT/source/lineage/all-view/schema/tamper，不改研究阈值、
promotion、paper-shadow、production或broker状态。Refined Method的transactional shared fixture因需完整树恢复且风险更高，
仅保留为后续候选，不进入S3K。`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。

S3K isolated before已在commit=`6c66f78a`、显式candidate `PYTHONPATH`、无其他candidate Python/pytest
进程且未开始代码实现的状态下顺序取得：Formal=`1 passed / 151.84s`（call=`148.59s`），
Gate=`1 passed / 119.22s`（call=`116.07s`），Evaluation=`1 passed / 150.26s`（call=
`147.10s`），Execution=`1 passed / 168.43s`（call=`163.09s`）。因此两次after的worst wall上限
分别冻结为Formal≤`121.84s`、Gate≤`89.22s`、Evaluation≤`120.26s`、Execution≤`138.43s`。
这些上限在实现前固定，不得以after抖动或full内并发值事后放宽；四个node均必须保持原nodeid与全部
断言。S3K现进入`IMPLEMENTING`，只允许按上表互斥文件范围并行编辑；所有Python/pytest计时仍由
coordinator顺序执行。

S3K isolated after已完成局部收益裁决。Formal两次after=`22.84/23.13s`（call=`19.53/
19.83s`），按worst相对before节省`128.71s / 84.77%`；Gate=`22.70/22.74s`（call=
`19.46/19.44s`），节省`96.48s / 80.92%`；Evaluation=`29.99/30.04s`（call=
`26.75/26.80s`），节省`120.22s / 80.01%`。三者均远低于实现前冻结上限，裁决=
`RETAINED_THRESHOLD_PASS`。

Execution纯QQQ invariant hoist的第一次after=`173.24s`（call=`167.88s`），高于before=
`168.43s`且远高于上限`138.43s`。因裁决取worst-of-2，第一次已使该lane在数学上无法
达标，故不再浪费第二次计时；production/test两文件已byte-exact撤回HEAD，裁决=
`REJECTED_THRESHOLD_MISS`。这证明QQQ benchmark并非当前主长尾，不引入未登记的rows/DQ cache；
若后续重开，必须先完成secondary source bytes、全部config/policy、DQ等价与deep-copy隔离的新安全合同。
S3K现进入`VALIDATING`；expanded focused、独立P0/P1审查、manifests/compatibility/deprecation/source hashes与
唯一自然integration-boundary architecture/contract/full仍待闭合，`stable_full_improvement_claimed=false`、
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

S3K pre-full集成门禁与唯一自然full已PASS。generated manifests=`948 modules / 1,126 test/support
files`，orphan/overlap/dependency/direct-writer violation均为0；compatibility/deprecation focused=`13 passed /
13.08s`。architecture=`344 passed / 59.64s`，artifact=`outputs/validation_runtime/
architecture-fitness_20260718T103046Z/test_runtime_summary.json`；contract=`236 passed / 44.76s`，
artifact=`outputs/validation_runtime/contract-validation_20260718T103154Z/test_runtime_summary.json`。

唯一full=`6,246 passed / 2 skipped / 642 warnings / 1,014.05s`，artifact=`outputs/validation_runtime/
full_20260718T103249Z/test_runtime_summary.json`，profile=`outputs/validation_runtime/full_20260718T103249Z/
test_runtime_profile.json`。exact node/file/worker=`6,248/1,068/16`，ordered/set hashes与S3J完全一致，
scheduler=`COMPLETE/applied/no-fallback`，telemetry/performance均PASS。相对S3J `090446Z`，runner wall=
`1,079.37 -> 1,014.05s`（`-65.32s / -6.05%`），file worker-s=`17,080.367 -> 16,032.724s`
（`-1,047.642s / -6.13%`），P95=`100.407 -> 79.219s`（`-21.10%`），P99=
`259.553 -> 249.819s`（`-3.75%`）。max因未修改的Layer1文件从`567.530`增至
`578.539s`（`+1.94%`），仍是最大长尾。

Formal/Gate/Evaluation full file分别为`259.553 -> 34.844s`（`-86.58%`）、
`229.607 -> 34.232s`（`-85.09%`）、`262.962 -> 48.350s`（`-81.61%`），三文件合计
约减少`634.695 worker-s`；Execution因已撤回，`243.386 -> 242.334s`仅属噪声水平。
worker busy mean=`1,067.523 -> 1,002.045s`，CV仍为近零的`0.0077%`，tail idle total/max=
`0.079/0.010s`。因仅有一次S3K after full，`stable_full_improvement_claimed=false`。

S3K post-full tracked-state closeout已闭合。首次architecture在
`architecture-fitness_20260718T105640Z`真实报告`343 passed/1 failed/97.51s`；module/test manifest逐项
SHA均无漂移，根因是唯一full完成后补写`docs/system_flow.md`性能证据，而aggregate shadow index仍绑定
补写前hash。按正式生成器刷新后，第二次architecture-fitness的compatibility baseline assertion又准确捕获
baseline仍绑定旧aggregate hash；
统一刷新active source hash后architecture=`344 passed/61.23s`，artifact=`outputs/validation_runtime/
architecture-fitness_20260718T110442Z/test_runtime_summary.json`。post-full contract初次复验=`236 passed/
45.87s`，最终tracked-state contract与compatibility/deprecation focused result由compatibility baseline记录。
S3K状态为`COMPLETE_RUNTIME_TASK_CONTINUES`，next owner转为S3L candidate coordinator；本批不运行第二次
full，不解锁EB1、下一callback或ARCH-005，`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。

### S3L：Confirmation module session与Expanded Search test session

S3L以commit=`27ce25ae`和`full_20260718T103249Z`为权威输入。只读profile审计在排除
Layer1不稳定撤回、Execution纯QQQ拒绝项及已完成Formal/Gate/Evaluation/Targeted/Diagnostics后，
选择三个互斥lane：

|lane|权威full file time|唯一允许编辑范围|候选边界|必须保留|
|---|---:|---|---|---|
|Confirmation Evaluate|`247.72s`|`tests/test_confirmation_evaluate.py`|仅让module-scoped `evaluation_bundle`从fixture build到teardown处于一个既有同步PASS-only validation session|5个既有node、5类output tamper、naive cutoff、live progress drift、FAIL真实重验|
|Confirmation Progress|`231.84s`|`tests/test_confirmation_progress.py`|仅让module-scoped `progress_bundle`从fixture build到teardown处于一个既有同步PASS-only validation session|4个既有node、5类output tamper、naive cutoff、live evidence-source drift、FAIL真实重验|
|Weight Expanded Search|`206.94s`|`tests/test_weight_expanded_search.py`|仅在现有单test function外延同步PASS-only validation session，使S3K已支持的stable upstream可在同一调用链复用|expanded Matrix与final Matrix validator真实执行、variant上限200、DQ状态、完整业务断言|

Confirmation helper已有nested function session，但module fixture外层不存在持续session，因此每个helper返回后会销毁
stable PASS state；S3L不得修改helper或production module，只允许在module fixture generator的`yield`期间保持外层
session。Confirmation output/live-source validator本身不得进入cache，必须逐次真实执行；只有eligible stable upstream
plan的PASS允许在session内按transitive fingerprint复用，plan bytes变化必须miss；`FAIL`或exception不得缓存。
Progress live-source drift仍为该artifact fixture的最后一个drift node且不允许被session掩盖。Expanded lane不得把
Adaptive、rewritten Matrix、expanded Matrix或DQ加入未登记resolver，也不得修改production variant count、fixture数据、
nodeid、skip/xfail或断言。

三lane分别在无其他candidate Python/pytest负载下顺序执行同一exact-file命令，先取得before，再冻结
`worst-of-2 after <= min(0.90 * before, before - 30s)`；Confirmation同时报告setup/call分布，任一tamper/
drift不再真实fail closed、出现P0/P1或未达门槛即byte-exact撤回该lane。单lane不跑full；只有保留lane整批合并、
expanded focused与独立审查通过后才允许一个自然integration-boundary architecture/contract/full。
`stable_full_improvement_claimed=false`、`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`，且不解锁EB1、下一callback或ARCH-005。

S3L isolated before已在commit=`27ce25ae`、显式candidate `PYTHONPATH`且无其他Python/pytest负载下顺序
闭合。Confirmation Evaluate=`5 passed / pytest 153.77s / wall 155.34s`，setup=`42.11s`、主要
call合计=`108.54s`；Confirmation Progress=`4 passed / pytest 166.68s / wall 167.21s`，setup=
`47.75s`、主要call合计=`115.83s`；Weight Expanded Search=`1 passed / pytest 116.37s / wall
116.88s`，call=`113.25s`。按`min(0.90 * before, before - 30s)`预冻结worst-of-2 wall上限分别为
Evaluate≤`125.34s`、Progress≤`137.21s`、Expanded≤`86.88s`，不得以after抖动或full并发值事后放宽。
S3L现进入`IMPLEMENTING`；三个互斥agent只可编辑各自登记的单一test file，不运行Python/pytest、不编辑
共享docs/manifests。所有after与focused由coordinator顺序执行。

S3L isolated after已完成局部收益裁决，三lane均`RETAINED_THRESHOLD_PASS`。Evaluate两次after wall=
`54.52/53.35s`（pytest=`53.98/52.78s`，setup=`46.24/44.91s`，主要call=`4.82/4.88s`），
按worst相对before节省`100.82s / 64.90%`；Progress=`52.23/52.39s`（pytest=`51.73/
51.85s`，setup=`43.99/44.08s`，主要call=`4.78/4.80s`），节省`114.82s / 68.67%`；
Expanded=`28.92/29.40s`（pytest=`28.37/28.85s`，call=`25.29/25.84s`），节省
`87.48s / 74.85%`。三条worst均显著低于预冻结上限，node数仍为`5/4/1`；Confirmation
tamper/live-drift节点继续PASS的含义是其内部FAIL断言真实成立，Expanded final Matrix validator仍在原位置。
S3L现进入`VALIDATING`；expanded focused、三轮交叉只读审查、manifests/compatibility/deprecation/source hashes
及整批唯一自然architecture/contract/full仍待闭合。`stable_full_improvement_claimed=false`、
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

S3L expanded focused=`136 passed / 2 skipped / 239.92s`，两项skip均为Windows无`os.fork`的既有
条件用例；覆盖validation-session基础设施、Confirmation plan/targets/progress/evaluate/dashboard/weekly及
Weight Matrix/Backfill/Scorecard/Robustness/Adaptive/Expanded/Evaluation hardening。三轮交叉只读审查
P0/P1/P2=`0/0/0`：每个agent审查另一lane，确认Confirmation output/live validator始终逐次真实执行，只有
eligible upstream plan PASS可按transitive fingerprint复用；decorator保留pytest fixture签名，unsupported root仍
bypass，Expanded final Matrix validator未被缓存或删除。下一步由
coordinator刷新system flow、manifests、compatibility/deprecation/source hashes，再执行pre-full architecture/
contract与整批唯一自然full。

S3L pre-full compatibility/deprecation focused=`8 passed / 13.35s`；architecture-fitness=`344 passed /
59.03s`，runtime artifact=`outputs/validation_runtime/architecture-fitness_20260718T120042Z/
test_runtime_summary.json`；contract-validation=`236 passed / 44.09s`，runtime artifact=`outputs/
validation_runtime/contract-validation_20260718T120418Z/test_runtime_summary.json`。集成只读审查P0/P1/P2=
`0/0/2`，两项均为文档一致性：task-register主行状态陈旧、Confirmation缓存机制表述不精确；代码行为无
P0/P1，现已修正文档与登记，刷新tracked hashes后复验门禁并运行整批唯一自然full。

S3L最终tracked-state pre-full复验=`8 passed / 13.04s`、architecture=`344 passed / 58.10s`
（`outputs/validation_runtime/architecture-fitness_20260718T121347Z/test_runtime_summary.json`）、contract=
`236 passed / 44.68s`（`outputs/validation_runtime/contract-validation_20260718T121452Z/
test_runtime_summary.json`）。唯一自然full=`6,246 passed / 2 skipped / 642 warnings / 979.11s`，
runtime summary/profile分别为`outputs/validation_runtime/full_20260718T121649Z/test_runtime_summary.json`与
`test_runtime_profile.json`；exact `6,248 nodes / 1,068 files / 16 workers`、ordered/set collection hashes
与S3K一致，scheduler=`COMPLETE/applied/no-fallback`，telemetry/performance=`PASS`，pytest outcome
authoritative且未override。相对S3K `1,014.05s`，wall减少`34.94s / 3.45%`；file worker-s=
`16,032.72 -> 15,481.94s`（`-550.78s / -3.44%`），P95=`79.22 -> 74.85s`（`-5.51%`）、
P99=`249.82 -> 243.27s`（`-2.62%`），max=`578.54 -> 578.68s`（`+0.02%`）。Evaluate=
`247.72 -> 83.26s`（`-66.39%`）、Progress=`231.84 -> 78.17s`（`-66.28%`）、Expanded=
`206.94 -> 53.15s`（`-74.31%`），各自node数仍`5/4/1`。只有一次S3L full，不能形成稳定全局提速声明；
post-full compatibility/architecture/contract exact artifacts与最终S3L状态以compatibility baseline为准。
下一候选从新profile选择，S4 trigger provenance仍待独立实现；EB1、下一callback、ARCH-005继续锁定，
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

### S3M：Weight Interpretation / Cluster outer session

S3M权威base=`ee68b98f`、source profile=`outputs/validation_runtime/full_20260718T121649Z/
test_runtime_profile.json`。在排除已闭合/已撤回lane后，只登记两个文件互斥、test-only且不扩张resolver的候选：

|lane|121649Z file time|唯一允许编辑范围|候选机制|必须保留|
|---|---:|---|---|---|
|Top Candidate Interpretation|`120.734s / 1 node`|`tests/test_weight_top_candidate_interpretation.py`|仅给现有test function增加`with_artifact_validation_session`，让helper build与final validator共享eligible stable upstream PASS|public Interpretation validator逐byte真实执行；完整views/lineage/DQ/断言|
|Candidate Cluster|`113.515s / 1 node`|`tests/test_weight_candidate_cluster.py`|仅给现有test function增加同一outer session，让helper build与final validator共享eligible stable upstream PASS|public Cluster validator逐byte真实执行；完整views/lineage/DQ/断言|

两文件在`103249Z`分别为`125.363/112.442s`，两次自然profile合计基本稳定；但full并发值只用于候选优先级，
不能替代isolated before。coordinator必须在无其他candidate Python/pytest负载下顺序取得exact-file baseline B，
实现前冻结`worst-of-2 after <= min(0.90 * B, B - 30s)`，同时记录call；任一node/skip/xfail/断言减少、
final validator被缓存、FAIL/exception被复用、unsupported root不再bypass、收益门槛失败或P0/P1即byte-exact撤回。
两个agent只编辑各自单test file，不运行Python/pytest、不编辑共享docs/manifests；after、expanded focused与最终
architecture/contract/full由coordinator顺序执行。Refined剩余`216.72s`主要来自独立tamper transactional重建，
需要try/finally exact restore、tree fingerprint与session隔离，故本批明确不实施；Equal Risk Growth Tilt/
Restart合计约`515.31s`只并行只读审计。`stable_full_improvement_claimed=false`、`production_effect=none`，
EB1、下一callback、ARCH-005仍锁定。

S3M isolated before已在commit=`ee68b98f`、显式candidate `PYTHONPATH`且无其他Python/pytest负载下顺序
闭合。Top Candidate Interpretation=`1 passed / pytest 79.21s / wall 79.90s`，call=`76.02s`；Candidate
Cluster=`1 passed / pytest 69.00s / wall 70.68s`，call=`65.69s`。实现前已冻结worst-of-2 wall门槛：
Interpretation≤`49.90s`、Cluster≤`40.68s`，不得事后放宽。S3M现进入`IMPLEMENTING`；两个互斥agent
只可编辑各自登记的单test file，不运行Python/pytest、不编辑共享docs/manifests，所有after/focused由coordinator
顺序执行。

S3M双after在相同无负载口径下闭合，两条lane均保留并进入`VALIDATING`：

|lane|after-1|after-2|worst vs before|裁决|
|---|---:|---:|---:|---|
|Top Candidate Interpretation|`pytest 24.19s / wall 24.78s / call 20.82s`|`pytest 23.39s / wall 23.97s / call 20.14s`|`79.90 -> 24.78s`，`-55.12s/-68.99%`|`RETAINED`，低于`49.90s`|
|Candidate Cluster|`pytest 23.41s / wall 23.97s / call 20.16s`|`pytest 23.22s / wall 23.76s / call 19.93s`|`70.68 -> 23.97s`，`-46.71s/-66.09%`|`RETAINED`，低于`40.68s`|

两文件仍各收集`1`个node；未改函数名、fixture、helper、断言、final public validator参数、production resolver
或supported roots。coordinator接手expanded focused、独立交叉审查、manifests/compatibility/deprecation/source
hashes及整批唯一一次natural full；单lane不补跑full。

S3M expanded focused=`89 passed / 1 skipped / pytest 99.62s / wall 100.17s`，使用`-n 16 --dist
loadfile`；skip为Windows无`os.fork`的既有session隔离条件用例。coverage显式包含
`test_artifact_validation_session.py`、两条目标文件，以及Weight Matrix/Backfill/Scorecard/Robustness/
Adaptive/Expanded/Evaluation/Follow-up hardening链。两个实现agent分别对另一lane完成独立静态交叉审查，
结论均为`P0/P1/P2=0/0/0`；decorator同步生命周期、`functools.wraps` nodeid/fixture保留、异常`finally`
清理、final public validator真实执行及production/helper/resolver不变均获确认。

S3M首轮pre-full共享门禁已PASS：`architecture_devex.py generate/validate`报告`948 modules / 1,126
test-support files / 0 orphan / 0 overlap / 0 violations`；compatibility/deprecation focused=`8 passed /
14.16s`；architecture-fitness=`344 passed / 60.72s`，artifact=`outputs/validation_runtime/
architecture-fitness_20260718T132125Z/test_runtime_summary.json`；contract-validation=`236 passed /
45.66s`，artifact=`outputs/validation_runtime/contract-validation_20260718T132233Z/
test_runtime_summary.json`。集成只读审查P0/P1/P2=`0/0/1`，唯一P2是requirements/task register把已
完成门禁仍写为下一步；代码、manifest、73个active source hashes与deprecation inventory均无mismatch。
现先修正文档并刷新hash，再复验current tracked state；整批唯一natural full仍未运行。

P2修正后的current tracked-state门再次PASS：compatibility/deprecation focused=`8 passed / 14.27s`，
architecture-fitness=`344 passed / 60.57s`，contract-validation=`236 passed / 46.67s`；随后只运行一次
S3M natural full。结果=`6,246 passed / 2 skipped / 642 warnings / pytest 990.51s / wall 991.64s`，
runtime summary/profile=`outputs/validation_runtime/full_20260718T133435Z/test_runtime_summary.json`与
`test_runtime_profile.json`，SHA-256分别为`5dce7e459731a17d367ada2e69dfad3fccf2cf96c99909ef5a6e346c7192cb8d`/
`d43a4b88e404c8775342d0b078258cb56155cfd17f0683edcae48814cf6b91f5`。exact `6,248 nodes /
1,068 files / 16 workers`、ordered/set collection SHA与S3L一致，scheduler=`COMPLETE/applied/no-fallback`、
telemetry/performance=`PASS`、pytest outcome authoritative且未override。

两目标在full中的局部收益明确成立：Top Interpretation=`120.734 -> 30.463s`（`-90.271s/-74.77%`），
Candidate Cluster=`113.515 -> 32.895s`（`-80.619s/-71.02%`），node数仍各`1`；合计=
`234.249 -> 63.358 worker-s`（`-170.891s/-72.95%`）。但相对S3L，runner wall=`979.11 ->
991.64s`（`+12.53s/+1.28%`）、file worker-s=`15,481.941 -> 15,672.453s`（`+190.512s/
+1.23%`），nearest-rank P95=`74.853 -> 76.122s`（`+1.70%`）、P99=`243.270 -> 251.115s`
（`+3.22%`）、max=`578.682 -> 589.598s`（`+1.89%`）。排除两目标后非目标file worker-s增加
`361.402s/+2.37%`；多个Smoothed长尾同步回升约`6%～7%`，而worker busy CV仍约`0.00884%`、
tail idle total仅`0.222s`，没有scheduler失衡证据。因此保留局部优化但不把单次full写成全局赢家，
`stable_full_improvement_claimed=false`；不运行第二次full，post-full tracked-state门仍待闭合。

S3M post-full第一轮tracked-state门已PASS：compatibility/deprecation focused=`8 passed / pytest
13.44s`，architecture-fitness=`344 passed / 68.71s`（`outputs/validation_runtime/
architecture-fitness_20260718T140944Z/test_runtime_summary.json`），contract-validation=`236 passed /
45.44s`（`outputs/validation_runtime/contract-validation_20260718T141058Z/test_runtime_summary.json`）。
full summary/profile/log/Reader Brief、collection、scheduler、telemetry、manifests、73 active sources、aggregate
bindings与deprecation inventory的独立只读审查均为0 mismatch。S3M转为
`COMPLETE_RUNTIME_TASK_CONTINUES`。最终审查修正raw profile统一舍入、EB1 phase-exit/owner授权语义和
worktree attribution 11/11路径覆盖；归属focused首轮按预期捕获未登记的新status枚举，随后保留既有schema
status并增加显式S3M staging authority；最终独立审查再为该authority补齐task/increment/status/base/path-set
逐字段契约断言，修正后focused/architecture/contract=`9/344/236 passed`。
最终tracked-state source hashes与exact artifacts以compatibility baseline为准，不运行第二次full。next owner=
S3N candidate coordinator基于`133435Z`新profile登记下一批；EB1、下一callback与ARCH-005仍锁定，
`production_effect=none`。

### S3N：Adaptive 与 Equal Risk 尾部有界收尾

2026-07-19 / owner复盘后继续推进一次有界低/中风险收尾，不启动Smoothed immutable authority/COW
架构。权威base=`13d85f1e`，source profile=`outputs/validation_runtime/full_20260718T133435Z/
test_runtime_profile.json`；该profile为`6,248 nodes / 1,068 files / 16 workers / 991.64s`，调度tail idle
仅`0.222s`，因此本批只减少真实test builder/CLI/validator重复工作，不再调整文件顺序。

|lane|profile证据|唯一允许范围|预登记机制与安全边界|
|---|---:|---|---|
|A Weight Adaptive|`tests/test_weight_adaptive_branch.py`=`116.378s / 1 node`|仅该test file|审计既有test-function outer validation session；public Adaptive validator必须逐次真实调用，branch snapshot、live commitments、DQ、lineage、nodeid与断言不变，不新增supported root。|
|B Equal Risk Restart|`tests/test_equal_risk_growth_research_restart.py`=`251.115s / 5 nodes`，其中roadmap-v2 node=`208.875s`|仅该文件的`test_roadmap_v2_real_result_convergence_builders_and_cli` test body|只允许消除该node内已由同次真实suite/CLI生成artifact后的重复builder/CLI工作；至少一次真实CLI、最终gate、安全字段、artifact bytes与所有断言必须保留，不改production helper。|
|C Equal Risk Tilt|`tests/test_equal_risk_growth_tilt.py`=`271.338s / 5 nodes`|仅五个既有test bodies；不得改文件尾部helper/cross-import或production source|只允许test-body内复用同node已生成的immutable payload/artifact，真实CLI/validator与research-only、DQ、AI regime、paper-shadow/production/broker safety断言保持；不得缩减candidate family或fixture日期。|

协调者先在clean base且无其他candidate Python/pytest负载下顺序取得A exact node、B roadmap-v2 exact node、
C whole-file isolated before；之后分别冻结`worst-of-2 after <= min(0.90 * B, B - 30s)`，不得按after结果
放宽。三agent只有在baseline登记后才能按互斥文件编辑，且不得自行运行Python/pytest或编辑共享docs。
任一lane未达门槛、减少nodeid/skip/xfail/断言、弱化真实CLI/validator/DQ/安全边界或出现P0/P1即
byte-exact撤回。coordinator顺序执行after与expanded focused；所有保留lane合并后才运行一次自然
architecture/contract/full。理论三文件合计`638.83 worker-s`，即使局部节省50%也只对应约`19.96s`
full容量，故裁决以isolated同命令证据为主，不要求单次full突破约35s自然波动。S3N后停止继续叠加
小型session wrapper，再决定是否单独登记Smoothed架构研究；`stable_full_improvement_claimed=false`、
`strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`，EB1、下一callback与
ARCH-005仍锁定。

S3N isolated before已在base=`13d85f1e`、显式candidate `PYTHONPATH`且无其他Python/pytest负载下顺序
闭合，三条命令均为`-n 16 --dist loadfile -q --durations=0`且无skip/failure。A Adaptive exact node=
`1 passed / pytest 59.29s / wall 59.889s / call 56.16s`，冻结worst-of-2 wall上限=`29.889s`；B Restart
roadmap-v2 exact node=`1 passed / pytest 127.66s / wall 129.064s / call 122.51s`，上限=`99.064s`；
C Tilt whole-file=`5 passed / pytest 147.57s / wall 148.079s`，五call=`57.43/47.19/32.59/5.11/0.16s`，
上限=`118.079s`。三上限均为`min(0.90*B,B-30s)`，不得事后放宽。当前进入`IMPLEMENTING`前安全点；
agent只能按A/B/C互斥test file实施已审计的最小差异，不自行运行Python/pytest，所有after与裁决由
coordinator顺序执行。

S3N三条lane的双after已在同样无负载、显式candidate `PYTHONPATH`和相同pytest命令下闭合，
均保留且不放宽冻结门槛。A Adaptive=`17.995/17.903s` worst wall，相对`59.889s`减少
`41.894s/-69.95%`；B Restart roadmap-v2=`78.485/78.410s`，相对`129.064s`减少
`50.579s/-39.19%`；C Tilt whole-file=`92.541/92.236s`，相对`148.079s`减少
`55.538s/-37.51%`，两轮均`5 passed`。三条isolated worst合计减少`148.011s`；该值只是同命令
局部证据，不等同于full wall收益。A/B独立只读交叉审查均为`P0/P1/P2=0/0/0`；C交叉审查、
expanded focused、manifests/compatibility/deprecation/source hashes、architecture/contract与本批唯一natural full
尚待闭合。当前状态=`VALIDATING`，next owner=integration coordinator，`production_effect=none`。

S3N expanded focused已按`-n 16 --dist loadfile`闭合为`97 passed / 1 skipped / pytest 108.37s /
wall 108.890s`；唯一skip为Windows缺少`os.fork`的既有条件用例。覆盖三条目标lane、
validation-session基础设施和Weight Foundation→Scorecard→Robustness→Adaptive→Expanded→
Evaluation/Follow-up hardening链。三条lane的独立只读交叉审查现均为
`P0/P1/P2=0/0/0`；C审查特别确认5个nodeid、四个真实CLI、原业务/安全断言、同node
JSON provenance、`ai_after_chatgpt`与`production_effect=none`均保持。三目标Ruff和`py_compile`
均PASS；尚需刷新共享tracked artifacts并执行architecture/contract和唯一natural full。

S3N pre-full generated manifests=`948 modules / 1,126 test-support files / 0 violations`，compatibility/
deprecation/attribution focused=`9 passed / wall 12.799s`，architecture=`344 passed / wall 49.83s`，
contract=`236 passed / wall 36.56s`。本批唯一natural full=`6,246 passed / 2 skipped / 643 warnings /
pytest 903.03s / wall 904.14s`，exact `6,248 nodes / 1,068 files / 16 workers`，ordered/set collection
hash与S3M一致，scheduler=`COMPLETE/applied/no-fallback`、telemetry/performance=`PASS`、tail idle total=
`0.108s`。不运行第二次full。

三目标full file time分别为Adaptive `116.378→28.473s`（`-75.53%`）、Restart
`251.115→167.742s`（`-33.20%`）、Tilt `271.338→117.175s`（`-56.82%`），合计
`638.831→313.389 worker-s`（`-325.442s/-50.94%`），node数仍`1/5/5`。相对S3M，本次总
wall/file worker-s=`-87.50s/-8.82%`与`-1,382.855s/-8.82%`，nearest-rank P95/P99/max=
`76.122/251.115/589.598→66.892/211.728/481.740s`（`-12.12%/-15.68%/-18.29%`）。但非目标
文件也减少`1,057.413 worker-s/-7.03%`，因此只声明三目标局部收益与本次overall
single-run improvement，不将非目标波动归因于S3N，`stable_full_improvement_claimed=false`。当前
进入`POST_FULL_TRACKED_STATE_VALIDATING`；next owner=closeout coordinator刷新final docs/authority/manifests/
source hashes并复验focused/architecture/contract，`strategy_logic_changed=false`、`cached_data_mutated=false`、
`production_effect=none`。

S3N第一轮post-full tracked-state focused/architecture/contract=`9/344/236 passed`，architecture与contract
wall=`65.41/43.63s`，runtime artifacts分别为`architecture-fitness_20260718T160951Z`和
`contract-validation_20260718T161104Z`。据此current staging authority转为
`COMPLETE_RUNTIME_TASK_CONTINUES`；final docs/authority/test manifest/aggregate/source hashes刷新后再对该最终
tracked state执行focused/architecture/contract复验，不跑第二次full。S3N后停止叠加小型
wrapper/session微优化，也不自动启动Smoothed COW、S4、EB1、下一callback或ARCH-005；next owner=
project owner / performance-review coordinator评估当前收益、剩余瓶颈与风险后选择后续方向。

S3N final tracked-state focused/architecture/contract=`13/344/236 passed`，focused pytest与architecture/
contract wall=`12.11/50.37/36.09s`，architecture/contract artifacts=
`architecture-fitness_20260718T163403Z`/`contract-validation_20260718T163513Z`。77个active sources、
12个changed tracked paths与12个declared paths
完全对齐，3个excluded user docs未改，deprecation inventory=`FRESH_UNCHANGED`。S3N据此完成为
`COMPLETE_RUNTIME_TASK_CONTINUES`；不运行第二次full，不解锁下一任务。next owner=project owner /
performance-review coordinator，本线程将暂停并输出优化进展、剩余长尾、收益/风险与后续方案。

### S4：持续回归约束

2026-07-19，project owner 选择方案 A，授权先完成 `S4_FULL_TRIGGER_PROVENANCE`，再恢复
G2.4 主线。S4 的目的不是增加新的 Full，而是让每一次 Full 都能回答“为什么现在运行、归属哪个任务、
处于哪个集成边界、是否为失败重跑”。冻结合同如下：

- canonical object 为 `validation_trigger_provenance.v1`，summary、full profile 和 Reader Brief 必须来自
  同一份对象，不允许三处独立推断；对象至少包含 `status`、`required_for_tier`、`trigger_reason`、
  `task_id`、`boundary_id`、nullable `parent_run`、逐字段 `field_sources` 与 `validation_errors`；
- runner CLI 使用 `--trigger-reason`、`--task-id`、`--boundary-id`、`--parent-run`；对应环境变量为
  `AITS_VALIDATION_TRIGGER_REASON`、`AITS_VALIDATION_TASK_ID`、`AITS_VALIDATION_BOUNDARY_ID`、
  `AITS_VALIDATION_PARENT_RUN`。只要出现任一CLI provenance参数，就必须从完整CLI envelope取值，不得从
  环境变量拼接缺失字段；完全没有CLI provenance参数时才读取完整environment envelope；
- Full 的 reviewed trigger enum 固定为 `natural_integration_boundary`、`phase_exit_or_handoff`、
  `broad_shared_contract_change`、`formal_performance_profile`、`failure_fix_rerun`、`scheduled_ci`。
  所有 Full 都要求 non-empty task/boundary；`failure_fix_rerun` 额外要求指向仓库内既有失败formal Full
  `test_runtime_summary.json`。runner必须重验schema v1、非benchmark/print-only、exit/status一致、PASS
  provenance、profile resolved fixed-sibling containment，并以同一份captured profile bytes完成formal语义、
  summary/profile派生字段及output inventory SHA/size验证，再冻结run id、相对路径、
  summary/profile SHA-256、失败依据与安全边界；opaque id、缺文件、非Full、benchmark/print-only、最小伪造
  summary、无有效失败依据、任一SHA不可验证或filesystem resolution异常都在pytest启动前以canonical
  `validation_errors`拒绝，不允许Python traceback替代结构化exit 2；
- 非 Full tier 未声明 provenance 时保持现有调用兼容并记录 `status=NOT_REQUIRED`；若显式声明，则同样
  使用 canonical object，但不把 S4 扩张为日常 focused/architecture/contract 的新阻断门；
- `.github/workflows/ci.yml` 保留既有 daily scheduled Full，但必须映射为 `scheduled_ci`、
  `CI_DAILY_FULL` 和 GitHub run boundary；push/PR 仍默认 fast-unit。manual dispatch 默认不应无意启动
  Full，手动 Full 必须选择受治理原因并由workflow传task/boundary；无法在clean checkout自证parent
  artifact的`failure_fix_rerun`不暴露为manual choice，保留给已下载/本地可验证prior summary的runner入口；
  CI runtime artifacts必须以`always()`持久化供审计；
- full profile plugin必须自行严格解析并验证同一canonical object，拒绝duplicate key、non-finite、未知字段、
  非法schema/reason/parent binding；`validation_provenance_binding_status=PASS`是performance evidence PASS的
  必要条件。runner再做expected-envelope exact compare；binding失败不得覆盖pytest exit，但必须关闭promotion
  evidence并在summary/Reader Brief显示FAIL；
- Full benchmark variants属于non-formal比较运行，不生成或登记不存在的单一runtime profile，明确记录
  `runtime_profile_status=NOT_APPLICABLE`；benchmark subprocess必须清除继承的formal selection、profile
  output与provenance环境变量，不能因父进程污染生成或覆盖formal sidecar；
- schema 采用 v1 additive field，不删除或重命名既有 summary/profile 字段；pytest subprocess exit 仍是
  correctness 权威，provenance 只在运行前阻止无归属 Full，不得把测试 FAIL 覆盖为 PASS；
- architecture/contract tests 校验 provenance enum、必填矩阵、whole-envelope CLI-over-env、失败summary/profile
  path/hash binding、fixed-sibling containment、same-captured-bytes证明与最小伪造拒绝、filesystem resolution异常结构化收敛、任意合法JSON类型的total validator、plugin fail-closed、benchmark env隔离、summary/profile同源、Reader Brief可读性、CI event mapping、runtime
  manifest freshness与调度确定性；
- S4 不单独运行 Full。focused/architecture/contract 与 tracked manifests/hashes 完成后提交推送；下一次
  自然 integration/phase/broad-contract/performance/failure/scheduled Full 才产生首份真实 profile 证据；
- 性能改善不能以关闭 source validation、byte rebuild、DQ/PIT 或 tamper tests 换取；固定
  `strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

S4 authority base=`2962e02f`，启动时状态=`IN_PROGRESS`，next owner=validation-runtime coordinator。该具名
G2.4 子任务的显式授权不改变 phase-level lock：`next_phase_or_slice_unblocked=false`，不进入 EB1、
下一 callback、ARCH-005 或 G2.5。S4 closeout 后只恢复到 G2.4 主线“选择下一合法 slice”的协调点。

2026-07-19 / S4 closeout：实现与合并focused=`81 passed / 14.47s`；final fail-closed review又捕获并修复
minimal parent forge、validator exception与benchmark inherited env三个P1，修正后targeted/expanded focused=
`89 passed / 14.31s`与`136 passed / 33.23s`。生成式索引=`949 modules / 1,126 test-support files /
0 violations`。正式architecture首轮`354 passed / 2 failed`真实捕获新增module导致的deprecation inventory
count与test manifest freshness漂移；刷新inventory/id/manifests及P1 hardening后，pre-final integration architecture=
`362 passed / 50.02s`，artifact=`architecture-fitness_20260718T185957Z`，SHA-256=
`ee0436deb6b478df808aeb07cfae8eb3dcf4615e1060a6706dd9b3ad83ae33d7`；contract=`254 passed /
36.00s`，artifact=`contract-validation_20260718T190052Z`，SHA-256=
`23137b86174a5662fe03fd40147eafdda61b85f2700c25bbb70e2462797f45c3`。24个changed/declared tracked paths一致，
83 active sources哈希fresh，deprecation inventory=`FRESH_REFRESHED`。本子任务Full run count=`0`，首份
真实`validation_trigger_provenance.v1` profile证据等待下一次自然Full；S4完成为
`COMPLETE_RUNTIME_TASK_CONTINUES`并返回G2.4协调点。`next_phase_or_slice_unblocked=false`，不得自动进入
EB1、下一callback、ARCH-005或G2.5，`production_effect=none`。

上述两份artifact是P1 hardening后的pre-final integration证据。为避免“活跃文档记录final artifact后又改变自身hash”
形成递归漂移，post-doc final tracked-state artifact/path/hash只在self-excluded
`inputs/architecture/arch_004_compatibility_baseline.yaml` 的`s4_full_trigger_provenance.validation`节点更新；
该节点是S4最终验证证据的canonical binding，活跃文档不复制其瞬时run id。

Post-pre-final security review又捕获并修复profile双读TOCTOU与外部symlink containment两个P1；formal
parent proof现要求resolved fixed sibling，并对同一份captured profile bytes同时完成语义验证、SHA/size与
inventory绑定。对应最终focused/formal artifact与计数仍按上述self-excluded compatibility节点canonical记录。

最终Python 3.11鲁棒性审查进一步闭合filesystem resolution exception P2：parent summary、validation
artifact allow-root/containment、fixed-sibling profile、output inventory、recorded profile path与最终repo-root
binding遇symlink loop或非法解析时统一返回canonical `validation_errors`/exit 2，且pytest不启动、stderr无
traceback；该回归不扩大Full运行次数。

2026-07-19 / EB1首次真实runtime-profile失败暴露S4恢复合同P1：runner把无效原始telemetry转换并持久化为
固定sibling canonical fail-closed sidecar后，parent validator又把该形状统一当作artifact missing/invalid，
使`RUNTIME_PROFILE_FAIL -> failure_fix_rerun`在实现上不可达。修复边界冻结为只接受captured bytes严格匹配
runner canonical失败schema、pytest exit、embedded PASS provenance、summary派生字段、安全边界与已验证
output inventory SHA/size的sidecar；普通PASS/FAIL telemetry仍走完整reader，缺失、损坏、替换、minimal forge、
未知字段或summary/profile漂移仍拒绝。该修复只恢复既有S4语义，不增加trigger enum、不把pytest FAIL改写为
PASS，也不为EB1增加第三次Full。

同日首个parent-bound Full证明上述恢复路径可达，但真实捕获3项由S4修复扩大tracked path后产生的
integration freshness FAIL；其formal profile完整覆盖`6,297 nodes / 1,069 files`并保持telemetry/provenance
PASS，pytest exit=1使performance FAIL。原“无需第三次Full”预期因此被实际失败证据否定：只有先刷新
module/test/aggregate/fitness、compatibility/deprecation/source hashes及attribution count并通过正式pre-gates，
才允许用该pytest FAIL summary作为新的`failure_fix_rerun` parent。该运行是S4定义的失败修复验证，不是
无parent的重复性能采样；若pre-gates不通过则不得启动。

刷新三项integration metadata并通过architecture/contract后，第二个parent-bound修复Full=
`6,295 passed / 2 skipped / 642 warnings / 1,066.73s`；profile/telemetry/performance/provenance与
PARTIAL_SEED order全部PASS。该sidecar成为EB1当前`6,297 nodes / 1,069 files`的唯一COMPLETE source，
机械生成的v4 manifest逐项绑定artifact、collection、file set/rows和expected order hash，offline exact
collection重验PASS。两次失败与最终PASS完整保留，不把失败运行计入稳定提速样本；本次wall较前一失败
Full约缩短3%，但单次结果仍不升级`stable_full_improvement_claimed=false`。

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
