# ARCH-004G2 Validation Runtime Budget 与 Immutable Fixture Reuse

最后更新：2026-07-15

## 任务信息

- task id：`ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE`
- priority：`P1`
- status：`READY`
- owner：architecture coordinator / test infrastructure owner
- dependency：G2.4 原子切片不得被打断；优先随正在迁移的 chain 分批治理
- production effect：`none`

## 问题与证据

G2.4CR/CS 已证明主要耗时不在投资计算，而在高扇入 artifact DAG 对同一 immutable source
反复做 live validation、byte rebuild 与大 snapshot 读取：

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
