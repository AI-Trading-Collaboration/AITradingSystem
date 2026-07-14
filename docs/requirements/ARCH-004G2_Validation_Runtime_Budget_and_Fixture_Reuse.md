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
- 接入 content-fingerprint validation session，并把 source commitments 改为 bounded contract；
- module fixture 共享 immutable upstream，tamper 场景 copy-on-write。

### S3：历史感知调度

- 用最近 N 次 runtime artifacts 生成 deterministic duration manifest；
- 比较 `loadfile`、`loadscope` 与显式 shard 的 wall-time/peak-memory；
- 根据机器可用内存选择 bounded worker count，并保证完整 nodeid 集合与失败传播一致。

### S4：持续回归约束

- architecture/contract tests 校验 runtime manifest freshness、调度确定性和 gate coverage；
- 对单 slice、单 module 和 full suite 记录 before/after；性能改善不能以关闭 source validation、
  byte rebuild、DQ/PIT 或 tamper tests 换取。

## 验收标准

- 当前 4 个 confirmation 长尾 module 的累计 wall time 至少降低 70%，且 tamper/drift focused
  tests 全部 PASS；
- full collected nodeid 集合与优化前一致（仅允许任务本身新增测试），无 skip/xfail 增量；
- 同一机器连续 3 次 full 的 P95 wall time 不高于当前 `2,554.80s` 的 60%；
- peak memory 与 read amplification 有 runtime artifact 证据，累计 read bytes 至少降低 80%；
- source/config/policy 任一 byte 修改会使 cache 失效，FAIL 不缓存；
- module/test manifests、architecture/contract/full validation PASS；
- `strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`。

## 当前状态

G2.4CR/CS 已完成第一批通用 validation session、bounded snapshot 和 fixture path isolation；本任务
承接剩余 confirmation/full-suite 长尾；G2.4CT/CU继续证明局部session reuse有效但full长尾仍由
旧confirmation DAG主导。G2.4CU单次full虽降至`1,939.34s`，仍需连续样本排除机器状态波动。
建议实施顺序保持S2→S3：先完成confirmation weekly/dashboard、rule queue/owner decision的content
fingerprint / bounded fixture治理，再用duration+peak-memory manifest比较loadfile/loadscope/explicit
shard，并补active-node heartbeat。它是研发效率治理，
不是 ARCH-004 G2.5 解锁条件，也不得绕过 owner 已批准的 phase-level
`arch_005_bootstrap_handoff.v1` 停止条件。
