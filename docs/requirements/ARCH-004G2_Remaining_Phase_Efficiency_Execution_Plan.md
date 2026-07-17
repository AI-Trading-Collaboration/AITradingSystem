# ARCH-004G2 剩余阶段效率优先执行计划

最后更新：2026-07-17

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
