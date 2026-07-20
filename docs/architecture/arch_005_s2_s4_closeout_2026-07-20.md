# ARCH-005 S2～S4 并行研发控制面收口

最后更新：2026-07-20

状态：`BASELINE_DONE_S2_S4_COMPLETE_S5_PENDING`

边界：本批只完成 reviewed pilot 的 dependency/readiness、lease/replay、read-only scheduler
和 controlled dispatch。`docs/task_register.md` / `docs/task_register_completed.md` 仍是唯一可写
事实源；S5 canonical cutover、self-hosting、自动任务状态变更、投资策略变更和生产操作均未发生。

## 1. 为什么这样设计

此前并行开发主要依赖共享 Markdown、人工记忆和 merge-time 冲突发现，无法在任务启动前回答
“依赖是否满足、资源是否冲突、base 是否过期、失败后由谁接管、证据是否绑定真实 bytes”。S2～S4
先建立一个受限、可删除回滚的控制面，在不切换事实源的前提下验证以下闭环：

1. 先将 dependency、readiness、resource claim 和 lease 变成 typed contract；
2. 再用 deterministic read-only scheduler 解释可运行任务，不派发、不改状态；
3. 最后只对显式 allowlist 的两条 domain lane 和一条 coordinator lane 开放真实 lease；
4. coordinator 只有在两条 domain evidence 都通过后才能启动；任一失败必须隔离、可重放并保留证据。

这种顺序把“决策正确性”和“执行副作用”分开验证，避免为追求并行速度直接把未成熟 scheduler
接到 canonical task source 或 production path。

## 2. 输入、计算与输出

|阶段|主要输入|核心计算|主要输出|
|---|---|---|---|
|S2|`arch_005_parallel_control_policy.yaml`、`arch_005_s2_s4_pilot.yaml`、S0/S1 schema 与 frozen base commit|dependency unknown/self/cycle 检查；governance/dependency/base/safety readiness；path/module/contract read-write conflict；single-arbiter lease acquire/release/expire/reassign；event hash/causal replay|typed readiness、lease acquisition、append-only lease events 与 replay report|
|S3|S2 task/dependency snapshot、reviewed priority/capacity policy、当前无 active lease 状态|按 P0→P3、task/change id 稳定排序；选择两个互不冲突 domain tasks；记录 selected/not-selected reason、alternative；相同输入连续运行两次并比较 canonical bytes|`outputs/architecture/arch_005_s4/shadow_governance_audit.json`|
|S4|S3 PASS audit、两条 domain adapter、coordinator dependencies、显式 failure injection|两条 domain lane 并发取 lease；工程 lane 首次受控失败后 expire/reassign；研究 lane 独立完成；依赖满足后 coordinator 汇合；重算 artifact SHA 和 lease replay|domain result、dispatch summary、controlled dispatch report/validation、run-specific lease store|

Reviewed pilot policy 固定 domain capacity=2、total lease capacity=3、TTL=1800 秒、最多一次
reassign、无 priority aging。它是有 owner、版本、理由和退出条件的 pilot baseline，不是永久最优参数。

## 3. 当前真实结果

### S2

- dependency unknown/self/cycle、stale base、unsafe production effect 与资源冲突均 fail closed；
- lease 支持 idempotent acquire、release、expire、一次 reassign、事件 hash 与 causal replay；
- S2～S4 聚焦测试 `17 passed`，包含报告和 lane artifact 篡改拒绝。

### S3

- governance cycle `001/002` 均为 `PASS`；
- 两轮 decision id 均为 `scheduler-decision-aff328cb5e0e70ad7ba2`；
- 两轮 canonical bytes SHA-256 均为
  `d0b67afe69b552454266933028eec0722bd85ae9e2a678dc5202fd9d2fb36429`；
- 选择 research `domain-01` 与 engineering `domain-02`，coordinator 因两个 dependency 未满足而
  保持 blocked；`dispatch_allowed=false`、`lease_acquisition_allowed=false`。

### S4

- 成功 dispatch id=`controlled-dispatch-aca2d27f60304e5a5c60`；
- 成功 run lease store=`outputs/architecture/arch_005_s4/runs/run-bdb412f86ab283a0/lease_store`；
- 工程 lane attempt 1 为预期注入失败，attempt 2 经 reassign 后 `PASS`；研究 lane attempt 1
  `PASS`，未被工程 lane 阻塞；coordinator 在两项 `EXECUTION_PASS` 后 `PASS`；
- 成功链 13 个事件重放 `PASS`，active lease=0；最终 pilot validator 29 项检查全部通过；
- engineering evidence：module=986、test=1137、architecture/direct-writer violation=0；
- research evidence：刷新后的 R0/R2 validator 均 `PASS`，R2 仍为
  `CONTINUE_EVIDENCE_CLOSURE`，没有恢复 candidate expansion 或 parameter search；
- 全链 `task_governance_status_mutated=false`、`strategy_logic_changed=false`、
  `paper_shadow_changed=false`、`production_effect=none`、`broker_action=none`。

## 4. 失败链与恢复证据

本批没有删除失败执行历史：

1. 首次演练暴露 research adapter 参数遗漏，以及 R0 artifact 对格式化前 policy bytes 的 stale
   fingerprint。旧根目录 lease chain 保留 14 个事件、replay PASS、active lease=0；修复方式是显式
   传入 R0 artifact path，并完整重建 R0→R1 walk-forward→R1 robustness→R2，而不是修改 checksum。
2. 第二次演练 `run-a26d89e41ec6829b` 暴露新增测试后 DevEx manifest stale。该链保留 10 个
   事件；失败 recovery lease 已追加 `FAILED_RUN_AUDIT_CLOSEOUT` expiry event，replay PASS、
   active lease=0。刷新 canonical manifests 后才启动新的 run identity。
3. R1 lineage-only refresh 的结果未被改写：walk-forward
   `r1-wf_6447beb5464bad37` 仍为负面 evidence，robustness
   `r1-robustness_8c93b0e2615d0ace` 仍为 incomplete evidence；R2
   `r2-decision_c761da11538fc58c` 仍要求继续闭合证据。

## 5. 后续优化空间

S5 前仍需 owner 显式授权。建议按以下顺序评估，而不是直接扩大自动执行范围：

1. 用更多 read-only governance cycles 统计 queue age、conflict、failure/rework 和 lease expiry；
2. 以数据决定 capacity、TTL、retry 和 fairness，任何变更升级 policy version 并重新做 failure rehearsal；
3. 把 run-specific lease cleanup、orphan lease scan 和 runtime artifact retention 纳入 S5 cutover gate；
4. 完成 consumer migration、最终 parity、rollback rehearsal 后，才把 Markdown 改为 generated view；
5. S5 稳定两个 governance cycles 后再考虑 S6 Understanding read model 和吞吐优化。

当前不应引入 priority aging、自动 status mutation、自动 merge、更多并行 lane 或跨策略副作用；这些
能力在缺少真实调度样本时只会扩大错误半径。

## 6. 验收边界

S2～S4 完成只表示“受控 pilot 基础版可用”。S5 仍未授权，canonical source 没有切换；
ARCH-004 G2.5 也未因本批自动恢复。

最终 tracked state 的并行验证如下：

|验证层|结果|pytest / wall|runtime artifact|
|---|---:|---:|---|
|architecture focused|85 passed|28.40s|focused one-off，不写 artifact|
|fast-unit|317 passed|40.08s / 40.62s|`outputs/validation_runtime/fast-unit_20260720T075916Z/test_runtime_summary.json`|
|architecture-fitness|436 passed|34.70s / 35.17s|`outputs/validation_runtime/architecture-fitness_20260720T080002Z/test_runtime_summary.json`|
|contract-validation|265 passed|150.71s / 151.82s|`outputs/validation_runtime/contract-validation_20260720T080048Z/test_runtime_summary.json`|
|reproducibility|23 passed|8.71s / 9.81s|`outputs/validation_runtime/reproducibility_20260720T080338Z/test_runtime_summary.json`|
|full|6420 passed、2 skipped、643 warnings|955.47s / 956.37s|`outputs/validation_runtime/full_20260720T080412Z/test_runtime_summary.json`|

Full 使用 16 workers / `loadfile`，trigger=`natural_integration_boundary`、boundary=
`ARCH-005-S2-S4`。相对上一轮 978.80s 基线缩短 22.43s（约 2.3%）；slowest 50 仍由既有
smoothed/forward、paper-shadow 和 research evidence 链构成，新增 S2～S4 测试未进入 slowest 50，
未发现本批性能回退。Architecture manifests 为 986 modules / 1137 tests / 858 direct writers / 0
violations；deprecation inventory=`arch_004g_deprecation_inventory_5d9c44151c3aa868b181`。
