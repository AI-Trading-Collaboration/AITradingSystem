# DEVX-009：并行集成发布栅栏与生成状态重建 V1

- priority: `P0`
- status: `PROPOSED`
- owner: Project Owner（启动决策）；Codex workflow coordinator（合同、实现与验证）
- governed mode: `SINGLE_LANE` serial workflow-contract wave
- contract change: `true`
- production effect: `none`
- broker action: `none`

## 1. 背景与决策

2026-08-24，Project Owner 要求在当前任务完成后的合适窗口登记“并行开发阻塞进一步优化”，并说明后续
仍可能引入神经网络分析趋势。登记时，`TRADING-2542C` 已完成工程收口：最终 Full 为
`9487 passed / 3 skipped`，`HEAD = local main = origin/main =
e5266c9aadfba067060b013d83ec26bd4f065604`，旧分支与临时计划已按生命周期清理。

本任务不是建立第二套 scheduler、lease 或 integration queue。ARCH-005 已有
`change_manifest.v1`、resource/checkout lease、integration queue、base-drift plan、generated views 与
validated-main closeout primitives；本任务要补的是它们之间尚未强制闭合的 coordinator transaction：

1. 同一时刻只能有一个 coordinator 进入共享生成状态与 `main` 发布边界；
2. plan/preflight 通过后，任何新的 `main` 漂移必须在首次写入前和每个关键 mutation phase 前被发现；
3. task source、task index、generated task views、compatibility/report-flow authority 只能在最新主线候选上
   由 coordinator 重建一次，不能把两个各自合法但相互过期的生成结果叠加；
4. formal Full 必须绑定一个 exact candidate 和一个独占 run identity，不能与另一 Full 竞争资源或产生
   可误认的证据；
5. fast-forward、ordinary push、SHA 等值复核、lease release 与临时资源清理必须形成可重放的 closeout
   evidence，而不是依赖聊天记忆或手工顺序。

## 2. 当前阻塞模式的直接证据

`TRADING-2542C` 在 frozen lane 到 final candidate 的过程中暴露了可重复的系统性问题：

- `main` 连续前进，最终使用第三轮 reviewed reconciliation plan 才能形成 latest-main candidate；
- 首次 final Full 为 `9462 passed / 21 failed / 3 skipped`，失败集中在 carrier binding、Atlas successor
  分类、compatibility/report-flow current authority 与 generated freshness，而非策略合同本身；
- task event 的 supporting-requirement 文本遗漏使 canonical `requirement_refs` 被清空，Atlas 按设计
  fail closed；
- append-only successor 已存在但 `LATEST_COMPATIBILITY_SECTION` 仍指向旧 authority，导致
  Architecture `805 passed / 61 failed`；
- 同一集成期间需要多次重建 task、Atlas、report-flow 与 compatibility authority，并保留失败 Full 的
  parent provenance 后重新运行。

这些门禁确实阻止了错误发布，但代价是大量人工 reconciliation、重复生成和长时间 Full。结论不是降低
门禁，而是把现有门禁前移并串成一个可审计事务，使 stale candidate 在昂贵验证和共享写入之前停止。

## 3. V1 目标合同

### 3.1 `integration_publication_fence.v1`

复用 ARCH-005/S4D lease authority，增加 coordinator 级发布事务，不新增永久全仓锁。事务至少绑定：

- `task_id`、`change_id`、actor、exact repository/worktree identity；
- `frozen_base_sha`、`lane_head_sha`、`expected_main_sha`、candidate SHA；
- reviewed `integration_revalidation_plan.v1` id/hash（适用时）；
- task-owned、coordinator-owned、generated/shared path claims；
- required validation tiers、Full run id、parent artifact（failure-fix 时）；
- acquire/heartbeat/release、terminal reason、cleanup evidence；
- `production_effect=none`、`broker_action=none`。

任何 stale `expected_main_sha`、活动冲突 lease、未声明 shared/generated path、错误 ancestry、dirty target、
plan tamper 或候选字节变化必须 fail closed，并在修改 task source、generated views、candidate 或 `main` 前停止。

### 3.2 阶段化 compare-and-set

至少在以下边界重新核验 exact `main` 与事务身份：

1. 获取 publication fence 前；
2. canonical task event / shared authority 首次写入前；
3. generated-state rebuild 前后；
4. formal validation dispatch 前；
5. candidate commit 与 local-main fast-forward 前；
6. fetch 后、ordinary push 前；
7. release/cleanup 前。

任一检查失败时，旧 plan 只作为只读 evidence 保留，不得在 worktree 中先写后回滚，也不得自动 rebase、
merge、cherry-pick、force-push 或修复远端分叉。

### 3.3 Generated-state rebuild-only

- canonical task fragments/events 仍是事实源；
- `docs/task_register.md`、`docs/task_register_completed.md`、task index、Atlas sidecar、report-flow、
  compatibility/current-hash authority 和 architecture manifests 只能通过各自官方 generator/append-only
  writer 从 latest-main final tree 重建；
- coordinator transaction 记录 generator 顺序、输入 SHA、输出 SHA 与 freshness result；
- 禁止用手工冲突拼接、旧 candidate 生成物覆盖新主线或仅修改 expected count 消除失败；
- shared/public contract 如需改变，先完成最小 serial contract wave，再让所有 consumer 从新 exact base
  重算。

### 3.4 Full 独占与证据绑定

- `run_validation_tier.py full` dispatch 前必须取得与 exact candidate 绑定的 validation resource lease；
- 同一 repository/resource policy 下已有 active Full 时，第二个 Full 不启动，并返回稳定 reason code；
- failure-fix rerun 必须绑定首次失败 artifact、候选 lineage 与 retry purpose，不覆盖第一次结果；
- focused/Architecture/Contract/Integration/Reproducibility 可按既有规则运行，但不得与唯一 heavyweight
  Full 争抢受治理资源；
- duration、heartbeat、expiry、capacity 等阈值必须来自 reviewed policy，不得在代码中新增投资无关但仍
  不可审计的 magic number。

## 4. 与神经网络趋势分析的关系

DEVX-009 不引入模型、特征、数据源、训练、回测或阈值，也不改变投资解释。它为后续趋势分析提供工程
前提：每次 dataset/feature/model/evaluation candidate 都能绑定唯一代码、数据、配置、生成状态与验证证据，
避免并行开发把不同版本的 lineage 混合成虚假趋势。

策略研究仍由
`TRADING-2544_CONDITIONAL_SOURCE_VALUE_AUDIT_SERIAL_CONTRACT_AND_FEASIBILITY_V1` 管理：先冻结
estimand、PIT/DQ、outer-fold OOF 与 capacity gate；只有门禁通过后才允许 small gated MLP challenger。
DEVX-009 的完成不自动授权 TRADING-2544 采数、训练、回测或提升状态。

## 5. 分阶段实施建议

### S0：合同与并发回归夹具

- 冻结 publication fence schema、reason taxonomy、阶段状态机与复用的 lease authority；
- 构造两个基于同一 exact main 的候选，覆盖 task registration、generated rebuild、Full 与 publish 竞争；
- 证明一个事务获准、另一个在任何共享写入/Full dispatch 前以 stable stale/conflict reason 停止。

### S1：Coordinator 写入与 generated rebuild 接入

- 将 task-source mutation、integration revalidation、checkout guard 和官方 generators 接入事务；
- 在 generator 顺序和 final-tree freshness 上建立 deterministic evidence；
- 保持 worker 不得写 task registry、root/shared wiring、generated views 或 formal artifacts。

### S2：Validation 与 closeout 接入

- 为 Full dispatch、failure-fix parent binding、local-main fast-forward、fetch/push/SHA verify 与清理增加
  同一事务 lineage；
- 证明 crash/resume、expired lease、stale plan、remote divergence 和 cleanup incomplete 均 fail closed；
- 更新 `docs/system_flow.md`、governed-development skill bundle/parity、相关 policy 与运行手册。

## 6. 验收标准

1. 不新建第二套 scheduler/lease/integration queue；V1 明确复用 ARCH-005 与 S4D authority；
2. 两个同 base 的 coordinator 候选竞争时，最多一个能修改 shared/generated state 或启动 Full；
3. `main` 在 plan 之后漂移时，旧 candidate 在首次 mutation 前被拒绝，并输出可审计 reason/evidence；
4. canonical task event 与所有 generated/current authorities 从 latest-main final tree 按官方顺序重建，
   不出现冲突 marker、旧 authority 指针、丢失 `requirement_refs` 或手工 expected-count 修补；
5. Full 绑定 exact candidate、唯一 active run 与 failure parent；并发第二 Full 不启动；
6. local-main fast-forward、ordinary non-force push、SHA equality、lease release、branch/worktree/plan cleanup
   形成单一 closeout receipt；remote divergence 等条件继续 fail closed；
7. concurrency、crash/resume、stale plan、generated freshness、failure-fix provenance 与 no-side-effect tests
   通过；focused、Architecture、Contract、Integration、Reproducibility 和 required Full 全部 PASS；
8. `docs/system_flow.md` 与 governed-development skill 同步，canonical/installed skill parity PASS；
9. 不改变 strategy、DQ/PIT、backtest、position、promotion、production、broker 或神经网络研究状态。

## 7. 当前 blocker、依赖与退出条件

- status 保持 `PROPOSED`；本轮只登记方向与需求，不实施；
- 启动前由 Project Owner/Codex workflow coordinator 复核 S0 schema 与现有 ARCH-005/S4D primitive 的
  最小扩展边界；
- 若分析发现必须改变 shared/public contract，先执行最小 `SINGLE_LANE` serial contract wave；
- 与投资研究无直接数据依赖，但实现时不得和另一个 coordinator publication 或 Full 重叠；
- 退出条件：第 6 节全部满足、正式门禁与普通发布完成、任务状态通过 append-only event 更新。

## 8. 本次登记生命周期

- registration branch：`codex/devx-009-parallel-integration-fence`；
- registration base：`e5266c9aadfba067060b013d83ec26bd4f065604`；
- 不创建额外 Git worktree、clone、download、cache 或 credential 文件；
- 本轮只新增 canonical task event、支持性需求及其 deterministic generated views/index；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、diff、stage
  或修改；
- 登记验证通过后提交 task branch，按 `--ff-only` 集成本地 `main`，普通 non-force push 并验证 SHA 等值，
  然后删除登记分支；
- `production_effect=none`、`broker_action=none`、`external_action=none`。

## 9. 进度

- 2026-08-24：`TRADING-2542C` 工程与发布收口后，READ_ONLY preflight PASS：`main`/`origin/main`=
  `e5266c9aadfba067060b013d83ec26bd4f065604`，active lease=0，task-owned dirty paths=0；选择此窗口登记
  DEVX-009。未启动实现、Full、神经网络训练、数据采集或回测。
