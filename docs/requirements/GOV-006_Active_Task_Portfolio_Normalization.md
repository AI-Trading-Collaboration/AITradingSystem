# GOV-006 Active Task Portfolio Normalization

最后更新：2026-07-23

## 任务信息

- task id：`GOV-006_ACTIVE_TASK_PORTFOLIO_NORMALIZATION`
- priority：`P0`
- status：`IN_PROGRESS`
- current phase：`N1_APPLIED_CLOSEOUT_PENDING`
- owner：governance coordinator / architecture coordinator
- related：`TRADING-2445_TASK_REGISTER_BASELINE_DONE_ACTIVE_SEMANTICS`、`TRADING-362_TASK_REGISTER_CONSISTENCY_CHECK`、`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`
- production effect：`none`

## 背景与问题

当前 `docs/task_register.md` 仍是未完成工作的权威事实源，但 active portfolio 已出现明显的决策失真：大量实现已完成的前序切片长期停在 `VALIDATING`，被后继关闭决定取代的研究路线仍表现为 owner blocker，且 P0 数量过高，无法表达真正的正确性关键路径。项目已经具备 ARCH-005 shadow registry 和一致性验证，缺口不是再建一套 task system，而是对现有任务做一次有证据、可回放的生命周期 reconciliation。

本任务只治理任务状态、当前路线语义和 generated views，不改变任何策略、数据、评分、回测、调度、报告运行时或 production 行为。

## 原则

1. **按任务自身验收，不按后继存在推断完成。** 后继任务可以作为 supporting evidence，但每个转 `DONE` 的任务必须证明其 own acceptance 已满足。
2. **实现完成与运行成熟分离。** 实现任务达到验收后归档；真实时间样本、owner 决策或外部数据应由独立 active task 承接，不能让旧实现任务无限停留在 `VALIDATING`。
3. **关闭路线不制造 owner 待办。** 已由权威后继决定关闭、替代或禁止继续的任务转 `DROPPED` / `SUPERSEDED`，未来只能以新任务和新证据重开。
4. **`BASELINE_DONE` 不批量终结。** 只有明确剩余缺口已经不存在时才转 `DONE`；真实外部时间或供应商依赖转 `BLOCKED_EXTERNAL`，明确 owner 数据/批准/业务决定转 `BLOCKED_OWNER_INPUT`。
5. **当前语义只修 active source。** 2021-02-22 是当前 primary research default；2022-12-01 只保留在 immutable history、legacy compatibility 或明确 market-regime/sensitivity 角色中，不重写历史 artifact。
6. **单写者集成。** audit worker 只生成建议和证据，只有 coordinator 修改 task register、completed register、shadow registry、generated views 和共享路线文档。
7. **S5 前减少行号级 generated churn。** 新增 active row 默认追加在“当前任务”表末尾；除 terminal move 或有明确语义需要外，不为分类美观重排既有行。该约束只减少 S1 shadow fragment 的无意义 line-number drift，不改变任务优先级或 scheduler 排序。

## 实施阶段

### N0：冻结 inventory 与状态判定合同（FORMAL_COMPLETE）

- 记录 active/completed 总数、priority/status 分布和 source hashes；
- 为每个候选任务记录 current status、own acceptance evidence、successor/closure evidence、remaining condition、recommended status 和 confidence；
- unknown、证据冲突或只有聊天结论的候选保持原状态并进入人工复核，不做猜测性迁移。

### N1：高置信 terminal reconciliation（APPLIED_CLOSEOUT_PENDING）

- 第一批只处理 own implementation/validation 已完成且有明确证据的前序任务；
- `DONE` / `DROPPED` 行必须同批移动到 `docs/task_register_completed.md`；
- 已关闭 research family 的旧 owner blocker 只有在权威 closure 明确覆盖其 route 时才允许 `DROPPED`；
- 每批都生成 before/after task ids、理由、evidence refs 和 source hashes。

### N2：等待条件与 program-level 收敛

- 将自然时间、样本成熟、供应商/历史 bytes 依赖与 owner 决策分别归入明确 blocker；
- 把数十个只表达同一 program 的活动切片收敛为少量 program-level row；
- 仍需开发的独立功能保持独立，不为了减少数量吞并其 acceptance 或 owner。

### N3：当前路线语义 reconciliation

- 修正 active roadmap 中残留的 2022-12-01 默认语义；
- 优先覆盖当前阅读入口与运行说明：`learning_path.md`、`product_strategy.md`、`development_plan.md`、
  operations/scheduler runbooks、system-flow generic backtest、artifact-catalog generic backtest；旧 requirement
  或 historical run 中的 2022 实际范围只改证据角色，不批量改写 immutable history；
- 区分两个 domain worker、一个 coordinator 与三个长期 workstream，禁止把三条工作流误写为三个 domain worker 同时开放；
- 刷新 runtime/Full 当前基线和“下一 wave”表述，旧数值保留为历史 evidence；
- 不改写 immutable historical artifact 或已归档结论。

### N4：generated views 与验证

- 重新生成 ARCH-005 S0/S1 shadow fragments、index 和 compatibility views；
- active/completed view 必须 byte-identical replay，task id 全局唯一；
- task-register consistency、documentation contract、freshness、architecture、contract 和本批 required Full 全部通过；
- 最终 artifact 记录 active/completed/priority/status before/after、迁移清单、source hashes、验证与 `production_effect=none`。

## 第一批边界

第一批仅允许：

- 当前 owner 指令、G2.5/DATA-GOV 状态和活动路线语义同步；
- 高置信前序实现任务与已明确关闭 family 的 lifecycle reconciliation；
- 生成审计矩阵和兼容 views。

第一批不允许：

- 根据任务数量目标强行归档；
- 降低研究、DQ、PIT、cost、promotion 或 production gate；
- 自动恢复被关闭的 B/C、growth-tilt 或其他候选路线；
- 切换 ARCH-005 S5 task source-of-truth；
- 执行任何 periodic command、真实数据刷新、研究 run 或 broker action。

## N0 dry-run 合同与当前结果

N0 采用 `gov_006_portfolio_normalization_policy.v2` 与只读 producer/validator，不直接编辑任何 register。
policy 拒绝 duplicate YAML key，并将 owner 授权严格限定为 governance task 与 parallel execution；owner
没有预先批准 30 条 exact disposition，具体清单由 `governance_coordinator` review。manifest 同时绑定
policy raw SHA、canonical semantic SHA、完整 manifest SHA、真实 Git base commit 及 all-legacy-row
inventory scope。公共 builder 只能使用当前仓库真实 HEAD；validator 自行确认 base commit 存在且为当前
HEAD 的祖先，调用方不能注入 SHA 或布尔证明，因此 artifact 可随提交进入 descendant 而不形成 hash
自循环，unknown/non-ancestor commit 继续 fail closed。

当前 Wave 1 dry-run 为 30 条高置信建议：18 条 `DONE`、12 条 `DROPPED`。每条 `DONE` 必须
`remaining_work=[]`；每条 `SUPERSEDED` 必须指向 `DROPPED` 且有 typed `terminal_closure` successor。
successor 同时绑定 task id、evidence role、expected source 与 expected status，source/status drift 阻断。
inventory 明确覆盖 main 422 + supplemental 8 + deferred 1 = 431 条 active legacy rows，并与 455 条
completed rows分区对账。`TRADING-1087` 的 temporary 10% overage exit 尚未闭合，`TRADING-1088`
也没有完成所要求的真实 split-window 执行，因此两者已从本批排除，保持 active。

N0 结果固定 `automatic_apply_allowed=false`。只有 coordinator 在共享文档冻结、正式验证通过后，才可
进入 N1 并逐行移动 terminal task；dry-run manifest 本身不改变任何 task status 或 generated view。

## 验收标准

- 每个状态迁移有 task-own acceptance 与 supporting evidence；
- `DONE/DROPPED` 只存在 completed register，active register 不含 terminal status；
- 等待时间、等待 owner、等待外部数据和真实可开发任务不再混在模糊 `VALIDATING` 中；
- active P0 只保留影响当前正确性、数据质量、投资解释或关键 cutover 的任务；
- 当前窗口、并行容量、runtime baseline 和 next-wave 语义在 active documents 中一致；
- ARCH-005 shadow registry、compatibility views、task consistency 和 required validation 全部 PASS；
- `production_effect=none`、`broker_action=none`。

## 状态记录

- 2026-07-23：N0 producer/validator 与 reviewed Wave 1 policy 完成有界实现；formal closeout 前的只读
  snapshot 为 `30 decisions / 431 active rows`，该数字只描述当时输入，不作为 disposition 已应用或当前
  active 总量的声明。duplicate policy key、policy/manifest drift、successor source/status、own-acceptance
  invariant、unknown/non-ancestor Git base 均 fail closed。共同 formal gate 收口为 focused=
  `183 passed / 1 skipped`、architecture=`482 passed`、contract=`266 passed`；Full append-only ledger
  保留 attempt 1=`6701 passed / 2 failed` 与 attempt 2=`6706 passed / 1 failed`，最终 attempt=
  `6710 passed / 0 failed / 3 skipped / 643 warnings`，runner=`1106.60s`。前两次 FAIL 继续作为不可覆盖
  证据保留；N0 标记 formal complete，GOV-006 overall 仍为 `IN_PROGRESS`。本次没有自动应用任何
  disposition，N1 未自动启动，`TRADING-1087/1088` 继续明确排除，`production_effect=none`。
- 2026-07-23：owner 授权结合现有并行能力推进前述长期优先任务。本任务以 coordinator 单写、worker 只读审计的方式进入 `IN_PROGRESS`；先冻结 inventory/判定合同，再执行高置信小批迁移，不以减少任务数量替代逐项验收。
- 2026-07-23：N1 coordinator review 已基于 refreshed manifest
  `gov_006_decision_manifest_3fb5f2a038eca2361179` 应用30条精确decision：18 `DONE`、12 `DROPPED`；
  active/completed=`435/457 -> 405/487`、总数892，active P0/P1/P2=`241/135/29`，source-of-truth仍为
  两份legacy Markdown且shadow replay byte-identical。当前状态为 applied closeout pending；需以独立
  application commit 生成 commit-bound before/after evidence，并完成compatibility、formal gates、
  commit/push后才转N1 complete或开放Wave14。未改变策略、数据、调度或production。
