# ARCH-005 S5 Canonical Task Source Cutover

## 任务状态

- Task ID：`ARCH-005S5_CANONICAL_TASK_SOURCE_CUTOVER`
- Priority：`P0`
- Status：`DONE`
- Governed mode：`SINGLE_LANE`
- Exact base：`7cfc81d6ab9d992c6c45e1084e22b33b9560d519`
- Owner decision：`owner_decision:DEVX-006C:2026-08-09:authorize_c_then_d_then_s5_serial_v1`
- Production effect：`none`
- Broker action：`none`

## 背景与目标

DEVX-006B 已证明 stable task shadow v2 对 final import 前的 legacy tasks 可无损重放；DEVX-006C/D
已先后解除 compatibility 与 report/catalog/flow authority 的共享单体放大。D ordinary push、
cleanup 与 exact release 已在 `7cfc81d6ab9d992c6c45e1084e22b33b9560d519` 完成，S5 现在
按 Owner 指定顺序独立登记，不复用 D frozen-tree formal evidence。

Final import 已冻结 974 个任务与 55 条 ambiguous extra-cell rows；canonical task record/event
authority 已切换到 stable per-task fragments 与确定性 index。原
`docs/task_register.md` 与 `docs/task_register_completed.md` 现在只保留为 generated/do-not-edit
compatibility views；动态 consumer inventory 的 manual semantic runtime read/write 均为 0。

## 决策冻结

- canonical root：`registry/development_tasks/`；stable path key 仍为 `sha256(task_id)`；
- canonical index：`inputs/architecture/arch_005_task_registry_index.yaml`；只保存定位、顺序、
  partition、fragment/file hash、chain 与 view/template seals；
- 每个 task fragment保存 versioned task record、append-only causal events、legacy import evidence
  和当前 compatibility projection；任务更新只改目标 fragment 与 canonical index/view；
- 现有 v1/v2 shadow 与 final legacy baseline 保留为 immutable migration/rollback evidence，不再
  作为可写 authority；禁止 YAML/Markdown dual write；
- `docs/task_register*.md` 保持现有路径和八列兼容表格，由 canonical replay 确定性生成并标记
  `GENERATED / DO NOT EDIT`；非 task-row skeleton 绑定 exact seal，手工改动 fail closed；
- runtime consumers 通过统一 canonical loader/validated generated-view contract 读取；仅测试、
  migration/audit 或 immutable history 可保留显式 literal path，必须在 inventory 分类；
- S5 本任务在 cutover 后通过 canonical event 自行更新状态，形成至少两个可重放 governance
  cycles；不启用自动 dispatch、自动 merge/push、PR、scheduler 或 broker/trading。

## 分步计划

### S5.0：final import 与冻结

- 冻结 active/completed Markdown、v2 index/fragments、parser/compiler、task/status/order/docs-link 集；
- 验证 55 个 ambiguous extra-cell legacy rows原样保留，duplicate/missing/terminal mismatch 为 0；
- 建立 cutover policy、manifest、consumer inventory 与 pre-cutover rollback owner/boundary。

### S5.1：canonical registry、loader 与 validator

- 从 final v2 shadow 一次性生成 canonical per-task fragments/index；
- validator 重算 task/event identity、event chain、fragment path/hash、index chain、partition/order、
  projection、task set、template/view seal与 source cutover manifest；
- duplicate、missing、unknown status、invalid transition、event fork/reorder、hash/path escape、symlink、
  non-canonical YAML、index/view/template tamper 全部 fail closed。

### S5.2：generated views 与 consumer migration

- 原路径生成 active/completed compatibility views，并在文档规则与 AGENTS governance 中改为
  canonical registry first、generated views 禁止人工编辑；
- runtime semantic consumers 统一走 canonical loader；hash/freshness/migration-only readers显式分类；
- dynamic inventory 要求 manual-Markdown semantic runtime consumer=0、manual writer=0。

### S5.3：self-hosting、两次 cycle 与 rollback rehearsal

- cycle 1：final import/cutover/render/validate；
- cycle 2：通过 canonical event 更新 S5/parent task，重放并刷新 generated views；
- rehearsal 从 canonical events 无损生成 legacy-compatible snapshots，证明所有新 event 可回投影，
  但不执行真实 source rollback；
- manual row-move workflow 仅在两个 cycle、consumer=0 与 rollback PASS 后从治理规则移除。

### S5.4：最终树验证与发布

- focused、Ruff、strict mypy、Architecture、Contract、Report、Integration、Reproducibility、Full；
- task commit、local-main ff-only、ordinary push、SHA equality、branch/runner/shared-path cleanup；
- exact release 后才通知暂停的 TRADING-2505 coordinator 从 latest main 重新登记。

## 验收标准

- 973+ tasks 一任务一 canonical identity，final import task/status/priority/owner/blocker/acceptance/
  notes/docs links/all raw cells/terminal partition/order 100% parity；
- canonical register repeat build/replay deterministic，fragment/index/view bytes 可重算；
- Markdown 仅为 generated/do-not-edit compatibility views，manual writer=0；
- manual-Markdown semantic runtime consumer=0，所有保留 literal consumer 有 typed role/rollback；
- self-hosted task update 两个 governance cycles 与 event chain PASS；
- rollback rehearsal 证明 post-cutover events 无损投影且不覆盖 canonical authority；
- S2-S4 lease/scheduler/dispatch 行为不回退，自动 dispatch/merge/push/status guess 仍关闭；
- final formal/Full 与 ordinary push/cleanup PASS，`production_effect=none`、`broker_action=none`。

## 开放问题与退出条件

- 若 legacy document skeleton 无法被 exact seal 稳定生成，停止 cutover并保留 Markdown authority；
- 若任何 runtime semantic consumer 不能迁移到 canonical loader，则保持 S5 blocked，不以 generated
  path 名义掩盖仍存在的 manual-source dependency；
- rollback 只允许生成 owner-review snapshot；不得自动把 source_of_truth 切回 Markdown；
- ARCH-004H 的其他 aggregate retirement 不在本任务范围，D 的 report/catalog/flow shadow仍 inactive。

## Progress notes

- 2026-08-09：DEVX-006D ordinary push/cleanup release 完成；exact `HEAD=local main=origin/main`
  为 `7cfc81d6ab9d992c6c45e1084e22b33b9560d519`，runner=0、governed dirty=0。
  S5 从该 exact latest main 独立登记，尚未发生 implementation write 或 formal evidence 复用。
- 2026-08-09：final legacy import=`974 tasks / active 469 / completed 505 / ambiguous 55`，v1/v2
  replay 均 byte-identical；cycle 1 已建立 `ARCH_005_TASK_REGISTRY` authority、stable canonical
  fragments、forward index chain、sealed templates/generated views 与 cutover manifest。
- 2026-08-09：CLI/report semantic consumers 已改为 validated canonical loader；inventory 为
  `manual_semantic_runtime_consumer_count=0 / manual_writer_count=0`。现进入 self-hosted cycle、
  rollback rehearsal、compatibility/current-hash refresh 与 formal validation；automatic dispatch/
  merge/push、production、broker 仍为 false/none。
- 2026-08-10：self-host cycle、rollback rehearsal、strict mypy、Ruff、focused=`170 passed`、
  Architecture=`865 passed`、Contract=`276 passed`、Report=`57 passed`、Integration=`995 passed`、
  Reproducibility=`24 passed` 与首次 Full=`8702 passed / 3 skipped / 643 warnings` 均 PASS。
  canonical task event 现把 S5 转为 `DONE`；最终 DONE-tree revalidation、ordinary push、SHA equality、
  branch/runner/shared-path cleanup 作为同一 closeout 继续执行，`production_effect=none`、
  `broker_action=none`。
