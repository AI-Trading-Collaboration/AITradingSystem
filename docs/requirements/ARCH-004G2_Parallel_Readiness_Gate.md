# ARCH-004G2 Parallel Readiness Gate

最后更新：2026-07-20

## 任务信息

- task id：`ARCH-004G2_PARALLEL_READINESS_GATE`
- parent：`ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION`
- priority：`P1`
- status：`PROPOSED`
- owner：architecture coordinator
- dependency：技术前置已满足：整个 G2.4 phase exit、formal validation、manifests/inventories、commit/push、`arch_005_bootstrap_handoff.v1` 与 ARCH-005 S0～S4A 均 PASS；治理前置仍未满足：handoff 固定 `next_slice_unblocked=false`，ARCH-004 G2.5 需要 owner 新的显式恢复指令
- foundation：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE` S0～S4A 与 S4B 双线 operating model；本任务负责把 G3/G4/G5 architecture wave 接入既有长期控制面
- production effect：`none`

## 背景与目标

ARCH-004 已规定默认采用一个 integration coordinator 和最多三个互不重叠的 domain lane，worker 不得并行修改 root CLI、共享 schema、aggregate、task register、system flow 或 compatibility baseline。ARCH-004E 已建立 file-level ownership、impact selection、scaffold 和 fragment shadow aggregate；ARCH-005 S2～S4A 又完成 change manifest、owned-path/module/contract 冲突、base drift、lease/replay、deterministic scheduler、isolated worktree 和 human-gated integration queue。因此 G2.5 不再实现第二套 primitive，而是把 G3/G4/G5 的真实 ownership 和 migration wave 接入既有控制面。

本任务作为 G2.5 的并行准备门禁，目标是在 G3 Reporting、G4 Operations、G5 Research Wrapper 启动前，把这些 architecture-specific fixtures、fragment preview 和 merge queue 接入 ARCH-005 contract。G2.4 phase exit、bootstrap handoff 与 ARCH-005 S0～S4A 已经完成；当前唯一启动 blocker 是 handoff 中保留的 `next_slice_unblocked=false` 和缺少 owner 对 ARCH-004 G2.5 的新显式恢复指令。G2 不负责重新实现 scheduler/lease，也不改变现有 aggregate source-of-truth。

## 当前前置证据

- G2.4 migration matrix=`967 migrated / 0 pending / 0 unresolved / 0 duplicate`；
- `arch_005_bootstrap_handoff.v1` 已验证、提交并推送；
- ARCH-005 S0/S1 shadow registry 与 byte-identical compatibility projection PASS；
- S2～S4 conflict/lease/scheduler/controlled dispatch 与 S4A supervised automation PASS；
- S4B 默认双线 operating model 已记录于 `docs/architecture/dual_lane_development_operating_model.md`；
- 上述完成不改变 `next_slice_unblocked=false`，因此本任务保持 `PROPOSED`，不得自动开始 G2.5。

## 实施阶段

### P1：冻结剩余迁移与 ownership 输入

- 生成 G2.4 remaining migration matrix，至少记录 command/callback、current owner、canonical owner、owned paths、shared paths、contract、risk class、dependencies、tests 和 removal target；
- 全部 remaining callback 必须有唯一 owner；unknown owner、overlap 或未冻结 public contract 时 fail closed；
- G2.4 尚未完成时只允许维护 G2.4 自身的 ownership/migration inventory 与本需求合同，不得实施 ARCH-005 S0、创建新 registry/scheduler runtime，或提前声明 parallel-ready。

当前 G2.4 matrix 已闭合；恢复 G2.5 后，本阶段只需从冻结的 phase-exit evidence 生成 G3/G4/G5
architecture-specific ownership snapshot，并重验相对新 base 是否漂移，不重做 G2.4 migration。

### P2：接入 ARCH-005 `change_manifest.v1`

由 ARCH-005 冻结长期 schema、parser 和 validator；G2 提供 G3/G4/G5 的 architecture-specific fixtures 与 acceptance。每个并行 slice 必须声明：

- `change_id`、`lane_id`、`base_commit`；
- `module_ids`、`owned_paths`、`shared_paths_requested`；
- `contract_versions`、`generated_outputs`、`removal_targets`；
- `validation_tiers`、`parallel_safety`；
- `production_effect`、`broker_action` 和 safety boundary。

### P3：实现 fail-closed overlap 与 drift validation

- 两个 active manifest 的 `owned_paths` 不得重叠；
- shared integration paths 只能由 coordinator manifest 持有；
- base commit drift、ownership mismatch、unknown module、contract version conflict、missing validation tier 和 unsafe production effect 必须阻断；
- worker 不得通过复制 shared helper 绕过冲突。

### P4：建立 fragment preview 与 merge queue

- worker 只写其 module、tests 和 report/artifact/flow fragments；
- fragment 到 aggregate 的 preview 必须 deterministic，并显示 duplicate、unknown owner、unsafe effect 和 source-of-truth diff；
- coordinator 集成顺序固定为 `contract -> adapter -> domain migration -> generated aggregate -> compatibility removal`；
- 当前 registry、catalog、system flow 的 source-of-truth 不因本任务自动切换。

### P5：三 lane readiness rehearsal

使用不修改投资逻辑的测试 fixture 证明：

- G3 Reporting、G4 Operations、G5 Research Wrapper 可以各自持有互不重叠的 paths；
- conflicting manifests 被拒绝，non-conflicting manifests 可以按固定顺序集成；
- impact-selected focused tests 只用于快速反馈，不能替代 lane/phase full validation。

## 后续并行波次

在 G2.4 handoff PASS、ARCH-005 S0/S1 PASS、新显式恢复指令以及本门禁 PASS 后，推荐执行顺序为：

1. G3 Reporting、G4 Operations、G5 Research Wrapper 三 lane 并行；
2. G6 的 `dynamic_v3_system_target`、`dynamic_v3_parameter_research`、`controlled_strategy_batch` 先并行 characterization，再由 coordinator 串行切换共同 contract 和投资解释敏感路径；
3. G7 统一生成 deprecation/reachability/removal ledger，并完成 H handoff；
4. ARCH-004H 按 surface 小批 cutover/removal。

## 与 ARCH-005 的交付边界

- ARCH-005 负责 canonical task/event registry、dependency DAG、readiness、`change_manifest.v1`、resource lease、scheduler decision、execution lane、event replay、integration queue 和 generated task views；
- G2 负责把当前 ARCH-004 ownership、remaining migration matrix、shared paths 和 G3/G4/G5 fixtures 接入这些长期 contracts；
- S0～S3 由 ARCH-005 在整个 G2.4 handoff PASS 后独立推进；ARCH-004 在 S0/S1 后仍需新的显式恢复指令才可进入 G2.5；
- G2 P3～P5 与 ARCH-005 S4 共用一次三 lane rehearsal 和验证证据，不重复实现或重复验收；
- G2 readiness PASS 只证明近期 lanes 的 contract/rehearsal 已满足，不自动授权 dispatch，也不代表 ARCH-005 已 self-hosted 或 task-register source-of-truth 已切换。

## 验收标准

- `change_manifest.v1` schema、parser、validator 和 deterministic id 完成；
- owned/shared path overlap、base drift、ownership mismatch、contract conflict 和 unsafe effect 均 fail closed；
- coordinator-only paths 和 merge order 有机器可读 contract；
- fragment preview deterministic，且不静默改变现有 aggregate source-of-truth；
- conflicting/non-conflicting/unknown-path/shared-path fixtures 覆盖；
- architecture-fitness、contract-validation、focused parallel pytest、Ruff 和 scoped mypy PASS；
- `production_effect=none`、`broker_action=none`，不改变策略、阈值、权重、DQ、PIT、回测、promotion、paper-shadow 或 production。

## 非目标

- 不在本任务内迁移 G3/G4/G5 runtime；
- 不自动批准 parallel shared-file edits；
- 不用 impact selection 替代 full validation；
- 不在 G2 内创建第二套 task register、change manifest、scheduler、system flow、report registry 或 architecture status；长期研发控制面统一由 ARCH-005 管理。

## 状态记录

- 2026-07-20：G2.4 phase exit、bootstrap handoff 与 ARCH-005 S0～S4A 已完成，原技术依赖已满足；
  owner 同日确认项目后续默认采用双线 operating model，但该决定不自动恢复 ARCH-004 G2.5。
  本任务继续保持 `PROPOSED`，唯一启动 blocker 为 owner 新的显式恢复指令；恢复后复用 ARCH-005
  primitives 和 S4B conflict protocol，不建立第二套 scheduler/lease/change manifest。
- 2026-07-12：根据 owner 对 ARCH-004 后续并行开发计划的确认登记为 `PROPOSED`。当前 G2.4 仍在推进；只有 G2.4 remaining migration matrix 和全部 callback ownership 闭合后，本任务才能转为 `READY`。
- 2026-07-12：owner 将并行研发调度提升为独立 `ARCH-005/P0/READY` 基础设施。G2 保持 `P1/PROPOSED` 的近期 architecture rehearsal；不再单独发明 task/change registry。最新 phase-level 决策取代原“ARCH-005 S0～S3 立即推进”表述：完整 G2.4 handoff PASS 前不得启动 S0；handoff 后 ARCH-004 停在 G2.5 前，S0/S1 通过且收到新的显式恢复指令后再进入本门禁。
