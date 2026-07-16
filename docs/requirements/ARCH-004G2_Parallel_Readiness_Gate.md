# ARCH-004G2 Parallel Readiness Gate

最后更新：2026-07-12

## 任务信息

- task id：`ARCH-004G2_PARALLEL_READINESS_GATE`
- parent：`ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION`
- priority：`P1`
- status：`PROPOSED`
- owner：architecture coordinator
- dependency：整个 G2.4 phase exit、focused/architecture/contract/full validation、module/test manifest、compatibility baseline、deprecation inventory、source hashes、worktree attribution、commit/push 与 `arch_005_bootstrap_handoff.v1` validation 全部 PASS；handoff 固定 `next_slice_unblocked=false`
- foundation：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`；G2 作为其首个三 lane architecture rehearsal
- production effect：`none`

## 背景与目标

ARCH-004 已规定默认采用一个 integration coordinator 和最多三个互不重叠的 domain lane，worker 不得并行修改 root CLI、共享 schema、aggregate、task register、system flow 或 compatibility baseline。ARCH-004E 已建立 file-level ownership、impact selection、scaffold 和 fragment shadow aggregate，但当前仍缺少面向一次真实并行变更的 change manifest、owned-path 冲突检查、base commit drift 检查和 coordinator merge queue。

本任务作为 G2.5 的并行准备门禁，目标是在 G3 Reporting、G4 Operations、G5 Research Wrapper 三个 lane 启动前，把并行开发约束变成可执行、可验证的 contract。它是 `ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE` 的首个 architecture rehearsal，不另建临时 scheduler 或第二套 task/change registry。ARCH-005 的任何 S0 实现都必须等待整个 G2.4 phase-level handoff PASS；handoff 后 ARCH-004 停在 G2.5 之前，ARCH-005 S0/S1 通过且收到新的显式恢复指令后，本任务才可继续。G2 不负责迁移 domain runtime，也不改变现有 aggregate source-of-truth。

## 实施阶段

### P1：冻结剩余迁移与 ownership 输入

- 生成 G2.4 remaining migration matrix，至少记录 command/callback、current owner、canonical owner、owned paths、shared paths、contract、risk class、dependencies、tests 和 removal target；
- 全部 remaining callback 必须有唯一 owner；unknown owner、overlap 或未冻结 public contract 时 fail closed；
- G2.4 尚未完成时只允许维护 G2.4 自身的 ownership/migration inventory 与本需求合同，不得实施 ARCH-005 S0、创建新 registry/scheduler runtime，或提前声明 parallel-ready。

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

- 2026-07-12：根据 owner 对 ARCH-004 后续并行开发计划的确认登记为 `PROPOSED`。当前 G2.4 仍在推进；只有 G2.4 remaining migration matrix 和全部 callback ownership 闭合后，本任务才能转为 `READY`。
- 2026-07-12：owner 将并行研发调度提升为独立 `ARCH-005/P0/READY` 基础设施。G2 保持 `P1/PROPOSED` 的近期 architecture rehearsal；不再单独发明 task/change registry。最新 phase-level 决策取代原“ARCH-005 S0～S3 立即推进”表述：完整 G2.4 handoff PASS 前不得启动 S0；handoff 后 ARCH-004 停在 G2.5 前，S0/S1 通过且收到新的显式恢复指令后再进入本门禁。
