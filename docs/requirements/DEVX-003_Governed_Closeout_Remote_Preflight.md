# DEVX-003：Governed Closeout Remote Preflight

最后更新：2026-07-26

稳定任务 ID：`DEVX-003_GOVERNED_CLOSEOUT_REMOTE_PREFLIGHT`

Owner continuation：
`owner_continuation:DEVX-003:2026-07-26:continue_long_term_engineering_goal`

状态：`BASELINE_DONE`

## 1. 问题

DATA-GOV-002 Phase B2 首次真实使用 DEVX-002 v2 closeout 流程时，在 validated
candidate fast-forward 到 local `main` 后执行：

```text
SINGLE_LANE / coordinator / CLOSEOUT / --remote-action
```

preflight 返回唯一 blocker：

```text
MUTATION_STAGE_ON_MAIN
```

原因是 `preflight.py` 当前把 `main` 上所有非 `START` stage 一律视为 mutation，但
`SKILL.md` 和 `workflow-modes.md` 又要求 local-main integration 后运行 closeout
preflight。这使文档规定的正式入口无法成功执行。

同一审计还确认：当前 `--remote-action` 只验证 `origin/main` 是否存在，没有把
`origin_only > 0`、非 `main` remote action 或 dirty local-main checkout 转为 typed
blocker。实际 push 仍可由人工祖先检查保护，但 deterministic preflight 没有完整表达
DEVX-002 v2 的 fail-closed remote contract。

本次 B2 使用项目明确允许的 equivalent READ_ONLY remote preflight，加显式
`origin/main` ancestry、单提交、普通 push 和双 SHA 复核完成收口；该路径只作为事故
处置证据，不成为新的隐式默认。

## 2. 目标

让 canonical preflight 直接、确定性地表达 validated local-main remote closeout：

1. `SINGLE_LANE` 或 `DUAL_LANE` coordinator 可在 local `main` 上运行
   `stage=CLOSEOUT --remote-action`；
2. 该组合是只读 publication gate，不被 `MUTATION_STAGE_ON_MAIN` 拦截；
3. remote closeout 必须发生在 `main`，否则 typed fail closed；
4. remote closeout 必须使用 `--remote-action`，否则 typed fail closed；
5. worktree audit 必须 PASS 且 task-owned dirty inventory 为空；
6. `origin/main` 必须存在，且 `origin_only=0`；local candidate 可以领先；
7. remote 与 candidate 相等或是 candidate 祖先时 PASS；
8. preflight 不执行 fetch、push、merge、rebase、PR、history rewrite 或 force-push；
9. push 后双 SHA 复核仍由 workflow 明确要求。

## 3. 合同设计

### 3.1 Branch/stage contract

- `START` 可以在 local `main` 上运行；
- `LANE`、`INTEGRATION` 仍不得在 `main` 上运行；
- `CLOSEOUT` 在任务分支上可用于 branch final-tree gate；
- `CLOSEOUT` 在 `main` 上只在 coordinator + `--remote-action` 时合法。

### 3.2 Remote-action blockers

新增或冻结以下 typed blockers：

- `REMOTE_ACTION_REQUIRES_MAIN`；
- `REMOTE_ACTION_REQUIRES_COORDINATOR`；
- `MAIN_CLOSEOUT_REQUIRES_REMOTE_ACTION`；
- `REMOTE_ACTION_DIRTY_WORKTREE`；
- `REMOTE_MAIN_UNAVAILABLE`；
- `REMOTE_MAIN_NOT_CANDIDATE_ANCESTOR`。

`origin_only > 0` 无论 local 是否同时领先，都必须阻断；工具不得自动 fetch、merge、
rebase 或修复 divergence。

### 3.3 Evidence

输出继续披露：

- current branch、HEAD/local-main/origin-main；
- local-only/origin-only；
- worktree audit、known-unrelated exclusion；
- active leases；
- remote action requested；
- typed blocker/status；
- production/broker boundary。

## 4. 实施步骤

1. 登记 DEVX-003 并冻结上述合同；
2. 提取可单元测试的 branch/stage/remote gate evaluator；
3. 更新 canonical `preflight.py`、`SKILL.md` 和 `workflow-modes.md`；
4. 增加 main closeout PASS、remote ahead、dirty、non-main、missing
   `--remote-action` 和 worker role 负例；
5. 同步 installed skill，验证 canonical/installed byte parity；
6. 运行 focused、Black、Ruff、strict mypy、skill validation、architecture/contract
   和适用的 Full；
7. 通过修复后的 `CLOSEOUT --remote-action` 完成本任务自身远端收口。

## 5. 验收标准

- DATA-GOV-002 B2 暴露的 exact main-closeout 场景由 canonical preflight PASS；
- 上述所有非法场景均返回稳定 typed blocker；
- remote gate 不修改 Git refs、文件或外部状态；
- canonical/installed skill byte-identical；
- task registry、compatibility authority 和相关 generated manifests fresh；
- final-tree validation、local-main fast-forward、普通 push 和双 SHA 复核通过；
- `production_effect=none`、`broker_action=none`。

## 6. 安全边界

- 不改变策略、数据质量判断、回测、报告结论或任何投资解释；
- 不授权自动 push、PR、force-push、history rewrite 或 divergence repair；
- 不读取、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- 不使用或删除其他现存 worktree、stash 或 runtime workspace；
- canonical skill 由 Git 恢复；installed skill 可由 canonical bundle重新部署。

## 7. 进度

- 2026-07-26：DATA-GOV-002 B2 closeout 真实暴露
  `MUTATION_STAGE_ON_MAIN` 与 remote divergence 未结构化阻断的合同缺口。按
  DEVX-002 的“发现新边界另建 follow-up”规则建立 DEVX-003，状态转
  `IN_PROGRESS`。
- 2026-07-26：canonical preflight 已提取 branch/stage/remote evaluator；clean
  main、remote ancestor/equal 正例和 non-main、worker、dirty、missing remote、
  remote ahead、wrong stage 等负例均由 typed blocker 覆盖。canonical 与
  installed bundle 5 个文件 byte-identical，skill validation、Black、Ruff、
  strict mypy 和 focused `25 passed`。
- 2026-07-26：兼容性账本专项 `64 passed`；task registry byte-identical；
  architecture DevEx PASS；正式 architecture `672 passed`、contract
  `275 passed`、reproducibility `23 passed`、Full
  `7365 passed / 3 skipped / 643 warnings`。Full 证据：
  `outputs/validation_runtime/full_20260726T112548Z/test_runtime_summary.json`。
  状态转 `VALIDATING_LOCAL_MAIN_CLOSEOUT`，下一步仅剩 candidate commit、
  local-main fast-forward、真实 `CLOSEOUT --remote-action` 和 ordinary push
  双 SHA 复核。
- 2026-07-26：validated candidate `69ef2555c04c99d7acb3b53d72640727f6e3546f`
  已 fast-forward 到 clean local `main`。刷新 remote 后，installed preflight 以
  `SINGLE_LANE / coordinator / CLOSEOUT / --remote-action` 和原 scope claims
  返回 `PASS`：`local_only=1`、`origin_only=0`、无 blocker、无 warning、
  worktree audit PASS、active lease 为空。状态转 `BASELINE_DONE`；该结果证明
  `origin/main` 为 candidate 祖先时正式入口可用，最终 evidence commit 后执行
  ordinary push 与双 SHA 复核。
