# DEVX-010：Base Drift Publication Preflight Binding V1

最后更新：2026-09-01

- stable task id：`DEVX-010_BASE_DRIFT_PUBLICATION_PREFLIGHT_BINDING`
- priority：`P0`
- status：`IN_PROGRESS`
- production effect：`none`
- broker action：`none`

## 1. 问题

RISK-012 从 frozen local-main `9e2e3f04a0092c5fe1477b88842135aa01834654`
完成后，local/main 已由独立且已验证的 TRADING-2552 推进到
`a6a0343b8afc975cbd17d39c63b75becead316b3`。受治理的
`integration_revalidation_plan.v1` 正确返回 `RECONCILIATION_REQUIRED`，且没有
contract conflict。

现有 publication/preflight 绑定却不能表达这一合法状态：

1. `integration_publication_fence.v1` 必须从 latest-main coordinator candidate base
   acquire，并要求 `expected_main` 是 transaction `lane_head` 的祖先；
2. `run-governed-development` INTEGRATION preflight 同时把 plan 的 frozen task
   `lane_head` 与当前 coordinator candidate HEAD 做强制相等比较；
3. 当 frozen lane 与 latest main 已分叉时，两项要求不可能同时成立，产生
   `INTEGRATION_REVALIDATION_BINDING_MISMATCH`。

这不是 RISK-012 domain conflict，也不能通过 rebase、merge、cherry-pick、放宽 plan 或跳过
publication fence 解决。此前 transaction
`risk-012-unknown-id-failclosed-20260901-v15` 已 fail closed 并释放 lease。

## 2. 预期修复

在 coordinator INTEGRATION 且 transaction 绑定 exact base-drift plan 时，preflight 应分别验证：

- plan `frozen_base` 等于 lane 的 frozen base；
- plan `lane_head` 保持原 frozen task lane identity；
- plan `latest_main` 等于 transaction `expected_main_sha`；
- 当前 coordinator HEAD 等于 transaction `lane_head_sha`，并且在 candidate mutation 前也等于
  exact latest main；
- transaction 绑定的 plan id/SHA 与传入 plan 完全一致；
- `RECONCILIATION_REQUIRED` 仍必须提供 exact reviewed plan id；
- 只有 plan 列出的 domain overlap 可由 coordinator reconcile，所有
  `COORDINATOR_REFRESH` bytes 必须从 final tree 重建；
- 无 transaction 的既有 lane-side预检、无 base drift、READY、SERIAL、tamper 和 CLOSEOUT
  语义保持不变。

不允许把 current coordinator HEAD 伪装成 frozen lane head，也不改变 publication fence 的
ancestry、lease、dirty attribution、candidate commit、formal Full 或 remote push 安全门禁。

## 3. Scope

- canonical skill：`tools/codex_skills/run-governed-development/`；
- installed skill：`C:\Users\32739\.codex\skills\run-governed-development\`，仅在 canonical
  focused validation 通过后同步 exact bytes；
- focused tests：`tests/test_governed_development_skill.py`；
- canonical task fragment、generated task views 与 architecture/compatibility authority。

当前复用既有 task-owned worktree `D:\Work\AITradingSystem_risk012_unknown_id`，branch=
`codex/devx-010-base-drift-publication-preflight`。DEVX-010 推送后该 worktree 将继续作为
RISK-012 coordinator candidate workspace，直到 RISK-012 完成 main/promotion/operations acceptance；
之后按原 RISK-012 lifecycle 审计并清理。它不得用来修改 root dirty checkout。

## 4. 验收标准

1. 新增 realistic regression：frozen lane 与 latest main 分叉、plan 为
   `RECONCILIATION_REQUIRED`、transaction 从 exact latest-main candidate base acquire 时，提供 exact
   reviewed plan id 后 INTEGRATION preflight PASS。
2. plan latest-main/transaction expected-main、transaction candidate-base/current HEAD、plan id/SHA
   任一漂移均 fail closed。
3. 无 transaction、错误 phase、错误 task、未复核 reconciliation、SERIAL contract wave、plan
   tamper 与普通无 drift 场景保持既有结果。
4. canonical/installed skill byte parity、quick validation、focused pytest-xdist、Architecture、Contract、
   Integration、Reproducibility 与 Full 全部 PASS。
5. validated candidate fast-forward local main、普通 non-force push，并确认
   `HEAD = local main = origin/main`。
6. `production_effect=none`、`broker_action=none`；不改变 DQ/PIT、scoring、strategy、weights 或
   trading behavior。

## 5. 进度记录

- 2026-09-01：从 RISK-012 exact base-drift replay 发现 typed control-plane contract gap；未执行
  rebase/merge/cherry-pick 或绕过 transaction。DEVX-010 作为最小 serial contract wave 登记。
- 2026-09-01：实现把 frozen plan lane 与 latest-main publication candidate base 分开绑定；transaction
  必须 exact-bind plan id/file SHA、expected main 与 candidate base HEAD。新增 candidate-base PASS 和
  expected-main/lane-head/plan missing/id drift fail-closed 回归；skill quick validation、Ruff 与
  pytest-xdist `46 passed`，canonical/installed 5 个 tracked files byte parity PASS。正式 generated/formal
  gates 仍绑定当前 publication transaction 后执行。
- 2026-09-01：v1 Architecture 完整执行为 `881 passed / 1 failed`；唯一失败是 canonical task
  registry 新增 DEVX-010 后，self-hosted exact-count ratchet 仍为 `1049`。失败 artifact=
  `outputs/validation_runtime/architecture-fitness_20260901T061840Z/test_runtime_summary.json`，v1 已
  fail closed 并释放 lease。v2 明确扩展 `tests/test_arch_005_s5_task_source_cutover.py`，把 reviewed
  exact count 更新为 `1050`；不改变 registry schema、task semantics 或任何业务路径。
