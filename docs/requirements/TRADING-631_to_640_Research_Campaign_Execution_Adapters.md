# TRADING-631 to 640 Research Campaign Execution Adapters

版本：2026-06-19

## 背景

TRADING-623~630 已把 Research Campaign 推进到 Control Plane v1，并形成
`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS` validation pack。
当前限制是 Campaign 可以汇总 audited artifacts，但还不能最小化替代 B2/B3 的
task-specific execution flow。

本阶段目标是把 Campaign 升级为最小 execution-control-plane：至少一个 B2 safe
diagnostic stage 通过真实 compute adapter 执行，并用 parity validation 证明其输出
和既有 audited evidence 一致或差异可解释。B3 只允许 signal-only smoke。

## 范围

- TRADING-631：复验、提交并推送 TRADING-623~630 validation pack。
- TRADING-632：定义 adapter run modes 和输出字段，Reader Brief 披露 run mode。
- TRADING-633：实现最小 B2 compute adapter smoke。
- TRADING-634：验证 B2 compute vs audited evidence parity。
- TRADING-635：实现并测试 `campaign run --stage next` safe B2 smoke。
- TRADING-636：验证 evidence budget forced transition。
- TRADING-637：实现或验证 B3 signal-only compute adapter smoke。
- TRADING-638：改善 Campaign status/plan/blocked/allowed/budget/source-artifacts UX。
- TRADING-639：定义旧 B2/B3 task-specific runner deprecation boundary。
- TRADING-640：生成 Research Campaign Control Plane v1 rc2 validation pack。

## 非目标和安全边界

- 不启用 paper-shadow、extended shadow 或 live trading。
- 不生成 official target weights。
- 不接入 broker/order artifacts。
- 不发生 production mutation。
- 不访问 untouched holdout。
- 不启用 B4/B5/B6/v3。
- 不调 B2 参数，不用 compute adapter 执行 tuning。
- 不把 imported audited evidence 标记为 fresh computation。

所有 adapter 输出必须披露：

```text
research_only = true
manual_review_only = true
official_target_weights = false
paper_shadow_activation = false
broker_effect = none
order_effect = none
production_effect = none
holdout_touched = false
```

## 实施顺序

1. 完成 TRADING-631 复验、提交和推送，记录 commit hash 与 validation status。
2. 扩展 adapter contract，加入 `AUDITED_ARTIFACT_MODE`、`COMPUTE_MODE`、
   `DRY_RUN_MODE`、`VALIDATION_ONLY_MODE`。
3. 为 adapter output 增加 `compute_performed`、`imported_evidence`、
   `parity_source`、`failure_mode` 和 safety metadata 字段，并更新 Reader Brief。
4. 接入最小 B2 compute adapter，调用既有 B2 risk overlay 计算路径，只处理安全诊断
   window，生成 fresh run artifact 和 evidence records。
5. 对比 B2 compute output 和 audited artifacts，输出 PASS、PASS_WITH_EXPLAINED_DIFFS
   或 FAIL；FAIL 且无解释时禁止后续 Campaign compute stage。
6. 让 `campaign run --stage next` 通过 state machine/plan 选择允许的 B2 safe stage，
   并在 adapter missing、budget exhausted、holdout 越权时 fail-closed。
7. 补充 evidence budget forced transition 测试，确保 exhausted budget 不再泛化输出
   `NEEDS_MORE_EVIDENCE`。
8. 接入 B3 signal-only smoke，禁止 weights、mini-backfill、B4/B5/B6/v3。
9. 改善 status/plan UX，并输出旧 runner deprecation boundary。
10. 生成 rc2 validation pack，披露 compute adapter smoke、parity、budget、holdout、
    UX 和 forbidden-effects 检查。

## 验收标准

- TRADING-631 已推送，状态保持
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。
- adapter contract validation 能识别 run mode 和必需 output 字段。
- AUDITED_ARTIFACT_MODE 不声称 fresh computation。
- COMPUTE_MODE 必须真实调用计算逻辑。
- B2 compute smoke 输出 `B2_COMPUTE_ADAPTER_SMOKE_PASS` 或带原因的 blocked status。
- B2 parity 输出 `B2_COMPUTE_PARITY_PASS` 或
  `B2_COMPUTE_PARITY_PASS_WITH_EXPLAINED_DIFFS`，否则 hard stop。
- `campaign run --stage next` smoke 可执行或明确 fail-closed。
- evidence budget exhausted 时强制转向 `NARROW_ROLE`、`RETURN_TO_DESIGN`、`WEAK`、
  `REJECTED` 或 `OWNER_OVERRIDE_REQUIRED`。
- B3 signal adapter 只输出 signal-direction evidence，不输出 weights/backfill。
- Campaign status/plan 能直接显示 current stage、outcome、reason codes、budget、
  allowed/blocked actions、owner actions、safety boundary 和 adapter run mode。
- deprecation boundary 明确旧 commands、replacement、parity status、compatibility
  window、未替代命令和 blockers。
- rc2 validation pack 为 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY`、
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS` 或
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_BLOCKED`。
- focused tests、Ruff、compileall、JSON parse、B2/B3 campaign validate、
  validation-pack 和 `git diff --check` 通过。

## 当前状态

2026-06-19：进入 `IN_PROGRESS`。TRADING-631 已完成：复验 focused pytest 15
passed、task-register terminal-status check、Ruff、compileall、JSON parse、adapter
validation、B2/B3 campaign validation、validation pack 和 `git diff --check` 均通过；
提交并推送 `ba323dd1`，validation pack 状态保持
`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。

2026-06-19：实现完成并转入 `VALIDATING`。新增 adapter run mode contract、B2
control-window compute adapter、B2 compute/audited parity、`campaign run --stage next`
safe B2 smoke、evidence budget forced transition report、B3 signal-only compute smoke、
Campaign status/plan/blocked/allowed/budget/source-artifacts UX、case-specific runner
deprecation plan 和 Control Plane v1 rc2 validation pack。当前 rc2 输出状态仍为
`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`，限制是 B2 compute adapter
只覆盖 control-window diagnostic smoke，B3 仍为 signal-only smoke，旧 task-specific
runners 仍需保留到 owner review 和兼容性覆盖完成。验证通过 focused pytest 17 passed、
Ruff、compileall、JSON parse、adapter validation、B2/B3 campaign validate、CLI UX smoke、
validation pack 和 `git diff --check`。
