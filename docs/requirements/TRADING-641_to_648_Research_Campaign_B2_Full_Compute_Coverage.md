# TRADING-641 to 648 Research Campaign B2 Full Compute Coverage
最后更新：2026-06-23

版本：2026-06-19

## 背景

TRADING-631~640 已把 Research Campaign 从 audited-evidence control plane 推进到
rc2 execution-control-plane：B2 control-window compute smoke、B2 compute/audited
parity、`campaign run --stage next` smoke、B3 signal-only compute smoke、预算 forced
transition 和 status/plan UX 均已通过。当前限制是 B2 compute adapter 只覆盖
control-window diagnostic smoke，TARGETED_EVIDENCE、FULL_DIAGNOSTIC 和 GATE 尚未
完整通过 Campaign compute path 表达。

本阶段目标是扩展 B2 compute coverage，让 Campaign 能以研究只读方式运行 B2 targeted
evidence、full diagnostic 和 gate，并输出 end-to-end compute、legacy parity、
budget final-decision drill、legacy runner deprecation readiness 和 rc3 validation pack。

## 范围

- TRADING-641：B2 TARGETED_EVIDENCE compute adapter。
- TRADING-642：B2 FULL_DIAGNOSTIC compute adapter。
- TRADING-643：B2 GATE compute adapter，从 Campaign evidence store 和政策生成决策。
- TRADING-644：B2 Campaign end-to-end compute path。
- TRADING-645：B2 Campaign compute path vs legacy full path parity。
- TRADING-646：Campaign evidence budget final-decision drill。
- TRADING-647：Legacy B2 runner deprecation readiness。
- TRADING-648：Research Campaign Control Plane v1 rc3 validation pack。

## 非目标和安全边界

- 不启用 B3/B4/B5/B6/v3。
- 不启用 paper-shadow、extended shadow 或 live trading。
- 不生成 official target weights。
- 不接入 broker/order artifacts。
- 不发生 production mutation。
- 不访问 untouched holdout。
- 不调 B2 参数，不执行 tuning。
- 不伪造 metrics；compute artifacts 必须来自既有 B2 研究计算路径或 Campaign evidence
  store。
- 不删除 legacy task-specific runner，除非 parity、CLI、测试和文档全部满足且 owner
  明确要求。

## 实施顺序

1. 扩展 B2 compute adapter，支持 TARGETED_EVIDENCE、FULL_DIAGNOSTIC 和 GATE。
2. TARGETED_EVIDENCE 调用既有 B2 targeted-evidence 研究计算路径，并生成 trigger、
   drawdown、re-entry、cost/benchmark 和 control-window evidence records。
3. FULL_DIAGNOSTIC 调用既有 B2 full diagnostic 与 control-window evidence 路径；只有
   risk-heavy 与 control-window evidence 同时 validated 才能输出 COMPLETE。
4. GATE 从 Campaign evidence store、B2 gate policy、budget 和 stop rules 生成 constrained
   decision；预算耗尽时禁止 generic `NEEDS_MORE_EVIDENCE`。
5. 在临时 Campaign root 中跑 B2 E2E compute path，避免污染 canonical state。
6. 对比 Campaign compute path 与 legacy B2 full artifacts 的 status、reason codes、
   evidence categories、next actions、blocked actions 和 safety metadata。
7. 生成 budget final-decision drill 和 legacy B2 runner readiness。
8. 生成 rc3 validation pack 并同步 system flow、artifact catalog、release note、task
   register 和 focused tests。

## 验收标准

- B2 targeted compute 输出 `B2_TARGETED_EVIDENCE_COMPUTE_PASS` 或带原因的 blocked status。
- B2 full diagnostic compute 输出 `B2_FULL_DIAGNOSTIC_COMPLETE`、
  `B2_FULL_DIAGNOSTIC_PARTIAL` 或 `B2_FULL_DIAGNOSTIC_BLOCKED`。
- B2 gate compute 输出 constrained B2 decision，不允许预算耗尽后继续泛化
  `NEEDS_MORE_EVIDENCE`。
- B2 E2E compute 输出 `B2_CAMPAIGN_E2E_COMPUTE_PASS`、
  `B2_CAMPAIGN_E2E_COMPUTE_PASS_WITH_LIMITATIONS` 或 `B2_CAMPAIGN_E2E_COMPUTE_BLOCKED`。
- Legacy parity 输出 `B2_CAMPAIGN_FULL_PARITY_PASS`、
  `B2_CAMPAIGN_FULL_PARITY_PASS_WITH_EXPLAINED_DIFFS` 或
  `B2_CAMPAIGN_FULL_PARITY_FAIL`；失败且不可解释时不得弃用 legacy runner。
- Evidence budget final-decision drill 输出 `CAMPAIGN_EVIDENCE_BUDGET_FINAL_DECISION_PASS`。
- Legacy B2 runner readiness 输出 keep/deprecate/not-ready，且不删除旧命令。
- rc3 validation pack 输出 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY`、
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS` 或
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_BLOCKED`。
- focused tests、Ruff、compileall、JSON parse、B2/B3 campaign validate、
  validation-pack 和 `git diff --check` 通过。

## 当前状态

2026-06-19：实现完成并进入 `VALIDATING`。B2 Campaign compute adapter 已覆盖
TARGETED_EVIDENCE、FULL_DIAGNOSTIC 和 GATE；targeted compute 输出
`B2_TARGETED_EVIDENCE_COMPUTE_PASS`，full diagnostic 输出
`B2_FULL_DIAGNOSTIC_COMPLETE`，gate compute 输出 constrained B2 decision；E2E drill 输出
`B2_CAMPAIGN_E2E_COMPUTE_PASS_WITH_LIMITATIONS`，budget final-decision drill 强制
`OWNER_OVERRIDE_REQUIRED`，legacy B2 runner readiness 保持
`LEGACY_B2_RUNNER_KEEP_COMPATIBILITY_LAYER`。RC3 validation pack 在默认入口输出
`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。本批次未扩大研究范围，
未触碰 untouched holdout，未放开 B3/B4/B5/B6/v3、paper-shadow/live、official weights、
broker/order 或 production mutation。
