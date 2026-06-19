# Secret Hygiene Generated Report False Positive

最后更新：2026-06-19

## 背景

2026-06-18 的每日运营入口 `aits ops daily-run` 选择
`as_of=2026-06-18`，35 个步骤均被调度，前 34 个步骤通过，最后的
`secret_hygiene` 以 fail-closed 结束，导致整体 daily-run 状态为 `FAIL`。

Run id:
`daily_ops_run:2026-06-18:20260618T223154Z`

Run bundle:
`D:\Work\AITradingSystem\outputs\runs\daily\20260618T223154Z\as_of_2026-06-18__daily_ops_run_2026-06-18_20260618T223154Z`

本轮未删除或改写任何历史报告，也未修改 scanner、allowlist、业务代码、
production weights、active shadow weights、broker/order 或 trading action。

## 观察到的 blocker

`outputs/reports/secret_hygiene_2026-06-18.md` 报告：

- status: `FAIL`
- scanned files: 9639
- errors: 12
- warnings: 0
- production_effect: `none`

命中集中在以下 generated reports：

- `outputs/reports/executable_binding_safety_audit_2026-06-17.json`
- `outputs/reports/executable_binding_safety_audit_2026-06-17.md`
- `outputs/reports/next_candidate_research_cycle_snapshot_2026-06-17.json`

脱敏核对显示命中内容来自 `api_key` finding id、warnings 汇总或
sensitive-field label metadata，例如安全审计报告在说明需要检查 `api_key`
字段名时被 secret scanner 当作疑似 secret literal。当前未确认有完整 API key
或 credential value 泄漏。

## Intended Best Solution

修复 secret hygiene 与 generated governance report 的契约，使 scanner 能区分：

- 真实 secret value；
- 只用于审计说明、finding id、field label 或 redaction policy 的敏感字段名；
- generated report 中的 source-code path / finding metadata。

优先候选方案：

1. 调整 generated report 序列化，避免把 `api_key:<path>:<line>` 这类
   finding id 写成会被 secret literal rule 匹配的值。
2. 或在 secret scanner 中新增结构化上下文识别，只对 generated report 的
   approved metadata field 做窄范围降级，同时继续 fail closed 检测真实 value。
3. 补充回归测试，覆盖 generated report metadata、真实 secret value、
   redaction 输出和 daily-run secret hygiene gate。

不得使用 broad allowlist 绕过 `outputs/`，不得删除历史审计报告作为通过手段，
不得降低真实凭据泄漏的 fail-closed 行为。

## 当前为何未直接修复

本次自动化请求明确要求执行周期性运营入口，并要求不得修改业务代码。修复需要
改动 scanner 或报告生成逻辑并补测试，超出本次运营执行边界。

因此本轮只按 no-silent-workaround 记录 blocker，不实施临时 workaround。

## 行为影响

- `validate-data` 已 PASS。
- PIT validation 已 PASS。
- `score-daily`、dashboard/latest checks、Reader Brief 和
  `validate-reader-brief` 均已执行。
- Reader Brief quality 为 `LIMITED_READER_CONTEXT`，仍缺 `trace_bundle` 重要上下文。
- 由于 secret hygiene fail closed，daily-run 总体状态必须保持 `FAIL`，
  后续结论不得视为完整运营通过。

## 风险

- 若这是 scanner false positive，当前风险是 daily-run 被 generated audit metadata
  阻断，影响每日运营完整性。
- 若其中存在真实 credential value，风险是敏感信息已进入 `outputs/reports`，
  必须立即人工复核、撤销/轮换相关 credential，并清理泄漏 artifact。
- 在修复前，不能把 secret hygiene 失败降级为普通 warning。

## 验证覆盖要求

修复完成前至少覆盖：

- generated report finding id / warning summary 不触发 `suspected_secret_literal`；
- `api_key`、`secret_key`、`token`、`password` 等字段名说明不会被误判为 value；
- 真实 token-like secret value 仍触发 ERROR；
- secret hygiene 报告继续只输出 redacted snippet；
- `aits security scan-secrets --as-of 2026-06-18` 对同一 artifact 集合通过或输出
  已审定 warning；
- `aits ops daily-run` 可完成 secret hygiene gate。

## Exit Condition

退出条件：

1. 对 2026-06-18 artifact 集合 rerun secret hygiene 不再因 generated metadata fail；
2. 回归测试证明真实 secret value 仍 fail closed；
3. daily-run 或等价 replay/validation artifact 显示 secret hygiene gate 通过；
4. task register 更新状态，并在日报/运营摘要中保留本次事件的修复说明。
