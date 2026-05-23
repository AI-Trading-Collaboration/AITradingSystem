# Parameter Governance Web View Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-020 生成只读 Parameter Governance Web View，把 TRADING-019 summary 中已有的
governance state、action level、production/shadow weights、promotion lifecycle、pending
items、safety boundary 和 artifact paths 渲染成单文件静态 HTML。

TRADING-020 不生成新的治理判断，不执行 promotion、apply、rollback、broker、replay 或
trading execution。

## 2. 如何生成 web view

```bash
python scripts/render_parameter_governance_web_view.py --date 2026-05-23
```

默认输出：

```text
data/derived/weight_iterations/governance/web/
  parameter_governance_web_view_YYYY-MM-DD.html
  parameter_governance_web_view_YYYY-MM-DD.json
```

如果不传 `--date`，脚本会读取 latest
`data/derived/weight_iterations/governance/parameter_governance_summary_*.json`。

## 3. 指定 governance summary file

```bash
python scripts/render_parameter_governance_web_view.py \
  --governance-summary-file data/derived/weight_iterations/governance/parameter_governance_summary_2026-05-23.json
```

可选输出路径：

```bash
python scripts/render_parameter_governance_web_view.py \
  --date 2026-05-23 \
  --output-file data/derived/weight_iterations/governance/web/custom.html \
  --metadata-file data/derived/weight_iterations/governance/web/custom.json
```

`--open-browser false` 是默认值；即使设为 `true`，也只是打开本地 HTML 文件，不运行 pipeline。

## 4. governance_state 如何展示

页面 header 展示：

- `governance_state`
- `action_required`
- `action_level`
- `recommended_action`

`SAFETY_ANOMALY` 会在顶部显示 `URGENT: Safety Anomaly Detected`，并重复展示 critical
findings。`ROLLBACK_COMPLETED` / safe observation 使用正常样式；shadow learning / applied
monitoring 使用 watch 样式；review / approval required 使用 attention 样式。

## 5. 阅读 production vs shadow weights

权重表按 key 展示 production、shadow 和 delta：

- production 缺失显示 `NOT_AVAILABLE`。
- shadow 缺失显示 `NOT_AVAILABLE`。
- delta 使用正负号。
- production / shadow key 不一致时显示 warning。

这个表只展示 TRADING-019 summary 中已有的权重与 delta，不写 production 或 shadow 文件。

## 6. 阅读 promotion lifecycle timeline

Timeline 固定展示：

```text
Proposal -> Preflight -> Apply -> Rollback -> Lifecycle Audit
```

每个 stage 显示 status、decision、executed 和 artifact path。页面只解释已有 artifact，不补跑
018D、018E1、018E2、018E3 或 018F。

## 7. 阅读 pending items

`pending_items` 包括：

- `pending_proposal_review`
- `pending_preflight`
- `pending_apply`
- `pending_rollback`
- `pending_lifecycle_audit`

`pending_apply=true` 时页面显示 `Manual approval/apply may be required.`。这不是授权，也不会触发
apply；真正 apply 仍只能由 TRADING-018E2 显式命令完成。

## 8. 处理 SAFETY_ANOMALY

如果页面显示 `URGENT: Safety Anomaly Detected`，先阅读 critical findings 和 safety boundary
audit。重点检查：

- 018F lifecycle audit 是否已经输出 safety anomaly；
- summary 或上游 artifact 是否记录 `broker_execution=true`、`replay_execution=true` 或
  `trading_execution=true`；
- apply / rollback 执行状态和 decision 是否矛盾；
- rollback snapshot 或 post validation 是否缺失或失败。

不要通过编辑 HTML 或 summary JSON 清除异常；应修复或重新生成源 artifact 后再渲染 web view。

## 9. 处理 SAFETY_BLOCKED render

`SAFETY_BLOCKED` 表示 TRADING-019 summary 顶层安全字段不满足 web view 渲染前置条件。常见原因：

- `production_effect != none`
- `governance_only != true`
- `apply_executed_by_governance != false`
- `rollback_executed_by_governance != false`
- `broker_execution` / `replay_execution` / `trading_execution` 不是 `false`

此时 HTML 是 blocked report，不是正常 dashboard。先检查 summary 生成过程和源 artifact，不要把
blocked report 当成治理状态正常。

## 10. 为什么 web view 不执行 apply/rollback

Web view 是人工阅读层。把 apply / rollback 接到页面或 dashboard 会让静态展示层具备生产修改能力，
破坏 TRADING-018E2 / 018E3 的单独 approval、danger flag、hash gate 和 post validation 边界。

因此 TRADING-020 固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "governance_only": true,
  "web_view_only": true,
  "apply_executed_by_web_view": false,
  "rollback_executed_by_web_view": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

## 11. Dashboard 读取边界

Daily task dashboard 只能读取
`data/derived/weight_iterations/governance/web/parameter_governance_web_view_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-020 render script，也不运行 TRADING-019 summary 或
018B、018C、018C2、018D、018E1、018E2、018E3、018F，不触发 scoring、broker、replay 或
trading execution。
