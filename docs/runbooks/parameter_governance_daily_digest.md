# Parameter Governance Daily Digest Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-021 生成只读 Parameter Governance Daily Digest，把 TRADING-019 summary 中已有的
governance state、action level、pending items、safety boundary、production/shadow weights、
review decision、promotion lifecycle 和 findings 压缩成每日快速阅读摘要。

TRADING-021 不生成新的治理判断，不执行 promotion、apply、rollback、broker、replay 或
trading execution。

## 2. 如何生成 daily digest

```bash
python scripts/run_parameter_governance_daily_digest.py --date 2026-05-23
```

默认输出：

```text
data/derived/weight_iterations/governance/digests/
  parameter_governance_daily_digest_YYYY-MM-DD.json
  parameter_governance_daily_digest_YYYY-MM-DD.md

data/derived/weight_iterations/governance/digests/logs/
  parameter_governance_daily_digest_run_YYYY-MM-DD.json
  parameter_governance_daily_digest_run_YYYY-MM-DD.md
```

常用参数：

```bash
python scripts/run_parameter_governance_daily_digest.py \
  --date 2026-05-23 \
  --data-root data \
  --lookback-days 7 \
  --governance-summary-file data/derived/weight_iterations/governance/parameter_governance_summary_2026-05-23.json
```

可选 `--web-view-metadata-file` 指定 TRADING-020 metadata。`--fail-on-safety-anomaly` 会在写出
artifact 后，对 safety anomaly 以非零状态退出。

## 3. digest_status 含义

|digest_status|含义|
|---|---|
|`OK`|治理状态稳定，无立即人工动作。|
|`WATCH`|治理状态可继续观察，但需要监控 shadow learning 或 post-apply monitoring。|
|`ACTION_REQUIRED`|存在 review / approval / rollback review 等人工动作。|
|`URGENT`|存在 safety anomaly 或 critical findings，需要立即人工检查。|
|`INPUT_MISSING`|找不到 TRADING-019 governance summary。|
|`INPUT_INVALID`|summary 无法解析或 `task_id` 不是 `TRADING-019`。|
|`SAFETY_BLOCKED`|summary 顶层安全字段异常，digest 只能生成 blocked 摘要。|
|`ERROR`|digest 运行异常。|

## 4. summary_level 含义

|summary_level|来源|
|---|---|
|`NORMAL`|`digest_status=OK`|
|`WATCH`|`digest_status=WATCH`|
|`ACTION`|`digest_status=ACTION_REQUIRED`|
|`URGENT`|`digest_status=URGENT`|
|`UNKNOWN`|input missing / invalid / safety blocked / error|

## 5. 如何解读 Today's Status

Today's Status 汇总 digest status、TRADING-019 `governance_state`、`action_required`、
`action_level` 和 `recommended_action`。这里不是新的治理判断，只是把 TRADING-019 的核心字段放到
每日阅读入口。

## 6. 如何解读 Pending Actions

Pending Actions 展示：

- `pending_proposal_review`
- `pending_preflight`
- `pending_apply`
- `pending_rollback`
- `pending_lifecycle_audit`

`pending_apply=true` 只表示 preflight 后存在人工 apply 决策点，不授权 apply。真正 apply 仍只能由
TRADING-018E2 显式命令和单独 approval artifact 完成。

## 7. 如何处理 ACTION_REQUIRED

先阅读 digest 的 Suggested Next Steps 和 TRADING-019 summary。常见处理：

- review pending proposal 或 multi-day shadow evidence；
- 检查 preflight 是否已经 PASS；
- 不要在缺单独 approval artifact、danger flag 和 target hash 的情况下运行 apply 或 rollback。

## 8. 如何处理 URGENT

`URGENT` 表示 safety anomaly 或 critical findings。先停止依赖自动 governance 输出做后续判断，检查：

- TRADING-019 `audit_findings.critical_findings`；
- TRADING-018F lifecycle audit；
- 是否有 `broker_execution=true`、`replay_execution=true` 或 `trading_execution=true`；
- apply / rollback executed 与 decision 是否矛盾；
- rollback snapshot 或 post validation 是否缺失或失败。

不要通过编辑 digest、summary 或 web view artifact 清除异常。

## 9. 如何处理 SAFETY_BLOCKED

`SAFETY_BLOCKED` 表示 TRADING-019 summary 顶层安全字段不满足 digest 前置条件。常见原因：

- `production_effect != none`
- `governance_only != true`
- `apply_executed_by_governance != false`
- `rollback_executed_by_governance != false`
- `broker_execution` / `replay_execution` / `trading_execution` 不是 `false`

Blocked digest 不是正常治理状态摘要。先修复或重新生成 TRADING-019 summary，再重新生成 digest。

## 10. 为什么 digest 不执行 apply / rollback

Digest 是每日阅读层。把 apply / rollback 接到 digest 或 dashboard 会绕过 TRADING-018E2 / 018E3
的单独 approval、danger flag、hash gate、rollback snapshot 和 post validation 边界。

因此 TRADING-021 固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "digest_only": true,
  "governance_only": true,
  "apply_executed_by_digest": false,
  "rollback_executed_by_digest": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

## 11. digest 与 TRADING-019 / TRADING-020 的关系

TRADING-019 是治理状态 source summary，负责聚合 production/shadow weights、review、proposal、
preflight、apply、rollback、lifecycle audit、pending items 和 safety boundary。

TRADING-020 是完整静态 Web View，适合下钻阅读。

TRADING-021 只读取已有 summary 和可选 web metadata，把信息压缩成每日摘要。它不运行 TRADING-019，
不渲染 TRADING-020，不触发 018B-018F、scoring、broker、replay 或 trading execution。
