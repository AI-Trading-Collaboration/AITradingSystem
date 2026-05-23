# Parameter Governance Summary Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-019 生成只读 Parameter Governance Summary，用一个 artifact 汇总 production
weights、current shadow weights、latest 018C2 review、018D proposal、018E1 preflight、
018E2 apply、018E3 rollback 和 018F lifecycle audit 的当前状态。它回答当前治理状态、
是否需要人工行动、是否有 pending apply / rollback / lifecycle audit，以及是否出现 safety
anomaly。

TRADING-019 不执行 promotion、apply、rollback、broker、replay 或 trading execution。

## 2. 手动运行

```bash
python scripts/run_parameter_governance_summary.py --date 2026-05-23
```

常用参数：

```bash
python scripts/run_parameter_governance_summary.py \
  --date 2026-05-23 \
  --data-root data \
  --production-profile config/weights/weight_profile_current.yaml \
  --shadow-weights-file data/derived/weight_iterations/shadow/current_shadow_weights.json \
  --lookback-days 14
```

默认输出：

```text
data/derived/weight_iterations/governance/
  parameter_governance_summary_YYYY-MM-DD.json
  parameter_governance_summary_YYYY-MM-DD.md

data/derived/weight_iterations/governance/logs/
  parameter_governance_summary_run_YYYY-MM-DD.json
  parameter_governance_summary_run_YYYY-MM-DD.md
```

可加 `--fail-on-safety-anomaly`，让命令在写出 summary artifact 后以非零状态退出。

## 3. governance_state 含义

|State|含义|
|---|---|
|`SAFE_OBSERVATION`|production 可读、无 pending action、无 safety anomaly。|
|`SHADOW_LEARNING`|current shadow weights 可读，但尚未进入强 review / proposal / preflight / apply 状态。|
|`SHADOW_REVIEW_READY`|latest 018C2 review 显示 `SHADOW_LOOKS_BETTER`，但还没有 proposal。|
|`PROPOSAL_PENDING_REVIEW`|018D proposal 等待人工 review 或 preflight。|
|`PREFLIGHT_READY`|018E1 preflight `PASS`，但 apply 尚未执行。|
|`APPLIED_NEEDS_MONITORING`|018E2 apply 已 `APPLIED`，尚无成功 rollback。|
|`ROLLBACK_COMPLETED`|018E3 rollback `ROLLED_BACK`，且 018F lifecycle audit 为 `COMPLETE_WITH_ROLLBACK`。|
|`SAFETY_ANOMALY`|lifecycle audit 或任一已找到 artifact 违反安全边界。|
|`INCOMPLETE_DATA`|缺关键输入，无法可靠判断治理状态。|
|`ERROR`|summary 运行异常。|

## 4. action_level 含义

|Action Level|处理方式|
|---|---|
|`NONE`|无需动作，继续观察。|
|`WATCH`|继续累计 shadow / post-apply 观察证据。|
|`REVIEW_REQUIRED`|需要人工 review shadow evidence 或 proposal。|
|`APPROVAL_REQUIRED`|preflight 已准备好，apply 前仍需显式人工 approval。|
|`ROLLBACK_REVIEW_REQUIRED`|post-apply monitoring 出现 warning，需要人工判断是否 rollback。|
|`URGENT`|安全异常，先调查 artifact / 执行边界。|

## 5. Production vs Shadow Weights

Summary 会读取 production profile 的 `weights` / `base_weights` / `production_weights` /
`target_weights` 中第一个可用权重字段，并读取 current shadow weights 的 `weights`。
报告会显示权重和、key 集合和 `delta_from_production`。权重和不等于 1 或 key 不一致时只输出
warning，不静默别名映射。

## 6. Promotion Status

`promotion_status` 汇总 proposal、preflight、apply、rollback 和 lifecycle audit 的 status、
decision 与 executed 字段。它只解释已存在 artifact，不会补跑缺失阶段。

## 7. Pending Items

`pending_items` 用于快速识别下一步：

- `pending_preflight`: proposal 已提出但 preflight 缺失。
- `pending_apply`: preflight `PASS` 但 apply 缺失或未执行。
- `pending_rollback`: apply `APPLIED` 但 rollback 缺失或未执行。
- `pending_lifecycle_audit`: apply / rollback 比 latest lifecycle audit 更新，或 audit 缺失。

## 8. 处理 INCOMPLETE_DATA

先检查缺失的是 production profile、shadow weights，还是 latest governance artifacts。不要手工编辑
summary JSON 来清除状态；应补齐或重新生成缺失的原始 upstream artifact，然后再重新生成 summary。

## 9. 处理 SAFETY_ANOMALY

`SAFETY_ANOMALY` 是人工调查项。重点检查：

- lifecycle audit 是否已报告 `SAFETY_ANOMALY`；
- 任一 artifact 是否出现 `broker_execution=true`、`replay_execution=true` 或
  `trading_execution=true`；
- apply / rollback 的 executed 与 decision 是否矛盾；
- apply 是否缺 rollback snapshot；
- rollback 的 `post_rollback_validation.status` 是否不是 `PASS`；
- 是否有 artifact 声称 scheduler 触发了 apply 或 rollback。

## 10. 为什么不执行 apply/rollback

TRADING-019 是治理状态聚合层，不是执行器。Apply 和 rollback 只能由 018E2 / 018E3 在单独
approval、danger flag、hash gate 和 post validation 下显式运行。把 summary/dashboard 接到执行路径
会让 scheduler 或页面间接修改 production。

## 11. Dashboard 读取边界

Daily task dashboard 只能读取
`data/derived/weight_iterations/governance/parameter_governance_summary_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 不运行 TRADING-019 script，也不导入或触发 018B、018C、018C2、
018D、018E1、018E2、018E3、018F、scoring、broker、replay 或 trading execution path。

## 12. 与 TRADING-018F 的关系

018F 审计单次 promotion lifecycle 的 artifact chain、weight lifecycle 和安全边界。TRADING-019
读取 latest 018F 结果，并结合 production / shadow / review / proposal / preflight / apply /
rollback artifacts 推导当前总体 governance state 和人工行动需求。
