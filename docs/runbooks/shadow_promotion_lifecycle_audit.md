# Shadow Promotion Lifecycle Audit Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-018F 用于把一次 shadow promotion 从 018D proposal、018E1 apply
preflight、018E2 apply result 到可选 018E3 rollback result 的 artifact chain 串成一份只读
审计报告。它回答 promotion 为什么被提出、preflight 是否通过、apply 是否执行、weights
如何变化、rollback snapshot 是否创建、是否发生 rollback、rollback 是否成功、是否存在安全边界异常，
以及所有 artifact 是否可追溯。

018F 只读读取已有 artifacts，不执行 promotion、apply、rollback、broker、replay 或 trading
execution。

## 2. 手动运行

```bash
python scripts/run_shadow_promotion_lifecycle_audit.py \
  --date 2026-05-23 \
  --promotion-date 2026-05-23
```

默认输出：

```text
data/derived/weight_iterations/promotion/audit/
  shadow_promotion_lifecycle_audit_YYYY-MM-DD.json
  shadow_promotion_lifecycle_audit_YYYY-MM-DD.md

data/derived/weight_iterations/promotion/audit/logs/
  shadow_promotion_lifecycle_audit_run_YYYY-MM-DD.json
  shadow_promotion_lifecycle_audit_run_YYYY-MM-DD.md
```

## 3. 指定 promotion_date

`--date` 是 audit run date，决定 audit artifact 文件名。`--promotion-date` 是要审计的
promotion event date，默认等于 `--date`。当隔日补审某次 promotion 时，应显式指定
`--promotion-date`。

## 4. 指定输入 artifact

```bash
python scripts/run_shadow_promotion_lifecycle_audit.py \
  --date 2026-05-23 \
  --promotion-date 2026-05-23 \
  --proposal-file data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-23.json \
  --preflight-file data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_2026-05-23.json \
  --apply-result-file data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_2026-05-23.json \
  --rollback-result-file data/derived/weight_iterations/promotion/rollback_results/shadow_promotion_rollback_result_2026-05-23.json
```

`--rollback-result-file` 可省略；缺 rollback result 不视为错误。可加
`--include-approval-artifacts` 把同日 approval artifact 的存在性和 hash 纳入
`input_artifacts.approval_artifacts`，但 audit 仍不修改 approval。

## 5. lifecycle_decision

|Decision|含义|
|---|---|
|`COMPLETE_WITH_ROLLBACK`|proposal、preflight、APPLIED apply result 和 ROLLED_BACK rollback result 均存在，chain 和安全边界一致。|
|`COMPLETE_APPLIED_NO_ROLLBACK`|proposal、preflight 和 APPLIED apply result 存在，rollback result 不存在，且 rollback snapshot 存在。|
|`PROPOSAL_ONLY`|只有 proposal artifact 存在，proposal 本身未触发安全异常。|
|`PREFLIGHT_ONLY`|proposal 和 preflight 存在，apply/rollback 不存在。|
|`APPLY_FAILED_OR_BLOCKED`|apply result 存在，但 `apply_decision != APPLIED` 或 `apply_executed=false`。|
|`ROLLBACK_FAILED_OR_BLOCKED`|rollback result 存在，但 `rollback_decision != ROLLED_BACK` 或 `rollback_executed=false`。|
|`INCOMPLETE_ARTIFACTS`|必要 artifact 缺失或引用字段缺失，尚无确认的安全违规。|
|`SAFETY_ANOMALY`|hash/path/date/target/snapshot 矛盾，或任一 artifact 出现 broker/replay/trading、只读边界破坏、执行状态矛盾。|
|`ERROR`|audit 运行异常。|

## 6. Artifact Chain

重点检查：

- preflight 是否引用同一个 018D proposal path/sha256/date/decision。
- apply result 是否引用同一个 018E1 preflight path/sha256，且 preflight 决策为 `PASS`。
- apply result 记录的 target profile preflight hash 是否与 preflight diff preview 一致。
- rollback result 是否引用同一个 apply result path/sha256。
- rollback snapshot sha 是否与 apply result 记录一致。
- rollback 当前 target profile expected hash 是否与 apply after hash 一致。

缺引用字段通常是 `INCOMPLETE_ARTIFACTS`；已存在但 hash/path/target/snapshot 不匹配是
`SAFETY_ANOMALY`。

## 7. Weight Lifecycle

报告会尽量提取：

- apply 前 production weights；
- apply 后 production weights；
- rollback 后 production weights；
- apply delta；
- rollback delta；
- lifecycle net delta。

缺 rollback result 时，`production_weights_after_rollback` 为 `null`，并输出 warning；这不是
safety anomaly。

## 8. Safety Boundary Audit

018F 扫描已找到 artifacts 的安全字段。以下情况必须视为 `SAFETY_ANOMALY`：

- 任一 artifact `broker_execution=true`、`replay_execution=true` 或 `trading_execution=true`。
- 018D proposal `promotion_executed=true`。
- 018E1 preflight `apply_executed=true` 或 `production_effect != none`。
- 018E2 `apply_executed=true` 但 `apply_decision != APPLIED`。
- 018E2 `apply_decision=APPLIED` 但 rollback snapshot 缺失。
- 018E3 `rollback_executed=true` 但 `rollback_decision != ROLLED_BACK`。
- 018E3 `rollback_executed=true` 但 `post_rollback_validation.status != PASS`。
- artifact safety contract 显示会触发 scoring、broker、replay、trading 或上游 promotion pipeline。

## 9. 处理 INCOMPLETE_ARTIFACTS

先检查缺失的是 artifact 文件还是 artifact 内部引用字段。不要补造上游结果。正确处理方式是：

1. 找到应该由 018D/018E1/018E2/018E3 生成的原始 artifact。
2. 确认 promotion_date 是否正确。
3. 若 artifact 确实未生成，保留 `INCOMPLETE_ARTIFACTS` 作为生命周期记录。
4. 若 artifact 存在但引用字段缺失，登记上游 artifact schema follow-up。

## 10. 处理 SAFETY_ANOMALY

`SAFETY_ANOMALY` 是人工调查项。不要用手工编辑 audit JSON 的方式清除异常。应检查：

- artifact 是否被替换或来自不同日期；
- 是否出现未授权 apply/rollback；
- snapshot 或 target profile hash 是否不一致；
- 是否有 dashboard/scheduler/import path 会触发 pipeline；
- 是否需要新增任务登记修复上游 artifact schema 或安全字段。

可用 `--fail-on-safety-anomaly` 让命令在写出 audit artifact 后以非零状态退出，便于只读监控。

## 11. 为什么 audit 不执行 apply/rollback

018F 的职责是审计已发生或已提出的 lifecycle，不是 lifecycle 的执行器。Apply 和 rollback
分别由 018E2/018E3 在人工 approval、danger flag、hash gate 和 post validation 下显式执行。
把 audit 与执行合并会破坏职责分离，并让 scheduler/dashboard 有机会间接修改 production。

## 12. Dashboard 边界

Dashboard 只能读取
`data/derived/weight_iterations/promotion/audit/shadow_promotion_lifecycle_audit_YYYY-MM-DD.json`
并展示轻量卡片。Dashboard 禁止执行或导入 018B、018C、018C2、018D、018E1、018E2、018E3、
018F、scoring、broker、replay 或 trading execution path。
