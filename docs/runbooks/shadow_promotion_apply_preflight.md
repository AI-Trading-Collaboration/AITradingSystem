# Shadow Promotion Apply Preflight Runbook

TRADING-018E1 只运行 approved apply preflight。它不执行 promotion、不修改 production profile、
不写 production weights，也不触发 broker、paper runner、replay runner 或 scoring pipeline。

## 1. 创建 approval artifact

默认路径：

```powershell
data/manual_approvals/shadow_promotion_approval_YYYY-MM-DD.json
```

最小结构：

```json
{
  "schema_version": "1.0",
  "approval_type": "shadow_promotion_apply_preflight",
  "approved": true,
  "approved_by": "manual_user",
  "approved_at": "2026-05-20T00:00:00Z",
  "proposal": {
    "proposal_date": "2026-05-20",
    "proposal_file": "data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json",
    "proposal_sha256": "<sha256 of proposal json>",
    "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
    "promotion_proposed": true
  },
  "target": {
    "target_profile_name": "production",
    "target_profile_path": "config/weights/weight_profile_current.yaml"
  },
  "approval_statement": "I manually reviewed the shadow promotion proposal and approve running apply preflight only. This approval does not authorize production modification.",
  "safety_acknowledgement": {
    "preflight_only": true,
    "apply_not_authorized": true,
    "production_modification_not_authorized": true
  }
}
```

该 approval 只授权 018E1 preflight，不授权 018E2 apply。

## 2. 计算 proposal sha256

PowerShell：

```powershell
Get-FileHash data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json -Algorithm SHA256
```

Python：

```powershell
python -c "import hashlib, pathlib; p=pathlib.Path('data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json'); print(hashlib.sha256(p.read_bytes()).hexdigest())"
```

把输出写入 approval artifact 的 `proposal.proposal_sha256`。

## 3. 运行 preflight

```powershell
python scripts/run_shadow_promotion_apply_preflight.py --date 2026-05-20 --approval-file data/manual_approvals/shadow_promotion_approval_2026-05-20.json
```

可选覆盖：

```powershell
python scripts/run_shadow_promotion_apply_preflight.py `
  --date 2026-05-20 `
  --data-root data `
  --proposal-file data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json `
  --production-profile config/weights/weight_profile_current.yaml `
  --target-profile-name production `
  --approval-file data/manual_approvals/shadow_promotion_approval_2026-05-20.json
```

输出：

- `data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_YYYY-MM-DD.json`
- `data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_YYYY-MM-DD.md`
- `data/derived/weight_iterations/promotion/preflight/logs/shadow_promotion_apply_preflight_run_YYYY-MM-DD.json`
- `data/derived/weight_iterations/promotion/preflight/logs/shadow_promotion_apply_preflight_run_YYYY-MM-DD.md`

## 4. 检查 diff preview

打开 preflight JSON 的 `diff_preview`：

- `production_weights_before` 是当前 production profile 中的只读 snapshot。
- `production_weights_after_preview` 来自 proposal 的 `proposed_production_weights`。
- `delta` 只是未来 apply 可能产生的字段差异。
- `changed_weight_keys` 列出非零 delta 的权重 key。

`diff_preview` 不是 production 写入记录。

## 5. 检查 rollback plan

打开 `rollback_plan`：

- `rollback_snapshot_would_be_created=true` 表示未来 apply 前应创建 snapshot。
- `rollback_snapshot_path_preview` 只是路径预览，018E1 不创建 rollback snapshot。
- `target_profile_sha256_before` 用于记录当前 production profile 的预检查 hash。

未来 018E2 如果实现 apply，必须在写入前真实创建 rollback snapshot。

## 6. 确认 production 未被修改

运行前后检查：

```powershell
git diff -- config/weights/weight_profile_current.yaml
git status --short config/weights
```

预期：018E1 不产生 production profile diff。

也应检查 preflight JSON：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "promotion_executed": false,
  "apply_executed": false,
  "preflight_only": true,
  "safe_for_production": false
}
```

## 7. 处理 APPROVAL_INVALID

常见原因：

- `approved` 不是 `true`
- `approval_type` 不是 `shadow_promotion_apply_preflight`
- `proposal_sha256` 与实际 proposal 文件 hash 不一致
- `proposal_date` 与本次 `--date` 不一致
- safety acknowledgement 未确认 `preflight_only` 或 `apply_not_authorized`

处理方式：

1. 重新人工复核 proposal 和 target profile。
2. 重新计算 proposal sha256。
3. 更新 approval artifact。
4. 重新运行 preflight。

不要通过修改 proposal 或 production profile 来适配错误 approval。

## 8. 处理 PROPOSAL_INVALID

常见原因：

- proposal 不是 `task_id=TRADING-018D`
- `proposal_decision` 不是 `PROPOSE_FOR_MANUAL_REVIEW`
- `promotion_proposed` 不是 `true`
- `promotion_executed` 不是 `false`
- `production_effect` 不是 `none`
- `manual_review_only` 不是 `true`
- 缺少 `proposed_production_weights`

处理方式：

1. 回到 TRADING-018D proposal gate 修复上游证据。
2. 重新生成 proposal。
3. 重新创建匹配该 proposal hash 的 018E1 approval artifact。

## 9. 处理 WEIGHT_MISMATCH

常见原因：

- production / shadow / proposed weight keys 不完全一致
- proposed weights sum 不等于 1.0
- proposed weights 不在 `[0, 1]`
- current shadow weights 与 proposal `proposed_production_weights` 不一致

处理方式：

1. 不做 alias mapping。
2. 先修复上游 shadow weights 或 proposal。
3. 重新生成 proposal 和 approval。

## 10. 为什么本任务不执行 apply

018E1 的目标是把“proposal 已经人工同意进入 apply 前检查”和“真正写 production”分开。

因此即使 approval artifact 存在且 preflight `PASS`，本任务仍然必须保持：

- `apply_executed=false`
- `promotion_executed=false`
- `production_effect=none`
- `safe_for_production=false`

这能避免 scheduler、dashboard 或误操作把 preflight 解读成 production 修改授权。

## 11. 未来 018E2 需要的额外保护

未来 explicit apply command 至少需要：

- 单独的 apply approval artifact，不能复用 018E1 approval。
- 明确 target production profile。
- 强制 CLI 参数，例如 `--i-understand-this-writes-production`。
- apply 前 diff 二次确认。
- apply 前 production snapshot。
- apply 后 validation。
- rollback snapshot。
- 禁止 scheduler 自动运行。
- 禁止无 approval apply。

018E1 不实现任何上述写入逻辑。
