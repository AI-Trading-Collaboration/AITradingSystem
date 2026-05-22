# TRADING-018E2 Shadow Promotion Apply Runbook

TRADING-018E2 只适用于已经完成 018D proposal 和 018E1 PASS preflight 的
shadow-to-production weights promotion。它是人工显式命令，不是 scheduler 任务。

## 适用条件

只有同时满足以下条件才可运行 apply：

- 018D `shadow_promotion_proposal_YYYY-MM-DD.json` 已生成，且
  `proposal_decision=PROPOSE_FOR_MANUAL_REVIEW`、`promotion_proposed=true`。
- 018E1 `shadow_promotion_apply_preflight_YYYY-MM-DD.json` 已生成，且
  `preflight_decision=PASS`。
- 单独的 018E2 apply approval artifact 已创建。
- 当前 target production profile sha256 与 preflight / approval 记录一致。
- 操作者明确提供 `--i-understand-this-writes-production`。

## 确认 018D Proposal

```powershell
Get-Content data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json
```

重点检查：

- `task_id=TRADING-018D`
- `proposal_decision=PROPOSE_FOR_MANUAL_REVIEW`
- `promotion_proposed=true`
- `promotion_executed=false`
- `production_effect=none`

## 确认 018E1 Preflight

```powershell
Get-Content data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_2026-05-20.json
```

重点检查：

- `task_id=TRADING-018E1`
- `preflight_decision=PASS`
- `preflight_only=true`
- `apply_executed=false`
- `promotion_executed=false`
- `production_effect=none`
- `diff_preview.production_weights_after_preview`
- `diff_preview.target_profile_sha256_before`

## 创建 Apply Approval Artifact

推荐路径：

```text
data/manual_approvals/shadow_promotion_apply_approval_YYYY-MM-DD.json
```

必须使用 `approval_type=shadow_promotion_apply`，不能复用 018E1 preflight approval。
`safety_acknowledgement` 必须明确：

- `apply_authorized=true`
- `production_modification_authorized=true`
- `weights_only_update=true`
- `rollback_required=true`
- `manual_command_required=true`
- `scheduler_execution_forbidden=true`
- `broker_execution_forbidden=true`
- `replay_execution_forbidden=true`
- `trading_execution_forbidden=true`

## 计算 SHA256

PowerShell：

```powershell
Get-FileHash data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_2026-05-20.json -Algorithm SHA256
Get-FileHash data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_2026-05-20.json -Algorithm SHA256
Get-FileHash config/weights/weight_profile_current.yaml -Algorithm SHA256
```

跨平台 Python：

```bash
python -c "import hashlib, pathlib; p=pathlib.Path('config/weights/weight_profile_current.yaml'); print(hashlib.sha256(p.read_bytes()).hexdigest())"
```

把 preflight、proposal 和 target profile hash 写入 apply approval artifact。

## 运行 Explicit Apply

PowerShell：

```powershell
python scripts/run_shadow_promotion_apply.py `
  --date 2026-05-20 `
  --preflight-file data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_2026-05-20.json `
  --apply-approval-file data/manual_approvals/shadow_promotion_apply_approval_2026-05-20.json `
  --target-profile config/weights/weight_profile_current.yaml `
  --i-understand-this-writes-production
```

Linux/macOS：

```bash
python scripts/run_shadow_promotion_apply.py \
  --date 2026-05-20 \
  --preflight-file data/derived/weight_iterations/promotion/preflight/shadow_promotion_apply_preflight_2026-05-20.json \
  --apply-approval-file data/manual_approvals/shadow_promotion_apply_approval_2026-05-20.json \
  --target-profile config/weights/weight_profile_current.yaml \
  --i-understand-this-writes-production
```

`--i-understand-this-writes-production` 表示操作者明确知道该命令会修改 target
production profile 的 weights。缺少该 flag 时，result 必须为
`apply_decision=DANGER_FLAG_MISSING`，production profile 不得变化。

## 确认 Rollback Snapshot

成功 apply 后必须存在：

```text
data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.json
data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.sha256
```

`.sha256` 内容必须等于 apply 前 target profile sha256。018E2 只创建 snapshot，不执行 rollback。

## 确认只修改 Weights

检查 apply result：

```text
post_apply_validation.status=PASS
post_apply_validation.only_allowed_fields_changed=true
target_profile_validation.hash_changed_after_apply=true
```

也应人工 diff target profile，确认除 `weights` 或项目当前 profile 的权重字段外，`broker`、
`execution`、`replay`、`scheduler`、`risk_limits`、`api_keys`、`account`、`credentials`
等字段没有变化。

## 失败处理

`DANGER_FLAG_MISSING`：重新确认是否真的要写 production profile。只有人工确认后才添加
danger flag 重跑。

`APPROVAL_INVALID`：修正 apply approval artifact。常见原因是 approval type 错误、未
`approved=true`、preflight/proposal hash 不匹配、target path 不匹配或 safety
acknowledgement 不完整。

`PREFLIGHT_INVALID`：重新运行或复核 018E1 preflight。preflight 必须是 PASS 且保持
`production_effect=none`、`preflight_only=true`、`apply_executed=false`。

`TARGET_PROFILE_CHANGED`：禁止继续 apply。说明 target profile 在 preflight 后变化，
必须重新生成 018E1 preflight 和 apply approval。

`ROLLBACK_SNAPSHOT_FAILED`：不要写 production profile。先修复权限、路径或磁盘问题，再重跑。

`WRITE_FAILED`：检查 target profile 目录权限、文件锁和 atomic replace 支持。确认 profile
hash 后再决定是否重跑。

`POST_APPLY_VALIDATION_FAILED`：不要把结果视为可用 production promotion。检查 apply result
和 rollback snapshot；rollback command 将由 TRADING-018E3 单独实现。

## Dashboard 和 Scheduler 边界

Dashboard 只能读取 latest
`data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_YYYY-MM-DD.json`
并展示 apply result。Dashboard 禁止触发 018B/018C/018C2/018D/018E1/018E2、scoring、
broker、replay 或 trading execution。

Scheduler 禁止执行 apply。018E2 必须由人工显式命令触发，因为它会修改 production profile。

## 后续 Rollback

TRADING-018E3 才实现 explicit rollback command。018E3 需要单独 rollback approval、
单独 danger flag、原子恢复 target profile，并验证恢复后 sha256 等于 rollback snapshot。
