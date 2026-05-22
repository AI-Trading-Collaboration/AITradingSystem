# TRADING-018E3 Shadow Promotion Rollback Runbook

TRADING-018E3 只适用于 TRADING-018E2 已经成功 `APPLIED` 后，需要人工显式把
target production profile weights 恢复到 apply 前 rollback snapshot 的场景。

它不是 scheduler 任务，也不是 dashboard 操作。Apply result 不是 rollback approval。

## 适用条件

只有同时满足以下条件才可运行 rollback：

- 018E2 `shadow_promotion_apply_result_YYYY-MM-DD.json` 存在。
- `apply_decision=APPLIED`、`apply_executed=true`、`promotion_executed=true`。
- 018E2 记录的 rollback snapshot 存在，sha256 与 apply result 记录一致。
- 单独的 018E3 rollback approval artifact 已创建。
- 当前 target production profile sha256 仍等于 apply 后 hash。
- 操作者明确提供 `--i-understand-this-rolls-back-production`。

## 确认 018E2 Apply Result

```powershell
Get-Content data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_2026-05-23.json
```

重点检查：

- `task_id=TRADING-018E2`
- `apply_decision=APPLIED`
- `apply_executed=true`
- `promotion_executed=true`
- `rollback.snapshot_created=true`
- `rollback.snapshot_path`
- `rollback.snapshot_file_sha256` 或 `rollback.snapshot_sha256`
- `post_apply_validation.status=PASS`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

## 确认 Rollback Snapshot

成功 apply 后必须存在：

```text
data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.json
data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_YYYY-MM-DD.sha256
```

`json` 文件是 apply 前 profile snapshot；`.sha256` 记录 apply 前 target profile hash。

## 创建 Rollback Approval Artifact

推荐路径：

```text
data/manual_approvals/shadow_promotion_rollback_approval_YYYY-MM-DD.json
```

必须使用 `approval_type=shadow_promotion_rollback`，不能复用 018E1 preflight approval 或
018E2 apply approval。`safety_acknowledgement` 必须明确：

- `rollback_authorized=true`
- `production_modification_authorized=true`
- `weights_only_restore=true`
- `current_snapshot_required=true`
- `manual_command_required=true`
- `scheduler_execution_forbidden=true`
- `broker_execution_forbidden=true`
- `replay_execution_forbidden=true`
- `trading_execution_forbidden=true`

## 计算 SHA256

PowerShell：

```powershell
Get-FileHash data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_2026-05-23.json -Algorithm SHA256
Get-FileHash data/derived/weight_iterations/promotion/rollback/production_profile_before_shadow_promotion_2026-05-23.json -Algorithm SHA256
Get-FileHash config/weights/weight_profile_current.yaml -Algorithm SHA256
```

跨平台 Python：

```bash
python -c "import hashlib, pathlib; p=pathlib.Path('config/weights/weight_profile_current.yaml'); print(hashlib.sha256(p.read_bytes()).hexdigest())"
```

把 apply result hash、rollback snapshot hash、当前 target profile hash 和 expected
rollback profile hash 写入 rollback approval artifact。

## 运行 Explicit Rollback

PowerShell：

```powershell
python scripts/run_shadow_promotion_rollback.py `
  --date 2026-05-23 `
  --apply-result-file data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_2026-05-23.json `
  --rollback-approval-file data/manual_approvals/shadow_promotion_rollback_approval_2026-05-23.json `
  --target-profile config/weights/weight_profile_current.yaml `
  --i-understand-this-rolls-back-production
```

Linux/macOS：

```bash
python scripts/run_shadow_promotion_rollback.py \
  --date 2026-05-23 \
  --apply-result-file data/derived/weight_iterations/promotion/apply/shadow_promotion_apply_result_2026-05-23.json \
  --rollback-approval-file data/manual_approvals/shadow_promotion_rollback_approval_2026-05-23.json \
  --target-profile config/weights/weight_profile_current.yaml \
  --i-understand-this-rolls-back-production
```

`--i-understand-this-rolls-back-production` 表示操作者明确知道该命令会修改 target
production profile weights。缺少该 flag 时，result 必须为
`rollback_decision=DANGER_FLAG_MISSING`，production profile 不得变化。

## 确认 Current Snapshot

rollback 写入前必须创建：

```text
data/derived/weight_iterations/promotion/rollback_current_snapshots/production_profile_before_rollback_YYYY-MM-DD.json
data/derived/weight_iterations/promotion/rollback_current_snapshots/production_profile_before_rollback_YYYY-MM-DD.sha256
```

该 snapshot 记录 rollback 前的当前 production profile，也就是 apply 后状态。

## 确认 Production Profile 已恢复

检查 rollback result：

```text
rollback_decision=ROLLED_BACK
rollback_executed=true
production_effect=profile_rolled_back_only_if_rollback_executed
post_rollback_validation.status=PASS
post_rollback_validation.weights_match_rollback_snapshot=true
post_rollback_validation.only_allowed_fields_changed=true
broker_execution=false
replay_execution=false
trading_execution=false
```

本阶段采用 weights-only restore；如果非 weights 字段在 apply 后由其他人工流程更新，rollback
不会恢复旧的非 weights 配置。

## 失败处理

`DANGER_FLAG_MISSING`：重新确认是否真的要 rollback production weights。只有人工确认后才添加
danger flag 重跑。

`APPROVAL_INVALID`：修正 rollback approval artifact。常见原因是 approval type 错误、未
`approved=true`、apply result hash 不匹配、rollback snapshot hash 不匹配、target path
不匹配或 safety acknowledgement 不完整。

`APPLY_RESULT_INVALID`：确认 apply result 是 018E2 成功 `APPLIED` 结果，且 rollback
snapshot 已创建、post apply validation 为 PASS。

`ROLLBACK_SNAPSHOT_INVALID`：确认 snapshot 文件未被改写、sha256 与 apply result 一致、
weights keys 与当前 profile 一致、weights 总和约等于 1.0 且每个 weight 在 `[0, 1]`。

`TARGET_PROFILE_CHANGED`：禁止继续 rollback。说明 target profile 在 apply 后又被其他流程修改，
必须先人工判断是否仍应 rollback，并重新生成 approval。

`CURRENT_SNAPSHOT_FAILED`：不要写 production profile。先修复权限、路径或磁盘问题，再重跑。

`WRITE_FAILED`：检查 target profile 目录权限、文件锁和 atomic replace 支持。确认 profile
hash 后再决定是否重跑。

`POST_ROLLBACK_VALIDATION_FAILED`：不要把结果视为已成功恢复。检查 rollback result、target
profile、current snapshot 和 rollback snapshot；必要时先人工保存现场，再决定下一步修复。

## Dashboard 和 Scheduler 边界

Dashboard 只能读取 latest
`data/derived/weight_iterations/promotion/rollback_results/shadow_promotion_rollback_result_YYYY-MM-DD.json`
并展示 rollback result。Dashboard 禁止触发 018B/018C/018C2/018D/018E1/018E2/018E3、
scoring、broker、replay 或 trading execution。

Scheduler 禁止执行 rollback。018E3 必须由人工显式命令触发，因为它会修改 production
profile weights，并且需要人工确认 apply 后问题确实需要恢复。
