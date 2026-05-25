# Notification Delivery Audit Summary Runbook

## 1. 目的

TRADING-035 用于把 TRADING-030 notification draft、TRADING-031 delivery preflight 和 TRADING-034 draft dispatch 串成统一只读审计摘要。它回答草稿是否生成、preflight 是否通过、draft dispatch 是否 ready、hash/latest 是否一致、是否存在 approval mismatch / safety blocked、是否发现外部发送副作用，以及当前 notification lifecycle 是否完整可追溯。

TRADING-035 不发送通知，不创建或修改 Gmail draft，不调用 webhook，不推送 mobile notification，不运行 operator brief、notification draft generator、delivery preflight、draft dispatch、market/backtest/scoring/data download/broker/replay/trading。

## 2. 如何运行

```bash
python scripts/run_notification_delivery_audit_summary.py --date 2026-05-24
```

常用选项：

```bash
python scripts/run_notification_delivery_audit_summary.py \
  --date 2026-05-24 \
  --data-root data \
  --fail-on-safety-anomaly
```

需要审计非默认路径时：

```bash
python scripts/run_notification_delivery_audit_summary.py \
  --date 2026-05-24 \
  --notification-draft-metadata-file data/derived/operator_briefs/notifications/operator_brief_notification_draft_2026-05-24.json \
  --delivery-preflight-file data/derived/operator_briefs/notifications/delivery_preflight/operator_brief_notification_delivery_preflight_2026-05-24.json \
  --dispatch-latest-file data/derived/operator_briefs/notifications/draft_dispatch/latest.json \
  --dispatch-file data/derived/operator_briefs/notifications/draft_dispatch/operator_brief_notification_draft_dispatch_2026-05-24.json
```

当前项目的 TRADING-034 默认目录是 `notifications/draft_dispatch/`。如果历史或外部实验使用 `notifications/dispatch/`，必须显式传入路径。

## 3. 输出

- JSON：`data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json`
- Markdown：`data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.md`
- Run log JSON：`data/derived/operator_briefs/notifications/delivery_audit/logs/notification_delivery_audit_summary_run_YYYY-MM-DD.json`
- Run log Markdown：`data/derived/operator_briefs/notifications/delivery_audit/logs/notification_delivery_audit_summary_run_YYYY-MM-DD.md`

## 4. audit_status

|状态|含义|处理方式|
|---|---|---|
|`PASS`|030/031/034 artifacts 存在，artifact chain 和 hash/latest 一致，未发现外部副作用。|可作为人工处理前追溯记录。|
|`PASS_WITH_WARNINGS`|主链路可审计，但有非安全警告，例如允许缺少 dispatch 或 lifecycle 未到 `DRAFT_READY`。|阅读 warnings，不自动发送。|
|`INCOMPLETE`|必需 030、031 或 034 artifact 缺失，且未允许缺少 dispatch。|补齐缺失 artifact 后重新审计。|
|`MISMATCH`|draft/preflight/dispatch/latest 的 path、sha256、date、status 或 draft hash 不一致。|停止使用该链路，先解释 mismatch。|
|`SAFETY_BLOCKED`|任一 artifact 出现 side-effect flag、execution flag、production_effect/read_only 异常或 safety blocked。|停止通知处理，人工调查。|
|`ERROR`|JSON 无法解析或 audit runtime error。|查看 run log 和 invalid artifact。|

## 5. notification_lifecycle_status

|状态|含义|
|---|---|
|`DRAFT_ONLY`|只确认 TRADING-030 draft metadata 存在。|
|`PREFLIGHT_READY`|TRADING-030 和 TRADING-031 已形成可人工 review 的 preflight 链路。|
|`DRAFT_READY`|TRADING-034 draft dispatch 为 `DRAFT_READY`，且审计链路一致。|
|`BLOCKED`|preflight 或 dispatch 处于 blocked / approval required / no-op 等非 ready 状态。|
|`APPROVAL_MISMATCH`|dispatch status 或 hash chain 显示 approval/hash mismatch。|
|`SAFETY_BLOCKED`|审计或输入 artifact 出现安全阻断。|
|`INCOMPLETE`|缺少 draft metadata 等关键输入。|
|`UNKNOWN`|输入状态无法可靠映射。|

## 6. Artifact Chain

审计重点：

- TRADING-031 是否引用正确的 TRADING-030 metadata path 和 sha256。
- TRADING-031 `notification_severity` 是否与 TRADING-030 一致。
- TRADING-034 若暴露 preflight/draft refs，是否指向同一 path 和 sha256。
- TRADING-034 `hashes.draft_hash` 是否与 dispatch canonical hash 一致。
- `draft_dispatch/latest.json` 是否指向同一个 dated dispatch artifact，且 date/status/hash 一致。

## 7. Draft Hash Mismatch

`draft_hash_match=false` 或 `dispatch_hash_stable=false` 表示 dispatch artifact 内容和其声明的 `hashes.draft_hash` 不一致，或 dispatch 引用了不同的 draft/preflight hash。处理方式：

1. 不使用该 dispatch artifact。
2. 对比 audit JSON 的 `artifact_chain.blocking_reasons`。
3. 检查 TRADING-034 dated artifact 和 `latest.json` 是否被人工改写。
4. 如需要，单独重新跑上游任务；不要在 TRADING-035 内绕过 hash 校验。

## 8. INCOMPLETE

`INCOMPLETE` 常见原因：

- 缺少 TRADING-030 draft metadata。
- 缺少 TRADING-031 delivery preflight。
- 缺少 TRADING-034 `latest.json` 或 dispatch artifact，且未使用 `--allow-missing-dispatch`。

处理方式是补齐缺失 artifact 后重新运行 TRADING-035。不要把 partial chain 当作可发送结论。

## 9. MISMATCH

`MISMATCH` 表示审计链路中出现 path/hash/status/date 不一致。处理方式：

1. 停止后续 send-related task。
2. 使用 audit JSON 的 `input_artifacts` 和 `artifact_chain.blocking_reasons` 定位差异。
3. 若 `latest.json` 指向错误 artifact，先恢复正确 latest 或重新生成 TRADING-034。
4. 若 draft/preflight hash 不一致，重新审查 TRADING-030 和 TRADING-031 的生成顺序。

## 10. SAFETY_BLOCKED

`SAFETY_BLOCKED` 表示至少一个安全边界被破坏，例如：

- `email_sent=true`
- `gmail_draft_created=true`
- `gmail_draft_modified=true`
- `webhook_called=true`
- `mobile_push_sent=true`
- `broker_execution=true`
- `replay_execution=true`
- `trading_execution=true`
- `pipelines_executed*`、`data_downloaded*`、`apply_executed*`、`rollback_executed*` 为 true

处理方式是立即停止通知处理，保留 artifacts，调查产生 true flag 的任务来源。不要手改 audit summary 来清除安全阻断。

## 11. 为什么不发送通知

TRADING-035 是审计层，只能证明已有 artifacts 是否一致、可追溯和无外部副作用。它不具备 recipient approval、Gmail draft creation approval、webhook secret handling、send authorization 或 broker/trading permission。

`safe_for_scheduler=true` 只说明 audit summary 可被定时生成；它不代表可以自动发送通知。

## 12. 进入 Gmail Draft / Webhook 前

未来真实 Gmail draft 或 webhook 相关任务必须单独拆分，并至少满足：

- 单独 approval artifact。
- 明确 recipient / channel / secret handling 策略。
- 明确 dry-run 和 preflight 仍不创建真实 Gmail draft。
- 真实创建 Gmail draft 或调用 webhook 必须有更严格人工确认。
- 继续保留 TRADING-035 audit summary 作为前置追溯记录。
