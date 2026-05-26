# Notification Delivery Failure Classification Runbook

## 1. 目的

TRADING-036 在 TRADING-035 `Notification Delivery Audit Summary` 之上新增只读失败分类和 retry readiness 报告。它把 `PASS`、`INCOMPLETE`、`MISMATCH`、`SAFETY_BLOCKED` 和未知状态转化为可人工处理的 category、manual review、safe_to_retry、retry blockers 和 recommended actions。

TRADING-036 不发送通知，不创建或修改 Gmail draft，不调用 webhook，不推送 mobile notification，不运行 TRADING-030/031/034/035，不自动 retry，不修改 delivery state，不修改 production 参数，不运行 market/backtest/scoring/data download/broker/replay/trading。

## 2. 如何运行

默认读取 latest TRADING-035 audit summary：

```bash
python scripts/run_notification_delivery_failure_classification.py
```

指定 source audit：

```bash
python scripts/run_notification_delivery_failure_classification.py \
  --audit-summary data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json
```

常用选项：

```bash
python scripts/run_notification_delivery_failure_classification.py \
  --audit-summary data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json \
  --output-dir outputs/notification_delivery_failure_classification \
  --as-of-date YYYY-MM-DD \
  --fail-on-critical
```

`--fail-on-critical` 只在 classification 结果为 `CRITICAL` 时返回非 0；默认不启用。

## 3. 输入

默认 source finder 支持：

- 当前项目 TRADING-035 路径：`data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json`
- 兼容路径：`outputs/notification_delivery_audit_summary/notification_delivery_audit_summary_YYYY-MM-DD.json`

如果指定 `--audit-summary`，只读取该文件。

## 4. 输出

- JSON：`outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`
- Markdown：`outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.md`
- Run log：`outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.log`

## 5. failure_categories

|Category|含义|Retry|
|---|---|---|
|`TRANSIENT_DELIVERY_FAILURE`|预留临时投递异常分类；当前 TRADING-036 不执行自动 retry。|仅未来任务可使用，当前不自动重试。|
|`CONFIGURATION_FAILURE`|notification target、recipient 或 channel config 类问题。|不可自动 retry，需人工复核。|
|`SAFETY_BLOCKED`|TRADING-035 已发现安全阻断。|不可 retry，人工复核前不得发送。|
|`CONTENT_MISMATCH`|Markdown/JSON/banner/dashboard/payload/target/hash/latest 等内容或链路不一致。|不可自动 retry，需解释 mismatch。|
|`MISSING_ARTIFACT`|TRADING-035 source 或其关键 artifact 缺失。|先补齐 source artifact，再重新运行 TRADING-035。|
|`UNKNOWN`|未知 audit status、malformed JSON 或暂未覆盖的状态。|不可自动 retry，需人工分类。|

## 6. 状态处理

|TRADING-035 audit_status|TRADING-036 处理|
|---|---|
|`PASS`|`overall_status=PASS`，`total_failures=0`，`safe_to_retry=false`，不需要 retry。|
|`INCOMPLETE`|默认 `MISSING_ARTIFACT`；如原因包含 target/recipient/channel config，则归类为 `CONFIGURATION_FAILURE`。|
|`MISMATCH`|归类为 `CONTENT_MISMATCH`，阻断后续通知链路。|
|`SAFETY_BLOCKED`|归类为 `SAFETY_BLOCKED`，`highest_severity=CRITICAL`。|
|unknown / malformed / missing source|归类为 `UNKNOWN` 或 `MISSING_ARTIFACT`，需要人工复核。|

## 7. Dashboard 行为

Daily task dashboard 只读取 latest TRADING-036 JSON，展示：

- `overall_status`
- `highest_severity`
- `total_failures`
- `requires_manual_review`
- `safe_to_retry`
- `blocks_notification_chain`
- source audit status
- `generated_at`

如果没有 TRADING-036 artifact，dashboard 显示 `No notification delivery failure classification report available.`，不得报错。Dashboard 不运行 classifier，不读取或修改外部通知状态，不发送 notification，不触发 retry。

## 8. 后续边界

TRADING-036 只是 read-only classify + report。未来 TRADING-037 retry candidate queue / manual approval gate 必须作为单独任务设计，且不得把 TRADING-036 的 `safe_to_retry` 字段解释为自动发送授权。
