# TRADING-013A 本机验证：IBKR Paper Controlled Fill No-Fill

验证日期：2026-05-17

关联任务：`TRADING-013A`

## 结论

- test_status=LIMITED
- connection_status=CONNECTED
- paper-only=true
- diagnostic-only=true
- manual_cli_only=true
- production_effect=none
- production_surface_impact=none
- fill_model_validated=false

本次验证只固化 IBKR Paper controlled fill test 的本机 no-fill 样本。它不是实盘交易，
不接 live account，不接 production 自动交易，不影响 daily-run、replay、dashboard、
production conclusion、paper signal quality、shadow impact、参数晋级或交易建议。

本次不修改 `PaperBroker` fill model。由于 `fill_seen=false`，本阶段只能证明
submit/open/cancel 链路，不能证明 fill model；fill model remains unvalidated。

## 验证命令

```powershell
python scripts/run_ibkr_paper_controlled_fill_test.py `
  --date 2026-05-17 `
  --config config/ibkr_paper_controlled_fill.local.yaml `
  --symbol NVDA `
  --side BUY `
  --quantity 1 `
  --limit-price 224.50
```

`config/ibkr_paper_controlled_fill.local.yaml` 是本机 ignored config，不提交。

## 订单样本

|字段|记录|
|---|---|
|symbol|`NVDA`|
|asset_type|`stock`|
|side|`BUY`|
|order_type|`LIMIT`|
|time_in_force|`DAY`|
|quantity|`1`|
|limit_price|`224.50`|
|broker_order_id|`[REDACTED_BROKER_ORDER_ID]`|
|perm_id|`[REDACTED_PERM_ID]`|

## IBKR Paper 观察

- account_id_masked=`DUP***0000`
- broker_order_id_redacted=true
- perm_id_redacted=true
- execution_id_seen=false
- open_order_seen=true
- order status path：`PendingSubmit -> PreSubmitted -> PendingCancel -> Cancelled`
- fill_seen=false
- fill_quantity=0
- commission_report_seen=false
- cancel_requested=true
- final_order_status=Cancelled
- issue=fill_not_seen_timeout

Likely explanation：non-regular trading session / order remained `PreSubmitted`。即使
`limit_price` 高于当时显示 ask，非正常交易时段下 Paper order 仍可能不成交。

## 解释边界

本次样本可说明：

- 本机 local config 能连接 IBKR Paper。
- `BUY LIMIT DAY quantity=1` 可以提交并进入 open / `PreSubmitted` 状态。
- 超时 no-fill 后 cancel 链路可执行，最终状态为 `Cancelled`。

本次样本不能说明：

- `PaperBroker` fill model 已被验证。
- IBKR Paper fill 行为已覆盖美股正常交易时段。
- 一次 controlled fill 脚本成功运行可以支持生产交易、参数晋级或交易建议。

单个 no-fill 样本不得用于修改 `PaperBroker`。

## 安全核验

- committed review / fixture 不包含完整 `DUP` account id。
- committed review / fixture 不包含真实 broker order id、perm id 或 execution id。
- committed review / fixture 不包含 broker credential value。
- raw local output 只留在 ignored `outputs/reports/` 本机目录，不作为 committed
  artifact。
