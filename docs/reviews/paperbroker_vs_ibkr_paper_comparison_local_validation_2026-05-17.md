# TRADING-011A 本机验证：PaperBroker vs IBKR Paper Comparison

验证日期：2026-05-17

关联任务：`TRADING-011A`

## 结论

- validation_status=PASS
- paper-only=true
- comparison_mode=diagnostic_only
- production_effect=none
- account_id_masked=`DUP***0000`
- broker_order_id_redacted=true
- full_account_id_written=false
- secret_written=false
- production_surface_impact=none

本次验证只比较本地 `PaperBroker` 与 IBKR Paper 的 open / orderStatus /
cancel / fill 观察结果，不是实盘交易，不接 live account，不接 production 自动交易，
不影响 daily-run、dashboard production conclusion、paper signal quality、shadow
impact、参数晋级或交易建议。

## 验证命令

```powershell
python scripts/run_paperbroker_vs_ibkr_paper_comparison.py `
  --date 2026-05-17 `
  --config config/ibkr_paper_order.local.yaml `
  --symbol NVDA `
  --side BUY `
  --quantity 1 `
  --limit-price 10
```

## 前置检查

- TWS Paper / IB Gateway Paper 已启动。
- API socket 已开启。
- 本地 ignored config：`config/ibkr_paper_order.local.yaml`。
- `paper_order_lifecycle_enabled=true`。
- `ibkr_paper_comparison_enabled=true`。
- `trading_mode=paper`。
- `production_effect=none`。
- account id 为 `DUP` Paper account，committed artifact 中只保留 masked placeholder。

第一轮运行在 `ibkr_paper_comparison_enabled=false` 时按预期 `BLOCK`，未连接 IBKR。
显式启用本地 comparison 开关后，第二轮真实运行通过。

## 订单样本

```json
{
  "symbol": "NVDA",
  "asset_type": "stock",
  "side": "BUY",
  "order_type": "LIMIT",
  "time_in_force": "DAY",
  "quantity": 1,
  "limit_price": 10.0
}
```

## Local PaperBroker 观察

- local_order_status=SUBMITTED
- local_open_order_seen=true
- local_fill_seen=false
- local_avg_fill_price=null
- local_cancel_result=CANCELLED
- local_final_status=CANCELLED
- local_reconciliation_status=PASS
- local_price_source=synthetic_far_from_market_snapshot

## IBKR Paper 观察

- broker_order_id=`[REDACTED_BROKER_ORDER_ID]`
- open_order_seen=true
- orderStatus events observed：`PendingSubmit`、`PreSubmitted`、`PendingCancel`、`Cancelled`
- cancel_requested=true
- final_status=Cancelled
- cancelled_confirmed=true
- fills_seen=false
- ibkr_reconciliation_status=PASS

## Diff

- status_match=true
- fill_match=true
- cancel_match=true
- local_filled_but_ibkr_not_filled=false
- ibkr_rejected_but_local_accepted=false
- lifecycle_event_gap=[]
- difference_labels=[]

本次样本没有发现 lifecycle 差异，因此不能据此修改 `PaperBroker` fill model。
TRADING-012 应等待出现 `LOCAL_SIM_TOO_OPTIMISTIC`、`BROKER_REJECTED`、
`INSUFFICIENT_MARKET_DATA` 或 `CANCEL_TIMING_DIFFERENCE` 的样本后再校准。

## 安全核验

- committed fixture 不包含完整 `DUP` account id。
- committed fixture 不包含真实 broker order id。
- review 和 fixture 不包含 password、token、API key、session cookie 或
  Authorization header。
- raw local output 只留在 ignored `outputs/reports/` 本机目录，不作为 committed
  artifact。
