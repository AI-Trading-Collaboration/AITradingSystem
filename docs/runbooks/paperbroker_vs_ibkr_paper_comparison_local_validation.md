# PaperBroker vs IBKR Paper Comparison 本机验证 Runbook

最后更新：2026-05-17

适用任务：`TRADING-011` / `TRADING-011A`

## 目的

本 runbook 用于人工显式验证 Local `PaperBroker` 与 IBKR Paper open / cancel /
fill lifecycle 的诊断比较。该流程只输出 `diagnostic_only` 报告，不是实盘交易，
不接 live account，不进入 production 自动交易。

## 前置条件

1. 启动 TWS Paper Trading 或 IB Gateway Paper Trading。
2. 开启 API socket。
3. 准备本地 ignored config：`config/ibkr_paper_order.local.yaml`。
4. 确认 local config 中：
   - `paper_order_lifecycle_enabled=true`
   - `ibkr_paper_comparison_enabled=true`
   - `trading_mode=paper`
   - `production_effect=none`
   - `account_id` 是 `DUP` Paper account
5. 不在 local config 中保存 password、token、API key、session cookie 或其他凭证。

## 推荐命令

```powershell
cd D:\Work\AITradingSystem
.\.venv\Scripts\Activate.ps1

python scripts/run_paperbroker_vs_ibkr_paper_comparison.py `
  --date 2026-05-17 `
  --config config/ibkr_paper_order.local.yaml `
  --symbol NVDA `
  --side BUY `
  --quantity 1 `
  --limit-price 10
```

如果 limit price 过于接近市场价，改用更远离市场价但合法的 `LIMIT` 价格。目标是
观察 open/cancel lifecycle，不追求成交。

## 预期输出

```text
outputs/reports/paperbroker_vs_ibkr_paper_comparison_YYYY-MM-DD.json
outputs/reports/paperbroker_vs_ibkr_paper_comparison_YYYY-MM-DD.md
```

报告应确认：

- `production_effect=none`
- `comparison_mode=diagnostic_only`
- account id 已 masked
- broker order id 已 redacted
- local `PaperBroker` status/open/fill/cancel/final/reconciliation
- IBKR Paper openOrder / orderStatus / cancel / final / fills / reconciliation
- `fills_seen=false` 是 open/cancel 样本的期望结果
- recommendations 只作为后续 quality feedback，不直接修改 `PaperBroker`

## Fail Closed 检查

以下情况必须在连接 IBKR 前停止：

- `trading_mode != paper`
- 非 `DUP` account
- `paper_order_lifecycle_enabled=false`
- `ibkr_paper_comparison_enabled=false`
- `production_effect != none`
- market / option / short / margin / GTC / bracket / stop / trailing / algo / linked order
- `quantity != 1`
- 非白名单 symbol

## 验证后处理

1. 检查 raw local JSON/Markdown 是否没有完整 account id、真实 broker order id、
   password、token、API key 或 session cookie。
2. 只提交 sanitized review / fixture，不提交 raw local output。
3. 若出现差异，只记录 `EXPECTED_DIFFERENCE`、`LOCAL_SIM_TOO_OPTIMISTIC`、
   `BROKER_REJECTED`、`INSUFFICIENT_MARKET_DATA` 或 `CANCEL_TIMING_DIFFERENCE`。
4. 不在本任务中修改 `PaperBroker` 行为；fill model 校准另走 TRADING-012。
