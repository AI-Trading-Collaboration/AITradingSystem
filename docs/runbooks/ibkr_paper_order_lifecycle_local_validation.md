# IBKR Paper Order Lifecycle 本机验证 Runbook

最后更新：2026-05-17

适用范围：`TRADING-010` 本机 IBKR Paper 订单生命周期验证。该流程只用于
Paper 账号 submit / status / cancel / report 链路测试，不是真实交易，不是生产
自动交易，不改变 production 仓位建议、paper signal quality、shadow impact 或
dashboard。

## 前置条件

- 已完成 `TRADING-009` / `TRADING-009A` read-only snapshot 验证，并确认
  `snapshot_status=PASS`、`connection_status=CONNECTED`、
  `reconciliation_status=PASS`、account id masked、`production_effect=none`。
- 已安装项目 Python 3.11 `.venv` 和 `.[dev,data,dashboard,brokers]` 依赖。
- 登录 TWS Paper Trading 或 IB Gateway Paper Trading，不能使用 live session。
- 只使用 `DUP` 开头的 Paper account。
- 不把完整 account id、姓名、邮箱、现金细节、password、token、session cookie、
  API key 或其他敏感标识提交到 repo、issue、PR、日志摘录或测试 fixture。

## 开启 Paper API

TWS Paper 常用端口是 `7497`，IB Gateway Paper 常用端口是 `4002`。本项目默认
使用：

- `host: 127.0.0.1`
- `port: 7497`
- `client_id: 19010`
- `trading_mode: paper`
- `production_effect: none`

注意：本流程需要 submit/cancel Paper order，因此如果 TWS / Gateway 强制
`Read-Only API`，订单可能被 IBKR 拒绝。拒绝时保持报告为 `BLOCK` / `ERROR`，
不要绕过安全校验或切到 live account。

## 准备本地配置

复制默认配置：

```powershell
Copy-Item config\ibkr_paper_order.yaml config\ibkr_paper_order.local.yaml
```

编辑 `config/ibkr_paper_order.local.yaml`：

```yaml
paper_order_lifecycle_enabled: true
host: 127.0.0.1
port: 7497
client_id: 19010
account_id: "DUP_REPLACE_WITH_PAPER_ACCOUNT_ID"
trading_mode: paper
production_effect: none
allowed_symbols:
  - NVDA
  - AAPL
  - TSM
max_quantity: 1
far_from_market_pct: 0.5
```

`config/*.local.yaml` 已被 `.gitignore` 覆盖，不得提交。

## 推荐命令

优先使用极小数量和远离市场价的显式限价：

```powershell
.\.venv\Scripts\python.exe scripts\run_ibkr_paper_order_lifecycle.py --date 2026-05-17 --config config\ibkr_paper_order.local.yaml --symbol NVDA --side BUY --quantity 1 --limit-price 10
```

`--dry-limit-price` 与 `--limit-price` 等价，用于强调该价格只服务 lifecycle 测试。

默认输出：

- `outputs/reports/ibkr_paper_order_lifecycle_2026-05-17.json`
- `outputs/reports/ibkr_paper_order_lifecycle_2026-05-17.md`

这些本机 lifecycle 输出可能包含真实 Paper 订单状态，`.gitignore` 已覆盖
`outputs/reports/ibkr_paper_order_lifecycle_*.json` 和 `.md`。

## 如何确认订单已取消

检查 JSON / Markdown：

- `production_effect=none`
- `trading_mode=paper`
- `account_id_masked` 只显示 masked account id
- `open_order_seen=true` 或 `order_status_events` 中出现提交后状态
- `cancel_requested=true`
- `final_order_status` 为 `Cancelled`、`Inactive` 或等价 final 状态
- `cancelled_confirmed=true`
- `fills_seen=false`；如果为 `true`，说明远离市场价失败或市场/订单行为导致成交，
  该次 lifecycle 只能作为受限样本复核，不得解释为策略成交质量。

可选在 TWS / Gateway 的 Paper order 窗口手工确认订单状态已经取消，且没有残留
open order。

## 常见错误

|现象|常见原因|处理|
|---|---|---|
|`paper_order_lifecycle_enabled=false`|默认配置未显式启用|只在确认 Paper session 后修改 local YAML 为 `true`|
|`trading_mode must be paper`|配置不是 paper|停止运行，切回 TWS / Gateway Paper session|
|`account_id must start with DUP`|配置缺失或不是 Paper account|确认登录 Paper Trading，只填 `DUP` account|
|`production_effect must be none`|配置试图接入生产效果|恢复 `production_effect: none`；本流程永不进入 production|
|连接失败 / 端口错误|TWS / Gateway 未启动、API socket 未开启、TWS/Gateway 端口混用|TWS Paper 用 `7497`，Gateway Paper 用 `4002`，重启后重试|
|订单被拒绝|API read-only、权限不足、symbol 不可交易或订单字段不被 IBKR 接受|保留 `BLOCK` / `ERROR` 报告，不切 live，不放宽订单限制|
|订单立即成交|limit price 离市场价不够远或行情快速变动|记录 `fills_seen=true`，人工复核后用更远离市场价的 limit price 重跑|
|cancel timeout|IBKR 未及时推送最终状态或订单已进入不可取消状态|检查 TWS open orders；报告保持 `LIMITED` / `ERROR`，不要静默认为已取消|
