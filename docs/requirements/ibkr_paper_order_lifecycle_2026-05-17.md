# TRADING-010：IBKR Paper Order Lifecycle Test

最后更新：2026-05-17

关联任务：`TRADING-010`

## 背景

`TRADING-009` / `TRADING-009A` 已完成本机 IBKR Paper read-only snapshot
验证，结果为 `snapshot_status=PASS`、`connection_status=CONNECTED`、
`reconciliation_status=PASS`、account id masked、`readonly=true`、
`production_effect=none`。

本任务在此基础上新增一个 Paper 专用订单生命周期测试能力，用于人工显式验证
IB Gateway / TWS Paper 的 submit / status / cancel / report 链路。该能力不是
真实交易，不是 production 自动交易，也不影响 production 仓位建议、paper signal
quality、shadow impact 或 dashboard。

## 范围

1. 新增 `src/ai_trading_system/trading_engine/brokers/ibkr_paper_order.py`：
   - 只允许 Paper / `DUP` account。
   - 只允许显式配置 `trading_mode=paper`、
     `paper_order_lifecycle_enabled=true`、`production_effect=none`。
   - 非 `DUP` account、非 paper mode 或非 `production_effect=none` 必须在连接前
     fail closed。
2. 新增 `config/ibkr_paper_order.yaml`：
   - 默认 `paper_order_lifecycle_enabled=false`。
   - 不保存 password、token、API key、session cookie 或其他凭证。
   - 记录第一版安全白名单和极小数量上限。
3. 新增 `scripts/run_ibkr_paper_order_lifecycle.py`：
   - 人工显式运行，不由 daily-run、candidate runner、dashboard 或 production
     流程触发。
   - 输出 `outputs/reports/ibkr_paper_order_lifecycle_YYYY-MM-DD.json`
     和 `.md`。
   - 即使配置阻断、连接失败、submit 后异常或 cancel timeout，也输出
     `BLOCK` / `ERROR` / `LIMITED` 报告，不静默退出。

## 订单限制

第一版只允许：

- `asset_type=stock`
- `order_type=LIMIT`
- `time_in_force=DAY`
- `side=BUY` 或 `SELL`
- `symbol` 在配置白名单内，默认 `NVDA`、`AAPL`、`TSM`
- `quantity` 极小，默认最大 `1`
- 明确设置 limit order，不支持 market order

禁止：

- option、ETF、future、crypto 或其他非 stock 资产
- short sell；`SELL` 必须能确认 Paper 账号已有足够 long quantity，否则 fail closed
- margin order 或任何显式 margin 语义
- `GTC` 或非 `DAY` time in force
- bracket、stop、trailing、algo、OCA、多腿或 parent/child order
- 从 daily candidates、paper runner、score-daily、dashboard 自动触发 IBKR order

## 限价策略

目标是测试 lifecycle，不是成交。

- 如果用户显式传入 `--limit-price` 或 `--dry-limit-price`，脚本使用该价格。
- 如果未传入，adapter 会尝试读取参考价：
  - `BUY` 使用低于参考价较远的 limit price；
  - `SELL` 使用高于参考价较远的 limit price。
- 如果无法取得有效参考价，不自动提交订单，输出 `lifecycle_status=LIMITED`，
  并在 `issues` 中说明 `reference_price_unavailable`。

## 生命周期流程

1. 读取本地配置。
2. 在连接前确认 Paper / `DUP` account、`trading_mode=paper`、
   `paper_order_lifecycle_enabled=true` 和 `production_effect=none`。
3. 连接 IBKR Paper。
4. 构造极小 `LIMIT DAY` stock order。
5. submit order。
6. 等待并记录 `openOrder` / `orderStatus` 或等价状态事件。
7. 记录 broker order id。
8. 立即 cancel order。
9. 等待 cancelled / inactive / final status。
10. 输出 lifecycle JSON 和 Markdown report。
11. disconnect。

## 报告字段

JSON 至少包含：

- `lifecycle_status`
- `connection_status`
- `account_id_masked`
- `production_effect`
- `trading_mode`
- `submitted_order`
- `broker_order_id`
- `order_status_events`
- `open_order_seen`
- `cancel_requested`
- `final_order_status`
- `cancelled_confirmed`
- `fills_seen`
- `safety_checks`
- `issues`

Markdown 使用中文解释，但保留字段名、ticker、status code 和配置键的英文原文。

## 安全边界

- 不允许 live account。
- 不允许真实交易模式。
- 不允许输出完整 account id。
- 不允许读取 broker API key、password、token 或 secret。
- 不允许把 lifecycle 结果写入 production dashboard。
- 不允许影响 paper signal quality / shadow impact / production conclusion。
- 不允许从 daily candidates 自动触发 IBKR order。
- 必须人工显式运行脚本。
- 所有输出必须包含 `production_effect=none`。

## 验收标准

- `python -m pytest tests/trading_engine/test_ibkr_paper_order_lifecycle.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 提交并 push 后确认 GitHub Actions 本次 commit 对应 CI run 通过。

## 实施步骤

1. 更新任务登记、需求文档、runbook、默认配置和系统流图。
2. 实现 Paper-only order lifecycle adapter、配置 loader、安全校验和 account id
   masking。
3. 实现 lifecycle 脚本、JSON/Markdown report writer 和异常报告路径。
4. 使用 mock IBKR client 覆盖 fail-closed、订单限制、submit/cancel 正常路径和
   submit 后异常报告。
5. 跑本地验收，提交、push 并检查 CI。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 明确 TRADING-009 / 009A 已完成本机
  Paper read-only snapshot 验证，要求推进 IBKR Paper submit / status / cancel /
  report lifecycle 测试能力；本阶段保持 paper-only、人工显式运行、
  `production_effect=none`，不进入 production 自动交易或任何生产结论链路。
- 2026-05-17：实现完成并进入验证。已新增 Paper-only lifecycle adapter、默认
  disabled 配置、人工显式脚本、JSON/Markdown 报告、限价和订单类型安全检查、
  account id masking、runbook、系统流图和 mock client 测试。本地验证通过
  `python -m pytest tests/trading_engine/test_ibkr_paper_order_lifecycle.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
