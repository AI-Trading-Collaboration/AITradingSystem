# IBKR Paper Controlled Fill 本机验证 Runbook

最后更新：2026-05-17

适用范围：`TRADING-013` IBKR Paper controlled small fill test。该流程只用于在
IBKR Paper account 中人工采集极小数量的 Paper fill 样本，并与本地 `PaperBroker`
做诊断对比。它不是实盘交易，不是 production 自动交易，不改变 production 仓位建议，
不影响 daily-run、replay、dashboard、paper signal quality 或 shadow impact。

## 前置条件

- 已完成 `TRADING-012`，当前 `calibration_status=LIFECYCLE_ALIGNED_FILL_UNTESTED`。
- 已安装项目 Python 3.11 `.venv` 和 `.[dev,data,dashboard,brokers]` 依赖。
- 登录 TWS Paper Trading 或 IB Gateway Paper Trading，不能使用 live session。
- 只使用 `DUP` 开头的 Paper account。
- 下单前人工确认 `limit_price`。不要让脚本自动推断成交价。
- `quantity` 必须极小；第一版固定为 `1`。
- 不把完整 account id、姓名、邮箱、现金细节、password、token、session cookie、
  API key 或其他敏感标识提交到 repo、issue、PR、日志摘录或测试 fixture。

## 准备本地配置

复制默认配置：

```powershell
Copy-Item config\ibkr_paper_controlled_fill.yaml config\ibkr_paper_controlled_fill.local.yaml
```

编辑 `config/ibkr_paper_controlled_fill.local.yaml`：

```yaml
controlled_fill_enabled: true
host: 127.0.0.1
port: 7497
client_id: 19013
account_id: "DUP_REPLACE_WITH_PAPER_ACCOUNT_ID"
trading_mode: paper
production_effect: none
max_quantity: 1
allowed_symbols:
  - NVDA
  - AAPL
  - TSM
allow_market_order: false
allow_sell: false
allow_option: false
allow_margin: false
allow_short: false
require_manual_limit_price: true
```

`config/*.local.yaml` 已被 `.gitignore` 覆盖，不得提交。

## 推荐命令

在 TWS / Gateway Paper 窗口确认当前行情、账户和 limit price 后，人工运行：

```powershell
.\.venv\Scripts\python.exe scripts\run_ibkr_paper_controlled_fill_test.py `
  --date 2026-05-17 `
  --config config\ibkr_paper_controlled_fill.local.yaml `
  --symbol NVDA `
  --side BUY `
  --quantity 1 `
  --limit-price <manual_limit_price>
```

默认输出：

- `outputs/reports/ibkr_paper_controlled_fill_2026-05-17.json`
- `outputs/reports/ibkr_paper_controlled_fill_2026-05-17.md`

这些本机输出被 `.gitignore` 覆盖。提交前只保留代码、文档和脱敏测试，不提交 raw
local report。

## 预期检查

打开 JSON / Markdown，确认：

- `production_effect=none`
- `trading_mode=paper`
- `manual_cli_only=true`
- `account_id_masked` 只显示 masked account id
- `broker_order_id` 为 redacted 形式
- `symbol` 在白名单内
- `side=BUY`
- `quantity=1`
- `order_type=LIMIT`
- `time_in_force=DAY`
- `limit_price` 是人工确认的价格
- `fill_seen=true` 时记录 `fill_quantity`、`avg_fill_price`、`fill_time` 和
  `commission_report_seen`
- `fill_seen=false` 时必须看到 `cancel_requested=true`，且报告状态为 `LIMITED`
  或更保守状态
- 缺少可靠 `MarketSnapshot` 时，`fill_match_status=INSUFFICIENT_MARKET_DATA`，
  不强行比较本地 `PaperBroker`

## No-Fill 的正常判定

非美股正常交易时段下，`BUY LIMIT DAY` 订单可能停留在 `PreSubmitted`。即使人工
设置的 `limit_price` 高于 TWS / Gateway 当时显示的 ask，IBKR Paper 也可能因为
session、routing 或 Paper matching 限制而不成交。

如果出现以下组合，本阶段可把 no-fill 判为正常的受限验证结果：

- `connection_status=CONNECTED`
- `order_status_events` 至少包含 open 状态，例如 `PendingSubmit` 或 `PreSubmitted`
- `fill_seen=false`
- `fill_quantity=0`
- `commission_report_seen=false`
- `cancel_requested=true`
- `final_order_status=Cancelled` 或 `ApiCancelled`
- `issues` 包含 `fill_not_seen_timeout`

该结果只能证明 submit/open/cancel 链路可用，不能证明 fill model。Calibration 层读取
这类样本时应归类为 `NO_FILL_LIFECYCLE_VALIDATED`，但仍应保持
`fill_tested=false` 和 `LIFECYCLE_ALIGNED_FILL_UNTESTED`。

## 成交后的处理

如果 `fill_seen=true`：

- 该成交仍然只是 IBKR Paper simulated account fill，不是实盘成交。
- 不把一次 fill 结果写成策略成交质量结论。
- 不修改 `PaperBroker` fill model。
- 不把结果接入 production 自动交易、daily-run、replay、dashboard、paper signal
  quality 或 shadow impact。
- 检查 Paper account 中是否有残留 open order 或异常 position；如有残留，先在
  TWS / Gateway Paper 手工处理并记录受限状态。
- 只提交脱敏总结或测试 fixture；不要提交完整账号、原始 broker order id、现金细节或
  broker 凭证值。

## 未成交后的处理

如果 `fill_seen=false`：

- 保留报告的 `LIMITED` 状态，不把脚本提交成功解释为 fill model 已验证。
- 不修改 `PaperBroker` fill model。
- 不把单个 no-fill 样本用于放宽或收紧本地 fill simulation。
- 建议在美股正常交易时段重新运行 near-ask `BUY LIMIT`，仍保持 `quantity=1`、
  Paper account、manual CLI only 和 `production_effect=none`。
- 若再次 no-fill，继续记录为诊断样本；等待多个时段、多个 symbol 或真实 fill 样本后
  再讨论 calibration 设计。

## 常见错误

|现象|常见原因|处理|
|---|---|---|
|`controlled_fill_enabled=false`|默认配置未显式启用|只在确认 Paper session 后修改 local YAML 为 `true`|
|`trading_mode must be paper`|配置不是 paper|停止运行，切回 TWS / Gateway Paper session|
|`account_id must start with DUP`|配置缺失或不是 Paper account|确认登录 Paper Trading，只填 `DUP` Paper account|
|`production_effect must be none`|配置试图接入生产效果|恢复 `production_effect: none`；本流程永不进入 production|
|`only BUY is allowed`|传入 SELL|停止运行；第一版 controlled fill 禁止 SELL / short|
|`only LIMIT orders are allowed`|传入 MARKET 或其他订单类型|停止运行；不要放宽到 market order|
|`quantity must equal 1`|数量超过第一版上限|恢复 `quantity=1`|
|成交但无本地对比|缺可靠当日 MarketSnapshot|保留 `INSUFFICIENT_MARKET_DATA`，不要补造 synthetic snapshot|
|`fill_not_seen_timeout` 且最终取消|非正常交易时段、Paper matching 未撮合或订单停留 `PreSubmitted`|记录为 `LIMITED` no-fill；只证明 submit/open/cancel，不修改 `PaperBroker`|
