# TRADING-013：IBKR Paper Controlled Small Fill Test

最后更新：2026-06-07

关联任务：`TRADING-013`

## 背景

`TRADING-012` 当前输出 `calibration_status=LIFECYCLE_ALIGNED_FILL_UNTESTED`：
IBKR Paper open / cancel lifecycle 已与本地路径对齐，但尚无真实 Paper fill 样本可
用于诊断 `PaperBroker` fill model。本任务新增一个人工显式运行的受控小额成交测试，
用于采集 IBKR Paper fill 观察，并在条件允许时与本地 `PaperBroker` 做诊断对比。

本任务不是实盘交易，不接 production 自动交易，不修改 `PaperBroker` fill 行为。

## 范围

1. 新增人工显式 CLI：
   `python scripts/run_ibkr_paper_controlled_fill_test.py --date YYYY-MM-DD --config config/ibkr_paper_order.local.yaml --symbol NVDA --side BUY --quantity 1 --limit-price <manual_limit_price>`
2. 新增默认关闭配置：
   `config/ibkr_paper_controlled_fill.yaml`
3. 输出：
   - `outputs/reports/ibkr_paper_controlled_fill_YYYY-MM-DD.json`
   - `outputs/reports/ibkr_paper_controlled_fill_YYYY-MM-DD.md`
4. 报告固定声明：
   - `production_effect=none`
   - `trading_mode=paper`
   - `manual_cli_only=true`
   - `production_surface_impact=none`

## 配置

默认配置必须 fail closed：

```yaml
controlled_fill_enabled: false
trading_mode: paper
production_effect: none
max_quantity: 1
allowed_symbols: [NVDA, AAPL, TSM]
allow_market_order: false
allow_sell: false
allow_option: false
allow_margin: false
allow_short: false
require_manual_limit_price: true
```

本地运行时可在 ignored `config/*.local.yaml` 中补充 TWS / IB Gateway Paper 连接字段
和 `DUP` Paper account。配置不得保存 password、token、API key、session cookie 或
其他凭证。

## 执行流程

1. 读取配置和 CLI 参数。
2. 在连接 IBKR 前校验：
   - `controlled_fill_enabled=true`
   - `trading_mode=paper`
   - `production_effect=none`
   - `account_id` 必须是 `DUP` Paper account
   - 只允许 `stock` / `BUY` / `LIMIT` / `DAY`
   - `quantity=1`
   - `symbol` 在白名单内
   - 必须人工传入正数 `limit_price`
3. 连接 IBKR Paper。
4. 构造 `BUY LIMIT DAY quantity=1` stock order。
5. 提交 IBKR Paper order。
6. 等待 `orderStatus` / `execDetails` / `commissionReport`。
7. 如果 timeout 内成交，记录 fill quantity、avg fill price、fill time 和
   commission report 是否出现。
8. 如果未成交，请求 cancel 并记录最终状态。
9. 即使失败，也必须输出 `BLOCK` / `ERROR` / `LIMITED` 报告。

## 报告字段

JSON 至少包含：

- `test_status`
- `production_effect=none`
- `account_id_masked`
- `symbol`
- `side`
- `quantity`
- `limit_price`
- `order_status_events`
- `open_order_seen`
- `fill_seen`
- `fill_quantity`
- `avg_fill_price`
- `fill_time`
- `cancel_requested`
- `final_order_status`
- `commission_report_seen`
- `issues`
- `safety_checks`

IBKR broker order id 只能输出 redacted 形式，不得写入完整 account id 或原始 broker
order id。

## PaperBroker 诊断对比

只有 `fill_seen=true` 时才尝试本地对比：

- 用同一个 `OrderIntent`。
- 优先使用调用方传入或本地可靠历史 OHLC 形成的当日 `MarketSnapshot`。
- 若缺少可靠 `MarketSnapshot`，不强行比较，输出 `fill_match_status=INSUFFICIENT_MARKET_DATA`。

存在可靠 snapshot 时输出：

- `local_fill_seen`
- `local_avg_fill_price`
- `ibkr_fill_seen`
- `ibkr_avg_fill_price`
- `fill_price_diff`
- `fill_match_status`

一次 Paper fill 结果不得直接用于修改 `PaperBroker` fill model，只能作为后续诊断证据。

## 安全边界

- 不允许 live account。
- 不允许 market order。
- 不允许 `SELL`。
- 不允许 `quantity > 1`。
- 不允许 `production_effect != none`。
- 不记录完整 account id。
- 不记录 password、token、API key、session cookie。
- 不影响 daily dashboard、replay、paper signal quality 或 shadow impact。
- 不自动修改 `PaperBroker` fill model。
- mock 测试不得连接真实 IBKR。

## 验收标准

- `python -m pytest tests/trading_engine/test_ibkr_paper_controlled_fill.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 提交并 push 后确认 GitHub Actions 本次 commit 对应 CI run 通过。

## 状态记录

- 2026-05-17：新增并进入实现。原因：TRADING-012 已确认 lifecycle 对齐但
  fill model 未验证，owner 要求新增受控 IBKR Paper 小额成交测试能力，用于采集
  Paper fill 样本并与本地 `PaperBroker` 做诊断对比；本阶段保持 paper-only、
  manual-only、`production_effect=none`，不接 production 自动交易，不修改
  `PaperBroker` fill 行为。
- 2026-05-17：实现完成并进入验证。已新增
  `config/ibkr_paper_controlled_fill.yaml`、
  `src/ai_trading_system/trading_engine/brokers/ibkr_paper_controlled_fill.py`、
  `scripts/run_ibkr_paper_controlled_fill_test.py`、JSON/Markdown report writer、
  broker order id redaction、同 `OrderIntent` 的本地 `PaperBroker` 诊断对比、
  runbook、系统流图、artifact catalog 和 mock IBKR client 测试。本地验证通过
  `python -m pytest tests/trading_engine/test_ibkr_paper_controlled_fill.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests`、`python -m black --check scripts src tests`
  和 `git diff --check`。
- 2026-05-17：本机 controlled fill 运行在写报告前触发
  `controlled fill output contains an unredacted broker order id` fail-closed
  检查。初步定位为短数字 IBKR broker order id 可能与 `quantity=1`、日期、
  redaction token 长度等非敏感输出冲突；当前处理要求修复结构化敏感输出检查、
  保留 fail-closed 语义、补充短 order id 回归测试，并在通过后重新运行本机验证。
- 2026-05-17：修复完成并重新验证。broker order id 文本脱敏和写盘前检查改为
  上下文感知，只在 `broker_order_id` / `orderId` / `permId` 等 broker id
  字段或明确文本上下文中识别原始 id，避免短数字 order id 与 `quantity=1`、
  日期或 redaction token 长度误匹配；仍保留 account id 和 broker id
  fail-closed 检查。新增短 `order_id=1` 回归测试。本机命令
  `python scripts/run_ibkr_paper_controlled_fill_test.py --date 2026-05-17 --config config/ibkr_paper_controlled_fill.local.yaml --symbol NVDA --side BUY --quantity 1 --limit-price 200`
  已正常生成 JSON/Markdown，`test_status=LIMITED`、`fill_seen=false`、
  `cancel_requested=true`、`final_order_status=Cancelled`，broker order id 为
  `[REDACTED_BROKER_ORDER_ID:len=N]` 形式。
- 2026-06-07：从 VALIDATING 改为 DONE。原因：当前 HEAD `725816a7` 的
  GitHub Actions CI run `27085392644` 已完成且通过；本轮复验
  `tests/trading_engine/test_ibkr_paper_controlled_fill.py`、相关 calibration /
  comparison focused tests 和触达文件 ruff 通过。受控 fill 脚本仍保持
  manual-only、paper-only、`production_effect=none`，不修改 `PaperBroker` fill
  行为。
