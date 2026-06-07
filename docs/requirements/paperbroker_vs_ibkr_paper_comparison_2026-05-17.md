# TRADING-011：Local PaperBroker vs IBKR Paper Lifecycle / Fill Comparison

最后更新：2026-06-07

关联任务：`TRADING-011`

## 背景

`TRADING-010` / `TRADING-010A` 已完成 IBKR Paper order lifecycle 本机验证，
并已生成脱敏 local validation report。本任务不扩展下单能力，不接 live account，
不接 production 自动交易；目标只是用同一个 `OrderIntent` 对比本地
`PaperBroker` 模拟和 IBKR Paper lifecycle 在 open / cancel / fill 观察上的差异。

## 范围

1. 新增人工显式 CLI：
   `python scripts/run_paperbroker_vs_ibkr_paper_comparison.py --date YYYY-MM-DD --config config/ibkr_paper_order.local.yaml --symbol NVDA --side BUY --quantity 1 --limit-price 10`
2. 输出：
   - `outputs/reports/paperbroker_vs_ibkr_paper_comparison_YYYY-MM-DD.json`
   - `outputs/reports/paperbroker_vs_ibkr_paper_comparison_YYYY-MM-DD.md`
3. JSON 和 Markdown 必须固定声明：
   - `production_effect=none`
   - `comparison_mode=diagnostic_only`
   - 不影响 daily-run、dashboard production conclusion、paper signal quality、
     shadow impact 或参数晋级。
4. 复用 TRADING-010 的 IBKR Paper order lifecycle 安全边界，并新增显式开关
   `ibkr_paper_comparison_enabled=true`。默认配置保持关闭。

## 输入限制

第一版只允许：

- `asset_type=stock`
- `order_type=LIMIT`
- `time_in_force=DAY`
- `side=BUY`
- `quantity=1`
- `symbol` 在白名单内，默认 `NVDA`、`AAPL`、`TSM`

禁止：

- market order
- option
- short / `SELL`
- margin
- `GTC`
- bracket / stop / trailing / algo / OCA / parent-child order
- live account 或非 `DUP` account

输入可以来自显式 CLI 参数，或后续用于本地复核的 `OrderIntent` fixture。无论来源，
比较工具都必须在连接 IBKR 前完成配置和订单限制校验。

## 比较流程

1. 读取配置并验证：
   - `trading_mode=paper`
   - `production_effect=none`
   - `paper_order_lifecycle_enabled=true`
   - `ibkr_paper_comparison_enabled=true`
   - `account_id` 必须为 `DUP` Paper account。
2. 构造或读取同一个 `OrderIntent`。
3. 在本地 `PaperBroker` 中提交、用 snapshot 触发或不触发 fill simulation，并尝试
   cancel open order。
4. 用相同 `OrderIntent` 提交 IBKR Paper lifecycle，记录 open/status/cancel/fill
   事件。
5. 输出差异分析和 recommendations；差异不是失败，除非触发安全边界或配置阻断。

## 报告字段

本地 `PaperBroker` 至少记录：

- `local_order_status`
- `local_open_order_seen`
- `local_fill_seen`
- `local_avg_fill_price`
- `local_cancel_result`
- `local_final_status`
- `local_reconciliation_status`

IBKR Paper 至少记录：

- `broker_order_id` masked/redacted
- `openOrder seen`
- `orderStatus events`
- `cancel requested`
- `final status`
- `fills seen`
- `ibkr_reconciliation_status`

差异输出至少包含：

- `status_match`
- `fill_match`
- `cancel_match`
- `local_filled_but_ibkr_not_filled`
- `ibkr_rejected_but_local_accepted`
- `local_price_source`
- `ibkr_reference_price_available`
- `lifecycle_event_gap`

差异标签只允许使用：

- `EXPECTED_DIFFERENCE`
- `LOCAL_SIM_TOO_OPTIMISTIC`
- `BROKER_REJECTED`
- `INSUFFICIENT_MARKET_DATA`
- `CANCEL_TIMING_DIFFERENCE`

recommendations 只输出诊断建议，不直接修改 `PaperBroker` 行为：

- `consider stricter fill simulation`
- `require historical bid/ask`
- `mark synthetic snapshot replay as LIMITED`
- `add broker_rejection_reason mapping`

## 安全边界

- 不允许 live account。
- 不允许 `production_effect != none`。
- 不允许由 daily candidates 自动触发。
- 必须人工运行 CLI。
- 不记录完整 account id。
- 不记录 password、token、API key 或 session secret。
- 不把结果写入 production dashboard。
- 不影响交易建议、paper signal quality、shadow impact 或参数晋级。
- mock 测试不得连接真实 IBKR。

## 验收标准

- `python -m pytest tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 提交并 push 后确认 GitHub Actions 本次 commit 对应 CI run 通过。

## 实施步骤

1. 更新任务登记、需求文档、配置默认值和系统流图。
2. 实现 diagnostic-only comparison 核心逻辑和 CLI。
3. 使用 mock IBKR client 覆盖 fail-closed、安全边界、正常 open/cancel、broker
   rejected、local filled but IBKR not filled、脱敏和不触发 production surface。
4. 跑本地验收，提交、push 并检查 CI。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 明确 TRADING-010 / 010A 已完成，
  要求比较本地 `PaperBroker` 与 IBKR Paper lifecycle/fill 观察差异；本任务只做
  `diagnostic_only` 报告，不修改 `PaperBroker` 行为，不扩展下单能力，不接 live 或
  production 自动交易。
- 2026-05-17：实现完成并进入验证。已新增 comparison 核心逻辑、人工显式 CLI、
  `ibkr_paper_comparison_enabled` 默认关闭开关、JSON/Markdown 报告、broker order
  id redaction、系统流图和 mock IBKR client 测试。本地验证通过
  `python -m pytest tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
- 2026-06-07：从 VALIDATING 改为 DONE。原因：当前 HEAD `725816a7` 的
  GitHub Actions CI run `27085392644` 已完成且通过；本轮复验
  `tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py` 和相关
  focused tests / ruff 通过。Diagnostic-only comparison 的安全边界和 CI
  验收均已闭合。
