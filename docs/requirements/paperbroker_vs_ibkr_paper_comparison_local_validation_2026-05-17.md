# TRADING-011A：Comparison Local Validation & Findings

最后更新：2026-05-17

关联任务：`TRADING-011A`

## 背景

`TRADING-011` 已在 `main` 实现 Local `PaperBroker` vs IBKR Paper lifecycle /
fill diagnostic comparison，并通过 CI。本任务只固化 2026-05-17 本机真实
IBKR Paper comparison 验证结果，确认报告安全边界和可审计字段。

本任务不是扩展下单能力，不修改 `PaperBroker` fill model，不接 live account，
不接 production 自动交易，也不改变交易建议、paper signal quality、shadow impact
或参数晋级。

## 本机验证范围

验证命令：

```powershell
python scripts/run_paperbroker_vs_ibkr_paper_comparison.py `
  --date 2026-05-17 `
  --config config/ibkr_paper_order.local.yaml `
  --symbol NVDA `
  --side BUY `
  --quantity 1 `
  --limit-price 10
```

验证前置：

- TWS Paper / IB Gateway Paper 已启动。
- API socket 已开启。
- `config/ibkr_paper_order.local.yaml` 为本地 ignored config，不提交。
- `paper_order_lifecycle_enabled=true`。
- `ibkr_paper_comparison_enabled=true`。
- `trading_mode=paper`。
- `production_effect=none`。
- account id 是 `DUP` Paper account。

## 结果摘要

本机真实 comparison 输出：

- `comparison_status=PASS`
- `comparison_mode=diagnostic_only`
- `production_effect=none`
- local `PaperBroker`：submitted -> open seen -> no fill -> cancelled
- IBKR Paper：open/orderStatus seen -> cancel requested -> final `Cancelled`
- `fills_seen=false`
- `status_match=true`
- `fill_match=true`
- `cancel_match=true`
- `difference_labels=[]`

第一轮运行因 `ibkr_paper_comparison_enabled=false` 被 fail closed 阻断，没有连接
IBKR。显式启用本地 comparison 开关后，第二轮真实运行通过。该行为符合
TRADING-011 安全边界。

## 固化产物

- `docs/reviews/paperbroker_vs_ibkr_paper_comparison_local_validation_2026-05-17.md`
- `tests/fixtures/paperbroker_vs_ibkr_paper_comparison_sanitized.json`
- `docs/runbooks/paperbroker_vs_ibkr_paper_comparison_local_validation.md`
- `tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py` 安全语义测试

## 安全检查

固化产物必须满足：

- 不包含完整 `DUP` account id。
- 不包含真实 broker order id。
- 不包含 password、token、API key、session cookie 或 Authorization header。
- 明确 `paper_only=true`。
- 明确 `comparison_mode=diagnostic_only`。
- 明确 `production_effect=none`。
- 明确不影响 daily-run、dashboard production conclusion、paper signal quality、
  shadow impact、参数晋级或交易建议。

## 后续发现

本次样本没有出现 lifecycle 差异，也没有出现 local filled but IBKR not filled。
因此 TRADING-012 不应基于单个 PASS/open-cancel 样本直接改变 fill model。

TRADING-012 的合理入口是后续积累出现差异的 comparison 样本后，再校准：

- synthetic snapshot fill 是否过度乐观；
- historical bid/ask 是否应作为 fill 模拟前置；
- broker rejection reason mapping；
- synthetic snapshot replay 是否应标记为 `LIMITED`。

## 验收标准

- `python -m pytest tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 推送 `main` 后确认 GitHub Actions CI 通过。

## 状态记录

- 2026-05-17：新增并进入实现。原因：TRADING-011 本机真实 IBKR Paper
  comparison 已跑通，需要固化 sanitized validation evidence 和 runbook，确认没有
  敏感账号、broker order id 或密钥泄露；本任务只记录验证结果，不修改交易执行行为。
- 2026-05-17：实现完成并进入验证。已新增本机验证 review、sanitized fixture、
  runbook 和安全语义测试。真实样本为 NVDA BUY LIMIT DAY 1 @ 10，本地
  `PaperBroker` 与 IBKR Paper 均 no fill / cancelled，status、fill、cancel 均
  match；committed artifact 不包含完整 account id、真实 broker order id 或密钥。
  本地验证通过
  `python -m pytest tests/trading_engine/test_paperbroker_vs_ibkr_paper_comparison.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
