# TRADING-013A：IBKR Paper Controlled Fill Local Validation Report

最后更新：2026-06-07

关联任务：`TRADING-013A`

## 背景

`TRADING-013` 已完成并通过 CI。本机运行 controlled fill test 时，IBKR Paper
连接成功，`NVDA` `BUY` `LIMIT` `DAY` `quantity=1` 订单成功提交，并进入
`PreSubmitted` / open 状态；随后因超时未观察到成交而取消。当前样本疑似发生在
非美股正常交易时段，因此 `fill_seen=false`。

本任务只固化这次本机验证结果，不新增交易能力，不修改 `PaperBroker` fill 行为，
也不把 controlled fill 脚本成功运行解释为 fill model 已验证。

## 本机验证事实

- `test_status=LIMITED`
- `connection_status=CONNECTED`
- `symbol=NVDA`
- `side=BUY`
- `quantity=1`
- `limit_price=224.50`
- order status path：`PendingSubmit -> PreSubmitted -> PendingCancel -> Cancelled`
- `fill_seen=false`
- `fill_quantity=0`
- `commission_report_seen=false`
- `cancel_requested=true`
- `final_order_status=Cancelled`
- `issue=fill_not_seen_timeout`
- likely explanation：non-regular trading session / order remained `PreSubmitted`

## 范围

新增并维护：

- `docs/reviews/ibkr_paper_controlled_fill_local_validation_2026-05-17.md`
- `tests/fixtures/ibkr_paper_controlled_fill_no_fill_sanitized.json`
- `docs/runbooks/ibkr_paper_controlled_fill_local_validation.md`
- `src/ai_trading_system/trading_engine/reports/paperbroker_fill_model_calibration.py`
- 目标测试

## 安全与生产边界

必须明确：

- `production_effect=none`
- paper-only
- manual CLI only
- 不影响 daily-run / replay / dashboard / production conclusion
- 不修改 `PaperBroker` fill model
- fill model remains unvalidated
- 单个 no-fill 样本不得用于修改 `PaperBroker`

提交的 fixture 和 review 不得包含完整 `DUP` account id、真实 broker order id、
perm id、execution id、password、token、API key 或完整账户标识。

## Calibration 语义

当 calibration 层读取 controlled fill no-fill artifact：

- `fill_seen=false` 且 `final_order_status=Cancelled` 应归类为
  `NO_FILL_LIFECYCLE_VALIDATED`。
- `fill_tested` 必须继续为 `false`。
- `calibration_status` 必须继续为 `LIFECYCLE_ALIGNED_FILL_UNTESTED`，不能因为
  controlled fill 脚本成功提交、open、cancel 就认为 fill model 已验证。
- 本阶段不得修改 `PaperBroker` fill model。

## 验收标准

- `python -m pytest tests/trading_engine/test_ibkr_paper_controlled_fill.py`
- `python -m pytest tests/trading_engine/test_paperbroker_fill_model_calibration.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 提交、push 后确认 GitHub Actions CI 通过。

## 状态记录

- 2026-05-18：新增并进入实现。原因：owner 要求在 TRADING-013 完成后固化
  2026-05-17 本机 controlled fill no-fill 验证结果，明确该样本只证明
  submit/open/cancel 链路，不证明 fill model；同时要求 calibration 继续输出
  `fill_tested=false` 和 `LIFECYCLE_ALIGNED_FILL_UNTESTED`。
- 2026-05-18：实现完成并进入验证。已新增本机验证 review、sanitized no-fill
  fixture、runbook no-fill 解释和复跑建议、calibration
  `NO_FILL_LIFECYCLE_VALIDATED` 只读分类、系统流图/产物目录更新和安全语义测试。
  本地验证通过
  `python -m pytest tests/trading_engine/test_ibkr_paper_controlled_fill.py`、
  `python -m pytest tests/trading_engine/test_paperbroker_fill_model_calibration.py`、
  `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests`、`python -m black --check scripts src tests`
  和 `git diff --check`。
- 2026-06-07：从 VALIDATING 改为 DONE。原因：当前 HEAD `725816a7` 的
  GitHub Actions CI run `27085392644` 已完成且通过；本轮复验 controlled fill /
  calibration focused tests 和触达文件 ruff 通过。No-fill local validation
  evidence 仍只证明 submit/open/cancel 链路，不证明 fill model。
