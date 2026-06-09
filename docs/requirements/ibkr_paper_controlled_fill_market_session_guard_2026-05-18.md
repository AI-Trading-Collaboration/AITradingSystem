# TRADING-014：IBKR Paper Controlled Fill Market Session Guard

最后更新：2026-06-09

关联任务：`TRADING-014`

## 背景

`TRADING-013A` 已固化一次本机 controlled fill no-fill 样本：order path 为
`PendingSubmit -> PreSubmitted -> PendingCancel -> Cancelled`，`fill_seen=false`，
issue 为 `fill_not_seen_timeout`。当前最合理解释是运行时不在美股 regular trading
session，订单停留在 `PreSubmitted`，因此该 no-fill 不应归因于 `PaperBroker` fill
model，也不应把 calibration 改成 fill-tested。

本任务在 controlled fill 订单提交前增加 market session guard，避免非 regular
session 下把 “高于 ask 但不成交” 误读为本地 fill model 问题。

## 范围

- 新增 `src/ai_trading_system/trading_engine/market_data/market_session_guard.py`。
- 在 `scripts/run_ibkr_paper_controlled_fill_test.py` / controlled fill 核心函数中，
  `submit_order` 前调用 guard。
- 默认只允许 `REGULAR` session 提交 controlled fill。
- `CLOSED` / `OUTSIDE_RTH` / `UNKNOWN` 默认不提交订单，并输出
  `controlled_fill_submission=blocked_by_session_guard`。
- 新增显式诊断覆盖参数 `--allow-outside-rth-diagnostic`。
- 报告和 runbook 解释非 regular session 下 `PreSubmitted` 不等于 fill failure。

## 非范围

- 不接 live trading。
- 不修改 `PaperBroker` fill 行为。
- 不接 daily-run、dashboard 自动执行或 paper signal quality 自动消费。
- 不把一次 outside-RTH 诊断结果解释为 calibration fill-tested。

## 实施步骤

1. `MarketSessionGuard` 解析 IBKR `contractDetails.tradingHours` /
   `contractDetails.liquidHours` 和 exchange timezone，输出 status、can-submit、
   reason、source。
2. controlled fill 核心函数连接 IBKR Paper 后、`submit_order` 前获取 contract
   details，并默认在非 `REGULAR` 时阻断提交。
3. CLI 增加 `--allow-outside-rth-diagnostic`，开启后允许非 regular session 继续提交，
   但报告必须标记 `outside_rth_override=true`、`test_status=LIMITED` 或更保守状态。
4. JSON/Markdown/runbook/system flow/artifact catalog 同步新增 session guard 字段和解释。
5. 测试覆盖 closed、regular、outside-RTH、unknown、override、blocked 不调用
   `submit_order`、`production_effect=none` 和不触发 daily-run/dashboard/API key。

## 验收

- `python -m pytest tests/trading_engine/test_market_session_guard.py`
- `python -m pytest tests/trading_engine/test_ibkr_paper_controlled_fill.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- commit、push 后 GitHub Actions CI 通过。

## 进展记录

- 2026-05-18：新增并进入 `IN_PROGRESS`。前置 `TRADING-013A` 已完成，本轮开始实现
  market session guard，防止非 regular session no-fill 被误读为 fill model 问题。
- 2026-05-18：从 `IN_PROGRESS` 改为 `VALIDATING`。已新增
  `market_session_guard.py`、IBKR Paper adapter contract details 查询、controlled fill
  submit 前 guard、`--allow-outside-rth-diagnostic`、JSON/Markdown session guard 字段、
  runbook、系统流图、artifact catalog 和 mock tests。本地验证通过目标 pytest、
  `tests/trading_engine`、全量 pytest、ruff 和 black check。
- 2026-05-18：补充后续路线。`TRADING-014` 后续不直接进入 fill model 改造，而是等待
  美股 `REGULAR` session 运行 controlled fill；`fill_seen=true` 时进入 `TRADING-014A`
  固化成功 fill 本机验证，再进入 `TRADING-014C` proposal；`fill_seen=false` 时进入
  `TRADING-014B` failure diagnostics。无真实 regular session fill 样本前，不修改
  `PaperBroker` fill 行为。
- 2026-06-09：`TRADING-014` 从 VALIDATING 归档为 DONE。归档复核确认本任务范围
  是 controlled fill 提交前 market session guard 和非 regular session fail-closed
  语义，真实 `REGULAR` session 成功成交/失败诊断继续由 `TRADING-014A` /
  `TRADING-014B` / `TRADING-014C` 承接，不作为本任务收口前置条件。验证通过
  `python -m pytest tests/trading_engine/test_market_session_guard.py
  tests/trading_engine/test_ibkr_paper_controlled_fill.py -q`（29 passed）；默认配置
  smoke 使用 `python scripts/run_ibkr_paper_controlled_fill_test.py --date 2026-05-18
  --output-dir outputs/reports/trading014_check --symbol NVDA --side BUY --quantity 1
  --limit-price 10` 输出 `test_status=BLOCK`、`connection_status=NOT_RUN`、
  `controlled_fill_enabled=false`、`controlled_fill_submission=not_evaluated`、
  `outside_rth_override=false`、`fill_seen=false`、`cancel_requested=false`，
  证明未显式启用 local Paper 配置时不会连接或提交订单。安全扫描未发现完整
  account id、credential 赋值或 forbidden production semantics；归档前代码基线
  GitHub Actions `CI` run `27175962934` success。
  追加复验 `python -m pytest tests/trading_engine -q` 为 939 passed。
