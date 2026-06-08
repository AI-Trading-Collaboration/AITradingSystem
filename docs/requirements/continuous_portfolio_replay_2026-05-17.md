# TRADING-007：Continuous Portfolio Replay

最后更新：2026-06-09

关联任务：`TRADING-007`

## 背景

现有 `paper_trading_replay` 已能以 `daily-independent` 方式逐日复用 candidate
runner，但该模式每天重新初始化 paper portfolio，不能解释持仓、cash、风险暴露或
PnL 的连续结转。项目 owner 本轮要求在现有 paper trading engine 上新增真正的
`continuous-portfolio` replay 模式，仍保持 paper-only 和 `production_effect=none`。

## 范围

1. Replay mode：
   - `scripts/run_paper_trading_replay.py --mode continuous-portfolio` 必须实现。
   - `daily-independent` 保持现有逻辑不变。
   - continuous 输出必须写入 `replay_mode=continuous_portfolio`、
     `portfolio_carry_forward=true`、`production_effect=none`。
2. 组合状态结转：
   - 在 start date 初始化一个 `PaperPortfolio`。
   - 每日读取 `order_intent_candidates_YYYY-MM-DD.json`。
   - 将可执行 candidate 转换为 `OrderIntent`。
   - 每日经过 `PreTradeRiskChecker`。
   - 使用同一个 `PaperPortfolio` 连续执行。
   - 每日结束后生成 `PortfolioState` snapshot。
   - DAY order 当日未成交则过期，不结转 open DAY order。
   - 不支持 GTC；如遇 GTC candidate，本阶段直接 rejected / limited。
3. 连续收益指标：
   - replay JSON/Markdown 输出 `equity_curve`、`daily_equity`、`daily_cash`、
     `daily_exposure`、`daily_realized_pnl`、`daily_unrealized_pnl`、
     `cumulative_realized_pnl`、`cumulative_unrealized_pnl`、`max_drawdown`、
     `exposure_peak`、`position_concentration_peak`。
4. 状态一致性：
   - 每日运行 reconciliation。
   - replay summary 输出 `reconciliation_status` distribution。
   - 任一天 reconciliation 非 PASS 时，整体 status 至少为 `LIMITED`。
   - 每日 portfolio snapshot 必须带 `date`、`cash`、`positions`、`equity`、
     `exposure`。
5. 报告语义：
   - Markdown 明确 continuous-portfolio 是 paper-only 连续组合模拟。
   - 它仍然不是实盘交易，也不是真实 broker 成交。
   - 报告明确 `production_effect=none`，不读取 broker API key。
   - 写清楚它与 `daily-independent` 的区别。

## 边界

- 本任务不接真实券商。
- 本任务不扩展订单类型。
- `continuous-portfolio` 只模拟 paper portfolio 的连续状态，不代表真实账户、
  真实成交、真实滑点、税费或券商状态。
- 不读取 broker API key，不调用 IBKR / Alpaca stub，不改变 production scoring、
  position gate、正式 ledger、approved overlay 或生产仓位建议。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode daily-independent`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode continuous-portfolio`
- 新增 `tests/trading_engine/test_continuous_portfolio_replay.py` 覆盖：
  - 持仓从 day1 结转到 day2。
  - cash 从 day1 结转到 day2。
  - DAY 未成交订单不会跨日保留。
  - realized / unrealized PnL 连续累计。
  - max_drawdown 计算存在。
  - `production_effect` 始终为 `none`。
  - 不调用真实 broker、不读取 API key。
  - `daily-independent` 旧逻辑不变。

## 实施步骤

1. 在 replay runner 内新增 continuous orchestration，复用现有
   `PaperPortfolio`、`PaperBroker`、`ExecutionService`、`PreTradeRiskChecker`、
   `MarketSnapshotProvider` 和 reconciliation。
2. 为 paper broker / execution service 补充 DAY open order 到期处理，使 daily
   close 后不存在跨日 open DAY order。
3. 增加连续 portfolio snapshot 和收益指标序列。
4. 更新 Markdown 报告和系统流图。
5. 补充目标测试并运行验收命令。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 明确要求推进 TRADING-007，在现有
  paper trading engine 上实现真正的 continuous-portfolio replay，同时保持
  paper-only、`production_effect=none` 和真实 broker 隔离边界。
- 2026-05-17：实现完成并进入验证。新增 continuous replay orchestration、同一
  `PaperPortfolio` 跨日结转、每日 `PortfolioState` snapshot、DAY open order
  到期、GTC candidate rejected / limited、每日 reconciliation、连续收益指标、
  Markdown 边界说明和 `tests/trading_engine/test_continuous_portfolio_replay.py`。
  验证通过 `python -m pytest tests/trading_engine`、`python -m pytest`、
  `python -m ruff check scripts src tests`、`python scripts/run_paper_trading_replay.py
  --start 2026-05-01 --end 2026-05-17 --mode daily-independent` 和
  `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17
  --mode continuous-portfolio`。
- 2026-06-09：复核归档。顺序运行两种 replay smoke，`daily-independent`
  输出 `replay_mode=daily_independent`、`portfolio_carry_forward=false`、
  `production_effect=none` 且不生成 continuous metrics；`continuous-portfolio`
  输出 `replay_mode=continuous_portfolio`、`portfolio_carry_forward=true`、
  `production_effect=none`、17 条 `equity_curve` / daily equity / cash /
  exposure 记录、`max_drawdown_pct=0`、`exposure_peak=0` 和
  `position_concentration_peak=0`。当前 smoke 没有可执行订单，因此收益和
  exposure 为 0；专项测试覆盖真实持仓/cash 结转、DAY/GTC 规则、realized /
  unrealized PnL、max drawdown、paper-only 边界和 `daily-independent` 不变。
  验证通过 `python -m pytest tests/trading_engine/test_continuous_portfolio_replay.py
  -q`（4 passed）、`python -m pytest tests/trading_engine -q`（939 passed,
  330 warnings）、`python -m pytest -q`（2270 passed, 330 warnings）、
  scoped Black/Ruff 和两种 replay smoke。任务从 `VALIDATING` 归档为
  `DONE`；后续真实 daily flow 可见性和持续观察由 TRADING-007A/TRADING-008
  等后续任务承接。
