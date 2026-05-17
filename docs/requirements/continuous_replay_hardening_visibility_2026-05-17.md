# TRADING-007A：Continuous Replay Hardening & Visibility

最后更新：2026-05-17

关联任务：`TRADING-007A`

## 背景

`TRADING-007` 已实现 `continuous-portfolio` replay。当前阶段需要加固连续复盘
的 JSON/Markdown 语义、可见性，以及与 daily task dashboard 和
paper signal quality 的衔接。目标仍是 paper-only 诊断闭环，不接真实券商，
不扩展订单类型，不把连续复盘解释为实盘上线依据。

## 范围

1. Continuous replay summary：
   - `paper_trading_replay_START_END.json` 必须明确输出 `replay_mode`、
     `portfolio_carry_forward`、`production_effect`、`order_expiration_policy`、
     `unsupported_order_policy` 和 `continuous_metrics_available`。
   - 增加 `expired_day_orders`、`rejected_gtc_orders`、
     `carried_positions_count`、`final_cash`、`final_equity`、
     `final_positions`、`max_position_concentration` 和 `max_drawdown_pct`。
2. Markdown 报告：
   - 明确 `daily-independent` 是每天重新初始化组合。
   - 明确 `continuous-portfolio` 是 paper-only 连续组合模拟。
   - 明确 continuous replay 仍不是真实账户收益、真实 broker 成交、
     税费/滑点完整模拟或实盘上线依据。
   - 展示 final portfolio summary、max drawdown、exposure peak 和
     expired DAY orders。
3. Daily task dashboard：
   - Paper Trading Trend 区块只读识别最近 existing replay 的 replay mode。
   - 最近 replay 为 `continuous-portfolio` 时展示 final equity、max drawdown、
     exposure peak、final positions count 和 `portfolio_carry_forward=true`。
   - 只有 `daily-independent` 时继续显示逐日独立 warning。
   - dashboard 不触发 replay，不读取 broker API key，不调用真实 broker。
4. Paper signal quality：
   - 使用 `continuous-portfolio` replay 时移除 `DAILY_INDEPENDENT_ONLY`
     warning。
   - 必须新增或保留 `PAPER_ONLY_SIMULATION` warning。
   - 不允许把 continuous replay 解释为 `READY_FOR_LIVE`、`SHOULD_TRADE`、
     `PROMOTE` 或任何上线/交易/晋级语义。
   - `evaluation_status` 和 `quality_status` 仍只能使用保守状态集合。

## 边界

- 不接真实券商。
- 不扩展订单类型。
- 不读取 broker API key。
- 不调用 IBKR / Alpaca stub 或任何真实 broker adapter。
- 不改变 production scoring、position gate、正式 ledger、approved overlay、
  参数晋级或生产仓位建议。
- continuous replay 只解释 paper portfolio 的连续状态，不代表真实账户收益、
  真实成交、完整税费/滑点模拟或实盘上线依据。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode daily-independent`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode continuous-portfolio`
- 提交并 push 后确认 GitHub Actions 本次 commit 对应 CI run 通过。

## 实施步骤

1. 增强 replay JSON 和 Markdown 的连续组合摘要、订单政策和风险边界字段。
2. 扩展 dashboard Paper Trading Trend 的只读 latest replay 摘要。
3. 调整 paper signal quality warning 生成和 Markdown 展示。
4. 更新系统流图、产物目录和测试。
5. 运行验收命令，提交、push 并检查 CI。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求在 TRADING-007 已实现
  continuous replay 后，加固 summary 语义、dashboard 可见性和
  paper signal quality 边界，同时保持 paper-only、`production_effect=none`、
  不接真实券商、不扩展订单类型。
- 2026-05-17：实现完成并进入验证。已完成 replay JSON/Markdown summary
  hardening、`order_expiration_policy`、`unsupported_order_policy`、
  `continuous_metrics_available`、expired DAY / rejected GTC 计数、final
  portfolio summary、dashboard latest replay 只读摘要，以及
  paper signal quality 的 `PAPER_ONLY_SIMULATION` warning；continuous replay
  输入下不再误报 `DAILY_INDEPENDENT_ONLY`。本地验证通过 `python -m pytest
  tests/trading_engine`、`python -m pytest tests/test_daily_task_dashboard.py`、
  `python -m pytest`、`python -m ruff check scripts src tests`、
  `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end
  2026-05-17 --mode daily-independent`、`python scripts/run_paper_trading_replay.py
  --start 2026-05-01 --end 2026-05-17 --mode continuous-portfolio` 和
  `git diff --check`。下一步确认本次 push 对应 GitHub Actions CI run。
