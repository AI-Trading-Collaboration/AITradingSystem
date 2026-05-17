# TRADING-002：Paper Trading Closed Loop

最后更新：2026-05-17

关联任务：`TRADING-002`

## 背景

当前 paper trading engine 已能用 demo 生成 OrderIntent、执行 paper broker、写
audit JSONL 和 trading daily report。下一步需要把模拟执行从孤立报告推进为每日
系统闭环的一部分：candidate 仍来自 daily decision / score / dashboard 结论，
交易仍然只允许 paper mode，执行结果必须回到 daily task dashboard。

## 范围

1. 为 `PaperPortfolio` 增加 reconciliation 能力：
   - 新增 `ReconciliationResult` schema。
   - 可从 fill logs 或 `ExecutionReport` 重建 expected portfolio。
   - 与当前 `PaperPortfolio` / `PortfolioState` 对比。
   - cash、position quantity、avg cost 不一致时输出 BLOCK 级结果。
   - `TradingDailyReport` 增加 reconciliation status。
2. 为候选意图新增 schema 与 adapter：
   - 新增 `OrderIntentCandidate`。
   - 由 daily decision / score / dashboard 结论生成 candidate，不执行交易。
   - 所有 candidate 默认 `production_effect=none`。
   - candidate 可 `blocked=true` 并记录 `blocked_by`。
   - 只有 `blocked=false` 且 `mode=paper` 时才允许转换为 `OrderIntent`。
   - 新增 `scripts/run_paper_trading_from_candidates.py`，保留
     `scripts/run_paper_trading_demo.py` 作为最小 demo。
3. daily task dashboard 增加 Paper Trading Summary：
   - generated_intents
   - approved / rejected
   - submitted
   - filled / open / cancelled
   - realized_pnl / unrealized_pnl
   - reconciliation_status
   - audit_log_path
   - `production_effect=none`

## 边界

- 该闭环仍为 paper-only，不能接真实 broker API。
- candidate 产物和 dashboard summary 均为 `production_effect=none`。
- blocked candidate 不得转换为 `OrderIntent`。
- 非 `mode=paper` 不得转换为 `OrderIntent`。
- demo script 保留为最小可运行样例；candidate runner 是日常 paper entrypoint，
  但仍不代表真实交易入口。

## 验收标准

- reconciliation 测试覆盖正常成交、拒单、取消、部分未成交。
- adapter 测试覆盖 blocked、非 paper mode 和 allowed paper conversion。
- candidate runner 测试确认从 candidate JSON 生成 paper trading report/audit，
  但不触发真实 broker。
- daily task dashboard HTML/JSON 显示 Paper Trading Summary，并链接交易日报 /
  audit log。
- 目标测试、全量 pytest、ruff、目标 mypy、diff check 和 demo smoke 通过。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求把 paper trading 从孤立报告
  接入 daily task dashboard，形成“趋势判断 -> 候选意图 -> paper 执行 ->
  reconciliation -> dashboard 复盘”的闭环，同时保持真实交易禁用。
- 2026-05-17：完成基础实现并进入验证。新增 `ReconciliationResult`、
  `OrderIntentCandidate`、candidate -> `OrderIntent` adapter、
  `scripts/run_paper_trading_from_candidates.py`、Paper Trading Summary JSON 和
  dashboard 区块；目标测试已覆盖 reconciliation、adapter、runner 和 dashboard
  接线。待全量测试、ruff、diff check 和 demo smoke 通过后更新任务状态。
