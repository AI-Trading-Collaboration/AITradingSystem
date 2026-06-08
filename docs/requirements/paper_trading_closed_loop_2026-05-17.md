# TRADING-002：Paper Trading Closed Loop

最后更新：2026-06-09

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
   - candidate_count
   - blocked_candidates
   - generated_intents
   - approved / rejected
   - submitted
   - filled / open / cancelled
   - realized_pnl / unrealized_pnl
   - reconciliation_status
   - audit_log_path
   - `production_effect=none`
4. 将 candidate runner 接入每日主流程产物链：
   - `scripts/run_paper_trading_from_candidates.py --date YYYY-MM-DD` 默认读取
     `outputs/reports/order_intent_candidates_YYYY-MM-DD.json`。
   - 若默认候选文件缺失，runner 必须先生成
     `daily_decision_summary_YYYY-MM-DD.json` 和正式
     `order_intent_candidates_YYYY-MM-DD.json`；缺少上游日报、dashboard 或
     decision snapshot 时只能标记 `missing` / `limited`，不得补造投资结论。
   - runner 输出 `outputs/reports/paper_trading_summary_YYYY-MM-DD.json`、
     `reports/trading_daily/YYYY-MM-DD.md` 和 `data/trading_engine/audit/`。
   - paper trading 失败只能在 summary/dashboard 中标记 `LIMITED` / `ERROR`；
     不阻断每日主报告，不改变 production scoring、position gate、ledger 或
     仓位建议。

## 边界

- 该闭环仍为 paper-only，不能接真实 broker API。
- candidate 产物和 dashboard summary 均为 `production_effect=none`。
- blocked candidate 不得转换为 `OrderIntent`。
- 非 `mode=paper` 不得转换为 `OrderIntent`。
- 运行过程不得读取真实 broker API key，不得实现真实 `submit_order`。
- paper trading 结果不得改变生产仓位建议、评分、回测或正式结论。
- demo script 保留为最小可运行样例；candidate runner 是日常 paper entrypoint，
  但仍不代表真实交易入口。

## 验收标准

- reconciliation 测试覆盖正常成交、拒单、取消、部分未成交。
- adapter 测试覆盖 blocked、非 paper mode 和 allowed paper conversion。
- candidate runner 测试确认从 candidate JSON 生成 paper trading report/audit，
  但不触发真实 broker。
- daily task dashboard HTML/JSON 显示 Paper Trading Summary，并链接交易日报 /
  audit log。
- 默认 `python scripts/run_paper_trading_from_candidates.py --date 2026-05-17`
  生成 `daily_decision_summary`、`order_intent_candidates`、
  `paper_trading_summary`、trading daily report 和 audit root。
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
- 2026-05-17：重新进入实现。原因：owner 明确本轮目标不是扩展真实券商能力，
  而是把当前 paper trading engine 接入每日主流程；需要补默认上游产物生成、
  runner 默认路径、dashboard candidate 维度、失败 LIMITED/ERROR 降级和
  2026-05-17 smoke 产物验收。
- 2026-05-17：本轮实现完成并进入验证。默认 candidate runner 在缺少候选文件时
  会先生成 limited `daily_decision_summary` 和正式 `order_intent_candidates`，
  缺少上游报告只标记 `missing` / `limited`；`paper_trading_summary` 新增
  status、candidate_count 和 blocked_candidates；daily task dashboard 展示
  candidate 维度并链接 trading daily report / audit root；`data_gate_blocked`
  已进入 blocked_by 语义。验证通过 trading_engine 测试、全量 pytest、ruff、
  diff check 和 2026-05-17 default runner smoke。
- 2026-06-09：系统验证收口并归档为 DONE。当前 default runner smoke
  `python scripts\run_paper_trading_from_candidates.py --date 2026-05-17`
  成功生成 limited upstream / candidate / paper summary / trading daily report，
  输出 candidate_count=1、generated_intents=0、runner status=`LIMITED`、
  reconciliation=`PASS`，符合缺少上游日报时 fail-closed 降级语义；目标测试
  `test_reconciliation.py`、`test_order_intent_candidate_adapter.py`、
  `test_candidate_runner.py` 和 `test_daily_task_dashboard.py` 通过（34 passed）；
  完整 `tests/trading_engine` 目录通过（939 passed, 330 warnings）；scoped Ruff
  通过。真实 broker、continuous replay、多日 replay、paper signal quality 和
  IBKR paper 扩展由后续 TRADING-003+ 任务承接，不再作为 TRADING-002 未完成项。
