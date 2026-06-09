# TRADING-006：Paper Signal Quality Evaluation

状态：`BASELINE_DONE`

最后更新：2026-06-09

关联任务：`TRADING-006`

## 背景

TRADING-002/003/004/005 已形成 paper trading daily summary、多日 replay、snapshot
source 质量标记和 daily task dashboard 趋势入口。当前缺口不是接真实券商，也不是扩展
订单类型，而是需要一个只读 evaluation 层，把 paper signal 的样本、成交、数据质量和
execution state 可信度显式评估出来，避免 dashboard trend 被误读成上线依据。

## 范围

1. 新增只读产物：
   - `outputs/reports/paper_signal_quality_YYYY-MM-DD.json`
   - `outputs/reports/paper_signal_quality_YYYY-MM-DD.md`
   - 读取最近 7 / 14 / 30 日 `paper_trading_summary_YYYY-MM-DD.json`。
   - 读取最近 7 / 14 / 30 日 `order_intent_candidates_YYYY-MM-DD.json`。
   - 可选读取 `paper_trading_replay_START_END.json`，只作为已有 replay 可见性补充，
     不主动触发 replay。
2. 增加 evaluation gate：
   - `INSUFFICIENT_SAMPLE`
   - `INSUFFICIENT_FILLED_SAMPLE`
   - `LOW_DATA_QUALITY`
   - `LIMITED_MARKET_DATA`
   - `UNRELIABLE_EXECUTION_STATE`
   - gate 只影响 paper signal quality 解释，不影响 production 仓位建议、参数晋级或
     真实交易。
3. 增加聚合分析：
   - 按 `strategy_id` 聚合。
   - 按 `symbol` 聚合。
   - 按 `reason_code` 聚合。
   - 按 `blocked_by` 聚合。
   - 按 confidence bucket 聚合。
   - 按 `market_snapshot_source` 聚合。
   - 每组输出 `sample_count`、`candidate_count`、`generated_intents`、`filled_count`、
     `avg_paper_pnl`、`synthetic_snapshot_ratio`、`quality_status`。
4. Daily task dashboard 新增轻量入口：
   - 只展示 `evaluation_status`、主要 `blocked_by`、`synthetic_snapshot_ratio`、
     `sample_count`、report link。
   - 不在 dashboard 塞详细聚合表。
   - 明确 `observe-only` / `production_effect=none`。

## Policy Manifest

Evaluation gate 阈值影响 paper signal 解释，属于投资面对的 heuristic。第一版阈值必须
放入 `config/paper_signal_quality_policy.yaml`，并在 JSON/Markdown 报告中暴露 policy
id、version、status、阈值和配置路径。该 policy 是 pilot baseline，不是实盘校准结论。

## 边界

- 不读取 broker API key。
- 不调用 IBKR、Alpaca 或任何真实 broker。
- 不触发 paper runner。
- 不触发 paper replay。
- 不改变 production position recommendation。
- 不影响参数晋级、正式 ledger、approved overlay 或 production scoring。
- 不把 paper PnL 当成上线依据；paper PnL 只能作为只读诊断字段。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check src tests scripts`
- `python -m black --check src tests scripts`

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求基于现有 paper trading summary、
  order intent candidates、multi-day replay 和 dashboard trend，新增只读 paper signal
  quality evaluation 层，同时保持所有 safety boundary。
- 2026-05-17：实现完成，继续保持 `IN_PROGRESS`。新增
  `config/paper_signal_quality_policy.yaml`、`scripts/run_paper_signal_quality.py`、
  `paper_signal_quality_YYYY-MM-DD.json/md` 生成器、evaluation gate、六类聚合、
  daily task dashboard 轻量卡片和文档/测试。验证通过 `python -m pytest
  tests/trading_engine`、`python -m pytest tests/test_daily_task_dashboard.py`、
  `python -m pytest`、`python -m ruff check src tests scripts` 和本次改动文件
  Black check；`python -m black --check src tests scripts` 在当前 Black 26.5.0 /
  Python 3.14.4 环境下仍报告 125 个既有文件 would reformat，未为通过该命令重排
  无关文件。
- 2026-06-07：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：本轮用项目 `.venv`
  Python 3.11 / Black 26.5.0 对 TRADING-006 触达的 dashboard 文件完成机械格式化，
  目标 paper signal quality tests、dashboard tests、ruff 和目标文件 Black check 通过。
  全仓 `python -m black --check src tests scripts` 仍受既有无关文件 baseline 阻断，
  不在本任务中重排无关模块。
- 2026-06-09：从 `VALIDATING` 改为 `BASELINE_DONE`。原因：本轮按 operations
  runbook 先执行数据质量门，结果为 `PASS_WITH_WARNINGS` / 错误数 0；isolated
  缺输入 smoke 输出 `INSUFFICIENT_DATA`、`sample_count=0`、`candidate_count=0`、
  `filled_count=0`，并保持 `market_regime=ai_after_chatgpt`、`production_effect=none`、
  `observe_only=true`、`PAPER_ONLY_SIMULATION` / `DAILY_INDEPENDENT_ONLY` warning、
  不调用 broker、不运行 replay、不改变参数晋级；目标 paper signal quality + dashboard
  pytest 25 passed，Ruff 和触达文件 Black check 通过。剩余真实 daily dashboard 连续
  观察已拆分为 `TRADING-006B`，repo-wide formatter baseline 已拆分为 `DEV-001`。
