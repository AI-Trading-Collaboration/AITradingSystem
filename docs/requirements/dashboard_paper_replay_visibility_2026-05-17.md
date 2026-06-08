# TRADING-005：Dashboard Paper Replay Visibility

最后更新：2026-06-09

关联任务：`TRADING-005`

## 背景

TRADING-003/004 已完成 paper trading daily summary、多日 replay、replay 语义标记、
market snapshot source 和质量指标。daily task dashboard 已有基础 Paper Trading Summary
和单窗口趋势读取能力，但还需要把 replay 质量和历史趋势信息更明确地带回每日主入口，
方便连续观察 paper flow 的执行质量。

本任务目标不是接真实券商，不扩展订单类型，也不让 paper replay 影响 production
仓位建议。dashboard 只读读取已有 JSON artifact，不主动运行 paper runner 或 replay。

## 范围

1. Daily task dashboard 增强 Paper Trading Trend：
   - 同时展示最近 7 / 14 / 30 日窗口。
   - 汇总 `candidate_count`、`blocked_candidates`、`generated_intents`、
     `filled/open/cancelled`、`realized/unrealized PnL`。
   - 汇总 `reconciliation_status` 分布。
   - 展示 `replay_mode=daily_independent` 和 `portfolio_carry_forward=false`。
2. 接入 replay 质量可见性：
   - 汇总 `market_snapshot_source_counts`。
   - 展示 synthetic snapshot 使用数量和比例。
   - 缺历史 summary 时显示 `LIMITED`，不补造趋势结论。
3. 接入 candidate 解释维度：
   - 只读读取同日 `order_intent_candidates_YYYY-MM-DD.json`。
   - 聚合 top `blocked_by` 和 top `reason_code`。
   - candidate 文件缺失不阻断 dashboard，只作为趋势限制说明。
4. 保持生产边界：
   - `production_effect=none`。
   - 不改变 production position recommendation。
   - 不影响参数晋级、正式 ledger 或 scoring/gate。
   - 不读取 broker API key，不调用真实 broker，不触发 paper runner / replay。

## 边界

- Paper Trading Trend 是 dashboard 汇总视图，不是连续组合收益。
- `replay_mode=daily_independent` 表示按日 summary 独立汇总；当前仍没有连续持仓、
  cash 或 open order 结转。
- synthetic snapshot 比例只用于解释 paper fill simulation 质量，不代表真实成交质量。
- 缺失历史 artifact 必须显示 `LIMITED`，不得用当前日或 replay 总表反推每日结论。

## 验收标准

- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- dashboard JSON 包含 7 / 14 / 30 日 Paper Trading Trend 窗口。
- dashboard HTML 展示 7 / 14 / 30 日趋势、synthetic snapshot 使用、top
  `blocked_by` 和 top `reason_code`。
- 历史 summary 或 candidate 缺失时不报错，趋势状态为 `LIMITED`。
- 所有 dashboard paper trend 输出保持 `production_effect=none`，并明确不改变生产仓位建议。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求在 TRADING-004 后把 paper replay
  结果和质量信号接回 daily task dashboard，同时保持只读、paper-only 和
  production_effect=none 边界。
- 2026-05-17：实现完成并进入验证。daily task dashboard 已同时输出 7/14/30 日
  Paper Trading Trend 窗口，包含 `replay_mode=daily_independent`、
  `portfolio_carry_forward=false`、只读 execution boundary、candidate / intent /
  fill / PnL / reconciliation 汇总、synthetic snapshot 使用比例、top `blocked_by`
  和 top `reason_code`；缺 summary 或 candidate JSON 时保持 `LIMITED`，不运行
  replay、不补造趋势结论。验证通过 `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest`、
  `python -m ruff check src tests scripts`、`python -m black --check
  src/ai_trading_system/daily_task_dashboard.py tests/test_daily_task_dashboard.py`
  和 `git diff --check`。
- 2026-06-09：系统验证收尾并归档为 DONE。当前 latest
  `daily_task_dashboard_2026-06-05.json/html` 字段级复核确认 Paper Trading Trend
  包含 7/14/30 窗口、`production_effect=none`、`replay_mode=daily_independent`、
  `portfolio_carry_forward=false`、只读 execution boundary、synthetic snapshot
  summary、top `blocked_by` 和 top `reason_code`。该 latest run 的 run-bundle
  reports 目录缺少对应 paper summaries，因此趋势按设计为 `LIMITED`，所有缺失日
  记录仍显式写入 `production_effect=none`，不反推、不补造、不运行 runner 或
  replay。验证通过 `python -m pytest tests/test_daily_task_dashboard.py -q`
  （22 passed）、`python -m pytest tests/trading_engine -q`（939 passed）、
  `python -m black --check src/ai_trading_system/daily_task_dashboard.py
  tests/test_daily_task_dashboard.py` 和 scoped Ruff。后续 paper signal quality、
  continuous replay visibility 与真实 daily flow 观察由 TRADING-006/TRADING-007A+
  承接，不作为 TRADING-005 未完成项。
