# TRADING-003：Paper Trading Flow Validation & Replay

最后更新：2026-05-17

关联任务：`TRADING-003`

## 背景

当前 paper trading 已能从 `order_intent_candidates_YYYY-MM-DD.json` 生成
paper-only `OrderIntent`、风控结果、paper order、模拟成交、reconciliation 和
summary。下一步不是扩展真实券商能力，而是验证 paper trading daily flow 是否能
连续运行、可复盘、可解释，并能在 dashboard 中看到短期趋势。

## 范围

1. 新增多日 replay 脚本：
   - `scripts/run_paper_trading_replay.py`
   - 参数：`--start YYYY-MM-DD --end YYYY-MM-DD`
   - 对窗口内每一天复用 `run_paper_trading_from_candidates.py` 的核心逻辑。
   - 默认读取 `outputs/reports/order_intent_candidates_YYYY-MM-DD.json`。
   - 候选文件缺失时，按当前 candidate runner 逻辑生成 limited upstream
     artifacts。
2. 新增 replay 汇总产物：
   - `outputs/reports/paper_trading_replay_START_END.json`
   - `outputs/reports/paper_trading_replay_START_END.md`
   - 汇总 candidate、风控、订单、成交、PnL、reconciliation status 分布。
   - 按 `symbol`、`strategy_id`、`reason_code`、`blocked_by` 聚合统计。
3. dashboard 增加历史摘要输入：
   - daily task dashboard 可读取最近 N 日
     `paper_trading_summary_YYYY-MM-DD.json`。
   - 展示最近 7/14/30 日 Paper Trading Trend。
   - 缺失历史数据时显示 `LIMITED`，不补造结论。
4. 测试覆盖：
   - replay 脚本可跑多个日期。
   - 缺失候选文件时生成 limited artifacts。
   - replay JSON schema 稳定。
   - `production_effect` 始终为 `none`。
   - replay 不调用真实 broker、不读取 API key。
   - dashboard 历史趋势缺数据时不报错。

## 边界

- replay 是 paper-only flow validation，不是实盘收益、不代表真实账户成交。
- replay 不读取真实 broker API key，不调用 IBKR、Alpaca 或任何真实下单接口。
- replay 输出 `production_effect=none`，不得改变 production scoring、position
  gate、正式 prediction ledger、approved overlay 或生产仓位建议。
- 缺少候选或上游日报时只能生成 limited artifacts，并在汇总中披露限制；不得补造
  investment conclusion 或 paper trading 结论。
- dashboard trend 只读取已存在的 daily paper summary；缺日只进入 `LIMITED`
  状态，不主动运行 replay 或 daily runner。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python scripts/run_paper_trading_from_candidates.py --date 2026-05-17`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17`
- replay JSON/Markdown 明确声明 paper-only、`production_effect=none`、不影响
  production 仓位建议。
- 文档同步更新 `docs/artifact_catalog.md`、`docs/system_flow.md` 和任务登记。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求继续推进 paper trading
  daily flow validation 和多日 replay；本轮只验证 paper-only 连续运行、复盘和
  dashboard trend，不扩展真实券商能力。
- 2026-05-17：实现完成并进入验证。新增多日 replay 脚本、replay JSON/Markdown
  汇总、按 symbol / strategy_id / reason_code / blocked_by 聚合、dashboard
  Paper Trading Trend 和目标测试；验证通过 `python -m pytest tests/trading_engine`、
  `python -m pytest`、`python -m ruff check scripts src tests`、`git diff --check`、
  `python scripts/run_paper_trading_from_candidates.py --date 2026-05-17` 和
  `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17`。
