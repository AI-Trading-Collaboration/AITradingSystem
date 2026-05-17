# TRADING-004：Paper Replay Quality & Continuous Portfolio

最后更新：2026-05-17

关联任务：`TRADING-004`

## 背景

TRADING-003 已新增多日 paper replay，但当前 replay 语义仍需要更明确：它逐日从
候选文件独立创建 paper portfolio，不会把前一日持仓、cash 或 open order 结转到
下一日。因此它只能验证 daily paper flow 是否可运行、可复盘、可解释，不能解释为
连续组合收益。

本任务目标不是接真实券商，也不是扩展订单类型，而是提高 paper replay 的解释边界、
market snapshot 数据来源透明度和质量标记。

## 范围

1. 明确 replay 语义：
   - `paper_trading_replay` JSON 新增 `replay_mode`。
   - 当前默认 `daily_independent`。
   - 新增 `portfolio_carry_forward=false`。
   - Markdown 明确说明当前 replay 是逐日独立模拟，不是连续组合收益。
2. 预留 continuous portfolio 模式：
   - CLI 增加 `--mode daily-independent / continuous-portfolio`。
   - 第一阶段只实现 `daily-independent`。
   - `continuous-portfolio` 返回 `NOT_IMPLEMENTED` 或受限状态，不生成虚假的连续持仓结转结果。
3. 改善 fill 模拟数据来源：
   - 新增 `MarketSnapshotProvider` 接口。
   - replay 优先读取历史 OHLC。
   - 读取不到历史 OHLC 时，允许使用 candidate metadata snapshot。
   - 仍读取不到时，才使用 `candidate.limit_price` synthetic snapshot。
   - daily summary 和 replay summary 记录 `market_snapshot_source` / source counts：
     `historical_ohlc`、`candidate_metadata`、`synthetic_limit_price`。
4. 增加 replay 质量指标：
   - `quality_flags.synthetic_snapshot_days`
   - `quality_flags.missing_candidate_days`
   - `quality_flags.limited_upstream_days`
   - `quality_flags.error_days`
   - `quality_flags.empty_candidate_days`
   - Markdown 展示这些 flags 和 synthetic snapshot 使用次数。
   - synthetic snapshot 使用占主导时，整体 status 至少为 `LIMITED`。
5. 格式化与可维护性：
   - 对 `scripts/run_paper_trading_from_candidates.py` 和
     `scripts/run_paper_trading_replay.py` 执行 Black 格式化。
   - 遵循项目 ruff line length。
   - 补充脚本可维护性测试，避免 replay 脚本退化为超长单行文件。

## 边界

- 不接真实 broker，不读取 broker API key，不调用 IBKR / Alpaca。
- 不扩展订单类型。
- `continuous-portfolio` 本阶段不实现真实持仓结转；输出必须明确
  `NOT_IMPLEMENTED`，不能产生连续组合收益。
- historical OHLC 用于提高 paper fill simulation 的真实性，但仍是 paper-only
  复盘，不代表真实成交价、真实流动性、滑点或券商状态。
- replay 输出继续保持 `production_effect=none`，不得改变 production scoring、
  position gate、正式 ledger、approved overlay 或生产仓位建议。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python scripts/run_paper_trading_from_candidates.py --date 2026-05-17`
- `python scripts/run_paper_trading_replay.py --start 2026-05-01 --end 2026-05-17 --mode daily-independent`
- replay JSON 包含 `replay_mode`、`portfolio_carry_forward`、`quality_flags`。
- 测试覆盖 historical OHLC 和 synthetic fallback。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求继续推进 paper replay 的语义、
  market snapshot 数据来源和质量标记，同时只做 continuous portfolio 结构预留。
- 2026-05-17：实现完成并进入验证。新增 `replay_mode`、
  `portfolio_carry_forward`、`quality_flags`、`--mode daily-independent /
  continuous-portfolio`、`MarketSnapshotProvider`、historical OHLC / metadata /
  synthetic snapshot source 记录、continuous portfolio `NOT_IMPLEMENTED` 边界、
  Black 配置和脚本可维护性测试。验证通过 `python -m pytest tests/trading_engine`、
  `python -m pytest`、`python -m black --check scripts/run_paper_trading_from_candidates.py
  scripts/run_paper_trading_replay.py`、`python -m ruff check scripts src tests`、
  `git diff --check`、`python scripts/run_paper_trading_from_candidates.py --date
  2026-05-17` 和 `python scripts/run_paper_trading_replay.py --start 2026-05-01
  --end 2026-05-17 --mode daily-independent`。
