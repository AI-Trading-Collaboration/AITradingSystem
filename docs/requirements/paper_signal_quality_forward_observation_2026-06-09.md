# TRADING-006B：Paper Signal Quality Forward Observation

状态：`BLOCKED_EXTERNAL`

最后更新：2026-06-09

关联任务：`TRADING-006B`

## 背景

`TRADING-006` 和 `TRADING-006A` 已完成 paper signal quality 的只读评估层、
policy snapshot、evaluation gate、dashboard 轻量入口和安全边界验证。当前剩余缺口
不是实现缺失，而是需要真实 daily flow 连续样本确认 dashboard 展示与报告语义在运行期
稳定。

## 范围

1. 连续观察 3 到 7 个已完成美股交易日或等价人工 daily-run 样本。
2. 每日记录：
   - `paper_signal_quality_YYYY-MM-DD.json/md` 是否生成；
   - `evaluation_status`、`blocked_by`、`sample_count`、`filled_count`；
   - `synthetic_snapshot_ratio`、`historical_ohlc_coverage`、
     `reconciliation_pass_ratio`；
   - daily task dashboard Paper Signal Quality 卡片是否只读展示同日 artifact；
   - `production_effect=none`、`observe_only=true` 和 broker/replay safety flags 是否保持。
3. 对缺输入、样本不足、synthetic snapshot 偏高、reconciliation 异常等情况保持
   fail-closed 解释，不补造样本、不把 paper PnL 解释为上线依据。

## 边界

- 不触发 paper runner。
- 不触发 replay。
- 不读取 broker API key。
- 不调用真实 broker。
- 不改变 production 仓位建议。
- 不影响参数晋级、正式 ledger、approved overlay 或 production scoring。

## 验收标准

- 至少 3 个已完成美股交易日或等价人工样本有可审计记录。
- dashboard 与 JSON/Markdown 报告状态一致。
- 缺输入或低质量输入时保持 `INSUFFICIENT_DATA`、`LOW_DATA_QUALITY` 或
  `UNRELIABLE` 等保守状态。
- 所有样本固定 `production_effect=none`，且安全边界字段未退化。
- 观察完成后更新任务登记、本文档和归档状态。

## 状态记录

- 2026-06-09：新增并进入 `BLOCKED_EXTERNAL`。原因：`TRADING-006/006A` 的工程
  基线已通过 isolated smoke、目标 pytest、Ruff 和 scoped Black，但当前没有连续真实
  daily dashboard / paper signal quality forward 样本；该运行期观察不能用单日 smoke
  替代。
