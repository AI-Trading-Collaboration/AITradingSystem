# TRADING-014A / TRADING-014B / TRADING-014C：Regular Session Fill Follow-up

最后更新：2026-05-18

关联任务：`TRADING-014A`、`TRADING-014B`、`TRADING-014C`

## 背景

`TRADING-014` 已为 controlled fill 增加 market session guard。当前最有价值的
下一步不是改造 `PaperBroker` fill 行为，而是在美股 regular session 内采集一个真实
IBKR Paper controlled fill 样本。

没有真实 regular session `fill_seen=true` 样本前，不进入真正的 fill model 改造。
当前 `PaperBroker` fill 行为保持不变，后续只允许做样本固化、诊断和 proposal。

## 推荐路线

```text
TRADING-014 完成
↓
等待正常交易时段
↓
运行 controlled fill
↓
如果 fill_seen=true：
    TRADING-014A Successful Fill Local Validation
    → TRADING-014C Fill Model Calibration Proposal
如果 fill_seen=false：
    TRADING-014B Fill Failure Diagnostics
```

## TRADING-014A：Successful Fill Local Validation

目标是在 `market_session_status=REGULAR` 且 `fill_seen=true` 时，固化本机真实
controlled fill 样本。

验收边界：

- 记录 sanitized review / fixture，不提交完整 `DUP` account、真实 broker order id、
  perm id、execution id、token、password 或 API key。
- 明确 `production_effect=none`、paper-only、manual-only。
- 记录 `market_session_status=REGULAR`、`fill_seen=true`、fill quantity、fill price、
  commission / execution event 是否可见，以及同 `OrderIntent` 的本地 `PaperBroker`
  诊断对比结果。
- 该任务只验证和固化样本，不修改 `PaperBroker` fill 行为。

## TRADING-014B：Fill Failure Diagnostics

目标是在 `market_session_status=REGULAR` 但 `fill_seen=false` 时，诊断为什么受控小额
订单仍未成交。

验收边界：

- 区分 broker rejection、market data / quote stale、limit price 不可成交、order status
  path 异常、TWS / IB Gateway 连接事件、contract details / liquid hours 解析问题。
- 输出 issue classification 和下一步检查建议。
- 仍不得把 no-fill 直接归因于 `PaperBroker` fill model。
- 不修改 `PaperBroker` fill 行为。

## TRADING-014C：Fill Model Calibration Proposal

目标是在 `TRADING-014A` 已有真实 `fill_seen=true` 样本后，提出可审计的 fill model
calibration 方案，而不是直接实现行为改造。

验收边界：

- 以真实 regular session fill evidence 为输入，说明当前 `PaperBroker` 与 IBKR Paper
  行为差异、数据需求、风险和验证计划。
- proposal 必须列出候选调整项、预期影响、回归测试范围、报告语义变化和退出条件。
- 任何阈值或启发式必须进入 reviewed policy / manifest，或作为有注释的不可调常量。
- 进入实际 `PaperBroker` fill 行为修改前，需要单独任务和 owner 决策。

## 状态记录

- 2026-05-18：新增 follow-up 路线。原因：owner 明确下一步最有价值的是获取真实
  regular session fill 样本；无 fill 样本前不进入真正 fill model 改造，尤其不得修改
  `PaperBroker` fill 行为。
