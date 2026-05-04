# 交易摩擦模型基础版

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`COST-001`、`EXEC-001`、`BACKTEST-001`、`PROD-001`

## 背景

当前回测已经支持基础单边交易成本 `cost_bps` 和线性滑点 `slippage_bps`，但生产就绪复盘指出，报告还不能充分表达 bid-ask spread、市场冲击、税费、汇率、融资和 ETF 成交延迟等假设。缺少这些显式假设时，回测收益容易被误读为可执行交易结果。

## 第一阶段范围

第一阶段不接入券商成交回报或逐笔盘口数据，先把成本假设拆成可审计字段，并写入回测明细、报告和 trace run manifest：

- commission / explicit trading fee。
- bid-ask spread。
- linear slippage。
- market impact。
- tax。
- FX conversion。
- annual financing carry。
- ETF execution delay 或申赎/成交延迟。

## 明确不做

- 不根据账户规模、订单簿深度或实时成交数据自动估计冲击。
- 不处理具体税务制度、券商费率表或融资账户条款。
- 不改变 `score-daily` 的 advisory 执行建议，也不生成真实订单。
- 不把基础成本模型视为生产成交质量验证。

## 验收标准

- 回测 CLI 支持显式传入 spread、market impact、tax、FX、financing 和 ETF delay 假设。
- `backtest_daily.csv` 保存每类成本扣减和总成本。
- 回测报告显示成本假设和成本扣减摘要，说明仍不等于真实成交回报。
- evidence bundle / run manifest 记录成本假设，便于复现。
- 测试覆盖成本拆分、CLI 输出和 trace manifest。

## 状态记录

- 2026-05-04：进入基础实现；目标是扩展回测成本字段、CLI 参数、报告摘要、trace manifest 和测试。完整 `DONE` 仍需要真实成交样本、券商费率/税费规则、容量约束和成交质量验证。
- 2026-05-04：基础版已完成：`aits backtest` 支持显式成本假设拆分，`backtest_daily.csv` 保存每类扣减，回测报告输出假设和摘要，trace run manifest 写入 `cost_assumptions`。完整 `DONE` 仍需要真实成交样本、券商费率/税费规则、容量约束、最小交易单位和成交质量验证。
