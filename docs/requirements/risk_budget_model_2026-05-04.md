# 风险预算模型基础版

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`SCORE-003`、`PORTFOLIO-002`、`SCORE-001`、`MACRO-001`、`GOV-001`

## 背景

当前日报已经把 AI 产业链评分映射为风险资产内 AI 仓位区间，并用 `position_gate` 叠加组合总资产上限、风险事件、估值拥挤、thesis 状态和数据置信度。生产就绪复盘指出，这仍不足以表达目标波动率、最大回撤、相关性、节点集中度和真实组合暴露约束。

`SCORE-003` 的基础版目标不是引入黑箱仓位优化器，而是把可解释、可审计的风险预算约束作为一个新的 `position_gate` 接入同一仓位约束链路。

## 第一阶段范围

第一阶段实现 `risk_budget` gate：

- 保留总分生成的基础仓位区间。
- 读取 `config/portfolio.yaml:risk_budget` 中的阈值和上限。
- 用已通过数据质量门禁的 `^VIX` 水平和分位作为市场压力约束。
- 在真实持仓 CSV 已接入且校验通过时，使用 `PORTFOLIO-002` 的单票、产业链节点、相关性簇和 ETF beta 覆盖率作为集中度约束。
- 缺少真实持仓时，不用观察池或模型建议仓位替代真实组合，只在原因中声明集中度约束未生效。
- 输出进入日报、`scores_daily.csv` gate 摘要、decision snapshot、回测每日明细和回测报告已有 gate 汇总。

## 明确不做

- 不做均值方差优化、自动下单、税费优化或融资约束。
- 不把未接入真实持仓解释为真实账户风险已受控。
- 不用 `config/watchlist.yaml`、模型目标仓位或 AI 产业链评分推断账户持仓。
- 不让候选风险预算规则绕过 `GOV-001` 规则治理。

## 分步开发

1. 扩展 `config/portfolio.yaml` 和配置 schema，增加 `risk_budget` 阈值、集中度上限、ETF beta 覆盖要求和触发后的仓位上限。
2. 新增 `risk_budget` position gate，按市场压力和真实组合集中度取最严格上限。
3. 在 `score-daily` 中传入风险预算配置和组合暴露报告；在回测中传入风险预算配置，回测只使用可 point-in-time 的市场压力约束。
4. 在日报和回测报告的现有“仓位闸门”中展示 `risk_budget` gate 来源、上限、触发状态和原因。
5. 更新 `docs/system_flow.md`、`docs/task_register.md` 和测试，覆盖高 VIX、单票/节点集中、缺少真实持仓不伪造约束、回测 gate 列输出。

## 验收标准

- 高 VIX 或高 VIX 分位能触发 `risk_budget` gate，降低最终风险资产内 AI 仓位上限。
- 真实持仓接入后，单票、节点、相关性簇或 ETF beta 覆盖不足能触发 `risk_budget` gate。
- 缺少真实持仓时，风险预算 gate 不使用观察池或模型仓位替代账户持仓。
- `score-daily` 和 `backtest` 使用同一 gate 语义；回测不读取未来或当日不可见的真实持仓。
- 报告明确风险预算是仓位上限约束，不是自动交易指令。

## 状态记录

- 2026-05-04：进入基础实现；先接入可审计 `risk_budget` gate、配置 schema、日报/回测输出和测试。完整 `DONE` 仍需要真实持仓、风险预算参数的长期验证、真实交易摩擦和 `GOV-001` 规则审批流程。
- 2026-05-04：基础版已完成：`config/portfolio.yaml:risk_budget`、共享 `risk_budget` position gate、`score-daily` 真实持仓集中度约束、回测市场压力约束、执行纪律 `risk_budget` 禁止主动加仓语义、系统流图和测试均已接入。当前仍为 `BASELINE_DONE`，完整 `DONE` 需要真实账户持仓、参数 replay/shadow 验证、流动性/税费/相关性扩展和 `GOV-001` 批准流程。
